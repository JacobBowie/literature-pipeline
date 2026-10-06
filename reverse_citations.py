"""Reverse-citation walker: what each library PDF cites (the backward walk).

For every PDF in a project's library, collect the papers it cites from up to five sources, in
this order (refactor scope 3.4; dispatch W3-B, K5 revised):

  1. s2        Semantic Scholar references, through litpipe.s2.batch_nested on ONE Session per run
               (6.5 s spacing unkeyed, 1.1 s with S2_API_KEY). A failed S2 walk is a FAILED seed: it
               keeps the rows the published CSV holds for it, and no other network source is asked.
  2. openalex  OpenAlex referenced_works (litpipe.openalex) for seeds S2 has no list for: elided by
               the publisher, a referenceCount of 0, or no S2 record. Needs OPENALEX_API_KEY; without
               it the leg is skipped with a one-line notice (never an error). A
               referenced_works_count of 0 means "unknown", never "no references" (V2-N8).
  3. crossref  Crossref `reference` deposits for seeds OpenAlex did not answer, de-duplicated (Human
               Kinetics deposits list each entry twice, V2-N6). No deposit means "unknown".
  4. sidecar   the JATS sidecar's structured `references`, as before.
  5. regex     the References-section parse of the text dump or the PDF text: a printed-list count
               and the last resort. Fixed: the first item of a numbered list is kept (V2-N1); the
               header must stand on its own line, and the first header past the middle of the text
               wins (V2-N3); author-year lists are split (V2-N4).
`--sources` (default s2,openalex,crossref,regex) picks the legs; "regex" is the local parse (the
sidecar and the text/PDF parse together). `--sources regex` sends nothing, as the walker did
before W3-B.

The union per seed: the first network source that answered gives the seed's network rows; the
sidecar and regex rows add only DOIs the network rows lack (all their rows when no network source
answered). Then: the seed's own DOI is dropped, an arXiv ID with no DOI becomes its 10.48550 DOI,
and every DOI is normalised by litpipe.doi (a structured source's DOI keeps its whole registered
form through normalise_structured; a regex DOI takes normalise). A seed whose network walk failed, or whose
sources were skipped or disabled this run, keeps the network rows it had; a seed whose prior rows
came from a higher source than this run's answer keeps them when that source could not answer this
run. A seed whose stored rows came from S2 or OpenAlex and whose PDF is unchanged is not requested
again within walk_cadence_days (projects.json, default 30 days; `--refresh` ignores it); its stamp
lives in the litpipe.state kv namespace "reverse_walk" and is written only when a run publishes.

Outputs (default prefix <lib>/_reverse_citations; a dotted --out-prefix is kept whole):
  <prefix>.jsonl               one JSON object per reference considered (seed, raw, doi, seed_doi,
                               source, kept); a side output
  <prefix>_parsed.csv          seed, first_author, year, title_snippet, doi, raw, seed_doi, source
                               (index_portfolio ingests this one; source is s2|openalex|crossref|
                               sidecar|regex, seed_doi is normalised and empty when unknown)
  <prefix>_unique.csv          the de-duplicated DOIs; a side output
The three are replaced only by a clean run. Otherwise this run's rows go to
<prefix>_parsed.degraded.csv and the outputs stay byte-identical.

Exit codes (the shared convention):
  0  clean: the outputs were written.
  1  usage or configuration error (no --project/--lib-dir, an unregistered project, a missing
     registry, a bad --sources value or s2/openalex block, a key OpenAlex rejects).
  2  degraded: more than 5 % of the seeds walked over the network failed, or fewer seeds have
     references than in the published CSV (`--force` accepts that one). Nothing published.
  3  aborted: a run budget is spent, a host deferred us, or a circuit breaker tripped. Nothing
     published; rerun later.
The last stdout line is "[step-summary] {json}" (reasons, aborted, transport_failures, ...).

Usage:
  python reverse_citations.py --project research_a
  python reverse_citations.py --project research_a --sources regex      # offline, as before W3-B
  python reverse_citations.py --lib-dir /path/to/library [--text-dir /path/to/text]
"""
import argparse
import csv
import io
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

import lit_util
from litpipe import config, ledger, net, s2
from litpipe import doi as _doi
from litpipe import openalex as oa
from litpipe.outcomes import Kind, Outcome
from litpipe.text import normalise_title

lit_util.utf8_stdout()

CONFIG_PATH   = Path(__file__).parent / "projects.json"

LEGACY_FIELDS = ["seed", "first_author", "year", "title_snippet", "doi", "raw"]
FIELDS = LEGACY_FIELDS + ["seed_doi", "source"]           # W3-C1 contract: the two new columns last
SOURCES = ("s2", "openalex", "crossref", "regex")         # --sources values ("regex" = the local parse)
NETWORK_SOURCES = ("s2", "openalex", "crossref")
ROW_SOURCES = ("s2", "openalex", "crossref", "sidecar", "regex")
STRUCTURED_ROW_SOURCES = frozenset(ROW_SOURCES) - {"regex"}   # = index_portfolio.STRUCTURED_SOURCES (locked)
_RANK = {s: i for i, s in enumerate(ROW_SOURCES)}
S2_REF_FIELDS = ("paperId", "externalIds", "title", "year", "authors")

FAIL_THRESHOLD = 0.05            # more than 5 % of network seed walks failed: degraded (exit 2)
DEFAULT_CADENCE_DAYS = 30
CADENCE_NS = "reverse_walk"
PARSE_SUSPECT_RATIO = 0.5        # V2-N4: printed-list count under half the network count
SUMMARY_MARKER = "[step-summary] "
TRANSPORT_KINDS = frozenset({"TRANSPORT", "OUTAGE", "REFUSED", "DEFERRED"})
EXIT_OK, EXIT_ERROR, EXIT_DEGRADED, EXIT_ABORTED = 0, 1, 2, 3

CROSSREF_WORKS = "https://api.crossref.org/works"
CROSSREF_BATCH = 25              # V2: 25 DOIs per filter list call measured (1,035-byte URL)
CROSSREF_BREAKER = 3

# ---------- references-section locator ----------

# V2-N3: the header must stand on its own line (an optional section number before it), or end a
# line right after a sentence ("... in conclusion. References"), or be a capitalised REFERENCES
# ending a line ("SDC 1 Table REFERENCES"); case as printed, so "with special reference\nto ..."
# inside a cited title no longer matches.
_HEADER_WORDS = (
    r"R[ \t]*E[ \t]*F[ \t]*E[ \t]*R[ \t]*E[ \t]*N[ \t]*C[ \t]*E[ \t]*S|References?|REFERENCES?"
    r"|Reference[ \t]+[Ll]ist|REFERENCE[ \t]+LIST|References[ \t]+and[ \t]+[Nn]otes|REFERENCES[ \t]+AND[ \t]+NOTES"
    r"|Bibliography|BIBLIOGRAPHY|Literature[ \t]+[Cc]ited|LITERATURE[ \t]+CITED|Works[ \t]+[Cc]ited|WORKS[ \t]+CITED")
REFS_HDR = re.compile(
    r"(?:^[ \t]*(?:\d{1,2}\.?[ \t]+)?(?:" + _HEADER_WORDS + r")"
    r"|(?<=[.)])[ \t]+References|(?<=\S)[ \t]+REFERENCES)[ \t]*:?[ \t]*$",
    re.MULTILINE)

NUM_BRACKET = re.compile(r'\n\s*\[\s*\d{1,3}\s*\]\s*')
NUM_DOT     = re.compile(r'\n\s*\d{1,3}\.\s+')
AUTHOR_YEAR_HANG = re.compile(
    r'\n(?=[A-Z][a-z]+(?:,\s*[A-Z]\.?|,?\s+[A-Z]\.)(?:[^.\n]{0,200}?\(\d{4})\b)',
    re.MULTILINE
)
YEAR_RE = re.compile(r'\b(19[5-9]\d|20[0-2]\d)\b')

# V2-N4: author-year lists. A reference starts at a line that opens with an author block ending in
# a year (`Surname AB, ... (YYYY).`, `Surname AB. YYYY.`, `Surname, A.B. (YYYY)`, `SURNAME, A.
# YYYY.`; names may wrap across lines), when the previous line ends a reference (a period, a
# bracket, a digit, a DOI or a URL) rather than continuing an author list.
_UP = "A-ZÀ-ÖØ-Þ"
_LO = "a-zß-öø-ÿ"
_PARTICLE = (r"(?:(?:[Vv]an|[Vv]on|[Dd]e|[Dd]el|[Dd]ella|[Dd]er|[Dd]en|[Dd]i|[Dd]a|[Dd]u|[Ll]a|[Ll]e"
             r"|[Tt]en|[Tt]er|[Dd]os|[Ss]t\.?)\s+)*+")
_SURNAME = (rf"(?:(?:[{_UP}]['’])?[{_UP}][^\W\d_]*[{_LO}][^\W\d_]*|[{_UP}]{{3,}})"
            rf"(?:[\-‐'’][{_UP}]?[^\W\d_]+)*+")
_INITIALS = rf"(?:[{_UP}]\.?(?:[\-‐][{_UP}]\.?)?\s?){{1,4}}(?=[\s,.;&(]|$)"
# Atomic author blocks and possessive separators (Python 3.11+): a line that is not a reference
# start fails in linear time instead of backtracking through every whitespace split.
_AUTHOR = rf"(?>{_PARTICLE}{_SURNAME},?\s+{_INITIALS})"
_SEP = r"\s*+[,;]?+\s*+(?:(?:&|and)\s++)?+"
AUTHOR_YEAR_START = re.compile(
    rf"{_AUTHOR}(?:{_SEP}{_AUTHOR}){{0,40}}+(?:,?\s*+et\.?\s+al\.?)?+"
    rf"\s*+[,.]?+\s*+\(?+(?:1[89]|20)\d{{2}}[a-z]?\)?(?=[\s.,;:)]|$)")
_REF_END = re.compile(r"(?:[.)\]\d]|\b(?:doi|https?)\S*)\s*$", re.IGNORECASE)
_LINE_NUMBER = re.compile(r"(?m)^[ \t]*\d{1,5}[ \t]*\n")
_ARXIV = re.compile(
    r"(?i)(?:\barxiv(?:\.org/(?:abs|pdf)/|\s*[:.]?\s*(?:preprint\s+)?(?:arxiv\s*:\s*)?)|\bcorr,?\s*abs/)"
    r"(\d{4}\.\d{4,5}|[a-z][a-z.\-]*/\d{7})(?:v\d+)?(?![\d])")


def locate_refs(text: str) -> str:
    """The References section: from the first own-line header in the second half of the text
    (else the last header anywhere) to the first Appendix/Supplementary/... line after it."""
    text = (text or "").replace("\r\n", "\n")
    matches = list(REFS_HDR.finditer(text))
    if not matches: return ""
    half = len(text) / 2
    later = [m for m in matches if m.start() >= half]
    start = (later[0] if later else matches[-1]).end()
    tail_cut = re.search(
        r'\n\s*(?:Appendix|APPENDIX|Supplementary|SUPPLEMENTARY|Acknowledg|'
        r'ACKNOWLEDG|Author contributions|Competing interests)\s',
        text[start:], re.IGNORECASE)
    end = start + tail_cut.start() if tail_cut else len(text)
    return text[start:end]


def split_author_year(refs_text: str) -> list:
    """Split an author-year list at the lines that start a reference (see AUTHOR_YEAR_START). Lines
    holding only a number (manuscript line numbers, page numbers) are dropped first."""
    blob = _LINE_NUMBER.sub("", (refs_text or "").replace("\r\n", "\n"))
    starts, prev, pos = [], "", 0
    for line in blob.split("\n"):
        stripped = line.strip()
        if stripped:
            lead = len(line) - len(line.lstrip())
            if (not prev or _REF_END.search(prev)) and AUTHOR_YEAR_START.match(blob, pos + lead):
                starts.append(pos + lead)
            prev = stripped
        pos += len(line) + 1
    bounds = starts + [len(blob)]
    return [blob[a:b].strip() for a, b in zip(bounds, bounds[1:]) if len(blob[a:b].strip()) > 30]


def split_refs(refs_text: str):
    """Individual references of a References section: a numbered list ([1] or 1.), else an
    author-year list, else the old hanging-indent split, else blank-line paragraphs."""
    blob = "\n" + (refs_text or "").replace("\r\n", "\n")  # V2-N1: item 1 needs a line start too
    chunks = NUM_BRACKET.split(blob)
    if len(chunks) > 5: return [c.strip() for c in chunks[1:] if len(c.strip()) > 30]
    chunks = NUM_DOT.split(blob)
    if len(chunks) > 5: return [c.strip() for c in chunks[1:] if len(c.strip()) > 30]
    chunks = split_author_year(refs_text)
    if len(chunks) > 5: return chunks
    chunks = AUTHOR_YEAR_HANG.split(blob)
    if len(chunks) > 5: return [c.strip() for c in chunks if len(c.strip()) > 30]
    chunks = re.split(r'\n\s*\n', blob)
    return [c.strip() for c in chunks if len(c.strip()) > 30]


def parse_one(raw: str) -> dict:
    raw_clean = re.sub(r'\s+', ' ', raw).strip()
    year = (m.group(1) if (m := YEAR_RE.search(raw_clean)) else "")
    # RC1: extract from the un-collapsed text so line-wrapped DOIs are re-joined
    # rather than truncated; helper drops malformed/suspicious (truncated) DOIs.
    doi  = lit_util.extract_doi_from_text(raw)
    first_author = ""
    m = re.match(r'^([A-Z][A-Za-z\-\']+)(?:,\s*[A-Z]|\s+[A-Z]\.)', raw_clean)
    if m: first_author = m.group(1)
    elif (m := re.match(r'^([A-Z][A-Za-z\-\']+)', raw_clean)): first_author = m.group(1)
    title = ""
    if (ym := YEAR_RE.search(raw_clean)):
        after = re.sub(r'^[\.\)\,\s]+', '', raw_clean[ym.end():])
        title = (m.group(1) if (m := re.match(r'(.+?)\.\s+(?=[A-Z])', after))
                 else after[:150])
    return {"first_author": first_author, "year": year,
            "title_snippet": title[:200].strip(),
            "doi": doi.lower() if doi else "",
            "raw": raw_clean[:500]}


def arxiv_doi(text) -> str:
    """The 10.48550 DOI of the first arXiv ID in `text` (`arXiv:2305.14152v2`, `arxiv.org/abs/...`,
    `CoRR abs/...`, old-style `hep-th/9901001`), version dropped, lower-cased; '' when none."""
    m = _ARXIV.search(text or "")
    return f"10.48550/arxiv.{m.group(1).lower()}" if m else ""


# ---------- source-priority text retrieval ----------

def text_from_sidecar_refs(sc_path: Path):
    """If JATS sidecar has structured references, return list of dicts directly."""
    if not sc_path.exists(): return None
    try:
        with open(sc_path, encoding="utf-8") as f:
            d = json.load(f)
        refs = d.get("references") or []
        if not refs: return None
        out = []
        for r in refs:
            if isinstance(r, dict):
                # RC1: normalize + validity-gate the sidecar-supplied DOI so
                # truncated/malformed values never reach the output CSV.
                doi = lit_util.normalize_doi(r.get("doi") or "")
                if not lit_util.is_valid_doi(doi) or lit_util.is_suspicious_doi(doi):
                    doi = ""
                out.append({
                    "first_author": r.get("first_author") or "",
                    "year":         str(r.get("year") or ""),
                    "title_snippet": (r.get("title") or "")[:200],
                    "doi":          doi,
                    "raw":          (r.get("text") or json.dumps(r))[:500],
                })
            else:
                out.append(parse_one(str(r)))
        return out
    except (OSError, ValueError): return None


def text_from_dump(text_dir: Path, stem: str):
    p = text_dir / (stem + ".txt") if text_dir else None
    if p and p.exists():
        try:
            with open(p, encoding="utf-8") as f:
                return f.read()
        except OSError: return None
    return None


def text_from_pdf(pdf_path: Path):
    try:
        import fitz
    except ImportError:
        return None
    try:
        doc = fitz.open(str(pdf_path))
        try:
            return "\n".join(p.get_text() for p in doc)
        finally:
            doc.close()
    except Exception:
        return None


# ---------- project resolution ----------

class ProjectError(config.ConfigError):
    """The library cannot be located (an unregistered project, a missing directory)."""


def load_registry(cfg=None) -> dict:
    """projects.json through litpipe.config.load: `cfg` when given, else this module's
    CONFIG_PATH. ConfigError (exit 1) when the file is missing or unreadable, never the exit 2 of
    ris_emit.load_projects_config."""
    if cfg is not None:
        return config.load(cfg)
    p = Path(CONFIG_PATH)
    if not p.exists():
        raise config.ConfigError(f"projects.json not found at {p} (copy projects.json.template)")
    try:
        return config.load(lit_util.load_projects_config(p))
    except (OSError, ValueError) as e:
        raise config.ConfigError(f"projects.json at {p} is unreadable: {ledger.redact(e)}") from None


def resolve_project(name: str, registry=None):
    """(library dir, text-dump dir or None) of a registered project; ProjectError when it is not."""
    projects = (registry if registry is not None else load_registry()).get("projects") or {}
    if name not in projects:
        raise ProjectError(f"'{name}' not in projects.json")
    p = projects[name] or {}
    if not p.get("lib_dir"):
        raise ProjectError(f"projects.json: '{name}' has no lib_dir")
    base = lit_util.PROJECTS_ROOT / (p.get("parent") or name)
    lib = base / p["lib_dir"]
    text_dir = (base / p["data_dir"] / "text") if p.get("data_dir") else None
    return lib, text_dir


def parse_sources(value) -> set:
    """The --sources set: a comma-separated string or a list of s2, openalex, crossref, regex."""
    if value is None:
        return set(SOURCES)
    items = value.split(",") if isinstance(value, str) else list(value)
    out = {str(v).strip().lower() for v in items if str(v).strip()}
    bad = out - set(SOURCES)
    if bad or not out:
        raise config.ConfigError(f"--sources: unknown or empty {sorted(bad) or value!r}; allowed: {','.join(SOURCES)}")
    return out


# ---------- paths ----------

def output_paths(out_prefix: Path) -> dict:
    """The three outputs and the degraded file. Names are appended to the prefix (never
    with_suffix, which would drop a dotted segment of --out-prefix)."""
    out_prefix = Path(out_prefix)
    parsed = out_prefix.parent / (out_prefix.name + "_parsed.csv")
    return {"jsonl": out_prefix.parent / (out_prefix.name + ".jsonl"),
            "parsed": parsed,
            "unique": out_prefix.parent / (out_prefix.name + "_unique.csv"),
            "degraded": parsed.with_name(parsed.stem + ".degraded.csv")}


# ---------- seeds ----------

@dataclass
class Seed:
    pdf: Path
    walked: bool
    doi: str = ""
    local_rows: list | None = None          # this run's sidecar or regex rows (None: leg off)
    local_label: str = ""                   # sidecar | text | pdf | NO_TEXT | NO_REFS_SECTION
    n_printed: int | None = None            # the regex printed-list count (or the sidecar's)
    text: str | None = None
    net_state: str = ""                     # answered | failed | skipped | api_empty | cadence | kept |
                                            # not_walked | no_doi | disabled
    net_source: str = ""
    net_rows: list | None = None
    legs: dict = field(default_factory=dict)        # source -> what it said
    definitive: dict = field(default_factory=dict)  # source -> answered this run (any answer)
    fail_kind: str = ""
    fail_reason: str = ""
    net_count: int | None = None            # the reference count S2 or OpenAlex reported
    parse_suspect: bool = False


def _gate(d, structured=True) -> str:
    """A seed DOI: structured (a .ris DO line, a sidecar field) keeps its whole registered form;
    text from the PDF head takes the text normaliser."""
    if not (isinstance(d, str) and d.strip()):
        return ""
    return (_doi.normalise_structured(d) if structured else _doi.normalise(d)) or ""


def seed_doi(pdf: Path, text=None, read_pdf=False) -> str:
    """The seed's own DOI: its .ris DO line, else its .fulltext.json `doi`, else the first DOI in
    the head of its text (the PDF is read only when `read_pdf`). Normalised; '' when unknown."""
    ris = lit_util.companion_path(pdf, ".ris")
    if ris.exists():
        d = _gate(lit_util.parse_ris(ris).get("doi") or "")
        if d:
            return d
    sc = lit_util.companion_path(pdf, ".fulltext.json")
    if sc.exists():
        try:
            with open(sc, encoding="utf-8") as f:
                d = _gate((json.load(f) or {}).get("doi") or "")
            if d:
                return d
        except (OSError, ValueError, AttributeError):
            pass
    if text is None and read_pdf:
        text = text_from_pdf(pdf)
    return _gate(lit_util.extract_doi_from_text(text, max_chars=5000), structured=False) if text else ""


def _local_leg(sd: Seed, text_dir):
    """The sidecar's references, else the regex parse of the text dump or the PDF text."""
    side = text_from_sidecar_refs(lit_util.companion_path(sd.pdf, ".fulltext.json"))
    if side:
        sd.local_label, sd.n_printed = "sidecar", len(side)
        sd.local_rows = [{**r, "source": "sidecar", "_cands": _doi.candidates(r.get("doi") or "")} for r in side]
        return
    text = text_from_dump(text_dir, sd.pdf.stem) if text_dir else None
    label = "text"
    if not text:
        text, label = text_from_pdf(sd.pdf), "pdf"
    sd.text = text
    if not text:
        sd.local_label, sd.local_rows, sd.n_printed = "NO_TEXT", [], 0
        return
    blob = locate_refs(text)
    if not blob:
        sd.local_label, sd.local_rows, sd.n_printed = "NO_REFS_SECTION", [], 0
        return
    chunks = split_refs(blob)
    sd.local_label, sd.n_printed = label, len(chunks)
    rows = []
    for c in chunks:
        r = parse_one(c)
        r["source"] = "regex"
        r["_cands"] = _doi.candidates(c)
        r["_arxiv"] = c
        rows.append(r)
    sd.local_rows = rows


# ---------- rows from the network sources ----------

def _surname(name) -> str:
    parts = str(name or "").replace(",", " ").split()
    return parts[-1] if parts else ""


def s2_row(paper) -> dict:
    """A parsed row from an S2 citedPaper object (DOI, else its arXiv ID)."""
    p = paper if isinstance(paper, dict) else {}
    ext = p.get("externalIds") if isinstance(p.get("externalIds"), dict) else {}
    authors = p.get("authors") if isinstance(p.get("authors"), list) else []
    first = authors[0].get("name") if authors and isinstance(authors[0], dict) else ""
    title = p.get("title") if isinstance(p.get("title"), str) else ""
    year = p.get("year")
    arx = ext.get("ArXiv")
    return {"first_author": _surname(first), "year": str(year) if isinstance(year, int) else "",
            "title_snippet": title[:200], "doi": ext.get("DOI") if isinstance(ext.get("DOI"), str) else "",
            "raw": f"s2 {p.get('paperId') or '-'}: {title}"[:500], "source": "s2",
            "_arxiv": f"arXiv:{arx}" if isinstance(arx, str) and arx else ""}


def openalex_row(work) -> dict:
    w = work if isinstance(work, dict) else {}
    title = w.get("display_name") or w.get("title") or ""
    title = title if isinstance(title, str) else ""
    year = w.get("publication_year")
    wid = str(w.get("id") or "").rsplit("/", 1)[-1]
    return {"first_author": "", "year": str(year) if isinstance(year, int) else "",
            "title_snippet": title[:200], "doi": w.get("doi") if isinstance(w.get("doi"), str) else "",
            "raw": f"openalex {wid}: {title}"[:500], "source": "openalex"}


def crossref_row(entry) -> dict:
    """A parsed row from one Crossref `reference` entry."""
    e = entry if isinstance(entry, dict) else {}
    un = e.get("unstructured") if isinstance(e.get("unstructured"), str) else ""
    r = parse_one(un) if un.strip() else {"first_author": "", "year": "", "title_snippet": "", "doi": "", "raw": ""}
    if isinstance(e.get("author"), str) and e["author"].strip():
        r["first_author"] = e["author"].replace(",", " ").split()[0]
    if isinstance(e.get("year"), str) and e["year"][:4].isdigit():
        r["year"] = e["year"][:4]
    title = next((e[k] for k in ("article-title", "volume-title", "series-title") if isinstance(e.get(k), str)
                  and e[k].strip()), "")
    if not title and not un.strip() and isinstance(e.get("journal-title"), str):
        title = e["journal-title"]
    if title:
        r["title_snippet"] = title[:200]
    if isinstance(e.get("DOI"), str) and e["DOI"].strip():
        r["doi"] = e["DOI"]
    if not r["raw"]:
        r["raw"] = " ".join(str(e[k]) for k in ("author", "year", "article-title", "volume-title",
                                                 "journal-title", "volume", "first-page") if e.get(k))[:500]
    r["source"] = "crossref"
    r["_arxiv"] = un
    return r


def dedupe_crossref(entries) -> list:
    """Crossref reference entries without repeats (Human Kinetics deposits list each entry twice,
    V2-N6): by normalised DOI, else by the normalised unstructured string or title, else by the
    entry's bibliographic fields."""
    out, seen = [], set()
    for e in entries or ():
        if not isinstance(e, dict):
            continue
        d = _doi.normalise_structured(e["DOI"]) if isinstance(e.get("DOI"), str) else None
        if d:
            key = ("doi", d)
        else:
            text = e.get("unstructured") or e.get("article-title") or e.get("volume-title")
            if isinstance(text, str) and normalise_title(text):
                key = ("text", normalise_title(text))
            else:
                key = ("fields",) + tuple(str(e.get(k) or "").strip().lower() for k in
                                          ("author", "year", "journal-title", "volume", "first-page"))
        if key in seen:
            continue
        seen.add(key)
        out.append(e)
    return out


class CrossrefLeg:
    """Run accounting for the Crossref leg: calls, kinds, and a breaker after CROSSREF_BREAKER
    consecutive failed calls (later batches are DEFERRED without sending)."""

    def __init__(self, breaker=CROSSREF_BREAKER):
        self.breaker = breaker
        self.calls = 0
        self.consecutive_failed = 0
        self.tripped = False
        self.kinds = {}

    def record(self, out: Outcome):
        self.calls += 1 if out.attempts else 0
        self.kinds[str(out.kind)] = self.kinds.get(str(out.kind), 0) + 1
        if out.ok:
            self.consecutive_failed = 0
            return
        self.consecutive_failed += 1
        if self.consecutive_failed >= self.breaker:
            self.tripped = True

    @property
    def aborted(self):
        return "breaker" if self.tripped else None

    def summary(self) -> dict:
        return {"calls": self.calls, "kinds": dict(self.kinds), "aborted": self.aborted}


def crossref_references(dois, *, cfg=None, state=None, leg=None) -> dict:
    """Crossref reference deposits for many DOIs: /works?filter=doi:a,doi:b,...&select=DOI,
    reference,references-count (a repeated filter is OR), CROSSREF_BATCH DOIs a call, through
    litpipe.net. Per DOI: OK (payload: the de-duplicated entries), NOT_AVAILABLE (no deposit:
    unknown, not zero), NO_MATCH (not in Crossref), ERROR (not on a truncated page), or the batch's
    failure. `select=reference-count` is refused by the list route (V2-N9): not used."""
    leg = leg or CrossrefLeg()
    host = "api.crossref.org"
    unique = list(dict.fromkeys(d for d in dois if d))
    result = {}
    for i in range(0, len(unique), CROSSREF_BATCH):
        chunk = unique[i:i + CROSSREF_BATCH]
        if leg.tripped:
            for d in chunk:
                result[d] = Outcome(Kind.DEFERRED, host=host,
                                    detail=f"crossref stopped after {leg.breaker} consecutive failed calls; nothing sent")
            continue
        out = net.get(CROSSREF_WORKS, params={"filter": ",".join("doi:" + d for d in chunk),
                                              "select": "DOI,reference,references-count",
                                              "rows": 2 * len(chunk)},
                      validate=net.expect_json, purpose=f"crossref references x{len(chunk)}", cfg=cfg, state=state)
        try:
            msg = out.payload.json().get("message") if out.ok else None
        except (ValueError, AttributeError):
            msg = None
        if out.ok and not (isinstance(msg, dict) and isinstance(msg.get("items"), list)):
            out = Outcome(Kind.ERROR, status=out.status, host=host, attempts=out.attempts,
                          elapsed_ms=out.elapsed_ms, detail="unexpected Crossref response: no message.items")
        leg.record(out)
        if not out.ok:
            for d in chunk:
                result[d] = Outcome(out.kind, status=out.status, host=host, detail=out.detail,
                                    attempts=out.attempts, elapsed_ms=out.elapsed_ms, retry_after=out.retry_after)
            continue
        items = [it for it in msg["items"] if isinstance(it, dict)]
        total = msg.get("total-results")
        truncated = isinstance(total, int) and total > len(items)
        found = {}
        for it in items:
            d = _doi.normalise_structured(it["DOI"]) if isinstance(it.get("DOI"), str) else None
            if d and d not in found:
                found[d] = it
        for d in chunk:
            base = dict(status=out.status, host=host, attempts=out.attempts, elapsed_ms=out.elapsed_ms)
            it = found.get(d)
            if it is None:
                result[d] = (Outcome(Kind.ERROR, detail=f"not on the returned page (total {total}); not a no-match", **base)
                             if truncated else Outcome(Kind.NO_MATCH, detail="not in Crossref", **base))
                continue
            refs = it.get("reference")
            if not isinstance(refs, list) or not refs:
                result[d] = Outcome(Kind.NOT_AVAILABLE, **base,
                                    detail=f"no reference deposit (references-count {it.get('references-count')}): "
                                           "unknown, not zero")
                continue
            kept = dedupe_crossref(refs)
            result[d] = Outcome(Kind.OK, payload=kept, **base,
                                detail=f"{len(kept)} references ({len(refs) - len(kept)} repeated entries dropped)")
    return result


# ---------- the published CSV and the cadence stamps ----------

def read_prior(path: Path):
    """{seed: [rows]} of a published parsed CSV (FIELDS keys; `source` '' in a legacy file), or None."""
    if not path.exists():
        return None
    out = {}
    with open(path, encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f):
            seed = r.get("seed") or ""
            if seed:
                out.setdefault(seed, []).append({k: (r.get(k) or "") for k in FIELDS})
    return out


def _state(state):
    if state is not None:
        return state
    if net.STATE is not None:
        return net.STATE
    import litpipe.state as st
    return st


def _stamp_key(parsed: Path, pdf: Path) -> str:
    return f"{Path(parsed).resolve()}|{pdf.name}"


def _fingerprint(pdf: Path):
    try:
        st = pdf.stat()
        return st.st_size, int(st.st_mtime)
    except OSError:
        return None, None


def _within_cadence(st, parsed, sd, prior_rows, days, now) -> bool:
    """A seed is not requested again when its published rows came from S2 or OpenAlex, its stamp
    is younger than `days`, and its DOI and PDF (size, mtime) are unchanged."""
    if not any(r.get("source") in ("s2", "openalex") for r in prior_rows or ()):
        return False
    try:
        stamp = st.kv_get(CADENCE_NS, _stamp_key(parsed, sd.pdf))
    except Exception:            # an unreadable state is "no stamp": walk it
        return False
    if not isinstance(stamp, dict) or stamp.get("doi") != sd.doi:
        return False
    size, mtime = _fingerprint(sd.pdf)
    if (stamp.get("size"), stamp.get("mtime")) != (size, mtime):
        return False
    walked = stamp.get("walked_at")
    return isinstance(walked, (int, float)) and 0 <= now - walked < days * 86400


# ---------- the run ----------

def run(*, project=None, lib_dir=None, text_dir=None, out_prefix=None, limit=0, sources=None,
        force=False, refresh=False, cfg=None, state=None, s2_session=None, oa_session=None) -> dict:
    """One backward walk. Returns the summary dict; its `exit_code` is the CLI's exit code."""
    try:
        registry = load_registry(cfg)
        srcs = parse_sources(sources)
        if project:
            lib, tdir = resolve_project(project, registry)
        elif lib_dir:
            lib, tdir = Path(lib_dir), None
        else:
            return _error("Pass --project or --lib-dir")
        if text_dir:
            tdir = Path(text_dir)
        if not lib.is_dir():
            return _error(f"not a directory: {lib}")
        days = (config.walk_cadence_days(project, registry) if project else None) or DEFAULT_CADENCE_DAYS
        sess = (s2_session or s2.Session(cfg=registry, state=state)) if "s2" in srcs else None
        oas = (oa_session or oa.Session(cfg=registry, state=state)) if "openalex" in srcs else None
    except config.ConfigError as e:
        return _error(ledger.redact(str(e)))
    return _Walk(lib, tdir, out_prefix, limit, srcs, force, refresh, registry, state, sess, oas, days).go()


def _error(msg) -> dict:
    print(f"[ERR] {msg}", file=sys.stderr)
    return {"step": "reverse_citations", "exit_code": EXIT_ERROR, "status": "error", "error": msg}


class _Walk:
    def __init__(self, lib, tdir, out_prefix, limit, srcs, force, refresh, registry, state, sess, oas, days):
        self.lib, self.tdir, self.srcs = lib, tdir, srcs
        self.force, self.refresh, self.registry, self.state = force, refresh, registry, state
        self.sess, self.oas, self.days = sess, oas, days
        self.crleg = CrossrefLeg()
        self.paths = output_paths(Path(out_prefix) if out_prefix else lib / "_reverse_citations")
        self.pdfs = sorted(lib.glob("*.pdf"))
        self.walk_n = min(limit, len(self.pdfs)) if limit else len(self.pdfs)
        self.network = [s for s in NETWORK_SOURCES if s in srcs]
        self.config_error = ""

    # ------------------------------------------------------------------ phases
    def go(self) -> dict:
        p = self.paths
        print(f"library:   {self.lib}")
        print(f"text-dir:  {self.tdir}  (used if no JATS refs)")
        print(f"PDFs:      {len(self.pdfs)}" + (f" (walking the first {self.walk_n})" if self.walk_n != len(self.pdfs) else ""))
        print(f"sources:   {','.join(s for s in SOURCES if s in self.srcs)}")
        print(f"outputs:   {p['jsonl'].name}, {p['parsed'].name}, {p['unique'].name} in {p['parsed'].parent}")
        if self.sess is not None:
            print(f"[s2] {s2.key_status()}  run budget={self.sess.budget} attempts  breaker={self.sess.breaker}")
        if self.oas is not None:
            print(f"[openalex] {oa.key_status()}  run budget={self.oas.budget} attempts  breaker={self.oas.breaker}")
        print()
        self.prior = read_prior(p["parsed"])
        seeds = [Seed(pdf, i < self.walk_n) for i, pdf in enumerate(self.pdfs)]
        for sd in seeds:
            if sd.walked and "regex" in self.srcs:
                _local_leg(sd, self.tdir)
            sd.doi = seed_doi(sd.pdf, text=sd.text, read_pdf=sd.walked and bool(self.network))
            sd.text = None
        self._network(seeds)
        rows, raw = self._assemble(seeds)
        for i, sd in enumerate(seeds[:self.walk_n], 1):
            print(f"  [{i:>4}/{len(self.pdfs)}] {sd.pdf.name[:50]:<50} {self._label(sd)}")
        return self._finish(seeds, rows, raw)

    def _prior_rows(self, sd):
        return (self.prior or {}).get(sd.pdf.name) or []

    def _network(self, seeds):
        todo = []
        for sd in seeds:
            if not sd.walked:
                continue
            if not self.network:
                sd.net_state = "disabled"
            elif not sd.doi:
                sd.net_state = "no_doi"
            else:
                todo.append(sd)
        if not todo:
            return
        if not self.refresh:
            st, now = _state(self.state), net.CLOCK.time()
            for sd in todo:
                if _within_cadence(st, self.paths["parsed"], sd, self._prior_rows(sd), self.days, now):
                    sd.net_state = "cadence"
        todo = [sd for sd in todo if not sd.net_state]
        cad = sum(1 for sd in seeds if sd.net_state == "cadence")
        if cad:
            print(f"[cadence] {cad} seed(s) walked within {self.days} days with an unchanged PDF: not requested")
        open_ = list(todo)
        if self.sess is not None and open_:
            print(f"[s2] references for {len(open_)} seed(s) ...", flush=True)
            open_ = self._s2(open_)
        if open_ and self.oas is not None:
            print(f"[openalex] {len(open_)} seed(s) without an S2 list ...", flush=True)
        if open_:
            open_ = self._openalex(open_)
        if open_ and "crossref" in self.srcs:
            print(f"[crossref] {len(open_)} seed(s) still without a list ...", flush=True)
        if open_:
            open_ = self._crossref(open_)
        for sd in todo:
            self._settle(sd)

    def _s2(self, seeds):
        dois = list(dict.fromkeys(sd.doi for sd in seeds))
        walks = s2.batch_nested(dois, "references", S2_REF_FIELDS, session=self.sess)
        nxt = []
        for sd in seeds:
            w = walks.get(sd.doi)
            if w is None or w.failed:
                kind = str(w.kind or Kind.ERROR) if w is not None else str(Kind.ERROR)
                reason = w.reason if w is not None else "no walk returned"
                if (w is not None and self.sess.aborted and kind == str(Kind.DEFERRED) and not w.attempts):
                    sd.net_state = "not_walked"
                else:
                    sd.net_state, sd.fail_kind, sd.fail_reason = "failed", kind, f"s2: {reason}"
                sd.legs["s2"] = f"FAILED {kind}"
                continue
            sd.definitive["s2"] = True
            sd.legs["s2"] = str(w.state)
            if w.count_at_walk:
                sd.net_count = w.count_at_walk
            if w.rows:                                     # complete or capped_9999
                sd.net_state, sd.net_source = "answered", "s2"
                sd.net_rows = [s2_row(p) for p in w.rows]
                sd.legs["s2"] = f"{w.state} {len(w.rows)}"
                continue
            nxt.append(sd)                                 # elided, empty (count 0) or not_found
        return nxt

    def _openalex(self, seeds):
        if self.oas is None:
            for sd in seeds:
                sd.legs["openalex"] = "disabled"
            return seeds
        if not oa.key_present():
            print(f"[openalex] {oa.KEY_ENV} is not set: the OpenAlex leg is skipped "
                  f"({len(seeds)} seed(s) fall through to Crossref, then the regex)")
            for sd in seeds:
                sd.legs["openalex"] = "skipped (no key)"
            return seeds
        res = oa.referenced_works_many(list(dict.fromkeys(sd.doi for sd in seeds)), session=self.oas)
        nxt = []
        for sd in seeds:
            out = res[sd.doi]
            if out.ok and out.payload.works:
                sd.definitive["openalex"] = True
                sd.net_state, sd.net_source = "answered", "openalex"
                sd.net_rows = [openalex_row(w) for w in out.payload.works]
                sd.net_count = sd.net_count or out.payload.count
                sd.legs["openalex"] = f"{len(out.payload.works)} works ({len(out.payload.dangling)} dangling)"
                continue
            if out.ok or out.kind in (Kind.NOT_AVAILABLE, Kind.NO_MATCH):
                sd.definitive["openalex"] = True
                sd.legs["openalex"] = ("no resolvable works" if out.ok else
                                       "count 0 (unknown)" if out.kind is Kind.NOT_AVAILABLE else "not in OpenAlex")
            elif out.kind is Kind.SKIPPED:
                sd.legs["openalex"] = "skipped"
            else:
                sd.legs["openalex"] = f"FAILED {out.kind}"
                sd.fail_kind = sd.fail_kind or str(out.kind)
                sd.fail_reason = sd.fail_reason or f"openalex: {out.detail}"
                if out.kind is Kind.CONFIG:
                    self.config_error = f"openalex: {out.detail or 'CONFIG'}"
            nxt.append(sd)
        return nxt

    def _crossref(self, seeds):
        if "crossref" not in self.srcs:
            for sd in seeds:
                sd.legs["crossref"] = "disabled"
            return seeds
        res = crossref_references(list(dict.fromkeys(sd.doi for sd in seeds)), cfg=self.registry,
                                  state=self.state, leg=self.crleg)
        nxt = []
        for sd in seeds:
            out = res[sd.doi]
            if out.ok:
                sd.definitive["crossref"] = True
                sd.net_state, sd.net_source = "answered", "crossref"
                sd.net_rows = [crossref_row(e) for e in out.payload]
                sd.legs["crossref"] = f"{len(out.payload)} refs"
                continue
            if out.kind in (Kind.NOT_AVAILABLE, Kind.NO_MATCH):
                sd.definitive["crossref"] = True
                sd.legs["crossref"] = "no deposit" if out.kind is Kind.NOT_AVAILABLE else "not in Crossref"
            else:
                sd.legs["crossref"] = f"FAILED {out.kind}"
                sd.fail_kind = sd.fail_kind or str(out.kind)
                sd.fail_reason = sd.fail_reason or f"crossref: {out.detail}"
            nxt.append(sd)
        return nxt

    def _settle(self, sd):
        """The seed's network verdict once every leg has spoken."""
        if sd.net_state in ("failed", "not_walked"):
            return
        prior_src = [r["source"] for r in self._prior_rows(sd) if r.get("source") in NETWORK_SOURCES]
        best = min(prior_src, key=_RANK.get) if prior_src else None
        if sd.net_state == "answered":
            if best and _RANK[best] < _RANK[sd.net_source] and not sd.definitive.get(best):
                sd.net_state = "kept"                    # never downgrade to a lower source
            return
        if sd.fail_kind:
            sd.net_state = "failed"
        elif all(sd.definitive.get(s) for s in NETWORK_SOURCES):
            sd.net_state = "api_empty"                   # every source answered: nothing to list
        else:
            sd.net_state = "skipped"                     # a source was skipped or disabled: keep

    # ------------------------------------------------------------------ the union
    def _assemble(self, seeds):
        rows, raw = [], []
        for sd in seeds:
            prior = self._prior_rows(sd)
            legacy = self._legacy_source(sd) if any(not r.get("source") for r in prior) else ""
            if not sd.walked:
                for r in prior:
                    rows.append({**r, "seed_doi": r.get("seed_doi") or sd.doi, "source": r.get("source") or legacy})
                continue
            prior_net = [r for r in prior if r.get("source") in NETWORK_SOURCES]
            prior_local = [{**r, "source": r.get("source") or legacy}
                           for r in prior if r.get("source") not in NETWORK_SOURCES]
            if sd.net_state == "answered":
                base = sd.net_rows
            elif sd.net_state in ("failed", "skipped", "cadence", "kept", "not_walked", "disabled"):
                base = prior_net
            else:                                          # api_empty, no_doi
                base = []
            local = sd.local_rows if sd.local_rows is not None else prior_local
            kept, dropped = union(base, local, sd.doi)
            if sd.n_printed and sd.net_count and sd.local_label in ("text", "pdf") \
                    and sd.n_printed / sd.net_count < PARSE_SUSPECT_RATIO:
                sd.parse_suspect = True
            for r in kept:
                rows.append({**{k: r.get(k, "") for k in LEGACY_FIELDS}, "seed": sd.pdf.name,
                             "seed_doi": sd.doi, "source": r["source"]})
            for r, k in [(r, True) for r in kept] + [(r, False) for r in dropped]:
                raw.append({"seed": sd.pdf.name, "raw": (r.get("raw") or "")[:1000], "doi": r.get("doi") or "",
                            "seed_doi": sd.doi, "source": r.get("source") or "", "kept": k})
        return rows, raw

    def _legacy_source(self, sd):
        """A row of a pre-W3-B CSV came from the sidecar when the PDF's sidecar has references."""
        return "sidecar" if text_from_sidecar_refs(lit_util.companion_path(sd.pdf, ".fulltext.json")) else "regex"

    # ------------------------------------------------------------------ verdict and outputs
    def _label(self, sd) -> str:
        legs = " ".join(f"{k}={v}" for k, v in sd.legs.items())
        loc = f"{sd.local_label} {sd.n_printed}" if sd.local_label else ""
        if sd.net_state == "answered":
            head = f"{sd.net_source} {len(sd.net_rows)}"
        elif sd.net_state == "failed":
            head = f"FAILED {sd.fail_kind}"
        elif sd.net_state == "disabled":
            head = ""
        else:
            head = sd.net_state.upper() if sd.net_state == "no_doi" else sd.net_state
        tag = " PARSE_SUSPECT" if sd.parse_suspect else ""
        return " | ".join(x for x in (head, loc, legs) if x) + tag

    def _finish(self, seeds, rows, raw) -> dict:
        p = self.paths
        walked = seeds[:self.walk_n]
        net_walked = [sd for sd in walked if sd.net_state not in ("", "disabled", "no_doi", "cadence")]
        failed = [sd for sd in walked if sd.net_state == "failed"]
        transport = [sd for sd in failed if sd.fail_kind in TRANSPORT_KINDS]
        on_disk = {pdf.name for pdf in self.pdfs}
        with_refs = len({r["seed"] for r in rows if r.get("doi")})
        prior_with = (len({s for s, rs in self.prior.items() if s in on_disk and any(r.get("doi") for r in rs)})
                      if self.prior is not None else None)
        aborted = [f"{name}:{a}" for name, a in (("s2", self.sess.aborted if self.sess else None),
                                                  ("openalex", self.oas.aborted if self.oas else None),
                                                  ("crossref", self.crleg.aborted)) if a]
        if self.config_error:
            aborted = [a for a in aborted if not a.startswith("openalex:")] + ["openalex:config"]
        reasons = []
        if self.config_error:
            reasons.append(f"configuration: {self.config_error}")
        if aborted and not self.config_error:
            n_not = sum(1 for sd in walked if sd.net_state == "not_walked")
            reasons.append(f"aborted ({', '.join(aborted)}); {n_not} seed(s) not walked")
        if len(failed) > FAIL_THRESHOLD * len(net_walked):
            reasons.append(f"{len(failed)} of {len(net_walked)} seed walks failed (over 5 %)")
        if prior_with is not None and with_refs < prior_with and not self.force:
            reasons.append(f"{with_refs} seeds with references, fewer than the published {prior_with}")
        code = (EXIT_ERROR if self.config_error else EXIT_ABORTED if aborted
                else EXIT_DEGRADED if reasons else EXIT_OK)

        p["parsed"].parent.mkdir(parents=True, exist_ok=True)
        dois = sorted({r["doi"] for r in rows if r.get("doi")})
        if code == EXIT_OK:
            lit_util.atomic_write_text(str(p["jsonl"]), "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in raw))
            lit_util.atomic_write_text(str(p["parsed"]), _csv_text(rows, FIELDS))
            buf = io.StringIO()
            w = csv.writer(buf); w.writerow(["doi"])
            for d in dois: w.writerow([d])
            lit_util.atomic_write_text(str(p["unique"]), buf.getvalue())
            self._stamp(walked)
            written = str(p["parsed"])
        else:
            lit_util.atomic_write_text(str(p["degraded"]), _csv_text(rows, FIELDS))
            written = str(p["degraded"])

        count = lambda st: sum(1 for sd in walked if sd.net_state == st)  # noqa: E731
        res = {
            "step": "reverse_citations", "exit_code": code,
            "status": ("error" if code == EXIT_ERROR else "aborted" if code == EXIT_ABORTED
                       else "degraded" if code == EXIT_DEGRADED else "ok"),
            "reasons": reasons, "aborted": ", ".join(aborted) or None,
            "transport_failures": len(transport),
            "sources": [s for s in SOURCES if s in self.srcs],
            "seeds": len(self.pdfs), "walked_pdfs": self.walk_n,
            "with_doi": sum(1 for sd in walked if sd.doi), "no_doi": sum(1 for sd in walked if not sd.doi),
            "network_walks": len(net_walked), "cadence_skipped": count("cadence"),
            "answered": {s: sum(1 for sd in walked if sd.net_state == "answered" and sd.net_source == s)
                         for s in self.network},
            "api_empty": count("api_empty"), "kept": count("kept"), "skipped": count("skipped"),
            "failed": len(failed), "not_walked": count("not_walked"),
            "local": {k: sum(1 for sd in walked if sd.local_label == k)
                      for k in ("sidecar", "text", "pdf", "NO_TEXT", "NO_REFS_SECTION")},
            "printed_refs": sum(sd.n_printed or 0 for sd in walked),
            "parse_suspect": sum(1 for sd in walked if sd.parse_suspect),
            "rows": len(rows), "unique_dois": len(dois),
            "rows_by_source": {s: sum(1 for r in rows if r["source"] == s) for s in ROW_SOURCES},
            "seeds_with_refs": with_refs, "prior_seeds_with_refs": prior_with,
            "written": written, "published": code == EXIT_OK,
            "s2": self.sess.summary() if self.sess else None,
            "openalex": self.oas.summary() if self.oas else None,
            "crossref": self.crleg.summary() if "crossref" in self.srcs else None,
        }
        self._print_summary(res)
        return res

    def _stamp(self, walked):
        fresh = [sd for sd in walked if sd.net_state == "answered" and sd.net_source in ("s2", "openalex")]
        if not fresh:
            return
        st, now = _state(self.state), net.CLOCK.time()
        for sd in fresh:
            size, mtime = _fingerprint(sd.pdf)
            st.kv_set(CADENCE_NS, _stamp_key(self.paths["parsed"], sd.pdf),
                      {"doi": sd.doi, "walked_at": now, "size": size, "mtime": mtime, "source": sd.net_source})

    def _print_summary(self, res):
        print()
        print("=== summary ===")
        print(f"  PDFs:                  {res['seeds']}" + (f" ({res['walked_pdfs']} walked)"
                                                            if res["walked_pdfs"] != res["seeds"] else ""))
        for k, v in res["local"].items():
            print(f"  {k:<22} {v}")
        print(f"  printed references:    {res['printed_refs']} (parse suspect: {res['parse_suspect']})")
        if self.network:
            print(f"  network walks:         {res['network_walks']} (cadence kept {res['cadence_skipped']}, "
                  f"no DOI {res['no_doi']})")
            print(f"  answered:              " + ", ".join(f"{k} {v}" for k, v in res["answered"].items()))
            print(f"  failed:                {res['failed']} (transport {res['transport_failures']}); "
                  f"api empty {res['api_empty']}; kept {res['kept']}; skipped {res['skipped']}")
        print(f"  rows:                  {res['rows']} " + str({k: v for k, v in res['rows_by_source'].items() if v}))
        print(f"  unique DOIs:           {res['unique_dois']}")
        print(f"  seeds with references: {res['seeds_with_refs']}"
              + (f" (published: {res['prior_seeds_with_refs']})" if res["prior_seeds_with_refs"] is not None else ""))
        if res["published"]:
            print(f"  raw:                   {self.paths['jsonl']}")
            print(f"  parsed:                {self.paths['parsed']}")
            print(f"  unique DOIs:           {self.paths['unique']}")
        else:
            status = {EXIT_ERROR: "ERROR", EXIT_ABORTED: "ABORTED"}.get(res["exit_code"], "DEGRADED")
            print(f"  [{status}] {'; '.join(res['reasons'])}")
            print(f"  NOT published: the outputs are left as they were; this run's rows are in {res['written']}")
            if any("fewer than the published" in r for r in res["reasons"]):
                print("  if the drop is real, rerun with --force to publish it")
        if self.sess is not None:
            print(f"  {self.sess.summary_line()}")
        if self.oas is not None:
            print(f"  {self.oas.summary_line()}")
        small = {k: v for k, v in res.items() if k not in ("s2", "openalex", "crossref")}
        for k in ("s2", "openalex"):
            if res[k]:
                small[k] = {x: res[k].get(x) for x in ("calls", "attempts", "attempts_not_ok", "budget", "aborted")}
        if res["crossref"]:
            small["crossref"] = res["crossref"]
        print(SUMMARY_MARKER + json.dumps(small, ensure_ascii=False), flush=True)


def _final(r: dict, seed_doi: str):
    """(row, is_self): the row with its DOI normalised by litpipe.doi (an arXiv ID becomes
    10.48550/arxiv.<id> when the row has no DOI), and whether that DOI, or any DOI candidate of a
    regex chunk, is the seed's own (then the row's DOI is blanked). A DOI from a structured source
    keeps its whole registered form (normalise_structured); a regex DOI takes the text normaliser."""
    raw_doi = r.get("doi") or ""
    norm = (_doi.normalise_structured if (r.get("source") or "") in STRUCTURED_ROW_SOURCES
            else _doi.normalise)
    d = norm(raw_doi) if raw_doi.strip() else None
    if not d:
        a = arxiv_doi(r.get("_arxiv") or r.get("raw") or "")
        d = _doi.normalise(a) if a else None
    d = d or ""
    is_self = bool(seed_doi) and (d == seed_doi or seed_doi in (r.get("_cands") or ()))
    return {**r, "doi": "" if is_self else d}, is_self


def union(base, local, seed_doi):
    """(kept rows, dropped local rows) for one seed. `base` are the network rows: each DOI kept
    once, a row naming the seed itself dropped. Local rows then add only DOIs the base lacks (a
    regex chunk any of whose DOI candidates is already in the base is the same reference); when
    the base is empty they are the last resort and every one is kept, the seed's own DOI blanked (a
    row without a DOI still counts a printed reference)."""
    kept, dropped, seen = [], [], set()
    for r in base or ():
        r, is_self = _final(r, seed_doi)
        if is_self:
            continue
        if r["doi"]:
            if r["doi"] in seen:
                continue
            seen.add(r["doi"])
        kept.append(r)
    have_base = bool(kept)
    for r in local or ():
        r, _ = _final(r, seed_doi)
        cands = set(r.get("_cands") or ()) | ({r["doi"]} if r["doi"] else set())
        if have_base and (not r["doi"] or cands & seen):
            dropped.append(r)
            continue
        if r["doi"]:
            if r["doi"] in seen:
                dropped.append(r)
                continue
            seen.add(r["doi"])
        kept.append(r)
    return kept, dropped


def _csv_text(rows, fields) -> str:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
    w.writeheader(); w.writerows(rows)
    return buf.getvalue()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    ap.add_argument("--project",  default=None,
                     help="Project name from projects.json (e.g. 'research_a').")
    ap.add_argument("--lib-dir",  default=None,
                     help="Explicit library path (legacy).")
    ap.add_argument("--text-dir", default=None,
                     help="Tier 1 text-dump dir (default: <data_dir>/text from projects.json).")
    ap.add_argument("--out-prefix", default=None,
                     help="Output file prefix (default: <lib>/_reverse_citations).")
    ap.add_argument("--limit", type=int, default=0,
                     help="Walk the first N PDFs only (testing); the others keep their published rows.")
    ap.add_argument("--sources", default=None,
                     help="Comma-separated legs: s2,openalex,crossref,regex (default all). "
                          "'--sources regex' sends nothing (the local parse only).")
    ap.add_argument("--force", action="store_true",
                     help="Publish even when fewer seeds have references than in the published CSV.")
    ap.add_argument("--refresh", action="store_true",
                     help="Ignore walk_cadence_days: request every seed again.")
    args = ap.parse_args(argv)
    res = run(project=args.project, lib_dir=args.lib_dir, text_dir=args.text_dir, out_prefix=args.out_prefix,
              limit=args.limit, sources=args.sources, force=args.force, refresh=args.refresh)
    return res["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
