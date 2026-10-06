"""Forward-citation walker (Semantic Scholar through litpipe.s2; OpenAlex through litpipe.openalex).

Finds the papers that CITE a project's seeds. Library mode seeds on the library; scoped mode
(DEC-30) seeds on DOI lists such as a chapter's reference list.

Seeds (library mode):
  - every PDF: its DOI from the `.ris` (the structured DO line), then the `.fulltext.json` `doi`,
    then the PDF text (the only seed taking the free-text DOI rule);
  - every text-only holding (DEC-08): a `.fulltext.json` with no PDF beside it that
    audit_portfolio.is_text_only_sidecar accepts, enumerated as index_portfolio does; its DOI from
    the same-stem `.ris`, else its `doi`; its `seed_pdf` is the sidecar's file name;
  - never an identity-flagged file (audit_portfolio.identity_flag on `.identity.json` or the
    sidecar): a review item, not a holding;
  - a placeholder DOI (litpipe.doi.is_placeholder, e.g. 10.1145/nnnnnnn.nnnnnnn) in the `.ris` or
    the sidecar is never walked and never falls through to the PDF text (which could pick up a cited
    DOI); it is counted as `placeholder`, apart from `no_doi`, and its published rows are left out of
    the "fewer seeds with citers" comparison;
  - a DOI held by two files is walked once; rows are written for each file, PDFs first.

The walk (litpipe.walk holds the cache, the routing and the planner):
  1. One metadata pass, POST /paper/batch per 500 seeds (paperId, citationCount, externalIds, year).
  2. The count gate: a seed is walked when it is new to the cache, its citationCount differs from
     count_at_walk, or its state is failed, unresolved, not_found or elided; `--refresh` walks every
     seed. Seeds the journal answered in this run skip the metadata pass and the walk.
  3. Routing: citationCount 0 is empty (no call); <= 1,000 nested batch (one POST per bin of <= 9,000
     citations and <= 500 ids); <= 9,999 paged GET (1,000 a page, never offset+limit >= 10000);
     above 9,999 OpenAlex `cites:` when OPENALEX_API_KEY is set (every citer, source openalex), else
     (or when OpenAlex does not hold the DOI, or its session stopped) S2 year windows (state
     capped_9999, the unreachable estimate recorded). A count mismatch from drift during the run is
     re-checked once.
  4. Each answered seed is written to the cache (<state_dir>/s2_cache.duckdb), replacing its rows
     for that source in one transaction, then appended to `<report stem>.partial.jsonl`.
  5. The report is regenerated from the cache for this library's seeds; a seed the cache cannot
     answer keeps the rows the published report holds for it.
DOI aliases (a seed S2 knows under another DOI, or two seeds on one S2 paperId) are reported, and one
paperId is never walked twice in a run.

Degraded-walk guard (K3), unchanged: a failed call is FAILED, never "0 citers"; a failed seed keeps
its rows; the report is replaced only on a clean run, otherwise `<report stem>.degraded.csv` is
written and the report is left byte-identical. The journal is the in-run resume record (a killed or
aborted run resumes from it; `--restart` discards it); the cache is the durable store across runs.

Scoped mode (`--seeds-from LIST [LIST ...] --scope NAME` with --project or --lib-dir naming the
output library): DOI lists filtered as the consumer's chapter walkers do (a valid DOI and, when the
columns exist, `status == HIGH` and not `bookish`); the first list naming a DOI claims its seed_label
and seed_chapter (a `chapter` column, else `LIST::LABEL`, else the list's file stem); seed_n_cites is
the largest `n_cites` over the lists. Outputs `_<scope>_forward_citations.csv` (the consumer's header
plus source, citing_oa, seed_n_cites) under the same publish rule, and `_<scope>_descendants.csv`:
one row per citing DOI not held anywhere in the portfolio, ranked by (-n_seeds, OA first, -cited_by,
-year), with the tier counts printed. A scoped run never writes `_forward_citations.csv`, and never
replaces a scoped CSV written outside the pipeline (no `source` column) unless --force.

`--source openalex` walks every seed through OpenAlex `cites:` on one OpenAlex session (the S2
metadata pass still feeds the count gate); a 429 or Remaining 0 stops it (exit 3), never retried.

Spacing comes from the clients: S2 6.5 s unkeyed, 1.1 s with S2_API_KEY. `--sleep` is ignored and
`--s2-key` is deprecated. Every run prints the planner's call count after the metadata pass.

Exit codes:
  0  clean: the report was written.
  2  degraded: more than 5 % of seed walks failed, or the result has fewer seeds with citers than
     the published report (`--force` accepts that one). Nothing published.
  3  aborted: an S2 run budget or breaker stop, an OpenAlex stop under --source openalex, or the
     cache held by another process. Nothing published; rerun the same command to resume.
  1  usage or configuration error.
The last stdout line is "[step-summary] {json}" for snowball.py.

Usage:
  python forward_citations.py --project research_a
  python forward_citations.py --lib-dir /path/to/library
  python forward_citations.py --project research_a --limit 5          # first 5 seeds only
  python forward_citations.py --project teaching_a --seeds-from ch09_refs.csv ch10_refs.csv::Ch10 --scope resp

Outputs (default location: <lib-dir>/_forward_citations.csv):
  Columns: seed_pdf, seed_doi, citing_paper_id, citing_doi, citing_title, citing_year,
           citing_authors, citing_venue, citing_cited_by, citing_abstract, source, citing_oa
  (citing_abstract stays for compatibility and is empty for new rows: the lean field set.)
"""
import argparse
import contextlib
import csv
import dataclasses
import json
import os
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

import lit_util
from litpipe import config, ledger, openalex, s2, walk
from litpipe import doi as _doi
from litpipe.outcomes import Kind, Outcome

lit_util.utf8_stdout()

CONFIG_PATH = Path(__file__).parent / "projects.json"

FIELDS = ["seed_pdf", "seed_doi", "citing_paper_id", "citing_doi", "citing_title",
          "citing_year", "citing_authors", "citing_venue", "citing_cited_by",
          "citing_abstract", "source", "citing_oa"]
# The consumer's scoped header, then this walker's columns (seed_n_cites last).
SCOPED_FIELDS = ["seed_doi", "seed_label", "seed_chapter", "citing_paper_id", "citing_doi",
                 "citing_title", "citing_year", "citing_authors", "citing_venue", "citing_cited_by",
                 "citing_abstract", "source", "citing_oa", "seed_n_cites"]
DESCENDANT_FIELDS = ["rank", "doi", "title", "authors", "year", "venue", "cited_by", "oa", "n_seeds",
                     "chapters", "seeds", "seed_dois", "sources", "seed_n_cites"]
_CITING = ["citing_paper_id", "citing_doi", "citing_title", "citing_year", "citing_authors",
           "citing_venue", "citing_cited_by", "citing_abstract", "source", "citing_oa"]
# The citing-paper fields (design 4.1 step 5: the lean set, no abstract) and the metadata pass.
CITING_FIELDS = walk.CITATION_FIELDS
RESOLVE_FIELDS = walk.META_FIELDS

SCOPE_RE = re.compile(r"^[a-z0-9][a-z0-9_-]*$")   # index_portfolio.SCOPED_FORWARD_RE's stem
FAIL_THRESHOLD = 0.05            # more than 5 % of seed walks failed: degraded (exit 2)
JOURNAL_VERSION = 1
SUMMARY_MARKER = "[step-summary] "
# Failure kinds that are the network's doing (snowball reads any of these as DEGRADED).
TRANSPORT_KINDS = frozenset({"TRANSPORT", "OUTAGE", "REFUSED", "DEFERRED"})
EXIT_OK, EXIT_ERROR, EXIT_DEGRADED, EXIT_ABORTED = 0, 1, 2, 3
CACHE_LOCKED = "cache locked"


class ProjectError(config.ConfigError):
    """The library cannot be located (unregistered project, missing directory)."""


class SeedListError(ValueError):
    """A --seeds-from list cannot be read."""


# ------------------------------------------------------------------------------ seed DOIs
def _gate_doi(doi: str) -> str:
    """RC1: normalize then drop malformed/suspicious (truncated) DOIs (kept for callers)."""
    d = lit_util.normalize_doi(doi)
    if not lit_util.is_valid_doi(d) or lit_util.is_suspicious_doi(d):
        return ""
    return d


def structured_doi(raw) -> tuple:
    """(doi, why) of a DOI from a structured field (a .ris DO line, a sidecar `doi`, a seed list):
    litpipe.doi.normalise_structured keeps the whole registered form. why is "" for a usable DOI,
    "placeholder" for a template or truncated DOI (litpipe.doi.is_placeholder), "no_doi" otherwise."""
    if not isinstance(raw, str) or not raw.strip():
        return "", "no_doi"
    d = _doi.normalise_structured(raw)
    if d and lit_util.is_valid_doi(d) and not lit_util.is_suspicious_doi(d):
        return d, ""
    rejected = []
    _doi.candidates(raw, rejected)
    if any(r[2] == "placeholder" for r in rejected) or (d and lit_util.is_valid_doi(d)):
        return "", "placeholder"
    return "", "no_doi"


def is_placeholder_doi(d) -> bool:
    """A well-formed DOI that is a template or a truncation (the rows the guard leaves out)."""
    d = (d or "").strip().lower()
    return bool(d) and lit_util.is_valid_doi(d) and _doi.is_placeholder(d)


_RIS_DO = re.compile(r"^DO\s{2}-\s?(.+)$")


def ris_doi(ris_path: Path) -> tuple:
    """(doi, why) of a .ris file's first DO line ("" and "no_doi" when absent or unreadable)."""
    try:
        with open(ris_path, encoding="utf-8") as f:
            for line in f:
                m = _RIS_DO.match(line)
                if m:
                    return structured_doi(m.group(1))
    except (OSError, UnicodeDecodeError):
        pass
    return "", "no_doi"


def doi_from_ris(ris_path: Path) -> str:
    """The structured DOI of a .ris file, "" when absent, malformed or a placeholder (snowball's
    library fingerprint imports this)."""
    if not Path(ris_path).exists():
        return ""
    return ris_doi(ris_path)[0]


def _sidecar_record(sc_path: Path):
    try:
        with open(sc_path, encoding="utf-8") as f:
            d = json.load(f)
    except (OSError, ValueError):
        return None
    return d if isinstance(d, dict) else None


def doi_from_sidecar(sc_path: Path) -> str:
    d = _sidecar_record(sc_path) if Path(sc_path).exists() else None
    return structured_doi(d.get("doi") or "")[0] if d else ""


def doi_from_pdf(pdf_path: Path, max_chars=5000, rejected=None) -> str:
    """The first DOI in a PDF's first pages (the free-text rule), "" when none. When nothing usable is
    found and `rejected` is a list, it receives litpipe.doi's (position, form, reason) for each form
    dropped, so a caller can tell a placeholder from no DOI at all."""
    try:
        import fitz
    except ImportError:
        return ""
    text = ""
    try:
        doc = fitz.open(str(pdf_path))
        try:
            for p in doc:
                text += p.get_text()
                if len(text) >= max_chars: break
        finally:
            doc.close()
    except Exception: return ""
    # RC1: re-joins line-wrapped DOIs and drops truncated/suspicious ones (free text).
    d = lit_util.extract_doi_from_text(text, max_chars=max_chars)
    if not d and rejected is not None:
        _doi.candidates(text[:max_chars], rejected)
    return d


def get_doi(pdf: Path) -> str:
    """A PDF's seed DOI: the .ris, then the sidecar, then the PDF text (no text fallback after a
    placeholder in the .ris or the sidecar)."""
    d, why = ris_doi(lit_util.companion_path(pdf, ".ris"))
    if d:
        return d
    rec = _sidecar_record(lit_util.companion_path(pdf, ".fulltext.json"))
    d2, why2 = structured_doi(rec.get("doi") or "" if rec else "")
    if d2:
        return d2
    if "placeholder" in (why, why2):
        return ""
    return doi_from_pdf(pdf)


# ------------------------------------------------------------------------------ seeds
@dataclasses.dataclass
class Seed:
    file: str                    # seed_pdf: the PDF or text-only sidecar ("" for a list seed)
    doi: str
    why: str = ""                # "" | "no_doi" | "placeholder"
    kind: str = "pdf"            # "pdf" | "text_only" | "list"
    label: str = ""
    chapter: str = ""
    n_cites: int | None = None


def library_seeds(lib: Path):
    """(seeds, stats) of a library: PDFs (sorted), then text-only holdings (sorted), never an
    identity-flagged file. Files are paired by case-folded stem exactly as index_portfolio does
    (scan_library_files; a read-only import). Raises index_portfolio.LibraryUnreadable."""
    import audit_portfolio
    import index_portfolio
    pdfs, sidecars, ris, idents = index_portfolio.scan_library_files(lib)
    flags = {}
    for stem, name in idents.items():
        d, _err = audit_portfolio.read_json(lib / name)
        if d is not None and audit_portfolio.identity_flag(d):
            flags[stem] = name
    sc_data = {}
    for stem, name in sidecars.items():
        d, _err = audit_portfolio.read_json(lib / name)
        sc_data[stem] = d
        if d is not None and audit_portfolio.identity_flag(d) and stem not in flags:
            flags[stem] = name
    seeds, flagged = [], []
    for stem, name in sorted(pdfs.items(), key=lambda kv: lib / kv[1]):
        if stem in flags:
            flagged.append(name)
            continue
        d, why = ris_doi(lib / ris[stem]) if stem in ris else ("", "no_doi")
        if not d and stem in sidecars:
            rec = sc_data.get(stem)
            d2, why2 = structured_doi(rec.get("doi") or "" if rec else "")
            d, why = (d2, "") if d2 else (d, "placeholder" if "placeholder" in (why, why2) else why2)
        if not d and why != "placeholder":
            rejected = []
            d = doi_from_pdf(lib / name, rejected=rejected)
            why = "" if d else ("placeholder" if any(r[2] == "placeholder" for r in rejected) else "no_doi")
        seeds.append(Seed(name, d, why, "pdf"))
    n_text, n_flagged_pdfs = 0, len(flagged)
    for stem, name in sorted(sidecars.items(), key=lambda kv: lib / kv[1]):
        if stem in pdfs:
            continue
        if stem in flags:
            flagged.append(name)                      # a flagged text-only record: a review item
            continue
        rec = sc_data.get(stem)
        if rec is None or not audit_portfolio.is_text_only_sidecar(rec):
            continue
        n_text += 1
        d, why = ris_doi(lib / ris[stem]) if stem in ris else ("", "no_doi")
        if not d:
            d2, why2 = structured_doi(rec.get("doi") or "")
            d, why = (d2, "") if d2 else (d, "placeholder" if "placeholder" in (why, why2) else why2)
        seeds.append(Seed(name, d, why, "text_only"))
    return seeds, {"pdfs": len(pdfs) - n_flagged_pdfs, "text_only": n_text, "flagged": len(flagged),
                   "flagged_files": flagged}


def _split_spec(spec):
    """(path, label) of a --seeds-from value: PATH or PATH::LABEL, split on the LAST '::' so a
    Windows drive letter survives."""
    s = str(spec)
    path, sep, label = s.rpartition("::")
    return (path, label.strip()) if sep and path else (s, "")


def _truthy(v) -> bool:
    return str(v or "").strip().lower() in ("1", "true", "yes", "y", "t")


def read_seed_lists(specs):
    """(seeds, stats) from DOI lists (CSV with a `doi` column; '#' comment lines skipped). The
    filter is the consumer's chapter-walker filter: a valid DOI and, when the columns exist,
    status == HIGH and not bookish. The first list naming a DOI claims its label and chapter;
    n_cites is the largest over the lists. Raises SeedListError."""
    seeds: dict = {}
    dropped: Counter = Counter()
    per_list = []
    for spec in specs:
        path_s, label = _split_spec(spec)
        p = Path(path_s)
        if not p.is_file():
            raise SeedListError(f"--seeds-from: no such file: {p}")
        try:
            with open(p, encoding="utf-8-sig", newline="") as f:
                lines = [ln for ln in f if not ln.lstrip().startswith("#")]
        except (OSError, UnicodeDecodeError) as e:
            raise SeedListError(f"--seeds-from: cannot read {p}: {type(e).__name__}: {e}") from None
        rd = csv.DictReader(lines)
        cols = [c.strip().lower() for c in rd.fieldnames] if rd.fieldnames else []
        rd.fieldnames = cols
        if "doi" not in cols:
            raise SeedListError(f"--seeds-from: {p.name} has no doi column")
        kept = 0
        for row in rd:
            d, why = structured_doi(row.get("doi") or "")
            if not d:
                dropped["placeholder" if why == "placeholder" else "no_valid_doi"] += 1
                continue
            if "bookish" in cols and _truthy(row.get("bookish")):
                dropped["bookish"] += 1
                continue
            if "status" in cols and str(row.get("status") or "").strip().upper() != "HIGH":
                dropped[f"status_{str(row.get('status') or '').strip() or 'blank'}"] += 1
                continue
            n = lit_util.coerce_int(row.get("n_cites"), None) if "n_cites" in cols else None
            kept += 1
            if d in seeds:
                s = seeds[d]
                if n is not None and (s.n_cites is None or n > s.n_cites):
                    s.n_cites = n
                dropped["duplicate"] += 1
                continue
            first_author = (row.get("authors") or "").split(";")[0].strip()
            lab = ((row.get("seed_label") or row.get("label") or "").strip()
                   or f"{first_author} {(row.get('year') or '').strip()}".strip()
                   or (row.get("title") or "")[:50].strip() or d)
            chapter = (row.get("chapter") or "").strip() if "chapter" in cols else ""
            seeds[d] = Seed("", d, "", "list", lab, chapter or label or p.stem, n)
        per_list.append({"list": p.name, "chapter": label or p.stem, "kept": kept})
    return list(seeds.values()), {"lists": per_list, "dropped": dict(dropped)}


# ------------------------------------------------------------------------------ registry and paths
def load_registry(cfg=None) -> dict:
    """projects.json through litpipe.config.load: `cfg` when given, else this module's CONFIG_PATH.
    ConfigError (exit 1) when the file is missing or unreadable (reverse_citations' pattern)."""
    if cfg is not None:
        return config.load(cfg)
    p = Path(CONFIG_PATH)
    if not p.exists():
        raise config.ConfigError(f"projects.json not found at {p} (copy projects.json.template)")
    try:
        return config.load(lit_util.load_projects_config(p))
    except (OSError, ValueError) as e:
        raise config.ConfigError(f"projects.json at {p} is unreadable: {ledger.redact(e)}") from None


def resolve_project(name: str, registry=None) -> Path:
    """The library directory of a registered project (ProjectError when it is not registered)."""
    projects = (registry if registry is not None else load_registry()).get("projects")
    projects = projects if isinstance(projects, dict) else {}
    if name not in projects:
        raise ProjectError(f"'{name}' not in projects.json")
    p = projects[name] if isinstance(projects[name], dict) else {}
    if not p.get("lib_dir"):
        raise ProjectError(f"projects.json: '{name}' has no lib_dir")
    base = lit_util.PROJECTS_ROOT / (p.get("parent") or name)
    return base / p["lib_dir"]


def degraded_path(out: Path) -> Path:
    return out.with_name(out.stem + ".degraded.csv")


def journal_path(out: Path) -> Path:
    return out.with_name(out.stem + ".partial.jsonl")


def unique_path(out: Path) -> Path:
    return out.with_name(out.stem + "_unique_dois.csv")


def scoped_paths(lib: Path, scope: str):
    return lib / f"_{scope}_forward_citations.csv", lib / f"_{scope}_descendants.csv"


# ------------------------------------------------------------------------------ rows
def citing_row(paper) -> dict:
    """The citing columns of one S2 citing-paper object (blank where S2 gave nothing; a null-paperId
    stub still counts toward citationCount). citing_abstract is empty: the lean field set."""
    r = walk.s2_citing(paper)
    return {**{k: r[k] for k in _CITING if k in r}, "citing_abstract": ""}


def _csv_citing(c: dict, source: str) -> dict:
    """A cache row as report columns."""
    def blank(v):
        return "" if v is None else v
    oa = c.get("citing_oa")
    return {"citing_paper_id": blank(c.get("citing_paper_id")), "citing_doi": blank(c.get("citing_doi")),
            "citing_title": blank(c.get("citing_title")), "citing_year": blank(c.get("citing_year")),
            "citing_authors": blank(c.get("citing_authors")), "citing_venue": blank(c.get("citing_venue")),
            "citing_cited_by": blank(c.get("citing_cited_by")), "citing_abstract": "", "source": source,
            "citing_oa": "" if oa is None else ("true" if oa else "false")}


def _read_rows(path: Path, fields, key_extra=None):
    """{seed_doi: [rows]} of a report, or None when there is none. Rows of one seed come from the
    first seed_pdf (library) listed for it."""
    if not path.exists():
        return None
    by_doi, first = {}, {}
    with open(path, encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f):
            d = (r.get("seed_doi") or "").strip().lower()
            if not d:
                continue
            k = r.get(key_extra) or "" if key_extra else ""
            if first.setdefault(d, k) != k:
                continue
            by_doi.setdefault(d, []).append({c: r.get(c, "") or "" for c in fields})
    return by_doi


def read_report(path: Path):
    """{seed_doi: [rows]} of a published library report, or None when there is none."""
    return _read_rows(path, FIELDS, "seed_pdf")


def read_scoped_report(path: Path):
    return _read_rows(path, SCOPED_FIELDS)


def _csv_header(path: Path) -> list:
    try:
        with open(path, encoding="utf-8-sig", newline="") as f:
            return next(csv.reader(f), [])
    except (OSError, UnicodeDecodeError):
        return []


# ------------------------------------------------------------------------------ journal
class Journal:
    """Per-seed append log, `<report stem>.partial.jsonl`: a header line, then one JSON line per
    finished seed (the last line for a DOI wins). Lines are flushed and fsynced as written, so a
    kill loses at most the seed in flight; a torn last line is dropped on load. A journal written
    for another library, source or scope is not resumed. Rows live in the cache, not here."""

    def __init__(self, path: Path, lib: Path, source="s2", scope=None):
        self.path = path
        self.lib = str(Path(lib).resolve())
        self.source = source
        self.scope = scope
        self.started_at = None
        self._fh = None

    def load(self, restart=False) -> dict:
        """The entries of a resumable journal ({doi: entry}); {} (and a fresh start) otherwise."""
        if restart or not self.path.exists():
            if restart and self.path.exists():
                print(f"[info] --restart: discarding {self.path.name}")
            return {}
        try:
            lines = self.path.read_text(encoding="utf-8").splitlines()
        except OSError as e:
            print(f"[warn] cannot read {self.path.name} ({ledger.redact(e)}); starting over")
            return {}
        header, entries = None, {}
        for i, line in enumerate(lines):
            try:
                obj = json.loads(line)
            except ValueError:
                continue                                   # a torn line from a kill
            if i == 0:
                header = obj
                continue
            if isinstance(obj, dict) and isinstance(obj.get("doi"), str):
                entries[obj["doi"]] = obj
        why = self._foreign(header)
        if why:
            print(f"[info] not resuming from {self.path.name}: {why}; starting over")
            return {}
        self.started_at = header["started_at"]
        return entries

    def _foreign(self, header):
        if not isinstance(header, dict) or header.get("journal") != "forward_citations":
            return "no journal header"
        if header.get("version") != JOURNAL_VERSION:
            return f"journal version {header.get('version')!r}"
        if header.get("lib") != self.lib:
            return "written for another library"
        if header.get("source", "s2") != self.source:
            return f"written for --source {header.get('source')}"
        if header.get("scope") != self.scope:
            return "written for another scope"
        if not isinstance(header.get("started_at"), str):
            return "no start time"
        return None

    def open(self, entries: dict):
        """Rewrite the journal compactly (header and the last entry per DOI), then append to it."""
        self.started_at = self.started_at or ledger.now_iso()
        header = {"journal": "forward_citations", "version": JOURNAL_VERSION, "lib": self.lib,
                  "source": self.source, "scope": self.scope, "started_at": self.started_at}
        text = "".join(json.dumps(o, ensure_ascii=False) + "\n" for o in [header, *entries.values()])
        self.path.parent.mkdir(parents=True, exist_ok=True)
        lit_util.atomic_write_text(str(self.path), text)
        self._fh = open(self.path, "a", encoding="utf-8", newline="\n")

    def append(self, entry: dict):
        self.append_many([entry])

    def append_many(self, entries):
        if not entries:
            return
        self._fh.write("".join(json.dumps(e, ensure_ascii=False) + "\n" for e in entries))
        self._fh.flush()
        os.fsync(self._fh.fileno())

    def close(self):
        if self._fh is not None:
            self._fh.close()
            self._fh = None

    def remove(self):
        self.close()
        with contextlib.suppress(FileNotFoundError):
            self.path.unlink()


def _answered(entry) -> bool:
    return bool(entry) and entry.get("state") != "failed"


def _entry(doi, stage, state, *, source="s2", count=None, expected=None, n_rows=None, paper_id=None,
           kind=None, status=None, reason="", attempts=0, mismatch=False, route=None) -> dict:
    return {"doi": doi, "stage": stage, "state": state, "source": source, "route": route, "count": count,
            "expected": count if expected is None else expected, "n_rows": n_rows, "paper_id": paper_id,
            "kind": kind, "status": status, "reason": ledger.redact(reason or "")[:300],
            "count_mismatch": mismatch, "attempts": attempts, "walked_at": ledger.now_iso()}


def _label(e) -> str:
    st = e["state"]
    if st == "failed":
        return f"FAILED {e['stage']} {e['kind']}: {e['reason'][:70]}"
    if st == "unresolved":
        return "S2_UNRESOLVED (no S2 record)"
    if e.get("n_rows") is None:
        return st.upper()
    tag = {"empty": " (zero citers)", "capped_9999": " (capped at 9,999)"}.get(st, "")
    src = "" if e.get("source", "s2") == "s2" else f" [{e['source']}]"
    return f"{e['n_rows']:>5} citing{tag}{src}"


# ------------------------------------------------------------------------------ key and flags
@contextlib.contextmanager
def _key_env(value):
    """Deprecated --s2-key: put the value in S2_API_KEY for this run only (litpipe.s2 reads
    nothing else), restore the previous environment afterwards, never print it."""
    if not value:
        yield
        return
    print("[deprecated] --s2-key: set the S2_API_KEY environment variable instead (a key on the "
          "command line lands in shell history); using it for this run only")
    old = os.environ.get(s2.KEY_ENV)
    os.environ[s2.KEY_ENV] = value
    try:
        yield
    finally:
        if old is None:
            os.environ.pop(s2.KEY_ENV, None)
        else:
            os.environ[s2.KEY_ENV] = old


# ------------------------------------------------------------------------------ the walk
def run(*, project=None, lib_dir=None, report=None, limit=0, sleep=None, s2_key=None, force=False,
        restart=False, session=None, refresh=False, seeds_from=None, scope=None, source="s2",
        cache_path=None, oa_session=None) -> dict:
    """One forward walk. Returns the summary dict (its `exit_code` is the CLI's exit code).
    `session` / `oa_session` inject the S2 / OpenAlex sessions; `cache_path` overrides the walk
    cache file (else litpipe.walk.CACHE_PATH, else <state_dir>/s2_cache.duckdb)."""
    if sleep is not None:
        print("[deprecated] --sleep is ignored: spacing comes from litpipe.s2 (6.5 s unkeyed, "
              "1.1 s keyed; projects.json \"s2\" block)")
    with _key_env(s2_key):
        return _Run(project=project, lib_dir=lib_dir, report=report, limit=limit, force=force,
                    restart=restart, session=session, refresh=refresh, seeds_from=seeds_from,
                    scope=scope, source=source, cache_path=cache_path, oa_session=oa_session).go()


def _error(msg) -> dict:
    print(f"[ERR] {msg}", file=sys.stderr)
    return {"step": "forward_citations", "exit_code": EXIT_ERROR, "status": "error", "error": msg,
            "reasons": [f"config: {msg}"], "aborted": None, "transport_failures": 0}


class _Run:
    def __init__(self, **kw):
        self.__dict__.update(kw)
        self.scoped = bool(self.seeds_from)
        self.meta = {}             # doi -> (paperId, citationCount, S2 DOI, year)
        self.groups = {}           # primary doi -> [dois on one S2 paperId]
        self.alias_rows = []
        self.plan = None
        self.locked = False
        self.cache = None
        self.s2_meta_failed = 0
        self.kept = 0

    # -- setup
    def go(self) -> dict:
        if self.source not in walk.SOURCES:
            return _error(f"--source must be one of {', '.join(walk.SOURCES)}, got {self.source!r}")
        if bool(self.seeds_from) != bool(self.scope):
            return _error("--seeds-from and --scope go together (with --project or --lib-dir naming the "
                          "output library)")
        if self.scope and not SCOPE_RE.match(self.scope):
            return _error(f"--scope {self.scope!r} must match {SCOPE_RE.pattern} (lower case, digits, '_' or '-')")
        if self.scoped and self.report:
            return _error("--report is for library mode; a scoped run writes _<scope>_forward_citations.csv")
        try:
            if self.project:
                self.reg = load_registry()
                lib = resolve_project(self.project, self.reg)
            elif self.lib_dir:
                self.reg = config.load()
                lib = Path(self.lib_dir)
            else:
                return _error("Pass --project or --lib-dir")
        except config.ConfigError as e:
            return _error(str(e))
        except (OSError, ValueError) as e:
            return _error(f"projects.json: {ledger.redact(e)}")
        if not lib.is_dir():
            return _error(f"not a directory: {lib}")
        self.lib = lib
        try:
            self.cpath = walk.cache_path(self.reg, self.cache_path)
            self.sess = self.session or s2.Session(cfg=self.reg)
            self.osess = self.oa_session or openalex.Session(cfg=self.reg)
        except config.ConfigError as e:
            return _error(f"projects.json: {ledger.redact(e)}")

        if self.scoped:
            self.out, self.desc_out = scoped_paths(lib, self.scope)
            try:
                self.seeds, self.seed_stats = read_seed_lists(self.seeds_from)
            except SeedListError as e:
                return _error(str(e))
            if self.out.exists() and "source" not in _csv_header(self.out) and not self.force:
                return _error(f"{self.out.name} exists and was not written by this pipeline (no source "
                              f"column); it is never replaced. Pass --force to replace it.")
        else:
            self.out = Path(self.report) if self.report else (lib / "_forward_citations.csv")
            import index_portfolio
            try:
                self.seeds, self.seed_stats = library_seeds(lib)
            except index_portfolio.LibraryUnreadable as e:
                return _error(f"library unreadable: {e}")
        n = len(self.seeds)
        self.walk_n = min(self.limit, n) if self.limit else n
        self.first = {}
        for s in self.seeds[:self.walk_n]:
            if s.doi:
                self.first.setdefault(s.doi, s.file or s.label)
        self.walk_dois = list(self.first)
        self._print_header()

        try:
            self.cache = walk.Cache(self.cpath)
        except walk.CacheLocked as e:
            print(f"[abort] {e}")
            return self._locked_result()
        except walk.CacheUnreadable as e:
            return _error(f"walk cache cannot be opened ({e}); move it aside and rerun (the next walk rebuilds it)")
        try:
            return self._walk_and_finish()
        finally:
            self.cache.close()

    def _print_header(self):
        print(f"library:  {self.lib}")
        if self.scoped:
            print(f"scope:    {self.scope}  ({len(self.seeds)} list seeds"
                  + (f", walking the first {self.walk_n}" if self.limit else "") + ")")
            for li in self.seed_stats["lists"]:
                print(f"  {li['list']:<48} {li['kept']:>4} seeds  [{li['chapter']}]")
            if self.seed_stats["dropped"]:
                print(f"  dropped: {self.seed_stats['dropped']}")
        else:
            st = self.seed_stats
            print(f"seeds:    {len(self.seeds)} ({st['pdfs']} PDFs, {st['text_only']} text-only holdings; "
                  f"{st['flagged']} identity-flagged files skipped)"
                  + (f" (walking the first {self.walk_n})" if self.limit else ""))
        print(f"output:   {self.out}")
        print(f"cache:    {self.cpath}")
        print(f"source:   {self.source}  OpenAlex {openalex.key_status()}")
        print(f"{s2.key_status()}  run budget={self.sess.budget} attempts  breaker={self.sess.breaker}\n")

    def _locked_result(self) -> dict:
        code, reasons = verdict(len(self.walk_dois), 0, 0, None, CACHE_LOCKED, False, len(self.walk_dois))
        res = {"step": "forward_citations", "exit_code": code, "status": "aborted", "reasons": reasons,
               "aborted": CACHE_LOCKED, "transport_failures": 0, "seeds": len(self.seeds),
               "seed_walks": len(self.walk_dois), "not_walked": len(self.walk_dois), "written": None,
               "published": False, "cache": str(self.cpath), "s2": self.sess.summary()}
        print(f"  [ABORTED] {'; '.join(reasons)}: another process holds {self.cpath.name}; nothing written")
        _emit_summary(res)
        return res

    def _walk_and_finish(self) -> dict:
        self.prior = read_scoped_report(self.out) if self.scoped else read_report(self.out)
        self.journal = Journal(journal_path(self.out), self.lib, self.source, self.scope)
        wanted = set(self.walk_dois)
        self.entries = {d: e for d, e in self.journal.load(self.restart).items() if d in wanted}
        self.resumed = sum(1 for d in self.walk_dois if _answered(self.entries.get(d)))
        if self.resumed:
            print(f"[resume] {self.journal.path.name} (started {self.journal.started_at}): {self.resumed} "
                  f"answered seed(s) kept, not requested again")
        todo = [d for d in self.walk_dois if not _answered(self.entries.get(d))]
        self.pos = {d: i for i, d in enumerate(self.walk_dois, 1)}
        self.cached = self.cache.states()
        self.journal.open(self.entries)
        try:
            self._walk(todo)
        finally:
            self.journal.close()
        return self._finish()

    # -- recording
    def _oa_ok(self):
        return openalex.key_present() and not self.osess.aborted

    def _print(self, entry):
        d = entry["doi"]
        print(f"  [{self.pos[d]:>4}/{len(self.walk_dois)}] {str(self.first[d])[:50]:<50} {d[:30]:<30} "
              f"{_label(entry)}", flush=True)

    def _append(self, entry):
        self.journal.append(entry)
        self.entries[entry["doi"]] = entry
        self._print(entry)

    def _record(self, primary, res: walk.Result, *, count, route, recount=False):
        """Cache, then journal, every DOI of the primary's group. A cache failure fails the seed;
        a cache held by another process stops the run."""
        pid = self.meta.get(primary, (None,))[0]
        for d in self.groups.get(primary, [primary]):
            state, kind, reason, stage = res.state, res.kind, res.reason, "citations"
            n_rows = len(res.rows) if res.rows is not None else None
            if not self.locked:
                try:
                    self.cache.record(d, res.source, state=res.state, count=count, paper_id=pid, rows=res.rows,
                                      kind=res.kind, reason=res.reason, unreachable=res.unreachable)
                except walk.CacheLocked as e:
                    self.locked = True
                    state, kind, stage, reason = "failed", "CACHE", "cache", f"{CACHE_LOCKED}: {e}"
                except walk.CacheWriteError as e:
                    state, kind, stage, reason = "failed", "CACHE", "cache", str(e)
            else:
                state, kind, stage, reason = "failed", "CACHE", "cache", CACHE_LOCKED
            e = _entry(d, stage, state, source=res.source, count=count, expected=res.expected, n_rows=n_rows,
                       paper_id=pid, kind=kind, status=res.status, reason=reason, attempts=res.attempts,
                       mismatch=res.mismatch and state == "failed" and stage == "citations", route=route)
            if recount:
                e["recount"] = True
            self._append(e)

    def _record_resolve(self, doi, slot, unresolved):
        if unresolved:
            e = _entry(doi, "resolve", "unresolved", kind=str(slot.kind) if isinstance(slot, Outcome) else None,
                       reason=slot.detail if isinstance(slot, Outcome) else "no S2 record (batch null)")
            if not self.locked:
                try:
                    self.cache.record(doi, "s2", state=walk.UNRESOLVED, reason=e["reason"])
                except walk.CacheLocked:
                    self.locked = True
                except walk.CacheWriteError as err:
                    e = _entry(doi, "cache", "failed", kind="CACHE", reason=str(err))
        elif isinstance(slot, Outcome):
            e = _entry(doi, "resolve", "failed", kind=str(slot.kind), status=slot.status, reason=slot.detail,
                       attempts=slot.attempts)
        else:
            e = _entry(doi, "resolve", "failed", kind=str(Kind.ERROR),
                       reason="unexpected S2 response: no citationCount")
        self._append(e)

    def _stopped(self, r) -> bool:
        if self.locked:
            return True
        if self.source == "openalex":
            return bool(self.osess.aborted)
        return bool(self.sess.aborted)

    # -- the walk
    def _walk(self, todo):
        if not todo:
            self.plan = walk.plan({}, n_metadata=0)
            print(walk.plan_line(self.plan) + " (every seed answered in this run's journal)")
            return
        batch = s2.paper_batch(todo, RESOLVE_FIELDS, session=self.sess)
        for doi, slot in zip(todo, batch.payload):
            got = walk.resolved(slot)
            if got is not None:
                self.meta[doi] = got
            elif self.source == "openalex" and not self.osess.aborted:
                self.meta[doi] = (None, None, "", None)       # OpenAlex resolves the DOI itself
                if not walk.is_unresolved(slot):
                    self.s2_meta_failed += 1
            elif isinstance(slot, Outcome) and self.sess.aborted and slot.kind is Kind.DEFERRED and not slot.attempts:
                continue                                       # never sent: the run stopped first
            else:
                self._record_resolve(doi, slot, walk.is_unresolved(slot))
        self._group_aliases()
        primaries = list(self.groups)
        counts = {p: self.meta[p][1] for p in primaries}
        self.plan = walk.plan(counts, self.cached, refresh=self.refresh, source=self.source,
                              openalex_key=self._oa_ok(), n_metadata=len(todo))
        print(walk.plan_line(self.plan))
        if self.alias_rows:
            print(f"[aliases] {len(self.alias_rows)} seed DOI(s) S2 knows under another DOI or paperId:")
            for a in self.alias_rows:
                print(f"    {a['doi']:<40} {a['relation']:<22} {a['other']}  (paperId {a['paper_id'] or '-'})")

        kept, nested, rest = [], {}, []
        self.wid_of = {}
        for p in primaries:
            pid, count, _s2doi, _year = self.meta[p]
            r = walk.route(count, source=self.source, oa_ok=self._oa_ok())
            src = walk.route_source(r)
            if not any(walk.needs_walk(count, self.cached.get((d, src)), refresh=self.refresh)
                       for d in self.groups[p]):
                kept.append((p, src, count))
                continue
            wid = walk.walk_id(pid, p)
            self.wid_of[p] = wid
            if r == walk.ROUTE_NESTED:
                nested[wid] = count
            else:
                rest.append((p, r))
        self._record_kept(kept)

        primary_of = {self.wid_of[p]: p for p in self.wid_of}
        if nested and not self._stopped(walk.ROUTE_NESTED):
            for wid, res in walk.walk_nested(nested, session=self.sess):
                if res.not_sent or self.locked:
                    continue
                self._record(primary_of[wid], res, count=self.meta[primary_of[wid]][1], route=walk.ROUTE_NESTED)
        for p, r in rest:
            if self._stopped(r):
                break
            res = self._walk_one(p, r)
            if res is None or res.not_sent:
                if self._stopped(r):
                    break
                continue
            self._record(p, res, count=self.meta[p][1], route=res.route_taken)
        self._recount()

    def _walk_one(self, p, r):
        pid, count, _s2doi, year = self.meta[p]
        wid = self.wid_of[p]
        if r == walk.ROUTE_OPENALEX and self.source == "s2" and not self._oa_ok():
            r = walk.ROUTE_WINDOWS                             # OpenAlex stopped during the run
        if r == walk.ROUTE_EMPTY:
            res = walk.Result(str(s2.WalkState.EMPTY), "s2", [], reason="count 0; no call")
        elif r == walk.ROUTE_PAGED:
            res = walk.walk_paged(wid, count, session=self.sess)
        elif r == walk.ROUTE_WINDOWS:
            res = walk.walk_windows(wid, count, year, session=self.sess)
        else:
            res = walk.walk_openalex(p, session=self.osess)
            if self.source == "s2" and count is not None and (res.state == str(s2.WalkState.NOT_FOUND)
                                                              or res.not_sent):
                r = walk.ROUTE_WINDOWS     # OpenAlex does not hold the DOI, or stopped before its next page
                res = walk.walk_windows(wid, count, year, session=self.sess)
        res.route_taken = r
        return res

    def _record_kept(self, kept):
        """Seeds the count gate keeps: journalled as answered (one fsync), not printed one by one."""
        batch = []
        for p, src, count in kept:
            for d in self.groups[p]:
                st = self.cached.get((d, src), {})
                e = _entry(d, "gate", st.get("state") or "complete", source=src, count=count,
                           n_rows=st.get("n_rows"), paper_id=self.meta[p][0],
                           reason="kept: citationCount unchanged since the cached walk")
                self.entries[d] = e
                batch.append(e)
        self.journal.append_many(batch)
        self.kept = len(batch)
        if batch:
            print(f"[gate] {len(batch)} seed(s) kept: citationCount unchanged since their cached walk")

    def _group_aliases(self):
        """One walk per S2 paperId; aliases reported (a seed on another seed's paperId, or a seed S2
        knows under another DOI form)."""
        by_pid = {}
        for d in self.meta:                                   # walk order
            pid, _count, s2doi, _year = self.meta[d]
            if pid and pid in by_pid:
                prim = by_pid[pid]
                self.groups[prim].append(d)
                self.alias_rows.append({"doi": d, "relation": "same S2 paper as", "other": prim, "paper_id": pid})
            else:
                if pid:
                    by_pid[pid] = d
                self.groups[d] = [d]
            if s2doi and s2doi != d:
                self.alias_rows.append({"doi": d, "relation": "S2 lists it as", "other": s2doi, "paper_id": pid})

    def _recount(self):
        """A count mismatch from drift during a long run (citationCount read at the start, citers
        paged later) is re-checked once: one batch call for the mismatched seeds, and a seed whose
        count changed is walked again against the new count. A mismatch with an unchanged count
        stays FAILED."""
        mism = [p for p in self.groups if self.entries.get(p, {}).get("count_mismatch")
                and self.entries.get(p, {}).get("source") == "s2"]
        if not mism or self.sess.aborted or self.locked:
            return
        print(f"\n[recount] {len(mism)} count mismatch(es): re-reading citationCount once")
        batch = s2.paper_batch(mism, RESOLVE_FIELDS, session=self.sess)
        for p, slot in zip(mism, batch.payload):
            if self.sess.aborted or self.locked:
                break
            got = walk.resolved(slot)
            if got is None or got[1] == self.entries[p]["expected"]:
                continue
            pid, count, _s2doi, year = self.meta[p]
            self.meta[p] = (pid, got[1], _s2doi, year)
            r = walk.route(got[1], source="s2", oa_ok=self._oa_ok())
            res = self._walk_one(p, walk.ROUTE_PAGED if r == walk.ROUTE_NESTED else r)
            if res.not_sent:
                continue
            self._record(p, res, count=got[1], route=res.route_taken, recount=True)

    # -- finishing
    def _cached_rows(self, dois):
        """{doi: [report-column rows]} from the cache for every seed whose stored set is valid: the
        source this run used for it, else the most recently stored source."""
        valid = defaultdict(list)            # doi -> [(rows_at, source)]: a stored set exists, whatever the
        for (d, src), st in self.cache.states().items():  # last walk said (a failed seed keeps its rows)
            if st.get("rows_at") is not None:
                valid[d].append((st["rows_at"], src))
        pick = {}
        for d in dict.fromkeys(dois):
            e = self.entries.get(d)
            pref = e.get("source") if e else None
            cands = valid.get(d)
            if not cands:
                continue
            srcs = {src for _, src in cands}
            pick[d] = pref if pref in srcs else max(cands)[1]
        got = self.cache.rows_many((d, src) for d, src in pick.items())
        return {d: [_csv_citing(c, src) for c in got.get((d, src), [])] for d, src in pick.items()}

    def _tally(self):
        final = [self.entries.get(d) for d in self.walk_dois]
        failed = [e for e in final if e and e["state"] == "failed"]
        return {
            "final": final, "failed": failed,
            "mismatch": [e for e in failed if e.get("count_mismatch")],
            "transport": [e for e in failed if e.get("kind") in TRANSPORT_KINDS],
            "not_walked": sum(1 for e in final if e is None),
        }

    def _aborted(self):
        if self.locked:
            return CACHE_LOCKED
        if self.source == "openalex" and self.osess.aborted:
            return f"openalex {self.osess.aborted}"
        return self.sess.aborted

    def _finish(self) -> dict:
        t = self._tally()
        cached = self._cached_rows(s.doi for s in self.seeds if s.doi)
        if self.scoped:
            rows, with_citers, prior_with = self._scoped_rows(cached)
            fields = SCOPED_FIELDS
        else:
            rows, with_citers, prior_with = self._library_rows(cached)
            fields = FIELDS
        aborted = self._aborted()
        # The 5 % rule's denominator is the seeds this run walked: a gate-kept seed was not walked, and
        # counting it would let 30 failures in 60 walks pass beside 1,000 kept seeds (K3).
        attempted = sum(1 for e in t["final"] if e is None or e.get("stage") != "gate")
        code, reasons = verdict(attempted, len(t["failed"]), with_citers, prior_with, aborted,
                                self.force, t["not_walked"])
        if self.source == "openalex" and self.osess.aborted == "config":
            code = EXIT_ERROR
            reasons.append("OpenAlex rejected the key (CONFIG)")
        too_many = len(t["failed"]) > FAIL_THRESHOLD * attempted

        self.out.parent.mkdir(parents=True, exist_ok=True)
        uniq = sorted({r["citing_doi"] for r in rows if r["citing_doi"]})
        desc = None
        if reasons:
            dpath = degraded_path(self.out)
            lit_util.atomic_write_csv(str(dpath), rows, fields)
            written = str(dpath)
            if aborted or too_many:
                self.journal.close()                         # kept: the next run resumes from it
            else:
                self.journal.remove()                        # nothing failed: re-walk next time
        else:
            lit_util.atomic_write_csv(str(self.out), rows, fields)
            if self.scoped:
                desc = self._write_descendants(rows)
            else:
                lit_util.atomic_write_csv(str(unique_path(self.out)), [{"doi": d} for d in uniq], ["doi"])
            self.journal.remove()
            written = str(self.out)

        final = t["final"]
        res = {
            "step": "forward_citations", "exit_code": code,
            "status": "error" if code == EXIT_ERROR else ("aborted" if aborted else ("degraded" if reasons else "ok")),
            "reasons": reasons, "mode": "scoped" if self.scoped else "library", "scope": self.scope,
            "source": self.source,
            "seeds": len(self.seeds), "walked_pdfs": self.walk_n,
            "with_doi": sum(1 for s in self.seeds[:self.walk_n] if s.doi),
            "no_doi": sum(1 for s in self.seeds[:self.walk_n] if not s.doi and s.why != "placeholder"),
            "placeholder": sum(1 for s in self.seeds[:self.walk_n] if s.why == "placeholder"),
            "text_only_seeds": sum(1 for s in self.seeds[:self.walk_n] if s.kind == "text_only"),
            "flagged": 0 if self.scoped else self.seed_stats["flagged"],
            "seed_walks": len(self.walk_dois), "resumed": self.resumed, "not_walked": t["not_walked"],
            "answered": sum(1 for e in final if _answered(e)),
            "kept": sum(1 for e in final if e and e.get("stage") == "gate"),
            "walked": sum(1 for e in final if e and e.get("stage") in ("citations", "cache")),
            "failed": len(t["failed"]),
            "failed_resolve": sum(1 for e in t["failed"] if e["stage"] == "resolve"),
            "failed_citations": sum(1 for e in t["failed"] if e["stage"] == "citations"),
            "failed_cache": sum(1 for e in t["failed"] if e["stage"] == "cache"),
            "count_mismatch": len(t["mismatch"]),
            "recounted": sum(1 for e in final if e and e.get("recount")),
            "transport_failures": len(t["transport"]),
            "unresolved": sum(1 for e in final if e and e["state"] == "unresolved"),
            "zero_citers": sum(1 for e in final if e and e["state"] == "empty"),
            "capped": sum(1 for e in final if e and e["state"] == "capped_9999"),
            "elided": sum(1 for e in final if e and e["state"] == "elided"),
            "not_found": sum(1 for e in final if e and e["state"] == "not_found"),
            "openalex_walked": sum(1 for e in final if e and e.get("source") == "openalex"
                                   and e.get("stage") == "citations"),
            "aliases": len({a["doi"] for a in self.alias_rows}
                           | {a["other"] for a in self.alias_rows if a["relation"] == "same S2 paper as"}),
            "alias_table": self.alias_rows,
            "s2_metadata_failed": self.s2_meta_failed,
            "seeds_with_citers": with_citers, "prior_seeds_with_citers": prior_with,
            "total_rows": len(rows), "unique_dois": len(uniq),
            "written": written, "published": code == EXIT_OK,
            "journal": str(self.journal.path) if self.journal.path.exists() else None,
            "cache": str(self.cpath), "plan": self.plan,
            "aborted": aborted, "s2": self.sess.summary(), "openalex": self.osess.summary(),
        }
        if desc is not None:
            res["descendants"] = desc
        _print_summary(res, self.out)
        return res

    def _library_rows(self, cached):
        rows = []
        for s in self.seeds:                                # every seed file: cache, else published
            if not s.doi:
                continue
            cites = cached.get(s.doi)
            if cites is None:
                cites = self.prior.get(s.doi, ()) if self.prior is not None else ()
            rows.extend({**c, "seed_pdf": s.file, "seed_doi": s.doi} for c in cites)
        current = {s.doi for s in self.seeds if s.doi}
        on_disk = {s.file for s in self.seeds}
        with_citers = len({r["seed_doi"] for r in rows})
        # A published seed still counts while its file is in the library, even when its DOI could not
        # be read this run (F-4): a removed file is a removed seed, an unreadable .ris is not. Rows
        # published under a placeholder DOI are left out: that seed is never walked again.
        prior_with = (len({d for d, rs in self.prior.items() if not is_placeholder_doi(d)
                           and (d in current or any(r.get("seed_pdf") in on_disk for r in rs))})
                      if self.prior is not None else None)
        return rows, with_citers, prior_with

    def _scoped_rows(self, cached):
        rows = []
        for s in self.seeds:
            cites = cached.get(s.doi)
            if cites is None:
                cites = self.prior.get(s.doi, ()) if self.prior is not None else ()
            n = "" if s.n_cites is None else s.n_cites
            rows.extend({**{k: c.get(k, "") for k in _CITING}, "seed_doi": s.doi, "seed_label": s.label,
                         "seed_chapter": s.chapter, "seed_n_cites": n} for c in cites)
        current = {s.doi for s in self.seeds}
        with_citers = len({r["seed_doi"] for r in rows})
        prior_with = (len({d for d in self.prior if d in current and not is_placeholder_doi(d)})
                      if self.prior is not None else None)
        return rows, with_citers, prior_with

    def _write_descendants(self, rows) -> dict:
        from litpipe import holdings
        try:
            cache_dir = self.cpath.parent if (self.cache_path is not None or walk.CACHE_PATH is not None) \
                and not self.reg.get("state_dir") else None
            hm = holdings.build(self.reg, cache_dir=cache_dir)
            held = lambda d: bool(hm.where(d))                 # noqa: E731
        except config.ConfigError as e:
            print(f"[warn] holdings unavailable ({ledger.redact(e)}); held DOIs are not dropped")
            held = None
        states = self.cache.states()
        pid_of = {}
        for (d, _src), st in states.items():
            if st.get("paper_id"):
                pid_of.setdefault(d, st["paper_id"])
        for p, members in self.groups.items():
            for d in members:
                if self.meta.get(p, (None,))[0]:
                    pid_of[d] = self.meta[p][0]
        ranked, stats = rank_descendants(rows, held=held,
                                         seed_key=lambda d: f"S2:{pid_of[d]}" if d in pid_of else d)
        lit_util.atomic_write_csv(str(self.desc_out), ranked, DESCENDANT_FIELDS)
        print(f"\n  descendants: {len(ranked)} citing DOIs ({stats['held']} held anywhere dropped, "
              f"{stats['no_doi']} rows without a DOI) -> {self.desc_out.name}")
        print("  seed-overlap tiers (distinct seeds cited):")
        for n_seeds, k in sorted(stats["tiers"].items(), reverse=True)[:10]:
            print(f"    cites {n_seeds:>2} distinct seed(s): {k:>6} papers")
        return {"path": str(self.desc_out), "rows": len(ranked), "held_dropped": stats["held"],
                "tiers": {str(k): v for k, v in sorted(stats["tiers"].items(), reverse=True)}}


# ------------------------------------------------------------------------------ descendants
_OA_RANK = {True: 0, None: 1, False: 2}


def rank_descendants(rows, *, held=None, seed_key=None):
    """(ranked descendant records, stats) of scoped forward rows given in file order. One record per
    citing DOI (rows without one are dropped), its metadata from the first row naming it; n_seeds =
    distinct seeds citing it (seed_key unions alias DOIs on one S2 paper); oa = True when any row
    says open access, False when one says closed and none open, else unknown; seed_n_cites = the sum
    of its distinct seeds' n_cites. A DOI `held(doi)` says is held is dropped. Ranked by (-n_seeds,
    OA first, -cited_by, -year); ties keep first appearance (a stable sort), so with OA unknown on
    every row the order is the consumer gate's (-n_seeds, -cited_by, -year)."""
    by_doi: dict = {}
    seeds_of = defaultdict(set)
    weights = defaultdict(dict)
    n_nodoi = n_held = 0
    for r in rows:
        d = walk.norm_doi(r.get("citing_doi") or "") or (r.get("citing_doi") or "").strip().lower()
        if not d:
            n_nodoi += 1
            continue
        seed = (r.get("seed_doi") or "").strip().lower()
        sk = seed_key(seed) if seed_key else seed
        seeds_of[d].add(sk)
        n = lit_util.coerce_int(r.get("seed_n_cites"), None)
        if n is not None:
            weights[d][sk] = max(n, weights[d].get(sk, n))
        rec = by_doi.get(d)
        if rec is None:
            rec = by_doi[d] = {"doi": d, "title": (r.get("citing_title") or "").strip(),
                               "authors": r.get("citing_authors") or "",
                               "year": lit_util.coerce_int(r.get("citing_year"), 0),
                               "venue": r.get("citing_venue") or "",
                               "cited_by": lit_util.coerce_int(r.get("citing_cited_by"), 0),
                               "oa": None, "_chapters": [], "_labels": [], "_seeds": [], "_sources": []}
        oa = walk._bool_or_none(r.get("citing_oa"))
        if oa is True or (oa is False and rec["oa"] is None):
            rec["oa"] = oa
        for key, val in (("_chapters", r.get("seed_chapter")), ("_labels", r.get("seed_label") or seed),
                         ("_seeds", seed), ("_sources", r.get("source"))):
            if val and val not in rec[key]:
                rec[key].append(val)
    kept = []
    for d, rec in by_doi.items():
        if held is not None and held(d):
            n_held += 1
            continue
        rec["n_seeds"] = len(seeds_of[d])
        rec["seed_n_cites"] = sum(weights[d].values()) if weights[d] else ""
        kept.append(rec)
    kept.sort(key=lambda r: (-r["n_seeds"], _OA_RANK[r["oa"]], -r["cited_by"], -r["year"]))
    ranked = []
    for i, rec in enumerate(kept, 1):
        ranked.append({"rank": i, "doi": rec["doi"], "title": rec["title"], "authors": rec["authors"],
                       "year": rec["year"] or "", "venue": rec["venue"], "cited_by": rec["cited_by"],
                       "oa": "" if rec["oa"] is None else ("true" if rec["oa"] else "false"),
                       "n_seeds": rec["n_seeds"], "chapters": ";".join(sorted(rec["_chapters"])),
                       "seeds": " | ".join(rec["_labels"][:6]), "seed_dois": ";".join(rec["_seeds"]),
                       "sources": ";".join(rec["_sources"]), "seed_n_cites": rec["seed_n_cites"]})
    tiers = Counter(r["n_seeds"] for r in ranked)
    return ranked, {"tiers": dict(tiers), "held": n_held, "no_doi": n_nodoi, "distinct": len(by_doi)}


# ------------------------------------------------------------------------------ verdict and summary
def verdict(seed_walks, failed, with_citers, prior_with_citers=None, aborted=None, force=False,
            not_walked=0) -> tuple:
    """(exit code, reasons) of a finished walk. Exit 3 when the session aborted; exit 2 when more
    than 5 % of seed walks failed, or when the result has fewer seeds with citers than the
    published report (unless `force`); else 0. Pure, so recorded runs can be replayed."""
    reasons = []
    if aborted:
        reasons.append(f"aborted ({aborted}); {not_walked} seed(s) not walked")
    elif not_walked:
        reasons.append(f"{not_walked} seed(s) not walked")
    if failed > FAIL_THRESHOLD * seed_walks:
        reasons.append(f"{failed} of {seed_walks} seed walks failed (over 5 %)")
    if prior_with_citers is not None and with_citers < prior_with_citers and not force:
        reasons.append(f"{with_citers} seeds with citers, fewer than the published {prior_with_citers}")
    code = EXIT_ABORTED if aborted else (EXIT_DEGRADED if reasons else EXIT_OK)
    return code, reasons


def _print_summary(res, out):
    print()
    print("=== summary ===")
    print(f"  seeds:              {res['seeds']}" + (f" ({res['walked_pdfs']} walked)"
                                                     if res["walked_pdfs"] != res["seeds"] else ""))
    print(f"  with DOI:           {res['with_doi']}")
    print(f"  no DOI:             {res['no_doi']}")
    print(f"  placeholder DOI:    {res['placeholder']} (never walked)")
    if res["mode"] == "library":
        print(f"  text-only seeds:    {res['text_only_seeds']}")
        print(f"  identity-flagged:   {res['flagged']} (not seeds)")
    print(f"  seed walks:         {res['seed_walks']} distinct DOIs ({res['resumed']} resumed, "
          f"{res['kept']} kept by the count gate, {res['walked']} walked)")
    print(f"  S2 resolve failed:  {res['failed_resolve']}")
    print(f"  citations failed:   {res['failed_citations']} (count mismatch {res['count_mismatch']}, "
          f"transport {res['transport_failures']})")
    if res["failed_cache"]:
        print(f"  cache failed:       {res['failed_cache']}")
    print(f"  S2 unresolved:      {res['unresolved']} (no S2 record; not a failure)")
    print(f"  zero citers:        {res['zero_citers']} (not a failure)")
    if res["capped"] or res["openalex_walked"]:
        print(f"  capped at 9,999:    {res['capped']}   walked on OpenAlex: {res['openalex_walked']}")
    if res["elided"] or res["not_found"]:
        print(f"  elided / not found: {res['elided']} / {res['not_found']} (prior rows kept)")
    print(f"  aliases:            {res['aliases']}")
    if res["not_walked"]:
        print(f"  not walked:         {res['not_walked']} (run stopped)")
    print(f"  seeds with citers:  {res['seeds_with_citers']}"
          + (f" (published: {res['prior_seeds_with_citers']})" if res["prior_seeds_with_citers"] is not None else ""))
    print(f"  total citing rows:  {res['total_rows']}")
    print(f"  unique citing DOIs: {res['unique_dois']}")
    if res["published"]:
        print(f"  report:             {out}")
        if res["mode"] == "library":
            print(f"  unique DOIs:        {unique_path(out)}")
    else:
        status = {"aborted": "ABORTED", "error": "ERROR"}.get(res["status"], "DEGRADED")
        print(f"  [{status}] {'; '.join(res['reasons'])}")
        print(f"  NOT published: {out.name} left as it was; this run's result is in {res['written']}")
        if res["journal"]:
            print(f"  resume: rerun the same command (answered seeds are kept in {Path(res['journal']).name})")
        if any("fewer than the published" in r for r in res["reasons"]):
            print("  if the drop is real, rerun with --force to publish it")
    print(f"  {s2_line(res)}")
    if res.get("openalex", {}).get("calls"):
        o = res["openalex"]
        print(f"  [openalex] calls={o.get('calls')} credits={o.get('credits')} remaining={o.get('remaining')}"
              + (f" ABORTED={o.get('aborted')}" if o.get("aborted") else ""))
    _emit_summary(res)


def _emit_summary(res):
    small = {k: v for k, v in res.items() if k not in ("s2", "openalex", "alias_table", "plan")}
    small["s2"] = {k: res.get("s2", {}).get(k) for k in ("calls", "attempts", "attempts_not_ok", "budget", "aborted")}
    if "openalex" in res:
        small["openalex_calls"] = res["openalex"].get("calls")
    if res.get("plan"):
        small["plan_s2_calls"] = res["plan"].get("s2_calls")
    print(SUMMARY_MARKER + json.dumps(small, ensure_ascii=False, default=str), flush=True)


def s2_line(res) -> str:
    s = res["s2"]
    return (f"[s2] {s.get('key')} calls={s.get('calls')} attempts={s.get('attempts')} "
            f"not_ok_attempts={s.get('attempts_not_ok')} budget={s.get('budget')}"
            + (f" ABORTED={s.get('aborted')}" if s.get("aborted") else ""))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    ap.add_argument("--project", default=None,
                     help="Project name from projects.json (e.g. 'research_a'); in scoped mode, the output library.")
    ap.add_argument("--lib-dir", default=None,
                     help="Explicit library path (legacy); in scoped mode, the output library.")
    ap.add_argument("--report",  default=None,
                     help="Output CSV (default: <lib-dir>/_forward_citations.csv; library mode only).")
    ap.add_argument("--limit",   type=int, default=0,
                     help="Walk the first N seeds only (testing); the others keep their rows.")
    ap.add_argument("--refresh", action="store_true",
                     help="Walk every seed, ignoring the count gate (citationCount unchanged since the cached walk).")
    ap.add_argument("--seeds-from", nargs="+", default=None, metavar="LIST[::LABEL]",
                     help="Scoped mode: seed on DOI lists (CSV with a doi column; status/bookish/chapter/n_cites "
                          "honoured when present). ::LABEL names the chapter. Needs --scope.")
    ap.add_argument("--scope",   default=None,
                     help="Scoped mode: NAME of the harvest, written to _<NAME>_forward_citations.csv and "
                          "_<NAME>_descendants.csv in the output library ([a-z0-9][a-z0-9_-]*).")
    ap.add_argument("--source",  default="s2", choices=list(walk.SOURCES),
                     help="Walk source (default s2; seeds over 9,999 citers use OpenAlex when keyed). "
                          "'openalex' walks every seed through OpenAlex cites:.")
    ap.add_argument("--sleep",   type=float, default=None,
                     help="Deprecated and ignored: spacing comes from litpipe.s2 (projects.json s2 block).")
    ap.add_argument("--s2-key",  default=None,
                     help="Deprecated: set the S2_API_KEY environment variable instead.")
    ap.add_argument("--force",   action="store_true",
                     help="Publish even when the result has fewer seeds with citers than the published report; "
                          "in scoped mode also replace a scoped CSV written outside the pipeline.")
    ap.add_argument("--restart", action="store_true",
                     help="Ignore a resumable <report>.partial.jsonl and start the run over (the count gate still applies).")
    args = ap.parse_args(sys.argv[1:] if argv is None else argv)
    res = run(project=args.project, lib_dir=args.lib_dir, report=args.report, limit=args.limit,
              sleep=args.sleep, s2_key=args.s2_key, force=args.force, restart=args.restart,
              refresh=args.refresh, seeds_from=args.seeds_from, scope=args.scope, source=args.source)
    return res["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
