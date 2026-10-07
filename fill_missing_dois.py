"""Recover DOIs for orphan PDFs: the front-matter DOI, else a strict Crossref title match.

An orphan is a PDF whose `.fulltext.json` has an empty or missing `doi`, or that has no sidecar.
The acceptance rule is refactor scope 3.6 (V3: the top1/top2 score ratio does not separate right
from wrong matches; at 1.10 it accepted 8 of 12 known-wrong ones). For each orphan, in order:

1. A file whose `.identity.json` or `.fulltext.json` flags it (audit_portfolio.identity_flag) is
   skipped, SKIP_IDENTITY_FLAG: its name describes the paper that was asked for, not the file.
2. The sidecar's `doi_candidate` (the text DOI extract_pdf_fulltext records, never as `doi`;
   DEC-20) is tried first and verified exactly as the front-matter DOI below (basis doi_candidate);
   one the record contradicts, or no source holds, falls through to the next steps.
3. Front-matter DOI: the first DOI in the first ~6,000 characters of the text (the sidecar
   `text`, else PyMuPDF), resolved with ris_emit.resolve_meta (Crossref, DataCite, content
   negotiation). It is accepted (HIGH, basis front_matter_doi) unless the record contradicts the
   file: its year must be within one of the filename year, and the filename author must be one of
   its authors or its strict title must occur in the text (V3: right on 1,007 of 1,123 rows).
4. Otherwise a Crossref `query.bibliographic` search (rows=8) from the filename hints:
   - never candidates: peer-review records, the `10.3410/` (Faculty Opinions) prefix, correction
     and retraction notices (`update-to`), records that review, comment on or reply to another
     work (`relation`), and titles that start "Faculty Opinions recommendation of", "Correction
     to", "Erratum", "Comment on", "Response to" and the like, unless the query has the phrase
     (notes/2026-08-17 Crossref gotchas);
   - a type-demoted record (dataset, component, journal issue, ...) is never a fallback
     (DEMOTED_ONLY); every candidate excluded gives EXCLUDED_ONLY;
   - AMBIG when a sibling (same first author and year) scores within the tie margin (1.10) of the
     top record: series parts and F1000 versions (V3-N2; the near-twin promotion is gone);
   - HIGH (basis strict_title) only when a candidate's title is strict (normalised, at least 40
     characters and 6 words) and occurs in the text, its first author matches the filename author,
     and its year (ris_emit.crossref_meta: print first) equals the filename year;
   - every other label (MED_*, LOW_TITLE_ONLY, AMBIG) is a review row and is never written.

--execute writes HIGH rows only (DEC-20), and never overwrites a `.ris` DOI:
- a `.ris` with the same DOI, or with none: the DOI goes to the sidecar (the `.ris` is kept);
- a `.ris` with another DOI: the sidecar keeps the `.ris` DOI and stores the match as
  `doi_published` (the `.ris` is a preprint) or `doi_unverified_match`;
- no `.ris`: the DOI goes to the sidecar and a `.ris` is emitted through ris_emit.write_ris
  (DEC-29 manifest), so the index sees the fill.
A malformed DOI is never written. Every request goes through litpipe.net (one identity from
LITPIPE_EMAIL, pacing, ledger; DEC-13).

Reports carry a run id (YYYY-MM-DD, then YYYY-MM-DD.N), so a same-day re-run never replaces one:
`_doi_fill_report.<run_id>.csv` and the applied / review / skipped subsets. A dry run writes
nothing unless --report-dir is given; --execute writes them to the library unless --report-dir
names another directory.

Exit codes: 0; 1 usage or configuration (no registry, an unknown project); 2 a row whose lookup
could not be answered (ERR_*) or whose write failed, after a final `[step-summary] {json}` line.

Usage:
  python fill_missing_dois.py --project NAME                        # dry run, prints the rows
  python fill_missing_dois.py --project NAME --report-dir DIR       # dry run, reports saved
  python fill_missing_dois.py --project NAME --limit 20             # smoke test
  python fill_missing_dois.py --project NAME --execute              # apply HIGH rows
  python fill_missing_dois.py --all                                 # dry run, all active projects

A fill is visible to the index once the `.ris` carries it: the tool emits a `.ris` where none
exists, then `python index_portfolio.py --project NAME`. A `.ris` that exists without a DO line
is kept (curated or pre-DEC-29). backfill_ris regenerates such files only with `--force`, and
`--force` replaces every curated `.ris` in the library, EndNote edits included, so run it without
`--commit` first and check its list:
  python backfill_ris.py --lib-dir <lib>                      # dry run: what would change
  python backfill_ris.py --lib-dir <lib> --commit --force     # only if every listed file may go
"""
import argparse
import csv
import functools
import json
import os
import re
import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import lit_util
from lit_util import safe_ascii  # noqa: F401  (kept importable; callers used to get it from here)
lit_util.utf8_stdout()

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import ris_emit  # noqa: E402
from audit_filenames import flag_reason  # noqa: E402
from litpipe import doi as _doi  # noqa: E402
from litpipe import net  # noqa: E402
from litpipe import text as _text  # noqa: E402
from litpipe.ledger import redact  # noqa: E402
from litpipe.outcomes import Kind  # noqa: E402

CROSSREF = "https://api.crossref.org/works"   # the search endpoint (callers import the name)
CONFIG_PATH = Path(__file__).parent / "projects.json"
SUMMARY_MARKER = "[step-summary] "

CANONICAL_RE = re.compile(r"^(\d{4})_([A-Z][A-Za-z\-']+)_([A-Z][A-Za-z0-9\-]+)\.pdf$")
CANONICAL_LOOSE_RE = re.compile(r"^(\d{4})_([A-Z][A-Za-z\-']+)_(.+)\.pdf$")
LEGACY_RE = re.compile(r"^([a-z]+)_(\d{4})_([a-z0-9_]+)\.pdf$")
BUNDLE_RE = re.compile(r"(?i)(?<![A-Za-z0-9])(vol\d|edition\d|issue\d|volume\d)(?![A-Za-z0-9])")
YEAR_RE = re.compile(r"(?<!\d)(?:19|20)\d{2}(?!\d)")

PREFERRED_TYPES = {"journal-article", "proceedings-article", "book-chapter",
                   "report", "monograph", "reference-entry", "review", "letter",
                   "editorial", "other"}
DEMOTED_TYPES = {"dataset", "peer-review", "component", "grant", "book",
                 "book-set", "book-series", "journal-issue", "journal-volume",
                 "journal", "posted-content", "standard"}

# Section 3.6 rule and the 2026-08-17 Crossref gotchas.
ROWS = 8                      # the real article sat at rank 3 behind three Faculty Opinions records
TIE_MARGIN = 1.10             # a sibling scoring within this ratio of the top record ties with it
STRICT_MIN_CHARS = 40         # a title shorter than this, or with fewer words, matches anything
STRICT_MIN_WORDS = 6          # ("National Aeronautics and Space Administration", I15)
FRONT_MATTER_CHARS = 6000     # where the front-matter DOI is read (V3 calibration)
TEXT_EVIDENCE_CHARS = 10000   # where a title must occur (page one and a cover sheet)
EXCLUDED_TYPES = frozenset({"peer-review"})
EXCLUDED_PREFIXES = ("10.3410/",)                                  # Faculty Opinions / F1000Prime
EXCLUDED_RELATIONS = ("is-review-of", "is-comment-on", "is-reply-to")
VERSION_UPDATE_TYPES = frozenset({"new_version", "new_edition"})   # an update-to that is not a notice
TITLE_PREFIX_GUARDS = (
    "faculty opinions recommendation of", "peer review report for", "correction to",
    "correction for", "correction:", "corrigendum", "erratum", "retraction:", "retraction note",
    "retraction notice", "notice of retraction", "retracted:", "retracted article",
    "expression of concern", "comment on", "commentary on", "commentary to", "commentary:",
    "response to", "reply to", "authors' reply", "author response")
# Preprint servers: a .ris carrying one of these is the preprint; a different match is its
# published version (doi_published). 10.1101 is shared with CSHL journals, so only its dated and
# six-digit preprint forms count.
PREPRINT_PREFIXES = ("10.64898/", "10.21203/", "10.20944/", "10.31219/", "10.31234/", "10.31235/",
                     "10.31236/", "10.35542/", "10.31222/", "10.31221/", "10.31730/", "10.33767/",
                     "10.51224/", "10.48550/", "10.2139/", "10.22541/", "10.36227/", "10.26434/")
_BIORXIV_SUFFIX = re.compile(r"^(?:\d{4}\.\d{2}\.\d{2}\.\d+|\d{6})(?:v\d+)?$")

WRITABLE = frozenset({"HIGH"})
CONFIDENCE_RANK = {"HIGH": 4, "MED_NO_YEAR": 3, "MED_AUTHOR_ONLY": 3,
                   "MED_TITLE_STRONG": 3,
                   "MED_AUTHOR_YEAR_MISMATCH": 2, "MED_TYPE_MISMATCH": 2,
                   "LOW_TITLE_ONLY": 1, "AMBIG": 0, "NO_RESULT": 0,
                   "ERROR": 0, "SKIP_BUNDLED_ISSUE": 0, "SKIP_NO_HINTS": 0,
                   "DEMOTED_ONLY": 0, "EXCLUDED_ONLY": 0, "SKIP_IDENTITY_FLAG": 0}
REPORT_PREFIXES = ("_doi_fill_report", "_doi_fill_applied", "_doi_fill_review", "_doi_fill_skipped")
FIELDNAMES = ["filename", "parse_class", "parsed_year", "parsed_author",
              "parsed_title", "status", "crossref_doi", "crossref_type",
              "crossref_title", "crossref_year", "crossref_first_author",
              "crossref_journal", "top1_score", "top2_score", "applied",
              # added by W3-D2 (appended; the leading columns keep their order)
              "basis", "front_matter_doi", "ris_doi", "action", "note",
              # added by W5-C2: the sidecar's doi_candidate, tried first
              "doi_candidate"]


class ConfigError(Exception):
    """No registry, or an unknown project: exit 1."""


class _Parser(argparse.ArgumentParser):
    def error(self, message):
        self.print_usage(sys.stderr)
        self.exit(1, f"{self.prog}: error: {message}\n")


# ------------------------------------------------------------------------------ filename hints
def parse_filename_hints(fn):
    """Return (year, author, title_hint, parse_class). year='' means unknown."""
    m = CANONICAL_RE.match(fn)
    if m:
        year, author, title_slug = m.groups()
        title_words = re.findall(r"[A-Z][a-z0-9\-']*", title_slug)
        title = " ".join(title_words)
        if year == "0000":
            year = ""
        return year, author, title, "canonical"
    m = CANONICAL_LOOSE_RE.match(fn)
    if m:
        year, author, rest = m.groups()
        parts = []
        for seg in rest.split("_"):
            words = re.findall(r"[A-Z][a-z0-9\-']*|[A-Z]+(?=[A-Z][a-z])|[A-Z]+|[a-z0-9]+", seg)
            parts.extend(w for w in words if w)
        title = " ".join(parts).strip()
        if year == "0000":
            year = ""
        return year, author, title, "canonical-loose"
    m = LEGACY_RE.match(fn)
    if m:
        author, year, title_slug = m.groups()
        title = title_slug.replace("_", " ")
        return year, author.title(), title, "legacy"
    ym = YEAR_RE.search(fn)
    year = ym.group(0) if ym else ""
    base = fn[:-4] if fn.lower().endswith(".pdf") else fn
    if year:
        base = base.replace(year, "")
    title = re.sub(r"_+", " ", base).strip()
    return year, None, title, "gibberish"


def normalize_for_match(s):
    """Lowercase + ASCII-fold (lit_util.safe_ascii handles ø, æ, ß, ł, which NFKD leaves), so
    `2020_Molmen_*.pdf` round-trips against Crossref's "Mølmen"."""
    return safe_ascii(s).lower() if s else ""


def _name_key(s):
    """A surname as a comparison key: folded, lower case, letters and digits only."""
    return re.sub(r"[^a-z0-9]", "", normalize_for_match(s))


def is_bundled_issue(fn, title_hint):
    """Detect journal-issue bundle patterns (`2007_IJCSS_Vol6_Edition2.pdf`)."""
    return bool(BUNDLE_RE.search(fn) or BUNDLE_RE.search(title_hint or ""))


def load_sidecar(sidecar_path):
    try:
        with open(sidecar_path, encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return None


def sidecar_title_hint(sidecar):
    """A title hint from the sidecar when the filename's is weak: its `title` field, else the first
    non-blank line of its `text` with four or more words (often the cover title)."""
    if not sidecar:
        return ""
    t = (sidecar.get("title") or "").strip()
    if t and len(t.split()) >= 3:
        return t
    text = sidecar.get("text") or ""
    for line in text.splitlines()[:30]:
        line = line.strip()
        if len(line.split()) >= 4 and not re.match(r"^[\d\W]+$", line):
            return line
    return t


def evidence_text(pdf_path, sidecar):
    """(text, note): the start of the file's own text, where its title and DOI are: the sidecar
    `text`, else the PDF's (PyMuPDF). '' with a note when neither can be read."""
    t = (sidecar or {}).get("text") if isinstance(sidecar, dict) else None
    if isinstance(t, str) and t.strip():
        return t[:TEXT_EVIDENCE_CHARS], ""
    try:
        import pymupdf
        doc = pymupdf.open(pdf_path)
        try:
            text = ""
            for p in doc:
                text += p.get_text()
                if len(text) >= TEXT_EVIDENCE_CHARS:
                    break
        finally:
            doc.close()
        return text[:TEXT_EVIDENCE_CHARS], ""
    except Exception as e:  # PyMuPDF raises FileDataError, EmptyFileError, RuntimeError, OSError
        return "", f"pdf text unreadable ({type(e).__name__})"


# ------------------------------------------------------------------------------ Crossref
def crossref_query(query_str, top_n=ROWS, retries=3):
    """Crossref `query.bibliographic` search through litpipe.net. Returns (items, status): status
    is "OK", or "ERR_<KIND>: <detail>" when Crossref could not answer (a failed call is never an
    empty result). `retries` is kept for callers; litpipe.net owns retries and pacing."""
    out = net.get(CROSSREF, params={"query.bibliographic": query_str, "rows": str(top_n)},
                  validate=net.expect_json, purpose="fill_missing_dois:search")
    if out.ok:
        try:
            body = out.payload.json()
        except (ValueError, AttributeError):
            return [], "ERR_OUTAGE: body is not JSON"
        items = ((body.get("message") or {}).get("items") or []) if isinstance(body, dict) else []
        return [it for it in items if isinstance(it, dict)], "OK"
    if out.kind is Kind.NO_MATCH:
        return [], "OK"
    kind = str(getattr(out.kind, "value", out.kind))
    return [], f"ERR_{kind}: {redact(str(out.detail or ''))[:120]}"


def extract_metadata(item):
    """The fields a match needs, from a Crossref work item: ris_emit.crossref_meta does the
    reading (print-first year, decoded and NFC names and titles, title joined with its subtitle),
    so `M&uuml;ndel` arrives as `Mündel`. `meta` is that flattened record (build_ris input)."""
    m = ris_emit.crossref_meta(item) if isinstance(item, dict) and item else {}
    raw_doi = ((item or {}).get("DOI") or "").strip()
    authors = [{"surname": a["family"], "given": a.get("given") or "", "source": "crossref"}
               for a in m.get("authors") or [] if a.get("family")]
    return {
        "score": (item or {}).get("score", 0) or 0,
        "doi": _doi.normalise(raw_doi) or raw_doi.lower(),
        "type": (item or {}).get("type") or "",
        "title": m.get("title", ""),
        "main_title": _text.display_field(ris_emit._first((item or {}).get("title"))),
        "first_family": authors[0]["surname"] if authors else "",
        "year": m.get("year", ""),
        "journal": m.get("container", ""),
        "authors": authors,
        "meta": m,
    }


def filter_by_type(items):
    """Partition into (preferred, demoted). Preferred (and unknown) types sort first."""
    preferred, demoted, unknown = [], [], []
    for it in items:
        t = (it.get("type") or "").lower()
        if t in PREFERRED_TYPES:
            preferred.append(it)
        elif t in DEMOTED_TYPES:
            demoted.append(it)
        else:
            unknown.append(it)
    return preferred + unknown, demoted


def _norm(s):
    """Comparison form for title containment: litpipe.text.comparison_fold (Greek letters by name,
    apostrophes dropped, Unicode dashes and quotes to ASCII, tags stripped, accents folded), then
    clean_field, ASCII-folded, lower case, every run of other characters one space. Both sides of
    every comparison pass through it, so `beta-adrenergic` finds the record's U+03B2 form and
    `ACSM's` finds `ACSM’s` (W5-C2, item 25)."""
    s = _text.comparison_fold(s or "")
    return " ".join(re.sub(r"[^a-z0-9]+", " ", safe_ascii(_text.clean_field(s)).lower()).split())


def strict_title(title):
    """The normalised title when it is specific (STRICT_MIN_CHARS and STRICT_MIN_WORDS), else ''."""
    t = _norm(title)
    return t if len(t) >= STRICT_MIN_CHARS and len(t.split()) >= STRICT_MIN_WORDS else ""


def _evidence(text):
    return f" {_norm((text or '')[:TEXT_EVIDENCE_CHARS])} " if text else ""


def title_in_text(meta, ev):
    """Does a strict form of the record's title (main title, or title with subtitle) occur in the
    normalised evidence `ev` (see _evidence), on word boundaries?"""
    if not ev or not ev.strip():
        return False
    for t in (meta.get("main_title"), meta.get("title")):
        s = strict_title(t)
        if s and f" {s} " in ev:
            return True
    return False


def excluded_reason(item, query=""):
    """Why a Crossref item can never be the paper ('' when it can): a peer-review record, the
    Faculty Opinions prefix, a correction or retraction notice, a review / comment / reply to
    another work, or a notice-style title the query itself does not ask for."""
    doi = (item.get("DOI") or "").strip().lower()
    if doi.startswith(EXCLUDED_PREFIXES):
        return "prefix:" + doi.split("/", 1)[0]
    typ = (item.get("type") or "").lower()
    if typ in EXCLUDED_TYPES:
        return "type:" + typ
    for u in item.get("update-to") or []:
        utype = (u.get("type") or "").lower() if isinstance(u, dict) else ""
        if utype not in VERSION_UPDATE_TYPES:
            return "update-notice:" + (utype or "update")
    rel = item.get("relation") if isinstance(item.get("relation"), dict) else {}
    for k in EXCLUDED_RELATIONS:
        if rel.get(k):
            return "relation:" + k
    title = " ".join(_text.display_field(ris_emit._first(item.get("title"))).lower().split())
    q = _norm(query)
    for p in TITLE_PREFIX_GUARDS:
        if title.startswith(p) and _norm(p) not in q:
            return "title:" + p
    return ""


def _author_match(hint, meta):
    """The filename author against the record's first author: equal keys, or the shorter (four or
    more letters) inside the longer (compound surnames)."""
    a, b = _name_key(hint), _name_key(meta.get("first_family"))
    if not a or not b or a == "unknown":
        return False
    if a == b:
        return True
    short, long_ = sorted((a, b), key=len)
    return len(short) >= 4 and short in long_


def _year_match(hint, meta):
    return bool(hint and meta.get("year") and str(hint) == str(meta["year"]))


def _siblings(a, b):
    """Same first author and same year: series parts, versions, companion papers."""
    fa, fb = _name_key(a.get("first_family")), _name_key(b.get("first_family"))
    return bool(fa and fa == fb and a.get("year") and a.get("year") == b.get("year"))


def _within(a, b):
    hi, lo = sorted((float(a.get("score") or 0), float(b.get("score") or 0)), reverse=True)
    return lo > 0 and hi / lo < TIE_MARGIN


_WARNED = []


def _warn_no_evidence():
    """Once per process: a caller written before section 3.6 (score_match without evidence=) gets
    no HIGH label any more; say so rather than degrade silently."""
    if not _WARNED:
        _WARNED.append(True)
        print("[fill_missing_dois] score_match called without evidence=: no match can be HIGH (section "
              "3.6 needs the record's title in the evidence text). Pass evidence=<the file's text or the "
              "reference string> and query=<the search string>.", file=sys.stderr)


def score_match(year_hint, author_hint, ranked_items, raw_items, evidence=None, query=""):
    """Label the Crossref hits for one orphan (section 3.6). Returns (status, meta, top1, top2).

    ranked_items are the type-preferred hits (filter_by_type), raw_items all hits; `evidence` is the
    file's own text (its title must occur there for HIGH; without it nothing is HIGH) and `query`
    the search string (a notice-style title the query asks for is not excluded). HIGH only for a
    strict title in the text plus first author plus year; AMBIG for a sibling within the tie
    margin; DEMOTED_ONLY / EXCLUDED_ONLY when no candidate is left (never a fallback); MED_* and
    LOW_TITLE_ONLY are review labels."""
    if not ranked_items and not raw_items:
        return "NO_RESULT", None, 0, 0
    if evidence is None:
        _warn_no_evidence()
    ev = _evidence(evidence)
    cands = [extract_metadata(it) for it in ranked_items if not excluded_reason(it, query)]
    if not cands:
        rest = [it for it in raw_items if not excluded_reason(it, query)]
        if rest:
            m = extract_metadata(rest[0])
            return "DEMOTED_ONLY", m, m["score"], (rest[1].get("score", 0) if len(rest) > 1 else 0)
        s = [it.get("score", 0) or 0 for it in raw_items[:2]] + [0, 0]
        return "EXCLUDED_ONLY", None, s[0], s[1]

    top = cands[0]
    top2 = cands[1]["score"] if len(cands) > 1 else 0
    if any(_siblings(top, c) and _within(top, c) for c in cands[1:]):
        return "AMBIG", top, top["score"], top2        # V3-N2: no near-twin promotion

    passing = [c for c in cands
               if title_in_text(c, ev) and _author_match(author_hint, c) and _year_match(year_hint, c)]
    if len(passing) == 1:
        ch = passing[0]
        others = [c for c in cands if c is not ch]
        if any(_siblings(ch, c) and _within(ch, c) for c in others):
            return "AMBIG", ch, ch["score"], max(c["score"] for c in others)
        return "HIGH", ch, ch["score"], max((c["score"] for c in others), default=0)
    if len(passing) > 1:
        return "AMBIG", passing[0], passing[0]["score"], passing[1]["score"]

    if len(cands) > 1 and _within(top, cands[1]):
        return "AMBIG", top, top["score"], top2
    author_match = _author_match(author_hint, top)
    if author_match and not year_hint:
        return "MED_NO_YEAR", top, top["score"], top2
    if author_match and top["year"]:
        try:
            if abs(int(year_hint) - int(top["year"])) >= 2:
                return "MED_AUTHOR_YEAR_MISMATCH", top, top["score"], top2
        except ValueError:
            pass
    if author_match:
        return "MED_AUTHOR_ONLY", top, top["score"], top2
    if title_in_text(top, ev) and (_year_match(year_hint, top) or not year_hint):
        return "MED_TITLE_STRONG", top, top["score"], top2
    return "LOW_TITLE_ONLY", top, top["score"], top2


# ------------------------------------------------------------------------------ front matter
def front_matter_candidates(text):
    """The DOI forms at the first DOI occurrence of `text` (most specific first; litpipe.doi)."""
    first, out = None, []
    for pos, cand in _doi.iter_candidates(text or ""):
        if first is None:
            first = pos
        if pos != first:
            break
        out.append(cand)
    return out


def _resolved_metadata(meta, source, doi):
    authors = [{"surname": a.get("family") or "", "given": a.get("given") or "", "source": source}
               for a in meta.get("authors") or [] if isinstance(a, dict) and a.get("family")]
    meta = dict(meta)
    meta["doi"] = _doi.normalise(meta.get("doi") or doi) or doi
    return {"score": 0, "doi": meta["doi"], "type": meta.get("type") or "", "title": meta.get("title") or "",
            "main_title": meta.get("title") or "", "first_family": meta.get("lastname") or "",
            "year": meta.get("year") or "", "journal": meta.get("container") or "", "authors": authors,
            "meta": meta, "source": source}


def candidate_match(cand, text, year_hint, author_hint):
    """(status, meta) for one DOI candidate, verified the one way every candidate is: it must
    resolve (ris_emit.resolve_meta) to a record with a title, its year within one of the filename
    year, and the filename author among its authors or its strict title in the text. ("HIGH", m),
    ("FRONT_MATTER_UNCONFIRMED", m) when the record contradicts the file, or (None, None) when no
    source holds it. Raises ris_emit.MetadataUnavailable."""
    meta, source = ris_emit.resolve_meta(cand)
    if not meta or not meta.get("title"):
        return None, None
    m = _resolved_metadata(meta, source, cand)
    year_ok = True
    if year_hint and m["year"]:
        try:
            year_ok = abs(int(year_hint) - int(m["year"])) <= 1
        except ValueError:
            year_ok = True
    author_ok = any(_author_match(author_hint, {"first_family": a["surname"]}) for a in m["authors"])
    if year_ok and (author_ok or title_in_text(m, _evidence(text))):
        return "HIGH", m
    return "FRONT_MATTER_UNCONFIRMED", m


def front_matter_match(text, year_hint, author_hint, tried=()):
    """(status, meta) for the file's front-matter DOI: ("HIGH", m) when it resolves and nothing
    contradicts it; ("FRONT_MATTER_UNCONFIRMED", m) when it resolves but the year is more than one
    off, or neither the author nor a strict title in the text supports it; (None, None) when the
    text has no DOI or no source holds it. Candidates in `tried` (already verified this run, e.g.
    the sidecar's doi_candidate) are not looked up again. Raises ris_emit.MetadataUnavailable."""
    for cand in front_matter_candidates((text or "")[:FRONT_MATTER_CHARS]):
        if cand in tried:
            continue
        status, m = candidate_match(cand, text, year_hint, author_hint)
        if status is not None:
            return status, m
    return None, None


def sidecar_doi_candidate(sidecar):
    """The text DOI extract_pdf_fulltext recorded as `doi_candidate` (W4-C; never `doi`, DEC-20),
    normalised; None when the sidecar has none or it is not a DOI."""
    raw = (sidecar or {}).get("doi_candidate") if isinstance(sidecar, dict) else None
    return _doi.normalise(raw) if isinstance(raw, str) and raw.strip() else None


# ------------------------------------------------------------------------------ writing
def update_sidecar(sidecar_path, sidecar_dict, match_meta):
    """Apply match metadata to the sidecar: `doi` set, title/year/journal/authors filled only when
    empty, everything else kept. Returns False (nothing written) when the DOI is not well formed
    (T7: the index keys on it). Written atomically. It does not go through lit_util.merge_sidecar
    on purpose (decided 2026-10-07, M123): it only fills empty fields on the record it read and
    writes the whole record atomically, which is what merge_sidecar would preserve anyway."""
    doi = (match_meta.get("doi") or "").strip()
    if not lit_util.is_valid_doi(doi):
        return False
    sd = sidecar_dict
    sd["doi"] = doi
    if not sd.get("title"):
        sd["title"] = match_meta["title"]
    if not sd.get("year"):
        sd["year"] = match_meta["year"]
    if not sd.get("journal"):
        sd["journal"] = match_meta["journal"]
    if not sd.get("authors") and match_meta["authors"]:
        sd["authors"] = match_meta["authors"]
    lit_util.atomic_write_json(sidecar_path, sd)
    return True


def is_preprint_doi(doi):
    d = (doi or "").lower()
    if d.startswith("10.1101/"):
        return bool(_BIORXIV_SUFFIX.match(d.split("/", 1)[1]))
    return d.startswith(PREPRINT_PREFIXES)


def ris_doi_of(ris_path):
    """The `.ris` DOI, normalised ('' when the file has none)."""
    return _doi.normalise(lit_util.parse_ris(ris_path).get("doi") or "") or ""


def apply_match(pdf_path, sidecar_path, sidecar, match_meta, today=None):
    """Write one accepted match (DEC-20). Returns (written, action). The `.ris` DOI is never
    overwritten: a `.ris` with another DOI keeps it, the sidecar takes the `.ris` DOI and stores the
    match as doi_published (the `.ris` is a preprint) or doi_unverified_match. Without a `.ris`, one
    is emitted through ris_emit.write_ris (overwrite=False; DEC-29 records it)."""
    today = today or date.today().isoformat()
    doi = (match_meta.get("doi") or "").strip()
    if not lit_util.is_valid_doi(doi):
        return False, "INVALID_DOI"
    ris_path = str(lit_util.companion_path(Path(pdf_path), ".ris"))
    if os.path.exists(ris_path):
        rdoi = ris_doi_of(ris_path)
        if rdoi and rdoi != doi:
            if sidecar is None:
                return False, f"kept: the .ris carries {rdoi} (no sidecar to note the match in)"
            key = "doi_published" if is_preprint_doi(rdoi) else "doi_unverified_match"
            if not (sidecar.get("doi") or "").strip():
                sidecar["doi"] = rdoi
            sidecar[key] = doi
            sidecar["doi_note"] = (f"{today}: fill_missing_dois matched {doi}; the .ris carries {rdoi} "
                                   f"and is kept (DEC-20)")
            lit_util.atomic_write_json(sidecar_path, sidecar)
            return True, key
        if sidecar is None:
            return False, "kept: no sidecar, and a .ris exists"
        update_sidecar(sidecar_path, sidecar, match_meta)
        return True, "sidecar" if rdoi else "sidecar; the .ris has no DOI and is kept"
    action = []
    if sidecar is not None:
        update_sidecar(sidecar_path, sidecar, match_meta)
        action.append("sidecar")
    meta = dict(match_meta.get("meta") or {})
    meta["doi"] = doi
    if ris_emit.write_ris(ris_path, ris_emit.build_ris(meta), overwrite=False):
        action.append("ris")
    return bool(action), "+".join(action) or "nothing written"


# ------------------------------------------------------------------------------ discovery
def discover_orphans(lib_dir):
    """Yield (pdf_filename, sidecar_path, sidecar_dict) for PDFs whose sidecar has an empty or
    missing DOI (sidecar_dict None when there is no sidecar)."""
    if not os.path.isdir(lib_dir):
        return
    for fn in sorted(os.listdir(lib_dir)):
        if not fn.lower().endswith(".pdf"):
            continue
        sidecar = os.path.join(lib_dir, fn[:-4] + ".fulltext.json")
        if not os.path.isfile(sidecar):
            yield fn, sidecar, None
            continue
        sd = load_sidecar(sidecar)
        if sd is None:
            continue
        if not (sd.get("doi") or "").strip():
            yield fn, sidecar, sd


def discover_projects(arg_project, arg_all):
    """[(name, lib_dir)] from the registry at CONFIG_PATH (active projects whose library exists).
    Raises ConfigError for a missing registry or an unknown project."""
    if not Path(CONFIG_PATH).exists():
        raise ConfigError(f"projects.json not found at {CONFIG_PATH}")
    cfg = lit_util.load_projects_config(CONFIG_PATH, missing_ok=True).get("projects", {})
    if arg_project and arg_project not in cfg:
        raise ConfigError(f"project '{arg_project}' not found in projects.json")
    out = []
    for name, p in cfg.items():
        if not p.get("active", True) or not p.get("lib_dir"):
            continue
        _base, lib, _data = lit_util.lib_paths(name, p)
        if lib.is_dir():
            out.append((name, lib))
    if arg_all:
        return out
    if arg_project:
        out = [(n, lib) for n, lib in out if n == arg_project]
        if not out:
            raise ConfigError(f"project '{arg_project}' is inactive or its library is missing")
        return out
    raise ConfigError("must specify --project NAME or --all")


def build_query_string(year, author, title):
    """Build the Crossref query.bibliographic string from the hints."""
    parts = []
    if author:
        parts.append(author)
    if title:
        parts.append(title)
    if year:
        parts.append(year)
    return " ".join(parts).strip()


def _fill_row(row, label, meta, top1, top2):
    row["status"] = label
    row["top1_score"] = f"{top1:.2f}" if top1 else ""
    row["top2_score"] = f"{top2:.2f}" if top2 else ""
    if meta:
        row["crossref_doi"] = meta["doi"]
        row["crossref_type"] = meta["type"]
        row["crossref_title"] = meta["title"]
        row["crossref_year"] = meta["year"]
        row["crossref_first_author"] = meta["first_family"]
        row["crossref_journal"] = meta["journal"]


def process_orphan(fn, sidecar_path, sidecar, polite_sleep=None):
    """Run one orphan through the rule. Returns (row, accepted_meta_or_review_meta).
    `polite_sleep` is ignored (litpipe.net paces Crossref)."""
    year, author, title_hint, klass = parse_filename_hints(fn)
    row = {k: "" for k in FIELDNAMES}
    row.update({"filename": fn, "parse_class": klass, "parsed_year": year or "",
                "parsed_author": author or "", "parsed_title": title_hint or "", "applied": False})

    if is_bundled_issue(fn, title_hint):
        row["status"] = "SKIP_BUNDLED_ISSUE"
        return row, None
    lib = os.path.dirname(sidecar_path)
    why = flag_reason(lib, fn[:-4])
    if why:
        row["status"] = "SKIP_IDENTITY_FLAG"
        row["note"] = why
        return row, None

    text, note = evidence_text(os.path.join(lib, fn), sidecar)
    row["note"] = note
    cand = sidecar_doi_candidate(sidecar)
    try:
        if cand:                                   # the sidecar's text DOI first (W5-C2, C062/C102)
            row["doi_candidate"] = cand
            c_status, c_meta = candidate_match(cand, text, year, author)
            if c_status == "HIGH":
                _fill_row(row, "HIGH", c_meta, 0, 0)
                row["basis"] = "doi_candidate"
                return row, c_meta
            if c_status:
                row["note"] = "; ".join(x for x in (row["note"], f"doi_candidate {cand} unconfirmed") if x)
            else:
                row["note"] = "; ".join(x for x in (row["note"], f"doi_candidate {cand} did not resolve") if x)
        fm_status, fm = front_matter_match(text, year, author, tried=(cand,) if cand else ())
    except ris_emit.MetadataUnavailable as e:
        row["status"] = f"ERR_META_UNAVAILABLE: {e}"[:200]
        return row, None
    if fm is not None:
        row["front_matter_doi"] = fm["doi"]
    if fm_status == "HIGH":
        _fill_row(row, "HIGH", fm, 0, 0)
        row["basis"] = "front_matter_doi"
        return row, fm
    if fm_status:
        row["note"] = "; ".join(x for x in (row["note"], f"front-matter DOI {fm['doi']} unconfirmed") if x)

    augmented_title = title_hint
    if sidecar and (not title_hint or len(title_hint.split()) < 4):
        sct = sidecar_title_hint(sidecar)
        if sct and len(sct.split()) > len((title_hint or "").split()):
            augmented_title = sct
            row["parsed_title"] = augmented_title

    if not author and not year and (not augmented_title or len(augmented_title.split()) < 3):
        row["status"] = "SKIP_NO_HINTS"
        return row, None
    query_str = build_query_string(year, author, augmented_title)
    if not query_str:
        row["status"] = "SKIP_NO_HINTS"
        return row, None

    items, status = crossref_query(query_str, top_n=ROWS)
    if status != "OK":
        row["status"] = status
        return row, None
    if not items:
        row["status"] = "NO_RESULT"
        return row, None
    preferred, _demoted = filter_by_type(items)
    label, meta, top1, top2 = score_match(year, author, preferred, items, evidence=text, query=query_str)
    _fill_row(row, label, meta, top1, top2)
    if label == "HIGH":
        row["basis"] = "strict_title+author+year"
    elif label == "EXCLUDED_ONLY":
        row["note"] = "; ".join(x for x in (row["note"], "every hit is a notice, review or commentary record") if x)
    return row, meta


# ------------------------------------------------------------------------------ reports
def report_run_id(directory, today=None):
    """YYYY-MM-DD for the first run of the day in `directory`, then YYYY-MM-DD.N: chosen so that no
    report of that id exists (dispatch 0.5 run-id rule)."""
    base = today or date.today().isoformat()
    rid, n = base, 1
    while any(os.path.exists(os.path.join(directory, f"{p}.{rid}.csv")) for p in REPORT_PREFIXES):
        n += 1
        rid = f"{base}.{n}"
    return rid


def write_report(rows, path, fieldnames):
    with open(path, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})


class _NoKVWrites:
    """ris_emit's state during a dry run (as import_downloads wraps it): kv reads pass through, kv
    writes (the doi.org agency cache `doi_ra`) are dropped, so a dry run leaves the pipeline state
    as it found it. Anything else is the inner state's."""

    def __init__(self, inner):
        self._inner = inner

    def kv_get(self, ns, key, default=None):
        return self._inner.kv_get(ns, key)

    def kv_set(self, ns, key, value, ttl_s=None):
        return None

    def __getattr__(self, name):
        return getattr(self._inner, name)


def _no_state_writes_in_a_dry_run(fn):
    """run_project's dry run writes no state: ris_emit.STATE is _NoKVWrites for its duration
    (restored after, also on an exception)."""
    @functools.wraps(fn)
    def wrapper(name, lib_dir, args):
        prev = ris_emit.STATE
        if not getattr(args, "execute", False):
            ris_emit.STATE = _NoKVWrites(ris_emit._state())
        try:
            return fn(name, lib_dir, args)
        finally:
            ris_emit.STATE = prev
    return wrapper


@_no_state_writes_in_a_dry_run
def run_project(name, lib_dir, args):
    """Fill one library. `args` needs execute, limit and (optionally) report_dir; min_confidence is
    retired (only HIGH is written). Returns the project totals. A dry run writes nothing: no
    sidecar, no `.ris`, no report without --report-dir, and no state (ris_emit's kv writes, the
    `doi_ra` cache, are dropped for its duration)."""
    execute = bool(getattr(args, "execute", False))
    limit = getattr(args, "limit", None)
    report_dir = getattr(args, "report_dir", None)
    lib_dir = Path(lib_dir)
    print(f"\n=== Project: {name} ===")
    print(f"  Library: {lib_dir}")
    orphans = list(discover_orphans(str(lib_dir)))
    print(f"  Orphans (sidecar.doi empty): {len(orphans)}")
    if limit:
        orphans = orphans[:limit]
        print(f"  Limited to first {len(orphans)} orphans")

    rows, applied_rows, review_rows, skip_rows = [], [], [], []
    tally = {"high": 0, "med": 0, "amb": 0, "low": 0, "skip": 0, "err": 0, "unresolved": 0}
    errors = []
    no_doi_ris = 0
    today = date.today().isoformat()
    for i, (fn, sidecar_path, sidecar) in enumerate(orphans, 1):
        row, meta = process_orphan(fn, sidecar_path, sidecar)
        status = row["status"]
        if status.startswith("SKIP_"):
            tally["skip"] += 1
            skip_rows.append(row)
        elif status.startswith("ERR_"):
            tally["err"] += 1
            errors.append(status.split(":", 1)[0])
        elif status == "NO_RESULT":
            tally["err"] += 1
        elif status == "HIGH":
            tally["high"] += 1
        elif status.startswith("MED"):
            tally["med"] += 1
            review_rows.append(row)
        elif status == "AMBIG":
            tally["amb"] += 1
            review_rows.append(row)
        elif status.startswith("LOW"):
            tally["low"] += 1
            review_rows.append(row)
        else:                                  # DEMOTED_ONLY, EXCLUDED_ONLY: for a human, never written
            tally["unresolved"] += 1
            review_rows.append(row)

        pdf_path = os.path.join(str(lib_dir), fn)
        if execute and meta and status in WRITABLE:
            try:
                written, action = apply_match(pdf_path, sidecar_path, sidecar, meta, today)
            except OSError as e:
                row["status"] = f"WRITE_ERROR_{e}"
                errors.append("WRITE_ERROR")
                print(f"  [{i}/{len(orphans)}] WRITE-ERROR {fn[:60]} ({e})")
            else:
                row["action"] = action
                if written:
                    row["applied"] = True
                    applied_rows.append(row)
                    no_doi_ris += action.startswith("sidecar; the .ris has no DOI")
                    print(f"  [{i}/{len(orphans)}] APPLIED {status:12s} {fn[:55]} -> {meta['doi']} ({action})")
                else:
                    row["status"] = f"{status}_{'INVALID_DOI' if action == 'INVALID_DOI' else 'NOT_WRITTEN'}"
                    print(f"  [{i}/{len(orphans)}] NOT WRITTEN {fn[:55]} (doi={meta['doi']!r}: {action})")
        else:
            tag = "DRY" if not execute else "SKIP"
            print(f"  [{i}/{len(orphans)}] {tag:5s} {status[:25]:25s} {fn[:50]:50s}"
                  f" {(meta['doi'] if meta else ''):<35}")
        rows.append(row)

    out_dir = Path(report_dir) if report_dir else (lib_dir if execute else None)
    written_reports = {}
    if out_dir is not None:
        out_dir.mkdir(parents=True, exist_ok=True)
        rid = report_run_id(str(out_dir), today)
        for prefix, subset in zip(REPORT_PREFIXES, (rows, applied_rows, review_rows, skip_rows)):
            if subset or prefix == "_doi_fill_report":
                path = out_dir / f"{prefix}.{rid}.csv"
                write_report(subset, path, FIELDNAMES)
                written_reports[prefix] = str(path)

    print(f"\n  --- Summary ({name}) ---")
    print(f"    Attempted:        {len(rows)}")
    print(f"    HIGH:             {tally['high']}")
    print(f"    MED (review):     {tally['med']}")
    print(f"    AMBIG (review):   {tally['amb']}")
    print(f"    LOW (review):     {tally['low']}")
    print(f"    Unresolved:       {tally['unresolved']} (only demoted or excluded hits; review)")
    print(f"    SKIPPED:          {tally['skip']}")
    print(f"    NO_RESULT/ERR:    {tally['err']}")
    print(f"    Applied:          {len(applied_rows)}")
    for prefix, path in written_reports.items():
        print(f"    {prefix.strip('_')}: {path}")
    if out_dir is None:
        print("    Reports: none written (dry run; pass --report-dir DIR to save them)")
    if applied_rows:
        print(f"\n  Next: python index_portfolio.py --project {name}   (picks up the new .ris files)")
    if no_doi_ris:
        print(f"  [!] {no_doi_ris} fill(s) landed in sidecars whose .ris has no DOI and was kept (curated or "
              f"pre-DEC-29). backfill_ris replaces such a file only with --force, and --force replaces "
              f"EVERY curated .ris in the library, EndNote edits included. Dry run first:\n"
              f"        python backfill_ris.py --lib-dir \"{lib_dir}\"\n"
              f"        python backfill_ris.py --lib-dir \"{lib_dir}\" --commit --force   "
              f"(only if every file it lists may be replaced)")

    return {"name": name, "attempted": len(rows), "high": tally["high"], "med": tally["med"],
            "amb": tally["amb"], "low": tally["low"], "skip": tally["skip"], "err": tally["err"],
            "unresolved": tally["unresolved"], "applied": len(applied_rows), "errors": errors,
            "reports": written_reports, "rows": rows}


def run(*, project=None, all_projects=False, execute=False, min_confidence="HIGH", limit=None,
        report_dir=None):
    """Fill the orphans of one project (or all active ones). Returns {exit_code, projects, reasons}."""
    res = {"exit_code": 0, "projects": [], "reasons": []}
    ris_emit.warn_if_default_email()
    print(f"Mode: {'EXECUTE' if execute else 'DRY RUN'}")
    if min_confidence and min_confidence != "HIGH":
        print(f"[fill_missing_dois] --min-confidence {min_confidence} is retired: section 3.6 accepts only "
              f"a front-matter DOI or a strict title + author + year match (HIGH); lower labels go to the "
              f"review report and are never written.")
    try:
        projects = discover_projects(project, all_projects)
    except ConfigError as e:
        print(f"[fill_missing_dois] {e}")
        res["exit_code"] = 1
        return res
    print(f"Projects to process: {len(projects)}  ({', '.join(n for n, _ in projects)})")

    a = SimpleNamespace(execute=execute, limit=limit, report_dir=report_dir)
    for name, lib in projects:
        res["projects"].append(run_project(name, lib, a))

    print("\n=== Portfolio summary ===")
    print(f"  {'project':22s} {'attempted':>10s} {'HIGH':>5s} {'MED':>5s}"
          f" {'AMBIG':>6s} {'LOW':>5s} {'SKIP':>5s} {'ERR':>5s} {'applied':>8s}")
    for t in res["projects"]:
        print(f"  {t['name']:22s} {t['attempted']:>10d} {t['high']:>5d} {t['med']:>5d}"
              f" {t['amb']:>6d} {t['low']:>5d} {t['skip']:>5d} {t['err']:>5d}"
              f" {t['applied']:>8d}")
    errs = {}
    for t in res["projects"]:
        for e in t["errors"]:
            errs[e] = errs.get(e, 0) + 1
    if errs:
        res["reasons"] = [f"{k}: {v} row(s)" for k, v in sorted(errs.items())]
        res["exit_code"] = 2
        transport = sum(v for k, v in errs.items() if k != "WRITE_ERROR")
        print(SUMMARY_MARKER + json.dumps({"reasons": res["reasons"], "aborted": None,
                                           "transport_failures": transport}, ensure_ascii=False), flush=True)
    return res


def main(argv=None):
    ap = _Parser(description="Recover DOIs for orphan PDFs (front-matter DOI, else a strict Crossref match).")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--project", help="Project name from projects.json")
    g.add_argument("--all", action="store_true", help="All active projects")
    ap.add_argument("--execute", action="store_true",
                    help="Write accepted (HIGH) DOIs: the sidecar, and a .ris where none exists "
                         "(default: dry run, nothing written)")
    ap.add_argument("--min-confidence", default="HIGH",
                    choices=["HIGH", "MED_NO_YEAR", "MED_AUTHOR_ONLY",
                             "MED_TITLE_STRONG",
                             "MED_AUTHOR_YEAR_MISMATCH", "MED_TYPE_MISMATCH"],
                    help="Retired: only HIGH matches are written (section 3.6); other values print a "
                         "notice and change nothing.")
    ap.add_argument("--limit", type=int, default=None,
                    help="Process only the first N orphans (smoke-test)")
    ap.add_argument("--report-dir", default=None,
                    help="Write the reports here (a dry run writes none without it; --execute "
                         "defaults to the library).")
    args = ap.parse_args(argv)
    res = run(project=args.project, all_projects=args.all, execute=args.execute,
              min_confidence=args.min_confidence, limit=args.limit, report_dir=args.report_dir)
    return res["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
