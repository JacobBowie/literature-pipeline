"""Import hand-downloaded PDFs (paywalled papers, interlibrary loans) from a Downloads folder into
one literature library: identify each paper, name it, and write its sidecar and `.ris`.

Library (one is required; there is no default library):
  --lib-dir PATH   the library directory itself; the report goes to its parent folder unless
                   --report-dir says otherwise;
  --project KEY    a project registered in projects.json (litpipe.config.load; the library through
                   lit_util.lib_paths, so a subproject key works); the report goes to the project's
                   own folder (lit_util.project_root) unless --report-dir says otherwise.

Per PDF in --downloads modified at or after --cutoff, in name order. The defaults (the Downloads
folder in your home directory, and today at local midnight) serve one workflow: papers saved by
a browser today. A shared drop folder or an older batch is the same tool with --downloads and
--cutoff set.
  0. A file without `%PDF` in its first 1,024 bytes (a web page saved as `.pdf`) is NOT_PDF:
     left where it is, before any reading or identity check (the extract_pdf_fulltext rule).
  1. Read up to 6 pages with PyMuPDF. Cover sheets are set aside: interlibrary-loan covers (the
     Title 17 copyright notice, ILLiad / RapidX fields; one arrived with two cover pages), Taylor &
     Francis "This article was downloaded by" pages, JSTOR covers, and any page that is little more
     than a copyright notice. The first 3 remaining pages are the article text, with boilerplate
     passages removed (EBSCO "Copyright of ... is the property of ...", the Title 17 notice,
     "Downloaded from ..." lines). A file matching a known misfetch fingerprint
     (unpaywall_fetch_v2.boilerplate_of) is BOILERPLATE.
  2. Identity. DOIs are read with litpipe.doi (article pages first, cover pages last) and resolved
     with ris_emit.resolve_meta (Crossref, DataCite, content negotiation), at most 4 per file. A
     DOI is accepted when its record's title is printed in the article text (litpipe.identity
     title similarity of at least 0.85). A record whose title is itself boilerplate (a copyright
     notice) never counts. When no DOI's title is confirmed, a Crossref bibliographic search on the
     article's opening text is tried, and its best record is accepted on the same title test.
     Failing both, a file that prints exactly one DOI, on its first article page, takes that DOI
     (litpipe.identity.check: the DOI is printed). A DOI printed only on a cover sheet is taken
     only when its record's title is printed on the article pages: an interlibrary-loan slip can
     carry its own, unrelated DOI, so a cover DOI is never a sole-DOI or review candidate. Otherwise
     a file with candidate article DOIs is IDENTITY_FLAG and one with none is UNKNOWN_LEAVE. A
     supplement (litpipe.identity.doc_kind) is IDENTITY_FLAG too. Flagged files stay in Downloads
     and are listed for review; --doi resolves one by hand.
  3. Dedup, against a holdings map built fresh (litpipe.holdings.build, no cache):
       DUP_DOI      the destination library holds the DOI with a PDF;
       DUP_IN_RUN   an earlier file in this run is the same DOI (never imported twice);
       HELD_ELSEWHERE  another library holds the DOI (a PDF or a text-only holding; an identity-
                    flagged file is not a holding): skipped, the file left in Downloads, the
                    holding library named in the report (decision C10). --import-held-elsewhere
                    imports such a copy anyway, for one run;
       DUP_TITLE / DUP_FUZZY  a destination PDF with no DOI on record has the same title (exact,
                    or similarity 0.85+) and no conflicting year;
       a text-only holding of the DOI in the destination (audit_portfolio.is_text_only_sidecar)
                    takes the PDF: the PDF gets the holding's stem and its sidecar gets has_pdf.
  4. Name: unpaywall_fetch_v2.build_filename (DEC-14/15) from the record's first personal author
     (a group placeholder such as "Writing Committee Members" is skipped; an author that leaves the
     name Unknown falls back to the record's own family name), year and title. Display strings go
     through litpipe.text.display_field, so no name carries markup. unpaywall_fetch_v2.resolve_dest
     keeps it collision-safe.
  5. With --execute, the companions first (decision C7): `<stem>.fulltext.json` (text through
     pdf_text_clean.clean_pdf_text, so no ligature survives; has_pdf and extracted_from_pdf true;
     the identity verdict) and `<stem>.ris` (ris_emit.build_ris / write_ris), then the PDF. An
     existing `.ris` is never replaced (DEC-29); an existing sidecar is kept and only gains
     has_pdf. When any write fails, what this run wrote is removed again (an existing sidecar is
     restored, a new `.ris` and its manifest record are removed), the PDF stays in Downloads and
     the row is ERR_WRITE.
  6. Cover sheets, identified by content (cover_tag), never by position: the interlibrary-loan
     slips (`ill`) and the publisher download notices (`tandf`, `jstor`) are not part of the
     paper. Their text never reaches the sidecar. With --execute the filed PDF is written without
     those pages (PyMuPDF, a new file put in place atomically) and the untouched original is kept
     under `<library>/_archive/originals/<new name>`; --keep-covers files the PDF whole. A page
     that is only a copyright notice is not stripped, and nothing is stripped when every scanned
     page looked like a cover.
Dry run is the default (--dry-run is accepted and changes nothing): nothing is moved and nothing
is written to the library, Downloads or the pipeline state; the report is still written, as
`_downloads_import_<run id>_DRYRUN.csv`, for review before --execute.

Every request goes through litpipe.net (via ris_emit): one identity (LITPIPE_EMAIL), pacing, the
ledger. A metadata source that cannot answer (ris_emit.MetadataUnavailable) is META_UNAVAILABLE:
counted, the file left in Downloads, the run continues.

Report `_downloads_import_<run id>[_DRYRUN].csv`, where the run id is the date for the first run of
the day and `<date>.N` after (as sweep names its artifacts), so a second run never overwrites the
first. Columns: source, size_kb, action, doi, year, first_au, title, new_name, dup_match, note
(the legacy ten), then identity, doc_kind, meta_source, held_elsewhere, ris, outcome, detail,
covers_stripped, archived_original. Actions: MOVED / WOULD_MOVE, DUP_DOI, DUP_IN_RUN,
HELD_ELSEWHERE, DUP_TITLE, DUP_FUZZY, IDENTITY_FLAG, UNKNOWN_LEAVE, META_UNAVAILABLE, BOILERPLATE,
NOT_PDF, ERR_READ, ERR_WRITE.

Exit codes: 0 the run completed (row outcomes are in the report); 1 usage or configuration (no
library, both or neither of --lib-dir and --project, an unknown project or missing registry, a
missing directory, a bad --cutoff, --doi without exactly one PDF or with a DOI that does not
resolve).

Usage:
  python import_downloads.py --lib-dir <library> --cutoff 2026-10-05T09:30            # dry run
  python import_downloads.py --lib-dir <library> --cutoff 2026-10-05T09:30 --execute
  python import_downloads.py --project <KEY> --report-dir <dir> --execute
  python import_downloads.py --lib-dir <library> --cutoff <time> --doi 10.xxxx/yyyy --execute
  python import_downloads.py --lib-dir <library> --downloads <folder> --cutoff 2026-09-01 --execute
"""
import argparse
import collections
import json
import os
import re
import shutil
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, time as dtime
from difflib import SequenceMatcher
from pathlib import Path

import lit_util
lit_util.utf8_stdout()

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import audit_portfolio as AP  # noqa: E402  (is_text_only_sidecar, identity_flag)
import ris_emit as R  # noqa: E402
import unpaywall_fetch_v2 as U  # noqa: E402  (build_filename, resolve_dest, boilerplate_of)
from litpipe import config, holdings  # noqa: E402
from litpipe import identity as _identity  # noqa: E402
from litpipe import text as _text  # noqa: E402
from litpipe.ledger import now_iso, redact, redact_obj  # noqa: E402
from litpipe.outcomes import Kind  # noqa: E402
from pdf_text_clean import clean_pdf_text  # noqa: E402

SCAN_PAGES = 6            # pages read to get past cover sheets (a two-page ILL cover was seen)
ARTICLE_PAGES = 3         # article pages used for DOIs and the identity check
MAX_DOI_LOOKUPS = 4       # distinct DOIs resolved per file
SEARCH_ROWS = 5           # Crossref: "2-5 rows might be enough" for a query
SNIPPET_CHARS = 300       # opening article text sent as query.bibliographic
TITLE_THRESHOLD = _identity.TITLE_THRESHOLD
DEST_KEY = "__import_destination__"
REPORT_FIELDS = ["source", "size_kb", "action", "doi", "year", "first_au", "title", "new_name",
                 "dup_match", "note", "identity", "doc_kind", "meta_source", "held_elsewhere", "ris",
                 "outcome", "detail", "covers_stripped", "archived_original"]
PDF_MAGIC = b"%PDF"
PDF_MAGIC_WINDOW = 1024   # extract_pdf_fulltext's rule: the magic within the first 1,024 bytes
# Cover tags whose pages are not part of the paper: interlibrary-loan slips and the publisher
# download notices cover_tag names. A 'copyright_notice' page is kept (it can be an article page
# with little text left once a watermark line is removed).
STRIP_COVER_TAGS = frozenset({"ill", "tandf", "jstor"})
ARCHIVE_ORIGINALS = ("_archive", "originals")   # under the library: originals of stripped PDFs
NO_LIBRARY = ("import_downloads: no library given; pass --lib-dir PATH or --project KEY "
              "(there is no default library)")


class UsageError(ValueError):
    """A usage or configuration problem: the CLI prints one line and exits 1."""


class PdfError(Exception):
    """PyMuPDF could not read the PDF."""


# ------------------------------------------------------------------------------ cover sheets
_ILL_SIGNALS = tuple(re.compile(p, re.IGNORECASE) for p in (
    r"protected\s+by\s+copyright\s+law",
    r"\btitle\s+17\b",
    r"inter-?\s?library\s+loan",
    r"\billiad\b",
    r"lending\s+string",
    r"\bborrower\s*:",
    r"\bpatron\s*:",
    r"\bill\s*(?:number|no\.|#)",
    r"\brapid\s*(?:x|ill)\b",
    r"\bodyssey\b",
    r"\btipasa\b",
    r"document\s+delivery",
    r"\bmax\s*cost\b",
    r"\btransaction\s+(?:number|date)\b",
    r"\bcall\s*#",
))
_TANDF_COVER = re.compile(r"this\s+article\s+was\s+downloaded\s+by|please\s+scroll\s+down\s+for\s+article",
                          re.IGNORECASE)
_JSTOR_COVER = re.compile(r"\bjstor\b.{0,2000}?(?:terms\s+(?:and|&)\s+conditions\s+of\s+use|stable\s+url)"
                          r"|(?:terms\s+(?:and|&)\s+conditions\s+of\s+use|stable\s+url).{0,2000}?\bjstor\b",
                          re.IGNORECASE | re.DOTALL)
# Boilerplate passages removed from article pages before any DOI, title or search use. EBSCO is
# handled here, not as a cover: its full-text PDFs put a citation block (with the title, and often
# the DOI) above the article on page 1, and its copyright notice under the article's last line.
_PASSAGES = re.compile(
    r"copyright\s+of\s+.{0,300}?\s+is\s+the\s+property\s+of\s+.{0,800}?"
    r"(?:individual\s+use|written\s+permission)[^.\n]*\.?"                       # EBSCO
    r"|(?:notice\s*:?\s*)?this\s+material\s+may\s+be\s+protected\s+by\s+copyright\s+law"
    r"[^\n]*"                                                                     # the Title 17 notice
    r"|this\s+article\s+was\s+downloaded\s+by\s*:?[^\n]*"                         # Taylor & Francis
    r"|^[ \t]*downloaded\s+(?:from|via|by)\b[^\n]*",                              # per-page watermarks
    re.IGNORECASE | re.DOTALL | re.MULTILINE)
COVER_MIN_CHARS = 200     # a page left with fewer non-space characters after the passages go is a notice


def _nonspace(s):
    return sum(1 for ch in s if not ch.isspace())


def strip_boilerplate(text):
    """Page text with the boilerplate passages removed."""
    return _PASSAGES.sub(" ", text or "")


def cover_tag(page_text):
    """Why a page is a cover sheet, or '': 'ill' (two or more interlibrary-loan signals), 'tandf',
    'jstor', or 'copyright_notice' (a page that is little more than a boilerplate passage, such as
    an EBSCO or Title 17 notice on a page of its own)."""
    t = page_text or ""
    if sum(1 for rx in _ILL_SIGNALS if rx.search(t)) >= 2:
        return "ill"
    if _TANDF_COVER.search(t):
        return "tandf"
    if _JSTOR_COVER.search(t):
        return "jstor"
    stripped = strip_boilerplate(t)
    if stripped != t and _nonspace(stripped) < COVER_MIN_CHARS:
        return "copyright_notice"
    return ""


# Records whose title is a notice rather than a work: a Crossref title search on cover text found
# one ("Copyright (c)2022 by ... All rights reserved") and filed it as a paper.
_BOILERPLATE_TITLE = re.compile(
    r"^\s*(?:copyright\b|\(c\)|©)|all\s+rights\s+reserved|is\s+the\s+property\s+of"
    r"|protected\s+by\s+copyright\s+law"
    r"|^\s*(?:front|back)\s+matter\s*$|^\s*issue\s+information\s*$|^\s*masthead\s*$"
    r"|^\s*editorial\s+board\s*$|^\s*table\s+of\s+contents\s*$|^\s*cover\s*$", re.IGNORECASE)


def is_boilerplate_record(meta):
    return bool(_BOILERPLATE_TITLE.search((meta or {}).get("title") or ""))


# ------------------------------------------------------------------------------ PDF reading
@dataclass
class Scan:
    n_pages: int
    pages: list                       # raw text of the scanned pages
    covers: dict                      # page index -> cover tag
    article: list                     # indices of the article pages used

    @property
    def article_text(self):
        return "\n".join(strip_boilerplate(self.pages[i]) for i in self.article)

    @property
    def first_article_text(self):
        return strip_boilerplate(self.pages[self.article[0]]) if self.article else ""

    @property
    def cover_text(self):
        return "\n".join(self.pages[i] for i in sorted(self.covers))

    def strip_pages(self):
        """Indices of the cover pages that are not part of the paper (STRIP_COVER_TAGS), chosen by
        content. Empty when every scanned page looked like a cover (the scan then reads them all
        as the article), and never every page of the document."""
        if not self.pages or len(self.covers) >= len(self.pages):
            return []
        idx = sorted(i for i, tag in self.covers.items() if tag in STRIP_COVER_TAGS)
        return idx if len(idx) < self.n_pages else []


def is_pdf_file(path):
    """(True, head) when `%PDF` is in the file's first PDF_MAGIC_WINDOW bytes, else (False, head).
    Raises OSError."""
    with open(path, "rb") as fh:
        head = fh.read(PDF_MAGIC_WINDOW)
    return PDF_MAGIC in head, head


def scan_pdf(path) -> Scan:
    """The first SCAN_PAGES pages, cover sheets set aside. Raises PdfError."""
    try:
        import pymupdf
        doc = pymupdf.open(str(path))
    except Exception as e:
        raise PdfError(f"{type(e).__name__}: {e}") from None
    try:
        n = doc.page_count
        pages = [doc[i].get_text() for i in range(min(SCAN_PAGES, n))]
    except Exception as e:
        raise PdfError(f"{type(e).__name__}: {e}") from None
    finally:
        doc.close()
    covers = {}
    article = []
    for i, t in enumerate(pages):
        tag = cover_tag(t)
        if tag:
            covers[i] = tag
        elif len(article) < ARTICLE_PAGES:
            article.append(i)
    if not article:                   # every scanned page looked like a cover: read them all
        article = [i for i in range(len(pages))][:ARTICLE_PAGES]
    return Scan(n, pages, covers, article)


def full_text(path, skip=()) -> tuple:
    """(raw text of every page not in `skip`, page count, pages used). Raises PdfError.
    make_sidecar cleans it (the one place sidecar text is written)."""
    skip = set(skip)
    try:
        import pymupdf
        doc = pymupdf.open(str(path))
    except Exception as e:
        raise PdfError(f"{type(e).__name__}: {e}") from None
    try:
        parts = [p.get_text() for i, p in enumerate(doc) if i not in skip]
        n = doc.page_count
    except Exception as e:
        raise PdfError(f"{type(e).__name__}: {e}") from None
    finally:
        doc.close()
    return "\n\n".join(parts), n, len(parts)


def write_without_pages(src, drop, out):
    """Write `src` without the pages in `drop` to `out` (a new file; `src` is never written).
    Raises PdfError or OSError."""
    import pymupdf
    try:
        doc = pymupdf.open(str(src))
    except Exception as e:
        raise PdfError(f"{type(e).__name__}: {e}") from None
    try:
        keep = [i for i in range(doc.page_count) if i not in set(drop)]
        doc.select(keep)
        doc.save(str(out), garbage=3, deflate=True)
    except OSError:
        raise
    except Exception as e:
        raise PdfError(f"{type(e).__name__}: {e}") from None
    finally:
        doc.close()


# ------------------------------------------------------------------------------ identity
@dataclass
class Ident:
    status: str                       # OK, FLAG, UNKNOWN, META_UNAVAILABLE
    doi: str = ""
    meta: dict = field(default_factory=dict)
    source: str = ""
    verdict: object = None            # litpipe.identity.Verdict
    how: str = ""                     # doi_article, doi_cover, title_search, sole_doi, --doi
    detail: str = ""
    kind: object = None               # the Kind of a metadata failure


def crossref_search(query):
    """Crossref /works?query.bibliographic items (ris_emit's client: litpipe.net, typed errors).
    Raises ris_emit.MetadataUnavailable when Crossref could not answer; [] when nothing matched."""
    body = R._get_json(R.CROSSREF_SEARCH, "crossref",
                       params={"query.bibliographic": query, "rows": SEARCH_ROWS})
    if not isinstance(body, dict):
        return []
    return ((body.get("message") or {}).get("items")) or []


def _snippet(scan):
    s = " ".join(scan.first_article_text.split())
    return s[:SNIPPET_CHARS]


def _short(meta):
    return (meta.get("title") or "")[:60]


STRICT_MIN_CHARS, STRICT_MIN_WORDS = 40, 6     # fill_missing_dois section 3.6: a shorter title matches anything


def _specific_title(title):
    t = _text.normalise_title(title or "")
    return len(t) >= STRICT_MIN_CHARS and len(t.split()) >= STRICT_MIN_WORDS


def identify(scan, forced=None) -> Ident:
    """Which work the PDF is (module docstring, step 2). `forced` is a resolved (doi, meta, source)
    from --doi. MetadataUnavailable is returned as status META_UNAVAILABLE, never as 'no match'."""
    art = scan.article_text
    head = scan.first_article_text      # where a paper prints its own title; a cited title sits later
    if forced is not None:
        d, meta, src = forced
        v = _identity.check(art, d, queue_title=meta.get("title"))
        return Ident("OK", d, meta, src, v, "--doi", f"identity {v.decision} (overridden by --doi)")
    tried, unconfirmed = [], []
    page1 = set(holdings.extract_dois(scan.first_article_text))
    art_dois = holdings.extract_dois(art)
    cands = [(d, "article") for d in art_dois]
    cands += [(d, "cover") for d in holdings.extract_dois(scan.cover_text) if d not in art_dois]
    try:
        for d, where in cands[:MAX_DOI_LOOKUPS]:
            meta, src = R.resolve_meta(d)
            if not meta:
                tried.append(f"{d}: no record ({src})")
                continue
            if is_boilerplate_record(meta):
                tried.append(f"{d}: boilerplate record '{_short(meta)}'")
                continue
            score = _identity.title_similarity(meta.get("title"), head)
            if score >= TITLE_THRESHOLD:
                v = _identity.check(art, d, queue_title=meta.get("title"))
                return Ident("OK", meta.get("doi") or d, meta, src, v, f"doi_{where}",
                             f"title {score:.2f}")
            unconfirmed.append((d, where, meta, src, score))
            tried.append(f"{d}: title {score:.2f} '{_short(meta)}'")

        snippet = _snippet(scan)
        if len(snippet) >= 20:
            best, best_score = None, 0.0
            for it in crossref_search(snippet):
                m = R.crossref_meta(it)
                if not m.get("title") or not m.get("doi") or is_boilerplate_record(m) \
                        or not _specific_title(m["title"]):
                    continue
                s = _identity.title_similarity(m["title"], head)
                if s > best_score:
                    best, best_score = m, s
            if best is not None and best_score >= TITLE_THRESHOLD:
                v = _identity.check(art, best["doi"], queue_title=best["title"])
                return Ident("OK", best["doi"], best, "crossref_search", v, "title_search",
                             f"title {best_score:.2f}")
            if best is not None:
                tried.append(f"search: best title {best_score:.2f} '{_short(best)}'")
    except R.MetadataUnavailable as e:
        return Ident("META_UNAVAILABLE", detail=str(e), kind=e.kind)

    if len(cands) == 1 and len(unconfirmed) == 1:
        d, where, meta, src, score = unconfirmed[0]
        if where == "article" and d in page1:
            v = _identity.check(art, d, queue_title=meta.get("title"))
            if v.decision == _identity.Decision.OK:
                return Ident("OK", meta.get("doi") or d, meta, src, v, "sole_doi",
                             f"the only DOI printed; title not confirmed ({score:.2f})")
    # A DOI printed only on a cover sheet is never a review candidate: an interlibrary-loan slip
    # can print its own, unrelated DOI (one named a 1985 conference paper as a 2012 law article),
    # so with its title not on the article pages it says nothing about this file.
    article_unconfirmed = [u for u in unconfirmed if u[1] == "article"]
    if article_unconfirmed:
        d = article_unconfirmed[0][0]
        v = _identity.check(art, d, queue_title=article_unconfirmed[0][2].get("title"))
        return Ident("FLAG", "", {}, "", v, "", "; ".join(tried))
    if unconfirmed:
        tried.append("cover-sheet DOI(s) not taken: their record titles are not printed in the article")
    why = "; ".join(tried) if tried else ("no DOI and no usable text (a scan? OCR it, or pass --doi)"
                                           if len(_snippet(scan)) < 20 else "no DOI printed")
    return Ident("UNKNOWN", detail=why)


# ------------------------------------------------------------------------------ names
_GROUP_WORDS = re.compile(
    r"\b(?:committee|members|group|collaboration|consortium|investigators|society|association|council"
    r"|college|organi[sz]ation|panel|network|team|task\s+force|working|writing|study|initiative"
    r"|foundation|institute|federation|academy|alliance|board|authors)\b", re.IGNORECASE)


def _is_group(a):
    fam = (a.get("family") or "").strip()
    return not (a.get("given") or "").strip() and bool(
        _GROUP_WORDS.search(fam) or re.search(r"[*#@&]", fam))


def filename_author(meta):
    """The record's first personal author (a group placeholder such as "Writing Committee
    Members*" is skipped), else its first author; None when it has none."""
    authors = [a for a in (meta.get("authors") or []) if (a.get("family") or "").strip()]
    return next((a for a in authors if not _is_group(a)), authors[0] if authors else None)


def proposed_name(meta):
    """(filename, first author's family) by unpaywall_fetch_v2.build_filename (DEC-14/15)."""
    title = _text.display_field(meta.get("title"))
    year = meta.get("year") or ""
    a = filename_author(meta)
    if a is None:
        return U.build_filename(year, "", title), ""
    fam, giv = _text.display_field(a.get("family")), _text.display_field(a.get("given"))
    fn = U.build_filename(year, f"{fam}, {giv}" if giv else fam, title)
    if U.last_name(f"{fam}, {giv}" if giv else fam) == "Unknown":
        fn = R.canonical_stem(year, fam, title) + ".pdf"       # the record's own family name
    return fn, fam


# ------------------------------------------------------------------------------ library and holdings
def resolve_library(lib_dir=None, project=None, cfg=None):
    """(library, default report dir) from --lib-dir or --project (exactly one). Raises UsageError."""
    if lib_dir and project:
        raise UsageError("import_downloads: pass --lib-dir or --project, not both")
    if not lib_dir and not project:
        raise UsageError(NO_LIBRARY)
    if project:
        projects = config.load(cfg).get("projects") or {}
        p = projects.get(project)
        if not isinstance(p, dict):
            raise UsageError(f"import_downloads: project {project!r} is not registered in projects.json "
                             f"({config.CONFIG_PATH})")
        if not p.get("lib_dir"):
            raise UsageError(f"import_downloads: project {project!r} declares no lib_dir")
        lib = lit_util.lib_paths(project, p)[1]
        report = lit_util.project_root(project, p)
    else:
        lib = Path(lib_dir)
        report = Path(os.path.abspath(str(lib))).parent
    if not lib.is_dir():
        raise UsageError(f"import_downloads: library not found: {lib}")
    return lib, report


def _norm(p):
    return os.path.normcase(os.path.abspath(str(p)))


def _same_path(a, b):
    """True when `a` and `b` name one file: one spelling (case-folded), or one file reached two
    ways (a junction, a symbolic link, a short name)."""
    if _norm(a) == _norm(b):
        return True
    try:
        return os.path.samefile(a, b)
    except OSError:
        return False


def _registry_with_destination(cfg, lib):
    """The registry plus the destination library, so one holdings scan covers both. An unregistered
    --lib-dir enters as an absolute `parent` (lit_util.lib_rel joins it onto the root, and an
    absolute path replaces the root); a registered one is deduplicated by holdings.libraries."""
    base = config.load(cfg)
    projects = dict(base.get("projects") or {})
    absolute = Path(os.path.abspath(str(lib)))
    projects[DEST_KEY] = {"parent": str(absolute.parent), "lib_dir": absolute.name}
    return {**base, "projects": projects}


def _read_json(path):
    try:
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
        return d if isinstance(d, dict) else None
    except (OSError, ValueError):
        return None


def _flagged_pdf(pdf_path):
    """An identity-FLAG or SUPPLEMENT verdict on the PDF's .identity.json or .fulltext.json."""
    for ext in (".identity.json", holdings.SIDECAR_SUFFIX):
        rec = _read_json(lit_util.companion_path(pdf_path, ext))
        if rec and AP.identity_flag(rec):
            return True
    return False


_RIS_TI = re.compile(r"(?m)^(?:TI|T1)\s+-\s?(.+?)\s*$")
_RIS_PY = re.compile(r"(?m)^(?:PY|Y1)\s+-\s?(\d{4})")


class Destination:
    """The destination library as the holdings map sees it, plus this run's own imports."""

    def __init__(self, lib, hm):
        self.lib, self.hm, self.key = Path(lib), hm, _norm(lib)
        self._doi_less = None

    def here(self, h):
        return _norm(h.library) == self.key

    def pdf_holding(self, doi):
        for h in self.hm.records(doi):
            if self.here(h) and h.kind == holdings.PDF and not _flagged_pdf(h.path):
                return h.path
        return None

    def text_only(self, doi):
        """(sidecar path, record) of a text-only holding of `doi` here, by the instruments' predicate."""
        for h in self.hm.records(doi):
            if self.here(h) and h.kind == holdings.TEXT_ONLY:
                rec = _read_json(h.path)
                pdf = Path(str(h.path)[:-len(holdings.SIDECAR_SUFFIX)] + ".pdf")
                if rec is not None and AP.is_text_only_sidecar(rec) and not pdf.exists():
                    return Path(h.path), rec
        return None

    def elsewhere_holdings(self, doi):
        """Content holdings of `doi` in other libraries; an identity-flagged PDF is not one."""
        return [h for h in self.hm.content(doi)
                if not self.here(h) and not (h.kind == holdings.PDF and _flagged_pdf(h.path))]

    def elsewhere(self, doi):
        return [str(h.path) for h in self.elsewhere_holdings(doi)]

    def doi_less_titles(self):
        """[(pdf name, normalised title, year)] for destination PDFs with no DOI on record."""
        if self._doi_less is None:
            with_doi = {Path(h.path).name.casefold() for d in self.hm.dois() for h in self.hm.records(d)
                        if self.here(h) and h.kind == holdings.PDF}
            out = []
            for p in sorted(self.lib.iterdir()):
                if p.suffix.lower() != ".pdf" or p.name.casefold() in with_doi:
                    continue
                title, year = "", ""
                rec = _read_json(lit_util.companion_path(p, holdings.SIDECAR_SUFFIX))
                if rec and not AP.identity_flag(rec):
                    title, year = str(rec.get("title") or ""), str(rec.get("year") or "")
                if not title:
                    try:
                        ris = lit_util.companion_path(p, ".ris").read_text(encoding="utf-8", errors="replace")
                        m, y = _RIS_TI.search(ris), _RIS_PY.search(ris)
                        title, year = (m.group(1) if m else ""), (y.group(1) if y else year)
                    except OSError:
                        pass
                t = _text.normalise_title(title)
                if t:
                    out.append((p.name, t, year[:4]))
            self._doi_less = out
        return self._doi_less

    def title_dup(self, title, year):
        """('DUP_TITLE' | 'DUP_FUZZY', pdf name) against DOI-less destination PDFs, or None."""
        t = _text.normalise_title(title)
        if not t:
            return None
        fuzzy = None
        for name, lt, ly in self.doi_less_titles():
            if ly.isdigit() and str(year).isdigit() and abs(int(ly) - int(year)) > 1:
                continue                      # a later edition under the same title is another work
            if lt == t:
                return "DUP_TITLE", name
            if fuzzy is None and SequenceMatcher(None, lt, t).ratio() >= TITLE_THRESHOLD:
                fuzzy = ("DUP_FUZZY", name)
        return fuzzy


# ------------------------------------------------------------------------------ writing
def make_sidecar(text, meta, source, doi, source_filename, ident, kind):
    """A new `.fulltext.json`: display-form metadata, cleaned text, has_pdf and the verdict."""
    rec = {
        "pmcid": "", "pmid": "",
        "doi": doi,
        "title": _text.display_field(meta.get("title")), "subtitle": "",
        "year": str(meta.get("year") or ""),
        "journal": _text.display_field(meta.get("container")),
        "authors": [{"given": _text.display_field(a.get("given")), "surname": _text.display_field(a.get("family")),
                     "source": source} for a in (meta.get("authors") or [])],
        "abstract": _text.abstract_field(meta.get("abstract")),
        "sections": [], "figures": [], "tables": [], "formulas": [],
        "n_formulas": 0, "formula_failures": {},
        "text": clean_pdf_text(text or ""),
        "has_pdf": True,
        "extracted_from_pdf": True,
        "extractor": "PyMuPDF",
        "text_cleaned": True,
        "metadata_source": f"{source}_via_downloads_import",
        "metadata_backfilled_at": now_iso(),
        "source_filename": source_filename,
        "doc_kind": str(kind),
        "identity_method": ident.how,
    }
    if ident.verdict is not None:
        rec.update(ident.verdict.as_dict())
    return redact_obj(rec)


def _mark_has_pdf(sidecar_path, rec, source_filename):
    rec = dict(rec)
    rec["has_pdf"] = True
    rec["pdf_source_filename"] = source_filename
    lit_util.atomic_write_json(str(sidecar_path), rec)


class WriteFailed(Exception):
    """A write of one import failed; what the import had written was removed again."""


class _Undo:
    """The writes of one import, undone newest first when a later step fails, so a failed import
    leaves no orphan companion and the PDF where it was."""

    def __init__(self):
        self._steps = []

    def add(self, label, fn):
        self._steps.append((label, fn))

    def run(self):
        """Undo every recorded write; returns the cleanup failures (normally [])."""
        failed = []
        for label, fn in reversed(self._steps):
            try:
                fn()
            except OSError as e:
                failed.append(f"{label}: {type(e).__name__}: {e}")
        self._steps = []
        return failed


def _restore_bytes(path, data):
    tmp = f"{path}.restore.tmp"
    with open(tmp, "wb") as fh:
        fh.write(data)
    lit_util._replace_with_retry(tmp, path)


def _remove(path):
    if os.path.lexists(path):
        os.remove(path)


def _forget_ris(path, key):
    """Remove a .ris this import wrote, and its DEC-29 manifest record."""
    _remove(path)
    R._kv_set(R.RIS_NS, key, None)


def _unique(path):
    """`path`, or `<stem>.<n><suffix>` for the first n that names no file."""
    path = Path(path)
    cand, n = path, 2
    while cand.exists():
        cand = path.with_name(f"{path.stem}.{n}{path.suffix}")
        n += 1
    return cand


def file_import(src, dest_pdf, *, sidecar_path, sidecar_rec, new_rec, ris_path, ris_text, strip=(),
                archive_dir=None):
    """Write the companions, then the PDF (decision C7). Returns (ris status, archived original or
    None). `strip`: page indices to leave out of the filed PDF; the untouched original then goes to
    `archive_dir`. Raises WriteFailed after undoing every write of this import: an existing sidecar
    is restored byte for byte, a new sidecar or `.ris` (and its manifest record) is removed, and
    the PDF is back where it was."""
    undo = _Undo()
    src, dest_pdf, sidecar_path, ris_path = Path(src), Path(dest_pdf), Path(sidecar_path), Path(ris_path)
    step = "sidecar"
    try:
        if sidecar_path.exists():
            before = sidecar_path.read_bytes()
            undo.add("restore the sidecar", lambda: _restore_bytes(sidecar_path, before))
        else:
            undo.add("remove the new sidecar", lambda: _remove(sidecar_path))
        if sidecar_rec is not None:
            _mark_has_pdf(sidecar_path, sidecar_rec, src.name)
        else:
            lit_util.atomic_write_json(str(sidecar_path), new_rec)

        step = ".ris"
        ris = "EXISTS"
        if not ris_path.exists():
            # the undo is registered before the write: write_ris can raise after the file exists (its
            # manifest step), which would leave an orphan .ris; a False return wrote nothing of ours
            key = R.manifest_key(str(ris_path))
            ours = {"ris": True}
            undo.add("remove the new .ris", lambda: _forget_ris(ris_path, key) if ours["ris"] else None)
            if R.write_ris(str(ris_path), ris_text, overwrite=False):
                ris = "WROTE"
            else:
                ours["ris"] = False
                ris = "NOT_WRITTEN"

        archived = None
        if strip:                         # also when dest is src (a Downloads folder that is the library)
            step = "the PDF without its cover pages"
            tmp = dest_pdf.with_name(dest_pdf.name + ".importing.tmp")
            undo.add("remove the temporary PDF", lambda: _remove(tmp))
            write_without_pages(src, strip, tmp)
            step = "the original into the archive"
            archive_dir = Path(archive_dir)
            archive_dir.mkdir(parents=True, exist_ok=True)
            archived = _unique(archive_dir / dest_pdf.name)

            def back():
                if src.exists():          # the move did not complete: drop a partial copy
                    _remove(archived)
                else:
                    shutil.move(str(archived), str(src))
            undo.add("return the original to its folder", back)
            shutil.move(str(src), str(archived))
            step = "the PDF"
            lit_util._replace_with_retry(tmp, dest_pdf)
        elif _norm(dest_pdf) != _norm(src):
            step = "the PDF"
            undo.add("remove a partial PDF copy",
                     lambda: _remove(dest_pdf) if src.exists() and dest_pdf.exists() else None)
            shutil.move(str(src), str(dest_pdf))
        return ris, archived
    except (OSError, PdfError) as e:
        failed = undo.run()
        why = f"writing {step}: {type(e).__name__}: {e}"
        if failed:
            why += "; cleanup failed: " + "; ".join(failed)
        raise WriteFailed(why) from None


class _NoKVWrites:
    """ris_emit's state during a dry run: kv reads pass through, kv writes (the doi.org agency
    cache) are dropped, so a dry run leaves the pipeline state as it found it."""

    def __init__(self, inner):
        self._inner = inner

    def kv_get(self, ns, key, default=None):
        return self._inner.kv_get(ns, key)

    def kv_set(self, ns, key, value, ttl_s=None):
        return None


# ------------------------------------------------------------------------------ the run
def _parse_cutoff(cutoff, today):
    if cutoff is None or cutoff == "":
        return datetime.combine(today, dtime.min)
    if isinstance(cutoff, datetime):
        dt = cutoff
    else:
        try:
            dt = datetime.fromisoformat(str(cutoff))
        except ValueError:
            raise UsageError(f"import_downloads: --cutoff is not an ISO date or datetime: {cutoff!r}") from None
    if dt.tzinfo is not None:
        dt = dt.astimezone().replace(tzinfo=None)
    return dt


def _row(p, size_kb, action, **kw):
    r = dict.fromkeys(REPORT_FIELDS, "")
    r.update(source=p.name, size_kb=size_kb, action=action)
    r.update({k: ("" if v is None else v) for k, v in kw.items()})
    r["detail"] = redact(str(r["detail"]))[:500] if r["detail"] else ""
    return r


def report_path(rep_dir, today, execute):
    """`_downloads_import_<run id>[_DRYRUN].csv` in `rep_dir`: the run id is the date for the first
    run of the day and `<date>.N` after (sweep's run-id rule), so no run overwrites another's report."""
    stamp = today.strftime("%Y-%m-%d")
    suffix = "" if execute else "_DRYRUN"
    n = 1
    while True:
        run_id = stamp if n == 1 else f"{stamp}.{n}"
        p = Path(rep_dir) / f"_downloads_import_{run_id}{suffix}.csv"
        if not p.exists():
            return p
        n += 1


def run(*, execute=False, cutoff=None, downloads=None, lib_dir=None, project=None, report_dir=None,
        doi=None, cfg=None, today=None, import_held_elsewhere=False, keep_covers=False) -> dict:
    """Import one cutoff's PDFs (stage function; module docstring). Raises UsageError for a usage or
    configuration problem. Returns the mode, the paths, the per-file rows, the action counts and the
    report path."""
    today = today or date.today()
    lib, default_report = resolve_library(lib_dir, project, cfg)
    # The default folder is one workflow (a browser's downloads, today); --downloads names any other.
    src_dir = Path(downloads) if downloads else Path.home() / "Downloads"
    rep_dir = Path(report_dir) if report_dir else default_report
    for label, d in (("--downloads", src_dir), ("--report-dir", rep_dir)):
        if not d.is_dir():
            raise UsageError(f"import_downloads: {label} does not exist: {d}")
    cut = _parse_cutoff(cutoff, today)

    cands = sorted((p for p in src_dir.iterdir()
                    if p.is_file() and p.suffix.lower() == ".pdf"
                    and datetime.fromtimestamp(p.stat().st_mtime) >= cut),
                   key=lambda p: p.name.casefold())
    mode = "EXECUTE" if execute else "DRY-RUN"
    print(f"== import downloads [{mode}] ==")
    print(f"  downloads: {src_dir}\n  library:   {lib}\n  cutoff:    {cut.isoformat(timespec='minutes')}")
    print(f"  candidate PDFs: {len(cands)}")

    prev_state = R.STATE
    if not execute:
        R.STATE = _NoKVWrites(R._state())
    try:
        forced = None
        if doi:
            if len(cands) != 1:
                raise UsageError(f"import_downloads: --doi requires exactly one candidate PDF, found "
                                 f"{len(cands)}; narrow --cutoff until it selects only the file you mean")
            try:
                meta, src = R.resolve_meta(doi)
            except R.MetadataUnavailable as e:
                raise UsageError(f"import_downloads: --doi {doi}: metadata unavailable: {e}") from None
            if not meta:
                raise UsageError(f"import_downloads: --doi {doi} did not resolve ({src})")
            forced = (meta.get("doi") or doi, meta, src)
            print(f"  --doi override: {_short(meta)}")

        rows = []
        if cands:
            registry = _registry_with_destination(cfg, lib)
            print(f"  building the holdings map over {len(holdings.libraries(registry))} libraries "
                  f"(no cache; a cold build can take a minute) ...")
            hm = holdings.build(registry, use_cache=False, write_cache=False)
            dest = Destination(lib, hm)
            seen = {}                  # doi -> source name imported (or planned) this run
            written = set()            # destination paths used this run
            for i, p in enumerate(cands, 1):
                row = _one(p, dest, execute, forced, seen, written,
                           import_held_elsewhere=import_held_elsewhere, keep_covers=keep_covers)
                rows.append(row)
                arrow = f" -> {row['new_name']}" if row["new_name"] else ""
                print(f"  [{i}/{len(cands)}] {row['action']:<16} {p.name[:45]}{arrow}")
    finally:
        R.STATE = prev_state

    report = report_path(rep_dir, today, execute)
    lit_util.atomic_write_csv(str(report), rows, fieldnames=REPORT_FIELDS)
    counts = collections.Counter(r["action"] for r in rows)
    print(f"\n=== {mode} done ===")
    for k in sorted(counts):
        print(f"  {k:<16} {counts[k]}")
    if counts.get("META_UNAVAILABLE"):
        print("  metadata unavailable: those files stay in Downloads; run again later")
    if counts.get("NOT_PDF"):
        print("  not a PDF (a saved web page?): those files stay in Downloads; download the PDF itself")
    if counts.get("HELD_ELSEWHERE"):
        print("  held in another library: those files stay in Downloads (--import-held-elsewhere "
              "imports them anyway)")
    if counts.get("ERR_WRITE"):
        print("  a write failed: nothing of those files was filed; they stay in Downloads")
    print(f"  report: {report}")
    return {"mode": mode, "downloads": str(src_dir), "lib_dir": str(lib), "report_dir": str(rep_dir),
            "cutoff": cut.isoformat(), "rows": rows, "counts": dict(counts), "report": str(report)}


def _one(p, dest, execute, forced, seen, written, *, import_held_elsewhere=False, keep_covers=False):
    """Process one candidate PDF; returns its report row."""
    size_kb = p.stat().st_size // 1024
    try:
        ok, head = is_pdf_file(p)
    except OSError as e:
        return _row(p, size_kb, "ERR_READ", note="unreadable; left in Downloads", outcome=Kind.ERROR.value,
                    detail=str(e))
    if not ok:
        return _row(p, size_kb, "NOT_PDF", note="not a PDF (no %PDF in its first 1,024 bytes; a saved web "
                    "page?); left in Downloads", outcome=Kind.SKIPPED.value,
                    detail=f"starts with {head[:16]!r}")
    try:
        scan = scan_pdf(p)
    except PdfError as e:
        return _row(p, size_kb, "ERR_READ", note="PyMuPDF could not read it; left in Downloads",
                    outcome=Kind.ERROR.value, detail=str(e))
    covers = ",".join(sorted(set(scan.covers.values())))
    cover_note = f"cover pages skipped: {covers}" if covers else ""
    try:
        content = p.read_bytes()
    except OSError as e:
        return _row(p, size_kb, "ERR_READ", note="unreadable; left in Downloads", outcome=Kind.ERROR.value,
                    detail=str(e))
    tag = U.boilerplate_of(content, "\n".join(scan.pages[:2]))
    if tag:
        return _row(p, size_kb, "BOILERPLATE", note="a known misfetch page, not a paper; left in Downloads",
                    outcome=Kind.SKIPPED.value, detail=tag)

    ident = identify(scan, forced)
    if ident.status == "META_UNAVAILABLE":
        return _row(p, size_kb, "META_UNAVAILABLE", note="metadata source could not answer; left in Downloads",
                    outcome=str(getattr(ident.kind, "value", ident.kind) or Kind.ERROR.value), detail=ident.detail)
    if ident.status == "UNKNOWN":
        return _row(p, size_kb, "UNKNOWN_LEAVE", note="no identified record; left in Downloads",
                    outcome=Kind.NO_MATCH.value, detail="; ".join(x for x in (cover_note, ident.detail) if x))
    first = scan.pages[scan.article[0]] if scan.article else ""
    kind = _identity.doc_kind(first, scan.n_pages)
    if ident.status == "FLAG" or (kind == _identity.DocKind.SUPPLEMENT and ident.how != "--doi"):
        why = ident.detail if ident.status == "FLAG" else "doc_kind=SUPPLEMENT"
        return _row(p, size_kb, "IDENTITY_FLAG", note="identity not confirmed; left in Downloads for review "
                    "(--doi resolves it)", identity=_identity.Decision.FLAG.value, doc_kind=str(kind),
                    outcome=Kind.SKIPPED.value, detail="; ".join(x for x in (cover_note, why) if x))

    meta, d = ident.meta, (ident.doi or "").lower()
    title = _text.display_field(meta.get("title"))
    year = str(meta.get("year") or "")
    new_name, first_au = proposed_name(meta)
    base = dict(doi=d, year=year, first_au=first_au, title=title,
                identity=str(ident.verdict.decision) if ident.verdict else "", doc_kind=str(kind),
                meta_source=ident.source, detail="; ".join(x for x in (cover_note, ident.how, ident.detail) if x))

    held = dest.pdf_holding(d)
    if held:
        return _row(p, size_kb, "DUP_DOI", new_name=new_name, dup_match=Path(held).stem,
                    note="skip: already in the library", outcome=Kind.SKIPPED.value, **base)
    if d in seen:
        return _row(p, size_kb, "DUP_IN_RUN", new_name=new_name, dup_match=seen[d],
                    note="skip: the same DOI as an earlier file in this run; left in Downloads",
                    outcome=Kind.SKIPPED.value, **base)
    held_other = dest.elsewhere_holdings(d)
    elsewhere = ";".join(str(h.path) for h in held_other)
    if held_other and not import_held_elsewhere:
        h = held_other[0]
        what = "a PDF" if h.kind == holdings.PDF else "a text-only holding"
        return _row(p, size_kb, "HELD_ELSEWHERE", new_name=new_name, dup_match=Path(h.path).stem,
                    note=f"skip: another library holds it ({what} in {h.project}: {h.library}); left in "
                         f"Downloads (--import-held-elsewhere imports it anyway)",
                    held_elsewhere=elsewhere, outcome=Kind.SKIPPED.value, **base)
    text_only = dest.text_only(d)
    if not text_only:
        td = dest.title_dup(title, year)
        if td:
            return _row(p, size_kb, td[0], new_name=new_name, dup_match=Path(td[1]).stem,
                        note="skip: a library PDF with no DOI on record has this title",
                        held_elsewhere=elsewhere, outcome=Kind.SKIPPED.value, **base)

    cover_pages = scan.strip_pages()        # never in the sidecar text
    try:
        text, n_pages, n_used = full_text(p, skip=cover_pages)
    except PdfError as e:
        return _row(p, size_kb, "ERR_READ", note="PyMuPDF could not read it; left in Downloads",
                    outcome=Kind.ERROR.value, detail=str(e))
    stats = _identity.text_stats(text, n_used)
    thin = _identity.suspect_file(None, None, None, stats)
    notes = []
    if elsewhere:
        notes.append("held elsewhere too (--import-held-elsewhere)")
    if thin:
        notes.append("thin text layer: check it (OCR?) before citing")
    strip = [] if keep_covers else cover_pages
    tags = ",".join(sorted({scan.covers[i] for i in cover_pages}))
    if strip:
        verb = "stripped" if execute else "would strip"
        notes.append(f"{verb} {len(strip)} cover page(s) ({tags}); the original kept under "
                     f"{'/'.join(ARCHIVE_ORIGINALS)}/")
    elif cover_pages:
        notes.append(f"cover page(s) kept in the PDF (--keep-covers; {tags}); left out of the sidecar text")
    base["covers_stripped"] = str(len(strip)) if strip else ""

    sidecar_rec = None
    if text_only:
        sc_path, sidecar_rec = text_only
        dest_path = Path(str(sc_path)[:-len(holdings.SIDECAR_SUFFIX)] + ".pdf")
        notes.insert(0, "fills the text-only holding (has_pdf becomes true)")
        dup_match = dest_path.stem
    else:
        taken = set(written)
        canon = Path(dest.lib) / new_name
        in_place = _same_path(canon, p) and _norm(canon) not in {_norm(w) for w in taken}
        if in_place:
            # already at its canonical name in the library (a Downloads folder that is the library):
            # filed in place. resolve_dest would read the file as another paper whenever its first
            # 5,000 characters print no DOI (an ILL cover, a title-identified paper) and suffix it.
            dest_s = str(p)
        else:
            while True:
                dest_s, _ = U.resolve_dest(str(dest.lib), new_name, d, taken)
                if not os.path.exists(dest_s) or _norm(dest_s) == _norm(p):
                    break
                taken.add(dest_s)             # never overwrite a PDF, whatever it holds
        dest_path = Path(dest_s)
        dup_match = ""
        if dest_path.name != new_name and not in_place:   # in place: its own name, never another's
            notes.append("name taken by another file: suffixed (check for a duplicate)")
    written.add(str(dest_path))
    seen[d] = p.name
    ris_path = lit_util.companion_path(dest_path, ".ris")
    sc_out = lit_util.companion_path(dest_path, holdings.SIDECAR_SUFFIX)

    if not execute:
        ris = "EXISTS" if ris_path.exists() else "WOULD_WRITE"
        return _row(p, size_kb, "WOULD_MOVE", new_name=dest_path.name, dup_match=dup_match,
                    note="; ".join(["would move"] + notes), held_elsewhere=elsewhere, ris=ris,
                    outcome=Kind.OK.value, **base)

    ris_text = R.build_ris(meta)
    if sidecar_rec is None and sc_out.exists():
        sidecar_rec = _read_json(sc_out)          # an orphan sidecar of this DOI: keep it
        if sidecar_rec is not None:
            notes.append("existing sidecar kept")
    new_rec = None if sidecar_rec is not None else make_sidecar(text, meta, ident.source, d, p.name, ident, kind)
    try:
        ris, archived = file_import(p, dest_path, sidecar_path=sc_out, sidecar_rec=sidecar_rec,
                                    new_rec=new_rec, ris_path=ris_path, ris_text=ris_text, strip=strip,
                                    archive_dir=dest.lib.joinpath(*ARCHIVE_ORIGINALS))
    except WriteFailed as e:
        seen.pop(d, None)
        written.discard(str(dest_path))
        return _row(p, size_kb, "ERR_WRITE", new_name=dest_path.name, dup_match=dup_match,
                    note="; ".join(["a write failed: nothing filed, left in Downloads (what this run "
                                    "wrote was removed)"] + notes), held_elsewhere=elsewhere,
                    outcome=Kind.ERROR.value, **{**base, "detail": str(e)})
    return _row(p, size_kb, "MOVED", new_name=dest_path.name, dup_match=dup_match,
                note="; ".join(["moved + sidecar written"] + notes), held_elsewhere=elsewhere, ris=ris,
                outcome=Kind.OK.value, archived_original=str(archived) if archived else "", **base)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="Import hand-downloaded PDFs into one literature library: identify, dedup, name, "
                    "and write the sidecar and .ris. Dry run by default.")
    ap.add_argument("--execute", action="store_true",
                    help="Actually move files and write sidecars and .ris. Default is a dry run.")
    ap.add_argument("--dry-run", action="store_true",
                    help="Accepted for clarity; a dry run is the default.")
    ap.add_argument("--cutoff", default=None,
                    help="ISO date or datetime; only PDFs modified at or after it. Default: today "
                         "at local midnight (the papers a browser saved today).")
    ap.add_argument("--downloads", default=None,
                    help="Folder to import from. Default: the Downloads folder in your home directory "
                         "(one workflow: a browser's downloads); name any other folder here.")
    ap.add_argument("--lib-dir", default=None,
                    help="Destination library. This or --project is required.")
    ap.add_argument("--project", default=None,
                    help="A project key registered in projects.json; its library is the destination. "
                         "This or --lib-dir is required.")
    ap.add_argument("--report-dir", default=None,
                    help="Where the report CSV is written. Default: the library's parent folder "
                         "(--lib-dir) or the project's folder (--project).")
    ap.add_argument("--doi", default=None,
                    help="Supply the DOI for a PDF this tool cannot identify on its own (or flags). "
                         "Refuses to run unless the cutoff narrows the sweep to exactly ONE PDF, so it "
                         "can never be attached to the wrong file.")
    ap.add_argument("--import-held-elsewhere", action="store_true",
                    help="Import a paper another library already holds (default: skip it as "
                         "HELD_ELSEWHERE and leave the file in place). Applies to this run only.")
    ap.add_argument("--keep-covers", action="store_true",
                    help="File the PDF whole. Default with --execute: interlibrary-loan cover slips and "
                         "publisher download-notice pages are left out of the filed PDF, and the "
                         "untouched original is kept under <library>/_archive/originals/. Cover text "
                         "never reaches the sidecar either way.")
    args = ap.parse_args(argv)
    try:
        if args.execute and args.dry_run:
            raise UsageError("import_downloads: pass --execute or --dry-run, not both")
        run(execute=args.execute, cutoff=args.cutoff, downloads=args.downloads, lib_dir=args.lib_dir,
            project=args.project, report_dir=args.report_dir, doi=args.doi,
            import_held_elsewhere=args.import_held_elsewhere, keep_covers=args.keep_covers)
    except UsageError as e:
        print(str(e), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
