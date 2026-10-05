"""Audit library filenames against canonical metadata; propose renames, or apply them.

For each top-level PDF in --lib-dir (sorted; --only and --only-prefix narrow the set):

1. Identity flag. A file whose `<stem>.identity.json` or `<stem>.fulltext.json` flags it
   (audit_portfolio.identity_flag: identity FLAG, or doc_kind SUPPLEMENT) is listed for review as
   IDENTITY_FLAG and never renamed: the DOI it was fetched for names another work.
2. DOI, most trusted first: the sibling `.ris` DO line (the load-bearing record); the
   `.fulltext.json` `doi`; the first DOI in the first ~5,000 characters of the PDF text (PyMuPDF,
   litpipe.doi); with --queue-history, the file's entry in the queue history. The row is AMBIGUOUS
   (listed for review, never renamed) when:
     - the sidecar or the queue history names another DOI than the `.ris`;
     - the queue history maps the filename to more than one DOI, or to a row with neither title nor
       authors (an `Unknown_Unknown_Untitled`-style name identifies nothing);
     - another PDF in the library carries the same DOI (a duplicate, or cross-linked metadata).
   A PDF that PyMuPDF cannot open is PDF_ERROR, not "no DOI".
3. Metadata: `crossref(doi)`, the module seam, wraps ris_emit.resolve_meta: Crossref through
   crossref_by_doi and crossref_meta (print-first year, the DOI percent-encoded in the URL path,
   decoded names), then DataCite, then DOI content negotiation for mEDRA, JaLC, KISTI and OP. A
   source that could not answer is META_UNAVAILABLE, never "no record". --offline reads the
   sibling `.ris` instead and sends nothing.
4. The canonical name: ris_emit.canonical_stem over litpipe.text.normalise_title(title), so
   markup and character references never reach a filename (`M&uuml;ndel` gives `Mundel`, never
   `Muumlndel`). A `_preprint` suffix on the current name is kept.
5. Safety checks, listed for review and never renamed:
     SKIP_YEAR_DIFF_<file>_vs_<record>  the filename year differs (0000 counts as unknown);
     REVIEW_AUTHOR         the filename names nobody among the record's authors and the title slug
                           disagrees too: the DOI may belong to another paper (a wrong `.ris`);
     REVIEW_GIVEN_NAME     the filename carries the first author's given name: either the file
                           used the given name or the deposit swapped given and family (both
                           occur), so a human decides;
     REVIEW_RECORD_AUTHOR  the record's first family name is an initial or a given name with
                           initials (a swapped deposit; the current name may be the right one).
   An author named at another position, a surname with its non-ASCII letters dropped, or a
   matching title slug (a file named after an organisation) fits the record.
6. WOULD_RENAME (dry run) or RENAMED (--execute). cascade_rename moves the PDF and every companion
   (`.fulltext.json`, `.identity.json`, `.ris`, `.xml`, `.fig*`) as one unit, carries the DEC-29
   manifest record of the `.ris` to its new path (a renamed pipeline-written `.ris` stays
   refreshable), rewrites the sidecar `image_path` and the identity record's `pdf` field, and
   rolls everything back if any step fails.

Other statuses: ALREADY_CANONICAL, WOULD_COLLIDE, NO_DOI, NO_CROSSREF (no source holds the DOI),
NO_RIS_METADATA (--offline, the `.ris` lacks a year or an author), RENAME_ERROR. A `_QH` suffix
marks a DOI taken from the queue history.

Report columns: current,proposed,doi,status (an AMBIGUOUS row lists its DOIs, ';'-separated). A dry
run writes nothing: pass --report PATH to save the report. --execute writes it to
<lib>/_filename_audit_report.csv unless --report names another path.

Queue history (--queue-history): comma-separated globs or directories. A directory contributes
its processed queue artifacts, both the run-id names `lit_pull_queue[.<tag>].<run_id>.processed.csv`
(run_id YYYY-MM-DD or YYYY-MM-DD.N) and the legacy `lit_pull_queue.<date>.processed[.N].csv`. Each
row maps both the current filename convention (unpaywall_fetch_v2.build_filename) and the legacy
one (legacy_build_filename), so files named before DEC-14/15 still map. Leading `#` lines are
skipped.

Exit codes: 0 clean (review rows included); 1 usage or configuration (no such library, an --only
name that is not in it); 2 a row with RENAME_ERROR, PDF_ERROR or META_UNAVAILABLE, after a final
stdout line `[step-summary] {json}`.

Usage:
  python audit_filenames.py --lib-dir DIR                          # dry run: prints proposals
  python audit_filenames.py --lib-dir DIR --report audit.csv       # dry run, report saved
  python audit_filenames.py --lib-dir DIR --only A.pdf B.pdf --execute
  python audit_filenames.py --lib-dir DIR --only-prefix 2016_Unknown_ --execute
  python audit_filenames.py --lib-dir DIR --offline --report audit.csv   # no network: the .ris
  python audit_filenames.py --lib-dir DIR --queue-history "<proj>/lit_pull_queue.*processed*.csv"
"""
import argparse
import csv
import errno
import glob
import json
import os
import re
import sys
import time

import fitz

import lit_util
from lit_util import safe_ascii  # noqa: F401  re-export: callers do `from audit_filenames import safe_ascii`
lit_util.utf8_stdout()

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import ris_emit  # noqa: E402
from audit_portfolio import identity_flag, lastname_matches  # noqa: E402
from litpipe import doi as _doi  # noqa: E402
from litpipe import text as _text  # noqa: E402

SUMMARY_MARKER = "[step-summary] "
REPORT_FIELDS = ["current", "proposed", "doi", "status"]
DEFAULT_REPORT = "_filename_audit_report.csv"
DOI_TEXT_CHARS = 5000
# Companions that share the PDF's stem and move with it (the .fig* figures are found by prefix).
COMPANION_EXTS = (".fulltext.json", ".identity.json", ".ris", ".xml")
ERROR_STATUSES = ("RENAME_ERROR", "PDF_ERROR", "META_UNAVAILABLE")
# The pipeline's filename: YYYY (or Unknown) _ lastname _ title slug [ _preprint ] .pdf
NAME_RE = re.compile(r"^(\d{4}|Unknown)_([^_]+)_(.+?)(_preprint)?\.pdf$", re.IGNORECASE)
PREPRINT_SUFFIX = "_preprint"
# Queue artifacts: the run-id names lit_pull_queue[.<tag>].<run_id>.<stage>.csv (run_id YYYY-MM-DD
# or YYYY-MM-DD.N; dispatch 0.5) and the legacy lit_pull_queue.<date>.<stage>[.N].csv.
QUEUE_ARTIFACT_RE = re.compile(
    r"^lit_pull_queue(?:\.(?P<tag>[a-z][a-z0-9_-]{0,31}))?\.(?P<date>\d{4}-\d{2}-\d{2})"
    r"(?:\.(?P<n>\d+))?\.(?P<stage>[a-z_]+)(?:\.(?P<legacy_n>\d+))?\.csv$")


class PdfReadError(RuntimeError):
    """PyMuPDF could not read the PDF (W2b forward: this used to read as "no DOI")."""


class _Parser(argparse.ArgumentParser):
    def error(self, message):
        """A usage error exits 1 (the shared convention); argparse would exit 2, which snowball
        and the runner read as DEGRADED when a summary line is present."""
        self.print_usage(sys.stderr)
        self.exit(1, f"{self.prog}: error: {message}\n")


# ------------------------------------------------------------------------------ names
def slug(text, n=6):
    """The title slug: ris_emit.slug, the one slug writer (DEC-15)."""
    return ris_emit.slug(text, n)


def canonical_filename(year, lastname, title):
    """`YYYY_Lastname_TitleSlug.pdf`: ris_emit.canonical_stem (the writer's own stem) over the
    comparison form of the title (litpipe.text.normalise_title: tags stripped, references decoded),
    so the auditor and the writer agree and markup never reaches a slug."""
    return ris_emit.canonical_stem(year, lastname, _text.normalise_title(title or "")) + ".pdf"


def proposed_name(fn, year, lastname, title):
    """The canonical name for `fn`, keeping a `_preprint` suffix the current name carries."""
    name = canonical_filename(year, lastname, title)
    m = NAME_RE.match(fn)
    if m and m.group(4):
        name = name[:-4] + PREPRINT_SUFFIX + ".pdf"
    return name


# ------------------------------------------------------------------------------ DOI sources
def extract_doi_from_pdf(pdf_path, max_chars=DOI_TEXT_CHARS):
    """The first DOI in the first `max_chars` characters of the PDF text (litpipe.doi.normalise),
    '' when there is none. Raises PdfReadError when PyMuPDF cannot read the file."""
    try:
        doc = fitz.open(pdf_path)
        try:
            text = ""
            for p in doc:
                text += p.get_text()
                if len(text) >= max_chars:
                    break
        finally:
            doc.close()
    except Exception as e:  # PyMuPDF raises FileDataError, EmptyFileError, RuntimeError, OSError
        raise PdfReadError(f"{type(e).__name__}: {e}") from None
    return _doi.normalise(text[:max_chars]) or ""


def _read_json(path):
    try:
        with open(path, encoding="utf-8") as fh:
            d = json.load(fh)
        return d if isinstance(d, dict) else None
    except (OSError, ValueError, UnicodeDecodeError):
        return None


def doi_from_sidecar(sidecar_path):
    """The `.fulltext.json` `doi`, normalised; '' when absent. A sidecar whose identity verdict is
    FLAG records the DOI of the paper that was asked for, not of this file, so it names none."""
    d = _read_json(sidecar_path) if os.path.exists(sidecar_path) else None
    if not d or identity_flag(d):
        return ""
    return _doi.normalise(d.get("doi") or "") or ""


def doi_from_ris(ris_path):
    """The `.ris` DOI (DO line, else a doi.org UR line), normalised; '' when absent."""
    if not os.path.exists(ris_path):
        return ""
    return _doi.normalise(lit_util.parse_ris(ris_path).get("doi") or "") or ""


def ris_meta(ris_path):
    """The `.ris` record in crossref()'s shape (for --offline): title, year, first family name and
    authors ({family, given}; an AU line without a comma is one family name, as build_ris writes
    an organisation or a mononym). None when there is no `.ris`."""
    if not os.path.exists(ris_path):
        return None
    r = lit_util.parse_ris(ris_path)
    authors = []
    for au in r.get("authors") or []:
        fam, _, giv = au.partition(",")
        if fam.strip():
            authors.append({"family": _text.display_field(fam), "given": _text.display_field(giv)})
    return {"doi": _doi.normalise(r.get("doi") or "") or "", "title": _text.display_field(r.get("title")),
            "year": str(r.get("year") or ""), "lastname": authors[0]["family"] if authors else "",
            "authors": authors, "source": "ris"}


def flag_reason(lib, stem):
    """The identity-flag reason for `<stem>.pdf` ('' when not flagged): the unpaywall/preprint
    `.identity.json`, then the pmc `.fulltext.json` identity field."""
    for ext in (".identity.json", ".fulltext.json"):
        p = os.path.join(lib, stem + ext)
        if os.path.exists(p):
            why = identity_flag(_read_json(p))
            if why:
                return f"{ext[1:]}: {why}"
    return ""


# ------------------------------------------------------------------------------ queue history
def queue_history_files(arg):
    """The queue-history CSVs named by `arg`: comma-separated globs or directories (a directory
    gives its processed queue artifacts, run-id and legacy names both). De-duplicated, in order."""
    out, seen = [], set()
    for item in (arg or "").split(","):
        item = item.strip()
        if not item:
            continue
        if os.path.isdir(item):
            found = []
            for f in sorted(os.listdir(item)):
                m = QUEUE_ARTIFACT_RE.match(f)
                if m and m.group("stage") == "processed":
                    found.append(os.path.join(item, f))
        else:
            found = sorted(glob.glob(item))
        for p in found:
            k = os.path.normcase(os.path.abspath(p))
            if k not in seen:
                seen.add(k)
                out.append(p)
    return out


def _queue_rows(path):
    """DictReader rows of a queue CSV, leading `#` comment lines skipped."""
    with open(path, encoding="utf-8-sig", newline="") as fh:
        lines = fh.read().splitlines(keepends=True)
    i = 0
    while i < len(lines) and lines[i].lstrip().startswith("#"):
        i += 1
    return list(csv.DictReader(lines[i:]))


def load_queue_history(globs_arg, *, with_conflicts=False):
    """{filename -> doi} from queue-history CSVs (doi,title,authors,year,destination,notes).

    Each row maps the filename the pipeline writes today (unpaywall_fetch_v2.build_filename) and
    the one it wrote before DEC-14/15 (legacy_build_filename), so legacy-named files still map.
    A filename that maps to more than one DOI, or that was built from a row with neither title nor
    authors, is left out of the mapping: it identifies nothing. Returns (mapping, paths), or with
    with_conflicts=True (mapping, paths, conflicts) where conflicts is {filename: [dois]}."""
    from unpaywall_fetch_v2 import build_filename, legacy_build_filename
    paths = queue_history_files(globs_arg)
    dois_by_name, vague = {}, set()
    for p in paths:
        try:
            rows = _queue_rows(p)
        except (OSError, csv.Error, UnicodeDecodeError) as e:
            print(f"  QUEUE-HISTORY READ ERROR ({p}): {e}")
            continue
        for r in rows:
            doi = _doi.normalise((r.get("doi") or "").strip())
            if not doi:
                continue
            year, authors, title = r.get("year") or "", r.get("authors") or "", r.get("title") or ""
            for fn in {build_filename(year, authors, title), legacy_build_filename(year, authors, title)}:
                if not fn:
                    continue
                dois_by_name.setdefault(fn, set()).add(doi)
                if not authors.strip() and not title.strip():
                    vague.add(fn)
    mapping = {fn: next(iter(ds)) for fn, ds in dois_by_name.items() if len(ds) == 1 and fn not in vague}
    conflicts = {fn: sorted(ds) for fn, ds in dois_by_name.items() if len(ds) > 1 or fn in vague}
    if with_conflicts:
        return mapping, paths, conflicts
    return mapping, paths


# ------------------------------------------------------------------------------ metadata
def crossref(doi):
    """The metadata seam (the name is historical; tests replace it): ris_emit.resolve_meta, which
    reads Crossref through crossref_by_doi + crossref_meta (CROSSREF_DATE_ORDER, encoded DOI path,
    decoded family names), then DataCite, then content negotiation. Returns {doi, title, year,
    lastname, authors, source}, or None when no source holds the DOI (or its agency has no https
    route). Raises ris_emit.MetadataUnavailable when a source could not answer."""
    meta, source = ris_emit.resolve_meta(doi)
    if not meta or not meta.get("title"):
        return None
    return {"doi": meta.get("doi") or doi, "title": meta.get("title") or "",
            "year": meta.get("year") or "", "lastname": meta.get("lastname") or "",
            "authors": meta.get("authors") or [], "source": source}


def _name_key(s):
    return re.sub(r"[^a-z0-9]", "", safe_ascii(s or "").lower())


_INITIALS_FAMILY = re.compile(r"^(?:[A-Z]{1,2}\.?|[A-Z]\.?[ -]?[A-Z]\.?|[A-Z][a-z]+ [A-Z]{1,2}\.?)$")


def doubtful_family(family):
    """A record's first family name that cannot be a surname: initials ("M", "B.", "MR", "J. R.")
    or a given name with initials ("Marc R"), the marks of a deposit with given and family swapped.
    Two-letter surnames ("Ma", "Li", "Wu") are real and pass."""
    f = safe_ascii((family or "").strip())
    return not f or bool(_INITIALS_FAMILY.match(f))


def author_review(fn, meta):
    """'' when the filename's author token fits this record, else the review status.

    Fits: the token names nobody (no pipeline name, `Unknown`); names the first author
    (audit_portfolio.lastname_matches: compounds and particles) or, exactly, a co-author (an
    author-order slip); is the first family name with its non-ASCII letters dropped (old
    ascii-ignore naming); or the title slug agrees (a file named after an organisation or a
    document type). Review: REVIEW_RECORD_AUTHOR when the record's first family name is an initial
    or a given name with initials (a swapped deposit; the file may be right); REVIEW_GIVEN_NAME when
    the token is one of the first author's given names (either the file used a given name or the
    deposit swapped given and family: the 2026-10-05 census found both, e.g. "Tongwu, Yu"); and
    REVIEW_AUTHOR otherwise (the DOI may be another paper's)."""
    m = NAME_RE.match(fn)
    if not m:
        return ""
    cur = m.group(2)
    if _name_key(cur) in ("", "unknown"):
        return ""
    first = meta.get("lastname") or ""
    if doubtful_family(first):
        return "REVIEW_RECORD_AUTHOR"
    authors = [a for a in (meta.get("authors") or []) if isinstance(a, dict)]
    if lastname_matches(cur, first):            # the first author, compounds and particles included
        return ""
    given = re.split(r"[\s\-]+", authors[0].get("given") or "") if authors else []
    if any(g and _name_key(g) == _name_key(cur) for g in given):
        return "REVIEW_GIVEN_NAME"
    if any(_name_key(a.get("family")) == _name_key(cur) for a in authors[1:]):
        return ""                               # a co-author (author-order slip): exact match only
    dropped = re.sub(r"[^A-Za-z0-9]", "", first.encode("ascii", "ignore").decode("ascii"))
    if dropped and dropped.lower() == re.sub(r"[^A-Za-z0-9]", "", cur).lower():
        return ""
    from unpaywall_fetch_v2 import legacy_slug_title
    title = meta.get("title") or ""
    cur_slug = m.group(3).lower()
    if cur_slug in (slug(_text.normalise_title(title)).lower(), legacy_slug_title(title).lower()):
        return ""
    return "REVIEW_AUTHOR"


# ------------------------------------------------------------------------------ rename
def cascade_rename(lib_dir, old_pdf, new_pdf):
    """Rename the PDF and its companions as one unit; return [(kind, old, new)].

    Companions: `.fulltext.json`, `.identity.json`, `.ris`, `.xml` and every `.fig*` file. A target
    that already exists (another file) stops the cascade before anything moves (os.rename replaces
    silently on POSIX). DEC-29: the `.ris` manifest record moves to the new path's key and the old
    key is blanked (manifest_key is the real path, so the old key is read before the rename). A2:
    if any step fails, the renames are undone newest first and both manifest keys are restored,
    so the library never lands half renamed. After every step has committed, the sidecar
    `image_path` values and the identity record's `pdf` field are rewritten atomically (F3)."""
    old_stem, new_stem = old_pdf[:-4], new_pdf[:-4]

    def path(name):
        return os.path.join(lib_dir, name)

    moves = [("pdf", old_pdf, new_pdf)]
    for ext in COMPANION_EXTS:
        if os.path.exists(path(old_stem + ext)):
            moves.append((ext.lstrip("."), old_stem + ext, new_stem + ext))
    for f in sorted(os.listdir(lib_dir)):
        if f.startswith(old_stem + ".fig"):
            moves.append(("figure", f, new_stem + f[len(old_stem):]))
    for _kind, o, n in moves:
        if os.path.exists(path(n)) and os.path.normcase(path(n)) != os.path.normcase(path(o)):
            raise FileExistsError(errno.EEXIST, f"target exists: {n}", path(n))

    old_ris = path(old_stem + ".ris")
    old_key = ris_emit.manifest_key(old_ris) if os.path.exists(old_ris) else None
    record = ris_emit._kv_get(ris_emit.RIS_NS, old_key) if old_key else None
    new_key = prev_new = None
    carried = False
    renamed = []
    try:
        for kind, o, n in moves:
            os.rename(path(o), path(n))
            renamed.append((kind, o, n))
        if record:
            new_key = ris_emit.manifest_key(path(new_stem + ".ris"))
            if new_key != old_key:
                prev_new = ris_emit._kv_get(ris_emit.RIS_NS, new_key)
                carried = True
                if not ris_emit._kv_set(ris_emit.RIS_NS, new_key, record):
                    raise OSError("the .ris manifest record could not be written under the new name")
                if not ris_emit._kv_set(ris_emit.RIS_NS, old_key, None):
                    raise OSError("the old .ris manifest record could not be cleared")
    except OSError:
        if carried:
            ris_emit._kv_set(ris_emit.RIS_NS, new_key, prev_new)
            ris_emit._kv_set(ris_emit.RIS_NS, old_key, record)
        for _kind, o, n in reversed(renamed):  # A2: undo, newest first
            try:
                os.rename(path(n), path(o))
            except OSError:
                print(f"      ROLLBACK FAILED: {n} -> {o}; the library may be partially renamed")
        raise

    new_sc = path(new_stem + ".fulltext.json")
    if os.path.exists(new_sc):
        try:
            d = _read_json(new_sc)
            if d is not None:
                changed = False
                for fig in d.get("figures") or []:
                    ip = fig.get("image_path", "") if isinstance(fig, dict) else ""
                    if ip and ip.startswith(old_stem):
                        fig["image_path"] = ip.replace(old_stem, new_stem, 1)
                        changed = True
                if changed:
                    lit_util.atomic_write_json(new_sc, d)
        except OSError as e:
            print(f"      (sidecar update warning: {e})")
    new_id = path(new_stem + ".identity.json")
    if os.path.exists(new_id):
        try:
            d = _read_json(new_id)
            if d is not None and d.get("pdf") != new_pdf:
                d["pdf"] = new_pdf
                lit_util.atomic_write_json(new_id, d)
        except OSError as e:
            print(f"      (identity record update warning: {e})")
    return renamed


# ------------------------------------------------------------------------------ the audit
def _row(fn, proposed="", doi="", status=""):
    return {"current": fn, "proposed": proposed, "doi": doi, "status": status}


def _doi_sources(lib, fn, cheap=False):
    """(ris_doi, sidecar_doi, pdf_doi): the PDF text is read only when the other two are empty and
    `cheap` is false (then PdfReadError can be raised)."""
    stem = fn[:-4]
    ris_doi = doi_from_ris(os.path.join(lib, stem + ".ris"))
    sc_doi = doi_from_sidecar(os.path.join(lib, stem + ".fulltext.json"))
    pdf_doi = ""
    if not cheap and not ris_doi and not sc_doi:
        pdf_doi = extract_doi_from_pdf(os.path.join(lib, fn))
    return ris_doi, sc_doi, pdf_doi


def run(*, lib_dir=None, execute=False, only_prefix=None, only=None, queue_history=None, report=None,
        offline=False):
    """Audit one library. Returns {exit_code, rows, counts, report, reasons}; see the module
    docstring for the statuses and exit codes."""
    t0 = time.time()
    res = {"exit_code": 0, "rows": [], "counts": {}, "report": None, "reasons": []}
    if not lib_dir or not os.path.isdir(lib_dir):
        print(f"[audit_filenames] library not found: {lib_dir}")
        res["exit_code"] = 1
        return res
    lib = os.path.abspath(lib_dir)
    all_pdfs = sorted(f for f in os.listdir(lib) if f.endswith(".pdf") and os.path.isfile(os.path.join(lib, f)))
    pdfs = list(all_pdfs)
    if only_prefix:
        pdfs = [p for p in pdfs if p.startswith(only_prefix)]
    if only:
        wanted = [os.path.basename(x) for x in only]
        missing = [x for x in wanted if x not in set(all_pdfs)]
        if missing:
            print(f"[audit_filenames] --only: not in the library: {', '.join(missing)}")
            res["exit_code"] = 1
            return res
        pdfs = [p for p in pdfs if p in set(wanted)]

    print(f"Library: {lib}")
    print(f"PDFs to audit: {len(pdfs)}")
    print(f"Mode: {'EXECUTE' if execute else 'DRY RUN'}" + (" (offline: metadata from the .ris)" if offline else ""))
    qh_map, qh_conflicts = {}, {}
    if queue_history:
        qh_map, qh_paths, qh_conflicts = load_queue_history(queue_history, with_conflicts=True)
        print(f"Queue history: {len(qh_paths)} CSV(s), {len(qh_map)} filename->DOI entries, "
              f"{len(qh_conflicts)} ambiguous names")
    print()

    # One cheap pass over the whole library (no PDF text): identity flags and the .ris / sidecar
    # DOIs. A DOI carried by two unflagged PDFs identifies neither of them.
    cheap = {}
    holders = {}
    for fn in all_pdfs:
        flag = flag_reason(lib, fn[:-4])
        ris_d, sc_d, _ = _doi_sources(lib, fn, cheap=True)
        cheap[fn] = (flag, ris_d, sc_d)
        if not flag and (ris_d or sc_d):
            holders.setdefault(ris_d or sc_d, []).append(fn)

    rows = res["rows"]
    seen_canonical = set(all_pdfs)
    n_renamed = 0

    def add(row, line=None):
        rows.append(row)
        if line:
            print(line)

    for fn in pdfs:
        stem = fn[:-4]
        why, ris_doi, sc_doi = cheap[fn]
        if why:
            add(_row(fn, status="IDENTITY_FLAG"), f"  FLAG     {fn[:65]:<65} ({why}; review, never renamed)")
            continue
        pdf_doi = ""
        if not ris_doi and not sc_doi:
            try:
                pdf_doi = extract_doi_from_pdf(os.path.join(lib, fn))
            except PdfReadError as e:
                add(_row(fn, status="PDF_ERROR"), f"  PDF_ERROR {fn[:64]:<64} ({e})")
                continue
        qh_doi = qh_map.get(fn, "")
        qh_tag = ""
        doi = ris_doi or sc_doi or pdf_doi
        clash = sorted({d for d in (sc_doi, qh_doi) if d and ris_doi and d != ris_doi})
        if clash:
            add(_row(fn, doi=";".join([ris_doi] + clash), status="AMBIGUOUS"),
                f"  AMBIG    {fn[:65]:<65} (.ris {ris_doi} vs {', '.join(clash)})")
            continue
        if not doi and fn in qh_conflicts:
            add(_row(fn, doi=";".join(qh_conflicts[fn]), status="AMBIGUOUS_QH"),
                f"  AMBIG    {fn[:65]:<65} (queue history: {len(qh_conflicts[fn])} DOI(s) for an "
                f"uninformative or shared name)")
            continue
        if not doi and qh_doi:
            doi, qh_tag = qh_doi, "_QH"
            print(f"  QH-DOI   {fn[:65]:<65} <- {doi} (queue-history-sourced DOI)")
        if not doi:
            add(_row(fn, status="NO_DOI"))
            continue
        others = [f for f in holders.get(doi, []) if f != fn]
        if others:
            add(_row(fn, doi=doi, status="AMBIGUOUS" + qh_tag),
                f"  AMBIG    {fn[:65]:<65} ({doi} is also carried by {', '.join(others[:3])})")
            continue
        try:
            meta = ris_meta(os.path.join(lib, stem + ".ris")) if offline else crossref(doi)
        except ris_emit.MetadataUnavailable as e:
            add(_row(fn, doi=doi, status="META_UNAVAILABLE" + qh_tag), f"  META_UNAVAILABLE {fn[:56]:<56} ({e})")
            continue
        if not meta or not meta.get("lastname") or not meta.get("year"):
            add(_row(fn, doi=doi, status=("NO_RIS_METADATA" if offline else "NO_CROSSREF") + qh_tag))
            continue
        proposed = proposed_name(fn, meta["year"], meta["lastname"], meta.get("title") or "")
        if proposed == fn:
            add(_row(fn, proposed, doi, "ALREADY_CANONICAL" + qh_tag))
            continue
        if proposed in seen_canonical:
            add(_row(fn, proposed, doi, "WOULD_COLLIDE" + qh_tag), f"  COLLIDE  {fn[:65]:<65} -> {proposed} (already in lib)")
            continue
        # Safety: a different year may mean the DOI is another version or another paper.
        cur_year_m = re.match(r"^(\d{4})_", fn)
        cur_year = cur_year_m.group(1) if cur_year_m else ""
        if cur_year and cur_year != "0000" and cur_year != str(meta["year"]):
            add(_row(fn, proposed, doi, f"SKIP_YEAR_DIFF_{cur_year}_vs_{meta['year']}" + qh_tag),
                f"  YEAR-DIFF {fn[:60]:<60} cur={cur_year} meta={meta['year']} ({proposed[:50]})")
            continue
        review = author_review(fn, meta)
        if review:
            add(_row(fn, proposed, doi, review + qh_tag),
                f"  REVIEW   {fn[:65]:<65} -> {proposed} ({review} for {doi})")
            continue
        tag = " [QH]" if qh_tag else ""
        if execute:
            try:
                cascade_rename(lib, fn, proposed)
            except OSError as e:
                add(_row(fn, proposed, doi, f"RENAME_ERROR_{e}" + qh_tag), f"  RENAME_ERROR {fn[:60]:<60} ({e})")
                continue  # nothing moved: the seen-set keeps both names as they are
            add(_row(fn, proposed, doi, "RENAMED" + qh_tag), f"  RENAMED{tag}  {fn[:65]:<65} -> {proposed}")
            n_renamed += 1
        else:
            add(_row(fn, proposed, doi, "WOULD_RENAME" + qh_tag), f"  PROPOSE{tag}  {fn[:65]:<65} -> {proposed}")
        # D3: the vacated name frees up and the proposed one is taken, in both modes, so a dry run
        # reports the collisions --execute would meet.
        seen_canonical.discard(fn)
        seen_canonical.add(proposed)

    counts = {}
    for r in rows:
        key = r["status"]
        for prefix in ("RENAME_ERROR", "SKIP_YEAR_DIFF"):
            if key.startswith(prefix):
                key = prefix + ("_QH" if key.endswith("_QH") else "")
        counts[key] = counts.get(key, 0) + 1
    res["counts"] = counts

    report_path = report or (os.path.join(lib, DEFAULT_REPORT) if execute else None)
    if report_path:
        with open(report_path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=REPORT_FIELDS)
            w.writeheader()
            w.writerows(rows)
        res["report"] = report_path

    def n(*keys):
        return sum(v for k, v in counts.items() if any(k == x or k == x + "_QH" for x in keys))

    print("\n=== Summary ===")
    print(f"  Total PDFs:           {len(pdfs)}")
    print(f"  Already canonical:    {n('ALREADY_CANONICAL')}")
    print(f"  {'Renamed' if execute else 'Would rename':<22}{n_renamed if execute else n('WOULD_RENAME')}")
    authors = ("REVIEW_AUTHOR", "REVIEW_GIVEN_NAME", "REVIEW_RECORD_AUTHOR")
    print(f"  Review (not renamed): {n('AMBIGUOUS', 'IDENTITY_FLAG', 'SKIP_YEAR_DIFF', 'WOULD_COLLIDE', *authors)}"
          f"  (ambiguous {n('AMBIGUOUS')}, identity flags {n('IDENTITY_FLAG')}, author {n(*authors)},"
          f" year {n('SKIP_YEAR_DIFF')}, collisions {n('WOULD_COLLIDE')})")
    print(f"  No DOI extractable:   {n('NO_DOI')}")
    print(f"  No metadata record:   {n('NO_CROSSREF', 'NO_RIS_METADATA')}")
    if queue_history:
        print(f"  DOI from queue-hist:  {sum(v for k, v in counts.items() if k.endswith('_QH'))}")
    errors = {k: n(k) for k in ERROR_STATUSES if n(k)}
    if errors:
        print(f"  ERRORS:               {errors}")
    print(f"  Wall time:            {time.time() - t0:.1f}s")
    print(f"\nReport: {report_path}" if report_path else
          "\nReport: none written (dry run; pass --report PATH to save it)")
    if errors:
        res["reasons"] = [f"{k}: {v} row(s)" for k, v in errors.items()]
        res["exit_code"] = 2
        print(SUMMARY_MARKER + json.dumps({"reasons": res["reasons"], "aborted": None,
                                           "transport_failures": errors.get("META_UNAVAILABLE", 0),
                                           "counts": counts}, ensure_ascii=False), flush=True)
    return res


def main(argv=None):
    ap = _Parser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Audit library filenames against canonical metadata; propose or apply renames.",
        epilog="""Queue-history fallback (for older scans without machine-readable DOI text):
  python audit_filenames.py --lib-dir DIR \\
      --queue-history "<proj>/lit_pull_queue.*processed*.csv"
  # Comma-separated globs or directories. The glob catches run-id names
  # (lit_pull_queue[.<tag>].<date>[.N].processed.csv) and legacy same-day re-sweeps
  # (lit_pull_queue.<date>.processed.2.csv).""")
    ap.add_argument("--lib-dir", required=True)
    ap.add_argument("--execute", action="store_true",
                    help="Apply renames. Without this, a dry run that writes nothing.")
    ap.add_argument("--only-prefix", default=None,
                    help="Only process PDFs whose filename starts with this")
    ap.add_argument("--only", nargs="+", default=None, metavar="FILE",
                    help="Only process these PDFs (file names in --lib-dir).")
    ap.add_argument("--queue-history", default=None,
                    help="Comma-separated globs or directories of processed queue CSVs, consulted as "
                         "a DOI fallback when neither the .ris, the sidecar nor the PDF text yields a "
                         "DOI. Run-id names (lit_pull_queue[.<tag>].<date>[.N].processed.csv) and legacy "
                         "names (lit_pull_queue.<date>.processed[.N].csv) are both read.")
    ap.add_argument("--report", default=None,
                    help="Report CSV path. Dry run: written only when given. --execute: default "
                         "<lib>/_filename_audit_report.csv.")
    ap.add_argument("--offline", action="store_true",
                    help="Take title, year and authors from each file's .ris instead of resolving "
                         "the DOI (no network).")
    args = ap.parse_args(argv)
    res = run(lib_dir=args.lib_dir, execute=args.execute, only_prefix=args.only_prefix, only=args.only,
              queue_history=args.queue_history, report=args.report, offline=args.offline)
    return res["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
