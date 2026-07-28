"""Shared safety primitives for the literature pipeline (added 2026-06-05 audit remediation).

Centralizes the fixes for the cross-cutting failure modes found in the 2026-06-05 code audit
(see notes/2026-06-05_pipeline_audit.md):
  - RC4  atomic_write_*  : crash-safe writes (tmp + os.replace) so an interrupt never truncates
                           a sidecar/.ris/CSV into invalid JSON.
  - RC1  DOI handling    : extract_doi_from_text() that does NOT truncate line-wrapped DOIs, plus
                           is_valid_doi()/is_suspicious_doi() gates to keep malformed DOIs
                           (e.g. '10.1002/cphy', '10.1001/archinte') out of the candidates/DB.
  - RC5  merge_sidecar() : preserve enriched fields (doi/title/year/authors/figures) when an
                           extractor re-writes a sidecar, instead of clobbering from an empty template.

Pure stdlib; safe to import from any pipeline script.
"""
import os, re, json, sys, tempfile, unicodedata, csv, time
from pathlib import Path

# ---------------------------------------------------------------- shared config
# Fallback contact mailto when LITPIPE_EMAIL is unset. Per Unpaywall/CrossRef/NCBI/
# Europe PMC/Semantic Scholar ToS every API UA must carry a mailto; absent an override
# the pipeline identifies as the maintainer (ris_emit.warn_if_default_email nags once).
# Single source of truth for the string that was hardcoded across ~14 fetcher modules
# (2026-07 Stage 3 c12); each still resolves it via os.environ.get("LITPIPE_EMAIL", DEFAULT_EMAIL).
DEFAULT_EMAIL = "JacobBowie@users.noreply.github.com"

# ---------------------------------------------------------------- console I/O hardening
def utf8_stdout():
    """Force UTF-8 (errors='replace', line-buffered) on this process's stdout AND
    stderr, and set PYTHONUTF8/PYTHONIOENCODING so subprocess children inherit it.

    Consolidates 26 drifted copies of the reconfigure idiom (2026-07 Stage 3). The
    per-site variants variously reconfigured stdout only (leaving stderr to crash a
    non-ASCII traceback under cp1252), omitted errors='replace' (crash-on-unencodable),
    or guarded on ``getattr(sys.stdout, "encoding", "").lower()`` which raises on a
    stream whose ``.encoding`` is None and then silently SKIPPED the reconfigure. Both
    streams are hardened here; the call is idempotent and import-time-safe even when
    another module already reconfigured or closed the streams (a closed-stream
    reconfigure raises ValueError, caught below)."""
    os.environ.setdefault("PYTHONUTF8", "1")
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
        except (AttributeError, ValueError, OSError):
            pass

# ---------------------------------------------------------------- companion (sidecar) paths
def companion_path(pdf, ext):
    """Path of a PDF's companion file (.ris, .fulltext.json, .txt, ...), replacing ONLY the
    final .pdf/.PDF suffix. Dot-safe and byte-identical to the writers (ris_emit os.path.splitext
    + ext; extract_pdf_fulltext / pmc_fetch fn[:-4] + ext).

    NEVER derive a companion via `pdf.with_suffix("").with_suffix(ext)`: that two-step idiom
    strips an *interior* dotted segment ("2010_Smith.4.26.pdf" -> "2010_Smith.4.ris") and
    silently de-links the sidecar from its PDF, so the index reads the paper as no-DOI
    (the 2026-06-25 dot-suffix bug). A single `with_suffix(ext)` replaces only the last suffix
    and is correct; this helper makes that the one named convention. Returns a pathlib.Path.
    """
    return Path(pdf).with_suffix(ext)

# ---------------------------------------------------------------- RC4: atomic writes
def atomic_write_text(path, text, newline="\n"):
    """Write text crash-safely: write a sibling tmp then os.replace (atomic on NTFS)."""
    d = os.path.dirname(path) or "."
    fd, tmp = tempfile.mkstemp(dir=d, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline=newline) as f:
            f.write(text)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            try: os.remove(tmp)
            except OSError: pass

def atomic_write_json(path, obj, indent=2):
    atomic_write_text(path, json.dumps(obj, ensure_ascii=False, indent=indent))

def atomic_write_csv(path, rows, fieldnames, newline="\n"):
    """Write a list-of-dicts to CSV crash-safely (tmp + os.replace) with LF row
    endings. `rows` is an iterable of dicts; `fieldnames` sets the header + column
    order. Mirrors atomic_write_text: an interrupt never leaves a truncated report,
    and the LF lineterminator keeps the report from picking up CRLF on Windows
    (the csv default). Consolidates the two backfill report writers (c12)."""
    d = os.path.dirname(path) or "."
    fd, tmp = tempfile.mkstemp(dir=d, suffix=".tmp")
    try:
        # newline="" so csv controls line endings; lineterminator=newline forces LF.
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, lineterminator=newline)
            w.writeheader()
            w.writerows(rows)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            try: os.remove(tmp)
            except OSError: pass

def coerce_int(v, default=0):
    """Parse an int from a possibly-messy CSV/JSON cell ('1,234', ' 12 ', 'n/a',
    None, '2020a', 'in press'). Returns `default` on anything non-numeric.

    Single source of truth for the int()-on-external-numeric bug class
    (2026-06-25 audit T5d + the sibling sweep that found build_priority's year
    sort-key crashing on a non-numeric residual-CSV year). Strips commas and
    catches both ValueError and TypeError so it never raises."""
    try:
        return int(float(str(v).replace(",", "").strip() or default))
    except (ValueError, TypeError):
        return default

# ---------------------------------------------------------------- filename ASCII normalization
# Non-decomposable specials that NFKD leaves intact (they carry no combining mark to strip),
# so they must be transliterated explicitly BEFORE the NFKD pass. Single source of truth for the
# byte-identical copies that lived in ris_emit + audit_filenames (2026-07 Stage 3 c14). Both
# re-export `safe_ascii` (from lit_util import safe_ascii) so their `from <mod> import safe_ascii`
# callers keep working unchanged.
_NON_DECOMPOSABLE = str.maketrans({
    # Nordic / Germanic
    "ø":"o","Ø":"O","æ":"ae","Æ":"Ae","ß":"ss","þ":"th","Þ":"Th",
    # Slavic / Polish / Croatian / Vietnamese
    "ł":"l","Ł":"L","đ":"d","Đ":"D",
    # French ligature
    "œ":"oe","Œ":"Oe",
    # Turkish dotted/dotless I (and stray look-alikes in author names)
    "ı":"i","İ":"I",
})


def safe_ascii(s):
    """Normalize Unicode → portable ASCII for filenames.
    First handles non-decomposable special chars (ø→o, æ→ae, ß→ss, ł→l...),
    then NFKD-normalizes accents and strips combining marks
    (Lüthi→Luthi, Périard→Periard, Mølmen→Molmen, Müller-García→Muller-Garcia)."""
    if not s: return ""
    s = s.translate(_NON_DECOMPOSABLE)
    nfkd = unicodedata.normalize("NFKD", s)
    no_combining = "".join(c for c in nfkd if not unicodedata.combining(c))
    return no_combining.encode("ascii", "ignore").decode("ascii")

# ---------------------------------------------------------------- project registry + path resolution
def load_projects_config(config_path, missing_ok=False):
    """Load and parse projects.json. Canonical loader for the whole pipeline
    (re-exported from ris_emit for backward-compatible `from ris_emit import ...`).

    On a missing file: if `missing_ok`, return {} (an empty registry — the caller's
    `.get("projects", {})` then yields no projects, e.g. build_priority's dedup
    degrading gracefully in CI where projects.json is gitignored). Otherwise print
    the copy-the-template hint and sys.exit(2) — the entry-point UX the fetch/index
    scripts rely on."""
    p = Path(config_path)
    if not p.exists():
        if missing_ok:
            return {}
        tmpl = p.with_name("projects.json.template")
        print(
            f"[litpipe] projects.json not found at {p}.\n"
            f"          Copy the template to start: cp {tmpl.name} {p.name}\n"
            f"          See the README §Quickstart for the schema.",
            file=sys.stderr,
        )
        sys.exit(2)
    with open(p, encoding="utf-8") as fh:
        return json.load(fh)

def _resolve_projects_root(cfg):
    """Resolve the projects root from a loaded projects.json dict. CONFIG-ONLY
    (no env var). Precedence: cfg["root"] -> default "~/Projects". Resolution:
    expanduser(); an absolute path is used verbatim; a bare-relative path is
    anchored to HOME (e.g. "Work/lit" -> ~/Work/lit). Kept pure (no I/O) so it
    unit-tests without import/reload gymnastics."""
    raw = (cfg or {}).get("root") or "~/Projects"
    p = Path(raw).expanduser()
    return p if p.is_absolute() else (Path.home() / p)

# The single projects-root anchor for the whole pipeline, resolved ONCE at import
# from the optional top-level "root" key in projects.json (config-only; ~/Projects
# default; missing_ok so a fresh clone / CI with no projects.json stays import-safe).
# Consumers read lit_util.PROJECTS_ROOT via ATTRIBUTE access (never
# `from lit_util import PROJECTS_ROOT`), so project_root/lib_paths pick up the value
# at call time and tests can monkeypatch this single attribute.
PROJECTS_ROOT = _resolve_projects_root(
    load_projects_config(Path(__file__).parent / "projects.json", missing_ok=True)
)

def project_root(key, p):
    """On-disk working dir of a (possibly nested) registered project — where its
    lit_pull_queue.csv and sweep artifacts live. TAIL-AWARE (RC11): a subproject
    key 'A/B' declaring parent 'A' resolves to <PROJECTS_ROOT>/A/B.

    This is the QUEUE / project path. It is deliberately NOT the library dir: the
    registry's lib_dir for a subproject already carries the tail (e.g. 'Yitts/
    literature'), so the lib base is the PARENT (see lib_rel/lib_paths). The two
    conventions reach the same subtree by different routes and must NOT be unified
    (unifying would double-count the tail). `p` is the single project's registry
    dict (as returned by cfg[key]); {} is treated as a top-level project."""
    parent = (p or {}).get("parent")
    if parent:
        tail = key[len(parent):].lstrip("/\\") or Path(key).name
        return PROJECTS_ROOT / parent / tail
    return PROJECTS_ROOT / key

def lib_rel(key, p):
    """PROJECTS_ROOT-relative library dir for a registered project, as a STRING:
    '<parent-or-key>/<lib_dir>'. For a subproject the lib_dir already includes the
    tail, so the base is the PARENT, not the tail-aware project_root. Returned as a
    string so os.path.join(ROOT, lib_rel(...)) join semantics are preserved (the
    build_priority LIBS values). `p` is the per-project registry dict."""
    return f"{p.get('parent') or key}/{p['lib_dir']}"

def lib_paths(key, p):
    """(base, lib, data) absolute Paths for a registered project. `base` is the
    PARENT root (a subproject's lib_dir carries the tail); `data` is None when the
    project declares no data_dir. Single source for the audit/index/pipeline_check
    lib resolvers."""
    base = PROJECTS_ROOT / (p.get("parent") or key)
    lib = PROJECTS_ROOT / lib_rel(key, p)
    data = (base / p["data_dir"]) if p.get("data_dir") else None
    return base, lib, data

# ---------------------------------------------------------------- RC1: DOI extraction + validity
# Start anchor for a DOI; the body is captured greedily then trimmed.
_DOI_START = re.compile(r"10\.\d{4,9}/", re.IGNORECASE)
_DOI_BODYCHAR = r"[A-Za-z0-9._;:()/\-]"
_DOI_TRAIL = re.compile(r"[.,;:)\]}>]+$")
_DOI_FULL = re.compile(r"^10\.\d{4,9}/\S+$", re.IGNORECASE)
# B2: unicode dashes/minus (U+2010..U+2015, U+2212) are PDF-extraction typography artifacts that
# never appear in a registered DOI but that NCBI idconv 400s on -- reject them at the validity gate
# (_DOI_FULL's \S+ otherwise admits them). NOT angle brackets: legit SICI-format DOIs (old Wiley /
# Blackwell, ~1996-2005) carry literal '<'/'>' in the suffix, so rejecting those here would silently
# drop real papers from the citation graph + index. idconv 400s on any odd DOI are handled instead
# by doi_to_pmcid_batch's one-at-a-time fallback, not this global gate.
_DOI_BAD_CHARS = re.compile(r"[‐-―−]")

def normalize_doi(doi):
    if not doi: return ""
    d = doi.strip().lower()
    if d.startswith("https://doi.org/"): d = d[len("https://doi.org/"):]
    elif d.startswith("http://dx.doi.org/"): d = d[len("http://dx.doi.org/"):]
    elif d.startswith("doi:"): d = d[4:]
    return _DOI_TRAIL.sub("", d).rstrip(".")

def is_valid_doi(doi):
    """Well-formedness: matches 10.<reg>/<suffix> with a non-empty suffix, and rejects unicode-dash
    typography artifacts (U+2010..U+2015, U+2212) that only appear from PDF-extraction mangling,
    never in a registered DOI (B2). Angle brackets are deliberately NOT rejected -- legit SICI-format
    DOIs contain literal '<'/'>'."""
    if not doi:
        return False
    d = doi.strip()
    return not _DOI_BAD_CHARS.search(d) and bool(_DOI_FULL.match(d))

def is_suspicious_doi(doi):
    """Flag DOIs that look like line-wrap TRUNCATIONS (the RC1 '10.1002/cphy' class).

    Real DOI suffixes essentially always contain a digit or a '.'; the observed truncations
    ('cphy', 'archinte') are bare lowercase journal-abbrev tokens with neither. Conservative:
    only flags suffixes that have NO digit AND NO dot (won't false-positive normal DOIs)."""
    if not is_valid_doi(doi): return True
    suffix = doi.split("/", 1)[1]
    return not any(c.isdigit() for c in suffix) and "." not in suffix

_BODYCHAR_RE = re.compile(_DOI_BODYCHAR)
_SUSPICIOUS_DROPPED = set()  # F2: valid-but-suspicious DOIs already reported dropped (dedupe stderr)

def extract_doi_from_text(text, max_chars=None):
    """Extract the first well-formed, non-suspicious DOI from text, re-joining line-wrapped DOIs.

    The old reverse_citations/forward_citations regexes collapsed whitespace before matching, so a
    DOI split across a line break ('10.1002/cphy.\\nc140066') truncated at the wrap. Here we consume
    DOI body characters and, on hitting whitespace, re-join the wrapped continuation ONLY when the
    last body char is DOI-internal punctuation ('.', '-', '/') -- the pattern of a mid-DOI wrap --
    which avoids over-joining a DOI that legitimately ends at end-of-line followed by prose.
    Returns '' if no non-suspicious DOI is found (the is_suspicious_doi gate drops truncations)."""
    if not text: return ""
    if max_chars is not None: text = text[:max_chars]
    n = len(text)
    for m in _DOI_START.finditer(text):
        i = m.end(); body = []
        while i < n:
            c = text[i]
            if _BODYCHAR_RE.match(c):
                body.append(c); i += 1
            elif c in " \t\r\n­":  # whitespace / soft-hyphen: maybe a wrapped DOI
                if body and body[-1] in ".-/":      # mid-DOI wrap -> rejoin (keep all chars;
                    j = i                            # DOIs aren't soft-hyphenated by typesetters)
                    while j < n and text[j] in " \t\r\n­": j += 1
                    if j < n and _BODYCHAR_RE.match(text[j]): i = j; continue
                break
            else:
                break
        cand = normalize_doi(m.group(0) + "".join(body))
        if is_valid_doi(cand):
            if is_suspicious_doi(cand):
                # F2: a valid-but-suspicious DOI (no digit AND no dot in the suffix) is
                # dropped as a likely line-wrap truncation. Surface each unique drop once so
                # a rare legit single-token DOI (e.g. 10.1093/nar) is not lost silently.
                if cand not in _SUSPICIOUS_DROPPED:
                    _SUSPICIOUS_DROPPED.add(cand)
                    print(f"[litpipe] dropped valid-but-suspicious DOI {cand!r} "
                          f"(suffix has no digit and no dot)", file=sys.stderr)
                continue
            return cand
    return ""

# ---------------------------------------------------------------- RIS metadata parsing
_RIS_TAG = re.compile(r"^([A-Z][A-Z0-9])\s{2}-\s?(.*)$")

def parse_ris(ris_path):
    """Parse a .ris file's canonical metadata. Single source of truth for the two former
    copies (index_portfolio + harvest_citations, Stage 3 c10/c11).

    Returns {doi, year, title, venue, lastname, authors, authors_raw}:
      - year is int-or-None (paper_metadata.year is INTEGER; the ingest invariant).
      - authors_raw is an ALIAS of authors, for the harvest/enw/nbib-family consumers
        that read that key; index/ingest reads authors.
    Union of the two prior behaviors: single-pass so a wrapped TI/JO value continuing on
    an untagged line is joined not truncated (T6); accepts the AU/A1, PY/Y1, TI/T1 tag
    aliases; falls back to a doi.org DOI in a UR line when no DO tag is present (hand-dropped
    EndNote/Zotero .ris). Reads with errors='replace' so a stray non-UTF-8 byte degrades
    gracefully instead of raising. Pipeline-written .ris are single-line with a DO tag, so
    canonical sidecars are unaffected."""
    out = {"doi": "", "year": None, "lastname": "", "title": "", "venue": "", "authors": []}
    try:
        with open(ris_path, encoding="utf-8", errors="replace") as f:
            text = f.read()
    except OSError:
        out["authors_raw"] = out["authors"]
        return out
    ur_vals = []
    cur_tag = None
    for line in text.splitlines():
        m = _RIS_TAG.match(line)
        if not m:
            cont = line.strip()  # untagged continuation of a wrapped TI/JO value
            if cont and cur_tag in ("TI", "T1") and out["title"]:
                out["title"] = (out["title"] + " " + cont).strip()
            elif cont and cur_tag == "JO" and out["venue"]:
                out["venue"] = (out["venue"] + " " + cont).strip()
            continue
        tag, val = m.group(1), m.group(2).strip()
        cur_tag = tag
        if tag == "DO" and not out["doi"]: out["doi"] = val.lower()
        elif tag == "UR": ur_vals.append(val)
        elif tag in ("PY", "Y1") and out["year"] is None and val[:4].isdigit():
            out["year"] = int(val[:4])
        elif tag in ("TI", "T1") and not out["title"]: out["title"] = val
        elif tag == "JO" and not out["venue"]: out["venue"] = val
        elif tag in ("AU", "A1"):
            out["authors"].append(val)
            if not out["lastname"]:
                out["lastname"] = val.split(",")[0].strip() if "," in val else val.split()[0].strip()
    if not out["doi"]:  # UR-only-DOI fallback: a .ris with no DO but a doi.org UR still has a DOI
        for u in ur_vals:
            d = extract_doi_from_text(u)
            if d:
                out["doi"] = d
                break
    out["authors_raw"] = out["authors"]  # alias for the harvest/enw/nbib-family consumers
    return out

# ---------------------------------------------------------------- RC5: non-clobbering sidecar merge
_ENRICHED_FIELDS = ("doi", "title", "subtitle", "year", "journal", "authors",
                    "pmid", "pmcid", "abstract", "figures", "metadata_source",
                    "metadata_correction_note")

def _is_empty(x):
    """True for a genuinely-absent value (None / '' / [] / {}), but NOT numeric 0 or False.

    F1: the old merge_sidecar guard treated a real 0/0.0/False in `new` as empty and let the
    old value clobber it. One predicate applied to BOTH sides keeps 0 a real value -- load
    bearing once merge_sidecar is extended to preserve n_formulas (D2), where 0 is legitimate."""
    return x is None or x in ("", [], {})

def merge_sidecar(old, new):
    """Return `new` augmented so it never DROPS a populated enriched field present in `old`.

    Used when an extractor re-writes a sidecar (e.g. extract --refresh / backfill --refresh):
    text/extractor/formula fields come from `new`, but doi/title/authors/figures etc. are
    preserved from `old` when `new` lacks them. Prevents the confirmed --refresh metadata wipe."""
    if not old: return new
    out = dict(new)
    for k in _ENRICHED_FIELDS:
        ov = old.get(k)
        if not _is_empty(ov) and _is_empty(out.get(k)):
            out[k] = ov
    return out

# ---------------------------------------------------------------- RC10: Drive-lock-tolerant DB open
def connect_db(db_path, on_fail="raise", tries=3, delays=(2, 4), read_only=False):
    """Open a DuckDB file, retrying the transient lock/IO errors GoogleDriveFS raises
    when it holds portfolio.duckdb open mid-sync (RC10).

    Single source of truth for the retry-open that index_portfolio.connect_with_retry and
    the two enrich_* connect_db copies each carried (2026-07 Stage 3 c9); also serves the
    snowball / seed_queue read-only candidate reads (read_only=True).

    on_fail chooses the exhaustion behavior:
      - "raise" (default): re-raise the last error after `tries` attempts -- the
        index_portfolio rebuild wants the traceback.
      - "exit": raise SystemExit with a paste-ready message -- the enrich_* CLI
        entry-points want a clean abort, not a traceback.

    `tries`/`delays` set the retry envelope; the inter-attempt sleep is
    delays[min(attempt-1, len(delays)-1)] (so delays=(3,) is a constant 3s). A broad
    `except Exception` is deliberate: duckdb's lock/IO error type is not a stable public
    class. duckdb is imported lazily so lit_util stays stdlib-pure for its many importers."""
    import duckdb
    if tries < 1:  # a shared public helper now: fail fast on a computed tries=0 rather than `raise None`
        raise ValueError(f"connect_db: tries must be >= 1, got {tries!r}")
    last = None
    for attempt in range(1, tries + 1):
        try:
            return duckdb.connect(db_path, read_only=read_only)
        except Exception as e:
            last = e
            if attempt < tries:
                wait = delays[min(attempt - 1, len(delays) - 1)]
                print(f"  [RC10] DB open failed (attempt {attempt}/{tries}): {e}\n"
                      f"        suspect Google Drive holding {db_path} open; retrying in {wait}s ...",
                      file=sys.stderr)
                time.sleep(wait)
    msg = (f"could not open {db_path} after {tries} attempts -- DB locked (suspect Google "
           f"Drive sync holding it open; pause Drive and retry). Last error: {last}")
    if on_fail == "exit":
        raise SystemExit(f"ERROR: {msg}")
    print(f"  [RC10] {msg}", file=sys.stderr)
    raise last
