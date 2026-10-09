"""Portfolio-wide audit of literature-pipeline outputs: the health check the maintainer (and the
scheduled runner) reads to decide whether the pipeline is healthy.

For every active project in projects.json it reports, per library (top level only):
  - holdings: PDFs, and TEXT_ONLY holdings (DEC-08): a `.fulltext.json` with no PDF that was not
    extracted from a PDF (pmc writes `has_pdf: false`; older JATS sidecars carry no `has_pdf` and
    no `extracted_from_pdf: true`). A `.ris` whose stem has such a sidecar belongs to it.
  - identity flags, review items that are neither holdings nor orphans: `<stem>.identity.json`
    with identity FLAG or doc_kind SUPPLEMENT (unpaywall, preprint), and a `<stem>.fulltext.json`
    with identity FLAG (pmc)
  - PDF integrity (every PDF: %PDF magic, under 10 KB) and sidecar integrity (parse, content)
  - sidecars waiting for OCR (`needs_ocr: true`, written empty by extract_pdf_fulltext's text
    validity gate): their own INFO line, the count and the first stems; not "empty" sidecars
  - pairing: orphan sidecars, orphan `.ris`, orphan `.identity.json`, PDFs without sidecar or `.ris`
  - text damage: sidecars and `.ris` carrying a ligature (U+FB00 to U+FB06), a no-break space,
    a character reference (`litpipe.text.unescape` changes it) or a markup tag
    (`litpipe.text.strip_tags` changes it)
  - `_mismatch/` quarantine folders (DEC-07; restoring them is a separate, live step)
  - filename quality, and deep checks (DOI consistency, filename versus `.ris` metadata)
  - queue history: sweep's class counts (`report.csv` rows with section=class) and residual items
    (`residual.csv`, a blank class is UNKNOWN), from the project dir, the project's artifact
    folder (projects.json `artifact_dir`, litpipe.config.artifact_dir), `--artifact-dir`, and any
    archive folder a `--report-dir` names (searched with its subfolders; a run there counts for
    a project when its report names that project's library, and with --project a run that names
    no library counts too); legacy runs with no residual fall back to the stage reports
    (`outcome`, else `litpipe.outcomes.from_legacy`); fetched queue titles against their `.ris`
  - the index (portfolio.duckdb, opened read-only): freshness (`index_runs` when present, else
    max(refreshed_at)) against the newest library file and the file count, and a DB-versus-disk
    reconcile
  - live pipeline runs from the state file (`litpipe.state.live_runs`, only when the file exists)

Severity: FAIL (exit 1), WARN, INFO. A FAIL is a registered active library that is missing, a
fake PDF, or an unparseable sidecar. Every check prints its count and at most 20 items; --full
prints all. The tool is read-only: it writes nothing but the --json file, and never creates the
state directory or the index.

Usage:
  python audit_portfolio.py                     # every active project
  python audit_portfolio.py --project research_a   # one project (a subproject tail works too)
  python audit_portfolio.py --json out.json     # also write machine-readable output
  python audit_portfolio.py --full              # print every item, not the first 20
  python audit_portfolio.py --project research_a --report-dir <archive folder>   # archived reports
"""
import argparse
import csv
import datetime as _dt
import difflib
import json
import os
import re
import sys
import time
from collections import Counter
from pathlib import Path

import lit_util
from lit_util import companion_path  # dot-safe sidecar naming
from litpipe import config as litconfig
from litpipe import doi as _doi
from litpipe import text as _text
from litpipe.outcomes import Kind, from_legacy
from pdf_text_clean import LIGATURES

lit_util.utf8_stdout()

CONFIG_PATH = Path(__file__).parent / "projects.json"

ITEM_CAP = 20                  # items printed per check without --full
DB_NAME = "portfolio.duckdb"
SIDECAR = ".fulltext.json"
IDENTITY = ".identity.json"
TINY_PDF_BYTES = 10_000
STALE_RUN_S = 6 * 3600         # T2 step 5: a live run older than this is named in a WARN
INDEX_SLACK_S = 120            # a file this much newer than the index stamp makes the index stale
TITLE_MIN_SIMILARITY = 0.6     # T2 step 6 (I28): fetched queue title against its .ris title
LIBRARY_SUFFIXES = (".pdf", ".ris", SIDECAR, IDENTITY)

# T2 step 2: the lastname capture stops at the first "_" (it was `[\w\-]*`, and `\w` matches "_",
# so "2023_Chen_PunicaMulti..." captured "Chen_PunicaMulti..." and false-alarmed against "Chen").
FILENAME_RE = re.compile(r"^(\d{4})_([A-Za-z][^_]*)_")

# Residual classes that need a person or a fix (sweep 0.5 table plus UNKNOWN for a blank class).
ATTENTION_CLASSES = frozenset({"UNKNOWN", "PENDING", "CONFIG"})

# Sweep run artifacts, current and legacy names (dispatch 0.5 "Queue names"):
# lit_pull_queue[.<tag>].<YYYY-MM-DD>[.<N>].<stage>.csv, plus the legacy .processed.<n>.csv.
_ARTIFACT_RE = re.compile(
    r"lit_pull_queue(?:\.(?P<tag>[a-z][a-z0-9_-]{0,31}))?"
    r"\.(?P<date>\d{4}-\d{2}-\d{2})(?:\.(?P<seq>\d+))?"
    r"\.(?P<stage>normalized|unpaywall|pmc|preprint|residual|report|processed)"
    r"(?:\.(?P<legacy_seq>\d+))?\.csv")
_REPORT_COLUMNS = ["section", "name", "count", "detail"]
_STAGES = ("unpaywall", "pmc", "preprint")
_STAGE_FILENAME = {"unpaywall": "filename", "pmc": "filename", "preprint": "preprint_filename"}

_LIG_RE = re.compile("[" + "".join(chr(c) for c in sorted(LIGATURES)) + "]")
DAMAGE_KINDS = ("ligature", "nbsp", "entity", "tag")
_SIDECAR_TEXT_KEYS = ("title", "abstract", "text", "body")

_RIS_DO = re.compile(r"^DO\s{2}-\s?(.*)$", re.M)
_RIS_PY = re.compile(r"^PY\s{2}-\s?(\d{4})", re.M)
_RIS_AU = re.compile(r"^AU\s{2}-\s?([^,\n]+)", re.M)
_RIS_TI = re.compile(r"^(?:TI|T1)\s{2}-\s?(.*)$", re.M)


# ---------------------------------------------------------------- registry
def load_registry():
    """The whole projects.json at CONFIG_PATH (exits 2 with the template hint when absent)."""
    return lit_util.load_projects_config(CONFIG_PATH)


def load_config():
    """Load projects.json. Returns dict[name -> {tier, lib_dir, data_dir?, parent?, active}]."""
    return load_registry().get("projects", {})


def select_projects(projects, project=None):
    """[(name, entry)] for the active projects, optionally only `project` (or a subproject tail)."""
    out = []
    for name, p in (projects or {}).items():
        if not isinstance(p, dict) or not p.get("active", True):
            continue
        if project and not (name == project or name.endswith("/" + project)):
            continue
        out.append((name, p))
    return out


def discover(projects, project=None):
    """(libs, missing): libs = [(name, entry, lib_path, tier, data_path)] for every selected active
    project whose library exists; missing = [{project, lib, reason}] for the rest (a FAIL)."""
    libs, missing = [], []
    for name, p in select_projects(projects, project):
        if not p.get("lib_dir"):
            missing.append({"project": name, "lib": "", "reason": "no lib_dir in projects.json"})
            continue
        _base, lp, dp = lit_util.lib_paths(name, p)
        if not lp.is_dir():
            missing.append({"project": name, "lib": str(lp), "reason": "library directory not found"})
            continue
        libs.append((name, p, lp, p.get("tier", 2), dp))
    return libs, missing


def discover_libs():
    """[(project_name, lib_path, tier, data_path|None)] for the active projects whose library exists
    (kept for callers of the old API; `discover` also returns the missing ones)."""
    libs, _missing = discover(load_config())
    return [(name, lp, tier, dp) for name, _p, lp, tier, dp in libs]


# ---------------------------------------------------------------- shared predicates
def read_json(path):
    """(dict, "") for a JSON object file, else (None, the reason)."""
    try:
        with open(path, encoding="utf-8") as fh:
            d = json.load(fh)
    except (OSError, ValueError) as e:      # JSONDecodeError and UnicodeDecodeError are ValueErrors
        return None, f"{type(e).__name__}: {str(e)[:80]}"
    if not isinstance(d, dict):
        return None, "not a JSON object"
    return d, ""


def identity_flag(d):
    """The review reason when a sidecar (`.identity.json` or `.fulltext.json`) flags its file, else
    "": identity FLAG (the file is another work), or doc_kind SUPPLEMENT (the file is a supplement)."""
    if not isinstance(d, dict):
        return ""
    if str(d.get("identity") or "").strip().upper() == "FLAG":
        return "identity=FLAG"
    if str(d.get("doc_kind") or "").strip().upper() == "SUPPLEMENT":
        return "doc_kind=SUPPLEMENT"
    return ""


def _has_text(d):
    t = d.get("text") if isinstance(d, dict) else None
    return isinstance(t, str) and bool(t.strip())


def has_content(d):
    """A sidecar with any text: `text`, `abstract` or `body`."""
    return isinstance(d, dict) and any(isinstance(d.get(k), str) and d.get(k).strip()
                                       for k in ("text", "abstract", "body"))


def needs_ocr(d):
    """A sidecar extract_pdf_fulltext's text-validity gate wrote empty for OCR (`needs_ocr: true`)."""
    return isinstance(d, dict) and d.get("needs_ocr") is True


def ocr_line(names, first=3):
    """The needs_ocr line's detail: the count and the first stems."""
    stems = [n[:-len(SIDECAR)] if n.lower().endswith(SIDECAR) else n for n in names[:first]]
    more = f", +{len(names) - first} more" if len(names) > first else ""
    return f"{len(names)} (first: {', '.join(stems)}{more}; extract_pdf_fulltext.py --ocr)"


def is_text_only_sidecar(d):
    """The shared TEXT_ONLY predicate (DEC-08) for a `.fulltext.json` whose PDF is absent: it is a
    JSON object, is not identity-flagged, carries non-empty `text`, and was not extracted from a
    PDF: `has_pdf` is false (pmc since W2a), or `has_pdf` is absent and `extracted_from_pdf` is not
    true (JATS sidecars written before W2a). A sidecar whose PDF was deleted or renamed
    (`has_pdf: true`, or `extracted_from_pdf: true`) is an orphan, not a holding."""
    if not isinstance(d, dict) or identity_flag(d) or not _has_text(d):
        return False
    if "has_pdf" in d:
        return d.get("has_pdf") is False
    return d.get("extracted_from_pdf") is not True


def text_damage(s):
    """The damage kinds a string shows: a ligature (pdf_text_clean.LIGATURES), U+00A0, a character
    reference (litpipe.text.unescape changes it) or a markup tag (litpipe.text.strip_tags changes
    it). The `&` and `<` guards only skip strings the two functions would return unchanged."""
    if not s:
        return []
    out = []
    if _LIG_RE.search(s):
        out.append("ligature")
    if "\N{NO-BREAK SPACE}" in s:
        out.append("nbsp")
    if "&" in s and _text.unescape(s) != s:
        out.append("entity")
    if "<" in s and _text.strip_tags(s) != s:
        out.append("tag")
    return out


def _sidecar_strings(d):
    return "\n".join(d[k] for k in _SIDECAR_TEXT_KEYS if isinstance(d.get(k), str))


def _ris_fields(text):
    def first(rx):
        m = rx.search(text)
        return m.group(1).strip() if m else ""
    return {"doi": first(_RIS_DO).lower(), "py": first(_RIS_PY), "au": first(_RIS_AU),
            "ti": first(_RIS_TI)}


def _name_key(s):
    """A surname as a comparison key: ASCII-folded (Periard for Périard, Luthi for Lüthi), lower
    case, letters and digits only (St. Pierre and Pérez-López fold to stpierre, perezlopez)."""
    return re.sub(r"[^a-z0-9]", "", lit_util.safe_ascii(s or "").lower())


def lastname_matches(file_last, ris_au):
    """Does the filename's lastname name the `.ris` first author? Both sides are folded the same
    way (the old check folded only the `.ris` side, so every non-ASCII filename mismatched). The
    `.ris` name may be "Last, First" or, without a comma, "First Last"; it matches when the file
    name is a prefix of the whole surname (compounds joined: GuimaraesFerreira), or equals one of
    its parts of 3+ letters (particles and compounds: de Keijzer, Ploutz-Snyder, Van Nguyen)."""
    f = _name_key(file_last)
    if not f:
        return True
    raw = (ris_au or "").strip()
    surname = raw.split(",", 1)[0] if "," in raw else raw
    whole = _name_key(surname)
    if not whole or whole.startswith(f):
        return True
    # split before folding: safe_ascii drops U+2010/U+2011, which would join the parts
    parts = [_name_key(p) for p in re.split(r"[\s\-\N{HYPHEN}\N{NON-BREAKING HYPHEN}]+", surname)]
    return any(len(p) >= 3 and p == f for p in parts)


def _norm_doi(raw):
    raw = (raw or "").strip()
    if not raw:
        return ""
    return _doi.normalise(raw) or raw.lower()


def _truthy(v):
    return str(v or "").strip().lower() in ("true", "1", "yes")


# ---------------------------------------------------------------- library scan
def scan_library(lib) -> dict:
    """One pass over the top level of a library: holdings, flags, pairing, integrity and text
    damage. Lists hold file names. `_records` (keyed by case-folded stem) feeds the deep checks
    and the title check and is not written to JSON."""
    lib = Path(lib)
    out = {
        "lib": str(lib), "exists": False, "error": None,
        "n_pdfs": 0, "n_sidecars": 0, "n_ris": 0, "n_identity": 0,
        "pdf_names": [], "pdf_holdings": 0, "text_only": [], "text_only_with_ris": 0,
        "flags": [], "fake_pdfs": [], "tiny_pdfs": [], "bad_sidecars": [], "empty_sidecars": [],
        "needs_ocr": [],
        "bad_identity": [], "orphan_sidecars": [], "orphan_ris": [], "orphan_identity": [],
        "pdfs_no_sidecar": [], "pdfs_no_ris": [],
        "damage": {"sidecar": {k: [] for k in DAMAGE_KINDS}, "ris": {k: [] for k in DAMAGE_KINDS}},
        "mismatch": {"dir": str(lib / "_mismatch"), "files": [], "pdfs": 0},
        "al_antipattern": [], "unknown_year": [], "upper_lastname_sample": [],
        "newest_mtime": None, "_records": {},
    }
    try:
        entries = list(os.scandir(lib))
    except OSError as e:
        out["error"] = f"{type(e).__name__}: {e}"
        return out
    out["exists"] = True
    pdfs, sidecars, ris, idents = {}, {}, {}, {}
    newest = None
    for e in entries:
        try:
            if not e.is_file():
                continue
            mtime = e.stat().st_mtime
        except OSError:
            continue
        name, low = e.name, e.name.lower()
        if low.endswith(SIDECAR):
            sidecars[name[:-len(SIDECAR)].casefold()] = name
        elif low.endswith(IDENTITY):
            idents[name[:-len(IDENTITY)].casefold()] = name
        elif low.endswith(".ris"):
            ris[name[:-4].casefold()] = name
        elif low.endswith(".pdf"):
            pdfs[name[:-4].casefold()] = name
        else:
            continue
        newest = mtime if newest is None else max(newest, mtime)
    out["newest_mtime"] = newest
    out.update(n_pdfs=len(pdfs), n_sidecars=len(sidecars), n_ris=len(ris), n_identity=len(idents),
               pdf_names=sorted(pdfs.values()))
    recs = out["_records"]

    def rec(stem):
        return recs.setdefault(stem, {"pdf": pdfs.get(stem), "sc": None, "ris": None,
                                      "flag": "", "flag_sources": [], "text_only": False})

    # identity verdicts first: a flagged stem is a review item everywhere below
    for stem, name in sorted(idents.items()):
        d, err = read_json(lib / name)
        if d is None:
            out["bad_identity"].append((name, err))
            continue
        reason = identity_flag(d)
        if reason:
            r = rec(stem)
            r["flag"] = r["flag"] or reason
            r["flag_sources"].append(f"{name} ({d.get('source') or 'identity'})")
        elif stem not in pdfs:
            out["orphan_identity"].append(name)

    for stem, name in sorted(sidecars.items()):
        d, err = read_json(lib / name)
        r = rec(stem)
        if d is None:
            out["bad_sidecars"].append((name, err))
            if stem not in pdfs and not r["flag"]:
                out["orphan_sidecars"].append(name)
            continue
        txt = sum(len(d[k]) for k in ("text", "body", "abstract") if isinstance(d.get(k), str))
        r["sc"] = {"doi": str(d.get("doi") or "").strip().lower(), "len": txt}
        reason = identity_flag(d)
        if reason:
            r["flag"] = r["flag"] or reason
            r["flag_sources"].append(f"{name} (fulltext)")
        for k in text_damage(_sidecar_strings(d)):
            out["damage"]["sidecar"][k].append(name)
        if r["flag"]:
            continue
        ocr = needs_ocr(d)
        if ocr:
            out["needs_ocr"].append(name)
        if stem in pdfs:
            if not has_content(d) and not ocr:      # an OCR to-do is empty by design, not damage
                out["empty_sidecars"].append(name)
        elif is_text_only_sidecar(d):
            r["text_only"] = True
            out["text_only"].append(name)
        else:
            out["orphan_sidecars"].append(name)

    for stem, name in sorted(ris.items()):
        r = rec(stem)
        try:
            with open(lib / name, encoding="utf-8", errors="replace") as fh:
                text = fh.read()
        except OSError:
            text = ""
        r["ris"] = _ris_fields(text)
        for k in text_damage(text):
            out["damage"]["ris"][k].append(name)
        if stem in pdfs or r["flag"]:
            continue
        if r["text_only"]:
            out["text_only_with_ris"] += 1
        else:
            out["orphan_ris"].append(name)

    for stem, name in sorted(pdfs.items()):
        r = rec(stem)
        p = lib / name
        try:
            with open(p, "rb") as f:
                magic = f.read(4)
            size = p.stat().st_size
            if magic != b"%PDF":
                out["fake_pdfs"].append(name)
            elif size < TINY_PDF_BYTES:
                out["tiny_pdfs"].append((name, size))
        except OSError as e:
            out["fake_pdfs"].append(f"{name} ({e})")
        if r["flag"]:
            continue
        out["pdf_holdings"] += 1
        if stem not in sidecars:
            out["pdfs_no_sidecar"].append(name)
        if stem not in ris:
            out["pdfs_no_ris"].append(name)

    for stem, r in sorted(recs.items()):
        if r["flag"]:
            shown = r["pdf"] or (sidecars.get(stem) or idents.get(stem) or stem)
            out["flags"].append({"file": shown, "reason": r["flag"],
                                 "sources": r["flag_sources"], "has_pdf": bool(r["pdf"])})

    names = sorted(pdfs.values())
    out["al_antipattern"] = [n for n in names if re.search(r"_al_", n)]
    out["unknown_year"] = [n for n in names if n.startswith("Unknown_")]
    out["upper_lastname_sample"] = [n for n in names if re.match(r"^\d{4}_[A-Z]{2,}_", n)][:5]

    mm = lib / "_mismatch"
    if mm.is_dir():
        files = []
        for dirpath, _dirs, fnames in os.walk(mm):
            for f in fnames:
                files.append(os.path.relpath(os.path.join(dirpath, f), mm))
        out["mismatch"]["files"] = sorted(files)
        out["mismatch"]["pdfs"] = sum(1 for f in files if f.lower().endswith(".pdf"))
    return out


def audit_lib(name: str, lib: Path) -> dict:
    a = scan_library(lib)
    a["project"] = name
    return a


# ---------------------------------------------------------------- deep checks
def doi_from_sources(pdf: Path) -> dict:
    """Get DOI from sidecar, ris, and PDF text. Return all three for comparison."""
    out = {"sidecar": "", "ris": "", "pdf": ""}
    sc = companion_path(pdf, SIDECAR)
    if sc.exists():
        d, _err = read_json(sc)
        if d is not None:
            out["sidecar"] = (d.get("doi") or "").lower()
    rp = companion_path(pdf, ".ris")
    if rp.exists():
        try:
            with open(rp, encoding="utf-8", errors="replace") as fh:
                out["ris"] = _ris_fields(fh.read())["doi"]
        except OSError:
            pass
    return out


def deep_audit_lib(lib: Path, scan=None) -> dict:
    """Deeper checks: DOI consistency + validity + sidecar text length + filename alignment.
    Reads the library once through `scan_library` (pass a scan to reuse it)."""
    from lit_util import is_valid_doi as _valid_doi, is_suspicious_doi as _susp_doi
    scan = scan if scan is not None else scan_library(lib)
    sidecar_lens = []        # (filename, len)
    doi_mismatch = []        # (filename, sources)
    malformed_dois = []      # (filename, source, doi): truncated or invalid DOIs on disk (RC1)
    fn_misalign = []         # (filename, kind, detail)
    doi_to_files = {}        # doi -> [filename]
    for _stem, r in sorted(scan["_records"].items()):
        pdf = r["pdf"]
        if not pdf or r["flag"]:
            continue
        sc, ris = r["sc"], r["ris"]
        if sc is not None:
            sidecar_lens.append((pdf, sc["len"]))
        dois = {"sidecar": (sc or {}).get("doi", ""), "ris": (ris or {}).get("doi", ""), "pdf": ""}
        non_empty = {k: v for k, v in dois.items() if v}
        if len(set(non_empty.values())) > 1:
            doi_mismatch.append((pdf, dois))
        for src, dv in non_empty.items():
            if not _valid_doi(dv) or _susp_doi(dv):
                malformed_dois.append((pdf, src, dv))
        canonical = next(iter(non_empty.values()), "")
        if canonical:
            doi_to_files.setdefault(canonical, []).append(pdf)
        m = FILENAME_RE.match(pdf)
        if m and ris is not None:
            fn_year, fn_last = m.group(1), m.group(2)
            exp_year, exp_last_raw = ris["py"], ris["au"]
            if exp_year and fn_year and abs(int(exp_year) - int(fn_year)) > 1:
                fn_misalign.append((pdf, "year", f"file={fn_year} ris={exp_year}"))
            elif exp_last_raw and not lastname_matches(fn_last, exp_last_raw):
                fn_misalign.append((pdf, "lastname", f"file={fn_last} ris={exp_last_raw}"))
    return {"sidecar_lens": sidecar_lens, "doi_mismatch": doi_mismatch,
            "malformed_dois": malformed_dois, "fn_misalign": fn_misalign,
            "doi_to_files": doi_to_files}


# ---------------------------------------------------------------- queue history
def project_artifact_dir(name, entry, registry=None):
    """Where sweep writes this project's run artifacts: litpipe.config.artifact_dir(key, cfg) when
    that accessor exists (projects.json "artifact_dir", global or per project; unset is the project
    root), else the project root. `registry` is the whole loaded projects.json."""
    root = lit_util.project_root(name, entry)
    fn = getattr(litconfig, "artifact_dir", None)
    if fn is None:
        return root
    cfg = registry if registry is not None else {"projects": {name: entry}}
    try:
        return Path(fn(name, cfg))
    except ValueError as e:                # config.ConfigError: say so, read the project root
        print(f"[WARN] {name}: artifact_dir unusable ({e}); reading the project folder only", file=sys.stderr)
        return root


def _walk(top):
    return [Path(dirpath) for dirpath, _dirs, _files in os.walk(top)]


def report_dirs(name, entry, projects=None, artifact_dir=None, extra_dirs=None, registry=None,
                claim_unnamed=False):
    """[(dir, mode)] where this project's sweep reports may sit. mode "own": every run there is the
    project's; "by_destination": a folder that may hold several projects' runs, where a run counts
    only when its report names this project's library (a legacy run naming none is unattributed);
    "claimed": the same, except that a run naming no library counts too (`claim_unnamed`: the
    caller asked about this one project).

    Searched: the project dir; its artifact folder (project_artifact_dir); `artifact_dir`
    (relative to the project dir, or absolute); each of `extra_dirs` (--report-dir) with its
    subfolders. No archive layout is built in: archived reports are read where a --report-dir
    names them."""
    root = lit_util.project_root(name, entry)
    if registry is None and projects is not None:
        registry = {"projects": projects}
    out = [(root, "own"), (project_artifact_dir(name, entry, registry), "own")]
    if artifact_dir:
        p = Path(artifact_dir).expanduser()
        out.append((p if p.is_absolute() else root / p, "own"))
    for extra in extra_dirs or ():
        p = Path(extra).expanduser()
        top = p if p.is_absolute() else root / p
        out += [(d, "claimed" if claim_unnamed else "by_destination") for d in _walk(top)]
    seen, uniq = set(), []
    for d, mode in out:
        k = os.path.normcase(os.path.abspath(d))
        if k not in seen:
            seen.add(k)
            uniq.append((Path(d), mode))
    return uniq


def find_runs(dirs) -> list:
    """Sweep runs in `dirs`: [{dir, mode, tag, run_id, files: {stage: name, processed: [names]}}],
    oldest first. Current run-id names and legacy dated names alike."""
    runs = {}
    for d, mode in dirs:
        try:
            names = sorted(os.listdir(d))
        except OSError:
            continue
        for n in names:
            m = _ARTIFACT_RE.fullmatch(n)
            if not m or (m["legacy_seq"] and (m["stage"] != "processed" or m["seq"])):
                continue
            run_id = m["date"] + (f".{m['seq']}" if m["seq"] else "")
            key = (os.path.normcase(str(d)), m["tag"] or "", run_id)
            r = runs.setdefault(key, {"dir": str(d), "mode": mode, "tag": m["tag"] or "",
                                      "run_id": run_id, "files": {}})
            if m["stage"] == "processed":
                r["files"].setdefault("processed", []).append(n)
            else:
                r["files"][m["stage"]] = n
    return sorted(runs.values(), key=_run_order)


def _run_order(r):
    date, _, seq = r["run_id"].partition(".")
    return (date, int(seq or 1), 0 if r["mode"] == "own" else 1)


def _read_csv(path):
    """(fieldnames, rows, error) of a CSV; ([], [], reason) when unreadable."""
    try:
        with open(path, encoding="utf-8-sig", errors="replace", newline="") as fh:
            rd = csv.DictReader(fh)
            rows = list(rd)
            return list(rd.fieldnames or []), rows, ""
    except (OSError, csv.Error) as e:
        return [], [], f"{type(e).__name__}: {str(e)[:80]}"


def stage_label(stage, r):
    """What one stage report row says, without re-classifying: "OK" (downloaded or already held),
    "IDENTITY_FLAG" (never counted as downloaded), a Kind name (the typed `outcome` column when
    present, else `from_legacy` of the legacy error or status), or "UNKNOWN" when blank."""
    raw = (r.get("status") if stage == "preprint" else r.get("error")) or ""
    raw = raw.strip()
    if (str(r.get("identity") or "").strip().upper() == "FLAG"
            or str(r.get("doc_kind") or "").strip().upper() == "SUPPLEMENT"
            or raw.startswith("DOI_MISMATCH")):
        return "IDENTITY_FLAG"
    if _truthy(r.get("downloaded")):
        return "OK"
    if stage == "unpaywall" and (r.get("oa_status") or "").strip() == "SKIP_EXISTS":
        return "OK"
    if stage != "unpaywall" and (_truthy(r.get("skipped")) or raw == "ALREADY_EXISTS"
                                 or (r.get("winning_source") or "").strip() == "ALREADY_EXISTS"):
        return "OK"
    typed = (r.get("outcome") or "").strip().upper()
    if typed in Kind.__members__:
        return typed
    if not raw and stage == "unpaywall":
        raw = (r.get("oa_status") or "").strip()
    if not raw:
        return "UNKNOWN"
    return str(from_legacy(raw, stage))


def stage_verdicts(run):
    """{doi: [(stage, label)]} from a run's stage reports, in stage order."""
    d = Path(run["dir"])
    out = {}
    for stage in _STAGES:
        name = run["files"].get(stage)
        if not name:
            continue
        _f, rows, _err = _read_csv(d / name)
        for r in rows:
            doi = _norm_doi(r.get("doi"))
            if doi:
                out.setdefault(doi, []).append((stage, stage_label(stage, r)))
    return out


def _doi_label(labels):
    """One row's label across stages: fetched when any stage is OK, IDENTITY_FLAG when flagged,
    else the distinct stage kinds in stage order joined by "/"."""
    ls = [lab for _s, lab in labels]
    if "OK" in ls:
        return "fetched"
    if "IDENTITY_FLAG" in ls:
        return "IDENTITY_FLAG"
    return "/".join(dict.fromkeys(ls)) or "UNKNOWN"


def summarise_run(run) -> dict:
    """Counts and items for one run: classes from sweep's report (section=class), items from its
    residual (blank class is UNKNOWN); a legacy report has no classes, so the residual counts;
    a legacy run with no residual falls back to the stage reports."""
    d = Path(run["dir"])
    files = run["files"]
    res = {"run_id": run["run_id"], "tag": run["tag"], "dir": run["dir"], "mode": run["mode"],
           "report": files.get("report"), "residual": files.get("residual"), "format": None,
           "class_source": None, "classes": {}, "downloaded": None, "destination": None,
           "items": [], "unknown": 0, "residual_has_class": None, "disagreement": {},
           "errors": []}
    if files.get("report"):
        fields, rows, err = _read_csv(d / files["report"])
        if err:
            res["errors"].append(f"{files['report']}: {err}")
        if fields[:4] == _REPORT_COLUMNS:
            res["format"] = "run"
            for r in rows:
                sec, nm = (r.get("section") or "").strip(), (r.get("name") or "").strip()
                if sec == "class":
                    res["classes"][nm] = lit_util.coerce_int(r.get("count"))
                elif sec == "run" and nm == "destination":
                    res["destination"] = (r.get("detail") or "").strip()
                elif sec == "total" and nm == "downloaded":
                    res["downloaded"] = lit_util.coerce_int(r.get("count"))
            res["class_source"] = "report"
        elif "stage" in fields and "downloaded" in fields:
            res["format"] = "legacy"
            for r in rows:
                if (r.get("stage") or "").strip() == "total":
                    res["downloaded"] = lit_util.coerce_int(r.get("downloaded"))
    verdicts = None
    if files.get("residual"):
        fields, rows, err = _read_csv(d / files["residual"])
        if err:
            res["errors"].append(f"{files['residual']}: {err}")
        res["residual_has_class"] = "residual_class" in fields
        if not res["residual_has_class"]:
            verdicts = stage_verdicts(run)    # detail only: the class stays UNKNOWN
        for r in rows:
            cls = (r.get("residual_class") or "").strip() or "UNKNOWN"
            doi = (r.get("doi") or "").strip()
            detail = (r.get("reason") or "").strip()
            if verdicts is not None:
                detail = "; ".join(f"{s}={lab}" for s, lab in verdicts.get(_norm_doi(doi), []))
            res["items"].append({"doi": doi, "class": cls, "detail": detail[:160],
                                 "title": (r.get("title") or "").strip()[:60]})
        counted = Counter(i["class"] for i in res["items"])
        if res["class_source"] == "report":
            for c in sorted(set(counted) | {c for c in res["classes"] if c != "fetched"}):
                if counted.get(c, 0) != res["classes"].get(c, 0):
                    res["disagreement"][c] = {"report": res["classes"].get(c, 0),
                                              "residual": counted.get(c, 0)}
        else:
            res["classes"] = dict(counted)
            res["class_source"] = "residual"
    elif res["class_source"] is None:
        verdicts = stage_verdicts(run)
        if verdicts:
            labels = {doi: _doi_label(v) for doi, v in verdicts.items()}
            res["classes"] = dict(Counter(labels.values()))
            res["class_source"] = "stages"
            res["items"] = [{"doi": doi, "class": lab, "title": "",
                             "detail": "; ".join(f"{s}={x}" for s, x in verdicts[doi])[:160]}
                            for doi, lab in sorted(labels.items()) if lab != "fetched"]
    res["unknown"] = sum(1 for i in res["items"] if i["class"] == "UNKNOWN")
    if res["class_source"] == "stages":
        res["unknown"] = sum(1 for i in res["items"] if "UNKNOWN" in i["class"].split("/"))
    return res


def title_check(runs, scan) -> dict:
    """T2 step 6 (I28), no network: for every fetched row of every run, the queue title (from the
    run's processed or normalized queue, else the stage report's own title, which may be cut short)
    against the `.ris` title of the file in the library; similarity below 0.6 is a WARN item."""
    recs = scan.get("_records") or {}
    out = {"checked": 0, "low": [], "not_in_library": [], "no_ris_title": []}
    seen = set()
    for run in runs:
        d = Path(run["dir"])
        titles = {}
        for name in list(run["files"].get("processed", [])) + [run["files"].get("normalized")]:
            if not name:
                continue
            _f, rows, _e = _read_csv(d / name)
            for r in rows:
                doi, t = _norm_doi(r.get("doi")), (r.get("title") or "").strip()
                if doi and t:
                    titles.setdefault(doi, t)
        for stage in _STAGES:
            name = run["files"].get(stage)
            if not name:
                continue
            _f, rows, _e = _read_csv(d / name)
            for r in rows:
                if stage_label(stage, r) != "OK" or not _truthy(r.get("downloaded")):
                    continue
                fn = (r.get(_STAGE_FILENAME[stage]) or "").strip()
                doi = _norm_doi(r.get("doi"))
                if not fn or (doi, fn.casefold()) in seen:
                    continue
                seen.add((doi, fn.casefold()))
                rec = recs.get(fn[:-4].casefold() if fn.lower().endswith(".pdf") else fn.casefold())
                if rec is None or not rec.get("pdf"):
                    out["not_in_library"].append(fn)
                    continue
                ris_title = ((rec.get("ris") or {}).get("ti") or "").strip()
                if not ris_title:
                    out["no_ris_title"].append(fn)
                    continue
                queue_title, cut = titles.get(doi), False
                if not queue_title:
                    queue_title, cut = (r.get("title") or "").strip(), True
                if not queue_title:
                    continue
                a, b = _text.normalise_title(queue_title), _text.normalise_title(ris_title)
                if cut and len(a) < len(b):
                    b = b[:len(a)]
                sim = difflib.SequenceMatcher(None, a, b).ratio()
                out["checked"] += 1
                if sim < TITLE_MIN_SIMILARITY:
                    out["low"].append({"file": fn, "doi": doi, "similarity": round(sim, 3),
                                       "queue_title": queue_title[:80], "ris_title": ris_title[:80],
                                       "run": run["run_id"]})
    return out


def audit_queue(proj, *, entry=None, name=None, projects=None, artifact_dir=None, lib=None,
                scan=None, extra_dirs=None, registry=None, claim_unnamed=False) -> dict:
    """Sweep history for one project: every run found in `report_dirs`, the latest run of each
    queue (tag) with its class counts and residual items, and the title check. `proj` is the
    project dir (used alone, it is the only directory searched besides `artifact_dir` and
    `extra_dirs`)."""
    if name is not None and entry is not None:
        dirs = report_dirs(name, entry, projects, artifact_dir, extra_dirs, registry, claim_unnamed)
    else:
        dirs = [(Path(proj), "own")]
        if artifact_dir:
            p = Path(artifact_dir).expanduser()
            dirs.append((p if p.is_absolute() else Path(proj) / p, "own"))
        for extra in extra_dirs or ():
            dirs += [(d, "claimed" if claim_unnamed else "by_destination") for d in _walk(Path(extra))]
    runs = find_runs(dirs)
    lib_key = os.path.normcase(os.path.abspath(lib)) if lib else None
    mine, unattributed = [], []
    summaries = {}
    for r in runs:
        if r["mode"] in ("by_destination", "claimed"):
            s = summarise_run(r)
            dest = s.get("destination")
            if dest and lib_key and os.path.normcase(os.path.abspath(dest)) == lib_key:
                mine.append(r)
                summaries[id(r)] = s
            elif not dest and r["mode"] == "claimed":
                mine.append(r)
                summaries[id(r)] = s
            elif not dest:
                unattributed.append({"dir": r["dir"], "run_id": r["run_id"], "tag": r["tag"]})
            continue
        mine.append(r)
    latest = {}
    for r in mine:
        latest[r["tag"]] = r            # runs are oldest first: the last one per tag wins
    latest_runs = []
    for tag in sorted(latest):
        r = latest[tag]
        latest_runs.append(summaries.get(id(r)) or summarise_run(r))
    out = {"dirs": [str(d) for d, _m in dirs], "runs": len(mine),
           "reports": sum(1 for r in mine if r["files"].get("report")),
           "run_list": [{"run_id": r["run_id"], "tag": r["tag"], "dir": r["dir"],
                         "stages": sorted(k for k in r["files"] if k != "processed")}
                        for r in mine],
           "latest": None, "latest_runs": latest_runs, "unattributed": unattributed,
           "failures": [], "titles": None}
    if latest_runs:
        newest = max(latest_runs, key=lambda s: _run_order({"run_id": s["run_id"], "mode": s["mode"]}))
        out["latest"] = newest["report"] or newest["residual"] or newest["run_id"]
    out["failures"] = [dict(i, run=s["run_id"], tag=s["tag"]) for s in latest_runs
                       for i in s["items"]
                       if i["class"] in ATTENTION_CLASSES or "UNKNOWN" in i["class"].split("/")]
    if scan is not None:
        out["titles"] = title_check(mine, scan)
    return out


# ---------------------------------------------------------------- index (read-only) and live runs
def default_db_path(cfg=None):
    return litconfig.db_dir(cfg) / DB_NAME


def _local_naive(v):
    """A DB timestamp (datetime, date, ISO string or epoch) as a local naive datetime, else None.
    The index writes `index_runs.finished_at` as an aware UTC instant (TIMESTAMPTZ), converted to
    local time here; the fallback `paper_locations.refreshed_at` is local naive time
    (datetime.now()) and is taken as it is."""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return _dt.datetime.fromtimestamp(v)
    if isinstance(v, str):
        try:
            v = _dt.datetime.fromisoformat(v.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
    if isinstance(v, _dt.datetime):
        return v.astimezone().replace(tzinfo=None) if v.tzinfo else v
    if isinstance(v, _dt.date):
        return _dt.datetime(v.year, v.month, v.day)
    return None


def index_status(db_path, projects) -> dict:
    """Freshness and DB-versus-disk reconcile per project, the DB opened read-only (never created).
    `projects` = [(key, scan)]. Freshness: the project's latest `index_runs` row (project,
    finished_at, n_files) when that table exists, else max(paper_locations.refreshed_at), against
    the newest library file and the file count."""
    db_path = Path(db_path)
    out = {"db": str(db_path), "exists": db_path.exists(), "error": None, "has_index_runs": False,
           "projects": {}}
    if not out["exists"]:
        return out
    try:
        import duckdb
        con = duckdb.connect(str(db_path), read_only=True)
    except Exception as e:      # a locked or foreign file: report it, never retry or create
        out["error"] = f"{type(e).__name__}: {str(e)[:160]}"
        return out
    try:
        tables = {r[0] for r in con.execute(
            "SELECT table_name FROM information_schema.tables").fetchall()}
        out["has_index_runs"] = "index_runs" in tables
        for key, scan in projects:
            out["projects"][key] = _index_project(con, tables, key, scan)
    except Exception as e:
        out["error"] = f"{type(e).__name__}: {str(e)[:160]}"
    finally:
        con.close()
    return out


def _index_project(con, tables, key, scan):
    stamp = n_index = source = None
    if "index_runs" in tables:
        row = con.execute("SELECT finished_at, n_files FROM index_runs WHERE project = ? "
                          "ORDER BY finished_at DESC LIMIT 1", [key]).fetchone()
        if row and row[0] is not None:
            stamp, n_index, source = row[0], row[1], "index_runs"
    loc = (con.execute("SELECT pdf_filename, has_pdf, refreshed_at FROM paper_locations "
                       "WHERE project = ?", [key]).fetchall() if "paper_locations" in tables else [])
    nodoi = ([r[0] for r in con.execute("SELECT pdf_filename FROM papers_no_doi WHERE project = ?",
                                        [key]).fetchall()] if "papers_no_doi" in tables else [])
    db_pdf_rows = [r for r in loc if r[1] is not False]
    db_text_rows = [r for r in loc if r[1] is False]
    n_disk_pdf = len(scan.get("pdf_names") or [])
    n_disk_text = len(scan.get("text_only") or [])
    if source is None:
        stamps = [r[2] for r in loc if r[2] is not None]
        stamp, source = (max(stamps) if stamps else None), "refreshed_at"
        n_index = len(db_pdf_rows) + len(nodoi)
        n_disk = n_disk_pdf
    else:
        n_disk = n_disk_pdf + n_disk_text      # forwarded contract: W3-C1's n_files counts both
    disk = set(scan.get("pdf_names") or [])
    db_names = {r[0] for r in db_pdf_rows if r[0]} | {n for n in nodoi if n}
    stamp_dt = _local_naive(stamp)
    newest = scan.get("newest_mtime")
    newest_dt = _dt.datetime.fromtimestamp(newest) if newest else None
    stale = None
    if stamp_dt is None:
        if disk or n_disk_text:
            stale = "never indexed (no stamp for this project)"
    elif newest_dt and newest_dt > stamp_dt + _dt.timedelta(seconds=INDEX_SLACK_S):
        stale = (f"newest library file {newest_dt:%Y-%m-%d %H:%M} is newer than the index stamp "
                 f"{stamp_dt:%Y-%m-%d %H:%M} ({source})")
    return {"source": source, "stamp": stamp_dt.isoformat(timespec="seconds") if stamp_dt else None,
            "newest_file": newest_dt.isoformat(timespec="seconds") if newest_dt else None,
            "stale": stale, "n_index": n_index, "n_disk": n_disk,
            "count_differs": n_index is not None and n_index != n_disk,
            "not_indexed": sorted(disk - db_names), "stale_rows": sorted(db_names - disk),
            "db_text_rows": len(db_text_rows), "disk_text_only": n_disk_text}


def live_runs_status() -> dict:
    """Live pipeline runs from the state file. litpipe.state.live_runs() only when the state file
    already exists (an instrument never creates the state dir or the DB); a run started more than
    6 h ago is stale (T2 step 5)."""
    out = {"state_file": None, "exists": False, "runs": [], "stale": [], "error": None}
    try:
        from litpipe import state
        p = state.db_path(create=False)
        out["state_file"] = str(p)
        out["exists"] = p.exists()
        if not out["exists"]:
            return out
        runs = state.live_runs()
    except Exception as e:
        out["error"] = f"{type(e).__name__}: {str(e)[:160]}"
        return out
    now = time.time()
    for r in runs:
        r = dict(r)
        try:
            age = now - _dt.datetime.fromisoformat(r["started"]).timestamp()
        except (KeyError, TypeError, ValueError):
            age = None
        r["age_s"] = None if age is None else round(age)
        out["runs"].append(r)
        if age is not None and age > STALE_RUN_S:
            out["stale"].append(r)
    return out


# ---------------------------------------------------------------- printing
def emit(lines, sev, label, items, full=False, fmt=str, count=None):
    """`  SEV label: N` then at most ITEM_CAP items (all with full)."""
    items = list(items)
    n = len(items) if count is None else count
    lines.append(f"  {sev} {label}: {n}")
    shown = items if full else items[:ITEM_CAP]
    for it in shown:
        lines.append(f"     {fmt(it)}")
    if len(items) > len(shown):
        lines.append(f"     ... +{len(items) - len(shown)} more (--full prints all)")


def _fmt_flag(f):
    return f"{f['file']} [{f['reason']}] {', '.join(f['sources'])}"


def fmt_report(audit: dict, queue: dict, index=None, full=False) -> str:
    lines = []
    name = audit["project"]
    tier = audit.get("tier", "?")
    lines.append(f"\n{'=' * 72}")
    lines.append(f"  {name}  [Tier {tier}]")
    lines.append(f"  lib: {audit['lib']}")
    lines.append('=' * 72)

    n_pdf, n_sc, n_ris = audit["n_pdfs"], audit["n_sidecars"], audit["n_ris"]
    pct_sc = (100 * n_sc / n_pdf) if n_pdf else 0
    pct_ris = (100 * n_ris / n_pdf) if n_pdf else 0
    lines.append(f"  PDFs:     {n_pdf}")
    lines.append(f"  sidecars: {n_sc} ({pct_sc:.0f}%)")
    lines.append(f"  .ris:     {n_ris} ({pct_ris:.0f}%)")
    lines.append(f"  holdings: {audit['pdf_holdings']} with PDF, {len(audit['text_only'])} text-only")

    if audit["fake_pdfs"]:
        emit(lines, "FAIL", "fake PDFs (no %PDF magic)", audit["fake_pdfs"], full)
    if audit["tiny_pdfs"]:
        emit(lines, "WARN", "tiny PDFs (<10KB)", audit["tiny_pdfs"], full,
             fmt=lambda t: f"{t[0]} ({t[1]}B)")
    if audit["bad_sidecars"]:
        emit(lines, "FAIL", "unparseable sidecars", audit["bad_sidecars"], full,
             fmt=lambda t: f"{t[0]}: {t[1]}")
    if audit["empty_sidecars"]:
        emit(lines, "WARN", "empty-content sidecars", audit["empty_sidecars"], full)
    if audit.get("needs_ocr"):
        lines.append(f"  INFO needs_ocr sidecars: {ocr_line(audit['needs_ocr'])}")
    if audit["orphan_sidecars"]:
        emit(lines, "WARN", "orphan sidecars (no PDF, not text-only)", audit["orphan_sidecars"], full)
    if audit["text_only"]:
        emit(lines, "INFO", f"TEXT_ONLY holdings (text, no PDF; {audit['text_only_with_ris']} with .ris)",
             audit["text_only"], full)
    if audit["flags"]:
        emit(lines, "WARN", "identity flags (review; not holdings)", audit["flags"], full,
             fmt=_fmt_flag)
    if audit["bad_identity"]:
        emit(lines, "WARN", "unparseable .identity.json", audit["bad_identity"], full,
             fmt=lambda t: f"{t[0]}: {t[1]}")
    if audit["orphan_identity"]:
        emit(lines, "WARN", "orphan .identity.json (no PDF)", audit["orphan_identity"], full)
    if audit["pdfs_no_sidecar"]:
        emit(lines, "INFO", "PDFs without sidecar", audit["pdfs_no_sidecar"], full)
    if audit["pdfs_no_ris"]:
        emit(lines, "INFO", "PDFs without .ris", audit["pdfs_no_ris"], full)
    if audit["orphan_ris"]:
        emit(lines, "WARN", "orphan .ris (no PDF, no text-only sidecar)", audit["orphan_ris"], full)
    for kind_of_file in ("sidecar", "ris"):
        for k in DAMAGE_KINDS:
            names = audit["damage"][kind_of_file][k]
            if names:
                emit(lines, "WARN", f"{kind_of_file} text damage: {k}", names, full)
    mm = audit["mismatch"]
    if mm["files"]:
        emit(lines, "WARN", f"_mismatch/ quarantine ({mm['pdfs']} PDFs)", mm["files"], full)

    if audit["al_antipattern"]:
        emit(lines, "WARN", "'_al_' antipattern (audit_filenames.py can fix)", audit["al_antipattern"], full)
    if audit["unknown_year"]:
        emit(lines, "WARN", "Unknown_ prefix", audit["unknown_year"], full)
    if audit["upper_lastname_sample"]:
        emit(lines, "INFO", "ALLCAPS lastnames (CrossRef quirk), sample", audit["upper_lastname_sample"], full)

    if index is not None:
        if index.get("stale"):
            lines.append(f"  WARN index stale: {index['stale']}")
        if index.get("count_differs"):
            lines.append(f"  WARN index count: {index['n_index']} in the index ({index['source']}) "
                         f"vs {index['n_disk']} on disk")
        if index.get("not_indexed"):
            emit(lines, "WARN", "PDFs on disk not in the index", index["not_indexed"], full)
        if index.get("stale_rows"):
            emit(lines, "WARN", "index rows with no file on disk", index["stale_rows"], full)
        if not (index.get("stale") or index.get("count_differs") or index.get("not_indexed")
                or index.get("stale_rows")):
            lines.append(f"  index: current ({index['n_index']} rows, stamp {index['stamp']})")

    if queue["runs"]:
        lines.append(f"  queue history: {queue['runs']} run(s), {queue['reports']} report(s) in "
                     f"{len(queue['dirs'])} searched dir(s); latest={queue['latest']}")
        for s in queue["latest_runs"]:
            label = f"queue {s['tag']!r}" if s["tag"] else "queue"
            src = s["class_source"] or "none"
            counts = ", ".join(f"{c} {n}" for c, n in sorted(s["classes"].items(), key=lambda t: -t[1])
                               if n) or "none"
            lines.append(f"    {label} run {s['run_id']} [{s['dir']}]: classes ({src}): {counts}")
            if s["downloaded"] is not None:
                lines.append(f"      downloaded (report total): {s['downloaded']}")
            if s["disagreement"]:
                lines.append(f"      WARN report and residual disagree: {s['disagreement']}")
            if s["unknown"]:
                sev = "WARN" if s["residual_has_class"] else "INFO"
                why = ("blank residual_class" if s["residual_has_class"]
                       else "legacy residual or stage reports with no class; never counted as success")
                lines.append(f"      {sev} unclassified rows (UNKNOWN, {why}): {s['unknown']}")
            if s["items"]:
                emit(lines, "INFO", f"residual items, run {s['run_id']}", s["items"], full,
                     fmt=lambda i: f"{i['class']:<16} {i['doi']:<40} {i['title']} {i['detail']}".rstrip())
            for e in s["errors"]:
                lines.append(f"      WARN unreadable: {e}")
        t = queue.get("titles")
        if t:
            lines.append(f"  fetched-title check: {t['checked']} checked against .ris; "
                         f"{len(t['not_in_library'])} file(s) no longer in the library; "
                         f"{len(t['no_ris_title'])} without a .ris title")
            if t["low"]:
                emit(lines, "WARN", f"fetched titles unlike their .ris (similarity < {TITLE_MIN_SIMILARITY})",
                     t["low"], full,
                     fmt=lambda i: f"{i['similarity']:.2f} {i['file']}: queue={i['queue_title']!r} ris={i['ris_title']!r}")
    else:
        lines.append(f"  queue history: none in {len(queue['dirs'])} searched dir(s)")
    return "\n".join(lines)


def _strip_private(obj):
    if isinstance(obj, dict):
        return {k: _strip_private(v) for k, v in obj.items() if not str(k).startswith("_")}
    if isinstance(obj, list):
        return [_strip_private(v) for v in obj]
    return obj


def portfolio_holdings(cfg, names):
    """DOI-level holdings for the audited projects through litpipe.holdings (no cache read or
    written): DOIs with content, text-only DOIs, DOIs in more than one library."""
    from litpipe import holdings
    reg = {"projects": {k: v for k, v in (cfg.get("projects") or {}).items() if k in names}}
    for k in ("root", "state_dir", "db_dir"):
        if k in cfg:
            reg[k] = cfg[k]
    hm = holdings.build(reg, use_cache=False, write_cache=False)
    st = hm.stats
    return {"dois": st.get("dois"), "dois_text_only": st.get("dois_text_only"),
            "dois_multi_library": st.get("dois_multi_library"), "files_scanned": st.get("files_scanned"),
            "unreadable": len(st.get("unreadable") or []), "elapsed_s": st.get("elapsed_s")}


# ---------------------------------------------------------------- run / main
def run(project=None, json_path=None, full=False, artifact_dir=None, db=None, holdings=True,
        report_dirs=None, **_ignored) -> dict:
    """Audit the portfolio (or one project), print the report, write `json_path` if given.
    Returns the result dict; result["exit_code"] is 1 on any FAIL, 2 when `project` matches no
    active registered project, else 0."""
    cfg = load_registry()
    projects = cfg.get("projects", {}) or {}
    report_dirs = [os.path.abspath(os.path.expanduser(str(d))) for d in (report_dirs or ())]
    libs, missing = discover(projects, project)
    if project and not libs and not missing:
        print(f"[ERR] no active registered project matches {project!r}", file=sys.stderr)
        return {"exit_code": 2, "projects": [], "summary": {}}

    print(f"Auditing {len(libs)} libraries (config: {CONFIG_PATH.name})\n")
    for m in missing:
        print(f"[FAIL] {m['project']}: {m['reason']} ({m['lib']})")
    entries = []
    portfolio_doi = {}  # doi -> [(project, filename)]
    for name, p, lib, tier, _data in libs:
        a = audit_lib(name, lib)
        a["tier"] = tier
        a["deep"] = deep_audit_lib(lib, scan=a)
        for doi, files in a["deep"]["doi_to_files"].items():
            for fn in files:
                portfolio_doi.setdefault(doi, []).append((name, fn))
        q = audit_queue(lit_util.project_root(name, p), entry=p, name=name, projects=projects,
                        artifact_dir=artifact_dir, lib=lib, scan=a, extra_dirs=report_dirs,
                        registry=cfg, claim_unnamed=bool(project) and len(libs) == 1)
        entries.append({"audit": a, "queue": q, "index": None})

    try:
        db_path = Path(db) if db else default_db_path(cfg)
    except litconfig.ConfigError as e:      # a bad db_dir in projects.json: report, do not guess
        idx = {"db": None, "exists": False, "error": str(e), "has_index_runs": False, "projects": {}}
    else:
        idx = index_status(db_path, [(e["audit"]["project"], e["audit"]) for e in entries])
    for e in entries:
        e["index"] = idx["projects"].get(e["audit"]["project"])
        print(fmt_report(e["audit"], e["queue"], e["index"], full))

    live = live_runs_status()
    hold = None
    if holdings and libs:
        try:
            hold = portfolio_holdings(cfg, {name for name, *_ in libs})
        except Exception as e:      # the summary must still print
            hold = {"error": f"{type(e).__name__}: {str(e)[:160]}"}
    summary = _print_summary(entries, missing, portfolio_doi, idx, live, hold, full)
    exit_code = 1 if summary["fail"] else 0
    result = {"exit_code": exit_code, "config": str(CONFIG_PATH),
              "projects": [_strip_private(e) for e in entries], "missing": missing,
              "index": {k: v for k, v in idx.items() if k != "projects"}, "live_runs": live,
              "holdings": hold, "summary": summary}
    if json_path:
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, default=str, ensure_ascii=False)
        print(f"\n  JSON written: {json_path}")
    print(f"\n  exit {exit_code} ({'FAIL' if exit_code else 'no FAIL'})")
    return result


def _print_summary(entries, missing, portfolio_doi, idx, live, hold, full):
    lines = [f"\n{'=' * 72}\n  PORTFOLIO SUMMARY\n{'=' * 72}"]
    audits = [e["audit"] for e in entries]
    total_pdfs = sum(a["n_pdfs"] for a in audits)
    total_sc = sum(a["n_sidecars"] for a in audits)
    total_ris = sum(a["n_ris"] for a in audits)
    lines.append(f"  total libraries: {len(audits)}")
    lines.append(f"  total PDFs:      {total_pdfs}")
    lines.append(f"  total sidecars:  {total_sc} ({100 * total_sc / max(1, total_pdfs):.0f}%)")
    lines.append(f"  total .ris:      {total_ris} ({100 * total_ris / max(1, total_pdfs):.0f}%)")
    n_hold_pdf = sum(a["pdf_holdings"] for a in audits)
    n_text = sum(len(a["text_only"]) for a in audits)
    lines.append(f"  holdings (files): {n_hold_pdf} with PDF, {n_text} text-only (INFO)")
    ocr = [f"[{a['project']}] {n}" for a in audits for n in a.get("needs_ocr", [])]
    if ocr:
        lines.append(f"  INFO needs_ocr sidecars: {ocr_line(ocr)}")
    if hold:
        if hold.get("error"):
            lines.append(f"  WARN holdings map failed: {hold['error']}")
        else:
            lines.append(f"  holdings (DOIs, litpipe.holdings): {hold['dois']} with content, "
                         f"{hold['dois_text_only']} text-only, {hold['dois_multi_library']} in 2+ libraries")
    fail_names = [m["project"] for m in missing]
    emit(lines, "FAIL" if missing else "INFO", "registered but missing", missing, full,
         fmt=lambda m: f"{m['project']}: {m['reason']} ({m['lib']})")
    flags = []
    for a in audits:
        if a["fake_pdfs"] or a["bad_sidecars"]:
            flags.append(f"BREAK: {a['project']}")
            fail_names.append(a["project"])
        elif (a["empty_sidecars"] or a["orphan_sidecars"] or a["tiny_pdfs"]
              or a["al_antipattern"] or a["unknown_year"]):
            flags.append(f"WARN:  {a['project']}")
    if flags:
        lines.append("  flagged projects:")
        lines += [f"    {f}" for f in flags]
    else:
        lines.append("  no projects flagged")
    all_flags = [(a["project"], f) for a in audits for f in a["flags"]]
    emit(lines, "WARN" if all_flags else "INFO", "identity flags (review items)", all_flags, full,
         fmt=lambda t: f"[{t[0]}] {_fmt_flag(t[1])}")
    for kind_of_file in ("sidecar", "ris"):
        for k in DAMAGE_KINDS:
            hits = [(a["project"], n) for a in audits for n in a["damage"][kind_of_file][k]]
            emit(lines, "WARN" if hits else "INFO", f"{kind_of_file} files with {k} damage", hits,
                 full, fmt=lambda t: f"[{t[0]}] {t[1]}")
    mm = [(a["project"], f) for a in audits for f in a["mismatch"]["files"]]
    emit(lines, "WARN" if mm else "INFO",
         f"_mismatch/ quarantined files ({sum(a['mismatch']['pdfs'] for a in audits)} PDFs; "
         f"restore with backfills/mismatch_restore.py)", mm, full, fmt=lambda t: f"[{t[0]}] {t[1]}")
    unattr = [u for e in entries for u in e["queue"]["unattributed"]]
    uniq_unattr = list({(u["dir"], u["tag"], u["run_id"]): u for u in unattr}.values())
    if uniq_unattr:
        emit(lines, "INFO", "legacy runs in a --report-dir folder that name no library, not attributed "
             "(pass --project with --report-dir to count them for one project)", uniq_unattr, full,
             fmt=lambda u: f"{u['run_id']} {u['tag']} {u['dir']}")

    if idx["error"]:
        lines.append(f"  WARN index DB could not be read (read-only): {idx['error']}")
    elif not idx["exists"]:
        lines.append(f"  INFO index DB not found: {idx['db']} (freshness not checked)")
    else:
        stale = [k for k, v in idx["projects"].items() if v and (v["stale"] or v["count_differs"])]
        src = "index_runs" if idx["has_index_runs"] else "max(refreshed_at)"
        emit(lines, "WARN" if stale else "INFO", f"stale or miscounted index entries ({src}; {idx['db']})",
             stale, full)
        recon = [(k, len(v["not_indexed"]), len(v["stale_rows"])) for k, v in idx["projects"].items()
                 if v and (v["not_indexed"] or v["stale_rows"])]
        emit(lines, "WARN" if recon else "INFO", "DB-versus-disk reconcile (project: not indexed, "
             "rows with no file)", recon, full, fmt=lambda t: f"{t[0]}: {t[1]}, {t[2]}")
    if live["error"]:
        lines.append(f"  WARN live runs: state file unreadable: {live['error']}")
    elif not live["exists"]:
        lines.append(f"  INFO live runs: no state file ({live['state_file']})")
    else:
        emit(lines, "INFO", "live runs (state file)", live["runs"], full,
             fmt=lambda r: f"{r['run_id']} {r['kind']} pid {r['pid']} age {r.get('age_s')}s")
        if live["stale"]:
            emit(lines, "WARN", "live runs older than 6 h", live["stale"], full,
                 fmt=lambda r: f"{r['run_id']} {r['kind']} pid {r['pid']} started {r['started']}")

    # Deep findings rollup
    lines.append("\n=== DEEP CHECKS ===")
    overlaps = {d: locs for d, locs in portfolio_doi.items() if len({p for p, _ in locs}) > 1}
    emit(lines, "INFO", "cross-project DOI overlap (DOIs in >1 project)", sorted(overlaps.items()),
         full, fmt=lambda t: f"{t[0]}: " + "; ".join(f"[{p}] {fn}" for p, fn in t[1]))
    same_proj_dups = {}
    for d, locs in portfolio_doi.items():
        by_proj = {}
        for p, fn in locs:
            by_proj.setdefault(p, []).append(fn)
        for p, fns in by_proj.items():
            if len(fns) > 1:
                same_proj_dups[(d, p)] = fns
    emit(lines, "WARN" if same_proj_dups else "INFO", "same-project DOI dupes",
         list(same_proj_dups.items()), full, fmt=lambda t: f"{t[0][0]} in [{t[0][1]}]: {t[1]}")
    all_lens = [(a["project"], n, ln) for a in audits for n, ln in a["deep"]["sidecar_lens"]]
    if all_lens:
        srt = sorted(all_lens, key=lambda x: x[2])
        lines.append(f"\n  sidecar text-length distribution (n={len(all_lens)}):")
        lines.append(f"     min={srt[0][2]} median={srt[len(srt) // 2][2]} max={srt[-1][2]}")
        very_short = [t for t in all_lens if t[2] < 1000]
        emit(lines, "INFO", "<1000 chars (likely empty/stub)", very_short, full,
             fmt=lambda t: f"[{t[0]}] {t[1]}: {t[2]} chars")
    misalign = [(a["project"], *m) for a in audits for m in a["deep"]["fn_misalign"]]
    emit(lines, "WARN" if misalign else "INFO", "filename vs metadata mismatches", misalign, full,
         fmt=lambda t: f"[{t[0]}] [{t[2]}] {t[1]}: {t[3]}")
    doi_mis = [(a["project"], *m) for a in audits for m in a["deep"]["doi_mismatch"]]
    emit(lines, "WARN" if doi_mis else "INFO", "intra-file DOI mismatches (sidecar vs ris)", doi_mis,
         full, fmt=lambda t: f"[{t[0]}] {t[1]}: {t[2]}")
    malformed = [(a["project"], *m) for a in audits for m in a["deep"].get("malformed_dois", [])]
    emit(lines, "WARN" if malformed else "INFO", "malformed/suspicious DOIs in library (sidecar/ris)",
         malformed, full, fmt=lambda t: f"[{t[0]}] {t[1]} [{t[2]}]: {t[3]}")
    print("\n".join(lines))
    return {"fail": sorted(set(fail_names)), "missing": [m["project"] for m in missing],
            "break": [f[7:] for f in flags if f.startswith("BREAK")],
            "holdings_files": {"pdf": n_hold_pdf, "text_only": n_text}, "needs_ocr": len(ocr),
            "identity_flags": len(all_flags), "mismatch_files": len(mm),
            "damage": {kf: {k: sum(len(a["damage"][kf][k]) for a in audits) for k in DAMAGE_KINDS}
                       for kf in ("sidecar", "ris")},
            "fn_misalign": len(misalign), "cross_project_overlap": len(overlaps),
            "same_project_dupes": len(same_proj_dups), "unattributed_runs": len(uniq_unattr)}


def build_parser():
    ap = argparse.ArgumentParser(description="Read-only portfolio audit of literature-pipeline "
                                             "libraries, queues, index and live runs.")
    ap.add_argument("--project", default=None,
                    help="Audit only this project (name, or a subproject's tail).")
    ap.add_argument("--json", dest="json_path", default=None,
                    help="Write machine-readable output to this path.")
    ap.add_argument("--full", action="store_true",
                    help="Print every item of every check (default: count plus the first 20).")
    ap.add_argument("--artifact-dir", default=None,
                    help="Also read sweep reports from this directory (relative to each project "
                         "dir, or absolute), as sweep's --artifact-dir writes them. The projects.json "
                         "artifact_dir is read without it.")
    ap.add_argument("--report-dir", dest="report_dirs", action="append", default=None, metavar="DIR",
                    help="Also read archived sweep reports under DIR (relative to the current folder) "
                         "and its subfolders (repeatable). "
                         "A run there counts for a project when its report names that project's "
                         "library; with --project, a run that names no library counts too.")
    ap.add_argument("--db", default=None,
                    help="Index DB to check, opened read-only (default: <db_dir>/portfolio.duckdb).")
    ap.add_argument("--no-holdings", dest="holdings", action="store_false",
                    help="Skip the DOI-level holdings summary (litpipe.holdings, no cache).")
    return ap


def main(argv=None):
    args = build_parser().parse_args(argv)
    return run(**vars(args))["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
