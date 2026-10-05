"""Holdings map: which DOIs are already on disk anywhere in the portfolio (dispatch 0.5, W1-D2).

`build(registry=None) -> HoldMap` reads the `doi` field of every `<base>.fulltext.json` and the DOI
of every `<base>.ris` in the top level of every registered library (a project with a `lib_dir`),
and `HoldMap.where(doi) -> list[Path]` says where a DOI is held. The top level only, like every
other library reader here (index, audit, pipeline_check): `_mismatch/` holds quarantined wrong-DOI
files and must never count as a holding.

A holding is one `<base>` in one library, keyed by every DOI its `.ris` and sidecar name:
  pdf         `<base>.pdf` exists (the path is the PDF)
  text_only   no PDF, and the sidecar parses and carries non-empty `text` (the path is the sidecar;
              DEC-08: a JATS / BioC / author-manuscript record is a first-class holding)
  empty_sidecar  no PDF, the sidecar names a DOI but has no text (not content; kept for audits)
  ris_only    a `.ris` with neither a PDF nor a text-bearing sidecar (not content)
`where()` returns content holdings only (PDFs first); `records()` returns all four kinds.

Cache. The libraries sit on a Google Drive mirror and one library holds 3,300 sidecars, so the
parsed DOI of every `.ris` and sidecar is cached under `state_dir` keyed by (size, mtime_ns). A warm
build lists each library (on Windows `os.scandir` returns size and mtime with the listing, no extra
call per file) and reads only the files that changed. PDF presence is never cached: it is re-listed
every build, so a PDF that arrives turns a text-only holding into a PDF holding at once. Unreadable
files are counted and retried next build, never cached. A corrupt or foreign cache file is ignored
(a cold build), and a failure to write it is recorded in `stats`, never raised.

DOIs are compared case-insensitively from any form (bare, backtick, `doi:` prefix, doi.org link,
percent-encoded link): `normalise_doi` here is an interim local normaliser; switch it to
`litpipe.doi.normalise` once W1-B lands (forwarded).
"""
import json
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote

import lit_util
from litpipe import config
from litpipe import doi as _doi

CACHE_VERSION = 4   # 2: DOIs normalised by litpipe.doi; 3: identity-FLAG sidecars give no DOI;
                    # 4: a PDF-extracted sidecar whose PDF is gone is an orphan, not text-only
CACHE_NAME = "holdings_cache.json"
SIDECAR_SUFFIX = ".fulltext.json"

PDF = "pdf"
TEXT_ONLY = "text_only"
EMPTY_SIDECAR = "empty_sidecar"
ORPHAN_SIDECAR = "orphan_sidecar"
RIS_ONLY = "ris_only"
CONTENT_KINDS = frozenset({PDF, TEXT_ONLY})

# ---------------------------------------------------------------- DOI normalisation (interim)
_RESOLVER = re.compile(r"^(?:https?://)?(?:www\.)?(?:dx\.)?doi\.org/", re.IGNORECASE)
_DOI_PREFIX = re.compile(r"^doi:\s*", re.IGNORECASE)
_TRAIL = ".,;:]}>'\"`*_"
# A DOI inside free text (a queue .md line, an error string): Crossref's recommended suffix class
# plus '<' and '>' (SICI DOIs carry them literally). It stops at a URL query or fragment, so the
# Unpaywall error text `.../v2/10.x/y?email=...` yields `10.x/y`, never the email.
_DOI_IN_TEXT = re.compile(r"10\.\d{4,9}/[-._;()/:A-Za-z0-9<>]+", re.IGNORECASE)
_PCT_SLASH = re.compile(r"%2[Ff]")


def normalise_doi(raw) -> str:
    """One comparable form of a DOI given in any form: bare, backtick-wrapped, `doi:` prefixed,
    a doi.org / dx.doi.org link, percent-encoded. Lower-case, no trailing punctuation (a closing
    parenthesis only when unbalanced, so `10.1016/0021-9681(72)90027-6` survives and a markdown
    link's `)` does not). Returns "" for anything that is not a DOI."""
    if not raw:
        return ""
    return _doi.normalise(str(raw)) or ""


def doi_key(raw) -> str:
    """The comparison key: the normalised DOI, else the stripped lower-cased string (so a malformed
    DOI still matches itself case-insensitively and is never silently dropped)."""
    return normalise_doi(raw) or str(raw or "").strip().lower()


def extract_dois(text) -> list:
    """Every DOI in free text, normalised, first-seen order, no repeats."""
    out, seen = [], set()
    for pos, d in _doi.iter_candidates(_PCT_SLASH.sub("/", text or "")):
        if pos in seen:          # the most specific form of each occurrence only
            continue
        seen.add(pos)
        if d not in out:
            out.append(d)
    return out


# ---------------------------------------------------------------- file readers
_RIS_DO = re.compile(r"(?m)^DO\s+-\s?(.+?)\s*$")
_RIS_UR = re.compile(r"(?m)^(?:UR|L2|LK)\s+-\s?(.+?)\s*$")


def read_ris_doi(path) -> str:
    """The normalised DOI of a .ris file: its DO tag, else a doi.org UR link. Raises OSError."""
    with open(path, encoding="utf-8", errors="replace") as f:
        text = f.read()
    m = _RIS_DO.search(text)
    if m:
        d = normalise_doi(m.group(1))
        if d:
            return d
    for u in _RIS_UR.findall(text):
        ds = extract_dois(u)
        if ds:
            return ds[0]
    return ""


def read_sidecar(path):
    """(doi, text_chars, has_pmcid, pdf_derived) of a .fulltext.json sidecar. Raises OSError, or ValueError when
    the file is not a JSON object. A sidecar whose identity verdict is FLAG gives no DOI: the PMC stage
    records the queue DOI there for a PDF judged to be another work, which is a review item, not a
    holding (W2a verifier A, V-A2). `pdf_derived` is true for text extracted from a PDF (`has_pdf` true,
    or no `has_pdf` key and `extracted_from_pdf` true): with that PDF gone the sidecar is an orphan, not
    a text-only holding (W2b verifier F; the instruments' predicate)."""
    with open(path, "rb") as f:
        data = json.loads(f.read().decode("utf-8", errors="replace"))
    if not isinstance(data, dict):
        raise ValueError("sidecar is not a JSON object")
    text = data.get("text")
    n = len(text.strip()) if isinstance(text, str) else 0
    doi = "" if data.get("identity") == "FLAG" else normalise_doi(data.get("doi") or "")
    pdf_derived = data.get("has_pdf") is True or ("has_pdf" not in data and data.get("extracted_from_pdf") is True)
    return doi, n, bool(data.get("pmcid")), pdf_derived


# ---------------------------------------------------------------- the map
@dataclass(frozen=True)
class Holding:
    doi: str
    path: Path       # the PDF when there is one, else the sidecar, else the .ris
    project: str     # the registry key whose library holds it
    library: Path
    kind: str        # PDF, TEXT_ONLY, EMPTY_SIDECAR, ORPHAN_SIDECAR or RIS_ONLY

    @property
    def has_pdf(self) -> bool:
        return self.kind == PDF

    @property
    def is_content(self) -> bool:
        return self.kind in CONTENT_KINDS


_KIND_ORDER = {PDF: 0, TEXT_ONLY: 1, EMPTY_SIDECAR: 2, ORPHAN_SIDECAR: 3, RIS_ONLY: 4}


class HoldMap:
    """DOI -> holdings. Build it with `build()`; tests and fakes may construct it from records."""

    def __init__(self, records=(), stats=None):
        self._by_doi = {}
        for h in records:
            self._by_doi.setdefault(doi_key(h.doi), []).append(h)
        for recs in self._by_doi.values():
            recs.sort(key=lambda h: (_KIND_ORDER.get(h.kind, 9), str(h.path).lower()))
        self.stats = dict(stats or {})

    def records(self, doi) -> list:
        """Every holding for the DOI, content or not, PDFs first."""
        return list(self._by_doi.get(doi_key(doi), ()))

    def content(self, doi) -> list:
        return [h for h in self.records(doi) if h.is_content]

    def where(self, doi) -> list:
        """Paths where the DOI is held with content (a PDF, or a text-only sidecar), PDFs first."""
        return [h.path for h in self.content(doi)]

    def has_pdf(self, doi) -> bool:
        return any(h.has_pdf for h in self.records(doi))

    def text_only(self, doi) -> bool:
        """Held with text somewhere and with a PDF nowhere."""
        c = self.content(doi)
        return bool(c) and not any(h.has_pdf for h in c)

    def dois(self) -> set:
        return {d for d, recs in self._by_doi.items() if any(h.is_content for h in recs)}

    def __contains__(self, doi) -> bool:
        return bool(self.content(doi))

    def __len__(self) -> int:
        return len(self.dois())


# ---------------------------------------------------------------- registry and cache
def _config(registry):
    """A full projects.json dict from `registry`: None reads litpipe.config.CONFIG_PATH; a dict with
    a `projects` key is taken as the whole file; any other dict is taken as the projects mapping."""
    if registry is None:
        return config.load(), True
    if isinstance(registry, dict) and isinstance(registry.get("projects"), dict):
        return registry, True
    return {"projects": dict(registry or {})}, False


def libraries(registry=None) -> list:
    """[(project_key, library_path, active)] for every registered project with a lib_dir, one entry
    per distinct directory, in registry order. Resolved against lit_util.PROJECTS_ROOT at call time
    (tests monkeypatch that attribute)."""
    cfg, _ = _config(registry)
    out, seen = [], set()
    for key, p in (cfg.get("projects") or {}).items():
        if not isinstance(p, dict) or not p.get("lib_dir"):
            continue
        lib = lit_util.PROJECTS_ROOT / lit_util.lib_rel(key, p)
        norm = os.path.normcase(os.path.abspath(lib))
        if norm in seen:
            continue
        seen.add(norm)
        out.append((key, lib, p.get("active", True) is not False))
    return out


def cache_path(registry=None, cache_dir=None, create=True) -> Path:
    if cache_dir is not None:
        return Path(cache_dir) / CACHE_NAME
    cfg, full = _config(registry)
    return config.state_dir(cfg if full else None, create=create) / CACHE_NAME


def _load_cache(path):
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError):
        return {}
    if not isinstance(data, dict) or data.get("version") != CACHE_VERSION:
        return {}
    libs = data.get("libraries")
    return libs if isinstance(libs, dict) else {}


def _save_cache(path, libs):
    path.parent.mkdir(parents=True, exist_ok=True)
    lit_util.atomic_write_text(str(path), json.dumps(
        {"version": CACHE_VERSION, "libraries": libs}, separators=(",", ":")))


def _entry_stat(entry):
    st = entry.stat()
    return [st.st_size, st.st_mtime_ns]


# ---------------------------------------------------------------- build
def build(registry=None, *, cache_dir=None, use_cache=True, write_cache=True) -> HoldMap:
    """Scan every registered library and return the HoldMap. `registry` is a loaded projects.json
    (or just its `projects` mapping); None reads litpipe.config.CONFIG_PATH. The cache lives in
    `cache_dir`, default `state_dir`; with no library to scan nothing is read or written there."""
    t0 = time.perf_counter()
    libs = libraries(registry)
    stats = {
        "libraries": [], "files_scanned": 0, "files_read": 0, "cache_hits": 0, "unreadable": [],
        "orphan_ris": 0, "empty_sidecars": 0, "ris_sidecar_doi_disagreements": [],
        "cache_path": None, "cache_used": False, "cache_written": False, "cache_error": None,
    }
    cpath = cache_path(registry, cache_dir, create=write_cache) if (use_cache and libs) else None
    old = _load_cache(cpath) if cpath else {}
    stats["cache_path"] = str(cpath) if cpath else None
    stats["cache_used"] = bool(old)
    new_cache, changed = {}, False
    records = []

    for key, lib, active in libs:
        lkey = os.path.normcase(os.path.abspath(lib))
        lstat = {"project": key, "path": str(lib), "active": active, "exists": False,
                 "pdf": 0, "ris": 0, "sidecar": 0}
        stats["libraries"].append(lstat)
        try:
            entries = list(os.scandir(lib))
        except FileNotFoundError:
            continue
        except OSError as e:
            stats["unreadable"].append({"path": str(lib), "error": f"{type(e).__name__}: {e}"})
            continue
        lstat["exists"] = True
        cached_files = (old.get(lkey) or {}).get("files") or {}
        files_out = {}
        pdfs, ris, sidecars = {}, {}, {}
        for e in entries:
            try:
                if not e.is_file():
                    continue
            except OSError:
                continue
            low = e.name.lower()
            if low.endswith(SIDECAR_SUFFIX):
                sidecars[e.name[:-len(SIDECAR_SUFFIX)]] = e
            elif low.endswith(".ris"):
                ris[e.name[:-4]] = e
            elif low.endswith(".pdf"):
                pdfs[e.name[:-4].casefold()] = e
        lstat.update(pdf=len(pdfs), ris=len(ris), sidecar=len(sidecars))
        stats["files_scanned"] += len(pdfs) + len(ris) + len(sidecars)

        def parsed(entry, reader):
            """The cached parse when (size, mtime) match, else a fresh read; None if unreadable."""
            nonlocal changed
            try:
                sig = _entry_stat(entry)
            except OSError as exc:
                stats["unreadable"].append({"path": entry.path, "error": f"{type(exc).__name__}: {exc}"})
                return None
            hit = cached_files.get(entry.name)
            if isinstance(hit, list) and hit[:2] == sig:
                stats["cache_hits"] += 1
                files_out[entry.name] = hit
                return hit[2:]
            try:
                val = reader(entry.path)
            except (OSError, ValueError) as exc:
                stats["unreadable"].append({"path": entry.path, "error": f"{type(exc).__name__}: {exc}"})
                return None
            stats["files_read"] += 1
            changed = True
            row = sig + (list(val) if isinstance(val, tuple) else [val])
            files_out[entry.name] = row
            return row[2:]

        for base in sorted(set(ris) | set(sidecars)):
            ris_doi = ""
            if base in ris:
                got = parsed(ris[base], read_ris_doi)
                ris_doi = got[0] if got else ""
            sc_doi, sc_chars, has_sc, pdf_derived = "", 0, False, False
            if base in sidecars:
                got = parsed(sidecars[base], read_sidecar)
                if got:
                    sc_doi, sc_chars, has_sc = got[0], int(got[1] or 0), True
                    pdf_derived = bool(got[3]) if len(got) > 3 else False
            dois = [d for d in dict.fromkeys((ris_doi, sc_doi)) if d]
            if not dois:
                continue
            if ris_doi and sc_doi and ris_doi != sc_doi:
                stats["ris_sidecar_doi_disagreements"].append(
                    {"library": str(lib), "base": base, "ris": ris_doi, "sidecar": sc_doi})
            pdf_entry = pdfs.get(base.casefold())
            if pdf_entry is not None:
                kind, path = PDF, Path(pdf_entry.path)
            elif has_sc and sc_chars > 0 and pdf_derived:
                kind, path = ORPHAN_SIDECAR, Path(sidecars[base].path)   # its PDF was renamed or deleted
                stats["orphan_sidecars"] = stats.get("orphan_sidecars", 0) + 1
            elif has_sc and sc_chars > 0:
                kind, path = TEXT_ONLY, Path(sidecars[base].path)
            elif has_sc:
                kind, path = EMPTY_SIDECAR, Path(sidecars[base].path)
                stats["empty_sidecars"] += 1
            else:
                kind, path = RIS_ONLY, Path(ris[base].path)
                stats["orphan_ris"] += 1
            for d in dois:
                records.append(Holding(d, path, key, lib, kind))
        if set(cached_files) != set(files_out):
            changed = True
        new_cache[lkey] = {"files": files_out}

    if cpath is not None and write_cache and (changed or set(old) != set(new_cache)):
        try:
            _save_cache(cpath, new_cache)
            stats["cache_written"] = True
        except OSError as e:
            stats["cache_error"] = f"{type(e).__name__}: {e}"

    hm = HoldMap(records, stats)
    libs_of = {}
    for h in records:
        if h.is_content:
            libs_of.setdefault(h.doi, set()).add(os.path.normcase(str(h.library)))
    stats["dois"] = len(hm)
    stats["dois_text_only"] = sum(1 for d in hm.dois() if hm.text_only(d))
    stats["dois_multi_library"] = sum(1 for s in libs_of.values() if len(s) > 1)
    stats["elapsed_s"] = round(time.perf_counter() - t0, 3)
    hm.stats = stats
    return hm
