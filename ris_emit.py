"""Shared RIS emission + CrossRef helpers.

Used by:
  - harvest_citations.py  (Downloads → _references/citations/*.ris)
  - backfill_ris.py        (existing PDF library → sidecar .ris)
  - unpaywall_fetch_v2.py  (write .ris alongside each newly-fetched PDF)
  - pmc_fetch.py           (same)

RIS format reference: https://en.wikipedia.org/wiki/RIS_(file_format)
EndNote ingests RIS natively via "Reference Manager (RIS)" import filter.
"""
import os, re, sys, difflib
import lit_net  # B1: shared GET with 429/5xx retry

# Config loader + safe_ascii live in lit_util (stdlib-pure); re-exported here so the many
# `from ris_emit import load_projects_config` / `from ris_emit import safe_ascii` call sites
# (audit_portfolio, preprint_fetch, unpaywall_fetch_v2, test_filenames) keep working unchanged.
from lit_util import load_projects_config, safe_ascii, DEFAULT_EMAIL

EMAIL = os.environ.get("LITPIPE_EMAIL", DEFAULT_EMAIL)
UA    = f"GETPAID-ris-emit/1.0 (mailto:{EMAIL})"

_email_warned = False


def warn_if_default_email():
    """Emit a one-shot stderr warning when LITPIPE_EMAIL is unset.

    Per Unpaywall/CrossRef/NCBI/Semantic Scholar ToS, API traffic should
    identify a contact mailto. The pipeline's default falls back to the
    maintainer's personal inbox — fine for the maintainer, surprising
    for anyone else. Entry-point scripts (sweep.py, snowball.py) call
    this on startup so unattributed traffic doesn't silently route to
    the wrong person.
    """
    global _email_warned
    if _email_warned or os.environ.get("LITPIPE_EMAIL"):
        return
    _email_warned = True
    print("[litpipe] LITPIPE_EMAIL not set — API requests will identify as "
          f"`{EMAIL}` (the maintainer). Set `export LITPIPE_EMAIL=you@example.org` "
          "to route traffic under your own contact.", file=sys.stderr)

CROSSREF_WORK   = "https://api.crossref.org/works/{doi}"
CROSSREF_SEARCH = "https://api.crossref.org/works"

SLUG_SKIP = {"a","an","the","of","in","on","and","to","for","at","from","with","by","as",
             "or","is","are","be","been","this","that","these","those"}


def slug(text: str, n: int = 6) -> str:
    text = re.sub(r"<[^>]+>", "", text or "")
    text = safe_ascii(text)
    text = re.sub(r"[^A-Za-z0-9\s\-]", " ", text)
    words = [w for w in text.split() if w.lower() not in SLUG_SKIP][:n]
    return "".join(re.sub(r"[^A-Za-z0-9\-]", "", w).capitalize() for w in words) or "Untitled"


def canonical_stem(year, lastname, title) -> str:
    yr   = str(year) if year and re.match(r"^\d{4}$", str(year)) else "Unknown"
    last = safe_ascii(re.sub(r"[^\w\-]", "", lastname or "")) or "Unknown"
    sl   = safe_ascii(slug(title or ""))
    return f"{yr}_{last}_{sl}"


def normalize_title(t: str) -> str:
    t = re.sub(r"<[^>]+>", "", t or "").lower()
    t = re.sub(r"[^a-z0-9 ]", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def title_similarity(a: str, b: str) -> float:
    return difflib.SequenceMatcher(None, normalize_title(a), normalize_title(b)).ratio()


def crossref_by_doi(doi: str, timeout=15):
    if not doi: return None
    try:
        r = lit_net.get(CROSSREF_WORK.format(doi=doi),
                         headers={"User-Agent": UA}, timeout=timeout)
        if r.status_code != 200: return None
        return r.json().get("message")
    except Exception:
        return None


def crossref_by_title(title: str, author_lastname: str = "", year: str = "",
                       timeout=20, sim_threshold: float = 0.85):
    """Fuzzy CrossRef search. Returns message dict if confident match, else None."""
    if not title or len(title.strip()) < 8: return None
    params = {"query.title": title, "rows": 5}
    if author_lastname: params["query.author"] = author_lastname
    try:
        r = lit_net.get(CROSSREF_SEARCH, params=params,
                         headers={"User-Agent": UA}, timeout=timeout)
        if r.status_code != 200: return None
        items = r.json().get("message", {}).get("items", []) or []
    except Exception:
        return None
    best = None; best_sim = 0.0
    for it in items:
        cand_title = (it.get("title") or [""])[0]
        sim = title_similarity(title, cand_title)
        if sim > best_sim:
            best_sim = sim; best = it
    if best and best_sim >= sim_threshold:
        # Optional year sanity check (within 1 year)
        if year:
            cand_year = ""
            for k in ("published-print","published-online","issued"):
                if k in best and best[k].get("date-parts"):
                    cand_year = str(best[k]["date-parts"][0][0]); break
            if cand_year and re.match(r"^\d{4}$", cand_year) and re.match(r"^\d{4}$", str(year)):
                if abs(int(cand_year) - int(year)) > 1: return None
        return best
    return None


def crossref_meta(msg: dict) -> dict:
    """Flatten a CrossRef /works message into a simple dict."""
    if not msg: return {}
    title = (msg.get("title") or [""])[0]
    year = ""
    date_iso = ""
    for k in ("published-print","published-online","issued"):
        if k in msg and msg[k].get("date-parts"):
            dp = msg[k]["date-parts"][0]
            year = str(dp[0]) if dp else ""
            date_iso = "/".join(f"{int(x):02d}" if i > 0 else str(x) for i, x in enumerate(dp))
            break
    authors = msg.get("author") or []
    first_last = (authors[0].get("family") or "").strip() if authors else ""
    return {
        "doi":      (msg.get("DOI") or "").lower(),
        "title":    title,
        "year":     year,
        "date":     date_iso,
        "lastname": first_last,
        "authors":  authors,
        "container": (msg.get("container-title") or [""])[0],
        "volume":   msg.get("volume") or "",
        "issue":    msg.get("issue") or "",
        "page":     msg.get("page") or "",
        "issn":     (msg.get("ISSN") or [""])[0] if msg.get("ISSN") else "",
        "abstract": re.sub(r"<[^>]+>", "", msg.get("abstract") or "") if msg.get("abstract") else "",
        "url":      f"https://doi.org/{msg.get('DOI')}" if msg.get("DOI") else "",
        "type":     msg.get("type") or "journal-article",
    }


# ---------- DataCite resolver (2026-07-20) ----------
# arXiv (10.48550/*), Zenodo (10.5281/*), figshare, Dryad, OSF, ChemRxiv ... are
# registered with DataCite, NOT CrossRef, so crossref_by_doi 404s and no .ris is
# written -> the paper is invisible to index_portfolio. This pair mirrors the
# CrossRef pair above and returns the SAME flattened dict shape build_ris consumes;
# resolve_meta keeps CrossRef first and falls back to DataCite on a miss.
# Full finding: notes/2026-07-20_datacite_ris_gap.md (validated 20/20 live, FRED prototype).
DATACITE_WORK = "https://api.datacite.org/dois/{doi}"

# Common DataCite registrants (fast-path hint; resolve_meta falls back on ANY CrossRef
# miss, so this list need not be exhaustive).
_DATACITE_PREFIXES = (
    "10.48550",  # arXiv
    "10.5281",   # Zenodo
    "10.6084",   # figshare
    "10.5061",   # Dryad
    "10.17605",  # OSF
    "10.26434",  # ChemRxiv
    "10.15468",  # GBIF
)

# DataCite resourceTypeGeneral -> CrossRef-style `type` key that _RIS_TYPE maps.
_DC_TYPE = {
    "Preprint": "posted-content", "Text": "journal-article",
    "JournalArticle": "journal-article", "ConferencePaper": "proceedings-article",
    "Dataset": "dataset", "Software": "posted-content", "Book": "book",
    "BookChapter": "book-chapter", "Report": "report", "Dissertation": "thesis",
}


def is_datacite_doi(doi: str) -> bool:
    d = (doi or "").strip().lower()
    return any(d.startswith(p) for p in _DATACITE_PREFIXES)


def datacite_by_doi(doi: str, timeout=20):
    """GET the DataCite record. Returns the `attributes` dict, or None."""
    if not doi:
        return None
    try:
        r = lit_net.get(DATACITE_WORK.format(doi=doi.strip()),
                         headers={"User-Agent": UA, "Accept": "application/vnd.api+json"},
                         timeout=timeout)
        if r.status_code != 200:
            return None
        return (r.json().get("data") or {}).get("attributes")
    except Exception:
        return None


def _dc_date(dates) -> str:
    """Pick a usable date -> 'YYYY/MM/DD' | 'YYYY/MM' | 'YYYY' | ''."""
    by_type = {}
    for d in dates or []:
        by_type.setdefault(d.get("dateType"), d.get("date") or "")
    for t in ("Issued", "Available", "Submitted", "Created", "Updated"):
        v = by_type.get(t) or ""
        for pat in (r"(\d{4})-(\d{2})-(\d{2})", r"(\d{4})-(\d{2})", r"(\d{4})"):
            m = re.match(pat, v)
            if m:
                return "/".join(m.groups())
    return ""


def _dc_authors(creators):
    """DataCite creators -> [{'family','given'}] in build_ris shape."""
    out = []
    for c in creators or []:
        fam = (c.get("familyName") or "").strip()
        giv = (c.get("givenName") or "").strip()
        if not fam:
            nm = (c.get("name") or "").strip()
            if not nm:
                continue
            if "," in nm:              # "Family, Given"
                fam, giv = [p.strip() for p in nm.split(",", 1)]
            else:                      # organizational or mononym
                fam = nm
        out.append({"family": fam, "given": giv})
    return out


def datacite_meta(attrs: dict) -> dict:
    """Flatten DataCite attributes into the same dict shape as crossref_meta."""
    if not attrs:
        return {}
    titles = attrs.get("titles") or []
    title = re.sub(r"\s+", " ", (titles[0].get("title") or "")).strip() if titles else ""
    authors = _dc_authors(attrs.get("creators"))
    abstract = ""
    for d in attrs.get("descriptions") or []:
        if (d.get("descriptionType") or "").lower() == "abstract":
            abstract = re.sub(r"<[^>]+>", "", d.get("description") or "").strip()
            break
    year = str(attrs.get("publicationYear") or "").strip()
    container = (attrs.get("container") or {}).get("title") or attrs.get("publisher") or ""
    rtg = (attrs.get("types") or {}).get("resourceTypeGeneral") or ""
    doi = (attrs.get("doi") or "").lower()
    return {
        "doi": doi, "title": title, "year": year,
        "date": _dc_date(attrs.get("dates")),
        "lastname": authors[0]["family"] if authors else "",
        "authors": authors, "container": container,
        "volume": "", "issue": "", "page": "", "issn": "",
        "abstract": abstract,
        "url": f"https://doi.org/{doi}" if doi else (attrs.get("url") or ""),
        "type": _DC_TYPE.get(rtg, "posted-content"),
    }


def resolve_meta(doi: str):
    """CrossRef first (unchanged pipeline behavior), DataCite fallback on miss.
    Returns (meta_dict, source) where source is 'crossref' | 'datacite' | 'none'."""
    msg = crossref_by_doi(doi)
    if msg:
        m = crossref_meta(msg)
        if m.get("title"):
            return m, "crossref"
    attrs = datacite_by_doi(doi)
    if attrs:
        m = datacite_meta(attrs)
        if m.get("title"):
            return m, "datacite"
    return {}, "none"


_RIS_TYPE = {
    "journal-article": "JOUR", "proceedings-article": "CPAPER", "book": "BOOK",
    "book-chapter": "CHAP", "report": "RPRT", "posted-content": "UNPD",
    "dataset": "DATA", "thesis": "THES", "monograph": "BOOK",
}


def _ris_pages(page: str):
    """CrossRef 'page' is often '267-277'; split into SP/EP."""
    if not page: return "", ""
    m = re.match(r"^\s*([\w\-]+)\s*[-–]\s*([\w\-]+)\s*$", page)
    if m: return m.group(1), m.group(2)
    return page.strip(), ""


def _ris_val(s) -> str:
    """Collapse internal whitespace/newlines to single spaces (F6: a raw newline in a
    free-text RIS field produces an orphaned continuation line with no `TAG  - ` prefix,
    which lenient importers mis-fold into the previous value or drop)."""
    return re.sub(r"\s+", " ", str(s)).strip()

def build_ris(meta: dict) -> str:
    """Build a single-record RIS string from a flattened meta dict."""
    if not meta: return ""
    ty = _RIS_TYPE.get(meta.get("type"), "JOUR")
    sp, ep = _ris_pages(meta.get("page", ""))
    lines = [f"TY  - {ty}"]
    for a in meta.get("authors", []):
        fam = _ris_val(a.get("family") or "")
        giv = _ris_val(a.get("given") or "")
        if fam:
            lines.append(f"AU  - {fam}, {giv}" if giv else f"AU  - {fam}")
    if meta.get("title"):     lines.append(f"TI  - {_ris_val(meta['title'])}")
    if meta.get("container"): lines.append(f"JO  - {_ris_val(meta['container'])}")
    if meta.get("year"):      lines.append(f"PY  - {meta['year']}")
    if meta.get("date"):      lines.append(f"DA  - {meta['date']}")
    if meta.get("volume"):    lines.append(f"VL  - {meta['volume']}")
    if meta.get("issue"):     lines.append(f"IS  - {meta['issue']}")
    if sp: lines.append(f"SP  - {sp}")
    if ep: lines.append(f"EP  - {ep}")
    if meta.get("doi"):       lines.append(f"DO  - {meta['doi']}")
    if meta.get("issn"):      lines.append(f"SN  - {meta['issn']}")
    if meta.get("url"):       lines.append(f"UR  - {meta['url']}")
    if meta.get("abstract"):  lines.append(f"AB  - {_ris_val(meta['abstract'])}")
    lines.append("ER  - ")
    return "\n".join(lines) + "\n"


def write_ris(path: str, ris_text: str, overwrite: bool = True) -> bool:
    """Write RIS string. Returns True if written, False if skipped (file exists & not overwrite)."""
    if (not overwrite) and os.path.exists(path):
        return False
    if not ris_text: return False
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    from lit_util import atomic_write_text  # RC4: crash-safe write (tmp + os.replace)
    atomic_write_text(path, ris_text)
    return True


# ---------- DOI extraction helpers (used by harvest + backfill) ----------

def extract_doi_from_text(text: str) -> str:
    # RC1 (2026-06-05 audit): delegate to lit_util, which re-joins line-wrapped DOIs and
    # rejects the truncation class ('10.1002/cphy', '10.1001/archinte') instead of the old
    # whitespace-stopping regex that produced those junk DOIs.
    from lit_util import extract_doi_from_text as _extract
    return _extract(text)


def emit_ris_for_pdf(doi: str, pdf_path: str, overwrite: bool = False) -> tuple:
    """One-shot: fetch CrossRef metadata for `doi`, write `<pdf_stem>.ris`.
    Used as a hook by the live fetchers (unpaywall_fetch_v2, pmc_fetch).

    Returns (status, ris_path) where status is one of:
      'OK'             - wrote a new RIS
      'EXISTS_SKIP'    - file already existed and overwrite=False
      'NO_DOI'         - empty doi argument
      'RESOLVE_FAIL'   - neither CrossRef nor DataCite returned usable metadata
    """
    if not doi:
        return ("NO_DOI", "")
    stem, _ = os.path.splitext(pdf_path)
    ris_path = stem + ".ris"
    if os.path.exists(ris_path) and not overwrite:
        return ("EXISTS_SKIP", ris_path)
    meta, _src = resolve_meta(doi)   # CrossRef first, DataCite fallback (arXiv/Zenodo/OSF/...)
    if not meta:
        return ("RESOLVE_FAIL", "")
    text = build_ris(meta)
    if not text:
        return ("RESOLVE_FAIL", "")
    write_ris(ris_path, text, overwrite=True)
    return ("OK", ris_path)
