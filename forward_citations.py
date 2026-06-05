"""Forward-citation walker (Semantic Scholar API).

For every PDF in a project's library, find the papers that CITE it. Promoted
from getpaid/tools/enrich_library_v2.py — simplified now that each PDF has a
.ris sidecar with canonical DOI baked in (no need to re-extract titles from PDF
first-pages and re-query CrossRef).

Reads DOI from each PDF's `.ris` sidecar (preferred), then falls back to
.fulltext.json sidecar, then extracts from PDF text.

S2 unauthenticated rate limit: ~1 req/sec averaged over time, but bursts get
429'd. Tool sleeps ~3.2s between calls and back-off-retries on 429. Get an
S2 API key (free) and pass --s2-key for ~10x faster runs.

Usage:
  # By project name (recommended)
  python forward_citations.py --project Physiological_Data

  # Explicit paths
  python forward_citations.py --lib-dir /path/to/library

  # First N seeds for testing
  python forward_citations.py --project Physiological_Data --limit 5

Outputs (default location: <lib-dir>/_forward_citations.csv):
  Columns: seed_pdf, seed_doi, citing_paper_id, citing_doi, citing_title,
           citing_year, citing_authors, citing_venue, citing_cited_by, citing_abstract
"""
import os, sys, io, csv, re, json, time, argparse
from pathlib import Path

import lit_util

try:
    if getattr(sys.stdout, "encoding", "").lower() != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, OSError):
    pass

PROJECTS_ROOT = Path(os.path.expanduser("~/Projects"))
CONFIG_PATH   = Path(__file__).parent / "projects.json"

EMAIL = os.environ.get("LITPIPE_EMAIL", "jacob.bowie2@gmail.com")
UA    = f"GETPAID-fwdcite/1.0 (mailto:{EMAIL})"
S2    = "https://api.semanticscholar.org/graph/v1"

import requests


def _gate_doi(doi: str) -> str:
    """RC1: normalize then drop malformed/suspicious (truncated) DOIs."""
    d = lit_util.normalize_doi(doi)
    if not lit_util.is_valid_doi(d) or lit_util.is_suspicious_doi(d):
        return ""
    return d


def doi_from_ris(ris_path: Path) -> str:
    if not ris_path.exists(): return ""
    try:
        with open(ris_path, encoding="utf-8") as f:
            for line in f:
                m = re.match(r"^DO\s{2}-\s?(.+)$", line)
                if m: return _gate_doi(m.group(1))
    except OSError: pass
    return ""


def doi_from_sidecar(sc_path: Path) -> str:
    if not sc_path.exists(): return ""
    try:
        with open(sc_path, encoding="utf-8") as f:
            d = json.load(f)
        return _gate_doi(d.get("doi") or "")
    except (OSError, ValueError): return ""


def doi_from_pdf(pdf_path: Path, max_chars=5000) -> str:
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
    # RC1: re-joins line-wrapped DOIs and drops truncated/suspicious ones.
    return lit_util.extract_doi_from_text(text, max_chars=max_chars)


def get_doi(pdf: Path) -> str:
    stem = pdf.with_suffix("")
    return (doi_from_ris(stem.with_suffix(".ris"))
         or doi_from_sidecar(stem.with_suffix(".fulltext.json"))
         or doi_from_pdf(pdf))


def s2_get(path, params=None, headers=None, timeout=20, retries=3):
    """GET with 429 back-off."""
    h = {"User-Agent": UA, **(headers or {})}
    for attempt in range(retries):
        try:
            r = requests.get(f"{S2}{path}", params=params, headers=h, timeout=timeout)
            if r.status_code == 200: return r.json()
            if r.status_code == 429:
                time.sleep(5 * (attempt + 1)); continue
            return {"error": f"HTTP {r.status_code}"}
        except Exception as e:
            if attempt == retries - 1: return {"error": str(e)}
            time.sleep(3)
    return {"error": "retries exhausted"}


def resolve_project(name: str):
    from ris_emit import load_projects_config
    cfg = load_projects_config(CONFIG_PATH).get("projects", {})
    if name not in cfg:
        print(f"[ERR] '{name}' not in projects.json", file=sys.stderr); sys.exit(2)
    p = cfg[name]
    base = PROJECTS_ROOT / (p.get("parent") or name)
    return base / p["lib_dir"]


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    ap.add_argument("--project", default=None,
                     help="Project name from projects.json (e.g. 'Physiological_Data').")
    ap.add_argument("--lib-dir", default=None,
                     help="Explicit library path (legacy).")
    ap.add_argument("--report",  default=None,
                     help="Output CSV (default: <lib-dir>/_forward_citations.csv).")
    ap.add_argument("--limit",   type=int, default=0,
                     help="Process first N seeds only (testing).")
    ap.add_argument("--sleep",   type=float, default=3.2,
                     help="Seconds between S2 calls (default 3.2 for unauthenticated).")
    ap.add_argument("--s2-key",  default=None,
                     help="Semantic Scholar API key (env: S2_API_KEY). Faster.")
    args = ap.parse_args()

    if args.project:
        lib = resolve_project(args.project)
    elif args.lib_dir:
        lib = Path(args.lib_dir)
    else:
        print("[ERR] Pass --project or --lib-dir", file=sys.stderr); sys.exit(2)

    if not lib.is_dir():
        print(f"[ERR] not a directory: {lib}", file=sys.stderr); sys.exit(2)

    s2_key = args.s2_key or os.environ.get("S2_API_KEY", "")
    s2_headers = {"x-api-key": s2_key} if s2_key else {}
    if s2_key:
        args.sleep = max(args.sleep, 1.1)  # still polite even with key
        print(f"[info] using S2 API key (faster rate limit)")

    pdfs = sorted(lib.glob("*.pdf"))
    if args.limit: pdfs = pdfs[:args.limit]

    out = Path(args.report) if args.report else (lib / "_forward_citations.csv")
    print(f"library:  {lib}")
    print(f"PDFs:     {len(pdfs)}")
    print(f"output:   {out}\n")

    fields = ["seed_pdf","seed_doi","citing_paper_id","citing_doi","citing_title",
              "citing_year","citing_authors","citing_venue","citing_cited_by",
              "citing_abstract"]
    rows = []
    no_doi = []; resolve_fail = []
    n_with_cites = 0; total_citing = 0

    for i, pdf in enumerate(pdfs, 1):
        doi = get_doi(pdf)
        if not doi:
            no_doi.append(pdf.name)
            print(f"  [{i:>3}/{len(pdfs)}] {pdf.name[:55]:<55}  NO_DOI")
            continue

        d = s2_get(f"/paper/DOI:{doi}",
                   params={"fields": "paperId,title,citationCount"},
                   headers=s2_headers)
        time.sleep(args.sleep)
        if "error" in d or not d.get("paperId"):
            resolve_fail.append((pdf.name, doi, d.get("error","no paperId")))
            print(f"  [{i:>3}/{len(pdfs)}] {pdf.name[:50]:<50} {doi[:30]:<30} S2_FAIL")
            continue

        pid = d["paperId"]
        cites = s2_get(f"/paper/{pid}/citations",
                       params={"limit": 1000,
                               "fields": "title,abstract,year,authors,citationCount,venue,externalIds"},
                       headers=s2_headers)
        time.sleep(args.sleep)
        data = cites.get("data", []) if isinstance(cites, dict) else []
        if data: n_with_cites += 1
        total_citing += len(data)

        for c in data:
            cp = c.get("citingPaper", {}) if "citingPaper" in c else c
            ext = cp.get("externalIds") or {}
            authors = cp.get("authors") or []
            # RC1: gate the S2-supplied citing DOI so malformed values don't
            # reach the CSV / unique-DOI list that feeds sweep.py.
            citing_doi = _gate_doi(ext.get("DOI") or "")
            rows.append({
                "seed_pdf":         pdf.name,
                "seed_doi":         doi,
                "citing_paper_id":  cp.get("paperId",""),
                "citing_doi":       citing_doi,
                "citing_title":     cp.get("title",""),
                "citing_year":      cp.get("year",""),
                "citing_authors":   "; ".join(a.get("name","") for a in authors),
                "citing_venue":     cp.get("venue",""),
                "citing_cited_by":  cp.get("citationCount",""),
                "citing_abstract":  (cp.get("abstract") or "")[:1500],
            })
        print(f"  [{i:>3}/{len(pdfs)}] {pdf.name[:50]:<50} {doi[:30]:<30} {len(data):>4} citing")

    out.parent.mkdir(parents=True, exist_ok=True)
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
    w.writeheader(); w.writerows(rows)
    lit_util.atomic_write_text(str(out), buf.getvalue())

    # Unique citing-DOI summary
    uniq_dois = sorted({r["citing_doi"] for r in rows if r["citing_doi"]})
    uniq_path = out.with_name(out.stem + "_unique_dois.csv")
    buf = io.StringIO()
    w = csv.writer(buf); w.writerow(["doi"])
    for d in uniq_dois: w.writerow([d])
    lit_util.atomic_write_text(str(uniq_path), buf.getvalue())

    print()
    print("=== summary ===")
    print(f"  seeds:              {len(pdfs)}")
    print(f"  with DOI:           {len(pdfs) - len(no_doi)}")
    print(f"  no DOI:             {len(no_doi)}")
    print(f"  S2 resolve failed:  {len(resolve_fail)}")
    print(f"  seeds with citers:  {n_with_cites}")
    print(f"  total citing rows:  {total_citing}")
    print(f"  unique citing DOIs: {len(uniq_dois)}")
    print(f"  report:             {out}")
    print(f"  unique DOIs:        {out.with_name(out.stem + '_unique_dois.csv')}")


if __name__ == "__main__":
    main()
