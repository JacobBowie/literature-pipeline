"""Backfill .fulltext.json sidecars for an existing PDF library.

Walks a directory of PDFs, looks up DOI→PMCID for each, and writes the full-text JSON next to
each PDF: Europe PMC's JATS for open-access articles, the BioC text for author manuscripts that
Europe PMC does not serve (both through litpipe.net; jats_to_text.fetch_fulltext).

Inputs:
  --lib-dir        Directory of PDFs to process.
  --doi-source     CSV with doi+filename columns (or filename column starting with year_author).
                   For example the Unpaywall stage report (it carries the DOI of every paper
                   that stage saw). For papers not in any CSV, see --pmcid-fallback.
  --pmcid-fallback Optional JSON map {filename: PMCID}. Used when no DOI lookup succeeds.
  --report         Where to write the per-file report CSV (default: <lib-dir>/_fulltext_backfill_report.csv).
  --dry-run        Show plan without fetching.

Usage:
  # DOIs from the stage reports
  python backfill_fulltext.py \\
      --lib-dir <project>/references/literature \\
      --doi-source <project>/unpaywall_fetch_report_v2.csv \\
      --doi-source <project>/pmc_fetch_report.csv

  # a PMCID map instead
  python backfill_fulltext.py \\
      --lib-dir <project>/docs/literature \\
      --pmcid-fallback <scratch>/pmcid_map.json

The script is read-only with respect to PDFs — it only writes sidecars next to them.
"""
import os, sys, csv, json, argparse
from xml.etree import ElementTree as ET

import lit_util
import lit_net  # B1/c8: doi_to_pmcid_batch (the DOI -> PMCID route and its identity are lit_net's)
lit_util.utf8_stdout()
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from jats_to_text import fetch_fulltext  # c13: the shared JATS (then BioC) fetch + parse
from fetch_figures import merge_figures


def fetch_sidecar(pmcid, sidecar_path):
    """Fetch the article's full text (Europe PMC JATS; BioC for an author manuscript Europe PMC does
    not hold, N-B4), parse it and write the sidecar. Returns (ok, status): "OK", "NOT_AVAILABLE" (in
    neither source), or the fetch's failure status ("TRANSPORT: ...", "HTTP_<code>", ...) or
    "ERROR_<type>:<msg>" for an unparseable document. A network failure never raises."""
    try:
        parsed, status, _source = fetch_fulltext(pmcid)
        if parsed is None:
            return False, status
        # RC5: --refresh re-fetches over an existing sidecar; the fresh parse has no fetched figure
        # image_path/image_url/licence and may lack a doi/authors the prior write carried.
        # merge_sidecar keeps those enriched fields; merge_figures carries the per-figure image
        # fields over (merge_sidecar keeps an old figures list only when the new one is empty).
        if os.path.exists(sidecar_path):
            try:
                with open(sidecar_path, encoding="utf-8") as f:
                    old = json.load(f)
            except (OSError, ValueError):
                old = None
            if isinstance(old, dict):
                parsed["figures"] = merge_figures(old.get("figures"), parsed.get("figures"))
            parsed = lit_util.merge_sidecar(old, parsed)
        lit_util.atomic_write_json(sidecar_path, parsed)  # RC4: crash-safe
        return True, "OK"
    except (OSError, ET.ParseError, ValueError) as e:
        return False, f"ERROR_{type(e).__name__}:{str(e)[:60]}"


def load_doi_map(sources):
    """Read all CSVs, build {filename: doi}.  Filename column = 'filename'."""
    fn2doi = {}
    for src in sources:
        if not os.path.exists(src):
            print(f"  WARN: missing {src}"); continue
        with open(src, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for r in reader:
                fn = (r.get("filename") or "").strip()
                doi = (r.get("doi") or "").strip().lower()
                if fn and doi:
                    fn2doi.setdefault(fn, doi)
    return fn2doi


def load_pmcid_fallback(path):
    if not path: return {"by_doi": {}, "by_filename": {}}
    if not os.path.exists(path):
        print(f"  WARN: pmcid-fallback {path} missing"); return {"by_doi": {}, "by_filename": {}}
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    # Two shapes accepted:
    #   {pmcid: {"pmid":..,"doi":..}}            — pmcid keys
    #   {filename: pmcid}                         — filename keys
    out_doi = {}; out_fn = {}
    for k,v in data.items():
        if k.startswith("PMC") and isinstance(v, dict):
            doi = (v.get("doi") or "").lower()
            if doi: out_doi[doi] = k
        elif isinstance(v, str) and v.startswith("PMC"):
            out_fn[k] = v
        elif isinstance(v, dict) and "pmcid" in v:
            out_fn[k] = v["pmcid"]
    return {"by_doi": out_doi, "by_filename": out_fn}


def ascii_only(s): return s.encode("ascii","ignore").decode("ascii")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lib-dir", required=True)
    ap.add_argument("--doi-source", action="append", default=[],
                     help="CSV with filename + doi columns. Can repeat.")
    ap.add_argument("--pmcid-fallback", default=None,
                     help="JSON map {pmcid:{doi:..}} or {filename: pmcid}.")
    ap.add_argument("--report", default=None)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--refresh", action="store_true",
                     help="Re-fetch and overwrite existing sidecars (for parser changes)")
    args = ap.parse_args()

    lib = os.path.abspath(args.lib_dir)
    if not os.path.isdir(lib):
        print(f"ERR: lib-dir not a directory: {lib}", file=sys.stderr); sys.exit(1)

    pdfs = sorted(f for f in os.listdir(lib) if f.endswith(".pdf"))
    print(f"Library: {lib}\n  {len(pdfs)} PDFs found")

    # 1. Map filename -> doi from CSVs
    fn2doi = load_doi_map(args.doi_source)
    # also try ASCII-stripped lookup (handles unicode-stripped saved filenames)
    ascii_fn2doi = {ascii_only(k): v for k,v in fn2doi.items()}
    # 2. Fallback PMCID map
    fb = load_pmcid_fallback(args.pmcid_fallback) if args.pmcid_fallback else {"by_doi":{},"by_filename":{}}

    # Resolve each PDF to a DOI first
    resolved = []  # list of (filename, doi-or-empty, pmcid-or-empty, source)
    needs_doi_lookup = []
    for fn in pdfs:
        sidecar_path = os.path.join(lib, fn[:-4] + ".fulltext.json")
        if os.path.exists(sidecar_path) and not args.refresh:
            resolved.append((fn, "", "", "ALREADY_EXISTS")); continue
        # 1) by filename CSV
        doi = fn2doi.get(fn) or ascii_fn2doi.get(fn) or ascii_fn2doi.get(ascii_only(fn))
        # 2) by filename in fallback
        pmcid = fb["by_filename"].get(fn, "")
        if doi:
            resolved.append((fn, doi, "", "csv_doi"))
            needs_doi_lookup.append(doi)
        elif pmcid:
            resolved.append((fn, "", pmcid, "fallback_pmcid"))
        else:
            resolved.append((fn, "", "", "NO_LOOKUP"))

    # Batch DOI -> PMCID
    print(f"\nLooking up {len(set(needs_doi_lookup))} unique DOIs in PMC...")
    doi2pmcid = (lit_net.doi_to_pmcid_batch(sorted(set(needs_doi_lookup)))
                 if needs_doi_lookup else {})
    # also fold in fallback by_doi
    for d, pmc in fb["by_doi"].items():
        doi2pmcid.setdefault(d, pmc)

    # 3) Fetch sidecars
    n_dl = n_skip = n_no_pmc = n_na = n_fail = 0
    rows = []
    for fn, doi, pmcid, src in resolved:
        if src == "ALREADY_EXISTS":
            n_skip += 1
            rows.append({"filename":fn,"doi":doi,"pmcid":pmcid,
                          "sidecar":True,"status":"EXISTS","source":src})
            continue
        if not pmcid and doi:
            pmcid = doi2pmcid.get(doi, "")
        if not pmcid:
            n_no_pmc += 1
            rows.append({"filename":fn,"doi":doi,"pmcid":"",
                          "sidecar":False,"status":"NO_PMCID","source":src})
            print(f"  --   {fn[:80]} (no PMCID)")
            continue
        sidecar_path = os.path.join(lib, fn[:-4] + ".fulltext.json")
        if args.dry_run:
            rows.append({"filename":fn,"doi":doi,"pmcid":pmcid,
                          "sidecar":False,"status":"DRY","source":src})
            print(f"  DRY  {pmcid:<12} -> {fn[:65]}")
            continue
        ok, st = fetch_sidecar(pmcid, sidecar_path)
        if ok:
            n_dl += 1
            print(f"  DL   {pmcid:<12} -> {fn[:65]} sidecar")
        elif st == "NOT_AVAILABLE":
            n_na += 1
            print(f"  --   {pmcid:<12} -> {fn[:65]} (not in Europe PMC's OA full text or PMC's BioC collections)")
        else:
            n_fail += 1
            print(f"  FAIL {pmcid:<12} -> {fn[:65]} ({st})")
        rows.append({"filename":fn,"doi":doi,"pmcid":pmcid,
                      "sidecar":ok,"status":st,"source":src})

    report = args.report or os.path.join(lib, "_fulltext_backfill_report.csv")
    lit_util.atomic_write_csv(report, rows,
        fieldnames=["filename","doi","pmcid","sidecar","status","source"])

    total = len(pdfs)
    print(f"\n=== Summary ===")
    print(f"  PDFs in library:        {total}")
    print(f"  Sidecars already there: {n_skip}")
    print(f"  Sidecars NEW:           {n_dl}")
    print(f"  Sidecars unavailable:   {n_na} (not in Europe PMC OA full text or BioC)")
    print(f"  No PMCID found:         {n_no_pmc}")
    print(f"  Errors:                 {n_fail}")
    print(f"\nReport: {report}")


if __name__ == "__main__":
    main()
