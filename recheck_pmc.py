"""Diligent-librarian sweep: for PDFs in a library that have NO sidecar yet,
extract their DOI from the PDF text, look up PMCID via NCBI idconv, and fetch
JATS sidecar + figures if a PMC entry exists.

This is the back-fill for papers that came in via channels other than our
Unpaywall/PMC fetch (e.g., manual collections, ILL, library proxy) — they
might have been retroactively deposited to PMC after the original publication.

Pipeline:
  1. List PDFs in --lib-dir that lack a matching .fulltext.json
  2. For each: pymupdf-extract first 5KB of text, regex DOI
  3. Batch-lookup DOI → PMCID (lit_net.doi_to_pmcid_batch)
  4. For PMCIDs found: fetch Europe PMC JATS (BioC for an author manuscript Europe PMC does not
     hold), parse via jats_to_text, save sidecar
  5. Figures: run fetch_figures.py --lib-dir DIR afterwards (PMC Cloud Service images + licences)

Usage:
  python recheck_pmc.py --lib-dir <project>/references/literature/
  python recheck_pmc.py --lib-dir DIR --only-prefix 197  # just 1970s papers
  python recheck_pmc.py --lib-dir DIR --dry-run         # show plan, don't fetch
"""
import os, sys, io, argparse, csv

import lit_util
import lit_net  # B1/c8: doi_to_pmcid_batch (the DOI -> PMCID route and its identity are lit_net's)
lit_util.utf8_stdout()
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import fitz  # pymupdf
from jats_to_text import fetch_fulltext  # c13: the shared JATS (then BioC) fetch + parse
import ris_emit as _R      # title_similarity for the sidecar title sanity check


def extract_pdf_head_text(pdf_path, max_chars=5000):
    """Return the first ~max_chars of a PDF's text (for DOI + title checks), or ''."""
    try:
        doc = fitz.open(pdf_path)
        text = ""
        try:
            for page in doc:
                text += page.get_text()
                if len(text) >= max_chars: break
        finally:
            doc.close()
    except (OSError, RuntimeError, ValueError):
        return ""
    return text[:max_chars]


def fetch_jats_sidecar(pmcid, sidecar_path, doi="", pdf_head_text="", title_sim_min=0.55):
    """Fetch the full text (Europe PMC JATS, else BioC for an author manuscript: N-B4), parse, write
    the sidecar. Returns (ok, status); a network failure is a status ("TRANSPORT: ...",
    "HTTP_<code>", ...), never an exception, and "NOT_AVAILABLE" means neither source holds it.

    RC3-style title sanity check: the DOI was scraped from the PDF's first 5KB and the
    PMCID resolved from that DOI, so a sidecar whose JATS title is nowhere in the PDF
    head signals a wrong-paper attachment (bad scrape / idconv collision). When PDF head
    text is available and the JATS title shares too little with it, refuse to write the
    sidecar and report TITLE_MISMATCH instead of silently filing a confidently-wrong one.
    RC4: the sidecar is written atomically (tmp + os.replace)."""
    try:
        parsed, status, _source = fetch_fulltext(pmcid)
        if parsed is None:
            return False, status
        jats_title = (parsed.get("title") or "").strip()
        if pdf_head_text and jats_title and len(jats_title) >= 8:
            head_norm = _R.normalize_title(pdf_head_text)
            title_norm = _R.normalize_title(jats_title)
            # Prefer a containment check (the title usually appears verbatim in the head);
            # fall back to fuzzy similarity against the head's leading slice.
            contained = title_norm and title_norm in head_norm
            sim = _R.title_similarity(jats_title, pdf_head_text[:max(len(jats_title) * 3, 120)])
            if not contained and sim < title_sim_min:
                return False, f"TITLE_MISMATCH_sim={sim:.2f}"
        # Annotate provenance
        parsed["_recheck_source_doi"] = doi
        lit_util.atomic_write_json(sidecar_path, parsed)  # RC4: crash-safe write
        return True, "OK"
    except Exception as e:
        return False, f"ERROR_{str(e)[:60]}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lib-dir", required=True)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only-prefix", default=None,
                     help="Only process PDFs whose filename starts with this (e.g., '197' for 1970s)")
    ap.add_argument("--report", default=None,
                     help="Output CSV report (default: <lib-dir>/_pmc_recheck_report.csv)")
    args = ap.parse_args()

    lib = os.path.abspath(args.lib_dir)
    pdfs = sorted(f for f in os.listdir(lib)
                   if f.endswith(".pdf")
                   and not os.path.exists(os.path.join(lib, f[:-4] + ".fulltext.json")))
    if args.only_prefix:
        pdfs = [p for p in pdfs if p.startswith(args.only_prefix)]

    print(f"Library: {lib}")
    print(f"PDFs without sidecar: {len(pdfs)}")
    if args.dry_run: print("(dry run)\n")

    print(f"\nExtracting DOIs from PDFs...")
    fn_to_doi = {}
    fn_to_text = {}   # head text retained for the sidecar title sanity check (RC3)
    for fn in pdfs:
        head = extract_pdf_head_text(os.path.join(lib, fn))
        fn_to_text[fn] = head
        doi = lit_util.extract_doi_from_text(head)
        if doi and lit_util.is_valid_doi(doi):
            fn_to_doi[fn] = doi
    print(f"  {len(fn_to_doi)}/{len(pdfs)} PDFs had a DOI in their first 5KB\n")

    if not fn_to_doi:
        print("Nothing to look up. Done.")
        return

    print(f"Batch-looking up {len(set(fn_to_doi.values()))} unique DOIs...")
    lookups = lit_net.doi_to_pmcid(list(set(fn_to_doi.values())))
    doi2pmc = {d: o.payload.pmcid for d, o in lookups.items() if o.ok and o.payload.pmcid}
    print(f"  {len(doi2pmc)}/{len(set(fn_to_doi.values()))} have PMCIDs\n")

    rows = []
    n_fetched = n_fail = n_no_pmc = 0
    for fn, doi in fn_to_doi.items():
        pmcid = doi2pmc.get(doi.strip().lower())
        rec = {"filename": fn, "doi": doi, "pmcid": pmcid or "",
                "sidecar": False, "status": ""}
        if not pmcid:
            # a lookup no service could answer (refused, down) is not "no PMCID" (W2-A1)
            rec["status"] = lit_net.lookup_status(lookups.get(doi.strip().lower()))
            if rec["status"] == "NO_PMCID":
                n_no_pmc += 1
            else:
                n_fail += 1
            rows.append(rec)
            continue
        sidecar_path = os.path.join(lib, fn[:-4] + ".fulltext.json")
        if args.dry_run:
            rec["status"] = "DRY_WOULD_FETCH"
            rows.append(rec)
            print(f"  DRY  {pmcid:<12} -> {fn[:60]}")
            continue
        ok, st = fetch_jats_sidecar(pmcid, sidecar_path, doi=doi,
                                    pdf_head_text=fn_to_text.get(fn, ""))
        rec["sidecar"] = ok; rec["status"] = st
        if ok:
            n_fetched += 1
            print(f"  OK   {pmcid:<12} -> {fn[:60]} (sidecar)")
        else:
            n_fail += 1
            print(f"  FAIL {pmcid:<12} -> {fn[:60]} ({st})")
        rows.append(rec)

    # Also: PDFs without DOI in their text — log for visibility
    no_doi = [fn for fn in pdfs if fn not in fn_to_doi]
    for fn in no_doi:
        rows.append({"filename": fn, "doi": "", "pmcid": "",
                     "sidecar": False, "status": "NO_DOI_IN_PDF_TEXT"})

    report_path = args.report or os.path.join(lib, "_pmc_recheck_report.csv")
    # RC4: build CSV in memory, write atomically (tmp + os.replace).
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=["filename","doi","pmcid","sidecar","status"],
                       extrasaction="ignore", lineterminator="\n")
    w.writeheader(); w.writerows(rows)
    lit_util.atomic_write_text(report_path, buf.getvalue())

    n_title_mismatch = sum(1 for r in rows if str(r.get("status","")).startswith("TITLE_MISMATCH"))
    print(f"\n=== Summary ===")
    print(f"  PDFs without sidecar:    {len(pdfs)}")
    print(f"  DOIs extracted:          {len(fn_to_doi)}")
    print(f"  PMCIDs found:            {len(doi2pmc)}")
    print(f"  Sidecars NEW:            {n_fetched}")
    print(f"  Fetch failed:            {n_fail}")
    print(f"  Title mismatch:          {n_title_mismatch} (JATS title not in PDF; sidecar skipped)")
    print(f"  No PMCID:                {n_no_pmc}")
    print(f"  No DOI in text:          {len(no_doi)}")
    print(f"\nReport: {report_path}")


if __name__ == "__main__":
    main()
