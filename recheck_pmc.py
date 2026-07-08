"""Diligent-librarian sweep: for PDFs in a library that have NO sidecar yet,
extract their DOI from the PDF text, look up PMCID via NCBI idconv, and fetch
JATS sidecar + figures if a PMC entry exists.

This is the back-fill for papers that came in via channels other than our
Unpaywall/PMC fetch (e.g., manual collections, ILL, library proxy) — they
might have been retroactively deposited to PMC after the original publication.

Pipeline:
  1. List PDFs in --lib-dir that lack a matching .fulltext.json
  2. For each: pymupdf-extract first 5KB of text, regex DOI
  3. Batch-lookup DOI → PMCID via NCBI ID converter
  4. For PMCIDs found: fetch JATS via Europe PMC, parse via jats_to_text, save sidecar
  5. (Optional, --fetch-figures) Run fetch_figures.py logic on the new sidecars

Usage:
  python recheck_pmc.py --lib-dir c:/Users/<user>/Projects/getpaid/references/literature/
  python recheck_pmc.py --lib-dir DIR --only-prefix 197  # just 1970s papers
  python recheck_pmc.py --lib-dir DIR --dry-run         # show plan, don't fetch
"""
import os, sys, io, re, json, time, argparse, csv
import requests

try:
    if getattr(sys.stdout, "encoding", "").lower() != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, OSError):
    pass
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import fitz  # pymupdf
from jats_to_text import parse_jats
import lit_util            # RC1/RC4: DOI validity gate + atomic writes
import ris_emit as _R      # title_similarity for the sidecar title sanity check

EMAIL    = os.environ.get("LITPIPE_EMAIL", "JacobBowie@users.noreply.github.com")
UA       = f"GETPAID-recheck/1.0 (mailto:{EMAIL})"
IDCONV   = "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/"
EPMC_XML = "https://www.ebi.ac.uk/europepmc/webservices/rest/{pmcid}/fullTextXML"

# DOI regex: 10.XXXX/<anything-not-whitespace-or-trailing-punct>
DOI_RE = re.compile(r"\b(10\.\d{4,9}/[^\s\)\]\>\"',]+)", re.IGNORECASE)
DOI_TRAIL_PUNCT = re.compile(r"[.,;:\)\]\}\>]+$")


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


def extract_doi_from_pdf(pdf_path, max_chars=5000):
    """First well-formed DOI in a PDF's first ~max_chars, or ''.

    RC1: routes through lit_util.extract_doi_from_text, which re-joins line-wrapped
    DOIs and drops the truncation class ('10.1002/cphy') -- the old local regex
    truncated at the wrap. is_valid_doi() is the explicit well-formedness gate."""
    text = extract_pdf_head_text(pdf_path, max_chars)
    if not text:
        return ""
    doi = lit_util.extract_doi_from_text(text)
    return doi if lit_util.is_valid_doi(doi) else ""


def doi_to_pmcid_batch(dois, batch_size=100):
    """Map list of DOIs to PMCIDs. Returns dict doi -> pmcid."""
    out = {}
    for i in range(0, len(dois), batch_size):
        chunk = dois[i:i+batch_size]
        try:
            r = requests.get(IDCONV,
                              params={"tool":"GETPAID","email":EMAIL,
                                      "ids":",".join(chunk),"idtype":"doi","format":"json"},
                              headers={"User-Agent":UA}, timeout=30)
            for rec in r.json().get("records", []):
                doi = (rec.get("doi") or rec.get("requested-id") or "").lower()
                if rec.get("pmcid"):
                    out[doi] = rec["pmcid"]
        except (requests.RequestException, ValueError) as e:
            print(f"  idconv error: {e}", file=sys.stderr)
        time.sleep(0.4)
    return out


def fetch_jats_sidecar(pmcid, sidecar_path, doi="", pdf_head_text="", title_sim_min=0.55):
    """Fetch JATS XML, parse, write sidecar. Returns (ok, status).

    RC3-style title sanity check: the DOI was scraped from the PDF's first 5KB and the
    PMCID resolved from that DOI, so a sidecar whose JATS title is nowhere in the PDF
    head signals a wrong-paper attachment (bad scrape / idconv collision). When PDF head
    text is available and the JATS title shares too little with it, refuse to write the
    sidecar and report TITLE_MISMATCH instead of silently filing a confidently-wrong one.
    RC4: the sidecar is written atomically (tmp + os.replace)."""
    try:
        r = requests.get(EPMC_XML.format(pmcid=pmcid),
                          headers={"User-Agent":UA}, timeout=30)
        if r.status_code != 200:
            return False, f"HTTP_{r.status_code}"
        if not r.content.strip().startswith(b"<"):
            return False, "EMPTY_OR_NON_XML"
        parsed = parse_jats(r.content)
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
    doi2pmc = doi_to_pmcid_batch(list(set(fn_to_doi.values())))
    print(f"  {len(doi2pmc)}/{len(set(fn_to_doi.values()))} have PMCIDs\n")

    if not doi2pmc:
        print("No PMCIDs found for any extracted DOI. Done.")
        return

    rows = []
    n_fetched = n_fail = n_no_pmc = 0
    for fn, doi in fn_to_doi.items():
        pmcid = doi2pmc.get(doi)
        rec = {"filename": fn, "doi": doi, "pmcid": pmcid or "",
                "sidecar": False, "status": ""}
        if not pmcid:
            n_no_pmc += 1
            rec["status"] = "NO_PMCID"
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
        time.sleep(0.6)

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
