"""Fetch PDFs from PMC for DOIs that Unpaywall couldn't deliver, plus a JSON
full-text sidecar from Europe PMC's JATS endpoint (when available).

Strategy:
  1. Convert DOI -> PMCID via NCBI ID converter (idconv API; same source the PubMed MCP uses).
  2. Try Europe PMC's getPdf endpoint (most reliable, returns PDF directly).
  3. Fall back to NCBI PMC: scrape the article page for citation_pdf_url meta.
  4. Whether or not the PDF succeeds, also try Europe PMC fullTextXML and write
     a `<filename>.fulltext.json` sidecar (parsed via tools/jats_to_text.py).

Input:  data/prior_art/discovered/unpaywall_fetch_report_v2.csv (downloaded=False rows)
Output: PDFs + .fulltext.json into references/literature/
        data/prior_art/discovered/pmc_fetch_report.csv

Usage:
  python tools/pmc_fetch.py [--dry-run] [--only-doi DOI ...] [--no-sidecar]
"""
import os, sys, io, csv, re, time, json, argparse
import requests
from urllib.parse import urljoin

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from jats_to_text import parse_jats
import ris_emit as _R
import lit_util  # RC2/RC3/RC4 audit-remediation helpers
# RC2/RC3: reuse the collision-safe dest + DOI<->content helpers (single source of truth).
from unpaywall_fetch_v2 import (resolve_dest, pdf_doi_disagrees,
                                doi_from_pdf_bytes, _doi_of_existing)

try:
    if getattr(sys.stdout, "encoding", "").lower() != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, OSError):
    pass

EMAIL      = os.environ.get("LITPIPE_EMAIL", "jacob.bowie2@gmail.com")
IDCONV     = "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/"
EPMC_PDF   = "https://europepmc.org/articles/{pmcid}?pdf=render"
EPMC_XML   = "https://www.ebi.ac.uk/europepmc/webservices/rest/{pmcid}/fullTextXML"
NCBI_PAGE  = "https://pmc.ncbi.nlm.nih.gov/articles/{pmcid}/"

BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
API_UA     = f"GETPAID-pmc-fetch/1.0 (mailto:{EMAIL})"

DEFAULT_REPORT_IN  = "data/prior_art/discovered/unpaywall_fetch_report_v2.csv"
DEFAULT_LIB        = "references/literature"
DEFAULT_REPORT_OUT = "data/prior_art/discovered/pmc_fetch_report.csv"


def safe_filename(name: str) -> str:
    # Strip non-ASCII (matches v2 naming convention which is ASCII-only).
    return name.encode("ascii", "ignore").decode("ascii")


def doi_to_pmcid_batch(dois, batch_size=100):
    """Map list of DOIs to PMCIDs using NCBI ID converter. Returns dict doi -> pmcid (lowercased doi keys)."""
    out = {}
    for i in range(0, len(dois), batch_size):
        chunk = dois[i:i+batch_size]
        params = {"tool": "GETPAID", "email": EMAIL, "ids": ",".join(chunk),
                  "idtype": "doi", "format": "json"}
        try:
            r = requests.get(IDCONV, params=params, headers={"User-Agent": API_UA}, timeout=30)
            data = r.json()
        except Exception as e:
            print(f"  [idconv batch {i}] error: {e}")
            continue
        for rec in data.get("records", []):
            doi = (rec.get("doi") or rec.get("requested-id") or "").lower()
            pmcid = rec.get("pmcid")
            if doi and pmcid:
                out[doi] = pmcid
        time.sleep(0.4)  # be nice to NCBI
    return out


def looks_like_pdf(b: bytes) -> bool:
    return b[:4] == b"%PDF"


def try_download(url, dest, timeout=30):
    """Stream-download. Returns (ok, status_label, msg)."""
    try:
        r = requests.get(url, headers={"User-Agent": BROWSER_UA, "Accept": "application/pdf,*/*"},
                          timeout=timeout, stream=True, allow_redirects=True)
        if r.status_code != 200:
            return False, f"HTTP_{r.status_code}", ""
        first = b""
        chunks = []
        total = 0
        for c in r.iter_content(chunk_size=8192):
            if not c: continue
            if not first: first = c
            chunks.append(c); total += len(c)
            if total > 60_000_000: break
        if not first:
            return False, "EMPTY", ""
        if looks_like_pdf(first):
            with open(dest, "wb") as f:
                for c in chunks: f.write(c)
            sz = os.path.getsize(dest)
            if sz < 10_000:
                os.remove(dest)
                return False, "TOO_SMALL", f"{sz}B"
            return True, "OK", f"{sz}B"
        return False, "HTML", b"".join(chunks)
    except Exception as e:
        return False, "ERROR", str(e)[:120]


def extract_citation_pdf_url(html_bytes: bytes, base: str):
    try:
        html = html_bytes.decode("utf-8", errors="replace")
    except Exception:
        return None
    m = re.search(r'<meta[^>]+name=["\']citation_pdf_url["\'][^>]+content=["\']([^"\']+)["\']',
                   html, re.IGNORECASE)
    return urljoin(base, m.group(1)) if m else None


def fetch_fulltext_sidecar(pmcid, sidecar_path):
    """Fetch JATS XML from Europe PMC, parse, write .fulltext.json sidecar.
    Returns (ok, status) where status is 'OK', 'NOT_AVAILABLE', or an error label."""
    try:
        r = requests.get(EPMC_XML.format(pmcid=pmcid),
                          headers={"User-Agent": API_UA}, timeout=30)
        if r.status_code == 404:
            return False, "NOT_AVAILABLE"
        if r.status_code != 200:
            return False, f"HTTP_{r.status_code}"
        if not r.content or not r.content.strip().startswith(b"<"):
            return False, "EMPTY_OR_NON_XML"
        parsed = parse_jats(r.content)
        lit_util.atomic_write_json(sidecar_path, parsed)  # RC4: crash-safe write
        return True, "OK"
    except Exception as e:
        return False, f"ERROR_{str(e)[:60]}"


def fetch_pmc_pdf(pmcid, dest):
    """Try Europe PMC first, then NCBI page + citation_pdf_url. Returns (ok, attempts)."""
    attempts = []

    # 1. Europe PMC getPdf
    epmc_url = EPMC_PDF.format(pmcid=pmcid)
    ok, st, msg = try_download(epmc_url, dest)
    attempts.append(("europepmc", epmc_url, st, msg if isinstance(msg, str) else f"<{len(msg)}B HTML>"))
    if ok: return True, attempts

    # 2. NCBI: fetch article page, parse citation_pdf_url
    page_url = NCBI_PAGE.format(pmcid=pmcid)
    ok2, st2, msg2 = try_download(page_url, dest)  # this won't be a PDF; treats as HTML
    attempts.append(("ncbi-page", page_url, st2, msg2 if isinstance(msg2, str) else f"<{len(msg2)}B HTML>"))
    if st2 == "HTML":
        pdf_url = extract_citation_pdf_url(msg2, page_url)
        if pdf_url:
            ok3, st3, msg3 = try_download(pdf_url, dest)
            attempts.append(("ncbi-citation_pdf_url", pdf_url, st3, str(msg3)[:60]))
            if ok3: return True, attempts

    return False, attempts


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    ap.add_argument("--dry-run", action="store_true",
                     help="Resolve PMCIDs but do not download PDFs or sidecars.")
    ap.add_argument("--only-doi", nargs="*", default=None,
                     help="Only attempt these DOIs (lowercase).")
    ap.add_argument("--no-sidecar", action="store_true",
                     help="Skip .fulltext.json sidecar fetch.")
    ap.add_argument("--no-write-ris", action="store_true",
                     help="Skip writing .ris sidecar next to each successfully fetched PDF.")
    ap.add_argument("--base-dir", default=os.getcwd(),
                     help="Project root. Default: CWD.")
    ap.add_argument("--report-in", default=None,
                     help=f"Input fetch report (default: <base-dir>/{DEFAULT_REPORT_IN})")
    ap.add_argument("--lib-dir", default=None,
                     help=f"PDF + sidecar destination (default: <base-dir>/{DEFAULT_LIB})")
    ap.add_argument("--report-out", default=None,
                     help=f"Output report CSV (default: <base-dir>/{DEFAULT_REPORT_OUT})")
    args = ap.parse_args()

    base = os.path.abspath(args.base_dir)
    report_in  = args.report_in  or os.path.join(base, DEFAULT_REPORT_IN)
    lib_dir    = args.lib_dir    or os.path.join(base, DEFAULT_LIB)
    report_out = args.report_out or os.path.join(base, DEFAULT_REPORT_OUT)

    # Read v2 report — keep rows where downloaded=False & not SKIP_EXISTS
    rows = []
    with open(report_in, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["downloaded"] == "True": continue
            if r["oa_status"] == "SKIP_EXISTS": continue
            d = r["doi"].strip().lower()
            if not d: continue
            if args.only_doi and d not in args.only_doi: continue
            rows.append(r)

    dois = [r["doi"].strip().lower() for r in rows]
    print(f"Project: {base}")
    print(f"Library: {lib_dir}")
    print(f"Looking up {len(dois)} DOIs in PMC...")
    doi2pmcid = doi_to_pmcid_batch(dois)
    print(f"  {len(doi2pmcid)}/{len(dois)} have PMCIDs ({100*len(doi2pmcid)/max(1,len(dois)):.0f}%)\n")

    os.makedirs(lib_dir, exist_ok=True)
    report_dir = os.path.dirname(report_out)
    if report_dir: os.makedirs(report_dir, exist_ok=True)
    existing = set(os.listdir(lib_dir))
    written_this_run = set()  # RC2: PDFs written this run; never clobber them
    out_rows = []
    n_dl = n_skip = n_no_pmc = n_fail = 0
    n_mismatch = 0
    n_sidecar = n_sidecar_skip = n_sidecar_na = 0

    for r in rows:
        doi = r["doi"].strip().lower()
        fn  = safe_filename(r["filename"])
        dest = os.path.join(lib_dir, fn)
        rec = {"doi": doi, "filename": fn, "pmcid": "", "downloaded": False,
               "skipped": False, "winning_source": "", "attempts": "", "error": "",
               "sidecar": False, "sidecar_status": ""}

        # RC2: skip only when the on-disk file is THIS doi (or carries no DOI to
        # contradict it); a same-name file for a DIFFERENT doi is a collision.
        if fn in existing and ((not _doi_of_existing(dest))
                                or _doi_of_existing(dest) == lit_util.normalize_doi(doi)):
            n_skip += 1
            # RC2 (sweep dedupe): ALREADY_EXISTS must NOT report downloaded=True —
            # use a distinct `skipped` flag so sweep doesn't treat a pre-existing
            # file as a fresh download and poison its dedupe tally.
            rec["skipped"] = True; rec["winning_source"] = "ALREADY_EXISTS"
            out_rows.append(rec)
            print(f"  SKIP {fn[:75]}")
            continue

        pmcid = doi2pmcid.get(doi)
        if not pmcid:
            n_no_pmc += 1
            rec["error"] = "NO_PMCID"
            out_rows.append(rec)
            print(f"  --   {doi[:55]:<55} no PMCID")
            continue
        rec["pmcid"] = pmcid

        if args.dry_run:
            out_rows.append(rec)
            print(f"  DRY  {pmcid:<12} -> {fn[:60]}")
            continue

        # RC2: pick a collision-safe destination (never clobber an existing PDF for a
        # different DOI, nor one written earlier in this run). Recompute fn so the
        # sidecar stem tracks the (possibly disambiguated) PDF name.
        dest, collided = resolve_dest(lib_dir, fn, doi, written_this_run)
        if collided:
            fn = os.path.basename(dest)
            rec["filename"] = fn

        ok, attempts = fetch_pmc_pdf(pmcid, dest)
        rec["attempts"] = " | ".join(f"{src}/{st}" for src,_,st,_ in attempts)
        doi_mismatch = False
        if ok:
            n_dl += 1
            rec["downloaded"] = True
            written_this_run.add(dest)
            existing.add(fn)
            winning = next((a for a in attempts if a[2] == "OK"), None)
            if winning: rec["winning_source"] = winning[0]
            print(f"  DL   {pmcid:<12} -> {fn[:60]} ({rec['winning_source']})")
            # RC3: verify the fetched bytes match the queue DOI before writing a
            # confidently-wrong .ris/sidecar.
            if pdf_doi_disagrees(dest, doi):
                doi_mismatch = True
                n_mismatch += 1
                rec["error"] = f"DOI_MISMATCH:pdf_doi={doi_from_pdf_bytes(dest)}"
                print(f"       DOI_MISMATCH: pdf DOI != queue DOI ({doi}); skipping .ris/sidecar")
            elif not args.no_write_ris:
                ris_status, _ = _R.emit_ris_for_pdf(doi, dest)
                print(f"       ris: {ris_status}")
        else:
            n_fail += 1
            rec["error"] = attempts[-1][2] if attempts else "no candidates"
            print(f"  FAIL {pmcid:<12} -> {fn[:55]} ({rec['error']})")

        # Sidecar: try regardless of PDF outcome (gated PDFs sometimes still have JATS),
        # UNLESS the fetched PDF's own DOI contradicted the queue DOI (RC3) — in that case
        # the PMCID provenance for THIS file is suspect, so don't attach a sidecar to it.
        if not args.no_sidecar and not doi_mismatch:
            sidecar_path = os.path.join(lib_dir, fn[:-4] + ".fulltext.json")
            if os.path.exists(sidecar_path):
                n_sidecar_skip += 1
                rec["sidecar"] = True; rec["sidecar_status"] = "EXISTS"
            else:
                ok_s, st_s = fetch_fulltext_sidecar(pmcid, sidecar_path)
                rec["sidecar"] = ok_s; rec["sidecar_status"] = st_s
                if ok_s:
                    n_sidecar += 1
                    print(f"       sidecar OK")
                elif st_s == "NOT_AVAILABLE":
                    n_sidecar_na += 1
                else:
                    print(f"       sidecar fail: {st_s}")
                time.sleep(0.4)

        out_rows.append(rec)
        time.sleep(0.8)

    # RC4: build CSV in memory, write atomically. `skipped` is distinct from `downloaded`
    # so sweep's dedupe treats ALREADY_EXISTS as a non-fetch.
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=["doi","filename","pmcid","downloaded","skipped",
                                          "winning_source","attempts","error",
                                          "sidecar","sidecar_status"],
                       extrasaction="ignore", lineterminator="\n")
    w.writeheader(); w.writerows(out_rows)
    lit_util.atomic_write_text(report_out, buf.getvalue())

    print(f"\n=== Summary ===")
    print(f"  Total candidates:    {len(rows)}")
    print(f"  Skipped (existing):  {n_skip}")
    print(f"  No PMCID found:      {n_no_pmc}")
    print(f"  PDFs DOWNLOADED:     {n_dl}")
    print(f"  PDFs failed:         {n_fail}")
    print(f"  DOI mismatch:        {n_mismatch} (PDF DOI disagreed; .ris/sidecar skipped)")
    if not args.no_sidecar:
        print(f"  Sidecars NEW:        {n_sidecar}")
        print(f"  Sidecars existed:    {n_sidecar_skip}")
        print(f"  Sidecars unavailable:{n_sidecar_na}")
    print(f"\nReport: {report_out}")


if __name__ == "__main__":
    main()
