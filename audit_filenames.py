"""Audit library filenames against CrossRef-canonical metadata.

For each PDF in --lib-dir:
  1. Extract DOI from first 5KB of text (or use sidecar's `doi` field)
  2. (Optional) Fall back to --queue-history CSVs for older scans whose DOI
     isn't machine-readable from the PDF text
  3. Look up CrossRef for canonical title/year/first-author-family
  4. Compute the canonical pipeline-convention filename
  5. If current ≠ canonical, propose a rename
  6. With --execute: rename PDF + .fulltext.json + .fig{N}.* + update image_path in sidecar

Why: filenames generated from heuristic parsing of source filenames sometimes
include co-author surnames in the title slug, ALL-CAPS first authors, or simply
suboptimal title-word choices. CrossRef metadata is canonical and consistent.

Usage:
  python audit_filenames.py --lib-dir DIR              # dry-run report only
  python audit_filenames.py --lib-dir DIR --execute    # apply renames
  python audit_filenames.py --lib-dir DIR --only-prefix 197  # subset filter

  # Use queue-history CSVs to recover DOIs for older scans without machine-readable DOI text:
  python audit_filenames.py --lib-dir DIR \
      --queue-history "/c/Users/jab18015/Projects/thermalphys/lit_pull_queue.*.processed.csv"
  # Multiple globs comma-separated:
  python audit_filenames.py --lib-dir DIR \
      --queue-history "<proj>/lit_pull_queue.*.processed.csv,<proj2>/lit_pull_queue.*.processed.csv"
"""
import os, sys, io, re, json, csv, time, glob, argparse, unicodedata
import requests
import fitz

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

EMAIL = os.environ.get("LITPIPE_EMAIL", "jacob.bowie2@gmail.com")
UA    = f"GETPAID-fnaudit/1.0 (mailto:{EMAIL})"
DOI_RE = re.compile(r"\b(10\.\d{4,9}/[^\s\)\]\>\"',]+)", re.IGNORECASE)
DOI_TRAIL = re.compile(r"[.,;:\)\]\}\>]+$")

SLUG_SKIP = {"a","an","the","of","in","on","and","to","for","at","from","with","by","as",
             "or","is","are","be","been","this","that","these","those"}


def slug(text, n=6):
    text = re.sub(r"<[^>]+>", "", text or "")
    text = re.sub(r"[^A-Za-z0-9\s\-]", " ", text)
    words = [w for w in text.split() if w.lower() not in SLUG_SKIP][:n]
    return "".join(re.sub(r"[^A-Za-z0-9\-]", "", w).capitalize() for w in words) or "Untitled"


_NON_DECOMPOSABLE = str.maketrans({
    "ø":"o","Ø":"O","æ":"ae","Æ":"Ae","ß":"ss","þ":"th","Þ":"Th",
    "ł":"l","Ł":"L","đ":"d","Đ":"D","œ":"oe","Œ":"Oe",
    "ı":"i","İ":"I",
})


def safe_ascii(s):
    """Normalize Unicode → portable ASCII. Handles non-decomposable specials
    (ø→o, æ→ae, ß→ss, ł→l) before NFKD strips combining marks.
    Lüthi→Luthi, Périard→Periard, Mølmen→Molmen, Müller-García→Muller-Garcia."""
    if not s: return ""
    s = s.translate(_NON_DECOMPOSABLE)
    nfkd = unicodedata.normalize("NFKD", s)
    no_combining = "".join(c for c in nfkd if not unicodedata.combining(c))
    return no_combining.encode("ascii", "ignore").decode("ascii")


def extract_doi_from_pdf(pdf_path, max_chars=5000):
    try:
        doc = fitz.open(pdf_path)
        text = ""
        for p in doc:
            text += p.get_text()
            if len(text) >= max_chars: break
        doc.close()
    except (OSError, RuntimeError, ValueError):
        return ""
    for m in DOI_RE.finditer(text[:max_chars]):
        d = DOI_TRAIL.sub("", m.group(1)).rstrip(".")
        if "/" in d and len(d) > 7: return d.lower()
    return ""


def doi_from_sidecar(sidecar_path):
    if not os.path.exists(sidecar_path): return ""
    try:
        with open(sidecar_path, encoding="utf-8") as fh:
            d = json.load(fh)
        return (d.get("doi") or "").lower()
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return ""


def load_queue_history(globs_arg):
    """Build {pipeline-canonical-filename -> doi} from queue-history CSVs.

    Each processed-queue CSV has schema: doi,title,authors,year,destination,notes.
    For each row, derive the filename the pipeline would have written and map it
    back to the row's DOI. This recovers the DOI for older scans whose PDF text
    doesn't expose a machine-readable DOI.
    """
    from unpaywall_fetch_v2 import build_filename
    paths = []
    for g in (globs_arg or "").split(","):
        g = g.strip()
        if not g: continue
        paths.extend(glob.glob(g))
    mapping = {}
    for p in paths:
        try:
            with open(p, encoding="utf-8") as fh:
                for r in csv.DictReader(fh):
                    doi = (r.get("doi") or "").strip().lower()
                    if not doi: continue
                    fn = build_filename(r.get("year", ""),
                                        r.get("authors", ""),
                                        r.get("title", ""))
                    if fn and fn not in mapping:
                        mapping[fn] = doi
        except (OSError, csv.Error, UnicodeDecodeError) as e:
            print(f"  queue-history read error ({p}): {e}", file=sys.stderr)
    return mapping, paths


def crossref(doi):
    try:
        r = requests.get(f"https://api.crossref.org/works/{doi}", timeout=15,
                          headers={"User-Agent": UA})
        if r.status_code != 200: return None
        m = r.json()["message"]
        title = (m.get("title") or [""])[0]
        year = ""
        for k in ("published-print","published-online","issued"):
            if k in m and m[k].get("date-parts"):
                year = str(m[k]["date-parts"][0][0]); break
        authors = m.get("author") or []
        first = authors[0] if authors else {}
        fam = (first.get("family") or "").strip()
        return {"title": title, "year": year, "lastname": fam}
    except (requests.RequestException, ValueError, KeyError):
        return None


def canonical_filename(year, lastname, title):
    yr = year if year and re.match(r"^\d{4}$", str(year)) else "Unknown"
    last = safe_ascii(re.sub(r"[^\w\-]", "", lastname or "")) or "Unknown"
    sl = safe_ascii(slug(title or ""))
    return f"{yr}_{last}_{sl}.pdf"


def cascade_rename(lib_dir, old_pdf, new_pdf):
    """Rename PDF + .fulltext.json + .ris + .fig{N}.*; update image_path in sidecar."""
    old_stem = old_pdf[:-4]
    new_stem = new_pdf[:-4]
    renamed = []
    # PDF
    os.rename(os.path.join(lib_dir, old_pdf), os.path.join(lib_dir, new_pdf))
    renamed.append(("pdf", old_pdf, new_pdf))
    # Sidecar (.fulltext.json)
    old_sc = old_stem + ".fulltext.json"
    new_sc = new_stem + ".fulltext.json"
    if os.path.exists(os.path.join(lib_dir, old_sc)):
        os.rename(os.path.join(lib_dir, old_sc), os.path.join(lib_dir, new_sc))
        # Update image_path inside sidecar
        try:
            with open(os.path.join(lib_dir, new_sc), encoding="utf-8") as fh:
                d = json.load(fh)
            for fig in d.get("figures", []):
                ip = fig.get("image_path", "")
                if ip and ip.startswith(old_stem):
                    fig["image_path"] = ip.replace(old_stem, new_stem, 1)
            with open(os.path.join(lib_dir, new_sc), "w", encoding="utf-8") as f:
                json.dump(d, f, indent=2, ensure_ascii=False)
        except (OSError, json.JSONDecodeError, UnicodeDecodeError) as e:
            print(f"      (sidecar update warning: {e})", file=sys.stderr)
        renamed.append(("sidecar", old_sc, new_sc))
    # RIS sidecar (.ris)
    old_ris = old_stem + ".ris"
    new_ris = new_stem + ".ris"
    if os.path.exists(os.path.join(lib_dir, old_ris)):
        os.rename(os.path.join(lib_dir, old_ris), os.path.join(lib_dir, new_ris))
        renamed.append(("ris", old_ris, new_ris))
    # Figure images
    for f in os.listdir(lib_dir):
        if f.startswith(old_stem + ".fig"):
            new_f = new_stem + f[len(old_stem):]
            os.rename(os.path.join(lib_dir, f), os.path.join(lib_dir, new_f))
            renamed.append(("figure", f, new_f))
    return renamed


def main():
    ap = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Queue-history fallback (for older scans without machine-readable DOI text):
  python audit_filenames.py --lib-dir DIR \\
      --queue-history "<proj>/lit_pull_queue.*.processed.csv"
  # Multiple globs comma-separated.""")
    ap.add_argument("--lib-dir", required=True)
    ap.add_argument("--execute", action="store_true",
                     help="Apply renames. Without this, dry-run only.")
    ap.add_argument("--only-prefix", default=None,
                     help="Only process PDFs whose filename starts with this")
    ap.add_argument("--queue-history", default=None,
                     help="Glob (or comma-separated globs) of processed queue CSVs "
                          "(<project>/lit_pull_queue.*.processed.csv) consulted as a "
                          "DOI fallback when neither sidecar nor PDF text yields a DOI.")
    ap.add_argument("--report", default=None)
    args = ap.parse_args()

    lib = os.path.abspath(args.lib_dir)
    pdfs = sorted(f for f in os.listdir(lib) if f.endswith(".pdf"))
    if args.only_prefix:
        pdfs = [p for p in pdfs if p.startswith(args.only_prefix)]

    print(f"Library: {lib}")
    print(f"PDFs to audit: {len(pdfs)}")
    print(f"Mode: {'EXECUTE' if args.execute else 'DRY RUN'}")

    qh_map = {}
    if args.queue_history:
        qh_map, qh_paths = load_queue_history(args.queue_history)
        print(f"Queue history: {len(qh_paths)} CSV(s), {len(qh_map)} filename→DOI entries")
    print()

    rows = []
    n_match = n_propose = n_no_doi = n_no_cr = n_renamed = 0
    n_qh_used = 0
    seen_canonical = set(p for p in pdfs)

    for fn in pdfs:
        # Get DOI: prefer sidecar over PDF text (sidecar's was looked up cleanly).
        # Fall back to queue-history map if neither yields a DOI.
        sidecar_path = os.path.join(lib, fn[:-4] + ".fulltext.json")
        doi = doi_from_sidecar(sidecar_path) or extract_doi_from_pdf(os.path.join(lib, fn))
        doi_source = "sidecar/pdf" if doi else ""
        if not doi and qh_map:
            qh_doi = qh_map.get(fn)
            if qh_doi:
                doi = qh_doi
                doi_source = "queue-history"
                n_qh_used += 1
                print(f"  QH-DOI   {fn[:65]:<65} <- {doi} (queue-history-sourced DOI)")
        if not doi:
            n_no_doi += 1
            rows.append({"current": fn, "proposed": "", "doi": "",
                          "status": "NO_DOI"})
            continue
        cr = crossref(doi)
        time.sleep(0.4)  # CrossRef polite pool
        if not cr or not cr["lastname"] or not cr["year"]:
            n_no_cr += 1
            rows.append({"current": fn, "proposed": "", "doi": doi,
                          "status": "NO_CROSSREF" + (
                              "_QH" if doi_source == "queue-history" else "")})
            continue
        proposed = canonical_filename(cr["year"], cr["lastname"], cr["title"])
        qh_tag = "_QH" if doi_source == "queue-history" else ""
        if proposed == fn:
            n_match += 1
            rows.append({"current": fn, "proposed": proposed, "doi": doi,
                          "status": "ALREADY_CANONICAL" + qh_tag})
            continue
        if proposed in seen_canonical:
            rows.append({"current": fn, "proposed": proposed, "doi": doi,
                          "status": "WOULD_COLLIDE" + qh_tag})
            print(f"  COLLIDE  {fn[:65]:<65} -> {proposed} (already in lib)")
            continue
        # Safety filter: if CrossRef says a different year, the DOI might map to
        # a different paper than the file we have. Skip and flag for manual review.
        cur_year_m = re.match(r"^(\d{4})_", fn)
        cur_year = cur_year_m.group(1) if cur_year_m else ""
        if cur_year and cur_year != cr["year"]:
            rows.append({"current": fn, "proposed": proposed, "doi": doi,
                          "status": f"SKIP_YEAR_DIFF_{cur_year}_vs_{cr['year']}" + qh_tag})
            print(f"  YEAR-DIFF {fn[:60]:<60} cur={cur_year} cr={cr['year']} ({proposed[:50]})")
            continue
        n_propose += 1
        if args.execute:
            try:
                cascade_rename(lib, fn, proposed)
                seen_canonical.discard(fn); seen_canonical.add(proposed)
                rows.append({"current": fn, "proposed": proposed, "doi": doi,
                              "status": "RENAMED" + qh_tag})
                n_renamed += 1
                tag = " [QH]" if qh_tag else ""
                print(f"  RENAMED{tag}  {fn[:65]:<65} -> {proposed}")
            except OSError as e:
                rows.append({"current": fn, "proposed": proposed, "doi": doi,
                              "status": f"RENAME_ERROR_{e}" + qh_tag})
                print(f"  ERR      {fn[:65]:<65} ({e})", file=sys.stderr)
        else:
            rows.append({"current": fn, "proposed": proposed, "doi": doi,
                          "status": "WOULD_RENAME" + qh_tag})
            tag = " [QH]" if qh_tag else ""
            print(f"  PROPOSE{tag}  {fn[:65]:<65} -> {proposed}")

    # Write report
    report_path = args.report or os.path.join(lib, "_filename_audit_report.csv")
    with open(report_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["current","proposed","doi","status"])
        w.writeheader(); w.writerows(rows)

    print(f"\n=== Summary ===")
    print(f"  Total PDFs:           {len(pdfs)}")
    print(f"  Already canonical:    {n_match}")
    print(f"  {'Renamed' if args.execute else 'Would rename':<22}{n_renamed if args.execute else n_propose}")
    print(f"  No DOI extractable:   {n_no_doi}")
    print(f"  No CrossRef record:   {n_no_cr}")
    if args.queue_history:
        print(f"  DOI from queue-hist:  {n_qh_used}")
    print(f"\nReport: {report_path}")


if __name__ == "__main__":
    main()
