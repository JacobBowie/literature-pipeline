"""Extract tables from PDF library using pdfplumber.

Walks references/literature/, extracts every detected table per page, and
writes them as CSVs into data/prior_art/tables/{filename_stem}/page_{N}_table_{i}.csv.

Why pdfplumber and not pymupdf:
- pymupdf gives flowing text, columns lost. pdfplumber detects ruling lines and
  white-space gridding to recover row/column structure.
- For papers without JATS sidecars (gated + non-PMC = ~70/96 in getpaid), this
  is the only structured-table source available.

Output:
  data/prior_art/tables/{stem}/page_{N}_table_{i}.csv  — one CSV per detected table
  data/prior_art/tables/_extraction_report.csv         — per-PDF summary

Usage:
  python tools/extract_tables.py [--lib-dir DIR] [--limit N]
"""
import os, sys, io, csv, argparse
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import pdfplumber

DEFAULT_LIB = "references/literature"
OUT_DIR     = "data/prior_art/tables"


def extract_pdf_tables(pdf_path, out_subdir, strategy="lines"):
    """Extract tables. Returns (n_tables, n_pages_with_tables, errors).

    strategy:
      'lines'     (default) — pdfplumber's default ruling-line detection. Reliable but
                  misses lineless/whitespace-only tables. Use this for production runs.
      'text'      — Use word positions to infer column grid. Catches lineless tables BUT
                  produces noisy false positives on multi-column prose pages (esp. BMC /
                  Springer / open-access journal layouts where page text gets mis-segmented).
                  For those papers, prefer the JATS sidecar's tables[] field if available,
                  otherwise extract by hand.
    """
    if strategy == "text":
        settings = {"vertical_strategy": "text", "horizontal_strategy": "text",
                    "min_words_vertical": 3, "min_words_horizontal": 1}
    else:
        settings = {}

    n_tables = 0
    pages_with_tables = 0
    errors = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page_idx, page in enumerate(pdf.pages, start=1):
                try:
                    tables = page.extract_tables(settings) or []
                except Exception as e:
                    errors.append(f"page {page_idx}: {str(e)[:60]}")
                    continue
                # Sanity filter: real tables have ≥3 non-empty rows, ≥2 columns,
                # and average cell length < 80 chars (longer = prose paragraph)
                def is_real_table(t):
                    if not t: return False
                    rows = [r for r in t if any(c and str(c).strip() for c in r)]
                    if len(rows) < 3: return False
                    n_cols = max(sum(1 for c in r if c and str(c).strip()) for r in rows)
                    if n_cols < 2: return False
                    cells = [str(c).strip() for r in rows for c in r if c and str(c).strip()]
                    if not cells: return False
                    return (sum(len(c) for c in cells) / len(cells)) < 80
                tables = [t for t in tables if is_real_table(t)]
                if not tables: continue
                pages_with_tables += 1
                os.makedirs(out_subdir, exist_ok=True)
                for t_idx, table in enumerate(tables, start=1):
                    csv_path = os.path.join(out_subdir,
                                              f"page_{page_idx:03d}_table_{t_idx}.csv")
                    with open(csv_path, "w", encoding="utf-8", newline="") as f:
                        w = csv.writer(f)
                        for row in table:
                            w.writerow(["" if c is None else str(c).strip() for c in row])
                    n_tables += 1
    except Exception as e:
        errors.append(f"open: {str(e)[:80]}")
    return n_tables, pages_with_tables, errors


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lib-dir", default=DEFAULT_LIB)
    ap.add_argument("--out-dir", default=OUT_DIR)
    ap.add_argument("--limit", type=int, default=0,
                     help="Process only N PDFs (0 = all)")
    ap.add_argument("--only-name", action="append", default=[],
                     help="Only process PDFs whose filename contains this substring "
                          "(use one --only-name flag per substring)")
    ap.add_argument("--strategy", choices=["lines","text"], default="lines",
                     help="lines (default, reliable) or text (lineless tables, noisy)")
    args = ap.parse_args()

    lib = os.path.abspath(args.lib_dir)
    out = os.path.abspath(args.out_dir)
    os.makedirs(out, exist_ok=True)

    pdfs = sorted(f for f in os.listdir(lib) if f.endswith(".pdf"))
    if args.only_name:
        pdfs = [p for p in pdfs if any(s in p for s in args.only_name)]
    if args.limit:
        pdfs = pdfs[:args.limit]

    print(f"Processing {len(pdfs)} PDFs from {lib}")
    print(f"Output:     {out}\n")

    report = []
    n_tot = 0; n_papers_with_tables = 0
    for fn in pdfs:
        stem = fn[:-4]
        pdf_path = os.path.join(lib, fn)
        out_sub = os.path.join(out, stem)
        n_t, n_p, errs = extract_pdf_tables(pdf_path, out_sub, strategy=args.strategy)
        n_tot += n_t
        if n_t: n_papers_with_tables += 1
        status = "OK" if not errs else f"ERR ({len(errs)})"
        print(f"  {fn[:55]:<55} tables={n_t:>3}  pages={n_p:>3}  {status}")
        report.append({"filename": fn, "n_tables": n_t,
                        "pages_with_tables": n_p, "strategy": args.strategy,
                        "errors": " | ".join(errs)[:200]})

    # report
    report_path = os.path.join(out, "_extraction_report.csv")
    with open(report_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["filename","n_tables","pages_with_tables","strategy","errors"])
        w.writeheader(); w.writerows(report)

    print(f"\n=== Summary ===")
    print(f"  PDFs processed:      {len(pdfs)}")
    print(f"  Papers with tables:  {n_papers_with_tables}")
    print(f"  Total tables:        {n_tot}")
    print(f"  Report:              {report_path}")


if __name__ == "__main__":
    main()
