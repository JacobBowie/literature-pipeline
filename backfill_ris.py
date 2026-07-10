"""Backfill .ris sidecars next to existing PDFs in a literature library.

For each <stem>.pdf in --lib-dir:
  1. If <stem>.ris exists and not --overwrite: skip
  2. Get DOI:
       a. From <stem>.fulltext.json sidecar (if present)
       b. Else extract from first 5KB of PDF text
  3. CrossRef /works/{doi} → canonical metadata
  4. Build RIS, write to <stem>.ris

Default is dry-run. Pass --commit to actually write.

Usage:
  # Default: dry-run on Physiological_Data library
  python backfill_ris.py

  # Custom library
  python backfill_ris.py --lib-dir /c/Users/<user>/Projects/getpaid/references/literature

  # Commit
  python backfill_ris.py --commit

  # Overwrite existing .ris files
  python backfill_ris.py --commit --overwrite
"""
import os, sys, io, re, json, time, csv, argparse
from pathlib import Path

try:
    if getattr(sys.stdout, "encoding", "").lower() != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, OSError):
    pass

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ris_emit as R
import lit_util  # companion_path (dot-safe sidecar naming)

EMAIL = os.environ.get("LITPIPE_EMAIL", "JacobBowie@users.noreply.github.com")
DEFAULT_LIB = os.path.expanduser("~/Projects/Physiological_Data/docs/literature")


def doi_from_sidecar(sidecar_path: Path) -> str:
    if not sidecar_path.exists(): return ""
    try:
        with open(sidecar_path, encoding="utf-8") as f:
            d = json.load(f)
        return (d.get("doi") or "").lower()
    except (OSError, ValueError):
        return ""


def doi_from_pdf(pdf_path: Path, max_chars=5000) -> str:
    """Extract DOI from first ~5KB of PDF text. Lazy-imports fitz."""
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
    except Exception:
        return ""
    return R.extract_doi_from_text(text[:max_chars])


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    ap.add_argument("--lib-dir",  default=DEFAULT_LIB,
                    help=f"PDF library directory (default: {DEFAULT_LIB}).")
    ap.add_argument("--commit",   action="store_true",
                    help="Actually write .ris files. Default is dry-run.")
    ap.add_argument("--overwrite", action="store_true",
                    help="Overwrite existing .ris files.")
    ap.add_argument("--limit",    type=int, default=0,
                    help="Process first N PDFs only (testing).")
    ap.add_argument("--sleep",    type=float, default=0.6,
                    help="Seconds between CrossRef calls (politeness).")
    args = ap.parse_args()

    lib = Path(args.lib_dir)
    if not lib.exists():
        print(f"[ERR] lib-dir not found: {lib}", file=sys.stderr); sys.exit(2)

    pdfs = sorted([p for p in lib.iterdir() if p.is_file() and p.suffix.lower() == ".pdf"])
    if args.limit: pdfs = pdfs[:args.limit]

    mode = "COMMIT" if args.commit else "DRY-RUN"
    print(f"== ris backfill [{mode}] ==")
    print(f"  lib-dir:  {lib}")
    print(f"  PDFs:     {len(pdfs)}")
    print()

    rows = []
    stats = {"skip_existing": 0, "wrote": 0, "doi_sidecar": 0, "doi_pdf": 0,
             "no_doi": 0, "crossref_fail": 0}

    for i, pdf in enumerate(pdfs, 1):
        ris_out = lit_util.companion_path(pdf, ".ris")

        if ris_out.exists() and not args.overwrite:
            stats["skip_existing"] += 1
            rows.append({"pdf": pdf.name, "doi": "", "status": "EXISTS_SKIP", "out": ris_out.name})
            print(f"  [{i:3d}/{len(pdfs)}] {pdf.name[:60]:<60} → EXISTS_SKIP")
            continue

        # Try sidecar first
        side = lit_util.companion_path(pdf, ".fulltext.json")
        doi = doi_from_sidecar(side)
        doi_src = "sidecar" if doi else ""
        if doi: stats["doi_sidecar"] += 1

        # Fallback: extract from PDF text
        if not doi:
            doi = doi_from_pdf(pdf)
            if doi:
                doi_src = "pdf"; stats["doi_pdf"] += 1

        if not doi:
            stats["no_doi"] += 1
            rows.append({"pdf": pdf.name, "doi": "", "status": "NO_DOI", "out": ""})
            print(f"  [{i:3d}/{len(pdfs)}] {pdf.name[:60]:<60} → NO_DOI")
            continue

        msg = R.crossref_by_doi(doi)
        time.sleep(args.sleep)
        if not msg:
            stats["crossref_fail"] += 1
            rows.append({"pdf": pdf.name, "doi": doi, "status": "CROSSREF_FAIL", "out": ""})
            print(f"  [{i:3d}/{len(pdfs)}] {pdf.name[:60]:<60} → CROSSREF_FAIL ({doi})")
            continue

        meta = R.crossref_meta(msg)
        ris_text = R.build_ris(meta)

        if args.commit:
            R.write_ris(str(ris_out), ris_text, overwrite=True)
            stats["wrote"] += 1
            status = "WROTE"
        else:
            status = f"DRY:{doi_src}"

        rows.append({"pdf": pdf.name, "doi": doi, "status": status, "out": ris_out.name})
        print(f"  [{i:3d}/{len(pdfs)}] {pdf.name[:60]:<60} → {status:<14} ({doi_src})")

    # Report CSV
    if args.commit:
        rep = lib / "_ris_backfill_report.csv"
        with open(rep, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["pdf","doi","status","out"])
            w.writeheader(); w.writerows(rows)
        print(f"\n  report: {rep}")

    print("\n== summary ==")
    for k, v in stats.items(): print(f"  {k:<18} {v}")


if __name__ == "__main__":
    main()
