"""Extract plain-text full text from PDFs into .fulltext.json sidecars.

Complements backfill_fulltext.py: that script fetches JATS XML from Europe PMC
for papers with a PMCID. Many PDFs in project libraries have no PMC mirror
(non-OA, Unpaywall-only, older papers). For those, extract text directly
from the PDF so the same downstream indexer (index_portfolio.py) sees a
sidecar with a populated `text` field.

Strategy:
  1. Primary: poppler `pdftotext` (fast, robust, preserves layout reasonably).
  2. Fallback: pdfplumber (pure-Python; slower but handles some PDFs poppler chokes on).

Schema parity:
  The sidecar matches the JATS schema written by jats_to_text.parse_jats(),
  with JATS-specific fields (pmcid, pmid, authors, sections, figures, tables,
  formulas, ...) left empty. Only `text`, `abstract` (empty — not parseable
  from PDF), and an `extracted_from_pdf: true` marker are populated. The
  paired .ris sidecar carries bibliographic metadata; this script does not
  duplicate it.

Idempotent — skips PDFs that already have a *.fulltext.json sidecar.

Usage:
  python extract_pdf_fulltext.py --lib-dir ../Physiological_Data/docs/literature
  python extract_pdf_fulltext.py --lib-dir ... --limit 5            # smoke test
  python extract_pdf_fulltext.py --lib-dir ... --refresh            # re-extract
"""
import os, sys, io, json, argparse, subprocess, shutil

try:
    if getattr(sys.stdout, "encoding", "").lower() != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, OSError):
    pass
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pdf_text_clean import clean_pdf_text

PDFTOTEXT = shutil.which("pdftotext")


def _empty_sidecar():
    """Shape matches jats_to_text.parse_jats() output; JATS-only fields empty."""
    return {
        "pmcid": "", "pmid": "", "doi": "",
        "title": "", "subtitle": "", "year": "", "journal": "",
        "authors": [],
        "abstract": "",
        "sections": [],
        "figures": [],
        "tables": [],
        "formulas": [],
        "n_formulas": 0,
        "formula_failures": {},
        "text": "",
        "extracted_from_pdf": True,
        "extractor": "",
    }


def extract_with_pdftotext(pdf_path):
    """Returns (text, status). pdftotext writes to stdout with -."""
    if not PDFTOTEXT:
        return "", "PDFTOTEXT_NOT_FOUND"
    try:
        r = subprocess.run([PDFTOTEXT, "-layout", "-nopgbrk", "-enc", "UTF-8",
                            pdf_path, "-"],
                           capture_output=True, timeout=120)
        if r.returncode != 0:
            return "", f"PDFTOTEXT_RC{r.returncode}:{r.stderr[:120].decode('utf-8','replace')}"
        return r.stdout.decode("utf-8", "replace"), "OK"
    except subprocess.TimeoutExpired:
        return "", "PDFTOTEXT_TIMEOUT"
    except Exception as e:
        return "", f"PDFTOTEXT_ERROR_{type(e).__name__}:{str(e)[:80]}"


def extract_with_pdfplumber(pdf_path):
    try:
        import pdfplumber
    except ImportError:
        return "", "PDFPLUMBER_NOT_INSTALLED"
    try:
        parts = []
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                t = page.extract_text() or ""
                parts.append(t)
        return "\n".join(parts), "OK"
    except Exception as e:
        return "", f"PDFPLUMBER_ERROR_{type(e).__name__}:{str(e)[:80]}"


def extract(pdf_path):
    """Returns (text, extractor, status)."""
    text, st = extract_with_pdftotext(pdf_path)
    if text.strip():
        return text, "pdftotext", st
    text2, st2 = extract_with_pdfplumber(pdf_path)
    if text2.strip():
        return text2, "pdfplumber", st2
    return "", "none", f"both_failed: pdftotext={st} pdfplumber={st2}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lib-dir", required=True)
    ap.add_argument("--limit", type=int, default=0,
                    help="Process at most N new PDFs (0 = no limit).")
    ap.add_argument("--refresh", action="store_true",
                    help="Re-extract and overwrite existing sidecars.")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    lib = os.path.abspath(args.lib_dir)
    if not os.path.isdir(lib):
        print(f"ERR: lib-dir not a directory: {lib}", file=sys.stderr); sys.exit(1)
    pdfs = sorted(f for f in os.listdir(lib) if f.endswith(".pdf"))
    print(f"Library: {lib}\n  {len(pdfs)} PDFs found  (pdftotext={'yes' if PDFTOTEXT else 'no'})")

    n_dl = n_skip = n_fail = 0
    for fn in pdfs:
        sidecar = os.path.join(lib, fn[:-4] + ".fulltext.json")
        if os.path.exists(sidecar) and not args.refresh:
            n_skip += 1; continue
        if args.limit and n_dl >= args.limit:
            print(f"  (limit {args.limit} reached)"); break
        pdf_path = os.path.join(lib, fn)
        if args.dry_run:
            print(f"  DRY  {fn[:70]}"); continue
        text, extractor, status = extract(pdf_path)
        if not text.strip():
            print(f"  FAIL {fn[:65]} ({status})", file=sys.stderr)
            n_fail += 1; continue
        text = clean_pdf_text(text)
        rec = _empty_sidecar()
        rec["text"] = text
        rec["extractor"] = extractor
        with open(sidecar, "w", encoding="utf-8") as f:
            json.dump(rec, f, indent=2, ensure_ascii=False)
        print(f"  DL   {extractor:<11} {len(text):>7}c -> {fn[:60]}")
        n_dl += 1

    print(f"\n=== Summary ===")
    print(f"  PDFs in library:        {len(pdfs)}")
    print(f"  Sidecars already there: {n_skip}")
    print(f"  Sidecars NEW:           {n_dl}")
    print(f"  Extraction failed:      {n_fail}")


if __name__ == "__main__":
    main()
