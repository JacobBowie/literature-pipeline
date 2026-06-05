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
import os, sys, io, json, re, argparse, subprocess, shutil

try:
    if getattr(sys.stdout, "encoding", "").lower() != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, OSError):
    pass
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pdf_text_clean import clean_pdf_text
from jats_to_text import parse_jats
import lit_util

PDFTOTEXT = shutil.which("pdftotext")

# pdfminer.six is the primary extractor as of 2026-05-22 (per the head-to-head
# shootout under `eval_pdf_abstract_heuristic_2026-05-22/research_permissive_extractors/`).
# License: MIT — clean for downstream MCP distribution. Slower than pdftotext
# (~1.6 s vs 0.25 s per PDF) but produces materially better output on multi-column
# papers (Frontiers median Jaccard 0.906 vs 0.179 in v2-downstream eval).
try:
    from pdfminer.high_level import extract_text as _pdfminer_extract_text
    _PDFMINER_AVAILABLE = True
except ImportError:
    _PDFMINER_AVAILABLE = False


def try_parse_jats_sibling(pdf_path):
    """If <pdf_stem>.xml exists, parse it as JATS and return (sidecar_dict, err).
    On failure, returns (None, error_str). The sidecar dict shape matches
    jats_to_text.parse_jats() output.
    """
    xml_path = pdf_path[:-4] + ".xml"
    if not os.path.isfile(xml_path):
        return None, "no_xml_sibling"
    try:
        with open(xml_path, "rb") as f:
            xml_bytes = f.read()
        parsed = parse_jats(xml_bytes)
        return parsed, None
    except Exception as e:
        return None, f"{type(e).__name__}:{str(e)[:80]}"

# High-signal text patterns suggesting equations are present in the PDF.
# Match against pdftotext output — pdftotext garbles math, but some artifacts survive.
_MATH_PATTERNS = [
    (r"\\(frac|sum|int|sqrt|alpha|beta|gamma|delta|theta|sigma|omega|mu|partial|nabla)\b", "latex_cmd"),
    (r"<mml:math|<math\s+xmlns", "mathml_inline"),
    (r"\$\$[^\$\n]{2,}\$\$|\\\[[^\]]{2,}\\\]", "tex_display_delim"),
    (r"\b[Ee]q(?:uation|n)?\.?\s*\(?\d+\)?", "equation_label"),
    (r"[∑∫∮√±∞≤≥≠≈⊕⊗∇∂Δ]", "math_unicode"),
    (r"\b[A-Za-z]\s*=\s*[-+]?\d+(\.\d+)?\s*[+\-*/×·]\s*", "inline_assignment"),
]
_MATH_RE = [(re.compile(p, re.IGNORECASE), tag) for p, tag in _MATH_PATTERNS]


def detect_math_indicators(text, pdf_path):
    """Return formula_failures dict per `2026-05-21_bug_formulas_extractor.md` Tier 1.

    The extractor itself does not parse equations from PDF text (pdftotext garbles
    math). This function records *what we know* about a PDF's math content so
    downstream callers can distinguish:
      (a) "no math present" — safe to skip
      (b) "math present but pipeline doesn't support PDF eqn extraction yet"
      (c) "JATS XML sibling exists — Tier 2 (parse MathML from XML) is viable here"
    """
    hits = {}
    for rx, tag in _MATH_RE:
        m = rx.search(text)
        if m:
            hits[tag] = m.group(0)[:60]
    jats_xml = pdf_path[:-4] + ".xml"
    has_xml_sibling = os.path.isfile(jats_xml)

    if not hits and not has_xml_sibling:
        return {"status": "skipped_no_math_indicators",
                "extractor_supports_equations": False}
    if has_xml_sibling:
        return {"status": "skipped_pdf_extractor_no_eqn_support_but_xml_sibling_available",
                "extractor_supports_equations": False,
                "jats_xml_sibling": os.path.basename(jats_xml),
                "tier2_candidate": True,
                "indicators_found": hits or None}
    return {"status": "skipped_pdf_extractor_no_eqn_support",
            "extractor_supports_equations": False,
            "indicators_found": hits}


def _load_existing_sidecar(sidecar_path):
    """Return the parsed existing sidecar dict, or None if absent/unreadable.

    Used on --refresh to merge-preserve enriched metadata (doi/title/year/authors/
    figures) that fill_missing_dois / backfill wrote, instead of clobbering it with
    a freshly re-extracted (text-only) record. See lit_util.merge_sidecar (RC5)."""
    if not os.path.exists(sidecar_path):
        return None
    try:
        with open(sidecar_path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


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


def extract_with_pdfminer(pdf_path):
    """Returns (text, status). MIT-licensed pure Python; primary extractor."""
    if not _PDFMINER_AVAILABLE:
        return "", "PDFMINER_NOT_INSTALLED"
    try:
        text = _pdfminer_extract_text(pdf_path) or ""
        return text, "OK"
    except Exception as e:
        return "", f"PDFMINER_ERROR_{type(e).__name__}:{str(e)[:80]}"


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
    """Returns (text, extractor, status).

    Order chosen per 2026-05-22 head-to-head shootout (eval directory):
      1. pdfminer.six (MIT) — best abstract-recoverability on multi-column
         papers (median Jaccard 0.906 vs 0.179 for pdftotext on Frontiers).
      2. pdftotext (poppler) — fast fallback; handles edge cases pdfminer
         chokes on (unusual font encodings, encrypted PDFs).
      3. pdfplumber — pure-Python last resort.

    All three are subprocess-free for pdfminer + pdfplumber and minimal-overhead
    for pdftotext.
    """
    text, st = extract_with_pdfminer(pdf_path)
    if text.strip():
        return text, "pdfminer.six", st
    text2, st2 = extract_with_pdftotext(pdf_path)
    if text2.strip():
        return text2, "pdftotext", st2
    text3, st3 = extract_with_pdfplumber(pdf_path)
    if text3.strip():
        return text3, "pdfplumber", st3
    return "", "none", f"all_failed: pdfminer={st} pdftotext={st2} pdfplumber={st3}"


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

    n_dl = n_skip = n_fail = n_jats = 0
    for fn in pdfs:
        sidecar = os.path.join(lib, fn[:-4] + ".fulltext.json")
        if os.path.exists(sidecar) and not args.refresh:
            n_skip += 1; continue
        if args.limit and n_dl >= args.limit:
            print(f"  (limit {args.limit} reached)"); break
        pdf_path = os.path.join(lib, fn)
        if args.dry_run:
            print(f"  DRY  {fn[:70]}"); continue

        # Tier 1: JATS XML sibling wins when present (carries formulas, structured
        # sections, abstract — none of which pdftotext gives us). Fall through to
        # PDF extraction on parse error or empty text.
        jats_rec, jats_err = try_parse_jats_sibling(pdf_path)
        if jats_rec is not None and (jats_rec.get("text") or "").strip():
            jats_rec["extracted_from_pdf"] = False
            jats_rec["extractor"] = "jats_xml_sibling"
            # RC5: on --refresh, never DROP enriched metadata (doi/title/year/authors/
            # figures) a prior fill/backfill wrote that this JATS parse lacks.
            if args.refresh:
                jats_rec = lit_util.merge_sidecar(_load_existing_sidecar(sidecar), jats_rec)
            lit_util.atomic_write_json(sidecar, jats_rec)  # RC4: crash-safe
            print(f"  JATS xml_sibling {len(jats_rec.get('text') or ''):>6}c "
                  f"({jats_rec.get('n_formulas',0)} formulas) -> {fn[:50]}")
            n_dl += 1; n_jats += 1
            continue
        if jats_err and jats_err != "no_xml_sibling":
            print(f"  WARN JATS parse failed ({jats_err}) for {fn[:55]} — falling back to PDF",
                  file=sys.stderr)

        text, extractor, status = extract(pdf_path)
        if not text.strip():
            print(f"  FAIL {fn[:65]} ({status})", file=sys.stderr)
            n_fail += 1; continue
        text = clean_pdf_text(text)
        rec = _empty_sidecar()
        rec["text"] = text
        rec["extractor"] = extractor
        rec["formula_failures"] = detect_math_indicators(text, pdf_path)
        # RC5: on --refresh, _empty_sidecar() has blank doi/title/year/authors/figures;
        # merge_sidecar preserves whatever fill_missing_dois/backfill already wrote
        # (this is the confirmed PD DOI-wipe bug). Plain re-extract only touches
        # text/extractor/formula_failures. RC4: write atomically.
        if args.refresh:
            rec = lit_util.merge_sidecar(_load_existing_sidecar(sidecar), rec)
        lit_util.atomic_write_json(sidecar, rec)
        print(f"  DL   {extractor:<11} {len(text):>7}c -> {fn[:60]}")
        n_dl += 1

    print(f"\n=== Summary ===")
    print(f"  PDFs in library:        {len(pdfs)}")
    print(f"  Sidecars already there: {n_skip}")
    print(f"  Sidecars NEW:           {n_dl}  (of which JATS-XML-sibling: {n_jats})")
    print(f"  Extraction failed:      {n_fail}")


if __name__ == "__main__":
    main()
