"""Extract plain-text full text from PDFs into .fulltext.json sidecars.

Complements backfill_fulltext.py: that script fetches JATS XML from Europe PMC
for papers with a PMCID. Many PDFs in project libraries have no PMC mirror
(non-OA, Unpaywall-only, older papers). For those, extract text directly
from the PDF so the same downstream indexer (index_portfolio.py) sees a
sidecar with a populated `text` field.

Strategy:
  1. A JATS XML sibling (<stem>.xml) wins when it parses.
  2. pdfminer.six, then poppler `pdftotext`, then pdfplumber (above 30 MB only pdftotext).
  3. The text-validity gate (W4-C) runs on the extracted text and on PyMuPDF's per-page text.
     When the text fails, PyMuPDF's own text and then pdftotext are tried (a font pdfminer
     cannot map gives '(cid:N)' for most words). A scan with a bad OCR layer, an image-only
     file or a mojibake layer does not become a normal sidecar: it gets `needs_ocr: true`,
     `needs_ocr_reason` and `text_metrics`, with `text` empty (an existing sidecar whose text
     passes the gate is kept instead).
  4. `--ocr` (off by default) renders the pages that failed with PyMuPDF and OCRs them with
     Tesseract through pytesseract, in the `.ris` LA language plus English. The result records
     `extractor: "tesseract"`, `ocr_dpi`, `ocr_lang`, `ocr_engine_version`,
     `ocr_mean_confidence`, `ocr_pages`, and `ocr_low_confidence` below the confidence floor.

Schema parity:
  The sidecar matches the JATS schema written by jats_to_text.parse_jats(),
  with JATS-specific fields (pmcid, pmid, authors, sections, figures, tables,
  formulas, ...) left empty. Only `text`, `abstract` (empty, not parseable
  from PDF), and an `extracted_from_pdf: true` marker are populated. The
  paired .ris sidecar carries bibliographic metadata; this script does not
  duplicate it. A `<stem>.identity.json` verdict (identity, identity_score,
  identity_evidence, doc_kind) is copied into the sidecar. A DOI is never taken from
  the text into `doi`: with no valid DOI after the merge and the `.ris` seed, the
  front-matter DOI is recorded as `doi_candidate` with `doi_source: "text"`.

Which sidecars a re-extract may replace (`--refresh`):
  only one whose `extractor` is one this pipeline writes (pdfminer.six, pdftotext, pdfplumber,
  PyMuPDF) or is empty, that carries none of text_preocr, repair_note, ocr_merged, licence,
  license, and that is not JATS-sourced. Every other sidecar is merged (OCR text, a hand
  repair, a JATS parse): `--refresh` prints KEEP and leaves it byte-identical, whatever
  siblings exist. `--refresh --force` re-extracts, keeps every field of the old sidecar, and
  moves the replaced text to `text_preocr`; when the new text fails the gate and the old text
  passes, it changes nothing (KEEP (gate)).

Idempotent: skips PDFs that already have a *.fulltext.json sidecar (except a `needs_ocr`
sidecar under `--ocr`).

Exit codes: 0 when every PDF was handled (needs_ocr and KEEP included); 1 on a usage error or
a missing --lib-dir; 2, after a final `[step-summary] {json}` line, when extraction raised or
`--ocr` found no Tesseract (or an OCR run failed).

Usage:
  python extract_pdf_fulltext.py --lib-dir ../research_a/docs/literature
  python extract_pdf_fulltext.py --lib-dir ... --limit 5            # smoke test
  python extract_pdf_fulltext.py --lib-dir ... --refresh            # re-extract pipeline sidecars
  python extract_pdf_fulltext.py --lib-dir ... --refresh --force    # also merged sidecars
  python extract_pdf_fulltext.py --lib-dir ... --ocr                # OCR the needs_ocr PDFs
  python extract_pdf_fulltext.py --lib-dir ... --suspect-report out.csv   # read-only re-fetch list
"""
import argparse
import io
import json
import os
import re
import shutil
import subprocess
import sys
import unicodedata
from collections import Counter

import lit_util
lit_util.utf8_stdout()
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pdf_text_clean import clean_pdf_text  # noqa: E402
from jats_to_text import parse_jats  # noqa: E402
from litpipe import identity as _identity  # noqa: E402

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

# PyMuPDF gives the per-page text the gate reads (pdftotext runs with -nopgbrk and pdfplumber's
# pages are joined, so neither gives page boundaries) and renders the pages OCR reads.
try:
    import pymupdf
except ImportError:          # pragma: no cover - a locked dependency
    pymupdf = None

SUMMARY_MARKER = "[step-summary] "
EXIT_OK, EXIT_USAGE, EXIT_DEGRADED = 0, 1, 2

# ---------------------------------------------------------------- sidecar provenance (W4-C)
# The extractors this pipeline writes. A sidecar with another extractor (OCR text, a hand-filed
# sidecar, the JATS tier) or with a merge marker is never replaced by a plain --refresh.
PIPELINE_EXTRACTORS = ("pdfminer.six", "pdftotext", "pdfplumber", "PyMuPDF")
MERGED_MARKERS = ("text_preocr", "repair_note", "ocr_merged", "licence", "license")
# The verdict fields pmc_fetch/unpaywall write (litpipe.identity.Verdict.as_dict, plus doc_kind).
IDENTITY_FIELDS = ("identity", "identity_score", "identity_evidence", "doc_kind")
# Fields every extraction decides afresh: never carried over from the sidecar it replaces.
EXTRACTION_FIELDS = ("text", "extractor", "extracted_from_pdf", "formula_failures",
                     "needs_ocr", "needs_ocr_reason", "text_metrics",
                     "ocr_dpi", "ocr_lang", "ocr_engine_version", "ocr_mean_confidence",
                     "ocr_low_confidence", "ocr_pages", "doi_candidate", "doi_source")
# Where a front-matter DOI is read: fill_missing_dois.FRONT_MATTER_CHARS (a test locks the two).
DOI_TEXT_WINDOW = 6000

# ---------------------------------------------------------------- the text-validity gate (W4-C)
# Calibrated 2026-10-06 on the fresh text layers of the 17 OCR'd papers in the registered
# libraries (the bad side) and the PDF-extracted sidecars of the teaching library and of every
# other registered library (the healthy side, about 5,500 files); the evidence JSON records
# every value and margin. A false positive empties a good paper's text on every sweep, so each
# threshold sits clear of the healthy extreme (the most common word of a real text is at most
# 15 % of its words; only '(cid:N)' runs go higher).
MIN_UNIQUE_WORDS = 20          # distinct 3+ letter words in the whole text
MAX_TOP_WORD_SHARE = 0.3       # one word ('cid' from a broken font map) is that much of the text
MIN_WORDS_FOR_SHARE = 50       # the share rule needs a text of some length
MIN_PRINTABLE_RATIO = 0.85     # control, format, private-use and replacement characters
MIN_WORD_CHAR_RATIO = 0.10     # characters inside words over non-space characters
WATERMARK_CHARS_PER_PAGE = _identity.WATERMARK_CHARS_PER_PAGE   # suspect_file's thin-text rule
PAGE_MIN_CHARS = 40            # a page with fewer non-space characters (after boilerplate) is blank
MAX_BAD_PAGE_SHARE = 0.6       # this share of blank (image-only) pages fails the document
BOILERPLATE_SHARE = 0.8        # a line on this share of pages is a stamp or running head

_WORD = re.compile(r"[^\W\d_]{2,}")
_WORD3 = re.compile(r"[^\W\d_]{3,}")
_UNPRINTABLE_CATS = frozenset({"Cc", "Cf", "Co", "Cn", "Cs"})

# ---------------------------------------------------------------- OCR (W4-C)
OCR_DPI = 300                  # render resolution; the evidence compares 200 and 300
OCR_LOW_CONFIDENCE = 70.0      # mean word confidence below this sets ocr_low_confidence
OCR_PAGE_TIMEOUT_S = 300       # one page's Tesseract run
MIN_LANGUAGES = 3              # 2 or fewer: Tesseract fell back to its bundled eng+osd set

# .ris LA values (ISO 639-1, ISO 639-2/B, English names) -> Tesseract language codes.
_LANG_ALIASES = {
    "en": "eng", "english": "eng", "es": "spa", "spanish": "spa", "de": "deu", "ger": "deu",
    "german": "deu", "fr": "fra", "fre": "fra", "french": "fra", "it": "ita", "italian": "ita",
    "pt": "por", "portuguese": "por", "nl": "nld", "dut": "nld", "dutch": "nld", "ru": "rus",
    "russian": "rus", "pl": "pol", "polish": "pol", "cs": "ces", "cze": "ces", "czech": "ces",
    "sv": "swe", "swedish": "swe", "da": "dan", "danish": "dan", "no": "nor", "nb": "nor",
    "norwegian": "nor", "fi": "fin", "finnish": "fin", "hu": "hun", "hungarian": "hun",
    "tr": "tur", "turkish": "tur", "el": "ell", "gre": "ell", "greek": "ell", "ja": "jpn",
    "japanese": "jpn", "zh": "chi_sim", "chi": "chi_sim", "zho": "chi_sim", "chinese": "chi_sim",
    "ko": "kor", "korean": "kor", "ar": "ara", "arabic": "ara", "he": "heb", "hebrew": "heb",
    "fa": "fas", "per": "fas", "persian": "fas", "ro": "ron", "rum": "ron", "romanian": "ron",
    "uk": "ukr", "ukrainian": "ukr", "hr": "hrv", "croatian": "hrv", "sr": "srp",
    "serbian": "srp", "sk": "slk", "slo": "slk", "slovak": "slk", "sl": "slv",
    "slovenian": "slv", "ca": "cat", "catalan": "cat", "lt": "lit", "lithuanian": "lit",
    "lv": "lav", "latvian": "lav", "et": "est", "estonian": "est", "bg": "bul",
    "bulgarian": "bul", "id": "ind", "indonesian": "ind", "th": "tha", "thai": "tha",
    "vi": "vie", "vietnamese": "vie", "hi": "hin", "hindi": "hin", "la": "lat", "latin": "lat",
}


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
            d = json.load(f)
    except (OSError, ValueError):
        return None
    return d if isinstance(d, dict) else None


def _is_jats_sourced(sidecar):
    """True if this sidecar carries JATS-derived structured content that a plain PDF re-extract
    (pdftotext/pdfminer) cannot reproduce -- structured sections, parsed formulas, or an explicit
    non-PDF provenance. Used by the D2 --refresh guard so re-extraction never downgrades a JATS
    sidecar to bare PDF text. Covers sidecars written by extract (extracted_from_pdf / extractor)
    AND by pmc_fetch/backfill/recheck (raw parse_jats output: sections/formulas but no provenance
    flag)."""
    if not sidecar:
        return False
    if sidecar.get("extracted_from_pdf") is False:
        return True
    if sidecar.get("extractor") == "jats_xml_sibling":
        return True
    # sections/formulas/tables are JATS-only structure a PDF re-extract cannot reproduce; a
    # PDF-sourced sidecar has all three empty (from _empty_sidecar), so no over-protection risk.
    # n_formulas is a count: a hand-written "0" is no formula (bool("0") would say JATS).
    return (bool(sidecar.get("sections")) or lit_util.coerce_int(sidecar.get("n_formulas")) > 0
            or bool(sidecar.get("tables")))


def is_replaceable(sidecar):
    """May a --refresh replace this sidecar? Only when its extractor is one this pipeline writes
    (or empty), it carries no merge marker, and it is not JATS-sourced. Everything else is a
    merged sidecar (OCR text, a hand repair or filing, a JATS parse) and is kept."""
    if not isinstance(sidecar, dict):
        return True                  # unreadable: nothing to keep (as before W4-C)
    ex = sidecar.get("extractor")
    if ex not in (None, "") and ex not in PIPELINE_EXTRACTORS:
        return False
    if any(m in sidecar for m in MERGED_MARKERS):
        return False
    return not _is_jats_sourced(sidecar)


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


def combine(old, new, *, keep_old_text=False):
    """The record that replaces `old`: lit_util.merge_sidecar(old, new) (RC5: no enriched field is
    dropped), then every other key of `old` the result lacks, except the fields an extraction
    decides afresh (EXTRACTION_FIELDS). `text` and `extractor` come from `new`; with
    `keep_old_text` the replaced text is kept as `text_preocr`."""
    if not old:
        return new
    out = lit_util.merge_sidecar(old, new)
    for k, v in old.items():
        if k not in out and k not in EXTRACTION_FIELDS:
            out[k] = v
    if keep_old_text and str(old.get("text") or "").strip():
        out["text_preocr"] = old["text"]
    return out


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
    # Large PDFs (usually scanned/image-heavy) can hang the in-process,
    # timeout-less pdfminer AND pdfplumber indefinitely — a 47 MB scan stalled a
    # whole sweep on 2026-06-23. Above a size threshold, use only the
    # subprocess-timeout-protected pdftotext so one giant scan can't wedge the run.
    LARGE_PDF_BYTES = 30 * 1024 * 1024
    try:
        too_large = os.path.getsize(pdf_path) > LARGE_PDF_BYTES
    except OSError:
        too_large = False

    if too_large:
        text2, st2 = extract_with_pdftotext(pdf_path)
        if text2.strip():
            return text2, "pdftotext", st2
        return "", "none", f"all_failed (large, pdfminer/pdfplumber skipped): pdftotext={st2}"

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


# ---------------------------------------------------------------- per-page text (PyMuPDF)
def pdf_page_texts(pdf_path):
    """(list of per-page texts, "") from PyMuPDF, or (None, the reason) when it cannot read the
    file. PyMuPDF is C code with no known hang on large scans, so it runs above 30 MB too."""
    if pymupdf is None:
        return None, "PYMUPDF_NOT_INSTALLED"
    try:
        with pymupdf.open(pdf_path) as doc:
            return [page.get_text() for page in doc], ""
    except Exception as e:
        return None, f"PYMUPDF_ERROR_{type(e).__name__}:{str(e)[:80]}"


def extract_gated(pdf_path, pages=None):
    """(text, extractor, status, gate): extract(), cleaned and gated. When that text fails the
    gate, PyMuPDF's own text (already read for the page measures) and then pdftotext (if
    extract() did not already return it or fail on it) are tried, and the first that passes is
    used: a font pdfminer cannot map ('(cid:N)' for most words) often reads cleanly through the
    other two (all 47 such files in the calibration did). PyMuPDF goes first because
    pdftotext -layout interleaves the columns of a two-column page. Otherwise the first
    extraction and its failed gate are returned."""
    n = len(pages) if pages is not None else None
    text, extractor, status = extract(pdf_path)
    text = clean_pdf_text(text)
    gate = text_validity(text, pages, n)
    if gate["ok"]:
        return text, extractor, status, gate
    if pages:
        t2 = clean_pdf_text("\n\n".join(pages))
        if t2.strip():
            g2 = text_validity(t2, pages, n)
            if g2["ok"]:
                return t2, "PyMuPDF", "OK", g2
    if extractor == "pdfminer.six":
        t3, st3 = extract_with_pdftotext(pdf_path)
        t3 = clean_pdf_text(t3)
        if t3.strip():
            g3 = text_validity(t3, pages, n)
            if g3["ok"]:
                return t3, "pdftotext", st3, g3
    return text, extractor, status, gate


# ---------------------------------------------------------------- the gate
def boilerplate_lines(page_texts, share=BOILERPLATE_SHARE):
    """Lines printed on at least `share` of the pages (three pages or more): a proxy stamp, a
    'Downloaded from' watermark or a running head. A computed set, not a character count, so a
    long stamp on every page of a scan still reads as an empty page."""
    if not page_texts or len(page_texts) < 3:
        return set()
    counts = Counter()
    for t in page_texts:
        for line in {ln.strip() for ln in (t or "").splitlines() if ln.strip()}:
            counts[line] += 1
    need = max(2, int(len(page_texts) * share))
    return {line for line, n in counts.items() if n >= need}


def _word_char_ratio(text, nonspace):
    return (sum(len(w) for w in _WORD3.findall(text)) / nonspace) if nonspace else 0.0


def bad_pages(page_texts):
    """0-based indices of the blank pages: fewer than PAGE_MIN_CHARS non-space characters once
    boilerplate is removed (an image-only page, or one carrying only a proxy stamp). A page of
    mojibake is not counted here: PyMuPDF can garble a font that pdftotext reads cleanly (the
    calibration found such a paper), and the document measures already judge the extracted
    text itself."""
    boil = boilerplate_lines(page_texts)
    out = []
    for i, t in enumerate(page_texts or []):
        t = t or ""
        if boil:
            t = "\n".join(ln for ln in t.splitlines() if ln.strip() not in boil)
        if sum(1 for ch in t if not ch.isspace()) < PAGE_MIN_CHARS:
            out.append(i)
    return out


def text_metrics(text, n_pages=None):
    """The document-level measures the gate reads."""
    text = text or ""
    nonspace = [ch for ch in text if not ch.isspace()]
    n_ns = len(nonspace)
    words = _WORD.findall(text)
    w3 = [w.lower() for w in _WORD3.findall(text)]
    top_word, top_n = (Counter(w3).most_common(1)[0] if w3 else ("", 0))
    unprintable = sum(1 for ch in nonspace
                      if ch == "\ufffd" or unicodedata.category(ch) in _UNPRINTABLE_CATS)
    st = _identity.text_stats(text, n_pages)
    return {
        "chars": n_ns,
        "words": len(words),
        "unique_words": len(set(w3)),
        "unique_ratio": round(len(set(w3)) / len(w3), 4) if w3 else 0.0,
        "top_word": top_word[:40],
        "top_word_share": round(top_n / len(w3), 4) if w3 else 0.0,
        "printable_ratio": round(1 - unprintable / n_ns, 4) if n_ns else 0.0,
        "word_char_ratio": round(_word_char_ratio(text, n_ns), 4),
        "n_pages": n_pages,
        "chars_per_page": round(st["chars_per_page"], 1),
        "top_line_share": round(st["top_line_share"], 4),
    }


def watermark_only(text, n_pages=None):
    """litpipe.identity.suspect_file's watermark rule on this text, confirmed: the text is thin
    (under WATERMARK_CHARS_PER_PAGE non-space characters a page), or one line repeats on half the
    lines AND the text without that line is thin. The confirmation keeps a healthy paper whose
    running head ('ARTICLE IN PRESS') or dot leaders make up half its lines."""
    st = _identity.text_stats(text, n_pages)
    if not _identity.suspect_file(None, None, None, st):
        return False
    if st["chars_per_page"] < WATERMARK_CHARS_PER_PAGE:
        return True
    lines = [ln.strip() for ln in str(text or "").splitlines() if ln.strip()]
    top = Counter(lines).most_common(1)[0][0] if lines else ""
    rest = "\n".join(ln for ln in lines if ln != top)
    return _identity.text_stats(rest, n_pages)["chars_per_page"] < WATERMARK_CHARS_PER_PAGE


def text_validity(text, page_texts=None, n_pages=None):
    """The text-validity gate. {"ok", "reasons", "metrics", "bad_pages"}: document measures on
    `text` (word count, distinct words, the share of the most common word, printable ratio,
    word-character ratio, suspect_file's watermark rule) and, when PyMuPDF's per-page text is
    given, the share of blank pages (a document average alone misses a scanned section).
    `bad_pages` lists the blank pages, 0-based."""
    if n_pages is None and page_texts is not None:
        n_pages = len(page_texts)
    m = text_metrics(text, n_pages)
    reasons = []
    if m["words"] == 0:
        reasons.append("no_text_layer")
    else:
        if m["unique_words"] < MIN_UNIQUE_WORDS:
            reasons.append("few_unique_words")
        if m["words"] >= MIN_WORDS_FOR_SHARE and m["top_word_share"] > MAX_TOP_WORD_SHARE:
            reasons.append("one_word_dominates")
        if m["printable_ratio"] < MIN_PRINTABLE_RATIO:
            reasons.append("unprintable_characters")
        if m["word_char_ratio"] < MIN_WORD_CHAR_RATIO:
            reasons.append("few_word_characters")
        if watermark_only(text, n_pages):
            reasons.append("watermark_only_text")
    bad = []
    if page_texts:
        bad = bad_pages(page_texts)
        m["bad_pages"] = len(bad)
        m["bad_page_share"] = round(len(bad) / len(page_texts), 4)
        if m["bad_page_share"] >= MAX_BAD_PAGE_SHARE:
            reasons.append("image_only_pages")
    return {"ok": not reasons, "reasons": reasons, "metrics": m, "bad_pages": bad}


# ---------------------------------------------------------------- Tesseract
def default_tessdata_dir():
    """%LOCALAPPDATA%\\Tesseract-OCR\\tessdata: the full language set on this machine (the
    winget build bundles only eng and osd)."""
    base = os.environ.get("LOCALAPPDATA") or os.path.join(os.path.expanduser("~"), "AppData", "Local")
    return os.path.join(base, "Tesseract-OCR", "tessdata")


def ensure_tessdata_prefix():
    """Set TESSDATA_PREFIX to default_tessdata_dir() when it is unset and that folder exists
    (setdefault: an inherited value always wins). Returns the value in effect, or ""."""
    d = default_tessdata_dir()
    if os.path.isdir(d):
        os.environ.setdefault("TESSDATA_PREFIX", d)
    return os.environ.get("TESSDATA_PREFIX", "")


def _tesseract_cmd():
    found = shutil.which("tesseract")
    if found:
        return found
    pf = os.environ.get("ProgramFiles")
    if pf:
        cand = os.path.join(pf, "Tesseract-OCR", "tesseract.exe")
        if os.path.isfile(cand):
            return cand
    return ""


def tesseract_status():
    """{"available", "cmd", "version", "languages", "tessdata_prefix", "error"} for the Tesseract
    OCR uses. Points pytesseract at the binary found. Runs only when OCR is requested."""
    out = {"available": False, "cmd": "", "version": "", "languages": [],
           "tessdata_prefix": ensure_tessdata_prefix(), "error": ""}
    cmd = _tesseract_cmd()
    if not cmd:
        out["error"] = "tesseract not found on PATH"
        return out
    try:
        import pytesseract
    except ImportError as e:      # pragma: no cover - a locked dependency
        out["error"] = f"pytesseract not importable: {e}"
        return out
    pytesseract.pytesseract.tesseract_cmd = cmd
    out["cmd"] = cmd
    try:
        r = subprocess.run([cmd, "--version"], capture_output=True, timeout=30)
        first = (r.stdout or r.stderr or b"").decode("utf-8", "replace").strip().splitlines()
        out["version"] = first[0].strip() if first else ""
        out["languages"] = sorted(pytesseract.get_languages())
    except Exception as e:
        out["error"] = f"{type(e).__name__}: {str(e)[:120]}"
        return out
    out["available"] = True
    return out


def ris_language(pdf_path):
    """The raw `LA` value of the PDF's `.ris`, or ""."""
    ris = pdf_path[:-4] + ".ris"
    try:
        with open(ris, encoding="utf-8", errors="replace") as f:
            for line in f:
                m = re.match(r"^LA\s{2}-\s?(.*)$", line.rstrip("\r\n"))
                if m and m.group(1).strip():
                    return m.group(1).strip()
    except OSError:
        pass
    return ""


def tesseract_lang(value):
    """A .ris LA value as a Tesseract language code ('en' and 'English' give 'eng', 'spa' stays
    'spa'); '' for no value. An unknown value is lower-cased and kept, so a missing language
    is reported by name."""
    v = str(value or "").strip().lower()
    if not v:
        return ""
    v = re.split(r"[-_;,/ ]", v)[0] if not v.startswith("chi_") else v
    return _LANG_ALIASES.get(v, v)


def parse_tsv(tsv):
    """(text, word confidences) from Tesseract's TSV output: the words of each line joined by a
    space, lines by a newline, blocks by a blank line; confidences (0-100) of the word rows that
    carry text. One Tesseract run gives both (pytesseract's txt+tsv multi-output needs the
    `configs/txt` file, which the full tessdata copy on this machine lacks)."""
    rows = (tsv or "").splitlines()
    if not rows:
        return "", []
    head = rows[0].split("\t")
    try:
        ix = {k: head.index(k) for k in ("level", "block_num", "par_num", "line_num", "conf", "text")}
    except ValueError:
        return "", []
    lines, confs = {}, []
    for ln in rows[1:]:
        parts = ln.split("\t")
        if len(parts) < len(head) or parts[ix["level"]] != "5":
            continue
        word = parts[ix["text"]].strip()
        if not word:
            continue
        key = (int(parts[ix["block_num"]]), int(parts[ix["par_num"]]), int(parts[ix["line_num"]]))
        lines.setdefault(key, []).append(word)
        try:
            c = float(parts[ix["conf"]])
        except ValueError:
            continue
        if c >= 0:
            confs.append(c)
    out, last_block = [], None
    for key in sorted(lines):
        if last_block is not None and key[0] != last_block:
            out.append("")
        out.append(" ".join(lines[key]))
        last_block = key[0]
    return "\n".join(out), confs


def ocr_pages(pdf_path, pages, lang, dpi=OCR_DPI):
    """OCR the 0-based `pages` of a PDF: each page rendered by PyMuPDF at `dpi` (grey) and read by
    Tesseract through pytesseract in `lang`. Returns ({page: text}, [word confidences])."""
    import pytesseract
    from PIL import Image
    texts, confs = {}, []
    with pymupdf.open(pdf_path) as doc:
        for i in pages:
            pix = doc[i].get_pixmap(dpi=dpi, colorspace=pymupdf.csGRAY)
            with Image.open(io.BytesIO(pix.tobytes("png"))) as img:
                tsv = pytesseract.image_to_data(img, lang=lang, timeout=OCR_PAGE_TIMEOUT_S)
            texts[i], c = parse_tsv(tsv)
            confs.extend(c)
    return texts, confs


# ---------------------------------------------------------------- record helpers
def _identity_fields(pdf_path):
    """The verdict fields of `<stem>.identity.json`, or {} (absent or unreadable)."""
    path = pdf_path[:-4] + ".identity.json"
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
    except (OSError, ValueError) as e:
        print(f"  WARN unreadable {os.path.basename(path)[:60]} ({type(e).__name__})", file=sys.stderr)
        return {}
    if not isinstance(d, dict):
        return {}
    return {k: d[k] for k in IDENTITY_FIELDS if k in d}


def finish_record(rec, pdf_path, *, text_ok):
    """Fields every written sidecar gets after the merge: the `.identity.json` verdict, the
    `.ris` DOI (Gap2) when no valid DOI remains, and else the front-matter DOI of a gate-passed
    text as `doi_candidate` (never as `doi`)."""
    rec.update(_identity_fields(pdf_path))
    # Gap2: seed the sidecar DOI from the companion .ris, but ONLY when it is still not a valid
    # DOI AFTER the merge -- so an enriched/corrected DOI (fill_missing_dois writes it to the
    # sidecar before the .ris is regenerated) is never clobbered by a stale .ris value (RC5).
    # normalize_doi + is_valid_doi gate it (matching fill_missing_dois' T7 discipline): a
    # URL-form .ris DOI is recovered, a malformed one is left out rather than written raw and
    # suppressing later recovery. Fills born-empty DataCite/arXiv sidecars, nothing else.
    if not lit_util.is_valid_doi(rec.get("doi", "")):
        ris_path = pdf_path[:-4] + ".ris"
        if os.path.exists(ris_path):
            ris_doi = lit_util.normalize_doi(lit_util.parse_ris(ris_path).get("doi", ""))
            if ris_doi and lit_util.is_valid_doi(ris_doi):
                rec["doi"] = ris_doi
    # DEC-20: a DOI read from the text is a candidate only; `doi` stays empty so
    # fill_missing_dois still treats the PDF as an orphan and verifies it before accepting.
    if not lit_util.is_valid_doi(rec.get("doi", "")) and text_ok and rec.get("text"):
        cand = lit_util.extract_doi_from_text(rec["text"], max_chars=DOI_TEXT_WINDOW)
        if cand:
            rec["doi_candidate"] = cand
            rec["doi_source"] = "text"
    return rec


def _needs_ocr_record(gate, extractor, reason=None):
    rec = _empty_sidecar()
    rec["extractor"] = extractor if extractor in PIPELINE_EXTRACTORS else ""
    rec["needs_ocr"] = True
    rec["needs_ocr_reason"] = reason or "; ".join(gate["reasons"])
    rec["text_metrics"] = gate["metrics"]
    return rec


def _old_text_passes(old, n_pages):
    t = str((old or {}).get("text") or "")
    return bool(t.strip()) and text_validity(t, None, n_pages)["ok"]


# ---------------------------------------------------------------- the run
class _Parser(argparse.ArgumentParser):
    """Usage errors exit 1 (argparse's 2 would read as DEGRADED to a caller)."""
    def error(self, message):
        self.print_usage(sys.stderr)
        self.exit(EXIT_USAGE, f"{self.prog}: error: {message}\n")


def _new_counts():
    return {"pdfs": 0, "skipped": 0, "new": 0, "jats": 0, "failed": 0, "needs_ocr": 0,
            "ocr": 0, "ocr_low_confidence": 0, "ocr_failed": 0, "kept_merged": 0, "kept_gate": 0}


class _Run:
    """One pass over a library: the per-PDF decisions and their counts."""

    def __init__(self, lib, *, refresh, force, ocr, ocr_dpi, tess):
        self.lib, self.refresh, self.force, self.ocr, self.ocr_dpi = lib, refresh, force, ocr, ocr_dpi
        self.tess = tess
        self.c = _new_counts()
        self.errors = []

    def write(self, sidecar, rec):
        lit_util.atomic_write_json(sidecar, rec)  # RC4: crash-safe

    def process(self, fn, old, exists):
        lib = self.lib
        sidecar = os.path.join(lib, fn[:-4] + ".fulltext.json")
        pdf_path = os.path.join(lib, fn)
        merged = exists and old is not None and not is_replaceable(old)
        if merged and not self.force:
            why = "JATS sidecar" if _is_jats_sourced(old) else "merged sidecar"
            print(f"  KEEP {fn[:60]} ({why}: {str(old.get('extractor') or '')[:40]!r}; "
                  f"--force replaces it)")
            self.c["kept_merged"] += 1
            return
        jats_old = merged and _is_jats_sourced(old)

        # Tier 1: JATS XML sibling wins when present (carries formulas, structured
        # sections, abstract — none of which pdftotext gives us). Fall through to
        # PDF extraction on parse error or empty text.
        jats_rec, jats_err = try_parse_jats_sibling(pdf_path)
        if jats_rec is not None and (jats_rec.get("text") or "").strip():
            jats_rec["extracted_from_pdf"] = False
            jats_rec["extractor"] = "jats_xml_sibling"
            # RC5: on --refresh, never DROP enriched metadata (doi/title/year/authors/
            # figures) a prior fill/backfill wrote that this JATS parse lacks.
            if old is not None:
                jats_rec = combine(old, jats_rec, keep_old_text=merged and not jats_old)
            finish_record(jats_rec, pdf_path, text_ok=True)
            self.write(sidecar, jats_rec)
            print(f"  JATS xml_sibling {len(jats_rec.get('text') or ''):>6}c "
                  f"({jats_rec.get('n_formulas',0)} formulas) -> {fn[:50]}")
            self.c["new"] += 1; self.c["jats"] += 1
            return
        if jats_err and jats_err != "no_xml_sibling":
            print(f"  WARN JATS parse failed ({jats_err}) for {fn[:55]} — falling back to PDF",
                  file=sys.stderr)

        # D2: with --force a JATS-sourced sidecar re-parses its sibling (above); when the sibling
        # is gone or failed, keep it -- re-extracting to bare PDF text would silently downgrade
        # the structured sections/formulas/abstract.
        if jats_old:
            print(f"  KEEP {fn[:60]} (refresh: JATS sidecar not downgraded to PDF text)")
            self.c["kept_merged"] += 1
            return

        pages, _perr = pdf_page_texts(pdf_path)
        n_pages = len(pages) if pages is not None else None
        if self.ocr and old is not None and old.get("needs_ocr") is True and not self.refresh:
            # an earlier run's needs_ocr sidecar: straight to OCR, no re-extraction (PyMuPDF's
            # text says which pages to OCR)
            gate = text_validity("\n\n".join(pages or []), pages, n_pages)
            self.ocr_and_write(fn, pdf_path, sidecar, old, merged, gate, pages,
                               str(old.get("extractor") or ""))
            return

        text, extractor, status, gate = extract_gated(pdf_path, pages)
        if not gate["ok"]:
            if _old_text_passes(old, n_pages):
                print(f"  KEEP {fn[:60]} (gate: new text fails ({', '.join(gate['reasons'])}); "
                      f"the sidecar's text passes)")
                self.c["kept_gate"] += 1
                return
            if self.ocr and self.tess and self.tess.get("available"):
                self.ocr_and_write(fn, pdf_path, sidecar, old, merged, gate, pages, extractor)
                return
            rec = _needs_ocr_record(gate, extractor)
            if old is not None:
                rec = combine(old, rec, keep_old_text=merged)
            finish_record(rec, pdf_path, text_ok=False)
            self.write(sidecar, rec)
            print(f"  OCR? {fn[:60]} (needs_ocr: {rec['needs_ocr_reason']}"
                  + (f"; {status}" if not text.strip() else "") + ")")
            self.c["needs_ocr"] += 1
            return

        rec = _empty_sidecar()
        rec["text"] = text
        rec["extractor"] = extractor
        rec["formula_failures"] = detect_math_indicators(text, pdf_path)
        # RC5: on --refresh, _empty_sidecar() has blank doi/title/year/authors/figures; merge_sidecar
        # preserves whatever fill_missing_dois/backfill already wrote (the confirmed PD DOI-wipe bug).
        # A plain re-extract only touches the extraction fields; a forced re-extract of a merged
        # sidecar keeps the replaced text as text_preocr.
        if old is not None:
            rec = combine(old, rec, keep_old_text=merged)
        finish_record(rec, pdf_path, text_ok=True)
        self.write(sidecar, rec)
        print(f"  DL   {extractor:<11} {len(text):>7}c -> {fn[:60]}")
        self.c["new"] += 1

    def ocr_and_write(self, fn, pdf_path, sidecar, old, merged, gate, pages, extractor):
        """OCR in the `.ris` LA language plus English, and write the result (or a needs_ocr
        record). Only the blank pages are OCR'd when blank pages were the gate's one reason (a
        scanned section; the other pages keep PyMuPDF's text); every page otherwise, and every
        page again when the mixed text still fails the gate."""
        installed = set(self.tess.get("languages") or [])
        lang = tesseract_lang(ris_language(pdf_path))
        if lang and lang not in installed:
            rec = _needs_ocr_record(gate, extractor, reason=f"language {lang} not installed")
            if old is not None:
                rec = combine(old, rec, keep_old_text=merged)
            finish_record(rec, pdf_path, text_ok=False)
            self.write(sidecar, rec)
            print(f"  OCR? {fn[:60]} (needs_ocr: language {lang} not installed)")
            self.c["needs_ocr"] += 1
            return
        langs = [x for x in dict.fromkeys([lang, "eng"]) if x and x in installed]
        ocr_lang = "+".join(langs) or "eng"
        n = len(pages) if pages is not None else 0
        if n == 0:
            self._ocr_failed(fn, pdf_path, sidecar, old, merged, gate, extractor,
                             "PyMuPDF cannot read the file")
            return
        every = list(range(n))
        targets = gate["bad_pages"] if gate["reasons"] == ["image_only_pages"] else every
        try:
            texts, confs = ocr_pages(pdf_path, targets, ocr_lang, self.ocr_dpi)
            final = [texts.get(i, pages[i]) for i in every]
            text = clean_pdf_text("\f".join(final))
            g2 = text_validity(text, final, n)
            if not g2["ok"] and targets != every:
                rest = [i for i in every if i not in texts]
                more, more_confs = ocr_pages(pdf_path, rest, ocr_lang, self.ocr_dpi)
                texts.update(more)
                confs += more_confs
                targets = every
                final = [texts[i] for i in every]
                text = clean_pdf_text("\f".join(final))
                g2 = text_validity(text, final, n)
        except Exception as e:
            self._ocr_failed(fn, pdf_path, sidecar, old, merged, gate, extractor,
                             f"{type(e).__name__}: {str(e)[:120]}")
            return
        mean_conf = round(sum(confs) / len(confs), 1) if confs else None
        if g2["ok"]:
            rec = _empty_sidecar()
            rec["extractor"] = "tesseract"
            rec["text"] = text
            rec["formula_failures"] = detect_math_indicators(text, pdf_path)
        else:
            # the attempt is recorded, but the record stays a pipeline needs_ocr sidecar (not a
            # merged one), so a later --refresh or --ocr may still replace it
            rec = _needs_ocr_record(g2, extractor,
                                    reason="ocr text fails the gate: " + "; ".join(g2["reasons"]))
        rec.update({"ocr_dpi": self.ocr_dpi, "ocr_lang": ocr_lang,
                    "ocr_engine_version": self.tess.get("version", ""),
                    "ocr_mean_confidence": mean_conf, "ocr_pages": [i + 1 for i in targets]})
        if mean_conf is not None and mean_conf < OCR_LOW_CONFIDENCE:
            rec["ocr_low_confidence"] = True
        if old is not None:
            rec = combine(old, rec, keep_old_text=merged)
        finish_record(rec, pdf_path, text_ok=g2["ok"])
        self.write(sidecar, rec)
        if g2["ok"]:
            low = " LOW-CONFIDENCE" if rec.get("ocr_low_confidence") else ""
            print(f"  OCR  {ocr_lang:<11} {len(text):>7}c conf={mean_conf}{low} "
                  f"pages={len(targets)}/{n} -> {fn[:50]}")
            self.c["ocr"] += 1; self.c["new"] += 1
            if rec.get("ocr_low_confidence"):
                self.c["ocr_low_confidence"] += 1
        else:
            print(f"  OCR? {fn[:60]} ({rec['needs_ocr_reason']})")
            self.c["needs_ocr"] += 1

    def _ocr_failed(self, fn, pdf_path, sidecar, old, merged, gate, extractor, why):
        rec = _needs_ocr_record(gate, extractor, reason=f"ocr failed: {why}")
        if old is not None:
            rec = combine(old, rec, keep_old_text=merged)
        finish_record(rec, pdf_path, text_ok=False)
        self.write(sidecar, rec)
        print(f"  FAIL {fn[:60]} (ocr failed: {why})", file=sys.stderr)
        self.c["ocr_failed"] += 1; self.c["needs_ocr"] += 1
        self.errors.append(f"ocr failed: {fn}: {why}")


def _refs_header(text):
    try:
        from reverse_citations import REFS_HDR
    except Exception:           # pragma: no cover - the walker module failed to import
        return None
    return bool(REFS_HDR.search(text or ""))


SUSPECT_FIELDS = ["file", "doi", "gate", "reasons", "n_pages", "words", "text_source",
                  "refresh_fixes"]


def write_suspect_report(lib, path):
    """Read-only: the PDFs the gate or litpipe.identity.suspect_file flags, as a CSV (file, doi,
    gate, reasons, ...): the re-fetch list. The text read is the sidecar's own when this
    pipeline extracted it, the recorded reasons of a needs_ocr sidecar, PyMuPDF's text for a
    JATS-sourced sidecar, and else a fresh extraction held in memory (an OCR'd or hand-filed
    sidecar, or none). A stored text that fails is re-extracted in memory too:
    `refresh_fixes` names the extractor whose text passes (a --refresh repairs that sidecar,
    no re-fetch needed). Nothing in the library is written."""
    rows = []
    for fn in sorted(f for f in os.listdir(lib) if f.endswith(".pdf")):
        pdf_path = os.path.join(lib, fn)
        old = _load_existing_sidecar(os.path.join(lib, fn[:-4] + ".fulltext.json"))
        doi = lit_util.normalize_doi(str((old or {}).get("doi") or ""))
        if not lit_util.is_valid_doi(doi) and os.path.exists(pdf_path[:-4] + ".ris"):
            doi = lit_util.normalize_doi(lit_util.parse_ris(pdf_path[:-4] + ".ris").get("doi", ""))
        pages, _ = pdf_page_texts(pdf_path)
        n = len(pages) if pages is not None else None
        fixes = ""
        if old is not None and old.get("needs_ocr") is True:
            reasons = [r.strip() for r in str(old.get("needs_ocr_reason") or "").split(";") if r.strip()]
            text, gate, source = "", "needs_ocr", "sidecar"
        else:
            if old is not None and is_replaceable(old) and str(old.get("text") or "").strip():
                text, source = old["text"], "stored"
                g = text_validity(text, pages, n)
                if not g["ok"]:
                    _t, ex, _st, g_new = extract_gated(pdf_path, pages)
                    fixes = ex if g_new["ok"] else ""
            elif old is not None and _is_jats_sourced(old) and pages is not None:
                # the text is the JATS parse; PyMuPDF's text judges the PDF's own layer cheaply
                text, source = clean_pdf_text("\n\n".join(pages)), "pymupdf"
                g = text_validity(text, pages, n)
            else:
                text, _ex, _st, g = extract_gated(pdf_path, pages)
                source = "fresh"
            reasons, gate = list(g["reasons"]), ("pass" if g["ok"] else "fail")
        sus = _identity.suspect_file(n, _refs_header(text) if text else None, None,
                                     _identity.text_stats(text, n) if text else None)
        if gate == "pass" and "watermark_only_text" in sus.reasons and not watermark_only(text, n):
            sus_reasons = [r for r in sus.reasons if r != "watermark_only_text"]
        else:
            sus_reasons = list(sus.reasons)
        reasons += [r for r in sus_reasons if r not in reasons]
        if gate != "pass" or reasons:
            rows.append({"file": fn, "doi": doi, "gate": gate, "reasons": ";".join(reasons),
                         "n_pages": "" if n is None else n,
                         "words": len(_WORD.findall(text)) if text else 0, "text_source": source,
                         "refresh_fixes": fixes})
    rows.sort(key=lambda r: (r["gate"] == "pass", r["file"]))
    lit_util.atomic_write_csv(path, rows, SUSPECT_FIELDS)
    return rows


def _summary_line(res):
    small = {"reasons": res["reasons"], "aborted": None, "transport_failures": 0,
             "counts": res["counts"]}
    print(SUMMARY_MARKER + json.dumps(small, ensure_ascii=False), flush=True)


def run(*, lib_dir, limit=0, refresh=False, force=False, dry_run=False, ocr=False,
        ocr_dpi=OCR_DPI, suspect_report=None, **_ignored) -> dict:
    """Extract every PDF in `lib_dir` without a sidecar (see the module docstring). The keywords
    are the CLI flags' dest names. Returns {"exit", "lib_dir", "counts", "reasons", "errors",
    "tesseract", "suspects"}."""
    res = {"exit": EXIT_OK, "lib_dir": "", "counts": _new_counts(), "reasons": [], "errors": [],
           "tesseract": None, "suspects": None}
    report_path = suspect_report
    if not lib_dir:
        print("ERR: --lib-dir is required", file=sys.stderr)
        res["exit"] = EXIT_USAGE
        return res
    lib = os.path.abspath(lib_dir)
    res["lib_dir"] = lib
    if not os.path.isdir(lib):
        print(f"ERR: lib-dir not a directory: {lib}", file=sys.stderr)
        res["exit"] = EXIT_USAGE
        return res
    if force and not refresh:
        print("ERR: --force needs --refresh (it replaces merged sidecars on a re-extract)",
              file=sys.stderr)
        res["exit"] = EXIT_USAGE
        return res
    if report_path:
        rows = write_suspect_report(lib, report_path)
        res["suspects"] = len(rows)
        n_fail = sum(1 for r in rows if r["gate"] != "pass")
        print(f"Suspect report: {len(rows)} PDF(s) flagged ({n_fail} fail the text gate) "
              f"-> {report_path}")
        return res

    pdfs = sorted(f for f in os.listdir(lib) if f.endswith(".pdf"))
    print(f"Library: {lib}\n  {len(pdfs)} PDFs found  (pdftotext={'yes' if PDFTOTEXT else 'no'})")

    tess = None
    if ocr:
        tess = tesseract_status()
        res["tesseract"] = {k: v for k, v in tess.items() if k != "languages"}
        res["tesseract"]["n_languages"] = len(tess["languages"])
        if not tess["available"]:
            print(f"ERR: --ocr: {tess['error']}; no OCR this run (the needs_ocr sidecars are "
                  f"still written)", file=sys.stderr)
            res["reasons"].append(f"tesseract unavailable: {tess['error']}")
        else:
            print(f"  OCR: {tess['version']} at {tess['cmd']}, {len(tess['languages'])} "
                  f"languages, {ocr_dpi} dpi")
            if len(tess["languages"]) < MIN_LANGUAGES:
                print("!" * 78 + f"\n!! WARNING: Tesseract lists only {len(tess['languages'])} "
                      f"language(s) ({', '.join(tess['languages'])}).\n!! TESSDATA_PREFIX is "
                      f"{tess['tessdata_prefix'] or 'unset'}: Tesseract fell back to its bundled "
                      f"set. Point TESSDATA_PREFIX at the full tessdata folder.\n!! Carrying on "
                      f"with eng; a PDF whose .ris LA asks for another language is not OCR'd.\n"
                      + "!" * 78, flush=True)

    runner = _Run(lib, refresh=refresh, force=force, ocr=ocr, ocr_dpi=ocr_dpi, tess=tess)
    c = runner.c
    c["pdfs"] = len(pdfs)
    n_done = 0
    for fn in pdfs:
        sidecar = os.path.join(lib, fn[:-4] + ".fulltext.json")
        exists = os.path.exists(sidecar)
        old = _load_existing_sidecar(sidecar) if exists else None
        wants_ocr = (ocr and old is not None and old.get("needs_ocr") is True
                     and bool(tess and tess.get("available")))
        if exists and not refresh and not wants_ocr:
            c["skipped"] += 1
            continue
        if limit and n_done >= limit:
            print(f"  (limit {limit} reached)")
            break
        if dry_run:
            print(f"  DRY  {fn[:70]}")
            continue
        before = c["new"] + c["needs_ocr"]
        try:
            runner.process(fn, old if exists else None, exists)
        except Exception as e:
            print(f"  FAIL {fn[:65]} (raised {type(e).__name__}: {str(e)[:120]})", file=sys.stderr)
            c["failed"] += 1
            runner.errors.append(f"raised: {fn}: {type(e).__name__}")
        n_done += (c["new"] + c["needs_ocr"]) - before

    print(f"\n=== Summary ===")
    print(f"  PDFs in library:        {len(pdfs)}")
    print(f"  Sidecars already there: {c['skipped']}")
    print(f"  Sidecars NEW:           {c['new']}  (of which JATS-XML-sibling: {c['jats']})")
    print(f"  Extraction failed:      {c['failed']}")
    print(f"  Needs OCR:              {c['needs_ocr']}  (sidecar written with needs_ocr: true)")
    print(f"  OCR'd:                  {c['ocr']}  (low confidence: {c['ocr_low_confidence']}; "
          f"failed: {c['ocr_failed']})")
    print(f"  Kept (merged):          {c['kept_merged']}")
    print(f"  Kept (gate):            {c['kept_gate']}")

    res["counts"] = dict(c)
    if c["failed"]:
        res["reasons"].append(f"extraction raised on {c['failed']} PDF(s)")
    if c["ocr_failed"]:
        res["reasons"].append(f"OCR failed on {c['ocr_failed']} PDF(s)")
    res["errors"] = runner.errors
    if res["reasons"]:
        res["exit"] = EXIT_DEGRADED
        _summary_line(res)
    return res


def main(argv=None) -> int:
    ap = _Parser(description="Extract PDF text into .fulltext.json sidecars, behind the "
                             "text-validity gate, with optional OCR.")
    ap.add_argument("--lib-dir", required=True)
    ap.add_argument("--limit", type=int, default=0,
                    help="Process at most N new PDFs (0 = no limit).")
    ap.add_argument("--refresh", action="store_true",
                    help="Re-extract and overwrite existing sidecars this pipeline wrote "
                         "(merged sidecars print KEEP).")
    ap.add_argument("--force", action="store_true",
                    help="With --refresh: also replace merged sidecars (OCR text, hand repairs, "
                         "JATS); every field is kept and the old text moves to text_preocr.")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--ocr", action="store_true",
                    help="OCR the PDFs whose text fails the gate, and needs_ocr sidecars, with "
                         "Tesseract (the .ris LA language plus English).")
    ap.add_argument("--ocr-dpi", type=int, default=OCR_DPI,
                    help=f"Render resolution for OCR (default {OCR_DPI}).")
    ap.add_argument("--suspect-report", default=None, metavar="PATH",
                    help="Read-only: write the re-fetch list (file, doi, reasons for the PDFs the "
                         "gate or suspect_file flags) to PATH and change nothing else.")
    args = ap.parse_args(argv)
    res = run(lib_dir=args.lib_dir, limit=args.limit, refresh=args.refresh, force=args.force,
              dry_run=args.dry_run, ocr=args.ocr, ocr_dpi=args.ocr_dpi,
              suspect_report=args.suspect_report)
    return res["exit"]


if __name__ == "__main__":
    sys.exit(main())
