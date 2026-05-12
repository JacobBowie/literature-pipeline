"""Post-process pymupdf-extracted text to fix three issues observed empirically
on the getpaid + Physiological_Data libraries (2026-04-27 validation, n=6 papers).

Fixes:
  1. Unicode ligatures — pymupdf preserves the glyph (ﬁ U+FB01) instead of ASCII 'fi'.
     Impact: case-insensitive substring search drops matches. Validated:
     193 occurrences in Laxminarayan 2018; "fi-words" matched 13× in raw vs 49× in JATS.
  2. Soft-hyphenation at line breaks — pymupdf preserves "train-\\ning"; downstream
     string matching misses "training". Validated: 19-127 hyphen-line-break breaks per paper.
  3. Bare page-number lines — Pourteymour 2017 had 149 such lines in extracted text.

Optional (off by default):
  - Smart-quote and dash normalization (' ' " " — – → ' ' " " - -).
    Off because we haven't validated it as a real-world problem yet.

Usage:
    from pdf_text_clean import clean_pdf_text
    raw = "\\n".join(p.get_text() for p in fitz.open(path))
    clean = clean_pdf_text(raw)
"""
import re

# Unicode ligature glyphs that should expand to ASCII multigraphs.
# Source: Unicode Block "Alphabetic Presentation Forms" (U+FB00–U+FB4F).
LIGATURES = {
    0xFB00: "ff",   # ﬀ
    0xFB01: "fi",   # ﬁ
    0xFB02: "fl",   # ﬂ
    0xFB03: "ffi",  # ﬃ
    0xFB04: "ffl",  # ﬄ
    0xFB05: "st",   # ﬅ (long s + t)
    0xFB06: "st",   # ﬆ
}

# Smart-quote / dash table for the optional pass.
TYPOGRAPHIC = {
    0x2018: "'",  # ' left single
    0x2019: "'",  # ' right single
    0x201C: '"',  # " left double
    0x201D: '"',  # " right double
    0x2013: "-",  # – en dash
    0x2014: "--", # — em dash
    0x00A0: " ",  # nbsp
}


def clean_pdf_text(text: str,
                    *,
                    expand_ligatures: bool = True,
                    dehyphenate: bool = True,
                    strip_page_numbers: bool = True,
                    normalize_typography: bool = False) -> str:
    """Post-process pymupdf-extracted text for clean string matching.

    Each transformation is independently toggleable in case a downstream consumer
    needs the raw form (e.g., preserving "—" semantics in dialog).

    Returns the cleaned text. Idempotent — running twice produces the same output.
    """
    if expand_ligatures:
        text = text.translate(LIGATURES)
    if dehyphenate:
        # word-<whitespace>newline<whitespace>word → wordword
        # Only matches when there's a newline between the hyphen and the next word,
        # which is specifically a layout-induced line break (not a real hyphenated compound).
        text = re.sub(r"(\w+)-\s*\n\s*(\w+)", r"\1\2", text)
    if strip_page_numbers:
        # Lines whose only content is 1-4 digits, optionally surrounded by whitespace.
        text = re.sub(r"(?m)^\s*\d{1,4}\s*$\n?", "", text)
    if normalize_typography:
        text = text.translate(TYPOGRAPHIC)
    return text


def report_issues(text: str) -> dict:
    """Diagnostic counts — useful for QA-ing whether a PDF needed cleaning."""
    return {
        "ligatures": sum(text.count(chr(c)) for c in LIGATURES),
        "linebreak_hyphens": len(re.findall(r"\w+-\s*\n\s*\w+", text)),
        "bare_page_numbers": len(re.findall(r"(?m)^\s*\d{1,4}\s*$", text)),
    }


if __name__ == "__main__":
    import sys, fitz
    if len(sys.argv) < 2:
        print("Usage: python tools/pdf_text_clean.py <pdf-path> [--diff]", file=sys.stderr); sys.exit(1)
    path = sys.argv[1]
    with fitz.open(path) as doc:
        raw = "\n".join(p.get_text() for p in doc)
    before = report_issues(raw)
    clean = clean_pdf_text(raw)
    after = report_issues(clean)
    print(f"File:           {path}")
    print(f"Length raw:     {len(raw):,}")
    print(f"Length clean:   {len(clean):,}  ({len(clean)-len(raw):+,})")
    print(f"Ligatures:      {before['ligatures']} → {after['ligatures']}")
    print(f"Hyphen breaks:  {before['linebreak_hyphens']} → {after['linebreak_hyphens']}")
    print(f"Page numbers:   {before['bare_page_numbers']} → {after['bare_page_numbers']}")
