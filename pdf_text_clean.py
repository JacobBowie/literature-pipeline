"""Post-process pymupdf-extracted text to fix three issues observed empirically
on two research libraries (2026-04-27 validation, n=6 papers).

Fixes:
  1. Unicode ligatures — pymupdf preserves the glyph (ﬁ U+FB01) instead of ASCII 'fi'.
     Impact: case-insensitive substring search drops matches. Validated:
     193 occurrences in Laxminarayan 2018; "fi-words" matched 13× in raw vs 49× in JATS.
  2. Soft-hyphenation at line breaks — pymupdf preserves "train-\\ning"; downstream
     string matching misses "training". Validated: 19-127 hyphen-line-break breaks per paper.
  3. Bare page-number lines — Pourteymour 2017 had 149 such lines in extracted text.

  4. Non-breaking and thin spaces (on by default, W4-C): U+00A0, U+2007, U+2009 and U+202F
     become a plain space and U+200B (zero-width space) is dropped, so a search for
     "10 mg" or "VO2max" matches the printed text. Not destructive: no visible character changes.

Optional (off by default):
  - Smart-quote and dash normalization (' ' " " — – → ' ' " " - -).
    Off because we haven't validated it as a real-world problem yet.

Usage:
    from pdf_text_clean import clean_pdf_text
    raw = "\\n".join(p.get_text() for p in pymupdf.open(path))
    clean = clean_pdf_text(raw)

The module imports only the standard library at top level: litpipe.text imports LIGATURES from
it, and audit_portfolio imports it too.
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

# Space characters a PDF text layer carries that a plain-text search does not match: three
# fixed-width spaces become an ordinary space, and the zero-width space is dropped.
SPACES = {
    0x00A0: " ",   # no-break space
    0x2007: " ",   # figure space
    0x2009: " ",   # thin space
    0x202F: " ",   # narrow no-break space
    0x200B: None,  # zero-width space (dropped)
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
                    aggressive_dehyphenate: bool = False,
                    strip_page_numbers: bool = False,
                    normalize_typography: bool = False,
                    normalize_spaces: bool = True) -> str:
    """Post-process pymupdf-extracted text for clean string matching.

    Each transformation is independently toggleable in case a downstream consumer
    needs the raw form (e.g., preserving "—" semantics in dialog).

    Ligature expansion and space normalization (SPACES: no-break, figure, thin and narrow
    no-break spaces to a space, the zero-width space dropped) are non-destructive, so they
    stay on by default. The other three are DESTRUCTIVE and default OFF (RC7, 2026-06-05 audit):
      - aggressive_dehyphenate fuses words across a line-break hyphen, which also
        fuses real compounds ("core-\\nbody" -> "corebody").
      - strip_page_numbers deletes any lone 1-4-digit line, which silently removes
        data values and years, not just page numbers.
      - normalize_typography rewrites smart quotes/dashes (unvalidated).
    Enable them explicitly only on text where the loss is acceptable.

    Returns the cleaned text. Idempotent — running twice produces the same output.
    """
    if expand_ligatures:
        text = text.translate(LIGATURES)
    if normalize_spaces:
        text = text.translate(SPACES)
    if aggressive_dehyphenate:
        # word-<whitespace>newline<whitespace>word → wordword
        # Matches a layout-induced line break after a hyphen, but ALSO fuses real
        # hyphenated compounds split across lines ("core-\nbody" -> "corebody"),
        # so it is opt-in.
        text = re.sub(r"(\w+)-\s*\n\s*(\w+)", r"\1\2", text)
    if strip_page_numbers:
        # Lines whose only content is 1-4 digits, optionally surrounded by whitespace.
        # Also deletes lone numeric data values / years, so it is opt-in.
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
    import argparse, sys
    ap = argparse.ArgumentParser(
        description="Library helpers for cleaning PDF text dumps (ligature, soft-hyphen, "
                    "bare-page-number post-process). Run with a PDF path for a quick diff probe.")
    ap.add_argument("pdf", nargs="?", help="Path to a PDF to clean + report issues for.")
    args = ap.parse_args()
    if not args.pdf:
        print("pdf_text_clean is a library module; import clean_pdf_text from it, "
              "or pass a PDF path to run a quick before/after diff.", file=sys.stderr)
        sys.exit(0)
    import pymupdf
    with pymupdf.open(args.pdf) as doc:
        raw = "\n".join(p.get_text() for p in doc)
    before = report_issues(raw)
    clean = clean_pdf_text(raw)
    after = report_issues(clean)
    print(f"File:           {args.pdf}")
    print(f"Length raw:     {len(raw):,}")
    print(f"Length clean:   {len(clean):,}  ({len(clean)-len(raw):+,})")
    print(f"Ligatures:      {before['ligatures']} → {after['ligatures']}")
    print(f"Hyphen breaks:  {before['linebreak_hyphens']} → {after['linebreak_hyphens']}")
    print(f"Page numbers:   {before['bare_page_numbers']} → {after['bare_page_numbers']}")
