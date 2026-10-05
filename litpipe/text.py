"""Text hygiene for metadata fields: markup tags, character references, compatibility forms,
ligatures and odd spaces (W1-B; plan I33, NEW-C0, NEW-A10).

Crossref returns JATS: titles and abstracts carry tags (`<jats:p>`, `<i>`), escaped text
(`P &lt; 0.05`, `Physiology &amp; Behavior`, double-escaped `&amp;lt;`), named references that
reach filenames (`M&uuml;ndel` gave the stem `Muumlndel`), and ISO Greek SGML entities that no
HTML parser knows (`&agr;`, `&bgr;`). PDF text layers add Alphabetic Presentation Forms ligatures
(`barore\\ufb02ex` defeats a grep for "reflex") and no-break or thin spaces.

Order is load-bearing: strip tags FIRST, then decode. Decoding first turns escaped literal text
(`The tag &lt;jats:italic&gt; is literal`) into a tag that the stripper then deletes
(2026-09-17 intake note, P1).

Standards (read 2026-09-30):
- Python `html.unescape` (https://docs.python.org/3/library/html.html): "Convert all named and
  numeric character references ... uses the rules defined by the HTML 5 standard for both valid
  and invalid character references, and the list of HTML 5 named character references." Probed
  (3.13.15): it also decodes LEGACY names with no semicolon as prefixes (`&parameter` becomes
  `\\u00b6meter`, `&copy2` becomes `\\u00a92`, `&notit;` becomes `\\u00acit;`), and it leaves
  `&agr;` alone. So `unescape` below decodes only semicolon-terminated references, via the same
  WHATWG table (`html.entities.html5`, 2231 names), plus isogrk1.
- WHATWG HTML 13.5 "Named character references" (https://html.spec.whatwg.org/multipage/named-characters.html):
  "This table lists the character reference names that are supported by HTML, and the code points
  to which they refer." `agr`/`bgr` are not in it; `uuml`, `nbsp`, `thinsp` are.
- W3C "XML Entity Definitions for Characters" (3rd ed., Recommendation 2023-03-07,
  https://www.w3.org/TR/xml-entity-names/) lists isogrk1 as "(not in MathML3 / HTML5)"; the
  table below is its isogrk1.ent (https://www.w3.org/2003/entities/2007/isogrk1.ent), 49 names,
  e.g. `<!ENTITY agr "&#x003B1;">`, `<!ENTITY bgr "&#x003B2;">`.
- Unicode normalization (https://docs.python.org/3/library/unicodedata.html): "The normal form KC
  (NFKC) first applies the compatibility decomposition, followed by the canonical composition."
  Probed on Python 3.13.15 (unidata 15.1.0; the 3.14 docs cite UCD 16.0.0): NFKC maps U+FB00 to
  U+FB06 to ff, fi, fl, ffi, ffl, st, st; U+00A0, U+2009, U+202F (and U+2007, U+200A, U+3000) to
  U+0020; it keeps U+00AD SOFT HYPHEN, U+2010/U+2013/U+2212 dashes and curly quotes; it also
  folds superscripts and subscripts to digits (m\\u00b2 becomes m2) and U+00B5 MICRO SIGN to
  U+03BC.
"""
import html
import html.entities
import re
import unicodedata

# Import-only: pdf_text_clean (owned by W4-C) keeps the one table of ligatures the pipeline
# expands; NFKC maps the same seven code points identically (probed), so this is belt and braces
# that keeps both cleaners on one definition. pdf_text_clean imports only `re` (no cycle).
from pdf_text_clean import LIGATURES

__all__ = ["strip_tags", "unescape", "clean_field", "display_field", "normalise_title", "ISOGRK1"]

# isogrk1 (W3C isogrk1.ent, 2007 entity set, current in the 2023 Recommendation).
ISOGRK1 = {
    "agr": "α", "Agr": "Α", "bgr": "β", "Bgr": "Β", "dgr": "δ",
    "Dgr": "Δ", "eegr": "η", "EEgr": "Η", "egr": "ε", "Egr": "Ε",
    "ggr": "γ", "Ggr": "Γ", "igr": "ι", "Igr": "Ι", "kgr": "κ",
    "Kgr": "Κ", "khgr": "χ", "KHgr": "Χ", "lgr": "λ", "Lgr": "Λ",
    "mgr": "μ", "Mgr": "Μ", "ngr": "ν", "Ngr": "Ν", "ogr": "ο",
    "Ogr": "Ο", "ohgr": "ω", "OHgr": "Ω", "pgr": "π", "Pgr": "Π",
    "phgr": "φ", "PHgr": "Φ", "psgr": "ψ", "PSgr": "Ψ", "rgr": "ρ",
    "Rgr": "Ρ", "sfgr": "ς", "sgr": "σ", "Sgr": "Σ", "tgr": "τ",
    "Tgr": "Τ", "thgr": "θ", "THgr": "Θ", "ugr": "υ", "Ugr": "Υ",
    "xgr": "ξ", "Xgr": "Ξ", "zgr": "ζ", "Zgr": "Ζ",
}

# A markup tag: a name that starts with a letter (optionally namespaced, `jats:p`, `mml:mi`),
# then only well-formed quoted attributes. "p<0.05", "x < y and z > w" and SICI "<385::AID...>"
# are not tags and survive.
_TAG = re.compile(
    r"<(/?)([A-Za-z][\w.-]*(?::[A-Za-z][\w.-]*)?)"
    r"(?:\s+[A-Za-z_:][\w:.-]*\s*=\s*(?:\"[^\"]*\"|'[^']*'))*\s*/?>")
_COMMENT = re.compile(r"<!--.*?-->", re.DOTALL)
# Block elements become a space so paragraphs do not fuse; inline ones (i, sup, sub, scp) vanish.
_BLOCK = frozenset({"p", "sec", "title", "list", "list-item", "li", "br", "div", "abstract",
                    "para", "td", "tr", "th", "table", "label", "caption", "disp-formula",
                    "fig", "table-wrap", "def-list", "boxed-text", "hr"})
_REF = re.compile(r"&(#[0-9]+|#[xX][0-9A-Fa-f]+|[A-Za-z][A-Za-z0-9]*);")
_INVISIBLE = dict.fromkeys(map(ord, "­​⁠﻿"))  # soft hyphen, zero-width, BOM
_SPACE_BEFORE = re.compile(r"\s+([,.;:!?)\]}])")
_SPACE_AFTER = re.compile(r"([(\[{])\s+")


def strip_tags(s):
    """Remove markup tags (and comments), keeping their text; block tags leave a space."""
    if not s:
        return ""
    s = _COMMENT.sub(" ", str(s))

    def repl(m):
        local = m.group(2).rsplit(":", 1)[-1].lower()
        return " " if local in _BLOCK else ""
    return _TAG.sub(repl, s)


def _ref(m):
    name = m.group(1)
    if name.startswith("#"):
        return html.unescape(m.group(0))                # HTML5 numeric rules (0 and >U+10FFFF give U+FFFD)
    if name + ";" in html.entities.html5:
        return html.entities.html5[name + ";"]
    if name in ISOGRK1:
        return ISOGRK1[name]
    return m.group(0)                                   # unknown (publisher-private &OV0312;): keep


def unescape(s, passes=2):
    """Decode semicolon-terminated character references: HTML5 named and numeric ones plus the
    ISO Greek isogrk1 set. At most `passes` rounds, so double encoding (`&amp;lt;`) resolves while
    an adversarial chain cannot loop. Unknown names stay as written."""
    if not s:
        return ""
    s = str(s)
    for _ in range(passes):
        d = _REF.sub(_ref, s)
        if d == s:
            break
        s = d
    return s


def clean_field(s):
    """Tags stripped, references decoded, NFKC, ligatures expanded, soft hyphens and zero-width
    characters dropped, no-break and thin spaces made plain, whitespace collapsed. Case kept."""
    if not s:
        return ""
    s = unescape(strip_tags(s))
    s = unicodedata.normalize("NFKC", s)
    s = s.translate(LIGATURES).translate(_INVISIBLE)
    return " ".join(s.split())                          # str.split() splits on U+00A0/U+2009/U+202F too


# The display form keeps compatibility characters: measured over 6,087 `.ris` (W2-E1, 2026-09-30),
# NFKC would flatten VO2max subscripts and split surnames carrying U+00B4. U+2011 NON-BREAKING
# HYPHEN becomes "-" so a title search for "high-intensity" finds it.
_DISPLAY_MAP = {**LIGATURES, **_INVISIBLE, 0x2011: "-"}


def display_field(s):
    """A metadata string as it should read in a bibliography or an abstract column: tags stripped,
    then references decoded, NFC (not NFKC), ligatures expanded, invisibles dropped, odd spaces
    made plain, whitespace collapsed. Sub/superscripts, the micro sign and U+00B4 are kept."""
    if not s:
        return ""
    s = unescape(strip_tags(str(s)))
    s = unicodedata.normalize("NFC", s).translate(_DISPLAY_MAP)
    return " ".join(s.split())


def normalise_title(s):
    """clean_field, lower-cased, with no space before closing punctuation or after opening
    brackets: the comparison form of a title (never a display form)."""
    s = clean_field(s).lower()
    s = _SPACE_BEFORE.sub(r"\1", s)
    s = _SPACE_AFTER.sub(r"\1", s)
    return " ".join(s.split())
