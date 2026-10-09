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

__all__ = ["strip_tags", "unescape", "clean_field", "display_field", "abstract_field", "normalise_title",
           "comparison_fold", "filename_title", "body_chars", "is_abstract_only", "ISOGRK1", "GREEK_NAMES",
           "TEXT_ONLY_MIN_BODY_CHARS"]

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


# A leading "Abstract" heading (W2-E2 census 2026-10-05: all inspected rows of 33,174 starting with
# "abstract" were a heading): the whole word, any case, then punctuation, space or the end; or a
# case-sensitive glued form (`AbstractBackground`, `ABSTRACTCigarette`). Never "Abstracts of" or
# "ABSTRACTION".
_HEADING_WORD = re.compile(r"^\s*abstract(?:\s*[:.–—-]\s*|\s+|\s*$)", re.IGNORECASE)
_HEADING_GLUED = re.compile(r"^\s*(?:Abstract(?=[A-Z])|ABSTRACT(?=[A-Z][a-z]|[A-Z]\s))")


def abstract_field(raw):
    """Plain-text abstract from a Crossref JATS `abstract`, a DataCite description, a CSL abstract or
    a stored abstract: display_field taken again until it stops changing (at most three more rounds),
    so markup that decoding revealed is stripped and deeper escaping is decoded, then one leading
    "Abstract" heading dropped. '' when nothing but a heading is left. Idempotent.

    The extra rounds are for escaped markup, which display_field alone (tags first, then two decoding
    passes) leaves as text: a publisher deposits `&lt;b&gt;&lt;i&gt;Purpose:&lt;/i&gt;&lt;/b&gt;`
    (display_field gives `<b><i>Purpose:</i></b>`), and one deposit (live 2026-10-05) wraps its whole
    abstract in `&amp;lt;jats:p&amp;gt;` with `p&amp;amp;lt;0,05` inside. Moved here from
    enrich_abstracts.clean_abstract (W2-E2) so the .ris writer's AB lines get the same cleaning."""
    s = display_field(raw)
    for _ in range(3):
        t = display_field(s)
        if t == s:
            break
        s = t
    m = _HEADING_WORD.match(s) or _HEADING_GLUED.match(s)
    if m:
        s = s[m.end():].strip()
    return s


# ------------------------------------------------------------------------------ the comparison fold
# The Greek letters a reference spells out ("beta-adrenergic") and a record prints (U+03B2), by name
# (W5-C2: a consumer's resolvers lost 158 + 36 correct matches over this alone). Final sigma and the
# symbol variants (U+03D0 to U+03F5) fold to the same names; capitals give capitalised names, so a
# caller's lower-casing reads both the same. U+00B5 MICRO SIGN is "mu" (NFKD maps it to U+03BC).
GREEK_NAMES = {
    "α": "alpha", "β": "beta", "γ": "gamma", "δ": "delta", "ε": "epsilon", "ζ": "zeta", "η": "eta",
    "θ": "theta", "ι": "iota", "κ": "kappa", "λ": "lambda", "μ": "mu", "ν": "nu", "ξ": "xi",
    "ο": "omicron", "π": "pi", "ρ": "rho", "σ": "sigma", "ς": "sigma", "τ": "tau", "υ": "upsilon",
    "φ": "phi", "χ": "chi", "ψ": "psi", "ω": "omega",
    "ϐ": "beta", "ϑ": "theta", "ϕ": "phi", "ϖ": "pi", "ϰ": "kappa", "ϱ": "rho", "ϵ": "epsilon",
    "ϒ": "Upsilon",
}
GREEK_NAMES.update({k.upper(): v.capitalize() for k, v in list(GREEK_NAMES.items())
                    if k.upper() != k and len(k.upper()) == 1 and k not in "ςϐϑϕϖϰϱϵ"})
GREEK_NAMES["µ"] = "mu"                                  # U+00B5 MICRO SIGN
# Quotes: the single forms (U+2018, U+2019, U+201A, U+201B, U+2032, U+02BC, U+00B4, U+0060 and the
# ASCII apostrophe) are REMOVED, not spaced, so "ACSM's", "ACSM’s" and "ACSMs" agree; the double
# forms (U+201C, U+201D, U+201E, U+201F, U+2033) become '"'. Dashes U+2010 to U+2015 and U+2212 are "-".
_FOLD_MAP = {**{ord(k): v for k, v in GREEK_NAMES.items()},
             **dict.fromkeys(map(ord, "'\u2018\u2019\u201a\u201b\u2032\u02bc\u00b4\u0060"), ""),
             **dict.fromkeys(map(ord, "\u201c\u201d\u201e\u201f\u2033"), '"'),
             **dict.fromkeys(map(ord, "\u2010\u2011\u2012\u2013\u2014\u2015\u2212"), "-")}


def comparison_fold(s):
    """The fold every title comparison applies to BOTH sides before its own normalisation: tags
    stripped and references decoded, the apostrophe-like quotes (and U+00B4) removed outright,
    NFKD with the combining marks dropped (accents: Périard, Periard), Greek letters spelled out
    (U+03B2 is "beta", U+00B5 MICRO SIGN "mu"), double quotes to '"', the Unicode dashes (U+2010 to
    U+2015, U+2212) to "-", whitespace collapsed. Latin case is kept. A comparison form only:
    never a display form, and never a stored key or a filename (see filename_title)."""
    if not s:
        return ""
    s = unescape(strip_tags(str(s)))
    s = s.translate({0x00B4: "", 0x02BC: ""})            # before NFKD splits U+00B4 into space + accent
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.translate(_FOLD_MAP)
    return " ".join(s.split())


def filename_title(s):
    """The title form audit_filenames builds canonical names from: normalise_title as it was before
    the comparison fold (clean_field, lower-cased, punctuation spacing tidied). Kept stable on purpose:
    a filename is a stored key, and folding it (Greek names, dropped apostrophes, ASCII dashes) would
    turn every such file into a proposed rename."""
    s = clean_field(s).lower()
    s = _SPACE_BEFORE.sub(r"\1", s)
    s = _SPACE_AFTER.sub(r"\1", s)
    return " ".join(s.split())


def normalise_title(s):
    """comparison_fold, then clean_field, lower-cased, with no space before closing punctuation or
    after opening brackets: the comparison form of a title (never a display form, never a filename:
    see filename_title)."""
    return filename_title(comparison_fold(s))


# ---------------------------------------------------------------- body text of a text sidecar
# A text-only holding has to carry the article's body, not only its abstract. An AHA statistics
# report deposited as an author manuscript held 2,379 characters (the abstract and a pointer to the
# supplementary material) and was counted as full text (VAP, 2026-10-09). Across the 2,111 text-only
# sidecars in the libraries on 2026-10-09, every one with 1,144 body characters or fewer was an
# abstract, a stub or a truncated text; the next held 3,438 (a short comment).
TEXT_ONLY_MIN_BODY_CHARS = 1500
_NON_BODY_SECTION = re.compile(r"supplement|reference|acknowledg|funding|conflicts? of interest|competing interest|"
                               r"disclosure|author contribution|data availability|abbreviation", re.IGNORECASE)
_FLOAT_LINE = re.compile(r"^\s*\[(?:Figure|Fig\.?|Table|Box|Scheme|Supplementary)\b[^\]]*\]", re.IGNORECASE)


def _text_body(record, nonbody):
    """`text` less its title, abstract, non-body sections and the figure, caption and table lines the
    JATS and BioC parsers append ("[Figure 1.] caption", "[Table 1.] caption" then " | " rows)."""
    text = record.get("text") if isinstance(record.get("text"), str) else ""
    keep, in_table = [], False
    for ln in text.split("\n"):
        if _FLOAT_LINE.match(ln):
            in_table = ln.lstrip().lower().startswith("[table")
            continue
        if in_table and " | " in ln:
            continue
        in_table = False
        keep.append(ln)
    out = len("\n".join(keep))
    for field in ("abstract", "title"):
        v = record.get(field)
        out -= len(v) if isinstance(v, str) else 0
    return max(0, out - nonbody)


def body_chars(record):
    """Characters of article body in a text sidecar record: the larger of two measures, so neither
    a structured nor an unstructured full text is undercounted.
    * Sections: the text of every section that is not body (supplementary material, references,
      acknowledgements, funding, conflicts, disclosures, author contributions, data availability,
      abbreviations) left out. JATS and BioC records keep their body here.
    * Text: `text` less the title, the abstract, the non-body sections and the appended figure,
      caption and table lines. A merged record (OCR or PDF text of the whole article beside the
      sections of a JATS body that has no <sec>) keeps its body here."""
    if not isinstance(record, dict):
        return 0

    def n(v):
        return len(v) if isinstance(v, str) else 0
    sections = [s for s in (record.get("sections") or []) if isinstance(s, dict)]
    body = nonbody = 0
    for s in sections:
        if _NON_BODY_SECTION.search(s.get("title") or ""):
            nonbody += n(s.get("title")) + n(s.get("text"))
        else:
            body += n(s.get("text"))
    return max(body, _text_body(record, nonbody))


def is_abstract_only(record):
    """True when a text sidecar record holds less article body than TEXT_ONLY_MIN_BODY_CHARS: its
    text is an abstract, a stub or a truncated text, not a text-only holding."""
    return body_chars(record) < TEXT_ONLY_MIN_BODY_CHARS
