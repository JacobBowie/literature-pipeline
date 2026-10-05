"""litpipe.text.display_field: the shared display cleaner (W2-E1 forward 8, for W2-E2's abstracts and
backfill_ris). It must equal ris_emit's display form exactly, so `.ris` fields and abstract columns
read the same; it differs from clean_field only by NFC instead of NFKC (plus U+2011)."""
import pytest

import ris_emit
from litpipe import text

CASES = [
    "P &lt; 0.05",
    "M&uuml;ndel",
    "<jats:p>Heat <i>acclimation</i></jats:p><jats:p>and VO<sub>2</sub>max</jats:p>",
    "&amp;lt;b&amp;gt; literal",                    # double-escaped: decodes to text, not a tag
    "&agr;-actinin and &bgr;-cells",                # ISO Greek isogrk1
    "VO₂max in m² at 5 µg",          # subscripts, superscripts, micro sign kept
    "D´Amico",                                 # U+00B4 kept (NFKC would make it ' ́')
    "baroreﬂex reﬁnement",                # ligatures expanded
    "high‑intensity",                          # non-breaking hyphen made "-"
    "soft­hyphen zero​width joiner⁠bom﻿",
    "no break thin narrow  spaces\n\t",
    "éclair",                                 # combining sequence composes under NFC
    "",
    None,
]


@pytest.mark.parametrize("s", CASES)
def test_display_field_equals_ris_emit_display(s):
    assert text.display_field(s) == ris_emit._display(s)


def test_display_field_keeps_compatibility_characters_that_clean_field_folds():
    s = "VO₂max 5 µg m² D´Amico"
    assert text.display_field(s) == s
    assert text.clean_field(s) != s                 # NFKC folds them: the reason two forms exist


def test_display_field_cleans_markup_entities_and_spacing():
    assert text.display_field("<jats:p>P &lt; 0.05</jats:p>") == "P < 0.05"
    assert text.display_field("M&uuml;ndel") == "Mündel"
    assert text.display_field("&amp;lt;i&amp;gt; is literal") == "<i> is literal"
    assert text.display_field("baroreﬂex") == "baroreflex"
    assert text.display_field("high‑intensity") == "high-intensity"
    assert text.display_field("a b c­x") == "a b cx"
    assert text.display_field("é") == "é"
    assert text.display_field(None) == "" and text.display_field("") == ""


def test_display_field_is_exported():
    assert "display_field" in text.__all__


# ---------------------------------------------------------------- abstract_field (W2-E2 forward 2)
@pytest.mark.parametrize("raw,want", [
    ("&lt;b&gt;&lt;i&gt;Purpose:&lt;/i&gt;&lt;/b&gt; To test.", "Purpose: To test."),
    ("&amp;lt;jats:p&amp;gt;Aim p&amp;amp;lt;0,05&amp;lt;/jats:p&amp;gt;", "Aim p<0,05"),
    ("ABSTRACT In addition, heat.", "In addition, heat."),
    ("<jats:title>Abstract</jats:title><jats:p>Body text.</jats:p>", "Body text."),
    ("AbstractBackground: x", "Background: x"),
    ("Abstract", ""),
])
def test_abstract_field_strips_escaped_markup_and_the_heading(raw, want):
    assert text.abstract_field(raw) == want
    assert text.abstract_field(want) == want                     # idempotent


@pytest.mark.parametrize("s", ["Abstracts of the annual meeting", "ABSTRACTION of signals", "VO\u2082max in m\u00b2"])
def test_abstract_field_keeps_text_that_is_not_a_heading(s):
    assert text.abstract_field(s) == s


def test_ris_writer_abstract_uses_abstract_field():
    """The .ris AB line is cleaned like the DB abstract (it carried `<b><i>Purpose:` and the heading)."""
    msg = {"DOI": "10.1000/x1", "title": ["A title"], "type": "journal-article",
           "abstract": "<jats:title>Abstract</jats:title><jats:p>&lt;b&gt;Purpose:&lt;/b&gt; To test.</jats:p>"}
    meta = ris_emit.crossref_meta(msg)
    assert meta["abstract"] == "Purpose: To test."
    assert "AB  - Purpose: To test." in ris_emit.build_ris(meta)
