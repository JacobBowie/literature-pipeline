"""W1-B: litpipe.identity (the check that replaces the DOI-mismatch quarantine) and litpipe.text
(the field cleaner it and the metadata writers share).

Texts are synthetic first pages modelled on the V0 mismatch corpus shapes; no files, no network.
"""
import re
import unicodedata

import pytest

from litpipe import identity as I
from litpipe import text as T


# ================================================================ litpipe.text
def test_strip_tags_keeps_text_and_non_tags():
    assert T.strip_tags("<jats:p>Heat <i>in vivo</i> H<sub>2</sub>O</jats:p>").strip() == "Heat in vivo H2O"
    assert T.strip_tags('<jats:sec id="s1"><jats:title>Aim</jats:title>x</jats:sec>').split() == ["Aim", "x"]
    assert T.strip_tags("P<0.05 and x < y and z > w") == "P<0.05 and x < y and z > w"
    assert T.strip_tags("16:4<385::AID-SIM380>3.0.CO;2-3") == "16:4<385::AID-SIM380>3.0.CO;2-3"
    assert T.strip_tags("a<br/>b<!-- note -->c") == "a b c"


@pytest.mark.parametrize("raw,out", [
    ("&agr;-actinin and &bgr;-cells, &kgr; light chain", "α-actinin and β-cells, κ light chain"),
    ("M&uuml;ndel", "Mündel"),
    ("Physiology &amp; Behavior", "Physiology & Behavior"),
    ("P &amp;lt; 0.001", "P < 0.001"),                      # double-encoded: two passes
    ("&amp;amp;amp;", "&amp;"),                              # capped at two passes
    ("&#x3b1; &#945;", "α α"),
    ("&OV0312; stays", "&OV0312; stays"),                    # publisher-private: unknown, kept
    ("&parameter and AT&T and &copy2", "&parameter and AT&T and &copy2"),  # no legacy prefix decoding
])
def test_unescape(raw, out):
    assert T.unescape(raw) == out


def test_isogrk1_table_is_the_w3c_set():
    assert len(T.ISOGRK1) == 49
    for name, ch in T.ISOGRK1.items():
        assert unicodedata.name(ch).startswith("GREEK "), name
        assert ch.isupper() == name[0].isupper(), name
    assert T.ISOGRK1["agr"] == "α" and T.ISOGRK1["sfgr"] == "ς" and T.ISOGRK1["OHgr"] == "Ω"


# The four entity cases of the 2026-09-17 intake note (P1), applied the way crossref_meta will call
# the cleaner (W2-E1 wires it): each fails with the legacy tags-only strip and passes here.
MSG = {"container-title": ["Physiology &amp; Behavior"],
       "abstract": "<jats:p>Thirst rose (P &lt; 0.05); intake fell (P &gt; 0.05). Head &amp; neck cooling.</jats:p>"}
DOUBLE = {"container-title": ["Medicine &amp;amp; Science"],
          "abstract": "<jats:p>Significant (P &amp;lt; 0.001).</jats:p>"}
TRAP = "<jats:p>The tag &lt;jats:italic&gt; is literal.</jats:p>"


def _meta(msg):
    return {"container": T.clean_field(msg["container-title"][0]), "abstract": T.clean_field(msg["abstract"])}


def test_entity_case_1_single_encoded():
    m = _meta(MSG)
    assert m["container"] == "Physiology & Behavior"
    assert "P < 0.05" in m["abstract"] and "P > 0.05" in m["abstract"] and "Head & neck" in m["abstract"]


def test_entity_case_2_double_encoded():
    m = _meta(DOUBLE)
    assert m["container"] == "Medicine & Science" and "P < 0.001" in m["abstract"]


def test_entity_case_3_strip_before_decode_keeps_literal_text():
    assert "<jats:italic>" in T.clean_field(TRAP)


def test_entity_case_4_output_carries_no_references_and_author_stem_is_clean():
    m = _meta(MSG)
    assert not re.search(r"&(amp|lt|gt|quot|apos|#\d+);", m["abstract"] + m["container"])
    import ris_emit
    assert ris_emit.canonical_stem("2008", T.clean_field("M&uuml;ndel"), "Exercise heat stress").startswith("2008_Mundel_")


def test_clean_field_ligatures_spaces_and_case():
    assert T.clean_field("baroreﬂex ﬁbrosis eﬀect") == "baroreflex fibrosis effect"
    assert T.clean_field("heat stress and cold­ness ​ ") == "heat stress and coldness"
    assert T.clean_field("  Heat   <i>Stress</i>\n Review ") == "Heat Stress Review"


def test_clean_field_applies_nfkc():
    assert T.clean_field("Ｈｅａｔ µg m²") == "Heat μg m2"   # full-width, micro, superscript


def test_normalise_title():
    assert T.normalise_title("Heat Stress : a <i>Review</i> ( Part 1 ) ,  2020 !") == "heat stress: a review (part 1), 2020!"
    assert T.normalise_title(None) == "" and T.clean_field("") == ""


# ================================================================ litpipe.identity
PAGE = """J Exerc Nutrition Biochem. 2018;22(2):007-011, http://dx.doi.org/10.20463/jenb.2018.0010
Physical Activity and Nutrition. 2020;24(3):007-012, https://doi.org/10.20463/pan.2020.0015
Effects of endurance exercise under hypoxia on acid-base and ion balance in healthy males
[Purpose] This study was performed to investigate the acid-base balance.
"""


def test_check_ok_on_containment_not_first_doi():
    v = I.check(PAGE, "10.20463/PAN.2020.0015")                 # the template DOI comes first (V0-N1)
    assert v.decision == I.Decision.OK and v.ok and v.evidence["doi_match"] == "exact"


def test_check_ok_on_wrapped_and_letter_spaced_and_glued_text():
    assert I.check("doi: 10.1152/japplphysiol.\n00077.2010\nAbstract", "10.1152/japplphysiol.00077.2010").evidence[
        "doi_match"] == "wrapped"
    spaced = "h t t p : / / d x . d o i . o r g / 1 0 . 1 0 1 6 / j . j a c c . 2 0 1 7 . 0 7 . 7 6 3"
    assert I.check(spaced, "10.1016/j.jacc.2017.07.763").evidence["doi_match"] == "despaced"
    sage = "SPHXXX10.1177/19417381241298293Stearns et al.Sports Health"
    assert I.check(sage, "10.1177/19417381241298293").decision == I.Decision.OK
    assert I.check("doi:10.1161/CIR.0000000000000950.\nAuthor manuscript", "10.1161/cir.0000000000000950").ok


def test_check_does_not_accept_a_longer_doi_or_a_sibling():
    assert I.check("doi 10.36959/987/234 page", "10.36959/987/23").decision == I.Decision.FLAG
    assert I.check("doi 10.36959/987/233 page", "10.36959/987/234").decision == I.Decision.FLAG
    assert I.check("doi 10.1234/abc.12x", "10.1234/abc.12").decision == I.Decision.FLAG
    spaced = "d o i 1 0 . 3 6 9 5 9 / 9 8 7 / 2 3 4 R e c e i v e d"      # letter-spaced layer
    assert I.check(spaced, "10.36959/987/23").decision == I.Decision.FLAG
    assert I.check(spaced, "10.36959/987/234").evidence["doi_match"] == "despaced"


TITLE = "Effects of endurance exercise under hypoxia on acid-base and ion balance in healthy males"


def test_check_title_match_when_doi_absent():
    text = "Header\nEffects of endurance ex-\nercise under hypoxia on acid–base and ion\nbalance in healthy males\nBody"
    v = I.check(text, "10.9999/not.1234", queue_title=TITLE)
    assert v.decision == I.Decision.TITLE_MATCH and v.score >= 0.95 and v.evidence["title_source"] == "queue"
    v2 = I.check(text, None, ris_title=TITLE)
    assert v2.decision == I.Decision.TITLE_MATCH and v2.evidence["title_source"] == "ris"


def test_check_flags_a_different_paper_with_shared_words():
    # Oyewole shape: slug words all present, different paper (V0's word recall called it right).
    text = ("Scientific Reports (2020) 10:14790 https://doi.org/10.1038/s41598-020-71908-9\n"
            "Global, regional, and national burden and trend of diabetes in 195 countries and territories\n"
            "Diabetes mellitus type 2 disability burden physical activity ...")
    v = I.check(text, "10.12998/wjcc.v11.i14.3128",
                queue_title="Burden of disability in type 2 diabetes mellitus and the moderating effects of physical activity")
    assert v.decision == I.Decision.FLAG and v.score < I.TITLE_THRESHOLD
    assert "10.1038/s41598-020-71908-9" in v.evidence["pdf_dois"]


def test_threshold_is_respected():
    near = "Effects of endurance exercise under hypoxia on the acid-base balance of healthy men"
    s = I.title_similarity(TITLE, near)
    assert 0.6 < s < 0.95
    assert I.check(near, None, queue_title=TITLE, threshold=s).decision == I.Decision.TITLE_MATCH
    assert I.check(near, None, queue_title=TITLE, threshold=min(1.0, s + 0.01)).decision == I.Decision.FLAG


def test_title_anchors_do_not_depend_on_hash_seed():
    import subprocess
    import sys
    from pathlib import Path
    title = "alpha bravo charl delta echos foxes golfs hotel india julie kilos limas"   # 12 five-letter words
    code = ("import sys; sys.path.insert(0, sys.argv[1]); from litpipe import identity as I; "
            f"print(I._anchors({title!r}))")
    root = str(Path(__file__).resolve().parents[1])
    outs = {subprocess.run([sys.executable, "-c", code, root], capture_output=True, text=True,
                           env={**__import__("os").environ, "PYTHONHASHSEED": str(seed)}).stdout.strip()
            for seed in range(5)}
    assert outs == {str(sorted(title.split())[:8])}


def test_check_is_total_and_records_evidence():
    v = I.check("", "", None, None)
    assert v.decision == I.Decision.FLAG and v.evidence["reason"].startswith("no usable queue DOI")
    d = I.check(PAGE, "not-a-doi").as_dict()
    assert d["identity"] == "FLAG" and d["identity_evidence"]["queue_doi_invalid"] == "not-a-doi"
    assert set(I.check(PAGE, "10.20463/pan.2020.0015").as_dict()) == {"identity", "identity_score", "identity_evidence"}


# ---------------------------------------------------------------- doc_kind
@pytest.mark.parametrize("page,kind", [
    ("Supplementary Appendix\nThis appendix has been provided by the authors to give readers additional "
     "information about their work.\nSupplement to: Aune D, et al. BMJ 2016;353:i2156", I.DocKind.SUPPLEMENT),
    ("Online supplementary material for\nBMI and all cause mortality", I.DocKind.SUPPLEMENT),
    ("Supplemental Digital Content 1\nTable S1", I.DocKind.SUPPLEMENT),
    ("Supplementary Material\nTable S1. Baseline characteristics", I.DocKind.SUPPLEMENT),
    ("International Consensus on Blood Pressure\nHHS Public Access\nAuthor manuscript\n"
     "SUPPLEMENTARY MATERIALS\nSupplementary data to this article can be found online", I.DocKind.AAM),
    ("QUT Digital Repository: http://eprints.qut.edu.au/\nKing NA ...", I.DocKind.AAM),
    ("bioRxiv preprint doi: https://doi.org/10.1101/2025.10.20.683423", I.DocKind.AAM),
    ("© 2016 World Obesity 664 17, 664-690 obesity reviews doi: 10.1111/obr.12406", I.DocKind.VOR),
    ("Received: 15 June 2023 | Accepted: 15 June 2023 DOI: 10.1097/HEP.0000000000000520", I.DocKind.VOR),
    ("Some text with no furniture at all", I.DocKind.UNKNOWN),
    ("", I.DocKind.UNKNOWN),
])
def test_doc_kind(page, kind):
    assert I.doc_kind(page, 12) == kind


def test_doc_kind_empty_document():
    assert I.doc_kind("© 2020 Elsevier", 0) == I.DocKind.UNKNOWN


# ---------------------------------------------------------------- suspect_file
def test_suspect_file_rules():
    ok_stats = I.text_stats("A real line\n" + "word " * 4000, 10)
    assert not I.suspect_file(10, True, 0.9, ok_stats)
    assert I.suspect_file(2, True, 0.9, ok_stats).reasons == ("pages<=2",)
    assert I.suspect_file(10, False, 0.9, ok_stats).reasons == ("no_references_header",)
    assert I.suspect_file(10, True, 0.3, ok_stats).reasons == ("parsed_vs_expected<0.5",)
    wm = "\n".join(["Downloaded from journals.physiology.org/journal/jappl on September 16, 2026."] * 13)
    st = I.text_stats(wm, 13)
    assert st["top_line_share"] == 1.0 and st["chars_per_page"] < I.WATERMARK_CHARS_PER_PAGE
    assert I.suspect_file(13, None, None, st).reasons == ("watermark_only_text",)
    assert not I.suspect_file(None, None, None, None)            # unknowns are not held against a file
