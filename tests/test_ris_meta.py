"""The metadata writer's field rules (W2-E1): subtitles (REG-I49), year precedence (plan 4.4),
entities and tags (2026-09-17 intake note P1), display normalisation (no lossy NFKC), UR encoding
(N-A8), aliases (N-A3). Offline: Crossref messages are live-probed fixtures (tests/fixtures/W2-E1,
2026-09-30) or synthesised."""
import json
import re
from pathlib import Path

import pytest

import ris_emit as R
from litpipe import text as T

FIX = Path(__file__).resolve().parent / "fixtures" / "W2-E1"


def message(name):
    return json.loads((FIX / name).read_text(encoding="utf-8"))["body"]["message"]


# ---------------------------------------------------------------- subtitles (REG-I49)
@pytest.mark.parametrize("name, title", [
    ("crossref_casa_subtitle.json", "Exertional Heat Stroke: New Concepts Regarding Cause and Care"),
    ("crossref_ketko_subtitle.json",
     "The thermal-circulatory ratio (TCR): An index to evaluate the tolerance to heat"),
])
def test_subtitle_is_kept(name, title):
    msg = message(name)
    assert msg["subtitle"]                                   # the registry holds it separately
    m = R.crossref_meta(msg)
    assert m["title"] == title
    assert f"TI  - {title}\n" in R.build_ris(m)


def test_title_without_subtitle_is_unchanged():
    msg = message("crossref_butler_print_online.json")
    assert R.crossref_meta(msg)["title"] == "Current Clinical Concepts: Heat Tolerance Testing"


@pytest.mark.parametrize("title, sub, out", [
    ("Main", "", "Main"),
    ("Main", "Sub", "Main: Sub"),
    ("Why heat?", "A review", "Why heat?: A review"),       # ": " unconditionally (plan 4.4)
    ("Main:", "Sub", "Main: Sub"),                          # never "::"
    ("Main: Sub", "Sub", "Main: Sub"),                      # a title that already carries it
    ("", "Sub", "Sub"),
    ("Heat &amp; <i>cold</i>", "P &lt; 0.05", "Heat & cold: P < 0.05"),
])
def test_join_title(title, sub, out):
    assert R.join_title(title, sub) == out


def test_only_the_first_title_and_subtitle_entries_are_used():
    msg = {"DOI": "10.1234/abc.1", "title": ["Main", "Titre traduit"], "subtitle": ["Sub", "Sous-titre"]}
    assert R.crossref_meta(msg)["title"] == "Main: Sub"


# ---------------------------------------------------------------- year precedence (plan 4.4)
def test_live_record_with_online_and_print_dates_gives_the_print_year():
    msg = message("crossref_butler_print_online.json")
    assert msg["issued"]["date-parts"][0][0] == 2021          # online first
    assert msg["published-print"]["date-parts"][0][0] == 2023
    m = R.crossref_meta(msg)
    assert (m["year"], m["date"]) == ("2023", "2023/02/01")
    assert "PY  - 2023\n" in R.build_ris(m)


def _dp(*parts):
    return {"date-parts": [list(parts)]}


# Synthesised from the four online-first cases of a 2026-09-27 manuscript build (the issue year is
# right, issued is a year early): published-print missing, the issue's print date present.
@pytest.mark.parametrize("print_year, online_year", [(2023, 2021), (2021, 2020), (2022, 2021), (2022, 2021)])
def test_journal_issue_print_date_beats_online_first(print_year, online_year):
    msg = {"DOI": "10.1234/abc.1", "title": ["T"],
           "published-online": _dp(online_year, 11, 18), "published": _dp(online_year, 11, 18),
           "issued": _dp(online_year, 11, 18),
           "journal-issue": {"issue": "2", "published-online": _dp(online_year, 11, 18),
                             "published-print": _dp(print_year, 2)}}
    m = R.crossref_meta(msg)
    assert (m["year"], m["date"]) == (str(print_year), f"{print_year}/02")


def test_published_beats_issued_and_null_parts_fall_through():
    msg = {"DOI": "10.1234/abc.1", "title": ["T"], "published-print": {"date-parts": [[None]]},
           "published": _dp(2019, 3), "issued": _dp(2018)}
    assert R.crossref_meta(msg)["year"] == "2019"
    msg = {"DOI": "10.1234/abc.1", "title": ["T"], "issued": {"date-parts": [[None]]}}
    m = R.crossref_meta(msg)
    assert (m["year"], m["date"]) == ("", "")
    assert "PY  -" not in R.build_ris(m)


def test_print_date_wins_over_everything():
    msg = {"DOI": "10.1234/abc.1", "title": ["T"], "published-print": _dp(2014, 9, 30),
           "journal-issue": {"issue": "2", "published-print": _dp(2015)},
           "published": _dp(2014, 7, 2), "issued": _dp(2014, 7, 2)}
    assert R.crossref_meta(msg)["date"] == "2014/09/30"


# ---------------------------------------------------------------- entities and tags (intake P1)
MSG = {"DOI": "10.1016/0031-9384(87)90212-5",
       "title": ["Thirst and fluid intake following graded hypohydration levels in humans"],
       "container-title": ["Physiology &amp; Behavior"],
       "issued": {"date-parts": [[1987]]},
       "author": [{"family": "Engell", "given": "Dianne B."}],
       "volume": "40", "page": "229-236", "type": "journal-article",
       "abstract": "<jats:p>Thirst rose (P &lt; 0.05); intake fell (P &gt; 0.05). "
                   "Head &amp; neck cooling.</jats:p>"}
DOUBLE = dict(MSG, **{"container-title": ["Medicine &amp;amp; Science"],
                      "abstract": "<jats:p>Significant (P &amp;lt; 0.001).</jats:p>"})
TRAP = dict(MSG, abstract="<jats:p>The tag &lt;jats:italic&gt; is literal.</jats:p>")


def test_single_encoded():
    m = R.crossref_meta(MSG)
    assert m["container"] == "Physiology & Behavior"
    assert "P < 0.05" in m["abstract"] and "P > 0.05" in m["abstract"]
    assert "Head & neck" in m["abstract"]
    assert not re.search(r"&(amp|lt|gt|quot|apos|#\d+);", m["abstract"] + m["container"])


def test_double_encoded():
    m = R.crossref_meta(DOUBLE)
    assert m["container"] == "Medicine & Science"
    assert "P < 0.001" in m["abstract"]


def test_strip_before_decode_keeps_literal_text():
    m = R.crossref_meta(TRAP)
    assert "<jats:italic>" in m["abstract"]
    assert "AB  - The tag <jats:italic> is literal." in R.build_ris(m)


def test_build_ris_is_clean():
    ris = R.build_ris(R.crossref_meta(MSG))
    assert "JO  - Physiology & Behavior" in ris
    assert "&lt;" not in ris and "&amp;" not in ris


def test_live_author_entity_reaches_neither_ris_nor_stem():
    msg = message("crossref_mundel_entity.json")
    assert msg["author"][0]["family"] == "M&uuml;ndel"        # as Crossref serves it (2026-09-30)
    m = R.crossref_meta(msg)
    assert m["lastname"] == "Mündel"
    assert R.canonical_stem(m["year"], m["lastname"], m["title"]).startswith("2008_Mundel_")
    assert "AU  - Mündel, Toby" in R.build_ris(m)


def test_canonical_stem_decodes_raw_input_too():
    assert R.canonical_stem("2008", "M&uuml;ndel", "Exercise &amp; heat").startswith("2008_Mundel_Exercise")


def test_title_markup_is_stripped():
    msg = {"DOI": "10.1234/abc.1", "title": ["Familial aggregation of V˙<scp>o</scp><sub>2max</sub> response"],
           "subtitle": ["the <i>HERITAGE</i> Family Study"]}
    assert R.crossref_meta(msg)["title"] == ("Familial aggregation of V˙o2max response: "
                                            "the HERITAGE Family Study")


def test_every_string_field_is_cleaned():
    msg = {"DOI": "10.1234/abc.1", "title": ["A &amp; B"], "subtitle": ["C &lt; D"],
           "container-title": ["J &amp; K"], "abstract": "<p>x &gt; y</p>",
           "author": [{"family": "O&apos;Brien", "given": "Se&aacute;n"}]}
    m = R.crossref_meta(msg)
    assert (m["title"], m["container"], m["abstract"]) == ("A & B: C < D", "J & K", "x > y")
    assert m["authors"] == [{"family": "O'Brien", "given": "Seán"}] and m["lastname"] == "O'Brien"


# ---------------------------------------------------------------- display normalisation (amendment 2)
@pytest.mark.parametrize("raw", [
    "Reduction in Maximal Oxygen Consumption (VO₂max) During Tamsulosin Use",
    "Body mass index (kg/m²) and 5 µg doses",
    "Iso-Inertial (YoYo™) Resistance Exercise",
    "Modulating CD8⁺ T cell immunity",
    "Bose-O´Reilly",
    "減輕：運動（DIC）",               # fullwidth CJK punctuation
])
def test_display_keeps_compatibility_characters(raw):
    assert R._display(raw) == raw                            # NFKC would change every one of these
    assert T.clean_field(raw) != raw                         # (the comparison form still folds them)


def test_display_expands_ligatures_and_plain_spaces_like_clean_field():
    raw = "baroﬂex control  of heat­ loss ​"
    assert R._display(raw) == T.clean_field(raw) == "baroflex control of heat loss"
    assert R._display("high‑intensity") == "high-intensity"
    assert R._display("Café") == "Café"                  # NFC composes


@pytest.mark.parametrize("raw", ["Plain title", "A &amp; B <i>C</i>", "Naïve Müller – test",
                                 "P &lt; 0.05 &agr;-actin", "  spaced\n\nout  "])
def test_display_equals_clean_field_when_nfkc_changes_nothing_else(raw):
    assert R._display(raw) == T.clean_field(raw)


def test_title_keeps_subscripts_in_the_ris():
    msg = {"DOI": "10.1234/abc.1", "title": ["Evaluating VO₂max in m²"]}
    assert "TI  - Evaluating VO₂max in m²\n" in R.build_ris(R.crossref_meta(msg))


# ---------------------------------------------------------------- DOIs in URLs (N-A8) and aliases (N-A3)
def test_ur_line_is_normalised_then_encoded():
    sici = "10.1002/(SICI)1097-4636(199706)35:4<401::AID-JBM1>3.0.CO;2-X"
    m = R.crossref_meta({"DOI": sici, "title": ["T"]})
    assert m["url"] == "https://doi.org/10.1002/(sici)1097-4636(199706)35:4%3C401::aid-jbm1%3E3.0.co;2-x"
    assert f"UR  - {m['url']}\n" in R.build_ris(m)


def test_build_ris_reencodes_a_raw_doi_org_url_from_another_writer():
    meta = {"doi": "10.1002/(sici)1097-4636(199706)35:4<401::aid-jbm1>3.0.co;2-x", "title": "T",
            "url": "https://doi.org/10.1002/(sici)1097-4636(199706)35:4<401::aid-jbm1>3.0.co;2-x"}
    assert "%3C401" in R.build_ris(meta) and "<401" not in R.build_ris(meta).split("UR  - ")[1]
    other = {"doi": "10.48550/arxiv.2506.02153", "title": "T", "url": "https://arxiv.org/abs/2506.02153"}
    assert "UR  - https://arxiv.org/abs/2506.02153\n" in R.build_ris(other)


def test_alias_record_writes_the_prime_doi():
    msg = message("crossref_alias_prime.json")                # fetched with the alias DOI
    m = R.crossref_meta(msg)
    assert m["doi"] == "10.5790/hongkong/9789888528011.003.0007"
    ris = R.build_ris(m)
    assert "DO  - 10.5790/hongkong/9789888528011.003.0007\n" in ris
    assert "UR  - https://doi.org/10.5790/hongkong/9789888528011.003.0007\n" in ris


# ---------------------------------------------------------------- authors and types
def test_organisation_author_is_kept():
    msg = {"DOI": "10.1234/abc.1", "title": ["Consensus"],
           "author": [{"name": "American College of Sports Medicine"}, {"family": "Roberts", "given": "W"}]}
    m = R.crossref_meta(msg)
    assert m["lastname"] == "American College of Sports Medicine"
    assert "AU  - American College of Sports Medicine\n" in R.build_ris(m)


@pytest.mark.parametrize("ctype, ris", [("dissertation", "THES"), ("edited-book", "EDBOOK"),
                                        ("book-section", "CHAP"), ("journal-article", "JOUR"),
                                        ("peer-review", "JOUR")])
def test_crossref_types_map_to_ris_types(ctype, ris):
    assert R.build_ris(R.crossref_meta({"DOI": "10.1234/abc.1", "title": ["T"], "type": ctype})).startswith(f"TY  - {ris}")


# ---------------------------------------------------------------- content negotiation records
def test_csl_medra_record():
    csl = json.loads((FIX / "csl_medra.json").read_text(encoding="utf-8"))["body"]
    m = R.csl_meta(csl)
    assert m["doi"] == "10.3305/nh.2015.31.3.8434" and m["year"] == "2015" and m["date"] == "2015/03/01"
    assert m["container"] == "NUTRICION HOSPITALARIA" and m["type"] == "journal-article"
    ris = R.build_ris(m)
    assert "SP  - 1217\n" in ris and "EP  - 1224\n" in ris and "AU  - Prieto, Jose Antonio" in ris


def test_csl_jalc_record_without_type_and_with_number():
    csl = json.loads((FIX / "csl_jalc.json").read_text(encoding="utf-8"))["body"]
    assert "type" not in csl and csl["number"] == "4"          # JaLC, live 2026-09-30
    m = R.csl_meta(csl)
    assert m["type"] == "journal-article" and m["issue"] == "4" and m["year"] == "2022"
    assert m["container"] == "Nagoya Journal of Medical Science"


def test_csl_datacite_record_uses_publisher_when_no_container():
    csl = json.loads((FIX / "csl_datacite_arxiv_v3.json").read_text(encoding="utf-8"))["body"]
    m = R.csl_meta(csl)
    assert m["doi"] == csl["DOI"].lower()                       # DataCite CSL upper-cases DOIs
    assert m["container"] and m["title"]


def test_csl_crossref_record_matches_crossref_meta_shape():
    csl = json.loads((FIX / "csl_crossref_v3.json").read_text(encoding="utf-8"))["body"]
    m = R.csl_meta(csl)
    assert m["year"] == "2025" and m["date"] == "2025/03/01" and m["page"] == "634-650"
    assert m["container"] == "Journal of Applied Physiology" and m["type"] == "journal-article"
