"""W1-B: litpipe.doi (one DOI normaliser) and the lit_util wrappers that delegate to it.

Every literal shape named in the dispatch (T5 expanded, plan section 2.3) is a test here that fails
if the defect returns. No network: the resolver in resolve_first is a stub.
"""
import inspect
import os

import pytest

import lit_util
from litpipe import doi as D
from litpipe.outcomes import Kind, Outcome


# ---------------------------------------------------------------- literals -> the true DOI first
LITERALS = [
    # T5 literals (AGENT_PLANS T5, reproduced 2026-09-16)
    ("doi: 10.25258/ijddt.16.42s.103.\nI. Introduction", "10.25258/ijddt.16.42s.103"),
    ("10.2337/dc08-1876.\nIntroduction", "10.2337/dc08-1876"),
    ("https://doi.org/10.1016/j.tics.2024.02.003.author", "10.1016/j.tics.2024.02.003"),
    ("10.1007/s00484-019-01673-6.author", "10.1007/s00484-019-01673-6"),
    ("10.1186/s13059-014-0550-8.http://dx.doi.org/10.1186/s13059-014-0550-8", "10.1186/s13059-014-0550-8"),
    ("10.1234/jphys.2020.001.nih-pa", "10.1234/jphys.2020.001"),
    ("10.1234/jphys.2020.001.competing", "10.1234/jphys.2020.001"),
    ("10.1234/jphys.2020.001.published", "10.1234/jphys.2020.001"),
    ("10.1234/jphys.2020.001.temperature", "10.1234/jphys.2020.001"),
    # V2-N2 line-number / year glue (V0 repro v0_repro_v2_parser.py)
    ("doi: 10.1002/cphy.c100082.\n834 Next line of text", "10.1002/cphy.c100082"),
    ("https://doi.org/10.1371/journal.pone.0055660.\n2013 Another ref", "10.1371/journal.pone.0055660"),
    ("J Appl Physiol 138: 1-10, 2025. doi:10.1152/japplphysiol.00775.2024.\n12 First page",
     "10.1152/japplphysiol.00775.2024"),
    ("doi: 10.1152/japplphysiol.00775.2024\nIntroduction", "10.1152/japplphysiol.00775.2024"),
    # NEW-C11 SAGE running head: surname glued with no dot (09-17 intake note)
    ("SPHXXX10.1177/19417381241298293Stearns et al.Sports Health", "10.1177/19417381241298293"),
    ("10.1177/19417381241298293stearns", "10.1177/19417381241298293"),
    ("doi:10.1016/j.jsams.2019.03.005Smith et al.J Sci Med Sport", "10.1016/j.jsams.2019.03.005"),
    # V3-N3 fragment and invalid percent escape
    ("10.1056/nejmc1113675#sa3", "10.1056/nejmc1113675"),
    ("10.24871/1322012%p", "10.24871/1322012"),
    # URL-encoded DOI (single token) is decoded before capture
    ("https://doi.org/10.1000%2F123", "10.1000/123"),
    # a book DOI whose first print was broken by a layout line; the second print is whole
    ("doi: 10.1007/978-3-319-\nEnergetics of muscular exercise 05636-4\nPublication DOI:\n"
     "10.1007/978-3-319-05636-4", "10.1007/978-3-319-05636-4"),
    # PMC author-manuscript banner right after the DOI (the V0 ".hhs" capture)
    ("at https://doi.org/10.1016/j.amjmed.2022.12.015.\nHHS Public Access", "10.1016/j.amjmed.2022.12.015"),
]

# V0 mismatch corpus over-captures (pdf_doi_now -> the paper's DOI): 20 with a report row, plus
# the no-row shapes. Taken from v0_mismatch_titles.csv.
V0_OVERCAPTURES = [
    ("10.1038/nbt.4038.http://dx.doi.org/10.1038/nbt.4038.published", "10.1038/nbt.4038"),
    ("10.1080/02640414.2024.2391648.title", "10.1080/02640414.2024.2391648"),
    ("10.1152/japplphysiol.00077.2010.epub", "10.1152/japplphysiol.00077.2010"),
    ("10.1111/j.1753-0407.2011.00172.x.author", "10.1111/j.1753-0407.2011.00172.x"),
    ("10.1001/jama.2012.113905.author", "10.1001/jama.2012.113905"),
    ("10.3945/an.115.008615.energy", "10.3945/an.115.008615"),
    ("10.5812/asjsm.35201.research", "10.5812/asjsm.35201"),
    ("10.5812/ijp.7145.research", "10.5812/ijp.7145"),
    ("10.1186/s12933-018-0727-7.http://dx.doi.org/10.1186/s12933-018-0727-7.published",
     "10.1186/s12933-018-0727-7"),
    ("10.3389/fphys.2019.00031/research", "10.3389/fphys.2019.00031"),
    ("10.31031/epmr.2019.02.000546.copyright", "10.31031/epmr.2019.02.000546"),
    ("10.1007/s11886-020-1273-y.author", "10.1007/s11886-020-1273-y"),
    ("10.5888/pcd17.200020.peer", "10.5888/pcd17.200020"),
    ("10.1161/cir.0000000000000950.author", "10.1161/cir.0000000000000950"),
    ("10.5812/asjsm-130134.research", "10.5812/asjsm-130134"),
    ("10.5812/aapm-135081.research", "10.5812/aapm-135081"),
    ("10.1007/s00198-025-07485-2.author", "10.1007/s00198-025-07485-2"),
    ("10.1161/circresaha.125.325526.author", "10.1161/circresaha.125.325526"),
    ("10.5152/rss.2026.25101.received", "10.5152/rss.2026.25101"),
    ("10.1093/nar/gkv007.http://dx.doi.org/10.1093/nar/gkv007.published", "10.1093/nar/gkv007"),
    ("10.18705/2782-3806-2022-2-6-42-53.issn", "10.18705/2782-3806-2022-2-6-42-53"),
    ("10.1016/j.amjmed.2022.12.015.hhs", "10.1016/j.amjmed.2022.12.015"),
    ("10.1093/ajh/hpac104.author", "10.1093/ajh/hpac104"),
]


@pytest.mark.parametrize("raw,true", LITERALS + V0_OVERCAPTURES)
def test_literal_yields_true_doi_first(raw, true):
    assert D.candidates(raw)[0] == true
    assert D.normalise(raw) == true
    assert lit_util.extract_doi_from_text(raw) == true          # the walkers' entry point


@pytest.mark.parametrize("raw", ["10.1145/nnnnnnn.nnnnnnn", "doi: 10.1145/NNNNNNN.NNNNNNN.",
                                 "ref 10.1093/nar end", "10.1371/journal.pbio"])
def test_placeholders_and_truncations_are_flagged(raw):
    rej = []
    assert D.candidates(raw, rej) == []
    assert D.normalise(raw) is None
    assert any(why == "placeholder" for _, _, why in rej)
    assert lit_util.extract_doi_from_text(raw) == ""


def test_placeholder_is_skipped_for_the_real_doi_after_it():
    text = "Template: https://doi.org/10.1145/nnnnnnn.nnnnnnn\n...\nPublished DOI 10.1145/3809166\n"
    assert lit_util.extract_doi_from_text(text) == "10.1145/3809166"


def test_placeholder_drop_is_reported_once(capsys):
    lit_util._SUSPICIOUS_DROPPED.discard("10.1145/nnnnnnn.nnnnnnn")
    lit_util.extract_doi_from_text("doi 10.1145/nnnnnnn.nnnnnnn")
    lit_util.extract_doi_from_text("doi 10.1145/nnnnnnn.nnnnnnn")
    err = capsys.readouterr().err
    assert err.count("10.1145/nnnnnnn.nnnnnnn") == 1 and "suspicious" in err


@pytest.mark.parametrize("doi,suspicious", [
    ("10.1145/nnnnnnn.nnnnnnn", True),      # placeholder (was False: it has a dot)
    ("10.1145/3nnnnnn.1234567", True),      # a run of 5+ n
    ("10.1371/journal.pbio", True),         # no digit (was False: it has a dot)
    ("10.1002/cphy", True),
    ("10.1002/cphy.c140066", False),
    ("10.1111/j.1753-0407.2011.00172.x", False),
    ("10.1056/NEJMoa2034577", False),
    ("10.32614/CRAN.package.boot", False),  # case-insensitive CRAN rule
    ("not a doi", True),
])
def test_is_suspicious_doi_placeholder_rule(doi, suspicious):
    assert lit_util.is_suspicious_doi(doi) is suspicious


# Every no-digit-suffix DOI in portfolio.duckdb (candidates, paper_metadata, papers,
# paper_locations; read-only census 2026-09-30, 116 distinct), split into real and truncated.
# REAL: nested paths, CRAN packages, letter codes under 5-digit registrants. Two OSF DOIs are HELD.
REAL_NO_DIGIT = [
    # OSF family (14; 10.31234/osf.io/kbyhm and 10.31236/osf.io/wubps are held in libraries)
    "10.17605/osf.io/znkge", "10.31219/osf.io/aqxer", "10.31219/osf.io/stakv",
    "10.31234/osf.io/kbyhm", "10.31234/osf.io/ksfvq", "10.31234/osf.io/ryspq",
    "10.31234/osf.io/spreb", "10.31234/osf.io/wgtpm", "10.31236/osf.io/ruybs",
    "10.31236/osf.io/wpvek", "10.31236/osf.io/wubps", "10.31236/osf.io/wugcr",
    "10.31236/osf.io/ycwvj", "10.31236/osf.io/yebmr",
    # CRAN packages
    "10.32614/cran.package.boot", "10.32614/cran.package.hmisc", "10.32614/cran.package.tidyr",
    # nucleodoconhecimento (18)
    *("10.32749/nucleodoconhecimento.com.br/" + p for p in (
        "bildung-de/psychophysiologische-beitraege", "ciencias-aeronauticas/transporte-aeromedico",
        "educacao-fisica/papel-do-personal-trainer", "educacao-fisica/treinamento-aerobico",
        "educacao-fisica/treinamento-com-restricao", "educacao/contribuicoes-psicofisiologicas",
        "educacion-es/contribuciones-psicofisiologicas", "education-fr/contributions-psychophysiologiques",
        "education/psychophysiological-contributions", "formazione-it/contributi-psicofisiologici",
        "gesundheit/akupunktur-in-pravention", "health/acupuncture-in-prevention",
        "health/generalized-anxiety", "salud/acupuntura-en-la-prevencion", "salud/ansiedad-generalizada",
        "salute/agopuntura-in-prevenzione", "sante/acupuncture-en-prevention",
        "saude/ansiedade-generalizada")),
    # other nested paths
    "10.35566/jbds/jikim", "10.59327/wmo/s/cri/soc/arab",
    # bare letter codes, all under 5-digit registrants
    "10.29007/scnh", "10.29007/ssjv", "10.24891/yzkiey", "10.61955/rtlekc", "10.70312/zikm",
    "10.26686/yare-mwnp", "10.65204/djms-may-tdansr", "10.65204/djms-may-afl-spp-da",
    "10.66968/pejuang", "10.67678/rxpxjemh", "10.64650/human-physiology",
    # dotted, no digit, registered at doi.org (2026-09-30): 5-digit registrants, and Ubiquity
    # Press (10.5334) letter codes; the 4-digit clause would otherwise flag the last two
    "10.37204/bioconversion.organic.waste", "10.67159/iaedu.seven-global-ai-challenges",
    "10.5334/jors.bi", "10.5334/bbe.c",
]
# TRUNCATED: journal codes cut at a segment boundary, glued words, bare tokens under 4-digit
# registrants (the 2026-06-05 audit reproduced 10.1002/cphy and 10.1001/archinte), placeholders.
TRUNCATED_NO_DIGIT = [
    *("10.1016/j." + c for c in (
        "amepre", "amjca", "amjmed", "archger", "arti", "ather", "autne", "autrev", "bpj", "buildenv",
        "celrep", "cmpb", "cpr", "ex", "gait", "gaitpost", "ijan", "ijca", "ijcard", "in", "ja", "jacbt",
        "jado", "jchf", "jchro", "jcm", "jncc", "jpeds", "jpeds.multicomponent", "jsa", "jsam", "jsams",
        "jther", "jtherbio", "mayocp", "molcel", "neubi", "neuro", "newideapsych", "nut", "physb",
        "physbeh", "rehab", "ridd", "ridd.down", "ridd.intensive", "scisp", "soard", "wem", "yebeh")),
    "10.1002/jcb.exercise-induced", "10.1002/mus.production", "10.1002/pmj.a.m.alsugair",
    "10.1021/acs.jprot", "10.1111/crj.allen", "10.1123/apaq.sports", "10.1152/japplphysiol.applied",
    "10.1249/mss.m", "10.1371/journal.adults", "10.1371/journal.pone", "10.1371/journal.pbio",
    "10.3760/cma.j.issn", "10.4103/cjp.cjp_",
    # registered (doi.org 2026-09-30) but indistinguishable from a template: the rule's one
    # known false flag, kept here so a change to it is deliberate
    "10.5779/hypothesis.vxxix.xxxx",
    "10.1002/cphy", "10.1001/archinte", "10.1093/nar",
    "10.1145/nnnnnnn.nnnnnnn",
]


@pytest.mark.parametrize("doi", REAL_NO_DIGIT)
def test_real_no_digit_dois_are_kept(doi):
    assert not D.is_placeholder(doi)
    assert not lit_util.is_suspicious_doi(doi)
    assert D.candidates(doi) == [doi]                       # not peeled, not dropped
    assert D.normalise(f"see https://doi.org/{doi} for the preprint") == doi
    assert lit_util.extract_doi_from_text(f"doi: {doi}\nIntroduction") == doi


@pytest.mark.parametrize("doi", TRUNCATED_NO_DIGIT)
def test_truncated_no_digit_dois_are_flagged(doi):
    assert D.is_placeholder(doi)
    assert lit_util.is_suspicious_doi(doi)
    assert D.candidates(doi) == []


def test_fixture_lists_cover_the_census():
    assert len(REAL_NO_DIGIT) == 52 and len(set(REAL_NO_DIGIT)) == 52
    assert not set(REAL_NO_DIGIT) & set(TRUNCATED_NO_DIGIT)


# ---------------------------------------------------------------- line wraps (the 2026-06-05 behaviour stays)
@pytest.mark.parametrize("text,true", [
    ("see 10.1002/cphy.\nc140066 here", "10.1002/cphy.c140066"),           # RC1: wrap after '.'
    ("10.1111/j.1748-1716.2006.\n01550.x", "10.1111/j.1748-1716.2006.01550.x"),
    ("10.1152/japplphysiol.\n00775.2024", "10.1152/japplphysiol.00775.2024"),  # suffix had no digit yet
    ("10.1097/00005768-199405000-\n00005 x", "10.1097/00005768-199405000-00005"),  # wrap after '-'
    ("doi:10.1016/\nS0140-6736(20)30183-5).", "10.1016/s0140-6736(20)30183-5"),   # wrap after '/'
    ("10.1002/14651858.\nCD001234.pub2", "10.1002/14651858.cd001234.pub2"),    # capital + digits joins
    ("10.1234/abc.­2020.001", "10.1234/abc.2020.001"),                     # soft hyphen
    ("see 10.1002/cphy.c140066 here", "10.1002/cphy.c140066"),
])
def test_wrapped_dois_are_rejoined(text, true):
    assert lit_util.extract_doi_from_text(text) == true


def test_refused_line_number_glue_is_kept_as_a_fallback():
    cands = D.candidates("doi:10.1152/japplphysiol.00775.2024.\n12 First page")
    assert cands == ["10.1152/japplphysiol.00775.2024", "10.1152/japplphysiol.00775.2024.12"]


def test_max_chars_semantics_unchanged():
    text = "see 10.1002/cphy.c140066 here"
    assert lit_util.extract_doi_from_text(text, max_chars=0) == ""
    assert lit_util.extract_doi_from_text(text, max_chars=5) == ""
    assert lit_util.extract_doi_from_text(None) == ""
    assert isinstance(lit_util.extract_doi_from_text("x 10.1002/cphy.c140066"), str)
    assert list(inspect.signature(lit_util.extract_doi_from_text).parameters) == ["text", "max_chars"]


# ---------------------------------------------------------------- shape details
def test_sici_angle_brackets_and_balanced_parens_survive():
    sici = "10.1002/(SICI)1097-0258(19970228)16:4<385::AID-SIM380>3.0.CO;2-3"
    assert D.normalise(f"ref {sici}, 1997") == sici.lower()
    assert D.normalise("(doi:10.1016/S0140-6736(20)30183-5)") == "10.1016/s0140-6736(20)30183-5"
    assert D.normalise("x <a>10.1234/abc.1</a>") == "10.1234/abc.1"      # non-SICI: '<' ends it


def test_unicode_hyphen_and_ligature_in_extracted_doi():
    assert D.normalise("10.1007/s00421‐011‐2011‐y") == "10.1007/s00421-011-2011-y"
    assert D.normalise("10.1016/j.ﬁeld.2020.01.001") == "10.1016/j.field.2020.01.001"


def test_markdown_escaped_doi():
    assert D.normalise(r"10.1002/\(SICI\)1097-0258") == "10.1002/(sici)1097-0258"


def test_real_alpha_tail_kept_first_and_peeled_form_offered():
    assert D.candidates("10.1002/9781118915455.index") == ["10.1002/9781118915455.index",
                                                            "10.1002/9781118915455"]
    assert D.normalise("10.1111/j.1600-0838.2010.01203.x") == "10.1111/j.1600-0838.2010.01203.x"


def test_raw_numeric_glue_offers_the_stripped_form():
    assert D.candidates("10.1101/gad.211649.112.234") == ["10.1101/gad.211649.112.234",
                                                           "10.1101/gad.211649.112"]


def test_occurrences_in_text_order_template_first():
    text = "J Exerc Nutrition Biochem. http://dx.doi.org/10.20463/jenb.2018.0010\n7\n" \
           "Physical Activity and Nutrition. https://doi.org/10.20463/pan.2020.0015\n"
    assert D.candidates(text) == ["10.20463/jenb.2018.0010", "10.20463/pan.2020.0015"]


def test_doi_module_is_pure():
    src = inspect.getsource(D)
    assert "import lit_util" not in src and "requests" not in src and "urlopen" not in src


# ---------------------------------------------------------------- encode_path
def test_encode_path_normalises_before_encoding():
    assert D.encode_path("10.1056/NEJMc1113675#sa3") == "10.1056/nejmc1113675"   # not %23sa3 (V3-N3)
    assert D.encode_path("https://doi.org/10.1016/j.tics.2024.02.003.author") == "10.1016/j.tics.2024.02.003"


def test_encode_path_handbook_4_7():
    sici = "10.1002/(SICI)1097-0258(19970228)16:4<385::AID-SIM380>3.0.CO;2-3"
    assert D.encode_path(sici) == "10.1002/(sici)1097-0258(19970228)16:4%3C385::aid-sim380%3E3.0.co;2-3"
    assert D.encode_path("10.1093/nar/gkv007") == "10.1093/nar/gkv007"
    assert D.encode_path("10.1093/nar/gkv007", strict=True) == "10.1093/nar%2Fgkv007"


@pytest.mark.parametrize("bad", ["", "not a doi", "10.1145/nnnnnnn.nnnnnnn"])
def test_encode_path_refuses_non_dois(bad):
    with pytest.raises(ValueError):
        D.encode_path(bad)


# ---------------------------------------------------------------- resolve_first
TRUE_DOIS = {t for _, t in LITERALS + V0_OVERCAPTURES} | {"10.1101/gad.211649.112"}


@pytest.mark.parametrize("raw,true", LITERALS + V0_OVERCAPTURES + [("10.1101/gad.211649.112.234",
                                                                   "10.1101/gad.211649.112")])
def test_every_literal_resolves_through_a_stub(raw, true):
    assert D.resolve_first(D.candidates(raw), lambda d: d in TRUE_DOIS) == true


def test_resolve_first_outcome_kinds():
    seen = []

    def resolver(d):
        seen.append(d)
        return {"10.1/a1": Outcome(Kind.NO_MATCH), "10.1/b2": Outcome(Kind.NOT_AT_RA)}.get(d, Outcome(Kind.OK))
    assert D.resolve_first(["10.1/a1", "10.1/b2", "10.1/c3"], resolver) == "10.1/b2"
    assert seen == ["10.1/a1", "10.1/b2"]


def test_resolve_first_failed_call_is_not_a_no():
    calls = []

    def resolver(d):
        calls.append(d)
        return Outcome(Kind.TRANSPORT, detail="timeout")
    with pytest.raises(D.ResolverUnavailable) as ei:
        D.resolve_first(["10.1/a1", "10.1/b2"], resolver)
    assert ei.value.candidate == "10.1/a1" and calls == ["10.1/a1"]   # no fall-through


def test_resolve_first_all_refuted_is_none():
    assert D.resolve_first(["10.1/a1"], lambda d: None) is None
    assert D.resolve_first([], lambda d: True) is None


# ---------------------------------------------------------------- DEC-13
def test_default_email_has_no_code_default():
    assert hasattr(lit_util, "DEFAULT_EMAIL") and lit_util.DEFAULT_EMAIL is None


# ---------------------------------------------------------------- lit_util atomic writes (W1-B owns lit_util)
def _flaky_replace(monkeypatch, fail_times):
    real = os.replace
    calls = []

    def fake(src, dst):
        calls.append((src, dst))
        if fail_times is None or len(calls) <= fail_times:
            raise PermissionError(13, "The process cannot access the file (WinError 5 stand-in)")
        return real(src, dst)
    monkeypatch.setattr(lit_util.os, "replace", fake)
    monkeypatch.setattr(lit_util.time, "sleep", lambda s: None)
    return calls


@pytest.mark.parametrize("writer", ["text", "csv"])
def test_atomic_write_retries_a_transient_permission_error(tmp_path, monkeypatch, writer):
    calls = _flaky_replace(monkeypatch, fail_times=2)
    target = tmp_path / "out.csv"
    if writer == "text":
        lit_util.atomic_write_text(str(target), "a,b\n1,2\n")
    else:
        lit_util.atomic_write_csv(str(target), [{"a": "1", "b": "2"}], fieldnames=["a", "b"])
    assert target.read_text(encoding="utf-8") == "a,b\n1,2\n"
    assert len(calls) == 3
    assert list(tmp_path.glob("*.tmp")) == []


@pytest.mark.parametrize("writer", ["text", "csv"])
def test_atomic_write_gives_up_after_five_attempts(tmp_path, monkeypatch, writer):
    calls = _flaky_replace(monkeypatch, fail_times=None)
    target = tmp_path / "out.csv"
    with pytest.raises(PermissionError):
        if writer == "text":
            lit_util.atomic_write_text(str(target), "x")
        else:
            lit_util.atomic_write_csv(str(target), [{"a": "1"}], fieldnames=["a"])
    assert len(calls) == 5
    assert not target.exists()
    assert list(tmp_path.glob("*.tmp")) == []


def test_atomic_write_does_not_retry_other_errors(tmp_path, monkeypatch):
    calls = []

    def fake(src, dst):
        calls.append(src)
        raise FileNotFoundError(2, "gone")
    monkeypatch.setattr(lit_util.os, "replace", fake)
    with pytest.raises(FileNotFoundError):
        lit_util.atomic_write_text(str(tmp_path / "x.txt"), "x")
    assert len(calls) == 1 and list(tmp_path.glob("*.tmp")) == []
