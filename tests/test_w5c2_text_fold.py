"""W5-C2 step 4: one comparison fold for titles (litpipe.text.comparison_fold), applied in
fill_missing_dois._norm and litpipe.text.normalise_title (so litpipe.identity's TITLE_MATCH too).
The live pairs are the consumer's (2026-10-07, real Crossref records): each was demoted to
MED_AUTHOR_ONLY because one side carried a Greek letter, a curly apostrophe or a Unicode dash.
filename_title keeps the pre-fold form: a canonical filename is a stored key."""
import pytest

import fill_missing_dois as F
from litpipe import identity, text

LIVE_PAIRS = [   # (the reference string's form, the Crossref record's form)
    ("Effects of beta-adrenergic blockade on thermoregulation during prolonged exercise in the heat",
     "Effects of β-adrenergic blockade on thermoregulation during prolonged exercise in the heat"),
    ("ACSM's guidelines for exercise testing and prescription eleventh edition handbook",
     "ACSM’s guidelines for exercise testing and prescription eleventh edition handbook"),
    ("Parkinson’s disease and aerobic exercise training effects on gait and balance outcomes",
     "Parkinson's disease and aerobic exercise training effects on gait and balance outcomes"),
]


# ------------------------------------------------------------------ the fold
def test_greek_letters_are_spelled_out_and_micro_is_mu():
    assert text.comparison_fold("β-adrenergic") == "beta-adrenergic"
    assert text.comparison_fold("TGF-β1 and κ-opioid") == "TGF-beta1 and kappa-opioid"
    assert text.comparison_fold("5 µg") == "5 mug" and text.comparison_fold("5 μg") == "5 mug"
    assert text.comparison_fold("Δ and Ω").lower() == "delta and omega"
    assert text.comparison_fold("ά") == "alpha"                     # tonos dropped, then named


@pytest.mark.parametrize("form", ["ACSM's", "ACSM’s", "ACSMs", "ACSM‘s", "ACSM′s", "ACSMʼs"])
def test_every_apostrophe_form_agrees(form):
    assert text.comparison_fold(form) == "ACSMs"
    assert F._norm(form) == "acsms"
    assert text.normalise_title(form) == "acsms"


def test_double_quotes_dashes_tags_and_accents():
    assert text.comparison_fold("“heat”") == '"heat"'
    for d in "‐‑‒–—―−":
        assert text.comparison_fold(f"high{d}intensity") == "high-intensity"
    assert text.comparison_fold("<i>In vivo</i> Périard") == "In vivo Periard"
    assert text.comparison_fold("&bgr;-alanine") == "beta-alanine"         # an isogrk1 entity, decoded first


def test_a_dash_pair_compares_equal_in_normalise_title():
    assert text.normalise_title("High–intensity interval training") == \
        text.normalise_title("High-intensity interval training")


def test_a_greek_only_title():
    assert text.normalise_title("αβγ") == "alphabetagamma"
    assert text.normalise_title("αβγ") == text.normalise_title("alphabetagamma")


def test_a_pair_that_must_stay_different():
    a, b = "β-blockers in heart failure", "α-blockers in heart failure"
    assert text.normalise_title(a) != text.normalise_title(b)
    assert F._norm(a) != F._norm(b)


# ------------------------------------------------------------------ _norm (fill_missing_dois)
@pytest.mark.parametrize("ref,record", LIVE_PAIRS)
def test_the_live_pairs_are_found_in_both_directions(ref, record):
    for title, evidence in ((record, ref), (ref, record)):
        meta = {"title": title, "main_title": title}
        assert F.strict_title(title)
        assert F.title_in_text(meta, F._evidence(evidence))


@pytest.mark.parametrize("ref,record", LIVE_PAIRS)
def test_score_match_is_high_for_the_live_pairs(ref, record):
    item = {"DOI": "10.5555/live.0001", "type": "journal-article", "score": 80.0, "title": [record],
            "author": [{"family": "Author", "given": "Ann"}], "issued": {"date-parts": [[2017]]}}
    status, meta, _s1, _s2 = F.score_match("2017", "Author", [item], [item], evidence=ref, query=ref)
    assert status == "HIGH", status


def test_unrelated_titles_stay_apart():
    meta = {"title": LIVE_PAIRS[0][1], "main_title": LIVE_PAIRS[0][1]}
    assert not F.title_in_text(meta, F._evidence(LIVE_PAIRS[2][0]))


# ------------------------------------------------------------------ identity (TITLE_MATCH)
def test_identity_title_match_reads_a_greek_title_against_spelled_out_text():
    title = LIVE_PAIRS[0][1]
    page = "Journal of Thermal Physiology\n" + LIVE_PAIRS[0][0] + "\nA. Author, B. Author\nAbstract ..."
    v = identity.check(page, "10.5555/not.printed.0001", queue_title=title)
    assert str(v.decision) == "TITLE_MATCH" and v.score >= identity.TITLE_THRESHOLD


def test_identity_reads_a_greek_heavy_title_through_the_fold():
    # without the fold the Greek letters weigh one character each against four to seven spelled out
    title = "α, β, γ and δ subunits of the κ and μ opioid receptors"
    page = "Journal X\nAlpha, beta, gamma and delta subunits of the kappa and mu opioid receptors\nA. Author"
    assert identity.title_similarity(title, page) >= identity.TITLE_THRESHOLD
    v = identity.check(page, "10.5555/not.printed.0003", queue_title=title)
    assert str(v.decision) == "TITLE_MATCH"


def test_identity_still_flags_an_unrelated_page():
    v = identity.check("Journal X\n" + LIVE_PAIRS[2][0], "10.5555/not.printed.0002", queue_title=LIVE_PAIRS[0][1])
    assert str(v.decision) == "FLAG"


# ------------------------------------------------------------------ the stored form stays stable
@pytest.mark.parametrize("title,stored", [
    ("Parkinson’s disease", "parkinson’s disease"),
    ("β-adrenergic blockade", "β-adrenergic blockade"),
    ("High–intensity <i>training</i> : a review", "high–intensity training: a review"),
])
def test_filename_title_is_the_pre_fold_form(title, stored):
    assert text.filename_title(title) == stored
