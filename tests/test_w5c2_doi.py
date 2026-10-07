"""W5-C2 step 3: SICI DOIs in the forward harvests (census of 2026-10-07, three harvests).

- A Wiley SICI whose check character is '#' (`...;2-#`) normalised to None: the '#' was read as a
  URL fragment. It is kept when it is the final character of a "(sici)" form right after ";2-"
  (any case; a trailing '.' or closing punctuation stripped); every other '#' stays a fragment.
- Three SICI DOIs whose ISSN(date) head carries a month range (`1520-6300(200102/03)`) fell to
  ONE key (`10.1002/1520-6300(200102/03)13:2`): the head now accepts the range.
"""
import pytest

from litpipe import doi

HASH_SICI = "10.1002/(sici)1097-0177(199809)213:1<147::aid-aja15>3.0.co;2-#"     # live, census harvest 2
NUR_SICI = "10.1002/(sici)1098-240x(200002)23:1<1::aid-nur1>3.0.co;2-#"          # the gate review's example
RANGE_SICI = ["10.1002/1520-6300(200102/03)13:2<162::aid-ajhb1025>3.0.co;2-t",   # live, census harvest 3
              "10.1002/1520-6300(200102/03)13:2<173::aid-ajhb1026>3.0.co;2-m",
              "10.1002/1520-6300(200102/03)13:2<180::aid-ajhb1027>3.0.co;2-r"]


@pytest.mark.parametrize("fn", [doi.normalise, doi.normalise_structured])
@pytest.mark.parametrize("raw", [HASH_SICI, NUR_SICI])
def test_a_sici_check_character_hash_is_kept(fn, raw):
    assert fn(raw) == raw


@pytest.mark.parametrize("fn", [doi.normalise, doi.normalise_structured])
def test_the_upper_case_sici_form_and_a_trailing_period(fn):
    assert fn("10.1002/(SICI)1098-240X(200002)23:1<1::AID-NUR1>3.0.CO;2-#") == NUR_SICI
    assert fn(NUR_SICI + ".") == NUR_SICI
    assert fn("https://doi.org/" + NUR_SICI) == NUR_SICI


def test_in_running_text_closing_punctuation_is_not_part_of_it():
    assert doi.normalise(f"(doi:{NUR_SICI}). Next sentence") == NUR_SICI
    assert doi.normalise(f"see {NUR_SICI}, and more") == NUR_SICI


def test_a_percent_encoded_hash_decodes_to_the_same_doi():
    assert doi.normalise("10.1002/(sici)1098-240x(200002)23:1%3C1::aid-nur1%3E3.0.co;2-%23") == NUR_SICI


def test_encode_path_writes_the_hash_as_percent_23():
    enc = doi.encode_path(NUR_SICI)
    assert enc.endswith(";2-%23") and "#" not in enc
    assert "%3C1::aid-nur1%3E" in enc


def test_every_other_hash_stays_a_fragment():
    assert doi.normalise("10.1056/nejmc1113675#sa3") == "10.1056/nejmc1113675"
    assert doi.normalise_structured("10.1056/NEJMc1113675#sa3") == "10.1056/nejmc1113675"
    assert doi.encode_path("10.1056/nejmc1113675#sa3") == "10.1056/nejmc1113675"
    # a '#' that is not the last character of the SICI form is a fragment (and leaves a truncation)
    assert doi.normalise(NUR_SICI + "sa3") is None
    # a '#' after ";2-" in a DOI with no "(sici)" marker is a fragment too (a truncation is left)
    assert doi.normalise("10.5555/abc.123;2-#") is None


@pytest.mark.parametrize("fn", [doi.normalise, doi.normalise_structured])
def test_sici_month_range_heads_keep_three_dois_apart(fn):
    got = [fn(d) for d in RANGE_SICI]
    assert got == RANGE_SICI and len(set(got)) == 3


@pytest.mark.parametrize("raw", [
    "10.1519/1533-4287(1990)004<0047:rbrasp>2.3.co;2",
    "10.1519/1533-4295(2006)28[44:msastr]2.0.co;2",
    "10.1002/(sici)1097-0142(20000915)89:6<1260::aid-cncr10>3.0.co;2-#".replace("#", "6"),
])
def test_sici_dois_already_pinned_are_unchanged(raw):
    assert doi.normalise_structured(raw) == raw and doi.normalise(raw) == raw
