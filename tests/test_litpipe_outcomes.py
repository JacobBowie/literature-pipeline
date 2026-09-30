"""litpipe.outcomes: the typed-outcome vocabulary and the legacy status-string mapping (W0).

The legacy strings below are the shapes the stages actually write (unpaywall_fetch_v2,
pmc_fetch, preprint_fetch report columns; urllib3 / urllib exception text), not invented ones.
"""
import dataclasses

import pytest

from litpipe.outcomes import Kind, Outcome, from_legacy, legacy_outcome

URLLIB3_DNS = ("HTTPSConnectionPool(host='api.unpaywall.org', port=443): Max retries exceeded with "
               "url: /v2/10.1/x?email=a%40b.c (Caused by NameResolutionError(\"<urllib3.connection."
               "HTTPSConnection object at 0x0>: Failed to resolve 'api.unpaywall.org' ([Errno 11001] "
               "getaddrinfo failed)\"))")
URLLIB3_READ_TIMEOUT = ("HTTPSConnectionPool(host='api.unpaywall.org', port=443): Read timed out. "
                        "(read timeout=15)")
URLLIB3_429_EXHAUSTED = ("HTTPSConnectionPool(host='x.org', port=443): Max retries exceeded with url: "
                         "/y (Caused by ResponseError('too many 429 error responses'))")
URLLIB3_503_EXHAUSTED = URLLIB3_429_EXHAUSTED.replace("429", "503")


def test_kind_is_exactly_the_contract_vocabulary():
    assert [k.value for k in Kind] == [
        "OK", "NO_MATCH", "NOT_AT_RA", "NOT_AVAILABLE", "EMBARGOED", "ALIASED", "REFUSED",
        "OUTAGE", "DEFERRED", "CONFIG", "ERROR", "TRANSPORT", "SKIPPED"]


def test_kind_writes_as_its_bare_name():
    """A str Enum whose str() and format() are the value, so a CSV column reads 'REFUSED'."""
    assert str(Kind.REFUSED) == "REFUSED" and f"{Kind.OK}" == "OK" and Kind.OK == "OK"


def test_ok_is_true_only_for_ok():
    assert [k for k in Kind if Outcome(k).ok] == [Kind.OK]


def test_outcome_contract_field_order_and_defaults():
    o = Outcome(Kind.REFUSED, 403, "example.org", "row refused", 1, 120)
    assert (o.retry_after, o.payload) == (None, None)
    assert [f.name for f in dataclasses.fields(Outcome)] == [
        "kind", "status", "host", "detail", "attempts", "elapsed_ms", "retry_after", "payload"]


def test_outcome_is_immutable():
    with pytest.raises(dataclasses.FrozenInstanceError):
        Outcome(Kind.OK).kind = Kind.ERROR


def test_string_kind_round_trips_and_unknown_raises():
    assert Outcome("OUTAGE").kind is Kind.OUTAGE
    with pytest.raises(ValueError):
        Outcome("NOT_A_KIND")


@pytest.mark.parametrize("s,stage,kind", [
    # 1. Europe PMC fullTextXML 500 / europepmc/HTTP_500 -> NOT_AVAILABLE
    ("europepmc/HTTP_500;ncbi-page/HTML", "pmc", Kind.NOT_AVAILABLE),
    ("HTTP_500", "fulltext", Kind.NOT_AVAILABLE),
    ("fullTextXML HTTP_500", None, Kind.NOT_AVAILABLE),
    # 2. 406 and a final 429 -> REFUSED
    ("HTTP_406", "unpaywall", Kind.REFUSED),
    ("HTTP_429", "preprint", Kind.REFUSED),
    (URLLIB3_429_EXHAUSTED, "unpaywall", Kind.REFUSED),
    # 3. 403 -> REFUSED, and it outranks a 5xx elsewhere in the string
    ("HTTP_403", "unpaywall", Kind.REFUSED),
    ("europepmc/HTTP_503;ncbi-page/HTTP_403", "pmc", Kind.REFUSED),
    # 4. other 5xx and an empty 200 -> OUTAGE
    ("HTTP_500", "unpaywall", Kind.OUTAGE),
    ("HTTP_520", None, Kind.OUTAGE),
    ("HTTP 503", "unpaywall", Kind.OUTAGE),
    ("HTTP Error 503: Service Unavailable", None, Kind.OUTAGE),
    (URLLIB3_503_EXHAUSTED, None, Kind.OUTAGE),
    ("EMPTY", "pmc", Kind.OUTAGE),
    # 5. DNS, timeout, connection -> TRANSPORT
    (URLLIB3_DNS, "unpaywall", Kind.TRANSPORT),
    (URLLIB3_READ_TIMEOUT, "unpaywall", Kind.TRANSPORT),
    ("ERR_('Connection aborted.', RemoteDisconnected('Remote end closed connection'))", "preprint",
     Kind.TRANSPORT),
    ("URLError: <urlopen error [Errno 11001] getaddrinfo failed>", None, Kind.TRANSPORT),
    ("TimeoutError: timed out", None, Kind.TRANSPORT),
    # 6. no record -> NO_MATCH; unpaywall_lookup's bare "HTTP 404" IS not-in-Unpaywall
    ("NO_PMCID", "pmc", Kind.NO_MATCH),
    ("NOT_FOUND", None, Kind.NO_MATCH),
    ("NOT_IN_UNPAYWALL(404)", None, Kind.NO_MATCH),
    ("NO_MATCH", "preprint", Kind.NO_MATCH),
    ("HTTP 404", "unpaywall", Kind.NO_MATCH),
    ("HTTP 404", None, Kind.NO_MATCH),
    # 7. CLOSED -> NOT_AVAILABLE
    ("CLOSED", "unpaywall", Kind.NOT_AVAILABLE),
    # 8. HTML walls -> REFUSED
    ("HTML", "pmc", Kind.REFUSED),
    ("NOT_PDF", "preprint", Kind.REFUSED),
    ("HTTP_202", "unpaywall", Kind.REFUSED),
    # 9. Unpaywall API 422 / 410 (the API's space form) -> CONFIG; a download 422 is not config
    ("HTTP 422", "unpaywall", Kind.CONFIG),
    ("HTTP 410", None, Kind.CONFIG),
    ("HTTP_422", "unpaywall", Kind.ERROR),
    ("HTTP 422", "pmc", Kind.ERROR),
    # 10. MANUAL_PREPRINT -> SKIPPED
    ("MANUAL_PREPRINT", "preprint", Kind.SKIPPED),
    # anything else -> ERROR (a dead OA link's 404, a mismatch, boilerplate, blanks)
    ("HTTP_404", "unpaywall", Kind.ERROR),
    ("DOI_MISMATCH:pdf_doi=10.1/other", "preprint", Kind.ERROR),
    ("BOILERPLATE_lww_author_permission_guidelines_v1", "preprint", Kind.ERROR),
    ("TOO_LARGE", "pmc", Kind.ERROR),
    ("no candidates", "pmc", Kind.ERROR),
    ("", None, Kind.ERROR),
    (None, None, Kind.ERROR),
    # non-failure strings from a whole status column
    ("OK", "pmc", Kind.OK),
    ("SKIP_EXISTS", "unpaywall", Kind.OK),
    ("ALREADY_EXISTS", "preprint", Kind.OK),
    ("DRY", "preprint", Kind.SKIPPED),
])
def test_from_legacy_maps_real_stage_strings(s, stage, kind):
    assert from_legacy(s, stage) is kind


def test_from_legacy_stage_is_case_insensitive():
    assert from_legacy("HTTP_500", "FullText") is Kind.NOT_AVAILABLE
    assert from_legacy("HTTP 422", " Unpaywall ") is Kind.CONFIG


def test_legacy_outcome_manual_preprint_detail():
    o = legacy_outcome("MANUAL_PREPRINT", "preprint", host="www.biorxiv.org")
    assert (o.kind, o.detail, o.host, o.status) == (Kind.SKIPPED, "manual_preprint",
                                                     "www.biorxiv.org", None)


def test_legacy_outcome_carries_status_and_original_detail():
    o = legacy_outcome("HTTP_403", "unpaywall")
    assert (o.kind, o.status, o.detail) == (Kind.REFUSED, 403, "HTTP_403")
    assert legacy_outcome(URLLIB3_429_EXHAUSTED).status == 429
    t = legacy_outcome(URLLIB3_DNS)
    assert t.kind is Kind.TRANSPORT and t.status is None and t.detail == URLLIB3_DNS
