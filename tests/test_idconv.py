"""DOI -> PMCID on sanctioned routes (W2-A1), the legacy idconv shim, and the W0 tests for
harvest_citations.pmid_to_doi and fetch_figures.fetch_pmc_html.

The mapping chain (lit_net.doi_to_pmcid: idconv while pmc.ncbi.nlm.nih.gov is not refused, then
Europe PMC REST search, then E-utilities esearch + esummary) runs through the REAL litpipe.net into
the scripted transport of tests/test_pmc_stage.py (`world`), fed by recorded answers from
2026-09-30. The 2026-09-01 urllib detour for the whole pmc.ncbi host is retired: its only other users
were the prohibited article-page routes, which lit_net now refuses before sending.
"""
import json
import subprocess
import sys
from pathlib import Path

import pytest
import requests

import lit_net
from lit_util import is_valid_doi
from litpipe import net
from litpipe.outcomes import Kind
from tests.test_pmc_stage import (AM_DOI, AM_PMC, Down, EMB_DOI, EMB_PMC, MISS_DOI, NEW_DOI, NEW_PMC,
                                  NONE_DOI, OA_DOI, OA_PMC, World, world)  # noqa: F401  (fixture)

IDC = ("pmc.ncbi.nlm.nih.gov", r"/tools/idconv/api/v1/articles/")
EPMC = ("www.ebi.ac.uk", r"/europepmc/webservices/rest/search")
ESEARCH = ("eutils.ncbi.nlm.nih.gov", r".*/esearch\.fcgi")
ESUMMARY = ("eutils.ncbi.nlm.nih.gov", r".*/esummary\.fcgi")


# ============================================================================ idconv first
def test_idconv_answers_ok_embargoed_and_not_found(world):
    res = lit_net.doi_to_pmcid([OA_DOI, AM_DOI.upper(), EMB_DOI, MISS_DOI, NEW_DOI])
    assert res[OA_DOI].kind is Kind.OK and res[OA_DOI].payload.pmcid == OA_PMC
    assert res[AM_DOI].kind is Kind.OK and res[AM_DOI].payload.pmcid == AM_PMC     # joined case-insensitively
    assert res[EMB_DOI].kind is Kind.EMBARGOED and res[EMB_DOI].payload.release_date == "2027-03-11"
    assert res[MISS_DOI].kind is Kind.NO_MATCH and res[MISS_DOI].payload.source == "idconv"
    assert res[NEW_DOI].kind is Kind.OK and res[NEW_DOI].payload.pmcid == NEW_PMC
    assert world.hosts_hit() == {"pmc.ncbi.nlm.nih.gov"}          # idconv answered all: no fallback
    assert len(world.hits) == 1


def test_idconv_batches_100(world):
    dois = [f"10.1234/x{i}" for i in range(150)]
    lit_net.doi_to_pmcid(dois)
    calls = world.hits_for("/tools/idconv/")
    assert [len(c.query["ids"][0].split(",")) for c in calls] == [100, 50]


def test_idconv_is_not_called_while_the_host_is_refused(world):
    world.env.state.refuse("pmc.ncbi.nlm.nih.gov", "HTTP 429 (earlier)")
    res = lit_net.doi_to_pmcid([OA_DOI, AM_DOI, EMB_DOI, NEW_DOI, MISS_DOI])
    assert not world.hits_for("pmc.ncbi.nlm.nih.gov")
    # not even handed to litpipe.net (which would log a not_sent line and short-circuit)
    assert not [ln for ln in world.env.ledger_lines() if ln.get("host") == "pmc.ncbi.nlm.nih.gov"]
    assert res[OA_DOI].kind is Kind.OK and res[OA_DOI].payload.source == "epmc_search"
    assert res[OA_DOI].payload.is_open_access == "Y"
    # Europe PMC has no pmcid for the embargoed and the just-released article: E-utilities decides
    assert res[EMB_DOI].kind is Kind.EMBARGOED and res[EMB_DOI].payload.source == "eutils"
    assert res[EMB_DOI].payload.release_date is None                    # esummary gives no date
    assert res[NEW_DOI].kind is Kind.OK and res[NEW_DOI].payload.source == "eutils"
    assert res[MISS_DOI].kind is Kind.NO_MATCH and res[MISS_DOI].payload.source == "eutils"
    assert res[OA_DOI].payload.steps[0].startswith("idconv:REFUSED")


def test_a_429_refuses_idconv_for_the_rest_of_the_run(world):
    world.override[IDC] = [(429, "<title>429</title>", {"Content-Type": "text/html"})]
    dois = [f"10.1234/x{i}" for i in range(150)] + [OA_DOI]
    res = lit_net.doi_to_pmcid(dois)
    assert len(world.hits_for("/tools/idconv/")) == 1                   # one 429, never retried, no 2nd chunk
    assert res[OA_DOI].kind is Kind.OK and res[OA_DOI].payload.source == "epmc_search"
    assert res["10.1234/x149"].kind is Kind.NO_MATCH                     # E-utilities answered: not in PMC


def test_idconv_400_goes_to_the_fallbacks_without_a_fan_out(world):
    """V1-N2: no one-request-per-DOI series on a 400."""
    world.override[IDC] = [(400, '{"status":"error"}', {"Content-Type": "application/json"})]
    res = lit_net.doi_to_pmcid([OA_DOI, AM_DOI, MISS_DOI])
    assert len(world.hits_for("/tools/idconv/")) == 1
    assert res[OA_DOI].kind is Kind.OK and res[AM_DOI].kind is Kind.OK
    assert res[MISS_DOI].kind is Kind.NO_MATCH


def test_every_step_down_keeps_the_first_root_cause(world):
    world.override[IDC] = [Down("reset")]
    world.override[EPMC] = [(503, "")]
    world.override[ESEARCH] = [(503, "")]
    res = lit_net.doi_to_pmcid([OA_DOI])
    o = res[OA_DOI]
    assert o.kind is Kind.TRANSPORT and o.payload.source == "idconv"      # never NO_MATCH
    assert [s.split(":")[0] for s in o.payload.steps] == ["idconv", "epmc_search", "eutils"]


def test_europe_pmc_no_pmcid_and_eutils_down_is_not_no_match(world):
    """Europe PMC does not list embargoed or just-released articles: its 'no PMCID' is not an
    answer. With E-utilities down the DOI keeps the first failure."""
    world.env.state.refuse("pmc.ncbi.nlm.nih.gov", "HTTP 429 (earlier)")
    world.override[ESEARCH] = [(503, "")]
    o = lit_net.doi_to_pmcid([NEW_DOI])[NEW_DOI]
    assert o.kind is Kind.REFUSED and "epmc_search:no PMCID" in o.payload.steps


def test_invalid_doi_is_no_match_without_a_request(world):
    res = lit_net.doi_to_pmcid(["10.1234/ab–cd", "not a doi"])
    assert {o.kind for o in res.values()} == {Kind.NO_MATCH} and world.hits == []


# ============================================================================ Europe PMC step
def test_europe_pmc_batches_stay_under_the_measured_url_limit():
    dois = [f"10.1016/j.example.2020.{i:06d}" for i in range(250)]
    batches = lit_net.epmc_batches(dois)
    assert sum(len(b) for b in batches) == 250
    assert all(len(b) <= lit_net.EPMC_MAX_DOIS for b in batches)
    assert all(lit_net._epmc_url_len(b) <= lit_net.EPMC_MAX_URL for b in batches)
    # 100 real-length DOIs made a 4,380-character URL that Europe PMC served; 200 got a 414
    assert lit_net.EPMC_MAX_URL < 4380


def test_europe_pmc_errcode_in_a_200_is_an_error_not_an_empty_answer(world):
    world.env.state.refuse("pmc.ncbi.nlm.nih.gov", "HTTP 429 (earlier)")
    world.override[EPMC] = [(200, '{"errCode":404,"errMsg":"Invalid page size provided"}')]
    o = lit_net.doi_to_pmcid([OA_DOI])[OA_DOI]
    assert o.kind is Kind.OK and o.payload.source == "eutils"             # E-utilities still answered
    assert "epmc_search:ERROR" in o.payload.steps


def test_europe_pmc_query_quotes_each_doi():
    assert lit_net.epmc_query(["10.1/a", "10.2/b"]) == 'DOI:"10.1/a" OR DOI:"10.2/b"'


# ============================================================================ E-utilities step
def test_esearch_hit_counts_only_when_esummary_carries_the_doi(world):
    """PMC's esearch reads [doi] as [All Fields]: a stray hit must not map a DOI."""
    world.env.state.refuse("pmc.ncbi.nlm.nih.gov", "HTTP 429 (earlier)")
    world.override[EPMC] = [(503, "")]
    world.override[ESEARCH] = [(200, json.dumps({"esearchresult": {"count": "1", "idlist": ["4587640"]}}))]
    o = lit_net.doi_to_pmcid([OA_DOI])[OA_DOI]
    assert o.kind is Kind.NO_MATCH and o.payload.pmcid is None and o.payload.source == "eutils"


def test_esearch_count_over_retmax_is_not_no_match(world):
    world.env.state.refuse("pmc.ncbi.nlm.nih.gov", "HTTP 429 (earlier)")
    world.override[EPMC] = [(503, "")]
    world.override[ESEARCH] = [(200, json.dumps({"esearchresult": {"count": "900", "idlist": ["4587640"]}}))]
    o = lit_net.doi_to_pmcid([OA_DOI])[OA_DOI]
    assert o.kind is Kind.REFUSED                                          # the first failure, not NO_MATCH
    assert "eutils:ERROR" in o.payload.steps


def test_pmclivedate_in_the_future_is_embargoed_with_that_date():
    import datetime as dt
    today = dt.date(2026, 9, 30)
    assert lit_net._live_or_release("2026/09/30", today) == (True, None)
    assert lit_net._live_or_release("2027/03/11", today) == (False, "2027-03-11")
    assert lit_net._live_or_release(None, today) == (False, None)


# ============================================================================ legacy dict form
def test_doi_to_pmcid_batch_keeps_the_dict_and_drops_embargoed(world, capsys):
    out = lit_net.doi_to_pmcid_batch([OA_DOI, EMB_DOI, MISS_DOI], ua="ignored", email="ignored")
    assert out == {OA_DOI: OA_PMC}
    assert "embargoed" in capsys.readouterr().err


def test_doi_to_pmcid_batch_says_failed_lookups_are_not_no_pmcid(world, capsys):
    world.override[IDC] = [Down("reset")]
    world.override[EPMC] = [(503, "")]
    world.override[ESEARCH] = [(503, "")]
    assert lit_net.doi_to_pmcid_batch([OA_DOI], ua="UA", email="e@x") == {}
    err = capsys.readouterr().err
    assert "could not be looked up" in err and "NOT 'no PMCID'" in err and "e@x" not in err


def test_doi_to_pmcid_batch_never_sends_the_callers_identity(world):
    lit_net.doi_to_pmcid_batch([OA_DOI], ua="Custom/1.0", email=None, tool="legacy-tool")
    h = world.hits_for("/tools/idconv/")[0]
    assert h.query["tool"] == ["literature-pipeline"] and h.query["email"] == ["tester@litpipe-test.org"]
    assert "None" not in h.url and "Custom/1.0" not in h.headers.get("User-Agent", "")


# ============================================================================ constants and the shim
def test_is_valid_doi_rejects_dash_artifacts_keeps_sici():
    assert is_valid_doi("10.1234/abc.123")
    assert is_valid_doi("10.1234/ab-cd")            # ASCII hyphen stays valid
    assert not is_valid_doi("10.1234/ab–cd")   # en dash (PDF artifact)
    assert not is_valid_doi("10.1234/ab−cd")   # minus sign (PDF artifact)
    # legit SICI-format DOIs carry literal angle brackets -- must NOT be rejected: is_valid_doi gates
    # the citation graph + index, and these are real registered DOIs (old Wiley/Blackwell).
    assert is_valid_doi("10.1002/(SICI)1097-0258(19970228)16:4<385::AID-SIM380>3.0.CO;2-3")
    assert not is_valid_doi("")
    assert not is_valid_doi("10.1/x")               # <4-digit registrant


def test_idconv_points_at_current_ncbi_host():
    """The legacy www.ncbi.nlm.nih.gov/pmc/utils path 301s to this host; pin it."""
    assert lit_net.IDCONV == "https://pmc.ncbi.nlm.nih.gov/tools/idconv/api/v1/articles/"
    assert "/pmc/utils/idconv" not in lit_net.IDCONV


def test_idconv_host_row_is_urllib_and_never_retries_a_429():
    from litpipe import hosts
    pol = hosts.policy(lit_net.IDCONV)
    assert pol.transport == "urllib" and 429 not in pol.retry.statuses and 429 in pol.refuse_host_statuses
    assert hosts.prohibited(lit_net.IDCONV) is None


def test_the_host_wide_urllib_detour_is_gone():
    for name in ("URLLIB_ONLY_HOSTS", "_urllib_only_host", "_urllib_get", "_urllib_stream"):
        assert not hasattr(lit_net, name), name


def test_idconv_get_shim_goes_through_litpipe_net(world):
    r = lit_net._idconv_get(lit_net.IDCONV, {"tool": "legacy-tool", "email": None, "ids": OA_DOI,
                                             "idtype": "doi", "format": "json"}, "UA")
    assert r.status_code == 200 and r.json()["records"][0]["pmcid"] == OA_PMC
    h = world.hits[0]
    assert h.url.count("?") == 1 and h.query["tool"] == ["literature-pipeline"]
    assert h.query["email"] == ["tester@litpipe-test.org"] and "None" not in h.url


def test_idconv_get_shim_transport_failure_is_status_zero(world):
    world.override[IDC] = [Down("getaddrinfo failed")]
    r = lit_net._idconv_get(lit_net.IDCONV, {"ids": "10.1/a"}, "UA")
    assert r.status_code == 0 and "TRANSPORT" in r.error


def test_idconv_get_shim_429_is_returned_not_retried(world):
    world.override[IDC] = [(429, "<title>429</title>", {"Content-Type": "text/html"})]
    r = lit_net._idconv_get(lit_net.IDCONV, {"ids": "10.1/a"}, "UA")
    assert r.status_code == 429 and len(world.hits) == 1
    r2 = lit_net._idconv_get(lit_net.IDCONV, {"ids": "10.1/b"}, "UA")     # refused now: nothing sent
    assert r2.status_code == 0 and "REFUSED" in r2.error and len(world.hits) == 1


# ============================================================================ prohibited routes
def test_get_refuses_a_pmc_article_page_without_sending(monkeypatch):
    def boom(*a, **k):
        raise AssertionError("a prohibited route reached the transport")
    monkeypatch.setattr(lit_net.requests, "get", boom)
    for url in ("https://pmc.ncbi.nlm.nih.gov/articles/PMC1/", "https://pmc.ncbi.nlm.nih.gov/./articles/PMC1/pdf/",
                "https://cdn.ncbi.nlm.nih.gov/pmc/blobs/x.jpg", "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC1/"):
        r = lit_net.get(url, headers={"User-Agent": "UA"})
        assert r.status_code == 0 and r.error.startswith("PROHIBITED")
        s = lit_net.stream_download(url, max_bytes=1000)
        assert s.status_code == 0 and s.error.startswith("PROHIBITED")


def test_other_urls_keep_the_requests_transport(monkeypatch):
    called = []

    class _R:
        status_code = 200
        headers = {}

        def iter_content(self, chunk_size=None):
            return iter([b"%PDF-", b"tail"])
    monkeypatch.setattr(lit_net.requests, "get", lambda url, **k: (called.append(url), _R())[1])
    assert lit_net.stream_download("https://example.org/a.pdf", max_bytes=10_000).status_code == 200
    assert lit_net.get("https://www.ncbi.nlm.nih.gov/research/bionlp/x").status_code == 200
    assert len(called) == 2


# ---------------------------------------------------------------- fetch_figures (W0; W2-A2 rewrites it)
import harvest_citations  # noqa: E402


# ---------------------------------------------------------------- harvest_citations.pmid_to_doi (W0)


def test_harvest_help_runs_from_a_foreign_cwd(tmp_path):
    """harvest_citations now imports litpipe; a flat script run from elsewhere must still find it
    (the repo root is sys.path[0] when the script runs)."""
    repo = Path(__file__).resolve().parent.parent
    r = subprocess.run([sys.executable, str(repo / "harvest_citations.py"), "--help"],
                       cwd=tmp_path, capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, r.stderr
