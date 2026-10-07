"""litpipe.canaries: the network checks, their refusals, the plan, the report and the CLI (W4-B).

Every network check runs against a MockServer on 127.0.0.1 (redirect targets on 127.0.0.2) with
answers trimmed from the 2026-10-06 live probes (tests/fixtures/W4-B/PROVENANCE.json). The stage
modules' URL constants are pointed at the mock, so the canary sends exactly what it would send to
the real host, through litpipe.net with the conftest FakeState, FakeClock and temp ledger."""
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from litpipe import canaries, hosts, ledger, net
from litpipe.outcomes import Kind
from tests.netmock import Reply

FIX = Path(__file__).parent / "fixtures" / "W4-B"
NOW = datetime(2026, 10, 6, 12, 0, tzinfo=timezone.utc)
KEY = "oa-test-key-0123456789"


def fx(name):
    return (FIX / name).read_bytes()


def fxj(name):
    return json.loads(fx(name))


JSON = {"Content-Type": "application/json"}
XML = {"Content-Type": "application/xml"}
PDF = b"%PDF-1.5\n" + b"0" * (canaries.OSF_BYTES - 9)

P = {
    "idconv": "/idconv/",
    "epmc_am": "/epmc/PMC11903078/fullTextXML",
    "epmc_oa": "/epmc/PMC9817969/fullTextXML",
    "efetch": "/eutils/efetch.fcgi",
    "s3_list": "/s3/",
    "s3_meta": "/s3/metadata/PMC9817969.1.json",
    "s3_pdf": "/s3/PMC9817969.1/PMC9817969.1.pdf",
    "bioc": "/bioc/PMC7983342/unicode",
    "details": "/biorxiv/details/medrxiv/10.1101/2020.09.09.20191205/na/json",
    "pubs": "/biorxiv/pubs/medrxiv/10.1101/2021.04.29.21256344/na/json",
    "osf_rec": "/osf/v2/files/60a530d48b826b010e8c03c8/",
    "osf_dl": "/osfdl/f6yza/",
    "osf_files": "/files/f6yza",
    "blob": "/blob",                       # on 127.0.0.2: the storage hop
    "oai": "/oai",
    "arxiv": "/arxiv/api/query",
    "datacite": "/datacite/dois/10.48550/arxiv.2605.29559",
    "doira": "/doiRA/10.1152,10.48550,10.3305,10.2903",
    "cn1": "/cn/10.1152/japplphysiol.00775.2024",
    "cn2": "/cn/10.48550/arxiv.2605.29559",
    "cnt1": "/cnt/crossref",                # on 127.0.0.2: the RA hosts
    "cnt2": "/cnt/datacite",
    "openalex": "/openalex/works",
    "crossref": "/crossref/works",
    "alias": "/crossref/works/10.1515/9789882204508-009",
    "prime": "/crossref/works/10.5790/hongkong/9789888528011.003.0007",
    "sici": "/doiorg/sici",
}


class Env:
    def __init__(self, net_env, srv, srv2):
        self.net_env, self.srv, self.srv2 = net_env, srv, srv2
        self.state, self.clock = net_env.state, net_env.clock

    def hits(self, *paths):
        return [h for s in (self.srv, self.srv2) for h in s.hits if not paths or h.path in paths]

    def one(self, cid, *, context=None, state=None):
        rctx = canaries._RunCtx(context=context if context is not None else CTX_ARXIV, state=state,
                                request=None, cfg=None, now=NOW)
        return canaries._run_network(canaries.CHECKS_BY_ID[cid], rctx)


CTX_ARXIV = {"projects": [{"key": "research_x", "sources": ["unpaywall", "pmc", "arxiv"]}]}
CTX_PLAIN = {"projects": [{"key": "teaching_x", "sources": ["unpaywall", "pmc"]}]}


def script_all(srv, srv2):
    """Every check's passing answers."""
    srv.script(P["idconv"], Reply(200, fx("idconv_peerj.json"), JSON))
    srv.script(P["epmc_am"], Reply(500, fx("epmc_am_500.json"), JSON))
    srv.script(P["epmc_oa"], Reply(200, fx("epmc_oa_head.xml"), XML))
    srv.script(P["efetch"], Reply(200, fx("efetch_pmc_3.xml"), {"Content-Type": "text/xml"}))
    srv.script(P["s3_list"], Reply(200, fx("s3_list_PMC9817969.xml"), XML))
    srv.script(P["s3_meta"], Reply(200, fx("s3_meta_PMC9817969.1.json"), {"Content-Type": "binary/octet-stream"}))
    srv.script(P["s3_pdf"], Reply(200, b"x" * 4096, {"Content-Type": "binary/octet-stream"}))
    srv.script(P["bioc"], Reply(200, fx("bioc_am_PMC7983342.json"), JSON))
    srv.script(P["details"], Reply(200, fx("biorxiv_details_medrxiv.json"), JSON))
    srv.script(P["pubs"], Reply(200, fx("biorxiv_pubs_medrxiv.json"), JSON))
    rec = fxj("osf_file_record.json")
    rec["data"]["links"]["download"] = srv.url(P["osf_dl"])
    srv.script(P["osf_rec"], Reply(200, json.dumps(rec), JSON))
    srv.script(P["osf_dl"], Reply(302, b"", {"Location": srv.url(P["osf_files"])}))
    srv.script(P["osf_files"], Reply(302, b"", {"Location": srv2.url(P["blob"])}))
    srv2.script(P["blob"], Reply(200, PDF, {"Content-Type": "application/octet-stream"}))
    srv.script(P["oai"], Reply(200, fx("sportrxiv_oai_listrecords.xml"), {"Content-Type": "text/xml"}))
    srv.script(P["arxiv"], Reply(200, fx("arxiv_atom_1511_07289.xml"), {"Content-Type": "application/atom+xml"}))
    srv.script(P["datacite"], Reply(200, fx("datacite_arxiv_2605_29559.json"), JSON))
    srv.script(P["doira"], Reply(200, fx("doira_4.json"), JSON))
    srv.script(P["cn1"], Reply(302, b"", {"Location": srv2.url(P["cnt1"])}))
    srv.script(P["cn2"], Reply(302, b"", {"Location": srv2.url(P["cnt2"])}))
    srv2.script(P["cnt1"], Reply(200, fx("cn_crossref_csl.json"), {"Content-Type": "application/vnd.citationstyles.csl+json"}))
    srv2.script(P["cnt2"], Reply(200, fx("cn_datacite_csl.json"), {"Content-Type": "application/vnd.citationstyles.csl+json"}))
    oa = fxj("openalex_list_3.json")
    srv.script(P["openalex"], Reply(200, json.dumps(oa["body"]), {**JSON, **oa["headers"]}))
    cr = fxj("crossref_refs_2.json")
    srv.script(P["crossref"], Reply(200, json.dumps(cr["body"]), {**JSON, **cr["headers"]}))
    srv.script(P["alias"], Reply(301, b"", {"Location": srv.url(P["prime"])}))
    srv.script(P["prime"], Reply(200, fx("crossref_alias_prime.json"), JSON))
    srv.script(P["sici"], Reply(302, b"", {"Location": "https://publisher.example/doi/landing"}))


@pytest.fixture
def env(net_env, mock_server, monkeypatch):
    import jats_to_text
    import lit_net
    import migrate_closed_to_md
    import pmc_fetch
    import preprint_fetch
    import ris_emit
    from litpipe import openalex

    srv, srv2 = mock_server(), mock_server("127.0.0.2")
    hosts.register(hosts.HostPolicy("127.0.0.1", min_interval_s=0.0, redirect_allow=("127.0.0.2",)))
    for mod, name, path in (
            (lit_net, "IDCONV", P["idconv"]), (jats_to_text, "EPMC_JATS_XML", "/epmc/{pmcid}/fullTextXML"),
            (lit_net, "EUTILS", "/eutils"), (pmc_fetch, "S3_BASE", "/s3"),
            (jats_to_text, "BIOC_JSON", "/bioc/{pmcid}/{encoding}"),
            (preprint_fetch, "BIORXIV_DETAILS", "/biorxiv/details/{server}/{doi}/na/json"),
            (preprint_fetch, "OSF_API", "/osf/v2/"), (preprint_fetch, "SPORTRXIV_OAI", P["oai"]),
            (preprint_fetch, "ARXIV_API", P["arxiv"]), (ris_emit, "DATACITE_WORK", "/datacite/dois/{doi}"),
            (ris_emit, "DOI_RA", "/doiRA/{doi}"), (ris_emit, "DOI_CN", "/cn/{doi}"),
            (openalex, "BASE", "/openalex"), (ris_emit, "CROSSREF_SEARCH", P["crossref"]),
            (ris_emit, "CROSSREF_WORK", "/crossref/works/{doi}")):
        monkeypatch.setattr(mod, name, srv.url(path))
    monkeypatch.setattr(migrate_closed_to_md, "doi_url", lambda d: srv.url(P["sici"]))
    monkeypatch.setenv("OPENALEX_API_KEY", KEY)
    script_all(srv, srv2)
    return Env(net_env, srv, srv2)


def status(o):
    return o.payload["status"]


# ------------------------------------------------------------------------------ pass, per check
NETWORK_IDS = [c.id for c in canaries.CHECKS if c.phase == "network"]


@pytest.mark.parametrize("cid", NETWORK_IDS)
def test_each_network_check_passes_on_the_recorded_answers(env, cid):
    o = env.one(cid)
    assert status(o) == canaries.PASS, o.payload
    assert o.kind is Kind.OK and o.detail == cid and o.host == "127.0.0.1"
    assert o.attempts == canaries.CHECKS_BY_ID[cid].requests      # a passing check sends its plan
    assert o.payload["action"] == canaries.ACTION_NONE
    assert set(o.payload) == {"id", "phase", "cadence", "target", "expected", "observed", "status", "action"}
    assert env.state.refused == {}
    lines = [x for x in env.net_env.ledger_lines() if x.get("purpose")]
    assert lines and all(x["purpose"] == f"canary:{cid}" for x in lines)


def test_the_canary_sends_with_no_status_retries(env):
    env.srv.script(P["epmc_oa"], Reply(503, b"busy"))
    env.one("epmc_fulltextxml")
    # 503 is a retry status of every host row; the canary asks for none, so each round sends once
    assert len(env.hits(P["epmc_oa"])) == 2


# ------------------------------------------------------------------------------ fail, per check
def test_idconv_fails_on_a_confirmed_outage(env):
    env.srv.script(P["idconv"], Reply(503, b"down"))
    o = env.one("idconv")
    assert status(o) == canaries.ALARM and o.kind is Kind.OUTAGE and o.status == 503
    assert o.payload["action"] == canaries.ACTION_REFUSED


def test_idconv_wrong_pmcid_is_drift(env):
    env.srv.script(P["idconv"], Reply(200, json.dumps({"records": [{"pmcid": "PMC1"}]}), JSON))
    o = env.one("idconv")
    assert status(o) == canaries.ALARM and "PMC1" in o.payload["observed"]
    assert env.state.refused == {}


def test_epmc_am_answering_404_again_is_drift(env):
    env.srv.script(P["epmc_am"], Reply(404, b"not found"))
    o = env.one("epmc_fulltextxml")
    assert status(o) == canaries.ALARM and "AM" in o.payload["observed"]
    assert env.state.refused == {}


def test_epmc_am_500_without_the_signature_is_drift(env):
    env.srv.script(P["epmc_am"], Reply(500, b"<html>Internal error</html>", {"Content-Type": "text/html"}))
    o = env.one("epmc_fulltextxml")
    assert status(o) == canaries.ALARM and "signature" in o.payload["observed"]
    assert env.state.refused == {}


def test_epmc_oa_outage_refuses_the_host(env):
    env.srv.script(P["epmc_oa"], Reply(502, b"bad gateway"))
    o = env.one("epmc_fulltextxml")
    assert status(o) == canaries.ALARM and o.payload["action"] == canaries.ACTION_REFUSED


def test_efetch_flag_drift(env):
    body = fx("efetch_pmc_3.xml").replace(b"<meta-name>pmc-prop-manuscript</meta-name><meta-value>yes",
                                          b"<meta-name>pmc-prop-manuscript</meta-name><meta-value>no")
    assert body != fx("efetch_pmc_3.xml")
    env.srv.script(P["efetch"], Reply(200, body, {"Content-Type": "text/xml"}))
    o = env.one("efetch")
    assert status(o) == canaries.ALARM and "PMC11903078" in o.payload["observed"]
    assert env.state.refused == {}


def test_s3_metadata_without_pdf_url_is_drift(env):
    meta = fxj("s3_meta_PMC9817969.1.json")
    del meta["pdf_url"]
    env.srv.script(P["s3_meta"], Reply(200, json.dumps(meta), JSON))
    o = env.one("s3")
    assert status(o) == canaries.ALARM and "pdf_url" in o.payload["observed"]
    assert o.attempts == 2 and env.state.refused == {}


def test_s3_head_403_refuses_at_once_without_a_recheck(env):
    env.srv.script(P["s3_pdf"], Reply(403, b""))
    o = env.one("s3")
    assert status(o) == canaries.ALARM and o.status == 403
    assert o.payload["action"] == canaries.ACTION_REFUSED and o.attempts == 3
    assert canaries.RECHECK_S not in env.clock.sleeps
    assert "127.0.0.1" in env.state.refused


def test_bioc_absent_inside_a_200_is_drift(env):
    env.srv.script(P["bioc"], Reply(200, b"[Error] : No result can be found.", {"Content-Type": "text/html"}))
    o = env.one("bioc")
    assert status(o) == canaries.ALARM and "absent" in o.payload["observed"]
    assert env.state.refused == {}


def test_biorxiv_empty_200_twice_refuses_the_host(env):
    env.srv.script(P["details"], Reply(200, b"", JSON))     # the 09-24 outage: 200, Content-Length 0
    o = env.one("biorxiv")
    assert status(o) == canaries.ALARM and o.payload["action"] == canaries.ACTION_REFUSED
    assert len(env.hits(P["details"])) == 2 and canaries.RECHECK_S in env.clock.sleeps


def test_biorxiv_pubs_not_ok_is_drift(env):
    env.srv.script(P["pubs"], Reply(200, json.dumps({"messages": [{"status": "no posts found"}],
                                                     "collection": []}), JSON))
    o = env.one("biorxiv")
    assert status(o) == canaries.ALARM and "pubs" in o.payload["observed"]
    assert env.state.refused == {}


def test_osf_wrong_size_is_drift(env):
    env.srv2.script(P["blob"], Reply(200, PDF[:-10], {"Content-Type": "application/octet-stream"}))
    o = env.one("osf")
    assert status(o) == canaries.ALARM and str(canaries.OSF_BYTES) in o.payload["observed"]


def test_sportrxiv_oai_error_is_drift(env):
    body = (b'<?xml version="1.0"?><OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">'
            b'<error code="badArgument">bad</error></OAI-PMH>')
    env.srv.script(P["oai"], Reply(200, body, {"Content-Type": "text/xml"}))
    o = env.one("sportrxiv")
    assert status(o) == canaries.ALARM and "badArgument" in o.payload["observed"]


def test_sportrxiv_no_records_match_passes(env):
    body = (b'<?xml version="1.0"?><OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">'
            b'<error code="noRecordsMatch">none</error></OAI-PMH>')
    env.srv.script(P["oai"], Reply(200, body, {"Content-Type": "text/xml"}))
    assert status(env.one("sportrxiv")) == canaries.PASS


def test_arxiv_406_refuses_at_once(env):
    env.srv.script(P["arxiv"], Reply(406, b""))
    o = env.one("arxiv")
    assert status(o) == canaries.ALARM and o.status == 406 and o.kind is Kind.REFUSED
    assert o.payload["action"] == canaries.ACTION_REFUSED and len(env.hits(P["arxiv"])) == 1


def test_datacite_publisher_object_is_drift_and_refuses_nothing(env):
    dc = fxj("datacite_arxiv_2605_29559.json")
    dc["data"]["attributes"]["publisher"] = {"name": "arXiv"}
    env.srv.script(P["datacite"], Reply(200, json.dumps(dc), JSON))
    o = env.one("datacite")
    assert status(o) == canaries.ALARM and "object" in o.payload["observed"]
    assert o.kind is Kind.ERROR and o.payload["action"] == canaries.ACTION_NONE
    assert env.state.refused == {}


def test_doira_shape_drift(env):
    env.srv.script(P["doira"], Reply(200, json.dumps([{"DOI": "10.1152", "RA": "Crossref"}]), JSON))
    o = env.one("doira")
    assert status(o) == canaries.ALARM and "10.48550 absent" in o.payload["observed"]


def test_openalex_count_below_floor_and_cost_drift(env):
    oa = fxj("openalex_list_3.json")
    oa["body"]["results"][0]["referenced_works_count"] = 3
    headers = {**JSON, **oa["headers"], "X-RateLimit-Cost-USD": "0.001"}
    env.srv.script(P["openalex"], Reply(200, json.dumps(oa["body"]), headers))
    o = env.one("openalex")
    assert status(o) == canaries.ALARM
    assert "< floor" in o.payload["observed"] and "cost 0.001" in o.payload["observed"]
    assert env.state.refused == {}


def test_openalex_sends_the_key_only_in_the_authorization_header(env):
    env.one("openalex")
    (hit,) = env.hits(P["openalex"])
    assert hit.headers.get("Authorization") == f"Bearer {KEY}"
    assert KEY not in json.dumps(hit.query) and KEY not in env.net_env.ledger_text()


def test_openalex_without_a_key_is_skipped_and_sends_nothing(env, monkeypatch):
    monkeypatch.delenv("OPENALEX_API_KEY")
    o = env.one("openalex")
    assert status(o) == canaries.SKIPPED and o.kind is Kind.SKIPPED and o.attempts == 0
    assert env.hits(P["openalex"]) == [] and env.state.refused == {}


def test_crossref_public_pool_is_drift(env):
    cr = fxj("crossref_refs_2.json")
    env.srv.script(P["crossref"], Reply(200, json.dumps(cr["body"]), {**JSON, "x-api-pool": "public-array"}))
    o = env.one("crossref_refs")
    assert status(o) == canaries.ALARM and "public-array" in o.payload["observed"]


def test_cn_failing_redirect_target_alarms_and_refuses_nothing(env):
    env.srv2.script(P["cnt1"], Reply(503, b"down"))
    o = env.one("content_negotiation")
    assert status(o) == canaries.ALARM and o.payload["action"] == canaries.ACTION_NONE
    assert "127.0.0.2" in o.payload["observed"] and env.state.refused == {}
    assert len(env.hits(P["cnt1"])) == 2          # re-checked once


def test_osf_failing_storage_hop_alarms_and_refuses_nothing(env):
    env.srv2.script(P["blob"], Reply(503, b"down"))
    o = env.one("osf")
    assert status(o) == canaries.ALARM and o.payload["action"] == canaries.ACTION_NONE
    assert env.state.refused == {}


def test_alias_without_the_redirect_is_drift(env):
    env.srv.script(P["alias"], Reply(200, json.dumps({"message": {"DOI": canaries.ALIAS_DOI}}), JSON))
    o = env.one("crossref_alias")
    assert status(o) == canaries.ALARM and canaries.ALIAS_DOI in o.payload["observed"]


def test_sici_not_found_is_drift(env):
    env.srv.script(P["sici"], Reply(404, b"not found"))
    o = env.one("sici")
    assert status(o) == canaries.ALARM and o.payload["action"] == canaries.ACTION_NONE


def test_sici_url_is_the_whole_percent_encoded_doi():
    from litpipe import doi as _doi
    # W5-C2: the `#` is the SICI check character, kept, and encode_path writes it %23 (DOI Handbook 4.7)
    assert _doi.normalise(canaries.SICI_HASH_DOI) == canaries.SICI_HASH_DOI.lower()
    assert canaries.sici_url() == ("https://doi.org/10.1002/(sici)1521-3951(199911)216:1%3C135"
                                   "::aid-pssb135%3E3.0.co;2-%23")


# ------------------------------------------------------------------------------ refusal rules
def test_a_single_503_then_200_on_the_recheck_refuses_nothing(env):
    env.srv.script(P["idconv"], Reply(503, b"blip"), Reply(200, fx("idconv_peerj.json"), JSON))
    o = env.one("idconv")
    assert status(o) == canaries.PASS and o.attempts == 2
    assert canaries.RECHECK_S in env.clock.sleeps
    assert env.state.refused == {} and o.payload["action"] == canaries.ACTION_NONE


def test_a_confirmed_failure_refuses_the_origin_for_the_run(env):
    env.srv.script(P["datacite"], Reply(500, b"oops"))
    o = env.one("datacite")
    assert status(o) == canaries.ALARM and o.attempts == 2
    reason, persistence = env.state.refused["127.0.0.1"]
    assert persistence == "run" and reason.startswith("canary datacite: ")
    assert ("refuse", "127.0.0.1", reason, "run") in env.state.calls


def test_refusal_through_the_real_state(env):
    """state=litpipe.state (conftest's temp DB_PATH): the refusal reads back as "run" in status()."""
    import litpipe.state as st
    env.srv.script(P["datacite"], Reply(503, b"down"))
    o = env.one("datacite", state=st)
    assert o.payload["action"] == canaries.ACTION_REFUSED
    row = next(h for h in st.status()["hosts"] if h["host"] == "127.0.0.1")
    assert row["refused"] == "run" and row["refused_reason"].startswith("canary datacite")
    # a manual refusal is never downgraded, and the check is then SKIPPED with nothing sent
    st.refuse("127.0.0.1", "manual: by hand", persistence="manual")
    before = len(env.hits())
    o2 = env.one("datacite", state=st)
    assert status(o2) == canaries.SKIPPED and "(manual)" in o2.payload["observed"]
    assert len(env.hits()) == before
    row = next(h for h in st.status()["hosts"] if h["host"] == "127.0.0.1")
    assert row["refused"] == "manual"


def test_a_skipped_check_refuses_nothing(env):
    env.state.refused["127.0.0.1"] = ("earlier stage", "run")
    o = env.one("doira")
    assert status(o) == canaries.SKIPPED and o.attempts == 0 and env.hits() == []
    assert env.state.refused == {"127.0.0.1": ("earlier stage", "run")}
    assert not [x for x in env.net_env.ledger_lines() if x.get("decision") == "not_sent"]


def test_worst_case_is_two_rounds_of_count_plus_transport_retries(env):
    env.srv.script(P["idconv"], Reply(close=True))
    o = env.one("idconv")
    assert status(o) == canaries.ALARM and o.kind is Kind.TRANSPORT
    assert o.attempts == canaries.worst_case("idconv") == 2 * (1 + 2)
    assert len(env.hits(P["idconv"])) == o.attempts


def test_worst_case_bounds_a_multi_request_check(env):
    env.srv.script(P["s3_pdf"], Reply(close=True))
    o = env.one("s3")
    assert o.attempts == len(env.hits()) <= canaries.worst_case("s3") == 2 * (3 + 2)


# ------------------------------------------------------------------------------ arXiv and idconv
def test_arxiv_manual_refusal_sends_nothing_and_logs_no_not_sent(env, monkeypatch):
    env.state.refuse("export.arxiv.org", "manual: refused since 2026-09-25", persistence="manual")

    def boom(*a, **k):
        raise AssertionError("net.request called for a refused arXiv")
    monkeypatch.setattr(net, "request", boom)
    o = env.one("arxiv")
    assert status(o) == canaries.SKIPPED and o.attempts == 0
    assert "export.arxiv.org" in o.payload["observed"]
    assert env.hits() == [] and env.net_env.ledger_lines() == []


def test_arxiv_refused_through_the_real_state_is_skipped(env):
    import litpipe.state as st
    for h in canaries.ARXIV_HOSTS:
        st.refuse(h, "manual: refused since 2026-09-25", persistence="manual")
    o = env.one("arxiv", state=st)
    assert status(o) == canaries.SKIPPED and "(manual)" in o.payload["observed"]
    assert env.hits() == [] and env.net_env.ledger_lines() == []


@pytest.mark.parametrize("host", ["arxiv.org", "www.arxiv.org"])
def test_any_arxiv_host_refused_skips_the_check(env, host):
    env.state.refuse(host, "run refusal", persistence="run")
    assert status(env.one("arxiv")) == canaries.SKIPPED and env.hits() == []


def test_arxiv_is_skipped_when_no_scheduled_project_uses_it(env):
    o = env.one("arxiv", context=CTX_PLAIN)
    assert status(o) == canaries.SKIPPED and "arxiv" in o.payload["observed"] and env.hits() == []


def test_arxiv_scheduled_and_not_refused_sends_one_request(env):
    o = env.one("arxiv", context=CTX_ARXIV)
    assert status(o) == canaries.PASS and len(env.hits(P["arxiv"])) == 1


def test_idconv_skipped_while_pmc_ncbi_is_refused(env, monkeypatch):
    import lit_net
    monkeypatch.setattr(lit_net, "IDCONV", "https://pmc.ncbi.nlm.nih.gov/tools/idconv/api/v1/articles/")
    env.state.refuse("pmc.ncbi.nlm.nih.gov", "429", persistence="run")
    o = env.one("idconv")
    assert status(o) == canaries.SKIPPED and o.host == "pmc.ncbi.nlm.nih.gov" and o.attempts == 0
    assert env.net_env.ledger_lines() == []      # nothing sent, no not_sent line either


def test_idconv_skip_reads_the_pmc_host_refusal_whatever_the_url(env):
    env.state.refuse("pmc.ncbi.nlm.nih.gov", "429", persistence="run")      # IDCONV points at the mock
    o = env.one("idconv")
    assert status(o) == canaries.SKIPPED and "pmc.ncbi.nlm.nih.gov" in o.payload["observed"]
    assert env.hits() == []


def test_idconv_sends_one_request_while_not_refused(env):
    o = env.one("idconv")
    assert status(o) == canaries.PASS and len(env.hits(P["idconv"])) == 1
    assert env.hits(P["idconv"])[0].query["ids"] == [canaries.IDCONV_DOI]


# ------------------------------------------------------------------------------ the plan
def test_planned_requests_per_profile(env):
    plan = {p: sum(canaries.planned_requests(p, context=CTX_ARXIV).values()) for p in canaries.PROFILES}
    # every_run 1; daily adds 17; weekly adds 9 (amendment 6 said 8: the Crossref alias costs 2,
    # a 301 and the prime record); monthly adds 0
    assert plan == {"every_run": 1, "daily": 18, "weekly": 27, "monthly": 27}
    daily = canaries.planned_requests("daily", context=CTX_ARXIV)
    assert {k: daily[k] for k in ("epmc_fulltextxml", "efetch", "s3", "bioc", "biorxiv", "osf", "sportrxiv",
                                  "arxiv", "datacite", "doira")} == {
        "epmc_fulltextxml": 2, "efetch": 1, "s3": 3, "bioc": 1, "biorxiv": 2, "osf": 4, "sportrxiv": 1,
        "arxiv": 1, "datacite": 1, "doira": 1}


def test_planned_requests_drop_skipped_checks(env, monkeypatch):
    monkeypatch.delenv("OPENALEX_API_KEY")
    env.state.refuse("export.arxiv.org", "manual", persistence="manual")
    env.state.refuse("pmc.ncbi.nlm.nih.gov", "429", persistence="run")
    plan = canaries.planned_requests("weekly", context=CTX_ARXIV)
    assert plan["arxiv"] == plan["openalex"] == plan["idconv"] == 0
    assert sum(plan.values()) == 27 - 3


@pytest.mark.parametrize("profile,expected", [("every_run", 1), ("daily", 18), ("weekly", 27), ("monthly", 27)])
def test_a_passing_run_sends_exactly_the_plan(env, profile, expected):
    outs = canaries.run(profile, phase="network", context=CTX_ARXIV, now=NOW)
    assert all(status(o) == canaries.PASS for o in outs), [o.payload for o in outs if status(o) != "PASS"]
    assert sum(o.attempts for o in outs) == expected == len(env.hits())
    assert len([x for x in env.net_env.ledger_lines() if x["purpose"].startswith("canary:")]) == expected


def test_profiles_are_cumulative():
    ids = {p: {c.id for c in canaries.checks(p)} for p in canaries.PROFILES}
    assert ids["every_run"] < ids["daily"] < ids["weekly"] == ids["monthly"]
    assert all(c.phase == "local" or c.id == "idconv" for c in canaries.checks("every_run"))


# ------------------------------------------------------------------------------ report and summary
def test_report_shape_and_redaction(env, monkeypatch):
    email = "tester@litpipe-test.org"
    env.srv.script(P["idconv"], Reply(200, json.dumps({"records": [{"pmcid": email}]}), JSON))
    env.srv.script(P["doira"], Reply(500, b"down"))
    outs = canaries.run("weekly", phase="network", context=CTX_ARXIV, now=NOW)
    rep = canaries.report(outs, run_id="R1", profile="weekly", started=NOW)
    assert set(rep) == {"run_id", "profile", "started", "checks", "refused_hosts", "requests"}
    assert rep["started"] == "2026-10-06T12:00:00Z" and rep["requests"] == sum(o.attempts for o in outs)
    assert all(c["phase"] == "network" for c in rep["checks"])
    assert rep["refused_hosts"] == ["127.0.0.1"]
    text = json.dumps(rep)
    for leak in (email, KEY, "email=", "mailto:"):
        assert leak not in text
    s = canaries.summary(rep)
    assert email not in s and KEY not in s


def test_summary_is_ten_lines_with_alarms_first():
    chk = canaries.CHECKS_BY_ID["doi_fixtures"]
    outs = [canaries._mk(chk, canaries.PASS, host="p", observed="ok") for _ in range(5)]
    outs += [canaries._mk(chk, canaries.ALARM, host=f"h{i}", observed=f"bad {i}") for i in range(12)]
    outs += [canaries._mk(chk, canaries.SKIPPED, host="s", observed="skip")]
    rep = canaries.report(outs, profile="daily")
    lines = canaries.summary(rep).splitlines()
    assert len(lines) <= canaries.SUMMARY_LINES
    assert lines[0].startswith("HEALTH ALARM: 12 alarm, 0 error, 5 pass, 1 skipped")
    assert all(x.startswith("ALARM ") for x in lines[1:-1])
    assert lines[-1] == "+4 more ALARM"


def test_summary_pass():
    chk = canaries.CHECKS_BY_ID["doi_fixtures"]
    rep = canaries.report([canaries._mk(chk, canaries.PASS, host="p", observed="ok")])
    assert canaries.summary(rep) == "HEALTH PASS: 0 alarm, 0 error, 1 pass, 0 skipped; 0 requests"


def test_a_check_that_raises_is_an_error_outcome(env, monkeypatch):
    import ris_emit
    monkeypatch.setattr(ris_emit, "DOI_RA", None)      # .format on None raises inside the check
    o = env.one("doira")
    assert status(o) == canaries.ERROR and o.kind is Kind.ERROR and env.state.refused == {}


# ------------------------------------------------------------------------------ CLI
def _cli_registry(net_env, tmp_path, **extra):
    net_env.write_config(db_dir=str(tmp_path / "db"), root=str(tmp_path / "root"), **extra)


def test_cli_dry_run_sends_nothing(env, monkeypatch, capsys):
    def boom(*a, **k):
        raise AssertionError("dry run sent a request")
    monkeypatch.setattr(net, "request", boom)
    assert canaries.main(["--profile", "weekly", "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "planned requests; dry run, nothing sent" in out and env.hits() == []
    assert env.net_env.ledger_lines() == []


def test_cli_exit_0_on_health_pass(env, tmp_path, monkeypatch, capsys):
    import lit_util
    _cli_registry(env.net_env, tmp_path)
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", tmp_path / "root")
    js = tmp_path / "out" / "health.json"
    assert canaries.main(["--profile", "every_run", "--json", str(js)]) == 0
    out = capsys.readouterr().out
    assert out.startswith("HEALTH PASS") and "[step-summary]" not in out
    rep = json.loads(js.read_text(encoding="utf-8"))
    assert {c["phase"] for c in rep["checks"]} == {"network", "local"}


def test_cli_exit_2_on_health_alarm_with_step_summary(env, tmp_path, monkeypatch, capsys):
    import lit_util
    _cli_registry(env.net_env, tmp_path)
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", tmp_path / "root")
    env.srv.script(P["idconv"], Reply(close=True))
    assert canaries.main(["--profile", "every_run", "--phase", "network"]) == 2
    last = capsys.readouterr().out.strip().splitlines()[-1]
    assert last.startswith("[step-summary] ")
    step = json.loads(last[len("[step-summary] "):])
    assert set(step) == {"reasons", "aborted", "transport_failures"}
    assert step["aborted"] is None and step["transport_failures"] == 1 and step["reasons"][0].startswith("idconv")


def test_cli_exit_1_on_usage_and_config_errors(env, tmp_path, capsys):
    assert canaries.main(["--profile", "hourly"]) == 1
    assert canaries.main([]) == 1
    env.net_env.write_config(projects={"teaching_x": {"lib_dir": "lib", "sources": ["nope"]}})
    assert canaries.main(["--profile", "daily", "--phase", "network"]) == 1
    assert "config error" in capsys.readouterr().err


def test_cli_reads_sys_argv(env, monkeypatch):
    monkeypatch.setattr("sys.argv", ["canaries", "--profile", "daily", "--dry-run"])
    assert canaries.main() == 0


def test_run_rejects_a_bad_profile_or_phase():
    with pytest.raises(ValueError):
        canaries.run("hourly")
    with pytest.raises(ValueError):
        canaries.run("daily", phase="both")
