"""ris_emit's clients through the real litpipe.net against loopback mocks (W2-E1): typed failures
(REG-I46), one identity (DEC-13), encoded DOI paths (I47), aliases (N-A3), DataCite's publisher
object, content negotiation on the redirect allow-list, and the sweep contract for resolve_meta."""
import json
from pathlib import Path

import pytest

import ris_emit as R
from litpipe import hosts
from litpipe.hosts import HostPolicy
from litpipe.outcomes import Kind
from tests.netmock import Reply

FIX = Path(__file__).resolve().parent / "fixtures" / "W2-E1"
A, B, C = "127.0.0.1", "127.0.0.2", "127.0.0.3"
JSON = {"Content-Type": "application/json"}


def body(name):
    return json.dumps(json.loads((FIX / name).read_text(encoding="utf-8"))["body"])


@pytest.fixture
def api(net_env, mock_server, monkeypatch):
    """A loopback server standing in for api.crossref.org, api.datacite.org and doi.org."""
    s = mock_server(A)
    monkeypatch.setattr(R, "CROSSREF_WORK", s.url("/works/{doi}"))
    monkeypatch.setattr(R, "CROSSREF_SEARCH", s.url("/works"))
    monkeypatch.setattr(R, "DATACITE_WORK", s.url("/dois/{doi}"))
    monkeypatch.setattr(R, "DOI_RA", s.url("/doiRA/{doi}"))
    monkeypatch.setattr(R, "DOI_CN", s.url("/cn/{doi}"))
    return s


# ---------------------------------------------------------------- typed failures (REG-I46)
def test_found_record_is_returned(api):
    api.script("/works/10.1249/jsr.0b013e31825615cc", Reply(200, body("crossref_casa_subtitle.json"), JSON))
    msg = R.crossref_by_doi("10.1249/JSR.0b013e31825615cc")
    assert msg["subtitle"] == ["New Concepts Regarding Cause and Care"]


def test_not_found_is_none(api):
    api.script("/works/10.1234/missing.1", Reply(404, "Resource not found.", {"Content-Type": "text/plain"}))
    assert R.crossref_by_doi("10.1234/missing.1") is None


@pytest.mark.parametrize("reply, kind", [(Reply(503), Kind.OUTAGE), (Reply(close=True), Kind.TRANSPORT),
                                         (Reply(200, "<html>maintenance</html>", {"Content-Type": "text/html"}), Kind.OUTAGE),
                                         (Reply(403), Kind.REFUSED)])
def test_failed_call_raises_never_none(api, reply, kind):
    api.script("/works/10.1234/down.1", reply)
    with pytest.raises(R.MetadataUnavailable) as e:
        R.crossref_by_doi("10.1234/down.1")
    assert e.value.kind is kind and e.value.source == "crossref"


def test_datacite_failure_raises_and_not_found_is_none(api):
    api.script("/dois/10.5281/zenodo.1", Reply(500))
    api.script("/dois/10.5281/zenodo.2", Reply(404))
    with pytest.raises(R.MetadataUnavailable):
        R.datacite_by_doi("10.5281/zenodo.1")
    assert R.datacite_by_doi("10.5281/zenodo.2") is None


def test_resolve_meta_raises_on_a_crossref_outage_without_asking_datacite(api):
    api.script("/works/10.1234/down.1", Reply(503))
    api.script("/dois/10.1234/down.1", Reply(200, "{}", JSON))
    with pytest.raises(R.MetadataUnavailable):
        R.resolve_meta("10.1234/down.1")
    assert api.hits_for("/dois/10.1234/down.1") == []          # an outage is not a Crossref miss


def test_emit_ris_for_pdf_reports_unavailable_and_writes_nothing(api, tmp_path):
    api.script("/works/10.1234/down.1", Reply(close=True))
    pdf = tmp_path / "2020_X_Title.pdf"
    assert R.emit_ris_for_pdf("10.1234/down.1", str(pdf)) == ("META_UNAVAILABLE", "")
    assert not (tmp_path / "2020_X_Title.ris").exists()


# ---------------------------------------------------------------- the sweep contract
def test_resolve_meta_shape_is_meta_and_source(api):
    api.script("/works/10.1249/jsr.0b013e31825615cc", Reply(200, body("crossref_casa_subtitle.json"), JSON))
    out = R.resolve_meta("10.1249/jsr.0b013e31825615cc")
    assert isinstance(out, tuple) and len(out) == 2
    meta, source = out
    assert source == "crossref" and meta["title"].endswith("Cause and Care")


def test_sweep_fill_metadata_sees_a_typed_failure(api, monkeypatch):
    import sweep
    api.script("/works/10.1234/down.1", Reply(503))
    row = {"doi": "10.1234/down.1", "title": "", "authors": "", "year": ""}
    assert sweep.fill_metadata(row, "10.1234/down.1") == "error:MetadataUnavailable"
    api.script("/works/10.1249/jsr.0b013e31825615cc", Reply(200, body("crossref_casa_subtitle.json"), JSON))
    row = {"doi": "10.1249/jsr.0b013e31825615cc", "title": "", "authors": "", "year": ""}
    assert sweep.fill_metadata(row, "10.1249/jsr.0b013e31825615cc") == "filled"
    assert row["title"] == "Exertional Heat Stroke: New Concepts Regarding Cause and Care"
    assert row["year"] == "2012" and row["authors"].startswith("Casa DJ")


# ---------------------------------------------------------------- identity (DEC-13)
def test_no_module_level_identity_and_one_mailto(api, monkeypatch):
    assert not hasattr(R, "UA") and not hasattr(R, "EMAIL")
    api.script("/works/10.1234/abc.1", Reply(200, '{"message": {"DOI": "10.1234/abc.1", "title": ["T"]}}', JSON))
    R.crossref_by_doi("10.1234/abc.1")
    ua = api.hits[-1].headers["User-Agent"]
    assert ua.count("mailto:") == 1 and "mailto:tester@litpipe-test.org" in ua


def test_unset_email_sends_no_mailto_at_all(api, monkeypatch):
    monkeypatch.delenv("LITPIPE_EMAIL", raising=False)
    api.script("/works/10.1234/abc.1", Reply(200, '{"message": {"DOI": "10.1234/abc.1", "title": ["T"]}}', JSON))
    R.crossref_by_doi("10.1234/abc.1")
    ua = api.hits[-1].headers["User-Agent"]
    assert "mailto" not in ua and "None" not in ua


# ---------------------------------------------------------------- DOIs in URL paths (I47)
def test_doi_is_normalised_before_it_is_encoded(api):
    api.script("/works/10.1056/nejmc1113675", Reply(200, '{"message": {"DOI": "10.1056/nejmc1113675", "title": ["T"]}}', JSON))
    assert R.crossref_by_doi("https://doi.org/10.1056/NEJMc1113675#sa3")["DOI"] == "10.1056/nejmc1113675"
    sici = "10.1002/(sici)1097-4636(199706)35:4<401::aid-jbm1>3.0.co;2-x"
    enc = "/works/10.1002/(sici)1097-4636(199706)35:4%3C401::aid-jbm1%3E3.0.co;2-x"
    api.script(enc, Reply(200, json.dumps({"message": {"DOI": sici, "title": ["T"]}}), JSON))
    assert R.crossref_by_doi(sici)["DOI"] == sici
    assert api.hits[-1].path == enc


def test_alias_redirect_is_followed_to_the_prime_record(api):
    prime = "10.5790/hongkong/9789888528011.003.0007"
    api.script("/works/10.1515/9789882204508-009", Reply(301, headers={"Location": api.url(f"/works/{prime}")}))
    api.script(f"/works/{prime}", Reply(200, body("crossref_alias_prime.json"), JSON))
    meta, source = R.resolve_meta("10.1515/9789882204508-009")
    assert source == "crossref" and meta["doi"] == prime


# ---------------------------------------------------------------- title search
def test_title_search_uses_query_bibliographic_and_the_print_first_year(api):
    api.script("/works", Reply(200, body("crossref_search_casa.json"), JSON))
    t = "Exertional heat stroke: new concepts regarding cause and care"
    it = R.crossref_by_title(t, "Casa", "2012")
    assert it["DOI"].lower() == "10.1249/jsr.0b013e31825615cc"
    q = api.hits[-1].query
    assert "query.bibliographic" in q and "query.title" not in q and q["query.author"] == ["Casa"]
    assert R.crossref_by_title(t, "Casa", "2016") is None            # year off by more than one


def test_title_search_matches_a_title_given_without_its_subtitle(api):
    api.script("/works", Reply(200, body("crossref_search_casa.json"), JSON))
    # a PDF-derived title that lost its subtitle still matches on the main title ...
    assert R.crossref_by_title("Exertional Heat Stroke", "Casa")["DOI"].lower() == "10.1249/jsr.0b013e31825615cc"
    # ... and one that kept it (no colon) matches on title + subtitle
    it = R.crossref_by_title("Exertional Heat Stroke New Concepts Regarding Cause and Care", "Casa")
    assert it["DOI"].lower() == "10.1249/jsr.0b013e31825615cc"


def test_title_search_failure_raises(api):
    api.script("/works", Reply(502))
    with pytest.raises(R.MetadataUnavailable):
        R.crossref_by_title("Exertional heat stroke: new concepts", "Casa")


# ---------------------------------------------------------------- DataCite
def test_datacite_asks_for_the_publisher_object(api):
    api.script("/dois/10.48550/arxiv.2605.29559", Reply(200, body("datacite_arxiv_publisher_object.json"), JSON))
    attrs = R.datacite_by_doi("10.48550/arXiv.2605.29559")
    assert api.hits[-1].query.get("publisher") == ["true"]
    assert R.datacite_meta(attrs)["container"] == "arXiv"


# ---------------------------------------------------------------- other agencies: content negotiation
@pytest.fixture
def cn(api, mock_server):
    """A (the doi.org stand-in) redirects CSL requests to B (an agency host on its allow-list)."""
    hosts.register(HostPolicy(A, min_interval_s=0.0, redirect_allow=(B,)))
    return api, mock_server(B)


def test_other_agency_resolves_through_content_negotiation(cn, net_env):
    a, b = cn
    doi = "10.3305/nh.2015.31.3.8434"
    for path in (f"/works/{doi}", f"/dois/{doi}"):
        a.script(path, Reply(404))
    a.script(f"/doiRA/{doi}", Reply(200, body("doira_medra.json"), JSON))
    a.script(f"/cn/{doi}", Reply(302, headers={"Location": b.url("/medra")}))
    b.script("/medra", Reply(200, body("csl_medra.json"), {"Content-Type": R.CSL_JSON}))
    meta, source = R.resolve_meta(doi)
    assert source == "cn:medra" and meta["container"] == "NUTRICION HOSPITALARIA"
    assert b.hits[-1].headers["Accept"] == R.CSL_JSON              # the Accept header rides the hop
    assert net_env.state.kv[("doi_ra", "10.3305")] == "medra"       # cached per prefix
    # the next DOI of that prefix goes straight to content negotiation
    doi2 = "10.3305/nh.2015.31.3.8435"
    a.script(f"/cn/{doi2}", Reply(302, headers={"Location": b.url("/medra")}))
    meta2, source2 = R.resolve_meta(doi2)
    assert source2 == "cn:medra"
    assert a.hits_for(f"/works/{doi2}") == [] and a.hits_for(f"/dois/{doi2}") == []
    assert a.hits_for(f"/doiRA/{doi2}") == []


def test_redirect_off_the_allow_list_is_not_followed(cn, mock_server):
    a, _ = cn
    c = mock_server(C)
    c.script("/x", Reply(200, "{}", JSON))
    a.script("/cn/10.3978/j.issn.2072-1439.2014.12.21", Reply(302, headers={"Location": c.url("/x")}))
    with pytest.raises(R.MetadataUnavailable) as e:
        R.csl_by_doi("10.3978/j.issn.2072-1439.2014.12.21")
    assert e.value.kind is Kind.REFUSED and c.hits == []


def test_content_negotiation_204_and_404_are_not_found(cn):
    a, _ = cn
    a.script("/cn/10.1234/none.1", Reply(204))
    a.script("/cn/10.1234/none.2", Reply(404))
    assert R.csl_by_doi("10.1234/none.1") is None
    assert R.csl_by_doi("10.1234/none.2") is None


def test_istic_is_unsupported_and_never_negotiated(api, net_env):
    doi = "10.3978/j.issn.2072-1439.2014.12.21"
    for path in (f"/works/{doi}", f"/dois/{doi}"):
        api.script(path, Reply(404))
    api.script(f"/doiRA/{doi}", Reply(200, json.dumps([{"DOI": doi, "RA": "ISTIC"}]), JSON))
    assert R.resolve_meta(doi) == ({}, "unsupported:istic")
    assert api.hits_for(f"/cn/{doi}") == []


def test_doi_that_no_agency_holds_is_none_and_not_cached(api, net_env):
    doi = "10.1234/ghost.1"
    for path in (f"/works/{doi}", f"/dois/{doi}"):
        api.script(path, Reply(404))
    api.script(f"/doiRA/{doi}", Reply(200, json.dumps([{"DOI": doi, "status": "DOI does not exist"}]), JSON))
    assert R.resolve_meta(doi) == ({}, "none")
    assert ("doi_ra", "10.1234") not in net_env.state.kv


def test_doi_ra_outage_raises(api):
    doi = "10.1234/ghost.2"
    for path in (f"/works/{doi}", f"/dois/{doi}"):
        api.script(path, Reply(404))
    api.script(f"/doiRA/{doi}", Reply(503))
    with pytest.raises(R.MetadataUnavailable):
        R.resolve_meta(doi)
