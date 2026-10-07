"""W2a verifier A: the PMC stage end to end with the REAL W2-A2 modules, and the gaps the builders'
own tests could not see.

tests/test_pmc_stage.py fakes jats_to_text.fetch_jats_xml, parse_bioc and fetch_figures.figures_from_s3
(W2-A1 built against a written contract). Here pmc_fetch.run drives the real jats_to_text
(fetch_jats_xml with its 500 handling, parse_jats, parse_bioc) and the real
fetch_figures.figures_from_s3 through the REAL litpipe.net into a scripted transport (the
test_pmc_stage World: recorded idconv / Europe PMC / E-utilities / S3 answers), temp state and ledger.

Tests commented `lock for APPLY V-A<n>` lock an APPLY item of the verifier's report: each failed on
the code at 0730c49 (checked with --runxfail) and passes with the fix.
"""
import csv
import hashlib
import json
import sys
from pathlib import Path

import pytest
import requests
from requests.structures import CaseInsensitiveDict

import fetch_figures
import jats_to_text
import lit_net
import pmc_fetch
from litpipe import hosts, net
from litpipe.outcomes import Kind, from_legacy
from tests.test_pmc_stage import (AM_DOI, AM_PMC, EMB_DOI, EMB_PMC, NONE_DOI, NONE_PMC, OA_DOI, OA_PMC,
                                  OA_TITLE, Down, World, assert_sanctioned_only, fx, make_pdf, md5, run_stage)

FIX2 = Path(__file__).resolve().parent / "fixtures" / "W2-A2"
ENT_DOI, ENT_PMC = "10.3390/e26110970", "PMC11592912"
ENT_TITLE = ("Complexity and Variation in Infectious Disease Birth Cohorts: Findings from HIV+ Medicare and "
             "Medicaid Beneficiaries, 1999-2020")
EPMC_500 = (FIX2 / "epmc_fulltextxml_500.json").read_bytes()


def jpeg(tag: str) -> bytes:
    return b"\xff\xd8\xff\xe0" + b"\x00\x10JFIF\x00" + tag.encode() + b"\xff\xd9"


class RealWorld(World):
    """The test_pmc_stage World plus the routes the real W2-A2 modules use: Europe PMC fullTextXML
    and S3 media objects, and one more OA article (Entropy PMC11592912, with 8 JATS figures and the
    S3 media list of its recorded metadata)."""

    def __init__(self):
        super().__init__()
        self.fulltext = {}                 # PMCID -> (status, body)
        self.media = {}                    # S3 path -> bytes
        jats = (FIX2 / "jats_PMC11592912_efetch.xml").read_bytes()
        self.idconv[ENT_DOI] = {"doi": ENT_DOI, "pmcid": ENT_PMC, "pmid": 39593914, "requested-id": ENT_DOI}
        self.efetch[ENT_PMC] = jats.decode("utf-8")
        self.s3_list[ENT_PMC] = fx("s3_list_PMC4924218.xml").replace(b"PMC4924218", ENT_PMC.encode())
        self.xml[ENT_PMC] = jats
        self.pdf[ENT_PMC] = make_pdf(f"Entropy 2024, 26, 970; doi:{ENT_DOI}", ENT_TITLE)
        meta = json.loads((FIX2 / "s3_meta_PMC11592912.1.json").read_text(encoding="utf-8"))
        meta["title"] = ENT_TITLE
        meta["pdf_url"] = f"s3://pmc-oa-opendata/{ENT_PMC}.1/{ENT_PMC}.1.pdf?md5={md5(self.pdf[ENT_PMC])}"
        meta["xml_url"] = f"s3://pmc-oa-opendata/{ENT_PMC}.1/{ENT_PMC}.1.xml?md5={md5(jats)}"
        media = []
        for u in meta["media_urls"]:
            key = u.split("?")[0][len("s3://pmc-oa-opendata"):]
            body = jpeg(key)
            self.media[key] = body
            media.append(f"s3://pmc-oa-opendata{key}?md5={md5(body)}")
        meta["media_urls"] = media
        self.meta[f"{ENT_PMC}.1"] = meta
        # the World's synthetic OA media URL (md5 of zeros) is not an image this test serves
        self.meta["PMC4924218.1"]["media_urls"] = []

    def route(self, host, path, q):
        if host == "www.ebi.ac.uk" and path.endswith("/fullTextXML"):
            pm = path.split("/rest/")[1].split("/")[0]
            st, body = self.fulltext.get(pm, (500, EPMC_500))
            ctype = "application/xml" if st == 200 else "application/json"
            return st, body, {"Content-Type": ctype}
        if host == "pmc-oa-opendata.s3.amazonaws.com" and path in self.media:
            return 200, self.media[path], {"Content-Type": "image/jpeg", "ETag": '"0123abcd-2"'}
        return super().route(host, path, q)


@pytest.fixture
def real(net_env, monkeypatch):
    """No W2-A2 fake: the stage reaches the real jats_to_text and fetch_figures."""
    w = RealWorld()
    for name in list(net._TRANSPORTS):
        monkeypatch.setitem(net._TRANSPORTS, name, w)
    w.env = net_env
    w.ris_calls = []
    monkeypatch.setattr(pmc_fetch._R, "emit_ris_for_pdf",
                        lambda doi, path: (w.ris_calls.append((doi, path)) or ("OK", None)))
    assert pmc_fetch._figures_fn() is fetch_figures.figures_from_s3
    assert pmc_fetch._parse_bioc_fn() is jats_to_text.parse_bioc
    return w


def sidecar_of(lib, row):
    return json.loads((lib / (row["filename"][:-4] + ".fulltext.json")).read_text(encoding="utf-8"))


# ============================================================================ 1. the seam, end to end
def test_seam_oa_row_pdf_sidecar_and_licensed_figures(real, tmp_path):
    """OA: PDF checked against the metadata md5 (the S3 ETag is a multipart one and never compared),
    a sidecar with has_pdf true from the S3 JATS through the real parse_jats, and the real
    figures_from_s3 writes 8 figure images with the article licence and its category."""
    real.pdf_headers = {"ETag": '"9b2cf535f27731c974343645a3985328-2"'}       # multipart: not an md5
    res, rows, lib = run_stage(tmp_path, [ENT_DOI], titles={ENT_DOI: ENT_TITLE})
    r = rows[ENT_DOI]
    assert r["downloaded"] == "True" and r["outcome"] == "OK" and r["route"] == "s3" and r["identity"] == "OK"
    assert (lib / r["filename"]).read_bytes() == real.pdf[ENT_PMC]
    sc = sidecar_of(lib, r)
    assert sc["has_pdf"] is True and sc["extractor"] == "pmc_s3_jats" and sc["doi"] == ENT_DOI
    assert sc["license"] == "CC BY" and sc["text"]
    figs = sc["figures"]
    assert r["figures"] == "8" and len(figs) == 8
    for i, f in enumerate(figs, 1):
        assert f["licence"] == "CC BY" and f["licence_category"] == "OK_WITH_CREDIT"
        assert f["image_status"] == "OK" and f["source"] == "pmc_s3"
        assert (lib / f["image_path"]).read_bytes() == real.media[f"/{ENT_PMC}.1/{f['graphic_href']}"]
    assert set(sc.get("figure_media_unmatched") or []) <= {k.rsplit("/", 1)[1] for k in real.media}
    assert real.ris_calls == [(ENT_DOI, str(lib / r["filename"]))]
    assert not real.hits_for("fullTextXML") and not real.hits_for("BioC_json")
    assert_sanctioned_only(real)
    assert res["downloaded"] == 1


def test_seam_am_row_s3_xml_first_text_only(real, tmp_path):
    _, rows, lib = run_stage(tmp_path, [AM_DOI])
    r = rows[AM_DOI]
    assert r["downloaded"] == "False" and r["sidecar_status"] == "OK" and r["pmc_class"] == "AM"
    assert not (lib / r["filename"]).exists()
    sc = sidecar_of(lib, r)
    assert sc["has_pdf"] is False and sc["extractor"] == "pmc_s3_jats" and sc["doi"] == AM_DOI and sc["text"]
    assert not real.hits_for("BioC_json") and not real.hits_for("fullTextXML")
    assert not any(h.path.endswith(".pdf") for h in real.hits)
    assert_sanctioned_only(real)


def test_seam_am_row_bioc_fallback_through_real_parse_bioc(real, tmp_path):
    real.override[("pmc-oa-opendata.s3.amazonaws.com", r"/PMC7983342\.1/.+\.xml")] = [
        (404, "<Error><Code>NoSuchKey</Code></Error>")]
    real.bioc[AM_PMC] = fx("bioc_PMC7983342_AM.json")
    _, rows, lib = run_stage(tmp_path, [AM_DOI])
    r = rows[AM_DOI]
    assert r["sidecar_status"] == "OK" and r["downloaded"] == "False"
    sc = sidecar_of(lib, r)
    assert sc["extractor"] == "pmc_bioc" and sc["has_pdf"] is False and sc["doi"] == AM_DOI
    assert sc["authors"] and "authors_may_include_editors" not in sc       # an AM list is authors only
    assert sc["text"] and sc["sections"]
    assert len(real.hits_for("BioC_json/PMC7983342")) == 1 and not real.hits_for("fullTextXML")
    assert "s3/NO_MATCH" in r["attempts"] and r["attempts"].endswith("bioc/OK")


def test_seam_am_bioc_429_is_sent_once_and_typed(real, tmp_path):
    """pmc_fetch has its own BioC GET (bioc_json), not jats_to_text.fetch_bioc: the host row's
    no-429-retry policy must hold for it too."""
    del real.meta["PMC7983342.1"]["xml_url"]
    real.override[("www.ncbi.nlm.nih.gov", r".*/BioC_json/.*")] = [
        (429, fx("bioc_absent.html"), {"Content-Type": "text/html"})]
    _, rows, _ = run_stage(tmp_path, [AM_DOI])
    r = rows[AM_DOI]
    assert len(real.hits_for("BioC_json")) == 1
    assert r["sidecar_status"] == "REFUSED" and "bioc/REFUSED" in r["attempts"]
    assert real.env.state.is_refused("www.ncbi.nlm.nih.gov")


def test_seam_embargoed_row_release_date_and_no_s3(real, tmp_path):
    _, rows, _ = run_stage(tmp_path, [EMB_DOI])
    r = rows[EMB_DOI]
    assert r["outcome"] == "EMBARGOED" and r["release_date"] == "2027-03-11" and r["pmcid"] == EMB_PMC
    assert from_legacy(r["error"], "pmc") is Kind.EMBARGOED
    assert not [h for h in real.hits if h.host == "pmc-oa-opendata.s3.amazonaws.com"]
    assert not real.hits_for("efetch") and not real.hits_for("fullTextXML")


def test_seam_none_row_flagged_oa_fulltextxml_500_is_one_request(real, tmp_path):
    """idconv refused, so Europe PMC maps the DOI and flags it OA; efetch says NONE; the real
    fetch_jats_xml sends fullTextXML once, its 500 is NOT_AVAILABLE, and the www.ebi.ac.uk row's
    retry policy is back to retrying 500 afterwards."""
    real.env.state.refuse("pmc.ncbi.nlm.nih.gov", "HTTP 429 (test)")
    real.epmc[NONE_DOI] = dict(real.epmc[NONE_DOI], isOpenAccess="Y")
    _, rows, lib = run_stage(tmp_path, [NONE_DOI])
    r = rows[NONE_DOI]
    assert len(real.hits_for(f"/{NONE_PMC}/fullTextXML")) == 1
    assert r["outcome"] == "NOT_AVAILABLE" and r["pmc_class"] == "NONE" and r["sidecar"] == "False"
    assert "epmc_fulltextxml/NOT_AVAILABLE" in r["attempts"]
    assert from_legacy(r["error"], "pmc") is Kind.NOT_AVAILABLE
    assert not [h for h in real.hits if h.host == "pmc-oa-opendata.s3.amazonaws.com"]
    assert 500 in hosts.policy("www.ebi.ac.uk").retry.statuses
    assert not real.env.state.is_refused("www.ebi.ac.uk")


def test_seam_s3_503_keeps_the_first_root_cause(real, tmp_path):
    """S3 down for an OA article: the OA fallback (real fullTextXML, 500, one request) runs, and the
    row carries S3's 503, not the fallback's NOT_AVAILABLE."""
    real.override[("pmc-oa-opendata.s3.amazonaws.com", r"/")] = [(503, "<Error><Code>SlowDown</Code></Error>")]
    _, rows, _ = run_stage(tmp_path, [OA_DOI], titles={OA_DOI: OA_TITLE})
    r = rows[OA_DOI]
    assert r["error"].startswith("s3/HTTP_503") and r["outcome"] == "OUTAGE" and r["first_status"] == "503"
    assert r["route"] == "s3" and from_legacy(r["error"], "pmc") is Kind.OUTAGE
    assert len(real.hits_for(f"/{OA_PMC}/fullTextXML")) == 1
    assert r["attempts"].endswith("epmc_fulltextxml/NOT_AVAILABLE")


def test_seam_every_row_path_writes_the_typed_columns(real, tmp_path):
    """Item 8: first_status/route/outcome on every failure path, and `error` maps through from_legacy
    to the same Kind as `outcome` (OK rows have an empty error)."""
    real.env.state.refuse("pmc.ncbi.nlm.nih.gov", "HTTP 429 (test)")
    miss = "10.1007/s40279-015-0365-0"
    _, rows, _ = run_stage(tmp_path, [ENT_DOI, AM_DOI, EMB_DOI, NONE_DOI, miss], titles={ENT_DOI: ENT_TITLE})
    for d, r in rows.items():
        assert r["outcome"], d
        if r["outcome"] == "OK":
            assert r["route"] == "s3" and r["first_status"] == "200" and r["error"] == ""
            continue
        assert r["route"] is not None and r["error"], d
        assert from_legacy(r["error"], "pmc") is Kind(r["outcome"]), (d, r["error"], r["outcome"])


# ============================================================================ 2. prohibited routes
@pytest.mark.parametrize("url", [
    "https://pmc.ncbi.nlm.nih.gov/articles/PMC123/",
    "https://pmc.ncbi.nlm.nih.gov/./articles/PMC123/pdf/x.pdf",
    "https://pmc.ncbi.nlm.nih.gov/tools/../articles/PMC123/",
    "https://pmc.ncbi.nlm.nih.gov//articles/PMC123/",
    "https://pmc.ncbi.nlm.nih.gov/%61rticles/PMC123/",
    "https://PMC.NCBI.NLM.NIH.GOV/articles/PMC123/",
    "https://pmc.ncbi.nlm.nih.gov./articles/PMC123/",
    "http://pmc.ncbi.nlm.nih.gov/articles/PMC123/",
    "https://pmc.ncbi.nlm.nih.gov:443/articles/PMC123/",
    "https://user@pmc.ncbi.nlm.nih.gov/articles/PMC123/",
    "https://cdn.ncbi.nlm.nih.gov/pmc/blobs/1/2/3/fig.jpg",
    "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC123/",
    "https://www.ncbi.nlm.nih.gov/pmc/./articles/PMC123/",
    "https://www.ncbi.nlm.nih.gov/pmc/%61rticles/PMC123/",
    "https://WWW.NCBI.NLM.NIH.GOV./pmc/articles/PMC123/",
])
def test_legacy_client_refuses_normalisation_tricks_unsent(url, monkeypatch):
    def boom(*a, **k):
        raise AssertionError(f"sent: {url}")
    monkeypatch.setattr(requests, "get", boom)
    r = lit_net.get(url)
    assert r.status_code == 0 and r.error.startswith("PROHIBITED")
    s = lit_net.stream_download(url, max_bytes=100)
    assert s.status_code == 0 and s.error.startswith("PROHIBITED")
    with pytest.raises(net.ProhibitedHost):
        net.get(url)


@pytest.mark.parametrize("url", [
    "https://europepmc.org/articles/PMC123?pdf=render",
    "https://EUROPEPMC.ORG./api/fulltextRepo?pprId=1&type=FILE&fileName=x.pdf",
    "http://www.europepmc.org/abstract/MED/1",
])
def test_litpipe_net_refuses_europepmc_website_routes(url):
    with pytest.raises(net.ProhibitedHost):
        net.get(url)


def _fake_adapter(monkeypatch, table):
    """requests' HTTPAdapter.send replaced: {url_prefix: (status, headers)}; records every URL that
    reached the wire, including redirect hops requests follows on its own."""
    sent = []

    def send(self, request, **kw):
        sent.append(request.url)
        for prefix, (status, hdrs) in table.items():
            if request.url.startswith(prefix):
                break
        else:
            status, hdrs = 200, {"Content-Type": "application/pdf"}
        resp = requests.Response()
        resp.status_code = status
        resp.headers = CaseInsensitiveDict(hdrs)
        resp.url = request.url
        resp.request = request
        resp.raw = __import__("io").BytesIO(b"%PDF-1.4 x")
        resp.connection = self
        return resp
    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    return sent


# lock for APPLY V-A7 (fixed in the dispatcher commit that follows 0730c49)
def test_legacy_client_does_not_follow_a_redirect_into_a_prohibited_route(monkeypatch):
    """requests follows a 30x on its own: the guard only sees the first URL."""
    target = "https://pmc.ncbi.nlm.nih.gov/articles/PMC123/pdf/x.pdf"
    sent = _fake_adapter(monkeypatch, {"https://example.org/": (302, {"Location": target})})
    lit_net.stream_download("https://example.org/paper.pdf", max_bytes=1000)
    lit_net.get("https://example.org/paper.pdf")
    assert target not in sent


# ============================================================================ 3. the idconv chain
def _chain_world(monkeypatch, replies):
    """A transport keyed by host: replies[host] is a list of (status, body[, headers]) or Down()."""
    sent = []

    def fake(method, url, hdrs, body, timeout, max_bytes):
        host = url.split("/")[2]
        sent.append(url)
        seq = replies[host]
        r = seq.pop(0) if len(seq) > 1 else seq[0]
        if isinstance(r, Down):
            return net._Raw(error="ConnectionError: reset")
        data = r[1].encode() if isinstance(r[1], str) else r[1]
        return net._Raw(r[0], CaseInsensitiveDict(r[2] if len(r) > 2 else {"Content-Type": "application/json"}),
                        data, data[:65536], len(data))
    for name in list(net._TRANSPORTS):
        monkeypatch.setitem(net._TRANSPORTS, name, fake)
    return sent


def test_chain_refused_mid_run_is_checked_per_chunk(net_env, monkeypatch):
    """idconv answers the first 100-DOI chunk with a 429 (refusing the host): the second chunk is not
    sent to idconv at all; every DOI goes on to Europe PMC; a down chain is never NO_MATCH."""
    dois = [f"10.1000/x{i}" for i in range(150)]
    sent = _chain_world(monkeypatch, {
        "pmc.ncbi.nlm.nih.gov": [(429, "<html>429</html>", {"Content-Type": "text/html"})],
        "www.ebi.ac.uk": [(503, "")],
        "eutils.ncbi.nlm.nih.gov": [Down()],
    })
    res = lit_net.doi_to_pmcid(dois)
    assert sum("pmc.ncbi.nlm.nih.gov" in u for u in sent) == 1
    assert {o.kind for o in res.values()} == {Kind.REFUSED}
    assert all(o.payload.source == "idconv" for o in res.values())
    assert all("NO_PMCID" not in pmc_fetch.legacy_error(o.payload.source, o) for o in res.values())


def test_chain_errcode_in_a_200_is_an_error_not_no_match(net_env, monkeypatch):
    sent = _chain_world(monkeypatch, {
        "pmc.ncbi.nlm.nih.gov": [(429, "x", {"Content-Type": "text/html"})],
        "www.ebi.ac.uk": [(200, json.dumps({"errCode": 400, "errMsg": "Syntax error"}))],
        "eutils.ncbi.nlm.nih.gov": [(503, "")],
    })
    res = lit_net.doi_to_pmcid(["10.1000/a"])
    o = res["10.1000/a"]
    assert o.kind is Kind.REFUSED and "epmc_search:ERROR" in o.payload.steps
    assert "eutils:OUTAGE" in o.payload.steps


def test_chain_definitive_answer_after_refusal_is_no_match(net_env, monkeypatch):
    """The recorded decision: a refused step is TRANSIENT only when no later step answered."""
    _chain_world(monkeypatch, {
        "pmc.ncbi.nlm.nih.gov": [(429, "x", {"Content-Type": "text/html"})],
        "www.ebi.ac.uk": [(200, json.dumps({"hitCount": 0, "resultList": {"result": []}}))],
        "eutils.ncbi.nlm.nih.gov": [(200, json.dumps({"esearchresult": {"count": "0", "idlist": []}}))],
    })
    assert lit_net.doi_to_pmcid(["10.1000/a"])["10.1000/a"].kind is Kind.NO_MATCH


def test_epmc_batches_respect_100_dois_and_4000_chars():
    dois = [f"10.1000/{'y' * 30}{i}" for i in range(250)]
    batches = lit_net.epmc_batches(dois)
    assert sum(len(b) for b in batches) == 250
    assert all(len(b) <= 100 and lit_net._epmc_url_len(b) <= 4000 for b in batches)


# lock for APPLY V-A4 (fixed in the dispatcher commit that follows 0730c49)
def test_backfill_report_does_not_call_a_refused_mapping_no_pmcid(net_env, monkeypatch, tmp_path):
    import backfill_fulltext
    lib = tmp_path / "lib"
    lib.mkdir()
    (lib / "2020_Doe_Heat.pdf").write_bytes(b"%PDF-1.4 stub")
    src = tmp_path / "dois.csv"
    src.write_text("filename,doi\n2020_Doe_Heat.pdf,10.1000/hx.2020.017\n", encoding="utf-8")
    _chain_world(monkeypatch, {
        "pmc.ncbi.nlm.nih.gov": [(429, "x", {"Content-Type": "text/html"})],
        "www.ebi.ac.uk": [(503, "")],
        "eutils.ncbi.nlm.nih.gov": [Down()],
    })
    monkeypatch.setattr(sys, "argv", ["backfill_fulltext.py", "--lib-dir", str(lib), "--doi-source", str(src)])
    backfill_fulltext.main()
    rows = list(csv.DictReader(open(lib / "_fulltext_backfill_report.csv", encoding="utf-8")))
    assert rows[0]["status"] != "NO_PMCID"
    assert from_legacy(rows[0]["status"]) in (Kind.REFUSED, Kind.OUTAGE, Kind.TRANSPORT)


# ============================================================================ 4. REG-I11
# lock for APPLY V-A1 (fixed in the dispatcher commit that follows 0730c49)
def test_reg_i11_a_different_paper_without_a_doi_does_not_block_the_fetch(real, tmp_path):
    lib = tmp_path / "lib"
    lib.mkdir()
    (lib / "Author_0_2020_paper0.pdf").write_bytes(make_pdf("Rowing economy in masters athletes",
                                                            "A scanned report with no identifier"))
    _, rows, lib = run_stage(tmp_path, [ENT_DOI], titles={ENT_DOI: ENT_TITLE})
    r = rows[ENT_DOI]
    assert r["winning_source"] != "ALREADY_EXISTS", "another paper's file was taken as this DOI"
    assert r["downloaded"] == "True" and r["filename"] != "Author_0_2020_paper0.pdf"


# lock for APPLY V-A2 (fixed in the dispatcher commit that follows 0730c49)
def test_identity_flag_is_not_fetched_on_the_next_run(real, tmp_path):
    real.pdf[ENT_PMC] = make_pdf("A cover page with nothing a reader can match")
    m = real.meta[f"{ENT_PMC}.1"]
    m["pdf_url"] = f"s3://pmc-oa-opendata/{ENT_PMC}.1/{ENT_PMC}.1.pdf?md5={md5(real.pdf[ENT_PMC])}"
    m["title"] = "Something else entirely"
    _, rows1, _ = run_stage(tmp_path, [ENT_DOI], titles={ENT_DOI: "An unrelated queue title about rowing"})
    assert rows1[ENT_DOI]["identity"] == "FLAG"
    _, rows2, _ = run_stage(tmp_path, [ENT_DOI], titles={ENT_DOI: "An unrelated queue title about rowing"})
    r = rows2[ENT_DOI]
    assert not (r["winning_source"] == "ALREADY_EXISTS" and r["outcome"] == "OK"), \
        "the flagged file now counts as the paper (holding by the sidecar's queue DOI)"


def test_reg_i11_same_paper_by_ris_is_still_skipped_before_any_request(real, tmp_path):
    """The positive side the fix must keep: a file whose .ris carries this DOI is ALREADY_EXISTS
    with no request sent."""
    lib = tmp_path / "lib"
    lib.mkdir()
    (lib / "Author_0_2020_paper0.pdf").write_bytes(b"%PDF-1.4 held")
    (lib / "Author_0_2020_paper0.ris").write_text(f"TY  - JOUR\nDO  - {ENT_DOI}\nER  - \n", encoding="utf-8")
    _, rows, _ = run_stage(tmp_path, [ENT_DOI])
    assert rows[ENT_DOI]["winning_source"] == "ALREADY_EXISTS" and real.hits == []


# ============================================================================ 5. the www.ebi.ac.uk stopgap
def test_fulltextxml_500_one_request_and_row_restored_even_on_error(net_env, monkeypatch):
    sent = _chain_world(monkeypatch, {"www.ebi.ac.uk": [(500, EPMC_500)]})
    assert jats_to_text.fetch_jats_xml("PMC1") == (None, "NOT_AVAILABLE")
    assert len(sent) == 1 and 500 in hosts.policy("www.ebi.ac.uk").retry.statuses

    def boom(*a, **k):
        raise RuntimeError("transport bug")
    for name in list(net._TRANSPORTS):
        monkeypatch.setitem(net._TRANSPORTS, name, boom)
    with pytest.raises(RuntimeError):
        jats_to_text.fetch_jats_xml("PMC1")
    assert 500 in hosts.policy("www.ebi.ac.uk").retry.statuses        # finally restored the row


def test_europe_pmc_search_500_is_still_retried(net_env, monkeypatch):
    """The stopgap must not leak: the same host's search call keeps retrying a 500."""
    sent = _chain_world(monkeypatch, {"pmc.ncbi.nlm.nih.gov": [(429, "x", {"Content-Type": "text/html"})],
                                      "www.ebi.ac.uk": [(500, "")], "eutils.ncbi.nlm.nih.gov": [Down()]})
    lit_net.doi_to_pmcid(["10.1000/a"])
    assert sum("webservices/rest/search" in u for u in sent) == 7      # 1 + 6 retries


def test_bioc_429_is_not_retried_through_jats_to_text(net_env, monkeypatch):
    sent = _chain_world(monkeypatch, {"www.ncbi.nlm.nih.gov": [(429, "<html></html>", {"Content-Type": "text/html"})]})
    data, st = jats_to_text.fetch_bioc("PMC1")
    assert data is None and st == "HTTP_429" and len(sent) == 1


# lock for APPLY V-A5 (fixed in the dispatcher commit that follows 0730c49)
def test_per_call_retry_statuses_option(net_env, monkeypatch):
    sent = _chain_world(monkeypatch, {"www.ebi.ac.uk": [(500, EPMC_500)]})
    o = net.get("https://www.ebi.ac.uk/europepmc/webservices/rest/PMC1/fullTextXML", retry_statuses=frozenset())
    assert o.status == 500 and len(sent) == 1
    assert not hasattr(jats_to_text, "_no_retry_on")


# ============================================================================ 6. harvest_citations
def test_harvest_metadata_unavailable_guard_is_not_empty_and_counts():
    """The harvest catches ris_emit.MetadataUnavailable looked up at call time (L-5: an import-time
    binding broke when tests/test_imports.py reloads ris_emit, which makes a new class object)."""
    import harvest_citations
    assert not hasattr(harvest_citations, "_META_UNAVAILABLE")
    cls = harvest_citations.R.MetadataUnavailable
    stats = {"metadata_lookup_failed": 0}

    def boom(doi):
        raise cls("Crossref: HTTP 503 after 6 retries")
    assert harvest_citations._crossref(boom, ("10.1000/x",), stats, "doi 10.1000/x") is None
    assert stats["metadata_lookup_failed"] == 1


# lock for APPLY V-A3 (fixed in the dispatcher commit that follows 0730c49)
def test_harvest_counts_a_kept_file_not_wrote(net_env, monkeypatch, tmp_path, capsys):
    import harvest_citations
    src = tmp_path / "src"
    src.mkdir()
    (src / "a.ris").write_text("TY  - JOUR\nTI  - Heat acclimation and plasma volume\nAU  - Doe, J\n"
                               "PY  - 2020\nDO  - 10.1000/hx.2020.017\nER  - \n", encoding="utf-8")
    out = tmp_path / "out"
    meta = {"title": "Heat acclimation and plasma volume", "lastname": "Doe", "year": "2020",
            "doi": "10.1000/hx.2020.017", "authors": [{"family": "Doe", "given": "J"}]}
    monkeypatch.setattr(harvest_citations.R, "crossref_by_doi", lambda doi: {"x": 1})
    monkeypatch.setattr(harvest_citations.R, "crossref_meta", lambda msg: dict(meta))
    monkeypatch.setattr(harvest_citations.R, "write_ris", lambda *a, **k: False)    # DEC-29: kept as curated
    monkeypatch.setattr(sys, "argv", ["harvest_citations.py", "--source-dir", str(src), "--out-dir", str(out),
                                      "--commit", "--overwrite", "--sleep", "0"])
    harvest_citations.main()
    text = capsys.readouterr().out
    assert "WROTE" not in text.split("== summary ==")[0]
    rows = list(csv.DictReader(open(out / "_index.csv", encoding="utf-8")))
    assert rows[0]["status"] != "WROTE"


# ============================================================================ 7. backfills/pmc_recovery.py
from tests.test_pmc_stage import recovery_root  # noqa: E402,F401  (the W2-A1 synthetic portfolio)


def _snapshot(root):
    return {str(p): (p.stat().st_mtime_ns, p.stat().st_size) for p in Path(root).rglob("*")}


def test_recovery_dry_run_touches_no_file_and_no_state(recovery_root, capsys):
    from backfills import pmc_recovery
    root, reg, arch, other = recovery_root
    before = _snapshot(root)
    s = pmc_recovery.run(report_dirs=[f"{arch}=alpha"], registry=reg)
    assert s["total_to_recover"] == 3
    assert _snapshot(root) == before                     # nothing written, nothing re-written
    assert not (root / "state").exists()                  # the state dir (holdings cache) untouched


@pytest.mark.parametrize("arg,want", [
    (r"C:\Users\some user\Projects\teaching\_archive\2026-09-30\lit_sweep_exhaust=teaching/course_a",
     (r"C:\Users\some user\Projects\teaching\_archive\2026-09-30\lit_sweep_exhaust", "teaching/course_a")),
    (r"C:\Users\some user\Projects\teaching\_archive", (r"C:\Users\some user\Projects\teaching\_archive", None)),
    (r"D:\x=", (r"D:\x", None)),
])
def test_recovery_dir_arg_parses_windows_paths(arg, want):
    from backfills import pmc_recovery
    d, key = pmc_recovery.parse_dir_arg(arg)
    assert (str(d), key) == (str(Path(want[0])), want[1])


def test_recovery_printed_commands_parse_with_the_real_clis(recovery_root, capsys, monkeypatch):
    """Every printed command line is valid for the argparse of the CLI it names (parsed, not run)."""
    import shlex

    import sweep
    from backfills import pmc_recovery
    root, reg, arch, other = recovery_root
    pmc_recovery.run(report_dirs=[f"{arch}=alpha"], registry=reg)
    lines = [ln.strip() for ln in capsys.readouterr().out.splitlines() if ln.strip().startswith("uv run")]
    assert len(lines) >= 4
    got = {}
    monkeypatch.setattr(pmc_recovery, "run", lambda **k: got.setdefault("pmc_recovery", []).append(k))
    monkeypatch.setattr(pmc_fetch, "run", lambda **k: got.setdefault("pmc_fetch", []).append(k))
    monkeypatch.setattr(sweep, "run", lambda *a, **k: got.setdefault("sweep", []).append(k) or {"exit_code": 0})
    for ln in lines:
        argv = [t[1:-1] if len(t) > 1 and t[0] == t[-1] == '"' else t for t in shlex.split(ln, posix=False)]
        assert argv[:5] == ["uv", "run", "--project", str(pmc_recovery.REPO), "python"], ln
        script, rest = Path(argv[5]).name, argv[6:]
        assert Path(argv[5]).is_file(), ln
        mod = {"pmc_recovery.py": pmc_recovery, "pmc_fetch.py": pmc_fetch, "sweep.py": sweep}[script]
        mod.main(rest)
    assert got["pmc_recovery"][0]["project"] == "alpha" and got["pmc_recovery"][0]["limit"] == 30
    assert got["pmc_recovery"][1]["stage"] is True
    assert got["pmc_fetch"][0]["report_in"].endswith("pmc_recovery_sample.csv")
    assert got["sweep"], "the sweep command did not reach sweep.run"


# ============================================================================ 9. identity never persisted
def test_no_email_in_report_sidecars_figures_or_ledger(real, tmp_path):
    real.env.state.refuse("pmc.ncbi.nlm.nih.gov", "HTTP 429 (test)")      # Europe PMC search + E-utilities path
    _, rows, lib = run_stage(tmp_path, [ENT_DOI, AM_DOI, NONE_DOI], titles={ENT_DOI: ENT_TITLE})
    blobs = [(tmp_path / "pmc.csv").read_text(encoding="utf-8"), real.env.ledger_text()]
    blobs += [p.read_text(encoding="utf-8") for p in lib.glob("*.json")]
    for needle in ("tester@litpipe-test.org", "tester%40litpipe-test.org", "tester%2540litpipe-test.org",
                   "email=None", "mailto:None"):
        assert not any(needle in b for b in blobs), needle
    ncbi = [h for h in real.hits if h.host in ("eutils.ncbi.nlm.nih.gov", "www.ncbi.nlm.nih.gov")]
    assert ncbi and all(h.query.get("email") == ["tester@litpipe-test.org"] for h in ncbi)
    s3 = [h for h in real.hits if h.host == "pmc-oa-opendata.s3.amazonaws.com"]
    assert s3 and all("mailto" not in h.headers.get("User-Agent", "") and "email" not in h.query for h in s3)


def test_no_email_param_when_unset(real, tmp_path, monkeypatch):
    monkeypatch.delenv("LITPIPE_EMAIL", raising=False)
    real.env.state.refuse("pmc.ncbi.nlm.nih.gov", "HTTP 429 (test)")
    run_stage(tmp_path, [ENT_DOI, AM_DOI], titles={ENT_DOI: ENT_TITLE})
    for h in real.hits:
        assert "email" not in h.query and "None" not in h.url and "mailto" not in h.headers.get("User-Agent", "")
