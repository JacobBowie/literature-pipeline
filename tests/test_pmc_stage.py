"""The PMC stage on sanctioned routes (W2-A1): offline replays of recorded answers.

Fixtures (tests/fixtures/W2-A1/) are trimmed, redacted copies of the endpoint audit's recorded
responses (2026-09-25) and of the W2-A1 verify-first probes (2026-09-30): idconv, Europe PMC search
(lite), E-utilities esearch / esummary / efetch db=pmc, PMC Cloud (S3) listings, metadata and JATS,
BioC. Every request runs through the REAL litpipe.net (host policy, prohibited check, refusals,
identity injection, validators, retries on a virtual clock) into a scripted transport, so a
request to a prohibited route raises before it is sent and a request nobody scripted fails the test.
The W2-A2 functions the stage calls (fetch_jats_xml, parse_bioc, figures_from_s3) are faked here.
"""
import copy
import csv
import hashlib
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest
from requests.structures import CaseInsensitiveDict

import jats_to_text
import lit_net
import pmc_fetch
from litpipe import hosts, net
from litpipe.outcomes import Kind, Outcome, from_legacy

FIX = Path(__file__).resolve().parent / "fixtures" / "W2-A1"
REPO = Path(__file__).resolve().parent.parent

OA_DOI, OA_PMC = "10.3390/nu8060377", "PMC4924218"
AM_DOI, AM_PMC = "10.1016/j.resp.2021.103638", "PMC7983342"
EMB_DOI, EMB_PMC = "10.1002/14651858.cd013617.pub2", "PMC12977955"
NEW_DOI, NEW_PMC = "10.21037/jss-20262-05", "PMC13598418"      # released 2026-09-30
NONE_DOI, NONE_PMC = "10.2337/db14-1771", "PMC4587640"
MISS_DOI = "10.1007/s40279-015-0365-0"                            # not in PMC
OA_TITLE = "Dietary Recommendations for Cyclists during Altitude Training"
SANCTIONED = {"pmc.ncbi.nlm.nih.gov", "www.ebi.ac.uk", "eutils.ncbi.nlm.nih.gov",
              "pmc-oa-opendata.s3.amazonaws.com", "www.ncbi.nlm.nih.gov"}


def fx(name):
    return (FIX / name).read_bytes()


def fxj(name):
    return json.loads((FIX / name).read_text(encoding="utf-8"))


def make_pdf(*lines) -> bytes:
    import fitz
    doc = fitz.open()
    page = doc.new_page()
    page.insert_text((72, 72), "\n".join(lines))
    return doc.tobytes()


def md5(b):
    return hashlib.md5(b).hexdigest()


class Down(Exception):
    """A handler result: the transport fails (no HTTP response)."""


@dataclass
class Hit:
    method: str
    url: str
    host: str
    path: str
    query: dict
    headers: dict


class World:
    """Recorded PMC answers behind a scripted litpipe.net transport. Tables are editable per test;
    `override[(host, path_regex)]` = a list of replies (status, body[, headers]) or Down(), the
    last repeating, replaces the table for that route."""

    def __init__(self):
        self.hits: list[Hit] = []
        self.override: dict = {}
        idc = fxj("idconv_2026-09-30.json")
        self.idconv = {r["requested-id"].lower(): r for r in idc["records"]}
        epmc = fxj("epmc_search_lite_2026-09-30.json")
        self.epmc = {r["doi"].lower(): r for r in epmc["resultList"]["result"]}
        esum = fxj("esummary_pmc_2026-09-30.json")["result"]
        self.esummary = {u: esum[u] for u in esum["uids"]}
        body = fx("efetch_pmc_2026-09-30.xml").decode("utf-8")
        self.efetch_head = body[:body.find("<article")]
        self.efetch = {}
        for art in re.split(r"(?=<article[\s>])", body)[1:]:
            art = art.replace("</pmc-articleset>", "")
            pm = re.search(r'pub-id-type="pmcid">(PMC\d+)', art).group(1)
            self.efetch[pm] = art
        self.pdf = {OA_PMC: make_pdf(f"Nutrients 2016, 8, 377; doi:{OA_DOI}", OA_TITLE)}
        self.xml = {OA_PMC: fx("s3_PMC4924218.1.xml"), AM_PMC: fx("s3_PMC7983342.1_AM.xml")}
        self.s3_list = {OA_PMC: fx("s3_list_PMC4924218.xml"), AM_PMC: fx("s3_list_PMC7983342.xml")}
        oa_meta = fxj("s3_meta_PMC4924218.1.json")
        oa_meta["pdf_url"] = f"s3://pmc-oa-opendata/PMC4924218.1/PMC4924218.1.pdf?md5={md5(self.pdf[OA_PMC])}"
        oa_meta["xml_url"] = f"s3://pmc-oa-opendata/PMC4924218.1/PMC4924218.1.xml?md5={md5(self.xml[OA_PMC])}"
        oa_meta["media_urls"] = ["s3://pmc-oa-opendata/PMC4924218.1/nutrients-08-00377-g001.jpg?md5=" + "0" * 32]
        am_meta = fxj("s3_meta_PMC7983342.1_AM.json")
        am_meta["xml_url"] = f"s3://pmc-oa-opendata/PMC7983342.1/PMC7983342.1.xml?md5={md5(self.xml[AM_PMC])}"
        self.meta = {"PMC4924218.1": oa_meta, "PMC7983342.1": am_meta}
        self.pdf_headers = {}
        self.bioc = {}

    # ------------------------------------------------------------------ routing
    def __call__(self, method, url, headers, body, timeout, max_bytes):
        parts = urlsplit(url)
        q = parse_qs(parts.query, keep_blank_values=True)
        self.hits.append(Hit(method, url, parts.hostname, parts.path, q, dict(headers)))
        reply = None
        for (host, rx), seq in self.override.items():
            if parts.hostname == host and re.fullmatch(rx, parts.path):
                reply = seq.pop(0) if len(seq) > 1 else seq[0]
                break
        if reply is None:
            reply = self.route(parts.hostname, parts.path, q)
        if isinstance(reply, Down):
            return net._Raw(error=f"ConnectionError: {reply}")
        status, data = reply[0], reply[1]
        hdrs = CaseInsensitiveDict(reply[2] if len(reply) > 2 else {})
        data = data.encode("utf-8") if isinstance(data, str) else data
        cap = max_bytes if max_bytes is not None else None
        trunc = cap is not None and len(data) > cap
        return net._Raw(status, hdrs, data[:cap] if trunc else data, data[:65536], len(data), trunc)

    def route(self, host, path, q):
        if host == "pmc.ncbi.nlm.nih.gov" and path == "/tools/idconv/api/v1/articles/":
            recs = []
            for i in q["ids"][0].split(","):
                r = self.idconv.get(i.lower())
                recs.append(dict(r, **{"requested-id": i}) if r else
                            {"requested-id": i, "status": "error", "errmsg": "Identifier not found in PMC"})
            return 200, json.dumps({"status": "ok", "records": recs}), {"Content-Type": "application/json"}
        if host == "www.ebi.ac.uk" and path == "/europepmc/webservices/rest/search":
            dois = re.findall(r'DOI:"([^"]+)"', q["query"][0])
            res = [self.epmc[d.lower()] for d in dois if d.lower() in self.epmc]
            return 200, json.dumps({"hitCount": len(res), "resultList": {"result": res}})
        if host == "eutils.ncbi.nlm.nih.gov" and path.endswith("/esearch.fcgi"):
            dois = [d.lower() for d in re.findall(r'"([^"]+)"\[doi\]', q["term"][0])]
            ids = [u for u, r in self.esummary.items()
                   if any(a["idtype"] == "doi" and a["value"].lower() in dois for a in r["articleids"])]
            return 200, json.dumps({"esearchresult": {"count": str(len(ids)), "retmax": str(len(ids)),
                                                      "idlist": ids}})
        if host == "eutils.ncbi.nlm.nih.gov" and path.endswith("/esummary.fcgi"):
            ids = q["id"][0].split(",")
            res = {"uids": [u for u in ids if u in self.esummary]}
            res.update({u: self.esummary[u] for u in res["uids"]})
            return 200, json.dumps({"header": {}, "result": res})
        if host == "eutils.ncbi.nlm.nih.gov" and path.endswith("/efetch.fcgi"):
            arts = "".join(self.efetch[f"PMC{i}"] for i in q["id"][0].split(",") if f"PMC{i}" in self.efetch)
            return 200, self.efetch_head + arts + "</pmc-articleset>", {"X-RateLimit-Limit": "3"}
        if host == "pmc-oa-opendata.s3.amazonaws.com":
            if path == "/":
                pm = q["prefix"][0].rstrip(".")
                return 200, self.s3_list.get(pm) or fx("s3_list_PMC12977955_empty.xml").replace(
                    b"PMC12977955", pm.encode())
            m = re.fullmatch(r"/metadata/(PMC\d+\.\d+)\.json", path)
            if m:
                if m.group(1) in self.meta:
                    return 200, json.dumps(self.meta[m.group(1)]), {"Content-Type": "binary/octet-stream"}
                return 404, "<Error><Code>NoSuchKey</Code></Error>"
            m = re.fullmatch(r"/(PMC\d+)\.\d+/.+\.(pdf|xml)", path)
            if m:
                src = self.pdf if m.group(2) == "pdf" else self.xml
                if m.group(1) in src:
                    hd = dict(self.pdf_headers) if m.group(2) == "pdf" else {}
                    hd.setdefault("ETag", f'"{md5(src[m.group(1)])}"')
                    return 200, src[m.group(1)], hd
                return 404, "<Error><Code>NoSuchKey</Code></Error>"
        if host == "www.ncbi.nlm.nih.gov" and "/research/bionlp/RESTful/pmcoa.cgi/BioC_json/" in path:
            pm = path.split("/BioC_json/")[1].split("/")[0]
            if pm in self.bioc:
                return 200, self.bioc[pm], {"Content-Type": "application/json"}
            return 200, fx("bioc_absent.html"), {"Content-Type": "text/html"}
        raise AssertionError(f"unscripted request: {host}{path}")

    # ------------------------------------------------------------------ assertions
    def hosts_hit(self):
        return {h.host for h in self.hits}

    def hits_for(self, needle):
        return [h for h in self.hits if needle in h.url]


@pytest.fixture
def world(net_env, monkeypatch):
    w = World()
    for name in list(net._TRANSPORTS):
        monkeypatch.setitem(net._TRANSPORTS, name, w)
    w.env = net_env
    w.ris_calls = []
    monkeypatch.setattr(pmc_fetch._R, "emit_ris_for_pdf", lambda doi, path: (w.ris_calls.append((doi, path)) or ("OK", None)))
    w.fulltext_calls = []
    w.fulltext_reply = (None, "HTTP_500")

    def fake_fetch_jats_xml(pmcid, ua=None, timeout=30):
        w.fulltext_calls.append(pmcid)
        return w.fulltext_reply
    monkeypatch.setattr(jats_to_text, "fetch_jats_xml", fake_fetch_jats_xml)
    w.figure_calls = []

    def fake_figures(article_meta, sidecar_path, *, lib_dir=None):
        w.figure_calls.append((article_meta.get("pmcid"), str(sidecar_path)))
        return [{"file": "fig1.jpg", "licence": "OK_WITH_CREDIT"}]
    monkeypatch.setattr(pmc_fetch, "_figures_fn", lambda: fake_figures)

    def fake_parse_bioc(bioc):
        docs = json.loads(bioc) if isinstance(bioc, (bytes, str)) else bioc
        doc = (docs[0] if isinstance(docs, list) else docs)["documents"][0]
        text = "\n".join(p.get("text", "") for p in doc.get("passages", []))
        return {"pmcid": "PMC" + str(doc.get("id", "")), "doi": "", "title": "", "text": text,
                "sections": [], "figures": [], "tables": [], "formulas": [], "n_formulas": 0}
    monkeypatch.setattr(pmc_fetch, "_parse_bioc_fn", lambda: fake_parse_bioc)
    return w


def write_report_in(path, dois, titles=None):
    titles = titles or {}
    with open(path, "w", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(f, fieldnames=["rank", "doi", "year", "cites", "filename", "title", "oa_status",
                                           "n_locations", "downloaded", "winning_host", "winning_url",
                                           "attempts", "error"])
        wr.writeheader()
        for i, d in enumerate(dois):
            wr.writerow({"rank": i, "doi": d, "filename": f"Author_{i}_2020_paper{i}.pdf",
                         "title": titles.get(d, ""), "oa_status": "CLOSED", "downloaded": "False"})


def run_stage(tmp_path, dois, titles=None, **kw):
    rin = tmp_path / "unpaywall.csv"
    write_report_in(rin, dois, titles)
    lib = tmp_path / "lib"
    rout = tmp_path / "pmc.csv"
    res = pmc_fetch.run(base_dir=str(tmp_path), report_in=str(rin), lib_dir=str(lib), report_out=str(rout), **kw)
    with open(rout, encoding="utf-8") as f:
        rows = {r["doi"]: r for r in csv.DictReader(f)}
    return res, rows, lib


def assert_sanctioned_only(world):
    for h in world.hits:
        assert h.host in SANCTIONED, h.url
        assert hosts.prohibited(h.url) is None, h.url
    assert not any(line.get("decision") == "prohibited" for line in world.env.ledger_lines())


# ============================================================================ classification
def test_efetch_fixture_classifies_oa_am_none_embargo():
    got = pmc_fetch.parse_efetch(fx("efetch_pmc_2026-09-30.xml").decode("utf-8"))
    assert {k: v.klass for k, v in got.items()} == {
        OA_PMC: "OA", NEW_PMC: "OA", NONE_PMC: "NONE", AM_PMC: "AM", EMB_PMC: "EMBARGOED"}
    assert got[OA_PMC].license == "CC BY"
    assert got[EMB_PMC].flags["pmc-status-embargo"] == "yes"


def test_efetch_reads_article_meta_flags_not_journal_meta():
    art = ('<pmc-articleset><article><front><journal-meta><custom-meta-group><custom-meta>'
           '<meta-name>pmc-prop-open-access</meta-name><meta-value>yes</meta-value></custom-meta>'
           '</custom-meta-group></journal-meta><article-meta><article-id pub-id-type="pmcid">PMC1</article-id>'
           '<custom-meta-group><custom-meta><meta-name>pmc-prop-open-access</meta-name><meta-value>no'
           '</meta-value></custom-meta></custom-meta-group></article-meta></front></article></pmc-articleset>')
    assert pmc_fetch.parse_efetch(art)["PMC1"].klass == "NONE"


def test_classify_batches_30_per_efetch_call(world):
    world.efetch.update({f"PMC{900 + i}": world.efetch[OA_PMC].replace(OA_PMC, f"PMC{900 + i}")
                         for i in range(40)})
    res = pmc_fetch.classify_pmcids([f"PMC{900 + i}" for i in range(40)])
    calls = world.hits_for("efetch.fcgi")
    assert len(calls) == 2 and len(calls[0].query["id"][0].split(",")) == 30
    assert all(o.ok and o.payload.klass == "OA" for o in res.values())


def test_classify_failure_is_typed_per_pmcid(world):
    world.override[("eutils.ncbi.nlm.nih.gov", r".*/efetch\.fcgi")] = [(503, "busy")]
    res = pmc_fetch.classify_pmcids([OA_PMC, AM_PMC])
    assert {o.kind for o in res.values()} == {Kind.OUTAGE}


# ============================================================================ PMC Cloud
def test_s3_https_parses_metadata_urls():
    assert pmc_fetch.s3_https("s3://pmc-oa-opendata/PMC1.1/PMC1.1.pdf?md5=" + "a" * 32) == (
        "https://pmc-oa-opendata.s3.amazonaws.com/PMC1.1/PMC1.1.pdf", "a" * 32)
    assert pmc_fetch.s3_https("https://elsewhere/x.pdf") == (None, None)


def test_s3_versions_takes_every_version_latest_last(world):
    world.s3_list["PMC7988253"] = fx("s3_list_PMC7988253_two_versions.xml")
    o = pmc_fetch.s3_versions("PMC7988253")
    assert o.ok and o.payload == ["PMC7988253.1", "PMC7988253.2"]
    assert pmc_fetch.s3_versions(EMB_PMC).payload == []


def test_md5_comes_from_the_metadata_not_the_etag(world):
    """V1-N6: a multipart upload's ETag is not an md5; the body's md5 must match `?md5=`."""
    meta = world.meta["PMC4924218.1"]
    world.pdf_headers = {"ETag": '"9b2cf535f27731c974343645a3985328-3"'}      # multipart form
    ok = pmc_fetch.s3_object(meta["pdf_url"], pdf=True)
    assert ok.ok and ok.payload == world.pdf[OA_PMC]
    # a body that is not the metadata's file fails even when the ETag claims the metadata md5
    want = meta["pdf_url"].split("md5=")[1]
    world.pdf[OA_PMC] = make_pdf("some other file")
    world.pdf_headers = {"ETag": f'"{want}"'}
    bad = pmc_fetch.s3_object(meta["pdf_url"], pdf=True)
    assert bad.kind is Kind.ERROR and "md5 mismatch" in bad.detail


def test_bioc_absent_is_not_available(world):
    o = pmc_fetch.bioc_json("PMC8029826")
    assert o.kind is Kind.NOT_AVAILABLE and o.status == 200


@pytest.mark.parametrize("reply,kind", [((None, "HTTP_500"), Kind.NOT_AVAILABLE),
                                        ((None, "NOT_AVAILABLE"), Kind.NOT_AVAILABLE),
                                        ((None, "HTTP_429"), Kind.REFUSED),
                                        ((None, "HTTP_503"), Kind.OUTAGE),
                                        ((b"<article/>", "OK"), Kind.OK)])
def test_fulltextxml_500_is_not_available(monkeypatch, reply, kind):
    monkeypatch.setattr(jats_to_text, "fetch_jats_xml", lambda pmcid, ua=None, timeout=30: reply)
    assert pmc_fetch._fulltextxml_outcome("PMC1").kind is kind


def test_fulltextxml_transport_exception_is_typed(monkeypatch):
    def boom(pmcid, ua=None, timeout=30):
        raise TimeoutError("timed out")
    monkeypatch.setattr(jats_to_text, "fetch_jats_xml", boom)
    assert pmc_fetch._fulltextxml_outcome("PMC1").kind is Kind.TRANSPORT


# ============================================================================ the acceptance replay
def test_replay_oa_am_embargo_none_missing(world, tmp_path):
    dois = [OA_DOI, AM_DOI, EMB_DOI, NONE_DOI, MISS_DOI, NEW_DOI]
    world.efetch[NEW_PMC] = world.efetch[NEW_PMC]            # released today: OA in efetch, not in S3 yet
    res, rows, lib = run_stage(tmp_path, dois, titles={OA_DOI: OA_TITLE})

    oa = rows[OA_DOI]
    assert oa["downloaded"] == "True" and oa["winning_source"] == "pmc_s3"
    assert oa["outcome"] == "OK" and oa["route"] == "s3" and oa["identity"] == "OK"
    pdf = lib / oa["filename"]
    assert pdf.read_bytes() == world.pdf[OA_PMC]
    sc = json.loads((lib / (oa["filename"][:-4] + ".fulltext.json")).read_text(encoding="utf-8"))
    assert sc["doi"] == OA_DOI and sc["has_pdf"] is True and sc["extractor"] == "pmc_s3_jats"
    assert sc["identity"] == "OK" and sc["license"] == "CC BY" and sc["text"]
    assert world.figure_calls == [(OA_PMC, str(lib / (oa["filename"][:-4] + ".fulltext.json")))]
    assert oa["figures"] == "1"
    assert world.ris_calls == [(OA_DOI, str(pdf))]

    am = rows[AM_DOI]
    assert am["downloaded"] == "False" and am["sidecar"] == "True" and am["sidecar_status"] == "OK"
    assert not (lib / am["filename"]).exists()
    amsc = json.loads((lib / (am["filename"][:-4] + ".fulltext.json")).read_text(encoding="utf-8"))
    assert amsc["has_pdf"] is False and amsc["doi"] == AM_DOI and amsc["license"] == "TDM" and amsc["text"]
    assert am["outcome"] == "NOT_AVAILABLE" and "author manuscript" in am["error"]

    emb = rows[EMB_DOI]
    assert emb["outcome"] == "EMBARGOED" and emb["release_date"] == "2027-03-11" and emb["pmcid"] == EMB_PMC
    assert "EMBARGOED until 2027-03-11" in emb["error"]

    none = rows[NONE_DOI]
    assert none["outcome"] == "NOT_AVAILABLE" and none["route"] == "efetch" and none["pmc_class"] == "NONE"

    miss = rows[MISS_DOI]
    assert miss["outcome"] == "NO_MATCH" and miss["error"].startswith("NO_PMCID")

    new = rows[NEW_DOI]
    assert new["pmcid"] == NEW_PMC and new["outcome"] == "NOT_AVAILABLE"
    assert "not in the PMC Cloud datasets" in new["error"]

    # nothing was requested for the embargoed and the publisher-copyright article beyond efetch
    for pm in (EMB_PMC, NONE_PMC):
        assert not any(pm in h.url for h in world.hits if h.host == "pmc-oa-opendata.s3.amazonaws.com")
    assert world.fulltext_calls == [NEW_PMC]          # OA per efetch, missing from S3: the OA-only fallback
    assert_sanctioned_only(world)
    assert res["downloaded"] == 1 and res["text_only"] == 1 and res["embargoed"] == 1 and res["no_pmcid"] == 1
    header = (tmp_path / "pmc.csv").read_text(encoding="utf-8").splitlines()[0].split(",")
    assert header[:10] == pmc_fetch.LEGACY_FIELDS
    assert {"first_status", "route", "outcome"} <= set(header)


def test_report_rows_route_through_sweep_as_expected(world, tmp_path):
    """sweep reads the legacy columns until W2-G switches it to `outcome`."""
    import sweep
    _, rows, _ = run_stage(tmp_path, [OA_DOI, AM_DOI, NONE_DOI, MISS_DOI], titles={OA_DOI: OA_TITLE})
    cls = {d: sweep.classify([sweep.pmc_verdict(r)])[0] for d, r in rows.items()}
    assert cls == {OA_DOI: "fetched", AM_DOI: "TEXT_ONLY", NONE_DOI: "TERMINAL_CLOSED",
                   MISS_DOI: "TERMINAL_CLOSED"}


def test_identity_flag_keeps_the_file_in_place(world, tmp_path):
    world.pdf[OA_PMC] = make_pdf("A scanned page with no text a reader can match")
    world.meta["PMC4924218.1"]["pdf_url"] = (
        f"s3://pmc-oa-opendata/PMC4924218.1/PMC4924218.1.pdf?md5={md5(world.pdf[OA_PMC])}")
    world.meta["PMC4924218.1"]["title"] = "Something else entirely"
    _, rows, lib = run_stage(tmp_path, [OA_DOI])
    r = rows[OA_DOI]
    assert r["identity"] == "FLAG" and r["downloaded"] == "False" and r["error"].startswith("DOI_MISMATCH")
    assert (lib / r["filename"]).exists()                               # never moved
    assert not (lib / "_mismatch").exists()
    sc = json.loads((lib / (r["filename"][:-4] + ".fulltext.json")).read_text(encoding="utf-8"))
    assert sc["identity"] == "FLAG" and "identity_evidence" in sc and sc["has_pdf"] is True
    assert world.ris_calls == []                                        # no .ris on a flagged file


def test_identity_flag_is_recorded_even_with_no_sidecar(world, tmp_path):
    world.pdf[OA_PMC] = make_pdf("no text to match")
    world.meta["PMC4924218.1"]["pdf_url"] = (
        f"s3://pmc-oa-opendata/PMC4924218.1/PMC4924218.1.pdf?md5={md5(world.pdf[OA_PMC])}")
    world.meta["PMC4924218.1"]["title"] = "Something else entirely"
    _, rows, lib = run_stage(tmp_path, [OA_DOI], no_sidecar=True)
    sc = json.loads((lib / (rows[OA_DOI]["filename"][:-4] + ".fulltext.json")).read_text(encoding="utf-8"))
    assert sc["identity"] == "FLAG" and sc["extractor"] == "identity_only"


def test_existing_text_sidecar_gets_the_pdf_verdict(world, tmp_path):
    lib = tmp_path / "lib"
    lib.mkdir()
    (lib / "Author_0_2020_paper0.fulltext.json").write_text(
        json.dumps({"doi": OA_DOI, "text": "earlier JATS text", "has_pdf": False}), encoding="utf-8")
    _, rows, _ = run_stage(tmp_path, [OA_DOI], titles={OA_DOI: OA_TITLE})
    assert rows[OA_DOI]["downloaded"] == "True" and rows[OA_DOI]["sidecar_status"] == "EXISTS"
    sc = json.loads((lib / "Author_0_2020_paper0.fulltext.json").read_text(encoding="utf-8"))
    assert sc["text"] == "earlier JATS text" and sc["has_pdf"] is True and sc["identity"] == "OK"


def test_s3_metadata_doi_must_be_the_queue_doi(world, tmp_path):
    world.meta["PMC4924218.1"]["doi"] = "10.9999/someone-else"
    _, rows, lib = run_stage(tmp_path, [OA_DOI])
    assert rows[OA_DOI]["outcome"] == "ERROR" and "not the queue DOI" in rows[OA_DOI]["error"]
    assert not any(h.path.endswith(".pdf") for h in world.hits)


def test_am_without_s3_xml_uses_bioc(world, tmp_path):
    del world.meta["PMC7983342.1"]["xml_url"]
    world.bioc[AM_PMC] = fx("bioc_PMC7983342_AM.json")
    _, rows, lib = run_stage(tmp_path, [AM_DOI])
    r = rows[AM_DOI]
    assert r["sidecar_status"] == "OK" and r["downloaded"] == "False"
    sc = json.loads((lib / (r["filename"][:-4] + ".fulltext.json")).read_text(encoding="utf-8"))
    assert sc["extractor"] == "pmc_bioc" and sc["has_pdf"] is False and sc["doi"] == AM_DOI
    assert world.hits_for("BioC_json/PMC7983342")


def test_none_class_with_europe_pmc_oa_flag_tries_fulltextxml_and_500_is_not_available(world, tmp_path):
    """idconv refused: Europe PMC maps the DOI and flags it OA; PMC classifies it NONE; the OA-only
    fullTextXML answers 500, which is NOT_AVAILABLE."""
    world.env.state.refuse("pmc.ncbi.nlm.nih.gov", "HTTP 429 (test)")
    world.epmc[NONE_DOI] = dict(world.epmc[NONE_DOI], isOpenAccess="Y")
    _, rows, _ = run_stage(tmp_path, [NONE_DOI])
    r = rows[NONE_DOI]
    assert world.fulltext_calls == [NONE_PMC]
    assert r["outcome"] == "NOT_AVAILABLE" and r["sidecar"] == "False"
    assert "epmc_fulltextxml/NOT_AVAILABLE" in r["attempts"]


def test_classification_failure_falls_back_to_s3(world, tmp_path):
    world.override[("eutils.ncbi.nlm.nih.gov", r".*/efetch\.fcgi")] = [(503, "busy")]
    _, rows, lib = run_stage(tmp_path, [OA_DOI, EMB_DOI.replace("pub2", "pub3")], titles={OA_DOI: OA_TITLE})
    assert rows[OA_DOI]["downloaded"] == "True" and rows[OA_DOI]["pmc_class"].startswith("OA (from S3")


def test_classification_failure_with_s3_miss_keeps_the_efetch_root_cause(world, tmp_path):
    world.idconv["10.1234/unknown"] = {"requested-id": "10.1234/unknown", "pmcid": "PMC555", "doi": "10.1234/unknown"}
    world.override[("eutils.ncbi.nlm.nih.gov", r".*/efetch\.fcgi")] = [(503, "busy")]
    _, rows, _ = run_stage(tmp_path, ["10.1234/unknown"])
    r = rows["10.1234/unknown"]
    assert r["outcome"] == "OUTAGE" and r["route"] == "efetch" and r["error"].startswith("efetch/HTTP_503")


# ============================================================================ first root cause
def test_error_column_carries_the_first_root_cause(world, tmp_path):
    """V1-N1: the S3 failure, not the last fallback's status, is the row's error."""
    world.override[("pmc-oa-opendata.s3.amazonaws.com", r"/")] = [(503, "SlowDown")]
    _, rows, _ = run_stage(tmp_path, [OA_DOI])
    r = rows[OA_DOI]
    assert world.fulltext_calls == [OA_PMC]                          # the OA fallback did run (500)
    assert r["error"].startswith("s3/HTTP_503") and r["outcome"] == "OUTAGE" and r["first_status"] == "503"
    assert r["attempts"].endswith("epmc_fulltextxml/NOT_AVAILABLE")


def test_refused_mapping_is_transient_never_no_pmcid(world, tmp_path):
    world.override[("pmc.ncbi.nlm.nih.gov", r"/tools/idconv/api/v1/articles/")] = [
        (429, "<title>429</title>429 Too Many Requests", {"Content-Type": "text/html"})]
    world.override[("www.ebi.ac.uk", r"/europepmc/webservices/rest/search")] = [(503, "")]
    world.override[("eutils.ncbi.nlm.nih.gov", r".*/esearch\.fcgi")] = [Down("reset")]
    _, rows, _ = run_stage(tmp_path, [OA_DOI, MISS_DOI])
    for d in (OA_DOI, MISS_DOI):
        r = rows[d]
        assert r["outcome"] == "REFUSED" and r["route"] == "idconv" and r["first_status"] == "429"
        assert "NO_PMCID" not in r["error"] and from_legacy(r["error"], "pmc") is Kind.REFUSED
    assert world.env.state.is_refused("pmc.ncbi.nlm.nih.gov")
    assert len(world.hits_for("/tools/idconv/")) == 1                # the 429 is never retried


# ============================================================================ legacy strings
@pytest.mark.parametrize("route,o", [
    ("idconv", Outcome(Kind.NO_MATCH, detail="Identifier not found in PMC")),
    ("s3", Outcome(Kind.NO_MATCH, status=404, detail="HTTP 404")),
    ("s3", Outcome(Kind.NOT_AVAILABLE, detail="author manuscript: no PDF on a sanctioned route")),
    ("epmc_fulltextxml", Outcome(Kind.NOT_AVAILABLE, status=500, detail="HTTP_500")),
    ("idconv", Outcome(Kind.REFUSED, status=429, detail="HTTP 429 after 0 retries; host refused for the run")),
    ("s3", Outcome(Kind.REFUSED, status=403, detail="HTTP 403 (1 consecutive)")),
    ("s3", Outcome(Kind.REFUSED, status=200, detail="HTML instead of PDF")),
    ("s3", Outcome(Kind.OUTAGE, status=503, detail="HTTP 503 after 6 retries")),
    ("epmc_search", Outcome(Kind.OUTAGE, status=200, detail="empty body")),
    ("eutils", Outcome(Kind.TRANSPORT, detail="ConnectionError: reset by peer")),
    ("s3", Outcome(Kind.ERROR, detail="md5 mismatch: body a, metadata b")),
])
def test_legacy_error_maps_back_to_the_same_kind(route, o):
    assert from_legacy(pmc_fetch.legacy_error(route, o), "pmc") is o.kind


# ============================================================================ run-level behaviour
def test_already_exists_is_decided_before_any_request(world, tmp_path):
    lib = tmp_path / "lib"
    lib.mkdir()
    (lib / "Author_0_2020_paper0.pdf").write_bytes(b"%PDF-1.4 held")
    # REG-I11: held only when something says it is THIS paper (a file with no DOI is not)
    (lib / "Author_0_2020_paper0.ris").write_text(f"TY  - JOUR\nDO  - {OA_DOI}\nER  - \n", encoding="utf-8")
    _, rows, _ = run_stage(tmp_path, [OA_DOI])
    assert rows[OA_DOI]["skipped"] == "True" and rows[OA_DOI]["winning_source"] == "ALREADY_EXISTS"
    assert world.hits == []


def test_dry_run_maps_only_and_writes_no_library(world, tmp_path):
    _, rows, lib = run_stage(tmp_path, [OA_DOI, MISS_DOI], dry_run=True)
    assert rows[OA_DOI]["error"] == "DRY" and rows[OA_DOI]["pmcid"] == OA_PMC
    assert from_legacy(rows[OA_DOI]["error"], "pmc") is Kind.SKIPPED
    assert rows[MISS_DOI]["error"].startswith("NO_PMCID")
    assert world.hosts_hit() == {"pmc.ncbi.nlm.nih.gov"} and not lib.exists()


def test_identity_is_injected_once_and_never_persisted(world, tmp_path):
    _, rows, _ = run_stage(tmp_path, [OA_DOI], titles={OA_DOI: OA_TITLE})
    ic = world.hits_for("/tools/idconv/")[0]
    assert ic.query["tool"] == ["literature-pipeline"] and ic.query["email"] == ["tester@litpipe-test.org"]
    for h in world.hits:
        assert "email=None" not in h.url and "mailto:None" not in h.headers.get("User-Agent", "")
        if h.host == "pmc-oa-opendata.s3.amazonaws.com":
            assert "email" not in h.query and "mailto" not in h.headers.get("User-Agent", "")
    text = (tmp_path / "pmc.csv").read_text(encoding="utf-8")
    assert "tester@litpipe-test.org" not in text and "email=" not in text


def test_help_keeps_every_legacy_flag():
    r = subprocess.run([sys.executable, str(REPO / "pmc_fetch.py"), "--help"], capture_output=True, text=True,
                       timeout=120)
    assert r.returncode == 0, r.stderr
    for flag in ("--dry-run", "--only-doi", "--no-sidecar", "--no-write-ris", "--base-dir", "--report-in",
                 "--lib-dir", "--report-out", "--no-figures"):
        assert flag in r.stdout


def test_no_quarantine_and_no_retired_routes_in_the_stage():
    src = (REPO / "pmc_fetch.py").read_text(encoding="utf-8")
    for gone in ("quarantine_mismatch", "pdf_doi_disagrees", "?pdf=render", "citation_pdf_url",
                 "/articles/{pmcid}", "LITPIPE_EMAIL", "DEFAULT_EMAIL"):
        assert gone not in src.split('"""', 2)[2], gone          # the module docstring may name them
    assert not hasattr(pmc_fetch, "quarantine_mismatch")


# ============================================================================ the recovery backfill
PMC_REPORT_FIELDS = ["doi", "filename", "pmcid", "downloaded", "skipped", "winning_source", "attempts",
                     "error", "sidecar", "sidecar_status"]


def _csv(path, fields, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)


def _row(doi, pmcid="", downloaded="False", **kw):
    return {"doi": doi, "filename": f"{doi.split('/')[-1]}.pdf", "pmcid": pmcid, "downloaded": downloaded,
            "skipped": kw.get("skipped", "False"), "winning_source": kw.get("winning_source", ""),
            "error": kw.get("error", "HTML"), "sidecar": kw.get("sidecar", "False")}


@pytest.fixture
def recovery_root(tmp_path, monkeypatch):
    """Two registered projects (one a subproject), synthetic PMC reports in a project root and in an
    archive outside every root, one DOI held as a PDF elsewhere, one held as text only."""
    import lit_util
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", tmp_path)
    reg = {"projects": {"alpha": {"lib_dir": "literature"},
                        "beta/sub": {"parent": "beta", "lib_dir": "sub/literature"}},
           "state_dir": str(tmp_path / "state")}
    alpha = tmp_path / "alpha"
    _csv(alpha / "lit_pull_queue.2026-09-20.pmc.csv", PMC_REPORT_FIELDS, [
        _row("10.1000/a1", "PMC11"),                                   # recover
        _row("10.1000/a2", "PMC12", downloaded="True", error=""),       # fetched
        _row("10.1000/a3", "", error="NO_PMCID"),                       # no PMCID
        _row("10.1000/a4", "PMC14", skipped="True", winning_source="ALREADY_EXISTS", error=""),
        _row("10.1000/a5", "PMC15"),                                   # held as a PDF in beta/sub
        _row("10.1000/a6", "PMC16", sidecar="True", error="HTTP_500"),  # text only: still wants the PDF
        _row("10.1000/a7", "PMC17"),                                   # already in a live queue
    ])
    _csv(alpha / "lit_pull_queue.2026-09-20.unpaywall.csv", ["doi", "title", "year", "filename"],
         [{"doi": "10.1000/a1", "title": "Title A1", "year": "2019"}])
    queue_cols = ["doi", "title", "authors", "year", "destination", "notes"]
    _csv(alpha / "lit_pull_queue.2026-09-20.normalized.csv", queue_cols,
         [{"doi": "10.1000/A1", "title": "Title A1", "authors": "Doe J; Roe R", "year": "2019",
           "destination": "literature/"}])
    _csv(alpha / "lit_pull_queue.csv", queue_cols, [{"doi": "10.1000/a7", "title": "t", "destination": "literature/"}])
    _csv(alpha / "notes.csv", ["doi", "pmcid"], [{"doi": "10.1000/zz", "pmcid": "PMC9"}])    # not a report
    arch = tmp_path / "archive" / "2026-09-30" / "lit_sweep_exhaust"
    _csv(arch / "lit_pull_queue.2026-09-15.pmc.csv", PMC_REPORT_FIELDS, [
        _row("10.1000/a1", "PMC11"),                                   # the same DOI again: counted once
        _row("10.1000/b1", "PMC21")])
    other = tmp_path / "elsewhere"
    _csv(other / "snapshot.b01.pmc.csv", PMC_REPORT_FIELDS, [_row("10.1000/c1", "PMC31")])
    lib_b = tmp_path / "beta" / "sub" / "literature"
    lib_b.mkdir(parents=True)
    (lib_b / "Held_2020_a5.pdf").write_bytes(b"%PDF-1.4")
    (lib_b / "Held_2020_a5.ris").write_text("TY  - JOUR\nDO  - 10.1000/a5\nER  - \n", encoding="utf-8")
    lib_a = alpha / "literature"
    lib_a.mkdir(parents=True)
    (lib_a / "Text_2020_a6.fulltext.json").write_text(json.dumps({"doi": "10.1000/a6", "text": "body"}),
                                                     encoding="utf-8")
    return tmp_path, reg, arch, other


def test_recovery_dry_run_counts_and_skips_held_dois(recovery_root):
    from backfills import pmc_recovery
    root, reg, arch, other = recovery_root
    before = sorted(str(p) for p in root.rglob("*"))
    s = pmc_recovery.run(report_dirs=[f"{arch}=alpha", str(other)], registry=reg)
    a = s["projects"]["alpha"]
    assert a["reports"] == 2 and a["rows_with_pmcid_not_fetched"] == 6
    assert a["distinct_dois"] == 5 and a["held_as_pdf"] == 1 and a["already_queued"] == 1
    assert a["text_only_still_wanting_pdf"] == 1 and a["to_recover"] == 3        # a1, a6, b1
    assert s["unassigned_dirs"] == [str(other)] and s["staged"] == {}
    assert sorted(str(p) for p in root.rglob("*")) == before                    # the dry run wrote nothing


def test_recovery_stage_writes_one_tagged_queue_and_never_overwrites(recovery_root):
    from backfills import pmc_recovery
    import sweep
    root, reg, arch, _ = recovery_root
    s = pmc_recovery.run(report_dirs=[f"{arch}=alpha"], registry=reg, stage=True)
    q = root / "alpha" / "lit_pull_queue.pmcrecover.csv"
    assert s["staged"]["alpha"]["rows"] == 3 and sweep.queue_tag(q.name) == "pmcrecover"
    fields, rows = sweep.read_queue(q)
    assert fields == ["doi", "title", "authors", "year", "destination", "notes"]
    by = {r["doi"]: r for r in rows}
    assert set(by) == {"10.1000/a1", "10.1000/a6", "10.1000/b1"}
    assert by["10.1000/a1"]["title"] == "Title A1" and by["10.1000/a1"]["authors"] == "Doe J; Roe R"
    assert {r["destination"] for r in rows} == {"literature"} and "PMC11" in by["10.1000/a1"]["notes"]
    q.write_text("doi,title,authors,year,destination,notes\n10.9/keep,k,,,literature,\n", encoding="utf-8")
    s2 = pmc_recovery.run(report_dirs=[f"{arch}=alpha"], registry=reg, stage=True)
    assert s2["staged"] == {} and "alpha" in s2["kept_existing"]
    assert "10.9/keep" in q.read_text(encoding="utf-8")


def test_recovery_sample_input_is_a_pmc_stage_input(recovery_root, tmp_path):
    from backfills import pmc_recovery
    root, reg, arch, _ = recovery_root
    out = tmp_path / "scratch" / "sample.csv"
    s = pmc_recovery.run(report_dirs=[f"{arch}=alpha"], registry=reg, project="alpha", limit=2,
                         sample_input=str(out))
    rows = pmc_fetch.read_rows(str(out))
    assert s["sample"]["rows"] == 2 and len(rows) == 2 and all(r["filename"].endswith(".pdf") for r in rows)
    with pytest.raises(SystemExit):
        pmc_recovery.run(report_dirs=[f"{arch}=alpha"], registry=reg, project="alpha", sample_input=str(out))


def test_recovery_prints_but_does_not_run_the_commands(recovery_root, capsys, monkeypatch):
    from backfills import pmc_recovery
    root, reg, arch, _ = recovery_root
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: pytest.fail("the recovery ran a command"))
    pmc_recovery.run(report_dirs=[f"{arch}=alpha"], registry=reg)
    out = capsys.readouterr().out
    assert "Not run. The 30-row sample" in out and "--sample-input" in out and "pmc_fetch.py" in out
    assert "Not run. The full recovery" in out and "--stage" in out and 'sweep.py" --project "alpha"' in out


def test_recovery_help_runs():
    r = subprocess.run([sys.executable, str(REPO / "backfills" / "pmc_recovery.py"), "--help"],
                       capture_output=True, text=True, timeout=120)
    assert r.returncode == 0 and "--stage" in r.stdout and "--sample-input" in r.stdout
