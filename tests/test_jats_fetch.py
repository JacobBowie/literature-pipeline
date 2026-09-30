"""c13 + W2-A2: fetch_jats_xml, the shared Europe PMC JATS GET, now through litpipe.net.

fetch_jats_xml(pmcid, ua=None, timeout=30) -> (bytes | None, status). Since 2026-09-16 Europe PMC
answers 500 (JSON body) for an article outside its OA full-text set, deterministically (V1 P3), so a
500 is NOT_AVAILABLE like the old 404, and it is sent once, never retried. A transport failure is a
status, not an exception (it used to propagate). `ua` is ignored: litpipe.net sends the identity.

Offline: litpipe.net's transports are replaced by a stub (tests/conftest.py fails any live request).
"""
import json
from pathlib import Path

import pytest
from requests.structures import CaseInsensitiveDict

import backfill_fulltext
import jats_to_text
import recheck_pmc
from litpipe import hosts, net
from litpipe.outcomes import Kind, from_legacy

FIX = Path(__file__).resolve().parent / "fixtures" / "W2-A2"
EPMC_500 = (FIX / "epmc_fulltextxml_500.json").read_bytes()
XML = {"Content-Type": "application/xml"}
JSONH = {"Content-Type": "application/json"}


def stub(monkeypatch, *replies):
    """Both transports answer from `replies` in turn (the last repeats): (status, body[, headers]),
    or an Exception instance for a transport failure. Returns the list of (method, url, headers)."""
    sent = []

    def fake(method, url, hdrs, body, timeout, max_bytes):
        sent.append((method, url, dict(hdrs)))
        r = replies[min(len(sent), len(replies)) - 1]
        if isinstance(r, Exception):
            return net._Raw(error=f"{type(r).__name__}: {r}")
        status, data, h = (tuple(r) + (None,))[:3]
        data = data if isinstance(data, bytes) else data.encode("utf-8")
        return net._Raw(status, CaseInsensitiveDict(h or XML), data, data[:net.CHUNK], len(data))

    monkeypatch.setitem(net._TRANSPORTS, "requests", fake)
    monkeypatch.setitem(net._TRANSPORTS, "urllib", fake)
    return sent


@pytest.fixture
def env(net_env):
    return net_env


# ---------- fetch_jats_xml status classification ----------

@pytest.mark.parametrize("reply,expect", [
    ((200, b"<article>x</article>"), "OK"),
    ((200, b"   <article/>"), "OK"),                     # leading ws stripped before '<' check
    ((404, b""), "NOT_AVAILABLE"),                       # the pre-2026-09-16 signal
    ((410, b""), "NOT_AVAILABLE"),
    ((500, EPMC_500, JSONH), "NOT_AVAILABLE"),           # the signal since 2026-09-16 (V1 P3)
    ((403, b""), "HTTP_403"),
    ((503, b"busy"), "HTTP_503"),                        # a real outage stays distinct, after retries
    ((200, b""), "EMPTY_OR_NON_XML"),                    # empty body
    ((200, b"not xml at all"), "EMPTY_OR_NON_XML"),      # 200 but not XML
])
def test_fetch_jats_xml_status(env, monkeypatch, reply, expect):
    stub(monkeypatch, reply)
    body, status = jats_to_text.fetch_jats_xml("PMC1")
    assert status == expect
    assert (body == reply[1]) if expect == "OK" else (body is None)


def test_fulltextxml_500_is_sent_once_never_retried(env, monkeypatch):
    """A deterministic 500 retried with litpipe.net's default policy would cost 7 requests and
    about two minutes of backoff per non-OA article."""
    sent = stub(monkeypatch, (500, EPMC_500, JSONH))
    assert jats_to_text.fetch_jats_xml("PMC10041407") == (None, "NOT_AVAILABLE")
    assert len(sent) == 1 and env.clock.sleeps == []      # no backoff either


def test_the_500_override_is_scoped_to_the_call(env, monkeypatch):
    stub(monkeypatch, (500, EPMC_500, JSONH))
    before = hosts.policy("www.ebi.ac.uk").retry.statuses
    jats_to_text.fetch_jats_xml("PMC1")
    assert 500 in before and hosts.policy("www.ebi.ac.uk").retry.statuses == before


def test_a_503_is_still_retried(env, monkeypatch):
    sent = stub(monkeypatch, (503, b"busy"), (200, b"<article/>"))
    assert jats_to_text.fetch_jats_xml("PMC1") == (b"<article/>", "OK")
    assert len(sent) == 2


def test_transport_failure_is_a_status_not_an_exception(env, monkeypatch):
    stub(monkeypatch, ConnectionError("getaddrinfo failed"))
    body, status = jats_to_text.fetch_jats_xml("PMC1")
    assert body is None and status.startswith("TRANSPORT: ")
    assert from_legacy(status) is Kind.TRANSPORT


def test_outcome_form_carries_the_kind(env, monkeypatch):
    """Callers that route on kinds use fetch_jats_outcome; the tuple form keeps legacy strings."""
    stub(monkeypatch, (500, EPMC_500, JSONH))
    o = jats_to_text.fetch_jats_outcome("PMC1")
    assert o.kind is Kind.NOT_AVAILABLE and o.status == 500 and o.payload is None and o.attempts == 1
    stub(monkeypatch, (200, b"<article/>"))
    o = jats_to_text.fetch_jats_outcome("PMC1")
    assert o.ok and o.payload == b"<article/>"
    stub(monkeypatch, (403, b""))
    assert jats_to_text.fetch_jats_outcome("PMC1").kind is Kind.REFUSED


def test_request_url_and_identity(env, monkeypatch):
    """One URL (Europe PMC REST, never the europepmc.org website); the UA is litpipe.net's, the
    caller's `ua` is ignored; no email= query and nothing reads mailto:None."""
    sent = stub(monkeypatch, (200, b"<article/>"))
    jats_to_text.fetch_jats_xml("PMC9817969", "legacy-ua/1.0 (mailto:None)")
    method, url, hdrs = sent[0]
    assert url == "https://www.ebi.ac.uk/europepmc/webservices/rest/PMC9817969/fullTextXML"
    assert hdrs["User-Agent"].startswith("literature-pipeline/") and "None" not in hdrs["User-Agent"]
    monkeypatch.delenv("LITPIPE_EMAIL", raising=False)
    sent = stub(monkeypatch, (200, b"<article/>"))
    jats_to_text.fetch_jats_xml("PMC9817969")
    assert "mailto" not in sent[0][2]["User-Agent"] and "email=" not in sent[0][1]


# ---------- callers: a 500 is NOT_AVAILABLE (N-B3), then BioC is tried (N-B4) ----------

BIOC_ABSENT = (200, (FIX / "bioc_absent.html").read_bytes(), {"Content-Type": "text/html"})


def test_recheck_500_then_absent_bioc_is_not_available(env, monkeypatch, tmp_path):
    sent = stub(monkeypatch, (500, EPMC_500, JSONH), BIOC_ABSENT)
    sidecar = tmp_path / "p.fulltext.json"
    ok, status = recheck_pmc.fetch_jats_sidecar("PMC404", str(sidecar))
    assert (ok, status) == (False, "NOT_AVAILABLE")
    assert not sidecar.exists()  # nothing written on a miss
    assert [u.split("/")[2] for _, u, _ in sent] == ["www.ebi.ac.uk", "www.ncbi.nlm.nih.gov"]


def test_backfill_500_counts_as_not_available(env, monkeypatch, tmp_path):
    stub(monkeypatch, (500, EPMC_500, JSONH), BIOC_ABSENT)
    ok, status = backfill_fulltext.fetch_sidecar("PMC404", str(tmp_path / "p.fulltext.json"))
    assert (ok, status) == (False, "NOT_AVAILABLE")


def test_author_manuscript_gets_a_bioc_sidecar(env, monkeypatch, tmp_path):
    """N-B4: Europe PMC 500 for an author manuscript, BioC holds its text."""
    bioc = (FIX / "bioc_am_PMC7983342.json").read_bytes()
    stub(monkeypatch, (500, EPMC_500, JSONH), (200, bioc, JSONH))
    sidecar = tmp_path / "p.fulltext.json"
    ok, status = backfill_fulltext.fetch_sidecar("PMC7983342", str(sidecar))
    assert (ok, status) == (True, "OK")
    data = json.loads(sidecar.read_text(encoding="utf-8"))
    assert data["doi"] == "10.1016/j.resp.2021.103638" and data["pmcid"] == "PMC7983342"
    assert data["sections"] and data["authors"][0] == "Spencer Matthew D."


def test_a_transport_failure_does_not_fall_through_to_bioc(env, monkeypatch, tmp_path):
    sent = stub(monkeypatch, ConnectionError("reset"))
    ok, status = recheck_pmc.fetch_jats_sidecar("PMC1", str(tmp_path / "p.json"))
    assert ok is False and status.startswith("TRANSPORT: ")
    assert {u.split("/")[2] for _, u, _ in sent} == {"www.ebi.ac.uk"}


# ---------- per-caller failure labels ----------


def test_backfill_parse_error_label_has_type(env, monkeypatch, tmp_path):
    """backfill keeps 'ERROR_<type>:<msg>' for a document it cannot parse."""
    stub(monkeypatch, (200, b"<article><unclosed></article>"))
    ok, status = backfill_fulltext.fetch_sidecar("PMC1", str(tmp_path / "p.json"))
    assert ok is False and status.startswith("ERROR_ParseError:")


# ---------- 200-OK happy path THROUGH each caller ----------
JATS_OK = (b"<article><front><article-meta>"
           b"<article-title>Test Heat Title</article-title>"
           b"</article-meta></front></article>")


def test_recheck_ok_writes_sidecar(env, monkeypatch, tmp_path):
    """Default pdf_head_text='' -> the title-sanity check is skipped; sidecar is written
    with the _recheck_source_doi provenance annotation."""
    stub(monkeypatch, (200, JATS_OK))
    sidecar = tmp_path / "p.fulltext.json"
    ok, status = recheck_pmc.fetch_jats_sidecar("PMC1", str(sidecar), doi="10.1/x")
    assert (ok, status) == (True, "OK")
    data = json.loads(sidecar.read_text(encoding="utf-8"))
    assert data["title"] == "Test Heat Title" and data["_recheck_source_doi"] == "10.1/x"


def test_recheck_title_mismatch_refuses_sidecar(env, monkeypatch, tmp_path):
    """The RC3 title-sanity branch: a JATS title absent from the PDF head signals a wrong-paper
    attachment -> refuse to write."""
    stub(monkeypatch, (200, JATS_OK))
    sidecar = tmp_path / "p.fulltext.json"
    ok, status = recheck_pmc.fetch_jats_sidecar(
        "PMC1", str(sidecar), pdf_head_text="totally unrelated words with no overlap at all")
    assert ok is False and status.startswith("TITLE_MISMATCH")
    assert not sidecar.exists()


def test_backfill_ok_writes_sidecar(env, monkeypatch, tmp_path):
    stub(monkeypatch, (200, JATS_OK))
    sidecar = tmp_path / "p.fulltext.json"
    ok, status = backfill_fulltext.fetch_sidecar("PMC1", str(sidecar))
    assert (ok, status) == (True, "OK")
    assert json.loads(sidecar.read_text(encoding="utf-8"))["title"] == "Test Heat Title"


def test_backfill_ok_merges_existing_sidecar(env, monkeypatch, tmp_path):
    """RC5 branch: a re-fetch over an existing sidecar keeps enriched fields (doi/figures) the
    minimal fresh JATS parse lacks."""
    stub(monkeypatch, (200, JATS_OK))
    sidecar = tmp_path / "p.fulltext.json"
    sidecar.write_text(json.dumps({"title": "old", "doi": "10.1/x", "figures": [{"id": "f1"}]}),
                       encoding="utf-8")
    ok, status = backfill_fulltext.fetch_sidecar("PMC1", str(sidecar))
    assert (ok, status) == (True, "OK")
    data = json.loads(sidecar.read_text(encoding="utf-8"))
    assert data["title"] == "Test Heat Title"      # fresh parse wins for a field it populated
    assert data["doi"] == "10.1/x"                   # RC5: enriched doi preserved
    assert data["figures"] == [{"id": "f1"}]         # RC5: figures preserved


def test_backfill_refresh_keeps_fetched_figure_images(env, monkeypatch, tmp_path):
    """A re-parse that lists the figures again used to replace the whole list and drop every
    fetched image_path and licence (merge_sidecar keeps old figures only when the new list is empty)."""
    jats = (b"<article><front><article-meta><article-title>T</article-title></article-meta></front>"
            b"<body><sec><title>R</title><fig id='f1'><label>Figure 1</label><caption><p>Cap</p></caption>"
            b"<graphic xmlns:xlink='http://www.w3.org/1999/xlink' xlink:href='x-g001.jpg'/></fig></sec></body>"
            b"</article>")
    stub(monkeypatch, (200, jats))
    sidecar = tmp_path / "p.fulltext.json"
    sidecar.write_text(json.dumps({"title": "T", "figures": [
        {"label": "Figure 1", "caption": "Cap", "graphic_href": "x-g001.jpg", "image_path": "p.fig1.jpg",
         "image_url": "https://x/y.jpg", "licence": "CC BY", "licence_category": "OK_WITH_CREDIT"}]}),
        encoding="utf-8")
    ok, _ = backfill_fulltext.fetch_sidecar("PMC1", str(sidecar))
    fig = json.loads(sidecar.read_text(encoding="utf-8"))["figures"][0]
    assert ok and fig["image_path"] == "p.fig1.jpg" and fig["licence_category"] == "OK_WITH_CREDIT"
    assert fig["caption"] == "Cap"


def test_smoke_test_return_code(env, monkeypatch):
    stub(monkeypatch, (200, JATS_OK))
    assert jats_to_text._smoke_test("PMC1", dump=False) == 0
    stub(monkeypatch, (500, EPMC_500, JSONH))
    assert jats_to_text._smoke_test("PMC1", dump=False) == 1


def test_no_caller_keeps_a_private_jats_fetch():
    """Amendment 2: recheck_pmc and backfill_fulltext call jats_to_text's functions; neither
    builds the Europe PMC URL or an identity of its own."""
    for mod in (recheck_pmc, backfill_fulltext):
        src = Path(mod.__file__).read_text(encoding="utf-8")
        assert "fullTextXML" not in src and "webservices/rest" not in src
        assert "mailto:" not in src and "LITPIPE_EMAIL" not in src
        assert "fetch_fulltext" in src
        for name in ("EMAIL", "UA", "API_UA"):
            assert not hasattr(mod, name), (mod.__name__, name)
