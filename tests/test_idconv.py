"""idconv consolidation (c8) + is_valid_doi tightening (B2).

Offline: monkeypatch lit_net._idconv_get (the stdlib-urllib GET doi_to_pmcid_batch calls;
see the 2026-09-01 note on IDCONV for why this is not the shared requests-based get())
+ lit_net.time.sleep. Guards the B2 hardening: poisoned-chunk 1-DOI fallback, the empty-key guard,
the up-front validity gate, and transport-error skip.
"""
import requests

import lit_net
from lit_util import is_valid_doi


class FakeJson:
    def __init__(self, status_code, data):
        self.status_code = status_code
        self.headers = {}
        self._data = data

    def json(self):
        return self._data


def _rec(doi, pmcid):
    return {"requested-id": doi, "pmcid": pmcid}


def test_batch_resolves_records(monkeypatch):
    def fake_get(url, params, ua, timeout=None):
        ids = params["ids"].split(",")
        return FakeJson(200, {"records": [_rec(i, "PMC" + i[-1]) for i in ids]})
    monkeypatch.setattr(lit_net, "_idconv_get", fake_get)
    monkeypatch.setattr(lit_net.time, "sleep", lambda s: None)
    out = lit_net.doi_to_pmcid_batch(["10.1234/a", "10.5678/b"], ua="UA", email="e@x")
    assert out == {"10.1234/a": "PMCa", "10.5678/b": "PMCb"}


def test_poisoned_chunk_falls_back_to_single(monkeypatch):
    """B2: one bad id 400s the whole batch -> retry one at a time so the rest still resolve."""
    calls = []

    def fake_get(url, params, ua, timeout=None):
        ids = params["ids"].split(",")
        calls.append(len(ids))
        if len(ids) > 1:
            return FakeJson(400, {"status": "error", "records": []})
        return FakeJson(200, {"records": [_rec(ids[0], "PMC" + ids[0][-1])]})
    monkeypatch.setattr(lit_net, "_idconv_get", fake_get)
    monkeypatch.setattr(lit_net.time, "sleep", lambda s: None)
    out = lit_net.doi_to_pmcid_batch(["10.1234/a", "10.5678/b"], ua="UA", email="e@x")
    assert out == {"10.1234/a": "PMCa", "10.5678/b": "PMCb"}
    assert calls[0] == 2 and calls[1:] == [1, 1]  # batch 400s, then two single-id retries


def test_empty_key_guard(monkeypatch):
    """A record with a pmcid but no doi/requested-id must NOT write out[''] (recheck_pmc bug)."""
    def fake_get(url, params, ua, timeout=None):
        return FakeJson(200, {"records": [{"pmcid": "PMC999"},
                                          {"requested-id": "10.1234/x", "pmcid": "PMC1"}]})
    monkeypatch.setattr(lit_net, "_idconv_get", fake_get)
    monkeypatch.setattr(lit_net.time, "sleep", lambda s: None)
    out = lit_net.doi_to_pmcid_batch(["10.1234/x"], ua="UA", email="e@x")
    assert "" not in out and out == {"10.1234/x": "PMC1"}


def test_invalid_dois_dropped_before_api(monkeypatch):
    """B2: malformed DOIs never reach idconv (would 400 the whole chunk)."""
    seen = []

    def fake_get(url, params, ua, timeout=None):
        seen.append(params["ids"])
        return FakeJson(200, {"records": [_rec("10.1234/ok", "PMC1")]})
    monkeypatch.setattr(lit_net, "_idconv_get", fake_get)
    monkeypatch.setattr(lit_net.time, "sleep", lambda s: None)
    out = lit_net.doi_to_pmcid_batch(
        ["10.1234/ok", "10.1234/ab–cd", "10.1234/ab—cd"], ua="UA", email="e@x")
    assert seen == ["10.1234/ok"]          # only the valid DOI reached idconv; dash artifacts dropped
    assert out == {"10.1234/ok": "PMC1"}


def test_transport_error_skips_chunk(monkeypatch):
    def fake_get(url, params, ua, timeout=None):
        raise requests.exceptions.ConnectionError("down")  # subclasses OSError, still caught
    monkeypatch.setattr(lit_net, "_idconv_get", fake_get)
    monkeypatch.setattr(lit_net.time, "sleep", lambda s: None)
    out = lit_net.doi_to_pmcid_batch(["10.1234/a"], ua="UA", email="e@x")
    assert out == {}  # logged + skipped, no crash


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
    """2026-09-01 regression: the legacy www.ncbi.nlm.nih.gov/pmc/utils path 302s to a host that
    403s requests/urllib3. Pinning the endpoint stops a silent revert to the dead URL."""
    assert lit_net.IDCONV == "https://pmc.ncbi.nlm.nih.gov/tools/idconv/api/v1/articles/"
    assert "/pmc/utils/idconv" not in lit_net.IDCONV


def test_idconv_does_not_use_the_requests_transport(monkeypatch):
    """2026-09-01 regression: doi_to_pmcid_batch must NOT route through lit_net.get(). That client
    gets HTTP 403 from the new PMC host for a byte-identical request that stdlib urllib serves 200,
    which silently zeroed the whole PMC stage. If someone reroutes it back, this fails loudly."""
    def boom(*a, **k):
        raise AssertionError("doi_to_pmcid_batch used the requests transport; it must use urllib")
    monkeypatch.setattr(lit_net, "get", boom)
    monkeypatch.setattr(lit_net.time, "sleep", lambda s: None)
    monkeypatch.setattr(lit_net, "_idconv_get",
                        lambda url, params, ua, timeout=None: FakeJson(
                            200, {"records": [_rec("10.1234/a", "PMCa")]}))
    assert lit_net.doi_to_pmcid_batch(["10.1234/a"], ua="UA", email="e@x") == {"10.1234/a": "PMCa"}


def test_idconv_get_builds_a_single_query_string():
    """The urllib helper must append params correctly whether or not the base URL already has a ?."""
    seen = {}

    class _FH:
        status = 200
        def read(self): return b'{"records": []}'
        def __enter__(self): return self
        def __exit__(self, *a): return False

    import urllib.request as _ur
    real = _ur.urlopen
    try:
        _ur.urlopen = lambda req, timeout=None: (seen.update(url=req.full_url), _FH())[1]
        lit_net._idconv_get("https://x.test/api", {"ids": "10.1/a", "format": "json"}, "UA")
    finally:
        _ur.urlopen = real
    assert seen["url"].count("?") == 1
    assert "ids=10.1%2Fa" in seen["url"] and "format=json" in seen["url"]


def test_blocked_host_routes_through_urllib(monkeypatch):
    """2026-09-01: pmc.ncbi.nlm.nih.gov 403s requests/urllib3 but serves stdlib urllib. This killed
    pmc_fetch's NCBI citation_pdf_url fallback and fetch_figures, reported only as 'ncbi-page/HTML'
    (a 403 page parsed as HTML). stream_download must not use requests for that host."""
    def boom(*a, **k):
        raise AssertionError("stream_download used requests for a URLLIB_ONLY_HOSTS url")
    monkeypatch.setattr(lit_net.requests, "get", boom)
    monkeypatch.setattr(lit_net, "_urllib_stream",
                        lambda url, **k: lit_net.StreamResult(200, b"%PDF-", b"%PDF-x", 6, False, ""))
    r = lit_net.stream_download("https://pmc.ncbi.nlm.nih.gov/articles/PMC1/",
                                max_bytes=10_000, headers={"User-Agent": "UA"})
    assert r.status_code == 200 and r.first_chunk == b"%PDF-"


def test_unblocked_host_still_uses_requests(monkeypatch):
    """The urllib detour is host-scoped. Everything else must keep the requests transport."""
    called = {}

    class _R:
        status_code = 200
        def iter_content(self, chunk_size=None): return iter([b"%PDF-", b"tail"])
    def fake_get(url, **k):
        called["url"] = url
        return _R()
    monkeypatch.setattr(lit_net.requests, "get", fake_get)
    monkeypatch.setattr(lit_net, "_urllib_stream",
                        lambda *a, **k: (_ for _ in ()).throw(AssertionError("wrong transport")))
    r = lit_net.stream_download("https://europepmc.org/articles/PMC1?pdf=render", max_bytes=10_000)
    assert called["url"].startswith("https://europepmc.org") and r.status_code == 200


def test_urllib_only_host_matcher_is_exact():
    """Must match the host, not a substring: a lookalike domain should not be routed."""
    assert lit_net._urllib_only_host("https://pmc.ncbi.nlm.nih.gov/articles/PMC1/")
    assert not lit_net._urllib_only_host("https://www.ncbi.nlm.nih.gov/pmc/articles/PMC1/")
    assert not lit_net._urllib_only_host("https://pmc.ncbi.nlm.nih.gov.evil.test/x")
    assert not lit_net._urllib_only_host("not a url")


# ---------------------------------------------------------------- REG-I22 / V1-N5 (W0, 2026-09-30)
# The urllib route caught only HTTPError, so a DNS failure or timeout on pmc.ncbi escaped as an
# exception past callers that catch requests exceptions; and get() skipped its retry loop there.
# These drive the REAL urllib code with a scripted urlopen (no network).
import email.message
import http.client
import io
import subprocess
import sys
import urllib.error
from pathlib import Path

import pytest

import fetch_figures
import harvest_citations
from litpipe.outcomes import Kind

PMC = "https://pmc.ncbi.nlm.nih.gov/tools/idconv/api/v1/articles/"
DNS_FAIL = urllib.error.URLError(OSError(11001, "getaddrinfo failed"))


class _FH:
    """A urlopen() result: context manager with status, headers and read()."""
    def __init__(self, status=200, body=b"", chunks=None, read_exc=None):
        self.status = status
        self.headers = email.message.Message()
        self._body, self._chunks, self._exc = body, list(chunks or []), read_exc

    def read(self, n=-1):
        if self._chunks:
            nxt = self._chunks.pop(0)
            if isinstance(nxt, BaseException):
                raise nxt
            return nxt
        if self._exc:
            raise self._exc
        return b"" if n != -1 else self._body

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _http_error(code, body=b"", retry_after=None):
    hdrs = email.message.Message()
    if retry_after is not None:
        hdrs["Retry-After"] = str(retry_after)
    return urllib.error.HTTPError(PMC, code, "x", hdrs, io.BytesIO(body))


def _script(monkeypatch, *items):
    """Each urlopen call takes the next item (the last repeats): raise it if an exception."""
    calls = []

    def fake(req, timeout=None):
        calls.append(req.full_url)
        item = items[min(len(calls), len(items)) - 1]
        if isinstance(item, BaseException):
            raise item
        return item
    monkeypatch.setattr(lit_net._urlrequest, "urlopen", fake)
    return calls


@pytest.fixture
def sleeps(monkeypatch):
    got = []
    monkeypatch.setattr(lit_net.time, "sleep", got.append)
    return got


@pytest.mark.parametrize("exc", [
    DNS_FAIL,
    TimeoutError("timed out"),
    ConnectionResetError(10054, "reset by peer"),
    http.client.RemoteDisconnected("Remote end closed connection without response"),
])
def test_urllib_get_transport_failure_is_a_failure_object_not_an_exception(monkeypatch, sleeps, exc):
    calls = _script(monkeypatch, exc)
    r = lit_net._urllib_get(PMC, params={"ids": "1"}, retries=3)
    assert r.status_code == 0 and r.error and type(exc).__name__ in r.error
    assert len(calls) == 3                      # retried like get()'s requests branch (V1-N5)
    assert sleeps == [1.0, 2.0]                 # same exponential backoff


def test_urllib_get_incomplete_read_mid_body_is_caught(monkeypatch, sleeps):
    """IncompleteRead is an http.client.HTTPException, not an OSError."""
    _script(monkeypatch, _FH(read_exc=http.client.IncompleteRead(b"par", 10)))
    r = lit_net._urllib_get(PMC, retries=1)
    assert r.status_code == 0 and "IncompleteRead" in r.error


def test_urllib_get_retries_5xx_then_succeeds(monkeypatch, sleeps):
    calls = _script(monkeypatch, _http_error(503), _FH(200, b'{"records": []}'))
    r = lit_net._urllib_get(PMC, retries=3)
    assert r.status_code == 200 and r.json() == {"records": []} and len(calls) == 2


def test_urllib_get_honours_retry_after_case_insensitively(monkeypatch, sleeps):
    _script(monkeypatch, _http_error(429, retry_after=7), _FH(200, b"{}"))
    assert lit_net._urllib_get(PMC, retries=2).status_code == 200
    assert sleeps == [7.0]


def test_urllib_get_404_is_terminal(monkeypatch, sleeps):
    calls = _script(monkeypatch, _http_error(404, b"nope"))
    r = lit_net._urllib_get(PMC, retries=3)
    assert r.status_code == 404 and r.text == "nope" and len(calls) == 1 and sleeps == []


def test_get_passes_its_retry_policy_to_the_urllib_route(monkeypatch, sleeps):
    """V1-N5: get(retries=...) on URLLIB_ONLY_HOSTS used to return before its retry loop."""
    calls = _script(monkeypatch, DNS_FAIL)
    r = lit_net.get(PMC, retries=2, backoff=0.5, headers={"User-Agent": "UA"}, timeout=5)
    assert r.status_code == 0 and len(calls) == 2 and sleeps == [0.5]


def test_get_urllib_route_never_raises_where_callers_catch_requests_errors(monkeypatch, sleeps):
    """The escape REG-I22 describes: an URLError is not a requests.RequestException."""
    _script(monkeypatch, DNS_FAIL)
    try:
        lit_net.get(PMC)
    except requests.RequestException:
        pytest.fail("get() raised for a pmc.ncbi transport failure")


def test_idconv_get_transport_failure_returns_status_zero(monkeypatch):
    _script(monkeypatch, DNS_FAIL)
    r = lit_net._idconv_get(PMC, {"ids": "10.1/a"}, "UA")
    assert r.status_code == 0 and "URLError" in r.error


def test_idconv_get_429_is_returned_not_retried(monkeypatch):
    """2026-09-30: the host 429s a second request within seconds; a blind retry extends that."""
    calls = _script(monkeypatch, _http_error(429, b"<title>429</title>"))
    r = lit_net._idconv_get(PMC, {"ids": "10.1/a"}, "UA")
    assert r.status_code == 429 and len(calls) == 1


def test_doi_to_pmcid_batch_reports_a_transport_failure(monkeypatch, sleeps, capsys):
    _script(monkeypatch, DNS_FAIL)
    assert lit_net.doi_to_pmcid_batch(["10.1234/a"], ua="UA", email="e@x") == {}
    assert "transport error" in capsys.readouterr().err


def test_urllib_stream_incomplete_read_returns_partial_with_error(monkeypatch):
    _script(monkeypatch, _FH(200, chunks=[b"%PDF-1.7", http.client.IncompleteRead(b"", 5)]))
    r = lit_net._urllib_stream(PMC, max_bytes=1000)
    assert r.status_code == 200 and r.first_chunk == b"%PDF-1.7" and "IncompleteRead" in r.error


def test_urllib_stream_dns_failure(monkeypatch):
    _script(monkeypatch, DNS_FAIL)
    r = lit_net._urllib_stream(PMC, max_bytes=1000)
    assert r.status_code == 0 and "getaddrinfo" in r.error


def test_fetch_figures_page_fetch_survives_transport_failure(monkeypatch, sleeps):
    """fetch_figures.py:62 caught only requests errors; the pmc.ncbi page goes through urllib."""
    _script(monkeypatch, DNS_FAIL)
    assert fetch_figures.fetch_pmc_html("PMC1") is None

    def raise_oserror(*a, **k):
        raise TimeoutError("timed out")
    monkeypatch.setattr(lit_net, "get", raise_oserror)
    assert fetch_figures.fetch_pmc_html("PMC1") is None


# ---------------------------------------------------------------- harvest_citations.pmid_to_doi
def _idconv(monkeypatch, status, text="", error=""):
    monkeypatch.setattr(lit_net, "_idconv_get",
                        lambda url, params, ua, timeout=None: lit_net._IdconvResponse(status, text, error))


@pytest.mark.parametrize("status,text,error,kind", [
    (200, '{"records": [{"doi": "10.1/ABC", "pmid": 1}]}', "", Kind.OK),
    (200, '{"records": [{"pmid": 1, "status": "error"}]}', "", Kind.NO_MATCH),
    (200, "<html>not json</html>", "", Kind.ERROR),
    (0, "", "URLError: getaddrinfo failed", Kind.TRANSPORT),
    (429, "", "", Kind.REFUSED),
    (403, "", "", Kind.REFUSED),
    (500, "", "", Kind.OUTAGE),
])
def test_pmid_to_doi_types_every_result(monkeypatch, status, text, error, kind):
    _idconv(monkeypatch, status, text, error)
    res = harvest_citations.pmid_to_doi("12345678")
    assert res.kind is kind
    if kind is Kind.OK:
        assert res.payload == "10.1/abc"
    else:
        assert res.payload is None      # a failure is never a DOI-shaped empty string


def test_harvest_counts_a_failed_pmid_lookup_instead_of_swallowing_it(monkeypatch, tmp_path, capsys):
    src = tmp_path / "in"
    src.mkdir()
    (src / "a.nbib").write_text("PMID- 12345678\nTI  - NBIB Title\nDP  - 2018 Jun\n"
                                "FAU - Vasquez, Vera\n", encoding="utf-8")
    _idconv(monkeypatch, 0, "", "URLError: getaddrinfo failed")
    monkeypatch.setattr(sys, "argv", ["harvest_citations.py", "--source-dir", str(src),
                                      "--out-dir", str(tmp_path / "out"), "--no-search",
                                      "--sleep", "0"])
    harvest_citations.main()
    out = capsys.readouterr()
    assert "pmid_lookup_failed 1" in " ".join(out.out.split())
    assert "TRANSPORT" in out.err
    assert not (tmp_path / "out").exists()     # still a dry run


def test_harvest_help_runs_from_a_foreign_cwd(tmp_path):
    """harvest_citations now imports litpipe; a flat script run from elsewhere must still find it
    (the repo root is sys.path[0] when the script runs)."""
    repo = Path(__file__).resolve().parent.parent
    r = subprocess.run([sys.executable, str(repo / "harvest_citations.py"), "--help"],
                       cwd=tmp_path, capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, r.stderr
