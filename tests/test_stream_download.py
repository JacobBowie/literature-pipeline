"""lit_net.stream_download characterization (c7) -- offline via monkeypatched requests.get.

Pins the shared streaming core the 4 fetchers (unpaywall/pmc/preprint PDF + fetch_figures image)
delegate to: HTTP-status handling, the byte cap + truncation, first-chunk capture, and the
never-raise StreamResult contract. The per-adapter validators + status strings are guarded by
their own tests + test_report_contract.py.
"""
import requests

import lit_net


class FakeStreamResp:
    def __init__(self, status_code, chunks=(), raise_after=None):
        self.status_code = status_code
        self.headers = {}
        self._chunks = list(chunks)
        self._raise_after = raise_after  # raise a transport error after yielding this many chunks

    def iter_content(self, chunk_size=8192):
        for i, c in enumerate(self._chunks):
            if self._raise_after is not None and i == self._raise_after:
                raise requests.exceptions.ChunkedEncodingError("mid-stream drop")
            yield c


def _patch(monkeypatch, resp_or_exc):
    def fake_get(url, **kwargs):
        if isinstance(resp_or_exc, Exception):
            raise resp_or_exc
        return resp_or_exc
    monkeypatch.setattr(lit_net.requests, "get", fake_get)


def test_200_under_cap_returns_full_content(monkeypatch):
    _patch(monkeypatch, FakeStreamResp(200, [b"%PDF", b"-1.4 body"]))
    res = lit_net.stream_download("http://x", max_bytes=1000)
    assert res.status_code == 200
    assert res.first_chunk == b"%PDF"
    assert res.content == b"%PDF-1.4 body"
    assert res.total == 13 and res.truncated is False and res.error == ""


def test_200_over_cap_truncates_and_flags(monkeypatch):
    _patch(monkeypatch, FakeStreamResp(200, [b"A" * 8, b"B" * 8, b"C" * 8]))
    res = lit_net.stream_download("http://x", max_bytes=10)
    assert res.truncated is True
    assert res.total > 10  # stops once the cap is exceeded
    assert res.first_chunk == b"A" * 8


def test_http_error_returns_status_no_body(monkeypatch):
    _patch(monkeypatch, FakeStreamResp(404))
    res = lit_net.stream_download("http://x", max_bytes=1000)
    assert res == lit_net.StreamResult(404, b"", b"", 0, False, "")


def test_transport_error_returns_error_not_raise(monkeypatch):
    _patch(monkeypatch, requests.exceptions.ConnectionError("boom"))
    res = lit_net.stream_download("http://x", max_bytes=1000)
    assert res.status_code == 0 and res.error == "boom" and res.content == b""


def test_empty_200_stream(monkeypatch):
    _patch(monkeypatch, FakeStreamResp(200, []))
    res = lit_net.stream_download("http://x", max_bytes=1000)
    assert res.status_code == 200 and res.first_chunk == b"" and res.content == b""
    assert res.total == 0 and res.truncated is False


def test_mid_stream_error_returns_partial(monkeypatch):
    _patch(monkeypatch, FakeStreamResp(200, [b"AAA", b"BBB", b"CCC"], raise_after=2))
    res = lit_net.stream_download("http://x", max_bytes=1000)
    assert res.status_code == 200 and res.error != ""
    assert res.first_chunk == b"AAA" and res.content == b"AAABBB"  # 2 chunks before the drop


def test_max_pdf_bytes_is_80mb():
    assert lit_net.MAX_PDF_BYTES == 80_000_000


def test_prohibited_route_returns_a_failure_without_sending(monkeypatch):
    """W2-A1: a PMC article page or CDN blob is refused before any request."""
    def boom(url, **kwargs):
        raise AssertionError("sent a request to a prohibited route")
    monkeypatch.setattr(lit_net.requests, "get", boom)
    for url in ("https://pmc.ncbi.nlm.nih.gov/articles/PMC1/pdf/x.pdf",
                "https://cdn.ncbi.nlm.nih.gov/pmc/blobs/a/b/fig1.jpg"):
        res = lit_net.stream_download(url, max_bytes=1000)
        assert res.status_code == 0 and res.error.startswith("PROHIBITED") and res.content == b""
