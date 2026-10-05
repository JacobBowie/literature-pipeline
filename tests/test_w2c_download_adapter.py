"""preprint_fetch.fetch_pdf -> (ok, status): the producer contract of tests/test_download_adapters.py,
on the litpipe.net route (W2-C).

The three `test_preprint_*` tests in test_download_adapters.py patch lit_net.stream_download, which
the stage no longer calls; these are their replacements (same statuses), patching litpipe.net's
transport instead (the W2-B pattern). Offline: the transport returns a canned net._Raw.
"""
import pytest
from requests.structures import CaseInsensitiveDict

import preprint_fetch as ppr
from litpipe import net

PDF_OK = b"%PDF-1.7\n" + b"x" * 11000        # PDF magic + >10KB, passes the size gate, not boilerplate


@pytest.fixture
def reply(net_env, monkeypatch):
    def set_reply(status=200, body=b"", error="", truncated=False, total=None):
        def fake(method, url, headers, data, timeout, max_bytes):
            if error:
                return net._Raw(error=error)
            return net._Raw(status, CaseInsensitiveDict({}), body, body[:65536],
                            len(body) if total is None else total, truncated)
        monkeypatch.setitem(net._TRANSPORTS, "requests", fake)
    return set_reply


def test_preprint_ok(tmp_path, reply):
    reply(200, PDF_OK)
    assert ppr.fetch_pdf("http://x.example.org/a.pdf", str(tmp_path / "a.pdf")) == (True, f"OK_{len(PDF_OK)}B")
    assert (tmp_path / "a.pdf").read_bytes() == PDF_OK


def test_preprint_not_pdf(tmp_path, reply):
    reply(200, b"<html>")
    assert ppr.fetch_pdf("http://x.example.org/a.pdf", str(tmp_path / "a.pdf")) == (False, "NOT_PDF")
    assert not (tmp_path / "a.pdf").exists()


def test_preprint_error(tmp_path, reply):
    reply(error="boom")
    assert ppr.fetch_pdf("http://x.example.org/a.pdf", str(tmp_path / "a.pdf")) == (False, "ERR_boom")


def test_preprint_http_error_and_small(tmp_path, reply):
    reply(404, b"")
    assert ppr.fetch_pdf("http://x.example.org/a.pdf", str(tmp_path / "a.pdf")) == (False, "HTTP_404")
    reply(200, b"%PDF-1.4 stub")
    assert ppr.fetch_pdf("http://x.example.org/a.pdf", str(tmp_path / "a.pdf")) == (False, "TOO_SMALL_13B")
    assert not (tmp_path / "a.pdf").exists()


def test_preprint_prohibited_route_is_not_sent(tmp_path, reply):
    reply(200, PDF_OK)
    ok, status = ppr.fetch_pdf("https://europepmc.org/articles/PMC1?pdf=render", str(tmp_path / "a.pdf"))
    assert ok is False and status.startswith("ERR_PROHIBITED")
