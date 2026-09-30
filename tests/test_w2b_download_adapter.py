"""unpaywall_fetch_v2.try_download -> (status, msg): the producer contract of
tests/test_download_adapters.py, on the litpipe.net route (W2-B).

The six `test_unpaywall_*` tests in test_download_adapters.py patch lit_net.stream_download, which
the stage no longer calls; these are their replacements (same statuses, same messages), patching
litpipe.net's transport instead. Offline: the transport returns a canned net._Raw.
"""
import pytest
from requests.structures import CaseInsensitiveDict

import unpaywall_fetch_v2 as unpw
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


def test_unpaywall_ok(tmp_path, reply):
    reply(200, PDF_OK)
    dest = tmp_path / "a.pdf"
    assert unpw.try_download("http://x.example.org/a.pdf", str(dest)) == ("OK", str(len(PDF_OK)))
    assert dest.exists()


def test_unpaywall_http_error(tmp_path, reply):
    reply(404, b"")
    assert unpw.try_download("http://x.example.org/a.pdf", str(tmp_path / "a.pdf")) == ("HTTP_404", "")


def test_unpaywall_too_large(tmp_path, reply):
    reply(200, b"%PDF" + b"x" * 100, truncated=True, total=99999)
    assert unpw.try_download("http://x.example.org/a.pdf", str(tmp_path / "a.pdf")) == ("TOO_LARGE", ">99999B")


def test_unpaywall_empty(tmp_path, reply):
    reply(200, b"")
    assert unpw.try_download("http://x.example.org/a.pdf", str(tmp_path / "a.pdf")) == ("EMPTY", "")


def test_unpaywall_error(tmp_path, reply):
    reply(error="boom")
    assert unpw.try_download("http://x.example.org/a.pdf", str(tmp_path / "a.pdf")) == ("ERROR", "boom")


def test_unpaywall_html_returns_bytes(tmp_path, reply):
    html = b"<html>landing</html>"
    reply(200, html)
    assert unpw.try_download("http://x.example.org/a.pdf", str(tmp_path / "a.pdf")) == ("HTML", html)


def test_unpaywall_small_pdf_is_not_written(tmp_path, reply):
    reply(200, b"%PDF-1.4 stub")
    dest = tmp_path / "a.pdf"
    assert unpw.try_download("http://x.example.org/a.pdf", str(dest)) == ("TOO_SMALL", "13B")
    assert not dest.exists()
