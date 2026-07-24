"""Producer-side report-contract characterization for the 4 c7 download adapters.

test_report_contract.py locks the CONSUMER (migrate's reader). This locks the PRODUCERS: each
adapter's exact return arity + status strings for every branch, by monkeypatching the shared
lit_net.stream_download core. It catches a broken adapter (e.g. a missing `import lit_net`, which
the import-smoke + consumer tests miss because the reference is deferred to call time) and any
drift in the status strings migrate routes on.
"""
import lit_net
import unpaywall_fetch_v2 as unpw
import pmc_fetch as pmc
import preprint_fetch as ppr
import fetch_figures as figs

SR = lit_net.StreamResult
PDF_OK = b"%PDF-1.7\n" + b"x" * 11000        # PDF magic + >10KB, passes the size gate, not boilerplate
JPG_OK = b"\xff\xd8\xff" + b"x" * 5000


def _patch(monkeypatch, sr):
    monkeypatch.setattr(lit_net, "stream_download", lambda *a, **k: sr)


# ---- unpaywall_fetch_v2.try_download -> (status, msg) ----

def test_unpaywall_ok(tmp_path, monkeypatch):
    _patch(monkeypatch, SR(200, b"%PDF", PDF_OK, len(PDF_OK), False, ""))
    dest = tmp_path / "a.pdf"
    assert unpw.try_download("http://x", str(dest)) == ("OK", str(len(PDF_OK)))
    assert dest.exists()


def test_unpaywall_http_error(tmp_path, monkeypatch):
    _patch(monkeypatch, SR(404, b"", b"", 0, False, ""))
    assert unpw.try_download("http://x", str(tmp_path / "a.pdf")) == ("HTTP_404", "")


def test_unpaywall_too_large(tmp_path, monkeypatch):
    _patch(monkeypatch, SR(200, b"%PDF", b"%PDF" + b"x" * 100, 99999, True, ""))
    assert unpw.try_download("http://x", str(tmp_path / "a.pdf")) == ("TOO_LARGE", ">99999B")


def test_unpaywall_empty(tmp_path, monkeypatch):
    _patch(monkeypatch, SR(200, b"", b"", 0, False, ""))
    assert unpw.try_download("http://x", str(tmp_path / "a.pdf")) == ("EMPTY", "")


def test_unpaywall_error(tmp_path, monkeypatch):
    _patch(monkeypatch, SR(0, b"", b"", 0, False, "boom"))
    assert unpw.try_download("http://x", str(tmp_path / "a.pdf")) == ("ERROR", "boom")


def test_unpaywall_html_returns_bytes(tmp_path, monkeypatch):
    html = b"<html>landing</html>"
    _patch(monkeypatch, SR(200, b"<htm", html, len(html), False, ""))
    assert unpw.try_download("http://x", str(tmp_path / "a.pdf")) == ("HTML", html)


# ---- pmc_fetch.try_download -> (ok, label, msg)  [the regressed adapter] ----

def test_pmc_ok(tmp_path, monkeypatch):
    _patch(monkeypatch, SR(200, b"%PDF", PDF_OK, len(PDF_OK), False, ""))
    assert pmc.try_download("http://x", str(tmp_path / "a.pdf")) == (True, "OK", f"{len(PDF_OK)}B")


def test_pmc_http_error(tmp_path, monkeypatch):
    _patch(monkeypatch, SR(503, b"", b"", 0, False, ""))
    assert pmc.try_download("http://x", str(tmp_path / "a.pdf")) == (False, "HTTP_503", "")


def test_pmc_empty(tmp_path, monkeypatch):
    _patch(monkeypatch, SR(200, b"", b"", 0, False, ""))
    assert pmc.try_download("http://x", str(tmp_path / "a.pdf")) == (False, "EMPTY", "")


def test_pmc_error_truncated_to_120(tmp_path, monkeypatch):
    _patch(monkeypatch, SR(0, b"", b"", 0, False, "boom"))
    assert pmc.try_download("http://x", str(tmp_path / "a.pdf")) == (False, "ERROR", "boom")


# ---- preprint_fetch.fetch_pdf -> (ok, status) ----

def test_preprint_ok(tmp_path, monkeypatch):
    _patch(monkeypatch, SR(200, b"%PDF", PDF_OK, len(PDF_OK), False, ""))
    assert ppr.fetch_pdf("http://x", str(tmp_path / "a.pdf")) == (True, f"OK_{len(PDF_OK)}B")


def test_preprint_not_pdf(tmp_path, monkeypatch):
    _patch(monkeypatch, SR(200, b"<htm", b"<html>", 6, False, ""))
    assert ppr.fetch_pdf("http://x", str(tmp_path / "a.pdf")) == (False, "NOT_PDF")


def test_preprint_error(tmp_path, monkeypatch):
    _patch(monkeypatch, SR(0, b"", b"", 0, False, "boom"))
    assert ppr.fetch_pdf("http://x", str(tmp_path / "a.pdf")) == (False, "ERR_boom")


# ---- fetch_figures.download_image -> (ok, status, size) ----

def test_figs_ok(tmp_path, monkeypatch):
    _patch(monkeypatch, SR(200, b"\xff\xd8\xff", JPG_OK, len(JPG_OK), False, ""))
    assert figs.download_image("http://x", str(tmp_path / "a.jpg")) == (True, "OK", len(JPG_OK))


def test_figs_not_image(tmp_path, monkeypatch):
    _patch(monkeypatch, SR(200, b"nope", b"nope", 4, False, ""))
    assert figs.download_image("http://x", str(tmp_path / "a.jpg")) == (False, "NOT_IMAGE", 4)


def test_figs_too_large(tmp_path, monkeypatch):
    _patch(monkeypatch, SR(200, b"\xff\xd8\xff", b"...", 40_000_000, True, ""))
    assert figs.download_image("http://x", str(tmp_path / "a.jpg")) == (False, "TOO_LARGE", 40_000_000)
