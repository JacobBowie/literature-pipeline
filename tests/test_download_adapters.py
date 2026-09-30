"""Producer-side report-contract characterization for the 4 c7 download adapters.

test_report_contract.py locks the CONSUMER (migrate's reader). This locks the PRODUCERS: each
adapter's exact return arity + status strings for every branch, by monkeypatching the shared
lit_net.stream_download core. It catches a broken adapter (e.g. a missing `import lit_net`, which
the import-smoke + consumer tests miss because the reference is deferred to call time) and any
drift in the status strings migrate routes on.
"""
import lit_net
import preprint_fetch as ppr

SR = lit_net.StreamResult
PDF_OK = b"%PDF-1.7\n" + b"x" * 11000        # PDF magic + >10KB, passes the size gate, not boilerplate


def _patch(monkeypatch, sr):
    monkeypatch.setattr(lit_net, "stream_download", lambda *a, **k: sr)


# ---- unpaywall_fetch_v2.try_download -> (status, msg) ----

# ---- pmc_fetch.try_download -> (ok, label, msg)  [the regressed adapter] ----

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


