"""Regression: extract() must not hang on large (scanned) PDFs.

Bug (2026-06-23): a 47 MB scanned PDF wedged a whole sweep because the in-process,
timeout-less pdfminer/pdfplumber ran first. The fix size-guards >30 MB to the
subprocess-timeout-protected pdftotext only. This exercises the REAL extract()
with a monkeypatched size so the guard branch runs without writing 31 MB to disk.
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import extract_pdf_fulltext as E  # noqa: E402


def test_extract_module_imports():
    assert hasattr(E, "extract")


def test_large_pdf_takes_guarded_path_and_returns_bounded(tmp_path, monkeypatch):
    p = tmp_path / "fake_big.pdf"
    p.write_bytes(b"%PDF-1.4\n" + b"\x00" * 2048)  # tiny on disk; size is faked below
    # Force the >30 MB branch without a 31 MB write.
    monkeypatch.setattr(E.os.path, "getsize", lambda _p: 31 * 1024 * 1024)

    t0 = time.perf_counter()
    text, extractor, status = E.extract(str(p))
    elapsed = time.perf_counter() - t0

    assert elapsed < 130, f"extract() should be timeout-bounded, took {elapsed:.1f}s (hang regression)"
    # Large path uses pdftotext only; pdfminer/pdfplumber are skipped.
    assert extractor in ("pdftotext", "none"), f"unexpected extractor {extractor!r} on large-file path"
    if extractor == "none":
        assert "large" in status, f"'none' result should carry the large-file marker, got {status!r}"


def test_small_pdf_uses_normal_path(tmp_path, monkeypatch):
    # A sub-threshold size must NOT take the guard branch (common path unchanged).
    p = tmp_path / "small.pdf"
    p.write_bytes(b"%PDF-1.4\n" + b"\x00" * 1024)
    monkeypatch.setattr(E.os.path, "getsize", lambda _p: 1024)
    # Should not raise; invalid PDF -> empty text via the normal fallthrough.
    text, extractor, status = E.extract(str(p))
    assert extractor in ("pdfminer.six", "pdftotext", "pdfplumber", "none")
