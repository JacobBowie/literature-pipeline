"""Smoke test: every top-level script must import cleanly.

This catches the kind of import-time side-effect bug we hit on 2026-05-12
(every script wrapped sys.stdout at import, which crashed when scripts
imported each other).
"""
import importlib
import pytest

MODULES = [
    "audit_filenames",
    "audit_portfolio",
    "backfill_fulltext",
    "backfill_ris",
    "build_pdf_library",
    "enrich_abstracts",
    "enrich_recommendations",
    "extract_pdf_fulltext",
    "extract_tables",
    "fetch_figures",
    "forward_citations",
    "harvest_citations",
    "index_portfolio",
    "jats_to_text",
    "pdf_text_clean",
    "pipeline_check",
    "pmc_fetch",
    "preprint_fetch",
    "recheck_pmc",
    "reverse_citations",
    "ris_emit",
    "seed_queue_from_top_candidates",
    "snowball",
    "sweep",
    "unpaywall_fetch_v2",
    # 2026-07 Stage 3 commit 3: the four utf8-shim sites previously uncovered
    "build_priority_paywall_queue",
    "fill_missing_dois",
    "paywall_pull",
    "run_daily",
]


@pytest.mark.parametrize("modname", MODULES)
def test_module_imports_cleanly(modname):
    """Importing twice must not raise (idempotent stdout reconfigure)."""
    importlib.import_module(modname)
    importlib.reload(importlib.import_module(modname))


def test_utf8_stdout_survives_none_encoding(monkeypatch):
    """A stream whose .encoding is None must still be reconfigured, not silently
    skipped. The pre-consolidation guard did `getattr(sys.stdout,"encoding","").lower()`
    which raised AttributeError on a None encoding and swallowed it, leaving the stream
    un-hardened (then dying on the first non-ASCII print)."""
    import sys
    import lit_util
    calls = []

    class DummyStream:
        encoding = None

        def reconfigure(self, **kw):
            calls.append(kw)

    monkeypatch.setattr(sys, "stdout", DummyStream())
    monkeypatch.setattr(sys, "stderr", DummyStream())
    lit_util.utf8_stdout()  # must not raise
    assert len(calls) == 2  # stdout AND stderr reconfigured
    assert all(kw.get("encoding") == "utf-8" and kw.get("errors") == "replace"
               for kw in calls)


def test_utf8_stdout_swallows_closed_stream(monkeypatch):
    """A closed/detached stream raises ValueError on reconfigure; the helper must
    swallow it (import-time safety) rather than propagate at module load — the
    cross-module-import crash class fixed on 2026-05-12."""
    import sys
    import lit_util

    class ClosedStream:
        encoding = "utf-8"

        def reconfigure(self, **kw):
            raise ValueError("I/O operation on closed file")

    monkeypatch.setattr(sys, "stdout", ClosedStream())
    monkeypatch.setattr(sys, "stderr", ClosedStream())
    lit_util.utf8_stdout()  # must not raise
