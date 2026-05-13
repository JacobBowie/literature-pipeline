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
]


@pytest.mark.parametrize("modname", MODULES)
def test_module_imports_cleanly(modname):
    """Importing twice must not raise (idempotent stdout reconfigure)."""
    importlib.import_module(modname)
    importlib.reload(importlib.import_module(modname))
