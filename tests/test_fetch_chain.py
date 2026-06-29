"""T5 (2026-06-25 audit) — fetch-chain robustness regression tests.

Covers the parts of T5 that are unit-isolatable:
  - T5a: is_known_boilerplate is wired into BOTH downstream fetchers (pmc_fetch,
    preprint_fetch) as the SAME object the Unpaywall stage uses, and it actually
    rejects a fingerprinted file.

T5b (sweep got_dois SKIP_EXISTS/ALREADY_EXISTS) and T5d (citation_count coercion)
live inside large entry-point functions (sweep.run_pipeline / unpaywall main) and are
integration-level per the audit ("queue a DOI whose PDF already exists; assert
preprint_fetch is NOT invoked"); they are guarded here only by the import smoke below.
"""
import hashlib
import importlib

import pytest


def test_t5a_boilerplate_shared_across_fetchers():
    """The boilerplate fingerprint must be the identical function in all three
    fetch stages — fixing one writer's PDF acceptance must not leave a sibling
    stage accepting the same landing page."""
    unpw = importlib.import_module("unpaywall_fetch_v2")
    pmc = importlib.import_module("pmc_fetch")
    ppr = importlib.import_module("preprint_fetch")
    assert pmc.is_known_boilerplate is unpw.is_known_boilerplate
    assert ppr.is_known_boilerplate is unpw.is_known_boilerplate


def test_t5a_rejects_fingerprinted_pdf(tmp_path, monkeypatch):
    unpw = importlib.import_module("unpaywall_fetch_v2")
    f = tmp_path / "landing.pdf"
    f.write_bytes(b"%PDF-1.4 fake publisher landing page boilerplate")
    md5 = hashlib.md5(f.read_bytes()).hexdigest()
    # Register this exact file as known boilerplate, then assert it is caught.
    monkeypatch.setitem(unpw.KNOWN_BOILERPLATE_MD5, md5, "TEST_LANDING")
    is_bp, tag = unpw.is_known_boilerplate(str(f))
    assert is_bp is True
    assert tag == "TEST_LANDING"


def test_t5a_passes_unknown_pdf(tmp_path):
    unpw = importlib.import_module("unpaywall_fetch_v2")
    f = tmp_path / "real.pdf"
    f.write_bytes(b"%PDF-1.7 genuinely a different document body " + b"x" * 200)
    is_bp, tag = unpw.is_known_boilerplate(str(f))
    assert is_bp is False
    assert tag is None


def test_t5_modules_import_clean():
    """The cross-module import (preprint_fetch -> unpaywall_fetch_v2) must not cycle."""
    for m in ("unpaywall_fetch_v2", "pmc_fetch", "preprint_fetch", "sweep"):
        assert importlib.import_module(m) is not None
