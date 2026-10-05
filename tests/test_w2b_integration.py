"""W2b seam locks (dispatcher, 2026-10-05): W2-C (preprint_fetch writes the typed report columns) and
W2-G (sweep reads them) were built in separate trees against a written contract. These drive rows in
the exact column set each stage writes through sweep's real verdict functions and classify."""
import pytest

import pmc_fetch
import preprint_fetch
import sweep
import unpaywall_fetch_v2

CONTRACT = {"outcome", "first_status", "route", "identity"}


@pytest.mark.parametrize("mod,extra", [
    (preprint_fetch, {"detail", "release_date", "landing_url", "sidecar", "sidecar_status"}),
    (pmc_fetch, {"release_date", "sidecar", "sidecar_status"}),
    (unpaywall_fetch_v2, {"detail", "doc_kind"}),
])
def test_each_stage_writes_the_typed_columns_sweep_reads(mod, extra):
    fields = set(mod.REPORT_FIELDS)
    assert CONTRACT | extra <= fields
    assert set(sweep.TYPED_COLUMNS) & fields >= CONTRACT


def _row(**kw):
    r = dict.fromkeys(preprint_fetch.REPORT_FIELDS, "")
    r.update(doi="10.1000/p1", downloaded="False", skipped="False", found="False")
    r.update({k: str(v) for k, v in kw.items()})
    assert set(r) == set(preprint_fetch.REPORT_FIELDS)      # only columns the stage really writes
    return r


UPW_NO = {"doi": "10.1000/p1", "downloaded": "False", "oa_status": "", "error": "HTTP 404",
          "outcome": "NO_MATCH", "route": "api"}
PMC_NO = {"doi": "10.1000/p1", "downloaded": "False", "skipped": "False", "error": "NO_PMCID",
          "outcome": "NO_MATCH", "pmcid": ""}


def _classify(pre):
    return sweep.classify([sweep.unpaywall_verdict(UPW_NO), sweep.pmc_verdict(PMC_NO),
                           sweep.preprint_verdict(pre)], attempts=1)[0]


def test_preprint_text_only_sidecar_is_text_only_not_closed():
    """W2-C writes Europe PMC preprint full text as a text-only sidecar (NOT_AVAILABLE + sidecar);
    it must route TEXT_ONLY, not TERMINAL_CLOSED to the ILL list (W2-C forward 4)."""
    pre = _row(status="NOT_AVAILABLE:text_only", outcome="NOT_AVAILABLE", route="epmc_fulltext",
               detail="text only", sidecar="True", sidecar_status="OK", found="True")
    assert sweep.preprint_verdict(pre).text_only
    assert _classify(pre) == "TEXT_ONLY"


def test_preprint_text_only_with_a_flag_is_not_text_only():
    pre = _row(status="DOI_MISMATCH:identity=FLAG", outcome="ERROR", identity="FLAG", sidecar="True",
               sidecar_status="OK", found="True", preprint_filename="2020_Doe_T_preprint.pdf")
    v = sweep.preprint_verdict(pre)
    assert v.identity and not v.text_only
    assert _classify(pre) == "IDENTITY_FLAG"


@pytest.mark.parametrize("typed", [True, False])
def test_source_excluded_is_dropped_typed_or_legacy(typed):
    """A source the project does not enable is not an enabled stage: alone it is SKIPPED_SOURCE,
    beside not-found stages the row is TERMINAL_CLOSED (never swallowed)."""
    kw = dict(status="SOURCE_EXCLUDED:biorxiv")
    if typed:
        kw.update(outcome="SKIPPED", detail="source_excluded:biorxiv", route="none")
    pre = _row(**kw)
    v = sweep.preprint_verdict(pre)
    assert v.excluded_source == "biorxiv"
    assert sweep.classify([v], attempts=1)[0] == "SKIPPED_SOURCE"
    assert _classify(pre) == "TERMINAL_CLOSED"


def test_arxiv_refusal_is_transient_not_oa_blocked():
    pre = _row(status="REFUSED [REFUSED]", outcome="REFUSED", route="arxiv",
               detail="host_refused:export.arxiv.org", found="False")
    assert _classify(pre) == "TRANSIENT"


def test_manual_preprint_is_oa_blocked_with_its_landing_link():
    pre = _row(status="MANUAL_PREPRINT", outcome="SKIPPED", route="biorxiv", detail="manual_preprint",
               found="True", landing_url="https://doi.org/10.64898/2026.03.02.705347")
    v = sweep.preprint_verdict(pre)
    assert v.manual_preprint and v.landing_url.endswith("705347")
    assert _classify(pre) == "OA_BLOCKED"


def test_row_deadline_is_transient():
    pre = _row(status="DEFERRED:row_deadline:90s", outcome="DEFERRED", route="deadline",
               detail="row_deadline:90s")
    assert _classify(pre) == "TRANSIENT"


# ---------------------------------------------------------------- the quarantine is gone (W2-C forward 1)
@pytest.mark.parametrize("modname", ["unpaywall_fetch_v2", "pmc_fetch", "preprint_fetch", "import_downloads"])
def test_no_stage_quarantines_a_file(modname):
    """The identity check replaced the _mismatch quarantine (W2-B, W2-A1, W2-C): no fetch stage keeps
    the helpers or moves a file into _mismatch/ (only the instruments read that folder, DEC-07)."""
    import importlib
    import inspect
    try:
        m = importlib.import_module(modname)
    except ImportError:
        pytest.skip(f"{modname} not in this tree yet")
    assert not hasattr(m, "quarantine_mismatch") and not hasattr(m, "pdf_doi_disagrees")
    src = inspect.getsource(m)
    assert '"_mismatch"' not in src and "'_mismatch'" not in src
