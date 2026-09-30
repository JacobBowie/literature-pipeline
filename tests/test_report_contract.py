"""Batch 4 guardrail (pin BEFORE c7 touches the fetchers), extended for W1-D2.

migrate_closed_to_md.read_report_chain decides which DOIs get routed where by reading specific
columns + magic status strings from the unpaywall / pmc / preprint report CSVs. If a fetcher renames
a column or changes a status string, migrate would SILENTLY mis-route a fetched paper (both sides use
.get() / extrasaction='ignore', so nothing crashes). This locks the contract: the columns
`downloaded`, `oa_status`, `winning_source`, `status`, `doi`, `attempts`, `sidecar` and the magic
values `SKIP_EXISTS` / `ALREADY_EXISTS` must survive the consolidation. W1-D2 adds the real report
shapes (tests/fixtures/W1-D2/chain_real: 11 rows of a consumer's 2026-09-22 chain, copied read-only,
email redacted) and the artifact-name and retry_later column contracts.
"""
import csv
import datetime
import json
import shutil
from pathlib import Path

import pytest

import lit_util
import migrate_closed_to_md as mig
from litpipe import config

FIX = Path(__file__).parent / "fixtures" / "W1-D2"
REAL_RUN = "2026-09-22"


def _write(path, fields, rows):
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader(); w.writerows(rows)


def test_report_chain_routing_contract(tmp_path):
    date = "2026-07-20"
    _write(tmp_path / f"lit_pull_queue.{date}.unpaywall.csv",
           ["doi", "downloaded", "oa_status", "title", "year", "winning_host", "error"], [
               {"doi": "10.1/A", "downloaded": "True",  "oa_status": "open",       "title": "A", "year": "2024", "winning_host": "h", "error": ""},
               {"doi": "10.1/B", "downloaded": "False", "oa_status": "SKIP_EXISTS", "title": "B", "year": "2024", "winning_host": "",  "error": ""},
               {"doi": "10.1/C", "downloaded": "False", "oa_status": "closed",      "title": "C", "year": "2024", "winning_host": "",  "error": ""},
               {"doi": "10.1/D", "downloaded": "False", "oa_status": "closed",      "title": "D", "year": "2024", "winning_host": "",  "error": ""},
           ])
    _write(tmp_path / f"lit_pull_queue.{date}.pmc.csv",
           ["doi", "downloaded", "skipped", "winning_source", "pmcid"], [
               {"doi": "10.1/C", "downloaded": "False", "skipped": "False", "winning_source": "ALREADY_EXISTS", "pmcid": ""},
               {"doi": "10.1/D", "downloaded": "False", "skipped": "False", "winning_source": "",               "pmcid": ""},
           ])
    _write(tmp_path / f"lit_pull_queue.{date}.preprint.csv",
           ["doi", "downloaded", "skipped", "status"], [
               {"doi": "10.1/D", "downloaded": "False", "skipped": "False", "status": "NO_MATCH"},
           ])

    residual = {r["doi"] for r in mig.read_report_chain(tmp_path, date)}
    # A downloaded; B already in lib (SKIP_EXISTS); C resolved by PMC (ALREADY_EXISTS) -> only D
    assert residual == {"10.1/D"}, residual


# ---------------------------------------------------------------- W1-D2: the real report shapes
# Expected classes for the real chain. With the library known and no sidecar on disk, a PMC refusal
# (europepmc/HTTP_403, ncbi-page/HTML) or an Unpaywall 403 / HTML makes the row OA_BLOCKED, ahead of
# the DNS error on two of them (first match wins); CLOSED + NO_PMCID + NO_MATCH and the API-form
# "HTTP 404" (not in Unpaywall) are TERMINAL_CLOSED. One row was downloaded and is not a residual.
REAL_EXPECTED = {
    "10.3390/nu16020189": "OA_BLOCKED", "10.1080/07853890.2024.2304650": "OA_BLOCKED",
    "10.1007/s40279-015-0365-0": "OA_BLOCKED", "10.1016/j.cmet.2018.04.010": "OA_BLOCKED",
    "10.1038/nrcardio.2017.224": "TERMINAL_CLOSED", "10.1016/s2213-8587(18)30137-2": "TERMINAL_CLOSED",
    "10.3305/nh.2015.31.3.8434": "TERMINAL_CLOSED", "10.1186/s12966-019-0858-6": "OA_BLOCKED",
    "10.3390/ijms23158713": "OA_BLOCKED", "10.1111/jch.13895": "OA_BLOCKED",
}
# The four rows whose PMC report says a sidecar was written (OK / EXISTS).
REAL_SIDECAR_ROWS = {"10.3390/nu16020189", "10.1080/07853890.2024.2304650",
                     "10.1186/s12966-019-0858-6", "10.3390/ijms23158713"}


@pytest.fixture
def real_project(tmp_path, monkeypatch):
    root = tmp_path / "Projects"
    proj = root / "P"
    shutil.copytree(FIX / "chain_real", proj)
    (proj / "literature").mkdir()
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    cfgp = tmp_path / "projects.json"
    cfgp.write_text(json.dumps({"state_dir": str(tmp_path / "state"), "projects": {}}), encoding="utf-8")
    monkeypatch.setattr(config, "CONFIG_PATH", cfgp)
    return proj


def _cfg(tmp_path, lib_dir):
    return {"state_dir": str(tmp_path / "state"), "projects": {"P": {"lib_dir": lib_dir} if lib_dir else {}}}


def test_real_chain_routes_by_class_with_the_library_known(real_project):
    rows = mig.read_report_chain(real_project, REAL_RUN)
    assert {r["doi"] for r in rows} == set(REAL_EXPECTED)
    for r in rows:
        mig.classify_row(r, default_lib=real_project / "literature")
    assert {r["doi"]: r["residual_class"] for r in rows} == REAL_EXPECTED


def test_real_chain_trusts_the_sidecar_flag_when_no_library_is_known(real_project):
    rows = mig.read_report_chain(real_project, REAL_RUN)   # no normalized artifact, no registry lib
    assert {r["doi"] for r in rows if r["residual_class"] == "TEXT_ONLY"} == REAL_SIDECAR_ROWS


def test_real_chain_writes_redacted_routes(real_project, tmp_path):
    res = mig.run("P", cfg=_cfg(tmp_path, "literature"), today=datetime.date(2026, 9, 30))
    assert res["counts"] == {"OA_BLOCKED": 7, "TERMINAL_CLOSED": 3}
    for name in (mig.ILL_NAME, mig.OA_BLOCKED_NAME, mig.RETRY_LATER_NAME,
                 f"lit_pull_queue.{REAL_RUN}.routing.csv"):
        body = (real_project / name).read_text(encoding="utf-8")
        assert "email=" not in body and "fixture.user" not in body, name


@pytest.mark.parametrize("name,parsed", [
    ("lit_pull_queue.2026-09-22.unpaywall.csv", (None, "2026-09-22", "unpaywall")),
    ("lit_pull_queue.2026-09-30.2.pmc.csv", (None, "2026-09-30.2", "pmc")),
    ("lit_pull_queue.retry.2026-09-30.preprint.csv", ("retry", "2026-09-30", "preprint")),
    ("lit_pull_queue.ch15_b01.2026-09-30.10.routing.csv", ("ch15_b01", "2026-09-30.10", "routing")),
])
def test_artifact_names_read(name, parsed):
    m = mig._ARTIFACT.match(name)
    assert m and (m.group("tag"), m.group("run"), m.group("stage")) == parsed


@pytest.mark.parametrize("name", [
    "lit_pull_queue.snapshot.4501bc_b01.unpaywall.csv",   # VAP's own snapshot names
    "lit_pull_queue.2026-09-22.processed.10.csv",         # legacy processed.N
    "lit_pull_queue.csv", "lit_pull_queue.retry_later.csv", "lit_pull_queue.ch15_b01.draft.csv",
])
def test_non_run_artifacts_are_not_read_as_chains(name):
    assert mig._ARTIFACT.match(name) is None


def test_retry_later_columns_lead_with_the_queue_contract():
    """sweep (W1-D1 admit_retries) re-admits due retry_later rows, every column, as the `retry`
    queue: the six queue columns come first, and `attempts` and `not_before` are carried."""
    assert mig.RETRY_FIELDS[:6] == ["doi", "title", "authors", "year", "destination", "notes"]
    assert {"residual_class", "not_before", "attempts"} <= set(mig.RETRY_FIELDS)
