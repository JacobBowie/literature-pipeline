"""Regression tests for the 2026-06-25-audit sibling-sweep fixes (post Batch-2 verification).

The adversarial sibling sweep that ran after the initial Batch-2 fixes landed found
real instances of the same bug-classes that the first pass missed. These lock them:
  - lit_util.coerce_int: shared int()-on-messy-CSV guard (T5d + the build_priority year-sort
    crash on a non-numeric residual-CSV year).
  - migrate_closed_to_md.read_report_chain: an ALREADY_EXISTS/skipped paper (PDF already on
    disk) must NOT be mis-routed to the manual-pull/ILL queue as closed-access (T5b's parallel
    oracle -- the exact harm T3 guards).
  - pipeline_check Stage 4b: the .ris-coverage check is a real per-project predicate now, not a
    hardcoded True no-op (the audit's prescribed 'delete all .ris -> assert exit 1' test).
"""
import csv
import importlib
import subprocess
import sys
from pathlib import Path

import pytest

import lit_util

REPO = Path(__file__).resolve().parent.parent


# ---------- lit_util.coerce_int ----------

@pytest.mark.parametrize("raw,expected", [
    ("1,234", 1234), ("  12 ", 12), ("2020.0", 2020), ("42", 42),
    ("in press", 0), ("2020a", 0), ("n/a", 0), ("", 0), (None, 0),
])
def test_coerce_int(raw, expected):
    assert lit_util.coerce_int(raw) == expected


def test_coerce_int_custom_default():
    assert lit_util.coerce_int("not-a-number", default=-1) == -1


def test_coerce_int_year_sort_key_no_crash():
    """The exact build_priority sort-key shape that used to ValueError on a non-numeric year."""
    rows = [{"score": 5, "year": "in press"}, {"score": 5, "year": "2020"}]
    rows.sort(key=lambda r: (-r["score"], -lit_util.coerce_int(r["year"])))  # must not raise
    assert [r["year"] for r in rows] == ["2020", "in press"]  # numeric year sorts ahead


# ---------- migrate_closed_to_md: ALREADY_EXISTS not mis-routed to ILL ----------

def _write_csv(path, fieldnames, rows):
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def _seed_unpaywall(root, date, doi):
    _write_csv(root / f"lit_pull_queue.{date}.unpaywall.csv",
               ["doi", "downloaded", "oa_status", "winning_host", "error", "title", "year"],
               [{"doi": doi, "downloaded": "False", "oa_status": "CLOSED",
                 "winning_host": "", "error": "", "title": "T", "year": "2020"}])


def test_migrate_pmc_already_exists_not_closed_access(tmp_path):
    migrate = importlib.import_module("migrate_closed_to_md")
    date = "2026-06-29"
    doi = "10.1234/already-pmc"
    _seed_unpaywall(tmp_path, date, doi)
    _write_csv(tmp_path / f"lit_pull_queue.{date}.pmc.csv",
               ["doi", "downloaded", "skipped", "winning_source", "pmcid"],
               [{"doi": doi, "downloaded": "False", "skipped": "True",
                 "winning_source": "ALREADY_EXISTS", "pmcid": ""}])
    out = migrate.read_report_chain(tmp_path, date)
    assert doi not in [r["doi"] for r in out]  # resolved (on disk), NOT closed-access


def test_migrate_preprint_already_exists_not_closed_access(tmp_path):
    migrate = importlib.import_module("migrate_closed_to_md")
    date = "2026-06-29"
    doi = "10.1234/already-ppr"
    _seed_unpaywall(tmp_path, date, doi)
    # PMC genuinely fails (no pmcid), preprint reports ALREADY_EXISTS
    _write_csv(tmp_path / f"lit_pull_queue.{date}.pmc.csv",
               ["doi", "downloaded", "skipped", "winning_source", "pmcid"],
               [{"doi": doi, "downloaded": "False", "skipped": "", "winning_source": "", "pmcid": ""}])
    _write_csv(tmp_path / f"lit_pull_queue.{date}.preprint.csv",
               ["doi", "downloaded", "skipped", "status"],
               [{"doi": doi, "downloaded": "False", "skipped": "True", "status": "ALREADY_EXISTS"}])
    out = migrate.read_report_chain(tmp_path, date)
    assert doi not in [r["doi"] for r in out]


def test_migrate_genuine_failure_still_reported(tmp_path):
    """Control: a DOI that genuinely failed every stage MUST still be reported as closed-access."""
    migrate = importlib.import_module("migrate_closed_to_md")
    date = "2026-06-29"
    doi = "10.1234/genuinely-closed"
    _seed_unpaywall(tmp_path, date, doi)
    _write_csv(tmp_path / f"lit_pull_queue.{date}.pmc.csv",
               ["doi", "downloaded", "skipped", "winning_source", "pmcid"],
               [{"doi": doi, "downloaded": "False", "skipped": "", "winning_source": "", "pmcid": ""}])
    out = migrate.read_report_chain(tmp_path, date)
    assert doi in [r["doi"] for r in out]


# ---------- pipeline_check Stage 4b: real predicate (the audit's prescribed test) ----------

def _make_pdf(path):
    path.write_bytes(b"%PDF-1.4\n" + b"x" * 200)  # valid magic byte, >0 size


def test_pipeline_check_stage4b_fails_on_zero_ris(tmp_path):
    """2 PDFs, 0 .ris (0% < 90% default floor) -> Stage 4b FAILs and pipeline_check exits 1.
    Before the fix this was check(..., True) and could never fail."""
    lib = tmp_path / "lib"
    lib.mkdir()
    _make_pdf(lib / "2020_Smith_Heat.pdf")
    _make_pdf(lib / "2021_Jones_Cold.pdf")
    proc = subprocess.run(
        [sys.executable, str(REPO / "pipeline_check.py"),
         "--base-dir", str(tmp_path), "--lib-dir", "lib", "--tier", "2"],
        capture_output=True, text=True)
    assert proc.returncode == 1, proc.stdout
    assert "below 90%" in proc.stdout  # the FAIL-branch detail string (proves the predicate fired)


def test_pipeline_check_stage4b_passes_empty_lib(tmp_path):
    """Empty lib (0 PDFs) -> the (not pdfs) guard keeps Stage 4b green (no false-fail)."""
    lib = tmp_path / "lib"
    lib.mkdir()
    proc = subprocess.run(
        [sys.executable, str(REPO / "pipeline_check.py"),
         "--base-dir", str(tmp_path), "--lib-dir", "lib", "--tier", "2"],
        capture_output=True, text=True)
    assert "below" not in proc.stdout  # Stage 4b did not fire a coverage failure
