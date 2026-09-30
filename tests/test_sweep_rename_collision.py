"""Regression: a same-day re-sweep must neither crash on the processed rename nor overwrite the
first run's record.

Bug 1 (2026-06-23): a SECOND sweep of one project on the same day crashed with FileExistsError
(WinError 183) because os.rename refuses an existing target. The first fix numeric-suffixed the
archive name (.processed.2.csv) but left every other artifact on the dated name, so the second
run overwrote the first run's unpaywall/pmc/report files (35 of about 133 sweep invocations lost
their record; 2026-09-15 an 822-row run's report shrank to 4 rows).

Fix (W1-D1): one run id per invocation, YYYY-MM-DD then YYYY-MM-DD.N, chosen so no artifact of
that id exists, and every artifact carries it. These tests drive the real run_pipeline.
"""
import types
from pathlib import Path

import sweep


def _dois(path):
    return [ln.split(",")[0] for ln in Path(path).read_text(encoding="utf-8").splitlines()[1:]]


def _fake_run(cmd, **kw):
    """Stage stand-in: every row CLOSED, NO_PMCID, NO_MATCH (a report row per input row)."""
    cmd = [str(c) for c in cmd]
    rep = lambda flag: cmd[cmd.index(flag) + 1]
    script = next(Path(c).name for c in cmd if c.endswith(".py"))
    if script == "unpaywall_fetch_v2.py":
        Path(rep("--report")).write_text("doi,oa_status,downloaded,error\n" + "".join(
            f"{d},CLOSED,False,\n" for d in _dois(rep("--triage"))), encoding="utf-8")
    elif script == "pmc_fetch.py":
        Path(rep("--report-out")).write_text("doi,downloaded,skipped,error,sidecar\n" + "".join(
            f"{d},False,False,NO_PMCID,False\n" for d in _dois(rep("--report-in"))), encoding="utf-8")
    elif script == "preprint_fetch.py":
        Path(rep("--report")).write_text("doi,downloaded,skipped,status\n" + "".join(
            f"{d},False,False,NO_MATCH\n" for d in _dois(rep("--triage"))), encoding="utf-8")
    return types.SimpleNamespace(returncode=0, stdout="", stderr="")


def _stage(project, doi):
    (project / "lit").mkdir(exist_ok=True)
    q = project / "lit_pull_queue.csv"
    q.write_text(f"doi,title,authors,year,destination,notes\n{doi},T,Smith J,2020,lit,\n",
                 encoding="utf-8")
    return q


def test_sweep_module_imports():
    assert hasattr(sweep, "run_pipeline")


def test_first_run_uses_the_plain_dated_names(tmp_path, monkeypatch):
    monkeypatch.setattr(sweep.subprocess, "run", _fake_run)
    q = _stage(tmp_path, "10.1000/one")
    out = sweep.run_pipeline(tmp_path, q, run_date="2026-06-23")
    assert Path(out["processed"]).name == "lit_pull_queue.2026-06-23.processed.csv"
    assert Path(out["report"]).name == "lit_pull_queue.2026-06-23.report.csv"
    assert not q.exists()


def test_same_day_resweep_does_not_crash_or_overwrite(tmp_path, monkeypatch):
    monkeypatch.setattr(sweep.subprocess, "run", _fake_run)
    first = sweep.run_pipeline(tmp_path, _stage(tmp_path, "10.1000/one"), run_date="2026-06-23")
    assert first["run_id"] == "2026-06-23"
    first_report = tmp_path / "lit_pull_queue.2026-06-23.unpaywall.csv"
    before = first_report.read_text(encoding="utf-8")
    second = sweep.run_pipeline(tmp_path, _stage(tmp_path, "10.1000/two"), run_date="2026-06-23")
    assert Path(second["processed"]).name == "lit_pull_queue.2026-06-23.2.processed.csv"
    assert first_report.read_text(encoding="utf-8") == before          # original NOT clobbered
    assert "10.1000/two" in (tmp_path / "lit_pull_queue.2026-06-23.2.unpaywall.csv").read_text(
        encoding="utf-8")


def test_legacy_processed_suffixes_count_as_taken(tmp_path, monkeypatch):
    """A day already carrying the old .processed.csv and .processed.2.csv gets run id .2 (no
    .2 run-id artifact exists yet); the old files are untouched."""
    monkeypatch.setattr(sweep.subprocess, "run", _fake_run)
    (tmp_path / "lit_pull_queue.2026-06-23.processed.csv").write_text("a", encoding="utf-8")
    (tmp_path / "lit_pull_queue.2026-06-23.processed.2.csv").write_text("b", encoding="utf-8")
    out = sweep.run_pipeline(tmp_path, _stage(tmp_path, "10.1000/x"), run_date="2026-06-23")
    assert out["run_id"] == "2026-06-23.2"
    assert (tmp_path / "lit_pull_queue.2026-06-23.processed.2.csv").read_text(encoding="utf-8") == "b"
    assert Path(out["processed"]).exists()
