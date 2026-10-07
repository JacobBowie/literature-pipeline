"""run_daily as a wrapper over `python -m litpipe.runner run --profile daily` (W4-A, amendment 14).

W4-0 locked how run_daily read snowball's exit (a DEGRADED walk did not stop seeding or sweeping;
argparse's exit 2 without the summary line was fatal). The wrapper no longer runs snowball: the
runner walks each project on its walk_cadence_days through the stage shim, so those locks are
rewritten here onto the runner's behaviour:
  * a DEGRADED walk never stops staging or sweeping (the walk runs after the sweep; one stage's
    failure never ends the run) and run_daily exits 2;
  * an exit 2 without a result is never read as DEGRADED: the shim's missing result is ERROR, and
    run_daily exits 1;
  * a FAILED walk is reported (exit 1) and every project still runs.
The flags are kept (--project, --with-snowball, --dry-run); main() reads sys.argv when argv is None.
The stages are the stand-ins of tests/fixtures/W4-A, run in-process (tests/fixtures/W4-A/w4a_world)."""
import sys
from pathlib import Path

import pytest

FIX = Path(__file__).resolve().parent / "fixtures" / "W4-A"
sys.path.insert(0, str(FIX))

import run_daily  # noqa: E402
from litpipe import runner  # noqa: E402
from w4a_world import World  # noqa: E402

DOIS = ["10.5555/x.0001", "10.5555/x.0002"]


@pytest.fixture
def w(tmp_path, monkeypatch):
    return World(tmp_path, monkeypatch)


def main(monkeypatch, *argv):
    monkeypatch.setattr(sys, "argv", ["run_daily.py", *argv])
    return run_daily.main()                                     # reads sys.argv (tests drive CLIs that way)


# ================================================================ the W4-0 locks, rewritten
def test_degraded_walk_still_stages_and_sweeps_and_exits_2(w, monkeypatch):
    w.register("X", auto_stage=True)
    w.register("Y")
    w.queue("Y", DOIS)
    w.set_fake("walk", "X", exit_code=2, reasons=["walk stand-in: 2 of 3 seed walks failed"])
    assert main(monkeypatch, "--with-snowball") == 2
    assert [c["stage"] for c in w.calls() if c["project"] == "X"][:3] == ["seed", "sweep", "route"]
    assert [c["project"] for c in w.calls("sweep")] == ["X", "Y"]          # the next project too
    s = w.summaries()[-1]
    assert [j["status"] for j in s["jobs"] if j["job"] == "walk" and j["project"] == "X"] == ["DEGRADED"]


def test_an_exit_2_without_a_result_is_never_degraded(w, monkeypatch):
    w.register("X", walk_cadence_days=1)
    w.set_fake("walk", "X", mode="exit2")                       # SystemExit(2): argparse's usage exit shape
    assert main(monkeypatch) == 1
    j = [j for j in w.summaries()[-1]["jobs"] if j["job"] == "walk"][0]
    assert j["status"] == "ERROR" and "no result" in j["reason"] and "shim exit 2" in j["reason"]


def test_a_failed_walk_exits_1_and_every_project_still_runs(w, monkeypatch):
    w.register("X", walk_cadence_days=1)
    w.register("Y", walk_cadence_days=1)
    w.queue("Y", DOIS)
    w.set_fake("walk", "X", exit_code=1, result={"error": "config"})
    assert main(monkeypatch) == 1
    assert [c["project"] for c in w.calls("walk")] == ["X", "Y"] and [c["project"] for c in w.calls("sweep")] == ["Y"]


def test_a_clean_run_exits_0(w, monkeypatch):
    w.register("X", walk_cadence_days=1)
    w.queue("X", DOIS)
    assert main(monkeypatch) == 0
    assert [c["stage"] for c in w.calls()] == ["sweep", "route", "walk"]


# ================================================================ the exit mapping
def _summ(code=0, jobs=(), health="PASS"):
    return {"exit_code": code, "health": {"status": health},
            "jobs": [{"job": j, "status": s, "counts_for_exit": c} for j, s, c in jobs]}


@pytest.mark.parametrize("summ,code", [
    (_summ(0), 0),
    (_summ(1), 1),                                             # usage or config
    (_summ(3), 1),                                             # aborted
    (_summ(2, [("sweep", "FAILED", True)]), 1),
    (_summ(2, [("walk", "ERROR", True)]), 1),                  # a crash or a timeout
    (_summ(2, [("walk", "DEGRADED", True)]), 2),
    (_summ(2, [("walk", "DEFERRED", True)]), 2),
    (_summ(2, [], health="ALARM"), 2),
    (_summ(2, [("walk", "DEGRADED", True), ("route", "FAILED", True)]), 1),
    (_summ(0, [("walk", "DEGRADED", False)]), 0),              # chronic only: not counted
    (_summ(0, [("sweep", "SKIPPED", False)]), 0),
])
def test_exit_mapping(summ, code):
    assert run_daily.exit_code(summ) == code


def test_unknown_project_exits_1_now(w, monkeypatch, capsys):
    """Before W4-A run_daily printed `not in projects.json (or inactive)` and exited 2; the runner
    reads an unknown or inactive --project as a config error (exit 1), so run_daily exits 1."""
    w.register("X")
    assert main(monkeypatch, "--project", "nope") == 1
    w.projects["X"]["active"] = False
    w.write()
    assert main(monkeypatch, "--project", "X") == 1
    assert "not registered" in capsys.readouterr().err


# ================================================================ the flags
def test_project_flag_selects_one_project(w, monkeypatch):
    for k in ("X", "Y"):
        w.register(k)
        w.queue(k, DOIS)
    assert main(monkeypatch, "--project", "Y") == 0
    assert [c["project"] for c in w.calls("sweep")] == ["Y"]


def test_dry_run_writes_nothing_and_exits_0(w, monkeypatch, capsys):
    w.register("X", auto_stage=True)
    before = w.listing()
    assert main(monkeypatch, "--dry-run") == 0
    assert w.listing() == before and w.calls() == []
    assert "would seed and stage" in capsys.readouterr().out


def test_with_snowball_forces_the_walks_and_the_index_with_db_writes(w, monkeypatch, capsys):
    w.register("X")                                              # no walk_cadence_days: never walked unattended
    assert main(monkeypatch, "--with-snowball") == 0
    out = capsys.readouterr().out
    assert "[deprecated] --with-snowball" in out and "litpipe.runner" in out
    assert [c["stage"] for c in w.calls()] == ["walk", "reverse", "index"]
    assert w.calls("reverse")[0]["sources"] == "openalex,crossref,regex"
    s = w.summaries()[-1]
    assert s["db_writes"] is True and s["profile_effective"] == "daily"


def test_without_snowball_no_walk_without_a_cadence_and_no_db_writes(w, monkeypatch):
    w.register("X")
    assert main(monkeypatch) == 0
    assert w.calls() == [] and w.summaries()[-1]["db_writes"] is False


def test_max_iter_is_gone_and_snowball_is_never_run(w, monkeypatch):
    w.register("X")

    def boom(*a, **k):
        raise AssertionError("run_daily must not start snowball")
    monkeypatch.setattr(runner.subprocess, "run", boom)
    assert main(monkeypatch, "--with-snowball") == 0
    assert "snowball" not in runner.STAGE_MODULES.values()
    with pytest.raises(SystemExit):
        run_daily.main(["--max-iter", "2"])


def test_queue_data_rows_stays(tmp_path):
    q = tmp_path / "lit_pull_queue.csv"
    q.write_text("# a comment\ndoi,title\n10.1/x,T\n\n", encoding="utf-8")
    assert run_daily.queue_data_rows(q) == 2
