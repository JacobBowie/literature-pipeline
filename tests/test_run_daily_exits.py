"""run_daily reads snowball's exit (W4-0; cutover checklist item 11).

snowball exits 2 when a walk is DEGRADED (a 429, a budget stop, a deferral) and closes with its
'# snowball: ...' summary line; argparse's usage error also exits 2, without that line. Before W4-0
run_daily treated every non-zero exit as fatal, so a degraded walk stopped that project's seeding and
sweeping. The stages here are real child processes (stand-in scripts in a temp dir), so these tests
exercise run_daily's own process handling, not a mock of it.
"""
import json
import sys
import types

import pytest

import run_daily

DAY = "2026-10-06"
STAGES = ("snowball.py", "seed_queue_from_top_candidates.py", "sweep.py", "migrate_closed_to_md.py")
DEGRADED_OUT = ["... done (exit 2)", "", "# snowball: 1 project(s); X DEGRADED; exit 2"]
DRAFT = ("# REVIEW before sweeping\n"
         "doi,title,authors,year,destination,notes\n"
         "10.5555/x.0001,T,A,2020,literature/,n\n")

# A stand-in stage: logs its name and argv, prints what the spec says, writes the draft, exits rc.
SCRIPT = '''\
import json, sys
from pathlib import Path
name = Path(__file__).name
with open({log!r}, "a", encoding="utf-8") as f:
    f.write(json.dumps([name] + sys.argv[1:]) + "\\n")
spec = {spec!r}.get(name, {{}})
for line in spec.get("out", []):
    print(line, flush=True)
for line in spec.get("err", []):
    print(line, file=sys.stderr, flush=True)
if spec.get("draft"):
    Path(spec["draft"]).write_text(spec["draft_text"], encoding="utf-8")
sys.exit(spec.get("rc", 0))
'''


@pytest.fixture
def stages(tmp_path, monkeypatch):
    here = tmp_path / "bin"
    here.mkdir()
    root = tmp_path / "proj"
    root.mkdir()
    log = tmp_path / "calls.jsonl"

    def install(snowball=None, seed_draft=True):
        spec = {"snowball.py": snowball or {}}
        if seed_draft:
            spec["seed_queue_from_top_candidates.py"] = {
                "out": ["Wrote 1 draft rows"], "draft": str(root / "lit_pull_queue.draft.csv"),
                "draft_text": DRAFT}
        for name in STAGES:
            (here / name).write_text(SCRIPT.format(log=str(log), spec=spec), encoding="utf-8")

    def calls():
        if not log.exists():
            return []
        return [json.loads(line)[0] for line in log.read_text(encoding="utf-8").splitlines()]

    monkeypatch.setattr(run_daily, "HERE", here)
    monkeypatch.setattr(run_daily, "project_dir", lambda p, cfg: root)
    return types.SimpleNamespace(install=install, root=root, calls=calls)


AUTO = {"X": {"auto_stage": True}}


def test_degraded_walk_still_seeds_stages_and_sweeps(stages, capsys):
    stages.install(snowball={"out": DEGRADED_OUT, "rc": 2})
    res = run_daily.pipeline_one("X", AUTO, True, False, DAY)
    assert stages.calls() == list(STAGES)                # before W4-0: ["snowball.py"] (StepError, project aborted)
    assert res == run_daily.DEGRADED
    assert (stages.root / "lit_pull_queue.csv").exists()
    out = capsys.readouterr().out
    assert "# snowball: 1 project(s); X DEGRADED; exit 2" in out   # the walk's output is streamed
    assert "[DEGRADED]" in out


def test_degraded_walk_with_auto_stage_off_still_sweeps_what_is_staged(stages):
    stages.install(snowball={"out": DEGRADED_OUT, "rc": 2}, seed_draft=False)
    (stages.root / "lit_pull_queue.teach.csv").write_text(
        "doi,title,authors,year,destination,notes\n10.5555/x.0002,T,A,2020,literature/,n\n",
        encoding="utf-8")
    res = run_daily.pipeline_one("X", {"X": {}}, True, False, DAY)
    assert stages.calls() == ["snowball.py", "sweep.py", "migrate_closed_to_md.py"]
    assert res == run_daily.DEGRADED


def test_usage_error_exit_2_without_the_summary_line_is_fatal(stages):
    stages.install(snowball={"err": ["usage: snowball.py [-h] [--project PROJECT]",
                                     "snowball.py: error: unrecognized arguments: --max-iter 2"], "rc": 2})
    assert run_daily.pipeline_one("X", AUTO, True, False, DAY) is False
    assert stages.calls() == ["snowball.py"]             # nothing seeded, nothing swept


def test_failed_walk_exit_1_is_fatal(stages):
    stages.install(snowball={"out": ["", "# snowball: 1 project(s); X FAILED; exit 1"], "rc": 1})
    assert run_daily.pipeline_one("X", AUTO, True, False, DAY) is False
    assert stages.calls() == ["snowball.py"]


def test_clean_walk_runs_the_chain(stages):
    stages.install(snowball={"out": ["", "# snowball: 1 project(s); X ok; exit 0"], "rc": 0})
    assert run_daily.pipeline_one("X", AUTO, True, False, DAY) is True
    assert stages.calls() == list(STAGES)


@pytest.mark.parametrize("results,code,line", [
    ({"A": True, "B": True}, 0, None),
    ({"A": "degraded", "B": True}, 2, "# Degraded walk (seeded and swept anyway): A"),
    ({"A": "degraded", "B": False}, 1, "# Failed: B"),
])
def test_main_exit_code(monkeypatch, capsys, results, code, line):
    monkeypatch.setattr(run_daily, "load_projects", lambda: {k: {} for k in results})
    monkeypatch.setattr(run_daily, "pipeline_one",
                        lambda proj, *a, **k: (getattr(run_daily, "DEGRADED", "degraded")
                                               if results[proj] == "degraded" else results[proj]))
    monkeypatch.setattr(sys, "argv", ["run_daily.py"])
    assert run_daily.main() == code                       # before W4-0: a degraded project read as ok, exit 0
    out = capsys.readouterr().out
    if line:
        assert line in out
