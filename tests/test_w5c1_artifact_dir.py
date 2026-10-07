"""W5-C1 item 4 (decision B1): the projects.json `artifact_dir` key, global and per project, read by
litpipe.config.artifact_dir and by sweep, migrate and the runner (pending batches, the local canary
context). Unset: the project root, exactly as before."""
import csv
import json
import sys
import types
from pathlib import Path

import pytest

import lit_util
from litpipe import config

FIX = Path(__file__).resolve().parent / "fixtures" / "W4-A"


def reg(root, projects, **top):
    return {"root": str(root), "projects": projects, **top}


# ================================================================ config.artifact_dir
def test_unset_is_the_project_root(tmp_path, monkeypatch):
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", tmp_path)
    cfg = reg(tmp_path, {"A": {"lib_dir": "lit"}, "Par/Sub": {"parent": "Par", "lib_dir": "Sub/lit"}})
    assert config.artifact_dir("A", cfg) == lit_util.project_root("A", cfg["projects"]["A"])
    assert config.artifact_dir("Par/Sub", cfg) == lit_util.project_root("Par/Sub", cfg["projects"]["Par/Sub"])
    assert config.artifact_dir("Unregistered", cfg) == lit_util.project_root("Unregistered", {})


def test_relative_absolute_global_and_per_project(tmp_path, monkeypatch):
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", tmp_path)
    abs_dir = tmp_path / "runs_everywhere"
    own = tmp_path / "own_runs"
    projects = {"A": {"lib_dir": "lit"}, "B": {"lib_dir": "lit", "artifact_dir": "b_runs"},
                "C": {"lib_dir": "lit", "artifact_dir": str(own)},
                "Par/Sub": {"parent": "Par", "lib_dir": "Sub/lit"}}
    root = lambda k: lit_util.project_root(k, projects[k])
    rel = reg(tmp_path, projects, artifact_dir="_runs")
    assert config.artifact_dir("A", rel) == root("A") / "_runs"            # global relative: under the root
    assert config.artifact_dir("B", rel) == root("B") / "b_runs"           # the project's own wins
    assert config.artifact_dir("C", rel) == own                            # per-project absolute: as given
    ab = reg(tmp_path, projects, artifact_dir=str(abs_dir))
    assert config.artifact_dir("A", ab) == abs_dir / "A"                   # global absolute: one dir per key
    assert config.artifact_dir("Par/Sub", ab) == abs_dir / "Par/Sub"


def test_two_projects_in_one_directory_is_config(tmp_path, monkeypatch):
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", tmp_path)
    same = str(tmp_path / "shared_runs")
    cfg = reg(tmp_path, {"A": {"lib_dir": "lit", "artifact_dir": same}, "B": {"lib_dir": "lit", "artifact_dir": same}})
    with pytest.raises(config.ConfigError, match="both resolve"):
        config.artifact_dir("A", cfg)
    # a project's own directory equal to another's root
    cfg = reg(tmp_path, {"A": {"lib_dir": "lit", "artifact_dir": str(tmp_path / "B")}, "B": {"lib_dir": "lit"}})
    with pytest.raises(config.ConfigError):
        config.artifact_dir("B", cfg)
    for bad in ("", "  ", 3, ["x"]):
        with pytest.raises(config.ConfigError):
            config.artifact_dir("A", reg(tmp_path, {"A": {"lib_dir": "lit", "artifact_dir": bad}}))
        with pytest.raises(config.ConfigError):
            config.artifact_dir("A", reg(tmp_path, {"A": {"lib_dir": "lit"}}, artifact_dir=bad))


def test_the_runner_refuses_an_invalid_artifact_dir_with_exit_1(tmp_path, monkeypatch):
    sys.path.insert(0, str(FIX))
    from w4a_world import World
    from litpipe import runner
    w = World(tmp_path, monkeypatch)
    w.register("research_a", artifact_dir=7)
    assert runner.main(["run", "--profile", "every_run"]) == 1
    assert not (w.state_dir / "runner").exists()


# ================================================================ the whole run path with the key set
DOIS = ["10.5555/art.0001", "10.5555/art.0002", "10.5555/art.0003", "10.5555/art.0004"]


def _stub_stages(monkeypatch):
    import sweep

    def stage(cmd):
        cmd = [str(c) for c in cmd]
        script = Path(cmd[1]).name

        def arg(flag):
            return cmd[cmd.index(flag) + 1]
        if script == "unpaywall_fetch_v2.py":
            with open(arg("--triage"), encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            Path(arg("--report")).write_text("doi,downloaded,oa_status,error\n" + "".join(
                f"{r['doi']},{'True' if i == 0 else 'False'},{'OA' if i == 0 else 'CLOSED'},{'' if i == 0 else 'CLOSED'}\n"
                for i, r in enumerate(rows)), encoding="utf-8")
        elif script == "pmc_fetch.py":
            with open(arg("--report-in"), encoding="utf-8") as f:
                left = [r["doi"] for r in csv.DictReader(f) if r["downloaded"] != "True"]
            Path(arg("--report-out")).write_text("doi,downloaded,error,pmcid\n" + "".join(
                f"{d},False,NO_PMCID,\n" for d in left), encoding="utf-8")
        return types.SimpleNamespace(returncode=0, stdout="", stderr="")
    monkeypatch.setattr(sweep, "_run_stage", stage)


class Killed(BaseException):
    pass


def test_sweep_route_and_a_resumed_batch_keep_every_artifact_under_the_key(tmp_path, monkeypatch):
    sys.path.insert(0, str(FIX))
    from w4a_world import World
    from litpipe import runner
    w = World(tmp_path, monkeypatch)
    w.top["artifact_dir"] = "_runs"
    root = w.register("research_a")
    monkeypatch.setitem(runner.STAGE_MODULES, "sweep", "sweep")
    monkeypatch.setitem(runner.STAGE_MODULES, "route", "migrate_closed_to_md")
    _stub_stages(monkeypatch)
    pools = tmp_path / "pools"
    pools.mkdir()
    pool = pools / "topic_pool.csv"
    pool.write_text("doi,title,authors,year\n" + "".join(f"{d},T {i},Author A,2021\n" for i, d in enumerate(DOIS)),
                    encoding="utf-8")
    before = {p.name for p in root.iterdir()}
    # a nightly-style run with a staged queue, then a batch killed after its sweep and resumed
    w.queue("research_a", DOIS[:2], tag="night")
    assert runner.main(["run", "--profile", "every_run"]) == 0
    monkeypatch.setattr(runner, "KILL_HOOK", lambda n: (_ for _ in ()).throw(Killed(n)) if n == "after_sweep" else None)
    with pytest.raises(Killed):
        runner.main(["batch", "--project", "research_a", "--pool", str(pool), "--size", "2"])
    monkeypatch.setattr(runner, "KILL_HOOK", None)
    assert runner.main(["batch", "--project", "research_a", "--pool", str(pool), "--size", "2", "--batches", "1"]) == 0
    runs = root / "_runs"
    arts = sorted(p.name for p in runs.iterdir())
    for stage in ("unpaywall", "pmc", "residual", "report", "processed", "routing"):
        assert any(n.startswith("lit_pull_queue.night.") and n.endswith(f".{stage}.csv") for n in arts), stage
        assert sum(1 for n in arts if n.startswith("lit_pull_queue.b-topic_pool.") and n.endswith(f".{stage}.csv")) == 2
    # the resume found the killed batch's processed artifact under the key, routed it, swept the next one
    from litpipe import worklists
    st = worklists.Pool(pool, registry=json.loads(w.cfg_path.read_text(encoding="utf-8"))).status()
    assert (st["swept"], st["remaining"], st["pending"]) == (4, 0, 0)
    gained = {p.name for p in root.iterdir() if p.is_file()} - before
    assert gained == {"lit_pull_queue.md"}, gained                  # only the worklist (the ILL list)
    assert {p.name for p in root.iterdir() if p.is_dir()} - before == {"_runs"}
    ctx = [c for c in w.canary_calls if c["phase"] == "local"][0]["context"]["projects"][0]
    assert Path(ctx["artifact_dir"]) == runs


def test_unset_keeps_every_artifact_in_the_project_root(tmp_path, monkeypatch):
    sys.path.insert(0, str(FIX))
    from w4a_world import World
    from litpipe import runner
    w = World(tmp_path, monkeypatch)
    root = w.register("research_a")
    monkeypatch.setitem(runner.STAGE_MODULES, "sweep", "sweep")
    monkeypatch.setitem(runner.STAGE_MODULES, "route", "migrate_closed_to_md")
    _stub_stages(monkeypatch)
    w.queue("research_a", DOIS[:2])
    assert runner.main(["run", "--profile", "every_run"]) == 0
    names = {p.name for p in root.iterdir()}
    assert any(n.endswith(".routing.csv") for n in names) and any(n.endswith(".processed.csv") for n in names)
    assert "_runs" not in names
