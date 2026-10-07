"""sweep's candidate_order pass-through (W4b amendment 9, DEC-11): sweep.run(..., candidate_order)
and `--candidate-order repository|publisher` reach the Unpaywall stage command only when given.
The stages are never run: sweep._run_stage is replaced by a recorder."""
import json
import types

import pytest

import lit_util
import sweep

DOI = "10.5555/order.0001"


@pytest.fixture
def world(tmp_path, monkeypatch):
    root = tmp_path / "Projects"
    lib = root / "research_a" / "literature"
    lib.mkdir(parents=True)
    reg = tmp_path / "projects.json"
    reg.write_text(json.dumps({"state_dir": str(tmp_path / "state"), "root": str(root),
                               "projects": {"research_a": {"lib_dir": "literature"}}}), encoding="utf-8")
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    monkeypatch.setattr(sweep, "CONFIG_PATH", reg)
    monkeypatch.setattr("litpipe.config.CONFIG_PATH", reg)
    monkeypatch.setattr(sweep, "_load_holdings", lambda registry: (None, "test"))
    (root / "research_a" / "lit_pull_queue.csv").write_text(
        f"doi,title,authors,year,destination,notes\n{DOI},T,A B,2020,literature/,n\n", encoding="utf-8")
    cmds = []

    def stage(cmd):
        cmds.append([str(c) for c in cmd])
        return types.SimpleNamespace(returncode=1, stdout="", stderr="stub: not run")
    monkeypatch.setattr(sweep, "_run_stage", stage)
    return types.SimpleNamespace(cmds=cmds, root=root)


def unpaywall_cmd(world):
    return next(c for c in world.cmds if c[1].endswith("unpaywall_fetch_v2.py"))


@pytest.mark.parametrize("order", ["repository", "publisher"])
def test_run_passes_the_order_to_the_unpaywall_stage(world, order):
    sweep.run(project="research_a", date="2026-10-07", loose_ends=False, candidate_order=order)
    c = unpaywall_cmd(world)
    assert c[c.index("--candidate-order") + 1] == order


def test_no_order_means_no_flag(world):
    sweep.run(project="research_a", date="2026-10-07", loose_ends=False)
    assert "--candidate-order" not in unpaywall_cmd(world)


def test_the_cli_flag_reaches_the_stage(world):
    sweep.main(["--project", "research_a", "--date", "2026-10-07", "--no-loose-ends", "--candidate-order",
                "publisher"])
    c = unpaywall_cmd(world)
    assert c[c.index("--candidate-order") + 1] == "publisher"


def test_a_bad_order_is_a_usage_error(world, capsys):
    with pytest.raises(SystemExit) as e:
        sweep.main(["--project", "research_a", "--candidate-order", "random"])
    assert e.value.code == 2
    assert sweep.run(project="research_a", date="2026-10-07", candidate_order="random")["exit_code"] == sweep.EXIT_USAGE
    assert world.cmds == []


def test_only_the_unpaywall_stage_gets_it(world, monkeypatch):
    sweep.run(project="research_a", date="2026-10-07", loose_ends=False, candidate_order="publisher")
    assert [c for c in world.cmds if "--candidate-order" in c] == [unpaywall_cmd(world)]


def test_the_orders_match_the_unpaywall_stage():
    import unpaywall_fetch_v2
    assert tuple(sweep.CANDIDATE_ORDERS) == tuple(unpaywall_fetch_v2.CANDIDATE_ORDERS)
