"""A1 regression: sweep.find_queues must be registry-driven and see subprojects.

Before the 2026-07-14 fix, find_queues walked PROJECTS.iterdir() (top-level dirs
only) and compared `child.name != only_project`, so a registered subproject with a
slash key ('Physiological_Data/Yitts') could never be found and its queue was
silently never swept -- a permanent no-op the orchestrator read as success.
"""
import json
import sys

import lit_util
import sweep


def _setup_registry(tmp_path, monkeypatch, projects):
    """Point lit_util.PROJECTS_ROOT + sweep.CONFIG_PATH at a temp tree + registry."""
    root = tmp_path / "Projects"
    root.mkdir()
    cfg_path = tmp_path / "projects.json"
    cfg_path.write_text(json.dumps({"projects": projects}), encoding="utf-8")
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    monkeypatch.setattr(sweep, "CONFIG_PATH", cfg_path)
    return root


def _stage_queue(root, rel):
    d = root / rel
    d.mkdir(parents=True, exist_ok=True)
    q = d / "lit_pull_queue.csv"
    q.write_text("doi\n10.1/x\n", encoding="utf-8")
    return q


def test_find_queues_sees_subproject(tmp_path, monkeypatch):
    """A registered subproject (slash key) has its queue discovered."""
    root = _setup_registry(tmp_path, monkeypatch, {
        "Parent/Child": {"parent": "Parent", "lib_dir": "Child/lit", "active": True},
    })
    q = _stage_queue(root, "Parent/Child")
    found = list(sweep.find_queues(only_project="Parent/Child"))
    assert len(found) == 1
    key, proj, queue = found[0]
    assert key == "Parent/Child"
    assert queue == q
    assert proj == root / "Parent" / "Child"


def test_find_queues_all_includes_subproject(tmp_path, monkeypatch):
    """The all-projects sweep (no --project) finds top-level AND subproject queues."""
    root = _setup_registry(tmp_path, monkeypatch, {
        "Top": {"lib_dir": "lit", "active": True},
        "Parent/Child": {"parent": "Parent", "lib_dir": "Child/lit", "active": True},
    })
    _stage_queue(root, "Top")
    _stage_queue(root, "Parent/Child")
    keys = {key for key, _, _ in sweep.find_queues()}
    assert keys == {"Top", "Parent/Child"}


def test_find_queues_skips_inactive(tmp_path, monkeypatch):
    """An inactive registered project is not swept in the all-projects path."""
    root = _setup_registry(tmp_path, monkeypatch, {
        "Live": {"lib_dir": "lit", "active": True},
        "Dead": {"lib_dir": "lit", "active": False},
    })
    _stage_queue(root, "Live")
    _stage_queue(root, "Dead")
    keys = {key for key, _, _ in sweep.find_queues()}
    assert keys == {"Live"}


def test_main_exit_1_on_explicit_project_no_queue(tmp_path, monkeypatch):
    """A1 defense-in-depth: explicit --project with no queue returns 1 so the
    orchestrator cannot read the no-op as success. (Temp registry: main() also looks for due
    retry_later rows, so it must not read the live one.)"""
    _setup_registry(tmp_path, monkeypatch, {"Nope": {"lib_dir": "lit"}})
    monkeypatch.setattr(sys, "argv", ["sweep.py", "--project", "Nope"])
    monkeypatch.setattr(sweep, "find_queues", lambda only_project=None: iter([]))
    assert sweep.main() == 1


def test_main_exit_0_on_no_project_no_queue(tmp_path, monkeypatch):
    """A bare sweep with nothing staged is a normal idle run (exit 0)."""
    _setup_registry(tmp_path, monkeypatch, {})
    monkeypatch.setattr(sys, "argv", ["sweep.py"])
    monkeypatch.setattr(sweep, "find_queues", lambda only_project=None: iter([]))
    assert sweep.main() == 0


def _stage_named(root, rel, name, header="doi,title,authors,year,destination,notes"):
    d = root / rel
    d.mkdir(parents=True, exist_ok=True)
    q = d / name
    q.write_text(f"{header}\n10.1000/x,T,A,2020,lit,\n", encoding="utf-8")
    return q


def test_find_queues_sees_tagged_queues_of_a_subproject(tmp_path, monkeypatch):
    """Dispatch 0.5: lit_pull_queue.<tag>.csv is a live queue too, found through the same
    registry resolution (subproject tail included); the untagged queue comes first."""
    root = _setup_registry(tmp_path, monkeypatch, {
        "Parent/Child": {"parent": "Parent", "lib_dir": "Child/lit", "active": True},
    })
    _stage_named(root, "Parent/Child", "lit_pull_queue.zeta.csv")
    _stage_named(root, "Parent/Child", "lit_pull_queue.alpha.csv")
    _stage_named(root, "Parent/Child", "lit_pull_queue.csv")
    names = [q.name for _, _, q in sweep.find_queues(only_project="Parent/Child")]
    assert names == ["lit_pull_queue.csv", "lit_pull_queue.alpha.csv", "lit_pull_queue.zeta.csv"]


def test_find_queues_ignores_artifacts_drafts_and_reserved_names(tmp_path, monkeypatch):
    root = _setup_registry(tmp_path, monkeypatch, {"Top": {"lib_dir": "lit"}})
    for name in ("lit_pull_queue.draft.csv", "lit_pull_queue.s09_fwd.draft.csv",
                 "lit_pull_queue.retry_later.csv", "lit_pull_queue.2026-09-30.report.csv",
                 "lit_pull_queue.rerun-2026-08-25.csv", "lit_pull_queue.4501_bodycomp_pool.csv",
                 "lit_pull_queue.bak.csv", "lit_pull_queue.Upper.csv"):
        _stage_named(root, "Top", name)
    ignored = []
    pool = _stage_named(root, "Top", "lit_pull_queue.ch15_pool.csv",
                        header="doi,title,year,venue,authors,cited_by")
    assert list(sweep.find_queues(only_project="Top", ignored=ignored)) == []
    assert ignored == [(pool, "not a queue (no destination column)")]
