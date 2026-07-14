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


def test_main_exit_1_on_explicit_project_no_queue(monkeypatch):
    """A1 defense-in-depth: explicit --project with no queue returns 1 so the
    orchestrator cannot read the no-op as success."""
    monkeypatch.setattr(sys, "argv", ["sweep.py", "--project", "Nope"])
    monkeypatch.setattr(sweep, "find_queues", lambda only_project=None: iter([]))
    assert sweep.main() == 1


def test_main_exit_0_on_no_project_no_queue(monkeypatch):
    """A bare sweep with nothing staged is a normal idle run (exit 0)."""
    monkeypatch.setattr(sys, "argv", ["sweep.py"])
    monkeypatch.setattr(sweep, "find_queues", lambda only_project=None: iter([]))
    assert sweep.main() == 0
