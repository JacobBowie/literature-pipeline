"""Phase-3 OPSEC + simplicity (2026-07-27): the cross-project lit-pull log path lives
in the gitignored projects.json "loose_ends" key -- never hardcoded in this public
source. One config surface. An absent key is a VISIBLE skip (the log is opt-in), not
a silent default: resolve returns None, append writes nothing, and main() prints one
actionable notice. The literature-pipeline skill/SOP documents wiring it.

resolve_loose_ends_path(): projects.json "loose_ends" -> Path (relative to
lit_util.PROJECTS_ROOT, or absolute); None when unset.
"""
import json
from pathlib import Path

import pytest

import lit_util
import sweep


def _point_config(tmp_path, monkeypatch, cfg):
    """Point sweep.CONFIG_PATH at a temp projects.json and PROJECTS_ROOT at a temp
    tree, so resolution is hermetic (independent of the developer's real config)."""
    root = tmp_path / "Projects"
    root.mkdir()
    cfg_path = tmp_path / "projects.json"
    cfg_path.write_text(json.dumps(cfg), encoding="utf-8")
    monkeypatch.setattr(sweep, "CONFIG_PATH", cfg_path)
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    return root


def test_config_key_relative_to_projects_root(tmp_path, monkeypatch):
    """A relative "loose_ends" key resolves under PROJECTS_ROOT -- the same anchor
    the rest of the pipeline uses, so there is one notion of the projects root."""
    root = _point_config(tmp_path, monkeypatch, {"loose_ends": "Ops/files/LOOSE_ENDS.md", "projects": {}})
    assert sweep.resolve_loose_ends_path() == root / "Ops" / "files" / "LOOSE_ENDS.md"


def test_config_key_absolute_preserved(tmp_path, monkeypatch):
    """An absolute "loose_ends" key is used verbatim, not re-anchored."""
    abs_target = tmp_path / "abs" / "LOOSE_ENDS.md"
    _point_config(tmp_path, monkeypatch, {"loose_ends": str(abs_target), "projects": {}})
    assert sweep.resolve_loose_ends_path() == abs_target


def test_unconfigured_key_returns_none(tmp_path, monkeypatch):
    """No "loose_ends" key -> None (opt-in feature off; no invented default)."""
    _point_config(tmp_path, monkeypatch, {"projects": {}})
    assert sweep.resolve_loose_ends_path() is None


def test_missing_projects_json_returns_none(tmp_path, monkeypatch):
    """Defensive: resolve never crashes even if projects.json is absent (a state the
    pipeline hard-errors on upstream); it degrades to the opt-in-off None."""
    root = tmp_path / "Projects"
    root.mkdir()
    monkeypatch.setattr(sweep, "CONFIG_PATH", tmp_path / "does-not-exist.json")
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    assert sweep.resolve_loose_ends_path() is None


def test_append_writes_and_returns_path_creating_parents(tmp_path, monkeypatch):
    """Configured: append writes the line, mkdir-s missing parents, returns the path
    (which main() prints, so the real destination is visible every run)."""
    root = _point_config(tmp_path, monkeypatch, {"loose_ends": "deep/nested/LOOSE_ENDS.md", "projects": {}})
    dest = sweep.append_loose_end("✅ test line")
    assert dest == root / "deep" / "nested" / "LOOSE_ENDS.md"
    assert dest.read_text(encoding="utf-8").strip() == "✅ test line"


def test_append_noop_when_unconfigured(tmp_path, monkeypatch):
    """Unconfigured: append writes nothing and returns None -- no surprise file."""
    root = _point_config(tmp_path, monkeypatch, {"projects": {}})
    assert sweep.append_loose_end("✅ nope") is None
    assert not list(root.rglob("LOOSE_ENDS.md"))


def test_public_source_has_no_hardcoded_internal_loose_ends_name():
    """OPSEC lock-in: the internal project name -- DERIVED at runtime from the
    gitignored projects.json "loose_ends" key so this PUBLIC test file never spells
    it out -- must not appear in the public sweep.py source. Skips on a fresh clone /
    CI with no local config, and when the key is an absolute path (no name to derive).
    Direct regression guard against a future rename commit re-hardcoding the path."""
    cfg = lit_util.load_projects_config(sweep.CONFIG_PATH, missing_ok=True)
    configured = cfg.get("loose_ends")
    if not configured:
        pytest.skip("no local projects.json loose_ends key to derive a name from")
    p = Path(configured)
    if p.is_absolute():
        pytest.skip("configured loose_ends is absolute; no project name to derive")
    internal_name = p.parts[0]  # "<Project>/files/LOOSE_ENDS.md" -> "<Project>"
    src = Path(sweep.__file__).read_text(encoding="utf-8")
    assert internal_name not in src


# ---------------------------------------------------------------- W1-D1: one line per state
def _result(retired, report="lit_pull_queue.2026-09-30.report.csv", **kw):
    r = {"retired": retired, "queue": "lit_pull_queue.csv", "report": report,
         "keep_reason": "" if retired else "pmc failed", "downloaded": 3, "rows": 10,
         "unpaywall": 2, "pmc": 1, "preprint": 0, "skip_exists": 0,
         "classes": {"fetched": 3, "TERMINAL_CLOSED": 7}, "stages": {"preprint": "skipped"}}
    r.update(kw)
    return r


def test_the_same_state_is_never_written_twice_in_a_row(tmp_path, monkeypatch):
    """19 identical PARTIAL lines on 2026-09-29: the report name changes every run, so the
    comparison is on the state part of the line, per project, ignoring other projects' lines."""
    _point_config(tmp_path, monkeypatch, {"loose_ends": "LE.md", "projects": {}})
    a = sweep.loose_end_line("P", [_result(False)], [])
    b = sweep.loose_end_line("P", [_result(False, report="lit_pull_queue.2026-09-30.2.report.csv")], [])
    assert a.startswith("⏸️ Lit pull PARTIAL: P/ — ") and a != b
    assert sweep.write_loose_end("P", a)[1] == "written"
    assert sweep.write_loose_end("Q", sweep.loose_end_line("Q", [_result(False)], []))[1] == "written"
    assert sweep.write_loose_end("P", b) == (None, "unchanged")
    done = sweep.loose_end_line("P", [_result(True)], [])
    assert sweep.write_loose_end("P", done)[1] == "written"
    assert sweep.write_loose_end("P", a)[1] == "written"      # a state change back is logged
    assert len(sweep.resolve_loose_ends_path().read_text(encoding="utf-8").splitlines()) == 4


def test_done_only_when_a_queue_retired_and_none_stayed():
    done = sweep.loose_end_line("P", [_result(True)], [])
    assert done.startswith("✅ Lit pull done: P/ — 3/10 fetched (Unpaywall 2, PMC 1, Preprint 0)")
    assert "7 closed" in done and "preprint skipped" in done
    assert sweep.loose_end_line("P", [_result(True), _result(False)], []).startswith("⏸️")
    assert sweep.loose_end_line("P", [_result(True)], ["lit_pull_queue.x.csv"]).startswith("⏸️")
    assert sweep.loose_end_line("P", [], []) is None
