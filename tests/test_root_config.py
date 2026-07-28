"""Config-only projects-root resolution (2026-07-28).

Locks the behavior of the single top-level `root` key in projects.json:
  precedence  cfg["root"] -> default "~/Projects"   (config-only; NO env var)
  resolution  expanduser(); absolute -> verbatim; bare-relative -> anchored to HOME

The whole pipeline reads one anchor, lit_util.PROJECTS_ROOT, resolved once at
import. These tests pin (a) the pure resolver's branches, (b) that the path
resolvers and index_portfolio.DB_PATH honor an override, (c) import-safety when
projects.json is absent, and (d) that no module re-binds the anchor with a
`from lit_util import PROJECTS_ROOT` copy (which would defeat the override).
"""
import glob
import importlib
import os
from pathlib import Path

import lit_util


# ---------------------------------------------------------------- pure resolver
def test_resolve_root_default_when_absent():
    """No 'root' key -> the historical ~/Projects default (backward compat)."""
    assert lit_util._resolve_projects_root({}) == Path(os.path.expanduser("~/Projects"))
    # empty-string root folds to the default too (the `or "~/Projects"` guard)
    assert lit_util._resolve_projects_root({"root": ""}) == Path(os.path.expanduser("~/Projects"))


def test_resolve_root_from_config_absolute_verbatim(tmp_path):
    """An absolute 'root' is used verbatim, never re-anchored to HOME."""
    target = tmp_path / "repo"
    assert lit_util._resolve_projects_root({"root": str(target)}) == target


def test_resolve_root_bare_relative_anchored_home():
    """A bare-relative 'root' anchors to HOME -- NOT to cwd and NOT to
    PROJECTS_ROOT (sweep.resolve_loose_ends_path anchors relative values to
    PROJECTS_ROOT; these two anchors are intentionally different)."""
    assert lit_util._resolve_projects_root({"root": "Work/lit"}) == Path.home() / "Work" / "lit"


def test_resolve_root_expanduser_tilde():
    """A leading ~ expands to HOME."""
    assert lit_util._resolve_projects_root({"root": "~/Custom"}) == Path.home() / "Custom"


# ---------------------------------------------------------------- override honored downstream
def test_project_root_and_lib_paths_honor_override(tmp_path, monkeypatch):
    """project_root / lib_paths read lit_util.PROJECTS_ROOT at CALL time, so a
    monkeypatch of the single anchor redirects the whole registry."""
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", tmp_path)
    assert lit_util.project_root("Foo", {}) == tmp_path / "Foo"
    assert lit_util.project_root("Parent/Child", {"parent": "Parent"}) == tmp_path / "Parent" / "Child"
    base, lib, data = lit_util.lib_paths("K", {"lib_dir": "lit"})
    assert base == tmp_path / "K"
    assert lib == tmp_path / "K" / "lit"
    assert data is None


def test_index_db_path_honors_override(tmp_path):
    """index_portfolio.DB_PATH derives from lit_util.PROJECTS_ROOT. It is bound
    at IMPORT time, so an override needs importlib.reload (documented here). A
    finally-block reload restores the real path for other tests."""
    import index_portfolio
    original = lit_util.PROJECTS_ROOT
    try:
        lit_util.PROJECTS_ROOT = tmp_path
        importlib.reload(index_portfolio)
        assert index_portfolio.DB_PATH == tmp_path / "_references" / "portfolio.duckdb"
    finally:
        lit_util.PROJECTS_ROOT = original
        importlib.reload(index_portfolio)


# ---------------------------------------------------------------- load->resolve chain + import safety
def test_config_drives_root_end_to_end(tmp_path):
    """The real load_projects_config -> _resolve_projects_root chain the
    import-time line uses, exercised without reloading lit_util itself."""
    cfg_path = tmp_path / "projects.json"
    custom = tmp_path / "custom_root"
    cfg_path.write_text(
        '{"root": ' + '"' + str(custom).replace("\\", "\\\\") + '"' + ', "projects": {}}',
        encoding="utf-8",
    )
    loaded = lit_util.load_projects_config(cfg_path, missing_ok=True)
    assert lit_util._resolve_projects_root(loaded) == custom


def test_missing_projects_json_import_safe(tmp_path):
    """Fresh-clone / CI state: no projects.json. missing_ok yields {} and the
    resolver falls to the default, so lit_util import stays safe (it already
    imported cleanly at collection, and PROJECTS_ROOT is a real Path)."""
    loaded = lit_util.load_projects_config(tmp_path / "does-not-exist.json", missing_ok=True)
    assert loaded == {}
    assert lit_util._resolve_projects_root(loaded) == Path(os.path.expanduser("~/Projects"))
    assert isinstance(lit_util.PROJECTS_ROOT, Path)


# ---------------------------------------------------------------- lock-in guard
def test_no_from_import_of_projects_root():
    """Regression guard (constraint D): no module may re-bind the anchor with a
    name-import copy -- that snapshot would ignore the config override and defeat
    the monkeypatch the existing sweep tests rely on. Attribute access only."""
    repo_root = Path(__file__).resolve().parent.parent
    offenders = []
    for p in glob.glob(str(repo_root / "*.py")):
        for i, line in enumerate(Path(p).read_text(encoding="utf-8").splitlines(), 1):
            s = line.strip()
            # a real import statement, not a comment/docstring mention of the anti-pattern
            if s.startswith("from lit_util import") and "PROJECTS_ROOT" in s:
                offenders.append(f"{os.path.basename(p)}:{i}")
    assert not offenders, f"name-bound PROJECTS_ROOT defeats the config override: {offenders}"
