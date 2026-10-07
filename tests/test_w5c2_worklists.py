"""W5-C2 step 6: worklists (L-1 of the issue review).

- The OA-blocked worklist renders the row's own link: a `best_oa_url` link migrate writes (W5-C1
  carries it into the `lit_pull_queue.oa_blocked.md` line) is the one-click link (a lock, exempt from
  fail-before: oa_blocked already read the row's own link).
- residual_csvs and the canaries' no-context fallback find sweep's residual CSVs in the project's
  artifact directory (litpipe.config.artifact_dir, W5-C1's; called through getattr) as well as the
  project root and its direct subdirectories; both branches (with and without artifact_dir).
- litpipe.doi.PATH_SAFE is public (C093); `_PATH_SAFE` stays as an alias.
- C178: Pool never rewrites the pool CSV.
"""
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

import lit_util
from litpipe import canaries, config, holdings
from litpipe import doi as D
from litpipe import worklists as WL

BEST = "https://repository.example.edu/bitstream/123/aam.pdf"
RESIDUAL_FIELDS = "doi,title,year,residual_class,reason\n"


# ------------------------------------------------------------------ the rendered link
def test_a_best_oa_url_link_is_the_one_rendered(tmp_path, monkeypatch):
    root = tmp_path / "Projects"
    proj = root / "teaching_a"
    proj.mkdir(parents=True)
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    (proj / WL.OA_BLOCKED_NAME).write_text(
        "# OA-blocked\n\n## Sweep 2026-10-07, 1 row\n\n"
        f"- [ ] **A hybrid paper** (2021) [10.1016/j.x.2021.01.002]({BEST}) cause `HTTP_403` via `unpaywall:publisher`\n",
        encoding="utf-8")
    rows = WL.oa_blocked({"projects": {"teaching_a": {"lib_dir": "literature"}}})
    assert rows[0]["link"] == BEST and rows[0]["parsed"]
    text = WL.render_oa_worklist(WL.group_by_host(rows), "2026-10-07")
    assert f"]({BEST})" in text and "doi.org/10.1016" not in text


# ------------------------------------------------------------------ residual CSVs and artifact_dir
def _fake_artifact_dir(base):
    """W5-C1's contract: unset -> the project root; a global absolute value -> <it>/<project key>;
    a per-project absolute value as given; an unregistered key -> its project root."""
    def artifact_dir(key, cfg=None):
        reg = cfg if cfg is not None else config.load()
        p = (reg.get("projects") or {}).get(key)
        if p is None:
            return lit_util.PROJECTS_ROOT / key
        if p.get("artifact_dir"):
            return Path(p["artifact_dir"])
        if reg.get("artifact_dir"):
            return Path(reg["artifact_dir"]) / key
        return lit_util.project_root(key, p)
    return artifact_dir


@pytest.fixture
def portfolio(tmp_path, monkeypatch):
    root = tmp_path / "Projects"
    for k in ("teaching_a", "research_b"):
        (root / k / "literature").mkdir(parents=True)
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    art = tmp_path / "artifacts"
    reg = {"state_dir": str(tmp_path / "state"), "artifact_dir": str(art),
           "projects": {"teaching_a": {"lib_dir": "literature"}, "research_b": {"lib_dir": "literature"}}}
    (art / "teaching_a").mkdir(parents=True)
    (art / "teaching_a" / "lit_pull_queue.2026-10-07.residual.csv").write_text(
        RESIDUAL_FIELDS + "10.5555/a.0001,A,2020,TERMINAL_CLOSED,closed\n", encoding="utf-8")
    (root / "research_b" / "lit_pull_queue.2026-10-07.residual.csv").write_text(
        RESIDUAL_FIELDS + "10.5555/b.0001,B,2020,TERMINAL_CLOSED,closed\n", encoding="utf-8")
    return {"root": root, "art": art, "reg": reg}


def test_residual_csvs_reads_the_artifact_dir_through_config(portfolio, monkeypatch):
    monkeypatch.setattr(config, "artifact_dir", _fake_artifact_dir(portfolio["art"]), raising=False)
    found = WL.residual_csvs(portfolio["reg"])
    assert sorted((k, str(p)) for k, p in found) == sorted([
        ("teaching_a", str(portfolio["art"] / "teaching_a" / "lit_pull_queue.2026-10-07.residual.csv")),
        ("research_b", str(portfolio["root"] / "research_b" / "lit_pull_queue.2026-10-07.residual.csv"))])
    rows, stats = WL.read_residuals(found)
    assert sorted(r["doi"] for _k, r in rows) == ["10.5555/a.0001", "10.5555/b.0001"]
    # a bare projects mapping is wrapped as {"projects": mapping}
    bare = WL.residual_csvs(portfolio["reg"]["projects"])
    assert {k for k, _p in bare} == {"research_b"}        # no global artifact_dir in a bare mapping


def test_without_artifact_dir_only_the_root_and_its_subdirectories_are_read(portfolio, monkeypatch):
    monkeypatch.delattr(config, "artifact_dir", raising=False)
    found = WL.residual_csvs(portfolio["reg"])
    assert [k for k, _p in found] == ["research_b"]


def test_a_bad_artifact_dir_value_is_a_worklist_error(portfolio, monkeypatch):
    def bad(key, cfg=None):
        raise config.ConfigError("artifact_dir must be a path string")
    monkeypatch.setattr(config, "artifact_dir", bad, raising=False)
    with pytest.raises(WL.WorklistError, match="artifact_dir"):
        WL.residual_csvs(portfolio["reg"])


def _local_projects(reg):
    now = datetime(2026, 10, 7, 12, 0, tzinfo=timezone.utc)
    rctx = canaries._RunCtx(context=None, state=None, request=None, cfg=reg, now=now)
    return {p.key: p for p in canaries._LocalCtx(rctx).projects}


def test_the_canaries_fallback_reads_the_artifact_dir(portfolio, monkeypatch):
    monkeypatch.setattr(config, "artifact_dir", _fake_artifact_dir(portfolio["art"]), raising=False)
    projects = _local_projects(portfolio["reg"])
    ta = projects["teaching_a"]
    assert portfolio["art"] / "teaching_a" in ta.dirs
    assert ta.run_ids == ["2026-10-07"]                     # its artifact was found there
    assert projects["research_b"].run_ids == ["2026-10-07"]


def test_the_canaries_fallback_without_artifact_dir(portfolio, monkeypatch):
    monkeypatch.delattr(config, "artifact_dir", raising=False)
    projects = _local_projects(portfolio["reg"])
    assert projects["teaching_a"].run_ids == [] and projects["research_b"].run_ids == ["2026-10-07"]


# ------------------------------------------------------------------ the public encode set (C093)
def test_path_safe_is_public_and_the_private_name_stays():
    assert D.PATH_SAFE == "!$&'()*+;=:@" and D._PATH_SAFE is D.PATH_SAFE
    assert "PATH_SAFE" in D.__all__
    letter_tail = "10.1088/2053-1591/acdecd"
    assert WL.doi_link(letter_tail) == "https://doi.org/10.1088/2053-1591/acdecd"


def test_worklists_no_longer_reads_the_private_name():
    src = Path(WL.__file__).read_text(encoding="utf-8")
    assert "_doi._PATH_SAFE" not in src


# ------------------------------------------------------------------ C178: the pool CSV is never rewritten
def test_pool_never_rewrites_the_pool_csv(tmp_path):
    pool = tmp_path / "lit_pull_queue.teaching_pool.csv"
    pool.write_bytes(b"doi,title,notes\r\n10.5555/p.0001,One,n1\r\n10.5555/p.0002,Two,n2\r\n"
                     b"10.5555/p.0003,Three,n3\r\n10.5555/p.0004,Four,n4\r\n")
    before = hashlib.sha256(pool.read_bytes()).hexdigest()
    p = WL.Pool(pool, holdings=holdings.HoldMap())
    batch = p.next_batch(2)
    p.mark_staged([r["doi"] for r in batch], "2026-10-07", str(tmp_path / "lit_pull_queue.b1.csv"))
    p.mark_swept([r["doi"] for r in batch], "2026-10-07",
                 {batch[0]["doi"]: "fetched", batch[1]["doi"]: "TERMINAL_CLOSED"})
    state = tmp_path / "staged.json"
    state.write_text(json.dumps({"staged_dois": ["10.5555/p.0003"]}), encoding="utf-8")
    p.seed_from(state)
    p.status()
    assert hashlib.sha256(pool.read_bytes()).hexdigest() == before
    assert p.state_path.exists() and p.state_path != pool
