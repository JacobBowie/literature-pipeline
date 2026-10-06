"""litpipe.walk's cache, gate, routing and planner as units (dispatch W3-A amendments 1, 3, 4, 11): the
replace-merge transaction, an explicit rollback on a failed statement, set-based writes only, UTC
timestamps, stub rows, the lock, the state set, and nothing created by an import, a read or --help."""
import ast
import os
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

from litpipe import s2, walk

REPO = Path(__file__).resolve().parent.parent


def rows(n, start=0):
    return [{"citing_paper_id": f"{i:040x}", "citing_doi": f"10.5555/c.{i}", "citing_title": f"T{i}",
             "citing_year": 2020, "citing_authors": "A", "citing_venue": "V", "citing_cited_by": i,
             "citing_oa": ""} for i in range(start, start + n)]


def test_replace_merge_is_one_transaction_and_keeps_stubs_in_order(tmp_path):
    c = walk.Cache(tmp_path / "c.duckdb")
    c.record("10.1/s", "s2", state="complete", count=6, paper_id="p" * 40, rows=rows(4))
    stubbed = rows(2) + [{"citing_paper_id": "", "citing_title": "stub a"}, {"citing_title": "stub b"}] + rows(1, 7)
    c.record("10.1/s", "s2", state="complete", count=5, rows=stubbed + [rows(1)[0]])      # a repeated id is dropped
    got = c.rows("10.1/s", "s2")
    assert [r["citing_title"] for r in got] == ["T0", "T1", "stub a", "stub b", "T7"]
    st = c.states()[("10.1/s", "s2")]
    assert (st["n_rows"], st["count_at_walk"], st["state"], st["paper_id"]) == (5, 5, "complete", "p" * 40)
    ids = [r[0] for r in c.con.execute("SELECT citing_id FROM citers ORDER BY pos").fetchall()]
    assert ids[2:4] == ["stub:2", "stub:3"]
    c.record("10.1/s", "s2", state="failed", count=9, kind="TRANSPORT", reason="503")          # state only
    assert len(c.rows("10.1/s", "s2")) == 5 and c.states()[("10.1/s", "s2")]["n_rows"] == 5
    assert c.states()[("10.1/s", "s2")]["state"] == "failed"
    c.close()


def test_a_failed_statement_is_rolled_back_explicitly_and_the_seed_marked_failed(tmp_path, monkeypatch):
    c = walk.Cache(tmp_path / "c.duckdb")
    c.record("10.1/s", "s2", state="complete", count=3, rows=rows(3))
    real = walk._citer_frame

    def bad(doi, source, rs):
        df = real(doi, source, rs)
        df.loc[0, "citing_id"] = None
        return df
    monkeypatch.setattr(walk, "_citer_frame", bad)
    with pytest.raises(walk.CacheWriteError, match="cache write failed"):
        c.record("10.1/s", "s2", state="complete", count=4, rows=rows(4))
    assert len(c.rows("10.1/s", "s2")) == 3                                            # the DELETE was undone
    st = c.states()[("10.1/s", "s2")]
    assert st["state"] == "failed" and st["kind"] == "CACHE" and st["n_rows"] == 3
    monkeypatch.undo()
    c.record("10.1/s", "s2", state="complete", count=4, rows=rows(4))                  # the connection still works
    assert len(c.rows("10.1/s", "s2")) == 4
    c.close()


def test_writes_are_set_based_and_timestamps_are_utc(tmp_path):
    src = (REPO / "litpipe" / "walk.py").read_text(encoding="utf-8")
    assert "SET TimeZone='UTC'" in src
    calls = [n.func.attr for n in ast.walk(ast.parse(src)) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)]
    assert "register" in calls and "executemany" not in calls
    c = walk.Cache(tmp_path / "c.duckdb")
    c.record("10.1/s", "openalex", state="empty", count=0, rows=[], walked_at="2026-10-06T03:04:05+00:00")
    assert c.con.execute("SELECT current_setting('TimeZone')").fetchone()[0] == "UTC"
    st = c.states()[("10.1/s", "openalex")]
    assert st["walked_at"].isoformat() == "2026-10-06T03:04:05+00:00" and st["rows_at"] == st["walked_at"]
    assert walk.now_utc().endswith("+00:00")
    c.close()


def test_the_cache_rejects_unknown_states_and_sources(tmp_path):
    c = walk.Cache(tmp_path / "c.duckdb")
    with pytest.raises(ValueError):
        c.record("10.1/s", "crossref", state="complete", rows=[])
    with pytest.raises(ValueError):
        c.record("10.1/s", "s2", state="truncated", rows=[])
    assert set(walk.STATES) == {str(s) for s in s2.WalkState} | {"unresolved"}
    assert not (tmp_path / "c.duckdb").exists()                                        # nothing written
    c.close()


def test_a_lock_error_on_open_is_cache_locked(tmp_path, monkeypatch):
    p = tmp_path / "c.duckdb"
    walk.Cache(p).record("10.1/s", "s2", state="empty", count=0, rows=[])

    def held(*a, **k):
        raise duckdb.IOException('IO Error: Cannot open file "c.duckdb": The process cannot access the file '
                                 'because it is being used by another process.')
    monkeypatch.setattr(duckdb, "connect", held)
    with pytest.raises(walk.CacheLocked):
        walk.Cache(p)


def test_the_gate():
    assert walk.needs_walk(5, None)
    assert not walk.needs_walk(5, {"state": "complete", "count_at_walk": 5})
    assert walk.needs_walk(6, {"state": "complete", "count_at_walk": 5})
    assert not walk.needs_walk(12000, {"state": "capped_9999", "count_at_walk": 12000, "n_rows": 9000})
    assert not walk.needs_walk(0, {"state": "empty", "count_at_walk": 0})
    for st in ("failed", "unresolved", "not_found", "elided"):
        assert walk.needs_walk(5, {"state": st, "count_at_walk": 5}), st
    assert walk.needs_walk(5, {"state": "complete", "count_at_walk": 5}, refresh=True)


def test_routing():
    r = walk.route
    assert [r(n) for n in (None, 0, 1, 1000, 1001, 9999, 10000)] == [
        None, "empty", "nested", "nested", "paged", "paged", "windows"]
    assert r(10000, oa_ok=True) == "openalex" and r(5, source="openalex") == "openalex"
    assert r(None, source="openalex") == "openalex"
    with pytest.raises(ValueError):
        r(5, source="crossref")


def test_the_planner_counts_bins_pages_windows_and_openalex():
    c = {"a": 3, "b": 4, "c": 900, "d": 1500, "e": 0, "f": None, "g": 19090}
    p = walk.plan(c)
    assert (p["metadata"], p["nested_bins"], p["pages"], p["windows"]) == (1, 1, 2, 20 + 3)
    assert p["s2_calls"] == 1 + 1 + 2 + 23 and p["empty"] == 1 and p["unwalkable"] == 1
    k = walk.plan(c, openalex_key=True)
    assert k["windows"] == 0 and (k["openalex_singletons"], k["openalex_lists"]) == (1, 191)
    o = walk.plan(c, source="openalex")
    assert o["s2_calls"] == 1 and o["openalex_singletons"] == 7
    assert walk.plan({}, n_metadata=0)["s2_calls"] == 0


def test_importing_and_help_create_no_state(tmp_path):
    env = {**os.environ, "HOME": str(tmp_path), "USERPROFILE": str(tmp_path)}
    proc = subprocess.run([sys.executable, str(REPO / "forward_citations.py"), "--help"], cwd=tmp_path, env=env,
                          capture_output=True, text=True, encoding="utf-8", timeout=120)
    assert proc.returncode == 0 and "--scope" in proc.stdout
    proc = subprocess.run([sys.executable, "-c", "import litpipe.walk, forward_citations"], cwd=REPO, env=env,
                          capture_output=True, text=True, encoding="utf-8", timeout=120)
    assert proc.returncode == 0, proc.stderr[-300:]
    assert not (tmp_path / ".local").exists()
