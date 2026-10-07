"""W5-C2 step 2 (P01, P02): snowball resolves its DB and its convergence log at call time from
projects.json `db_dir` (its OWN registry), or --db; nothing touches <root>/_references when db_dir
points elsewhere. The log moves beside the DB; an old <root>/_references log is carried over by
copy and never moved or deleted. A DB_PATH/LOG_PATH a caller sets still wins (tests)."""
import json
from pathlib import Path

import duckdb
import pytest

import lit_util
import snowball

DB_TOOLS = ("index_portfolio", "enrich_recommendations", "enrich_abstracts")


@pytest.fixture
def world(tmp_path, monkeypatch):
    """A temp root and registry with db_dir set; snowball.DB_PATH/LOG_PATH left at their defaults
    (the resolution under test); every child step recorded, never run."""
    root = tmp_path / "root"
    dbdir = tmp_path / "dbhome"
    reg = tmp_path / "projects.json"
    reg.write_text(json.dumps({"db_dir": str(dbdir), "projects": {"teaching_a": {"lib_dir": "literature"}}}),
                   encoding="utf-8")
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    monkeypatch.setattr(snowball, "CONFIG_PATH", reg)
    lib = root / "teaching_a" / "literature"
    lib.mkdir(parents=True)
    (lib / "2020_Seed.pdf").write_bytes(b"%PDF")
    calls = []

    def runner(cmd, label):
        calls.append([str(c) for c in cmd])
        return snowball.StepResult(label, list(cmd), 0, {"exit_code": 0, "transport_failures": 0})

    return {"root": root, "dbdir": dbdir, "reg": reg, "calls": calls, "runner": runner}


def _make_db(path, n):
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    con.execute("CREATE TABLE candidates (doi VARCHAR, source_project VARCHAR)")
    con.execute("CREATE TABLE paper_locations (doi VARCHAR)")
    for i in range(n):
        con.execute("INSERT INTO candidates VALUES (?, 'teaching_a')", [f"10.5555/c.{i}"])
    con.close()


def test_db_dir_drives_every_step_the_count_and_the_log(world):
    _make_db(world["dbdir"] / "portfolio.duckdb", 4)
    res = snowball.run(project="teaching_a", with_recs=True, step_runner=world["runner"])
    want = str(world["dbdir"] / "portfolio.duckdb")
    assert res["exit_code"] == 0 and res["db"] == want
    writers = [c for c in world["calls"] if Path(c[1]).stem in DB_TOOLS]
    assert len(writers) == 3 and all(c[c.index("--db") + 1] == want for c in writers)
    it = res["projects"][0]["iterations"][0]
    assert it["n_before"] == 4 and it["n_after"] == 4                  # counted from the db_dir DB
    log = world["dbdir"] / "convergence_log.csv"
    assert res["log"] == str(log) and len(snowball.read_log(log)) == 1
    assert snowball.read_log() == snowball.read_log(log)
    assert not (world["root"] / "_references").exists()                # nothing touched the old home


def test_the_registry_is_snowballs_own_not_litpipe_configs(world, monkeypatch, tmp_path):
    from litpipe import config
    other = tmp_path / "other.json"
    other.write_text(json.dumps({"db_dir": str(tmp_path / "wrong")}), encoding="utf-8")
    monkeypatch.setattr(config, "CONFIG_PATH", other)
    assert snowball.db_path() == world["dbdir"] / "portfolio.duckdb"


def test_db_flag_overrides_db_dir_and_the_log_follows_it(world, tmp_path, monkeypatch):
    alt = tmp_path / "alt" / "x.duckdb"
    monkeypatch.setattr(snowball, "run_step", world["runner"])          # main's children, recorded
    monkeypatch.setattr("ris_emit.warn_if_default_email", lambda *a, **k: None)
    assert snowball.main(["--project", "teaching_a", "--skip-abstracts", "--db", str(alt)]) == 0
    idx = [c for c in world["calls"] if Path(c[1]).stem == "index_portfolio"][-1]
    assert idx[idx.index("--db") + 1] == str(alt)
    assert (alt.parent / "convergence_log.csv").exists()
    assert not (world["dbdir"] / "convergence_log.csv").exists()
    assert not (world["root"] / "_references").exists()
    assert snowball.db_path() == world["dbdir"] / "portfolio.duckdb"     # the override ended with the run


def test_unset_db_dir_keeps_the_old_home(world):
    world["reg"].write_text(json.dumps({"projects": {"teaching_a": {"lib_dir": "literature"}}}), encoding="utf-8")
    assert snowball.db_path() == world["root"] / "_references" / "portfolio.duckdb"
    assert snowball.log_path() == world["root"] / "_references" / "convergence_log.csv"


def test_an_old_log_is_carried_into_the_new_one_and_left_in_place(world):
    old = world["root"] / "_references" / "convergence_log.csv"
    old.parent.mkdir(parents=True)
    old_bytes = (b"date,project,iter,n_before,n_after,growth_pct,reason\r\n"
                 b"2026-09-01,teaching_a,1,10,12,20.00,ok; stop: single iteration\r\n")
    old.write_bytes(old_bytes)
    snowball.run(project="teaching_a", skip_abstracts=True, step_runner=world["runner"])
    new = world["dbdir"] / "convergence_log.csv"
    rows = snowball.read_log(new)
    assert [r["date"] for r in rows][0] == "2026-09-01" and len(rows) == 2
    assert old.read_bytes() == old_bytes                                # never moved or changed
    snowball.run(project="teaching_a", skip_abstracts=True, step_runner=world["runner"])
    assert len(snowball.read_log(new)) == 3                             # carried once, then appended
    assert old.read_bytes() == old_bytes


def test_a_bad_db_dir_is_exit_1_before_any_step(world):
    world["reg"].write_text(json.dumps({"db_dir": "", "projects": {"teaching_a": {"lib_dir": "literature"}}}),
                            encoding="utf-8")
    res = snowball.run(project="teaching_a", step_runner=world["runner"])
    assert res["exit_code"] == 1 and world["calls"] == []


def test_a_patched_db_path_still_wins(world, monkeypatch, tmp_path):
    monkeypatch.setattr(snowball, "DB_PATH", tmp_path / "pinned.duckdb")
    monkeypatch.setattr(snowball, "LOG_PATH", tmp_path / "pinned_log.csv")
    res = snowball.run(project="teaching_a", skip_abstracts=True, step_runner=world["runner"])
    assert res["db"] == str(tmp_path / "pinned.duckdb") and res["log"] == str(tmp_path / "pinned_log.csv")
    assert (tmp_path / "pinned_log.csv").exists()


def test_db_path_attribute_stays_importable():
    assert isinstance(snowball.DB_PATH, Path) and snowball.DB_PATH.name == "portfolio.duckdb"
    assert isinstance(snowball.LOG_PATH, Path) and snowball.LOG_PATH.name == "convergence_log.csv"
