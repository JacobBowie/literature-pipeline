"""W3-C1: the index CLI in-process: index_runs freshness, exit codes, the DB path, --rename-project.

- Each run appends exactly one index_runs row per project it indexed (finished_at an aware UTC
  instant); a skipped or rolled-back project gets none; audit_portfolio.index_status reads the
  fresh index as count_differs False and stale None.
- A skip (an active project's library missing, a file that could not be read) prints the
  "[step-summary]" line last and exits 2; an inactive project is not a skip; a missing registry,
  an unknown --project or an unusable entry exits 1.
- The DB path comes from projects.json db_dir at call time (--db overrides) and is printed.
- --rename-project OLD NEW is a dry run unless --execute, then one transaction that leaves 0 rows
  under OLD in every project-bearing column.
"""
import datetime
import json

import duckdb
import pytest

import audit_portfolio as AP
import index_portfolio as I
import lit_util
import snowball


def ris(doi):
    return f"TY  - JOUR\nAU  - Smith, Jane\nPY  - 2019\nTI  - Title\nDO  - {doi}\nER  - \n"


def make_lib(root, key, dois, text_only=(), sub="literature"):
    lib = root / key / sub
    lib.mkdir(parents=True, exist_ok=True)
    for i, d in enumerate(dois):
        (lib / f"2019_Paper{i:03d}.pdf").write_bytes(b"%PDF-1.4 stub")
        (lib / f"2019_Paper{i:03d}.ris").write_text(ris(d), encoding="utf-8")
    for i, d in enumerate(text_only):
        (lib / f"2015_Text{i:03d}.fulltext.json").write_text(json.dumps(
            {"doi": d, "title": "T", "year": "2015", "journal": "J", "authors": ["Lee Kim"], "text": "body " * 30}),
            encoding="utf-8")
    return lib


@pytest.fixture
def env(tmp_path, monkeypatch, capsys):
    root = tmp_path / "root"
    root.mkdir()
    cfg = tmp_path / "projects.json"
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    monkeypatch.setattr(I, "CONFIG_PATH", cfg)
    db = tmp_path / "refs" / "portfolio.duckdb"

    class Env:
        pass
    e = Env()
    e.root, e.cfg, e.db, e.tmp = root, cfg, db, tmp_path

    def registry(projects, **top):
        cfg.write_text(json.dumps({"state_dir": str(tmp_path / "state"), "projects": projects, **top}),
                       encoding="utf-8")

    def index(*argv, db_flag=True):
        args = (["--db", str(db)] if db_flag else []) + list(argv)
        rc = I.main(args)
        out = capsys.readouterr().out
        return rc, out

    def q(sql, params=(), path=None):
        con = duckdb.connect(str(path or db), read_only=True)
        try:
            return con.execute(sql, list(params)).fetchall()
        finally:
            con.close()
    e.registry, e.index, e.q = registry, index, q
    return e


def summary_of(out):
    last = out.rstrip("\n").splitlines()[-1]
    assert last.startswith(I.SUMMARY_MARKER), last
    return json.loads(last[len(I.SUMMARY_MARKER):])


# ---------------------------------------------------------------- index_runs and freshness
def test_each_run_appends_one_aware_row_per_indexed_project(env):
    env.registry({"research_a": {"lib_dir": "literature"}, "teaching_b": {"lib_dir": "literature"},
                  "research_old": {"lib_dir": "literature", "active": False}})
    make_lib(env.root, "research_a", ["10.5555/a.0001", "10.5555/a.0002"], text_only=["10.5555/a.0003"])
    lib_b = make_lib(env.root, "teaching_b", ["10.5555/b.0001"])
    (lib_b / "2019_NoDoi.pdf").write_bytes(b"%PDF-1.4 stub")
    t0 = datetime.datetime.now(datetime.timezone.utc)
    rc, out = env.index()
    assert rc == 0, out
    rc, out = env.index()
    assert rc == 0
    rc, out = env.index("--project", "teaching_b")
    assert rc == 0
    t1 = datetime.datetime.now(datetime.timezone.utc)
    rows = env.q("SELECT project, finished_at, n_files, db_path FROM index_runs ORDER BY finished_at")
    assert [r[0] for r in rows].count("research_a") == 2 and [r[0] for r in rows].count("teaching_b") == 3
    assert len(rows) == 5                                     # nothing for the inactive project
    for project, stamp, n_files, dbp in rows:
        assert stamp.tzinfo is not None and t0 <= stamp <= t1    # an aware instant, not naive local
        assert n_files == {"research_a": 3, "teaching_b": 2}[project]
        assert dbp == str(env.db)
    assert env.q("SELECT typeof(finished_at) FROM index_runs LIMIT 1") == [("TIMESTAMP WITH TIME ZONE",)]


def test_index_status_reads_a_fresh_index(env):
    env.registry({"research_a": {"lib_dir": "literature"}})
    lib = make_lib(env.root, "research_a", ["10.5555/a.0001"], text_only=["10.5555/a.0002"])
    (lib / "2019_Flag.pdf").write_bytes(b"%PDF-1.4 stub")
    (lib / "2019_Flag.identity.json").write_text(json.dumps({"identity": "FLAG"}), encoding="utf-8")
    assert env.index()[0] == 0
    st = AP.index_status(env.db, [("research_a", AP.scan_library(lib))])
    p = st["projects"]["research_a"]
    assert st["has_index_runs"] and p["source"] == "index_runs"
    assert p["count_differs"] is False and p["stale"] is None, p
    assert (p["n_index"], p["n_disk"], p["db_text_rows"], p["disk_text_only"]) == (3, 3, 1, 1)
    assert p["not_indexed"] == [] and p["stale_rows"] == []


def test_skipped_project_exits_2_after_the_summary_and_gets_no_row(env):
    env.registry({"research_a": {"lib_dir": "literature"}, "research_gone": {"lib_dir": "literature"},
                  "research_off": {"lib_dir": "literature", "active": False}})
    make_lib(env.root, "research_a", ["10.5555/a.0001"])
    rc, out = env.index()
    assert rc == 2
    s = summary_of(out)
    assert s["exit_code"] == 2 and s["status"] == "degraded" and s["aborted"] is None
    assert s["transport_failures"] == 0 and len(s["reasons"]) == 1 and "research_gone" in s["reasons"][0]
    assert s["indexed"] == ["research_a"]
    assert snowball.degraded_exit(rc, snowball._summary(out.rstrip().splitlines()[-1]))   # DEGRADED, not FAILED
    assert env.q("SELECT project FROM index_runs") == [("research_a",)]


def test_inactive_project_is_not_a_skip(env):
    env.registry({"research_a": {"lib_dir": "literature"}, "research_off": {"lib_dir": "literature", "active": False}})
    make_lib(env.root, "research_a", ["10.5555/a.0001"])
    rc, out = env.index()
    assert rc == 0 and summary_of(out)["reasons"] == []
    rc, out = env.index("--project", "research_off")
    assert rc == 0 and "[inactive] research_off" in out


def test_unreadable_file_exits_2_but_the_project_is_indexed(env):
    env.registry({"research_a": {"lib_dir": "literature"}})
    lib = make_lib(env.root, "research_a", ["10.5555/a.0001"])
    (lib / "2019_Paper000.fulltext.json").write_text("{broken", encoding="utf-8")
    (lib / "_reverse_citations_parsed.csv").write_bytes(b"seed,doi\n\xff\xfe\x00bad")    # not UTF-8
    rc, out = env.index()
    s = summary_of(out)
    assert rc == 2 and s["unreadable"] == 2
    assert any("2019_Paper000.fulltext.json" in r for r in s["reasons"])
    assert any("_reverse_citations_parsed.csv" in r for r in s["reasons"])
    assert env.q("SELECT project, n_files FROM index_runs") == [("research_a", 1)]


def test_rolled_back_project_gets_no_row(env, monkeypatch):
    env.registry({"research_a": {"lib_dir": "literature"}, "research_b": {"lib_dir": "literature"}})
    make_lib(env.root, "research_a", ["10.5555/a.0001"])
    lib_b = make_lib(env.root, "research_b", ["10.5555/b.0001"])
    (lib_b / "_forward_citations.csv").write_text("seed_doi,citing_doi\n10.5555/b.0001,10.5555/c.0001\n",
                                                  encoding="utf-8")
    real = I.ingest_forward

    def boom(con, name, path, lib=None):
        if name == "research_b":
            real(con, name, path, lib)
            raise RuntimeError("simulated lock mid-project")
        return real(con, name, path, lib)
    monkeypatch.setattr(I, "ingest_forward", boom)
    with pytest.raises(RuntimeError):
        env.index()
    assert env.q("SELECT project FROM index_runs") == [("research_a",)]
    assert env.q("SELECT COUNT(*) FROM candidates") == [(0,)]          # research_b's writes rolled back
    assert env.q("SELECT project FROM paper_locations") == [("research_a",)]


def test_index_runs_row_is_inside_the_project_transaction(env, monkeypatch):
    """A failure after the row is written (the last step before COMMIT) leaves no row and no data."""
    env.registry({"research_a": {"lib_dir": "literature"}})
    make_lib(env.root, "research_a", ["10.5555/a.0001"])
    real = I.record_index_run

    def write_then_fail(con, project, n_files, db_path):
        real(con, project, n_files, db_path)
        raise RuntimeError("simulated failure at commit time")
    monkeypatch.setattr(I, "record_index_run", write_then_fail)
    with pytest.raises(RuntimeError):
        env.index()
    assert env.q("SELECT COUNT(*) FROM index_runs") == [(0,)]
    assert env.q("SELECT COUNT(*) FROM paper_locations") == [(0,)]


def test_missing_registry_exits_1_and_creates_nothing(env):
    assert not env.cfg.exists()
    rc, out = env.index()
    assert rc == 1 and summary_of(out)["exit_code"] == 1
    assert not env.db.exists() and not env.db.parent.exists()


def test_unknown_project_and_unusable_entry_exit_1(env):
    env.registry({"research_a": {"lib_dir": "literature"}})
    rc, out = env.index("--project", "research_zz")
    assert rc == 1 and "research_zz" in summary_of(out)["reasons"][0]
    env.registry({"research_a": {"lib_dir": "literature"}, "research_bad": {"active": True}})
    rc, out = env.index()
    assert rc == 1 and "research_bad" in summary_of(out)["reasons"][0]
    assert not env.db.exists()


def test_db_path_comes_from_db_dir_and_is_printed(env):
    dbdir = env.tmp / "elsewhere" / "db"
    env.registry({"research_a": {"lib_dir": "literature"}}, db_dir=str(dbdir))
    make_lib(env.root, "research_a", ["10.5555/a.0001"])
    rc, out = env.index(db_flag=False)
    want = dbdir / "portfolio.duckdb"
    assert rc == 0 and want.exists()
    assert out.splitlines()[0] == f"DB: {want}"
    assert summary_of(out)["db"] == str(want)
    assert not (env.root / "_references").exists()
    # --db overrides it
    rc, out = env.index()
    assert rc == 0 and out.splitlines()[0] == f"DB: {env.db}" and env.db.exists()


def test_default_db_without_db_dir_is_root_references(env):
    env.registry({"research_a": {"lib_dir": "literature"}})
    assert I.default_db_path() == env.root / "_references" / "portfolio.duckdb"


# ---------------------------------------------------------------- --rename-project
def _rename_fixture(env):
    env.registry({"teaching_old": {"lib_dir": "literature"}, "teaching_new": {"lib_dir": "literature"}})
    lib_old = make_lib(env.root, "teaching_old", ["10.5555/r.0001", "10.5555/r.0002"], text_only=["10.5555/r.0003"])
    (lib_old / "2019_NoDoi.pdf").write_bytes(b"%PDF-1.4 stub")
    (lib_old / "_forward_citations.csv").write_text(
        "seed_doi,citing_doi,citing_cited_by\n10.5555/r.0001,10.5555/c.0001,4\n10.5555/r.0002,10.5555/c.0002,2\n",
        encoding="utf-8")
    (lib_old / "_resp_forward_citations.csv").write_text(
        "seed_doi,seed_label,seed_chapter,citing_doi\n10.5555/r.0001,L,Ch1,10.5555/s.0001\n", encoding="utf-8")
    # the new key holds a subset (the folder was copied before the last two papers arrived)
    lib_new = make_lib(env.root, "teaching_new", ["10.5555/r.0001"])
    (lib_new / "_forward_citations.csv").write_text(
        "seed_doi,citing_doi,citing_cited_by\n10.5555/r.0001,10.5555/c.0001,4\n", encoding="utf-8")
    assert env.index()[0] == 0


def _keyed_rows(env, key):
    out = {}
    for table, col in (("paper_locations", "project"), ("papers_no_doi", "project"), ("candidates", "source_project"),
                       ("cites", "source_project"), ("scoped_candidates", "project"), ("scoped_cites", "project"),
                       ("index_runs", "project")):
        out[f"{table}.{col}"] = env.q(f"SELECT COUNT(*) FROM {table} WHERE {col} = ?", [key])[0][0]
    return out


def test_rename_project_is_a_dry_run_by_default(env):
    _rename_fixture(env)
    before = _keyed_rows(env, "teaching_old")
    rc, out = env.index("--rename-project", "teaching_old", "teaching_new")
    assert rc == 0 and "dry run" in out
    assert _keyed_rows(env, "teaching_old") == before and before["paper_locations.project"] == 3
    plan = {f"{p['table']}.{p['column']}": p for p in summary_of(out)["rename"]["plan"]}
    assert plan["paper_locations.project"]["move"] == 2 and plan["paper_locations.project"]["covered_by_new"] == 1
    assert set(plan) >= {"paper_locations.project", "papers_no_doi.project", "candidates.source_project",
                         "cites.source_project", "scoped_candidates.project", "scoped_cites.project",
                         "index_runs.project"}


def test_rename_project_execute_leaves_no_rows_under_the_old_key(env):
    _rename_fixture(env)
    old_dois = set(env.q("SELECT doi FROM paper_locations WHERE project = 'teaching_old'"))
    new_dois = set(env.q("SELECT doi FROM paper_locations WHERE project = 'teaching_new'"))
    n_runs = env.q("SELECT COUNT(*) FROM index_runs")[0][0]
    rc, out = env.index("--rename-project", "teaching_old", "teaching_new", "--execute")
    assert rc == 0
    assert set(_keyed_rows(env, "teaching_old").values()) == {0}
    assert set(env.q("SELECT doi FROM paper_locations WHERE project = 'teaching_new'")) == old_dois | new_dois
    assert env.q("SELECT COUNT(*) FROM index_runs")[0][0] == n_runs          # history moved, not lost
    assert env.q("SELECT COUNT(*) FROM scoped_candidates WHERE project = 'teaching_new'") == [(1,)]
    assert env.q("SELECT pdf_filename FROM papers_no_doi") == [("2019_NoDoi.pdf",)]
    assert sorted(env.q("SELECT doi FROM candidates WHERE source_project = 'teaching_new'")) == [
        ("10.5555/c.0001",), ("10.5555/c.0002",)]
    assert "NOTE: projects.json still registers 'teaching_old'" in out


def test_rename_project_is_one_transaction(env):
    _rename_fixture(env)
    con = duckdb.connect(str(env.db))
    # a late table refuses the new key: everything before it must roll back too
    con.execute("CREATE TABLE zz_guard (project VARCHAR CHECK (project <> 'teaching_new'), n INTEGER)")
    con.execute("INSERT INTO zz_guard VALUES ('teaching_old', 1)")
    con.close()
    before = _keyed_rows(env, "teaching_old")
    with pytest.raises(duckdb.Error):
        env.index("--rename-project", "teaching_old", "teaching_new", "--execute")
    assert _keyed_rows(env, "teaching_old") == before


def test_rename_project_needs_two_keys(env):
    env.registry({"research_a": {"lib_dir": "literature"}})
    make_lib(env.root, "research_a", ["10.5555/a.0001"])
    assert env.index()[0] == 0
    assert env.index("--rename-project", "research_a", "research_a")[0] == 1
