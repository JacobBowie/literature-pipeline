"""B6/A3+C1: index_portfolio --rebuild hardening.

C1 -- --rebuild snapshots the old DB to <db>.bak (atomic move) before dropping it, so a rebuild that
      dies partway, or a regretted --rebuild, is recoverable instead of an irreversible loss.
A3 -- the abstract-count pre-flight closes its read-only handle in a finally; even when the COUNT
      raises (e.g. no paper_metadata table), the handle can't keep the file locked and block the
      backup/drop on Windows.
"""
import sys

import duckdb
import pytest

import index_portfolio as I


def test_rebuild_backs_up_old_db_and_survives_missing_table(tmp_path, monkeypatch):
    db = tmp_path / "portfolio.duckdb"
    # An OLD db WITHOUT paper_metadata -> the --rebuild pre-flight COUNT raises, exercising A3's
    # finally-close (a leaked read-only handle would lock the file and fail the C1 backup below).
    c = duckdb.connect(str(db))
    c.execute("CREATE TABLE junk (x INTEGER)")
    c.execute("INSERT INTO junk VALUES (1)")
    c.close()

    monkeypatch.setattr(I, "load_config", lambda: {})   # no projects -> ingest loop is a no-op
    monkeypatch.setattr(sys, "argv", ["index_portfolio.py", "--db", str(db), "--rebuild"])
    I.main()

    bak = tmp_path / "portfolio.duckdb.bak"
    assert bak.exists()                                  # C1: previous DB preserved (not deleted)
    assert db.exists()                                   # a fresh DB was rebuilt in place

    # the .bak is the OLD db (has junk); the fresh db has the schema, not junk (proves rebuild ran
    # against a clean file, which requires A3's pre-flight handle to have been closed before backup)
    cb = duckdb.connect(str(bak), read_only=True)
    old_tables = {r[0] for r in cb.execute("SHOW TABLES").fetchall()}
    cb.close()
    cn = duckdb.connect(str(db), read_only=True)
    new_tables = {r[0] for r in cn.execute("SHOW TABLES").fetchall()}
    cn.close()
    assert "junk" in old_tables
    assert "paper_metadata" in new_tables and "junk" not in new_tables


def test_rebuild_backup_overwrites_prior_bak(tmp_path, monkeypatch):
    """A second --rebuild replaces an existing .bak atomically (db_path.replace overwrites)."""
    db = tmp_path / "portfolio.duckdb"
    bak = tmp_path / "portfolio.duckdb.bak"
    bak.write_bytes(b"stale-old-backup")                 # a leftover .bak from a prior rebuild
    c = duckdb.connect(str(db)); c.execute("CREATE TABLE paper_metadata (doi VARCHAR)"); c.close()

    monkeypatch.setattr(I, "load_config", lambda: {})
    monkeypatch.setattr(sys, "argv", ["index_portfolio.py", "--db", str(db), "--rebuild"])
    I.main()

    assert bak.exists() and bak.read_bytes()[:4] != b"stal"   # replaced with the real prior DB
    cb = duckdb.connect(str(bak), read_only=True)             # and it's a valid duckdb file
    assert "paper_metadata" in {r[0] for r in cb.execute("SHOW TABLES").fetchall()}
    cb.close()


def test_rebuild_does_not_clobber_richer_bak(tmp_path, monkeypatch):
    """Review fix (finding 3): a crash-then-retry must NOT overwrite a .bak that holds more harvested
    abstracts than the current DB. A crashed rebuild leaves a 0-abstract DB; the retry must preserve
    the good pre-crash snapshot rather than clobber it (which destroyed the only recoverable copy)."""
    db = tmp_path / "portfolio.duckdb"
    bak = tmp_path / "portfolio.duckdb.bak"
    # .bak = the good pre-crash snapshot WITH a harvested abstract
    cb = duckdb.connect(str(bak))
    cb.execute("CREATE TABLE paper_metadata (doi VARCHAR, abstract VARCHAR)")
    cb.execute("INSERT INTO paper_metadata VALUES ('10.1/a', 'a real harvested abstract')")
    cb.close()
    # current DB = a partial/crashed rebuild with NO abstracts
    cc = duckdb.connect(str(db))
    cc.execute("CREATE TABLE paper_metadata (doi VARCHAR, abstract VARCHAR)")
    cc.execute("INSERT INTO paper_metadata VALUES ('10.1/b', NULL)")
    cc.close()

    monkeypatch.setattr(I, "load_config", lambda: {})
    monkeypatch.setattr(sys, "argv", ["index_portfolio.py", "--db", str(db), "--rebuild"])
    I.main()

    # the good .bak (with the abstract) survived; it was NOT overwritten by the 0-abstract current DB
    cbak = duckdb.connect(str(bak), read_only=True)
    n_abs = cbak.execute("SELECT COUNT(*) FROM paper_metadata "
                         "WHERE abstract IS NOT NULL AND abstract != ''").fetchone()[0]
    cbak.close()
    assert n_abs == 1                                         # pre-crash abstract preserved
    assert db.exists()                                        # a fresh DB was still rebuilt in place


# ---------------------------------------------------------------- W3-C1: history tables survive
W3E_DDL = (
    "CREATE TABLE recent_feed (seed_doi VARCHAR, recommended_doi VARCHAR, pool VARCHAR, rank INTEGER, "
    "first_seen_at TIMESTAMPTZ, last_seen_at TIMESTAMPTZ, PRIMARY KEY (seed_doi, recommended_doi, pool))",
    "CREATE TABLE rec_attempts (seed_doi VARCHAR PRIMARY KEY, outcome VARCHAR, status VARCHAR, "
    "attempted_at TIMESTAMPTZ)",
    "CREATE TABLE s2_enrichment (doi VARCHAR PRIMARY KEY, oa_url VARCHAR, citation_count INTEGER, "
    "reference_count INTEGER, abstract_elided BOOLEAN, fetched_at TIMESTAMPTZ)",
)


def test_rebuild_carries_history_tables_from_the_bak(tmp_path, monkeypatch):
    """--rebuild keeps abstract_attempts (W2-E2) and recent_feed / rec_attempts / s2_enrichment
    (W3-E), keys included, from the .bak; the abstracts themselves are still discarded."""
    import enrich_abstracts
    db = tmp_path / "portfolio.duckdb"
    c = duckdb.connect(str(db))
    c.execute(I.SCHEMA)
    c.execute(enrich_abstracts.ATTEMPTS_DDL)
    for ddl in W3E_DDL:
        c.execute(ddl)
    c.execute("INSERT INTO paper_metadata (doi, abstract) VALUES ('10.5555/a.0001', 'an abstract')")
    c.execute("INSERT INTO abstract_attempts VALUES ('10.5555/a.0001', 'NO_MATCH', '404', '', "
              "TIMESTAMP '2026-09-01')")
    c.execute("INSERT INTO recent_feed VALUES ('10.5555/s.0001', '10.5555/r.0001', 'recent', 1, "
              "TIMESTAMPTZ '2026-09-01 00:00:00+00', TIMESTAMPTZ '2026-09-02 00:00:00+00')")
    c.execute("INSERT INTO rec_attempts VALUES ('10.5555/s.0001', 'OK', '200', "
              "TIMESTAMPTZ '2026-09-01 00:00:00+00')")
    c.execute("INSERT INTO s2_enrichment VALUES ('10.5555/a.0001', '', 3, 4, false, "
              "TIMESTAMPTZ '2026-09-01 00:00:00+00')")
    c.close()

    monkeypatch.setattr(I, "load_config", lambda: {})
    assert I.main(["--db", str(db), "--rebuild"]) == 0
    cn = duckdb.connect(str(db))
    try:
        assert cn.execute("SELECT doi, outcome, status FROM abstract_attempts").fetchall() == [
            ("10.5555/a.0001", "NO_MATCH", "404")]
        assert cn.execute("SELECT seed_doi, recommended_doi, rank FROM recent_feed").fetchall() == [
            ("10.5555/s.0001", "10.5555/r.0001", 1)]
        assert cn.execute("SELECT COUNT(*) FROM rec_attempts").fetchone()[0] == 1
        assert cn.execute("SELECT citation_count FROM s2_enrichment").fetchone()[0] == 3
        assert cn.execute("SELECT COUNT(*) FROM paper_metadata").fetchone()[0] == 0   # abstracts discarded
        with pytest.raises(duckdb.ConstraintException):                               # the key came along
            cn.execute("INSERT INTO abstract_attempts (doi) VALUES ('10.5555/a.0001')")
    finally:
        cn.close()
    assert (tmp_path / "portfolio.duckdb.bak").exists()                               # .bak unchanged


def test_rebuild_creates_no_history_table_the_bak_lacks(tmp_path, monkeypatch):
    db = tmp_path / "portfolio.duckdb"
    c = duckdb.connect(str(db))
    c.execute(I.SCHEMA)
    c.close()
    monkeypatch.setattr(I, "load_config", lambda: {})
    assert I.main(["--db", str(db), "--rebuild"]) == 0
    cn = duckdb.connect(str(db), read_only=True)
    tables = {r[0] for r in cn.execute("SHOW TABLES").fetchall()}
    cn.close()
    assert not tables & set(I.CARRY_OVER_TABLES)
    assert {"index_runs", "scoped_candidates", "scoped_cites"} <= tables


def test_rebuild_with_an_unreadable_bak_is_degraded(tmp_path, monkeypatch, capsys):
    """The .bak kept from a crashed rebuild cannot be attached: the rebuild still runs, and exits 2
    with the reason, rather than dropping the history tables silently."""
    db = tmp_path / "portfolio.duckdb"
    bak = tmp_path / "portfolio.duckdb.bak"
    c = duckdb.connect(str(db))
    c.execute(I.SCHEMA)
    c.close()
    monkeypatch.setattr(I, "load_config", lambda: {})

    def broken(con, path):
        raise duckdb.IOException(f"simulated: cannot open {path.name}")
    monkeypatch.setattr(I, "carry_over", broken)
    assert I.main(["--db", str(db), "--rebuild"]) == 2
    out = capsys.readouterr().out
    assert "could not carry history tables" in out and out.rstrip().splitlines()[-1].startswith(I.SUMMARY_MARKER)
    assert bak.exists() and db.exists()
