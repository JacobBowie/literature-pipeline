"""Batch 3 (c11/E1): the shared _write_citation_rows helper.

Pins the behaviors the refactor changed vs. the old per-key-DELETE+INSERT blocks:
  - cites use ON CONFLICT (the cites PK excludes source_pipeline), so the SAME
    (citing,cited,project) edge found by both the forward and the reverse pass updates
    the pipeline tag (last-writer-wins) instead of raising a PK violation;
  - candidates/cites dedup within a batch;
  - paper_metadata inserts only new DOIs (never overwrites a richer existing row).
"""
import duckdb
import index_portfolio as ip


def _db(tmp_path):
    con = duckdb.connect(str(tmp_path / "c.duckdb"))
    con.execute(ip.SCHEMA)
    return con


def test_cites_on_conflict_cross_pipeline(tmp_path):
    con = _db(tmp_path)
    ip._write_citation_rows(con, [], [], [("10.a/x", "10.b/y", "forward", "P")])
    assert con.execute("SELECT source_pipeline FROM cites").fetchone()[0] == "forward"
    # reverse pass re-emits the SAME (citing,cited,project) edge -> ON CONFLICT, not a crash
    ip._write_citation_rows(con, [], [], [("10.a/x", "10.b/y", "reverse", "P")])
    rows = con.execute("SELECT source_pipeline FROM cites").fetchall()
    assert rows == [("reverse",)]           # one row, last-writer-wins
    con.close()


def test_within_batch_dedup(tmp_path):
    con = _db(tmp_path)
    dup_cand = [("10.a/x", "forward", "10.s/seed", "P", 3, "2026-07-20")] * 2
    dup_cite = [("10.a/x", "10.s/seed", "forward", "P")] * 2
    ip._write_citation_rows(con, dup_cand, [], dup_cite)   # duplicates in one batch must not PK-collide
    assert con.execute("SELECT count(*) FROM candidates").fetchone()[0] == 1
    assert con.execute("SELECT count(*) FROM cites").fetchone()[0] == 1
    con.close()


def test_metadata_never_overwrites_existing(tmp_path):
    con = _db(tmp_path)
    con.execute("INSERT INTO paper_metadata (doi, title, abstract) VALUES "
                "('10.a/x', 'Rich RIS title', 'ENRICHED')")
    # a candidate row for the same DOI must NOT overwrite the richer existing metadata
    meta = [("10.a/x", 2020, "", "Thin forward title", "", "", "2026-07-20")]
    ip._write_citation_rows(con, [], meta, [])
    title, abstract = con.execute(
        "SELECT title, abstract FROM paper_metadata WHERE doi='10.a/x'").fetchone()
    assert title == "Rich RIS title" and abstract == "ENRICHED"   # abstract invariant preserved
    con.close()


def test_C2_ingest_writes_enrolled_in_transaction(tmp_path):
    """C2: ingest_forward + prune_citations write through `con`, so a con-level ROLLBACK
    undoes them. If they used a child cursor (the footgun), the rollback would NOT undo --
    this test would then see the rows survive."""
    con = _db(tmp_path)
    lib = tmp_path / "lib"; lib.mkdir()
    fwd = lib / "_forward.csv"
    fwd.write_text(
        "seed_doi,citing_doi,citing_year,citing_title,citing_venue,citing_authors,citing_cited_by\n"
        "10.1234/seed.1,10.5678/cand.2,2024,Cand,Venue,Auth,5\n", encoding="utf-8")
    con.execute("BEGIN TRANSACTION")
    ip.ingest_forward(con, "P", fwd, lib)
    assert con.execute("SELECT count(*) FROM candidates").fetchone()[0] == 1   # written inside txn
    assert con.execute("SELECT count(*) FROM cites").fetchone()[0] == 1
    con.execute("ROLLBACK")
    assert con.execute("SELECT count(*) FROM candidates").fetchone()[0] == 0   # enrolled -> undone
    assert con.execute("SELECT count(*) FROM cites").fetchone()[0] == 0
    con.close()
