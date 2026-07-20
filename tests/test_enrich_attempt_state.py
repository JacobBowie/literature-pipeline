"""Batch 2: enrich_abstracts attempt-state column.

Stops the ~47k permanent CrossRef-fails from being re-queried every run: a hit, a
genuine miss (200/no-abstract), and a PERMANENT 404 (arXiv/DataCite not in CrossRef)
mark abstract_attempted_at; a transient 5xx AND a transient 403 (WAF/CDN block) stay
unmarked so the next run retries them. Also exercises the self-healing ADD COLUMN on
an older-schema DB, in BOTH entry points (enrich_abstracts + index_portfolio's SCHEMA).
"""
import sys
import duckdb
import enrich_abstracts as ea


def _make_old_schema_db(tmp_path):
    p = str(tmp_path / "as.duckdb")
    con = duckdb.connect(p)
    con.execute("CREATE TABLE paper_metadata (doi VARCHAR PRIMARY KEY, abstract VARCHAR)")  # no attempt col
    con.executemany("INSERT INTO paper_metadata (doi, abstract) VALUES (?, ?)", [
        ("10.1/hit", None), ("10.2/miss", None), ("10.48550/arxiv.404", None),
        ("10.3/transient", None), ("10.4/waf403", None),
    ])
    con.close()
    return p


def _fake_crossref(call_log):
    def f(doi, timeout=15):
        call_log.append(doi)
        if doi == "10.1/hit":
            return "An abstract"
        if doi == "10.2/miss":
            return ""
        if doi == "10.48550/arxiv.404":
            raise ea.CrossRefError("HTTP 404", status=404)   # permanent (not in CrossRef)
        if doi == "10.3/transient":
            raise ea.CrossRefError("HTTP 503", status=503)   # transient
        if doi == "10.4/waf403":
            raise ea.CrossRefError("HTTP 403", status=403)   # transient (WAF/CDN block)
        return ""
    return f


def test_attempt_state_marks_permanent_and_retries_transient(tmp_path, monkeypatch):
    db = _make_old_schema_db(tmp_path)

    log1 = []
    monkeypatch.setattr(ea, "crossref_abstract", _fake_crossref(log1))
    monkeypatch.setattr(sys, "argv", ["enrich_abstracts.py", "--db", db, "--sleep", "0"])
    ea.main()   # run 1 -- self-heals the column, attempts all 5
    assert sorted(log1) == ["10.1/hit", "10.2/miss", "10.3/transient",
                            "10.4/waf403", "10.48550/arxiv.404"]

    con = duckdb.connect(db)
    def r(doi):
        return con.execute("SELECT abstract, abstract_attempted_at FROM paper_metadata "
                           "WHERE doi=?", [doi]).fetchone()
    assert r("10.1/hit")[0] == "An abstract" and r("10.1/hit")[1] is not None
    assert r("10.2/miss")[0] is None and r("10.2/miss")[1] is not None      # genuine miss marked
    assert r("10.48550/arxiv.404")[1] is not None                           # permanent 404 marked
    assert r("10.3/transient")[1] is None                                   # transient 503 NOT marked
    assert r("10.4/waf403")[1] is None                                      # transient 403 NOT marked
    con.close()

    # run 2: only the still-eligible transient DOIs are re-queried
    log2 = []
    monkeypatch.setattr(ea, "crossref_abstract", _fake_crossref(log2))
    ea.main()
    assert sorted(log2) == ["10.3/transient", "10.4/waf403"], log2


def test_retry_after_days_zero_reattempts_everything(tmp_path, monkeypatch):
    db = _make_old_schema_db(tmp_path)
    log1 = []
    monkeypatch.setattr(ea, "crossref_abstract", _fake_crossref(log1))
    monkeypatch.setattr(sys, "argv",
                        ["enrich_abstracts.py", "--db", db, "--sleep", "0", "--retry-after-days", "0"])
    ea.main()
    log2 = []
    monkeypatch.setattr(ea, "crossref_abstract", _fake_crossref(log2))
    ea.main()
    # --retry-after-days 0 disables the skip: the 4 still-missing DOIs are re-attempted
    assert sorted(log2) == ["10.2/miss", "10.3/transient", "10.4/waf403", "10.48550/arxiv.404"]


def test_index_schema_self_heals_pre_migration_db(tmp_path):
    """REGRESSION: index_portfolio.SCHEMA must bind the `papers` view on a DB whose
    paper_metadata predates abstract_attempted_at (the normal non-rebuild re-index path)."""
    import index_portfolio as ip
    p = str(tmp_path / "old.duckdb")
    con = duckdb.connect(p)
    con.execute("""CREATE TABLE paper_metadata (
        doi VARCHAR PRIMARY KEY, year INTEGER, lastname VARCHAR, title VARCHAR,
        venue VARCHAR, authors VARCHAR, abstract VARCHAR, refreshed_at TIMESTAMP)""")
    con.execute(ip.SCHEMA)   # ALTER back-fills the column BEFORE the papers view binds
    cols = [row[1] for row in con.execute("PRAGMA table_info('paper_metadata')").fetchall()]
    assert "abstract_attempted_at" in cols
    con.execute("SELECT * FROM papers").fetchall()   # binds -> no BinderException
    con.close()
