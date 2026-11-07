"""enrich_abstracts --project: targets only the DOIs a project may fetch or holds (its candidates
and its library), not every DOI in paper_metadata. 2026-11-05: a one-project snowball asked for
226,078 portfolio-wide DOIs and held the DB write lock for most of a day."""
import duckdb
import enrich_abstracts as ea


def _db(tmp_path):
    p = str(tmp_path / "scope.duckdb")
    con = duckdb.connect(p)
    con.execute("CREATE TABLE paper_metadata (doi VARCHAR PRIMARY KEY, abstract VARCHAR)")
    con.executemany("INSERT INTO paper_metadata VALUES (?, NULL)",
                    [("10.1/a_cand",), ("10.1/a_held",), ("10.1/b_cand",), ("10.1/orphan",)])
    con.execute("CREATE TABLE candidates (doi VARCHAR, source_type VARCHAR, source_seed_doi VARCHAR, "
                "source_project VARCHAR, citing_cited_by INTEGER, refreshed_at TIMESTAMP)")
    con.executemany("INSERT INTO candidates VALUES (?, 'forward', '10.9/seed', ?, 0, NULL)",
                    [("10.1/a_cand", "A"), ("10.1/b_cand", "B")])
    con.execute("CREATE TABLE paper_locations (doi VARCHAR, project VARCHAR)")
    con.execute("INSERT INTO paper_locations VALUES ('10.1/a_held', 'A')")
    con.close()
    return p


def _calls(monkeypatch):
    log = []
    monkeypatch.setattr(ea, "crossref_abstract", lambda doi, timeout=15: log.append(doi) or "")
    return log


def test_project_scopes_targets_to_its_candidates_and_holdings(tmp_path, monkeypatch):
    log = _calls(monkeypatch)
    assert ea.main(["--db", _db(tmp_path), "--project", "A"]) == 0
    assert sorted(log) == ["10.1/a_cand", "10.1/a_held"]


def test_project_repeats(tmp_path, monkeypatch):
    log = _calls(monkeypatch)
    assert ea.main(["--db", _db(tmp_path), "--project", "A", "--project", "B"]) == 0
    assert sorted(log) == ["10.1/a_cand", "10.1/a_held", "10.1/b_cand"]


def test_no_project_is_still_portfolio_wide(tmp_path, monkeypatch):
    log = _calls(monkeypatch)
    assert ea.main(["--db", _db(tmp_path)]) == 0
    assert sorted(log) == ["10.1/a_cand", "10.1/a_held", "10.1/b_cand", "10.1/orphan"]
