"""W3-C1 acceptance: the public citation-network build reads the same rows before and after.

The public build (a sibling repo's extract.py, `fetch_extension` and `compute_extension_families`)
reads `cites` joined on a seed DOI list, forward and backward, plus `candidates` and
`paper_metadata`. FETCH_EXTENSION_SQL and FAMILIES_SQL below are those two queries verbatim, with the
project literal turned into a parameter. On a fixture with no text-only holdings and only normalised
DOIs (tests/fixtures/W3-C1/public_build_fixture.json) the index built by this code must answer them
exactly as the pre-W3-C1 index (ce0479b) did; public_build_expected.json holds that answer, made by
running the ce0479b module's ingest on the same fixture. The reindex also leaves the abstract count
unchanged, and scoped harvests change none of the rows.
"""
import json
import shutil
from pathlib import Path

import duckdb
import pytest

import index_portfolio as I
import lit_util

FIX = Path(__file__).parent / "fixtures" / "W3-C1"

FETCH_EXTENSION_SQL = """
    WITH seeds AS (SELECT UNNEST(?::VARCHAR[]) AS doi),
    forward AS (
        SELECT c.citing_doi AS doi, c.cited_doi AS seed_doi
        FROM cites c JOIN seeds s ON s.doi = c.cited_doi
        WHERE c.source_project = ?
    ),
    backward AS (
        SELECT c.cited_doi AS doi, c.citing_doi AS seed_doi
        FROM cites c JOIN seeds s ON s.doi = c.citing_doi
        WHERE c.source_project = ?
    ),
    combined AS (SELECT * FROM forward UNION ALL SELECT * FROM backward),
    scored AS (
        SELECT
            combined.doi,
            COUNT(DISTINCT combined.seed_doi) AS n_seed_links,
            MAX(cand.citing_cited_by) AS cite_count
        FROM combined
        LEFT JOIN candidates cand
            ON cand.doi = combined.doi AND cand.source_project = ?
        WHERE combined.doi NOT IN (SELECT doi FROM seeds)
        GROUP BY combined.doi
    )
    SELECT
        s.doi,
        s.n_seed_links,
        s.cite_count,
        p.year, p.lastname, p.title, p.venue, p.authors
    FROM scored s
    LEFT JOIN paper_metadata p ON p.doi = s.doi
    WHERE p.year IS NOT NULL
    ORDER BY s.n_seed_links DESC, COALESCE(s.cite_count, 0) DESC
    LIMIT {top_k}
"""

FAMILIES_SQL = """
    WITH ext AS (SELECT UNNEST(?::VARCHAR[]) AS doi),
    seeds AS (SELECT UNNEST(?::VARCHAR[]) AS doi),
    links AS (
        SELECT c.citing_doi AS ext_doi, c.cited_doi AS seed_doi
        FROM cites c
        WHERE c.source_project = ?
          AND c.citing_doi IN (SELECT doi FROM ext)
          AND c.cited_doi  IN (SELECT doi FROM seeds)
        UNION ALL
        SELECT c.cited_doi AS ext_doi, c.citing_doi AS seed_doi
        FROM cites c
        WHERE c.source_project = ?
          AND c.cited_doi  IN (SELECT doi FROM ext)
          AND c.citing_doi IN (SELECT doi FROM seeds)
    )
    SELECT ext_doi, seed_doi FROM links
"""


def load_fixture():
    return json.loads((FIX / "public_build_fixture.json").read_text(encoding="utf-8"))


def materialise(fx, root: Path) -> Path:
    """Write the fixture library under root/<project>/<lib_dir>; returns the library path."""
    lib = root / fx["project"] / fx["lib_dir"]
    lib.mkdir(parents=True, exist_ok=True)
    for name, text in fx["files"].items():
        (lib / name).write_text(text, encoding="utf-8")
    for key, fname in (("forward_csv", "_forward_citations.csv"), ("reverse_csv", "_reverse_citations_parsed.csv")):
        rows = fx[key]
        lines = [",".join(rows[0])] + [",".join(r) for r in rows[1:]]
        (lib / fname).write_text("\n".join(lines) + "\n", encoding="utf-8")
    return lib


def public_build(con, fx):
    """The two queries as the public build runs them; rows sorted (ties have no defined order)."""
    p = fx["project"]
    real = [d for d in fx["seeds"] if not d.startswith("manual:")]
    ext = con.execute(FETCH_EXTENSION_SQL.format(top_k=int(fx["top_k"])), [real, p, p, p]).fetchall()
    ext_dois = [r[0] for r in ext]
    fam_seeds = [d for d in fx["families"] if not d.startswith("manual:")]
    links = con.execute(FAMILIES_SQL, [ext_dois, fam_seeds, p, p]).fetchall()
    norm = lambda rows: sorted([list(r) for r in rows], key=lambda r: json.dumps(r, default=str))
    return {"fetch_extension": norm(ext), "families": norm(links)}


def index_fixture(tmp_path, monkeypatch, fx, extra_files=None):
    root = tmp_path / "root"
    lib = materialise(fx, root)
    for name, text in (extra_files or {}).items():
        (lib / name).write_text(text, encoding="utf-8")
    reg = {"state_dir": str(tmp_path / "state"),
           "projects": {fx["project"]: {"lib_dir": fx["lib_dir"], "active": True}}}
    cfg = tmp_path / "projects.json"
    cfg.write_text(json.dumps(reg), encoding="utf-8")
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    monkeypatch.setattr(I, "CONFIG_PATH", cfg)
    db = tmp_path / "refs" / "portfolio.duckdb"
    assert I.main(["--db", str(db)]) == 0
    return db, lib


def test_public_build_queries_return_the_pre_w3c1_rows(tmp_path, monkeypatch):
    fx = load_fixture()
    expected = json.loads((FIX / "public_build_expected.json").read_text(encoding="utf-8"))
    db, _lib = index_fixture(tmp_path, monkeypatch, fx)
    con = duckdb.connect(str(db), read_only=True)
    try:
        got = public_build(con, fx)
        cites = sorted(list(r) for r in con.execute(
            "SELECT citing_doi, cited_doi, source_pipeline, source_project FROM cites").fetchall())
    finally:
        con.close()
    assert got["fetch_extension"], "the fixture must exercise the query"
    assert got == expected["queries"]
    assert cites == expected["cites"]


def test_scoped_harvests_and_reindex_change_no_public_rows(tmp_path, monkeypatch):
    fx = load_fixture()
    expected = json.loads((FIX / "public_build_expected.json").read_text(encoding="utf-8"))
    scoped = ("seed_doi,seed_label,seed_chapter,citing_paper_id,citing_doi,citing_title,citing_year,"
              "citing_authors,citing_venue,citing_cited_by,citing_abstract\n"
              "10.5555/seed.0001,Alpha A 2001,Ch01,x1,10.8888/scoped.0001,Scoped citer,2022,Z,VZ,9,\n"
              "10.5555/seed.0002,Beta B 2004,Ch02,x2,10.6666/cite.0101,Citer one,2010,A; B,V1,40,\n")
    db, lib = index_fixture(tmp_path, monkeypatch, fx, {"_teach_forward_citations.csv": scoped})
    con = duckdb.connect(str(db))
    try:
        assert public_build(con, fx) == expected["queries"]
        con.execute("UPDATE paper_metadata SET abstract = 'ENRICHED' WHERE doi LIKE '10.6666/%'")
        n_abs = con.execute("SELECT COUNT(*) FROM paper_metadata WHERE abstract <> ''").fetchone()[0]
    finally:
        con.close()
    assert n_abs > 0
    assert I.main(["--db", str(db)]) == 0                       # a second (non-rebuild) index
    con = duckdb.connect(str(db), read_only=True)
    try:
        assert con.execute("SELECT COUNT(*) FROM paper_metadata WHERE abstract <> ''").fetchone()[0] == n_abs
        assert public_build(con, fx) == expected["queries"]
        assert con.execute("SELECT COUNT(*) FROM scoped_candidates").fetchone()[0] == 2
    finally:
        con.close()
