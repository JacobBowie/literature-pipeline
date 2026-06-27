"""Stale-row prune regression tests (T2, 2026-06-25 audit).

Verify the project-scoped prunes in index_portfolio against a scratch DuckDB:
- a PDF that gains a DOI loses its stale papers_no_doi row (the '130' inflation bug)
- removing the last no-DOI PDF clears papers_no_doi for the project (else-branch)
- a shrunk citation CSV drops orphan candidate/cite rows
- the --gc DELETE reclaims only zero-reference paper_metadata rows
"""
import datetime
import duckdb
import pytest

import index_portfolio as I


def _con(tmp_path):
    con = duckdb.connect(str(tmp_path / "t.duckdb"))
    con.execute(I.SCHEMA)
    return con


def _ris(doi):
    return f"TY  - JOUR\nDO  - {doi}\nER  - \n"


def test_papers_no_doi_pruned_when_pdf_gains_doi(tmp_path):
    lib = tmp_path / "lib"; lib.mkdir()
    (lib / "2099_Foo.pdf").write_bytes(b"%PDF-1.4 foo")
    (lib / "2099_Foo.ris").write_text(_ris("10.9999/foo1"), encoding="utf-8")
    (lib / "2099_Bar.pdf").write_bytes(b"%PDF-1.4 bar")  # no .ris -> no-DOI
    con = _con(tmp_path)
    I.ingest_papers(con, "T", lib)
    assert con.execute("SELECT COUNT(*) FROM papers_no_doi WHERE project='T'").fetchone()[0] == 1
    assert con.execute("SELECT doi FROM paper_locations WHERE project='T'").fetchone()[0] == "10.9999/foo1"
    # Bar gains a .ris -> re-ingest -> its stale no-DOI row must be pruned
    (lib / "2099_Bar.ris").write_text(_ris("10.9999/bar1"), encoding="utf-8")
    I.ingest_papers(con, "T", lib)
    assert con.execute("SELECT COUNT(*) FROM papers_no_doi WHERE project='T'").fetchone()[0] == 0
    assert con.execute("SELECT COUNT(*) FROM paper_locations WHERE project='T'").fetchone()[0] == 2


def test_papers_no_doi_cleared_when_last_nodoi_pdf_removed(tmp_path):
    lib = tmp_path / "lib"; lib.mkdir()
    (lib / "2099_Bar.pdf").write_bytes(b"%PDF-1.4 bar")
    con = _con(tmp_path)
    I.ingest_papers(con, "T", lib)
    assert con.execute("SELECT COUNT(*) FROM papers_no_doi WHERE project='T'").fetchone()[0] == 1
    (lib / "2099_Bar.pdf").unlink()          # remove the only no-DOI PDF
    I.ingest_papers(con, "T", lib)
    assert con.execute("SELECT COUNT(*) FROM papers_no_doi WHERE project='T'").fetchone()[0] == 0


def test_candidates_cites_pruned_on_shrunk_forward_csv(tmp_path):
    lib = tmp_path / "lib"; lib.mkdir()
    csv = lib / "_forward_citations.csv"
    hdr = "seed_doi,citing_doi,citing_year,citing_title,citing_venue,citing_authors,citing_cited_by\n"
    csv.write_text(hdr
        + "10.1000/seed1,10.2000/cand1,2020,T1,V,A,5\n"
        + "10.1000/seed1,10.2000/cand2,2021,T2,V,A,3\n", encoding="utf-8")
    con = _con(tmp_path)
    I.ingest_forward(con, "T", csv, lib)
    assert con.execute("SELECT COUNT(*) FROM candidates WHERE source_project='T'").fetchone()[0] == 2
    assert con.execute("SELECT COUNT(*) FROM cites WHERE source_project='T'").fetchone()[0] == 2
    # shrink to 1 -> orphan cand2 + its cite must be pruned
    csv.write_text(hdr + "10.1000/seed1,10.2000/cand1,2020,T1,V,A,5\n", encoding="utf-8")
    I.ingest_forward(con, "T", csv, lib)
    assert con.execute("SELECT COUNT(*) FROM candidates WHERE source_project='T'").fetchone()[0] == 1
    assert con.execute("SELECT COUNT(*) FROM cites WHERE source_project='T'").fetchone()[0] == 1
    assert con.execute("SELECT doi FROM candidates WHERE source_project='T'").fetchone()[0] == "10.2000/cand1"


def test_gc_reclaims_only_orphans(tmp_path):
    con = _con(tmp_path)
    now = datetime.datetime.now().isoformat(timespec="seconds")
    con.execute("INSERT INTO paper_metadata (doi, refreshed_at) VALUES ('10.0000/orphan', ?)", [now])
    con.execute("INSERT INTO paper_metadata (doi, refreshed_at) VALUES ('10.0000/pinned', ?)", [now])
    con.execute("INSERT INTO candidates (doi, source_type, source_seed_doi, source_project, citing_cited_by, refreshed_at)"
                " VALUES ('10.0000/pinned','forward','10.0000/s','T',1,?)", [now])
    # Exercise the REAL shared GC helper (not an inlined copy of the SQL) so the test guards main().
    assert I.gc_orphan_metadata(con) == 1
    remaining = {r[0] for r in con.execute("SELECT doi FROM paper_metadata").fetchall()}
    assert remaining == {"10.0000/pinned"}


def test_candidates_cites_pruned_on_shrunk_reverse_csv(tmp_path):
    lib = tmp_path / "lib"; lib.mkdir()
    (lib / "2099_Seed.pdf").write_bytes(b"%PDF-1.4 seed")          # seed PDF + .ris -> resolvable seed DOI
    (lib / "2099_Seed.ris").write_text(_ris("10.1000/seed1"), encoding="utf-8")
    csv = lib / "_reverse_citations_parsed.csv"
    hdr = "seed,first_author,year,title_snippet,doi,raw\n"
    csv.write_text(hdr
        + "2099_Seed.pdf,A,2020,T1,10.2000/rev1,raw\n"
        + "2099_Seed.pdf,B,2021,T2,10.2000/rev2,raw\n", encoding="utf-8")
    con = _con(tmp_path)
    I.ingest_reverse(con, "T", csv, lib)
    assert con.execute("SELECT COUNT(*) FROM candidates WHERE source_project='T'").fetchone()[0] == 2
    assert con.execute("SELECT COUNT(*) FROM cites WHERE source_project='T'").fetchone()[0] == 2
    csv.write_text(hdr + "2099_Seed.pdf,A,2020,T1,10.2000/rev1,raw\n", encoding="utf-8")
    I.ingest_reverse(con, "T", csv, lib)
    assert con.execute("SELECT COUNT(*) FROM candidates WHERE source_project='T'").fetchone()[0] == 1
    assert con.execute("SELECT COUNT(*) FROM cites WHERE source_project='T'").fetchone()[0] == 1


def test_prune_citations_clears_orphans_when_csv_deleted(tmp_path):
    # The deleted-CSV completion: main() calls prune_citations when the CSV is absent.
    lib = tmp_path / "lib"; lib.mkdir()
    csv = lib / "_forward_citations.csv"
    hdr = "seed_doi,citing_doi,citing_year,citing_title,citing_venue,citing_authors,citing_cited_by\n"
    csv.write_text(hdr + "10.1000/seed1,10.2000/cand1,2020,T1,V,A,5\n", encoding="utf-8")
    con = _con(tmp_path)
    I.ingest_forward(con, "T", csv, lib)
    assert con.execute("SELECT COUNT(*) FROM candidates WHERE source_project='T'").fetchone()[0] == 1
    csv.unlink()                                  # CSV deleted outright
    I.prune_citations(con, "T", "forward")        # what main() now calls when fwd is None
    assert con.execute("SELECT COUNT(*) FROM candidates WHERE source_project='T'").fetchone()[0] == 0
    assert con.execute("SELECT COUNT(*) FROM cites WHERE source_project='T'").fetchone()[0] == 0


def test_parse_ris_continuation_and_ur_fallback(tmp_path):
    # T6: a wrapped TI value must NOT be truncated into the canonical stem
    p = tmp_path / "a.ris"
    p.write_text("TY  - JOUR\nTI  - A Very Long Title That Wraps\nAcross Two Lines\n"
                 "DO  - 10.1234/abc1\nER  - \n", encoding="utf-8")
    m = I.parse_ris(p)
    assert m["title"] == "A Very Long Title That Wraps Across Two Lines"
    assert m["doi"] == "10.1234/abc1"
    # T6: a .ris with no DO but a doi.org UR still yields a DOI (not no-DOI)
    q = tmp_path / "b.ris"
    q.write_text("TY  - JOUR\nTI  - No DO Tag Here\nUR  - https://doi.org/10.5678/xyz9\nER  - \n", encoding="utf-8")
    assert I.parse_ris(q)["doi"] == "10.5678/xyz9"
    # canonical single-line .ris is unaffected
    r = tmp_path / "c.ris"
    r.write_text("TY  - JOUR\nTI  - Single Line Title\nDO  - 10.9999/canon1\nER  - \n", encoding="utf-8")
    mr = I.parse_ris(r)
    assert mr["title"] == "Single Line Title" and mr["doi"] == "10.9999/canon1"
