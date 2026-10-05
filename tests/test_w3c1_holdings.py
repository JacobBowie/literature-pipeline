"""W3-C1: text-only holdings (DEC-08), identity flags, n_files parity and the paper_locations GC.

- A text-only sidecar (audit_portfolio.is_text_only_sidecar: the pre-W2a JATS shape with no
  `has_pdf` key, or pmc's `has_pdf: false`) is a paper_locations row with has_pdf=false and
  pdf_filename = the sidecar, survives every prune, and leaves top_candidates.
- A file whose `.identity.json` or pmc `.fulltext.json` carries identity FLAG (or doc_kind
  SUPPLEMENT) is a review item: never indexed as its queue DOI.
- n_files = every top-level PDF + every top-level text-only sidecar, exactly what
  audit_portfolio.scan_library counts.
- GC: a row whose file is gone or whose DOI changed is deleted (the old file-name prune kept a row
  under the old DOI while the file name still existed).
"""
import json

import duckdb
import pytest

import audit_portfolio as AP
import index_portfolio as I


def _con(tmp_path):
    con = duckdb.connect(str(tmp_path / "t.duckdb"))
    con.execute(I.SCHEMA)
    return con


def ris(doi, title="A title", py="2019", au="Smith, Jane"):
    return f"TY  - JOUR\nAU  - {au}\nPY  - {py}\nTI  - {title}\nJO  - A Journal\nDO  - {doi}\nER  - \n"


def jats_sidecar(doi, **extra):
    """The pre-W2a JATS text-only shape: no has_pdf, no extracted_from_pdf."""
    d = {"pmcid": "PMC100001", "pmid": "1", "doi": doi, "title": "Text only paper", "subtitle": "",
         "year": "2015", "journal": "A Journal", "authors": ["Morgan Avery-Lee", "Petrov Ilan"],
         "abstract": "An abstract.", "sections": [], "figures": [], "tables": [], "text": "Body text " * 20}
    d.update(extra)
    return json.dumps(d)


def write(lib, name, text):
    p = lib / name
    p.write_text(text, encoding="utf-8")
    return p


@pytest.fixture
def lib(tmp_path):
    d = tmp_path / "lib"
    d.mkdir()
    return d


def test_text_only_sidecar_without_has_pdf_key_is_indexed_and_survives_prune(tmp_path, lib):
    (lib / "2019_Smith_Held.pdf").write_bytes(b"%PDF-1.4 x")
    write(lib, "2019_Smith_Held.ris", ris("10.5555/held.0001"))
    sc = write(lib, "2015_Morgan_TextOnly.fulltext.json", jats_sidecar("10.5555/text.0002"))
    assert "has_pdf" not in json.loads(sc.read_text(encoding="utf-8"))
    con = _con(tmp_path)
    st = {}
    I.ingest_papers(con, "T", lib, stats=st)
    row = con.execute("SELECT doi, pdf_filename, has_pdf, has_sidecar, has_ris, sidecar_text_len > 0 "
                      "FROM paper_locations WHERE has_pdf = false").fetchall()
    assert row == [("10.5555/text.0002", "2015_Morgan_TextOnly.fulltext.json", False, True, False, True)]
    meta = con.execute("SELECT year, lastname, title, venue, authors FROM paper_metadata "
                       "WHERE doi = '10.5555/text.0002'").fetchone()
    assert meta == (2015, "Morgan", "Text only paper", "A Journal", "Morgan, Avery-Lee; Petrov, Ilan")
    assert (st["n_files"], st["n_pdfs"], st["n_text_only"], st["n_text_only_indexed"]) == (2, 1, 1, 1)
    # a re-index (the prune runs every time) keeps it; so does removing the PDF next to it
    (lib / "2019_Smith_Held.pdf").unlink()
    I.ingest_papers(con, "T", lib)
    assert con.execute("SELECT doi, has_pdf FROM paper_locations").fetchall() == [("10.5555/text.0002", False)]


def test_pmc_has_pdf_false_shape_and_same_stem_ris(tmp_path, lib):
    write(lib, "2020_Lee_Pmc.fulltext.json", jats_sidecar("10.5555/text.0003", has_pdf=False))
    write(lib, "2020_Lee_Pmc.ris", ris("10.5555/TEXT.0003", title="From the ris", py="2020", au="Lee, Kim"))
    con = _con(tmp_path)
    I.ingest_papers(con, "T", lib)
    assert con.execute("SELECT doi, has_pdf, has_ris FROM paper_locations").fetchall() == [
        ("10.5555/text.0003", False, True)]
    assert con.execute("SELECT title, year, lastname FROM paper_metadata").fetchone() == ("From the ris", 2020, "Lee")


@pytest.mark.parametrize("sidecar", [
    jats_sidecar("10.5555/orphan.0004", extracted_from_pdf=True),   # text extracted from a PDF that is gone
    jats_sidecar("10.5555/orphan.0004", has_pdf=True),              # pmc wrote it beside a PDF that is gone
    jats_sidecar("10.5555/orphan.0004", text=""),                   # no text
    jats_sidecar("10.5555/orphan.0004", identity="FLAG"),           # pmc judged it another work
])
def test_sidecars_that_are_not_holdings_are_not_indexed(tmp_path, lib, sidecar):
    write(lib, "2018_Orphan_Gone.fulltext.json", sidecar)
    con = _con(tmp_path)
    st = {}
    I.ingest_papers(con, "T", lib, stats=st)
    assert con.execute("SELECT COUNT(*) FROM paper_locations").fetchone()[0] == 0
    assert con.execute("SELECT COUNT(*) FROM paper_metadata").fetchone()[0] == 0
    assert st["n_files"] == 0 == len(AP.scan_library(lib)["text_only"])


@pytest.mark.parametrize("flag_file,flag_body", [
    ("2021_Wrong_Paper.identity.json", {"identity": "FLAG", "source": "unpaywall"}),
    ("2021_Wrong_Paper.identity.json", {"identity": "PASS", "doc_kind": "SUPPLEMENT"}),
    ("2021_Wrong_Paper.fulltext.json", {"doi": "10.5555/queue.0005", "identity": "FLAG", "text": "x" * 50}),
])
def test_identity_flagged_file_is_not_indexed_as_its_queue_doi(tmp_path, lib, flag_file, flag_body):
    (lib / "2021_Wrong_Paper.pdf").write_bytes(b"%PDF-1.4 x")
    write(lib, "2021_Wrong_Paper.ris", ris("10.5555/queue.0005"))
    write(lib, flag_file, json.dumps(flag_body))
    fwd = write(lib, "_forward_citations.csv",
                "seed_doi,citing_doi,citing_year,citing_title,citing_venue,citing_authors,citing_cited_by\n"
                "10.5555/seed.0009,10.5555/queue.0005,2021,Queue title,V,A,3\n")
    con = _con(tmp_path)
    st = {}
    I.ingest_papers(con, "T", lib, stats=st)
    I.ingest_forward(con, "T", fwd)
    assert con.execute("SELECT COUNT(*) FROM paper_locations").fetchone()[0] == 0
    assert con.execute("SELECT pdf_filename, reason FROM papers_no_doi").fetchall() == [
        ("2021_Wrong_Paper.pdf", "identity_flag")]
    # the queue DOI is still something to fetch, and its metadata came from the walk, not the flagged .ris
    assert con.execute("SELECT doi FROM top_candidates").fetchall() == [("10.5555/queue.0005",)]
    assert con.execute("SELECT title FROM paper_metadata WHERE doi = '10.5555/queue.0005'").fetchone() == ("Queue title",)
    assert st["n_flagged"] == 1 and st["n_files"] == 1          # the PDF is still a file on disk


def test_text_only_holding_leaves_top_candidates(tmp_path, lib):
    write(lib, "2015_Morgan_TextOnly.fulltext.json", jats_sidecar("10.5555/text.0002"))
    fwd = write(lib, "_forward_citations.csv",
                "seed_doi,citing_doi,citing_year,citing_title,citing_venue,citing_authors,citing_cited_by\n"
                "10.5555/seed.0001,10.5555/text.0002,2015,T,V,A,3\n"
                "10.5555/seed.0001,10.5555/other.0003,2016,T,V,A,3\n")
    con = _con(tmp_path)
    I.ingest_forward(con, "T", fwd)
    before = {r[0] for r in con.execute("SELECT doi FROM top_candidates").fetchall()}
    I.ingest_papers(con, "T", lib)
    after = {r[0] for r in con.execute("SELECT doi FROM top_candidates").fetchall()}
    assert before == {"10.5555/text.0002", "10.5555/other.0003"} and after == {"10.5555/other.0003"}


def test_n_files_matches_the_instruments_scan(tmp_path, lib):
    """n_files == len(scan.pdf_names) + len(scan.text_only) on a library with every edge case."""
    for n in ("2019_A_Held.pdf", "2019_B_NoRis.pdf", "2019_C_Flagged.pdf", "2019_D_Upper.PDF",
              "2019_E_BadDoi.pdf"):
        (lib / n).write_bytes(b"%PDF-1.4 x")
    write(lib, "2019_A_Held.ris", ris("10.5555/a.0001"))
    write(lib, "2019_C_Flagged.ris", ris("10.5555/c.0003"))
    write(lib, "2019_C_Flagged.identity.json", json.dumps({"identity": "FLAG"}))
    write(lib, "2019_D_Upper.ris", ris("10.5555/d.0004"))
    write(lib, "2019_E_BadDoi.ris", ris("10.1145/nnnnnnn.nnnnnnn"))
    write(lib, "2019_A_Held.fulltext.json", jats_sidecar("10.5555/a.0001", extracted_from_pdf=True))
    write(lib, "2015_F_Text.fulltext.json", jats_sidecar("10.5555/f.0006"))
    write(lib, "2015_G_TextFalse.fulltext.json", jats_sidecar("10.5555/g.0007", has_pdf=False))
    write(lib, "2015_H_TextNoDoi.fulltext.json", jats_sidecar(""))
    write(lib, "2015_I_FlaggedText.fulltext.json", jats_sidecar("10.5555/i.0009"))
    write(lib, "2015_I_FlaggedText.identity.json", json.dumps({"identity": "FLAG"}))
    write(lib, "2015_J_Orphan.fulltext.json", jats_sidecar("10.5555/j.0010", extracted_from_pdf=True))
    (lib / "sub").mkdir()
    (lib / "sub" / "2019_Z_Nested.pdf").write_bytes(b"%PDF-1.4 x")      # not top level
    write(lib, "_forward_citations.csv", "seed_doi,citing_doi\n")
    con = _con(tmp_path)
    st = {}
    n_pdf, n_no = I.ingest_papers(con, "T", lib, stats=st)
    scan = AP.scan_library(lib)
    assert st["n_files"] == len(scan["pdf_names"]) + len(scan["text_only"]) == 5 + 3
    assert (n_pdf, n_no) == (2, 3)
    assert st["n_text_only"] == 3 and st["n_text_only_indexed"] == 2      # H has no DOI to index by
    assert sorted(con.execute("SELECT pdf_filename, reason FROM papers_no_doi").fetchall()) == [
        ("2019_B_NoRis.pdf", "no_ris"), ("2019_C_Flagged.pdf", "identity_flag"),
        ("2019_E_BadDoi.pdf", "ris_doi_invalid")]
    assert sorted(con.execute("SELECT pdf_filename, has_pdf FROM paper_locations").fetchall()) == [
        ("2015_F_Text.fulltext.json", False), ("2015_G_TextFalse.fulltext.json", False),
        ("2019_A_Held.pdf", True), ("2019_D_Upper.PDF", True)]


def test_unreadable_sidecar_is_reported(tmp_path, lib):
    (lib / "2019_A_Held.pdf").write_bytes(b"%PDF-1.4 x")
    write(lib, "2019_A_Held.ris", ris("10.5555/a.0001"))
    write(lib, "2019_A_Held.fulltext.json", "{not json")
    con = _con(tmp_path)
    st = {}
    I.ingest_papers(con, "T", lib, stats=st)
    assert [f for f, _ in st["unreadable"]] == ["2019_A_Held.fulltext.json"]
    assert con.execute("SELECT doi FROM paper_locations").fetchall() == [("10.5555/a.0001",)]


def test_gc_drops_rows_whose_file_is_gone_or_whose_doi_changed(tmp_path, lib):
    (lib / "2025_Url_Form.pdf").write_bytes(b"%PDF-1.4 x")
    write(lib, "2025_Url_Form.ris", ris("https://doi.org/10.5555/jtb.2025.104085"))
    (lib / "2026_Preprint_Then.pdf").write_bytes(b"%PDF-1.4 x")
    write(lib, "2026_Preprint_Then.ris", ris("10.64898/2026.01.01.000001"))
    (lib / "2019_Deleted_Later.pdf").write_bytes(b"%PDF-1.4 x")
    write(lib, "2019_Deleted_Later.ris", ris("10.5555/gone.0001"))
    con = _con(tmp_path)
    I.ingest_papers(con, "T", lib)
    # the URL-form DOI is stored bare at ingest (litpipe.doi), so it never splits
    assert ("10.5555/jtb.2025.104085",) in con.execute("SELECT doi FROM paper_locations").fetchall()
    # the published version replaces the preprint DOI in the same .ris; one PDF is deleted
    write(lib, "2026_Preprint_Then.ris", ris("10.1371/journal.pone.0000001"))
    (lib / "2019_Deleted_Later.pdf").unlink()
    st = {}
    I.ingest_papers(con, "T", lib, stats=st)
    rows = sorted(con.execute("SELECT pdf_filename, doi FROM paper_locations").fetchall())
    assert rows == [("2025_Url_Form.pdf", "10.5555/jtb.2025.104085"),
                    ("2026_Preprint_Then.pdf", "10.1371/journal.pone.0000001")]
    assert (st["gc_doi_changed"], st["gc_file_gone"]) == (1, 1)
    # one row per file: the 2-more-rows-than-PDFs state cannot recur
    assert con.execute("SELECT COUNT(*), COUNT(DISTINCT pdf_filename) FROM paper_locations").fetchone() == (2, 2)


def test_gc_is_per_project(tmp_path, lib):
    (lib / "2019_A_Held.pdf").write_bytes(b"%PDF-1.4 x")
    write(lib, "2019_A_Held.ris", ris("10.5555/a.0001"))
    con = _con(tmp_path)
    I.ingest_papers(con, "P1", lib)
    I.ingest_papers(con, "P2", lib)
    (lib / "2019_A_Held.pdf").unlink()
    I.ingest_papers(con, "P1", lib)
    assert con.execute("SELECT project FROM paper_locations").fetchall() == [("P2",)]


def test_empty_library_clears_its_rows(tmp_path, lib):
    (lib / "2019_A_Held.pdf").write_bytes(b"%PDF-1.4 x")
    write(lib, "2019_A_Held.ris", ris("10.5555/a.0001"))
    con = _con(tmp_path)
    I.ingest_papers(con, "T", lib)
    for p in list(lib.iterdir()):
        p.unlink()
    st = {}
    I.ingest_papers(con, "T", lib, stats=st)
    assert con.execute("SELECT COUNT(*) FROM paper_locations").fetchone()[0] == 0
    assert st["n_files"] == 0 and st["gc_file_gone"] == 1
