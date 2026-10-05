"""W3-C1: ingest cleanup (T8 steps 3 and 7, amendment 6) and the co-citation view.

- DOIs are normalised at ingest through litpipe.doi: text-derived DOIs take normalise() (tails
  such as `.url` peeled); structured DOIs keep their whole form when litpipe.doi.candidates()
  offers it (normalise() alone shortens real DOIs such as `10.1089/ther.2017.29031.mkb`);
  placeholders and truncations are dropped.
- seed_doi_for prefers the reverse CSV's seed_doi column, then the current file's .ris, then ''.
  With the column, renaming a PDF keeps its edges.
- coerce_int replaces safe_int; ingest_forward's `lib` is optional.
- paper_metadata is refreshed set-based (TEMP table + UPDATE ... FROM), never row by row.
- project_cocitations ranks a project's missing DOIs by how many of its own papers cite them.
"""
import duckdb
import pytest

import index_portfolio as I

FWD_HDR = "seed_doi,citing_doi,citing_year,citing_title,citing_venue,citing_authors,citing_cited_by\n"
REV_HDR = "seed,first_author,year,title_snippet,doi,raw"


def _con(tmp_path):
    con = duckdb.connect(str(tmp_path / "t.duckdb"))
    con.execute(I.SCHEMA)
    return con


def ris(doi, title="T", py="2019", au="Smith, Jane"):
    return f"TY  - JOUR\nAU  - {au}\nPY  - {py}\nTI  - {title}\nDO  - {doi}\nER  - \n"


# ---------------------------------------------------------------- normalisation
@pytest.mark.parametrize("raw,want", [
    ("10.1089/ther.2017.29031.mkb", "10.1089/ther.2017.29031.mkb"),        # real letter tails, kept whole
    ("10.1088/2053-1591/acdecd", "10.1088/2053-1591/acdecd"),
    ("10.1149/2.0381907jss", "10.1149/2.0381907jss"),
    ("https://doi.org/10.1016/J.JTHERBIO.2025.104085", "10.1016/j.jtherbio.2025.104085"),
    ("doi:10.1007/s00421-012-2453-7.", "10.1007/s00421-012-2453-7"),
    ("10.1056/nejmc1113675#sa3", "10.1056/nejmc1113675"),                  # a URL fragment is cut
    ("10.1145/nnnnnnn.nnnnnnn", None),                                     # ACM template placeholder
    ("10.1016/j.amepre", None),                                            # a cut journal code
    ("", None), (None, None),
])
def test_norm_doi_structured(raw, want):
    assert I.norm_doi(raw) == want


@pytest.mark.parametrize("raw,want", [
    ("10.48550/arxiv.2312.17180.url", "10.48550/arxiv.2312.17180"),       # reference-text junk peeled
    ("10.1249/mss.0b013e31818cb278.pubmed", "10.1249/mss.0b013e31818cb278"),
    ("10.1126/science.aax1566.publisher", "10.1126/science.aax1566"),
    ("10.1145/nnnnnnn.nnnnnnn", None),
])
def test_norm_doi_text(raw, want):
    assert I.norm_doi(raw, structured=False) == want


def test_forward_ingest_normalises_and_drops_placeholders(tmp_path):
    csv = tmp_path / "_forward_citations.csv"
    csv.write_text(FWD_HDR
                   + "https://doi.org/10.5555/SEED.0001,10.6666/CITE.0001,2020,T,V,A,\"1,234\"\n"
                   + "10.5555/seed.0001,10.1145/nnnnnnn.nnnnnnn,2020,T,V,A,3\n"
                   + "10.5555/seed.0001,10.1089/ther.2017.29031.mkb,2017,T,V,A,2.0\n", encoding="utf-8")
    con = _con(tmp_path)
    assert I.ingest_forward(con, "T", csv) == 2                    # `lib` is optional (unused)
    assert sorted(con.execute("SELECT doi, source_seed_doi, citing_cited_by FROM candidates").fetchall()) == [
        ("10.1089/ther.2017.29031.mkb", "10.5555/seed.0001", 2),
        ("10.6666/cite.0001", "10.5555/seed.0001", 1234)]           # coerce_int: '1,234' -> 1234 (safe_int gave 0)
    assert not con.execute("SELECT * FROM paper_metadata WHERE doi LIKE '%nnnn%'").fetchall()
    assert not hasattr(I, "safe_int")


def test_reverse_ingest_reads_the_source_column(tmp_path):
    lib = tmp_path / "lib"
    lib.mkdir()
    (lib / "2019_Seed.pdf").write_bytes(b"%PDF-1.4 x")
    (lib / "2019_Seed.ris").write_text(ris("10.5555/seed.0001"), encoding="utf-8")
    csv = lib / "_reverse_citations_parsed.csv"
    csv.write_text(REV_HDR + ",seed_doi,source\n"
                   + "2019_Seed.pdf,A,2017,T,10.1089/ther.2017.29031.mkb,raw,10.5555/seed.0001,crossref\n"
                   + "2019_Seed.pdf,B,2023,T,10.48550/arxiv.2312.17180.url,raw,10.5555/seed.0001,regex\n"
                   + "2019_Seed.pdf,C,2020,T,10.7777/ref.0003.pubmed,raw,10.5555/seed.0001,\n", encoding="utf-8")
    con = _con(tmp_path)
    I.ingest_reverse(con, "T", csv, lib)
    assert sorted(r[0] for r in con.execute("SELECT doi FROM candidates").fetchall()) == [
        "10.1089/ther.2017.29031.mkb", "10.48550/arxiv.2312.17180", "10.7777/ref.0003"]


# ---------------------------------------------------------------- seed_doi_for (T8 step 3)
def test_seed_doi_for_prefers_the_column_then_the_ris(tmp_path):
    lib = tmp_path / "lib"
    lib.mkdir()
    (lib / "2019_Seed.ris").write_text(ris("10.5555/from.ris.0001"), encoding="utf-8")
    assert I.seed_doi_for({"seed": "2019_Seed.pdf", "seed_doi": "10.5555/FROM.CSV.0002"}, lib) == "10.5555/from.csv.0002"
    assert I.seed_doi_for({"seed": "2019_Seed.pdf", "seed_doi": ""}, lib) == "10.5555/from.ris.0001"
    assert I.seed_doi_for({"seed": "2019_Seed.txt"}, lib) == "10.5555/from.ris.0001"           # legacy shape
    assert I.seed_doi_for({"seed": "2019_Seed.pdf", "seed_doi": "10.1016/j.amepre"}, lib) == "10.5555/from.ris.0001"
    assert I.seed_doi_for({"seed": "2019_Renamed.pdf"}, lib) == ""


@pytest.mark.parametrize("with_column", [True, False])
def test_renaming_a_pdf_keeps_its_edges_when_the_csv_names_the_seed_doi(tmp_path, with_column):
    """The walker CSV still names the OLD file after a rename (it is rewritten only by the next
    walk). With W3-B's seed_doi column the edges survive; the legacy shape loses them (the seed is
    found by file name only), which is why the column was added."""
    lib = tmp_path / "lib"
    lib.mkdir()
    (lib / "2019_Old_Name.pdf").write_bytes(b"%PDF-1.4 x")
    (lib / "2019_Old_Name.ris").write_text(ris("10.5555/seed.0001"), encoding="utf-8")
    hdr = REV_HDR + (",seed_doi,source\n" if with_column else "\n")
    tail = ",10.5555/seed.0001,regex" if with_column else ""
    csv = lib / "_reverse_citations_parsed.csv"
    csv.write_text(hdr + "".join(f"2019_Old_Name.pdf,A,2001,T,10.7777/ref.000{i},raw{tail}\n" for i in (1, 2)),
                   encoding="utf-8")
    con = _con(tmp_path)
    I.ingest_papers(con, "T", lib)
    I.ingest_reverse(con, "T", csv, lib)
    edges = sorted(con.execute("SELECT citing_doi, cited_doi FROM cites").fetchall())
    assert edges == [("10.5555/seed.0001", "10.7777/ref.0001"), ("10.5555/seed.0001", "10.7777/ref.0002")]
    for ext in (".pdf", ".ris"):                                   # a cascade rename moves both
        (lib / f"2019_Old_Name{ext}").rename(lib / f"2019_New_Name{ext}")
    I.ingest_papers(con, "T", lib)
    I.ingest_reverse(con, "T", csv, lib)
    after = sorted(con.execute("SELECT citing_doi, cited_doi FROM cites").fetchall())
    assert con.execute("SELECT pdf_filename FROM paper_locations").fetchall() == [("2019_New_Name.pdf",)]
    assert after == (edges if with_column else [])


# ---------------------------------------------------------------- set-based metadata refresh
class Recorder:
    """A connection proxy that records every execute/executemany (DuckDB's own methods cannot be
    patched)."""
    def __init__(self, con):
        self._con, self.sql, self.many = con, [], []

    def execute(self, sql, *a, **k):
        self.sql.append(" ".join(sql.split()))
        return self._con.execute(sql, *a, **k)

    def executemany(self, sql, *a, **k):
        self.many.append(sql)
        return self._con.executemany(sql, *a, **k)

    def __getattr__(self, name):
        return getattr(self._con, name)


def test_metadata_refresh_is_set_based_and_keeps_abstracts(tmp_path):
    lib = tmp_path / "lib"
    lib.mkdir()
    for i in range(5):
        (lib / f"p{i}.pdf").write_bytes(b"%PDF-1.4 x")
        (lib / f"p{i}.ris").write_text(ris(f"10.5555/m.000{i}", title=f"Old {i}", py="2001"), encoding="utf-8")
    con = _con(tmp_path)
    I.ingest_papers(con, "T", lib)
    con.execute("UPDATE paper_metadata SET abstract = 'ENRICHED', abstract_attempted_at = TIMESTAMP '2026-01-01'")
    for i in range(5):                                             # titles change; two years and a name change
        (lib / f"p{i}.ris").write_text(ris(f"10.5555/m.000{i}", title=f"New {i}", py="2002" if i < 2 else "2001",
                                           au="Jones, Al" if i == 4 else "Smith, Jane"), encoding="utf-8")
    rec = Recorder(con)
    I.ingest_papers(rec, "T", lib)
    assert rec.many == []                                          # no per-row executemany UPDATE
    assert any(s.startswith("UPDATE paper_metadata AS m SET title") and "FROM _meta_upd" in s for s in rec.sql)
    rows = con.execute("SELECT doi, title, year, lastname, abstract, abstract_attempted_at IS NOT NULL "
                       "FROM paper_metadata ORDER BY doi").fetchall()
    assert rows == [(f"10.5555/m.000{i}", f"New {i}", 2002 if i < 2 else 2001, "Jones" if i == 4 else "Smith",
                     "ENRICHED", True) for i in range(5)]
    assert "_meta_upd" not in I._existing_tables(con)


def test_citation_metadata_insert_is_an_anti_join(tmp_path):
    con = _con(tmp_path)
    con.execute("INSERT INTO paper_metadata (doi, title, abstract) VALUES ('10.5555/x.0001', 'Rich', 'ABS')")
    rec = Recorder(con)
    I._write_citation_rows(rec, [], [("10.5555/x.0001", 2020, "", "Thin", "", "", "2026-07-20"),
                                     ("10.5555/x.0002", 2021, "", "New", "", "", "2026-07-20")], [])
    assert not any(" IN (?" in s for s in rec.sql)                 # no giant IN-list of placeholders
    assert con.execute("SELECT doi, title, abstract FROM paper_metadata ORDER BY doi").fetchall() == [
        ("10.5555/x.0001", "Rich", "ABS"), ("10.5555/x.0002", "New", None)]


# ---------------------------------------------------------------- co-citations
def test_project_cocitations_view(tmp_path):
    con = _con(tmp_path)
    loc = [("10.5555/own.0001", "research_a", True), ("10.5555/own.0002", "research_a", True),
           ("10.5555/text.0003", "research_a", False), ("10.5555/held.0009", "research_b", True)]
    con.executemany("INSERT INTO paper_locations (doi, project, has_pdf) VALUES (?, ?, ?)", loc)
    edges = [("10.5555/own.0001", "10.7777/missing.0001", "reverse", "research_a"),
             ("10.5555/own.0002", "10.7777/missing.0001", "reverse", "research_a"),
             ("10.5555/own.0002", "10.7777/missing.0001", "forward", "research_b"),   # same pair, other project
             ("10.5555/own.0001", "10.5555/text.0003", "reverse", "research_a"),      # held text-only: not missing
             ("10.5555/own.0001", "10.5555/own.0002", "reverse", "research_a"),       # held PDF: not missing
             ("10.5555/own.0001", "10.5555/held.0009", "reverse", "research_a"),      # held elsewhere only
             ("10.5555/own.0001", "10.5555/own.0001", "reverse", "research_a"),       # self-citation
             ("10.9999/stranger.0001", "10.7777/missing.0002", "forward", "research_a")]  # citer not held
    con.executemany("INSERT INTO cites VALUES (?, ?, ?, ?)", edges)
    assert I.project_cocitations(con, "research_a") == [
        ("research_a", "10.7777/missing.0001", 2, False),
        ("research_a", "10.5555/held.0009", 1, True)]
    assert I.project_cocitations(con, "research_a", limit=1) == [("research_a", "10.7777/missing.0001", 2, False)]
    cols = [r[0] for r in con.execute("DESCRIBE project_cocitations").fetchall()]
    assert cols == ["project", "doi", "n_own_citing", "held_anywhere"]
    assert con.execute("SELECT data_type FROM information_schema.columns WHERE table_name = 'project_cocitations' "
                       "AND column_name = 'held_anywhere'").fetchone() == ("BOOLEAN",)
    assert I.project_cocitations(con, "research_b") == []
