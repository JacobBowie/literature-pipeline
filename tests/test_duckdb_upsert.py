"""Lock in the paper_metadata UPSERT contract — against the REAL production code.

index_portfolio.ingest_papers refreshes metadata with UPDATE-then-INSERT-if-missing
(NOT `INSERT OR REPLACE`) so that an `abstract` populated separately by
enrich_abstracts.py (CrossRef; ~95 min for ~17k DOIs) is not wiped on re-index.
This is the single most load-bearing invariant in the index.

The previous version of this file tested a test-local schema copy and a
reimplemented UPSERT, never importing index_portfolio, so a switch to
`INSERT OR REPLACE` in production would have passed silently. These tests drive
the real `ingest_papers` twice against a scratch library so the invariant is
actually guarded. Fixture pattern mirrors test_index_prune.py.
"""
import duckdb

import index_portfolio as I


def _con(tmp_path):
    con = duckdb.connect(str(tmp_path / "t.duckdb"))
    con.execute(I.SCHEMA)
    return con


def _lib_with_paper(tmp_path, doi, title):
    lib = tmp_path / "lib"
    lib.mkdir(exist_ok=True)
    (lib / "p.pdf").write_bytes(b"%PDF-1.4 x")
    (lib / "p.ris").write_text(
        f"TY  - JOUR\nDO  - {doi}\nTI  - {title}\nER  - \n", encoding="utf-8")
    return lib


def test_reindex_preserves_enriched_abstract(tmp_path):
    """Re-indexing must NOT clobber a CrossRef-enriched abstract (the crown jewel)."""
    doi = "10.1152/jappl.1972.32.6.812"
    lib = _lib_with_paper(tmp_path, doi, "Predicting rectal temperature")
    con = _con(tmp_path)

    I.ingest_papers(con, "T", lib)  # first index -> abstract is NULL
    # enrich_abstracts.py writes the abstract out-of-band:
    con.execute("UPDATE paper_metadata SET abstract = ? WHERE doi = ?",
                ["ENRICHED_SENTINEL", doi])

    # refresh the .ris with a longer title, then re-index the same library:
    (lib / "p.ris").write_text(
        f"TY  - JOUR\nDO  - {doi}\n"
        "TI  - Predicting rectal temperature response to work and clothing\nER  - \n",
        encoding="utf-8")
    I.ingest_papers(con, "T", lib)

    title, abstract = con.execute(
        "SELECT title, abstract FROM paper_metadata WHERE doi = ?", [doi]).fetchone()
    assert title.endswith("clothing"), "title should be refreshed by the re-index (UPDATE ran)"
    assert abstract == "ENRICHED_SENTINEL", \
        "re-index must preserve the enriched abstract (INSERT OR REPLACE would wipe it)"


def test_new_doi_indexed_with_null_abstract(tmp_path):
    """A freshly-seen DOI gets a row with NULL abstract until enrich_abstracts runs."""
    doi = "10.1234/new.paper"
    lib = _lib_with_paper(tmp_path, doi, "Brand new paper")
    con = _con(tmp_path)

    I.ingest_papers(con, "T", lib)

    row = con.execute(
        "SELECT doi, title, abstract FROM paper_metadata WHERE doi = ?", [doi]).fetchone()
    assert row[0] == doi
    assert row[1] == "Brand new paper"
    assert row[2] is None, "new row should have NULL abstract until enrichment"
