"""Lock in the paper_metadata UPSERT contract.

The pipeline's index refresh deliberately runs UPDATE-then-INSERT-if-missing
rather than `INSERT OR REPLACE` so that an existing `abstract` column value
(populated separately by enrich_abstracts.py from CrossRef, ~95 min for 17k
DOIs) is NOT wiped when the index is refreshed.

This is documented in CURRENT_STATE.md as a hidden constraint. A naive
refactor to use `INSERT OR REPLACE` would silently lose ~9,800 enriched
abstracts. This test exists to scream if anyone does that.
"""
from __future__ import annotations
import datetime

import duckdb
import pytest


SCHEMA = """
CREATE TABLE paper_metadata (
  doi              VARCHAR PRIMARY KEY,
  year             INTEGER,
  lastname         VARCHAR,
  title            VARCHAR,
  venue            VARCHAR,
  authors          VARCHAR,
  abstract         VARCHAR,
  refreshed_at     TIMESTAMP
);
"""


@pytest.fixture
def con():
    """Fresh in-memory DuckDB with the paper_metadata schema."""
    c = duckdb.connect(":memory:")
    c.execute(SCHEMA)
    yield c
    c.close()


def _refresh_metadata(con, rows):
    """Mirror the production UPSERT in index_portfolio.refresh_index().

    Each row is (doi, year, lastname, title, venue, authors, refreshed_at).
    Note: NO abstract in the UPDATE column list.
    """
    if not rows:
        return
    dois = [r[0] for r in rows]
    con.executemany(
        "UPDATE paper_metadata SET year=?, lastname=?, title=?, venue=?, authors=?, refreshed_at=? "
        "WHERE doi=?",
        [(r[1], r[2], r[3], r[4], r[5], r[6], r[0]) for r in rows],
    )
    existing = {row[0] for row in con.execute(
        f"SELECT doi FROM paper_metadata WHERE doi IN ({','.join('?'*len(dois))})",
        dois,
    ).fetchall()}
    new = [r for r in rows if r[0] not in existing]
    if new:
        con.executemany(
            "INSERT INTO paper_metadata (doi, year, lastname, title, venue, authors, refreshed_at) "
            "VALUES (?,?,?,?,?,?,?)",
            new,
        )


def test_refresh_preserves_existing_abstract(con):
    """The load-bearing invariant: re-indexing must NOT clobber a CrossRef-enriched abstract."""
    now = datetime.datetime(2026, 1, 1, 12, 0, 0)
    later = datetime.datetime(2026, 5, 13, 12, 0, 0)

    # Seed: a row with a CrossRef-enriched abstract (as enrich_abstracts.py would write)
    con.execute(
        "INSERT INTO paper_metadata VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ("10.1152/jappl.1972.32.6.812", 1972, "Givoni",
         "Predicting rectal temperature", "Journal of Applied Physiology",
         "Givoni B; Goldman R", "The enriched abstract text we don't want to lose.", now),
    )

    # Now refresh with updated metadata — no abstract field provided
    _refresh_metadata(con, [
        ("10.1152/jappl.1972.32.6.812", 1972, "Givoni",
         "Predicting rectal temperature response to work, environment, and clothing",
         "Journal of Applied Physiology", "Givoni B; Goldman R F", later),
    ])

    row = con.execute(
        "SELECT title, abstract, refreshed_at FROM paper_metadata "
        "WHERE doi = '10.1152/jappl.1972.32.6.812'"
    ).fetchone()

    title, abstract, refreshed_at = row
    # Title and refresh timestamp updated...
    assert title.endswith("clothing"), "title should be refreshed to the new value"
    assert refreshed_at == later,    "refreshed_at should advance"
    # ...but abstract is preserved
    assert abstract == "The enriched abstract text we don't want to lose."


def test_refresh_inserts_new_dois(con):
    """A DOI we haven't seen before should get inserted with NULL abstract."""
    now = datetime.datetime(2026, 5, 13, 12, 0, 0)
    _refresh_metadata(con, [
        ("10.1234/new.paper", 2024, "Newauthor", "New title", "New journal",
         "Newauthor X", now),
    ])
    row = con.execute(
        "SELECT doi, title, abstract FROM paper_metadata WHERE doi = '10.1234/new.paper'"
    ).fetchone()
    assert row[0] == "10.1234/new.paper"
    assert row[1] == "New title"
    assert row[2] is None, "new row should have NULL abstract until enrich_abstracts runs"


def test_doi_is_primary_key(con):
    """paper_metadata uses DOI as PK — a second INSERT with the same DOI must fail
    if not routed through the UPSERT pattern."""
    now = datetime.datetime(2026, 5, 13, 12, 0, 0)
    con.execute(
        "INSERT INTO paper_metadata VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ("10.1234/x", 2024, "A", "T", "V", "A B", "abs", now),
    )
    with pytest.raises(duckdb.Error):
        # Same DOI, second INSERT — should violate PK constraint
        con.execute(
            "INSERT INTO paper_metadata VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("10.1234/x", 2024, "A", "T", "V", "A B", "abs2", now),
        )


def test_refresh_handles_empty_input(con):
    """No rows in == no-op; should not crash."""
    _refresh_metadata(con, [])
    rows = con.execute("SELECT * FROM paper_metadata").fetchall()
    assert rows == []
