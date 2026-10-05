"""W3a seam locks (dispatcher, 2026-10-05): builders in separate trees against a written contract."""
import index_portfolio
import reverse_citations


def test_every_structured_reverse_source_is_one_the_index_trusts():
    """W3-B writes `source` per row of _reverse_citations_parsed.csv; W3-C1 trusts the structured ones
    (kept whole by its DOI guard) and treats `regex` as text-derived. A new source in one module and
    not the other would silently change how its DOIs are normalised."""
    assert set(reverse_citations.ROW_SOURCES) - {"regex"} == set(index_portfolio.STRUCTURED_SOURCES)
    assert "regex" not in index_portfolio.STRUCTURED_SOURCES
