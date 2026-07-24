"""Gap3 (2026-07-20 DataCite-gap note): arXiv-by-ID fetch shortcut in preprint_fetch.

Pure-helper tests: the DOI->arXiv-ID extraction and the direct-match construction that lets an
arXiv-DOI seed bypass the fuzzy title search (which NO_MATCHed ~1-in-5 arXiv seeds).
"""
import preprint_fetch as ppr


def test_arxiv_id_extraction():
    assert ppr.arxiv_id("10.48550/arXiv.2103.00020") == "2103.00020"
    assert ppr.arxiv_id("10.48550/ARXIV.2103.00020v2") == "2103.00020v2"   # version + case-insensitive
    assert ppr.arxiv_id("  10.48550/arXiv.2401.12345  ") == "2401.12345"   # whitespace
    assert ppr.arxiv_id("10.48550/arxiv.cs/0303006") == "cs/0303006"       # old-style id
    assert ppr.arxiv_id("10.1234/notarxiv") == ""
    assert ppr.arxiv_id("") == ""
    assert ppr.arxiv_id(None) == ""


def test_arxiv_match_by_doi_builds_direct_match():
    m = ppr.arxiv_match_by_doi("10.48550/arXiv.2103.00020", "A CS Paper")
    assert m == {
        "source": "arxiv-id", "id": "2103.00020", "sim": 1.0, "title": "A CS Paper",
        "doi": "10.48550/arxiv.2103.00020",
        "pdf_url": "https://arxiv.org/pdf/2103.00020.pdf",
    }


def test_arxiv_match_by_doi_none_for_non_arxiv():
    assert ppr.arxiv_match_by_doi("10.1234/journal.article", "T") is None
    assert ppr.arxiv_match_by_doi("", "T") is None
