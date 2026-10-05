"""Gap3 (2026-07-20 DataCite-gap note) and W2-C: arXiv identifiers in preprint_fetch.

Pure-helper tests: the DOI->arXiv-ID extraction, the IDs a queue row can carry (DEC-09: such a row
is looked up on arXiv whatever the project's sources), the version-free base used to batch
`id_list`, and the direct-match construction kept for callers of the old shortcut. The PDF URL is
`arxiv.org/pdf/<id>`, fetched only when projects.json `hosts.arxiv_pdf_allowed` is on.
"""
import pytest

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
        "pdf_url": "https://arxiv.org/pdf/2103.00020",
    }


def test_arxiv_match_by_doi_none_for_non_arxiv():
    assert ppr.arxiv_match_by_doi("10.1234/journal.article", "T") is None
    assert ppr.arxiv_match_by_doi("", "T") is None


@pytest.mark.parametrize("row,want", [
    ({"doi": "10.48550/arXiv.2103.00020"}, "2103.00020"),
    ({"doi": "arXiv:2103.00020v3"}, "2103.00020v3"),
    ({"doi": "https://arxiv.org/abs/hep-ex/0307015v1"}, "hep-ex/0307015v1"),
    ({"doi": "https://arxiv.org/pdf/2401.12345.pdf"}, "2401.12345"),
    ({"doi": "10.1249/mss.1", "arxiv_id": "math.GT/0309136"}, "math.gt/0309136"),
    ({"doi": "10.1249/mss.1"}, ""),
    ({"doi": "10.48550/arXiv.notanid"}, ""),
    ({"doi": ""}, ""),
])
def test_arxiv_id_of_a_queue_row(row, want):
    assert ppr.arxiv_id_of(row) == want


def test_arxiv_base_drops_only_the_version():
    assert ppr.arxiv_base("2103.00020v2") == "2103.00020"
    assert ppr.arxiv_base("hep-ex/0307015v1") == "hep-ex/0307015"
    assert ppr.arxiv_base("2103.00020") == "2103.00020"
