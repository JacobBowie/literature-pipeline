"""Batch 2 (Gap 1): DataCite resolver + CrossRef-first fallback in ris_emit.

Offline (the pipeline CI has no network). A representative arXiv-shaped DataCite
record exercises the flatten + build_ris path; monkeypatched crossref/datacite hooks
exercise resolve_meta ordering. The LIVE integration check lives in the FRED prototype
(FRED/tools/datacite_ris/test_datacite_ris.py, 20/20).
"""
import re
import ris_emit

# arXiv-shaped DataCite `attributes` (the dict datacite_by_doi returns).
ARXIV_ATTRS = {
    "doi": "10.48550/arxiv.2506.02153",
    "titles": [{"title": "A Study of\nOn-Device Inference"}],
    "creators": [
        {"familyName": "Smith", "givenName": "Jane"},
        {"familyName": "Doe", "givenName": "John A."},
        {"name": "Roe, Richard"},     # name-only "Family, Given"
        {"name": "OpenAI"},           # organizational mononym
    ],
    "descriptions": [
        {"descriptionType": "Abstract", "description": "We present a method.\n\nResults follow."},
        {"descriptionType": "Other", "description": "ignore me"},
    ],
    "publicationYear": 2025,
    "dates": [{"dateType": "Issued", "date": "2025-06-02"}],
    "publisher": "arXiv",
    "types": {"resourceTypeGeneral": "Preprint"},
    "url": "https://arxiv.org/abs/2506.02153",
}


def test_is_datacite_doi():
    assert ris_emit.is_datacite_doi("10.48550/arXiv.2506.02153")
    assert ris_emit.is_datacite_doi("10.5281/zenodo.123")
    assert not ris_emit.is_datacite_doi("10.1152/japplphysiol.00100.2024")
    assert not ris_emit.is_datacite_doi("")


def test_datacite_meta_flatten():
    m = ris_emit.datacite_meta(ARXIV_ATTRS)
    assert m["title"] == "A Study of On-Device Inference"   # whitespace collapsed
    assert m["year"] == "2025"
    assert m["date"] == "2025/06/02"
    assert m["type"] == "posted-content"                    # Preprint
    assert m["doi"] == "10.48550/arxiv.2506.02153"
    fams = [a["family"] for a in m["authors"]]
    assert fams == ["Smith", "Doe", "Roe", "OpenAI"]        # incl. name-only + mononym
    assert m["authors"][2]["given"] == "Richard"            # "Roe, Richard" split
    assert "We present a method." in m["abstract"]
    assert ris_emit.datacite_meta({}) == {}
    assert ris_emit.datacite_meta(None) == {}


def test_datacite_meta_feeds_build_ris():
    ris = ris_emit.build_ris(ris_emit.datacite_meta(ARXIV_ATTRS))
    for line in ris.splitlines():
        if line.strip():
            assert re.match(r"^[A-Z][A-Z0-9]  - ", line), f"orphan RIS line: {line!r}"
    assert "TY  - UNPD" in ris                              # posted-content -> UNPD
    assert "AU  - Smith, Jane" in ris
    assert "AU  - OpenAI" in ris                            # mononym, no comma
    assert "TI  - A Study of On-Device Inference" in ris    # newline scrubbed by _ris_val
    assert "DO  - 10.48550/arxiv.2506.02153" in ris


def test_resolve_meta_crossref_wins_and_short_circuits(monkeypatch):
    monkeypatch.setattr(ris_emit, "crossref_by_doi", lambda doi, **k: {"ok": 1})
    monkeypatch.setattr(ris_emit, "crossref_meta", lambda msg: {"title": "Journal Paper"})
    hit = []
    monkeypatch.setattr(ris_emit, "datacite_by_doi", lambda doi, **k: hit.append(doi))
    meta, src = ris_emit.resolve_meta("10.1152/japplphysiol.x")
    assert src == "crossref" and meta["title"] == "Journal Paper"
    assert hit == []                                        # DataCite NOT hit when CrossRef wins


def test_resolve_meta_datacite_fallback(monkeypatch):
    monkeypatch.setattr(ris_emit, "crossref_by_doi", lambda doi, **k: None)
    monkeypatch.setattr(ris_emit, "datacite_by_doi", lambda doi, **k: ARXIV_ATTRS)
    meta, src = ris_emit.resolve_meta("10.48550/arxiv.2506.02153")
    assert src == "datacite" and meta["title"] == "A Study of On-Device Inference"


def test_resolve_meta_none(monkeypatch):
    monkeypatch.setattr(ris_emit, "crossref_by_doi", lambda doi, **k: None)
    monkeypatch.setattr(ris_emit, "datacite_by_doi", lambda doi, **k: None)
    assert ris_emit.resolve_meta("10.99/nope") == ({}, "none")
