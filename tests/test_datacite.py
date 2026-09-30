"""DataCite resolver + Crossref-first fallback in ris_emit (Batch 2 Gap 1; W2-E1).

Offline. A representative arXiv-shaped DataCite record exercises flatten + build_ris; live-probed
records (tests/fixtures/W2-E1, 2026-09-30) pin the publisher object, the OSF date rule and the
subtitle join; monkeypatched hooks exercise resolve_meta ordering. The dead `is_datacite_doi`
prefix helper (N-A9) is gone with its test.
"""
import json
import re
from datetime import date
from pathlib import Path

import pytest

import ris_emit

FIX = Path(__file__).resolve().parent / "fixtures" / "W2-E1"


def fixture(name):
    return json.loads((FIX / name).read_text(encoding="utf-8"))["body"]


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


def test_is_datacite_doi_is_gone():
    assert not hasattr(ris_emit, "is_datacite_doi")          # N-A9: dead helper deleted
    assert not hasattr(ris_emit, "_DATACITE_PREFIXES")


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
    assert m["container"] == "arXiv"                        # string publisher
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


# ---------------------------------------------------------------- live-probed records (N-A4, N-A5)
def test_publisher_object_writes_its_name_not_a_dict():
    attrs = fixture("datacite_arxiv_publisher_object.json")["data"]["attributes"]
    assert isinstance(attrs["publisher"], dict)             # live 2026-09-30 with publisher=true
    m = ris_emit.datacite_meta(attrs)
    assert m["container"] == "arXiv"
    ris = ris_emit.build_ris(m)
    assert "JO  - arXiv\n" in ris
    assert "{" not in ris and "'name'" not in ris


def test_publisher_object_without_container_on_an_osf_record():
    attrs = fixture("datacite_osf_no_issued.json")["data"]["attributes"]
    m = ris_emit.datacite_meta(attrs)
    assert m["container"] == "OSF Registries"


def test_osf_record_without_issued_leaves_da_empty():
    attrs = fixture("datacite_osf_no_issued.json")["data"]["attributes"]
    assert {d["dateType"] for d in attrs["dates"]} == {"Created", "Updated"}
    m = ris_emit.datacite_meta(attrs)
    assert m["date"] == ""                                  # never the Created timestamp
    assert "DA  -" not in ris_emit.build_ris(m)
    assert m["year"] == "2026"                              # PY still from publicationYear


def test_dc_date_prefers_issued_then_a_past_available_never_created():
    today = date(2026, 9, 30)
    f = ris_emit._dc_date
    assert f([{"dateType": "Created", "date": "2026-09-25"}, {"dateType": "Updated", "date": "2026-09-26"}],
             today) == ""
    assert f([{"dateType": "Available", "date": "2030-09-24"}], today) == ""      # an OSF embargo end
    assert f([{"dateType": "Available", "date": "2026-05"}], today) == "2026/05"
    assert f([{"dateType": "Submitted", "date": "2026-05-28T08:11:57Z"}], today) == ""
    assert f([{"dateType": "Available", "date": "2026-05"}, {"dateType": "Issued", "date": "2026"}],
             today) == "2026"
    assert f([{"dateType": "Issued", "date": "2020-01-01/2020-02-01"}], today) == "2020/01/01"


def test_arxiv_record_keeps_its_issued_date():
    m = ris_emit.datacite_meta(fixture("datacite_arxiv_publisher_object.json")["data"]["attributes"])
    assert m["date"] == "2026" and m["year"] == "2026"


def test_datacite_subtitle_joined_with_colon_space():
    attrs = fixture("datacite_zenodo_subtitles.json")["data"]["attributes"]
    kinds = [t.get("titleType") for t in attrs["titles"]]
    assert kinds.count("Subtitle") == 2 and "Other" in kinds
    m = ris_emit.datacite_meta(attrs)
    main = attrs["titles"][0]["title"]
    first_sub = next(t["title"] for t in attrs["titles"] if t.get("titleType") == "Subtitle")
    assert m["title"] == f"{main}: {first_sub}"             # first Subtitle only; Other ignored


def test_datacite_main_title_is_the_untyped_one():
    attrs = dict(ARXIV_ATTRS, titles=[{"title": "Titre", "titleType": "TranslatedTitle"},
                                      {"title": "Main"}, {"title": "Sub", "titleType": "Subtitle"}])
    assert ris_emit.datacite_meta(attrs)["title"] == "Main: Sub"


def test_datacite_entities_and_tags_are_cleaned():
    attrs = dict(ARXIV_ATTRS, titles=[{"title": "Heat &amp; <i>cold</i>"}],
                 creators=[{"familyName": "M&uuml;ndel", "givenName": "Toby"}],
                 descriptions=[{"descriptionType": "Abstract", "description": "<p>P &lt; 0.05</p>"}])
    m = ris_emit.datacite_meta(attrs)
    assert m["title"] == "Heat & cold"
    assert m["lastname"] == "Mündel"
    assert m["abstract"] == "P < 0.05"


def test_organisational_creator_with_a_comma_is_not_split():
    attrs = dict(ARXIV_ATTRS, creators=[{"name": "University of X, Heat Lab", "nameType": "Organizational"}])
    assert ris_emit.datacite_meta(attrs)["authors"] == [{"family": "University of X, Heat Lab", "given": ""}]


# ---------------------------------------------------------------- resolve_meta ordering
def test_resolve_meta_crossref_wins_and_short_circuits(monkeypatch):
    monkeypatch.setattr(ris_emit, "crossref_by_doi", lambda doi, **k: {"ok": 1})
    monkeypatch.setattr(ris_emit, "crossref_meta", lambda msg: {"title": "Journal Paper"})
    hit = []
    monkeypatch.setattr(ris_emit, "datacite_by_doi", lambda doi, **k: hit.append(doi))
    monkeypatch.setattr(ris_emit, "doi_ra", lambda doi: hit.append(("ra", doi)))
    meta, src = ris_emit.resolve_meta("10.1152/japplphysiol.00100.2024")
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
    asked = []
    monkeypatch.setattr(ris_emit, "doi_ra", lambda doi: asked.append(doi))   # "DOI does not exist"
    assert ris_emit.resolve_meta("10.9999/nope.123") == ({}, "none")
    assert asked == ["10.9999/nope.123"]


def test_resolve_meta_not_a_doi_sends_nothing(monkeypatch):
    def boom(*a, **k):
        raise AssertionError("no lookup for a non-DOI")
    for name in ("crossref_by_doi", "datacite_by_doi", "doi_ra", "csl_by_doi"):
        monkeypatch.setattr(ris_emit, name, boom)
    assert ris_emit.resolve_meta("") == ({}, "none")
    assert ris_emit.resolve_meta("not a doi") == ({}, "none")
