"""Tests for fill_missing_dois.py — filename parsing, type filtering, scoring.

Network calls (crossref_query, run_project, main) are not exercised here.
Those are covered by manual smoke runs documented in
notes/2026-05-21_scope_fill_missing_dois.md.
"""
import json
import os
import tempfile

import pytest

from fill_missing_dois import (
    BUNDLE_RE,
    YEAR_RE,
    build_query_string,
    discover_orphans,
    extract_metadata,
    filter_by_type,
    is_bundled_issue,
    load_sidecar,
    normalize_for_match,
    parse_filename_hints,
    score_match,
    sidecar_title_hint,
    update_sidecar,
)


# ---------- regex word-boundary regressions ----------
# These caught real bugs on 2026-05-21: Python's `\b` doesn't fire next to
# `_` because `_` is a word character. Lock the underscore-friendly
# replacements in.

@pytest.mark.parametrize("fn,want", [
    ("2007_IJCSS_Vol6_Edition2.pdf", "2007"),
    ("2002_IJCSS_Vol1_Edition1.pdf", "2002"),
    ("1976_Calvert_SystemsModel.pdf", "1976"),
    ("nofileyearhere.pdf", None),
])
def test_year_re_handles_underscore_boundaries(fn, want):
    m = YEAR_RE.search(fn)
    assert (m.group(0) if m else None) == want


@pytest.mark.parametrize("fn,want", [
    ("2007_IJCSS_Vol6_Edition2.pdf", True),
    ("2002_IJCSS_Vol1_Edition1.pdf", True),
    ("2003_IJCSS_Vol2_Issue3.pdf",   True),
    ("1976_Calvert_SystemsModel.pdf", False),
    ("2006_Hellard_Banister_Limitations.pdf", False),  # not a bundle
])
def test_bundle_re_handles_underscore_boundaries(fn, want):
    assert is_bundled_issue(fn, "") is want


# ---------- parse_filename_hints ----------

@pytest.mark.parametrize("fn,want", [
    # canonical strict (YYYY_LastName_TitleSlug.pdf)
    ("1976_Calvert_SystemsModelEffectsTrainingPhysicalPerformance.pdf",
     ("1976", "Calvert", "canonical")),
    ("1991_Fitz-Clarke_OptimizingAthleticPerformanceInfluenceCurves.pdf",
     ("1991", "Fitz-Clarke", "canonical")),
    # year=0000 → empty year (NO_YEAR semantics)
    ("0000_Bishop_SystemsModelOfTrainingResponses.pdf",
     ("", "Bishop", "canonical")),
    # canonical-loose (multi-segment after author)
    ("2006_Hellard_Banister_Limitations.pdf",
     ("2006", "Hellard", "canonical-loose")),
    ("2008_Pfeiffer_FFM_PerPot_AntagonisticComparison_IJCSS.pdf",
     ("2008", "Pfeiffer", "canonical-loose")),
    # legacy lowercase_year_title
    ("schweiker_2016_comf.pdf",
     ("2016", "Schweiker", "legacy")),
    ("akavian_2025_bsa_htt.pdf",
     ("2025", "Akavian", "legacy")),
])
def test_parse_filename_hints_class(fn, want):
    y, a, _t, k = parse_filename_hints(fn)
    assert (y, a, k) == want


def test_parse_filename_hints_canonical_title_words():
    _y, _a, t, _k = parse_filename_hints(
        "1976_Calvert_SystemsModelEffectsTrainingPhysicalPerformance.pdf")
    assert "Systems Model Effects Training Physical Performance" in t


def test_parse_filename_hints_legacy_title_underscores():
    _y, _a, t, _k = parse_filename_hints("akavian_2025_bsa_htt.pdf")
    assert t == "bsa htt"


def test_parse_filename_hints_canonical_loose_recovers_title_tokens():
    _y, _a, t, _k = parse_filename_hints(
        "2006_Hellard_Banister_Limitations.pdf")
    assert "Banister" in t and "Limitations" in t


# ---------- normalize_for_match (accent folding) ----------

@pytest.mark.parametrize("a,b", [
    ("Périard", "Periard"),
    ("Müller",  "Muller"),
    ("Mølmen",  "Molmen"),
    ("LÜTHI",   "luthi"),
])
def test_normalize_for_match_collapses_accents_and_case(a, b):
    assert normalize_for_match(a) == normalize_for_match(b)


# ---------- type filter ----------

def _item(score, doi, typ):
    return {"score": score, "DOI": doi, "type": typ,
            "title": [], "author": [], "issued": {}, "container-title": []}


def test_filter_by_type_preferred_and_demoted():
    items = [
        _item(100, "10.1/dataset",  "dataset"),
        _item(80,  "10.1/article",  "journal-article"),
        _item(60,  "10.1/chapter",  "book-chapter"),
        _item(50,  "10.1/review",   "peer-review"),
        _item(40,  "10.1/unknown",  "weird-new-thing"),
    ]
    pref, dem = filter_by_type(items)
    assert [x["DOI"] for x in pref] == ["10.1/article", "10.1/chapter", "10.1/unknown"]
    assert [x["DOI"] for x in dem]  == ["10.1/dataset", "10.1/review"]


# ---------- scorer ----------

def _full(score, doi, typ, title, family, year):
    return {
        "score": score, "DOI": doi, "type": typ,
        "title": [title], "author": [{"family": family, "given": ""}] if family else [],
        "issued": {"date-parts": [[int(year)]]} if year else {},
        "container-title": ["J"],
    }


def test_score_high():
    items = [
        _full(120, "10.1/a", "journal-article", "T", "Calvert", "1976"),
        _full(50,  "10.1/b", "journal-article", "T", "Other",   "1980"),
    ]
    label, top, _, _ = score_match("1976", "Calvert", items, items)
    assert label == "HIGH"
    assert top["doi"] == "10.1/a"


def test_score_near_twin_collapse():
    """Two near-duplicate CrossRef records (same first_family + year) should
    collapse, not trigger AMBIG even when scores are within 1.10."""
    items = [
        _full(100.05, "10.1/a", "journal-article", "T", "Hoeger", "1990"),
        _full(100.00, "10.1/b", "journal-article", "T", "Hoeger", "1990"),
    ]
    label, _, _, _ = score_match("1990", "Hoeger", items, items)
    assert label == "HIGH"


def test_score_ambig_real():
    """Distinct authors with near-identical scores should remain AMBIG."""
    items = [
        _full(100, "10.1/a", "journal-article", "T", "Stone",  "1994"),
        _full(95,  "10.1/b", "journal-article", "T", "Nieman", "1995"),
    ]
    label, _, _, _ = score_match("1995", "Nieman", items, items)
    assert label == "AMBIG"


def test_score_med_no_year():
    items = [
        _full(120, "10.1/a", "journal-article", "T", "Bishop", "2003"),
        _full(50,  "10.1/b", "journal-article", "T", "Other",  "2003"),
    ]
    label, _, _, _ = score_match("", "Bishop", items, items)
    assert label == "MED_NO_YEAR"


def test_score_med_author_year_mismatch():
    items = [
        _full(120, "10.1/a", "journal-article", "T", "Bishop", "2008"),
        _full(50,  "10.1/b", "journal-article", "T", "Other",  "2007"),
    ]
    label, _, _, _ = score_match("1976", "Bishop", items, items)
    assert label == "MED_AUTHOR_YEAR_MISMATCH"


def test_score_med_type_mismatch():
    """Only demoted types in raw results → MED_TYPE_MISMATCH."""
    raw = [_full(150, "10.1/a", "dataset", "T", "X", "2020")]
    pref, _ = filter_by_type(raw)
    label, _, _, _ = score_match("2020", "X", pref, raw)
    assert label == "MED_TYPE_MISMATCH"


def test_score_med_title_strong():
    """No author in record + ratio >= 1.20 + year matches → MED_TITLE_STRONG."""
    items = [
        _full(150, "10.1/a", "journal-article", "T", "", "2003"),
        _full(70,  "10.1/b", "journal-article", "T", "Other", "2003"),
    ]
    label, _, _, _ = score_match("2003", "Bishop", items, items)
    assert label == "MED_TITLE_STRONG"


def test_score_no_result():
    label, top, _, _ = score_match("2020", "X", [], [])
    assert label == "NO_RESULT"
    assert top is None


def test_score_accent_insensitive_author_match():
    items = [
        _full(120, "10.1/a", "journal-article", "T", "Périard", "2020"),
        _full(50,  "10.1/b", "journal-article", "T", "Other",   "2020"),
    ]
    label, _, _, _ = score_match("2020", "Periard", items, items)
    assert label == "HIGH"


# ---------- build_query_string ----------

@pytest.mark.parametrize("year,author,title,want", [
    ("1976", "Calvert", "Systems Model", "Calvert Systems Model 1976"),
    ("",     "Bishop",  "Sprint",        "Bishop Sprint"),
    ("1976", None,      "Title",         "Title 1976"),
    ("",     None,      "",              ""),
])
def test_build_query_string(year, author, title, want):
    assert build_query_string(year, author, title) == want


# ---------- sidecar I/O ----------

def test_load_sidecar_returns_none_on_bad_json(tmp_path):
    p = tmp_path / "broken.fulltext.json"
    p.write_text("not json {", encoding="utf-8")
    assert load_sidecar(str(p)) is None


def test_load_sidecar_returns_none_on_missing(tmp_path):
    assert load_sidecar(str(tmp_path / "nope.fulltext.json")) is None


def test_update_sidecar_writes_doi_and_preserves_existing(tmp_path):
    """Writes doi, year, title, journal when empty.
    Preserves existing values; does NOT clobber."""
    p = tmp_path / "x.fulltext.json"
    pre = {
        "doi": "",
        "title": "Pre-existing curated title",  # should be preserved
        "year": "",
        "journal": "",
        "authors": [],
        "text": "long pre-existing extracted text",
        "sections": [{"heading": "Intro", "text": "..."}],
        "figures": [{"image_path": "x.fig1.png"}],
    }
    p.write_text(json.dumps(pre), encoding="utf-8")
    match = {
        "doi": "10.1/new",
        "title": "CrossRef Title",
        "year": "2020",
        "journal": "CrossRef Journal",
        "authors": [{"surname": "X", "given": "Y", "source": "crossref"}],
    }
    update_sidecar(str(p), pre, match)
    after = json.loads(p.read_text(encoding="utf-8"))
    assert after["doi"] == "10.1/new"
    assert after["title"] == "Pre-existing curated title"  # preserved
    assert after["year"] == "2020"  # was empty → set
    assert after["journal"] == "CrossRef Journal"  # was empty → set
    assert after["authors"][0]["surname"] == "X"  # was empty list → set
    # heavy fields preserved
    assert after["text"].startswith("long pre-existing")
    assert after["sections"][0]["heading"] == "Intro"
    assert after["figures"][0]["image_path"] == "x.fig1.png"


def test_update_sidecar_overwrites_doi_when_present_but_other_fields_protected(tmp_path):
    """If caller passes a sidecar with non-empty title/year/journal/authors,
    update_sidecar must overwrite ONLY doi (which is always overwritten)."""
    p = tmp_path / "x.fulltext.json"
    pre = {
        "doi": "10.1/old",
        "title": "Existing",
        "year": "1999",
        "journal": "Old J",
        "authors": [{"surname": "Pre", "given": "", "source": "manual"}],
    }
    p.write_text(json.dumps(pre), encoding="utf-8")
    match = {"doi": "10.1/new", "title": "CR", "year": "2020",
             "journal": "CR J", "authors": [{"surname": "Crref"}]}
    update_sidecar(str(p), pre, match)
    after = json.loads(p.read_text(encoding="utf-8"))
    assert after["doi"] == "10.1/new"  # always overwritten
    assert after["title"] == "Existing"
    assert after["year"] == "1999"
    assert after["journal"] == "Old J"
    assert after["authors"][0]["surname"] == "Pre"


# ---------- sidecar_title_hint ----------

def test_sidecar_title_hint_prefers_title_field():
    assert sidecar_title_hint({"title": "Real Paper Title Here"}) == "Real Paper Title Here"


def test_sidecar_title_hint_falls_back_to_text():
    sd = {"title": "x", "text": "1\n\nLong enough first real line about something\n..."}
    assert sidecar_title_hint(sd) == "Long enough first real line about something"


def test_sidecar_title_hint_handles_empty():
    assert sidecar_title_hint({}) == ""
    assert sidecar_title_hint(None) == ""


# ---------- discover_orphans ----------

def test_discover_orphans_yields_only_empty_doi_sidecars(tmp_path):
    # one with doi, one without, one bare PDF (no sidecar)
    p1 = tmp_path / "a.pdf"; p1.write_bytes(b"%PDF-1.4")
    p2 = tmp_path / "b.pdf"; p2.write_bytes(b"%PDF-1.4")
    p3 = tmp_path / "c.pdf"; p3.write_bytes(b"%PDF-1.4")
    (tmp_path / "a.fulltext.json").write_text(json.dumps({"doi": "10.1/has"}), encoding="utf-8")
    (tmp_path / "b.fulltext.json").write_text(json.dumps({"doi": ""}), encoding="utf-8")
    # c has no sidecar

    found = list(discover_orphans(str(tmp_path)))
    names = sorted(fn for fn, _, _ in found)
    assert names == ["b.pdf", "c.pdf"]
    by_fn = {fn: (sc_path, sc) for fn, sc_path, sc in found}
    assert by_fn["b.pdf"][1] is not None      # sidecar dict loaded
    assert by_fn["c.pdf"][1] is None          # no sidecar


# ---------- extract_metadata ----------

def test_extract_metadata_handles_missing_author():
    """Real CrossRef records sometimes drop the `author` array (legacy
    Elsevier abstract records). Ensure extract_metadata doesn't crash."""
    item = {
        "score": 50, "DOI": "10.1/x", "type": "journal-article",
        "title": ["Real Title"],
        "issued": {"date-parts": [[2003]]},
        "container-title": ["JSAMS"],
        # NO `author` key at all
    }
    meta = extract_metadata(item)
    assert meta["doi"] == "10.1/x"
    assert meta["first_family"] == ""
    assert meta["title"] == "Real Title"
    assert meta["year"] == "2003"
    assert meta["journal"] == "JSAMS"
    assert meta["authors"] == []
