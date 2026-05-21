"""Tests for fill_missing_dois.py — filename parsing, type filtering, scoring.

Network calls (crossref_query, run_project, main) are not exercised here.
Those are covered by manual smoke runs documented in
notes/2026-05-21_scope_fill_missing_dois.md.
"""
import json

import pytest

from fill_missing_dois import (
    BUNDLE_RE,
    YEAR_RE,
    build_query_string,
    discover_orphans,
    extract_metadata,
    filter_by_type,
    is_bundled_issue,
    normalize_for_match,
    parse_filename_hints,
    score_match,
    sidecar_title_hint,
    update_sidecar,
)


# ---------- regex word-boundary regressions ----------
# Caught a real bug on 2026-05-21: Python's `\b` doesn't fire next to `_`
# because `_` is a word character. Lock the underscore-friendly
# replacements in.

@pytest.mark.parametrize("fn,want", [
    ("2007_IJCSS_Vol6_Edition2.pdf", "2007"),   # underscore-adjacent
    ("nofileyearhere.pdf",            None),    # no year present
])
def test_year_re_handles_underscore_boundaries(fn, want):
    m = YEAR_RE.search(fn)
    assert (m.group(0) if m else None) == want


@pytest.mark.parametrize("fn,want", [
    ("2007_IJCSS_Vol6_Edition2.pdf",            True),   # underscore-adjacent
    ("2006_Hellard_Banister_Limitations.pdf",   False),  # not a bundle
])
def test_bundle_re_handles_underscore_boundaries(fn, want):
    assert is_bundled_issue(fn, "") is want


# ---------- parse_filename_hints ----------

@pytest.mark.parametrize("fn,want", [
    # canonical strict
    ("1976_Calvert_SystemsModelEffectsTrainingPhysicalPerformance.pdf",
     ("1976", "Calvert", "canonical")),
    # canonical-loose (multi-segment after author)
    ("2006_Hellard_Banister_Limitations.pdf",
     ("2006", "Hellard", "canonical-loose")),
    # legacy lowercase_year_title
    ("schweiker_2016_comf.pdf",
     ("2016", "Schweiker", "legacy")),
    # year=0000 → empty year (NO_YEAR semantics)
    ("0000_Bishop_SystemsModelOfTrainingResponses.pdf",
     ("", "Bishop", "canonical")),
])
def test_parse_filename_hints_class(fn, want):
    y, a, _t, k = parse_filename_hints(fn)
    assert (y, a, k) == want


@pytest.mark.parametrize("fn,must_contain", [
    # canonical: CamelCase split into space-separated words
    ("1976_Calvert_SystemsModelEffectsTrainingPhysicalPerformance.pdf",
     "Systems Model Effects Training Physical Performance"),
    # legacy: underscores → spaces
    ("akavian_2025_bsa_htt.pdf", "bsa htt"),
    # canonical-loose: title tokens from rest-of-filename
    ("2006_Hellard_Banister_Limitations.pdf", "Banister"),
])
def test_parse_filename_hints_title_extraction(fn, must_contain):
    _y, _a, t, _k = parse_filename_hints(fn)
    assert must_contain in t


# ---------- normalize_for_match (accent folding) ----------

@pytest.mark.parametrize("a,b", [
    ("Périard", "Periard"),   # NFKD-decomposable
    ("Mølmen",  "Molmen"),    # non-decomposable; caught real bug on 2026-05-21
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


# ---------- scorer (one test per confidence label) ----------

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


# ---------- build_query_string ----------

def test_build_query_string_joins_all_hints():
    assert build_query_string("1976", "Calvert", "Systems Model") == "Calvert Systems Model 1976"


# ---------- sidecar update (preserve-vs-overwrite contract) ----------

def test_update_sidecar_writes_doi_and_preserves_existing(tmp_path):
    """doi is always overwritten; other fields only filled when empty.
    Heavy fields (text/sections/figures) are never touched."""
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
    assert after["year"] == "2020"                         # was empty → set
    assert after["journal"] == "CrossRef Journal"          # was empty → set
    assert after["authors"][0]["surname"] == "X"           # was empty list → set
    # heavy fields preserved
    assert after["text"].startswith("long pre-existing")
    assert after["sections"][0]["heading"] == "Intro"
    assert after["figures"][0]["image_path"] == "x.fig1.png"


def test_update_sidecar_only_overwrites_doi_when_other_fields_populated(tmp_path):
    """Pre-existing curated title/year/journal/authors must survive — only
    the DOI is unconditionally overwritten."""
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
    assert after["doi"] == "10.1/new"
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


# ---------- discover_orphans (orphan-definition contract) ----------

def test_discover_orphans_yields_only_empty_doi_sidecars(tmp_path):
    # a: has doi (not an orphan); b: empty doi (orphan); c: no sidecar (orphan, sidecar=None)
    for fn in ("a.pdf", "b.pdf", "c.pdf"):
        (tmp_path / fn).write_bytes(b"%PDF-1.4")
    (tmp_path / "a.fulltext.json").write_text(json.dumps({"doi": "10.1/has"}), encoding="utf-8")
    (tmp_path / "b.fulltext.json").write_text(json.dumps({"doi": ""}), encoding="utf-8")

    found = list(discover_orphans(str(tmp_path)))
    names = sorted(fn for fn, _, _ in found)
    assert names == ["b.pdf", "c.pdf"]
    by_fn = {fn: sc for fn, _, sc in found}
    assert by_fn["b.pdf"] is not None
    assert by_fn["c.pdf"] is None


# ---------- extract_metadata: real CrossRef edge case ----------

def test_extract_metadata_handles_missing_author():
    """Some CrossRef records (legacy Elsevier abstracts) drop the `author`
    array entirely. Caught on 2026-05-21 with 10.1016/s1440-2440(03)80190-1."""
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
