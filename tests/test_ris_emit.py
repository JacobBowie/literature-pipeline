"""Regression tests for ris_emit's CrossRef-message → RIS pipeline.

The functions tested here are network-free helpers (`crossref_meta`,
`build_ris`, `_ris_pages`, `write_ris`). The live CrossRef HTTP call
(`crossref_by_doi`) is excluded — tested separately at the integration layer.
"""
from __future__ import annotations
import os
import tempfile

import pytest

from ris_emit import build_ris, crossref_meta, write_ris


def _crossref_msg_sample() -> dict:
    """Realistic CrossRef /works message — fields that build_ris consumes."""
    return {
        "DOI": "10.1152/jappl.1972.32.6.812",
        "title": ["Predicting rectal temperature response to work, environment, and clothing"],
        "container-title": ["Journal of Applied Physiology"],
        "volume": "32",
        "issue": "6",
        "page": "812-822",
        "issued": {"date-parts": [[1972, 6]]},
        "author": [
            {"family": "Givoni",  "given": "B"},
            {"family": "Goldman", "given": "R F"},
        ],
        "ISSN": ["8750-7587"],
        "type": "journal-article",
        "abstract": "<jats:p>Method for predicting rectal temperature…</jats:p>",
    }


# ---------- crossref_meta: schema flattening ----------

class TestCrossrefMeta:
    def test_extracts_canonical_fields(self):
        m = crossref_meta(_crossref_msg_sample())
        assert m["doi"]       == "10.1152/jappl.1972.32.6.812"
        assert m["year"]      == "1972"
        assert m["lastname"]  == "Givoni"
        assert m["container"] == "Journal of Applied Physiology"
        assert m["volume"]    == "32"
        assert m["page"]      == "812-822"

    def test_strips_jats_tags_from_abstract(self):
        m = crossref_meta(_crossref_msg_sample())
        assert "<jats:p>" not in m["abstract"]
        assert "Method for predicting" in m["abstract"]

    def test_empty_message_returns_empty_dict(self):
        assert crossref_meta({}) == {}
        assert crossref_meta(None) == {}

    def test_missing_optional_fields_default_to_empty_string(self):
        minimal = {"DOI": "10.1234/x", "title": ["X"], "type": "journal-article"}
        m = crossref_meta(minimal)
        assert m["doi"] == "10.1234/x"
        assert m["year"]     == ""
        assert m["container"] == ""
        assert m["abstract"]  == ""

    def test_date_isoish_format(self):
        m = crossref_meta(_crossref_msg_sample())
        # date is "1972/06" (year/two-digit-month) per the formatter
        assert m["date"].startswith("1972")


# ---------- build_ris: RIS schema correctness ----------

class TestBuildRis:
    def test_minimum_valid_ris(self):
        ris = build_ris(crossref_meta(_crossref_msg_sample()))
        # RIS must start with a TY tag and end with ER
        assert ris.splitlines()[0].startswith("TY  - "),  ris.splitlines()[0]
        assert ris.rstrip().splitlines()[-1].strip() == "ER  -"
        # Required fields land
        assert "TI  - Predicting rectal temperature" in ris
        assert "PY  - 1972"   in ris
        assert "JO  - Journal of Applied Physiology" in ris
        assert "VL  - 32"     in ris
        assert "IS  - 6"      in ris
        assert "SP  - 812"    in ris
        assert "EP  - 822"    in ris
        assert "DO  - 10.1152/jappl.1972.32.6.812" in ris
        assert "AU  - Givoni, B"   in ris
        assert "AU  - Goldman, R F" in ris

    def test_journal_article_type_maps_to_JOUR(self):
        ris = build_ris(crossref_meta(_crossref_msg_sample()))
        assert ris.startswith("TY  - JOUR")

    def test_preprint_type_maps_to_UNPD(self):
        msg = _crossref_msg_sample() | {"type": "posted-content"}
        ris = build_ris(crossref_meta(msg))
        assert ris.startswith("TY  - UNPD")

    def test_empty_meta_returns_empty_string(self):
        assert build_ris({}) == ""

    def test_authors_without_given_name_dont_emit_trailing_comma(self):
        msg = _crossref_msg_sample()
        msg["author"] = [{"family": "Mononym"}]
        ris = build_ris(crossref_meta(msg))
        assert "AU  - Mononym\n" in ris
        assert "AU  - Mononym,"  not in ris

    def test_pages_without_dash_emit_only_SP(self):
        msg = _crossref_msg_sample() | {"page": "812"}
        ris = build_ris(crossref_meta(msg))
        assert "SP  - 812\n" in ris
        assert "EP  -"        not in ris


# ---------- write_ris: file IO ----------

class TestWriteRis:
    def test_writes_to_path(self, tmp_path):
        path = tmp_path / "sub" / "test.ris"
        ris = build_ris(crossref_meta(_crossref_msg_sample()))
        assert write_ris(str(path), ris) is True
        assert path.exists()
        content = path.read_text(encoding="utf-8")
        assert "TI  - Predicting rectal temperature" in content

    def test_skips_existing_when_overwrite_false(self, tmp_path):
        path = tmp_path / "test.ris"
        path.write_text("original\n", encoding="utf-8")
        assert write_ris(str(path), "new", overwrite=False) is False
        assert path.read_text(encoding="utf-8") == "original\n"

    def test_overwrites_when_overwrite_true(self, tmp_path):
        path = tmp_path / "test.ris"
        path.write_text("original\n", encoding="utf-8")
        assert write_ris(str(path), "new", overwrite=True) is True
        assert path.read_text(encoding="utf-8") == "new"

    def test_empty_ris_string_returns_false(self, tmp_path):
        path = tmp_path / "test.ris"
        assert write_ris(str(path), "") is False
        assert not path.exists()
