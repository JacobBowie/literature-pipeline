"""B6/D2+Gap2: extract_pdf_fulltext --refresh JATS-content guard + sidecar-DOI seeding.

D2   -- on --refresh, a plain PDF re-extract must NOT downgrade an existing JATS-sourced sidecar
        (structured sections/formulas/abstract pdftotext can't reproduce) when the JATS sibling is
        gone/failed and we fall to the PDF path.
Gap2 -- a NEW sidecar is seeded with the DOI from the companion .ris, so DataCite/arXiv PDFs (DOI
        only in the .ris, not in machine-readable PDF text) aren't born doi-empty.
"""
import json
import sys

import extract_pdf_fulltext as E


def test_is_jats_sourced():
    f = E._is_jats_sourced
    assert f({"extracted_from_pdf": False}) is True
    assert f({"extractor": "jats_xml_sibling"}) is True
    assert f({"sections": [{"title": "Intro"}]}) is True
    assert f({"n_formulas": 3}) is True
    assert f({"tables": [{"caption": "T1"}]}) is True     # tables-only JATS (no sections/formulas)
    # a genuine PDF-sourced sidecar is NOT jats-sourced (D2 must not over-protect)
    assert f({"extracted_from_pdf": True, "extractor": "pdfminer.six",
              "sections": [], "n_formulas": 0, "tables": []}) is False
    assert f({}) is False
    assert f(None) is False


def _drive(monkeypatch, lib, *, refresh, text="pdf body text"):
    """Run extract_pdf_fulltext.main() over `lib` with the extractors mocked (no real PDF/JATS)."""
    monkeypatch.setattr(E, "try_parse_jats_sibling", lambda p: (None, "no_xml_sibling"))
    monkeypatch.setattr(E, "extract", lambda p: (text, "pdfminer.six", "ok"))
    monkeypatch.setattr(E, "detect_math_indicators", lambda t, p: {})
    monkeypatch.setattr(E, "clean_pdf_text", lambda t: t)
    argv = ["extract_pdf_fulltext.py", "--lib-dir", str(lib)]
    if refresh:
        argv.append("--refresh")
    monkeypatch.setattr(sys, "argv", argv)
    E.main()


def test_d2_refresh_keeps_jats_sidecar(tmp_path, monkeypatch):
    lib = tmp_path / "lib"; lib.mkdir()
    (lib / "p.pdf").write_bytes(b"%PDF-1.4\n")
    jats = {"doi": "10.1/x", "text": "rich jats full text",
            "sections": [{"title": "Methods"}], "n_formulas": 2,
            "extracted_from_pdf": False, "extractor": "jats_xml_sibling"}
    (lib / "p.fulltext.json").write_text(json.dumps(jats), encoding="utf-8")
    _drive(monkeypatch, lib, refresh=True)                 # no sibling -> PDF path -> D2 guards
    after = json.loads((lib / "p.fulltext.json").read_text(encoding="utf-8"))
    assert after == jats                                   # untouched: JATS sidecar kept, not clobbered


def test_d2_refresh_still_updates_pdf_sidecar(tmp_path, monkeypatch):
    """Control: a PDF-sourced sidecar IS re-extracted on --refresh (D2 doesn't over-protect)."""
    lib = tmp_path / "lib"; lib.mkdir()
    (lib / "p.pdf").write_bytes(b"%PDF-1.4\n")
    (lib / "p.fulltext.json").write_text(json.dumps(
        {"doi": "10.1/x", "text": "old pdf text", "extractor": "pdfminer.six",
         "extracted_from_pdf": True, "sections": [], "n_formulas": 0}), encoding="utf-8")
    _drive(monkeypatch, lib, refresh=True, text="new pdf text")
    after = json.loads((lib / "p.fulltext.json").read_text(encoding="utf-8"))
    assert after["text"] == "new pdf text"                 # re-extracted
    assert after["doi"] == "10.1/x"                        # RC5: enriched DOI preserved


def test_gap2_seeds_doi_from_ris(tmp_path, monkeypatch):
    lib = tmp_path / "lib"; lib.mkdir()
    (lib / "p.pdf").write_bytes(b"%PDF-1.4\n")
    (lib / "p.ris").write_text("TY  - JOUR\nDO  - 10.48550/arXiv.2401.00001\nER  - \n", encoding="utf-8")
    _drive(monkeypatch, lib, refresh=False)                # fresh sidecar
    sc = json.loads((lib / "p.fulltext.json").read_text(encoding="utf-8"))
    assert sc["doi"] == "10.48550/arxiv.2401.00001"        # Gap2: born WITH the .ris DOI (lowercased)
    assert sc["text"] == "pdf body text"


def test_gap2_no_ris_leaves_doi_empty(tmp_path, monkeypatch):
    lib = tmp_path / "lib"; lib.mkdir()
    (lib / "p.pdf").write_bytes(b"%PDF-1.4\n")
    _drive(monkeypatch, lib, refresh=False)
    sc = json.loads((lib / "p.fulltext.json").read_text(encoding="utf-8"))
    assert sc["doi"] == ""                                 # no .ris -> nothing to seed


def test_gap2_refresh_preserves_enriched_doi_over_stale_ris(tmp_path, monkeypatch):
    """Review fix (finding 1): on --refresh, a stale/divergent .ris DOI must NOT clobber an
    enriched/corrected sidecar DOI. The seed runs AFTER merge and only when the merged DOI is still
    invalid, so RC5 preservation holds for doi (pre-fix, the .ris value overrode it)."""
    lib = tmp_path / "lib"; lib.mkdir()
    (lib / "p.pdf").write_bytes(b"%PDF-1.4\n")
    # PDF-sourced sidecar (so D2 does NOT skip it) carrying a VALID enriched DOI
    (lib / "p.fulltext.json").write_text(json.dumps(
        {"doi": "10.1234/enriched-correct", "text": "old", "extractor": "pdfminer.six",
         "extracted_from_pdf": True, "sections": [], "n_formulas": 0, "tables": []}), encoding="utf-8")
    (lib / "p.ris").write_text("DO  - 10.5555/stale-preprint\n", encoding="utf-8")   # divergent
    _drive(monkeypatch, lib, refresh=True, text="new pdf text")
    sc = json.loads((lib / "p.fulltext.json").read_text(encoding="utf-8"))
    assert sc["doi"] == "10.1234/enriched-correct"         # enriched DOI preserved, NOT the .ris one
    assert sc["text"] == "new pdf text"                    # text still refreshed


def test_gap2_rejects_malformed_ris_doi(tmp_path, monkeypatch):
    """Review fix (finding 2): a malformed .ris DOI (unicode-dash, per the B2 gate) is NOT written
    into a born-empty sidecar -- it stays empty so fill_missing_dois can still recover it."""
    lib = tmp_path / "lib"; lib.mkdir()
    (lib / "p.pdf").write_bytes(b"%PDF-1.4\n")
    (lib / "p.ris").write_text("DO  - 10.1234/foo‐bar\n", encoding="utf-8")   # U+2010 hyphen
    _drive(monkeypatch, lib, refresh=False)
    sc = json.loads((lib / "p.fulltext.json").read_text(encoding="utf-8"))
    assert sc["doi"] == ""                                 # malformed -> not seeded (is_valid_doi gate)


def test_gap2_recovers_url_form_ris_doi(tmp_path, monkeypatch):
    """Review fix (finding 2): a URL-form .ris DOI is normalized (prefix stripped) and, if valid,
    seeded -- normalize_doi recovers it rather than rejecting outright."""
    lib = tmp_path / "lib"; lib.mkdir()
    (lib / "p.pdf").write_bytes(b"%PDF-1.4\n")
    (lib / "p.ris").write_text("DO  - https://doi.org/10.1234/foo.1\n", encoding="utf-8")
    _drive(monkeypatch, lib, refresh=False)
    sc = json.loads((lib / "p.fulltext.json").read_text(encoding="utf-8"))
    assert sc["doi"] == "10.1234/foo.1"                    # normalized + seeded
