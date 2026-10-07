"""W4-C: which sidecars a re-extract may replace, the needs_ocr record, the .identity.json merge and
the text DOI candidate.

Real PDFs are made with PyMuPDF in tmp_path (pdfminer reads their text layer); a blank page is an
image-only page to the gate.
"""
import hashlib
import json
import shutil
import sys
from pathlib import Path

import pymupdf
import pytest

import audit_portfolio
import extract_pdf_fulltext as E

FIX = Path(__file__).parent / "fixtures" / "W4-C"

LINES = [
    "Skeletal muscle adapts to repeated bouts of exercise through changes in mitochondrial content.",
    "Twelve volunteers completed six weeks of interval training on a cycle ergometer.",
    "Biopsies from the vastus lateralis showed citrate synthase activity rising by a third.",
    "Resting glycogen content increased in parallel with the oxidative enzymes measured.",
    "Peak oxygen uptake improved modestly and time to exhaustion at a fixed workload grew.",
    "Capillary density per fibre rose in the type one fibres but not in the type two fibres.",
    "These findings suggest brief intense training produces peripheral adaptations quickly.",
    "Central cardiovascular responses differed between the sexes and across age groups.",
]


def page_text(i, extra=""):
    return extra + "\n".join(f"{n + 1 + 10 * i}. {ln}" for n, ln in enumerate(LINES))


def make_pdf(path, pages):
    """A PDF whose pages carry `pages` as a text layer ('' is a blank, image-only page)."""
    doc = pymupdf.open()
    for t in pages:
        page = doc.new_page()
        if t:
            page.insert_textbox(pymupdf.Rect(40, 40, 560, 800), t, fontsize=9)
    doc.save(str(path))
    doc.close()
    return path


def good_pdf(path, n=3, extra=""):
    return make_pdf(path, [page_text(i, extra if i == 0 else "") for i in range(n)])


def blank_pdf(path, n=4):
    return make_pdf(path, [""] * n)


def sha(p):
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def run(lib, *flags, capsys=None):
    rc = E.main(["--lib-dir", str(lib), *flags])
    out = capsys.readouterr().out if capsys else ""
    return rc, out


def write_json(p, d):
    Path(p).write_text(json.dumps(d, indent=1), encoding="utf-8")


JATS = b"""<?xml version="1.0"?>
<article><front><article-meta>
<article-id pub-id-type="doi">10.1234/teaching.jats.2020</article-id>
<title-group><article-title>A sibling parsed as JATS</article-title></title-group>
</article-meta></front>
<body><sec><title>Results</title><p>The JATS sibling carries this structured body text about
oxygen uptake, glycogen and capillary density in trained cyclists.</p></sec></body></article>
"""

OCR_TEXT = "--- page 1 [OCR] ---\n" + "\n".join(LINES) + "\n" + "\n".join(LINES[::-1])


# ---------------------------------------------------------------- merged sidecars survive --refresh
MERGED = {
    "census_ocr_extractor": {"extractor": "OCR (tesseract) via tools/ocr_scans.py, re-ingested 2026-09-23"},
    "census_tesseract_dpi": {"extractor": "tesseract-5.5.3.20260724-300dpi"},
    "census_read_tool": {"extractor": "read-tool-2026-06-09"},
    "census_hand_filed": {"extractor": "pymupdf, filed by hand 2026-09-08"},
    "our_tesseract": {"extractor": "tesseract", "ocr_dpi": 300},
    "text_preocr": {"extractor": "PyMuPDF", "text_preocr": "x"},
    "repair_note": {"extractor": "PyMuPDF", "repair_note": "fixed by hand"},
    "ocr_merged": {"extractor": "pdfminer.six", "ocr_merged": True},
    "licence": {"extractor": "pdfminer.six", "licence": "CC-BY"},
    "license": {"extractor": "pdfminer.six", "license": "CC-BY"},
    "jats_extractor": {"extractor": "jats_xml_sibling", "extracted_from_pdf": False},
    "jats_no_flag": {"sections": [{"title": "Methods"}], "n_formulas": 0},
}


@pytest.mark.parametrize("with_xml", [False, True], ids=["no_xml", "xml_sibling"])
@pytest.mark.parametrize("kind", sorted(MERGED))
def test_merged_sidecar_survives_refresh_byte_identical(tmp_path, capsys, kind, with_xml):
    good_pdf(tmp_path / "p.pdf")
    sc = tmp_path / "p.fulltext.json"
    write_json(sc, {"doi": "10.1234/teaching.2020", "text": OCR_TEXT, **MERGED[kind]})
    if with_xml:
        (tmp_path / "p.xml").write_bytes(JATS)
    before = sha(sc)
    rc, out = run(tmp_path, "--refresh", capsys=capsys)
    assert rc == 0
    assert sha(sc) == before
    assert "KEEP" in out and "Kept (merged):          1" in out


def test_is_replaceable():
    f = E.is_replaceable
    for ex in ("pdfminer.six", "pdftotext", "pdfplumber", "PyMuPDF"):
        assert f({"extractor": ex, "text": "t"}) is True
    for ex in ("pdfminer.six", "pdftotext", "pdfplumber", "PyMuPDF", "", None):
        assert f({"extractor": ex, "text": "t", "extracted_from_pdf": True}) is True
    for ex in ("", None):   # W4a verifier L APPLY-1: hand-made or JATS text, not proven pipeline output
        assert f({"extractor": ex, "text": "t"}) is False
    assert f({}) is True and f(None) is True
    for d in MERGED.values():
        assert f(d) is False


# ---------------------------------------------------------------- --refresh --force
def test_force_keeps_every_field_and_moves_old_text(tmp_path, capsys):
    good_pdf(tmp_path / "p.pdf")
    old = {"pmcid": "", "doi": "10.1234/teaching.2020", "title": "A teaching paper",
           "authors": [{"given": "A.", "surname": "Author"}], "text": OCR_TEXT,
           "text_preocr": "the bad layer", "extractor": "OCR (tesseract) via ocr_scans.py",
           "extracted_from_pdf": True, "repair_note": "watermark-only layer replaced",
           "metadata_source": "corrected_by_hand", "metadata_backfilled_at": "2026-09-15T20:07:59Z",
           "source_filename": "s-1.pdf", "ocr_note": "verify digits", "n_formulas": "0"}
    write_json(tmp_path / "p.fulltext.json", old)
    rc, out = run(tmp_path, "--refresh", "--force", capsys=capsys)
    assert rc == 0, out
    new = json.loads((tmp_path / "p.fulltext.json").read_text(encoding="utf-8"))
    assert new["extractor"] == "pdfminer.six"
    assert "Twelve volunteers completed" in new["text"] and new["text"] != OCR_TEXT
    assert new["text_preocr"] == OCR_TEXT                 # the replaced text, not the older one
    for k, v in old.items():
        if k not in ("text", "text_preocr", "extractor", "extracted_from_pdf", "n_formulas"):
            assert new[k] == v, k
    assert "needs_ocr" not in new


def test_force_with_gate_failed_new_extraction_changes_nothing(tmp_path, capsys):
    blank_pdf(tmp_path / "p.pdf")
    sc = tmp_path / "p.fulltext.json"
    write_json(sc, {"doi": "10.1234/teaching.2020", "text": OCR_TEXT,
                    "extractor": "OCR (tesseract) via ocr_scans.py", "repair_note": "x"})
    before = sha(sc)
    rc, out = run(tmp_path, "--refresh", "--force", capsys=capsys)
    assert rc == 0
    assert sha(sc) == before
    assert "KEEP" in out and "gate" in out and "Kept (gate):            1" in out


def test_force_on_jats_sourced_reparses_the_sibling(tmp_path, capsys):
    good_pdf(tmp_path / "p.pdf")
    (tmp_path / "p.xml").write_bytes(JATS)
    sc = tmp_path / "p.fulltext.json"
    write_json(sc, {"doi": "10.1234/teaching.jats.2020", "text": "old jats text",
                    "extractor": "jats_xml_sibling", "extracted_from_pdf": False,
                    "metadata_source": "pmc"})
    before = sha(sc)
    assert run(tmp_path, "--refresh", capsys=capsys)[0] == 0
    assert sha(sc) == before                               # plain --refresh: KEEP
    rc, out = run(tmp_path, "--refresh", "--force", capsys=capsys)
    assert rc == 0
    new = json.loads(sc.read_text(encoding="utf-8"))
    assert new["extractor"] == "jats_xml_sibling" and "structured body text" in new["text"]
    assert new["metadata_source"] == "pmc"
    assert "text_preocr" not in new                        # today's behaviour for a JATS sidecar


def test_force_jats_sourced_without_sibling_keeps(tmp_path, capsys):
    good_pdf(tmp_path / "p.pdf")
    sc = tmp_path / "p.fulltext.json"
    write_json(sc, {"text": "rich jats", "sections": [{"title": "Methods"}], "n_formulas": 2,
                    "extracted_from_pdf": False})
    before = sha(sc)
    rc, out = run(tmp_path, "--refresh", "--force", capsys=capsys)
    assert rc == 0 and sha(sc) == before and "not downgraded" in out


def test_force_needs_refresh(tmp_path, capsys):
    assert run(tmp_path, "--force", capsys=capsys)[0] == 1


# ---------------------------------------------------------------- pipeline sidecars on --refresh
def test_replaceable_refresh_keeps_extra_fields_drops_gate_fields(tmp_path, capsys):
    good_pdf(tmp_path / "p.pdf")
    old = {"doi": "10.1234/teaching.2020", "text": "", "extractor": "pdfminer.six",
           "extracted_from_pdf": True, "has_pdf": True, "identity": "OK", "doc_kind": "VOR",
           "identity_method": "doi", "needs_ocr": True, "needs_ocr_reason": "no_text_layer",
           "text_metrics": {"words": 0}, "doi_candidate": "10.9999/stale"}
    write_json(tmp_path / "p.fulltext.json", old)
    rc, out = run(tmp_path, "--refresh", capsys=capsys)
    assert rc == 0
    new = json.loads((tmp_path / "p.fulltext.json").read_text(encoding="utf-8"))
    assert "Twelve volunteers" in new["text"]
    for k in ("has_pdf", "identity", "doc_kind", "identity_method", "doi"):
        assert new[k] == old[k], k
    for k in ("needs_ocr", "needs_ocr_reason", "text_metrics", "doi_candidate", "text_preocr"):
        assert k not in new, k


def test_old_text_passes_new_fails_keeps_byte_identical(tmp_path, capsys):
    blank_pdf(tmp_path / "p.pdf")
    sc = tmp_path / "p.fulltext.json"
    write_json(sc, {"doi": "10.1234/teaching.2020", "text": OCR_TEXT, "extractor": "pdfminer.six"})
    before = sha(sc)
    rc, out = run(tmp_path, "--refresh", capsys=capsys)
    assert rc == 0 and sha(sc) == before and "KEEP" in out


def test_refresh_of_a_failing_pipeline_text_writes_needs_ocr(tmp_path, capsys):
    """A pipeline sidecar holding a watermark-only text: --refresh empties it and flags it."""
    stamp = "Brought to you by TEACHING LIBRARY | Downloaded 09/15/26"
    make_pdf(tmp_path / "p.pdf", [stamp] * 6)
    write_json(tmp_path / "p.fulltext.json", {"doi": "10.1234/teaching.2020", "title": "Kept title",
                                              "text": "\n".join([stamp] * 6), "extractor": "pdfminer.six"})
    rc, out = run(tmp_path, "--refresh", capsys=capsys)
    assert rc == 0
    new = json.loads((tmp_path / "p.fulltext.json").read_text(encoding="utf-8"))
    assert new["needs_ocr"] is True and new["text"] == ""
    assert new["title"] == "Kept title" and new["doi"] == "10.1234/teaching.2020"
    assert "watermark_only_text" in new["needs_ocr_reason"]


# ---------------------------------------------------------------- the needs_ocr record and exits
def test_gate_failed_pdf_writes_needs_ocr_and_exits_0(tmp_path, capsys):
    blank_pdf(tmp_path / "scan.pdf")
    good_pdf(tmp_path / "good.pdf")
    rc, out = run(tmp_path, capsys=capsys)
    assert rc == 0
    assert "[step-summary]" not in out                 # needs_ocr is not a degraded run
    assert "Needs OCR:              1" in out and "Sidecars NEW:           1" in out
    sc = json.loads((tmp_path / "scan.fulltext.json").read_text(encoding="utf-8"))
    assert sc["needs_ocr"] is True and sc["text"] == ""
    assert "no_text_layer" in sc["needs_ocr_reason"] and "image_only_pages" in sc["needs_ocr_reason"]
    assert sc["text_metrics"]["n_pages"] == 4 and sc["text_metrics"]["bad_page_share"] == 1.0
    assert sc["extracted_from_pdf"] is True and sc["extractor"] == ""
    # an existing needs_ocr sidecar is skipped without --ocr, like any other sidecar
    rc2, out2 = run(tmp_path, capsys=capsys)
    assert rc2 == 0 and "Sidecars already there: 2" in out2


def test_needs_ocr_sidecar_replaced_on_refresh_when_text_now_passes(tmp_path, capsys):
    good_pdf(tmp_path / "p.pdf")
    write_json(tmp_path / "p.fulltext.json", {"text": "", "extractor": "", "needs_ocr": True,
                                              "needs_ocr_reason": "no_text_layer"})
    rc, _ = run(tmp_path, "--refresh", capsys=capsys)
    new = json.loads((tmp_path / "p.fulltext.json").read_text(encoding="utf-8"))
    assert rc == 0 and "needs_ocr" not in new and "Twelve volunteers" in new["text"]


def test_extraction_raised_exits_2_with_step_summary(tmp_path, capsys, monkeypatch):
    good_pdf(tmp_path / "p.pdf")

    def boom(_p):
        raise RuntimeError("extractor crashed")
    monkeypatch.setattr(E, "extract", boom)
    rc, out = run(tmp_path, capsys=capsys)
    assert rc == 2
    last = out.strip().splitlines()[-1]
    assert last.startswith("[step-summary] ")
    summary = json.loads(last[len("[step-summary] "):])
    assert summary["aborted"] is None and summary["transport_failures"] == 0
    assert any("raised" in r for r in summary["reasons"])
    assert not (tmp_path / "p.fulltext.json").exists()


def test_usage_errors_exit_1(tmp_path, capsys):
    assert E.main(["--lib-dir", str(tmp_path / "missing")]) == 1
    with pytest.raises(SystemExit) as ei:
        E.main(["--lib-dir", str(tmp_path), "--no-such-flag"])
    assert ei.value.code == 1
    with pytest.raises(SystemExit) as ei:
        E.main([])
    assert ei.value.code == 1


def test_run_returns_a_dict(tmp_path):
    good_pdf(tmp_path / "p.pdf")
    res = E.run(lib_dir=str(tmp_path))
    assert res["exit"] == 0 and res["counts"]["new"] == 1 and res["counts"]["pdfs"] == 1


def test_dry_run_writes_nothing(tmp_path, capsys):
    blank_pdf(tmp_path / "scan.pdf")
    good_pdf(tmp_path / "good.pdf")
    rc, out = run(tmp_path, "--dry-run", capsys=capsys)
    assert rc == 0 and sorted(p.name for p in tmp_path.iterdir()) == ["good.pdf", "scan.pdf"]


def test_cid_text_falls_back_to_another_extractor(tmp_path, monkeypatch):
    """pdfminer's '(cid:N)' output fails the gate; pdftotext or PyMuPDF reads the same file."""
    good_pdf(tmp_path / "p.pdf")
    cid = " ".join(f"(cid:{i % 30})" for i in range(3000))
    monkeypatch.setattr(E, "extract", lambda p: (cid, "pdfminer.six", "OK"))
    res = E.run(lib_dir=str(tmp_path))
    sc = json.loads((tmp_path / "p.fulltext.json").read_text(encoding="utf-8"))
    assert res["exit"] == 0 and "cid" not in sc["text"] and "Twelve volunteers" in sc["text"]
    assert sc["extractor"] == "PyMuPDF"                  # PyMuPDF first: no column interleaving


@pytest.mark.skipif(E.PDFTOTEXT is None, reason="poppler pdftotext not on PATH")
def test_pdftotext_reads_what_pdfminer_and_pymupdf_garble(tmp_path, monkeypatch):
    """The 2003 teaching-library case: pdfminer '(cid:N)', PyMuPDF control characters,
    pdftotext clean."""
    good_pdf(tmp_path / "p.pdf")
    cid = " ".join(f"(cid:{i % 30})" for i in range(3000))
    junk = ["".join(chr(0xE000 + ((i * 7 + p * 13) % 500)) for i in range(2000)) for p in range(3)]
    monkeypatch.setattr(E, "extract", lambda p: (cid, "pdfminer.six", "OK"))
    monkeypatch.setattr(E, "pdf_page_texts", lambda p: (junk, ""))
    E.run(lib_dir=str(tmp_path))
    sc = json.loads((tmp_path / "p.fulltext.json").read_text(encoding="utf-8"))
    assert sc["extractor"] == "pdftotext" and "Twelve volunteers" in sc["text"]


def test_partial_cid_text_over_the_share_falls_back(tmp_path, monkeypatch):
    """Real text with a third of its words '(cid:N)' (the calibration found such files at
    0.31 to 0.50, every one readable through PyMuPDF): the cleaner text is written."""
    good_pdf(tmp_path / "p.pdf")
    real = " ".join(f"{w}{i}" if i % 7 == 0 else w for i, w in
                    enumerate((" ".join(LINES) + " ").split() * 12))
    words = real.split()
    mixed = " ".join(w if i % 2 else f"(cid:{i % 40}) {w}" for i, w in enumerate(words))
    g = E.text_validity(mixed)
    assert "one_word_dominates" in g["reasons"] and 0.3 < g["metrics"]["top_word_share"] < 0.5
    monkeypatch.setattr(E, "extract", lambda p: (mixed, "pdfminer.six", "OK"))
    E.run(lib_dir=str(tmp_path))
    sc = json.loads((tmp_path / "p.fulltext.json").read_text(encoding="utf-8"))
    assert sc["extractor"] == "PyMuPDF" and "(cid:" not in sc["text"]


# ---------------------------------------------------------------- the DOI candidate (DEC-20)
def test_text_doi_becomes_a_candidate_never_the_doi(tmp_path):
    good_pdf(tmp_path / "p.pdf", extra="Journal of Teaching 2020 doi:10.1234/teaching.2020.001\n")
    E.run(lib_dir=str(tmp_path))
    sc = json.loads((tmp_path / "p.fulltext.json").read_text(encoding="utf-8"))
    assert sc["doi"] == ""
    assert sc["doi_candidate"] == "10.1234/teaching.2020.001" and sc["doi_source"] == "text"


def test_ris_doi_wins_over_the_text(tmp_path):
    good_pdf(tmp_path / "p.pdf", extra="doi:10.1234/teaching.2020.001\n")
    (tmp_path / "p.ris").write_text("TY  - JOUR\nDO  - 10.1234/teaching.ris.7\nER  - \n", encoding="utf-8")
    E.run(lib_dir=str(tmp_path))
    sc = json.loads((tmp_path / "p.fulltext.json").read_text(encoding="utf-8"))
    assert sc["doi"] == "10.1234/teaching.ris.7"
    assert "doi_candidate" not in sc and "doi_source" not in sc


def test_no_candidate_from_a_gate_failed_text(tmp_path):
    stamp = "Downloaded via doi:10.1234/teaching.2020.009 TEACHING LIBRARY"
    make_pdf(tmp_path / "p.pdf", [stamp] * 5)
    E.run(lib_dir=str(tmp_path))
    sc = json.loads((tmp_path / "p.fulltext.json").read_text(encoding="utf-8"))
    assert sc["needs_ocr"] is True and sc["doi"] == "" and "doi_candidate" not in sc


# ---------------------------------------------------------------- the .identity.json merge
@pytest.mark.parametrize("fixture,flag", [
    ("identity_ok.identity.json", ""),
    ("identity_flag.identity.json", "identity=FLAG"),
    ("identity_supplement.identity.json", "doc_kind=SUPPLEMENT"),
])
def test_identity_json_merged_and_flag_agrees(tmp_path, fixture, flag):
    good_pdf(tmp_path / "p.pdf")
    shutil.copyfile(FIX / fixture, tmp_path / "p.identity.json")
    ident = json.loads((FIX / fixture).read_text(encoding="utf-8"))
    E.run(lib_dir=str(tmp_path))
    sc = json.loads((tmp_path / "p.fulltext.json").read_text(encoding="utf-8"))
    for k in E.IDENTITY_FIELDS:
        assert sc[k] == ident[k], k
    assert audit_portfolio.identity_flag(sc) == audit_portfolio.identity_flag(ident) == flag


def test_identity_json_merged_on_refresh_and_needs_ocr(tmp_path):
    blank_pdf(tmp_path / "p.pdf")
    shutil.copyfile(FIX / "identity_flag.identity.json", tmp_path / "p.identity.json")
    E.run(lib_dir=str(tmp_path))
    sc = json.loads((tmp_path / "p.fulltext.json").read_text(encoding="utf-8"))
    assert sc["needs_ocr"] is True and sc["identity"] == "FLAG"
    assert audit_portfolio.identity_flag(sc) == "identity=FLAG"


# ---------------------------------------------------------------- the suspect report (read-only)
def test_suspect_report_is_read_only(tmp_path, capsys):
    lib = tmp_path / "lib"
    lib.mkdir()
    blank_pdf(lib / "2020_Author_Scan.pdf")
    good_pdf(lib / "2021_Author_Good.pdf", n=4)
    make_pdf(lib / "2019_Author_Stamp.pdf", ["Downloaded from TEACHING LIBRARY"] * 5)
    write_json(lib / "2019_Author_Stamp.fulltext.json",
               {"doi": "10.1234/teaching.stamp", "text": "", "extractor": "pdfminer.six",
                "needs_ocr": True, "needs_ocr_reason": "few_unique_words; watermark_only_text"})
    (lib / "2020_Author_Scan.ris").write_text("DO  - 10.1234/teaching.scan\n", encoding="utf-8")
    blank_pdf(lib / "2018_Author_Jats.pdf")
    write_json(lib / "2018_Author_Jats.fulltext.json", {"doi": "10.1234/teaching.jats", "text": OCR_TEXT,
                                                        "extracted_from_pdf": False})
    before = {p.name: sha(p) for p in lib.iterdir()}
    out_csv = tmp_path / "suspects.csv"
    rc, out = run(lib, "--suspect-report", str(out_csv), capsys=capsys)
    assert rc == 0
    assert {p.name: sha(p) for p in lib.iterdir()} == before
    import csv
    rows = {r["file"]: r for r in csv.DictReader(out_csv.open(encoding="utf-8"))}
    assert rows["2020_Author_Scan.pdf"]["gate"] == "fail"
    assert rows["2020_Author_Scan.pdf"]["doi"] == "10.1234/teaching.scan"
    assert "no_text_layer" in rows["2020_Author_Scan.pdf"]["reasons"]
    assert rows["2019_Author_Stamp.pdf"]["gate"] == "needs_ocr"
    assert "watermark_only_text" in rows["2019_Author_Stamp.pdf"]["reasons"]
    jats = rows["2018_Author_Jats.pdf"]                 # the JATS text is fine; the PDF is a scan
    assert jats["gate"] == "fail" and jats["text_source"] == "pymupdf" and jats["doi"] == "10.1234/teaching.jats"
    good = rows.get("2021_Author_Good.pdf")
    assert good is None or good["gate"] == "pass"     # only suspect_file's reasons, if any


def test_suspect_report_names_the_refresh_fix(tmp_path, capsys, monkeypatch):
    lib = tmp_path / "lib"
    lib.mkdir()
    good_pdf(lib / "p.pdf", n=4)
    cid = " ".join(f"(cid:{i % 30})" for i in range(3000))
    write_json(lib / "p.fulltext.json", {"doi": "10.1234/teaching.cid", "text": cid,
                                         "extractor": "pdfminer.six"})
    out_csv = tmp_path / "s.csv"
    run(lib, "--suspect-report", str(out_csv), capsys=capsys)
    import csv
    rows = list(csv.DictReader(out_csv.open(encoding="utf-8")))
    assert rows[0]["gate"] == "fail" and rows[0]["text_source"] == "stored"
    assert rows[0]["refresh_fixes"] in ("pdfminer.six", "pdftotext", "PyMuPDF")
