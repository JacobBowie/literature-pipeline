"""W4-C: OCR behind --ocr (Tesseract through pytesseract), the language rules and TESSDATA_PREFIX.

The test that runs the real Tesseract is skipped when `shutil.which("tesseract")` finds none (the
CI matrix has no Tesseract); the others stub `tesseract_status` / `ocr_pages`.
"""
import io
import json
import shutil
from pathlib import Path

import pymupdf
import pytest

import extract_pdf_fulltext as E

LINES = [
    "Skeletal muscle adapts to repeated bouts of exercise through mitochondrial growth.",
    "Twelve volunteers completed six weeks of interval training on a cycle ergometer.",
    "Biopsies showed citrate synthase activity rising, and glycogen content increased.",
    "Capillary density rose in the oxidative fibres while peak oxygen uptake improved.",
]
OCR_PAGE = "\n".join(LINES * 3)


def make_pdf(path, pages):
    doc = pymupdf.open()
    for t in pages:
        page = doc.new_page()
        if t:
            page.insert_textbox(pymupdf.Rect(40, 40, 560, 800), t, fontsize=9)
    doc.save(str(path))
    doc.close()
    return path


def scanned_pdf(path, words):
    """A one-page PDF whose only content is an image of `words` (no text layer): a scan."""
    from PIL import Image, ImageDraw, ImageFont
    img = Image.new("L", (1700, 2200), 255)
    draw = ImageDraw.Draw(img)
    font = ImageFont.load_default(size=44)
    y = 150
    for line in words:
        draw.text((120, y), line, fill=0, font=font)
        y += 90
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    doc = pymupdf.open()
    page = doc.new_page(width=612, height=792)
    page.insert_image(page.rect, stream=buf.getvalue())
    doc.save(str(path))
    doc.close()
    return path


def status(langs=("eng", "osd", "spa"), available=True):
    return {"available": available, "cmd": "tesseract", "version": "tesseract v5.5.3",
            "languages": list(langs), "tessdata_prefix": "", "error": "" if available else "no"}


def sidecar(p):
    return json.loads(Path(p).read_text(encoding="utf-8"))


# ---------------------------------------------------------------- the real Tesseract
@pytest.mark.skipif(shutil.which("tesseract") is None, reason="no Tesseract on PATH (CI)")
def test_scanned_page_fixture_gets_text_with_ocr(tmp_path, capsys):
    words = ["Skeletal muscle glycogen and capillary density",
             "increased after interval training in twelve",
             "volunteers on a cycle ergometer for six weeks.",
             "Citrate synthase activity rose by one third, while",
             "peak oxygen uptake improved and lactate fell."]
    pdf = scanned_pdf(tmp_path / "2020_Author_TeachingScan.pdf", words)
    assert not "".join(p.get_text() for p in pymupdf.open(str(pdf))).strip()   # no text layer
    # without --ocr: a needs_ocr sidecar, exit 0
    assert E.main(["--lib-dir", str(tmp_path)]) == 0
    first = sidecar(tmp_path / "2020_Author_TeachingScan.fulltext.json")
    assert first["needs_ocr"] is True and first["text"] == ""
    # --ocr picks the needs_ocr sidecar up without --refresh
    assert E.main(["--lib-dir", str(tmp_path), "--ocr"]) == 0
    out = capsys.readouterr().out
    sc = sidecar(tmp_path / "2020_Author_TeachingScan.fulltext.json")
    assert sc["extractor"] == "tesseract" and sc["ocr_dpi"] == 300 and sc["ocr_lang"] == "eng"
    assert sc["ocr_pages"] == [1] and sc["ocr_engine_version"].startswith("tesseract")
    assert isinstance(sc["ocr_mean_confidence"], float) and sc["ocr_mean_confidence"] > 70
    assert "ocr_low_confidence" not in sc and "needs_ocr" not in sc
    text = sc["text"].lower()
    for w in ("skeletal", "glycogen", "capillary", "ergometer", "citrate", "lactate"):
        assert w in text, (w, sc["text"])
    assert "OCR'd:                  1" in out


@pytest.mark.skipif(shutil.which("tesseract") is None, reason="no Tesseract on PATH (CI)")
def test_tesseract_status_reads_the_real_install():
    st = E.tesseract_status()
    assert st["available"], st
    assert st["version"].startswith("tesseract") and "eng" in st["languages"]


# ---------------------------------------------------------------- Tesseract missing or thin
def test_ocr_without_tesseract_exits_2_and_still_writes_needs_ocr(tmp_path, capsys, monkeypatch):
    make_pdf(tmp_path / "p.pdf", ["", "", ""])
    monkeypatch.setattr(E, "_tesseract_cmd", lambda: "")
    rc = E.main(["--lib-dir", str(tmp_path), "--ocr"])
    out = capsys.readouterr().out
    assert rc == 2
    last = out.strip().splitlines()[-1]
    assert last.startswith("[step-summary] ")
    assert any("tesseract unavailable" in r for r in json.loads(last[15:])["reasons"])
    assert sidecar(tmp_path / "p.fulltext.json")["needs_ocr"] is True


def test_no_language_check_without_ocr(tmp_path, monkeypatch):
    make_pdf(tmp_path / "p.pdf", ["", ""])

    def never():
        raise AssertionError("tesseract_status ran without --ocr")
    monkeypatch.setattr(E, "tesseract_status", never)
    assert E.run(lib_dir=str(tmp_path))["exit"] == 0


def test_language_not_installed_fails_the_document_not_the_run(tmp_path, capsys, monkeypatch):
    make_pdf(tmp_path / "es.pdf", ["", ""])
    (tmp_path / "es.ris").write_text("TY  - JOUR\nLA  - spa\nER  - \n", encoding="utf-8")
    make_pdf(tmp_path / "en.pdf", ["", ""])
    monkeypatch.setattr(E, "tesseract_status", lambda: status(("eng", "osd")))
    calls = []
    monkeypatch.setattr(E, "ocr_pages", lambda p, pages, lang, dpi=300: (
        calls.append(lang) or ({i: OCR_PAGE + f"\npage {i}" for i in pages}, [91.0] * 40)))
    rc = E.main(["--lib-dir", str(tmp_path), "--ocr"])
    out = capsys.readouterr().out
    assert rc == 0
    assert out.count("WARNING: Tesseract lists only 2") == 1 and "TESSDATA_PREFIX" in out
    es = sidecar(tmp_path / "es.fulltext.json")
    assert es["needs_ocr"] is True and es["needs_ocr_reason"] == "language spa not installed"
    en = sidecar(tmp_path / "en.fulltext.json")
    assert en["extractor"] == "tesseract" and en["ocr_lang"] == "eng"
    assert calls == ["eng"]                                # carried on with eng


def test_ris_language_plus_english(tmp_path, monkeypatch):
    make_pdf(tmp_path / "p.pdf", [""])
    (tmp_path / "p.ris").write_text("TY  - JOUR\nLA  - es\nER  - \n", encoding="utf-8")
    monkeypatch.setattr(E, "tesseract_status", lambda: status())
    seen = {}

    def fake(p, pages, lang, dpi=300):
        seen.update(lang=lang, dpi=dpi, pages=list(pages))
        return {i: OCR_PAGE for i in pages}, [88.0] * 50
    monkeypatch.setattr(E, "ocr_pages", fake)
    assert E.main(["--lib-dir", str(tmp_path), "--ocr", "--ocr-dpi", "200"]) == 0
    sc = sidecar(tmp_path / "p.fulltext.json")
    assert seen == {"lang": "spa+eng", "dpi": 200, "pages": [0]}
    assert sc["ocr_lang"] == "spa+eng" and sc["ocr_dpi"] == 200 and sc["ocr_mean_confidence"] == 88.0


def test_only_the_bad_pages_are_ocred(tmp_path, monkeypatch):
    good = ["\n".join(f"{i}.{n} {ln}" for n, ln in enumerate(LINES * 2)) for i in range(2)]
    make_pdf(tmp_path / "p.pdf", good + ["", "", ""])
    monkeypatch.setattr(E, "tesseract_status", lambda: status())
    seen = []

    def fake(p, pages, lang, dpi=300):
        seen.extend(pages)
        return {i: f"OCRPAGE{i} " + OCR_PAGE for i in pages}, [90.0] * 30
    monkeypatch.setattr(E, "ocr_pages", fake)
    E.run(lib_dir=str(tmp_path), ocr=True)
    sc = sidecar(tmp_path / "p.fulltext.json")
    assert seen == [2, 3, 4] and sc["ocr_pages"] == [3, 4, 5]
    assert "0.0 Skeletal" in sc["text"] and "OCRPAGE4" in sc["text"]   # native pages kept


def test_mixed_text_that_still_fails_is_ocred_whole(tmp_path, monkeypatch):
    """Only the blank pages first; when the mixed text still fails (PyMuPDF's text of the other
    pages is mojibake, though the extractor's text was clean), every page."""
    make_pdf(tmp_path / "p.pdf", ["", "", "", "", ""])
    clean = "\n".join(f"{n} {ln}" for n, ln in enumerate(LINES * 4))
    junk = ["".join(chr(0xE000 + ((i * 7 + p * 13) % 500)) for i in range(3000)) for p in range(2)]
    monkeypatch.setattr(E, "extract", lambda p: (clean, "pdftotext", "OK"))
    monkeypatch.setattr(E, "pdf_page_texts", lambda p: (junk + ["", "", ""], ""))
    monkeypatch.setattr(E, "tesseract_status", lambda: status())
    calls = []

    def fake(p, pages, lang, dpi=300):
        calls.append(list(pages))
        return {i: f"WHOLE{i} " + OCR_PAGE for i in pages}, [90.0] * 30
    monkeypatch.setattr(E, "ocr_pages", fake)
    E.run(lib_dir=str(tmp_path), ocr=True)
    sc = sidecar(tmp_path / "p.fulltext.json")
    assert calls == [[2, 3, 4], [0, 1]]
    assert sc["ocr_pages"] == [1, 2, 3, 4, 5] and "WHOLE0" in sc["text"]
    assert "" not in sc["text"]


def test_garbage_layer_is_ocred_on_every_page(tmp_path, monkeypatch):
    """A mojibake layer (no blank page) fails on the document measures: every page is OCR'd."""
    cid = " ".join(f"(cid:{i % 40})" for i in range(600))
    make_pdf(tmp_path / "p.pdf", [cid, cid + " ", cid + "  "])
    monkeypatch.setattr(E, "PDFTOTEXT", None)
    monkeypatch.setattr(E, "tesseract_status", lambda: status())
    seen = []
    monkeypatch.setattr(E, "ocr_pages", lambda p, pages, lang, dpi=300: (
        seen.extend(pages) or ({i: f"P{i} " + OCR_PAGE for i in pages}, [91.0] * 30)))
    E.run(lib_dir=str(tmp_path), ocr=True)
    assert seen == [0, 1, 2]
    assert sidecar(tmp_path / "p.fulltext.json")["extractor"] == "tesseract"


def test_low_confidence_keeps_the_text(tmp_path, monkeypatch):
    make_pdf(tmp_path / "p.pdf", ["", ""])
    monkeypatch.setattr(E, "tesseract_status", lambda: status())
    monkeypatch.setattr(E, "ocr_pages", lambda p, pages, lang, dpi=300: (
        {i: OCR_PAGE for i in pages}, [40.0] * 30 + [80.0] * 10))
    res = E.run(lib_dir=str(tmp_path), ocr=True)
    sc = sidecar(tmp_path / "p.fulltext.json")
    assert sc["ocr_low_confidence"] is True and sc["ocr_mean_confidence"] == 50.0
    assert "Skeletal muscle" in sc["text"] and res["counts"]["ocr_low_confidence"] == 1
    assert res["exit"] == 0


def test_ocr_text_that_fails_the_gate_stays_needs_ocr(tmp_path, monkeypatch):
    make_pdf(tmp_path / "p.pdf", ["", ""])
    monkeypatch.setattr(E, "tesseract_status", lambda: status())
    monkeypatch.setattr(E, "ocr_pages", lambda p, pages, lang, dpi=300: ({i: "" for i in pages}, []))
    res = E.run(lib_dir=str(tmp_path), ocr=True)
    sc = sidecar(tmp_path / "p.fulltext.json")
    assert sc["needs_ocr"] is True and sc["text"] == ""
    assert sc["needs_ocr_reason"].startswith("ocr text fails the gate: ")
    assert sc["ocr_mean_confidence"] is None and sc["ocr_dpi"] == 300
    assert res["exit"] == 0
    # still a pipeline sidecar: the next --ocr run tries again instead of keeping it as merged
    assert sc["extractor"] == "" and E.is_replaceable(sc)
    calls = []
    monkeypatch.setattr(E, "ocr_pages", lambda p, pages, lang, dpi=300: (
        calls.append(1) or ({i: OCR_PAGE for i in pages}, [90.0] * 20)))
    E.run(lib_dir=str(tmp_path), ocr=True)
    assert calls == [1] and sidecar(tmp_path / "p.fulltext.json")["extractor"] == "tesseract"


def test_ocr_error_is_a_degraded_run(tmp_path, capsys, monkeypatch):
    make_pdf(tmp_path / "p.pdf", ["", ""])
    monkeypatch.setattr(E, "tesseract_status", lambda: status())

    def boom(*a, **k):
        raise RuntimeError("tesseract crashed")
    monkeypatch.setattr(E, "ocr_pages", boom)
    rc = E.main(["--lib-dir", str(tmp_path), "--ocr"])
    assert rc == 2 and capsys.readouterr().out.strip().splitlines()[-1].startswith("[step-summary] ")
    sc = sidecar(tmp_path / "p.fulltext.json")
    assert sc["needs_ocr"] is True and sc["needs_ocr_reason"].startswith("ocr failed: RuntimeError")


def test_ocr_on_refresh_force_keeps_a_passing_merged_text(tmp_path, monkeypatch):
    """An OCR'd sidecar from another tool, the PDF still image-only: --refresh --force --ocr
    changes nothing (the old text passes the gate) and OCRs nothing."""
    make_pdf(tmp_path / "p.pdf", ["", ""])
    p = tmp_path / "p.fulltext.json"
    p.write_text(json.dumps({"text": OCR_PAGE, "extractor": "tesseract-5.5.3-300dpi"}), encoding="utf-8")
    before = p.read_bytes()
    monkeypatch.setattr(E, "tesseract_status", lambda: status())
    monkeypatch.setattr(E, "ocr_pages", lambda *a, **k: pytest.fail("OCR ran"))
    res = E.run(lib_dir=str(tmp_path), refresh=True, force=True, ocr=True)
    assert p.read_bytes() == before and res["counts"]["kept_gate"] == 1


# ---------------------------------------------------------------- helpers
def test_tesseract_lang():
    f = E.tesseract_lang
    assert f("") == "" and f(None) == ""
    assert f("en") == f("English") == f("eng") == f("en-US") == "eng"
    assert f("spa") == f("es") == f("Spanish") == "spa"
    assert f("ger") == f("de") == "deu" and f("fre") == "fra" and f("ru") == "rus"
    assert f("zh") == "chi_sim" and f("chi_tra") == "chi_tra"
    assert f("Klingon") == "klingon"            # unknown: kept, so it is reported by name


def test_ris_language(tmp_path):
    pdf = tmp_path / "p.pdf"
    assert E.ris_language(str(pdf)) == ""
    (tmp_path / "p.ris").write_text("TY  - JOUR\nTI  - x\nLA  - Spanish\nER  - \n", encoding="utf-8")
    assert E.ris_language(str(pdf)) == "Spanish"


def test_parse_tsv():
    tsv = ("level\tpage_num\tblock_num\tpar_num\tline_num\tword_num\tleft\ttop\twidth\theight\tconf\ttext\n"
           "1\t1\t0\t0\t0\t0\t0\t0\t100\t100\t-1\t\n"
           "5\t1\t1\t1\t1\t1\t0\t0\t1\t1\t96.5\tMuscle\n"
           "5\t1\t1\t1\t1\t2\t0\t0\t1\t1\t90\tglycogen\n"
           "5\t1\t1\t1\t2\t1\t0\t0\t1\t1\t80\trose\n"
           "5\t1\t2\t1\t1\t1\t0\t0\t1\t1\t-1\t \n"
           "5\t1\t2\t1\t1\t2\t0\t0\t1\t1\t70\tagain\n")
    text, confs = E.parse_tsv(tsv)
    assert text == "Muscle glycogen\nrose\n\nagain"
    assert confs == [96.5, 90.0, 80.0, 70.0]
    assert E.parse_tsv("") == ("", [])


def test_ensure_tessdata_prefix(tmp_path, monkeypatch):
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path))
    monkeypatch.delenv("TESSDATA_PREFIX", raising=False)
    assert E.ensure_tessdata_prefix() == ""                 # the folder does not exist: unset
    td = tmp_path / "Tesseract-OCR" / "tessdata"
    td.mkdir(parents=True)
    assert E.default_tessdata_dir() == str(td)
    assert E.ensure_tessdata_prefix() == str(td)
    monkeypatch.setenv("TESSDATA_PREFIX", str(tmp_path / "inherited"))
    assert E.ensure_tessdata_prefix() == str(tmp_path / "inherited")   # setdefault: kept


def test_no_miniconda_tessdata_anywhere():
    for mod in ("extract_pdf_fulltext.py", "build_pdf_library.py"):
        src = (Path(E.__file__).parent / mod).read_text(encoding="utf-8")
        assert "miniconda3" not in src and "share\\tessdata" not in src, mod
        assert "os.environ['TESSDATA_PREFIX'] =" not in src, mod     # only setdefault
