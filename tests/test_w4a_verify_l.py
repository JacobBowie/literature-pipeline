"""W4a verifier L: the W4-C text-validity gate, OCR and merged-sidecar protection, through the real
consumers (sweep, litpipe.holdings, audit_portfolio, index_portfolio, fill_missing_dois) on temp worlds.

The tests named for APPLY-1 and APPLY-3 locked defects found in verification: each failed on 964c26f
and passes with its fix, landed in the same commit. APPLY-2 (Devanagari) is deferred to W5 and stays
xfail(strict=True) until then. No network; every PDF is made here.
"""
import contextlib
import hashlib
import io
import json
import os
import random
import shutil
import sys
import types
from pathlib import Path

import pymupdf
import pytest

import audit_portfolio
import build_pdf_library as B
import extract_pdf_fulltext as E
import fill_missing_dois
import lit_util
import sweep
from litpipe import holdings

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
WATERMARK = "Downloaded from journals.example.org at University Library on March 3, 2024"


def page_text(i):
    return "\n".join(f"{n + 1 + 10 * i}. {ln}" for n, ln in enumerate(LINES))


def make_pdf(path, pages):
    """A PDF whose pages carry `pages` as a text layer ('' is a blank, image-only page)."""
    doc = pymupdf.open()
    for t in pages:
        page = doc.new_page()
        if t:
            page.insert_textbox(pymupdf.Rect(40, 40, 560, 800), t, fontsize=9)
    doc.save(str(path))
    doc.close()
    return Path(path)


def good_pdf(path, n=3, first_extra=""):
    return make_pdf(path, [(first_extra if i == 0 else "") + page_text(i) for i in range(n)])


def blank_pdf(path, n=4):
    return make_pdf(path, [""] * n)


def sha(p):
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def sidecar(p):
    return json.loads(Path(p).read_text(encoding="utf-8"))


def run_extract(lib, *flags):
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        rc = E.main(["--lib-dir", str(lib), *flags])
    return rc, out.getvalue(), err.getvalue()


def gate(pages):
    return E.text_validity("\n\n".join(pages), pages, len(pages))


# ================================================================ item 2: hostile layers fail
def test_running_heads_only_layer_fails():
    pages = [f"Journal of Applied Physiology  Vol 90  {i + 1}\nSMITH ET AL." if i % 2 else
             f"{i + 1}  Regulation of muscle metabolism" for i in range(12)]
    g = gate(pages)
    assert not g["ok"] and "few_unique_words" in g["reasons"]


def test_two_real_pages_and_ten_blank_fail_on_pages():
    pages = [page_text(0) * 4, page_text(1) * 4] + [""] * 10
    g = gate(pages)
    assert not g["ok"] and g["reasons"] == ["image_only_pages"]


@pytest.mark.parametrize("pages", [
    [WATERMARK] * 12,                                        # one stamp line on every page
    [f"{WATERMARK} page {i + 1}" for i in range(12)],       # a stamp that differs per page
    [WATERMARK] * 2,                                         # too few pages for boilerplate
], ids=["same_stamp", "numbered_stamp", "two_pages"])
def test_scan_with_a_watermark_line_fails(pages):
    assert not gate(pages)["ok"]


def test_scan_with_a_watermark_line_writes_needs_ocr(tmp_path):
    lib = tmp_path / "lib"
    lib.mkdir()
    make_pdf(lib / "2001_Teaching_Scan.pdf", [WATERMARK] * 6)
    rc, out, _ = run_extract(lib)
    sc = sidecar(lib / "2001_Teaching_Scan.fulltext.json")
    assert rc == 0 and sc["needs_ocr"] is True and sc["text"] == ""


def test_cid_layer_over_the_share_fails():
    toks = [f"(cid:{i % 90 + 3})" for i in range(310)] + [LINES[i % 8].split()[i % 5] + "s" for i in range(690)]
    random.Random(1).shuffle(toks)
    assert gate(["\n".join(" ".join(toks[i:i + 12]) for i in range(0, 1000, 12))])["reasons"] == ["one_word_dominates"]


# ---------------------------------------------------------------- healthy non-Latin text passes
VOCAB = {
    "greek": "άσκηση αντοχής αυξάνει πυκνότητα τριχοειδών αγγείων δραστηριότητα οξειδωτικών ενζύμων "
             "σκελετικούς μύες εθελοντών συμμετείχαν μελέτη αιματική ροή καρδιακή παροχή όγκος παλμού "
             "συχνότητα γαλακτικό κατώφλι αερισμού μιτοχόνδρια βιοψία πρωτόκολλο μετρήσεις σημαντικά",
    "cyrillic": "аэробная тренировка увеличивает плотность капилляров активность окислительных "
                "ферментов скелетных мышцах добровольцев участвовавших исследовании сравнению "
                "контролем кровоток сердечный выброс ударный объём частота сокращений лактат порог",
    "arabic": "التدريب التحمل كثافة الشعيرات الدموية نشاط الإنزيمات المؤكسدة العضلات الهيكلية "
              "للمتطوعين المشاركين الدراسة تدفق الدم النتاج القلبي حجم الضربة معدل اللاكتات عتبة",
    "devanagari": "धीरज प्रशिक्षण कंकाल मांसपेशियों केशिका घनत्व ऑक्सीकरण एंजाइम गतिविधि अध्ययन "
                  "स्वयंसेवकों रक्त प्रवाह हृदय उत्पादन स्ट्रोक मात्रा आवृत्ति लैक्टेट सीमा श्वसन "
                  "माइटोकॉन्ड्रिया बायोप्सी प्रोटोकॉल प्रतिभागियों माप महत्वपूर्ण वृद्धि कमी भार",
}


def script_pages(words, n_pages=6, seed=1):
    r = random.Random(seed)
    words = words.split()
    return ["\n".join(" ".join(r.choice(words) for _ in range(12)) + "." for _ in range(40))
            for _ in range(n_pages)]


@pytest.mark.parametrize("script", ["greek", "cyrillic", "arabic"])
def test_healthy_non_latin_text_passes(script):
    g = gate(script_pages(VOCAB[script]))
    assert g["ok"], g["reasons"]


@pytest.mark.xfail(strict=True, reason="APPLY-2, deferred to W5 (no Indic-script paper in any library "
                                       "today; landing it needs the 17-paper bad-side calibration "
                                       "re-run): combining vowel signs split Indic words, so the gate "
                                       "empties a healthy Devanagari paper (few_word_characters)")
def test_healthy_devanagari_text_passes():
    g = gate(script_pages(VOCAB["devanagari"]))
    assert g["ok"], g["reasons"]


# ================================================================ item 3: merged sidecars
OCR_TEXT = "\n".join(LINES) + "\n" + "\n".join(LINES[::-1]) + "\n" + "\n".join(LINES[1::2])

EXTRACTORLESS = {
    # a hand-made sidecar for a scanned article: provenance notes, no `extractor`
    "hand_scan": {"pmcid": "", "doi": "10.5555/teaching.scan.1989", "title": "Norms for a test",
                  "source": "interlibrary loan scan, retrieved 2026-09-15",
                  "extraction_note": "SCANNED ARTICLE: digits in the tables checked by hand",
                  "sections": [], "tables": [], "formulas": [], "n_formulas": 0, "text": OCR_TEXT},
    # a hand OCR sidecar with ocr_* provenance, no `extractor`
    "hand_ocr": {"doi": "10.5555/research.ocr.1991", "ocr": True, "ocr_engine": "tesseract 5.5.3",
                 "ocr_dpi": 300, "ocr_note": "image-only scan; text is OCR output", "source": "copy",
                 "text": OCR_TEXT},
    # a JATS parse of a commentary (no <sec>, so no sections): pmc_fetch's raw parse_jats shape
    "jats_commentary": {"pmcid": "PMC0000001", "pmid": "1", "doi": "10.5555/teaching.comment.2022",
                        "title": "Comment", "abstract": "", "sections": [], "figures": [], "tables": [],
                        "formulas": [], "n_formulas": 0, "formula_failures": {}, "text": OCR_TEXT},
}


@pytest.mark.parametrize("kind", sorted(EXTRACTORLESS))
def test_extractorless_non_pdf_sidecar_survives_refresh(tmp_path, kind):
    lib = tmp_path / "lib"
    lib.mkdir()
    good_pdf(lib / "1989_Teaching_Scan.pdf")
    sc = lib / "1989_Teaching_Scan.fulltext.json"
    sc.write_text(json.dumps(EXTRACTORLESS[kind], indent=1), encoding="utf-8")
    before = sha(sc)
    rc, out, _ = run_extract(lib, "--refresh")
    assert rc == 0 and sha(sc) == before and "KEEP" in out


def test_legacy_pipeline_sidecar_without_extractor_is_still_replaced(tmp_path):
    """Control for APPLY-1: a sidecar the extractor wrote before the `extractor` field existed is
    marked extracted_from_pdf and stays replaceable."""
    lib = tmp_path / "lib"
    lib.mkdir()
    good_pdf(lib / "2010_Teaching_Old.pdf")
    sc = lib / "2010_Teaching_Old.fulltext.json"
    sc.write_text(json.dumps({"doi": "10.5555/old.2010", "text": "old text", "extracted_from_pdf": True,
                              "sections": [], "tables": [], "n_formulas": 0}), encoding="utf-8")
    rc, _out, _ = run_extract(lib, "--refresh")
    new = sidecar(sc)
    assert rc == 0 and new["extractor"] == "pdfminer.six" and new["doi"] == "10.5555/old.2010"
    assert "Skeletal muscle" in new["text"]


def test_merged_sidecar_with_every_sibling_is_byte_identical_after_refresh(tmp_path):
    lib = tmp_path / "lib"
    lib.mkdir()
    good_pdf(lib / "2001_Teaching_Repaired.pdf")
    (lib / "2001_Teaching_Repaired.xml").write_bytes(b"<article><body><p>JATS body text here</p></body></article>")
    (lib / "2001_Teaching_Repaired.ris").write_text("TY  - JOUR\nDO  - 10.5555/teaching.r.2001\nER  - \n",
                                                     encoding="utf-8")
    shutil.copyfile(FIX / "identity_flag.identity.json", lib / "2001_Teaching_Repaired.identity.json")
    sc = lib / "2001_Teaching_Repaired.fulltext.json"
    sc.write_text(json.dumps({"extractor": "OCR (tesseract) via a consumer tool", "text": OCR_TEXT,
                              "text_preocr": "bad layer", "repair_note": "watermark-only layer replaced",
                              "doi": ""}), encoding="utf-8")
    before = {p.name: sha(p) for p in lib.iterdir()}
    rc, out, _ = run_extract(lib, "--refresh")
    assert rc == 0 and {p.name: sha(p) for p in lib.iterdir()} == before and "KEEP" in out


# ================================================================ item 4: exits through the real sweep
class ExtractStages:
    """sweep.subprocess.run stand-in: the fetch stages are test_sweep_runs.FakeStages; the extract
    stage runs the REAL extract_pdf_fulltext.main in-process. The unpaywall stage drops `pdfs` into
    its --lib-dir first, as a download would."""

    def __init__(self, fake, pdfs):
        self.fake, self.pdfs, self.extract = fake, pdfs, None

    def __call__(self, cmd, **kw):
        cmd = [str(c) for c in cmd]
        script = Path(next(c for c in cmd if c.endswith(".py"))).name
        if script == "unpaywall_fetch_v2.py":
            lib = Path(cmd[cmd.index("--lib-dir") + 1])
            lib.mkdir(parents=True, exist_ok=True)
            for name, maker in self.pdfs.items():
                maker(lib / name)
        if script != "extract_pdf_fulltext.py":
            return self.fake(cmd, **kw)
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            rc = E.main(cmd[cmd.index("--lib-dir"):])
        self.extract = types.SimpleNamespace(rc=rc, lib=Path(cmd[cmd.index("--lib-dir") + 1]),
                                             out=out.getvalue())
        return types.SimpleNamespace(returncode=rc, stdout=out.getvalue(), stderr=err.getvalue())


@pytest.fixture
def sweep_env(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).parent))
    import test_sweep_runs as tsr
    env = tsr.Env(tmp_path, monkeypatch)

    def install(pdfs):
        stages = ExtractStages(env.stages, pdfs)
        monkeypatch.setattr(sweep.subprocess, "run", stages)
        return stages
    env.install = install
    return env


def test_sweep_needs_ocr_is_a_completed_extract_stage(sweep_env):
    env = sweep_env
    stages = env.install({"2020_Teaching_Good.pdf": good_pdf, "2020_Teaching_Scan.pdf": blank_pdf})
    env.queue(["10.1000/a1"])
    rc = env.sweep()
    assert stages.extract.rc == 0
    assert sidecar(stages.extract.lib / "2020_Teaching_Scan.fulltext.json")["needs_ocr"] is True
    assert sidecar(stages.extract.lib / "2020_Teaching_Good.fulltext.json").get("needs_ocr") is None
    assert env.report()[("stage", "pdf_extract")]["detail"] == "completed"
    assert rc != sweep.EXIT_STAGE_FAILED and env.art("processed").exists()


def test_sweep_raised_extraction_fails_the_stage_but_retires_the_queue(sweep_env, monkeypatch):
    env = sweep_env
    stages = env.install({"2020_Teaching_Good.pdf": good_pdf, "2020_Teaching_Boom.pdf": good_pdf})
    real = E.extract

    def extract(path):
        if "Boom" in path:
            raise RuntimeError("extractor crashed")
        return real(path)
    monkeypatch.setattr(E, "extract", extract)
    env.queue(["10.1000/a1"])
    assert env.sweep() == sweep.EXIT_STAGE_FAILED
    assert stages.extract.rc == 2 and stages.extract.out.strip().splitlines()[-1].startswith("[step-summary] ")
    assert not (stages.extract.lib / "2020_Teaching_Boom.fulltext.json").exists()   # no empty sidecar
    assert env.report()[("stage", "pdf_extract")]["detail"] == "failed"
    assert env.art("processed").exists()


# ================================================================ item 5: consumers of the sidecar shape
DOI = "10.5555/teaching.scan.2019"


@pytest.fixture
def world(tmp_path, monkeypatch):
    root = tmp_path / "root"
    lib = root / "teaching_a" / "literature"
    lib.mkdir(parents=True)
    cfg = {"state_dir": str(tmp_path / "state"), "projects": {"teaching_a": {"lib_dir": "literature"}}}
    (tmp_path / "projects.json").write_text(json.dumps(cfg), encoding="utf-8")
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    return types.SimpleNamespace(root=root, lib=lib, cfg=cfg, cfg_path=tmp_path / "projects.json", tmp=tmp_path)


def hold(w):
    return holdings.build(w.cfg, use_cache=False, write_cache=False)


def test_needs_ocr_sidecar_keeps_its_pdf_a_holding_and_is_not_an_orphan(world):
    blank_pdf(world.lib / "2019_Smith_Scan.pdf")
    (world.lib / "2019_Smith_Scan.ris").write_text(f"TY  - JOUR\nDO  - {DOI}\nER  - \n", encoding="utf-8")
    rc, _out, _ = run_extract(world.lib)
    sc = sidecar(world.lib / "2019_Smith_Scan.fulltext.json")
    assert rc == 0 and sc["needs_ocr"] is True and sc["text"] == "" and sc["doi"] == DOI
    recs = hold(world).records(DOI)
    assert [r.kind for r in recs] == [holdings.PDF]
    audit = audit_portfolio.scan_library(world.lib)
    assert "2019_Smith_Scan.fulltext.json" in audit["empty_sidecars"]
    assert "2019_Smith_Scan.fulltext.json" not in audit["orphan_sidecars"]
    # the PDF without a DOI on record is still an orphan to fill_missing_dois (empty sidecar doi)
    (world.lib / "2019_Smith_Scan.ris").unlink()
    run_extract(world.lib, "--refresh")
    assert sidecar(world.lib / "2019_Smith_Scan.fulltext.json")["doi"] == DOI     # RC5 keeps the DOI
    blank_pdf(world.lib / "2019_Jones_Scan.pdf")
    run_extract(world.lib)
    orphans = [fn for fn, _p, _d in fill_missing_dois.discover_orphans(str(world.lib))]
    assert orphans == ["2019_Jones_Scan.pdf"]


def test_index_row_for_a_needs_ocr_sidecar(world, monkeypatch):
    import duckdb
    import index_portfolio
    monkeypatch.setattr(index_portfolio, "CONFIG_PATH", world.cfg_path)
    blank_pdf(world.lib / "2019_Smith_Scan.pdf")
    (world.lib / "2019_Smith_Scan.ris").write_text(
        f"TY  - JOUR\nAU  - Smith, Jane\nPY  - 2019\nTI  - A scan\nDO  - {DOI}\nER  - \n", encoding="utf-8")
    run_extract(world.lib)
    db = world.tmp / "refs" / "portfolio.duckdb"
    with contextlib.redirect_stdout(io.StringIO()):
        assert index_portfolio.main(["--db", str(db)]) == 0
    con = duckdb.connect(str(db), read_only=True)
    try:
        row = con.execute("SELECT has_pdf, has_sidecar, sidecar_text_len FROM paper_locations "
                          "WHERE doi = ?", [DOI]).fetchone()
    finally:
        con.close()
    assert row == (True, True, 0)


def test_text_doi_candidate_never_makes_a_holding_and_flag_merge_drops_the_sidecar_doi(world):
    other = "10.5555/another.work.2001"
    good_pdf(world.lib / "2001_Teaching_Flagged.pdf", first_extra=f"doi:{other}\n")
    shutil.copyfile(FIX / "identity_flag.identity.json", world.lib / "2001_Teaching_Flagged.identity.json")
    queue_doi = json.loads((FIX / "identity_flag.identity.json").read_text(encoding="utf-8"))["queue_doi"]
    before = hold(world)
    rc, _out, _ = run_extract(world.lib)
    sc = sidecar(world.lib / "2001_Teaching_Flagged.fulltext.json")
    assert rc == 0 and sc["identity"] == "FLAG" and sc["doi"] == "" and sc["doi_candidate"] == other
    after = hold(world)
    for d in (queue_doi, other):
        assert before.where(d) == [] and after.where(d) == []
    assert audit_portfolio.identity_flag(sc) == audit_portfolio.identity_flag(
        json.loads((FIX / "identity_flag.identity.json").read_text(encoding="utf-8")))
    # an old sidecar that carried the DOI counted as a holding until the verdict was merged
    (world.lib / "2001_Teaching_Flagged.fulltext.json").write_text(
        json.dumps({"doi": queue_doi, "text": "x", "extractor": "pdfminer.six", "extracted_from_pdf": True}),
        encoding="utf-8")
    assert hold(world).where(queue_doi) != []
    run_extract(world.lib, "--refresh")
    assert sidecar(world.lib / "2001_Teaching_Flagged.fulltext.json")["identity"] == "FLAG"
    assert hold(world).where(queue_doi) == []


def test_import_downloads_identity_reads_raw_text_and_sidecar_text_is_cleaned():
    import inspect
    import import_downloads as I
    src = inspect.getsource(I.make_sidecar)
    assert '"text": clean_pdf_text(text or "")' in src
    ident = inspect.getsource(I.identify)
    assert "clean_pdf_text" not in ident and "scan.article_text" in ident


# ================================================================ item 6: OCR and Tesseract
TESS = shutil.which("tesseract")
SPANISH = ["La resistencia aeróbica aumenta la densidad capilar del músculo esquelético.",
           "Doce voluntarios completaron seis semanas de entrenamiento en cicloergómetro.",
           "Las biopsias mostraron mayor actividad de citrato sintasa después del programa.",
           "El glucógeno muscular en reposo aumentó junto con las enzimas oxidativas.",
           "El consumo máximo de oxígeno mejoró y el tiempo hasta el agotamiento creció.",
           "Estos hallazgos sugieren adaptaciones periféricas rápidas con poco volumen.",
           "Las respuestas cardiovasculares centrales difieren entre hombres y mujeres."]


def scanned_pdf(path, lines):
    from PIL import Image, ImageDraw, ImageFont
    img = Image.new("L", (1700, 2200), 255)
    draw = ImageDraw.Draw(img)
    font = ImageFont.load_default(size=40)
    for i, line in enumerate(lines):
        draw.text((100, 150 + 90 * i), line, fill=0, font=font)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    doc = pymupdf.open()
    page = doc.new_page(width=612, height=792)
    page.insert_image(page.rect, stream=buf.getvalue())
    doc.save(str(path))
    doc.close()


@pytest.mark.skipif(TESS is None, reason="no Tesseract on PATH (CI)")
def test_real_ocr_spanish_page_and_a_missing_language(tmp_path):
    langs = E.tesseract_status()["languages"]
    if "spa" not in langs or "xxx" in langs:
        pytest.skip("Tesseract without spa")
    lib = tmp_path / "lib"
    lib.mkdir()
    scanned_pdf(lib / "1976_Teaching_Spanish.pdf", SPANISH * 2)
    (lib / "1976_Teaching_Spanish.ris").write_text("TY  - JOUR\nLA  - spa\nER  - \n", encoding="utf-8")
    scanned_pdf(lib / "1976_Teaching_Other.pdf", SPANISH)
    (lib / "1976_Teaching_Other.ris").write_text("TY  - JOUR\nLA  - xxx\nER  - \n", encoding="utf-8")
    rc, out, _ = run_extract(lib, "--ocr")
    es = sidecar(lib / "1976_Teaching_Spanish.fulltext.json")
    other = sidecar(lib / "1976_Teaching_Other.fulltext.json")
    assert rc == 0, out
    assert es["extractor"] == "tesseract" and es["ocr_lang"] == "spa+eng" and es["ocr_dpi"] == 300
    # PIL's default font draws no accented glyphs, so check unaccented words
    assert "voluntarios" in es["text"] and "resistencia" in es["text"]
    assert other["needs_ocr"] is True and other["needs_ocr_reason"] == "language xxx not installed"


@pytest.mark.skipif(TESS is None, reason="no Tesseract on PATH (CI)")
def test_real_two_language_tessdata_warns_and_carries_on_in_english(tmp_path, monkeypatch):
    src = [d for d in (os.environ.get("TESSDATA_PREFIX", ""), E.default_tessdata_dir(),
                       os.path.join(os.path.dirname(TESS), "tessdata"))
           if d and all(os.path.isfile(os.path.join(d, f"{x}.traineddata")) for x in ("eng", "osd"))]
    if not src:
        pytest.skip("no eng/osd traineddata to copy")
    two = tmp_path / "tessdata"
    two.mkdir()
    for x in ("eng", "osd"):
        shutil.copyfile(os.path.join(src[0], f"{x}.traineddata"), two / f"{x}.traineddata")
    monkeypatch.setenv("TESSDATA_PREFIX", str(two))
    lib = tmp_path / "lib"
    lib.mkdir()
    scanned_pdf(lib / "2001_Teaching_English.pdf", LINES)
    (lib / "2001_Teaching_Spanish.pdf").write_bytes((lib / "2001_Teaching_English.pdf").read_bytes())
    (lib / "2001_Teaching_Spanish.ris").write_text("TY  - JOUR\nLA  - es\nER  - \n", encoding="utf-8")
    rc, out, _ = run_extract(lib, "--ocr")
    assert rc == 0 and out.count("WARNING: Tesseract lists only 2 language(s)") == 1
    assert "TESSDATA_PREFIX is " + str(two) in out
    assert sidecar(lib / "2001_Teaching_English.fulltext.json")["ocr_lang"] == "eng"
    assert sidecar(lib / "2001_Teaching_Spanish.fulltext.json")["needs_ocr_reason"] == "language spa not installed"


def test_ocr_with_no_tesseract_anywhere_exits_2_with_step_summary(tmp_path, monkeypatch):
    monkeypatch.setenv("PATH", str(tmp_path / "empty"))
    monkeypatch.setenv("ProgramFiles", str(tmp_path / "empty"))
    lib = tmp_path / "lib"
    lib.mkdir()
    blank_pdf(lib / "2001_Teaching_Scan.pdf")
    rc, out, err = run_extract(lib, "--ocr")
    assert rc == 2 and "tesseract not found" in err
    last = json.loads(out.strip().splitlines()[-1][len("[step-summary] "):])
    assert last["counts"]["needs_ocr"] == 1 and last["reasons"][0].startswith("tesseract unavailable")


def test_no_ocr_and_no_tesseract_probe_without_the_flag(tmp_path, monkeypatch):
    def boom(*a, **k):
        raise AssertionError("OCR machinery touched without --ocr")
    monkeypatch.setattr(E, "ocr_pages", boom)
    monkeypatch.setattr(E, "tesseract_status", boom)
    monkeypatch.setattr(E, "ensure_tessdata_prefix", boom)
    lib = tmp_path / "lib"
    lib.mkdir()
    blank_pdf(lib / "2001_Teaching_Scan.pdf")
    rc, _out, _ = run_extract(lib)
    assert rc == 0 and sidecar(lib / "2001_Teaching_Scan.fulltext.json")["needs_ocr"] is True


# ================================================================ item 9: an unreadable file
def test_unreadable_pdf_records_the_read_error(tmp_path):
    lib = tmp_path / "lib"
    lib.mkdir()
    (lib / "2020_Teaching_Truncated.pdf").write_bytes(b"%PDF-1.4\n1 0 obj << /Type /Catalog >>\n")
    rc, _out, _ = run_extract(lib)
    sc = sidecar(lib / "2020_Teaching_Truncated.fulltext.json")
    assert rc == 0 and sc["needs_ocr"] is True
    assert "unreadable" in sc["needs_ocr_reason"] and "PDFMINER_ERROR" in sc["needs_ocr_reason"]


def test_an_html_page_saved_as_pdf_is_unreadable_not_an_ocr_job(tmp_path):
    # dispatcher addition (APPLY-3): PyMuPDF opens HTML as a one-page document, so "no page" alone
    # would send a publisher's 403 page to OCR; the missing %PDF header says re-fetch
    lib = tmp_path / "lib"
    lib.mkdir()
    (lib / "2020_Teaching_Forbidden.pdf").write_bytes(
        b"<!DOCTYPE html><html><body><h1>403 Forbidden</h1></body></html>")
    rc, _out, _ = run_extract(lib)
    sc = sidecar(lib / "2020_Teaching_Forbidden.fulltext.json")
    assert rc == 0 and sc["needs_ocr"] is True
    assert sc["needs_ocr_reason"].startswith("pdf unreadable")


def test_a_scan_that_every_extractor_reads_as_empty_is_still_an_ocr_job(tmp_path):
    # dispatcher addition (APPLY-3): a scan parses cleanly (every extractor reports OK, with no text),
    # so it is not "unreadable" even though extract() reports all_failed
    pdf = tmp_path / "scan.pdf"
    pdf.write_bytes(b"%PDF-1.4\n% a scan\n")
    ok = "all_failed: pdfminer=OK pdftotext=OK pdfplumber=OK"
    assert E._pdf_unreadable(str(pdf), ["", ""], ok) is False
    assert E._pdf_unreadable(str(pdf), [], ok) is False
    assert E._pdf_unreadable(str(pdf), None, "all_failed (large, pdfminer/pdfplumber skipped): pdftotext=OK") is False
    bad = "all_failed: pdfminer=PDFMINER_ERROR_PSEOF:x pdftotext=PDFTOTEXT_RC1:y pdfplumber=PDFPLUMBER_ERROR_Z:z"
    assert E._pdf_unreadable(str(pdf), [], bad) is True
    assert E._pdf_unreadable(str(pdf), ["page"], bad) is False   # PyMuPDF found a page: let OCR try


# ================================================================ item 7: build_pdf_library
OUTPUTS = ("metadata.csv", "abstracts.md", "library_report.md")


@pytest.mark.parametrize("n_good,n_bad,trips", [
    (0, 1, True), (1, 1, False), (2, 2, False), (1, 2, True), (2, 3, True), (2, 1, False), (0, 3, True)])
def test_build_error_rate_gate_boundaries(tmp_path, n_good, n_bad, trips):
    lib = tmp_path / "references" / "literature"
    out = tmp_path / "data" / "prior_art"
    lib.mkdir(parents=True)
    out.mkdir(parents=True)
    for name in OUTPUTS:
        (out / name).write_text(f"previous {name}\r\n", encoding="utf-8")
    before = {n: sha(out / n) for n in OUTPUTS}
    for i in range(n_good):
        make_pdf(lib / f"2001_Good{i}_Paper.pdf", ["Abstract\n" + page_text(j) for j in range(3)])
    for i in range(n_bad):
        blank_pdf(lib / f"2001_Scan{i}_Paper.pdf", 3)
    with contextlib.redirect_stdout(io.StringIO()) as so, contextlib.redirect_stderr(io.StringIO()):
        rc = B.main(["--base-dir", str(tmp_path)])
    after = {n: sha(out / n) for n in OUTPUTS}
    if trips:
        assert rc == 2 and after == before and (out / "metadata.errors.csv").exists()
        assert so.getvalue().strip().splitlines()[-1].startswith("[step-summary] ")
    else:
        assert rc == 0 and all(after[n] != before[n] for n in OUTPUTS)


def test_txt_dump_names_are_stable_across_runs():
    names = ["b.pdf", "B.pdf", "a.pdf", "A__2.pdf", "a__2.pdf"]
    first = B.txt_dump_names(names)
    assert first == B.txt_dump_names(list(reversed(names)))
    assert len({v.casefold() for v in first.values()}) == len(names)
