"""W4-C: the text-validity gate (extract_pdf_fulltext.text_validity) and clean_pdf_text's spaces.

The gate reads the extracted text (word count, distinct words, the share of the most common word,
printable and word-character ratios, suspect_file's watermark rule) and PyMuPDF's per-page text
(the share of blank or degenerate pages). The two teaching-library papers whose text layers
started W4-C fail it when their PDFs are on this machine (found by name and a pinned sha256 under
lit_util.PROJECTS_ROOT; skipped otherwise).
"""
import hashlib
import inspect
import shutil

import pytest

import extract_pdf_fulltext as E
import lit_util
from litpipe import identity
from pdf_text_clean import clean_pdf_text

PARA = (
    "Skeletal muscle adapts to repeated bouts of exercise through changes in mitochondrial "
    "content, capillary density and the activity of oxidative enzymes. Twelve volunteers "
    "completed six weeks of interval training on a cycle ergometer, three sessions each week. "
    "Biopsies from the vastus lateralis showed citrate synthase activity rising by about thirty "
    "percent, while resting glycogen content increased in parallel. Peak oxygen uptake "
    "improved modestly and time to exhaustion at a fixed workload was extended.\n")
_N = iter(range(10**6))


def body(n_lines):
    """`n_lines` distinct paragraph lines (real text never repeats a whole line)."""
    return "".join(f"Paragraph {next(_N)}. " + PARA for _ in range(n_lines))


HEALTHY_PAGES = [body(3), body(3), body(3)]
HEALTHY = "\n\n".join(HEALTHY_PAGES)
STAMP = "Brought to you by TEACHING LIBRARY | Unauthenticated | Downloaded 09/15/26 10:11 PM UTC"


# ---------------------------------------------------------------- the gate on synthetic texts
def test_healthy_text_passes():
    g = E.text_validity(HEALTHY, HEALTHY_PAGES)
    assert g["ok"], g
    assert g["reasons"] == [] and g["bad_pages"] == []
    assert g["metrics"]["unique_words"] >= E.MIN_UNIQUE_WORDS


def test_empty_text_is_no_text_layer():
    g = E.text_validity("", ["", "", ""])
    assert not g["ok"]
    assert "no_text_layer" in g["reasons"] and "image_only_pages" in g["reasons"]


def test_cid_garbage_fails_one_word_dominates():
    """pdfminer's output for a font it cannot map: '(cid:N)' for nearly every glyph."""
    text = " ".join(f"(cid:{i % 40})(cid:{(i * 7) % 40})" for i in range(3000)) + "\nAbstract"
    g = E.text_validity(text)
    assert not g["ok"]
    assert "one_word_dominates" in g["reasons"]
    assert g["metrics"]["top_word"] == "cid"


def test_partial_cid_garbage_still_fails():
    """Real text under a flood of unmapped glyphs: the most common word is still 'cid'."""
    text = body(4) + " ".join(f"(cid:{i % 50})" for i in range(4000))
    g = E.text_validity(text)
    assert "one_word_dominates" in g["reasons"]


def test_watermark_only_text_fails():
    """The proxy stamp on every page of an image-only scan, and nothing else."""
    text = "\n\n".join([STAMP] * 22)
    g = E.text_validity(text, [STAMP + "\n"] * 22)
    assert not g["ok"]
    assert {"few_unique_words", "watermark_only_text", "image_only_pages"} <= set(g["reasons"])


def test_long_stamp_repeated_line_confirmed():
    """A stamp longer than the thin rule's 200 characters a page: the repeated-line half of
    suspect_file's rule flags it, and nothing is left once that line is removed."""
    long_stamp = (STAMP + " ") * 4
    text = "\n".join([long_stamp] * 10)
    assert identity.text_stats(text, 10)["chars_per_page"] >= E.WATERMARK_CHARS_PER_PAGE
    assert E.watermark_only(text, 10)
    assert "watermark_only_text" in E.text_validity(text, n_pages=10)["reasons"]


def test_running_head_on_half_the_lines_is_not_a_watermark():
    """suspect_file flags a healthy paper whose 'ARTICLE IN PRESS' head is half its lines (the
    calibration found three); the gate confirms the rule on the text without that line."""
    lines = []
    for _ in range(30):
        lines += ["ARTICLE IN PRESS", body(1).strip()]
    text = "\n".join(lines)
    assert identity.suspect_file(None, None, None, identity.text_stats(text, 10)).reasons == (
        "watermark_only_text",)
    g = E.text_validity(text, n_pages=10)
    assert g["ok"], g


def test_unprintable_text_fails():
    """PyMuPDF's form of a broken font map: control characters where letters should be."""
    junk = "".join(chr(1 + (i % 30)) for i in range(4000))
    g = E.text_validity(junk + " the study", [junk[:2000], junk[2000:]])
    assert not g["ok"]
    assert "unprintable_characters" in g["reasons"]
    # a page of junk is not a blank page: the document measures judge it, not the page share
    # (PyMuPDF may garble a font another extractor reads cleanly)
    assert "image_only_pages" not in g["reasons"] and g["bad_pages"] == []


def test_garbled_pymupdf_pages_do_not_fail_a_clean_text():
    """The calibration case: pdftotext reads the paper, PyMuPDF's per-page text is mojibake."""
    junk = ["".join(chr(0xE000 + ((i * 7 + p * 13) % 500)) for i in range(3000)) for p in range(7)]
    g = E.text_validity(HEALTHY, junk)
    assert g["ok"], g
    assert not E.text_validity("\n".join(junk), junk)["ok"]      # the PyMuPDF text itself fails


def test_few_word_characters_fails():
    text = " ".join(["1.23 4.56 7.89 0.12 3.45"] * 400) + " " + PARA
    g = E.text_validity(text)
    assert "few_word_characters" in g["reasons"]


def test_scanned_section_fails_on_pages_not_on_average():
    """Two cover pages with real text, then eight image-only pages with a stamp: the document
    average is not thin, the per-page share is."""
    pages = [body(6), body(6)] + [STAMP] * 8
    text = "\n\n".join(pages)
    g = E.text_validity(text, pages)
    assert "watermark_only_text" not in g["reasons"]
    assert "image_only_pages" in g["reasons"]
    assert g["bad_pages"] == list(range(2, 10))
    assert g["metrics"]["bad_page_share"] == 0.8


def test_a_few_figure_pages_pass():
    pages = [body(4) for _ in range(7)] + ["Figure 3", "", "Table 2  12.1  13.4  15.0"]
    g = E.text_validity("\n\n".join(pages), pages)
    assert g["ok"], g
    assert g["metrics"]["bad_page_share"] == 0.3


def test_bad_page_share_threshold_is_inclusive():
    blank = ""
    pages = [body(4), body(4), blank, blank, blank]          # 0.6
    assert "image_only_pages" in E.text_validity("\n".join(pages), pages)["reasons"]
    pages = [body(4), body(4), body(4), blank, blank]        # 0.4
    assert "image_only_pages" not in E.text_validity("\n".join(pages), pages)["reasons"]


def test_boilerplate_needs_three_pages():
    assert E.boilerplate_lines(["a\nb", "a\nc"]) == set()
    assert E.boilerplate_lines(["stamp\nx", "stamp\ny", "stamp\nz"]) == {"stamp"}


def test_thresholds_are_the_calibrated_values():
    """Lock the calibrated thresholds (evidence: calib_thresholds.json); a change re-runs the
    calibration."""
    assert (E.MIN_UNIQUE_WORDS, E.MAX_TOP_WORD_SHARE, E.MIN_WORDS_FOR_SHARE) == (20, 0.3, 50)
    assert (E.MIN_PRINTABLE_RATIO, E.MIN_WORD_CHAR_RATIO) == (0.85, 0.10)
    assert E.WATERMARK_CHARS_PER_PAGE == identity.WATERMARK_CHARS_PER_PAGE == 200
    assert (E.PAGE_MIN_CHARS, E.MAX_BAD_PAGE_SHARE, E.BOILERPLATE_SHARE) == (40, 0.6, 0.8)


def test_doi_window_matches_fill_missing_dois():
    import fill_missing_dois
    assert E.DOI_TEXT_WINDOW == fill_missing_dois.FRONT_MATTER_CHARS


# ---------------------------------------------------------------- the two teaching-library layers
TEACHING_FILES = {
    "2001_Gibala_RegulationSkeletalMuscleAminoAcidMetabolism.pdf":
        "f218a9c6b7d0483451d789840061b10151c931c520a8e10dd43728abdbb20ac0",
    "2003_Johansen_P31MRSCharacterizationOfSprintEndurance.pdf":
        "9b7509b1bc5a338fd53545662a38824659454fca0194909ca69cb4a020ef46ca",
}


def _find(name, sha):
    root = lit_util.PROJECTS_ROOT
    for pattern in (f"*/*/literature/{name}", f"*/literature/{name}"):
        for p in sorted(root.glob(pattern)):
            if hashlib.sha256(p.read_bytes()).hexdigest() == sha:
                return p
    return None


@pytest.mark.parametrize("name", sorted(TEACHING_FILES))
def test_teaching_library_bad_layers_fail_the_gate(name, tmp_path):
    src = _find(name, TEACHING_FILES[name])
    if src is None:
        pytest.skip(f"{name} (pinned sha256) is not under PROJECTS_ROOT on this machine")
    pdf = tmp_path / name
    shutil.copyfile(src, pdf)                       # extract a copy, never the library's file
    pages, err = E.pdf_page_texts(str(pdf))
    assert pages, err
    # the bad layers: the pipeline's first extraction (pdfminer) and PyMuPDF's text both fail
    text, extractor, _st = E.extract(str(pdf))
    first = E.text_validity(clean_pdf_text(text), pages)
    assert extractor == "pdfminer.six" and not first["ok"], first
    pym = E.text_validity(clean_pdf_text("\n\n".join(pages)), pages)
    assert not pym["ok"], pym
    import json
    sc_path = tmp_path / (name[:-4] + ".fulltext.json")
    if "Gibala" in name:
        # the proxy stamp alone: thin, few words, every page blank once the stamp is removed
        assert {"few_unique_words", "watermark_only_text", "image_only_pages"} <= set(first["reasons"])
        res = E.run(lib_dir=str(tmp_path))
        assert res["exit"] == 0 and res["counts"]["needs_ocr"] == 1
        sc = json.loads(sc_path.read_text(encoding="utf-8"))
        assert sc["needs_ocr"] is True and sc["text"] == ""
        assert sc["needs_ocr_reason"] and sc["text_metrics"]["n_pages"] == len(pages)
        assert "doi_candidate" not in sc                # never from a gate-failed text
    else:
        # mojibake from pdfminer ('(cid:N)') and PyMuPDF; poppler's pdftotext decodes the font,
        # so the gated chain writes a clean pdftotext text instead of a needs_ocr sidecar
        assert {"few_unique_words", "one_word_dominates"} <= set(first["reasons"])
        res = E.run(lib_dir=str(tmp_path))
        sc = json.loads(sc_path.read_text(encoding="utf-8"))
        assert res["exit"] == 0
        if E.PDFTOTEXT:
            assert sc["extractor"] == "pdftotext" and "needs_ocr" not in sc
            assert "sprint trained" in sc["text"] and "(cid:" not in sc["text"]
        else:
            assert sc["needs_ocr"] is True and sc["text"] == ""


# ---------------------------------------------------------------- clean_pdf_text spaces
def test_nbsp_and_thin_spaces_become_plain_spaces():
    raw = "10 mg, 5 kg, 3 min, 1 234 and VO​2max"
    assert clean_pdf_text(raw) == "10 mg, 5 kg, 3 min, 1 234 and VO2max"


def test_spaces_normalisation_is_idempotent_and_optional():
    raw = "a b c​d"
    once = clean_pdf_text(raw)
    assert clean_pdf_text(once) == once
    assert clean_pdf_text(raw, normalize_spaces=False) == raw


def test_existing_keywords_and_defaults_unchanged():
    params = inspect.signature(clean_pdf_text).parameters
    assert params["expand_ligatures"].default is True
    assert params["aggressive_dehyphenate"].default is False
    assert params["strip_page_numbers"].default is False
    assert params["normalize_typography"].default is False
    assert params["normalize_spaces"].default is True
    assert all(p.kind is inspect.Parameter.KEYWORD_ONLY for n, p in params.items() if n != "text")
    # the other transforms behave as before
    assert clean_pdf_text("ﬁne") == "fine"
    assert clean_pdf_text("train-\ning") == "train-\ning"
    assert clean_pdf_text("train-\ning", aggressive_dehyphenate=True) == "training"
    assert clean_pdf_text("x\n12\ny") == "x\n12\ny"
    assert clean_pdf_text("x\n12\ny", strip_page_numbers=True) == "x\ny"
    assert clean_pdf_text("“q”") == "“q”"
    assert clean_pdf_text("“q”", normalize_typography=True) == '"q"'


def test_pdf_text_clean_top_level_is_stdlib_only():
    """litpipe.text imports LIGATURES from it: a third-party or litpipe import at the top would
    risk an import cycle. The __main__ probe imports pymupdf (not fitz) inside the block."""
    import pathlib
    import pdf_text_clean
    src = pathlib.Path(pdf_text_clean.__file__).read_text(encoding="utf-8")
    top = [ln for ln in src.splitlines() if ln.startswith(("import ", "from "))]
    assert top == ["import re"]
    assert "import fitz" not in src and "import pymupdf" in src
