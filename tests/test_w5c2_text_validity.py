"""W5-C2 step 10 (item 7; C114, C215, C113): the text-validity gate and its siblings.

- A glyph-rich mojibake layer (real-looking letters, no real words) fails on its function-word share
  (few_function_words); a healthy Latin-script text in English or Spanish passes; a non-Latin text
  is exempt from the rule.
- Word measures read words whole across combining marks (Devanagari, decomposed Latin).
- extract_gated prefers PyMuPDF's text when pdfminer's passes with more than 1 % '(cid:N)' tokens
  and PyMuPDF's passes too.
- A `none(extract_failed)` sidecar with no text is retried by a plain --refresh; merged, OCR'd,
  hand-made and JATS sidecars stay kept.
- build_pdf_library writes its text dumps only after its error-rate gate decides.
"""
import random
import unicodedata

import pytest

import build_pdf_library as B
import extract_pdf_fulltext as E
from tests.test_w4c_build_library import LINES as BLINES, blank, digests, good, project  # noqa: F401

LINES = [
    "Skeletal muscle adapts to repeated bouts of exercise through changes in mitochondrial content.",
    "Twelve volunteers completed six weeks of interval training on a cycle ergometer in the laboratory.",
    "Biopsies from the vastus lateralis showed that citrate synthase activity rose by a third.",
    "Resting glycogen content increased in parallel with the oxidative enzymes that were measured.",
    "Peak oxygen uptake improved and the time to exhaustion at a fixed workload was longer.",
    "Capillary density per fibre rose in the type one fibres but not in the type two fibres.",
    "These findings suggest that brief intense training produces peripheral adaptations quickly.",
    "Central responses differed between the sexes and across the age groups of the cohort.",
]
SPANISH = [
    "El entrenamiento de resistencia aumenta la densidad capilar en los músculos de los voluntarios.",
    "Los participantes completaron seis semanas de ejercicio en un cicloergómetro con la misma carga.",
    "La actividad de las enzimas oxidativas fue mayor que en el grupo control después del estudio.",
    "Se observó una mejora del consumo de oxígeno y del tiempo hasta el agotamiento en la prueba.",
]


def english(n=40, seed=1):
    r = random.Random(seed)
    return "\n".join(r.choice(LINES) for _ in range(n))


def cipher(text, seed=7):
    """A substitution cipher of the letters: the shape of a glyph-rich mojibake layer (every
    measure but the function-word share looks like text; no word is a word)."""
    abc = "abcdefghijklmnopqrstuvwxyz"
    r = random.Random(seed)
    shuffled = list(abc)
    r.shuffle(shuffled)
    table = str.maketrans(abc + abc.upper(), "".join(shuffled) + "".join(shuffled).upper())
    return text.translate(table)


# ------------------------------------------------------------------ the function-word rule
def test_a_cipher_mojibake_layer_fails_on_function_words_and_the_original_passes():
    text = english()
    assert E.text_validity(text)["ok"], E.text_validity(text)["reasons"]
    g = E.text_validity(cipher(text))
    assert not g["ok"] and g["reasons"] == ["few_function_words"]
    assert g["metrics"]["stopword_share"] < E.MIN_STOPWORD_SHARE <= E.text_validity(text)["metrics"]["stopword_share"]


def test_a_glyph_rich_accented_mojibake_layer_fails():
    r = random.Random(3)
    glyphs = "ÃƒÂ©ÅÆØÐÑÞßæøðñþÿŒœŠšŽžƒ"
    words = ["".join(r.choice(glyphs) for _ in range(r.randint(3, 9))) for _ in range(400)]
    text = "\n".join(" ".join(words[i:i + 10]) for i in range(0, 400, 10))
    g = E.text_validity(text)
    assert "few_function_words" in g["reasons"]


def test_a_healthy_spanish_text_passes():
    r = random.Random(2)
    g = E.text_validity("\n".join(r.choice(SPANISH) for _ in range(60)))
    assert g["ok"], g["reasons"]


def test_a_non_latin_text_is_exempt_from_the_rule():
    greek = ("άσκηση αντοχής αυξάνει πυκνότητα τριχοειδών αγγείων δραστηριότητα οξειδωτικών ενζύμων "
             "σκελετικούς μύες εθελοντών συμμετείχαν μελέτη αιματική ροή καρδιακή παροχή").split()
    r = random.Random(4)
    text = "\n".join(" ".join(r.choice(greek) for _ in range(12)) + "." for _ in range(40))
    g = E.text_validity(text)
    assert g["metrics"]["latin_share"] < E.STOPWORD_LATIN_SHARE and "few_function_words" not in g["reasons"]


def test_a_short_text_is_exempt_from_the_rule():
    g = E.text_validity(cipher("\n".join(LINES)))
    assert g["metrics"]["words"] < E.STOPWORD_MIN_WORDS and "few_function_words" not in g["reasons"]


# ------------------------------------------------------------------ combining marks
def test_decomposed_latin_words_are_read_whole():
    text = unicodedata.normalize("NFD", english()).replace("the", "thé")
    m = E.text_metrics(text)
    assert m["word_char_ratio"] > 0.6 and E.text_validity(text)["ok"]
    assert E._without_marks(unicodedata.normalize("NFD", "thé")) == "the" and E._without_marks("plain") == "plain"
    assert E._without_marks("thé") == "thé"             # a precomposed letter is a letter, kept


# ------------------------------------------------------------------ the cid preference
def _gated(monkeypatch, miner_text, pages):
    monkeypatch.setattr(E, "extract", lambda p: (miner_text, "pdfminer.six", "OK"))
    monkeypatch.setattr(E, "extract_with_pdftotext", lambda p: ("", "PDFTOTEXT_NOT_FOUND"))
    return E.extract_gated("x.pdf", pages)


def _with_cid(text, share):
    words = text.split()
    n = int(len(words) * share) + 1
    r = random.Random(5)
    for k in range(n):
        words.insert(r.randrange(len(words)), f"(cid:{k % 40 + 3})")
    return " ".join(words)


def test_pdfminer_text_over_the_cid_share_gives_way_to_pymupdf(monkeypatch):
    clean = english(60)
    pages = [clean[i:i + len(clean) // 3] for i in range(0, len(clean), len(clean) // 3)][:3]
    text, ex, status, gate = _gated(monkeypatch, _with_cid(clean, 0.05), pages)
    assert ex == "PyMuPDF" and gate["ok"] and "(cid:" not in text


def test_pdfminer_text_under_the_cid_share_stays(monkeypatch):
    clean = english(60)
    pages = [clean]
    text, ex, _s, _g = _gated(monkeypatch, _with_cid(clean, 0.002), pages)
    assert ex == "pdfminer.six"


def test_pdfminer_text_stays_when_pymupdf_text_fails(monkeypatch):
    clean = english(60)
    text, ex, _s, gate = _gated(monkeypatch, _with_cid(clean, 0.05), ["", "", ""])
    assert ex == "pdfminer.six" and gate["metrics"]["cid_share"] > E.CID_PREFER_SHARE


def test_cid_share_counts_tokens_over_words():
    assert E.cid_share("") == 0.0 and E.cid_share("plain words only here") == 0.0
    assert 0.2 < E.cid_share("(cid:3) word another (cid:4) third fourth") < 0.7


# ------------------------------------------------------------------ none(extract_failed) is retried
@pytest.mark.parametrize("sidecar,replaceable", [
    ({"extractor": "none(extract_failed)", "text": ""}, True),
    ({"extractor": "none", "text": "   "}, True),
    ({"extractor": "none(extract_failed)", "text": "a hand transcription"}, False),
    ({"extractor": "none(extract_failed)", "text": "", "repair_note": "fixed by hand"}, False),
    ({"extractor": "none(extract_failed)", "text": "", "sections": [{"title": "Intro"}]}, False),
    ({"extractor": "tesseract", "text": "ocr text"}, False),
    ({"extractor": "nonesuch", "text": ""}, False),
])
def test_a_failed_extraction_record_is_retried_by_refresh(sidecar, replaceable):
    assert E.is_replaceable(sidecar) is replaceable


# ------------------------------------------------------------------ build_pdf_library: dumps after the gate
def test_a_degraded_build_writes_no_dump(project, capsys):
    base, lib, out = project
    good(lib / "2001_Author_Good.pdf", "a")
    blank(lib / "2002_Author_ScanA.pdf")
    blank(lib / "2003_Author_ScanB.pdf")
    assert B.main(["--base-dir", str(base)]) == 2
    assert not (out / "text").exists()                  # the passing PDF's dump waited for the gate


def test_a_degraded_build_leaves_earlier_dumps_byte_identical(project, capsys):
    base, lib, out = project
    good(lib / "2001_Author_Good.pdf", "a")
    blank(lib / "2002_Author_ScanA.pdf")
    blank(lib / "2003_Author_ScanB.pdf")
    (out / "text").mkdir()
    (out / "text" / "2001_Author_Good.txt").write_text("the earlier dump", encoding="utf-8")
    assert B.main(["--base-dir", str(base)]) == 2
    assert (out / "text" / "2001_Author_Good.txt").read_text(encoding="utf-8") == "the earlier dump"


def test_a_clean_build_writes_every_dump(project, capsys):
    base, lib, out = project
    good(lib / "2001_Author_Good.pdf", "a")
    good(lib / "2002_Author_Other.pdf", "b")
    assert B.main(["--base-dir", str(base)]) == 0
    assert sorted(p.name for p in (out / "text").glob("*.txt")) == ["2001_Author_Good.txt", "2002_Author_Other.txt"]


def test_a_build_killed_mid_write_leaves_every_output_whole(project, monkeypatch):
    """M134 (fixed, unlocked until now): every report and dump goes through lit_util's atomic writers,
    so a kill at the replace leaves the earlier files byte-identical and no temp file behind."""
    import lit_util
    base, lib, out = project
    good(lib / "2001_Author_Good.pdf", "a")
    good(lib / "2002_Author_Other.pdf", "b")
    (out / "text").mkdir()
    (out / "text" / "2001_Author_Good.txt").write_text("the earlier dump", encoding="utf-8")
    before = digests(out)

    def killed(src, dst):
        raise KeyboardInterrupt("killed between write and replace")
    monkeypatch.setattr(lit_util, "_replace_with_retry", killed)
    with pytest.raises(KeyboardInterrupt):
        B.main(["--base-dir", str(base)])
    assert digests(out) == before
    assert (out / "text" / "2001_Author_Good.txt").read_text(encoding="utf-8") == "the earlier dump"
    assert not [p for p in out.rglob("*.tmp")]


# ------------------------------------------------------------------ Tesseract defaults (portability, P07/Pd06)
def test_tessdata_prefix_wins_and_the_windows_fallback_is_windows_only(tmp_path, monkeypatch):
    monkeypatch.setenv("TESSDATA_PREFIX", str(tmp_path / "mine"))
    assert E.ensure_tessdata_prefix() == str(tmp_path / "mine")
    monkeypatch.delenv("TESSDATA_PREFIX")
    monkeypatch.setattr(E, "_windows", lambda: False)
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path))
    (tmp_path / "Tesseract-OCR" / "tessdata").mkdir(parents=True)
    assert E.default_tessdata_dir() == "" and E.ensure_tessdata_prefix() == ""
    monkeypatch.setenv("ProgramFiles", str(tmp_path))
    (tmp_path / "Tesseract-OCR" / "tesseract.exe").write_bytes(b"")
    monkeypatch.setattr(E.shutil, "which", lambda name: None)
    assert E._tesseract_cmd() == ""                     # never the Windows path off Windows
    monkeypatch.setattr(E, "_windows", lambda: True)
    assert E._tesseract_cmd() == str(tmp_path / "Tesseract-OCR" / "tesseract.exe")
    monkeypatch.setattr(E.shutil, "which", lambda name: "/usr/bin/tesseract")
    assert E._tesseract_cmd() == "/usr/bin/tesseract"   # PATH first, on every platform
