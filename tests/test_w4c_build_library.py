"""W4-C: build_pdf_library's text gate, its error-rate gate and the .txt dump names."""
import csv
import hashlib
import json
from pathlib import Path

import pymupdf
import pytest

import build_pdf_library as B

LINES = [
    "Skeletal muscle adapts to repeated bouts of exercise through changes in mitochondrial content.",
    "Twelve volunteers completed six weeks of interval training on a cycle ergometer.",
    "Biopsies from the vastus lateralis showed citrate synthase activity rising by a third.",
    "Resting glycogen content increased in parallel with the oxidative enzymes measured.",
    "Peak oxygen uptake improved modestly and time to exhaustion at a fixed workload grew.",
    "Capillary density per fibre rose in the type one fibres but not in the type two fibres.",
]
OUTPUTS = ("metadata.csv", "abstracts.md", "library_report.md")


def make_pdf(path, pages):
    doc = pymupdf.open()
    for t in pages:
        page = doc.new_page()
        if t:
            page.insert_textbox(pymupdf.Rect(40, 40, 560, 800), t, fontsize=9)
    doc.save(str(path))
    doc.close()


def good(path, tag):
    make_pdf(path, ["Abstract\n" + "\n".join(f"{tag}{i}.{n} {ln}" for n, ln in enumerate(LINES))
                    for i in range(3)])


def blank(path):
    make_pdf(path, ["", "", ""])


@pytest.fixture
def project(tmp_path):
    lib = tmp_path / "references" / "literature"
    out = tmp_path / "data" / "prior_art"
    lib.mkdir(parents=True)
    out.mkdir(parents=True)
    for name in OUTPUTS:
        (out / name).write_text(f"previous {name}\r\n", encoding="utf-8")
    return tmp_path, lib, out


def build(base, capsys=None):
    rc = B.main(["--base-dir", str(base)])
    return rc, (capsys.readouterr() if capsys else None)


def digests(out):
    return {n: hashlib.sha256((out / n).read_bytes()).hexdigest() for n in OUTPUTS}


def test_all_error_library_leaves_the_outputs_byte_identical(project, capsys):
    base, lib, out = project
    for n in ("2001_Author_One.pdf", "2002_Author_Two.pdf", "2003_Author_Three.pdf"):
        (lib / n).write_bytes(b"not a pdf at all")
    before = digests(out)
    rc, cap = build(base, capsys)
    assert rc != 0 and rc == 2
    assert digests(out) == before
    rows = list(csv.DictReader((out / "metadata.errors.csv").open(encoding="utf-8")))
    assert [r["filename"] for r in rows] == ["2001_Author_One.pdf", "2002_Author_Two.pdf",
                                            "2003_Author_Three.pdf"]
    assert all(r["text_gate"] == "error" for r in rows)
    assert "NOT overwritten" in cap.err
    last = cap.out.strip().splitlines()[-1]
    assert last.startswith("[step-summary] ") and json.loads(last[15:])["reasons"]


def test_more_than_half_failed_the_gate_trips_it(project, capsys):
    base, lib, out = project
    good(lib / "2001_Author_Good.pdf", "a")
    blank(lib / "2002_Author_ScanA.pdf")
    blank(lib / "2003_Author_ScanB.pdf")
    before = digests(out)
    rc, _ = build(base, capsys)
    assert rc == 2 and digests(out) == before
    rows = {r["filename"]: r for r in csv.DictReader((out / "metadata.errors.csv").open(encoding="utf-8"))}
    assert rows["2002_Author_ScanA.pdf"]["text_gate"].startswith("fail: no_text_layer")
    assert rows["2001_Author_Good.pdf"]["text_gate"] == "pass"
    # the failed scans left no dump (a broken run does not empty earlier dumps)
    assert not (out / "text" / "2002_Author_ScanA.txt").exists()


def test_half_failed_builds_and_flags_the_scan(project, capsys):
    base, lib, out = project
    good(lib / "2001_Author_Good.pdf", "a")
    blank(lib / "2002_Author_Scan.pdf")
    (out / "text").mkdir()
    (out / "text" / "2002_Author_Scan.txt").write_text("old garbage dump", encoding="utf-8")
    rc, cap = build(base, capsys)
    assert rc == 0
    assert not (out / "metadata.errors.csv").exists()
    rows = {r["filename"]: r for r in csv.DictReader((out / "metadata.csv").open(encoding="utf-8"))}
    assert rows["2001_Author_Good.pdf"]["text_gate"] == "pass"
    assert rows["2002_Author_Scan.pdf"]["text_gate"].startswith("fail: ")
    assert rows["2002_Author_Scan.pdf"]["abstract_snippet"] == ""
    assert (out / "text" / "2002_Author_Scan.txt").read_text(encoding="utf-8") == ""
    assert "Twelve volunteers" in (out / "text" / "2001_Author_Good.txt").read_text(encoding="utf-8")
    assert len(list((out / "text").glob("*.txt"))) == 2          # one dump per PDF (pipeline_check)
    ab = (out / "abstracts.md").read_text(encoding="utf-8")
    assert "failed the validity gate" in ab
    rep = (out / "library_report.md").read_text(encoding="utf-8")
    assert "## Text-validity gate" in rep and "2002_Author_Scan.pdf" in rep
    assert "Cleaning applied" in rep                                # pipeline_check reads it
    # line endings as before W4-C: csv CRLF rows
    assert b"\r\n" in (out / "metadata.csv").read_bytes()


def test_txt_dump_names_disambiguate_case_collisions():
    names = B.txt_dump_names(["paper.pdf", "Paper.pdf", "PAPER.pdf", "paper__2.pdf", "other.pdf"])
    assert names["PAPER.pdf"] == "PAPER.txt"               # first in sorted order keeps its name
    assert names["paper__2.pdf"] == "paper__2.txt"         # a real stem keeps its own name
    assert names["Paper.pdf"] == "Paper__3.txt" and names["paper.pdf"] == "paper__4.txt"
    assert names["other.pdf"] == "other.txt"
    assert len({v.casefold() for v in names.values()}) == len(names)
    assert B.txt_dump_names(["paper.pdf", "Paper.pdf"]) == B.txt_dump_names(["Paper.pdf", "paper.pdf"])


class _FakePage:
    def __init__(self, text):
        self.text = text

    def get_text(self, sort=False):
        return self.text


class _FakeDoc(list):
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def test_two_stems_differing_only_in_case_give_two_dumps(project, monkeypatch, capsys):
    """Two such files cannot sit side by side on Windows, so the listing and the reader are
    faked; the dumps are written for real."""
    base, lib, out = project
    texts = {"Paper_A.pdf": "upper", "paper_a.pdf": "lower"}
    real_listdir = B.os.listdir
    monkeypatch.setattr(B.os, "listdir", lambda p: list(texts) if Path(p) == lib else real_listdir(p))

    def fake_open(path):
        tag = texts[Path(path).name]
        return _FakeDoc(_FakePage(f"Abstract {tag}\n" + "\n".join(f"{tag} {p}.{i} {ln}" for i, ln in
                                                                 enumerate(LINES))) for p in range(3))
    monkeypatch.setattr(B.pymupdf, "open", fake_open)
    rc, _ = build(base, capsys)
    assert rc == 0
    dumps = sorted(p.name for p in (out / "text").glob("*.txt"))
    assert dumps == ["Paper_A.txt", "paper_a__2.txt"]
    assert "upper" in (out / "text" / "Paper_A.txt").read_text(encoding="utf-8")
    assert "lower" in (out / "text" / "paper_a__2.txt").read_text(encoding="utf-8")
    rows = {r["filename"]: r["txt_file"] for r in csv.DictReader((out / "metadata.csv").open(encoding="utf-8"))}
    assert rows == {"Paper_A.pdf": "Paper_A.txt", "paper_a.pdf": "paper_a__2.txt"}


def test_usage_and_missing_library_exit_1(tmp_path, capsys):
    assert B.main(["--base-dir", str(tmp_path)]) == 1                 # no references/literature
    (tmp_path / "references" / "literature").mkdir(parents=True)
    assert B.main(["--base-dir", str(tmp_path)]) == 1                 # no PDFs
    with pytest.raises(SystemExit) as ei:
        B.main(["--no-such-flag"])
    assert ei.value.code == 1


def test_run_returns_a_dict(project):
    base, lib, out = project
    good(lib / "2001_Author_Good.pdf", "a")
    res = B.run(base_dir=str(base))
    assert res["exit"] == 0 and res["ok"] == 1 and res["metadata"].endswith("metadata.csv")


def test_imports_pymupdf_not_fitz():
    src = Path(B.__file__).read_text(encoding="utf-8")
    assert "import fitz" not in src and "\nimport pymupdf" in src
