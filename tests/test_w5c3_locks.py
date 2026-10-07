"""W5-C3 step 9: locks for fixes that already work (M119, M128, M134, M322, M323, M324). Test-only:
each fails when its fix is reverted (the W5-C3 mutation check re-injected every one).

- audit_filenames resolves each DOI at most once and never sleeps (pacing is litpipe.net's, on
  real requests only); a DOI carried by two files goes AMBIGUOUS before any lookup.
- cascade_rename moves only figures whose name STARTS with '<old stem>.fig', and a case-only
  rename is not a collision on a case-insensitive file system (os.path.normcase).
- build_pdf_library and build_priority_paywall_queue replace their reports atomically: a crash at
  the replace leaves every earlier report byte for byte and no temp file.
- preprint_fetch (read-only here): the arXiv title phrase has no quotes or parentheses, the OSF
  title prefix has no trailing punctuation, and an OSF candidate's DOI is `links.preprint_doi`,
  never `attributes.doi` (the published version's DOI)."""
import hashlib
import os
import sys
import textwrap
from pathlib import Path
from types import SimpleNamespace

import pymupdf
import pytest

import audit_filenames as AF
import build_pdf_library as BPL
import build_priority_paywall_queue as B
import lit_util
import preprint_fetch as PF
from litpipe.outcomes import Kind, Outcome


def sha(p):
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def pdf(path, text="x", pages=1):
    d = pymupdf.open()
    for _ in range(pages):
        lines = []
        for para in text.split("\n"):
            lines.extend(textwrap.wrap(para, 95) or [""])
        d.new_page().insert_text((40, 50), "\n".join(lines[:70]), fontsize=8)
    d.save(str(path))
    d.close()
    return Path(path)


# ================================================================ M119: audit_filenames lookups
def ris(doi, au="Smith, Jane", py="2020", ti="A title"):
    return f"TY  - JOUR\nAU  - {au}\nPY  - {py}\nTI  - {ti}\nDO  - {doi}\nER  - \n"


def test_each_doi_is_resolved_at_most_once_and_nothing_sleeps(tmp_path, monkeypatch, capsys):
    lib = tmp_path / "lib"
    lib.mkdir()
    for stem, doi in (("2020_Smith_One", "10.1234/shared.1"), ("2020_Smith_Two", "10.1234/shared.1"),
                      ("2019_Jones_Three", "10.1234/own.3"), ("2018_Lee_Four", "10.1234/own.4")):
        pdf(lib / f"{stem}.pdf")
        (lib / f"{stem}.ris").write_text(ris(doi), encoding="utf-8")
    calls = []

    def lookup(doi):
        calls.append(doi)
        return {"doi": doi, "title": "Another title entirely", "year": "2020", "lastname": "Smith",
                "authors": [{"family": "Smith", "given": "Jane"}], "source": "crossref"}

    def no_sleep(*a, **k):
        raise AssertionError("audit_filenames slept")
    monkeypatch.setattr(AF, "crossref", lookup)
    monkeypatch.setattr(AF.time, "sleep", no_sleep)
    res = AF.run(lib_dir=str(lib))
    assert sorted(calls) == ["10.1234/own.3", "10.1234/own.4"]      # each once; the shared DOI never
    by = {r["current"]: r["status"] for r in res["rows"]}
    assert by["2020_Smith_One.pdf"] == by["2020_Smith_Two.pdf"] == "AMBIGUOUS"
    assert "time.sleep" not in Path(AF.__file__).read_text(encoding="utf-8")


# ================================================================ M128: cascade_rename
def test_only_figures_that_start_with_the_old_stem_move(tmp_path):
    lib = tmp_path / "lib"
    lib.mkdir()
    pdf(lib / "2020_Smith_Old.pdf")
    (lib / "2020_Smith_Old.fig1.png").write_bytes(b"fig")
    (lib / "Copy_2020_Smith_Old.fig1.png").write_bytes(b"other")      # contains the stem, not a prefix
    (lib / "X2020_Smith_Old.fig2.png").write_bytes(b"other2")
    AF.cascade_rename(str(lib), "2020_Smith_Old.pdf", "2020_Smith_New.pdf")
    names = sorted(p.name for p in lib.iterdir())
    assert names == ["2020_Smith_New.fig1.png", "2020_Smith_New.pdf", "Copy_2020_Smith_Old.fig1.png",
                     "X2020_Smith_Old.fig2.png"]


def _case_insensitive(tmp):
    probe = Path(tmp) / "CaseProbe.txt"
    probe.write_text("x", encoding="utf-8")
    try:
        return (Path(tmp) / "caseprobe.TXT").exists()
    finally:
        probe.unlink()


def test_a_case_only_rename_is_not_a_collision(tmp_path):
    if not _case_insensitive(tmp_path):
        pytest.skip("case-sensitive file system: a case-only rename cannot collide here")
    lib = tmp_path / "lib"
    lib.mkdir()
    pdf(lib / "2020_smith_heat.pdf")
    (lib / "2020_smith_heat.ris").write_text(ris("10.1234/x.1"), encoding="utf-8")
    moved = AF.cascade_rename(str(lib), "2020_smith_heat.pdf", "2020_Smith_Heat.pdf")
    assert sorted(p.name for p in lib.iterdir()) == ["2020_Smith_Heat.pdf", "2020_Smith_Heat.ris"]
    assert [k for k, *_ in moved] == ["pdf", "ris"]


def test_a_real_collision_still_stops_the_rename(tmp_path):
    lib = tmp_path / "lib"
    lib.mkdir()
    pdf(lib / "2020_Smith_A.pdf")
    pdf(lib / "2020_Smith_B.pdf")
    with pytest.raises(FileExistsError):
        AF.cascade_rename(str(lib), "2020_Smith_A.pdf", "2020_Smith_B.pdf")
    assert sorted(p.name for p in lib.iterdir()) == ["2020_Smith_A.pdf", "2020_Smith_B.pdf"]


# ================================================================ M134: atomic report writers
def _crash_at_replace(monkeypatch):
    real = os.replace

    def crash(src, dst):
        if str(src).endswith(".tmp"):
            raise OSError("crash at the replace (injected)")
        return real(src, dst)
    monkeypatch.setattr(lit_util.os, "replace", crash)


BODY = ("Participants completed a graded exercise test on a cycle ergometer while heart rate, blood "
        "pressure and oxygen uptake were recorded continuously and analysed with mixed models. ") * 12


def test_build_pdf_library_reports_survive_a_crash_at_the_replace(tmp_path, monkeypatch):
    base = tmp_path / "proj"
    lib = base / "references" / "literature"
    lib.mkdir(parents=True)
    for i in range(3):
        pdf(lib / f"202{i}_Smith_Paper{i}.pdf", BODY, pages=2)
    out = base / "data" / "prior_art"
    assert BPL.run(base_dir=str(base))["exit"] == 0
    reports = [out / "metadata.csv", out / "abstracts.md", out / "library_report.md"]
    before = {p: sha(p) for p in reports}
    pdf(lib / "2023_Smith_Paper3.pdf", BODY, pages=2)            # the next run would change all three
    _crash_at_replace(monkeypatch)
    with pytest.raises(OSError, match="injected"):
        BPL.run(base_dir=str(base))
    assert {p: sha(p) for p in reports} == before
    assert not list(out.glob("*.tmp"))


def test_the_paywall_queue_files_survive_a_crash_at_the_replace(tmp_path, monkeypatch):
    root = tmp_path / "root"
    (root / "teaching_a" / "literature").mkdir(parents=True)
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    (root / "teaching_a" / "lit_pull_queue.2026-09-01.residual.csv").write_text(
        "doi,title,year\n10.7000/closed.1,T,2020\n", encoding="utf-8")
    reg = {"state_dir": str(tmp_path / "state"), "projects": {"teaching_a": {"lib_dir": "literature"}}}
    out = tmp_path / "out"
    kw = dict(date="2026-10-07", db=str(tmp_path / "none.duckdb"), out_dir=str(out), registry=reg)
    B.run(**kw)
    files = sorted(out.glob("2026-10-07_priority_paywall_queue.*"))
    before = {p: sha(p) for p in files}
    (root / "teaching_a" / "lit_pull_queue.2026-09-02.residual.csv").write_text(
        "doi,title,year\n10.7000/closed.2,T2,2021\n", encoding="utf-8")
    _crash_at_replace(monkeypatch)
    with pytest.raises(OSError, match="injected"):
        B.run(**kw)
    assert len(files) == 2 and {p: sha(p) for p in files} == before
    assert not list(out.glob("*.tmp"))


# ================================================================ M322, M323, M324: preprint_fetch
@pytest.mark.parametrize("title, phrase", [
    ('The "hot" (and cold) truth about heat', "The hot and cold truth about heat"),
    ("<i>In vivo</i> sweat (sodium)", "In vivo sweat sodium"),
])
def test_arxiv_title_phrase_has_no_quotes_or_parentheses(title, phrase):
    got = PF._arxiv_phrase(title)
    assert got == phrase and not set('"()') & set(got)


def test_arxiv_title_phrase_is_cut_at_a_word_boundary():
    got = PF._arxiv_phrase("word " * 60)
    assert len(got) <= 200 and not got.endswith(" ") and got.split()[-1] == "word"


@pytest.mark.parametrize("title, prefix", [
    ("Heat stress in athletes: a review.", "Heat stress in athletes: a review"),
    ("Does it work?!", "Does it work"),
    ("A very long preprint title about heat acclimation and its effects on endurance performance",
     "A very long preprint title about heat acclimation and its"),
])
def test_osf_title_prefix_has_no_trailing_punctuation(title, prefix):
    assert PF._title_prefix(title) == prefix


def test_osf_candidate_doi_is_links_preprint_doi_never_attributes_doi(monkeypatch):
    data = {"data": [
        {"id": "abcde", "attributes": {"title": "A preprint", "date_published": "2021-05-01",
                                       "doi": "10.9999/journal.vor.1"},
         "links": {"preprint_doi": "https://doi.org/10.31236/osf.io/abcde", "html": "https://osf.io/abcde"},
         "relationships": {"primary_file": {"data": {"id": "f1"}}}},
        {"id": "fghij", "attributes": {"title": "No preprint DOI", "doi": "10.9999/journal.vor.2"},
         "links": {"html": "https://osf.io/fghij"}}]}
    seen = {}

    def fake_get(url, **kw):
        seen["url"], seen["params"] = url, kw.get("params")
        return Outcome(Kind.OK, status=200, host="api.osf.io", detail="", attempts=1, elapsed_ms=1,
                       payload=SimpleNamespace(json=lambda: data))
    monkeypatch.setattr(PF.net, "get", fake_get)
    o = PF.osf_title_search("Heat stress in athletes: a review.")
    assert o.ok and seen["params"]["filter[title]"] == "Heat stress in athletes: a review"
    a, b = o.payload
    assert a.doi == "10.31236/osf.io/abcde" and a.osf_file == "f1"
    assert b.doi == ""                                 # never the published version's DOI


def test_the_arxiv_title_search_still_runs_in_the_stage():
    """Record correction (W5 plan item 13): N-D5 fixed the phrase; it did not remove the search."""
    src = Path(PF.__file__).read_text(encoding="utf-8")
    assert 'row.add("arxiv_search", arxiv_title_search(' in src
