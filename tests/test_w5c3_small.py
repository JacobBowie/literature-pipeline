"""W5-C3 small defects, instruments and portability rows (items 4, 6, 14; C054, C086, M320, P05,
P06, P09/PA09, Pd07; item 6 import name).

- lit_util.project_root removes a parent from a subproject key only when the key starts with
  '<parent>/' ('teaching_small2' under 'Teach' is Teach/teaching_small2, never Teach/ing_small2).
- The seeder's title filter keeps titles that start with a non-ASCII letter.
- audit_portfolio and pipeline_check report needs_ocr sidecars on their own line, never as empty or
  bad sidecars; _local_naive's docstring matches the index's TIMESTAMPTZ.
- Neutral wording: the DB-open failure names no sync product; no person or internal project in the
  touched docstrings and comments.
- The modules this task owns import PyMuPDF as `pymupdf`."""
import datetime as _dt
import json
import re
from pathlib import Path

import pytest

import audit_portfolio as ap
import lit_util
import pipeline_check as pc
from tests.test_instruments import make_acceptance_lib, world  # noqa: F401
from tests.test_seeder import World as SeedWorld, dois_of

REPO = Path(__file__).resolve().parent.parent


# ================================================================ lit_util.project_root (C086)
@pytest.mark.parametrize("key, parent, tail", [
    ("teaching_small2", "Teach", "teaching_small2"),      # not prefixed: the old code gave 'ing_small2'
    ("Teachsub", "Teach", "Teachsub"),                    # prefixed without a separator
    ("Teach/sub", "Teach", "sub"),
    ("Teach\\sub", "Teach", "sub"),
    ("sub", "Teach", "sub"),                              # a key that does not repeat its parent
    ("Teach/a/b", "Teach", "a/b"),
    ("Teach", "Teach", "Teach"),
])
def test_project_root_slices_only_a_real_parent_prefix(tmp_path, monkeypatch, key, parent, tail):
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", tmp_path)
    assert lit_util.subproject_tail(key, parent) == tail
    assert lit_util.project_root(key, {"parent": parent, "lib_dir": "x"}) == tmp_path / parent / tail


def test_project_root_of_a_top_level_key_is_unchanged(tmp_path, monkeypatch):
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", tmp_path)
    assert lit_util.project_root("research_a", {}) == tmp_path / "research_a"


# ================================================================ seeder title filter (C086 part 3)
@pytest.fixture
def seed_world(tmp_path, monkeypatch, capsys):
    return SeedWorld(tmp_path, monkeypatch, capsys)


def test_titles_starting_with_a_non_ascii_letter_are_kept(seed_world):
    w = seed_world
    w.register("teaching_a", "literature")
    w.hold("teaching_a", "10.5555/seed.0001")
    titles = {"10.5555/u.0001": "Étude de la tolérance à la chaleur",
              "10.5555/u.0002": "β-Alanine supplementation in the heat",
              "10.5555/u.0003": "Ölçek geliştirme çalışması",
              "10.5555/u.0004": "1. Introduction",
              "10.5555/u.0005": "[Not available]",
              "10.5555/u.0006": "Heat acclimation in athletes"}
    w.reverse("teaching_a", [("10.5555/seed.0001", d) for d in titles], titles=titles)
    w.index()
    rc, out, err = w.seed("--project", "teaching_a")
    assert rc == 0, err
    head, fields, rows = w.draft(w.proot("teaching_a") / "lit_pull_queue.draft.csv")
    got = set(dois_of(rows))
    assert {"10.5555/u.0001", "10.5555/u.0002", "10.5555/u.0003", "10.5555/u.0006"} <= got
    assert not got & {"10.5555/u.0004", "10.5555/u.0005"}


# ================================================================ needs_ocr (item 4) and C054
def _ocr_sidecar(lib, stem, reason="cid share 0.4"):
    (lib / f"{stem}.pdf").write_bytes(b"%PDF-1.4 scan" + b"0" * 20000)
    (lib / f"{stem}.ris").write_text(f"TY  - JOUR\nTI  - T\nDO  - 10.1/{stem.lower()}\nER  - \n", encoding="utf-8")
    (lib / f"{stem}.fulltext.json").write_text(json.dumps(
        {"doi": f"10.1/{stem.lower()}", "text": "", "needs_ocr": True, "needs_ocr_reason": reason,
         "extracted_from_pdf": True, "has_pdf": True}), encoding="utf-8")


def test_audit_reports_needs_ocr_on_its_own_line(world, capsys):
    lib = world.lib()
    make_acceptance_lib(lib)
    for s in ("2001_Ocr_One", "2002_Ocr_Two", "2003_Ocr_Three", "2004_Ocr_Four"):
        _ocr_sidecar(lib, s)
    scan = ap.scan_library(lib)
    assert sorted(scan["needs_ocr"]) == [f"{s}.fulltext.json" for s in
                                         ("2001_Ocr_One", "2002_Ocr_Two", "2003_Ocr_Three", "2004_Ocr_Four")]
    assert not any("Ocr" in n for n in scan["empty_sidecars"])          # not damage
    res = ap.run(holdings=False)
    out = capsys.readouterr().out
    lines = [ln for ln in out.splitlines() if "needs_ocr sidecars" in ln]
    assert lines and all("4 (first: " in ln for ln in lines)
    assert "2001_Ocr_One" in lines[0] and "+1 more" in lines[0] and ".fulltext.json" not in lines[0]
    assert res["summary"]["needs_ocr"] == 4
    assert "empty-content sidecars" not in out


def test_pipeline_check_lists_needs_ocr_and_does_not_fail_on_it(world, capsys):
    lib = world.lib()
    make_acceptance_lib(lib)
    _ocr_sidecar(lib, "2001_Ocr_One")
    base, lb, data, tier, thr = pc.resolve_from_config(ap.select_projects(world.projects)[0][0])
    res = pc.check_project(base, lb, data, tier, 0, key="research_alpha")
    out = capsys.readouterr().out
    assert "[INFO] needs_ocr sidecars (OCR to-do: extract_pdf_fulltext.py --ocr): 1" in out
    assert "2001_Ocr_One" in out
    assert res["counts"]["needs_ocr"] == 1
    assert not any("sidecars parse + have content" in i for i in res["issues"])


def test_local_naive_reads_the_index_runs_utc_instant_and_its_docstring_says_so():
    aware = _dt.datetime(2026, 10, 7, 12, 0, tzinfo=_dt.timezone.utc)
    assert ap._local_naive(aware) == aware.astimezone().replace(tzinfo=None)
    assert ap._local_naive("2026-10-07T12:00:00") == _dt.datetime(2026, 10, 7, 12, 0)
    doc = ap._local_naive.__doc__
    assert "TIMESTAMPTZ" in doc and "index_runs" in doc


# ================================================================ portability rows
def test_db_open_failure_names_no_sync_product(monkeypatch, tmp_path, capsys):
    import duckdb

    def locked(*a, **k):
        raise duckdb.IOException("Could not set lock on file")
    monkeypatch.setattr(duckdb, "connect", locked)
    monkeypatch.setattr(lit_util.time, "sleep", lambda s: None)
    with pytest.raises(SystemExit) as e:
        lit_util.connect_db(str(tmp_path / "x.duckdb"), on_fail="exit", tries=2)
    msg = str(e.value) + capsys.readouterr().err
    assert "another process or a sync client may hold the file" in msg
    assert "Drive" not in msg and "Google" not in msg


def test_touched_docstrings_and_comments_are_neutral():
    assert ap.__doc__.splitlines()[0].startswith("Portfolio-wide audit of literature-pipeline outputs: "
                                                  "the health check the maintainer")
    src = (REPO / "lit_util.py").read_text(encoding="utf-8")
    assert "e.g. 'course_a/" in src and "Drive" not in src
    assert re.search(r"No module reads\s+#\s+os\.environ\.get\(\"LITPIPE_EMAIL\", DEFAULT_EMAIL\)", src)
    pcsrc = (REPO / "pipeline_check.py").read_text(encoding="utf-8")
    assert "e.g. a library that also holds technical reports or data" in pcsrc


def test_no_module_reads_the_old_email_fallback():
    hits = [p.name for p in REPO.glob("*.py")
            if 'os.environ.get("LITPIPE_EMAIL", DEFAULT_EMAIL)' in p.read_text(encoding="utf-8")]
    hits += [p.name for p in (REPO / "litpipe").glob("*.py")
             if 'os.environ.get("LITPIPE_EMAIL", DEFAULT_EMAIL)' in p.read_text(encoding="utf-8")]
    assert hits == ["lit_util.py"]           # the comment that says no module reads it


# ================================================================ item 6: the import name
@pytest.mark.parametrize("name", ["audit_filenames.py", "backfill_ris.py", "import_downloads.py",
                                  "paywall_pull.py"])
def test_owned_modules_import_pymupdf(name):
    src = (REPO / name).read_text(encoding="utf-8")
    assert not re.search(r"^\s*import fitz\b", src, re.M) and re.search(r"^\s*import pymupdf\b", src, re.M)
