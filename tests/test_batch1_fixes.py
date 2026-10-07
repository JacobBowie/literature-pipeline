"""Batch 1 regressions — AUTO-severity correctness fixes + snowball enrich decoupling.

Covers: F1 (merge_sidecar numeric-0), F2a (max_chars=0), F2b (visible suspicious-DOI
drop), F6 (RIS newline scrub), A4 (comment-only-queue backup guard), A5 (migrate render
after dedup), D1 (partial-stage queue survival), and the run_daily snowball-enrich
decoupling. Drives the REAL modules; integration paths use monkeypatch, not mirrors.
"""
import json
import re
import sys
import types

# conftest puts the repo root on sys.path
import lit_util
import ris_emit
import migrate_closed_to_md as mig
import run_daily
import sweep


# ----------------------------------------------------------------- F1
def test_F1_is_empty_excludes_numeric_zero():
    assert lit_util._is_empty(0) is False
    assert lit_util._is_empty(0.0) is False
    assert lit_util._is_empty(False) is False
    assert lit_util._is_empty("") is True
    assert lit_util._is_empty([]) is True
    assert lit_util._is_empty({}) is True
    assert lit_util._is_empty(None) is True


def test_F1_merge_sidecar_keeps_real_zero_and_fills_empty():
    # a real 0 in `new` must NOT be clobbered by the old value
    assert lit_util.merge_sidecar({"year": 2024}, {"year": 0})["year"] == 0
    # a genuinely-absent new field is still filled from old
    assert lit_util.merge_sidecar({"title": "Kept"}, {"title": ""})["title"] == "Kept"
    # a populated new field is preserved
    assert lit_util.merge_sidecar({"title": "Old"}, {"title": "New"})["title"] == "New"


# ----------------------------------------------------------------- F2a / F2b
def test_F2a_max_chars_zero_scans_zero_chars():
    text = "see 10.1002/cphy.c140066 here"
    assert lit_util.extract_doi_from_text(text) == "10.1002/cphy.c140066"
    assert lit_util.extract_doi_from_text(text, max_chars=0) == ""   # 0 != 'no limit'
    assert lit_util.extract_doi_from_text(text, max_chars=5) == ""


def test_F2b_suspicious_doi_dropped_visibly(capsys):
    lit_util._SUSPICIOUS_DROPPED.clear()
    assert lit_util.extract_doi_from_text("ref 10.1093/nar end") == ""
    err = capsys.readouterr().err
    assert "10.1093/nar" in err and "suspicious" in err.lower()


# ----------------------------------------------------------------- F6
def test_F6_build_ris_scrubs_newlines():
    meta = {
        "type": "journal-article",
        "title": "A study of\nheat tolerance",
        "container": "Journal of\nThermal Biology",
        "abstract": "Background.\n\nMethods: second para.\nResults: third.",
        "authors": [{"family": "Smith", "given": "J"}],
        "year": "2024", "doi": "10.1/x",
    }
    ris = ris_emit.build_ris(meta)
    for line in ris.splitlines():
        if line.strip():
            assert re.match(r"^[A-Z][A-Z0-9]  - ", line), f"orphan RIS line: {line!r}"
    assert "TI  - A study of heat tolerance" in ris
    assert "AB  - Background. Methods: second para. Results: third." in ris


# ----------------------------------------------------------------- A4
def test_A4_comment_only_queue_measurement_gap(tmp_path):
    q = tmp_path / "lit_pull_queue.csv"
    q.write_text("# chase via ILL, not auto-fetchable\n# see debrief\n", encoding="utf-8")
    # OLD guard keyed on queue_data_rows (excludes '#') -> 0 -> skipped backup (the bug)
    assert run_daily.queue_data_rows(q) == 0
    # NEW guard is byte-based -> non-empty -> backs up
    assert q.stat().st_size > 0


# ----------------------------------------------------------------- A5
def test_A5_render_after_dedup_no_duplicate(tmp_path, monkeypatch):
    proj = tmp_path
    md = proj / "lit_pull_queue.md"
    md.write_text("# Manual Pull Queue\n\n- [ ] **A paper** (2024) "
                  "— DOI `10.x/a` — oa_status=closed\n", encoding="utf-8")
    cfg_path = tmp_path / "projects.json"
    cfg_path.write_text(json.dumps({"projects": {"P": {}}}), encoding="utf-8")

    monkeypatch.setattr(mig, "CONFIG_PATH", str(cfg_path))
    monkeypatch.setattr(mig, "project_dir", lambda name, pc: proj)
    row = lambda d, t, y: {"doi": d, "title": t, "year": y, "oa_status": "closed",
                           "error": "", "stage_pmc": "skip", "stage_preprint": "skip"}
    monkeypatch.setattr(mig, "read_report_chain",
                        lambda root, date: [row("10.x/a", "A paper", "2024"),
                                            row("10.y/b", "B paper", "2025")])
    monkeypatch.setattr(sys, "argv",
                        ["migrate_closed_to_md.py", "--project", "P", "--date", "2026-07-20"])
    assert mig.main() == 0
    text = md.read_text(encoding="utf-8")
    assert text.count("10.x/a") == 1   # already present -> NOT re-appended (the A5 bug)
    assert text.count("10.y/b") == 1   # genuinely new -> appended once


# ----------------------------------------------------------------- D1
def _fake_stage_run(pmc_ok: bool):
    """subprocess.run stand-in: stage 1 + preprint + extract succeed; PMC per `pmc_ok`."""
    def run(cmd, **kw):
        script = next((str(c) for c in cmd if str(c).endswith(".py")), "")
        rep = lambda flag: cmd[cmd.index(flag) + 1] if flag in cmd else None
        rc = 0
        if script.endswith("unpaywall_fetch_v2.py"):
            open(rep("--report"), "w", encoding="utf-8").write(
                "doi,downloaded,oa_status\n10.1/x,False,closed\n")
        elif script.endswith("pmc_fetch.py"):
            if pmc_ok:
                open(rep("--report-out"), "w", encoding="utf-8").write(
                    "doi,downloaded,skipped,winning_source,pmcid\n10.1/x,True,False,,PMC1\n")
            else:
                rc = 1  # crashed: no report written
        elif script.endswith("preprint_fetch.py"):
            open(rep("--report"), "w", encoding="utf-8").write("doi,downloaded,status\n")
        return types.SimpleNamespace(returncode=rc, stdout="", stderr="boom")
    return run


def _make_queue(tmp_path):
    (tmp_path / "lit").mkdir()
    q = tmp_path / "lit_pull_queue.csv"
    q.write_text("doi,title,destination\n10.1/x,Foo,lit\n", encoding="utf-8")
    return q


def test_D1_partial_stage_leaves_queue(tmp_path, monkeypatch):
    q = _make_queue(tmp_path)
    monkeypatch.setattr(sweep, "normalize_queue_for_pipeline",
                        lambda src, dst: open(dst, "w", encoding="utf-8").write(
                            open(src, encoding="utf-8").read()))
    monkeypatch.setattr(sweep.subprocess, "run", _fake_stage_run(pmc_ok=False))
    result = sweep.run_pipeline(tmp_path, q)
    assert result.get("partial") is True
    assert result.get("processed") is None
    assert q.exists()                                       # queue LEFT for re-sweep
    assert not list(tmp_path.glob("*.processed*.csv"))      # NOT renamed


def test_D1_all_stages_ok_renames_queue(tmp_path, monkeypatch):
    q = _make_queue(tmp_path)
    monkeypatch.setattr(sweep, "normalize_queue_for_pipeline",
                        lambda src, dst: open(dst, "w", encoding="utf-8").write(
                            open(src, encoding="utf-8").read()))
    monkeypatch.setattr(sweep.subprocess, "run", _fake_stage_run(pmc_ok=True))
    result = sweep.run_pipeline(tmp_path, q)
    assert not result.get("partial")
    assert not q.exists()                                   # renamed away
    assert list(tmp_path.glob("*.processed*.csv"))          # to .processed.csv


# ----------------------------------------------------------------- snowball decoupling
# W4b: run_daily is a wrapper over litpipe.runner, so the three orchestrator tests that stood here
# (snowball skipped enrich per project; enrich ran once; sweep and migrate shared the run date) pinned
# code that no longer exists. Their intents are locked in the runner's tests:
#   --with-snowball runs the walk, the reverse top-up and the index, never enrich:
#     tests/test_run_daily_exits.py::test_with_snowball_forces_the_walks_and_the_index_with_db_writes
#   portfolio-wide jobs run once per run: tests/test_runner_jobs.py
#   the route takes the sweep's own run id and date:
#     tests/test_runner.py::test_one_staged_queue_one_runs_row_one_summary_one_loose_ends_line
