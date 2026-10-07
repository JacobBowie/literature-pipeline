"""W5-C1 side of the `best_oa_url` contract with W5-C2: the Unpaywall report's `best_oa_url` (from
the same response's best_oa_location; empty when none) is copied by sweep into a new last residual
column, and migrate's oa_blocked.md line links it. Link order: a manual preprint's landing_url,
then best_oa_url, then the refused-PMC article page, then doi.org. Hand-written reports with and
without the column."""
import csv
import json

import pytest

import lit_util
import migrate_closed_to_md as mig
import tests.test_sweep_runs as T
from litpipe.outcomes import Kind

BEST = "https://repository.example.org/bitstream/paper.pdf"


@pytest.fixture
def env(tmp_path, monkeypatch):
    return T.Env(tmp_path, monkeypatch)


def test_sweep_copies_best_oa_url_into_a_last_residual_column(env, monkeypatch):
    monkeypatch.setattr(T, "UNPW_FIELDS", T.UNPW_FIELDS + ["best_oa_url"])
    env.queue(["10.1000/blocked1", "10.1000/closed1"])
    env.stages.spec["10.1000/blocked1"] = {"unpaywall": {"oa_status": "OA", "error": "HTTP_403",
                                                         "attempts": "publisher/publishedVersion/HTTP_403",
                                                         "best_oa_url": BEST}}
    assert env.sweep() == 0
    with open(env.art("residual"), encoding="utf-8") as f:
        header = next(csv.reader(f))
    assert header[-1] == "best_oa_url"
    res = env.residual()
    assert res["10.1000/blocked1"]["residual_class"] == "OA_BLOCKED"
    assert res["10.1000/blocked1"]["best_oa_url"] == BEST and res["10.1000/closed1"]["best_oa_url"] == ""


def test_a_report_without_the_column_leaves_it_empty(env):
    env.queue(["10.1000/closed2"])
    assert env.sweep() == 0
    res = env.residual()["10.1000/closed2"]
    assert "best_oa_url" in res and res["best_oa_url"] == ""


def _sig(stage, kind, where="download", detail=""):
    return {"stage": stage, "kind": kind, "where": where, "detail": detail}


def test_the_link_order():
    base = {"doi": "10.1000/x1", "signals": [_sig("pmc", Kind.REFUSED, detail="ncbi-page/HTML")],
            "pmcid": "PMC123"}
    assert mig._oa_link({**base, "best_oa_url": BEST}) == BEST                                  # best beats PMC
    assert mig._oa_link(base) == "https://pmc.ncbi.nlm.nih.gov/articles/PMC123/"
    manual = {"doi": "10.1000/x2", "landing_url": "https://osf.example/abc", "best_oa_url": BEST,
              "signals": [_sig("preprint", Kind.SKIPPED, detail="manual_preprint:osf")]}
    assert mig._oa_link(manual) == "https://osf.example/abc"                                   # landing wins
    assert mig._oa_link({"doi": "10.1000/x3", "best_oa_url": ""}) == "https://doi.org/10.1000/x3"
    assert mig._oa_link({"doi": "10.1000/x4", "best_oa_url": "javascript:alert(1)"}) == "https://doi.org/10.1000/x4"


def _project(tmp_path, monkeypatch):
    root = tmp_path / "Projects"
    (root / "P").mkdir(parents=True)
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    return root / "P", {"projects": {"P": {"lib_dir": "lit"}}, "state_dir": str(tmp_path / "s")}


DAY = "2026-10-07"


def _write(path, header, rows):
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)


def test_migrate_links_best_oa_url_from_the_typed_residual(tmp_path, monkeypatch):
    proj, cfg = _project(tmp_path, monkeypatch)
    _write(proj / f"lit_pull_queue.{DAY}.residual.csv",
           ["doi", "title", "year", "destination", "residual_class", "reason", "attempts", "best_oa_url"],
           [["10.1000/r1", "Blocked one", "2020", "lit", "OA_BLOCKED", "unpaywall: HTTP_403", "1", BEST],
            ["10.1000/r2", "Blocked two", "2021", "lit", "OA_BLOCKED", "unpaywall: HTTP_403", "1", ""]])
    res = mig.run("P", run_id=DAY, cfg=cfg, use_holdings=False)
    assert res["status"] == "ok"
    md = (proj / mig.OA_BLOCKED_NAME).read_text(encoding="utf-8")
    assert f"[10.1000/r1]({BEST})" in md and "[10.1000/r2](https://doi.org/10.1000/r2)" in md
    with open(proj / f"lit_pull_queue.{DAY}.routing.csv", encoding="utf-8") as f:
        routing = {r["doi"]: r for r in csv.DictReader(f)}
    assert routing["10.1000/r1"]["best_oa_url"] == BEST


def test_migrate_reads_best_oa_url_from_a_legacy_unpaywall_report(tmp_path, monkeypatch):
    proj, cfg = _project(tmp_path, monkeypatch)
    _write(proj / f"lit_pull_queue.{DAY}.unpaywall.csv",
           ["doi", "title", "oa_status", "downloaded", "error", "attempts", "best_oa_url"],
           [["10.1000/l1", "Legacy", "OA", "False", "HTTP_403", "publisher/publishedVersion/HTTP_403", BEST]])
    _write(proj / f"lit_pull_queue.{DAY}.pmc.csv", ["doi", "downloaded", "error", "pmcid"],
           [["10.1000/l1", "False", "NO_PMCID", ""]])
    res = mig.run("P", run_id=DAY, cfg=cfg, use_holdings=False, skip_preprint=True)
    assert res["counts"] == {"OA_BLOCKED": 1}
    assert f"[10.1000/l1]({BEST})" in (proj / mig.OA_BLOCKED_NAME).read_text(encoding="utf-8")
