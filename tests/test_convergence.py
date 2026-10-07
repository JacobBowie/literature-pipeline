"""snowball.py: DEGRADED walks, the convergence log's reason column, one iteration by default,
recommendations and abstracts out of the loop, the top_candidates count (dispatch W2-D2, K3).

Every test points snowball.LOG_PATH, DB_PATH and CONFIG_PATH and lit_util.PROJECTS_ROOT at temp
paths (an autouse fixture); child steps are replaced by a recording runner except in the
streaming test, whose child is a three-line script. Fixtures in tests/fixtures/W2-D2/ hold the
recorded numbers (projects renamed)."""
import csv
import json
import shutil
import sys
import textwrap
import time
from pathlib import Path

import pytest

import forward_citations as fc
import lit_util
import snowball

FIX = Path(__file__).resolve().parent / "fixtures" / "W2-D2"
OLD_LOG = FIX / "convergence_log_v1.csv"


@pytest.fixture(autouse=True)
def temp_paths(tmp_path, monkeypatch):
    refs = tmp_path / "refs"
    monkeypatch.setattr(snowball, "LOG_PATH", refs / "convergence_log.csv")
    monkeypatch.setattr(snowball, "DB_PATH", refs / "portfolio.duckdb")
    reg = tmp_path / "projects.json"
    reg.write_text(json.dumps({"projects": {
        "teaching_a": {"lib_dir": "literature"},
        "teaching_b": {"lib_dir": "literature"},
        "research_off": {"lib_dir": "literature", "active": False}}}), encoding="utf-8")
    monkeypatch.setattr(snowball, "CONFIG_PATH", reg)
    root = tmp_path / "root"
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    for p in ("teaching_a", "teaching_b"):
        lib = root / p / "literature"
        lib.mkdir(parents=True)
        (lib / "2020_Seed.pdf").write_bytes(b"%PDF")
        (lib / "2020_Seed.ris").write_text("TY  - JOUR\nDO  - 10.5555/seed.1\nER  - \n", encoding="utf-8")
    yield tmp_path


class Runner:
    """Stands in for run_step: records every child command; `counts` scripts candidate_count per
    project (a list consumed in order, the last value repeating); `rc`/`summary` per tool stem;
    `on` runs a callback when a tool starts (e.g. to add a PDF)."""

    def __init__(self, monkeypatch, counts, rc=None, summary=None, on=None):
        self.calls = []
        self.rc = rc or {}
        self.summary = summary or {}
        self.on = on or {}
        self.counts = {k: list(v) for k, v in counts.items()}
        monkeypatch.setattr(snowball, "candidate_count", self._count)

    def _count(self, project):
        seq = self.counts[project]
        return seq.pop(0) if len(seq) > 1 else seq[0]

    def tools(self):
        return [Path(cmd[1]).stem for _, cmd in self.calls]

    def __call__(self, cmd, label):
        self.calls.append((label, list(cmd)))
        stem = Path(cmd[1]).stem
        if stem in self.on:
            self.on[stem]()
        return snowball.StepResult(label, list(cmd), self.rc.get(stem, 0), self.summary.get(stem))


def log_rows():
    return snowball.read_log()


# ================================================================ replays of the recorded numbers
def _row(rows, date, project, it):
    return next(r for r in rows if r["date"] == date and r["project"] == project and r["iter"] == str(it))


@pytest.mark.parametrize("date,project,it", [("2026-08-19", "teaching_a", 2), ("2026-08-19", "teaching_b", 2),
                                             ("2026-09-11", "teaching_a", 2)])
def test_replay_negative_growth_rows_are_degraded(date, project, it):
    """08-19 lost 2,953 candidates (-11.21 %, -14.07 %); 09-11 logged -0.20 % as converged. Every
    step exited 0 then: the count alone makes them DEGRADED now."""
    r = _row(snowball.read_log(OLD_LOG), date, project, it)
    steps = [snowball.StepResult(n, [], 0) for n in ("forward_citations", "reverse_citations", "index_portfolio")]
    status, why = snowball.classify(int(r["n_before"]), int(r["n_after"]), steps)
    assert status == "DEGRADED" and "negative growth" in why


def test_replay_0916_second_walk_is_degraded():
    """09-16: the second forward walk printed 8 resolve failures and 141 seeds with citers against
    the published 222, and exited 0. Those numbers now give exit 2 (nothing published) and, with
    the failures recorded as transport failures, DEGRADED."""
    rec = json.loads((FIX / "replay_0916.json").read_text(encoding="utf-8"))
    it1, it2 = rec["forward"]["iter1"], rec["forward"]["iter2"]
    code, reasons = fc.verdict(it2["with_doi"], it2["resolve_failed"], it2["seeds_with_citers"],
                               prior_with_citers=it1["seeds_with_citers"])
    assert code == 2 and any("fewer than the published 222" in r for r in reasons)
    step = snowball.StepResult("forward_citations", [], code,
                               {"reasons": reasons, "transport_failures": it2["resolve_failed"]})
    n = rec["candidates"]["n_after_iter1"]
    status, why = snowball.classify(n, n, [step])
    assert status == "DEGRADED" and "forward_citations exit 2" in why and "8 transport failure" in why


@pytest.mark.parametrize("counts,rc,summary", [
    ([1507, 1338], {}, {}),                                                  # 08-19
    ([14460, 14431], {}, {}),                                                # 09-11
    ([36767, 36767], {"forward_citations": 2},                               # 09-16
     {"forward_citations": {"transport_failures": 8, "reasons": ["141 seeds with citers, fewer than the published 222"]}}),
])
def test_replays_through_the_loop_log_degraded_and_exit_2(monkeypatch, counts, rc, summary):
    run = Runner(monkeypatch, {"teaching_a": counts}, rc=rc, summary=summary)
    res = snowball.run(project="teaching_a", until_convergence=True, max_iter=2, step_runner=run)
    assert res["exit_code"] == 2 and res["projects"][0]["status"] == "DEGRADED"
    rows = log_rows()
    assert len(rows) == 1 and rows[0]["reason"].startswith("DEGRADED: ")
    assert run.tools().count("forward_citations") == 1                       # the loop stopped


# ================================================================ DEGRADED and FAILED
@pytest.mark.parametrize("rc", [2, 3])
def test_forward_exit_2_or_3_is_degraded_even_with_growth(monkeypatch, rc):
    run = Runner(monkeypatch, {"teaching_a": [100, 150]}, rc={"forward_citations": rc},
                 summary={"forward_citations": {"aborted": "breaker"} if rc == 3 else {"reasons": ["2 of 10 seed walks failed"]}})
    res = snowball.run(project="teaching_a", step_runner=run)
    assert res["exit_code"] == 2
    r = log_rows()[0]
    assert r["reason"].startswith(f"DEGRADED: forward_citations exit {rc}")
    assert ("aborted: breaker" in r["reason"]) == (rc == 3)


def test_a_recorded_transport_failure_under_the_threshold_is_degraded(monkeypatch):
    run = Runner(monkeypatch, {"teaching_a": [100, 150]},
                 summary={"forward_citations": {"exit_code": 0, "transport_failures": 1}})
    res = snowball.run(project="teaching_a", step_runner=run)
    assert res["exit_code"] == 2 and "1 transport failure" in log_rows()[0]["reason"]


def test_a_count_mismatch_alone_is_not_a_transport_failure(monkeypatch):
    run = Runner(monkeypatch, {"teaching_a": [100, 150]},
                 summary={"forward_citations": {"exit_code": 0, "transport_failures": 0, "count_mismatch": 1}})
    assert snowball.run(project="teaching_a", step_runner=run)["exit_code"] == 0


def test_a_step_crash_is_failed_exit_1(monkeypatch):
    run = Runner(monkeypatch, {"teaching_a": [100, 150]}, rc={"index_portfolio": 1})
    res = snowball.run(project="teaching_a", step_runner=run)
    assert res["exit_code"] == 1 and log_rows()[0]["reason"] == "FAILED: index_portfolio(teaching_a) exit 1"


def test_degraded_and_failed_together_exit_1(monkeypatch):
    run = Runner(monkeypatch, {"teaching_a": [100, 90]}, rc={"index_portfolio": 1})
    res = snowball.run(project="teaching_a", step_runner=run)
    assert res["exit_code"] == 1 and log_rows()[0]["reason"].startswith("DEGRADED: negative growth")


def test_clean_iteration_exits_0_with_an_ok_reason(monkeypatch):
    run = Runner(monkeypatch, {"teaching_a": [100, 120]})
    assert snowball.run(project="teaching_a", step_runner=run)["exit_code"] == 0
    assert log_rows()[0]["reason"] == "ok; stop: single iteration"


# ================================================================ iterations
def test_one_iteration_by_default(monkeypatch):
    run = Runner(monkeypatch, {"teaching_a": [100, 200, 300]})
    snowball.run(project="teaching_a", step_runner=run)
    assert run.tools().count("forward_citations") == 1 and len(log_rows()) == 1


def test_until_convergence_on_an_unchanged_library_makes_no_second_pass(monkeypatch):
    run = Runner(monkeypatch, {"teaching_a": [100, 200, 300]})
    snowball.run(project="teaching_a", until_convergence=True, max_iter=3, step_runner=run)
    assert run.tools().count("forward_citations") == 1
    assert log_rows()[0]["reason"] == "ok; stop: no_new_seeds (library unchanged)"


def test_iteration_2_runs_when_the_library_changed(monkeypatch, temp_paths):
    lib = temp_paths / "root" / "teaching_a" / "literature"
    added = []

    def add_pdf():
        if not added:
            (lib / "2021_New.pdf").write_bytes(b"%PDF")
            added.append(1)
    run = Runner(monkeypatch, {"teaching_a": [100, 200, 300]}, on={"reverse_citations": add_pdf})
    snowball.run(project="teaching_a", until_convergence=True, max_iter=3, step_runner=run)
    assert run.tools().count("forward_citations") == 2
    assert [r["reason"] for r in log_rows()] == ["ok; continue: library changed",
                                                "ok; stop: no_new_seeds (library unchanged)"]


def test_a_changed_ris_doi_counts_as_a_library_change(monkeypatch, temp_paths):
    ris = temp_paths / "root" / "teaching_a" / "literature" / "2020_Seed.ris"
    run = Runner(monkeypatch, {"teaching_a": [100, 200, 300]},
                 on={"forward_citations": lambda: ris.write_text("TY  - JOUR\nDO  - 10.5555/seed.2\nER  - \n",
                                                                 encoding="utf-8")})
    snowball.run(project="teaching_a", until_convergence=True, max_iter=2, step_runner=run)
    assert run.tools().count("forward_citations") == 2
    assert log_rows()[-1]["reason"] == "ok; stop: max_iter 2"


def test_converged_stops_before_the_library_check(monkeypatch):
    run = Runner(monkeypatch, {"teaching_a": [1000, 1005]})
    snowball.run(project="teaching_a", until_convergence=True, step_runner=run)
    assert log_rows()[0]["reason"] == "ok; stop: converged (growth 0.50% < 1%)"


# ================================================================ n_before == 0
def test_growth_when_n_before_is_zero():
    assert snowball.growth_pct(0, 0) == 0.0
    assert snowball.growth_pct(0, 7) is None
    assert snowball.growth_pct(200, 150) == -25.0


def test_zero_to_zero_converges_in_one_iteration(monkeypatch, temp_paths):
    """05-20 logged three 0 -> 0 iterations with growth inf for one project."""
    lib = temp_paths / "root" / "teaching_a" / "literature"
    run = Runner(monkeypatch, {"teaching_a": [0, 0]}, on={"reverse_citations": lambda: (lib / "x.pdf").write_bytes(b"")})
    snowball.run(project="teaching_a", until_convergence=True, max_iter=3, step_runner=run)
    rows = log_rows()
    assert len(rows) == 1 and rows[0]["growth_pct"] == "0.00" and "converged" in rows[0]["reason"]


def test_first_count_logs_inf_and_is_not_degraded(monkeypatch):
    run = Runner(monkeypatch, {"teaching_a": [0, 1663]})
    assert snowball.run(project="teaching_a", step_runner=run)["exit_code"] == 0
    r = log_rows()[0]
    assert r["growth_pct"] == "inf" and float(r["growth_pct"]) == float("inf") and r["reason"].startswith("ok")


# ================================================================ recommendations and abstracts
def test_default_flags_make_zero_recommendation_calls(monkeypatch):
    run = Runner(monkeypatch, {"teaching_a": [100, 120]})
    snowball.run(project="teaching_a", step_runner=run)
    assert "enrich_recommendations" not in run.tools()
    assert run.tools() == ["forward_citations", "reverse_citations", "index_portfolio", "enrich_abstracts"]


def test_main_with_default_flags_makes_zero_recommendation_calls(monkeypatch):
    run = Runner(monkeypatch, {"teaching_a": [100, 120]})
    monkeypatch.setattr(snowball, "run_step", run)
    assert snowball.main(["--project", "teaching_a"]) == 0
    assert "enrich_recommendations" not in run.tools()


def test_run_daily_flags_make_zero_recommendation_or_abstract_calls(monkeypatch):
    run = Runner(monkeypatch, {"teaching_a": [100, 200]})
    monkeypatch.setattr(snowball, "run_step", run)
    assert snowball.main(["--project", "teaching_a", "--until-convergence", "--max-iter", "2",
                          "--skip-recs", "--skip-abstracts"]) == 0
    assert run.tools() == ["forward_citations", "reverse_citations", "index_portfolio"]


def test_with_recs_runs_once_after_every_project(monkeypatch):
    run = Runner(monkeypatch, {"teaching_a": [100, 120], "teaching_b": [50, 60]})
    monkeypatch.setattr(snowball, "run_step", run)
    assert snowball.main(["--all", "--with-recs"]) == 0
    tools = run.tools()
    assert tools.count("enrich_recommendations") == 1 and tools.count("enrich_abstracts") == 1
    assert tools[-2:] == ["enrich_recommendations", "enrich_abstracts"]       # after both loops
    assert tools.count("forward_citations") == 2                              # research_off is inactive


def test_skip_recs_wins_over_with_recs(monkeypatch):
    run = Runner(monkeypatch, {"teaching_a": [100, 120]})
    snowball.run(project="teaching_a", with_recs=True, skip_recs=True, skip_abstracts=True, step_runner=run)
    assert "enrich_recommendations" not in run.tools() and "enrich_abstracts" not in run.tools()


def test_db_writers_get_the_db_path(monkeypatch):
    run = Runner(monkeypatch, {"teaching_a": [100, 120]})
    snowball.run(project="teaching_a", with_recs=True, step_runner=run)
    for label, cmd in run.calls:
        if Path(cmd[1]).stem in ("index_portfolio", "enrich_recommendations", "enrich_abstracts"):
            assert cmd[cmd.index("--db") + 1] == str(snowball.DB_PATH)


def test_main_without_a_project_or_a_registry_is_exit_1(monkeypatch, tmp_path):
    """Exit 2 now means DEGRADED, so a usage or registry error must not use it."""
    monkeypatch.setattr(snowball, "run_step", lambda *a: pytest.fail("no step may run"))
    assert snowball.main([]) == 1
    monkeypatch.setattr(snowball, "CONFIG_PATH", tmp_path / "absent.json")
    assert snowball.main(["--all"]) == 1


# ================================================================ the convergence log
def test_old_logs_still_parse():
    rows = snowball.read_log(OLD_LOG)
    assert len(rows) == 14 and all(r["reason"] == "" for r in rows)
    assert float(rows[2]["growth_pct"]) == float("inf")
    assert list(rows[0])[:7] == snowball.LOG_FIELDS


def test_an_old_log_is_upgraded_in_place_and_keeps_every_row(temp_paths):
    snowball.LOG_PATH.parent.mkdir(parents=True)
    old_text = OLD_LOG.read_text(encoding="utf-8").replace("\r\n", "\n").replace("\n", "\r\n")
    snowball.LOG_PATH.write_bytes(old_text.encode("utf-8"))
    snowball.log_iteration("teaching_a", 1, 10, 12, 20.0, "ok; stop: single iteration")
    raw = snowball.LOG_PATH.read_bytes().decode("utf-8")
    assert raw.count("\r\n") == raw.count("\n")                              # CRLF throughout, as before
    rows = snowball.read_log()
    assert len(rows) == 15 and raw.splitlines()[0] == ",".join(snowball.LOG_FIELDS)
    old = snowball.read_log(OLD_LOG)
    assert [{k: r[k] for k in snowball.LOG_FIELDS[:6]} for r in rows[:14]] == \
           [{k: r[k] for k in snowball.LOG_FIELDS[:6]} for r in old]
    assert rows[-1]["reason"] == "ok; stop: single iteration" and rows[-1]["growth_pct"] == "20.00"


def test_a_new_log_gets_the_reason_header():
    snowball.log_iteration("teaching_a", 1, 0, 5, None, "ok")
    with open(snowball.LOG_PATH, encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))
    assert rows[0] == snowball.LOG_FIELDS and rows[1][5:] == ["inf", "ok"]


def test_legacy_positional_call_still_works():
    snowball.log_iteration("teaching_a", 1, 100, 101, 1.0)
    assert snowball.read_log()[0]["reason"] == ""


# ================================================================ the count (REG-I30)
def test_candidate_count_is_the_top_candidates_quantity_and_ignores_recommendations():
    import duckdb
    import index_portfolio
    snowball.DB_PATH.parent.mkdir(parents=True)
    con = duckdb.connect(str(snowball.DB_PATH))
    con.execute(index_portfolio.SCHEMA)
    con.executemany("INSERT INTO candidates (doi, source_type, source_seed_doi, source_project) VALUES (?,?,?,?)", [
        ("10.1/a", "forward", "10.9/s1", "teaching_a"),
        ("10.1/a", "reverse", "10.9/s2", "teaching_a"),       # same DOI twice: counted once
        ("10.1/b", "forward", "10.9/s1", "teaching_a"),
        ("10.1/held", "forward", "10.9/s1", "teaching_a"),    # a library holds it
        ("10.1/c", "forward", "10.9/s3", "teaching_b"),       # another project's
    ])
    con.execute("INSERT INTO paper_locations (doi, project) VALUES ('10.1/held', 'teaching_b'), ('10.9/s1', 'teaching_a')")
    con.execute("INSERT INTO recommendations (seed_doi, recommended_doi, rank) VALUES ('10.9/s1', '10.1/rec', 1)")
    top = con.execute("SELECT COUNT(*) FROM top_candidates WHERE list_contains(string_split(via_projects, ','), ?)",
                      ["teaching_a"]).fetchone()[0]
    con.close()
    assert snowball.candidate_count("teaching_a") == top == 2


def test_candidate_count_without_a_db_is_zero():
    assert snowball.candidate_count("teaching_a") == 0


# ================================================================ streaming
def test_run_step_streams_lines_as_they_come_with_timestamps(tmp_path, capsys, monkeypatch):
    child = tmp_path / "child.py"
    child.write_text(textwrap.dedent("""
        import os, sys, time
        print("unbuffered=" + str(os.environ.get("PYTHONUNBUFFERED")))
        time.sleep(1.5)
        print('[step-summary] {"exit_code": 3, "aborted": "breaker", "transport_failures": 2}')
        sys.exit(3)
    """), encoding="utf-8")
    seen = []
    t0 = time.monotonic()

    def stamp():
        seen.append(time.monotonic() - t0)
        return f"t{len(seen)}"
    monkeypatch.setattr(snowball, "stamp", stamp)
    res = snowball.run_step([sys.executable, str(child)], "child")
    out = capsys.readouterr().out
    assert res.rc == 3 and res.summary == {"exit_code": 3, "aborted": "breaker", "transport_failures": 2}
    lines = [ln for ln in out.splitlines() if ln.startswith("    [t")]
    assert lines[0].endswith("unbuffered=1") and "[step-summary]" in lines[1]
    # stamps: 1 header, 2 first line, 3 second line, 4 done; the first line arrived before the sleep ended
    assert seen[2] - seen[1] >= 1.0
    status, why = snowball.classify(10, 10, [res])
    assert status == "DEGRADED" and "aborted: breaker" in why and "2 transport failure" in why


# ================================================================ library_fingerprint: text-only holdings (W4b)
def _sidecar(lib, stem, **rec):
    (lib / f"{stem}.fulltext.json").write_text(json.dumps(rec), encoding="utf-8")


def test_library_fingerprint_counts_text_only_sidecars(temp_paths):
    """A text-only holding (DEC-08) is a seed, so adding one changes the library; before W4b the
    fingerprint read only PDFs and .ris DOIs and missed it."""
    lib = temp_paths / "root" / "teaching_a" / "literature"
    fp0 = snowball.library_fingerprint(lib)
    _sidecar(lib, "2016_Jats", doi="10.5555/t.1", text="JATS full text")              # pre-W2a JATS shape
    fp1 = snowball.library_fingerprint(lib)
    assert fp1 != fp0 and fp1[2] == ("2016_Jats.fulltext.json",)
    _sidecar(lib, "2017_Pmc", doi="10.5555/t.2", text="text", has_pdf=False)
    assert snowball.library_fingerprint(lib)[2] == ("2016_Jats.fulltext.json", "2017_Pmc.fulltext.json")


@pytest.mark.parametrize("rec", [
    {"doi": "10.5555/o.1", "text": "t", "has_pdf": True},                 # an orphan: its PDF is gone
    {"doi": "10.5555/o.2", "text": "t", "extracted_from_pdf": True},
    {"doi": "10.5555/o.3", "text": "t", "identity": "FLAG"},              # a review item, not a holding
    {"doi": "10.5555/o.4", "text": ""},                                    # no text
])
def test_library_fingerprint_ignores_sidecars_that_are_not_text_only_holdings(temp_paths, rec):
    lib = temp_paths / "root" / "teaching_a" / "literature"
    fp0 = snowball.library_fingerprint(lib)
    _sidecar(lib, "2018_Other", **rec)
    assert snowball.library_fingerprint(lib) == fp0


def test_library_fingerprint_skips_a_pdfs_own_sidecar_and_unreadable_files(temp_paths):
    lib = temp_paths / "root" / "teaching_a" / "literature"
    _sidecar(lib, "2020_Seed", doi="10.5555/seed.1", text="extracted", extracted_from_pdf=True)   # beside its PDF
    (lib / "2019_Broken.fulltext.json").write_text("{not json", encoding="utf-8")
    assert snowball.library_fingerprint(lib)[2] == ()


def test_a_new_text_only_holding_lets_iteration_2_run(monkeypatch, temp_paths):
    lib = temp_paths / "root" / "teaching_a" / "literature"
    run = Runner(monkeypatch, {"teaching_a": [100, 200, 300]},
                 on={"reverse_citations": lambda: _sidecar(lib, "2015_New", doi="10.5555/n.1", text="jats")})
    snowball.run(project="teaching_a", until_convergence=True, max_iter=2, step_runner=run)
    assert run.tools().count("forward_citations") == 2
