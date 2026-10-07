"""python -m litpipe.runner `run` (dispatch W4-A; the W4b amendments).

In-process: runner.run / runner.main with the shim run in this process (runner.inprocess_launcher),
the stand-in stage modules of tests/fixtures/W4-A, stubbed preflight and canaries, and a temp
registry whose state_dir, root, db_dir and loose_ends are temp. No test runs a real fetch stage.
Real child processes are in tests/test_runner_procs.py, the batch in tests/test_runner_batch.py."""
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

FIX = Path(__file__).resolve().parent / "fixtures" / "W4-A"
sys.path.insert(0, str(FIX))

import lit_util  # noqa: E402
from litpipe import canaries, config, ledger, runner, state, walk  # noqa: E402
from litpipe.outcomes import Kind, Outcome  # noqa: E402
from w4a_world import World  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
DAY = 86400.0


@pytest.fixture
def w(tmp_path, monkeypatch):
    world = World(tmp_path, monkeypatch)
    yield world


def run(**kw):
    kw.setdefault("profile", "daily")
    return runner.run(**kw)


# ================================================================ amendment 1: imports
def test_import_leaves_every_stage_module_out_of_sys_modules():
    stage = ["sweep", "migrate_closed_to_md", "forward_citations", "reverse_citations", "index_portfolio",
             "enrich_abstracts", "enrich_recommendations", "audit_portfolio", "seed_queue_from_top_candidates",
             "snowball", "litpipe.enrich_s2", "unpaywall_fetch_v2", "pmc_fetch", "preprint_fetch",
             "extract_pdf_fulltext", "litpipe.worklists"]
    code = ("import sys, json, litpipe.runner as r; "
            f"print(json.dumps([m for m in {stage!r} if m in sys.modules]))")
    out = subprocess.run([sys.executable, "-c", code], cwd=str(REPO), capture_output=True, text=True, timeout=120)
    assert out.returncode == 0, out.stderr
    assert json.loads(out.stdout.strip().splitlines()[-1]) == []


def test_stage_modules_table_names_every_stage():
    assert runner.STAGE_MODULES == {
        "seed": "seed_queue_from_top_candidates", "sweep": "sweep", "route": "migrate_closed_to_md",
        "walk": "forward_citations", "reverse": "reverse_citations", "index": "index_portfolio",
        "abstracts": "enrich_abstracts", "recommendations": "enrich_recommendations",
        "audit": "audit_portfolio", "extract": "extract_pdf_fulltext"}


# ================================================================ amendment 2: the result table
def _sr(result, **kw):
    return runner.StageRun(kw.pop("rc", 0), result, **kw)


SWEEP_RES = lambda code, retired=(True,), refused=(): {  # noqa: E731
    "exit_code": code, "projects": {"P": {"run_id": "2026-10-07", "refused": list(refused), "results": [
        {"retired": r, "failed_stages": ["pmc"] if not r else [], "queue": "lit_pull_queue.csv"} for r in retired]}}}


@pytest.mark.parametrize("job,result,want", [
    ("sweep", SWEEP_RES(0), (runner.OK, 0)),
    ("sweep", SWEEP_RES(1), (runner.SKIPPED, 1)),
    ("sweep", SWEEP_RES(2), (runner.FAILED, 2)),
    ("sweep", SWEEP_RES(3, retired=(False,)), (runner.FAILED, 3)),
    ("sweep", SWEEP_RES(3, retired=(True, True)), (runner.DEGRADED, 3)),
    ("sweep", SWEEP_RES(3, retired=(True, False)), (runner.FAILED, 3)),
    ("sweep", SWEEP_RES(4, refused=("lit_pull_queue.x.csv",)), (runner.DEGRADED, 4)),
    ("route", {"status": "ok"}, (runner.OK, 0)),
    ("route", {"status": "nothing"}, (runner.SKIPPED, 0)),
    ("route", {"status": "config"}, (runner.FAILED, 2)),
    ("route", {"status": "error", "error": "x"}, (runner.FAILED, 2)),
    ("abstracts", {"interrupted": True, "stopped_early": False, "write_failures": []}, (runner.ABORTED, 130)),
    ("abstracts", {"interrupted": False, "stopped_early": True, "write_failures": [], "stop_reason": "403"},
     (runner.DEGRADED, 1)),
    ("abstracts", {"interrupted": False, "stopped_early": False, "write_failures": ["10.1/x"]}, (runner.FAILED, 1)),
    ("abstracts", {"interrupted": False, "stopped_early": True, "write_failures": ["10.1/x"]}, (runner.FAILED, 1)),
    ("abstracts", {"interrupted": False, "stopped_early": False, "write_failures": []}, (runner.OK, 0)),
    ("extract", {"exit": 0}, (runner.OK, 0)),
    ("extract", {"exit": 1}, (runner.FAILED, 1)),
    ("extract", {"exit": 2, "reasons": ["OCR failed on 1 PDF(s)"]}, (runner.DEGRADED, 2)),
    ("audit", {"exit_code": 0}, (runner.OK, 0)),
    ("audit", {"exit_code": 1}, (runner.DEGRADED, 1)),
    ("audit", {"exit_code": 2}, (runner.SKIPPED, 2)),
    ("recommendations", {"status": "off", "exit_code": 0}, (runner.SKIPPED, 0)),
    ("recommendations", {"status": "no_key", "exit_code": 0}, (runner.SKIPPED, 0)),
    ("recommendations", {"status": "ok", "exit_code": 0}, (runner.OK, 0)),
    ("recommendations", {"status": "degraded", "exit_code": 2, "reasons": ["x"]}, (runner.DEGRADED, 2)),
    ("recommendations", {"status": "interrupted", "exit_code": 130}, (runner.ABORTED, 130)),
    ("walk", {"exit_code": 0}, (runner.OK, 0)),
    ("walk", {"exit_code": 1, "error": "no registry"}, (runner.FAILED, 1)),
    ("walk", {"exit_code": 2, "reasons": ["3 of 10 seed walks failed (over 5 %)"]}, (runner.DEGRADED, 2)),
    ("walk", {"exit_code": 3, "aborted": "budget"}, (runner.DEGRADED, 3)),
    ("walk", {"exit_code": 7}, (runner.FAILED, 7)),
    ("reverse", {"exit_code": 2, "reasons": ["x"]}, (runner.DEGRADED, 2)),
    ("index", {"exit_code": 0}, (runner.OK, 0)),
    ("index", {"exit_code": 1}, (runner.FAILED, 1)),
    ("index", {"exit_code": 2, "reasons": ["skipped P: library unreadable"]}, (runner.DEGRADED, 2)),
    ("seed", {"exit_code": 0}, (runner.OK, 0)),
    ("seed", {"exit_code": 1}, (runner.FAILED, 1)),
    ("seed", {"exit_code": 2, "reasons": ["pmc-check: 1 of 2 lookup(s) failed"]}, (runner.DEGRADED, 2)),
])
def test_result_table_every_row(job, result, want):
    status, _, code = runner.classify(job, _sr(result), "P")
    assert (status, code) == want


def test_result_table_reasons_name_what_happened():
    assert runner.classify("walk", _sr({"exit_code": 3, "aborted": "budget"}))[1].startswith("aborted (budget)")
    assert "403" in runner.classify("abstracts", _sr({"interrupted": False, "stopped_early": True,
                                                      "write_failures": [], "stop_reason": "403"}))[1]
    assert runner.classify("audit", _sr({"exit_code": 1}))[1] == "FAIL items"
    assert "lit_pull_queue.x.csv" in runner.classify("sweep", _sr(SWEEP_RES(4, refused=("lit_pull_queue.x.csv",))),
                                                     "P")[1]


@pytest.mark.parametrize("sr,status,reason", [
    (dict(result=None, problem="no result file", rc=0), runner.ERROR, "no result file"),
    (dict(result=None, problem="empty result file", rc=0), runner.ERROR, "empty result file"),
    (dict(result=None, problem="malformed result file (x)", rc=0), runner.ERROR, "malformed"),
    (dict(result=None, problem="", rc=1), runner.ERROR, "shim exit 1"),
    (dict(result=None, problem="", rc=None, killed="timeout", timeout_s=7), runner.ERROR, "timeout after 7 s"),
    (dict(result=None, problem="", rc=None, killed="heartbeat"), runner.ABORTED, "heartbeat lost"),
    (dict(result={"exit_code": 0}, traceback=True), runner.DEGRADED, "traceback in the stage log"),
])
def test_result_table_shim_failures(sr, status, reason):
    rc = sr.pop("rc", 0)
    res = sr.pop("result")
    got = runner.classify("walk", runner.StageRun(rc, res, **sr))
    assert got[0] == status and reason in got[1]


@pytest.mark.parametrize("job,res", [("sweep", {"exit_code": 0}), ("route", {"rows": 1}),
                                     ("abstracts", {"interrupted": False, "write_failures": []}),
                                     ("walk", {"status": "ok"}), ("extract", {"exit_code": 0}),
                                     ("recommendations", {"status": "ok"})])
def test_a_dict_without_the_tables_keys_is_error(tmp_path, job, res):
    out = tmp_path / "r.json"
    out.write_text(json.dumps(res), encoding="utf-8")
    got, why = runner.read_result(out, runner.RESULT_TABLE[job][0])
    assert got is None and why.startswith("result lacks")


def test_read_result_missing_empty_malformed_and_not_an_object(tmp_path):
    p = tmp_path / "r.json"
    assert runner.read_result(p) == (None, "no result file")
    p.write_text("  \n", encoding="utf-8")
    assert runner.read_result(p) == (None, "empty result file")
    p.write_text("{not json", encoding="utf-8")
    assert runner.read_result(p)[1].startswith("malformed result file")
    p.write_text("[1, 2]", encoding="utf-8")
    assert runner.read_result(p)[1].startswith("result is not a JSON object")


def test_shim_writes_a_redacted_result_and_restores_its_bindings(w, monkeypatch, tmp_path):
    import w4a_fake_walk
    w.register("research_a")
    w.set_fake("walk", "research_a", result={"note": "contact me at someone@university.edu"})
    before = (config.CONFIG_PATH, lit_util.PROJECTS_ROOT, state.DB_PATH, ledger.LEDGER_DIR)
    out = tmp_path / "res.json"
    runner.shim("w4a_fake_walk", {"project": "research_a"}, out, w.cfg_path, restore=True)
    assert (config.CONFIG_PATH, lit_util.PROJECTS_ROOT, state.DB_PATH, ledger.LEDGER_DIR) == before
    text = out.read_text(encoding="utf-8")
    assert "someone@university.edu" not in text and json.loads(text)["exit_code"] == 0
    assert w4a_fake_walk  # imported


def test_shim_binds_the_registry_paths_before_the_import(w, tmp_path):
    w.register("research_a")
    seen = {}
    import types
    mod = types.ModuleType("w4a_probe_stage")
    mod.CONFIG_PATH = Path("nowhere")

    def probe_run(**kw):
        seen.update(cfg=config.CONFIG_PATH, root=lit_util.PROJECTS_ROOT, db=state.DB_PATH,
                    ledger=ledger.LEDGER_DIR, own=mod.CONFIG_PATH)
        return {"exit_code": 0}
    mod.run = probe_run
    sys.modules["w4a_probe_stage"] = mod
    try:
        other = tmp_path / "other"
        other.mkdir()
        reg = other / "projects.json"
        reg.write_text(json.dumps({"root": str(other / "R"), "state_dir": str(other / "S"), "projects": {}}),
                       encoding="utf-8")
        runner.shim("w4a_probe_stage", {}, tmp_path / "o.json", reg, restore=True)
    finally:
        del sys.modules["w4a_probe_stage"]
    assert seen == {"cfg": reg, "root": other / "R", "db": other / "S" / state.DB_NAME,
                    "ledger": other / "S" / "ledger", "own": reg}
    assert mod.CONFIG_PATH == Path("nowhere")


# ================================================================ acceptance 1: the dry run
def _dry_world(w):
    w.register("research_a", walk_cadence_days=7)
    w.register("teaching_b", auto_stage=True)
    w.register("research_c")
    w.queue("research_a", ["10.5555/w4a.0001"])
    return w


@pytest.mark.parametrize("profile", ["daily", "monthly"])
def test_dry_run_lists_every_job_with_its_skip_reason_and_writes_nothing(w, capsys, profile):
    _dry_world(w)
    before = w.listing()
    assert runner.main(["run", "--profile", profile, "--dry-run", "--json", str(w.tmp / "x.json")]) == 0
    assert w.listing() == before                   # no state file change, summary, log, LOOSE_ENDS line
    out = capsys.readouterr().out
    for want in ("research_a", "teaching_b", "research_c", "auto_stage off", "would seed and stage",
                 "would sweep: lit_pull_queue.csv", "skip: nothing staged", "walk_cadence_days unset",
                 "DB writes off", "canaries (", "planned request"):
        assert want in out, want
    if profile == "daily":
        assert "profile not escalated" in out
    else:
        assert "S2 key absent" in out
    assert w.runs() == [] and w.summaries() == [] and w.calls() == []


def test_dry_run_without_any_state_creates_no_state_file(tmp_path, monkeypatch, capsys):
    w = World(tmp_path, monkeypatch, seed_state=False)
    _dry_world(w)
    before = w.listing()
    assert runner.main(["run", "--profile", "weekly", "--scheduled", "--dry-run"]) == 0
    assert w.listing() == before
    assert not w.state_dir.exists()
    assert "would walk" in capsys.readouterr().out    # never walked: due


def test_dry_run_reads_the_escalation_stamps_when_state_exists(w, monkeypatch, capsys):
    _dry_world(w)
    monkeypatch.setattr(runner, "_now", lambda: 1_800_000_000.0)
    state.kv_set("runner", "profile_done:weekly", 1_800_000_000.0 - DAY)
    state.kv_set("runner", "profile_done:monthly", 1_800_000_000.0 - DAY)
    res = runner.run(profile="daily", scheduled=True, dry_run=True)
    assert res["profile_effective"] == "daily"
    state.kv_set("runner", "profile_done:weekly", 1_800_000_000.0 - 7 * DAY)
    assert runner.run(profile="daily", scheduled=True, dry_run=True)["profile_effective"] == "weekly"


# ================================================================ amendment 5: the registry checks
def test_config_problems_exit_1_before_registering(w, capsys):
    w.register("research_a")
    assert runner.main(["run", "--profile", "daily", "--project", "nope"]) == 1
    w.projects["research_a"]["sources"] = ["unpaywall", "telepathy"]
    w.write()
    assert runner.main(["run", "--profile", "daily"]) == 1
    w.projects["research_a"]["sources"] = ["unpaywall"]
    w.projects["research_a"]["auto_stage"] = "yes"
    w.write()
    assert runner.main(["run", "--profile", "daily"]) == 1
    w.projects["research_a"]["auto_stage"] = False
    w.projects["research_a"]["walk_cadence_days"] = 0
    w.write()
    assert runner.main(["run", "--profile", "daily"]) == 1
    del w.projects["research_a"]["walk_cadence_days"]
    w.top["runner"] = {"unattended_db_writes": "yes"}
    w.write()
    assert runner.main(["run", "--profile", "daily"]) == 1
    w.top["runner"] = {"candidate_order": "random"}
    w.write()
    assert runner.main(["run", "--profile", "daily"]) == 1
    w.top.pop("runner")
    w.projects["research_a"]["active"] = False
    w.write()
    assert runner.main(["run", "--profile", "daily"]) == 1          # no project selected
    assert runner.main(["run", "--profile", "daily", "--project", "research_a"]) == 1   # inactive
    assert w.runs() and all(r["kind"] != "runner" for r in w.runs()) or w.runs() == []


def test_missing_or_unparsable_registry_exits_1(w):
    w.register("research_a")
    w.cfg_path.write_text("{oops", encoding="utf-8")
    assert runner.main(["run", "--profile", "daily"]) == 1
    w.cfg_path.unlink()
    assert runner.main(["run", "--profile", "daily"]) == 1
    assert not [r for r in w.runs() if r["kind"] == "runner"]


def test_usage_errors_exit_1(w):
    assert runner.main(["run"]) == 1
    assert runner.main(["run", "--profile", "hourly"]) == 1
    assert runner.main(["run", "--profile", "daily", "--timeout", "walk"]) == 1
    assert runner.main(["frobnicate"]) == 1


# ================================================================ acceptance 2: one staged queue
DOIS = ["10.5555/w4a.0001", "10.5555/w4a.0002"]


def test_one_staged_queue_one_runs_row_one_summary_one_loose_ends_line(w, capsys):
    w.register("research_a")
    w.register("research_b")
    w.queue("research_a", DOIS)
    w.set_fake("sweep", "research_a", classes={DOIS[1]: "TERMINAL_CLOSED"})
    assert runner.main(["run", "--profile", "daily"]) == 0
    runs = [r for r in w.runs() if r["kind"] == "runner"]
    assert len(runs) == 1 and runs[0]["status"] == "ok" and runs[0]["finished"]
    summ = w.summaries()
    assert len(summ) == 1 and summ[0]["exit_code"] == 0 and summ[0]["run_id"] == runs[0]["run_id"]
    lines = w.loose_lines()
    assert len(lines) == 1 and lines[0].startswith("✅ Lit pull done: research_a/")
    sw = w.calls("sweep")
    assert [c["project"] for c in sw] == ["research_a"] and sw[0]["loose_ends"] is False
    assert sw[0]["run_env"] is None or sw[0]["run_env"] == runs[0]["run_id"]
    route = w.calls("route")
    assert route and route[0]["run_id"] == w.jobs(summ[0], "sweep", "research_a")[0]["counts"]["run_id"]
    assert w.jobs(summ[0], "sweep", "research_b")[0]["status"] == "SKIPPED"
    # the second identical run: the same state, no new line
    w.queue("research_a", DOIS)
    assert runner.main(["run", "--profile", "daily"]) == 0
    assert len(w.loose_lines()) == 1
    assert w.summaries()[-1]["loose_ends"] == {"research_a": "unchanged"}
    assert len([r for r in w.runs() if r["kind"] == "runner"]) == 2
    assert w.ledger_arxiv_lines() == 0


def test_a_changed_state_writes_a_new_loose_ends_line(w):
    w.register("research_a")
    w.queue("research_a", DOIS)
    runner.main(["run", "--profile", "daily"])
    w.queue("research_a", DOIS)
    w.set_fake("sweep", "research_a", retire=False, keep_reason="pmc failed", exit_code=3,
               stages={"pmc": "failed"})
    assert runner.main(["run", "--profile", "daily"]) == 2
    lines = w.loose_lines()
    assert len(lines) == 2 and lines[1].startswith("⏸️ Lit pull PARTIAL: research_a/")


def test_loose_ends_unconfigured_writes_nothing_and_says_so(w):
    w.top.pop("loose_ends")
    w.register("research_a")
    w.queue("research_a", DOIS)
    assert runner.main(["run", "--profile", "daily"]) == 0
    assert "unconfigured" in w.summaries()[-1]["loose_ends"]["research_a"]
    assert state.kv_get("runner", "loose_ends:research_a") is None


def test_the_summary_records_its_fields(w):
    w.register("research_a")
    w.queue("research_a", DOIS)
    runner.main(["run", "--profile", "daily", "--json", str(w.tmp / "copy.json")])
    s = w.summaries()[-1]
    for k in ("profile_requested", "profile_effective", "scheduled", "started", "finished", "exit_code", "jobs",
              "hosts_at_end", "canaries", "candidate_order", "ncbi_offpeak", "chronic_walk_failures",
              "skipped_db_jobs", "needs_ocr"):
        assert k in s, k
    assert json.loads((w.tmp / "copy.json").read_text(encoding="utf-8"))["run_id"] == s["run_id"]
    assert s["hosts_at_end"]["refused"] and {h["host"] for h in s["hosts_at_end"]["refused"]} >= {"export.arxiv.org"}
    log = Path(w.jobs(s, "sweep", "research_a")[0]["log"])
    assert log.is_file() and log.parent.parent.name == s["run_id"]


def test_canary_contexts_follow_the_contract(w):
    w.register("research_a", sources=["unpaywall", "pmc", "arxiv"])
    w.queue("research_a", DOIS)
    # a residual row, so migrate writes a routing CSV and `routing` is expected (W4b verifier N-3)
    w.set_fake("sweep", "research_a", classes={DOIS[1]: "TERMINAL_CLOSED"})
    runner.main(["run", "--profile", "daily"])
    net_call = next(c for c in w.canary_calls if c["phase"] == "network")
    loc = next(c for c in w.canary_calls if c["phase"] == "local")
    assert net_call["context"]["projects"] == [{"key": "research_a", "sources": ["arxiv", "pmc", "unpaywall"]}]
    assert net_call["now"] is not None and loc["now"] is None      # never `now` to the local phase
    p = loc["context"]["projects"][0]
    assert loc["context"]["since"].endswith("+00:00")              # aware
    assert p["sweep_run_ids"] and set(p["stages"]) == {"unpaywall", "residual", "report", "pmc", "processed",
                                                         "routing"}
    assert "normalized" not in p["stages"] and "preprint" not in p["stages"]   # skipped, no report


# ================================================================ the summary is redacted
def test_everything_persisted_is_redacted(w, monkeypatch):
    w.register("research_a")
    w.queue("research_a", DOIS)
    w.set_fake("sweep", "research_a", refused=["x@university.edu"], exit_code=4)
    runner.main(["run", "--profile", "daily"])
    text = "".join(p.read_text(encoding="utf-8") for p in (w.state_dir / "runner").rglob("*.json"))
    text += "".join(p.read_text(encoding="utf-8") for p in (w.state_dir / "runner").rglob("*.log"))
    assert "x@university.edu" not in text


def test_kv_values_are_redacted_through_state(w):
    state.kv_set("runner", "t", {"a": ["mailto:x@university.edu"]})
    assert "university.edu" not in json.dumps(state.kv_get("runner", "t"))


# ================================================================ acceptance 2 with the real sweep and migrate
def test_end_to_end_with_the_real_sweep_and_the_real_migrate(w, monkeypatch):
    """The real sweep.run and migrate_closed_to_md.run through the shim (in-process). Only sweep's
    stage subprocesses are replaced (sweep._run_stage, as tests/test_batch1_fixes.py does): no fetch
    stage runs and nothing is sent."""
    import types
    import sweep
    monkeypatch.setitem(runner.STAGE_MODULES, "sweep", "sweep")
    monkeypatch.setitem(runner.STAGE_MODULES, "route", "migrate_closed_to_md")
    w.register("research_a")
    w.queue("research_a", DOIS)
    seen = []

    def stage(cmd):
        cmd = [str(c) for c in cmd]
        script = Path(cmd[1]).name
        seen.append(script)

        def arg(flag):
            return cmd[cmd.index(flag) + 1]
        if script == "unpaywall_fetch_v2.py":
            Path(arg("--report")).write_text(
                "doi,downloaded,oa_status,error\n"
                f"{DOIS[0]},True,OA,\n{DOIS[1]},False,CLOSED,CLOSED\n", encoding="utf-8")
        elif script == "pmc_fetch.py":
            Path(arg("--report-out")).write_text(f"doi,downloaded,error,pmcid\n{DOIS[1]},False,NO_PMCID,\n",
                                                 encoding="utf-8")
        return types.SimpleNamespace(returncode=0, stdout="", stderr="")
    monkeypatch.setattr(sweep, "_run_stage", stage)
    assert runner.main(["run", "--profile", "daily"]) == 0
    assert seen == ["unpaywall_fetch_v2.py", "pmc_fetch.py", "extract_pdf_fulltext.py"]
    s = w.summaries()[-1]
    sw = [j for j in s["jobs"] if j["job"] == "sweep"][0]
    assert sw["status"] == "OK" and sw["counts"]["classes"] == {"fetched": 1, "TERMINAL_CLOSED": 1}
    run_id = sw["counts"]["run_id"]
    root = w.proot("research_a")
    assert (root / f"lit_pull_queue.{run_id}.processed.csv").is_file()
    assert (root / f"lit_pull_queue.{run_id}.routing.csv").is_file()
    assert DOIS[1] in (root / "lit_pull_queue.md").read_text(encoding="utf-8")          # the ILL list
    lines = w.loose_lines()
    assert len(lines) == 1 and lines[0].startswith("✅ Lit pull done: research_a/ — 1/2 fetched")
    loc = next(c for c in w.canary_calls if c["phase"] == "local")["context"]["projects"][0]
    assert set(loc["stages"]) == {"unpaywall", "pmc", "residual", "report", "processed", "routing"}
    assert len([r for r in w.runs() if r["kind"] == "runner"]) == 1
    assert w.ledger_arxiv_lines() == 0
