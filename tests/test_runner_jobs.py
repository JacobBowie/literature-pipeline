"""python -m litpipe.runner `run`, job by job (the W4b amendments 3-10, 13): auto-stage, walks and
their deferral, the heartbeat, DB writes, escalation, the keyed S2 call, the candidate-order A/B,
Crossref's 10 % rule, the NCBI hint, needs_ocr counts, the index fingerprint, retention, status,
schedule-print and the one-runner rule. In-process, on a temp world (tests/fixtures/W4-A/w4a_world)."""
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import pytest

FIX = Path(__file__).resolve().parent / "fixtures" / "W4-A"
sys.path.insert(0, str(FIX))

from litpipe import canaries, runner, state, walk  # noqa: E402
from w4a_world import World  # noqa: E402

DAY = 86400.0
DOIS = ["10.5555/w4a.0001", "10.5555/w4a.0002"]
S2 = "api.semanticscholar.org"


@pytest.fixture
def w(tmp_path, monkeypatch):
    return World(tmp_path, monkeypatch)


def summary(w):
    return w.summaries()[-1]


def job(w, name, project=None, s=None):
    s = s or summary(w)
    return [j for j in s["jobs"] if j["job"] == name and j["project"] == project]


def alarm(check_id, host, observed="x"):
    return canaries._mk(canaries.CHECKS_BY_ID[check_id], "ALARM", host=host, observed=observed)


# ================================================================ amendment 5 (a): auto_stage
def test_auto_stage_false_is_never_staged(w):
    w.register("teaching_a")
    assert runner.main(["run", "--profile", "daily"]) == 0
    assert w.calls("seed") == [] and not (w.proot("teaching_a") / "lit_pull_queue.csv").exists()
    assert job(w, "sweep", "teaching_a")[0]["reason"] == "nothing staged"
    w.queue("teaching_a", DOIS, tag="teach")                      # what is staged is still swept
    assert runner.main(["run", "--profile", "daily"]) == 0
    assert w.calls("seed") == [] and [c["project"] for c in w.calls("sweep")] == ["teaching_a"]


def test_auto_stage_true_stages_the_draft_at_the_subproject_root(w):
    w.register("Teach", "literature")
    w.register("Teach/teaching_small", "teaching_small/literature", parent="Teach", auto_stage=True)
    w.set_fake("sweep", "Teach/teaching_small", retire=False, keep_reason="held for the test")
    proot = w.proot("Teach/teaching_small")
    assert proot == w.root / "Teach" / "teaching_small"
    w.set_fake("seed", "Teach/teaching_small", destination="literature/")
    runner.main(["run", "--profile", "daily", "--project", "Teach/teaching_small"])
    queue = proot / "lit_pull_queue.csv"
    text = queue.read_text(encoding="utf-8")
    assert not text.startswith("#") and text.count("10.5555/w4a.seed") == 2      # comment lines stripped
    assert not (proot / "lit_pull_queue.draft.csv").exists()
    assert job(w, "seed", "Teach/teaching_small")[0]["counts"]["staged"] == 2
    assert [c["project"] for c in w.calls("sweep")] == ["Teach/teaching_small"]


def test_auto_stage_backs_up_a_non_empty_queue_and_never_overwrites_a_backup(w):
    root = w.register("teaching_a", auto_stage=True)
    w.set_fake("sweep", "teaching_a", retire=False)
    (root / "lit_pull_queue.csv").write_text("# chase via ILL\n# see the debrief\n", encoding="utf-8")
    runner.main(["run", "--profile", "daily"])
    assert (root / "lit_pull_queue.bak.csv").read_text(encoding="utf-8").startswith("# chase via ILL")
    (root / "lit_pull_queue.csv").write_text("doi,title\n", encoding="utf-8")       # header only: READY
    runner.main(["run", "--profile", "daily"])
    assert (root / "lit_pull_queue.bak.csv").read_text(encoding="utf-8").startswith("# chase via ILL")
    stamped = list(root.glob("lit_pull_queue.bak.2*.csv"))
    assert len(stamped) == 1 and stamped[0].read_text(encoding="utf-8") == "doi,title\n"


def test_auto_stage_never_seeds_over_a_staged_queue_with_rows(w):
    root = w.register("teaching_a", auto_stage=True)
    w.queue("teaching_a", DOIS)
    runner.main(["run", "--profile", "daily"])
    assert w.calls("seed") == [] and "staged with rows" in job(w, "seed", "teaching_a")[0]["reason"]
    assert [c["project"] for c in w.calls("sweep")] == ["teaching_a"]
    assert not (root / "lit_pull_queue.bak.csv").exists()


def test_a_seeder_exit_1_fails_the_project_and_nothing_is_swept(w):
    w.register("teaching_a", auto_stage=True)
    w.register("teaching_b")
    w.queue("teaching_b", DOIS)
    w.set_fake("seed", "teaching_a", exit_code=1)
    assert runner.main(["run", "--profile", "daily"]) == 2
    assert job(w, "seed", "teaching_a")[0]["status"] == "FAILED"
    assert job(w, "sweep", "teaching_a")[0]["reason"].startswith("the seed step failed")
    assert not (w.proot("teaching_a") / "lit_pull_queue.csv").exists()
    assert [c["project"] for c in w.calls("sweep")] == ["teaching_b"]         # the next project still runs


def test_an_empty_draft_is_removed_and_nothing_is_staged(w):
    root = w.register("teaching_a", auto_stage=True)
    w.set_fake("seed", "teaching_a", dois=[])
    runner.main(["run", "--profile", "daily"])
    assert not (root / "lit_pull_queue.draft.csv").exists() and not (root / "lit_pull_queue.csv").exists()
    assert w.calls("sweep") == []


# ================================================================ sweep exits and the abort
def test_a_config_abort_skips_every_later_projects_sweep_but_not_their_walks(w, monkeypatch):
    for k in ("research_a", "research_b", "research_c"):
        w.register(k, walk_cadence_days=1)
        w.queue(k, DOIS)
    w.set_fake("sweep", "research_b", exit_code=2, retire=False)
    assert runner.main(["run", "--profile", "daily"]) == 2
    s = summary(w)
    assert [c["project"] for c in w.calls("sweep")] == ["research_a", "research_b"]
    assert job(w, "sweep", "research_b", s)[0]["status"] == "FAILED"
    assert job(w, "sweep", "research_c", s)[0]["reason"] == "CONFIG abort in research_b"
    assert job(w, "route", "research_b", s)[0]["reason"].startswith("CONFIG aborts the run")
    assert [c["project"] for c in w.calls("walk")] == ["research_a", "research_b", "research_c"]


def test_a_failed_stage_never_ends_the_run(w):
    w.register("research_a", walk_cadence_days=1)
    w.register("research_b", walk_cadence_days=1)
    w.set_fake("walk", "research_a", mode="crash")
    assert runner.main(["run", "--profile", "daily"]) == 2
    s = summary(w)
    a = job(w, "walk", "research_a", s)[0]
    assert a["status"] == "ERROR" and "shim exit 1" in a["reason"]
    assert job(w, "walk", "research_b", s)[0]["status"] == "OK"
    assert "RuntimeError: stand-in crash" in Path(a["log"]).read_text(encoding="utf-8")


def test_ok_with_a_traceback_in_the_log_is_degraded(w):
    w.register("research_a", walk_cadence_days=1)
    w.set_fake("walk", "research_a", traceback=True)
    assert runner.main(["run", "--profile", "daily"]) == 2
    assert job(w, "walk", "research_a")[0]["reason"] == "traceback in the stage log"


def test_exit_2_prints_the_step_summary_last(w, capsys):
    w.register("research_a", walk_cadence_days=1)
    w.set_fake("walk", "research_a", exit_code=2, reasons=["3 of 4 seed walks failed (over 5 %)"],
               transport_failures=2)
    assert runner.main(["run", "--profile", "daily"]) == 2
    last = capsys.readouterr().out.strip().splitlines()[-1]
    assert last.startswith("[step-summary] ")
    step = json.loads(last[len("[step-summary] "):])
    assert step["aborted"] is None and step["transport_failures"] == 2
    assert any("research_a walk DEGRADED" in r for r in step["reasons"])


# ================================================================ amendment 8: walks
def test_walk_cadence_and_its_stamp(w, monkeypatch):
    w.register("research_a", walk_cadence_days=7)
    w.register("research_b")                                      # no cadence: never unattended
    t = [1_800_000_000.0]
    monkeypatch.setattr(runner, "_now", lambda: t[0])
    runner.main(["run", "--profile", "daily"])
    assert [c["project"] for c in w.calls("walk")] == ["research_a"]
    assert state.kv_get("runner", "walk:research_a") == t[0]
    assert "never walked unattended" in job(w, "walk", "research_b")[0]["reason"]
    t[0] += 6 * DAY
    runner.main(["run", "--profile", "daily"])
    assert len(w.calls("walk")) == 1 and "walk not due" in job(w, "walk", "research_a")[0]["reason"]
    t[0] += DAY
    runner.main(["run", "--profile", "daily"])
    assert len(w.calls("walk")) == 2
    runner.main(["run", "--profile", "every_run"])
    assert "walks run daily and up" in job(w, "walk", "research_a")[0]["reason"]


@pytest.mark.parametrize("code,stamped", [(0, True), (2, True), (3, False), (1, False)])
def test_the_walk_stamp_only_on_exit_0_or_2(w, code, stamped):
    w.register("research_a", walk_cadence_days=7)
    w.set_fake("walk", "research_a", exit_code=code, aborted="budget" if code == 3 else None)
    runner.main(["run", "--profile", "daily"])
    assert (state.kv_get("runner", "walk:research_a") is not None) is stamped


def test_walk_gets_the_project_and_the_cache_path(w):
    w.register("research_a", walk_cadence_days=1)
    runner.main(["run", "--profile", "daily"])
    c = w.calls("walk")[0]
    assert c["project"] == "research_a" and Path(c["cache_path"]) == walk.cache_path()


def test_an_s2_refusal_by_a_stage_defers_every_later_walk_and_ends_with_the_run(w):
    for k in ("research_a", "research_b", "research_c"):
        w.register(k, walk_cadence_days=1)
    w.set_fake("walk", "research_a", refuse=[S2], exit_code=3, aborted="S2 refused")
    assert runner.main(["run", "--profile", "daily"]) == 2
    s = summary(w)
    assert [c["project"] for c in w.calls("walk")] == ["research_a"]
    for k in ("research_b", "research_c"):
        j = job(w, "walk", k, s)[0]
        assert j["status"] == "DEFERRED" and S2 in j["reason"]
    assert any(h["host"] == S2 for h in s["hosts_at_end"]["refused"])     # held for the whole run
    assert not state.is_refused(S2)                                        # ended at finish_run


def test_an_s2_deferral_defers_the_walk(w):
    w.register("research_a", walk_cadence_days=1)
    state.defer(S2, time.time() + 3600, "429 Retry-After")
    assert runner.main(["run", "--profile", "daily"]) == 2
    assert job(w, "walk", "research_a")[0]["status"] == "DEFERRED" and w.calls("walk") == []


def _seed_failed(*dois):
    c = walk.Cache(walk.cache_path())
    try:
        for d in dois:
            c.record(d, "s2", state="failed", kind="TRANSPORT", reason="earlier failure")
    finally:
        c.close()


FIVE = ["1 of 10 seed walks failed (over 5 %)"]


def test_chronic_failures_only_are_degraded_but_do_not_make_exit_2(w):
    w.register("research_a", walk_cadence_days=1)
    _seed_failed("10.5555/chronic.0001")
    w.set_fake("walk", "research_a", exit_code=2, reasons=FIVE, fail_seeds=["10.5555/chronic.0001"])
    assert runner.main(["run", "--profile", "daily"]) == 0
    j = job(w, "walk", "research_a")[0]
    assert j["status"] == "DEGRADED" and j["reason"].startswith("chronic only") and not j["counts_for_exit"]
    assert summary(w)["chronic_walk_failures"]["research_a"]["chronic"] == 1


def test_a_new_failure_beside_a_chronic_one_counts(w):
    w.register("research_a", walk_cadence_days=1)
    _seed_failed("10.5555/chronic.0001")
    w.set_fake("walk", "research_a", exit_code=2, reasons=FIVE,
               fail_seeds=["10.5555/chronic.0001", "10.5555/new.0002"])
    assert runner.main(["run", "--profile", "daily"]) == 2
    info = summary(w)["chronic_walk_failures"]["research_a"]
    assert (info["chronic"], info["new"]) == (1, 1) and info["new_seeds"] == ["10.5555/new.0002"]


def test_a_degraded_walk_for_another_reason_is_not_chronic_only(w):
    w.register("research_a", walk_cadence_days=1)
    _seed_failed("10.5555/chronic.0001")
    w.set_fake("walk", "research_a", exit_code=2, fail_seeds=["10.5555/chronic.0001"],
               reasons=["5 seeds with citers, fewer than the published 9"])
    assert runner.main(["run", "--profile", "daily"]) == 2


def test_a_locked_cache_is_chronic_unknown_and_never_waited_on(w, monkeypatch):
    w.register("research_a", walk_cadence_days=1)

    class Locked:
        def __init__(self, *a, **k):
            raise walk.CacheLocked("held by another process")
    monkeypatch.setattr(walk, "Cache", Locked)
    w.set_fake("walk", "research_a", exit_code=2, reasons=FIVE)
    assert runner.main(["run", "--profile", "daily"]) == 2
    assert summary(w)["chronic_walk_failures"]["research_a"]["chronic"] == "chronic: unknown (cache locked)"


def test_weekly_backward_top_up_for_cadence_projects_only(w, monkeypatch):
    w.register("research_a", walk_cadence_days=7)
    w.register("research_b")
    runner.main(["run", "--profile", "daily"])
    assert w.calls("reverse") == []
    runner.main(["run", "--profile", "weekly"])
    calls = w.calls("reverse")
    assert [c["project"] for c in calls] == ["research_a"] and calls[0]["sources"] == "openalex,crossref,regex"
    monkeypatch.setenv("S2_API_KEY", "test-key-0123456789")
    runner.main(["run", "--profile", "weekly"])
    assert w.calls("reverse")[-1]["sources"] is None                  # keyed: every leg


# ================================================================ amendment 4: the heartbeat
def test_the_heartbeat_keeps_the_run_live_and_its_refusals_hold(w, monkeypatch):
    monkeypatch.setattr(state, "HEARTBEAT_STALE_S", 1.5)
    monkeypatch.setattr(runner, "HEARTBEAT_EVERY_S", 0.2)
    monkeypatch.setattr(runner, "HEARTBEAT_SLICE_S", 0.05)
    w.register("research_a", walk_cadence_days=1)
    w.register("research_b", walk_cadence_days=1)
    obs = w.tmp / "observe.jsonl"
    w.set_fake("walk", "research_a", refuse=[S2], sleep=4.0, observe=str(obs), exit_code=3, aborted="S2")
    assert runner.main(["run", "--profile", "daily"]) == 2
    seen = [json.loads(x) for x in obs.read_text(encoding="utf-8").splitlines()]
    assert len(seen) >= 10 and seen[-1]["t"] - seen[0]["t"] > 3.0          # outlived HEARTBEAT_STALE_S twice
    assert all(o["live"] and o["s2_refused"] for o in seen)
    assert job(w, "walk", "research_b")[0]["status"] == "DEFERRED"
    assert summary(w)["heartbeat"]["beats"] >= 5


def test_the_main_thread_beats_before_each_stage_and_a_lost_run_launches_nothing(w, monkeypatch):
    w.register("research_a", walk_cadence_days=1)
    w.register("research_b", walk_cadence_days=1)
    real = state.heartbeat
    calls = []

    def beat(run_id=None):
        calls.append(run_id)
        if len(calls) >= 2:                      # the second beat: another process abandoned the run
            return False
        return real(run_id)
    monkeypatch.setattr(state, "heartbeat", beat)
    monkeypatch.setattr(runner, "HEARTBEAT_EVERY_S", 3600.0)          # only the main thread beats
    assert runner.main(["run", "--profile", "daily"]) == 3
    s = summary(w)
    assert [c["project"] for c in w.calls("walk")] == ["research_a"]
    assert job(w, "walk", "research_b", s)[0]["status"] == "ABORTED"
    assert s["aborted"].startswith("heartbeat lost") and s["exit_code"] == 3
    assert [r for r in w.runs() if r["kind"] == "runner"][-1]["status"].startswith("aborted: heartbeat lost")


def test_heartbeat_unit_beats_on_a_wall_clock_jump_and_records_it(monkeypatch):
    beats = []
    hb = runner.Heartbeat("r", beat=lambda rid: beats.append(time.time()) or True)
    monkeypatch.setattr(runner, "HEARTBEAT_EVERY_S", 5.0)
    monkeypatch.setattr(runner, "HEARTBEAT_SLICE_S", 0.05)
    monkeypatch.setattr(runner, "JUMP_TOLERANCE_S", 30.0)
    real = time.time
    offset = [0.0]
    monkeypatch.setattr(runner.time, "time", lambda: real() + offset[0])
    hb.start()
    try:
        time.sleep(0.3)
        assert beats == []                                     # nothing due yet
        offset[0] = 3600.0                                     # the lid opens an hour later
        deadline = real() + 3
        while not beats and real() < deadline:
            time.sleep(0.02)
    finally:
        hb.stop()
    assert len(beats) == 1 and hb.jumps == 1 and hb.jumped() >= 3500


def test_heartbeat_unit_errors_are_logged_and_retried(monkeypatch, capsys):
    n = []

    def flaky(rid):
        n.append(1)
        if len(n) < 3:
            raise OSError("database is locked")
        return True
    monkeypatch.setattr(runner, "HEARTBEAT_EVERY_S", 0.05)
    monkeypatch.setattr(runner, "HEARTBEAT_SLICE_S", 0.02)
    hb = runner.Heartbeat("r", beat=flaky).start()
    try:
        deadline = time.time() + 3
        while hb.beats == 0 and time.time() < deadline:
            time.sleep(0.02)
    finally:
        hb.stop()
    assert hb.errors == 2 and hb.beats >= 1 and not hb.lost.is_set()
    assert "heartbeat failed" in capsys.readouterr().err


# ================================================================ amendment 6: DB writes
def test_db_jobs_are_skipped_by_default_with_their_commands(w):
    w.register("research_a")
    w.local_outs = [alarm("index_freshness", "research_a", "never indexed")]
    assert runner.main(["run", "--profile", "weekly"]) == 0      # the stale index alone: not exit 2
    s = summary(w)
    assert w.calls("index") == [] and w.calls("abstracts") == []
    cmds = {j["job"]: j["command"] for j in s["skipped_db_jobs"]}
    assert "index_portfolio.py --project research_a --db" in cmds["index"] and "enrich_abstracts.py" in cmds["abstracts"]
    assert s["health"]["status"] == "PASS"
    assert s["health"]["not_counted"][0]["note"] == "index stale: DB writes off"
    assert s["canaries"]["checks"][0]["status"] == "ALARM"          # the canary report itself unchanged


def test_any_other_alarm_makes_exit_2(w):
    w.register("research_a")
    w.local_outs = [alarm("index_freshness", "research_a"), alarm("markup", "research_a", "1 file")]
    assert runner.main(["run", "--profile", "daily"]) == 2
    assert [c["id"] for c in summary(w)["health"]["counted"]] == ["markup"]


def test_with_db_writes_the_jobs_run_and_the_stale_index_counts(w):
    w.register("research_a")
    (w.lib("research_a") / "2020_A.pdf").write_bytes(b"%PDF")
    w.local_outs = [alarm("index_freshness", "research_a")]
    assert runner.main(["run", "--profile", "weekly", "--db-writes"]) == 2
    assert [c["project"] for c in w.calls("index")] == ["research_a"] and len(w.calls("abstracts")) == 1
    assert Path(w.calls("index")[0]["db"]) == w.db_dir / "portfolio.duckdb"
    assert summary(w)["health"]["counted"][0]["id"] == "index_freshness"
    assert state.kv_get("runner", "index_fp:research_a")
    w.local_outs = []
    runner.main(["run", "--profile", "daily", "--db-writes"])           # unchanged: not indexed again
    assert len(w.calls("index")) == 1
    assert job(w, "index", "research_a")[0]["reason"] == "library unchanged since its last index"
    (w.lib("research_a") / "2021_B.pdf").write_bytes(b"%PDF")          # a change made outside the runner
    runner.main(["run", "--profile", "daily", "--db-writes"])
    assert len(w.calls("index")) == 2


def test_the_registry_block_turns_db_writes_on_and_the_flag_forces_them_off(w):
    w.register("research_a")
    w.top["runner"] = {"unattended_db_writes": True}
    w.write()
    runner.main(["run", "--profile", "daily"])
    assert len(w.calls("index")) == 1 and summary(w)["db_writes_source"] == "projects.json"
    (w.lib("research_a") / "x.pdf").write_bytes(b"%PDF")
    runner.main(["run", "--profile", "daily", "--no-db-writes"])
    assert len(w.calls("index")) == 1 and summary(w)["db_writes"] is False


def test_index_stamp_only_after_exit_0(w):
    w.register("research_a")
    w.set_fake("index", "research_a", exit_code=2, reasons=["skipped research_a: library unreadable"])
    assert runner.main(["run", "--profile", "daily", "--db-writes"]) == 2
    assert state.kv_get("runner", "index_fp:research_a") is None


# ================================================================ amendment 5: escalation, A10
def _nights(w, monkeypatch, days, key=None, paper_batch=None):
    from litpipe import s2
    sent = []
    if key:
        monkeypatch.setenv("S2_API_KEY", key)
    monkeypatch.setattr(s2, "Session", lambda **kw: "session")
    monkeypatch.setattr(s2, "paper_batch", lambda ids, fields=None, session=None: (
        sent.append((list(ids), fields)), paper_batch)[1])
    clock = {"t": 1_800_000_000.0}

    def now():
        clock["t"] += 1.0
        return clock["t"]
    monkeypatch.setattr(runner, "_now", now)
    effective = {}
    for d in range(days):
        clock["t"] = 1_800_000_000.0 + d * DAY + 3600
        runner.main(["run", "--profile", "daily", "--scheduled"])
        effective[d] = summary(w)["profile_effective"]
    return effective, sent


def test_scheduled_runs_escalate_weekly_on_day_7_and_monthly_on_day_28(w, monkeypatch):
    from litpipe.outcomes import Kind, Outcome
    w.register("research_a")
    eff, sent = _nights(w, monkeypatch, 60, key="test-key-0123456789", paper_batch=Outcome(Kind.OK, attempts=1))
    assert [d for d, p in eff.items() if p == "monthly"] == [0, 28, 56]
    assert [d for d, p in eff.items() if p in ("weekly", "monthly")] == [0, 7, 14, 21, 28, 35, 42, 49, 56]
    assert len(sent) == 3 and sent[0] == (["DOI:10.1371/journal.pone.0012033"], "paperId")   # once per 27 days


def test_no_s2_key_the_keepalive_is_skipped_and_sends_nothing(w, monkeypatch):
    w.register("research_a")
    eff, sent = _nights(w, monkeypatch, 1)
    assert eff[0] == "monthly" and sent == []
    j = job(w, "s2_keepalive")[0]
    assert j["status"] == "SKIPPED" and "no S2 key" in j["reason"]
    assert job(w, "recommendations")[0]["reason"].startswith("no S2 key")


def test_manual_runs_do_not_escalate_and_stamps_skip_refusals_aborts_and_dry_runs(w, monkeypatch):
    w.register("research_a")
    runner.main(["run", "--profile", "daily"])
    assert summary(w)["profile_effective"] == "daily"
    assert state.kv_get("runner", "profile_done:weekly") is None
    runner.main(["run", "--profile", "daily", "--scheduled", "--dry-run"])
    assert state.kv_get("runner", "profile_done:weekly") is None
    monkeypatch.setattr("litpipe.preflight.run", lambda **kw: [__import__("litpipe.outcomes", fromlist=["x"]).Outcome(
        __import__("litpipe.outcomes", fromlist=["x"]).Kind.CONFIG, host="env", detail="LITPIPE_EMAIL is unset",
        payload={"check": "email"})])
    assert runner.main(["run", "--profile", "daily", "--scheduled"]) == 3
    assert state.kv_get("runner", "profile_done:weekly") is None
    assert [r for r in w.runs() if r["kind"] == "runner"][-1]["status"] == "preflight"
    assert w.calls() == []                                            # nothing else ran


def test_recommendations_run_monthly_with_the_key_and_db_writes(w, monkeypatch):
    from litpipe import s2
    from litpipe.outcomes import Kind, Outcome
    w.register("research_a")
    monkeypatch.setenv("S2_API_KEY", "test-key-0123456789")
    monkeypatch.setattr(s2, "Session", lambda **kw: "session")
    monkeypatch.setattr(s2, "paper_batch", lambda *a, **k: Outcome(Kind.OK, attempts=1))
    runner.main(["run", "--profile", "monthly", "--db-writes"])
    assert w.calls("recommendations")[0]["recent_feed"] is True
    assert json.loads((Path(w.calls("audit")[0]["json_path"])).read_text(encoding="utf-8"))["exit_code"] == 0
    assert w.calls("audit")[0]["holdings"] is False


# ================================================================ amendment 9: the A/B
def test_candidate_order_ab_over_the_first_two_scheduled_runs_whose_unpaywall_ran(w, monkeypatch):
    w.register("research_a")
    orders = []

    def night(stage=True, scheduled=True):
        if stage:
            w.queue("research_a", DOIS)
        argv = ["run", "--profile", "daily"] + (["--scheduled"] if scheduled else [])
        runner.main(argv)
        s = summary(w)
        orders.append((s["candidate_order"]["order"], s["candidate_order"]["source"]))
        return s
    night(stage=False)                                   # nothing staged: Unpaywall did not run, not consumed
    night(scheduled=False)                               # manual: never consumes the A/B
    s = night()
    assert s["candidate_order"]["per_project"]["research_a"] == {
        "order": "repository", "rows": 2, "unpaywall_downloads": 2, "downloaded": 2}
    night()
    night()
    assert orders == [("repository", "a/b"), (None, None), ("repository", "a/b"), ("publisher", "a/b"),
                      ("repository", "default")]
    assert [c["candidate_order"] for c in w.calls("sweep")] == [None, "repository", "publisher", "repository"]
    w.top["runner"] = {"candidate_order": "publisher"}
    w.write()
    night()
    night(scheduled=False)
    assert orders[-2:] == [("publisher", "projects.json"), ("publisher", "projects.json")]


# ================================================================ amendment 8: Crossref's 10 % rule
def _lines(n_ok, n_err, status_err=503, **extra):
    base = {"host": "api.crossref.org", "attempt": 1, "hop": 0, "purpose": "metadata", "decision": "final"}
    return ([{**base, "status": 404, "kind": "NO_MATCH", **extra} for _ in range(n_ok)]
            + [{**base, "status": status_err, "kind": "OUTAGE" if status_err >= 500 else "REFUSED", **extra}
               for _ in range(n_err)])


@pytest.mark.parametrize("n_ok,n_err,extra,refused", [
    (20, 0, {}, False),                    # 20 404s: answers, not errors
    (18, 2, {}, True),                     # 10 %
    (19, 1, {}, False),                    # 5 %
    (17, 2, {}, False),                    # 19 lines: below the minimum
    (0, 20, {"purpose": "canary:crossref_refs"}, False),
    (0, 20, {"attempt": 2}, False),
    (0, 20, {"decision": "not_sent"}, False),
    (0, 20, {"run_id": "another-run"}, False),
])
def test_crossref_ten_percent_rule(w, n_ok, n_err, extra, refused):
    w.register("research_a")
    w.queue("research_a", DOIS)
    w.set_fake("sweep", "research_a", ledger=_lines(n_ok, n_err, **extra))
    runner.main(["run", "--profile", "daily"])
    s = summary(w)
    assert s["crossref_rule"]["refused"] is refused
    assert any(h["host"] == "api.crossref.org" for h in s["hosts_at_end"]["refused"]) is refused
    assert not state.is_refused("api.crossref.org")                   # a run refusal ends with the run


def test_crossref_rule_counts_429_and_transport(w):
    w.register("research_a")
    w.queue("research_a", DOIS)
    lines = _lines(16, 2, status_err=429) + [{**_lines(0, 1)[0], "status": None, "kind": "TRANSPORT"}] * 2
    w.set_fake("sweep", "research_a", ledger=lines)
    runner.main(["run", "--profile", "daily"])
    assert summary(w)["crossref_rule"]["errors"] == 4 and summary(w)["crossref_rule"]["refused"] is True


# ================================================================ amendments 7, 10: hints and counts
def _ts(y, mo, d, h, mi=0):
    from zoneinfo import ZoneInfo
    return datetime(y, mo, d, h, mi, tzinfo=ZoneInfo("America/New_York")).timestamp()


@pytest.mark.parametrize("when,off", [
    ((2026, 10, 7, 12), False), ((2026, 10, 7, 21), True), ((2026, 10, 8, 4, 59), True), ((2026, 10, 8, 5), False),
    ((2026, 10, 9, 20, 59), False), ((2026, 10, 10, 12), True), ((2026, 10, 11, 23), True), ((2026, 10, 12, 3), True),
])
def test_ncbi_offpeak_is_a_hint(when, off):
    assert runner.ncbi_offpeak(_ts(*when))["offpeak"] is off


def test_ncbi_hint_never_blocks(w, monkeypatch):
    w.register("research_a")
    monkeypatch.setattr(runner, "_now", lambda: _ts(2026, 10, 7, 12))
    assert runner.main(["run", "--profile", "daily"]) == 0
    assert summary(w)["ncbi_offpeak"]["offpeak"] is False


def test_needs_ocr_counts_new_sidecars_and_keeps_unreadable_apart(w, tmp_path):
    w.register("research_a")
    lib = w.lib("research_a")
    old = lib / "2010_Old.fulltext.json"
    old.write_text(json.dumps({"needs_ocr": True, "needs_ocr_reason": "no text layer"}), encoding="utf-8")
    os.utime(old, (time.time() - 10 * DAY, time.time() - 10 * DAY))
    since = time.time() - 1
    (lib / "2020_A.fulltext.json").write_text(json.dumps({"needs_ocr": True, "needs_ocr_reason": "text gate failed"}),
                                              encoding="utf-8")
    (lib / "2020_B.fulltext.json").write_text(json.dumps({"needs_ocr": True,
                                                          "needs_ocr_reason": "pdf unreadable: not a PDF"}),
                                              encoding="utf-8")
    (lib / "2020_C.fulltext.json").write_text(json.dumps({"text": "fine"}), encoding="utf-8")
    c = runner.needs_ocr_counts(lib, since)
    assert (c["needs_ocr"], c["pdf_unreadable"]) == (1, 1)
    assert c["command"].endswith(f"extract_pdf_fulltext.py --lib-dir {lib} --ocr") or "--ocr" in c["command"]
    only_bad = tmp_path / "bad"
    only_bad.mkdir()
    (only_bad / "x.fulltext.json").write_text(json.dumps({"needs_ocr": True, "needs_ocr_reason": "pdf unreadable: html"}),
                                              encoding="utf-8")
    c2 = runner.needs_ocr_counts(only_bad, since)
    assert c2["pdf_unreadable"] == 1 and "command" not in c2                 # never sent to OCR


def test_index_fingerprint_changes_with_the_library_and_the_walker_csvs(w):
    w.register("research_a")
    lib = w.lib("research_a")
    fp = lambda: runner.index_fingerprint(lib)  # noqa: E731
    a = fp()
    assert a == fp()
    (lib / "2020_A.pdf").write_bytes(b"%PDF")
    b = fp()
    assert b != a
    (lib / "2020_A.ris").write_text("TY  - JOUR\nDO  - 10.5555/a.1\nER  - \n", encoding="utf-8")
    c = fp()
    assert c != b
    (lib / "_forward_citations.csv").write_text("seed_pdf\n", encoding="utf-8")
    d = fp()
    assert d != c
    (lib / "_resp_forward_citations.csv").write_text("seed_pdf\n", encoding="utf-8")
    e = fp()
    assert e != d
    (lib / "2016_T.fulltext.json").write_text(json.dumps({"doi": "10.5555/t.1", "text": "jats text"}),
                                              encoding="utf-8")
    assert fp() != e
    (lib / "_reverse_citations_unique.csv").write_text("doi\n", encoding="utf-8")   # a side output
    f = fp()
    (lib / "_reverse_citations_unique.csv").write_text("doi\n10.1/x\n", encoding="utf-8")
    assert fp() == f
    assert runner.index_fingerprint(w.tmp / "missing") is None


def test_the_newest_90_run_directories_are_kept(w):
    w.register("research_a")
    base = w.state_dir / "runner"
    for i in range(95):
        d = base / f"20200101T{i // 60:02d}{i % 60:02d}00Z-runner-1-abc{i:03d}"
        d.mkdir(parents=True)
        (d / "summary.json").write_text("{}", encoding="utf-8")
    (base / "keep-me").mkdir()
    runner.main(["run", "--profile", "daily"])
    dirs = sorted(d.name for d in base.iterdir() if runner.RUN_DIR_RE.match(d.name))
    assert len(dirs) == 90 and dirs[-1] == summary(w)["run_id"]
    assert (base / "keep-me").is_dir()
    assert not (base / "20200101T000000Z-runner-1-abc000").exists()


# ================================================================ amendment 13: status, schedule-print
def test_status_without_state_creates_nothing(tmp_path, monkeypatch, capsys):
    w = World(tmp_path, monkeypatch, seed_state=False)
    assert runner.main(["status"]) == 0
    assert "no state yet" in capsys.readouterr().out and not w.state_dir.exists()


def test_status_prints_runs_hosts_and_counts_and_registers_nothing(w, capsys):
    w.register("research_a")
    w.queue("research_a", DOIS)
    runner.main(["run", "--profile", "daily"])
    n = len(w.runs())
    capsys.readouterr()
    assert runner.main(["status"]) == 0
    out = capsys.readouterr().out
    assert "live runs: 0" in out and "research_a" in out and "export.arxiv.org" in out
    assert "last run per project: 1" in out and "sweep OK" in out
    assert len(w.runs()) == n


@pytest.fixture
def no_process(monkeypatch):
    def boom(*a, **k):
        raise AssertionError("schedule-print must not start a process")
    monkeypatch.setattr(runner.subprocess, "run", boom)
    monkeypatch.setattr(runner.subprocess, "Popen", boom)


LINUX_WANT = ("[Service]", "Type=oneshot", "WorkingDirectory=", "ExecStart=", "/.venv/bin/python ",
              "-m litpipe.runner run --profile daily --scheduled", "TimeoutStartSec=20h", "[Timer]",
              "OnCalendar=*-*-* 01:00:00", "Persistent=true", "WantedBy=timers.target",
              "#Environment=LITPIPE_EMAIL=<", "#Environment=S2_API_KEY=<", "uv run --no-sync --project",
              "systemctl --user enable --now litpipe-runner.timer", "loginctl enable-linger",
              "0 1 * * * cd ", "/runner/cron.log", " 2>&1", "nothing was registered")
WINDOWS_WANT = ("Register-ScheduledTask", "New-ScheduledTaskAction", "-WorkingDirectory '", "set PYTHONUTF8=1&&",
                "\\.venv\\Scripts\\python.exe", "-m litpipe.runner run --profile daily --scheduled", "-At 01:00",
                "-StartWhenAvailable", "-AllowStartIfOnBatteries", "-DontStopIfGoingOnBatteries",
                "-ExecutionTimeLimit (New-TimeSpan -Hours 20)", "uv run --no-sync --project", "schtasks /Create",
                'cmd /c \\"cd /d ', "cannot be set", "nothing was registered")


@pytest.mark.parametrize("platform,want", [("linux", LINUX_WANT), ("windows", WINDOWS_WANT)])
def test_schedule_print_prints_one_nightly_task_and_never_registers(w, no_process, capsys, platform, want):
    before = w.listing()
    assert runner.main(["schedule-print", "--platform", platform]) == 0
    out = capsys.readouterr().out
    for s in want:
        assert s in out, s
    assert out.count("Register-ScheduledTask -TaskName") == (1 if platform == "windows" else 0)
    assert out.count("[Timer]") == (1 if platform == "linux" else 0)
    assert w.listing() == before


def test_schedule_print_defaults_to_this_platform_and_fills_in_this_checkout(w, no_process, capsys):
    assert runner.main(["schedule-print"]) == 0
    out = capsys.readouterr().out
    here = "windows" if os.name == "nt" else "linux"          # an oracle independent of the runner
    assert ("[Timer]" in out) is (here == "linux") and ("Register-ScheduledTask" in out) is (here == "windows")
    assert str(runner.REPO_ROOT) in out
    other = "windows" if here == "linux" else "linux"
    assert runner.main(["schedule-print", "--platform", other]) == 0
    out2 = capsys.readouterr().out
    assert "<checkout>" in out2 and str(runner.REPO_ROOT) not in out2     # another machine's path is not ours


def test_schedule_print_is_a_template_without_secrets(w, no_process, monkeypatch, capsys):
    monkeypatch.setenv("LITPIPE_EMAIL", "someone@university.edu")
    monkeypatch.setenv("S2_API_KEY", "s2-secret-key-0123456789")
    for platform in ("linux", "windows"):
        runner.main(["schedule-print", "--platform", platform])
    out = capsys.readouterr().out
    assert "someone@university.edu" not in out and "s2-secret-key-0123456789" not in out
    t = runner.schedule_text("linux", checkout="/srv/litpipe")
    if runner.current_platform() == "linux":
        assert f"{w.state_dir}/runner/cron.log" in t      # the cron log goes under the registry's state_dir
    else:
        assert "<state_dir>/runner/cron.log" in t         # another machine's state_dir is not ours
    t = runner.schedule_text("linux", checkout="/srv/litpipe")
    assert "WorkingDirectory=/srv/litpipe\n" in t and "ExecStart=/srv/litpipe/.venv/bin/python -m" in t
    assert "0 1 * * * cd /srv/litpipe && /srv/litpipe/.venv/bin/python -m" in t
    spaced = runner.schedule_text("linux", checkout="/srv/lit pipe")
    assert 'ExecStart="/srv/lit pipe/.venv/bin/python" -m' in spaced and "cd '/srv/lit pipe' &&" in spaced


def test_schedule_print_rejects_another_platform(capsys):
    assert runner.main(["schedule-print", "--platform", "macos"]) == 1


# ================================================================ amendment 3: the one-runner rule (unit)
def _live(run_id, seq, pid, kind="runner"):
    return {"run_id": run_id, "pid": pid, "kind": kind, "writer": True, "started": "2026-10-07T01:00:00+00:00",
            "heartbeat_age_s": 0.0, "seq": seq}


def test_one_runner_rule_orders_by_seq_not_started_or_pid():
    first = _live("20261007T010000Z-runner-9000-aaaaaa", 1, 9000)
    second = _live("20261007T010000Z-runner-1000-bbbbbb", 2, 1000)     # same second, the smaller pid
    live = sorted([first, second], key=lambda r: (r["started"], r["run_id"]))
    assert live[0] is second                                             # the second-string rule picks wrong
    assert runner.earlier_runner(live, second["run_id"]) is first        # the loser is the second
    assert runner.earlier_runner(live, first["run_id"]) is None
    other_kind = _live("20261007T005959Z-sweep-5-cccccc", 0, 5, kind="sweep")
    assert runner.earlier_runner([other_kind, first], first["run_id"]) is None   # interactive CLIs are not blocked


def test_a_refusing_runner_finishes_before_anything_else_and_exits_3(w, monkeypatch):
    w.register("research_a")
    w.queue("research_a", DOIS)
    winner = "20261007T010000Z-runner-9000-aaaaaa"
    real_live = state.live_runs

    def live():
        mine = real_live()
        return [_live(winner, 0, 9000)] + mine
    monkeypatch.setattr(state, "live_runs", live)
    pre = []
    monkeypatch.setattr("litpipe.preflight.run", lambda **kw: pre.append(1) or [])
    assert runner.main(["run", "--profile", "daily"]) == 3
    r = [x for x in w.runs() if x["kind"] == "runner"][-1]
    assert r["status"] == f"refused: runner {winner} is live" and r["finished"]
    assert pre == [] and w.calls() == []
    assert summary(w)["aborted"] == f"refused: runner {winner} is live"
