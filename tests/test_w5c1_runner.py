"""W5-C1 runner items: verifier O's follow-ups (item 6), the closing LOOSE_ENDS line (item 7), the
nightly `ris` job (item 8), verifier N's forwards (item 9) and the schedule portability rows (item 11)."""
import json
import sys
import threading
import time
from pathlib import Path

import pytest

from litpipe import runner, state

FIX = Path(__file__).resolve().parent / "fixtures" / "W4-A"
sys.path.insert(0, str(FIX))

from w4a_world import World  # noqa: E402

DOIS = ["10.5555/run.0001", "10.5555/run.0002"]


@pytest.fixture
def w(tmp_path, monkeypatch):
    return World(tmp_path, monkeypatch)


def summary(w):
    return w.summaries()[-1]


def jobs(w, job, project=None):
    return [j for j in summary(w)["jobs"] if j["job"] == job and (project is None or j["project"] == project)]


# ================================================================ item 6: verifier O's follow-ups
def test_a_zero_row_unpaywall_stage_spends_no_ab_slot(w):
    w.register("research_a")
    (w.proot("research_a") / "lit_pull_queue.csv").write_text("doi,title,authors,year,destination,notes\n",
                                                             encoding="utf-8")
    assert runner.main(["run", "--profile", "every_run", "--scheduled"]) == 0
    s = summary(w)
    assert s["candidate_order"]["source"] == "a/b" and s["candidate_order"]["unpaywall_ran"] is False
    assert state.kv_get("runner", "candidate_order_ab") is None              # the slot is still owed
    w.queue("research_a", DOIS)
    assert runner.main(["run", "--profile", "every_run", "--scheduled"]) == 0
    assert state.kv_get("runner", "candidate_order_ab") == ["repository"]


def test_the_escalation_is_stamped_with_the_runs_start(w, monkeypatch):
    w.register("research_a")
    t0 = 1_900_000_000.0
    clock = {"n": 0}

    def now():
        clock["n"] += 1
        return t0 if clock["n"] == 1 else t0 + 13 * 3600         # a run over 12 h
    monkeypatch.setattr(runner, "_now", now)
    assert runner.main(["run", "--profile", "weekly", "--scheduled"]) == 0
    assert state.kv_get("runner", "profile_done:weekly") == t0
    assert runner.MONTHLY_AFTER_S == 27.5 * 86400 and runner.WEEKLY_AFTER_S == 6.5 * 86400


# ================================================================ item 7: the closing LOOSE_ENDS line
PARTIAL = ("⏸️ Lit pull PARTIAL: research_a/ — lit_pull_queue.csv: pmc failed, left for re-sweep. "
           "Report: lit_pull_queue.2026-10-05.report.csv")


def test_a_clean_run_after_a_laptops_partial_writes_one_closing_line(w):
    w.register("research_a")
    w.loose.parent.mkdir(parents=True, exist_ok=True)
    w.loose.write_text(PARTIAL + "\n", encoding="utf-8")           # written by a laptop sweep: no kv here
    w.queue("research_a", DOIS)
    assert runner.main(["run", "--profile", "every_run"]) == 0
    lines = w.loose_lines()
    assert len(lines) == 2
    assert lines[1].startswith("✅ Lit pull done: research_a/ — closes the PARTIAL of 2026-10-05: 2/2 fetched")
    assert runner.CLOSES_MARK in lines[1] and summary(w)["loose_ends"]["research_a"].startswith("written (closes")
    w.queue("research_a", DOIS)                                    # a second clean run, same state
    assert runner.main(["run", "--profile", "every_run"]) == 0
    assert w.loose_lines() == lines                                 # nothing new


def test_the_closing_line_unit():
    done = "✅ Lit pull done: k/ — 1/2 fetched (Unpaywall 1, PMC 0, Preprint 0); 1 closed. Report: r.csv"
    assert runner.closing_line("k", done, PARTIAL.replace("research_a", "k")) == (
        "✅ Lit pull done: k/ — closes the PARTIAL of 2026-10-05: 1/2 fetched (Unpaywall 1, PMC 0, Preprint 0); "
        "1 closed. Report: r.csv")
    undated = "⏸️ Lit pull PARTIAL: k/ — lit_pull_queue.csv: refused, left for re-sweep."
    assert "closes the earlier PARTIAL: 1/2" in runner.closing_line("k", done, undated)
    assert runner.closing_line("k", done, done) is None                     # the previous line is not a PARTIAL
    assert runner.closing_line("k", undated, undated) is None               # this run is not clean
    assert runner.closing_line("k", done, None) is None


# ================================================================ item 8: text-only holdings get a .ris
def _text_only(lib, stem, doi):
    (lib / f"{stem}.fulltext.json").write_text(json.dumps(
        {"doi": doi, "title": "A text-only paper", "authors": ["Smith, Jane"], "year": 2020, "journal": "J",
         "text": "body text", "has_pdf": False}), encoding="utf-8")


def test_the_ris_job_writes_a_ris_for_a_text_only_holding_and_keeps_a_curated_one(w, monkeypatch):
    import ris_emit
    monkeypatch.setitem(runner.STAGE_MODULES, "ris", "backfill_ris")       # the real module, in-process
    # since W5 a text-only holding asks its agency first; no source holds this DOI: the sidecar is used
    monkeypatch.setattr(ris_emit, "resolve_meta", lambda doi: ({}, "none"))
    w.register("research_a")
    lib = w.lib("research_a")
    _text_only(lib, "2020_Smith_TextOnly", "10.5555/text.0001")
    (lib / "2019_Doe_Curated.pdf").write_bytes(b"%PDF-1.4")
    curated = lib / "2019_Doe_Curated.ris"
    curated.write_text("TY  - JOUR\nTI  - Hand edited\nDO  - 10.5555/cur.0001\nER  - \n", encoding="utf-8")
    before = curated.read_bytes()
    assert runner.main(["run", "--profile", "daily"]) == 0
    ris = lib / "2020_Smith_TextOnly.ris"
    assert ris.is_file() and "10.5555/text.0001" in ris.read_text(encoding="utf-8")
    assert curated.read_bytes() == before
    j = jobs(w, "ris", "research_a")[0]
    assert j["status"] == "OK" and j["counts"]["written"] == 1 and j["counts"]["needed"] == 1


def test_a_network_failure_in_the_ris_job_is_degraded_never_failed(w, monkeypatch):
    import ris_emit
    monkeypatch.setitem(runner.STAGE_MODULES, "ris", "backfill_ris")

    def unavailable(doi):
        raise ris_emit.MetadataUnavailable("crossref", detail="connect timeout")
    monkeypatch.setattr(ris_emit, "resolve_meta", unavailable)
    w.register("research_a")
    lib = w.lib("research_a")
    (lib / "2021_Roe_NoRis.pdf").write_bytes(b"%PDF-1.4")
    (lib / "2021_Roe_NoRis.fulltext.json").write_text(json.dumps({"doi": "10.5555/net.0001", "text": "t",
                                                                  "extracted_from_pdf": True}), encoding="utf-8")
    assert runner.main(["run", "--profile", "daily"]) == 2
    j = jobs(w, "ris", "research_a")[0]
    assert j["status"] == "DEGRADED" and "network" in j["reason"] and j["counts"]["meta_unavailable"] == 1
    assert not (lib / "2021_Roe_NoRis.ris").exists()


def test_the_ris_limit_cuts_at_the_nth_holding_that_needs_one(w):
    w.top["runner"] = {"ris_limit": 2}
    w.register("research_a")
    lib = w.lib("research_a")
    for n in ("a", "b", "c", "d", "e"):
        (lib / f"{n}.pdf").write_bytes(b"%PDF")
    (lib / "a.ris").write_text("TY  - JOUR\nER  - \n", encoding="utf-8")
    (lib / "c.ris").write_text("TY  - JOUR\nER  - \n", encoding="utf-8")
    _text_only(lib, "f_textonly", "10.5555/t.1")
    cfg = json.loads(w.cfg_path.read_text(encoding="utf-8"))
    r = runner._ScheduledRun(cfg, ["research_a"])
    need, limit = r._ris_todo(r.projects[0])
    assert need == 4 and limit == 4                  # sorted a b c d e f: b and d need one; d is item 4
    w.top["runner"] = {"ris_limit": 200}
    w.write()
    r = runner._ScheduledRun(json.loads(w.cfg_path.read_text(encoding="utf-8")), ["research_a"])
    assert r._ris_todo(r.projects[0]) == (4, 0)      # under the limit: no cut


def test_the_ris_job_is_daily_listed_by_the_dry_run_and_passes_its_arguments(w, capsys):
    w.register("research_a")
    _text_only(w.lib("research_a"), "x", "10.5555/t.2")
    assert runner.main(["run", "--profile", "every_run"]) == 0
    assert jobs(w, "ris", "research_a")[0]["status"] == "SKIPPED" and w.calls("ris") == []
    assert runner.main(["run", "--profile", "daily", "--dry-run"]) == 0
    assert "would write a .ris for up to 200 of 1 holding(s)" in capsys.readouterr().out
    assert runner.main(["run", "--profile", "daily"]) == 0
    call = w.calls("ris")[0]
    assert call["project"] == "research_a" and call["commit"] is True and call["include_text_only"] is True
    assert call["limit"] == 0


# ================================================================ item 9: verifier N's forwards
def test_heartbeat_slice_is_one_second():
    assert runner.HEARTBEAT_SLICE_S == 1.0


def test_the_heartbeat_records_a_wall_clock_jump_within_one_slice(monkeypatch):
    hb = runner.Heartbeat("r", beat=lambda rid: True)
    monkeypatch.setattr(runner, "HEARTBEAT_EVERY_S", 3600.0)
    real = time.time
    offset = [0.0]
    monkeypatch.setattr(runner.time, "time", lambda: real() + offset[0])
    hb.start()
    try:
        time.sleep(0.2)
        offset[0] = 7200.0                                   # the lid opens two hours later
        deadline = real() + 1.0 + 1.5                        # one slice (1 s) plus scheduling slack
        while hb.jumps == 0 and real() < deadline:
            time.sleep(0.05)
        assert hb.jumps >= 1 and hb.jumped() > 3600
    finally:
        hb.stop()


def test_runner_timeouts_come_from_the_registry_and_the_cli_wins(w):
    w.top["runner"] = {"timeouts": {"sweep": 120, "ris": 60}}
    w.register("research_a")
    cfg = json.loads(w.cfg_path.read_text(encoding="utf-8"))
    r = runner._ScheduledRun(cfg, ["research_a"])
    assert r.timeouts["sweep"] == 120 and r.timeouts["ris"] == 60 and r.timeouts["walk"] == runner.TIMEOUTS["walk"]
    r = runner._ScheduledRun(cfg, ["research_a"], timeouts={"sweep": 7})
    assert r.timeouts["sweep"] == 7
    for bad in ({"nope": 5}, {"sweep": 0}, {"sweep": "1h"}, ["sweep"]):
        w.top["runner"] = {"timeouts": bad}
        w.write()
        assert runner.main(["run", "--profile", "every_run"]) == 1


def test_a_stage_record_is_kept_while_the_child_runs(monkeypatch):
    monkeypatch.setattr(runner, "ORPHAN_CHECK", True)
    monkeypatch.setattr(runner, "_proc_start", lambda pid: f"proc:{pid}")
    step = runner.Step("sweep", "sweep", {}, "research_a", 10, Path("l"), Path("k"), Path("o"), Path("r"), "run-1")
    runner._record_stage(step, 4321)
    rec = state.kv_get("runner", runner.ACTIVE_STAGE_KEY)
    assert rec["pid"] == 4321 and rec["pgid"] == 4321 and rec["start"] == "proc:4321" and rec["run_id"] == "run-1"
    runner._record_stage(step, None)
    assert state.kv_get("runner", runner.ACTIVE_STAGE_KEY) is None


class FakeProcs:
    """A fake process table for the orphan check."""

    def __init__(self, monkeypatch, alive=True, start="proc:777"):
        self.alive, self.start, self.killed = alive, start, []
        monkeypatch.setattr(runner, "ORPHAN_CHECK", True)
        monkeypatch.setattr(runner, "_group_alive", lambda pgid: self.alive)
        monkeypatch.setattr(runner, "_proc_start", lambda pid: self.start)
        monkeypatch.setattr(runner, "_kill_group", lambda pgid: self.killed.append(pgid) or "SIGTERM")


def _leave_stage(run_id="20261006T010000Z-runner-999-abc", start="proc:777"):
    state.kv_set("runner", runner.ACTIVE_STAGE_KEY, {"run_id": run_id, "pid": 31337, "pgid": 31337,
                                                     "start": start, "job": "sweep", "project": "research_a"})


def test_an_orphaned_stage_group_is_reported_and_killed_when_its_start_matches(w, monkeypatch):
    w.register("research_a")
    procs = FakeProcs(monkeypatch)
    _leave_stage()
    assert runner.main(["run", "--profile", "every_run"]) == 0
    o = jobs(w, "orphaned_stage")[0]
    assert o["status"] == "DEGRADED" and o["counts_for_exit"] is False and "killed" in o["reason"]
    assert procs.killed == [31337] and state.kv_get("runner", runner.ACTIVE_STAGE_KEY) is None


def test_a_reused_pid_is_reported_never_killed(w, monkeypatch):
    w.register("research_a")
    procs = FakeProcs(monkeypatch, start="proc:123456")             # the pid's start time differs
    _leave_stage()
    assert runner.main(["run", "--profile", "every_run"]) == 0
    o = jobs(w, "orphaned_stage")[0]
    assert "not killed" in o["reason"] and procs.killed == []


def test_a_gone_group_reports_nothing_and_a_live_run_is_left_alone(w, monkeypatch):
    w.register("research_a")
    procs = FakeProcs(monkeypatch, alive=False)
    _leave_stage()
    assert runner.main(["run", "--profile", "every_run"]) == 0
    assert jobs(w, "orphaned_stage") == [] and procs.killed == []
    assert state.kv_get("runner", runner.ACTIVE_STAGE_KEY) is None
    procs.alive = True
    live = state.register_run("runner")                              # its runner is alive: not an orphan
    _leave_stage(run_id=live)
    assert runner.find_orphans("me") == [] and procs.killed == []
    state.finish_run(live)


def test_a_wholly_fetched_chain_gets_a_header_only_routing_csv(tmp_path, monkeypatch):
    import csv
    import lit_util
    import migrate_closed_to_md as mig
    root = tmp_path / "Projects"
    (root / "P").mkdir(parents=True)
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    cfg = {"projects": {"P": {"lib_dir": "lit"}}, "state_dir": str(tmp_path / "s")}
    day = "2026-10-07"
    for tag in ("", "b-all"):
        name = f"lit_pull_queue.{tag + '.' if tag else ''}{day}.residual.csv"
        (root / "P" / name).write_text("doi,residual_class,reason\n", encoding="utf-8")
    res = mig.run("P", run_id=day, cfg=cfg, use_holdings=False)
    assert res["status"] == "nothing"
    for tag in ("", "b-all"):
        p = root / "P" / f"lit_pull_queue.{tag + '.' if tag else ''}{day}.routing.csv"
        with open(p, encoding="utf-8") as f:
            assert next(csv.reader(f)) == mig.ROUTING_FIELDS and f.read() == ""
    assert not (root / "P" / f"lit_pull_queue.other.{day}.routing.csv").exists()
    dry = tmp_path / "Projects" / "P" / f"lit_pull_queue.{day}.routing.csv"
    dry.unlink()
    mig.run("P", run_id=day, cfg=cfg, use_holdings=False, dry_run=True)
    assert not dry.exists()                                          # a dry run writes nothing


def test_a_multi_queue_night_with_a_wholly_fetched_chain_passes_lost_artifacts(w, monkeypatch):
    """N-3's night with the stand-in route, which is now the real migrate."""
    from litpipe import canaries

    def local(profile, *, phase="all", context=None, cfg=None, **kw):
        w.canary_calls.append({"phase": phase, "context": context})
        return REAL_CANARIES(profile, phase=phase, context=context, cfg=cfg) if phase == "local" else []
    monkeypatch.setattr(canaries, "run", local)
    w.register("research_a")
    w.queue("research_a", DOIS)
    w.queue("research_a", ["10.5555/run.0003"], tag="b-extra")
    w.set_fake("sweep", "research_a", classes={DOIS[1]: "TERMINAL_CLOSED"})
    code = runner.main(["run", "--profile", "every_run"])
    s = summary(w)
    ctx = [c for c in w.canary_calls if c["phase"] == "local"][0]["context"]["projects"][0]
    assert "routing" in ctx["stages"]
    lost = [c for c in s["canaries"]["checks"] if c["id"] == "lost_artifacts"]
    assert lost and all(c["status"] == "PASS" for c in lost), lost
    assert code == 0, s["reasons"]


# ================================================================ item 11: schedule portability
def test_macos_gets_a_best_effort_crontab_line_only():
    assert runner.current_platform("posix", "darwin") == "macos"
    assert runner.current_platform("posix", "linux") == "linux" and runner.current_platform("nt", "win32") == "windows"
    t = runner.schedule_text("macos", checkout="/Users/someone/litpipe", at="01:00")
    assert "BEST-EFFORT" in t and "[Unit]" not in t and "Register-ScheduledTask" not in t
    cron = [ln for ln in t.splitlines() if ln and not ln.startswith("#")]
    assert len(cron) == 1 and cron[0].startswith("0 1 * * * cd /Users/someone/litpipe && ")
    assert runner.main(["schedule-print", "--platform", "macos"]) == 0


def test_the_schedule_time_reaches_every_template(monkeypatch):
    lin = runner.schedule_text("linux", checkout="/srv/litpipe", at="02:30")
    assert "OnCalendar=*-*-* 02:30:00" in lin and "\n30 2 * * * cd /srv/litpipe" in lin and "01:00" not in lin
    win = runner.schedule_text("windows", checkout="C:\\litpipe", at="02:30")
    assert "-Daily -At 02:30" in win and "/ST 02:30" in win and "01:00" not in win
    mac = runner.schedule_text("macos", checkout="/Users/x/litpipe", at="23:05")
    assert "\n5 23 * * * cd /Users/x/litpipe" in mac
    default = runner.schedule_text("linux", checkout="/srv/litpipe", at=None)
    assert "OnCalendar=*-*-* 01:00:00" in default and "\n0 1 * * * cd /srv/litpipe" in default


def test_the_registry_schedule_time_is_used_and_validated(w, capsys):
    w.top["runner"] = {"schedule_time": "03:15"}
    w.register("research_a")
    assert runner.main(["schedule-print", "--platform", "linux"]) == 0
    assert "OnCalendar=*-*-* 03:15:00" in capsys.readouterr().out
    for bad in ("3:15", "25:00", "noon", 315):
        w.top["runner"] = {"schedule_time": bad}
        w.write()
        assert runner.main(["schedule-print", "--platform", "linux"]) == 1
        assert runner.main(["run", "--profile", "every_run"]) == 1


from litpipe import canaries as _canaries  # noqa: E402

REAL_CANARIES = _canaries.run
