"""Verifier O (W4b): locks for the scheduled runner's surfaces: the result table against the real
stage dicts, run_daily as a wrapper, escalation, the database-writes gate, the dry run, the registry
exits and schedule-print. In-process on the builder's temp world (tests/fixtures/W4-A/w4a_world);
nothing here starts the runner as a subprocess, registers a task or sends a request."""
import configparser
import json
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import pytest

FIX = Path(__file__).resolve().parent / "fixtures" / "W4-A"
sys.path.insert(0, str(FIX))

from litpipe import canaries, runner, state  # noqa: E402
from w4a_world import World  # noqa: E402

REAL_CANARIES_RUN = canaries.run          # the world stubs it per test; the gate test needs the local phase
DOIS = ["10.5555/w4bo.0001", "10.5555/w4bo.0002"]
S2 = "api.semanticscholar.org"


@pytest.fixture
def w(tmp_path, monkeypatch):
    return World(tmp_path, monkeypatch)


def last(w):
    return w.summaries()[-1]


def jobs(s, name, project="*"):
    return [j for j in s["jobs"] if j["job"] == name and (project == "*" or j["project"] == project)]


# ================================================================ findings O-1 to O-4 (each failed on af45c48; fixed in the same commit)
@pytest.mark.skipif(sys.platform != "win32", reason="cmd.exe parses the schtasks fallback")
def test_o1_the_schtasks_fallback_reaches_schtasks_as_one_command_under_cmd(tmp_path):
    checkout = tmp_path / "no_such_checkout"            # never created: a split line runs nothing real
    text = runner.schedule_text("windows", checkout=str(checkout))
    line = next(ln for ln in text.splitlines() if ln.startswith("schtasks "))
    probe, argv_out = tmp_path / "argv_probe.py", tmp_path / "argv.json"
    probe.write_text("import json, sys\nopen(sys.argv[1], 'w', encoding='utf-8').write(json.dumps(sys.argv[2:]))\n",
                     encoding="utf-8")
    bat = tmp_path / "probe.cmd"
    bat.write_bytes(("@echo off\r\n" + f'"{sys.executable}" "{probe}" "{argv_out}"' + line[len("schtasks"):]
                     + "\r\n").encode("utf-8"))
    r = subprocess.run(["cmd", "/d", "/c", str(bat)], cwd=str(tmp_path), capture_output=True, text=True,
                       timeout=120)
    args = json.loads(argv_out.read_text(encoding="utf-8"))
    tr = args[args.index("/TR") + 1]
    assert "set PYTHONUTF8=1" in tr and tr.rstrip('"').endswith("--profile daily --scheduled"), (tr, r.stderr)
    assert r.stderr.strip() == "", r.stderr                           # cmd ran nothing after schtasks


def test_o2_a_manual_weekly_run_never_stamps_the_scheduled_escalation(w):
    w.register("research_a")
    w.register("research_b")
    assert runner.main(["run", "--profile", "weekly", "--project", "research_a"]) == 0
    assert last(w)["scheduled"] is False
    assert state.kv_get("runner", "profile_done:weekly") is None     # research_b's weekly jobs still owed


def _ps_parse_errors(tmp_path, text):
    exe = shutil.which("pwsh") or shutil.which("powershell")
    f = tmp_path / "task.ps1"
    f.write_text(text, encoding="utf-8")
    script = ("$t=$null;$e=$null;$s=Get-Content -Raw -LiteralPath '" + str(f) + "';"
              "[void][System.Management.Automation.Language.Parser]::ParseInput($s,[ref]$t,[ref]$e);$e.Count")
    r = subprocess.run([exe, "-NoProfile", "-NonInteractive", "-Command", script], capture_output=True, text=True,
                       timeout=180)
    return r.stdout.strip() + r.stderr.strip()


@pytest.mark.skipif(not (shutil.which("pwsh") or shutil.which("powershell")), reason="no PowerShell to parse with")
def test_o3_the_windows_task_command_parses_when_the_checkout_holds_an_apostrophe(tmp_path):
    text = runner.schedule_text("windows", checkout="D:\\o'brien\\litpipe")
    assert _ps_parse_errors(tmp_path, text[:text.index("# schtasks fallback")]) == "0"


def test_o4_a_failed_unpaywall_stage_never_consumes_the_candidate_order_ab(w):
    w.register("research_a")
    w.queue("research_a", DOIS)
    w.set_fake("sweep", "research_a", exit_code=3, retire=False, keep_reason="unpaywall failed",
               stages={"unpaywall": "failed", "pmc": "not_run", "preprint": "not_run", "extract": "not_run"})
    runner.main(["run", "--profile", "daily", "--scheduled"])
    assert last(w)["candidate_order"]["order"] == "repository"     # the A/B's first order was offered
    assert not state.kv_get("runner", "candidate_order_ab", [])    # ... and not used up: nothing was fetched


# ================================================================ the result table, from real dicts
REAL = [
    # job, module, kwargs, project, status
    ("sweep", "sweep", {"project": "research_a", "date": "2031-03-14", "loose_ends": False}, "research_a", "SKIPPED"),
    ("sweep", "sweep", {"project": "research_a", "date": "2031-03-14", "loose_ends": False,
                        "candidate_order": "random"}, "research_a", "FAILED"),
    ("route", "migrate_closed_to_md", {"project": "not_registered"}, "not_registered", "FAILED"),
    ("route", "migrate_closed_to_md", {"project": "research_a"}, "research_a", "SKIPPED"),
    ("recommendations", "enrich_recommendations", {}, None, "SKIPPED"),                     # status off
    ("recommendations", "enrich_recommendations", {"recent_feed": True}, None, "SKIPPED"),  # no_key
    ("reverse", "reverse_citations", {}, None, "FAILED"),
    ("index", "index_portfolio", {"project": "not_registered"}, None, "FAILED"),
    ("seed", "seed_queue_from_top_candidates", {"project": "research_a", "rank": "nonsense"}, "research_a", "FAILED"),
    ("audit", "audit_portfolio", {"project": "not_registered", "holdings": False}, None, "SKIPPED"),
]


@pytest.mark.parametrize("job,module,kwargs,project,want", REAL,
                         ids=[f"{r[0]}-{i}" for i, r in enumerate(REAL)])
def test_o_the_real_stage_dicts_map_through_the_result_table(w, job, module, kwargs, project, want):
    w.register("research_a")
    kwargs = dict(kwargs)
    if job == "index":
        kwargs["db"] = str(w.tmp / "idx" / "portfolio.duckdb")
    out = w.tmp / "result.json"
    runner.shim(module, kwargs, out, w.cfg_path, restore=True)        # the runner's own shim, in-process
    res, why = runner.read_result(out, runner.RESULT_TABLE[job][0])
    assert res is not None, why                                       # the keys the table needs are there
    status, reason, _ = runner.classify(job, runner.StageRun(0, res), project)
    assert status == want, (res.get("exit_code", res.get("status")), reason)


# ================================================================ the retired orchestrator locks (8a)
def test_o_two_projects_the_portfolio_jobs_run_once_with_project_none(w, monkeypatch):
    from litpipe import s2
    from litpipe.outcomes import Kind, Outcome
    w.register("research_a")
    w.register("research_b")
    monkeypatch.setenv("S2_API_KEY", "test-key-0123456789")
    monkeypatch.setattr(s2, "Session", lambda **kw: "session")
    monkeypatch.setattr(s2, "paper_batch", lambda *a, **k: Outcome(Kind.OK, attempts=1))
    runner.main(["run", "--profile", "monthly", "--db-writes"])
    s = last(w)
    for name in ("abstracts", "recommendations", "audit", "s2_keepalive"):
        js = jobs(s, name)
        assert len(js) == 1 and js[0]["project"] is None and js[0]["status"] == "OK", (name, js)
    assert len(w.calls("abstracts")) == 1 and len(w.calls("recommendations")) == 1
    assert sorted(c["project"] for c in w.calls("index")) == ["research_a", "research_b"]   # per project, by contrast


def test_o_with_snowball_over_two_projects_runs_walk_reverse_index_and_never_enrich(w, monkeypatch):
    import run_daily
    w.register("research_a")
    w.register("research_b")
    started = []
    monkeypatch.setattr(runner.subprocess, "run", lambda *a, **k: started.append(a) or (_ for _ in ()).throw(
        AssertionError("no subprocess")))
    res = run_daily.run(with_snowball=True)
    assert sorted(c["stage"] for c in w.calls()) == ["index", "index", "reverse", "reverse", "walk", "walk"]
    assert not [j for j in res["jobs"] if j["job"] in ("abstracts", "recommendations") and j["status"] != "SKIPPED"]
    assert "snowball" not in runner.STAGE_MODULES.values() and started == []
    assert res["run_daily_exit"] == 0 and res["db_writes"] is True


def test_o_the_sweep_gets_the_runs_date_and_the_route_its_run_id_across_midnight(w, monkeypatch):
    w.register("research_a")
    w.queue("research_a", DOIS)
    t0 = datetime(2031, 3, 14, 23, 59, 59).timestamp()          # local, like sweep's date.today()
    n = {"calls": 0}

    def now():
        n["calls"] += 1
        return t0 if n["calls"] == 1 else t0 + 5.0             # every later read is past midnight
    monkeypatch.setattr(runner, "_now", now)
    runner.main(["run", "--profile", "daily"])
    assert w.calls("sweep")[0]["date"] == "2031-03-14"
    rid = jobs(last(w), "sweep", "research_a")[0]["counts"]["run_id"]
    assert rid.startswith("2031-03-14") and w.calls("route")[0]["run_id"] == rid


# ================================================================ the DB-writes gate, instrumented
def _spy_duckdb(monkeypatch):
    import duckdb
    real, opens = duckdb.connect, []

    def spy(database=":memory:", read_only=False, *a, **k):
        opens.append((Path(str(database)).name, bool(read_only)))
        return real(database, read_only, *a, **k)
    monkeypatch.setattr(duckdb, "connect", spy)
    return opens


def test_o_gate_off_no_job_opens_portfolio_duckdb_for_writing_and_on_the_index_does(w, monkeypatch):
    from litpipe import s2
    from litpipe.outcomes import Kind, Outcome
    w.register("research_a", auto_stage=True)
    (w.lib("research_a") / "2020_Smith_Heat.pdf").write_bytes(b"%PDF-1.4\n%%EOF\n")
    db = w.db_dir / "portfolio.duckdb"
    w.db_dir.mkdir(parents=True, exist_ok=True)
    runner.shim("index_portfolio", {"project": "research_a", "db": str(db)}, w.tmp / "build.json", w.cfg_path,
                restore=True)                                    # a temp index for the read-only readers
    assert db.is_file()
    for job, mod in (("seed", "seed_queue_from_top_candidates"), ("audit", "audit_portfolio"),
                     ("index", "index_portfolio"), ("abstracts", "enrich_abstracts"),
                     ("recommendations", "enrich_recommendations")):
        monkeypatch.setitem(runner.STAGE_MODULES, job, mod)    # the real modules
    monkeypatch.setattr(canaries, "run", lambda profile, *, phase="all", **kw: (
        [] if phase == "network" else REAL_CANARIES_RUN(profile, phase=phase, **kw)))
    monkeypatch.setenv("S2_API_KEY", "test-key-0123456789")
    monkeypatch.setattr(s2, "Session", lambda **kw: "session")
    monkeypatch.setattr(s2, "paper_batch", lambda *a, **k: Outcome(Kind.OK, attempts=1))
    opens = _spy_duckdb(monkeypatch)
    runner.main(["run", "--profile", "monthly"])                # the gate off (the default)
    s = last(w)
    assert {j["job"] for j in s["skipped_db_jobs"]} == {"index", "abstracts", "recommendations"}
    mine = [ro for name, ro in opens if name == "portfolio.duckdb"]
    assert mine and all(mine), opens                            # read: yes (seed, audit, canary); write: never
    del opens[:]
    (w.lib("research_a") / "2021_Jones_Cold.pdf").write_bytes(b"%PDF-1.4\n%%EOF\n")
    runner.main(["run", "--profile", "daily", "--db-writes"])   # the control: the spy sees a write open
    assert (("portfolio.duckdb", False) in opens), opens


# ================================================================ registry exits before anything (item 8)
REGISTRY_CASES = ["missing", "bad_json", "bad_sources", "bad_auto_stage", "bad_cadence", "no_active_project",
                  "unknown_project", "runner_not_object", "runner_writes_type", "runner_order_value"]


@pytest.mark.parametrize("case", REGISTRY_CASES)
def test_o_registry_problems_exit_1_before_any_state_is_created(tmp_path, monkeypatch, case):
    w = World(tmp_path, monkeypatch, seed_state=False)
    argv = ["run", "--profile", "daily", "--scheduled"]
    entry = {"research_a": {}}
    if case == "bad_sources":
        entry = {"research_a": {"sources": "pubmed"}}
    elif case == "bad_auto_stage":
        entry = {"research_a": {"auto_stage": "yes"}}
    elif case == "bad_cadence":
        entry = {"research_a": {"walk_cadence_days": "weekly"}}
    elif case == "no_active_project":
        entry = {"research_a": {"active": False}}
    elif case == "unknown_project":
        argv += ["--project", "not_registered"]
    for key, extra in entry.items():
        w.register(key, **extra)
    if case == "runner_not_object":
        w.top["runner"] = ["unattended_db_writes"]
    elif case == "runner_writes_type":
        w.top["runner"] = {"unattended_db_writes": "yes"}
    elif case == "runner_order_value":
        w.top["runner"] = {"candidate_order": "random"}
    w.write()
    if case == "missing":
        w.cfg_path.unlink()
    elif case == "bad_json":
        w.cfg_path.write_bytes(b'{"projects": {')
    assert runner.main(argv) == 1
    assert not w.state_dir.exists() and w.calls() == []          # no state file, no runner/ directory


# ================================================================ the dry run over an existing state file
def test_o_a_dry_run_over_an_existing_state_file_changes_no_file(w):
    w.register("research_a", auto_stage=True, walk_cadence_days=7)
    w.queue("research_a", DOIS)
    assert runner.main(["run", "--profile", "daily", "--scheduled"]) in (0, 2)
    rid = last(w)["run_id"]
    state.refuse(S2, "an old run's refusal", persistence="run", run_id=rid)
    w.queue("research_a", DOIS)
    time.sleep(0.05)
    before = w.listing()
    assert runner.main(["run", "--profile", "daily", "--scheduled", "--dry-run", "--db-writes"]) == 0
    assert w.listing() == before


# ================================================================ schedule-print (both platforms, portable)
def _ini(block):
    cp = configparser.ConfigParser(interpolation=None, strict=True, delimiters=("=",))
    cp.optionxform = str
    cp.read_string(block)
    return {s: dict(cp[s]) for s in cp.sections()}


def test_o_the_linux_units_parse_as_ini_with_every_key_and_no_windows_trace(monkeypatch):
    # printed from another platform, so the cron log is the portable <state_dir> placeholder (on Linux
    # itself schedule_text names the registry's real state_dir; the next test covers that branch)
    monkeypatch.setattr(runner, "current_platform", lambda *a, **k: "windows")
    text = runner.schedule_text("linux", checkout="/srv/litpipe")
    service = text[text.index("[Unit]"):text.index("# Save as ~/.config/systemd/user/litpipe-runner.timer")]
    timer = text[text.index("# Save as ~/.config/systemd/user/litpipe-runner.timer"):text.index("# Enable it")]
    sv, tm = _ini(service), _ini(timer)
    assert sv["Service"] == {"Type": "oneshot", "WorkingDirectory": "/srv/litpipe",
                             "Environment": "PYTHONUNBUFFERED=1",
                             "ExecStart": "/srv/litpipe/.venv/bin/python -m litpipe.runner run --profile daily --scheduled",
                             "TimeoutStartSec": "20h"}
    assert sv["Unit"]["After"] == "network-online.target"
    assert tm == {"Unit": {"Description": "Run the literature pipeline runner nightly at 01:00"},
                  "Timer": {"OnCalendar": "*-*-* 01:00:00", "Persistent": "true"},
                  "Install": {"WantedBy": "timers.target"}}
    named = re.findall(r"^#Environment=([A-Z0-9_]+)=<[^>]*>$", text, re.M)
    assert named == ["LITPIPE_EMAIL", "S2_API_KEY", "OPENALEX_API_KEY"]   # names, never values
    cron = [ln for ln in text.splitlines() if ln.startswith("0 1 * * * ")]
    assert cron == ["0 1 * * * cd /srv/litpipe && /srv/litpipe/.venv/bin/python -m litpipe.runner run --profile "
                    "daily --scheduled >> '<state_dir>/runner/cron.log' 2>&1"]
    assert not re.search(r"[A-Za-z]:\\|\\\\|\bcmd\b|\.exe\b", text)


def test_o_the_cron_log_follows_the_registry_and_the_checkout_follows_the_runner(w, monkeypatch):
    w.top["state_dir"] = str(w.tmp / "litpipe-elsewhere-state")
    w.write()
    monkeypatch.setattr(runner, "current_platform", lambda: "linux")
    monkeypatch.setattr(runner, "REPO_ROOT", "/srv/litpipe-elsewhere")
    text = runner.schedule_text()
    assert "WorkingDirectory=/srv/litpipe-elsewhere" in text
    cron = next(ln for ln in text.splitlines() if ln.startswith("0 1 * * * "))
    assert "litpipe-elsewhere-state" in cron and "cron.log" in cron


@pytest.mark.skipif(not (shutil.which("pwsh") or shutil.which("powershell")), reason="no PowerShell to parse with")
def test_o_the_windows_task_command_parses_and_follows_the_checkout(tmp_path):
    co = "D:\\other\\litpipe"
    text = runner.schedule_text("windows", checkout=co)
    ps = text[:text.index("# schtasks fallback")]
    assert _ps_parse_errors(tmp_path, ps) == "0"
    for want in ("-WorkingDirectory 'D:\\other\\litpipe'", "-StartWhenAvailable", "-AllowStartIfOnBatteries",
                 "-DontStopIfGoingOnBatteries", "-ExecutionTimeLimit (New-TimeSpan -Hours 20)",
                 '"D:\\other\\litpipe\\.venv\\Scripts\\python.exe" -m litpipe.runner run --profile daily --scheduled',
                 "set PYTHONUTF8=1&&", "Register-ScheduledTask"):
        assert want in ps, want
    assert not re.search(r"[A-Za-z]:\\(?!other)", text)            # no drive path but the one given
