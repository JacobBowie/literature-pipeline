"""litpipe.state (cross-process pacing, budgets, deferrals, refusals, run registry, kv) and
litpipe.preflight (dispatch W1-A2).

Isolation: an autouse fixture points litpipe.config.CONFIG_PATH at a temp registry whose state_dir
is a temp dir, so no test can reach the real state dir; child processes get the temp db path
explicitly. No test makes a network request: preflight runs with a fake `request` (or
network=False). Tests that start child processes use the real clock (a child cannot share the
fake one); the rest use FakeClock so pacing tests take milliseconds.
"""
import json
import os
import sqlite3
import subprocess
import sys
import textwrap
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

import litpipe.config as config
from litpipe import preflight, state
from litpipe.outcomes import Kind, Outcome

REPO = Path(__file__).resolve().parent.parent
T0 = 1_790_000_000.0  # 2026-09-21T14:13:20Z, a fixed fake-clock origin


@pytest.fixture(autouse=True)
def isolated_state(tmp_path, monkeypatch):
    sd = tmp_path / "state"
    reg = tmp_path / "projects.json"
    reg.write_text(json.dumps({"state_dir": str(sd)}), encoding="utf-8")
    monkeypatch.setattr(config, "CONFIG_PATH", reg)
    monkeypatch.setattr(state, "DB_PATH", None)
    monkeypatch.setattr(state, "_current_run", None)
    monkeypatch.delenv(state.RUN_ENV, raising=False)
    return sd / state.DB_NAME


class FakeClock:
    def __init__(self, t=T0):
        self.t = t
        self.slept = []

    def time(self):
        return self.t

    def sleep(self, s):
        self.slept.append(s)
        self.t += max(s, 0.0)


@pytest.fixture
def clock(monkeypatch):
    c = FakeClock()
    monkeypatch.setattr(state, "_time", c.time)
    monkeypatch.setattr(state, "_sleep", c.sleep)
    return c


PRE = textwrap.dedent("""
    import json, sys, time
    sys.path.insert(0, sys.argv[1])
    from pathlib import Path
    from litpipe import state
    state.DB_PATH = Path(sys.argv[2])
""")


def child(code, db, *args, env_extra=None, stdin=False):
    env = {k: v for k, v in os.environ.items() if k != state.RUN_ENV}
    env.update(env_extra or {})
    return subprocess.Popen([sys.executable, "-c", PRE + textwrap.dedent(code), str(REPO), str(db),
                             *map(str, args)],
                            stdin=subprocess.PIPE if stdin else subprocess.DEVNULL,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)


def run_child(code, db, *args, **kw):
    p = child(code, db, *args, **kw)
    out, err = p.communicate(timeout=60)
    assert p.returncode == 0, err
    return out


def db_row(db, sql, args=()):
    with sqlite3.connect(db) as con:
        return con.execute(sql, args).fetchone()


# ================================================================== location and schema
def test_db_lives_in_config_state_dir_in_wal_mode(isolated_state):
    assert state.db_path() == isolated_state
    state.kv_set("t", "k", 1)
    assert isolated_state.exists()
    assert db_row(isolated_state, "PRAGMA journal_mode")[0] == "wal"
    assert db_row(isolated_state, "PRAGMA user_version")[0] == state.SCHEMA_VERSION


def test_db_path_without_create_does_not_make_the_dir(isolated_state):
    p = state.db_path(create=False)
    assert p == isolated_state and not p.parent.exists()


def test_schema_is_rebuilt_when_the_file_is_replaced(isolated_state):
    state.kv_set("t", "k", 1)
    for suffix in ("", "-wal", "-shm"):
        Path(str(isolated_state) + suffix).unlink(missing_ok=True)
    state.kv_set("t", "k", 2)  # no "no such table": the schema check runs per connection
    assert state.kv_get("t", "k") == 2


# ================================================================== cross-process pacing
PACE_WORKER = """
    interval, n, go = float(sys.argv[3]), int(sys.argv[4]), float(sys.argv[5])
    slow = float(sys.argv[6]) if len(sys.argv) > 6 else 0.0
    if slow:
        state._before_claim = lambda: time.sleep(slow)
    while time.time() < go:
        time.sleep(0.002)
    out = []
    for _ in range(n):
        s = state.acquire("pace.test", interval=interval, budget=None, concurrency=1)
        time.sleep(0.02)                 # the request
        end = time.time()
        state.release("pace.test", True, s)
        out.append([s.start, end])
    print(json.dumps(out))
"""


def run_pacers(db, n_procs, interval, n, slow=0.0):
    state.kv_set("init", "k", 0)  # create the file and schema before the race
    go = time.time() + 2.0
    procs = [child(PACE_WORKER, db, interval, n, go, slow) for _ in range(n_procs)]
    slots = []
    for i, p in enumerate(procs):
        out, err = p.communicate(timeout=120)
        assert p.returncode == 0, err
        slots += [(s, e, i) for s, e in json.loads(out)]
    assert len(slots) == n_procs * n
    return sorted(slots)


@pytest.mark.parametrize("n_procs", [2, 4])
def test_processes_keep_every_gap_at_or_above_the_interval(isolated_state, n_procs):
    interval, n = 0.1, 5
    slots = run_pacers(isolated_state, n_procs, interval, n)
    for (s1, e1, _), (s2, _, _) in zip(slots, slots[1:]):
        assert s2 - s1 >= interval - 1e-6          # start to start
        assert s2 - e1 >= interval - 1e-6          # end of the previous attempt to start
    owners = [o for _, _, o in slots]
    assert sum(a != b for a, b in zip(owners, owners[1:])) >= n_procs - 1  # they really contended
    assert state.day_count("pace.test") == n_procs * n


def test_a_slow_claim_is_still_atomic_across_processes(isolated_state):
    # Stretch the read-to-write window to 30 ms: under BEGIN IMMEDIATE the others wait on the
    # lock; without it two processes read the same next-allowed time and both claim it.
    interval = 0.05
    slots = run_pacers(isolated_state, 4, interval, 4, slow=0.03)
    for (s1, _, _), (s2, _, _) in zip(slots, slots[1:]):
        assert s2 - s1 >= interval - 1e-6


def test_acquire_waits_out_the_interval(clock):
    s1 = state.acquire("a.test", interval=2.0, budget=None, concurrency=1)
    state.release("a.test", True, s1)
    s2 = state.acquire("a.test", interval=2.0, budget=None, concurrency=1)
    assert s2.start - s1.start >= 2.0


def test_release_pushes_next_allowed_to_end_of_a_long_attempt(clock):
    s1 = state.acquire("slow.test", interval=1.0, budget=None, concurrency=1)
    clock.t += 3.0                                   # the attempt outlasted the interval
    end = clock.t
    state.release("slow.test", True, s1)
    s2 = state.acquire("slow.test", interval=1.0, budget=None, concurrency=1)
    assert s2.start >= end + 1.0                     # spacing counts from the end
    assert sum(clock.slept) == pytest.approx(1.0)


def test_release_spaces_from_end_even_after_a_short_attempt(clock):
    s1 = state.acquire("fast.test", interval=1.0, budget=None, concurrency=1)
    clock.t += 0.3
    end = clock.t
    state.release("fast.test", True, s1)
    s2 = state.acquire("fast.test", interval=1.0, budget=None, concurrency=1)
    assert s2.start >= end + 1.0


def test_host_given_as_url_is_the_same_host(clock):
    state.acquire("https://API.Example.test/v2/x?y=1", interval=0, budget=None, concurrency=5)
    assert state.day_count("api.example.test") == 1


def test_clock_step_back_does_not_block_the_host_for_the_step(clock):
    clock.t = T0 + 10_000
    state.release("step.test", True, state.acquire("step.test", interval=1.0, budget=None,
                                                   concurrency=1))
    clock.t = T0                                     # the wall clock stepped back ~3 h
    clock.slept.clear()
    state.acquire("step.test", interval=1.0, budget=None, concurrency=1)
    assert sum(clock.slept) <= 1.0 + 1e-9


def test_policy_comes_from_litpipe_hosts_by_bare_host(monkeypatch):
    import types
    import litpipe
    seen = []

    class Pol:
        min_interval_s, concurrency, daily_budget = 0.75, 2, 9
    fake = types.ModuleType("litpipe.hosts")
    fake.policy = lambda h: seen.append(h) or Pol()
    monkeypatch.setitem(sys.modules, "litpipe.hosts", fake)
    monkeypatch.setattr(litpipe, "hosts", fake, raising=False)
    assert state._policy_values("api.crossref.org") == (0.75, 9, 2)
    assert seen == ["api.crossref.org"]


def test_policy_falls_back_to_the_unknown_host_default_without_litpipe_hosts(monkeypatch):
    import litpipe
    monkeypatch.setitem(sys.modules, "litpipe.hosts", None)   # import raises ModuleNotFoundError
    monkeypatch.delattr(litpipe, "hosts", raising=False)
    assert state._policy_values("x.test") == (state.DEFAULT_INTERVAL_S, None, 1)


def test_policy_defaults_apply_when_the_caller_passes_none(clock, monkeypatch):
    monkeypatch.setattr(state, "_policy_values", lambda host: (5.0, 1, 1))
    s1 = state.acquire("policy.test")
    state.release("policy.test", True, s1)
    with pytest.raises(state.BudgetExhausted):
        state.acquire("policy.test")


# ================================================================== concurrency leases
def test_a_held_lease_blocks_until_release(clock):
    state.acquire("lease.test", interval=0, budget=None, concurrency=1, lease_s=1.0)
    clock.slept.clear()
    state.acquire("lease.test", interval=0, budget=None, concurrency=1)
    assert sum(clock.slept) >= 1.0 - 1e-9            # waited for the unreleased lease to expire


def test_concurrency_two_admits_two_leases(clock):
    state.acquire("c2.test", interval=0, budget=None, concurrency=2)
    state.acquire("c2.test", interval=0, budget=None, concurrency=2)
    assert clock.slept == []


def test_a_dead_holders_lease_is_reclaimed(isolated_state):
    run_child("""
        state.acquire("orphan.test", interval=0, budget=None, concurrency=1, lease_s=3600)
    """, isolated_state)  # exits without release
    t = time.monotonic()
    state.acquire("orphan.test", interval=0, budget=None, concurrency=1)
    assert time.monotonic() - t < 2.0


# ================================================================== budget and deferral
def test_exhausted_budget_raises_until_the_next_utc_day(clock):
    for _ in range(3):
        state.release("b.test", True, state.acquire("b.test", interval=0, budget=3, concurrency=1))
    assert state.day_count("b.test") == 3
    with pytest.raises(state.BudgetExhausted) as e:
        state.acquire("b.test", interval=0, budget=3, concurrency=1)
    midnight = datetime(2026, 9, 22, tzinfo=timezone.utc).timestamp()
    assert e.value.until == midnight and e.value.host == "b.test"
    assert e.value.retry_after == pytest.approx(midnight - T0)   # seconds, for net's DEFERRED
    assert state.day_count("b.test") == 3            # the refused claim did not count
    clock.t = midnight + 1
    assert state.day_count("b.test") == 0
    state.acquire("b.test", interval=0, budget=3, concurrency=1)
    assert state.day_count("b.test") == 1


def test_failed_attempts_count_against_the_budget(clock):
    state.release("f.test", False, state.acquire("f.test", interval=0, budget=1, concurrency=1))
    with pytest.raises(state.BudgetExhausted):
        state.acquire("f.test", interval=0, budget=1, concurrency=1)


def test_deferred_host_raises_until_the_deferral_ends(clock):
    state.defer("d.test", T0 + 600, "Retry-After 600")
    state.defer("d.test", T0 + 60, "shorter")        # the later deferral wins
    with pytest.raises(state.HostDeferred) as e:
        state.acquire("d.test", interval=0, budget=None, concurrency=1)
    assert isinstance(e.value, state.BudgetExhausted)  # net maps BudgetExhausted to DEFERRED
    assert e.value.until == T0 + 600 and "Retry-After 600" in e.value.reason
    assert e.value.retry_after == 600
    assert state.deferred_until("d.test") == T0 + 600
    clock.t = T0 + 601
    state.acquire("d.test", interval=0, budget=None, concurrency=1)
    assert state.deferred_until("d.test") is None


def test_defer_takes_an_aware_datetime_and_rejects_a_naive_one(clock):
    state.defer("dt.test", datetime.fromtimestamp(T0 + 30, timezone.utc))
    assert state.deferred_until("dt.test") == T0 + 30
    with pytest.raises(ValueError):
        state.defer("dt.test", datetime(2026, 9, 30))
    assert state.clear_deferral("dt.test") is True
    assert state.deferred_until("dt.test") is None


# ================================================================== refusals
def test_manual_refusal_survives_a_new_run_and_clears_only_with_clear_refusal(isolated_state):
    r1 = state.register_run("runner")
    state.refuse("export.arxiv.org", "HTTP 406", persistence="manual")
    state.finish_run(r1, "ok")
    r2 = state.register_run("runner")
    assert state.is_refused("export.arxiv.org")
    assert run_child("print(state.is_refused('export.arxiv.org'))", isolated_state).strip() == "True"
    state.finish_run(r2, "ok")
    assert state.is_refused("export.arxiv.org")
    assert state.clear_refusal("export.arxiv.org") is True
    assert not state.is_refused("export.arxiv.org")
    assert state.clear_refusal("export.arxiv.org") is False


def test_run_refusal_clears_when_the_run_finishes():
    r = state.register_run("runner")
    state.refuse("www.example.test", "HTTP 403")     # default persistence: run
    assert state.is_refused("www.example.test")
    state.finish_run(r, "ok")
    assert not state.is_refused("www.example.test")


def test_run_refusal_ends_with_the_refusing_process_but_manual_does_not(isolated_state):
    run_child("""
        state.register_run("sweep")
        state.refuse("run.test", "HTTP 403")
        state.refuse("manual.test", "HTTP 406", persistence="manual")
    """, isolated_state)  # exits without finish_run
    assert not state.is_refused("run.test")
    assert state.is_refused("manual.test")


def test_stage_subprocess_refusal_holds_for_the_parent_run(isolated_state):
    r = state.register_run("runner")
    run_child("state.refuse('stage.test', 'HTTP 429 final')", isolated_state,
              env_extra={state.RUN_ENV: r})          # the runner passes its run to a stage
    assert state.is_refused("stage.test")             # the stage exited; the run is live
    state.finish_run(r, "ok")
    assert not state.is_refused("stage.test")


def test_a_run_refusal_never_downgrades_a_manual_one():
    state.refuse("m.test", "HTTP 406", persistence="manual")
    r = state.register_run("runner")
    state.refuse("m.test", "HTTP 403")
    state.finish_run(r)
    assert state.is_refused("m.test")


def test_refuse_rejects_an_unknown_persistence():
    with pytest.raises(ValueError):
        state.refuse("x.test", "why", persistence="forever")


def test_refusal_reason_is_redacted(isolated_state, monkeypatch):
    monkeypatch.setenv("LITPIPE_EMAIL", "pipeline.owner@uconn.edu")
    state.refuse("r.test", "HTTP 406 for https://r.test/q?email=pipeline.owner%40uconn.edu&x=1 "
                 "(mailto:pipeline.owner@uconn.edu)", persistence="manual")
    reason = db_row(isolated_state, "SELECT refused_reason FROM hosts WHERE host='r.test'")[0]
    assert "HTTP 406" in reason
    for form in ("pipeline.owner@uconn.edu", "pipeline.owner%40uconn.edu", "uconn.edu"):
        assert form not in reason


# ================================================================== run registry
def test_stale_heartbeat_drops_a_run_from_live_runs(clock):
    r = state.register_run("runner")
    assert [x["run_id"] for x in state.live_runs()] == [r]
    clock.t += state.HEARTBEAT_STALE_S + 1
    assert state.live_runs() == []
    assert state.heartbeat(r) is True                # a heartbeat revives it
    assert [x["run_id"] for x in state.live_runs()] == [r]


def test_live_runs_drops_a_run_whose_process_is_gone(isolated_state):
    # The run records the interpreter's own pid. Under a Windows venv, sys.executable is a
    # launcher that starts the interpreter as its child, so Popen.pid is NOT that pid.
    p = child("""
        import os
        print(state.register_run("sweep"), os.getpid(), flush=True)
        sys.stdin.read()
    """, isolated_state, stdin=True)
    rid, pid = p.stdout.readline().split()
    live = {x["run_id"]: x for x in state.live_runs()}
    assert rid in live and live[rid]["writer"] is True and live[rid]["pid"] == int(pid)
    p.stdin.close()
    p.wait(timeout=30)
    assert rid not in {x["run_id"] for x in state.live_runs()}
    mine = state.register_run("runner")               # registering tidies the dead run up
    assert db_row(isolated_state, "SELECT status FROM runs WHERE run_id=?", (rid,))[0] == "abandoned"
    assert [x["run_id"] for x in state.live_runs()] == [mine]


def test_finish_run_closes_the_run_and_its_leases(isolated_state):
    r = state.register_run("walk", writer=False)
    assert state.current_run() == r and state.live_runs()[0]["writer"] is False
    state.acquire("fr.test", interval=0, budget=None, concurrency=1)
    state.finish_run(r, "ok")
    assert state.current_run() is None and state.live_runs() == []
    assert db_row(isolated_state, "SELECT COUNT(*) FROM leases")[0] == 0
    assert db_row(isolated_state, "SELECT status FROM runs WHERE run_id=?", (r,))[0] == "ok"
    assert state.heartbeat(r) is False


def test_run_env_names_the_current_run(monkeypatch):
    monkeypatch.setenv(state.RUN_ENV, "20260930T000000Z-runner-1-abc")
    assert state.current_run() == "20260930T000000Z-runner-1-abc"


def test_register_run_rejects_an_empty_kind():
    with pytest.raises(ValueError):
        state.register_run("  ")


# ================================================================== process liveness
def test_pid_alive_probes_without_killing_and_sees_the_exit():
    p = subprocess.Popen([sys.executable, "-c", "import sys; sys.stdin.read()"],
                         stdin=subprocess.PIPE)
    try:
        assert state.pid_alive(p.pid) is True
        time.sleep(0.3)
        assert p.poll() is None, "the liveness probe killed the process"
        assert state.pid_alive(p.pid) is True
    finally:
        p.stdin.close()
    assert p.wait(timeout=30) == 0
    assert state.pid_alive(p.pid) is False


@pytest.mark.parametrize("pid", [0, -1, None, 0x7FFFFFF0, 2**40])
def test_pid_alive_false_for_no_such_process(pid):
    assert state.pid_alive(pid) is False


def test_pid_alive_self():
    assert state.pid_alive(os.getpid()) is True


@pytest.mark.skipif(sys.platform != "win32", reason="Windows liveness branch")
def test_windows_liveness_never_calls_os_kill(monkeypatch):
    def boom(*a):
        raise AssertionError("os.kill on Windows calls TerminateProcess")
    monkeypatch.setattr(os, "kill", boom)
    p = subprocess.Popen([sys.executable, "-c", "import sys; sys.stdin.read()"],
                         stdin=subprocess.PIPE)
    try:
        assert state.pid_alive(p.pid) is True
        assert state.pid_alive(4) is True            # System: exists, access denied
    finally:
        p.stdin.close()
        p.wait(timeout=30)
    assert state.pid_alive(p.pid) is False


def test_posix_liveness_uses_signal_zero(monkeypatch):
    calls = []
    outcome = {}

    def fake_kill(pid, sig):
        calls.append((pid, sig))
        if outcome.get("raise"):
            raise outcome["raise"]
    monkeypatch.setattr(state, "_IS_WINDOWS", False)
    monkeypatch.setattr(os, "kill", fake_kill)
    assert state.pid_alive(12345) is True
    outcome["raise"] = ProcessLookupError()
    assert state.pid_alive(12345) is False
    outcome["raise"] = PermissionError()
    assert state.pid_alive(12345) is True
    assert all(sig == 0 for _, sig in calls) and len(calls) == 3


# ================================================================== key-value
def test_kv_round_trips_json_values_per_namespace():
    state.kv_set("a", "k", {"x": [1, 2], "y": None})
    state.kv_set("b", "k", "other")
    assert state.kv_get("a", "k") == {"x": [1, 2], "y": None}
    assert state.kv_get("b", "k") == "other"
    state.kv_set("a", "k", 3)
    assert state.kv_get("a", "k") == 3
    assert state.kv_get("a", "missing") is None
    assert state.kv_get("a", "missing", default=0) == 0


def test_kv_ttl_expires(clock):
    state.kv_set("c", "k", "v", ttl_s=10)
    clock.t += 9
    assert state.kv_get("c", "k") == "v"
    clock.t += 2
    assert state.kv_get("c", "k", default="gone") == "gone"


# ================================================================== state CLI
def test_cli_clear_refusal_and_status(capsys):
    state.refuse("export.arxiv.org", "HTTP 406", persistence="manual")
    assert state.main(["--status"]) == 0
    assert "REFUSED (manual): HTTP 406" in capsys.readouterr().out
    assert state.main(["--status", "--json"]) == 0
    st = json.loads(capsys.readouterr().out)
    assert st["hosts"][0]["refused"] == "manual"
    assert state.main(["--clear-refusal", "export.arxiv.org"]) == 0
    assert "refusal cleared" in capsys.readouterr().out
    assert not state.is_refused("export.arxiv.org")


def test_cli_does_not_create_state_it_would_only_read(isolated_state, capsys):
    assert state.main(["--status"]) == 0
    assert state.main(["--clear-refusal", "x.test"]) == 0
    assert "no state yet" in capsys.readouterr().out
    assert not isolated_state.parent.exists()


@pytest.mark.parametrize("mod", [state, preflight])
def test_help_touches_no_state(mod, monkeypatch):
    def forbidden(*a, **k):
        raise AssertionError("--help must not resolve the state dir")
    monkeypatch.setattr(config, "state_dir", forbidden)
    with pytest.raises(SystemExit) as e:
        mod.main(["--help"])
    assert e.value.code == 0


@pytest.mark.parametrize("module", ["litpipe.state", "litpipe.preflight"])
def test_help_exits_zero_as_a_module(module, tmp_path):
    env = {**os.environ, "USERPROFILE": str(tmp_path), "HOME": str(tmp_path)}
    p = subprocess.run([sys.executable, "-m", module, "--help"], cwd=REPO, env=env,
                       capture_output=True, text=True, timeout=60)
    assert p.returncode == 0, p.stderr
    assert "usage:" in p.stdout
    assert not (tmp_path / ".local").exists()


# ================================================================== preflight
EMAIL = "pipeline.owner@uconn.edu"
DOI = preflight.PREFLIGHT_DOI


def resp(status, headers=None, content=b"", kind=None):
    kind = kind or (Kind.OK if status == 200 else Kind.ERROR)
    return Outcome(kind, status=status, payload={"status": status, "headers": headers or {},
                                                 "content": content})


# Trimmed from the live probes of 2026-09-30 (notes/2026-09-29_update_evidence/W1-A2/).
UNPAYWALL_OK = resp(200, {"Content-Type": "application/json"}, json.dumps(
    {"doi": DOI, "is_oa": True, "oa_status": "gold", "updated": "2026-08-11T13:04:24Z"}).encode())
UNPAYWALL_422 = resp(422, {"Content-Type": "application/json"}, json.dumps(
    {"HTTP_status_code": 422, "error": True, "message": "Please use your own email address in "
     "API calls. See http://unpaywall.org/products/api"}).encode())
CROSSREF_POLITE = resp(200, {"x-rate-limit-limit": "10", "x-rate-limit-interval": "1s",
                             "x-concurrency-limit": "3", "x-api-pool": "polite-single"},
                       b'{"status":"ok","message-type":"work"}')
CROSSREF_PUBLIC = resp(200, {"x-rate-limit-limit": "5", "x-rate-limit-interval": "1s",
                             "x-concurrency-limit": "1", "x-api-pool": "public-single"},
                       b'{"status":"ok"}')


def fake_net(unpaywall=UNPAYWALL_OK, crossref=CROSSREF_POLITE):
    calls = []

    def request(method, url, **kw):
        calls.append((method, url, kw))
        out = unpaywall if url.startswith(preflight.UNPAYWALL_URL) else \
            crossref if url.startswith(preflight.CROSSREF_URL) else None
        assert out is not None, f"unexpected request {url}"
        if isinstance(out, Exception):
            raise out
        return out
    request.calls = calls
    return request


def by_check(outs):
    return {o.payload["check"]: o for o in outs}


@pytest.mark.parametrize("value", [
    None, "", "   ", "jacob", "@uconn.edu", "someone@", "some one@uconn.edu",
    "x@example.com", "X@EXAMPLE.COM", "x@mail.example.com", "x@example.org", "x@example.net",
    "x@lab.test", "x@foo.invalid", "x@localhost", "x@site.example", "x@example.com.",
])
def test_preflight_rejects_a_missing_or_placeholder_email(value):
    env = {} if value is None else {"LITPIPE_EMAIL": value}
    net = fake_net()
    outs = preflight.run(env=env, request=net)
    o = by_check(outs)
    assert o["email"].kind is Kind.CONFIG
    assert "\n" not in o["email"].detail and "setx LITPIPE_EMAIL" in o["email"].detail
    assert o["unpaywall"].kind is Kind.SKIPPED and o["crossref"].kind is Kind.SKIPPED
    assert net.calls == []                            # a placeholder is never sent
    assert preflight.exit_code(outs) == 2 and not preflight.ok(outs)


@pytest.mark.parametrize("value", ["pipeline.owner@uconn.edu", "someone@myexample.com",
                                   "a.b+lit@gmail.com", "x@example.com.au", " x@state.edu "])
def test_preflight_accepts_a_real_email(value):
    assert preflight.check_email({"LITPIPE_EMAIL": value}).kind is Kind.OK


def test_preflight_all_green():
    net = fake_net()
    outs = preflight.run(env={"LITPIPE_EMAIL": EMAIL}, request=net)
    assert [o.kind for o in outs] == [Kind.OK] * len(outs)
    assert preflight.exit_code(outs) == 0
    (m1, u1, kw1), (m2, u2, kw2) = net.calls
    # identity (email= for Unpaywall, mailto in the UA for Crossref) is litpipe.net's job
    assert (m1, u1) == ("GET", preflight.UNPAYWALL_URL) and "params" not in kw1
    assert (m2, u2) == ("GET", preflight.CROSSREF_URL) and "params" not in kw2
    assert EMAIL not in repr(net.calls)
    o = by_check(outs)
    assert o["crossref"].payload["pool"] == "polite-single"
    assert o["unpaywall"].status == 200


@pytest.mark.parametrize("unpaywall, kind", [
    (UNPAYWALL_422, Kind.CONFIG),
    (resp(410, {}, b'{"message": "gone"}'), Kind.CONFIG),
    (resp(200, {"Content-Type": "text/html"}, b"<html>maintenance</html>"), Kind.OUTAGE),
    (resp(200, {}, json.dumps({"doi": "10.1000/other"}).encode()), Kind.OUTAGE),
    (Outcome(Kind.TRANSPORT, detail="getaddrinfo failed"), Kind.TRANSPORT),
    (Outcome(Kind.REFUSED, status=403, detail="HTTP 403"), Kind.REFUSED),
    (Outcome(Kind.DEFERRED, detail="budget spent"), Kind.DEFERRED),
])
def test_preflight_unpaywall_failures(unpaywall, kind):
    outs = preflight.run(env={"LITPIPE_EMAIL": EMAIL}, request=fake_net(unpaywall=unpaywall))
    assert by_check(outs)["unpaywall"].kind is kind
    assert preflight.exit_code(outs) == (2 if kind is Kind.CONFIG else 1)


@pytest.mark.parametrize("crossref, kind", [
    (CROSSREF_PUBLIC, Kind.CONFIG),
    (resp(200, {"x-rate-limit-limit": "10"}, b"{}"), Kind.ERROR),
    (resp(200, {"X-Api-Pool": "Polite-Array"}, b"{}"), Kind.OK),
    (resp(200, {"x-api-pool": "plus"}, b"{}"), Kind.OK),
    (Outcome(Kind.OUTAGE, status=503, detail="HTTP 503"), Kind.OUTAGE),
])
def test_preflight_crossref_pool(crossref, kind):
    outs = preflight.run(env={"LITPIPE_EMAIL": EMAIL}, request=fake_net(crossref=crossref))
    assert by_check(outs)["crossref"].kind is kind


def test_preflight_survives_a_transport_that_raises_and_redacts_it():
    net = fake_net(unpaywall=RuntimeError(f"boom at https://x/?email={EMAIL}"))
    outs = preflight.run(env={"LITPIPE_EMAIL": EMAIL}, request=net)
    o = by_check(outs)["unpaywall"]
    assert o.kind is Kind.ERROR and "RuntimeError" in o.detail and EMAIL not in o.detail


SECRETS = {"S2_API_KEY": "s2-SECRET-9f8e7d6c5b", "OPENALEX_API_KEY": "oa-SECRET-1a2b3c4d5e"}


def secret_forms():
    return (*SECRETS.values(), EMAIL, EMAIL.replace("@", "%40"), EMAIL.replace("@", "%2540"))


def test_preflight_reports_keys_present_or_absent_never_the_value(capsys):
    env = {"LITPIPE_EMAIL": EMAIL, **SECRETS}
    outs = preflight.run(env=env, request=fake_net())
    o = by_check(outs)
    assert o["s2_api_key"].detail == "S2_API_KEY present" and o["s2_api_key"].payload["present"]
    assert o["openalex_api_key"].detail == "OPENALEX_API_KEY present"
    assert preflight.main([], env=env, request=fake_net()) == 0
    assert preflight.main(["--json"], env=env, request=fake_net()) == 0
    text = repr(outs) + capsys.readouterr().out
    for secret in secret_forms():
        assert secret not in text
    absent = by_check(preflight.run(env={"LITPIPE_EMAIL": EMAIL}, network=False))
    assert absent["s2_api_key"].detail == "S2_API_KEY absent"
    assert absent["openalex_api_key"].payload["present"] is False


def test_preflight_scrubs_a_secret_that_a_transport_echoed(capsys):
    env = {"LITPIPE_EMAIL": EMAIL, **SECRETS}
    leaky = Outcome(Kind.REFUSED, 403, "api.crossref.org",
                    f"HTTP 403 key {SECRETS['S2_API_KEY']} mailto:{EMAIL.replace('@', '%40')} "
                    f"loc {EMAIL.replace('@', '%2540')}")
    outs = preflight.run(env=env, request=fake_net(crossref=leaky))
    o = by_check(outs)["crossref"]
    assert o.kind is Kind.REFUSED and "<REDACTED>" in o.detail
    assert preflight.main([], env=env, request=fake_net(crossref=leaky)) == 1
    text = repr(outs) + capsys.readouterr().out
    for secret in secret_forms():
        assert secret not in text


def test_preflight_defers_while_another_writer_run_is_live(isolated_state):
    mine = state.register_run("runner")               # our own run never blocks us
    p = child("""
        print(state.register_run("sweep"), flush=True)
        state.register_run("status", writer=False)
        sys.stdin.read()
    """, isolated_state, stdin=True)
    try:
        other = p.stdout.readline().strip()
        o = by_check(preflight.run(env={"LITPIPE_EMAIL": EMAIL}, network=False))["writer"]
        assert o.kind is Kind.DEFERRED and other in o.detail and o.payload["runs"] == [other]
    finally:
        p.stdin.close()
        p.wait(timeout=30)
    outs = preflight.run(env={"LITPIPE_EMAIL": EMAIL}, network=False, own_run_id=mine)
    assert by_check(outs)["writer"].kind is Kind.OK
    assert preflight.exit_code(outs) == 0


def test_preflight_ignores_reader_runs(isolated_state):
    p = child("""
        print(state.register_run("canary", writer=False), flush=True)
        sys.stdin.read()
    """, isolated_state, stdin=True)
    try:
        p.stdout.readline()
        assert by_check(preflight.run(env={"LITPIPE_EMAIL": EMAIL},
                                      network=False))["writer"].kind is Kind.OK
    finally:
        p.stdin.close()
        p.wait(timeout=30)


def test_preflight_without_litpipe_net_is_an_error_not_a_pass(monkeypatch):
    monkeypatch.setattr(preflight, "_net_request", lambda: None)
    outs = preflight.run(env={"LITPIPE_EMAIL": EMAIL})
    o = by_check(outs)
    assert o["unpaywall"].kind is Kind.ERROR and o["crossref"].kind is Kind.ERROR
    assert preflight.exit_code(outs) == 1


def test_preflight_cli_config_exit_code(capsys):
    assert preflight.main(["--no-network"], env={"LITPIPE_EMAIL": "me@example.com"}) == 2
    out = capsys.readouterr().out
    assert "CONFIG" in out and "STOP" in out and "me@example.com" not in out


# ---------------------------------------------------------------- W1 integration (dispatcher)
def test_ledger_run_id_follows_the_registered_state_run(monkeypatch):
    """ledger.current_run_id() used to see only set_run_id / LITPIPE_RUN_ID, so a runner that
    registered its run in state wrote ledger lines with no run id and keyed net's consecutive-403
    counter under '-' while the refusal belonged to the state run (W1-A2 forward)."""
    from litpipe import ledger
    monkeypatch.setattr(ledger, "RUN_ID", None)
    assert ledger.current_run_id() is None
    r = state.register_run("runner")
    assert ledger.current_run_id() == r
    monkeypatch.setattr(ledger, "RUN_ID", "explicit")      # set_run_id still wins
    assert ledger.current_run_id() == "explicit"


def test_state_redaction_is_the_ledger_implementation():
    """One redaction implementation: state no longer carries its own fallback."""
    from litpipe import ledger
    s = "https://x.test/v2/10.1/a?email=jane.doe%40uni.edu&api_key=k1"
    assert state._redact(s) == ledger.redact(s)
    assert not hasattr(state, "_fallback_redact")
