"""The runner as real processes (amendment 16): only through tests/fixtures/W4-A/run_runner.py
(temp registry, loopback-only transports, stubbed preflight and canaries). Each stage runs in a real
child through the shim; the children run the stand-in modules, which send nothing.

Covered here: two runners started on a barrier; a real child's crash, timeout (its grandchild killed
with it), missing result, malformed result, result without the table's keys, a traceback after OK,
and 50,000 lines of undecodable output; a lost heartbeat killing the running child's tree; a run
refusal made by a child holding for later projects; a `runner batch` killed mid-sweep, resumed."""
import json
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pytest

FIX = Path(__file__).resolve().parent / "fixtures" / "W4-A"
sys.path.insert(0, str(FIX))

from litpipe import runner, state, worklists  # noqa: E402
from w4a_world import World, stand_in_overrides  # noqa: E402

S2 = "api.semanticscholar.org"


@pytest.fixture
def w(tmp_path, monkeypatch):
    world = World(tmp_path, monkeypatch)
    world.overrides(**stand_in_overrides())
    return world


def wait_for(pred, timeout=60, step=0.05):
    end = time.time() + timeout
    while time.time() < end:
        if pred():
            return True
        time.sleep(step)
    return False


def pid_of(path):
    return int(Path(path).read_text(encoding="utf-8"))


def last_summary(w):
    s = w.summaries()
    assert s, w.harness_log()
    return s[-1]


def jobs(s, name):
    return {j["project"]: j for j in s["jobs"] if j["job"] == name}


# ================================================================ acceptance 4: a second runner
def test_two_runners_started_on_a_barrier_one_refuses_with_exit_3(w):
    w.register("research_a")
    w.queue("research_a", ["10.5555/w4a.0001"])
    w.set_fake("sweep", "research_a", sleep=6)                  # the winner stays live while the loser checks
    barrier = w.tmp / "barrier"
    env = {"LITPIPE_TEST_BARRIER": str(barrier)}
    procs = [w.spawn("run", "--profile", "daily", env=env) for _ in range(2)]
    try:
        assert wait_for(lambda: len(list(w.tmp.glob("barrier.*.ready"))) == 2, timeout=90), w.harness_log()
        barrier.write_text("go", encoding="utf-8")
        codes = sorted(p.wait(timeout=120) for p in procs)
    finally:
        for p in procs:
            if p.poll() is None:
                p.kill()
    assert codes == [0, 3], w.harness_log()
    runs = [r for r in w.runs() if r["kind"] == "runner"]
    assert len(runs) == 2
    loser = next(r for r in runs if r["status"].startswith("refused"))
    winner = next(r for r in runs if r is not loser)
    assert loser["status"] == f"refused: runner {winner['run_id']} is live" and winner["status"] == "ok"
    assert len(w.calls("sweep")) == 1                         # the loser swept nothing
    assert w.ledger_arxiv_lines() == 0


# ================================================================ acceptance 6: failed children
def test_a_real_childs_timeout_kills_its_tree_and_the_next_project_still_runs(w):
    gc, child = w.tmp / "grandchild.pid", w.tmp / "child.pid"
    w.register("p1_slow", walk_cadence_days=1)
    w.register("p2_next", walk_cadence_days=1)
    w.set_fake("walk", "p1_slow", sleep=60, pid_file=str(child), grandchild=str(gc))
    w.overrides(**stand_in_overrides(), TIMEOUTS={"walk": 4})
    code = w.harness("run", "--profile", "daily", timeout=180)
    assert code == 2, w.harness_log()
    j = jobs(last_summary(w), "walk")
    assert j["p1_slow"]["status"] == "ERROR" and j["p1_slow"]["reason"] == "timeout after 4 s"
    assert j["p1_slow"]["counts"]["killed_by"] in ("job object", "taskkill /T /F", "process group")
    assert 4 <= j["p1_slow"]["elapsed_s"] < 30
    assert not state.pid_alive(pid_of(child)) and not state.pid_alive(pid_of(gc))     # the tree died
    assert j["p2_next"]["status"] == "OK"


def test_failed_children_are_recorded_by_the_table_and_the_next_project_still_runs(w):
    behaviours = {
        "p1_crash": {"mode": "crash"},
        "p3_noresult": {"mode": "no_result"},
        "p4_malformed": {"mode": "malformed"},
        "p5_nokeys": {"mode": "no_keys"},
        "p6_traceback": {"traceback": True},
        "p7_flood": {"flood": 50000},
        "p8_ok": {},
    }
    for key, spec in behaviours.items():
        w.register(key, walk_cadence_days=1)
        w.set_fake("walk", key, **spec)
    t0 = time.time()
    code = w.harness("run", "--profile", "daily", timeout=240)
    assert code == 2, w.harness_log()
    s = last_summary(w)
    j = jobs(s, "walk")
    assert j["p1_crash"]["status"] == "ERROR" and "shim exit 1" in j["p1_crash"]["reason"]
    assert "RuntimeError: stand-in crash" in Path(j["p1_crash"]["log"]).read_text(encoding="utf-8")
    assert j["p3_noresult"]["status"] == "ERROR" and "no result file" in j["p3_noresult"]["reason"]
    assert j["p4_malformed"]["status"] == "ERROR" and "malformed result file" in j["p4_malformed"]["reason"]
    assert j["p5_nokeys"]["status"] == "ERROR" and "result lacks exit_code" in j["p5_nokeys"]["reason"]
    assert j["p6_traceback"]["status"] == "DEGRADED" and j["p6_traceback"]["reason"] == "traceback in the stage log"
    assert j["p7_flood"]["status"] == "OK" and j["p7_flood"]["counts"]["log_lines"] >= 50000
    flood_log = Path(j["p7_flood"]["log"]).read_text(encoding="utf-8")
    assert "line 49999�" in flood_log            # undecodable bytes read, replaced, never a crash
    assert j["p8_ok"]["status"] == "OK"
    assert [c["project"] for c in w.calls("walk")] == [k for k in behaviours]
    assert time.time() - t0 < 200
    runs = [r for r in w.runs() if r["kind"] == "runner"]
    assert runs[-1]["status"] == "completed with failures"
    assert "[step-summary] " in w.harness_log().strip().splitlines()[-1]


# ================================================================ acceptance 7: the heartbeat lost
def test_a_lost_heartbeat_kills_the_running_child_and_exits_3(w):
    gc, child = w.tmp / "grandchild.pid", w.tmp / "child.pid"
    w.register("research_a", walk_cadence_days=1)
    w.register("research_b", walk_cadence_days=1)
    w.set_fake("walk", "research_a", sleep=90, pid_file=str(child), grandchild=str(gc))
    w.overrides(**stand_in_overrides(), HEARTBEAT_EVERY_S=0.3, HEARTBEAT_SLICE_S=0.1)
    p = w.spawn("run", "--profile", "daily")
    try:
        assert wait_for(lambda: gc.exists() and child.exists(), timeout=90), w.harness_log()
        con = sqlite3.connect(str(w.state_dir / state.DB_NAME), timeout=30)
        try:                              # what another process's _mark_abandoned does to a stale run
            con.execute("UPDATE runs SET finished=?, status='abandoned' WHERE kind='runner' AND finished IS NULL",
                        (time.time(),))
            con.commit()
        finally:
            con.close()
        t = time.time()
        code = p.wait(timeout=60)
        took = time.time() - t
    finally:
        if p.poll() is None:
            p.kill()
    assert code == 3, w.harness_log()
    assert took < 30
    assert not state.pid_alive(pid_of(child)) and not state.pid_alive(pid_of(gc))
    s = last_summary(w)
    assert jobs(s, "walk")["research_a"]["status"] == "ABORTED"
    assert s["aborted"].startswith("heartbeat lost") and "research_b" not in jobs(s, "walk")
    assert [c["project"] for c in w.calls("walk")] == ["research_a"]


# ================================================================ acceptance 7: a child's refusal
def test_a_run_refusal_made_by_a_child_defers_later_walks_and_ends_at_finish(w, monkeypatch):
    for k in ("research_a", "research_b", "research_c"):
        w.register(k, walk_cadence_days=1)
    w.set_fake("walk", "research_a", refuse=[S2], exit_code=3, aborted="S2 final 429")
    assert w.harness("run", "--profile", "daily") == 2, w.harness_log()
    s = last_summary(w)
    j = jobs(s, "walk")
    assert j["research_b"]["status"] == "DEFERRED" and j["research_c"]["status"] == "DEFERRED"
    held = [h for h in s["hosts_at_end"]["refused"] if h["host"] == S2]
    assert held and held[0]["refused"] == "run"
    assert w.calls("walk")[0]["run_env"] == s["run_id"]      # the child joined the run
    assert not state.is_refused(S2)                          # ended with the run


def kill_runner(p):
    """Kill a harness runner the way the platform does: Windows TerminateProcess on the tree (the
    stage child's job object closes with the runner); POSIX SIGTERM (what systemd sends; the runner
    kills its child's process group before it exits)."""
    if sys.platform == "win32":
        subprocess.run(["taskkill", "/T", "/F", "/PID", str(p.pid)], capture_output=True, timeout=60)
    else:
        import signal
        os.kill(p.pid, signal.SIGTERM)
    return p.wait(timeout=60)


# ================================================================ acceptance 3: a batch killed mid-sweep
def test_a_batch_killed_mid_sweep_resumes_and_sweeps_only_the_surviving_file_again(w, monkeypatch):
    w.register("teaching_a")
    pool = w.proot("teaching_a") / "lit_pull_queue.unit3_pool.csv"
    dois = [f"10.5555/pool.{i:04d}" for i in range(1, 7)]
    pool.write_bytes(("doi,title,authors,year\n" + "".join(f"{d},T {i},A,2021\n" for i, d in enumerate(dois)))
                     .encode("utf-8"))
    marker, child = w.tmp / "sweep.started", w.tmp / "sweep.pid"
    w.set_fake("sweep", "teaching_a", sleep=90, started_marker=str(marker), pid_file=str(child))
    p = w.spawn("batch", "--project", "teaching_a", "--pool", str(pool), "--size", "2", "--batches", "3")
    try:
        assert wait_for(marker.exists, timeout=90), w.harness_log()
        kill_runner(p)
    finally:
        if p.poll() is None:
            p.kill()
    assert wait_for(lambda: not state.pid_alive(pid_of(child)), timeout=30)   # the child died with the runner
    bfile = w.proot("teaching_a") / "lit_pull_queue.b-unit3_pool.csv"
    assert bfile.is_file()                                                   # the file survived the kill
    cfg = json.loads(w.cfg_path.read_text(encoding="utf-8"))
    assert worklists.Pool(pool, registry=cfg).status()["pending"] == 2
    w.set_fake("sweep", "teaching_a")
    marks = []
    real = worklists.Pool.mark_swept
    monkeypatch.setattr(worklists.Pool, "mark_swept",
                        lambda self, d, r, c: (marks.append(list(d)), real(self, d, r, c))[1])
    assert runner.main(["batch", "--project", "teaching_a", "--pool", str(pool), "--size", "2", "--batches", "3"]) == 0
    flat = [d for m in marks for d in m]
    assert sorted(flat) == sorted(dois) and len(flat) == len(set(flat))      # once per DOI, none lost
    sweeps = w.calls("sweep")
    assert len(sweeps) == 4                                                  # batch 1 twice (its file survived)
    st = worklists.Pool(pool, registry=cfg).status()
    assert (st["swept"], st["pending"], st["remaining"]) == (6, 0, 0)


# ================================================================ the process tree, per platform
TREE = ("import subprocess, sys, time, signal, os\n"
        "{prelude}"
        "g = subprocess.Popen([sys.executable, '-c', 'import time, signal, os\\n{gprelude}time.sleep(600)'])\n"
        "open(sys.argv[1], 'w', encoding='utf-8').write(str(os.getpid()) + ' ' + str(g.pid))\n"
        "time.sleep({sleep})\n")


def _tree(tmp_path, *, ignore_term=False, sleep=600):
    pids = tmp_path / "pids.txt"
    pre = "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n" if ignore_term else ""
    gpre = "signal.signal(signal.SIGTERM, signal.SIG_IGN)\\n" if ignore_term else ""
    code = TREE.format(prelude=pre, gprelude=gpre, sleep=sleep)
    t = runner.ProcessTree.start([sys.executable, "-c", code, str(pids)], stdin=subprocess.DEVNULL,
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    assert wait_for(lambda: pids.exists() and len(pids.read_text().split()) == 2, timeout=60)
    child, grand = (int(x) for x in pids.read_text().split())
    return t, child, grand


@pytest.mark.skipif(sys.platform != "win32", reason="the Windows path: a job object")
def test_windows_tree_kill_uses_the_job_object_and_takes_the_grandchild(tmp_path):
    t, child, grand = _tree(tmp_path)
    assert t.job is not None
    assert t.kill() == "job object"
    assert wait_for(lambda: not state.pid_alive(child) and not state.pid_alive(grand), timeout=30)
    t.close()


@pytest.mark.skipif(sys.platform != "win32", reason="the Windows path: a job object")
def test_windows_closing_the_job_kills_what_outlived_the_child(tmp_path):
    t, child, grand = _tree(tmp_path, sleep=0)
    t.proc.wait(timeout=60)                              # the child ended; its grandchild still sleeps
    assert state.pid_alive(grand)
    t.close()
    assert wait_for(lambda: not state.pid_alive(grand), timeout=30)


@pytest.mark.skipif(sys.platform == "win32", reason="the POSIX path: a process group")
def test_posix_tree_kill_sends_sigterm_then_sigkill_to_the_group(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, "KILL_GRACE_S", 1.0)
    t, child, grand = _tree(tmp_path, ignore_term=True)
    assert os.getpgid(child) == child and os.getpgid(grand) == child          # one new session and group
    assert t.kill() == "process group (SIGTERM, then SIGKILL)"
    assert wait_for(lambda: not state.pid_alive(child) and not state.pid_alive(grand), timeout=30)
    t.close()


@pytest.mark.skipif(sys.platform == "win32", reason="the POSIX path: a process group")
def test_posix_sigterm_alone_is_enough_for_a_cooperative_tree(tmp_path):
    t, child, grand = _tree(tmp_path)
    assert t.kill().startswith("process group (SIGTERM")
    assert wait_for(lambda: not state.pid_alive(child) and not state.pid_alive(grand), timeout=30)


@pytest.mark.skipif(sys.platform == "win32", reason="the POSIX path: a process group")
def test_posix_closing_kills_what_outlived_the_child(tmp_path):
    t, child, grand = _tree(tmp_path, sleep=0)
    t.proc.wait(timeout=60)
    assert state.pid_alive(grand)
    t.close()
    assert wait_for(lambda: not state.pid_alive(grand), timeout=30)


@pytest.mark.skipif(sys.platform == "win32", reason="SIGTERM is POSIX's stop signal (systemd)")
def test_posix_sigterm_to_the_runner_kills_the_child_tree_and_exits_3(w):
    gc, child = w.tmp / "grandchild.pid", w.tmp / "child.pid"
    w.register("research_a", walk_cadence_days=1)
    w.set_fake("walk", "research_a", sleep=90, pid_file=str(child), grandchild=str(gc))
    p = w.spawn("run", "--profile", "daily")
    try:
        assert wait_for(lambda: gc.exists() and child.exists(), timeout=90), w.harness_log()
        assert kill_runner(p) == 3, w.harness_log()
    finally:
        if p.poll() is None:
            p.kill()
    assert wait_for(lambda: not state.pid_alive(pid_of(child)) and not state.pid_alive(pid_of(gc)), timeout=30)
    s = last_summary(w)
    assert s["aborted"] == "terminated (SIGTERM)" and jobs(s, "walk")["research_a"]["status"] == "ABORTED"


# ---------------------------------------------------------------- the platform logic, on any platform
class _FakeProc:
    pid = 4242

    def __init__(self, exits_on_term):
        self.exits_on_term = exits_on_term
        self.calls = []

    def wait(self, timeout=None):
        self.calls.append(("wait", timeout))
        if not self.exits_on_term and len([c for c in self.calls if c[0] == "wait"]) == 1:
            raise subprocess.TimeoutExpired("x", timeout)
        return -15

    def kill(self):
        self.calls.append(("kill",))


@pytest.mark.parametrize("exits_on_term,lingering,want", [
    (True, False, "process group (SIGTERM)"),
    (False, True, "process group (SIGTERM, then SIGKILL)"),
])
def test_posix_kill_logic(monkeypatch, exits_on_term, lingering, want):
    import signal
    sent = []

    def killpg(pgid, sig):
        sent.append((pgid, sig))
        if sig == getattr(signal, "SIGKILL", 9) and not lingering:
            raise ProcessLookupError
    monkeypatch.setattr(runner.ProcessTree, "POSIX", True)
    monkeypatch.setattr(runner.os, "killpg", killpg, raising=False)
    monkeypatch.setattr(runner, "KILL_GRACE_S", 0.01)
    proc = _FakeProc(exits_on_term)
    assert runner.ProcessTree(proc).kill() == want
    assert sent[0] == (4242, getattr(signal, "SIGTERM", 15))
    assert sent[1] == (4242, getattr(signal, "SIGKILL", 9))           # the group's leftovers, after the grace
    assert ("wait", 0.01) in proc.calls


def test_windows_kill_logic_falls_back_to_taskkill_without_a_job(monkeypatch):
    ran = []
    monkeypatch.setattr(runner.ProcessTree, "POSIX", False)
    monkeypatch.setattr(runner.subprocess, "run", lambda cmd, **k: ran.append(cmd))
    assert runner.ProcessTree(_FakeProc(True)).kill() == "taskkill /T /F"
    assert ran == [["taskkill", "/T", "/F", "/PID", "4242"]]

    class Job:
        def terminate(self):
            return True
    ran.clear()
    assert runner.ProcessTree(_FakeProc(True), Job()).kill() == "job object" and ran == []
