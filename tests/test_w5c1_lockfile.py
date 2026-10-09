"""W5-C1 item 3: the per-project sweep lock FILE (litpipe.lockfile), and sweep, the runner and
runner batch under it. Every lock lives in tmp_path; the races use real processes."""
import fnmatch
import json
import multiprocessing
import os
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from litpipe import lockfile

FIX = Path(__file__).parent / "fixtures" / "W4-A"
REPO = Path(__file__).resolve().parent.parent


def write_record(root, **over):
    now = time.time()
    rec = {"host": "other-host.example", "pid": 4242, "run_id": "other-run", "started": lockfile._iso(now),
           "heartbeat": lockfile._iso(now), "tool": "sweep", **over}
    (Path(root) / lockfile.LOCK_NAME).write_text(json.dumps(rec), encoding="utf-8")
    return rec


def dead_pid():
    p = subprocess.Popen([sys.executable, "-c", "pass"])
    p.wait()
    return p.pid


# ================================================================ the file and its name
def test_the_lock_name_is_never_a_queue(tmp_path):
    import sweep
    for name in (lockfile.LOCK_NAME, lockfile.LOCK_NAME + ".stale-0a1b2c", lockfile.LOCK_NAME + ".beat-0a1b.tmp"):
        (tmp_path / name).write_text("{}", encoding="utf-8")
        assert not fnmatch.fnmatch(name, "lit_pull_queue*.csv")       # sweep's discovery glob
        assert sweep.queue_tag(name) is None and sweep.parse_artifact(name) is None
    # the name is not lit_pull_queue.<a valid tag>.csv: "lock" would be a valid tag, the suffix is not .csv
    assert sweep.is_valid_tag("lock") and lockfile.LOCK_NAME != "lit_pull_queue.lock.csv"
    assert sweep.discover_queues(tmp_path) == []


def test_take_writes_the_record_and_release_deletes_it(tmp_path):
    lk = lockfile.Lock(tmp_path, tool="sweep", run_id="run-1", heartbeat=False).acquire()
    rec = lockfile.read(tmp_path)
    assert set(rec) == set(lockfile.FIELDS)
    assert rec["host"] == socket.gethostname() and rec["pid"] == os.getpid() and rec["run_id"] == "run-1"
    assert rec["tool"] == "sweep" and lockfile._epoch(rec["heartbeat"]) and lockfile._epoch(rec["started"])
    assert lk.held and not lk.joined
    assert lk.release() is True and lockfile.read(tmp_path) is None


def test_read_has_no_side_effect(tmp_path):
    assert lockfile.read(tmp_path) is None and list(tmp_path.iterdir()) == []
    write_record(tmp_path)
    p = tmp_path / lockfile.LOCK_NAME
    before = (p.read_bytes(), p.stat().st_mtime_ns)
    assert lockfile.read(tmp_path)["host"] == "other-host.example"
    assert (p.read_bytes(), p.stat().st_mtime_ns) == before and sorted(x.name for x in tmp_path.iterdir()) == [p.name]
    p.write_text("{not json", encoding="utf-8")
    assert lockfile.read(tmp_path)["unreadable"] is True


def test_a_lock_from_another_host_is_honoured_until_stale(tmp_path, monkeypatch):
    rec = write_record(tmp_path, tool="runner")
    with pytest.raises(lockfile.LockHeld) as e:
        lockfile.Lock(tmp_path, tool="sweep", heartbeat=False).acquire()
    msg = str(e.value)
    assert "other-host.example" in msg and "runner" in msg and "heartbeat" in msg and " ago" in msg
    assert lockfile.read(tmp_path) == rec                         # a live lock is never broken
    t0 = lockfile._epoch(rec["heartbeat"])
    monkeypatch.setattr(lockfile, "_time", lambda: t0 + lockfile.STALE_S - 60)
    with pytest.raises(lockfile.LockHeld):
        lockfile.Lock(tmp_path, tool="sweep", heartbeat=False).acquire()
    monkeypatch.setattr(lockfile, "_time", lambda: t0 + lockfile.STALE_S + 1)
    lk = lockfile.Lock(tmp_path, tool="sweep", run_id="mine", heartbeat=False).acquire()
    assert lk.broke["run_id"] == "other-run" and lockfile.read(tmp_path)["run_id"] == "mine"
    assert sorted(p.name for p in tmp_path.iterdir()) == [lockfile.LOCK_NAME]   # no tombstone left
    lk.release()


def test_a_dead_local_pid_is_reclaimed_and_a_live_one_is_not(tmp_path):
    write_record(tmp_path, host=socket.gethostname(), pid=dead_pid())
    lk = lockfile.Lock(tmp_path, tool="sweep", heartbeat=False).acquire()
    assert lk.broke is not None
    lk.release()
    alive = subprocess.Popen([sys.executable, "-c", "import sys; sys.stdin.read()"], stdin=subprocess.PIPE)
    try:
        write_record(tmp_path, host=socket.gethostname(), pid=alive.pid)
        with pytest.raises(lockfile.LockHeld):
            lockfile.Lock(tmp_path, tool="sweep", heartbeat=False).acquire()
    finally:
        alive.communicate(b"")


def test_a_crash_leaves_the_lock_to_go_stale_never_forever(tmp_path, monkeypatch):
    """A process that dies holding the lock leaves the file; on its own host the dead pid frees it at
    once, from another host its heartbeat goes stale after STALE_S."""
    code = ("import os, sys; sys.path.insert(0, sys.argv[1]); from litpipe import lockfile; "
            "lockfile.Lock(sys.argv[2], tool='sweep', run_id='crashed', heartbeat=False).acquire(); os._exit(1)")
    assert subprocess.run([sys.executable, "-c", code, str(REPO), str(tmp_path)]).returncode == 1
    rec = lockfile.read(tmp_path)
    assert rec["run_id"] == "crashed"                               # left behind by the crash
    (tmp_path / lockfile.LOCK_NAME).write_text(json.dumps({**rec, "host": "elsewhere"}), encoding="utf-8")
    with pytest.raises(lockfile.LockHeld):                         # another host's lock: honoured
        lockfile.Lock(tmp_path, tool="sweep", heartbeat=False).acquire()
    t0 = lockfile._epoch(rec["heartbeat"])
    monkeypatch.setattr(lockfile, "_time", lambda: t0 + lockfile.STALE_S)
    lockfile.Lock(tmp_path, tool="sweep", heartbeat=False).acquire().release()
    (tmp_path / lockfile.LOCK_NAME).write_text(json.dumps(rec), encoding="utf-8")
    monkeypatch.setattr(lockfile, "_time", time.time)
    lk = lockfile.Lock(tmp_path, tool="sweep", heartbeat=False).acquire()   # this host, dead pid
    assert lk.broke["run_id"] == "crashed"
    lk.release()


def _race(tmp_path, monkeypatch, n=2):
    monkeypatch.syspath_prepend(str(FIX))
    import w5c1_lock_worker
    ctx = multiprocessing.get_context("spawn")
    barrier, out = ctx.Barrier(n), ctx.Queue()
    procs = [ctx.Process(target=w5c1_lock_worker.race, args=(str(tmp_path), barrier, out, f"racer-{i}"))
             for i in range(n)]
    for p in procs:
        p.start()
    got = [out.get(timeout=120) for _ in procs]
    for p in procs:
        p.join(timeout=120)
        assert p.exitcode == 0
    return got


def test_two_processes_race_for_one_lock_and_one_wins(tmp_path, monkeypatch):
    got = _race(tmp_path, monkeypatch)
    took = [g for g in got if g[0] == "took"]
    held = [g for g in got if g[0] == "held"]
    assert len(took) == 1 and len(held) == 1
    assert held[0][2] == took[0][1]                                 # the loser saw the winner's lock
    assert lockfile.read(tmp_path) is None


def test_two_processes_race_to_break_one_stale_lock_and_one_wins(tmp_path, monkeypatch):
    old = time.time() - lockfile.STALE_S - 600
    write_record(tmp_path, heartbeat=lockfile._iso(old), started=lockfile._iso(old))
    got = _race(tmp_path, monkeypatch)
    took = [g for g in got if g[0] == "took"]
    held = [g for g in got if g[0] == "held"]
    assert len(took) == 1 and len(held) == 1, got
    # the stale record was broken (by the winner, or by the other just before the winner's create won)
    assert took[0][2] in ("other-run", None)
    assert held[0][2] == took[0][1]                                 # the other saw the winner's live lock
    assert not [p for p in tmp_path.iterdir() if lockfile.TOMB_INFIX in p.name]


# ================================================================ heartbeat, loss, joining
def test_a_heartbeat_after_a_takeover_detects_the_loss(tmp_path):
    lk = lockfile.Lock(tmp_path, tool="sweep", run_id="mine", stale_s=4.0).acquire()   # a beat every 0.5 s
    try:
        deadline = time.time() + 5
        while lk.beats < 1 and time.time() < deadline:
            time.sleep(0.02)
        assert lk.beats >= 1 and not lk.lost.is_set()                # it beats while it holds the lock
        theirs = write_record(tmp_path, run_id="taker")             # another process took it over
        assert lk.lost.wait(5)
        assert "another" in lk.lost_reason
    finally:
        assert lk.release() is False                                # never deletes the taker's lock
    assert lockfile.read(tmp_path) == theirs


def test_a_lapsed_own_heartbeat_reads_as_lost_and_writes_nothing(tmp_path, monkeypatch):
    lk = lockfile.Lock(tmp_path, tool="sweep", run_id="mine", stale_s=100, heartbeat=False).acquire()
    before = (tmp_path / lockfile.LOCK_NAME).read_bytes()
    monkeypatch.setattr(lockfile, "_time", lambda: lk.last_beat + 101)     # the machine slept past STALE_S
    assert lk.beat() is False and lk.lost.is_set() and "lapsed" in lk.lost_reason
    assert (tmp_path / lockfile.LOCK_NAME).read_bytes() == before


def test_the_same_run_id_on_this_host_joins_and_never_releases(tmp_path, monkeypatch):
    taker = lockfile.Lock(tmp_path, tool="runner", run_id="20261007T010000Z-runner-1-abc", heartbeat=False).acquire()
    # a stage child of that run: another process, LITPIPE_RUN_ID set by the runner's shim
    rec = dict(lockfile.read(tmp_path), pid=dead_pid())
    (tmp_path / lockfile.LOCK_NAME).write_text(json.dumps(rec), encoding="utf-8")
    monkeypatch.setenv("LITPIPE_RUN_ID", "20261007T010000Z-runner-1-abc")
    child = lockfile.Lock(tmp_path, tool="sweep").acquire()
    assert child.joined and not child.held and child._thread is None
    assert child.release() is False and lockfile.read(tmp_path) == rec
    monkeypatch.setenv("LITPIPE_RUN_ID", "another-run")
    taker.release()


def test_the_lock_this_process_holds_is_joined(tmp_path, monkeypatch):
    monkeypatch.delenv("LITPIPE_RUN_ID", raising=False)
    taker = lockfile.Lock(tmp_path, tool="runner", run_id="r", heartbeat=False).acquire()
    inner = lockfile.Lock(tmp_path, tool="sweep").acquire()            # the in-process stage launcher
    assert inner.joined and inner.release() is False and lockfile.read(tmp_path)["tool"] == "runner"
    assert taker.release() is True


def test_lock_stale_s_comes_from_the_runner_block():
    from litpipe import config
    assert lockfile.stale_s_from({}) == lockfile.STALE_S == 7200.0
    assert lockfile.stale_s_from({"runner": {"lock_stale_s": 90}}) == 90.0
    for bad in (0, -1, "2h", True):
        with pytest.raises(config.ConfigError):
            lockfile.stale_s_from({"runner": {"lock_stale_s": bad}})


# ================================================================ sweep under the lock
@pytest.fixture
def env(tmp_path, monkeypatch):
    from tests.test_sweep_runs import Env
    return Env(tmp_path, monkeypatch)


def test_sweep_holds_the_lock_while_it_works_and_releases_it(env):
    import sweep
    seen = []
    real = env.stages.__call__

    def spy(cmd, **kw):
        seen.append(lockfile.read(env.pdir()))
        return real(cmd, **kw)
    env.stages.__class__ = type("Spy", (env.stages.__class__,), {"__call__": lambda self, c, **k: spy(c, **k)})
    env.queue(["10.1000/a1"])
    assert env.sweep() == 0
    assert seen and all(r and r["tool"] == "sweep" and r["pid"] == os.getpid() for r in seen)
    assert lockfile.read(env.pdir()) is None
    assert sweep.discover_queues(env.pdir()) == []


def test_a_held_lock_skips_the_project_before_admission_with_exit_4(env, capsys):
    import sweep
    q = env.queue(["10.1000/a1"])
    rl = env.pdir() / sweep.RETRY_LATER_FILE
    rl.write_text("doi,title,authors,year,destination,notes,not_before\n10.1000/due9,T,A,2020,lit,,2026-01-01\n",
                  encoding="utf-8")
    before = {p.name: p.read_bytes() for p in env.pdir().iterdir() if p.is_file()}
    write_record(env.pdir(), tool="runner")
    assert env.sweep() == sweep.EXIT_QUEUE_REFUSED
    err = capsys.readouterr().err
    assert "other-host.example" in err and "runner" in err and "heartbeat" in err
    assert env.stages.calls == []
    after = {p.name: p.read_bytes() for p in env.pdir().iterdir() if p.is_file() and p.name != lockfile.LOCK_NAME}
    assert after == before and q.exists()                          # nothing admitted, nothing touched
    assert env.loose_lines() == []


def test_a_dry_run_takes_no_lock_and_writes_nothing(env, capsys):
    env.queue(["10.1000/a1"])
    snap = lambda: {p.name: (p.stat().st_mtime_ns, p.stat().st_size) for p in env.pdir().rglob("*")}
    before = snap()
    assert env.sweep("--dry-run") == 0
    assert snap() == before and lockfile.read(env.pdir()) is None
    write_record(env.pdir())
    before = snap()
    assert env.sweep("--dry-run") == 0
    assert snap() == before and "a real sweep would skip this project" in capsys.readouterr().out


def test_a_lost_lock_stops_the_project_as_an_abort(env):
    import sweep
    env.register({"P": {"lib_dir": "lit"}}, loose_ends=True)
    cfg = json.loads(env.cfg_path.read_text(encoding="utf-8"))
    cfg["runner"] = {"lock_stale_s": 0.8}
    env.cfg_path.write_text(json.dumps(cfg), encoding="utf-8")
    real = env.stages.__call__

    def taken_over(cmd, **kw):
        r = real(cmd, **kw)
        if any(str(c).endswith("unpaywall_fetch_v2.py") for c in cmd):
            write_record(env.pdir(), run_id="usurper")              # another process takes the project
            time.sleep(1.5)                                         # the heartbeat sees it
        return r
    env.stages.__class__ = type("Spy", (env.stages.__class__,), {"__call__": lambda self, c, **k: taken_over(c, **k)})
    q = env.queue(["10.1000/a1"])
    assert env.sweep("--migrate") == sweep.EXIT_STAGE_FAILED
    assert env.stages.stages_called() == ["unpaywall"]              # PMC, preprint, extract, migrate: not run
    assert q.exists() and env.loose_lines() == []
    rep = env.report()
    assert rep[("queue", "kept")]["detail"].startswith("project lock lost")
    assert lockfile.read(env.pdir())["run_id"] == "usurper"         # the taker's lock is left alone


# ================================================================ the runner and runner batch
@pytest.fixture
def w(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(FIX))
    from w4a_world import World
    return World(tmp_path, monkeypatch)


DOIS = ["10.5555/lock.0001", "10.5555/lock.0002"]


def test_the_runner_defers_a_project_whose_lock_is_held_and_goes_on(w):
    from litpipe import runner
    w.register("research_a")
    w.register("research_b")
    w.queue("research_a", DOIS)
    w.queue("research_b", DOIS)
    write_record(w.proot("research_a"), tool="sweep", host="laptop.example")
    assert runner.main(["run", "--profile", "every_run"]) == 0
    s = w.summaries()[-1]
    lock = [j for j in s["jobs"] if j["job"] == "lock"]
    assert len(lock) == 1 and lock[0]["project"] == "research_a" and lock[0]["status"] == "DEFERRED"
    assert lock[0]["counts_for_exit"] is False and "laptop.example" in lock[0]["reason"]
    assert [c["project"] for c in w.calls("sweep")] == ["research_b"]
    assert (w.proot("research_a") / "lit_pull_queue.csv").exists()
    assert lockfile.read(w.proot("research_b")) is None                    # released after the route


def test_the_runner_takes_the_lock_with_its_run_id(w, monkeypatch):
    from litpipe import runner
    taken = []
    real = lockfile.Lock

    class Spy(real):
        def acquire(self):
            out = super().acquire()
            taken.append((self.root.name, self.tool, self.run_id, out.held))
            return out
    monkeypatch.setattr(runner.lockfile, "Lock", Spy)
    w.register("research_a")
    w.queue("research_a", DOIS)
    assert runner.main(["run", "--profile", "every_run"]) == 0
    rid = w.summaries()[-1]["run_id"]
    assert taken == [("research_a", "runner", rid, True)]


def test_runner_batch_refuses_a_held_lock_with_exit_1_before_drawing(w):
    from litpipe import runner
    w.register("research_a")
    pool = w.proot("research_a") / "unit3_pool.csv"
    pool.write_text("doi,title,authors,year\n" + "".join(f"{d},T,A,2020\n" for d in DOIS), encoding="utf-8")
    write_record(w.proot("research_a"), tool="sweep")
    assert runner.main(["batch", "--project", "research_a", "--pool", str(pool), "--size", "1"]) == 1
    assert not list(w.proot("research_a").glob("lit_pull_queue.b-*.csv"))
    assert not list(pool.parent.glob("*.drawdown.json"))
    assert w.calls("sweep") == []


def test_a_lock_lost_during_the_sweep_stops_the_projects_route(w, monkeypatch):
    from litpipe import runner
    w.top["runner"] = {"lock_stale_s": 0.4}
    w.register("research_a")
    w.queue("research_a", DOIS)
    w.set_fake("sweep", "research_a", sleep=2.0)

    def usurp():
        # A takeover of a lock that is not stale is what the protocol never does, so one write can land
        # inside a heartbeat's read-then-replace and be overwritten (it failed that way on a loaded CI
        # runner). Writing for a second instead means the first heartbeat that reads it marks the lock
        # lost and stops beating, whatever the timing.
        deadline = time.time() + 10
        while time.time() < deadline:
            if lockfile.read(w.proot("research_a")) is not None:
                time.sleep(0.2)
                until = time.time() + 1.0
                while time.time() < until:
                    write_record(w.proot("research_a"), run_id="usurper")
                    time.sleep(0.03)
                return
            time.sleep(0.02)
    t = threading.Thread(target=usurp, daemon=True)
    t.start()
    assert runner.main(["run", "--profile", "every_run"]) == 2
    t.join(5)
    s = w.summaries()[-1]
    lock = [j for j in s["jobs"] if j["job"] == "lock"]
    assert lock and lock[0]["status"] == "ABORTED" and lock[0]["counts_for_exit"]
    assert w.calls("route") == []
    assert lockfile.read(w.proot("research_a"))["run_id"] == "usurper"
