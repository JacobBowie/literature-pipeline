"""W5 verifier P: an adversarial review of the run path (W5-C1: sweep, migrate, the runner, the
lock file, config, net, state). Every lock lives in tmp_path; the races use real processes that
import only litpipe.lockfile (the worker module is written into tmp_path at run time); the network
is stubbed (a recording transport or the stage stand-ins); nothing here touches live state.

Tests that lock a defect this review found (P-1 to P-6) were strict xfails at `7002ee9`; the fixes
landed on 2026-10-08 and they are plain tests now (the P-6 four-process stress race included)."""
import csv
import datetime
import importlib
import io
import json
import multiprocessing
import os
import socket
import subprocess
import sys
import time
import types
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

import pytest

import lit_util
from litpipe import lockfile

REPO = Path(__file__).resolve().parent.parent

# ================================================================ 1. the lock file
WORKER_SRC = '''
from litpipe import lockfile


def rounds(root, barrier, out, me, n_rounds):
    """Each round: wait for the start, try to take the lock (no heartbeat thread), report, wait until
    every worker has tried, release a taken lock, wait until every worker has released."""
    for i in range(n_rounds):
        barrier.wait(timeout=120)
        lk = lockfile.Lock(root, tool="race", run_id=f"{me}-r{i}", heartbeat=False)
        try:
            lk.acquire()
            out.put((i, "took", f"{me}-r{i}", None, (lk.broke or {}).get("run_id")))
        except lockfile.LockHeld as e:
            rec = e.record or {}
            out.put((i, "held", f"{me}-r{i}", "UNREADABLE" if rec.get("unreadable") else rec.get("run_id"),
                     (lk.broke or {}).get("run_id")))
        except BaseException as e:   # anything but LockHeld is a defect: report it, never hang
            out.put((i, "error", f"{me}-r{i}", f"{type(e).__name__}: {e}", None))
        barrier.wait(timeout=120)
        if lk.held:
            lk.release()
        barrier.wait(timeout=120)
'''


def _worker(tmp_path, monkeypatch):
    d = tmp_path / "_p_worker"
    d.mkdir(exist_ok=True)
    (d / "p_lock_worker.py").write_text(WORKER_SRC, encoding="utf-8")
    monkeypatch.syspath_prepend(str(d))
    sys.modules.pop("p_lock_worker", None)
    return importlib.import_module("p_lock_worker")


def _stale_record(root, run_id):
    old = time.time() - lockfile.STALE_S - 900
    rec = {"host": "other-host.example", "pid": 4242, "run_id": run_id, "started": lockfile._iso(old),
           "heartbeat": lockfile._iso(old), "tool": "sweep"}
    (Path(root) / lockfile.LOCK_NAME).write_text(json.dumps(rec), encoding="utf-8")
    return rec


def _rounds(tmp_path, monkeypatch, *, n_workers, n_rounds, stale_each):
    mod = _worker(tmp_path, monkeypatch)
    lock_root = tmp_path / "proj"
    lock_root.mkdir()
    ctx = multiprocessing.get_context("spawn")
    barrier, out = ctx.Barrier(n_workers + 1), ctx.Queue()
    procs = [ctx.Process(target=mod.rounds, args=(str(lock_root), barrier, out, f"w{i}", n_rounds))
             for i in range(n_workers)]
    for p in procs:
        p.start()
    leftovers = []
    try:
        for i in range(n_rounds):
            if stale_each:
                _stale_record(lock_root, f"stale-r{i}")
            barrier.wait(timeout=180)              # start
            barrier.wait(timeout=180)              # every worker tried
            barrier.wait(timeout=180)              # every taken lock released
            leftovers.append(sorted(p.name for p in lock_root.iterdir()))
        got = [out.get(timeout=60) for _ in range(n_workers * n_rounds)]
    finally:
        for p in procs:
            p.join(timeout=60)
            if p.is_alive():
                p.kill()
    assert all(p.exitcode == 0 for p in procs)
    return got, leftovers


def test_lock_race_four_processes_twenty_rounds_exactly_one_holder_each_round(tmp_path, monkeypatch):
    got, leftovers = _rounds(tmp_path, monkeypatch, n_workers=4, n_rounds=20, stale_each=False)
    errors = [g for g in got if g[1] == "error"]
    assert not errors, errors
    for i in range(20):
        rnd = [g for g in got if g[0] == i]
        took = [g for g in rnd if g[1] == "took"]
        assert len(took) == 1, rnd
        # every loser saw the winner's record (or the winner's file between its create and its write)
        assert all(g[3] in (took[0][2], "UNREADABLE") for g in rnd if g[1] == "held"), rnd
    assert all(names == [] for names in leftovers), leftovers       # released, no tombstone left


def test_lock_race_two_processes_break_one_stale_lock_exactly_once_per_round(tmp_path, monkeypatch):
    got, leftovers = _rounds(tmp_path, monkeypatch, n_workers=2, n_rounds=20, stale_each=True)
    assert not [g for g in got if g[1] == "error"]
    for i in range(20):
        rnd = [g for g in got if g[0] == i]
        took = [g for g in rnd if g[1] == "took"]
        assert len(took) == 1, rnd
        assert len([g for g in rnd if g[4] == f"stale-r{i}"]) == 1, rnd
        assert all(g[3] in (took[0][2], "UNREADABLE") for g in rnd if g[1] == "held"), rnd
    assert all(names == [] for names in leftovers), leftovers


def test_lock_race_four_processes_break_one_stale_lock_exactly_once_per_round(tmp_path, monkeypatch):
    got, leftovers = _rounds(tmp_path, monkeypatch, n_workers=4, n_rounds=40, stale_each=True)
    assert not [g for g in got if g[1] == "error"]
    for i in range(40):
        rnd = [g for g in got if g[0] == i]
        took = [g for g in rnd if g[1] == "took"]
        assert len(took) == 1, rnd
        breakers = [g for g in rnd if g[4] == f"stale-r{i}"]
        assert len(breakers) == 1, rnd                                # the stale record broken once
        assert all(g[3] in (took[0][2], "UNREADABLE") for g in rnd if g[1] == "held"), rnd
    assert all(names == [] for names in leftovers), leftovers


def test_a_breaker_never_moves_a_fresh_lock_out_from_under_its_holder(tmp_path, monkeypatch):
    p = tmp_path / lockfile.LOCK_NAME
    _stale_record(tmp_path, "stale-old")
    real_inspect, real_retry = lockfile._inspect, lockfile._retry_fs
    seen_stale = real_inspect(p)                       # C read the old record before anyone broke it
    a = lockfile.Lock(tmp_path, tool="sweep", run_id="A", heartbeat=False).acquire()   # A breaks it, takes it
    _forget(a)
    assert a.held and a.broke["run_id"] == "stale-old"
    b = lockfile.Lock(tmp_path, tool="runner", run_id="B", heartbeat=False)
    c = lockfile.Lock(tmp_path, tool="sweep", run_id="C", heartbeat=False)
    state = {"c_first": True, "b_tried": False, "b": None}

    def c_inspect(path):
        if state["c_first"] and Path(path) == p:
            state["c_first"] = False
            return seen_stale                          # C's view: the old, stale record
        return real_inspect(path)

    def retry(fn, *args):
        out = real_retry(fn, *args)
        if fn is os.replace and Path(args[0]) == p and not state["b_tried"]:
            state["b_tried"] = True                    # B arrives while the path is empty
            try:
                b.acquire()
                _forget(b)                             # B is another process
                state["b"] = "took"
            except lockfile.LockHeld:
                state["b"] = "held"
        return out

    monkeypatch.setattr(lockfile, "_inspect", c_inspect)
    monkeypatch.setattr(lockfile, "_retry_fs", retry)
    with pytest.raises(lockfile.LockHeld):
        c.acquire()
    monkeypatch.setattr(lockfile, "_inspect", real_inspect)
    monkeypatch.setattr(lockfile, "_retry_fs", real_retry)
    if not state["b_tried"]:
        with pytest.raises(lockfile.LockHeld):
            b.acquire()
        state["b"] = "held"
    assert state["b"] == "held"                        # today: B takes the lock in the gap
    assert lockfile.read(tmp_path)["run_id"] == "A"   # today: A's record was deleted; A still thinks it holds
    assert not [q for q in tmp_path.iterdir() if q.name != lockfile.LOCK_NAME]


def _forget(lk):
    """Drop `lk` from this process's held table, so another Lock here behaves as another process."""
    with lockfile._HELD_GUARD:
        lockfile._HELD.pop(lockfile._key(lk.path), None)


def test_a_stalled_heartbeat_never_overwrites_the_new_holders_file(tmp_path, monkeypatch):
    t = {"now": 1_000_000.0}
    monkeypatch.setattr(lockfile, "_time", lambda: t["now"])
    old = lockfile.Lock(tmp_path, tool="sweep", run_id="old-holder", stale_s=100, heartbeat=False).acquire()
    _forget(old)
    real_inspect = lockfile._inspect
    taker = {}

    def inspect_then_sleep(path):
        got = real_inspect(path)
        if not taker and Path(path) == old.path:
            # the machine sleeps right after the old holder read its own record; meanwhile another
            # process judges the lock stale by its heartbeat age and takes it over
            t["now"] += 101
            taker["lk"] = lockfile.Lock(tmp_path, tool="runner", run_id="new-holder", stale_s=100,
                                        heartbeat=False)
            taker["lk"].acquire()
            assert lockfile.read(tmp_path)["run_id"] == "new-holder"
        return got

    monkeypatch.setattr(lockfile, "_inspect", inspect_then_sleep)
    t["now"] += 50                                     # an ordinary beat, well inside STALE_S
    wrote = old.beat()
    monkeypatch.setattr(lockfile, "_inspect", real_inspect)
    assert lockfile.read(tmp_path)["run_id"] == "new-holder"      # today: "old-holder" (overwritten)
    assert wrote is False and old.lost.is_set()


def test_unreadable_and_truncated_locks_never_crash_and_never_break_a_live_one(tmp_path):
    p = tmp_path / lockfile.LOCK_NAME
    for raw in (b"", b'{"host": "x", "pid": 1, "run_', b"\xff\xfe\x00garbage", b"[1, 2, 3]", b"null"):
        p.write_bytes(raw)                                            # fresh: a writer mid-write
        with pytest.raises(lockfile.LockHeld) as e:
            lockfile.Lock(tmp_path, tool="sweep", run_id="me", heartbeat=False).acquire()
        assert e.value.record.get("unreadable") is True
        assert p.read_bytes() == raw                                  # a live (fresh) one is left as is
        old = time.time() - lockfile.STALE_S - 60
        os.utime(p, (old, old))                                       # the writer crashed long ago
        lk = lockfile.Lock(tmp_path, tool="sweep", run_id="me", heartbeat=False).acquire()
        assert lk.held and lk.broke and lk.broke.get("unreadable") is True
        lk.release()
        assert not p.exists()


def test_a_record_without_a_parseable_time_goes_stale_by_its_file_age(tmp_path):
    p = tmp_path / lockfile.LOCK_NAME
    p.write_text(json.dumps({"host": "other-host.example", "pid": 4242, "run_id": "x", "tool": "sweep",
                             "heartbeat": "yesterday-ish"}), encoding="utf-8")
    old = time.time() - lockfile.STALE_S - 60
    os.utime(p, (old, old))
    lk = lockfile.Lock(tmp_path, tool="sweep", run_id="me", heartbeat=False).acquire()   # LockHeld today
    assert lk.held
    lk.release()


def _sleeper():
    return subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])


def test_an_exported_run_id_does_not_let_two_sweeps_share_a_project(tmp_path, monkeypatch):
    monkeypatch.setenv(lockfile.RUN_ENV, "exported-in-a-shell")
    first = lockfile.Lock(tmp_path, tool="sweep", heartbeat=False).acquire()
    _forget(first)
    other = _sleeper()                       # the first sweep is another live process on this host
    try:
        rec = dict(lockfile.read(tmp_path), pid=other.pid)
        (tmp_path / lockfile.LOCK_NAME).write_text(json.dumps(rec), encoding="utf-8")
        with pytest.raises(lockfile.LockHeld):                        # today: joined
            lockfile.Lock(tmp_path, tool="sweep", heartbeat=False).acquire()
    finally:
        other.kill()
        other.wait()


def test_a_runners_stage_child_still_joins_by_run_id(tmp_path, monkeypatch):
    """The guard proposed for P-2 keeps the runner's own join: a lock taken by a runner tool."""
    other = _sleeper()
    try:
        for tool in ("runner", "runner batch"):
            rec = {"host": lockfile.hostname(), "pid": other.pid, "run_id": "20261007T010000Z-runner-1-abc",
                   "started": lockfile._iso(time.time()), "heartbeat": lockfile._iso(time.time()), "tool": tool}
            (tmp_path / lockfile.LOCK_NAME).write_text(json.dumps(rec), encoding="utf-8")
            monkeypatch.setenv(lockfile.RUN_ENV, "20261007T010000Z-runner-1-abc")
            child = lockfile.Lock(tmp_path, tool="sweep").acquire()
            assert child.joined and not child.held and child._thread is None
            assert child.release() is False and lockfile.read(tmp_path) == rec
            monkeypatch.setenv(lockfile.RUN_ENV, "another-run")
            with pytest.raises(lockfile.LockHeld):                    # a different run id is refused
                lockfile.Lock(tmp_path, tool="sweep", heartbeat=False).acquire()
    finally:
        other.kill()
        other.wait()


def test_the_parent_pid_guard_is_not_portable_a_venv_launcher_sits_between(tmp_path):
    """Why P-2's guard is the taker's tool and not "the record holds my parent pid": on Windows a
    venv's python.exe is a launcher, so a child started with sys.executable sees the launcher as its
    parent, not the runner (this asserts only what the platform shows; it is evidence, not a lock)."""
    out = subprocess.run([sys.executable, "-c", "import os; print(os.getppid())"], capture_output=True,
                         text=True, check=True).stdout.strip()
    if sys.platform == "win32" and Path(sys.executable).parent.name.lower() == "scripts":
        assert int(out) != os.getpid()
    else:
        assert int(out) > 0


def test_a_lost_heartbeat_thread_stops_and_release_never_deletes_the_takers_lock(tmp_path):
    lk = lockfile.Lock(tmp_path, tool="sweep", run_id="mine", stale_s=0.8).acquire()   # a beat every 0.1 s
    try:
        deadline = time.time() + 5
        while lk.beats < 1 and time.time() < deadline:
            time.sleep(0.02)
        theirs = {"host": "other-host.example", "pid": 1, "run_id": "taker", "tool": "runner",
                  "started": lockfile._iso(time.time()), "heartbeat": lockfile._iso(time.time())}
        (tmp_path / lockfile.LOCK_NAME).write_text(json.dumps(theirs), encoding="utf-8")
        assert lk.lost.wait(5)
        beats = lk.beats
        time.sleep(0.4)
        assert lk.beats == beats                                     # the thread stopped beating
    finally:
        assert lk.release() is False
    assert lockfile.read(tmp_path) == theirs


# ================================================================ 2. DEC-31 sources: the real sweep into the real migrate
class InprocStages:
    """sweep._run_stage stand-in that runs the REAL stage module's main() in this process (so the
    test's temp ledger, FakeState and transports apply); the network is the recording transport."""

    def __init__(self):
        self.calls = []
        self.current = None

    def __call__(self, cmd):
        cmd = [str(c) for c in cmd]
        script = Path(cmd[1]).name
        self.calls.append(script)
        self.current = script
        mod = importlib.import_module(script[:-3])
        out, err = io.StringIO(), io.StringIO()
        old = sys.argv
        sys.argv = [script] + cmd[2:]
        try:
            with redirect_stdout(out), redirect_stderr(err):
                try:
                    rc = mod.main() if script == "unpaywall_fetch_v2.py" else mod.main(cmd[2:])
                except SystemExit as e:
                    rc = e.code if isinstance(e.code, int) else (0 if e.code is None else 1)
        finally:
            sys.argv = old
            self.current = None
        return types.SimpleNamespace(returncode=rc or 0, stdout=out.getvalue(), stderr=err.getvalue())


def _recording_transport(stages, seen):
    from litpipe import net

    def send(method, url, hdrs, body, timeout, max_bytes, *a, **k):
        from urllib.parse import urlsplit
        seen.append((stages.current, urlsplit(url).hostname))
        return net._Raw(status=404, content=b'{"message": "not found"}', first_chunk=b"{", total=24)
    return send


UNPW_HOSTS = {"api.unpaywall.org"}
PMC_HOSTS = {"pmc.ncbi.nlm.nih.gov", "eutils.ncbi.nlm.nih.gov", "www.ncbi.nlm.nih.gov", "www.ebi.ac.uk",
             "pmc-oa-opendata.s3.amazonaws.com"}

# sources -> (stage scripts launched, exit code)
SOURCE_SETS = {
    "unset": (None, ["unpaywall_fetch_v2.py", "pmc_fetch.py", "extract_pdf_fulltext.py"], 0),
    "unpaywall": (["unpaywall"], ["unpaywall_fetch_v2.py", "extract_pdf_fulltext.py"], 0),
    "pmc": (["pmc"], ["pmc_fetch.py", "extract_pdf_fulltext.py"], 0),
    "unpaywall+pmc": (["unpaywall", "pmc"], ["unpaywall_fetch_v2.py", "pmc_fetch.py", "extract_pdf_fulltext.py"], 0),
    "preprint-only": (["biorxiv"], ["preprint_fetch.py", "extract_pdf_fulltext.py"], 0),
    "empty": ([], [], 4),
    "openalex_content": (["openalex_content"], [], 4),
}


@pytest.mark.parametrize("label", list(SOURCE_SETS))
def test_sources_drive_the_real_sweep_into_the_real_migrate(label, tmp_path, monkeypatch):
    import migrate_closed_to_md as mig
    import sweep
    import unpaywall_fetch_v2 as U
    from litpipe import config, ledger, net
    from tests.test_sweep_runs import DAY, Env, read_csv
    srcs, launched, want_exit = SOURCE_SETS[label]
    env = Env(tmp_path, monkeypatch)
    entry = {"lib_dir": "lit"} if srcs is None else {"lib_dir": "lit", "sources": srcs}
    env.register({"P": entry})
    cfg = json.loads(env.cfg_path.read_text(encoding="utf-8"))
    cfg["state_dir"] = str(tmp_path / "state")                   # holdings caches stay in tmp
    env.cfg_path.write_text(json.dumps(cfg), encoding="utf-8")
    monkeypatch.setattr(config, "CONFIG_PATH", env.cfg_path)
    monkeypatch.setattr(mig, "CONFIG_PATH", env.cfg_path)
    monkeypatch.setenv("LITPIPE_EMAIL", "tester@litpipe-test.org")
    stages, seen = InprocStages(), []
    monkeypatch.setattr(sweep, "_run_stage", stages)
    for name in list(net._TRANSPORTS):
        monkeypatch.setitem(net._TRANSPORTS, name, _recording_transport(stages, seen))
    dois = ["10.1000/held1", "10.1000/a1", "10.1000/b1"]
    q = env.queue(dois)
    lib = env.pdir() / "lit"
    lib.mkdir(parents=True, exist_ok=True)
    held_fn = U.build_filename("2020", "Smith J", "Title 0")
    (lib / held_fn).write_bytes(b"%PDF-1.4 held")
    (lib / (held_fn[:-4] + ".fulltext.json")).write_text(json.dumps({"doi": dois[0], "text": "x" * 400}),
                                                         encoding="utf-8")

    res = sweep.run(project="P", date=DAY)
    assert res["exit_code"] == want_exit, (label, res)
    assert stages.calls == launched, (label, stages.calls)
    led = {(r.get("host") or "") for r in ledger.read(date="*")}
    hosts = {h for _s, h in seen}
    assert led == hosts                                            # one ledger line per attempt sent
    if srcs is not None and "unpaywall" not in srcs:
        assert not hosts & UNPW_HOSTS, (label, hosts)              # 0 Unpaywall requests
    if srcs is not None and "pmc" not in srcs:
        assert not {h for s, h in seen if s != "preprint_fetch.py"} & PMC_HOSTS, (label, seen)
    if want_exit == 4:                                             # refused before fetching: queue kept
        assert q.exists() and not seen and not list(env.pdir().glob("lit_pull_queue.*.csv"))
        return
    assert not q.exists()                                          # retired
    resid = {r["doi"]: r for r in read_csv(env.art("residual"))}
    rep = env.report()
    # no row lost: every queue row is fetched (or held) or has a residual class
    assert set(resid) | {dois[0]} == set(dois), (label, resid)
    assert all(r["residual_class"] for r in resid.values())
    skipped = {s for s in ("unpaywall", "pmc") if srcs is not None and s not in srcs}
    for r in resid.values():
        assert skipped <= set(filter(None, r["skipped_sources"].split(";"))), (label, r)
        assert r["residual_class"] != "PENDING"
    for s in skipped:
        assert rep[("stage", sweep._STAGE_LABEL[s])]["detail"] == \
            f"skipped (the project's sources omit {s})"
    pmc_in = env.art(sweep.PMC_INPUT_STAGE)
    if label == "pmc":
        rows = {r["doi"]: r for r in read_csv(pmc_in)}
        assert rows[dois[0]]["oa_status"] == "SKIP_EXISTS" and rows[dois[0]]["filename"] == held_fn
        assert rows[dois[1]]["filename"] == U.build_filename("2020", "Smith J", "Title 1")
    elif srcs is None or "unpaywall" in srcs:
        assert not pmc_in.exists()
    # the REAL migrate routes the run sweep printed, exactly as sweep built the command
    cmd = res["projects"]["P"]["migrate"]
    assert cmd is not None
    out = io.StringIO()
    with redirect_stdout(out):
        assert mig.main([str(c) for c in cmd[2:]]) == 0, out.getvalue()
    routing = read_csv(env.art("routing"))
    assert {r["doi"] for r in routing} == set(resid), (label, routing)
    ill = (env.pdir() / mig.ILL_NAME).read_text(encoding="utf-8") if (env.pdir() / mig.ILL_NAME).exists() else ""
    for d, r in resid.items():
        if mig.ROUTE_OF.get(r["residual_class"]) == "ill":
            assert f"`{d}`" in ill


def _registered_env(tmp_path, monkeypatch, entry):
    import migrate_closed_to_md as mig
    from litpipe import config
    from tests.test_sweep_runs import Env
    env = Env(tmp_path, monkeypatch)
    env.register({"P": entry})
    cfg = json.loads(env.cfg_path.read_text(encoding="utf-8"))
    cfg["state_dir"] = str(tmp_path / "state")
    env.cfg_path.write_text(json.dumps(cfg), encoding="utf-8")
    monkeypatch.setattr(config, "CONFIG_PATH", env.cfg_path)
    monkeypatch.setattr(mig, "CONFIG_PATH", env.cfg_path)
    return env


def _hold_dec14(env, doi, i=0):
    import unpaywall_fetch_v2 as U
    lib = env.pdir() / "lit"
    lib.mkdir(parents=True, exist_ok=True)
    fn = U.build_filename("2020", "Smith J", f"Title {i}")            # the DEC-14 name every stage writes
    (lib / fn).write_bytes(b"%PDF-1.4 held")
    (lib / (fn[:-4] + ".fulltext.json")).write_text(json.dumps({"doi": doi, "text": "x"}), encoding="utf-8")
    return fn


def test_a_preprint_only_project_never_fetches_or_ill_lists_a_paper_it_holds(tmp_path, monkeypatch):
    import migrate_closed_to_md as mig
    import sweep
    from tests.test_sweep_runs import DAY, read_csv
    env = _registered_env(tmp_path, monkeypatch, {"lib_dir": "lit", "sources": ["biorxiv"]})
    held, new = "10.1000/held1", "10.1000/new1"
    env.queue([held, new])
    _hold_dec14(env, held)
    res = sweep.run(project="P", date=DAY)
    assert res["exit_code"] == 0
    assert env.stages.stages_called() == ["preprint", "extract"]
    assert held not in env.stages.triage_dois("preprint")            # today: the held paper is searched
    resid = {r["doi"]: r for r in read_csv(env.art("residual"))}
    assert held not in resid                                         # today: TERMINAL_CLOSED
    cmd = res["projects"]["P"]["migrate"]
    with redirect_stdout(io.StringIO()):
        assert mig.main([str(c) for c in cmd[2:]]) == 0
    ill = env.pdir() / mig.ILL_NAME
    assert not ill.exists() or f"`{held}`" not in ill.read_text(encoding="utf-8")


def test_today_a_preprint_only_project_lists_a_held_paper_for_ill(tmp_path, monkeypatch):
    """The behaviour P-3 locks against, shown on the code as it stands (delete once P-3 is applied)."""
    import migrate_closed_to_md as mig
    import sweep
    from tests.test_sweep_runs import DAY, read_csv
    env = _registered_env(tmp_path, monkeypatch, {"lib_dir": "lit", "sources": ["biorxiv"]})
    held = "10.1000/held1"
    env.queue([held])
    _hold_dec14(env, held)
    res = sweep.run(project="P", date=DAY)
    resid = {r["doi"]: r for r in read_csv(env.art("residual"))}
    if held in resid:                                                 # the defect is present
        assert resid[held]["residual_class"] == "TERMINAL_CLOSED"
        with redirect_stdout(io.StringIO()):
            assert mig.main([str(c) for c in res["projects"]["P"]["migrate"][2:]]) == 0
        assert f"`{held}`" in (env.pdir() / mig.ILL_NAME).read_text(encoding="utf-8")


# ================================================================ 3. artifact_dir: every reader finds the artifacts
def _art_registry(tmp_path, monkeypatch, art):
    root = tmp_path / "Projects"
    root.mkdir()
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    cfg = {"state_dir": str(tmp_path / "state"), "artifact_dir": art, "projects": {
        "Group": {"lib_dir": "lib"},
        "Group/teaching_x": {"parent": "Group", "lib_dir": "teaching_x/lit"},
        "research_y": {"lib_dir": "lit"}}}
    for k, e in cfg["projects"].items():
        lit_util.project_root(k, e).mkdir(parents=True, exist_ok=True)
    return cfg


def test_a_global_absolute_artifact_dir_is_read_by_worklists_and_the_audit(tmp_path, monkeypatch):
    import audit_portfolio
    from litpipe import config, worklists
    art = tmp_path / "artifacts"
    cfg = _art_registry(tmp_path, monkeypatch, str(art))
    d = config.artifact_dir("Group/teaching_x", cfg)
    assert d == art / "Group/teaching_x" and d != config.artifact_dir("Group", cfg)
    d.mkdir(parents=True)
    res = d / "lit_pull_queue.2026-10-01.residual.csv"
    res.write_text("doi,residual_class\n10.1000/x,TERMINAL_CLOSED\n", encoding="utf-8")
    found = worklists.residual_csvs(cfg)
    assert found == [("Group/teaching_x", res)]                       # once, for its own project only
    dirs = [p for p, _m in audit_portfolio.report_dirs("Group/teaching_x", cfg["projects"]["Group/teaching_x"],
                                                      registry=cfg)]
    assert d in dirs
    assert audit_portfolio.project_artifact_dir("Group", cfg["projects"]["Group"], cfg) == art / "Group"


def test_two_projects_on_one_artifact_dir_is_config_and_the_readers_say_so(tmp_path, monkeypatch, capsys):
    import audit_portfolio
    from litpipe import config, worklists
    cfg = _art_registry(tmp_path, monkeypatch, None)
    shared = str(tmp_path / "one_dir")
    cfg["projects"]["research_y"]["artifact_dir"] = shared
    cfg["projects"]["Group"]["artifact_dir"] = shared
    with pytest.raises(config.ConfigError):
        config.artifact_dir("research_y", cfg)
    with pytest.raises(worklists.WorklistError):
        worklists.residual_csvs(cfg)
    root = lit_util.project_root("research_y", cfg["projects"]["research_y"])
    assert audit_portfolio.project_artifact_dir("research_y", cfg["projects"]["research_y"], cfg) == root
    assert "artifact_dir unusable" in capsys.readouterr().err


def test_an_unset_artifact_dir_is_the_project_root_for_every_key(tmp_path, monkeypatch):
    from litpipe import config
    cfg = _art_registry(tmp_path, monkeypatch, None)
    del cfg["artifact_dir"]
    for k, e in cfg["projects"].items():
        assert config.artifact_dir(k, cfg) == lit_util.project_root(k, e)
    assert config.artifact_dir("not_registered", cfg) == lit_util.project_root("not_registered", {})


# ================================================================ 4. the import (fold-in A3)
EMAIL = "someone.private@example.edu"
LONG_REVIEW = "review (never reached Unpaywall; settled or partial run)"
ERR = (f"HTTPSConnectionPool(host='api.unpaywall.org', port=443): Max retries exceeded with url: "
       f"/v2/10.1/x?email={EMAIL}")


def _import_world(tmp_path, monkeypatch):
    import migrate_closed_to_md as mig
    root = tmp_path / "Projects"
    root.mkdir()
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    cfg = {"state_dir": str(tmp_path / "state"), "projects": {
        "Courses": {"lib_dir": "lib"},
        "Courses/teaching_a": {"parent": "Courses", "lib_dir": "teaching_a/literature"},
        "Courses/teaching_b": {"parent": "Courses", "lib_dir": "teaching_b/literature"}}}
    for k, e in cfg["projects"].items():
        lit_util.project_root(k, e).mkdir(parents=True, exist_ok=True)
    p = tmp_path / "projects.json"
    p.write_text(json.dumps(cfg), encoding="utf-8")
    monkeypatch.setattr(mig, "CONFIG_PATH", p)
    return cfg


class _HoldMap:
    def __init__(self, held):
        self.held = {d.lower() for d in held}

    def content(self, doi):
        return [types.SimpleNamespace(has_pdf=True, path=Path("x.pdf"))] if str(doi).lower() in self.held else []


def _import_csv(tmp_path, rows):
    p = tmp_path / "unledgered_swept.csv"
    with open(p, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["project", "doi", "pool", "first_swept", "last_oa_status", "last_error",
                                          "suggested_worklist", "title", "year"], lineterminator="\n")
        w.writeheader()
        for r in rows:
            w.writerow({"pool": "pool.csv", "first_swept": "2026-09-20", "last_error": ERR, "year": "2019", **r})
    return p


IMPORT_ROWS = [
    {"project": "teaching_a", "doi": "10.1000/ILL.1", "suggested_worklist": "ill", "last_oa_status": "CLOSED",
     "title": f"Closed [one] with `DOI 10.9/wrong` and {EMAIL}"},
    {"project": "teaching_a", "doi": "https://doi.org/10.1000/ill.2", "suggested_worklist": "ill", "title": "Two"},
    {"project": "teaching_b", "doi": "10.1000/ill.2", "suggested_worklist": "ill", "title": "Two again"},
    {"project": "teaching_a", "doi": "10.1000/listed.1", "suggested_worklist": "ill", "title": "Listed"},
    {"project": "teaching_a", "doi": "10.1000/held.1", "suggested_worklist": "oa-blocked", "title": "Held"},
    {"project": "teaching_a", "doi": "10.1000/oa.1", "suggested_worklist": "oa-blocked", "last_oa_status": "OA",
     "title": "Open [but] (blocked)"},
    {"project": "teaching_a", "doi": "10.1000/rev.1", "suggested_worklist": LONG_REVIEW, "title": "Review one"},
    {"project": "teaching_a", "doi": "10.1000/rev.2", "suggested_worklist": LONG_REVIEW, "title": "Review two"},
    {"project": "Courses/teaching_b", "doi": "10.1000/rev.1", "suggested_worklist": LONG_REVIEW, "title": "R"},
]


def _read_lists(cfg, key, today):
    import sweep
    from litpipe import worklists
    root = lit_util.project_root(key, cfg["projects"][key])
    oa = [r["doi"].lower() for r in worklists.oa_blocked(cfg, projects=[key])]
    ill = [worklists.parse_ill_line(body)[0].lower() for _l, _c, body in
           worklists._checkbox_lines(root / "lit_pull_queue.md")] if (root / "lit_pull_queue.md").exists() else []
    _f, due, waiting, bad = sweep.split_retry_later(root, today.isoformat())
    return root, ill, oa, [r["doi"].lower() for r in due], waiting, bad


def test_the_import_writes_every_target_that_migrates_own_readers_read_back(tmp_path, monkeypatch):
    import migrate_closed_to_md as mig
    cfg = _import_world(tmp_path, monkeypatch)
    root_a = lit_util.project_root("Courses/teaching_a", cfg["projects"]["Courses/teaching_a"])
    (root_a / mig.ILL_NAME).write_text("# Manual Pull Queue\n\n- [ ] **Old** — DOI `10.1000/LISTED.1` — earlier\n",
                                       encoding="utf-8")
    src = _import_csv(tmp_path, IMPORT_ROWS)
    before = src.read_bytes()
    today = datetime.date(2026, 10, 15)
    res = mig.import_csv(src, cfg=cfg, holdmap=_HoldMap({"10.1000/held.1"}), commit=True, today=today)
    assert not res["unregistered"] and not res["malformed"] and src.read_bytes() == before
    a = res["projects"]["Courses/teaching_a"]
    assert (a["ill"]["write"], a["ill"]["listed"], a["oa-blocked"]["write"], a["oa-blocked"]["held"],
            a["review"]["write"]) == (2, 1, 1, 1, 2)
    root, ill, oa, due, waiting, bad = _read_lists(cfg, "Courses/teaching_a", today)
    assert sorted(ill) == ["10.1000/ill.1", "10.1000/ill.2", "10.1000/listed.1"]   # the readers get the DOI
    assert oa == ["10.1000/oa.1"]
    assert sorted(due) == ["10.1000/rev.1", "10.1000/rev.2"] and not waiting and not bad
    _r, ill_b, _oa, due_b, _w, _b = _read_lists(cfg, "Courses/teaching_b", today)
    assert ill_b == ["10.1000/ill.2"] and due_b == ["10.1000/rev.1"]  # one DOI, two projects: both lists
    assert not (root / "lit_pull_queue.review.md").exists()           # the identity-flag list is never written
    written = b"".join(p.read_bytes() for p in (tmp_path / "Projects").rglob("*") if p.is_file())
    assert EMAIL.encode() not in written and b"HTTPSConnectionPool" not in written
    # idempotent: a second commit writes nothing
    snap = {p: p.read_bytes() for p in (tmp_path / "Projects").rglob("*") if p.is_file()}
    res2 = mig.import_csv(src, cfg=cfg, holdmap=_HoldMap({"10.1000/held.1"}), commit=True, today=today)
    assert res2["written"] == {}
    assert snap == {p: p.read_bytes() for p in (tmp_path / "Projects").rglob("*") if p.is_file()}


def test_review_as_list_only_writes_no_review_row_and_the_next_sweep_admits_retry_rows(tmp_path, monkeypatch):
    import migrate_closed_to_md as mig
    import sweep
    cfg = _import_world(tmp_path, monkeypatch)
    src = _import_csv(tmp_path, [r for r in IMPORT_ROWS if r["suggested_worklist"] == LONG_REVIEW])
    today = datetime.date(2026, 10, 15)
    res = mig.import_csv(src, cfg=cfg, holdmap=_HoldMap(()), commit=True, today=today, review_as="list-only")
    assert res["written"] == {} and not list((tmp_path / "Projects").rglob("*.csv"))
    res = mig.import_csv(src, cfg=cfg, holdmap=_HoldMap(()), commit=True, today=today)
    root = lit_util.project_root("Courses/teaching_a", cfg["projects"]["Courses/teaching_a"])
    entry = cfg["projects"]["Courses/teaching_a"]
    dest = os.path.relpath(lit_util.lib_paths("Courses/teaching_a", entry)[1], root).replace(os.sep, "/")
    adm = sweep.admit_retries(root, today.isoformat(), default_destination=dest)   # what sweep passes
    assert adm["admitted"] == 2
    rows = sweep.read_queue(root / f"lit_pull_queue.{sweep.RETRY_TAG}.csv")[1]
    assert sorted(r["doi"] for r in rows) == ["10.1000/rev.1", "10.1000/rev.2"]
    assert all(r["destination"] == dest for r in rows), rows          # the registry library, as sweep's
    # the admitted rows are still "listed" for a re-run of the import (the live queue counts)
    res2 = mig.import_csv(src, cfg=cfg, holdmap=_HoldMap(()), commit=False, today=today)
    assert res2["projects"]["Courses/teaching_a"]["review"]["listed"] == 2


# ================================================================ 5. runner follow-ups
@pytest.mark.parametrize("label, offset_s", [("on time", 0), ("1 h late", 3600), ("1 h early", -3600),
                                             ("DST autumn", 3600), ("DST spring", -3600),
                                             ("11 h late", 11 * 3600)])
def test_the_escalation_thresholds_hold_on_a_late_run_and_a_dst_shift(label, offset_s, monkeypatch):
    """Nightly runs at one local time; the stamp is the START of the run that last escalated. The
    weekly fires on the 7th night and the monthly on the 28th, never a night early or late, when a
    night's start is off by up to 11 h (a late run, or a DST shift of the wall clock)."""
    from litpipe import runner
    day = 86400.0
    stamp = 2_000_000_000.0
    for kind, nights, rank in (("weekly", 7, "weekly"), ("monthly", 28, "monthly")):
        for n, want in ((nights - 1, False), (nights, True)):
            r = object.__new__(runner._ScheduledRun)
            r.profile, r.scheduled, r.t0 = "daily", True, stamp + n * day + offset_s
            kv = {"profile_done:weekly": stamp, "profile_done:monthly": stamp}
            if kind == "monthly":   # weekly done the night before, so only the monthly rule is in play
                kv["profile_done:weekly"] = r.t0 - day
            monkeypatch.setattr(runner, "_kv_get", lambda k, d=None, read_only=False, _kv=kv: _kv.get(k, d))
            eff = r._escalate()
            assert (runner.RANK[eff] >= runner.RANK[rank]) is want, (label, kind, n, eff)


@pytest.mark.parametrize("block, needle", [
    ({"lock_stale_s": True}, "lock_stale_s"), ({"lock_stale_s": 0}, "lock_stale_s"),
    ({"lock_stale_s": "7200"}, "lock_stale_s"), ({"ris_limit": 0}, "ris_limit"), ({"ris_limit": 1.5}, "ris_limit"),
    ({"ris_limit": True}, "ris_limit"), ({"schedule_time": "24:00"}, "schedule_time"),
    ({"schedule_time": "1:00"}, "schedule_time"), ({"schedule_time": 100}, "schedule_time"),
    ({"timeouts": {"sweep": 0}}, "timeouts"), ({"timeouts": {"no_such_job": 60}}, "timeouts"),
    ({"timeouts": {"sweep": True}}, "timeouts"), ({"timeouts": [60]}, "timeouts")])
def test_the_runner_keys_are_validated(block, needle):
    from litpipe import config, runner
    with pytest.raises(config.ConfigError, match=needle):
        runner.runner_block({"projects": {}, "runner": block})


def test_valid_runner_keys_pass_through():
    from litpipe import runner
    b = runner.runner_block({"projects": {}, "runner": {"lock_stale_s": 600, "ris_limit": 5, "schedule_time": "23:59",
                                                        "timeouts": {"sweep": 30}}})
    assert (b["lock_stale_s"], b["ris_limit"], b["schedule_time"], b["timeouts"]) == (600.0, 5, "23:59", {"sweep": 30.0})
    assert lockfile.stale_s_from({"projects": {}, "runner": {"lock_stale_s": 600}}) == 600.0


@pytest.mark.parametrize("raw", ["NaN", "Infinity"])
def test_a_non_finite_lock_stale_s_is_a_config_error_everywhere(raw):
    from litpipe import config, runner
    cfg = json.loads('{"projects": {}, "runner": {"lock_stale_s": %s}}' % raw)
    with pytest.raises(config.ConfigError):
        lockfile.stale_s_from(cfg)
    with pytest.raises(config.ConfigError):
        runner.runner_block(cfg)


# ================================================================ 6. the lease: renewed while a body drains
def test_a_long_transfer_keeps_its_slot_against_another_claim_and_renewal_stops_after(tmp_path, monkeypatch):
    from litpipe import net, state
    from tests import netmock
    monkeypatch.setattr(state, "DB_PATH", tmp_path / "s.sqlite")
    wall = {"t": 1_000_000.0}
    monkeypatch.setattr(state, "_time", lambda: wall["t"])

    class Clock(netmock.FakeClock):
        def monotonic(self):
            return wall["t"]
    monkeypatch.setattr(net, "CLOCK", Clock())
    host = "lease.example.org"
    slot = state.acquire(host, interval=0, budget=None, concurrency=1)
    renewer = net._LeaseRenewer(state, slot)
    claims = []

    def chunks():
        for _ in range(8):                                            # 8 chunks, 700 s apart: 5,600 s
            wall["t"] += 700.0
            claims.append(state._try_claim(host, 0.0, None, 1, state.LEASE_S, None))
            yield b"x" * 10

    net._LOCAL.renewer = renewer
    try:
        net._drain(chunks(), 10 ** 6)
    finally:
        net._LOCAL.renewer = None
    assert wall["t"] - 1_000_000.0 > state.LEASE_S                    # longer than one lease
    assert renewer.renewals >= 6
    assert all(not isinstance(c, state.Slot) for c in claims)         # nobody else got the host's slot
    n = renewer.renewals
    net._drain(iter([b"y"] * 3), 10 ** 6)                             # no renewer bound: nothing renewed
    assert renewer.renewals == n
    state.release(host, slot=slot)


def test_a_failed_transfer_stops_renewing(net_env, mock_server, monkeypatch):
    from litpipe import hosts, net
    from litpipe.hosts import HostPolicy
    from tests import netmock

    class St(netmock.FakeState):
        LEASE_S = 3.0

        def __init__(self, clock):
            super().__init__(clock)
            self.renewed = 0

        def renew(self, slot):
            self.renewed += 1
            return True
    hosts.register(HostPolicy("127.0.0.1", min_interval_s=0.0, transport="urllib"))
    clock = netmock.FakeClock()
    st = St(clock)
    monkeypatch.setattr(net, "CLOCK", clock)

    def body():
        for _ in range(4):
            clock.t += 2.0                                            # each chunk takes 2 s > LEASE_S / 3
            yield b"%PDF" + b"x" * 100
        raise OSError("connection reset mid-body")

    def stream_then_fail(method, url, hdrs, b, timeout, max_bytes, *a, **k):
        try:
            net._drain(body(), 10 ** 6)                               # the transports drain through _drain
        except OSError as e:
            return net._Raw(error=f"TRANSPORT: {e}")
        return net._Raw(status=200)
    monkeypatch.setitem(net._TRANSPORTS, "urllib", stream_then_fail)
    o = net.request("GET", "http://127.0.0.1:9/x", state=st)
    assert not o.ok and st.renewed >= 1                               # renewed while the body drained
    assert getattr(net._LOCAL, "renewer", None) is None               # and not after the failure
    n = st.renewed
    net._drain(iter([b"z"] * 5), 10 ** 6)
    assert st.renewed == n


# ================================================================ 7. best_oa_url: the Unpaywall report to the worklist
def test_best_oa_url_from_the_unpaywall_writer_reaches_the_worklist_line_and_routing(tmp_path, monkeypatch):
    import migrate_closed_to_md as mig
    import unpaywall_fetch_v2 as U
    import tests.test_sweep_runs as T
    from litpipe import worklists
    from tests.test_sweep_runs import DAY, read_csv
    env = _registered_env(tmp_path, monkeypatch, {"lib_dir": "lit"})
    monkeypatch.setattr(T, "UNPW_FIELDS", list(U.REPORT_FIELDS))      # the real report's columns
    upw = {"doi": "10.1016/s0140-6736(20)30183-5", "is_oa": True,
           "best_oa_location": {"url_for_pdf": "https://www.thelancet.com/action/showPdf?pii=S0140-6736(20)30183-5",
                                "url": "https://www.thelancet.com/article/S0140-6736(20)30183-5/fulltext"}}
    best = U.best_oa_url(upw)
    blocked, nobest = "10.1016/s0140-6736(20)30183-5", "10.1000/nobest1"
    env.queue([blocked, nobest])
    refused = {"oa_status": "OA", "error": "HTTP_403", "attempts": "publisher/publishedVersion/HTTP_403",
               "winning_host": "www.thelancet.com", "first_status": "403", "route": "download",
               "outcome": "REFUSED", "detail": "HTTP 403 from www.thelancet.com"}   # the typed row W2-B writes
    env.stages.spec[blocked] = {"unpaywall": U._clean(dict(refused, best_oa_url=best))}
    env.stages.spec[nobest] = {"unpaywall": U._clean(dict(refused, best_oa_url=""))}
    res = sweep_run = __import__("sweep").run(project="P", date=DAY)
    assert sweep_run["exit_code"] == 0
    resid = {r["doi"]: r for r in read_csv(env.art("residual"))}
    assert resid[blocked]["residual_class"] == "OA_BLOCKED" and resid[blocked]["best_oa_url"] == best, \
        {d: (r["residual_class"], r["reason"], r["stages"]) for d, r in resid.items()}
    with redirect_stdout(io.StringIO()):
        assert mig.main([str(c) for c in res["projects"]["P"]["migrate"][2:]]) == 0
    rows = {r["doi"].lower(): r for r in worklists.oa_blocked(json.loads(env.cfg_path.read_text()), projects=["P"])}
    assert rows[blocked]["parsed"] and rows[blocked]["link"] == best           # parentheses survive the link
    assert rows[nobest]["link"] == mig.doi_url(nobest)                         # no best_oa_url: doi.org
    routing = {r["doi"]: r for r in read_csv(env.art("routing"))}
    assert routing[blocked]["best_oa_url"] == best and routing[nobest]["best_oa_url"] == ""
