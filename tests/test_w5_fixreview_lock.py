"""Fix review of e69e299 (lock file): reproductions written at 5487c3c. Every lock lives in tmp_path."""
import multiprocessing
import os
import time
from pathlib import Path

import pytest

from litpipe import lockfile
from tests.test_w5_verify_p import _forget, _stale_record, _worker


def _breaker(root):
    return Path(str(Path(root) / lockfile.LOCK_NAME) + lockfile.BREAK_SUFFIX)


def _plant_old_breaker(root, age_s=None):
    b = _breaker(root)
    b.write_text("4242", encoding="ascii")                     # a breaker that crashed mid-break
    old = time.time() - (lockfile.BREAK_STALE_S + 60 if age_s is None else age_s)
    os.utime(b, (old, old))
    return b


class _OsProxy:
    """lockfile's view of `os`, with one hook on the breaker file's first stat."""

    def __init__(self, on_stat):
        self._on_stat = on_stat

    def __getattr__(self, name):
        return getattr(os, name)

    def stat(self, p, *a, **k):
        st = os.stat(p, *a, **k)
        self._on_stat(Path(p))
        return st


# ---------------------------------------------------------------- FR-1: the stale-breaker check-then-act
def test_fr1_two_breakers_past_one_stale_breaker_file_never_both_hold_it(tmp_path, monkeypatch):
    lock = tmp_path / lockfile.LOCK_NAME
    b = _plant_old_breaker(tmp_path)
    got = {}

    def p1_arrives(p):
        # P2 has looked at the OLD breaker file (stale); before it removes it, P1 does the same look,
        # removes the old file and creates its own
        if p == b and "p1" not in got:
            got["p1"] = None
            got["p1"] = lockfile._take_breaker(lock)

    monkeypatch.setattr(lockfile, "os", _OsProxy(p1_arrives))
    p2 = lockfile._take_breaker(lock)
    monkeypatch.setattr(lockfile, "os", os)
    assert got["p1"], "P1 should have taken the breaker"
    assert not p2, "P2 also holds the breaker: it removed P1's fresh breaker file and created its own"
    assert b.exists() and not [q for q in tmp_path.iterdir() if q.name not in (b.name,)]


WORKER_SRC = '''
from litpipe import lockfile


def rounds(root, barrier, out, me, n_rounds):
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
        except BaseException as e:
            out.put((i, "error", f"{me}-r{i}", f"{type(e).__name__}: {e}", None))
        barrier.wait(timeout=120)
        if lk.held:
            lk.release()
        barrier.wait(timeout=120)
'''


def _rounds_with_old_breaker(tmp_path, monkeypatch, n_workers, n_rounds):
    import importlib
    import sys
    d = tmp_path / "_fr_worker"
    d.mkdir(exist_ok=True)
    (d / "fr_lock_worker.py").write_text(WORKER_SRC, encoding="utf-8")
    monkeypatch.syspath_prepend(str(d))
    sys.modules.pop("fr_lock_worker", None)
    mod = importlib.import_module("fr_lock_worker")
    root = tmp_path / "proj"
    root.mkdir()
    ctx = multiprocessing.get_context("spawn")
    barrier, out = ctx.Barrier(n_workers + 1), ctx.Queue()
    procs = [ctx.Process(target=mod.rounds, args=(str(root), barrier, out, f"w{i}", n_rounds))
             for i in range(n_workers)]
    for p in procs:
        p.start()
    leftovers = []
    try:
        for i in range(n_rounds):
            _stale_record(root, f"stale-r{i}")
            _plant_old_breaker(root)
            barrier.wait(timeout=180)
            barrier.wait(timeout=180)
            barrier.wait(timeout=180)
            leftovers.append(sorted(p.name for p in root.iterdir()))
            for q in root.iterdir():                     # next round starts clean
                q.unlink()
        got = [out.get(timeout=60) for _ in range(n_workers * n_rounds)]
    finally:
        for p in procs:
            p.join(timeout=60)
            if p.is_alive():
                p.kill()
    return got, leftovers


def test_fr1b_four_processes_past_a_crashed_breakers_old_file_one_holder_each_round(tmp_path, monkeypatch):
    """Evidence run (not a lock): the P-6 race with a crashed breaker's old file planted each round."""
    n = int(os.environ.get("FR_ROUNDS", "60"))
    got, leftovers = _rounds_with_old_breaker(tmp_path, monkeypatch, 4, n)
    errors = [g for g in got if g[1] == "error"]
    bad = []
    for i in range(n):
        rnd = [g for g in got if g[0] == i]
        took = [g for g in rnd if g[1] == "took"]
        breakers = [g for g in rnd if g[4] == f"stale-r{i}"]
        if len(took) != 1 or len(breakers) != 1:
            bad.append(rnd)
    print(f"\nFR-1b: {n} rounds, {len(bad)} with a holder or break count != 1, {len(errors)} errors")
    for r in bad[:5]:
        print("  ", r)
    for e in errors[:5]:
        print("  ", e)
    assert not errors and not bad


# ---------------------------------------------------------------- FR-2: the 60 s stale-breaker removal has no lock
def test_fr2_a_crashed_breakers_old_file_is_removed_and_the_stale_lock_broken(tmp_path):
    _stale_record(tmp_path, "stale-old")
    b = _plant_old_breaker(tmp_path)
    lk = lockfile.Lock(tmp_path, tool="sweep", run_id="me", heartbeat=False).acquire()
    _forget(lk)
    assert lk.held and (lk.broke or {}).get("run_id") == "stale-old"
    assert lk.release() is True
    assert not b.exists() and not list(tmp_path.iterdir())


def test_fr2b_a_fresh_crashed_breakers_file_keeps_a_stale_lock_held_until_it_ages(tmp_path):
    _stale_record(tmp_path, "stale-old")
    _plant_old_breaker(tmp_path, age_s=1)
    with pytest.raises(lockfile.LockHeld):
        lockfile.Lock(tmp_path, tool="sweep", run_id="me", heartbeat=False).acquire()


# ---------------------------------------------------------------- FR-1c: a Windows sharing violation on the breaker file
def _breaker_create_denied(monkeypatch):
    real = lockfile._write_new

    def write_new(path, data):
        if str(path).endswith(lockfile.BREAK_SUFFIX):
            raise PermissionError(13, "Permission denied (injected: the breaker name is delete-pending)", str(path))
        return real(path, data)
    monkeypatch.setattr(lockfile, "_write_new", write_new)


def test_fr1c_a_sharing_violation_on_the_breaker_file_reads_as_held_never_crashes_acquire(tmp_path, monkeypatch):
    _stale_record(tmp_path, "stale-old")
    _breaker_create_denied(monkeypatch)
    with pytest.raises(lockfile.LockHeld):
        lockfile.Lock(tmp_path, tool="sweep", run_id="me", heartbeat=False).acquire()


def test_fr1c_a_crashed_breakers_file_that_stays_held_open_reads_as_held_never_crashes(tmp_path, monkeypatch):
    """The reap's move of an old breaker file meets a sharing violation past every retry: the
    stale lock reads as held (another process is at it), never a PermissionError out of acquire()."""
    _stale_record(tmp_path, "stale-old")
    _plant_old_breaker(tmp_path)
    real = os.replace

    def replace(src, dst, *a, **k):
        if str(src).endswith(lockfile.BREAK_SUFFIX):
            raise PermissionError(13, "Permission denied (injected: held open)", str(src))
        return real(src, dst, *a, **k)
    monkeypatch.setattr(lockfile.os, "replace", replace)
    monkeypatch.setattr(lockfile, "FS_WAIT_S", 0)
    with pytest.raises(lockfile.LockHeld):
        lockfile.Lock(tmp_path, tool="sweep", run_id="me", heartbeat=False).acquire()


def test_fr1c_a_sharing_violation_on_the_breaker_file_never_crashes_release(tmp_path, monkeypatch):
    lk = lockfile.Lock(tmp_path, tool="sweep", run_id="me", heartbeat=False).acquire()
    _breaker_create_denied(monkeypatch)
    assert lk.release() is True
    assert lockfile.read(tmp_path) is None
