"""litpipe.lockfile: the per-project sweep lock FILE (W5 plan item 9; dispatch W5-C1).

State DBs are per machine (the scheduled runner on one machine, interactive sweeps on another), so
the lock that keeps two of them off one project's queues is a FILE in the project directory:

    <project root>/lit_pull_queue.lock     (sweep's queue discovery never matches the name: it is
                                            not `lit_pull_queue*.csv`)

holding one JSON object:

    {"host": socket.gethostname(), "pid": the taker's pid, "run_id": the taker's run id,
     "started": UTC ISO time, "heartbeat": UTC ISO time, "tool": "sweep" | "runner" | ...}

Taking it: an atomic exclusive create (os.open with O_CREAT | O_EXCL), then the JSON written and
fsynced. `read(project_root)` returns the record, or None when there is no lock, and has no side
effect (a file that is not JSON, such as one caught between its create and its write, reads as
{"unreadable": True, "mtime": <its UTC ISO mtime>}).

Stale: the heartbeat is older than STALE_S (projects.json `runner.lock_stale_s` overrides the
2 h default), or the holder is on THIS host and its pid is dead (litpipe.state.pid_alive, which
never signals a process: on Windows os.kill would terminate it). An unreadable lock is stale when
its file is older than STALE_S. A live lock is never broken. A stale one is broken by os.replace to
a unique tombstone name (lit_pull_queue.lock.stale-<token>), re-read there, and the takeover goes
on only when the tombstone holds the very record judged stale (same bytes, same mtime); then the
exclusive create is tried again. When the tombstone holds another record (a racer broke the stale
lock and took it first), it is put back (a hard link, which never overwrites) and the racer's lock
reads as live. The warning names the old holder.

Holding it: the taker heartbeats from a daemon thread every STALE_S / 8. Each beat re-reads the
file and treats the lock as LOST when the file no longer holds our host, pid and run id; a taker
whose own last beat is older than STALE_S (the machine slept) does not write again, it reads its
lock as lost, since another process may have judged it stale and taken it. Callers check `lost`
(an Event) at their stage boundaries and stop that project's work as an abort. Release deletes the
file only while it still holds our host, pid and run id.

Re-entrant by run id: a lock held on THIS host with the SAME run id (LITPIPE_RUN_ID, which the
runner's stage shim sets for its children) is joined, never refused; the joiner never heartbeats
and never releases it (only its taker does). A lock this process itself holds is joined the same
way (the runner's in-process test launcher).

Known limit: sync latency. In a folder that a sync client mirrors between machines, a lock taken on
one machine may appear on the other only after the sync, so two machines can both take one
project's lock inside that window. The day/night split (interactive sweeps by day on one machine,
the scheduled runner by night on another) covers it; the lock is the guard against the overlap
that split leaves, not a distributed mutex. Windows and POSIX use the same calls (O_EXCL create,
os.replace, os.link), never fcntl or msvcrt locks.
"""
from __future__ import annotations

import json
import os
import secrets
import socket
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

LOCK_NAME = "lit_pull_queue.lock"
TOMB_INFIX = ".stale-"            # lit_pull_queue.lock.stale-<token>
TMP_INFIX = ".beat-"              # lit_pull_queue.lock.beat-<token>.tmp (a heartbeat being written)
STALE_S = 7200.0                  # 2 h; projects.json runner.lock_stale_s overrides
RUN_ENV = "LITPIPE_RUN_ID"
FIELDS = ("host", "pid", "run_id", "started", "heartbeat", "tool")
TAKE_TRIES = 20                   # create / break rounds before a contended lock reads as held
FS_RETRIES = 40                   # a Windows sharing violation (a reader has the file open) retries
FS_WAIT_S = 0.025
MISSING_RETRIES = 3               # a heartbeat that finds no file looks again (a racer's restore)
MISSING_WAIT_S = 0.1

_time = time.time                 # test seam: a fake clock for staleness
_HELD: dict = {}                  # normcased lock path -> Lock taken by this process
_HELD_GUARD = threading.Lock()


class LockHeld(Exception):
    """The project's lock is held by a live holder: `record` is its JSON record."""

    def __init__(self, path, record):
        self.path = Path(path)
        self.record = record or {}
        super().__init__(f"{self.path.parent.name}: {describe(self.record)}")


# ------------------------------------------------------------------------------ small helpers
def lock_path(project_root) -> Path:
    return Path(project_root) / LOCK_NAME


def hostname() -> str:
    return socket.gethostname()


def _iso(ts) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _epoch(s):
    try:
        d = datetime.fromisoformat(str(s).strip().replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.timestamp()


def _same_host(a, b) -> bool:
    return str(a or "").strip().casefold() == str(b or "").strip().casefold()


def _key(path) -> str:
    return os.path.normcase(os.path.abspath(str(path)))


def _warn(msg):
    print(f"[lock] {msg}", file=sys.stderr, flush=True)


def _retry_fs(fn, *args):
    """fn(*args), retrying a PermissionError (Windows: another process has the file open for a
    moment) a few times; FileNotFoundError and FileExistsError pass straight through."""
    for i in range(FS_RETRIES):
        try:
            return fn(*args)
        except (FileNotFoundError, FileExistsError):
            raise
        except PermissionError:
            if i == FS_RETRIES - 1:
                raise
            time.sleep(FS_WAIT_S)


def _inspect(path):
    """(raw bytes, mtime_ns, record) of the file at `path`, or None when there is none. `record` is
    the parsed object, or {"unreadable": True, "mtime": ISO} when the bytes are not a JSON object."""
    path = Path(path)
    for i in range(FS_RETRIES):
        try:
            with open(path, "rb") as f:
                raw = f.read()
                mt = os.fstat(f.fileno()).st_mtime_ns
            break
        except FileNotFoundError:
            return None
        except PermissionError:
            if i == FS_RETRIES - 1:
                raise
            time.sleep(FS_WAIT_S)
        except OSError:
            return None
    try:
        rec = json.loads(raw.decode("utf-8"))
        if not isinstance(rec, dict):
            raise ValueError
    except (UnicodeDecodeError, ValueError):
        rec = {"unreadable": True, "mtime": _iso(mt / 1e9)}
    return raw, mt, rec


def read(project_root) -> dict | None:
    """The project's lock record, or None when it holds no lock. Reads only: no side effect."""
    got = _inspect(lock_path(project_root))
    return None if got is None else got[2]


def age_s(record, now=None):
    """Seconds since the record's heartbeat (an unreadable record: since its file's mtime), or None."""
    now = _time() if now is None else now
    t = _epoch((record or {}).get("mtime" if (record or {}).get("unreadable") else "heartbeat"))
    if t is None and not (record or {}).get("unreadable"):
        t = _epoch((record or {}).get("started"))
    return None if t is None else max(0.0, now - t)


def _fmt_age(s):
    if s is None:
        return "an unknown time"
    if s < 120:
        return f"{int(s)} s"
    if s < 7200:
        return f"{int(s // 60)} min"
    return f"{s / 3600:.1f} h"


def describe(record, now=None) -> str:
    """One line naming the holder: host, tool, pid, run id and heartbeat age."""
    r = record or {}
    if r.get("unreadable"):
        return f"lock file unreadable (written {_fmt_age(age_s(r, now))} ago)"
    return (f"lock held by host {r.get('host') or '?'}, {r.get('tool') or '?'} (pid {r.get('pid') or '?'}, "
            f"run {r.get('run_id') or '?'}), heartbeat {_fmt_age(age_s(r, now))} ago")


def _pid_alive(pid) -> bool:
    try:
        from litpipe import state
        return state.pid_alive(int(pid))
    except (TypeError, ValueError):
        return False
    except Exception:   # noqa: BLE001 - liveness unknown: read as alive (never break a live lock)
        return True


def is_stale(record, *, stale_s=None, now=None):
    """(stale, why) for a lock record (see the module docstring)."""
    stale_s = STALE_S if stale_s is None else float(stale_s)
    r = record or {}
    age = age_s(r, now)
    if age is None:
        return False, "no heartbeat time to judge"
    if age >= stale_s:
        return True, f"heartbeat {_fmt_age(age)} old (stale after {_fmt_age(stale_s)})"
    if not r.get("unreadable") and _same_host(r.get("host"), hostname()) and r.get("pid") is not None \
            and not _pid_alive(r.get("pid")):
        return True, f"its process {r.get('pid')} on this host is gone"
    return False, "live"


def stale_s_from(cfg=None) -> float:
    """projects.json runner.lock_stale_s (seconds, > 0), else STALE_S. litpipe.config.ConfigError on
    a value that is not a positive number."""
    from litpipe import config
    b = config.load(cfg).get("runner") or {}
    if not isinstance(b, dict):
        return STALE_S
    v = b.get("lock_stale_s")
    if v is None:
        return STALE_S
    if isinstance(v, bool) or not isinstance(v, (int, float)) or v <= 0:
        raise config.ConfigError(f"runner.lock_stale_s must be a positive number of seconds, got {v!r}")
    return float(v)


def _write_new(path, data: bytes):
    """The atomic exclusive create: O_CREAT | O_EXCL, then the bytes, fsynced. FileExistsError when
    a lock is there."""
    fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0), 0o644)
    try:
        os.write(fd, data)
        os.fsync(fd)
    except BaseException:
        os.close(fd)
        try:
            os.unlink(path)
        except OSError:
            pass
        raise
    os.close(fd)


def _unlink(path):
    try:
        _retry_fs(os.unlink, str(path))
    except FileNotFoundError:
        pass


def _restore(tomb, path):
    """Put a record moved aside by mistake back, never over a lock someone created meanwhile."""
    try:
        os.link(str(tomb), str(path))
    except FileExistsError:
        # A third process holds the path now; the moved record's taker reads its lock as lost at
        # its next heartbeat and stops.
        _unlink(tomb)
        return
    except OSError:                       # no hard links on this filesystem
        if not Path(path).exists():
            try:
                _retry_fs(os.replace, str(tomb), str(path))
                return
            except OSError:
                pass
        _unlink(tomb)
        return
    _unlink(tomb)


# ------------------------------------------------------------------------------ the lock
class Lock:
    """One project's lock. `acquire()` takes or joins it (LockHeld when a live holder has it);
    `release()` gives it back. Use as a context manager. See the module docstring."""

    def __init__(self, project_root, *, tool, run_id=None, stale_s=None, heartbeat=True):
        self.root = Path(project_root)
        self.path = lock_path(self.root)
        self.tool = str(tool)
        env_id = os.environ.get(RUN_ENV) or None
        self.run_id = str(run_id or env_id or f"{self.tool.replace(' ', '_')}-{os.getpid()}-{secrets.token_hex(4)}")
        self._joinable_id = bool(run_id or env_id)
        self.stale_s = STALE_S if stale_s is None else float(stale_s)
        self.heartbeat = bool(heartbeat)
        self.host = hostname()
        self.pid = os.getpid()
        self.record = None
        self.joined = False
        self.held = False
        self.broke = None                 # the stale record this take broke, when it broke one
        self.lost = threading.Event()
        self.lost_reason = ""
        self.last_beat = None
        self.beats = 0
        self._stop = threading.Event()
        self._thread = None

    # -- who holds it
    def _ours(self, rec) -> bool:
        return bool(rec) and not rec.get("unreadable") and _same_host(rec.get("host"), self.host) \
            and str(rec.get("pid")) == str(self.pid) and str(rec.get("run_id")) == self.run_id

    def _joinable(self, rec) -> bool:
        if not rec or rec.get("unreadable"):
            return False
        with _HELD_GUARD:
            mine = _HELD.get(_key(self.path))
        if mine is not None and mine is not self and mine._ours(rec):
            return True
        return self._joinable_id and _same_host(rec.get("host"), self.host) \
            and str(rec.get("run_id") or "") == self.run_id

    # -- take, beat, give back
    def acquire(self):
        now = _time()
        rec = {"host": self.host, "pid": self.pid, "run_id": self.run_id, "started": _iso(now),
               "heartbeat": _iso(now), "tool": self.tool}
        data = json.dumps(rec, ensure_ascii=False).encode("utf-8")
        for _ in range(TAKE_TRIES):
            try:
                _write_new(self.path, data)
            except FileExistsError:
                pass
            else:
                self.record, self.held, self.last_beat = rec, True, now
                with _HELD_GUARD:
                    _HELD[_key(self.path)] = self
                if self.heartbeat:
                    self._thread = threading.Thread(target=self._loop, name="litpipe-lock-heartbeat", daemon=True)
                    self._thread.start()
                return self
            got = _inspect(self.path)
            if got is None:
                continue                  # released or broken between our create and our read
            raw, mt, cur = got
            if self._joinable(cur):
                self.record, self.joined = cur, True
                return self
            stale, why = is_stale(cur, stale_s=self.stale_s)
            if not stale:
                raise LockHeld(self.path, cur)
            tomb = self.path.with_name(f"{LOCK_NAME}{TOMB_INFIX}{secrets.token_hex(6)}")
            try:
                _retry_fs(os.replace, str(self.path), str(tomb))
            except FileNotFoundError:
                continue                  # a racer broke it first: try the create again
            moved = _inspect(tomb)
            if moved is not None and moved[0] == raw and moved[1] == mt:
                _warn(f"{self.root.name}: breaking a stale lock ({why}); old holder: {describe(cur)}")
                self.broke = cur
                _unlink(tomb)
                continue
            _restore(tomb, self.path)     # a racer's fresh lock: put it back; it reads as live next round
        cur = read(self.root)
        raise LockHeld(self.path, cur or {"unreadable": True, "mtime": _iso(_time())})

    def _mark_lost(self, why):
        if not self.lost.is_set():
            self.lost_reason = why
            self.lost.set()
            _warn(f"{self.root.name}: lock LOST ({why}); this project's work stops")

    def beat(self) -> bool:
        """One heartbeat: True when written, False when the lock is lost (or was never ours)."""
        if not self.held or self.lost.is_set():
            return False
        now = _time()
        if self.last_beat is not None and now - self.last_beat >= self.stale_s:
            self._mark_lost(f"our own heartbeat lapsed {_fmt_age(now - self.last_beat)} (stale after "
                            f"{_fmt_age(self.stale_s)}): another process may have taken it")
            return False
        cur = None
        for i in range(MISSING_RETRIES):
            got = _inspect(self.path)
            cur = None if got is None else got[2]
            if cur is not None:
                break
            time.sleep(MISSING_WAIT_S)
        if not self._ours(cur):
            self._mark_lost("the lock file is gone" if cur is None else f"the file now holds another: {describe(cur, now)}")
            return False
        rec = dict(cur)
        rec["heartbeat"] = _iso(now)
        tmp = self.path.with_name(f"{LOCK_NAME}{TMP_INFIX}{secrets.token_hex(4)}.tmp")
        try:
            with open(tmp, "wb") as f:
                f.write(json.dumps(rec, ensure_ascii=False).encode("utf-8"))
                f.flush()
                os.fsync(f.fileno())
            _retry_fs(os.replace, str(tmp), str(self.path))
        except OSError as e:              # skipped this beat; the next one tries again
            _unlink(tmp)
            _warn(f"{self.root.name}: heartbeat not written ({type(e).__name__}); retrying at the next beat")
            return True
        self.record, self.last_beat = rec, now
        self.beats += 1
        return True

    def _loop(self):
        interval = max(0.05, self.stale_s / 8.0)
        while not self._stop.wait(interval):
            if not self.beat():
                return

    def release(self) -> bool:
        """Delete the file when it still holds our host, pid and run id. A joiner releases nothing."""
        if self._thread is not None:
            self._stop.set()
            self._thread.join(timeout=5)
            self._thread = None
        if not self.held:
            return False
        self.held = False
        with _HELD_GUARD:
            if _HELD.get(_key(self.path)) is self:
                del _HELD[_key(self.path)]
        got = _inspect(self.path)
        if got is not None and self._ours(got[2]):
            _unlink(self.path)
            return True
        return False

    def __enter__(self):
        return self.acquire()

    def __exit__(self, *exc):
        self.release()
        return False


def acquire(project_root, *, tool, run_id=None, stale_s=None, heartbeat=True) -> Lock:
    """Take (or join) the project's lock; LockHeld when a live holder has it."""
    return Lock(project_root, tool=tool, run_id=run_id, stale_s=stale_s, heartbeat=heartbeat).acquire()
