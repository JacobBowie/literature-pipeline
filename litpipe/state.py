"""Shared cross-process state: host pacing, daily budgets, deferrals, refusals, the run registry
and a small key-value store (dispatch 0.5 "State"; plan R1 and REG-I27).

Stages still run as separate processes and people still run the CLIs by hand, so per-host pacing
cannot live inside one process. Everything here sits in one stdlib sqlite3 file,
`config.state_dir()/litpipe_state.sqlite`, in WAL mode. Every read-modify-write runs inside
`BEGIN IMMEDIATE`, which takes the write lock before the read, so two processes can never both
read the same "next allowed" time and both claim it (tested: 2 and 4 processes keep every gap at
or above the interval). WAL only works when every process is on the same host and the file is on
a local disk (sqlite.org/wal.html); the default state_dir is local, off the Drive mirror.

Pacing (`acquire` / `release`):
  acquire(host) blocks until the host's next-allowed time and a free concurrency lease, then claims
  one slot: next-allowed = now + interval, the day's counter + 1, and a lease row. It raises
  BudgetExhausted when the daily budget (UTC day) is spent and HostDeferred (a BudgetExhausted
  subclass, so a caller that maps BudgetExhausted to DEFERRED handles it too) while the host is
  deferred. release(host, ok) drops the lease and sets next-allowed to at least end + interval, so
  spacing counts from the END of the previous attempt, as the S2 probe measured it: a slow response
  never lets the next request follow it closely. Interval, budget and concurrency come from
  `litpipe.hosts.policy(host)` (fields min_interval_s, daily_budget, concurrency) unless the
  caller passes them; without litpipe.hosts, the unknown-host default (2 s, no budget, 1).
  The exceptions carry `until` (epoch) and `retry_after` (seconds). The budget day is the UTC day.
  Always pair acquire with release in a `finally`: a lease that is never released holds the host
  until LEASE_S passes or the holding process exits.

Refusals: `refuse(host, reason, persistence="run")` holds while the refusing run is live and clears
when it finishes (or dies); "manual" survives every run until `clear_refusal(host)` or
`python -m litpipe.state --clear-refusal HOST`. A stage subprocess joins its runner's run through
the LITPIPE_RUN_ID environment variable (runtime context passed parent to child, not config);
with no run at all, a "run" refusal lasts as long as the refusing process.

Runs: `register_run(kind)` records pid, kind and a heartbeat; `live_runs()` drops runs whose pid is
gone or whose heartbeat is older than HEARTBEAT_STALE_S. Process liveness on Windows uses
OpenProcess + GetExitCodeProcess, never os.kill: on Windows os.kill(pid, 0) calls TerminateProcess
and kills the process (docs.python.org os.kill).

Tests point DB_PATH at a temp file (or monkeypatch litpipe.config.CONFIG_PATH to a registry whose
state_dir is a temp dir) and may replace `_time` and `_sleep` with a fake clock.
"""
import argparse
import json
import os
import re
import secrets
import sqlite3
import sys
import time
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

DB_NAME = "litpipe_state.sqlite"
DB_PATH = None               # a path overrides config.state_dir()/DB_NAME (tests, --db)
BUSY_TIMEOUT_S = 30.0        # sqlite busy handler: how long a claim waits for another writer
DEFAULT_INTERVAL_S = 2.0     # a host with no policy row (dispatch W1-A1: unknown hosts get 2 s)
LEASE_S = 300.0              # a lease whose holder never released it expires after this
HEARTBEAT_STALE_S = 1800.0   # a run silent this long is not live (the runner heartbeats more often)
POLL_S = 0.05                # re-check interval while every concurrency lease is taken
CLOCK_SLACK_S = 1.0          # next-allowed further ahead than interval + this means the clock stepped back
RUN_ENV = "LITPIPE_RUN_ID"
SCHEMA_VERSION = 1

# Clock and sleep, replaceable by tests (a fake clock makes pacing tests take milliseconds).
_time = time.time
_sleep = time.sleep
_before_claim = None         # test seam, called inside the claim transaction before the write

_IS_WINDOWS = sys.platform == "win32"
_current_run = None
_POLICY = object()          # "look it up in litpipe.hosts"

_SCHEMA = (
    """CREATE TABLE IF NOT EXISTS hosts (
        host             TEXT PRIMARY KEY,
        next_ok          REAL NOT NULL DEFAULT 0,
        interval_s       REAL NOT NULL DEFAULT 0,
        day              TEXT,
        day_count        INTEGER NOT NULL DEFAULT 0,
        deferred_until   REAL,
        defer_reason     TEXT,
        refused          TEXT,
        refused_reason   TEXT,
        refused_run      TEXT,
        refused_pid      INTEGER,
        refused_at       REAL,
        consecutive_fail INTEGER NOT NULL DEFAULT 0,
        last_release     REAL)""",
    """CREATE TABLE IF NOT EXISTS leases (
        id      INTEGER PRIMARY KEY AUTOINCREMENT,
        host    TEXT NOT NULL,
        pid     INTEGER NOT NULL,
        run_id  TEXT,
        started REAL NOT NULL,
        expires REAL NOT NULL)""",
    "CREATE INDEX IF NOT EXISTS leases_host ON leases(host)",
    """CREATE TABLE IF NOT EXISTS runs (
        run_id    TEXT PRIMARY KEY,
        pid       INTEGER NOT NULL,
        kind      TEXT NOT NULL,
        writer    INTEGER NOT NULL DEFAULT 1,
        started   REAL NOT NULL,
        heartbeat REAL NOT NULL,
        finished  REAL,
        status    TEXT)""",
    """CREATE TABLE IF NOT EXISTS kv (
        ns      TEXT NOT NULL,
        key     TEXT NOT NULL,
        value   TEXT NOT NULL,
        expires REAL,
        updated REAL NOT NULL,
        PRIMARY KEY (ns, key))""",
)


# ------------------------------------------------------------------------------ exceptions
class HostUnavailable(RuntimeError):
    """The host cannot be used now. `until` is an epoch time (None when unknown); `retry_after`
    is the seconds from the raise to `until` (what net.request puts on its DEFERRED Outcome)."""

    def __init__(self, host, until=None, reason=""):
        self.host, self.until, self.reason = host, until, reason
        self.retry_after = None if until is None else max(0.0, round(until - _time(), 3))
        when = f" until {_iso(until)}" if until else ""
        super().__init__(f"{host}: {reason}{when}")


class BudgetExhausted(HostUnavailable):
    """The host's daily budget (UTC day) is spent; `until` is the next UTC midnight."""


class HostDeferred(BudgetExhausted):
    """The host is deferred (a long Retry-After); `until` is when it may be used again.
    Subclasses BudgetExhausted so a caller that turns BudgetExhausted into DEFERRED handles it."""


@dataclass(frozen=True)
class Slot:
    """What acquire claimed: pass it back to release to drop exactly this lease."""
    host: str
    lease_id: int
    start: float


# ------------------------------------------------------------------------------ helpers
def _iso(ts):
    return datetime.fromtimestamp(ts, timezone.utc).isoformat(timespec="seconds")


def _day(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d")


def _next_midnight(ts):
    d = datetime.fromtimestamp(ts, timezone.utc).date() + timedelta(days=1)
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp()


def _norm_host(host):
    """A bare host name, lower-case; a URL is reduced to its host."""
    h = str(host).strip()
    if "://" in h:
        from urllib.parse import urlsplit
        h = urlsplit(h).hostname or ""
    h = h.lower().rstrip(".")
    if not h:
        raise ValueError(f"not a host: {host!r}")
    return h


def _epoch(until):
    if isinstance(until, datetime):
        if until.tzinfo is None:
            raise ValueError("defer(until=datetime) needs a timezone-aware datetime")
        return until.timestamp()
    return float(until)


def _redact(text):
    """Scrub contact emails and keys before a string is persisted: the one implementation,
    litpipe.ledger.redact (dispatch 0.5). Imported lazily: ledger imports config, not state."""
    if text is None:
        return None
    from litpipe.ledger import redact
    return redact(str(text))


def _policy_values(host):
    """(interval_s, daily_budget, concurrency) from litpipe.hosts, or the unknown-host default."""
    try:
        import litpipe.hosts as _hosts  # dotted form: a missing module raises ModuleNotFoundError
    except ModuleNotFoundError as e:
        if e.name != "litpipe.hosts":
            raise
        return DEFAULT_INTERVAL_S, None, 1
    pol = _hosts.policy(host)  # takes a URL or a bare host name
    interval = getattr(pol, "min_interval_s", DEFAULT_INTERVAL_S)
    concurrency = getattr(pol, "concurrency", 1)
    return (DEFAULT_INTERVAL_S if interval is None else float(interval),
            getattr(pol, "daily_budget", None),
            1 if not concurrency else int(concurrency))


# ------------------------------------------------------------------------------ process liveness
PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
STILL_ACTIVE = 259
ERROR_ACCESS_DENIED = 5
_k32 = None


def _kernel32():
    global _k32
    if _k32 is None:
        import ctypes
        from ctypes import wintypes
        k = ctypes.WinDLL("kernel32", use_last_error=True)
        k.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        k.OpenProcess.restype = wintypes.HANDLE
        k.GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
        k.GetExitCodeProcess.restype = wintypes.BOOL
        k.CloseHandle.argtypes = [wintypes.HANDLE]
        k.CloseHandle.restype = wintypes.BOOL
        _k32 = k
    return _k32


def _win_pid_alive(pid):
    import ctypes
    from ctypes import wintypes
    k = _kernel32()
    h = k.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
    if not h:
        # ERROR_INVALID_PARAMETER (87): no such process. ACCESS_DENIED: it exists but is not ours
        # to open (System, CSRSS, an elevated process), so it is alive.
        return ctypes.get_last_error() == ERROR_ACCESS_DENIED
    try:
        code = wintypes.DWORD()
        if not k.GetExitCodeProcess(h, ctypes.byref(code)):
            return True  # the process object exists; unknown state reads as alive (conservative)
        # A process that exited with code 259 reads as alive (documented GetExitCodeProcess
        # caveat); the heartbeat filter in live_runs covers it.
        return code.value == STILL_ACTIVE
    finally:
        k.CloseHandle(h)


def pid_alive(pid) -> bool:
    """Is a process with this pid running? Never signals it (see the module docstring)."""
    if pid is None:
        return False
    pid = int(pid)
    if pid <= 0 or pid > 0xFFFFFFFF:
        return False
    if pid == os.getpid():
        return True
    if _IS_WINDOWS:
        return _win_pid_alive(pid)
    try:
        os.kill(pid, 0)  # POSIX only: signal 0 checks existence and permission, sends nothing
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


# ------------------------------------------------------------------------------ connection
def db_path(create=True) -> Path:
    """The state file: DB_PATH when set, else config.state_dir()/litpipe_state.sqlite."""
    if DB_PATH is not None:
        return Path(DB_PATH)
    from litpipe import config
    return config.state_dir(create=create) / DB_NAME


def _connect():
    p = db_path()
    p.parent.mkdir(parents=True, exist_ok=True)  # created on first use, DB_PATH included
    # isolation_level=None: the sqlite3 module opens no implicit transactions, so the explicit
    # BEGIN IMMEDIATE below is the only one (works on 3.11; `autocommit` is 3.12+).
    con = sqlite3.connect(str(p), timeout=BUSY_TIMEOUT_S, isolation_level=None)
    try:
        if con.execute("PRAGMA user_version").fetchone()[0] != SCHEMA_VERSION:
            _init(con, p)
        con.execute("PRAGMA synchronous=NORMAL")
    except BaseException:
        con.close()
        raise
    return con


def _init(con, p):
    mode = con.execute("PRAGMA journal_mode=WAL").fetchone()[0]
    if str(mode).lower() != "wal":
        raise RuntimeError(f"litpipe state at {p} could not enter WAL mode (got {mode!r}); "
                           "the state file must be on a local disk (set state_dir in projects.json)")
    con.execute("BEGIN IMMEDIATE")
    try:
        for stmt in _SCHEMA:
            con.execute(stmt)
        con.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise


class _Tx:
    """`with _Tx() as con:` one BEGIN IMMEDIATE transaction on a fresh connection."""

    def __enter__(self):
        self.con = _connect()
        try:
            self.con.execute("BEGIN IMMEDIATE")
        except BaseException:
            self.con.close()
            raise
        return self.con

    def __exit__(self, exc_type, exc, tb):
        try:
            self.con.execute("ROLLBACK" if exc_type else "COMMIT")
        finally:
            self.con.close()
        return False


def _read():
    return closing(_connect())


# ------------------------------------------------------------------------------ pacing
def acquire(host, *, interval=_POLICY, budget=_POLICY, concurrency=_POLICY, run_id=None,
            lease_s=None) -> Slot:
    """Block until `host` may be called, then claim the slot (see the module docstring).

    interval: minimum seconds between attempts; budget: attempts per UTC day (None = unlimited);
    concurrency: simultaneous leases. Each defaults to the host's litpipe.hosts policy.
    Raises BudgetExhausted when the day's budget is spent, HostDeferred while the host is deferred.
    """
    host = _norm_host(host)
    if _POLICY in (interval, budget, concurrency):
        pol = _policy_values(host)
        interval = pol[0] if interval is _POLICY else interval
        budget = pol[1] if budget is _POLICY else budget
        concurrency = pol[2] if concurrency is _POLICY else concurrency
    interval = max(0.0, float(interval))
    concurrency = max(1, int(concurrency))
    lease_s = LEASE_S if lease_s is None else float(lease_s)
    run_id = run_id or current_run()
    while True:
        got = _try_claim(host, interval, budget, concurrency, lease_s, run_id)
        if isinstance(got, Slot):
            return got
        _sleep(got)


def _try_claim(host, interval, budget, concurrency, lease_s, run_id):
    """One BEGIN IMMEDIATE pass: a Slot when claimed, else the seconds to wait."""
    pid = os.getpid()
    with _Tx() as con:
        now = _time()
        row = con.execute("SELECT next_ok, day, day_count, deferred_until, defer_reason "
                          "FROM hosts WHERE host=?", (host,)).fetchone()
        if row is None:
            con.execute("INSERT INTO hosts(host) VALUES (?)", (host,))
            row = (0.0, None, 0, None, None)
        next_ok, day, count, deferred_until, defer_reason = row
        if deferred_until is not None and deferred_until > now:
            raise HostDeferred(host, deferred_until, defer_reason or "deferred")
        today = _day(now)
        if day != today:
            count = 0
        if budget is not None and count >= budget:
            raise BudgetExhausted(host, _next_midnight(now), f"daily budget {budget} spent")

        con.execute("DELETE FROM leases WHERE host=? AND expires<=?", (host, now))
        leases = con.execute("SELECT id, pid FROM leases WHERE host=?", (host,)).fetchall()
        if len(leases) >= concurrency:
            dead = [lid for lid, lpid in leases if lpid != pid and not pid_alive(lpid)]
            for lid in dead:
                con.execute("DELETE FROM leases WHERE id=?", (lid,))
            if len(leases) - len(dead) >= concurrency:
                return POLL_S

        if next_ok - now > interval + CLOCK_SLACK_S:  # the wall clock stepped back
            con.execute("UPDATE hosts SET next_ok=? WHERE host=?", (now + interval, host))
            return interval
        if now < next_ok:
            return next_ok - now

        if _before_claim is not None:
            _before_claim()  # test seam: widens the read-to-write window the lock must cover
        cur = con.execute("INSERT INTO leases(host, pid, run_id, started, expires) "
                          "VALUES (?, ?, ?, ?, ?)", (host, pid, run_id, now, now + lease_s))
        con.execute("UPDATE hosts SET next_ok=?, interval_s=?, day=?, day_count=? WHERE host=?",
                    (now + interval, interval, today, count + 1, host))
        return Slot(host, cur.lastrowid, now)


def release(host, ok=True, slot=None):
    """End an attempt: drop its lease and push next-allowed to at least now + interval."""
    host = _norm_host(host)
    pid = os.getpid()
    with _Tx() as con:
        now = _time()
        if slot is not None:
            lid = slot.lease_id if isinstance(slot, Slot) else int(slot)
            con.execute("DELETE FROM leases WHERE id=?", (lid,))
        else:
            row = con.execute("SELECT id FROM leases WHERE host=? AND pid=? ORDER BY started, id "
                              "LIMIT 1", (host, pid)).fetchone()
            if row:
                con.execute("DELETE FROM leases WHERE id=?", (row[0],))
        row = con.execute("SELECT next_ok, interval_s, consecutive_fail FROM hosts WHERE host=?",
                          (host,)).fetchone()
        if row is not None:
            next_ok, interval_s, fails = row
            con.execute("UPDATE hosts SET next_ok=?, consecutive_fail=?, last_release=? "
                        "WHERE host=?",
                        (max(next_ok, now + interval_s), 0 if ok else fails + 1, now, host))


def day_count(host) -> int:
    """Attempts claimed for `host` on the current UTC day."""
    host = _norm_host(host)
    with _read() as con:
        row = con.execute("SELECT day, day_count FROM hosts WHERE host=?", (host,)).fetchone()
    return int(row[1]) if row and row[0] == _day(_time()) else 0


def defer(host, until, reason=""):
    """Hold the host until `until` (epoch seconds or an aware datetime); the later deferral wins."""
    host = _norm_host(host)
    until = _epoch(until)
    with _Tx() as con:
        con.execute("INSERT INTO hosts(host, deferred_until, defer_reason) VALUES (?, ?, ?) "
                    "ON CONFLICT(host) DO UPDATE SET "
                    "defer_reason = CASE WHEN COALESCE(deferred_until, 0) < excluded.deferred_until "
                    "  THEN excluded.defer_reason ELSE defer_reason END, "
                    "deferred_until = MAX(COALESCE(deferred_until, 0), excluded.deferred_until)",
                    (host, until, _redact(reason)))


def deferred_until(host):
    """The epoch time the host is deferred to, or None when it is not deferred now."""
    host = _norm_host(host)
    with _read() as con:
        row = con.execute("SELECT deferred_until FROM hosts WHERE host=?", (host,)).fetchone()
    return row[0] if row and row[0] is not None and row[0] > _time() else None


def clear_deferral(host) -> bool:
    host = _norm_host(host)
    with _Tx() as con:
        cur = con.execute("UPDATE hosts SET deferred_until=NULL, defer_reason=NULL "
                          "WHERE host=? AND deferred_until IS NOT NULL", (host,))
        return cur.rowcount > 0


# ------------------------------------------------------------------------------ refusals
def refuse(host, reason, persistence="run", run_id=None):
    """Mark the host refused. "run": until the refusing run ends; "manual": until clear_refusal.
    A manual refusal is never downgraded to a run refusal."""
    if persistence not in ("run", "manual"):
        raise ValueError(f"persistence must be 'run' or 'manual', got {persistence!r}")
    host = _norm_host(host)
    run_id = run_id or current_run()
    with _Tx() as con:
        now = _time()
        row = con.execute("SELECT refused FROM hosts WHERE host=?", (host,)).fetchone()
        if row is None:
            con.execute("INSERT INTO hosts(host) VALUES (?)", (host,))
        elif row[0] == "manual" and persistence == "run":
            return
        con.execute("UPDATE hosts SET refused=?, refused_reason=?, refused_run=?, refused_pid=?, "
                    "refused_at=? WHERE host=?",
                    (persistence, _redact(reason), run_id, os.getpid(), now, host))


def _run_live(con, run_id, fallback_pid, now, stale_s):
    row = con.execute("SELECT pid, heartbeat, finished FROM runs WHERE run_id=?",
                      (run_id,)).fetchone()
    if row is None:
        return pid_alive(fallback_pid)
    pid, beat, finished = row
    return finished is None and beat >= now - stale_s and pid_alive(pid)


def _refusal_holds(con, refused, run_id, pid, now):
    if refused == "manual":
        return True
    if refused != "run":
        return False
    if run_id:
        return _run_live(con, run_id, pid, now, HEARTBEAT_STALE_S)
    return pid_alive(pid)


def is_refused(host) -> bool:
    host = _norm_host(host)
    with _read() as con:
        row = con.execute("SELECT refused, refused_run, refused_pid FROM hosts WHERE host=?",
                          (host,)).fetchone()
        return bool(row) and _refusal_holds(con, row[0], row[1], row[2], _time())


def clear_refusal(host) -> bool:
    """Clear any refusal of `host`; True when one was set."""
    host = _norm_host(host)
    with _Tx() as con:
        cur = con.execute("UPDATE hosts SET refused=NULL, refused_reason=NULL, refused_run=NULL, "
                          "refused_pid=NULL, refused_at=NULL WHERE host=? AND refused IS NOT NULL",
                          (host,))
        return cur.rowcount > 0


# ------------------------------------------------------------------------------ runs
def current_run():
    """This process's run: the one it registered, else the one its parent passed in LITPIPE_RUN_ID."""
    return _current_run or os.environ.get(RUN_ENV) or None


_KIND = re.compile(r"[^a-z0-9_-]+")


def register_run(kind, *, writer=True) -> str:
    """Record a run of this process and make it current. writer=False for read-only runs
    (status, canaries) that must not block the writer check."""
    global _current_run
    kind_s = _KIND.sub("-", str(kind).strip().lower()).strip("-")
    if not kind_s:
        raise ValueError(f"run kind must be a non-empty name, got {kind!r}")
    pid = os.getpid()
    with _Tx() as con:
        now = _time()
        _mark_abandoned(con, now)
        run_id = (f"{datetime.fromtimestamp(now, timezone.utc):%Y%m%dT%H%M%SZ}-{kind_s}-{pid}-"
                  f"{secrets.token_hex(3)}")
        con.execute("INSERT INTO runs(run_id, pid, kind, writer, started, heartbeat) "
                    "VALUES (?, ?, ?, ?, ?, ?)", (run_id, pid, kind_s, int(bool(writer)), now, now))
    _current_run = run_id
    return run_id


def _mark_abandoned(con, now):
    """Close unfinished runs whose process is gone or whose heartbeat is stale; drop their run
    refusals and leases. Called inside a write transaction."""
    rows = con.execute("SELECT run_id, pid, heartbeat FROM runs WHERE finished IS NULL").fetchall()
    for run_id, pid, beat in rows:
        if beat < now - HEARTBEAT_STALE_S or not pid_alive(pid):
            con.execute("UPDATE runs SET finished=?, status='abandoned' WHERE run_id=?",
                        (now, run_id))
            _end_run_effects(con, run_id)


def _end_run_effects(con, run_id):
    con.execute("UPDATE hosts SET refused=NULL, refused_reason=NULL, refused_run=NULL, "
                "refused_pid=NULL, refused_at=NULL WHERE refused='run' AND refused_run=?",
                (run_id,))
    con.execute("DELETE FROM leases WHERE run_id=?", (run_id,))


def heartbeat(run_id=None) -> bool:
    """Refresh a live run's heartbeat; False when the run is unknown or finished."""
    run_id = run_id or current_run()
    if not run_id:
        return False
    with _Tx() as con:
        cur = con.execute("UPDATE runs SET heartbeat=? WHERE run_id=? AND finished IS NULL",
                          (_time(), run_id))
        return cur.rowcount > 0


def finish_run(run_id=None, status="ok"):
    """Close a run; its "run" refusals and any leases it still holds end with it."""
    global _current_run
    run_id = run_id or current_run()
    if not run_id:
        return
    with _Tx() as con:
        con.execute("UPDATE runs SET finished=?, status=? WHERE run_id=? AND finished IS NULL",
                    (_time(), _redact(status), run_id))
        _end_run_effects(con, run_id)
    if _current_run == run_id:
        _current_run = None


def live_runs(stale_s=None) -> list[dict]:
    """Unfinished runs whose process is alive and whose heartbeat is fresh, oldest first."""
    stale_s = HEARTBEAT_STALE_S if stale_s is None else float(stale_s)
    with _read() as con:
        now = _time()
        rows = con.execute("SELECT run_id, pid, kind, writer, started, heartbeat FROM runs "
                           "WHERE finished IS NULL ORDER BY started").fetchall()
    out = []
    for run_id, pid, kind, writer, started, beat in rows:
        if beat < now - stale_s or not pid_alive(pid):
            continue
        out.append({"run_id": run_id, "pid": pid, "kind": kind, "writer": bool(writer),
                    "started": _iso(started), "heartbeat_age_s": round(now - beat, 1)})
    return out


# ------------------------------------------------------------------------------ key-value
def kv_set(ns, key, value, ttl_s=None):
    """Store a JSON-serialisable value; with ttl_s it reads as absent after that many seconds."""
    text = json.dumps(value, ensure_ascii=False)
    with _Tx() as con:
        now = _time()
        con.execute("INSERT INTO kv(ns, key, value, expires, updated) VALUES (?, ?, ?, ?, ?) "
                    "ON CONFLICT(ns, key) DO UPDATE SET value=excluded.value, "
                    "expires=excluded.expires, updated=excluded.updated",
                    (str(ns), str(key), text, None if ttl_s is None else now + float(ttl_s), now))


def kv_get(ns, key, default=None):
    with _read() as con:
        row = con.execute("SELECT value, expires FROM kv WHERE ns=? AND key=?",
                          (str(ns), str(key))).fetchone()
    if row is None or (row[1] is not None and row[1] <= _time()):
        return default
    return json.loads(row[0])


# ------------------------------------------------------------------------------ status + CLI
def status() -> dict:
    """A snapshot for --status and preflight: hosts, live runs, leases."""
    with _read() as con:
        now = _time()
        hosts = []
        for (host, next_ok, interval_s, day, count, until, dreason, refused, rreason, rrun, rpid,
             rat, fails) in con.execute(
                "SELECT host, next_ok, interval_s, day, day_count, deferred_until, defer_reason, "
                "refused, refused_reason, refused_run, refused_pid, refused_at, consecutive_fail "
                "FROM hosts ORDER BY host").fetchall():
            holds = bool(refused) and _refusal_holds(con, refused, rrun, rpid, now)
            hosts.append({
                "host": host, "interval_s": interval_s,
                "day_count": count if day == _day(now) else 0,
                "next_ok_in_s": round(max(0.0, next_ok - now), 2),
                "deferred_until": _iso(until) if until and until > now else None,
                "defer_reason": dreason if until and until > now else None,
                "refused": refused if holds else None,
                "refused_reason": rreason if holds else None,
                "refused_at": _iso(rat) if holds and rat else None,
                "consecutive_fail": fails})
        leases = [{"host": h, "pid": p, "run_id": r, "age_s": round(now - s, 1)}
                  for h, p, r, s in con.execute(
                      "SELECT host, pid, run_id, started FROM leases WHERE expires>? "
                      "ORDER BY host, started", (now,)).fetchall()]
        kv_n = con.execute("SELECT COUNT(*) FROM kv").fetchone()[0]
    return {"db": str(db_path(create=False)), "hosts": hosts, "leases": leases,
            "live_runs": live_runs(), "kv_entries": kv_n}


def _print_status(st):
    print(f"state file: {st['db']}")
    print(f"live runs: {len(st['live_runs'])}")
    for r in st["live_runs"]:
        role = "writer" if r["writer"] else "reader"
        print(f"  {r['run_id']}  {r['kind']}  pid {r['pid']}  {role}  "
              f"heartbeat {r['heartbeat_age_s']}s ago")
    print(f"hosts: {len(st['hosts'])}")
    for h in st["hosts"]:
        flags = []
        if h["refused"]:
            flags.append(f"REFUSED ({h['refused']}): {h['refused_reason']}")
        if h["deferred_until"]:
            flags.append(f"deferred until {h['deferred_until']}: {h['defer_reason']}")
        print(f"  {h['host']:<32} today {h['day_count']:>6}  interval {h['interval_s']:g}s  "
              + ("; ".join(flags) if flags else "ok"))
    if st["leases"]:
        print(f"leases held: {len(st['leases'])}")
        for ls in st["leases"]:
            print(f"  {ls['host']}  pid {ls['pid']}  {ls['age_s']}s")
    print(f"kv entries: {st['kv_entries']}")


def main(argv=None):
    global DB_PATH
    ap = argparse.ArgumentParser(
        prog="python -m litpipe.state",
        description="Inspect or clear the literature pipeline's shared state "
                    "(<state_dir>/litpipe_state.sqlite).")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--status", action="store_true",
                   help="print hosts (budgets, deferrals, refusals), live runs and leases")
    g.add_argument("--clear-refusal", metavar="HOST",
                   help="clear a host refusal, including a 'manual' one (e.g. export.arxiv.org)")
    g.add_argument("--clear-deferral", metavar="HOST", help="clear a host deferral")
    ap.add_argument("--json", action="store_true", help="print --status as JSON")
    ap.add_argument("--db", metavar="PATH", help="state file to use instead of the configured one")
    args = ap.parse_args(argv)
    if args.db:
        DB_PATH = Path(args.db)
    p = db_path(create=False)
    if not p.exists():  # nothing to show or clear; never create the state just for this
        print(f"no state yet: {p} does not exist (it is created on first use)")
        return 0
    if args.status:
        st = status()
        if args.json:
            print(json.dumps(st, indent=2, ensure_ascii=False))
        else:
            _print_status(st)
        return 0
    if args.clear_refusal:
        host = _norm_host(args.clear_refusal)
        print(f"{host}: refusal cleared" if clear_refusal(host) else f"{host}: was not refused")
        return 0
    host = _norm_host(args.clear_deferral)
    print(f"{host}: deferral cleared" if clear_deferral(host) else f"{host}: was not deferred")
    return 0


if __name__ == "__main__":
    sys.exit(main())
