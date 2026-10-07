"""python -m litpipe.runner: the scheduled runner (dispatch W4-A; plan 2.3; DEC-02 to DEC-05,
DEC-11, DEC-12, DEC-18, DEC-31).

Commands
  run --profile every_run|daily|weekly|monthly [--project KEY ...] [--dry-run] [--scheduled]
      [--db-writes|--no-db-writes] [--json PATH] [--timeout JOB=SECONDS ...]
  batch --project KEY --pool CSV [--size 100] [--batches 1] [--tag TAG] [--skip-preprint]
      [--stage-only] [--dry-run] [--json PATH]
  status            live runs, the last run per project, refused and deferred hosts, today's
                    per-host counts; registers nothing and creates no state file
  schedule-print [--platform linux|windows]
                    the ONE nightly task (DEC-03, 01:00) as a template filled from this checkout and
                    the registry, for this machine's platform by default: a systemd user service and
                    timer (OnCalendar=*-*-* 01:00:00, Persistent=true) plus a crontab line on Linux, a
                    Register-ScheduledTask command plus a schtasks line on Windows. Variable names
                    only, never values; prints only, never registers
  _stage            internal: the child-process shim (below)

Profiles are cumulative (daily includes every_run, weekly daily, monthly weekly). With
--scheduled the profile escalates (one nightly task runs the weekly and monthly jobs too): kv
namespace "runner", keys profile_done:weekly / profile_done:monthly hold the UTC time a run of that
profile or higher last finished with exit 0 or 2; weekly runs when 6.5 days or more have passed,
monthly when 27 days or more, never lower than --profile. A key is stamped only when those jobs ran
(not on a refusal, an abort or a dry run).

Order of one `run`
  1. Registry: exit 1 BEFORE registering when projects.json is missing, does not parse, a selected
     project's sources / auto_stage / walk_cadence_days or the top-level "runner" block is invalid,
     or no project is selected (the active ones, or --project). Then register a writer run
     (state.register_run("runner")), refuse when another runner registered first (by `seq`, the runs
     rowid: `started` is truncated to the second), and start the heartbeat.
  2. Preflight (litpipe.preflight.run): not ok -> nothing else runs, exit 3.
  3. Network canaries (litpipe.canaries, phase "network"): a confirmed failure refuses its origin
     host in litpipe.state for the run; the stages read that.
  4. Per project, in registry order: (a) auto_stage projects only: seed and stage a draft (a
     non-empty queue is backed up first; comment lines stripped; written atomically); (b) sweep, only
     when something is staged (a queue, a tagged queue, or due retry_later rows), with
     loose_ends=False and the candidate order; (c) route (migrate_closed_to_md) the sweep's run id;
     (d) the forward walk when due (walk_cadence_days; kv runner walk:<project>).
  5. Weekly: the backward top-up (reverse_citations) for the cadence-driven projects.
  6. Index each project whose index fingerprint changed (DB writes only).
  7. Profile jobs: enrich_abstracts (weekly), enrich_recommendations --recent-feed (monthly, S2 key
     only, DEC-18), one keyed S2 call (monthly, so an idle key is not pruned), audit_portfolio
     (weekly, read-only, holdings=False).
  8. Local canaries over what the run wrote, the health report.
  9. LOOSE_ENDS (one line per project, only when its state changed), the summary, finish_run (in a
     finally, so a crash still ends the run and its run refusals).
--dry-run lists every job per project with the reason for each skip, and the canaries' planned
requests; it registers nothing and writes nothing (no state file, summary, log, LOOSE_ENDS line or
pool state), and exits 0.

`batch` draws a pool down on litpipe.worklists.Pool's drawdown contract (W4a verifier M): the pool
CSV's row order is its rank; each batch is written as lit_pull_queue.<tag>.csv in the project root
(the 6-column queue contract, destination = the registry library relative to the project root) and
swept with sweep's own run id. The tag is --tag, else `b-` + the pool's file name (a leading
`lit_pull_queue.` and the `.csv` dropped, lower-cased, characters outside [a-z0-9_-] as '-', leading
characters that are neither letters nor digits dropped, 32 characters at most); it must pass
sweep.is_valid_tag. Exit 1 before anything is written when the batch queue would be the pool CSV,
when sweep would sweep the pool itself, when another queue is staged (sweep's own `retry` queue
aside), or when a file sits at the batch path that the pool does not track. Pending batches are
resolved first (a file present: swept; a processed artifact of its tag holding its DOIs: routed if
its routing CSV is missing, then marked swept, never swept again; neither: the file is written again
from the staged DOIs, never staged twice); no new row is drawn while one is unresolved. One batch:
next_batch, mark_staged (before the write), the write, the sweep, the route, mark_swept with each
DOI's residual class ("fetched" when absent from the residual). A batch whose own queue did not
retire is not marked swept: the loop stops and the batch waits for the next `runner batch`.
--batches counts the batches swept by one invocation, a resumed one included. --stage-only stages
the next batch and stops (the curation pause, no preflight or canaries: nothing is sent); the next
`runner batch` marks the rows a person removed from the file `curated_out` and sweeps the rest; a
DOI in the file that the batch did not stage is exit 1. "Archive the stage set": sweep's run-scoped
artifacts (lit_pull_queue.<tag>.<run_id>.<stage>.csv, the processed queue among them) are the
archive; nothing is copied.

Exit codes: 0 every job OK or deliberately SKIPPED, and HEALTH PASS; 1 usage or config; 2 completed
with failures (a job DEGRADED, FAILED, ERROR, ABORTED or timed out; a walk DEFERRED; HEALTH ALARM,
except an index_freshness ALARM for a project whose index the runner skipped because DB writes are
off); 3 aborted (preflight failed, another runner live, heartbeat lost, interrupted, the runner
crashed). Before an exit 2 or 3 the last stdout line is `[step-summary] {json}` with reasons,
aborted and transport_failures.

Database writes (DEC-05 not executed): index_portfolio, enrich_abstracts, enrich_recommendations
(and litpipe.enrich_s2, which the runner does not call) open portfolio.duckdb for writing. They run
only when projects.json has "runner": {"unattended_db_writes": true} or --db-writes is passed
(--no-db-writes forces them off); otherwise the summary lists each with the command to run. The
"runner" block also takes "candidate_order": "repository" | "publisher" (DEC-11).

Stage isolation (ruling 2 of the W4 plan). Every (project, stage) runs in a child process:
    python -m litpipe.runner _stage --module M --kwargs K.json --out R.json --config REGISTRY
with cwd = the repo root (pyproject has package = false, so -m resolves only there), and
LITPIPE_RUN_ID, PYTHONUTF8=1 and PYTHONUNBUFFERED=1 in its environment. Before importing M the shim
binds, from REGISTRY: litpipe.config.CONFIG_PATH, lit_util.PROJECTS_ROOT (the registry's root),
litpipe.state.DB_PATH and litpipe.ledger.LEDGER_DIR (its state_dir); after the import, M's own
CONFIG_PATH. It calls M.run(**kwargs) and writes the returned dict as JSON (default=str, every string
redacted), atomically. A missing, empty or malformed result file, a non-zero shim exit without one,
or a dict without the keys the result table expects is that stage's ERROR, never an empty success.
The child's stdout and stderr go line by line, redacted, to <state_dir>/runner/<run_id>/logs/. Linux
and Windows are both first-class; the platform branch lives in ProcessTree alone. POSIX: each child
starts in a new session (its own process group); a timeout or a lost heartbeat sends SIGTERM to the
group, then SIGKILL after KILL_GRACE_S; whatever is left of the group when the child ends is killed;
SIGTERM to the runner (systemd stopping the unit) kills the running child's group and ends the run
as aborted (a SIGKILLed runner cannot: under systemd the unit's cgroup goes with it). Windows: each
child is created suspended, put in its own job object with JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE, then
resumed (a venv's python.exe is a launcher that starts the real interpreter as its own child at
once, so a child assigned while running could leave the interpreter and sweep's stages outside the
job); its descendants join the job, the tree dies with the runner, and a kill terminates the job
(taskkill /T /F when no job could be made). One stage's failure never ends the run.
This module imports no stage module at module level (several bind live paths at import).
Grandchildren: sweep starts its fetch stages and migrate as subprocesses with sys.executable and no
--config (sweep.py run_pipeline and run); they read the same registry file the shim was given,
because in production that is the repo's projects.json, the file they resolve themselves.
In-process: preflight, the canaries, status, the summary, LOOSE_ENDS and the runner's reads of
state, the ledger and the walk cache. Tests pass launcher=inprocess_launcher, which runs the same
shim in this process with its bindings saved and restored.

The result table (one row per stage; tests pin every row). Statuses: OK, DEGRADED, FAILED, ERROR,
SKIPPED, ABORTED (and DEFERRED, set by the runner before a walk).
  job       module                          result -> status                                   kept
  seed      seed_queue_from_top_candidates  exit_code 0 OK, 1 FAILED, 2 DEGRADED               rows, output
  sweep     sweep                           exit_code 0 OK; 1 SKIPPED (nothing to sweep);     run_id, queues,
                                            2 FAILED (CONFIG: later projects' sweeps SKIPPED   rows, downloaded,
                                            "CONFIG abort in <key>"); 3 FAILED, or DEGRADED    unpaywall, pmc,
                                            when every result retired; 4 DEGRADED (a queue     preprint, retired,
                                            refused before fetching)                           kept, refused, classes
  route     migrate_closed_to_md            status ok OK; nothing SKIPPED; config, error       rows, counts,
                                            FAILED                                             written
  walk      forward_citations               exit_code 0 OK; 1 FAILED; 2 DEGRADED (reasons);    seeds, walked, failed,
                                            3 DEGRADED (aborted); a 2 whose failed seeds are   transport_failures,
                                            all chronic is DEGRADED "chronic only" and does   chronic, new
                                            not count toward exit 2
  reverse   reverse_citations               exit convention (as walk)                          network_walks, ...
  index     index_portfolio                 exit convention (0 OK, 1 FAILED, 2 DEGRADED)        indexed, skipped
  abstracts enrich_abstracts                interrupted ABORTED; write_failures FAILED;        targets, attempted,
                                            stopped_early DEGRADED (stop_reason); else OK      hits, misses
  recommendations enrich_recommendations    status off / no_key SKIPPED; else the exit         asked, ok, failed
                                            convention (130 ABORTED)
  audit     audit_portfolio                 exit_code 0 OK; 1 DEGRADED "FAIL items";           summary
                                            2 SKIPPED (no matching project)
  extract   extract_pdf_fulltext            exit 0 OK, 1 FAILED, 2 DEGRADED (not run by the     counts
                                            runner itself: sweep runs it)
  any       a stage that returns OK while its log holds "Traceback (most recent call last):" is
            DEGRADED "traceback in the stage log" (a stage that swallows a grandchild's crash still
            exits 0).
  The exit convention: 0 OK, 1 FAILED, 2 DEGRADED, 3 DEGRADED (aborted: budget or breaker), 130
  ABORTED (interrupted), anything else FAILED.

Walks. The forward walk runs daily and up for a project whose walk_cadence_days is set (None means
never unattended) once that many days have passed since the last walk (kv runner walk:<project>,
stamped on exit 0 or 2). It is DEFERRED, not FAILED, while api.semanticscholar.org is refused for
the run or deferred (a final S2 429 refuses it for the run, so every later project's walk is
DEFERRED too). Failed seeds are read from the walk cache before and after (walk.Cache.states(); a
locked cache is "chronic: unknown", never waited on), so seeds that failed before and still fail are
reported apart from new failures. The runner never runs snowball's convergence loop.

Crossref's 10 % rule: after each sweep and walk, this run's first attempts to api.crossref.org
(attempt 1, hop 0, not a canary, not not_sent / prohibited) are counted from the ledger; with 20 or
more and an error share (429, 5xx, TRANSPORT, OUTAGE, REFUSED; a 404 is an answer) of 10 % or more
the host is refused for the run.

Time. On Windows time.monotonic() is QueryPerformanceCounter, which counts the time the machine
sleeps ("including the time when the machine was in a sleep state such as standby, hibernate, or
connected standby", learn.microsoft.com, Acquiring high-resolution time stamps), while a wait's
timeout does not count down during sleep on Windows 8 and later (WaitForSingleObject). So the
heartbeat thread waits in slices of at most HEARTBEAT_SLICE_S, beats as soon as time.time() has
advanced HEARTBEAT_EVERY_S since its last beat (a wake from sleep beats at once), and counts any
wall-clock advance beyond a requested wait as a jump; a stage's elapsed time for its timeout is the
wall-clock time minus the jumps observed while it ran, so a closed lid does not time a stage out. The
measure does not depend on how a platform's monotonic clock treats suspend; a server that never
sleeps simply observes no jump.

Outputs: one runs row in litpipe.state; <state_dir>/runner/<run_id>/summary.json (the newest
KEEP_RUN_DIRS run directories are kept); the stage logs; one LOOSE_ENDS line per project per run
when that project's state changed (written through sweep.append_loose_end with sweep.CONFIG_PATH
bound to this registry; nothing is written when projects.json has no `loose_ends` key). Everything
persisted passes litpipe.ledger.redact_obj.
"""
from __future__ import annotations

import argparse
import contextlib
import csv
import hashlib
import importlib
import io
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import traceback
from dataclasses import dataclass, field
from datetime import date as _date
from datetime import datetime, timedelta, timezone
from pathlib import Path

import lit_util
from litpipe import canaries, config, ledger, preflight, state

REPO_ROOT = Path(__file__).resolve().parent.parent
PROFILES = ("every_run", "daily", "weekly", "monthly")
RANK = {p: i for i, p in enumerate(PROFILES)}
KV = "runner"
RUN_ENV = "LITPIPE_RUN_ID"

OK, DEGRADED, FAILED, ERROR, SKIPPED, ABORTED, DEFERRED = (
    "OK", "DEGRADED", "FAILED", "ERROR", "SKIPPED", "ABORTED", "DEFERRED")
FAILURE_STATUSES = frozenset({DEGRADED, FAILED, ERROR, ABORTED, DEFERRED})
EXIT_OK, EXIT_CONFIG, EXIT_FAILURES, EXIT_ABORTED = 0, 1, 2, 3

HEARTBEAT_EVERY_S = 300.0       # beat when time.time() has advanced this much since the last beat
HEARTBEAT_SLICE_S = 15.0        # the heartbeat thread never waits longer than this at a time
JUMP_TOLERANCE_S = 30.0         # a wall-clock advance this far beyond a wait is a jump (sleep, clock step)
POLL_S = 0.2                    # the wait loop's poll while a child runs
JOIN_S = 10.0                   # join the log reader after the child ends (a grandchild may hold the pipe)
KILL_GRACE_S = 10.0             # POSIX: SIGTERM to a stage's process group, then SIGKILL after this
KEEP_RUN_DIRS = 90
WEEKLY_AFTER_S = 6.5 * 86400
MONTHLY_AFTER_S = 27 * 86400
CANDIDATE_ORDERS = ("repository", "publisher")
AB_ORDERS = ("repository", "publisher")      # DEC-11: the first two scheduled runs with Unpaywall
S2_HOST = "api.semanticscholar.org"
CROSSREF_HOST = "api.crossref.org"
CROSSREF_MIN_LINES = 20
CROSSREF_ERROR_SHARE = 0.10
KEEPALIVE_DOI = "10.1371/journal.pone.0012033"   # preflight's DOI; one keyed S2 call a month
REVERSE_SOURCES_UNKEYED = "openalex,crossref,regex"
TRACEBACK_MARK = "Traceback (most recent call last):"
HEARTBEAT_LOST = "heartbeat lost (machine asleep?)"
QUEUE_FILE = "lit_pull_queue.csv"
DRAFT_FILE = "lit_pull_queue.draft.csv"
QUEUE_COLUMNS = ("doi", "title", "authors", "year", "destination", "notes")
RUN_DIR_RE = re.compile(r"^\d{8}T\d{6}Z-[a-z0-9_-]+-\d+-[0-9a-f]+$")

# The stage modules (job -> module). Tests point entries at stand-ins.
STAGE_MODULES = {
    "seed": "seed_queue_from_top_candidates",
    "sweep": "sweep",
    "route": "migrate_closed_to_md",
    "walk": "forward_citations",
    "reverse": "reverse_citations",
    "index": "index_portfolio",
    "abstracts": "enrich_abstracts",
    "recommendations": "enrich_recommendations",
    "audit": "audit_portfolio",
    "extract": "extract_pdf_fulltext",
}
# Per-stage timeouts in seconds (--timeout JOB=SECONDS overrides one run). enrich_abstracts and
# enrich_recommendations get 3 h, not the 30 min default: about 17,000 DOIs take about 95 min at
# Crossref's pacing (enrich_abstracts' docstring), and 5,872 seeds at the keyed 1.1 s take about
# 108 min.
TIMEOUTS = {"sweep": 4 * 3600, "walk": 6 * 3600, "reverse": 6 * 3600, "index": 3600,
            "abstracts": 3 * 3600, "recommendations": 3 * 3600}
DEFAULT_TIMEOUT_S = 1800
DB_JOBS = frozenset({"index", "abstracts", "recommendations"})

# Test seams: a fake clock for the scheduling logic (escalation, walk cadence, stamps) and a hook
# called at each batch checkpoint (a test raises there to simulate a kill).
_now = time.time
KILL_HOOK = None


class ConfigProblem(Exception):
    """The registry cannot be used: exit 1 before registering."""


class _Abort(Exception):
    def __init__(self, reason):
        super().__init__(reason)
        self.reason = reason


class _BatchStop(Exception):
    """A batch condition that exits with a code (1: usage, before or after registering)."""

    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


# ------------------------------------------------------------------------------ small helpers
def _iso(ts):
    return datetime.fromtimestamp(ts, timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _slug(s):
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", str(s)).strip("_") or "x"


def _say(msg):
    print(f"[runner] {msg}", flush=True)


def _checkpoint(name):
    if KILL_HOOK is not None:
        KILL_HOOK(name)


@contextlib.contextmanager
def _bound(obj, attr, value):
    old = getattr(obj, attr)
    setattr(obj, attr, value)
    try:
        yield
    finally:
        setattr(obj, attr, old)


def command(script, *args) -> str:
    """The exact command a person runs for a job the runner skipped."""
    parts = [sys.executable, str(REPO_ROOT / script), *(str(a) for a in args)]
    if os.name == "nt":
        return subprocess.list2cmdline(parts)
    import shlex
    return shlex.join(parts)


def _state_exists() -> bool:
    try:
        return state.db_path(create=False).exists()
    except Exception:   # noqa: BLE001 - a bad state_dir is reported by the registry check
        return False


def _kv_get(key, default=None, *, read_only=False):
    if read_only and not _state_exists():
        return default
    return state.kv_get(KV, key, default)


def _host_refused(host, *, read_only=False) -> bool:
    if read_only and not _state_exists():
        return False
    return state.is_refused(host) or state.deferred_until(host) is not None


def _data_lines(path) -> int:
    """Header plus data lines of a queue CSV (non-blank, not a '#' comment)."""
    with open(path, encoding="utf-8") as f:
        return sum(1 for line in f if line.strip() and not line.startswith("#"))


# ------------------------------------------------------------------------------ the registry
def runner_block(cfg) -> dict:
    """projects.json "runner": {"unattended_db_writes": bool, "candidate_order": str}, validated."""
    b = config.load(cfg).get("runner", {})
    if b is None:
        b = {}
    if not isinstance(b, dict):
        raise config.ConfigError(f"runner must be an object, got {b!r}")
    udw = b.get("unattended_db_writes", False)
    if not isinstance(udw, bool):
        raise config.ConfigError(f"runner.unattended_db_writes must be true or false, got {udw!r}")
    order = b.get("candidate_order")
    if order is not None and order not in CANDIDATE_ORDERS:
        raise config.ConfigError(f"runner.candidate_order must be one of {', '.join(CANDIDATE_ORDERS)}, "
                                 f"got {order!r}")
    return {"unattended_db_writes": udw, "candidate_order": order}


def load_registry(projects=None):
    """(cfg, [selected keys in registry order]); ConfigProblem for every exit-1 case."""
    path = Path(config.CONFIG_PATH)
    if not path.is_file():
        raise ConfigProblem(f"projects.json not found at {path} (config.load would read it as empty)")
    try:
        with open(path, encoding="utf-8") as f:
            cfg = json.load(f)
    except json.JSONDecodeError as e:
        raise ConfigProblem(f"{path} does not parse as JSON: {e}") from None
    except OSError as e:
        raise ConfigProblem(f"{path} cannot be read: {e}") from None
    if not isinstance(cfg, dict):
        raise ConfigProblem(f"{path} is not a JSON object")
    reg = cfg.get("projects") or {}
    if not isinstance(reg, dict):
        raise ConfigProblem(f"{path}: projects must be an object")
    if projects:
        for k in projects:
            if k not in reg:
                raise ConfigProblem(f"--project {k!r} is not registered in {path.name}")
            if not (reg[k] or {}).get("active", True):
                raise ConfigProblem(f"--project {k!r} is inactive in {path.name}")
        want = set(projects)
        keys = [k for k in reg if k in want]
    else:
        keys = [k for k, v in reg.items() if (v or {}).get("active", True)]
    if not keys:
        raise ConfigProblem("no project selected (no active project in the registry)")
    try:
        for k in keys:
            config.sources(k, cfg=cfg)
            config.auto_stage(k, cfg=cfg)
            config.walk_cadence_days(k, cfg=cfg)
        runner_block(cfg)
        config.state_dir(cfg, create=False)
        config.db_dir(cfg)
    except config.ConfigError as e:
        raise ConfigProblem(str(e)) from None
    return cfg, keys


@dataclass
class Project:
    key: str
    entry: dict
    root: Path
    lib: Path | None
    sources: list
    auto_stage: bool
    cadence: int | None
    seed_failed: bool = False
    sweep: dict | None = None          # {"exit", "run_id", "results", "refused"}
    route_status: str | None = None
    staged_rows: int = 0


def _projects(cfg, keys):
    reg = cfg.get("projects") or {}
    out = []
    for k in keys:
        e = reg.get(k) or {}
        out.append(Project(k, e, lit_util.project_root(k, e),
                           lit_util.lib_paths(k, e)[1] if e.get("lib_dir") else None,
                           sorted(config.sources(k, cfg=cfg)), config.auto_stage(k, cfg=cfg),
                           config.walk_cadence_days(k, cfg=cfg)))
    return out


# ------------------------------------------------------------------------------ the result table
def _first(x):
    if isinstance(x, (list, tuple)):
        return "; ".join(str(v) for v in x if v)
    return str(x or "")


def _exit_convention(res, _project=None):
    code = res.get("exit_code")
    reasons = _first(res.get("reasons"))
    if code == 0:
        return OK, "", 0
    if code == 1:
        return FAILED, _first(res.get("error")) or reasons or "exit 1", 1
    if code == 2:
        return DEGRADED, reasons or "degraded (exit 2)", 2
    if code == 3:
        why = f"aborted ({res.get('aborted')})" if res.get("aborted") else "aborted (exit 3)"
        return DEGRADED, why + (f"; {reasons}" if reasons and reasons not in why else ""), 3
    if code == 130:
        return ABORTED, "interrupted", 130
    return FAILED, f"exit {code}", code if isinstance(code, int) else None


def _c_sweep(res, project):
    code = res.get("exit_code")
    proj = (res.get("projects") or {}).get(project) or {}
    results = proj.get("results") or []
    if code == 0:
        return OK, "", 0
    if code == 1:
        return SKIPPED, "--project had nothing to sweep", 1
    if code == 2:
        return FAILED, "usage or CONFIG: sweep aborted the run", 2
    if code == 3:
        failed = sorted({s for r in results for s in (r.get("failed_stages") or [])})
        why = f"stage failed: {', '.join(failed)}" if failed else "a stage failed"
        if results and all(r.get("retired") for r in results):
            return DEGRADED, f"{why} (every queue retired)", 3
        return FAILED, why, 3
    if code == 4:
        refused = proj.get("refused") or []
        return DEGRADED, f"queue refused before fetching: {', '.join(refused) or 'see the log'}", 4
    return FAILED, f"exit {code}", code if isinstance(code, int) else None


def _c_route(res, _project=None):
    st = res.get("status")
    if st == "ok":
        return OK, "", 0
    if st == "nothing":
        return SKIPPED, "nothing to route", 0
    if st in ("config", "error"):
        return FAILED, f"{st}: {_first(res.get('error'))}".rstrip(": "), 2   # migrate's main() exits 2 on both
    return FAILED, f"unknown status {st!r}", None


def _c_abstracts(res, _project=None):
    if res.get("interrupted"):
        return ABORTED, "interrupted", 130
    wf = res.get("write_failures") or []
    if wf:
        return FAILED, f"{len(wf)} write failure(s)", 1
    if res.get("stopped_early"):
        return DEGRADED, f"stopped early: {res.get('stop_reason') or 'host out for the run'}", 1
    return OK, "", 0


def _c_recs(res, project=None):
    if res.get("status") == "off":
        return SKIPPED, "off (no --recent-feed)", 0
    if res.get("status") == "no_key":
        return SKIPPED, "no S2 key", 0
    return _exit_convention(res, project)


def _c_audit(res, _project=None):
    code = res.get("exit_code")
    if code == 0:
        return OK, "", 0
    if code == 1:
        return DEGRADED, "FAIL items", 1
    if code == 2:
        return SKIPPED, "no matching project", 2
    return FAILED, f"exit {code}", code if isinstance(code, int) else None


def _c_extract(res, _project=None):
    code = res.get("exit")
    if code == 0:
        return OK, "", 0
    if code == 1:
        return FAILED, _first(res.get("reasons")) or "usage error", 1
    if code == 2:
        return DEGRADED, _first(res.get("reasons")) or "degraded", 2
    return FAILED, f"exit {code}", code if isinstance(code, int) else None


RESULT_TABLE = {
    "seed": (("exit_code",), _exit_convention),
    "sweep": (("exit_code", "projects"), _c_sweep),
    "route": (("status",), _c_route),
    "walk": (("exit_code",), _exit_convention),
    "reverse": (("exit_code",), _exit_convention),
    "index": (("exit_code",), _exit_convention),
    "abstracts": (("interrupted", "stopped_early", "write_failures"), _c_abstracts),
    "recommendations": (("status", "exit_code"), _c_recs),
    "audit": (("exit_code",), _c_audit),
    "extract": (("exit",), _c_extract),
}

KEPT = {
    "seed": ("rows", "output", "transport_failures"),
    "route": ("status", "rows", "counts", "written"),
    "walk": ("seeds", "seed_walks", "walked", "kept", "failed", "transport_failures", "aborted", "published",
             "not_walked"),
    "reverse": ("seeds", "network_walks", "cadence_skipped", "failed", "transport_failures", "aborted",
                "published"),
    "index": ("indexed", "skipped", "unreadable", "transport_failures"),
    "abstracts": ("targets", "attempted", "hits", "misses", "errors_permanent", "errors_transient",
                  "rows_written", "stop_reason"),
    "recommendations": ("status", "asked", "ok", "not_found", "failed", "transport_failures", "feed_rows"),
    "audit": ("summary",),
    "extract": ("counts",),
}


def kept_counts(job, res, project=None) -> dict:
    if not isinstance(res, dict):
        return {}
    if job == "sweep":
        proj = (res.get("projects") or {}).get(project) or {}
        results = proj.get("results") or []
        classes = {}
        for r in results:
            for c, n in (r.get("classes") or {}).items():
                classes[c] = classes.get(c, 0) + (n or 0)
        out = {"run_id": proj.get("run_id"), "queues": [r.get("queue") for r in results],
               "refused": list(proj.get("refused") or []), "classes": classes,
               "retired": sum(1 for r in results if r.get("retired")),
               "kept": sum(1 for r in results if not r.get("retired"))}
        for k in ("rows", "downloaded", "unpaywall", "pmc", "preprint"):
            out[k] = sum(int(r.get(k) or 0) for r in results)
        return out
    return {k: res[k] for k in KEPT.get(job, ()) if k in res}


def classify(job, run, project=None):
    """(status, reason, exit) of one stage run, per the result table."""
    if run.killed == "timeout":
        return ERROR, f"timeout after {int(run.timeout_s)} s", None
    if run.killed == "heartbeat":
        return ABORTED, HEARTBEAT_LOST, None
    if run.result is None:
        rc = "" if run.rc is None else f"; shim exit {run.rc}"
        return ERROR, f"{run.problem or 'no result'}{rc}", None
    _, fn = RESULT_TABLE[job]
    status, reason, code = fn(run.result, project)
    if status == OK and run.traceback:
        return DEGRADED, "traceback in the stage log", code
    return status, reason, code


# ------------------------------------------------------------------------------ the shim
def _bindings(registry):
    """[(object, attribute, value)] the shim applies before importing a stage module."""
    from litpipe import ledger as _ledger
    from litpipe import state as _state
    registry = Path(registry)
    with open(registry, encoding="utf-8") as f:
        cfg = json.load(f)
    sd = config.state_dir(cfg, create=False)
    return [(config, "CONFIG_PATH", registry),
            (lit_util, "PROJECTS_ROOT", lit_util._resolve_projects_root(cfg)),
            (_state, "DB_PATH", sd / _state.DB_NAME),
            (_ledger, "LEDGER_DIR", sd / "ledger")]


def write_result(out, res):
    """The stage's dict as JSON (default=str), every string redacted, written atomically."""
    payload = ledger.redact_obj(json.loads(json.dumps(res, default=str)))
    out = Path(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    lit_util.atomic_write_text(str(out), json.dumps(payload, ensure_ascii=False, indent=1) + "\n")


def shim(module, kwargs, out, registry, *, restore=False):
    """Bind the registry's paths, import `module`, bind its CONFIG_PATH, call run(**kwargs), write
    the result. restore=True puts every binding back (the in-process launcher)."""
    saved = []
    try:
        for obj, attr, val in _bindings(registry):
            saved.append((obj, attr, getattr(obj, attr)))
            setattr(obj, attr, val)
        mod = importlib.import_module(module)
        if hasattr(mod, "CONFIG_PATH"):
            saved.append((mod, "CONFIG_PATH", mod.CONFIG_PATH))
            mod.CONFIG_PATH = Path(registry)
        res = mod.run(**kwargs)
        write_result(out, res)
    finally:
        if restore:
            for obj, attr, old in reversed(saved):
                setattr(obj, attr, old)
    return 0


def read_result(out, required=()):
    """(dict, "") or (None, why): missing, empty, malformed, not an object, or a missing key."""
    out = Path(out)
    if not out.is_file():
        return None, "no result file"
    try:
        text = out.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as e:
        return None, f"unreadable result file ({type(e).__name__})"
    if not text.strip():
        return None, "empty result file"
    try:
        obj = json.loads(text)
    except ValueError as e:
        return None, f"malformed result file ({ledger.redact(str(e))[:120]})"
    if not isinstance(obj, dict):
        return None, f"result is not a JSON object ({type(obj).__name__})"
    missing = [k for k in required if k not in obj]
    if missing:
        return None, f"result lacks {', '.join(missing)}"
    return obj, ""


# ------------------------------------------------------------------------------ the heartbeat
class Heartbeat:
    """A daemon thread that keeps the run's heartbeat fresh (amendment 4); see the module docstring.
    `lost` is set when state.heartbeat() returns False (another process marked the run abandoned,
    which cleared its run refusals and leases) or no beat has succeeded for HEARTBEAT_STALE_S / 2."""

    def __init__(self, run_id, beat=None):
        self.run_id = run_id
        self._beat = beat or state.heartbeat
        self.stop_event = threading.Event()
        self.lost = threading.Event()
        self.lost_reason = ""
        self.jumped_s = 0.0
        self.jumps = 0
        self.beats = 0
        self.errors = 0
        t = time.time()
        self.last_ok = t
        self.last_try = t
        self.thread = None
        self._lock = threading.Lock()

    def start(self):
        self.thread = threading.Thread(target=self._loop, name="litpipe-runner-heartbeat", daemon=True)
        self.thread.start()
        return self

    def stop(self):
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(timeout=HEARTBEAT_SLICE_S + 5)

    def jumped(self) -> float:
        with self._lock:
            return self.jumped_s

    def _loop(self):
        while not self.stop_event.is_set():
            due_in = HEARTBEAT_EVERY_S - (time.time() - self.last_try)
            wait = max(0.05, min(HEARTBEAT_SLICE_S, due_in))
            before = time.time()
            if self.stop_event.wait(wait):
                break
            jump = (time.time() - before) - wait
            if jump > JUMP_TOLERANCE_S:
                with self._lock:
                    self.jumped_s += jump
                    self.jumps += 1
            if time.time() - self.last_try >= HEARTBEAT_EVERY_S:
                self.beat_now()

    def beat_now(self):
        """True on a beat, False when the run is no longer live, None when the beat raised."""
        self.last_try = time.time()
        try:
            ok = self._beat(self.run_id)
        except Exception as e:   # noqa: BLE001 - logged and retried at the next slice
            self.errors += 1
            print(f"[runner] heartbeat failed ({ledger.redact(f'{type(e).__name__}: {e}')}); retrying",
                  file=sys.stderr, flush=True)
            self.last_try = time.time() - HEARTBEAT_EVERY_S + HEARTBEAT_SLICE_S
            if time.time() - self.last_ok > state.HEARTBEAT_STALE_S / 2:
                self._mark_lost("no successful heartbeat for over HEARTBEAT_STALE_S / 2")
            return None
        if ok:
            self.last_ok = time.time()
            self.beats += 1
            return True
        self._mark_lost("the run was marked abandoned or finished by another process")
        return False

    def _mark_lost(self, why):
        if not self.lost.is_set():
            self.lost_reason = why
            self.lost.set()

    def check_before_launch(self) -> bool:
        """The main thread's beat right before a stage starts."""
        if self.lost.is_set():
            return False
        r = self.beat_now()
        if r is False:
            return False
        if r is None and time.time() - self.last_ok > state.HEARTBEAT_STALE_S / 2:
            self._mark_lost("no successful heartbeat for over HEARTBEAT_STALE_S / 2")
            return False
        return not self.lost.is_set()


class _NoHeartbeat:
    """Stands in when no run is registered (never used for a real run)."""
    lost = threading.Event()

    def jumped(self):
        return 0.0

    def check_before_launch(self):
        return True


# ------------------------------------------------------------------------------ launching a stage
@dataclass
class Step:
    job: str
    module: str
    kwargs: dict
    project: str | None
    timeout_s: float
    log_path: Path
    kwargs_path: Path
    out_path: Path
    registry: Path
    run_id: str
    heartbeat: object = None


@dataclass
class StageRun:
    rc: int | None
    result: dict | None
    problem: str = ""
    killed: str = ""               # "timeout" | "heartbeat" | ""
    kill_method: str = ""
    traceback: bool = False
    lines: int = 0
    elapsed_s: float = 0.0
    timeout_s: float = 0.0
    log: str | None = None
    reader_abandoned: bool = False


class _Pump:
    """Reads a child's merged stdout/stderr line by line (bytes, so undecodable output cannot stop
    it) and writes each line, redacted, to the stage log."""

    def __init__(self, stream, log_path):
        self.stream = stream
        self.fh = open(log_path, "w", encoding="utf-8", newline="\n")
        self.lines = 0
        self.traceback = False
        self.thread = threading.Thread(target=self._run, name="litpipe-runner-log", daemon=True)
        self._closed = False
        self._lock = threading.Lock()

    def start(self):
        self.thread.start()
        return self

    def _run(self):
        try:
            for raw in iter(self.stream.readline, b""):
                line = raw.decode("utf-8", "replace").rstrip("\r\n")
                if TRACEBACK_MARK in line:
                    self.traceback = True
                with self._lock:
                    if self._closed:
                        continue
                    self.fh.write(ledger.redact(line) + "\n")
                self.lines += 1
        except (OSError, ValueError):
            pass

    def close(self):
        with self._lock:
            self._closed = True
            try:
                self.fh.close()
            except OSError:
                pass


class _JobObject:
    """A Windows job object with JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE holding one child and its
    descendants (learn.microsoft.com: JOBOBJECT_BASIC_LIMIT_INFORMATION, ..._EXTENDED_...,
    AssignProcessToJobObject). None on any failure; the caller falls back to taskkill."""

    KILL_ON_JOB_CLOSE = 0x2000
    EXTENDED_LIMIT_INFORMATION = 9          # JOBOBJECTINFOCLASS JobObjectExtendedLimitInformation
    PROCESS_SET_QUOTA = 0x0100
    PROCESS_TERMINATE = 0x0001
    CREATE_SUSPENDED = 0x00000004
    TH32CS_SNAPTHREAD = 0x00000004
    THREAD_SUSPEND_RESUME = 0x0002
    _k32 = None

    def __init__(self, handle):
        self.h = handle

    @classmethod
    def _kernel32(cls):
        if cls._k32 is None:
            import ctypes
            from ctypes import wintypes
            k = ctypes.WinDLL("kernel32", use_last_error=True)
            k.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
            k.CreateJobObjectW.restype = wintypes.HANDLE
            k.SetInformationJobObject.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD]
            k.SetInformationJobObject.restype = wintypes.BOOL
            k.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
            k.AssignProcessToJobObject.restype = wintypes.BOOL
            k.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
            k.TerminateJobObject.restype = wintypes.BOOL
            k.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
            k.OpenProcess.restype = wintypes.HANDLE
            k.CloseHandle.argtypes = [wintypes.HANDLE]
            k.CloseHandle.restype = wintypes.BOOL
            k.CreateToolhelp32Snapshot.argtypes = [wintypes.DWORD, wintypes.DWORD]
            k.CreateToolhelp32Snapshot.restype = wintypes.HANDLE
            k.Thread32First.argtypes = [wintypes.HANDLE, ctypes.c_void_p]
            k.Thread32First.restype = wintypes.BOOL
            k.Thread32Next.argtypes = [wintypes.HANDLE, ctypes.c_void_p]
            k.Thread32Next.restype = wintypes.BOOL
            k.OpenThread.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
            k.OpenThread.restype = wintypes.HANDLE
            k.ResumeThread.argtypes = [wintypes.HANDLE]
            k.ResumeThread.restype = wintypes.DWORD
            cls._k32 = k
        return cls._k32

    @classmethod
    def resume(cls, pid) -> int:
        """Resume every thread of a process created with CREATE_SUSPENDED (its one primary thread):
        Toolhelp32 snapshot, OpenThread, ResumeThread. Returns the threads resumed."""
        import ctypes
        from ctypes import wintypes

        class THREADENTRY32(ctypes.Structure):
            _fields_ = [("dwSize", wintypes.DWORD), ("cntUsage", wintypes.DWORD), ("th32ThreadID", wintypes.DWORD),
                        ("th32OwnerProcessID", wintypes.DWORD), ("tpBasePri", ctypes.c_long),
                        ("tpDeltaPri", ctypes.c_long), ("dwFlags", wintypes.DWORD)]
        k = cls._kernel32()
        snap = k.CreateToolhelp32Snapshot(cls.TH32CS_SNAPTHREAD, 0)
        if not snap or snap == ctypes.c_void_p(-1).value:
            return 0
        n = 0
        try:
            te = THREADENTRY32()
            te.dwSize = ctypes.sizeof(THREADENTRY32)
            ok = k.Thread32First(snap, ctypes.byref(te))
            while ok:
                if te.th32OwnerProcessID == int(pid):
                    th = k.OpenThread(cls.THREAD_SUSPEND_RESUME, False, te.th32ThreadID)
                    if th:
                        try:
                            if k.ResumeThread(th) != 0xFFFFFFFF:
                                n += 1
                        finally:
                            k.CloseHandle(th)
                ok = k.Thread32Next(snap, ctypes.byref(te))
        finally:
            k.CloseHandle(snap)
        return n

    @staticmethod
    def _info():
        import ctypes
        from ctypes import wintypes

        class BASIC(ctypes.Structure):
            _fields_ = [("PerProcessUserTimeLimit", ctypes.c_int64), ("PerJobUserTimeLimit", ctypes.c_int64),
                        ("LimitFlags", wintypes.DWORD), ("MinimumWorkingSetSize", ctypes.c_size_t),
                        ("MaximumWorkingSetSize", ctypes.c_size_t), ("ActiveProcessLimit", wintypes.DWORD),
                        ("Affinity", ctypes.c_size_t), ("PriorityClass", wintypes.DWORD),
                        ("SchedulingClass", wintypes.DWORD)]

        class IO(ctypes.Structure):
            _fields_ = [(n, ctypes.c_uint64) for n in ("ReadOperationCount", "WriteOperationCount",
                                                       "OtherOperationCount", "ReadTransferCount",
                                                       "WriteTransferCount", "OtherTransferCount")]

        class EXTENDED(ctypes.Structure):
            _fields_ = [("BasicLimitInformation", BASIC), ("IoInfo", IO),
                        ("ProcessMemoryLimit", ctypes.c_size_t), ("JobMemoryLimit", ctypes.c_size_t),
                        ("PeakProcessMemoryUsed", ctypes.c_size_t), ("PeakJobMemoryUsed", ctypes.c_size_t)]
        return EXTENDED()

    @classmethod
    def for_pid(cls, pid):
        if os.name != "nt":
            return None
        try:
            import ctypes
            k = cls._kernel32()
            h = k.CreateJobObjectW(None, None)
            if not h:
                return None
            info = cls._info()
            info.BasicLimitInformation.LimitFlags = cls.KILL_ON_JOB_CLOSE
            if not k.SetInformationJobObject(h, cls.EXTENDED_LIMIT_INFORMATION, ctypes.byref(info),
                                             ctypes.sizeof(info)):
                k.CloseHandle(h)
                return None
            ph = k.OpenProcess(cls.PROCESS_SET_QUOTA | cls.PROCESS_TERMINATE, False, int(pid))
            if not ph:
                k.CloseHandle(h)
                return None
            try:
                ok = k.AssignProcessToJobObject(h, ph)
            finally:
                k.CloseHandle(ph)
            if not ok:
                k.CloseHandle(h)
                return None
            return cls(h)
        except Exception:   # noqa: BLE001 - no job object: taskkill is the fallback
            return None

    def terminate(self) -> bool:
        if not self.h:
            return False
        try:
            return bool(self._kernel32().TerminateJobObject(self.h, 1))
        except Exception:   # noqa: BLE001
            return False

    def close(self):
        if self.h:
            try:
                self._kernel32().CloseHandle(self.h)
            except Exception:   # noqa: BLE001
                pass
            self.h = None


class ProcessTree:
    """One stage child and its descendants; the ONE place the platform branch lives (both platforms
    are first-class).

    POSIX: the child starts in a new session (start_new_session=True), so its process group holds it
    and everything it starts; kill() sends SIGTERM to the group, waits KILL_GRACE_S, then SIGKILL;
    close() sends SIGKILL to whatever is left of the group after the child exited (the analogue of
    closing the job). A runner killed with SIGKILL cannot do this: under systemd the unit's cgroup is
    killed with it (KillMode=control-group, the default).
    Windows: the child starts suspended (CREATE_SUSPENDED) in a new process group, goes into its own
    job object with JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE, then is resumed; kill() terminates the job
    (taskkill /T /F when no job could be made); close() closes the job, which kills what is left."""

    POSIX = os.name != "nt"

    def __init__(self, proc, job=None):
        self.proc = proc
        self.job = job

    @classmethod
    def start(cls, cmd, **kw):
        """Popen(cmd, **kw) with the platform's tree setup. Raises OSError when it cannot start."""
        if cls.POSIX:
            return cls(subprocess.Popen(cmd, start_new_session=True, **kw))
        # Suspended until it is in the job: a venv's python.exe is a launcher that starts the real
        # interpreter as its own child at once, so a child assigned after it runs can leave that
        # interpreter, and its stages, outside the job (measured: the grandchild outlived the kill).
        flags = kw.pop("creationflags", 0) | subprocess.CREATE_NEW_PROCESS_GROUP | _JobObject.CREATE_SUSPENDED
        proc = subprocess.Popen(cmd, creationflags=flags, **kw)
        tree = cls(proc, _JobObject.for_pid(proc.pid))
        if not _JobObject.resume(proc.pid):
            tree.kill()
            tree.close()
            raise OSError("could not resume the suspended stage process")
        return tree

    @property
    def pid(self):
        return self.proc.pid

    def _signal_group(self, sig) -> bool:
        try:
            os.killpg(self.proc.pid, sig)
            return True
        except (ProcessLookupError, PermissionError, OSError):
            return False

    def kill(self) -> str:
        """Kill the child and every descendant; returns how."""
        import signal
        how = ""
        if self.POSIX:
            term = getattr(signal, "SIGTERM", 15)
            hard = getattr(signal, "SIGKILL", 9)
            if self._signal_group(term):
                how = "process group (SIGTERM)"
                try:
                    self.proc.wait(timeout=KILL_GRACE_S)
                except subprocess.TimeoutExpired:
                    pass
                if self._signal_group(hard):        # what is left of the group after the grace period
                    how = "process group (SIGTERM, then SIGKILL)"
            else:
                how = "process group gone"
        elif self.job is not None and self.job.terminate():
            how = "job object"
        else:
            try:
                subprocess.run(["taskkill", "/T", "/F", "/PID", str(self.proc.pid)], capture_output=True,
                               timeout=60)
                how = "taskkill /T /F"
            except (OSError, subprocess.SubprocessError):
                how = "taskkill failed"
        try:
            self.proc.kill()
        except OSError:
            pass
        try:
            self.proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            pass
        return how

    def close(self):
        """After the child ended: nothing of its tree outlives it."""
        if self.POSIX:
            import signal
            self._signal_group(getattr(signal, "SIGKILL", 9))
        elif self.job is not None:
            self.job.close()                         # KILL_ON_JOB_CLOSE: a lingering grandchild dies now
            self.job = None


def _kill_tree(proc, job=None) -> str:
    """Kill a child's tree (kept for callers that hold a bare Popen)."""
    return ProcessTree(proc, job).kill()


class _Terminated(BaseException):
    """SIGTERM reached the runner (POSIX: systemd stopping the unit, `kill <pid>`): the running
    child's tree is killed and the run ends as aborted."""


@contextlib.contextmanager
def _sigterm_aborts():
    """On POSIX, SIGTERM raises _Terminated in the main thread for the duration of a run, so the
    stage launcher kills the running child's tree and the summary is still written. Windows has no
    deliverable SIGTERM (TerminateProcess cannot be caught); the job objects cover it there."""
    import signal
    if not ProcessTree.POSIX or threading.current_thread() is not threading.main_thread():
        yield
        return

    def handler(signum, frame):
        raise _Terminated("SIGTERM")
    old = signal.signal(signal.SIGTERM, handler)
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, old)


def subprocess_launcher(step: Step) -> StageRun:
    """Run one stage in a child process through the shim (the production launcher)."""
    hb = step.heartbeat or _NoHeartbeat()
    for p in (step.kwargs_path, step.out_path, step.log_path):
        p.parent.mkdir(parents=True, exist_ok=True)
    step.out_path.unlink(missing_ok=True)
    lit_util.atomic_write_text(str(step.kwargs_path), json.dumps(step.kwargs, ensure_ascii=False, default=str))
    cmd = [sys.executable, "-m", "litpipe.runner", "_stage", "--module", step.module, "--kwargs",
           str(step.kwargs_path), "--out", str(step.out_path), "--config", str(step.registry)]
    env = {**os.environ, RUN_ENV: step.run_id, "PYTHONUTF8": "1", "PYTHONUNBUFFERED": "1"}
    t0, j0 = time.time(), hb.jumped()

    def elapsed():
        return max(0.0, (time.time() - t0) - (hb.jumped() - j0))

    try:
        tree = ProcessTree.start(cmd, cwd=str(REPO_ROOT), env=env, stdin=subprocess.DEVNULL,
                                 stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    except OSError as e:
        step.kwargs_path.unlink(missing_ok=True)
        return StageRun(None, None, problem=f"could not start the stage: {e}", timeout_s=step.timeout_s)
    proc = tree.proc
    pump = _Pump(proc.stdout, step.log_path).start()
    killed, how, rc = "", "", None
    try:
        while True:
            try:
                rc = proc.wait(timeout=POLL_S)
                break
            except subprocess.TimeoutExpired:
                pass
            if hb.lost.is_set():
                killed = "heartbeat"
                break
            if elapsed() > step.timeout_s:
                killed = "timeout"
                break
        if killed:
            how = tree.kill()
            rc = proc.poll()
    except BaseException:                     # KeyboardInterrupt, SIGTERM: the tree dies with the run
        tree.kill()
        raise
    finally:
        pump.thread.join(JOIN_S)
        tree.close()
        pump.thread.join(JOIN_S)
        abandoned = pump.thread.is_alive()
        pump.close()
        try:
            proc.stdout.close()
        except OSError:
            pass
        step.kwargs_path.unlink(missing_ok=True)
    result, problem = (None, "") if killed else read_result(step.out_path, RESULT_TABLE[step.job][0])
    return StageRun(rc, result, problem, killed, how, pump.traceback, pump.lines, round(elapsed(), 1),
                    step.timeout_s, str(step.log_path), abandoned)


def inprocess_launcher(step: Step) -> StageRun:
    """The same shim in this process, its bindings saved and restored (tests; amendment 16). No
    timeout can be enforced in-process."""
    for p in (step.out_path, step.log_path):
        p.parent.mkdir(parents=True, exist_ok=True)
    step.out_path.unlink(missing_ok=True)
    buf = io.StringIO()
    t0 = time.time()
    rc = 0
    with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
        try:
            shim(step.module, json.loads(json.dumps(step.kwargs, default=str)), step.out_path, step.registry,
                 restore=True)
        except SystemExit as e:
            rc = e.code if isinstance(e.code, int) else 1
            traceback.print_exc()
        except Exception:   # noqa: BLE001 - the stage crashed: ERROR, as from a child
            rc = 1
            traceback.print_exc()
    text = buf.getvalue()
    lines = text.splitlines()
    with open(step.log_path, "w", encoding="utf-8", newline="\n") as f:
        for line in lines:
            f.write(ledger.redact(line) + "\n")
    result, problem = read_result(step.out_path, RESULT_TABLE[step.job][0])
    return StageRun(rc, result, problem, "", "", any(TRACEBACK_MARK in ln for ln in lines), len(lines),
                    round(time.time() - t0, 1), step.timeout_s, str(step.log_path))


# ------------------------------------------------------------------------------ the index fingerprint
def index_fingerprint(lib):
    """sha256 over snowball.library_fingerprint(lib) and (name, size, mtime) of the walker CSVs the
    index ingests (_forward_citations.csv, _reverse_citations_parsed.csv, scoped forward CSVs);
    None when the library cannot be read."""
    if lib is None or not Path(lib).is_dir():
        return None
    import index_portfolio
    import snowball
    base = snowball.library_fingerprint(lib)
    if base is None:
        return None
    files = []
    try:
        with os.scandir(lib) as it:
            for e in it:
                n = e.name
                if n in ("_forward_citations.csv", "_reverse_citations_parsed.csv") or \
                        index_portfolio.SCOPED_FORWARD_RE.match(n):
                    try:
                        s = e.stat()
                    except OSError:
                        continue
                    files.append([n, s.st_size, s.st_mtime_ns])
    except OSError:
        return None
    payload = json.dumps([list(map(list, base)), sorted(files)], ensure_ascii=False, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# ------------------------------------------------------------------------------ hints and counts
def ncbi_offpeak(ts) -> dict:
    """NCBI's off-peak window (US Eastern 21:00-05:00 on weekdays, all weekend). A hint only: the
    runner never blocks or delays on it."""
    rule = "US Eastern 21:00-05:00 on weekdays, all weekend (a hint; never delays a run)"
    try:
        from zoneinfo import ZoneInfo
        t = datetime.fromtimestamp(ts, ZoneInfo("America/New_York"))
    except Exception as e:   # noqa: BLE001 - no tz data: say so, never block
        return {"offpeak": None, "eastern": None, "rule": rule, "note": f"zoneinfo unavailable ({type(e).__name__})"}
    off = t.weekday() >= 5 or t.hour >= 21 or t.hour < 5
    return {"offpeak": off, "eastern": t.isoformat(timespec="minutes"), "rule": rule}


def needs_ocr_counts(lib, since_ts) -> dict | None:
    """needs_ocr sidecars written since the run started: {needs_ocr, pdf_unreadable, command}."""
    if lib is None or not Path(lib).is_dir():
        return None
    n = bad = 0
    try:
        entries = list(os.scandir(lib))
    except OSError:
        return None
    for e in entries:
        if not e.name.endswith(".fulltext.json"):
            continue
        try:
            if e.stat().st_mtime < since_ts:
                continue
            with open(e.path, encoding="utf-8") as f:
                d = json.load(f)
        except (OSError, ValueError):
            continue
        if not isinstance(d, dict) or d.get("needs_ocr") is not True:
            continue
        if str(d.get("needs_ocr_reason") or "").startswith("pdf unreadable"):
            bad += 1
        else:
            n += 1
    if not n and not bad:
        return None
    out = {"lib": str(lib), "needs_ocr": n, "pdf_unreadable": bad}
    if n:
        out["command"] = command("extract_pdf_fulltext.py", "--lib-dir", lib, "--ocr")
    if bad:
        out["pdf_unreadable_note"] = "re-fetch items: never sent to OCR"
    return out


def _is_crossref_error(r) -> bool:
    st = r.get("status")
    if st == 429 or (isinstance(st, int) and 500 <= st <= 599):
        return True
    return str(r.get("kind") or "") in ("TRANSPORT", "OUTAGE", "REFUSED")


# ------------------------------------------------------------------------------ one run
def earlier_runner(live, run_id):
    """The live `runner` run registered before `run_id` (DEC-02), or None. Registration order is
    the runs rowid (`seq`), never `started` (truncated to the second) or the run id (which sorts by
    pid within a second)."""
    mine = next((r for r in live if r["run_id"] == run_id), None)
    others = sorted((r for r in live if r["kind"] == "runner" and r["run_id"] != run_id
                     and (mine is None or r["seq"] < mine["seq"])), key=lambda r: r["seq"])
    return others[0] if others else None


class _Run:
    """Shared by `run` and `batch`: registration, heartbeat, launching, the summary and finish."""

    kind = "runner run"

    def __init__(self, cfg, keys, *, launcher=None, timeouts=None, json_path=None, db_writes=None,
                 profile="daily", scheduled=False, force_walks=False):
        self.cfg = cfg
        self.keys = keys
        self.projects = _projects(cfg, keys)
        self.launcher = launcher or subprocess_launcher
        self.timeouts = {**TIMEOUTS, **(timeouts or {})}
        self.json_path = json_path
        self.runner_cfg = runner_block(cfg)
        if db_writes is None:
            self.db_writes, self.db_writes_source = self.runner_cfg["unattended_db_writes"], "projects.json"
        else:
            self.db_writes, self.db_writes_source = bool(db_writes), ("--db-writes" if db_writes else "--no-db-writes")
        self.profile = profile
        self.effective = profile
        self.scheduled = scheduled
        self.force_walks = force_walks
        self.registry = Path(config.CONFIG_PATH)
        self.db_path = config.db_dir(cfg) / "portfolio.duckdb"
        self.t0 = _now()
        self.wall0 = time.time()
        self.run_date = datetime.fromtimestamp(self.t0).date().isoformat()
        self.run_id = None
        self.run_dir = None
        self.hb = None
        self.jobs = []
        self.aborted = None
        self.finish_status = None
        self.step_n = 0
        self.sweep_abort = None
        self.index_skipped_db = set()
        self.skipped_db = []
        self.chronic = {}
        self.crossref = {"lines": 0, "errors": 0, "share": 0.0, "refused": False}
        self.canary_outs = []
        self.canary_report = None
        self.health = {"status": None, "counted": [], "not_counted": []}
        self.loose = {}
        self.order, self.order_source, self.ab_used = None, None, []
        self.order_yield = {}
        self.unpaywall_ran = False
        self.skip_preprint = False
        self.exit_code = None
        self._prev_ledger_run = None
        self.extra = {}

    # -- jobs
    def _job(self, project, job, status, reason="", *, exit=None, elapsed=0.0, counts=None, log=None,
             command_=None, counts_for_exit=None):
        if counts_for_exit is None:
            counts_for_exit = status in FAILURE_STATUSES
        rec = {"project": project, "job": job, "status": status, "reason": reason, "exit": exit,
               "elapsed_s": elapsed, "counts": counts or {}, "log": log, "command": command_,
               "counts_for_exit": bool(counts_for_exit)}
        self.jobs.append(rec)
        tag = f"{project}/{job}" if project else job
        _say(f"{tag}: {status}" + (f" ({reason})" if reason else "") + (f"; run: {command_}" if command_ else ""))
        return rec

    def _launch(self, job, project, kwargs):
        if self.hb is not None and not self.hb.check_before_launch():
            self._job(project, job, ABORTED, HEARTBEAT_LOST + (f": {self.hb.lost_reason}" if self.hb.lost_reason else ""))
            raise _Abort(HEARTBEAT_LOST)
        self.step_n += 1
        slug = f"{self.step_n:02d}-{job}" + (f"-{_slug(project)}" if project else "")
        step = Step(job, STAGE_MODULES[job], kwargs, project, float(self.timeouts.get(job, DEFAULT_TIMEOUT_S)),
                    self.run_dir / "logs" / f"{slug}.log", self.run_dir / "stages" / f"{slug}.kwargs.json",
                    self.run_dir / "stages" / f"{slug}.result.json", self.registry, self.run_id, self.hb)
        _say(f"{slug}: {step.module}.run() (timeout {int(step.timeout_s)} s)")
        try:
            sr = self.launcher(step)
        except _Abort:
            raise
        except (KeyboardInterrupt, _Terminated) as e:     # the launcher killed the child's tree first
            why = "terminated (SIGTERM)" if isinstance(e, _Terminated) else "interrupted"
            self._job(project, job, ABORTED, f"{why}: the stage's process tree was killed", log=str(step.log_path))
            raise
        except Exception as e:   # noqa: BLE001 - a launcher bug is that stage's ERROR
            sr = StageRun(None, None, problem=f"launcher raised {type(e).__name__}: {ledger.redact(str(e))[:200]}",
                          timeout_s=step.timeout_s, log=str(step.log_path))
        status, reason, code = classify(job, sr, project)
        counts = kept_counts(job, sr.result, project)
        if sr.lines:
            counts["log_lines"] = sr.lines
        if sr.kill_method:
            counts["killed_by"] = sr.kill_method
        rec = self._job(project, job, status, reason, exit=code, elapsed=sr.elapsed_s, counts=counts, log=sr.log)
        if sr.killed == "heartbeat":
            raise _Abort(HEARTBEAT_LOST)
        return rec, sr.result

    # -- registration and the one-runner rule
    def _open(self):
        self.run_id = state.register_run("runner", writer=True)
        self._prev_ledger_run = ledger.RUN_ID
        ledger.set_run_id(self.run_id)
        first = earlier_runner(state.live_runs(), self.run_id)
        if first is not None:
            why = f"refused: runner {first['run_id']} is live"
            state.finish_run(self.run_id, status=why)
            self.finish_status = why
            self.run_dir = config.state_dir(self.cfg) / "runner" / self.run_id
            raise _Abort(why)
        self.run_dir = config.state_dir(self.cfg) / "runner" / self.run_id
        (self.run_dir / "logs").mkdir(parents=True, exist_ok=True)
        self.hb = Heartbeat(self.run_id).start()
        _say(f"run {self.run_id} registered ({self.kind}; profile {self.profile}"
             + (f" -> {self.effective}" if self.effective != self.profile else "") + ")")

    def _preflight(self):
        outs = preflight.run(own_run_id=self.run_id)
        bad = [o for o in outs if o.kind not in preflight.PASS_KINDS]
        if not preflight.ok(outs):
            why = "; ".join(f"{(o.payload or {}).get('check', o.host)}: {o.kind} {o.detail}"[:200] for o in bad)
            self._job(None, "preflight", FAILED, why, exit=preflight.exit_code(outs))
            self.finish_status = "preflight"
            raise _Abort(f"preflight failed: {why}")
        self._job(None, "preflight", OK, counts={"checks": len(outs)}, counts_for_exit=False)

    def _canary_projects(self):
        return [{"key": p.key, "sources": list(p.sources)} for p in self.projects]

    def _network_canaries(self, profile):
        try:
            outs = canaries.run(profile, phase="network", context={"run_id": self.run_id,
                                                                    "projects": self._canary_projects()},
                                cfg=self.cfg, now=self.t0)
        except Exception as e:   # noqa: BLE001 - recorded, the run goes on
            self._job(None, "canaries_network", ERROR, f"{type(e).__name__}: {ledger.redact(str(e))[:200]}")
            return
        self.canary_outs += list(outs or [])
        alarms = sum(1 for o in outs or [] if (o.payload or {}).get("status") in ("ALARM", "ERROR"))
        self._job(None, "canaries_network", OK, counts={"checks": len(outs or []), "alarm": alarms},
                  counts_for_exit=False)

    # -- Crossref's 10 % rule
    def _crossref_check(self):
        if self.crossref["refused"]:
            return
        day = datetime.fromtimestamp(self.wall0, timezone.utc).date()
        end = datetime.now(timezone.utc).date()
        rows = []
        while day <= end:
            rows += ledger.read(date=day.isoformat())
            day += timedelta(days=1)
        rel = [r for r in rows if r.get("run_id") == self.run_id and r.get("host") == CROSSREF_HOST
               and r.get("attempt") == 1 and r.get("hop") == 0
               and not str(r.get("purpose") or "").startswith("canary:")
               and r.get("decision") not in ("not_sent", "prohibited")]
        n = len(rel)
        err = sum(1 for r in rel if _is_crossref_error(r))
        share = err / n if n else 0.0
        self.crossref.update(lines=n, errors=err, share=round(share, 4))
        if n >= CROSSREF_MIN_LINES and share >= CROSSREF_ERROR_SHARE:
            reason = (f"runner: {err} of {n} first Crossref attempts this run were errors "
                      f"({share:.0%} >= {CROSSREF_ERROR_SHARE:.0%})")
            state.refuse(CROSSREF_HOST, reason, persistence="run", run_id=self.run_id)
            self.crossref["refused"] = True
            self.crossref["reason"] = reason
            _say(f"{CROSSREF_HOST} refused for the run: {reason}")

    # -- the end of a run
    def _hosts(self):
        try:
            st = state.status()
        except Exception as e:   # noqa: BLE001
            return {"error": f"{type(e).__name__}"}
        refused = [{"host": h["host"], "refused": h["refused"], "reason": h["refused_reason"]}
                   for h in st.get("hosts", []) if h.get("refused")]
        deferred = [{"host": h["host"], "until": h["deferred_until"], "reason": h["defer_reason"]}
                    for h in st.get("hosts", []) if h.get("deferred_until")]
        return {"refused": refused, "deferred": deferred}

    def _health(self):
        rep = self.canary_report
        if not rep:
            return
        counted, not_counted = [], []
        for c in rep.get("checks") or []:
            if c.get("status") not in ("ALARM", "ERROR"):
                continue
            if c.get("id") == "index_freshness" and c.get("status") == "ALARM" and c.get("host") in self.index_skipped_db:
                not_counted.append({"id": c.get("id"), "host": c.get("host"), "note": "index stale: DB writes off",
                                    "command": command("index_portfolio.py", "--project", c.get("host"),
                                                       "--db", self.db_path)})
                continue
            counted.append({"id": c.get("id"), "host": c.get("host"), "status": c.get("status"),
                            "observed": canaries._observed_text(c)[:200]})
        self.health = {"status": "ALARM" if counted else "PASS", "counted": counted, "not_counted": not_counted}

    def _compute_exit(self):
        if self.aborted:
            return EXIT_ABORTED
        bad = [j for j in self.jobs if j["counts_for_exit"] and j["status"] in FAILURE_STATUSES]
        return EXIT_FAILURES if bad or self.health.get("counted") else EXIT_OK

    def _reasons(self):
        out = []
        for j in self.jobs:
            if j["counts_for_exit"] and j["status"] in FAILURE_STATUSES:
                out.append(f"{j['project'] or 'portfolio'} {j['job']} {j['status']}" +
                           (f": {j['reason']}" if j["reason"] else ""))
        for c in self.health.get("counted") or []:
            out.append(f"health {c['id']} {c['host']}: {c['status']} {c['observed']}".strip())
        if self.aborted:
            out.append(f"aborted: {self.aborted}")
        return [ledger.redact(r)[:300] for r in out]

    def _transport_failures(self):
        n = 0
        for j in self.jobs:
            v = (j.get("counts") or {}).get("transport_failures")
            if isinstance(v, int):
                n += v
        for c in (self.canary_report or {}).get("checks") or []:
            n += c.get("kind") == "TRANSPORT"
        return n

    def _loose_ends(self):
        import sweep
        for p in self.projects:
            sw = p.sweep
            if not sw or (not sw["results"] and not sw["refused"]):
                continue
            line = sweep.loose_end_line(p.key, sw["results"], sw["refused"])
            if not line:
                continue
            line = ledger.redact(line)
            st = sweep._loose_state(line)
            if _kv_get(f"loose_ends:{p.key}") == st:
                self.loose[p.key] = "unchanged"
                continue
            with _bound(sweep, "CONFIG_PATH", self.registry):
                path = sweep.append_loose_end(line)
            if path is None:
                self.loose[p.key] = "unconfigured: projects.json has no loose_ends key; nothing written"
                continue
            state.kv_set(KV, f"loose_ends:{p.key}", st)
            self.loose[p.key] = "written"

    def _stamps(self):
        pass

    def _summary(self, finished):
        projects = {}
        for j in self.jobs:
            projects.setdefault(j["project"] or "_portfolio", []).append(j)
        needs = {}
        for p in self.projects:
            c = needs_ocr_counts(p.lib, self.wall0)
            if c:
                needs[p.key] = c
        out = {
            "run_id": self.run_id, "kind": self.kind, "profile_requested": self.profile,
            "profile_effective": self.effective, "scheduled": self.scheduled,
            "started": _iso(self.t0), "finished": _iso(finished), "exit_code": self.exit_code,
            "aborted": self.aborted, "db_writes": self.db_writes, "db_writes_source": self.db_writes_source,
            "registry": str(self.registry), "projects_selected": list(self.keys),
            "jobs": self.jobs, "by_project": projects, "reasons": self._reasons(),
            "transport_failures": self._transport_failures(),
            "hosts_at_end": self._hosts(), "canaries": self.canary_report, "health": self.health,
            "candidate_order": {"order": self.order, "source": self.order_source,
                                "ab_used_before": self.ab_used, "unpaywall_ran": self.unpaywall_ran,
                                "per_project": self.order_yield},
            "ncbi_offpeak": ncbi_offpeak(self.t0), "chronic_walk_failures": self.chronic,
            "skipped_db_jobs": self.skipped_db, "needs_ocr": needs, "crossref_rule": self.crossref,
            "loose_ends": self.loose,
            "heartbeat": ({"beats": self.hb.beats, "errors": self.hb.errors, "jumps": self.hb.jumps,
                           "jumped_s": round(self.hb.jumped(), 1), "lost": self.hb.lost.is_set(),
                           "lost_reason": self.hb.lost_reason} if isinstance(self.hb, Heartbeat) else None),
        }
        out.update(self.extra)
        return ledger.redact_obj(json.loads(json.dumps(out, default=str)))

    def _write_summary(self, summ):
        if self.run_dir is None:
            return None
        self.run_dir.mkdir(parents=True, exist_ok=True)
        path = self.run_dir / "summary.json"
        lit_util.atomic_write_text(str(path), json.dumps(summ, ensure_ascii=False, indent=1) + "\n")
        if self.json_path:
            jp = Path(self.json_path)
            jp.parent.mkdir(parents=True, exist_ok=True)
            lit_util.atomic_write_text(str(jp), json.dumps(summ, ensure_ascii=False, indent=1) + "\n")
        return path

    def _prune(self):
        base = config.state_dir(self.cfg, create=False) / "runner"
        if not base.is_dir():
            return 0
        def age(d):                      # the run id's UTC second, then the directory's mtime
            try:
                return d.name[:16], d.stat().st_mtime_ns
            except OSError:
                return d.name[:16], 0
        dirs = sorted((d for d in base.iterdir() if d.is_dir() and RUN_DIR_RE.match(d.name)), key=age)
        old = [d for d in dirs[:-KEEP_RUN_DIRS] if d.name != self.run_id] if len(dirs) > KEEP_RUN_DIRS else []
        for d in old:
            shutil.rmtree(d, ignore_errors=True)
        return len(old)

    def _finish(self):
        """LOOSE_ENDS, stamps, the summary, retention, then finish_run (always)."""
        if self.hb is not None:
            self.hb.stop()
        summ = None
        refused = (self.finish_status or "").startswith("refused")
        try:
            if self.run_id is not None and not refused:
                try:
                    self._loose_ends()
                except Exception as e:   # noqa: BLE001 - reported, the run still finishes
                    self.loose["_error"] = f"{type(e).__name__}: {ledger.redact(str(e))[:200]}"
            self._health()
            self.exit_code = self._compute_exit()
            if self.run_id is not None and not self.aborted and self.exit_code in (EXIT_OK, EXIT_FAILURES):
                try:
                    self._stamps()
                except Exception as e:   # noqa: BLE001
                    self.extra["stamp_error"] = f"{type(e).__name__}: {e}"
            self._consume_ab()
            finished = _now()
            summ = self._summary(finished)
            path = self._write_summary(summ)
            if path:
                _say(f"summary: {path}")
            try:
                self._prune()
            except OSError:
                pass
            if self.exit_code in (EXIT_FAILURES, EXIT_ABORTED):
                step = {"reasons": summ["reasons"], "aborted": self.aborted,
                        "transport_failures": summ["transport_failures"]}
                print("[step-summary] " + json.dumps(ledger.redact_obj(step), ensure_ascii=False), flush=True)
        finally:
            if refused:
                pass                                   # finished before anything else (amendment 3)
            elif self.run_id is not None:
                status = self.finish_status or {EXIT_OK: "ok", EXIT_FAILURES: "completed with failures",
                                                EXIT_ABORTED: f"aborted: {self.aborted}"}.get(self.exit_code, "ended")
                try:
                    state.finish_run(self.run_id, status=status)
                except Exception as e:   # noqa: BLE001
                    print(f"[runner] finish_run failed: {ledger.redact(str(e))}", file=sys.stderr)
            ledger.set_run_id(self._prev_ledger_run)
        return summ

    def _consume_ab(self):
        pass


class _ScheduledRun(_Run):
    """`runner run`."""

    def __init__(self, cfg, keys, *, dry_run=False, **kw):
        super().__init__(cfg, keys, **kw)
        self.dry_run = dry_run

    # -- escalation and the candidate order
    def _escalate(self, read_only=False):
        eff = self.profile
        if self.scheduled:
            now = self.t0
            w = _kv_get("profile_done:weekly", read_only=read_only)
            m = _kv_get("profile_done:monthly", read_only=read_only)
            if RANK[eff] < RANK["weekly"] and (w is None or now - float(w) >= WEEKLY_AFTER_S):
                eff = "weekly"
            if RANK[eff] < RANK["monthly"] and (m is None or now - float(m) >= MONTHLY_AFTER_S):
                eff = "monthly"
        self.effective = eff
        return eff

    def _choose_order(self, read_only=False):
        cfg_order = self.runner_cfg["candidate_order"]
        if self.scheduled:
            used = _kv_get("candidate_order_ab", [], read_only=read_only) or []
            self.ab_used = list(used)
            if len(used) < len(AB_ORDERS):
                self.order, self.order_source = AB_ORDERS[len(used)], "a/b"
                return
            self.order, self.order_source = (cfg_order or "repository"), ("projects.json" if cfg_order else "default")
            return
        self.order, self.order_source = cfg_order, ("projects.json" if cfg_order else None)

    def _stamps(self):
        if not self.scheduled:          # a manual run never moves the nightly task's escalation
            return
        end = _now()
        if RANK[self.effective] >= RANK["weekly"]:
            state.kv_set(KV, "profile_done:weekly", end)
        if RANK[self.effective] >= RANK["monthly"]:
            state.kv_set(KV, "profile_done:monthly", end)

    def _consume_ab(self):
        if self.run_id is None or self.order_source != "a/b" or not self.unpaywall_ran:
            return
        if self.finish_status and self.finish_status.startswith("refused"):
            return
        try:
            state.kv_set(KV, "candidate_order_ab", list(self.ab_used) + [self.order])
        except Exception as e:   # noqa: BLE001
            self.extra["ab_error"] = f"{type(e).__name__}: {e}"

    # -- per project
    def _auto_stage(self, p):
        root = p.root
        queue = root / QUEUE_FILE
        if queue.exists() and _data_lines(queue) > 1:
            self._job(p.key, "seed", SKIPPED, f"{QUEUE_FILE} is staged with rows: not seeding over it",
                      counts_for_exit=False)
            return
        rec, res = self._launch("seed", p.key, {"project": p.key})
        if rec["status"] not in (OK, DEGRADED):
            p.seed_failed = True
            return
        draft = root / DRAFT_FILE
        out = Path(str((res or {}).get("output") or ""))
        if (res or {}).get("output") and out.is_file():
            draft = out
        if not draft.is_file():
            rec["counts"]["staged"] = 0
            rec["reason"] = (rec["reason"] + "; " if rec["reason"] else "") + "no draft produced"
            return
        with open(draft, encoding="utf-8") as f:
            data_rows = sum(1 for line in f if line.strip() and not line.startswith("#")
                            and not line.startswith("doi,"))
        if data_rows == 0:
            draft.unlink()
            rec["counts"]["staged"] = 0
            rec["reason"] = (rec["reason"] + "; " if rec["reason"] else "") + "0 candidates after filters; empty draft removed"
            return
        if queue.exists() and queue.stat().st_size > 0:
            backup = root / "lit_pull_queue.bak.csv"
            if backup.exists():          # keep the prior good backup; a timestamp only on collision
                backup = root / f"lit_pull_queue.bak.{datetime.now():%Y%m%d_%H%M%S}.csv"
            queue.replace(backup)
            rec["counts"]["backup"] = backup.name
            _say(f"{p.key}: existing queue backed up to {backup.name} before staging the draft")
        with open(draft, encoding="utf-8") as fin:
            staged = "".join(line for line in fin if not line.startswith("#"))
        lit_util.atomic_write_text(str(queue), staged)
        draft.unlink()
        p.staged_rows = data_rows
        rec["counts"]["staged"] = data_rows
        _say(f"{p.key}: staged {data_rows} DOIs to {QUEUE_FILE}")

    def _staged(self, p):
        import sweep
        queues = sweep.discover_queues(p.root)
        try:
            due = sweep.split_retry_later(p.root, self.run_date)[1]
        except (OSError, ValueError, csv.Error):
            due = []
        return queues, due

    def _sweep(self, p):
        if p.seed_failed:
            self._job(p.key, "sweep", SKIPPED, "the seed step failed: nothing staged this run", counts_for_exit=False)
            return
        if self.sweep_abort:
            self._job(p.key, "sweep", SKIPPED, f"CONFIG abort in {self.sweep_abort}", counts_for_exit=False)
            return
        queues, due = self._staged(p)
        if not queues and not due:
            self._job(p.key, "sweep", SKIPPED, "nothing staged", counts_for_exit=False)
            return
        kwargs = {"project": p.key, "date": self.run_date, "loose_ends": False}
        if self.order:
            kwargs["candidate_order"] = self.order
        if self.skip_preprint:
            kwargs["skip_preprint"] = True
        rec, res = self._launch("sweep", p.key, kwargs)
        proj = ((res or {}).get("projects") or {}).get(p.key) or {}
        p.sweep = {"exit": rec["exit"], "status": rec["status"], "run_id": proj.get("run_id"),
                   "results": [r for r in proj.get("results") or [] if isinstance(r, dict) and not r.get("dry")],
                   "refused": list(proj.get("refused") or [])}
        if rec["exit"] == 2:
            self.sweep_abort = p.key
        ran = any((r.get("stages") or {}).get("unpaywall") == "completed" for r in p.sweep["results"])
        if ran:
            self.unpaywall_ran = True
            c = rec["counts"]
            self.order_yield[p.key] = {"order": self.order or "repository (the stage's default)",
                                       "rows": c.get("rows", 0), "unpaywall_downloads": c.get("unpaywall", 0),
                                       "downloaded": c.get("downloaded", 0)}
        self._crossref_check()

    def _route(self, p, tags=None):
        sw = p.sweep
        if not sw:
            return
        if not sw["results"]:
            self._job(p.key, "route", SKIPPED, "nothing swept to route", counts_for_exit=False)
            return
        if sw["exit"] == 2 and not any(r.get("retired") for r in sw["results"]):
            self._job(p.key, "route", SKIPPED, "CONFIG aborts the run: nothing is routed", counts_for_exit=False)
            return
        if not sw["run_id"]:
            self._job(p.key, "route", ERROR, "the sweep result names no run id")
            return
        import sweep
        skip = bool(self.skip_preprint) or sweep._preprint_excluded(p.key, self.cfg)
        kwargs = {"project": p.key, "run_id": sw["run_id"], "skip_preprint": skip}
        if tags:
            kwargs["tags"] = list(tags)
        rec, res = self._launch("route", p.key, kwargs)
        p.route_status = (res or {}).get("status")

    def _walk_due(self, p, read_only=False):
        if self.force_walks:
            return True, "forced (--with-snowball)"
        if p.cadence is None:
            return False, "walk_cadence_days unset: never walked unattended"
        if RANK[self.effective] < RANK["daily"]:
            return False, f"profile {self.effective}: walks run daily and up"
        last = _kv_get(f"walk:{p.key}", read_only=read_only)
        if last is not None and self.t0 - float(last) < p.cadence * 86400:
            nxt = _iso(float(last) + p.cadence * 86400)
            return False, f"walk not due (every {p.cadence} d; next after {nxt})"
        return True, "due"

    def _failed_seeds(self, since=None):
        from litpipe import walk
        try:
            with walk.Cache(walk.cache_path(self.cfg)) as c:
                st = c.states()
        except walk.CacheLocked:
            return None, "chronic: unknown (cache locked)"
        except Exception as e:   # noqa: BLE001 - CacheUnreadable or a driver error: unknown, never waited on
            return None, f"chronic: unknown (cache unreadable: {type(e).__name__})"
        out = set()
        for k, v in st.items():
            if v.get("state") != "failed":
                continue
            if since is not None:
                w = v.get("walked_at")
                if isinstance(w, datetime):
                    w = w if w.tzinfo else w.replace(tzinfo=timezone.utc)
                    if w < since:
                        continue
            out.add(tuple(k))
        return out, ""

    def _walk(self, p):
        due, why = self._walk_due(p)
        if not due:
            self._job(p.key, "walk", SKIPPED, why, counts_for_exit=False)
            return
        if _host_refused(S2_HOST):
            self._job(p.key, "walk", DEFERRED, f"{S2_HOST} is refused or deferred for this run")
            return
        before, note = self._failed_seeds()
        start = datetime.now(timezone.utc) - timedelta(seconds=1)
        from litpipe import walk
        rec, res = self._launch("walk", p.key, {"project": p.key, "cache_path": str(walk.cache_path(self.cfg))})
        after, note2 = self._failed_seeds(since=start)
        info = {"failed_before": None if before is None else len(before)}
        if before is None or after is None:
            info["chronic"] = note or note2
        else:
            chronic, new = after & before, after - before
            info.update(chronic=len(chronic), new=len(new), chronic_seeds=sorted(d for d, _ in chronic)[:50],
                        new_seeds=sorted(d for d, _ in new)[:50])
            reasons = (res or {}).get("reasons") or []
            five = bool(reasons) and all("seed walks failed" in str(r) for r in reasons)
            if rec["status"] == DEGRADED and rec["exit"] == 2 and five and chronic and not new:
                rec["reason"] = f"chronic only: {len(chronic)} seed(s) failed before and still fail ({rec['reason']})"
                rec["counts_for_exit"] = False
        rec["counts"]["chronic"] = info
        self.chronic[p.key] = info
        if (res or {}).get("exit_code") in (0, 2):
            state.kv_set(KV, f"walk:{p.key}", _now())
        self._crossref_check()

    def _reverse(self):
        weekly = RANK[self.effective] >= RANK["weekly"]
        from litpipe import s2
        for p in self.projects:
            if p.cadence is None and not self.force_walks:
                continue
            if not weekly and not self.force_walks:
                self._job(p.key, "reverse", SKIPPED, f"weekly job (profile {self.effective})", counts_for_exit=False)
                continue
            kwargs = {"project": p.key}
            if not s2.key_present() or _host_refused(S2_HOST):
                kwargs["sources"] = REVERSE_SOURCES_UNKEYED
            self._launch("reverse", p.key, kwargs)

    def _index(self):
        for p in self.projects:
            if p.lib is None:
                self._job(p.key, "index", SKIPPED, "no lib_dir in the registry", counts_for_exit=False)
                continue
            fp = index_fingerprint(p.lib)
            if fp is None:
                self._job(p.key, "index", SKIPPED, f"library not readable: {p.lib}", counts_for_exit=False)
                continue
            if _kv_get(f"index_fp:{p.key}") == fp:
                self._job(p.key, "index", SKIPPED, "library unchanged since its last index", counts_for_exit=False)
                continue
            cmd = command("index_portfolio.py", "--project", p.key, "--db", self.db_path)
            if not self.db_writes:
                self.index_skipped_db.add(p.key)
                self.skipped_db.append({"project": p.key, "job": "index", "command": cmd})
                self._job(p.key, "index", SKIPPED, "DB writes off", command_=cmd, counts_for_exit=False)
                continue
            rec, res = self._launch("index", p.key, {"project": p.key, "db": str(self.db_path)})
            if (res or {}).get("exit_code") == 0:
                state.kv_set(KV, f"index_fp:{p.key}", fp)

    def _profile_jobs(self):
        weekly = RANK[self.effective] >= RANK["weekly"]
        monthly = RANK[self.effective] >= RANK["monthly"]
        from litpipe import s2
        keyed = s2.key_present()
        cmd = command("enrich_abstracts.py", "--db", self.db_path)
        if not weekly:
            self._job(None, "abstracts", SKIPPED, f"weekly job (profile {self.effective})", counts_for_exit=False)
        elif not self.db_writes:
            self.skipped_db.append({"project": None, "job": "abstracts", "command": cmd})
            self._job(None, "abstracts", SKIPPED, "DB writes off", command_=cmd, counts_for_exit=False)
        else:
            self._launch("abstracts", None, {"db": str(self.db_path)})
        cmd = command("enrich_recommendations.py", "--recent-feed", "--db", self.db_path)
        if not monthly:
            self._job(None, "recommendations", SKIPPED, f"monthly job (profile {self.effective})", counts_for_exit=False)
        elif not keyed:
            self._job(None, "recommendations", SKIPPED, "no S2 key (DEC-18: off until the key)", counts_for_exit=False)
        elif not self.db_writes:
            self.skipped_db.append({"project": None, "job": "recommendations", "command": cmd})
            self._job(None, "recommendations", SKIPPED, "DB writes off", command_=cmd, counts_for_exit=False)
        else:
            self._launch("recommendations", None, {"db": str(self.db_path), "recent_feed": True})
        if not monthly:
            self._job(None, "s2_keepalive", SKIPPED, f"monthly job (profile {self.effective})", counts_for_exit=False)
        elif not keyed:
            self._job(None, "s2_keepalive", SKIPPED, "no S2 key: nothing sent", counts_for_exit=False)
        else:
            self._keepalive()
        if not weekly:
            self._job(None, "audit", SKIPPED, f"weekly job (profile {self.effective})", counts_for_exit=False)
        else:
            self._launch("audit", None, {"holdings": False, "json_path": str(self.run_dir / "audit.json")})

    def _keepalive(self):
        """One keyed S2 call, in-process, so an idle key is not pruned (S2 prunes after about 60 days)."""
        from litpipe import s2
        if self.hb is not None and not self.hb.check_before_launch():
            self._job(None, "s2_keepalive", ABORTED, HEARTBEAT_LOST)
            raise _Abort(HEARTBEAT_LOST)
        t = time.time()
        try:
            o = s2.paper_batch([f"DOI:{KEEPALIVE_DOI}"], fields="paperId", session=s2.Session())
        except Exception as e:   # noqa: BLE001
            self._job(None, "s2_keepalive", ERROR, f"{type(e).__name__}: {ledger.redact(str(e))[:160]}")
            return
        if o.ok:
            self._job(None, "s2_keepalive", OK, counts={"attempts": o.attempts}, elapsed=round(time.time() - t, 1),
                      counts_for_exit=False)
        else:
            self._job(None, "s2_keepalive", DEGRADED, f"{o.kind} {o.status or ''} {o.detail}"[:200],
                      counts={"attempts": o.attempts})

    def _local_ctx(self):
        import sweep
        projects = []
        for p in self.projects:
            sw = p.sweep or {}
            results = sw.get("results") or []
            run_ids = sorted({r.get("run_id") for r in results if r.get("run_id")})
            stages = []
            if results:
                stages = ["unpaywall", "residual", "report"]
                for s in ("pmc", "preprint"):
                    if all((r.get("stages") or {}).get(s) in ("completed", "failed")
                           and (p.root / sweep.artifact_name(r.get("tag") or "", r.get("run_id"), s)).is_file()
                           for r in results):
                        stages.append(s)
                if all(r.get("retired") for r in results):
                    stages.append("processed")
                if p.route_status == "ok":
                    stages.append("routing")
            entry = {"key": p.key, "root": str(p.root), "sources": list(p.sources), "artifact_dir": str(p.root),
                     "sweep_run_ids": run_ids, "stages": stages}
            if p.lib is not None:
                entry["lib_dir"] = str(p.lib)
            projects.append(entry)
        return {"run_id": self.run_id, "since": datetime.fromtimestamp(self.wall0, timezone.utc).isoformat(),
                "db_path": str(self.db_path), "projects": projects}

    def _local_canaries(self):
        try:
            outs = canaries.run(self.effective, phase="local", context=self._local_ctx(), cfg=self.cfg)
            self.canary_outs += list(outs or [])
        except Exception as e:   # noqa: BLE001
            self._job(None, "canaries_local", ERROR, f"{type(e).__name__}: {ledger.redact(str(e))[:200]}")
        self._report()

    def _report(self):
        try:
            self.canary_report = canaries.report(self.canary_outs, run_id=self.run_id, profile=self.effective,
                                                 started=datetime.fromtimestamp(self.wall0, timezone.utc))
            print(canaries.summary(self.canary_report), flush=True)
        except Exception as e:   # noqa: BLE001
            self._job(None, "canaries_report", ERROR, f"{type(e).__name__}: {ledger.redact(str(e))[:200]}")

    # -- the whole run
    def _work(self):
        self._open()
        self._escalate()
        self._choose_order()
        _say(f"profile {self.profile}" + (f" (scheduled: effective {self.effective})" if self.scheduled else "")
             + f"; DB writes {'on' if self.db_writes else 'off'} ({self.db_writes_source}); candidate order "
             + f"{self.order or 'stage default'}")
        self._preflight()
        self._network_canaries(self.effective)
        for p in self.projects:
            if p.auto_stage:
                self._auto_stage(p)
            self._sweep(p)
            self._route(p)
            self._walk(p)
        self._reverse()
        self._index()
        self._profile_jobs()
        self._local_canaries()

    def execute(self):
        try:
            with _sigterm_aborts():
                self._work()
        except _Abort as e:
            self.aborted = e.reason
            if self.canary_outs and self.canary_report is None:
                self._report()
        except KeyboardInterrupt:
            self.aborted = "interrupted"
        except _Terminated:
            self.aborted = "terminated (SIGTERM)"
        except Exception as e:   # noqa: BLE001 - the runner itself crashed: still finish the run
            self.aborted = f"runner crashed: {type(e).__name__}: {ledger.redact(str(e))[:200]}"
            traceback.print_exc()
        finally:
            summ = self._finish()
        return summ

    # -- the dry run
    def plan(self):
        rows = []

        def add(project, job, verdict):
            rows.append({"project": project, "job": job, "plan": verdict})
            print(f"  {(project or 'portfolio'):<28} {job:<16} {verdict}")

        self._escalate(read_only=True)
        self._choose_order(read_only=True)
        print(f"# runner run --dry-run: profile {self.profile}"
              + (f" (scheduled: effective {self.effective})" if self.scheduled else "")
              + f"; DB writes {'on' if self.db_writes else 'off'} ({self.db_writes_source});"
              + f" candidate order {self.order or 'stage default'}; nothing is written or sent")
        from litpipe import s2
        keyed = s2.key_present()
        s2_out = _host_refused(S2_HOST, read_only=True)
        weekly = RANK[self.effective] >= RANK["weekly"]
        monthly = RANK[self.effective] >= RANK["monthly"]
        for p in self.projects:
            queue = p.root / QUEUE_FILE
            queues, due = self._staged(p)
            if not p.auto_stage:
                add(p.key, "seed", "skip: auto_stage off")
            elif queue.exists() and _data_lines(queue) > 1:
                add(p.key, "seed", f"skip: {QUEUE_FILE} is staged with rows")
            else:
                add(p.key, "seed", "would seed and stage a draft (seed_queue_from_top_candidates)")
            if queues or due:
                what = ", ".join(q.name for q in queues) + (f"; {len(due)} due retry_later row(s)" if due else "")
                add(p.key, "sweep", f"would sweep: {what.strip('; ')}")
                add(p.key, "route", "would route the sweep's run (migrate_closed_to_md)")
            elif p.auto_stage and not (queue.exists() and _data_lines(queue) > 1):
                add(p.key, "sweep", "would sweep the draft if the seeder stages one")
                add(p.key, "route", "would route the sweep's run if one runs")
            else:
                add(p.key, "sweep", "skip: nothing staged")
                add(p.key, "route", "skip: nothing staged")
            ok, why = self._walk_due(p, read_only=True)
            if ok and s2_out:
                add(p.key, "walk", f"skip: {S2_HOST} refused or deferred (would be DEFERRED)")
            else:
                add(p.key, "walk", "would walk (forward_citations)" if ok else f"skip: {why}")
            if p.cadence is not None or self.force_walks:
                if weekly or self.force_walks:
                    add(p.key, "reverse", "would run the backward top-up"
                        + ("" if keyed and not s2_out else f" with --sources {REVERSE_SOURCES_UNKEYED}"))
                else:
                    add(p.key, "reverse", f"skip: profile not escalated (weekly job; profile {self.effective})")
            if p.lib is None:
                add(p.key, "index", "skip: no lib_dir")
            elif not self.db_writes:
                add(p.key, "index", "skip: DB writes off; run: " + command("index_portfolio.py", "--project",
                                                                             p.key, "--db", self.db_path))
            else:
                add(p.key, "index", "would index when the library changed since its last index")
        if not weekly:
            add(None, "abstracts", f"skip: profile not escalated (weekly job; profile {self.effective})")
        elif not self.db_writes:
            add(None, "abstracts", "skip: DB writes off; run: " + command("enrich_abstracts.py", "--db", self.db_path))
        else:
            add(None, "abstracts", "would run enrich_abstracts")
        if not monthly:
            add(None, "recommendations", f"skip: profile not escalated (monthly job; profile {self.effective})")
        elif not keyed:
            add(None, "recommendations", "skip: S2 key absent")
        elif not self.db_writes:
            add(None, "recommendations", "skip: DB writes off; run: " + command(
                "enrich_recommendations.py", "--recent-feed", "--db", self.db_path))
        else:
            add(None, "recommendations", "would run enrich_recommendations --recent-feed")
        if not monthly:
            add(None, "s2_keepalive", f"skip: profile not escalated (monthly job; profile {self.effective})")
        else:
            add(None, "s2_keepalive", "would send one keyed S2 call" if keyed else "skip: S2 key absent (nothing sent)")
        add(None, "audit", "would run audit_portfolio (read-only, no holdings map)" if weekly
            else f"skip: profile not escalated (weekly job; profile {self.effective})")
        try:
            planned = canaries.planned_requests(self.effective, context={"projects": self._canary_projects()},
                                                cfg=self.cfg)
        except Exception as e:   # noqa: BLE001
            planned = {"error": f"{type(e).__name__}: {e}"}
        n = sum(v for v in planned.values() if isinstance(v, int))
        print(f"  canaries ({self.effective}, network): {n} planned request(s): "
              + ", ".join(f"{k} {v}" for k, v in planned.items()))
        print("  preflight: 2 requests (Unpaywall, Crossref) when LITPIPE_EMAIL is set")
        if self.json_path:
            print("  (--json is not written on a dry run)")
        return {"exit_code": EXIT_OK, "dry_run": True, "profile_requested": self.profile,
                "profile_effective": self.effective, "jobs": rows, "planned_requests": planned,
                "candidate_order": self.order}


def run(*, profile="daily", projects=None, dry_run=False, scheduled=False, db_writes=None, json_path=None,
        launcher=None, timeouts=None, force_walks=False) -> dict:
    """`runner run`. Returns the summary dict (its exit_code is the CLI's exit code)."""
    if profile not in RANK:
        print(f"[runner] usage: --profile must be one of {', '.join(PROFILES)}", file=sys.stderr)
        return {"exit_code": EXIT_CONFIG, "error": f"bad profile {profile!r}"}
    try:
        cfg, keys = load_registry(projects)
    except ConfigProblem as e:
        msg = ledger.redact(str(e))
        print(f"[runner] config: {msg}", file=sys.stderr)
        return {"exit_code": EXIT_CONFIG, "error": msg}
    r = _ScheduledRun(cfg, keys, dry_run=dry_run, launcher=launcher, timeouts=timeouts, json_path=json_path,
                      db_writes=db_writes, profile=profile, scheduled=scheduled, force_walks=force_walks)
    if dry_run:
        return r.plan()
    return r.execute()


# ------------------------------------------------------------------------------ runner batch
def batch_tag(pool_name) -> str:
    """The batch tag for a pool file: drop a leading `lit_pull_queue.` and the `.csv`, lower-case,
    every character outside [a-z0-9_-] becomes '-', leading characters that are neither letters nor
    digits go, then `b-` and at most 32 characters. lit_pull_queue.ch15_pool.csv -> b-ch15_pool;
    2024_cohort_pool.csv -> b-2024_cohort_pool; _ranked_pool.csv -> b-ranked_pool."""
    s = Path(pool_name).name
    if s.lower().startswith("lit_pull_queue."):
        s = s[len("lit_pull_queue."):]
    if s.lower().endswith(".csv"):
        s = s[:-4]
    s = re.sub(r"[^a-z0-9_-]", "-", s.lower())
    s = re.sub(r"^[^a-z0-9]+", "", s)
    return ("b-" + s)[:32]


def _same_file(a, b) -> bool:
    try:
        return os.path.normcase(os.path.realpath(a)) == os.path.normcase(os.path.realpath(b))
    except OSError:
        return False


def _queue_dois(path):
    import sweep
    from litpipe import worklists
    _, rows = sweep.read_queue(path)
    out = {}
    for r in rows:
        d = (r.get("doi") or "").strip()
        if d:
            out.setdefault(worklists.doi_key(d), d)
    return out


class _BatchRun(_ScheduledRun):
    kind = "runner batch"

    def __init__(self, cfg, key, *, pool, size, batches, tag, skip_preprint, stage_only, **kw):
        super().__init__(cfg, [key], **kw)
        self.key = key
        self.p = self.projects[0]
        self.pool_path = Path(pool).resolve()
        self.size = int(size)
        self.n_batches = int(batches)
        self.tag = tag
        self.skip_preprint = bool(skip_preprint)
        self.stage_only = bool(stage_only)
        self.batch_path = (self.p.root / f"lit_pull_queue.{tag}.csv").absolute()
        self.pool = None
        self.done = []
        self.extra["batch"] = {"pool": str(self.pool_path), "tag": tag, "batch_path": str(self.batch_path),
                               "size": self.size, "batches_requested": self.n_batches,
                               "stage_only": self.stage_only, "skip_preprint": self.skip_preprint,
                               "batches": self.done}

    # -- the checks that exit 1 before anything is written
    def check(self, pool):
        import sweep
        if not self.pool_path.is_file():
            raise _BatchStop(EXIT_CONFIG, f"pool not found: {self.pool_path}")
        if _same_file(self.batch_path, self.pool_path):
            raise _BatchStop(EXIT_CONFIG, f"the batch queue {self.batch_path.name} would be the pool CSV itself; "
                                          f"pass --tag")
        listed = sweep.discover_queues(self.p.root)
        if any(_same_file(q, self.pool_path) for q in listed):
            raise _BatchStop(EXIT_CONFIG, f"sweep would sweep the pool {self.pool_path.name} whole (it has doi and "
                                          f"destination columns in the project root); move or rename it first")
        ours = {os.path.normcase(os.path.realpath(b["batch_path"])) for b in pool.pending() if b.get("batch_path")}
        other = [q for q in listed if os.path.normcase(os.path.realpath(q)) not in ours
                 and sweep.queue_tag(q.name) != sweep.RETRY_TAG]
        if other:
            raise _BatchStop(EXIT_CONFIG, "another queue is staged in the project: "
                             + ", ".join(q.name for q in other) + "; sweep or move it before a batch")
        if self.batch_path.exists() and os.path.normcase(os.path.realpath(self.batch_path)) not in ours:
            raise _BatchStop(EXIT_CONFIG, f"{self.batch_path.name} exists and this pool does not track it as a "
                                          f"staged batch; it is never written over (move it, or pass --tag)")
        if not self.p.entry.get("lib_dir"):
            raise _BatchStop(EXIT_CONFIG, f"{self.key} has no lib_dir: a batch needs the registry library")

    def _destination(self):
        return os.path.relpath(lit_util.lib_paths(self.key, self.p.entry)[1], self.p.root).replace(os.sep, "/")

    def _write_batch(self, path, dois):
        from litpipe import worklists
        rows_by_key = {worklists.doi_key(r["doi"]): r for r in self.pool.rows()}
        dest = self._destination()
        out = []
        for d in dois:
            r = rows_by_key.get(worklists.doi_key(d)) or {"doi": d}
            out.append({"doi": r.get("doi") or d, "title": r.get("title", "") or "", "authors": r.get("authors", "") or "",
                        "year": r.get("year", "") or "", "destination": dest,
                        "notes": r.get("notes", "") or f"runner batch {self.tag}"})
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        lit_util.atomic_write_csv(str(path), out, list(QUEUE_COLUMNS))

    def _residual_classes(self, tag, run_id, dois):
        import sweep
        from litpipe import worklists
        path = self.p.root / sweep.artifact_name(tag, run_id, "residual")
        cls = {}
        if path.is_file():
            with open(path, encoding="utf-8-sig", newline="") as f:
                for r in csv.DictReader(f):
                    d = (r.get("doi") or "").strip()
                    if d:
                        cls[worklists.doi_key(d)] = (r.get("residual_class") or "").strip() or "UNKNOWN"
        return {d: cls.get(worklists.doi_key(d), "fetched") for d in dois}

    def _processed_run(self, tag, dois):
        """The run id of a processed artifact of `tag` that holds every DOI in `dois`, newest first."""
        import sweep
        from litpipe import worklists
        want = {worklists.doi_key(d) for d in dois}
        hits = []
        for q in self.p.root.glob(f"lit_pull_queue.{tag}.*.processed.csv"):
            a = sweep.parse_artifact(q.name)
            if not a or a.tag != tag or a.stage != "processed":
                continue
            try:
                have = set(_queue_dois(q))
            except (OSError, csv.Error, UnicodeDecodeError):
                continue
            if want <= have:
                hits.append(a.run_id)
        return sorted(hits, key=lambda rid: (rid[:10], int(rid[11:] or 1)))[-1] if hits else None

    def _sweep_batch(self, path, dois, label):
        """Sweep the batch file, route, mark swept. False when the batch was not retired."""
        import sweep
        tag = sweep.queue_tag(Path(path).name) or ""
        self.p.sweep = None
        self._sweep(self.p)
        sw = self.p.sweep
        if not sw:
            self.done.append({"label": label, "batch_path": str(path), "dois": len(dois), "swept": False,
                              "left_for_next_run": True, "reason": "the sweep did not run"})
            return False
        _checkpoint("after_sweep")
        mine = [r for r in sw.get("results") or [] if (r.get("tag") or "") == tag]
        if not mine or not all(r.get("retired") for r in mine):
            why = "; ".join(f"{r.get('queue')}: {r.get('keep_reason') or 'kept'}" for r in mine) or \
                  "no result for the batch queue"
            self.done.append({"label": label, "batch_path": str(path), "dois": len(dois), "swept": False,
                              "left_for_next_run": True, "reason": why})
            self._job(self.key, "batch", DEGRADED, f"{Path(path).name} not retired ({why}); nothing marked swept; "
                                                   f"left for the next runner batch")
            return False
        self._route(self.p, tags=None)
        _checkpoint("after_route")
        run_id = sw["run_id"]
        classes = self._residual_classes(tag, run_id, dois)
        self.pool.mark_swept(dois, run_id, classes)
        _checkpoint("after_mark_swept")
        counts = {}
        for c in classes.values():
            counts[c] = counts.get(c, 0) + 1
        self.done.append({"label": label, "batch_path": str(path), "dois": len(dois), "swept": True,
                          "sweep_run_id": run_id, "classes": counts})
        return True

    def _resolve_pending(self):
        """Resume first (amendment 11). True when every pending batch is resolved; self.resumed
        counts the pending batches swept now (they count toward --batches)."""
        import sweep
        from litpipe import worklists
        self.resumed = 0
        for b in self.pool.pending():
            bpath = Path(b["batch_path"]) if b.get("batch_path") else self.batch_path
            tag = sweep.queue_tag(bpath.name) or self.tag
            dois = list(b["dois"])
            if bpath.exists():
                if self.stage_only:          # the curation pause: leave a staged file as it is
                    _say(f"{bpath.name} is staged and pending; sweep it with runner batch (without --stage-only)")
                    self.done.append({"label": "pending", "batch_path": str(bpath), "dois": len(dois),
                                      "swept": False, "reason": "pending: --stage-only sweeps nothing"})
                    return False
                file_dois = _queue_dois(bpath)
                staged = {worklists.doi_key(d) for d in dois}
                extra = [d for k, d in file_dois.items() if k not in staged]
                if extra:
                    raise _BatchStop(EXIT_CONFIG, f"{bpath.name} holds DOIs the batch did not stage: "
                                                  + ", ".join(extra[:5]) + (" ..." if len(extra) > 5 else ""))
                out = [d for d in dois if worklists.doi_key(d) not in file_dois]
                if out:
                    self.pool.mark_swept(out, self.run_id, {d: "curated_out" for d in out})
                    _say(f"{len(out)} DOI(s) curated out of {bpath.name}: marked curated_out")
                keep = [d for d in dois if worklists.doi_key(d) in file_dois]
                if not keep:                 # every row curated out: nothing to sweep
                    bpath.unlink()
                    self.done.append({"label": "pending (all curated out)", "batch_path": str(bpath),
                                      "dois": 0, "swept": False, "curated_out": len(out)})
                    continue
                if not self._sweep_batch(bpath, keep, "pending (file present)"):
                    return False
                self.resumed += 1
                continue
            rid = self._processed_run(tag, dois)
            if rid:
                routing = self.p.root / sweep.artifact_name(tag, rid, "routing")
                if not routing.is_file():          # killed after the sweep, before the route
                    prev = self.p.sweep
                    self.p.sweep = {"exit": 0, "status": OK, "run_id": rid, "results": [{"retired": True, "tag": tag}],
                                    "refused": []}
                    try:
                        self._route(self.p)
                    finally:
                        self.p.sweep = prev        # this run swept nothing for it: no LOOSE_ENDS line
                self.pool.mark_swept(dois, rid, self._residual_classes(tag, rid, dois))
                self.done.append({"label": "pending (swept before a kill)", "batch_path": str(bpath),
                                  "dois": len(dois), "swept": True, "sweep_run_id": rid, "resweep": False})
                _say(f"{bpath.name}: swept in run {rid} before a kill; routed and marked swept without a re-sweep")
                continue
            # killed between mark_staged and the write: write it now, never mark_staged again
            self._write_batch(bpath, dois)
            _say(f"{bpath.name}: rewritten from the staged DOIs (killed before the write)")
            if self.stage_only:
                self.done.append({"label": "pending (rewritten)", "batch_path": str(bpath), "dois": len(dois),
                                  "swept": False, "reason": "--stage-only: curate it, then run runner batch"})
                return False
            if not self._sweep_batch(bpath, dois, "pending (rewritten)"):
                return False
            self.resumed += 1
        return True

    def _one_batch(self, i):
        rows = self.pool.next_batch(self.size)
        _checkpoint("after_next_batch")
        if not rows:
            _say("the pool is drawn down: nothing left to stage")
            return False
        dois = [r["doi"] for r in rows]
        self.pool.mark_staged(dois, self.run_id, str(self.batch_path))
        _checkpoint("after_mark_staged")
        self._write_batch(self.batch_path, dois)
        _checkpoint("after_write")
        _say(f"batch {i}: staged {len(dois)} DOI(s) in {self.batch_path.name}")
        if self.stage_only:
            self.done.append({"label": f"batch {i}", "batch_path": str(self.batch_path), "dois": len(dois),
                              "swept": False, "reason": "--stage-only: curate it, then run runner batch"})
            _say(f"--stage-only: curate {self.batch_path}, then run runner batch again to sweep it")
            return False
        return self._sweep_batch(self.batch_path, dois, f"batch {i}")

    def _work(self):
        from litpipe import worklists
        self._open()
        self.pool = worklists.Pool(self.pool_path, registry=self.cfg, cache_dir=config.state_dir(self.cfg))
        self.order, self.order_source = self.runner_cfg["candidate_order"], (
            "projects.json" if self.runner_cfg["candidate_order"] else None)
        if not self.stage_only:
            self._preflight()
            self._network_canaries("every_run")
        if self._resolve_pending():
            # --batches counts the batches swept by this invocation, a resumed one included: after
            # a curation pause, `runner batch` sweeps the curated batch and draws no uncurated one
            for i in range(1, self.n_batches - self.resumed + 1):
                if not self._one_batch(i):
                    break
        if not self.stage_only:
            self._local_canaries()

    def execute(self):
        from litpipe import worklists
        try:
            with _sigterm_aborts():
                self._work()
        except _Terminated:
            self.aborted = "terminated (SIGTERM)"
        except _BatchStop as e:
            self.extra["usage_error"] = str(e)
            self._job(self.key, "batch", FAILED, str(e))
            self.finish_status = "config"
            print(f"[runner] {e}", file=sys.stderr)
            self.exit_override = e.code
        except _Abort as e:
            self.aborted = e.reason
        except KeyboardInterrupt:
            self.aborted = "interrupted"
        except worklists.WorklistError as e:
            self.extra["usage_error"] = str(e)
            self._job(self.key, "batch", FAILED, str(e))
            self.finish_status = "config"
            self.exit_override = EXIT_CONFIG
        except Exception as e:   # noqa: BLE001
            self.aborted = f"runner crashed: {type(e).__name__}: {ledger.redact(str(e))[:200]}"
            traceback.print_exc()
        finally:
            summ = self._finish()
        return summ

    def _compute_exit(self):
        code = getattr(self, "exit_override", None)
        if code is not None:
            return code
        return super()._compute_exit()

    def _stamps(self):
        pass

    def _consume_ab(self):
        pass

    def _local_canaries(self):
        try:
            outs = canaries.run("every_run", phase="local", context=self._local_ctx(), cfg=self.cfg)
            self.canary_outs += list(outs or [])
        except Exception as e:   # noqa: BLE001
            self._job(None, "canaries_local", ERROR, f"{type(e).__name__}: {ledger.redact(str(e))[:200]}")
        self._report()

    def plan(self):
        from litpipe import worklists
        pool = worklists.Pool(self.pool_path, registry=self.cfg, cache_dir=None)
        self.check(pool)
        print(f"# runner batch --dry-run: project {self.key}, pool {self.pool_path.name}, tag {self.tag}; "
              f"nothing is written or sent")
        pend = pool.pending()
        for b in pend:
            exists = Path(b["batch_path"]).exists() if b.get("batch_path") else False
            print(f"  pending batch {Path(b['batch_path']).name if b.get('batch_path') else '?'}: {len(b['dois'])} DOI(s), "
                  f"file {'present: would sweep it' if exists else 'missing: would resolve from artifacts or rewrite it'}")
        nxt = [] if pend else pool.next_batch(self.size)
        if pend:
            print("  no new rows are drawn while a pending batch is unresolved")
        else:
            print(f"  next batch: {len(nxt)} DOI(s) to {self.batch_path}" + (" (stage only)" if self.stage_only else ""))
        st = pool.status()
        print(f"  pool: {st['rows']} rows, {st['staged']} staged, {st['swept']} swept, {st['remaining']} remaining")
        return {"exit_code": EXIT_OK, "dry_run": True, "tag": self.tag, "batch_path": str(self.batch_path),
                "pending": len(pend), "next_batch": len(nxt)}


def batch(*, project, pool, size=100, batches=1, tag=None, skip_preprint=False, stage_only=False, dry_run=False,
          json_path=None, launcher=None, timeouts=None) -> dict:
    """`runner batch` (amendment 11); see the Pool contract in litpipe.worklists."""
    import sweep
    try:
        cfg, keys = load_registry([project])
    except ConfigProblem as e:
        msg = ledger.redact(str(e))
        print(f"[runner] config: {msg}", file=sys.stderr)
        return {"exit_code": EXIT_CONFIG, "error": msg}
    t = tag or batch_tag(pool)
    if not sweep.is_valid_tag(t):
        msg = (f"batch tag {t!r} is not a valid queue tag ([a-z][a-z0-9_-]{{0,31}}, not date-like, not reserved); "
               f"pass --tag")
        print(f"[runner] usage: {msg}", file=sys.stderr)
        return {"exit_code": EXIT_CONFIG, "error": msg}
    if int(size) < 1 or int(batches) < 1:
        print("[runner] usage: --size and --batches must be 1 or more", file=sys.stderr)
        return {"exit_code": EXIT_CONFIG, "error": "bad size or batches"}
    r = _BatchRun(cfg, project, pool=pool, size=size, batches=batches, tag=t, skip_preprint=skip_preprint,
                  stage_only=stage_only, launcher=launcher, timeouts=timeouts, json_path=json_path,
                  profile="every_run")
    from litpipe import worklists
    try:
        if dry_run:
            return r.plan()
        r.check(worklists.Pool(r.pool_path, registry=cfg, cache_dir=None))
    except (_BatchStop, worklists.WorklistError) as e:
        msg = ledger.redact(str(e))
        print(f"[runner] {msg}", file=sys.stderr)
        return {"exit_code": EXIT_CONFIG, "error": msg}
    return r.execute()


# ------------------------------------------------------------------------------ status and schedule-print
def status() -> dict:
    """Live runs, the last run per project, refused and deferred hosts, today's per-host counts."""
    p = state.db_path(create=False)
    if not p.exists():
        print(f"no state yet: {p} does not exist (it is created on first use)")
        return {"exit_code": EXIT_OK, "state": None}
    st = state.status()
    print(f"state file: {st['db']}")
    print(f"live runs: {len(st['live_runs'])}")
    for r in st["live_runs"]:
        print(f"  {r['run_id']}  {r['kind']}  pid {r['pid']}  {'writer' if r['writer'] else 'reader'}  "
              f"heartbeat {r['heartbeat_age_s']} s ago")
    last = {}
    try:
        base = p.parent / "runner"
        if base.is_dir():
            dirs = sorted((d for d in base.iterdir() if d.is_dir() and RUN_DIR_RE.match(d.name)),
                          key=lambda d: (d.name[:16], d.stat().st_mtime_ns), reverse=True)
            for d in dirs:
                s = d / "summary.json"
                if not s.is_file():
                    continue
                try:
                    summ = json.loads(s.read_text(encoding="utf-8"))
                except (OSError, ValueError):
                    continue
                for key, jobs in (summ.get("by_project") or {}).items():
                    if key == "_portfolio" or key in last:
                        continue
                    last[key] = {"run_id": summ.get("run_id"), "finished": summ.get("finished"),
                                 "exit_code": summ.get("exit_code"),
                                 "jobs": {j["job"]: j["status"] for j in jobs}}
    except OSError:
        pass
    print(f"last run per project: {len(last)}")
    for key, v in sorted(last.items()):
        print(f"  {key:<28} {v['run_id']}  exit {v['exit_code']}  "
              + ", ".join(f"{j} {s}" for j, s in v["jobs"].items()))
    refused = [h for h in st["hosts"] if h["refused"]]
    deferred = [h for h in st["hosts"] if h["deferred_until"]]
    print(f"refused hosts: {len(refused)}")
    for h in refused:
        print(f"  {h['host']}  ({h['refused']}) {h['refused_reason']}")
    print(f"deferred hosts: {len(deferred)}")
    for h in deferred:
        print(f"  {h['host']}  until {h['deferred_until']}: {h['defer_reason']}")
    print("today's requests per host (UTC day):")
    for h in st["hosts"]:
        if h["day_count"]:
            print(f"  {h['host']:<32} {h['day_count']}")
    return {"exit_code": EXIT_OK, "state": st, "last_runs": last}


TASK_NAME = "literature-pipeline nightly"
UNIT = "litpipe-runner"
PLATFORMS = ("linux", "windows")
RUN_ARGS = "-m litpipe.runner run --profile daily --scheduled"
SECRET_ENVS = (("LITPIPE_EMAIL", "required: your contact address"),
               ("S2_API_KEY", "optional: a Semantic Scholar key"),
               ("OPENALEX_API_KEY", "optional: an OpenAlex key"))


def current_platform() -> str:
    return "windows" if os.name == "nt" else "linux"


def _checkout_for(platform, checkout):
    """The checkout path printed: this runner's own repo on its own platform, `<checkout>` when the
    other platform's form is asked for (this machine's path means nothing there)."""
    if checkout is not None:
        return str(checkout)
    return str(REPO_ROOT) if platform == current_platform() else "<checkout>"


def _registry_state_dir():
    """The state_dir the registry names (for the cron log), or a placeholder when it cannot be read."""
    try:
        return str(config.state_dir(config.load(), create=False))
    except Exception:   # noqa: BLE001 - a template still prints without a registry
        return "<state_dir>"


def _linux_text(checkout, state_dir):
    import shlex
    co = checkout.rstrip("/") or "/"
    py = f"{co}/.venv/bin/python"
    exec_py = f'"{py}"' if " " in py else py
    log = f"{state_dir.rstrip('/')}/runner/cron.log"
    env_lines = "".join(f"#Environment={name}=<{what}>\n" for name, what in SECRET_ENVS)
    return (
        "# litpipe runner: ONE nightly run at 01:00 (DEC-03), as a systemd user service and timer.\n"
        "# Printed only: nothing was registered. Values are never printed; set them yourself.\n"
        f"# Save as ~/.config/systemd/user/{UNIT}.service\n"
        "[Unit]\n"
        "Description=literature pipeline runner (daily profile, escalating to weekly and monthly)\n"
        "Wants=network-online.target\n"
        "After=network-online.target\n"
        "\n"
        "[Service]\n"
        "Type=oneshot\n"
        f"WorkingDirectory={co}\n"
        "# The variables the runner reads (uncomment and fill in, or set them in the user manager):\n"
        f"{env_lines}"
        "Environment=PYTHONUNBUFFERED=1\n"
        f"ExecStart={exec_py} {RUN_ARGS}\n"
        f"# Without the venv path: ExecStart=/usr/bin/env uv run --no-sync --project {co} python {RUN_ARGS}\n"
        "#   (a bare `uv run` syncs first)\n"
        "TimeoutStartSec=20h\n"
        "\n"
        f"# Save as ~/.config/systemd/user/{UNIT}.timer\n"
        "[Unit]\n"
        "Description=Run the literature pipeline runner nightly at 01:00\n"
        "\n"
        "[Timer]\n"
        "OnCalendar=*-*-* 01:00:00\n"
        "Persistent=true\n"
        "\n"
        "[Install]\n"
        "WantedBy=timers.target\n"
        "\n"
        f"# Enable it by hand: systemctl --user daemon-reload && systemctl --user enable --now {UNIT}.timer\n"
        "# Persistent=true runs a 01:00 missed while the machine was off at the next start (the analogue of\n"
        "# start-when-available). A user timer runs with nobody logged in only with lingering:\n"
        "#   loginctl enable-linger \"$USER\"\n"
        "\n"
        "# crontab fallback (crontab -e). cron has no Persistent=: a night the machine is off is skipped.\n"
        "# Set the variables at the top of the crontab: "
        + ", ".join(f"{name}=..." for name, _ in SECRET_ENVS) + "\n"
        f"0 1 * * * cd {shlex.quote(co)} && {shlex.quote(py)} {RUN_ARGS} >> {shlex.quote(log)} 2>&1\n")


def _windows_text(checkout):
    co = checkout.rstrip("\\") or checkout
    py = f"{co}\\.venv\\Scripts\\python.exe"
    cmdline = f'set PYTHONUTF8=1&& "{py}" {RUN_ARGS}'
    inner = f"cd /d {co} && set PYTHONUTF8=1&& {py} {RUN_ARGS}"

    def q(s):                     # a PowerShell single-quoted string doubles its own quote
        return s.replace("'", "''")
    return (
        "# litpipe runner: ONE nightly run at 01:00 (DEC-03), as a Windows scheduled task.\n"
        "# PowerShell, run once by hand. A scheduled task does not read your shell profile, so PYTHONUTF8 is\n"
        "# set in the command; " + ", ".join(n for n, _ in SECRET_ENVS)
        + " come from your user environment (setx).\n"
        f"$action = New-ScheduledTaskAction -Execute 'cmd.exe' -Argument '/c {q(cmdline)}' -WorkingDirectory '{q(co)}'\n"
        "$trigger = New-ScheduledTaskTrigger -Daily -At 01:00\n"
        "$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries "
        "-DontStopIfGoingOnBatteries -ExecutionTimeLimit (New-TimeSpan -Hours 20)\n"
        f"Register-ScheduledTask -TaskName '{TASK_NAME}' -Action $action -Trigger $trigger -Settings $settings "
        "-Description 'litpipe runner: daily profile, escalating to weekly and monthly'\n"
        f"# Interpreter: {py}. Without the venv path:\n"
        f"#   uv run --no-sync --project \"{co}\" python {RUN_ARGS}   (a bare `uv run` syncs first)\n"
        "\n"
        "# schtasks fallback. Its battery settings cannot be set this way (StartWhenAvailable,\n"
        "# AllowStartIfOnBatteries, DontStopIfGoingOnBatteries): a laptop on battery at 01:00 may skip\n"
        "# or stop the run. Prefer the PowerShell form. Quote any path that contains a space.\n"
        f'schtasks /Create /TN "{TASK_NAME}" /SC DAILY /ST 01:00 /TR "cmd /c {inner}"\n')


def schedule_text(platform=None, checkout=None) -> str:
    """The one nightly task (DEC-03) as a template filled from this runner's own location and the
    registry: a systemd user service and timer plus a crontab line on Linux, a Task Scheduler
    command on Windows. Never registers anything."""
    platform = platform or current_platform()
    if platform not in PLATFORMS:
        raise ValueError(f"platform must be one of {PLATFORMS}, got {platform!r}")
    co = _checkout_for(platform, checkout)
    if platform == "windows":
        return _windows_text(co)
    return _linux_text(co, _registry_state_dir() if platform == current_platform() else "<state_dir>")


def schedule_print(platform=None):
    print(schedule_text(platform))
    print("# Printed only: nothing was registered.")
    return {"exit_code": EXIT_OK}


# ------------------------------------------------------------------------------ CLI
class _Usage(Exception):
    pass


class _Parser(argparse.ArgumentParser):
    def error(self, message):
        raise _Usage(message)


def _timeouts(values):
    out = {}
    for v in values or ():
        job, _, secs = v.partition("=")
        if job not in STAGE_MODULES or not secs:
            raise _Usage(f"--timeout takes JOB=SECONDS with JOB one of {', '.join(STAGE_MODULES)}, got {v!r}")
        try:
            out[job] = float(secs)
        except ValueError:
            raise _Usage(f"--timeout {v!r}: SECONDS must be a number") from None
    return out


def _parser():
    ap = _Parser(prog="python -m litpipe.runner",
                 description="The literature pipeline's scheduled runner. Exit 0 clean, 1 usage or config, "
                             "2 completed with failures, 3 aborted.")
    sub = ap.add_subparsers(dest="command", required=True, parser_class=_Parser)
    r = sub.add_parser("run", help="one run of a profile")
    r.add_argument("--profile", required=True, choices=PROFILES)
    r.add_argument("--project", action="append", default=None, help="only this project (repeatable)")
    r.add_argument("--dry-run", action="store_true", help="list the jobs and skip reasons; write and send nothing")
    r.add_argument("--scheduled", action="store_true",
                   help="the nightly task: escalate to weekly / monthly when due (kv stamps)")
    g = r.add_mutually_exclusive_group()
    g.add_argument("--db-writes", dest="db_writes", action="store_true", default=None,
                   help="run the portfolio.duckdb writers this run")
    g.add_argument("--no-db-writes", dest="db_writes", action="store_false",
                   help="never run the portfolio.duckdb writers this run")
    r.add_argument("--json", metavar="PATH", help="also write the summary here")
    r.add_argument("--timeout", action="append", metavar="JOB=SECONDS", help="override one stage's timeout")
    b = sub.add_parser("batch", help="draw a pool down in batches (resumable)")
    b.add_argument("--project", required=True)
    b.add_argument("--pool", required=True, metavar="CSV")
    b.add_argument("--size", type=int, default=100)
    b.add_argument("--batches", type=int, default=1)
    b.add_argument("--tag", default=None, help="the batch queue tag (default: b-<pool name>)")
    b.add_argument("--skip-preprint", action="store_true", help="passed to sweep and migrate")
    b.add_argument("--stage-only", action="store_true",
                   help="stage the next batch and stop, so a person can curate it; the next runner batch sweeps it")
    b.add_argument("--dry-run", action="store_true")
    b.add_argument("--json", metavar="PATH")
    b.add_argument("--timeout", action="append", metavar="JOB=SECONDS")
    sub.add_parser("status", help="live runs, last run per project, refused and deferred hosts, today's counts")
    sp = sub.add_parser("schedule-print", help="print the nightly task as a template (never registers it)")
    sp.add_argument("--platform", choices=PLATFORMS, default=None,
                    help="linux (a systemd user service and timer, plus a crontab line) or windows (Task "
                         "Scheduler); default: this machine's")
    s = sub.add_parser("_stage", help=argparse.SUPPRESS)
    s.add_argument("--module", required=True)
    s.add_argument("--kwargs", required=True)
    s.add_argument("--out", required=True)
    s.add_argument("--config", required=True)
    return ap


def _stage_main(args):
    with open(args.kwargs, encoding="utf-8") as f:
        kwargs = json.load(f)
    return shim(args.module, kwargs, args.out, args.config)


def main(argv=None) -> int:
    lit_util.utf8_stdout()
    argv = sys.argv[1:] if argv is None else list(argv)
    try:
        args = _parser().parse_args(argv)
        timeouts = _timeouts(getattr(args, "timeout", None))
    except _Usage as e:
        print(f"[runner] usage error: {e}", file=sys.stderr)
        return EXIT_CONFIG
    if args.command == "_stage":
        return _stage_main(args)
    if args.command == "status":
        return status()["exit_code"]
    if args.command == "schedule-print":
        return schedule_print(args.platform)["exit_code"]
    if args.command == "run":
        res = run(profile=args.profile, projects=args.project, dry_run=args.dry_run, scheduled=args.scheduled,
                  db_writes=args.db_writes, json_path=args.json, timeouts=timeouts)
        return res["exit_code"]
    res = batch(project=args.project, pool=args.pool, size=args.size, batches=args.batches, tag=args.tag,
                skip_preprint=args.skip_preprint, stage_only=args.stage_only, dry_run=args.dry_run,
                json_path=args.json, timeouts=timeouts)
    return res["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
