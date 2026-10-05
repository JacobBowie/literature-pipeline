"""Snowball-loop orchestrator.

One iteration, per project:
  1. forward_citations.py   (S2 forward citations on every PDF; exit 2 degraded, 3 aborted)
  2. reverse_citations.py   (what every PDF cites: S2, OpenAlex and Crossref over the network, then
                             the local parse; exit 2 degraded, 3 aborted). While S2_API_KEY is
                             unset it runs with --sources openalex,crossref,regex: the shared
                             unkeyed S2 pool answers 429 at once, which would make every
                             iteration DEGRADED.
  3. index_portfolio.py     (refresh the DuckDB index with the new candidates)
After the loop, once per invocation (both tools are portfolio-wide; D8):
  4. enrich_recommendations.py --recent-feed   only with --with-recs (and it sends nothing
     without S2_API_KEY). S2's recommendations are a last-60-days feed, so they are no longer
     part of the loop or of the count.
  5. enrich_abstracts.py         incremental (only missing); --skip-abstracts skips it.

Iterations: one by default. With --until-convergence, iteration N+1 runs only when the
library changed during iteration N (its PDF set or the DOIs in its .ris files differ) and
growth was at least 1 %, up to --max-iter. Nothing in the snowball adds PDFs, so an idle
library gets one iteration: a second pass over unchanged seeds only measures throttling.

DEGRADED (the iteration's growth is not a measurement): a step exited 2 or 3, the candidate
count fell, or forward_citations recorded a transport failure (a final 429, 5xx, timeout,
refusal or deferral). FAILED: a step exited with any other non-zero code. Either stops that
project's loop. Every iteration appends a row to the convergence log, whose `reason` column
says ok (and why the loop stopped or went on), DEGRADED: ... or FAILED: .... Logs written
before the column existed still parse (read_log) and are upgraded in place on the next write.

The count is what top_candidates draws from: distinct DOIs in `candidates` for the project
that no library holds (REG-I30). A first count of 0 has no growth percentage ("inf" in the
log); 0 -> 0 is converged.

Each step's output streams line by line with timestamps; children get PYTHONUNBUFFERED=1.
forward_citations' last line, "[step-summary] {json}", carries its failure counts.

Exit codes: 0 every iteration ok; 2 some project DEGRADED; 1 a step FAILED or bad arguments.

The wrapper does NOT auto-fetch the discovered candidates. That requires
a project-agent's relevance call and a manual lit_pull_queue.csv write.

Usage:
  python snowball.py --project research_a                 # one iteration
  python snowball.py --project research_a --skip-forward  # skip slow S2 forward step
  python snowball.py --project research_a --until-convergence --max-iter 3
  python snowball.py --project research_a --with-recs     # plus the recs feed, once
  python snowball.py --all                                # every active project
"""
import argparse
import csv
import datetime
import io
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import lit_util
lit_util.utf8_stdout()

HERE = Path(__file__).parent
CONFIG_PATH = HERE / "projects.json"
DB_PATH = lit_util.PROJECTS_ROOT / "_references" / "portfolio.duckdb"
LOG_PATH = lit_util.PROJECTS_ROOT / "_references" / "convergence_log.csv"

LOG_FIELDS = ["date", "project", "iter", "n_before", "n_after", "growth_pct", "reason"]
CONVERGED_PCT = 1.0
SUMMARY_MARKER = "[step-summary] "      # forward_citations.SUMMARY_MARKER
DEGRADED_EXITS = frozenset({2, 3})
EXIT_OK, EXIT_FAILED, EXIT_DEGRADED = 0, 1, 2


@dataclass
class StepResult:
    label: str
    cmd: list
    rc: int
    summary: dict | None = None
    lines: int = 0
    elapsed_s: float = 0.0


@dataclass
class Options:
    until_convergence: bool = False
    max_iter: int = 3
    skip_forward: bool = False
    skip_reverse: bool = False
    with_recs: bool = False
    skip_abstracts: bool = False
    projects: list = field(default_factory=list)


# ------------------------------------------------------------------------------ convergence log
def _growth_text(growth_pct) -> str:
    return "inf" if growth_pct is None else f"{growth_pct:.2f}"


def _ensure_log_schema():
    """Create the log with the current header, or upgrade a log written before `reason` existed
    (its rows get a blank reason; nothing else changes)."""
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not LOG_PATH.exists() or LOG_PATH.stat().st_size == 0:
        with open(LOG_PATH, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(LOG_FIELDS)
        return
    with open(LOG_PATH, encoding="utf-8", newline="") as f:
        header = next(csv.reader(f), [])
    if "reason" in header:
        return
    with open(LOG_PATH, encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))[1:]
    new_header = header + [c for c in LOG_FIELDS if c not in header]
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(new_header)
    for r in rows:
        if r:
            w.writerow(r + [""] * (len(new_header) - len(r)))
    lit_util.atomic_write_text(str(LOG_PATH), buf.getvalue(), newline="")


def log_iteration(project: str, iter_num: int, n_before: int, n_after: int, growth_pct, reason: str = ""):
    """Append one row to the convergence log (header created or upgraded first). growth_pct None
    (or inf) is a first count with no percentage."""
    _ensure_log_schema()
    with open(LOG_PATH, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([datetime.date.today().isoformat(), project, iter_num,
                                n_before, n_after, _growth_text(growth_pct), reason])


def read_log(path=None) -> list:
    """Every row of a convergence log, old (6-column) or current, as dicts over LOG_FIELDS plus any
    extra columns; a missing reason reads as ""."""
    p = Path(path) if path is not None else LOG_PATH
    if not p.exists():
        return []
    with open(p, encoding="utf-8", newline="") as f:
        return [{**{k: "" for k in LOG_FIELDS}, **{k: (v or "") for k, v in r.items() if k is not None}}
                for r in csv.DictReader(f)]


# ------------------------------------------------------------------------------ the count
def py(): return sys.executable


def candidate_count(project: str) -> int:
    """Unique candidate DOIs the project's top_candidates rows draw from: `candidates` sourced
    from the project (forward and reverse) that no library holds.

    REG-I30: this is the top_candidates quantity and nothing else. The RC9 version also unioned
    the `recommendations` table, which top_candidates never reads, so the recs feed (a
    last-60-days list) inflated convergence; recommendations no longer count."""
    if not DB_PATH.exists(): return 0
    con = lit_util.connect_db(str(DB_PATH), read_only=True)  # c9: shared RC10 open (adds Drive-lock retry)
    try:
        n = con.execute(
            """
            SELECT COUNT(DISTINCT c.doi)
            FROM candidates c
            WHERE c.source_project = ?
              AND NOT EXISTS (SELECT 1 FROM paper_locations l WHERE l.doi = c.doi)
            """,
            [project]).fetchone()[0]
    finally:
        con.close()
    return n


def growth_pct(n_before: int, n_after: int):
    """Percent growth; None when n_before is 0 and something was found (no percentage)."""
    if n_before:
        return 100.0 * (n_after - n_before) / n_before
    return 0.0 if n_after == 0 else None


# ------------------------------------------------------------------------------ library change
def library_dir(project: str, registry=None):
    reg = registry if registry is not None else lit_util.load_projects_config(CONFIG_PATH, missing_ok=True)
    p = (reg.get("projects") or {}).get(project)
    if not p or not p.get("lib_dir"):
        return None
    return lit_util.PROJECTS_ROOT / (p.get("parent") or project) / p["lib_dir"]


def library_fingerprint(lib):
    """(PDF names, DOIs in .ris files) of a library, or None when it cannot be read."""
    if lib is None or not Path(lib).is_dir():
        return None
    from forward_citations import doi_from_ris
    lib = Path(lib)
    pdfs = tuple(sorted(p.name for p in lib.glob("*.pdf")))
    dois = tuple(sorted({d for d in (doi_from_ris(r) for r in lib.glob("*.ris")) if d}))
    return pdfs, dois


# ------------------------------------------------------------------------------ steps
def stamp() -> str:
    return datetime.datetime.now().strftime("%H:%M:%S")


def _summary(line: str):
    try:
        obj = json.loads(line[len(SUMMARY_MARKER):])
    except ValueError:
        return None
    return obj if isinstance(obj, dict) else None


def run_step(cmd, label) -> StepResult:
    """Run one step as a child process and stream its output line by line, each line prefixed
    with the time it arrived. stderr is merged into stdout. The child gets PYTHONUNBUFFERED=1 so
    its prints are not held back until it exits."""
    cmd = [str(c) for c in cmd]
    print(f"\n>>> [{stamp()}] {label}")
    print(f"    {' '.join(cmd)}", flush=True)
    t0 = time.monotonic()
    env = {**os.environ, "PYTHONUNBUFFERED": "1"}
    summary, n = None, 0
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                                encoding="utf-8", errors="replace", bufsize=1, env=env)
    except OSError as e:
        print(f"    [{stamp()}] could not start: {e}", flush=True)
        return StepResult(label, cmd, 127)
    with proc:
        for line in proc.stdout:
            line = line.rstrip("\r\n")
            n += 1
            if line.startswith(SUMMARY_MARKER):
                summary = _summary(line) or summary
            print(f"    [{stamp()}] {line}", flush=True)
        rc = proc.wait()
    elapsed = time.monotonic() - t0
    print(f"    ... done at {stamp()} (exit {rc}, {elapsed / 60:.1f} min)", flush=True)
    return StepResult(label, cmd, rc, summary, n, elapsed)


REVERSE_SOURCES_UNKEYED = "openalex,crossref,regex"


def reverse_source_args() -> list:
    """The reverse step's --sources: every leg with an S2 key; without one, S2 is left out (its
    unkeyed 429 would mark the iteration DEGRADED while the other legs could answer)."""
    from litpipe import s2
    if s2.key_present():
        return []
    print(f"  [note] S2_API_KEY unset: reverse_citations runs --sources {REVERSE_SOURCES_UNKEYED}",
          flush=True)
    return ["--sources", REVERSE_SOURCES_UNKEYED]


def one_iteration(project: str, skip_forward: bool, skip_reverse: bool, step_runner=None) -> list:
    """The per-project discovery steps and the index refresh. Every step runs even when an
    earlier one failed (the index also refreshes reverse candidates and library locations)."""
    runner = step_runner or run_step
    steps = []
    if not skip_forward:
        steps.append(runner([py(), str(HERE / "forward_citations.py"), "--project", project],
                            "forward_citations"))
    if not skip_reverse:
        steps.append(runner([py(), str(HERE / "reverse_citations.py"), "--project", project,
                             *reverse_source_args()], "reverse_citations"))
    steps.append(runner([py(), str(HERE / "index_portfolio.py"), "--project", project,
                         "--db", str(DB_PATH)], f"index_portfolio({project})"))
    return steps


# ------------------------------------------------------------------------------ classification
def degraded_exit(rc, summary) -> bool:
    """The walker's verdict: exit 2 or 3 WITH its [step-summary] line. Any other exit 2 (argparse
    usage errors, ris_emit.load_projects_config without a registry) is a failed step, not a
    degraded walk. The steps' own config errors exit 1 (W2b, W3a)."""
    return rc in DEGRADED_EXITS and summary is not None


def degraded_reasons(n_before: int, n_after: int, steps=()) -> list:
    why = []
    for s in steps:
        summ = s.summary or {}
        if degraded_exit(s.rc, s.summary):
            detail = (f"aborted: {summ['aborted']}" if summ.get("aborted")
                      else "; ".join(summ.get("reasons") or []))
            why.append(f"{s.label} exit {s.rc}" + (f" ({detail})" if detail else ""))
        n = summ.get("transport_failures") or 0
        if isinstance(n, int) and n > 0:
            why.append(f"{s.label} recorded {n} transport failure(s)")
    if n_after < n_before:
        why.append(f"negative growth {n_before}->{n_after} ({growth_pct(n_before, n_after):+.2f}%)")
    return why


def classify(n_before: int, n_after: int, steps=()) -> tuple:
    """("ok" | "DEGRADED" | "FAILED", why) for one iteration."""
    from litpipe.ledger import redact
    deg = degraded_reasons(n_before, n_after, steps)
    fail = [f"{s.label} exit {s.rc}" for s in steps if s.rc != 0 and not degraded_exit(s.rc, s.summary)]
    if deg:
        return "DEGRADED", redact("; ".join(deg + fail))
    if fail:
        return "FAILED", redact("; ".join(fail))
    return "ok", ""


def next_step(it: int, opts: Options, n_before: int, n_after: int, lib_changed: bool) -> tuple:
    """(go on?, why) after an ok iteration."""
    if not opts.until_convergence:
        return False, "stop: single iteration"
    if it >= opts.max_iter:
        return False, f"stop: max_iter {opts.max_iter}"
    g = growth_pct(n_before, n_after)
    if g is not None and g < CONVERGED_PCT:
        return False, f"stop: converged (growth {g:.2f}% < {CONVERGED_PCT:g}%)"
    if not lib_changed:
        return False, "stop: no_new_seeds (library unchanged)"
    return True, "continue: library changed"


def snowball_project(project: str, opts: Options, step_runner=None) -> dict:
    print(f"\n{'='*72}\n  SNOWBALL: {project}\n{'='*72}")
    lib = library_dir(project)
    n_before = candidate_count(project)
    print(f"  starting candidate count: {n_before}")
    iters, status = [], "ok"
    for it in range(1, max(1, opts.max_iter) + 1):
        print(f"\n--- iteration {it}" + (f"/{opts.max_iter}" if opts.until_convergence else "") + " ---")
        fp0 = library_fingerprint(lib)
        steps = one_iteration(project, opts.skip_forward, opts.skip_reverse, step_runner)
        n_after = candidate_count(project)
        g = growth_pct(n_before, n_after)
        status, why = classify(n_before, n_after, steps)
        go = False
        if status == "ok":
            changed = fp0 is not None and library_fingerprint(lib) != fp0
            go, stop = next_step(it, opts, n_before, n_after, changed)
            reason = f"ok; {stop}"
        else:
            reason = f"{status}: {why}"
        g_txt = "n/a" if g is None else f"{g:+.2f}%"
        print(f"\n  iter {it}: {n_before} -> {n_after} candidates ({n_after - n_before:+d}, {g_txt})")
        print(f"  [{reason}]", flush=True)
        log_iteration(project, it, n_before, n_after, g, reason)
        iters.append({"iter": it, "n_before": n_before, "n_after": n_after, "growth_pct": g,
                      "status": status, "reason": reason,
                      "steps": [{"label": s.label, "rc": s.rc, "summary": s.summary} for s in steps]})
        if status != "ok":
            print(f"  [WARN] {status}: stopping WITHOUT declaring convergence")
            break
        if not go:
            break
        n_before = n_after
    return {"project": project, "status": status, "iterations": iters}


def post_loop(opts: Options, step_runner=None) -> list:
    """The portfolio-wide tools, once per invocation, after every project's loop (D8)."""
    runner = step_runner or run_step
    steps = []
    if opts.with_recs:
        steps.append(runner([py(), str(HERE / "enrich_recommendations.py"), "--db", str(DB_PATH),
                             "--recent-feed"],
                            "enrich_recommendations (portfolio-wide, once, --with-recs)"))
    if not opts.skip_abstracts:
        steps.append(runner([py(), str(HERE / "enrich_abstracts.py"), "--db", str(DB_PATH)],
                            "enrich_abstracts (portfolio-wide, once, only missing)"))
    return steps


def run(*, project=None, all_projects=False, until_convergence=False, max_iter=3, skip_forward=False,
        skip_reverse=False, skip_recs=False, with_recs=False, skip_abstracts=False,
        step_runner=None) -> dict:
    """The snowball for one project or every active one. Returns {"exit_code", "projects",
    "post_steps"}; `step_runner(cmd, label) -> StepResult` replaces the child processes in tests."""
    if all_projects:
        cfg = lit_util.load_projects_config(CONFIG_PATH, missing_ok=True).get("projects", {})
        if not cfg:
            print(f"[ERR] no projects registered in {CONFIG_PATH}", file=sys.stderr)
            return {"exit_code": EXIT_FAILED, "projects": [], "post_steps": [], "error": "no registry"}
        projects = [n for n, p in cfg.items() if p.get("active", True)]
    elif project:
        projects = [project]
    else:
        print("[ERR] Pass --project NAME or --all", file=sys.stderr)
        return {"exit_code": EXIT_FAILED, "projects": [], "post_steps": [], "error": "no project"}
    opts = Options(until_convergence=until_convergence, max_iter=max_iter, skip_forward=skip_forward,
                   skip_reverse=skip_reverse, with_recs=with_recs and not skip_recs,
                   skip_abstracts=skip_abstracts, projects=projects)
    results = [snowball_project(p, opts, step_runner) for p in projects]
    post = post_loop(opts, step_runner)
    def crashed(rc, summary):
        return rc != 0 and not degraded_exit(rc, summary)
    failed = (any(crashed(s["rc"], s["summary"]) for r in results for it in r["iterations"] for s in it["steps"])
              or any(crashed(s.rc, s.summary) for s in post))
    degraded = (any(r["status"] == "DEGRADED" for r in results)
                or any(degraded_exit(s.rc, s.summary) for s in post))
    code = EXIT_FAILED if failed else (EXIT_DEGRADED if degraded else EXIT_OK)
    print(f"\n# snowball: {len(results)} project(s); "
          + ", ".join(f"{r['project']} {r['status']}" for r in results)
          + (f"; post-loop: " + ", ".join(f"{s.label.split()[0]} exit {s.rc}" for s in post) if post else "")
          + f"; exit {code}")
    return {"exit_code": code, "projects": results,
            "post_steps": [{"label": s.label, "rc": s.rc} for s in post]}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    ap.add_argument("--project", default=None,
                    help="Project name from projects.json")
    ap.add_argument("--all", action="store_true",
                    help="Run every active project")
    ap.add_argument("--until-convergence", action="store_true",
                    help="Allow more iterations: the next one runs only when the library changed "
                         "during the last and growth was at least 1%%")
    ap.add_argument("--max-iter", type=int, default=3,
                    help="Hard cap on iterations with --until-convergence (default 3)")
    ap.add_argument("--skip-forward",   action="store_true")
    ap.add_argument("--skip-reverse",   action="store_true")
    ap.add_argument("--with-recs",      action="store_true",
                    help="Also run the portfolio-wide recommendations feed (enrich_recommendations "
                         "--recent-feed; sends nothing without S2_API_KEY), once, after the loop")
    ap.add_argument("--skip-recs",      action="store_true",
                    help="Accepted for compatibility: recommendations are off unless --with-recs")
    ap.add_argument("--skip-abstracts", action="store_true",
                    help="Skip the once-per-run incremental abstracts pass after the loop")
    args = ap.parse_args(argv)

    from ris_emit import warn_if_default_email
    warn_if_default_email()

    res = run(project=args.project, all_projects=args.all, until_convergence=args.until_convergence,
              max_iter=args.max_iter, skip_forward=args.skip_forward, skip_reverse=args.skip_reverse,
              skip_recs=args.skip_recs, with_recs=args.with_recs, skip_abstracts=args.skip_abstracts)
    return res["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
