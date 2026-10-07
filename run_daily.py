"""Daily literature pipeline: a wrapper over `python -m litpipe.runner run --profile daily` (W4-A).

The runner does the work, in-process (never as a subprocess): preflight, the canaries, then per
project the auto-stage step (only for projects with `auto_stage: true` in projects.json; a staged
queue with rows is never seeded over, and any other non-empty queue is backed up before a draft is
staged), the sweep of whatever is staged (a queue, a tagged queue, due retry_later rows), routing,
and the forward walk when walk_cadence_days says it is due; then the index (DB writes) and the
health report. See litpipe/runner.py for the order, the summary and the exit codes.

Usage (the flags are unchanged):
  python run_daily.py                  # the daily profile over every active project
  python run_daily.py --dry-run        # list every job and why it would be skipped; write nothing
  python run_daily.py --project KEY    # one project
  python run_daily.py --with-snowball  # deprecated: also force the forward walk and the backward
                                       # top-up for the selected projects, then the index with DB
                                       # writes on for this run (snowball.py stays the manual tool)

Exit codes (mapped from the runner's run):
  1  the runner exited 1 (usage or config, e.g. an unknown or inactive --project) or 3 (aborted),
     or any job FAILED, ERROR or timed out;
  2  otherwise any job DEGRADED or DEFERRED (a walk deferred while Semantic Scholar is refused), or
     HEALTH ALARM;
  0  otherwise.
An unknown --project exited 2 before W4-A; it now exits 1 (the runner's config exit).
"""
import argparse
import sys

import lit_util

lit_util.utf8_stdout()

DEPRECATION = ("[deprecated] --with-snowball: run_daily.py is a wrapper over `python -m litpipe.runner run "
               "--profile daily`, which walks each project on its walk_cadence_days; this flag forces the "
               "forward walk and the backward top-up now, then the index with DB writes on. snowball.py "
               "stays the manual tool for the convergence loop.")


def queue_data_rows(queue) -> int:
    """Count non-comment, non-blank lines in a queue CSV (header + data rows)."""
    with open(queue, encoding="utf-8") as f:
        return sum(1 for line in f if line.strip() and not line.startswith("#"))


def exit_code(summary: dict) -> int:
    """run_daily's exit code for a runner summary (see the module docstring)."""
    code = summary.get("exit_code")
    if code in (1, 3) or code is None:
        return 1
    jobs = [j for j in summary.get("jobs") or [] if j.get("counts_for_exit", True)]
    if any(j.get("status") in ("FAILED", "ERROR") for j in jobs):
        return 1
    health = (summary.get("health") or {}).get("status")
    if any(j.get("status") in ("DEGRADED", "DEFERRED", "ABORTED") for j in jobs) or health == "ALARM":
        return 2
    return 0 if code == 0 else 2


def run(*, project=None, with_snowball=False, dry_run=False, launcher=None) -> dict:
    """The daily profile through the runner. Returns the runner's summary with `run_daily_exit`."""
    from litpipe import runner
    if with_snowball:
        print(DEPRECATION, flush=True)
    res = runner.run(profile="daily", projects=[project] if project else None, dry_run=dry_run,
                     force_walks=bool(with_snowball), db_writes=True if with_snowball else None,
                     launcher=launcher)
    res = dict(res)
    res["run_daily_exit"] = 0 if dry_run and res.get("exit_code") == 0 else exit_code(res)
    return res


def main(argv=None):
    ap = argparse.ArgumentParser(description="Daily literature pipeline (a wrapper over "
                                             "`python -m litpipe.runner run --profile daily`).")
    ap.add_argument("--project", default=None, help="Single project (else all active)")
    ap.add_argument("--with-snowball", action="store_true",
                    help="Deprecated: force the forward walk and the backward top-up, then the index with "
                         "DB writes on, for the selected projects")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(sys.argv[1:] if argv is None else argv)
    res = run(project=args.project, with_snowball=args.with_snowball, dry_run=args.dry_run)
    code = res["run_daily_exit"]
    print(f"# run_daily: runner exit {res.get('exit_code')}; run_daily exit {code}")
    return code


if __name__ == "__main__":
    raise SystemExit(main())
