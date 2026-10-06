"""Daily literature pipeline orchestrator.

Walks every active project in projects.json and runs the full
seed → stage → sweep → migrate-closed-to-md chain. Idempotent:
- Skips projects whose lit_pull_queue.csv already exists at root (pending
  human triage — don't blow it away)
- Skips projects with 0 actionable candidates (no seeds, or snowball
  graph empty after filters)
- sweep.py natively skips DOIs whose PDFs already exist in the lib_dir

Usage:
  python run_daily.py                # seed + sweep + migrate (~30-60 min wall)
  python run_daily.py --dry-run      # report what each project would do, no I/O
  python run_daily.py --project LIV  # single project
  python run_daily.py --with-snowball  # also runs forward+reverse cite walk first
                                       # (slow; recommend weekly not daily)

Exit code is 0 if everything ran cleanly, non-zero if any project errored
(but other projects still get attempted — failures isolated).
"""
import argparse
import json
import subprocess
import sys
import traceback
from datetime import datetime
from pathlib import Path

import lit_util  # RC4: atomic_write_text for crash-safe queue staging
from litpipe import config

# Force UTF-8 I/O. On Windows a piped stdout defaults to cp1252, which can't
# encode the step-label arrow, so every project died at its first print before
# doing any work. Setting PYTHONUTF8 in os.environ propagates to the subprocess
# children (they inherit it at startup); reconfigure fixes this process's own
# already-open streams.
lit_util.utf8_stdout()

HERE = Path(__file__).parent
CONFIG_PATH = HERE / "projects.json"
PY = sys.executable


def load_projects():
    with open(CONFIG_PATH, encoding="utf-8") as f:
        cfg = json.load(f).get("projects", {})
    return {k: v for k, v in cfg.items() if v.get("active", True)}


def project_dir(project: str, cfg: dict) -> Path:
    """On-disk project directory for a registry key (tail-aware for subprojects).
    Thin wrapper over lit_util.project_root; `cfg` is the full {key: dict} map."""
    return lit_util.project_root(project, cfg.get(project, {}) if cfg else {})


def queue_data_rows(queue: Path) -> int:
    """Count non-comment, non-blank lines in a queue CSV (header + data rows)."""
    with open(queue, encoding="utf-8") as f:
        return sum(1 for line in f if line.strip() and not line.startswith("#"))


def project_status(proj_root: Path):
    """Inspect the project state. Returns one of:
    PENDING_QUEUE  - lit_pull_queue.csv exists, sweep but skip seed
    NO_LIB         - lib_dir empty/missing, skip entirely
    READY          - normal seed+sweep+migrate
    """
    queue = proj_root / "lit_pull_queue.csv"
    if queue.exists():
        if queue_data_rows(queue) > 1:  # header + at least one data row
            return "PENDING_QUEUE"
    return "READY"


def _waiting(proj_root: Path) -> bool:
    """A queue sweep would take (lit_pull_queue.csv or a tagged queue), or a retry_later file."""
    import sweep
    return bool(sweep.discover_queues(proj_root)) or (proj_root / sweep.RETRY_LATER_FILE).exists()


class StepError(Exception):
    """A pipeline subprocess exited non-zero; abort this project."""


def run(label: str, cmd: list, capture: bool = False):
    print(f"  → {label}")
    result = subprocess.run(cmd, capture_output=capture, text=True,
                            encoding="utf-8", errors="replace")
    if result.returncode != 0:
        print(f"    [ERR] exit {result.returncode}")
        if capture and result.stderr:
            print(f"    {result.stderr[-500:]}")
        # RC6: a failed step must abort the project (not read as empty/success)
        # so the overall process exit code reflects the failure.
        raise StepError(f"{label} exited {result.returncode}")
    return result


def pipeline_one(project: str, cfg: dict, with_snowball: bool, dry_run: bool, run_date: str):
    print(f"\n=== {project} ===")
    proj_root = project_dir(project, cfg)   # RC11: parent-aware resolution
    status = project_status(proj_root)
    print(f"  status: {status}")
    try:   # projects.json `auto_stage` (default false): may this run seed and stage a draft itself
        auto = status != "READY" or config.auto_stage(project, {"projects": cfg})
    except config.ConfigError as e:
        print(f"  [ERR] {e}")
        return False

    if dry_run:
        if status == "PENDING_QUEUE":
            print(f"  WOULD: sweep + migrate (existing queue)")
        elif not auto:
            print(f"  WOULD: sweep + migrate what is staged (auto_stage off: no seeding)")
        else:
            print(f"  WOULD: seed + stage + sweep + migrate")
            if with_snowball:
                print(f"  WOULD: also run snowball first")
        return True

    try:
        if status == "READY":
            if with_snowball:
                # Decouple enrichment from the per-project walk: enrich_recommendations and
                # enrich_abstracts are BOTH portfolio-wide (ignore --project), so running them
                # inside every project's snowball re-pays a full portfolio pass per project.
                # Skip them here; run each ONCE after all projects (see main()). Snowball keeps
                # forward+reverse discovery + the cheap per-project index refresh.
                run("snowball (forward + reverse walk)",
                    [PY, str(HERE / "snowball.py"), "--project", project,
                     "--until-convergence", "--max-iter", "2",
                     "--skip-recs", "--skip-abstracts"])
            if not auto:
                if not _waiting(proj_root):
                    print(f"  [--] auto_stage is off and nothing is staged; skipping")
                    return True
                print(f"  [--] auto_stage is off: not seeding; sweeping what is staged")

        if status == "READY" and auto:
            seed = run("seed_queue_from_top_candidates",
                       [PY, str(HERE / "seed_queue_from_top_candidates.py"),
                        "--project", project], capture=True)
            print(seed.stdout.strip().split("\n")[0] if seed.stdout else "")

            draft = proj_root / "lit_pull_queue.draft.csv"
            if not draft.exists():
                print(f"  [--] no draft produced; skipping sweep")
                return True

            with open(draft, encoding="utf-8") as f:
                data_rows = sum(1 for line in f
                                if line.strip() and not line.startswith("#")
                                and not line.startswith("doi,"))
            if data_rows == 0:
                print(f"  [--] 0 candidates after filters; removing empty draft")
                draft.unlink()
                return True

            # RC8: never silently overwrite an existing non-empty curated queue.
            # project_status() only flags PENDING_QUEUE at >1 data row, so a
            # 1-row / comment-only human queue could slip past and get clobbered
            # by the auto-staged draft. Back it up before staging.
            queue = proj_root / "lit_pull_queue.csv"
            # A4: back up ANY non-empty queue before staging over it. The old guard keyed on
            # queue_data_rows() >= 1, which EXCLUDES '#' comment lines -- so a comment-only
            # human queue (0 data rows but real annotations) slipped past and was clobbered
            # with no backup. Byte-based guard + timestamped name (a re-fire can't overwrite
            # a prior good .bak).
            if queue.exists() and queue.stat().st_size > 0:
                backup = proj_root / "lit_pull_queue.bak.csv"
                if backup.exists():   # keep the prior good backup; timestamp only on collision
                    backup = proj_root / f"lit_pull_queue.bak.{datetime.now():%Y%m%d_%H%M%S}.csv"
                queue.replace(backup)
                print(f"  [!!] existing queue backed up to {backup.name} "
                      f"before auto-staging draft")

            # Stage: strip comment lines, leave header + data (RC4: atomic)
            with open(draft, encoding="utf-8") as fin:
                staged = "".join(line for line in fin
                                 if not line.startswith("#"))
            lit_util.atomic_write_text(str(queue), staged)
            draft.unlink()
            print(f"  staged {data_rows} DOIs to lit_pull_queue.csv")

        run("sweep", [PY, str(HERE / "sweep.py"), "--project", project,
                      "--date", run_date])   # D4b: same run_date used for migrate below, so
                                              # sweep's report artifacts and migrate agree
        run("migrate_closed_to_md",
            [PY, str(HERE / "migrate_closed_to_md.py"), "--project", project,
             "--date", run_date])   # D4b: pin migrate to the run's date so a no-report
                                     # day can't fall back to a stale prior-day report
        return True
    except StepError as e:
        # RC6: subprocess failure aborts THIS project (counted as a failure so
        # the overall exit code is non-zero); other projects still attempted.
        print(f"  [ABORT] {e}")
        return False
    except Exception as e:
        # B4: log the traceback so an unexpected coding bug is distinguishable from an
        # ordinary StepError fetch failure in the unattended daily log. (KeyboardInterrupt/
        # SystemExit are BaseException and still propagate -- do NOT narrow this except.)
        print(f"  [FATAL] {e}")
        print(traceback.format_exc())
        return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default=None, help="Single project (else all active)")
    ap.add_argument("--with-snowball", action="store_true",
                    help="Run forward+reverse+recommendations walk before seeding")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    started = datetime.now()
    print(f"# Daily literature pipeline — started {started:%Y-%m-%d %H:%M}")
    if args.with_snowball:
        print("# Mode: with snowball (slow)")
    if args.dry_run:
        print("# Mode: DRY RUN")

    projects = load_projects()
    if args.project:
        if args.project not in projects:
            print(f"[ERR] {args.project} not in projects.json (or inactive)")
            return 2
        targets = {args.project: projects[args.project]}
    else:
        targets = projects

    run_date = f"{started:%Y-%m-%d}"
    ok, fail = [], []
    for proj in targets:
        if pipeline_one(proj, projects, args.with_snowball, args.dry_run, run_date):
            ok.append(proj)
        else:
            fail.append(proj)

    # Snowball decoupling (2026-07-20): enrich_recommendations + enrich_abstracts are
    # portfolio-wide and were re-run inside every project's snowball. Run each exactly ONCE
    # here, after all projects, on a --with-snowball run. (enrich_abstracts is still a full
    # portfolio pass until the attempt-state column lands -- Batch 2 -- but now once, not Nx.)
    if args.with_snowball and not args.dry_run:
        for tool in ("enrich_recommendations.py", "enrich_abstracts.py"):
            try:
                run(f"{tool} (portfolio-wide, once)", [PY, str(HERE / tool)])
            except StepError as e:
                print(f"  [ABORT] {e}")
                fail.append(tool)

    elapsed = (datetime.now() - started).total_seconds() / 60
    print(f"\n# Done — {len(ok)} ok, {len(fail)} failed, {elapsed:.1f} min")
    if fail:
        print(f"# Failed: {', '.join(fail)}")
    return 0 if not fail else 1


if __name__ == "__main__":
    raise SystemExit(main())
