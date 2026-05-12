"""Snowball-loop orchestrator.

One iteration of the snowball:
  1. forward_citations.py   (S2 forward citations on every PDF)
  2. reverse_citations.py   (References-section parse on every PDF)
  3. enrich_recommendations.py (S2 /recommendations on every PDF)
  4. enrich_abstracts.py    (CrossRef abstracts for any new candidate DOIs)
  5. index_portfolio.py     (refresh DuckDB index with the new candidates)

Each step is a separate tool — this is an orchestration wrapper, not a
re-implementation. Use --until-convergence to repeat until the candidate
DOI count stops growing materially.

The wrapper does NOT auto-fetch the discovered candidates. That requires
a project-agent's relevance call and a manual lit_pull_queue.csv write.

Usage:
  python snowball.py --project Physiological_Data         # one iteration
  python snowball.py --project Physiological_Data --skip-forward  # skip slow S2 forward step
  python snowball.py --project Physiological_Data --until-convergence --max-iter 3
  python snowball.py --all                                # every active project, one iter
"""
import os, sys, io, csv, json, subprocess, argparse, time, datetime
from pathlib import Path

import duckdb

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

HERE = Path(__file__).parent
CONFIG_PATH = HERE / "projects.json"
DB_PATH = Path(os.path.expanduser(r"~\Projects\_references\portfolio.duckdb"))
LOG_PATH = Path(os.path.expanduser(r"~\Projects\_references\convergence_log.csv"))


def log_iteration(project: str, iter_num: int, n_before: int, n_after: int, growth_pct: float):
    """Append one row to the convergence log. Creates header on first write."""
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    new_file = not LOG_PATH.exists()
    with open(LOG_PATH, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(["date", "project", "iter", "n_before", "n_after", "growth_pct"])
        w.writerow([datetime.date.today().isoformat(), project, iter_num,
                    n_before, n_after, f"{growth_pct:.2f}"])


def py(): return sys.executable


def candidate_count(project: str) -> int:
    """Total unique candidate DOIs sourced from a project (forward+reverse+recs)."""
    if not DB_PATH.exists(): return 0
    con = duckdb.connect(str(DB_PATH), read_only=True)
    n = con.execute(
        "SELECT COUNT(DISTINCT doi) FROM candidates WHERE source_project = ?",
        [project]).fetchone()[0]
    con.close()
    return n


def run(cmd, label):
    print(f"\n>>> {label}")
    print(f"    {' '.join(cmd)}")
    r = subprocess.run(cmd, capture_output=True, text=True)
    out = (r.stdout or "")[-1500:]
    print(out)
    if r.returncode != 0:
        err = (r.stderr or "")[-500:]
        print(f"    ERR (rc={r.returncode}): {err}")
        return False
    return True


def one_iteration(project: str, skip_forward: bool, skip_reverse: bool,
                  skip_recs: bool, skip_abstracts: bool):
    """Run one snowball pass for a project. Returns the new candidate count."""
    tools = []
    if not skip_forward:
        tools.append(([py(), str(HERE / "forward_citations.py"),
                       "--project", project], "forward_citations"))
    if not skip_reverse:
        tools.append(([py(), str(HERE / "reverse_citations.py"),
                       "--project", project], "reverse_citations"))
    if not skip_recs:
        tools.append(([py(), str(HERE / "enrich_recommendations.py")],
                      "enrich_recommendations (portfolio-wide)"))

    for cmd, label in tools:
        run(cmd, label)

    # Refresh the index (cheap: ~few seconds per project)
    run([py(), str(HERE / "index_portfolio.py"),
         "--project", project], f"index_portfolio({project})")

    if not skip_abstracts:
        # Only enrich abstracts for DOIs we don't already have one for
        run([py(), str(HERE / "enrich_abstracts.py")],
            "enrich_abstracts (incremental — only missing)")

    return candidate_count(project)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default=None,
                    help="Project name from projects.json")
    ap.add_argument("--all", action="store_true",
                    help="Run one iteration for every active project")
    ap.add_argument("--until-convergence", action="store_true",
                    help="Repeat until candidate count stops growing >1%%")
    ap.add_argument("--max-iter", type=int, default=3,
                    help="Hard cap on iterations (default 3)")
    ap.add_argument("--skip-forward",   action="store_true")
    ap.add_argument("--skip-reverse",   action="store_true")
    ap.add_argument("--skip-recs",      action="store_true")
    ap.add_argument("--skip-abstracts", action="store_true")
    args = ap.parse_args()

    if args.all:
        with open(CONFIG_PATH, encoding="utf-8") as f:
            cfg = json.load(f).get("projects", {})
        projects = [n for n, p in cfg.items() if p.get("active", True)]
    elif args.project:
        projects = [args.project]
    else:
        print("[ERR] Pass --project NAME or --all", file=sys.stderr); sys.exit(2)

    for project in projects:
        print(f"\n{'='*72}\n  SNOWBALL: {project}\n{'='*72}")
        n_before = candidate_count(project)
        print(f"  starting candidate count: {n_before}")

        for it in range(1, args.max_iter + 1):
            print(f"\n--- iteration {it}/{args.max_iter} ---")
            n_after = one_iteration(project,
                                     args.skip_forward, args.skip_reverse,
                                     args.skip_recs, args.skip_abstracts)
            growth = n_after - n_before
            growth_pct = (100 * growth / max(1, n_before)) if n_before else float("inf")
            print(f"\n  iter {it}: {n_before} → {n_after} candidates "
                  f"(+{growth}, +{growth_pct:.1f}%)")
            log_iteration(project, it, n_before, n_after, growth_pct)
            if not args.until_convergence: break
            if it >= args.max_iter: break
            if n_before > 0 and growth_pct < 1.0:
                print(f"  converged (growth < 1%); stopping")
                break
            n_before = n_after


if __name__ == "__main__":
    main()
