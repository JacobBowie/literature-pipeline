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

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except (AttributeError, OSError):
    pass

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
    """Unique *unowned* candidate DOIs sourced from a project (forward+reverse+recs).

    RC9: the old query counted COUNT(DISTINCT doi) FROM candidates only — which
    (a) ignored the `recommendations` signal entirely, so a snowball iteration
    that grew only via recs read as zero growth (false convergence), and
    (b) counted candidates already in the library, unlike the `top_candidates`
    view the seeder actually consumes (which filters NOT EXISTS paper_locations).
    Convergence must track the same quantity the downstream queue draws from, so
    we now mirror top_candidates' ownership filter AND union the recommendations
    table (attributed to the project via its seed paper's library location)."""
    if not DB_PATH.exists(): return 0
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        n = con.execute(
            """
            WITH proj_candidates AS (
                -- forward + reverse discovery, restricted to this project,
                -- excluding papers already owned anywhere (top_candidates parity)
                SELECT DISTINCT c.doi
                FROM candidates c
                WHERE c.source_project = ?
                  AND NOT EXISTS (SELECT 1 FROM paper_locations l WHERE l.doi = c.doi)
                UNION
                -- S2 recommendations: the table has no source_project, so attribute
                -- a recommendation to this project when its SEED paper lives in this
                -- project's library; same NOT-EXISTS unowned filter
                SELECT DISTINCT r.recommended_doi AS doi
                FROM recommendations r
                JOIN paper_locations sl ON sl.doi = r.seed_doi AND sl.project = ?
                WHERE NOT EXISTS (
                    SELECT 1 FROM paper_locations l WHERE l.doi = r.recommended_doi)
            )
            SELECT COUNT(DISTINCT doi) FROM proj_candidates
            """,
            [project, project]).fetchone()[0]
    finally:
        con.close()
    return n


def run(cmd, label):
    t0 = datetime.datetime.now().strftime("%H:%M:%S")
    print(f"\n>>> [{t0}] {label}")
    print(f"    {' '.join(cmd)}")
    r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    t1 = datetime.datetime.now().strftime("%H:%M:%S")
    print(f"    ... done at {t1}")
    out = (r.stdout or "")[-1500:]
    print(out)
    if r.returncode != 0:
        err = (r.stderr or "")[-500:]
        print(f"    ERR (rc={r.returncode}): {err}")
        return False
    return True


def one_iteration(project: str, skip_forward: bool, skip_reverse: bool,
                  skip_recs: bool, skip_abstracts: bool):
    """Run one snowball pass for a project.

    Returns (new_candidate_count, ok) where `ok` is False if ANY step's
    subprocess exited non-zero. RC6: a failed discovery step (DNS/timeout/5xx
    swallowed into an empty result) must NOT be read as zero growth and trip
    false convergence — the caller refuses to declare convergence when ok is
    False."""
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

    ok = True
    for cmd, label in tools:
        ok = run(cmd, label) and ok

    # Refresh the index (cheap: ~few seconds per project)
    ok = run([py(), str(HERE / "index_portfolio.py"),
              "--project", project], f"index_portfolio({project})") and ok

    if not skip_abstracts:
        # Only enrich abstracts for DOIs we don't already have one for
        ok = run([py(), str(HERE / "enrich_abstracts.py")],
                 "enrich_abstracts (incremental — only missing)") and ok

    return candidate_count(project), ok


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

    from ris_emit import warn_if_default_email
    warn_if_default_email()

    if args.all:
        from ris_emit import load_projects_config
        cfg = load_projects_config(CONFIG_PATH).get("projects", {})
        projects = [n for n, p in cfg.items() if p.get("active", True)]
    elif args.project:
        projects = [args.project]
    else:
        print("[ERR] Pass --project NAME or --all", file=sys.stderr); sys.exit(2)

    any_failure = False
    for project in projects:
        print(f"\n{'='*72}\n  SNOWBALL: {project}\n{'='*72}")
        n_before = candidate_count(project)
        print(f"  starting candidate count: {n_before}")

        for it in range(1, args.max_iter + 1):
            print(f"\n--- iteration {it}/{args.max_iter} ---")
            n_after, ok = one_iteration(project,
                                        args.skip_forward, args.skip_reverse,
                                        args.skip_recs, args.skip_abstracts)
            growth = n_after - n_before
            growth_pct = (100 * growth / max(1, n_before)) if n_before else float("inf")
            print(f"\n  iter {it}: {n_before} → {n_after} candidates "
                  f"(+{growth}, +{growth_pct:.1f}%)")
            log_iteration(project, it, n_before, n_after, growth_pct)
            if not ok:
                # RC6: a step failed this iteration; flat/zero growth here is
                # NOT trustworthy as convergence. Stop the loop but flag it as a
                # failure rather than silently logging false convergence.
                any_failure = True
                print(f"  [WARN] a step failed this iteration; stopping WITHOUT "
                      f"declaring convergence (results may be incomplete)")
                break
            if not args.until_convergence: break
            if it >= args.max_iter: break
            if n_before > 0 and growth_pct < 1.0:
                print(f"  converged (growth < 1%); stopping")
                break
            n_before = n_after

    if any_failure:
        # Non-zero exit so an overnight `--all --until-convergence` run that
        # silently no-op'd on errors is visible to the orchestrator/operator.
        sys.exit(1)


if __name__ == "__main__":
    main()
