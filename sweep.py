"""Sweep project lit-pull queues and run the pipeline against each.

Per the project-notes skill convention, downstream sessions can drop a
`lit_pull_queue.csv` at the root of any project under `Projects/`, with this
shape:

    doi,title,authors,year,destination,notes
    10.1152/jappl.1972.32.6.812,"Predicting rectal temperature","Givoni B; Goldman R",1972,docs/literature/,for Chapter 3

This script:
  1. Walks `Projects/<project>/lit_pull_queue.csv` files
  2. For each, runs Unpaywall v2 + PMC fetch (and optionally preprint) against
     it, with `--triage` pointed at the queue and `--lib-dir` pointed at the
     `<project>/<destination>/` from the CSV (uses the first row's destination —
     all rows in one queue should share a destination)
  3. Renames the queue to `lit_pull_queue.<YYYY-MM-DD>.processed.csv`
     (a same-day re-sweep gets a numeric suffix: `.processed.2.csv`, `.3.csv`, ...)
  4. Writes a `lit_pull_queue.<YYYY-MM-DD>.report.csv` next to it
  5. Appends a `✅ Lit pull done:` line to LOOSE_ENDS.md

Usage:
  python sweep.py                                         # walks all projects
  python sweep.py --project Physiological_Data            # one project
  python sweep.py --dry-run                               # show plan without running
"""
import sys, csv, argparse, subprocess, datetime
from pathlib import Path

import lit_util
lit_util.utf8_stdout()

HERE = Path(__file__).resolve().parent
CONFIG_PATH = HERE / "projects.json"


def find_queues(only_project=None):
    """Yield (project_key, project_dir, queue_csv_path) for each active registered
    project that has a staged lit_pull_queue.csv.

    Registry-driven (2026-07-14, A1 fix): resolve each projects.json key via
    lit_util.project_root (tail-aware), so a subproject key like
    'Physiological_Data/Yitts' resolves to <Projects>/Physiological_Data/Yitts and
    ITS queue is found. Replaces the old PROJECTS.iterdir() filesystem walk, which
    only saw top-level dirs and whose `child.name` compare could never match a slash
    key -- so a registered subproject's queue was silently never swept (a permanent
    no-op that the orchestrator read as success). Registration is now the entry
    point: only active registered projects are swept. An explicit --project that is
    not in the registry still resolves as a top-level dir (backward compatible).
    """
    cfg = lit_util.load_projects_config(CONFIG_PATH).get("projects", {})
    keys = [only_project] if only_project else [
        k for k, v in cfg.items() if v.get("active", True)]
    for key in keys:
        root = lit_util.project_root(key, cfg.get(key, {}))
        queue = root / "lit_pull_queue.csv"
        if queue.exists():
            yield key, root, queue


def first_destination(queue_csv):
    """Read the first row's destination column. All rows should share."""
    with open(queue_csv, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            d = (r.get("destination") or "").strip()
            if d: return d
    return None


def normalize_queue_for_pipeline(queue_csv, out_csv):
    """The unpaywall_fetch_v2 script expects a `citation_count` column.
    Rewrite the queue to add it (defaulting to 0) and pass through the rest."""
    with open(queue_csv, encoding="utf-8") as fin, \
         open(out_csv, "w", encoding="utf-8", newline="") as fout:
        rdr = csv.DictReader(fin)
        cols = list(rdr.fieldnames or [])
        if "citation_count" not in cols: cols.append("citation_count")
        w = csv.DictWriter(fout, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for r in rdr:
            r.setdefault("citation_count", "0")
            w.writerow(r)


def run_pipeline(project_dir, queue_csv, dry_run=False, run_date=None):
    """Run unpaywall_v2 → pmc_fetch against this queue. Returns dict of results."""
    dest_rel = first_destination(queue_csv)
    if not dest_rel:
        print(f"  ERR no `destination` column in {queue_csv}", file=sys.stderr)
        return None
    project_root = project_dir.resolve()
    lib_dir = (project_dir / dest_rel).resolve()
    try:
        lib_dir.relative_to(project_root)
    except ValueError:
        print(f"  ERR destination escapes project root: {dest_rel!r} in {queue_csv}", file=sys.stderr)
        return None
    lib_dir.mkdir(parents=True, exist_ok=True)
    # D4b: honor an explicit run date (run_daily passes its start date) so this run's report
    # artifacts and migrate's --date stay in lockstep even if the sweep crosses midnight.
    today = run_date or datetime.date.today().isoformat()
    norm_csv = queue_csv.with_name(f"lit_pull_queue.{today}.normalized.csv")
    report_unpw = queue_csv.with_name(f"lit_pull_queue.{today}.unpaywall.csv")
    report_pmc  = queue_csv.with_name(f"lit_pull_queue.{today}.pmc.csv")
    report_ppr  = queue_csv.with_name(f"lit_pull_queue.{today}.preprint.csv")
    residual_csv = queue_csv.with_name(f"lit_pull_queue.{today}.residual.csv")
    summary_csv = queue_csv.with_name(f"lit_pull_queue.{today}.report.csv")

    normalize_queue_for_pipeline(queue_csv, norm_csv)
    with open(norm_csv, encoding="utf-8") as f:
        n_rows = sum(1 for _ in csv.DictReader(f))

    if dry_run:
        print(f"  DRY would process {n_rows} rows -> {lib_dir}")
        return {"dry": True, "rows": n_rows, "destination": str(lib_dir)}

    py = sys.executable
    stages_ok = True   # D1: flips False on a non-fatal stage failure (PMC/preprint) so the
                       # queue is NOT renamed to .processed -- which would hide it from
                       # find_queues and defeat the auto re-sweep the WARNs advise.

    # Stage 1: Unpaywall
    cmd1 = [py, str(HERE / "unpaywall_fetch_v2.py"),
            "--top-n", str(n_rows + 5),
            "--triage", str(norm_csv),
            "--lib-dir", str(lib_dir),
            "--report", str(report_unpw),
            "--base-dir", str(project_dir)]
    print(f"  -> {' '.join(cmd1)}")
    r1 = subprocess.run(cmd1, capture_output=True, text=True, encoding="utf-8", errors="replace")
    print(r1.stdout[-1500:] if r1.stdout else "")
    if r1.returncode != 0:
        print(f"  ERR unpaywall stage failed:\n{r1.stderr[-500:]}")
        return None

    # Stage 2: PMC (uses unpaywall report as input to find failures)
    cmd2 = [py, str(HERE / "pmc_fetch.py"),
            "--report-in", str(report_unpw),
            "--lib-dir", str(lib_dir),
            "--report-out", str(report_pmc),
            "--base-dir", str(project_dir)]
    print(f"  -> {' '.join(cmd2)}")
    r2 = subprocess.run(cmd2, capture_output=True, text=True, encoding="utf-8", errors="replace")
    print(r2.stdout[-1500:] if r2.stdout else "")
    if r2.returncode != 0 or not report_pmc.exists():
        # T3 (2026-06-25): PMC stage crashed (non-zero exit or no report) -> its successes go
        # uncounted and migrate_closed_to_md may list PMC-fetchable papers as ILL. WARN loudly;
        # a missing report must NOT read as 'PMC found nothing'.
        print(f"  [WARN] PMC stage did NOT complete (exit {r2.returncode}, "
              f"report={'present' if report_pmc.exists() else 'MISSING'}); residuals may be "
              f"mis-routed to ILL -- re-sweep before treating them as closed-access."
              + (f"\n{r2.stderr[-400:]}" if r2.stderr else ""))
        stages_ok = False   # D1: PMC incomplete -> keep the queue for a re-sweep

    # Stage 3: preprint_fetch (arXiv/bioRxiv/OSF/Europe PMC preprints) for any
    # rows that BOTH unpaywall and PMC failed on. Filter to those before calling
    # so we don't waste API calls or risk duplicating already-fetched papers
    # with a preprint version.
    # T5b (2026-06-25 audit): an already-present paper reports oa_status=SKIP_EXISTS (unpaywall)
    # or skipped/winning_source=ALREADY_EXISTS (pmc) with downloaded=False. If we only treat
    # downloaded==true as "got", those fall through to preprint_fetch and a _preprint duplicate
    # is fetched (the _preprint slug differs, so preprint's own skip-exists guard misses it).
    got_dois = set()
    def _got(r):
        d = (r.get("doi") or "").strip().lower()
        if d: got_dois.add(d)
    if report_unpw.exists():
        with open(report_unpw, encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                if r.get("downloaded", "").lower() == "true" or r.get("oa_status", "") == "SKIP_EXISTS":
                    _got(r)
    if report_pmc.exists():
        with open(report_pmc, encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                if (r.get("downloaded", "").lower() == "true"
                        or r.get("skipped", "").lower() == "true"
                        or r.get("winning_source", "") == "ALREADY_EXISTS"):
                    _got(r)

    n_ppr = 0
    with open(norm_csv, encoding="utf-8") as fin:
        residual_rows = [r for r in csv.DictReader(fin)
                         if r.get("doi", "").strip().lower() not in got_dois]
    if residual_rows:
        with open(residual_csv, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=residual_rows[0].keys())
            w.writeheader(); w.writerows(residual_rows)
        cmd3 = [py, str(HERE / "preprint_fetch.py"),
                "--triage", str(residual_csv),
                "--lib-dir", str(lib_dir),
                "--report", str(report_ppr)]
        print(f"  -> {' '.join(cmd3)}")
        r3 = subprocess.run(cmd3, capture_output=True, text=True, encoding="utf-8", errors="replace")
        print(r3.stdout[-1500:] if r3.stdout else "")
        if r3.returncode != 0 or not report_ppr.exists():
            print(f"  [WARN] preprint stage did NOT complete (exit {r3.returncode}, "
                  f"report={'present' if report_ppr.exists() else 'MISSING'})."
                  + (f"\n{r3.stderr[-400:]}" if r3.stderr else ""))
            stages_ok = False   # D1: preprint incomplete -> keep the queue for a re-sweep
        if report_ppr.exists():
            with open(report_ppr, encoding="utf-8") as fh:
                n_ppr = sum(1 for r in csv.DictReader(fh)
                            if r.get("downloaded", "").lower() == "true")

    # Build a final summary
    n_unpw = n_pmc = 0
    if report_unpw.exists():
        with open(report_unpw, encoding="utf-8") as fh:
            n_unpw = sum(1 for r in csv.DictReader(fh)
                         if r.get("downloaded","").lower() == "true")
    if report_pmc.exists():
        with open(report_pmc, encoding="utf-8") as fh:
            n_pmc = sum(1 for r in csv.DictReader(fh)
                        if r.get("downloaded","").lower() == "true")
    n_total = n_unpw + n_pmc + n_ppr

    with open(summary_csv, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["stage","downloaded"])
        w.writerow(["unpaywall_v2", n_unpw])
        w.writerow(["pmc_fetch", n_pmc])
        w.writerow(["preprint_fetch", n_ppr])
        w.writerow(["total", n_total])

    # Stage 4: PDF text extraction for any PDFs in lib_dir that lack a
    # .fulltext.json sidecar (PMC backfill leaves non-OA / Unpaywall-only PDFs
    # unindexed; this fills them in via pdftotext + pdfplumber fallback).
    cmd4 = [py, str(HERE / "extract_pdf_fulltext.py"),
            "--lib-dir", str(lib_dir)]
    print(f"  -> {' '.join(cmd4)}")
    r4 = subprocess.run(cmd4, capture_output=True, text=True, encoding="utf-8", errors="replace")
    print(r4.stdout[-1500:] if r4.stdout else "")
    if r4.returncode != 0:
        print(f"  WARN pdf-extract stage failed (continuing):\n{r4.stderr[-500:]}")

    # D1: only mark the queue processed if the fetch stages (PMC + preprint) completed. A
    # non-fatal PMC/preprint failure leaves stages_ok False; renaming to .processed removes
    # the queue from find_queues (which matches only 'lit_pull_queue.csv') and defeats the
    # auto re-sweep. Stage-4 PDF-extract failure does NOT block -- it is indexing-only, runs
    # over the whole lib_dir, and is independently idempotent (re-runnable next sweep).
    if not stages_ok:
        print(f"  [WARN] fetch stage(s) incomplete; LEAVING {queue_csv.name} in place for the "
              f"next sweep to retry (NOT renamed to .processed).")
        return {"rows": n_rows, "downloaded": n_total,
                "unpaywall": n_unpw, "pmc": n_pmc, "preprint": n_ppr,
                "report": str(summary_csv), "processed": None, "partial": True}

    # Mark queue as processed. A same-day re-sweep would collide on this name
    # (FileExistsError on Windows os.rename), so disambiguate with a numeric suffix.
    processed = queue_csv.with_name(f"lit_pull_queue.{today}.processed.csv")
    if processed.exists():
        n = 2
        while queue_csv.with_name(f"lit_pull_queue.{today}.processed.{n}.csv").exists():
            n += 1
        processed = queue_csv.with_name(f"lit_pull_queue.{today}.processed.{n}.csv")
    queue_csv.rename(processed)
    norm_csv.unlink(missing_ok=True)

    return {"rows": n_rows, "downloaded": n_total,
            "unpaywall": n_unpw, "pmc": n_pmc, "preprint": n_ppr,
            "report": str(summary_csv), "processed": str(processed)}


def resolve_loose_ends_path():
    """Path of the cross-project lit-pull log, from the gitignored projects.json
    "loose_ends" key (relative to lit_util.PROJECTS_ROOT, or an absolute path).

    Returns None when the key is absent. The log is an OPT-IN feature -- the
    literature-pipeline skill/SOP documents wiring it -- so the code does not invent a
    location; the caller reports the skip and the sweep proceeds. Keeping the path in
    gitignored config (not source) is also what keeps an internal project name out of
    this public repo.
    """
    cfg = lit_util.load_projects_config(CONFIG_PATH, missing_ok=True)
    configured = cfg.get("loose_ends")
    if not configured:
        return None
    p = Path(configured).expanduser()
    return p if p.is_absolute() else (lit_util.PROJECTS_ROOT / p)


def append_loose_end(line):
    """Append `line` to the configured lit-pull log and return the path written, or
    None when no "loose_ends" key is set (nothing written -- the caller reports it)."""
    path = resolve_loose_ends_path()
    if path is None:
        return None
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(line.rstrip() + "\n")
    return path


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    ap.add_argument("--project", help="Process only this project (default: all)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Show the queues that would be processed without fetching anything.")
    ap.add_argument("--date", default=None,
                    help="YYYY-MM-DD to date this run's report artifacts (default: today). "
                         "run_daily passes its run date so sweep + migrate stay in lockstep.")
    args = ap.parse_args()

    from ris_emit import warn_if_default_email
    warn_if_default_email()

    queues = list(find_queues(only_project=args.project))
    if not queues:
        scope = args.project or "any project"
        print(f"No lit_pull_queue.csv found in {scope}.")
        # A1 defense-in-depth: an explicit --project with no queue is a failure the
        # orchestrator must NOT read as success (the old silent no-op loop). A bare
        # sweep with nothing staged is a normal idle run (exit 0).
        return 1 if args.project else 0

    print(f"Found {len(queues)} queue(s):\n")
    for key, proj, q in queues:
        print(f"  {key}/{q.name}")
    print()

    if resolve_loose_ends_path() is None:
        print("[litpipe] no `loose_ends` key in projects.json -- cross-project "
              "lit-pull log is off this run; see the literature-pipeline skill to "
              "enable it.", file=sys.stderr)

    for key, proj, q in queues:
        print(f"\n=== {key} ===")
        result = run_pipeline(proj, q, dry_run=args.dry_run, run_date=args.date)
        if not result:
            continue
        if result.get("dry"):
            continue
        if result.get("partial"):
            # D1: fetch stage(s) failed; queue left in place for the next sweep. Do NOT write a
            # '✅ done' line -- flag it partial so the re-sweep isn't read as already-closed.
            partial = (f"⏸️ Lit pull PARTIAL: {key}/ — fetch stage incomplete, "
                       f"{key}/lit_pull_queue.csv left for re-sweep. "
                       f"Report: {Path(result['report']).name}")
            dest = append_loose_end(partial)
            if dest:
                print(f"\n  {dest} updated: {partial}")
            continue
        line = (f"✅ Lit pull done: {key}/ — {result['downloaded']}/{result['rows']} "
                f"fetched (Unpaywall {result['unpaywall']}, PMC {result['pmc']}, "
                f"Preprint {result.get('preprint',0)}). Report: {Path(result['report']).name}")
        dest = append_loose_end(line)
        if dest:
            print(f"\n  {dest} updated: {line}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
