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
  4. Writes a `lit_pull_queue.<YYYY-MM-DD>.report.csv` next to it
  5. Appends a `✅ Lit pull done:` line to LOOSE_ENDS.md

Usage:
  python sweep.py                                         # walks all projects
  python sweep.py --project Physiological_Data            # one project
  python sweep.py --dry-run                               # show plan without running
"""
import os, sys, io, csv, argparse, subprocess, datetime
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

HERE = Path(__file__).resolve().parent
PROJECTS = HERE.parent.parent  # _tools/literature_pipeline/ -> Projects/
LOOSE_ENDS = PROJECTS / "Git-R-Dun" / "files" / "LOOSE_ENDS.md"


def find_queues(only_project=None):
    """Yield (project_dir, queue_csv_path) for every active queue."""
    for child in PROJECTS.iterdir():
        if not child.is_dir(): continue
        if child.name.startswith("_"): continue  # skip _tools, _portfolio, etc.
        if only_project and child.name != only_project: continue
        queue = child / "lit_pull_queue.csv"
        if queue.exists():
            yield child, queue


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


def run_pipeline(project_dir, queue_csv, dry_run=False):
    """Run unpaywall_v2 → pmc_fetch against this queue. Returns dict of results."""
    dest_rel = first_destination(queue_csv)
    if not dest_rel:
        print(f"  ERR no `destination` column in {queue_csv}")
        return None
    lib_dir = project_dir / dest_rel
    lib_dir.mkdir(parents=True, exist_ok=True)
    today = datetime.date.today().isoformat()
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

    # Stage 1: Unpaywall
    cmd1 = [py, str(HERE / "unpaywall_fetch_v2.py"),
            "--top-n", str(n_rows + 5),
            "--triage", str(norm_csv),
            "--lib-dir", str(lib_dir),
            "--report", str(report_unpw),
            "--base-dir", str(project_dir)]
    print(f"  -> {' '.join(cmd1)}")
    r1 = subprocess.run(cmd1, capture_output=True, text=True)
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
    r2 = subprocess.run(cmd2, capture_output=True, text=True)
    print(r2.stdout[-1500:] if r2.stdout else "")

    # Stage 3: preprint_fetch (arXiv/bioRxiv/OSF/Europe PMC preprints) for any
    # rows that BOTH unpaywall and PMC failed on. Filter to those before calling
    # so we don't waste API calls or risk duplicating already-fetched papers
    # with a preprint version.
    got_dois = set()
    if report_unpw.exists():
        with open(report_unpw, encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                if r.get("downloaded", "").lower() == "true": got_dois.add(r["doi"].strip().lower())
    if report_pmc.exists():
        with open(report_pmc, encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                if r.get("downloaded", "").lower() == "true": got_dois.add(r["doi"].strip().lower())

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
        r3 = subprocess.run(cmd3, capture_output=True, text=True)
        print(r3.stdout[-1500:] if r3.stdout else "")
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
    r4 = subprocess.run(cmd4, capture_output=True, text=True)
    print(r4.stdout[-1500:] if r4.stdout else "")
    if r4.returncode != 0:
        print(f"  WARN pdf-extract stage failed (continuing):\n{r4.stderr[-500:]}")

    # Mark queue as processed
    processed = queue_csv.with_name(f"lit_pull_queue.{today}.processed.csv")
    queue_csv.rename(processed)
    norm_csv.unlink(missing_ok=True)

    return {"rows": n_rows, "downloaded": n_total,
            "unpaywall": n_unpw, "pmc": n_pmc, "preprint": n_ppr,
            "report": str(summary_csv), "processed": str(processed)}


def append_loose_end(line):
    LOOSE_ENDS.parent.mkdir(parents=True, exist_ok=True)
    with open(LOOSE_ENDS, "a", encoding="utf-8") as f:
        f.write(line.rstrip() + "\n")


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    ap.add_argument("--project", help="Process only this project (default: all)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Show the queues that would be processed without fetching anything.")
    args = ap.parse_args()

    queues = list(find_queues(only_project=args.project))
    if not queues:
        scope = args.project or "any project"
        print(f"No lit_pull_queue.csv found in {scope}.")
        return

    print(f"Found {len(queues)} queue(s):\n")
    for proj, q in queues:
        print(f"  {proj.name}/{q.name}")
    print()

    for proj, q in queues:
        print(f"\n=== {proj.name} ===")
        result = run_pipeline(proj, q, dry_run=args.dry_run)
        if not result:
            continue
        if result.get("dry"):
            continue
        proj_rel = proj.name
        dest = first_destination(Path(result["processed"])) or "(unknown)"
        line = (f"✅ Lit pull done: {proj_rel}/ — {result['downloaded']}/{result['rows']} "
                f"fetched (Unpaywall {result['unpaywall']}, PMC {result['pmc']}, "
                f"Preprint {result.get('preprint',0)}). Report: {Path(result['report']).name}")
        append_loose_end(line)
        print(f"\n  LOOSE_ENDS.md updated: {line}")


if __name__ == "__main__":
    main()
