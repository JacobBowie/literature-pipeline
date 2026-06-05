"""Seed a draft lit_pull_queue.csv from the top_candidates view.

Reads `top_candidates` for a project, applies simple heuristic filters,
joins paper_metadata for authors, and emits a draft queue at the project
root with a `# REVIEW BEFORE SWEEP` header comment + `.draft.csv` suffix.

The user reviews the draft, drops irrelevant rows, then renames it to
`lit_pull_queue.csv` and runs `sweep.py --project NAME`.

Defaults (calibrated 2026-05-05 against PD + getpaid distributions):
  --year-min 2010   (drops pre-2010 unless overridden)
  --min-seeds 3     (drops one-off references)
  --min-cites 0     (don't filter recent low-citation papers)
  --limit 100       (cap at 100 rows for human review)

Usage:
  python seed_queue_from_top_candidates.py --project Physiological_Data
  python seed_queue_from_top_candidates.py --project getpaid --min-seeds 5 --limit 50
"""
import argparse, csv, json, os, sys, io
from pathlib import Path

import duckdb

import lit_util  # RC4: atomic_write_text for crash-safe draft writes

try:
    if getattr(sys.stdout, "encoding", "").lower() != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, OSError):
    pass

HERE = Path(__file__).parent
CONFIG_PATH = HERE / "projects.json"
DB_PATH = Path(os.path.expanduser(r"~\Projects\_references\portfolio.duckdb"))
PROJECTS_ROOT = Path(os.path.expanduser(r"~\Projects"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", required=True, help="Project name from projects.json")
    ap.add_argument("--year-min", type=int, default=2010)
    ap.add_argument("--min-seeds", type=int, default=3)
    ap.add_argument("--min-cites", type=int, default=0)
    ap.add_argument("--limit", type=int, default=100)
    ap.add_argument("--destination", default=None,
                    help="Override per-project destination (default: lib_dir from projects.json)")
    ap.add_argument("--output", default=None,
                    help="Override output path (default: <project>/lit_pull_queue.draft.csv)")
    args = ap.parse_args()

    from ris_emit import load_projects_config
    cfg = load_projects_config(CONFIG_PATH).get("projects", {})
    if args.project not in cfg:
        print(f"[ERR] '{args.project}' not in projects.json", file=sys.stderr); sys.exit(2)
    proj_cfg = cfg[args.project]

    destination = args.destination or proj_cfg.get("lib_dir", "docs/literature") + "/"
    if not destination.endswith("/"):
        destination += "/"

    out_path = Path(args.output) if args.output else (
        PROJECTS_ROOT / args.project / "lit_pull_queue.draft.csv")

    # RC11: `via_projects` is a comma-joined STRING_AGG of source_project names.
    # The old `LIKE '%project%'` substring match leaked subproject candidates
    # into the parent (e.g. 'Physiological_Data' matched the token
    # 'Physiological_Data/Yitts'). Anchor on the comma delimiters so we match a
    # WHOLE token only: wrap both the column and the needle in commas.
    project_token_match = "(',' || t.via_projects || ',') LIKE ?"
    project_needle = f"%,{args.project},%"
    where_clause = f"""
            WHERE {project_token_match}
              AND t.year >= ?
              AND t.n_seeds_pointing >= ?
              AND t.max_cited_by >= ?
              AND t.title IS NOT NULL
              AND substr(t.title, 1, 1) ~ '[A-Za-z]'
    """
    filter_params = [project_needle, args.year_min, args.min_seeds, args.min_cites]

    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        # Count the full filtered set first so we can warn on silent truncation.
        total_matching = con.execute(
            f"SELECT COUNT(*) FROM top_candidates t {where_clause}",
            filter_params).fetchone()[0]
        rows = con.execute(f"""
            SELECT t.doi, t.title, m.authors, t.year,
                   t.n_seeds_pointing, t.max_cited_by, t.sources
            FROM top_candidates t
            LEFT JOIN paper_metadata m ON m.doi = t.doi
            {where_clause}
            ORDER BY t.n_seeds_pointing DESC, t.max_cited_by DESC
            LIMIT ?
        """, filter_params + [args.limit]).fetchall()
    finally:
        con.close()

    if total_matching > args.limit:
        print(f"[WARN] {total_matching} candidates passed the filters but "
              f"--limit={args.limit} truncated the draft to the top {args.limit}. "
              f"Raise --limit (or tighten --min-seeds/--year-min) to see the rest.",
              file=sys.stderr)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    # Build the draft in-memory, then write atomically (RC4): a crash/Ctrl-C
    # mid-write must never leave a truncated draft that the stager would treat
    # as a valid queue.
    buf = io.StringIO()
    buf.write(f"# REVIEW BEFORE SWEEP — drop irrelevant rows, then `mv {out_path.name} lit_pull_queue.csv`\n")
    buf.write(f"# Source: top_candidates view, project={args.project}\n")
    buf.write(f"# Filters: year>={args.year_min}, n_seeds>={args.min_seeds}, max_cited_by>={args.min_cites}, limit={args.limit}\n")
    buf.write(f"# Total rows: {len(rows)}\n")
    w = csv.writer(buf)
    w.writerow(["doi", "title", "authors", "year", "destination", "notes"])
    for doi, title, authors, year, seeds, cites, sources in rows:
        note = f"n_seeds={seeds} cites={cites} src={sources}"
        w.writerow([doi, (title or "").strip(), authors or "", year or "",
                    destination, note])
    lit_util.atomic_write_text(str(out_path), buf.getvalue())

    print(f"Wrote {len(rows)} draft rows to {out_path}")
    print(f"Next: review, drop irrelevant rows, then:")
    print(f"  mv {out_path} {out_path.parent / 'lit_pull_queue.csv'}")
    print(f"  python {HERE / 'sweep.py'} --project {args.project}")


if __name__ == "__main__":
    main()
