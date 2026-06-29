"""Migrate closed-access / failed sweep residuals to <project>/lit_pull_queue.md.

After a sweep run, parses the latest *.unpaywall.csv + *.pmc.csv + *.preprint.csv
trio for a project and identifies DOIs that are NOT in the library (closed-access,
failed downloads, no preprint). Appends them as a dated checklist section to the
project's lit_pull_queue.md so the user can ILLIAD them.

Idempotent: skips DOIs whose PDF already exists in the project's lib_dir.

Usage:
  python migrate_closed_to_md.py --project LIV
  python migrate_closed_to_md.py --project LIV --date 2026-05-27  # explicit date
"""
import argparse
import csv
import json
import os
import re
import sys
from datetime import date
from pathlib import Path

import lit_util  # RC4: atomic_write_text for crash-safe .md writes

HERE = Path(__file__).parent
CONFIG_PATH = HERE / "projects.json"
PROJECTS_ROOT = Path(os.path.expanduser(r"~\Projects"))


def project_dir(project: str, proj_cfg: dict) -> Path:
    """Resolve the on-disk project directory for a registry key.

    RC11: a subproject (e.g. 'Physiological_Data/Yitts') declares a `parent`;
    its sweep artifacts + lit_pull_queue.md live under the parent's tree, not at
    PROJECTS_ROOT/<rawkey>. Mirror audit_portfolio's parent-aware resolution so
    the path is correct regardless of the '/' embedded in the registry key.
    """
    parent = (proj_cfg or {}).get("parent")
    if parent:
        tail = project[len(parent):].lstrip("/\\") or Path(project).name
        return PROJECTS_ROOT / parent / tail
    return PROJECTS_ROOT / project


def latest_sweep_date(project_root: Path) -> str | None:
    """Find the most recent YYYY-MM-DD prefix on .report.csv files at root."""
    pattern = re.compile(r"lit_pull_queue\.(\d{4}-\d{2}-\d{2})\.report\.csv$")
    dates = []
    for f in project_root.iterdir():
        m = pattern.match(f.name)
        if m:
            dates.append(m.group(1))
    return max(dates) if dates else None


def read_report_chain(project_root: Path, sweep_date: str):
    """Read the unpaywall report and figure out which DOIs ultimately failed.
    Returns a list of dicts with: doi, title, year, oa_status, why."""
    unpaywall_path = project_root / f"lit_pull_queue.{sweep_date}.unpaywall.csv"
    pmc_path = project_root / f"lit_pull_queue.{sweep_date}.pmc.csv"
    preprint_path = project_root / f"lit_pull_queue.{sweep_date}.preprint.csv"

    if not unpaywall_path.exists():
        return []

    # Start from unpaywall — anything NOT downloaded there
    not_downloaded = {}
    with open(unpaywall_path, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r.get("downloaded", "").lower() not in ("true", "1"):
                # Skip rows whose PDF is already in lib (unpaywall short-circuited)
                if r.get("oa_status", "").strip().upper() == "SKIP_EXISTS":
                    continue
                doi = r.get("doi", "").strip()
                if doi:
                    not_downloaded[doi] = {
                        "doi": doi,
                        "title": r.get("title", "").strip(),
                        "year": r.get("year", "").strip(),
                        "oa_status": r.get("oa_status", "").strip(),
                        "winning_host": r.get("winning_host", "").strip(),
                        "error": r.get("error", "").strip(),
                        "stage_unpaywall": "FAIL",
                        "stage_pmc": "skip",
                        "stage_preprint": "skip",
                    }

    # Mark PMC successes
    if pmc_path.exists():
        with open(pmc_path, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                doi = r.get("doi", "").strip()
                if doi in not_downloaded:
                    # 2026-06-25 audit sibling sweep (T5b parallel oracle): an already-present
                    # paper reports skipped=True / winning_source=ALREADY_EXISTS with
                    # downloaded=False. Treat it as resolved (the PDF is on disk) so it is NOT
                    # mis-routed to the manual-pull/ILL queue as closed-access (the exact harm T3
                    # guards). Mirrors sweep.py got_dois.
                    if (r.get("downloaded", "").lower() in ("true", "1")
                            or r.get("skipped", "").lower() in ("true", "1")
                            or r.get("winning_source", "") == "ALREADY_EXISTS"):
                        not_downloaded.pop(doi)
                    else:
                        not_downloaded[doi]["stage_pmc"] = "no_pmcid" if not r.get("pmcid") else "fail"
    else:
        # T3 (2026-06-25): PMC report ABSENT (stage crashed or never ran). We cannot conclude
        # these are closed-access (PMC may have fetched them). Tag so render flags 're-sweep'.
        for v in not_downloaded.values():
            v["stage_pmc"] = "no_report"

    # Mark preprint successes
    if preprint_path.exists():
        with open(preprint_path, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                doi = r.get("doi", "").strip()
                if doi in not_downloaded:
                    # 2026-06-25 audit sibling sweep: preprint emits status=ALREADY_EXISTS /
                    # skipped=True for an already-present paper; treat as resolved, not closed-access.
                    if (r.get("downloaded", "").lower() in ("true", "1")
                            or r.get("skipped", "").lower() in ("true", "1")
                            or r.get("status", "") == "ALREADY_EXISTS"):
                        not_downloaded.pop(doi)
                    else:
                        not_downloaded[doi]["stage_preprint"] = "no_match"
    else:
        for v in not_downloaded.values():
            if v["stage_preprint"] == "skip":
                v["stage_preprint"] = "no_report"

    return list(not_downloaded.values())


def render_md_block(project: str, sweep_date: str, rows: list) -> str:
    if not rows:
        return ""

    lines = [
        "",
        "---",
        "",
        f"## Sweep residuals {sweep_date}: {len(rows)} closed-access / failed (auto-migrated)",
        "",
        f"Auto-migrated from `lit_pull_queue.{sweep_date}.*.csv` by `_tools/literature_pipeline/migrate_closed_to_md.py`. ",
        "Each row failed Unpaywall + PMC + preprint fetcher chain. ILLIAD candidates.",
        "",
    ]
    if any(r["stage_pmc"] == "no_report" or r["stage_preprint"] == "no_report" for r in rows):
        lines.append("> NOTE: rows tagged `pmc=no_report` / `preprint=no_report` had that stage's report "
                     "ABSENT (crash or skip) -- they are NOT confirmed closed-access. RE-SWEEP before ILLIAD.")
        lines.append("")
    for r in rows:
        title = r["title"] or "(no title)"
        # Compact stage trace
        why_bits = []
        if r["oa_status"]:
            why_bits.append(f"oa_status={r['oa_status']}")
        if r["error"]:
            why_bits.append(f"unpaywall_err={r['error']}")
        if r["stage_pmc"] not in ("skip",):
            why_bits.append(f"pmc={r['stage_pmc']}")
        if r["stage_preprint"] not in ("skip",):
            why_bits.append(f"preprint={r['stage_preprint']}")
        why = "; ".join(why_bits) or "unknown"
        year = f" ({r['year']})" if r["year"] else ""
        lines.append(f"- [ ] **{title}**{year} — DOI `{r['doi']}` — {why}")
    lines.append("")
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", required=True)
    ap.add_argument("--date", default=None, help="YYYY-MM-DD; defaults to latest sweep")
    args = ap.parse_args()

    with open(CONFIG_PATH, encoding="utf-8") as f:
        cfg = json.load(f).get("projects", {})
    if args.project not in cfg:
        print(f"[ERR] '{args.project}' not in projects.json", file=sys.stderr)
        return 2

    project_root = project_dir(args.project, cfg[args.project])  # RC11
    if not project_root.exists():
        print(f"[ERR] project root missing: {project_root}", file=sys.stderr)
        return 2

    sweep_date = args.date or latest_sweep_date(project_root)
    if not sweep_date:
        print(f"[--] no sweep artifacts found at {project_root.name}; nothing to migrate")
        return 0

    rows = read_report_chain(project_root, sweep_date)
    if not rows:
        print(f"[--] {args.project} sweep {sweep_date}: no closed/failed residuals")
        return 0

    md_path = project_root / "lit_pull_queue.md"
    block = render_md_block(args.project, sweep_date, rows)

    existing = md_path.read_text(encoding="utf-8") if md_path.exists() else ""

    # Content-aware dedup (replaces the old date-header skip). Drop residual rows
    # whose DOI is already present in the .md, rather than skipping the whole date.
    # A legit same-day SECOND sweep (different queue; .unpaywall/.pmc/.preprint
    # overwritten) carries NEW DOIs that a date-level skip would silently drop.
    # DOIs are rendered backtick-wrapped (`DOI \`<doi>\``) by render_md_block below.
    existing_dois = set(re.findall(r"DOI `([^`]+)`", existing))
    rows = [r for r in rows if r["doi"] not in existing_dois]
    if not rows:
        print(f"[--] {args.project}: sweep {sweep_date} residuals already in "
              f"{md_path.name}; nothing new to append")
        return 0

    # Init the md file with a header if absent
    if not existing:
        existing = (
            f"# Manual Pull Queue — {args.project}\n\n"
            "Papers requiring **manual fetch** (closed-access, paywalled, paper-only). "
            "NOT consumed by `sweep.py`. Track ILLIAD requests here.\n"
        )

    # RC4: rewrite the whole file atomically (header + prior content + new block)
    lit_util.atomic_write_text(str(md_path), existing + block)

    print(f"[OK] {args.project}: appended {len(rows)} residual row(s) to {md_path.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
