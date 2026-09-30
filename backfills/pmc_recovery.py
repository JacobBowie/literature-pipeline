"""Recover the PMC stage's losses: DOIs that had a PMCID in a PMC stage report but were never
fetched (W2-A1 step 9: the September Europe PMC 403 wall, then the idconv 429, lost them).

Read-only by default: a dry-run count per project. `--stage` writes one tagged queue per project,
`lit_pull_queue.pmcrecover.csv` in the project root, never over an existing file.
`--sample-input CSV` writes a PMC-stage input (the Unpaywall report shape) for one project so the
stage can be measured on its own. Nothing here fetches anything: the commands for the 30-row
sample and for the full recovery are printed, and the recovery runs later, on the owner's go.

Where reports are read:
  * every registered project root (top level): `*.pmc.csv` and `pmc_fetch_report*.csv`;
  * `--artifact-dir DIR` under each project root (sweep's option), top level;
  * extra report directories given as arguments, `DIR` or `DIR=PROJECT`, scanned recursively (a
    consumer's archive of dated sweep exhaust, for example). A directory inside a registered
    project root belongs to the deepest such project; any other needs `=PROJECT` and is listed
    as unassigned until it gets one.
A report is any CSV with doi, pmcid and downloaded columns. A row counts when it has a PMCID and
was neither downloaded nor skipped as already present (a text-only sidecar still counts: it wants
its PDF). Dropped: DOIs the portfolio holds as a PDF (litpipe.holdings), and DOIs already waiting in
one of the project's live queues. Titles, authors and years come from the report's sibling run
artifacts (.normalized, .processed, .unpaywall) when they exist; sweep fills any row left blank.

Usage:
  python backfills/pmc_recovery.py [DIR[=PROJECT] ...] [--project KEY] [--artifact-dir DIR]
                                   [--stage] [--limit N] [--sample-input CSV] [--json OUT]
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import os
import random
import sys
from dataclasses import dataclass, field
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import lit_util  # noqa: E402
from litpipe import config, holdings  # noqa: E402
from litpipe import doi as _doi  # noqa: E402

QUEUE_NAME = "lit_pull_queue.pmcrecover.csv"
QUEUE_COLUMNS = ["doi", "title", "authors", "year", "destination", "notes"]
SAMPLE_COLUMNS = ["rank", "doi", "year", "cites", "filename", "title", "oa_status", "n_locations",
                  "downloaded", "winning_host", "winning_url", "attempts", "error"]
REPORT_GLOBS = ("*.pmc.csv", "pmc_fetch_report*.csv")
SIBLING_STAGES = ("normalized", "processed", "unpaywall")
NCBI_SERIES_LIMIT = 100          # NCBI: any series over 100 requests runs off-peak


def _truthy(v):
    return str(v or "").strip().lower() in ("true", "1", "yes")


@dataclass
class Candidate:
    doi: str
    pmcid: str
    filename: str = ""
    title: str = ""
    authors: str = ""
    year: str = ""
    report: str = ""
    last_error: str = ""


@dataclass
class ProjectTally:
    key: str
    root: Path
    reports: list = field(default_factory=list)
    rows_with_pmcid_not_fetched: int = 0
    candidates: dict = field(default_factory=dict)          # doi -> Candidate (first seen)
    held_pdf: list = field(default_factory=list)
    queued: list = field(default_factory=list)
    text_only_kept: int = 0

    def to_recover(self):
        drop = set(self.held_pdf) | set(self.queued)
        return [c for d, c in self.candidates.items() if d not in drop]


# ---------------------------------------------------------------- reading
def _read_csv(path):
    with open(path, encoding="utf-8-sig", newline="") as f:
        text = f.read()
    rdr = csv.DictReader(io.StringIO(text))
    return list(rdr.fieldnames or []), list(rdr)


def is_pmc_report(fields) -> bool:
    return {"doi", "pmcid", "downloaded"} <= {f.strip() for f in fields}


def _sibling_meta(report: Path) -> dict:
    """{doi: {title, authors, year}} from the report's sibling run artifacts."""
    name = report.name
    out = {}
    if not name.endswith(".pmc.csv"):
        return out
    stem = name[: -len(".pmc.csv")]
    for stage in SIBLING_STAGES:
        for sib in sorted(report.parent.glob(f"{stem}.{stage}*.csv")):
            try:
                _, rows = _read_csv(sib)
            except (OSError, UnicodeDecodeError, csv.Error):
                continue
            for r in rows:
                d = _doi.normalise(r.get("doi") or "")
                if not d:
                    continue
                cur = out.setdefault(d, {})
                for k in ("title", "authors", "year"):
                    if not cur.get(k) and (r.get(k) or "").strip():
                        cur[k] = r[k].strip()
    return out


def scan_report(path: Path):
    """[(Candidate, text_only)] for the rows of one PMC report with a PMCID and no fetch; None when
    the file is not a PMC report."""
    try:
        fields, rows = _read_csv(path)
    except (OSError, UnicodeDecodeError, csv.Error):
        return None
    if not is_pmc_report(fields):
        return None
    meta = None
    out = []
    for r in rows:
        pmcid = (r.get("pmcid") or "").strip()
        if not pmcid or _truthy(r.get("downloaded")) or _truthy(r.get("skipped")):
            continue
        if (r.get("winning_source") or "").strip() == "ALREADY_EXISTS":
            continue
        d = _doi.normalise(r.get("doi") or "")
        if not d:
            continue
        if meta is None:
            meta = _sibling_meta(path)
        m = meta.get(d, {})
        out.append((Candidate(d, pmcid, (r.get("filename") or "").strip(), m.get("title", ""),
                              m.get("authors", ""), m.get("year", ""), path.name,
                              (r.get("error") or "").strip()), _truthy(r.get("sidecar"))))
    return out


def report_files(directory: Path, recursive: bool):
    seen = set()
    for pat in REPORT_GLOBS:
        for p in (directory.rglob(pat) if recursive else directory.glob(pat)):
            if p.is_file() and p not in seen:
                seen.add(p)
                yield p


def live_queue_dois(root: Path) -> set:
    """DOIs waiting in the project's live queues (lit_pull_queue.csv and tagged queues)."""
    import sweep
    out = set()
    if not root.is_dir():
        return out
    for p in root.glob("lit_pull_queue*.csv"):
        if sweep.queue_tag(p.name) is None:
            continue
        try:
            _, rows = sweep.read_queue(p)
        except (OSError, UnicodeDecodeError, csv.Error):
            continue
        for r in rows:
            d = _doi.normalise(r.get("doi") or "")
            if d:
                out.add(d)
    return out


# ---------------------------------------------------------------- projects and directories
def _registry(registry):
    cfg = config.load(registry)
    return cfg, (cfg.get("projects") or {})


def project_roots(projects: dict, only=None) -> dict:
    return {k: lit_util.project_root(k, p) for k, p in projects.items()
            if isinstance(p, dict) and (only is None or k in only)}


def owner_of(directory: Path, roots: dict):
    """The registered project whose root is the deepest one containing `directory`, or None."""
    d = os.path.normcase(os.path.abspath(directory))
    best, best_len = None, -1
    for key, root in roots.items():
        r = os.path.normcase(os.path.abspath(root))
        if (d == r or d.startswith(r + os.sep)) and len(r) > best_len:
            best, best_len = key, len(r)
    return best


def parse_dir_arg(arg):
    """'DIR' or 'DIR=PROJECT' -> (Path, project or None). The split is on the last '=' (a Windows
    path has no '='; a project key has none either)."""
    if "=" in arg:
        d, key = arg.rsplit("=", 1)
        return Path(d), (key.strip() or None)
    return Path(arg), None


# ---------------------------------------------------------------- the run
def run(*, report_dirs=(), project=None, artifact_dir=None, stage=False, limit=None, sample_input=None,
        json_out=None, registry=None, holdmap=None, repo=REPO, seed=20260930) -> dict:
    cfg, projects = _registry(registry)
    only = {project} if project else None
    if project and project not in projects:
        raise SystemExit(f"--project {project!r} is not in the registry")
    roots = project_roots(projects, only)
    all_roots = project_roots(projects)
    tallies = {k: ProjectTally(k, r) for k, r in roots.items()}
    unassigned = []

    sources = []                         # (key, directory, recursive)
    for k, r in roots.items():
        sources.append((k, r, False))
        if artifact_dir:
            a = Path(artifact_dir)
            sources.append((k, a if a.is_absolute() else r / a, False))
    for arg in report_dirs or ():
        d, key = parse_dir_arg(arg)
        key = key or owner_of(d, all_roots)
        if key is None:
            unassigned.append(str(d))
            continue
        if key not in projects:
            raise SystemExit(f"{arg}: project {key!r} is not in the registry")
        if key in tallies:
            sources.append((key, d, True))

    for key, d, recursive in sources:
        if not d.is_dir():
            continue
        t = tallies[key]
        for rep in report_files(d, recursive):
            got = scan_report(rep)
            if got is None:
                continue
            t.reports.append(str(rep))
            for cand, _text_only in got:
                t.rows_with_pmcid_not_fetched += 1
                t.candidates.setdefault(cand.doi, cand)

    if holdmap is None and any(t.candidates for t in tallies.values()):
        holdmap = holdings.build(cfg, write_cache=bool(stage or sample_input))
    for key, t in tallies.items():
        queued = live_queue_dois(t.root)
        for d in t.candidates:
            if holdmap is not None and holdmap.has_pdf(d):
                t.held_pdf.append(d)            # a PDF anywhere in the portfolio: nothing to recover
            elif d in queued:
                t.queued.append(d)
            elif holdmap is not None and holdmap.text_only(d):
                t.text_only_kept += 1           # held as text only: still wants its PDF

    staged, kept_existing = {}, {}
    if stage:
        for key, t in tallies.items():
            rows = t.to_recover()[: limit or None]
            if not rows:
                continue
            q = t.root / QUEUE_NAME
            if q.exists():
                kept_existing[key] = str(q)
                continue
            dest = os.path.relpath(lit_util.lib_paths(key, projects[key])[1], t.root).replace(os.sep, "/")
            lit_util.atomic_write_csv(str(q), [
                {"doi": c.doi, "title": c.title, "authors": c.authors, "year": c.year, "destination": dest,
                 "notes": f"pmc recovery: {c.pmcid} not fetched in {c.report}"} for c in rows], QUEUE_COLUMNS)
            staged[key] = {"path": str(q), "rows": len(rows)}
    sample = None
    if sample_input:
        if not project:
            raise SystemExit("--sample-input needs --project")
        t = tallies[project]
        pool = t.to_recover()
        # a seeded random draw: the first rows in scan order would over-sample the oldest reports
        rows = random.Random(seed).sample(pool, min(limit or 30, len(pool)))
        p = Path(sample_input)
        if p.exists():
            raise SystemExit(f"{p} exists; not overwritten")
        p.parent.mkdir(parents=True, exist_ok=True)
        lit_util.atomic_write_csv(str(p), [
            {"rank": i + 1, "doi": c.doi, "year": c.year, "filename": c.filename, "title": c.title,
             "oa_status": "PMC_RECOVERY", "downloaded": "False", "error": ""} for i, c in enumerate(rows)],
            SAMPLE_COLUMNS)
        sample = {"path": str(p), "rows": len(rows)}

    summary = {
        "projects": {k: {"root": str(t.root), "reports": len(t.reports),
                         "rows_with_pmcid_not_fetched": t.rows_with_pmcid_not_fetched,
                         "distinct_dois": len(t.candidates), "held_as_pdf": len(t.held_pdf),
                         "already_queued": len(t.queued), "text_only_still_wanting_pdf": t.text_only_kept,
                         "to_recover": len(t.to_recover())}
                     for k, t in tallies.items()},
        "unassigned_dirs": unassigned, "staged": staged, "kept_existing": kept_existing, "sample": sample,
    }
    summary["total_to_recover"] = sum(p["to_recover"] for p in summary["projects"].values())
    _print(summary, tallies, projects, report_dirs, artifact_dir, repo)
    if json_out:
        Path(json_out).parent.mkdir(parents=True, exist_ok=True)
        Path(json_out).write_text(json.dumps(summary, indent=1), encoding="utf-8")
    return summary


def ncbi_requests(n):
    """NCBI requests the PMC stage sends for n DOIs: idconv per 100 plus efetch per 30."""
    return -(-n // 100) + -(-n // 30)


def _print(summary, tallies, projects, report_dirs, artifact_dir, repo):
    print(f"{'project':<34} {'reports':>7} {'rows':>6} {'dois':>6} {'held':>6} {'queued':>6} {'recover':>7}")
    for k, p in summary["projects"].items():
        if not (p["reports"] or p["distinct_dois"]):
            continue
        print(f"{k:<34} {p['reports']:>7} {p['rows_with_pmcid_not_fetched']:>6} {p['distinct_dois']:>6} "
              f"{p['held_as_pdf']:>6} {p['already_queued']:>6} {p['to_recover']:>7}")
    print(f"total to recover: {summary['total_to_recover']}")
    for d in summary["unassigned_dirs"]:
        print(f"  unassigned (pass {d}=PROJECT): {d}")
    for k, s in summary["staged"].items():
        print(f"  staged {s['rows']} row(s): {s['path']}")
    for k, pth in summary["kept_existing"].items():
        print(f"  {k}: {pth} already exists; not overwritten")
    if summary["sample"]:
        print(f"  sample input: {summary['sample']['rows']} row(s) -> {summary['sample']['path']}")

    todo = {k: p for k, p in summary["projects"].items() if p["to_recover"]}
    if not todo:
        return
    def cmd(*parts):
        return "  " + " ".join(p for p in parts if p)

    py = f'uv run --project "{repo}" python'
    script = f'"{repo / "backfills" / "pmc_recovery.py"}"'
    dirs = " ".join(f'"{d}"' for d in (report_dirs or ()))
    ad = f'--artifact-dir "{artifact_dir}"' if artifact_dir else ""
    big = max(todo, key=lambda k: todo[k]["to_recover"])
    lib = lit_util.lib_paths(big, projects[big])[1]
    root = tallies[big].root
    print("\nNot run. The 30-row sample (the PMC stage alone, into the project's library):")
    print(cmd(py, script, dirs, ad, f'--project "{big}" --limit 30',
              '--sample-input "<scratch>/pmc_recovery_sample.csv"'))
    print(cmd(py, f'"{repo / "pmc_fetch.py"}"', f'--base-dir "{root}"',
              '--report-in "<scratch>/pmc_recovery_sample.csv"', f'--lib-dir "{lib}"',
              '--report-out "<scratch>/pmc_recovery_sample.pmc.csv"'))
    print("  PDF yield = downloaded / rows in that report: 70% or more, run the recovery; 50-70%, check the "
          "author-manuscript share (pmc_class AM) first; under 50%, stop and diagnose S3 access.")
    n = summary["total_to_recover"]
    est = ncbi_requests(n)
    print(f"\nNot run. The full recovery (about {est} NCBI requests for {n} DOIs"
          + ("; over 100, so run it on a weekend or 21:00-05:00 US Eastern" if est > NCBI_SERIES_LIMIT else "")
          + "):")
    print(cmd(py, script, dirs, ad, "--stage"))
    for k in todo:
        print(cmd(py, f'"{repo / "sweep.py"}"', f'--project "{k}"', "--skip-preprint"))


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("report_dirs", nargs="*", metavar="DIR[=PROJECT]",
                    help="Extra directories of PMC stage reports, scanned recursively (a consumer's archive "
                         "of dated sweep exhaust); '=PROJECT' names the registered project they belong to.")
    ap.add_argument("--project", default=None, help="Only this registered project.")
    ap.add_argument("--artifact-dir", default=None,
                    help="Also read reports from this directory under each project root (sweep's option).")
    ap.add_argument("--stage", action="store_true",
                    help=f"Write {QUEUE_NAME} in each project root (never over an existing file).")
    ap.add_argument("--limit", type=int, default=None, help="At most N rows per staged queue or sample.")
    ap.add_argument("--sample-input", default=None,
                    help="Write a PMC-stage input CSV (Unpaywall report shape) for --project; default 30 rows.")
    ap.add_argument("--json", dest="json_out", default=None, help="Write the summary as JSON here.")
    a = ap.parse_args(argv)
    run(report_dirs=a.report_dirs, project=a.project, artifact_dir=a.artifact_dir, stage=a.stage, limit=a.limit,
        sample_input=a.sample_input, json_out=a.json_out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
