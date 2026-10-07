"""End-to-end fault check for the literature pipeline.

Two tiers (declared in projects.json):
  - Tier 1 (full pipeline): discovery, fetch, tables, text, reports.
    Used by systematic-review projects with `data_dir` set (a Tier 1 review project).
  - Tier 2 (library-only): just PDFs + .fulltext.json + .ris sidecars.
    Used by projects that just need a place to drop related papers.

Stages run by tier:
  Tier 1: 1 (discovery) to 7 (cross-checks) + tools sanity. All checks active.
  Tier 2: 3 (library state) + 4 (sidecar integrity) + 7-light (orphan sidecars, orphan .ris)
          + tools sanity. Skips discovery/triage/tables/text checks.

Library state uses the same predicates as audit_portfolio.py (one scan, shared):
  - every PDF is checked for the %PDF magic (it used to be the first 3);
  - a `.fulltext.json` with no PDF that was not extracted from a PDF is a TEXT_ONLY holding
    (INFO, DEC-08), and its `.ris` belongs to it: neither is an orphan FAIL;
  - an identity FLAG (`.identity.json` FLAG or SUPPLEMENT, `.fulltext.json` FLAG) is a review item
    (WARN): not a holding (not in the .ris coverage), not an orphan;
  - text damage (ligature, NBSP, character reference, markup tag) and `_mismatch/` are WARNs;
  - a sidecar waiting for OCR (`needs_ocr: true`, empty by design) is an INFO line of its own
    (the count and the first stems), never a bad sidecar.
With a registry project it also checks the index (read-only; freshness and DB-versus-disk) and,
once per invocation, live runs from the state file (never created by this tool).

Every check prints its count and at most 20 items; --full prints all. Exit 1 on any FAIL
(including a missing library), 2 on a usage or registry error, else 0. Read-only: nothing is
written except the --json file.

Usage:
  # By project name (loads layout from projects.json)
  python pipeline_check.py --project research_a
  python pipeline_check.py --project parent/sub

  # Every active registered project, with one summary table and one exit code
  python pipeline_check.py --all

  # Explicit paths (legacy mode, useful for projects not yet in registry)
  python pipeline_check.py --base-dir /path/to/proj --lib-dir docs/literature
"""
import argparse
import csv
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lit_util  # coerce_int (2026-06-25 audit sibling sweep)
import audit_portfolio as ap  # the shared library scan, index and live-run checks

lit_util.utf8_stdout()


CONFIG_PATH = Path(__file__).parent / "projects.json"
ITEM_CAP = ap.ITEM_CAP


def load_registry():
    return lit_util.load_projects_config(CONFIG_PATH)


def resolve_from_config(name: str, cfg=None):
    """Look up project in projects.json. Returns (base, lib, data_or_None, tier, ris_threshold)."""
    projects = (cfg if cfg is not None else load_registry()).get("projects", {})
    if name not in projects:
        print(f"[ERR] project '{name}' not in projects.json. "
              f"Known: {', '.join(sorted(projects.keys()))}", file=sys.stderr)
        sys.exit(2)
    p = projects[name]
    base, lib, data = lit_util.lib_paths(name, p)
    tier = p.get("tier", 2)
    # T8 (2026-06-25 audit): per-project .ris-coverage floor for Stage 4b. Default 90; a project
    # with legitimately DOI-less PDFs (e.g. a library that also holds technical reports or data
    # documentation, ~86%) lowers it in projects.json, so a tight floor is kept per project
    # instead of one global worst-case value.
    ris_threshold = p.get("ris_threshold", 90)
    return base, lib, data, tier, ris_threshold


class Report:
    """Collects one project's checks. `echo` prints every line; otherwise only FAIL and WARN
    lines print (the --all view)."""

    def __init__(self, project, full=False, echo=True):
        self.data = {"project": project, "checks": [], "warnings": [], "info": [], "issues": []}
        self.full, self.echo = full, echo

    def say(self, line, important=False):
        if self.echo or important:
            print(line)

    def section(self, title):
        self.say(f"\n=== {title} ===")

    def _items(self, items, fmt, important):
        items = list(items)
        shown = items if self.full else items[:ITEM_CAP]
        for it in shown:
            self.say(f"      {fmt(it)}", important)
        if len(items) > len(shown):
            self.say(f"      ... +{len(items) - len(shown)} more (--full prints all)", important)

    def check(self, label, ok, detail="", items=(), fmt=str):
        icon = "OK" if ok else "FAIL"
        self.say(f"  [{icon}] {label}{(' — ' + detail) if detail else ''}", important=not ok)
        self.data["checks"].append({"label": label, "ok": bool(ok), "detail": detail,
                                    "items": [fmt(i) for i in items]})
        if not ok:
            self.data["issues"].append(label)
            self._items(items, fmt, True)

    def warn(self, label, items, fmt=str):
        items = list(items)
        if not items:
            return
        self.say(f"  [WARN] {label}: {len(items)}", important=True)
        self._items(items, fmt, True)
        self.data["warnings"].append({"label": label, "count": len(items),
                                      "items": [fmt(i) for i in items]})

    def note(self, label, items=(), fmt=str, count=None):
        items = list(items)
        n = len(items) if count is None else count
        self.say(f"  [INFO] {label}: {n}")
        self._items(items, fmt, False)
        self.data["info"].append({"label": label, "count": n, "items": [fmt(i) for i in items]})


def check_project(base, lib, data, tier, ris_threshold, *, key=None, full=False, echo=True,
                  index=None, scan=None) -> dict:
    """Run the stages for one project; returns its result dict (issues = the FAIL labels).
    `index` is this project's entry from audit_portfolio.index_status (None: not checked);
    `scan` is audit_portfolio.scan_library(lib) when the caller already has it."""
    rep = Report(key, full=full, echo=echo)
    res = rep.data
    res.update(base=str(base), lib=str(lib), data=str(data) if data else None, tier=tier,
               ris_threshold=ris_threshold)
    rep.say(f"Project base: {base}")
    rep.say(f"Tier:         {tier}")
    rep.say(f"Library:      {lib}")
    if data:
        rep.say(f"Data dir:     {data}")
    rep.say("")

    check = rep.check

    # ---------- Stage 1: Discovery (Tier 1 only) ----------
    if tier == 1 and data:
        rep.section("Stage 1: Discovery")
        triage = data / "discovered/triage_not_in_library.csv"
        check("triage_not_in_library.csv exists", triage.exists())
        if triage.exists():
            with open(triage, encoding="utf-8") as fh:
                rows = list(csv.DictReader(fh))
            n_with_doi = sum(1 for r in rows if r.get("doi"))
            check("triage parses + has DOIs", len(rows) > 0 and n_with_doi > 0,
                  detail=f"{len(rows)} rows, {n_with_doi} with DOI")

        # ---------- Stage 2: Fetch reports ----------
        rep.section("Stage 2: Fetch reports")
        unpw = data / "discovered/unpaywall_fetch_report_v2.csv"
        pmc = data / "discovered/pmc_fetch_report.csv"
        ppr = data / "discovered/preprint_fetch_report.csv"
        for p, label in [(unpw, "unpaywall_v2"), (pmc, "pmc"), (ppr, "preprint")]:
            if p.exists():
                with open(p, encoding="utf-8") as fh:
                    rows = list(csv.DictReader(fh))
                n_dl = sum(1 for r in rows if r.get("downloaded", "").lower() == "true")
                check(f"{label}: {len(rows)} rows, {n_dl} downloaded", len(rows) > 0)
            else:
                check(f"{label} report exists", False)
    elif tier == 2:
        rep.say("\n[Tier 2: skipping Stages 1-2 (discovery, fetch reports)]")

    # ---------- Stage 3: Library state ----------
    rep.section("Stage 3: Library state")
    if scan is None and lib.is_dir():
        scan = ap.scan_library(lib)
    if not lib.is_dir():
        scan = None
    pdfs = (scan or {}).get("pdf_names") or []
    n_sidecars = (scan or {}).get("n_sidecars", 0)
    check("library exists", lib.is_dir(), detail=f"{len(pdfs)} PDFs, {n_sidecars} sidecars")
    res["counts"] = {k: (scan or {}).get(k, 0) for k in ("n_pdfs", "n_sidecars", "n_ris",
                                                         "n_identity", "pdf_holdings")}
    res["counts"]["text_only"] = len((scan or {}).get("text_only") or [])
    res["counts"]["identity_flags"] = len((scan or {}).get("flags") or [])
    if scan is not None:
        check(f"all {len(pdfs)} PDFs are real PDFs (%PDF magic)", not scan["fake_pdfs"],
              detail="" if not scan["fake_pdfs"] else f"{len(scan['fake_pdfs'])} not a PDF",
              items=scan["fake_pdfs"])
        rep.warn("tiny PDFs (<10KB)", scan["tiny_pdfs"], fmt=lambda t: f"{t[0]} ({t[1]}B)")
        if scan["text_only"]:
            rep.note(f"TEXT_ONLY holdings (DEC-08; {scan['text_only_with_ris']} with .ris)",
                     scan["text_only"])
        rep.warn("identity flags (review; not holdings)", scan["flags"], fmt=ap._fmt_flag)
        rep.warn("orphan .identity.json (no PDF)", scan["orphan_identity"])
        rep.warn("unparseable .identity.json", scan["bad_identity"], fmt=lambda t: f"{t[0]}: {t[1]}")
        mm = scan["mismatch"]
        rep.warn(f"_mismatch/ quarantine (DEC-07; {mm['pdfs']} PDFs; restoring is a separate step)",
                 mm["files"])
        for kind_of_file in ("sidecar", "ris"):
            for k in ap.DAMAGE_KINDS:
                rep.warn(f"{kind_of_file} text damage: {k}", scan["damage"][kind_of_file][k])

    # ---------- Stage 4: Sidecar integrity ----------
    rep.section("Stage 4: Sidecar integrity")
    bad = []
    if scan is not None:
        bad = [(n, e) for n, e in scan["bad_sidecars"]] + [(n, "empty text+abstract")
                                                           for n in scan["empty_sidecars"]]
    check(f"all {n_sidecars} sidecars parse + have content", not bad,
          detail="" if not bad else f"{len(bad)} bad", items=bad, fmt=lambda t: f"bad: {t[0]} ({t[1]})")
    ocr = (scan or {}).get("needs_ocr") or []
    res["counts"]["needs_ocr"] = len(ocr)
    if ocr:      # written empty on purpose by the text-validity gate: an OCR to-do, not a bad sidecar
        rep.note("needs_ocr sidecars (OCR to-do: extract_pdf_fulltext.py --ocr)",
                 [n[:-len(ap.SIDECAR)] if n.lower().endswith(ap.SIDECAR) else n for n in ocr])

    # ---------- Stage 4b: .ris coverage (all tiers) ----------
    rep.section("Stage 4b: .ris coverage")
    n_hold = (scan or {}).get("pdf_holdings", 0)
    n_no_ris = len((scan or {}).get("pdfs_no_ris") or [])
    with_ris = n_hold - n_no_ris
    pct = (100 * with_ris / n_hold) if n_hold else 0
    # T8 (2026-06-25 audit): this was hardcoded check(..., True) -- a no-op that could NEVER
    # fail, defeating the one guard positioned to catch a backfill/naming regression (Stage 4b is
    # the only .ris gate on the tier-2 projects). A real predicate against a per-project floor
    # (default 90). Empty libs pass. Identity-flagged PDFs are review items, not holdings, so
    # they are outside the denominator (an Unpaywall FLAG is kept with no .ris by design).
    ris_ok = (not n_hold) or (pct >= ris_threshold)
    check(f"{with_ris}/{n_hold} PDFs have .ris ({pct:.0f}%, floor {ris_threshold}%)", ris_ok,
          "" if ris_ok else f"below {ris_threshold}% -- likely a backfill/naming regression; run backfill_ris",
          items=[] if ris_ok else (scan or {}).get("pdfs_no_ris") or [])
    orphan_ris = (scan or {}).get("orphan_ris") or []
    check("no orphan .ris (no PDF)", not orphan_ris,
          detail="" if not orphan_ris else f"{len(orphan_ris)} orphans", items=orphan_ris)

    # ---------- Stage 5-6: Tables + build artifacts (Tier 1 only) ----------
    text_files = []
    if tier == 1 and data:
        rep.section("Stage 5: Tables")
        tables_dir = data / "tables"
        table_report = tables_dir / "_extraction_report.csv"
        check("tables/_extraction_report.csv exists", table_report.exists())
        if table_report.exists():
            with open(table_report, encoding="utf-8") as fh:
                rows = list(csv.DictReader(fh))
            n_with_t = sum(1 for r in rows if lit_util.coerce_int(r.get("n_tables")) > 0)
            n_total_t = sum(lit_util.coerce_int(r.get("n_tables")) for r in rows)
            check(f"  {n_with_t}/{len(rows)} papers with tables, {n_total_t} total", len(rows) > 0)

        rep.section("Stage 6: Library build artifacts")
        text_dir = data / "text"
        meta = data / "metadata.csv"
        abst = data / "abstracts.md"
        lib_rep = data / "library_report.md"
        check("text/ dir exists", text_dir.is_dir())
        text_files = list(text_dir.glob("*.txt")) if text_dir.is_dir() else []
        check("text files count matches PDFs", len(text_files) == len(pdfs),
              detail=f"{len(text_files)} txt vs {len(pdfs)} pdf")
        check("metadata.csv exists", meta.exists())
        check("abstracts.md exists", abst.exists())
        check("library_report.md exists", lib_rep.exists())
        if lib_rep.exists():
            rep_text = lib_rep.read_text(encoding="utf-8")
            check("library_report includes cleaning section", "Cleaning applied" in rep_text)
    else:
        rep.say("\n[Tier 2: skipping Stages 5-6 (tables, build artifacts)]")

    # ---------- Stage 7: Cross-checks ----------
    rep.section("Stage 7: Cross-checks")
    pdf_stems = {p[:-4] for p in pdfs}
    orphan_sidecars = (scan or {}).get("orphan_sidecars") or []
    check("no orphan sidecars (no PDF)", not orphan_sidecars,
          detail="" if not orphan_sidecars else f"{len(orphan_sidecars)} orphans (not text-only)",
          items=orphan_sidecars)
    rep.note("jats_only / TEXT_ONLY holdings (not orphans)", count=len((scan or {}).get("text_only") or []))

    if tier == 1 and data:
        txt_stems = {p.name[:-4] for p in text_files}
        missing_txt = sorted(pdf_stems - txt_stems)
        check("every PDF has a text dump", not missing_txt,
              detail="" if not missing_txt else f"{len(missing_txt)} missing", items=missing_txt)

        pmc = data / "discovered/pmc_fetch_report.csv"
        if pmc.exists():
            with open(pmc, encoding="utf-8") as fh:
                pmc_rows = list(csv.DictReader(fh))
            dl_rows = [r for r in pmc_rows if r.get("downloaded", "").lower() == "true"]
            missing_pdfs = []
            for r in dl_rows:
                fn = r.get("filename", "")
                if fn and fn not in pdfs:
                    ascii_fn = fn.encode("ascii", "ignore").decode("ascii")
                    if ascii_fn not in pdfs:
                        missing_pdfs.append(fn)
            check("every PMC-downloaded filename is in the library", not missing_pdfs,
                  detail="" if not missing_pdfs else f"{len(missing_pdfs)} missing", items=missing_pdfs)

    # ---------- Index (read-only), registry projects only ----------
    if index is not None:
        rep.section("Index (portfolio.duckdb, read-only)")
        if index.get("stale"):
            rep.warn("index stale", [index["stale"]])
        if index.get("count_differs"):
            rep.warn("index count differs from disk",
                     [f"{index['n_index']} in the index ({index['source']}) vs {index['n_disk']} on disk"])
        rep.warn("PDFs on disk not in the index", index.get("not_indexed") or [])
        rep.warn("index rows with no file on disk", index.get("stale_rows") or [])
        if not (index.get("stale") or index.get("count_differs") or index.get("not_indexed")
                or index.get("stale_rows")):
            rep.say(f"  [OK] index current ({index['n_index']} rows, stamp {index['stamp']})")
    res["index"] = index
    return res


def tools_sanity(rep):
    """The tools are co-located with this script (the elevated location)."""
    rep.section("Tools sanity")
    here = Path(__file__).parent
    expected_tools = [
        "unpaywall_fetch_v2.py", "pmc_fetch.py", "backfill_fulltext.py",
        "extract_tables.py", "preprint_fetch.py", "pdf_text_clean.py",
        "jats_to_text.py", "build_pdf_library.py", "pipeline_check.py",
    ]
    for t in expected_tools:
        rep.check(t, (here / t).exists())
    rep.check("vendor/mathml_to_latex/", (here / "vendor" / "mathml_to_latex").is_dir())


def _live_runs(rep, live):
    if live["error"]:
        rep.warn("live runs: state file unreadable", [live["error"]])
    elif not live["exists"]:
        rep.say(f"  [INFO] live runs: no state file ({live['state_file']})")
    else:
        rep.note("live runs (state file)", live["runs"],
                 fmt=lambda r: f"{r['run_id']} {r['kind']} pid {r['pid']} age {r.get('age_s')}s")
        rep.warn("live runs older than 6 h", live["stale"],
                 fmt=lambda r: f"{r['run_id']} {r['kind']} pid {r['pid']} started {r['started']}")


def _index_for(db, cfg, scans):
    """audit_portfolio.index_status for [(key, scan)]; the DB is opened read-only, never created."""
    try:
        db_path = Path(db) if db else ap.default_db_path(cfg)
    except Exception as e:      # a bad db_dir in projects.json: report, do not guess
        return {"db": None, "exists": False, "error": f"{type(e).__name__}: {e}", "projects": {}}
    return ap.index_status(db_path, scans)


def _index_note(rep, idx):
    if idx is None:
        return
    if idx.get("error"):
        rep.warn("index DB could not be read (read-only)", [idx["error"]])
    elif not idx["exists"]:
        rep.say(f"  [INFO] index DB not found: {idx['db']} (freshness not checked)")


def run(project=None, all_projects=False, base_dir=None, lib_dir="references/literature",
        data_dir="data/prior_art", tier=None, json_path=None, full=False, db=None, **_ignored) -> dict:
    """Check one project (--project), every active registered project (--all) or explicit paths
    (legacy --base-dir). Returns the result dict; result["exit_code"] is 1 on any FAIL, 2 on a
    usage or registry error, else 0."""
    if not (project or all_projects or base_dir):
        print("[ERR] pass --project NAME, --all, or --base-dir PATH (legacy); with none of them "
              "pipeline_check used to audit the current directory", file=sys.stderr)
        return {"exit_code": 2, "mode": None, "projects": []}
    out = {"mode": None, "projects": [], "missing": [], "inactive": [], "issues": []}
    live = ap.live_runs_status()
    out["live_runs"] = live

    if all_projects:
        out["mode"] = "all"
        cfg = load_registry()
        projects = cfg.get("projects", {}) or {}
        out["inactive"] = sorted(k for k, p in projects.items() if isinstance(p, dict)
                                 and not p.get("active", True))
        libs, missing = ap.discover(projects)
        out["missing"] = missing
        scans = {name: ap.scan_library(lib) for name, _p, lib, _t, _d in libs}
        idx = _index_for(db, cfg, list(scans.items()))
        out["index"] = {k: v for k, v in idx.items() if k != "projects"}
        print(f"Checking {len(libs)} active registered project(s) (config: {CONFIG_PATH.name})")
        for name, _p, _lib, _t, _d in libs:
            print(f"\n----- {name} -----")
            base, lib_, data_, tier_, thr = resolve_from_config(name, cfg)
            r = check_project(base, lib_, data_, tier_, thr, key=name, full=full, echo=full,
                              index=idx["projects"].get(name), scan=scans[name])
            out["projects"].append(r)
        rep = Report(None, full=full, echo=True)
        print(f"\n{'=' * 72}\n  SUMMARY ({len(out['projects'])} projects)\n{'=' * 72}")
        print(f"  {'project':<44} {'exit':>4} {'FAIL':>4} {'WARN':>4} {'PDFs':>6} {'text':>5} {'flag':>4}")
        for r in out["projects"]:
            c = r["counts"]
            print(f"  {str(r['project'])[:44]:<44} {1 if r['issues'] else 0:>4} {len(r['issues']):>4} "
                  f"{len(r['warnings']):>4} {c['n_pdfs']:>6} {c['text_only']:>5} {c['identity_flags']:>4}")
        rep.check("every active registered library exists", not missing,
                  detail="" if not missing else f"registered but missing: {len(missing)}",
                  items=missing, fmt=lambda m: f"{m['project']}: {m['reason']} ({m['lib']})")
        if out["inactive"]:
            rep.note("inactive (skipped)", out["inactive"])
        tot_text = sum(r["counts"]["text_only"] for r in out["projects"])
        tot_flags = sum(r["counts"]["identity_flags"] for r in out["projects"])
        rep.note("jats_only / TEXT_ONLY holdings, all projects", count=tot_text)
        rep.note("identity flags (review), all projects", count=tot_flags)
        _index_note(rep, idx)
    else:
        idx = index = scan = None
        if project:
            out["mode"] = "project"
            cfg = load_registry()
            known = cfg.get("projects", {}) or {}
            if project not in known:
                print(f"[ERR] project '{project}' not in projects.json. "
                      f"Known: {', '.join(sorted(known))}", file=sys.stderr)
                out["exit_code"] = 2
                return out
            base, lib, data, tier_, ris_threshold = resolve_from_config(project, cfg)
            if lib.is_dir():
                scan = ap.scan_library(lib)
                idx = _index_for(db, cfg, [(project, scan)])
                out["index"] = {k: v for k, v in idx.items() if k != "projects"}
                index = idx["projects"].get(project)
        else:
            out["mode"] = "legacy"
            base = Path(base_dir).resolve()
            lib = base / lib_dir
            data = base / data_dir
            tier_ = tier or 1   # legacy default: assume full pipeline
            ris_threshold = 90  # legacy path has no config; use the default floor
        r = check_project(base, lib, data, tier_, ris_threshold, key=project, full=full, echo=True,
                          index=index, scan=scan)
        out["projects"].append(r)
        rep = Report(project, full=full, echo=True)
        _index_note(rep, idx)

    rep.section("Live runs")
    _live_runs(rep, live)
    tools_sanity(rep)

    issues = [f"{r['project'] or 'project'}: {i}" if all_projects else i
              for r in out["projects"] for i in r["issues"]] + rep.data["issues"]
    out["issues"] = issues
    out["warnings"] = rep.data["warnings"]
    out["exit_code"] = 1 if issues else 0
    print("\n=== Result ===")
    if issues:
        print(f"  {len(issues)} issue(s) found:")
        for i in issues:
            print(f"    - {i}")
    else:
        print("  All stages OK.")
    if json_path:
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2, default=str, ensure_ascii=False)
        print(f"  JSON written: {json_path}")
    return out


def build_parser():
    p = argparse.ArgumentParser(description="End-to-end fault check for literature-pipeline projects "
                                            "(read-only).")
    p.add_argument("--project", default=None,
                   help="Project name from projects.json (e.g. 'research_a', or a subproject 'parent/sub').")
    p.add_argument("--all", dest="all_projects", action="store_true",
                   help="Check every active registered project; one summary table and one exit code.")
    p.add_argument("--base-dir", default=None,
                   help="(legacy) Project root. Used if --project not given.")
    p.add_argument("--lib-dir", default="references/literature",
                   help="(legacy) PDF + sidecar directory, relative to --base-dir.")
    p.add_argument("--data-dir", default="data/prior_art",
                   help="(legacy) Build-artifact directory, relative to --base-dir.")
    p.add_argument("--tier", type=int, default=None,
                   help="(legacy) Override tier (1 or 2). Inferred from --project if used.")
    p.add_argument("--json", dest="json_path", default=None,
                   help="Write machine-readable output to this path.")
    p.add_argument("--full", action="store_true",
                   help="Print every item of every check (default: count plus the first 20).")
    p.add_argument("--db", default=None,
                   help="Index DB to check, opened read-only (default: <db_dir>/portfolio.duckdb).")
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    return run(**vars(args))["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
