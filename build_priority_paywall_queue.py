"""Build a consolidated, prioritized paywall queue for a manual browser session.

Inputs (all read-only):
  - residual CSVs (lit_pull_queue.*.residual.csv) in each registered project root and its direct
    subdirectories (archive/ and _archive/ skipped): a typed row counts only when its
    residual_class is TERMINAL_CLOSED; every row of a legacy CSV (no residual_class column) counts
  - litpipe.holdings                           DOIs already held anywhere (PDF or text-only; an
                                               identity-flagged file is not a holding)
  - portfolio.duckdb top_candidates            ranking signal (n_seeds_pointing, max_cited_by)

Output (written atomically, LF line endings):
  - <out-dir>/<date>_priority_paywall_queue.md   human-readable, tiered, doi.org links
  - <out-dir>/<date>_priority_paywall_queue.csv  rank,doi,url,title,year,score,seeds_pointing,cited_by,projects
  <out-dir> is --out-dir, else projects.json's global "portfolio_dir" (the folder paywall_pull
  reads; '~' expands, a relative path is under the projects root). With neither the run exits 1
  saying how to set it: there is no built-in folder.

Logic:
  1. Gather residual closed-access DOIs (best citation_count + which projects want each).
  2. Drop any DOI already held anywhere in the portfolio (litpipe.holdings, DOI-grounded).
  3. Left-join top_candidates for n_seeds_pointing + max_cited_by.
  4. score = n_seeds_pointing*10 + max_cited_by  (papers many of my seeds cite rank highest).
  5. Emit ranked .md (Priority A = top 50) + full .csv.

The registry, root and DB are resolved when run() is called (this module's CONFIG_PATH,
lit_util.PROJECTS_ROOT, projects.json db_dir or --db); importing the module opens no DB.
`lib_dois()`, `LIBS` and `ROOT` stay importable (API surface); lib_dois() reads each `.ris` DO
line and each sidecar `doi` as structured DOIs (litpipe.doi.normalise_structured). paywall_pull
decides "done" through litpipe.holdings, like run() here.

Exit codes: 0 written; 1 registry error, or no output folder (no --out-dir and no
"portfolio_dir"); 2 written without the DB ranking (the index could not be read), with a final
"[step-summary] {json}" line.

Usage:
  python build_priority_paywall_queue.py --date 2026-06-22
  python build_priority_paywall_queue.py --date 2026-06-22 --top-a 50 --db <portfolio.duckdb> --out-dir <dir>
"""
import argparse, glob, os, json, re, sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lit_util  # coerce_int: shared int()-on-messy-CSV-cell guard (2026-06-25 audit sibling sweep)

lit_util.utf8_stdout()

ROOT = str(lit_util.PROJECTS_ROOT)
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "projects.json")
DB_NAME = "portfolio.duckdb"
CSV_FIELDS = ["rank", "doi", "url", "title", "year", "score", "seeds_pointing", "cited_by", "projects"]


def load_libs():
    """{project: ROOT-relative lib_dir} from the gitignored projects.json registry.

    Keeps real project names and the absolute user path out of this committed file
    (they live only in projects.json, which is .gitignore'd). Mirrors the loader
    convention in index_portfolio.py / pipeline_check.py. Covers every active,
    lib_dir-bearing project in the registry, so dedup stays in sync as projects
    are added there rather than needing a hand-edit here.
    """
    cfg = lit_util.load_projects_config(CONFIG_PATH, missing_ok=True).get("projects", {})
    if not cfg:
        # projects.json is gitignored -> absent on fresh clone / in CI. No registry
        # means no known libraries: return {} (not raise) so imports/--help keep
        # working (paywall_pull does `from build_priority_paywall_queue import LIBS`).
        print(f"[warn] no libraries loaded from projects.json ({CONFIG_PATH}); dedup disabled.",
              file=sys.stderr)
        return {}
    return {name: lit_util.lib_rel(name, p)
            for name, p in cfg.items()
            if p.get("active", True) and p.get("lib_dir")}


LIBS = load_libs()
DOI_RE = re.compile(r"10\.\d{4,9}/[-._;()/:A-Za-z0-9]+")   # kept importable; lib_dois() no longer scans with it
_RIS_DO = re.compile(r"^DO\s+-\s?(.+?)\s*$", re.MULTILINE)


def _structured(raw):
    from litpipe import doi as _doi
    return (_doi.normalise_structured(raw) or "").lower()


def lib_dois():
    """The DOIs the libraries in LIBS under ROOT carry: every `.ris` DO line and every sidecar `doi`,
    each read as a structured DOI (litpipe.doi.normalise_structured), lower case. A DOI in a note,
    an abstract or a reference line is not the record's DOI and is not read (M125). The queue
    decides held-ness through litpipe.holdings inside run()."""
    have = set()
    for rel in LIBS.values():
        lib = os.path.join(ROOT, rel)
        if not os.path.isdir(lib):
            continue
        for ris in glob.glob(os.path.join(lib, "*.ris")):
            try:
                with open(ris, encoding="utf-8", errors="replace") as fh:
                    text = fh.read()
            except OSError:
                continue
            for m in _RIS_DO.findall(text):
                d = _structured(m)
                if d:
                    have.add(d)
        for sc in glob.glob(os.path.join(lib, "*.fulltext.json")):
            try:
                with open(sc, encoding="utf-8") as fh:
                    rec = json.load(fh)
            except (OSError, ValueError):
                continue
            d = _structured(rec.get("doi") if isinstance(rec, dict) else "")
            if d:
                have.add(d)
    return have


def portfolio_dir(registry):
    """The registry's global "portfolio_dir" as a Path ('~' expands; a relative path is under
    lit_util.PROJECTS_ROOT, read at call time), or None when unset. A value that is not a non-empty
    string raises config.ConfigError."""
    from litpipe import config
    raw = (registry or {}).get("portfolio_dir")
    if raw is None:
        return None
    if not isinstance(raw, str) or not raw.strip():
        raise config.ConfigError(f"portfolio_dir must be a non-empty path string, got {raw!r}")
    p = Path(raw.strip()).expanduser()
    return p if p.is_absolute() else Path(lit_util.PROJECTS_ROOT) / p


NO_OUT_DIR = ('no output folder: set "portfolio_dir" (global) in projects.json, the folder that holds '
              'the paywall queue files, or pass --out-dir')


def load_registry(cfg=None):
    """projects.json through litpipe.config.load: `cfg` when given, else this module's CONFIG_PATH
    read now (a missing or unreadable file raises config.ConfigError: exit 1)."""
    from litpipe import config
    if cfg is not None:
        return config.load(cfg)
    if not os.path.exists(CONFIG_PATH):
        raise config.ConfigError(f"projects.json not found at {CONFIG_PATH} (copy projects.json.template)")
    try:
        reg = config.load(lit_util.load_projects_config(CONFIG_PATH))
    except (OSError, ValueError) as e:
        raise config.ConfigError(f"projects.json at {CONFIG_PATH} is unreadable: {type(e).__name__}: {e}") from None
    if not isinstance(reg, dict) or not isinstance(reg.get("projects") or {}, dict):
        raise config.ConfigError(f"projects.json at {CONFIG_PATH} has no `projects` object")
    return reg


def residual_dois(registry=None, *, stats=None):
    """{doi key: {doi, title, year, cites_csv, projs}} of the residual rows that count (see the
    module docstring). The key is litpipe.holdings.doi_key, so a DOI that does not normalise is
    kept in its stripped lower-case form, never dropped. `stats` (a dict) receives the CSV and row
    counts (worklists.read_residuals)."""
    from litpipe import doi as _doi, worklists
    registry = registry if registry is not None else load_registry()
    rows, st = worklists.read_residuals(worklists.residual_csvs(registry))
    if stats is not None:
        stats.update(st)
    resid = {}
    for proj, row in rows:
        raw = (row.get("doi") or "").strip()
        if not raw:
            continue
        k = worklists.doi_key(raw)
        c = lit_util.coerce_int(row.get("citation_count"))
        e = resid.setdefault(k, {"doi": _doi.normalise_structured(raw) or k, "title": "", "year": "",
                                 "cites_csv": 0, "projs": set()})
        e["cites_csv"] = max(e["cites_csv"], c)
        e["projs"].add(proj)
        if not e["title"] and row.get("title"):
            e["title"] = row["title"].strip()
        if not e["year"] and row.get("year"):
            e["year"] = str(row["year"]).strip()
    return resid


def db_signal(entries, db_path):
    """({doi key: (n_seeds_pointing, max_cited_by, title, year)}, error) from top_candidates, with a
    paper_metadata fallback for title/year. `entries` is {key: {"doi": ...}}; each DOI is looked up
    in its structured form and its key. error is None, or why the DB could not be read."""
    sig = {}
    if not entries:
        return sig, None
    try:
        import duckdb
    except ImportError:
        return sig, "duckdb not importable"
    if not os.path.exists(db_path):
        return sig, f"no index at {db_path}"
    try:
        con = duckdb.connect(str(db_path), read_only=True)
    except Exception as e:
        return sig, f"DB locked/unavailable ({e})"
    forms = {}
    for k, e in entries.items():
        for f in {e["doi"].lower(), k}:
            forms.setdefault(f, []).append(k)
    look = sorted(forms)
    err = None
    try:
        for doi, seeds, cited, title, year in con.execute(
                "SELECT lower(doi), n_seeds_pointing, max_cited_by, title, year FROM top_candidates "
                "WHERE lower(doi) IN (" + ",".join("?" * len(look)) + ")", look).fetchall():
            for k in forms.get(doi, ()):
                sig.setdefault(k, (seeds or 0, cited or 0, title or "", year or ""))
        missing = sorted({f for f, ks in forms.items() if not any(k in sig for k in ks)})
        if missing:
            for doi, title, year in con.execute(
                    "SELECT lower(doi), title, year FROM paper_metadata "
                    "WHERE lower(doi) IN (" + ",".join("?" * len(missing)) + ")", missing).fetchall():
                for k in forms.get(doi, ()):
                    sig.setdefault(k, (0, 0, title or "", year or ""))
    except Exception as e:
        err = f"DB query failed ({e}); partial ranking"
    finally:
        con.close()
    return sig, err


def render_md(rows, date, top_a, csv_name):
    ranked_signal = [r for r in rows if r["score"] > 0]
    out = [f"# Priority paywall queue — {date}\n\n",
           f"**{len(rows)} closed-access papers** still missing from the libraries "
           f"(deduped against current .ris/sidecars). Open each `https://doi.org/...` "
           f"link via institutional access / ILLIAD and drop the PDF into the requesting "
           f"project's lib_dir, then re-run `index_portfolio.py`.\n\n",
           f"Ranking score = `n_seeds_pointing*10 + cited_by` "
           f"(papers many of your seed PDFs cite rank highest). "
           f"{len(ranked_signal)} have a DB signal; the rest are unranked residuals.\n\n",
           f"## Priority A — top {min(top_a, len(rows))} (work these first)\n\n"]
    for i, r in enumerate(rows[:top_a], 1):
        out.append(f"{i}. [{r['doi']}]({r['url']}) "
                   f"— {r['title'] or '(title pending)'} ({r['year'] or 'n.d.'}) "
                   f"— score {r['score']} (seeds {r['seeds']}, cites {r['cites']}) "
                   f"— _{r['projs']}_\n")
    out.append(f"\n## Priority B — remaining {max(0, len(rows) - top_a)} "
               f"(see the .csv for the full ranked list)\n\n")
    out.append(f"Full machine-readable list: `{csv_name}`\n")
    return "".join(out)


def run(*, date, top_a=50, db=None, out_dir=None, registry=None, holdmap=None) -> dict:
    """Build the queue. Returns {"exit_code", "status", "csv", "md", "rows", "priority_a",
    "with_signal", "held", "residual" (CSV and row counts), "db", "db_error"}."""
    from litpipe import config, holdings, worklists
    res = {"exit_code": 0, "status": "ok", "date": date}
    try:
        registry = load_registry(registry)
        db_path = str(db) if db else str(config.db_dir(registry) / DB_NAME)
        if not out_dir:
            out_dir = portfolio_dir(registry)
            if out_dir is None:
                raise config.ConfigError(NO_OUT_DIR)
    except config.ConfigError as e:
        print(f"[ERR] {e}", file=sys.stderr)
        return {**res, "exit_code": 1, "status": "config", "error": str(e)}
    out_dir = str(out_dir)

    stats = {}
    resid = residual_dois(registry, stats=stats)
    unreadable = stats.get("unreadable") or []
    for p in unreadable:
        print(f"[warn] residual CSV not read (open in another program?): {p}", file=sys.stderr)
    hm = holdmap if holdmap is not None else holdings.build(registry, write_cache=False)
    held = {k for k in resid if hm.where(k)}
    missing = {k: v for k, v in resid.items() if k not in held}
    print(f"residual CSVs={stats.get('csvs', 0)} ({stats.get('legacy_csvs', 0)} legacy)  rows read="
          f"{stats.get('rows_read', 0)} kept={stats.get('rows_kept', 0)}  excluded by class="
          f"{sum(stats.get('excluded', {}).values())}")
    print(f"held DOIs={len(held)}  residual DOIs={len(resid)}  still-missing={len(missing)}")

    sig, db_error = db_signal(missing, db_path)
    if db_error:
        print(f"[warn] {db_error}; skipping DB ranking", file=sys.stderr)
    rows = []
    for k, v in missing.items():
        seeds, cited, t_db, y_db = sig.get(k, (0, 0, "", ""))
        title = v["title"] or t_db
        year = v["year"] or (str(y_db) if y_db else "")
        cites = max(v["cites_csv"], cited)
        score = seeds * 10 + cites
        rows.append({"doi": v["doi"], "url": worklists.doi_link(v["doi"]), "title": title, "year": year,
                     "seeds": seeds, "cites": cites, "score": score, "projs": ",".join(sorted(v["projs"]))})
    # 2026-06-25 audit sibling sweep (HIGH): r["year"] is a raw residual-CSV cell and can be
    # non-numeric ('in press', '2020a'); a bare int() here crashed the whole queue build.
    # Ties fall to the DOI, never to the order the files were listed in.
    rows.sort(key=lambda r: (-r["score"], -lit_util.coerce_int(r["year"]), r["doi"]))

    os.makedirs(out_dir, exist_ok=True)
    csv_p = os.path.join(out_dir, f"{date}_priority_paywall_queue.csv")
    md_p = os.path.join(out_dir, f"{date}_priority_paywall_queue.md")
    lit_util.atomic_write_csv(csv_p, [
        {"rank": i, "doi": r["doi"], "url": r["url"], "title": r["title"], "year": r["year"],
         "score": r["score"], "seeds_pointing": r["seeds"], "cited_by": r["cites"], "projects": r["projs"]}
        for i, r in enumerate(rows, 1)], CSV_FIELDS)
    lit_util.atomic_write_text(md_p, render_md(rows, date, top_a, os.path.basename(csv_p)))

    n_signal = sum(1 for r in rows if r["score"] > 0)
    print(f"wrote {md_p}")
    print(f"wrote {csv_p}")
    print(f"Priority A: {min(top_a, len(rows))}   total: {len(rows)}   with DB signal: {n_signal}")
    res.update(csv=csv_p, md=md_p, rows=len(rows), priority_a=min(top_a, len(rows)), with_signal=n_signal,
               held=len(held), residual=stats, db=db_path, db_error=db_error)
    reasons = ([db_error] if db_error else []) + (
        [f"{len(unreadable)} residual CSV(s) could not be read"] if unreadable else [])
    if reasons:
        res.update(exit_code=2, status="degraded")
        print("[step-summary] " + json.dumps({"step": "build_priority_paywall_queue", "exit_code": 2,
                                              "reasons": reasons, "aborted": None, "transport_failures": 0}))
    return res


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--date", required=True)
    ap.add_argument("--top-a", type=int, default=50)
    ap.add_argument("--db", default=None,
                    help="index to rank with, opened read-only (default: <db_dir>/portfolio.duckdb from projects.json)")
    ap.add_argument("--out-dir", default=None,
                    help='where the queue files go (default: projects.json "portfolio_dir"; with neither, '
                         'the run exits 1)')
    args = ap.parse_args(sys.argv[1:] if argv is None else argv)
    return run(date=args.date, top_a=args.top_a, db=args.db, out_dir=args.out_dir)["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
