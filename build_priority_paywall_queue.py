"""Build a consolidated, prioritized paywall queue for a manual browser session.

Inputs (all read-only):
  - <project>/lit_pull_queue.*.residual.csv   closed-access DOIs sweep couldn't fetch
  - each lib's .ris + .fulltext.json           DOIs we already HAVE (dedup target)
  - portfolio.duckdb top_candidates            ranking signal (n_seeds_pointing, max_cited_by)

Output:
  - _portfolio/<date>_priority_paywall_queue.md   human-readable, tiered, doi.org links
  - _portfolio/<date>_priority_paywall_queue.csv  doi,url,title,year,score,seeds,cites,projects

Logic:
  1. Gather residual closed-access DOIs (best citation_count + which projects want each).
  2. Drop any DOI already present in a current library (DOI-grounded dedup, not filenames).
  3. Left-join top_candidates for n_seeds_pointing + max_cited_by.
  4. score = n_seeds_pointing*10 + max_cited_by  (papers many of my seeds cite rank highest).
  5. Emit ranked .md (Priority A = top 50) + full .csv.

Usage:
  python build_priority_paywall_queue.py --date 2026-06-22
  python build_priority_paywall_queue.py --date 2026-06-22 --top-a 50
"""
import argparse, glob, csv, os, json, re, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lit_util  # coerce_int: shared int()-on-messy-CSV-cell guard (2026-06-25 audit sibling sweep)

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

ROOT = os.path.expanduser("~/Projects")
DB = os.path.join(ROOT, "_references", "portfolio.duckdb")
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "projects.json")


def load_libs():
    """{project: ROOT-relative lib_dir} from the gitignored projects.json registry.

    Keeps real project names and the absolute user path out of this committed file
    (they live only in projects.json, which is .gitignore'd). Mirrors the loader
    convention in index_portfolio.py / pipeline_check.py. Covers every active,
    lib_dir-bearing project in the registry, so dedup stays in sync as projects
    are added there rather than needing a hand-edit here.
    """
    with open(CONFIG_PATH, encoding="utf-8") as f:
        cfg = json.load(f)
    libs = {}
    for name, p in cfg.get("projects", {}).items():
        if not p.get("active", True) or not p.get("lib_dir"):
            continue
        base = p.get("parent") or name
        libs[name] = f"{base}/{p['lib_dir']}"
    return libs


LIBS = load_libs()
DOI_RE = re.compile(r"10\.\d{4,9}/[-._;()/:A-Za-z0-9]+")


def lib_dois():
    have = set()
    for rel in LIBS.values():
        lib = os.path.join(ROOT, rel)
        if not os.path.isdir(lib):
            continue
        for ris in glob.glob(os.path.join(lib, "*.ris")):
            try:
                for m in DOI_RE.findall(open(ris, encoding="utf-8", errors="replace").read()):
                    have.add(m.lower().rstrip(".").rstrip(")"))
            except OSError:
                pass
        for sc in glob.glob(os.path.join(lib, "*.fulltext.json")):
            try:
                d = (json.load(open(sc, encoding="utf-8")).get("doi") or "").strip().lower()
                if d:
                    have.add(d)
            except (OSError, json.JSONDecodeError):
                pass
    return have


def residual_dois():
    resid = {}
    for f in glob.glob(os.path.join(ROOT, "*", "lit_pull_queue.*.residual.csv")):
        proj = os.path.basename(os.path.dirname(f))
        try:
            for row in csv.DictReader(open(f, encoding="utf-8", errors="replace")):
                doi = (row.get("doi") or "").strip().lower()
                if not doi:
                    continue
                c = lit_util.coerce_int(row.get("citation_count"))
                e = resid.setdefault(doi, {"title": "", "year": "", "cites_csv": 0, "projs": set()})
                e["cites_csv"] = max(e["cites_csv"], c)
                e["projs"].add(proj)
                if not e["title"] and row.get("title"):
                    e["title"] = row["title"].strip()
                if not e["year"] and row.get("year"):
                    e["year"] = str(row["year"]).strip()
        except OSError:
            pass
    return resid


def db_signal(dois):
    """Return {doi: (n_seeds_pointing, max_cited_by, title, year)} from top_candidates/paper_metadata."""
    sig = {}
    try:
        import duckdb
    except ImportError:
        print("[warn] duckdb not importable; skipping DB ranking")
        return sig
    try:
        con = duckdb.connect(DB, read_only=True)
    except Exception as e:
        print(f"[warn] DB locked/unavailable ({e}); skipping DB ranking")
        return sig
    try:
        # top_candidates carries the seed-pointing + cite signal
        for doi, seeds, cited, title, year in con.execute(
            "SELECT lower(doi), n_seeds_pointing, max_cited_by, title, year "
            "FROM top_candidates WHERE lower(doi) IN "
            "(" + ",".join("?" * len(dois)) + ")", list(dois)
        ).fetchall() if dois else []:
            sig[doi] = (seeds or 0, cited or 0, title or "", year or "")
        # paper_metadata fallback for title/year where not a candidate
        # (paper_metadata carries no cite count; cites live only in top_candidates)
        missing = [d for d in dois if d not in sig]
        if missing:
            for doi, title, year in con.execute(
                "SELECT lower(doi), title, year FROM paper_metadata "
                "WHERE lower(doi) IN (" + ",".join("?" * len(missing)) + ")", missing
            ).fetchall():
                sig[doi] = (0, 0, title or "", year or "")
    except Exception as e:
        print(f"[warn] DB query failed ({e}); partial ranking")
    finally:
        con.close()
    return sig


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--top-a", type=int, default=50)
    args = ap.parse_args()

    have = lib_dois()
    resid = residual_dois()
    missing = {d: v for d, v in resid.items() if d not in have}
    print(f"lib DOIs={len(have)}  residual DOIs={len(resid)}  still-missing={len(missing)}")

    sig = db_signal(list(missing.keys()))
    rows = []
    for doi, v in missing.items():
        seeds, cited, t_db, y_db = sig.get(doi, (0, 0, "", ""))
        title = v["title"] or t_db
        year = v["year"] or (str(y_db) if y_db else "")
        cites = max(v["cites_csv"], cited)
        score = seeds * 10 + cites
        rows.append({"doi": doi, "title": title, "year": year, "seeds": seeds,
                     "cites": cites, "score": score, "projs": ",".join(sorted(v["projs"]))})
    # 2026-06-25 audit sibling sweep (HIGH): r["year"] is a raw residual-CSV cell and can be
    # non-numeric ('in press', '2020a'); a bare int() here crashed the whole queue build.
    rows.sort(key=lambda r: (-r["score"], -lit_util.coerce_int(r["year"])))

    outdir = os.path.join(ROOT, "_portfolio")
    csv_p = os.path.join(outdir, f"{args.date}_priority_paywall_queue.csv")
    md_p = os.path.join(outdir, f"{args.date}_priority_paywall_queue.md")

    with open(csv_p, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["rank", "doi", "url", "title", "year", "score", "seeds_pointing", "cited_by", "projects"])
        for i, r in enumerate(rows, 1):
            w.writerow([i, r["doi"], f"https://doi.org/{r['doi']}", r["title"], r["year"],
                        r["score"], r["seeds"], r["cites"], r["projs"]])

    ranked_signal = [r for r in rows if r["score"] > 0]
    with open(md_p, "w", encoding="utf-8") as f:
        f.write(f"# Priority paywall queue — {args.date}\n\n")
        f.write(f"**{len(rows)} closed-access papers** still missing from the libraries "
                f"(deduped against current .ris/sidecars). Open each `https://doi.org/...` "
                f"link via institutional access / ILLIAD and drop the PDF into the requesting "
                f"project's lib_dir, then re-run `index_portfolio.py`.\n\n")
        f.write(f"Ranking score = `n_seeds_pointing*10 + cited_by` "
                f"(papers many of your seed PDFs cite rank highest). "
                f"{len(ranked_signal)} have a DB signal; the rest are unranked residuals.\n\n")
        f.write(f"## Priority A — top {min(args.top_a, len(rows))} (work these first)\n\n")
        for i, r in enumerate(rows[:args.top_a], 1):
            f.write(f"{i}. [{r['doi']}](https://doi.org/{r['doi']}) "
                    f"— {r['title'] or '(title pending)'} ({r['year'] or 'n.d.'}) "
                    f"— score {r['score']} (seeds {r['seeds']}, cites {r['cites']}) "
                    f"— _{r['projs']}_\n")
        f.write(f"\n## Priority B — remaining {max(0, len(rows)-args.top_a)} "
                f"(see the .csv for the full ranked list)\n\n")
        f.write(f"Full machine-readable list: `{os.path.basename(csv_p)}`\n")

    print(f"wrote {md_p}")
    print(f"wrote {csv_p}")
    print(f"Priority A: {min(args.top_a, len(rows))}   total: {len(rows)}   with DB signal: {len(ranked_signal)}")


if __name__ == "__main__":
    main()
