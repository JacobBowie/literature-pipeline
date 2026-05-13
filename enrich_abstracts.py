"""Enrich paper_metadata.abstract via CrossRef /works/{doi}.

CrossRef returns a JATS-encoded `abstract` field on most modern journal articles.
We strip the markup and store plain text. Used by the embedding-similarity layer
(Stage A in ROADMAP.md) — embeddings on titles+abstracts are noticeably better
than titles alone.

Polite-pool rate: ~50 req/s with mailto User-Agent. We use ~3 req/s to be conservative.
For ~17k DOIs that's ~95 min. Idempotent — only fetches DOIs without an abstract.

Usage:
  python enrich_abstracts.py                        # all DOIs missing abstract
  python enrich_abstracts.py --limit 100            # first 100 only (testing)
  python enrich_abstracts.py --only-papers          # restrict to PDFs we have, not candidates
  python enrich_abstracts.py --sleep 0.3            # speed knob
"""
import os, sys, io, re, time, argparse
import duckdb, requests

try:
    if getattr(sys.stdout, "encoding", "").lower() != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, OSError):
    pass

EMAIL = os.environ.get("LITPIPE_EMAIL", "jacob.bowie2@gmail.com")
UA    = f"GETPAID-abstract-enrich/1.0 (mailto:{EMAIL})"
DB_PATH = os.path.expanduser(r"~\Projects\_references\portfolio.duckdb")
CROSSREF = "https://api.crossref.org/works/{doi}"


def crossref_abstract(doi: str, timeout=15) -> str:
    """Return plain-text abstract or '' if not available."""
    try:
        r = requests.get(CROSSREF.format(doi=doi),
                         headers={"User-Agent": UA}, timeout=timeout)
        if r.status_code != 200: return ""
        msg = r.json().get("message", {})
        raw = msg.get("abstract") or ""
        if not raw: return ""
        # Strip JATS-style XML tags + decode common entities
        text = re.sub(r"<[^>]+>", " ", raw)
        text = re.sub(r"&lt;", "<", text)
        text = re.sub(r"&gt;", ">", text)
        text = re.sub(r"&amp;", "&", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text
    except Exception:
        return ""


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    ap.add_argument("--db",     default=DB_PATH,
                    help=f"DuckDB portfolio index path (default: {DB_PATH}).")
    ap.add_argument("--limit",  type=int, default=0,
                    help="Process first N DOIs only (testing).")
    ap.add_argument("--sleep",  type=float, default=0.3,
                    help="Seconds between CrossRef calls.")
    ap.add_argument("--only-papers", action="store_true",
                    help="Only enrich DOIs we have PDFs for (skip candidates).")
    args = ap.parse_args()

    con = duckdb.connect(args.db)

    # Pick DOIs missing abstract
    where_extra = ""
    if args.only_papers:
        where_extra = " AND EXISTS (SELECT 1 FROM paper_locations l WHERE l.doi = m.doi)"
    q = f"""
        SELECT doi FROM paper_metadata m
        WHERE (abstract IS NULL OR abstract = '') {where_extra}
        ORDER BY doi
    """
    if args.limit: q += f" LIMIT {int(args.limit)}"
    targets = [r[0] for r in con.execute(q).fetchall()]

    print(f"DB: {args.db}")
    print(f"Targets: {len(targets)} DOIs missing abstract")
    print(f"Sleep:   {args.sleep}s between calls")
    print(f"ETA:     ~{(len(targets)*args.sleep)/60:.1f} min\n")

    n_hit = n_miss = 0
    for i, doi in enumerate(targets, 1):
        abs_text = crossref_abstract(doi)
        if abs_text:
            con.execute("UPDATE paper_metadata SET abstract = ? WHERE doi = ?",
                        [abs_text, doi])
            n_hit += 1
            tag = "OK"
        else:
            n_miss += 1
            tag = "--"
        if i % 50 == 0 or i <= 10 or i == len(targets):
            print(f"  [{i:>5}/{len(targets)}] {tag}  {doi}  hits={n_hit} misses={n_miss}")
        time.sleep(args.sleep)

    print(f"\n=== summary ===")
    print(f"  attempted:  {len(targets)}")
    print(f"  abstracts:  {n_hit} ({100*n_hit/max(1,len(targets)):.0f}%)")
    print(f"  misses:     {n_miss}")
    con.close()


if __name__ == "__main__":
    main()
