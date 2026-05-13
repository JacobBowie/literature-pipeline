"""Enrich the index with Semantic Scholar /paper/{id}/recommendations.

For every paper we have in `paper_locations`, query S2's recommendations
endpoint to get semantically-similar papers (by S2's internal embeddings).
Adds rows to the `recommendations` table — a third candidate signal alongside
forward + reverse citations.

Why this is useful:
  - S2 recommendations capture topical neighbors that aren't directly cited
  - Often surfaces highly-relevant papers our citation walker missed
  - Free signal, paper-qa2 uses the same approach internally

Rate limit: S2 unauthenticated ~1 req/s. With ~370 papers, ~6-7 min.
Idempotent — refreshes existing rows.

Usage:
  python enrich_recommendations.py
  python enrich_recommendations.py --limit 10  # testing
  python enrich_recommendations.py --s2-key XXX  # faster
"""
import os, sys, io, time, argparse, datetime
import duckdb, requests

try:
    if getattr(sys.stdout, "encoding", "").lower() != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, OSError):
    pass

EMAIL = os.environ.get("LITPIPE_EMAIL", "jacob.bowie2@gmail.com")
UA    = f"GETPAID-recs/1.0 (mailto:{EMAIL})"
S2    = "https://api.semanticscholar.org/recommendations/v1"
S2_GRAPH = "https://api.semanticscholar.org/graph/v1"
DB_PATH = os.path.expanduser(r"~\Projects\_references\portfolio.duckdb")


def s2_get(url, headers=None, timeout=15, retries=3):
    """GET with 429 backoff."""
    h = {"User-Agent": UA, **(headers or {})}
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=h, timeout=timeout)
            if r.status_code == 200: return r.json()
            if r.status_code == 429:
                time.sleep(5 * (attempt + 1)); continue
            return None
        except Exception:
            if attempt == retries - 1: return None
            time.sleep(3)
    return None


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    ap.add_argument("--db",     default=DB_PATH,
                    help=f"DuckDB portfolio index path (default: {DB_PATH}).")
    ap.add_argument("--limit",  type=int, default=0,
                    help="Process first N seeds only (testing).")
    ap.add_argument("--sleep",  type=float, default=1.1,
                    help="Seconds between S2 calls.")
    ap.add_argument("--s2-key", default=None,
                    help="Semantic Scholar API key (env: S2_API_KEY).")
    ap.add_argument("--top-n",  type=int, default=20,
                    help="Recommendations per seed paper (max 100).")
    args = ap.parse_args()

    s2_key = args.s2_key or os.environ.get("S2_API_KEY", "")
    headers = {"x-api-key": s2_key} if s2_key else {}

    con = duckdb.connect(args.db)

    # Seeds = papers we have anywhere in the portfolio
    seeds = [r[0] for r in con.execute(
        "SELECT DISTINCT doi FROM paper_locations ORDER BY doi"
    ).fetchall()]
    if args.limit: seeds = seeds[:args.limit]

    print(f"DB: {args.db}")
    print(f"Seed papers: {len(seeds)}")
    print(f"Per-seed limit: {args.top_n}")
    print(f"Sleep: {args.sleep}s\n")

    now = datetime.datetime.now().isoformat(timespec="seconds")
    n_resolved = 0
    new_rec_rows = []
    new_meta_rows = []

    for i, seed_doi in enumerate(seeds, 1):
        # Use the recommendations endpoint with a DOI directly (S2 supports this).
        url = f"{S2}/papers/forpaper/DOI:{seed_doi}?fields=title,year,authors,externalIds,citationCount&limit={args.top_n}"
        data = s2_get(url, headers=headers)
        time.sleep(args.sleep)
        if not data or "recommendedPapers" not in data:
            print(f"  [{i:>3}/{len(seeds)}] {seed_doi[:50]:<50}  no_recs")
            continue
        recs = data.get("recommendedPapers", []) or []
        n_resolved += 1
        for rank, p in enumerate(recs, 1):
            ext = p.get("externalIds") or {}
            rec_doi = (ext.get("DOI") or "").strip().lower()
            if not rec_doi: continue
            new_rec_rows.append((seed_doi, rec_doi, rank, now))
            authors = "; ".join(a.get("name","") for a in (p.get("authors") or []))
            new_meta_rows.append((
                rec_doi, p.get("year"), "", p.get("title") or "",
                "", authors, now,
            ))
        print(f"  [{i:>3}/{len(seeds)}] {seed_doi[:50]:<50}  {len(recs):>3} recs")

    # Insert recommendations (delete-then-insert by seed)
    cur = con.cursor()
    if new_rec_rows:
        seeds_done = list({r[0] for r in new_rec_rows})
        cur.executemany("DELETE FROM recommendations WHERE seed_doi = ?",
                        [(d,) for d in seeds_done])
        cur.executemany("""
            INSERT INTO recommendations (seed_doi, recommended_doi, rank, refreshed_at)
            VALUES (?,?,?,?)
            ON CONFLICT (seed_doi, recommended_doi) DO UPDATE SET
              rank = excluded.rank, refreshed_at = excluded.refreshed_at
        """, new_rec_rows)

    # Add metadata for new DOIs (don't overwrite richer existing rows)
    if new_meta_rows:
        seen = set(); dedup = []
        for r in new_meta_rows:
            if r[0] not in seen: seen.add(r[0]); dedup.append(r)
        existing = {row[0] for row in cur.execute(
            f"SELECT doi FROM paper_metadata WHERE doi IN ({','.join('?'*len(dedup))})",
            [r[0] for r in dedup]).fetchall()}
        new_only = [r for r in dedup if r[0] not in existing]
        if new_only:
            cur.executemany("""
                INSERT INTO paper_metadata (doi, year, lastname, title, venue, authors, refreshed_at)
                VALUES (?,?,?,?,?,?,?)
            """, new_only)

    n_recs   = con.execute("SELECT COUNT(*) FROM recommendations").fetchone()[0]
    n_unique = con.execute("SELECT COUNT(DISTINCT recommended_doi) FROM recommendations").fetchone()[0]
    n_novel  = con.execute(
        "SELECT COUNT(DISTINCT r.recommended_doi) FROM recommendations r "
        "WHERE NOT EXISTS (SELECT 1 FROM paper_locations l WHERE l.doi = r.recommended_doi)"
    ).fetchone()[0]

    print(f"\n=== summary ===")
    print(f"  seeds with recs:     {n_resolved}/{len(seeds)}")
    print(f"  total rec rows:      {n_recs}")
    print(f"  unique rec DOIs:     {n_unique}")
    print(f"  novel (not in lib):  {n_novel}")
    con.close()


if __name__ == "__main__":
    main()
