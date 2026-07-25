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
import os, sys, time, argparse, datetime
import urllib.parse
import requests

import lit_util
lit_util.utf8_stdout()

EMAIL = os.environ.get("LITPIPE_EMAIL", lit_util.DEFAULT_EMAIL)
UA    = f"GETPAID-recs/1.0 (mailto:{EMAIL})"
S2    = "https://api.semanticscholar.org/recommendations/v1"
DB_PATH = os.path.expanduser("~/Projects/_references/portfolio.duckdb")


class S2Error(Exception):
    """Network/HTTP failure talking to Semantic Scholar (distinct from a genuine no-result)."""


def s2_get(url, headers=None, timeout=15, retries=3):
    """GET with 429 backoff.

    Returns parsed JSON on success. Raises S2Error on a network/transport failure
    or a non-200/429 HTTP status (RC6) so the caller can distinguish an outage
    from a genuine 'no recommendations' result.
    """
    h = {"User-Agent": UA, **(headers or {})}
    last = None
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=h, timeout=timeout)
        except requests.exceptions.RequestException as e:
            last = e
            if attempt == retries - 1:
                raise S2Error(str(e))
            time.sleep(3)
            continue
        if r.status_code == 200:
            try:
                return r.json()
            except ValueError as e:
                raise S2Error(f"bad JSON: {e}")
        if r.status_code == 429:
            time.sleep(5 * (attempt + 1)); continue
        raise S2Error(f"HTTP {r.status_code}")
    raise S2Error(f"exhausted {retries} retries (last: {last})")


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

    con = lit_util.connect_db(args.db, on_fail="exit", tries=5, delays=(3,))  # c9: shared RC10 open

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
    n_err = 0
    new_rec_rows = []
    new_meta_rows = []

    for i, seed_doi in enumerate(seeds, 1):
        # Use the recommendations endpoint with a DOI directly (S2 supports this).
        # RC12: URL-encode the DOI value (keep the literal `DOI:` ID-scheme prefix)
        # so DOIs with ?#<> etc. don't corrupt the path and silently miss.
        seed_q = urllib.parse.quote(seed_doi, safe="")
        url = f"{S2}/papers/forpaper/DOI:{seed_q}?fields=title,year,authors,externalIds,citationCount&limit={args.top_n}"
        try:
            data = s2_get(url, headers=headers)
        except S2Error as e:
            # RC6: a network/HTTP failure is NOT a genuine 'no recs' - tally separately
            # so an S2 outage doesn't masquerade as 'no recommendations'.
            n_err += 1
            print(f"  [{i:>3}/{len(seeds)}] {seed_doi[:50]:<50}  net_err ({e})", file=sys.stderr)
            time.sleep(args.sleep)
            continue
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

    # C3: wrap the recommendations DELETE-then-INSERT (+ new metadata) in ONE transaction on the
    # connection -- write through `con`, not a child cursor, so the BEGIN enrolls the writes (the
    # same footgun as C2). A crash between the DELETE and the INSERT otherwise loses that seed's recs.
    cur = con
    con.execute("BEGIN TRANSACTION")
    try:
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

        # Add metadata for new DOIs (don't overwrite richer existing rows). ON CONFLICT DO NOTHING
        # is belt-and-suspenders: the SELECT-then-filter already prevents the PK violation, but the
        # clause makes a concurrent-writer race harmless for free.
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
                    ON CONFLICT (doi) DO NOTHING
                """, new_only)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

    n_recs   = con.execute("SELECT COUNT(*) FROM recommendations").fetchone()[0]
    n_unique = con.execute("SELECT COUNT(DISTINCT recommended_doi) FROM recommendations").fetchone()[0]
    n_novel  = con.execute(
        "SELECT COUNT(DISTINCT r.recommended_doi) FROM recommendations r "
        "WHERE NOT EXISTS (SELECT 1 FROM paper_locations l WHERE l.doi = r.recommended_doi)"
    ).fetchone()[0]

    print(f"\n=== summary ===")
    print(f"  seeds with recs:     {n_resolved}/{len(seeds)}")
    print(f"  net errors:          {n_err}")
    print(f"  total rec rows:      {n_recs}")
    print(f"  unique rec DOIs:     {n_unique}")
    print(f"  novel (not in lib):  {n_novel}")
    con.close()


if __name__ == "__main__":
    main()
