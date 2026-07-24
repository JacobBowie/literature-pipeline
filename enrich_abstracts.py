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
import os, sys, re, time, argparse
import urllib.parse
import duckdb, requests

import lit_util
import lit_net  # B1: shared GET with 429/5xx retry
lit_util.utf8_stdout()

EMAIL = os.environ.get("LITPIPE_EMAIL", "JacobBowie@users.noreply.github.com")
UA    = f"GETPAID-abstract-enrich/1.0 (mailto:{EMAIL})"
DB_PATH = os.path.expanduser("~/Projects/_references/portfolio.duckdb")
CROSSREF = "https://api.crossref.org/works/{doi}"


def connect_db(db_path, retries=5, delay=3.0):
    """RC10: open the (Drive-synced) DuckDB with a short retry on lock errors.

    portfolio.duckdb often lives under Google Drive, which holds the file open
    and surfaces as a DuckDB lock/IO error. Retry briefly with a clear message
    instead of crashing on a transient sync hold.
    """
    last = None
    for attempt in range(1, retries + 1):
        try:
            return duckdb.connect(db_path)
        except (duckdb.IOException, duckdb.Error) as e:
            last = e
            print(f"  DB locked (attempt {attempt}/{retries}) - suspect Google Drive "
                  f"holding {db_path} open; retrying in {delay:.0f}s...", file=sys.stderr)
            if attempt < retries:
                time.sleep(delay)
    raise SystemExit(
        f"ERROR: could not open {db_path} after {retries} attempts - DB locked "
        f"(suspect Google Drive sync holding it open; pause Drive and retry). Last error: {last}"
    )


class CrossRefError(Exception):
    """Network/HTTP failure talking to CrossRef (distinct from a genuine no-abstract result).

    `status` carries the HTTP code (or None for a transport error) so the caller can tell a
    PERMANENT miss (4xx other than 429 -- e.g. a 404 on an arXiv/DataCite DOI CrossRef never
    had) from a TRANSIENT outage (429/5xx/network) that should be retried next run."""
    def __init__(self, msg, status=None):
        super().__init__(msg)
        self.status = status


def crossref_abstract(doi: str, timeout=15) -> str:
    """Return plain-text abstract, or '' if the work genuinely has no abstract.

    Raises CrossRefError on a network/transport failure or a non-200 HTTP status,
    so the caller can distinguish an outage from a real 'no abstract' (RC6).
    """
    # RC12: URL-encode the DOI for the path segment so DOIs with ?#<> etc. don't
    # silently miss (raw interpolation would corrupt the URL).
    url = CROSSREF.format(doi=urllib.parse.quote(doi, safe=""))
    try:
        r = lit_net.get(url, headers={"User-Agent": UA}, timeout=timeout)
    except requests.exceptions.RequestException as e:
        raise CrossRefError(str(e))
    if r.status_code != 200:
        raise CrossRefError(f"HTTP {r.status_code}", status=r.status_code)
    try:
        msg = r.json().get("message", {})
    except ValueError as e:
        raise CrossRefError(f"bad JSON: {e}")
    raw = msg.get("abstract") or ""
    if not raw: return ""
    # Strip JATS-style XML tags + decode common entities
    text = re.sub(r"<[^>]+>", " ", raw)
    text = re.sub(r"&lt;", "<", text)
    text = re.sub(r"&gt;", ">", text)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


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
    ap.add_argument("--retry-after-days", type=int, default=30,
                    help="Re-attempt a previously-failed DOI only after N days (0 = always). "
                         "Stops re-querying the ~47k permanent CrossRef-fails every run.")
    args = ap.parse_args()

    con = connect_db(args.db)
    # Self-healing migration: the attempt-state column may not exist on an older DB.
    # (A fresh --rebuild creates it from index_portfolio's schema.)
    con.execute("ALTER TABLE paper_metadata ADD COLUMN IF NOT EXISTS abstract_attempted_at TIMESTAMP")

    # Pick DOIs missing abstract
    where_extra = ""
    if args.only_papers:
        where_extra = " AND EXISTS (SELECT 1 FROM paper_locations l WHERE l.doi = m.doi)"
    # Attempt-state: skip DOIs we already tried recently (permanent CrossRef-fails --
    # closed publishers, arXiv/DataCite -- otherwise re-queried every run indefinitely).
    where_attempt = ""
    if args.retry_after_days > 0:
        where_attempt = (f" AND (abstract_attempted_at IS NULL "
                         f"OR abstract_attempted_at < now() - INTERVAL '{int(args.retry_after_days)} days')")
    q = f"""
        SELECT doi FROM paper_metadata m
        WHERE (abstract IS NULL OR abstract = '') {where_extra} {where_attempt}
        ORDER BY doi
    """
    if args.limit: q += f" LIMIT {int(args.limit)}"
    targets = [r[0] for r in con.execute(q).fetchall()]

    print(f"DB: {args.db}")
    print(f"Targets: {len(targets)} DOIs missing abstract")
    print(f"Sleep:   {args.sleep}s between calls")
    print(f"ETA:     ~{(len(targets)*args.sleep)/60:.1f} min\n")

    n_hit = n_miss = n_err = 0
    for i, doi in enumerate(targets, 1):
        try:
            abs_text = crossref_abstract(doi)
        except CrossRefError as e:
            # RC6: a network/HTTP failure is NOT a genuine 'no abstract'. But a PERMANENT
            # 4xx (e.g. 404 on an arXiv/DataCite DOI CrossRef never had) will never succeed
            # via CrossRef, so mark it attempted -- otherwise it is re-queried every run. A
            # transient 429/5xx/network error stays unmarked so the next run retries it.
            # Only codes that mean "CrossRef has no such work" are permanent. 403 (WAF/CDN
            # bot-block), 408, 425, 429, and all 5xx/network are transient -> stay unmarked
            # so the next run retries (a transient 4xx storm must not suppress real papers).
            permanent = e.status in (400, 404)
            if permanent:
                con.execute("UPDATE paper_metadata SET abstract_attempted_at = now() WHERE doi = ?", [doi])
            n_err += 1
            tag = "ER!" if permanent else "ER"
            print(f"  [{i:>5}/{len(targets)}] {tag}  {doi}  ({e})", file=sys.stderr)
            time.sleep(args.sleep)
            continue
        if abs_text:
            con.execute("UPDATE paper_metadata SET abstract = ?, abstract_attempted_at = now() WHERE doi = ?",
                        [abs_text, doi])
            n_hit += 1
            tag = "OK"
        else:
            # genuine CrossRef 200 with no abstract (closed publisher): mark attempted so this
            # permanent-fail is not re-queried until --retry-after-days elapses.
            con.execute("UPDATE paper_metadata SET abstract_attempted_at = now() WHERE doi = ?", [doi])
            n_miss += 1
            tag = "--"
        if i % 50 == 0 or i <= 10 or i == len(targets):
            print(f"  [{i:>5}/{len(targets)}] {tag}  {doi}  hits={n_hit} misses={n_miss} errs={n_err}")
        time.sleep(args.sleep)

    print(f"\n=== summary ===")
    print(f"  attempted:  {len(targets)}")
    print(f"  abstracts:  {n_hit} ({100*n_hit/max(1,len(targets)):.0f}%)")
    print(f"  no-abstract:{n_miss}")
    print(f"  net errors: {n_err}")
    con.close()


if __name__ == "__main__":
    main()
