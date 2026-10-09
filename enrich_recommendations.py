"""Recent-feed recommendations from Semantic Scholar.
(Maintainer refs: DEC-18, K7, T1; dispatch W3-E.)

What the feed is: /recommendations/v1/papers/forpaper/{DOI} with the default `recent` pool returns
papers published in about the last 60 days that resemble a seed (probe P8-C1, C2). It is a "new
papers near my library" feed, not the older topical neighbours the citation walks find, and it
never counts toward convergence or top_candidates.

OFF BY DEFAULT (DEC-18: a monthly recent feed, after the S2 key arrives):
  no --recent-feed                   prints one line, sends nothing, exits 0 (run_daily and
                                     snowball --with-recs call this script bare until they pass
                                     the flag);
  --recent-feed without S2_API_KEY   prints one line, sends nothing, exits 0, unless
                                     --allow-unkeyed is given too (unkeyed spacing is 6.5 s).

With --recent-feed the seeds are the library DOIs (paper_locations; --project KEY: that project's
only). Every request goes through litpipe.s2 with one Session per run (spacing, retries, the run
budget, the circuit breaker, the request ledger, the key only in the x-api-key header). Per seed:
  OK         the list, possibly empty, is recorded;
  NO_MATCH   a 404: S2 does not hold the seed ("not found", not a network error);
  failure    a 429, 5xx, transport error, deferral or malformed answer is typed and counted. The
             seed's earlier rows stay as they are, and it is asked again on the next run. A
             failed list is never read as an empty one.

Writes: set-based (a registered DataFrame, then UPDATE ... FROM and INSERT ... SELECT ... WHERE NOT
EXISTS; never executemany, which took hours on the 230k-row index), one transaction per
--commit-every seeds (default 200). A hard kill loses at most the batch in flight; a rerun resumes
from the attempt ledger.
  recent_feed (new, append-only)  (seed_doi, recommended_doi, pool) with rank, first_seen_at and
                                  last_seen_at. A pair seen again updates rank and last_seen_at; a
                                  new pair is inserted; nothing is ever deleted (K7).
  rec_attempts (new)              one row per seed: outcome (a litpipe.outcomes Kind name), status,
                                  attempted_at. A seed answered (OK, NO_MATCH, SKIPPED) within
                                  --retry-after-days (default 21, under the monthly cadence and the
                                  60-day window) is not asked again unless --refresh.
  recommendations (index table)   still written, as a mirror of the recent pool for its readers
                                  (index_portfolio's metadata GC among them): upsert of rank and
                                  refreshed_at on (seed_doi, recommended_doi). No DELETE: the old
                                  per-seed DELETE is gone.
  paper_metadata                  new rows only, for recommended DOIs it lacks (lower-case DOI
                                  join; ON CONFLICT DO NOTHING). An existing row, and every
                                  abstract, is never touched. The abstract count is checked inside
                                  each transaction; a drop rolls the batch back.

Exit codes (shared convention): 0 clean, and both "off" gates; 1 config error (an unregistered
--project, no registry for it, no DB, a bad s2 block or flag value); 2 degraded (more than 5 % of
the seeds asked failed); 3 aborted (the run budget spent, the breaker tripped, or a batch write
rolled back); 130 after Ctrl-C (the seeds already answered are committed first). A run that got as
far as asking S2 prints "[step-summary] {json}" as its last line.

Usage:
  python enrich_recommendations.py                                  # off: one line, exit 0
  python enrich_recommendations.py --recent-feed                    # needs S2_API_KEY
  python enrich_recommendations.py --recent-feed --project <key>    # one project's seeds
  python enrich_recommendations.py --recent-feed --allow-unkeyed --limit 10   # testing
"""
import argparse
import contextlib
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import lit_util
from litpipe import config, ledger, s2
from litpipe import doi as _doi
from litpipe import enrich_s2 as _db
from litpipe import text as _text

lit_util.utf8_stdout()

CONFIG_PATH = Path(__file__).parent / "projects.json"
POOL = "recent"
FIELDS = ("externalIds", "title", "year", "authors")
TOP_N = 20
COMMIT_EVERY = 200
RETRY_AFTER_DAYS = 21
FAIL_THRESHOLD = 0.05                 # more than 5 % of the seeds asked failed: degraded (exit 2)
ANSWERED = frozenset({"OK", "NO_MATCH", "SKIPPED"})
TRANSPORT_KINDS = _db.TRANSPORT_KINDS
SUMMARY_MARKER = _db.SUMMARY_MARKER
EXIT_OK, EXIT_CONFIG, EXIT_DEGRADED, EXIT_ABORTED, EXIT_INTERRUPTED = 0, 1, 2, 3, 130
REQUIRED_TABLES = ("paper_locations", "paper_metadata", "recommendations")

OFF_LINE = ("[enrich_recommendations] off: recommendations run only with --recent-feed (a "
            "monthly recent feed once an S2 key is set); nothing sent")
NO_KEY_LINE = ("[enrich_recommendations] --recent-feed needs S2_API_KEY (key: absent); nothing sent. "
               "Pass --allow-unkeyed to run unkeyed at 6.5 s per request")

FEED_DDL = """CREATE TABLE IF NOT EXISTS recent_feed (
  seed_doi         VARCHAR,
  recommended_doi  VARCHAR,       -- litpipe.doi.normalise form (lower case)
  pool             VARCHAR,       -- S2 pool: 'recent' (the 60-day feed)
  rank             INTEGER,       -- position in S2's list the last time it was seen
  first_seen_at    TIMESTAMPTZ,
  last_seen_at     TIMESTAMPTZ,
  PRIMARY KEY (seed_doi, recommended_doi, pool)
)"""
ATTEMPTS_DDL = """CREATE TABLE IF NOT EXISTS rec_attempts (
  seed_doi      VARCHAR PRIMARY KEY,
  outcome       VARCHAR,          -- litpipe.outcomes Kind name of the last attempt
  status        VARCHAR,          -- HTTP status of the last attempt ('' when none)
  attempted_at  TIMESTAMPTZ
)"""


# ------------------------------------------------------------------------------ key
@contextlib.contextmanager
def _key_env(value):
    """Deprecated --s2-key: copied into S2_API_KEY before the Session is built (litpipe.s2 reads
    nothing else, and ledger.redact scrubs that env value), restored afterwards, never printed."""
    if not value:
        yield
        return
    print("[deprecated] --s2-key: set the S2_API_KEY environment variable instead (a key on the "
          "command line lands in shell history); using it for this run only")
    old = os.environ.get(s2.KEY_ENV)
    os.environ[s2.KEY_ENV] = value
    try:
        yield
    finally:
        if old is None:
            os.environ.pop(s2.KEY_ENV, None)
        else:
            os.environ[s2.KEY_ENV] = old


# ------------------------------------------------------------------------------ one seed
def parse_recs(seed, papers):
    """(feed rows, metadata rows) from one seed's recommendedPapers list. rank is the position in
    S2's list (1-based, DOI-less entries included, as before); a DOI is kept once per seed."""
    feed, meta, seen = [], [], set()
    self_doi = _doi.normalise_structured(seed)
    for rank, p in enumerate(papers, 1):
        if not isinstance(p, dict):
            continue
        ext = p.get("externalIds") if isinstance(p.get("externalIds"), dict) else {}
        d = _doi.normalise_structured(ext.get("DOI")) if ext.get("DOI") else None
        if not d or d in seen or d == self_doi:
            continue
        seen.add(d)
        feed.append((seed, d, POOL, rank))
        names = [_text.display_field(a.get("name")) for a in (p.get("authors") or [])
                 if isinstance(a, dict) and a.get("name")]
        year = p.get("year") if isinstance(p.get("year"), int) and not isinstance(p.get("year"), bool) else None
        meta.append((d, year, _text.display_field(p.get("title")), "; ".join(n for n in names if n)))
    return feed, meta


def ask(seed, top_n, session):
    """One seed: (outcome name, status, feed rows, metadata rows, detail, attempts)."""
    try:
        out = s2.recommend_for_paper(seed, pool=POOL, limit=top_n, fields=FIELDS, session=session)
    except ValueError as e:                              # not a DOI: never sent
        return "SKIPPED", "", [], [], ledger.redact(str(e))[:200], 0
    status = str(out.status or "")
    if out.ok:
        feed, meta = parse_recs(seed, out.payload)
        return "OK", status, feed, meta, f"{len(out.payload)} recommended", out.attempts
    return str(out.kind), status, [], [], ledger.redact(out.detail or "")[:200], out.attempts


# ------------------------------------------------------------------------------ writes
def write_batch(con, feed, meta, attempts, now) -> dict:
    """One batch in one transaction (enrich_s2.write_transaction: the abstract guard, rollback on
    any failure). `feed` (seed, rec, pool, rank) is unique on its key, `meta` (doi, year, title,
    authors) on doi, `attempts` (seed, outcome, status) on seed."""
    import pandas as pd
    ts = _db.utc_iso(now)
    local = now.astimezone().replace(tzinfo=None).isoformat(timespec="seconds")   # TIMESTAMP columns: local, as index_portfolio
    ff = pd.DataFrame(feed, columns=["seed_doi", "recommended_doi", "pool", "rank"])
    ff["rank"] = ff["rank"].astype("Int64")
    ff["ts"], ff["local_ts"] = ts, local
    mf = pd.DataFrame(meta, columns=["doi", "year", "title", "authors"])
    mf["year"] = mf["year"].astype("Int64")
    mf["local_ts"] = local
    af = pd.DataFrame(attempts, columns=["seed_doi", "outcome", "status"])
    af["ts"] = ts
    got = {"new_pairs": 0, "seen_again": 0, "recommendations_upserted": 0, "metadata_inserted": 0}
    with _db.write_transaction(con):
        if len(ff):
            with _db.staged(con, "_w3e_feed", ff):
                upd, ins = _db.upsert(
                    con, "recent_feed", "_w3e_feed", ["seed_doi", "recommended_doi", "pool"],
                    {"rank": "s.rank", "last_seen_at": "CAST(s.ts AS TIMESTAMPTZ)"},
                    {"seed_doi": "s.seed_doi", "recommended_doi": "s.recommended_doi", "pool": "s.pool",
                     "rank": "s.rank", "first_seen_at": "CAST(s.ts AS TIMESTAMPTZ)",
                     "last_seen_at": "CAST(s.ts AS TIMESTAMPTZ)"})
                got["seen_again"], got["new_pairs"] = upd, ins
                r_upd, r_ins = _db.upsert(
                    con, "recommendations", "_w3e_feed", ["seed_doi", "recommended_doi"],
                    {"rank": "s.rank", "refreshed_at": "CAST(s.local_ts AS TIMESTAMP)"},
                    {"seed_doi": "s.seed_doi", "recommended_doi": "s.recommended_doi", "rank": "s.rank",
                     "refreshed_at": "CAST(s.local_ts AS TIMESTAMP)"})
                got["recommendations_upserted"] = r_upd + r_ins
        if len(mf):
            with _db.staged(con, "_w3e_meta", mf):
                n = con.execute(
                    "INSERT INTO paper_metadata (doi, year, lastname, title, venue, authors, refreshed_at) "
                    "SELECT s.doi, s.year, '', s.title, '', s.authors, CAST(s.local_ts AS TIMESTAMP) "
                    "FROM _w3e_meta AS s "
                    "WHERE NOT EXISTS (SELECT 1 FROM paper_metadata AS m WHERE lower(m.doi) = s.doi) "
                    "ON CONFLICT (doi) DO NOTHING").fetchone()[0]
                got["metadata_inserted"] = int(n or 0)
        if len(af):
            with _db.staged(con, "_w3e_att", af):
                _db.upsert(con, "rec_attempts", "_w3e_att", ["seed_doi"],
                           {"outcome": "s.outcome", "status": "s.status",
                            "attempted_at": "CAST(s.ts AS TIMESTAMPTZ)"},
                           {"seed_doi": "s.seed_doi", "outcome": "s.outcome", "status": "s.status",
                            "attempted_at": "CAST(s.ts AS TIMESTAMPTZ)"})
    return got


class _Batch:
    def __init__(self):
        self.feed, self.meta, self.attempts = [], {}, []

    def add(self, seed, outcome, status, feed, meta):
        self.feed.extend(feed)
        for row in meta:
            self.meta.setdefault(row[0], row)
        self.attempts.append((seed, outcome, status))

    def __len__(self):
        return len(self.attempts)


# ------------------------------------------------------------------------------ the run
def _error(msg) -> dict:
    msg = ledger.redact(msg)
    print(f"[ERR] {msg}", file=sys.stderr)
    return {"step": "enrich_recommendations", "exit_code": EXIT_CONFIG, "status": "error", "error": msg}


def run(*, db=None, limit=0, sleep=None, s2_key=None, top_n=TOP_N, project=None, recent_feed=False,
        allow_unkeyed=False, refresh=False, commit_every=COMMIT_EVERY, retry_after_days=RETRY_AFTER_DAYS,
        session=None, cfg=None, now=None) -> dict:
    """The recommendation pass. Returns the summary dict; its `exit_code` is the CLI's exit code.
    Without `recent_feed` (DEC-18) it prints one line and sends nothing."""
    if not recent_feed:
        print(OFF_LINE)
        return {"step": "enrich_recommendations", "exit_code": EXIT_OK, "status": "off", "sent": 0}
    with _key_env(s2_key):
        if not s2.key_present() and not allow_unkeyed:
            print(NO_KEY_LINE)
            return {"step": "enrich_recommendations", "exit_code": EXIT_OK, "status": "no_key", "sent": 0}
        if sleep is not None:
            print("[deprecated] --sleep is ignored: spacing comes from litpipe.s2 (6.5 s unkeyed, "
                  "1.1 s keyed; projects.json \"s2\" block)")
        return _run(db, limit, top_n, project, refresh, commit_every, retry_after_days, session, cfg, now)


def _run(db, limit, top_n, project, refresh, commit_every, retry_after_days, session, cfg, now) -> dict:
    try:
        if not 1 <= int(top_n) <= 500:
            raise config.ConfigError(f"--top-n must be in 1..500 (S2's maximum), got {top_n}")
        if int(commit_every) < 1 or int(retry_after_days) < 0:
            raise config.ConfigError("--commit-every must be >= 1 and --retry-after-days >= 0")
        reg, path = _db.resolve(project, db, cfg, CONFIG_PATH)
        sess = session or s2.Session(cfg=reg)
    except config.ConfigError as e:
        return _error(str(e))
    clock = (lambda: now) if now is not None else (lambda: datetime.now(timezone.utc))
    con = lit_util.connect_db(str(path), on_fail="exit", tries=5, delays=(3,))  # c9: shared RC10 open
    try:
        miss = _db.missing_tables(con, REQUIRED_TABLES)
        if miss:
            return _error(f"{path} is not a portfolio index (missing {', '.join(miss)})")
        con.execute(FEED_DDL)
        con.execute(ATTEMPTS_DDL)
        q, args = "SELECT DISTINCT doi FROM paper_locations", []
        if project is not None:
            q, args = q + " WHERE project = ?", [project]
        seeds = [r[0] for r in con.execute(q + " ORDER BY doi", args).fetchall()]
        recent = set()
        if not refresh:
            cutoff = _db.utc_iso(clock() - timedelta(days=int(retry_after_days)))
            recent = {r[0] for r in con.execute(
                "SELECT seed_doi FROM rec_attempts WHERE outcome IN ('OK', 'NO_MATCH', 'SKIPPED') "
                "AND attempted_at >= CAST(? AS TIMESTAMPTZ)", [cutoff]).fetchall()}
        todo = [d for d in seeds if d not in recent]
        if limit:
            todo = todo[:int(limit)]
        abstracts_before = _db.abstract_count(con)

        print(f"DB:            {path}")
        print(f"scope:         {('project ' + project) if project else 'every project'}")
        n_recent = sum(1 for d in seeds if d in recent)
        print(f"seeds:         {len(seeds)} library DOIs; {n_recent} answered within {retry_after_days} d"
              + (" (ignored: --refresh)" if refresh else "") + f"; asking {len(todo)}")
        print(f"pool:          {POOL} (about the last 60 days), {top_n} per seed")
        print(f"{s2.key_status()}  run budget={sess.budget} attempts  breaker={sess.breaker}  "
              f"commit every {commit_every} seeds\n")

        n = {"ok": 0, "not_found": 0, "skipped": 0, "failed": 0, "not_sent": 0, "transport": 0,
             "rec_rows": 0, "batches": 0, "new_pairs": 0, "seen_again": 0, "recommendations_upserted": 0,
             "metadata_inserted": 0}
        first_failure = ""
        write_error = ""
        interrupted = False
        batch = _Batch()

        def flush():
            nonlocal batch, write_error
            if not len(batch) or write_error:
                return
            try:
                got = write_batch(con, batch.feed, list(batch.meta.values()), batch.attempts, clock())
            except Exception as e:
                write_error = f"{type(e).__name__}: {ledger.redact(e)}"
                print(f"  [db] batch of {len(batch)} seeds rolled back ({write_error}); stopping",
                      file=sys.stderr)
                return
            n["batches"] += 1
            for k, v in got.items():
                n[k] += v
            batch = _Batch()

        try:
            for i, seed in enumerate(todo, 1):
                if sess.aborted or write_error:
                    break
                outcome, status, feed, meta, detail, attempts = ask(seed, top_n, sess)
                tag = f"  [{i:>5}/{len(todo)}] {seed[:50]:<50}"
                if outcome in ANSWERED:
                    n[{"OK": "ok", "NO_MATCH": "not_found", "SKIPPED": "skipped"}[outcome]] += 1
                    n["rec_rows"] += len(feed)
                    batch.add(seed, outcome, status, feed, meta)
                    print(f"{tag}  {len(feed):>3} recs" if outcome == "OK" else
                          f"{tag}  not_found" if outcome == "NO_MATCH" else f"{tag}  skipped ({detail})")
                elif attempts:
                    n["failed"] += 1
                    n["transport"] += outcome in TRANSPORT_KINDS
                    first_failure = first_failure or f"{outcome} {status} {detail}".strip()
                    batch.add(seed, outcome, status, [], [])          # recorded; asked again next run
                    print(f"{tag}  FAILED {outcome} {status} {detail[:80]}".rstrip(), file=sys.stderr)
                else:
                    n["not_sent"] += 1
                    print(f"{tag}  not sent ({outcome}: {detail[:80]})", file=sys.stderr)
                if len(batch) >= int(commit_every):
                    flush()
        except KeyboardInterrupt:
            interrupted = True
            print(f"\n  [interrupt] committing the {len(batch)} seed(s) already answered", file=sys.stderr)
        flush()

        totals = con.execute(
            "SELECT count(*), count(DISTINCT recommended_doi), "
            "count(DISTINCT recommended_doi) FILTER (WHERE NOT EXISTS "
            "(SELECT 1 FROM paper_locations l WHERE l.doi = f.recommended_doi)) FROM recent_feed f").fetchone()
        abstracts_after = _db.abstract_count(con)
    finally:
        con.close()

    asked = n["ok"] + n["not_found"] + n["failed"]
    aborted = sess.aborted or ("write_rolled_back" if write_error else None)
    reasons = []
    if n["failed"] and n["failed"] > FAIL_THRESHOLD * max(1, asked):
        reasons.append(f"{n['failed']} of {asked} seeds failed (over {FAIL_THRESHOLD:.0%}); first: {first_failure[:120]}")
    if aborted:
        reasons.append(f"aborted ({aborted}); {len(todo) - asked - n['skipped']} seed(s) not asked"
                       + (f": {write_error}" if write_error else ""))
    code = EXIT_ABORTED if aborted else (EXIT_DEGRADED if reasons else EXIT_OK)
    if interrupted and code == EXIT_OK:
        code = EXIT_INTERRUPTED
    res = {"step": "enrich_recommendations", "exit_code": code,
           "status": "aborted" if aborted else ("degraded" if reasons else ("interrupted" if interrupted else "ok")),
           "db": str(path), "project": project, "pool": POOL, "seeds": len(seeds),
           "answered_recently": n_recent,
           "to_ask": len(todo), "asked": asked, "ok": n["ok"], "not_found": n["not_found"],
           "skipped": n["skipped"], "failed": n["failed"], "not_sent": n["not_sent"],
           "transport_failures": n["transport"], "rec_rows": n["rec_rows"], "batches": n["batches"],
           "new_pairs": n["new_pairs"], "seen_again": n["seen_again"],
           "recommendations_upserted": n["recommendations_upserted"],
           "metadata_inserted": n["metadata_inserted"], "write_error": write_error,
           "interrupted": interrupted, "feed_rows": totals[0], "feed_unique_dois": totals[1],
           "feed_novel_dois": totals[2], "abstracts_before": abstracts_before,
           "abstracts_after": abstracts_after, "reasons": reasons, "aborted": aborted,
           "s2": sess.summary()}
    _print_summary(res)
    return res


def _print_summary(res):
    print("\n=== summary ===")
    print(f"  seeds asked:         {res['asked']} of {res['to_ask']} ({res['answered_recently']} answered recently, "
          f"not asked)")
    print(f"  with a list:         {res['ok']}  (rec rows {res['rec_rows']})")
    print(f"  not found (404):     {res['not_found']}")
    print(f"  failed:              {res['failed']} (transport {res['transport_failures']}); "
          f"their earlier rows are kept and they are asked next run")
    if res["skipped"] or res["not_sent"]:
        print(f"  skipped / not sent:  {res['skipped']} / {res['not_sent']}")
    print(f"  recent_feed:         {res['new_pairs']} new pairs, {res['seen_again']} seen again; "
          f"{res['feed_rows']} rows, {res['feed_unique_dois']} DOIs, {res['feed_novel_dois']} not in a library")
    print(f"  paper_metadata:      {res['metadata_inserted']} new rows; abstracts {res['abstracts_before']} -> "
          f"{res['abstracts_after']}")
    print(f"  batches committed:   {res['batches']}")
    if res["interrupted"]:
        print("  INTERRUPTED: the answered seeds were committed; rerun to continue")
    if res["status"] != "ok":
        print(f"  [{res['status'].upper()}] {'; '.join(res['reasons'])}")
        print("  resume: rerun the same command (answered seeds are in rec_attempts)")
    s = res["s2"]
    print(f"  [s2] {s.get('key')} calls={s.get('calls')} attempts={s.get('attempts')} "
          f"not_ok_attempts={s.get('attempts_not_ok')} budget={s.get('budget')}"
          + (f" ABORTED={s.get('aborted')}" if s.get("aborted") else ""))
    small = {k: v for k, v in res.items() if k != "s2"}
    small["s2"] = {k: s.get(k) for k in ("calls", "attempts", "attempts_not_ok", "budget", "aborted")}
    print(SUMMARY_MARKER + json.dumps(small, ensure_ascii=False), flush=True)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    ap.add_argument("--recent-feed", action="store_true",
                    help="Run the recommendation pass. Without it nothing is sent.")
    ap.add_argument("--allow-unkeyed", action="store_true",
                    help="With --recent-feed: run even without S2_API_KEY (6.5 s per request).")
    ap.add_argument("--db", default=None,
                    help="DuckDB portfolio index (default: portfolio.duckdb in projects.json db_dir).")
    ap.add_argument("--project", default=None,
                    help="Only this registered project's library DOIs as seeds.")
    ap.add_argument("--limit", type=int, default=0,
                    help="Ask the first N seeds still due only (testing).")
    ap.add_argument("--top-n", type=int, default=TOP_N,
                    help=f"Recommendations per seed (1..500; default {TOP_N}).")
    ap.add_argument("--refresh", action="store_true",
                    help="Ask every seed, including those answered within --retry-after-days.")
    ap.add_argument("--retry-after-days", type=int, default=RETRY_AFTER_DAYS,
                    help=f"Skip seeds answered within N days (default {RETRY_AFTER_DAYS}).")
    ap.add_argument("--commit-every", type=int, default=COMMIT_EVERY,
                    help=f"Seeds per DB transaction (default {COMMIT_EVERY}); a kill loses at most one batch.")
    ap.add_argument("--sleep", type=float, default=None,
                    help="Deprecated and ignored: spacing comes from litpipe.s2 (projects.json s2 block).")
    ap.add_argument("--s2-key", default=None,
                    help="Deprecated: set the S2_API_KEY environment variable instead.")
    args = ap.parse_args(argv)
    res = run(db=args.db, limit=args.limit, sleep=args.sleep, s2_key=args.s2_key, top_n=args.top_n,
              project=args.project, recent_feed=args.recent_feed, allow_unkeyed=args.allow_unkeyed,
              refresh=args.refresh, commit_every=args.commit_every,
              retry_after_days=args.retry_after_days)
    return res["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
