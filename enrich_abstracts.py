"""Enrich paper_metadata.abstract from the Crossref REST API (/works/{doi}).

Crossref returns a JATS-encoded `abstract` on most modern journal articles. clean_abstract() makes
it plain text (W2-E2): litpipe.text.display_field (tags stripped FIRST, then HTML5 and ISO Greek
isogrk1 references decoded in two passes, NFC, ligatures, odd spaces), repeated until it stops
changing so escaped markup (`&lt;b&gt;`, `&amp;lt;jats:p&amp;gt;`) is stripped and deeper escaping
decoded, then a leading "Abstract" heading dropped. The four shapes V0 found in the stored
abstracts (A-P11, N-A7): the heading (33,174 of 99,524 rows start with it; the 12 of 38 inspected
with a lower-case next word were headings too, e.g. "Abstract d-Psicose", "Abstract van Doorn et
al."), ISO Greek entities (`&agr;`), tags (534 rows: escaped markup the old cleaner decoded AFTER
stripping), and JATS residue (`&#x0D;`, `&nbsp;`, `&lt;` left from double escaping). Used by the
embedding-similarity layer. Existing rows are never rewritten here; the one-off cleanup of stored
abstracts is W5-B's (clean_abstract applies to a stored string as well).

Network: every request goes through litpipe.net (one identity: the User-Agent carries the
LITPIPE_EMAIL mailto, or none when unset, DEC-13; pacing, retries, ledger, typed Outcome). The
host table paces api.crossref.org at 0.34 s (about 3 requests/s, one at a time), under Crossref's
documented polite-pool limits: since 21 July 2026 Crossref "will rate-limit by email address" (it
was per IP); single-record requests "10 requests per second for the polite pool", lists 3/s
(https://community.crossref.org/t/refining-rest-api-limits-for-improved-stability-and-reliability/16137);
current values come back in the x-rate-limit-limit and x-concurrency-limit headers
(https://www.crossref.org/documentation/retrieve-metadata/rest-api/access-and-authentication/).
About 17k DOIs take about 95 min. Idempotent: only DOIs without an abstract are fetched. A host
refused or deferred for the run (a 403, a final 429, a long Retry-After) stops the run: the rest
stay unattempted for the next run, and the exit code is 1.

Writes (never an existing abstract; a failed fetch writes no abstract):
  paper_metadata.abstract                 only when a non-empty cleaned abstract came back, and
                                          only into a row whose abstract is still empty;
  paper_metadata.abstract_attempted_at    a hit, a genuine miss (200 without an abstract) and a
                                          permanent miss (400/404/410, not a DOI); a transient
                                          failure (403, 429, 5xx, transport, deferral) stays NULL
                                          so the next run retries it;
  abstract_attempts (new table)           the typed record of every attempt: doi, outcome (a
                                          litpipe.outcomes Kind name), status, detail (redacted),
                                          attempted_at.
Rows are buffered and committed in one transaction per --commit-every rows, at the end, and on an
interrupt (Ctrl-C): a fetched abstract is never lost to a crash of the batch. A batch whose
transaction fails is rewritten row by row, so one bad row cannot drop the others.

Usage:
  python enrich_abstracts.py                        # all DOIs missing abstract
  python enrich_abstracts.py --limit 100            # first 100 only (testing)
  python enrich_abstracts.py --only-papers          # restrict to PDFs we have, not candidates
  python enrich_abstracts.py --db <temp.duckdb>     # another DB (default: <db_dir>/portfolio.duckdb)
"""
import argparse
import re
import sys
import time

import lit_util
from litpipe import config, hosts, net
from litpipe import doi as _doi
from litpipe import text as _text
from litpipe.ledger import redact
from litpipe.outcomes import Kind

lit_util.utf8_stdout()

CROSSREF = "https://api.crossref.org/works/{doi}"   # {doi}: litpipe.doi.encode_path output
DB_NAME = "portfolio.duckdb"
COMMIT_EVERY = 100
PERMANENT_STATUSES = frozenset({400, 404, 410})       # "Crossref has no such work": never retried early

ATTEMPTS_DDL = """CREATE TABLE IF NOT EXISTS abstract_attempts (
  doi          VARCHAR PRIMARY KEY,
  outcome      VARCHAR,     -- litpipe.outcomes Kind name of the last attempt
  status       VARCHAR,     -- HTTP status of the last attempt ('' when none)
  detail       VARCHAR,     -- redacted reason
  attempted_at TIMESTAMP
)"""


def default_db() -> str:
    """portfolio.duckdb in projects.json `db_dir` (default <projects root>/_references), resolved at
    call time so a test's temp registry or root applies."""
    return str(config.db_dir() / DB_NAME)


# ------------------------------------------------------------------------------ cleaning
# A leading heading: the whole word "Abstract" in any case, then punctuation, whitespace or the end
# ("Abstract Background", "ABSTRACT: While", "Abstract"; never "Abstracts of" or "Abstraction"), or
# the word glued to the next one, case-sensitive ("AbstractBackground", "ABSTRACTCigarette",
# "ABSTRACTA systematic": 13 stored rows; never "ABSTRACTION").
_HEADING_WORD = re.compile(r"^\s*abstract(?:\s*[:.\u2013\u2014-]\s*|\s+|\s*$)", re.IGNORECASE)
_HEADING_GLUED = re.compile(r"^\s*(?:Abstract(?=[A-Z])|ABSTRACT(?=[A-Z][a-z]|[A-Z]\s))")


def clean_abstract(raw) -> str:
    """Plain-text abstract from a Crossref JATS `abstract` (or any stored abstract string): the
    display form (litpipe.text.display_field) taken again until it stops changing (at most three
    more rounds), so markup that decoding revealed is stripped and deeper escaping is decoded, then
    one leading "Abstract" heading dropped. '' when nothing but a heading is left.

    The extra rounds are for escaped markup, which display_field alone (tags first, then two
    decoding passes) leaves as text: Karger deposits `&lt;b&gt;&lt;i&gt;Purpose:&lt;/i&gt;&lt;/b&gt;`
    (display_field gives `<b><i>Purpose:</i></b>`), and one deposit (live 2026-10-05) wraps its
    whole abstract in `&amp;lt;jats:p&amp;gt;` with `p&amp;amp;lt;0,05` inside (it gives
    `<jats:p>` and `p&lt;0,05`)."""
    s = _text.display_field(raw)
    for _ in range(3):
        t = _text.display_field(s)
        if t == s:
            break
        s = t
    m = _HEADING_WORD.match(s) or _HEADING_GLUED.match(s)
    if m:
        s = s[m.end():].strip()
    return s


# ------------------------------------------------------------------------------ Crossref
def _kind_of(status) -> Kind:
    """litpipe.net's mapping of a final status, for a CrossRefError built from a status alone."""
    if status is None:
        return Kind.TRANSPORT
    if status in (404, 410):
        return Kind.NO_MATCH
    if status in (403, 406, 429):
        return Kind.REFUSED
    if status >= 500:
        return Kind.OUTAGE
    return Kind.ERROR


class CrossRefError(Exception):
    """A Crossref call that did not answer with a record (distinct from a genuine no-abstract).

    `status` is the HTTP status (None for a transport failure or a deferral), `kind` the
    litpipe.outcomes Kind, `host_blocked` True when api.crossref.org is refused or deferred for the
    run, so further calls would not be sent. A PERMANENT miss (400/404/410: Crossref never had the
    work, e.g. an arXiv or DataCite DOI) is marked attempted; a TRANSIENT one (403, 429, 5xx,
    transport) is retried next run."""

    def __init__(self, msg, status=None, kind=None, host_blocked=False):
        super().__init__(redact(str(msg)))
        self.status = status
        self.kind = Kind(kind) if kind is not None else _kind_of(status)
        self.host_blocked = host_blocked

    @property
    def permanent(self) -> bool:
        return self.status in PERMANENT_STATUSES or self.kind is Kind.NO_MATCH


def _state():
    if net.STATE is not None:
        return net.STATE
    import litpipe.state as st
    return st


def _host_blocked(out) -> bool:
    """True when the outcome means api.crossref.org is out for the rest of the run."""
    if out.kind is Kind.DEFERRED:
        return True
    if out.kind is not Kind.REFUSED:
        return False
    try:
        return bool(_state().is_refused(out.host or hosts.host_of(CROSSREF)))
    except Exception:                       # a broken state file: keep going row by row
        return False


def crossref_abstract(doi: str, timeout=15) -> str:
    """The cleaned abstract of `doi`'s Crossref record, or '' when the record has none.

    Raises CrossRefError for anything else: not a DOI (permanent, NO_MATCH), a 404/410 (permanent),
    and every failed call (transport, 403, 429, 5xx, deferral, a 200 that is not JSON), so an
    outage is never read as "no abstract" (RC6)."""
    try:
        path = _doi.encode_path(doi)        # normalised, then percent-encoded per segment (RC12, I47)
    except ValueError:
        raise CrossRefError("not a DOI", status=None, kind=Kind.NO_MATCH) from None
    out = net.get(CROSSREF.format(doi=path), timeout=timeout, validate=net.expect_json,
                  purpose="enrich_abstracts")
    if not out.ok:
        raise CrossRefError(out.detail or str(out.kind), status=out.status, kind=out.kind,
                            host_blocked=_host_blocked(out))
    try:
        body = out.payload.json()
    except ValueError as e:
        raise CrossRefError(f"bad JSON: {e}", status=out.status, kind=Kind.OUTAGE) from None
    msg = body.get("message") if isinstance(body, dict) else None
    if not isinstance(msg, dict):
        raise CrossRefError("200 without a message object", status=out.status, kind=Kind.OUTAGE)
    return clean_abstract(msg.get("abstract") or "")


# ------------------------------------------------------------------------------ batched writes
class _Writer:
    """Buffers per-DOI results and writes them in one transaction per `every` rows. A failed
    transaction is rolled back and its rows are written one by one (autocommit), so a single bad
    row never drops a batch; rows that still fail are listed in `failed`."""

    def __init__(self, con, every):
        self.con = con
        self.every = max(1, int(every or 1))
        self.buf = []
        self.in_tx = False
        self.commits = 0
        self.written = 0
        self.failed = []

    def add(self, doi, abstract, mark, outcome, status, detail):
        self.buf.append((doi, abstract, mark, outcome, status, detail))
        if len(self.buf) >= self.every:
            self.flush()

    def _write(self, row):
        doi, abstract, mark, outcome, status, detail = row
        if abstract:
            self.con.execute("UPDATE paper_metadata SET abstract = ?, abstract_attempted_at = now() "
                             "WHERE doi = ? AND (abstract IS NULL OR abstract = '')", [abstract, doi])
        elif mark:
            self.con.execute("UPDATE paper_metadata SET abstract_attempted_at = now() WHERE doi = ?", [doi])
        self.con.execute("INSERT OR REPLACE INTO abstract_attempts (doi, outcome, status, detail, attempted_at) "
                         "VALUES (?, ?, ?, ?, now())", [doi, outcome, status, detail])

    def _rollback(self):
        if self.in_tx:
            try:
                self.con.rollback()
            except Exception:
                pass
            self.in_tx = False

    def flush(self):
        if not self.buf:
            return
        rows = list(self.buf)
        try:
            self.con.begin()
            self.in_tx = True
            for r in rows:
                self._write(r)
            self.con.commit()
            self.in_tx = False
            self.commits += 1
            self.written += len(rows)
        except Exception as e:              # duckdb raises its own classes; the batch is aborted
            self._rollback()
            print(f"  [db] batch of {len(rows)} failed ({type(e).__name__}: {e}); writing row by row",
                  file=sys.stderr)
            for r in rows:
                try:
                    self._write(r)
                    self.written += 1
                except Exception as e2:
                    self.failed.append((r[0], f"{type(e2).__name__}: {e2}"))
                    print(f"  [db] write failed for {r[0]}: {type(e2).__name__}: {e2}", file=sys.stderr)
        self.buf.clear()

    def finish(self):
        """Write whatever is buffered (also after an interrupt that hit mid-transaction)."""
        self._rollback()
        self.flush()


# ------------------------------------------------------------------------------ the stage
def run(*, db=None, limit=0, sleep=0.0, only_papers=False, retry_after_days=30,
        commit_every=COMMIT_EVERY) -> dict:
    """Fetch abstracts for the DOIs missing one. Returns the counts; `interrupted` and
    `stopped_early` say whether the target list was finished."""
    db = db or default_db()
    con = lit_util.connect_db(db, on_fail="exit", tries=5, delays=(3,))  # c9: shared RC10 open
    # Self-healing migration: the attempt-state column and table may not exist on an older DB.
    # (A fresh --rebuild creates the column from index_portfolio's schema.)
    con.execute("ALTER TABLE paper_metadata ADD COLUMN IF NOT EXISTS abstract_attempted_at TIMESTAMP")
    con.execute(ATTEMPTS_DDL)

    where_extra = ""
    if only_papers:
        where_extra = " AND EXISTS (SELECT 1 FROM paper_locations l WHERE l.doi = m.doi)"
    # Attempt-state: skip DOIs we already tried recently (permanent Crossref misses -- closed
    # publishers, arXiv/DataCite -- otherwise re-queried every run indefinitely).
    where_attempt = ""
    if retry_after_days and retry_after_days > 0:
        where_attempt = (f" AND (abstract_attempted_at IS NULL "
                         f"OR abstract_attempted_at < now() - INTERVAL '{int(retry_after_days)} days')")
    q = f"""
        SELECT doi FROM paper_metadata m
        WHERE (abstract IS NULL OR abstract = '') {where_extra} {where_attempt}
        ORDER BY doi
    """
    if limit:
        q += f" LIMIT {int(limit)}"
    targets = [r[0] for r in con.execute(q).fetchall()]

    pace = max(float(sleep or 0), hosts.policy(CROSSREF).min_interval_s)
    print(f"DB: {db}")
    print(f"Targets: {len(targets)} DOIs missing abstract")
    print(f"Pacing:  litpipe.net {hosts.policy(CROSSREF).min_interval_s:.2f} s per Crossref request"
          + (f", plus --sleep {sleep} s" if sleep else ""))
    print(f"ETA:     ~{(len(targets) * pace) / 60:.1f} min\n")

    w = _Writer(con, commit_every)
    n_hit = n_miss = n_perm = n_trans = 0
    done = 0
    interrupted = stopped = False
    stop_reason = ""
    try:
        for i, doi in enumerate(targets, 1):
            try:
                abs_text = crossref_abstract(doi)
            except CrossRefError as e:
                # RC6: a network/HTTP failure is NOT a genuine 'no abstract'. A PERMANENT miss
                # (400/404/410, not a DOI) will never succeed via Crossref, so it is marked
                # attempted; a transient 403/429/5xx/transport/deferral stays unmarked so the next
                # run retries it (a transient 4xx storm must not suppress real papers).
                permanent = e.permanent
                w.add(doi, "", permanent, str(e.kind), str(e.status or ""), str(e))
                done += 1
                if permanent:
                    n_perm += 1
                else:
                    n_trans += 1
                tag = "ER!" if permanent else "ER"
                print(f"  [{i:>5}/{len(targets)}] {tag}  {doi}  ({e.kind}: {e})", file=sys.stderr)
                if e.host_blocked:
                    stopped, stop_reason = True, str(e)
                    print(f"  [stop] api.crossref.org is out for this run ({e}); "
                          f"{len(targets) - i} DOIs left for the next run", file=sys.stderr)
                    break
                if sleep:
                    time.sleep(sleep)
                continue
            if abs_text:
                w.add(doi, abs_text, True, str(Kind.OK), "200", "")
                n_hit += 1
                tag = "OK"
            else:
                # a genuine Crossref 200 with no abstract (closed publisher): mark attempted so
                # this permanent miss is not re-queried until --retry-after-days elapses.
                w.add(doi, "", True, str(Kind.NOT_AVAILABLE), "200", "no abstract in the Crossref record")
                n_miss += 1
                tag = "--"
            done += 1
            if i % 50 == 0 or i <= 10 or i == len(targets):
                print(f"  [{i:>5}/{len(targets)}] {tag}  {doi}  hits={n_hit} misses={n_miss} "
                      f"errs={n_perm + n_trans}")
            if sleep:
                time.sleep(sleep)
    except KeyboardInterrupt:
        interrupted = True
        print(f"\n  [interrupt] committing {len(w.buf)} buffered rows before exit", file=sys.stderr)
    finally:
        w.finish()
        con.close()

    print("\n=== summary ===")
    print(f"  targets:    {len(targets)}")
    print(f"  attempted:  {done}")
    print(f"  abstracts:  {n_hit} ({100 * n_hit / max(1, done):.0f}%)")
    print(f"  no-abstract:{n_miss}")
    print(f"  errors:     {n_perm} permanent, {n_trans} transient")
    print(f"  commits:    {w.commits} (rows written {w.written}, failed {len(w.failed)})")
    if stopped:
        print(f"  STOPPED EARLY: {stop_reason}")
    if interrupted:
        print("  INTERRUPTED: buffered rows were committed")
    return {"db": db, "targets": len(targets), "attempted": done, "hits": n_hit, "misses": n_miss,
            "errors_permanent": n_perm, "errors_transient": n_trans, "commits": w.commits,
            "rows_written": w.written, "write_failures": list(w.failed), "interrupted": interrupted,
            "stopped_early": stopped, "stop_reason": stop_reason}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    ap.add_argument("--db", default=None,
                    help="DuckDB portfolio index path (default: portfolio.duckdb in projects.json "
                         "db_dir, else <projects root>/_references).")
    ap.add_argument("--limit", type=int, default=0,
                    help="Process first N DOIs only (testing).")
    ap.add_argument("--sleep", type=float, default=0.0,
                    help="Extra seconds between DOIs, on top of litpipe.net's Crossref pacing "
                         "(0.34 s per request). Default 0.")
    ap.add_argument("--only-papers", action="store_true",
                    help="Only enrich DOIs we have PDFs for (skip candidates).")
    ap.add_argument("--retry-after-days", type=int, default=30,
                    help="Re-attempt a previously-failed DOI only after N days (0 = always). "
                         "Stops re-querying the ~47k permanent CrossRef-fails every run.")
    ap.add_argument("--commit-every", type=int, default=COMMIT_EVERY,
                    help=f"Rows per DB transaction (default {COMMIT_EVERY}); buffered rows are also "
                         "committed at the end and on Ctrl-C.")
    args = ap.parse_args(argv)
    res = run(db=args.db, limit=args.limit, sleep=args.sleep, only_papers=args.only_papers,
              retry_after_days=args.retry_after_days, commit_every=args.commit_every)
    if res["interrupted"]:
        return 130
    return 1 if res["stopped_early"] or res["write_failures"] else 0


if __name__ == "__main__":
    sys.exit(main())
