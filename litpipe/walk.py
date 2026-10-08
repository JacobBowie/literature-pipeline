"""litpipe.walk: the forward walk's cache, routing and call planner (K2; dispatch W3-A; DEC-17).

forward_citations.py is the CLI over this module; W4's runner may call it without the CLI.

The cache (`<state_dir>/s2_cache.duckdb`, DEC-17). Only this module opens it; it is never a table in
portfolio.duckdb. Two tables:
  seed_state  one row per (seed DOI, source): the S2 paperId, count_at_walk (always S2's
              citationCount from the metadata pass, whichever source walked the seed), n_rows (the
              stored citer set's size), state (a litpipe.s2.WalkState value, or `unresolved` for a
              seed paper_batch answered null or SKIPPED), kind and reason of the last walk,
              n_unreachable (capped_9999), walked_at (the last walk, TIMESTAMPTZ UTC), rows_at
              (when the stored citer set was last replaced; NULL when no walk has stored one) and
              oa_cited_by (OpenAlex's cited_by_count from the walk's free singleton; NULL for an S2
              walk or a cache written before the column existed, which is added on open).
  citers      the citer set of each (seed DOI, source), keyed by (seed DOI, source, citing id):
              the S2 paperId or OpenAlex W-id, or `stub:<position>` for a null-paperId stub (stubs
              count toward the length check and are kept; only rows with a DOI become candidates).
A walk that answered with rows (complete, empty, capped_9999) replaces that seed's rows for the walked
source in ONE transaction: delete, then a set-based insert (design 4.1 step 6: citers leave S2's lists).
Any other walk (failed, elided, not_found, unresolved) updates the state only, so a failed seed keeps
its rows. On any failed statement the transaction is rolled back explicitly before anything else (in
DuckDB 1.5.5 a commit() after a failed statement silently rolls the whole transaction back) and the
caller records the seed as failed. Writes are set-based only (a registered DataFrame; executemany took
10.8 s for 5,000 rows, W3-E). Every connection runs `SET TimeZone='UTC'`.

The path is resolved once per run: an explicit `cache_path`, else the module override CACHE_PATH
(tests set it, like litpipe.state.DB_PATH), else config.state_dir(reg, create=False) / CACHE_NAME.
state_dir() is never called with create=True: with no `state_dir` key it is the real
~/.local/db/literature_pipeline. The parent directory is made at the first cache WRITE, never on an
import, a read or --help. DuckDB allows one writer per file: a cache held by another process raises
CacheLocked (the walker exits 3, "aborted: cache locked"); nothing waits or retries.

The count gate (needs_walk). A seed is walked when it is new to the cache (for the source it routes
to), when its citationCount differs from count_at_walk, or when its state is failed, unresolved,
not_found or elided (never read as zero citers). citationCount 0 is `empty` with no call; `refresh`
walks every seed. This replaces design 4.1 step 3's `citationCount != n_rows` test on purpose: a
capped_9999 seed always has n_rows < citationCount, so that test would re-walk every capped seed on
every run. A seed S2 holds no count for (source "openalex") is gated on OpenAlex instead: the
caller reads its current cited_by_count with openalex_counts() (one free singleton per seed) and
passes it as `oa_count`; the seed is walked again when it differs from the oa_cited_by its last
walk stored. Without `oa_count` such a seed compares None with None and is kept, as before.

The OpenAlex length check (walk_openalex). The rows OpenAlex RETURNED, before W-id de-duplication,
must equal the first or the last page's meta.count (the count can move while the cursor pages).
A W-id repeated across cursor pages is then dropped from the result (first wins), counted in
Result.duplicates and named in Result.reason, and the seed is complete: a cross-page duplicate is
OpenAlex paging, never a short list.

Routing (route; design 4.1 step 4, B-cons). With source "s2": 0 -> empty (no call); <= 1,000 ->
nested batch (s2.batch_nested with the metadata pass's counts; it packs and re-fetches mismatched
lists itself); <= 9,999 -> paged GET (s2.citations; never offset+limit >= 10000); above 9,999 (O2) ->
OpenAlex `cites:` when an OpenAlex key is set and the run's OpenAlex session has not stopped, else S2
year windows (capped_9999, unreachable estimate recorded); forward_citations also takes the windows
when OpenAlex does not hold the DOI. With source "openalex" every seed goes to OpenAlex. The planner (plan) applies the same rules to a count vector and returns the call count
without sending anything (design section 5 and its appendix).
"""
from __future__ import annotations

import dataclasses
import math
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping

from litpipe import config, ledger, openalex, s2
from litpipe import doi as _doi
from litpipe.outcomes import Kind, Outcome

CACHE_NAME = "s2_cache.duckdb"
CACHE_PATH = None            # a path overrides config.state_dir()/CACHE_NAME (tests, W4)
SCHEMA_VERSION = 2           # 2: seed_state.oa_cited_by (added in place on open; nullable)

SOURCES = ("s2", "openalex")
UNRESOLVED = "unresolved"
STATES = tuple(str(s) for s in s2.WalkState) + (UNRESOLVED,)
WALK_AGAIN = frozenset({str(s2.WalkState.FAILED), UNRESOLVED, str(s2.WalkState.NOT_FOUND),
                        str(s2.WalkState.ELIDED)})
ROWS_STATES = frozenset({str(s2.WalkState.COMPLETE), str(s2.WalkState.EMPTY), str(s2.WalkState.CAPPED)})

NESTED_MAX = 1000            # B-cons: nested batch for citationCount <= 1,000 (largest tested list 942)
PAGED_MAX = s2.REACHABLE     # paged GET up to 9,999; above that S2 cannot page (O2)
ROUTE_EMPTY, ROUTE_NESTED, ROUTE_PAGED = "empty", "nested", "paged"
ROUTE_OPENALEX, ROUTE_WINDOWS = "openalex", "windows"

# The metadata pass (one paper_batch per 500 seeds) and the citing-paper fields (design 4.1 step 5:
# the lean set, no abstract).
META_FIELDS = ("paperId", "citationCount", "externalIds", "year")
CITATION_FIELDS = s2.CITATION_FIELDS
# Top-level fields only: OpenAlex "select" takes top-level fields, not nested properties
# (help.openalex.org/api/selecting-fields, read 2026-10-06).
OA_SELECT = ("id", "doi", "display_name", "publication_year", "cited_by_count", "authorships",
             "primary_location", "open_access")

CITER_COLUMNS = ("citing_paper_id", "citing_doi", "citing_title", "citing_year", "citing_authors",
                 "citing_venue", "citing_cited_by", "citing_oa")
_PAPER_SHA = re.compile(r"^[0-9a-f]{40}$")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS cache_meta (key VARCHAR PRIMARY KEY, value VARCHAR);
CREATE TABLE IF NOT EXISTS seed_state (
  doi           VARCHAR NOT NULL,
  source        VARCHAR NOT NULL,
  paper_id      VARCHAR,
  count_at_walk BIGINT,
  n_rows        BIGINT,
  state         VARCHAR NOT NULL,
  kind          VARCHAR,
  reason        VARCHAR,
  n_unreachable BIGINT,
  walked_at     TIMESTAMPTZ NOT NULL,
  rows_at       TIMESTAMPTZ,
  oa_cited_by   BIGINT,
  PRIMARY KEY (doi, source)
);
ALTER TABLE seed_state ADD COLUMN IF NOT EXISTS oa_cited_by BIGINT;
CREATE TABLE IF NOT EXISTS citers (
  seed_doi        VARCHAR NOT NULL,
  source          VARCHAR NOT NULL,
  citing_id       VARCHAR NOT NULL,
  pos             INTEGER NOT NULL,
  citing_paper_id VARCHAR,
  citing_doi      VARCHAR,
  citing_title    VARCHAR,
  citing_year     INTEGER,
  citing_authors  VARCHAR,
  citing_venue    VARCHAR,
  citing_cited_by BIGINT,
  citing_oa       BOOLEAN,
  PRIMARY KEY (seed_doi, source, citing_id)
);
"""

_STATE_COLS = ("doi", "source", "paper_id", "count_at_walk", "n_rows", "state", "kind", "reason",
               "n_unreachable", "walked_at", "rows_at", "oa_cited_by")


# ------------------------------------------------------------------------------ path
def cache_path(reg=None, override=None) -> Path:
    """The cache file for this run: `override`, else CACHE_PATH, else <state_dir>/s2_cache.duckdb
    with state_dir resolved WITHOUT creating it (ConfigError on a bad state_dir value)."""
    if override is not None:
        return Path(override)
    if CACHE_PATH is not None:
        return Path(CACHE_PATH)
    return config.state_dir(reg, create=False) / CACHE_NAME


def now_utc() -> str:
    """An offset-bearing UTC instant: never bind a naive string to TIMESTAMPTZ."""
    return datetime.now(timezone.utc).isoformat()


# ------------------------------------------------------------------------------ the cache
class CacheLocked(Exception):
    """Another process holds the cache (DuckDB allows one writer per file)."""


class CacheUnreadable(Exception):
    """The cache file exists but DuckDB cannot open it (not a database, corrupt, unreadable)."""


class CacheWriteError(Exception):
    """A cache statement failed; the transaction was rolled back."""


_LOCK_MARKERS = ("already open", "being used by another process", "could not set lock", "conflicting lock",
                 "lock on file")


def _is_lock_error(exc) -> bool:
    msg = str(exc).lower()
    return any(m in msg for m in _LOCK_MARKERS)


class Cache:
    """The walk cache. Opening an existing file takes the writer lock at once (CacheLocked when another
    process holds it); a missing file is created at the first write, so a run that writes nothing
    creates nothing. Reads on a cache that does not exist yet are empty."""

    def __init__(self, path):
        self.path = Path(path)
        self.con = None
        self.writes = 0
        self.write_failures = 0
        if self.path.exists():
            self._connect()

    # -- connection
    def _connect(self):
        import duckdb
        try:
            con = duckdb.connect(str(self.path))
        except duckdb.Error as e:
            if _is_lock_error(e):
                raise CacheLocked(f"{self.path.name} is held by another process: {ledger.redact(str(e))[:200]}") from None
            raise CacheUnreadable(f"{self.path}: {type(e).__name__}: {ledger.redact(str(e))[:200]}") from None
        try:
            con.execute("SET TimeZone='UTC'")
            con.execute(_SCHEMA)
            # a migrated older cache records the version it now has (never lowered by an older reader)
            con.execute("INSERT INTO cache_meta VALUES ('schema_version', ?) ON CONFLICT (key) DO UPDATE SET "
                        "value = excluded.value WHERE TRY_CAST(cache_meta.value AS INTEGER) < "
                        "TRY_CAST(excluded.value AS INTEGER)", [str(SCHEMA_VERSION)])
        except Exception:
            con.close()
            raise
        self.con = con

    def _ensure(self):
        if self.con is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)     # the first cache write
            self._connect()

    def close(self):
        if self.con is not None:
            self.con.close()
            self.con = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()

    # -- reads
    def states(self) -> dict:
        """{(doi, source): state dict} for every seed in the cache ({} before the first write)."""
        if self.con is None:
            return {}
        cur = self.con.execute(f"SELECT {', '.join(_STATE_COLS)} FROM seed_state")
        return {(r[0], r[1]): dict(zip(_STATE_COLS, r)) for r in cur.fetchall()}

    def rows_many(self, pairs: Iterable) -> dict:
        """{(doi, source): [citer dict, ...]} in stored order, for the (doi, source) pairs asked
        (pairs with no stored rows are absent). One set-based query."""
        pairs = list(dict.fromkeys(tuple(p) for p in pairs))
        if self.con is None or not pairs:
            return {}
        import pandas as pd
        want = pd.DataFrame(pairs, columns=["doi", "source"])
        self.con.register("_walk_want", want)
        try:
            cur = self.con.execute(
                "SELECT c.seed_doi, c.source, " + ", ".join(f"c.{x}" for x in CITER_COLUMNS) +
                " FROM citers c JOIN _walk_want w ON c.seed_doi = w.doi AND c.source = w.source "
                "ORDER BY c.seed_doi, c.source, c.pos")
            out: dict = {}
            for r in cur.fetchall():
                out.setdefault((r[0], r[1]), []).append(dict(zip(CITER_COLUMNS, r[2:])))
            return out
        finally:
            try:
                self.con.unregister("_walk_want")
            except Exception:
                pass

    def rows(self, doi, source) -> list | None:
        return self.rows_many([(doi, source)]).get((doi, source))

    # -- writes
    def record(self, doi, source, *, state, count=None, paper_id=None, rows=None, kind=None, reason="",
               unreachable=None, walked_at=None, oa_cited_by=None):
        """Record one seed's walk. With `rows` (a list, possibly empty) the seed's citer set for
        `source` is replaced in one transaction (delete, then a set-based insert) together with its
        state; without rows only the state changes and the stored citers stay. `oa_cited_by`
        (Result.oa_cited_by of an OpenAlex walk) is stored when given and kept otherwise. Raises
        CacheLocked or CacheWriteError (after an explicit ROLLBACK, and a best-effort state row
        marking the seed failed)."""
        if source not in SOURCES:
            raise ValueError(f"source must be one of {SOURCES}, got {source!r}")
        if state not in STATES:
            raise ValueError(f"state must be one of {STATES}, got {state!r}")
        self._ensure()
        walked_at = walked_at or now_utc()
        reason = ledger.redact(reason or "")[:300]
        oa_cited_by = _int_or_none(oa_cited_by)
        con = self.con
        try:
            con.execute("BEGIN TRANSACTION")
            if rows is not None:
                con.execute("DELETE FROM citers WHERE seed_doi = ? AND source = ?", [doi, source])
                stored = _citer_frame(doi, source, rows)
                if len(stored):
                    con.register("_walk_rows", stored)
                    try:
                        cols = ", ".join(stored.columns)
                        con.execute(f"INSERT INTO citers ({cols}) SELECT {cols} FROM _walk_rows")
                    finally:
                        con.unregister("_walk_rows")
                con.execute(
                    "INSERT INTO seed_state (" + ", ".join(_STATE_COLS) + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, "
                    "CAST(? AS TIMESTAMPTZ), CAST(? AS TIMESTAMPTZ), ?) ON CONFLICT (doi, source) DO UPDATE SET "
                    "paper_id = coalesce(excluded.paper_id, seed_state.paper_id), "
                    "count_at_walk = excluded.count_at_walk, n_rows = excluded.n_rows, state = excluded.state, "
                    "kind = excluded.kind, reason = excluded.reason, n_unreachable = excluded.n_unreachable, "
                    "walked_at = excluded.walked_at, rows_at = excluded.rows_at, "
                    "oa_cited_by = coalesce(excluded.oa_cited_by, seed_state.oa_cited_by)",
                    [doi, source, paper_id, count, len(stored), state, kind, reason, unreachable,
                     walked_at, walked_at, oa_cited_by])
            else:
                self._state_only(con, doi, source, state, count, paper_id, kind, reason, walked_at, oa_cited_by)
            con.execute("COMMIT")
            self.writes += 1
        except Exception as e:
            self.write_failures += 1
            try:
                con.execute("ROLLBACK")
            except Exception:
                pass
            if _is_lock_error(e):
                raise CacheLocked(ledger.redact(str(e))[:200]) from None
            why = f"cache write failed: {type(e).__name__}: {ledger.redact(str(e))[:160]}"
            try:                                     # its own short transaction: mark the seed failed
                con.execute("BEGIN TRANSACTION")
                self._state_only(con, doi, source, str(s2.WalkState.FAILED), count, paper_id, "CACHE", why,
                                 walked_at)
                con.execute("COMMIT")
            except Exception:
                try:
                    con.execute("ROLLBACK")
                except Exception:
                    pass
            raise CacheWriteError(why) from None

    @staticmethod
    def _state_only(con, doi, source, state, count, paper_id, kind, reason, walked_at, oa_cited_by=None):
        con.execute(
            "INSERT INTO seed_state (doi, source, paper_id, count_at_walk, n_rows, state, kind, reason, walked_at, "
            "oa_cited_by) VALUES (?, ?, ?, ?, 0, ?, ?, ?, CAST(? AS TIMESTAMPTZ), ?) ON CONFLICT (doi, source) "
            "DO UPDATE SET paper_id = coalesce(excluded.paper_id, seed_state.paper_id), "
            "count_at_walk = excluded.count_at_walk, state = excluded.state, kind = excluded.kind, "
            "reason = excluded.reason, walked_at = excluded.walked_at, "
            "oa_cited_by = coalesce(excluded.oa_cited_by, seed_state.oa_cited_by)",
            [doi, source, paper_id, count, state, kind, reason, walked_at, oa_cited_by])


def _citer_frame(doi, source, rows):
    """The rows as the citers table's columns, deduplicated on the citing id (first wins)."""
    import pandas as pd
    seen, out = set(), []
    for pos, r in enumerate(rows):
        pid = str(r.get("citing_paper_id") or "").strip()
        cid = pid or f"stub:{pos}"
        if cid in seen:
            continue
        seen.add(cid)
        out.append((doi, source, cid, pos, pid or None, r.get("citing_doi") or None, r.get("citing_title") or None,
                    _int_or_none(r.get("citing_year")), r.get("citing_authors") or None,
                    r.get("citing_venue") or None, _int_or_none(r.get("citing_cited_by")),
                    _bool_or_none(r.get("citing_oa"))))
    df = pd.DataFrame(out, columns=["seed_doi", "source", "citing_id", "pos", "citing_paper_id", "citing_doi",
                                    "citing_title", "citing_year", "citing_authors", "citing_venue",
                                    "citing_cited_by", "citing_oa"])
    for c in ("pos", "citing_year", "citing_cited_by"):
        df[c] = df[c].astype("Int64")
    df["citing_oa"] = df["citing_oa"].astype("boolean")
    return df


def _int_or_none(v):
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    try:
        return int(str(v).strip())
    except (TypeError, ValueError):
        return None


def _bool_or_none(v):
    if isinstance(v, bool):
        return v
    s = str(v or "").strip().lower()
    if s in ("true", "1", "yes"):
        return True
    if s in ("false", "0", "no"):
        return False
    return None


# ------------------------------------------------------------------------------ rows
def norm_doi(raw) -> str:
    """A DOI from a structured field (S2 externalIds.DOI, OpenAlex doi, a .ris DO line, a sidecar
    `doi`, a seed list) in its whole registered form; "" for a placeholder or a malformed value."""
    if not isinstance(raw, str) or not raw.strip():
        return ""
    return _doi.normalise_structured(raw) or ""


def _text(v) -> str:
    return v if isinstance(v, str) else ("" if v is None else str(v))


def s2_citing(paper) -> dict:
    """The citer columns of one S2 citing-paper object (nested list item or unwrapped citingPaper).
    Missing or null fields are blank; nothing is invented; OA is unknown (the lean field set)."""
    p = paper if isinstance(paper, dict) else {}
    ext = p.get("externalIds")
    raw = ext.get("DOI") if isinstance(ext, dict) else None
    authors = p.get("authors")
    names = ([a.get("name") for a in authors if isinstance(a, dict) and isinstance(a.get("name"), str)]
             if isinstance(authors, list) else [])
    year, cited = p.get("year"), p.get("citationCount")
    return {"citing_paper_id": _text(p.get("paperId")), "citing_doi": norm_doi(raw),
            "citing_title": _text(p.get("title")),
            "citing_year": year if isinstance(year, int) and not isinstance(year, bool) else "",
            "citing_authors": "; ".join(names), "citing_venue": _text(p.get("venue")),
            "citing_cited_by": cited if isinstance(cited, int) and not isinstance(cited, bool) else "",
            "citing_oa": ""}


def openalex_citing(work) -> dict:
    """The citer columns of one OpenAlex work (select OA_SELECT): the W-id, display_name,
    publication_year, the authorships' author display names `; `-joined,
    primary_location.source.display_name, cited_by_count and open_access.is_oa."""
    w = work if isinstance(work, dict) else {}
    try:
        wid = openalex.work_id(w.get("id"))
    except ValueError:
        wid = ""
    names = []
    for a in w.get("authorships") or []:
        au = a.get("author") if isinstance(a, dict) else None
        n = au.get("display_name") if isinstance(au, dict) else None
        if isinstance(n, str) and n.strip():
            names.append(n.strip())
    loc = w.get("primary_location")
    src = loc.get("source") if isinstance(loc, dict) else None
    venue = src.get("display_name") if isinstance(src, dict) else None
    oa = w.get("open_access")
    is_oa = oa.get("is_oa") if isinstance(oa, dict) else None
    year, cited = w.get("publication_year"), w.get("cited_by_count")
    return {"citing_paper_id": wid, "citing_doi": norm_doi(w.get("doi")),
            "citing_title": _text(w.get("display_name")),
            "citing_year": year if isinstance(year, int) and not isinstance(year, bool) else "",
            "citing_authors": "; ".join(names), "citing_venue": _text(venue),
            "citing_cited_by": cited if isinstance(cited, int) and not isinstance(cited, bool) else "",
            "citing_oa": is_oa if isinstance(is_oa, bool) else ""}


# ------------------------------------------------------------------------------ the gate and routing
def needs_walk(count, cached, *, refresh=False, oa_count=None) -> bool:
    """True when the seed must be walked: `refresh`, nothing cached for the source it routes to, a
    state in WALK_AGAIN (failed, unresolved, not_found, elided), or citationCount != count_at_walk.
    A seed with no S2 count (`count` None) is gated on `oa_count`, OpenAlex's current
    cited_by_count, when the caller has it: walked when it differs from the oa_cited_by the last
    walk stored (NULL before this column existed, so such a seed is walked once to store it).
    See the module docstring for why this is not design 4.1 step 3's n_rows test."""
    if refresh or not cached:
        return True
    if cached.get("state") in WALK_AGAIN:
        return True
    if count is None and oa_count is not None:
        return cached.get("oa_cited_by") != oa_count
    return cached.get("count_at_walk") != count


def openalex_counts(dois, *, session=None) -> dict:
    """{doi: OpenAlex cited_by_count} for the seeds S2 holds no count for, one free singleton each
    (openalex.cited_by_count). A DOI whose lookup did not answer with a count maps to None, so the
    gate falls back to its count_at_walk test for it (kept, never re-walked on a failed lookup)."""
    out = {}
    for d in dict.fromkeys(dois):
        if session is not None and session.aborted:
            out[d] = None
            continue
        got = openalex.cited_by_count(d, session=session)
        out[d] = got.payload if got.ok and isinstance(got.payload, int) else None
    return out


def route(count, *, source="s2", oa_ok=False):
    """The walk route of a seed with S2 citationCount `count` (None: unknown). None when the seed
    cannot be walked (no count from S2 under source "s2")."""
    if source == "openalex":
        return ROUTE_OPENALEX
    if source != "s2":
        raise ValueError(f"source must be 's2' or 'openalex', got {source!r}")
    if count is None:
        return None
    if count == 0:
        return ROUTE_EMPTY
    if count <= NESTED_MAX:
        return ROUTE_NESTED
    if count <= PAGED_MAX:
        return ROUTE_PAGED
    return ROUTE_OPENALEX if oa_ok else ROUTE_WINDOWS


def route_source(r) -> str:
    return "openalex" if r == ROUTE_OPENALEX else "s2"


# ------------------------------------------------------------------------------ the planner
def plan(counts: Mapping, cached: Mapping | None = None, *, refresh=False, source="s2", openalex_key=False,
         n_metadata=None, oa_counts: Mapping | None = None) -> dict:
    """The calls a run would make, without sending anything (design section 5, appendix formulas).
    `counts`: {seed: S2 citationCount, or None when S2 has no record}; `cached`: {(seed, source):
    state dict} as Cache.states() returns. Metadata = ceil(seeds / 500) (or ceil(n_metadata / 500));
    nested bins = first-fit-decreasing bins (<= 9,000 citations, <= 500 ids) over walked seeds with
    0 < count <= 1,000; pages = sum ceil(count / 1,000) for 1,000 < count <= 9,999; windows (above
    9,999 without OpenAlex) = ceil(count / 1,000) + ceil(count / 9,000) probes; OpenAlex (above 9,999
    with a key, or every seed under source "openalex") = 1 free singleton + ceil(count / 100) list
    calls, estimated from S2's count. `oa_counts` ({seed: cited_by_count}, openalex_counts()) gates
    the seeds with no S2 count as needs_walk does."""
    cached = cached or {}
    oa_counts = oa_counts or {}
    n_meta = len(counts) if n_metadata is None else n_metadata
    nested, out = {}, {"metadata": math.ceil(n_meta / s2.BATCH_MAX_IDS) if n_meta else 0,
                       "nested_bins": 0, "pages": 0, "windows": 0, "openalex_singletons": 0,
                       "openalex_lists": 0, "walk": 0, "kept": 0, "empty": 0, "unwalkable": 0}
    for seed, cc in counts.items():
        r = route(cc, source=source, oa_ok=openalex_key)
        if r is None:
            out["unwalkable"] += 1
            continue
        if not needs_walk(cc, cached.get((seed, route_source(r))), refresh=refresh, oa_count=oa_counts.get(seed)):
            out["kept"] += 1
            continue
        out["walk"] += 1
        if r == ROUTE_EMPTY:
            out["empty"] += 1
        elif r == ROUTE_NESTED:
            nested[seed] = cc
        elif r == ROUTE_PAGED:
            out["pages"] += math.ceil(cc / s2.PAGE_MAX)
        elif r == ROUTE_WINDOWS:
            out["windows"] += math.ceil(cc / s2.PAGE_MAX) + math.ceil(cc / s2.DEFAULT_NESTED_CAP)
        else:
            out["openalex_singletons"] += 1
            out["openalex_lists"] += max(1, math.ceil((cc or 0) / openalex.PER_PAGE))
    out["nested_bins"] = len(s2.pack_ffd(nested, s2.DEFAULT_NESTED_CAP, s2.BATCH_MAX_IDS)) if nested else 0
    out["s2_calls"] = out["metadata"] + out["nested_bins"] + out["pages"] + out["windows"]
    out["openalex_calls"] = out["openalex_singletons"] + out["openalex_lists"]
    return out


def plan_line(p: dict) -> str:
    return (f"[plan] S2 calls {p['s2_calls']} = metadata {p['metadata']} + nested bins {p['nested_bins']} "
            f"+ pages {p['pages']} + window calls {p['windows']}; OpenAlex about {p['openalex_calls']} "
            f"({p['openalex_singletons']} free singletons); walk {p['walk']}, kept by the count gate "
            f"{p['kept']}, no S2 count {p['unwalkable']}")


# ------------------------------------------------------------------------------ walking one seed
@dataclasses.dataclass
class Result:
    """One seed's walk, source-neutral. rows: citer dicts (CITER_COLUMNS) for complete, empty and
    capped_9999; None otherwise. not_sent: the run stopped before this walk sent anything (the seed
    is "not walked", never failed)."""
    state: str
    source: str
    rows: list | None = None
    kind: str | None = None
    status: int | None = None
    reason: str = ""
    attempts: int = 0
    mismatch: bool = False
    unreachable: int | None = None
    not_sent: bool = False
    expected: int | None = None      # the count the list was checked against (S2: from the same call)
    route_taken: str | None = None
    duplicates: int = 0              # OpenAlex: W-ids repeated across cursor pages, dropped from rows
    oa_cited_by: int | None = None   # OpenAlex: the seed's cited_by_count from the walk's singleton

    @property
    def failed(self) -> bool:
        return self.state == str(s2.WalkState.FAILED)


def from_s2(w: s2.Walk, session=None) -> Result:
    """A Result from a litpipe.s2 Walk. A count mismatch is failed (kind COUNT_MISMATCH); the rows
    s2 fetched before failing sit in Walk.partial, which is diagnosis, never a result."""
    if w.failed:
        stopped = session is not None and session.aborted and w.kind is Kind.DEFERRED and not w.attempts
        mismatch = w.reason.startswith("count_mismatch")
        return Result(str(s2.WalkState.FAILED), "s2", None,
                      kind="COUNT_MISMATCH" if mismatch else str(w.kind or Kind.ERROR), status=w.status,
                      reason=w.reason, attempts=w.attempts, mismatch=mismatch, not_sent=stopped,
                      expected=w.count_at_walk)
    rows = [s2_citing(p) for p in w.rows] if w.rows is not None else None
    return Result(str(w.state), "s2", rows, status=w.status, reason=w.reason, attempts=w.attempts,
                  unreachable=w.n_unreachable_est, expected=w.count_at_walk)


def walk_nested(counts: Mapping, *, session) -> Iterable:
    """Yield (walk id, Result) for seeds of <= 1,000 citers, one nested batch per bin: the bins come
    from s2.pack_ffd (<= 9,000 citations, <= 500 ids) and each bin goes through s2.batch_nested with
    the metadata pass's counts (no second metadata call), which re-fetches mismatched lists by paged
    GET itself. Stops before a bin when the session has stopped; the caller records per bin."""
    for group in s2.pack_ffd(dict(counts), s2.DEFAULT_NESTED_CAP, s2.BATCH_MAX_IDS):
        if session.aborted:
            return
        got = s2.batch_nested(group, "citations", CITATION_FIELDS, counts={k: counts[k] for k in group},
                              session=session)
        for k in group:
            yield k, from_s2(got[k], session)


def walk_paged(wid, count, *, session) -> Result:
    return from_s2(s2.citations(wid, CITATION_FIELDS, expected=count, session=session), session)


def walk_windows(wid, count, year, *, session) -> Result:
    return from_s2(s2.citations_windowed(wid, CITATION_FIELDS, expected=count, year_from=year, session=session),
                   session)


def _is_count(v) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def walk_openalex(doi, *, session, select=OA_SELECT) -> Result:
    """Every citer of `doi` from OpenAlex `cites:` with cursor paging. The length check counts the
    rows RETURNED (before W-id de-duplication) against the first or the last page's meta.count;
    either passes (citers without a DOI count toward it and are kept as rows; only DOI rows become
    candidates). A W-id repeated across pages is then dropped (first wins), counted in `duplicates`
    and named in the reason. A failed page fails the seed; a DOI OpenAlex does not hold is
    not_found; a 429 or Remaining 0 is DEFERRED (the session stops, nothing is retried).
    `oa_cited_by` carries the seed's cited_by_count from the free singleton (the count gate's
    input for a seed S2 holds no count for)."""
    rows, seen, dups, first, last, returned, attempts, cited_by = [], set(), [], None, None, 0, 0, None
    for out in openalex.citing_works(doi, select=select, session=session):
        attempts += out.attempts
        if not out.ok:
            if out.kind is Kind.DEFERRED and not out.attempts and session.aborted:
                return Result(str(s2.WalkState.FAILED), "openalex", kind=str(out.kind), reason=out.detail,
                              attempts=attempts, not_sent=True)
            if out.kind is Kind.NO_MATCH:
                return Result(str(s2.WalkState.NOT_FOUND), "openalex", status=out.status,
                              reason=out.detail or "not in OpenAlex", attempts=attempts)
            return Result(str(s2.WalkState.FAILED), "openalex", kind=str(out.kind), status=out.status,
                          reason=out.detail or str(out.kind), attempts=attempts, oa_cited_by=cited_by)
        page = out.payload
        if cited_by is None and _is_count(getattr(page, "cited_by_count", None)):
            cited_by = page.cited_by_count
        if first is None:
            first = page.count
        last = page.count
        for w in page:
            returned += 1
            r = openalex_citing(w)
            key = r["citing_paper_id"]
            if key and key in seen:
                dups.append(key)
                continue
            if key:
                seen.add(key)
            rows.append(r)
    if not _is_count(first):
        return Result(str(s2.WalkState.FAILED), "openalex", kind=str(Kind.ERROR), attempts=attempts,
                      reason="unexpected OpenAlex response: no meta.count", oa_cited_by=cited_by)
    accepted = [c for c in (first, last) if _is_count(c)]
    if returned not in accepted:
        shown = f"meta.count {first}" if last in (None, first) else f"meta.count {first} (first page), {last} (last page)"
        uniq = f" ({len(rows)} unique)" if dups else ""
        return Result(str(s2.WalkState.FAILED), "openalex", kind="COUNT_MISMATCH", attempts=attempts,
                      mismatch=True, reason=f"count_mismatch: {returned} rows{uniq}, {shown}",
                      expected=first, oa_cited_by=cited_by)
    reason = ""
    if dups:
        named = ", ".join(dict.fromkeys(dups))
        reason = f"{len(dups)} W-id(s) repeated across cursor pages dropped: {named}"[:300]
    state = s2.WalkState.EMPTY if not rows else s2.WalkState.COMPLETE
    return Result(str(state), "openalex", rows, attempts=attempts, expected=returned, reason=reason,
                  duplicates=len(dups), oa_cited_by=cited_by)


def walk_id(paper_id, doi) -> str:
    """The id a walk is sent with: the 40-hex S2 paperId when known, else the DOI."""
    return paper_id if isinstance(paper_id, str) and _PAPER_SHA.fullmatch(paper_id) else doi


def resolved(slot):
    """(paperId, citationCount, S2 DOI, year) of a metadata-pass record; None when it has no usable
    count. An Outcome slot (not answered) or None (no S2 record) is the caller's to classify."""
    if not isinstance(slot, dict):
        return None
    n = slot.get("citationCount")
    if isinstance(n, bool) or not isinstance(n, int) or n < 0:
        return None
    ext = slot.get("externalIds")
    year = slot.get("year")
    return (slot.get("paperId"), n, norm_doi(ext.get("DOI")) if isinstance(ext, dict) else "",
            year if isinstance(year, int) and not isinstance(year, bool) else None)


def is_unresolved(slot) -> bool:
    """paper_batch answered null (no S2 record under this DOI) or never sent it (SKIPPED)."""
    return slot is None or (isinstance(slot, Outcome) and slot.kind is Kind.SKIPPED)
