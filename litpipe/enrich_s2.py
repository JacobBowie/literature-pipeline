"""litpipe.enrich_s2: library abstracts, open-access PDF URLs and citation counts from Semantic
Scholar's paper batch (K6; dispatch W3-E; design 2026-09-23 section 4.3; probe P4).

One POST /graph/v1/paper/batch per 500 library DOIs (about 12 calls for the whole portfolio;
unkeyed is fine) through litpipe.s2, one Session per run. DRY RUN by default: it fetches, prints
what a commit would fill per project and the --commit command, and opens the DB read-only.
`--commit` writes in ONE transaction, set-based (a registered DataFrame, then UPDATE ... FROM and
INSERT ... SELECT ... WHERE NOT EXISTS; never executemany):

  s2_enrichment (this module's table)  one row per lower-case DOI: oa_url (openAccessPdf.url, NULL
                                       when empty; P4-C11: read the url, not the status),
                                       citation_count, reference_count, abstract_elided,
                                       fetched_at. A DOI S2 holds no record of gets a row of NULLs
                                       ("not in S2 at fetched_at"), so it is not asked again
                                       before --max-age-days.
  paper_metadata.abstract              only where it is NULL or '', and only from a non-empty S2
                                       abstract that is not publisher-elided, cleaned with
                                       litpipe.text.abstract_field. Nothing else in paper_metadata
                                       is written (not abstract_attempted_at, not
                                       abstract_attempts: those are enrich_abstracts').

Rules (K6):
- A non-empty abstract is never overwritten. The abstract count is checked inside the
  transaction; a drop rolls the whole write back (exit 3).
- Elision: S2 marks a publisher-elided abstract in openAccessPdf.disclaimer ("... have been
  elided by the publisher: {'abstract'} ...": 530 of the 580 abstract-less rows in P4-C9, and 0
  rows with an abstract). Such a DOI is never filled from S2 and is not requested again on later
  runs (--recheck-elided asks again).
- Joins use lower-case DOIs (litpipe.doi.normalise): S2 returns 31 % of DOIs in publisher casing
  (V_P4_P1). The batch answer is aligned with the ids sent; a row whose externalIds.DOI
  normalises to another DOI is not used (counted as a mismatch).
- A failed chunk (429, 5xx, transport, breaker) is typed: its DOIs are not written and their
  previous rows stay.

The table helpers below (write_transaction, upsert, staged, abstract_count) are shared with
enrich_recommendations.py.

Exit codes (shared convention): 0 clean; 1 config error (no registry for --project, an unknown
project, no DB, a bad s2 block); 2 degraded (a chunk failed, a mismatch), after the summary
line; 3 aborted (run budget or breaker, a write rolled back), after the summary line.

Usage (from the repository root):
  python -m litpipe.enrich_s2                       # dry run over every library DOI
  python -m litpipe.enrich_s2 --project <key>       # one project's library
  python -m litpipe.enrich_s2 --commit              # write (close other DB users first)
"""
from __future__ import annotations

import argparse
import contextlib
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import lit_util
from litpipe import config, ledger, s2
from litpipe import doi as _doi
from litpipe import text as _text
from litpipe.outcomes import Outcome

REPO = Path(__file__).resolve().parent.parent
CONFIG_PATH = REPO / "projects.json"
DB_NAME = "portfolio.duckdb"
FIELDS = ("externalIds", "abstract", "openAccessPdf", "citationCount", "referenceCount")
MAX_AGE_DAYS = 30
SUMMARY_MARKER = "[step-summary] "
EXIT_OK, EXIT_CONFIG, EXIT_DEGRADED, EXIT_ABORTED = 0, 1, 2, 3
# Failure kinds that are the network's doing (snowball reads a positive count as DEGRADED).
TRANSPORT_KINDS = frozenset({"TRANSPORT", "OUTAGE", "REFUSED", "DEFERRED"})
REQUIRED_TABLES = ("paper_metadata", "paper_locations")

DDL = """CREATE TABLE IF NOT EXISTS s2_enrichment (
  doi             VARCHAR PRIMARY KEY,  -- litpipe.doi.normalise form (lower case)
  oa_url          VARCHAR,              -- openAccessPdf.url; NULL when S2 has none
  citation_count  INTEGER,
  reference_count INTEGER,
  abstract_elided BOOLEAN,              -- S2's disclaimer: the publisher elided the abstract
  abstract_offered BOOLEAN,             -- S2 returned a usable abstract (non-empty, not elided)
  fetched_at      TIMESTAMPTZ
)"""

_ELIDED = re.compile(r"elided by the publisher:\s*\{([^}]*)\}", re.IGNORECASE)


# ------------------------------------------------------------------------------ shared DB helpers
class AbstractCountDropped(RuntimeError):
    """The abstract count fell inside a write transaction; the write was rolled back."""


def abstract_count(con) -> int:
    """paper_metadata rows with a non-empty abstract (the T1/K6 invariant: never drops)."""
    return con.execute("SELECT count(*) FROM paper_metadata "
                       "WHERE abstract IS NOT NULL AND abstract <> ''").fetchone()[0]


def utc_iso(dt=None) -> str:
    """An ISO instant with an explicit +00:00 offset, safe to CAST to TIMESTAMPTZ under any
    session time zone (the duckdb skill's naive-string double-offset rule)."""
    dt = dt or datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def table_exists(con, name) -> bool:
    return con.execute("SELECT count(*) FROM information_schema.tables "
                       "WHERE table_name = ? AND table_type = 'BASE TABLE'", [name]).fetchone()[0] > 0


@contextlib.contextmanager
def staged(con, name, frame):
    """`frame` (a pandas DataFrame) visible to SQL as `name` for the block."""
    con.register(name, frame)
    try:
        yield name
    finally:
        with contextlib.suppress(Exception):
            con.unregister(name)


@contextlib.contextmanager
def write_transaction(con):
    """BEGIN; the block; the abstract guard; COMMIT. The guard snapshots every non-empty abstract
    (DOI and hash) at BEGIN and fails the write when any of them is gone or changed at the end, or
    the count fell: a net count alone would let a fill mask a loss. Any exception, the guard's
    included, rolls back and propagates: a COMMIT after a failed statement would silently roll
    the batch back anyway (DuckDB 1.5.5), so it is never attempted."""
    con.execute("BEGIN TRANSACTION")
    try:
        before = abstract_count(con)
        con.execute("CREATE OR REPLACE TEMP TABLE _w3e_abstracts AS SELECT doi, hash(abstract) AS h "
                    "FROM paper_metadata WHERE abstract IS NOT NULL AND abstract <> ''")
        yield
        after = abstract_count(con)
        lost = con.execute(
            "SELECT count(*) FROM _w3e_abstracts AS a LEFT JOIN paper_metadata AS m ON m.doi = a.doi "
            "WHERE m.doi IS NULL OR m.abstract IS NULL OR m.abstract = '' OR hash(m.abstract) <> a.h").fetchone()[0]
        con.execute("DROP TABLE _w3e_abstracts")
        if lost or after < before:
            raise AbstractCountDropped(f"{lost} existing abstract(s) would be lost or changed (count {before} "
                                       f"-> {after}); the write was rolled back")
        con.execute("COMMIT")
    except BaseException:
        with contextlib.suppress(Exception):
            con.execute("ROLLBACK")
        raise


def upsert(con, table, src, keys, update, insert) -> tuple[int, int]:
    """Set-based upsert of the staged rows `src` (unique on `keys`) into `table`: UPDATE ... FROM
    for keys already present, then INSERT ... SELECT ... WHERE NOT EXISTS for the rest. Needs no
    PRIMARY KEY on `table` (a carried-over copy without one still works) and never deletes.
    `update` and `insert` map target columns to SQL expressions over the staged alias `s`.
    Returns (rows updated, rows inserted)."""
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    n_upd = 0
    if update:
        sets = ", ".join(f"{c} = {e}" for c, e in update.items())
        n_upd = con.execute(f"UPDATE {table} AS t SET {sets} FROM {src} AS s WHERE {on}").fetchone()[0]
    cols = ", ".join(insert)
    exprs = ", ".join(insert.values())
    n_ins = con.execute(f"INSERT INTO {table} ({cols}) SELECT {exprs} FROM {src} AS s "
                        f"WHERE NOT EXISTS (SELECT 1 FROM {table} AS t WHERE {on})").fetchone()[0]
    return int(n_upd or 0), int(n_ins or 0)


# ------------------------------------------------------------------------------ registry and DB
def registry(config_path, cfg=None) -> dict:
    """The loaded projects.json: `cfg` when given, else the file at `config_path` ({} if absent)."""
    if cfg is not None:
        return config.load(cfg)
    return lit_util.load_projects_config(config_path, missing_ok=True)


def resolve(project, db, cfg, config_path):
    """(registry dict, DB path) for a run. ConfigError for a --project without a registry, an
    unregistered project, or a DB file that does not exist (duckdb.connect would create one)."""
    if project is not None and cfg is None and not Path(config_path).is_file():
        raise config.ConfigError(f"--project needs the registry, and there is none at {config_path}")
    reg = registry(config_path, cfg)
    if project is not None and project not in (reg.get("projects") or {}):
        raise config.ConfigError(f"project {project!r} is not registered in projects.json")
    path = Path(db) if db else config.db_dir(reg) / DB_NAME
    if not path.is_file():
        raise config.ConfigError(f"DB not found: {path}")
    return reg, path


def missing_tables(con, names=REQUIRED_TABLES) -> list:
    return [t for t in names if not table_exists(con, t)]


# ------------------------------------------------------------------------------ S2 rows
def abstract_elided(row) -> bool:
    """True when S2's openAccessPdf.disclaimer says the publisher elided the abstract."""
    pdf = row.get("openAccessPdf") if isinstance(row, dict) else None
    d = pdf.get("disclaimer") if isinstance(pdf, dict) else None
    if not isinstance(d, str):
        return False
    m = _ELIDED.search(d)
    return bool(m and re.search(r"\babstract\b", m.group(1), re.IGNORECASE))


def _int(v):
    return v if isinstance(v, int) and not isinstance(v, bool) else None


def parse_row(row: dict) -> dict:
    """The fields this module stores from one batch record."""
    pdf = row.get("openAccessPdf")
    url = pdf.get("url") if isinstance(pdf, dict) else None
    url = url.strip() if isinstance(url, str) and url.strip() else None
    elided = abstract_elided(row)
    abstract = "" if elided else _text.abstract_field(row.get("abstract") or "")
    return {"found": True, "oa_url": url, "citation_count": _int(row.get("citationCount")),
            "reference_count": _int(row.get("referenceCount")), "abstract_elided": elided,
            "abstract": abstract}


# A DOI S2 holds no record of (a positional null in the batch answer): stored as a row of NULLs.
NOT_IN_S2 = {"found": False, "oa_url": None, "citation_count": None, "reference_count": None,
             "abstract_elided": None, "abstract": ""}


# ------------------------------------------------------------------------------ the pass
def _library(con, project):
    q = "SELECT DISTINCT doi, project FROM paper_locations"
    args = []
    if project is not None:
        q += " WHERE project = ?"
        args.append(project)
    return con.execute(q + " ORDER BY doi, project", args).fetchall()


def _metadata_state(con):
    """{library doi: has a non-empty abstract} for every library DOI with a paper_metadata row."""
    rows = con.execute("SELECT m.doi, (m.abstract IS NOT NULL AND m.abstract <> '') FROM paper_metadata m "
                       "WHERE m.doi IN (SELECT doi FROM paper_locations)").fetchall()
    return {d: bool(h) for d, h in rows}


def _enrichment_state(con, cutoff_iso):
    """{doi: (abstract_elided, fresh)} from s2_enrichment ({} when the table does not exist)."""
    if not table_exists(con, "s2_enrichment"):
        return {}
    rows = con.execute("SELECT doi, abstract_elided, fetched_at >= CAST(? AS TIMESTAMPTZ), "
                       "coalesce(abstract_offered, false) FROM s2_enrichment", [cutoff_iso]).fetchall()
    return {d: (bool(e), bool(f), bool(o)) for d, e, f, o in rows}


def _commit_command(project, db, limit, refresh, max_age_days, recheck_elided) -> str:
    def q(s):
        s = str(s)
        return f'"{s}"' if any(c in s for c in ' &()') else s
    parts = ["uv", "run", "python", "-m", "litpipe.enrich_s2", "--commit"]
    if db:
        parts += ["--db", q(db)]
    if project:
        parts += ["--project", q(project)]
    if limit:
        parts += ["--limit", str(int(limit))]
    if refresh:
        parts.append("--refresh")
    if max_age_days != MAX_AGE_DAYS:
        parts += ["--max-age-days", str(int(max_age_days))]
    if recheck_elided:
        parts.append("--recheck-elided")
    return " ".join(parts)


def _error(msg, step="enrich_s2") -> dict:
    print(f"[ERR] {ledger.redact(msg)}", file=sys.stderr)
    return {"step": step, "exit_code": EXIT_CONFIG, "status": "error", "error": ledger.redact(msg)}


def run(*, db=None, project=None, commit=False, limit=0, refresh=False, max_age_days=MAX_AGE_DAYS,
        recheck_elided=False, session=None, cfg=None, now=None) -> dict:
    """One K6 pass. Returns the summary dict; its `exit_code` is the CLI's exit code."""
    try:
        reg, path = resolve(project, db, cfg, CONFIG_PATH)
        sess = session or s2.Session(cfg=reg)
    except config.ConfigError as e:
        return _error(str(e))
    now = now or datetime.now(timezone.utc)
    cutoff = utc_iso(now - timedelta(days=max(0, int(max_age_days))))

    con = lit_util.connect_db(str(path), on_fail="exit", tries=5, delays=(3,), read_only=not commit)
    try:
        miss = missing_tables(con)
        if miss:
            return _error(f"{path} is not a portfolio index (missing {', '.join(miss)})")
        if commit:
            con.execute(DDL)
        lib = _library(con, project)
        has_abs = _metadata_state(con)
        state = _enrichment_state(con, cutoff)

        by_norm = defaultdict(lambda: {"dois": set(), "projects": set()})
        not_doi = set()
        for d, proj in lib:
            n = _doi.normalise_structured(d)
            if n is None:
                not_doi.add(d)
                continue
            by_norm[n]["dois"].add(d)
            by_norm[n]["projects"].add(proj)
        targets, skipped_elided, skipped_fresh = [], [], []
        for n in sorted(by_norm):
            elided, fresh, offered = state.get(n, (False, False, False))
            lost = offered and any(not has_abs.get(d, True) for d in by_norm[n]["dois"])
            if elided and not recheck_elided:
                skipped_elided.append(n)
            elif fresh and not refresh and n in state and not lost:
                skipped_fresh.append(n)
            else:
                targets.append(n)
        if limit:
            targets = targets[:int(limit)]

        mode = "COMMIT" if commit else "DRY RUN (nothing written; DB opened read-only)"
        print(f"[enrich_s2] {mode}")
        print(f"DB:         {path}")
        print(f"scope:      {('project ' + project) if project else 'every project'}")
        print(f"library:    {len(by_norm)} distinct DOIs ({len(not_doi)} values that are not a DOI)")
        print(f"skipped:    {len(skipped_fresh)} fetched within {max_age_days} d, "
              f"{len(skipped_elided)} with an elided abstract")
        n_calls = -(-len(targets) // s2.BATCH_MAX_IDS)
        print(f"requesting: {len(targets)} DOIs in {n_calls} batch call(s); {s2.key_status()}\n")

        out = s2.paper_batch(targets, FIELDS, session=sess) if targets else None
        slots = out.payload if out is not None else []
        rows, failed, mismatch, unresolved = {}, [], [], []
        for n, slot in zip(targets, slots):
            if isinstance(slot, Outcome):
                failed.append((n, slot))
                continue
            if slot is None:
                rows[n] = dict(NOT_IN_S2)
                unresolved.append(n)
                continue
            ext = slot.get("externalIds") if isinstance(slot.get("externalIds"), dict) else {}
            back = _doi.normalise_structured(ext.get("DOI")) if ext.get("DOI") else None
            if back is not None and back != n:
                mismatch.append(n)
                continue
            rows[n] = parse_row(slot)

        fill = []                                   # (library doi, abstract)
        per = defaultdict(Counter)
        for n, info in by_norm.items():
            for proj in info["projects"]:
                per[proj]["dois"] += 1
        for n in targets:
            info = by_norm[n]
            r = rows.get(n)
            for proj in info["projects"]:
                c = per[proj]
                c["requested"] += 1
                if r is None:
                    c["failed"] += 1
                    continue
                if not r["found"]:
                    c["not_in_s2"] += 1
                    continue
                c["s2_record"] += 1
                if r["oa_url"]:
                    c["oa_url"] += 1
                missing_local = any(not has_abs.get(d, True) for d in info["dois"])
                if r["abstract_elided"] and missing_local:
                    c["elided_missing"] += 1
                if r["abstract"] and missing_local:
                    c["would_fill"] += 1
            if r is not None and r["abstract"]:
                for d in sorted(info["dois"]):
                    if d in has_abs and not has_abs[d]:
                        fill.append((d, r["abstract"]))

        cols = ("dois", "requested", "s2_record", "would_fill", "elided_missing", "oa_url", "not_in_s2", "failed")
        print(f"  {'project':<34}" + "".join(f"{c:>15}" for c in cols))
        for proj in sorted(per):
            print(f"  {proj[:34]:<34}" + "".join(f"{per[proj][c]:>15}" for c in cols))
        print()

        written = {"s2_enrichment_updated": 0, "s2_enrichment_inserted": 0, "abstracts_filled": 0}
        abstracts_before = abstracts_after = abstract_count(con)
        write_error = ""
        if commit and rows:
            try:
                written = _write(con, rows, fill, now)
            except Exception as e:
                write_error = f"{type(e).__name__}: {ledger.redact(e)}"
                print(f"  [db] the write was rolled back ({write_error})", file=sys.stderr)
            abstracts_after = abstract_count(con)
    finally:
        con.close()

    aborted = sess.aborted or ("write_rolled_back" if write_error else None)
    reasons = []
    if failed:
        reasons.append(f"{len(failed)} DOI(s) in failed batch chunk(s): {failed[0][1].kind} {failed[0][1].detail[:120]}")
    if mismatch:
        reasons.append(f"{len(mismatch)} batch row(s) answered for another DOI")
    if write_error:
        reasons.append(f"write rolled back: {write_error}")
    code = EXIT_ABORTED if aborted else (EXIT_DEGRADED if reasons else EXIT_OK)
    transport = sum(1 for _, o in failed if str(o.kind) in TRANSPORT_KINDS)
    res = {"step": "enrich_s2", "exit_code": code,
           "status": "aborted" if aborted else ("degraded" if reasons else "ok"),
           "dry_run": not commit, "db": str(path), "project": project,
           "library_dois": len(by_norm), "not_a_doi": len(not_doi), "requested": len(targets),
           "skipped_fresh": len(skipped_fresh), "skipped_elided": len(skipped_elided),
           "s2_records": sum(1 for r in rows.values() if r["found"]),
           "not_in_s2": len(unresolved), "failed": len(failed), "mismatch": len(mismatch),
           "elided": sum(1 for r in rows.values() if r["abstract_elided"]),
           "oa_urls": sum(1 for r in rows.values() if r["oa_url"]),
           "would_fill": len({d for d, _ in fill}), **written,
           "abstracts_before": abstracts_before, "abstracts_after": abstracts_after,
           "reasons": reasons, "aborted": aborted, "transport_failures": transport,
           "per_project": {p: dict(c) for p, c in sorted(per.items())}, "s2": sess.summary()}
    _print_summary(res, commit, project, db, limit, refresh, max_age_days, recheck_elided)
    return res


def _write(con, rows, fill, now) -> dict:
    import pandas as pd
    ts = utc_iso(now)
    ef = pd.DataFrame([{"doi": n, "oa_url": r["oa_url"], "citation_count": r["citation_count"],
                        "reference_count": r["reference_count"], "abstract_elided": r["abstract_elided"],
                        "abstract_offered": bool(r["abstract"]),
                        "ts": ts} for n, r in sorted(rows.items())])
    ef["citation_count"] = ef["citation_count"].astype("Int64")
    ef["reference_count"] = ef["reference_count"].astype("Int64")
    ef["abstract_elided"] = ef["abstract_elided"].astype("boolean")
    ef["abstract_offered"] = ef["abstract_offered"].astype("boolean")
    seen, uniq = set(), []
    for d, a in fill:
        if d not in seen:
            seen.add(d)
            uniq.append((d, a))
    ff = pd.DataFrame(uniq, columns=["doi", "abstract"], dtype=object)
    values = {"oa_url": "s.oa_url", "citation_count": "s.citation_count",
              "reference_count": "s.reference_count", "abstract_elided": "s.abstract_elided",
              "abstract_offered": "s.abstract_offered",
              "fetched_at": "CAST(s.ts AS TIMESTAMPTZ)"}
    with write_transaction(con):
        with staged(con, "_w3e_enrich", ef):
            n_upd, n_ins = upsert(con, "s2_enrichment", "_w3e_enrich", ["doi"], values,
                                  {"doi": "s.doi", **values})
        n_fill = 0
        if uniq:
            with staged(con, "_w3e_fill", ff):
                n_fill = con.execute(
                    "UPDATE paper_metadata AS t SET abstract = s.abstract FROM _w3e_fill AS s "
                    "WHERE t.doi = s.doi AND (t.abstract IS NULL OR t.abstract = '') "
                    "AND s.abstract IS NOT NULL AND s.abstract <> ''").fetchone()[0]
    return {"s2_enrichment_updated": n_upd, "s2_enrichment_inserted": n_ins, "abstracts_filled": int(n_fill or 0)}


def _print_summary(res, commit, project, db, limit, refresh, max_age_days, recheck_elided):
    print("=== summary ===")
    print(f"  requested:          {res['requested']} ({res['skipped_fresh']} fresh and "
          f"{res['skipped_elided']} elided not requested)")
    print(f"  S2 records:         {res['s2_records']}  (not in S2: {res['not_in_s2']}, failed: {res['failed']}, "
          f"mismatch: {res['mismatch']})")
    print(f"  elided abstracts:   {res['elided']}")
    print(f"  OA PDF URLs:        {res['oa_urls']}")
    if commit:
        print(f"  abstracts filled:   {res['abstracts_filled']} (count {res['abstracts_before']} -> "
              f"{res['abstracts_after']})")
        print(f"  s2_enrichment:      {res['s2_enrichment_inserted']} new, {res['s2_enrichment_updated']} refreshed")
    else:
        print(f"  would fill:         {res['would_fill']} abstracts (count now {res['abstracts_before']})")
        print()
        print("  To write: close other users of the DB (index_portfolio, snowball, enrich_*), check that")
        print("  Google Drive sync is not holding it, then run from the repository root:")
        print(f"    {_commit_command(project, db, limit, refresh, max_age_days, recheck_elided)}")
    if res["status"] != "ok":
        print(f"  [{res['status'].upper()}] {'; '.join(res['reasons'] or [str(res['aborted'])])}")
    s = res["s2"]
    print(f"  [s2] {s.get('key')} calls={s.get('calls')} attempts={s.get('attempts')} "
          f"not_ok_attempts={s.get('attempts_not_ok')} budget={s.get('budget')}"
          + (f" ABORTED={s.get('aborted')}" if s.get("aborted") else ""))
    small = {k: v for k, v in res.items() if k not in ("s2", "per_project")}
    small["s2"] = {k: s.get(k) for k in ("calls", "attempts", "attempts_not_ok", "budget", "aborted")}
    print(SUMMARY_MARKER + json.dumps(small, ensure_ascii=False), flush=True)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m litpipe.enrich_s2",
                                 description="Library abstracts, OA PDF URLs and citation counts from "
                                             "Semantic Scholar's paper batch (K6). Dry run unless --commit.")
    ap.add_argument("--db", default=None,
                    help="DuckDB portfolio index (default: portfolio.duckdb in projects.json db_dir).")
    ap.add_argument("--project", default=None, help="Only this registered project's library DOIs.")
    ap.add_argument("--commit", action="store_true",
                    help="Write s2_enrichment and fill empty abstracts, in one transaction.")
    ap.add_argument("--limit", type=int, default=0, help="Request the first N target DOIs only (testing).")
    ap.add_argument("--refresh", action="store_true",
                    help="Request DOIs fetched within --max-age-days too (elided ones still need --recheck-elided).")
    ap.add_argument("--max-age-days", type=int, default=MAX_AGE_DAYS,
                    help=f"Re-request a DOI only when its s2_enrichment row is older (default {MAX_AGE_DAYS}).")
    ap.add_argument("--recheck-elided", action="store_true",
                    help="Also request DOIs whose abstract S2 marked as publisher-elided.")
    args = ap.parse_args(argv)
    res = run(db=args.db, project=args.project, commit=args.commit, limit=args.limit,
              refresh=args.refresh, max_age_days=args.max_age_days, recheck_elided=args.recheck_elided)
    return res["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
