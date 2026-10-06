"""Seed a draft lit_pull_queue from the portfolio index.

Reads the index (portfolio.duckdb, opened read-only) for one registered project, ranks the
candidates it does not hold yet, applies the filters below, and writes a draft queue at the
project root: `lit_pull_queue.draft.csv`, or `lit_pull_queue.<tag>.draft.csv` with --tag. A draft
opens with a `# REVIEW BEFORE SWEEP` header (sweep skips leading `#` lines) and is never swept
under its draft name (`draft` is a reserved tag; a tagged draft name is no queue name). Review it,
drop irrelevant rows, then rename it to `lit_pull_queue.csv` (or `lit_pull_queue.<tag>.csv`) and
run `sweep.py --project NAME`.

Ranks (--rank, default project):
  project     the project's own seeds: candidates rows with source_project = the key (one key per
              row, so a parent's count excludes its subprojects). Ties: max_cited_by, then the
              portfolio count, then DOI. --min-seeds counts own seeds (default 1).
  portfolio   the pre-2026-10 order: the top_candidates view's portfolio-wide seed count
              (n_seeds_pointing) for candidates this project's walks found, then max_cited_by.
              --min-seeds counts portfolio seeds (default 3).
  cocitation  index_portfolio.project_cocitations(con, project) order (n_own_citing, then DOI),
              rows another project holds dropped. --min-seeds counts n_own_citing (default 1).
--scope NAME draws from scoped_candidates for (project, NAME) instead, ordered by scope seeds,
citing_cited_by, year (newest first), then DOI; --min-seeds counts scope seeds (default 1).

Every rank, and --scope, excludes a DOI held anywhere in the portfolio (any paper_locations row,
text-only holdings included). --year-min, --min-cites and the title filter (a title that starts
with a letter) apply to every rank. A candidate with no year (NULL or 0 in the index) is dropped
and counted; --include-undated keeps it. --recent-first sorts by year, newest first (undated
last), then by the rank: an open copy exists more often for recent papers, but the effect is
topic-dependent, so it is opt-in. The draft header names the order used.

--pmc-check looks every draft DOI up once through lit_net.doi_to_pmcid (idconv unless its host is
refused, then Europe PMC, then E-utilities) and adds two columns at the end: `pmcid`, and
`pmc_status` (`OK`, or lit_net.lookup_status: NO_PMCID only when a service answered). Off by
default: without it the seeder sends nothing.

The destination column is the library relative to the project root (tail-aware, so a
subproject's tail is never doubled); a library outside its project root exits 1 unless
--destination is given. The DB is --db, else <db_dir>/portfolio.duckdb from projects.json
(default <root>/_references), resolved at call time.

Exit codes: 0 clean; 1 usage or config (registry missing, unknown project, no index, an index
without the view or table a rank needs); 2 when --pmc-check lookups failed for some rows (the draft
is written; their pmc_status names the failure). The last stdout line is "[step-summary] {json}".

Usage:
  python seed_queue_from_top_candidates.py --project research_a
  python seed_queue_from_top_candidates.py --project research_a --rank portfolio --min-seeds 5 --limit 50
  python seed_queue_from_top_candidates.py --project teaching_b --scope ch07 --recent-first --pmc-check
"""
import argparse
import csv
import io
import json
import os
import re
import sys
from pathlib import Path, PurePath

import lit_util  # RC4: atomic_write_text for crash-safe draft writes
from litpipe import config, ledger

lit_util.utf8_stdout()

HERE = Path(__file__).parent
CONFIG_PATH = HERE / "projects.json"
# An override only (scripts and tests). None: --db, else config.db_dir(registry)/DB_NAME, resolved
# at call time (an import-time path would bind the live DB before a test can patch the root).
DB_PATH = None
DB_NAME = "portfolio.duckdb"
SUMMARY_MARKER = "[step-summary] "

EXIT_OK, EXIT_CONFIG, EXIT_DEGRADED = 0, 1, 2
RANKS = ("project", "portfolio", "cocitation")
DEFAULT_RANK = "project"
DEFAULT_MIN_SEEDS = {"project": 1, "portfolio": 3, "cocitation": 1, "scope": 1}
QUEUE_PREFIX = "lit_pull_queue"
BASE_COLUMNS = ["doi", "title", "authors", "year", "destination", "notes"]   # the queue contract
PMC_COLUMNS = ["pmcid", "pmc_status"]
INDEX_COMMAND = "python index_portfolio.py --project {key}"

ORDER_TEXT = {
    "project": "n_seeds_project desc, max_cited_by desc, n_seeds_portfolio desc, doi",
    "portfolio": "n_seeds_portfolio desc, max_cited_by desc, doi",
    "cocitation": "n_own_citing desc, doi (project_cocitations order)",
    "scope": "n_seeds_scope desc, citing_cited_by desc, year desc, doi",
}
_ORDER_SQL = {
    "project": "s.rank_n DESC, s.cites DESC NULLS LAST, s.n_port DESC, s.doi",
    "portfolio": "s.rank_n DESC, s.cites DESC NULLS LAST, s.doi",
    "cocitation": "s.rank_n DESC, s.doi",
    "scope": "s.rank_n DESC, s.cites DESC NULLS LAST, s.year DESC NULLS LAST, s.doi",
}
RECENT_SQL = "(COALESCE(s.year, 0) <= 0), s.year DESC NULLS LAST, "
SEED_COLUMN = {"project": "n_seeds_project", "portfolio": "n_seeds_portfolio",
               "cocitation": "n_own_citing", "scope": "n_seeds_scope"}

# ---------------------------------------------------------------- SQL
_OWN = ("own AS (SELECT doi, COUNT(DISTINCT source_seed_doi) AS n_own FROM candidates "
        "WHERE source_project = $key GROUP BY doi)")
_PORT = ("port AS (SELECT doi, COUNT(DISTINCT source_seed_doi) AS n_port, "
         "MAX(citing_cited_by) AS max_cites, "
         "STRING_AGG(DISTINCT source_type, ',' ORDER BY source_type) AS sources "
         "FROM candidates GROUP BY doi)")
_SCOPED = ("sc AS (SELECT doi, COUNT(DISTINCT source_seed_doi) AS n_scope, "
           "MAX(citing_cited_by) AS cites, MAX(NULLIF(year, 0)) AS year, "
           "MAX(NULLIF(title, '')) AS title, MAX(NULLIF(authors, '')) AS authors "
           "FROM scoped_candidates WHERE project = $key AND scope = $scope GROUP BY doi)")
# Every source yields: doi, title, authors, year, n_own, n_port, cites, sources, rank_n, n_extra.
_SOURCE = {
    "project": """
        SELECT o.doi, m.title, m.authors, m.year, o.n_own, p.n_port, p.max_cites AS cites,
               p.sources, o.n_own AS rank_n, CAST(NULL AS BIGINT) AS n_extra
        FROM own o JOIN port p ON p.doi = o.doi
        LEFT JOIN paper_metadata m ON m.doi = o.doi""",
    "portfolio": """
        SELECT t.doi, t.title, m.authors, t.year, COALESCE(o.n_own, 0) AS n_own,
               t.n_seeds_pointing AS n_port, t.max_cited_by AS cites, t.sources,
               t.n_seeds_pointing AS rank_n, CAST(NULL AS BIGINT) AS n_extra
        FROM top_candidates t
        LEFT JOIN paper_metadata m ON m.doi = t.doi
        LEFT JOIN own o ON o.doi = t.doi
        WHERE (',' || t.via_projects || ',') LIKE $needle""",
    "cocitation": """
        SELECT pc.doi, m.title, m.authors, m.year, COALESCE(o.n_own, 0) AS n_own,
               COALESCE(p.n_port, 0) AS n_port, COALESCE(p.max_cites, 0) AS cites,
               COALESCE(p.sources, 'cites') AS sources, pc.n_own_citing AS rank_n,
               pc.n_own_citing AS n_extra
        FROM project_cocitations pc
        LEFT JOIN paper_metadata m ON m.doi = pc.doi
        LEFT JOIN own o ON o.doi = pc.doi
        LEFT JOIN port p ON p.doi = pc.doi
        WHERE pc.project = $key AND NOT pc.held_anywhere""",
    "scope": """
        SELECT sc.doi, COALESCE(sc.title, m.title) AS title, COALESCE(sc.authors, m.authors) AS authors,
               COALESCE(sc.year, NULLIF(m.year, 0)) AS year, COALESCE(o.n_own, 0) AS n_own,
               COALESCE(p.n_port, 0) AS n_port, sc.cites, 'scope:' || $scope AS sources,
               sc.n_scope AS rank_n, sc.n_scope AS n_extra
        FROM sc
        LEFT JOIN paper_metadata m ON m.doi = sc.doi
        LEFT JOIN own o ON o.doi = sc.doi
        LEFT JOIN port p ON p.doi = sc.doi""",
}
# Held anywhere (today's top_candidates rule: any paper_locations row, text-only included), the
# title filter, the seed and citation floors. The year clause is added per query.
_FILTERS = """
    NOT EXISTS (SELECT 1 FROM paper_locations l WHERE l.doi = s.doi)
    AND s.title IS NOT NULL AND substr(s.title, 1, 1) ~ '[A-Za-z]'
    AND s.rank_n >= $min_seeds
    AND COALESCE(s.cites, 0) >= $min_cites"""
UNDATED = "COALESCE(s.year, 0) <= 0"
_YEAR = {
    False: "COALESCE(s.year, 0) > 0 AND s.year >= $year_min",
    True: "(COALESCE(s.year, 0) <= 0 OR s.year >= $year_min)",
}
_PARAM_RE = re.compile(r"\$([a-z_]+)")


class SeedError(config.ConfigError):
    """A usage or config problem: exit 1 with a one-line message."""


# ---------------------------------------------------------------- registry, paths, names
def load_registry(cfg=None) -> dict:
    """projects.json through litpipe.config.load: `cfg` when given, else this module's
    CONFIG_PATH. ConfigError (exit 1) when the file is missing or unreadable, never the exit 2 of
    ris_emit.load_projects_config."""
    if cfg is not None:
        reg = config.load(cfg)
    else:
        p = Path(CONFIG_PATH)
        if not p.exists():
            raise SeedError(f"projects.json not found at {p} (copy projects.json.template)")
        try:
            reg = config.load(lit_util.load_projects_config(p))
        except (OSError, ValueError) as e:
            raise SeedError(f"projects.json at {p} is unreadable: {ledger.redact(e)}") from None
    if not isinstance(reg, dict) or not isinstance(reg.get("projects") or {}, dict):
        raise SeedError("projects.json has no `projects` object")
    return reg


def project_entry(key, registry) -> dict:
    projects = registry.get("projects") or {}
    if key not in projects:
        raise SeedError(f"'{key}' not in projects.json")
    return projects[key] or {}


def destination_for(key, entry):
    """(destination, None), or (None, why) when the registry library is not inside the project
    root. The destination is the library relative to lit_util.project_root, POSIX, with a
    trailing slash: exactly what sweep resolves and checks against lit_util.lib_paths."""
    if not entry.get("lib_dir"):
        return None, f"projects.json gives '{key}' no lib_dir"
    root = lit_util.project_root(key, entry)
    lib = lit_util.lib_paths(key, entry)[1]
    try:
        rel = os.path.relpath(lib, root)
    except ValueError:                                     # another drive (Windows)
        return None, f"the library {lib} is not under the project root {root}"
    dest = PurePath(rel).as_posix().rstrip("/") + "/"
    root_r, lib_r = root.resolve(), (root / dest).resolve()
    try:
        lib_r.relative_to(root_r)
    except ValueError:
        return None, f"the library {lib} is outside the project root {root}"
    if lib_r != lib.resolve():
        return None, f"{dest!r} under {root} does not resolve to the library {lib}"
    return dest, None


def draft_name(tag=None) -> str:
    return f"{QUEUE_PREFIX}.{tag}.draft.csv" if tag else f"{QUEUE_PREFIX}.draft.csv"


def final_name(tag=None) -> str:
    return f"{QUEUE_PREFIX}.{tag}.csv" if tag else f"{QUEUE_PREFIX}.csv"


def check_tag(tag):
    if tag is None:
        return None
    import sweep   # lazy: only a tagged draft needs sweep's tag rule
    if not sweep.is_valid_tag(tag):
        raise SeedError(f"--tag {tag!r} is not a valid queue tag (lower-case letter first, then "
                        f"[a-z0-9_-], at most 32 characters, not date-like, not reserved)")
    return tag


def resolve_db(db, registry) -> Path:
    if db:
        return Path(db)
    if DB_PATH is not None:
        return Path(DB_PATH)
    return config.db_dir(registry) / DB_NAME


# ---------------------------------------------------------------- queries
def _params(sql, values):
    used = set(_PARAM_RE.findall(sql))
    return {k: v for k, v in values.items() if k in used}


def _relations(con) -> set:
    return {r[0] for r in con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_catalog = current_database() AND table_schema = 'main'").fetchall()}


def check_schema(con, key, mode):
    have = _relations(con)
    base = {"candidates", "paper_locations", "paper_metadata"}
    if not base <= have:
        raise SeedError(f"not a portfolio index (missing {sorted(base - have)})")
    need = {"portfolio": "top_candidates", "cocitation": "project_cocitations",
            "scope": "scoped_candidates"}.get(mode)
    if need and need not in have:
        what = {"cocitation": "--rank cocitation", "scope": "--scope"}.get(mode, f"--rank {mode}")
        raise SeedError(f"{what} needs the `{need}` {'view' if need != 'scoped_candidates' else 'table'}, "
                        f"which this index lacks (indexed before W3-C1's schema); re-index first: "
                        f"{INDEX_COMMAND.format(key=key)}")


def _with(mode) -> str:
    ctes = [_OWN, _PORT] + ([_SCOPED] if mode == "scope" else [])
    return "WITH " + ",\n".join(ctes) + "\n"


def select_rows(con, *, key, mode, scope, year_min, min_seeds, min_cites, limit, recent_first,
                include_undated):
    """(rows, total matching, undated count). Each row is a dict."""
    values = {"key": key, "scope": scope, "needle": f"%,{key},%", "year_min": year_min,
              "min_seeds": min_seeds, "min_cites": min_cites, "limit": limit}
    src = _with(mode) + f"SELECT * FROM ({_SOURCE[mode]}) s WHERE {_FILTERS}"
    order = (RECENT_SQL if recent_first else "") + _ORDER_SQL[mode]
    sql = f"{src} AND {_YEAR[include_undated]} ORDER BY {order} LIMIT $limit"
    cols = ["doi", "title", "authors", "year", "n_own", "n_port", "cites", "sources", "rank_n", "n_extra"]
    rows = [dict(zip(cols, r)) for r in con.execute(sql, _params(sql, values)).fetchall()]
    count_sql = f"SELECT COUNT(*) FROM ({src} AND {_YEAR[include_undated]}) q"
    total = con.execute(count_sql, _params(count_sql, values)).fetchone()[0]
    undated_sql = f"SELECT COUNT(*) FROM ({src} AND {UNDATED}) q"
    undated = con.execute(undated_sql, _params(undated_sql, values)).fetchone()[0]
    return rows, total, undated


def scope_known(con, key) -> list:
    """The scopes the index holds for `key` (empty when none)."""
    return [r[0] for r in con.execute(
        "SELECT DISTINCT scope FROM scoped_candidates WHERE project = ? ORDER BY scope", [key]).fetchall()]


# ---------------------------------------------------------------- PMC check
def check_pmc(rows, *, state=None, cfg=None):
    """One batched lit_net.doi_to_pmcid call over the rows' DOIs. Sets row['pmcid'] and
    row['pmc_status'] in place; returns {failed, transport, by_status, n}. A refused or failed
    lookup keeps its lookup_status token (HTTP_429, DEFERRED, ...), never NO_PMCID."""
    import lit_net
    from litpipe.outcomes import Kind
    answered = (Kind.OK, Kind.NO_MATCH, Kind.EMBARGOED)
    dois = [r["doi"] for r in rows if r["doi"]]
    try:
        res = lit_net.doi_to_pmcid(dois, state=state, cfg=cfg) if dois else {}
    except Exception as e:                    # never a silent "no PMCID" for a crashed lookup
        res, crash = None, f"ERROR: {type(e).__name__}: {ledger.redact(str(e))}"[:200]
    else:
        crash = None
    failed = transport = 0
    by_status = {}
    for r in rows:
        o = None if res is None else res.get((r["doi"] or "").strip().lower())
        if res is None or o is None:
            r["pmcid"], r["pmc_status"] = "", crash or "ERROR: not looked up"
            failed += 1
        else:
            hit = o.payload
            r["pmcid"] = (getattr(hit, "pmcid", None) or "") if hit is not None else ""
            r["pmc_status"] = "OK" if o.kind is Kind.OK else lit_net.lookup_status(o)
            if o.kind not in answered:
                failed += 1
                transport += o.kind is Kind.TRANSPORT
        key = r["pmc_status"].split(":")[0].split(" ")[0]
        by_status[key] = by_status.get(key, 0) + 1
    return {"n": len(rows), "failed": failed, "transport": transport, "by_status": by_status}


# ---------------------------------------------------------------- output
def _note(r, mode, scope):
    parts = [f"n_seeds_project={r['n_own'] or 0}", f"n_seeds_portfolio={r['n_port'] or 0}",
             f"cites={r['cites'] or 0}", f"src={r['sources'] or ''}"]
    if mode == "cocitation":
        parts.append(f"n_own_citing={r['n_extra']}")
    if mode == "scope":
        parts += [f"scope={scope}", f"n_seeds_scope={r['n_extra']}"]
    return "; ".join(parts)


def _year_cell(y):
    return str(y) if isinstance(y, int) and y > 0 else ""


def render(rows, *, key, mode, scope, destination, out_name, tag, year_min, min_seeds, min_cites,
           limit, recent_first, include_undated, n_undated, total, db_path, pmc):
    extra = PMC_COLUMNS if pmc is not None else []
    columns = BASE_COLUMNS + extra
    order = ("year desc (undated last), then " if recent_first else "") + ORDER_TEXT[mode]
    if mode == "scope":
        source = (f"scoped_candidates, project={key}, scope={scope} (the index has no OA column, so "
                  f"this order differs from the walker's _{scope}_descendants.csv only by OA)")
    else:
        source = {"project": "candidates (this project's own seeds)",
                  "portfolio": "top_candidates (portfolio-wide seed counts)",
                  "cocitation": "project_cocitations (this project's own reference lists)"}[mode]
        source += f", project={key}, rank={mode}"
    undated = (f"{n_undated} candidate(s) with no year kept" + (" (sorted last)" if recent_first else "")
               if include_undated else
               f"{n_undated} candidate(s) with no year dropped (--include-undated keeps them)")
    buf = io.StringIO()
    buf.write(f"# REVIEW BEFORE SWEEP -- drop irrelevant rows, then `mv {out_name} {final_name(tag)}`\n")
    buf.write(f"# Source: {source}; index {db_path.name}\n")
    buf.write(f"# Order: {order}\n")
    buf.write(f"# Filters: year>={year_min}, {SEED_COLUMN[mode]}>={min_seeds}, max_cited_by>={min_cites}, "
              f"title starts with a letter, DOIs held anywhere excluded, limit={limit}\n")
    buf.write(f"# Undated: {undated}\n")
    if pmc is not None:
        buf.write(f"# PMC check: {pmc['n'] - pmc['failed']} of {pmc['n']} answered; "
                  f"{pmc['failed']} lookup(s) failed (pmc_status names the failure, never NO_PMCID)\n")
    buf.write(f"# Total rows: {len(rows)} (of {total} matching)\n")
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(columns)
    for r in rows:
        cells = [r["doi"], (r["title"] or "").strip(), r["authors"] or "", _year_cell(r["year"]),
                 destination, _note(r, mode, scope)]
        if pmc is not None:
            cells += [r.get("pmcid") or "", r.get("pmc_status") or ""]
        w.writerow(cells)
    return buf.getvalue()


def _summary(res):
    keys = ("step", "exit_code", "status", "reasons", "aborted", "transport_failures", "project", "rank",
            "scope", "order", "min_seeds", "output", "db", "destination", "rows", "total_matching",
            "undated", "pmc")
    print(SUMMARY_MARKER + json.dumps({k: res.get(k) for k in keys}, ensure_ascii=False), flush=True)


def _error(msg, **kw) -> dict:
    msg = ledger.redact(str(msg))
    print(f"[ERR] {msg}", file=sys.stderr)
    res = {"step": "seed_queue", "exit_code": EXIT_CONFIG, "status": "error", "error": msg,
           "reasons": [msg], "aborted": None, "transport_failures": 0, **kw}
    _summary(res)
    return res


# ---------------------------------------------------------------- run
def run(*, project, rank=None, scope=None, year_min=2010, min_seeds=None, min_cites=0, limit=100,
        destination=None, output=None, tag=None, recent_first=False, include_undated=False,
        pmc_check=False, db=None, cfg=None, state=None) -> dict:
    """One draft. Returns the summary dict; its `exit_code` is the CLI's exit code. `cfg` (a
    loaded projects.json) replaces CONFIG_PATH; `state` goes to lit_net.doi_to_pmcid."""
    if scope is not None and rank is not None:
        return _error(f"--scope has its own order ({ORDER_TEXT['scope']}); --rank does not apply to it",
                      project=project)
    mode = "scope" if scope is not None else (rank or DEFAULT_RANK)
    if mode not in RANKS + ("scope",):
        return _error(f"--rank must be one of {', '.join(RANKS)}", project=project)
    seeds_min = DEFAULT_MIN_SEEDS[mode] if min_seeds is None else min_seeds
    if limit is None or limit < 0:
        return _error("--limit must be 0 or more", project=project)
    try:
        registry = load_registry(cfg)
        entry = project_entry(project, registry)
        tag = check_tag(tag)
        if destination:
            dest = destination if destination.endswith("/") else destination + "/"
        else:
            dest, why = destination_for(project, entry)
            if dest is None:
                raise SeedError(f"{why}: sweep would refuse this destination; pass --destination "
                                f"to set one explicitly")
        out_path = Path(output) if output else lit_util.project_root(project, entry) / draft_name(tag)
        db_path = resolve_db(db, registry)
    except config.ConfigError as e:
        return _error(e, project=project)
    if not db_path.is_file():
        return _error(f"no index at {db_path}; build it first: {INDEX_COMMAND.format(key=project)}",
                      project=project, db=str(db_path))

    import duckdb
    con = lit_util.connect_db(str(db_path), read_only=True)  # RC10 retry-open; never writes
    try:
        check_schema(con, project, mode)
        known = scope_known(con, project) if mode == "scope" else []
        if mode == "scope" and scope not in known:
            raise SeedError(f"the index holds no scope {scope!r} for '{project}' "
                            f"(known: {', '.join(known) or 'none'})")
        rows, total, n_undated = select_rows(
            con, key=project, mode=mode, scope=scope, year_min=year_min, min_seeds=seeds_min,
            min_cites=min_cites, limit=limit, recent_first=recent_first, include_undated=include_undated)
    except config.ConfigError as e:
        return _error(e, project=project, db=str(db_path))
    except duckdb.Error as e:
        return _error(f"index query failed on {db_path}: {type(e).__name__}: {e}", project=project,
                      db=str(db_path))
    finally:
        con.close()

    pmc = check_pmc(rows, state=state, cfg=registry) if pmc_check else None
    text = render(rows, key=project, mode=mode, scope=scope, destination=dest, out_name=out_path.name,
                  tag=tag, year_min=year_min, min_seeds=seeds_min, min_cites=min_cites, limit=limit,
                  recent_first=recent_first, include_undated=include_undated, n_undated=n_undated,
                  total=total, db_path=db_path, pmc=pmc)
    # Built in memory, written atomically (RC4): a crash mid-write must never leave a truncated
    # draft that the stager would treat as a valid queue.
    out_path.parent.mkdir(parents=True, exist_ok=True)
    lit_util.atomic_write_text(str(out_path), text)

    reasons = []
    if pmc is not None and pmc["failed"]:
        detail = ", ".join(f"{k}: {v}" for k, v in sorted(pmc["by_status"].items()) if k not in
                           ("OK", "NO_PMCID", "EMBARGOED"))
        reasons.append(f"pmc-check: {pmc['failed']} of {pmc['n']} lookup(s) failed ({detail})")
    code = EXIT_DEGRADED if reasons else EXIT_OK
    res = {"step": "seed_queue", "exit_code": code, "status": "degraded" if reasons else "ok",
           "reasons": reasons, "aborted": None,
           "transport_failures": pmc["transport"] if pmc is not None else 0,
           "project": project, "rank": mode if mode != "scope" else None, "scope": scope,
           "order": ("year desc, then " if recent_first else "") + ORDER_TEXT[mode],
           "output": str(out_path), "db": str(db_path), "destination": dest,
           "rows": len(rows), "total_matching": total, "undated": n_undated,
           "include_undated": include_undated, "min_seeds": seeds_min, "pmc": pmc,
           "dois": [r["doi"] for r in rows]}

    print(f"Wrote {len(rows)} draft rows to {out_path}")
    print(f"  rank: {mode if mode != 'scope' else 'scope ' + scope}; order: {res['order']}")
    if include_undated:
        print(f"  undated: {n_undated} candidate(s) with no year kept")
    else:
        print(f"  undated: {n_undated} candidate(s) with no year dropped (--include-undated keeps them)")
    if total > limit:
        print(f"[WARN] {total} candidates passed the filters but --limit={limit} truncated the draft "
              f"to the top {limit}. Raise --limit (or tighten --min-seeds/--year-min) to see the rest.",
              file=sys.stderr)
    if mode == "project" and not rows and total == 0:
        print(f"  no candidate from '{project}''s own seeds passed the filters; has its walk been "
              f"indexed? (--rank portfolio ranks by the whole portfolio)")
    if pmc is not None:
        print(f"  pmc-check: {pmc['n'] - pmc['failed']} of {pmc['n']} answered; {pmc['failed']} failed "
              f"({', '.join(f'{k}={v}' for k, v in sorted(pmc['by_status'].items()))})")
    print("Next: review, drop irrelevant rows, then:")
    print(f"  mv {out_path} {out_path.parent / final_name(tag)}")
    print(f"  python {HERE / 'sweep.py'} --project {project}")
    _summary(res)
    return res


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--project", required=True, help="Project name from projects.json")
    ap.add_argument("--rank", choices=RANKS, default=None,
                    help="project (default: the project's own seeds), portfolio (the old order), "
                         "or cocitation (project_cocitations)")
    ap.add_argument("--scope", default=None,
                    help="Draw from scoped_candidates for this scope (its own order; no --rank)")
    ap.add_argument("--year-min", type=int, default=2010)
    ap.add_argument("--min-seeds", type=int, default=None,
                    help="Seed floor on the rank's own count (default 1; 3 under --rank portfolio)")
    ap.add_argument("--min-cites", type=int, default=0)
    ap.add_argument("--limit", type=int, default=100)
    ap.add_argument("--recent-first", action="store_true",
                    help="Sort by year (newest first, undated last), then by the rank")
    ap.add_argument("--include-undated", action="store_true",
                    help="Keep candidates with no year (dropped and counted by default)")
    ap.add_argument("--pmc-check", action="store_true",
                    help="Look every draft DOI up in PMC (lit_net.doi_to_pmcid): adds pmcid, pmc_status")
    ap.add_argument("--tag", default=None,
                    help="Write lit_pull_queue.<tag>.draft.csv (renamed later to lit_pull_queue.<tag>.csv)")
    ap.add_argument("--db", default=None,
                    help="Index file (default: <db_dir>/portfolio.duckdb from projects.json)")
    ap.add_argument("--destination", default=None,
                    help="Override the destination (default: the registry library relative to the project root)")
    ap.add_argument("--output", default=None,
                    help="Override the output path (default: <project root>/lit_pull_queue[.<tag>].draft.csv)")
    args = ap.parse_args(argv)
    res = run(project=args.project, rank=args.rank, scope=args.scope, year_min=args.year_min,
              min_seeds=args.min_seeds, min_cites=args.min_cites, limit=args.limit,
              destination=args.destination, output=args.output, tag=args.tag,
              recent_first=args.recent_first, include_undated=args.include_undated,
              pmc_check=args.pmc_check, db=args.db)
    return res["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
