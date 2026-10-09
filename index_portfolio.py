"""Build/refresh a DuckDB index of the portfolio's literature.

Walks every active project listed in projects.json and ingests, per library (top level only):
  - every PDF with its .ris metadata and .fulltext.json status -> paper_metadata + paper_locations
    (has_pdf=true); a PDF without a usable .ris DOI, or whose identity verdict is FLAG, goes to
    papers_no_doi instead (a flagged file is a review item, never a holding of its queue DOI)
  - every text-only holding (DEC-08): a .fulltext.json with text and no PDF beside it that
    audit_portfolio.is_text_only_sidecar accepts -> paper_locations with has_pdf=false and
    pdf_filename = the sidecar's name. Its DOI and metadata come from the .ris with the same stem,
    else from the sidecar itself.
  - _forward_citations.csv (or the legacy s2_forward_citations[_v2].csv)
                  -> candidates (source_type='forward') + cites (citing = the candidate)
  - _reverse_citations_parsed.csv (or the legacy parsed_references.csv)
                  -> candidates (source_type='reverse') + cites (citing = our seed)
  - every scoped forward harvest _<scope>_forward_citations.csv (DEC-30)
                  -> scoped_candidates + scoped_cites, keyed by (project, scope). Scoped ingest adds
                     no row to candidates, cites or paper_metadata, so top_candidates and the public
                     citation-network build never see it. *.degraded.csv, *.partial.jsonl and
                     _forward_citations_unique_dois.csv are never ingested.
Each project is one transaction; a committed project appends one index_runs row
(project, finished_at TIMESTAMPTZ in UTC, n_files, db_path) for the freshness instruments.

DOIs are normalised at ingest through litpipe.doi. A DOI read from free text (reverse rows whose
`source` is regex or absent) takes litpipe.doi.normalise as is: the most specific form, with
`.url` / `.pubmed` style tails peeled. A DOI from a structured source (S2, OpenAlex, Crossref, a
.ris DO line, a sidecar) keeps its whole form when litpipe.doi.candidates() offers it, because the
peeling heuristics shorten real DOIs (`.mkb`, `.klu`, `/acdecd`; measured on the live index
2026-10-05). Placeholders and malformed DOIs are dropped either way.

The DB path is <db_dir>/portfolio.duckdb (projects.json `db_dir`, default <root>/_references),
resolved at call time; --db overrides it. The path is printed on every run.

Exit codes (the shared step convention): 0 clean; 1 usage or config error (no registry, an
unknown --project, a registry entry without lib_dir); 2 degraded: an active project's library is
missing or unreadable, a file could not be read, or --rebuild could not carry a history table over.
The last stdout line is always "[step-summary] {json}" (keys: reasons, aborted, transport_failures,
...), so snowball reads exit 2 as DEGRADED rather than FAILED.

Usage:
  python index_portfolio.py                       # refresh every active project
  python index_portfolio.py --project research_a  # refresh one project
  python index_portfolio.py --no-citations        # papers only (faster)
  python index_portfolio.py --gc                  # also reclaim zero-reference paper_metadata rows
  python index_portfolio.py --rebuild             # drop+recreate (backs up to <db>.bak first)
  python index_portfolio.py --rename-project OLD NEW            # dry run: what would move
  python index_portfolio.py --rename-project OLD NEW --execute  # move it, one transaction
  python index_portfolio.py --db <temp.duckdb>    # another DB
"""
import argparse
import csv
import datetime
import functools
import json
import os
import re
import sys
from pathlib import Path

import duckdb
import pandas as pd  # E1: register()+INSERT..SELECT bulk path (see _bulk_insert)

import lit_util  # RC1 DOI validity gate, RC4 atomic writes (shared, pre-tested)
import audit_portfolio as _audit  # the shared DEC-08 / identity-flag predicates (W2-F)
from litpipe import config as _config
from litpipe import doi as _doi
from litpipe.config import ConfigError

lit_util.utf8_stdout()

CONFIG_PATH   = Path(__file__).parent / "projects.json"
# Kept for importers and tests/test_root_config.py; the run resolves <db_dir>/portfolio.duckdb at
# call time through default_db_path() (projects.json `db_dir`), and --db overrides both.
DB_PATH       = lit_util.PROJECTS_ROOT / "_references" / "portfolio.duckdb"
DB_NAME       = "portfolio.duckdb"

SIDECAR  = ".fulltext.json"
IDENTITY = ".identity.json"
SUMMARY_MARKER = "[step-summary] "
EXIT_OK, EXIT_CONFIG, EXIT_DEGRADED = 0, 1, 2

# A consumer's scoped forward harvest (DEC-30), e.g. _resp_forward_citations.csv. Names that end in
# _unique_dois.csv or .degraded.csv cannot match.
SCOPED_FORWARD_RE = re.compile(r"^_([a-z0-9][a-z0-9_-]*)_forward_citations\.csv$")
# Reverse-walker `source` values whose DOI came from structured metadata (W3-B contract); `regex`
# and a missing column mean the DOI was read from reference text.
STRUCTURED_SOURCES = frozenset({"s2", "openalex", "crossref", "sidecar"})
# History tables other tasks own that --rebuild carries over from the .bak, each only if it
# exists there (abstract_attempts: W2-E2; the other three: W3-E).
CARRY_OVER_TABLES = ("abstract_attempts", "recent_feed", "rec_attempts", "s2_enrichment")
# SCHEMA tables only enrich_recommendations fills: their rows are carried over too, or a rebuild leaves
# rec_attempts saying "answered" for seeds whose rows were dropped (W3a verifier G-3).
CARRY_OVER_ROWS = ("recommendations",)

SCHEMA = """
-- Schema v2 (2026-05-04). Normalized:
--   paper_metadata = canonical metadata per DOI (one row per DOI, paper or candidate)
--   paper_locations = which projects hold the paper (many-to-many); has_pdf=false is a
--                     text-only holding (DEC-08) whose pdf_filename is its .fulltext.json
--   candidates = discovery records (cite-pointer + impact signal; no metadata duplication)
--   cites = citation graph edges
-- Joins back to paper_metadata for title/abstract/etc. when querying candidates.

CREATE TABLE IF NOT EXISTS paper_metadata (
  doi              VARCHAR PRIMARY KEY,
  year             INTEGER,
  lastname         VARCHAR,
  title            VARCHAR,
  venue            VARCHAR,
  authors          VARCHAR,
  abstract         VARCHAR,    -- enriched by enrich_abstracts.py (CrossRef)
  abstract_attempted_at TIMESTAMP,  -- last enrich attempt (hit or permanent miss); NULL = never tried
  refreshed_at     TIMESTAMP
);
-- Self-healing migration: back-fill abstract_attempted_at on a PRE-EXISTING DB (the
-- CREATE TABLE IF NOT EXISTS above is a no-op there, so the papers-view GROUP BY below
-- would otherwise fail to bind on a normal non-rebuild re-index). No-op on a fresh DB.
ALTER TABLE paper_metadata ADD COLUMN IF NOT EXISTS abstract_attempted_at TIMESTAMP;

CREATE TABLE IF NOT EXISTS paper_locations (
  doi              VARCHAR,
  project          VARCHAR,
  lib_path         VARCHAR,
  pdf_filename     VARCHAR,
  has_pdf          BOOLEAN DEFAULT FALSE,
  has_sidecar      BOOLEAN DEFAULT FALSE,
  has_ris          BOOLEAN DEFAULT FALSE,
  sidecar_text_len INTEGER DEFAULT 0,
  refreshed_at     TIMESTAMP,
  PRIMARY KEY (doi, project)
);

CREATE TABLE IF NOT EXISTS papers_no_doi (
  pdf_filename     VARCHAR,
  project          VARCHAR,
  lib_path         VARCHAR,
  reason           VARCHAR,           -- no_ris | ris_lacks_doi | ris_doi_invalid | identity_flag
  PRIMARY KEY (pdf_filename, project)
);

CREATE TABLE IF NOT EXISTS candidates (
  doi              VARCHAR,
  source_type      VARCHAR,           -- 'forward' | 'reverse'
  source_seed_doi  VARCHAR,
  source_project   VARCHAR,
  citing_cited_by  INTEGER,           -- impact proxy (forward only; 0 for reverse)
  refreshed_at     TIMESTAMP,
  PRIMARY KEY (doi, source_type, source_seed_doi, source_project)
);

CREATE TABLE IF NOT EXISTS cites (
  citing_doi       VARCHAR,
  cited_doi        VARCHAR,
  source_pipeline  VARCHAR,
  source_project   VARCHAR,
  PRIMARY KEY (citing_doi, cited_doi, source_project)
);

CREATE TABLE IF NOT EXISTS recommendations (
  -- S2 /paper/{id}/recommendations enrichment
  seed_doi         VARCHAR,
  recommended_doi  VARCHAR,
  rank             INTEGER,
  refreshed_at     TIMESTAMP,
  PRIMARY KEY (seed_doi, recommended_doi)
);

-- DEC-30: a consumer's scoped harvests (<lib>/_<scope>_forward_citations.csv), kept apart from
-- candidates/cites so top_candidates and the public build are untouched. Rewritten per
-- (project, scope) on every index; a scope whose CSV is gone loses its rows.
CREATE TABLE IF NOT EXISTS scoped_candidates (
  project          VARCHAR,
  scope            VARCHAR,
  doi              VARCHAR,           -- the citing paper (the candidate)
  source_type      VARCHAR,           -- 'forward'
  source_seed_doi  VARCHAR,
  seed_label       VARCHAR,
  seed_chapter     VARCHAR,
  citing_cited_by  INTEGER,
  year             INTEGER,
  title            VARCHAR,
  venue            VARCHAR,
  authors          VARCHAR,
  refreshed_at     TIMESTAMP,
  PRIMARY KEY (project, scope, doi, source_type, source_seed_doi)
);

CREATE TABLE IF NOT EXISTS scoped_cites (
  project          VARCHAR,
  scope            VARCHAR,
  citing_doi       VARCHAR,
  cited_doi        VARCHAR,
  source_pipeline  VARCHAR,
  PRIMARY KEY (project, scope, citing_doi, cited_doi)
);

-- One row per project per committed index run, read by audit_portfolio / pipeline_check.
-- finished_at is an aware UTC instant; n_files = every top-level *.pdf (no-DOI and flagged
-- included) plus every top-level text-only sidecar.
CREATE TABLE IF NOT EXISTS index_runs (
  project          VARCHAR,
  finished_at      TIMESTAMPTZ,
  n_files          INTEGER,
  db_path          VARCHAR
);

CREATE INDEX IF NOT EXISTS idx_meta_year       ON paper_metadata(year);
CREATE INDEX IF NOT EXISTS idx_meta_lastname   ON paper_metadata(lastname);
CREATE INDEX IF NOT EXISTS idx_loc_project     ON paper_locations(project);
CREATE INDEX IF NOT EXISTS idx_cand_source     ON candidates(source_type);
CREATE INDEX IF NOT EXISTS idx_cand_cited_by   ON candidates(citing_cited_by);
CREATE INDEX IF NOT EXISTS idx_cand_project    ON candidates(source_project);
CREATE INDEX IF NOT EXISTS idx_cites_cited     ON cites(cited_doi);
CREATE INDEX IF NOT EXISTS idx_cites_citing    ON cites(citing_doi);

-- View: papers we have somewhere in the portfolio (union over locations)
CREATE OR REPLACE VIEW papers AS
SELECT
  m.*,
  STRING_AGG(DISTINCT l.project, ',') AS projects,
  COUNT(DISTINCT l.project)             AS n_locations,
  BOOL_OR(l.has_pdf)                    AS has_pdf_anywhere,
  BOOL_OR(l.has_sidecar)                AS has_sidecar_anywhere,
  BOOL_OR(l.has_ris)                    AS has_ris_anywhere
FROM paper_metadata m
LEFT JOIN paper_locations l ON l.doi = m.doi
WHERE EXISTS (SELECT 1 FROM paper_locations l2 WHERE l2.doi = m.doi)
GROUP BY m.doi, m.year, m.lastname, m.title, m.venue, m.authors, m.abstract, m.abstract_attempted_at, m.refreshed_at;

-- View: top fetch candidates (not yet in any library, ordered by seed-count + impact)
CREATE OR REPLACE VIEW top_candidates AS
SELECT
  c.doi,
  COUNT(DISTINCT c.source_seed_doi) AS n_seeds_pointing,
  MAX(c.citing_cited_by) AS max_cited_by,
  STRING_AGG(DISTINCT c.source_type, ',') AS sources,
  STRING_AGG(DISTINCT c.source_project, ',') AS via_projects,
  m.year,
  m.title,
  m.abstract
FROM candidates c
LEFT JOIN paper_metadata m ON m.doi = c.doi
WHERE NOT EXISTS (SELECT 1 FROM paper_locations l WHERE l.doi = c.doi)
GROUP BY c.doi, m.year, m.title, m.abstract
ORDER BY n_seeds_pointing DESC, max_cited_by DESC;

-- View: cross-project DOI overlaps (papers that live in 2+ project libs)
CREATE OR REPLACE VIEW cross_project_papers AS
SELECT m.doi, m.title, m.year,
       STRING_AGG(l.project, ',') AS projects,
       COUNT(*) AS n_projects
FROM paper_metadata m
JOIN paper_locations l ON l.doi = m.doi
GROUP BY m.doi, m.title, m.year
HAVING COUNT(*) > 1
ORDER BY n_projects DESC;

-- View: per-project co-citation ranking (see project_cocitations() for the column contract).
-- project        a project key from paper_locations
-- doi            a DOI one of that project's own papers cites, missing from the project
--                (not in its paper_locations; a text-only holding counts as held)
-- n_own_citing   how many distinct papers the project holds cite it (any cites edge)
-- held_anywhere  true when another project holds it
CREATE OR REPLACE VIEW project_cocitations AS
SELECT l.project,
       c.cited_doi                  AS doi,
       COUNT(DISTINCT c.citing_doi) AS n_own_citing,
       EXISTS (SELECT 1 FROM paper_locations h WHERE h.doi = c.cited_doi) AS held_anywhere
FROM cites c
JOIN paper_locations l ON l.doi = c.citing_doi
WHERE c.cited_doi <> c.citing_doi
  AND NOT EXISTS (SELECT 1 FROM paper_locations o WHERE o.project = l.project AND o.doi = c.cited_doi)
GROUP BY l.project, c.cited_doi
ORDER BY l.project, n_own_citing DESC, doi;
"""


# ---------- helpers ----------

# parse_ris was promoted to lit_util (Stage 3 c10, unioning the harvest_citations copy: single-pass
# continuation lines + AU/A1, PY/Y1, TI/T1 aliases + int-or-None year + UR-DOI fallback). Re-exported
# here so the `parse_ris(...)` / `I.parse_ris(...)` call sites (and test_index_prune) keep working.
parse_ris = lit_util.parse_ris


class LibraryUnreadable(OSError):
    """The library directory could not be listed: the project is skipped (exit 2), not indexed."""


class FileUnreadable(OSError):
    """One input file (a citation CSV) could not be read: that input is left as it was (exit 2)."""


def _whole_doi(raw) -> str:
    s = str(raw or "").strip().lower()
    i = s.find("10.")
    return (s[i:] if i >= 0 else s).rstrip(".,;:")


def norm_doi(raw, structured=True):
    """The index form of a DOI, or None for a placeholder, a malformed value or no DOI at all.

    litpipe.doi is the one normaliser. Text-derived input (`structured=False`) takes
    litpipe.doi.normalise: the most specific form. A DOI from structured metadata keeps its whole
    form (resolver prefix, case and trailing punctuation removed) when litpipe.doi.candidates()
    offers that form; otherwise the most specific candidate (a `#fragment` or an embedded URL is cut).
    The structured rule exists because the text heuristics shorten registered DOIs that end in
    letters: `10.1089/ther.2017.29031.mkb`, `10.1088/2053-1591/acdecd`, `10.1149/2.0381907jss`."""
    if raw is None:
        return None
    return _norm_doi_cached(str(raw), bool(structured))


@functools.lru_cache(maxsize=262144)
def _norm_doi_cached(raw, structured):
    if not raw.strip():
        return None
    return _doi.normalise_structured(raw) if structured else _doi.normalise(raw)


def _ok_doi(d) -> bool:
    """RC1 gate (defense in depth after normalisation): well-formed and not a placeholder."""
    return bool(d) and lit_util.is_valid_doi(d) and not lit_util.is_suspicious_doi(d)


def _text_len(d) -> int:
    if not isinstance(d, dict):
        return 0
    return sum(len(d[k]) for k in ("text", "body", "abstract") if isinstance(d.get(k), str))


def sidecar_info(path: Path):
    """Return (has, text_len) for a .fulltext.json sidecar (kept for callers of the old API)."""
    if not Path(path).exists():
        return (0, 0)
    d, _err = _audit.read_json(path)
    return (1, _text_len(d))


def _parse_ris_checked(path: Path):
    """(parse_ris(path), error): error is non-empty only when the file exists but cannot be opened
    (parse_ris itself reads an unreadable file as empty, which would hide it as ris_lacks_doi)."""
    meta = parse_ris(path)
    if meta.get("doi"):
        return meta, ""
    try:
        with open(path, "rb"):
            pass
    except OSError as e:
        return meta, f"{type(e).__name__}: {e}"
    return meta, ""


def _text_only_meta(d: dict) -> dict:
    """paper_metadata fields for a text-only holding with no usable .ris: the sidecar's own
    metadata, mapped the way backfill_ris builds its .ris (so a later .ris changes nothing)."""
    import backfill_ris  # lazy: pulls ris_emit, needed only for sidecar-only holdings
    m = backfill_ris.sidecar_meta(d)
    authors = []
    for a in m.get("authors") or []:
        fam, giv = a.get("family") or "", a.get("given") or ""
        authors.append(f"{fam}, {giv}" if giv else fam)
    year = m.get("year") or ""
    return {"year": int(year) if str(year).isdigit() else None, "lastname": m.get("lastname") or "",
            "title": m.get("title") or "", "venue": m.get("container") or "", "authors": authors}


def scan_library_files(lib: Path):
    """The library's top-level files by kind, keyed by case-folded stem, exactly as
    audit_portfolio.scan_library pairs them: (pdfs, sidecars, ris, identity). Values are file
    names. Raises LibraryUnreadable when the directory cannot be listed."""
    try:
        entries = list(os.scandir(lib))
    except OSError as e:
        raise LibraryUnreadable(f"{lib}: {type(e).__name__}: {e}") from None
    pdfs, sidecars, ris, idents = {}, {}, {}, {}
    for e in entries:
        try:
            if not e.is_file():
                continue
            e.stat()
        except OSError:
            continue
        name, low = e.name, e.name.lower()
        if low.endswith(SIDECAR):
            sidecars[name[:-len(SIDECAR)].casefold()] = name
        elif low.endswith(IDENTITY):
            idents[name[:-len(IDENTITY)].casefold()] = name
        elif low.endswith(".ris"):
            ris[name[:-4].casefold()] = name
        elif low.endswith(".pdf"):
            pdfs[name[:-4].casefold()] = name
    return pdfs, sidecars, ris, idents


def _bulk_insert(con, table, columns, rows, *, int_cols=(), on_conflict=""):
    """E1: fast columnar bulk INSERT via a registered pandas DataFrame (register + INSERT..SELECT).

    ~250x faster than per-row `executemany` on the ART-indexed tables here -- DuckDB's insert cost
    scales with TABLE size, not row count, so per-row PK maintenance dominates at real scale (see
    the duckdb skill's bulk_insert.md; staging-temp SQL was only ~4x, so the pandas path is load-
    bearing). Rows must already be Python-deduped on the conflict target -- ON CONFLICT does NOT
    dedupe rows within a single statement.

    - columns: target column names, in each tuple's field order.
    - int_cols: columns coerced to pandas nullable Int64 so a None (e.g. a missing `year`) is
      written as SQL NULL deterministically. A mixed None/int column otherwise infers float64
      (NaN); DuckDB 1.5.5 happens to cast that NaN to NULL, but the explicit Int64 keeps the
      None-handling correct and version-independent rather than relying on that leniency.
    - on_conflict: optional trailing "ON CONFLICT (...) DO ..." clause.

    DuckDB implicitly casts the SELECTed columns to the target types (VARCHAR->TIMESTAMP,
    Int64->INTEGER) -- verified against this schema -- so no per-column CAST is needed.
    """
    if not rows:
        return
    df = pd.DataFrame(rows, columns=list(columns))
    for c in int_cols:
        df[c] = df[c].astype("Int64")
    con.register("_bulk_df", df)
    try:
        collist = ", ".join(columns)
        con.execute(f"INSERT INTO {table} ({collist}) SELECT {collist} FROM _bulk_df {on_conflict}")
    finally:
        try:
            con.unregister("_bulk_df")
        except Exception:
            pass


_META_COLS = ["doi", "year", "lastname", "title", "venue", "authors", "refreshed_at"]


def _stage_metadata(con, rows, name):
    """Rows (in _META_COLS order, deduped on doi) as a TEMP table `name`."""
    df = pd.DataFrame(rows, columns=_META_COLS)
    df["year"] = df["year"].astype("Int64")
    con.register("_meta_df", df)
    try:
        con.execute(f"CREATE OR REPLACE TEMP TABLE {name} AS SELECT doi, year, lastname, title, venue, "
                    "authors, CAST(refreshed_at AS TIMESTAMP) AS refreshed_at FROM _meta_df")
    finally:
        try:
            con.unregister("_meta_df")
        except Exception:
            pass


def _drop_temp(con, name):
    try:
        con.execute(f"DROP TABLE IF EXISTS {name}")
    except Exception:       # an aborted transaction: the caller's ROLLBACK discards it anyway
        pass


def _dedup_first(rows, key=lambda r: r[0]):
    seen, out = set(), []
    for r in rows:
        k = key(r)
        if k not in seen:
            seen.add(k)
            out.append(r)
    return out


def upsert_library_metadata(con, rows):
    """Refresh paper_metadata for the DOIs a library holds, set-based (a TEMP table plus
    UPDATE ... FROM, then an anti-join INSERT for new DOIs). `abstract` and `abstract_attempted_at`
    are never written (the abstract invariant, tests/test_duckdb_upsert.py).

    year and lastname carry secondary ART indexes, so DuckDB rewrites an updated row's index
    entries; they are updated only where they changed. Probe (2026-10-05, DuckDB 1.5.5, 6,000 rows
    into a 100,000-row table, reopened DB): every column every time 4.0 s, the split 0.05 s; the
    per-row executemany it replaces took 7-8 s."""
    rows = _dedup_first(rows)
    if not rows:
        return
    _stage_metadata(con, rows, "_meta_upd")
    try:
        con.execute("UPDATE paper_metadata AS m SET title = u.title, venue = u.venue, "
                    "authors = u.authors, refreshed_at = u.refreshed_at "
                    "FROM _meta_upd AS u WHERE m.doi = u.doi")
        con.execute("UPDATE paper_metadata AS m SET year = u.year, lastname = u.lastname "
                    "FROM _meta_upd AS u WHERE m.doi = u.doi "
                    "AND (m.year IS DISTINCT FROM u.year OR m.lastname IS DISTINCT FROM u.lastname)")
        con.execute("INSERT INTO paper_metadata (doi, year, lastname, title, venue, authors, refreshed_at) "
                    "SELECT u.doi, u.year, u.lastname, u.title, u.venue, u.authors, u.refreshed_at "
                    "FROM _meta_upd AS u "
                    "WHERE NOT EXISTS (SELECT 1 FROM paper_metadata m WHERE m.doi = u.doi)")
    finally:
        _drop_temp(con, "_meta_upd")


def insert_new_metadata(con, rows):
    """paper_metadata rows for DOIs not present yet (anti-join INSERT); an existing row (richer
    .ris data, an enriched abstract) is never overwritten."""
    rows = _dedup_first(rows)
    if not rows:
        return
    _stage_metadata(con, rows, "_meta_new")
    try:
        con.execute("INSERT INTO paper_metadata (doi, year, lastname, title, venue, authors, refreshed_at) "
                    "SELECT n.doi, n.year, n.lastname, n.title, n.venue, n.authors, n.refreshed_at "
                    "FROM _meta_new AS n "
                    "WHERE NOT EXISTS (SELECT 1 FROM paper_metadata m WHERE m.doi = n.doi)")
    finally:
        _drop_temp(con, "_meta_new")


def _now():
    return datetime.datetime.now().isoformat(timespec="seconds")


def ingest_papers(con, name: str, lib: Path, stats=None, allow_empty=False):
    """Walk a library's top level; write paper_metadata, paper_locations and papers_no_doi rows for
    project `name`. Returns (pdf_count, no_doi_count) as before; `stats` (a dict), when given,
    receives n_files, n_pdfs, n_text_only, n_text_only_indexed, n_flagged, unreadable [(file, why)],
    gc_file_gone and gc_doi_changed.

    Holdings: a PDF with a usable .ris DOI (has_pdf=true), and a text-only sidecar
    (audit_portfolio.is_text_only_sidecar; has_pdf=false, pdf_filename = the sidecar name, DOI from
    the same-stem .ris else the sidecar). A PDF whose `.identity.json` or pmc `.fulltext.json`
    carries an identity FLAG (audit_portfolio.identity_flag) is a review item: papers_no_doi with
    reason identity_flag, never a location of its queue DOI. A flagged or PDF-derived sidecar
    without its PDF is not a holding.

    GC (T8 step 5): every row of this project whose DOI the library no longer holds is deleted,
    whether its file is gone or its DOI changed. The old prune compared file names only, so a PDF
    whose .ris DOI was corrected (a URL-form DOI made bare, a preprint DOI replaced by the
    published one) kept a second row under its old DOI with the same, still-present file name:
    that is why one research library showed two more paper_locations rows than PDFs while every
    file name reconciled (read-only look at the live index, 2026-10-05).

    An empty listing (no PDF, sidecar, .ris or identity file) while the index holds rows for the
    project is read as an unlistable library (LibraryUnreadable: rolled back, exit 2, nothing
    pruned), not as "every paper is gone": a synced folder can list empty for a moment, and the GC
    would delete every row (W3a verifier G). `allow_empty` (`--allow-empty-library`) accepts a
    library emptied on purpose.

    Writes go through `con` so the caller's per-project transaction enrolls them (C2)."""
    st = stats if stats is not None else {}
    cur = con
    now = _now()
    pdfs, sidecars, ris_files, idents = scan_library_files(lib)
    if not (pdfs or sidecars or ris_files or idents) and not allow_empty:
        held = cur.execute("SELECT COUNT(*) FROM paper_locations WHERE project = ?", [name]).fetchone()[0]
        if held:
            raise LibraryUnreadable(f"{lib}: lists no PDF, sidecar or .ris, but the index holds {held} "
                                    f"location row(s) for {name}; nothing pruned (pass "
                                    f"--allow-empty-library if it was emptied on purpose)")
    unreadable = []

    flags = {}
    for stem, fname in sorted(idents.items()):
        d, err = _audit.read_json(lib / fname)
        if d is None:
            unreadable.append((fname, err))
            continue
        why = _audit.identity_flag(d)
        if why:
            flags[stem] = why
    sc_data = {}
    for stem, fname in sorted(sidecars.items()):
        d, err = _audit.read_json(lib / fname)
        sc_data[stem] = d
        if d is None:
            unreadable.append((fname, err))
            continue
        why = _audit.identity_flag(d)
        if why and stem not in flags:
            flags[stem] = why

    meta_rows = []; pdf_loc = []; text_loc = []; rows_no_doi = []
    pdf_count = no_doi_count = n_flagged = 0
    for stem, pdf_name in sorted(pdfs.items(), key=lambda kv: kv[1]):
        ris_name = ris_files.get(stem)
        d = sc_data.get(stem)
        has_sc = stem in sidecars
        if stem in flags:
            rows_no_doi.append((pdf_name, name, str(lib), "identity_flag"))
            n_flagged += 1
            no_doi_count += 1
            continue
        meta, err = _parse_ris_checked(lib / ris_name) if ris_name else ({}, "")
        if err:
            unreadable.append((ris_name, err))
        raw = meta.get("doi")
        doi = norm_doi(raw) if raw else None
        if not _ok_doi(doi):
            reason = "no_ris" if not ris_name else ("ris_doi_invalid" if raw else "ris_lacks_doi")
            rows_no_doi.append((pdf_name, name, str(lib), reason))
            no_doi_count += 1
            continue
        meta_rows.append((doi, meta.get("year"), meta.get("lastname"), meta.get("title"),
                          meta.get("venue"), "; ".join(meta.get("authors", [])), now))
        pdf_loc.append((doi, name, str(lib), pdf_name, True, has_sc, bool(ris_name), _text_len(d), now))
        pdf_count += 1

    n_text = n_text_indexed = 0
    for stem, sc_name in sorted(sidecars.items(), key=lambda kv: kv[1]):
        if stem in pdfs or stem in flags:
            continue
        d = sc_data.get(stem)
        if d is None or not _audit.is_text_only_sidecar(d):
            continue
        n_text += 1
        ris_name = ris_files.get(stem)
        meta, err = _parse_ris_checked(lib / ris_name) if ris_name else ({}, "")
        if err:
            unreadable.append((ris_name, err))
        doi = norm_doi(meta.get("doi")) if meta.get("doi") else None
        if _ok_doi(doi):
            m = {"year": meta.get("year"), "lastname": meta.get("lastname"), "title": meta.get("title"),
                 "venue": meta.get("venue"), "authors": meta.get("authors", [])}
        else:
            doi = norm_doi(d.get("doi")) if d.get("doi") else None
            if not _ok_doi(doi):
                continue          # counted in n_files (the instruments count it too), not indexable
            m = _text_only_meta(d)
        meta_rows.append((doi, m["year"], m["lastname"], m["title"], m["venue"],
                          "; ".join(m["authors"]), now))
        text_loc.append((doi, name, str(lib), sc_name, False, True, bool(ris_name), _text_len(d), now))
        n_text_indexed += 1

    # paper_metadata: one row per DOI (same DOI in 2 PDFs = LeBris case; a PDF's .ris wins over a
    # text-only sidecar). abstract is never touched (enriched separately by enrich_abstracts.py).
    upsert_library_metadata(cur, meta_rows)

    # paper_locations: one row per (doi, project); a PDF row wins over a text-only row.
    dedup_loc = _dedup_first(pdf_loc + text_loc)
    cur_df = pd.DataFrame({"doi": [r[0] for r in dedup_loc]}, dtype="object")
    con.register("_cur_loc", cur_df)
    try:
        stale = cur.execute("SELECT doi, pdf_filename FROM paper_locations WHERE project = ? "
                            "AND doi NOT IN (SELECT doi FROM _cur_loc)", [name]).fetchall()
        cur.execute("DELETE FROM paper_locations WHERE project = ? AND doi NOT IN (SELECT doi FROM _cur_loc)",
                    [name])
    finally:
        try:
            con.unregister("_cur_loc")
        except Exception:
            pass
    present = {n.casefold() for n in list(pdfs.values()) + list(sidecars.values())}
    st["gc_doi_changed"] = sum(1 for _, fn in stale if (fn or "").casefold() in present)
    st["gc_file_gone"] = len(stale) - st["gc_doi_changed"]
    _bulk_insert(cur, "paper_locations",
                 ["doi", "project", "lib_path", "pdf_filename", "has_pdf", "has_sidecar",
                  "has_ris", "sidecar_text_len", "refreshed_at"],
                 dedup_loc,
                 on_conflict=("ON CONFLICT (doi, project) DO UPDATE SET "
                              "lib_path=excluded.lib_path, pdf_filename=excluded.pdf_filename, "
                              "has_pdf=excluded.has_pdf, has_sidecar=excluded.has_sidecar, "
                              "has_ris=excluded.has_ris, sidecar_text_len=excluded.sidecar_text_len, "
                              "refreshed_at=excluded.refreshed_at"))

    # T2 (2026-06-25): prune papers_no_doi project-wide, mirroring the paper_locations prune
    # above. A PDF that GAINS a DOI / is renamed / is deleted must lose its stale no-DOI row
    # (else it persists forever, inflating the 'PDFs without DOI' count and defeating the
    # backfill fix). Runs UNCONDITIONALLY so a project whose last no-DOI PDF got fixed clears.
    current_nodoi = sorted({r[0] for r in rows_no_doi})
    if current_nodoi:
        _ph = ",".join(["?"] * len(current_nodoi))
        cur.execute(f"DELETE FROM papers_no_doi WHERE project = ? AND pdf_filename NOT IN ({_ph})",
                    [name, *current_nodoi])
    else:
        cur.execute("DELETE FROM papers_no_doi WHERE project = ?", [name])
    _bulk_insert(cur, "papers_no_doi", ["pdf_filename", "project", "lib_path", "reason"],
                 _dedup_first(rows_no_doi, key=lambda r: (r[0], r[1])),
                 on_conflict=("ON CONFLICT (pdf_filename, project) DO UPDATE SET "
                              "lib_path=excluded.lib_path, reason=excluded.reason"))

    st.update(n_files=len(pdfs) + n_text, n_pdfs=len(pdfs), n_text_only=n_text,
              n_text_only_indexed=n_text_indexed, n_flagged=n_flagged, unreadable=unreadable)
    return pdf_count, no_doi_count


def prune_citations(con, name, kind):
    """T2: delete a project's candidate/cite rows for one citation 'kind' ('forward'|'reverse').
    Called from ingest_forward/ingest_reverse AND from main() when the CSV is ABSENT, so a
    DELETED citation CSV (not just a shrunk one) cannot leave orphan candidate/cite rows that
    inflate top_candidates."""
    cur = con   # C2: write through the connection so the caller's per-project transaction enrolls these
    cur.execute("DELETE FROM candidates WHERE source_project=? AND source_type=?", [name, kind])
    cur.execute("DELETE FROM cites WHERE source_project=? AND source_pipeline=?", [name, kind])


def _write_citation_rows(con, cand_rows, meta_rows, cite_rows):
    """Shared candidate/metadata/cite writer for ingest_forward + ingest_reverse (c11).

    prune_citations() has ALREADY removed this project's rows for this source_type/pipeline
    immediately before this call, so:
      - candidates: NO per-key DELETE. The candidates PK includes source_type, so the prune
        cleared every row this batch could collide with -- the old per-key executemany DELETE
        was a strict no-op (~44s/40k on a --rebuild, measured).
      - cites: ON CONFLICT, because the cites PK EXCLUDES source_pipeline -- the same
        (citing,cited,project) edge discovered by BOTH the forward and reverse pass must update
        the pipeline tag rather than collide (the guard the old per-key cites DELETE provided;
        last-writer-wins, matching the old DELETE-then-INSERT).
      - paper_metadata: insert only genuinely-new DOIs (an anti-join INSERT); never overwrite a
        richer .ris-sourced row (the abstract invariant -- test_duckdb_upsert).
    Writes through `con` (not a child cursor) so a future per-project transaction can enroll them."""
    if cand_rows:
        # E1: bulk insert. prune_citations already cleared this (project, source_type) slice, so
        # no per-key DELETE / ON CONFLICT needed (the candidates PK includes source_type).
        _bulk_insert(con, "candidates",
                     ["doi", "source_type", "source_seed_doi", "source_project",
                      "citing_cited_by", "refreshed_at"],
                     _dedup_first(cand_rows, key=lambda r: (r[0], r[1], r[2], r[3])),
                     int_cols=["citing_cited_by"])
    if meta_rows:
        insert_new_metadata(con, meta_rows)
    if cite_rows:
        # E1: bulk insert; cites PK excludes source_pipeline, so the same (citing,cited,project)
        # edge found by BOTH passes updates the tag rather than colliding (see docstring).
        _bulk_insert(con, "cites",
                     ["citing_doi", "cited_doi", "source_pipeline", "source_project"],
                     _dedup_first(cite_rows, key=lambda r: (r[0], r[1], r[3])),
                     on_conflict=("ON CONFLICT (citing_doi, cited_doi, source_project) "
                                  "DO UPDATE SET source_pipeline = excluded.source_pipeline"))


def _existing_tables(con):
    """Base tables of this DB's main schema (information_schema also lists ATTACHed catalogs)."""
    return {r[0] for r in con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_catalog = current_database() "
        "AND table_schema = 'main' AND table_type = 'BASE TABLE'").fetchall()}


def gc_orphan_metadata(con):
    """T2: delete paper_metadata rows referenced by NOTHING (no location/candidate/cite/
    recommendation, nor recent_feed or scoped_* rows where those tables exist) and return the
    count reclaimed. Conservative -- a row pinned by even a stale candidate survives. Gated behind
    main()'s --gc; shared so the test guards the real SQL."""
    cur = con.cursor()
    tables = _existing_tables(cur)
    spare = [
        "NOT EXISTS (SELECT 1 FROM paper_locations l WHERE l.doi = m.doi)",
        "NOT EXISTS (SELECT 1 FROM candidates c WHERE c.doi = m.doi OR c.source_seed_doi = m.doi)",
        "NOT EXISTS (SELECT 1 FROM cites ci WHERE ci.citing_doi = m.doi OR ci.cited_doi = m.doi)",
        "NOT EXISTS (SELECT 1 FROM recommendations r WHERE r.seed_doi = m.doi OR r.recommended_doi = m.doi)",
    ]
    if "recent_feed" in tables:     # W3-E's append-only feed
        spare.append("NOT EXISTS (SELECT 1 FROM recent_feed f "
                     "WHERE f.seed_doi = m.doi OR f.recommended_doi = m.doi)")
    if "scoped_candidates" in tables:
        spare.append("NOT EXISTS (SELECT 1 FROM scoped_candidates sc "
                     "WHERE sc.doi = m.doi OR sc.source_seed_doi = m.doi)")
    if "scoped_cites" in tables:
        spare.append("NOT EXISTS (SELECT 1 FROM scoped_cites sx "
                     "WHERE sx.citing_doi = m.doi OR sx.cited_doi = m.doi)")
    before = cur.execute("SELECT COUNT(*) FROM paper_metadata").fetchone()[0]
    cur.execute("DELETE FROM paper_metadata m WHERE " + " AND ".join(spare))
    after = cur.execute("SELECT COUNT(*) FROM paper_metadata").fetchone()[0]
    return before - after


def _read_csv_rows(csv_path: Path):
    """All rows of a citation CSV as dicts, plus its header; FileUnreadable when it cannot be read."""
    try:
        with open(csv_path, encoding="utf-8", newline="") as f:
            rd = csv.DictReader(f)
            rows = list(rd)
            return rows, list(rd.fieldnames or [])
    except (OSError, UnicodeDecodeError, csv.Error) as e:
        raise FileUnreadable(f"{csv_path.name}: {type(e).__name__}: {e}") from None


def ingest_forward(con, name: str, csv_path: Path, lib: Path = None):
    """Ingest forward-citation CSV. Schema:
      - candidates row per (citing_doi, seed_doi, project) — discovery record
      - paper_metadata row per citing_doi — inserted only when new (preserves any existing row)
      - cites edge: citing → seed (the candidate cites our seed)
    `lib` is accepted for call compatibility and unused: every forward row carries its seed_doi,
    so no library file is read (the 075bd3a deferral, resolved W3-C1)."""
    if not csv_path.exists(): return 0
    now = _now()
    cand_rows = []; meta_rows = []; cite_rows = []
    n_rejected = n_changed = 0  # RC1-gate: malformed / truncated / placeholder DOIs in the CSV
    rows, _hdr = _read_csv_rows(csv_path)
    for r in rows:
        seed_raw = (r.get("seed_doi") or "").strip()
        cited_raw = (r.get("citing_doi") or "").strip()
        if not (seed_raw and cited_raw): continue
        seed, cited = norm_doi(seed_raw), norm_doi(cited_raw)      # both from structured metadata
        # RC1-gate (defense-in-depth): drop rows whose DOI is malformed or looks like a
        # line-wrap truncation or a placeholder before it pollutes candidates/cites/meta.
        if not (_ok_doi(seed) and _ok_doi(cited)):
            n_rejected += 1
            continue
        n_changed += (seed != seed_raw.lower()) + (cited != cited_raw.lower())
        cand_rows.append((
            cited, "forward", seed, name,
            lit_util.coerce_int(r.get("citing_cited_by")), now,
        ))
        meta_rows.append((
            cited,
            lit_util.coerce_int(r.get("citing_year")),
            "",  # lastname not in forward CSV; left blank
            r.get("citing_title") or "",
            r.get("citing_venue") or "",
            r.get("citing_authors") or "",
            now,
        ))
        cite_rows.append((cited, seed, "forward", name))  # citing=candidate, cited=seed

    # T2 (2026-06-25): rewrite the full per-(project, source_type) set from this CSV. Without a
    # scope-wide prune a SHRUNK forward CSV leaves orphan candidate/cite rows that inflate
    # top_candidates. (main() also prunes when the CSV is ABSENT -> deleted-CSV case.)
    prune_citations(con, name, "forward")

    # c11/E1: shared writer -- no redundant per-key DELETEs (prune ran); cites via ON CONFLICT.
    _write_citation_rows(con, cand_rows, meta_rows, cite_rows)
    if n_rejected:
        print(f"  [doi-gate] forward: dropped {n_rejected} row(s) with malformed/truncated/placeholder DOI")
    if n_changed:
        print(f"  [doi] forward: normalised {n_changed} DOI value(s)")
    return len(cand_rows)


def seed_doi_for(row, lib: Path, cache=None) -> str:
    """A reverse row's seed DOI (T8 step 3): the CSV's `seed_doi` column when it holds a DOI (the
    reverse walker writes it, W3-B), else the DOI in the `.ris` of the seed file named in `seed`
    (`.txt`/`.pdf` stripped), else ''. Keyed by DOI, so a renamed PDF keeps its edges once the
    walker has written the column."""
    col = (row.get("seed_doi") or "").strip()
    if col:
        d = norm_doi(col)
        if _ok_doi(d):
            return d
    seed_filename = row.get("seed") or ""
    cache = {} if cache is None else cache
    if seed_filename in cache:
        return cache[seed_filename]
    stem = seed_filename
    for ext in (".txt", ".pdf"):
        if stem.endswith(ext): stem = stem[:-len(ext)]
    d = ""
    if stem and lib is not None:
        ris = Path(lib) / (stem + ".ris")
        raw = parse_ris(ris).get("doi", "") if ris.exists() else ""
        nd = norm_doi(raw) if raw else None
        d = nd if _ok_doi(nd) else ""
    cache[seed_filename] = d
    return d


def ingest_reverse(con, name: str, csv_path: Path, lib: Path):
    """Reverse citations CSV. Schemas accepted:
      - new pipeline: <lib>/_reverse_citations_parsed.csv with columns
        seed, first_author, year, title_snippet, doi, raw[, seed_doi, source]
        (`seed_doi` and `source` are the W3-B additions; both are optional)
      - the legacy data_dir layout (a Tier 1 project from before the pipeline's walkers):
        <data_dir>/references/parsed_references.csv (same columns)
    A cited DOI whose `source` is s2/openalex/crossref/sidecar is read as structured metadata; a
    `regex` or missing source as reference text (litpipe.doi.normalise, tails peeled)."""
    if not csv_path.exists(): return 0
    now = _now()
    cand_rows = []; cite_rows = []; meta_rows = []
    seed_cache = {}
    n_rejected = n_changed = 0  # RC1-gate: malformed / truncated DOIs already sitting in the CSV
    rows, _hdr = _read_csv_rows(csv_path)
    for r in rows:
        raw = (r.get("doi") or "").strip()
        if not raw: continue
        structured = (r.get("source") or "").strip().lower() in STRUCTURED_SOURCES
        cited_doi = norm_doi(raw, structured)
        # RC1-gate (defense-in-depth): drop the candidate/meta row if the cited DOI
        # is malformed or looks like a line-wrap truncation (the '10.1002/cphy' class).
        if not _ok_doi(cited_doi):
            n_rejected += 1
            continue
        n_changed += cited_doi != raw.lower()
        seed_doi = seed_doi_for(r, lib, seed_cache)
        cand_rows.append((
            cited_doi, "reverse", seed_doi, name,
            0,    # cited_by unknown for reverse
            now,
        ))
        meta_rows.append((
            cited_doi,
            lit_util.coerce_int(r.get("year")),
            r.get("first_author") or "",
            (r.get("title_snippet") or "")[:400],
            "", "",   # no venue / authors from reverse parser
            now,
        ))
        # Only emit a citation edge when the seed DOI is itself well-formed (seed_doi_for gates it).
        if seed_doi:
            cite_rows.append((seed_doi, cited_doi, "reverse", name))

    # T2 (2026-06-25): rewrite the full per-(project, source_type) set from this CSV (see
    # ingest_forward). A shrunk/deleted reverse CSV must not leave orphan candidate/cite rows.
    prune_citations(con, name, "reverse")

    # c11/E1: shared writer (see ingest_forward) -- prune ran, so no redundant per-key DELETEs.
    _write_citation_rows(con, cand_rows, meta_rows, cite_rows)
    if n_rejected:
        print(f"  [doi-gate] reverse: dropped {n_rejected} row(s) with malformed/truncated/placeholder DOI")
    if n_changed:
        print(f"  [doi] reverse: normalised {n_changed} DOI value(s)")
    return len(cand_rows)


# ---------- scoped harvests (DEC-30) ----------

def find_scoped_csvs(lib: Path) -> dict:
    """{scope: path} for every top-level _<scope>_forward_citations.csv (SCOPED_FORWARD_RE)."""
    out = {}
    try:
        entries = list(os.scandir(lib))
    except OSError:
        return out
    for e in entries:
        m = SCOPED_FORWARD_RE.match(e.name)
        if not m:
            continue
        try:
            if e.is_file():
                out[m.group(1)] = Path(e.path)
        except OSError:
            continue
    return dict(sorted(out.items()))


def prune_scopes(con, name, keep=()):
    """Delete project `name`'s scoped rows for every scope not in `keep` (a scope whose CSV is gone)."""
    keep = sorted(set(keep))
    if keep:
        ph = ",".join("?" * len(keep))
        con.execute(f"DELETE FROM scoped_candidates WHERE project = ? AND scope NOT IN ({ph})", [name, *keep])
        con.execute(f"DELETE FROM scoped_cites WHERE project = ? AND scope NOT IN ({ph})", [name, *keep])
    else:
        con.execute("DELETE FROM scoped_candidates WHERE project = ?", [name])
        con.execute("DELETE FROM scoped_cites WHERE project = ?", [name])


def ingest_scoped(con, name: str, scope: str, csv_path: Path):
    """One scoped forward harvest into scoped_candidates + scoped_cites, replacing the rows of
    (project, scope) and touching no other table. Columns read: seed_doi, seed_label, seed_chapter,
    citing_doi, citing_title, citing_year, citing_authors, citing_venue, citing_cited_by (no
    seed_pdf). Returns the candidate row count."""
    now = _now()
    rows, _hdr = _read_csv_rows(csv_path)
    cand, cites = [], []
    n_rejected = 0
    for r in rows:
        seed_raw = (r.get("seed_doi") or "").strip()
        cited_raw = (r.get("citing_doi") or "").strip()
        if not (seed_raw and cited_raw):
            continue
        seed, cited = norm_doi(seed_raw), norm_doi(cited_raw)
        if not (_ok_doi(seed) and _ok_doi(cited)):
            n_rejected += 1
            continue
        cand.append((name, scope, cited, "forward", seed, r.get("seed_label") or "",
                     r.get("seed_chapter") or "", lit_util.coerce_int(r.get("citing_cited_by")),
                     lit_util.coerce_int(r.get("citing_year"), None), r.get("citing_title") or "",
                     r.get("citing_venue") or "", r.get("citing_authors") or "", now))
        cites.append((name, scope, cited, seed, "forward"))
    con.execute("DELETE FROM scoped_candidates WHERE project = ? AND scope = ?", [name, scope])
    con.execute("DELETE FROM scoped_cites WHERE project = ? AND scope = ?", [name, scope])
    cand = _dedup_first(cand, key=lambda r: (r[2], r[3], r[4]))
    _bulk_insert(con, "scoped_candidates",
                 ["project", "scope", "doi", "source_type", "source_seed_doi", "seed_label",
                  "seed_chapter", "citing_cited_by", "year", "title", "venue", "authors", "refreshed_at"],
                 cand, int_cols=["citing_cited_by", "year"])
    _bulk_insert(con, "scoped_cites", ["project", "scope", "citing_doi", "cited_doi", "source_pipeline"],
                 _dedup_first(cites, key=lambda r: (r[2], r[3])))
    if n_rejected:
        print(f"  [doi-gate] scope {scope}: dropped {n_rejected} row(s) with malformed/placeholder DOI")
    return len(cand)


# ---------- co-citations ----------

def project_cocitations(con, project, limit=None):
    """Rows of the `project_cocitations` view for one project, ranked (W3-C2 reads the view).

    Columns:
      project        VARCHAR  the project key (paper_locations.project)
      doi            VARCHAR  a DOI cited by at least one paper the project holds, and missing
                              from the project: not in its paper_locations. A text-only holding
                              (has_pdf=false) counts as held, so it is never listed as missing.
      n_own_citing   BIGINT   how many distinct DOIs the project holds cite it, over every `cites`
                              edge whatever project discovered it (reverse edges: the project's
                              own reference lists; forward edges found by other projects too)
      held_anywhere  BOOLEAN  true when any project's paper_locations holds the DOI
    Ordered by n_own_citing descending, then doi. Self-citations are excluded."""
    sql = ("SELECT project, doi, n_own_citing, held_anywhere FROM project_cocitations "
           "WHERE project = ? ORDER BY n_own_citing DESC, doi")
    if limit:
        sql += f" LIMIT {int(limit)}"
    return con.execute(sql, [project]).fetchall()


# ---------- runs, rename, rebuild ----------

def record_index_run(con, project, n_files, db_path):
    """Append the index_runs row for a project, inside its transaction (a rollback drops it).
    finished_at is bound as an offset-bearing UTC string, so it is the right instant whatever
    the session TimeZone (a naive string would be read in the session zone, 4-5 h off)."""
    stamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
    con.execute("INSERT INTO index_runs (project, finished_at, n_files, db_path) "
                "VALUES (?, CAST(? AS TIMESTAMPTZ), ?, ?)", [project, stamp, int(n_files), str(db_path)])


def project_columns(con):
    """[(table, column)] for every base-table column that holds a project key: `project` or a
    name ending in `_project` (information_schema; views are skipped)."""
    return con.execute(
        "SELECT c.table_name, c.column_name FROM information_schema.columns c "
        "JOIN information_schema.tables t ON t.table_catalog = c.table_catalog "
        "AND t.table_schema = c.table_schema AND t.table_name = c.table_name "
        "WHERE t.table_type = 'BASE TABLE' AND t.table_catalog = current_database() AND t.table_schema = 'main' "
        "AND (c.column_name = 'project' OR c.column_name LIKE '%\\_project' ESCAPE '\\') "
        "ORDER BY c.table_name, c.column_name").fetchall()


def _table_columns(con, table):
    return [r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_catalog = current_database() "
        "AND table_schema = 'main' AND table_name = ? ORDER BY ordinal_position", [table]).fetchall()]


def _primary_key(con, table):
    row = con.execute("SELECT constraint_column_names FROM duckdb_constraints() "
                      "WHERE database_name = current_database() AND schema_name = 'main' "
                      "AND table_name = ? AND constraint_type = 'PRIMARY KEY'", [table]).fetchone()
    return list(row[0]) if row else None


def rename_project(con, old, new, execute=False):
    """--rename-project OLD NEW (T8 step 4). For every project-bearing column (project_columns):
    rows under OLD whose key (the primary key, or every column, without the project column) is not
    already under NEW are moved to NEW; rows NEW already covers are dropped (NEW's copy is the
    newer index). Dry run unless `execute`: then one transaction, which is rolled back unless
    every table ends with 0 rows under OLD. Returns {"plan": [...], "executed": bool}."""
    plan = []
    if execute:
        con.execute("BEGIN TRANSACTION")
    try:
        for table, col in project_columns(con):
            cols = _table_columns(con, table)
            key = [c for c in (_primary_key(con, table) or cols) if c != col]
            match = " AND ".join(f'n."{k}" IS NOT DISTINCT FROM o."{k}"' for k in key) or "TRUE"
            uniq = (f'FROM "{table}" o WHERE o."{col}" = ? AND NOT EXISTS '
                    f'(SELECT 1 FROM "{table}" n WHERE n."{col}" = ? AND {match})')
            n_old = con.execute(f'SELECT COUNT(*) FROM "{table}" WHERE "{col}" = ?', [old]).fetchone()[0]
            n_move = con.execute(f"SELECT COUNT(*) {uniq}", [old, new]).fetchone()[0]
            plan.append({"table": table, "column": col, "old_rows": n_old, "move": n_move,
                         "covered_by_new": n_old - n_move})
            if execute and n_old:
                sel = ", ".join("?" if c == col else f'o."{c}"' for c in cols)
                collist = ", ".join(f'"{c}"' for c in cols)
                con.execute(f'INSERT INTO "{table}" ({collist}) SELECT {sel} {uniq}', [new, old, new])
                con.execute(f'DELETE FROM "{table}" WHERE "{col}" = ?', [old])
        if execute:
            left = {f"{p['table']}.{p['column']}": con.execute(
                f'SELECT COUNT(*) FROM "{p["table"]}" WHERE "{p["column"]}" = ?', [old]).fetchone()[0]
                for p in plan}
            if any(left.values()):
                raise RuntimeError(f"rename left rows under {old!r}: {left}")
            con.execute("COMMIT")
    except Exception:
        if execute:
            con.execute("ROLLBACK")
        raise
    return {"plan": plan, "executed": bool(execute)}


def carry_over(con, bak: Path):
    """--rebuild: copy CARRY_OVER_TABLES and the rows of CARRY_OVER_ROWS from the .bak into the fresh
    DB, each only if it exists there, keeping its DDL (keys included). Returns {table: rows};
    raises on an unreadable .bak."""
    out = {}
    path = str(bak).replace("'", "''")
    con.execute(f"ATTACH '{path}' AS _carry_bak (READ_ONLY)")
    try:
        ddl = dict(con.execute("SELECT table_name, sql FROM duckdb_tables() "
                               "WHERE database_name = '_carry_bak' AND schema_name = 'main'").fetchall())
        have = _existing_tables(con)
        for t in CARRY_OVER_TABLES + CARRY_OVER_ROWS:
            if t not in ddl:
                continue
            if t not in have:
                con.execute(ddl[t])
            con.execute(f'INSERT INTO main."{t}" BY NAME SELECT * FROM _carry_bak.main."{t}"')
            out[t] = con.execute(f'SELECT COUNT(*) FROM main."{t}"').fetchone()[0]
    finally:
        con.execute("DETACH _carry_bak")
    return out


def _abstract_count(db_path_str):
    """Harvested-abstract count in a DuckDB file, 0 if unreadable/absent. Read-only and ALWAYS
    closes its handle in a finally (a leaked handle would lock the file on Windows and block the
    --rebuild drop/backup -- the A3 concern). Sizes the DISCARDS-N warning AND stops C1 from
    overwriting a .bak that holds more abstracts than the current (e.g. crashed-rebuild) DB."""
    c = None
    try:
        c = duckdb.connect(db_path_str, read_only=True)
        return c.execute("SELECT COUNT(*) FROM paper_metadata "
                         "WHERE abstract IS NOT NULL AND abstract != ''").fetchone()[0]
    except Exception:
        return 0
    finally:
        if c is not None:
            try: c.close()
            except Exception: pass


# ---------- registry and paths ----------

def load_registry() -> dict:
    """projects.json at this module's CONFIG_PATH, through litpipe.config.load. A missing or
    unreadable file raises ConfigError (exit 1), never ris_emit's exit 2."""
    p = Path(CONFIG_PATH)
    if not p.exists():
        raise ConfigError(f"projects.json not found at {p} (copy projects.json.template to start)")
    try:
        raw = lit_util.load_projects_config(p)
    except (OSError, ValueError) as e:
        raise ConfigError(f"projects.json at {p} could not be read: {type(e).__name__}: {e}") from None
    cfg = _config.load(raw)
    if not isinstance(cfg, dict) or not isinstance(cfg.get("projects") or {}, dict):
        raise ConfigError(f"projects.json at {p} has no `projects` object")
    return cfg


def load_config():
    """The registered projects ({key: entry}); ConfigError when the registry is missing."""
    return load_registry().get("projects") or {}


def default_db_path(cfg=None) -> Path:
    """<db_dir>/portfolio.duckdb, resolved now (projects.json `db_dir`, default <root>/_references)."""
    return _config.db_dir(cfg if cfg is not None else load_registry()) / DB_NAME


def project_paths(name: str, p: dict):
    return lit_util.lib_paths(name, p)


def find_forward_csv(lib: Path, data: Path):
    """Look for forward-citation CSV in expected locations: the library's own file first, then the
    legacy data_dir layout (a Tier 1 project's discovery files from before the walkers wrote into
    the library), kept as fallbacks only."""
    candidates = [
        lib / "_forward_citations.csv",                                # new pipeline
    ]
    if data:
        candidates += [
            data / "discovered" / "s2_forward_citations_v2.csv",       # legacy data_dir layout, v2
            data / "discovered" / "s2_forward_citations.csv",          # legacy data_dir layout, v1
        ]
    for c in candidates:
        if c.exists(): return c
    return None


def find_reverse_csv(lib: Path, data: Path):
    candidates = [
        lib / "_reverse_citations_parsed.csv",                         # new pipeline
    ]
    if data:
        candidates += [
            data / "references" / "parsed_references.csv",             # legacy data_dir layout
        ]
    for c in candidates:
        if c.exists(): return c
    return None


# ---------- run ----------

def _summary(res):
    keys = ("step", "exit_code", "status", "reasons", "aborted", "transport_failures", "db", "indexed",
            "skipped", "unreadable", "rename")
    print(SUMMARY_MARKER + json.dumps({k: res.get(k) for k in keys if k in res}, ensure_ascii=False,
                                      default=str), flush=True)


def _result(code, reasons=(), **kw):
    status = {EXIT_OK: "ok", EXIT_CONFIG: "error", EXIT_DEGRADED: "degraded"}[code]
    res = {"step": "index_portfolio", "exit_code": code, "status": status, "reasons": list(reasons),
           "aborted": None, "transport_failures": 0}
    res.update(kw)
    return res


def _config_error(msg, **kw):
    print(f"[config] {msg}", file=sys.stderr)
    return _result(EXIT_CONFIG, [f"config: {msg}"], **kw)


def _rebuild_drop(db_path: Path):
    """--rebuild: back the DB up to <db>.bak (C1) and drop it; returns the .bak path (or None)."""
    # The abstract column (enriched by enrich_abstracts.py from CrossRef) lives ONLY in the DB;
    # dropping it discards that harvest. Count + warn loudly so it's never an accidental loss.
    # A3: _abstract_count opens read-only and always closes, so a COUNT failure can't leave a
    # handle that locks the file against the drop/backup below.
    _n = _abstract_count(str(db_path))
    print(f"[--rebuild] dropping {db_path.name}"
          + (f" — DISCARDS {_n} harvested abstracts; re-run enrich_abstracts.py to restore"
             if _n else ""), file=sys.stderr)
    # C1: snapshot the old DB to <db>.bak (atomic move) BEFORE dropping, so a --rebuild that dies
    # partway -- or a regretted --rebuild -- is recoverable instead of an irreversible loss. But
    # NEVER overwrite an existing .bak that holds MORE harvested abstracts than the current DB: a
    # crashed rebuild leaves a 0-abstract DB (abstracts come from a separate enrich pass, not the
    # rebuild), and the natural retry must not clobber the good pre-crash snapshot with it.
    # Abstracts are the DB-only layer C1 exists to protect (a fresh rebuild regenerates the rest).
    # Normal shutdowns checkpoint the WAL into the DB, so .bak is self-contained; a stale WAL is
    # cleared for the fresh rebuild.
    bak = db_path.with_suffix(db_path.suffix + ".bak")
    if bak.exists() and _abstract_count(str(bak)) > _n:
        print(f"[--rebuild] keeping existing {bak.name} (more harvested abstracts than the "
              f"current DB) rather than overwrite it; dropping current DB", file=sys.stderr)
        db_path.unlink()
    else:
        try:
            db_path.replace(bak)
            print(f"[--rebuild] previous DB backed up to {bak.name}", file=sys.stderr)
        except OSError:
            db_path.unlink()  # fallback: if the backup move fails, still drop as before
    wal = db_path.with_suffix(db_path.suffix + ".wal")
    if wal.exists(): wal.unlink()
    return bak if bak.exists() else None


def run(*, project=None, db=None, no_citations=False, rebuild=False, gc=False,
        rename_project=None, execute=False, allow_empty_library=False) -> dict:
    """One index run (or a --rename-project). Returns the summary dict; its `exit_code` is the
    CLI's exit code. Prints the DB path first and "[step-summary] {json}" last."""
    try:
        projects = load_config()
        db_path = Path(db) if db else default_db_path()
    except ConfigError as e:
        res = _config_error(str(e))
        _summary(res)
        return res
    print(f"DB: {db_path}\n")

    if rename_project:
        return _run_rename(db_path, projects, rename_project, execute)

    if project is not None:
        if project not in projects:
            res = _config_error(f"unknown --project {project!r}: not registered in {CONFIG_PATH}", db=str(db_path))
            _summary(res)
            return res
        projects = {project: projects[project]}

    # Resolve every active project's paths before any write: a broken registry entry is a config
    # error for the whole run, not a half-indexed portfolio.
    plan = []
    try:
        for name, p in projects.items():
            if not (p or {}).get("active", True):
                if project is not None:
                    print(f"[inactive] {name}: not indexed (inactive projects are not skips)")
                continue
            base, lib, data = project_paths(name, p or {})
            plan.append((name, lib, data))
    except (KeyError, TypeError) as e:
        res = _config_error(f"registry entry {name!r} is unusable ({type(e).__name__}: {e}; "
                            f"every project needs a lib_dir)", db=str(db_path))
        _summary(res)
        return res

    reasons, skipped, unreadable, indexed = [], [], [], []
    db_path.parent.mkdir(parents=True, exist_ok=True)
    bak = _rebuild_drop(db_path) if (rebuild and db_path.exists()) else None
    con = lit_util.connect_db(str(db_path))  # RC10 retry-open (on_fail="raise" default)
    totals = {}
    try:
        con.execute("SET TimeZone = 'UTC'")   # duckdb skill rule 1; index_runs binds aware UTC anyway
        con.execute(SCHEMA)
        if bak is not None:
            try:
                carried = carry_over(con, bak)
                for t, n in carried.items():
                    print(f"[--rebuild] carried {t} over from {bak.name}: {n} rows", file=sys.stderr)
            except Exception as e:
                reasons.append(f"rebuild: could not carry history tables from {bak.name} "
                               f"({type(e).__name__}: {str(e)[:160]})")
                print(f"[--rebuild] WARNING {reasons[-1]}", file=sys.stderr)

        for name, lib, data in plan:
            if not lib.is_dir():
                print(f"[skip] {name}: lib not found ({lib})")
                skipped.append({"project": name, "reason": f"library not found: {lib}"})
                continue
            print(f"=== {name} ===")
            st = {}
            # C2: one transaction per project on `con` -- the SAME handle every ingest fn writes
            # through. A crash mid-project (e.g. a sync client or another process locking the file on an
            # INSERT) rolls that project back cleanly instead of committing a DELETE without its
            # INSERT; already-finished projects stay durable. Inner fns must NOT open their own
            # transaction (a nested BEGIN raises).
            con.execute("BEGIN TRANSACTION")
            try:
                n_pdf, n_no = ingest_papers(con, name, lib, stats=st, allow_empty=allow_empty_library)
                print(f"  papers: {n_pdf} ingested, {n_no} no-DOI"
                      + (f" ({st['n_flagged']} identity-flagged)" if st.get("n_flagged") else "")
                      + f"; text-only holdings: {st['n_text_only_indexed']} indexed"
                      + (f" of {st['n_text_only']}" if st["n_text_only"] != st["n_text_only_indexed"] else ""))
                if st.get("gc_file_gone") or st.get("gc_doi_changed"):
                    print(f"  [gc] paper_locations: {st['gc_file_gone']} row(s) whose file is gone, "
                          f"{st['gc_doi_changed']} whose DOI changed")
                file_errors = list(st.get("unreadable") or [])
                if not no_citations:
                    fwd = find_forward_csv(lib, data)
                    rev = find_reverse_csv(lib, data)
                    for kind, path, fn in (("forward", fwd, ingest_forward), ("reverse", rev, ingest_reverse)):
                        if not path:
                            prune_citations(con, name, kind)   # T2: deleted CSV -> drop orphan rows
                            continue
                        try:
                            n = fn(con, name, path, lib)
                            print(f"  {kind} citations from {path.name}: {n} candidates")
                        except FileUnreadable as e:   # read before any write: prior rows stay
                            file_errors.append((path.name, str(e)))
                    if not (fwd or rev): print(f"  (no citation CSVs found)")
                    scopes = find_scoped_csvs(lib)
                    kept = []
                    for scope, path in scopes.items():
                        try:
                            n = ingest_scoped(con, name, scope, path)
                            print(f"  scope {scope} from {path.name}: {n} scoped candidates")
                        except FileUnreadable as e:
                            file_errors.append((path.name, str(e)))
                        kept.append(scope)
                    prune_scopes(con, name, kept)
                record_index_run(con, name, st["n_files"], db_path)
                con.execute("COMMIT")
            except LibraryUnreadable as e:
                con.execute("ROLLBACK")
                print(f"[skip] {name}: library unreadable ({e})")
                skipped.append({"project": name, "reason": f"library unreadable: {e}"})
                continue
            except Exception:
                con.execute("ROLLBACK")
                raise
            indexed.append(name)
            for fname, why in file_errors:
                print(f"  [unreadable] {fname}: {why}")
                unreadable.append({"project": name, "file": fname, "error": why})

        if gc:
            print(f"\n  [--gc] reclaimed {gc_orphan_metadata(con)} orphan paper_metadata rows")
            registered = set(projects) if project is None else set(load_config())
            for table, col in project_columns(con):
                for key, n in con.execute(f'SELECT "{col}", COUNT(*) FROM "{table}" GROUP BY 1').fetchall():
                    if key not in registered:
                        print(f"  [--gc] {table}: {n} row(s) under unregistered key {key!r} "
                              f"(--rename-project {key!r} NEW, if it was renamed)")

        q = lambda sql: con.execute(sql).fetchone()[0]
        totals = {
            "paper_metadata": q("SELECT COUNT(*) FROM paper_metadata"),
            "paper_locations": q("SELECT COUNT(*) FROM paper_locations"),
            "text_only": q("SELECT COUNT(*) FROM paper_locations WHERE has_pdf = false"),
            "papers": q("SELECT COUNT(*) FROM papers"),
            "papers_no_doi": q("SELECT COUNT(*) FROM papers_no_doi"),
            "candidate_dois": q("SELECT COUNT(DISTINCT doi) FROM candidates"),
            "fetch_targets": q("SELECT COUNT(DISTINCT doi) FROM candidates c "
                               "WHERE NOT EXISTS (SELECT 1 FROM paper_locations l WHERE l.doi = c.doi)"),
            "cites": q("SELECT COUNT(*) FROM cites"),
            "cross_project": q("SELECT COUNT(*) FROM cross_project_papers"),
            "abstracts": q("SELECT COUNT(*) FROM paper_metadata WHERE abstract IS NOT NULL AND abstract != ''"),
            "scoped_candidates": q("SELECT COUNT(*) FROM scoped_candidates"),
            "scopes": q("SELECT COUNT(DISTINCT project || '/' || scope) FROM scoped_candidates"),
        }
        print()
        print("=== final counts ===")
        print(f"  paper_metadata rows:      {totals['paper_metadata']}")
        print(f"  paper_locations rows:     {totals['paper_locations']}    "
              f"(holdings across projects; {totals['text_only']} text-only)")
        print(f"  unique papers held:       {totals['papers']}")
        print(f"  cross-project papers:     {totals['cross_project']}  (same DOI in 2+ project libs)")
        print(f"  with abstract:            {totals['abstracts']}")
        print(f"  PDFs without DOI:         {totals['papers_no_doi']}  (incl. identity-flagged)")
        print(f"  unique candidate DOIs:    {totals['candidate_dois']}")
        print(f"  candidates not in lib:    {totals['fetch_targets']}  (fetch targets)")
        print(f"  citation edges:           {totals['cites']}")
        print(f"  scoped candidates:        {totals['scoped_candidates']}  ({totals['scopes']} scopes)")
    finally:
        con.close()
    try:
        print(f"  DB size:                  {db_path.stat().st_size // 1024} KB")
    except OSError:
        pass

    for s in skipped:
        reasons.append(f"skipped {s['project']}: {s['reason']}")
    for u in unreadable:
        reasons.append(f"unreadable {u['project']}/{u['file']}")
    code = EXIT_DEGRADED if reasons else EXIT_OK
    res = _result(code, reasons, db=str(db_path), indexed=indexed, skipped=skipped,
                  unreadable=len(unreadable), totals=totals)
    if code:
        print(f"\n[DEGRADED] {len(reasons)} problem(s): " + "; ".join(reasons[:10]))
    _summary(res)
    return res


def _run_rename(db_path, projects, pair, execute):
    old, new = (str(x).strip() for x in pair)
    if not old or not new or old == new:
        res = _config_error(f"--rename-project needs two different keys, got {old!r} {new!r}", db=str(db_path))
        _summary(res)
        return res
    if not db_path.exists():
        res = _config_error(f"no index at {db_path}", db=str(db_path))
        _summary(res)
        return res
    con = lit_util.connect_db(str(db_path), read_only=not execute)   # a dry run never takes the write lock
    try:
        out = rename_project(con, old, new, execute=execute)
    finally:
        con.close()
    verb = "moved" if execute else "would move"
    total = 0
    for p in out["plan"]:
        if p["old_rows"]:
            total += p["old_rows"]
            print(f"  {p['table']}.{p['column']}: {p['old_rows']} row(s) under {old!r}; {verb} "
                  f"{p['move']}, {p['covered_by_new']} already under {new!r} "
                  + ("dropped" if execute else "would be dropped"))
    if not total:
        print(f"  no rows under {old!r}; nothing to rename")
    elif not execute:
        print(f"  dry run: add --execute to apply (one transaction)")
    if old in projects:
        print(f"  NOTE: projects.json still registers {old!r}; rename the key there as well")
    elif new not in projects:
        print(f"  NOTE: {new!r} is not registered in projects.json")
    res = _result(EXIT_OK, [], db=str(db_path), rename={"old": old, "new": new, **out})
    _summary(res)
    return res


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--project", default=None,
                    help="Refresh only this project (default: all active in projects.json).")
    ap.add_argument("--db",      default=None,
                    help="DuckDB file (default: <db_dir>/portfolio.duckdb from projects.json, "
                         "<root>/_references when db_dir is unset).")
    ap.add_argument("--no-citations", action="store_true",
                    help="Skip forward, reverse and scoped citation ingestion (faster).")
    ap.add_argument("--rebuild", action="store_true",
                    help="Drop and recreate all tables before ingesting. WARNING: this DISCARDS the "
                         "enrich_abstracts CrossRef abstract layer — abstracts live ONLY in the DB, not in "
                         ".ris/sidecar — so re-run enrich_abstracts.py afterward. A plain (non-rebuild) "
                         "re-index PRESERVES abstracts; use --rebuild only to flush stale candidate rows. "
                         "The old DB is kept as <db>.bak, and the abstract_attempts, recent_feed, "
                         "rec_attempts and s2_enrichment history tables and the recommendations "
                         "rows are carried over from it.")
    ap.add_argument("--gc", action="store_true",
                    help="Garbage-collect paper_metadata rows with zero references (no location, "
                         "candidate, cite, recommendation, recent_feed or scoped row), and list rows "
                         "under unregistered project keys. Off by default; best after a full index.")
    ap.add_argument("--rename-project", nargs=2, metavar=("OLD", "NEW"), default=None,
                    help="Move every row keyed by project OLD to NEW (rows NEW already has are "
                         "dropped). Dry run unless --execute. Indexes nothing.")
    ap.add_argument("--execute", action="store_true",
                    help="With --rename-project: apply the move in one transaction.")
    ap.add_argument("--allow-empty-library", action="store_true",
                    help="Index a library that lists no PDF or sidecar even though the index holds rows "
                         "for it (its rows are then pruned). Without it such a library is skipped "
                         "(exit 2) and its rows kept, since a synced folder can list empty for a moment.")
    args = ap.parse_args(sys.argv[1:] if argv is None else argv)
    res = run(project=args.project, db=args.db, no_citations=args.no_citations, rebuild=args.rebuild,
              gc=args.gc, rename_project=args.rename_project, execute=args.execute,
              allow_empty_library=args.allow_empty_library)
    return res["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
