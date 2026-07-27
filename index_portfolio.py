"""Build/refresh a DuckDB index of the portfolio's literature.

Walks every project listed in projects.json and ingests:
  - Each PDF + its .ris sidecar metadata + .fulltext.json status → `papers` table
  - Each project's _forward_citations.csv (or s2_forward_citations_v2.csv)
                  → `candidates` (source='forward') + `cites` rows
  - Each project's _reverse_citations_parsed.csv (or parsed_references.csv)
                  → `candidates` (source='reverse') + `cites` rows

The DB is the source of truth for "what we have" and "what we should fetch
next." Idempotent — re-run any time after sweep / citation-walk completes.

Why DuckDB (not SQLite):
  - Same storage tier showcased by ATHENA HR pipeline (resume consistency)
  - Native CSV reading (could query existing _forward_citations.csv as views)
  - Vector extension (`vss`) for embeddings if/when we add a RAG layer
  - Better SQL surface (window funcs, list/struct types) for citation-graph analytics

Output: ~/Projects/_references/portfolio.duckdb

Usage:
  python index_portfolio.py                  # rebuild full index
  python index_portfolio.py --project getpaid  # refresh one project
  python index_portfolio.py --no-citations     # papers table only (faster)
  python index_portfolio.py --rebuild          # drop+recreate all tables first
"""
import os, sys, csv, re, json, argparse, datetime
from pathlib import Path

import duckdb
import pandas as pd  # E1: register()+INSERT..SELECT bulk path (see _bulk_insert)

import lit_util  # RC1 DOI validity gate, RC4 atomic writes (shared, pre-tested)

lit_util.utf8_stdout()

PROJECTS_ROOT = Path(os.path.expanduser("~/Projects"))
CONFIG_PATH   = Path(__file__).parent / "projects.json"
DB_PATH       = PROJECTS_ROOT / "_references" / "portfolio.duckdb"

SCHEMA = """
-- Schema v2 (2026-05-04). Normalized:
--   paper_metadata = canonical metadata per DOI (one row per DOI, paper or candidate)
--   paper_locations = which projects have the PDF (many-to-many)
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
  reason           VARCHAR,
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
"""


# ---------- helpers ----------

# parse_ris was promoted to lit_util (Stage 3 c10, unioning the harvest_citations copy: single-pass
# continuation lines + AU/A1, PY/Y1, TI/T1 aliases + int-or-None year + UR-DOI fallback). Re-exported
# here so the `parse_ris(...)` / `I.parse_ris(...)` call sites (and test_index_prune) keep working.
parse_ris = lit_util.parse_ris


def sidecar_info(path: Path):
    """Return (has, text_len) for a .fulltext.json sidecar."""
    if not path.exists(): return (0, 0)
    try:
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
        text_len = len((d.get("text") or "") + (d.get("body") or "") + (d.get("abstract") or ""))
        return (1, text_len)
    except (OSError, json.JSONDecodeError, ValueError):
        return (1, 0)


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


def ingest_papers(con, name: str, lib: Path):
    """Walk a library; insert metadata rows (paper_metadata) + location rows (paper_locations)."""
    cur = con   # C2: write through the connection so the caller's per-project BEGIN enrolls these
                # (a BEGIN on con does NOT enroll a child cursor's writes -- verified on 1.5.3).
    now = datetime.datetime.now().isoformat(timespec="seconds")
    pdf_count = no_doi_count = 0
    meta_rows = []; loc_rows = []; rows_no_doi = []

    for pdf in sorted(lib.glob("*.pdf")):
        ris = lit_util.companion_path(pdf, ".ris")
        sc  = lit_util.companion_path(pdf, ".fulltext.json")
        meta = parse_ris(ris) if ris.exists() else {}
        has_sc, sc_len = sidecar_info(sc)
        if not meta.get("doi"):
            rows_no_doi.append((pdf.name, name, str(lib),
                                "no_ris" if not ris.exists() else "ris_lacks_doi"))
            no_doi_count += 1
            continue
        meta_rows.append((
            meta["doi"], meta.get("year"), meta.get("lastname"), meta.get("title"),
            meta.get("venue"), "; ".join(meta.get("authors", [])), now,
        ))
        loc_rows.append((
            meta["doi"], name, str(lib), pdf.name,
            True, bool(has_sc), ris.exists(), sc_len, now,
        ))
        pdf_count += 1

    # paper_metadata: dedupe by DOI within this batch (same DOI in 2 PDFs = LeBris case)
    if meta_rows:
        seen = set(); dedup_meta = []
        for r in meta_rows:
            if r[0] not in seen: seen.add(r[0]); dedup_meta.append(r)
        # Insert into paper_metadata WITHOUT overwriting an existing abstract
        # (abstract is enriched separately by enrich_abstracts.py — we don't want
        # the index refresh to wipe abstracts that are already there).
        # Pattern: DELETE only the columns we're refreshing, preserve abstract.
        dois = [r[0] for r in dedup_meta]
        # E1: SELECT existing FIRST, then UPDATE only rows that exist. The old code ran the
        # UPDATE over EVERY dedup_meta row including brand-new DOIs, each a no-op point-lookup
        # against the ART index (~88k wasted on a --rebuild). Final state is identical.
        existing = {row[0] for row in cur.execute(
            f"SELECT doi FROM paper_metadata WHERE doi IN ({','.join('?'*len(dois))})", dois).fetchall()}
        upd = [(r[1], r[2], r[3], r[4], r[5], r[6], r[0]) for r in dedup_meta if r[0] in existing]
        if upd:
            # abstract is intentionally NOT in the SET list (enriched separately by
            # enrich_abstracts; the abstract invariant -- test_duckdb_upsert).
            cur.executemany(
                "UPDATE paper_metadata SET year=?, lastname=?, title=?, venue=?, authors=?, refreshed_at=? WHERE doi=?",
                upd)
        new_meta = [r for r in dedup_meta if r[0] not in existing]
        # E1: bulk-insert new rows (abstract omitted -> defaults NULL; enriched out-of-band).
        _bulk_insert(cur, "paper_metadata",
                     ["doi", "year", "lastname", "title", "venue", "authors", "refreshed_at"],
                     new_meta, int_cols=["year"])

    # paper_locations: dedupe by (doi, project), and ALSO drop any prior row
    # for this project whose pdf_filename is no longer on disk (so quarantined
    # / deleted PDFs don't leak stale rows). Without this prune, the UPSERT
    # only refreshes rows for PDFs still present; rows for missing PDFs persist
    # indefinitely. 2026-05-22 patch — caught by the LWW boilerplate quarantine.
    current_filenames = sorted({r[3] for r in loc_rows})
    if current_filenames:
        placeholders = ",".join(["?"] * len(current_filenames))
        cur.execute(
            f"DELETE FROM paper_locations WHERE project = ? AND pdf_filename NOT IN ({placeholders})",
            [name, *current_filenames],
        )
    else:
        cur.execute("DELETE FROM paper_locations WHERE project = ?", [name])

    if loc_rows:
        seen = set(); dedup_loc = []
        for r in loc_rows:
            k = (r[0], r[1])
            if k not in seen: seen.add(k); dedup_loc.append(r)
        # E1: fold the per-key DELETE into an ON CONFLICT upsert (PK = doi, project); the
        # scope-wide prune above already dropped rows for PDFs no longer on disk.
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

    if rows_no_doi:
        # E1: dedupe on the PK (pdf_filename, project), then upsert via ON CONFLICT, folding the
        # per-key DELETE. The scope-wide prune above already cleared removed/renamed PDFs.
        seen = set(); dedup_nodoi = []
        for r in rows_no_doi:
            k = (r[0], r[1])
            if k not in seen: seen.add(k); dedup_nodoi.append(r)
        _bulk_insert(cur, "papers_no_doi",
                     ["pdf_filename", "project", "lib_path", "reason"],
                     dedup_nodoi,
                     on_conflict=("ON CONFLICT (pdf_filename, project) DO UPDATE SET "
                                  "lib_path=excluded.lib_path, reason=excluded.reason"))
    return pdf_count, no_doi_count


def safe_int(s, default=0):
    try: return int(s)
    except (ValueError, TypeError): return default


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
      - paper_metadata: insert only genuinely-new DOIs; never overwrite a richer .ris-sourced
        row (the abstract invariant -- test_duckdb_upsert).
    Writes through `con` (not a child cursor) so a future per-project transaction can enroll them."""
    if cand_rows:
        seen = set(); dedup = []
        for r in cand_rows:
            k = (r[0], r[1], r[2], r[3])
            if k not in seen: seen.add(k); dedup.append(r)
        # E1: bulk insert. prune_citations already cleared this (project, source_type) slice, so
        # no per-key DELETE / ON CONFLICT needed (the candidates PK includes source_type).
        _bulk_insert(con, "candidates",
                     ["doi", "source_type", "source_seed_doi", "source_project",
                      "citing_cited_by", "refreshed_at"],
                     dedup, int_cols=["citing_cited_by"])
    if meta_rows:
        # metadata for any candidate DOI not already present; do NOT overwrite existing rows
        # (those carry richer .ris data -- the abstract invariant).
        seen = set(); dedup = []
        for r in meta_rows:
            if r[0] not in seen: seen.add(r[0]); dedup.append(r)
        existing = {row[0] for row in con.execute(
            f"SELECT doi FROM paper_metadata WHERE doi IN ({','.join('?'*len(dedup))})",
            [r[0] for r in dedup]).fetchall()}
        new_meta = [r for r in dedup if r[0] not in existing]
        # E1: bulk-insert genuinely-new candidate DOIs (abstract omitted -> NULL; invariant).
        _bulk_insert(con, "paper_metadata",
                     ["doi", "year", "lastname", "title", "venue", "authors", "refreshed_at"],
                     new_meta, int_cols=["year"])
    if cite_rows:
        seen = set(); dedup = []
        for r in cite_rows:
            k = (r[0], r[1], r[3])
            if k not in seen: seen.add(k); dedup.append(r)
        # E1: bulk insert; cites PK excludes source_pipeline, so the same (citing,cited,project)
        # edge found by BOTH passes updates the tag rather than colliding (see docstring).
        _bulk_insert(con, "cites",
                     ["citing_doi", "cited_doi", "source_pipeline", "source_project"],
                     dedup,
                     on_conflict=("ON CONFLICT (citing_doi, cited_doi, source_project) "
                                  "DO UPDATE SET source_pipeline = excluded.source_pipeline"))


def gc_orphan_metadata(con):
    """T2: delete paper_metadata rows referenced by NOTHING (no location/candidate/cite/
    recommendation) and return the count reclaimed. Conservative -- a row pinned by even a
    stale candidate survives. Gated behind main()'s --gc; shared so the test guards the real SQL."""
    cur = con.cursor()
    before = cur.execute("SELECT COUNT(*) FROM paper_metadata").fetchone()[0]
    cur.execute("""
        DELETE FROM paper_metadata m
        WHERE NOT EXISTS (SELECT 1 FROM paper_locations l WHERE l.doi = m.doi)
          AND NOT EXISTS (SELECT 1 FROM candidates c WHERE c.doi = m.doi OR c.source_seed_doi = m.doi)
          AND NOT EXISTS (SELECT 1 FROM cites ci WHERE ci.citing_doi = m.doi OR ci.cited_doi = m.doi)
          AND NOT EXISTS (SELECT 1 FROM recommendations r WHERE r.seed_doi = m.doi OR r.recommended_doi = m.doi)
    """)
    after = cur.execute("SELECT COUNT(*) FROM paper_metadata").fetchone()[0]
    return before - after


def ingest_forward(con, name: str, csv_path: Path, lib: Path):
    """Ingest forward-citation CSV. Schema:
      - candidates row per (citing_doi, seed_doi, project) — discovery record
      - paper_metadata row per citing_doi — UPSERT (preserves any existing abstract)
      - cites edge: citing → seed (the candidate cites our seed)
    """
    if not csv_path.exists(): return 0
    now = datetime.datetime.now().isoformat(timespec="seconds")
    cand_rows = []; meta_rows = []; cite_rows = []
    n_rejected = 0  # RC1-gate: malformed / truncated DOIs already sitting in the CSV
    with open(csv_path, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            seed = (r.get("seed_doi") or "").strip().lower()
            cited = (r.get("citing_doi") or "").strip().lower()
            if not (seed and cited): continue
            # RC1-gate (defense-in-depth): drop rows whose DOI is malformed or looks
            # like a line-wrap truncation (the '10.1002/cphy' class) before it pollutes
            # candidates/cites/meta. Applies to both the candidate (cited) and seed DOI.
            if (not lit_util.is_valid_doi(cited) or lit_util.is_suspicious_doi(cited)
                    or not lit_util.is_valid_doi(seed) or lit_util.is_suspicious_doi(seed)):
                n_rejected += 1
                continue
            cand_rows.append((
                cited, "forward", seed, name,
                safe_int(r.get("citing_cited_by")), now,
            ))
            meta_rows.append((
                cited,
                safe_int(r.get("citing_year")),
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
        print(f"  [RC1-gate] forward: dropped {n_rejected} row(s) with malformed/truncated DOI")
    return len(cand_rows)


def ingest_reverse(con, name: str, csv_path: Path, lib: Path):
    """Reverse citations CSV. Schemas accepted:
      - new pipeline: <lib>/_reverse_citations_parsed.csv with columns
        seed, first_author, year, title_snippet, doi, raw
      - legacy getpaid: data/prior_art/references/parsed_references.csv (same columns)
    """
    if not csv_path.exists(): return 0
    now = datetime.datetime.now().isoformat(timespec="seconds")
    cand_rows = []; cite_rows = []
    seed_doi_cache = {}  # filename -> doi (read .ris next to seed)

    def seed_doi_for(seed_filename):
        if seed_filename in seed_doi_cache: return seed_doi_cache[seed_filename]
        # Strip .txt or .pdf to get stem
        stem = seed_filename
        for ext in (".txt", ".pdf"):
            if stem.endswith(ext): stem = stem[:-len(ext)]
        # Try .ris in lib -- c11: route through the shared parse_ris (adds the UR-only-DOI
        # fallback these seed reads previously lacked) rather than a bespoke DO-line regex.
        ris = lib / (stem + ".ris")
        d = parse_ris(ris).get("doi", "") if ris.exists() else ""
        seed_doi_cache[seed_filename] = d
        return d

    meta_rows = []
    n_rejected = 0  # RC1-gate: malformed / truncated DOIs already sitting in the CSV
    with open(csv_path, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            cited_doi = (r.get("doi") or "").strip().lower()
            if not cited_doi: continue
            # RC1-gate (defense-in-depth): drop the candidate/meta row if the cited DOI
            # is malformed or looks like a line-wrap truncation (the '10.1002/cphy' class).
            if not lit_util.is_valid_doi(cited_doi) or lit_util.is_suspicious_doi(cited_doi):
                n_rejected += 1
                continue
            seed_filename = r.get("seed") or ""
            seed_doi = seed_doi_for(seed_filename)
            cand_rows.append((
                cited_doi, "reverse", seed_doi, name,
                0,    # cited_by unknown for reverse
                now,
            ))
            meta_rows.append((
                cited_doi,
                safe_int(r.get("year")),
                r.get("first_author") or "",
                (r.get("title_snippet") or "")[:400],
                "", "",   # no venue / authors from reverse parser
                now,
            ))
            # Only emit a citation edge when the seed DOI is itself well-formed (RC1-gate);
            # a truncated seed DOI would pollute the cites graph with a bad endpoint.
            if seed_doi and lit_util.is_valid_doi(seed_doi) and not lit_util.is_suspicious_doi(seed_doi):
                cite_rows.append((seed_doi, cited_doi, "reverse", name))

    # T2 (2026-06-25): rewrite the full per-(project, source_type) set from this CSV (see
    # ingest_forward). A shrunk/deleted reverse CSV must not leave orphan candidate/cite rows.
    prune_citations(con, name, "reverse")

    # c11/E1: shared writer (see ingest_forward) -- prune ran, so no redundant per-key DELETEs.
    _write_citation_rows(con, cand_rows, meta_rows, cite_rows)
    if n_rejected:
        print(f"  [RC1-gate] reverse: dropped {n_rejected} row(s) with malformed/truncated DOI")
    return len(cand_rows)


# ---------- main ----------

# RC10 connect-with-retry was promoted to lit_util.connect_db(on_fail="raise") in Stage 3 (c9);
# the rebuild path below calls it directly. (enrich_*/snowball/seed_queue share the same helper.)


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


def load_config():
    from ris_emit import load_projects_config
    return load_projects_config(CONFIG_PATH).get("projects", {})


def project_paths(name: str, p: dict):
    return lit_util.lib_paths(name, p)


def find_forward_csv(lib: Path, data: Path):
    """Look for forward-citation CSV in expected locations."""
    candidates = [
        lib / "_forward_citations.csv",                                # new pipeline
    ]
    if data:
        candidates += [
            data / "discovered" / "s2_forward_citations_v2.csv",       # legacy getpaid v2
            data / "discovered" / "s2_forward_citations.csv",          # legacy getpaid v1
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
            data / "references" / "parsed_references.csv",             # legacy getpaid
        ]
    for c in candidates:
        if c.exists(): return c
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default=None,
                    help="Refresh only this project (default: all in projects.json).")
    ap.add_argument("--db",      default=str(DB_PATH))
    ap.add_argument("--no-citations", action="store_true",
                    help="Skip forward+reverse citation ingestion (faster).")
    ap.add_argument("--rebuild", action="store_true",
                    help="Drop and recreate all tables before ingesting. WARNING: this DISCARDS the "
                         "enrich_abstracts CrossRef abstract layer — abstracts live ONLY in the DB, not in "
                         ".ris/sidecar — so re-run enrich_abstracts.py afterward. A plain (non-rebuild) "
                         "re-index PRESERVES abstracts; use --rebuild only to flush stale candidate rows.")
    ap.add_argument("--gc", action="store_true",
                    help="Garbage-collect paper_metadata rows with zero references (no location, "
                         "candidate, cite, or recommendation). Off by default; best after a full index.")
    args = ap.parse_args()

    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if args.rebuild and db_path.exists():
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
    con = lit_util.connect_db(str(db_path))  # RC10 retry-open (on_fail="raise" default)
    try:
        con.execute(SCHEMA)

        projects = load_config()
        if args.project: projects = {args.project: projects[args.project]}

        print(f"DB: {db_path}\n")
        for name, p in projects.items():
            if not p.get("active", True): continue
            base, lib, data = project_paths(name, p)
            if not lib.is_dir():
                print(f"[skip] {name}: lib not found ({lib})")
                continue
            print(f"=== {name} ===")
            # C2: one transaction per project on `con` -- the SAME handle every ingest fn writes
            # through. A crash mid-project (e.g. the recurring GoogleDriveFS lock surfacing on an
            # INSERT) rolls that project back cleanly instead of committing a DELETE without its
            # INSERT; already-finished projects stay durable. Inner fns must NOT open their own
            # transaction (a nested BEGIN raises).
            con.execute("BEGIN TRANSACTION")
            try:
                n_pdf, n_no = ingest_papers(con, name, lib)
                print(f"  papers: {n_pdf} ingested, {n_no} no-DOI")
                if not args.no_citations:
                    fwd = find_forward_csv(lib, data)
                    if fwd:
                        n = ingest_forward(con, name, fwd, lib)
                        print(f"  forward citations from {fwd.name}: {n} candidates")
                    else:
                        prune_citations(con, name, "forward")   # T2: deleted CSV -> drop orphan rows
                    rev = find_reverse_csv(lib, data)
                    if rev:
                        n = ingest_reverse(con, name, rev, lib)
                        print(f"  reverse citations from {rev.name}: {n} candidates")
                    else:
                        prune_citations(con, name, "reverse")   # T2: deleted CSV -> drop orphan rows
                    if not (fwd or rev): print(f"  (no citation CSVs found)")
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise

        if args.gc:
            print(f"\n  [--gc] reclaimed {gc_orphan_metadata(con)} orphan paper_metadata rows")

        n_meta     = con.execute("SELECT COUNT(*) FROM paper_metadata").fetchone()[0]
        n_loc      = con.execute("SELECT COUNT(*) FROM paper_locations").fetchone()[0]
        n_pap      = con.execute("SELECT COUNT(*) FROM papers").fetchone()[0]  # the view
        n_nod      = con.execute("SELECT COUNT(*) FROM papers_no_doi").fetchone()[0]
        n_cand     = con.execute("SELECT COUNT(DISTINCT doi) FROM candidates").fetchone()[0]
        n_cite     = con.execute("SELECT COUNT(*) FROM cites").fetchone()[0]
        n_new_cand = con.execute(
            "SELECT COUNT(DISTINCT doi) FROM candidates c "
            "WHERE NOT EXISTS (SELECT 1 FROM paper_locations l WHERE l.doi = c.doi)"
        ).fetchone()[0]
        n_xproj = con.execute("SELECT COUNT(*) FROM cross_project_papers").fetchone()[0]
        n_with_abs = con.execute(
            "SELECT COUNT(*) FROM paper_metadata WHERE abstract IS NOT NULL AND abstract != ''"
        ).fetchone()[0]

        print()
        print("=== final counts ===")
        print(f"  paper_metadata rows:      {n_meta}")
        print(f"  paper_locations rows:     {n_loc}    (PDF copies across projects)")
        print(f"  unique papers (have PDF): {n_pap}")
        print(f"  cross-project papers:     {n_xproj}  (same DOI in 2+ project libs)")
        print(f"  with abstract:            {n_with_abs}")
        print(f"  PDFs without DOI:         {n_nod}")
        print(f"  unique candidate DOIs:    {n_cand}")
        print(f"  candidates not in lib:    {n_new_cand}  (fetch targets)")
        print(f"  citation edges:           {n_cite}")
        sz_kb = db_path.stat().st_size // 1024
        print(f"  DB size:                  {sz_kb} KB")
    finally:
        con.close()


if __name__ == "__main__":
    main()
