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
import os, sys, io, csv, re, json, argparse, datetime, time
from pathlib import Path

import duckdb

import lit_util  # RC1 DOI validity gate, RC4 atomic writes (shared, pre-tested)

try:
    if getattr(sys.stdout, "encoding", "").lower() != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, OSError):
    pass

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
  refreshed_at     TIMESTAMP
);

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
GROUP BY m.doi, m.year, m.lastname, m.title, m.venue, m.authors, m.abstract, m.refreshed_at;

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

def parse_ris(ris_path: Path) -> dict:
    """Pull the canonical metadata fields out of a .ris file.

    T6 (2026-06-25 audit): handles continuation lines (a wrapped TI/JO value continues on an
    untagged line) so a line-wrapped title is not truncated into the canonical stem, and falls
    back to a DOI in a UR line when no DO tag is present (hand-dropped EndNote/Zotero .ris often
    carry the DOI only as a doi.org URL). Pipeline-written .ris are single-line with a DO tag,
    so canonical sidecars are unaffected."""
    out = {"doi": "", "year": None, "lastname": "", "title": "",
           "venue": "", "authors": []}
    try:
        with open(ris_path, encoding="utf-8") as f:
            text = f.read()
    except OSError:
        return out
    ur_vals = []
    cur_tag = None
    for line in text.splitlines():
        m = re.match(r"^([A-Z][A-Z0-9])\s{2}-\s?(.*)$", line)
        if not m:
            cont = line.strip()
            if cont and cur_tag in ("TI", "T1") and out["title"]:
                out["title"] = (out["title"] + " " + cont).strip()
            elif cont and cur_tag == "JO" and out["venue"]:
                out["venue"] = (out["venue"] + " " + cont).strip()
            continue
        tag, val = m.group(1), m.group(2).strip()
        cur_tag = tag
        if tag == "DO" and not out["doi"]: out["doi"] = val.lower()
        elif tag == "UR": ur_vals.append(val)
        elif tag == "PY" and not out["year"] and val[:4].isdigit(): out["year"] = int(val[:4])
        elif tag in ("TI", "T1") and not out["title"]: out["title"] = val
        elif tag == "JO" and not out["venue"]: out["venue"] = val
        elif tag == "AU":
            out["authors"].append(val)
            if not out["lastname"]:
                out["lastname"] = val.split(",")[0].strip() if "," in val else val.split()[0].strip()
    # T6: UR-only-DOI fallback -- a .ris with no DO but a doi.org UR still carries a DOI.
    if not out["doi"]:
        for u in ur_vals:
            d = lit_util.extract_doi_from_text(u)
            if d:
                out["doi"] = d
                break
    return out


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


def ingest_papers(con, name: str, lib: Path):
    """Walk a library; insert metadata rows (paper_metadata) + location rows (paper_locations)."""
    cur = con.cursor()
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
        cur.executemany(
            "UPDATE paper_metadata SET year=?, lastname=?, title=?, venue=?, authors=?, refreshed_at=? WHERE doi=?",
            [(r[1], r[2], r[3], r[4], r[5], r[6], r[0]) for r in dedup_meta])
        # Insert any that didn't exist
        existing = {row[0] for row in cur.execute(
            f"SELECT doi FROM paper_metadata WHERE doi IN ({','.join('?'*len(dois))})", dois).fetchall()}
        new_meta = [r for r in dedup_meta if r[0] not in existing]
        if new_meta:
            cur.executemany("""
                INSERT INTO paper_metadata (doi, year, lastname, title, venue, authors, refreshed_at)
                VALUES (?,?,?,?,?,?,?)
            """, new_meta)

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
        keys = [(r[0], r[1]) for r in dedup_loc]
        cur.executemany("DELETE FROM paper_locations WHERE doi=? AND project=?", keys)
        cur.executemany("""
            INSERT INTO paper_locations
            (doi, project, lib_path, pdf_filename, has_pdf, has_sidecar, has_ris, sidecar_text_len, refreshed_at)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, dedup_loc)

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
        keys = [(r[0], r[1]) for r in rows_no_doi]
        cur.executemany("DELETE FROM papers_no_doi WHERE pdf_filename=? AND project=?", keys)
        cur.executemany("""
            INSERT INTO papers_no_doi (pdf_filename, project, lib_path, reason)
            VALUES (?,?,?,?)
        """, rows_no_doi)
    return pdf_count, no_doi_count


def safe_int(s, default=0):
    try: return int(s)
    except (ValueError, TypeError): return default


def prune_citations(con, name, kind):
    """T2: delete a project's candidate/cite rows for one citation 'kind' ('forward'|'reverse').
    Called from ingest_forward/ingest_reverse AND from main() when the CSV is ABSENT, so a
    DELETED citation CSV (not just a shrunk one) cannot leave orphan candidate/cite rows that
    inflate top_candidates."""
    cur = con.cursor()
    cur.execute("DELETE FROM candidates WHERE source_project=? AND source_type=?", [name, kind])
    cur.execute("DELETE FROM cites WHERE source_project=? AND source_pipeline=?", [name, kind])


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
    cur = con.cursor()
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

    if cand_rows:
        seen = set(); dedup = []
        for r in cand_rows:
            k = (r[0], r[1], r[2], r[3])
            if k not in seen: seen.add(k); dedup.append(r)
        cand_rows = dedup
        keys = [(r[0], r[1], r[2], r[3]) for r in cand_rows]
        cur.executemany(
            "DELETE FROM candidates WHERE doi=? AND source_type=? AND source_seed_doi=? AND source_project=?",
            keys)
        cur.executemany("""
            INSERT INTO candidates (doi, source_type, source_seed_doi, source_project,
                                    citing_cited_by, refreshed_at)
            VALUES (?,?,?,?,?,?)
        """, cand_rows)
    if meta_rows:
        # Insert metadata for any candidate DOI not already in paper_metadata.
        # Don't overwrite existing rows (those have richer data from .ris).
        seen = set(); dedup = []
        for r in meta_rows:
            if r[0] not in seen: seen.add(r[0]); dedup.append(r)
        existing = {row[0] for row in cur.execute(
            f"SELECT doi FROM paper_metadata WHERE doi IN ({','.join('?'*len(dedup))})",
            [r[0] for r in dedup]).fetchall()}
        new_meta = [r for r in dedup if r[0] not in existing]
        if new_meta:
            cur.executemany("""
                INSERT INTO paper_metadata (doi, year, lastname, title, venue, authors, refreshed_at)
                VALUES (?,?,?,?,?,?,?)
            """, new_meta)
    if cite_rows:
        seen = set(); dedup = []
        for r in cite_rows:
            k = (r[0], r[1], r[3])
            if k not in seen: seen.add(k); dedup.append(r)
        cite_rows = dedup
        keys = [(r[0], r[1], r[3]) for r in cite_rows]
        cur.executemany(
            "DELETE FROM cites WHERE citing_doi=? AND cited_doi=? AND source_project=?", keys)
        cur.executemany("""
            INSERT INTO cites (citing_doi, cited_doi, source_pipeline, source_project)
            VALUES (?,?,?,?)
        """, cite_rows)
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
    cur = con.cursor()
    now = datetime.datetime.now().isoformat(timespec="seconds")
    cand_rows = []; cite_rows = []
    seed_doi_cache = {}  # filename -> doi (read .ris next to seed)

    def seed_doi_for(seed_filename):
        if seed_filename in seed_doi_cache: return seed_doi_cache[seed_filename]
        # Strip .txt or .pdf to get stem
        stem = seed_filename
        for ext in (".txt", ".pdf"):
            if stem.endswith(ext): stem = stem[:-len(ext)]
        # Try .ris in lib
        ris = lib / (stem + ".ris")
        d = ""
        if ris.exists():
            with open(ris, encoding="utf-8") as fh:
                for line in fh:
                    m = re.match(r"^DO\s{2}-\s?(.+)$", line)
                    if m: d = m.group(1).strip().lower(); break
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

    if cand_rows:
        seen = set(); dedup = []
        for r in cand_rows:
            k = (r[0], r[1], r[2], r[3])
            if k not in seen: seen.add(k); dedup.append(r)
        cand_rows = dedup
        keys = [(r[0], r[1], r[2], r[3]) for r in cand_rows]
        cur.executemany(
            "DELETE FROM candidates WHERE doi=? AND source_type=? AND source_seed_doi=? AND source_project=?",
            keys)
        cur.executemany("""
            INSERT INTO candidates (doi, source_type, source_seed_doi, source_project,
                                    citing_cited_by, refreshed_at)
            VALUES (?,?,?,?,?,?)
        """, cand_rows)
    if meta_rows:
        seen = set(); dedup = []
        for r in meta_rows:
            if r[0] not in seen: seen.add(r[0]); dedup.append(r)
        existing = {row[0] for row in cur.execute(
            f"SELECT doi FROM paper_metadata WHERE doi IN ({','.join('?'*len(dedup))})",
            [r[0] for r in dedup]).fetchall()}
        new_meta = [r for r in dedup if r[0] not in existing]
        if new_meta:
            cur.executemany("""
                INSERT INTO paper_metadata (doi, year, lastname, title, venue, authors, refreshed_at)
                VALUES (?,?,?,?,?,?,?)
            """, new_meta)
    if cite_rows:
        seen = set(); dedup = []
        for r in cite_rows:
            k = (r[0], r[1], r[3])
            if k not in seen: seen.add(k); dedup.append(r)
        cite_rows = dedup
        keys = [(r[0], r[1], r[3]) for r in cite_rows]
        cur.executemany(
            "DELETE FROM cites WHERE citing_doi=? AND cited_doi=? AND source_project=?", keys)
        cur.executemany("""
            INSERT INTO cites (citing_doi, cited_doi, source_pipeline, source_project)
            VALUES (?,?,?,?)
        """, cite_rows)
    if n_rejected:
        print(f"  [RC1-gate] reverse: dropped {n_rejected} row(s) with malformed/truncated DOI")
    return len(cand_rows)


# ---------- main ----------

def connect_with_retry(db_path_str, tries=3, delays=(2, 4)):
    """RC10: open the DuckDB file with a small retry. The DB lives on Google-Drive-synced
    storage, and GoogleDriveFS sometimes holds portfolio.duckdb open mid-sync → a transient
    IO/lock error. Retry a couple of times, then surface a clear Drive-suspect message before
    re-raising (don't re-diagnose as a code bug; suspect Drive first)."""
    last_err = None
    for attempt in range(1, tries + 1):
        try:
            return duckdb.connect(db_path_str)
        except Exception as e:  # duckdb.IOException / lock errors are not a stable public type
            last_err = e
            if attempt < tries:
                wait = delays[min(attempt - 1, len(delays) - 1)]
                print(f"  [RC10] DB open failed (attempt {attempt}/{tries}): {e}\n"
                      f"        retrying in {wait}s ...")
                time.sleep(wait)
    print("  [RC10] DB locked - suspect Google Drive holding portfolio.duckdb open "
          "(pause Drive sync or move the DB off Drive-synced storage, then retry).")
    raise last_err


def load_config():
    from ris_emit import load_projects_config
    return load_projects_config(CONFIG_PATH).get("projects", {})


def project_paths(name: str, p: dict):
    base = PROJECTS_ROOT / (p.get("parent") or name)
    lib  = base / p["lib_dir"]
    data = (base / p["data_dir"]) if p.get("data_dir") else None
    return base, lib, data


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
        _n = None
        try:
            _c = duckdb.connect(str(db_path), read_only=True)
            _n = _c.execute("SELECT COUNT(*) FROM paper_metadata "
                            "WHERE abstract IS NOT NULL AND abstract != ''").fetchone()[0]
            _c.close()  # MUST close before unlink (Windows locks open files)
        except Exception:
            pass
        print(f"[--rebuild] dropping {db_path.name}"
              + (f" — DISCARDS {_n} harvested abstracts; re-run enrich_abstracts.py to restore"
                 if _n else ""), file=sys.stderr)
        db_path.unlink()
        # Also clear the WAL if present
        wal = db_path.with_suffix(db_path.suffix + ".wal")
        if wal.exists(): wal.unlink()
    con = connect_with_retry(str(db_path))
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
            n_pdf, n_no = ingest_papers(con, name, lib)
            print(f"  papers: {n_pdf} ingested, {n_no} no-DOI")
            if args.no_citations: continue
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
