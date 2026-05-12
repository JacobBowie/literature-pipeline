# Literature pipeline (`Projects/_tools/literature_pipeline/`)

Shared toolkit for biomedical / sports-science paper acquisition, citation walking, indexing, and structured extraction. Lives at the portfolio level so any project under `Projects/` can use it.

Originally built inside `getpaid/tools/`, elevated 2026-04-28 after a wheel-reinvention audit confirmed genuine novelty (76% pdflatex-pass MathML→LaTeX inside JSON sidecars; no other Python tool does this). Major expansion 2026-05-04: citation walker promotion, DuckDB portfolio index, EndNote citation harvest, and full bug-fix pass on filename canonicalization.

**Forward-looking plan** (RAG layer, MCP server, embedding-similarity ranking) lives in [ROADMAP.md](ROADMAP.md). State-of-the-pipeline as-of last touch lives in [CURRENT_STATE.md](CURRENT_STATE.md).

## Install

```bash
pip install -r requirements.txt
```

Runtime deps: `requests`, `duckdb`, `pymupdf` (imported as `fitz`), `pdfplumber`. The `vendor/mathml_to_latex/` package is bundled, not installed.

## Configuration

The pipeline polls Unpaywall, CrossRef, Europe PMC, NCBI ID Converter, and Semantic Scholar. All five require a contact email in the User-Agent (it's their ToS, not a secret). Default is `jacob.bowie2@gmail.com`; override with:

```bash
export LITPIPE_EMAIL="you@example.org"
```

Tesseract OCR is optional and only used by `build_pdf_library.py`. Override `TESSDATA_PREFIX` if your installation isn't at the conda-default Windows path.

## Layout

```
Projects/_tools/literature_pipeline/
├── README.md
├── ROADMAP.md                  # forward plan (Stage A → MCP)
├── CURRENT_STATE.md            # one-paragraph "what's true right now"
├── projects.json               # registry: which projects use the pipeline (Tier 1 / Tier 2)
│
│   ── FETCH (the puller chain) ──
├── sweep.py                    # walk <project>/lit_pull_queue.csv → run all 3 stages
├── unpaywall_fetch_v2.py       # Stage 1: primary OA fetch (Unpaywall API + HTML fallback)
├── pmc_fetch.py                # Stage 2: PMC fallback (NCBI ID conv + Europe PMC + JATS sidecar)
├── preprint_fetch.py           # Stage 3: preprint fetch (arXiv + Europe PMC preprints + OSF + bioRxiv)
│
│   ── METADATA + CITATIONS ──
├── ris_emit.py                 # shared: CrossRef → RIS, safe_ascii NFKD-normalized, build_ris
├── harvest_citations.py        # ~/Downloads/*.{enw,ris,nbib} → _references/citations/*.ris
├── backfill_ris.py             # write .ris next to existing PDFs (CrossRef-driven)
├── forward_citations.py        # S2 /paper/{id}/citations — papers that cite each seed
├── reverse_citations.py        # parse References sections (sidecar → text dump → PDF fallback)
├── enrich_recommendations.py   # S2 /paper/{id}/recommendations (semantic neighbors)
├── enrich_abstracts.py         # CrossRef abstract backfill into paper_metadata
│
│   ── ORCHESTRATION ──
├── snowball.py                 # forward+reverse+recs+abstracts+index, single command
├── seed_queue_from_top_candidates.py  # bridge: top_candidates → draft lit_pull_queue.csv (heuristic-filtered, manual review)
│
│   ── INDEX ──
├── index_portfolio.py          # walks every project → DuckDB at _references/portfolio.duckdb
│
│   ── EXTRACTION (Tier 1 only) ──
├── extract_tables.py           # pdfplumber tables (lines strategy)
├── pdf_text_clean.py           # ligature + soft-hyphen + page-num post-process
├── jats_to_text.py             # JATS XML → JSON sidecar (with MathML→LaTeX)
├── build_pdf_library.py        # text dump + metadata + abstracts + library_report
├── extract_references.py       # legacy (getpaid/tools/-only); promoted version is reverse_citations.py
├── fetch_figures.py            # PMC figure scrape → image files alongside PDFs
├── backfill_fulltext.py        # retro-fetch JATS sidecars
│
│   ── AUDIT + HYGIENE ──
├── pipeline_check.py           # per-project end-to-end validator (Tier 1 / Tier 2 aware)
├── audit_portfolio.py          # cross-project audit; deep checks (DOI overlap, sidecar quality, fn alignment)
├── audit_filenames.py          # CrossRef-canonical rename pass (cascades PDF + .fulltext.json + .ris + figures)
├── recheck_pmc.py
│
│   ── VENDOR ──
├── vendor/
│   ├── mathml_to_latex/        # vendored MIT-licensed v1.0.0 (one local fix)
│   └── VENDORED.md             # provenance + fix log
│
└── lit_pull_queue.template.csv
```

## Project registry (projects.json)

The pipeline supports two tiers:

- **Tier 1 — full systematic-review build** (discovery → fetch → tables → text dumps → reports). Currently: `getpaid` only.
- **Tier 2 — library-only** (PDFs + sidecars + .ris; no discovery / build artifacts). Currently: `Physiological_Data`, `thermalphys`, `SOC`, `Genova_Diagnostics`, `Yitts`.

Add a project to `projects.json` and tools become layout-aware via `--project NAME`. Promotion 2 → 1 is opt-in, never automatic.

## How downstream projects use it

### Per-project layout (assumed)

```
<project>/
├── references/literature/         # PDFs land here (configurable)
│   └── <foo>.pdf
│   └── <foo>.fulltext.json        # sidecar
├── data/prior_art/                # pipeline build artifacts
│   ├── discovered/*.csv           # fetch reports
│   ├── text/*.txt                 # cleaned text dumps
│   ├── tables/<paper>/*.csv       # extracted tables
│   └── library_report.md
└── lit_pull_queue.csv             # request file (see contract below)
```

Every tool takes `--base-dir`, `--lib-dir`, etc. — defaults assume the layout above but every path is overridable.

### Common commands

All paths assume `cd /c/Users/jab18015/Projects/_tools/literature_pipeline`.

```bash
# Audit the whole portfolio:
python audit_portfolio.py
python pipeline_check.py --project getpaid              # Tier 1, runs all stages
python pipeline_check.py --project Physiological_Data   # Tier 2, skips Tier 1 stages

# Refresh the DuckDB index after any sweep / citation walk:
python index_portfolio.py                  # all projects
python index_portfolio.py --project getpaid
python index_portfolio.py --rebuild        # drop-and-recreate (use sparingly)

# Citation snowball — one command runs forward + reverse + recs + abstracts + index:
python snowball.py --project Physiological_Data
python snowball.py --all                                  # every active project
python snowball.py --until-convergence --max-iter 3       # repeat until candidate growth <1%

# Single steps (when you don't need the whole snowball):
python forward_citations.py --project Physiological_Data  # S2 forward citations
python reverse_citations.py --project Physiological_Data  # parse References sections
python enrich_recommendations.py                          # S2 /paper/{id}/recommendations
python enrich_abstracts.py                                # CrossRef abstract backfill (~95 min for 17k DOIs)

# Bridge: emit a draft lit_pull_queue.csv from top_candidates (manual review required):
python seed_queue_from_top_candidates.py --project Physiological_Data
# Defaults: --year-min 2010 --min-seeds 3 --min-cites 0 --limit 100
# Writes <project>/lit_pull_queue.draft.csv with header comment + filter trace.
# After triage, strip comments and rename to lit_pull_queue.csv before sweep.
# See _portfolio/sop/literature_session.md for the full workflow.

# Library hygiene:
python audit_filenames.py --lib-dir <path>                # CrossRef-canonical rename pass (cascades PDF + .ris + sidecar)
python audit_filenames.py --lib-dir <path> \
       --queue-history '<project>/lit_pull_queue.*.processed.csv'  # fall back to queue history for un-OCR'd scans
python backfill_ris.py --lib-dir <path>                   # write .ris next to existing PDFs
python backfill_fulltext.py --lib-dir <path>              # retro-fetch JATS sidecars (Tier 1)
python build_pdf_library.py --base-dir <project>          # text dumps + metadata + library_report (Tier 1)
```

## The lit_pull_queue contract

Per the **`project-notes`** skill (see `~/.claude/skills/project-notes/SKILL.md`), a downstream session writes a queue file at the project root and pings `LOOSE_ENDS.md`:

### Step 1 — downstream project session writes:

```bash
cat >> /c/Users/jab18015/Projects/Physiological_Data/lit_pull_queue.csv <<'EOF'
doi,title,authors,year,destination,notes
10.1152/jappl.1972.32.6.812,"Predicting rectal temperature","Givoni B; Goldman R",1972,docs/literature/,baseline
EOF

echo "📚 Lit pull queued: Physiological_Data/lit_pull_queue.csv (1 paper — Ch.3 baseline)" \
  >> /c/Users/jab18015/Projects/Git-R-Dun/files/LOOSE_ENDS.md
```

### Step 2 — meta-PM session (here, or wherever the pipeline is run):

```bash
# Sweep all pending queues:
python /c/Users/jab18015/Projects/_tools/literature_pipeline/sweep.py

# Or just one project:
python sweep.py --project Physiological_Data

# Dry run (show plan, don't fetch):
python sweep.py --dry-run
```

`sweep.py` walks `Projects/*/lit_pull_queue.csv`, runs Unpaywall v2 → PMC against each, writes PDFs to `<project>/<destination>/`, renames the queue to `lit_pull_queue.<date>.processed.csv`, writes a report next to it, and appends a `✅ Lit pull done:` line to `LOOSE_ENDS.md`.

### CSV schema

```csv
doi,title,authors,year,destination,notes
```

- `doi` (required) — the paper's DOI
- `title`, `authors`, `year` — used for filename synthesis (`<year>_<lastname>_<slug>.pdf`)
- `destination` (required) — relative path from `<project>/` where the PDF should land
- `notes` — free-text reason or context

All rows in one queue should share a `destination`. If you need different destinations, write multiple queues (one per destination) — but realistically a project usually has one `docs/literature/` directory.

## What gets pulled

Three-tier fetch in order:

1. **Unpaywall v2** (primary): respects `is_oa` flag strictly. CLOSED papers go to manual queue, never bypassed.
2. **PMC fetch** (secondary): for v2's failures, queries NCBI ID Converter for PMCIDs, fetches PDF + JATS XML from Europe PMC.
3. **Preprint fetch** (tertiary): arXiv + Europe PMC preprints + OSF + bioRxiv. Title-similarity matching with year-delta gate. Wired into `sweep.py` 2026-05-04 — runs on residuals from stages 1+2.
4. **Figure fetch** (`fetch_figures.py`, opt-in): for papers with PMC sidecars, scrapes the rendered NCBI article HTML for `<img src="cdn.ncbi.nlm.nih.gov/pmc/blobs/...">` URLs, matches by filename to JATS `<graphic>` href, downloads images as `<paper-stem>.fig{N}.{ext}` next to the PDF, updates the sidecar's `figures[i].image_path` and `image_url`.

Each successful fetch also emits a `.ris` sidecar via `ris_emit.py` (CrossRef-driven canonical metadata).

## DuckDB portfolio index

The single source of truth for "what we have" and "what we should fetch next" lives at `~/Projects/_references/portfolio.duckdb`. Schema (v2, 2026-05-04):

| Table | Purpose |
|---|---|
| `paper_metadata` | Canonical per-DOI metadata (title, year, authors, abstract). One row per DOI — paper or candidate. |
| `paper_locations` | Many-to-many: which projects have a given paper's PDF, with sidecar/.ris/has_text flags. |
| `candidates` | Discovery records: source_type ∈ {forward, reverse, recommendation}, source_seed_doi, source_project, citing_cited_by. No metadata duplication — joins to `paper_metadata`. |
| `cites` | Citation graph edges (citing_doi → cited_doi). |
| `recommendations` | S2 `/paper/{id}/recommendations` — semantic neighbors per seed. |
| `papers_no_doi` | PDFs we have whose DOI couldn't be extracted. |

Plus three views: `papers` (papers we have, with cross-project location info), `top_candidates` (not-yet-fetched DOIs ordered by seed-pointing count + impact), `cross_project_papers` (DOIs in 2+ project libs).

Refresh after every sweep / citation walk:

```bash
python index_portfolio.py
```

Sample queries:

```sql
-- Highest-priority unfetched candidates
SELECT * FROM top_candidates LIMIT 50;

-- Papers we have in 2+ projects
SELECT * FROM cross_project_papers;

-- Coverage by project
SELECT project, COUNT(*) FROM paper_locations GROUP BY project;
```

## EndNote citation harvest

`harvest_citations.py` consolidates a backlog of `.ris` / `.enw` / `.nbib` files (e.g., from `~/Downloads/`) into a single canonical `.ris` library at `~/Projects/_references/citations/`. Used 2026-05-04 to import 132 papers from Downloads into a single EndNote-ingestible folder.

### Figure fetch + Claude vision workflow

`fetch_figures.py` only stores images; it does NOT do plot-data extraction. The intended consumption pattern:

1. **Validate / improve captions:** open the figure as an image attachment in a Claude Code session. The model can confirm whether the JATS-extracted caption matches the actual figure content, flag mis-captions, and suggest enhancements.
2. **Approximate values from plots:** ask Claude vision "what are the y-values at x = 5, 10, 15?" Expect ~10-20% accuracy — usable for sketch-level reasoning, NOT for cited values in manuscripts.
3. **Exact numerical extraction:** when accuracy matters (paper-cited values, methods replication, calibration data), use [WebPlotDigitizer](https://automeris.io/wpd/) manually. ~5 min per figure, gold-standard accuracy.

The image fetcher is **PMC-only** (~30% of getpaid corpus, all of `Physiological_Data/docs/literature/`). Non-PMC papers have JATS captions only, no images. NCBI rate-limits aggressively after ~5-7 papers; expect to need 2-3 retry passes with `--sleep 3.0` to clear a full library.

```bash
# Single library, default 1.5s delay
python fetch_figures.py --lib-dir <project>/references/literature/

# Slower for rate-limit recovery
python fetch_figures.py --lib-dir <project>/references/literature/ --sleep 4.0

# Single sidecar
python fetch_figures.py --sidecar path/to/foo.fulltext.json
```

What's NOT done:
- ❌ Bypass paywalls (no Sci-Hub / LibGen)
- ❌ Spoof institutional IPs or sessions
- ❌ Hammer rate limits (1s sleeps between API calls)
- ❌ Anonymous requests (every UA includes `mailto:jacob.bowie2@gmail.com`)

See [vendor/VENDORED.md](vendor/VENDORED.md) for the bundled MathML→LaTeX library's provenance and one local patch.

## Audited 2026-04-28

The `mathml-to-latex` vendor was audited against a 25-case ground-truth benchmark with `pdflatex` compile-checks. **76% pass rate.** Mitigations baked into `jats_to_text.py::_formula_latex()`:

- Function operators (log/sin/lim) emit U+2061 → stripped from output
- Matrices missing `\begin{matrix}` wrapper → flagged with status `ok-but-matrix-likely-broken`
- Multi-letter `<mi>` joining via post-process regex

For papers where these fail, the offending MathML is preserved in the sidecar's `formulas[i].mathml_input` for diagnosis.

## Comparison to existing tools (audit 2026-04-28)

| Need | Closest OSS | Verdict |
|---|---|---|
| Unpaywall API client | `unpywall` (abandoned 2024) | Ours has citation_pdf_url HTML fallback nobody else does |
| PMC PDF + XML | `paperscraper`, `pubget` | Partial overlap; could swap optionally |
| Preprint search | `paperscraper` | Partial; no OSF or year-delta gate |
| JATS → JSON + MathML→LaTeX | `s2orc-doc2json` | **Genuine gap**: doc2json `raise NotImplementedError('Display formula!')` |
| PDF text clean | none | Bespoke |
| End-to-end orchestration + sidecar refresh + fault check | none | Bespoke |

Verdict: ~1.5 of 9 tools have a mature replacement; the rest are genuine contributions. Skip the swap for now.
