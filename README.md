# literature-pipeline

[![CI](https://github.com/JacobBowie/literature-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/JacobBowie/literature-pipeline/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

A Python toolkit for systematic biomedical literature workflows. Given a list of DOIs, it pulls open-access PDFs from Unpaywall → PMC → preprint servers (in that order), walks the citation graph via Semantic Scholar, indexes everything into a portable DuckDB, and extracts structured text + equations + figures from the resulting library.

Built for the workflow where the same library is consumed by multiple downstream projects — a sports-science postdoc shop, a thermoregulation systematic review, a wearable-data clustering paper — without duplicating fetches or re-OCR'ing PDFs.

**What's interesting under the hood:**
- Three-tier OA fetch chain (Unpaywall v2 → PMC + Europe PMC → arXiv / bioRxiv / OSF preprints) with citation-graph HTML fallback nobody else does
- JATS-XML → JSON sidecar with MathML→LaTeX conversion via a vendored MIT library — common math (fractions, sums, integrals, Greek, sub/sup) round-trips cleanly; complex matrices and custom operators may need manual cleanup. The original MathML is preserved in each `formulas[i].mathml_input` so you can re-process with your own tooling if ours fails for your corpus. (`s2orc-doc2json` raises `NotImplementedError` here, so even partial coverage is a step up.)
- Cross-project DuckDB index — one source of truth for "what we have" and "what to fetch next" ordered by seed-pointing citation count
- One-command snowball: forward + reverse + recommendations + abstracts + reindex
- No paywall bypass, no Sci-Hub, no spoofed institutional IPs. Every request identifies itself by mailto contact per API ToS.

**Forward plan** (embedding-similarity ranking, local RAG, MCP server): [ROADMAP.md](ROADMAP.md).

## Install

```bash
pip install -r requirements.txt
```

Runtime deps: `requests`, `duckdb`, `pymupdf` (imported as `fitz`), `pdfplumber`. The `vendor/mathml_to_latex/` package is bundled, not installed.

## Configuration

**Set your contact email first.** Unpaywall, CrossRef, Europe PMC, NCBI, and Semantic Scholar all require a `mailto:` in the User-Agent under their ToS. If `LITPIPE_EMAIL` is unset, the pipeline falls back to the maintainer's inbox and emits a warning on startup — you don't want that.

```bash
export LITPIPE_EMAIL="you@example.org"
```

Tesseract OCR is optional and only used by `build_pdf_library.py`. Override `TESSDATA_PREFIX` if your installation isn't at the default location.

## Compatibility

| Platform | Status |
|---|---|
| Python 3.11 / 3.12 / 3.13 | Tested in CI (Ubuntu + Windows runners, every push) |
| Linux (Ubuntu) | CI-tested |
| Windows 11 | Primary dev platform; CI-tested |
| macOS | Untested in CI. Pure Python deps + `pymupdf` wheels publish for macOS, so it should work — report issues if not |
| Python ≤ 3.10 | Not tested; relies on `sys.stdout.reconfigure()` and modern type hints (likely works on 3.10, definitely won't on 3.9) |

The pipeline writes UTF-8 everywhere and tolerates Windows console cp1252 via `sys.stdout.reconfigure()` at script entry. No platform-specific dependencies beyond optional Tesseract for OCR.

## Quickstart

Two minutes from clone to your first PDF.

```bash
git clone https://github.com/JacobBowie/literature-pipeline.git
cd literature-pipeline
pip install -r requirements.txt
export LITPIPE_EMAIL="you@example.org"

# 1. Set up the project registry from the template
cp projects.json.template projects.json
# Edit projects.json — one entry pointing at any directory you want PDFs to land in.
# Tier 2 (library-only) is the simplest entry point; see schema in the file.

# 2. Make a project directory and a queue
mkdir -p ~/my_review/literature
cd ~/my_review
cat > lit_pull_queue.csv <<'EOF'
doi,title,authors,year,destination,notes
10.1152/jappl.1972.32.6.812,Predicting rectal temperature,Givoni B; Goldman R,1972,literature/,baseline
EOF

# 3. Dry-run first — confirms the queue is found and the destination is sane
python /path/to/literature-pipeline/sweep.py --project my_review --dry-run

# 4. Pull the PDF
python /path/to/literature-pipeline/sweep.py --project my_review
# → ~/my_review/literature/1972_Givoni_PredictingRectalTemperature.pdf
# → ~/my_review/literature/1972_Givoni_PredictingRectalTemperature.fulltext.json
# → ~/my_review/literature/1972_Givoni_PredictingRectalTemperature.ris
```

The same project registry feeds every downstream tool (`snowball.py` for citation expansion, `audit_filenames.py` for canonical renames, `index_portfolio.py` for the DuckDB index).

## Layout

```
Projects/_tools/literature_pipeline/
├── README.md
├── ROADMAP.md                  # forward plan (Stage A → MCP)
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

All commands below assume you're inside the cloned `literature-pipeline/` directory. `$PROJECT` is whichever project name you registered in `projects.json`.

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
       --queue-history '<project>/lit_pull_queue.*.processed*.csv'  # fall back to queue history for un-OCR'd scans (processed*.csv also catches same-day re-sweep .processed.2.csv)
python backfill_ris.py --lib-dir <path>                   # write .ris next to existing PDFs
python backfill_fulltext.py --lib-dir <path>              # retro-fetch JATS sidecars (Tier 1)
python build_pdf_library.py --base-dir <project>          # text dumps + metadata + library_report (Tier 1)
```

## The lit_pull_queue contract

`sweep.py` looks for `<project_root>/lit_pull_queue.csv` (where `<project_root>` is the parent directory of the `lib_dir` registered in `projects.json`). Append rows when you want PDFs pulled; the next sweep picks them up.

```bash
# Add a paper to the queue
cat >> $PROJECT_ROOT/lit_pull_queue.csv <<'EOF'
doi,title,authors,year,destination,notes
10.1152/jappl.1972.32.6.812,"Predicting rectal temperature","Givoni B; Goldman R",1972,literature/,baseline
EOF

# Sweep all projects with pending queues
python sweep.py

# Or limit to one project
python sweep.py --project $PROJECT

# Dry run (show plan, don't fetch)
python sweep.py --dry-run
```

For each row, `sweep.py` runs Unpaywall → PMC → preprint-server in order, writes the PDF to `<project_root>/<destination>/`, renames the queue to `lit_pull_queue.<date>.processed.csv` so the next sweep doesn't reprocess it, and writes a report alongside.

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
- ❌ Anonymous requests (every UA includes `mailto:$LITPIPE_EMAIL`)

### The institutional-access boundary

The pipeline is **anonymous by design**. In a typical biomedical corpus, expect ~5–15% of papers to be genuinely paywalled to anonymous traffic — pre-mandate-era US journals (J Appl Physiol, MSSE, JSC, Hum Kinetics, etc.), older Wiley/Springer/FASEB papers without green-OA mirrors. Those land in the residual report (`needs_manual_pull.csv` for getpaid-style Tier 1 setups) with a `bucket` column tagging the reason: `PAYWALL`, `OA_NO_URL`, `PMC_GATED_WEB_ONLY`.

**For those papers, the legitimate workflow is:**

1. Open the DOI in a browser session authenticated to your institutional library (EZproxy, Shibboleth, OpenAthens — whatever your institution provides). Google Scholar with library link-resolver enabled works well here.
2. Save the PDF into the project's `<lib_dir>/` with any filename — `audit_filenames.py` will normalize it on the next sweep.
3. Run `python backfill_ris.py --lib-dir <path>` to attach CrossRef-canonical metadata + `.ris` sidecars.

This isn't a pipeline limitation we're going to fix — it's a fundamental property of "no auth, no proxy" tooling. Tried adding an OpenAlex Tier 2 to chase green-OA repo mirrors against this exact residual (68 DOIs); empirically zero net new downloads. The papers that exist outside the open-access ecosystem cannot be reached by any anonymous tool.

See [vendor/VENDORED.md](vendor/VENDORED.md) for the bundled MathML→LaTeX library's provenance and one local patch.

## MathML→LaTeX: scope + known limits

The pipeline vendors `py-mathml-to-latex` (MIT) for math conversion — we don't own that library, just wrap it. Our smoke tests confirm the common cases (inline fractions, sums with sub/superscripts, Greek letters, basic operators) round-trip. Beyond those, behavior varies:

- **Function operators** (`log`, `sin`, `lim`): U+2061 invisible-apply tokens are stripped post-process
- **Matrices** missing `\begin{matrix}` wrapper: flagged with status `ok-but-matrix-likely-broken` rather than silently corrupted
- **Multi-letter `<mi>` tokens** are joined via a post-process regex

When the converter fails, the offending MathML is preserved in `sidecar.formulas[i].mathml_input` so you can re-process with your own tooling (pandoc, mathjax, a different library, or Claude/Copilot with vision). The pipeline doesn't commit to math-conversion correctness — it commits to *preserving the source so you can verify or replace*.

### Verify-it-yourself

```bash
python - <<'EOF'
from xml.etree import ElementTree as ET
from jats_to_text import _formula_latex

mathml = '<math xmlns="http://www.w3.org/1998/Math/MathML"><mfrac><mn>1</mn><mi>x</mi></mfrac></math>'
latex, status, source = _formula_latex(ET.fromstring(mathml))
print(f"latex:  {latex}")    # → \frac{1}{x}
print(f"status: {status}")   # → ok
EOF
```

Paste any MathML expression from any JATS sidecar (`grep -A2 mathml_input <paper>.fulltext.json`) into that snippet to see what we produce on your real inputs. If the result is wrong, the original MathML is still in the sidecar — process it with your own tool.

## Comparison to existing tools

Maturity legend: ★★★ active + good fit · ★★ usable but partial · ★ stale/abandoned · — no comparable tool

| Need | Closest OSS | Maturity | Verdict |
|---|---|---|---|
| Unpaywall API client | [`unpywall`](https://github.com/unpywall/unpywall) | ★ | Last release 2024, archived; ours adds `citation_pdf_url` meta-tag HTML fallback for OA landing pages that don't serve a direct PDF |
| PMC PDF + JATS XML | [`paperscraper`](https://github.com/PhosphorylatedRabbits/paperscraper), [`pubget`](https://github.com/neuroquery/pubget) | ★★ | Both active; could swap for the PMC stage. Ours adds NCBI ID-converter retry + Europe PMC + arXiv-id-on-PMC fallback |
| Preprint search | `paperscraper` | ★★ | Active. Ours adds OSF (sportRxiv etc), Europe PMC preprints, year-delta gate on title-similarity matching, and direct download for bioRxiv/medRxiv DOI patterns |
| JATS → JSON + MathML→LaTeX | [`s2orc-doc2json`](https://github.com/allenai/s2orc-doc2json) | — | doc2json raises `NotImplementedError('Display formula!')` on display math; we vendor `py-mathml-to-latex` and wrap it with a sidecar that preserves the source MathML for re-processing |
| Citation graph (forward+reverse) | None bundled; S2 API directly | — | Bespoke wrapper over Semantic Scholar `/paper/{id}/citations` + `/recommendations`, plus a References-section parser (sidecar → text dump → fitz fallback) |
| Cross-project DuckDB index | None | — | Bespoke. One source of truth for "what we have" + `top_candidates` view ordered by seed-pointing count |
| End-to-end orchestration + sidecar refresh + fault check | None | — | Bespoke. `sweep.py`, `snowball.py`, `pipeline_check.py`, `audit_portfolio.py` |
| PDF text clean | None | — | Bespoke (ligature de-merge, soft-hyphen rejoin, bare-page-number strip) |

**Bottom line:** PMC and preprint stages overlap with active OSS (`paperscraper`, `pubget`) — those swaps are reasonable if you'd rather depend on a maintained dependency than carry our wrappers. Everything else — Unpaywall + landing-page fallback, MathML→LaTeX with source preservation, citation walking with sidecar fallbacks, the DuckDB index — has no off-the-shelf equivalent. Drop-in candidates: see `paperscraper` if you want one less custom module.
