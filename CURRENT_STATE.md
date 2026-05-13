# Current state — Literature pipeline

_Last touched: 2026-05-13 (afternoon)_

The pipeline is **stable through Stage 0** (fetch + citation walking + DuckDB index). Stages A → D (RAG, MCP, ensemble ranking) are planned in [ROADMAP.md](ROADMAP.md), not yet built.

## Live edge

Just completed (2026-05-04 → 2026-05-05):
- **Schema v2 refactor**: `paper_metadata` + `paper_locations` + `candidates` + `cites` + `recommendations` + 3 views. Cross-project DOI tracking now works (was collapsed by old single-PK).
- **Citation walking promoted** from `getpaid/tools/` → portfolio-level (`forward_citations.py`, `reverse_citations.py`). Tier 2 projects (no text-dump dir) supported via PDF-text fallback.
- **Filename + Unicode bug fixes**: `last_name()` now handles comma-separated authors + strips "et al"; `safe_ascii()` NFKD-normalizes accents + handles `ø/æ/ß/ł/đ/œ`. 35 PDFs renamed across the portfolio via `audit_filenames.py --execute`.
- **3 quick wins from ROADMAP**: `enrich_abstracts.py` (CrossRef → 9,834 abstracts on 17,617 DOIs, 56%), `enrich_recommendations.py` (S2 `/paper/{id}/recommendations`), `snowball.py` (one-command orchestration).
- **EndNote harvest**: 132 citations from `~/Downloads/` consolidated to `_references/citations/` as canonical `.ris` files.

Index now contains:
- 17,617 unique DOIs in `paper_metadata` (368 with PDFs + ~17,249 candidates)
- 373 paper locations (5 cross-project copies)
- 9,834 abstracts enriched
- 21,104 citation edges
- DB size: ~14.9 MB

## Where to pick up next

Two threads, pick whichever has time:

1. **Stage A — embedding-similarity ranking layer** (highest leverage; 1 weekend; no LLM dependencies). DuckDB `vss` extension + SPECTER2 embeddings → relevance score per (candidate, seed-set). Detailed in ROADMAP.md.
2. **Stage B/C — local RAG + MCP server**. Blocked on `nvidia-smi` hardware confirmation. ROADMAP.md has the full plan, including 6 mandatory MCP security mitigations and pinned paper-qa2 version (v2026.02.27, NOT v2026.03.18 — it has unresolved indexing regression #1321).

## Hidden constraints (surface for future sessions)

- DuckDB vss persistent indexes need `SET hnsw_enable_experimental_persistence = true;` and WAL recovery isn't implemented. Mitigation when Stage A ships: nightly index rebuild as insurance.
- Abstract enrichment hit rate is 56% — older papers + closed publishers don't return CrossRef abstracts. S2 fallback could be added (separate enrichment pass).
- `paper_metadata` UPSERT logic preserves existing abstracts when re-indexing — don't refactor that without preserving the same property.
- `snowball.py` shells out via `subprocess.run(..., text=True)` without `encoding=`. On Windows that defaults to cp1252, which can crash on unicode subprocess output (accented author names, etc.). Latent; trips intermittently. Fix is one keyword-arg sweep across `snowball.py`'s `run()` helper.
- `--queue-history` filename derivation imports `build_filename` from `unpaywall_fetch_v2.py`. If those filename rules ever change, the queue-history map will silently miss instead of erroring — leave a note here when that ever happens.

## Next 1–3 moves

1. **Verify final state** (today): re-run `audit_portfolio.py` post-recommendations + spot-check sample queries on new DB.
2. **Stage A build session** (next available weekend): SPECTER2 + DuckDB vss + relevance score column. ~150-200 LOC, code skeleton in ROADMAP.md.
3. **Hardware check before Stage B**: `nvidia-smi` + Ollama install test. Determines whether the local-RAG plan is "weekend project" or "occasional-use research tool" (CPU-only path is ~30s/query).

## Linked artifacts

- Forward plan: [ROADMAP.md](ROADMAP.md)
- README: [README.md](README.md) — full layout + commands
- Project registry: [projects.json](projects.json) — Tier 1 / Tier 2 declarations
- Portfolio entry: `_portfolio/projects/literature_pipeline.md`
- DuckDB index: `~/Projects/_references/portfolio.duckdb`
- EndNote citation library: `~/Projects/_references/citations/`

## Change log

- 2026-05-13 (afternoon) — final-polish pass. LICENSE stripped of vendor addendum so GitHub detects MIT (vendor note already lives in `vendor/VENDORED.md`). ROADMAP.md stripped of session-only artifact references (`_archived/research_2026_05_04/`, "conversation transcript") — the DuckDB-VSS code skeleton is now inline instead of behind a dead link. Compatibility table added to README; comparison table got maturity scores. `requirements.txt` now has version ceilings (`<3`, `<2`); `requirements.lock.txt` ships exact versions tested in CI. Added 25 integration tests covering RIS emission (15), CSV schema + path-traversal guard (6), and the load-bearing `paper_metadata` UPSERT-preserves-abstract contract (4). 83/83 tests green.
- 2026-05-13 — usability pass for strangers (v0.1.1). Fresh-eyes test + red-team audit drove a punch list of seven items:
  1. `jats_to_text.py` and `pdf_text_clean.py` now expose proper `--help` (previously crashed because their `__main__` was a dev smoke test that consumed `--help` as a positional arg). Smoke-test paths preserved behind explicit flags.
  2. `LITPIPE_EMAIL` not-set emits a one-shot stderr warning at `sweep.py` / `snowball.py` startup — closes the silent-traffic-to-maintainer footgun.
  3. `projects.json` migrated to `projects.json.template` + `.gitignore`'d. Missing-config now prints a friendly "copy the template" message instead of a `FileNotFoundError`. Centralized loader (`ris_emit.load_projects_config`) used across all 7 config-consuming scripts.
  4. README §Quickstart added — clone → install → set email → register project → write queue → fetch first PDF in <2 min.
  5. README personal paths stripped (`/c/Users/jab18015/...`, `~/.claude/skills/...`, hardcoded email reference). All commands now use generic placeholders.
  6. `extract_references.py` line removed from §Layout (file isn't in the repo).
  7. CI `--help` smoke test no longer excludes `jats_to_text.py` and `pdf_text_clean.py` (they now exit 0 properly).
- 2026-05-12 (afternoon) — deep-research stress-test pass. Five HIGH-impact fixes:
  1. `subprocess.run(..., text=True)` in `sweep.py` (4 sites) and `snowball.py` (1 site) now passes `encoding="utf-8", errors="replace"` — closes the latent cp1252 UnicodeDecodeError on accented subprocess output.
  2. **Writer/auditor filename drift closed.** `unpaywall_fetch_v2.last_name()` and `slug_title()` now NFKD-normalize via `ris_emit.safe_ascii` *before* tokenization, so files are written with ASCII names (`2024_Muller_*.pdf` not `2024_Müller_*.pdf`). `audit_filenames.slug()` was also doing it backwards (ASCII-strip then NFKD); now NFKD first. Verified writer ≡ auditor across `Müller / Périard / Mølmen / García-López` inputs.
  3. **Cross-module import bug fixed.** All 22 scripts re-wrapped `sys.stdout` at import time, which crashed when one script imported another (`ValueError: I/O operation on closed file`). Replaced with idempotent `sys.stdout.reconfigure(encoding="utf-8", errors="replace")` gated on `encoding != utf-8`.
  4. `audit_filenames.py --queue-history` flag landed (closes the un-OCR'd-scan loose end from the 2026-05-05 entry).
  5. `sweep.py` now resolves `<project>/<destination>` and rejects paths that escape the project root — defends against path traversal in user-authored or distributed `lit_pull_queue.csv`.
- 2026-05-12 (morning) — polish + `git init` pass. Email + Tesseract path env-overridable (`LITPIPE_EMAIL`, `TESSDATA_PREFIX`); hardcoded conda Python in `sweep.py` replaced with `sys.executable`; file-handle and DuckDB-connection leaks closed across ~15 scripts; bare exceptions narrowed. LICENSE, requirements.txt, .gitignore added. No behavior changes to fetch logic, UPSERT semantics, or MathML→LaTeX post-processing.
- 2026-05-05 — abstract enrichment completed (9,834/17,617 = 56%); recommendations enrichment running; CURRENT_STATE introduced per SOP.
- 2026-05-04 — major build session: schema v2, citation walker promotion, snowball orchestrator, EndNote harvest, filename+Unicode bug fixes, ROADMAP.md drafted, four sub-agent reports compiled into the plan.
- 2026-04-28 — pipeline elevated from `getpaid/tools/` to portfolio level after wheel-reinvention audit.
