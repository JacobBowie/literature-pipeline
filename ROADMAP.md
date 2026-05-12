# Literature Pipeline — Roadmap

**Last updated**: 2026-05-06
**Status**: pipeline core stable; bridge script + skill + SOP added 2026-05-06; RAG + MCP layers planned, not built; Stage 0 (external topic search) added as new pre-Stage-A candidate 2026-05-06.

This roadmap is the persistent plan that survives across Claude Code sessions. The pipeline as-of 2026-05-04 (puller, citation-walker, DuckDB index) is described in [README.md](README.md). This file captures what's *planned* on top of that.

---

## Current state (what's built)

| Layer | Tool | Status |
|---|---|---|
| Fetch (3-stage) | `sweep.py` → `unpaywall_fetch_v2.py` → `pmc_fetch.py` → `preprint_fetch.py` | **stable** |
| Citation walking | `forward_citations.py` (S2), `reverse_citations.py` (PDF parse) | **stable** |
| Library hygiene | `audit_filenames.py`, `audit_portfolio.py`, `backfill_ris.py`, `backfill_fulltext.py` | **stable** |
| Index | `index_portfolio.py` → `_references/portfolio.duckdb` | **stable** |
| Project registry | `projects.json` (Tier 1 / Tier 2 layouts) | **stable** |
| Citation harvest from EndNote downloads | `harvest_citations.py` | **stable** |

Portfolio totals (2026-05-04): 368 papers indexed, 17,527 unique candidate DOIs, 21,104 citation edges.

---

## Build plan: 0 → A → B → C → D

Each stage produces standalone value. None require finishing later stages to be useful.

### Stage 0 — External topic-search wrapper (open-world front-end) 🆕 added 2026-05-06

**Goal**: enable topic-led queries ("find evidence for X across the broader literature") without requiring the topic to be in the existing seed neighborhood. Today the pipeline is closed-world snowball — it walks out from existing seeds. Stage 0 adds the front-end discovery from external indexes.

**Approach** (~150 LOC across two scripts):
- `pubmed_search.py` — wraps NCBI E-utilities (`esearch` + `esummary`); accepts a query string + filters (year, journal, MeSH); emits a draft queue with DOIs, titles, authors, year. Free + no auth needed.
- `s2_topic_search.py` — wraps Semantic Scholar `/graph/v1/paper/search` (broader than PubMed; includes preprints + non-biomed). Free with a courteous mailto.
- Both emit `<project>/lit_pull_queue.draft.csv` in the same format as `seed_queue_from_top_candidates.py` — drops cleanly into the existing triage → sweep → ingest workflow.

**Why Stage 0 (and not just Stage A)**: the open-world variant in [`_portfolio/sop/literature_session.md`](../../_portfolio/sop/literature_session.md) currently relies on WebSearch + manual DOI extraction for step 1. Stage 0 automates that step end-to-end. Together with Stage B (paper-qa2 RAG), this closes the full agentic-research loop the user described 2026-05-06: "use WebSearch OR the tool to find papers, citation-walk, pull, ingest, return evidence."

**Why before Stage A** (revised order): topic search produces immediate user-facing value with no GPU dependency. Stage A (embedding ranking) refines what's already there; Stage 0 adds a capability that doesn't exist.

**Skills showcase**: NCBI E-utils API integration, courteous-API patterns, end-to-end workflow design.

**Implementation gotchas**:
- E-utils rate limits: 3 req/sec without API key, 10 req/sec with one. Add throttle.
- S2 search returns soft 429s under load; respect `Retry-After`.
- DOIs aren't in every PubMed result — must `esummary` for ArticleId, filter where `IdType=doi`.

**Trigger to build**: when Jacob says "find papers about X" and the resulting top_candidates probe returns empty/off-topic 2+ times in a row, this is the unblocker.

---

### Stage A — Embedding-similarity ranking layer ⭐ build first

**Goal**: rank the 17k candidate DOIs by relevance to a project's seed papers, without LLM tokens.

**Approach**:
- DuckDB `vss` extension + SPECTER2 embeddings (`allenai/specter2_base` proximity adapter)
- 768-dim cosine; HNSW index (M=16, ef_construction=128, ef_search=64)
- Stored in `papers.embedding` and `candidates.embedding` as `FLOAT[768]`
- Relevance score = cosine sim to seed-set centroid

**Why first**:
- Decoupled from any LLM/Ollama/paper-qa decision
- Re-uses ~50% of eventual RAG infrastructure (vector store + embedding pipeline)
- Immediate puller improvement (relevance-ranked candidates beats unranked)
- ~150-200 LOC; one weekend session

**Why SPECTER2 (not BGE/nomic/MiniLM)**:
- Trained on 6M citation triplets across 23 fields including medicine + physics
- Beats general-purpose models on SciDocs / SciRepEval benchmarks (Singh et al., EMNLP 2023, [arXiv:2211.13308](https://arxiv.org/abs/2211.13308))
- Allen AI explicitly notes naive fine-tunes on citation-pair objectives perform *worse* than SPECTER2 base — saved us a futile Tier B detour.

**Skills showcase**: DuckDB vector extension, sentence-transformers, scientific embeddings.

**Code skeleton**: see sub-agent report at `_archived/research_2026_05_04/duckdb_vss_embeddings.md` (or in conversation transcript) — ~30 lines.

**Implementation gotchas**:
- DuckDB vss persistent indexes need `SET hnsw_enable_experimental_persistence = true;`
- WAL recovery isn't implemented for HNSW yet — risk of index loss on crash. Mitigation: nightly rebuild as insurance.
- Vectors must be `FLOAT[N]` (32-bit only); no `DOUBLE` or `FLOAT16`.

---

### Stage B — Local RAG (1-weekend spike, two paths in parallel)

**Goal**: question-answering over the indexed corpus. "Find evidence for plasma volume + heat acclimation" → ranked passages with citations.

**Stack (consensus across both research agents)**:
- **Runtime**: Ollama (always-on daemon, paper-qa-blessed, free concurrency)
- **LLM**: Qwen 2.5 7B Instruct Q4_K_M (NOT Llama 3.2 — has tool-calling failure paper-qa #1128)
- **CPU fallback**: Llama 3.2 3B for non-tool synthesis when 7B is too slow
- **Embeddings**: nomic-embed-text via Ollama, OR sentence-transformers SPECTER2 (already loaded for Stage A)

**Path 1 — paper-qa2 (try first)**
- Pin `paper-qa==v2026.02.27` (avoid v2026.03.18 indexing regression #1321)
- **Critical config**: `api_type: "ollama"` on EVERY litellm config block — most common silent failure (silently falls back to OpenAI)
- Test on 10 PDFs first, scale to 100, then 500
- Wrap behind thin abstraction: `(question, paper_dir) → {answer, citations}`

**Path 2 — DIY RAG (insurance, ~300 LOC)**
- pypdf → chunk → SPECTER2/nomic embed → DuckDB vss top-K → Ollama generate
- Single file; no LiteLLM dependency hell; no surprise OpenAI fallback
- Build *in parallel* as a 1-day spike before committing to paper-qa2
- Decision rule: if paper-qa2 breaks twice in a week of use, switch to DIY

**Hardware reality**:
- CPU only (16GB RAM): ~5-12 tok/s → 30s per 200-word answer (slow but usable)
- RTX 3060/4060 8GB: ~80-100 tok/s → 3-5s per answer (comfortable)
- iGPU: ~10-20 tok/s (marginal over CPU)
- **TO CONFIRM before this stage**: run `nvidia-smi` to check GPU availability

**Skills showcase**: local LLM serving, RAG architecture, retrieval engineering, Ollama production patterns.

---

### Stage C — MCP server wrap

**Goal**: expose the pipeline as an MCP (Model Context Protocol) server so any Claude Code session in any project can call `lit_search`, `lit_candidates`, `lit_queue_for_fetch`, etc. as native tools.

**Verdict from MCP security agent**: GO. Single-user, single-machine, stdio = lowest-risk MCP profile. Matches every official server pattern.

**Six mandatory security mitigations** (before first run):
1. `shell=False` everywhere subprocess fires (no shell=True ever)
2. Regex-validate every DOI/URL arg: `^10\.\d{4,9}/[\w.\-/:]+$`
3. Hostname allowlist for outbound: Unpaywall, S2, NCBI, CrossRef only — no others
4. Claude Code `settings.json` permission split:
   ```json
   "allow": ["mcp__litpipe__search", "mcp__litpipe__list_*"],
   "ask":   ["mcp__litpipe__queue_*", "mcp__litpipe__run_sweep"],
   "deny":  ["mcp__litpipe__delete_*", "mcp__litpipe__shell_exec"]
   ```
5. Server-side `plan` + `confirm_token` pattern for mutating tools (defense in depth — survives misconfigured settings.json)
6. stderr-only logging (stdout = JSON-RPC framing; logging to stdout corrupts framing)

**Transport**: stdio. Always. Skip SSE/HTTP. (Streamable HTTP introduces session hijacking + CSRF + SSRF risk; stdio has zero auth complexity.)

**Packaging**: `uvx mcp-server-litpipe` (PyPI-distributed, semver-pinned). Layout per Anthropic's official servers: `src/mcp_server_litpipe/__init__.py` + `__main__.py`, console script in `pyproject.toml`.

**Threat model — top concerns** (from sub-agent report):
- **Indirect prompt injection via fetched paper text**: a malicious paper title/abstract could try to redirect Claude's tool use after being returned by `lit_search`. Mitigation: never re-feed raw fetched text into another tool call without explicit user review.
- **Command injection** via DOI args: regex validation closes this.
- **Path traversal** in queue writes: `Path.resolve().is_relative_to(project_root)` assertion.
- **Resource exhaustion**: cap queue size, per-call PDF size.
- **Credential leakage**: API keys via env vars only, never tool args (transcript leakage).

**Notable gotchas**:
- Tool descriptions are LLM-visible attack surface (tool poisoning attacks, Snyk/Elastic/Tenable docs)
- Schema drift between sessions = "rug pull" attacks ([Docker MCP horror stories](https://www.docker.com/blog/mcp-horror-stories-github-prompt-injection/))
- Windows + stdio quirks: `PYTHONIOENCODING=utf-8` required or fetch hangs; CRLF in JSON-RPC framing breaks some libs
- CVE-2025-6515 — session hijacking on HTTP transports (N/A for stdio)

**Skills showcase**: MCP server design (HOT skill 2026), security engineering, agent/tool design.

**Resume line**: "Built and open-sourced an MCP server (`mcp-server-litpipe`) exposing a literature-pipeline RAG system to Claude/MCP-compatible clients with documented security mitigations covering prompt-injection, command-injection, SSRF, and confirmation patterns."

---

### Stage D — Optional: ensemble relevance ranking

**Goal**: better candidate ranking by combining embedding similarity (topical relevance) with citation-graph PageRank (network importance).

**Approach**:
- Compute PageRank over the `cites` edge set (NetworkX or graph-tool)
- Combine: `final_score = α · cosine_similarity + (1-α) · pagerank_score`
- α tunable per project; calibrate against a small hand-labeled gold set

**When**: after A+B+C are stable and being used. This is a refinement, not a foundation.

**Skills showcase**: graph algorithms, ensemble methods.

---

## Hardware confirmation needed (blocker for Stage B)

Run `nvidia-smi` once and confirm:
- Discrete GPU? Which model? VRAM?
- If no dGPU: CPU-only RAG works at ~30s/query for Qwen 2.5 7B. Painful for interactive flow but feasible for occasional research use.
- iGPU only: marginal speedup over CPU.

This determines whether Stage B is "weekend project" (with GPU) or "occasional-use research tool" (CPU-only).

---

## Decoupling decisions (intentional)

- **Stage A is independent of all later stages** — build it whenever, even before deciding on RAG.
- **Stage B paths 1 and 2 are independent** — build the DIY scaffold in parallel as insurance against paper-qa2 breakage.
- **MCP wrap (C) waits until B is stable** — wrapping a flaky tool layer in MCP just makes the flakiness harder to debug.
- **Tier B fine-tuning is rejected** — SPECTER2 already trained on 6M citation triplets; our 21k is <0.5% of that and Allen AI's own data shows the fine-tune typically *hurts* performance.

---

## Skills mapping (target roles from `Git-R-Dun/files/skill_gap_crossref.md`)

| Stage | Skill | Roles unlocked |
|---|---|---|
| A: SPECTER2 + DuckDB vss | vector DB, scientific embeddings | GenAI/LLMs (4 roles) |
| B: Ollama + RAG | local LLM serving, retrieval engineering | GenAI/LLMs (4) |
| C: MCP server | agent design, security engineering | GenAI/LLMs (4), generic SWE |
| D: PageRank ensemble | graph algorithms | nice-to-have |

Cross-cutting: Python production patterns, DuckDB consistency with ATHENA HR pipeline.

**Resume narrative target**: *"Built a domain-specialized RAG system over a 17k-paper heat-physiology citation graph (forward+reverse extraction via Semantic Scholar), exposed as an MCP server with documented security mitigations. Fully local stack: Ollama + Qwen 2.5 7B + DuckDB vss + SPECTER2 embeddings. Open-sourced as `mcp-server-litpipe`."*

---

## Quick wins available *before* committing to A-D

These don't require RAG/MCP planning — they improve the existing pipeline today:

1. **CrossRef abstract enrichment for candidates**: call `/works/{doi}` for the 17k candidates, store `abstract` column. Without abstracts, embeddings (Stage A) are titles-only — usable but weaker. ~2-3 hrs of polite-rate API time.
2. **S2 `/paper/{id}/recommendations` enrichment**: free signal. Returns semantically-similar papers per S2's internal embeddings. Adds `s2_similarity_score` column. Bonus signal for candidate ranking.
3. **Cross-project paper schema fix**: current `papers.doi` is PRIMARY KEY which collapses cross-project duplicates. Should be either composite PK `(doi, project)` or split into `papers` (canonical metadata) + `paper_locations` (many-to-many). Surfaced during DuckDB port.
4. **Snowball loop wrapper**: a script that runs (citation walk → discover → queue → sweep → re-index) until convergence. Currently each step is manual.

---

## Open decisions deferred

- **Hardware story** (GPU? Block on `nvidia-smi`)
- **paper-qa2 vs DIY commitment** (decide after Stage B spike)
- **Distribution model for the MCP server** (private / public-on-PyPI / public-on-GitHub-only)
- **Per-project relevance terms config**: do we want this in `projects.json` as a fallback for when the project agent hasn't yet expressed a research goal? Or fully agent-driven, no config?

---

## Source material (sub-agent reports, 2026-05-04)

The four sub-agent reports that informed this plan are in the conversation transcript for the 2026-05-04 session. Key citations:

- **MCP**: [MCP Security Best Practices spec](https://modelcontextprotocol.io/specification/2025-06-18/basic/security_best_practices), [Claude Code settings.json](https://code.claude.com/docs/en/settings), [Anthropic's official servers](https://github.com/modelcontextprotocol/servers), [Simon Willison on MCP prompt injection](https://simonwillison.net/2025/Apr/9/mcp-prompt-injection/), [Snyk Labs on prompt injection in MCP](https://labs.snyk.io/resources/prompt-injection-mcp/)
- **paper-qa2**: [Future-House/paper-qa](https://github.com/Future-House/paper-qa), pinned-version recommendation v2026.02.27, issue #1321 (open regression), #1128 (closed Llama 3.2 tool-call), #1237 (closed router error)
- **DuckDB vss**: [VSS docs](https://duckdb.org/docs/current/core_extensions/vss.html), [What's new Oct 2024](https://duckdb.org/2024/10/23/whats-new-in-the-vss-extension)
- **Embeddings**: [SPECTER2 paper, EMNLP 2023, arXiv:2211.13308](https://arxiv.org/abs/2211.13308), [Allen AI SPECTER2 blog](https://allenai.org/blog/specter2-adapting-scientific-document-embeddings-to-multiple-fields-and-task-formats-c95686c06567), [MTEB leaderboard](https://huggingface.co/spaces/mteb/leaderboard)
- **Local LLM**: [Ollama OpenAI compat docs](https://docs.ollama.com/api/openai-compatibility), [DatabaseMart Ollama benchmarks](https://www.databasemart.com/blog/ollama-gpu-benchmark-rtx4060), [paper-qa README](https://github.com/future-house/paper-qa)

If/when this roadmap is executed, copy-paste the relevant sub-agent reports into `_archived/research_2026_05_04/` for permanent reference.
