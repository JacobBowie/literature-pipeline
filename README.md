# literature-pipeline

A Python toolkit for literature acquisition across several research projects that share one
portfolio. You give it a curated list of DOIs per project; it fetches the open-access copies it is
allowed to fetch (Unpaywall, PubMed Central, preprint servers), names and files them in each
project's library with a text sidecar and a `.ris` record, routes everything it could not fetch to
worklists a person acts on, walks the citation graph around each library, and keeps one DuckDB
index of what the portfolio holds and what it might want next. A scheduled runner does the
routine parts unattended.

It never bypasses a paywall: no Sci-Hub, no spoofed sessions, no scraping of pages a provider
prohibits. Every request identifies itself with your contact address, is paced per host, and is
logged.

Contents: [What it does](#what-it-does) | [Install](#install) | [Environment](#environment) |
[Quickstart](#quickstart) | [The registry](#the-registry-projectsjson) | [Sweep](#sweep-the-fetch-run) |
[Library files](#what-lands-in-a-library) | [Walks and the index](#walks-and-the-index) |
[The runner](#the-runner) | [Worklists, pools and the gate](#worklists-pools-and-the-gate) |
[Manual acquisition](#manual-acquisition-tools) | [Text, OCR, tables, figures](#text-ocr-tables-and-figures) |
[Health checks](#health-checks-and-audits) | [Caveats](#caveats) | [Platforms and scope](#platforms-and-scope) |
[Layout](#layout) | [Related tools](#related-tools)

## What it does

The mental model, in the order the data moves:

1. **Seed library.** Each registered project has a library folder: PDFs, `.fulltext.json` text
   sidecars and `.ris` records. What a library holds is what the project "has"; it seeds the walks.
2. **Walks.** The forward walk finds the papers that cite each seed (Semantic Scholar, or OpenAlex
   for very highly cited seeds); the backward walk finds what each seed cites (Semantic Scholar,
   OpenAlex, Crossref and a local parse of the reference section).
3. **Index.** `index_portfolio.py` loads every library and every walk into `portfolio.duckdb`:
   which DOIs are held where, the citation edges, and the candidates no library holds yet.
4. **Curated queue.** A person (or, for projects that opt in, the runner) drafts a queue from the
   index (`seed_queue_from_top_candidates.py`), reviews it, and saves it as the project's
   `lit_pull_queue.csv`. Only queued DOIs are ever fetched.
5. **Sweep.** `sweep.py` runs the fetch stages the project's `sources` allow, in order (Unpaywall,
   PMC, preprint servers), then text extraction. Fetched files land in the library.
6. **Route.** Every row that was not fetched gets a residual class, and `migrate_closed_to_md.py`
   routes it: a retry later, a browser worklist for blocked open-access links, an interlibrary-loan
   (ILL) list, or a review list for files that failed the identity check.
7. **Worklists.** People work those lists by hand (a browser click, an ILL request, a review), and
   the manual tools file what they bring back.

## Install

Requirements: Python 3.11 to 3.13 and uv (the dependency manager; install it as its own
documentation describes for your platform). From the repository root:

```bash
uv sync
```

This creates a project-local `.venv/` from `uv.lock`. Run every command through uv:

```bash
uv run python sweep.py --help                          # from the repository root
uv run --project /path/to/checkout python /path/to/checkout/sweep.py --help   # from anywhere else
```

The scripts are a flat layout, not an installed package (`pyproject.toml` sets `package = false`):
run them by path, or `python -m litpipe.<module>` from the repository root. Runtime dependencies
are in `pyproject.toml` (requests, duckdb, pandas, pymupdf, pdfplumber, pytesseract, pillow). The
MathML-to-LaTeX converter is vendored under `vendor/`.

## Environment

Configuration lives in `projects.json` (next section). Environment variables are only for values
that identify you or are secret:

| Variable | Required | What it does |
|---|---|---|
| `LITPIPE_EMAIL` | yes | Your contact address. It goes in every API request's User-Agent (`mailto:`) and in the `email=` parameter Unpaywall and NCBI require. There is no default. |
| `S2_API_KEY` | no | A Semantic Scholar API key, sent only in the `x-api-key` header. |
| `OPENALEX_API_KEY` | no | An OpenAlex API key, sent only as `Authorization: Bearer`. |
| `TESSDATA_PREFIX` | no | Where Tesseract's language files live, for OCR (see [OCR](#text-ocr-tables-and-figures)). |

**`LITPIPE_EMAIL` unset, empty, malformed, or on a reserved placeholder domain** (`example.com`,
`example.org`, `example.net`, or the `.test`, `.example`, `.invalid` and `.localhost` top-level
domains): the pipeline sends no placeholder. The Unpaywall stage stops before its first request
with a CONFIG outcome, so `sweep.py` exits 2 and leaves the queue in place; the runner's preflight
fails and the run exits 3 (`run_daily.py` exits 1); the other tools send their requests with no
contact address at all (`sweep.py` and `snowball.py` print a warning first). Set it in your shell
profile:

```bash
export LITPIPE_EMAIL="your.name@your-institution.edu"      # Linux, macOS: add to ~/.bashrc or ~/.profile
```
```powershell
setx LITPIPE_EMAIL "your.name@your-institution.edu"        # Windows PowerShell; then open a new shell
```

**`S2_API_KEY`.** Without a key, Semantic Scholar requests share the public pool, spaced 6.5 s
apart, and snowball and the runner run the backward walk without its Semantic Scholar leg (OpenAlex,
Crossref and the local parse only), since the shared pool throttles at once. With a key, requests
are spaced 1.1 s apart, the backward walk keeps the Semantic Scholar leg, and
`enrich_recommendations.py --recent-feed` runs (it sends nothing without a key unless you pass
`--allow-unkeyed`). Request a key on Semantic Scholar's API page with an
institutional address (keys are not issued to free-mail addresses). An idle key is pruned after
about 60 days, so the runner's monthly profile makes one keyed call.

**`OPENALEX_API_KEY`.** Without a key, OpenAlex calls still work within the keyless daily
allowance, but the forward walk cannot use OpenAlex for seeds with more than 9,999 citers (it falls
back to Semantic Scholar year windows, which can miss citers) and the backward walk skips its
OpenAlex leg. With a key the allowance is 10,000 credits a day (a single-work lookup costs 0, a
list call 1); a first keyed walk of a large library may need a higher
`openalex.max_requests_per_run` (see the registry).

## Quickstart

From a clean checkout to your first PDF. Commands are bash (Git Bash works on Windows); PowerShell
users set the email as shown above and create the folders and files with any editor.

```bash
uv sync
export LITPIPE_EMAIL="your.name@your-institution.edu"     # your real address

# 1. A registry with one project. projects.json is gitignored; the template documents every key.
cat > projects.json <<'EOF'
{
  "root": "~/litpipe-projects",
  "projects": {
    "my-review": {"tier": 2, "lib_dir": "literature", "active": true}
  }
}
EOF

# 2. The project folder, its library, and a one-row queue at the project ROOT.
#    Title, authors and year may be left blank: sweep fills them from Crossref or DataCite.
mkdir -p ~/litpipe-projects/my-review/literature
printf 'doi,title,authors,year,destination,notes\n10.1371/journal.pone.0012033,,,,literature/,quickstart\n' \
  > ~/litpipe-projects/my-review/lit_pull_queue.csv

# 3. Dry run: finds the queue, prints the run id and the resolved destination, writes nothing.
uv run python sweep.py --project my-review --dry-run

# 4. The real run, routing the residuals afterwards.
uv run python sweep.py --project my-review --migrate
```

The PDF lands in `~/litpipe-projects/my-review/literature/` as `<year>_<Lastname>_<TitleWords>.pdf`
with a `.fulltext.json` sidecar and a `.ris` beside it. The queue is renamed to
`lit_pull_queue.<run_id>.processed.csv`, and the run's report and residual CSVs sit next to it in
the project root.

## The registry (`projects.json`)

Every tool reads `projects.json` beside the scripts. Copy `projects.json.template` to start: its
`_schema` block documents every key, and its values are a working two-project example. Only
registered, active projects are swept, indexed or run; a folder that is not in the registry is
invisible to every tool.

Top-level keys:

| Key | Default | What it does |
|---|---|---|
| `root` | `~/Projects` | The folder holding every project; project `my-review` is `<root>/my-review`. A relative value is anchored to your home directory. |
| `db_dir` | `<root>/_references` | Where `portfolio.duckdb` lives (also `harvest_citations.py`'s `citations/` and snowball's convergence log). A relative value is anchored to `<root>`. |
| `state_dir` | `~/.local/db/literature_pipeline` | The pipeline's own state: `litpipe_state.sqlite` (pacing, refusals, runs), `s2_cache.duckdb` (the walk cache), `ledger/` (one line per request attempt), `runner/` (run summaries and stage logs). One per machine, on a local disk. |
| `artifact_dir` | unset (the project root) | Where sweep and the runner write each run's artifacts. Relative: inside each project; absolute: `<value>/<project key>`. |
| `loose_ends` | unset (nothing written) | A Markdown file that gets one status line per project per run (done, partial, or a line closing an earlier partial). Relative to `<root>`. |
| `portfolio_dir` | none | The folder for portfolio-level paywall queues (`build_priority_paywall_queue.py` writes there, `paywall_pull.py` reads there). Without it those tools need `--out-dir` or `--queue`, else they exit 1. |
| `ezproxy_host` | none | Use-case-only: your library's EZproxy host for `paywall_pull.py --access ezproxy` (which exits 1 without it or `--ezproxy-host`). |
| `hosts` | both `false` | `arxiv_pdf_allowed`, `biorxiv_pdf_allowed`: may arXiv PDFs, and bioRxiv or medRxiv PDFs, be fetched automatically. Off, such rows become a manual click. |
| `s2` | see template | Semantic Scholar client: `spacing_s` 6.5, `spacing_keyed_s` 1.1, `max_requests_per_run` 2000 (null: no cap), `breaker` 3. |
| `openalex` | see template | OpenAlex client: `max_requests_per_run` 2000, `breaker` 3, `content_max_per_run` 0 (paid cached PDFs). |
| `runner` | see template | `unattended_db_writes` false, `candidate_order` (repository or publisher), `schedule_time` "01:00", `timeouts` {job: seconds}, `lock_stale_s` 7200, `ris_limit` 200. See [The runner](#the-runner). |

Per-project keys (under `"projects": {"my-review": {...}}`):

| Key | Default | What it does |
|---|---|---|
| `lib_dir` | required | The library folder, relative to the project folder. |
| `tier` | 2 | 1: a systematic-review layout with a `data_dir` of build artifacts; 2: a library only. |
| `data_dir` | none | Tier 1 build artifacts (text dumps, tables, discovery files), relative to the project folder. |
| `active` | true | false keeps the entry but skips it everywhere. |
| `sources` | `["unpaywall", "pmc"]` | The fetch sources the project allows (see [stages](#the-stages-and-sources)). |
| `auto_stage` | false | May the runner draft and stage a queue from the index by itself. |
| `walk_cadence_days` | none | Walk forward citations unattended every N days (runner, daily profile and up). |
| `ris_threshold` | 90 | `pipeline_check.py`'s minimum `.ris` coverage, in percent. |
| `artifact_dir` | the global value | This project's artifact folder (absolute values are used as given). |
| `parent` | none | Subprojects only: the containing project's key. |

**Subprojects.** A key `parent/sub` with `"parent": "parent"` lives at `<root>/parent/sub`. Its
`lib_dir` is relative to the PARENT folder (`"sub/literature"`), but a queue's `destination` is
relative to the SUBPROJECT folder (`literature/`). Copying `lib_dir` into `destination` would file
PDFs in `<root>/parent/sub/sub/literature`; sweep refuses a destination that is not the registry
library (`--allow-destination` overrides), and `--dry-run` prints the resolved folder.

## Sweep (the fetch run)

### The queue

`sweep.py` looks in each active project's root (or the one `--project` names) for:

| File | What it is |
|---|---|
| `lit_pull_queue.csv` | the live queue |
| `lit_pull_queue.<tag>.csv` | a tagged queue (`<tag>`: lower-case letters, digits, `_` or `-`, at most 32 characters, not date-like); it needs `doi` and `destination` columns |
| `lit_pull_queue.retry_later.csv` | rows waiting for a retry date: those due on the run date are moved into `lit_pull_queue.retry.csv` and swept |

The queue's columns are `doi,title,authors,year,destination,notes`. `doi` and `destination` are
required; `destination` is the library folder relative to the project folder and must be the
registry library; title, authors and year name the file (`<year>_<Lastname>_<TitleWords>.pdf`) and
are filled from Crossref or DataCite when blank. Lines starting with `#` are ignored.
`lit_pull_queue.template.csv` is an example.

### The stages and `sources`

Each project's `sources` decide which stages run (DEC-31):

| Stage | Sources | What it does |
|---|---|---|
| Unpaywall | `unpaywall` | Asks Unpaywall (Crossref DOIs only) for open-access locations and downloads the PDF, repositories first by default. A row the library already holds is `SKIP_EXISTS`. |
| PMC | `pmc` | For rows Unpaywall did not deliver: finds the PMCID, then fetches the PDF and JATS full text only from the routes NCBI allows for automated retrieval (the PMC Cloud Service and E-utilities), or Europe PMC's full text for articles it marks open access. An author manuscript has no PDF there and becomes a text-only holding. |
| Preprint | `europepmc_preprints`, `biorxiv`, `medrxiv`, `osf`, `sportrxiv`, `arxiv` | Looks for a preprint copy on the servers the project names. A row with an arXiv DOI or ID reaches arXiv whatever the sources. |
| Extraction | always | Writes the `.fulltext.json` text sidecar for every new PDF. |

`openalex_content` allows paid cached PDFs from OpenAlex (`litpipe.openalex.content_pdf`, capped by
`openalex.content_max_per_run`); no sweep stage uses it. A stage whose source is not listed is
skipped and recorded as skipped, never as a failure. `--skip-preprint` skips the preprint stage for
one run, and `--sources LIST` (on `sweep.py`, `runner run` and `runner batch`) replaces the
project's list for one run. A source list that leaves no fetch stage refuses the queue before
anything is fetched.

### Artifacts

Each run of a project gets a run id: the date (`YYYY-MM-DD`), or `YYYY-MM-DD.N` for a later run of
the same day, so nothing is overwritten. Every artifact carries it:

```
lit_pull_queue[.<tag>].<run_id>.<stage>.csv
    stage: normalized, unpaywall, pmc, preprint, residual, report, processed
lit_pull_queue[.<tag>].<run_id>.routing.csv        (written by the route step)
```

They go to the project root unless `artifact_dir` (or `--artifact-dir`) says otherwise. Leave
`artifact_dir` unset when another tool of yours reads sweep's artifacts from the project root.
The queue retires (is renamed to its `.processed.csv`) when every fetch stage completed and every
row has a class; a failed stage keeps the queue for the next sweep. Each project's run id is
printed as `[sweep] run_id=<id> project=<key>`.

### Residual classes and routing

Every row that was not fetched gets one class in the residual CSV, and the route step
(`--migrate`, or `migrate_closed_to_md.py --project KEY` afterwards) acts on it:

| Class | Meaning | Where it goes |
|---|---|---|
| `HELD_ELSEWHERE` | another registered library already holds the DOI as a PDF | nothing queued; the path is reported |
| `TEXT_ONLY` | full text was saved, no PDF exists on the allowed routes | nothing queued (a text-only holding) |
| `TRANSIENT` | an embargo, a host refused or deferred for the run, an outage, a transport error | `lit_pull_queue.retry_later.csv` with a `not_before` date (1 day; an embargo's release date) |
| `OA_BLOCKED` | an open-access copy exists but its host refused a script, or a preprint needs a click | `lit_pull_queue.oa_blocked.md` (a browser worklist linking the preprint's page or the open-access URL Unpaywall reported) and a retry in 3 days |
| `IDENTITY_FLAG` | the served file failed the identity check (another paper, or a supplement) | `lit_pull_queue.review.md` with the file's path and evidence; the file stays where it is |
| `TERMINAL_CLOSED` | every enabled stage said there is no open copy (or an error repeated three runs) | the ILL list `lit_pull_queue.md` |
| `NO_METADATA` | no title or authors could be found | nothing queued; listed in the routing report |
| `SKIPPED_SOURCE` | no enabled stage covers the row | nothing |
| `INVALID_DOI` | a malformed or placeholder DOI | reported only |
| `PENDING` | a stage left no verdict for the row | nothing; the queue stays for a re-sweep |
| `CONFIG` | a source refused the configuration (for example a bad email) | the run aborts, nothing is written |

A row whose only open source is a host refused until someone clears it (arXiv, see
[Health checks](#health-checks-and-audits)) waits 30 days instead of 1 or 3. The worklists dedupe
by DOI in any form, and every persisted error string is redacted (the contact address never
reaches a file).

### Exit codes

| Code | sweep.py |
|---|---|
| 0 | every stage of every queue completed (also when nothing was fetched or nothing was staged) |
| 1 | `--project` named a project with nothing to sweep |
| 2 | usage or configuration error (bad arguments, an invalid `sources` list, a CONFIG outcome) |
| 3 | a stage failed or crashed; the queue stays for the next sweep |
| 4 | a queue was refused before fetching (a bad destination, an unusable source list, or another process holds the project's lock) |

### The project lock

While sweep, the runner, or a commit of the route step's CSV import works on a project, it holds
`lit_pull_queue.lock` in the project root (host, process id, run id, heartbeat). A second sweep of
the same project skips it and names the holder. A lock whose heartbeat is older than
`runner.lock_stale_s` (default 2 hours), or whose process on this host is gone, is stale and is
taken over. On a synced folder another machine may see the lock late; see the day and night split
under [The runner](#the-runner).

## What lands in a library

| File | What it is |
|---|---|
| `<stem>.pdf` | the paper, `<year>_<Lastname>_<TitleWords>.pdf` |
| `<stem>.fulltext.json` | the text sidecar: extracted text (or JATS full text with tables, formulas and figure captions), metadata, the DOI. A PDF whose text layer fails the validity gate gets `needs_ocr: true` and empty text. |
| `<stem>.ris` | a bibliographic record from Crossref or DataCite metadata. A `.ris` edited by a person is never overwritten (the pipeline records a hash of every `.ris` it writes). |
| `<stem>.identity.json` | the identity check's verdict for a downloaded PDF. `FLAG` (the file is another paper) or a supplement means the file is a review item, never a holding: it is not indexed under the queued DOI and gets no `.ris`. The PMC stage writes the same verdict into the sidecar's `identity` fields. |
| `<stem>.fig<N>.<ext>` | figure images (`fetch_figures.py`, PMC only), each tagged with its reuse licence |

**Text-only holdings.** A `.fulltext.json` with text and no PDF beside it (an author manuscript
from PMC, a preprint's full text from Europe PMC) is a full holding: it counts as held, seeds the
walks, is indexed with `has_pdf = false`, and is never pruned as an orphan. Its `.ris` comes from
the DOI's registration agency (Crossref, DataCite), as a PDF's does, with the sidecar's own fields
as the fallback when no agency holds the DOI (`backfill_ris.py --include-text-only`, and the
runner's daily `ris` job).

## Walks and the index

```bash
uv run python forward_citations.py --project my-review     # who cites the library
uv run python reverse_citations.py --project my-review     # what the library cites
uv run python index_portfolio.py                           # load every library and walk into the DB
uv run python snowball.py --project my-review              # forward + backward + index (+ abstracts)
```

* **Forward walk** (`forward_citations.py`): one metadata pass, then only the seeds that are new,
  failed before, or whose citation count changed since the cached walk (`--refresh` walks all).
  The cache is `<state_dir>/s2_cache.duckdb`. A run that loses seeds to failures does not replace the published
  `_forward_citations.csv`; it writes `<report>.degraded.csv` and exits 2 (3 on a spent budget or a
  tripped breaker). A killed run resumes from `<report>.partial.jsonl`. Scoped mode
  (`--seeds-from LIST --scope NAME`) walks a DOI list, such as one chapter's references.
* **Backward walk** (`reverse_citations.py`): Semantic Scholar, OpenAlex, Crossref and the local
  parse (`--sources` picks the legs) into `_reverse_citations_parsed.csv`.
* **Snowball** (`snowball.py`): one iteration per project of forward walk, backward walk and index,
  then an incremental abstracts pass. `--until-convergence --max-iter N` repeats while the library
  changed and candidates grew by 1 % or more; a degraded walk is never read as convergence. Every
  iteration appends a row to `<db_dir>/convergence_log.csv`. Exit 0 ok, 2 a project degraded, 1 a
  step failed. Snowball never fetches anything.
* **Index** (`index_portfolio.py`): `<db_dir>/portfolio.duckdb`. Tables: `paper_metadata`,
  `paper_locations` (who holds what, `has_pdf` false for text-only), `papers_no_doi`, `candidates`,
  `cites`, `recommendations`, `scoped_candidates`, `scoped_cites`, `index_runs`; the enrichment
  tools add `abstract_attempts`, `recent_feed`, `rec_attempts` and `s2_enrichment`. Views: `papers`,
  `top_candidates`, `cross_project_papers`, `project_cocitations`. `--rebuild` recreates the
  schema and keeps the old file as `<db>.bak`; `--gc` removes rows nothing references.
* **Enrichment**: `enrich_abstracts.py` (Crossref abstracts), `enrich_recommendations.py
  --recent-feed` (Semantic Scholar's recent recommendations), `python -m litpipe.enrich_s2`
  (abstracts, open-access URLs and citation counts; dry run unless `--commit`). All three write
  `portfolio.duckdb`.

**Drafting a queue from the index.**

```bash
uv run python seed_queue_from_top_candidates.py --project my-review
```

writes `<project>/lit_pull_queue.draft.csv` (never swept: the name does not match). `--rank
project` (default) ranks by the project's own seeds, `portfolio` by every project's, `cocitation`
by co-citation; `--scope NAME` draws from a scoped walk; `--pmc-check` adds each DOI's PMC status.
Review the draft, then save it as `lit_pull_queue.csv`.

## The runner

`python -m litpipe.runner` runs the routine work unattended: one registered run at a time, each
(project, stage) in its own child process with a timeout, a per-run summary at
`<state_dir>/runner/<run_id>/summary.json` and per-stage logs beside it.

```bash
uv run python -m litpipe.runner run --profile daily --dry-run     # list every job and skip reason
uv run python -m litpipe.runner run --profile daily               # one run
uv run python -m litpipe.runner status                            # live runs, last run per project, refused hosts
uv run python -m litpipe.runner schedule-print                    # the nightly task, as text to install
```

**Profiles** are cumulative. `every_run`: preflight, network checks, per project an auto-staged
draft (only `auto_stage` projects), a sweep when something is staged, the route step.
`daily` adds the forward walk for projects whose `walk_cadence_days` is due and a `.ris` for
text-only holdings (at most `runner.ris_limit` per project). `weekly` adds the backward walk for
the projects with a `walk_cadence_days`, `enrich_abstracts` and a read-only `audit_portfolio`;
`monthly` adds `enrich_recommendations --recent-feed` and the keyed Semantic Scholar call. The index is refreshed for each project whose
library or walks changed. `run_daily.py` is a thin wrapper over `run --profile daily`.

**The one nightly task.** `schedule-print` prints the task for this platform at
`runner.schedule_time` (default 01:00): a systemd user service and timer (plus a crontab line) on
Linux, a `Register-ScheduledTask` command (plus a `schtasks` line) on Windows, and a crontab line
on macOS (`--platform linux|windows|macos`). It runs `run --profile daily --scheduled`, which
escalates to the weekly profile after 6.5 days and the monthly after 27. It prints only; you
register the task yourself and set the environment variables it names.

**Unattended DB writes** are off by default (`runner.unattended_db_writes`). With them off, the
runner skips the index and enrichment jobs and lists each with the command to run by hand, and a
stale index is reported, not failed. Turn them on where the runner and `portfolio.duckdb` share a
local disk and no person writes the DB during the run window (`--db-writes` / `--no-db-writes`
override for one run).

**Hand-run DB writers.** `index_portfolio.py`, `snowball.py`, `enrich_abstracts.py`,
`enrich_recommendations.py` and `python -m litpipe.enrich_s2 --commit` open `portfolio.duckdb` for
writing and do not register with the runner: nothing stops one from running beside a run. DuckDB
allows one writer, so the second fails to open the file. Run them on the machine that holds the
DB, outside the runner's window, after `runner status` shows no live run.

**The day and night split.** The intended setup is one machine (often a server) that runs the
nightly task and holds the DB, and interactive sweeps by day, from any machine that sees the
project folders. The project lock keeps a day sweep and a night run off the same project; because
a synced folder can show a lock late, keep interactive sweeps out of the night window.

**`runner batch`** draws a pool down: `runner batch --project KEY --pool CSV [--size 100]
[--batches N] [--stage-only]`. The pool CSV lists `doi` (plus optional title, authors, year,
notes) in rank order. Each batch is written as `lit_pull_queue.<tag>.csv`, swept and routed, and
every DOI is marked with its residual class in `<pool stem>.drawdown.json` beside the pool; a
killed batch resumes. `--stage-only` stops after writing the batch so a person can remove rows; the next
`batch` marks the removed rows `curated_out` and sweeps the rest.

| Code | runner |
|---|---|
| 0 | every job OK or deliberately skipped, health checks pass |
| 1 | usage or configuration error (nothing ran) |
| 2 | completed with failures (a job degraded, failed, timed out or was deferred; a health alarm) |
| 3 | aborted (preflight failed, another runner was live, the heartbeat was lost, interrupted) |

Every step the runner and snowball call follows one convention: 0 clean, 1 usage or config,
2 degraded, 3 aborted (budget or breaker); before a 2 or 3 the last stdout line is
`[step-summary] {json}` with `reasons`, `aborted` and `transport_failures`.

## Worklists, pools and the gate

```bash
uv run python -m litpipe.worklists oa-blocked --write oa_blocked.md   # blocked OA links, grouped by host
uv run python -m litpipe.worklists ill --write ill.md                 # the ILL list, ranked by co-citation
uv run python -m litpipe.worklists coverage                           # share of each library's PDFs parsed as seeds
uv run python -m litpipe.worklists pool-status --pool pool.csv        # a pool's drawdown state
```

The per-project files (`lit_pull_queue.md`, `.oa_blocked.md`, `.review.md`,
`.retry_later.csv`) are written by the route step; these commands build portfolio-wide views and
write only where `--write` points. DOIs held anywhere are marked done (`oa-blocked`) or left out
(`ill`).

`migrate_closed_to_md.py --import-csv PATH [--project-map TAIL=KEY] [--review-as retry|list-only]
[--commit]` imports a DOI-keyed CSV (columns `project`, `doi`, `suggested_worklist`) into the
worklists, dry by default, skipping DOIs already listed or held.

### The gate

`python -m litpipe.gate` turns a scoped forward harvest into a ranked selection, driven by a JSON
spec you own (topic matchers, quotas, lanes and controls):

```bash
uv run python -m litpipe.gate build  --project KEY --spec spec.json            # dry: the funnel and the plan
uv run python -m litpipe.gate plan   --project KEY --spec spec.json --pool pool.csv
uv run python -m litpipe.gate report --project KEY --spec spec.json
uv run python -m litpipe.gate build  --project KEY --spec spec.json --write    # writes a run folder
uv run python -m litpipe.gate promote --project KEY --spec spec.json --run RUN_ID
```

`build`, `plan` and `report` also take `--lib-dir DIR`, `--holdings-as-of FILE`, `--out-root DIR`
and `--json PATH`. The command is dry by default; with `--write` a run writes the immutable folder
`<lib>/_gate/<name>/<run_id>/`, and `promote` (refusing while the pool has batches pending, unless
`--force`) copies its selection to `<lib>/_<name>_gate_selection.csv`, which `runner batch --pool`
draws from. Exit 0 done, 1 a usage or spec error, 2 a failed control, a sanity abort or zero
inputs. The schema is documented field by field in the `litpipe/gate.py` module docstring;
`examples/gate_spec.example.json` is a worked example.

## Manual acquisition tools

These file what a person brings back. The ones that change a library are dry runs until you pass
their write flag (`--execute`, `--apply`, `--commit`).

* `import_downloads.py --project KEY [--execute]`: identifies PDFs you downloaded (default
  folder: `~/Downloads`, files modified since local midnight; `--downloads`, `--cutoff`). It refuses
  a file that is not a PDF (`NOT_PDF`), checks each PDF is the paper it claims, skips copies another
  library already holds (`HELD_ELSEWHERE`; `--import-held-elsewhere` imports them for one run),
  and with `--execute` strips interlibrary-loan cover pages from the filed copy (the original is
  kept under `<lib>/_archive/originals/`; `--keep-covers` turns it off), writes the sidecar and
  `.ris` first, then files the PDF under its canonical name (a failed write is `ERR_WRITE` and the
  PDF stays where it was). The report is `_downloads_import_<date>[.N][_DRYRUN].csv`.
* Use-case-only, for users with institutional library access: `build_priority_paywall_queue.py
  --date YYYY-MM-DD` builds a citation-ranked queue of paywalled DOIs in `portfolio_dir`, and
  `paywall_pull.py --open N` / `--finish [--apply]` runs a browser session over it;
  `--access ezproxy` routes the links through `ezproxy_host`.
* `harvest_citations.py [--commit]`: consolidates `.ris`, `.enw` and `.nbib` exports from a folder
  (default `~/Downloads`) into canonical `.ris` files under `<db_dir>/citations`.
* `audit_filenames.py --lib-dir DIR [--execute]`: renames files to the canonical form (PDF,
  sidecar, `.ris`, figures together). `fill_missing_dois.py --project KEY [--execute]`: recovers
  missing DOIs (front matter, then a strict Crossref match). `backfill_ris.py --project KEY
  [--commit] [--include-text-only]`: writes missing `.ris` records.

## Text, OCR, tables and figures

* `extract_pdf_fulltext.py --lib-dir DIR` writes `.fulltext.json` sidecars (sweep runs it after the
  fetch stages). A text layer that fails the validity gate (garbled, image-only pages) gets
  `needs_ocr: true` and empty text instead of noise. `--ocr` runs Tesseract on those PDFs (about 2
  to 4 s a page; manual only). `--refresh` re-extracts sidecars the pipeline wrote and keeps merged
  or hand-repaired ones (`--force` replaces those too); `--suspect-report PATH` lists PDFs worth
  re-fetching and changes nothing.
* **Tesseract (optional, for `--ocr`).** Install the engine and the language data your papers need
  (OCR asks for the `.ris` record's language plus English):
  Debian or Ubuntu `sudo apt install tesseract-ocr` (languages as `tesseract-ocr-<lang>`), macOS
  `brew install tesseract tesseract-lang`, Windows the Tesseract installer of your choice. The
  pipeline uses the `tesseract` on your PATH; on Windows it also finds
  `%ProgramFiles%\Tesseract-OCR\tesseract.exe`. Set `TESSDATA_PREFIX` to the folder holding the
  `.traineddata` files when they are not where the engine looks by default (on Windows, a
  `%LOCALAPPDATA%\Tesseract-OCR\tessdata` folder is used when present and the variable is unset:
  use-case-only).
* `build_pdf_library.py --base-dir PROJECT` (Tier 1): text dumps, metadata and a library report.
  `extract_tables.py --project KEY` (or `--lib-dir`/`--out-dir`): pdfplumber tables as CSVs.
* JATS full text (from PMC and Europe PMC) is converted with MathML formulas rendered to LaTeX by
  the vendored `py-mathml-to-latex` (`vendor/VENDORED.md`); the source MathML stays in each
  `formulas[i].mathml_input`, so a wrong conversion can be redone with another tool.
* `fetch_figures.py --lib-dir DIR`: figure images for PMC sidecars from the PMC Cloud Service,
  matched to the JATS `<graphic>` names and tagged with the article's licence and a reuse category.
  Images only: plot values have to be digitised by hand.

## Health checks and audits

```bash
uv run python pipeline_check.py --all                  # every active project, one summary (read-only)
uv run python audit_portfolio.py                       # libraries, queues, index and live runs (read-only)
uv run python -m litpipe.canaries --profile daily --dry-run   # the runner's health checks, listed
uv run python -m litpipe.state --status                # hosts, refusals, deferrals, live runs
```

`pipeline_check.py` and `audit_portfolio.py` open the index read-only and report `needs_ocr`
sidecars on their own INFO line. `audit_portfolio.py` reads archived sweep reports from each
project's `artifact_dir` and from any `--report-dir DIR` you name (repeatable); it assumes no
folder layout of its own.

A host that refuses scripted traffic is refused for the rest of the run; arXiv refusals persist
until cleared by hand (`python -m litpipe.state --clear-refusal export.arxiv.org`), because retrying
a refusing host only prolongs it. Every request attempt is a line in `<state_dir>/ledger/`, with
the contact address and keys redacted.

## Caveats

* **Snowball selection bias.** A corpus grown by citation walks from your seeds over-represents your
  seeds' neighbourhood. It cannot evidence how common its own seed terms or findings are in the
  field, and a paper outside that neighbourhood is invisible however important it is. Search
  independently before claiming coverage.
* **Recency.** Forward walks favour older papers (they have had time to be cited), while
  recency-weighted ranking does the opposite: one recency-weighted relevance pass over a topic
  dropped both of its anchor papers. `--recent-first` sorts by year; check that your anchors survive
  any ranking you apply.
* **PDF-only retrieval.** The fetch stages retrieve the article (a PDF, or its full text) and
  nothing else: supplementary files, datasets and videos are never fetched.
* **Anonymous access.** Open-access copies only. Papers without one go to the ILL list; that is a
  property of the tool, not a gap to be closed. Unpaywall covers Crossref DOIs only (other
  registration agencies are reported as not at Crossref).
* **What `top_candidates` can and cannot tell you.** It lists DOIs no library holds, ranked by how
  many distinct seeds point to them (citing or cited) and then by citation count, pooled over the
  whole portfolio and as fresh as the last index run. It cannot judge topical relevance, see DOIs
  that no walk returned (a failed or unwalked seed, a reference without a DOI), tell an article
  from a commentary or erratum, or know about papers newer than the last walk. A text-only holding
  counts as held. Use the seeder's `--rank project` for one project's own seeds, and review every
  draft.

## Platforms and scope

| | Status |
|---|---|
| Linux, Windows | Supported and tested in CI on every push (Ubuntu and Windows runners), including the Windows job-object process control the runner uses |
| macOS | Best effort: not tested in CI; `schedule-print` gives a crontab line only |
| Python | 3.11 to 3.13 (CI tests 3.11 and 3.13) |

* **The state DB needs a local disk.** `state_dir` holds a SQLite database in WAL mode, which does
  not work on a synced or network folder. Keep `state_dir` local, one per machine.
* **`db_dir` defaults inside the projects tree** (`<root>/_references`), which suits one machine.
  A machine that runs the nightly task should hold `portfolio.duckdb` on its own local disk (set
  `db_dir`) with unattended DB writes on.
* Everything a user sets up is configuration (`projects.json`) or data (queues, pools, gate specs);
  no path, institution or project is built into the code.

**Use-case-only features** (they work, but they serve one kind of setup; nothing else depends on
them):

* EZproxy routing, the paywall pull session and the ILL and paywall worklists, for users with
  institutional library access;
* the `~/Downloads` default of `import_downloads.py`, `harvest_citations.py` and `paywall_pull.py`
  (each takes a folder flag);
* a projects tree on a synced folder shared between machines (the project lock and the day and
  night split exist for it);
* the Windows Tesseract default paths (`%ProgramFiles%`, `%LOCALAPPDATA%`).

## Layout

```
README.md, ROADMAP.md, CITATION.cff, LICENSE, pyproject.toml, uv.lock
projects.json.template          the registry template (copy to projects.json)
lit_pull_queue.template.csv     an example queue

sweep.py                        queue discovery, the fetch stages, residual classes
unpaywall_fetch_v2.py           stage: Unpaywall
pmc_fetch.py                    stage: PMC (PMC Cloud Service, Europe PMC)
preprint_fetch.py               stage: preprint servers
extract_pdf_fulltext.py         text sidecars, the validity gate, OCR
migrate_closed_to_md.py         the route step and the worklist files
run_daily.py                    wrapper over the runner's daily profile

forward_citations.py, reverse_citations.py, snowball.py        the walks
index_portfolio.py, seed_queue_from_top_candidates.py          the index and drafting
enrich_abstracts.py, enrich_recommendations.py                 index enrichment

import_downloads.py, paywall_pull.py, build_priority_paywall_queue.py,
harvest_citations.py, audit_filenames.py, fill_missing_dois.py, backfill_ris.py,
backfill_fulltext.py, recheck_pmc.py                            library tools
build_pdf_library.py, extract_tables.py, pdf_text_clean.py,
jats_to_text.py, fetch_figures.py                               extraction
pipeline_check.py, audit_portfolio.py                           read-only instruments
lit_util.py, lit_net.py, ris_emit.py                            shared helpers

litpipe/        net (the HTTP client), hosts (host policy), state, ledger, outcomes, config,
                doi, text, identity, holdings, s2, openalex, walk, worklists, canaries,
                preflight, runner, lockfile, gate, enrich_s2
backfills/      one-off repair scripts (dry run by default)
vendor/         the vendored MathML-to-LaTeX converter
tests/          the test suite (offline: every network call is mocked)
```

## Related tools

| Need | Closest open-source tools | How this differs |
|---|---|---|
| Unpaywall client | unpywall | adds landing-page PDF discovery, host refusal handling and identity checks |
| PMC PDFs and JATS | paperscraper, pubget | uses only NCBI's sanctioned automated routes; text-only holdings for author manuscripts |
| Preprints | paperscraper | per-project source selection, OSF and SportRxiv, Europe PMC preprint full text |
| JATS to JSON with math | s2orc-doc2json | display formulas converted, source MathML kept |
| Citation graph, portfolio index, orchestration | none bundled | walks with a cache and a degraded-run guard, one DuckDB index across projects, a scheduled runner |

If you only need one stage, a maintained package may serve you better; the pipeline's value is the
whole loop across many projects.

## License and citation

MIT (see `LICENSE`). To cite the software, use `CITATION.cff`. The forward plan is in `ROADMAP.md`.
