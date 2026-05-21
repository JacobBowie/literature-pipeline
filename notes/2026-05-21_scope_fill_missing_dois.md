# Scope: `fill_missing_dois.py`

**Purpose:** Recover DOIs for the 317 "PDFs without DOI" orphans across the 4 active project libs by querying CrossRef title-match from filename hints.

**Status:** scoped + empirically tested 2026-05-21; not yet built.

---

## Empirical test results (21-sample run, 2026-05-21)

Ran a prototype CrossRef title-match against a stratified sample of 21 orphans (12 canonical-format, 5 legacy-format `lastname_year_slug`, 4 gibberish).

### Hit-rate breakdown by filename class

| Class | n in test | HIGH | MED | UNCLEAR | Hit rate (HIGH+verifiable) |
|---|---|---|---|---|---|
| Canonical `YYYY_LastName_Title.pdf` | 12 | 9 | 1 (year mismatch — real catch) | 2 | ~83% |
| Legacy `lastname_YYYY_slug.pdf` | 5 | 2 | 1 (year mismatch) | 2 | ~40% |
| Gibberish (journal issue / multi-author bundle) | 4 | 0 | 0 | 4 | 0% |

### Population extrapolation (317 orphans, breakdown from `orphan_audit.py`)

| Class | Count | Expected recovery |
|---|---|---|
| Canonical | 266 (84%) | ~220 (~83% × 266) |
| Legacy | 5 (2%) | ~2 |
| Gibberish | 46 (14%) | ~0 (these are journal issues like `2007_IJCSS_Vol6_Edition2.pdf`, structurally unmatchable) |
| **Total** | **317** | **~220 recoverable (~70%)** |

### Failure modes observed

1. **Wrong-type DOI**: when paper is *about* a software package, CrossRef may return the package's DataCite DOI instead of the paper's. Example: `schweiker_2016_comf.pdf` is an R Journal article, but CrossRef returned `10.32614/cran.package.comf` (CRAN metadata DOI). **Mitigation: filter results by `type` field, prefer `journal-article` / `proceedings-article` / `book-chapter`.**

2. **Year mismatch**: filename year wrong, CrossRef returns paper with different year but same author + title. Example: `1976_Bishop_AppliedResearchModelSportSciences.pdf` → CrossRef says 2008. The CrossRef record is correct; the filename year is wrong. **Mitigation: flag for review, don't auto-write — but DON'T discard either, the catch is valuable.**

3. **Ambiguous top-2**: top result and #2 result have similar scores (within ~10%). Example: `1995_Nieman_AcuteImmuneResponseExhaustiveResistanceExercise.pdf` returned Stone 1994 as top match (different author, same title — possibly co-authored or follow-up paper). **Mitigation: flag AMBIG and require human review.**

4. **Short/uninformative titles**: legacy filenames like `schiffmann_2021_cvri.pdf` have 4-letter title slugs. CrossRef can't discriminate. **Mitigation: read first 5KB of `.fulltext.json` text field to extract title, fall back to filename hint only when text-derived title is also empty.**

5. **Bundled journal issues**: `2007_IJCSS_Vol6_Edition2.pdf` — these are full journal issues, not single papers. No DOI exists at the issue level for IJCSS. **Mitigation: detect issue-level patterns (`Vol\d`, `Edition\d`, `Issue\d`) and skip with `SKIP_BUNDLED_ISSUE` status.**

---

## Functional spec

### CLI

```bash
# Dry-run (default) — emits report CSV, doesn't touch sidecars
python fill_missing_dois.py --project getpaid

# Apply HIGH-confidence updates only
python fill_missing_dois.py --project getpaid --execute --min-confidence HIGH

# Apply HIGH+MED, write proposed-but-not-yet-applied entries to a manual-review queue
python fill_missing_dois.py --project getpaid --execute --min-confidence MED \
    --review-queue /c/tmp/litpipe_logs/manual_review_getpaid.csv

# All projects
python fill_missing_dois.py --all --execute

# Test on N orphans only
python fill_missing_dois.py --project getpaid --limit 20
```

### Algorithm (per orphan)

1. **Parse hints from filename**: year, author surname, title fragment, parse class (canonical / legacy / gibberish).
2. **Detect SKIP cases early**:
   - Bundled journal issue pattern → `SKIP_BUNDLED_ISSUE`
   - No year AND no author AND no useful title → `SKIP_NO_HINTS`
3. **Augment with sidecar text** when filename hint is short (< 4 words): grep first 2KB of `.fulltext.json` text field for "Title:", first capitalized line, or use 1st non-blank line as title.
4. **Query CrossRef** `query.bibliographic` with `{author} {title} {year}`, request 5 results.
5. **Filter by type**: prefer `journal-article`, `proceedings-article`, `book-chapter`, `report`. Demote `dataset`, `peer-review`, `component`, `dataset`, `grant`, `book` (whole-book), `book-set`, `book-series`.
6. **Score top filtered result** against parsed hints:
   - `HIGH` — author match (case-insensitive substring) AND year match AND top1 score ≥ 80% of theoretical max AND top1/top2 ratio ≥ 1.10
   - `MED_AUTHOR_YEAR_MISMATCH` — author match, year differs by ≥ 2 (flag for human: filename year likely wrong)
   - `MED_AUTHOR_ONLY` — author match, year unknown (filename year is `0000` or missing)
   - `MED_TYPE_MISMATCH` — top result has wrong type (e.g. dataset) — emit the next valid type result with confidence dropped
   - `LOW_TITLE_ONLY` — title match, no author match
   - `AMBIG` — top1/top2 ratio < 1.10 (two candidates equally plausible)
   - `NO_RESULT` — empty CrossRef response
7. **Write to** `.fulltext.json` sidecar (`doi`, `title`, `year`, `authors`, `journal` fields) when status meets `--min-confidence` threshold AND `--execute`.
8. **Log all proposals** to report CSV regardless of execute/dry-run, with columns:
   `filename, parsed_year, parsed_author, parsed_title, status, crossref_doi, crossref_type, crossref_title, crossref_year, crossref_authors, top1_score, top2_score, applied`

### Integration with existing pipeline

After `fill_missing_dois.py --execute` lands:

1. **Re-run `audit_filenames.py --execute`** → newly populated sidecar DOIs let it propose canonical renames for previously orphaned PDFs.
2. **Re-run `index_portfolio.py --project NAME`** → newly DOI'd PDFs get `paper_locations` rows + flow into the citation graph.
3. **Next snowball** picks up the freshly-indexed PDFs as seeds.

This is a 3-step retrofill chain. The whole-portfolio version takes ~10 minutes wall clock total: ~3 min CrossRef queries, ~5 min audit_filenames CrossRef lookups (already done for non-orphans), ~2 min indexer.

### Output: 4 artifacts per run

1. `<project>/_doi_fill_report.<YYYY-MM-DD>.csv` — all proposals (every orphan that was attempted)
2. `<project>/_doi_fill_applied.<YYYY-MM-DD>.csv` — successful sidecar updates
3. `<project>/_doi_fill_review.<YYYY-MM-DD>.csv` — MED-confidence + AMBIG cases for human review
4. `<project>/_doi_fill_skipped.<YYYY-MM-DD>.csv` — SKIP_BUNDLED_ISSUE / SKIP_NO_HINTS (no API call wasted)

---

## Dependency analysis: build vs library

Surveyed Python CrossRef wrappers:

| Option | Pros | Cons | Verdict |
|---|---|---|---|
| **Custom `requests`** (prototype already works) | No new deps, full control over query construction + scoring | Manual rate-limiting + retry logic | **Use this** |
| `habanero` (~2k stars, active) | Pythonic, handles politeness | Adds dep for ~50 LOC of value | Skip |
| `crossrefapi` | Simple wrapper | Less maintained | Skip |
| `pyalex` (OpenAlex) | OpenAlex has better citation graph than CrossRef in some niches | Different DB, separate eval needed | Skip for this scope, revisit if hit rate disappoints |

**Conclusion: stick with `requests`. The prototype is ~50 LOC and works.** Total spec implementation: ~200 LOC (parser, scorer, type filter, sidecar writer, CLI, report generator).

---

## Risk assessment

| Risk | Likelihood | Severity | Mitigation |
|---|---|---|---|
| Wrong DOI written to sidecar, downstream poisons indexer | Med (~5-10% of MED-confidence cases) | High (citation graph corruption) | Require `HIGH` for `--execute` by default. MED/AMBIG go to review queue. |
| CrossRef API rate-limit / 429 | Low (we'd be at ~1.7 req/sec, well below polite cap) | Low (retry with backoff) | Set polite User-Agent with mailto. Sleep 250ms between calls. Retry 3× on 429. |
| Sidecar write race-condition (script + concurrent indexer) | Low | Med | Document: don't run concurrently with snowball or index_portfolio. Acquire portfolio.duckdb lock check before starting. |
| Filename hints worse than expected | Med | Med | Empirical test shows canonical is reliable; gibberish skipped; legacy reads sidecar text as fallback. |
| CrossRef changes API | Very low | Low | Pin to v1 (already standard). Add `--api-version` flag if needed. |

---

## Estimated effort + payoff

- **Build effort**: ~3-4 hours (custom code based on prototype + integration tests + CLI + report writer)
- **Total runtime on portfolio**: ~3 min CrossRef queries + ~5 min audit_filenames + ~2 min index = ~10 min total wall clock
- **Recovery**: ~220 of 317 orphans (~70%) → from 803 → ~1,020 unique papers in `paper_metadata` with `paper_locations`
- **Citation-graph effect**: those 220 newly-indexed PDFs become seeds in next snowball, surfacing potentially thousands of new candidates (especially for getpaid's 183 orphans, many of which are foundational pre-2000 papers that would expand the dose-response network significantly)

**Payoff/effort ratio is high.** Build is bounded (~half-day), runtime is fast, and the citation graph downstream effect is substantial.

---

## Open questions for next-session implementation

1. **`year=0000` handling**: filename had no extractable year. Should the script try a single-pass lookup with no year filter, or skip these? Test data suggests they ARE matchable (Bishop 2003 case) — keep them, classify as `MED_NO_YEAR`.
2. **Multi-author filename hints**: e.g. `2006_Hellard_Banister_Limitations.pdf` — current script parses only the first capitalized word as author. Should it concatenate? Test showed `Hellard` alone was enough; CrossRef ranking handled the rest. Probably leave as-is, but document.
3. **What about the 46 gibberish files** (mostly IJCSS issues)? Recommend a separate one-off triage: these may need to be SPLIT into individual papers (extract per-article DOIs from the ToC) OR kept as bundle PDFs with a `_is_bundle: true` flag in sidecar. Out of scope for this script.

---

## Test artifacts (pre-implementation)

- `C:\tmp\orphan_audit.py` — orphan classifier (4 libs, 317 orphans); logic absorbed into `fill_missing_dois.discover_orphans` post-ship.
- `C:\tmp\crossref_titlematch_test.py` — 21-sample CrossRef prototype (results above); superseded by `fill_missing_dois.py` + `tests/test_fill_missing_dois.py`.
- Both scratch scripts deleted 2026-05-21 after the shipped tool + 29-case pytest suite passed validation.

---

## Implementation order (when ready to build)

1. Refactor `crossref_titlematch_test.py` into a module with `query_crossref()` + `score_match()` functions
2. Add type-filtering against CrossRef `type` field
3. Add sidecar-text fallback for short-title cases
4. Add SKIP detection (bundled issues, no hints)
5. Add CSV report writers (4 artifact files)
6. Add `--execute` sidecar update path
7. Smoke-test on getpaid (largest orphan set, 183)
8. Run on all 4 projects after sanity-check of getpaid results
9. Chain: `audit_filenames --execute` → `index_portfolio` per project
10. Re-run `orphan_audit.py` to measure final orphan count (target: < 100 from 317)
