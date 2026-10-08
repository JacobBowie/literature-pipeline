"""Sweep project lit-pull queues and run the fetch pipeline against each.

Queues (dispatch 0.5), looked for in every active registered project, or in the one --project names:
  lit_pull_queue.csv              the live queue
  lit_pull_queue.<tag>.csv        a tagged queue: <tag> matches ^[a-z][a-z0-9_-]{0,31}$, is not
                                  date-like and not reserved, and the file has `doi` and
                                  `destination` columns (a pool or candidate list without them is
                                  reported and left alone)
  lit_pull_queue.retry_later.csv  rows whose `not_before` is on or before the run date move into
                                  lit_pull_queue.retry.csv (tag `retry`) and are swept
The queue shape is doi,title,authors,year,destination,notes, for example
    10.1152/jappl.1972.32.6.812,"Predicting rectal temperature","Givoni B; Goldman R",1972,docs/literature/,for Chapter 3
Leading '#' lines are ignored. Every row shares the first row's destination.

Each invocation picks one run id per project: YYYY-MM-DD for the first run of the day, then
YYYY-MM-DD.2, .3, ..., the first id no artifact uses, so a same-day re-sweep never overwrites an
earlier record. Every artifact of the run carries it:
    lit_pull_queue[.<tag>].<run_id>.<stage>.csv
    stage: normalized, unpaywall, pmc, preprint, residual, report, processed (and pmc_input,
           PMC's input when the sources omit unpaywall)
The dated names of earlier versions (lit_pull_queue.<date>.<stage>.csv and the same-day
lit_pull_queue.<date>.processed.<n>.csv) stay readable (parse_artifact).

The artifacts go to the project root, or to projects.json `artifact_dir` when it is set
(litpipe.config.artifact_dir: per project, else global; a global absolute path holds one folder per
project), or to --artifact-dir for one run. The queue files, retry_later and the worklist .md
files always stay in the project root.

The lock: every project with something staged is swept under its lock file,
<project root>/lit_pull_queue.lock (litpipe.lockfile: host, pid, run id, start and heartbeat;
stale after 2 h, projects.json runner.lock_stale_s), taken before the retry_later admission and the
queue discovery and released after --migrate. A lock another live process holds (the scheduled
runner, a sweep on another machine) skips that project untouched, as exit 4's "refused before
fetching", naming the holder's host, tool and heartbeat age; a runner's stage child joins its
runner's lock (the same run id). A lock lost while the project runs (another process took it over)
stops that project's later stages as an abort (exit 3, the queue kept, no LOOSE_ENDS line, no
routing). --dry-run takes no lock and only reports a held one.

Sources (DEC-31): each project's `sources` list (default unpaywall + pmc), or --sources for one
run, gates every stage. A stage whose source is not listed is skipped, as --skip-preprint skips the
preprint stage: the report reads "skipped (the project's sources omit <stage>)", the residual lists
it in skipped_sources, and it never blocks retirement. Without Unpaywall, sweep's own-library check
builds lit_pull_queue[.<tag>].<run_id>.pmc_input.csv from the queue (each row's DEC-14 filename, and
SKIP_EXISTS where the library already holds the paper) whatever the later stages are, PMC's input
when PMC runs, and a held paper never reaches the preprint stage or the ILL list (also for
preprint-only sources). A sources list that leaves no sweep stage
(an empty list, or openalex_content only) refuses the project's queues before fetching (exit 4).

Per queue: a row with an invalid or placeholder DOI (NO_DOI_*) is skipped (INVALID_DOI); a row
held in another registered library is not fetched (HELD_ELSEWHERE, with the path); a blank title
or author list is filled from CrossRef/DataCite, and a title that stays blank is not fetched
(NO_METADATA; a source that could not answer is asked once more in the run first). The rest go
through Unpaywall v2, PMC and the preprint stage (each when the sources list it), then PDF text
extraction. The preprint stage runs (with --project) only when the project's `sources` name a
preprint server (DEC-31: a project with no `sources` key has unpaywall and pmc only), or for the
rows with a 10.48550/ (arXiv) DOI, which reach arXiv whatever the sources (DEC-09); --skip-preprint
skips it outright.

Every row that was not fetched gets a residual class (dispatch 0.5) in the residual CSV, read
from each stage report's typed columns (outcome, detail, identity, release_date, landing_url)
when present and from the legacy columns (error / status through from_legacy) otherwise. The
residual CSV also carries not_before (an embargo's release date), flagged_path (a file the
identity check flagged), landing_url (the page a person opens for a manual preprint) and, last,
best_oa_url (copied from the Unpaywall report's column of that name; empty when the report has
none). The report counts rows per class and per stage, SKIP_EXISTS apart from fetched.
The queue retires (renamed to its .processed.csv) when every fetch stage completed and every row
has a class. A deliberately skipped stage (--skip-preprint, or a project whose sources name no
preprint server) does not block retirement; a failed or crashed stage does, and the queue stays
for the next sweep. A stage that exits 2, or a CONFIG outcome in any row, aborts the run.
Routing the residual rows (ILL list, retry_later, review) is migrate_closed_to_md.py's job:
--migrate runs it for this run id, otherwise the exact command is printed.
"""
import argparse
import csv
import datetime
import io
import os
import re
import shlex
import subprocess
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

import lit_util
lit_util.utf8_stdout()

from litpipe.outcomes import Kind, from_legacy  # noqa: E402
from litpipe import lockfile as _lockfile  # noqa: E402

HERE = Path(__file__).resolve().parent
CONFIG_PATH = HERE / "projects.json"

EXIT_OK = 0
EXIT_NO_QUEUE = 1
EXIT_USAGE = 2
EXIT_STAGE_FAILED = 3
EXIT_QUEUE_REFUSED = 4
EXIT_CODES_HELP = """exit codes:
  0  every stage of every queue completed, including when nothing was fetched or nothing was
     staged (a bare sweep with no queue is an idle run)
  1  --project named a project with nothing to sweep
  2  usage or configuration error (bad arguments, an invalid `sources` list or --sources, an
     invalid artifact_dir or runner.lock_stale_s, a CONFIG outcome such as Unpaywall 422, or a
     stage that exited 2; each aborts the run)
  3  a stage failed or crashed (unpaywall, pmc, preprint, pdf extract, or --migrate), or a
     project's lock was lost mid-run; the queue is left in place for the next sweep unless only
     the extract or migrate step failed
  4  a queue was refused before fetching (no destination; a destination outside the project or
     not the registry library for the project; sources that name no sweep stage), or a project
     was skipped because another live process holds its lock file; it is left in place
When several apply, the first of 2, 3, 4 wins.

Each project's run id is printed as `[sweep] run_id=<id> project=<key>`, so a wrapper can find
this run's artifacts (lit_pull_queue.<id>.<stage>.csv) after a same-day re-sweep."""

# ---------------------------------------------------------------- queue and artifact names
QUEUE_FILE = "lit_pull_queue.csv"
RETRY_LATER_FILE = "lit_pull_queue.retry_later.csv"
RETRY_TAG = "retry"
QUEUE_COLUMNS = ("doi", "title", "authors", "year", "destination", "notes")
TAG_RE = re.compile(r"[a-z][a-z0-9_-]{0,31}")
DATE_LIKE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
RESERVED_TAGS = frozenset({
    "template", "draft", "retry_later", "bak", "processed", "normalized", "unpaywall", "pmc",
    "preprint", "residual", "report", "oa_blocked", "review", "corrections"})
ARTIFACT_STAGES = ("normalized", "unpaywall", "pmc", "preprint", "residual", "report", "processed",
                   "pmc_input")
PMC_INPUT_STAGE = "pmc_input"   # PMC's input when the project's sources omit unpaywall (DEC-31)
_ARTIFACT_RE = re.compile(
    r"lit_pull_queue(?:\.(?P<tag>[a-z][a-z0-9_-]{0,31}))?"
    r"\.(?P<date>\d{4}-\d{2}-\d{2})(?:\.(?P<seq>\d+))?"
    r"\.(?P<stage>[a-z_]+)(?:\.(?P<legacy_seq>\d+))?\.csv")
PREPRINT_SOURCES = frozenset({"europepmc_preprints", "biorxiv", "medrxiv", "osf", "sportrxiv", "arxiv"})
SWEEP_SOURCES = frozenset({"unpaywall", "pmc"}) | PREPRINT_SOURCES   # sources a sweep stage serves
ARXIV_DOI_PREFIX = "10.48550/"   # DEC-09: arXiv DOIs reach arXiv whatever the project's sources
CANDIDATE_ORDERS = ("repository", "publisher")   # unpaywall_fetch_v2 --candidate-order (DEC-11)

LOOSE_DONE = "✅ Lit pull done:"
LOOSE_PARTIAL = "⏸️ Lit pull PARTIAL:"


@dataclass(frozen=True)
class ArtifactName:
    tag: str             # "" for the untagged queue
    run_id: str          # YYYY-MM-DD or YYYY-MM-DD.N
    stage: str
    legacy_seq: int | None = None   # the old same-day .processed.<n>.csv suffix

    @property
    def date(self):
        return self.run_id[:10]


def parse_artifact(name):
    """ArtifactName for a run artifact file name, current or legacy; None for anything else."""
    m = _ARTIFACT_RE.fullmatch(name)
    if not m:
        return None
    if m["legacy_seq"] and (m["stage"] != "processed" or m["seq"]):
        return None
    run_id = m["date"] + (f".{m['seq']}" if m["seq"] else "")
    return ArtifactName(m["tag"] or "", run_id, m["stage"],
                        int(m["legacy_seq"]) if m["legacy_seq"] else None)


def artifact_name(tag, run_id, stage):
    return f"lit_pull_queue{'.' + tag if tag else ''}.{run_id}.{stage}.csv"


def run_ids_in_use(dirs):
    """Run ids that already have a sweep artifact in any of `dirs` (legacy names included)."""
    used = set()
    for d in dirs:
        d = Path(d) if d else None
        if d is None or not d.is_dir():
            continue
        for p in d.glob("lit_pull_queue.*.csv"):
            a = parse_artifact(p.name)
            if a and a.stage in ARTIFACT_STAGES:
                used.add(a.run_id)
    return used


def run_id_dirs(project_dir, art_dir):
    """Where a run id may already be in use: the project, this run's artifact dir, and every direct
    subdirectory of the project (a relative --artifact-dir of an earlier run)."""
    pd = Path(project_dir)
    subs = [p for p in pd.iterdir() if p.is_dir()] if pd.is_dir() else []
    return {pd, Path(art_dir), *subs}


def choose_run_id(dirs, date):
    """`date` when no artifact carries it yet, else the first free `date.N` (N >= 2)."""
    used = run_ids_in_use(dirs)
    if date not in used:
        return date
    n = 2
    while f"{date}.{n}" in used:
        n += 1
    return f"{date}.{n}"


def is_valid_tag(tag):
    return bool(TAG_RE.fullmatch(tag or "")) and not DATE_LIKE_RE.search(tag) and tag not in RESERVED_TAGS


def queue_tag(name):
    """"" for lit_pull_queue.csv, the tag of a validly tagged queue name, None for anything else."""
    if name == QUEUE_FILE:
        return ""
    m = re.fullmatch(r"lit_pull_queue\.([^.]+)\.csv", name)
    if m and is_valid_tag(m.group(1)):
        return m.group(1)
    return None


# ---------------------------------------------------------------- reading queues
def read_queue(path):
    """(fieldnames, rows) of a queue-shaped CSV. Leading '#' comment lines (seeder drafts carry
    them) and blank lines are dropped, and so is any row whose doi cell starts with '#'."""
    with open(path, encoding="utf-8-sig", newline="") as f:
        lines = f.read().splitlines(keepends=True)
    i = 0
    while i < len(lines) and (not lines[i].strip() or lines[i].lstrip().startswith("#")):
        i += 1
    rdr = csv.DictReader(io.StringIO("".join(lines[i:])))
    fields = list(rdr.fieldnames or [])
    rows = []
    for r in rdr:
        r.pop(None, None)   # cells past the header
        if (r.get("doi") or "").lstrip().startswith("#"):
            continue
        rows.append({k: (v if v is not None else "") for k, v in r.items()})
    return fields, rows


def _missing_queue_columns(path):
    try:
        fields, _ = read_queue(path)
    except (OSError, UnicodeDecodeError, csv.Error) as e:
        return [f"readable content ({type(e).__name__})"]
    return [c for c in ("doi", "destination") if c not in fields]


def first_destination(queue_csv):
    """Read the first row's destination column. All rows should share."""
    _, rows = read_queue(queue_csv)
    for r in rows:
        d = (r.get("destination") or "").strip()
        if d:
            return d
    return None


def normalize_queue_for_pipeline(queue_csv, out_csv):
    """The unpaywall_fetch_v2 script expects a `citation_count` column.
    Rewrite the queue to add it (defaulting to 0) and pass through the rest."""
    fields, rows = read_queue(queue_csv)
    if "citation_count" not in fields:
        fields.append("citation_count")
    for r in rows:
        if not (r.get("citation_count") or "").strip():
            r["citation_count"] = "0"
    _write_csv(out_csv, rows, fields)


def _write_csv(path, rows, fields):
    lit_util.atomic_write_csv(str(path), [{k: r.get(k, "") for k in fields} for r in rows],
                              list(fields))


def _union(*field_lists):
    out = []
    for fl in field_lists:
        for f in fl:
            if f not in out:
                out.append(f)
    return out


# ---------------------------------------------------------------- discovery
def _registry():
    return lit_util.load_projects_config(CONFIG_PATH).get("projects", {})


def _project_roots(only_project=None, cfg=None):
    cfg = _registry() if cfg is None else cfg
    keys = [only_project] if only_project else [
        k for k, v in cfg.items() if v.get("active", True)]
    for key in keys:
        yield key, lit_util.project_root(key, cfg.get(key, {}))


def discover_queues(root, ignored=None):
    """Live queues in one project directory: lit_pull_queue.csv first, then tagged queues by name.
    A tag-shaped file without `doi` and `destination` columns is not a queue (a consumer keeps candidate
    pools such as lit_pull_queue.unit3_pool.csv beside its batches); it is appended to `ignored`
    as (path, reason) and never swept."""
    root = Path(root)
    if not root.is_dir():
        return []
    found = []
    for p in root.glob("lit_pull_queue*.csv"):
        tag = queue_tag(p.name)
        if tag is None or not p.is_file():
            continue
        if tag:
            missing = _missing_queue_columns(p)
            if missing:
                if ignored is not None:
                    ignored.append((p, f"not a queue (no {', '.join(missing)} column)"))
                continue
        found.append((tag, p))
    found.sort(key=lambda t: (t[0] != "", t[0]))
    return [p for _, p in found]


def find_queues(only_project=None, ignored=None):
    """Yield (project_key, project_dir, queue_csv_path) for each live queue of each active
    registered project (or of `only_project`).

    Registry-driven (2026-07-14, A1 fix): resolve each projects.json key via
    lit_util.project_root (tail-aware), so a subproject key like
    'Parent/Sub' resolves to <Projects>/Parent/Sub and
    ITS queue is found. Replaces the old PROJECTS.iterdir() filesystem walk, which
    only saw top-level dirs and whose `child.name` compare could never match a slash
    key -- so a registered subproject's queue was silently never swept (a permanent
    no-op that the orchestrator read as success). Registration is now the entry
    point: only active registered projects are swept. An explicit --project that is
    not in the registry still resolves as a top-level dir (backward compatible).
    Tagged queues (dispatch 0.5) are found alongside lit_pull_queue.csv.
    """
    for key, root in _project_roots(only_project):
        for q in discover_queues(root, ignored):
            yield key, root, q


# ---------------------------------------------------------------- retry_later admission
def _parse_date(s):
    try:
        return datetime.date.fromisoformat((s or "").strip()[:10])
    except ValueError:
        return None


def split_retry_later(root, today):
    """(fieldnames, due, waiting, unreadable) for <root>/lit_pull_queue.retry_later.csv.
    A row is due when `not_before` is blank or on or before `today`; an unparseable date keeps
    the row waiting and is reported."""
    p = Path(root) / RETRY_LATER_FILE
    if not p.exists():
        return [], [], [], []
    fields, rows = read_queue(p)
    t = datetime.date.fromisoformat(today)
    due, waiting, bad = [], [], []
    for r in rows:
        nb = (r.get("not_before") or "").strip()
        d = _parse_date(nb)
        if not nb or (d is not None and d <= t):
            due.append(r)
        else:
            waiting.append(r)
            if d is None:
                bad.append(r)
    return fields, due, waiting, bad


def admit_retries(root, today, default_destination=None):
    """Move the due rows of lit_pull_queue.retry_later.csv into lit_pull_queue.retry.csv (a
    tagged queue, appended and de-duplicated by DOI) and remove them from retry_later. The retry
    queue is written first, so a crash between the two writes duplicates rows (harmless: the
    next admission de-duplicates) rather than losing them. Returns counts."""
    root = Path(root)
    fields, due, waiting, bad = split_retry_later(root, today)
    out = {"admitted": 0, "waiting": len(waiting), "unreadable_not_before": len(bad)}
    if not due:
        return out
    rq = root / f"lit_pull_queue.{RETRY_TAG}.csv"
    old_fields, old_rows = read_queue(rq) if rq.exists() else ([], [])
    seen = {lit_util.normalize_doi(r.get("doi")) for r in old_rows}
    add = []
    for r in due:
        d = lit_util.normalize_doi(r.get("doi"))
        if d and d in seen:
            continue
        seen.add(d)
        if default_destination and not (r.get("destination") or "").strip():
            r["destination"] = default_destination
        add.append(r)
    header = _union(QUEUE_COLUMNS, old_fields, fields)
    _write_csv(rq, old_rows + add, header)
    _write_csv(root / RETRY_LATER_FILE, waiting, fields or list(QUEUE_COLUMNS))
    out["admitted"] = len(add)
    return out


def _dkey(raw):
    """The DOI key rows are compared by: the shared normaliser, else the legacy lower-case form."""
    raw = str(raw or "").strip()
    return _doi.normalise(raw) or lit_util.normalize_doi(raw)


def retry_history(root):
    """{doi key: attempts} from <root>/lit_pull_queue.retry_later.csv, every row (due or waiting).
    run() reads it before admission, so a DOI re-queued fresh (not through `retry`) keeps its count
    and an ERROR row still closes on its third run. {} when the file is absent or unreadable."""
    p = Path(root) / RETRY_LATER_FILE
    if not p.exists():
        return {}
    try:
        _, rows = read_queue(p)
    except (OSError, UnicodeDecodeError, csv.Error):
        return {}
    out = {}
    for r in rows:
        k = _dkey(r.get("doi"))
        if k:
            out[k] = max(out.get(k, 0), lit_util.coerce_int(r.get("attempts")))
    return out


# ---------------------------------------------------------------- per-row preparation
def _resolve_meta(doi):
    """ris_emit.resolve_meta, imported late (network; tests replace this function)."""
    import ris_emit
    return ris_emit.resolve_meta(doi)


def _initials(given):
    return "".join(p[0].upper() for p in re.split(r"[\s.\-]+", given or "") if p)


def _format_authors(authors):
    out = []
    for a in authors or []:
        if isinstance(a, dict):
            fam = (a.get("family") or a.get("name") or "").strip()
            ini = _initials(a.get("given"))
            if fam:
                out.append(f"{fam} {ini}".strip())
        elif str(a).strip():
            out.append(str(a).strip())
    return "; ".join(out)


def _metadata_unavailable():
    """ris_emit.MetadataUnavailable (a source could not answer), imported late; () when absent, so
    isinstance(e, ...) is simply False."""
    try:
        import ris_emit
        return ris_emit.MetadataUnavailable
    except (ImportError, AttributeError):
        return ()


def fill_metadata(row, doi):
    """Fill a blank title, author list or year in place from CrossRef, then DataCite
    (ris_emit.resolve_meta). Returns "filled", "unavailable" (no source holds a record: a genuine
    not-found) or "error:<ExceptionType>" ("error:MetadataUnavailable" when a source could not
    answer). Only the exception type is kept: its text can carry a request URL with an email."""
    return _fill(row, doi)[0]


def _fill(row, doi):
    """fill_metadata's work: (result, source_unavailable), the flag true when resolve_meta raised
    ris_emit.MetadataUnavailable (prepare_rows asks such a row once more in the run)."""
    try:
        meta, _source = _resolve_meta(doi)
    except Exception as e:   # resolve_meta raises MetadataUnavailable when a source cannot answer
        return f"error:{type(e).__name__}", isinstance(e, _metadata_unavailable())
    meta = meta or {}
    changed = False
    if not (row.get("title") or "").strip() and (meta.get("title") or "").strip():
        row["title"] = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", meta["title"])).strip()
        changed = True
    if not (row.get("authors") or "").strip():
        a = _format_authors(meta.get("authors"))
        if a:
            row["authors"] = a
            changed = True
    if not (row.get("year") or "").strip() and meta.get("year"):
        row["year"] = str(meta["year"])
        changed = True
    return ("filled" if changed else "unavailable"), False


def _held_elsewhere(holdings, doi, lib_dir):
    """PDF paths holding `doi` outside `lib_dir` (this queue's own library), from a HoldMap. A copy
    held elsewhere only as a text-only sidecar does not count: the PDF is still worth fetching."""
    if holdings is None:
        return []
    try:
        paths = [h.path for h in holdings.records(doi) if h.has_pdf]
    except Exception as e:
        print(f"  [hold-check] lookup failed for {doi} ({type(e).__name__}); fetching it")
        return []
    lib = Path(lib_dir).resolve()
    out = []
    for p in paths:
        pp = Path(p)
        try:
            rp = pp.resolve()
        except OSError:
            rp = pp
        if rp == lib or lib in rp.parents:
            continue
        out.append(str(pp))
    return out


def _load_holdings(registry):
    """(HoldMap, None) from litpipe.holdings.build, or (None, reason) when it is unavailable."""
    try:
        from litpipe import holdings as _holdings
    except ImportError as e:
        return None, f"litpipe.holdings is not available ({type(e).__name__})"
    try:
        return _holdings.build(registry), None
    except Exception as e:
        return None, f"holdings build failed ({type(e).__name__})"


NO_METADATA_UNAVAILABLE = "metadata unavailable"   # a source could not answer, twice in the run


def _no_metadata_reason(res):
    """The NO_METADATA reason for a fill result: a genuine not-found reads apart from an error."""
    return "blank title; metadata not found" if res == "unavailable" else f"blank title; metadata {res}"


def prepare_rows(norm_csv, lib_dir, holdings=None):
    """Split the normalized queue into rows to fetch and rows settled before any fetch
    (INVALID_DOI, HELD_ELSEWHERE, NO_METADATA), fill blank metadata, and rewrite `norm_csv` with
    only the rows to fetch (DOIs normalized). A blank-title row whose metadata source could not
    answer (ris_emit.MetadataUnavailable) is asked once more after the other rows; if it still
    cannot, the row is settled with the reason "metadata unavailable" (TRANSIENT until its third run,
    then NO_METADATA; decided where attempts are known), apart from a genuine not-found ("metadata
    not found", NO_METADATA at once). Returns (fields, to_fetch, settled, meta_counts)."""
    fields, rows = read_queue(norm_csv)
    if "citation_count" not in fields:
        fields.append("citation_count")
    meta = Counter()
    slots = []      # [row, ("fetch",) | ("settled", class, reason, held) | ("retry",)] in queue order
    for r in rows:
        raw = (r.get("doi") or "").strip()
        doi = None if raw.upper().startswith("NO_DOI") else _doi.normalise(raw)
        if not doi or not lit_util.is_valid_doi(doi):
            slots.append([r, ("settled", "INVALID_DOI", f"invalid or placeholder DOI {raw!r}", "")])
            continue
        r["doi"] = doi
        held = _held_elsewhere(holdings, doi, lib_dir)
        if held:
            slots.append([r, ("settled", "HELD_ELSEWHERE", "held in another library", "; ".join(held))])
            continue
        if not (r.get("title") or "").strip() or not (r.get("authors") or "").strip():
            res, unavailable = _fill(r, doi)
            if not (r.get("title") or "").strip():
                if unavailable:
                    slots.append([r, ("retry",)])
                    continue
                meta[res] += 1
                slots.append([r, ("settled", "NO_METADATA", _no_metadata_reason(res), "")])
                continue
            meta[res] += 1
        slots.append([r, ("fetch",)])
    for slot in slots:   # one more ask in the run for a source that could not answer
        r, state = slot
        if state[0] != "retry":
            continue
        meta["retried"] += 1
        res, unavailable = _fill(r, r["doi"])
        meta[res] += 1
        if (r.get("title") or "").strip():
            slot[1] = ("fetch",)
        else:
            reason = NO_METADATA_UNAVAILABLE if unavailable else _no_metadata_reason(res)
            slot[1] = ("settled", "NO_METADATA", reason, "")
    to_fetch, settled = [], []
    for r, state in slots:
        if state[0] == "settled":
            settled.append((r, *state[1:]))
            continue
        if not (r.get("citation_count") or "").strip():
            r["citation_count"] = "0"
        to_fetch.append(r)
    _write_csv(norm_csv, to_fetch, fields)
    return fields, to_fetch, settled, meta


# ---------------------------------------------------------------- stage verdicts and classes
# The typed report columns (W2b contract): a reader takes whichever exist. `outcome` is a Kind name
# (the row's FIRST root cause); `detail` is exactly `manual_preprint` or `source_excluded:<source>`
# for SKIPPED; `identity` is OK / TITLE_MATCH / FLAG / blank; `release_date` is YYYY-MM-DD for
# EMBARGOED; `landing_url` is the page a person opens for a manual preprint.
TYPED_COLUMNS = ("outcome", "first_status", "route", "detail", "identity", "release_date",
                 "landing_url")
MANUAL_PREPRINT_DETAIL = "manual_preprint"
SOURCE_EXCLUDED_PREFIX = "source_excluded:"
# unpaywall `route` values that are not a download host (a refusal there is the API's, or nothing
# was sent): anything else (publisher, repository, ...) names the host type of a download
_UNPAYWALL_LOOKUP_ROUTES = frozenset({"", "api", "ra", "none", "exists", "dry_run"})
# a W1-era download 404 (the underscore form; the API's "HTTP 404" is NO_MATCH): a dead OA link
_DOWNLOAD_404 = re.compile(r"(?<![A-Za-z0-9])HTTP_404(?!\d)")
_EMBARGO_UNTIL = re.compile(r"EMBARGOED\s+until\s+(\d{4}-\d{2}-\d{2})")


@dataclass(frozen=True)
class Verdict:
    """What one stage's report says about one row: the typed `outcome` column when the report has
    one, else the legacy string mapped by from_legacy."""
    stage: str
    kind: Kind
    raw: str = ""
    downloaded: bool = False
    skip_exists: bool = False
    text_only: bool = False
    download_host: bool = False   # the refusal came from a download route, not a lookup API
    identity: bool = False        # flagged: identity FLAG, a SUPPLEMENT, or a DOI_MISMATCH string
    manual_preprint: bool = False
    typed: bool = False           # read from the typed `outcome` column
    excluded_source: str = ""     # SKIPPED source_excluded:<source>: the project does not enable it
    release_date: str = ""        # EMBARGOED: the release date (the row's not_before), "" unknown
    landing_url: str = ""         # manual_preprint: the page a person opens
    flagged_file: str = ""        # identity: the flagged file's name in the library


def _truthy(v):
    return str(v or "").strip().lower() in ("true", "1", "yes")


def _cell(row, col):
    return str(row.get(col) or "").strip()


def _typed_kind(row):
    """The Kind the typed `outcome` column names; None when it is absent, blank or not a Kind."""
    s = _cell(row, "outcome").upper()
    try:
        return Kind(s) if s else None
    except ValueError:
        return None


def _legacy_kind(raw, stage):
    """from_legacy, plus the W1-era download `HTTP_404` (a dead OA link) as NOT_AVAILABLE: the
    stages write `HTTP_404 [NOT_AVAILABLE]` since W2a, and an old report must route the same way.
    BOILERPLATE and TOO_LARGE stay ERROR (counted, closed on the third run)."""
    kind = from_legacy(raw, stage)
    if kind is Kind.ERROR and _DOWNLOAD_404.search(raw or ""):
        return Kind.NOT_AVAILABLE
    return kind


def is_flagged(row, legacy_col="error"):
    """True when a stage report row is identity-flagged (W2b contract): identity == FLAG, a
    SUPPLEMENT (unpaywall doc_kind), or a legacy error / status starting DOI_MISMATCH. A flagged
    row never counts as downloaded, whatever its outcome says (pmc writes OK, unpaywall ERROR)."""
    return (_cell(row, "identity").upper() == "FLAG" or _cell(row, "doc_kind").upper() == "SUPPLEMENT"
            or _cell(row, legacy_col).startswith("DOI_MISMATCH"))


def _verdict(stage, row, raw, *, legacy_col, flagged, download_host, file_col, text_only=False):
    """A not-fetched row's verdict: the typed outcome when present (a SKIPPED whose detail is
    neither manual_preprint nor source_excluded reads through the legacy columns), else
    from_legacy. A flagged verdict is never OK; a typed OK without a file is not a success."""
    kind, detail = _typed_kind(row), _cell(row, "detail")
    if kind is Kind.SKIPPED and not (detail == MANUAL_PREPRINT_DETAIL
                                     or detail.startswith(SOURCE_EXCLUDED_PREFIX)):
        kind = None
    typed = kind is not None
    if not typed:
        kind = _legacy_kind(raw, stage)
        if SOURCE_EXCLUDED_PREFIX in raw.lower():   # no legacy token names it; the detail text does
            kind = Kind.SKIPPED
    excluded = ""
    if kind is Kind.SKIPPED:
        src = (detail if typed else raw).lower()
        if SOURCE_EXCLUDED_PREFIX in src:
            excluded = (src.split(SOURCE_EXCLUDED_PREFIX, 1)[1].split() or ["unknown"])[0].strip(";,") \
                or "unknown"
    manual = kind is Kind.SKIPPED and not excluded and (
        detail == MANUAL_PREPRINT_DETAIL if typed else "MANUAL_PREPRINT" in raw)
    if kind is Kind.OK:
        # success comes from `downloaded` or an already-held file (the callers checked both); an OK
        # left here is a flagged file (pmc writes OK beside identity FLAG) or a report with no file
        kind = Kind.ERROR
        if not flagged:
            raw = f"{raw} (outcome OK but no file)"
    release = ""
    if kind is Kind.EMBARGOED:
        m = _EMBARGO_UNTIL.search(raw)
        release = _cell(row, "release_date")[:10] or (m.group(1) if m else "")
    return Verdict(stage, kind, raw, text_only=text_only and not flagged,
                   download_host=download_host, identity=flagged, manual_preprint=manual,
                   typed=typed, excluded_source=excluded, release_date=release,
                   landing_url=_cell(row, "landing_url"),
                   flagged_file=_cell(row, file_col) if flagged else "")


def _raw(row, legacy_col):
    """The text a verdict shows: the legacy string, else the typed outcome and its detail."""
    s = _cell(row, legacy_col)
    if s:
        return s
    kind, detail = _cell(row, "outcome"), _cell(row, "detail")
    return (f"{kind}: {detail}" if detail else kind) if kind else "UNKNOWN"


def unpaywall_verdict(u):
    if u is None:
        return None
    flagged = is_flagged(u, "error")
    oa = _cell(u, "oa_status")
    if not flagged and _truthy(u.get("downloaded")):
        return Verdict("unpaywall", Kind.OK, "downloaded", downloaded=True)
    if not flagged and (oa == "SKIP_EXISTS" or _truthy(u.get("skipped"))):
        return Verdict("unpaywall", Kind.OK, "SKIP_EXISTS", skip_exists=True)
    err = _cell(u, "error")
    raw = err or ("OA_NO_URL" if oa == "OA" else oa) or _raw(u, "error")
    route = _cell(u, "route").lower()
    # a download host refused us: the record was OA and (typed reports) the route is a host type
    download_host = oa == "OA" and ("route" not in u or route not in _UNPAYWALL_LOOKUP_ROUTES)
    return _verdict("unpaywall", u, raw, legacy_col="error", flagged=flagged,
                    download_host=download_host, file_col="filename")


def _pmc_text_only(p):
    """TEXT_ONLY, exactly (W2b): a text sidecar written or already there, no PDF downloaded, no
    PDF judged (identity blank), and, where pmc_class is present, an author manuscript or a PDF that
    failed for good (outcome NOT_AVAILABLE / NO_MATCH / NOT_AT_RA): an OA- or NONE-class row whose
    text is held is not an ILL request, while a transient PDF failure still retries (verifier E,
    W2-G open question 1)."""
    klass = _cell(p, "pmc_class").upper()
    terminal_pdf = _cell(p, "outcome").upper() in ("NOT_AVAILABLE", "NO_MATCH", "NOT_AT_RA")
    return (_truthy(p.get("sidecar")) and _cell(p, "sidecar_status").upper() in ("OK", "EXISTS")
            and not _truthy(p.get("downloaded")) and not _cell(p, "identity")
            and (not klass or klass.startswith("AM") or terminal_pdf))


def _preprint_text_only(r):
    """TEXT_ONLY for the preprint stage (W2-C writes Europe PMC preprint full text as a text-only
    `<stem>_preprint.fulltext.json`, outcome NOT_AVAILABLE, sidecar True): the same rule as pmc's
    without the class test (integration of W2-C and W2-G, 2026-10-05)."""
    return (_truthy(r.get("sidecar")) and _cell(r, "sidecar_status").upper() in ("OK", "EXISTS")
            and not _truthy(r.get("downloaded")) and not _cell(r, "identity"))


def pmc_verdict(p):
    if p is None:
        return None
    flagged = is_flagged(p, "error")
    if not flagged and _truthy(p.get("downloaded")):
        return Verdict("pmc", Kind.OK, "downloaded", downloaded=True)
    if not flagged and (_truthy(p.get("skipped")) or _cell(p, "winning_source") == "ALREADY_EXISTS"):
        return Verdict("pmc", Kind.OK, "ALREADY_EXISTS", skip_exists=True)
    return _verdict("pmc", p, _raw(p, "error"), legacy_col="error", flagged=flagged,
                    download_host=bool(_cell(p, "pmcid")), file_col="filename",
                    text_only=_pmc_text_only(p))


def preprint_verdict(r):
    if r is None:
        return None
    flagged = is_flagged(r, "status")
    st = _cell(r, "status")
    if not flagged and _truthy(r.get("downloaded")):
        return Verdict("preprint", Kind.OK, "downloaded", downloaded=True)
    if not flagged and (_truthy(r.get("skipped")) or st == "ALREADY_EXISTS"):
        return Verdict("preprint", Kind.OK, "ALREADY_EXISTS", skip_exists=True)
    return _verdict("preprint", r, _raw(r, "status"), legacy_col="status", flagged=flagged,
                    download_host=_truthy(r.get("found")),
                    file_col="preprint_filename" if _cell(r, "preprint_filename") else "filename",
                    text_only=_preprint_text_only(r))


def fetched_by(v):
    """True for a verdict that fetched the row: OK and not identity-flagged."""
    return v is not None and v.kind is Kind.OK and not v.identity


_TERMINAL_KINDS = frozenset({Kind.NO_MATCH, Kind.NOT_AVAILABLE, Kind.NOT_AT_RA})
_TRANSIENT_KINDS = frozenset({Kind.OUTAGE, Kind.TRANSPORT, Kind.DEFERRED})
ERROR_RUNS_TO_TERMINAL = 3
RESIDUAL_CLASSES = ("HELD_ELSEWHERE", "TEXT_ONLY", "OA_BLOCKED", "TRANSIENT", "IDENTITY_FLAG",
                    "SKIPPED_SOURCE", "NO_METADATA", "TERMINAL_CLOSED", "INVALID_DOI", "CONFIG",
                    "PENDING")
REPORT_CLASSES = ("fetched",) + RESIDUAL_CLASSES


def _enabled(verdicts):
    """The verdicts of enabled stages: None dropped, and a SKIPPED source_excluded too (the
    project does not enable that source, so it is not an enabled stage's answer)."""
    return [v for v in verdicts if v is not None and not v.excluded_source]


def _embargo(vs):
    """(stage, release date or "") of the embargo a row waits on: the earliest dated one."""
    emb = [v for v in vs if v.kind is Kind.EMBARGOED]
    dated = sorted((v.release_date, v.stage) for v in emb if v.release_date)
    return (dated[0][1], dated[0][0]) if dated else (emb[0].stage, "")


def classify(verdicts, attempts=1):
    """The residual class of a row from its enabled stages' verdicts (dispatch 0.5 table, top
    to bottom, first match wins). HELD_ELSEWHERE, NO_METADATA and INVALID_DOI are settled before
    any fetch (prepare_rows). A SKIPPED source_excluded verdict is dropped first: the row is
    SKIPPED_SOURCE only when no enabled stage's verdict remains. An identity-flagged verdict never
    counts as OK or TEXT_ONLY. `attempts` counts sweeps of this row including this one; an
    ERROR row turns TERMINAL_CLOSED on its third. Returns (class, reason)."""
    vs = _enabled(verdicts)
    if any(fetched_by(v) for v in vs):
        return "fetched", ""
    if any(v.text_only and not v.identity for v in vs):
        return "TEXT_ONLY", "text sidecar, no PDF"
    if any(v.kind is Kind.EMBARGOED for v in vs):
        stage, until = _embargo(vs)
        return "TRANSIENT", (f"{stage}: EMBARGOED until {until}" if until
                             else f"{stage}: EMBARGOED (release date unknown)")
    blocked = [v for v in vs if (v.kind is Kind.REFUSED and v.download_host) or v.manual_preprint]
    if blocked:
        return "OA_BLOCKED", f"{blocked[0].stage}: {blocked[0].raw}"
    refused = [v for v in vs if v.kind is Kind.REFUSED]
    if refused:
        return "TRANSIENT", f"{refused[0].stage} refused: {refused[0].raw}"
    transient = [v for v in vs if v.kind in _TRANSIENT_KINDS]
    if transient:
        return "TRANSIENT", f"{transient[0].stage}: {transient[0].raw}"
    flagged = [v for v in vs if v.identity]
    if flagged:
        return "IDENTITY_FLAG", f"{flagged[0].stage}: {flagged[0].raw}"
    if not vs:
        return "SKIPPED_SOURCE", "no enabled stage covers this row"
    if any(v.kind is Kind.CONFIG for v in vs):
        return "CONFIG", "configuration refused by a source"
    if all(v.kind in _TERMINAL_KINDS for v in vs):
        return "TERMINAL_CLOSED", "; ".join(f"{v.stage}: {v.raw}" for v in vs)
    err = next((v for v in vs if v.kind not in _TERMINAL_KINDS), vs[0])
    if attempts >= ERROR_RUNS_TO_TERMINAL:
        return "TERMINAL_CLOSED", f"error in {attempts} runs, last {err.stage}: {err.raw}"
    return "TRANSIENT", f"error (run {attempts} of {ERROR_RUNS_TO_TERMINAL}) {err.stage}: {err.raw}"


RESIDUAL_EXTRA_FIELDS = ("not_before", "flagged_path", "landing_url", "best_oa_url")


def residual_extras(verdicts, cls, lib_dir=None, best_oa_url=""):
    """The residual CSV's per-row extras for a classified row (W2-G; best_oa_url W5-C1):
    not_before    an embargo's release date when the row is TRANSIENT on an embargo ("" when the
                  date is unknown: migrate applies its default delay)
    flagged_path  the library path of every identity-flagged file, "; "-joined (the file stays put;
                  its evidence is <stem>.identity.json, or pmc's <stem>.fulltext.json identity_*)
    landing_url   the page a person opens for a manual preprint
    best_oa_url   the Unpaywall report's `best_oa_url` for the row (the URL of the same response's
                  best_oa_location), copied as it is; "" when the report has no such column"""
    vs = _enabled(verdicts)
    out = dict.fromkeys(RESIDUAL_EXTRA_FIELDS, "")
    out["best_oa_url"] = str(best_oa_url or "").strip()
    if cls == "TRANSIENT" and any(v.kind is Kind.EMBARGOED for v in vs):
        out["not_before"] = _embargo(vs)[1]
    files = [v.flagged_file for v in vs if v.identity and v.flagged_file]
    if files:
        out["flagged_path"] = "; ".join(str(Path(lib_dir) / f) if lib_dir else f
                                        for f in dict.fromkeys(files))
    out["landing_url"] = next((v.landing_url for v in vs if v.manual_preprint and v.landing_url),
                              next((v.landing_url for v in vs if v.landing_url), ""))
    return out


from litpipe.ledger import redact as _ledger_redact   # the one implementation (dispatch 0.5)
from litpipe import doi as _doi


def _redact(text):
    return _ledger_redact(str(text or ""))


def _index(path):
    """{normalized doi: row} of a stage report; {} when it is absent or unreadable."""
    if not path or not Path(path).exists():
        return {}
    try:
        _, rows = read_queue(path)
    except (OSError, UnicodeDecodeError, csv.Error):
        return {}
    return {lit_util.normalize_doi(r.get("doi")): r for r in rows if r.get("doi")}


def _count(index, pred):
    return sum(1 for r in index.values() if pred(r))


# ---------------------------------------------------------------- one queue
_STAGE_ORDER = ("unpaywall", "pmc", "preprint")
_STAGE_LABEL = {"unpaywall": "unpaywall_v2", "pmc": "pmc_fetch", "preprint": "preprint_fetch",
                "extract": "pdf_extract"}
def _stage_env():
    """The stage subprocess environment: PYTHONUNBUFFERED=1 so a stage's progress is not
    block-buffered behind a pipe (I13: a stalled preprint stage showed nothing for hours)."""
    return {**os.environ, "PYTHONUNBUFFERED": "1"}


def _run_stage(cmd):
    print(f"  -> {' '.join(cmd)}")
    r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace",
                       env=_stage_env())
    print(r.stdout[-1500:] if r.stdout else "")
    return r


def _artifact_dir(project_dir, artifact_dir):
    if not artifact_dir:
        return Path(project_dir)
    p = Path(artifact_dir).expanduser()
    return p if p.is_absolute() else Path(project_dir) / p


def project_artifact_dir(key, project_dir, artifact_dir=None, cfg=None):
    """Where a project's run artifacts go: --artifact-dir for one run (relative: under the project),
    else projects.json `artifact_dir` (litpipe.config.artifact_dir: per project, else global; unset:
    the project root, as before), else the project root (an unregistered --project dir)."""
    if artifact_dir:
        return _artifact_dir(project_dir, artifact_dir)
    if key is not None and key in ((cfg or {}).get("projects") or {}):
        from litpipe import config as lp_config
        return Path(lp_config.artifact_dir(key, cfg=cfg))
    return Path(project_dir)


PMC_INPUT_FIELDS = ("doi", "title", "authors", "year", "filename", "oa_status", "downloaded")


def build_pmc_input(rows, lib_dir, out_csv):
    """PMC's --report-in when the project's sources omit unpaywall (DEC-31). pmc_fetch takes two things
    from the Unpaywall report: each row's DEC-14 filename, and the own-library check (it skips rows
    whose oa_status is SKIP_EXISTS). So each queue row gets the filename unpaywall_fetch_v2.build_filename
    gives it, and SKIP_EXISTS when the library already holds the paper: the stage's library index
    (a `.ris` or identity sidecar naming the DOI, when the stage has one), else a file at that name
    (or the legacy name) that unpaywall_fetch_v2.existing_holds judges "same". Writes `out_csv` and
    returns the normalised DOIs held."""
    import unpaywall_fetch_v2 as U
    lib_dir = Path(lib_dir)
    try:
        existing = set(os.listdir(lib_dir))
    except OSError:
        existing = set()
    index_fn = getattr(U, "library_index", None)
    try:
        index = index_fn(str(lib_dir)) if index_fn is not None and existing else {}
    except Exception:   # noqa: BLE001 - the index is a shortcut; the per-file check still runs
        index = {}
    legacy = getattr(U, "legacy_build_filename", None)
    out, held = [], set()
    for r in rows:
        doi = (r.get("doi") or "").strip()
        title, authors, year = r.get("title") or "", r.get("authors") or "", r.get("year") or ""
        fn = U.build_filename(year, authors, title)
        hit = index.get(_doi.normalise(doi) or doi)
        if not hit:
            names = [fn] + ([legacy(year, authors, title)] if legacy is not None else [])
            for name in dict.fromkeys(names):
                if name in existing and U.existing_holds(str(lib_dir / name), doi, title) == "same":
                    hit = name
                    break
        if hit:
            held.add(lit_util.normalize_doi(doi))
        out.append({"doi": doi, "title": title, "authors": authors, "year": year, "filename": hit or fn,
                    "oa_status": "SKIP_EXISTS" if hit else "", "downloaded": "False"})
    _write_csv(out_csv, out, PMC_INPUT_FIELDS)
    return held


def _refuse(msg):
    print(f"  ERR {msg}", file=sys.stderr)
    return None


def _stage_status(r, report):
    """completed (exit 0 and a report), config (exit 2: a stage's CONFIG abort, or a usage error
    in the command sweep built), else failed."""
    if r.returncode == 0 and Path(report).exists():
        return "completed"
    return "config" if r.returncode == EXIT_USAGE else "failed"


def is_arxiv_doi(doi):
    return str(doi or "").strip().lower().startswith(ARXIV_DOI_PREFIX)


def run_pipeline(project_dir, queue_csv, dry_run=False, run_date=None, skip_preprint=False, *,
                 key=None, registry=None, run_id=None, holdings=None, artifact_dir=None,
                 allow_destination=False, skip_reason=None, preprint_arxiv_only=False,
                 history=None, candidate_order=None, sources=None, stop=None):
    """Run unpaywall_v2 -> pmc_fetch -> preprint_fetch -> pdf extract against one queue and
    classify every row. Returns a result dict, or None when the queue is refused before any
    fetch (no destination, a destination that escapes the project or, given `key` and
    `registry`, is not the registry library for `key`, unless `allow_destination`).

    `preprint_arxiv_only` (DEC-31: the project's sources name no preprint server) sends only the
    10.48550/ rows to the preprint stage, and skips it when there are none. `sources` (DEC-31, the
    project's sources set; None: unpaywall and pmc both run, as before) skips the Unpaywall or PMC
    stage the set omits, as --skip-preprint skips the preprint stage: the stage reads "skipped (the
    project's sources omit <stage>)" in the report and is listed in the residual's skipped_sources;
    without Unpaywall, the own-library check (build_pmc_input) runs for every source set, PMC's
    input when PMC runs, and held rows skip every later stage. `stop` (a callable)
    is asked before each stage: True (the project's lock was lost) stops the queue as an abort, its
    later stages not_run and the queue kept. `history` is {doi: attempts} from retry_later before
    admission (default: read it now), so a DOI re-queued fresh keeps its count."""
    project_dir = Path(project_dir)
    queue_csv = Path(queue_csv)
    tag = queue_tag(queue_csv.name) or ""
    dest_rel = first_destination(queue_csv)
    if not dest_rel:
        return _refuse(f"no `destination` column in {queue_csv}")
    project_root = project_dir.resolve()
    lib_dir = (project_dir / dest_rel).resolve()
    try:
        lib_dir.relative_to(project_root)
    except ValueError:
        return _refuse(f"destination escapes project root: {dest_rel!r} in {queue_csv}")
    if key is not None and registry is not None and key in registry and not allow_destination:
        entry = registry.get(key) or {}
        if entry.get("lib_dir"):
            expected = lit_util.lib_paths(key, entry)[1].resolve()
            if lib_dir != expected:
                # REG-I07 backstop: a subproject destination with the tail doubled lands a shadow
                # library the index never reads.
                return _refuse(f"destination {dest_rel!r} in {queue_csv.name} resolves to {lib_dir}, "
                               f"not the registry library {expected}; pass --allow-destination "
                               f"to sweep it anyway")
    today = run_date or datetime.date.today().isoformat()
    art_dir = _artifact_dir(project_dir, artifact_dir)
    if run_id is None:
        run_id = choose_run_id(run_id_dirs(project_dir, art_dir), today)
    names = {s: art_dir / artifact_name(tag, run_id, s) for s in ARTIFACT_STAGES}

    if dry_run:
        # A dry run must not touch the filesystem: mkdir used to fire before the dry-run guard,
        # so `--dry-run` on a queue with a wrong `destination` created the very shadow library
        # the dry run was meant to warn about (2026-08-19, nested subprojects). It also makes no
        # network call (no metadata fill, no hold map).
        _, rows = read_queue(queue_csv)
        bad = sum(1 for r in rows
                  if (r.get("doi") or "").strip().upper().startswith("NO_DOI")
                  or _doi.normalise((r.get("doi") or "").strip()) is None)
        blank = sum(1 for r in rows if not (r.get("title") or "").strip()
                    or not (r.get("authors") or "").strip())
        print(f"  DRY would process {len(rows)} rows -> {lib_dir} (run {run_id}; "
              f"{bad} invalid DOI, {blank} with blank title or authors)")
        return {"dry": True, "rows": len(rows), "destination": str(lib_dir), "run_id": run_id,
                "tag": tag, "queue": queue_csv.name, "invalid_doi": bad, "needs_metadata": blank}

    lib_dir.mkdir(parents=True, exist_ok=True)
    art_dir.mkdir(parents=True, exist_ok=True)
    norm_csv = names["normalized"]
    normalize_queue_for_pipeline(queue_csv, norm_csv)
    fields, to_fetch, settled, meta_counts = prepare_rows(norm_csv, lib_dir, holdings)
    n_rows = len(to_fetch) + len(settled)

    py = sys.executable
    status = {"unpaywall": "not_run", "pmc": "not_run", "preprint": "not_run", "extract": "not_run"}
    stage_note = {}
    report_unpw, report_pmc, report_ppr = names["unpaywall"], names["pmc"], names["preprint"]
    residual_csv, summary_csv = names["residual"], names["report"]
    preprint_input, gated = [], set()   # gated: rows the DEC-31 gate kept from the preprint stage
    src = None if sources is None else {str(s).strip().lower() for s in sources}
    pre_held = set()                    # DOIs the library holds already (sweep's own check, no Unpaywall)
    lock_lost = False

    def _stopped(stage):
        """True when `stop` says the project's lock was lost: this stage and the later ones do not run."""
        nonlocal lock_lost
        if lock_lost or (stop is not None and stop()):
            lock_lost = True
            stage_note.setdefault(stage, "project lock lost: not run")
            return True
        return False

    # DEC-31: a stage whose source the project's sources omit is skipped, as --skip-preprint is
    for s in ("unpaywall", "pmc"):
        if src is not None and s not in src:
            status[s] = "skipped"
            stage_note[s] = f"the project's sources omit {s}"
    if status["unpaywall"] == "skipped":
        print(f"  [SKIP] unpaywall stage skipped ({stage_note['unpaywall']})")
    if status["pmc"] == "skipped":
        print(f"  [SKIP] pmc stage skipped ({stage_note['pmc']})")

    # Stage 1: Unpaywall (with no rows it writes an empty report).
    # DEC-11: --candidate-order only when given (the stage's own default is repository).
    if status["unpaywall"] != "skipped" and not _stopped("unpaywall"):
        r1 = _run_stage([py, str(HERE / "unpaywall_fetch_v2.py"),
                         "--top-n", str(len(to_fetch) + 5),
                         "--triage", str(norm_csv),
                         "--lib-dir", str(lib_dir),
                         "--report", str(report_unpw),
                         "--base-dir", str(project_dir)]
                        + (["--candidate-order", candidate_order] if candidate_order else []))
        status["unpaywall"] = _stage_status(r1, report_unpw)
        if status["unpaywall"] != "completed":
            print(f"  ERR unpaywall stage {status['unpaywall']} (exit {r1.returncode}, report "
                  f"{'present' if report_unpw.exists() else 'MISSING'}); PMC and preprint cannot run:\n"
                  f"{(r1.stderr or '')[-500:]}")
    # the later stages run when Unpaywall completed or the project's sources skip it
    upstream_ok = status["unpaywall"] in ("completed", "skipped")

    if status["unpaywall"] == "skipped" and not lock_lost:
        # Without Unpaywall, sweep's own-library check (each row's DEC-14 filename, existing_holds)
        # stands in for its SKIP_EXISTS for EVERY later stage: PMC reads it as its input, and the
        # preprint stage (whose own check knows only _preprint names) never fetches, nor routes to
        # ILL, a paper the library holds
        pre_held = build_pmc_input(to_fetch, lib_dir, names[PMC_INPUT_STAGE])
    if upstream_ok and status["pmc"] != "skipped" and not _stopped("pmc"):
        # Stage 2: PMC (reads the unpaywall report to find the rows still missing; without
        # Unpaywall, an input built from the queue with the same filename and own-library check)
        pmc_in = names[PMC_INPUT_STAGE] if status["unpaywall"] == "skipped" else report_unpw
        r2 = _run_stage([py, str(HERE / "pmc_fetch.py"),
                         "--report-in", str(pmc_in),
                         "--lib-dir", str(lib_dir),
                         "--report-out", str(report_pmc),
                         "--base-dir", str(project_dir)])
        status["pmc"] = _stage_status(r2, report_pmc)
        if status["pmc"] != "completed":
            # T3 (2026-06-25): a missing report must NOT read as 'PMC found nothing'.
            print(f"  [WARN] PMC stage did NOT complete ({status['pmc']}, exit {r2.returncode}, "
                  f"report={'present' if report_pmc.exists() else 'MISSING'}); the queue stays "
                  f"for a re-sweep." + (f"\n{r2.stderr[-400:]}" if r2.stderr else ""))

    if upstream_ok and not lock_lost:
        # Stage 3: preprint_fetch for the rows neither Unpaywall nor PMC got.
        # T5b (2026-06-25 audit): an already-present paper reports oa_status=SKIP_EXISTS
        # (unpaywall) or skipped/winning_source=ALREADY_EXISTS (pmc) with downloaded=False; treat
        # those as got, or a _preprint duplicate is fetched. An identity-flagged file is not got.
        got = {d for d, r in _index(report_unpw).items() if fetched_by(unpaywall_verdict(r))}
        got |= {d for d, r in _index(report_pmc).items() if fetched_by(pmc_verdict(r))}
        got |= pre_held
        preprint_input = [r for r in to_fetch if lit_util.normalize_doi(r.get("doi")) not in got]
        if preprint_arxiv_only and not skip_preprint:
            # DEC-31: no preprint server in the project's sources; DEC-09: arXiv DOIs go anyway
            gated = {lit_util.normalize_doi(r.get("doi")) for r in preprint_input
                     if not is_arxiv_doi(r.get("doi"))}
            preprint_input = [r for r in preprint_input if is_arxiv_doi(r.get("doi"))]
            if preprint_input:
                stage_note["preprint"] = (f"{len(preprint_input)} {ARXIV_DOI_PREFIX} row(s) only; the "
                                          f"project's sources name no preprint server")
        if skip_preprint or (preprint_arxiv_only and not preprint_input):
            status["preprint"] = "skipped"
            n_left = len(preprint_input) + len(gated)
            why = skip_reason or ("--skip-preprint" if skip_preprint else
                                  "the project's sources name no preprint server")
            print(f"  [SKIP] preprint stage skipped ({why}); "
                  f"{n_left} row(s) classified on Unpaywall and PMC alone")
            gated = set()
        elif preprint_input and status["pmc"] == "config":
            # the run aborts at PMC's CONFIG: the stage is not run, and its rows are PENDING
            print(f"  [SKIP] preprint stage not run: the run aborts (PMC exited {EXIT_USAGE}, CONFIG)")
        elif preprint_input and _stopped("preprint"):
            pass                                  # not_run: the queue stays (project lock lost)
        elif preprint_input:
            _write_csv(residual_csv, preprint_input, fields)
            cmd = [py, str(HERE / "preprint_fetch.py"),
                   "--triage", str(residual_csv),
                   "--lib-dir", str(lib_dir),
                   "--report", str(report_ppr)]
            if key and (registry is None or key in registry):
                cmd += ["--project", key]   # the stage selects its servers from the project's sources
            r3 = _run_stage(cmd)
            status["preprint"] = _stage_status(r3, report_ppr)
            if status["preprint"] != "completed":
                print(f"  [WARN] preprint stage did NOT complete ({status['preprint']}, exit "
                      f"{r3.returncode}, report={'present' if report_ppr.exists() else 'MISSING'})."
                      + (f"\n{r3.stderr[-400:]}" if r3.stderr else ""))
        else:
            status["preprint"] = "not_needed"
            gated = set()

    if upstream_ok and "config" not in status.values() and not _stopped("extract"):
        # Stage 4: PDF text extraction for PDFs in lib_dir without a .fulltext.json sidecar.
        # Indexing only and idempotent, so a failure does not keep the queue; it does set the
        # exit code. A CONFIG stage aborts the run, so it does not run then.
        r4 = _run_stage([py, str(HERE / "extract_pdf_fulltext.py"), "--lib-dir", str(lib_dir)])
        status["extract"] = "completed" if r4.returncode == 0 else "failed"
        if r4.returncode != 0:
            print(f"  WARN pdf-extract stage failed (continuing):\n{(r4.stderr or '')[-500:]}")

    # ---- classify every row
    u_idx, p_idx, r_idx = _index(report_unpw), _index(report_pmc), _index(report_ppr)
    # the rows the preprint stage was given, or would have been (not_run: the run aborted first)
    ppr_dois = ({lit_util.normalize_doi(r.get("doi")) for r in preprint_input}
                if status["preprint"] in ("completed", "failed", "config", "not_run") else set())
    skipped_sources = [s for s in _STAGE_ORDER if status[s] == "skipped"]
    history = retry_history(project_dir) if history is None else history
    classes = Counter()
    config_rows = 0   # rows with any CONFIG verdict: the run aborts whatever class the row gets
    residual = []
    res_fields = _union(fields, ["residual_class", "reason", "stages", "held_at",
                                 "skipped_sources", "attempts", "run_id"],
                        RESIDUAL_EXTRA_FIELDS)

    def _attempts(r):
        """Sweeps of this row including this one: the row's own count (a re-admitted retry row
        carries it) or retry_later's for its DOI (a fresh re-queue), whichever is higher."""
        return max(lit_util.coerce_int(r.get("attempts")), history.get(_dkey(r.get("doi")), 0)) + 1

    def _residual_row(r, cls, reason, stages="", held="", skipped=(), extras=None):
        out = dict(r)
        out.update(residual_class=cls, reason=_redact(reason), stages=_redact(stages),
                   held_at=held, skipped_sources=";".join(skipped_sources + [
                       s for s in skipped if s not in skipped_sources]),
                   attempts=str(_attempts(r)), run_id=run_id)
        # not_before, flagged_path, landing_url: always written, so a stale not_before carried
        # in by a re-admitted retry row is not mistaken for this run's
        out.update({k: _redact(v) for k, v in (extras or dict.fromkeys(RESIDUAL_EXTRA_FIELDS, "")).items()})
        residual.append(out)

    for r, cls, reason, held in settled:
        if cls == "NO_METADATA" and reason == NO_METADATA_UNAVAILABLE                 and _attempts(r) < ERROR_RUNS_TO_TERMINAL:
            # a source that could not answer is not a "no" (verifier E, W2-G open question 2): the
            # row is retried via retry_later and closes as NO_METADATA only on its third run
            cls = "TRANSIENT"
        classes[cls] += 1
        _residual_row(r, cls, reason, held=held)
    for r in to_fetch:
        d = lit_util.normalize_doi(r.get("doi"))
        verdicts, missing = [], []
        u_row = u_idx.get(d)
        uv = unpaywall_verdict(u_row)
        # pmc_fetch reads every unpaywall row not downloaded and not SKIP_EXISTS (its own rule)
        expected = {"unpaywall": True,
                    "pmc": u_row is None or not (_truthy(u_row.get("downloaded"))
                                                 or _cell(u_row, "oa_status") == "SKIP_EXISTS"),
                    "preprint": d in ppr_dois}
        vfun = {"unpaywall": lambda: uv, "pmc": lambda: pmc_verdict(p_idx.get(d)),
                "preprint": lambda: preprint_verdict(r_idx.get(d))}
        if d in pre_held:   # sources without unpaywall: sweep's own-library check found it held
            verdicts.append(Verdict("pmc", Kind.OK, "SKIP_EXISTS", skip_exists=True))
        for s in _STAGE_ORDER:
            if d in pre_held or status[s] in ("skipped", "not_needed") or not expected[s]:
                continue
            v = vfun[s]()
            if v is None:
                missing.append(f"{s} {status[s]}" if status[s] != "completed" else f"no {s} report row")
            else:
                verdicts.append(v)
        stages_txt = "; ".join(f"{v.stage}={v.raw}" for v in verdicts)
        config_rows += any(v.kind is Kind.CONFIG for v in verdicts)
        if any(fetched_by(v) for v in verdicts):
            cls, reason = "fetched", ""
        elif missing:
            cls, reason = "PENDING", "; ".join(missing)
        else:
            cls, reason = classify(verdicts, attempts=_attempts(r))
        classes[cls] += 1
        if cls != "fetched":
            skipped = (["preprint"] if d in gated else []) + [
                v.stage for v in verdicts if v.excluded_source]
            _residual_row(r, cls, reason, stages_txt, skipped=skipped,
                          extras=residual_extras(verdicts, cls, lib_dir,
                                                 best_oa_url=_cell(u_row, "best_oa_url") if u_row else ""))
    _write_csv(residual_csv, residual, res_fields)

    # ---- counts and the report (a flagged file is never a download, whatever its row says)
    verdict_of = {"unpaywall": unpaywall_verdict, "pmc": pmc_verdict, "preprint": preprint_verdict}

    def _n(index, stage, field):
        return _count(index, lambda r: getattr(verdict_of[stage](r), field))

    n_unpw, n_pmc, n_ppr = (_n(u_idx, "unpaywall", "downloaded"), _n(p_idx, "pmc", "downloaded"),
                            _n(r_idx, "preprint", "downloaded"))
    sk_unpw, sk_pmc, sk_ppr = (_n(u_idx, "unpaywall", "skip_exists"), _n(p_idx, "pmc", "skip_exists"),
                               _n(r_idx, "preprint", "skip_exists"))
    sk_pmc += len(pre_held)   # held before PMC ran (sweep's own-library check, no Unpaywall stage)
    n_total, n_skip = n_unpw + n_pmc + n_ppr, sk_unpw + sk_pmc + sk_ppr

    failed = [s for s in ("unpaywall", "pmc", "preprint", "extract") if status[s] == "failed"]
    blocking = [s for s in _STAGE_ORDER if status[s] in ("failed", "not_run", "config")]
    config_error = config_rows > 0 or "config" in status.values()
    retired = not blocking and not classes["PENDING"] and not config_error and not lock_lost
    if retired:
        keep_reason = ""
    elif lock_lost:
        keep_reason = "project lock lost (another process holds the project): " + (
            ", ".join(f"{s} {status[s]}" for s in blocking) or "stopped before the extract step")
    elif blocking:
        keep_reason = ", ".join(f"{s} {status[s]}" for s in blocking)
    elif config_error:
        keep_reason = "a source refused the configuration (CONFIG)"
    else:
        keep_reason = f"{classes['PENDING']} row(s) unclassified"
    skip_why = skip_reason or ("--skip-preprint" if skip_preprint else
                               "the project's sources name no preprint server")

    def _st(s):
        if status[s] == "skipped":
            note = skip_why if s == "preprint" else stage_note.get(s)
        else:
            note = stage_note.get(s)
        return status[s] + (f" ({note})" if note else "")

    rep = [("run", "run_id", "", run_id), ("run", "queue", "", queue_csv.name),
           ("run", "destination", "", str(lib_dir)),
           ("stage", _STAGE_LABEL["unpaywall"], n_unpw, _st("unpaywall")),
           ("stage", _STAGE_LABEL["pmc"], n_pmc, _st("pmc")),
           ("stage", _STAGE_LABEL["preprint"], n_ppr, _st("preprint")),
           ("stage", _STAGE_LABEL["extract"], "", _st("extract")),
           ("skip_exists", _STAGE_LABEL["unpaywall"], sk_unpw, ""),
           ("skip_exists", _STAGE_LABEL["pmc"], sk_pmc, ""),
           ("skip_exists", _STAGE_LABEL["preprint"], sk_ppr, ""),
           ("total", "rows", n_rows, ""), ("total", "downloaded", n_total, ""),
           ("total", "skip_exists", n_skip, "")]
    rep += [("class", c, classes[c], "") for c in REPORT_CLASSES]
    rep += [("metadata", k, v, "") for k, v in sorted(meta_counts.items())]
    processed = names["processed"]
    rep.append(("queue", "retired" if retired else "kept", "",
                processed.name if retired else keep_reason))
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(["section", "name", "count", "detail"])
    w.writerows(rep)
    lit_util.atomic_write_text(str(summary_csv), buf.getvalue())

    print("  classes: " + ", ".join(f"{c} {classes[c]}" for c in REPORT_CLASSES if classes[c]))
    result = {"rows": n_rows, "downloaded": n_total, "unpaywall": n_unpw, "pmc": n_pmc,
              "preprint": n_ppr, "skip_exists": n_skip, "report": str(summary_csv),
              "residual": str(residual_csv), "processed": None, "partial": not retired,
              "retired": retired, "run_id": run_id, "tag": tag, "queue": queue_csv.name,
              "stages": dict(status), "failed_stages": failed, "classes": dict(classes),
              "config_error": config_error, "keep_reason": keep_reason, "lock_lost": lock_lost,
              "sources": None if src is None else sorted(src)}
    if not retired:
        print(f"  [WARN] {keep_reason}; LEAVING {queue_csv.name} in place for the next sweep "
              f"(NOT renamed to .processed).")
        return result

    queue_csv.rename(processed)
    norm_csv.unlink(missing_ok=True)
    print(f"  queue retired -> {processed.name}")
    result["processed"] = str(processed)
    return result


# ---------------------------------------------------------------- LOOSE_ENDS
def resolve_loose_ends_path():
    """Path of the cross-project lit-pull log, from the gitignored projects.json
    "loose_ends" key (relative to lit_util.PROJECTS_ROOT, or an absolute path).

    Returns None when the key is absent. The log is an OPT-IN feature -- the
    literature-pipeline skill/SOP documents wiring it -- so the code does not invent a
    location; the caller reports the skip and the sweep proceeds. Keeping the path in
    gitignored config (not source) is also what keeps an internal project name out of
    this public repo.
    """
    cfg = lit_util.load_projects_config(CONFIG_PATH, missing_ok=True)
    configured = cfg.get("loose_ends")
    if not configured:
        return None
    p = Path(configured).expanduser()
    return p if p.is_absolute() else (lit_util.PROJECTS_ROOT / p)


def append_loose_end(line):
    """Append `line` to the configured lit-pull log and return the path written, or
    None when no "loose_ends" key is set (nothing written -- the caller reports it)."""
    path = resolve_loose_ends_path()
    if path is None:
        return None
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(line.rstrip() + "\n")
    return path


def _loose_state(line):
    """The part of a lit-pull line that says what state the project is in (not which report)."""
    return line.split(" Report:")[0].strip()


def last_loose_end(key, path=None):
    """The most recent lit-pull line for `key` in the log, or None."""
    path = path or resolve_loose_ends_path()
    if path is None or not Path(path).exists():
        return None
    last = None
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if (s.startswith(LOOSE_DONE) or s.startswith(LOOSE_PARTIAL)) and f" {key}/ " in s:
            last = s
    return last


def write_loose_end(key, line):
    """Append `line` unless the project's last lit-pull line already says the same state
    (dispatch 0.5: never the same line twice in a row). Returns (path or None, why)."""
    path = resolve_loose_ends_path()
    if path is None:
        return None, "unconfigured"
    prev = last_loose_end(key, path)
    if prev is not None and _loose_state(prev) == _loose_state(line):
        return None, "unchanged"
    return append_loose_end(line), "written"


def loose_end_line(key, results, refused):
    """One line per project per run, or None. PARTIAL when a queue stayed (a stage failed, the
    queue was refused, rows unclassified); done only when a queue retired and none stayed."""
    kept = [r for r in results if not r.get("retired")]
    reports = ", ".join(Path(r["report"]).name for r in results)
    if kept or refused:
        why = [f"{r['queue']}: {r['keep_reason']}" for r in kept]
        why += [f"{name}: refused" for name in refused]
        return (f"{LOOSE_PARTIAL} {key}/ — " + "; ".join(why) + ", left for re-sweep."
                + (f" Report: {reports}" if reports else ""))
    if not results:
        return None
    tot = Counter()
    for r in results:
        for k in ("downloaded", "rows", "unpaywall", "pmc", "preprint", "skip_exists"):
            tot[k] += r.get(k, 0)
        for c, n in r["classes"].items():
            tot[c] += n
    skipped_stages = [s for s in _STAGE_ORDER
                      if results and all(r["stages"].get(s) == "skipped" for r in results)]
    tail = [f"{tot[c]} {label}" for c, label in (
        ("TERMINAL_CLOSED", "closed"), ("OA_BLOCKED", "OA-blocked"), ("TRANSIENT", "transient"),
        ("TEXT_ONLY", "text-only"), ("HELD_ELSEWHERE", "held elsewhere"),
        ("IDENTITY_FLAG", "identity-flagged"), ("NO_METADATA", "no metadata"),
        ("INVALID_DOI", "invalid DOI")) if tot[c]]
    if tot["skip_exists"]:
        tail.insert(0, f"{tot['skip_exists']} already held")
    tail += [f"{s} skipped" for s in skipped_stages]
    return (f"{LOOSE_DONE} {key}/ — {tot['downloaded']}/{tot['rows']} fetched (Unpaywall "
            f"{tot['unpaywall']}, PMC {tot['pmc']}, Preprint {tot['preprint']})"
            + (f"; {', '.join(tail)}" if tail else "") + f". Report: {reports}")


# ---------------------------------------------------------------- the run
def project_sources(key, cfg, override=None) -> set:
    """DEC-31: the sources a sweep of `key` uses. `override` (sweep --sources, one run) replaces
    everything; else the project's own `sources` list, else the default unpaywall + pmc (an
    unregistered --project dir gets the default too). Raises litpipe.config.ConfigError on an
    invalid list."""
    from litpipe import config as lp_config
    if override is not None:
        return lp_config.sources(key, override=override, cfg=cfg)
    if key not in ((cfg or {}).get("projects") or {}):
        return set(lp_config.DEFAULT_SOURCES)
    return lp_config.sources(key, cfg=cfg)


def _preprint_excluded(key, cfg, override=None):
    """DEC-31: True when the project's sources (its own `sources` list, else the default
    unpaywall + pmc; `override` for one run) name no preprint server. The preprint stage then runs
    only for rows with a 10.48550/ DOI (DEC-09: arXiv rows reach arXiv whatever the sources), and is
    skipped when there are none. Raises litpipe.config.ConfigError on an invalid `sources` list."""
    return not (project_sources(key, cfg, override) & PREPRINT_SOURCES)


def migrate_command(key, run_id, artifact_dir=None, skip_preprint=False, sources=None):
    cmd = [sys.executable, str(HERE / "migrate_closed_to_md.py"), "--project", key, "--date", run_id]
    if artifact_dir:
        cmd += ["--artifact-dir", str(artifact_dir)]
    if skip_preprint:   # a deliberately skipped stage's missing report must not block routing
        cmd += ["--skip-preprint"]
    if sources is not None:   # the run's sources (a one-run --sources): migrate gates on the same set
        cmd += ["--sources", ",".join(sorted(sources))]
    return cmd


def _lock_note(record):
    return _lockfile.describe(record)


def run(project=None, dry_run=False, skip_preprint=False, date=None, loose_ends=True,
        migrate=False, artifact_dir=None, allow_destination=False, candidate_order=None, sources=None):
    """The whole sweep (dispatch 0.5 stage function). Returns {"exit_code", "projects",
    "ignored", "admitted"}; main() parses argv and calls this. `candidate_order` ("repository" or
    "publisher", DEC-11) goes to the Unpaywall stage only when given. `sources` (a list or a
    comma-separated string) replaces every project's DEC-31 sources for this run.

    Each project with something staged runs under its lock file (litpipe.lockfile,
    lit_pull_queue.lock in the project root): retry_later admission, queue discovery, the stages
    and --migrate. A lock another live process holds skips that project (exit 4's "refused before
    fetching": nothing of it is touched). A dry run takes no lock and writes nothing."""
    empty = {"exit_code": EXIT_USAGE, "projects": {}, "ignored": [], "admitted": {}}
    if candidate_order is not None and candidate_order not in CANDIDATE_ORDERS:
        print(f"ERR --candidate-order must be one of {', '.join(CANDIDATE_ORDERS)}, got {candidate_order!r}",
              file=sys.stderr)
        return empty
    today = date or datetime.date.today().isoformat()
    out = {"exit_code": EXIT_OK, "projects": {}, "ignored": [], "admitted": {}}
    try:
        datetime.date.fromisoformat(today)
        if len(today) != 10:
            raise ValueError
    except ValueError:
        print(f"ERR --date must be YYYY-MM-DD, got {today!r}", file=sys.stderr)
        out["exit_code"] = EXIT_USAGE
        return out
    if artifact_dir and Path(artifact_dir).expanduser().is_absolute() and not project:
        print("ERR an absolute --artifact-dir needs --project (projects would share one "
              "directory and one run id namespace)", file=sys.stderr)
        out["exit_code"] = EXIT_USAGE
        return out
    from litpipe import config as lp_config
    if sources is not None:
        try:
            lp_config.sources(None, override=sources)
        except lp_config.ConfigError as e:
            print(f"ERR --sources: {e}", file=sys.stderr)
            out["exit_code"] = EXIT_USAGE
            return out

    from ris_emit import warn_if_default_email
    warn_if_default_email()

    cfg_full = lit_util.load_projects_config(CONFIG_PATH, missing_ok=True)
    registry = cfg_full.get("projects") or {}
    try:
        stale_s = _lockfile.stale_s_from(cfg_full)
    except lp_config.ConfigError as e:
        print(f"ERR {e}", file=sys.stderr)
        out["exit_code"] = EXIT_USAGE
        return out

    # Read-only look first (no lock, nothing written): which projects have anything staged, and the
    # tag-shaped files that are not queues.
    candidates = []
    due_dry = 0
    ignored = []
    for key, root in _project_roots(project, registry):
        pre_q = discover_queues(root, ignored)
        _, due, _waiting, bad = split_retry_later(root, today) if Path(root).is_dir() else ([], [], [], [])
        if dry_run and (due or bad):
            print(f"  DRY {key}: would admit {len(due)} due retry_later row(s)"
                  + (f"; {len(bad)} with an unreadable not_before" if bad else ""))
        due_dry += len(due) if dry_run else 0
        if pre_q or due:
            candidates.append((key, root, pre_q))
    for p, why in ignored:
        print(f"  [skip] {p.parent.name}/{p.name}: {why}; left alone")
        out["ignored"].append((str(p), why))

    holdings, hold_loaded = None, False
    failed_any = refused_any = config_abort = False
    any_queue = False
    warned_loose = False
    for key, proj, pre_q in candidates:
        try:
            srcs = project_sources(key, cfg_full, sources)
            excluded = not (srcs & PREPRINT_SOURCES)
            art_dir = project_artifact_dir(key, proj, artifact_dir, cfg_full)
        except lp_config.ConfigError as e:   # an invalid `sources` list or artifact_dir
            print(f"  ERR {key}: {e}", file=sys.stderr)
            out["exit_code"] = EXIT_USAGE
            return out
        gate_sources = srcs if (sources is not None or key in registry) else None

        if dry_run:
            held = _lockfile.read(proj)
            if held is not None:
                print(f"  DRY {key}: {_lock_note(held)}; a real sweep would skip this project")
            if not (srcs & SWEEP_SOURCES):
                print(f"  DRY {key}: the project's sources name no sweep stage; a real sweep would refuse "
                      f"its {len(pre_q)} queue(s) before fetching (exit 4)")
                any_queue = any_queue or bool(pre_q)
                refused_any = refused_any or bool(pre_q)
                continue
            qs = pre_q
        if not dry_run and not (srcs & SWEEP_SOURCES):
            # DEC-31: no sweep stage serves these sources ([], or openalex_content only): every queue
            # is refused before fetching and kept; nothing is admitted or written
            print(f"\n=== {key} ===")
            names = [q.name for q in pre_q]
            print(f"  ERR {key}: the project's sources ({', '.join(sorted(srcs)) or 'none'}) name no sweep "
                  f"stage (unpaywall, pmc or a preprint server); {len(names)} queue(s) kept, nothing fetched",
                  file=sys.stderr)
            refused_any = True
            out["projects"][key] = {"run_id": None, "results": [], "refused": names, "loose_end": None,
                                    "migrate": None, "no_sweep_stage": True}
            any_queue = any_queue or bool(names)
            continue

        lock = None
        if not dry_run:
            try:
                lock = _lockfile.Lock(proj, tool="sweep", stale_s=stale_s).acquire()
            except _lockfile.LockHeld as e:
                print(f"\n=== {key} ===")
                print(f"  SKIP {key}: {_lock_note(e.record)}; nothing of this project is touched this run "
                      f"(refused before fetching)", file=sys.stderr)
                refused_any = True
                any_queue = True
                out["projects"][key] = {"run_id": None, "results": [], "refused": [], "loose_end": None,
                                        "migrate": None, "lock_held": e.record}
                continue
        try:
            if not dry_run:
                # Due retry_later rows become the `retry` tagged queue before discovery. The project's
                # retry_later attempts are read first: admission removes the due rows, and a DOI also
                # re-queued fresh must keep its count.
                history = retry_history(proj)
                default_dest = None
                entry = registry.get(key) or {}
                if entry.get("lib_dir"):
                    default_dest = os.path.relpath(lit_util.lib_paths(key, entry)[1], proj).replace(os.sep, "/")
                adm = admit_retries(proj, today, default_destination=default_dest)
                if adm["admitted"] or adm["unreadable_not_before"]:
                    print(f"  {key}: admitted {adm['admitted']} due retry_later row(s) as "
                          f"lit_pull_queue.{RETRY_TAG}.csv"
                          + (f"; {adm['unreadable_not_before']} with an unreadable not_before stay"
                             if adm["unreadable_not_before"] else ""))
                out["admitted"][key] = adm
                qs = discover_queues(proj)
            else:
                history = retry_history(proj)
            if not qs:
                continue
            any_queue = True
            print(f"Found {len(qs)} queue(s) in {key}:")
            for q in qs:
                print(f"  {key}/{q.name}")
            if loose_ends and not dry_run and not warned_loose and resolve_loose_ends_path() is None:
                warned_loose = True
                print("[litpipe] no `loose_ends` key in projects.json -- cross-project "
                      "lit-pull log is off this run; see the literature-pipeline skill to "
                      "enable it.", file=sys.stderr)
            print(f"\n=== {key} ===")
            run_id = choose_run_id(run_id_dirs(proj, art_dir), today)
            print(f"[sweep] run_id={run_id} project={key}")
            if not dry_run and not hold_loaded:
                holdings, why = _load_holdings(registry)
                hold_loaded = True
                if holdings is None:
                    print(f"  [hold-check] off this run: {why}; rows held in another library are "
                          f"fetched again")
            stop = (lambda lk=lock: lk.lost.is_set()) if lock is not None else None
            results, refused = [], []
            lost = False
            for q in qs:
                if len(qs) > 1:
                    print(f"\n  --- {q.name} ---")
                if stop is not None and stop():
                    lost = True
                    break
                res = run_pipeline(proj, q, dry_run=dry_run, run_date=today,
                                   skip_preprint=skip_preprint, key=key,
                                   registry=registry, run_id=run_id, holdings=holdings,
                                   artifact_dir=art_dir, allow_destination=allow_destination,
                                   skip_reason=None if skip_preprint else (
                                       "the project's sources list names no preprint server"
                                       if excluded else None),
                                   preprint_arxiv_only=excluded, history=history,
                                   candidate_order=candidate_order, sources=gate_sources, stop=stop)
                if res is None:
                    refused.append(q.name)
                    refused_any = True
                    continue
                results.append(res)
                if res.get("dry"):
                    continue
                if res["failed_stages"]:
                    failed_any = True
                if res.get("lock_lost"):
                    lost = True
                    break
                if res["config_error"]:
                    config_abort = True
                    break
            proj_out = {"run_id": run_id, "results": results, "refused": refused,
                        "loose_end": None, "migrate": None}
            out["projects"][key] = proj_out
            if dry_run:
                continue
            if lost or (lock is not None and lock.lost.is_set()):
                # the project's lock was lost: another process may hold it now, so nothing more of the
                # project is written (no LOOSE_ENDS line, no routing); its queues stay
                proj_out["lock_lost"] = lock.lost_reason if lock is not None else "lost"
                print(f"  ERR {key}: project lock lost ({proj_out['lock_lost']}); the project's work "
                      f"stops as an abort, its queues stay", file=sys.stderr)
                failed_any = True
                continue
            line = loose_end_line(key, results, refused)
            if line and loose_ends:
                dest, why = write_loose_end(key, line)
                if dest:
                    print(f"\n  {dest} updated: {line}")
                elif why == "unchanged":
                    print(f"\n  LOOSE_ENDS unchanged (same state as the last {key} line)")
                proj_out["loose_end"] = line if dest else None
            if results:
                cmd = migrate_command(key, run_id, artifact_dir, skip_preprint or excluded,
                                      sources=srcs if sources is not None else None)
                proj_out["migrate"] = cmd
                if config_abort and not any(r.get("retired") for r in results):
                    # CONFIG aborts the run: nothing is routed (a stage that exited 2 may have left
                    # no CONFIG row for migrate to see)
                    print("  migrate not run: CONFIG aborts the run; the queue stays for a re-sweep "
                          "once the configuration is fixed")
                elif migrate:
                    # a CONFIG abort after a queue of this run retired: route the run now (a later
                    # run has another run id); the CONFIG queue's rows are PENDING (route none) and
                    # migrate refuses any chain that carries a CONFIG row
                    print(f"  -> migrate: {shlex.join(cmd)}")
                    rm = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8",
                                        errors="replace", env=_stage_env())
                    print(rm.stdout[-1500:] if rm.stdout else "")
                    if rm.returncode != 0:
                        print(f"  ERR migrate failed (exit {rm.returncode}):\n{(rm.stderr or '')[-500:]}")
                        failed_any = True
                else:
                    print(f"  next: route this run's residuals with\n    {shlex.join(cmd)}")
        finally:
            if lock is not None:
                lock.release()
        if config_abort:
            print("  ERR a source refused our configuration (CONFIG); aborting the run",
                  file=sys.stderr)
            break

    if not any_queue:
        scope = project or "any project"
        print(f"No lit_pull_queue.csv found in {scope}.")
        # A1 defense-in-depth: an explicit --project with no queue is a failure the
        # orchestrator must NOT read as success (the old silent no-op loop). A bare
        # sweep with nothing staged is a normal idle run (exit 0).
        if project and not due_dry:
            out["exit_code"] = EXIT_NO_QUEUE
        return out

    if config_abort:
        out["exit_code"] = EXIT_USAGE
    elif failed_any:
        out["exit_code"] = EXIT_STAGE_FAILED
    elif refused_any:
        out["exit_code"] = EXIT_QUEUE_REFUSED
    return out


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None,
                                 epilog=EXIT_CODES_HELP,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--project", help="Process only this project (default: all)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Show the queues, run ids and row counts that would be processed. "
                         "Writes nothing and makes no network call.")
    ap.add_argument("--skip-preprint", action="store_true",
                    help="Skip stage 3 (preprint_fetch). Use when the queue is older literature "
                         "that preprint servers will not hold: on a 2026-09-14 teaching-library "
                         "sweep the stage returned 0 hits in 28 rows at ~2 rows/min, then blocked "
                         "on a socket that never timed out. A skipped stage does not block "
                         "retirement: rows are classified on Unpaywall and PMC alone and the "
                         "residual CSV lists preprint under skipped_sources.")
    ap.add_argument("--date", default=None,
                    help="YYYY-MM-DD the run id starts from (default: today); the run id is this "
                         "date, or date.N when an earlier run of that date left artifacts. "
                         "run_daily passes its run date so sweep + migrate stay in lockstep.")
    ap.add_argument("--no-loose-ends", action="store_true",
                    help="Write no LOOSE_ENDS line (the runner writes its own, one per project "
                         "per run).")
    ap.add_argument("--migrate", action="store_true",
                    help="Run migrate_closed_to_md.py for this run id after each project; "
                         "without it the exact command is printed.")
    ap.add_argument("--artifact-dir", default=None,
                    help="Directory for this run's artifacts (relative paths are under each "
                         "project; default: projects.json artifact_dir, else the project directory, "
                         "as before).")
    ap.add_argument("--allow-destination", action="store_true",
                    help="Sweep a queue whose destination is not the registry library for its "
                         "project (refused by default: a doubled subproject tail lands a shadow "
                         "library).")
    ap.add_argument("--candidate-order", choices=CANDIDATE_ORDERS, default=None,
                    help="Unpaywall candidate order, passed to the Unpaywall stage (DEC-11; the stage's "
                         "default is repository). Omitted: the stage decides.")
    ap.add_argument("--sources", default=None, metavar="LIST",
                    help="Comma-separated fetch sources for this run only, replacing every project's "
                         "`sources` (DEC-31): unpaywall, pmc, europepmc_preprints, biorxiv, medrxiv, osf, "
                         "sportrxiv, arxiv, openalex_content. A stage whose source is not listed is skipped "
                         "(reported, not a failure), and a row retires on the listed stages alone (one "
                         "the run never tried can be routed to ILL); an invalid list exits 2.")
    args = ap.parse_args(argv)
    res = run(project=args.project, dry_run=args.dry_run, skip_preprint=args.skip_preprint,
              date=args.date, loose_ends=not args.no_loose_ends, migrate=args.migrate,
              artifact_dir=args.artifact_dir, allow_destination=args.allow_destination,
              candidate_order=args.candidate_order, sources=args.sources)
    return res["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
