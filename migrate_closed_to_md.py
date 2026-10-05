"""Route a sweep run's residual rows by residual class (dispatch 0.5; W1-D2).

After a sweep, every row that no stage fetched is classified by the residual table (first match
wins) and routed:

  HELD_ELSEWHERE   the DOI is held with a PDF somewhere in the portfolio (litpipe.holdings), or as
                   text only in another library: nothing queued; the routing report has the path
  TEXT_ONLY        a text-only sidecar was written in this project's library, no PDF: nothing
                   queued (a partial success; the index carries it with has_pdf=false)
  PENDING          an enabled stage has no verdict for the row (its report or row is missing):
                   nothing queued; sweep keeps the queue for a re-sweep
  TRANSIENT        an embargo, a source refused or deferred for the run, an outage, a transport
                   error, or an unclassified ERROR (on the row's third sweep, `attempts` >= 3, it
                   becomes TERMINAL_CLOSED with its reason) -> lit_pull_queue.retry_later.csv with
                   not_before (1 day; an embargo's release date, 7 days when the date is unknown)
                   and attempts
  OA_BLOCKED       a download host refused us (403, an HTML wall, a final 429), or a preprint needs a
                   manual click -> lit_pull_queue.oa_blocked.md (browser worklist; a manual preprint
                   links its landing page) and retry_later (3 days)
  IDENTITY_FLAG    the served file failed the identity check (another DOI, or a supplement) ->
                   lit_pull_queue.review.md with the file's path and its evidence (the file stays put)
  SKIPPED_SOURCE   no enabled stage attempted the row: nothing
  NO_METADATA      blank title and authors: nothing queued; listed in the routing report
  TERMINAL_CLOSED  every enabled stage said NO_MATCH / NOT_AVAILABLE -> the ILL list lit_pull_queue.md
  INVALID_DOI      (sweep) a NO_DOI_* or invalid DOI: reported, never routed
  CONFIG           a stage reported a configuration error: the run aborts, nothing is written, exit 2

Artifacts: the untagged chain `lit_pull_queue.<run_id>.<stage>.csv` and every tagged chain
`lit_pull_queue.<tag>.<run_id>.<stage>.csv` of the run, in the project root or sweep's
--artifact-dir; run_id is `YYYY-MM-DD` or `YYYY-MM-DD.N`, so the legacy dated names read unchanged.
When the chain has sweep's typed residual CSV (a `residual_class` column, W1-D1) the rows route by
that class, with its not_before, flagged_path and landing_url columns (W2-G) and the stage
reports supplying the worklist detail; a chain without one is classified here from its stage
reports, reading each row's typed columns through sweep's verdict readers when the report has
them and the legacy columns otherwise. Each routed chain also gets
`lit_pull_queue[.<tag>].<run_id>.routing.csv` (one row per residual: class, reason, held paths).

The .md lists and retry_later dedup by DOI in any form (bare, backtick, `doi:`, doi.org link),
case-insensitively (REG-I25); retry_later appends and updates in place, never rewrites other rows.
Every persisted error string passes through litpipe.ledger.redact (I20: the Unpaywall exception
text carries the `email=` query value).

Usage:
  python migrate_closed_to_md.py --project MYPROJ
  python migrate_closed_to_md.py --project MYPROJ --date 2026-05-27          # a legacy dated run
  python migrate_closed_to_md.py --project MYPROJ --run-id 2026-09-30.2      # same flag, a run id
  python migrate_closed_to_md.py --project MYPROJ --tag retry --dry-run
"""
import argparse
import csv
import datetime
import os
import re
import sys
from pathlib import Path
from urllib.parse import quote

import lit_util  # RC4: atomic writes for crash-safe .md / .csv writes
from litpipe import config, holdings
from litpipe import doi as _doi
from litpipe import ledger as _ledger
from litpipe.outcomes import Kind, from_legacy

HERE = Path(__file__).parent
CONFIG_PATH = HERE / "projects.json"

ILL_NAME = "lit_pull_queue.md"
OA_BLOCKED_NAME = "lit_pull_queue.oa_blocked.md"
REVIEW_NAME = "lit_pull_queue.review.md"
RETRY_LATER_NAME = "lit_pull_queue.retry_later.csv"

OA_BLOCKED_DAYS = 3
TRANSIENT_DAYS = 1
EMBARGO_DEFAULT_DAYS = 7
ERROR_RUN_LIMIT = 3

# Residual classes (dispatch 0.5)
HELD_ELSEWHERE = "HELD_ELSEWHERE"
TEXT_ONLY = "TEXT_ONLY"
TRANSIENT = "TRANSIENT"
OA_BLOCKED = "OA_BLOCKED"
IDENTITY_FLAG = "IDENTITY_FLAG"
SKIPPED_SOURCE = "SKIPPED_SOURCE"
NO_METADATA = "NO_METADATA"
TERMINAL_CLOSED = "TERMINAL_CLOSED"
CONFIG = "CONFIG"
PENDING = "PENDING"          # an enabled stage has no verdict for the row: sweep keeps the queue
INVALID_DOI = "INVALID_DOI"  # sweep: NO_DOI_* or an invalid DOI, reported, never fetched
ROUTE_OF = {
    HELD_ELSEWHERE: "none", TEXT_ONLY: "none", SKIPPED_SOURCE: "none", NO_METADATA: "none",
    TRANSIENT: "retry_later", OA_BLOCKED: "oa_blocked+retry_later", IDENTITY_FLAG: "review",
    TERMINAL_CLOSED: "ill", CONFIG: "abort", PENDING: "none (queue kept for a re-sweep)",
    INVALID_DOI: "none",
}

PREPRINT_SOURCES = frozenset(config.ALLOWED_SOURCES - {"unpaywall", "pmc", "openalex_content"})
_TERMINAL_KINDS = frozenset({Kind.NO_MATCH, Kind.NOT_AVAILABLE, Kind.NOT_AT_RA})
_TRANSIENT_KINDS = frozenset({Kind.OUTAGE, Kind.TRANSPORT, Kind.DEFERRED})
_TRUE = ("true", "1", "yes")

RUN_ID_RE = r"\d{4}-\d{2}-\d{2}(?:\.\d+)?"
TAG_RE = r"[a-z][a-z0-9_-]{0,31}"
_ARTIFACT = re.compile(rf"^lit_pull_queue(?:\.(?P<tag>{TAG_RE}))?\.(?P<run>{RUN_ID_RE})"
                       rf"\.(?P<stage>[a-z_]+)\.csv$")

# retry_later: the six queue columns first (sweep's admit_retries copies every column into the
# `retry` queue, so `attempts` rides through re-admission and back into the residual CSV).
RETRY_FIELDS = ["doi", "title", "authors", "year", "destination", "notes", "residual_class",
                "reason", "not_before", "attempts", "first_seen", "last_seen", "last_run"]
ROUTING_FIELDS = ["doi", "title", "year", "residual_class", "route", "reason", "held_paths",
                  "not_before", "attempts", "stage_unpaywall", "stage_pmc", "stage_preprint",
                  "flagged_path", "landing_url"]


# ---------------------------------------------------------------- redaction (I20)
def redact(text) -> str:
    """Every string this module persists or prints from a report goes through here: the one
    implementation, litpipe.ledger.redact (dispatch 0.5)."""
    if text is None:
        return ""
    s = str(text)
    return _ledger.redact(s) if s else s


# ---------------------------------------------------------------- artifact names
def project_dir(project: str, proj_cfg: dict) -> Path:
    """On-disk project directory for a registry key (tail-aware for subprojects).
    Thin wrapper over lit_util.project_root; `proj_cfg` is the single project's dict."""
    return lit_util.project_root(project, proj_cfg)


def _run_key(run_id):
    date, _, n = run_id.partition(".")
    return date, int(n) if n else 1


def artifact_path(project_root: Path, run_id: str, stage: str, tag=None) -> Path:
    prefix = f"lit_pull_queue.{tag}" if tag else "lit_pull_queue"
    return Path(project_root) / f"{prefix}.{run_id}.{stage}.csv"


def _artifacts(project_root):
    try:
        names = os.listdir(project_root)
    except OSError:
        return []
    return [m for m in map(_ARTIFACT.match, names) if m]


def latest_sweep_date(project_root: Path) -> str | None:
    """The latest run id (YYYY-MM-DD or YYYY-MM-DD.N) with a report, unpaywall or residual artifact,
    tagged or not. The name is kept for callers; a legacy dated run returns its date."""
    runs = {m.group("run") for m in _artifacts(project_root)
            if m.group("stage") in ("report", "unpaywall", "residual")}
    return max(runs, key=_run_key) if runs else None


def find_tags(project_root: Path, run_id: str) -> list:
    """Tags with an unpaywall or residual artifact for this run id (the untagged chain is not
    listed). A run whose Unpaywall stage failed still writes its typed residual CSV."""
    return sorted({m.group("tag") for m in _artifacts(project_root)
                   if m.group("tag") and m.group("run") == run_id
                   and m.group("stage") in ("unpaywall", "residual")})


def resolve_artifact_dir(project_root, artifact_dir=None) -> Path:
    """Where the run artifacts live: sweep's --artifact-dir (absolute, or relative to the project),
    else the project root. The .md lists and retry_later always live in the project root."""
    if not artifact_dir:
        return Path(project_root)
    p = Path(artifact_dir).expanduser()
    return p if p.is_absolute() else Path(project_root) / p


# ---------------------------------------------------------------- reading a chain
def _read_csv(path):
    with open(path, encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _truthy(v):
    return str(v or "").strip().lower() in _TRUE


def _sig(stage, kind, where, detail):
    return {"stage": stage, "kind": Kind(kind), "where": where, "detail": str(detail or "")}


def _split_attempts(s):
    return [a.strip() for a in (s or "").split(" | ") if a.strip()]


def _sweep():
    """sweep, imported late: its verdict readers are the one implementation of the typed report
    columns (W2-G), so a chain routed here reads a typed row exactly the way sweep does."""
    import sweep
    return sweep


def _verdict_signals(v, r):
    """(signals, identity) from sweep's Verdict for a typed report row: one signal from the
    row's `outcome` (its first root cause), none for a fetched row, a source the project
    excludes, or an identity-flagged file (the flag itself is returned as `identity`)."""
    identity = (v.raw or "identity FLAG") if v.identity else ""
    if v.excluded_source or v.identity or _sweep().fetched_by(v):
        return [], identity
    if v.manual_preprint:
        src = (r.get("source") or "").strip()
        return [_sig(v.stage, Kind.SKIPPED, "download", f"manual_preprint:{src}".rstrip(":"))], ""
    return [_sig(v.stage, v.kind, "download" if v.download_host else "api", v.raw)], ""


def _unpaywall_signals(r):
    """Signals from one unpaywall report row (not downloaded, not SKIP_EXISTS)."""
    v = _sweep().unpaywall_verdict(r)
    if v.typed or v.identity and not (r.get("error") or "").strip().startswith("DOI_MISMATCH"):
        return _verdict_signals(v, r)
    sig, identity = [], ""
    err = (r.get("error") or "").strip()
    oa = (r.get("oa_status") or "").strip().upper()
    attempts = _split_attempts(r.get("attempts"))
    if err.startswith("DOI_MISMATCH"):
        identity = err
    if attempts:
        for a in attempts:  # "host_type/version/STATUS"
            kind = from_legacy(a.rsplit("/", 1)[-1], "unpaywall")
            if kind is not Kind.OK:  # an OK attempt on a DOI_MISMATCH row served the wrong paper
                sig.append(_sig("unpaywall", kind, "download", a))
    elif err and not identity:
        # No download was attempted: the error came from the lookup API (or oa_status says OA and
        # the only failure is the final status). An API-form "HTTP 404" is NO_MATCH, 422/410 CONFIG.
        where = "download" if oa == "OA" else "api"
        sig.append(_sig("unpaywall", from_legacy(err, "unpaywall"), where, err))
    elif oa == "CLOSED":
        sig.append(_sig("unpaywall", Kind.NOT_AVAILABLE, "api", "CLOSED"))
    elif oa == "OA" and not identity:
        sig.append(_sig("unpaywall", Kind.NOT_AVAILABLE, "api", "OA_NO_URL"))
    elif not identity:
        sig.append(_sig("unpaywall", Kind.ERROR, "api", err or "unknown"))
    return sig, identity


def _pmc_signals(r):
    sig, identity = [], ""
    err = (r.get("error") or "").strip()
    attempts = _split_attempts(r.get("attempts"))
    if err.startswith("DOI_MISMATCH"):
        identity = err
    v = _sweep().pmc_verdict(r)
    if v.typed or (v.identity and not identity):
        sig, identity = _verdict_signals(v, r)
    elif attempts:
        for a in attempts:  # "source/STATUS"; the source prefix is load-bearing (europepmc/HTTP_500)
            kind = from_legacy(a, "pmc")
            if kind is not Kind.OK:
                sig.append(_sig("pmc", kind, "download", a))
    elif err and not identity:
        sig.append(_sig("pmc", from_legacy(err, "pmc"), "api" if err == "NO_PMCID" else "download", err))
    elif not (r.get("pmcid") or "").strip() and not identity:
        sig.append(_sig("pmc", Kind.NO_MATCH, "api", "NO_PMCID"))
    elif not identity:
        sig.append(_sig("pmc", Kind.ERROR, "download", "no attempt recorded"))
    # The JATS sidecar fetch is a separate request: an outage or transport failure there says the
    # text may still come later, so it keeps the row out of the ILL list.
    st = (r.get("sidecar_status") or "").strip()
    if st and st not in ("OK", "EXISTS", "NOT_AVAILABLE"):
        kind = from_legacy(st, "fulltext")
        if kind in _TRANSIENT_KINDS:
            sig.append(_sig("pmc", kind, "api", f"sidecar/{st}"))
    return sig, identity


def _preprint_signals(r):
    st = (r.get("status") or "").strip()
    v = _sweep().preprint_verdict(r)
    if v.typed or v.identity or v.excluded_source:
        return _verdict_signals(v, r)
    if st.startswith("DOI_MISMATCH"):
        return [], st
    if not st or st == "DRY":
        return [], ""
    if st == "MANUAL_PREPRINT":
        src = (r.get("source") or "").strip()
        return [_sig("preprint", Kind.SKIPPED, "download", f"manual_preprint:{src}".rstrip(":"))], ""
    kind = from_legacy(st, "preprint")
    if kind is Kind.OK:
        return [], ""
    return [_sig("preprint", kind, "api" if kind is Kind.NO_MATCH else "download", st)], ""


def _key(doi):
    """The comparison key of a report DOI: normalised, or the lower-cased raw string when it is not
    a well-formed DOI (such a row is still routed, never silently dropped)."""
    return holdings.doi_key(doi)


def _resolved(r, *flags):
    """The stage holds the paper: downloaded, skipped or ALREADY_EXISTS, and not identity-flagged
    (a flagged file is never the paper, whatever the row says)."""
    flagged = any(_sweep().is_flagged(r, f) for f in ("error", "status"))
    return not flagged and (_truthy(r.get("downloaded")) or _truthy(r.get("skipped"))
                            or any((r.get(f) or "").strip() == "ALREADY_EXISTS" for f in flags))


def _flagged_paths(v, lib_dir):
    """The library path of a flagged file named by a stage verdict, as a one-item list."""
    if not (v.identity and v.flagged_file):
        return []
    return [str(Path(lib_dir) / v.flagged_file) if lib_dir is not None else v.flagged_file]


def _add_extras(row, v):
    """Carry a later stage's typed extras onto a chain row: the earliest embargo release date, a
    flagged file, a manual preprint's landing page."""
    if v.kind is Kind.EMBARGOED and v.release_date and not v.excluded_source:
        row["not_before"] = min(filter(None, (row.get("not_before"), v.release_date)))
    for p in _flagged_paths(v, row.get("lib_dir")):
        if p not in row["flagged_path"]:
            row["flagged_path"].append(p)
    if v.landing_url and not row.get("landing_url") and not v.excluded_source:
        row["landing_url"] = v.landing_url


def read_report_chain(project_root: Path, sweep_date: str, tag=None, *, project_dir=None):
    """Read one chain (the untagged one unless `tag`) of run `sweep_date` from the artifacts in
    `project_root` and return one dict per DOI that no stage fetched, with the per-stage signals and
    a preliminary residual class (default sources, no holdings map). `run()` re-classifies with the
    project's sources and the holdings map. `project_dir` anchors the queue's `destination` when the
    artifacts sit elsewhere (sweep --artifact-dir). Returns [] when the chain has no unpaywall
    report."""
    project_root = Path(project_root)
    anchor = Path(project_dir) if project_dir is not None else project_root
    unpaywall_path = artifact_path(project_root, sweep_date, "unpaywall", tag)
    pmc_path = artifact_path(project_root, sweep_date, "pmc", tag)
    preprint_path = artifact_path(project_root, sweep_date, "preprint", tag)
    norm_path = artifact_path(project_root, sweep_date, "normalized", tag)
    if not unpaywall_path.exists():
        return []

    queue = {}
    if norm_path.exists():
        for q in _read_csv(norm_path):
            d = _key(q.get("doi"))
            if d:
                queue.setdefault(d, q)

    out = {}
    sw = _sweep()
    for r in _read_csv(unpaywall_path):
        held = _truthy(r.get("downloaded")) or (r.get("oa_status") or "").strip().upper() == "SKIP_EXISTS"
        if held and not sw.is_flagged(r, "error"):
            continue
        doi = (r.get("doi") or "").strip()
        d = _key(doi)
        if not d or d in out:
            continue
        sig, identity = _unpaywall_signals(r)
        uv = sw.unpaywall_verdict(r)
        q = queue.get(d, {})
        dest = (q.get("destination") or "").strip()
        lib_dir = Path(os.path.abspath(anchor / dest)) if dest else None
        out[d] = {
            "doi": doi, "doi_norm": d, "tag": tag, "run_id": sweep_date,
            "title": (r.get("title") or q.get("title") or "").strip(),
            "year": (r.get("year") or q.get("year") or "").strip(),
            "authors": (q.get("authors") or "").strip(),
            "notes": (q.get("notes") or "").strip(),
            "prior_attempts": lit_util.coerce_int(q.get("attempts")),
            "first_seen": (q.get("first_seen") or "").strip(),
            "destination": dest,
            "lib_dir": lib_dir,
            "oa_status": (r.get("oa_status") or "").strip(),
            "winning_host": (r.get("winning_host") or "").strip(),
            "error": (r.get("error") or "").strip(),
            "filename": (r.get("filename") or "").strip(),
            "pmcid": "", "pmc_filename": "", "sidecar_flag": False,
            "stage_unpaywall": "FAIL", "stage_pmc": "skip", "stage_preprint": "skip",
            "signals": sig, "identity": identity, "identity_stage": "unpaywall" if identity else "",
            "missing": {},
            # W2-G: the typed extras (an embargo's release date, a flagged file, a landing page)
            "not_before": uv.release_date if uv.kind is Kind.EMBARGOED else "",
            "flagged_path": _flagged_paths(uv, lib_dir), "landing_url": "",
        }

    if pmc_path.exists():
        seen = set()
        for r in _read_csv(pmc_path):
            d = _key(r.get("doi"))
            if d not in out:
                continue
            seen.add(d)
            # 2026-06-25 audit sibling sweep (T5b parallel oracle): an already-present paper reports
            # skipped=True / winning_source=ALREADY_EXISTS with downloaded=False. It is resolved (the
            # PDF is on disk), so it is NOT routed to the ILL list (the exact harm T3 guards).
            if _resolved(r, "winning_source"):
                out.pop(d)
                continue
            row = out[d]
            sig, identity = _pmc_signals(r)
            row["signals"] += sig
            if identity and not row["identity"]:
                row["identity"], row["identity_stage"] = identity, "pmc"
            pv = sw.pmc_verdict(r)
            _add_extras(row, pv)
            row["pmcid"] = (r.get("pmcid") or "").strip()
            row["pmc_filename"] = (r.get("filename") or "").strip()
            # TEXT_ONLY exactly as sweep reads it: a sidecar, no PDF judged, never a flagged file
            row["sidecar_flag"] = pv.text_only
            row["stage_pmc"] = "no_pmcid" if not row["pmcid"] else "fail"
        for d, row in out.items():
            if d not in seen:
                row["missing"]["pmc"] = "not_in_report"
    else:
        # T3 (2026-06-25): PMC report ABSENT (stage crashed or never ran): not confirmed closed.
        for row in out.values():
            row["stage_pmc"] = "no_report"
            row["missing"]["pmc"] = "report_absent"

    if preprint_path.exists():
        seen = set()
        for r in _read_csv(preprint_path):
            d = _key(r.get("doi"))
            if d not in out:
                continue
            seen.add(d)
            # 2026-06-25 audit sibling sweep: status=ALREADY_EXISTS / skipped=True is resolved.
            if _resolved(r, "status"):
                out.pop(d)
                continue
            row = out[d]
            sig, identity = _preprint_signals(r)
            row["signals"] += sig
            if identity and not row["identity"]:
                row["identity"], row["identity_stage"] = identity, "preprint"
            rv = sw.preprint_verdict(r)
            _add_extras(row, rv)
            st = (r.get("status") or "").strip()
            row["stage_preprint"] = ("excluded" if rv.excluded_source else
                                     "no_match" if st in ("NO_MATCH", "") else
                                     "manual" if st == "MANUAL_PREPRINT" or rv.manual_preprint else "fail")
        for d, row in out.items():
            if d not in seen:
                row["missing"]["preprint"] = "not_in_report"
    else:
        for row in out.values():
            if row["stage_preprint"] == "skip":
                row["stage_preprint"] = "no_report"
            row["missing"]["preprint"] = "report_absent"

    rows = list(out.values())
    for row in rows:
        classify_row(row)
    return rows


_REASON_STAGE = re.compile(r"^(unpaywall|pmc|preprint)(?: refused)?:\s*(.+)$")


def read_residual(project_root: Path, run_id: str, tag=None, *, project_dir=None):
    """The rows of sweep's typed residual CSV (W1-D1: `residual_class`, `reason`, `stages`,
    `held_at`, `skipped_sources`, `attempts`, `run_id`) for one chain, as routing dicts with the
    class sweep decided. None when the chain has no residual CSV or only the legacy untyped one
    (no `residual_class` column), so the caller falls back to the report chain."""
    path = artifact_path(project_root, run_id, "residual", tag)
    if not path.exists():
        return None
    with open(path, encoding="utf-8-sig", newline="") as f:
        rdr = csv.DictReader(f)
        if "residual_class" not in (rdr.fieldnames or []):
            return None
        raw = list(rdr)
    anchor = Path(project_dir) if project_dir is not None else Path(project_root)
    rows = []
    for r in raw:
        doi = (r.get("doi") or "").strip()
        cls = (r.get("residual_class") or "").strip()
        reason = (r.get("reason") or "").strip()
        dest = (r.get("destination") or "").strip()
        row = {
            "doi": doi, "doi_norm": _key(doi), "tag": tag, "run_id": run_id, "typed": True,
            "title": (r.get("title") or "").strip(), "year": (r.get("year") or "").strip(),
            "authors": (r.get("authors") or "").strip(), "notes": (r.get("notes") or "").strip(),
            "destination": dest, "lib_dir": Path(os.path.abspath(anchor / dest)) if dest else None,
            "residual_class": cls, "reason": reason, "stages": (r.get("stages") or "").strip(),
            "held_paths": [p.strip() for p in (r.get("held_at") or "").split(";") if p.strip()],
            "skipped_sources": (r.get("skipped_sources") or "").strip(),
            "attempts": lit_util.coerce_int(r.get("attempts")) or "",
            "not_before_in": (r.get("not_before") or "").strip(),
            "first_seen": (r.get("first_seen") or "").strip(),
            "signals": [], "identity": "", "identity_stage": "", "pmcid": "",
            "stage_unpaywall": "", "stage_pmc": "", "stage_preprint": "",
            # W2-G extras (absent in a W1-D1 residual)
            "flagged_path": [p.strip() for p in (r.get("flagged_path") or "").split(";") if p.strip()],
            "landing_url": (r.get("landing_url") or "").strip(),
        }
        if cls == IDENTITY_FLAG:
            m = _REASON_STAGE.match(reason)
            row["identity"], row["identity_stage"] = (m.group(2), m.group(1)) if m else (reason, "")
        rows.append(row)
    return rows


_CHAIN_DETAIL = ("signals", "pmcid", "pmc_filename", "sidecar_flag", "oa_status", "error", "filename",
                 "stage_unpaywall", "stage_pmc", "stage_preprint")


def _enrich(typed_rows, chain_rows):
    """Copy the per-stage detail (signals, pmcid, stage labels) of the report chain onto the typed
    rows, for the worklist's cause / link and the ILL line's trace. The class stays sweep's."""
    by = {r["doi_norm"]: r for r in chain_rows}
    for t in typed_rows:
        c = by.get(t["doi_norm"])
        if c is None:
            continue
        for k in _CHAIN_DETAIL:
            if k in c:
                t[k] = c[k]
        if not t["identity"] and c.get("identity"):
            t["identity"], t["identity_stage"] = c["identity"], c.get("identity_stage", "")
        # a residual written before W2-G has no extras columns: the reports supply them
        if not t.get("flagged_path") and c.get("flagged_path"):
            t["flagged_path"] = list(c["flagged_path"])
        if not t.get("landing_url") and c.get("landing_url"):
            t["landing_url"] = c["landing_url"]
        if not t.get("not_before_in") and c.get("not_before"):
            t["not_before_in"] = c["not_before"]
    return typed_rows


# ---------------------------------------------------------------- classification
def _legacy_signals(row):
    """Signals for a row in the pre-W1-D2 shape (oa_status / error / stage_pmc / stage_preprint)."""
    sig = []
    oa = (row.get("oa_status") or "").strip().upper()
    err = (row.get("error") or "").strip()
    if err:
        sig.append(_sig("unpaywall", from_legacy(err, "unpaywall"), "download" if oa == "OA" else "api", err))
    elif oa == "CLOSED":
        sig.append(_sig("unpaywall", Kind.NOT_AVAILABLE, "api", "CLOSED"))
    pmc = (row.get("stage_pmc") or "").strip()
    if pmc == "no_pmcid":
        sig.append(_sig("pmc", Kind.NO_MATCH, "api", "NO_PMCID"))
    elif pmc == "fail":
        sig.append(_sig("pmc", Kind.ERROR, "download", "fail"))
    elif pmc == "no_report":
        sig.append(_sig("pmc", Kind.DEFERRED, "stage", "report_absent"))
    if (row.get("stage_preprint") or "").strip() == "no_match":
        sig.append(_sig("preprint", Kind.NO_MATCH, "api", "NO_MATCH"))
    return sig


def _norm_lib(p):
    return os.path.normcase(os.path.abspath(str(p)))


def _sidecar_text_on_disk(row, lib):
    """True when this row's PMC sidecar sits in `lib` with text and no PDF beside it."""
    fn = row.get("pmc_filename") or row.get("filename") or ""
    if not fn or lib is None or not fn.lower().endswith(".pdf"):
        return False
    lib = Path(lib)
    sc = lib / (fn[:-4] + holdings.SIDECAR_SUFFIX)
    if not sc.exists() or (lib / fn).exists():
        return False
    try:
        return holdings.read_sidecar(sc)[1] > 0
    except (OSError, ValueError):
        return False


def _reason(signals, limit=240):
    bits = []
    for s in signals:
        d = redact(s["detail"])
        bits.append(f"{s['stage']}:{s['kind'].value}" + (f"({d[:limit]})" if d else ""))
    return "; ".join(bits)


def classify_row(row, *, holdmap=None, own_libs=None, default_lib=None, sources=None, attempts=1):
    """Set row['residual_class'], ['reason'], ['held_paths'], ['attempts'] by the 0.5 table, first
    match wins, in sweep's order (W1-D1 `classify`). `own_libs`: normcased library paths that count
    as this project's own; `sources`: the enabled sources (default litpipe.config.DEFAULT_SOURCES);
    `attempts`: sweeps of this row including this one (an ERROR row closes on the third)."""
    sources = set(config.DEFAULT_SOURCES if sources is None else sources)
    signals = list(row["signals"]) if "signals" in row else _legacy_signals(row)
    enabled = {"pmc": "pmc" in sources, "preprint": bool(sources & PREPRINT_SOURCES)}
    # A missing report (or row) of an enabled stage: the stage did not attempt the row, so the row
    # is PENDING and the queue stays for a re-sweep. A stage the project excludes blocks nothing.
    missing = [f"{stage} {why}" for stage, why in (row.get("missing") or {}).items()
               if enabled.get(stage, True)]
    identity = row.get("identity") or ""
    kinds = {s["kind"] for s in signals}
    row.update(held_paths=[], attempts=attempts, reason="")

    def done(cls, reason):
        # An identity flag outranked by an earlier class (the row is OA-blocked, say) stays visible.
        if identity and cls != IDENTITY_FLAG:
            reason = f"{reason}; identity: {redact(identity)}"
        row["residual_class"], row["reason"] = cls, reason
        return cls

    own = set(own_libs or ())
    lib = row.get("lib_dir") or default_lib
    if lib is not None:
        own.add(_norm_lib(lib))
    held = holdmap.content(row["doi"]) if holdmap is not None else []
    pdfs = [h for h in held if h.has_pdf]
    if pdfs:
        row["held_paths"] = [str(h.path) for h in pdfs]
        return done(HELD_ELSEWHERE, f"held with a PDF: {pdfs[0].path}")
    if missing:
        return done(PENDING, "; ".join(missing))
    own_text = [h for h in held if _norm_lib(h.library) in own]
    # The sidecar on disk decides when the library is known; with no library to look in, the PMC
    # report's own sidecar flag (OK / EXISTS) is the evidence.
    if own_text or _sidecar_text_on_disk(row, lib) or (lib is None and row.get("sidecar_flag")):
        row["held_paths"] = [str(h.path) for h in own_text]
        return done(TEXT_ONLY, "text-only sidecar, no PDF")
    if held:
        row["held_paths"] = [str(h.path) for h in held]
        return done(HELD_ELSEWHERE, f"held as text only: {held[0].path}")
    if Kind.EMBARGOED in kinds:
        return done(TRANSIENT, _reason([s for s in signals if s["kind"] is Kind.EMBARGOED]))
    blocked = [s for s in signals if (s["kind"] is Kind.REFUSED and s["where"] == "download")
               or (s["kind"] is Kind.SKIPPED and s["detail"].startswith("manual_preprint"))]
    if blocked:
        return done(OA_BLOCKED, _reason(blocked))
    refused_src = [s for s in signals if s["kind"] in (Kind.REFUSED, Kind.DEFERRED) and s["where"] == "api"]
    if refused_src:
        return done(TRANSIENT, _reason(refused_src))
    transient = [s for s in signals if s["kind"] in _TRANSIENT_KINDS]
    if transient:
        return done(TRANSIENT, _reason(transient))
    if identity:
        return done(IDENTITY_FLAG, redact(identity))
    blocking = kinds - {Kind.SKIPPED, Kind.ALIASED}
    if not blocking:
        return done(SKIPPED_SOURCE, "no enabled stage attempted the row")
    if not (row.get("title") or "").strip() and not (row.get("authors") or "").strip():
        return done(NO_METADATA, "blank title and authors")
    if Kind.CONFIG in kinds:
        return done(CONFIG, _reason([s for s in signals if s["kind"] is Kind.CONFIG]))
    if blocking <= _TERMINAL_KINDS:
        return done(TERMINAL_CLOSED, _reason(signals))
    errors = [s for s in signals if s["kind"] is Kind.ERROR] or signals
    if attempts >= ERROR_RUN_LIMIT:
        return done(TERMINAL_CLOSED, f"error in {attempts} runs: {_reason(errors)}")
    return done(TRANSIENT, f"error (run {attempts} of {ERROR_RUN_LIMIT}): {_reason(errors)}")


def has_config_signal(row):
    return any(s["kind"] is Kind.CONFIG for s in row.get("signals") or ())


# ---------------------------------------------------------------- rendering
_BACKTICK_DOI = re.compile(r"DOI `([^`]+)`")


def existing_dois(text) -> set:
    """Every DOI already present in a .md list, normalised: any form (bare, backtick, `doi:`,
    doi.org link; REG-I25), plus every `DOI \\`...\\`` token this tool writes even when it is not
    DOI-shaped, so a malformed DOI it listed once is not listed again."""
    out = set(holdings.extract_dois(text))
    out.update(_key(m) for m in _BACKTICK_DOI.findall(text or ""))
    return out


def _title(r, n=150):
    t = re.sub(r"\s+", " ", (r.get("title") or "").replace("**", "")).strip()
    return t[:n] or "(no title)"


def render_md_block(project: str, sweep_date: str, rows: list, tag=None) -> str:
    """The ILL section for TERMINAL_CLOSED rows (the `DOI \\`<doi>\\`` token is what dedup reads)."""
    if not rows:
        return ""
    stem = f"lit_pull_queue.{tag}.{sweep_date}" if tag else f"lit_pull_queue.{sweep_date}"
    label = f"{sweep_date} [{tag}]" if tag else sweep_date
    lines = [
        "",
        "---",
        "",
        f"## Sweep residuals {label}: {len(rows)} closed-access (auto-migrated)",
        "",
        f"Auto-migrated from `{stem}.*.csv` by `_tools/literature_pipeline/migrate_closed_to_md.py`. ",
        "Every enabled fetch stage found no copy (closed access, no PMC record, no preprint). ILLIAD candidates.",
        "",
    ]
    if any(r.get("stage_pmc") == "no_report" or r.get("stage_preprint") == "no_report" for r in rows):
        lines.append("> NOTE: rows tagged `pmc=no_report` / `preprint=no_report` had that stage's report "
                     "ABSENT (the stage is not enabled for this project, or it crashed).")
        lines.append("")
    for r in rows:
        why_bits = []
        if r.get("stages"):  # sweep's own per-stage trace (typed residual CSV)
            why_bits.append(r["stages"])
        else:
            if r.get("oa_status"):
                why_bits.append(f"oa_status={r['oa_status']}")
            if r.get("error"):
                why_bits.append(f"unpaywall_err={r['error']}")
            if r.get("stage_pmc", "skip") not in ("skip", ""):
                why_bits.append(f"pmc={r['stage_pmc']}")
            if r.get("stage_preprint", "skip") not in ("skip", ""):
                why_bits.append(f"preprint={r['stage_preprint']}")
        if str(r.get("reason", "")).lower().startswith("error in"):
            why_bits.append(r["reason"])
        why = redact("; ".join(why_bits) or "unknown")
        year = f" ({r['year']})" if r.get("year") else ""
        lines.append(f"- [ ] **{_title(r, 200)}**{year} — DOI `{r['doi']}` — {why}")
    lines.append("")
    return "\n".join(lines)


def _is_manual(r):
    """The row is OA-blocked because a preprint needs a person (manual_preprint)."""
    if any(s["detail"].startswith("manual_preprint") for s in r.get("signals", ())):
        return True
    return "manual_preprint" in (r.get("reason") or "").lower()


def doi_url(doi):
    """https://doi.org/ plus the DOI percent-encoded for a URL path (DOI Handbook 2025, 4.7: "The
    percent-encoding algorithm specified at RFC 3986 is applied whenever a DOI name is used in the
    path component of a URL"), after normalising, so a SICI `<...>` or a `#` cannot break the
    markdown link or the browser click. A string that holds no DOI is percent-encoded as it is."""
    try:
        return "https://doi.org/" + _doi.encode_path(doi)
    except ValueError:
        return "https://doi.org/" + quote(str(doi or "").strip(), safe="/")


def _oa_link(r):
    if r.get("landing_url") and _is_manual(r):   # the preprint's landing page (W2b landing_url)
        return r["landing_url"]
    if r.get("pmcid") and any(s["stage"] == "pmc" and s["kind"] is Kind.REFUSED for s in r.get("signals", ())):
        return f"https://pmc.ncbi.nlm.nih.gov/articles/{r['pmcid']}/"
    return doi_url(r["doi"])


def _oa_cause(r):
    """(cause, via) of the first blocking signal: `HTTP_403`, `unpaywall:publisher`;
    `HTML`, `pmc:ncbi-page`; `manual_preprint`, `preprint:osf`."""
    for s in r.get("signals", ()):
        manual = s["detail"].startswith("manual_preprint")
        if manual or (s["kind"] is Kind.REFUSED and s["where"] == "download"):
            parts = s["detail"].split(":" if manual else "/")
            cause = "manual_preprint" if manual else parts[-1]
            host = (parts[1] if len(parts) > 1 else "") if manual else (parts[0] if len(parts) > 1 else "")
            return redact(cause), (f"{s['stage']}:{host}" if host else s["stage"])
    m = _REASON_STAGE.match(r.get("reason") or "")   # a typed row with no report detail
    if m:
        cause = m.group(2).split(";")[0].strip()
        return ("manual_preprint" if "manual_preprint" in cause.lower() else redact(cause)), m.group(1)
    return "unknown", "unknown"


def _n_rows(rows):
    return f"{len(rows)} row" + ("" if len(rows) == 1 else "s")


def render_oa_blocked_block(sweep_date, rows, tag=None):
    if not rows:
        return ""
    label = f"{sweep_date} [{tag}]" if tag else sweep_date
    lines = ["", f"## Sweep {label}, {_n_rows(rows)}", ""]
    for r in rows:
        cause, via = _oa_cause(r)
        year = f" ({r['year']})" if r.get("year") else ""
        lines.append(f"- [ ] **{_title(r)}**{year} [{r['doi']}]({_oa_link(r)}) cause `{cause}` via `{via}`")
    return "\n".join(lines) + "\n"


def identity_evidence(path, stage=""):
    """Where the identity verdict of a flagged file lives: `<stem>.identity.json` (unpaywall,
    preprint), or the identity_* fields of pmc's `<stem>.fulltext.json`. With no stage, the
    sidecar on disk decides."""
    p = str(path)
    stem = p[:-4] if p.lower().endswith(".pdf") else p
    pmc_form = f"{stem}.fulltext.json (identity_* fields)"
    if stage == "pmc":
        return pmc_form
    if not stage and not Path(stem + ".identity.json").exists() and Path(stem + ".fulltext.json").exists():
        return pmc_form
    return f"{stem}.identity.json"


def render_review_block(sweep_date, rows, tag=None):
    if not rows:
        return ""
    label = f"{sweep_date} [{tag}]" if tag else sweep_date
    lines = ["", f"## Sweep {label}, {_n_rows(rows)}", ""]
    for r in rows:
        year = f" ({r['year']})" if r.get("year") else ""
        stage = r.get("identity_stage") or ""
        line = (f"- [ ] **{_title(r)}**{year} DOI `{r['doi']}` flag `{redact(r.get('identity', ''))}` "
                f"stage `{stage or 'unknown'}`")
        for p in r.get("flagged_path") or ():
            line += f" file `{redact(p)}` evidence `{redact(identity_evidence(p, stage))}`"
        lines.append(line)
    return "\n".join(lines) + "\n"


_HEADERS = {
    ILL_NAME: lambda p: (f"# Manual Pull Queue — {p}\n\n"
                         "Papers requiring **manual fetch** (closed-access, paywalled, paper-only). "
                         "NOT consumed by `sweep.py`. Track ILLIAD requests here.\n"),
    OA_BLOCKED_NAME: lambda p: (f"# {p}: open access, blocked from this host\n\n"
                                "Every row is free to read in a browser: a download host refused this "
                                "machine (HTTP 403, an HTML wall, a final 429) or a preprint needs a manual "
                                "click. Each row also sits in `lit_pull_queue.retry_later.csv` for an "
                                "automatic retry.\n\nAccumulating ledger, append only.\n"),
    REVIEW_NAME: lambda p: (f"# {p}: identity review\n\n"
                            "The file served for each row failed the identity check (it named another DOI, "
                            "or it is a supplement). Nothing was moved or deleted: the file stays at its "
                            "`file` path and the verdict is in its `evidence` sidecar. Check the file and "
                            "fetch the version of record.\n\nAccumulating ledger, append only.\n"),
}


# ---------------------------------------------------------------- retry_later
def read_retry_later(project_root):
    """(fieldnames, rows) of the project's retry_later file; ([], []) when absent."""
    p = Path(project_root) / RETRY_LATER_NAME
    if not p.exists():
        return [], []
    with open(p, encoding="utf-8-sig", newline="") as f:
        rdr = csv.DictReader(f)
        return list(rdr.fieldnames or []), list(rdr)


def _run_label(run_id, tag):
    return f"{run_id}:{tag}" if tag else run_id


_EMBARGO_REASON = re.compile(r"\bEMBARGOED\b")


def _is_embargo(r):
    """A TRANSIENT row waiting on an embargo: an EMBARGOED signal from the reports, or sweep's
    typed reason (`<stage>: EMBARGOED until <date>` / `(release date unknown)`)."""
    if any(s["kind"] is Kind.EMBARGOED for s in r.get("signals", ())):
        return True
    return r["residual_class"] == TRANSIENT and bool(r.get("typed")) and bool(
        _EMBARGO_REASON.search(r.get("reason") or ""))


def _not_before(r, today):
    """3 days for OA_BLOCKED, 1 day otherwise; an embargo waits for its release date (sweep's
    not_before, at least 1 day), or 7 days when the date is unknown. A later date the row
    already carries wins. A past date carried through a re-admitted retry queue is stale and
    ignored, or the row would come back every day."""
    given = []
    for g in (r.get("not_before"), r.get("not_before_in")):
        try:
            given.append(datetime.date.fromisoformat(str(g or "").strip()[:10]))
        except ValueError:
            continue
    if r["residual_class"] == OA_BLOCKED:
        nb = today + datetime.timedelta(days=OA_BLOCKED_DAYS)
    elif _is_embargo(r) and not given:
        nb = today + datetime.timedelta(days=EMBARGO_DEFAULT_DAYS)
    else:
        nb = today + datetime.timedelta(days=TRANSIENT_DAYS)
    return max([nb, *given]).isoformat()


def upsert_retry_later(project_root, rows, today, dry_run=False):
    """Append new DOIs and update existing ones in place (dedup by normalised DOI). Rows already in
    the file for other DOIs are kept unchanged and in order, with any extra columns. Carries
    `attempts` and `not_before`. Returns {"appended": n, "updated": m}."""
    fields, existing = read_retry_later(project_root)
    fields = RETRY_FIELDS + [f for f in fields if f not in RETRY_FIELDS]
    index = {}
    for i, e in enumerate(existing):
        index.setdefault(_key(e.get("doi")), i)
    iso = today.isoformat()
    appended = 0
    for r in rows:
        rec = {
            "doi": r["doi"], "title": r.get("title", ""), "authors": r.get("authors", ""),
            "year": r.get("year", ""), "destination": r.get("destination", ""),
            "notes": r.get("notes", ""),
            "residual_class": r["residual_class"], "reason": redact(r.get("reason", "")),
            "not_before": _not_before(r, today), "attempts": r.get("attempts", ""),
            "first_seen": r.get("first_seen") or iso, "last_seen": iso,
            "last_run": _run_label(r["run_id"], r.get("tag")),
        }
        r["not_before"] = rec["not_before"]
        i = index.get(r["doi_norm"])
        if i is None:
            index[r["doi_norm"]] = len(existing)
            existing.append(rec)
            appended += 1
        else:
            old = existing[i]
            rec["first_seen"] = old.get("first_seen") or rec["first_seen"]
            for k in ("title", "authors", "year", "destination", "notes"):
                rec[k] = rec[k] or old.get(k, "")
            existing[i] = {**old, **rec}
    if not dry_run and rows:
        lit_util.atomic_write_csv(str(Path(project_root) / RETRY_LATER_NAME),
                                  [{k: redact(v) for k, v in e.items() if k in fields} for e in existing],
                                  fields)
    return {"appended": appended, "updated": len(rows) - appended}


def _prior_attempts(retry_rows):
    """{doi: (attempts, last_run)} from retry_later, so a re-run of the same run id does not count
    the row twice."""
    out = {}
    for e in retry_rows:
        d = _key(e.get("doi"))
        n = lit_util.coerce_int(e.get("attempts"))
        if n >= out.get(d, (-1, ""))[0]:
            out[d] = (n, (e.get("last_run") or "").strip())
    return out


# ---------------------------------------------------------------- the stage function
def _append_md(md_path, project, blocks, dry_run):
    """Append new blocks to a .md list (header written first when the file is new)."""
    text = "".join(b for b in blocks if b)
    if not text:
        return False
    existing = md_path.read_text(encoding="utf-8") if md_path.exists() else ""
    if not existing:
        existing = _HEADERS[md_path.name](project)
    if not dry_run:
        # RC4: rewrite the whole file atomically (header + prior content + new blocks)
        lit_util.atomic_write_text(str(md_path), existing + redact(text))
    return True


def _new_rows(rows, present):
    out = []
    for r in rows:
        if r["doi_norm"] not in present:
            present.add(r["doi_norm"])
            out.append(r)
    return out


def run(project, run_id=None, tags=None, dry_run=False, skip_preprint=False, use_holdings=True,
        holdmap=None, cfg=None, today=None, artifact_dir=None) -> dict:
    """Route one project's run. Returns {status, project, run_id, chains, counts, written, rows}.
    status: "ok", "nothing" (no artifacts or no residuals), "config" (aborted, nothing written),
    or "error". A chain with sweep's typed residual CSV routes by its `residual_class` (W1-D1);
    a legacy chain is classified here from its stage reports."""
    cfg = cfg if cfg is not None else lit_util.load_projects_config(CONFIG_PATH)
    projects = cfg.get("projects", {})
    res = {"status": "ok", "project": project, "run_id": run_id, "chains": [], "counts": {},
           "written": {}, "rows": 0, "dry_run": bool(dry_run)}
    if project not in projects:
        return {**res, "status": "error", "error": f"'{project}' not in projects.json"}
    pcfg = projects[project] or {}
    project_root = project_dir(project, pcfg)
    if not project_root.exists():
        return {**res, "status": "error", "error": f"project root missing: {project_root}"}
    art_dir = resolve_artifact_dir(project_root, artifact_dir)
    run_id = run_id or latest_sweep_date(art_dir)
    res["run_id"] = run_id
    if not run_id:
        print(f"[--] no sweep artifacts found at {art_dir.name}; nothing to migrate")
        return {**res, "status": "nothing"}
    # not_before counts from the later of the clock and the run's own date, so sweep --date and
    # migrate stay in lockstep (a run dated ahead of the clock never re-admits its rows the same day)
    today = today or max(datetime.date.today(), datetime.date.fromisoformat(run_id[:10]))

    def chain_rows(tag):
        if tag is None and art_dir == project_root:
            return read_report_chain(art_dir, run_id)   # the historical two-argument call shape
        return read_report_chain(art_dir, run_id, tag, project_dir=project_root)

    chains = list(tags) if tags else [None] + find_tags(art_dir, run_id)
    by_chain, typed_chains = {}, set()
    for tag in chains:
        typed = read_residual(art_dir, run_id, tag, project_dir=project_root)
        if typed is None:
            by_chain[tag] = chain_rows(tag)
        else:
            typed_chains.add(tag)
            by_chain[tag] = _enrich(typed, chain_rows(tag)) if typed else typed
    rows = [r for rs in by_chain.values() for r in rs]
    res["chains"] = [t or "" for t in chains]
    res["rows"] = len(rows)
    if not rows:
        print(f"[--] {project} sweep {run_id}: no closed/failed residuals")
        return {**res, "status": "nothing"}
    for tag, rs in by_chain.items():
        for r in rs:
            r.setdefault("tag", tag)
            r.setdefault("run_id", run_id)
            r.setdefault("doi_norm", _key(r.get("doi")))

    legacy = [r for r in rows if r.get("tag") not in typed_chains]
    if legacy:
        try:
            sources = config.sources(project, cfg=cfg)
        except config.ConfigError as e:
            print(f"[ERR] {project}: {e}", file=sys.stderr)
            return {**res, "status": "config", "error": str(e)}
        if skip_preprint:
            sources -= PREPRINT_SOURCES
        default_lib = lit_util.lib_paths(project, pcfg)[1] if pcfg.get("lib_dir") else None
        own_libs = {_norm_lib(default_lib)} if default_lib is not None else set()
        if holdmap is None and use_holdings:
            try:
                holdmap = holdings.build(registry=cfg, write_cache=not dry_run)
            except Exception as e:  # noqa: BLE001 -- routing still works without the map; say so
                print(f"[WARN] holdings map unavailable ({type(e).__name__}: {redact(e)}); "
                      f"held papers may be routed as residuals", file=sys.stderr)
                holdmap = None
        prior = _prior_attempts(read_retry_later(project_root)[1])
        for r in legacy:
            n_old, last_run = prior.get(r["doi_norm"], (0, ""))
            if last_run == _run_label(run_id, r.get("tag")):
                attempts = max(n_old, 1)            # this run was already counted
            else:
                attempts = max(n_old, r.get("prior_attempts") or 0) + 1
            classify_row(r, holdmap=holdmap, own_libs=own_libs, default_lib=default_lib,
                         sources=sources, attempts=attempts)
    for r in rows:
        if r["residual_class"] not in ROUTE_OF:
            print(f"[WARN] {project}: unknown residual class {r['residual_class']!r} for "
                  f"{r['doi']}; not routed", file=sys.stderr)

    bad = [r for r in rows if has_config_signal(r) or r["residual_class"] == CONFIG]
    bad_chains = {r.get("tag") for r in bad}
    if bad and len(bad_chains) < len(by_chain):
        print(f"[ERR] {project} sweep {run_id}: {len(bad)} row(s) carry a CONFIG outcome; chain(s) "
              f"{sorted(t or '(untagged)' for t in bad_chains)} not routed, the others are",
              file=sys.stderr)
        by_chain = {t: rs for t, rs in by_chain.items() if t not in bad_chains}
        rows = [r for rs in by_chain.values() for r in rs]
        res["status"] = "config"
        bad = []
    if bad:
        print(f"[ERR] {project} sweep {run_id}: {len(bad)} row(s) carry a CONFIG outcome "
              f"(first: {redact(bad[0]['reason'] or _reason(bad[0]['signals']))}); the run aborts and "
              f"nothing is routed. Fix the configuration and re-run.", file=sys.stderr)
        return {**res, "status": "config", "counts": _counts(rows)}

    res["counts"] = _counts(rows)
    written = res["written"]

    # ILL list, browser worklist, review list: dedup against the file, in any DOI form (REG-I25)
    for name, cls, render in ((ILL_NAME, TERMINAL_CLOSED, None),
                              (OA_BLOCKED_NAME, OA_BLOCKED, render_oa_blocked_block),
                              (REVIEW_NAME, IDENTITY_FLAG, render_review_block)):
        md_path = project_root / name
        present = existing_dois(md_path.read_text(encoding="utf-8")) if md_path.exists() else set()
        blocks, n = [], 0
        for tag, rs in by_chain.items():
            new = _new_rows([r for r in rs if r["residual_class"] == cls], present)
            n += len(new)
            if new:
                blocks.append(render_md_block(project, run_id, new, tag) if render is None
                              else render(run_id, new, tag))
        if _append_md(md_path, project, blocks, dry_run):
            written[name] = n

    retry = [r for r in rows if r["residual_class"] in (TRANSIENT, OA_BLOCKED)]
    if retry:
        up = upsert_retry_later(project_root, retry, today, dry_run=dry_run)
        written[RETRY_LATER_NAME] = f"{up['appended']} appended, {up['updated']} updated"

    for tag, rs in by_chain.items():
        if not rs:
            continue
        path = artifact_path(art_dir, run_id, "routing", tag)
        out = [{
            "doi": r["doi"], "title": r.get("title", ""), "year": r.get("year", ""),
            "residual_class": r["residual_class"], "route": ROUTE_OF.get(r["residual_class"], "none (unknown class)"),
            "reason": r.get("reason", ""), "held_paths": " | ".join(r.get("held_paths") or ()),
            "not_before": r.get("not_before", ""), "attempts": r.get("attempts", ""),
            "stage_unpaywall": r.get("stage_unpaywall", ""), "stage_pmc": r.get("stage_pmc", ""),
            "stage_preprint": r.get("stage_preprint", ""),
            "flagged_path": " | ".join(r.get("flagged_path") or ()), "landing_url": r.get("landing_url", ""),
        } for r in rs]
        if not dry_run:
            lit_util.atomic_write_csv(str(path), [{k: redact(v) for k, v in o.items()} for o in out],
                                      ROUTING_FIELDS)
        written[path.name] = len(out)

    verb = "would write" if dry_run else "wrote"
    summary = ", ".join(f"{k}={v}" for k, v in sorted(res["counts"].items()))
    print(f"[OK] {project} sweep {run_id}: {len(rows)} residual row(s): {summary}")
    for name, n in written.items():
        print(f"     {verb} {n if isinstance(n, str) else f'{n} row(s)'} to {name}")
    return res


def _counts(rows):
    c = {}
    for r in rows:
        c[r["residual_class"]] = c.get(r["residual_class"], 0) + 1
    return c


def main(argv=None):
    ap = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    ap.add_argument("--project", required=True)
    ap.add_argument("--date", "--run-id", dest="date", default=None,
                    help="run id: YYYY-MM-DD (legacy dated runs) or YYYY-MM-DD.N; defaults to the latest run")
    ap.add_argument("--tag", action="append", default=None,
                    help="route only this tagged chain (repeatable); default: the untagged chain and every tag")
    ap.add_argument("--skip-preprint", action="store_true",
                    help="the preprint stage was skipped on purpose: a missing preprint report blocks nothing")
    ap.add_argument("--no-holdings", action="store_true",
                    help="do not build the portfolio holdings map (held papers may route as residuals)")
    ap.add_argument("--artifact-dir", default=None,
                    help="where the run's artifacts are (sweep --artifact-dir: absolute, or relative "
                         "to the project); the .md lists and retry_later stay in the project root")
    ap.add_argument("--dry-run", action="store_true", help="classify and print; write nothing")
    args = ap.parse_args(argv)
    if args.date and not re.fullmatch(RUN_ID_RE, args.date):
        print(f"[ERR] --date must be YYYY-MM-DD or YYYY-MM-DD.N, got {args.date!r}", file=sys.stderr)
        return 2
    bad_tags = [t for t in args.tag or () if not re.fullmatch(TAG_RE, t)]
    if bad_tags:
        print(f"[ERR] invalid --tag {bad_tags}", file=sys.stderr)
        return 2

    cfg = lit_util.load_projects_config(CONFIG_PATH)
    if args.project not in cfg.get("projects", {}):
        print(f"[ERR] '{args.project}' not in projects.json", file=sys.stderr)
        return 2
    res = run(args.project, run_id=args.date, tags=args.tag, dry_run=args.dry_run,
              skip_preprint=args.skip_preprint, use_holdings=not args.no_holdings, cfg=cfg,
              artifact_dir=args.artifact_dir)
    if res["status"] == "error":
        print(f"[ERR] {res['error']}", file=sys.stderr)
        return 2
    if res["status"] == "config":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
