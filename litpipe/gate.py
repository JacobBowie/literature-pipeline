"""Spec-driven pool-selection gate over a scoped forward harvest (schema `litpipe.gate/1`).

A scoped forward walk (`forward_citations.py --seeds-from ... --scope NAME`) writes
`<lib>/_<NAME>_forward_citations.csv`: one row per (seed, citing paper). The gate turns that
harvest into a ranked candidate pool and a topic-balanced selection of `target` rows, drawn down
later by `python -m litpipe.runner batch --pool <selection CSV>`. Everything a project decides
(topic matchers, controls, quotas, lanes, budgets) is DATA in a JSON spec; this module is the one
engine. It sends no network request.

CLI (dry by default: without --write nothing is written anywhere)
  python -m litpipe.gate build|plan|report --project KEY --spec PATH [--lib-dir DIR]
                         [--holdings-as-of FILE] [--write] [--out-root DIR] [--json PATH]
  python -m litpipe.gate plan|report ... --pool CSV     plan from a recorded candidate pool
  python -m litpipe.gate promote --project KEY --spec PATH --run RUN_ID [--force]
                         [--lib-dir DIR] [--out-root DIR]
  build    controls, collapse, evaluate, rank: prints the funnel; --write keeps pool.csv,
           drops.csv, report.json, report.md, manifest.json in a new run folder.
  plan     build, then the quota plan, the order, the lanes and the invariants; --write also keeps
           selection.csv and lane_<name>.csv. With --pool CSV the plan starts from that pool
           (row order = rank order; columns `doi`, `n_seeds`, `topics`, the rest optional), with no
           re-evaluation, no held and no drawn filter; lanes that set `years` need the harvest and
           are skipped.
  report   what plan computes, printed as the full report (report.md).
  promote  copy a run's selection.csv and lane CSVs to the stable Pool paths
           `<lib>/_<name>_gate_selection.csv` and `<lib>/_<name>_gate_lane_<lane>.csv` (atomic).
           It refuses while the stable path's Pool has pending batches (`--force` overrides), and
           when two promoted files of the project would give the same `runner batch` tag.
  --lib-dir DIR       the library to use instead of the registry's (spec paths resolve against it).
  --holdings-as-of F  replay: the held set is the DOIs in F (one per line; only a WHOLE line that
                      starts with `#` is a comment, since a SICI DOI can end in `#`), matched as
                      exact strings against each record's DOI key. Never a spec field.
  --out-root DIR      the parent of `_gate/` instead of the library.
  --json PATH         also write the report as JSON to PATH (written even in a dry run).

Exit codes
  0  done.
  1  usage, registry or spec error: unknown keys, duplicate JSON keys, bad enums, unsupported
     switch combinations, dangling references, missing inputs, an unreadable Pool state; a
     promote whose files would share a runner batch tag.
  2  refused: a control failed; a sanity abort (zero candidates, no off-topic drops); a zero input
     (the harvest has 0 rows, 0 records survive the collapse, an as-of snapshot holds 0 DOIs, live
     holdings for the scope give fewer than 4 DOIs); an impossible trim; an invariant failure; a
     promote while the stable Pool has pending batches. A `[step-summary] {json}` line ends stdout.

Run model
  Each `--write` creates an immutable run folder `<lib or --out-root>/_gate/<name>/<run_id>/`
  (run_id = UTC time plus a content hash; it appears in the folder name only). Files: pool.csv
  (candidates, rank order), selection.csv and lane_<name>.csv (Pool CSVs: `doi` first, row order =
  draw order), drops.csv (every dropped record with ALL its reasons), report.json, report.md,
  manifest.json (sha256 of the spec, harvest and pool; the holdings mode with the snapshot hash or
  the live set's hash and size; the drawn-set hash; the engine version; the CLI). Files use `\\n`
  line endings and UTF-8 without BOM and carry no clock, so two runs on the same inputs write
  byte-identical files. selection.csv and lane CSV columns: doi, order, title, authors, year,
  venue, cited_by, n_seeds, topics (every matching topic, spec order, ';'-joined), assigned_topic
  (the bucket the row was taken for; `_tier` for tier rows; a lane row's first lane topic),
  topic_chapter, chapters (seed chapters), seeds, pool_rank, lane (`main` or the lane name), one
  column per annotation, notes. pool.csv: pool_rank, doi, title, authors, year, venue, cited_by,
  n_seeds, topics, chapters, seeds, first_seen, annotations. drops.csv: doi, title, year,
  n_seeds, primary_reason, reasons, stage (build, plan or invariant). There is no held
  column: held is computed live. `promote` publishes a run to the stable Pool paths; their
  `.drawdown.json` (litpipe.worklists) persists across promotes. A rebuild reads the drawn set
  (every DOI staged or swept in the `.drawdown.json` of `_<name>_gate_selection.csv` and of each
  `_<name>_gate_lane_*.csv`), drops those records with reason `drawn`, and plans a fresh target.

Algorithm (pure core: (spec, harvest rows, held, drawn) -> result; I/O at the edges)
  1. Load the spec strictly (below). 2. Compile the matchers; a pattern is never rewritten.
  3. Run the controls; any failure exits 2. With live holdings also: the scope holds at least 4
     DOIs, the DO lines of the first 4 `.ris` files (sorted) in the project library are in
     `HoldMap.records()`, and `10.9999/this-doi-does-not-exist` is not.
  4. Read the drawn set. 5. Read the harvest (`utf-8-sig`, `newline=''`, unlimited field size).
  6. Collapse to one record per DOI key (first appearance = `first_seen`). 7. Evaluate every
     record and keep EVERY reason: `year` / `year_unparsed`, `no_title`, `missing:<facet>` or
     `no_topic`, `excluded:<name>`, `held`, `drawn`. A candidate has no reason.
  8. Sanity: zero candidates, or no record failing inclusion, exit 2 (switchable).
  9. Rank by the explicit tuple (rank keys..., first_seen); `pool_rank` from 1.
  10. Plan: the seed tier (`tier_take_all`), then `rarest_match` + `declared` caps or
     `fill_order_claim` + `equal_share` caps; the protected trim (half-to-even `Fraction`
     rounding, take-all and tier rows never cut, exit 2 when the capped rows cannot absorb the
     overflow, the undershoot top-up); the order (`round_robin` over the assigned buckets, or
     `rank`: a stable sort of the allocation order by the rank keys only). Plan-stage reasons:
     `over_cap:<t>`, `no_cap:<t>`, `trimmed:<t>`, `quota_full:<t>`, `no_quota_topic`.
  11. Lanes, in array order: steps 6 to 9 again when `lane.years` is set (replacing `years`; the
     zero-input and zero-candidate refusals apply, the off-topic check does not, since the main
     build already proved the matcher discriminates), else the main candidates; those whose topic
     set meets `lane.topics`, minus the main selection, earlier lanes and the drawn set, sorted by
     (lane rank or rank, first_seen), the first `size`.
  12. Invariants (exit 2 on failure): the order is a permutation of the allocation; every take-all
     and tier row is present; taken <= cap; |selection| <= target; DOIs distinct under the spec
     key and under `litpipe.holdings.doi_key` (a collision keeps the first row and is logged as
     `pool_key_collision`, so `Pool.rows()` never drops a row silently).
  13-15. Write (with --write), promote, report (computed live, never fed back as input).

Schema `litpipe.gate/1` (field by field; unknown keys and duplicate JSON keys are errors, arrays
carry the load-bearing order, every pattern is applied with `re.search` and IGNORECASE to the
cleaned title, and the engine never rewrites a pattern)
  schema          "litpipe.gate/1" (required).
  name            the gate name, [a-z0-9][a-z0-9_-]* (required); names the run folders and the
                  stable paths.
  description     str, default "".
  provenance, recorded_run   objects passed through untouched (default {}).
  input.harvest   a path (relative to the library) or {"scope": NAME}, meaning
                  `<lib>/_<NAME>_forward_citations.csv` (required).
  input.seeds_manifest   path or null (default null): the per-seed status report reads it. Two
                  shapes: a JSON object keyed by seed DOI whose values carry `status` (and
                  optionally `label`), or a forward_citations journal (`.partial.jsonl`); else every
                  seed is UNKNOWN.
  input.columns   {doi, title, year, authors, venue, cited_by}: harvest column names (defaults
                  citing_doi, citing_title, citing_year, citing_authors, citing_venue,
                  citing_cited_by); `seed`: a list, the first non-empty value (stripped, case kept)
                  is the row's seed key and n_seeds counts distinct keys; a row with no seed key adds
                  none (counted as rows_blank_seed) (default ["seed_doi"]); `seed_chapter`: str or
                  null (default "seed_chapter"), its values are unioned per record.
                  doi, title, year, cited_by and one seed column must exist in the harvest.
  input.doi_key   the collapse key: "lower" (strip, lower-case; default) or
                  "lower_unresolve_rstrip" (also drops a doi.org resolver prefix and trailing
                  " .,;"). Never a pipeline normaliser: those merge distinct SICI and suffixed DOIs.
  collapse.record_row   "first" (the first kept row of the DOI) or "first_with_title" (the first
                  row with a title; default) supplies title, authors, venue, year and, under
                  `cited_by: record_row`, cited_by.
  collapse.fill_empty   subset of [title, authors, venue, year] filled from the first later row
                  that has one (default []).
  collapse.cited_by     "record_row" or "max" over rows, non-integers skipped (default "max").
                  Numbers are parsed with lit_util.coerce_int (unparsed counts as 0).
  collapse.text   subset of [html_unescape, strip_tags, collapse_whitespace] applied to title,
                  authors and venue (strip_tags, then html.unescape, then whitespace), then strip
                  (always). Default: all three.
  years           null (default) or {min, max (int or null), unparsed: "drop" (default) | "keep",
                  scope: "record" (default; the record's year) | "row" (each harvest row is kept or
                  dropped before the collapse)}. Bounds are inclusive.
  topics[]        matchers (at least one), each with an optional `chapter` (str or null).
  require[]       matchers; every one must match (default []: at least one topic must match).
  exclude[]       matchers; any match drops the record (default []).
    A matcher is {name, substrings?, phrases?, regex?, controls?: {hit: [T], miss: [T]},
    provenance?} with at least one pattern. substrings compile as re.escape(s), unanchored;
    phrases as (?<![a-z]) + re.escape(p) + (?![a-z]) (letter-bounded); regex as given. A pattern
    that matches the empty title is refused. T is a title string or {title, source}. Names are
    [a-z0-9][a-z0-9_]* and unique within their list.
  controls        {match_none: [T] (match no topic), exclude_none: [T] (match no exclude),
                  admit: [T] (match every require facet and no exclude, or with no require at
                  least one topic), contentless: bool (default true: a contentless title passes no
                  inclusion), minimum: "gate" (default: at least one positive control, a topic or
                  require `hit` or `admit`, and one negative, `match_none` or a `miss`) | "none"
                  (accepted only when the spec sets it; a warning is printed and reported),
                  require_offtopic_drops: bool (default true), require_nonempty_pool: bool
                  (default true)}.
  holdings        {filter: bool (default true), scope: "portfolio" (default) | "project"}. Live
                  runs use litpipe.holdings.build(...).where(doi) (content holdings only; `project`
                  keeps holdings whose Holding.project is --project). Replay injects a snapshot.
  rank            [{field: n_seeds | cited_by | year, order: desc | asc}], default n_seeds desc,
                  cited_by desc, year desc; an unparsed year ranks as 0. The final tiebreak is
                  fixed: first appearance in the harvest.
  quotas.target   int >= 1 (required): the most rows the selection holds.
  quotas.tier_take_all   null (default) or {min_seeds}: candidates with n_seeds >= min_seeds are
                  taken whole, topic-blind, ahead of the quota.
  quotas.assign   "rarest_match" (default: each row goes to the rarest of its topics by tag
                  count over the pool, ties by name) | "fill_order_claim" (topics claim rows in
                  fill order; a row already claimed is skipped and does not use up a cap).
  quotas.caps     "declared" (default) | "equal_share" (each topic, scarcest first with ties in
                  spec order, gets min(available, remaining // topics_left)).
  quotas.declared [{topic, cap: int >= 0 | "take_all"}] in order (required under declared).
  quotas.default_cap   int or null (default null): the cap of a topic missing from `declared`;
                  with null every topic must be declared.
  quotas.allocation_order   "bucket_size_desc" (default; ties by first appearance in the pool) |
                  "cap_desc" (declared order, take_all first, stable on array order).
    Supported combinations (v1; the loader rejects the rest, naming the combination):
    (rarest_match, declared, no tier) with round_robin or rank; (fill_order_claim, equal_share,
    tier optional) with rank or round_robin.
  chapter_budgets null or {chapter: int}: report only (taken per topic chapter against budget).
  order           {method: "round_robin" (default) | "rank", bucket_order: "size_asc" (default)
                  | "size_desc" (round_robin only)}. round_robin: each round emits the next row of
                  every non-empty bucket; tier rows form the bucket "_tier".
  lanes[]         {name, topics: [topic], size: int >= 1, years?, rank?}; the name "main" is
                  reserved for the main selection.
  annotate[]      {column, dois_from (path), doi_column (default "doi"), comment_prefix (default
                  "#")}: a y/blank column for records whose DOI key is listed in the file.
  reports.direction_split[]   {topic, sides: [{name, words: [str]}] (two or more), match:
                  "substring"}: over the selected rows assigned to `topic`, a row counts for a
                  side when only that side's words occur in its lower-cased title, else unstated;
                  a warning when a side is empty.
  output.notes_template   default "gate={gate}; lane={lane}; topic={assigned_topic};
                  n_seeds={n_seeds}; ch={chapters}" (also: topics, cited_by, year,
                  topic_chapter, pool_rank, order); fills the `notes` column `runner batch` copies.
                  `output.destination` was dropped: the runner always uses the registry library.
"""
from __future__ import annotations

import argparse
import csv
import datetime as _dt
import hashlib
import html
import io
import json
import os
import re
import string
import sys
from collections import Counter
from dataclasses import dataclass, field
from fractions import Fraction
from pathlib import Path

import lit_util
from litpipe import config
from litpipe import holdings as _holdings
from litpipe import text as _text

SCHEMA = "litpipe.gate/1"
ENGINE_VERSION = "1"
EXIT_OK, EXIT_USAGE, EXIT_REFUSED = 0, 1, 2
CONTENTLESS_TITLE = "An investigation of the phenomenon under consideration"
ABSENT_DOI = "10.9999/this-doi-does-not-exist"
LIVE_MIN_DOIS = 4
TAKE_ALL = "take_all"
MAIN_LANE = "main"
TIER_BUCKET = "_tier"
GATE_DIR = "_gate"
SUMMARY_MARKER = "[step-summary]"

_GATE_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_]{0,63}$")
_COLUMN_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_SCOPE_RE = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
_RESOLVER = re.compile(r"^(https?://)?(dx\.)?doi\.org/")
_RIS_DO = re.compile(r"(?m)^DO\s+-\s+(\S+)")

TEXT_OPS = ("html_unescape", "strip_tags", "collapse_whitespace")
FILL_FIELDS = ("title", "authors", "venue", "year")
RANK_FIELDS = ("n_seeds", "cited_by", "year")
DEFAULT_RANK = [{"field": "n_seeds", "order": "desc"}, {"field": "cited_by", "order": "desc"},
                {"field": "year", "order": "desc"}]
DEFAULT_COLUMNS = {"doi": "citing_doi", "title": "citing_title", "year": "citing_year",
                   "authors": "citing_authors", "venue": "citing_venue", "cited_by": "citing_cited_by",
                   "seed": ["seed_doi"], "seed_chapter": "seed_chapter"}
DEFAULT_NOTES = "gate={gate}; lane={lane}; topic={assigned_topic}; n_seeds={n_seeds}; ch={chapters}"
NOTES_FIELDS = ("gate", "lane", "assigned_topic", "topics", "n_seeds", "chapters", "cited_by", "year",
                "topic_chapter", "pool_rank", "order")
SUPPORTED = (("rarest_match", "declared", False), ("fill_order_claim", "equal_share", False),
             ("fill_order_claim", "equal_share", True))

POOL_COLUMNS = ["pool_rank", "doi", "title", "authors", "year", "venue", "cited_by", "n_seeds", "topics",
                "chapters", "seeds", "first_seen"]
SELECTION_COLUMNS = ["doi", "order", "title", "authors", "year", "venue", "cited_by", "n_seeds", "topics",
                     "assigned_topic", "topic_chapter", "chapters", "seeds", "pool_rank", "lane"]
DROP_COLUMNS = ["doi", "title", "year", "n_seeds", "primary_reason", "reasons", "stage"]


# ---------------------------------------------------------------- errors
class GateError(Exception):
    """A usage, registry, spec or input error: exit 1."""
    exit_code = EXIT_USAGE


class SpecError(GateError):
    """The spec cannot be used as written: exit 1."""


class GateAbort(GateError):
    """A control, sanity, zero-input, trim or invariant refusal: exit 2."""
    exit_code = EXIT_REFUSED

    def __init__(self, message, reasons=None, partial=None):
        super().__init__(message)
        self.reasons = list(reasons or [message])
        self.partial = partial or {}


# ---------------------------------------------------------------- small helpers
def _int(v, default):
    """lit_util.coerce_int, which also survives 'inf' (an OverflowError there)."""
    try:
        return lit_util.coerce_int(v, default)
    except (OverflowError, ValueError, TypeError):
        return default


def _sha256_bytes(b):
    return hashlib.sha256(b).hexdigest()


def _sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _set_hash(keys):
    return _sha256_bytes("\n".join(sorted(keys)).encode("utf-8"))


def _unlimited_fields():
    lim = sys.maxsize
    while True:
        try:
            csv.field_size_limit(lim)
            return
        except OverflowError:
            lim //= 10


def doi_key_fn(mode):
    """The spec's collapse key: `lower` strips and lower-cases; `lower_unresolve_rstrip` also drops a
    doi.org resolver prefix and trailing ' .,;'."""
    if mode == "lower":
        return lambda s: str(s or "").strip().lower()

    def key(s):
        d = _RESOLVER.sub("", str(s or "").strip().lower())
        return d.rstrip(" .,;")
    return key


def _clean_fn(ops):
    ops = frozenset(ops)

    def clean(s):
        s = "" if s is None else str(s)
        if "strip_tags" in ops:
            s = _text.strip_tags(s)
        if "html_unescape" in ops:
            s = html.unescape(s)
        if "collapse_whitespace" in ops:
            s = " ".join(s.split())
        return s.strip()
    return clean


def _csv_text(columns, rows):
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=columns, lineterminator="\n", extrasaction="ignore")
    w.writeheader()
    for r in rows:
        w.writerow({c: r.get(c, "") for c in columns})
    return buf.getvalue()


def _json_text(obj):
    return json.dumps(obj, indent=1, sort_keys=True, ensure_ascii=False) + "\n"


# ---------------------------------------------------------------- spec: strict loader
def _pairs_hook(pairs):
    out = {}
    for k, v in pairs:
        if k in out:
            raise SpecError(f"duplicate JSON key {k!r}")
        out[k] = v
    return out


def _bad_constant(name):
    raise SpecError(f"JSON constant {name} is not allowed")


def parse_spec_text(text):
    """The spec as a dict; duplicate keys and NaN/Infinity are SpecErrors."""
    try:
        data = json.loads(text, object_pairs_hook=_pairs_hook, parse_constant=_bad_constant)
    except SpecError:
        raise
    except ValueError as e:
        raise SpecError(f"spec is not valid JSON: {e}") from None
    if not isinstance(data, dict):
        raise SpecError("spec must be a JSON object")
    return data


def _obj(v, where, allowed, required=()):
    if not isinstance(v, dict):
        raise SpecError(f"{where} must be an object, got {type(v).__name__}")
    unknown = sorted(set(v) - set(allowed))
    if unknown:
        if where == "output" and "destination" in unknown:
            raise SpecError("output.destination was dropped from litpipe.gate/1: runner batch always "
                            "writes the registry library")
        raise SpecError(f"{where}: unknown key(s) {', '.join(map(repr, unknown))}; allowed: "
                        f"{', '.join(sorted(allowed))}")
    missing = [k for k in required if k not in v]
    if missing:
        raise SpecError(f"{where}: missing required key(s) {', '.join(map(repr, missing))}")
    return v


def _str(v, where, *, empty=False):
    if not isinstance(v, str):
        raise SpecError(f"{where} must be a string, got {type(v).__name__}")
    if not empty and not v.strip():
        raise SpecError(f"{where} must not be empty")
    return v


def _intv(v, where, *, minimum=None, null=False):
    if v is None and null:
        return None
    if isinstance(v, bool) or not isinstance(v, int):
        raise SpecError(f"{where} must be an integer{' or null' if null else ''}, got {v!r}")
    if minimum is not None and v < minimum:
        raise SpecError(f"{where} must be >= {minimum}, got {v}")
    return v


def _boolv(v, where):
    if not isinstance(v, bool):
        raise SpecError(f"{where} must be true or false, got {v!r}")
    return v


def _enum(v, where, allowed):
    if v not in allowed:
        raise SpecError(f"{where} must be one of {', '.join(map(repr, allowed))}, got {v!r}")
    return v


def _listv(v, where, *, min_len=0):
    if not isinstance(v, list):
        raise SpecError(f"{where} must be an array, got {type(v).__name__}")
    if len(v) < min_len:
        raise SpecError(f"{where} needs at least {min_len} entr{'y' if min_len == 1 else 'ies'}")
    return v


def _subset(v, where, allowed):
    _listv(v, where)
    for i, x in enumerate(v):
        _enum(x, f"{where}[{i}]", allowed)
    if len(set(v)) != len(v):
        raise SpecError(f"{where} lists a value twice")
    return list(v)


def _name(v, where, rx=_NAME_RE):
    _str(v, where)
    if not rx.match(v):
        raise SpecError(f"{where} {v!r} must match {rx.pattern}")
    return v


def _titles(v, where):
    out = []
    for i, t in enumerate(_listv(v, where)):
        w = f"{where}[{i}]"
        if isinstance(t, dict):
            _obj(t, w, {"title", "source"}, required=("title",))
            _str(t["title"], f"{w}.title")
            if "source" in t:
                _str(t["source"], f"{w}.source", empty=True)
            out.append(dict(t))
        else:
            out.append(_str(t, w))
    return out


def _title_of(t):
    return t["title"] if isinstance(t, dict) else t


def _norm_matcher(m, where, *, topic=False):
    allowed = {"name", "substrings", "phrases", "regex", "controls", "provenance"}
    if topic:
        allowed.add("chapter")
    _obj(m, where, allowed, required=("name",))
    out = {"name": _name(m["name"], f"{where}.name")}
    n = 0
    for kind in ("substrings", "phrases", "regex"):
        pats = _listv(m.get(kind, []), f"{where}.{kind}")
        for i, p in enumerate(pats):
            _str(p, f"{where}.{kind}[{i}]")
        out[kind] = list(pats)
        n += len(pats)
    if not n:
        raise SpecError(f"{where} ({out['name']}) has no pattern: give substrings, phrases or regex")
    c = _obj(m.get("controls", {}), f"{where}.controls", {"hit", "miss"})
    out["controls"] = {"hit": _titles(c.get("hit", []), f"{where}.controls.hit"),
                       "miss": _titles(c.get("miss", []), f"{where}.controls.miss")}
    if topic:
        ch = m.get("chapter")
        out["chapter"] = None if ch is None else _str(ch, f"{where}.chapter")
    if "provenance" in m:
        out["provenance"] = m["provenance"]
    return out


def _norm_years(v, where):
    if v is None:
        return None
    _obj(v, where, {"min", "max", "unparsed", "scope"})
    out = {"min": _intv(v.get("min"), f"{where}.min", null=True),
           "max": _intv(v.get("max"), f"{where}.max", null=True),
           "unparsed": _enum(v.get("unparsed", "drop"), f"{where}.unparsed", ("drop", "keep")),
           "scope": _enum(v.get("scope", "record"), f"{where}.scope", ("record", "row"))}
    if out["min"] is not None and out["max"] is not None and out["min"] > out["max"]:
        raise SpecError(f"{where}: min {out['min']} is above max {out['max']}")
    return out


def _norm_rank(v, where):
    out, seen = [], set()
    for i, k in enumerate(_listv(v, where)):
        w = f"{where}[{i}]"
        _obj(k, w, {"field", "order"}, required=("field",))
        f = _enum(k["field"], f"{w}.field", RANK_FIELDS)
        if f in seen:
            raise SpecError(f"{where} names {f!r} twice")
        seen.add(f)
        out.append({"field": f, "order": _enum(k.get("order", "desc"), f"{w}.order", ("desc", "asc"))})
    return out


def _norm_input(v):
    _obj(v, "input", {"harvest", "seeds_manifest", "columns", "doi_key"}, required=("harvest",))
    h = v["harvest"]
    if isinstance(h, dict):
        _obj(h, "input.harvest", {"scope"}, required=("scope",))
        s = _str(h["scope"], "input.harvest.scope")
        if not _SCOPE_RE.match(s):
            raise SpecError(f"input.harvest.scope {s!r} must match {_SCOPE_RE.pattern}")
        harvest = {"scope": s}
    else:
        harvest = _str(h, "input.harvest")
    sm = v.get("seeds_manifest")
    cols_in = _obj(v.get("columns", {}), "input.columns", set(DEFAULT_COLUMNS))
    cols = {}
    for k in ("doi", "title", "year", "authors", "venue", "cited_by"):
        cols[k] = _str(cols_in.get(k, DEFAULT_COLUMNS[k]), f"input.columns.{k}")
    seed = cols_in.get("seed", DEFAULT_COLUMNS["seed"])
    _listv(seed, "input.columns.seed", min_len=1)
    cols["seed"] = [_str(s, f"input.columns.seed[{i}]") for i, s in enumerate(seed)]
    sc = cols_in.get("seed_chapter", DEFAULT_COLUMNS["seed_chapter"])
    cols["seed_chapter"] = None if sc is None else _str(sc, "input.columns.seed_chapter")
    return {"harvest": harvest,
            "seeds_manifest": None if sm is None else _str(sm, "input.seeds_manifest"),
            "columns": cols,
            "doi_key": _enum(v.get("doi_key", "lower"), "input.doi_key", ("lower", "lower_unresolve_rstrip"))}


def _norm_quotas(v, topic_names):
    _obj(v, "quotas", {"target", "tier_take_all", "assign", "caps", "declared", "default_cap",
                       "allocation_order"}, required=("target",))
    q = {"target": _intv(v["target"], "quotas.target", minimum=1)}
    tier = v.get("tier_take_all")
    if tier is not None:
        _obj(tier, "quotas.tier_take_all", {"min_seeds"}, required=("min_seeds",))
        tier = {"min_seeds": _intv(tier["min_seeds"], "quotas.tier_take_all.min_seeds", minimum=1)}
    q["tier_take_all"] = tier
    q["assign"] = _enum(v.get("assign", "rarest_match"), "quotas.assign", ("rarest_match", "fill_order_claim"))
    q["caps"] = _enum(v.get("caps", "declared"), "quotas.caps", ("declared", "equal_share"))
    combo = (q["assign"], q["caps"], tier is not None)
    if combo not in SUPPORTED:
        raise SpecError(f"unsupported combination: quotas.assign {q['assign']!r} with quotas.caps "
                        f"{q['caps']!r}{' and a tier_take_all' if tier is not None else ' without a tier'}; "
                        f"v1 supports (rarest_match, declared, no tier) and (fill_order_claim, equal_share, "
                        f"tier optional)")
    if q["caps"] == "equal_share":
        for k in ("declared", "default_cap", "allocation_order"):
            if k in v:
                raise SpecError(f"unsupported combination: quotas.{k} does not apply to quotas.caps "
                                f"'equal_share'")
        q.update(declared=[], default_cap=None, allocation_order=None)
        return q
    if "declared" not in v:
        raise SpecError("quotas.declared is required when quotas.caps is 'declared'")
    declared, seen = [], set()
    for i, d in enumerate(_listv(v["declared"], "quotas.declared")):
        w = f"quotas.declared[{i}]"
        _obj(d, w, {"topic", "cap"}, required=("topic", "cap"))
        t = _str(d["topic"], f"{w}.topic")
        if t not in topic_names:
            raise SpecError(f"{w}.topic {t!r} is not a topic")
        if t in seen:
            raise SpecError(f"quotas.declared names {t!r} twice")
        seen.add(t)
        cap = d["cap"]
        if cap != TAKE_ALL:
            cap = _intv(cap, f"{w}.cap", minimum=0)
        declared.append({"topic": t, "cap": cap})
    q["declared"] = declared
    q["default_cap"] = _intv(v.get("default_cap"), "quotas.default_cap", minimum=0, null=True)
    if q["default_cap"] is None:
        missing = [t for t in topic_names if t not in seen]
        if missing:
            raise SpecError(f"quotas.declared does not cap {', '.join(missing)} and quotas.default_cap is "
                            f"null: declare them (cap 0 to take none) or set a default_cap")
    q["allocation_order"] = _enum(v.get("allocation_order", "bucket_size_desc"), "quotas.allocation_order",
                                  ("bucket_size_desc", "cap_desc"))
    return q


def _template_sample():
    ints = {"n_seeds", "cited_by", "pool_rank", "order"}
    return {k: (1 if k in ints else "x") for k in NOTES_FIELDS}


def _check_template(tpl):
    fmt = string.Formatter()
    try:
        for _, fname, _, _ in fmt.parse(tpl):
            if fname is None:
                continue
            base = re.split(r"[.\[]", fname, maxsplit=1)[0]
            if base not in NOTES_FIELDS:
                raise SpecError(f"output.notes_template uses {{{fname}}}; allowed: "
                                f"{', '.join(NOTES_FIELDS)}")
        tpl.format(**_template_sample())
    except SpecError:
        raise
    except (ValueError, IndexError, KeyError, AttributeError) as e:
        raise SpecError(f"output.notes_template is not a valid format string: {e}") from None
    return tpl


def normalise_spec(raw):
    """The effective spec: `raw` validated strictly, every default filled in. Raises SpecError."""
    _obj(raw, "spec", {"schema", "name", "description", "provenance", "recorded_run", "input", "collapse",
                       "years", "topics", "require", "exclude", "controls", "holdings", "rank", "quotas",
                       "chapter_budgets", "order", "lanes", "annotate", "reports", "output"},
         required=("schema", "name", "input", "topics", "quotas"))
    if raw["schema"] != SCHEMA:
        raise SpecError(f"schema must be {SCHEMA!r}, got {raw['schema']!r}")
    eff = {"schema": SCHEMA, "name": _name(raw["name"], "name", _GATE_NAME_RE),
           "description": _str(raw.get("description", ""), "description", empty=True)}
    for k in ("provenance", "recorded_run"):
        if not isinstance(raw.get(k, {}), dict):
            raise SpecError(f"{k} must be an object")
        eff[k] = raw.get(k, {})
    eff["input"] = _norm_input(raw["input"])
    c = _obj(raw.get("collapse", {}), "collapse", {"record_row", "fill_empty", "cited_by", "text"})
    eff["collapse"] = {
        "record_row": _enum(c.get("record_row", "first_with_title"), "collapse.record_row",
                            ("first", "first_with_title")),
        "fill_empty": _subset(c.get("fill_empty", []), "collapse.fill_empty", FILL_FIELDS),
        "cited_by": _enum(c.get("cited_by", "max"), "collapse.cited_by", ("record_row", "max")),
        "text": _subset(c.get("text", list(TEXT_OPS)), "collapse.text", TEXT_OPS)}
    eff["years"] = _norm_years(raw.get("years"), "years")
    for key, minimum, topic in (("topics", 1, True), ("require", 0, False), ("exclude", 0, False)):
        ms = [_norm_matcher(m, f"{key}[{i}]", topic=topic)
              for i, m in enumerate(_listv(raw.get(key, []), key, min_len=minimum))]
        names = [m["name"] for m in ms]
        dup = sorted({n for n in names if names.count(n) > 1})
        if dup:
            raise SpecError(f"{key} names {', '.join(dup)} more than once")
        eff[key] = ms
    topic_names = [t["name"] for t in eff["topics"]]
    ctl = _obj(raw.get("controls", {}), "controls", {"match_none", "exclude_none", "admit", "contentless",
                                                     "minimum", "require_offtopic_drops", "require_nonempty_pool"})
    eff["controls"] = {"match_none": _titles(ctl.get("match_none", []), "controls.match_none"),
                       "exclude_none": _titles(ctl.get("exclude_none", []), "controls.exclude_none"),
                       "admit": _titles(ctl.get("admit", []), "controls.admit"),
                       "contentless": _boolv(ctl.get("contentless", True), "controls.contentless"),
                       "minimum": _enum(ctl.get("minimum", "gate"), "controls.minimum", ("gate", "none")),
                       "require_offtopic_drops": _boolv(ctl.get("require_offtopic_drops", True),
                                                        "controls.require_offtopic_drops"),
                       "require_nonempty_pool": _boolv(ctl.get("require_nonempty_pool", True),
                                                       "controls.require_nonempty_pool")}
    hd = _obj(raw.get("holdings", {}), "holdings", {"filter", "scope"})
    eff["holdings"] = {"filter": _boolv(hd.get("filter", True), "holdings.filter"),
                       "scope": _enum(hd.get("scope", "portfolio"), "holdings.scope", ("portfolio", "project"))}
    eff["rank"] = _norm_rank(raw.get("rank", DEFAULT_RANK), "rank")
    eff["quotas"] = _norm_quotas(raw["quotas"], topic_names)
    cb = raw.get("chapter_budgets")
    if cb is not None:
        _obj(cb, "chapter_budgets", set(cb))
        cb = {k: _intv(n, f"chapter_budgets[{k!r}]", minimum=0) for k, n in cb.items()}
    eff["chapter_budgets"] = cb
    o = _obj(raw.get("order", {}), "order", {"method", "bucket_order"})
    method = _enum(o.get("method", "round_robin"), "order.method", ("round_robin", "rank"))
    if method == "rank" and "bucket_order" in o:
        raise SpecError("unsupported combination: order.bucket_order applies to order.method 'round_robin' only")
    eff["order"] = {"method": method,
                    "bucket_order": None if method == "rank" else
                    _enum(o.get("bucket_order", "size_asc"), "order.bucket_order", ("size_asc", "size_desc"))}
    lanes, seen = [], set()
    for i, ln in enumerate(_listv(raw.get("lanes", []), "lanes")):
        w = f"lanes[{i}]"
        _obj(ln, w, {"name", "topics", "size", "years", "rank"}, required=("name", "topics", "size"))
        nm = _name(ln["name"], f"{w}.name", _GATE_NAME_RE)
        if nm == MAIN_LANE or nm in seen:
            raise SpecError(f"{w}.name {nm!r} is reserved or used twice")
        seen.add(nm)
        tops = _listv(ln["topics"], f"{w}.topics", min_len=1)
        for j, t in enumerate(tops):
            if t not in topic_names:
                raise SpecError(f"{w}.topics[{j}] {t!r} is not a topic")
        lanes.append({"name": nm, "topics": list(tops), "size": _intv(ln["size"], f"{w}.size", minimum=1),
                      "years": _norm_years(ln.get("years"), f"{w}.years"),
                      "rank": _norm_rank(ln["rank"], f"{w}.rank") if "rank" in ln else None})
    eff["lanes"] = lanes
    ann, cols = [], set(SELECTION_COLUMNS) | set(POOL_COLUMNS) | {"notes"}
    for i, a in enumerate(_listv(raw.get("annotate", []), "annotate")):
        w = f"annotate[{i}]"
        _obj(a, w, {"column", "dois_from", "doi_column", "comment_prefix"}, required=("column", "dois_from"))
        col = _name(a["column"], f"{w}.column", _COLUMN_RE)
        if col in cols:
            raise SpecError(f"{w}.column {col!r} collides with an output column")
        cols.add(col)
        ann.append({"column": col, "dois_from": _str(a["dois_from"], f"{w}.dois_from"),
                    "doi_column": _str(a.get("doi_column", "doi"), f"{w}.doi_column"),
                    "comment_prefix": _str(a.get("comment_prefix", "#"), f"{w}.comment_prefix", empty=True)})
    eff["annotate"] = ann
    rp = _obj(raw.get("reports", {}), "reports", {"direction_split"})
    splits = []
    for i, d in enumerate(_listv(rp.get("direction_split", []), "reports.direction_split")):
        w = f"reports.direction_split[{i}]"
        _obj(d, w, {"topic", "sides", "match"}, required=("topic", "sides"))
        if d["topic"] not in topic_names:
            raise SpecError(f"{w}.topic {d['topic']!r} is not a topic")
        sides, snames = [], set()
        for j, s in enumerate(_listv(d["sides"], f"{w}.sides", min_len=2)):
            sw = f"{w}.sides[{j}]"
            _obj(s, sw, {"name", "words"}, required=("name", "words"))
            sn = _name(s["name"], f"{sw}.name")
            if sn in snames:
                raise SpecError(f"{w} names side {sn!r} twice")
            snames.add(sn)
            words = [_str(x, f"{sw}.words[{k}]") for k, x in enumerate(_listv(s["words"], f"{sw}.words", min_len=1))]
            sides.append({"name": sn, "words": words})
        splits.append({"topic": d["topic"], "sides": sides,
                       "match": _enum(d.get("match", "substring"), f"{w}.match", ("substring",))})
    eff["reports"] = {"direction_split": splits}
    out = _obj(raw.get("output", {}), "output", {"notes_template"})
    eff["output"] = {"notes_template": _check_template(_str(out.get("notes_template", DEFAULT_NOTES),
                                                            "output.notes_template", empty=True))}
    return eff


# ---------------------------------------------------------------- compiled spec
@dataclass
class Matcher:
    name: str
    patterns: tuple            # ((kind, source text, compiled), ...)
    hit: tuple = ()
    miss: tuple = ()
    chapter: str | None = None

    def search(self, title):
        for _, _, rx in self.patterns:
            if rx.search(title):
                return True
        return False


def compile_pattern(kind, text, where=""):
    if kind == "substrings":
        src = re.escape(text)
    elif kind == "phrases":
        src = r"(?<![a-z])" + re.escape(text) + r"(?![a-z])"
    else:
        src = text
    try:
        rx = re.compile(src, re.IGNORECASE)
    except re.error as e:
        raise SpecError(f"{where}: pattern {text!r} does not compile: {e}") from None
    if rx.search(""):
        raise SpecError(f"{where}: pattern {text!r} matches the empty title, so it would match every title")
    return rx


def _compile_matcher(m, where):
    pats = tuple((kind, p, compile_pattern(kind, p, f"{where} ({m['name']})"))
                 for kind in ("substrings", "phrases", "regex") for p in m[kind])
    return Matcher(m["name"], pats, tuple(_title_of(t) for t in m["controls"]["hit"]),
                   tuple(_title_of(t) for t in m["controls"]["miss"]), m.get("chapter"))


@dataclass
class Spec:
    eff: dict
    topics: tuple
    require: tuple
    exclude: tuple
    sha256: str = ""

    @property
    def name(self):
        return self.eff["name"]

    def topic(self, name):
        return next(t for t in self.topics if t.name == name)


def load_spec(source):
    """A compiled Spec from a path, JSON text or a dict. Raises SpecError (exit 1)."""
    if isinstance(source, dict):
        raw, sha = source, _sha256_bytes(json.dumps(source, sort_keys=True).encode("utf-8"))
    else:
        p = Path(source)
        if isinstance(source, Path) or (isinstance(source, str) and not source.lstrip().startswith("{")):
            try:
                data = p.read_bytes()
            except OSError as e:
                raise SpecError(f"cannot read the spec {p}: {type(e).__name__}: {e}") from None
            sha = _sha256_bytes(data)
            try:
                text = data.decode("utf-8-sig")
            except UnicodeDecodeError as e:
                raise SpecError(f"spec {p} is not UTF-8: {e}") from None
        else:
            text, sha = source, _sha256_bytes(source.encode("utf-8"))
        raw = parse_spec_text(text)
    eff = normalise_spec(raw)
    return Spec(eff, tuple(_compile_matcher(m, f"topics[{i}]") for i, m in enumerate(eff["topics"])),
                tuple(_compile_matcher(m, f"require[{i}]") for i, m in enumerate(eff["require"])),
                tuple(_compile_matcher(m, f"exclude[{i}]") for i, m in enumerate(eff["exclude"])), sha)


# ---------------------------------------------------------------- controls (step 3)
def _topics_of(spec, title):
    return [t.name for t in spec.topics if t.search(title)]


def _included(spec, title):
    if spec.require:
        return all(f.search(title) for f in spec.require)
    return any(t.search(title) for t in spec.topics)


def run_controls(spec):
    """{"checked": n, "failures": [str], "positives": n, "negatives": n, "minimum": ...}."""
    fails, checked, pos, neg = [], 0, 0, 0
    for group, matchers in (("topic", spec.topics), ("require", spec.require), ("exclude", spec.exclude)):
        for m in matchers:
            for t in m.hit:
                checked += 1
                pos += group in ("topic", "require")
                if not m.search(t):
                    fails.append(f"{group} {m.name}: hit control does not match: {t!r}")
            for t in m.miss:
                checked += 1
                neg += group in ("topic", "require")
                if m.search(t):
                    fails.append(f"{group} {m.name}: miss control matches: {t!r}")
    ctl = spec.eff["controls"]
    for t in map(_title_of, ctl["match_none"]):
        checked += 1
        neg += 1
        got = _topics_of(spec, t)
        if got:
            fails.append(f"match_none control matches topic(s) {got}: {t!r}")
    for t in map(_title_of, ctl["exclude_none"]):
        checked += 1
        got = [e.name for e in spec.exclude if e.search(t)]
        if got:
            fails.append(f"exclude_none control matches exclude(s) {got}: {t!r}")
    for t in map(_title_of, ctl["admit"]):
        checked += 1
        pos += 1
        if not _included(spec, t) or any(e.search(t) for e in spec.exclude):
            fails.append(f"admit control is not admitted: {t!r}")
    if ctl["contentless"]:
        checked += 1
        if _included(spec, CONTENTLESS_TITLE):
            fails.append(f"contentless control passes inclusion: {CONTENTLESS_TITLE!r}")
    if ctl["minimum"] == "gate":
        if not pos:
            fails.append("controls.minimum 'gate': no positive control (a topic or require hit, or admit)")
        if not neg:
            fails.append("controls.minimum 'gate': no negative control (match_none, or a topic or require miss)")
    return {"checked": checked, "failures": fails, "positives": pos, "negatives": neg,
            "minimum": ctl["minimum"]}


def live_controls(hm, *, project, lib, scope):
    """The step-3 live-holdings controls. Raises GateAbort (exit 2)."""
    if scope == "project":
        n = sum(1 for d in hm.dois() if any(h.project == project for h in hm.content(d)))
    else:
        n = len(hm)
    if n < LIVE_MIN_DOIS:
        raise GateAbort(f"zero input: live holdings for scope {scope!r} give {n} DOI(s), fewer than "
                        f"{LIVE_MIN_DOIS}; the held filter would be meaningless", ["zero_input:holdings"])
    fails, picked = [], []
    try:
        names = sorted(e.name for e in os.scandir(lib) if e.is_file() and e.name.lower().endswith(".ris"))
    except OSError as e:
        names = []
        fails.append(f"cannot list the project library {lib}: {type(e).__name__}")
    for nm in names:
        try:
            text = (Path(lib) / nm).read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        m = _RIS_DO.search(text)
        if m:
            picked.append((nm, m.group(1)))
        if len(picked) == LIVE_MIN_DOIS:
            break
    if len(picked) < LIVE_MIN_DOIS:
        fails.append(f"the project library has {len(picked)} .ris file(s) with a DO line, fewer than "
                     f"{LIVE_MIN_DOIS}")
    for nm, d in picked:
        if not hm.records(d):
            fails.append(f"independent read: {nm} names {d!r}, which the holdings map does not list")
    if hm.records(ABSENT_DOI):
        fails.append(f"the holdings map lists the fabricated DOI {ABSENT_DOI}")
    if fails:
        raise GateAbort("live holdings controls failed: " + "; ".join(fails), ["controls:holdings"])
    return {"dois_in_scope": n, "ris_checked": [nm for nm, _ in picked]}


# ---------------------------------------------------------------- collapse and evaluate (steps 6-9)
@dataclass(slots=True)
class Record:
    key: str
    first_seen: int
    title: str = ""
    authors: str = ""
    venue: str = ""
    year: str = ""
    cited_by: int = 0
    titled: bool = False
    seeds: set = field(default_factory=set)
    chapters: set = field(default_factory=set)
    n_rows: int = 0
    n_seeds: int = 0
    topics: tuple = ()
    reasons: list = field(default_factory=list)
    pool_rank: int = 0
    annotations: dict = field(default_factory=dict)


def _year_ok(raw, years):
    y = _int(raw, None)
    if y is None:
        return years["unparsed"] == "keep", "year_unparsed"
    if (years["min"] is not None and y < years["min"]) or (years["max"] is not None and y > years["max"]):
        return False, "year"
    return True, ""


def collapse(spec, rows, years):
    """(records {key: Record} in first-appearance order, counts). Pure."""
    e = spec.eff
    cols, col = e["input"]["columns"], e["collapse"]
    keyf, clean = doi_key_fn(e["input"]["doi_key"]), _clean_fn(col["text"])
    seed_cols, ch_col = cols["seed"], cols["seed_chapter"]
    by_row = years is not None and years["scope"] == "row"
    with_title, rec_cb, fill = col["record_row"] == "first_with_title", col["cited_by"] == "record_row", col["fill_empty"]
    recs, counts, seed_rows = {}, Counter(), Counter()
    for i, r in enumerate(rows):
        counts["harvest_rows"] += 1
        key = keyf(r.get(cols["doi"]))
        if not key:
            counts["rows_no_doi"] += 1
            continue
        if by_row:
            ok, _ = _year_ok(r.get(cols["year"]), years)
            if not ok:
                counts["rows_year_dropped"] += 1
                continue
        title, year = clean(r.get(cols["title"])), str(r.get(cols["year"]) or "").strip()
        authors, venue = clean(r.get(cols["authors"])), clean(r.get(cols["venue"]))
        cb = _int(r.get(cols["cited_by"]), None)
        rec = recs.get(key)
        if rec is None:
            rec = recs[key] = Record(key, i, title, authors, venue, year, cb if cb is not None else 0, bool(title))
        else:
            if with_title and not rec.titled and title:
                rec.title, rec.authors, rec.venue, rec.year, rec.titled = title, authors, venue, year, True
                if rec_cb:
                    rec.cited_by = cb if cb is not None else 0
            for f in fill:
                if not getattr(rec, f):
                    v = {"title": title, "authors": authors, "venue": venue, "year": year}[f]
                    if v:
                        setattr(rec, f, v)
                        if f == "title":
                            rec.titled = True
            if not rec_cb and cb is not None and cb > rec.cited_by:
                rec.cited_by = cb
        seed = next((str(r.get(c) or "").strip() for c in seed_cols if str(r.get(c) or "").strip()), "")
        if seed:
            rec.seeds.add(seed)
            seed_rows[seed] += 1
        else:
            counts["rows_blank_seed"] += 1
        if ch_col:
            ch = str(r.get(ch_col) or "").strip()
            if ch:
                rec.chapters.add(ch)
        rec.n_rows += 1
    for rec in recs.values():
        rec.n_seeds = len(rec.seeds)
    counts["records"] = len(recs)
    return recs, counts, seed_rows


def evaluate(spec, recs, years, held=None, drawn=frozenset(), memo=None):
    """Attach topics and every reason to each record; returns the candidates in first-appearance
    order. `held(key) -> bool` or None; `drawn` holds litpipe.holdings DOI keys."""
    memo = {} if memo is None else memo
    by_record = years is not None and years["scope"] == "record"
    use_held = spec.eff["holdings"]["filter"] and held is not None
    cands = []
    for rec in recs.values():
        reasons = []
        if by_record:
            ok, why = _year_ok(rec.year, years)
            if not ok:
                reasons.append(why)
        if not rec.title:
            reasons.append("no_title")
        got = memo.get(rec.title)
        if got is None:
            t = rec.title
            got = memo[rec.title] = (
                tuple(m.name for m in spec.topics if t and m.search(t)),
                tuple(f.name for f in spec.require if not (t and f.search(t))),
                tuple(x.name for x in spec.exclude if t and x.search(t)))
        rec.topics = got[0]
        if spec.require:
            reasons.extend("missing:" + f for f in got[1])
        elif not got[0]:
            reasons.append("no_topic")
        reasons.extend("excluded:" + x for x in got[2])
        if use_held and held(rec.key):
            reasons.append("held")
        if drawn and _holdings.doi_key(rec.key) in drawn:
            reasons.append("drawn")
        rec.reasons = reasons
        if not reasons:
            cands.append(rec)
    return cands


def _inclusion_failed(rec):
    return any(r == "no_topic" or r.startswith("missing:") for r in rec.reasons)


def _rank_tuple(rec, keys):
    out = []
    for k in keys:
        f = k["field"]
        v = rec.n_seeds if f == "n_seeds" else rec.cited_by if f == "cited_by" else _int(rec.year, 0)
        out.append(-v if k["order"] == "desc" else v)
    return tuple(out)


def rank(cands, keys):
    """Sort by the explicit tuple (rank keys..., first_seen); assigns pool_rank from 1."""
    out = sorted(cands, key=lambda r: (_rank_tuple(r, keys), r.first_seen))
    for i, r in enumerate(out, 1):
        r.pool_rank = i
    return out


def _sanity(spec, recs, cands, where="build", offtopic=True):
    ctl, reasons = spec.eff["controls"], []
    if ctl["require_nonempty_pool"] and not cands:
        reasons.append(f"sanity ({where}): zero candidates")
    if offtopic and ctl["require_offtopic_drops"] and recs and not any(_inclusion_failed(r) for r in recs.values()):
        reasons.append(f"sanity ({where}): no record failed inclusion; the matcher keeps everything")
    if reasons:
        raise GateAbort("; ".join(reasons), reasons)


def build_pool(spec, rows, *, years, held=None, drawn=frozenset(), memo=None, where="build", offtopic=True):
    """Steps 6 to 9: (records, ranked candidates, counts, seed_rows). Raises GateAbort. A lane re-run
    passes offtopic=False: the matcher's discrimination is established by the main build over the
    whole harvest, and a narrow lane window can hold only on-topic records."""
    recs, counts, seed_rows = collapse(spec, rows, years)
    if not counts["harvest_rows"]:
        raise GateAbort(f"zero input ({where}): the harvest has 0 rows", ["zero_input:harvest"])
    if not recs:
        raise GateAbort(f"zero input ({where}): 0 records survive the collapse "
                        f"({counts['rows_no_doi']} rows without a DOI, {counts['rows_year_dropped']} out of "
                        f"the year window)", ["zero_input:records"])
    cands = evaluate(spec, recs, years, held, drawn, memo)
    _sanity(spec, recs, cands, where, offtopic)
    return recs, rank(cands, spec.eff["rank"]), counts, seed_rows


# ---------------------------------------------------------------- plan (step 10)
@dataclass
class Plan:
    order: list                 # selected records, draw order
    assigned: dict              # key -> bucket name
    reasons: dict               # key -> plan-stage reason
    table: dict                 # topic -> {available, carrying, cap, taken, short, trimmed}
    allocation: list            # bucket names in allocation order
    trim: dict
    tier: list
    alloc_rows: list            # S before ordering, allocation order
    caps: dict
    protected: list             # every take-all and tier row, before the trim


def _cap_value(c):
    return float("inf") if c == TAKE_ALL else c


def plan(spec, cands):
    """The quota plan over ranked candidates (pure). Raises GateAbort on an impossible trim."""
    q, names = spec.eff["quotas"], [t.name for t in spec.topics]
    target, tier = q["target"], q["tier_take_all"]
    if tier:
        A = [r for r in cands if r.n_seeds >= tier["min_seeds"]]
        P = [r for r in cands if r.n_seeds < tier["min_seeds"]]
    else:
        A, P = [], list(cands)
    reasons, buckets, caps, table = {}, {}, {}, {}
    for t in names:
        table[t] = {"available": 0, "carrying": sum(1 for r in P if t in r.topics), "cap": None, "taken": 0,
                    "short": 0, "trimmed": 0}
    if q["assign"] == "rarest_match":
        freq = Counter(t for r in P for t in r.topics)
        by_topic = {}
        for r in P:
            if not r.topics:
                reasons[r.key] = "no_quota_topic"
                continue
            by_topic.setdefault(min(r.topics, key=lambda t: (freq[t], t)), []).append(r)
        cap_of = {d["topic"]: d["cap"] for d in q["declared"]}

        def cap_for(t):
            return cap_of[t] if t in cap_of else (q["default_cap"] if q["default_cap"] is not None else 0)
        if q["allocation_order"] == "bucket_size_desc":
            keys = list(by_topic)
            order = [keys[i] for i in sorted(range(len(keys)), key=lambda i: (-len(by_topic[keys[i]]), i))]
        else:
            entries = [d["topic"] for d in q["declared"]] + [t for t in names if t not in cap_of]
            order = [entries[i] for i in sorted(range(len(entries)),
                                                key=lambda i: (-_cap_value(cap_for(entries[i])), i))
                     if by_topic.get(entries[i])]
        for t in names:
            table[t]["cap"] = cap_for(t)
            table[t]["available"] = len(by_topic.get(t, ()))
        for t in order:
            rows, c = by_topic[t], cap_for(t)
            take = len(rows) if c == TAKE_ALL else min(c, len(rows))
            buckets[t], caps[t] = rows[:take], c
            for r in rows[take:]:
                reasons[r.key] = f"no_cap:{t}" if c == 0 else f"over_cap:{t}"
    else:
        avail = {t: [r for r in P if t in r.topics] for t in names}
        order = [names[i] for i in sorted(range(len(names)), key=lambda i: (len(avail[names[i]]), i))]
        rem = max(0, target - len(A))
        for i, t in enumerate(order):
            caps[t] = min(len(avail[t]), rem // (len(order) - i))
            rem -= caps[t]
            table[t]["available"], table[t]["cap"] = len(avail[t]), caps[t]
        seen = {r.key for r in A}
        for t in order:
            got = []
            for r in avail[t]:
                if len(got) >= caps[t]:
                    break
                if r.key in seen:
                    continue
                seen.add(r.key)
                got.append(r)
            buckets[t] = got
        for r in P:
            if r.key not in seen:
                first = next((t for t in order if t in r.topics), None)
                reasons[r.key] = f"quota_full:{first}" if first else "no_quota_topic"
    alloc = ([(TIER_BUCKET, list(A))] if A else []) + [(t, list(buckets[t])) for t in order]
    protected = {TIER_BUCKET} | {t for t in caps if caps[t] == TAKE_ALL}
    protected_rows = [r for t, rs in alloc if t in protected for r in rs]
    total = sum(len(rs) for _, rs in alloc)
    trim = {"overflow": max(0, total - target), "allocated": total, "buckets": {}, "dropped_largest": 0,
            "restored": 0}
    if total > target:
        over = total - target
        capped = [(t, rs) for t, rs in alloc if t not in protected]
        ctotal = sum(len(rs) for _, rs in capped)
        if ctotal < over:
            raise GateAbort(f"impossible trim: {total} rows allocated for a target of {target}, and the capped "
                            f"buckets hold {ctotal} rows (take-all and tier rows alone are {total - ctotal}); "
                            f"raise the target or cap a take-all topic deliberately", ["trim:impossible"])
        kept, cut = {}, {}
        for t, rs in capped:
            share = round(Fraction(over * len(rs), ctotal))
            keep = max(0, len(rs) - share)
            kept[t], cut[t] = rs[:keep], rs[keep:]
            trim["buckets"][t] = {"before": len(rs), "after": keep}
        total = sum(len(rs) for t, rs in alloc if t in protected) + sum(len(v) for v in kept.values())
        corder = [t for t, _ in capped]
        while total > target:
            t = max(corder, key=lambda x: (len(kept[x]), -corder.index(x)))
            cut[t].insert(0, kept[t].pop())
            total -= 1
            trim["dropped_largest"] += 1
        while total < target and any(cut.values()):
            t = max(corder, key=lambda x: (len(cut[x]), -corder.index(x)))
            kept[t].append(cut[t].pop(0))
            total += 1
            trim["restored"] += 1
        for t in corder:
            trim["buckets"][t]["after"] = len(kept[t])
            table[t]["trimmed"] = len(cut[t])
            for r in cut[t]:
                reasons[r.key] = f"trimmed:{t}"
        alloc = [(t, rs if t in protected else kept[t]) for t, rs in alloc]
    assigned = {r.key: t for t, rs in alloc for r in rs}
    for t, rs in alloc:
        if t in table:
            table[t]["taken"] = len(rs)
            c = table[t]["cap"]
            table[t]["short"] = max(0, c - len(rs)) if isinstance(c, int) else 0
    alloc_rows = [r for _, rs in alloc for r in rs]
    if spec.eff["order"]["method"] == "round_robin":
        live = [(t, rs) for t, rs in alloc if rs]
        sign = 1 if spec.eff["order"]["bucket_order"] == "size_asc" else -1
        idx = sorted(range(len(live)), key=lambda i: (sign * len(live[i][1]), i))
        out, pos = [], [0] * len(live)
        while len(out) < len(alloc_rows):
            placed = False
            for i in idx:
                if pos[i] < len(live[i][1]):
                    out.append(live[i][1][pos[i]])
                    pos[i] += 1
                    placed = True
            if not placed:
                break
    else:
        keys = spec.eff["rank"]
        out = [alloc_rows[i] for i in sorted(range(len(alloc_rows)),
                                             key=lambda i: (_rank_tuple(alloc_rows[i], keys), i))]
    return Plan(out, assigned, reasons, table, [t for t, _ in alloc], trim, list(A), alloc_rows, caps,
                protected_rows)


# ---------------------------------------------------------------- invariants (step 12)
def check_invariants(spec, pl, lanes_out):
    """Raises GateAbort on a failure; returns (checks, collisions) and drops colliding rows (the
    later one) from pl.order and the lanes, in place."""
    q, fails = spec.eff["quotas"], []
    if Counter(r.key for r in pl.order) != Counter(r.key for r in pl.alloc_rows):
        fails.append("the order is not a permutation of the allocation")
    present = {r.key for r in pl.order}
    missing = [r.key for r in pl.protected if r.key not in present]
    if missing:
        fails.append(f"{len(missing)} take-all or tier row(s) missing, first {missing[0]}")
    for t, c in pl.caps.items():
        taken = sum(1 for r in pl.order if pl.assigned.get(r.key) == t)
        if isinstance(c, int) and taken > c:
            fails.append(f"topic {t}: taken {taken} above cap {c}")
    if len(pl.order) > q["target"]:
        fails.append(f"selection {len(pl.order)} above target {q['target']}")
    all_keys = [r.key for r in pl.order] + [r.key for _, rows in lanes_out for r in rows]
    if len(set(all_keys)) != len(all_keys):
        fails.append("a DOI key appears twice across the selection and lanes")
    if fails:
        raise GateAbort("invariant failure: " + "; ".join(fails), ["invariant"])
    seen, collisions = {}, []

    def keep(rows, where):
        out = []
        for r in rows:
            hk = _holdings.doi_key(r.key)
            if hk in seen:
                collisions.append({"kept": seen[hk][0], "kept_in": seen[hk][1], "dropped": r.key,
                                   "dropped_from": where, "holdings_key": hk})
                continue
            seen[hk] = (r.key, where)
            out.append(r)
        return out
    pl.order[:] = keep(pl.order, MAIN_LANE)
    for name, rows in lanes_out:
        rows[:] = keep(rows, name)
    checks = ["permutation", "take_all_and_tier_present", "taken_le_cap", "selection_le_target",
              "distinct_spec_keys", "distinct_holdings_keys"]
    return checks, collisions


def pool_key_collisions(records):
    """Pairs of distinct spec keys that share one litpipe.holdings.doi_key (report)."""
    by = {}
    for r in records:
        by.setdefault(_holdings.doi_key(r.key), []).append(r.key)
    return [{"holdings_key": k, "keys": v} for k, v in sorted(by.items()) if len(v) > 1]


# ---------------------------------------------------------------- the pure core
@dataclass
class Result:
    spec: Spec
    command: str
    records: dict = field(default_factory=dict)
    candidates: list = field(default_factory=list)
    counts: Counter = field(default_factory=Counter)
    seed_rows: Counter = field(default_factory=Counter)
    controls: dict = field(default_factory=dict)
    plan: Plan | None = None
    lanes: list = field(default_factory=list)       # [(name, [Record])]
    lane_info: dict = field(default_factory=dict)
    checks: list = field(default_factory=list)
    collisions: list = field(default_factory=list)
    pool_collisions: list = field(default_factory=list)
    warnings: list = field(default_factory=list)
    from_pool: bool = False


def gate(spec, rows=None, *, held=None, drawn=frozenset(), annotations=None, pool_rows=None,
         command="plan"):
    """The pure core: (spec, harvest rows, held, drawn) -> Result. `rows` are harvest dicts (column ->
    value) in file order; `held` is a callable (key -> bool), a set of keys (exact-string replay), or
    None; `drawn` is a set of litpipe.holdings DOI keys; `annotations` maps column -> set of keys.
    With `pool_rows` (a recorded candidate pool) the plan starts from them. Raises GateAbort."""
    if isinstance(held, (set, frozenset)):
        snap = held
        held = snap.__contains__
    drawn = frozenset(_holdings.doi_key(d) for d in (drawn or ()))
    res = Result(spec, command)
    res.controls = run_controls(spec)
    if res.controls["failures"]:
        raise GateAbort(f"controls failed ({len(res.controls['failures'])}): "
                        + "; ".join(res.controls["failures"]), ["controls"], {"controls": res.controls})
    if res.controls["minimum"] == "none":
        res.warnings.append("controls.minimum is 'none': the gate runs without a guaranteed positive and "
                            "negative control")
    memo = {}
    years = spec.eff["years"]
    if pool_rows is not None:
        res.from_pool = True
        recs = _records_from_pool(spec, pool_rows)
        if not recs:
            raise GateAbort("zero input: the pool has 0 rows", ["zero_input:pool"])
        res.records = recs
        res.candidates = list(recs.values())
        for i, r in enumerate(res.candidates, 1):
            r.pool_rank = i
        res.counts = Counter(records=len(recs), pool_rows=len(recs))
    else:
        rows = list(rows or [])
        recs, cands, counts, seed_rows = build_pool(spec, rows, years=years, held=held, drawn=drawn, memo=memo)
        res.records, res.candidates, res.counts, res.seed_rows = recs, cands, counts, seed_rows
    for col, keys in (annotations or {}).items():
        for r in res.records.values():
            r.annotations[col] = "y" if r.key in keys else ""
    res.pool_collisions = pool_key_collisions(res.candidates)
    if command == "build":
        return res
    res.plan = plan(spec, res.candidates)
    taken = {r.key for r in res.plan.order}
    for ln in spec.eff["lanes"]:
        if ln["years"] is None:
            lane_cands = res.candidates
        elif res.from_pool:
            res.warnings.append(f"lane {ln['name']}: sets years, which needs the harvest; skipped in --pool mode")
            res.lane_info[ln["name"]] = {"skipped": "needs the harvest (lane years)"}
            continue
        else:
            lrecs, lane_cands, _, _ = build_pool(spec, rows, years=ln["years"], held=held, drawn=drawn, memo=memo,
                                                 where=f"lane {ln['name']}", offtopic=False)
            for col, keys in (annotations or {}).items():
                for r in lrecs.values():
                    r.annotations[col] = "y" if r.key in keys else ""
        want = set(ln["topics"])
        elig = [r for r in lane_cands if want.intersection(r.topics) and r.key not in taken]
        keys = ln["rank"] if ln["rank"] is not None else spec.eff["rank"]
        elig.sort(key=lambda r: (_rank_tuple(r, keys), r.first_seen))
        got = elig[:ln["size"]]
        taken |= {r.key for r in got}
        res.lanes.append((ln["name"], got))
        res.lane_info[ln["name"]] = {"candidates": len(elig), "taken": len(got), "size": ln["size"],
                                     "topics": list(ln["topics"]), "years": ln["years"],
                                     "rank": keys}
        if not got:
            res.warnings.append(f"lane {ln['name']}: no row qualifies")
    res.checks, res.collisions = check_invariants(spec, res.plan, res.lanes)
    return res


def _split(v):
    return [s.strip() for s in re.split(r"[;|]", str(v or "")) if s.strip()]


def _records_from_pool(spec, pool_rows):
    """Records from a recorded candidate pool: row order is rank order; `doi`, `n_seeds` and
    `topics` are read, the rest is optional (a first_seen column, when present, breaks lane ties)."""
    keyf = doi_key_fn(spec.eff["input"]["doi_key"])
    order = {t.name: j for j, t in enumerate(spec.topics)}
    recs = {}
    for i, row in enumerate(pool_rows):
        key = keyf(row.get("doi"))
        if not key or key in recs:
            continue
        tops = _split(row.get("topics"))
        unknown = [t for t in tops if t not in order]
        if unknown:
            raise GateError(f"pool row {i + 1} ({key}) carries topic(s) the spec does not define: "
                            f"{', '.join(unknown)}")
        title = str(row.get("title") or "").strip()
        fs = _int(row.get("first_seen"), None)
        rec = Record(key, i if fs is None else fs, title, str(row.get("authors") or "").strip(),
                     str(row.get("venue") or "").strip(), str(row.get("year") or "").strip(),
                     _int(row.get("cited_by"), 0), bool(title))
        rec.n_seeds = _int(row.get("n_seeds"), 0)
        rec.seeds = set(_split(row.get("seeds")))
        rec.chapters = {c.strip() for c in str(row.get("chapters") or "").split(";") if c.strip()}
        rec.topics = tuple(sorted(set(tops), key=order.__getitem__))
        recs[key] = rec
    return recs


# ---------------------------------------------------------------- reports (step 15)
def _pct(n, d):
    return round(100.0 * n / d, 1) if d else 0.0


def _tiers(recs):
    hist = Counter(r.n_seeds for r in recs)
    return {"n": len(recs), "ge2": sum(v for k, v in hist.items() if k >= 2),
            "ge3": sum(v for k, v in hist.items() if k >= 3), "ge4": sum(v for k, v in hist.items() if k >= 4),
            "histogram": {str(k): hist[k] for k in sorted(hist)}}


def _shares(spec, rows):
    return {t.name: {"rows": sum(1 for r in rows if t.name in r.topics),
                     "pct": _pct(sum(1 for r in rows if t.name in r.topics), len(rows))} for t in spec.topics}


def _direction(spec, rows, assigned, d):
    sel = [r for r in rows if assigned.get(r.key) == d["topic"]]
    out = {s["name"]: 0 for s in d["sides"]}
    out["unstated"] = 0
    for r in sel:
        t = r.title.lower()
        hits = [s["name"] for s in d["sides"] if any(w.lower() in t for w in s["words"])]
        out[hits[0] if len(hits) == 1 else "unstated"] += 1
    empty = [s["name"] for s in d["sides"] if not out[s["name"]]]
    return {"topic": d["topic"], "rows": len(sel), "counts": out, "empty_sides": empty}


def _infix_lint(spec, cands):
    out = {}
    for t in spec.topics:
        subs = [rx for kind, _, rx in t.patterns if kind == "substrings"]
        others = [rx for kind, _, rx in t.patterns if kind != "substrings"]
        if not subs:
            continue
        hits = []
        for r in cands:
            if t.name not in r.topics or any(rx.search(r.title) for rx in others):
                continue
            starts = [m.start() for rx in subs for m in rx.finditer(r.title)]
            if starts and all(s > 0 and r.title[s - 1].isalpha() for s in starts):
                hits.append(r)
        if hits:
            out[t.name] = {"rows": len(hits), "examples": [f"rank {r.pool_rank}: {r.title[:90]}" for r in hits[:5]]}
    return out


def build_report(res, *, holdings_info=None, drawn_info=None, seeds_info=None, coverage=None, inputs=None):
    spec, e = res.spec, res.spec.eff
    recs = list(res.records.values())
    reasons, primary = Counter(), Counter()
    for r in recs:
        reasons.update(r.reasons)
        if r.reasons:
            primary[r.reasons[0]] += 1
    funnel = {"harvest_rows": res.counts.get("harvest_rows", 0), "rows_no_doi": res.counts.get("rows_no_doi", 0),
              "rows_year_dropped": res.counts.get("rows_year_dropped", 0),
              "rows_blank_seed": res.counts.get("rows_blank_seed", 0), "records": len(recs),
              "reasons": dict(sorted(reasons.items())), "primary_reasons": dict(sorted(primary.items())),
              "inclusion_failures": sum(1 for r in recs if _inclusion_failed(r)),
              "candidates": len(res.candidates)}
    if res.from_pool:
        funnel["from_pool"] = True
    rep = {"schema": SCHEMA, "engine_version": ENGINE_VERSION, "name": spec.name, "command": res.command,
           "spec_sha256": spec.sha256, "inputs": inputs or {}, "funnel": funnel,
           "tiers": {"records": _tiers(recs), "candidates": _tiers(res.candidates)},
           "controls": {k: res.controls.get(k) for k in ("checked", "positives", "negatives", "minimum")},
           "holdings": holdings_info or {"mode": "none"}, "drawn": drawn_info or {"n": 0},
           "pool_key_collisions": res.pool_collisions, "lint": {"infix_only": _infix_lint(spec, res.candidates)},
           "annotations": {}, "warnings": list(res.warnings)}
    for a in e["annotate"]:
        c = a["column"]
        rep["annotations"][c] = {"records": sum(1 for r in recs if r.annotations.get(c)),
                                 "candidates": sum(1 for r in res.candidates if r.annotations.get(c))}
    pl = res.plan
    if pl is not None:
        sel = pl.order
        q = e["quotas"]
        rep["plan"] = {"target": q["target"], "selected": len(sel), "tier": len(pl.tier),
                       "room": max(0, q["target"] - len(pl.tier)) if q["tier_take_all"] else None,
                       "assign": q["assign"], "caps": q["caps"], "allocation_order": pl.allocation,
                       "order": e["order"], "topics": pl.table, "trim": pl.trim,
                       "plan_reasons": dict(sorted(Counter(pl.reasons.values()).items()))}
        take_all = {t for t, c in pl.caps.items() if c == TAKE_ALL} | {TIER_BUCKET}
        n_core = sum(1 for r in sel if pl.assigned.get(r.key) in take_all)
        rep["take_all_share"] = {"rows": n_core, "of": len(sel), "pct": _pct(n_core, len(sel))}
        rep["shares"] = {"selection": _shares(spec, sel), "baseline": _shares(spec, res.candidates[:q["target"]])}
        ch_of = {t.name: t.chapter for t in spec.topics}
        by_ch = Counter(ch_of.get(pl.assigned.get(r.key)) or "" for r in sel)
        seed_ch = Counter(c for r in sel for c in r.chapters)
        budgets = e["chapter_budgets"] or {}
        rep["chapters"] = {"by_topic_chapter": {k: by_ch[k] for k in sorted(by_ch)},
                           "budgets": {k: {"budget": v, "taken": by_ch.get(k, 0), "short": max(0, v - by_ch.get(k, 0))}
                                       for k, v in sorted(budgets.items())},
                           "by_seed_chapter": {k: seed_ch[k] for k in sorted(seed_ch)}}
        rep["direction_splits"] = [_direction(spec, sel, pl.assigned, d) for d in e["reports"]["direction_split"]]
        for d in rep["direction_splits"]:
            if d["rows"] and d["empty_sides"]:
                res.warnings.append(f"direction split {d['topic']}: side(s) {', '.join(d['empty_sides'])} empty")
        rep["lanes"] = res.lane_info
        rep["invariants"] = {"ok": True, "checks": res.checks, "collisions": res.collisions}
        for a in e["annotate"]:
            rep["annotations"][a["column"]]["selected"] = sum(1 for r in sel if r.annotations.get(a["column"]))
    rep["seeds"] = seeds_info or {"status": "UNKNOWN", "note": "no seeds manifest configured"}
    if res.seed_rows and isinstance(seeds_info, dict) and seeds_info.get("entries") is not None:
        rep["seeds"]["per_seed"] = _per_seed(res, seeds_info["entries"])
    rep["coverage"] = coverage or {"note": "no live holdings in this run"}
    rep["warnings"] = list(res.warnings)
    return rep


def _per_seed(res, entries):
    """Per harvest seed key: manifest status (matched by DOI, else by the manifest's `label`), harvest
    rows, distinct citers, candidates, selected; then manifest seeds no harvest row names."""
    sel = {r.key for r in res.plan.order} if res.plan else set()
    cand = {r.key for r in res.candidates}
    citers = {}
    for r in res.records.values():
        for s in r.seeds:
            citers.setdefault(s, []).append(r.key)
    by_label = {}
    for k, v in sorted(entries.items()):
        if isinstance(v, dict) and v.get("label"):
            by_label.setdefault(str(v["label"]), k)
    out, matched = [], set()
    for s in sorted(res.seed_rows):
        mk = s.lower() if s.lower() in entries else by_label.get(s)
        if mk:
            matched.add(mk)
        ks = citers.get(s, [])
        out.append({"seed": s, "manifest_key": mk, "status": entries[mk].get("status", "UNKNOWN") if mk else "UNKNOWN",
                    "rows": res.seed_rows[s], "citers": len(ks), "candidates": sum(1 for k in ks if k in cand),
                    "selected": sum(1 for k in ks if k in sel)})
    for k in sorted(set(entries) - matched):
        out.append({"seed": k, "manifest_key": k, "status": entries[k].get("status", "UNKNOWN"), "rows": 0,
                    "citers": 0, "candidates": 0, "selected": 0})
    return out


def render_markdown(rep):
    L = [f"# Gate report: {rep['name']} ({rep['command']})", ""]
    f = rep["funnel"]
    L += ["## Funnel", "", "| step | n |", "|---|---:|"]
    for k in ("harvest_rows", "rows_no_doi", "rows_year_dropped", "rows_blank_seed", "records",
              "inclusion_failures", "candidates"):
        L.append(f"| {k} | {f.get(k, 0)} |")
    L += ["", "| reason (every reason counted) | records |", "|---|---:|"]
    L += [f"| {k} | {v} |" for k, v in f["reasons"].items()]
    t = rep["tiers"]
    L += ["", "## Seed tiers", "", "| over | n | n_seeds >= 2 | >= 3 | >= 4 |", "|---|---:|---:|---:|---:|"]
    for k in ("records", "candidates"):
        L.append(f"| {k} | {t[k]['n']} | {t[k]['ge2']} | {t[k]['ge3']} | {t[k]['ge4']} |")
    c = rep["controls"]
    L += ["", f"Controls: {c['checked']} checked, {c['positives']} positive, {c['negatives']} negative, "
              f"minimum {c['minimum']}.", f"Holdings: {json.dumps(rep['holdings'], sort_keys=True)}.",
          f"Drawn set: {rep['drawn'].get('n', 0)} DOI(s)."]
    p = rep.get("plan")
    if p:
        L += ["", "## Plan", "", f"Target {p['target']}, selected {p['selected']}, tier {p['tier']}, "
              f"assign {p['assign']}, caps {p['caps']}, order {p['order']['method']}"
              + (f" ({p['order']['bucket_order']})" if p['order'].get('bucket_order') else "") + ".",
              f"Allocation order: {', '.join(p['allocation_order'])}.", "",
              "| topic | available | carrying | cap | taken | short | trimmed |", "|---|---:|---:|---:|---:|---:|---:|"]
        for name, row in p["topics"].items():
            L.append(f"| {name} | {row['available']} | {row['carrying']} | {row['cap']} | {row['taken']} | "
                     f"{row['short']} | {row['trimmed']} |")
        tr = p["trim"]
        L += ["", f"Trim: overflow {tr['overflow']} of {tr['allocated']} allocated; "
              + (", ".join(f"{k} {v['before']} to {v['after']}" for k, v in tr["buckets"].items()) or "none")
              + f"; largest-bucket drops {tr['dropped_largest']}; top-up restores {tr['restored']}."]
        s = rep["take_all_share"]
        L += [f"Take-all share: {s['rows']} of {s['of']} ({s['pct']} %).", "",
              "| topic | selection % | rank-only baseline % |", "|---|---:|---:|"]
        for name in rep["shares"]["selection"]:
            L.append(f"| {name} | {rep['shares']['selection'][name]['pct']} | {rep['shares']['baseline'][name]['pct']} |")
        ch = rep["chapters"]
        if ch["budgets"]:
            L += ["", "| chapter | budget | taken | short |", "|---|---:|---:|---:|"]
            L += [f"| {k} | {v['budget']} | {v['taken']} | {v['short']} |" for k, v in ch["budgets"].items()]
        if ch["by_topic_chapter"]:
            L += ["", "By topic chapter: " + ", ".join(f"{k or '(none)'} {v}" for k, v in ch["by_topic_chapter"].items()) + "."]
        if ch["by_seed_chapter"]:
            L += ["By seed chapter (a row counts once per chapter): "
                  + ", ".join(f"{k} {v}" for k, v in ch["by_seed_chapter"].items()) + "."]
        for d in rep["direction_splits"]:
            L += ["", f"Direction split over {d['rows']} {d['topic']} rows: "
                  + ", ".join(f"{k} {v}" for k, v in d["counts"].items()) + "."]
        if rep["lanes"]:
            L += ["", "## Lanes", "", "| lane | candidates | taken | size |", "|---|---:|---:|---:|"]
            for k, v in rep["lanes"].items():
                L.append(f"| {k} | {v.get('candidates', '')} | {v.get('taken', '')} | {v.get('size', '')} |")
        inv = rep["invariants"]
        L += ["", f"Invariants: {', '.join(inv['checks'])}; collisions dropped: {len(inv['collisions'])}."]
    sd = rep["seeds"]
    L += ["", "## Seeds", "", f"Status: {json.dumps({k: v for k, v in sd.items() if k not in ('per_seed', 'entries')}, sort_keys=True)}"]
    if rep["lint"]["infix_only"]:
        L += ["", "## Lint: substring tags carried only by mid-word hits", ""]
        for k, v in rep["lint"]["infix_only"].items():
            L.append(f"- {k}: {v['rows']} row(s); " + "; ".join(v["examples"][:3]))
    if rep["warnings"]:
        L += ["", "## Warnings", ""] + [f"- {w}" for w in rep["warnings"]]
    return "\n".join(L) + "\n"


# ---------------------------------------------------------------- I/O edges
def read_harvest(path, spec):
    """(rows, header): the columns the spec names, every row in file order. Raises GateError."""
    cols = spec.eff["input"]["columns"]
    _unlimited_fields()
    try:
        f = open(path, encoding="utf-8-sig", newline="")
    except OSError as e:
        raise GateError(f"cannot read the harvest {path}: {type(e).__name__}: {e}") from None
    with f:
        rd = csv.DictReader(f)
        header = rd.fieldnames or []
        missing = [cols[k] for k in ("doi", "title", "year", "cited_by") if cols[k] not in header]
        if not header:
            return [], header
        if missing:
            raise GateError(f"harvest {Path(path).name} lacks column(s) {', '.join(missing)} named by input.columns")
        seeds = [c for c in cols["seed"] if c in header]
        if not seeds:
            raise GateError(f"harvest {Path(path).name} has none of the seed columns {cols['seed']}")
        want = [cols[k] for k in ("doi", "title", "year", "authors", "venue", "cited_by")] + seeds
        if cols["seed_chapter"]:
            want.append(cols["seed_chapter"])
        want = [c for c in dict.fromkeys(want) if c in header]
        rows = [{c: (r.get(c) or "") for c in want} for r in rd]
    return rows, header


def read_snapshot(path):
    """The as-of held set: one DOI per line, a whole line starting with '#' is a comment."""
    try:
        text = Path(path).read_text(encoding="utf-8-sig")
    except OSError as e:
        raise GateError(f"cannot read the holdings snapshot {path}: {type(e).__name__}: {e}") from None
    out = set()
    for line in text.splitlines():
        if line.startswith("#"):
            continue
        s = line.strip()
        if s:
            out.add(s)
    return out


def read_annotation(path, a, keyf):
    try:
        with open(path, encoding="utf-8-sig", newline="") as f:
            pre = a["comment_prefix"]
            lines = [ln for ln in f if not (pre and ln.startswith(pre))]
    except OSError as e:
        raise GateError(f"cannot read annotate source {path}: {type(e).__name__}: {e}") from None
    rd = csv.DictReader(lines)
    if a["doi_column"] not in (rd.fieldnames or []):
        raise GateError(f"annotate source {Path(path).name} has no column {a['doi_column']!r}")
    return {k for k in (keyf(r.get(a["doi_column"])) for r in rd) if k}


def read_seeds_manifest(path):
    """{"status": "READ"|"UNKNOWN", "counts": {status: n}, "entries": {doi: {...}}}."""
    if path is None:
        return {"status": "UNKNOWN", "note": "no seeds manifest configured"}
    p = Path(path)
    if not p.exists():
        return {"status": "UNKNOWN", "note": f"manifest not found: {p.name}"}
    try:
        text = p.read_text(encoding="utf-8-sig")
    except OSError as e:
        return {"status": "UNKNOWN", "note": f"manifest unreadable: {type(e).__name__}"}
    entries = None
    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            entries = {str(k).strip().lower(): v for k, v in obj.items() if isinstance(v, dict)}
    except ValueError:
        lines = text.splitlines()
        try:
            head = json.loads(lines[0]) if lines else None
        except ValueError:
            head = None
        if isinstance(head, dict) and head.get("journal") == "forward_citations":
            entries = {}
            for ln in lines[1:]:
                try:
                    o = json.loads(ln)
                except ValueError:
                    continue
                if isinstance(o, dict) and isinstance(o.get("doi"), str):
                    entries[o["doi"].strip().lower()] = {"status": str(o.get("state") or "").upper()}
    if entries is None:
        return {"status": "UNKNOWN", "note": f"manifest shape not recognised: {p.name}"}
    counts = Counter(str(v.get("status") or "UNKNOWN") for v in entries.values())
    return {"status": "READ", "file": p.name, "n": len(entries), "counts": dict(sorted(counts.items())),
            "truncated": sorted(k for k, v in entries.items() if v.get("status") == "TRUNCATED"),
            "entries": entries}


def stable_paths(lib, spec):
    name = spec.name
    return ([Path(lib) / f"_{name}_gate_selection.csv"]
            + [Path(lib) / f"_{name}_gate_lane_{ln['name']}.csv" for ln in spec.eff["lanes"]])


def read_drawn(lib, spec):
    """(set of litpipe.holdings DOI keys staged or swept in the gate's stable Pools, info)."""
    from litpipe import worklists as _w
    name = spec.name
    files = {_w.state_path_for(p) for p in stable_paths(lib, spec)}
    try:
        files |= {Path(lib) / e.name for e in os.scandir(lib)
                  if e.name.startswith(f"_{name}_gate_lane_") and e.name.endswith(_w.STATE_SUFFIX)}
    except OSError:
        pass
    keys, classes, unswept, used = set(), Counter(), 0, []
    for p in sorted(files):
        if not p.exists():
            continue
        try:
            st = json.loads(p.read_text(encoding="utf-8-sig"))
        except (OSError, ValueError) as e:
            raise GateError(f"pool state {p.name} is unreadable ({type(e).__name__}); fix or move it") from None
        if not isinstance(st, dict) or st.get("version") != _w.STATE_VERSION or not isinstance(st.get("dois"), dict):
            raise GateError(f"pool state {p.name}: version {st.get('version') if isinstance(st, dict) else None!r} "
                            f"is not {_w.STATE_VERSION} or it has no `dois` object")
        used.append(p.name)
        for k, rec in st["dois"].items():
            if not isinstance(rec, dict):
                raise GateError(f"pool state {p.name}: the record for {k!r} is not an object")
            if rec.get("staged") or rec.get("swept"):
                keys.add(_holdings.doi_key(k))
                if rec.get("swept"):
                    classes[str(rec["swept"].get("class", ""))] += 1
                else:
                    unswept += 1
    return keys, {"n": len(keys), "sha256": _set_hash(keys), "files": used,
                  "swept_classes": dict(sorted(classes.items())), "staged_unswept": unswept}


def _resolve(lib, p):
    q = Path(p)
    return q if q.is_absolute() else Path(lib) / q


def harvest_path(lib, spec):
    h = spec.eff["input"]["harvest"]
    return Path(lib) / f"_{h['scope']}_forward_citations.csv" if isinstance(h, dict) else _resolve(lib, h)


def _lib_for(project, lib_dir, cfg_holder):
    if lib_dir:
        return Path(lib_dir)
    cfg = cfg_holder()
    p = (cfg.get("projects") or {}).get(project)
    if not isinstance(p, dict):
        raise GateError(f"project {project!r} is not registered in projects.json (or pass --lib-dir)")
    if not p.get("lib_dir"):
        raise GateError(f"projects.json gives {project!r} no lib_dir (or pass --lib-dir)")
    return lit_util.lib_paths(project, p)[1]


def _row_out(r, n, lane, assigned, spec, ann_cols):
    t = assigned or ""
    ch = next((m.chapter for m in spec.topics if m.name == t), None) or ""
    row = {"doi": r.key, "order": n, "title": r.title, "authors": r.authors, "year": r.year, "venue": r.venue,
           "cited_by": r.cited_by, "n_seeds": r.n_seeds, "topics": ";".join(r.topics), "assigned_topic": t,
           "topic_chapter": ch, "chapters": ";".join(sorted(r.chapters)), "seeds": ";".join(sorted(r.seeds)),
           "pool_rank": r.pool_rank, "lane": lane}
    for c in ann_cols:
        row[c] = r.annotations.get(c, "")
    fields = {k: row[k] for k in ("lane", "assigned_topic", "topics", "n_seeds", "chapters", "cited_by", "year",
                                  "topic_chapter", "pool_rank", "order")}
    row["notes"] = spec.eff["output"]["notes_template"].format(gate=spec.name, **fields)
    return row


def output_rows(res):
    """{"pool": [...], "selection": [...], "lanes": {name: [...]}, "drops": [...]} as CSV-ready dicts."""
    spec = res.spec
    ann = [a["column"] for a in spec.eff["annotate"]]
    pool = []
    for r in res.candidates:
        row = {"pool_rank": r.pool_rank, "doi": r.key, "title": r.title, "authors": r.authors, "year": r.year,
               "venue": r.venue, "cited_by": r.cited_by, "n_seeds": r.n_seeds, "topics": ";".join(r.topics),
               "chapters": ";".join(sorted(r.chapters)), "seeds": ";".join(sorted(r.seeds)),
               "first_seen": r.first_seen}
        row.update({c: r.annotations.get(c, "") for c in ann})
        pool.append(row)
    drops = []
    for r in sorted(res.records.values(), key=lambda x: x.first_seen):
        if r.reasons:
            drops.append({"doi": r.key, "title": r.title, "year": r.year, "n_seeds": r.n_seeds,
                          "primary_reason": r.reasons[0], "reasons": ";".join(r.reasons), "stage": "build"})
    out = {"pool": pool, "drops": drops, "selection": [], "lanes": {}}
    if res.plan is None:
        return out
    pl = res.plan
    for r in res.candidates:
        if r.key in pl.reasons:
            drops.append({"doi": r.key, "title": r.title, "year": r.year, "n_seeds": r.n_seeds,
                          "primary_reason": pl.reasons[r.key], "reasons": pl.reasons[r.key], "stage": "plan"})
    by_key = {r.key: r for r in res.candidates}
    for c in res.collisions:
        r = by_key.get(c["dropped"])
        drops.append({"doi": c["dropped"], "title": r.title if r else "", "year": r.year if r else "",
                      "n_seeds": r.n_seeds if r else "", "primary_reason": "pool_key_collision",
                      "reasons": f"pool_key_collision:{c['kept']}", "stage": "invariant"})
    out["selection"] = [_row_out(r, i, MAIN_LANE, pl.assigned.get(r.key), spec, ann)
                        for i, r in enumerate(pl.order, 1)]
    for name, rows in res.lanes:
        want = next(ln["topics"] for ln in spec.eff["lanes"] if ln["name"] == name)
        out["lanes"][name] = [_row_out(r, i, name, next((t for t in want if t in r.topics), ""), spec, ann)
                              for i, r in enumerate(rows, 1)]
    return out


def write_run(root, res, report, rows, manifest, run_id=None):
    """Write an immutable run folder under <root>/_gate/<name>/; returns its path."""
    spec = res.spec
    files = {"pool.csv": _csv_text(POOL_COLUMNS + [a["column"] for a in spec.eff["annotate"]], rows["pool"]),
             "drops.csv": _csv_text(DROP_COLUMNS, rows["drops"])}
    sel_cols = SELECTION_COLUMNS + [a["column"] for a in spec.eff["annotate"]] + ["notes"]
    if res.plan is not None:
        files["selection.csv"] = _csv_text(sel_cols, rows["selection"])
        for name, lrows in rows["lanes"].items():
            files[f"lane_{name}.csv"] = _csv_text(sel_cols, lrows)
    files["report.json"] = _json_text(report)
    files["report.md"] = render_markdown(report)
    manifest = dict(manifest)
    manifest["pool_sha256"] = _sha256_bytes(files["pool.csv"].encode("utf-8"))
    manifest["files"] = {k: _sha256_bytes(v.encode("utf-8")) for k, v in sorted(files.items())}
    manifest["outputs"] = {"selection": "selection.csv" if res.plan is not None else None,
                           "lanes": {n: f"lane_{n}.csv" for n in rows["lanes"]}}
    files["manifest.json"] = _json_text(manifest)
    base = Path(root) / GATE_DIR / spec.name
    base.mkdir(parents=True, exist_ok=True)
    if run_id is None:
        stamp = _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        run_id = f"{stamp}-{_sha256_bytes(files['manifest.json'].encode('utf-8'))[:8]}"
        n, stem = 1, run_id
        while (base / run_id).exists():
            n += 1
            run_id = f"{stem}-{n}"
    final = base / run_id
    if final.exists():
        raise GateError(f"run folder {final} exists; run folders are immutable")
    tmp = base / f".{run_id}.tmp"
    tmp.mkdir()
    try:
        for name, text in files.items():
            lit_util.atomic_write_text(str(tmp / name), text, newline="\n")
        os.rename(tmp, final)
    finally:
        if tmp.exists():
            for p in tmp.iterdir():
                p.unlink()
            tmp.rmdir()
    return final


# ---------------------------------------------------------------- run (CLI core)
def _held_for(spec, *, project, lib, holdings_as_of, holdmap, cfg_holder, from_pool):
    """(held callable or None, info, live HoldMap or None). Raises GateAbort / GateError."""
    h = spec.eff["holdings"]
    if not h["filter"]:
        info = {"mode": "off"}
        if holdings_as_of:
            info["note"] = "holdings.filter is false: --holdings-as-of ignored"
        return None, info, None
    if from_pool:
        return None, {"mode": "not_applied", "note": "plan --pool does not re-evaluate held"}, None
    if holdings_as_of:
        snap = read_snapshot(holdings_as_of)
        if not snap:
            raise GateAbort(f"zero input: the as-of snapshot {Path(holdings_as_of).name} holds 0 DOIs",
                            ["zero_input:snapshot"])
        return snap.__contains__, {"mode": "as_of", "file": Path(holdings_as_of).name,
                                   "sha256": _sha256_file(holdings_as_of), "n": len(snap)}, None
    if holdmap is None:
        cfg = cfg_holder()
        holdmap = _holdings.build(cfg, use_cache=True, write_cache=False)
    info = {"mode": "live", "scope": h["scope"]}
    info["controls"] = live_controls(holdmap, project=project, lib=lib, scope=h["scope"])
    dois = holdmap.dois()
    if h["scope"] == "project":
        dois = {d for d in dois if any(x.project == project for x in holdmap.content(d))}

        def held(k):
            return any(x.project == project for x in holdmap.content(k))
    else:
        def held(k):
            return bool(holdmap.where(k))
    info.update(n=len(dois), sha256=_set_hash(dois))
    return held, info, holdmap


def _coverage(res, holdmap, project, scope):
    if holdmap is None or res.plan is None:
        return None

    def is_held(k):
        c = holdmap.content(k)
        return any(x.project == project for x in c) if scope == "project" else bool(c)
    out = {}
    for name, rows in [(MAIN_LANE, res.plan.order)] + list(res.lanes):
        by_topic = Counter()
        n = 0
        for r in rows:
            if is_held(r.key):
                n += 1
                by_topic[res.plan.assigned.get(r.key) or ""] += 1
        out[name] = {"rows": len(rows), "held_now": n, "pct": _pct(n, len(rows)),
                     "held_by_topic": {k: by_topic[k] for k in sorted(by_topic)}}
    return out


def _summary_line(reasons):
    return SUMMARY_MARKER + " " + json.dumps({"reasons": reasons, "aborted": None, "transport_failures": 0},
                                             sort_keys=True)


def run(command="plan", *, project=None, spec=None, lib_dir=None, holdings_as_of=None, write=False,
        out_root=None, json_path=None, pool=None, run_id=None, force=False, registry=None, holdmap=None,
        drawn=None, argv=None, quiet=False) -> dict:
    """One CLI command. Returns {"exit_code", "command", "report", "rows", "run_dir", "error",
    "warnings"}. `registry` (a loaded projects.json), `holdmap` (a HoldMap) and `drawn` (a set of DOI
    keys) inject what the CLI would otherwise read."""
    res = {"command": command, "exit_code": EXIT_OK, "report": None, "rows": None, "run_dir": None,
           "error": None, "warnings": []}
    say = (lambda *a, **k: None) if quiet else print
    cfg_box = {}

    def cfg_holder():
        if "cfg" not in cfg_box:
            cfg_box["cfg"] = config.load(registry)
        return cfg_box["cfg"]
    try:
        if command not in ("build", "plan", "report", "promote"):
            raise GateError(f"unknown command {command!r}")
        if not project or not spec:
            raise GateError("--project and --spec are required")
        sp = load_spec(spec)
        lib = _lib_for(project, lib_dir, cfg_holder)
        if command == "promote":
            res.update(promote(sp, lib, run_id, out_root=out_root, force=force, say=say))
            return res
        from_pool = pool is not None
        held, hinfo, hm = _held_for(sp, project=project, lib=lib, holdings_as_of=holdings_as_of, holdmap=holdmap,
                                    cfg_holder=cfg_holder, from_pool=from_pool)
        if from_pool:
            drawn_keys, dinfo = frozenset(), {"n": 0, "note": "plan --pool does not apply the drawn set"}
        elif drawn is not None:
            drawn_keys = frozenset(_holdings.doi_key(d) for d in drawn)
            dinfo = {"n": len(drawn_keys), "sha256": _set_hash(drawn_keys), "injected": True}
        else:
            drawn_keys, dinfo = read_drawn(lib, sp)
        keyf = doi_key_fn(sp.eff["input"]["doi_key"])
        anns = {a["column"]: read_annotation(_resolve(lib, a["dois_from"]), a, keyf) for a in sp.eff["annotate"]}
        inputs = {"project": project}
        rows = pool_rows = None
        if from_pool:
            _unlimited_fields()
            try:
                with open(pool, encoding="utf-8-sig", newline="") as f:
                    rd = csv.DictReader(f)
                    need = [c for c in ("doi", "n_seeds", "topics") if c not in (rd.fieldnames or [])]
                    if need:
                        raise GateError(f"pool {Path(pool).name} lacks column(s) {', '.join(need)}")
                    pool_rows = list(rd)
            except OSError as e:
                raise GateError(f"cannot read the pool {pool}: {type(e).__name__}: {e}") from None
            inputs["pool"] = {"file": Path(pool).name, "sha256": _sha256_file(pool), "rows": len(pool_rows)}
        else:
            hp = harvest_path(lib, sp)
            rows, _ = read_harvest(hp, sp)
            inputs["harvest"] = {"file": hp.name, "sha256": _sha256_file(hp), "rows": len(rows)}
        cmd = "build" if command == "build" else "plan"
        result = gate(sp, rows, held=held, drawn=drawn_keys, annotations=anns, pool_rows=pool_rows, command=cmd)
        result.command = command
        seeds = read_seeds_manifest(_resolve(lib, sp.eff["input"]["seeds_manifest"])
                                    if sp.eff["input"]["seeds_manifest"] else None)
        cov = _coverage(result, hm, project, sp.eff["holdings"]["scope"])
        if cov is not None and dinfo.get("swept_classes") is not None:
            cov["pool_state"] = {"swept_classes": dinfo["swept_classes"], "staged_unswept": dinfo["staged_unswept"]}
        report = build_report(result, holdings_info=hinfo, drawn_info=dinfo, seeds_info=seeds, coverage=cov,
                              inputs=inputs)
        if isinstance(report.get("seeds"), dict):
            report["seeds"].pop("entries", None)
        out_rows = output_rows(result)
        res.update(report=report, rows=out_rows, warnings=list(report["warnings"]))
        _print_summary(report, say, command)
        if json_path:
            Path(json_path).parent.mkdir(parents=True, exist_ok=True)
            lit_util.atomic_write_text(str(json_path), _json_text(report))
        if write:
            manifest = {"engine": "litpipe.gate", "engine_version": ENGINE_VERSION, "schema": SCHEMA,
                        "name": sp.name, "command": command, "cli": list(argv) if argv is not None else None,
                        "spec_sha256": sp.sha256, "spec": sp.eff, "inputs": inputs, "holdings": hinfo,
                        "drawn": {k: dinfo.get(k) for k in ("n", "sha256")}}
            rd = write_run(Path(out_root) if out_root else lib, result, report, out_rows, manifest, run_id=run_id)
            res["run_dir"] = str(rd)
            say(f"wrote run folder {rd}")
        else:
            say("dry run: nothing written (pass --write)")
    except GateAbort as e:
        res.update(exit_code=EXIT_REFUSED, error=str(e), reasons=e.reasons)
        if not quiet:
            print(f"[REFUSED] {e}", file=sys.stderr)
        say(_summary_line(e.reasons))
    except GateError as e:
        res.update(exit_code=e.exit_code, error=str(e))
        if not quiet:
            print(f"[ERR] {e}", file=sys.stderr)
    except config.ConfigError as e:
        res.update(exit_code=EXIT_USAGE, error=str(e))
        if not quiet:
            print(f"[ERR] {e}", file=sys.stderr)
    return res


def _print_summary(rep, say, command):
    f = rep["funnel"]
    say(f"gate {rep['name']} ({command}): {f['harvest_rows']} harvest rows, {f['rows_no_doi']} without a DOI, "
        f"{f['rows_year_dropped']} out of the year window (row scope), {f['records']} records, "
        f"{f['candidates']} candidates")
    for k, v in f["reasons"].items():
        say(f"  {k:<32} {v:>7}")
    p = rep.get("plan")
    if p:
        say(f"plan: target {p['target']}, selected {p['selected']}, tier {p['tier']}, trim overflow "
            f"{p['trim']['overflow']}, order {p['order']['method']}")
        for name, v in rep.get("lanes", {}).items():
            say(f"  lane {name}: {v.get('taken', 0)} of {v.get('candidates', 0)} candidates")
        inv = rep["invariants"]
        say(f"invariants: {len(inv['checks'])} passed; holdings-key collisions dropped {len(inv['collisions'])}")
    for w in rep["warnings"]:
        say(f"WARNING: {w}")


# ---------------------------------------------------------------- promote (step 14)
def promote(spec, lib, run_id, *, out_root=None, force=False, say=print):
    from litpipe import runner as _runner
    from litpipe import worklists as _w
    if not run_id:
        raise GateError("promote needs --run RUN_ID")
    rd = Path(out_root or lib) / GATE_DIR / spec.name / run_id
    try:
        man = json.loads((rd / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        raise GateError(f"run {run_id}: no readable manifest.json in {rd} ({type(e).__name__})") from None
    if man.get("name") != spec.name or not (man.get("outputs") or {}).get("selection"):
        raise GateError(f"run {run_id} is not a plan run of gate {spec.name!r}")
    pairs = [(rd / man["outputs"]["selection"], Path(lib) / f"_{spec.name}_gate_selection.csv")]
    for ln, fn in sorted((man["outputs"].get("lanes") or {}).items()):
        pairs.append((rd / fn, Path(lib) / f"_{spec.name}_gate_lane_{ln}.csv"))
    for src, _ in pairs:
        if not src.exists():
            raise GateError(f"run {run_id} lacks {src.name}")
    existing = set()
    try:
        existing = {Path(lib) / e.name for e in os.scandir(lib)
                    if e.name.startswith("_") and e.name.endswith(".csv")
                    and (e.name.endswith("_gate_selection.csv") or "_gate_lane_" in e.name)}
    except OSError:
        pass
    tags = {}
    for p in sorted(existing | {dst for _, dst in pairs}):
        tags.setdefault(_runner.batch_tag(p.name), []).append(p.name)
    clash = {t: v for t, v in tags.items() if len(v) > 1}
    if clash:
        raise GateError("promote refused: these promoted files would share a runner batch tag: "
                        + "; ".join(f"{t}: {', '.join(v)}" for t, v in sorted(clash.items())))
    pending = []
    for _, dst in pairs:
        try:
            pend = _w.Pool(dst).pending()
        except _w.WorklistError as e:
            raise GateError(str(e)) from None
        if pend:
            pending.append(f"{dst.name}: {sum(len(b['dois']) for b in pend)} DOI(s) in {len(pend)} batch(es)")
    if pending and not force:
        raise GateAbort("promote refused: pending batches (sweep or mark them first, or pass --force): "
                        + "; ".join(pending), ["promote:pending"])
    done = []
    for src, dst in pairs:
        lit_util.atomic_write_text(str(dst), src.read_text(encoding="utf-8"), newline="\n")
        done.append(str(dst))
        say(f"promoted {src.name} to {dst}")
    return {"promoted": done, "pending_overridden": pending if force else []}


# ---------------------------------------------------------------- CLI
class _Parser(argparse.ArgumentParser):
    def error(self, message):
        self.print_usage(sys.stderr)
        print(f"{self.prog}: error: {message}", file=sys.stderr)
        raise SystemExit(EXIT_USAGE)


def main(argv=None) -> int:
    lit_util.utf8_stdout()
    args_in = sys.argv[1:] if argv is None else list(argv)
    ap = _Parser(prog="python -m litpipe.gate",
                 description="Spec-driven pool-selection gate over a scoped forward harvest (dry by default).")
    sub = ap.add_subparsers(dest="command", required=True, parser_class=_Parser)
    for name in ("build", "plan", "report", "promote"):
        p = sub.add_parser(name)
        p.add_argument("--project", required=True, help="the registry key of the project")
        p.add_argument("--spec", required=True, help="the gate spec (JSON, schema litpipe.gate/1)")
        p.add_argument("--lib-dir", default=None, help="use this library instead of the registry's")
        p.add_argument("--out-root", default=None, help="parent of _gate/ (default: the library)")
        if name == "promote":
            p.add_argument("--run", required=True, help="the run id (a folder name under _gate/<name>/)")
            p.add_argument("--force", action="store_true", help="promote even while batches are pending")
            continue
        p.add_argument("--holdings-as-of", default=None, help="replay: the held set, one DOI per line")
        p.add_argument("--write", action="store_true", help="write the run folder (default: dry run)")
        p.add_argument("--json", default=None, help="also write the report JSON here")
        if name in ("plan", "report"):
            p.add_argument("--pool", default=None, help="plan from this recorded candidate pool CSV")
    try:
        args = ap.parse_args(args_in)
    except SystemExit as e:              # usage errors exit 1 (house convention), --help exits 0
        return int(e.code or 0)
    kw = {"project": args.project, "spec": args.spec, "lib_dir": args.lib_dir, "out_root": args.out_root,
          "argv": args_in}
    if args.command == "promote":
        kw.update(run_id=args.run, force=args.force)
    else:
        kw.update(holdings_as_of=args.holdings_as_of, write=args.write, json_path=args.json,
                  pool=getattr(args, "pool", None))
    res = run(args.command, **kw)
    if args.command == "report" and res.get("report") is not None:
        print(render_markdown(res["report"]))
    return res["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
