"""litpipe.s2: the Semantic Scholar client (K1; dispatch W2-D1). The only code that talks to
api.semanticscholar.org; every request goes through litpipe.net (host policy, per-attempt ledger,
redaction, typed Outcomes) and no function returns a bare empty list for a failed call.

Public functions (design 2026-09-23 section 3; every one takes an optional `session=`):
  paper_batch(ids, fields)                  POST /graph/v1/paper/batch in chunks of 500 -> Outcome whose
                                            payload is aligned with `ids`: a dict (the record), None (S2
                                            has no record under that id; "unresolved", not "absent"), or
                                            an Outcome (that position was not answered: its chunk failed,
                                            or the id is not a DOI or an S2 id and was never sent).
  citations(id, fields, *, expected)        paged GET /paper/{id}/citations -> Walk
  citations_windowed(id, fields, expected)  publicationDateOrYear windows for lists over 9,999 -> Walk
  references(id, fields, *, expected)       paged GET /paper/{id}/references -> Walk
  batch_nested(ids, rel, fields, *, cap)    nested citations.* / references.* batch, verified per id,
                                            mismatches re-fetched by paged GET -> dict[id, Walk]
  match_title(title, *, year, ...)          GET /paper/search/match -> Match | NotFound | Failed
  recommend_for_paper(id, pool, limit)      GET /recommendations/v1/papers/forpaper/{id} -> Outcome
  recommend(pos, neg, limit)                POST /recommendations/v1/papers -> Outcome

Walk states (WalkState): complete (rows == count), empty (count 0, or `[]` with `offset`), elided
(`data: null`, the publisher-elision marker), not_found (404), capped_9999 (the list runs past the
9,999 rows S2 lets anyone page to), failed (reason, kind, status). Every Walk carries n_rows,
count_at_walk, walked_at and attempts. A failed Walk has rows=None and n_rows=None, never [] or 0:
the rows it did fetch sit in `partial`, which is diagnosis, never a result.

Paging (probe P2, re-probed 2026-09-30): S2 answers `offset + limit >= 10000` with HTTP 400
"offset + limit must be < 10000", so a page asks for limit=min(1000, 9999 - offset) and paging stops
at offset 9999. No request with offset + limit >= 10000 is ever built (_page_params raises first).

Retry (P7 d_proposed, through litpipe.net): first wait 2 s (never 0), doubling plus U(0, 0.5) s
jitter, at most 6 retries on 429/500/502/503/504; transport failures at most 2 retries (net's cap);
Retry-After honoured up to 600 s, above that the host is deferred and the call is DEFERRED. A final
429 or 406 refuses the host for the run (net), so later calls return REFUSED without sending.

Spacing, from the END of the previous attempt, retries included (litpipe.state): 6.5 s unkeyed (a
consumer project's measurement; the design's 3.2 s drew 429s), 1.1 s with a key (S2: "1 request per
second rate across all endpoints"). The row is registered per call, not at import, so importing
this module never changes the process-wide host table.

Run accounting (Session): every attempt counts against the run budget (projects.json
s2.max_requests_per_run, default 2000; null = no cap), checked before each send, mid-retry too; a
spent budget returns DEFERRED and marks the session aborted. `breaker` (default 3) consecutive failed
calls trip the circuit breaker: later calls return DEFERRED without sending. A walker exits 3 when
`session.aborted`. `session.summary()` counts calls that sent something, attempts, attempts that got
no 2xx/3xx answer (retried or final), final kinds, walk states and bytes.

Key: S2_API_KEY from the environment, sent only in the x-api-key header, never logged (the ledger
keeps header names, and redact() strips x-api-key values anyway), never printed: key_status() says
"key: present" or "key: absent".

projects.json "s2" block (all optional; config philosophy, not env vars):
  spacing_s             unkeyed spacing, default 6.5, at least 1.1
  spacing_keyed_s       keyed spacing, default 1.1, at least 1.1
  max_requests_per_run  attempts per Session, default 2000; null for no cap
  breaker               consecutive failed calls before the breaker trips, default 3

Tests point BASE at a loopback mock (the S2 row is then copied onto that host) or stub
litpipe.net's transport; see tests/test_s2_client.py.
"""
from __future__ import annotations

import argparse
import dataclasses
import difflib
import os
import re
import sys
import unicodedata
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, timedelta
from enum import StrEnum
from typing import Any
from urllib.parse import quote

from litpipe import config, hosts, ledger, net
from litpipe import doi as _doi
from litpipe.hosts import RetryPolicy
from litpipe.outcomes import Kind, Outcome

BASE = "https://api.semanticscholar.org"
S2_HOST = "api.semanticscholar.org"
GRAPH = "/graph/v1"
RECS = "/recommendations/v1"
KEY_ENV = "S2_API_KEY"

BATCH_MAX_IDS = 500              # swagger /paper/batch: "Can only process 500 paper ids at a time."
BATCH_NESTED_MAX = 9999          # swagger /paper/batch: "Can only return up to 9999 citations at a time."
PAGE_MAX = 1000                  # swagger /citations, /references: limit "Must be <= 1000"
OFFSET_CAP = 10_000              # undocumented; HTTP 400 "offset + limit must be < 10000" (P2, 2026-09-30)
REACHABLE = OFFSET_CAP - 1       # at most 9,999 rows of any one list can be paged to
WINDOW_PROBE_OFFSET = 8999       # citations_windowed sizes a window with offset=8999&limit=1
DEFAULT_NESTED_CAP = 9000        # headroom under the 9,999 nested cap (P1-C6: over it, silent truncation)

DEFAULT_SPACING_S = 6.5
KEYED_SPACING_S = 1.1
MIN_SPACING_S = 1.1
DEFAULT_RUN_BUDGET = 2000
DEFAULT_BREAKER = 3

TIMEOUT_GET = (10, 30)
TIMEOUT_BATCH = (10, 60)
TIMEOUT_NESTED = (10, 90)        # one nested call took 5.7 s of server time; over-cap with retries 10.5 s

# P7 d_proposed, pinned here so the S2 row cannot silently inherit a changed default.
S2_RETRY = RetryPolicy(max_retries=6, first_wait_s=2.0, factor=2.0, jitter_s=0.5, max_wait_s=120.0,
                       statuses=frozenset({429, 500, 502, 503, 504}), transport_retries=2)
RETRY_AFTER_CAP_S = 600.0

# Default field sets (design 4.1 step 5, 4.1 step 2, 4.5).
CITATION_FIELDS = ("paperId", "externalIds", "title", "year", "publicationDate", "authors", "venue",
                   "citationCount")
METADATA_FIELDS = ("externalIds", "citationCount", "referenceCount", "publicationDate", "year", "title",
                   "abstract", "openAccessPdf", "venue")
MATCH_FIELDS = ("paperId", "externalIds", "title", "year", "authors")
REC_FIELDS = ("paperId", "externalIds", "title", "year", "publicationDate")

_REL = {"citations": ("citingPaper", "citationCount"), "references": ("citedPaper", "referenceCount")}
_ID_PREFIX = re.compile(r"(?i)^(DOI|CorpusId|ARXIV|MAG|ACL|PMID|PMCID|URL):(.+)$")
_PAPER_SHA = re.compile(r"^[0-9a-f]{40}$")
_FAILED_KINDS = frozenset(k for k in Kind if k not in (Kind.OK, Kind.NO_MATCH))


class _Missing:
    def __repr__(self):
        return "<missing>"


_MISSING = _Missing()


# ------------------------------------------------------------------------------ key and settings
def _key():
    """The key, for the header only. Never returned by any public function."""
    return (os.environ.get(KEY_ENV) or "").strip() or None


def key_present() -> bool:
    return _key() is not None


def key_status() -> str:
    return "key: present" if key_present() else "key: absent"


@dataclass(frozen=True)
class Settings:
    spacing_s: float = DEFAULT_SPACING_S
    spacing_keyed_s: float = KEYED_SPACING_S
    max_requests_per_run: int | None = DEFAULT_RUN_BUDGET
    breaker: int = DEFAULT_BREAKER


def _number(block, name, default, *, minimum):
    v = block.get(name, default)
    if isinstance(v, bool) or not isinstance(v, (int, float)) or v < minimum:
        raise config.ConfigError(f"s2.{name} must be a number >= {minimum}, got {v!r}")
    return float(v)


def settings(cfg=None) -> Settings:
    """The projects.json `s2` block, validated (a wrong value is CONFIG: the run aborts)."""
    b = config.s2(cfg)
    budget = b.get("max_requests_per_run", DEFAULT_RUN_BUDGET)
    if budget is not None and (isinstance(budget, bool) or not isinstance(budget, int) or budget < 1):
        raise config.ConfigError(f"s2.max_requests_per_run must be a positive integer or null, got {budget!r}")
    breaker = b.get("breaker", DEFAULT_BREAKER)
    if isinstance(breaker, bool) or not isinstance(breaker, int) or breaker < 1:
        raise config.ConfigError(f"s2.breaker must be a positive integer, got {breaker!r}")
    return Settings(spacing_s=_number(b, "spacing_s", DEFAULT_SPACING_S, minimum=MIN_SPACING_S),
                    spacing_keyed_s=_number(b, "spacing_keyed_s", KEYED_SPACING_S, minimum=MIN_SPACING_S),
                    max_requests_per_run=budget, breaker=breaker)


def host_policy(s: Settings | None = None) -> hosts.HostPolicy:
    """The S2 row as this client runs it, for the host BASE points at: the table row with the
    pinned retry, the 600 s Retry-After cap and the key-dependent interval."""
    s = s or Settings()
    interval = s.spacing_keyed_s if key_present() else s.spacing_s
    return dataclasses.replace(hosts.policy(S2_HOST), host=hosts.host_of(BASE), min_interval_s=interval,
                               retry=S2_RETRY, retry_after_cap_s=RETRY_AFTER_CAP_S)


def apply_host_policy(s: Settings | None = None) -> hosts.HostPolicy:
    """Register host_policy() when the table differs (keyed tuning, W1-A1 forward)."""
    want = host_policy(s)
    if hosts.policy(want.host) != want:
        hosts.register(want)
    return want


# ------------------------------------------------------------------------------ run accounting
class RunBudgetSpent(Exception):
    """The session's attempt budget is spent; litpipe.net turns it into DEFERRED, nothing sent."""

    def __init__(self, host, budget):
        self.host, self.budget, self.retry_after = host, budget, None
        super().__init__(f"s2 run budget of {budget} attempts spent")


class _RunState:
    """The shared state as one Session sees it: acquire() counts the attempt against the run
    budget before claiming the pacing slot, so a retry inside litpipe.net is budget-checked too.
    Everything else is the shared state's own."""

    def __init__(self, inner, session):
        self._inner = inner
        self._session = session
        inner_exc = getattr(inner, "BudgetExhausted", None)
        # litpipe.net catches `st.BudgetExhausted`; an except clause takes a tuple of classes.
        self.BudgetExhausted = (inner_exc, RunBudgetSpent) if inner_exc else RunBudgetSpent

    def acquire(self, host, *a, **k):
        s = self._session
        if s.budget is not None and s.attempts >= s.budget:
            s.budget_spent = True
            raise RunBudgetSpent(host, s.budget)
        got = self._inner.acquire(host, *a, **k)
        s.attempts += 1
        return got

    def release(self, host, ok=True, *a, **k):
        if not ok:
            self._session.attempts_not_ok += 1
        return self._inner.release(host, ok, *a, **k)

    def __getattr__(self, name):
        return getattr(self._inner, name)


class Session:
    """One run's view of S2: budget, circuit breaker and counters. Module functions use a
    process-wide default session unless given one; a walker makes its own per run."""

    def __init__(self, *, budget=_MISSING, breaker=None, cfg=None, state=None):
        self.cfg = cfg
        self.settings = settings(cfg)
        self.budget = self.settings.max_requests_per_run if budget is _MISSING else budget
        self.breaker = breaker if breaker is not None else self.settings.breaker
        self.state = state
        self.calls = 0
        self.attempts = 0
        self.attempts_not_ok = 0
        self.bytes = 0
        self.consecutive_failed = 0
        self.tripped = False
        self.budget_spent = False
        self.kinds: Counter = Counter()
        self.walks: Counter = Counter()

    @property
    def aborted(self) -> str | None:
        """"budget" or "breaker" when the run must stop (a walker exits 3), else None."""
        if self.budget_spent:
            return "budget"
        if self.tripped:
            return "breaker"
        return None

    def _inner_state(self):
        if self.state is not None:
            return self.state
        if net.STATE is not None:
            return net.STATE
        import litpipe.state as st
        return st

    def _record(self, out: Outcome, sent: bool):
        if sent and out.attempts:
            self.calls += 1
        self.kinds[str(out.kind)] += 1
        p = out.payload
        if isinstance(p, net.Response):
            self.bytes += p.total_bytes
        if out.kind in _FAILED_KINDS:
            self.consecutive_failed += 1
            if self.consecutive_failed >= self.breaker:
                self.tripped = True
        else:
            self.consecutive_failed = 0

    def summary(self) -> dict:
        return {"calls": self.calls, "attempts": self.attempts, "attempts_not_ok": self.attempts_not_ok,
                "budget": self.budget, "bytes": self.bytes, "kinds": dict(self.kinds),
                "walks": dict(self.walks), "aborted": self.aborted, "key": key_status()}

    def summary_line(self) -> str:
        w = self.walks
        return (f"[s2] {key_status()} calls={self.calls} attempts={self.attempts} "
                f"not_ok_attempts={self.attempts_not_ok} failed={w.get('failed', 0)} "
                f"elided={w.get('elided', 0)} capped={w.get('capped_9999', 0)} bytes={self.bytes}"
                + (f" ABORTED={self.aborted}" if self.aborted else ""))


_DEFAULT_SESSION: Session | None = None


def default_session() -> Session:
    global _DEFAULT_SESSION
    if _DEFAULT_SESSION is None:
        _DEFAULT_SESSION = Session()
    return _DEFAULT_SESSION


def reset_default_session():
    global _DEFAULT_SESSION
    _DEFAULT_SESSION = None


# ------------------------------------------------------------------------------ ids and requests
def paper_id(x) -> str:
    """The S2 id for a DOI (bare, URL or `DOI:` form; normalised by litpipe.doi) or an S2 id
    (40-hex paperId, CorpusId:, ARXIV:, PMID:, ...). ValueError when it is neither."""
    s = str(x or "").strip()
    m = _ID_PREFIX.match(s)
    if m and m.group(1).upper() != "DOI":
        return s
    if _PAPER_SHA.match(s):
        return s
    d = _doi.normalise_structured(m.group(2) if m else s)
    if d is None:
        raise ValueError(f"not a DOI or a Semantic Scholar id: {s!r}")
    return "DOI:" + d


def _path_id(pid: str, safe: str) -> str:
    """`DOI:` + quote(doi, safe=...): '/' literal for the graph API (P1 A_*_q_slash), nothing
    literal for recommendations (P8-C5)."""
    prefix, _, rest = pid.partition(":")
    if not rest:
        return quote(pid, safe="")
    return f"{prefix}:{quote(rest, safe=safe)}"


def _fields(fields) -> str:
    if isinstance(fields, str):
        parts = fields.split(",")
    else:
        parts = list(fields)
    out = [str(f).strip() for f in parts if str(f).strip()]
    if not out:
        raise ValueError("fields must name at least one field")
    return ",".join(out)


def _page_params(offset: int, limit: int, extra=None) -> dict:
    """Paging parameters; refuses to build a request past S2's offset cap."""
    if offset < 0 or limit < 1 or offset + limit >= OFFSET_CAP:
        raise ValueError(f"offset {offset} + limit {limit} must be < {OFFSET_CAP} (S2 answers 400)")
    params = dict(extra) if extra is not None else {}
    params.update(offset=offset, limit=limit)
    return params


def _json_body(out: Outcome):
    p = out.payload
    if not isinstance(p, net.Response) or p.content is None:
        return _MISSING
    try:
        return p.json()
    except ValueError:
        return _MISSING


def _error_text(out: Outcome) -> str:
    body = _json_body(out)
    if isinstance(body, dict):
        for k in ("error", "message"):
            if isinstance(body.get(k), str):
                return ledger.redact(body[k][:200])
    return ""


def _host() -> str:
    return hosts.host_of(BASE)


def _request(session: Session | None, method, path, *, params=None, json=None, timeout=None,
             purpose="") -> Outcome:
    s = session or default_session()
    apply_host_policy(s.settings)
    if s.aborted:
        out = Outcome(Kind.DEFERRED, host=_host(), detail=f"s2 run aborted ({s.aborted}); nothing sent")
        s._record(out, sent=False)
        return out
    key = _key()
    out = net.request(method, BASE.rstrip("/") + path, params=params, json=json,
                      headers={"x-api-key": key} if key else None,
                      timeout=TIMEOUT_GET if timeout is None else timeout,
                      validate=net.expect_json, purpose=f"s2 {purpose}".strip(),
                      state=_RunState(s._inner_state(), s), cfg=s.cfg)
    s._record(out, sent=True)
    if not out.ok and out.kind is not Kind.NO_MATCH:
        err = _error_text(out)
        if err and err not in out.detail:
            out = dataclasses.replace(out, detail=f"{out.detail}: {err}" if out.detail else err)
    return out


def _malformed(out: Outcome, why: str) -> Outcome:
    return dataclasses.replace(out, kind=Kind.ERROR, detail=f"unexpected S2 response: {why}")


# ------------------------------------------------------------------------------ paper_batch
def paper_batch(ids, fields=METADATA_FIELDS, *, session: Session | None = None) -> Outcome:
    """POST /paper/batch over `ids` in chunks of 500. The payload is aligned with `ids` (same
    length and order): dict, None (no S2 record under that id) or an Outcome (not answered). The
    returned kind is OK only when every chunk answered; otherwise it is the first failed chunk's
    kind, and the answered positions are still filled. DOIs come back in publisher casing
    (V_P4_P1): join on litpipe.doi.normalise, not on the returned string."""
    ids = list(ids)
    fl = _fields(fields)
    slots: list[Any] = [None] * len(ids)
    send: list[tuple[int, str]] = []
    skipped = 0
    for i, raw in enumerate(ids):
        try:
            send.append((i, paper_id(raw)))
        except ValueError as e:
            slots[i] = Outcome(Kind.SKIPPED, host=_host(), detail=ledger.redact(str(e)))
            skipped += 1
    first_fail: Outcome | None = None
    last: Outcome | None = None
    attempts = elapsed = 0
    n_chunks = n_ok = 0
    for c in range(0, len(send), BATCH_MAX_IDS):
        chunk = send[c:c + BATCH_MAX_IDS]
        n_chunks += 1
        out = _request(session, "POST", f"{GRAPH}/paper/batch", params={"fields": fl},
                       json={"ids": [pid for _, pid in chunk]}, timeout=TIMEOUT_BATCH,
                       purpose=f"paper_batch ids={len(chunk)}")
        attempts += out.attempts
        elapsed += out.elapsed_ms
        last = out
        body = _json_body(out) if out.ok else _MISSING
        if out.ok and not (isinstance(body, list) and len(body) == len(chunk)):
            got = len(body) if isinstance(body, list) else type(body).__name__
            out = _malformed(out, f"batch of {len(chunk)} ids answered with {got} rows")
        if not out.ok:
            first_fail = first_fail or out
            for i, _ in chunk:
                slots[i] = out
            continue
        n_ok += 1
        for (i, _), row in zip(chunk, body):
            slots[i] = row if (row is None or isinstance(row, dict)) else _malformed(out, "row is not an object")
    detail = f"{n_ok} of {n_chunks} chunks answered" + (f"; {skipped} ids skipped (not a DOI or S2 id)"
                                                      if skipped else "")
    base = first_fail or last
    return Outcome(first_fail.kind if first_fail else Kind.OK,
                   status=base.status if base else None, host=_host(),
                   detail=f"{detail}; first failure: {first_fail.detail}" if first_fail else detail,
                   attempts=attempts, elapsed_ms=elapsed,
                   retry_after=first_fail.retry_after if first_fail else None, payload=slots)


# ------------------------------------------------------------------------------ walks
class WalkState(StrEnum):
    COMPLETE = "complete"
    EMPTY = "empty"
    ELIDED = "elided"
    NOT_FOUND = "not_found"
    CAPPED = "capped_9999"
    FAILED = "failed"


@dataclass(frozen=True)
class Walk:
    """One seed's citation or reference list. rows: paper dicts (the citingPaper / citedPaper
    object; edge fields such as contexts or isInfluential, when requested, under "_edge"); a list
    for complete, empty ([]) and capped_9999; None for elided, not_found and failed."""
    state: WalkState
    id: str
    rel: str
    rows: list | None
    n_rows: int | None
    count_at_walk: int | None
    walked_at: str
    attempts: int
    reason: str = ""
    kind: Kind | None = None
    status: int | None = None
    retry_after: float | None = None
    partial: list = field(default_factory=list)
    n_unreachable_est: int | None = None

    @property
    def failed(self) -> bool:
        return self.state is WalkState.FAILED

    @property
    def answered(self) -> bool:
        """S2 gave a definite answer (every state but failed): the seed's stored rows may be replaced."""
        return self.state is not WalkState.FAILED


def _walk(session, state, pid, rel, *, rows=None, expected=None, attempts=0, reason="", out=None,
          partial=None, unreachable=None, tally=True) -> Walk:
    w = Walk(state=state, id=pid, rel=rel, rows=rows, n_rows=len(rows) if rows is not None else None,
             count_at_walk=expected, walked_at=ledger.now_iso(), attempts=attempts, reason=reason,
             kind=out.kind if (out is not None and state is WalkState.FAILED) else None,
             status=out.status if out is not None else None,
             retry_after=out.retry_after if out is not None else None,
             partial=list(partial) if partial else [], n_unreachable_est=unreachable)
    if tally:
        (session or default_session()).walks[str(state)] += 1
    return w


def _disclaimer(paper) -> str:
    """openAccessPdf.disclaimer of a paper object, "" when absent (it carries S2's elision notice)."""
    pdf = paper.get("openAccessPdf") if isinstance(paper, dict) else None
    d = pdf.get("disclaimer") if isinstance(pdf, dict) else None
    return d if isinstance(d, str) else ""


def _unwrap(item, wrapper):
    if not isinstance(item, dict):
        return item
    paper = item.get(wrapper, _MISSING)
    if paper is _MISSING:
        return item
    if not isinstance(paper, dict):
        return paper
    edge = {k: v for k, v in item.items() if k != wrapper}
    return {**paper, "_edge": edge} if edge else dict(paper)


def _paged(session, pid, rel, fields, expected, *, extra=None, purpose=None, tally=True) -> Walk:
    wrapper, _ = _REL[rel]
    fl = _fields(fields)
    query = dict(extra) if extra is not None else {}
    query["fields"] = fl
    rows: list = []
    offset = attempts = 0
    last = None

    def walk(state, **kw):
        return _walk(session, state, pid, rel, expected=expected, attempts=attempts, tally=tally, **kw)

    def failed(out, reason):
        return walk(WalkState.FAILED, out=out, reason=reason, partial=rows)

    if expected == 0:
        return walk(WalkState.EMPTY, rows=[], reason="count 0; no call")
    path = f"{GRAPH}/paper/{_path_id(pid, '/')}/{rel}"
    while True:
        if offset >= REACHABLE:
            if expected is not None and len(rows) == expected:
                break
            unreachable = max(0, expected - len(rows)) if expected is not None else None
            return walk(WalkState.CAPPED, rows=rows, out=last, unreachable=unreachable,
                        reason=f"list continues past row {REACHABLE}; offset + limit must stay < {OFFSET_CAP}")
        limit = min(PAGE_MAX, REACHABLE - offset)
        out = _request(session, "GET", path, params=_page_params(offset, limit, query),
                       purpose=purpose or f"{rel} offset={offset}")
        attempts += out.attempts
        last = out
        if not out.ok:
            if out.kind is Kind.NO_MATCH and offset == 0:
                return walk(WalkState.NOT_FOUND, out=out, reason=_error_text(out) or out.detail)
            return failed(out, out.detail or str(out.kind))
        body = _json_body(out)
        if not isinstance(body, dict) or "data" not in body:
            return failed(_malformed(out, "no data key"), "malformed: no data key")
        data = body["data"]
        if data is None:                                  # the publisher-elision marker (V_P6_P8)
            if offset:
                return failed(_malformed(out, "data: null after the first page"), "malformed: data null mid-walk")
            disclaimer = _disclaimer(body.get("citingPaperInfo"))
            return walk(WalkState.ELIDED, out=out,
                        reason=ledger.redact(disclaimer[:300]) if disclaimer else "data: null")
        if not isinstance(data, list):
            return failed(_malformed(out, "data is not a list"), "malformed: data not a list")
        if not data and "offset" not in body:
            return failed(_malformed(out, "[] without offset"), "malformed: [] without offset")
        rows.extend(_unwrap(item, wrapper) for item in data)
        nxt = body.get("next")
        if nxt is None:
            break
        if isinstance(nxt, bool) or not isinstance(nxt, int) or nxt <= offset:
            return failed(_malformed(out, f"next={nxt!r} after offset {offset}"), "malformed: next does not advance")
        offset = nxt
    if expected is not None and len(rows) != expected:
        return failed(last, f"count_mismatch: {len(rows)} rows, count {expected}")
    return walk(WalkState.EMPTY if not rows else WalkState.COMPLETE, rows=rows, out=last)


def citations(id, fields=CITATION_FIELDS, *, expected, session: Session | None = None) -> Walk:
    """Every citer of `id` by paged GET. `expected` is the citationCount from the same pass
    (paper_batch); 0 answers empty with no call; None skips the count check. A list longer than
    9,999 comes back capped_9999 with the first 9,999 rows (use citations_windowed)."""
    return _paged(session, paper_id(id), "citations", fields, expected)


def references(id, fields=CITATION_FIELDS, *, expected, session: Session | None = None) -> Walk:
    """Every reference of `id` by paged GET (same paging rule). `data: null` is elided (publisher
    elision; fall back to the regex parse); `[]` with offset is empty; `[]` while referenceCount > 0
    is failed (count_mismatch), never empty (design 4.2 step 2)."""
    return _paged(session, paper_id(id), "references", fields, expected)


def citations_windowed(id, fields=CITATION_FIELDS, expected=None, *, year_from=None, year_to=None,
                       session: Session | None = None) -> Walk:
    """Citers of a seed with more than 9,999, collected by publicationDateOrYear windows. A window
    is sized with one offset=8999&limit=1 call and halved while it holds 9,000 rows or more; each
    window is then paged under the cap. The union (deduplicated on paperId) is capped_9999 always:
    the date filter drops year-only and undated citers (P3-C1), so n_unreachable_est = expected -
    union. year_from should be the seed's publication year (default 1900)."""
    pid = paper_id(id)
    s = session or default_session()
    fl = _fields(fields)
    if "paperId" not in fl.split(","):
        fl = "paperId," + fl
    lo = date(int(year_from or 1900), 1, 1)
    hi = date(int(year_to or date.today().year), 12, 31)
    todo = [(lo, hi)]
    rows: list = []
    seen: set = set()
    attempts = 0
    oversized_days = 0
    last = None
    path = f"{GRAPH}/paper/{_path_id(pid, '/')}/citations"
    while todo:
        a, b = todo.pop(0)
        window = f"{a.isoformat()}:{b.isoformat()}"
        probe = _request(s, "GET", path, params=_page_params(WINDOW_PROBE_OFFSET, 1,
                                                              {"publicationDateOrYear": window, "fields": "paperId"}),
                         purpose=f"citations window-size {window}")
        attempts += probe.attempts
        last = probe
        body = _json_body(probe) if probe.ok else _MISSING
        if probe.ok and not (isinstance(body, dict) and isinstance(body.get("data"), list)):
            probe = _malformed(probe, "window probe without a data list")
        if not probe.ok:
            if probe.kind is Kind.NO_MATCH:
                return _walk(s, WalkState.NOT_FOUND, pid, "citations", expected=expected, attempts=attempts,
                             out=probe, reason=_error_text(probe) or probe.detail)
            return _walk(s, WalkState.FAILED, pid, "citations", expected=expected, attempts=attempts, out=probe,
                         reason=f"window {window}: {probe.detail}", partial=rows)
        if body["data"] and b > a:
            mid = a + timedelta(days=(b - a).days // 2)
            todo[:0] = [(a, mid), (mid + timedelta(days=1), b)]
            continue
        w = _paged(s, pid, "citations", fl, None, extra={"publicationDateOrYear": window},
                   purpose=f"citations window {window}", tally=False)   # a window is not a seed walk
        attempts += w.attempts
        if w.state is WalkState.FAILED:
            return _walk(s, WalkState.FAILED, pid, "citations", expected=expected, attempts=attempts,
                         reason=f"window {window}: {w.reason}", partial=rows + w.partial,
                         out=Outcome(w.kind or Kind.ERROR, status=w.status, retry_after=w.retry_after))
        if w.state is WalkState.NOT_FOUND:
            return _walk(s, WalkState.NOT_FOUND, pid, "citations", expected=expected, attempts=attempts,
                         reason=w.reason)
        if w.state is WalkState.CAPPED:
            oversized_days += 1
        for r in w.rows if w.rows is not None else ():
            key = r.get("paperId") if isinstance(r, dict) else None
            if key is not None:
                if key in seen:
                    continue
                seen.add(key)
            rows.append(r)
    unreachable = max(0, expected - len(rows)) if expected is not None else None
    reason = "date windows; year-only and undated citers are unreachable"
    if oversized_days:
        reason += f"; {oversized_days} one-day window(s) still over the cap"
    return _walk(s, WalkState.CAPPED, pid, "citations", rows=rows, expected=expected, attempts=attempts,
                 out=last, unreachable=unreachable, reason=reason)


def pack_ffd(counts: dict, cap: int, max_ids: int = BATCH_MAX_IDS) -> list[list]:
    """First-fit-decreasing bins: each bin's counts sum to at most `cap`, at most `max_ids` ids."""
    bins: list[list] = []
    sums: list[int] = []
    for key, n in sorted(counts.items(), key=lambda kv: -kv[1]):
        for j, tot in enumerate(sums):
            if tot + n <= cap and len(bins[j]) < max_ids:
                bins[j].append(key)
                sums[j] += n
                break
        else:
            bins.append([key])
            sums.append(n)
    return bins


def batch_nested(ids, rel, fields=CITATION_FIELDS, *, cap=DEFAULT_NESTED_CAP, counts=None,
                 session: Session | None = None) -> dict:
    """Citations or references of many seeds through POST /paper/batch with nested fields.
    Packs ids first-fit-decreasing by count (at most `cap` rows and 500 ids per call) and asks for
    the top-level count in the same call. Per id: len == count is complete (empty at 0); a null list
    is elided; a mismatch (the nested cap truncates silently and not from the tail, P1-C6) is
    re-fetched by paged GET. Seeds over `cap` go straight to paged GET. `counts` (id -> count) may
    come from the caller's paper_batch; without it one metadata pass is made. Keys of the result are
    the ids as given."""
    if rel not in _REL:
        raise ValueError(f"rel must be 'citations' or 'references', got {rel!r}")
    if not 0 < cap <= BATCH_NESTED_MAX:
        raise ValueError(f"cap must be in 1..{BATCH_NESTED_MAX}")
    s = session or default_session()
    _, count_field = _REL[rel]
    fl = _fields(fields)
    order = list(dict.fromkeys(ids))
    result: dict = {}
    known: dict = {}
    if counts is None:
        meta = paper_batch(order, (count_field,), session=s)
        for key, row in zip(order, meta.payload):
            if isinstance(row, Outcome):
                result[key] = _walk(s, WalkState.FAILED, str(key), rel, attempts=0, out=row,
                                    reason=f"metadata: {row.detail or row.kind}")
            elif row is None:
                result[key] = _walk(s, WalkState.NOT_FOUND, str(key), rel, reason="no S2 record (batch null)")
            elif not isinstance(row.get(count_field), int):
                result[key] = _walk(s, WalkState.FAILED, str(key), rel, reason=f"metadata: no {count_field}",
                                    out=_malformed(meta, f"no {count_field}"))
            else:
                known[key] = row[count_field]
    else:
        for key in order:
            n = counts.get(key)
            if isinstance(n, bool) or not isinstance(n, int) or n < 0:
                raise ValueError(f"counts[{key!r}] must be a non-negative int, got {n!r}")
            known[key] = n
    pids = {}
    for key in known:
        pids[key] = paper_id(key)
    small = {}
    for key, n in known.items():
        if n == 0:
            result[key] = _walk(s, WalkState.EMPTY, pids[key], rel, rows=[], expected=0, reason="count 0; no call")
        elif n > cap:
            walker = citations if rel == "citations" else references
            result[key] = walker(pids[key], fl, expected=n, session=s)
        else:
            small[key] = n
    nested = _fields([count_field] + [f"{rel}.{f}" for f in fl.split(",")])
    for group in pack_ffd(small, cap):
        out = _request(s, "POST", f"{GRAPH}/paper/batch", params={"fields": nested},
                       json={"ids": [pids[k] for k in group]}, timeout=TIMEOUT_NESTED,
                       purpose=f"batch_nested {rel} ids={len(group)} rows<={sum(small[k] for k in group)}")
        body = _json_body(out) if out.ok else _MISSING
        if out.ok and not (isinstance(body, list) and len(body) == len(group)):
            out = _malformed(out, f"nested batch of {len(group)} ids answered with "
                                  f"{len(body) if isinstance(body, list) else type(body).__name__} rows")
        if not out.ok:
            for k in group:
                result[k] = _walk(s, WalkState.FAILED, pids[k], rel, expected=small[k], attempts=out.attempts,
                                  out=out, reason=out.detail or str(out.kind))
            continue
        for k, row in zip(group, body):
            pid = pids[k]
            if row is None:
                result[k] = _walk(s, WalkState.NOT_FOUND, pid, rel, expected=small[k], attempts=out.attempts,
                                  reason="no S2 record (batch null)")
                continue
            if not isinstance(row, dict) or rel not in row:
                result[k] = _walk(s, WalkState.FAILED, pid, rel, expected=small[k], attempts=out.attempts,
                                  out=_malformed(out, f"row without {rel}"), reason=f"malformed: row without {rel}")
                continue
            lst, cnt = row[rel], row.get(count_field)
            if lst is None:
                disclaimer = _disclaimer(row)
                result[k] = _walk(s, WalkState.ELIDED, pid, rel, expected=cnt, attempts=out.attempts, out=out,
                                  reason=ledger.redact(disclaimer[:300]) if disclaimer else f"{rel}: null")
                continue
            if not isinstance(lst, list) or isinstance(cnt, bool) or not isinstance(cnt, int):
                result[k] = _walk(s, WalkState.FAILED, pid, rel, expected=small[k], attempts=out.attempts,
                                  out=_malformed(out, f"{rel} or {count_field} of the wrong type"),
                                  reason="malformed: list or count of the wrong type")
                continue
            if len(lst) == cnt:
                result[k] = _walk(s, WalkState.EMPTY if cnt == 0 else WalkState.COMPLETE, pid, rel,
                                  rows=[dict(p) if isinstance(p, dict) else p for p in lst], expected=cnt,
                                  attempts=out.attempts, out=out)
                continue
            walker = citations if rel == "citations" else references
            result[k] = walker(pid, fl, expected=cnt, session=s)     # re-fetch: nested list truncated
    return {k: result[k] for k in order}


# ------------------------------------------------------------------------------ title match
@dataclass(frozen=True)
class Match:
    paper: dict
    similarity: float
    decision: str          # "accept" or "review" (design 4.5)
    rule: str
    attempts: int = 0


@dataclass(frozen=True)
class NotFound:
    """search/match found nothing: "unresolved", not "absent" (P5-C9)."""
    title: str
    attempts: int = 0


@dataclass(frozen=True)
class Failed:
    kind: Kind
    status: int | None
    detail: str
    attempts: int = 0
    retry_after: float | None = None


def _norm(t) -> str:
    t = unicodedata.normalize("NFKC", t or "").casefold()
    return "".join(ch for ch in t if ch.isalnum())


def _tokens(t) -> list:
    return re.findall(r"[^\W_]+", unicodedata.normalize("NFKC", t or "").casefold())


def title_similarity(a, b) -> float:
    """P5's similarity: difflib ratio of the NFKC-casefolded alphanumerics, autojunk off."""
    return difflib.SequenceMatcher(None, _norm(a), _norm(b), autojunk=False).ratio()


def _author_hit(first_author, name) -> bool:
    """P5 author_hit: the matched author's surname (letters only, 3+) occurs in the local first
    author, or the local first author occurs in the matched author's full name."""
    def letters(x):
        return re.sub(r"[^a-z]", "", "".join(ch for ch in unicodedata.normalize("NFKD", x or "")
                                             if not unicodedata.combining(ch)).casefold())
    fa = letters(first_author)
    if not name or not fa:
        return False
    sur = letters(name.split()[-1])
    return (len(sur) >= 3 and sur in fa) or fa in letters(name)


def _head_containment(title, head) -> float:
    a, b = _norm(title), _norm(head)
    if not a or not b:
        return 0.0
    m = difflib.SequenceMatcher(None, a, b, autojunk=False).find_longest_match(0, len(a), 0, len(b))
    return m.size / len(a)


def match_title(title, *, year=None, first_author=None, text_head=None, fields=MATCH_FIELDS,
                session: Session | None = None):
    """GET /paper/search/match with a real title (never a filename slug; K8 filters). Returns
    Match (decision per design 4.5: >= 0.95 accept; 0.80-0.95 accept when |year delta| <= 1 and the
    first author matches; < 0.80 accept when every query token is in the matched title and the PDF
    text head contains it at >= 0.9; else review), NotFound (404, "unresolved"), or Failed. `year`,
    `first_author` and `text_head` feed the rule only; nothing but the title is sent."""
    q = " ".join(str(title or "").split())
    if not q:
        raise ValueError("match_title needs a non-empty title")
    out = _request(session, "GET", f"{GRAPH}/paper/search/match", params={"query": q, "fields": _fields(fields)},
                   purpose="search/match")
    if out.kind is Kind.NO_MATCH:
        return NotFound(q, out.attempts)
    body = _json_body(out) if out.ok else _MISSING
    if out.ok and not (isinstance(body, dict) and isinstance(body.get("data"), list) and body["data"]
                       and isinstance(body["data"][0], dict)):
        out = _malformed(out, "search/match without a data row")
    if not out.ok:
        return Failed(out.kind, out.status, out.detail, out.attempts, out.retry_after)
    paper = body["data"][0]
    got = paper.get("title") or ""
    sim = round(title_similarity(q, got), 4)
    if sim >= 0.95:
        return Match(paper, sim, "accept", "similarity >= 0.95", out.attempts)
    if sim >= 0.80:
        yr = paper.get("year")
        authors = paper.get("authors") if isinstance(paper.get("authors"), list) else []
        name = authors[0].get("name") if authors and isinstance(authors[0], dict) else None
        if year is not None and isinstance(yr, int) and abs(int(year) - yr) <= 1 \
                and first_author and _author_hit(first_author, name):
            return Match(paper, sim, "accept", "0.80-0.95, year within 1 and first author", out.attempts)
        return Match(paper, sim, "review", "0.80-0.95 without year and first-author agreement", out.attempts)
    gt = set(_tokens(got))
    if text_head and all(t in gt for t in _tokens(q)) and _head_containment(got, text_head) >= 0.9:
        return Match(paper, sim, "accept", "< 0.80, query tokens in title and title in text head", out.attempts)
    return Match(paper, sim, "review", "< 0.80", out.attempts)


# ------------------------------------------------------------------------------ recommendations
def _recs(out: Outcome) -> Outcome:
    if not out.ok:
        return out
    body = _json_body(out)
    if not (isinstance(body, dict) and isinstance(body.get("recommendedPapers"), list)):
        return _malformed(out, "no recommendedPapers list")
    return dataclasses.replace(out, payload=body["recommendedPapers"])


def recommend_for_paper(id, pool="recent", limit=20, fields=REC_FIELDS, *,
                        session: Session | None = None) -> Outcome:
    """GET /recommendations/v1/papers/forpaper/{id}: OK with the list (possibly empty), NO_MATCH
    on 404 (the input paper is not in S2: not a network error), else the failure kind. `recent`
    is a last-60-days feed, not topical neighbours (P8-C1); `all-cs` is for CS projects only."""
    if pool not in ("recent", "all-cs"):
        raise ValueError(f"pool must be 'recent' or 'all-cs', got {pool!r}")
    if not 1 <= int(limit) <= 500:
        raise ValueError("limit must be in 1..500")
    pid = paper_id(id)
    out = _request(session, "GET", f"{RECS}/papers/forpaper/{_path_id(pid, '')}",
                   params={"from": pool, "limit": int(limit), "fields": _fields(fields)},
                   purpose=f"recommend_for_paper {pool}")
    return _recs(out)


def recommend(positive, negative=(), limit=100, fields=REC_FIELDS, *, session: Session | None = None) -> Outcome:
    """POST /recommendations/v1/papers with positive and negative seed ids (same result contract
    as recommend_for_paper; the pool is the same 60-day feed, P8-C4)."""
    pos = [paper_id(x) for x in positive]
    neg = [paper_id(x) for x in negative]
    if not pos:
        raise ValueError("recommend needs at least one positive id")
    if not 1 <= int(limit) <= 500:
        raise ValueError("limit must be in 1..500")
    out = _request(session, "POST", f"{RECS}/papers", params={"limit": int(limit), "fields": _fields(fields)},
                   json={"positivePaperIds": pos, "negativePaperIds": neg},
                   purpose=f"recommend pos={len(pos)} neg={len(neg)}")
    return _recs(out)


# ------------------------------------------------------------------------------ CLI
def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m litpipe.s2",
                                 description="Semantic Scholar client settings (no network).")
    ap.add_argument("--status", action="store_true",
                    help="print key presence, spacing, run budget and breaker (the key value is never printed)")
    args = ap.parse_args(argv)
    if not args.status:
        ap.print_help()
        return 0
    try:
        st = settings()
    except config.ConfigError as e:
        print(f"[s2] config error: {ledger.redact(e)}", file=sys.stderr)
        return 2
    pol = host_policy(st)
    print(f"[s2] {key_status()}  base={hosts.host_of(BASE)}  spacing={pol.min_interval_s}s  "
          f"retry_after_cap={pol.retry_after_cap_s:.0f}s  max_requests_per_run={st.max_requests_per_run}  "
          f"breaker={st.breaker}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
