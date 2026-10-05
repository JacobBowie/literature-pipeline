"""litpipe.openalex: the OpenAlex client (dispatch 0.5 contract; W3-B). The only code that talks to
api.openalex.org and content.openalex.org; every request goes through litpipe.net (host policy,
per-attempt ledger, redaction, typed Outcomes) and no function returns an empty result for a
failed call.

Public functions (each takes an optional `session=`):
  works_by_doi(dois, select)          GET /works?filter=doi:a|b|... in chunks of at most 100 values
                                      and at most URL_SAFE_BYTES of URL -> dict[input, Outcome]:
                                      OK (payload: the work), NO_MATCH (not in the default corpus),
                                      SKIPPED (not a DOI; never sent), or the chunk's failure.
  works_by_id(wids, select)           GET /works?filter=openalex:W1|W2|... (same chunking) ->
                                      dict[input, Outcome]; NO_MATCH is a dangling ID.
  referenced_works(doi)               the free singleton /works/doi:{doi}, then the referenced
                                      W-IDs resolved in batches -> Outcome whose payload is a RefList:
                                      a list of DOIs, with .dangling, .no_doi, .works, .count.
  referenced_works_many(dois)         the same for many seeds (seed batch + one shared resolve) ->
                                      dict[input, Outcome]; what the backward walk uses.
  citing_works(doi_or_wid)            filter=cites:W... with cursor paging -> Iterator[Outcome], one
                                      per page (payload: a Page of works, .count, .next_cursor); a
                                      failed page is yielded and the iteration stops.
  content_pdf(wid, allow_paid=False)  the cached PDF from content.openalex.org, $0.01 a call: SKIPPED
                                      unless allow_paid=True AND the project's DEC-31 sources include
                                      "openalex_content" AND the per-run cap (projects.json
                                      openalex.content_max_per_run, default 0) is not reached.

What the answers mean (notes/2026-09-25_endpoint_docs/stage3/V2/FINDINGS.md section 5):
  * referenced_works_count == 0 is UNKNOWN (not yet processed, V2-N8), never "no references":
    referenced_works answers NOT_AVAILABLE with payload None, and the caller falls through to
    Crossref, then the regex. An empty list is never an answer.
  * About 1.9 % of referenced W-IDs point to nothing (V2-N7; a single lookup is a 404 with an HTML
    body). They are reported in RefList.dangling and are not DOIs.
  * A DOI missing from a batch answer is NO_MATCH (not in the core corpus); a batch whose
    meta.count exceeds the page it returned marks its missing DOIs ERROR, never NO_MATCH.

URL limit: the server counts 8,190 bytes after re-encoding '/' (V2 C-P5; the docs say 4,094). A
chunk is closed when the URL litpipe.net will send, plus 2 bytes for every literal '/', would pass
URL_SAFE_BYTES (7,500), or at 100 values (help.openalex.org/api/filtering: "You can combine up to
100 values with | in a single filter").

Key and auth (help.openalex.org/api/authentication, read 2026-10-05: "Add it as an api_key query
parameter ... or send it as a bearer token in the Authorization header ... Both work
identically."): OPENALEX_API_KEY from the environment, sent only as `Authorization: Bearer`, never
in a URL; litpipe.net logs header names only and drops Authorization on a redirect off the host;
ledger.redact strips the literal key value anyway. key_status() says "key: present" or "absent".
Without a key the calls still work (the keyless daily budget); the backward walk skips its
OpenAlex leg instead (reverse_citations).

Budget (same page: "Two things return 429 Too Many Requests: exceeding your daily budget, or
making more than 100 requests per second"; headers X-RateLimit-Limit, -Remaining, -Credits-Used,
-Reset "Seconds until reset (midnight UTC)"): every answer's X-RateLimit-* headers are read when
present (a 400 URL-too-long carries none; tolerated). X-RateLimit-Remaining 0, or a 429, defers
the host in litpipe.state until X-RateLimit-Reset (else Retry-After, else the next UTC midnight)
and stops the session: later calls return DEFERRED without sending. A 429 is never retried
(help.openalex.org/api/errors: the daily budget does not clear with a backoff); 5xx is retried
1 s, 2 s, 4 s (same page: "wait 1s, 2s, 4s"). A 401 (a rejected key) is CONFIG.

Run accounting (Session): attempts count against openalex.max_requests_per_run (default 2000;
null for no cap), checked before each send; `breaker` (default 3) consecutive failed calls stop
the session. `session.aborted` is "budget", "breaker", "config" or None; a walker exits 3 (1 for
config) when it is set.

projects.json "openalex" block (all optional):
  max_requests_per_run   attempts per Session, default 2000; null for no cap
  breaker                consecutive failed calls before the session stops, default 3
  content_max_per_run    content_pdf downloads per Session, default 0 (none)

Tests stub litpipe.net's transports (tests/test_w3b_openalex.py); content_pdf is never called by
a test or a probe against the live host.
"""
from __future__ import annotations

import dataclasses
import os
import re
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Iterator

from litpipe import config, hosts, ledger, net
from litpipe import doi as _doi
from litpipe.hosts import RetryPolicy
from litpipe.outcomes import Kind, Outcome

BASE = "https://api.openalex.org"
CONTENT_BASE = "https://content.openalex.org"
API_HOST = "api.openalex.org"
CONTENT_HOST = "content.openalex.org"
KEY_ENV = "OPENALEX_API_KEY"

OR_MAX = 100                     # values per OR filter (docs)
URL_LIMIT_BYTES = 8190           # the server's count (V2 C-P5: 8,200 wire -> 400 "over the 8190-byte limit")
URL_SAFE_BYTES = 7500            # our ceiling on the estimate (V2 section 5: "Keep the wire URL <= 7,500 bytes")
PER_PAGE = 100                   # docs: "100 is the supported maximum"
TIMEOUT = (10, 30)               # docs: "Use 30-second timeouts"
TIMEOUT_PDF = (10, 60)

WORK_SELECT = ("id", "doi", "display_name", "publication_year")
SEED_SELECT = ("id", "doi", "referenced_works", "referenced_works_count")

DEFAULT_RUN_BUDGET = 2000
DEFAULT_BREAKER = 3
DEFAULT_CONTENT_CAP = 0

# 5xx only: a 429 is the daily budget (or a burst over 100 rps) and is final at once (DEFERRED).
OA_RETRY = RetryPolicy(max_retries=3, first_wait_s=1.0, factor=2.0, jitter_s=0.5, max_wait_s=30.0,
                       statuses=frozenset({500, 502, 503, 504}), transport_retries=2)
_STATUS_KINDS = {429: Kind.DEFERRED, 401: Kind.CONFIG}
BURST_DEFER_S = 60.0             # a 429 while budget remains (the 100 rps rule) without Retry-After

_WID = re.compile(r"(?i)^(?:https?://(?:api\.)?openalex\.org/(?:works/)?)?(w\d+)$")
_FAILED_KINDS = frozenset(k for k in Kind if k not in (Kind.OK, Kind.NO_MATCH, Kind.NOT_AVAILABLE, Kind.SKIPPED))


# ------------------------------------------------------------------------------ key
def _key():
    """The key, for the Authorization header only. Never returned by any public function."""
    return (os.environ.get(KEY_ENV) or "").strip() or None


def key_present() -> bool:
    return _key() is not None


def key_status() -> str:
    return "key: present" if key_present() else "key: absent"


def _auth_headers():
    k = _key()
    return {"Authorization": f"Bearer {k}"} if k else None


# ------------------------------------------------------------------------------ settings and host rows
@dataclasses.dataclass(frozen=True)
class Settings:
    max_requests_per_run: int | None = DEFAULT_RUN_BUDGET
    breaker: int = DEFAULT_BREAKER
    content_max_per_run: int = DEFAULT_CONTENT_CAP


def _int_setting(block, name, default, *, minimum, nullable=False):
    v = block.get(name, default)
    if v is None and nullable:
        return None
    if isinstance(v, bool) or not isinstance(v, int) or v < minimum:
        null = " or null" if nullable else ""
        raise config.ConfigError(f"openalex.{name} must be an integer >= {minimum}{null}, got {v!r}")
    return v


def settings(cfg=None) -> Settings:
    """The projects.json `openalex` block, validated (a wrong value is CONFIG: the run aborts)."""
    b = config.openalex(cfg)
    return Settings(max_requests_per_run=_int_setting(b, "max_requests_per_run", DEFAULT_RUN_BUDGET,
                                                      minimum=1, nullable=True),
                    breaker=_int_setting(b, "breaker", DEFAULT_BREAKER, minimum=1),
                    content_max_per_run=_int_setting(b, "content_max_per_run", DEFAULT_CONTENT_CAP, minimum=0))


def host_policy() -> hosts.HostPolicy:
    """The api.openalex.org row as this client runs it (for the host BASE points at): the table
    row with 5xx-only retries, a 429 that defers instead of refusing, and a 401 that is CONFIG."""
    return dataclasses.replace(hosts.policy(API_HOST), host=hosts.host_of(BASE), retry=OA_RETRY,
                               refuse_host_statuses=frozenset({406}), status_kinds=dict(_STATUS_KINDS),
                               note="1,000 credits/day keyless, $1/day keyed; URL <= 8,190 B as counted; "
                                    "budget from X-RateLimit-*; 429 defers to X-RateLimit-Reset "
                                    "(litpipe.openalex registers)")


def content_policy() -> hosts.HostPolicy:
    """The content.openalex.org row: $0.01 per call, no redirect off the host is followed (the
    docs do not say where a download redirects), a 429 defers, a 401 is CONFIG."""
    return hosts.HostPolicy(hosts.host_of(CONTENT_BASE), min_interval_s=1.0, retry=OA_RETRY,
                            refuse_host_statuses=frozenset({406}), status_kinds=dict(_STATUS_KINDS),
                            note="cached OpenAlex PDFs, $0.01 per call; content_pdf(allow_paid=True), "
                                 "DEC-31 source openalex_content and a per-run cap only")


def apply_host_policy() -> tuple:
    """Register both rows when the table differs (as s2.apply_host_policy does; per call, not at
    import, so importing this module never changes the process-wide host table)."""
    out = []
    for want in (host_policy(), content_policy()):
        if hosts.policy(want.host) != want:
            hosts.register(want)
        out.append(want)
    return tuple(out)


# ------------------------------------------------------------------------------ rate-limit headers
def _num(v, cast):
    try:
        return cast(str(v).strip())
    except (TypeError, ValueError):
        return None


def rate_limit(headers) -> dict:
    """The X-RateLimit-* values of one answer ({} keys absent when the header is; tolerated)."""
    if headers is None:
        return {}
    get = headers.get
    out = {"limit": _num(get("X-RateLimit-Limit"), int), "remaining": _num(get("X-RateLimit-Remaining"), int),
           "credits_used": _num(get("X-RateLimit-Credits-Used"), int),
           "reset_s": _num(get("X-RateLimit-Reset"), float), "cost_usd": _num(get("X-RateLimit-Cost-USD"), float),
           "limit_usd": _num(get("X-RateLimit-Limit-USD"), float),
           "remaining_usd": _num(get("X-RateLimit-Remaining-USD"), float)}
    return {k: v for k, v in out.items() if v is not None}


def _seconds_to_utc_midnight(now) -> float:
    t = datetime.fromtimestamp(now, tz=timezone.utc)
    nxt = (t + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return max(1.0, (nxt - t).total_seconds())


# ------------------------------------------------------------------------------ run accounting
class RunBudgetSpent(Exception):
    """The session's attempt budget is spent; litpipe.net turns it into DEFERRED, nothing sent."""

    def __init__(self, host, budget):
        self.host, self.budget, self.retry_after = host, budget, None
        super().__init__(f"openalex run budget of {budget} attempts spent")


class _RunState:
    """The shared state as one Session sees it (s2._RunState's pattern): acquire() counts the
    attempt against the run budget before claiming the pacing slot, retries included."""

    def __init__(self, inner, session):
        self._inner = inner
        self._session = session
        inner_exc = getattr(inner, "BudgetExhausted", None)
        self.BudgetExhausted = (inner_exc, RunBudgetSpent) if inner_exc else RunBudgetSpent

    def acquire(self, host, *a, **k):
        s = self._session
        if s.budget is not None and s.attempts >= s.budget:
            s.run_budget_spent = True
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


_MISSING = object()


class Session:
    """One run's view of OpenAlex: run budget, rate-limit state, breaker, counters. Module
    functions use a process-wide default session unless given one; a walker makes its own."""

    def __init__(self, *, budget=_MISSING, breaker=None, content_cap=None, cfg=None, state=None):
        self.cfg = cfg
        self.settings = settings(cfg)
        self.budget = self.settings.max_requests_per_run if budget is _MISSING else budget
        self.breaker = breaker if breaker is not None else self.settings.breaker
        self.content_cap = self.settings.content_max_per_run if content_cap is None else int(content_cap)
        self.state = state
        self.calls = 0
        self.attempts = 0
        self.attempts_not_ok = 0
        self.bytes = 0
        self.credits = 0
        self.cost_usd = 0.0
        self.remaining = None            # the last X-RateLimit-Remaining seen
        self.limit = None
        self.deferred_until = None       # epoch, when the budget is spent
        self.content_calls = 0
        self.consecutive_failed = 0
        self.tripped = False
        self.run_budget_spent = False
        self.budget_spent = False        # X-RateLimit-Remaining 0, a 429, or a host deferred in state
        self.config_error = ""
        self.kinds: Counter = Counter()

    @property
    def aborted(self) -> str | None:
        """"config" (a rejected key), "budget" (the daily budget is spent, a 429, the host is
        deferred in state, or the run's attempt budget is spent), "breaker", or None."""
        if self.config_error:
            return "config"
        if self.budget_spent or self.run_budget_spent:
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
        if isinstance(out.payload, net.Response):
            self.bytes += out.payload.total_bytes
        if out.kind is Kind.CONFIG:
            self.config_error = out.detail or "CONFIG"
        if out.kind in _FAILED_KINDS:
            self.consecutive_failed += 1
            if self.consecutive_failed >= self.breaker:
                self.tripped = True
        else:
            self.consecutive_failed = 0

    def summary(self) -> dict:
        return {"calls": self.calls, "attempts": self.attempts, "attempts_not_ok": self.attempts_not_ok,
                "budget": self.budget, "credits": self.credits, "cost_usd": round(self.cost_usd, 6),
                "remaining": self.remaining, "limit": self.limit, "bytes": self.bytes,
                "content_calls": self.content_calls, "kinds": dict(self.kinds), "aborted": self.aborted,
                "key": key_status()}

    def summary_line(self) -> str:
        return (f"[openalex] {key_status()} calls={self.calls} attempts={self.attempts} "
                f"not_ok_attempts={self.attempts_not_ok} credits={self.credits} remaining={self.remaining}"
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


# ------------------------------------------------------------------------------ requests
def _note_budget(s: Session, out: Outcome, host: str):
    """Read the answer's X-RateLimit-* headers; a spent budget or a 429 defers the host and stops
    the session. A DEFERRED that sent nothing means the host is already deferred in state."""
    if out.kind is Kind.DEFERRED and not out.attempts:
        s.budget_spent = True
        return
    p = out.payload
    rl = rate_limit(p.headers) if isinstance(p, net.Response) else {}
    if out.attempts and out.kind is not Kind.DEFERRED:
        s.credits += rl.get("credits_used", 0)
        s.cost_usd += rl.get("cost_usd", 0.0)
    if "remaining" in rl:
        s.remaining = rl["remaining"]
    if "limit" in rl:
        s.limit = rl["limit"]
    spent = rl.get("remaining") == 0
    if out.status != 429 and not spent:
        return
    now = net.CLOCK.time()
    ra = net.retry_after_seconds(p.headers.get("Retry-After")) if isinstance(p, net.Response) else None
    if spent or (out.status == 429 and "remaining" not in rl):
        wait = rl.get("reset_s") or ra or _seconds_to_utc_midnight(now)
        s.budget_spent = True
    else:                                          # a 429 while budget remains: the 100 rps rule
        wait = ra or BURST_DEFER_S
    s.deferred_until = now + wait
    s._inner_state().defer(host, now + wait)


def _error_text(out: Outcome) -> str:
    p = out.payload
    if not isinstance(p, net.Response) or not p.content:
        return ""
    try:
        body = p.json()
    except ValueError:
        return ""
    if isinstance(body, dict):
        for k in ("message", "error"):
            if isinstance(body.get(k), str):
                return ledger.redact(body[k][:200])
    return ""


def _request(session: Session | None, url, *, params=None, purpose="", validate=net.expect_json,
             stream=False, timeout=TIMEOUT, max_bytes=None) -> Outcome:
    s = session or default_session()
    apply_host_policy()
    host = hosts.host_of(url)
    if s.aborted:
        out = Outcome(Kind.DEFERRED, host=host, detail=f"openalex session stopped ({s.aborted}); nothing sent",
                      retry_after=(max(0.0, s.deferred_until - net.CLOCK.time()) if s.deferred_until else None))
        s._record(out, sent=False)
        return out
    out = net.request("GET", url, params=params, headers=_auth_headers(), timeout=timeout, stream=stream,
                      max_bytes=max_bytes, validate=validate, purpose=f"openalex {purpose}".strip(),
                      state=_RunState(s._inner_state(), s), cfg=s.cfg)
    if out.kind is Kind.DEFERRED and out.status == 429:
        out = dataclasses.replace(out, detail=f"{out.detail}: daily budget spent or over 100 requests/s")
    s._record(out, sent=True)
    _note_budget(s, out, host)
    if out.kind is Kind.DEFERRED and s.deferred_until and out.retry_after is None:
        out = dataclasses.replace(out, retry_after=max(0.0, s.deferred_until - net.CLOCK.time()))
    if not out.ok and out.kind is not Kind.NO_MATCH:
        err = _error_text(out)
        if err and err not in out.detail:
            out = dataclasses.replace(out, detail=f"{out.detail}: {err}" if out.detail else err)
    return out


def _json(out: Outcome):
    p = out.payload
    if not isinstance(p, net.Response) or p.content is None:
        return _MISSING
    try:
        return p.json()
    except ValueError:
        return _MISSING


def _malformed(out: Outcome, why: str) -> Outcome:
    return dataclasses.replace(out, kind=Kind.ERROR, detail=f"unexpected OpenAlex response: {why}")


# ------------------------------------------------------------------------------ ids and chunking
def work_id(x) -> str:
    """The short OpenAlex work id ("W2098500937") of "W...", "w...", or an openalex.org URL.
    ValueError when it is none of these."""
    m = _WID.match(str(x or "").strip())
    if not m:
        raise ValueError(f"not an OpenAlex work id: {x!r}")
    return m.group(1).upper()


def url_bytes(url: str) -> int:
    """The URL's size as the server counts it, estimated high: the wire bytes plus 2 for every
    literal '/' (the server re-encodes '/' as %2F before it counts; V2 C-P5)."""
    return len(url.encode("utf-8")) + 2 * url.count("/")


def _list_url(path, params):
    return net._merge_query(BASE.rstrip("/") + path, params)


def chunks(values, field, params, *, path="/works", max_values=OR_MAX, max_bytes=URL_SAFE_BYTES) -> list[list]:
    """Split `values` into OR-filter chunks: at most `max_values` per chunk, and the URL litpipe.net
    will send (with `params` and filter=<field>:a|b|...) at most `max_bytes` by url_bytes(). A value
    that alone overflows raises ValueError (no DOI comes near it: p99 33, max 53 characters)."""
    out, cur = [], []
    for v in values:
        trial = cur + [v]
        ok = len(trial) <= max_values and url_bytes(
            _list_url(path, {**params, "filter": f"{field}:" + "|".join(trial)})) <= max_bytes
        if ok:
            cur = trial
            continue
        if not cur:
            raise ValueError(f"a single {field} value overflows the {max_bytes}-byte URL budget")
        out.append(cur)
        cur = [v]
        if url_bytes(_list_url(path, {**params, "filter": f"{field}:{v}"})) > max_bytes:
            raise ValueError(f"a single {field} value overflows the {max_bytes}-byte URL budget")
    if cur:
        out.append(cur)
    return out


def _select(select, *need) -> str:
    fields = [f for f in (select.split(",") if isinstance(select, str) else select) if f]
    for n in need:
        if n not in fields:
            fields.insert(0, n)
    return ",".join(dict.fromkeys(fields))


def _work_doi(work) -> str | None:
    d = work.get("doi") if isinstance(work, dict) else None
    return _doi.normalise(d) if isinstance(d, str) and d else None


def _list_call(session, field, values, select, purpose, extra=None):
    """One OR-filter list call: (outcome, results list or None)."""
    params = {"select": select, "per_page": PER_PAGE, **(extra or {})}
    params["filter"] = f"{field}:" + "|".join(values)
    out = _request(session, BASE.rstrip("/") + "/works", params=params, purpose=purpose)
    if not out.ok:
        return out, None
    body = _json(out)
    if not (isinstance(body, dict) and isinstance(body.get("results"), list)):
        return _malformed(out, "no results list"), None
    meta = body.get("meta") if isinstance(body.get("meta"), dict) else {}
    count = meta.get("count")
    results = [r for r in body["results"] if isinstance(r, dict)]
    if isinstance(count, int) and count > len(results):
        out = dataclasses.replace(out, detail=f"meta.count {count} > {len(results)} results on the page")
        return out, (results, True)
    return out, (results, False)


def works_by_doi(dois, select=WORK_SELECT, *, corpus=None, session: Session | None = None) -> dict:
    """Works for many DOIs by OR-filter list calls (1 credit each). Keys are the inputs as given.
    `corpus="all"` adds the expansion corpus (DataCite repository DOIs; V2 C-P2)."""
    dois = list(dois)
    sel = _select(select, "id", "doi")
    result: dict = {}
    norm: dict = {}
    for raw in dois:
        d = _doi.normalise(raw) if isinstance(raw, str) else None
        if d is None:
            result[raw] = Outcome(Kind.SKIPPED, host=API_HOST, detail=ledger.redact(f"not a DOI: {raw!r}"[:200]))
        else:
            norm[raw] = d
    unique = list(dict.fromkeys(norm.values()))
    extra = {"corpus": corpus} if corpus else None
    answers: dict = {}
    for chunk in chunks(unique, "doi", {"select": sel, "per_page": PER_PAGE, **(extra or {})}):
        out, got = _list_call(session, "doi", chunk, sel, f"works doi x{len(chunk)}", extra)
        if got is None:
            for d in chunk:
                answers[d] = out
            continue
        results, truncated = got
        found = {}
        for w in results:
            d = _work_doi(w)
            if d is not None and d not in found:
                found[d] = w
        for d in chunk:
            if d in found:
                answers[d] = dataclasses.replace(out, kind=Kind.OK, detail="", payload=found[d])
            elif truncated:
                answers[d] = dataclasses.replace(out, kind=Kind.ERROR, payload=None,
                                                 detail=f"not on the returned page ({out.detail}); not a no-match")
            else:
                answers[d] = dataclasses.replace(out, kind=Kind.NO_MATCH, payload=None,
                                                 detail="not in OpenAlex (default corpus)")
    for raw, d in norm.items():
        result[raw] = answers[d]
    return {raw: result[raw] for raw in dois}


def works_by_id(wids, select=WORK_SELECT, *, session: Session | None = None) -> dict:
    """Works for many OpenAlex ids by OR-filter list calls. Keys are the inputs as given; an id
    that does not come back is NO_MATCH (dangling, V2-N7)."""
    wids = list(wids)
    sel = _select(select, "id")
    result: dict = {}
    short: dict = {}
    for raw in wids:
        try:
            short[raw] = work_id(raw)
        except ValueError as e:
            result[raw] = Outcome(Kind.SKIPPED, host=API_HOST, detail=ledger.redact(str(e))[:200])
    unique = list(dict.fromkeys(short.values()))
    answers: dict = {}
    for chunk in chunks(unique, "openalex", {"select": sel, "per_page": PER_PAGE}):
        out, got = _list_call(session, "openalex", chunk, sel, f"works id x{len(chunk)}")
        if got is None:
            for w in chunk:
                answers[w] = out
            continue
        results, truncated = got
        found = {}
        for r in results:
            try:
                found.setdefault(work_id(r.get("id")), r)
            except ValueError:
                continue
        for w in chunk:
            if w in found:
                answers[w] = dataclasses.replace(out, kind=Kind.OK, detail="", payload=found[w])
            elif truncated:
                answers[w] = dataclasses.replace(out, kind=Kind.ERROR, payload=None,
                                                 detail=f"not on the returned page ({out.detail}); not dangling")
            else:
                answers[w] = dataclasses.replace(out, kind=Kind.NO_MATCH, payload=None,
                                                 detail="dangling: the id resolves to no work")
    for raw, w in short.items():
        result[raw] = answers[w]
    return {raw: result[raw] for raw in wids}


# ------------------------------------------------------------------------------ references
class RefList(list):
    """referenced_works' payload: the DOIs of the resolved referenced works (normalised, unique,
    in OpenAlex order) as a plain list. Attributes: work_id (the seed's W-id), count
    (referenced_works_count), works (every resolved work, with or without a DOI), no_doi (resolved
    works without a DOI; kept for title matching), dangling (W-ids that resolve to nothing)."""

    def __init__(self, dois=(), *, work_id="", count=0, works=(), no_doi=(), dangling=()):
        super().__init__(dois)
        self.work_id = work_id
        self.count = count
        self.works = list(works)
        self.no_doi = list(no_doi)
        self.dangling = list(dangling)


def _seed_refs(out: Outcome, seed: dict):
    """(W-ids, count) of a seed work, or an Outcome when the answer is not a usable list."""
    ids, count = seed.get("referenced_works"), seed.get("referenced_works_count")
    if not isinstance(ids, list) or isinstance(count, bool) or not isinstance(count, int):
        return _malformed(out, "referenced_works or referenced_works_count missing")
    if not ids:
        if count == 0:
            return dataclasses.replace(out, kind=Kind.NOT_AVAILABLE, payload=None,
                                       detail="referenced_works_count 0: unknown (not processed yet), not zero (V2-N8)")
        return _malformed(out, f"referenced_works empty while referenced_works_count is {count}")
    wids = []
    for x in ids:
        try:
            wids.append(work_id(x))
        except ValueError:
            return _malformed(out, f"referenced_works holds {str(x)[:40]!r}")
    return list(dict.fromkeys(wids)), count


def _assemble(out: Outcome, seed: dict, wids: list, count: int, resolved: dict) -> Outcome:
    """A seed's RefList from its resolved W-ids, or the failure of the resolve call that left any of
    them unanswered (a partial list is never OK)."""
    works, dois, no_doi, dangling = [], [], [], []
    for w in wids:
        r = resolved[w]
        if r.kind is Kind.NO_MATCH:
            dangling.append(w)
            continue
        if not r.ok:
            return dataclasses.replace(r, payload=None, detail=f"resolving referenced works: {r.detail or r.kind}")
        works.append(r.payload)
        d = _work_doi(r.payload)
        if d is None:
            no_doi.append(r.payload)
        elif d not in dois:
            dois.append(d)
    try:
        wid = work_id(seed.get("id"))
    except ValueError:
        wid = ""
    refs = RefList(dois, work_id=wid, count=count, works=works, no_doi=no_doi, dangling=dangling)
    return dataclasses.replace(out, kind=Kind.OK, payload=refs,
                               detail=f"{len(dois)} DOIs from {len(wids)} referenced works "
                                      f"({len(no_doi)} without a DOI, {len(dangling)} dangling)")


def referenced_works_many(dois, *, select=WORK_SELECT, session: Session | None = None) -> dict:
    """referenced_works for many seeds: one seed batch per 100 DOIs, then every referenced W-id
    resolved once. Keys are the inputs as given; each value as referenced_works'."""
    s = session or default_session()
    seeds = works_by_doi(dois, SEED_SELECT, session=s)
    plan: dict = {}
    want: list = []
    result: dict = {}
    for raw, out in seeds.items():
        if not out.ok:
            result[raw] = out
            continue
        got = _seed_refs(out, out.payload)
        if isinstance(got, Outcome):
            result[raw] = got
            continue
        plan[raw] = (out, got[0], got[1])
        want.extend(got[0])
    resolved = works_by_id(list(dict.fromkeys(want)), select, session=s) if want else {}
    for raw, (out, wids, count) in plan.items():
        result[raw] = _assemble(out, out.payload, wids, count, resolved)
    return {raw: result[raw] for raw in seeds}


def referenced_works(doi, *, select=WORK_SELECT, session: Session | None = None) -> Outcome:
    """The works one DOI cites: the free singleton /works/doi:{doi}, then its referenced W-ids
    resolved in list calls. OK: payload RefList (DOIs; .dangling, .no_doi, .works, .count).
    NOT_AVAILABLE: referenced_works_count 0 (unknown). NO_MATCH: the DOI is not in OpenAlex.
    Otherwise the failure, payload None."""
    s = session or default_session()
    d = _doi.normalise(doi) if isinstance(doi, str) else None
    if d is None:
        return Outcome(Kind.SKIPPED, host=API_HOST, detail=ledger.redact(f"not a DOI: {doi!r}"[:200]))
    out = _request(s, f"{BASE.rstrip('/')}/works/doi:{_doi.encode_path(d)}",
                   params={"select": ",".join(SEED_SELECT)}, purpose="referenced_works seed")
    if not out.ok:
        if out.kind is Kind.NO_MATCH:
            return dataclasses.replace(out, payload=None, detail="not in OpenAlex")
        return dataclasses.replace(out, payload=None)
    body = _json(out)
    if not isinstance(body, dict):
        return _malformed(out, "the work is not an object")
    got = _seed_refs(out, body)
    if isinstance(got, Outcome):
        return got
    wids, count = got
    resolved = works_by_id(wids, select, session=s)
    return _assemble(out, body, wids, count, resolved)


# ------------------------------------------------------------------------------ citing works
class Page(list):
    """One page of citing works (a plain list) with .count (meta.count), .next_cursor, .page and
    .work_id."""

    def __init__(self, works=(), *, count=None, next_cursor=None, page=1, work_id=""):
        super().__init__(works)
        self.count = count
        self.next_cursor = next_cursor
        self.page = page
        self.work_id = work_id


def _resolve_wid(s, doi_or_wid):
    try:
        return work_id(doi_or_wid), None
    except ValueError:
        pass
    d = _doi.normalise(doi_or_wid) if isinstance(doi_or_wid, str) else None
    if d is None:
        return None, Outcome(Kind.SKIPPED, host=API_HOST,
                             detail=ledger.redact(f"not a DOI or an OpenAlex id: {doi_or_wid!r}"[:200]))
    out = _request(s, f"{BASE.rstrip('/')}/works/doi:{_doi.encode_path(d)}", params={"select": "id"},
                   purpose="citing_works seed")
    if not out.ok:
        return None, dataclasses.replace(out, payload=None,
                                         detail="not in OpenAlex" if out.kind is Kind.NO_MATCH else out.detail)
    body = _json(out)
    try:
        return work_id(body.get("id") if isinstance(body, dict) else None), None
    except ValueError:
        return None, _malformed(out, "the work has no id")


def citing_works(doi_or_wid, select=WORK_SELECT, *, per_page=PER_PAGE, max_pages=None,
                 session: Session | None = None) -> Iterator[Outcome]:
    """Every work citing a DOI or W-id: filter=cites:W... with cursor=* then next_cursor until it is
    null or the page is empty (no 10,000 cap). One Outcome per page (payload a Page); a failure
    is yielded and ends the iteration. A DOI costs one free singleton lookup first. meta.count
    can differ from cited_by_count (V2: 22,447 against 22,988)."""
    s = session or default_session()
    if not 1 <= int(per_page) <= PER_PAGE:
        raise ValueError(f"per_page must be in 1..{PER_PAGE}")
    wid, fail = _resolve_wid(s, doi_or_wid)
    if fail is not None:
        yield fail
        return
    sel = _select(select, "id")
    cursor, n = "*", 0
    while cursor and (max_pages is None or n < max_pages):
        n += 1
        out = _request(s, f"{BASE.rstrip('/')}/works",
                       params={"filter": f"cites:{wid}", "select": sel, "per_page": int(per_page), "cursor": cursor},
                       purpose=f"citing_works page {n}")
        if not out.ok:
            yield dataclasses.replace(out, payload=None)
            return
        body = _json(out)
        if not (isinstance(body, dict) and isinstance(body.get("results"), list)):
            yield _malformed(out, "no results list")
            return
        meta = body.get("meta") if isinstance(body.get("meta"), dict) else {}
        nxt = meta.get("next_cursor")
        results = [r for r in body["results"] if isinstance(r, dict)]
        page = Page(results, count=meta.get("count"), next_cursor=nxt if isinstance(nxt, str) and nxt else None,
                    page=n, work_id=wid)
        yield dataclasses.replace(out, payload=page, detail=f"page {n}: {len(results)} works of {page.count}")
        if not results or page.next_cursor is None:
            return
        cursor = page.next_cursor


# ------------------------------------------------------------------------------ cached PDFs (paid)
def content_pdf(wid, *, allow_paid=False, project=None, cfg=None, session: Session | None = None) -> Outcome:
    """The cached PDF of a work from content.openalex.org, at $0.01 per call. SKIPPED (nothing
    sent) unless allow_paid=True, the project's DEC-31 sources include "openalex_content", a key is
    set, and the session's per-run cap (openalex.content_max_per_run, default 0) is not reached.
    A call counts against the cap before it is sent, so a failed download still spends one."""
    host = hosts.host_of(CONTENT_BASE)
    if not allow_paid:
        return Outcome(Kind.SKIPPED, host=host, detail="content_pdf costs $0.01 per call; pass allow_paid=True")
    if not project:
        return Outcome(Kind.SKIPPED, host=host, detail="content_pdf needs the project whose sources allow it")
    try:
        srcs = config.sources(project, cfg=cfg)
    except config.ConfigError as e:
        return Outcome(Kind.CONFIG, host=host, detail=ledger.redact(str(e))[:300])
    if "openalex_content" not in srcs:
        return Outcome(Kind.SKIPPED, host=host,
                       detail=f"project {project!r}: sources do not include openalex_content (DEC-31)")
    if not key_present():
        return Outcome(Kind.SKIPPED, host=host, detail=f"{KEY_ENV} is not set (the content API needs a key)")
    try:
        w = work_id(wid)
    except ValueError as e:
        return Outcome(Kind.SKIPPED, host=host, detail=ledger.redact(str(e))[:200])
    s = session or default_session()               # the cap is per run: pass the run's session
    if s.content_calls >= s.content_cap:
        return Outcome(Kind.SKIPPED, host=host,
                       detail=f"per-run cap reached: {s.content_calls} of openalex.content_max_per_run={s.content_cap}")
    s.content_calls += 1
    return _request(s, f"{CONTENT_BASE.rstrip('/')}/works/{w}.pdf", purpose="content_pdf (paid)",
                    validate=net.expect_pdf, stream=True, timeout=TIMEOUT_PDF)


# ------------------------------------------------------------------------------ CLI
def main(argv=None) -> int:
    import argparse
    ap = argparse.ArgumentParser(prog="python -m litpipe.openalex",
                                 description="OpenAlex client settings (no network).")
    ap.add_argument("--status", action="store_true",
                    help="print key presence, the run budget, breaker and content cap (the key value is never printed)")
    args = ap.parse_args(argv)
    if not args.status:
        ap.print_help()
        return 0
    try:
        st = settings()
    except config.ConfigError as e:
        print(f"[openalex] config error: {ledger.redact(e)}", file=sys.stderr)
        return 1
    pol = host_policy()
    print(f"[openalex] {key_status()}  base={pol.host}  spacing={pol.min_interval_s}s  "
          f"max_requests_per_run={st.max_requests_per_run}  breaker={st.breaker}  "
          f"content_max_per_run={st.content_max_per_run}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
