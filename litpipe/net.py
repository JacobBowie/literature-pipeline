"""litpipe.net: the one network client (plan 2.1 R1; dispatch W1-A1). Every HTTP call of the pipeline
goes through request(), which returns a typed Outcome and never an empty result for a failed call.

Order of one call (dispatch W1-A1 step 3):
  1. prohibited check: a never-automated route raises ProhibitedHost before anything is sent;
  2. refused short-circuit: a host refused for this run (or manually) returns REFUSED, nothing sent;
  3. state.acquire(host) per attempt (pacing, concurrency, daily budget); BudgetExhausted, which the
     state also raises for a deferred host, becomes DEFERRED;
  4. identity: one User-Agent `literature-pipeline/<version> (mailto:<LITPIPE_EMAIL>)` (no mailto at
     all when the variable is unset or empty), NCBI `tool=literature-pipeline` and `email=`,
     Unpaywall `email=`, per the host's identity mode (litpipe.hosts);
  5. transport per host: requests, or stdlib urllib (pmc.ncbi.nlm.nih.gov 403s requests). Neither
     follows redirects on its own: this module follows each hop itself, only to hosts on the
     issuing host's allow-list, never to a prohibited or refused host, never https -> http; every
     hop is paced, counted as an attempt and ledgered;
  6. retry on the policy's statuses (default 429/500/502/503/504) and on transport failures: the
     first wait is at least 2 s, then doubling plus jitter, at most 6 retries (transport failures
     at most 2), no wait after the last attempt. A Retry-After (delay-seconds or HTTP-date, RFC 9110
     §10.2.3) up to the host's inline cap replaces the wait when longer (Retry-After: 0 never
     bursts); above the cap the host is deferred and the call returns DEFERRED with retry_after;
  7. a 403 is REFUSED for the row and counts toward the host's consecutive-403 threshold (default 1:
     the first 403 refuses the host for the run, DEC-06); a final 406 or 429 refuses the host; on
     pmc.ncbi and arXiv a 429 is never retried, so the first one refuses the host;
  8. validate(payload) on a 2xx: None/True passes; False or a reason string is OUTAGE (an empty 200,
     HTML instead of JSON); a Kind, or (Kind, detail), sets that kind (expect_pdf returns REFUSED for
     an HTML wall);
  9. one ledger line per attempt (litpipe.ledger, redacted); state.release after each attempt.

Status mapping of a final response: 2xx OK (202 REFUSED: a queued/challenge answer, as legacy
HTTP_202); 403/406/429 REFUSED; 404/410 NO_MATCH; 5xx OUTAGE; other 4xx and 3xx ERROR; a host's
`status_kinds` override (Unpaywall 422/410 -> CONFIG). A response over max_bytes is ERROR with
detail "too_large" and no content (the first chunk is kept for diagnosis).

The state object is injectable: request(..., state=obj), else the module attribute STATE, else
litpipe.state (W1-A2; imported lazily so importing this module never needs it). CLOCK (monotonic,
time, sleep) and RANDOM are module attributes tests replace; tests/netmock.py has the fakes.
"""
from __future__ import annotations

import http.client
import json as _json
import os
import random
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import timezone
from email.utils import parsedate_to_datetime
from urllib.parse import unquote_plus, urlencode, urljoin, urlsplit, urlunsplit

import requests
from requests.structures import CaseInsensitiveDict

from litpipe import __version__, config, hosts, ledger
from litpipe.hosts import ProhibitedHost  # noqa: F401  (re-exported: net.ProhibitedHost)
from litpipe.outcomes import Kind, Outcome

TOOL = "literature-pipeline"
MAX_REDIRECTS = 5
CHUNK = 64 * 1024
ERROR_BODY_CAP = 256 * 1024          # bytes kept from a non-2xx body (diagnosis only)
DEFAULT_STREAM_CAP = 80_000_000      # a stream without max_bytes (lit_net.MAX_PDF_BYTES, c7)
REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
_RESP_META = ("content-type", "content-length", "retry-after", "location")


class SystemClock:
    @staticmethod
    def monotonic():
        return time.monotonic()

    @staticmethod
    def time():
        return time.time()

    @staticmethod
    def sleep(s):
        time.sleep(s)


CLOCK = SystemClock()
RANDOM = random.random
STATE = None


class _NeverRaised(Exception):
    pass


def _state(state):
    if state is not None:
        return state
    if STATE is not None:
        return STATE
    import litpipe.state as st   # W1-A2
    return st


# ------------------------------------------------------------------------------ payload
@dataclass(frozen=True)
class Response:
    """Outcome.payload of every call that got an HTTP response (also on failure, for diagnosis).
    `url` is the final URL, redacted (the identity email= and any key never ride along);
    `headers` are the server's, unredacted: pass anything persisted through ledger.redact."""
    status: int
    headers: CaseInsensitiveDict
    content: bytes | None
    url: str
    first_chunk: bytes = b""
    total_bytes: int = 0
    truncated: bool = False

    @property
    def content_type(self) -> str:
        return (self.headers.get("Content-Type") or "").split(";")[0].strip().lower()

    @property
    def text(self) -> str:
        return (self.content or b"").decode("utf-8", "replace")

    def json(self):
        return _json.loads(self.text)


def expect_json(p: Response):
    """Validator: a non-empty JSON body (not an HTML page)."""
    body = (p.content or b"").strip()
    if not body:
        return "empty body"
    if body[:1] == b"<" or "html" in p.content_type:
        return "HTML instead of JSON"
    try:
        _json.loads(body)
    except ValueError:
        return "body is not JSON"
    return None


def expect_pdf(p: Response):
    """Validator: `%PDF` within the first 1024 bytes. HTML is a wall (REFUSED), as legacy HTML/NOT_PDF."""
    head = (p.first_chunk or b"")[:1024]
    if not head.strip():
        return (Kind.OUTAGE, "empty body")
    if b"%PDF" in head:
        return None
    low = head.lower()
    if b"<html" in low or b"<!doctype" in low:
        return (Kind.REFUSED, "HTML instead of PDF")
    return (Kind.REFUSED, "not a PDF")


# ------------------------------------------------------------------------------ identity
def contact_email():
    """LITPIPE_EMAIL, or None when unset or blank (then no mailto and no email= is sent)."""
    e = (os.environ.get("LITPIPE_EMAIL") or "").strip()
    return e or None


def user_agent(with_mailto=True) -> str:
    base = f"{TOOL}/{__version__}"
    e = contact_email() if with_mailto else None
    return f"{base} (mailto:{e})" if e else base


def _merge_query(url, params, drop=()):
    """Append `params` (None values skipped) to url's query without re-encoding what is there;
    `drop` removes existing keys first."""
    if not params and not drop:
        return url
    parts = urlsplit(url)
    pieces = [p for p in parts.query.split("&") if p]
    if drop:
        pieces = [p for p in pieces if unquote_plus(p.split("=", 1)[0]) not in drop]
    items = [(k, v) for k, v in (params.items() if hasattr(params, "items") else (params or ())) if v is not None]
    if items:
        pieces.append(urlencode(items, doseq=True))
    return urlunsplit(parts._replace(query="&".join(pieces)))


def _identity(pol, url, headers):
    h = CaseInsensitiveDict(headers or {})
    if pol.identity == "download":
        if not h.get("User-Agent"):
            h["User-Agent"] = user_agent(with_mailto=False)
    elif pol.identity == "anonymous":
        h["User-Agent"] = user_agent(with_mailto=False)
    else:
        h["User-Agent"] = user_agent()
    email = contact_email()
    if pol.identity == "ncbi":
        url = _merge_query(url, {"tool": TOOL, "email": email}, drop=("tool", "email"))
    elif pol.identity == "email_param":
        url = _merge_query(url, {"email": email}, drop=("email",))
    return url, h


def _hop_headers(caller_headers, pol, to_origin_host, content_type=None):
    """Headers for a redirect hop, rebuilt from the caller's own: credentials (Authorization,
    x-api-key, cookies) never leave the host the caller addressed; identity is the target's, so an
    API host's mailto User-Agent is not carried onto a download host."""
    out = CaseInsensitiveDict(caller_headers or {})
    if not to_origin_host:
        for k in list(out):
            if k.lower() in ledger.SENSITIVE_HEADERS:
                del out[k]
    _, out = _identity(pol, "http://x/", out)
    if content_type:
        out["Content-Type"] = content_type
    return out


# ------------------------------------------------------------------------------ Retry-After
def retry_after_seconds(value, now=None) -> float | None:
    """RFC 9110 §10.2.3: delay-seconds or an HTTP-date (IMF-fixdate, RFC 850, asctime; a zoneless
    date is read as UTC). Floored at 0; None when absent or unparseable."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    if s.isdigit():
        return float(int(s))
    try:
        if float(s) < 0:          # non-conformant negative delay: floor, never sleep(<0)
            return 0.0
    except ValueError:
        pass
    try:
        d = parsedate_to_datetime(s)
    except (TypeError, ValueError, IndexError):
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    now = CLOCK.time() if now is None else now
    return max(0.0, d.timestamp() - now)


# ------------------------------------------------------------------------------ transports
@dataclass
class _Raw:
    status: int = 0
    headers: CaseInsensitiveDict = field(default_factory=CaseInsensitiveDict)
    content: bytes = b""
    first_chunk: bytes = b""
    total: int = 0
    truncated: bool = False
    error: str = ""


def _drain(chunks, cap):
    first, parts, total, trunc = b"", [], 0, False
    for c in chunks:
        if not c:
            continue
        if not first:
            first = c
        parts.append(c)
        total += len(c)
        if cap is not None and total > cap:
            trunc = True
            break
    return first, b"".join(parts), total, trunc


def _cap(status, max_bytes):
    if 200 <= status < 300:
        return max_bytes
    return ERROR_BODY_CAP if max_bytes is None else min(max_bytes, ERROR_BODY_CAP)


def _err(e) -> str:
    return f"{type(e).__name__}: {e}"


def _send_requests(method, url, headers, body, timeout, max_bytes) -> _Raw:
    try:
        r = requests.request(method, url, headers=dict(headers), data=body, timeout=timeout,
                             stream=True, allow_redirects=False)
    except requests.exceptions.RequestException as e:
        return _Raw(error=_err(e))
    hdrs = CaseInsensitiveDict(r.headers)
    try:
        with r:
            first, content, total, trunc = _drain(r.iter_content(CHUNK), _cap(r.status_code, max_bytes))
    except (requests.exceptions.RequestException, OSError, http.client.HTTPException) as e:
        return _Raw(headers=hdrs, error=f"mid-body after HTTP {r.status_code}: {_err(e)}")
    return _Raw(r.status_code, hdrs, content, first, total, trunc)


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Returning None from redirect_request makes urllib raise HTTPError for the 30x (docs:
    "return None if you can't but another handler might"; HTTPDefaultErrorHandler then raises),
    so the hop comes back to request() instead of being followed silently."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


_OPENER = None


def _opener():
    global _OPENER
    if _OPENER is None:
        _OPENER = urllib.request.build_opener(_NoRedirect)
    return _OPENER


def _send_urllib(method, url, headers, body, timeout, max_bytes) -> _Raw:
    # urllib takes one socket timeout for every blocking operation (docs: "for blocking operations
    # like the connection attempt"): use the read value (the default 30 s is legacy _idconv_get's).
    t = timeout[1] if isinstance(timeout, (tuple, list)) else timeout
    req = urllib.request.Request(url, data=body, headers=dict(headers), method=method)
    try:
        fh = _opener().open(req, timeout=t)
    except urllib.error.HTTPError as e:   # 4xx/5xx and the unfollowed 30x: a response, not a failure
        fh = e
    except (OSError, http.client.HTTPException) as e:   # URLError (DNS, refused), TimeoutError, resets
        return _Raw(error=_err(e))
    status = int(getattr(fh, "status", None) or getattr(fh, "code", 0) or 0)
    hdrs = CaseInsensitiveDict()
    for k, v in (fh.headers.items() if fh.headers is not None else ()):
        hdrs[k] = f"{hdrs[k]}, {v}" if k in hdrs else v
    try:
        first, content, total, trunc = _drain(iter(lambda: fh.read(CHUNK), b""), _cap(status, max_bytes))
    except (OSError, http.client.HTTPException, ValueError) as e:
        if status >= 300:           # an HTTPError without a readable body: keep the status
            return _Raw(status, hdrs)
        return _Raw(headers=hdrs, error=f"mid-body after HTTP {status}: {_err(e)}")
    finally:
        try:
            fh.close()
        except Exception:
            pass
    return _Raw(status, hdrs, content, first, total, trunc)


_TRANSPORTS = {"requests": _send_requests, "urllib": _send_urllib}


# ------------------------------------------------------------------------------ the call
class _Call:
    def __init__(self, method, purpose, cfg, stream):
        self.method = method
        self.purpose = purpose
        self.cfg = cfg
        self.stream = stream
        self.t0 = CLOCK.monotonic()
        self.attempts = 0
        self.hops = 0

    def log(self, pol, url, decision, *, kind=None, raw=None, ts=None, elapsed_ms=None, wait_s=0.0,
            req_headers=None, note="", error=None):
        meta = {}
        if raw is not None:
            for k, v in raw.headers.items():
                kl = k.lower()
                if kl in _RESP_META or kl.startswith(("x-ratelimit", "ratelimit", "x-rate-limit")):
                    meta[kl] = v
        ra_raw = raw.headers.get("Retry-After") if raw is not None else None
        notes = [n for n in (note, "" if pol.known else pol.note) if n]
        ledger.write({
            "ts": ts or ledger.now_iso(),
            "run_id": ledger.current_run_id(),
            "host": pol.host,
            "method": self.method,
            "url": url,
            "purpose": self.purpose,
            "stream": self.stream,
            "attempt": self.attempts,
            "hop": self.hops,
            "transport": pol.transport,
            "status": (raw.status or None) if raw is not None else None,
            "decision": decision,
            "kind": str(kind) if kind is not None else None,
            "elapsed_ms": elapsed_ms,
            "bytes": raw.total if raw is not None else None,
            "retry_after": ra_raw,
            "retry_after_s": retry_after_seconds(ra_raw),
            "wait_s": round(wait_s, 3),
            "req_headers": sorted(req_headers or ()),
            "resp_headers": sorted(raw.headers.keys()) if raw is not None else [],
            "resp_meta": meta,
            "error": error if error is not None else (raw.error or None if raw is not None else None),
            "note": "; ".join(notes) or None,
        }, cfg=self.cfg)

    def outcome(self, kind, host, *, status=None, detail="", retry_after=None, payload=None):
        return Outcome(kind, status=status, host=host, detail=ledger.redact(detail) or "",
                       attempts=self.attempts, elapsed_ms=int((CLOCK.monotonic() - self.t0) * 1000),
                       retry_after=retry_after, payload=payload)


def _count_403(st, host, reset=False):
    """Consecutive 403s from `host` in this run, kept in the state kv so every process sees it."""
    key = f"{ledger.current_run_id() or '-'}:{host}"
    cur = int(st.kv_get("net.consecutive_403", key) or 0)
    if reset:
        if cur:
            st.kv_set("net.consecutive_403", key, 0, ttl_s=86400)
        return 0
    st.kv_set("net.consecutive_403", key, cur + 1, ttl_s=86400)
    return cur + 1


def _validated(validate, payload):
    if validate is None:
        return Kind.OK, ""
    try:
        r = validate(payload)
    except Exception as e:  # a broken validator is our bug, not the host's
        return Kind.ERROR, f"validator raised {_err(e)}"
    if r is None or r is True:
        return Kind.OK, ""
    if r is False:
        return Kind.OUTAGE, "validator rejected the body"
    if isinstance(r, Kind):
        return r, f"validator: {r}"
    if isinstance(r, tuple) and r and isinstance(r[0], Kind):
        return r[0], str(r[1]) if len(r) > 1 else f"validator: {r[0]}"
    return Kind.OUTAGE, str(r)


def _check_url(url):
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise ValueError(f"litpipe.net: not an http(s) URL: {ledger.redact(url)!r}")


def request(method, url, *, params=None, json=None, headers=None, timeout=(10, 30), stream=False,
            max_bytes=None, validate=None, purpose="", state=None, cfg=None) -> Outcome:
    """One logical HTTP call under the host policy. See the module docstring for the order.
    `json` is sent as an application/json body. `stream=True` without max_bytes caps the body at
    DEFAULT_STREAM_CAP. `state` and `cfg` (a loaded projects.json dict) are injectable."""
    method = method.upper()
    cfg = config.load(cfg)
    st = _state(state)
    budget_exc = getattr(st, "BudgetExhausted", _NeverRaised)
    if stream and max_bytes is None:
        max_bytes = DEFAULT_STREAM_CAP
    call = _Call(method, purpose, cfg, stream)

    url = _merge_query(url, params)
    _check_url(url)
    pol = hosts.policy(url)
    rule = hosts.prohibited(url, cfg)
    if rule is not None:
        call.log(pol, url, "prohibited", note=f"{rule.host}{rule.path_prefix}: {rule.reason}")
        raise ProhibitedHost(url, rule)
    if st.is_refused(pol.host):
        call.log(pol, url, "not_sent", kind=Kind.REFUSED, note="host refused")
        return call.outcome(Kind.REFUSED, pol.host, detail=f"host {pol.host} is refused; nothing sent")

    origin_host = pol.host
    url, hdrs = _identity(pol, url, headers)
    body = None
    if json is not None:
        body = _json.dumps(json).encode("utf-8")
        hdrs["Content-Type"] = "application/json"

    retries = transport_fails = 0
    wait_s = 0.0
    while True:
        host = pol.host
        try:
            st.acquire(host)
        except budget_exc as e:
            ra = getattr(e, "retry_after", None)
            call.log(pol, url, "not_sent", kind=Kind.DEFERRED, note=f"deferred by state: {e}")
            return call.outcome(Kind.DEFERRED, host, detail=f"{host} deferred: {e}", retry_after=ra)
        call.attempts += 1
        ok = False
        try:
            ts = ledger.now_iso()
            t_send = time.perf_counter()
            raw = _TRANSPORTS[pol.transport](method, url, hdrs, body, timeout, max_bytes)
            elapsed_ms = round((time.perf_counter() - t_send) * 1000, 1)
            ok = not raw.error and 0 < raw.status < 400
            act = _decide(raw, pol, url, retries, transport_fails, st, cfg, max_bytes, validate, call.hops)
            call.log(pol, url, act.decision, kind=act.kind, raw=raw, ts=ts, elapsed_ms=elapsed_ms,
                     wait_s=wait_s, req_headers=hdrs.keys(), note=act.note)
        finally:
            st.release(host, ok)

        if act.decision == "retry":
            retries += 1
            transport_fails += 1 if raw.error else 0
            wait_s = act.wait
            CLOCK.sleep(wait_s)
            continue
        if act.decision == "redirect":
            call.hops += 1
            if act.method_to_get and method not in ("GET", "HEAD"):
                method, body = "GET", None
                call.method = method
            ctype = "application/json" if body is not None else None
            hdrs = _hop_headers(headers, act.target_policy, act.target_policy.host == origin_host, ctype)
            url, pol, wait_s = act.target, act.target_policy, 0.0
            continue
        payload = None
        if raw.status:
            payload = Response(raw.status, raw.headers, None if raw.truncated else raw.content, ledger.redact(url),
                               raw.first_chunk, raw.total, raw.truncated)
        return call.outcome(act.kind, host, status=raw.status or None, detail=act.detail,
                            retry_after=act.retry_after, payload=payload)


@dataclass
class _Act:
    decision: str                     # retry | redirect | redirect_blocked | deferred | final
    kind: Kind | None = None
    detail: str = ""
    wait: float = 0.0
    retry_after: float | None = None
    target: str = ""
    target_policy: object = None
    method_to_get: bool = False
    note: str = ""


def _final(kind, detail, **kw):
    return _Act("final", kind=kind, detail=detail, **kw)


def _refuse(st, pol, reason):
    st.refuse(pol.host, reason, persistence=pol.refusal_persistence)
    scope = "until cleared (manual)" if pol.refusal_persistence == "manual" else "for the run"
    return f"{reason}; host {pol.host} refused {scope}"


def _decide(raw, pol, url, retries, transport_fails, st, cfg, max_bytes, validate, hops) -> _Act:
    rp = pol.retry
    status = raw.status
    can_retry = retries < rp.max_retries
    if raw.error:
        if can_retry and transport_fails < rp.transport_retries:
            return _Act("retry", wait=rp.backoff(retries + 1, RANDOM()), note="transport")
        return _final(Kind.TRANSPORT, raw.error)

    if status == 403:
        n = _count_403(st, pol.host)
        if n >= pol.refuse_after_consecutive_403:
            return _final(Kind.REFUSED, _refuse(st, pol, f"HTTP 403 ({n} consecutive)"))
        return _final(Kind.REFUSED, f"HTTP 403 ({n} of {pol.refuse_after_consecutive_403} consecutive)")
    _count_403(st, pol.host, reset=True)

    if status in REDIRECT_STATUSES and raw.headers.get("Location"):
        if hops >= MAX_REDIRECTS:
            return _Act("redirect_blocked", kind=Kind.ERROR, note="too many redirects",
                        detail=f"HTTP {status}: more than {MAX_REDIRECTS} redirects")
        return _redirect(raw, pol, url, st, cfg)

    ra = retry_after_seconds(raw.headers.get("Retry-After"))
    if status in rp.statuses:
        if ra is not None and ra > pol.retry_after_cap_s:
            st.defer(pol.host, CLOCK.time() + ra)
            return _Act("deferred", kind=Kind.DEFERRED, retry_after=ra,
                        detail=f"HTTP {status} Retry-After {ra:.0f} s exceeds the inline cap "
                               f"{pol.retry_after_cap_s:.0f} s; host {pol.host} deferred")
        if can_retry:
            return _Act("retry", wait=max(rp.backoff(retries + 1, RANDOM()), ra or 0.0))
        if status in pol.refuse_host_statuses:
            return _final(Kind.REFUSED, _refuse(st, pol, f"HTTP {status} after {retries} retries"),
                          retry_after=ra)
        kind = Kind.OUTAGE if status >= 500 else Kind.REFUSED
        return _final(kind, f"HTTP {status} after {retries} retries", retry_after=ra)

    if status in pol.refuse_host_statuses:
        return _final(Kind.REFUSED, _refuse(st, pol, f"HTTP {status}"), retry_after=ra)
    if status in pol.status_kinds:
        return _final(Kind(pol.status_kinds[status]), f"HTTP {status}")
    if 200 <= status < 300:
        if status == 202:
            return _final(Kind.REFUSED, "HTTP 202 (accepted, not served)")
        if raw.truncated:
            return _final(Kind.ERROR, f"too_large: body over {max_bytes} bytes; content dropped")
        payload = Response(status, raw.headers, raw.content, ledger.redact(url), raw.first_chunk, raw.total, False)
        kind, detail = _validated(validate, payload)
        return _final(kind, detail)
    if status in (404, 410):
        return _final(Kind.NO_MATCH, f"HTTP {status}")
    if status >= 500:
        return _final(Kind.OUTAGE, f"HTTP {status}")
    return _final(Kind.ERROR, f"HTTP {status}")


def _redirect(raw, pol, url, st, cfg) -> _Act:
    status = raw.status
    target = urljoin(url, raw.headers.get("Location").strip())
    tparts = urlsplit(target)
    thost = (tparts.hostname or "").lower()
    why = None
    if tparts.scheme not in ("http", "https") or not thost:
        why = "not an http(s) target"
    elif pol.https_only and urlsplit(url).scheme == "https" and tparts.scheme != "https":
        why = "https to http downgrade"
    elif hosts.prohibited(target, cfg) is not None:
        why = "target is a prohibited route"
    elif not pol.allows_redirect_to(thost):
        why = f"{thost} is not on {pol.host}'s redirect allow-list"
    elif st.is_refused(thost):
        why = f"{thost} is refused"
    if why:
        return _Act("redirect_blocked", kind=Kind.REFUSED, note=why,
                    detail=f"HTTP {status} redirect not followed: {why}")
    # 303 always becomes GET; 301/302 turn a POST into GET as browsers and requests do; 307/308
    # keep the method and body (RFC 9110 §15.4).
    return _Act("redirect", target=target, target_policy=hosts.policy(target),
                method_to_get=status in (301, 302, 303), note=f"to {thost}")


def get(url, **kw) -> Outcome:
    return request("GET", url, **kw)


def post(url, **kw) -> Outcome:
    return request("POST", url, **kw)
