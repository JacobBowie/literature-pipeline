"""lit_net -- the legacy shared HTTP helpers, and the DOI -> PMCID mapping (W2-A1).

`get` and `stream_download` are the pre-litpipe clients the older stages still call (B1, c7). `get`
is a drop-in for requests.get: it RETURNS the final requests.Response, so callers keep their own
`r.status_code` / `r.json()` handling. It retries 429 and 5xx (500/502/503/504) with exponential
backoff, honoring a Retry-After header (capped). 404 and every other status are terminal. On a
network/transport error it retries too, and if every attempt fails it re-raises the last exception
(the surface requests.get already presents). `stream_download` never raises: it returns a
StreamResult. New code uses litpipe.net instead; the stages move onto it task by task (W2).

Prohibited routes (W2-A1). The PMC article pages (`pmc.ncbi.nlm.nih.gov/articles/`,
`cdn.ncbi.nlm.nih.gov`, `www.ncbi.nlm.nih.gov/pmc/articles/`) are never automated (NCBI bars
scripting its web pages; litpipe.hosts.PROHIBITED). `get` and `stream_download` refuse them before
anything is sent and return a status-0 failure whose `error` starts with PROHIBITED. The host-wide
urllib detour these two used for pmc.ncbi.nlm.nih.gov (2026-09-01) is gone: its only users were
those article-page routes. The other prohibited routes (Europe PMC website pages, bioRxiv, arXiv
PDFs) are still sent by the legacy preprint stage until it moves onto litpipe.net (W2-C), which
raises ProhibitedHost for them; a status-0 failure here would change that stage's routing.

DOI -> PMCID (W2-A1, refactor scope §3.1). `doi_to_pmcid(dois)` returns one typed Outcome per DOI
from a chain of sanctioned services, each through litpipe.net (host policy, pacing, refusals,
ledger, identity):
  1. the PMC ID Converter (idconv), only while pmc.ncbi.nlm.nih.gov is not refused (stdlib urllib
     transport per the host row; it 403s requests/urllib3). Up to 200 IDs per call are documented;
     we send 100. `live:false` is EMBARGOED with its `release-date`; "Identifier not found in PMC"
     is NO_MATCH. A 400 no longer fans out one request per DOI (V1-N2: a malformed DOI gets a
     per-record error, not a batch 400, so the fan-out's trigger does not occur, and it was an
     uncapped >100-request series); the chunk goes to the fallbacks instead.
  2. Europe PMC REST search, `DOI:"a" OR DOI:"b"`, resultType=lite (returns pmcid and
     isOpenAccess). Batched to at most 100 DOIs and 4,000 characters of URL: on 2026-09-30 a
     100-DOI query (4,380 characters) answered 200 and a 200-DOI query (8,633) got nginx 414
     Request-URI Too Large. Europe PMC does not return embargoed or just-released articles (pmcid
     null, inPMC N), so a DOI it cannot map goes on to step 3 rather than reading NO_MATCH.
  3. E-utilities esearch db=pmc (`"<doi>"[doi]` ORed, 20 per call; PMC's search translates the
     tag to [All Fields], so every hit is verified) then esummary db=pmc, whose articleids carry
     the DOI and PMCID; a null or future `pmclivedate` is EMBARGOED (release date the future date,
     or unknown).
A DOI resolves at the first step that answers it. A step that is refused or down records its
failure and the DOI moves on; when no step reaches a definitive answer the DOI's outcome is the
FIRST failure (REFUSED / OUTAGE / TRANSPORT / DEFERRED / ERROR: the root cause, never NO_MATCH).

`doi_to_pmcid_batch` (dispatch 0.6) keeps its legacy dict return, {doi_lower: pmcid}, for
backfill_fulltext and recheck_pmc: it delegates to doi_to_pmcid, keeps live PMCIDs only (an
embargoed PMCID is not fetchable, N-B5), and says on stderr how many DOIs could not be looked up
(they are NOT "no PMCID"). Its `ua`, `email` and `tool` arguments are accepted and ignored:
litpipe.net injects the identity (DEC-13: never `email=None`).
"""
from __future__ import annotations

import datetime as _dt
import json as _json
import sys
import time
from collections import Counter, namedtuple
from dataclasses import dataclass
from urllib.parse import urlencode as _urlencode

import requests

import lit_util  # is_valid_doi gate (lit_util is stdlib-pure -> no import cycle)
from litpipe import hosts as _hosts
from litpipe.outcomes import Kind, Outcome

RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
DEFAULT_RETRIES = 3
DEFAULT_BACKOFF = 1.0    # base seconds for exponential backoff between attempts
MAX_RETRY_AFTER = 30.0   # cap a server Retry-After so an absurd value can't stall a run

# c7: one PDF-size cap across the three PDF fetchers (was 50MB unpaywall / 60MB pmc+preprint).
# Real library measured 2026-07 (n=4364 PDFs): max 50.94MB -> 80MB is ~57% headroom, rejects zero
# real papers, still catches an absurd mis-fetch. fetch_figures keeps its own (smaller) image cap.
MAX_PDF_BYTES = 80_000_000

StreamResult = namedtuple("StreamResult", "status_code first_chunk content total truncated error")

# The hosts whose prohibited rules (litpipe.hosts.PROHIBITED) the legacy client enforces itself.
GUARDED_HOSTS = frozenset({"pmc.ncbi.nlm.nih.gov", "cdn.ncbi.nlm.nih.gov", "www.ncbi.nlm.nih.gov"})


def _retry_after_seconds(resp, fallback):
    """Seconds to wait per a Retry-After header (integer-seconds form; the HTTP-date form is
    ignored and falls back to `fallback`), capped at MAX_RETRY_AFTER."""
    ra = resp.headers.get("Retry-After")
    if ra:
        try:
            # Floor at 0 as well as cap: a non-conformant negative Retry-After would otherwise
            # produce time.sleep(<0) -> ValueError, escaping past a caller's narrow except.
            return max(0.0, min(float(int(ra)), MAX_RETRY_AFTER))
        except (ValueError, TypeError):
            pass
    return fallback


class _NoResponse:
    """What get() returns for a request it refused to send: status_code 0 (no response), an
    empty body and `error` saying why. Every caller's `!= 200` branch treats it as a failure."""
    __slots__ = ("status_code", "content", "headers", "error")

    def __init__(self, error):
        self.status_code = 0
        self.content = b""
        self.headers = {}
        self.error = error

    @property
    def text(self):
        return ""

    def json(self):
        raise ValueError(self.error)


def prohibited_reason(url):
    """'PROHIBITED: <rule>; nothing sent' when `url` is a PMC article-page route, else None."""
    try:
        if _hosts.host_of(url) not in GUARDED_HOSTS:
            return None
        rule = _hosts.prohibited(url)
    except Exception:
        return None
    if rule is None:
        return None
    return f"PROHIBITED: {rule.host}{rule.path_prefix} ({rule.reason}); nothing sent"


def get(url, *, retries=DEFAULT_RETRIES, backoff=DEFAULT_BACKOFF,
        retry_statuses=RETRY_STATUSES, **kwargs):
    """GET `url` with bounded retry on transient statuses.

    `kwargs` pass straight through to requests.get (params, headers, timeout, ...). Returns the
    final requests.Response (a persistent 5xx/429 comes back as that response so the caller can
    tell it apart from a genuine 404); raises the last requests exception only if EVERY attempt
    hit a network/transport error. A prohibited PMC article-page URL is not sent: the return is a
    status-0 object whose `.error` starts with PROHIBITED.
    """
    why = prohibited_reason(url)
    if why:
        print(f"  [lit_net] {why}", file=sys.stderr)
        return _NoResponse(why)
    resp = None
    for attempt in range(retries):
        try:
            resp = requests.get(url, **kwargs)
        except requests.exceptions.RequestException:
            if attempt == retries - 1:
                raise
            time.sleep(backoff * (2 ** attempt))
            continue
        if resp.status_code in retry_statuses and attempt < retries - 1:
            time.sleep(_retry_after_seconds(resp, backoff * (2 ** attempt)))
            continue
        return resp
    return resp


def stream_download(url, *, max_bytes, timeout=30, chunk_size=8192, headers=None, **kwargs):
    """Stream `url` into memory under a hard byte cap, capturing the first chunk for the caller's
    magic-byte validation (c7). Never raises for HTTP/size/transport issues -- returns a
    StreamResult so each adapter keeps its own validator, status strings, and return arity:

      - transport error:  StreamResult(0, b"", b"", 0, False, str(exc))
      - prohibited route: StreamResult(0, b"", b"", 0, False, "PROHIBITED: ...") (nothing sent)
      - HTTP != 200:       StreamResult(status_code, b"", b"", 0, False, "")
      - HTTP 200:          StreamResult(200, first_chunk, content, total, truncated, "")

    `truncated=True` means the stream exceeded `max_bytes` and `content` is the partial buffer --
    the caller MUST NOT persist a truncated download. A mid-stream transport error returns whatever
    was read so far with `error` set. The caller owns the empty/too-small/boilerplate checks, the
    file write, and status-string formatting, so per-adapter report contracts stay byte-identical.
    `headers` and any extra kwargs pass straight to requests.get.
    """
    why = prohibited_reason(url)
    if why:
        print(f"  [lit_net] {why}", file=sys.stderr)
        return StreamResult(0, b"", b"", 0, False, why)
    try:
        r = requests.get(url, headers=headers, timeout=timeout, stream=True,
                         allow_redirects=True, **kwargs)
    except requests.exceptions.RequestException as e:
        return StreamResult(0, b"", b"", 0, False, str(e))
    if r.status_code != 200:
        return StreamResult(r.status_code, b"", b"", 0, False, "")
    first = b""
    chunks = []
    total = 0
    truncated = False
    try:
        for c in r.iter_content(chunk_size=chunk_size):
            if not c:
                continue
            if not first:
                first = c
            chunks.append(c)
            total += len(c)
            if total > max_bytes:
                truncated = True
                break
    except requests.exceptions.RequestException as e:
        return StreamResult(r.status_code, first, b"".join(chunks), total, truncated, str(e))
    return StreamResult(r.status_code, first, b"".join(chunks), total, truncated, "")


# ============================================================================ DOI -> PMCID
# The PMC ID Converter API (reference/ncbi_pmc/id_converter_api.md, "Last modified: Tue June 10
# 2025"): base URL below, `tool` and `email` requested (litpipe.net adds both for this host), up
# to 200 IDs of one type per call. The legacy www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/ path
# answers 301 to it. The host 403s requests/urllib3 and serves stdlib urllib (V1-P4), so its
# litpipe.hosts row uses the urllib transport; since 2026-09-30 it has also answered 429 for
# stretches (W0 drift) and 200 again later that day (W2-A1 probe): the row refuses the host for
# the run on the first 429 and the chain below falls back.
IDCONV = "https://pmc.ncbi.nlm.nih.gov/tools/idconv/api/v1/articles/"
IDCONV_HOST = "pmc.ncbi.nlm.nih.gov"
IDCONV_BATCH = 100
EPMC_SEARCH = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
EPMC_MAX_DOIS = 100
EPMC_MAX_URL = 4000          # characters of encoded URL; 4,380 answered 200, 8,633 got 414
EPMC_PAGE_SIZE = 1000        # the documented maximum (REF: "Range 0 to 1000")
EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
ESEARCH_BATCH = 20
ESEARCH_RETMAX = 500
ESUMMARY_BATCH = 200

ROUTE_IDCONV = "idconv"
ROUTE_EPMC = "epmc_search"
ROUTE_EUTILS = "eutils"
MAP_ROUTES = (ROUTE_IDCONV, ROUTE_EPMC, ROUTE_EUTILS)


@dataclass(frozen=True)
class PmcidHit:
    """Outcome.payload of a DOI -> PMCID lookup."""
    doi: str                       # the DOI as looked up (lower case)
    pmcid: str | None = None
    release_date: str | None = None   # EMBARGOED: YYYY-MM-DD when a source gave one
    source: str = ""               # the route that answered: idconv | epmc_search | eutils
    is_open_access: str | None = None  # Europe PMC isOpenAccess (Y/N) when that step answered
    steps: tuple = ()              # every step's result for this DOI, in order ("idconv:REFUSED", ...)


def _net():
    from litpipe import net   # lazy: importing lit_net stays cheap for the metadata callers
    return net


def _refused(host, state=None) -> bool:
    try:
        return bool(_net()._state(state).is_refused(host))
    except Exception:
        return False


def _key(doi) -> str:
    return (doi or "").strip().lower()


class _Chain:
    """Per-DOI bookkeeping across the three steps."""

    def __init__(self, keys):
        self.done: dict[str, Outcome] = {}
        self.failures: dict[str, list] = {k: [] for k in keys}   # (route, Outcome)
        self.steps: dict[str, list] = {k: [] for k in keys}
        self.oa: dict[str, str] = {}

    def note(self, k, route, what):
        self.steps[k].append(f"{route}:{what}")

    def fail(self, k, route, o: Outcome):
        self.failures[k].append((route, o))
        self.note(k, route, str(o.kind))

    def settle(self, k, kind, route, host, *, status=None, pmcid=None, release=None, detail="",
               attempts=0):
        self.note(k, route, str(kind))
        hit = PmcidHit(k, pmcid, release, route, self.oa.get(k), tuple(self.steps[k]))
        self.done[k] = Outcome(kind, status=status, host=host, detail=detail, attempts=attempts,
                               payload=hit)

    def finish(self, k):
        """No step answered definitively: the first failure is the root cause; with none (only
        'Europe PMC has no PMCID' notes) the DOI is NO_MATCH."""
        fails = self.failures.get(k) or []
        hit = PmcidHit(k, None, None, fails[0][0] if fails else "", self.oa.get(k), tuple(self.steps[k]))
        if fails:
            route, o = fails[0]
            detail = f"{route}: {o.detail or o.kind} (steps: {'; '.join(self.steps[k])})"
            self.done[k] = Outcome(o.kind, status=o.status, host=o.host, detail=detail,
                                   attempts=o.attempts, retry_after=o.retry_after, payload=hit)
        else:
            self.done[k] = Outcome(Kind.NO_MATCH, host="", detail="; ".join(self.steps[k]) or "no PMCID",
                                   payload=hit)


def _chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def _idconv_step(pending, ch, *, batch, state, cfg):
    """Returns the DOIs idconv did not answer (sent on to the fallbacks)."""
    net = _net()
    left = []
    for chunk in _chunks(pending, batch):
        if _refused(IDCONV_HOST, state):
            o = Outcome(Kind.REFUSED, host=IDCONV_HOST,
                        detail=f"{IDCONV_HOST} refused earlier in this run; idconv not called")
            for k in chunk:
                ch.fail(k, ROUTE_IDCONV, o)
            left += chunk
            continue
        o = net.get(IDCONV, params={"ids": ",".join(chunk), "idtype": "doi", "format": "json"},
                    validate=net.expect_json, purpose="doi->pmcid (idconv)", state=state, cfg=cfg)
        if not o.ok:
            for k in chunk:
                ch.fail(k, ROUTE_IDCONV, o)
            left += chunk
            continue
        try:
            records = o.payload.json().get("records") or []
        except (ValueError, AttributeError) as e:
            bad = Outcome(Kind.OUTAGE, status=o.status, host=IDCONV_HOST, detail=f"unreadable JSON: {e}")
            for k in chunk:
                ch.fail(k, ROUTE_IDCONV, bad)
            left += chunk
            continue
        by_req = {}
        for rec in records:
            req = _key(rec.get("requested-id") or rec.get("doi"))
            if req:
                by_req.setdefault(req, rec)
        for k in chunk:
            rec = by_req.get(k)
            if rec is None:
                ch.note(k, ROUTE_IDCONV, "absent")
                left.append(k)
            elif rec.get("pmcid") and rec.get("live") is False:
                ch.settle(k, Kind.EMBARGOED, ROUTE_IDCONV, IDCONV_HOST, status=o.status, pmcid=rec["pmcid"],
                          release=rec.get("release-date"), attempts=o.attempts,
                          detail=f"live:false, release-date {rec.get('release-date') or 'unknown'}")
            elif rec.get("pmcid"):
                ch.settle(k, Kind.OK, ROUTE_IDCONV, IDCONV_HOST, status=o.status, pmcid=rec["pmcid"],
                          attempts=o.attempts)
            else:
                ch.settle(k, Kind.NO_MATCH, ROUTE_IDCONV, IDCONV_HOST, status=o.status, attempts=o.attempts,
                          detail=rec.get("errmsg") or "no pmcid in the idconv record")
    return left


def epmc_query(dois) -> str:
    return " OR ".join(f'DOI:"{d}"' for d in dois)


def _epmc_url_len(dois) -> int:
    return len(EPMC_SEARCH) + 1 + len(_urlencode({"query": epmc_query(dois), "resultType": "lite",
                                                   "format": "json", "pageSize": EPMC_PAGE_SIZE}))


def epmc_batches(dois, max_dois=EPMC_MAX_DOIS, max_url=EPMC_MAX_URL):
    """Greedy batches of at most `max_dois` DOIs whose search URL stays within `max_url`
    characters. A single DOI longer than the cap still gets a batch of its own."""
    out, cur = [], []
    for d in dois:
        if cur and (len(cur) >= max_dois or _epmc_url_len(cur + [d]) > max_url):
            out.append(cur)
            cur = []
        cur.append(d)
    if cur:
        out.append(cur)
    return out


def _epmc_step(pending, ch, *, state, cfg):
    """Returns the DOIs Europe PMC did not map to a PMCID (sent on to E-utilities)."""
    net = _net()
    left = []
    usable = [k for k in pending if '"' not in k]
    for k in pending:
        if '"' in k:
            ch.note(k, ROUTE_EPMC, "not sent (quote in DOI)")
            left.append(k)
    for batch in epmc_batches(usable):
        o = net.get(EPMC_SEARCH, params={"query": epmc_query(batch), "resultType": "lite", "format": "json",
                                         "pageSize": EPMC_PAGE_SIZE},
                    validate=net.expect_json, timeout=(10, 60), purpose="doi->pmcid (Europe PMC search)",
                    state=state, cfg=cfg)
        if o.ok:
            try:
                j = o.payload.json()
            except ValueError as e:
                j = {"errCode": "json", "errMsg": str(e)}
            if j.get("errCode") is not None or "resultList" not in j:
                # V1-P11: Europe PMC reports a rejected request as 200 + errCode, never as an empty list
                o = Outcome(Kind.ERROR, status=o.status, host=o.host, attempts=o.attempts,
                            detail=f"Europe PMC errCode {j.get('errCode')}: {j.get('errMsg', '')}"[:200])
        if not o.ok:
            for k in batch:
                ch.fail(k, ROUTE_EPMC, o)
            left += batch
            continue
        results = (j.get("resultList") or {}).get("result") or []
        hits = {}
        for r in results:
            d = _key(r.get("doi"))
            if d and r.get("pmcid") and d not in hits:
                hits[d] = r
        complete = int(j.get("hitCount") or 0) <= len(results)
        for k in batch:
            r = hits.get(k)
            if r is not None:
                ch.oa[k] = r.get("isOpenAccess")
                ch.settle(k, Kind.OK, ROUTE_EPMC, o.host, status=o.status, pmcid=r["pmcid"], attempts=o.attempts)
            elif not complete:
                ch.fail(k, ROUTE_EPMC, Outcome(Kind.ERROR, status=o.status, host=o.host, attempts=o.attempts,
                                               detail="Europe PMC answer incomplete (hitCount over one page)"))
                left.append(k)
            else:
                ch.note(k, ROUTE_EPMC, "no PMCID")
                left.append(k)
    return left


def _live_or_release(pmclivedate, today):
    """(live, release YYYY-MM-DD or None) from an esummary pmclivedate ('2026/09/30' or null)."""
    s = (pmclivedate or "").strip()
    if not s:
        return False, None
    try:
        d = _dt.date(*[int(x) for x in s.replace("-", "/").split("/")[:3]])
    except (ValueError, TypeError):
        return True, None
    if d > today:
        return False, d.isoformat()
    return True, None


def _eutils_step(pending, ch, *, state, cfg, today=None):
    """Returns the DOIs E-utilities could not decide (their failure is already recorded)."""
    net = _net()
    today = today or _dt.date.today()
    left = []
    usable = [k for k in pending if '"' not in k]
    for k in pending:
        if '"' in k:
            ch.note(k, ROUTE_EUTILS, "not sent (quote in DOI)")
            left.append(k)
    for batch in _chunks(usable, ESEARCH_BATCH):
        term = " OR ".join(f'"{d}"[doi]' for d in batch)
        o = net.get(f"{EUTILS}/esearch.fcgi", params={"db": "pmc", "term": term, "retmode": "json",
                                                        "retmax": ESEARCH_RETMAX},
                    validate=net.expect_json, purpose="doi->pmcid (esearch db=pmc)", state=state, cfg=cfg)
        es = {}
        if o.ok:
            try:
                j = o.payload.json()
                es = j.get("esearchresult") or {}
                if "ERROR" in j or "ERROR" in es or not es:
                    raise ValueError(str(j.get("ERROR") or es.get("ERROR") or "no esearchresult"))
            except (ValueError, AttributeError) as e:
                o = Outcome(Kind.ERROR, status=o.status, host=o.host, attempts=o.attempts,
                            detail=f"esearch error: {e}"[:200])
        if not o.ok:
            for k in batch:
                ch.fail(k, ROUTE_EUTILS, o)
            left += batch
            continue
        ids = [str(i) for i in es.get("idlist") or []]
        try:
            count = int(es.get("count") or 0)
        except ValueError:
            count = len(ids)
        complete = count <= len(ids)
        found = {}
        failed = None
        for idc in _chunks(ids, ESUMMARY_BATCH):
            s = net.get(f"{EUTILS}/esummary.fcgi", params={"db": "pmc", "id": ",".join(idc), "retmode": "json"},
                        validate=net.expect_json, purpose="doi->pmcid (esummary db=pmc)", state=state, cfg=cfg)
            res = None
            if s.ok:
                try:
                    res = s.payload.json().get("result") or {}
                except ValueError as e:
                    s = Outcome(Kind.OUTAGE, status=s.status, host=s.host, detail=f"unreadable JSON: {e}")
            if not s.ok:
                failed = s
                break
            for uid in res.get("uids") or [k for k in res if k != "uids"]:
                rec = res.get(str(uid)) or {}
                aids = {}
                for a in rec.get("articleids") or []:
                    aids.setdefault(str(a.get("idtype") or "").lower(), str(a.get("value") or ""))
                d = _key(aids.get("doi"))
                if d in batch and d not in found:
                    pmcid = aids.get("pmcid") or f"PMC{uid}"
                    if not pmcid.upper().startswith("PMC"):
                        pmcid = f"PMC{pmcid}"
                    found[d] = (pmcid, rec.get("pmclivedate"), s)
        if failed is not None:
            for k in batch:
                if k not in found:
                    ch.fail(k, ROUTE_EUTILS, failed)
                    left.append(k)
        for k in batch:
            if k in found:
                pmcid, livedate, s = found[k]
                live, release = _live_or_release(livedate, today)
                if live:
                    ch.settle(k, Kind.OK, ROUTE_EUTILS, s.host, status=s.status, pmcid=pmcid, attempts=s.attempts)
                else:
                    ch.settle(k, Kind.EMBARGOED, ROUTE_EUTILS, s.host, status=s.status, pmcid=pmcid,
                              release=release, attempts=s.attempts,
                              detail=f"esummary pmclivedate {livedate or 'null'}: not live in PMC yet")
            elif failed is not None:
                continue
            elif complete:
                ch.settle(k, Kind.NO_MATCH, ROUTE_EUTILS, o.host, status=o.status, attempts=o.attempts,
                          detail="no PMC record carries this DOI")
            else:
                ch.fail(k, ROUTE_EUTILS, Outcome(Kind.ERROR, status=o.status, host=o.host, attempts=o.attempts,
                                                 detail=f"esearch count {count} over retmax {len(ids)}"))
                left.append(k)
    return left


def doi_to_pmcid(dois, *, state=None, cfg=None, idconv_batch=IDCONV_BATCH, today=None) -> dict:
    """{doi_lower: Outcome} for every non-blank DOI in `dois` (see the module docstring for the
    chain). Outcome kinds: OK (payload.pmcid), EMBARGOED (payload.release_date when known),
    NO_MATCH (a service answered: not in PMC; an invalid DOI is NO_MATCH without a request), or
    the first failure's kind when no step could answer. payload is always a PmcidHit whose
    `steps` list what each step said."""
    keys = []
    for d in dois or ():
        k = _key(d)
        if k and k not in keys:
            keys.append(k)
    ch = _Chain(keys)
    pending = []
    for k in keys:
        if lit_util.is_valid_doi(k):
            pending.append(k)
        else:
            ch.settle(k, Kind.NO_MATCH, "", "", detail="not a valid DOI; not looked up")
    if pending:
        if _refused(IDCONV_HOST, state):
            o = Outcome(Kind.REFUSED, host=IDCONV_HOST, detail=f"{IDCONV_HOST} is refused; idconv not called")
            for k in pending:
                ch.fail(k, ROUTE_IDCONV, o)
        else:
            pending = _idconv_step(pending, ch, batch=max(1, min(int(idconv_batch), 200)), state=state, cfg=cfg)
    if pending:
        pending = _epmc_step(pending, ch, state=state, cfg=cfg)
    if pending:
        pending = _eutils_step(pending, ch, state=state, cfg=cfg, today=today)
    for k in pending:
        ch.finish(k)
    return {k: ch.done[k] for k in keys}


def doi_to_pmcid_batch(dois, *, ua=None, email=None, tool=None, batch_size=IDCONV_BATCH):
    """Legacy dict form of doi_to_pmcid: {doi_lower: pmcid} for DOIs with a live PMCID (dispatch
    0.6; backfill_fulltext, recheck_pmc). `ua`, `email` and `tool` are ignored (litpipe.net sends
    the identity). DOIs whose lookup failed are listed on stderr: they are NOT "no PMCID"."""
    res = doi_to_pmcid(dois, idconv_batch=batch_size)
    out = {k: o.payload.pmcid for k, o in res.items() if o.kind is Kind.OK and o.payload.pmcid}
    failed = Counter(str(o.kind) for o in res.values()
                     if o.kind not in (Kind.OK, Kind.NO_MATCH, Kind.EMBARGOED))
    emb = sum(1 for o in res.values() if o.kind is Kind.EMBARGOED)
    if failed:
        first = next(o for o in res.values() if o.kind not in (Kind.OK, Kind.NO_MATCH, Kind.EMBARGOED))
        from litpipe.ledger import redact
        print(f"  [pmcid] {sum(failed.values())} DOI(s) could not be looked up "
              f"({', '.join(f'{k} {v}' for k, v in sorted(failed.items()))}); they are NOT 'no PMCID'. "
              f"First cause: {redact(first.detail)[:160]}", file=sys.stderr)
    if emb:
        print(f"  [pmcid] {emb} DOI(s) are embargoed in PMC (not fetchable yet); left out", file=sys.stderr)
    return out


# ---------------------------------------------------------------- legacy idconv shim
class _IdconvResponse:
    """Minimal requests-Response shim (harvest_citations.pmid_to_doi reads status_code, text,
    json(), error). status_code 0 means no response (transport failure, refused host, prohibited
    route); `error` says why."""
    __slots__ = ("status_code", "text", "error")

    def __init__(self, status_code, text, error=""):
        self.status_code = status_code
        self.text = text
        self.error = error

    def json(self):
        return _json.loads(self.text)


def _idconv_get(url, params, ua=None, timeout=30):
    """One idconv GET through litpipe.net, in the legacy response shape (harvest_citations until
    W2-A2 moves pmid_to_doi to esummary). `ua` and any `tool` / `email` in `params` are ignored:
    litpipe.net sends the identity, never `email=None`. Never raises; no blind retry (the host row
    refuses pmc.ncbi for the run on its first 429)."""
    net = _net()
    p = {k: v for k, v in (params or {}).items() if k not in ("tool", "email")}
    try:
        o = net.get(url, params=p, timeout=(10, timeout), purpose="idconv (legacy)")
    except net.ProhibitedHost as e:
        return _IdconvResponse(0, "", f"PROHIBITED: {e}")
    if o.payload is not None:
        return _IdconvResponse(o.payload.status, o.payload.text)
    return _IdconvResponse(0, "", f"{o.kind}: {o.detail}")
