"""lit_net -- shared HTTP GET with bounded retry for the fetch/enrich stack (B1).

One hardened GET so every metadata call gets the same 429/5xx backoff that
forward_citations.s2_get and fill_missing_dois.crossref_query already implement ad hoc.

Drop-in for requests.get: it RETURNS the final requests.Response, so callers keep their own
`r.status_code` / `r.json()` handling unchanged. It retries 429 and 5xx (500/502/503/504) with
exponential backoff, honoring a Retry-After header (capped). 404 and every other status are
terminal -- returned immediately. On a network/transport error it retries too, and if every
attempt fails it re-raises the last exception -- the same surface requests.get already presents,
so no caller that already wraps requests.get gains new raise-exposure.

Scope note: B4a routes the metadata GETs through here (Unpaywall lookup, CrossRef/DataCite,
arXiv/EPMC/OSF search, CrossRef abstract). The streaming PDF/image downloads move to a stream
helper in c7, and the NCBI id-converter batch in c8.
"""
import sys
import time
from collections import namedtuple

import requests

import lit_util  # is_valid_doi gate for doi_to_pmcid_batch (lit_util is stdlib-pure -> no import cycle)

RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
DEFAULT_RETRIES = 3
DEFAULT_BACKOFF = 1.0    # base seconds for exponential backoff between attempts
MAX_RETRY_AFTER = 30.0   # cap a server Retry-After so an absurd value can't stall a run

# c7: one PDF-size cap across the three PDF fetchers (was 50MB unpaywall / 60MB pmc+preprint).
# Real library measured 2026-07 (n=4364 PDFs): max 50.94MB -> 80MB is ~57% headroom, rejects zero
# real papers, still catches an absurd mis-fetch. fetch_figures keeps its own (smaller) image cap.
MAX_PDF_BYTES = 80_000_000


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


class _UrllibResponse:
    """requests-Response subset used by get()'s callers: status_code, text, content, json(),
    headers (case-insensitive .get, like requests'). status_code 0 means no response at all
    (DNS, timeout, connection failure) and `error` carries the reason."""
    __slots__ = ("status_code", "content", "headers", "error")

    def __init__(self, status_code, content, headers=None, error=""):
        self.status_code = status_code
        self.content = content
        self.headers = headers if headers is not None else {}
        self.error = error

    @property
    def text(self):
        return self.content.decode("utf-8", "replace")

    def json(self):
        return _json.loads(self.text)


def _urllib_get_once(url, headers, timeout):
    """One urllib GET. Never raises for HTTP status or transport failure (REG-I22): URLError
    (DNS, refused), TimeoutError, connection resets and http.client errors such as IncompleteRead
    all come back as status 0 with the error text, which every caller's `!= 200` branch already
    treats as a failure."""
    req = _urlrequest.Request(url, headers=dict(headers or {}))
    try:
        with _urlrequest.urlopen(req, timeout=timeout) as fh:
            return _UrllibResponse(fh.status, fh.read(), fh.headers)
    except _urlerror.HTTPError as e:
        try:
            body = e.read() if e.fp else b""
        except (OSError, _httpclient.HTTPException):
            body = b""
        return _UrllibResponse(e.code, body, e.headers)
    except (OSError, _httpclient.HTTPException) as e:  # URLError and TimeoutError are OSErrors
        return _UrllibResponse(0, b"", None, f"{type(e).__name__}: {e}")


def _urllib_get(url, *, params=None, headers=None, timeout=30, retries=DEFAULT_RETRIES,
                backoff=DEFAULT_BACKOFF, retry_statuses=RETRY_STATUSES, **_ignored):
    """get()'s transport twin for URLLIB_ONLY_HOSTS, with get()'s retry loop (V1-N5): transport
    failures and `retry_statuses` are retried with the same backoff, honouring Retry-After. It
    never raises; after the last attempt it returns that attempt's response (status 0 for a
    transport failure)."""
    if params:
        url = url + ("&" if "?" in url else "?") + _urlencode(params)
    resp = None
    for attempt in range(max(1, retries)):
        resp = _urllib_get_once(url, headers, timeout)
        if attempt < retries - 1:
            if resp.status_code == 0:
                time.sleep(backoff * (2 ** attempt))
                continue
            if resp.status_code in retry_statuses:
                time.sleep(_retry_after_seconds(resp, backoff * (2 ** attempt)))
                continue
        return resp
    return resp


def get(url, *, retries=DEFAULT_RETRIES, backoff=DEFAULT_BACKOFF,
        retry_statuses=RETRY_STATUSES, **kwargs):
    """GET `url` with bounded retry on transient statuses.

    `kwargs` pass straight through to requests.get (params, headers, timeout, ...). Returns the
    final requests.Response (a persistent 5xx/429 comes back as that response so the caller can
    tell it apart from a genuine 404); raises the last requests exception only if EVERY attempt
    hit a network/transport error. Exception: URLLIB_ONLY_HOSTS never raise; an all-transport-
    failure run returns a status-0 response whose `.error` says why.
    """
    if _urllib_only_host(url):
        # 2026-09-01: same host block as stream_download. fetch_figures hit this and coded a
        # ">30KB means real page" size heuristic around the 403 interstitial rather than
        # diagnosing it. Route through urllib so callers get the real page.
        return _urllib_get(url, retries=retries, backoff=backoff,
                           retry_statuses=retry_statuses, **kwargs)
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


import http.client as _httpclient
import json as _json
import urllib.request as _urlrequest
import urllib.error as _urlerror
from urllib.parse import urlencode as _urlencode, urlparse as _urlparse

StreamResult = namedtuple("StreamResult", "status_code first_chunk content total truncated error")


# Hosts that reject the requests/urllib3 client but serve stdlib urllib (see stream_download).
URLLIB_ONLY_HOSTS = ("pmc.ncbi.nlm.nih.gov",)


def _urllib_only_host(url):
    try:
        return _urlparse(url).hostname in URLLIB_ONLY_HOSTS
    except Exception:
        return False


def _urllib_stream(url, *, max_bytes, timeout=30, chunk_size=8192, headers=None):
    """stream_download's transport twin for URLLIB_ONLY_HOSTS. Same StreamResult contract:
    never raises for HTTP/size/transport issues."""
    req = _urlrequest.Request(url, headers=dict(headers or {}))
    try:
        fh = _urlrequest.urlopen(req, timeout=timeout)
    except _urlerror.HTTPError as e:
        return StreamResult(e.code, b"", b"", 0, False, "")
    except (OSError, _httpclient.HTTPException) as e:  # URLError, TimeoutError, resets
        return StreamResult(0, b"", b"", 0, False, str(e))
    first, chunks, total, truncated = b"", [], 0, False
    try:
        with fh:
            if fh.status != 200:
                return StreamResult(fh.status, b"", b"", 0, False, "")
            while True:
                c = fh.read(chunk_size)
                if not c:
                    break
                if not first:
                    first = c
                chunks.append(c)
                total += len(c)
                if total > max_bytes:
                    truncated = True
                    break
    except (OSError, _httpclient.HTTPException) as e:  # IncompleteRead is not an OSError
        return StreamResult(200, first, b"".join(chunks), total, truncated, str(e))
    return StreamResult(200, first, b"".join(chunks), total, truncated, "")


def stream_download(url, *, max_bytes, timeout=30, chunk_size=8192, headers=None, **kwargs):
    """Stream `url` into memory under a hard byte cap, capturing the first chunk for the caller's
    magic-byte validation (c7). Never raises for HTTP/size/transport issues -- returns a
    StreamResult so each adapter keeps its own validator, status strings, and return arity:

      - transport error:  StreamResult(0, b"", b"", 0, False, str(exc))
      - HTTP != 200:       StreamResult(status_code, b"", b"", 0, False, "")
      - HTTP 200:          StreamResult(200, first_chunk, content, total, truncated, "")

    `truncated=True` means the stream exceeded `max_bytes` and `content` is the partial buffer --
    the caller MUST NOT persist a truncated download. A mid-stream transport error returns whatever
    was read so far with `error` set. The caller owns the empty/too-small/boilerplate checks, the
    file write, and status-string formatting, so per-adapter report contracts stay byte-identical.
    `headers` and any extra kwargs pass straight to requests.get.
    """
    if _urllib_only_host(url):
        # 2026-09-01: pmc.ncbi.nlm.nih.gov 403s requests/urllib3 at a level below headers while
        # serving stdlib urllib normally. This killed pmc_fetch's NCBI citation_pdf_url fallback
        # (reported as the unexplained "ncbi-page/HTML" -- it was a 403 error page being parsed as
        # HTML) and fetch_figures. Same block as IDCONV; fixed once here rather than per call site.
        return _urllib_stream(url, max_bytes=max_bytes, timeout=timeout,
                              chunk_size=chunk_size, headers=headers)
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


# 2026-09-01: NCBI retired the www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/ path. It now 302s to
# the address below, and that host returns HTTP 403 to requests/urllib3 while returning 200 to
# stdlib urllib for a byte-identical URL, UA and header set (verified back-to-back in one process;
# UA, Accept, Accept-Encoding and Connection were each ruled out, so the block is below the header
# layer). The whole PMC stage was silently dead: every row came back NO_PMCID. Point at the real
# endpoint and fetch it with urllib. See _idconv_get.
IDCONV = "https://pmc.ncbi.nlm.nih.gov/tools/idconv/api/v1/articles/"


class _IdconvResponse:
    """Minimal requests-Response shim so doi_to_pmcid_batch's status/json logic is unchanged.
    status_code 0 means no response (DNS, timeout, reset); `error` says why."""
    __slots__ = ("status_code", "text", "error")

    def __init__(self, status_code, text, error=""):
        self.status_code = status_code
        self.text = text
        self.error = error

    def json(self):
        return _json.loads(self.text)


def _idconv_get(url, params, ua, timeout=30):
    """GET idconv via stdlib urllib. Returns an _IdconvResponse; never raises for HTTP status or
    transport failure (REG-I22). No retry here: since 2026-09-30 the host answers 429 to a second
    request within seconds, and pacing belongs to the host policy (W2-A1), not a blind retry."""
    full = url + ("&" if "?" in url else "?") + _urlencode(params)
    req = _urlrequest.Request(full, headers={"User-Agent": ua})
    try:
        with _urlrequest.urlopen(req, timeout=timeout) as fh:
            return _IdconvResponse(fh.status, fh.read().decode("utf-8", "replace"))
    except _urlerror.HTTPError as e:
        try:
            body = e.read().decode("utf-8", "replace") if e.fp else ""
        except (OSError, _httpclient.HTTPException):
            body = ""
        return _IdconvResponse(e.code, body)
    except (OSError, _httpclient.HTTPException) as e:  # URLError and TimeoutError are OSErrors
        return _IdconvResponse(0, "", f"{type(e).__name__}: {e}")


def doi_to_pmcid_batch(dois, *, ua, email, tool="GETPAID", batch_size=100):
    """Map DOIs -> PMCIDs via the NCBI ID converter (idconv). Returns {doi_lower: pmcid} (c8).

    Consolidates 3 byte-identical copies (pmc_fetch / backfill_fulltext / recheck_pmc). B2 hardening
    over those copies:
      - malformed DOIs are dropped up front (lit_util.is_valid_doi, which rejects the unicode-dash
        typography artifacts idconv 400s on) so they never poison a chunk;
      - the GET goes through get() -> bounded 429/5xx retry;
      - a chunk that still returns HTTP 400 (idconv 400s the WHOLE batch for a single bad id and
        returns records=[], which the old code silently reclassified as no-PMCID for up to batch_size
        valid papers) is retried one DOI at a time, so at most the one offending id is dropped;
      - only records with BOTH a doi and a pmcid are recorded -- guards the empty-key bug where
        recheck_pmc wrote out[''] for a record whose doi/requested-id was blank.
    """
    out = {}
    valid = [d for d in dois if lit_util.is_valid_doi(d)]
    for i in range(0, len(valid), batch_size):
        chunk = valid[i:i + batch_size]
        params = {"tool": tool, "email": email, "ids": ",".join(chunk),
                  "idtype": "doi", "format": "json"}
        try:
            r = _idconv_get(IDCONV, params, ua, timeout=30)
        except (OSError, _urlerror.URLError) as e:
            print(f"  [idconv batch {i}] error: {e}", file=sys.stderr)
            continue
        if r.status_code == 400 and len(chunk) > 1:
            # B2 poisoned-chunk recovery: one malformed id 400s the whole batch. Isolate it so the
            # rest still resolve, instead of losing up to batch_size valid papers to "no-PMCID".
            for one in chunk:
                out.update(doi_to_pmcid_batch([one], ua=ua, email=email, tool=tool, batch_size=1))
            continue
        if r.status_code != 200:
            why = f"transport error: {r.error}" if r.status_code == 0 else f"HTTP {r.status_code}"
            print(f"  [idconv batch {i}] {why}; chunk of {len(chunk)} skipped (its rows will read "
                  f"as NO_PMCID)", file=sys.stderr)
            time.sleep(0.4)
            continue
        try:
            data = r.json()
        except ValueError as e:
            print(f"  [idconv batch {i}] non-JSON body (HTTP {r.status_code}): {e}", file=sys.stderr)
            time.sleep(0.4)
            continue
        for rec in data.get("records", []):
            doi = (rec.get("doi") or rec.get("requested-id") or "").lower()
            pmcid = rec.get("pmcid")
            if doi and pmcid:
                out[doi] = pmcid
        time.sleep(0.4)  # be nice to NCBI
    return out
