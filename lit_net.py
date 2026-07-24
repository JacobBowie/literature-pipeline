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
import time
from collections import namedtuple

import requests

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


def get(url, *, retries=DEFAULT_RETRIES, backoff=DEFAULT_BACKOFF,
        retry_statuses=RETRY_STATUSES, **kwargs):
    """GET `url` with bounded retry on transient statuses.

    `kwargs` pass straight through to requests.get (params, headers, timeout, ...). Returns the
    final requests.Response (a persistent 5xx/429 comes back as that response so the caller can
    tell it apart from a genuine 404); raises the last requests exception only if EVERY attempt
    hit a network/transport error.
    """
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


StreamResult = namedtuple("StreamResult", "status_code first_chunk content total truncated error")


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
