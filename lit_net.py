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

import requests

RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
DEFAULT_RETRIES = 3
DEFAULT_BACKOFF = 1.0    # base seconds for exponential backoff between attempts
MAX_RETRY_AFTER = 30.0   # cap a server Retry-After so an absurd value can't stall a run


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
