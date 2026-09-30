"""Typed outcomes: one vocabulary for what happened to a request or a row (dispatch 0.5).

Every client in the pipeline used to invent its own status strings ("HTTP_403", "NO_PMCID",
"HTML", "ERR_<exception text>"), and several turned a failed call into an empty result. The
typed Kind says what happened; the residual router (sweep / migrate_closed_to_md) decides what
to do with it. A failed call is never an empty payload: it is an Outcome whose kind says why.

`from_legacy` maps the strings the stages still write until they are rewired (W2), in the
order the dispatch fixes; the first matching rule wins.
"""
import re
from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class Kind(StrEnum):
    OK = "OK"
    NO_MATCH = "NO_MATCH"            # the source answered: it has no such record
    NOT_AT_RA = "NOT_AT_RA"          # the DOI is registered with an agency this source does not cover
    NOT_AVAILABLE = "NOT_AVAILABLE"  # the record exists, the content does not (closed, no full text)
    EMBARGOED = "EMBARGOED"          # available later; retry after the release date
    ALIASED = "ALIASED"              # resolved under another DOI
    REFUSED = "REFUSED"              # the host declined (403, 406, final 429, HTML wall)
    OUTAGE = "OUTAGE"                # the host failed (5xx, empty 200)
    DEFERRED = "DEFERRED"            # not attempted now: budget spent or a long Retry-After
    CONFIG = "CONFIG"                # our configuration is wrong; the run aborts
    ERROR = "ERROR"                  # anything unclassified
    TRANSPORT = "TRANSPORT"          # DNS, timeout, connection failure
    SKIPPED = "SKIPPED"              # deliberately not attempted


@dataclass(frozen=True, slots=True)
class Outcome:
    """What one request (or one row's stage) came to.

    status: the HTTP status of the response that decided the outcome, None when there was none.
    attempts: requests actually sent. payload: the result on OK (parsed JSON, bytes, a DOI...),
    and whatever diagnostic body a client chooses to keep otherwise. A string kind is coerced
    to Kind, so typed columns read back from a CSV round-trip; an unknown string raises.
    """
    kind: Kind
    status: int | None = None
    host: str = ""
    detail: str = ""
    attempts: int = 0
    elapsed_ms: int = 0
    retry_after: float | None = None
    payload: Any = None

    def __post_init__(self):
        if not isinstance(self.kind, Kind):
            object.__setattr__(self, "kind", Kind(self.kind))

    @property
    def ok(self) -> bool:
        return self.kind is Kind.OK


# ---------------------------------------------------------------- legacy status strings
# Shapes seen in the stage reports (plan_evidence/B_usage/errors_by_month_corpus.csv and the
# fetchers' writers): "HTTP_403" (download), "HTTP 404" (unpaywall_lookup's API form, a space,
# nothing else in the string), "europepmc/HTTP_500" (pmc attempts), urllib's "HTTP Error 404",
# bare tokens ("HTML", "NO_PMCID", "CLOSED", "MANUAL_PREPRINT"), and raw exception text
# ("HTTPSConnectionPool(...): Max retries exceeded ... NameResolutionError", "ERR_...").
_HTTP_CODE = re.compile(r"\bHTTP(?:[ _]|\s+Error\s+)(\d{3})\b")
_TOO_MANY = re.compile(r"too many (\d{3}) error responses", re.IGNORECASE)  # urllib3 retry exhaustion
_API_FORM = re.compile(r"HTTP (\d{3})")                                    # unpaywall_lookup, fullmatch
_TRANSPORT = re.compile(
    r"NameResolution|getaddrinfo|Failed to resolve|Name or service not known|nodename nor servname"
    r"|Temporary failure in name resolution|No address associated|timed out|Timeout"
    r"|ConnectionError|Connection aborted|Connection reset|Connection refused|RemoteDisconnected"
    r"|ConnectionResetError|ConnectionRefusedError|ConnectionAbortedError|URLError|SSLError"
    r"|SSLEOFError|ProxyError|ChunkedEncodingError|IncompleteRead|ProtocolError"
    r"|HTTPS?ConnectionPool|WinError 100\d\d|Max retries exceeded",
    re.IGNORECASE)
_FULLTEXT_STAGES = frozenset({"fulltext", "fulltextxml", "jats", "sidecar"})
_NO_MATCH_TOKENS = frozenset({"NO_PMCID", "NOT_FOUND", "NOT_IN_UNPAYWALL", "NO_MATCH"})
_REFUSED_TOKENS = frozenset({"HTML", "NOT_PDF"})
_DONE_TOKENS = frozenset({"OK", "SKIP_EXISTS", "ALREADY_EXISTS"})  # a file is in place


def _words(s):
    return set(re.split(r"[^A-Za-z0-9_]+", s))


def from_legacy(error_string, stage=None) -> Kind:
    """Map a legacy stage status string to a Kind. First matching rule wins (dispatch 0.5):

      1. Europe PMC fullTextXML 500, `europepmc/HTTP_500`   -> NOT_AVAILABLE
      2. HTTP 406, a final HTTP 429                          -> REFUSED (host-level)
      3. HTTP 403                                            -> REFUSED (row-level)
      4. other 5xx, an empty 200 (`EMPTY`)                   -> OUTAGE
      5. DNS, timeout, connection errors                     -> TRANSPORT
      6. NO_PMCID, NOT_FOUND, NOT_IN_UNPAYWALL, NO_MATCH,
         and unpaywall_lookup's "HTTP 404" (that IS not-in-Unpaywall) -> NO_MATCH
      7. CLOSED                                              -> NOT_AVAILABLE
      8. HTML, NOT_PDF, HTTP 202                             -> REFUSED
      9. Unpaywall API 422 / 410 ("HTTP 422", the API form)  -> CONFIG
     10. MANUAL_PREPRINT                                     -> SKIPPED (detail manual_preprint)
      else                                                   -> ERROR

    Before rule 1, the non-failure strings OK / SKIP_EXISTS / ALREADY_EXISTS map to OK and DRY
    to SKIPPED, so a caller passing a whole status column never files a success as ERROR.
    `stage` names the producing stage ("unpaywall", "pmc", "preprint", "fulltext"); rules 1 and
    9 read it. Every other rule is stage-independent.
    """
    s = (error_string or "").strip()
    st = (stage or "").strip().lower()
    if s in _DONE_TOKENS:
        return Kind.OK
    if s == "DRY":
        return Kind.SKIPPED
    low = s.lower()
    codes = [int(c) for c in _HTTP_CODE.findall(s)] + [int(c) for c in _TOO_MANY.findall(s)]
    api = _API_FORM.fullmatch(s)
    api_code = int(api.group(1)) if api and st in ("", "unpaywall") else None
    words = _words(s)

    if "europepmc/http_500" in low or (500 in codes and (st in _FULLTEXT_STAGES or "fulltextxml" in low)):
        return Kind.NOT_AVAILABLE
    if 406 in codes or 429 in codes:
        return Kind.REFUSED
    if 403 in codes:
        return Kind.REFUSED
    if any(500 <= c <= 599 for c in codes) or "EMPTY" in words:
        return Kind.OUTAGE
    if _TRANSPORT.search(s):
        return Kind.TRANSPORT
    if words & _NO_MATCH_TOKENS or api_code == 404:
        return Kind.NO_MATCH
    if "CLOSED" in words:
        return Kind.NOT_AVAILABLE
    if words & _REFUSED_TOKENS or 202 in codes:
        return Kind.REFUSED
    if api_code in (410, 422):
        return Kind.CONFIG
    if "MANUAL_PREPRINT" in words:
        return Kind.SKIPPED
    return Kind.ERROR


def legacy_outcome(error_string, stage=None, host="") -> Outcome:
    """from_legacy as a full Outcome: the kind, the first HTTP status found in the string, and
    the detail. MANUAL_PREPRINT carries detail "manual_preprint" (the residual router sends it to
    OA_BLOCKED); every other detail is the original string. The detail is NOT redacted here:
    whoever persists it passes it through litpipe.ledger.redact (legacy exception text can carry
    an `email=` query value)."""
    s = (error_string or "").strip()
    kind = from_legacy(s, stage)
    m = _HTTP_CODE.search(s) or _TOO_MANY.search(s)
    detail = "manual_preprint" if kind is Kind.SKIPPED and "MANUAL_PREPRINT" in _words(s) else s
    return Outcome(kind, status=int(m.group(1)) if m else None, host=host, detail=detail)
