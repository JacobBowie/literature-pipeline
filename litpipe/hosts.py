"""Host policy table: how the pipeline may talk to each host (dispatch 0.5, refactor scope §2.2).

`policy(url_or_host)` returns the HostPolicy for a URL or a bare host name; `register(policy)` lets a
client module add or tune its own row at import (litpipe/s2.py registers api.semanticscholar.org
with its key-dependent interval, litpipe/openalex.py its budget). `prohibited(url)` / `check(url)`
apply the (host, path_prefix) rules for routes that are never automated.

The table is data for three consumers:
  * litpipe.net      identity, transport, redirect allow-list, retry and refusal rules;
  * litpipe.state    pacing (min_interval_s, measured from the END of the previous attempt),
                     concurrency and daily_budget, looked up with `policy(host)`;
  * the runner       the off-peak window (NCBI asks for large series to run off-peak).

Host matching: exact host name first, then the longest "suffix row" (a row whose host starts with
".", e.g. ".osf.io" for files.de-1.osf.io). The returned policy always carries the real host name,
so pacing and refusals key on the host actually contacted. An unknown host gets DEFAULT: 2 s,
concurrency 1, identity "download", and `known=False` (the ledger notes it).

Two rows depend on projects.json `hosts` switches (config.hosts(), default off):
  arxiv_pdf_allowed    arxiv.org/pdf/ is prohibited unless on; when on, arxiv.org is paced at 15 s
                       (robots.txt Crawl-delay: 15; plan DEC-09);
  biorxiv_pdf_allowed  www.biorxiv.org and www.medrxiv.org are prohibited unless on (DEC-10).
"""
from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from urllib.parse import urlsplit

from litpipe import config
from litpipe.outcomes import Kind

# Identity modes (litpipe.net applies them):
#   "ua"           API host: our User-Agent with the contact mailto; a caller's User-Agent is replaced.
#   "ncbi"         "ua" plus the NCBI `tool=literature-pipeline` and `email=` query parameters.
#   "email_param"  "ua" plus an `email=` query parameter (Unpaywall: the email is its only credential).
#   "anonymous"    our User-Agent without the mailto (public object stores).
#   "download"     a caller's User-Agent is kept (publisher PDF hosts block API clients), else our
#                  User-Agent without the mailto: the email is not handed to arbitrary hosts.
IDENTITY_MODES = frozenset({"ua", "ncbi", "email_param", "anonymous", "download"})
TRANSPORTS = frozenset({"requests", "urllib"})
PERSISTENCE = frozenset({"run", "manual"})
ANY_HOST = "*"   # redirect_allow entry: follow to any host that is not prohibited


class ProhibitedHost(Exception):
    """The URL is on the never-automated list; raised before anything is sent."""

    def __init__(self, url, rule):
        self.rule = rule
        super().__init__(f"prohibited route {rule.host}{rule.path_prefix} ({rule.reason})")


@dataclass(frozen=True)
class RetryPolicy:
    """S2Retry generalised (notes/2026-09-23_s2_probes/P7_findings.md, d_proposed): the first
    wait is first_wait_s (never 0), then doubling plus U(0, jitter_s), capped at max_wait_s, at
    most max_retries retries. A Retry-After at or under the host's inline cap replaces the wait
    only when longer (a Retry-After of 0 never bursts). Transport failures (DNS, reset, timeout)
    share the same waits but are capped separately at transport_retries."""
    max_retries: int = 6
    first_wait_s: float = 2.0
    factor: float = 2.0
    jitter_s: float = 0.5
    max_wait_s: float = 120.0
    statuses: frozenset = frozenset({429, 500, 502, 503, 504})
    transport_retries: int = 2

    def backoff(self, n, rand=0.0):
        """Wait before retry n (1-based): first_wait_s * factor**(n-1) + rand*jitter_s, capped."""
        return max(0.0, min(self.max_wait_s,
                            self.first_wait_s * self.factor ** (n - 1) + rand * self.jitter_s))


@dataclass(frozen=True)
class OffPeak:
    """A scheduling hint for the runner (not enforced by litpipe.net)."""
    start: str          # "HH:MM" local to tz
    end: str
    tz: str
    weekends: bool = True


NCBI_OFFPEAK = OffPeak("21:00", "05:00", "America/New_York", weekends=True)


@dataclass(frozen=True)
class HostPolicy:
    host: str
    min_interval_s: float = 2.0
    concurrency: int = 1
    daily_budget: int | None = None
    offpeak: OffPeak | None = None
    identity: str = "ua"
    transport: str = "requests"
    redirect_allow: tuple = ()          # hosts a redirect from this host may go to (same host is always allowed)
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    refuse_after_consecutive_403: int = 1   # DEC-06 (measured): the first 403 refuses the host for the run
    refuse_host_statuses: frozenset = frozenset({406, 429})   # a FINAL response with one of these refuses the host
    refusal_persistence: str = "run"    # "manual": stays refused until `python -m litpipe.state --clear-refusal HOST`
    retry_after_cap_s: float = 30.0     # a Retry-After above this defers the host instead of sleeping in the row
    status_kinds: dict = field(default_factory=dict)   # final-status overrides, e.g. Unpaywall 422 -> CONFIG
    https_only: bool = True             # a redirect hop may not downgrade https to http
    known: bool = True
    note: str = ""

    def __post_init__(self):
        object.__setattr__(self, "host", self.host.strip().lower())
        if self.identity not in IDENTITY_MODES:
            raise ValueError(f"{self.host}: identity {self.identity!r} not in {sorted(IDENTITY_MODES)}")
        if self.transport not in TRANSPORTS:
            raise ValueError(f"{self.host}: transport {self.transport!r} not in {sorted(TRANSPORTS)}")
        if self.refusal_persistence not in PERSISTENCE:
            raise ValueError(f"{self.host}: refusal_persistence {self.refusal_persistence!r}")
        if self.refuse_after_consecutive_403 < 1:
            raise ValueError(f"{self.host}: refuse_after_consecutive_403 must be >= 1")
        object.__setattr__(self, "redirect_allow", tuple(h.lower() for h in self.redirect_allow))

    def allows_redirect_to(self, target_host) -> bool:
        t = (target_host or "").lower()
        if t == self.host:
            return True
        for a in self.redirect_allow:
            if a == ANY_HOST or a == t or (a.startswith(".") and t.endswith(a)):
                return True
        return False


@dataclass(frozen=True)
class ProhibitedRule:
    host: str
    path_prefix: str = "/"
    reason: str = ""
    unless_switch: str | None = None    # a config.hosts() switch that lifts the rule when true


# Retry without 429: on these hosts a 429 is final at once (and refuses the host), never retried
# into a longer ban.
_NO_429_RETRY = RetryPolicy(statuses=frozenset({500, 502, 503, 504}))

# Where doi.org content negotiation sends a request (V3 raw/s30*, s31*: Crossref, DataCite via
# crosscite, mEDRA, KISTI, JaLC, OP). The ISTIC hop is plain http to a raw IP: not followed.
_DOI_CN_TARGETS = ("api.crossref.org", "data.crossref.org", "data.crosscite.org", "api.datacite.org",
                   "data.datacite.org", "data.medra.org", "data.doi.or.kr", "japanlinkcenter.org",
                   "ra.publications.europa.eu")
_OSF_HOPS = ("osf.io", ".osf.io", "storage.googleapis.com")   # V4: download = 1 API call + 3 hops


def _rows():
    """The §2.2 table. Intervals are the runner settings; sources in the refactor scope."""
    return [
        HostPolicy("api.crossref.org", min_interval_s=0.34,
                   note="polite pool (limits per email address since 2026-07-21, per IP before): 10/s singleton, 3/s list, concurrency 3 (paced to 3/s, concurrency 1); stop at >=10% errors"),
        HostPolicy("api.datacite.org", min_interval_s=0.5, note="1000 / 5 min per IP; no rate headers"),
        HostPolicy("doi.org", min_interval_s=1.0, redirect_allow=_DOI_CN_TARGETS,
                   note="RA lookup and content negotiation; follows the https RA allow-list only"),
        HostPolicy("api.unpaywall.org", min_interval_s=1.0, identity="email_param", daily_budget=100_000,
                   status_kinds={422: Kind.CONFIG, 410: Kind.CONFIG},
                   note="100k/day; the email is the only credential (redacted); 422/410 abort the run; 404 bodies are HTML"),
        HostPolicy("api.openalex.org", min_interval_s=0.2,
                   note="1,000 credits/day keyless; URL <= 8,190 B; budget from X-RateLimit-* (litpipe.openalex registers)"),
        HostPolicy("pmc.ncbi.nlm.nih.gov", min_interval_s=1.0, transport="urllib", identity="ncbi",
                   offpeak=NCBI_OFFPEAK, retry=_NO_429_RETRY,
                   note="idconv only (/articles/ prohibited); requests gets 403, urllib 200; since 2026-09-30 a "
                        "sustained 429 without Retry-After: a 429 refuses the host for the run, never retried"),
        HostPolicy("eutils.ncbi.nlm.nih.gov", min_interval_s=0.4, identity="ncbi", offpeak=NCBI_OFFPEAK,
                   note="3 rps unkeyed (X-Ratelimit-Limit: 3), 10 with a key; idconv fallback (W2-A1)"),
        HostPolicy("pmc-oa-opendata.s3.amazonaws.com", min_interval_s=0.25, identity="anonymous",
                   note="no stated limit; verify md5 against ?md5=, not the ETag"),
        HostPolicy("www.ncbi.nlm.nih.gov", min_interval_s=0.5, identity="ncbi", offpeak=NCBI_OFFPEAK,
                   note="BioC API; 'absent' is 200 text/html; /pmc/articles/ prohibited"),
        HostPolicy("www.ebi.ac.uk", min_interval_s=1.0,
                   note="Europe PMC REST (/europepmc/webservices/rest/) only; fullTextXML 500 = not available; "
                        "30 s timeout; europepmc.org website pages prohibited"),
        HostPolicy("export.arxiv.org", min_interval_s=3.5, retry=_NO_429_RETRY, refusal_persistence="manual",
                   note="1 req / 3 s, one connection, all machines; 406/429 refuse until cleared (DEC-09); "
                        "the http base adds a 301 with Retry-After: 0"),
        HostPolicy("arxiv.org", min_interval_s=15.0, retry=_NO_429_RETRY, refusal_persistence="manual",
                   note="/pdf/ only when hosts.arxiv_pdf_allowed (robots Crawl-delay: 15)"),
        HostPolicy("www.arxiv.org", min_interval_s=15.0, retry=_NO_429_RETRY, refusal_persistence="manual",
                   note="as arxiv.org"),
        HostPolicy("api.osf.io", min_interval_s=36.0, daily_budget=2_400, redirect_allow=_OSF_HOPS,
                   note="100/h anonymous (paced to 1 per 36 s), 10k/day with a token; no rate headers"),
        HostPolicy("osf.io", min_interval_s=1.0, redirect_allow=_OSF_HOPS, note="download hop 1 of 3"),
        HostPolicy(".osf.io", min_interval_s=1.0, redirect_allow=_OSF_HOPS, note="files.*.osf.io download hop"),
        HostPolicy("storage.googleapis.com", min_interval_s=1.0, identity="anonymous", note="OSF file blobs"),
        HostPolicy("sportrxiv.org", min_interval_s=3.0,
                   note="PKP OPS OAI; at most 1 harvest/day; ToS bars bulk republication"),
        HostPolicy("api.biorxiv.org", min_interval_s=1.0, note="type from the body; /details empty 09-24 to 09-28"),
        HostPolicy("www.biorxiv.org", min_interval_s=5.0, note="bot-walled; only when hosts.biorxiv_pdf_allowed"),
        HostPolicy("www.medrxiv.org", min_interval_s=5.0, note="bot-walled; only when hosts.biorxiv_pdf_allowed"),
        HostPolicy("api.semanticscholar.org", min_interval_s=6.5, retry_after_cap_s=600.0,
                   note="unkeyed 6.5 s (measured in a consumer project); litpipe.s2 registers 1.1 s when keyed; x-api-key"),
    ]


PROHIBITED = (
    ProhibitedRule("europepmc.org", "/", "Europe PMC website pages, including ?pdf=render"),
    ProhibitedRule("www.europepmc.org", "/", "Europe PMC website pages, including ?pdf=render"),
    ProhibitedRule("pmc.ncbi.nlm.nih.gov", "/articles/", "PMC article pages (NCBI bars scripting its web pages)"),
    ProhibitedRule("cdn.ncbi.nlm.nih.gov", "/", "PMC CDN blobs"),
    ProhibitedRule("www.ncbi.nlm.nih.gov", "/pmc/articles/", "legacy PMC article pages"),
    ProhibitedRule("www.biorxiv.org", "/", "bot-walled", unless_switch="biorxiv_pdf_allowed"),
    ProhibitedRule("biorxiv.org", "/", "bot-walled", unless_switch="biorxiv_pdf_allowed"),
    ProhibitedRule("www.medrxiv.org", "/", "bot-walled", unless_switch="biorxiv_pdf_allowed"),
    ProhibitedRule("medrxiv.org", "/", "bot-walled", unless_switch="biorxiv_pdf_allowed"),
    ProhibitedRule("arxiv.org", "/pdf/", "arXiv PDFs (not the export API)", unless_switch="arxiv_pdf_allowed"),
    ProhibitedRule("www.arxiv.org", "/pdf/", "arXiv PDFs (not the export API)", unless_switch="arxiv_pdf_allowed"),
)

DEFAULT = HostPolicy("", min_interval_s=2.0, concurrency=1, identity="download", redirect_allow=(ANY_HOST,),
                     known=False, note="unknown host: default policy (2 s, concurrency 1)")

_TABLE: dict[str, HostPolicy] = {}


def reset():
    """Restore the built-in rows (tests; a registered row is otherwise process-wide)."""
    _TABLE.clear()
    for p in _rows():
        _TABLE[p.host] = p


def register(policy: HostPolicy) -> HostPolicy:
    """Add or replace the row for policy.host (a leading "." makes it a suffix row)."""
    if not isinstance(policy, HostPolicy):
        raise TypeError("register() takes a HostPolicy")
    if not policy.host:
        raise ValueError("register() needs a host")
    _TABLE[policy.host] = policy
    return policy


def rows() -> dict[str, HostPolicy]:
    return dict(_TABLE)


def host_of(url_or_host) -> str:
    s = (url_or_host or "").strip()
    if "://" in s:
        return (urlsplit(s).hostname or "").lower()
    return s.split("/", 1)[0].split(":", 1)[0].lower()


def policy(url_or_host) -> HostPolicy:
    """The policy for a URL or bare host name, carrying the real host name. (arxiv.org is paced at
    15 s whether or not the /pdf/ switch is on; the switch only lifts the prohibition.)"""
    h = host_of(url_or_host)
    row = _TABLE.get(h)
    if row is None:
        suffixes = [k for k in _TABLE if k.startswith(".") and h.endswith(k)]
        row = _TABLE[max(suffixes, key=len)] if suffixes else DEFAULT
    if row.host != h:
        row = dataclasses.replace(row, host=h)
    return row


def prohibited(url, cfg=None) -> ProhibitedRule | None:
    """The first never-automated rule matching `url`, or None. Switch-gated rules read
    config.hosts(cfg) (projects.json `hosts`; both switches default off)."""
    parts = urlsplit(url)
    h = (parts.hostname or "").lower()
    path = parts.path or "/"
    switches = None
    for rule in PROHIBITED:
        if h != rule.host or not path.startswith(rule.path_prefix):
            continue
        if rule.unless_switch:
            if switches is None:
                switches = config.hosts(cfg)
            if switches.get(rule.unless_switch):
                continue
        return rule
    return None


def check(url, cfg=None) -> None:
    """Raise ProhibitedHost when `url` is on the never-automated list."""
    rule = prohibited(url, cfg)
    if rule is not None:
        raise ProhibitedHost(url, rule)


reset()
