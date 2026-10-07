"""litpipe.canaries: health checks and the health report for the scheduled runner (dispatch W4-B;
refactor scope section 4, the canary table; plan section 5, the floors).

From 2026-09-14 to 09-25 the PMC stage fetched almost nothing and nobody noticed for 11 days: the
403s were recorded as empty results. A cheap request against a known answer catches that kind of
change the same night; local checks over what a run wrote catch the rest. Every check is one
`Outcome`; `report()` turns them into JSON and `summary()` into at most ten lines.

API (in-process; like litpipe.preflight.run, the exception to the `run(**kwargs) -> dict` rule:
the runner never runs canaries through its stage shim):
  run(profile, *, phase="all", context=None, state=None, request=None, cfg=None, now=None)
      -> list[Outcome]. `profile` is every_run | daily | weekly | monthly, cumulative (daily runs
      the every_run checks too, and so on up). phase "network" runs the network checks and their
      refusals, "local" the 0-request checks over `context`, "all" (the CLI default) both.
  report(outcomes, *, run_id=None, profile=None, started=None) -> dict
      {"run_id", "profile", "started", "checks": [...], "refused_hosts": [...], "requests": n};
      `checks` carries both phases, each entry with its `phase`. Every value passes through
      litpipe.ledger.redact_obj.
  summary(report) -> str, at most SUMMARY_LINES lines: "HEALTH PASS" or "HEALTH ALARM" with the
      counts, then ALARM (and ERROR) lines, as many as fit, then "+N more ALARM".
  planned_requests(profile, *, context=None, state=None, cfg=None) -> {check id: requests}
  main(argv=None): python -m litpipe.canaries --profile P [--phase network|local|all]
      [--json PATH] [--dry-run]. Exit 0 HEALTH PASS; 2 HEALTH ALARM (after a final
      `[step-summary] {json}` line with reasons, aborted, transport_failures); 1 a usage or
      config error. --dry-run lists the checks and their planned requests and sends nothing.

Each Outcome: `host` the checked host (a network check's origin host; a project key or "ledger",
"local", "portfolio.duckdb" for a local check), `detail` the check id, `attempts` the requests
it sent (re-check included), `status` the deciding HTTP status, `kind` OK for PASS, the network
outcome's Kind for a failed request (ERROR for a wrong answer on a 2xx, or for a local ALARM),
SKIPPED for a skip, and `payload` {"id", "phase", "cadence", "target", "expected", "observed",
"status", "action"} with status PASS | ALARM | SKIPPED | ERROR (ERROR: the check itself raised)
and action "refused for the run" | "none".

Call sites (W4-A codes against these):
  1. Network phase, after preflight and before the sweep:
         outs = canaries.run(profile, phase="network", context=ctx, cfg=cfg)
     It reads only ctx["projects"][i]["key"] and ["sources"] (is arXiv scheduled?). A confirmed
     source failure refuses that host in litpipe.state for the run, so the stages see a refused
     source and their rows route TRANSIENT (dispatch 0.5).
  2. Local phase, after route and index and before the run summary, over what the run wrote:
         outs += canaries.run(profile, phase="local", context=ctx, cfg=cfg)
         rep = canaries.report(outs, run_id=ctx["run_id"], profile=profile, started=t0)
         print(canaries.summary(rep))

Context contract (what W4-A passes; every key optional):
  {"run_id":  the litpipe.state run id (`%Y%m%dT%H%M%SZ-<kind>-<pid>-<hex>`); filters ledger lines,
   "since":   ISO 8601 start of the run (a trailing Z is fine); files are "new" when their mtime is
              at or after it,
   "db_path": portfolio.duckdb (default config.db_dir(cfg) / "portfolio.duckdb"),
   "projects": [{"key":            registry key,
                 "root":           the project directory (lit_util.project_root),
                 "lib_dir":        the library (lit_util.lib_paths(...)[1]); relative: under root,
                 "sources":        DEC-31 fetch sources (default config.sources(key)),
                 "artifact_dir":   sweep's --artifact-dir (default root); relative: under root,
                 "sweep_run_ids":  sweep's own run ids this run (`YYYY-MM-DD[.N]`, printed as
                                   `[sweep] run_id=<id> project=<key>`; NOT the state run id),
                 "stages":         artifact stage names every queue swept for it this run keeps,
                                   as in the file names (unpaywall, pmc, preprint, residual,
                                   report, processed, routing); each is checked per (tag, run id).
                                   `normalized` is ignored: sweep deletes it when a queue retires.
                                   A stage some queue skipped (preprint not_needed) would alarm
                                   as lost, so pass only the stages common to the run's queues}]}
  With no context (the CLI): `since` is the start of today UTC, the ledger is today's file (every
  line since `since`), the projects are every active one in the registry (config.load()), their
  run ids those of the artifacts (in the project dir or a direct subdirectory) whose run id starts
  with today's date, and lost-artifact checks are SKIPPED (no stage list without a runner).

Network checks (section 4 rows except preflight's Unpaywall and Crossref-pool checks; targets and
pass values in TARGETS, sources cited there). Each request goes through litpipe.net with
purpose="canary:<id>", no 5xx retries (only a 429 keeps the host row's own retry; W4a verifier
K-1) and the resolved state, so identity, pacing, refusals,
the ledger and redaction come with it. A canary is a request against a known answer: the URL
constants and response predicates are the stage modules' own (imported at call time).

Verdicts and refusals (dispatch amendment 3):
  * a 403, or a status the host row refuses on (406, a final 429), counts at once (litpipe.net has
    already refused the host it came from);
  * a 5xx, an empty 2xx or a transport failure is re-checked once after RECHECK_S (60 s on CLOCK,
    else net.CLOCK) by running the whole check again; only a second failure is confirmed. A status
    section 4 names as the pass value (the Europe PMC author-manuscript 500) is a PASS;
  * a confirmed failure on the check's ORIGIN host calls
    state.refuse(origin, "canary <id>: <observed>", persistence="run"); a failure on a redirect
    target (storage.googleapis.com, a content-negotiation target such as api.crossref.org or
    data.crosscite.org) or on a second host of the check (the OSF download link) is ALARM only.
    litpipe.net itself still refuses a host that answers 403/406/final 429, as for every stage;
  * a wrong answer on a 2xx (drift), a SKIPPED check and an ERROR refuse nothing; refuse() never
    downgrades a manual refusal (litpipe.state), and this module does not work around that.
  A check whose origin host is already refused is SKIPPED and sends nothing. arXiv is SKIPPED with
  nothing sent while any of export.arxiv.org, arxiv.org, www.arxiv.org is refused (manual or run)
  or when no scheduled project's sources include "arxiv"; idconv while pmc.ncbi.nlm.nih.gov is
  refused; OpenAlex without OPENALEX_API_KEY.

Cost (planned requests; a passing run sends exactly these, --dry-run reports them):
  every_run 1 (idconv; 0 while refused);
  daily adds 17: Europe PMC 2, efetch 1, S3 3, BioC 1, bioRxiv 2, OSF 4 (hops included),
      SportRxiv 1, arXiv 1 (0 while refused or not scheduled), DataCite 1, doiRA 1;
  weekly adds 9: OpenAlex 1 (0 without a key), Crossref references 1, content negotiation 4,
      Crossref alias 2, encoded SICI 1. Amendment 6 counted "alias and SICI 2"; the alias costs 2
      by itself (Crossref answers an aliased DOI with a 301 to the prime record, live since
      2025-02-25, and litpipe.net follows it; W2-E1 measured attempts 2 on 2026-09-30), so the
      weekly total is 9, not 8;
  monthly adds 0.
  Worst case: a check stops at its first failed request, and litpipe.net retries a transport
  failure at most TRANSPORT_RETRIES (2) times, which no call can override. So one round sends at
  most count + 2 and a failing check (two rounds) at most 2 * (count + 2): worst_case(check id).
  Not in that bound: a 429 on a host whose row retries 429 can cost up to its max_retries (6)
  more requests, and each request can follow up to net.MAX_REDIRECTS (5) unplanned hops (a
  self-redirect then a dropped connection sent 11 against a worst case of 6).

Local checks (0 requests, read only; never a library, queue or portfolio.duckdb write):
  first_attempts   per host, from the run's ledger lines with attempt 1 and hop 0, excluding
                   purpose "canary:*" and decision not_sent / prohibited: a status histogram and
                   the typed-outcome rates. ALARM when REFUSED (kind REFUSED other than a blocked
                   redirect, or a 403/406/429 that was retried) is 5 % or more on a host with 20
                   or more first attempts, and on any Europe PMC (www.ebi.ac.uk) 403. The ledger has
                   no project field: per host only.
  yield_pmc        PMC downloaded / rows with a PMCID (rows the stage skipped as already held are
                   not counted): ALARM below 0.50 when there are 20 or more such rows.
  epmc_403         any Europe PMC 403 in the PMC report (a typed epmc route with first_status 403,
                   or a legacy `europepmc/HTTP_403`): ALARM.
  unpaywall_403    HTTP_403 share of the OA download attempts sent (the report's `attempts` tokens;
                   HOST_REFUSED, PROHIBITED and DEFERRED were not sent): ALARM above 0.45 with at
                   least UNPAYWALL_MIN_ATTEMPTS (20) attempts (the plan sets no minimum; 20 matches
                   the PMC and per-host rules).
  yield_mdpi_bmc   downloaded share of the OA rows under 10.3390/ (MDPI) and 10.1186/ (BMC): ALARM
                   below 0.60 with at least MDPI_BMC_MIN_ROWS (10) rows (the minimum chosen here:
                   both publishers are fully OA, so 10 rows already make a share below 0.60 a
                   route problem, not chance).
  preprint_outcomes  only for a project whose sources include a preprint server (DEC-31): the
                   typed outcome counts of its preprint report (informational; plan section 5 sets
                   no preprint floor).
  mismatch_growth  any file under <lib>/_mismatch/ newer than `since` (no stage moves files since
                   W2a): ALARM.
  markup           .ris text lines and .fulltext.json title/subtitle/journal/abstract/authors in
                   library files written since `since`, through litpipe.text strip_tags and
                   unescape: the rate of files with a leftover tag or entity; ALARM above 0.
  email            the run's report CSVs (written since `since`; plan section 5 counts 90 legacy
                   hits that are W5-B's to scrub) and the run's ledger lines: a plain grep for
                   `email=` and `mailto:` (litpipe.ledger.redact replaces both whole, so any hit is
                   a leak) and for the configured address. ALARM names the file and line, never
                   the value.
  doi_fixtures     DOI_FIXTURES through litpipe.doi.normalise / normalise_structured: ALARM on a
                   mismatch.
  index_freshness  index_runs.finished_at per project against the newest top-level PDF, .ris or
                   .fulltext.json in its library: ALARM when the library is newer by more than a
                   day. The DB is opened read-only with tries=1 and closed before returning; a
                   missing DB or table, a lock or an open error is SKIPPED.
  lost_artifacts   every stage in `stages` has lit_pull_queue[.<tag>].<sweep_run_id>.<stage>.csv in
                   artifact_dir or the project root: ALARM on a missing one.

State: resolved once per run(): an explicit `state=`, else net.STATE, else litpipe.state, and
passed to every net.request. "manual" versus "run" is read from litpipe.state.status() only when
the real state is in use (a stand-in state's is_refused is read as refused). A state file that
does not exist yet is never created to answer "is this host refused?".
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

from litpipe import config, hosts, ledger
from litpipe.outcomes import Kind, Outcome

PROFILES = ("every_run", "daily", "weekly", "monthly")
_RANK = {p: i for i, p in enumerate(PROFILES)}
PHASES = ("network", "local", "all")

PASS, ALARM, SKIPPED, ERROR = "PASS", "ALARM", "SKIPPED", "ERROR"
ACTION_REFUSED = "refused for the run"
ACTION_NONE = "none"

RECHECK_S = 60.0                 # wait before the one re-check of a 5xx, empty 2xx or transport failure
MAX_ROUNDS = 2                   # the check and its re-check
TRANSPORT_RETRIES = hosts.RetryPolicy().transport_retries   # litpipe.net's, not overridable per call
SUMMARY_LINES = 10
CLOCK = None                     # tests may set a FakeClock; default net.CLOCK at call time

REFUSED_ALARM_SHARE = 0.05       # first-attempt REFUSED share per host (refactor scope section 4)
REFUSED_MIN_ATTEMPTS = 20
PMC_FLOOR, PMC_MIN_ROWS = 0.50, 20               # plan section 5
UNPAYWALL_403_CEILING, UNPAYWALL_MIN_ATTEMPTS = 0.45, 20
MDPI_BMC_FLOOR, MDPI_BMC_MIN_ROWS = 0.60, 10
MDPI_BMC_PREFIXES = ("10.3390/", "10.1186/")
INDEX_STALE_S = 86400.0          # the library may be newer than index_runs by at most a day
_NOT_SENT_TOKENS = frozenset({"HOST_REFUSED", "PROHIBITED", "DEFERRED"})
_REDIRECTS = frozenset({301, 302, 303, 307, 308})
_TRANSIENT_STAGES = frozenset({"normalized"})    # sweep deletes it when the queue retires (sweep.py:1188)
ARXIV_HOSTS = ("export.arxiv.org", "arxiv.org", "www.arxiv.org")
EPMC_HOST = "www.ebi.ac.uk"
PREPRINT_SOURCES = frozenset({"europepmc_preprints", "biorxiv", "medrxiv", "osf", "sportrxiv", "arxiv"})

# ------------------------------------------------------------------------------ targets
# Known answers. Recorded 2026-09-25 to 10-05 (sources per entry) and re-probed 2026-10-06 (the
# trimmed answers are tests/fixtures/W4-B/). Growing counts are floors: the lower of the
# 2026-09-27 value and the 2026-10-06 probe, never an exact value.
IDCONV_DOI, IDCONV_PMCID = "10.7717/peerj.14493", "PMC9817969"   # V1 C2 (raw/P7_*); W0 idconv_drift
EPMC_AM, EPMC_OA = "PMC11903078", "PMC9817969"                   # V1 C1 (raw/P1c_*, P3_*)
# efetch db=pmc (V1 C4, raw/P13_*): (pmc-prop-open-access, pmc-prop-manuscript, no-XML comment)
EFETCH_EXPECT = {"PMC9817969": ("yes", "no", False), "PMC11903078": ("no", "yes", False),
                 "PMC4806772": ("no", "no", True)}
EFETCH_NO_XML = "does not allow downloading of the full text in XML form"
S3_PMCID = "PMC9817969"                                          # V1 C3 (raw/P1a_*)
BIOC_AM = "PMC7983342"                                           # V1 C6 (raw/P1d_bioc_PMC7983342)
BIORXIV_DETAILS_TARGET = ("medrxiv", "10.1101/2020.09.09.20191205")   # V4 C1 raw/001, W2-C B3 (10-05)
BIORXIV_PUBS_TARGET = ("medrxiv", "10.1101/2021.04.29.21256344", "10.1371/journal.pone.0256482")  # V4 raw/042
OSF_FILE, OSF_BYTES = "60a530d48b826b010e8c03c8", 671_832       # V4 C3 raw/023-026, W2-C O2/O3 (10-05)
ARXIV_ID = "1511.07289"                                          # V4 C5 (never sent by a probe)
DATACITE_DOI = "10.48550/arxiv.2605.29559"                       # V3 C5 (raw/s50, s30)
DOIRA_EXPECT = {"10.1152": "Crossref", "10.48550": "DataCite", "10.3305": "mEDRA", "10.2903": "OP"}  # V3 C7 raw/s20
# OpenAlex referenced_works_count: 96 / 56 / 41 on 2026-09-27 (V2 canary 2), 96 / 56 / 40 on
# 2026-10-06 (the third seed lost one reference), so the floors are 96 / 56 / 40. The keyed list
# call cost X-RateLimit-Cost-USD 0.0001 both times (X-RateLimit-Credits-Used 1 on 2026-10-06).
OPENALEX_FLOORS = {"10.1152/japplphysiol.00775.2024": 96, "10.1249/mss.0000000000002991": 56,
                   "10.1123/ijspp.2022-0026": 40}
OPENALEX_COST_USD = 0.0001
# Crossref len(reference): 101 / 39 on 2026-09-27 (V2 canary 3) and on 2026-10-06
CROSSREF_REF_FLOORS = {"10.1152/japplphysiol.00775.2024": 101, "10.1123/ijspp.2022-0026": 39}
CN_DOIS = ("10.1152/japplphysiol.00775.2024", "10.48550/arxiv.2605.29559")   # V3 C8 raw/s30 (Crossref, DataCite)
ALIAS_DOI, ALIAS_PRIME = "10.1515/9789882204508-009", "10.5790/hongkong/9789888528011.003.0007"  # V3 C4 raw/s50
# The doc SICI DOI (V3 C3, raw/s10): Crossref holds it with the `#`. litpipe.doi reads `#` as a URL
# fragment (doi.py module docstring, "Deliberate deviations"), so normalise() gives None and
# encode_path raises; migrate_closed_to_md.doi_url falls back to percent-encoding it whole (%23).
SICI_HASH_DOI = "10.1002/(SICI)1521-3951(199911)216:1<135::AID-PSSB135>3.0.CO;2-#"

# DOI fixtures (input, function, expected) as the normaliser is built (W1-B, W3a verifier G).
# The deliberate ones are cited from litpipe/doi.py's docstrings.
DOI_FIXTURES = (
    ("https://doi.org/10.1152/JAPPLPHYSIOL.00775.2024", "normalise", "10.1152/japplphysiol.00775.2024"),
    # a trailing period is sentence punctuation (candidates step 2, `_trim`)
    ("doi:10.1371/journal.pone.0012033.", "normalise", "10.1371/journal.pone.0012033"),
    # SICI DOIs keep their angle brackets (the Crossref class plus `<>`, "Deliberate deviations")
    ("10.1002/(SICI)1097-4636(199709)36:3<385::AID-JBM12>3.0.CO;2-E", "normalise",
     "10.1002/(sici)1097-4636(199709)36:3<385::aid-jbm12>3.0.co;2-e"),
    # a `;2-#` SICI DOI: `#` is a fragment, so nothing is left ("Deliberate deviations"; W5 question)
    (SICI_HASH_DOI, "normalise", None),
    # a letter-tail DOI: free text shortens it, a structured field keeps it (normalise_structured)
    ("10.1088/2053-1591/acdecd", "normalise", "10.1088/2053-1591"),
    ("10.1088/2053-1591/acdecd", "normalise_structured", "10.1088/2053-1591/acdecd"),
    ("10.1056/nejmc1113675#sa3", "normalise", "10.1056/nejmc1113675"),     # fragment cut (step 5)
    ("10.1145/nnnnnnn.nnnnnnn", "normalise", None),                         # placeholder (is_placeholder)
    ("10.1016/j.amepre", "normalise", None),                                # truncated journal code
    ("10.31234/osf.io/kbyhm", "normalise", "10.31234/osf.io/kbyhm"),         # nested path, 5-digit registrant
    ("10.1152/japplphysiol.00775.2024.author", "normalise", "10.1152/japplphysiol.00775.2024"),  # running head
)


# ------------------------------------------------------------------------------ small helpers
def _clock():
    if CLOCK is not None:
        return CLOCK
    from litpipe import net
    return net.CLOCK


def _resolve_state(state):
    """The shared rule: an explicit state, else net.STATE, else litpipe.state."""
    if state is not None:
        return state
    from litpipe import net
    if net.STATE is not None:
        return net.STATE
    import litpipe.state as st
    return st


def _is_real_state(st) -> bool:
    import litpipe.state as real
    return st is real


def _refusal(st, host) -> str | None:
    """None when `host` is not refused, else "manual", "run" or "refused" (a stand-in state).
    The real state file is never created just to answer this."""
    if _is_real_state(st):
        try:
            if not st.db_path(create=False).exists():
                return None
        except Exception:      # noqa: BLE001 - no readable state: nothing is refused yet
            return None
    try:
        if not st.is_refused(host):
            return None
    except Exception:          # noqa: BLE001 - an unreadable state answers nothing
        return None
    if _is_real_state(st):
        try:
            for h in st.status().get("hosts", []):
                if h.get("host") == host and h.get("refused"):
                    return str(h["refused"])
        except Exception:      # noqa: BLE001
            pass
    return "refused"


def _now(now) -> datetime:
    if now is None:
        return datetime.now(timezone.utc)
    if isinstance(now, (int, float)):
        return datetime.fromtimestamp(now, timezone.utc)
    return now if now.tzinfo else now.replace(tzinfo=timezone.utc)


def _parse_iso(s) -> datetime | None:
    if not s:
        return None
    if isinstance(s, datetime):
        return s if s.tzinfo else s.replace(tzinfo=timezone.utc)
    try:
        d = datetime.fromisoformat(str(s).strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


def _short(s, n=160) -> str:
    s = " ".join(str(s).split())
    return s if len(s) <= n else s[:n - 3] + "..."


# ------------------------------------------------------------------------------ the check table
@dataclass(frozen=True)
class Check:
    id: str
    cadence: str                  # every_run | daily | weekly | monthly
    phase: str                    # network | local
    requests: int                 # planned requests of a passing run (0 for a local check)
    target: str
    expected: str
    fn: Callable
    origin: Callable | None = None     # network: () -> the URL whose host is the origin host
    skip: Callable | None = None       # network: (rctx) -> a reason to send nothing, or None


@dataclass
class _V:
    """One round's verdict: pass | drift | fail | refused | skip | error."""
    verdict: str
    observed: str
    host: str = ""                # where the deciding response came from
    kind: Kind | None = None
    status: int | None = None


class _Probe:
    """One round of one network check: every request through `request` with the canary's purpose,
    no status retries and the resolved state; counts what was sent."""

    def __init__(self, chk, rctx):
        self.chk, self.rctx = chk, rctx
        self.sent = 0
        self.elapsed_ms = 0

    def send(self, method, url, **kw):
        o = self.rctx.request(method, url, purpose=f"canary:{self.chk.id}",
                              retry_statuses=_retry_statuses(url),
                              state=self.rctx.state, cfg=self.rctx.cfg, **kw)
        self.sent += int(o.attempts or 0)
        self.elapsed_ms += int(o.elapsed_ms or 0)
        return o

    def get(self, url, **kw):
        return self.send("GET", url, **kw)


def _retry_statuses(url) -> tuple:
    """No 5xx retries (the canary re-checks a 5xx once after RECHECK_S), but a 429 keeps the origin
    host row's own handling: retried with its Retry-After where the row retries 429, the host
    deferred when Retry-After passes the inline cap, final where the row says so (pmc.ncbi.nlm.nih.gov,
    BioC, arXiv). With retry_statuses=() the FIRST 429 was final, and litpipe.net refuses a host on
    a final 429: one rate-limit answer took the source (or a redirect target) off for the run."""
    return (429,) if 429 in hosts.policy(url).retry.statuses else ()


def _resp(o):
    p = getattr(o, "payload", None)
    return p if (hasattr(p, "status") and hasattr(p, "headers")) else None


def _content(o) -> bytes:
    r = _resp(o)
    return (r.content or b"") if r is not None else b""


def _json(o):
    try:
        return json.loads(_content(o).decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None


def _header(o, name):
    r = _resp(o)
    if r is None or r.headers is None:
        return None
    return r.headers.get(name)


def _refuse_statuses(host) -> frozenset:
    return frozenset({403}) | hosts.policy(host).refuse_host_statuses


def _failed(o, what, *, head=False) -> _V | None:
    """The generic failures every check shares; None when the host answered and the check's own
    predicate decides."""
    st, host = o.status, o.host
    if o.kind is Kind.TRANSPORT:
        return _V("fail", f"{what}: transport failure ({_short(o.detail, 100)})", host, o.kind, None)
    if st is None:
        if o.kind in (Kind.DEFERRED, Kind.REFUSED) and not o.attempts:
            return _V("skip", f"{what}: not sent ({o.kind}: {_short(o.detail, 100)})", host, o.kind, None)
        return _V("fail", f"{what}: no response ({o.kind}: {_short(o.detail, 100)})", host, o.kind, None)
    if st in _refuse_statuses(host or "x"):
        return _V("refused", f"{what}: HTTP {st} from {host}", host, o.kind, st)
    if st >= 500:
        return _V("fail", f"{what}: HTTP {st} from {host}", host, o.kind, st)
    if 200 <= st < 300 and not head:
        r = _resp(o)
        if r is not None and r.truncated:
            return _V("drift", f"{what}: body over the size cap", host, Kind.ERROR, st)
        if not _content(o).strip():
            return _V("fail", f"{what}: empty {st} from {host}", host, o.kind, st)
    return None


def _ok(observed, o=None) -> _V:
    return _V("pass", observed, getattr(o, "host", ""), Kind.OK, getattr(o, "status", None))


def _drift(observed, o) -> _V:
    kind = o.kind if (o is not None and o.kind is not Kind.OK) else Kind.ERROR
    return _V("drift", observed, getattr(o, "host", ""), kind, getattr(o, "status", None))


# ------------------------------------------------------------------------------ network checks
def _c_idconv(p, rctx):
    import lit_net
    o = p.get(lit_net.IDCONV, params={"ids": IDCONV_DOI, "idtype": "doi", "format": "json"})
    v = _failed(o, "idconv")
    if v:
        return v
    j = _json(o)
    recs = (j.get("records") or []) if isinstance(j, dict) else []
    rec = recs[0] if recs and isinstance(recs[0], dict) else {}
    pmcid = rec.get("pmcid")
    if pmcid == IDCONV_PMCID and rec.get("live") is not False:
        return _ok(f"{IDCONV_DOI} -> {pmcid}", o)
    if pmcid == IDCONV_PMCID:
        return _drift(f"{IDCONV_DOI} -> {pmcid} with live:false", o)
    return _drift(f"{IDCONV_DOI} -> {pmcid or 'no pmcid'} (HTTP {o.status})", o)


def _epmc_not_available(o) -> bool:
    """The stored signature of fullTextXML's "not in the OA subset" answer (since 2026-09-16):
    HTTP 500 with a JSON body {"timestamp", "status": 500, "error", "path": ".../fullTextXML"}."""
    j = _json(o)
    return (o.status == 500 and isinstance(j, dict) and j.get("status") == 500 and "error" in j
            and str(j.get("path") or "").endswith("/fullTextXML"))


def _c_epmc(p, rctx):
    import jats_to_text as J
    am = p.get(J.EPMC_JATS_XML.format(pmcid=EPMC_AM))
    drift = None
    if am.status == 500 and not _epmc_not_available(am):
        drift = f"AM {EPMC_AM}: HTTP 500 without the stored JSON signature"
    elif am.status != 500:
        v = _failed(am, f"AM {EPMC_AM}")
        if v:
            return v
        drift = f"AM {EPMC_AM}: HTTP {am.status}, expected the not-available 500"
    oa = p.get(J.EPMC_JATS_XML.format(pmcid=EPMC_OA))
    v = _failed(oa, f"OA {EPMC_OA}")
    if v:
        return v
    if J._expect_xml(_resp(oa)) is not None:
        return _drift(f"OA {EPMC_OA}: HTTP {oa.status} body is not XML", oa)
    if drift:
        return _drift(drift, am)
    return _ok(f"AM {EPMC_AM} 500 (signature), OA {EPMC_OA} 200 XML", oa)


def _c_efetch(p, rctx):
    import lit_net
    import pmc_fetch
    ids = ",".join(k[3:] for k in EFETCH_EXPECT)
    o = p.get(f"{lit_net.EUTILS}/efetch.fcgi", params={"db": "pmc", "id": ids, "retmode": "xml"},
              max_bytes=pmc_fetch.EFETCH_MAX_BYTES, timeout=(10, 120))
    v = _failed(o, "efetch")
    if v:
        return v
    text = _content(o).decode("utf-8", "replace")
    parsed = pmc_fetch.parse_efetch(text)
    chunks = re.split(r"(?=<article[\s>])", text)[1:]
    got, bad = [], []
    for pmcid, (want_oa, want_am, want_comment) in EFETCH_EXPECT.items():
        c = parsed.get(pmcid)
        if c is None:
            bad.append(f"{pmcid} absent")
            continue
        oa = c.flags.get("pmc-prop-open-access", "no")
        am = c.flags.get("pmc-prop-manuscript", "no")
        num = pmcid[3:]
        chunk = next((ch for ch in chunks if re.search(
            r'<article-id pub-id-type="pmc(?:id|aid)">\s*(?:PMC)?' + num + r"\b", ch)), "")
        comment = EFETCH_NO_XML in chunk
        got.append(f"{pmcid} {oa}/{am}{' comment' if comment else ''}")
        if (oa, am, comment) != (want_oa, want_am, want_comment):
            bad.append(f"{pmcid} open-access {oa} manuscript {am} comment {comment}")
    if bad:
        return _drift("efetch flags: " + "; ".join(bad), o)
    return _ok("efetch: " + "; ".join(got), o)


def _c_s3(p, rctx):
    import pmc_fetch
    lst = p.get(f"{pmc_fetch.S3_BASE}/", params={"list-type": "2", "prefix": f"{S3_PMCID}.",
                                                 "delimiter": "/"})
    v = _failed(lst, "S3 list")
    if v:
        return v
    if pmc_fetch._expect_s3_list(_resp(lst)) is not None:
        return _drift(f"S3 list: HTTP {lst.status} is not a ListBucketResult", lst)
    text = _content(lst).decode("utf-8", "replace")
    vs = []
    for pfx in re.findall(r"<CommonPrefixes>\s*<Prefix>([^<]+?)/?</Prefix>", text):
        m = re.fullmatch(r"(PMC\d+)\.(\d+)", pfx.strip())
        if m and m.group(1).upper() == S3_PMCID:
            vs.append((int(m.group(2)), f"{m.group(1)}.{m.group(2)}"))
    if not vs:
        return _drift(f"S3 list: no version of {S3_PMCID}", lst)
    latest = max(vs)[1]
    meta = p.get(f"{pmc_fetch.S3_BASE}/metadata/{latest}.json")
    v = _failed(meta, "S3 metadata")
    if v:
        return v
    j = _json(meta)
    pdf = j.get("pdf_url") if isinstance(j, dict) else None
    url = pmc_fetch.s3_https(pdf)[0] if pdf else None
    if not url:
        return _drift(f"S3 metadata {latest}: no pdf_url", meta)
    head = p.send("HEAD", url)
    v = _failed(head, "S3 HEAD pdf_url", head=True)
    if v:
        return v
    try:
        size = int(_header(head, "Content-Length") or 0)
    except ValueError:
        size = 0
    if head.status != 200 or size <= 0:
        return _drift(f"S3 HEAD pdf_url: HTTP {head.status}, Content-Length {size}", head)
    return _ok(f"S3 {latest}: list 200, pdf_url present, HEAD 200 ({size} B)", head)


def _c_bioc(p, rctx):
    import jats_to_text as J
    o = p.get(J.BIOC_JSON.format(pmcid=BIOC_AM, encoding="unicode"))
    v = _failed(o, "BioC")
    if v:
        return v
    r = _resp(o)
    if J._is_bioc_absent(r):
        return _drift(f"BioC {BIOC_AM}: 'absent' inside a {o.status} ({r.content_type or 'no type'})", o)
    try:
        doc = J._bioc_document(json.loads(r.text))
    except (ValueError, KeyError, IndexError, TypeError) as e:
        return _drift(f"BioC {BIOC_AM}: not a BioC document ({_short(e, 80)})", o)
    n = len(doc.get("passages") or [])
    if o.status != 200 or n < 1:
        return _drift(f"BioC {BIOC_AM}: HTTP {o.status}, {n} passages", o)
    return _ok(f"BioC {BIOC_AM}: 200 JSON, {n} passages", o)


def _biorxiv_base():
    import preprint_fetch
    return preprint_fetch.BIORXIV_DETAILS.split("/details/", 1)[0]


def _c_biorxiv(p, rctx):
    import preprint_fetch
    from litpipe import doi as _doi
    server, ddoi = BIORXIV_DETAILS_TARGET
    d = p.get(preprint_fetch.BIORXIV_DETAILS.format(server=server, doi=_doi.encode_path(ddoi)))
    v = _failed(d, "bioRxiv details")
    if v:
        return v
    j = _json(d)
    if not isinstance(j, dict):
        return _drift(f"details: HTTP {d.status} body is not JSON", d)
    coll = [c for c in (j.get("collection") or []) if isinstance(c, dict)]
    if not coll:
        msg = "; ".join(str(m.get("status")) for m in (j.get("messages") or []) if isinstance(m, dict))
        # The /details outage (2026-09-24 to 09-28) answered 200 with nothing for known records:
        # an empty answer for a known DOI is a failed source, not drift.
        return _V("fail", f"details {ddoi}: empty collection ({msg or 'no message'})", d.host, Kind.OUTAGE,
                  d.status)
    if not any(str(c.get("doi") or "").lower() == ddoi for c in coll):
        return _drift(f"details {ddoi}: the collection does not carry the DOI", d)
    pserver, pdoi, published = BIORXIV_PUBS_TARGET
    pu = p.get(f"{_biorxiv_base()}/pubs/{pserver}/{_doi.encode_path(pdoi)}/na/json")
    v = _failed(pu, "bioRxiv pubs")
    if v:
        return v
    j = _json(pu)
    msgs = [m for m in (j.get("messages") or []) if isinstance(m, dict)] if isinstance(j, dict) else []
    coll = [c for c in (j.get("collection") or []) if isinstance(c, dict)] if isinstance(j, dict) else []
    status = str(msgs[0].get("status")) if msgs else "none"
    if status != "ok" or not any(str(c.get("published_doi") or "").lower() == published for c in coll):
        return _drift(f"pubs {pdoi}: status {status}, published_doi not {published}", pu)
    return _ok(f"details ok ({ddoi}); pubs ok ({pdoi} -> {published})", pu)


def _c_osf(p, rctx):
    import lit_net
    import preprint_fetch
    from litpipe import net
    rec = p.get(preprint_fetch.OSF_API + f"files/{OSF_FILE}/")
    v = _failed(rec, "OSF file record")
    if v:
        return v
    j = _json(rec)
    data = (j.get("data") or {}) if isinstance(j, dict) else {}
    link = ((data.get("links") or {}).get("download") or "") if isinstance(data, dict) else ""
    if not link:
        return _drift("OSF file record has no data.links.download", rec)
    dl = p.get(link, headers={"Accept": "application/pdf,*/*"}, stream=True,
               max_bytes=lit_net.MAX_PDF_BYTES)
    v = _failed(dl, "OSF download")
    if v:
        return v
    r = _resp(dl)
    if r is None or net.expect_pdf(r) is not None:
        why = _short(dl.detail, 100) if dl.detail else "not a PDF"
        return _drift(f"OSF download: HTTP {dl.status} from {dl.host}: {why}", dl)
    if r.total_bytes != OSF_BYTES:
        return _drift(f"OSF download: %PDF but {r.total_bytes} B, expected {OSF_BYTES}", dl)
    return _ok(f"OSF {OSF_FILE}: %PDF, {r.total_bytes} B via {dl.host} ({dl.attempts} hops)", dl)


def _c_sportrxiv(p, rctx):
    import preprint_fetch
    since = (rctx.now - timedelta(days=1)).strftime("%Y-%m-%d")
    o = p.get(preprint_fetch.SPORTRXIV_OAI, params={"verb": "ListRecords", "metadataPrefix": "oai_dc",
                                                     "from": since})
    v = _failed(o, "SportRxiv OAI")
    if v:
        return v
    try:
        recs, token, err = preprint_fetch.parse_oai(_content(o))
    except ValueError as e:
        return _drift(f"SportRxiv OAI: HTTP {o.status} not parseable ({_short(e, 80)})", o)
    if o.status != 200 or err not in ("", "noRecordsMatch"):
        return _drift(f"SportRxiv OAI: HTTP {o.status}, OAI error {err}", o)
    return _ok(f"SportRxiv OAI from={since}: 200, {len(recs)} records"
               + (" (noRecordsMatch)" if err else "") + (", resumption token" if token else ""), o)


def _arxiv_skip(rctx):
    for h in dict.fromkeys((*ARXIV_HOSTS, _origin_host(CHECKS_BY_ID["arxiv"]))):
        r = _refusal(rctx.state, h)
        if r:
            return f"{h} is refused ({r}); arXiv not sent"
    if not rctx.arxiv_scheduled():
        return "no scheduled project's sources include arxiv"
    return None


def _c_arxiv(p, rctx):
    import preprint_fetch
    o = p.get(preprint_fetch.ARXIV_API, params={"id_list": ARXIV_ID, "max_results": 1})
    v = _failed(o, "arXiv id_list")
    if v:
        return v
    body = _content(o)
    if b"rate exceeded" in body[:400].lower():
        return _V("refused", "arXiv id_list: 200 'Rate exceeded'", o.host, Kind.REFUSED, o.status)
    try:
        entries = preprint_fetch.parse_arxiv_feed(body)
    except ValueError as e:
        return _drift(f"arXiv id_list: HTTP {o.status} not Atom ({_short(e, 80)})", o)
    ok = [e for e in entries if e.get("base") == ARXIV_ID]
    if o.status != 200 or len(ok) != 1:
        return _drift(f"arXiv id_list: HTTP {o.status}, {len(ok)} entries for {ARXIV_ID}", o)
    return _ok(f"arXiv {ARXIV_ID}: 200 Atom, 1 entry", o)


def _c_datacite(p, rctx):
    import ris_emit
    from litpipe import doi as _doi
    o = p.get(ris_emit.DATACITE_WORK.format(doi=_doi.encode_path(DATACITE_DOI)),
              headers={"Accept": "application/vnd.api+json"})
    v = _failed(o, "DataCite")
    if v:
        return v
    j = _json(o)
    attrs = ((j.get("data") or {}).get("attributes") if isinstance(j, dict) else None) or {}
    pub = attrs.get("publisher") if isinstance(attrs, dict) else None
    if isinstance(pub, str) and pub:
        return _ok(f"DataCite {DATACITE_DOI}: publisher is a string", o)
    if isinstance(pub, dict):
        return _drift(f"DataCite {DATACITE_DOI}: publisher is an object (the default flip announced "
                      f"for September 2027): ris_emit already asks publisher=true", o)
    return _drift(f"DataCite {DATACITE_DOI}: HTTP {o.status}, publisher {type(pub).__name__}", o)


def _c_doira(p, rctx):
    import ris_emit
    o = p.get(ris_emit.DOI_RA.format(doi=",".join(DOIRA_EXPECT)))
    v = _failed(o, "doiRA")
    if v:
        return v
    j = _json(o)
    got = {str(r.get("DOI") or ""): str(r.get("RA") or "") for r in (j if isinstance(j, list) else [])
           if isinstance(r, dict)}
    bad = [f"{k} {got.get(k) or 'absent'}" for k, want in DOIRA_EXPECT.items()
           if (got.get(k) or "").lower() != want.lower()]
    if bad:
        return _drift(f"doiRA: {'; '.join(bad)} (expected {', '.join(DOIRA_EXPECT.values())})", o)
    return _ok("doiRA: " + ", ".join(got[k] for k in DOIRA_EXPECT), o)


def _c_cn(p, rctx):
    import ris_emit
    from litpipe import doi as _doi
    origin = _origin_host(CHECKS_BY_ID["content_negotiation"])
    pol = hosts.policy(origin)
    seen = []
    last = None
    for d in CN_DOIS:
        o = last = p.get(ris_emit.DOI_CN.format(doi=_doi.encode_path(d)), headers={"Accept": ris_emit.CSL_JSON})
        v = _failed(o, f"CN {d}")
        if v:
            return v
        j = _json(o)
        ident = str((j.get("DOI") or j.get("id") or "") if isinstance(j, dict) else "").lower()
        if o.status != 200 or not isinstance(j, dict) or not ident.endswith(d):
            return _drift(f"CN {d}: HTTP {o.status} from {o.host}, not its CSL-JSON", o)
        if o.host == origin or not pol.allows_redirect_to(o.host):
            return _drift(f"CN {d}: CSL from {o.host} after {o.attempts} requests, expected a "
                          f"redirect to an allow-listed host", o)
        seen.append(o.host)
    return _ok("CN: 302 then 200 CSL from " + ", ".join(seen), last)


def _openalex_skip(rctx):
    from litpipe import openalex
    return None if openalex.key_present() else "no OPENALEX_API_KEY (the keyed check only)"


def _c_openalex(p, rctx):
    from litpipe import openalex
    o = p.get(f"{openalex.BASE}/works",
              params={"filter": "doi:" + "|".join(OPENALEX_FLOORS),
                      "select": "id,doi,referenced_works_count", "per-page": len(OPENALEX_FLOORS)},
              headers=openalex._auth_headers())
    v = _failed(o, "OpenAlex list")
    if v:
        return v
    j = _json(o)
    rows = (j.get("results") or []) if isinstance(j, dict) else []
    counts = {}
    for w in rows:
        if isinstance(w, dict):
            d = re.sub(r"(?i)^https?://doi\.org/", "", str(w.get("doi") or "")).lower()
            counts[d] = w.get("referenced_works_count")
    bad = []
    for d, floor in OPENALEX_FLOORS.items():
        n = counts.get(d)
        if not isinstance(n, int) or isinstance(n, bool):
            bad.append(f"{d} count {type(n).__name__}")
        elif n <= 0 or n < floor:
            bad.append(f"{d} {n} < floor {floor}")
    cost = openalex.rate_limit(_resp(o).headers if _resp(o) else None).get("cost_usd")
    if cost is None:
        bad.append("no X-RateLimit-Cost-USD header")
    elif abs(cost - OPENALEX_COST_USD) > 1e-9:
        bad.append(f"cost {cost} USD, expected {OPENALEX_COST_USD}")
    shown = " / ".join(str(counts.get(d)) for d in OPENALEX_FLOORS)
    if bad:
        return _drift(f"OpenAlex: {'; '.join(bad)} (counts {shown})", o)
    return _ok(f"OpenAlex referenced_works_count {shown}; cost {cost} USD", o)


def _c_crossref_refs(p, rctx):
    import ris_emit
    o = p.get(ris_emit.CROSSREF_SEARCH,
              params={"filter": ",".join(f"doi:{d}" for d in CROSSREF_REF_FLOORS),
                      "select": "DOI,reference,references-count", "rows": len(CROSSREF_REF_FLOORS)})
    v = _failed(o, "Crossref references")
    if v:
        return v
    pool = str(_header(o, "x-api-pool") or "")
    j = _json(o)
    items = ((j.get("message") or {}).get("items") or []) if isinstance(j, dict) else []
    lens = {}
    for it in items:
        if isinstance(it, dict):
            ref = it.get("reference")
            lens[str(it.get("DOI") or "").lower()] = len(ref) if isinstance(ref, list) else type(ref).__name__
    bad = [] if pool.lower().startswith("polite") else [f"pool {pool or 'missing'}"]
    for d, floor in CROSSREF_REF_FLOORS.items():
        n = lens.get(d)
        if not isinstance(n, int):
            bad.append(f"{d} reference {n or 'absent'}")
        elif n < floor:
            bad.append(f"{d} {n} < floor {floor}")
    shown = " / ".join(str(lens.get(d)) for d in CROSSREF_REF_FLOORS)
    if bad:
        return _drift(f"Crossref references: {'; '.join(bad)} (lengths {shown})", o)
    return _ok(f"Crossref {pool}: reference lengths {shown}", o)


def _c_alias(p, rctx):
    import ris_emit
    from litpipe import doi as _doi
    o = p.get(ris_emit.CROSSREF_WORK.format(doi=_doi.encode_path(ALIAS_DOI)))
    v = _failed(o, "Crossref alias")
    if v:
        return v
    j = _json(o)
    got = str(((j.get("message") or {}).get("DOI") if isinstance(j, dict) else "") or "").lower()
    if o.status != 200 or got != ALIAS_PRIME:
        return _drift(f"alias {ALIAS_DOI}: HTTP {o.status}, message.DOI {got or 'absent'}", o)
    return _ok(f"alias {ALIAS_DOI} -> prime {ALIAS_PRIME} ({o.attempts} requests)", o)


def sici_url() -> str:
    """The URL the SICI check sends: migrate_closed_to_md.doi_url of the `;2-#` doc SICI DOI."""
    import migrate_closed_to_md
    return migrate_closed_to_md.doi_url(SICI_HASH_DOI)


def _c_sici(p, rctx):
    o = p.get(sici_url())
    if o.status in _REDIRECTS and _header(o, "Location"):
        from urllib.parse import urlsplit
        to = urlsplit(str(_header(o, "Location"))).hostname or "?"
        return _ok(f"encoded SICI resolved: HTTP {o.status} to {to}", o)
    v = _failed(o, "encoded SICI")
    if v:
        return v
    return _drift(f"encoded SICI: HTTP {o.status} (expected a 30x from doi.org)", o)


def _const(module, name):
    """An origin getter: the stage module's own URL constant, read at call time (tests point the
    constants at a MockServer; importing the stage modules waits until a check runs)."""
    def get():
        import importlib
        return getattr(importlib.import_module(module), name)
    get.__qualname__ = f"_const({module}.{name})"
    return get


def _idconv_skip(rctx):
    import lit_net
    for h in dict.fromkeys((lit_net.IDCONV_HOST, _origin_host(CHECKS_BY_ID["idconv"]))):
        r = _refusal(rctx.state, h)
        if r:
            return f"{h} is refused ({r}); idconv not sent"
    return None


# ------------------------------------------------------------------------------ local checks
def _l_first_attempts(chk, lc):
    by_host: dict[str, dict] = {}
    for _, _, _, rec in lc.ledger_records():
        if int(rec.get("hop") or 0) != 0:
            continue
        if str(rec.get("purpose") or "").startswith("canary:"):
            continue
        if rec.get("decision") in ("not_sent", "prohibited"):
            continue
        h = str(rec.get("host") or "?")
        status = rec.get("status")
        kind = rec.get("kind")
        if rec.get("attempt") == 1:
            e = by_host.setdefault(h, {"n": 0, "statuses": Counter(), "kinds": Counter(), "refused": 0})
            e["n"] += 1
            e["statuses"][str(status) if status else str(kind or rec.get("decision") or "none")] += 1
            e["kinds"][str(kind or rec.get("decision") or "none")] += 1
        # the call's typed outcome is its FINAL line (attempt 1, or the last retry): a 429 that
        # litpipe.net retried and then served is not REFUSED; one whose retries ran out is
        if kind == "REFUSED" and rec.get("decision") == "final":
            by_host.setdefault(h, {"n": 0, "statuses": Counter(), "kinds": Counter(), "refused": 0})["refused"] += 1
    alarms, hosts_out = [], {}
    for h, e in sorted(by_host.items()):
        share = e["refused"] / e["n"] if e["n"] else 0.0
        hosts_out[h] = {"n": e["n"], "statuses": dict(e["statuses"]),
                        "kinds": {k: round(v / e["n"], 3) for k, v in e["kinds"].items()},
                        "refused_share": round(share, 3)}
        if e["n"] >= REFUSED_MIN_ATTEMPTS and share >= REFUSED_ALARM_SHARE:
            alarms.append(f"{h} REFUSED {e['refused']}/{e['n']} ({share:.0%})")
        if h == EPMC_HOST and e["statuses"].get("403"):
            alarms.append(f"{h} 403 on {e['statuses']['403']} first attempts")
    n = sum(e["n"] for e in by_host.values())
    summ = ("; ".join(alarms) if alarms else
            f"{n} first attempts on {len(by_host)} hosts; no host at REFUSED >= 5 % of 20+")
    return [_mk(chk, ALARM if alarms else PASS, host="ledger", target=lc.ledger_target(),
                observed={"summary": summ, "hosts": hosts_out})]


class _Unreadable(Exception):
    pass


def _read_rows(paths):
    rows, bad = [], []
    for p in paths:
        try:
            with open(p, encoding="utf-8-sig", newline="") as f:
                rows.extend(csv.DictReader(f))
        except (OSError, csv.Error, UnicodeDecodeError) as e:
            bad.append(f"{Path(p).name} ({type(e).__name__})")
    if bad:
        raise _Unreadable("unreadable report: " + ", ".join(bad))
    return rows


def _errors(cids, pr, e):
    return [_mk(CHECKS_BY_ID[c], ERROR, host=pr.key, target=pr.target(), observed=str(e)) for c in cids]


def _truthy(v) -> bool:
    return str(v or "").strip().lower() in ("true", "1", "yes")


def _yield_checks(lc):
    """yield_pmc, epmc_403, unpaywall_403, yield_mdpi_bmc and preprint_outcomes per project."""
    out = []
    for pr in lc.projects:
        if not pr.run_ids:
            continue
        reports = pr.reports()
        src = pr.sources
        if "pmc" in src:
            try:
                rows = _read_rows(reports.get("pmc", []))
            except _Unreadable as e:
                out += _errors(("yield_pmc", "epmc_403"), pr, e)
                rows = None
            if rows is None:
                pass
            elif not reports.get("pmc"):
                for cid in ("yield_pmc", "epmc_403"):
                    out.append(_mk(CHECKS_BY_ID[cid], SKIPPED, host=pr.key, target=pr.target(),
                                   observed="no PMC report for this run"))
            else:
                with_id = [r for r in rows if (r.get("pmcid") or "").strip() and not _truthy(r.get("skipped"))]
                dl = sum(1 for r in with_id if _truthy(r.get("downloaded")))
                n = len(with_id)
                share = dl / n if n else 0.0
                alarm = n >= PMC_MIN_ROWS and share < PMC_FLOOR
                out.append(_mk(CHECKS_BY_ID["yield_pmc"], ALARM if alarm else PASS, host=pr.key,
                               target=pr.target(),
                               observed=f"PMC downloaded {dl}/{n} rows with a PMCID ({share:.2f})"
                                        + ("" if n >= PMC_MIN_ROWS else f"; under {PMC_MIN_ROWS} rows, no floor")))
                e403 = [r for r in rows if (str(r.get("route") or "").startswith("epmc")
                                            and str(r.get("first_status") or "").strip() == "403")
                        or "europepmc/HTTP_403" in f"{r.get('attempts') or ''} {r.get('error') or ''}"]
                out.append(_mk(CHECKS_BY_ID["epmc_403"], ALARM if e403 else PASS, host=pr.key,
                               target=pr.target(), observed=f"{len(e403)} Europe PMC 403 rows in the PMC report"))
        if "unpaywall" in src:
            try:
                urows = _read_rows(reports.get("unpaywall", []))
            except _Unreadable as e:
                out += _errors(("unpaywall_403", "yield_mdpi_bmc"), pr, e)
                urows = None
            if urows is None:
                pass
            elif not reports.get("unpaywall"):
                for cid in ("unpaywall_403", "yield_mdpi_bmc"):
                    out.append(_mk(CHECKS_BY_ID[cid], SKIPPED, host=pr.key, target=pr.target(),
                                   observed="no Unpaywall report for this run"))
            else:
                rows = urows
                sent = n403 = 0
                for r in rows:
                    for tok in str(r.get("attempts") or "").split(" | "):
                        tok = tok.strip()
                        if not tok:
                            continue
                        status = tok.rsplit("/", 1)[-1]
                        if status in _NOT_SENT_TOKENS:
                            continue
                        sent += 1
                        n403 += status == "HTTP_403"
                share = n403 / sent if sent else 0.0
                alarm = sent >= UNPAYWALL_MIN_ATTEMPTS and share > UNPAYWALL_403_CEILING
                out.append(_mk(CHECKS_BY_ID["unpaywall_403"], ALARM if alarm else PASS, host=pr.key,
                               target=pr.target(),
                               observed=f"Unpaywall HTTP_403 {n403}/{sent} OA download attempts ({share:.2f})"
                                        + ("" if sent >= UNPAYWALL_MIN_ATTEMPTS
                                           else f"; under {UNPAYWALL_MIN_ATTEMPTS} attempts, no ceiling")))
                mb = [r for r in rows if str(r.get("doi") or "").strip().lower().startswith(MDPI_BMC_PREFIXES)
                      and str(r.get("oa_status") or "").strip().upper() == "OA"]
                dl = sum(1 for r in mb if _truthy(r.get("downloaded")))
                share = dl / len(mb) if mb else 0.0
                alarm = len(mb) >= MDPI_BMC_MIN_ROWS and share < MDPI_BMC_FLOOR
                out.append(_mk(CHECKS_BY_ID["yield_mdpi_bmc"], ALARM if alarm else PASS, host=pr.key,
                               target=pr.target(),
                               observed=f"MDPI+BMC downloaded {dl}/{len(mb)} OA rows ({share:.2f})"
                                        + ("" if len(mb) >= MDPI_BMC_MIN_ROWS
                                           else f"; under {MDPI_BMC_MIN_ROWS} rows, no floor")))
        if src & PREPRINT_SOURCES and reports.get("preprint"):
            try:
                rows = _read_rows(reports["preprint"])
            except _Unreadable as e:
                out += _errors(("preprint_outcomes",), pr, e)
                continue
            kinds = Counter(str(r.get("outcome") or "none") for r in rows)
            out.append(_mk(CHECKS_BY_ID["preprint_outcomes"], PASS, host=pr.key, target=pr.target(),
                           observed=f"{len(rows)} preprint rows: "
                                    + ", ".join(f"{k} {v}" for k, v in kinds.most_common())))
    return out


def _l_yields(chk, lc):
    """All five yield checks at once (one pass over each report); run() calls it once."""
    return _yield_checks(lc)


def _new_files(paths, since_ts):
    out = []
    for p in paths:
        try:
            if p.stat().st_mtime >= since_ts:
                out.append(p)
        except OSError:
            continue
    return out


def _l_mismatch(chk, lc):
    out = []
    for pr in lc.projects:
        if pr.lib is None or not pr.lib.is_dir():
            continue
        mm = pr.lib / "_mismatch"
        files = [p for p in mm.rglob("*") if p.is_file()] if mm.is_dir() else []
        new = _new_files(files, lc.since_ts)
        out.append(_mk(chk, ALARM if new else PASS, host=pr.key, target=f"{pr.key}: {mm.name}/",
                       observed=f"{len(new)} new files in _mismatch/"
                                + (f" (e.g. {', '.join(p.name for p in new[:3])})" if new else "")))
    return out


_RIS_TEXT_TAGS = frozenset({"TI", "T1", "T2", "T3", "BT", "ST", "JO", "JF", "JA", "J2", "AB", "N2",
                            "AU", "A1", "A2", "A3", "A4", "ED", "PB", "KW", "CY"})
_RIS_LINE = re.compile(r"^([A-Z][A-Z0-9])  - ?(.*)$")
_SIDECAR_FIELDS = ("title", "subtitle", "journal", "abstract", "authors")


def _dirty(s) -> bool:
    from litpipe import text as _text
    s = str(s or "")
    return bool(s) and (_text.strip_tags(s) != s or _text.unescape(s) != s)


def _file_has_markup(p: Path) -> bool:
    try:
        if p.name.endswith(".ris"):
            for line in p.read_text(encoding="utf-8-sig", errors="replace").splitlines():
                m = _RIS_LINE.match(line)
                if m and m.group(1) in _RIS_TEXT_TAGS and _dirty(m.group(2)):
                    return True
            return False
        rec = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    if not isinstance(rec, dict):
        return False
    for k in _SIDECAR_FIELDS:
        v = rec.get(k)
        for s in (v if isinstance(v, list) else [v]):
            if isinstance(s, str) and _dirty(s):
                return True
    return False


def _l_markup(chk, lc):
    out = []
    for pr in lc.projects:
        if pr.lib is None or not pr.lib.is_dir():
            continue
        cands = [p for p in pr.lib.iterdir() if p.is_file()
                 and (p.name.endswith(".ris") or p.name.endswith(".fulltext.json"))]
        new = _new_files(cands, lc.since_ts)
        if not new:
            continue
        bad = [p for p in new if _file_has_markup(p)]
        rate = len(bad) / len(new)
        out.append(_mk(chk, ALARM if bad else PASS, host=pr.key, target=f"{pr.key}: new .ris / .fulltext.json",
                       observed={"summary": f"markup or entities in {len(bad)}/{len(new)} new files ({rate:.1%})"
                                            + (f", e.g. {', '.join(p.name for p in bad[:3])}" if bad else ""),
                                 "files_checked": len(new), "with_markup": len(bad), "rate": round(rate, 4)}))
    return out


def _email_patterns():
    pats = [re.compile(r"(?i)e-?mail="), re.compile(r"(?i)mailto:")]
    for e in ledger._configured_emails():
        for form in ledger._literal_forms(e):
            pats.append(re.compile(re.escape(form), re.IGNORECASE))
    return pats


def _grep_lines(lines, pats):
    return [i for i, line in lines if any(p.search(line) for p in pats)]


def _l_email(chk, lc):
    pats = _email_patterns()
    out = []
    for pr in lc.projects:
        if not pr.run_ids:
            continue
        files = _new_files([p for ps in pr.reports().values() for p in ps], lc.since_ts)
        hits = []
        for f in files:
            try:
                with open(f, encoding="utf-8", errors="replace") as fh:
                    hits += [f"{f.name}:{i}" for i in _grep_lines(enumerate(fh, 1), pats)]
            except OSError:
                continue
        out.append(_mk(chk, ALARM if hits else PASS, host=pr.key, target=f"{pr.key}: run report CSVs",
                       observed=f"{len(hits)} lines with an email token in {len(files)} report files"
                                + (f": {', '.join(hits[:5])}" + (f" (+{len(hits) - 5} more)" if len(hits) > 5 else "")
                                   if hits else "")))
    led = [(f"{path.name}:{i}", raw) for path, i, raw, _ in lc.ledger_records()]
    hits = [loc for loc, raw in led if any(p.search(raw) for p in pats)]
    out.append(_mk(chk, ALARM if hits else PASS, host="ledger", target=lc.ledger_target(),
                   observed=f"{len(hits)} ledger lines with an email token of {len(led)}"
                            + (f": {', '.join(hits[:5])}" if hits else "")))
    return out


def _l_doi_fixtures(chk, lc):
    from litpipe import doi as _doi
    bad = []
    for i, (raw, fn, want) in enumerate(DOI_FIXTURES):
        got = getattr(_doi, fn)(raw)
        if got != want:
            bad.append(f"#{i} {fn}({raw!r}) = {got!r}, expected {want!r}")
    return [_mk(chk, ALARM if bad else PASS, host="local", target=f"{len(DOI_FIXTURES)} fixtures",
                observed="; ".join(bad) if bad else f"{len(DOI_FIXTURES)} fixtures pass")]


def _newest_lib_mtime(lib: Path) -> float | None:
    newest = None
    try:
        with os.scandir(lib) as it:
            for e in it:
                n = e.name
                if e.is_file() and (n.lower().endswith(".pdf") or n.endswith(".ris") or n.endswith(".fulltext.json")):
                    t = e.stat().st_mtime
                    newest = t if newest is None or t > newest else newest
    except OSError:
        return None
    return newest


def _l_index(chk, lc):
    path = lc.db_path()
    if path is None or not Path(path).is_file():
        return [_mk(chk, SKIPPED, host="portfolio.duckdb", target=str(path), observed="no portfolio.duckdb")]
    import lit_util
    try:
        con = lit_util.connect_db(str(path), read_only=True, tries=1)
    except Exception as e:  # noqa: BLE001 - a lock or an unreadable file: skipped, never retried
        return [_mk(chk, SKIPPED, host="portfolio.duckdb", target=str(path),
                    observed=f"could not open read-only ({type(e).__name__})")]
    out = []
    try:
        tables = {r[0] for r in con.execute("SELECT table_name FROM information_schema.tables").fetchall()}
        if "index_runs" not in tables:
            return [_mk(chk, SKIPPED, host="portfolio.duckdb", target=str(path), observed="no index_runs table")]
        for pr in lc.projects:
            if pr.lib is None or not pr.lib.is_dir():
                continue
            row = con.execute("SELECT max(epoch(finished_at)) FROM index_runs WHERE project = ?",
                              [pr.key]).fetchone()
            stamp = float(row[0]) if row and row[0] is not None else None
            newest = _newest_lib_mtime(pr.lib)
            if newest is None:
                obs, st = "library holds no PDF or sidecar", PASS
            elif stamp is None:
                obs, st = "never indexed (no index_runs row) while the library holds files", ALARM
            else:
                lag = newest - stamp
                st = ALARM if lag > INDEX_STALE_S else PASS
                obs = (f"index {datetime.fromtimestamp(stamp, timezone.utc):%Y-%m-%d %H:%M}Z, newest file "
                       f"{datetime.fromtimestamp(newest, timezone.utc):%Y-%m-%d %H:%M}Z"
                       + (f" ({lag / 3600:.0f} h newer)" if lag > 0 else ""))
            out.append(_mk(chk, st, host=pr.key, target=f"{pr.key}: index_runs vs library", observed=obs))
    except Exception as e:  # noqa: BLE001 - a foreign or broken DB: skipped
        return [_mk(chk, SKIPPED, host="portfolio.duckdb", target=str(path),
                    observed=f"index_runs unreadable ({type(e).__name__})")]
    finally:
        con.close()
    return out


def _l_lost(chk, lc):
    out = []
    for pr in lc.projects:
        if not pr.run_ids:
            continue
        if pr.stages is None:
            out.append(_mk(chk, SKIPPED, host=pr.key, target=pr.target(),
                           observed="no stage list (a runner context names the stages run)"))
            continue
        ids = set(pr.run_ids)
        arts = [a for _, a in pr.artifacts() if a.run_id in ids]
        have = {(a.tag, a.run_id, a.stage) for a in arts}
        tags: dict = {}
        for a in arts:
            tags.setdefault(a.run_id, set()).add(a.tag)
        stages = [s for s in pr.stages if s not in _TRANSIENT_STAGES]
        want = [(t, sid, s) for sid in pr.run_ids for t in sorted(tags.get(sid) or {""}) for s in stages]
        missing = [f"{t + '.' if t else ''}{sid}.{s}" for t, sid, s in want if (t, sid, s) not in have]
        out.append(_mk(chk, ALARM if missing else PASS, host=pr.key, target=pr.target(),
                       observed=(f"missing artifacts: {', '.join(missing[:6])}"
                                 + (f" (+{len(missing) - 6} more)" if len(missing) > 6 else ""))
                       if missing else f"{len(want)} artifacts present"))
    return out


# ------------------------------------------------------------------------------ the table
def _net_check(id, cadence, requests, target, expected, fn, origin, skip=None):
    return Check(id, cadence, "network", requests, target, expected, fn, origin, skip)


def _local_check(id, target, expected, fn):
    return Check(id, "every_run", "local", 0, target, expected, fn)


CHECKS = (
    _net_check("idconv", "every_run", 1, f"idconv ids={IDCONV_DOI}", f"200 JSON, pmcid {IDCONV_PMCID}",
               _c_idconv, _const("lit_net", "IDCONV"), _idconv_skip),
    _net_check("epmc_fulltextxml", "daily", 2, f"Europe PMC fullTextXML AM {EPMC_AM} + OA {EPMC_OA}",
               "AM 500 with the stored JSON signature; OA 200 XML",
               _c_epmc, _const("jats_to_text", "EPMC_JATS_XML")),
    _net_check("efetch", "daily", 1, "efetch db=pmc " + ",".join(EFETCH_EXPECT),
               "open-access yes/no/no; manuscript no/yes/no; no-XML comment on the third",
               _c_efetch, _const("lit_net", "EUTILS")),
    _net_check("s3", "daily", 3, f"PMC Cloud list + metadata + HEAD pdf_url ({S3_PMCID})",
               "200 ListBucketResult / pdf_url present / HEAD 200 with a length",
               _c_s3, _const("pmc_fetch", "S3_BASE")),
    _net_check("bioc", "daily", 1, f"BioC JSON {BIOC_AM} (author manuscript)", "200 JSON, >= 1 passage",
               _c_bioc, _const("jats_to_text", "BIOC_JSON")),
    _net_check("biorxiv", "daily", 2,
               f"bioRxiv details {BIORXIV_DETAILS_TARGET[1]} + pubs {BIORXIV_PUBS_TARGET[1]}",
               "details non-empty with the DOI; pubs ok with the published DOI",
               _c_biorxiv, _const("preprint_fetch", "BIORXIV_DETAILS")),
    _net_check("osf", "daily", 4, f"OSF files/{OSF_FILE}/ -> links.download -> follow",
               f"%PDF, {OSF_BYTES:,} B", _c_osf, _const("preprint_fetch", "OSF_API")),
    _net_check("sportrxiv", "daily", 1, "SportRxiv OAI ListRecords from=<yesterday>", "200, parseable OAI-PMH",
               _c_sportrxiv, _const("preprint_fetch", "SPORTRXIV_OAI")),
    _net_check("arxiv", "daily", 1, f"arXiv id_list={ARXIV_ID}", "200 Atom with 1 entry; otherwise skip arXiv",
               _c_arxiv, _const("preprint_fetch", "ARXIV_API"), _arxiv_skip),
    _net_check("datacite", "daily", 1, f"DataCite /dois/{DATACITE_DOI}", "publisher is a string (alert on an object)",
               _c_datacite, _const("ris_emit", "DATACITE_WORK")),
    _net_check("doira", "daily", 1, "doi.org /doiRA/" + ",".join(DOIRA_EXPECT), ", ".join(DOIRA_EXPECT.values()),
               _c_doira, _const("ris_emit", "DOI_RA")),
    _net_check("openalex", "weekly", 1, "OpenAlex list of " + ", ".join(OPENALEX_FLOORS),
               "referenced_works_count >= floors " + "/".join(map(str, OPENALEX_FLOORS.values()))
               + f"; X-RateLimit-Cost-USD {OPENALEX_COST_USD}",
               _c_openalex, _const("litpipe.openalex", "BASE"), _openalex_skip),
    _net_check("crossref_refs", "weekly", 1, "Crossref references of " + ", ".join(CROSSREF_REF_FLOORS),
               "polite pool; len(reference) >= " + "/".join(map(str, CROSSREF_REF_FLOORS.values())),
               _c_crossref_refs, _const("ris_emit", "CROSSREF_SEARCH")),
    _net_check("content_negotiation", "weekly", 4, "doi.org CSL-JSON for " + ", ".join(CN_DOIS),
               "302 to an allow-listed RA host, then 200 CSL-JSON", _c_cn, _const("ris_emit", "DOI_CN")),
    _net_check("crossref_alias", "weekly", 2, f"Crossref /works/{ALIAS_DOI} (alias)", f"prime {ALIAS_PRIME}",
               _c_alias, _const("ris_emit", "CROSSREF_WORK")),
    _net_check("sici", "weekly", 1, "doi.org migrate_closed_to_md.doi_url(<;2-# SICI DOI>)",
               "a 30x (the encoded # resolves)", _c_sici, sici_url),
    _local_check("first_attempts", "first attempts of the run per host (ledger)",
                 "REFUSED < 5 % per host with >= 20 first attempts; no Europe PMC 403", _l_first_attempts),
    _local_check("yield_pmc", "PMC report", f"downloaded / PMCID rows >= {PMC_FLOOR} (>= {PMC_MIN_ROWS} rows)",
                 _l_yields),
    _local_check("epmc_403", "PMC report", "no Europe PMC 403", _l_yields),
    _local_check("unpaywall_403", "Unpaywall report",
                 f"HTTP_403 <= {UNPAYWALL_403_CEILING} of OA download attempts (>= {UNPAYWALL_MIN_ATTEMPTS})",
                 _l_yields),
    _local_check("yield_mdpi_bmc", "Unpaywall report, 10.3390/ + 10.1186/ OA rows",
                 f"downloaded share >= {MDPI_BMC_FLOOR} (>= {MDPI_BMC_MIN_ROWS} rows)", _l_yields),
    _local_check("preprint_outcomes", "preprint report (projects with a preprint source)",
                 "informational: typed outcome counts", _l_yields),
    _local_check("mismatch_growth", "<lib>/_mismatch/", "0 new files", _l_mismatch),
    _local_check("markup", "new .ris and .fulltext.json", "0 % with tags or entities", _l_markup),
    _local_check("email", "the run's report CSVs and ledger lines", "0 hits (email or mailto tokens, the configured address)", _l_email),
    _local_check("doi_fixtures", "litpipe.doi fixtures", "every fixture as built", _l_doi_fixtures),
    _local_check("index_freshness", "index_runs vs the library", "library at most a day newer than the index",
                 _l_index),
    _local_check("lost_artifacts", "lit_pull_queue[.<tag>].<run_id>.<stage>.csv",
                 "every stage run has its artifact", _l_lost),
)
CHECKS_BY_ID = {c.id: c for c in CHECKS}


def _origin_host(chk) -> str:
    return hosts.host_of(chk.origin())


def checks(profile, phase="all") -> list[Check]:
    """The checks a profile runs (cumulative), in table order."""
    if profile not in _RANK:
        raise ValueError(f"profile must be one of {PROFILES}, got {profile!r}")
    if phase not in PHASES:
        raise ValueError(f"phase must be one of {PHASES}, got {phase!r}")
    return [c for c in CHECKS if _RANK[c.cadence] <= _RANK[profile] and phase in ("all", c.phase)]


def worst_case(check_id) -> int:
    """Requests a failing network check can send: two rounds, each at most its planned count plus
    litpipe.net's transport retries on the request that failed."""
    return MAX_ROUNDS * (CHECKS_BY_ID[check_id].requests + TRANSPORT_RETRIES)


# ------------------------------------------------------------------------------ contexts
@dataclass
class _Project:
    key: str
    root: Path | None
    lib: Path | None
    sources: set
    dirs: list
    run_ids: list
    stages: list | None

    def target(self):
        return f"{self.key} run {','.join(self.run_ids)}"

    def artifacts(self):
        import sweep
        seen, out = set(), []
        for d in self.dirs:
            try:
                names = sorted(d.glob("lit_pull_queue.*.csv"))
            except OSError:
                continue
            for p in names:
                a = sweep.parse_artifact(p.name)
                if a and p.resolve() not in seen:
                    seen.add(p.resolve())
                    out.append((p, a))
        return out

    def reports(self) -> dict:
        """{stage: [paths]} of this run's artifacts (any tag)."""
        out: dict[str, list] = {}
        ids = set(self.run_ids)
        for p, a in self.artifacts():
            if a.run_id in ids:
                out.setdefault(a.stage, []).append(p)
        return out


def _abs(base, p):
    if p is None or p == "":
        return None
    p = Path(p).expanduser()
    return p if p.is_absolute() or base is None else Path(base) / p


def _registry_projects(cfg):
    import lit_util
    reg = config.load(cfg).get("projects") or {}
    out = []
    for key, p in reg.items():
        if not isinstance(p, dict) or not p.get("active", True):
            continue
        out.append((key, p, lit_util.project_root(key, p),
                    lit_util.lib_paths(key, p)[1] if p.get("lib_dir") else None))
    return out


class _RunCtx:
    """What one run() resolves once: state, request, cfg, now, and the scheduled projects."""

    def __init__(self, *, context, state, request, cfg, now):
        from litpipe import net
        self.context = context or {}
        self.state = _resolve_state(state)
        self.request = request or net.request
        self.cfg = cfg
        self.now = _now(now)
        self._sources = None

    def project_sources(self) -> list:
        if self._sources is None:
            if self.context.get("projects") is not None:
                out = []
                for p in self.context["projects"]:
                    s = p.get("sources")
                    if s is None:
                        try:
                            s = config.sources(p.get("key"), cfg=self.cfg)
                        except config.ConfigError:
                            s = config.DEFAULT_SOURCES
                    out.append(set(s))
                self._sources = out
            else:
                self._sources = [config.sources(k, cfg=self.cfg) for k, *_ in _registry_projects(self.cfg)]
        return self._sources

    def arxiv_scheduled(self) -> bool:
        return any("arxiv" in s for s in self.project_sources())


class _LocalCtx:
    def __init__(self, rctx: _RunCtx):
        ctx = rctx.context
        self.cfg = rctx.cfg
        self.now = rctx.now
        self.run_id = ctx.get("run_id")
        start_of_day = self.now.replace(hour=0, minute=0, second=0, microsecond=0)
        self.since = _parse_iso(ctx.get("since")) or start_of_day
        self.since_ts = self.since.timestamp()
        self._db = ctx.get("db_path")
        self._ledger = None
        self.projects = self._projects(ctx)

    def _projects(self, ctx):
        out = []
        if ctx.get("projects") is not None:
            for p in ctx["projects"]:
                root = _abs(None, p.get("root"))
                art = _abs(root, p.get("artifact_dir")) or root
                dirs = [d for d in dict.fromkeys([art, root]) if d is not None]
                src = p.get("sources")
                if src is None:
                    try:
                        src = config.sources(p.get("key"), cfg=self.cfg)
                    except config.ConfigError:
                        src = config.DEFAULT_SOURCES
                stages = p.get("stages")
                out.append(_Project(str(p.get("key")), root, _abs(root, p.get("lib_dir")), set(src), dirs,
                                    [str(s) for s in (p.get("sweep_run_ids") or [])],
                                    None if stages is None else [str(s) for s in stages]))
            return out
        today = self.since.strftime("%Y-%m-%d")
        for key, p, root, lib in _registry_projects(self.cfg):
            dirs = [root] + ([d for d in sorted(root.iterdir()) if d.is_dir()] if root.is_dir() else [])
            pr = _Project(key, root, lib, set(config.sources(key, cfg=self.cfg)), dirs, [], None)
            pr.run_ids = sorted({a.run_id for _, a in pr.artifacts() if a.run_id.startswith(today)})
            out.append(pr)
        return out

    def db_path(self):
        if self._db:
            return Path(self._db)
        return config.db_dir(self.cfg) / "portfolio.duckdb"

    def ledger_target(self):
        if self.run_id:
            return f"ledger lines of run {self.run_id}"
        return f"ledger lines since {self.since:%Y-%m-%dT%H:%MZ}"

    def ledger_records(self):
        """[(path, line number, raw line, record)] of the run (or, without a run id, since `since`),
        read without creating the ledger or state directory."""
        if self._ledger is not None:
            return self._ledger
        d = Path(ledger.LEDGER_DIR) if ledger.LEDGER_DIR is not None else \
            config.state_dir(self.cfg, create=False) / "ledger"
        out = []
        day = self.since.date()
        while d.is_dir() and day <= self.now.date():
            f = d / f"{day:%Y-%m-%d}.jsonl"
            day += timedelta(days=1)
            if not f.is_file():
                continue
            with open(f, encoding="utf-8", errors="replace") as fh:
                for i, raw in enumerate(fh, 1):
                    try:
                        rec = json.loads(raw)
                    except ValueError:
                        continue
                    if not isinstance(rec, dict):
                        continue
                    if self.run_id:
                        if rec.get("run_id") != self.run_id:
                            continue
                    else:
                        ts = _parse_iso(rec.get("ts"))
                        if ts is None or ts < self.since:
                            continue
                    out.append((f, i, raw, rec))
        self._ledger = out
        return out


# ------------------------------------------------------------------------------ running checks
def _mk(chk, status, *, host, observed, target=None, kind=None, action=ACTION_NONE, http=None,
        attempts=0, elapsed_ms=0):
    if kind is None:
        kind = {PASS: Kind.OK, SKIPPED: Kind.SKIPPED}.get(status, Kind.ERROR)
    payload = ledger.redact_obj({"id": chk.id, "phase": chk.phase, "cadence": chk.cadence,
                                 "target": target or chk.target, "expected": chk.expected,
                                 "observed": observed, "status": status, "action": action})
    return Outcome(kind, status=http, host=host, detail=chk.id, attempts=attempts,
                   elapsed_ms=elapsed_ms, payload=payload)


def _round(chk, rctx):
    p = _Probe(chk, rctx)
    try:
        v = chk.fn(p, rctx)
    except Exception as e:  # noqa: BLE001 - a canary bug is an ERROR outcome, never a crash
        v = _V("error", f"{type(e).__name__}: {_short(e, 160)}", "", Kind.ERROR, None)
    return v, p


def _run_network(chk, rctx) -> Outcome:
    origin = _origin_host(chk)
    reason = chk.skip(rctx) if chk.skip else None
    if reason is None:
        r = _refusal(rctx.state, origin)
        if r:
            reason = f"{origin} is refused ({r}); not sent"
    if reason:
        return _mk(chk, SKIPPED, host=origin, observed=reason)
    v, p = _round(chk, rctx)
    sent, elapsed = p.sent, p.elapsed_ms
    first = None
    if v.verdict == "fail":
        first = v
        _clock().sleep(RECHECK_S)
        v, p2 = _round(chk, rctx)
        sent, elapsed = sent + p2.sent, elapsed + p2.elapsed_ms
    common = dict(host=origin, http=v.status, attempts=sent, elapsed_ms=elapsed)
    obs = v.observed
    if first is not None:
        obs += f" (re-checked after {RECHECK_S:.0f} s; first round: {first.observed})"
    if v.verdict == "pass":
        return _mk(chk, PASS, observed=obs, kind=Kind.OK, **common)
    if v.verdict == "skip":
        return _mk(chk, SKIPPED, observed=obs, kind=Kind.SKIPPED, **common)
    if v.verdict == "error":
        return _mk(chk, ERROR, observed=obs, kind=Kind.ERROR, **common)
    kind = v.kind if v.kind not in (None, Kind.OK) else Kind.ERROR
    # Confirmed: a refusal status at once, or a failure that the re-check repeated.
    confirmed = v.verdict == "refused" or (v.verdict == "fail" and first is not None)
    if confirmed and v.host == origin:
        rctx.state.refuse(origin, ledger.redact(f"canary {chk.id}: {_short(v.observed, 200)}"), persistence="run")
        return _mk(chk, ALARM, observed=obs, kind=kind, action=ACTION_REFUSED, **common)
    if confirmed:
        by_net = _refusal(rctx.state, v.host) if v.host else None
        obs += (f" (on {v.host}, not the origin {origin}: litpipe.net refused {v.host} ({by_net}); the canary "
                f"refused nothing)" if by_net else
                f" (on {v.host or 'a redirect target'}, not the origin {origin}: nothing refused)")
    return _mk(chk, ALARM, observed=obs, kind=kind, **common)


def _run_local(chk, lc) -> list:
    try:
        return chk.fn(chk, lc)
    except Exception as e:  # noqa: BLE001
        return [_mk(chk, ERROR, host="local", observed=f"{type(e).__name__}: {_short(e, 160)}")]


def run(profile, *, phase="all", context=None, state=None, request=None, cfg=None, now=None) -> list[Outcome]:
    """Run the profile's checks for `phase` (see the module docstring). Raises ValueError on a bad
    profile or phase and config.ConfigError on a registry it cannot use."""
    selected = checks(profile, phase)
    _check_context(context)
    rctx = _RunCtx(context=context, state=state, request=request, cfg=cfg, now=now)
    outs = [_run_network(c, rctx) for c in selected if c.phase == "network"]
    local = [c for c in selected if c.phase == "local"]
    if local:
        lc = _LocalCtx(rctx)
        done = set()
        for c in local:
            if c.fn in done:          # the five yield checks share one function and one pass
                continue
            done.add(c.fn)
            outs += _run_local(c, lc)
    return outs


def _check_context(ctx):
    """config.ConfigError for a context the runner built wrong."""
    if ctx is None:
        return
    if not isinstance(ctx, dict):
        raise config.ConfigError(f"canaries: context must be a dict, got {type(ctx).__name__}")
    if ctx.get("since") is not None and _parse_iso(ctx["since"]) is None:
        raise config.ConfigError(f"canaries: context['since'] is not an ISO 8601 time: {ctx['since']!r}")
    projects = ctx.get("projects")
    if projects is None:
        return
    if not isinstance(projects, (list, tuple)):
        raise config.ConfigError("canaries: context['projects'] must be a list of dicts")
    for i, p in enumerate(projects):
        if not isinstance(p, dict) or not p.get("key"):
            raise config.ConfigError(f"canaries: context['projects'][{i}] must be a dict with a 'key'")
        for f in ("sources", "sweep_run_ids", "stages"):
            v = p.get(f)
            if v is not None and not isinstance(v, (list, tuple, set, frozenset)):
                raise config.ConfigError(f"canaries: context['projects'][{i}][{f!r}] must be a list, "
                                         f"got {type(v).__name__}")


def planned_requests(profile, *, context=None, state=None, cfg=None) -> dict:
    """{check id: planned requests} of the profile's network checks: 0 for a check that would be
    SKIPPED now (a refused host, arXiv not scheduled, no OpenAlex key). Sends nothing and never
    creates the state file."""
    _check_context(context)
    rctx = _RunCtx(context=context, state=state, request=None, cfg=cfg, now=None)
    out = {}
    for c in checks(profile, "network"):
        reason = c.skip(rctx) if c.skip else None
        if reason is None and _refusal(rctx.state, _origin_host(c)):
            reason = "refused"
        out[c.id] = 0 if reason else c.requests
    return out


# ------------------------------------------------------------------------------ report
def report(outcomes, *, run_id=None, profile=None, started=None) -> dict:
    checks_out = []
    for o in outcomes:
        p = dict(o.payload or {}) if isinstance(o.payload, dict) else {"id": o.detail}
        p.update({"host": o.host, "kind": str(o.kind), "http_status": o.status, "requests": int(o.attempts or 0)})
        checks_out.append(p)
    refused = sorted({c["host"] for c in checks_out if c.get("action") == ACTION_REFUSED})
    if isinstance(started, datetime):
        started = started.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    rep = {"run_id": run_id, "profile": profile, "started": started, "checks": checks_out,
           "refused_hosts": refused, "requests": sum(c["requests"] for c in checks_out)}
    return ledger.redact_obj(rep)


def _observed_text(c) -> str:
    ob = c.get("observed")
    if isinstance(ob, dict):
        ob = ob.get("summary", "")
    return str(ob or "")


def summary(rep) -> str:
    cs = rep.get("checks") or []
    n = Counter(c.get("status") for c in cs)
    bad = [c for c in cs if c.get("status") == ALARM] + [c for c in cs if c.get("status") == ERROR]
    head = (f"HEALTH {'ALARM' if bad else 'PASS'}: {n[ALARM]} alarm, {n[ERROR]} error, {n[PASS]} pass, "
            f"{n[SKIPPED]} skipped; {rep.get('requests', 0)} requests"
            + (f" ({rep['profile']})" if rep.get("profile") else ""))
    lines = [head]
    room = SUMMARY_LINES - 1
    shown = bad if len(bad) <= room else bad[:room - 1]
    for c in shown:
        act = " [refused for the run]" if c.get("action") == ACTION_REFUSED else ""
        lines.append(_short(f"{c.get('status')} {c.get('id')} {c.get('host')}: {_observed_text(c)}{act}", 200))
    if len(bad) > len(shown):
        lines.append(f"+{len(bad) - len(shown)} more ALARM")
    elif len(lines) < SUMMARY_LINES and rep.get("refused_hosts"):
        lines.append("refused for the run: " + ", ".join(rep["refused_hosts"]))
    return "\n".join(lines[:SUMMARY_LINES])


def health_ok(rep) -> bool:
    return not any(c.get("status") in (ALARM, ERROR) for c in rep.get("checks") or [])


# ------------------------------------------------------------------------------ CLI
class _Usage(Exception):
    pass


class _Parser(argparse.ArgumentParser):
    def error(self, message):
        raise _Usage(message)


def _parser():
    ap = _Parser(prog="python -m litpipe.canaries",
                 description="Health checks (canaries) and the health report. Exit 0 HEALTH PASS, 2 HEALTH "
                             "ALARM (after a final [step-summary] line), 1 a usage or config error.")
    ap.add_argument("--profile", required=True, choices=PROFILES,
                    help="every_run, daily, weekly or monthly (cumulative)")
    ap.add_argument("--phase", default="all", choices=PHASES,
                    help="network checks, local (0-request) checks, or all (default)")
    ap.add_argument("--json", metavar="PATH", help="write the report JSON here")
    ap.add_argument("--dry-run", action="store_true",
                    help="list the checks and their planned requests; send nothing")
    return ap


def _dry_run(profile, phase, cfg):
    planned = planned_requests(profile, cfg=cfg) if phase in ("network", "all") else {}
    for c in checks(profile, phase):
        n = planned.get(c.id, 0)
        note = "" if c.phase == "local" or n == c.requests else " (skipped now)"
        print(f"{c.id:<20} {c.cadence:<9} {c.phase:<7} {n} request(s){note}  {c.target}")
    print(f"[canaries] {profile}/{phase}: {sum(planned.values())} planned requests; dry run, nothing sent")


def main(argv=None):
    argv = sys.argv[1:] if argv is None else list(argv)
    try:
        args = _parser().parse_args(argv)
    except _Usage as e:
        _parser().print_usage(sys.stderr)
        print(f"[canaries] usage error: {e}", file=sys.stderr)
        return 1
    try:
        cfg = config.load()
        if args.dry_run:
            _dry_run(args.profile, args.phase, cfg)
            return 0
        started = datetime.now(timezone.utc)
        outs = run(args.profile, phase=args.phase, cfg=cfg, now=started)
    except (config.ConfigError, ValueError) as e:
        print(f"[canaries] config error: {ledger.redact(str(e))}", file=sys.stderr)
        return 1
    rep = report(outs, run_id=ledger.current_run_id(), profile=args.profile, started=started)
    print(summary(rep))
    if args.json:
        import lit_util
        p = Path(args.json)
        p.parent.mkdir(parents=True, exist_ok=True)
        lit_util.atomic_write_text(str(p), json.dumps(rep, indent=2, ensure_ascii=False) + "\n")
    if health_ok(rep):
        return 0
    bad = [c for c in rep["checks"] if c.get("status") in (ALARM, ERROR)]
    step = {"reasons": [_short(f"{c['id']} {c['host']}: {_observed_text(c)}", 200) for c in bad],
            "aborted": None,
            "transport_failures": sum(1 for c in rep["checks"] if c.get("kind") == str(Kind.TRANSPORT))}
    print("[step-summary] " + json.dumps(ledger.redact_obj(step), ensure_ascii=False))
    return 2


if __name__ == "__main__":
    sys.exit(main())
