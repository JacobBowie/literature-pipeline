"""Preprint stage: preprint copies of queue rows from arXiv, bioRxiv/medRxiv, OSF and SportRxiv, routed per project.

Why a third stage: Unpaywall and PMC handle published open access. This one catches a preprint of a
paper that is paywalled, or a paper that is only a preprint. It runs only the servers the project
opted into (plan DEC-31): projects.json `projects.<key>.sources` (config names `arxiv`, `biorxiv`,
`medrxiv`, `osf`, `sportrxiv`, `europepmc_preprints`); with no `sources` key a project gets
`unpaywall` and `pmc` only, so this stage then sends nothing for an ordinary row. The one exception
(plan DEC-09): a row carrying an arXiv ID or a `10.48550/arXiv.` DOI is looked up on arXiv whatever
the project's sources. `--sources` replaces the project's list for one run. Without `--project` the
project is found by matching `--lib-dir` against the registry; when nothing matches, the default
sources apply and one line says so.

Every request goes through litpipe.net (host policy, pacing, budgets, refusals, ledger, redaction).
Routes, per server:
  arXiv       IDs batched through `id_list` on https://export.arxiv.org/api/query (3.5 s, one
              connection); a title search (`ti:"..."`) only for projects whose sources include
              `arxiv`. A 406 or 429 refuses export.arxiv.org with manual persistence (the host row);
              every arXiv row after that is REFUSED `host_refused:export.arxiv.org`, never NO_MATCH,
              and nothing more is sent until `python -m litpipe.state --clear-refusal
              export.arxiv.org`. PDFs from arxiv.org/pdf/<id> only when projects.json
              `hosts.arxiv_pdf_allowed` is true (default false; DEC-09); otherwise the row is a
              manual preprint with its abstract-page link.
  bioRxiv /   the server comes from Europe PMC's `bookOrReportDetails.publisher` (not the DOI prefix:
  medRxiv     10.1101 and 10.64898 both cover both servers); metadata from
              api.biorxiv.org/details/{server}/{doi} (works for 10.64898; an empty body or an empty
              collection is OUTAGE); full text from Europe PMC's REST fullTextXML for the PPR record
              when Europe PMC marks it open access (written as a text-only `.fulltext.json`, no PDF);
              PDFs from www.biorxiv.org / www.medrxiv.org only when `hosts.biorxiv_pdf_allowed`
              (default false; DEC-10); otherwise a manual preprint with its doi.org link.
  Europe PMC  `(<title keywords>) AND SRC:PPR` (never an empty keyword group); an `errCode` inside a
              200 is ERROR. Gated by `europepmc_preprints` for servers without a source key of their
              own, and by `biorxiv`/`medrxiv` for those servers' rows. The europepmc.org website
              (`?pdf=render`, fulltextRepo) is never contacted.
  OSF         api.osf.io/v2/preprints/?filter[title]=<title prefix> (a case-insensitive substring
              filter: trailing punctuation misses), then the file record files/{primary_file}/ and
              its `data.links.download` (osf.io -> files.*.osf.io -> storage.googleapis.com). The
              preprint's DOI is `links.preprint_doi`. 100 requests/hour anonymous: paced at 36 s,
              2,400 a day (the host row); a spent budget is DEFERRED. SportRxiv preprints posted
              before 2021-08-27 live here (provider `sportrxiv`).
  SportRxiv   (PKP OPS since 2021) a daily OAI-PMH ListRecords cache under state_dir (at most one
              harvest request per UTC day), a Crossref lookup under prefix 10.51224
              (/prefixes/10.51224/works, or /works/{doi} for a 10.51224 row), the galley from OAI
              (the cache, else GetRecord), then /preprint/download/{id}/{galley}.

Identity (replaces the DOI-mismatch quarantine): a fetched PDF is judged with
unpaywall_fetch_v2.judge_identity against the preprint's own DOI and title, then the queue's; the
verdict is written to `<stem>.identity.json` (source "preprint"). A FLAG, or a supplement, keeps the
file where it is, writes no `.ris`, and reports `identity=FLAG` with a `DOI_MISMATCH:` status. No
file is ever moved or deleted. A file already in the library counts only when existing_holds says
it is this paper ("same"); a file with no readable DOI is not a holding.

Each row has a wall-clock deadline (`--row-deadline`, default 90 s): a row that reaches it is
DEFERRED `row_deadline:<n>s`. Progress lines are flushed.

Report: the legacy columns (doi, title, year, preprint_filename, found, source, match_id,
similarity, downloaded, skipped, status; sweep and migrate read `status`, `found`, `downloaded`,
`skipped`) plus the typed ones: first_status, route, outcome (a litpipe Kind: the first root cause),
detail, identity, release_date, landing_url, and doc_kind, sidecar, sidecar_status. Every legacy
`status` maps through litpipe.outcomes.from_legacy(status, "preprint") to the row's `outcome`,
except SKIPPED `source_excluded:<source>` (no legacy token names it; the status is
`SOURCE_EXCLUDED:<source>`). Every string written passes litpipe.ledger.redact.

Exit codes: 0 done; 1 triage CSV missing; 2 CONFIG (an unregistered --project, a bad --sources).

Usage:
  python preprint_fetch.py --triage residual.csv --lib-dir references/literature \\
      [--project KEY] [--sources arxiv,biorxiv] [--report out.csv] [--row-deadline 90] [--dry-run]
"""
import argparse
import csv
import io
import json
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from urllib.parse import quote
from xml.etree import ElementTree as ET

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import jats_to_text  # noqa: E402  (fetch_jats_outcome: fullTextXML with its 500 sent once)
import lit_net  # noqa: E402  (MAX_PDF_BYTES: the one download cap)
import lit_util  # noqa: E402
import ris_emit as _R  # noqa: E402
# T5a (2026-06-25 audit): the boilerplate fingerprint is the SAME object the Unpaywall stage uses
# (tests/test_fetch_chain.py pins it). RC2: the collision-safe destination; DEC-15: one slug writer;
# W2-B: the identity check that replaced the quarantine.
from unpaywall_fetch_v2 import (Attempt, MIN_PDF_BYTES, _flagged, _pdf_text, boilerplate_of,  # noqa: E402
                                build_filename, existing_holds, is_known_boilerplate, judge_identity,
                                legacy_build_filename, resolve_dest, write_identity_sidecar)
from litpipe import config, ledger, net  # noqa: E402
from litpipe import doi as _doi  # noqa: E402
from litpipe import text as _text  # noqa: E402
from litpipe.hosts import ProhibitedHost  # noqa: E402
from litpipe.outcomes import Kind, Outcome, from_legacy  # noqa: E402

lit_util.utf8_stdout()

ARXIV_HOST = "export.arxiv.org"
ARXIV_API = f"https://{ARXIV_HOST}/api/query"
ARXIV_ABS = "https://arxiv.org/abs/{id}"
ARXIV_PDF = "https://arxiv.org/pdf/{id}"                  # only when hosts.arxiv_pdf_allowed
EPMC_SEARCH = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
OSF_API = "https://api.osf.io/v2/"
BIORXIV_DETAILS = "https://api.biorxiv.org/details/{server}/{doi}/na/json"
BIORXIV_PDF = "https://www.{server}.org/content/{doi}v{version}.full.pdf"   # only when hosts.biorxiv_pdf_allowed
SPORTRXIV_OAI = "https://sportrxiv.org/index.php/server/oai"
SPORTRXIV_OAI_ID = "oai:ojs.scholarsportal.info:preprint/{id}"
SPORTRXIV_DOWNLOAD = "https://sportrxiv.org/index.php/server/preprint/download/{id}/{galley}"
CROSSREF_51224_WORKS = "https://api.crossref.org/prefixes/10.51224/works"
CROSSREF_WORK = "https://api.crossref.org/works/{doi}"

ARXIV_NS = {"a": "http://www.w3.org/2005/Atom", "ax": "http://arxiv.org/schemas/atom"}
OAI_NS = {"o": "http://www.openarchives.org/OAI/2.0/", "dc": "http://purl.org/dc/elements/1.1/",
          "oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/"}

PREPRINT_SOURCES = ("europepmc_preprints", "biorxiv", "medrxiv", "osf", "sportrxiv", "arxiv")
OPENRXIV_PREFIXES = ("10.1101/", "10.64898/")      # bioRxiv AND medRxiv (10.64898 from 2025-12-01)
OSF_PREFIXES = ("10.31219/", "10.31234/", "10.31235/", "10.31236/", "10.35542/", "10.31222/",
                "10.31221/", "10.31730/", "10.33767/")
SPORTRXIV_PREFIX = "10.51224/"
# Europe PMC `bookOrReportDetails.publisher` -> config source name (anything else: europepmc_preprints)
_PUBLISHER_SOURCE = {"biorxiv": "biorxiv", "medrxiv": "medrxiv", "arxiv": "arxiv", "sportrxiv": "sportrxiv",
                     "sportrχiv": "sportrxiv", "psyarxiv": "osf", "socarxiv": "osf", "edarxiv": "osf",
                     "engrxiv": "osf", "metaarxiv": "osf", "lawarxiv": "osf", "mindrxiv": "osf",
                     "africarxiv": "osf", "osf preprints": "osf", "biohackrxiv": "osf", "marxiv": "osf",
                     "paleorxiv": "osf", "thesis commons": "osf"}

DEFAULT_REPORT = "data/prior_art/discovered/preprint_fetch_report.csv"
ROW_DEADLINE_S = 90.0
MAX_YEAR_DELTA = 2
ARXIV_BATCH = 50                    # ids per id_list request
EXACT_SIM = 0.97                    # a candidate this close ends the title search for the row
SPORTRXIV_FIRST_HARVEST_DAYS = 90   # the cache's first ListRecords window (Crossref covers older ones)
EXIT_OK, EXIT_NO_TRIAGE, EXIT_CONFIG = 0, 1, 2

LEGACY_FIELDS = ["doi", "title", "year", "preprint_filename", "found", "source", "match_id",
                 "similarity", "downloaded", "skipped", "status"]
TYPED_FIELDS = ["first_status", "route", "outcome", "detail", "identity", "release_date", "landing_url"]
REPORT_FIELDS = LEGACY_FIELDS + TYPED_FIELDS + ["doc_kind", "sidecar", "sidecar_status"]


# ---------------------------------------------------------------- titles and names
def norm_title(t):
    t = _text.comparison_fold(t or "")      # Greek letters, quotes and dashes compare alike (W5 item 25)
    t = re.sub(r"<[^>]+>", "", t)
    t = re.sub(r"[^a-zA-Z0-9 ]+", " ", t).lower()
    return re.sub(r"\s+", " ", t).strip()


def title_similarity(a, b):
    return SequenceMatcher(None, norm_title(a), norm_title(b)).ratio()


def slug_filename(year, author, title):
    """The preprint's filename: the one stem writer (DEC-15, unpaywall_fetch_v2.build_filename)
    plus `_preprint`."""
    return build_filename(year, author, title)[:-4] + "_preprint.pdf"


def legacy_slug_filename(year, author, title):
    """The name the old preprint writer produced (14-word stoplist, first-token surname): files
    already named by it are recognised, never renamed."""
    return legacy_build_filename(year, author, title)[:-4] + "_preprint.pdf"


def _keywordize(title):
    """Significant title words for a Europe PMC keyword group, cut at a word boundary (at most 200
    characters). '' when nothing is left: the caller then sends nothing."""
    skip = {"a", "an", "the", "of", "in", "on", "and", "or", "not", "to", "for", "at", "from", "with",
            "by", "as", "is", "are", "was", "were", "be", "been", "this", "that", "these", "those"}
    words = [w for w in re.findall(r"[a-zA-Z0-9]{3,}", (title or "").lower()) if w not in skip]
    out = []
    for w in words:
        if len(" ".join(out + [w])) > 200:
            break
        out.append(w)
    return " ".join(out)


def _title_prefix(title, max_chars=60):
    """A title prefix for OSF's substring filter: whole words, no trailing punctuation (V4-O3)."""
    t = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", title or "")).strip()
    if len(t) > max_chars:
        cut = t[:max_chars]
        t = cut[:cut.rfind(" ")] if " " in cut else cut
    return re.sub(r"[^\w]+$", "", t).strip()


def _arxiv_phrase(title, max_chars=200):
    """A title for `ti:"..."`: no embedded quotes or parentheses, cut at a word boundary (N-D5)."""
    t = re.sub(r"[\"()]", " ", re.sub(r"<[^>]+>", "", title or ""))
    t = re.sub(r"\s+", " ", t).strip()
    if len(t) > max_chars:
        cut = t[:max_chars]
        t = cut[:cut.rfind(" ")] if " " in cut else cut
    return t.strip()


# ---------------------------------------------------------------- arXiv identifiers
ARXIV_DOI_RE = re.compile(r"10\.48550/arxiv\.(.+)$", re.IGNORECASE)
_ARXIV_ID_RE = re.compile(r"^(?:\d{4}\.\d{4,5}|[a-z][a-z.\-]*/\d{7})(?:v\d+)?$", re.IGNORECASE)
_ARXIV_REF_RE = re.compile(r"(?:^arxiv:\s*|arxiv\.org/(?:abs|pdf)/)([^\s?#]+?)(?:\.pdf)?/?$", re.IGNORECASE)


def arxiv_id(doi):
    """The arXiv ID embedded in a 10.48550/arXiv.<id> DOI (lowercased), or '' if not an arXiv DOI."""
    m = ARXIV_DOI_RE.search((doi or "").strip().lower())
    return m.group(1) if m else ""


def arxiv_id_of(row):
    """The arXiv ID a queue row carries: a 10.48550/arXiv.<id> DOI, an `arXiv:<id>` or an
    arxiv.org/abs/<id> link in the doi field, or an `arxiv_id` column. '' when none (or malformed)."""
    for raw in (row.get("doi"), row.get("arxiv_id")):
        s = (raw or "").strip()
        if not s:
            continue
        aid = arxiv_id(s)
        if not aid:
            m = _ARXIV_REF_RE.search(s)
            aid = m.group(1).lower() if m else (s.lower() if _ARXIV_ID_RE.match(s) else "")
        if aid and _ARXIV_ID_RE.match(aid):
            return aid
    return ""


def arxiv_base(aid):
    """The ID without its version suffix (`2103.00020v2` -> `2103.00020`; old style kept whole)."""
    return re.sub(r"v\d+$", "", (aid or "").strip().lower())


def arxiv_match_by_doi(doi, title):
    """A direct match for a 10.48550/arXiv.<id> DOI (exact ID, sim 1.0), or None. Kept for callers
    of the old shortcut; the stage itself looks the ID up through the batched `id_list` call."""
    aid = arxiv_id(doi)
    if not aid:
        return None
    return {"source": "arxiv-id", "id": aid, "sim": 1.0, "title": title,
            "doi": doi.strip().lower(), "pdf_url": ARXIV_PDF.format(id=aid)}


def parse_arxiv_feed(body):
    """Atom 1.0 from the arXiv API -> list of entries {id, base, title, year, authors, doi,
    error}. An error entry (id under /api/errors) carries `error`. Raises ValueError when the body
    is not an Atom feed. The id keeps everything after /abs/ (the old-style `hep-ex/0307015v1`
    contains a slash, N-D3)."""
    try:
        root = ET.fromstring(body)
    except ET.ParseError as e:
        raise ValueError(f"not XML: {e}") from None
    if root.tag != "{http://www.w3.org/2005/Atom}feed":
        raise ValueError(f"not an Atom feed (root {root.tag})")
    out = []
    for entry in root.findall("a:entry", ARXIV_NS):
        id_text = (entry.findtext("a:id", "", ARXIV_NS) or "").strip()
        if "/api/errors" in id_text:
            out.append({"error": " ".join((entry.findtext("a:summary", "", ARXIV_NS) or
                                            entry.findtext("a:title", "", ARXIV_NS) or "error").split())})
            continue
        m = re.search(r"/abs/(.+?)/?$", id_text)
        if not m:
            continue
        aid = m.group(1).strip()
        title = " ".join((entry.findtext("a:title", "", ARXIV_NS) or "").split())
        published = (entry.findtext("a:published", "", ARXIV_NS) or "").strip()
        authors = [" ".join((a.findtext("a:name", "", ARXIV_NS) or "").split())
                   for a in entry.findall("a:author", ARXIV_NS)]
        jdoi = _doi.normalise(entry.findtext("ax:doi", "", ARXIV_NS) or "")   # placeholders dropped
        out.append({"id": aid, "base": arxiv_base(aid), "title": title, "year": published[:4],
                    "authors": "; ".join(a for a in authors if a), "journal_doi": jdoi or ""})
    return out


# ---------------------------------------------------------------- shared helpers
def is_pdf(b):
    return (b or b"")[:4] == b"%PDF"


def _st(state):
    """The state object litpipe.net uses (the injected one, net.STATE, else litpipe.state)."""
    if state is not None:
        return state
    if net.STATE is not None:
        return net.STATE
    import litpipe.state as s
    return s


def _refused(host, state):
    return bool(_st(state).is_refused(host))


def _host_refused(host):
    return Outcome(Kind.REFUSED, host=host, detail=f"host_refused:{host}")


def _today(clock=None):
    return datetime.fromtimestamp((clock or net.CLOCK).time(), timezone.utc).strftime("%Y-%m-%d")


def _doi_url(doi):
    """https://doi.org/ + the DOI normalised and percent-encoded (DOI Handbook 2025 4.7, through
    litpipe.doi.encode_path, as ris_emit and the Unpaywall stage do); '' when `doi` holds no DOI."""
    try:
        return "https://doi.org/" + _doi.encode_path(doi) if doi else ""
    except ValueError:
        return ""


def _year_of(v):
    m = re.search(r"\b(1[89]\d\d|20\d\d)\b", str(v or ""))
    return m.group(1) if m else ""


def _source_of_publisher(publisher):
    p = (publisher or "").strip().lower()
    return _PUBLISHER_SOURCE.get(p, "europepmc_preprints")


def _doi_server_hint(doi):
    """The config source a DOI prefix suggests, for a `source_excluded` row that sends nothing
    (a hint only: the server itself comes from Europe PMC's publisher)."""
    d = (doi or "").lower()
    if d.startswith("10.48550/"):
        return "arxiv"
    if d.startswith(OPENRXIV_PREFIXES):
        return "biorxiv"
    if d.startswith(SPORTRXIV_PREFIX):
        return "sportrxiv"
    if d.startswith(OSF_PREFIXES):
        return "osf"
    return ""


def _expect_atom(p):
    """Validator for the arXiv API: an Atom body. A 200 saying "Rate exceeded" is a refusal."""
    body = (p.content or b"").strip()
    if not body:
        return (Kind.OUTAGE, "empty body")
    if b"rate exceeded" in body[:400].lower():
        return (Kind.REFUSED, "Rate exceeded")
    if not body.startswith(b"<"):
        return (Kind.ERROR, "not Atom")
    return None


def _download(url, *, state, cfg, purpose):
    """GET a PDF through litpipe.net (stream, the one size cap, `%PDF` validated). A prohibited
    route is never sent: NOT_AVAILABLE with detail `PROHIBITED <rule>` (as the Unpaywall stage)."""
    try:
        return net.get(url, headers={"Accept": "application/pdf,*/*"}, stream=True,
                       max_bytes=lit_net.MAX_PDF_BYTES, validate=net.expect_pdf, state=state, cfg=cfg,
                       purpose=purpose)
    except ProhibitedHost as e:
        return Outcome(Kind.NOT_AVAILABLE, detail=f"PROHIBITED {e.rule.host}{e.rule.path_prefix} (never automated)",
                       host=e.rule.host)
    except ValueError as e:                  # not an http(s) URL
        return Outcome(Kind.ERROR, detail=ledger.redact(str(e))[:200])


def _write_bytes(dest, content):
    tmp = f"{dest}.part"
    with open(tmp, "wb") as f:
        f.write(content)
    os.replace(tmp, dest)


def fetch_pdf(url, dest, timeout=30):
    """Legacy adapter: GET `url` through litpipe.net and write it to `dest` when it is a PDF of at
    least MIN_PDF_BYTES that is not known boilerplate. Returns (ok, status): (True, "OK_<n>B"),
    (False, "NOT_PDF" | "HTTP_<code>" | "TOO_LARGE" | "TOO_SMALL_<n>B" | "BOILERPLATE_<tag>" |
    "ERR_<detail>"). `timeout` is accepted for old callers; litpipe.net's (10 s, 30 s) applies."""
    try:
        o = _download(url, state=None, cfg=None, purpose="preprint: fetch_pdf")
    except Exception as e:  # noqa: BLE001  (the adapter never raises, as before)
        return False, f"ERR_{ledger.redact(str(e))[:80]}"
    p = o.payload
    if o.ok and p is not None and p.content is not None:
        content = p.content
        if len(content) < MIN_PDF_BYTES:
            return False, f"TOO_SMALL_{len(content)}B"
        _write_bytes(dest, content)
        is_bp, tag = is_known_boilerplate(dest)
        if is_bp:
            os.remove(dest)           # our own just-written copy, never a library file
            return False, f"BOILERPLATE_{tag or ''}"
        return True, f"OK_{len(content)}B"
    if p is not None and p.truncated:
        return False, "TOO_LARGE"
    if o.status and 200 <= o.status < 300:
        return False, "NOT_PDF"
    if o.status:
        return False, f"HTTP_{o.status}"
    return False, f"ERR_{ledger.redact(o.detail or str(o.kind))[:80]}"


# ---------------------------------------------------------------- candidates
@dataclass
class Candidate:
    """One preprint a discovery route found for a row."""
    server: str                 # config source name: arxiv, biorxiv, medrxiv, osf, sportrxiv, europepmc_preprints
    route: str                  # the discovery route
    id: str = ""                # arXiv id, Europe PMC PPR id, OSF preprint id, SportRxiv DOI
    title: str = ""
    year: str = ""
    authors: str = ""
    doi: str = ""               # the preprint's own DOI (normalised)
    landing_url: str = ""
    sim: float = 0.0
    ppr: str = ""               # Europe PMC PPR id (fullTextXML)
    epmc_oa: bool = False       # Europe PMC marks it open access
    osf_file: str = ""          # OSF primary file id
    preprint_num: str = ""      # SportRxiv preprint number
    galley: str = ""            # SportRxiv galley id
    server_known: bool = True   # False: an openRxiv DOI whose server Europe PMC did not name
    exact: bool = False         # matched by an identifier (arXiv id, DOI), not by title: no year filter
    year_delta: int = 999
    year_match: bool = False

    @property
    def direct_pdf(self):
        return self.server in ("osf", "sportrxiv")


def _epmc_candidate(rec):
    doi = _doi.normalise(rec.get("doi") or "") or ""
    ppr = rec.get("id") if rec.get("source") == "PPR" else ""
    ft_ids = ((rec.get("fullTextIdList") or {}).get("fullTextId") or [])
    oa = (rec.get("isOpenAccess") == "Y") or bool(ppr and ppr in ft_ids)
    server = _source_of_publisher((rec.get("bookOrReportDetails") or {}).get("publisher"))
    return Candidate(server=server, route="epmc_search", id=ppr or rec.get("id") or "",
                     title=rec.get("title") or "", year=str(rec.get("pubYear") or ""),
                     authors=rec.get("authorString") or "", doi=doi,
                     landing_url=_doi_url(doi), ppr=ppr or "", epmc_oa=oa)


# ---------------------------------------------------------------- Europe PMC
def epmc_keyword_query(title):
    kws = _keywordize(title)
    return f"({kws}) AND SRC:PPR" if kws else ""


def epmc_doi_query(doi):
    return f'DOI:"{doi}" AND SRC:PPR'


def epmc_preprint_search(query, *, state=None, cfg=None, n=5):
    """Europe PMC REST search -> Outcome (payload: list of Candidate). An `errCode` in a 200 is
    ERROR, never an empty answer; hitCount 0 is NO_MATCH. An empty query is never sent."""
    if not (query or "").strip():
        return Outcome(Kind.ERROR, detail="empty Europe PMC query (not sent)")
    o = net.get(EPMC_SEARCH, params={"query": query, "format": "json", "resultType": "core", "pageSize": n},
                validate=net.expect_json, state=state, cfg=cfg, purpose="preprint: Europe PMC search")
    if not o.ok:
        return o
    try:
        j = o.payload.json()
    except ValueError as e:
        return Outcome(Kind.ERROR, status=o.status, host=o.host, attempts=o.attempts, detail=f"JSON: {e}")
    if j.get("errCode") is not None or "resultList" not in j:
        return Outcome(Kind.ERROR, status=o.status, host=o.host, attempts=o.attempts,
                       detail=f"Europe PMC errCode {j.get('errCode')}: {j.get('errMsg', '')}"[:200])
    cands = [_epmc_candidate(r) for r in (j["resultList"].get("result") or []) if isinstance(r, dict)]
    return Outcome(Kind.OK if cands else Kind.NO_MATCH, status=o.status, host=o.host, attempts=o.attempts,
                   detail="" if cands else "Europe PMC: no preprint", payload=cands)


# ---------------------------------------------------------------- arXiv
def arxiv_lookup_ids(ids, *, state=None, cfg=None):
    """{base id: Outcome} for arXiv IDs, batched through `id_list` (ARXIV_BATCH per request). OK
    carries the entry dict; NO_MATCH when the feed has no entry for the ID; every ID of a failed
    batch gets that call's kind. Nothing is sent while export.arxiv.org is refused."""
    out = {}
    uniq = list(dict.fromkeys(arxiv_base(a) for a in ids if a))
    for i in range(0, len(uniq), ARXIV_BATCH):
        batch = uniq[i:i + ARXIV_BATCH]
        if _refused(ARXIV_HOST, state):
            for a in batch:
                out[a] = _host_refused(ARXIV_HOST)
            continue
        o = net.get(ARXIV_API, params={"id_list": ",".join(batch), "max_results": len(batch)},
                    validate=_expect_atom, state=state, cfg=cfg, purpose="preprint: arXiv id_list")
        o = _arxiv_refusal(o, state)
        if not o.ok:
            for a in batch:
                out[a] = o
            continue
        try:
            entries = parse_arxiv_feed(o.payload.content)
        except ValueError as e:
            bad = Outcome(Kind.ERROR, status=o.status, host=ARXIV_HOST, attempts=o.attempts, detail=str(e)[:200])
            for a in batch:
                out[a] = bad
            continue
        errors = [e["error"] for e in entries if "error" in e]
        by_base = {e["base"]: e for e in entries if "error" not in e}
        for a in batch:
            e = by_base.get(a)
            if e is not None:
                out[a] = Outcome(Kind.OK, status=o.status, host=ARXIV_HOST, attempts=o.attempts, payload=e)
            elif errors:
                out[a] = Outcome(Kind.ERROR, status=o.status, host=ARXIV_HOST, attempts=o.attempts,
                                 detail=f"arXiv error entry: {errors[0]}"[:200])
            else:
                out[a] = Outcome(Kind.NO_MATCH, status=o.status, host=ARXIV_HOST, attempts=o.attempts,
                                 detail="arXiv has no entry for this id")
    return out


def _arxiv_refusal(o, state):
    """A "Rate exceeded" 200 refuses export.arxiv.org like a 406/429 does (manual persistence)."""
    if o.kind is Kind.REFUSED and o.status == 200 and "Rate exceeded" in (o.detail or ""):
        _st(state).refuse(ARXIV_HOST, "manual: arXiv answered 'Rate exceeded'", persistence="manual")
    return o


def arxiv_title_search(title, *, state=None, cfg=None, n=3):
    """arXiv `ti:"<title>"` -> Outcome (payload: list of Candidate). REFUSED with nothing sent while
    export.arxiv.org is refused."""
    if _refused(ARXIV_HOST, state):
        return _host_refused(ARXIV_HOST)
    q = _arxiv_phrase(title)
    if not q:
        return Outcome(Kind.NO_MATCH, host=ARXIV_HOST, detail="no searchable title (not sent)")
    o = net.get(ARXIV_API, params={"search_query": f'ti:"{q}"', "max_results": n}, validate=_expect_atom,
                state=state, cfg=cfg, purpose="preprint: arXiv title search")
    o = _arxiv_refusal(o, state)
    if not o.ok:
        return o
    try:
        entries = parse_arxiv_feed(o.payload.content)
    except ValueError as e:
        return Outcome(Kind.ERROR, status=o.status, host=ARXIV_HOST, attempts=o.attempts, detail=str(e)[:200])
    errors = [e["error"] for e in entries if "error" in e]
    if errors and len(errors) == len(entries):
        return Outcome(Kind.ERROR, status=o.status, host=ARXIV_HOST, attempts=o.attempts,
                       detail=f"arXiv error entry: {errors[0]}"[:200])
    cands = [_arxiv_candidate(e, "arxiv_search") for e in entries if "error" not in e]
    return Outcome(Kind.OK if cands else Kind.NO_MATCH, status=o.status, host=ARXIV_HOST,
                   attempts=o.attempts, payload=cands)


def _arxiv_candidate(e, route):
    return Candidate(server="arxiv", route=route, id=e["id"], title=e["title"], year=e["year"],
                     authors=e["authors"], doi=f"10.48550/arxiv.{e['base']}",
                     landing_url=ARXIV_ABS.format(id=e["id"]))


# ---------------------------------------------------------------- bioRxiv / medRxiv
def biorxiv_details(server, doi, *, state=None, cfg=None):
    """api.biorxiv.org/details/{server}/{doi}/na/json -> Outcome (payload: the latest version's
    record). Every answer is a 200, so the kind comes from the body: an empty body (the
    2026-09-24..28 outage) or an empty collection is OUTAGE."""
    try:
        url = BIORXIV_DETAILS.format(server=server, doi=_doi.encode_path(doi))
    except ValueError:
        return Outcome(Kind.ERROR, host="api.biorxiv.org", detail=f"not a DOI: {doi!r} (not sent)")
    o = net.get(url, state=state, cfg=cfg, purpose=f"preprint: {server} details")
    if not o.ok:
        return o
    body = (o.payload.content or b"").strip()
    if not body:
        return Outcome(Kind.OUTAGE, status=o.status, host=o.host, attempts=o.attempts,
                       detail="api.biorxiv.org /details: empty body")
    try:
        j = json.loads(body)
    except ValueError:
        return Outcome(Kind.OUTAGE, status=o.status, host=o.host, attempts=o.attempts,
                       detail="api.biorxiv.org /details: body is not JSON")
    coll = [c for c in (j.get("collection") or []) if isinstance(c, dict)] if isinstance(j, dict) else []
    if not coll:
        msgs = "; ".join(str(m.get("status") or "") for m in (j.get("messages") or []) if isinstance(m, dict))
        return Outcome(Kind.OUTAGE, status=o.status, host=o.host, attempts=o.attempts,
                       detail=f"api.biorxiv.org /details: empty collection ({msgs or 'no message'})")
    latest = max(coll, key=lambda c: int(c["version"]) if str(c.get("version") or "").isdigit() else 0)
    return Outcome(Kind.OK, status=o.status, host=o.host, attempts=o.attempts, payload=latest)


def _no_posts(o):
    """/details' answer for a DOI that server does not hold: an empty collection, "no posts found"."""
    return o.kind is Kind.OUTAGE and "no posts found" in (o.detail or "").lower()


# ---------------------------------------------------------------- OSF
def osf_title_search(title, *, state=None, cfg=None, n=5):
    """api.osf.io/v2/preprints/?filter[title]=<prefix> -> Outcome (payload: list of Candidate)."""
    prefix = _title_prefix(title)
    if not prefix:
        return Outcome(Kind.NO_MATCH, host="api.osf.io", detail="no searchable title (not sent)")
    o = net.get(OSF_API + "preprints/", params={"filter[title]": prefix, "page[size]": n},
                validate=net.expect_json, state=state, cfg=cfg, purpose="preprint: OSF title search")
    if not o.ok:
        return o
    try:
        data = o.payload.json().get("data") or []
    except (ValueError, AttributeError) as e:
        return Outcome(Kind.ERROR, status=o.status, host=o.host, attempts=o.attempts, detail=f"JSON: {e}")
    cands = []
    for rec in data[:n]:
        if not isinstance(rec, dict):
            continue
        attrs = rec.get("attributes") or {}
        rel = rec.get("relationships") or {}
        links = rec.get("links") or {}
        pf = ((rel.get("primary_file") or {}).get("data") or {}).get("id") or ""
        cands.append(Candidate(server="osf", route="osf_search", id=rec.get("id") or "",
                               title=attrs.get("title") or "", year=(attrs.get("date_published") or "")[:4],
                               doi=_doi.normalise(links.get("preprint_doi") or "") or "",
                               landing_url=links.get("html") or "", osf_file=pf))
    return Outcome(Kind.OK if cands else Kind.NO_MATCH, status=o.status, host=o.host, attempts=o.attempts,
                   payload=cands)


def osf_download(file_id, *, state=None, cfg=None):
    """The file record files/{id}/, then its `data.links.download` (3 hops to the blob)."""
    o = net.get(OSF_API + f"files/{quote(file_id, safe='')}/", validate=net.expect_json, state=state, cfg=cfg,
                purpose="preprint: OSF file record")
    if not o.ok:
        return o
    try:
        dl = ((o.payload.json().get("data") or {}).get("links") or {}).get("download") or ""
    except (ValueError, AttributeError):
        dl = ""
    if not dl:
        return Outcome(Kind.ERROR, status=o.status, host=o.host, attempts=o.attempts,
                       detail="OSF file record has no data.links.download")
    return _download(dl, state=state, cfg=cfg, purpose="preprint: OSF download")


# ---------------------------------------------------------------- SportRxiv
def parse_oai(xml_bytes):
    """OAI-PMH oai_dc -> (records, resumption_token, error_code). A record: {oai_id, num, doi,
    title, landing, galley, date, creators}; deleted records are skipped."""
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as e:
        raise ValueError(f"not XML: {e}") from None
    err = root.find("o:error", OAI_NS)
    if err is not None:
        return [], "", err.get("code") or "error"
    recs = []
    for r in root.iter("{http://www.openarchives.org/OAI/2.0/}record"):
        hdr = r.find("o:header", OAI_NS)
        if hdr is None or hdr.get("status") == "deleted":
            continue
        oai_id = (hdr.findtext("o:identifier", "", OAI_NS) or "").strip()
        dc = r.find(".//oai_dc:dc", OAI_NS)
        if dc is None:
            continue
        title = " ".join((dc.findtext("dc:title", "", OAI_NS) or "").split())
        ids = [(x.text or "").strip() for x in dc.findall("dc:identifier", OAI_NS)]
        doi = next((_doi.normalise(x) for x in ids if x.lower().startswith(SPORTRXIV_PREFIX)), None) or ""
        landing = next((x for x in ids if x.startswith("http")), "")
        num = galley = ""
        for rel in dc.findall("dc:relation", OAI_NS):
            m = re.search(r"/preprint/view/(\d+)/(\d+)", rel.text or "")
            if m:
                num, galley = m.group(1), m.group(2)
                break
        if not num:
            m = re.search(r"preprint/(\d+)$", oai_id) or re.search(r"/preprint/view/(\d+)", landing)
            num = m.group(1) if m else ""
        recs.append({"oai_id": oai_id, "num": num, "doi": doi, "title": title, "landing": landing,
                     "galley": galley, "date": (dc.findtext("dc:date", "", OAI_NS) or "").strip(),
                     "creators": "; ".join((c.text or "").strip() for c in dc.findall("dc:creator", OAI_NS))})
    tok = root.find(".//o:resumptionToken", OAI_NS)
    return recs, ((tok.text or "").strip() if tok is not None else ""), ""


class SportRxivIndex:
    """The daily OAI ListRecords cache (`<state_dir>/cache/sportrxiv_oai.json`): at most one harvest
    request per UTC day; a resumption token is kept for the next day's request. A dry run harvests
    into memory and writes nothing."""

    def __init__(self, cfg=None, state=None, dry_run=False):
        self.cfg, self.state, self.dry_run = cfg, state, dry_run
        self.path = config.state_dir(cfg, create=not dry_run) / "cache" / "sportrxiv_oai.json"
        self.data = None
        self.harvest = None          # the Outcome of today's harvest, when this run made it

    def load(self):
        if self.data is None:
            try:
                self.data = json.loads(self.path.read_text(encoding="utf-8"))
                if not isinstance(self.data, dict):
                    raise ValueError
            except (OSError, ValueError):
                self.data = {"version": 1, "records": {}}
        return self.data

    def refresh(self):
        """Harvest once per UTC day. Returns the harvest Outcome when a request was made now."""
        d = self.load()
        today = _today()
        if d.get("attempted_on") == today:
            return None
        params = {"verb": "ListRecords"}
        if d.get("token"):
            params["resumptionToken"] = d["token"]
        else:
            since = d.get("harvested_on") or (datetime.strptime(today, "%Y-%m-%d")
                                               - timedelta(days=SPORTRXIV_FIRST_HARVEST_DAYS)).strftime("%Y-%m-%d")
            params.update(metadataPrefix="oai_dc", **{"from": since})
        d["attempted_on"] = today
        o = net.get(SPORTRXIV_OAI, params=params, state=self.state, cfg=self.cfg, purpose="preprint: SportRxiv OAI harvest")
        if o.ok:
            try:
                recs, token, err = parse_oai(o.payload.content or b"")
            except ValueError as e:
                o = Outcome(Kind.ERROR, status=o.status, host=o.host, attempts=o.attempts, detail=str(e)[:200])
            else:
                if err and err != "noRecordsMatch":
                    d["token"] = ""          # e.g. badResumptionToken: start again from the date
                    o = Outcome(Kind.ERROR, status=o.status, host=o.host, attempts=o.attempts,
                                detail=f"OAI error {err}")
                else:
                    for r in recs:
                        if r["doi"]:
                            d["records"][r["doi"]] = r
                    d["token"] = token
                    if not token:
                        d["harvested_on"] = today
        self.harvest = o
        self._save()
        return o

    def _save(self):
        if self.dry_run:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        lit_util.atomic_write_text(str(self.path), json.dumps(self.data, ensure_ascii=False, indent=1))

    def match(self, title, min_sim):
        out = []
        for r in self.load().get("records", {}).values():
            s = title_similarity(title, r.get("title") or "")
            if s >= min_sim:
                out.append(_sportrxiv_candidate(r, "sportrxiv_oai", s))
        return out

    def by_doi(self, doi):
        return self.load().get("records", {}).get(_doi.normalise(doi or "") or "")


def _sportrxiv_candidate(r, route, sim=0.0):
    return Candidate(server="sportrxiv", route=route, id=r.get("doi") or "", title=r.get("title") or "",
                     year=_year_of(r.get("date")), authors=r.get("creators") or "", doi=r.get("doi") or "",
                     landing_url=r.get("landing") or _doi_url(r.get("doi")),
                     preprint_num=r.get("num") or "", galley=r.get("galley") or "", sim=sim)


def _crossref_candidate(it, route):
    doi = _doi.normalise(it.get("DOI") or "") or ""
    landing = ((it.get("resource") or {}).get("primary") or {}).get("URL") or ""
    m = re.search(r"/preprint/view/(\d+)", landing)
    issued = ((it.get("issued") or it.get("posted") or {}).get("date-parts") or [[None]])[0][0]
    return Candidate(server="sportrxiv", route=route, id=doi, title=" ".join((it.get("title") or [""])[0].split()),
                     year=str(issued or ""), doi=doi, landing_url=landing or _doi_url(doi),
                     preprint_num=m.group(1) if m else "")


def crossref_sportrxiv_search(title, *, state=None, cfg=None, n=3):
    """Crossref works under prefix 10.51224 (SportRxiv on PKP OPS) by bibliographic query."""
    o = net.get(CROSSREF_51224_WORKS, params={"query.bibliographic": (title or "")[:300], "rows": n,
                                              "select": "DOI,title,resource,issued,type"},
                validate=net.expect_json, state=state, cfg=cfg, purpose="preprint: Crossref 10.51224 search")
    if not o.ok:
        return o
    try:
        items = ((o.payload.json().get("message") or {}).get("items") or [])
    except (ValueError, AttributeError) as e:
        return Outcome(Kind.ERROR, status=o.status, host=o.host, attempts=o.attempts, detail=f"JSON: {e}")
    cands = [_crossref_candidate(it, "crossref_51224") for it in items if isinstance(it, dict)]
    return Outcome(Kind.OK if cands else Kind.NO_MATCH, status=o.status, host=o.host, attempts=o.attempts,
                   payload=cands)


def crossref_sportrxiv_work(doi, *, state=None, cfg=None):
    """Crossref /works/{doi} for a 10.51224 DOI -> Outcome (payload: Candidate, sim 1.0)."""
    o = net.get(CROSSREF_WORK.format(doi=_doi.encode_path(doi)), validate=net.expect_json, state=state,
                cfg=cfg, purpose="preprint: Crossref 10.51224 work")
    if not o.ok:
        return o
    try:
        msg = o.payload.json().get("message") or {}
    except (ValueError, AttributeError) as e:
        return Outcome(Kind.ERROR, status=o.status, host=o.host, attempts=o.attempts, detail=f"JSON: {e}")
    c = _crossref_candidate(msg, "crossref_51224")
    c.sim, c.exact = 1.0, True
    return Outcome(Kind.OK, status=o.status, host=o.host, attempts=o.attempts, payload=c)


def sportrxiv_galley(num, *, state=None, cfg=None):
    """OAI GetRecord for one preprint -> Outcome (payload: (num, galley)). idDoesNotExist is
    NO_MATCH; a record without a galley relation is NOT_AVAILABLE."""
    o = net.get(SPORTRXIV_OAI, params={"verb": "GetRecord", "metadataPrefix": "oai_dc",
                                        "identifier": SPORTRXIV_OAI_ID.format(id=num)},
                state=state, cfg=cfg, purpose="preprint: SportRxiv OAI GetRecord")
    if not o.ok:
        return o
    try:
        recs, _, err = parse_oai(o.payload.content or b"")
    except ValueError as e:
        return Outcome(Kind.ERROR, status=o.status, host=o.host, attempts=o.attempts, detail=str(e)[:200])
    if err:
        kind = Kind.NO_MATCH if err == "idDoesNotExist" else Kind.ERROR
        return Outcome(kind, status=o.status, host=o.host, attempts=o.attempts, detail=f"OAI error {err}")
    rec = recs[0] if recs else {}
    if not rec.get("galley"):
        return Outcome(Kind.NOT_AVAILABLE, status=o.status, host=o.host, attempts=o.attempts,
                       detail="SportRxiv record has no PDF galley")
    return Outcome(Kind.OK, status=o.status, host=o.host, attempts=o.attempts, payload=(rec["num"], rec["galley"]))


# ---------------------------------------------------------------- one row
class _Deadline(Exception):
    pass


class _Row:
    def __init__(self, raw, deadline_s):
        self.raw = raw
        raw_doi = (raw.get("doi") or "").strip()
        self.doi = _doi.normalise(raw_doi) or ""
        self.doi_col = self.doi or raw_doi.lower()
        self.title = (raw.get("title") or "").strip()
        self.year = (raw.get("year") or "").strip()
        self.authors = (raw.get("authors") or "").strip()
        self.aid = arxiv_id_of(raw)
        self.deadline_s = deadline_s
        self.deadline_at = net.CLOCK.monotonic() + deadline_s if deadline_s else None
        self.steps = []                       # (route, Outcome) in order
        self.rec = {k: "" for k in REPORT_FIELDS}
        self.rec.update(doi=self.doi_col, title=self.title[:80], year=self.year, found=False,
                        downloaded=False, skipped=False, sidecar=False)

    def check(self):
        if self.deadline_at is not None and net.CLOCK.monotonic() >= self.deadline_at:
            raise _Deadline()

    def add(self, route, o):
        self.steps.append((route, o))
        return o


def _legacy_status(kind, token):
    """A legacy status that from_legacy(status, "preprint") maps to `kind` (SKIPPED excepted:
    MANUAL_PREPRINT and DRY map; SOURCE_EXCLUDED has no legacy token)."""
    if kind is Kind.OK or kind is Kind.SKIPPED:
        return token
    if from_legacy(token, "preprint") is not kind:
        token = f"{token} [{kind}]"
    return token


def _token_of(o):
    """The legacy token for a failed Outcome."""
    if o.kind is Kind.REFUSED and not o.attempts:
        return f"HOST_REFUSED:{o.host}" if o.host else "HOST_REFUSED"
    if o.kind is Kind.NO_MATCH:
        return "NO_MATCH"
    if o.kind is Kind.DEFERRED:
        return "DEFERRED"
    if o.kind is Kind.TRANSPORT:
        return "TRANSPORT"
    if o.status and o.status != 200:
        return f"HTTP_{o.status}"
    if o.kind is Kind.OUTAGE:
        return "OUTAGE"
    if o.kind is Kind.REFUSED:
        return "NOT_PDF" if o.status == 200 else "REFUSED"
    return str(o.kind)


def _finish(row, kind, route, token, detail="", status=None):
    rec = row.rec
    rec["outcome"], rec["route"] = str(kind), route
    rec["status"] = _legacy_status(kind, token)
    rec["detail"] = ledger.redact(detail or "")
    if status:
        rec["first_status"] = str(status)
    return rec


def _fail(row, route, o, *, detail=None):
    """Finish a row with a failed Outcome as its root cause."""
    d = detail if detail is not None else (
        f"host_refused:{o.host}" if (o.kind is Kind.REFUSED and o.host and
                                     ("refused" in (o.detail or "") or not o.attempts)) else o.detail)
    return _finish(row, o.kind, route, _token_of(o), d, o.status)


@dataclass
class _Ctx:
    lib_dir: str
    sources: set
    min_similarity: float = 0.80
    dry_run: bool = False
    no_write_ris: bool = False
    state: object = None
    cfg: dict = None
    arxiv_pdf: bool = False
    biorxiv_pdf: bool = False
    arxiv_ids: dict = field(default_factory=dict)
    sportrxiv: object = None
    existing: set = field(default_factory=set)
    written_this_run: set = field(default_factory=set)
    counts: dict = field(default_factory=dict)

    def enabled(self, source):
        return source in self.sources


def _held(ctx, row):
    """The library file that already is this row's paper ("same"), else None (REG-I11: a file
    with no readable DOI and no title match is not a holding)."""
    for name in dict.fromkeys((slug_filename(row.year, row.authors, row.title),
                               legacy_slug_filename(row.year, row.authors, row.title))):
        if name in ctx.existing and existing_holds(os.path.join(ctx.lib_dir, name), row.doi, row.title) == "same":
            return name
    return None


def _score(row, c, title_based=True):
    if title_based and c.sim < 1.0:
        c.sim = title_similarity(row.title, c.title)
    if row.doi and c.doi and c.doi == row.doi:
        c.sim, c.exact = 1.0, True
    ty, cy = _year_of(row.year), _year_of(c.year)
    if ty and cy:
        c.year_delta = abs(int(cy) - int(ty))
        c.year_match = c.year_delta == 0
    return c


def _sort_key(c):
    return (c.sim, c.year_match, c.direct_pdf)


def discover(ctx, row):
    """(enabled candidates, excluded candidates, ran, hint): every lookup is added to row.steps."""
    cands, excluded = [], []

    def take(c):
        (cands if ctx.enabled(c.server) else excluded).append(c)

    # a) arXiv ID (DEC-09: whatever the sources); the lookup ran before the row loop, batched
    if row.aid:
        o = row.add("arxiv_id", ctx.arxiv_ids.get(arxiv_base(row.aid))
                    or Outcome(Kind.ERROR, host=ARXIV_HOST, detail="arXiv id not looked up"))
        if o.ok:
            c = _arxiv_candidate(o.payload, "arxiv_id")
            c.sim, c.exact = 1.0, True
            cands.append(_score(row, c, title_based=False))
        return cands, excluded, True, "arxiv"

    hint = _doi_server_hint(row.doi)
    # b) a bioRxiv/medRxiv DOI: the server from Europe PMC's publisher
    if hint == "biorxiv":
        if not (ctx.enabled("biorxiv") or ctx.enabled("medrxiv")):
            return cands, excluded, False, hint
        row.check()
        o = row.add("epmc_doi", epmc_preprint_search(epmc_doi_query(row.doi), state=ctx.state, cfg=ctx.cfg))
        hits = [c for c in (o.payload or []) if o.ok and c.doi == row.doi]
        if hits:
            for c in hits:
                if c.server not in ("biorxiv", "medrxiv"):
                    c.server = "biorxiv"            # an openRxiv DOI is one of the two
                take(_score(row, c))
        elif o.kind in (Kind.OK, Kind.NO_MATCH):
            # Europe PMC does not hold it: /details decides between the enabled servers
            servers = [s for s in ("biorxiv", "medrxiv") if ctx.enabled(s)]
            cands.append(Candidate(server=servers[0], route="doi", id=row.doi, title=row.title, doi=row.doi,
                                   landing_url=_doi_url(row.doi), sim=1.0, server_known=False,
                                   exact=True))
        return cands, excluded, True, hint

    # c) a SportRxiv DOI: Crossref's record, then the OAI galley
    if hint == "sportrxiv":
        if not ctx.enabled("sportrxiv"):
            return cands, excluded, False, hint
        rec = ctx.sportrxiv.by_doi(row.doi) if ctx.sportrxiv else None
        if rec:
            c = _sportrxiv_candidate(rec, "sportrxiv_oai", 1.0)
            c.exact = True
            cands.append(c)
            return cands, excluded, True, hint
        row.check()
        o = row.add("crossref_51224", crossref_sportrxiv_work(row.doi, state=ctx.state, cfg=ctx.cfg))
        if o.ok:
            cands.append(_score(row, o.payload, title_based=False))
        return cands, excluded, True, hint

    # d) title searches, cheapest first; a near-exact enabled match ends the search
    ran = False
    if not row.title:
        return cands, excluded, ran, hint

    def done():
        return any(c.sim >= EXACT_SIM for c in cands)

    if ctx.enabled("europepmc_preprints") or ctx.enabled("biorxiv") or ctx.enabled("medrxiv"):
        q = epmc_keyword_query(row.title)
        ran = True
        if q:
            row.check()
            o = row.add("epmc_search", epmc_preprint_search(q, state=ctx.state, cfg=ctx.cfg))
            for c in (o.payload or []) if o.ok else []:
                take(_score(row, c))
        else:                       # never an empty keyword group (V1-N4): nothing is sent
            row.add("epmc_search", Outcome(Kind.NO_MATCH, host="www.ebi.ac.uk",
                                           detail="no searchable title words (not sent)"))
    if ctx.enabled("sportrxiv") and not done():
        ran = True
        row.check()
        h = ctx.sportrxiv.refresh()
        if h is not None and not h.ok:
            print(f"        [sportrxiv] OAI harvest {h.kind}: {ledger.redact(h.detail)[:80]}", flush=True)
        local = ctx.sportrxiv.match(row.title, ctx.min_similarity)
        for c in local:
            take(_score(row, c, title_based=False))
        if not local:
            row.check()
            o = row.add("crossref_51224", crossref_sportrxiv_search(row.title, state=ctx.state, cfg=ctx.cfg))
            for c in (o.payload or []) if o.ok else []:
                take(_score(row, c))
    if ctx.enabled("arxiv") and not done():
        ran = True
        row.check()
        o = row.add("arxiv_search", arxiv_title_search(row.title, state=ctx.state, cfg=ctx.cfg))
        for c in (o.payload or []) if o.ok else []:
            take(_score(row, c))
    if ctx.enabled("osf") and not done():
        ran = True
        row.check()
        o = row.add("osf_search", osf_title_search(row.title, state=ctx.state, cfg=ctx.cfg))
        for c in (o.payload or []) if o.ok else []:
            take(_score(row, c))
    return cands, excluded, ran, hint or "europepmc_preprints"


def _best(cands, min_sim):
    """The best candidate at or above `min_sim` (title matches more than MAX_YEAR_DELTA years from
    the queue year are dropped: a similar title from later or earlier work); None when none."""
    ok = [c for c in cands if c.exact or (c.sim >= min_sim and (c.year_delta <= MAX_YEAR_DELTA
                                                                or c.year_delta == 999))]
    return max(ok, key=_sort_key) if ok else None


def _root_failure(row):
    return next(((r, o) for r, o in row.steps if o.kind not in (Kind.OK, Kind.NO_MATCH, Kind.SKIPPED)), None)


# ---------------------------------------------------------------- acquisition
@dataclass
class _Got:
    kind: str                         # pdf | text | manual | fail
    route: str = ""
    outcome: Outcome = None
    content: bytes = None
    url: str = ""
    text: dict = None


def _pdf_got(route, o, url):
    """A download Outcome as a _Got. `url` is the address recorded on the artifact: the one we
    asked for, never the final hop (an OSF blob URL carries a signature and an expiry)."""
    if o.ok and o.payload is not None and o.payload.content is not None:
        return _Got("pdf", route, o, content=o.payload.content, url=url)
    if o.kind is Kind.NO_MATCH:      # a dead link to a file that was found: NOT_AVAILABLE
        o = Outcome(Kind.NOT_AVAILABLE, status=o.status, host=o.host, attempts=o.attempts, detail=o.detail)
    return _Got("fail", route, o)


def _epmc_text(ctx, row, c):
    """Europe PMC fullTextXML for the PPR record (its 500 is sent once: NOT_AVAILABLE)."""
    if not (c.ppr and c.epmc_oa):
        return None
    row.check()
    fo = row.add("epmc_fulltext", jats_to_text.fetch_jats_outcome(c.ppr))
    if not fo.ok:
        return None
    try:
        parsed = jats_to_text.parse_jats(fo.payload)
    except Exception as e:  # noqa: BLE001  (a parse failure must not lose the row)
        row.add("epmc_fulltext", Outcome(Kind.ERROR, host=fo.host, detail=f"parse_jats: {e}"[:200]))
        return None
    return _Got("text", "epmc_fulltext", fo, text=parsed)


def acquire(ctx, row, c):
    """Fetch the chosen candidate: a PDF on a sanctioned route, else Europe PMC's preprint text,
    else a manual preprint (its landing link). Every request is added to row.steps."""
    if c.server == "arxiv":
        aid = c.id or row.aid
        if not ctx.arxiv_pdf:
            return _Got("manual", "arxiv_pdf_off")
        if _refused("arxiv.org", ctx.state):
            return _Got("fail", "arxiv_pdf", row.add("arxiv_pdf", _host_refused("arxiv.org")))
        row.check()
        url = ARXIV_PDF.format(id=aid)
        return _pdf_got("arxiv_pdf", row.add("arxiv_pdf", _download(url, state=ctx.state, cfg=ctx.cfg,
                                                                      purpose="preprint: arXiv PDF")), url)

    if c.server in ("biorxiv", "medrxiv"):
        if not c.doi:
            return _epmc_text(ctx, row, c) or _Got("manual", f"{c.server}_manual")
        servers = [c.server] + ([s for s in ("biorxiv", "medrxiv") if s != c.server and ctx.enabled(s)]
                                if not c.server_known else [])
        meta, first = None, None
        for s in servers:
            row.check()
            o = row.add(f"{s}_details", biorxiv_details(s, c.doi, state=ctx.state, cfg=ctx.cfg))
            first = first or o
            if o.ok:
                meta, c.server = o.payload, s
                break
        if meta is None:
            # "no posts found" from every server tried, for a DOI Europe PMC did not place on a
            # server: the wrong-server answer (W2-C probe B2), so not an openRxiv preprint (a CSHL
            # Press journal article): NO_MATCH, not an OUTAGE retried on every run
            answers = [o for r, o in row.steps if r.endswith("_details")][-len(servers):]
            if not c.server_known and answers and all(_no_posts(o) for o in answers):
                return _Got("fail", f"{servers[0]}_details", Outcome(
                    Kind.NO_MATCH, status=first.status, host=first.host, attempts=first.attempts,
                    detail=f"api.biorxiv.org /details: no posts found on {', '.join(servers)}"))
            return _Got("fail", f"{servers[0]}_details", first)
        version = str(meta.get("version") or "1")
        c.landing_url = _doi_url(c.doi)
        if ctx.biorxiv_pdf:
            row.check()
            url = BIORXIV_PDF.format(server=c.server, doi=_doi.encode_path(c.doi), version=quote(version, safe=""))
            g = _pdf_got(f"{c.server}_pdf", row.add(f"{c.server}_pdf", _download(
                url, state=ctx.state, cfg=ctx.cfg, purpose=f"preprint: {c.server} PDF")), url)
            if g.kind == "pdf":
                return g
        return _epmc_text(ctx, row, c) or _Got("manual", f"{c.server}_manual")

    if c.server == "osf":
        if not c.osf_file:
            return _Got("manual", "osf_no_file")
        row.check()
        o = row.add("osf_download", osf_download(c.osf_file, state=ctx.state, cfg=ctx.cfg))
        return _pdf_got("osf_download", o, c.landing_url)

    if c.server == "sportrxiv":
        num, galley = c.preprint_num, c.galley
        if not galley:
            rec = ctx.sportrxiv.by_doi(c.doi) if (ctx.sportrxiv and c.doi) else None
            if rec:                         # the harvested record says whether a galley exists
                num, galley = rec.get("num") or num, rec.get("galley") or ""
                if not galley:
                    return _Got("manual", "sportrxiv_no_galley")
        if not galley:
            if not num:
                return _Got("manual", "sportrxiv_no_galley")
            row.check()
            go = row.add("sportrxiv_oai", sportrxiv_galley(num, state=ctx.state, cfg=ctx.cfg))
            if not go.ok:
                if go.kind in (Kind.NO_MATCH, Kind.NOT_AVAILABLE):
                    return _Got("manual", "sportrxiv_no_galley")
                return _Got("fail", "sportrxiv_oai", go)
            num, galley = go.payload
        row.check()
        url = SPORTRXIV_DOWNLOAD.format(id=num, galley=galley)
        return _pdf_got("sportrxiv_galley", row.add("sportrxiv_galley", _download(
            url, state=ctx.state, cfg=ctx.cfg, purpose="preprint: SportRxiv galley")), url)

    # any other server Europe PMC indexes: its preprint text where offered, else a person
    return _epmc_text(ctx, row, c) or _Got("manual", "epmc_manual")


def _judge(content, c, row):
    """judge_identity against the preprint's own DOI and title, then the queue's; the first
    verdict that is not a FLAG wins, else the last."""
    tries = []
    for d, t in ((c.doi, c.title or row.title), (row.doi, row.title)):
        if (d, t) not in tries and (d or t):
            tries.append((d, t))
    verdict = kind = n = None
    for d, t in tries:
        verdict, kind, n = judge_identity(content, d, t)
        if verdict.ok:
            break
    return verdict, kind, n


def _write_text_sidecar(ctx, row, c, fn, got):
    """The Europe PMC preprint text as `<stem>.fulltext.json` (has_pdf false). An existing sidecar
    is kept. Returns 'OK' or 'EXISTS'."""
    path = os.path.join(ctx.lib_dir, fn[:-4] + ".fulltext.json")
    if os.path.exists(path):
        return "EXISTS"
    sc = dict(got.text or {})
    sc.update({"doi": row.doi or c.doi, "preprint_doi": c.doi, "ppr": c.ppr, "server": c.server,
               "has_pdf": False, "extracted_from_pdf": False, "extractor": "europepmc_preprint_fulltextxml",
               "source": "preprint",
               "fetched_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")})
    lit_util.atomic_write_json(path, ledger.redact_obj(sc))
    return "OK"


def process_row(ctx, raw, deadline_s):
    """One queue row -> its report record (every row path writes one)."""
    row = _Row(raw, deadline_s)
    rec = row.rec
    fn = slug_filename(row.year, row.authors, row.title) if row.title else ""
    rec["preprint_filename"] = fn
    if not row.title and not row.aid:
        return _finish(row, Kind.ERROR, "none", "NO_TITLE", "no title to search and no arXiv id")
    hit = _held(ctx, row) if row.title else None
    if hit:
        rec.update(preprint_filename=hit, skipped=True)
        return _finish(row, Kind.OK, "library", "ALREADY_EXISTS", "already in the library")
    try:
        cands, excluded, ran, hint = discover(ctx, row)
        c = _best(cands, ctx.min_similarity)
        if c is None:
            root = _root_failure(row)
            if root is not None:
                return _fail(row, root[0], root[1])
            ex = _best(excluded, ctx.min_similarity)
            if ex is not None:
                return _finish(row, Kind.SKIPPED, "source", f"SOURCE_EXCLUDED:{ex.server}",
                               f"source_excluded:{ex.server}")
            if not ran:
                src = hint or "europepmc_preprints"
                return _finish(row, Kind.SKIPPED, "source", f"SOURCE_EXCLUDED:{src}", f"source_excluded:{src}")
            last = next((r for r, o in reversed(row.steps) if o.kind is Kind.NO_MATCH), "search")
            return _finish(row, Kind.NO_MATCH, last, "NO_MATCH", "no preprint matched"
                           + (f" (best excluded: {excluded[0].server})" if excluded else ""))
        rec.update(found=True, source=c.server, match_id=c.id, similarity=f"{c.sim:.2f}",
                   landing_url=c.landing_url)
        if not fn:
            fn = slug_filename(row.year or c.year, row.authors or c.authors, c.title)
            rec["preprint_filename"] = fn
        if ctx.dry_run:
            return _finish(row, Kind.SKIPPED, c.route, "DRY", "dry run: nothing downloaded")
        got = acquire(ctx, row, c)
    except _Deadline:
        return _finish(row, Kind.DEFERRED, "deadline", f"DEFERRED:row_deadline:{int(deadline_s)}s",
                       f"row_deadline:{int(deadline_s)}s")
    rec["landing_url"], rec["source"] = c.landing_url, c.server     # /details may have named the server
    if got.kind == "manual":
        return _finish(row, Kind.SKIPPED, got.route, "MANUAL_PREPRINT", "manual_preprint")
    if got.kind == "fail":
        return _fail(row, got.route, got.outcome)
    if got.kind == "text":
        status = _write_text_sidecar(ctx, row, c, fn, got)
        rec.update(sidecar=True, sidecar_status=status)
        return _finish(row, Kind.NOT_AVAILABLE, got.route, "NOT_AVAILABLE:text_only",
                       f"text only: Europe PMC preprint full text ({c.ppr}); no PDF on a sanctioned route",
                       got.outcome.status)

    # a PDF: size, boilerplate, then write and judge (nothing is ever moved)
    content = got.content
    rec["first_status"] = str(got.outcome.status or "")
    if len(content) < MIN_PDF_BYTES:
        return _finish(row, Kind.ERROR, got.route, "TOO_SMALL", f"{len(content)} B PDF from {got.outcome.host}")
    tag = boilerplate_of(content, _pdf_text(content)[0])
    if tag:
        return _finish(row, Kind.ERROR, got.route, "BOILERPLATE", f"{tag}:{len(content)}B")
    dest, collided = resolve_dest(ctx.lib_dir, fn, row.doi or c.doi, ctx.written_this_run)
    if collided:
        rec["preprint_filename"] = os.path.basename(dest)
    _write_bytes(dest, content)
    ctx.written_this_run.add(dest)
    ctx.existing.add(os.path.basename(dest))
    verdict, dkind, n_pages = _judge(content, c, row)
    attempt = Attempt(host_type=c.server, version="submittedVersion", url=ledger.redact(got.url),
                      status="OK", kind=Kind.OK, host=got.outcome.host or "")
    write_identity_sidecar(dest, row.doi or c.doi, verdict, dkind, n_pages, attempt, source="preprint")
    rec["doc_kind"] = str(dkind)
    if _flagged(verdict, dkind):
        rec["identity"] = "FLAG"
        pdf_doi = (verdict.evidence.get("pdf_dois") or [""])[0]
        why = "doc_kind=SUPPLEMENT" if verdict.ok else f"identity={verdict.decision}"
        return _finish(row, Kind.ERROR, got.route, f"DOI_MISMATCH:pdf_doi={pdf_doi or 'none'};{why}",
                       f"identity flag kept in place: {verdict.evidence.get('reason') or why}")
    rec["identity"] = str(verdict.decision)
    rec["downloaded"] = True
    _finish(row, Kind.OK, got.route, "OK", f"{got.outcome.host}; {len(content)} B; identity {verdict.decision}")
    if not ctx.no_write_ris and (row.doi or c.doi):
        try:
            ris_status, _ = _R.emit_ris_for_pdf(row.doi or c.doi, dest)
            print(f"        ris: {ris_status}", flush=True)
        except Exception as e:  # noqa: BLE001  (a .ris failure must not lose the row)
            print(f"        ris: ERROR {ledger.redact(str(e))[:80]}", flush=True)
    return rec


# ---------------------------------------------------------------- the stage
def resolve_project(project, lib_dir, cfg):
    """(key, how): the --project key (ConfigError when unregistered), else the registered project
    whose library is --lib-dir, else (None, None)."""
    projects = (cfg or {}).get("projects") or {}
    if project:
        if project not in projects:
            raise config.ConfigError(f"project {project!r} is not registered in projects.json")
        return project, "--project"
    want = os.path.normcase(os.path.abspath(lib_dir))
    for key, p in projects.items():
        if not isinstance(p, dict) or not p.get("lib_dir"):
            continue
        try:
            _, lib, _ = lit_util.lib_paths(key, p)
        except (KeyError, TypeError, ValueError):
            continue
        if os.path.normcase(os.path.abspath(str(lib))) == want:
            return key, "--lib-dir"
    return None, None


def _summary_line(tag, rec):
    return (f"{tag:<9} {rec['outcome']:<13} {(rec['source'] or '-'):<20} "
            f"{(rec['detail'] or rec['status'])[:70]}")


def _clean(rec):
    return {k: (ledger.redact(v) if isinstance(v, str) else v) for k, v in rec.items()}


def run(*, triage, lib_dir, report=None, min_similarity=0.80, limit=0, dry_run=False, no_write_ris=False,
        project=None, sources=None, row_deadline=ROW_DEADLINE_S, state=None, cfg=None) -> dict:
    """The preprint stage (dispatch 0.5 stage-function contract). Returns a summary dict with
    `exit_code`."""
    out_csv = report or DEFAULT_REPORT
    try:
        cfg = config.load(cfg)
        key, how = resolve_project(project, lib_dir, cfg)
        if key:
            srcs = config.sources(key, sources, cfg=cfg)
        elif sources is not None:
            srcs = config.sources(None, sources, cfg=cfg)
        else:
            srcs = set(config.DEFAULT_SOURCES)
        switches = config.hosts(cfg)
    except config.ConfigError as e:
        print(f"CONFIG: {e}", file=sys.stderr, flush=True)
        return {"exit_code": EXIT_CONFIG, "report": out_csv, "error": str(e)}
    if not os.path.exists(triage):
        print(f"ERR: triage CSV not found: {triage}", file=sys.stderr, flush=True)
        return {"exit_code": EXIT_NO_TRIAGE, "report": out_csv}

    with open(triage, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    if limit:
        rows = rows[:limit]
    enabled = sorted(s for s in srcs if s in PREPRINT_SOURCES)
    if key:
        print(f"[preprint] project {key} (from {how}); preprint sources: {', '.join(enabled) or 'none'}", flush=True)
    else:
        print(f"[preprint] no registered project matches --lib-dir {lib_dir}; using "
              + ("the --sources list" if sources is not None else "the default sources (unpaywall, pmc)")
              + f"; preprint sources: {', '.join(enabled) or 'none'}"
              + ("" if enabled else " (only rows with an arXiv ID or a 10.48550 DOI are tried)"), flush=True)

    if not dry_run:
        os.makedirs(lib_dir, exist_ok=True)
    ctx = _Ctx(lib_dir, srcs, min_similarity, dry_run, no_write_ris, state, cfg,
               arxiv_pdf=bool(switches.get("arxiv_pdf_allowed")),
               biorxiv_pdf=bool(switches.get("biorxiv_pdf_allowed")),
               existing=set(os.listdir(lib_dir)) if os.path.isdir(lib_dir) else set())
    if "sportrxiv" in srcs:
        ctx.sportrxiv = SportRxivIndex(cfg, state, dry_run)

    ids = [arxiv_id_of(r) for r in rows]
    ids = [a for a in ids if a]
    if ids:
        print(f"[preprint] arXiv: {len(ids)} row(s) carry an arXiv id; looking them up in batches of {ARXIV_BATCH}",
              flush=True)
        ctx.arxiv_ids = arxiv_lookup_ids(ids, state=state, cfg=cfg)

    results = []
    counts = {"rows": len(rows), "downloaded": 0, "already": 0, "flag": 0, "text_only": 0, "manual": 0,
              "no_match": 0, "refused": 0, "deferred": 0, "outage": 0, "skipped_source": 0, "error": 0,
              "dry": 0, "other": 0}
    for i, raw in enumerate(rows, 1):
        try:
            rec = process_row(ctx, raw, row_deadline)
        except Exception as e:  # noqa: BLE001  (a bug in one row must not lose the stage report)
            row = _Row(raw, 0)
            rec = _finish(row, Kind.ERROR, "preprint_fetch", "ERROR",
                          f"{type(e).__name__}: {ledger.redact(str(e))}"[:200])
        results.append(rec)
        kind = rec["outcome"]
        if rec["downloaded"]:
            tag = "DL"; counts["downloaded"] += 1
        elif rec["skipped"]:
            tag = "SKIP"; counts["already"] += 1
        elif rec["identity"] == "FLAG":
            tag = "FLAG"; counts["flag"] += 1
        elif rec["sidecar"]:
            tag = "TEXT"; counts["text_only"] += 1
        elif rec["status"] == "MANUAL_PREPRINT":
            tag = "MANUAL"; counts["manual"] += 1
        elif rec["status"] == "DRY":
            tag = "DRY"; counts["dry"] += 1
        elif kind == "SKIPPED":
            tag = "EXCLUDED"; counts["skipped_source"] += 1
        else:
            tag = {"NO_MATCH": "--", "REFUSED": "REFUSED", "DEFERRED": "DEFERRED", "OUTAGE": "OUTAGE"}.get(kind, "FAIL")
            key_ = {"NO_MATCH": "no_match", "REFUSED": "refused", "DEFERRED": "deferred", "OUTAGE": "outage",
                    "ERROR": "error"}.get(kind, "other")
            counts[key_] += 1
        print(f"  [{i:>3}/{len(rows)}] {_summary_line(tag, rec)} | {rec['title'][:50]}", flush=True)

    out_dir = os.path.dirname(out_csv)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=REPORT_FIELDS, extrasaction="ignore", lineterminator="\n")
    w.writeheader()
    w.writerows(_clean(r) for r in results)
    lit_util.atomic_write_text(out_csv, buf.getvalue())

    refused = sorted(h for h in ("export.arxiv.org", "arxiv.org") if _refused(h, state))
    print("\n=== Summary ===", flush=True)
    print(f"  Rows processed:    {len(rows)}")
    print(f"  Already had file:  {counts['already']}")
    print(f"  Downloaded:        {counts['downloaded']}")
    print(f"  Identity flag:     {counts['flag']} (kept in place; no .ris)")
    print(f"  Text only:         {counts['text_only']} (Europe PMC preprint text, no PDF)")
    print(f"  Manual preprint:   {counts['manual']} (a person opens landing_url)")
    print(f"  Source excluded:   {counts['skipped_source']} (not in this project's sources; nothing sent)")
    print(f"  Refused:           {counts['refused']}" + (f" (refused hosts: {', '.join(refused)})" if refused else ""))
    print(f"  Deferred:          {counts['deferred']}  Outage: {counts['outage']}  Error: {counts['error']}")
    print(f"  No match:          {counts['no_match']}")
    print(f"\nReport: {out_csv}", flush=True)
    return {"exit_code": EXIT_OK, "report": out_csv, "project": key, "sources": sorted(srcs),
            "refused_hosts": refused, **counts}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    ap.add_argument("--triage", required=True,
                    help="CSV with columns: doi,title,year,authors")
    ap.add_argument("--lib-dir", required=True,
                    help="Directory to save fetched preprints")
    ap.add_argument("--min-similarity", type=float, default=0.80,
                    help="Minimum normalized-title SequenceMatcher ratio to accept a match (default: 0.80).")
    ap.add_argument("--limit", type=int, default=0,
                    help="Process only first N rows (0 = all)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Search preprint sources and log matches, but do not download PDFs.")
    ap.add_argument("--report", default=None,
                    help=f"Output CSV (default: {DEFAULT_REPORT})")
    ap.add_argument("--no-write-ris", action="store_true",
                    help="Skip writing .ris sidecar next to each successfully fetched PDF.")
    ap.add_argument("--project", default=None,
                    help="Registered project key: its projects.json `sources` pick the preprint servers "
                         "(default: the project whose library is --lib-dir).")
    ap.add_argument("--sources", default=None,
                    help="Comma-separated sources replacing the project's list for this run "
                         "(arxiv,biorxiv,medrxiv,osf,sportrxiv,europepmc_preprints,...).")
    ap.add_argument("--row-deadline", type=float, default=ROW_DEADLINE_S,
                    help=f"Wall-clock seconds per row before it is DEFERRED (default: {ROW_DEADLINE_S:.0f}; 0 = none).")
    args = ap.parse_args(argv)
    res = run(triage=args.triage, lib_dir=args.lib_dir, report=args.report, min_similarity=args.min_similarity,
              limit=args.limit, dry_run=args.dry_run, no_write_ris=args.no_write_ris, project=args.project,
              sources=args.sources, row_deadline=args.row_deadline)
    return res["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
