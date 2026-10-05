"""Unpaywall stage: fetch open-access PDFs for queue rows (refactor scope 3.2; dispatch W2-B).

Every request goes through litpipe.net (host policy, pacing, refusals, ledger, redaction; the
Unpaywall `email=` identity is added by net itself). Per row, in order:

  1. SKIP_EXISTS when the destination library already holds the DOI: a `.ris` DO line or an
     `.identity.json` verdict whose PDF is present, or a file at the row's filename (or at its
     pre-DEC-14/15 legacy filename) that is this paper (sidecar DOI, else the identity check on
     its text). A file with no DOI and no matching title is NOT this paper (REG-I11).
  2. Registration agency: doi.org `/doiRA/<prefix>,<prefix>,...` (batched, cached per prefix for
     30 days in litpipe.state kv). Unpaywall covers Crossref DOIs only, so any other agency is
     NOT_AT_RA and Unpaywall is not asked. A failed RA lookup falls through to Unpaywall.
  3. Unpaywall `/v2/{doi}` (the DOI normalised, then path-encoded): 404 NO_MATCH; 422/410 CONFIG
     aborts the stage (exit 2); a placeholder or missing LITPIPE_EMAIL is CONFIG before any send.
  4. Candidates from `oa_locations`: `--candidate-order repository` (default, DEC-11) tries
     repositories, then publishers; `publisher` the reverse. A `doi.org` landing URL is resolved
     through doi.org's handle API (net follows doi.org redirects only to content-negotiation hosts).
  5. Downloads: stream, capped at lit_net.MAX_PDF_BYTES, validated as PDF. A 403 fails the
     candidate and refuses the host for the run at its threshold (DEC-06: the first); later
     candidates on a refused host are not sent. A bot interstitial (Access Denied, Client
     Challenge, Just a moment...), or an identical-size non-PDF body from one host for two DOIs,
     refuses the host for the run too. An ordinary HTML landing page is followed through its
     citation_pdf_url / PDF links (at most 3).
  6. Identity (replaces the DOI-mismatch quarantine): litpipe.identity.check and doc_kind on the
     PDF's first pages. The verdict is written to `<final stem>.identity.json`; a FLAG (or a
     supplement) keeps the file where it is, writes no `.ris`, and reports DOI_MISMATCH. No file
     is ever moved or deleted.

Report: the legacy columns (sweep and pmc_fetch read them) plus typed ones: `first_status` (the
first HTTP status sent for the row), `route`, `outcome` (a litpipe Kind, the root cause rather
than the last fallback), `detail`, `ra`, `identity`, `doc_kind`. Every string written passes
litpipe.ledger.redact.

Exit codes: 0 done; 1 triage CSV missing; 2 CONFIG (bad email, Unpaywall 422/410): the stage
stops, and the report holds the rows done so far plus the CONFIG row.

Usage:
  python unpaywall_fetch_v2.py [--top-n 100] [--dry-run] [--candidate-order repository|publisher]
"""
import argparse
import csv
import hashlib
import importlib
import io
import json
import os
import re
import sys
from dataclasses import dataclass
from urllib.parse import urljoin, urlsplit

import lit_net  # MAX_PDF_BYTES (c7: the one download cap)
import lit_util
from litpipe import doi as _doi
from litpipe import identity as _identity
from litpipe import ledger, net, preflight
from litpipe.hosts import ProhibitedHost
from litpipe.outcomes import Kind, Outcome, from_legacy

lit_util.utf8_stdout()

# Local module
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ris_emit as _R  # noqa: E402

UNPAYWALL = "https://api.unpaywall.org/v2"
DOI_RA = "https://doi.org/doiRA/"
DOI_HANDLES = "https://doi.org/api/handles/"
DOI_HOSTS = frozenset({"doi.org", "dx.doi.org", "www.doi.org"})

# Browser-ish UA for *download* GETs: publisher hosts block API clients. litpipe.net keeps a
# caller's User-Agent on download hosts (identity "download") and never adds the email there.
BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

DEFAULT_TRIAGE = "data/prior_art/discovered/triage_not_in_library.csv"
DEFAULT_LIB    = "references/literature"
DEFAULT_REPORT = "data/prior_art/discovered/unpaywall_fetch_report_v2.csv"

CANDIDATE_ORDERS = ("repository", "publisher")
RA_KV_NS = "doi.ra"                 # litpipe.state kv namespace: prefix -> registration agency
RA_TTL_S = 30 * 86400               # a prefix's agency does not change; re-check monthly
RA_BATCH = 100                      # prefixes per /doiRA/ call (125 worked 2026-09-25)
MIN_PDF_BYTES = 10_000              # smaller "PDFs" are error pages or stubs
IDENTITY_PAGES = 3                  # first pages read for the identity check
HTML_FALLBACK_LINKS = 3
EXIT_OK, EXIT_NO_TRIAGE, EXIT_CONFIG = 0, 1, 2

REPORT_FIELDS = ["rank", "doi", "year", "cites", "filename", "title", "oa_status",
                 "n_locations", "downloaded", "winning_host", "winning_url", "attempts", "error",
                 # typed columns (W2-B; sweep reads them from W2-G on)
                 "first_status", "route", "outcome", "detail", "ra", "identity", "doc_kind"]
SIDECAR_EXTS = (".ris", ".fulltext.json", ".identity.json", ".xml")

# ---------- filename synthesis ----------

def slug_title(title, max_words=6):
    """The title slug: ris_emit.slug, the one slug writer (DEC-15; 23-word stoplist)."""
    return _R.slug(title or "", max_words)


# Initials: 1 to 5 capitals, each optionally dotted, a hyphen allowed between two (J, JM, J.M.,
# J.-M., JVGA). Particles joined onto the surname with no space, case kept (DEC-14).
_INITIALS = re.compile(r"(?:[A-Z]\.?(?:-[A-Z]\.?)?){1,5}")
_PARTICLES = frozenset({
    "van", "von", "der", "den", "de", "del", "della", "dello", "dei", "degli", "des", "di", "da",
    "dal", "dos", "das", "do", "du", "la", "le", "lo", "ter", "ten", "te", "vande", "vander", "st",
    "saint", "bin", "ibn", "al", "el", "zu", "af", "av"})


def _is_particle(tok):
    return tok.lower().rstrip(".") in _PARTICLES


def last_name(authors_str):
    """The first author's family name as one filename token (DEC-14, REG-I17).

      - "Periard JD; Casa DJ"          -> "Periard"      (Surname Initials)
      - "Durnin JVGA; Womersley J"     -> "Durnin"       (up to 5 initials)
      - "van der Walt JHA; Smith B"    -> "vanderWalt"   (leading particles joined, case kept)
      - "De Vries H"                   -> "DeVries"
      - "Garcia-Lopez J"               -> "Garcia-Lopez" (hyphen kept)
      - "Stephenson Mark D"            -> "Stephenson"   (Surname Given Initial: the first word)
      - "J Smith; K Jones", "T. Gabbett", "Charlotte E. Stevens" -> the last token
      - "Hugo De Vries", "I. Di Domenico" -> "DeVries", "DiDomenico" (particles before the last
        token, but never the first token of a "Given Surname" pair: "Bin Yang" -> "Yang")
      - "Le Roux, Elisa"               -> "LeRoux"       ("Surname, Given": the whole surname)
      - "Smith, John; Doe, Jane", "Cramer MN, Jay O", "Malchaire J, Piette A, et al" -> first
      - "Tseng et al. (Cornell)"       -> "Tseng"        (cut at "et al")
    Accents fold to ASCII first (safe_ascii), so "Mølmen Ø" gives "Molmen". "Unknown" when there
    is no usable name. Existing files keep their names: legacy_last_name is the old rule."""
    if not authors_str:
        return "Unknown"
    from ris_emit import safe_ascii
    s = safe_ascii(str(authors_str).strip())
    s = re.sub(r"\bet\s+al\b.*$", "", s, flags=re.IGNORECASE | re.DOTALL).strip().rstrip(",;").strip()
    first = s.split(";")[0].strip()
    comma = "," in first
    if comma:
        first = first.split(",")[0].strip()
    parts = first.split()
    if not parts:
        return "Unknown"
    if len(parts) > 1 and _INITIALS.fullmatch(parts[-1]):
        # "Surname [Given] Initials": drop the initials run, skip a stray leading initial
        # ("A Purcell S"), then the leading particles and the first word are the surname.
        body = parts[:-1]
        while len(body) > 1 and len(body[-1].replace(".", "")) <= 2 and _INITIALS.fullmatch(body[-1]):
            body = body[:-1]               # "Coyne Joseph O C": a second short initials token
        i = 0
        while i < len(body) - 1 and re.fullmatch(r"[A-Z]\.?", body[i]):
            i += 1
        j = i
        while j < len(body) - 1 and _is_particle(body[j]):
            j += 1
        surname = body[i:j + 1]
    else:
        # "Given Names Surname" / "Initials Surname": the last token and the particles before it,
        # keeping at least one given token in front ("Bin Yang", "Le Ma" are Given Surname). A
        # "Surname, Given" part (comma form) is all surname: "Le Roux, Elisa".
        k = len(parts) - 1
        floor = 0 if comma else 1
        while k > floor and _is_particle(parts[k - 1]):
            k -= 1
        surname = parts[k:]
    return re.sub(r"[^A-Za-z0-9\-]", "", "".join(surname)) or "Unknown"


def build_filename(year, authors, title):
    """`YYYY_Lastname_TitleSlug.pdf` through ris_emit.canonical_stem (one stem writer)."""
    return _R.canonical_stem(year, last_name(authors), title) + ".pdf"


# The pre-2026-09-30 rules, kept so files already named by them are recognised (DEC-14/15: new
# files only, nothing renamed) and so a queue-history map can be built for both.
_LEGACY_SLUG_SKIP = {"a", "an", "the", "of", "in", "on", "and", "to", "for", "at", "from", "with",
                     "by", "as"}


def legacy_slug_title(title, max_words=6):
    from ris_emit import safe_ascii
    t = re.sub(r"<[^>]+>", "", title or "")
    t = safe_ascii(t)
    t = re.sub(r"[^\w\s\-]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    words = [w for w in t.split() if w.lower() not in _LEGACY_SLUG_SKIP]
    return "".join(w.capitalize() for w in words[:max_words]) or "Untitled"


def legacy_last_name(authors_str):
    if not authors_str:
        return "Unknown"
    from ris_emit import safe_ascii
    s = safe_ascii(authors_str.strip())
    s = re.sub(r",?\s*et\s+al\.?\s*$", "", s, flags=re.IGNORECASE).strip()
    first = s.split(";")[0].strip()
    if "," in first:
        first = first.split(",")[0].strip()
    parts = first.split()
    if not parts:
        return "Unknown"
    cand = parts[-1]
    if len(parts) > 1 and re.fullmatch(r"[A-Z]{1,3}\.?", cand):
        cand = parts[0]
    return re.sub(r"[^A-Za-z0-9\-]", "", cand) or "Unknown"


def legacy_build_filename(year, authors, title):
    """The filename the stage wrote before DEC-14/15 (14-word stoplist, first-token surname)."""
    yr = year if year and re.match(r'^\d{4}$', str(year)) else "Unknown"
    return f"{yr}_{legacy_last_name(authors)}_{legacy_slug_title(title)}.pdf"


def first_author_of(upw):
    """The first author's raw name from an Unpaywall record (`z_authors[].raw_author_name`,
    `author_position` "first"; older records carry `family`/`given`). '' when absent."""
    authors = (upw or {}).get("z_authors") or []
    if not isinstance(authors, list) or not authors:
        return ""
    first = next((a for a in authors if isinstance(a, dict) and a.get("author_position") == "first"),
                 authors[0])
    if not isinstance(first, dict):
        return ""
    if first.get("raw_author_name"):
        return str(first["raw_author_name"])
    if first.get("family"):
        return f"{first['family']}, {first.get('given') or ''}".strip(", ")
    return ""

# ---------- RC2: collision-safe destination; helpers shared with pmc_fetch / preprint_fetch ----------

def _doi_hash(doi, n=6):
    """Short, stable hex tag for a DOI, used to disambiguate colliding stems."""
    return hashlib.sha1((doi or "").strip().lower().encode("utf-8")).hexdigest()[:n]


def _read_identity_sidecar(pdf_path):
    try:
        with open(lit_util.companion_path(pdf_path, ".identity.json"), encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except (OSError, ValueError):
        return None


def _sidecar_doi(pdf_path):
    """The DOI a PDF's `.ris` (DO) or `.fulltext.json` (doi) records, normalised; '' if none. A
    `.fulltext.json` whose identity verdict is FLAG records the queue DOI of a PDF judged to be
    another work (the PMC stage), so it names no DOI here."""
    ris_path = str(lit_util.companion_path(pdf_path, ".ris"))
    if os.path.exists(ris_path):
        try:
            with open(ris_path, encoding="utf-8") as f:
                for line in f:
                    if line.startswith("DO  - "):
                        d = _doi.normalise(line[6:].strip())
                        if d:
                            return d
        except OSError:
            pass
    sc_path = str(lit_util.companion_path(pdf_path, ".fulltext.json"))
    if os.path.exists(sc_path):
        try:
            with open(sc_path, encoding="utf-8") as f:
                rec = json.load(f) or {}
            d = "" if rec.get("identity") == "FLAG" else _doi.normalise(rec.get("doi") or "")
            if d:
                return d
        except (OSError, ValueError, AttributeError):
            pass
    return ""


def _doi_of_existing(pdf_path):
    """Best-effort DOI already associated with an on-disk PDF (RC2).

    Reads the sibling .ris (DO field) or .fulltext.json sidecar (doi field) first, then the
    `.identity.json` this stage writes: a verified file gives its queue DOI; a FLAGged one gives
    `unverified:<doi>` (the PDF's own first DOI when it printed one), so no caller treats a file
    that failed the identity check as the queued paper (REG-I11). Only then the PDF bytes.
    Returns a normalized DOI, that marker, or ''. (lit_util.normalize_doi form: pmc_fetch and
    preprint_fetch compare it with lit_util.normalize_doi.)"""
    stem, _ = os.path.splitext(pdf_path)
    ris_path = stem + ".ris"
    if os.path.exists(ris_path):
        try:
            with open(ris_path, encoding="utf-8") as f:
                for line in f:
                    if line.startswith("DO  - "):
                        d = lit_util.normalize_doi(line[6:].strip())
                        if d:
                            return d
        except OSError:
            pass
    sc_path = stem + ".fulltext.json"
    if os.path.exists(sc_path):
        try:
            with open(sc_path, encoding="utf-8") as f:
                d = lit_util.normalize_doi((json.load(f) or {}).get("doi") or "")
            if d:
                return d
        except (OSError, ValueError):
            pass
    ids = _read_identity_sidecar(pdf_path)
    if ids and ids.get("queue_doi"):
        if ids.get("identity") in ("OK", "TITLE_MATCH") and ids.get("doc_kind") != "SUPPLEMENT":
            return lit_util.normalize_doi(ids["queue_doi"])
        found = ((ids.get("identity_evidence") or {}).get("pdf_dois") or [None])[0]
        return found or f"unverified:{ids['queue_doi']}"
    return doi_from_pdf_bytes(pdf_path)


def doi_from_pdf_bytes(pdf_path, max_chars=5000):
    """Extract the first well-formed DOI from a PDF's first ~5KB (RC3). '' on failure.
    (pmc_fetch and preprint_fetch still use it; this stage uses the identity check instead.)"""
    try:
        import fitz
    except ImportError:
        return ""
    text = ""
    try:
        doc = fitz.open(str(pdf_path))
        try:
            for p in doc:
                text += p.get_text()
                if len(text) >= max_chars:
                    break
        finally:
            doc.close()
    except Exception:
        return ""
    return lit_util.extract_doi_from_text(text[:max_chars])


def _stem_occupied(path):
    """A PDF at `path`, or an orphan sidecar at its stem (REG-I11): the stem is taken."""
    if os.path.exists(path):
        return True
    return any(os.path.exists(str(lit_util.companion_path(path, ext))) for ext in SIDECAR_EXTS)


def _occupant_is(path, doi):
    """True only when what occupies `path`'s stem is known to be `doi`."""
    want = lit_util.normalize_doi(doi)
    if not want:
        return False
    ids = _read_identity_sidecar(path)
    if ids and not _sidecar_doi(path) and _doi.normalise(ids.get("queue_doi") or "") == _doi.normalise(doi):
        return True          # our own earlier copy for this DOI (a flag included): rewrite it in place
    if os.path.exists(path):
        return _doi_of_existing(path) == want
    d = _sidecar_doi(path)                       # an orphan sidecar waiting for its PDF
    if d:
        return d == _doi.normalise(doi)
    ids = _read_identity_sidecar(path)
    return bool(ids) and ids.get("identity") in ("OK", "TITLE_MATCH") and \
        _doi.normalise(ids.get("queue_doi") or "") == _doi.normalise(doi)


def resolve_dest(lib_dir, fn, doi, written_this_run):
    """Return a collision-safe destination path for `fn` (RC2, REG-I11).

    - Never clobber a PDF written earlier in THIS run: if the stem was already used,
      disambiguate with a DOI-hash suffix.
    - A stem held by anything not known to be this DOI (a PDF with another or no DOI, or an
      orphan .ris / .fulltext.json / .identity.json / .xml sidecar) is occupied: disambiguate
      rather than overwrite or pair this PDF with another paper's sidecar.
    Returns (dest_path, collided: bool). Sidecars follow the returned name (companion_path)."""
    dest = os.path.join(lib_dir, fn)
    collided = dest in written_this_run or (_stem_occupied(dest) and not _occupant_is(dest, doi))
    if not collided:
        return dest, False
    stem, ext = os.path.splitext(fn)
    n = 1
    cand = os.path.join(lib_dir, f"{stem}_{_doi_hash(doi)}{ext}")
    while cand in written_this_run or (_stem_occupied(cand) and not _occupant_is(cand, doi)):
        n += 1
        cand = os.path.join(lib_dir, f"{stem}_{_doi_hash(doi)}_{n}{ext}")
    return cand, True


def pdf_doi_disagrees(pdf_path, queue_doi):
    """DEPRECATED (W2-B): this stage uses litpipe.identity.check. Kept only because
    preprint_fetch still imports it (W2-C rewires that stage); remove once nothing imports it.
    True iff the PDF's first DOI disagrees with `queue_doi`."""
    found = doi_from_pdf_bytes(pdf_path)
    if not found:
        return False
    return found != lit_util.normalize_doi(queue_doi)


def quarantine_mismatch(dest, lib_dir):
    """DEPRECATED (W2-B): the identity check replaced the quarantine and this stage never moves a
    file. Kept, unchanged, only because preprint_fetch still imports it (W2-C rewires that stage);
    remove once nothing imports it. Moves `dest` to <lib>/_mismatch/; True if moved."""
    try:
        mismatch_dir = os.path.join(lib_dir, "_mismatch")
        os.makedirs(mismatch_dir, exist_ok=True)
        base = os.path.basename(dest)
        target = os.path.join(mismatch_dir, base)
        if os.path.exists(target):
            stem, ext = os.path.splitext(base)
            n = 1
            while os.path.exists(os.path.join(mismatch_dir, f"{stem}_{n}{ext}")):
                n += 1
            target = os.path.join(mismatch_dir, f"{stem}_{n}{ext}")
        os.replace(dest, target)
        return True
    except OSError:
        return False

# ---------- state (the same object litpipe.net uses) ----------

def _state():
    return net.STATE if net.STATE is not None else importlib.import_module("litpipe.state")

# ---------- registration agency ----------

def _prefix(doi):
    d = _doi.normalise(doi or "")
    return d.split("/", 1)[0] if d else ""


def registration_agencies(dois, *, state=None):
    """{prefix: agency or None} for the DOIs' prefixes. Cached per prefix in the state kv for
    RA_TTL_S; uncached prefixes are asked of doi.org `/doiRA/` in comma batches. None means
    unknown (the lookup failed, or doi.org answered "DOI does not exist" / "Unknown" / "Invalid
    DOI"): the caller falls through to Unpaywall rather than guess."""
    st = state or _state()
    out, todo = {}, []
    for p in sorted({_prefix(d) for d in dois if _prefix(d)}):
        cached = st.kv_get(RA_KV_NS, p)
        if cached:
            out[p] = cached
        else:
            out[p] = None
            todo.append(p)
    for i in range(0, len(todo), RA_BATCH):
        chunk = todo[i:i + RA_BATCH]
        o = net.get(DOI_RA + ",".join(chunk), validate=net.expect_json, state=state,
                    purpose="unpaywall: registration agency by prefix")
        if not o.ok:
            print(f"  [RA] lookup failed for {len(chunk)} prefix(es): {o.kind} {ledger.redact(o.detail)}; "
                  f"asking Unpaywall for those rows", file=sys.stderr)
            continue
        try:
            body = o.payload.json()
        except ValueError:
            continue
        for item in body if isinstance(body, list) else []:
            if not isinstance(item, dict):
                continue
            p, ra = str(item.get("DOI") or "").lower(), item.get("RA")
            if p in out and ra:
                out[p] = str(ra)
                st.kv_set(RA_KV_NS, p, str(ra), ttl_s=RA_TTL_S)
    return out

# ---------- Unpaywall query ----------

def unpaywall_lookup(doi, *, state=None):
    """GET /v2/{doi} through litpipe.net (it adds `email=`; the caller never does). Returns the
    Outcome: OK (payload a net.Response; `.json()` is the record); NO_MATCH (404); CONFIG
    (422/410); REFUSED, OUTAGE, TRANSPORT, DEFERRED, ERROR otherwise. Never an empty result for
    a failed call. The DOI is normalised, then path-encoded (DOI Handbook 4.7)."""
    try:
        path = _doi.encode_path(doi)
    except ValueError:
        return Outcome(Kind.ERROR, host="api.unpaywall.org", detail=f"not a DOI: {doi!r}")
    return net.get(f"{UNPAYWALL}/{path}", validate=net.expect_json, state=state,
                   purpose="unpaywall: lookup")


def candidate_urls(upw, order="repository"):
    """Ordered list of (host_type, version, url, field) candidates.

    order "repository" (default, DEC-11): repositories, then publishers, then anything else;
    "publisher": publishers first. Within a tier publishedVersion > acceptedVersion >
    submittedVersion, and url_for_pdf before url (a landing page). URLs de-duplicated."""
    if order not in CANDIDATE_ORDERS:
        raise ValueError(f"candidate order must be one of {CANDIDATE_ORDERS}, got {order!r}")
    locs = [loc for loc in (upw.get("oa_locations") or []) if isinstance(loc, dict)]
    repos = [loc for loc in locs if loc.get("host_type") == "repository"]
    pubs = [loc for loc in locs if loc.get("host_type") == "publisher"]
    other = [loc for loc in locs if loc.get("host_type") not in ("repository", "publisher")]
    tiers = (repos, pubs, other) if order == "repository" else (pubs, repos, other)
    ver_rank = {"publishedVersion": 0, "acceptedVersion": 1, "submittedVersion": 2}
    out = []
    for tier in tiers:
        for loc in sorted(tier, key=lambda l: ver_rank.get(l.get("version") or "submittedVersion", 3)):
            for url_field in ("url_for_pdf", "url"):
                u = loc.get(url_field)
                if u:
                    out.append((loc.get("host_type"), loc.get("version"), u, url_field))
    seen, uniq = set(), []
    for tup in out:
        if tup[2] not in seen:
            seen.add(tup[2])
            uniq.append(tup)
    return uniq

# ---------- download ----------

# Known publisher-boilerplate fingerprints. When Unpaywall's OA URL resolves to
# a permissions / author-guidelines PDF instead of the article, the PDF is a
# valid PDF (passes %PDF + size checks) but its text is the same boilerplate
# across every DOI from that publisher. Listed by md5 + a fallback text snippet
# (md5 catches the exact byte file; text snippet catches re-spun versions).
#
# Add new entries when the LWW-style trap is observed for other publishers.
# The original 2026-05-22 batch: Currier 2026, Lim 2022, Agostinho 2015.
KNOWN_BOILERPLATE_MD5 = {
    "518fe51393a7ba381f861b58f296832e": "lww_author_permission_guidelines_v1",
    # 2026-05-22 audit additions (Agent C cross-project sweep):
    "9aeef9e74d08bbd6b39996fb963fd8cb": "plos_manuscript_body_formatting_template",
    "b9d50b11d4901b8fb7d5eaab473193dc": "jmlr_scikit_learn_misfetch_for_jmir_dois",
}
KNOWN_BOILERPLATE_TEXT = (
    # All matched against the first ~3000 chars of pdftotext output, lowercased
    ("lippincott journal portfolio",      "lww_author_permission_guidelines"),
    ("author permission guidelines",      "lww_author_permission_guidelines"),
    # 2026-05-22 audit additions:
    ("manuscript body formatting guidelines", "plos_template"),
    ("cite figures as \"fig 1\"",             "plos_template"),
    ("scikit-learn: machine learning in python", "jmlr_misfetch"),
    ("portal de periódicos da capes",    "capes_redirect_page"),
)


def is_known_boilerplate(pdf_path):
    """Return (True, tag) if the downloaded PDF matches a known publisher
    boilerplate fingerprint; (False, None) otherwise.

    Cheap path first (md5), then a 2-page pdftotext probe. (pmc_fetch, preprint_fetch and
    paywall_pull call it on a file; this stage checks the bytes in memory: boilerplate_of.)
    """
    import hashlib as _h
    try:
        with open(pdf_path, "rb") as f:
            h = _h.md5(f.read()).hexdigest()
        if h in KNOWN_BOILERPLATE_MD5:
            return True, KNOWN_BOILERPLATE_MD5[h]
    except Exception:
        return False, None

    import shutil, subprocess
    pdftotext = shutil.which("pdftotext")
    if not pdftotext:
        return False, None
    try:
        r = subprocess.run([pdftotext, "-l", "2", "-enc", "UTF-8", pdf_path, "-"],
                           capture_output=True, timeout=20)
        if r.returncode != 0:
            return False, None
        snippet = r.stdout.decode("utf-8", "replace")[:3000].lower()
    except Exception:
        return False, None
    for needle, tag in KNOWN_BOILERPLATE_TEXT:
        if needle in snippet:
            return True, tag
    return False, None


def boilerplate_of(content, first_text):
    """The boilerplate tag for PDF bytes (md5) or their first-pages text (snippet), else None."""
    tag = KNOWN_BOILERPLATE_MD5.get(hashlib.md5(content).hexdigest())
    if tag:
        return tag
    snippet = (first_text or "")[:3000].lower()
    return next((t for needle, t in KNOWN_BOILERPLATE_TEXT if needle in snippet), None)


def extract_pdf_links_from_html(html_bytes, base_url):
    """Find PDF URLs embedded in an HTML landing page (citation_pdf_url meta,
    embed/iframe src, or a-tag href with .pdf)."""
    try:
        html = html_bytes.decode("utf-8", errors="replace")
    except Exception:
        return []
    candidates = []

    # Highly reliable: <meta name="citation_pdf_url" content="...">
    for m in re.finditer(r'<meta[^>]+name=["\']citation_pdf_url["\'][^>]+content=["\']([^"\']+)["\']',
                          html, re.IGNORECASE):
        candidates.append(m.group(1))
    # <embed src=...> with PDF mime
    for m in re.finditer(r'<embed[^>]+src=["\']([^"\']+\.pdf[^"\']*)["\']', html, re.IGNORECASE):
        candidates.append(m.group(1))
    for m in re.finditer(r'<iframe[^>]+src=["\']([^"\']+\.pdf[^"\']*)["\']', html, re.IGNORECASE):
        candidates.append(m.group(1))
    # <a href=...pdf>
    for m in re.finditer(r'<a[^>]+href=["\']([^"\']+\.pdf[^"\']*)["\']', html, re.IGNORECASE):
        candidates.append(m.group(1))

    out = []
    for c in candidates:
        u = urljoin(base_url, c)
        if u not in out:
            out.append(u)
    return out


# Bot walls and challenge pages (the "interstitial signature", DEC-06). Matched against the
# <title> and the first 4 KB of a non-PDF body. Live 2026-09-30: MDPI answers 403 "Access
# Denied" (Akamai); BMC landing pages answer 200 "Client Challenge".
INTERSTITIAL = re.compile(
    r"<title>\s*(?:access denied|client challenge|just a moment|attention required|"
    r"request rejected|pardon our interruption|are you a robot|security check|"
    r"checking your browser|ddos-guard|bot verification|one more step)"
    r"|cf-chl-|challenge-platform|_incapsula_resource|px-captcha|captcha-delivery|"
    r"/cdn-cgi/challenge|errors\.edgesuite\.net",
    re.IGNORECASE)


def _mentions(body, doi):
    """Does a page name this DOI (plain or percent-encoded)? A landing page does; a wall does not."""
    d = (_doi.normalise(doi or "") or "").lower()
    if not d or not body:
        return False
    low = body.decode("latin-1").lower()
    return d in low or d.replace("/", "%2f") in low


def interstitial_signature(body):
    """The matched wall phrase in a non-PDF body, or None."""
    head = (body or b"")[:4096].decode("utf-8", "replace")
    m = INTERSTITIAL.search(head)
    return m.group(0).strip()[:60] if m else None


@dataclass
class Attempt:
    """One candidate URL tried for a row (legacy `attempts` column: host_type/version/status)."""
    host_type: str
    version: str
    url: str
    status: str                      # legacy token: OK, HTML, HTTP_403, HOST_REFUSED, ...
    kind: Kind
    host: str = ""
    http_status: int | None = None
    sent: bool = True
    detail: str = ""
    size: int = 0
    field_: str = ""


@dataclass
class _Fetched:
    attempt: Attempt
    content: bytes | None = None     # the PDF bytes (OK only)
    html: bytes | None = None        # a landing page to mine for PDF links
    final_url: str = ""


def _legacy_status(o, validator_detail=""):
    """The legacy status token for a download Outcome (legacy strings the router still reads)."""
    if o.kind is Kind.OK:
        return "OK"
    if o.status is None:
        if o.kind is Kind.REFUSED:
            return "HOST_REFUSED" if o.attempts == 0 else "REDIRECT_BLOCKED"
        if o.kind in (Kind.DEFERRED, Kind.TRANSPORT):
            return str(o.kind)
        return "ERROR"
    if o.status == 200:
        if "HTML" in validator_detail:
            return "HTML"
        if "empty" in validator_detail:
            return "EMPTY"
        if "too_large" in validator_detail:
            return "TOO_LARGE"
        return "NOT_PDF"
    if 300 <= o.status < 400 and o.kind is Kind.REFUSED:
        return "REDIRECT_BLOCKED"
    return f"HTTP_{o.status}"


def _download_kind(o):
    """Row-level kind of a download Outcome: a dead OA link (404/410) is NOT_AVAILABLE."""
    return Kind.NOT_AVAILABLE if o.kind is Kind.NO_MATCH else o.kind


class Stage:
    """Per-run memory: the non-PDF body sizes seen per host (the identical-size signature) and
    the doi.org landing URLs already resolved."""

    def __init__(self, state=None):
        self.state = state
        self.nonpdf_sizes = {}           # host -> {size: doi}
        self.landing = {}                # doi.org url -> landing url or None
        self.refused_by_signature = {}   # host -> reason

    def _refuse(self, host, reason):
        if host and host not in self.refused_by_signature:
            (self.state or _state()).refuse(host, reason, persistence="run")
            self.refused_by_signature[host] = reason
            print(f"        [refuse] {host}: {reason} (refused for the rest of the run)")

    def _note_nonpdf(self, host, size, doi, body):
        """The identical-size signature: the same byte count of non-PDF body from one host for
        two DOIs, neither page naming its own DOI (a canned page, not two landing pages; a
        per-request token can vary the bytes but not the size). True when it refuses the host."""
        mentions = _mentions(body, doi)
        seen = self.nonpdf_sizes.setdefault(host, {})
        other = seen.get(size)
        if other is not None and other[0] != doi and not (mentions or other[1]):
            self._refuse(host, f"identical {size}-byte non-PDF body for two DOIs")
            return True
        seen.setdefault(size, (doi, mentions))
        return False

    def resolve_landing(self, url):
        """A doi.org URL's registered landing URL via the handle API (no redirect followed)."""
        if url in self.landing:
            return self.landing[url], None
        path = urlsplit(url).path.lstrip("/")
        d = _doi.normalise(path)
        if not d:
            self.landing[url] = None
            return None, None
        o = net.get(DOI_HANDLES + _doi.encode_path(d), params={"type": "URL"},
                    validate=net.expect_json, state=self.state, purpose="unpaywall: doi landing URL")
        landing = None
        if o.ok:
            try:
                for v in o.payload.json().get("values") or []:
                    if v.get("type") == "URL":
                        landing = (v.get("data") or {}).get("value")
                        break
            except (ValueError, AttributeError):
                landing = None
        self.landing[url] = landing
        return landing, o

    def fetch(self, url, doi, host_type="", version="", field_=""):
        """One GET of a candidate through litpipe.net. Nothing is written here."""
        a = dict(host_type=host_type or "None", version=version or "None", field_=field_)
        try:
            o = net.get(url, headers={"User-Agent": BROWSER_UA, "Accept": "application/pdf,*/*"},
                        stream=True, max_bytes=lit_net.MAX_PDF_BYTES, validate=net.expect_pdf,
                        state=self.state, purpose="unpaywall: download")
        except ProhibitedHost as e:
            return _Fetched(Attempt(url=ledger.redact(url), status="PROHIBITED", kind=Kind.SKIPPED,
                                    host=urlsplit(url).hostname or "", sent=False,
                                    detail=f"never automated: {e.rule.host}{e.rule.path_prefix}", **a))
        except ValueError as e:              # not an http(s) URL
            return _Fetched(Attempt(url=ledger.redact(url), status="ERROR", kind=Kind.ERROR,
                                    host="", sent=False, detail=str(e), **a))
        p = o.payload
        body = b""
        if p is not None:
            body = p.content if p.content is not None else (p.first_chunk or b"")
        status = _legacy_status(o, o.detail or "")
        att = Attempt(url=ledger.redact(url), status=status, kind=_download_kind(o), host=o.host,
                      http_status=o.status, sent=o.attempts > 0, detail=ledger.redact(o.detail or ""),
                      size=(p.total_bytes if p is not None else 0), **a)
        final_url = p.url if p is not None else url
        if o.ok:
            return _Fetched(att, content=p.content, final_url=final_url)
        # A non-PDF 2xx body: an HTML landing page, a wall, or a canned page.
        if o.status is not None and 200 <= o.status < 300 and status in ("HTML", "NOT_PDF", "EMPTY"):
            wall = interstitial_signature(body)
            if wall:
                att.status, att.kind = "HTML", Kind.REFUSED
                att.detail = f"interstitial ({wall})"
                self._refuse(o.host, f"interstitial signature: {wall}")
                return _Fetched(att, final_url=final_url)
            if status != "EMPTY" and self._note_nonpdf(o.host, att.size, doi, body):
                att.kind = Kind.REFUSED
                att.detail = f"identical {att.size}-byte non-PDF body for two DOIs"
                return _Fetched(att, final_url=final_url)
            if status == "HTML":
                return _Fetched(att, html=body, final_url=final_url)
        elif o.status == 403 and interstitial_signature(body):
            att.detail = f"{att.detail}; interstitial ({interstitial_signature(body)})"
        return _Fetched(att, final_url=final_url)


def try_download(url, dest, timeout=30):
    """Legacy adapter: GET `url` and, when it is a PDF of at least MIN_PDF_BYTES that is not
    known boilerplate, write it to `dest`. Returns (status, msg): ("OK", size), ("HTML", bytes),
    ("HTTP_xxx", ""), ("TOO_SMALL", "nB"), ("TOO_LARGE", ">nB"), ("EMPTY", ""), ("BOILERPLATE",
    tag), ("ERROR", detail), ... The stage itself uses Stage.fetch, which writes nothing.
    `timeout` is accepted for old callers; litpipe.net's (10 s connect, 30 s read) applies."""
    f = Stage().fetch(url, "")
    a = f.attempt
    if a.status == "OK" and f.content is not None:
        if len(f.content) < MIN_PDF_BYTES:
            return "TOO_SMALL", f"{len(f.content)}B"
        tag = boilerplate_of(f.content, _pdf_text(f.content)[0])
        if tag:
            return "BOILERPLATE", f"{tag}:{len(f.content)}B"
        _write_bytes(dest, f.content)
        return "OK", f"{len(f.content)}"
    if a.status == "HTML" and f.html is not None:
        return "HTML", f.html
    if a.status == "TOO_LARGE":
        return "TOO_LARGE", f">{a.size}B"
    if a.status.startswith("HTTP_") or a.status in ("EMPTY", "NOT_PDF", "HTML"):
        return a.status, ""
    return "ERROR" if a.status in ("ERROR", "TRANSPORT", "HOST_REFUSED", "REDIRECT_BLOCKED", "DEFERRED") else a.status, a.detail


def _write_bytes(dest, content):
    """Write PDF bytes crash-safely: a temp file beside dest, then os.replace."""
    tmp = f"{dest}.part"
    with open(tmp, "wb") as f:
        f.write(content)
    os.replace(tmp, dest)


def _pdf_text(content_or_path, pages=IDENTITY_PAGES):
    """(first-pages text, first-page text, page count) of PDF bytes or a path; ('', '', 0) when
    unreadable."""
    try:
        import fitz
    except ImportError:
        return "", "", 0
    try:
        if isinstance(content_or_path, (bytes, bytearray)):
            doc = fitz.open(stream=bytes(content_or_path), filetype="pdf")
        else:
            doc = fitz.open(str(content_or_path))
    except Exception:
        return "", "", 0
    try:
        n = doc.page_count
        texts = [doc[i].get_text() for i in range(min(pages, n))]
    except Exception:
        return "", "", 0
    finally:
        doc.close()
    return "\n".join(texts), (texts[0] if texts else ""), n


def judge_identity(content_or_path, doi, title):
    """(Verdict, DocKind, n_pages) for a PDF against the queue DOI and title."""
    text, first, n = _pdf_text(content_or_path)
    verdict = _identity.check(text, doi, queue_title=title)
    kind = _identity.doc_kind(first, n)
    return verdict, kind, n


def _flagged(verdict, kind):
    return (not verdict.ok) or kind == _identity.DocKind.SUPPLEMENT


def write_identity_sidecar(pdf_path, doi, verdict, kind, n_pages, attempt, source="unpaywall"):
    """`<final stem>.identity.json`: the decision and its evidence on the artifact (never moved)."""
    rec = {"queue_doi": _doi.normalise(doi) or doi, "pdf": os.path.basename(pdf_path),
           "source": source, "source_url": ledger.redact(attempt.url), "host": attempt.host,
           "host_type": attempt.host_type, "version": attempt.version,
           "checked_at": ledger.now_iso(), "n_pages": n_pages, "doc_kind": str(kind)}
    rec.update(verdict.as_dict())
    path = str(lit_util.companion_path(pdf_path, ".identity.json"))
    lit_util.atomic_write_json(path, json.loads(ledger.redact(json.dumps(rec, ensure_ascii=False))))
    return path


def existing_holds(pdf_path, doi, title=""):
    """Is the file at `pdf_path` the paper `doi`? "same", "other" or "unknown" (REG-I11: a file
    with no DOI and no title match is not this paper). Sidecar DOI first, then this stage's
    identity sidecar, then the identity check on the file's text."""
    want = _doi.normalise(doi)
    d = _sidecar_doi(pdf_path)
    if d:
        return "same" if d == want else "other"
    ids = _read_identity_sidecar(pdf_path)
    if ids and _doi.normalise(ids.get("queue_doi") or "") == want:
        ok = ids.get("identity") in ("OK", "TITLE_MATCH") and ids.get("doc_kind") != "SUPPLEMENT"
        return "same" if ok else "other"
    verdict, kind, n = judge_identity(pdf_path, doi, title)
    if not _flagged(verdict, kind):
        return "same"
    return "other" if verdict.evidence.get("pdf_dois") else "unknown"


def library_index(lib_dir):
    """{doi: pdf filename} for the destination library: `.ris` DO lines and verified
    `.identity.json` records whose PDF is present (the same-library holding check)."""
    out = {}
    try:
        names = os.listdir(lib_dir)
    except OSError:
        return out
    present = set(names)
    for fn in names:
        low = fn.lower()
        if low.endswith(".ris"):
            pdf = fn[:-4] + ".pdf"
            if pdf not in present:
                continue
            d = _sidecar_doi(os.path.join(lib_dir, pdf))
            if d:
                out.setdefault(d, pdf)
        elif low.endswith(".identity.json"):
            pdf = fn[:-len(".identity.json")] + ".pdf"
            if pdf not in present:
                continue
            ids = _read_identity_sidecar(os.path.join(lib_dir, pdf)) or {}
            d = _doi.normalise(ids.get("queue_doi") or "")
            if d and ids.get("identity") in ("OK", "TITLE_MATCH") and ids.get("doc_kind") != "SUPPLEMENT":
                out.setdefault(d, pdf)
    return out


def _row_kind(attempts):
    """The row's root-cause kind over failed attempts: a download host that refused us first
    (OA_BLOCKED), then a transient failure, then the first attempt actually made."""
    if any(a.kind is Kind.REFUSED for a in attempts):
        return Kind.REFUSED
    for k in (Kind.OUTAGE, Kind.TRANSPORT, Kind.DEFERRED):
        if any(a.kind is k for a in attempts):
            return k
    sent = [a for a in attempts if a.sent]
    if sent:
        return sent[0].kind
    return Kind.NOT_AVAILABLE      # nothing could be sent: only never-automated routes


def _decisive(attempts, kind):
    """The root-cause attempt for the row's kind: a hard answer (a non-200 status, a host
    refusal, a wall or canned-page signature) before a plain HTML page of the same kind."""
    same = [a for a in attempts if a.kind is kind]
    hard = [a for a in same if (a.http_status and a.http_status != 200) or not a.sent
            or a.detail.startswith(("interstitial", "identical"))]
    return (hard or same or attempts or [None])[0]


def download_row(stage, cands, doi, title):
    """Try each candidate (and up to HTML_FALLBACK_LINKS links from an HTML landing page) until
    one yields a PDF that passes the size and boilerplate checks. Writes nothing. Returns
    (attempts, winner Attempt or None, PDF bytes or None, (Verdict, DocKind, n_pages) or None);
    the identity verdict does not stop the loop's success: a FLAGged PDF is still the result,
    and the caller keeps it where it writes it."""
    attempts = []

    def consider(f):
        a = f.attempt
        if a.status != "OK" or f.content is None:
            return None
        if len(f.content) < MIN_PDF_BYTES:
            a.status, a.kind, a.detail = "TOO_SMALL", Kind.ERROR, f"{len(f.content)}B"
            return None
        text, first, n = _pdf_text(f.content)
        tag = boilerplate_of(f.content, text)
        if tag:
            a.status, a.kind, a.detail = "BOILERPLATE", Kind.ERROR, f"{tag}:{len(f.content)}B"
            return None
        verdict = _identity.check(text, doi, queue_title=title)
        kind = _identity.doc_kind(first, n)
        return verdict, kind, n

    for host_type, version, url, field_ in cands:
        target = url
        if (urlsplit(url).hostname or "").lower() in DOI_HOSTS:
            landing, ho = stage.resolve_landing(url)
            if not landing:
                attempts.append(Attempt(host_type or "None", version or "None", ledger.redact(url),
                                        "NO_LANDING" if ho is None or ho.ok else f"HANDLE:{ho.kind}",
                                        Kind.ERROR if ho is None or ho.ok else ho.kind,
                                        host="doi.org", http_status=getattr(ho, "status", None),
                                        sent=bool(ho and ho.attempts), field_=field_,
                                        detail="doi.org handle has no URL" if ho is None or ho.ok
                                        else ledger.redact(ho.detail)))
                continue
            target = landing
        f = stage.fetch(target, doi, host_type, version, field_)
        attempts.append(f.attempt)
        got = consider(f)
        if got:
            return attempts, f.attempt, f.content, got
        if f.html is not None:
            for link in extract_pdf_links_from_html(f.html, f.final_url or target)[:HTML_FALLBACK_LINKS]:
                f2 = stage.fetch(link, doi, "html-fallback", version, "link")
                attempts.append(f2.attempt)
                got = consider(f2)
                if got:
                    return attempts, f2.attempt, f2.content, got
    return attempts, None, None, None

# ---------- main ----------

def _clean(row):
    """Every string that reaches the report passes ledger.redact (no email, key or token)."""
    return {k: (ledger.redact(v) if isinstance(v, str) else v) for k, v in row.items()}


def _config_problem():
    return preflight.email_problem(os.environ.get(preflight.EMAIL_ENV))


def run(*, top_n=100, dry_run=False, min_cites=0, base_dir=None, triage=None, lib_dir=None,
        report=None, no_write_ris=False, candidate_order="repository", state=None):
    """The stage (dispatch 0.5 stage-function contract). Returns a summary dict with `exit_code`."""
    base = os.path.abspath(base_dir or os.getcwd())
    triage_csv = triage or os.path.join(base, DEFAULT_TRIAGE)
    lib_dir = lib_dir or os.path.join(base, DEFAULT_LIB)
    out_report = report or os.path.join(base, DEFAULT_REPORT)
    if candidate_order not in CANDIDATE_ORDERS:
        raise ValueError(f"candidate_order must be one of {CANDIDATE_ORDERS}")

    if not os.path.exists(triage_csv):
        print(f"ERR: triage CSV not found: {triage_csv}", file=sys.stderr)
        return {"exit_code": EXIT_NO_TRIAGE}

    with open(triage_csv, encoding="utf-8") as f:
        triage_rows = list(csv.DictReader(f))
    # T5d (2026-06-25 audit): a non-numeric citation_count cell ('n/a', '1,234') would crash
    # the whole project's fetch with ValueError. Coerce via the shared lit_util helper.
    triage_top = [r for r in triage_rows
                  if lit_util.coerce_int(r.get("citation_count")) >= min_cites][:top_n]
    print(f"Project: {base}")
    print(f"Triage:  {triage_csv}")
    print(f"Library: {lib_dir}")
    print(f"Report:  {out_report}")
    print(f"Attempting top {len(triage_top)} (min_cites={min_cites}, candidate order {candidate_order})"
          f"{' [DRY RUN]' if dry_run else ''}\n")

    os.makedirs(lib_dir, exist_ok=True)
    report_dir = os.path.dirname(out_report)
    if report_dir:
        os.makedirs(report_dir, exist_ok=True)
    existing = set(os.listdir(lib_dir))
    held = library_index(lib_dir)
    written_this_run = set()   # RC2: dest paths written in this run; never clobber them
    stage = Stage(state)
    results = []
    counts = dict(oa=0, dl=0, skip=0, closed=0, oa_no_url=0, fail=0, flag=0, not_at_ra=0, no_match=0)
    aborted = None

    ras = registration_agencies([r.get("doi") or "" for r in triage_top], state=state) if triage_top else {}

    for i, row in enumerate(triage_top, 1):
        raw_doi = (row.get("doi") or "").strip()
        if not raw_doi:
            continue
        doi = _doi.normalise(raw_doi)
        doi_col = doi or raw_doi.lower()
        title = row.get("title", "") or ""
        year = row.get("year", "") or ""
        authors = row.get("authors", "") or ""
        cites = row.get("citation_count", 0)
        fn = build_filename(year, authors, title)
        out = {"rank": i, "doi": doi_col, "year": year, "cites": cites, "filename": fn,
               "title": title[:120], "oa_status": "", "n_locations": 0, "downloaded": False,
               "winning_host": "", "winning_url": "", "attempts": "", "error": "",
               "first_status": "", "route": "", "outcome": "", "detail": "", "ra": "",
               "identity": "", "doc_kind": ""}

        def done(kind, route, error="", detail=""):
            # legacy readers (sweep, migrate) map `error` through from_legacy: make it name `kind`
            if error and kind not in (Kind.OK, Kind.ERROR) and from_legacy(error, "unpaywall") is not kind:
                error = f"{error} [{kind}]"
            out.update(outcome=str(kind), route=route, error=error, detail=detail)
            results.append(out)

        if not doi:
            counts["fail"] += 1
            done(Kind.ERROR, "none", "INVALID_DOI", f"not a DOI: {raw_doi!r}")
            print(f"  [{i:>3}] {raw_doi[:55]:<55} INVALID_DOI")
            continue

        # 1. already in the destination library (by DOI, then by this or the legacy filename)
        hit = held.get(doi)
        if not hit:
            for name in dict.fromkeys((fn, legacy_build_filename(year, authors, title))):
                if name in existing and existing_holds(os.path.join(lib_dir, name), doi, title) == "same":
                    hit = name
                    break
        if hit:
            counts["skip"] += 1
            out.update(filename=hit, oa_status="SKIP_EXISTS")
            done(Kind.OK, "exists", detail="already in the library")
            print(f"  [{i:>3}] {hit[:75]:<75} SKIP")
            continue

        # 2. registration agency: Unpaywall holds Crossref DOIs only
        ra = ras.get(_prefix(doi))
        out["ra"] = ra or ""
        if ra and ra.lower() != "crossref":
            counts["not_at_ra"] += 1
            done(Kind.NOT_AT_RA, "ra", f"NOT_IN_UNPAYWALL: registered at {ra} (not sent)",
                 f"{ra} DOI; Unpaywall covers Crossref DOIs only")
            print(f"  [{i:>3}] {doi[:55]:<55} NOT_AT_RA ({ra})")
            continue

        # 3. Unpaywall
        problem = _config_problem()
        if problem:
            aborted = problem
            done(Kind.CONFIG, "api", f"CONFIG: {problem}", f"{problem}; {preflight.EMAIL_FIX}")
            print(f"  [{i:>3}] {doi[:55]:<55} CONFIG: {problem}; stage stopped", file=sys.stderr)
            break
        o = unpaywall_lookup(doi, state=state)
        if not o.ok:
            out["first_status"] = o.status or ""
            legacy = f"HTTP {o.status}" if o.status else ledger.redact(o.detail) or str(o.kind)
            if o.kind is Kind.NO_MATCH:
                counts["no_match"] += 1
            else:
                counts["fail"] += 1
            done(o.kind, "api", legacy, ledger.redact(o.detail))
            print(f"  [{i:>3}] {doi[:55]:<55} API {o.kind}: {legacy}")
            if o.kind is Kind.CONFIG:
                aborted = f"Unpaywall answered {o.status}: check LITPIPE_EMAIL (or a retired endpoint)"
                print(f"  CONFIG: {aborted}; stage stopped", file=sys.stderr)
                break
            continue
        upw = o.payload.json()
        out["first_status"] = o.status or ""
        if not isinstance(upw, dict):
            counts["fail"] += 1
            done(Kind.OUTAGE, "api", "NOT_A_RECORD", "Unpaywall answered JSON that is not a record")
            print(f"  [{i:>3}] {doi[:55]:<55} API: not a record")
            continue

        is_oa = bool(upw.get("is_oa", False))
        cands = candidate_urls(upw, candidate_order) if is_oa else []
        out["oa_status"] = "OA" if is_oa else "CLOSED"
        out["n_locations"] = len(cands)
        if not is_oa:
            counts["closed"] += 1
            done(Kind.NOT_AVAILABLE, "api", "", "closed: no OA location")
            print(f"  [{i:>3}] {doi[:55]:<55} CLOSED")
            continue
        counts["oa"] += 1
        if not cands:
            counts["oa_no_url"] += 1
            done(Kind.NOT_AVAILABLE, "api", "OA_NO_URL", "OA record without a URL")
            print(f"  [{i:>3}] {doi[:55]:<55} OA-no-URL")
            continue

        # DEC-14: an unusable queue author takes the record's first author (new files only)
        if last_name(authors) == "Unknown":
            ra_name = first_author_of(upw)
            if ra_name and last_name(ra_name) != "Unknown":
                fn = build_filename(year, ra_name, title)
                out["filename"] = fn
                if fn in existing and existing_holds(os.path.join(lib_dir, fn), doi, title) == "same":
                    counts["skip"] += 1
                    out["oa_status"] = "SKIP_EXISTS"
                    done(Kind.OK, "exists", detail="already in the library (record's first author)")
                    print(f"  [{i:>3}] {fn[:75]:<75} SKIP")
                    continue

        if dry_run:
            out["winning_url"] = cands[0][2]
            done(Kind.SKIPPED, "dry_run", "DRY", "dry run: nothing downloaded")
            print(f"  [{i:>3}] {fn[:60]:<60} OA: {len(cands)} cand, [0]={cands[0][2][:60]}")
            continue

        # 4-5. download
        attempts, winner, content, judged = download_row(stage, cands, doi, title)
        out["attempts"] = " | ".join(f"{a.host_type}/{a.version}/{a.status}" for a in attempts)
        if winner is None:
            counts["fail"] += 1
            kind = _row_kind(attempts)
            decisive = _decisive(attempts, kind)
            # first_status: the root cause's HTTP status, not the last fallback's (blank: not sent)
            out["first_status"] = decisive.http_status if decisive and decisive.sent and \
                decisive.http_status else ""
            error = decisive.status if decisive else "no candidates"
            detail = f"{decisive.host}: {decisive.status} {decisive.detail}".strip() if decisive else ""
            done(kind, decisive.host_type if decisive else "none", error, detail)
            print(f"  [{i:>3}] {fn[:65]:<65} FAIL ({len(attempts)} tries: {error}; {kind})")
            continue

        # 6. write, then record the identity verdict on the artifact (never moved)
        verdict, dkind, n_pages = judged
        out["first_status"] = winner.http_status or ""
        dest, collided = resolve_dest(lib_dir, fn, doi, written_this_run)
        if collided:
            out["filename"] = os.path.basename(dest)
        _write_bytes(dest, content)
        written_this_run.add(dest)
        existing.add(os.path.basename(dest))
        write_identity_sidecar(dest, doi, verdict, dkind, n_pages, winner)
        out.update(identity=str(verdict.decision), doc_kind=str(dkind),
                   winning_host=winner.host_type, winning_url=winner.url)
        shown = out["filename"]
        if _flagged(verdict, dkind):
            counts["flag"] += 1
            pdf_doi = (verdict.evidence.get("pdf_dois") or [""])[0]
            why = "doc_kind=SUPPLEMENT" if verdict.ok else f"identity={verdict.decision}"
            done(Kind.ERROR, winner.host_type, f"DOI_MISMATCH:pdf_doi={pdf_doi or 'none'};{why}",
                 f"identity flag kept in place: {verdict.evidence.get('reason') or why}")
            print(f"  [{i:>3}] {shown[:65]:<65} IDENTITY FLAG ({why}); file kept, no .ris")
            continue
        counts["dl"] += 1
        out["downloaded"] = True
        done(Kind.OK, winner.host_type, "", f"{winner.host}; identity {verdict.decision}")
        print(f"  [{i:>3}] {shown[:65]:<65} DL ({winner.host_type}, {len(attempts)} tries)")
        if not no_write_ris:
            ris_status, _ = _R.emit_ris_for_pdf(doi, dest)
            print(f"        ris: {ris_status}")

    # RC4: build the CSV in memory then write atomically (tmp + os.replace).
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=REPORT_FIELDS, extrasaction="ignore", lineterminator="\n")
    w.writeheader()
    w.writerows(_clean(r) for r in results)
    lit_util.atomic_write_text(out_report, buf.getvalue())

    print("\n=== Summary ===")
    print(f"  Attempted:   {len(triage_top)}")
    print(f"  Skipped:     {counts['skip']} (already in library)")
    print(f"  Not at RA:   {counts['not_at_ra']} (not a Crossref DOI; Unpaywall not asked)")
    print(f"  Not found:   {counts['no_match']} (Unpaywall 404)")
    print(f"  Closed:      {counts['closed']}")
    print(f"  OA-no-URL:   {counts['oa_no_url']}")
    print(f"  OA usable:   {counts['oa'] - counts['oa_no_url']}")
    print(f"  DOWNLOADED:  {counts['dl']}")
    print(f"  Identity flag:{counts['flag']} (file kept in place, no .ris)")
    print(f"  Failed:      {counts['fail']}")
    if stage.refused_by_signature:
        print(f"  Hosts refused by signature: {', '.join(sorted(stage.refused_by_signature))}")
    print(f"\nReport: {out_report}")
    code = EXIT_CONFIG if aborted else EXIT_OK
    if aborted:
        print(f"\nCONFIG: {aborted}. {preflight.EMAIL_FIX}", file=sys.stderr)
    return {"exit_code": code, "report": out_report, "rows": len(results), "aborted": aborted,
            **counts, "refused_hosts": sorted(stage.refused_by_signature)}


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    ap.add_argument("--top-n", type=int, default=100,
                     help="Maximum number of triage rows to attempt (default: 100).")
    ap.add_argument("--dry-run", action="store_true",
                     help="Look up Unpaywall metadata but do not download PDFs.")
    ap.add_argument("--min-cites", type=int, default=0,
                     help="Skip rows with citation_count below this threshold.")
    ap.add_argument("--base-dir", default=os.getcwd(),
                     help="Project root. Default: CWD.")
    ap.add_argument("--triage", default=None,
                     help=f"Triage CSV path (default: <base-dir>/{DEFAULT_TRIAGE})")
    ap.add_argument("--lib-dir", default=None,
                     help=f"PDF destination (default: <base-dir>/{DEFAULT_LIB})")
    ap.add_argument("--report", default=None,
                     help=f"Output report CSV (default: <base-dir>/{DEFAULT_REPORT})")
    ap.add_argument("--no-write-ris", action="store_true",
                     help="Skip writing .ris sidecar next to each successfully fetched PDF.")
    ap.add_argument("--candidate-order", choices=CANDIDATE_ORDERS, default="repository",
                     help="Try repository locations first (default) or publisher locations first "
                          "(plan DEC-11: the runner alternates the two for an A/B).")
    args = ap.parse_args()
    res = run(top_n=args.top_n, dry_run=args.dry_run, min_cites=args.min_cites,
              base_dir=args.base_dir, triage=args.triage, lib_dir=args.lib_dir,
              report=args.report, no_write_ris=args.no_write_ris,
              candidate_order=args.candidate_order)
    if res["exit_code"]:            # success returns, as before: in-process callers carry on
        sys.exit(res["exit_code"])


if __name__ == "__main__":
    main()
