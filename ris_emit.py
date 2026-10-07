"""Shared RIS emission and the metadata clients behind it (W2-E1; refactor scope 3.5).

Used by harvest_citations (Downloads to _references/citations/*.ris), backfill_ris (existing PDF
libraries), and as a hook by unpaywall_fetch_v2, pmc_fetch, preprint_fetch and paywall_pull. The
`.ris` files feed EndNote bibliographies, so each field is written the way the registry holds it:

- Text: tags stripped FIRST, then character references decoded (litpipe.text: HTML5 plus the ISO
  Greek set, two passes for double escaping), canonical composition (NFC), the seven ligatures
  expanded, soft hyphens and zero-width characters dropped, no-break and thin spaces and the
  no-break hyphen made plain. Not full NFKC: it is lossy for display (VO₂max, m²,
  µ, ™, CD8⁺, "O´Reilly" gains a space). Measured 2026-09-30 over 6,087 live
  `.ris` files and 229,580 `paper_metadata` titles: NFKC would change 9 TI, 106 AB, 2 AU lines and
  439 DB titles beyond ligatures and spaces, every one a display loss. Comparison keeps NFKC
  (litpipe.text.normalise_title); display fields use _display().
- Title and subtitle are joined with ": " (Crossref `subtitle[]`, DataCite titleType Subtitle;
  REG-I49; plan 4.4, decided 2026-09-27), unless the title already ends with the subtitle.
- Year and date: Crossref `published-print`, then `journal-issue.published-print`, then
  `published`, then `issued` (plan 4.4). `issued` is the earliest of print and online, so it gave
  online-first years (4 wrong years in one manuscript build).
- DataCite `publisher` is a string today and an object by default from September 2027; either
  gives its name (N-A4). DA comes only from Issued or a past Available date: Created and Updated
  are registration timestamps, and OSF records carry no Issued since Q2 2026 (N-A5).
- DOIs are normalised (litpipe.doi) before they are encoded into a URL path or a UR line (I47,
  N-A8). The record's own DOI is written, so an alias yields the prime DOI (N-A3).
- Every request goes through litpipe.net (one identity, pacing, ledger, typed Outcome). A
  transport failure, 5xx, refusal or deferral raises MetadataUnavailable; only a genuine
  not-found is None (REG-I46). resolve_meta keeps its (meta, source) shape.
- Agencies other than Crossref and DataCite (mEDRA, JaLC, KISTI, OP) are read through DOI content
  negotiation, which litpipe.net follows only to its https allow-list; ISTIC's http-only host is
  never followed (source "unsupported:istic").
- DEC-29: write_ris records the sha256 of every file it writes (state kv namespace "ris", keyed by
  the real path) and replaces an existing file only while its hash still equals that record;
  a file with no record, or edited since, is treated as curated and kept unless force=True.

Sources read 2026-09-30:
- Crossref REST API swagger (https://api.crossref.org/swagger-docs, version 4.1.0): Work has
  `title` and `subtitle` (array of string), `published`, `published-print`, `published-online`,
  `issued` (DateParts) and `journal-issue` (WorkJournalIssue: `issue`, `published-online`,
  `published-print`); DateParts `date-parts` items are integers or null. The field reference
  (https://github.com/CrossRef/rest-api-doc/blob/master/api_format.md): `issued` "Earliest of
  `published-print` and `published-online`"; `subtitle` "Work subtitles, including original
  language and translated"; date-parts "an ordered array of `year`, `month`, `day of month`. Only
  `year` is required." `query.title` "has been deprecated and is no longer available. Please use
  `query.bibliographic` instead."
- Crossref rate limits (https://community.crossref.org/t/refining-rest-api-limits-for-improved-stability-and-reliability/16137,
  21 July 2026): "we will rate-limit by email address"; "Repeated use of invalid email addresses
  could lead to your API access being blocked"; polite 10/s single, 3/s lists.
- DataCite (https://support.datacite.org/docs/api-get-doi, updated 2026-09-25): "add
  `?affiliation=true&publisher=true` to your request" for the publisher object (`name`,
  `publisherIdentifier`, ...); from September 2027 that is the default. Metadata Schema 4.7:
  "Unless otherwise indicated by titleType, a title is considered to be the main title";
  titleType values "AlternativeTitle, Subtitle, TranslatedTitle, Other"; dateType Issued "The date
  that the resource is published or distributed", Available "The date the resource is made
  publicly available. May be a range.", Created "the date the resource itself was put together".
- DOI content negotiation (https://citation.doi.org/docs.html): requests "that ask for a content
  type which isn't 'text/html' will generally be redirected to a metadata service hosted by the
  DOI's registration agency"; CSL-JSON is `application/vnd.citationstyles.csl+json`; 204 "no
  metadata available", 404 "The DOI requested doesn't exist", 406 "Can't serve any requested
  content type."
- RIS (https://en.wikipedia.org/wiki/RIS_(file_format)): tag lines are "Two letters, two spaces
  and a hyphen"; TY first, ER last; dates "a slash-separated list of 4-digit year, 2-digit month,
  2-digit day".
"""
import difflib
import hashlib
import os
import re
import sys
from datetime import date

# Config loader + safe_ascii live in lit_util (stdlib-pure); re-exported here so the many
# `from ris_emit import load_projects_config` / `from ris_emit import safe_ascii` call sites
# (audit_portfolio, preprint_fetch, unpaywall_fetch_v2, test_filenames) keep working unchanged.
# load_projects_config still exits 2 on a missing file (tests/test_api_surface.py).
from lit_util import load_projects_config, safe_ascii  # noqa: F401
from litpipe import doi as _doi
from litpipe import net
from litpipe import text as _text
from litpipe.ledger import redact
from litpipe.outcomes import Kind

CROSSREF_WORK = "https://api.crossref.org/works/{doi}"      # {doi}: litpipe.doi.encode_path output
CROSSREF_SEARCH = "https://api.crossref.org/works"
DATACITE_WORK = "https://api.datacite.org/dois/{doi}"
DOI_RA = "https://doi.org/doiRA/{doi}"
DOI_CN = "https://doi.org/{doi}"
CSL_JSON = "application/vnd.citationstyles.csl+json"
# DataCite: ask for the publisher object now; it becomes the default in September 2027.
DATACITE_PARAMS = {"publisher": "true"}
RA_CACHE_TTL_S = 30 * 24 * 3600
# Agencies read through content negotiation (doi.org /doiRA names, lower-cased). ISTIC's CN host is
# plain http on a raw IP (V3), which litpipe.net never follows from https.
CN_AGENCIES = frozenset({"medra", "jalc", "kisti", "op"})
UNSUPPORTED_AGENCIES = frozenset({"istic"})

# Tests inject a state object here; otherwise litpipe.net's injected state, else litpipe.state.
STATE = None


class MetadataUnavailable(RuntimeError):
    """A metadata source could not answer (transport failure, 5xx, refusal, deferral, a 200 that is
    not the expected JSON): nothing can be concluded about the DOI. A not-found is None instead.
    The message is redacted; `outcome` is the litpipe.net Outcome when there was one."""

    def __init__(self, source, outcome=None, detail=""):
        self.source = source
        self.outcome = outcome
        self.kind = getattr(outcome, "kind", Kind.ERROR)
        self.status = getattr(outcome, "status", None)
        why = detail or getattr(outcome, "detail", "") or ""
        msg = f"{source}: {self.kind}" + (f" (HTTP {self.status})" if self.status else "")
        super().__init__(redact(msg + (f": {why}" if why else "")))


# ---------------------------------------------------------------------------- contact address
_email_warned = False


def warn_if_default_email():
    """One-shot stderr warning when LITPIPE_EMAIL is unset, empty or not usable (not an address, or
    an RFC 2606 reserved domain such as example.com or .test). There is no code default (DEC-13):
    without an address litpipe.net sends no mailto and no email= at all. Entry points (sweep.py,
    snowball.py) call this at startup. The address itself is never printed."""
    global _email_warned
    if _email_warned:
        return
    from litpipe.preflight import EMAIL_FIX, email_problem
    problem = email_problem(os.environ.get("LITPIPE_EMAIL"))
    if not problem:
        return
    _email_warned = True
    print(f"[litpipe] {problem}: API requests go out without a usable contact address. Unpaywall "
          "answers HTTP 422 to a missing address (and to example.com), which fails every "
          "Unpaywall lookup, and Crossref says repeated invalid addresses can get API access "
          f"blocked. Fix: {EMAIL_FIX}.", file=sys.stderr)


# ---------------------------------------------------------------------------- text
def _display(s) -> str:
    """A metadata string as it should read in a bibliography: litpipe.text.display_field (tags
    stripped, then references decoded, NFC, ligatures expanded, invisibles dropped, U+2011 made "-",
    odd spaces made plain, whitespace collapsed; compatibility characters kept). One definition,
    shared with the abstract and backfill writers."""
    return _text.display_field(s)


def _first(v) -> str:
    """The first string of a Crossref array field (or the field itself when it is a string)."""
    if isinstance(v, (list, tuple)):
        for x in v:
            if isinstance(x, str) and x.strip():
                return x
        return ""
    return v if isinstance(v, str) else ""


def join_title(title, subtitle="") -> str:
    """Main title and subtitle as one display title, joined with ": " (plan 4.4). A subtitle the
    title already ends with is not repeated; a title ending in ':' does not gain a second one."""
    t, s = _display(title), _display(subtitle)
    if not s:
        return t
    if not t:
        return s
    if _text.normalise_title(t).endswith(_text.normalise_title(s)):
        return t
    return t.rstrip(" :") + ": " + s


# ---------------------------------------------------------------------------- slugs and stems
SLUG_SKIP = {"a","an","the","of","in","on","and","to","for","at","from","with","by","as",
             "or","is","are","be","been","this","that","these","those"}


def slug(text: str, n: int = 6) -> str:
    text = re.sub(r"<[^>]+>", "", text or "")
    text = safe_ascii(text)
    text = re.sub(r"[^A-Za-z0-9\s\-]", " ", text)
    words = [w for w in text.split() if w.lower() not in SLUG_SKIP][:n]
    return "".join(re.sub(r"[^A-Za-z0-9\-]", "", w).capitalize() for w in words) or "Untitled"


def canonical_stem(year, lastname, title) -> str:
    """YYYY_Lastname_TitleSlug. Entities are decoded first, so `M&uuml;ndel` gives `Mundel`, not
    `Muumlndel` (2026-09-17 intake note, P1)."""
    yr   = str(year) if year and re.match(r"^\d{4}$", str(year)) else "Unknown"
    last = safe_ascii(re.sub(r"[^\w\-]", "", _display(lastname))) or "Unknown"
    sl   = safe_ascii(slug(_display(title)))
    return f"{yr}_{last}_{sl}"


def normalize_title(t: str) -> str:
    t = re.sub(r"<[^>]+>", "", t or "").lower()
    t = re.sub(r"[^a-z0-9 ]", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def title_similarity(a: str, b: str) -> float:
    # The comparison fold (W5 item 25) applies here only: normalize_title itself stays unfolded,
    # because harvest_citations keys a paper's identity (and a collision hash) on it.
    fold = _text.comparison_fold
    return difflib.SequenceMatcher(None, normalize_title(fold(a or "")), normalize_title(fold(b or ""))).ratio()


# ---------------------------------------------------------------------------- network
def _doi_url(doi) -> str:
    """https://doi.org/<encoded DOI>, or "" when `doi` holds no DOI."""
    try:
        return "https://doi.org/" + _doi.encode_path(doi)
    except ValueError:
        return ""


def _state():
    if STATE is not None:
        return STATE
    if net.STATE is not None:
        return net.STATE
    import litpipe.state as st
    return st


def _get_json(url, source, *, params=None, headers=None, timeout=None, validate=net.expect_json):
    """The parsed JSON body; None for a not-found (404/410, or a validator's NO_MATCH);
    MetadataUnavailable for anything else."""
    kw = {"params": params, "headers": headers, "validate": validate, "purpose": f"ris_emit:{source}"}
    if timeout is not None:
        kw["timeout"] = timeout
    out = net.get(url, **kw)
    if out.ok:
        try:
            return out.payload.json()
        except ValueError:
            raise MetadataUnavailable(source, out, "body is not JSON") from None
    if out.kind in (Kind.NO_MATCH, Kind.NOT_AVAILABLE):   # 404/410, a CN 204, a CN 406 (d16149d)
        return None
    raise MetadataUnavailable(source, out)


def crossref_by_doi(doi: str, timeout=15):
    """The Crossref /works message for `doi`; None when Crossref has no such record (404).
    Raises MetadataUnavailable when Crossref could not answer. An alias redirects to its prime
    record, whose DOI the message carries."""
    d = _doi.normalise(doi) if doi else None
    if not d:
        return None
    body = _get_json(CROSSREF_WORK.format(doi=_doi.encode_path(d)), "crossref", timeout=timeout)
    if body is None:
        return None
    msg = body.get("message") if isinstance(body, dict) else None
    if not isinstance(msg, dict):
        raise MetadataUnavailable("crossref", detail="200 without a message object")
    return msg


def _date_parts(obj) -> list:
    """[year, month?, day?] from a Crossref partial date ({"date-parts": [[y, m, d]]}); [] when
    absent or null ({"date-parts": [[null]]} occurs)."""
    dp = obj.get("date-parts") if isinstance(obj, dict) else None
    first = dp[0] if isinstance(dp, list) and dp and isinstance(dp[0], list) else []
    parts = []
    for x in first[:3]:
        try:
            parts.append(int(x))
        except (TypeError, ValueError):
            break
    if not parts or not 1000 <= parts[0] <= 9999:
        return []
    return parts


# Plan 4.4 (decided 2026-09-27): print, then the issue's print date, then published, then issued.
CROSSREF_DATE_ORDER = (("published-print",), ("journal-issue", "published-print"), ("published",),
                       ("issued",))


def _crossref_date(msg) -> list:
    for path in CROSSREF_DATE_ORDER:
        obj = msg
        for k in path:
            obj = obj.get(k) if isinstance(obj, dict) else None
        parts = _date_parts(obj)
        if parts:
            return parts
    return []


def _date_str(parts) -> str:
    return "/".join(str(x) if i == 0 else f"{x:02d}" for i, x in enumerate(parts))


def crossref_by_title(title: str, author_lastname: str = "", year: str = "",
                      timeout=20, sim_threshold: float = 0.85):
    """Fuzzy Crossref search. Returns the best item's message dict when its title (with or without
    its subtitle) is at least `sim_threshold` similar and, given a year, its year (plan 4.4
    precedence) is within one; else None. Raises MetadataUnavailable when Crossref could not
    answer. Uses query.bibliographic (query.title is deprecated)."""
    if not title or len(title.strip()) < 8:
        return None
    params = {"query.bibliographic": title, "rows": 5}
    if author_lastname:
        params["query.author"] = author_lastname
    body = _get_json(CROSSREF_SEARCH, "crossref", params=params, timeout=timeout)
    if body is None:
        return None
    items = ((body.get("message") or {}).get("items") or []) if isinstance(body, dict) else []
    best = None
    best_sim = 0.0
    for it in items:
        main = _display(_first(it.get("title")))
        full = join_title(_first(it.get("title")), _first(it.get("subtitle")))
        sim = max(title_similarity(title, main), title_similarity(title, full))
        if sim > best_sim:
            best_sim = sim
            best = it
    if best and best_sim >= sim_threshold:
        if year and re.match(r"^\d{4}$", str(year)):
            parts = _crossref_date(best)
            if parts and abs(parts[0] - int(year)) > 1:
                return None
        return best
    return None


def _author(family, given) -> dict:
    return {"family": _display(family), "given": _display(given)}


def crossref_meta(msg: dict) -> dict:
    """Flatten a Crossref /works message into the dict build_ris consumes."""
    if not msg:
        return {}
    parts = _crossref_date(msg)
    authors = []
    for a in msg.get("author") or []:
        if not isinstance(a, dict):
            continue
        fam = a.get("family") or a.get("name") or ""      # an organisation has only `name`
        if _display(fam):
            authors.append(_author(fam, a.get("given")))
    d = (msg.get("DOI") or "").strip().lower()
    issn = msg.get("ISSN") or []
    return {
        "doi":      d,
        "title":    join_title(_first(msg.get("title")), _first(msg.get("subtitle"))),
        "year":     str(parts[0]) if parts else "",
        "date":     _date_str(parts),
        "lastname": authors[0]["family"] if authors else "",
        "authors":  authors,
        "container": _display(_first(msg.get("container-title"))),
        "volume":   _display(msg.get("volume")),
        "issue":    _display(msg.get("issue")),
        "page":     _display(msg.get("page")),
        "issn":     _first(issn) if isinstance(issn, list) else str(issn or ""),
        "abstract": _text.abstract_field(msg.get("abstract")),
        "url":      _doi_url(d) if d else "",
        "type":     msg.get("type") or "journal-article",
    }


# ---------- DataCite resolver (2026-07-20) ----------
# arXiv (10.48550/*), Zenodo (10.5281/*), figshare, Dryad, OSF are registered with DataCite, NOT
# Crossref, so crossref_by_doi finds nothing and resolve_meta falls back to DataCite, which returns
# the same flattened dict shape.

# DataCite resourceTypeGeneral -> Crossref-style `type` key that _RIS_TYPE maps.
_DC_TYPE = {
    "Preprint": "posted-content", "Text": "journal-article",
    "JournalArticle": "journal-article", "ConferencePaper": "proceedings-article",
    "Dataset": "dataset", "Software": "posted-content", "Book": "book",
    "BookChapter": "book-chapter", "Report": "report", "Dissertation": "thesis",
}


def datacite_by_doi(doi: str, timeout=20):
    """The DataCite `attributes` dict for `doi` (publisher as an object); None when DataCite has
    no such record (404). Raises MetadataUnavailable when DataCite could not answer."""
    d = _doi.normalise(doi) if doi else None
    if not d:
        return None
    body = _get_json(DATACITE_WORK.format(doi=_doi.encode_path(d)), "datacite", params=DATACITE_PARAMS,
                     headers={"Accept": "application/vnd.api+json"}, timeout=timeout)
    if body is None:
        return None
    attrs = (body.get("data") or {}).get("attributes") if isinstance(body, dict) else None
    if not isinstance(attrs, dict):
        raise MetadataUnavailable("datacite", detail="200 without data.attributes")
    return attrs


def _dc_date(dates, today=None) -> str:
    """'YYYY/MM/DD' | 'YYYY/MM' | 'YYYY' | '' from the Issued date, else a past Available date.
    Created and Updated are registration timestamps, not publication dates (N-A5: OSF records have
    had no Issued since Q2 2026; their Available is an embargo end, often years ahead)."""
    today = today or date.today()
    by_type = {}
    for d in dates or []:
        if isinstance(d, dict):
            by_type.setdefault(d.get("dateType"), str(d.get("date") or "").strip())
    for t in ("Issued", "Available"):
        m = re.match(r"(\d{4})(?:-(\d{2})(?:-(\d{2}))?)?", by_type.get(t) or "")
        if not m:
            continue
        parts = [p for p in m.groups() if p]
        y, mo, dd = int(parts[0]), int(parts[1]) if len(parts) > 1 else 1, int(parts[2]) if len(parts) > 2 else 1
        try:
            if date(y, mo, dd) > today:
                continue
        except ValueError:
            continue
        return "/".join(parts)
    return ""


def _dc_authors(creators):
    """DataCite creators -> [{'family','given'}] in build_ris shape."""
    out = []
    for c in creators or []:
        if not isinstance(c, dict):
            continue
        fam = _display(c.get("familyName"))
        giv = _display(c.get("givenName"))
        if not fam:
            nm = _display(c.get("name"))
            if not nm:
                continue
            if "," in nm and c.get("nameType") != "Organizational":   # "Family, Given"
                fam, giv = [p.strip() for p in nm.split(",", 1)]
            else:                                                      # organisation or mononym
                fam = nm
        out.append({"family": fam, "given": giv})
    return out


def _publisher_name(p) -> str:
    """DataCite `publisher`: a string, or (with publisher=true, and by default from September
    2027) an object whose `name` is the publisher."""
    if isinstance(p, dict):
        return _display(p.get("name"))
    return _display(p) if isinstance(p, str) else ""


def _dc_titles(titles):
    main = sub = ""
    for t in titles or []:
        if not isinstance(t, dict):
            continue
        kind = t.get("titleType")
        if not kind and not main:
            main = t.get("title") or ""
        elif kind == "Subtitle" and not sub:
            sub = t.get("title") or ""
    if not main and titles and isinstance(titles[0], dict):
        main = titles[0].get("title") or ""          # every entry typed: the first one
    return join_title(main, sub)


def datacite_meta(attrs: dict) -> dict:
    """Flatten DataCite attributes into the same dict shape as crossref_meta."""
    if not attrs:
        return {}
    authors = _dc_authors(attrs.get("creators"))
    abstract = ""
    for d in attrs.get("descriptions") or []:
        if isinstance(d, dict) and (d.get("descriptionType") or "").lower() == "abstract":
            abstract = _text.abstract_field(d.get("description"))
            break
    container = attrs.get("container") if isinstance(attrs.get("container"), dict) else {}
    rtg = (attrs.get("types") or {}).get("resourceTypeGeneral") or ""
    doi = (attrs.get("doi") or "").strip().lower()
    return {
        "doi": doi, "title": _dc_titles(attrs.get("titles")),
        "year": str(attrs.get("publicationYear") or "").strip(),
        "date": _dc_date(attrs.get("dates")),
        "lastname": authors[0]["family"] if authors else "",
        "authors": authors,
        "container": _display(container.get("title")) or _publisher_name(attrs.get("publisher")),
        "volume": "", "issue": "", "page": "", "issn": "",
        "abstract": abstract,
        "url": _doi_url(doi) if doi else (attrs.get("url") or ""),
        "type": _DC_TYPE.get(rtg, "posted-content"),
    }


# ---------- other registration agencies: content negotiation ----------
def _kv_get(ns, key):
    try:
        return _state().kv_get(ns, key)
    except Exception as e:                       # a locked or broken state file: no cache
        print(f"  [ris] state read failed ({type(e).__name__}); treating {ns} as unrecorded", file=sys.stderr)
        return None


def _kv_set(ns, key, value, ttl_s=None):
    try:
        _state().kv_set(ns, key, value, ttl_s=ttl_s)
        return True
    except Exception as e:
        print(f"  [ris] state write failed ({type(e).__name__}) for {ns}", file=sys.stderr)
        return False


def doi_ra(doi):
    """The registration agency holding `doi`, lower-cased ('crossref', 'datacite', 'medra',
    'jalc', 'kisti', 'istic', 'op', ...), from doi.org's /doiRA service and cached per prefix in
    the state kv; None when doi.org says the DOI does not exist. Raises MetadataUnavailable when
    doi.org could not answer."""
    d = _doi.normalise(doi) if doi else None
    if not d:
        return None
    prefix = d.split("/", 1)[0]
    cached = _kv_get("doi_ra", prefix)
    if cached:
        return cached
    body = _get_json(DOI_RA.format(doi=_doi.encode_path(d)), "doi.org")
    row = body[0] if isinstance(body, list) and body and isinstance(body[0], dict) else {}
    ra = (row.get("RA") or "").strip().lower()
    if not ra:
        return None                              # {"status": "DOI does not exist"} and the like
    _kv_set("doi_ra", prefix, ra, ttl_s=RA_CACHE_TTL_S)
    return ra


def _expect_csl(p):
    if p.status == 204:
        return (Kind.NO_MATCH, "HTTP 204: no metadata available")
    return net.expect_json(p)


def csl_by_doi(doi, timeout=None):
    """CSL-JSON for `doi` through DOI content negotiation (doi.org redirects to the agency's
    metadata service; litpipe.net follows only its https allow-list). None for 404, 204 or 406;
    MetadataUnavailable otherwise (including a redirect it would not follow)."""
    d = _doi.normalise(doi) if doi else None
    if not d:
        return None
    body = _get_json(DOI_CN.format(doi=_doi.encode_path(d)), "doi.org", headers={"Accept": CSL_JSON},
                     timeout=timeout, validate=_expect_csl)
    if body is None:
        return None
    if not isinstance(body, dict):
        raise MetadataUnavailable("doi.org", detail="CSL-JSON is not an object")
    return body


# CSL item types (both vocabularies seen at doi.org: CSL proper, and Crossref's own) -> our keys.
_CSL_TYPE = {
    "article-journal": "journal-article", "journal-article": "journal-article",
    "chapter": "book-chapter", "book-chapter": "book-chapter",
    "paper-conference": "proceedings-article", "proceedings-article": "proceedings-article",
    "book": "book", "monograph": "book", "report": "report", "thesis": "thesis",
    "dataset": "dataset", "article": "posted-content", "posted-content": "posted-content",
}


def csl_meta(csl: dict) -> dict:
    """Flatten a CSL-JSON item into the same dict shape as crossref_meta. A missing type (JaLC
    omits it; live 2026-09-30) reads as a journal article."""
    if not csl:
        return {}
    parts = _crossref_date(csl)        # same keys in CSL-JSON; the same print-first precedence
    authors = []
    for a in csl.get("author") or []:
        if isinstance(a, dict):
            fam = a.get("family") or a.get("literal") or a.get("name") or ""
            if _display(fam):
                authors.append(_author(fam, a.get("given")))
    d = (csl.get("DOI") or "").strip().lower()
    return {
        "doi": d,
        "title": join_title(_first(csl.get("title")), _first(csl.get("subtitle"))),
        "year": str(parts[0]) if parts else "",
        "date": _date_str(parts),
        "lastname": authors[0]["family"] if authors else "",
        "authors": authors,
        "container": _display(_first(csl.get("container-title"))) or _display(_first(csl.get("publisher"))),
        "volume": _display(csl.get("volume")),
        "issue": _display(csl.get("issue") or csl.get("number")),     # JaLC sends the issue as `number`
        "page": _display(csl.get("page")),
        "issn": _first(csl.get("ISSN")),
        "abstract": _text.abstract_field(csl.get("abstract")),
        "url": _doi_url(d) if d else "",
        "type": _CSL_TYPE.get(csl.get("type") or "article-journal", "journal-article"),
    }


def resolve_meta(doi: str):
    """(meta, source): Crossref first, DataCite second, then, for a DOI neither holds, the agency
    doi.org names, read through content negotiation.

    source is 'crossref', 'datacite', 'cn:<agency>' (mEDRA, JaLC, KISTI, OP), 'none' (no source
    holds the DOI, or it is not a DOI) or 'unsupported:<agency>' (ISTIC: no https route); meta is
    {} for the last two. Raises MetadataUnavailable when a source could not answer: a failed
    call is never read as "no metadata" (REG-I46). A prefix known to belong to a content-
    negotiation agency skips the two lookups that cannot hold it."""
    d = _doi.normalise(doi) if doi else None
    if not d:
        return {}, "none"
    ra = _kv_get("doi_ra", d.split("/", 1)[0])
    if ra not in CN_AGENCIES and ra not in UNSUPPORTED_AGENCIES:
        msg = crossref_by_doi(d)
        if msg:
            m = crossref_meta(msg)
            if m.get("title"):
                return m, "crossref"
        attrs = datacite_by_doi(d)
        if attrs:
            m = datacite_meta(attrs)
            if m.get("title"):
                return m, "datacite"
        if ra is None:
            ra = doi_ra(d)
    if ra in UNSUPPORTED_AGENCIES:
        return {}, f"unsupported:{ra}"
    if ra in CN_AGENCIES:
        csl = csl_by_doi(d)
        if csl:
            m = csl_meta(csl)
            if m.get("title"):
                return m, f"cn:{ra}"
    return {}, "none"


# ---------------------------------------------------------------------------- RIS
_RIS_TYPE = {
    "journal-article": "JOUR", "proceedings-article": "CPAPER", "book": "BOOK",
    "book-chapter": "CHAP", "report": "RPRT", "posted-content": "UNPD",
    "dataset": "DATA", "thesis": "THES", "monograph": "BOOK",
    # Crossref type names that were falling through to JOUR
    "dissertation": "THES", "edited-book": "EDBOOK", "reference-book": "BOOK",
    "book-section": "CHAP", "book-part": "CHAP",
}


def _ris_pages(page: str):
    """CrossRef 'page' is often '267-277'; split into SP/EP."""
    if not page: return "", ""
    m = re.match(r"^\s*([\w\-]+)\s*[-–]\s*([\w\-]+)\s*$", page)
    if m: return m.group(1), m.group(2)
    return page.strip(), ""


def _ris_val(s) -> str:
    """Collapse internal whitespace/newlines to single spaces (F6: a raw newline in a
    free-text RIS field produces an orphaned continuation line with no `TAG  - ` prefix,
    which lenient importers mis-fold into the previous value or drop)."""
    return re.sub(r"\s+", " ", str(s)).strip()


_DOI_URL = re.compile(r"^https?://(?:dx\.)?doi\.org/", re.IGNORECASE)


def build_ris(meta: dict) -> str:
    """Build a single-record RIS string from a flattened meta dict. A doi.org UR is rebuilt from
    the normalised, path-encoded DOI (N-A8); any other URL is written as given."""
    if not meta: return ""
    ty = _RIS_TYPE.get(meta.get("type"), "JOUR")
    sp, ep = _ris_pages(meta.get("page", ""))
    url = meta.get("url") or ""
    if url and meta.get("doi") and _DOI_URL.match(url):
        url = _doi_url(meta["doi"]) or url
    lines = [f"TY  - {ty}"]
    for a in meta.get("authors", []):
        fam = _ris_val(a.get("family") or "")
        giv = _ris_val(a.get("given") or "")
        if fam:
            lines.append(f"AU  - {fam}, {giv}" if giv else f"AU  - {fam}")
    if meta.get("title"):     lines.append(f"TI  - {_ris_val(meta['title'])}")
    if meta.get("container"): lines.append(f"JO  - {_ris_val(meta['container'])}")
    if meta.get("year"):      lines.append(f"PY  - {meta['year']}")
    if meta.get("date"):      lines.append(f"DA  - {meta['date']}")
    if meta.get("volume"):    lines.append(f"VL  - {meta['volume']}")
    if meta.get("issue"):     lines.append(f"IS  - {meta['issue']}")
    if sp: lines.append(f"SP  - {sp}")
    if ep: lines.append(f"EP  - {ep}")
    if meta.get("doi"):       lines.append(f"DO  - {meta['doi']}")
    if meta.get("issn"):      lines.append(f"SN  - {meta['issn']}")
    if url:                   lines.append(f"UR  - {url}")
    if meta.get("abstract"):  lines.append(f"AB  - {_ris_val(meta['abstract'])}")
    lines.append("ER  - ")
    return "\n".join(lines) + "\n"


# ---------- DEC-29: the pipeline owns only the .ris files it wrote and nobody has edited ----------
RIS_NS = "ris"


def manifest_key(path) -> str:
    """The manifest key for a .ris path: its real absolute path (on Windows, with the on-disk case
    of an existing file), so every spelling of one file shares one record."""
    return os.path.realpath(os.path.abspath(str(path)))


def _sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def ris_owner(path) -> str:
    """'absent', 'pipeline' (written by write_ris and unchanged since), 'edited' (written by
    write_ris, changed since) or 'unrecorded' (no record: curated, or written before DEC-29)."""
    if not os.path.exists(path):
        return "absent"
    rec = _kv_get(RIS_NS, manifest_key(path))
    if not rec:
        return "unrecorded"
    return "pipeline" if rec == _sha256_file(path) else "edited"


def write_ris(path: str, ris_text: str, overwrite: bool = True, force: bool = False) -> bool:
    """Write a RIS string and record its sha256 in the manifest (DEC-29). Returns True when the
    file now holds `ris_text`, False when it was skipped.

    An existing file is skipped when overwrite=False. With overwrite=True it is replaced only when
    it is pipeline-owned (ris_owner == 'pipeline'); an edited or unrecorded file is kept, with a
    stderr line, unless force=True. A file that already holds exactly `ris_text` is not rewritten;
    its hash is recorded."""
    if not ris_text:
        return False
    path = str(path)
    if os.path.exists(path):
        if not (overwrite or force):
            return False
        if not force:
            owner = ris_owner(path)
            if owner != "pipeline":
                if _sha256_file(path) == hashlib.sha256(ris_text.encode("utf-8")).hexdigest():
                    _kv_set(RIS_NS, manifest_key(path), _sha256_file(path))
                    return True
                why = "edited since the pipeline wrote it" if owner == "edited" else "no pipeline record"
                print(f"  [ris] kept {os.path.basename(path)}: {why} (DEC-29); force=True replaces it",
                      file=sys.stderr)
                return False
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    from lit_util import atomic_write_text  # RC4: crash-safe write (tmp + os.replace)
    atomic_write_text(path, ris_text)
    _kv_set(RIS_NS, manifest_key(path), _sha256_file(path))
    return True


# ---------- DOI extraction helpers (used by harvest + backfill) ----------

def extract_doi_from_text(text: str) -> str:
    # RC1 (2026-06-05 audit): delegate to lit_util, which re-joins line-wrapped DOIs and
    # rejects the truncation class ('10.1002/cphy', '10.1001/archinte') instead of the old
    # whitespace-stopping regex that produced those junk DOIs.
    from lit_util import extract_doi_from_text as _extract
    return _extract(text)


def emit_ris_for_pdf(doi: str, pdf_path: str, overwrite: bool = False) -> tuple:
    """One-shot: fetch metadata for `doi`, write `<pdf_stem>.ris`.
    Used as a hook by the live fetchers (unpaywall_fetch_v2, pmc_fetch, preprint_fetch).

    Returns (status, ris_path) where status is one of:
      'OK'               - wrote the RIS (or the file already held exactly this record)
      'EXISTS_SKIP'      - file already existed and overwrite=False
      'EXISTS_KEPT'      - overwrite=True, but the file is curated or edited (DEC-29)
      'NO_DOI'           - empty doi argument
      'RESOLVE_FAIL'     - no source holds usable metadata for the DOI
      'META_UNAVAILABLE' - a source could not answer (transport, 5xx, refusal): try again later
    """
    if not doi:
        return ("NO_DOI", "")
    stem, _ = os.path.splitext(pdf_path)
    ris_path = stem + ".ris"
    if os.path.exists(ris_path) and not overwrite:
        return ("EXISTS_SKIP", ris_path)
    try:
        meta, _src = resolve_meta(doi)
    except MetadataUnavailable as e:
        print(f"  [ris] metadata unavailable for {os.path.basename(ris_path)}: {e}", file=sys.stderr)
        return ("META_UNAVAILABLE", "")
    if not meta:
        return ("RESOLVE_FAIL", "")
    text = build_ris(meta)
    if not text:
        return ("RESOLVE_FAIL", "")
    if not write_ris(ris_path, text, overwrite=True):
        return ("EXISTS_KEPT", ris_path)
    return ("OK", ris_path)
