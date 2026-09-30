"""One DOI normaliser for every DOI the pipeline reads (plan section 2.3, T5 expanded; W1-B).

The old extractor took the first "10.xxxx/" run in a text and re-joined it across any line break
that followed a '.', '-' or '/'. That glued running heads, line numbers and prose onto real DOIs
(`.author`, `.published`, `...00775.2024.12`, `...298293Stearns`), passed template placeholders
(`10.1145/nnnnnnn.nnnnnnn`), and let URL fragments (`#sa3`) through, which an encode-only fix
would then turn into a permanent 404. The DOI-mismatch guard compared those captures against the
queue DOI and quarantined the right paper (63 of 66 files, V0).

`candidates(raw)` turns a raw DOI, a URL or a text blob into DOI candidates, most specific first,
in the section 2.3 order:

1. lower-case (after the case-sensitive glue checks below have read the original case) and drop
   resolver prefixes (`https://doi.org/`, `doi:`, ...): capture always starts at the `10.` of the
   directory indicator;
2. split at an embedded URL (`.http://dx.doi.org/...`), peel trailing `.<word>` / `/<word>` tails
   (`.author`, `.published`, `/research`) and a capitalised surname glued onto a digit
   (`...298293Stearns`);
3. reject placeholders and truncations (`is_placeholder`): a run of five or more `n`, or a
   no-digit suffix that is a cut journal code (`j.amepre`, `cphy`), but not a nested path
   (`osf.io/kbyhm`), a CRAN package, or a letter code under a 5-digit registrant;
4. no glue across a line break onto digits (V2-N2) or onto a capitalised word (a sentence);
5. reject `#` fragments and invalid `%` escapes: capture stops there, so the fragment never
   reaches a candidate;
6. only then encode for a URL path (`encode_path`).

Pure: stdlib only, no network, no I/O, and no import of lit_util (lit_util imports this module).

Standards (read 2026-09-30):
- DOI Handbook (DOI Foundation, 2025-12-01 PDF, https://www.doi.org/doi-handbook/DOIHandbook_2025.pdf)
  4.3.1 "A DOI name consists of an ordered sequence of code points of the Graphic type ... arranged
  in a DOI prefix and a DOI suffix separated by U+002F SOLIDUS." 4.3.2 "The directory indicator ...
  is usually equal to '10' ... The registrant code consists of sequences of digits". 4.4.1 (the
  case rule) "a code point in the range U+0041..U+005A ... is considered identical to the
  corresponding code point in the range U+0061..U+007A ... case-insensitive only when testing for
  equivalence and only with respect to the Basic Latin Unicode block." 4.7 "The percent-encoding
  algorithm specified at RFC 3986 is applied whenever a DOI name is used in the path component of a
  URL ... applying the following algorithm to each of its prefix and suffix separately ... output
  the byte unmodified if the byte is one of the following: ALPHA, DIGIT, "-", ".", "_", "~", "!",
  "$", "&", "'", "(", ")", "*", "+", ";", "=", ":", or "@" ... otherwise, replace the byte with the
  US-ASCII byte triplet resulting from percent-encoding the byte." Example: "10.1000/456#789 is
  percent-encoded to 10.1000/456%23789".
- Crossref, "DOIs and matching regular expressions" (https://www.crossref.org/blog/dois-and-matching-regular-expressions/):
  `/^10.\\d{4,9}/[-._;()/:A-Z0-9]+$/i` matches 74.4M of 74.9M Crossref DOIs; early Wiley DOIs
  need `/^10.1002/[^\\s]+$/i`.
Deliberate deviations: capture uses the Crossref character class (plus `<` `>` inside SICI DOIs),
not the Handbook's "any graphic code point", because the input is free text; a `#` is read as a URL
fragment, not as part of a DOI (the Handbook allows it; the Crossref class does not; the only live
case is an S2 fragment, V3-N3); dotted registrant codes (`10.500.100`) are not captured, to stay
consistent with `lit_util.is_valid_doi`. `encode_path` keeps `/` inside the suffix literal by
default (today's wire behaviour at Crossref and Unpaywall); `strict=True` gives the Handbook form.
"""
import re
from urllib.parse import quote, unquote

__all__ = ["candidates", "iter_candidates", "normalise", "encode_path", "resolve_first",
           "is_placeholder", "ResolverUnavailable"]

# ---------------------------------------------------------------- shapes
# Directory indicator 10 + registrant code, not preceded by a digit (a glued year "2010.1234/").
_START = re.compile(r"(?<![0-9])10\.\d{4,9}/", re.IGNORECASE)
_BODY = re.compile(r"[A-Za-z0-9._;:()/\-]")        # the Crossref suffix class
_SICI_EXTRA = "<>"                                  # SICI DOIs carry literal angle brackets
_SICI_ISSN_DATE = re.compile(r"\d{4}-\d{3}[\dxX]\(\d{4,8}\)")  # the SICI item head: ISSN(date)
_REVISION_TAIL = re.compile(r"r{1,4}")               # FASEB revision suffixes: fj.201900106rrr is real
_WRAP_WS = " \t\r\n\f\v\u00ad\u00a0\u2009\u202f"    # whitespace and a soft hyphen at a line wrap
_VALID = re.compile(r"^10\.\d{4,9}/\S+$")
_BAD_DASH = re.compile(r"[\u2010-\u2015\u2212]")
# Text-mode typography that PDF extraction substitutes inside DOIs: hyphen / non-breaking hyphen
# for '-', and the Alphabetic Presentation Forms ligatures (NFKC maps them identically; probed).
_TEXT_FIX = str.maketrans({"\u2010": "-", "\u2011": "-", "\ufb00": "ff", "\ufb01": "fi",
                           "\ufb02": "fl", "\ufb03": "ffi", "\ufb04": "ffl", "\ufb05": "st",
                           "\ufb06": "st"})
_MD_ESCAPE = re.compile(r"\\([\[\]()<>_*.#\-])")    # Markdown-escaped punctuation (V3-N3 SICI)
_PCT_VALID = re.compile(r"%[0-9A-Fa-f]{2}")

# An embedded URL after a DOI: "...0550-8.http://dx.doi.org/10.1186/..." (I01).
_HTTP_TAIL = re.compile(r"[./]?https?:", re.IGNORECASE)
# A capitalised word glued onto a digit with no separator: SAGE running heads (NEW-C11). Read on
# the original case; the lower-case fallback needs a last segment of 5+ digits then 3+ letters.
_SURNAME_GLUE = re.compile(r"(?<=\d)[A-Z][a-z]{2,}$")
_SURNAME_GLUE_LC = re.compile(r"(?:^|[./])\d{5,}([a-z]{3,})$")
# A trailing alphabetic tail of 3+ letters after '.' or '/' (".author", ".nih-pa", "/research").
_ALPHA_TAIL = re.compile(r"[./]([a-z]+(?:-[a-z]+)*)$")
# Real DOIs that end in an alphabetic segment (Wiley book front/back matter): keep them first.
_REAL_ALPHA_TAILS = frozenset({"fmatter", "bmatter", "index", "toc"})
# A short numeric tail (a line number or year glued on; V2-N2): offered only as a fallback.
_NUM_TAIL = re.compile(r"\.\d{1,4}$")
_PLACEHOLDER_RUN = re.compile(r"n{5,}", re.IGNORECASE)
_CRAN_PACKAGE = re.compile(r"^cran\.package\.[a-z][a-z0-9.]*$", re.IGNORECASE)
_REGISTRANT = re.compile(r"10\.(\d+)")
# 4-digit registrants that issue digit-free letter-coded suffixes (doi.org, 2026-09-30):
# Ubiquity Press, 10.5334/jors.bi and 10.5334/bbe.c are registered.
LETTER_CODE_REGISTRANTS = frozenset({"5334"})
_TRAIL_PUNCT = ".,;:-"


def is_placeholder(doi: str) -> bool:
    """True for a template placeholder or a truncated DOI; False for a DOI that may be real.

    - A run of five or more 'n' is a placeholder (`10.1145/nnnnnnn.nnnnnnn`, the ACM acmart
      default that put 660 edges in one project's citation graph; V4-N3).
    - A suffix with a digit is otherwise never flagged.
    - A suffix with no digit is real under a registrant of 5+ digits (`10.31234/osf.io/kbyhm`,
      `10.32614/cran.package.boot`, `10.29007/scnh`, `10.37204/bioconversion.organic.waste`),
      under a registrant in LETTER_CODE_REGISTRANTS (`10.5334/jors.bi`), or when it holds a
      nested path or is a CRAN package.
    - Otherwise (a 4-digit registrant) it is a journal code cut at a segment boundary
      (`10.1016/j.amepre`, `10.1371/journal.pbio`, `10.1249/mss.m`) or a bare truncation
      (`10.1002/cphy`, `10.1001/archinte`, `10.1093/nar`): flagged.
    Measured 2026-09-30 against doi.org's handle API over all 116 digit-free DOIs in
    portfolio.duckdb: 53 registered, 63 not. This rule keeps 52 (all registered) and flags 64;
    its one false flag is `10.5779/hypothesis.vxxix.xxxx`, which is registered but reads as a
    template (evidence: notes/2026-09-29_update_evidence/W1-B/nodigit_doi_census_resolved.csv).
    Until 2026-09-30 any no-digit suffix was flagged, which dropped 14 OSF preprints (2 held),
    3 CRAN packages, 18 nucleodoconhecimento articles and 17 other registered DOIs."""
    if not doi or "/" not in doi:
        return True
    prefix, suffix = doi.split("/", 1)
    if _PLACEHOLDER_RUN.search(suffix):
        return True
    if any(c.isdigit() for c in suffix):
        return False
    m = _REGISTRANT.match(prefix)
    registrant = m.group(1) if m else ""
    if len(registrant) >= 5 or registrant in LETTER_CODE_REGISTRANTS:
        return False
    if "/" in suffix or _CRAN_PACKAGE.match(suffix):
        return False
    return True


def _well_formed(doi: str) -> bool:
    return bool(_VALID.match(doi)) and not _BAD_DASH.search(doi)


def _trim(s: str) -> str:
    """Drop trailing sentence punctuation, and a closing ')' or '>' only when it is unbalanced
    (a DOI may end in a balanced ')' such as `...(20)30183-5`; a sentence '(doi:...)' may not)."""
    while s:
        c = s[-1]
        if c in _TRAIL_PUNCT:
            s = s[:-1]
        elif c == ")" and s.count(")") > s.count("("):
            s = s[:-1]
        elif c == ">" and s.count(">") > s.count("<"):
            s = s[:-1]
        elif c in "]}":
            s = s[:-1]
        else:
            break
    return s


def _next_token(text: str, j: int) -> str:
    k = j
    while k < len(text) and not text[k].isspace():
        k += 1
    return text[j:k]


def _capture(text: str, start: int, end: int):
    """Capture one DOI body from the directory indicator at `start` (its '/' ends at `end`).

    Returns (primary, joined_alternative_or_None). A line break is re-joined only where the DOI is
    visibly incomplete: after '-' or '/', or after '.' when the continuation is a lower-case run
    (`cphy.\\nc140066`) or the suffix so far has no digit (`japplphysiol.\\n00775.2024`). It is NOT
    re-joined after '.' onto an all-digit token when the suffix already has a digit (a manuscript
    line number or a year, V2-N2; the joined form is then offered as a lower-priority alternative
    for an injected resolver to confirm), nor after '.' or '-' onto a capitalised word with no
    digit (`dc08-1876.\\nIntroduction`, `...103.\\nI. Introduction`, `978-3-319-\\nEnergetics`).
    A capture left ending in '-' is a truncation; the caller rejects it."""
    n = len(text)
    body = []
    alt = None
    i = end
    while i < n:
        c = text[i]
        head = "".join(body[:17])
        # SICI: Wiley's "(sici)" marker, or the bare ISSN(date) form NSCA, AMS and others registered
        # (10.1519/1533-4287(1990)004<0047:rbrasp>2.3.co;2); 25 of 30 index SICI DOIs lack the marker
        sici = "(sici)" in head[:8].lower() or bool(_SICI_ISSN_DATE.match(head))
        if _BODY.match(c) or (sici and c in _SICI_EXTRA):
            body.append(c)
            i += 1
            continue
        last = body[-1] if body else "/"                # an empty body sits right after the prefix '/'
        if c in _WRAP_WS and last in ".-/":
            j = i
            while j < n and text[j] in _WRAP_WS:
                j += 1
            if j >= n or not _BODY.match(text[j]):
                break
            tok = _next_token(text, j)
            suffix_has_digit = any(ch.isdigit() for ch in body)
            if last in ".-" and re.fullmatch(r"[A-Z][A-Za-z]*[.,:;)]*", tok):
                break                                   # prose starts: "Introduction", "I.", "Energetics"
            if last == ".":
                if re.fullmatch(r"\d{1,4}[.,;:)]*", tok) and suffix_has_digit:
                    if alt is None:
                        alt = "".join(body) + re.sub(r"[.,;:)]+$", "", tok)
                    break                               # a line number or year (V2-N2)
            i = j
            continue
        break
    return "".join(body), alt


def _variants(raw_doi: str, numeric_fallback: bool = True):
    """Candidates for one captured DOI (original case in, lower case out), best first.
    `numeric_fallback` adds the form without a short numeric tail (a stored `...112.234`); text
    capture already refuses that glue, so iter_candidates enables it for single-token input only."""
    first, later = [], []
    s = _trim(raw_doi)
    parts = _HTTP_TAIL.split(s, maxsplit=1)
    if len(parts) > 1:
        s = _trim(parts[0])
    m = _SURNAME_GLUE.search(s)
    if m:                                               # "...298293Stearns": peel, keep original last
        later.append(s.lower())
        s = s[:m.start()]
    s = s.lower()
    if not m and "/" in s:
        m2 = _SURNAME_GLUE_LC.search(s.split("/", 1)[1])
        if m2 and not _REVISION_TAIL.fullmatch(m2.group(1)):   # the same glue, already lower-cased
            later.append(s)
            s = s[: len(s) - len(m2.group(1))]
    # Peel alphabetic tails one at a time; the fully peeled form is the most specific.
    peeled = [s]
    while True:
        t = _ALPHA_TAIL.search(s)
        if not t or len(t.group(1).replace("-", "")) < 3:
            break
        rest = _trim(s[: t.start()])
        if "/" not in rest or not any(ch.isdigit() for ch in rest.split("/", 1)[1]):
            break                                       # peeling would leave no real suffix
        if t.group(1) in _REAL_ALPHA_TAILS:
            later.append(rest)                          # a real Wiley tail: keep it first
            break
        s = rest
        peeled.append(s)
    first.append(peeled[-1])
    later = list(reversed(peeled[:-1])) + later         # less peeled, then the raw glued form
    # A short numeric tail is usually real; the stripped form is only a fallback (V2-N2).
    nt = _NUM_TAIL.search(peeled[-1])
    if numeric_fallback and nt and "/" in peeled[-1][: nt.start()]:
        later.append(peeled[-1][: nt.start()])
    return first + later


def iter_candidates(raw: str, rejected=None):
    """Yield (position, candidate) in text order, best candidate first for each occurrence."""
    if not raw:
        return
    text = str(raw).translate(_TEXT_FIX)
    text = _MD_ESCAPE.sub(r"\1", text)
    single = not any(ch.isspace() for ch in text.strip())
    if single and _PCT_VALID.search(text):
        text = unquote(text)                            # a URL-form DOI: "10.1000%2F123"
    for m in _START.finditer(text):
        body, alt = _capture(text, m.start(), m.end())
        head = text[m.start():m.end()]
        if body.endswith("-"):                          # stopped mid-DOI ("978-3-319-\nEnergetics")
            if rejected is not None:
                rejected.append((m.start(), (head + body).lower(), "truncated"))
            continue
        forms = _variants(head + body, numeric_fallback=single)
        if alt is not None:
            forms += [f for f in _variants(head + alt, numeric_fallback=single) if f not in forms]
        for cand in forms:
            if not _well_formed(cand):
                if rejected is not None:
                    rejected.append((m.start(), cand, "malformed"))
                continue
            if is_placeholder(cand):
                if rejected is not None:
                    rejected.append((m.start(), cand, "placeholder"))
                continue
            yield m.start(), cand


def candidates(raw, rejected=None) -> list:
    """DOI candidates in `raw` (a DOI, a DOI URL, or free text), lower-cased, de-duplicated.

    Occurrences come in text order; within one occurrence the most specific form comes first
    (peeled), then the fallbacks an injected resolver may confirm (less peeled, the raw glued
    form, a stripped numeric tail, a joined line wrap). Placeholders and malformed forms never
    appear; pass a list as `rejected` to receive `(position, form, reason)` for each one dropped."""
    out, seen = [], set()
    for _, cand in iter_candidates(raw, rejected):
        if cand not in seen:
            seen.add(cand)
            out.append(cand)
    return out


def normalise(raw):
    """The single most specific DOI in `raw`, or None when there is none."""
    for _, cand in iter_candidates(raw):
        return cand
    return None


_PATH_SAFE = "!$&'()*+;=:@"                             # DOI Handbook 4.7, beyond ALPHA DIGIT - . _ ~


def encode_path(doi, *, strict=False) -> str:
    """Percent-encode a DOI for a URL path, AFTER normalising it (section 2.3 step 6).

    Normalising first is the point: encoding `10.1056/nejmc1113675#sa3` as-is gives `%23sa3`,
    which no server can resolve (V3-N3); normalising cuts the fragment. Prefix and suffix are
    encoded separately with the Handbook 4.7 unreserved set. `strict=False` (the default) also
    keeps '/' inside the suffix literal, which is how every caller sends it today; `strict=True`
    encodes it as %2F exactly as 4.7 specifies. Raises ValueError when `doi` holds no DOI."""
    d = normalise(doi)
    if d is None:
        raise ValueError(f"not a DOI: {doi!r}")
    prefix, suffix = d.split("/", 1)
    safe = _PATH_SAFE if strict else _PATH_SAFE + "/"
    return quote(prefix, safe=_PATH_SAFE) + "/" + quote(suffix, safe=safe)


class ResolverUnavailable(RuntimeError):
    """The injected resolver failed (transport, refusal, outage): nothing can be concluded about
    this candidate, so resolve_first stops rather than falling through to a less likely one."""

    def __init__(self, candidate, outcome):
        super().__init__(f"resolver unavailable for {candidate!r}: {getattr(outcome, 'kind', outcome)}")
        self.candidate = candidate
        self.outcome = outcome


# The question put to the resolver is "is this DOI registered?". These kinds say yes (the record
# exists, perhaps at another agency, under an alias, or without available content); NO_MATCH says
# no; every other kind (REFUSED, OUTAGE, TRANSPORT, DEFERRED, CONFIG, ERROR, SKIPPED) says nothing.
_CONFIRMS = frozenset({"OK", "ALIASED", "NOT_AT_RA", "NOT_AVAILABLE", "EMBARGOED"})
_REFUTES = frozenset({"NO_MATCH"})


def resolve_first(cands, resolver):
    """The first candidate the injected `resolver` confirms, or None when it refutes all of them.

    `resolver(doi)` may return a truthy/falsy value (for example `ris_emit.resolve_meta`'s dict or
    None) or a `litpipe.outcomes.Outcome`: OK, ALIASED, NOT_AT_RA, NOT_AVAILABLE and EMBARGOED
    confirm the DOI exists, NO_MATCH refutes it, and any other kind raises ResolverUnavailable,
    because a failed call is not a "no" (dispatch 0.8 item 5) and falling through to the next,
    less likely candidate could pick a different paper. A plain resolver cannot tell a failure
    from a "no"; prefer one that returns an Outcome. Exceptions from the resolver propagate."""
    for cand in cands:
        r = resolver(cand)
        kind = getattr(r, "kind", None)
        if kind is not None and hasattr(r, "ok"):
            k = str(getattr(kind, "value", kind))
            if k in _CONFIRMS:
                return cand
            if k in _REFUTES:
                continue
            raise ResolverUnavailable(cand, r)
        if r:
            return cand
    return None
