"""Shared safety primitives for the literature pipeline (added 2026-06-05 audit remediation).

Centralizes the fixes for the cross-cutting failure modes found in the 2026-06-05 code audit
(see notes/2026-06-05_pipeline_audit.md):
  - RC4  atomic_write_*  : crash-safe writes (tmp + os.replace) so an interrupt never truncates
                           a sidecar/.ris/CSV into invalid JSON.
  - RC1  DOI handling    : extract_doi_from_text() that does NOT truncate line-wrapped DOIs, plus
                           is_valid_doi()/is_suspicious_doi() gates to keep malformed DOIs
                           (e.g. '10.1002/cphy', '10.1001/archinte') out of the candidates/DB.
  - RC5  merge_sidecar() : preserve enriched fields (doi/title/year/authors/figures) when an
                           extractor re-writes a sidecar, instead of clobbering from an empty template.

Pure stdlib; safe to import from any pipeline script.
"""
import os, re, json, tempfile

# ---------------------------------------------------------------- RC4: atomic writes
def atomic_write_text(path, text, newline="\n"):
    """Write text crash-safely: write a sibling tmp then os.replace (atomic on NTFS)."""
    d = os.path.dirname(path) or "."
    fd, tmp = tempfile.mkstemp(dir=d, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline=newline) as f:
            f.write(text)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            try: os.remove(tmp)
            except OSError: pass

def atomic_write_json(path, obj, indent=2):
    atomic_write_text(path, json.dumps(obj, ensure_ascii=False, indent=indent))

# ---------------------------------------------------------------- RC1: DOI extraction + validity
# Start anchor for a DOI; the body is captured greedily then trimmed.
_DOI_START = re.compile(r"10\.\d{4,9}/", re.IGNORECASE)
_DOI_BODYCHAR = r"[A-Za-z0-9._;:()/\-]"
_DOI_TRAIL = re.compile(r"[.,;:)\]}>]+$")
_DOI_FULL = re.compile(r"^10\.\d{4,9}/\S+$", re.IGNORECASE)

def normalize_doi(doi):
    if not doi: return ""
    d = doi.strip().lower()
    if d.startswith("https://doi.org/"): d = d[len("https://doi.org/"):]
    elif d.startswith("http://dx.doi.org/"): d = d[len("http://dx.doi.org/"):]
    elif d.startswith("doi:"): d = d[4:]
    return _DOI_TRAIL.sub("", d).rstrip(".")

def is_valid_doi(doi):
    """Well-formedness only: matches 10.<reg>/<suffix> with a non-empty suffix."""
    return bool(doi) and bool(_DOI_FULL.match(doi.strip()))

def is_suspicious_doi(doi):
    """Flag DOIs that look like line-wrap TRUNCATIONS (the RC1 '10.1002/cphy' class).

    Real DOI suffixes essentially always contain a digit or a '.'; the observed truncations
    ('cphy', 'archinte') are bare lowercase journal-abbrev tokens with neither. Conservative:
    only flags suffixes that have NO digit AND NO dot (won't false-positive normal DOIs)."""
    if not is_valid_doi(doi): return True
    suffix = doi.split("/", 1)[1]
    return not any(c.isdigit() for c in suffix) and "." not in suffix

_BODYCHAR_RE = re.compile(_DOI_BODYCHAR)

def extract_doi_from_text(text, max_chars=None):
    """Extract the first well-formed, non-suspicious DOI from text, re-joining line-wrapped DOIs.

    The old reverse_citations/forward_citations regexes collapsed whitespace before matching, so a
    DOI split across a line break ('10.1002/cphy.\\nc140066') truncated at the wrap. Here we consume
    DOI body characters and, on hitting whitespace, re-join the wrapped continuation ONLY when the
    last body char is DOI-internal punctuation ('.', '-', '/') -- the pattern of a mid-DOI wrap --
    which avoids over-joining a DOI that legitimately ends at end-of-line followed by prose.
    Returns '' if no non-suspicious DOI is found (the is_suspicious_doi gate drops truncations)."""
    if not text: return ""
    if max_chars: text = text[:max_chars]
    n = len(text)
    for m in _DOI_START.finditer(text):
        i = m.end(); body = []
        while i < n:
            c = text[i]
            if _BODYCHAR_RE.match(c):
                body.append(c); i += 1
            elif c in " \t\r\n­":  # whitespace / soft-hyphen: maybe a wrapped DOI
                if body and body[-1] in ".-/":      # mid-DOI wrap -> rejoin (keep all chars;
                    j = i                            # DOIs aren't soft-hyphenated by typesetters)
                    while j < n and text[j] in " \t\r\n­": j += 1
                    if j < n and _BODYCHAR_RE.match(text[j]): i = j; continue
                break
            else:
                break
        cand = normalize_doi(m.group(0) + "".join(body))
        if is_valid_doi(cand) and not is_suspicious_doi(cand):
            return cand
    return ""

# ---------------------------------------------------------------- RC5: non-clobbering sidecar merge
_ENRICHED_FIELDS = ("doi", "title", "subtitle", "year", "journal", "authors",
                    "pmid", "pmcid", "abstract", "figures", "metadata_source",
                    "metadata_correction_note")

def merge_sidecar(old, new):
    """Return `new` augmented so it never DROPS a populated enriched field present in `old`.

    Used when an extractor re-writes a sidecar (e.g. extract --refresh / backfill --refresh):
    text/extractor/formula fields come from `new`, but doi/title/authors/figures etc. are
    preserved from `old` when `new` lacks them. Prevents the confirmed --refresh metadata wipe."""
    if not old: return new
    out = dict(new)
    for k in _ENRICHED_FIELDS:
        ov = old.get(k)
        nv = out.get(k)
        empty_new = nv in (None, "", [], {}, 0)
        if ov not in (None, "", [], {}) and empty_new:
            out[k] = ov
    return out
