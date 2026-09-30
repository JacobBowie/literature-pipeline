"""Is the fetched file the paper that was asked for, and what kind of document is it? (W1-B)

Replaces the DOI-mismatch quarantine (I02). The old guard compared the FIRST DOI in 5,000
characters against the queue DOI and moved the file to `_mismatch/` on any difference. V0 found
63 of 66 quarantined files were the right paper: the first DOI was an over-capture (`.author`), a
journal template header (JENB prints `jenb.2018.0010` above every paper's own DOI), or a
placeholder. The check here asks a different question, in this order:

1. OK: the normalised queue DOI appears anywhere in the first pages (containment, not
   first-DOI equality). Case-insensitive in Basic Latin only, as the DOI Handbook 4.4.1 defines
   DOI equivalence; tolerant of a line wrap after '.', '/' or '-', and of letter-spaced text
   layers (`1 0 . 1 0 1 6 / j . j a c c`, seen in a JACC PDF).
2. TITLE_MATCH: the queue or `.ris` title matches a stretch of the first pages with similarity
   at least TITLE_THRESHOLD (0.85, difflib ratio on the alphanumeric-only normalised strings).
3. FLAG otherwise. Never move or delete: the caller records the verdict and its evidence on the
   artifact (sidecar), not only in a run report (22 of 66 quarantines had no surviving row).

`doc_kind` classifies the first page as VOR, AAM, SUPPLEMENT or UNKNOWN (consumer intake finding F1: a 180-page
BMJ supplement passed the DOI check because it prints the article's citation). `suspect_file`
flags files that are not a whole article (V2-N5: 2-page previews; NEW-A10: watermark-only text).

Pure: no network, no file I/O. Callers extract the text (pymupdf `page.get_text()`; its default
order is the content stream, and headers or DOI footers can land at the END of the page text, so
nothing here assumes the DOI comes first).
"""
import re
from collections import Counter
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from enum import StrEnum

from litpipe import doi as _doi
from litpipe.text import clean_field, normalise_title

__all__ = ["Decision", "Verdict", "check", "title_similarity", "TITLE_THRESHOLD", "DocKind",
           "doc_kind", "SuspectCheck", "suspect_file", "text_stats"]

TITLE_THRESHOLD = 0.85


class Decision(StrEnum):
    OK = "OK"                    # the queue DOI is printed in the first pages
    TITLE_MATCH = "TITLE_MATCH"  # no DOI match, but the title matches
    FLAG = "FLAG"                # neither: keep the file in place, record, review


@dataclass(frozen=True)
class Verdict:
    decision: Decision
    score: float | None = None          # best title similarity, when a title was compared
    evidence: dict = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.decision in (Decision.OK, Decision.TITLE_MATCH)

    def as_dict(self) -> dict:
        """The sidecar form: {"identity": ..., "identity_score": ..., "identity_evidence": {...}}."""
        return {"identity": str(self.decision),
                "identity_score": None if self.score is None else round(self.score, 3),
                "identity_evidence": dict(self.evidence)}


# ---------------------------------------------------------------- DOI containment
_WRAP = r"[\s­]*"


def _doi_pattern(d):
    parts = []
    for ch in d:
        parts.append(re.escape(ch))
        if ch in "./-":
            parts.append(_WRAP)                         # a line wrap after DOI punctuation
    return re.compile(r"(?<![0-9])" + "".join(parts), re.IGNORECASE | re.ASCII)


def _end_ok(text, end, d, despaced):
    """The match must end where the DOI ends: not followed by a digit (a longer DOI) and, in
    spaced text, not by a lower-case letter either. A capital right after a final digit is the
    SAGE surname glue (`...298293Stearns`) and is allowed."""
    if end >= len(text):
        return True
    c = text[end]
    if not c.isascii() or not c.isalnum():
        return True
    if c.isdigit():
        return False
    if despaced:
        return True                                     # words are glued there by construction
    return c.isupper() and d[-1].isdigit()


def _find_doi(text, d):
    t = text.translate(_doi._TEXT_FIX)
    pat = _doi_pattern(d)
    for m in pat.finditer(t):
        if _end_ok(t, m.end(), d, False):
            how = "exact" if m.group(0).lower() == d else "wrapped"
            return {"doi_match": how, "doi_pos": m.start()}
    # Letter-spaced text layers: search with whitespace removed, but only accept a match whose
    # original span actually contained whitespace (a contiguous one was already judged above).
    keep = [k for k, ch in enumerate(t) if not (ch.isspace() or ch == "­")]
    despaced = "".join(t[k] for k in keep)
    pat2 = re.compile(re.escape(d), re.IGNORECASE | re.ASCII)
    for m in pat2.finditer(despaced):
        span = t[keep[m.start()]: keep[m.end() - 1] + 1]
        if not any(ch.isspace() for ch in span):
            continue
        if (m.start() == 0 or not despaced[m.start() - 1].isdigit()) and _end_ok(despaced, m.end(), d, True):
            return {"doi_match": "despaced", "doi_pos": keep[m.start()]}
    return None


# ---------------------------------------------------------------- title similarity
def _alnum(s):
    return "".join(ch for ch in s if ch.isalnum())


def _anchors(tnorm, k=8):
    """The title's k longest distinct words (5+ characters), longest first, ties alphabetical.
    Deterministic on purpose: ordering a set by length alone depends on PYTHONHASHSEED, and the
    score then changed between runs (0.398 vs 0.466 for the same file, 2026-09-30)."""
    return sorted({_alnum(w) for w in tnorm.split() if len(_alnum(w)) >= 5},
                  key=lambda w: (-len(w), w))[:k]


def title_similarity(title, text):
    """Best difflib ratio between the title and any same-length stretch of `text`, both reduced
    to lower-case alphanumerics (so hyphenation, spacing, punctuation and letter-spacing do not
    count). Windows are anchored on occurrences of the title's longer words, so the cost stays
    small on a full first page. 0.0 when the title is empty or no anchor word occurs."""
    tnorm = normalise_title(title)
    tt = _alnum(tnorm)
    if len(tt) < 8:
        return 0.0
    body = _alnum(normalise_title(text))                # hyphenation and line breaks vanish here
    if not body:
        return 0.0
    words = _anchors(tnorm)
    if not words:
        words = [tt[:8]]
    best = 0.0
    L = len(tt)
    seen = set()
    for w in words:
        off = tt.find(w)
        pos = body.find(w)
        while pos != -1:
            base = pos - off
            for delta in range(-12, 13, 2):
                start = max(0, base + delta)
                for length in (L, int(L * 1.1)):
                    key = (start, length)
                    if key in seen:
                        continue
                    seen.add(key)
                    win = body[start:start + length]
                    sm = SequenceMatcher(None, tt, win, autojunk=False)
                    if sm.real_quick_ratio() <= best or sm.quick_ratio() <= best:
                        continue
                    best = max(best, sm.ratio())
            pos = body.find(w, pos + 1)
        if best >= 0.999:
            break
    return best


def clean_field_keep_lines(text):
    """clean_field for page text, keeping line breaks (the hyphenation repair needs them)."""
    return "\n".join(clean_field(line) for line in str(text or "").splitlines())


# ---------------------------------------------------------------- the check
def check(first_pages_text, queue_doi, queue_title=None, ris_title=None, *,
          threshold=TITLE_THRESHOLD) -> Verdict:
    """OK when the normalised queue DOI is in the first pages; TITLE_MATCH when the queue or
    `.ris` title reaches `threshold`; else FLAG. Evidence says which rule decided and why."""
    text = first_pages_text or ""
    ev = {"chars": len(text)}
    d = _doi.normalise(queue_doi) if queue_doi else None
    if queue_doi and d is None:
        ev["queue_doi_invalid"] = str(queue_doi)
    if d:
        ev["queue_doi"] = d
        hit = _find_doi(text, d)
        if hit:
            ev.update(hit)
            return Verdict(Decision.OK, None, ev)
        found = _doi.candidates(text)
        ev["pdf_dois"] = found[:5]
    scores = {}
    for src, t in (("queue", queue_title), ("ris", ris_title)):
        if t and str(t).strip():
            scores[src] = round(title_similarity(t, text), 3)
    ev["title_scores"] = scores
    best = max(scores.values()) if scores else None
    if best is not None and best >= threshold:
        ev["title_source"] = max(scores, key=scores.get)
        return Verdict(Decision.TITLE_MATCH, best, ev)
    ev["reason"] = ("no queue DOI in text; " if d else "no usable queue DOI; ") + (
        f"title below {threshold}" if scores else "no title to compare")
    return Verdict(Decision.FLAG, best, ev)


# ---------------------------------------------------------------- document kind
class DocKind(StrEnum):
    VOR = "VOR"                  # the publisher's version of record
    AAM = "AAM"                  # author accepted manuscript or repository / preprint copy
    SUPPLEMENT = "SUPPLEMENT"    # an online supplement or appendix, not the article
    UNKNOWN = "UNKNOWN"


# Phrases only a supplement prints, anywhere on page 1.
_SUPPLEMENT = re.compile(
    r"supplement(?:ary|al)?\s+(?:online\s+)?(?:appendix|material|materials|data|information)\s+(?:for|to)\b"
    r"(?!\s+(?:this|the|our)\s+(?:article|paper|manuscript|study|online))"   # "... data to this article"
    r"|\bsupplement\s+to\s*:"
    r"|this\s+appendix\s+(?:has\s+been|was)\s+provided\s+by\s+the\s+authors"
    r"|online[- ]only\s+supplement|\bonline\s+(?:data\s+)?supplement\b|\bdata\s+supplement\b"
    r"|(?:^|\n)\s*supplemental\s+digital\s+content\s+\d",
    re.IGNORECASE)
# A bare heading that articles also use in their body (an HHS manuscript has a
# "SUPPLEMENTARY MATERIALS" section on page 1): counts only as the page's opening line.
_SUPPLEMENT_HEAD = re.compile(
    r"^\s*(?:supplement(?:ary|al)\s+(?:appendix|material|materials|information|tables?|figures?)"
    r"|e?appendix|web\s+appendix|supporting\s+information)\s*\d*\s*(?:\n|$)",
    re.IGNORECASE)
_AAM = re.compile(
    r"author\s+manuscript|accepted\s+manuscript|author'?s?\s+accepted\s+(?:manuscript|version)"
    r"|author'?s\s+(?:final\s+)?version|this\s+is\s+(?:the|an)\s+(?:author|accepted)"
    r"|hhs\s+public\s+access|nih[- ]pa\s+author|europe\s+pmc\s+funders"
    r"|\bpost-?print\b|\bpre-?print\b|not\s+(?:yet\s+)?peer[- ]reviewed|\barxiv:\s*\d"
    r"|\b(?:bio|med|psy|sport|socarxiv)rxiv\b|\beprints?\b|\bwrap\.warwick|warwick\s+research\s+archive"
    r"|escholarship|this\s+manuscript\s+version\s+is\s+made\s+available",
    re.IGNORECASE)
_VOR = re.compile(
    r"©|copyright|published\s+by|published\s+online|journal\s+homepage|all\s+rights\s+reserved"
    r"|open\s+access\s+this\s+article|creative\s+commons|cite\s+this\s+article|\bcitation:"
    r"|downloaded\s+from|received:?\s.{0,40}accepted",
    re.IGNORECASE | re.DOTALL)


def doc_kind(first_page_text, n_pages) -> DocKind:
    """Classify page 1: SUPPLEMENT first (a supplement prints the article's citation and DOI),
    then AAM (repository and manuscript banners; an AAM often carries a copyright line too), then
    VOR (publisher furniture). UNKNOWN when nothing matches or there is no text. `n_pages` only
    rules out an empty document; comparing it with the expected length is suspect_file's job
    (`parsed_vs_expected`)."""
    t = clean_field_keep_lines(first_page_text)
    if not t.strip() or not n_pages:
        return DocKind.UNKNOWN
    if _SUPPLEMENT.search(t) or _SUPPLEMENT_HEAD.match(t.lstrip()):
        return DocKind.SUPPLEMENT
    if _AAM.search(t):
        return DocKind.AAM
    if _VOR.search(t):
        return DocKind.VOR
    return DocKind.UNKNOWN


# ---------------------------------------------------------------- suspect files
@dataclass(frozen=True)
class SuspectCheck:
    suspect: bool
    reasons: tuple = ()

    def __bool__(self):
        return self.suspect


def text_stats(text, n_pages):
    """Counts suspect_file reads: non-space characters per page and how much of the text is one
    repeated line (a 'Downloaded from ... on <date>' watermark on every page of a scan)."""
    lines = [ln.strip() for ln in str(text or "").splitlines() if ln.strip()]
    chars = sum(1 for ch in str(text or "") if not ch.isspace())
    top = Counter(lines).most_common(1)[0][1] if lines else 0
    pages = max(int(n_pages or 0), 1)
    return {"chars": chars, "pages": int(n_pages or 0), "chars_per_page": chars / pages,
            "lines": len(lines), "distinct_lines": len(set(lines)), "top_line_count": top,
            "top_line_share": (top / len(lines)) if lines else 0.0}


WATERMARK_CHARS_PER_PAGE = 200


def suspect_file(n_pages, has_refs_header, parsed_vs_expected, text_stats) -> SuspectCheck:
    """A file that is probably not the whole article: two pages or fewer, no references header,
    fewer than half the expected references parsed, or a text layer that is only a watermark
    (under 200 non-space characters a page, or one line repeated at least 3 times and making up
    half the lines). Unknown inputs (None) are skipped, not counted against the file."""
    reasons = []
    if n_pages is not None and n_pages <= 2:
        reasons.append("pages<=2")
    if has_refs_header is False:
        reasons.append("no_references_header")
    if parsed_vs_expected is not None and parsed_vs_expected < 0.5:
        reasons.append("parsed_vs_expected<0.5")
    st = text_stats or {}
    if st:
        thin = st.get("chars_per_page", 1e9) < WATERMARK_CHARS_PER_PAGE
        repeated = st.get("top_line_count", 0) >= 3 and st.get("top_line_share", 0.0) >= 0.5
        if thin or repeated:
            reasons.append("watermark_only_text")
    return SuspectCheck(bool(reasons), tuple(reasons))
