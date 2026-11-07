"""Known wrong-content documents: files that pass every PDF check but are not a paper.

A repository or publisher sometimes serves the same non-article document for every DOI it is
asked about: a privacy notice, an open-access policy, a citation guide, a permissions page. The
file is a valid PDF, so the %PDF and size checks pass it. Each entry is listed two ways:

  MD5   the exact bytes seen (catches the file whatever its text layer says);
  TEXT  a phrase from its first pages (catches a re-spun copy of the same document).

The fetchers reject a match before writing (unpaywall_fetch_v2.boilerplate_of and
is_known_boilerplate, which pmc_fetch, preprint_fetch, paywall_pull and import_downloads use), and
audit_portfolio reports a held file that matches (its PDF bytes or its sidecar's text).

A TEXT phrase must name the document itself, never a cover page a repository puts in front of
a real paper: such a cover page would reject the paper too. Those documents are listed by MD5
only (the Chapman "Rights and Terms of Use" page below). Every phrase was checked against every
sidecar on FRED on 2026-11-07 (8,639 with text): each matched only the files listed here.

Matching is on normalise(text)[:SNIPPET_CHARS]: accents and dashes folded, whitespace collapsed,
lowercased; the phrases are normalised the same way when this module loads.
"""
import hashlib
import re
import unicodedata

SNIPPET_CHARS = 3000

MD5 = {
    "518fe51393a7ba381f861b58f296832e": "lww_author_permission_guidelines_v1",
    # 2026-05-22 audit additions (Agent C cross-project sweep):
    "9aeef9e74d08bbd6b39996fb963fd8cb": "plos_manuscript_body_formatting_template",
    "b9d50b11d4901b8fb7d5eaab473193dc": "jmlr_scikit_learn_misfetch_for_jmir_dois",
    # 2026-11-07, week-1 health F1 and the 2026-11-03 identity review (bytes read on FRED):
    "bbb20e478ed231ccc6575eab698103ad": "pure_research_portal_privacy_notice",  # Teesside, 9 files
    "0bcf1792a3ca9ce31442e1e6aa53ca0a": "anid_open_access_policy",              # 6 files
    "7b64caf2ff1737c5b9f8ff0a7abc88d2": "uchile_citation_guide",                # 3 files
    "3843d4757c21ac3660e22bd8ea348968": "aviso_legal_privacy_policy",           # 2 files
    "0de56e6fe8e4916e06cf436c17c60cdc": "chapman_rights_terms_of_use",          # 3 files
    "94715e4d56fd44e138140dac7575fea1": "bmj_self_archiving_policy",
    "a58ff5c16a8e383d0f1792c417322242": "nfhs_participation_survey",
    "4d0cd1dae38074391f11dbddbb52e9ff": "cms_therapy_code_list",
    "f82beb0b0087a5c295463019502802d7": "uevora_repository_regulation",         # scanned; no usable text
}

# Byte sizes of the MD5 entries whose files were seen in a library (the three 2026-05-22 entries were
# rejected at fetch and never measured). audit_portfolio hashes a PDF of one of these sizes, so a
# held copy is found without hashing every PDF; a phrase still finds one of an unlisted size.
SIZES = frozenset({240007, 299342, 135403, 305666, 105882, 293298, 995139, 192990, 3725999})

_TEXT = (
    ("lippincott journal portfolio",      "lww_author_permission_guidelines"),
    ("author permission guidelines",      "lww_author_permission_guidelines"),
    ("manuscript body formatting guidelines", "plos_template"),
    ("cite figures as \"fig 1\"",             "plos_template"),
    ("scikit-learn: machine learning in python", "jmlr_misfetch"),
    ("portal de periódicos da capes",    "capes_redirect_page"),
    # 2026-11-07:
    ("legal & governance services research portal", "pure_research_portal_privacy_notice"),
    ("política de acceso abierto a la información científica", "anid_open_access_policy"),
    ("redacción de citas bibliográficas",  "uchile_citation_guide"),
    ("aviso legal y política de privacidad", "aviso_legal_privacy_policy"),
    ("bmj authors self-archiving policy",  "bmj_self_archiving_policy"),
    ("high school athletics participation survey", "nfhs_participation_survey"),
    ("mln matters number",                 "cms_therapy_code_list"),
)


def normalise(text) -> str:
    """Accents and en/em dashes folded, whitespace collapsed, lowercased."""
    s = unicodedata.normalize("NFKD", str(text or ""))
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.replace("–", "-").replace("—", "-")
    return re.sub(r"\s+", " ", s).strip().lower()


TEXT = tuple((normalise(needle), tag) for needle, tag in _TEXT)


def of_bytes(content) -> str | None:
    """The tag for these exact bytes, else None."""
    return MD5.get(hashlib.md5(content).hexdigest()) if content else None


def of_text(text) -> str | None:
    """The tag whose phrase is in the first SNIPPET_CHARS of the normalised text, else None."""
    # Only the opening matters: normalise a raw prefix (whitespace collapse can only shorten it,
    # so 4x leaves room for runs of spaces and line breaks) instead of a whole paper's text.
    snippet = normalise(str(text or "")[:4 * SNIPPET_CHARS])[:SNIPPET_CHARS]
    if not snippet:
        return None
    return next((tag for needle, tag in TEXT if needle in snippet), None)


def of(content, text) -> str | None:
    """Bytes first, then text."""
    return of_bytes(content) or of_text(text)
