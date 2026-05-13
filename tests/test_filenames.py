"""Filename construction regression tests.

These lock in the writer/auditor agreement that was broken before 2026-05-12.
Previously:
  - `unpaywall_fetch_v2.build_filename` wrote `2024_Müller_*.pdf`
  - `audit_filenames.canonical_filename` proposed `2024_Muller_*.pdf`
  - Result: audit_filenames would propose a rename forever, churning the library.

The fix moved NFKD normalization (via `ris_emit.safe_ascii`) up-front in both
writers, and confirmed cross-script equality on accented inputs.
"""
import pytest

from ris_emit import safe_ascii
from unpaywall_fetch_v2 import build_filename, last_name, slug_title
from audit_filenames import canonical_filename, slug
from preprint_fetch import slug_filename


# ---------- safe_ascii primitive ----------

@pytest.mark.parametrize("inp,want", [
    ("Périard",       "Periard"),
    ("Müller-García", "Muller-Garcia"),
    ("Lüthi",         "Luthi"),
    ("Mølmen",        "Molmen"),
    ("Hjørnevik",     "Hjornevik"),
    ("ß",             "ss"),
    ("Ø",             "O"),
    ("",              ""),
    (None,            ""),
])
def test_safe_ascii(inp, want):
    assert safe_ascii(inp) == want


# ---------- last_name spec (documented in unpaywall_fetch_v2.py:48) ----------

@pytest.mark.parametrize("authors,want", [
    ("Periard JD; Casa DJ",            "Periard"),    # LastName + Initial
    ("J Smith; K Jones",               "Smith"),      # Initial + LastName
    ("Cramer MN, Jay O",               "Cramer"),     # comma-separated authors
    ("Smith, John; Doe, Jane",         "Smith"),      # "Last, First" via ;
    ("Hoffman GE; Roussos P",          "Hoffman"),    # LastName + multi-letter Initials
    ("Malchaire J, Piette A, et al",   "Malchaire"),  # "et al" stripped
    ("T. Gabbett",                     "Gabbett"),    # Initial-LastName
    ("Mølmen Ø; Stensrud T",           "Molmen"),     # Unicode last + Unicode initial
    ("Müller A, García-López B",       "Muller"),     # Umlaut + hyphenated co-author
])
def test_last_name(authors, want):
    assert last_name(authors) == want


# ---------- writer/auditor agreement on accented inputs ----------

# Each entry: (year, authors_string, title, expected_filename).
@pytest.mark.parametrize("year,authors,title,expected", [
    ("2024", "Müller A; Schmidt B",  "Cardiovascular Drift in Périard Athletes",
     "2024_Muller_CardiovascularDriftPeriardAthletes.pdf"),
    ("2021", "Mølmen Ø; Stensrud T", "Living high training low",
     "2021_Molmen_LivingHighTrainingLow.pdf"),
    ("2019", "García-López J",       "Heat acclimation review",
     "2019_Garcia-Lopez_HeatAcclimationReview.pdf"),
    ("2023", "Periard JD",           "Heat adaptation in athletes",
     "2023_Periard_HeatAdaptationAthletes.pdf"),
])
def test_build_filename_is_ascii_only(year, authors, title, expected):
    out = build_filename(year, authors, title)
    assert out == expected
    # Defense in depth: never write non-ASCII bytes
    assert out.encode("ascii", errors="strict") == out.encode("utf-8")


def test_writer_and_auditor_agree():
    """The bug we just fixed: build_filename and canonical_filename diverged
    on accented authors. They must now produce identical filenames so the
    audit pass doesn't churn."""
    inputs = [
        ("2024", "Müller A; Schmidt B",  "Cardiovascular Drift in Périard Athletes"),
        ("2021", "Mølmen Ø; Stensrud T", "Living high training low"),
        ("2019", "García-López J",       "Heat acclimation review"),
        ("2023", "Periard JD",           "Heat adaptation in athletes"),
    ]
    for year, authors, title in inputs:
        writer = build_filename(year, authors, title)
        first_author = authors.split(";")[0].split(",")[0].split()[0]
        auditor = canonical_filename(year, first_author, title)
        assert writer == auditor, (
            f"writer/auditor drift on ({year}, {authors!r}, {title!r}): "
            f"{writer!r} != {auditor!r}"
        )


def test_preprint_filename_mirrors_build_filename():
    """Preprint filenames should use the same NFKD-normalized stem as
    build_filename, with a `_preprint` suffix appended."""
    inputs = [
        ("2024", "Müller A; Schmidt B",  "Cardiovascular Drift in Périard Athletes"),
        ("2021", "Mølmen Ø; Stensrud T", "Living high training low"),
    ]
    for year, authors, title in inputs:
        base  = build_filename(year, authors, title)
        ppr   = slug_filename(year, authors, title)
        # ppr should be base.pdf → base_preprint.pdf
        assert ppr == base.replace(".pdf", "_preprint.pdf"), (
            f"preprint name diverges from canonical: {ppr!r} vs base {base!r}"
        )


# ---------- slug normalization ----------

@pytest.mark.parametrize("title,want", [
    ("Périard Heat Stress",                "PeriardHeatStress"),
    ("Lüthi cardiovascular drift",         "LuthiCardiovascularDrift"),
    ("<i>In vivo</i> heat acclimation",    "VivoHeatAcclimation"),  # HTML stripped, stop words dropped
])
def test_slug_drops_html_and_normalizes(title, want):
    assert slug(title) == want


def test_slug_title_unpaywall_equivalent():
    """unpaywall_fetch_v2.slug_title and audit_filenames.slug both produce
    the title slug used downstream — they should agree on accented inputs."""
    samples = ["Périard Heat Stress", "Lüthi cardiovascular drift",
               "Mølmen Living High Training Low"]
    for s in samples:
        a = slug_title(s)
        b = slug(s)
        assert a == b, f"slug drift on {s!r}: writer={a!r} auditor={b!r}"
