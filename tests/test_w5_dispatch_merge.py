"""Locks the dispatcher added when merging W5-C2 onto W5-C3 (2026-10-07).

- F2: audit_filenames builds canonical names from litpipe.text.filename_title (unfolded). A filename
  is a stored key; the comparison fold in normalise_title would otherwise turn 133 of 6,274 canonical
  library PDFs into proposed renames. The pinned names are the ones 53ca3e6 produced.
- F3-F5 (W5 item 25, the sibling sweep): the title-similarity functions of preprint_fetch, litpipe.s2
  and ris_emit read Greek letters, curly quotes and Unicode dashes alike; unrelated titles stay apart.
  ris_emit.normalize_title itself stays unfolded (harvest_citations keys identity on it).
"""
import pytest

import audit_filenames
import preprint_fetch
import ris_emit
from litpipe import s2


@pytest.mark.parametrize("title, want", [
    ("β-adrenergic blockade in heat", "2020_Smith_-adrenergicBlockadeHeat.pdf"),
    ("Parkinson's disease and exercise", "2020_Smith_ParkinsonSDiseaseExercise.pdf"),
])
def test_canonical_filename_is_unfolded(title, want):
    assert audit_filenames.canonical_filename("2020", "Smith", title) == want


SIMILARITY = [preprint_fetch.title_similarity, s2.title_similarity, ris_emit.title_similarity]
PAIRS_ALIKE = [
    ("β-adrenergic blockade during exercise in the heat", "beta-adrenergic blockade during exercise in the heat"),
    ("ACSM’s guidelines for exercise testing", "ACSM's guidelines for exercise testing"),
    ("Heat strain – a field study of workers", "Heat strain - a field study of workers"),
]


@pytest.mark.parametrize("sim", SIMILARITY, ids=["preprint_fetch", "s2", "ris_emit"])
@pytest.mark.parametrize("a, b", PAIRS_ALIKE)
def test_title_similarity_folds_variants(sim, a, b):
    assert sim(a, b) == pytest.approx(1.0)


@pytest.mark.parametrize("sim", SIMILARITY, ids=["preprint_fetch", "s2", "ris_emit"])
def test_title_similarity_keeps_unrelated_titles_apart(sim):
    assert sim("Sweat sodium losses in American football players",
               "Cerebral blood flow during hypoxic exercise") < 0.6


def test_ris_normalize_title_stays_unfolded():
    assert ris_emit.normalize_title("β-blockers") != ris_emit.normalize_title("beta-blockers")
