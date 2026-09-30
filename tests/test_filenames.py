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
from lit_util import companion_path
import os
from pathlib import Path


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


# ---------- last_name spec (documented in unpaywall_fetch_v2.last_name) ----------

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


# ---------- DEC-14 (REG-I17): initials of 1-5 capitals, particles joined, new files only ----------

@pytest.mark.parametrize("authors,want", [
    # the dispatch cases
    ("Durnin JVGA; Womersley J",          "Durnin"),
    ("van der Walt JHA; Smith B",         "vanderWalt"),
    ("De Vries H",                        "DeVries"),
    ("Garcia-Lopez J",                    "Garcia-Lopez"),
    ("García-López J",                    "Garcia-Lopez"),
    # shapes found in the queue history (census 2026-09-30)
    ("Alberti KGMM",                      "Alberti"),
    ("Masset KVDSB",                      "Masset"),       # 5 initials
    ("Pan L-M",                           "Pan"),          # hyphenated initials
    ("Stephenson Mark D",                 "Stephenson"),   # Surname Given Initial
    ("Coyne Joseph O C",                  "Coyne"),
    ("A Purcell S",                       "Purcell"),
    ("Del Giudice M",                     "DelGiudice"),
    ("van Amstel RBE",                    "vanAmstel"),
    ("St. Pierre J",                      "StPierre"),     # the auditor's own example
    ("Smith J.M.",                        "Smith"),
    ("Smith J M",                         "Smith"),
    ("Hugo De Vries",                     "DeVries"),      # Given Particle Surname
    ("I. Di Domenico",                    "DiDomenico"),
    ("S. van der Zwaard",                 "vanderZwaard"),
    ("Gabriel G. de la Torre",            "delaTorre"),
    ("Tom H. B. den Ouden",               "denOuden"),
    ("Moda Tomé Edson dos Reis",          "dosReis"),
    ("Bin Yang",                          "Yang"),         # a given name, not a particle
    ("Le Ma",                             "Ma"),
    ("Di Tang",                           "Tang"),
    ("Le Roux, Elisa",                    "LeRoux"),       # Surname, Given: all of it
    ("Del Vecchio A, Casolo A, Negro F, et al.", "DelVecchio"),
    ("Tseng et al. (Cornell)",            "Tseng"),        # cut at "et al"
    ("et al",                             "Unknown"),
    ("",                                  "Unknown"),
    (None,                                "Unknown"),
])
def test_last_name_dec14(authors, want):
    assert last_name(authors) == want


@pytest.mark.parametrize("authors,old", [
    ("Durnin JVGA; Womersley J",  "JVGA"),
    ("van der Walt JHA; Smith B", "van"),
    ("De Vries H",                "De"),
    ("Periard JD; Casa DJ",       "Periard"),
])
def test_legacy_last_name_pins_the_old_rule(authors, old):
    """Existing files keep their names (DEC-14: new files only); the stage recognises them by the
    old rule, so the old rule must stay exactly as it was."""
    from unpaywall_fetch_v2 import legacy_last_name
    assert legacy_last_name(authors) == old


def test_particle_surnames_agree_with_the_auditors():
    """audit_filenames builds the expected name from Crossref's family name with spaces removed
    (`van der Walt` -> `vanderWalt`); the writer now agrees instead of writing `van`."""
    for authors, family in (("van der Walt JHA; Smith B", "van der Walt"), ("De Vries H", "De Vries"),
                            ("Del Giudice M", "Del Giudice")):
        assert build_filename("2019", authors, "Heat and the heart") == \
            canonical_filename("2019", family, "Heat and the heart")


# ---------- DEC-15: one slug writer (ris_emit.slug) ----------

def test_slug_title_is_ris_emit_slug():
    import ris_emit
    for t in ["Is this the effect that heat has, or not?", "These are those that been",
              "<i>In vivo</i> heat acclimation", "Périard Heat Stress", "", "A the of"]:
        assert slug_title(t) == ris_emit.slug(t), t
    assert slug_title("Is heat stress or cold stress worse", max_words=2) == "HeatStress"


def test_slug_title_has_no_stoplist_of_its_own(monkeypatch):
    """Delegation, not a copy: a word added to ris_emit.SLUG_SKIP disappears from slug_title too."""
    import ris_emit
    monkeypatch.setattr(ris_emit, "SLUG_SKIP", ris_emit.SLUG_SKIP | {"heat"})
    assert slug_title("Heat acclimation in athletes") == "AcclimationAthletes"


def test_build_filename_is_canonical_stem():
    import ris_emit
    from unpaywall_fetch_v2 import last_name as ln
    for year, authors, title in [("2024", "Müller A; Schmidt B", "Is the drift in athletes real"),
                                 ("n/a", "", ""), ("2019", "van der Walt JHA", "The heart")]:
        assert build_filename(year, authors, title) == ris_emit.canonical_stem(year, ln(authors), title) + ".pdf"


def test_legacy_names_stay_derivable():
    """Files written before DEC-14/15 keep their names; legacy_build_filename rebuilds them (the
    stage's SKIP_EXISTS check and a queue-history map for old files need it)."""
    from unpaywall_fetch_v2 import legacy_build_filename
    args = ("2019", "van der Walt JHA; Smith B", "Is the effect of heat on the heart large")
    assert legacy_build_filename(*args) == "2019_van_IsEffectHeatHeartLarge.pdf"
    assert build_filename(*args) == "2019_vanderWalt_EffectHeatHeartLarge.pdf"


# ---------- lock-in: audit_filenames --queue-history still maps a known file ----------

KNOWN_ROW = {"doi": "10.1123/ijspp.2019-0123", "title": "Heat acclimation and athletic performance",
             "authors": "Periard JD; Racinais S", "year": "2020", "destination": "lib", "notes": ""}
KNOWN_FILE = "2020_Periard_HeatAcclimationAthleticPerformance.pdf"


def _queue_history(tmp_path, rows):
    import csv as _csv
    p = tmp_path / "lit_pull_queue.2026-09-01.processed.csv"
    with open(p, "w", encoding="utf-8", newline="") as f:
        w = _csv.DictWriter(f, fieldnames=["doi", "title", "authors", "year", "destination", "notes"])
        w.writeheader()
        w.writerows(rows)
    return str(tmp_path / "lit_pull_queue.*.processed*.csv")


def test_queue_history_maps_a_known_file(tmp_path):
    import audit_filenames
    from unpaywall_fetch_v2 import legacy_build_filename
    row = KNOWN_ROW
    assert build_filename(row["year"], row["authors"], row["title"]) == KNOWN_FILE
    assert legacy_build_filename(row["year"], row["authors"], row["title"]) == KNOWN_FILE
    mapping, paths = audit_filenames.load_queue_history(_queue_history(tmp_path, [row]))
    assert len(paths) == 1 and mapping == {KNOWN_FILE: row["doi"]}


def test_queue_history_cli_path_uses_the_map_for_a_known_file(tmp_path, monkeypatch):
    """The --queue-history flag end to end: a known file with no DOI in its text or sidecar gets
    its DOI from the queue history (Crossref stubbed; nothing is renamed without --execute)."""
    import csv as _csv
    import sys as _sys
    import fitz
    import audit_filenames
    lib = tmp_path / "lib"
    lib.mkdir()
    doc = fitz.open()
    doc.new_page().insert_text((72, 72), "a scan with no identifiers")
    (lib / KNOWN_FILE).write_bytes(doc.tobytes())
    doc.close()
    seen = []
    monkeypatch.setattr(audit_filenames, "crossref", lambda doi: seen.append(doi) or
                        {"title": KNOWN_ROW["title"], "year": "2020", "lastname": "Periard"})
    monkeypatch.setattr(audit_filenames.time, "sleep", lambda s: None)
    report = tmp_path / "audit.csv"
    monkeypatch.setattr(_sys, "argv", ["audit_filenames.py", "--lib-dir", str(lib), "--queue-history",
                                       _queue_history(tmp_path, [KNOWN_ROW]), "--report", str(report)])
    audit_filenames.main()
    assert seen == [KNOWN_ROW["doi"]]
    with open(report, encoding="utf-8") as f:
        rows = list(_csv.DictReader(f))
    assert rows == [{"current": KNOWN_FILE, "proposed": KNOWN_FILE, "doi": KNOWN_ROW["doi"],
                     "status": "ALREADY_CANONICAL_QH"}]
    assert sorted(p.name for p in lib.iterdir()) == [KNOWN_FILE]


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


# ---------- companion_path: dot-safe sidecar naming (2026-06-25 dot-suffix bug regression) ----------

@pytest.mark.parametrize("pdf_name,ext,want", [
    ("2010_Smith_HeatStress.pdf",  ".ris",           "2010_Smith_HeatStress.ris"),
    ("2010_Smith_Heat.4.26.pdf",   ".ris",           "2010_Smith_Heat.4.26.ris"),    # LWW article-number dot
    ("heat_versus_altitude.7.pdf", ".ris",           "heat_versus_altitude.7.ris"),
    ("2024_Pelland_RT_v1.2.pdf",   ".ris",           "2024_Pelland_RT_v1.2.ris"),    # version-string dot
    ("9782889634996.PDF",          ".ris",           "9782889634996.ris"),           # uppercase .PDF
    ("2010_Smith_Heat.4.26.pdf",   ".fulltext.json", "2010_Smith_Heat.4.26.fulltext.json"),
    ("normal.pdf",                 ".txt",           "normal.txt"),
])
def test_companion_path_is_dot_safe(pdf_name, ext, want):
    assert companion_path(Path(pdf_name), ext).name == want


@pytest.mark.parametrize("pdf_name", [
    "2010_Smith_HeatStress.pdf", "2010_Smith_Heat.4.26.pdf",
    "heat_versus_altitude.7.pdf", "2024_Pelland_RT_v1.2.pdf", "9782889634996.PDF",
])
def test_companion_reader_equals_writer(pdf_name):
    """The index/backfill/audit READER (companion_path) must name the .ris exactly as the
    WRITERS do: ris_emit.emit_ris_for_pdf uses os.path.splitext; extract_pdf_fulltext /
    pmc_fetch use fn[:-4]. A mismatch silently de-links the sidecar (the 2026-06-25 bug)."""
    reader = companion_path(Path(pdf_name), ".ris").name
    writer_splitext = os.path.basename(os.path.splitext(pdf_name)[0] + ".ris")  # ris_emit rule
    writer_slice    = pdf_name[:-4] + ".ris"                                    # extract/pmc fn[:-4] rule
    assert reader == writer_splitext == writer_slice


def test_companion_path_differs_from_old_buggy_idiom_on_dotted_stems():
    """Regression: the OLD idiom `pdf.with_suffix("").with_suffix(ext)` mis-derived dotted
    stems (foo.26.pdf -> foo.ris). companion_path must NOT reproduce that."""
    pdf = Path("2010_Smith_Heat.4.26.pdf")
    buggy = pdf.with_suffix("").with_suffix(".ris").name   # documents the OLD wrong behavior
    fixed = companion_path(pdf, ".ris").name
    assert buggy == "2010_Smith_Heat.4.ris"
    assert fixed == "2010_Smith_Heat.4.26.ris" and fixed != buggy
