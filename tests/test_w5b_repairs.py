"""W5-B repair scripts (backfills/): each one, on a synthetic library, changes exactly the planted
files under --commit and nothing under the dry run; a curated `.ris`, an identity-flagged sidecar and
a file the script does not own stay byte-identical; a second --commit changes nothing; the backup is
byte-identical to the original. Paths with spaces and non-ASCII names are part of every fixture."""
import hashlib
import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

import lit_util
import ris_emit as R
from backfills import (abstract_cleanup, email_scrub, mismatch_restore, ris_text_repair,
                       sidecar_doi_from_ris, sidecar_text_repair)
from litpipe import config
from tests import netmock

REPO = Path(__file__).resolve().parent.parent
SCRIPTS = ("mismatch_restore", "ris_text_repair", "sidecar_text_repair", "abstract_cleanup",
           "sidecar_doi_from_ris", "email_scrub")
TEST_EMAIL = "tester@litpipe-test.org"


# ---------------------------------------------------------------- fixtures
def digest_tree(root: Path) -> dict:
    """{relative path: sha256} for every file under `root`."""
    out = {}
    for d, _dirs, files in os.walk(root):
        for f in files:
            p = Path(d) / f
            out[str(p.relative_to(root))] = hashlib.sha256(p.read_bytes()).hexdigest()
    return out


@pytest.fixture
def env(tmp_path, monkeypatch):
    """A temp projects root with two registered projects (one a subproject), a temp registry whose
    state_dir is under tmp_path, and a work dir as cwd (where default reports land)."""
    root = tmp_path / "projects"
    lib = root / "teaching_alpha" / "literature"
    sub_lib = root / "research_parent" / "research_child" / "papers"
    lib.mkdir(parents=True)
    sub_lib.mkdir(parents=True)
    reg = {"state_dir": str(tmp_path / "state"), "db_dir": str(tmp_path / "db"),
           "projects": {"teaching_alpha": {"lib_dir": "literature", "active": True},
                        "research_parent/research_child": {"lib_dir": "research_child/papers",
                                                           "parent": "research_parent", "active": True}}}
    cfg = tmp_path / "projects.json"
    cfg.write_text(json.dumps(reg), encoding="utf-8")
    monkeypatch.setattr(config, "CONFIG_PATH", cfg)
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    work = tmp_path / "work"
    work.mkdir()
    monkeypatch.chdir(work)
    monkeypatch.setenv("LITPIPE_EMAIL", TEST_EMAIL)

    class E:
        pass
    e = E()
    e.tmp, e.root, e.lib, e.sub_lib, e.work, e.cfg = tmp_path, root, lib, sub_lib, work, cfg
    return e


@pytest.fixture
def fake_manifest(monkeypatch):
    st = netmock.FakeState()
    monkeypatch.setattr(R, "STATE", st)
    return st


def sha(p) -> str:
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


# ================================================================ ris_text_repair
DAMAGED = ("TY  - JOUR\nAU  - Smith, John\nAU  - M&uuml;ller, Hans\nTI  - Heat &amp; <i>cold</i> stress\n"
           "JO  - Physiology &amp; Behavior\nPY  - 2020\nDA  - 2020/01/02\nDO  - 10.1000/test.1\n"
           "UR  - https://doi.org/10.1000/test.1\nAB  - Background: P &lt; 0.05 in\n\ntwo lines.\nER  - \n")
CLEAN_LITERAL = ("TY  - JOUR\nAU  - Doe, Jane\nTI  - When a < b and c > d\nPY  - 2019\n"
                 "AB  - Values a < b hold; p<0.05.\nER  - \n")
ESCAPED_TITLE = "TY  - JOUR\nTI  - &lt;b&gt;Bold title&lt;/b&gt;\nPY  - 2021\nER  - \n"
CURATED_CRLF = DAMAGED.replace("\n", "\r\n")
CURATED_TAGS = ("TY  - JOUR\nAU  - Smith, John\nTI  - Heat &amp; cold\nT2  - Physiology &amp; Behavior\n"
                "N1  - curated note\nPY  - 2020\nER  - \n")
RESIDUE = ("TY  - JOUR\nAU  - M&uuml;ndel, Anna\nTI  - Heat &amp; cold adaptation in runners\n"
           "PY  - 2019\nER  - \n")


def plant_ris(e, fake):
    lib = e.lib
    files = {
        "2020_Smith_HeatColdStress.ris": DAMAGED,
        "2019_Doe_WhenBHold.ris": CLEAN_LITERAL,
        "2021_Unknown_BoldTitle.ris": ESCAPED_TITLE,
        "2020_Smith_Curated CRLF.ris": CURATED_CRLF,
        "2020_Smith_CuratedTags.ris": CURATED_TAGS,
        "2020_Edited_SinceRecord.ris": DAMAGED,
        "2020_Recorded_Pipeline.ris": DAMAGED,
        "2020_Flagged_Stem.ris": DAMAGED,
        "2021_Ünal_Café au lait.ris": DAMAGED,
        "2019_Muumlndel_HeatAmpColdAdaptationRunners.ris": RESIDUE,
        "notes.txt": "Heat &amp; cold\n",
        "2020_Smith_Old.ris.bak": DAMAGED,
    }
    for name, text in files.items():
        (lib / name).write_bytes(text.encode("utf-8"))
    (lib / "2020_Flagged_Stem.identity.json").write_text(json.dumps({"identity": "FLAG"}), encoding="utf-8")
    # the manifest: one file recorded and unchanged, one recorded and edited since
    fake.kv[("ris", R.manifest_key(lib / "2020_Recorded_Pipeline.ris"))] = sha(lib / "2020_Recorded_Pipeline.ris")
    fake.kv[("ris", R.manifest_key(lib / "2020_Edited_SinceRecord.ris"))] = "0" * 64
    return files


RIS_TOUCHED = {"2020_Smith_HeatColdStress.ris", "2021_Unknown_BoldTitle.ris", "2020_Recorded_Pipeline.ris",
               "2021_Ünal_Café au lait.ris", "2019_Muumlndel_HeatAmpColdAdaptationRunners.ris"}


def test_ris_dry_run_writes_nothing_but_the_report(env, fake_manifest):
    plant_ris(env, fake_manifest)
    before = digest_tree(env.root)
    res = ris_text_repair.run(lib_dirs=[env.lib])
    assert res["exit_code"] == 0
    assert digest_tree(env.root) == before
    assert res["changed_files"] == len(RIS_TOUCHED)
    rep = Path(res["report"])
    assert rep.parent == env.work and rep.name.startswith("ris_text_repair_")
    rows = rep.read_text(encoding="utf-8")
    assert "M&uuml;ller" in rows and "Müller" in rows


def test_ris_commit_changes_exactly_the_planted_files(env, fake_manifest):
    files = plant_ris(env, fake_manifest)
    originals = {n: (env.lib / n).read_bytes() for n in files}
    res = ris_text_repair.run(lib_dirs=[env.lib], commit=True)
    assert res["exit_code"] == 0
    for n, data in originals.items():
        now = (env.lib / n).read_bytes()
        if n in RIS_TOUCHED:
            assert now != data, n
            bak = env.lib / (n + ".bak-w5b")
            assert bak.read_bytes() == data, n          # byte-identical backup
        else:
            assert now == data, n                       # curated, flagged, clean, foreign: untouched
            assert not (env.lib / (n + ".bak-w5b")).exists()
    fixed = (env.lib / "2020_Smith_HeatColdStress.ris").read_text(encoding="utf-8")
    assert fixed == ("TY  - JOUR\nAU  - Smith, John\nAU  - Müller, Hans\nTI  - Heat & cold stress\n"
                     "JO  - Physiology & Behavior\nPY  - 2020\nDA  - 2020/01/02\nDO  - 10.1000/test.1\n"
                     "UR  - https://doi.org/10.1000/test.1\nAB  - Background: P < 0.05 in two lines.\nER  - \n")
    # escaped markup loses its tags (display_field repeated), so a second run cannot change it again
    assert "TI  - Bold title\n" in (env.lib / "2021_Unknown_BoldTitle.ris").read_text(encoding="utf-8")
    # the literal "a < b" is text, not a tag
    assert (env.lib / "2019_Doe_WhenBHold.ris").read_text(encoding="utf-8") == CLEAN_LITERAL
    # the recorded file's manifest entry follows the repair; the edited one keeps its record
    rec = fake_manifest.kv[("ris", R.manifest_key(env.lib / "2020_Recorded_Pipeline.ris"))]
    assert rec == sha(env.lib / "2020_Recorded_Pipeline.ris")
    assert fake_manifest.kv[("ris", R.manifest_key(env.lib / "2020_Edited_SinceRecord.ris"))] == "0" * 64
    assert ("ris", R.manifest_key(env.lib / "2020_Smith_HeatColdStress.ris")) not in fake_manifest.kv


def test_ris_second_commit_changes_nothing(env, fake_manifest):
    plant_ris(env, fake_manifest)
    ris_text_repair.run(lib_dirs=[env.lib], commit=True)
    after_first = digest_tree(env.root)
    res = ris_text_repair.run(lib_dirs=[env.lib], commit=True)
    assert res["changed_files"] == 0
    assert digest_tree(env.root) == after_first


def test_ris_stem_residue_is_reported_not_renamed(env, fake_manifest):
    plant_ris(env, fake_manifest)
    res = ris_text_repair.run(lib_dirs=[env.lib], commit=True)
    assert res["stem_residue"] >= 1
    text = Path(res["report"]).read_text(encoding="utf-8")
    assert "filename stem" in text and "uuml" in text and "audit_filenames" in text
    assert (env.lib / "2019_Muumlndel_HeatAmpColdAdaptationRunners.ris").exists()


def test_ris_unreadable_manifest_stops_the_file(env, monkeypatch):
    class Broken:
        def kv_get(self, ns, key):
            raise RuntimeError("database is locked")
    monkeypatch.setattr(R, "STATE", Broken())
    (env.lib / "a.ris").write_bytes(DAMAGED.encode("utf-8"))
    res = ris_text_repair.run(lib_dirs=[env.lib], commit=True)
    assert res["exit_code"] == 2 and res["errors"] == 1 and res["changed_files"] == 0
    assert (env.lib / "a.ris").read_bytes() == DAMAGED.encode("utf-8")


def test_ris_damage_only_leaves_cosmetic_files(env, fake_manifest):
    cosmetic = "TY  - JOUR\nTI  - Fine title\nPY  - 2020\nAB  - AbstractBackground: the aim.\nER  - \n"
    (env.lib / "2020_A_Cosmetic.ris").write_bytes((cosmetic).encode("utf-8"))
    (env.lib / "2020_B_Damaged.ris").write_bytes((DAMAGED).encode("utf-8"))
    res = ris_text_repair.run(lib_dirs=[env.lib], commit=True, damage_only=True)
    assert res["changed_files"] == 1 and res["cosmetic_files"] == 1
    assert (env.lib / "2020_A_Cosmetic.ris").read_text(encoding="utf-8") == cosmetic
    assert "--damage-only" in Path(res["report"]).read_text(encoding="utf-8")
    res = ris_text_repair.run(lib_dirs=[env.lib], commit=True)
    assert res["changed_files"] == 1
    assert "AB  - Background: the aim.\n" in (env.lib / "2020_A_Cosmetic.ris").read_text(encoding="utf-8")


def test_ris_stem_pattern_residue(env, fake_manifest):
    for n in ("2018_Peacuteriard_HeatStress.pdf", "2017_Smith_PhysiologyAmpBehavior.pdf",
              "2016_Jones_AmpkActivation.pdf", "2015_Lee_EffectsAcuteExercise.pdf",
              "2014_Wu_MicrocirculationBearings.pdf"):
        (env.lib / n).write_bytes(b"%PDF-1.4")
    res = ris_text_repair.run(lib_dirs=[env.lib])
    text = Path(res["report"]).read_text(encoding="utf-8")
    assert res["stem_residue"] == 2
    assert "Peacuteriard" in text and "PhysiologyAmpBehavior" in text
    assert "AmpkActivation" not in text and "EffectsAcuteExercise" not in text and "Microcirculation" not in text


def test_ris_pipeline_shape_rules():
    assert ris_text_repair.pipeline_shape(DAMAGED.encode())[0] is not None
    for bad, why in ((CURATED_CRLF, "CR"), (CURATED_TAGS, "T2"), ("\ufeff" + DAMAGED, "byte-order"),
                     (DAMAGED.replace("ER  - \n", "ER  -\n"), "ER"), (DAMAGED + "\n", "ER"),
                     ("TY  - JOUR\nPY  - 2020\nTI  - x\nER  - \n", "order"),
                     ("TY  - XYZ\nTI  - x\nER  - \n", "TY type")):
        entries, reason = ris_text_repair.pipeline_shape(bad.encode("utf-8"))
        assert entries is None and why in reason, (why, reason)
    # build_ris output is pipeline-shaped, and the shape rule knows every type build_ris writes
    out = R.build_ris({"type": "posted-content", "title": "T", "authors": [{"family": "A", "given": "B"}],
                       "year": "2020", "doi": "10.1/x", "url": "https://doi.org/10.1/x", "abstract": "x"})
    assert ris_text_repair.pipeline_shape(out.encode())[0] is not None
    assert set(R._RIS_TYPE.values()) <= ris_text_repair.PIPELINE_TYPES


def test_ris_dry_run_never_creates_the_state_file(env, monkeypatch):
    from litpipe import state
    monkeypatch.setattr(R, "STATE", state)              # the real module, on the conftest temp DB_PATH
    (env.lib / "a.ris").write_bytes((DAMAGED).encode("utf-8"))
    res = ris_text_repair.run(lib_dirs=[env.lib])
    assert res["changed_files"] == 1
    assert not Path(state.DB_PATH).exists()


def test_ris_project_selection_and_report_guard(env, fake_manifest):
    (env.sub_lib / "2020_X_Y.ris").write_bytes((DAMAGED).encode("utf-8"))
    res = ris_text_repair.run(projects=["research_parent/research_child"])
    assert res["changed_files"] == 1 and res["per_library"]["research_parent/research_child"]["ris"] == 1
    assert ris_text_repair.run(projects=["nope"])["exit_code"] == 1
    assert ris_text_repair.run(lib_dirs=[env.lib], report=str(env.lib / "r.csv"))["exit_code"] == 1
    every = ris_text_repair.run()                        # default: every active project
    assert every["libraries"] == 2


# ================================================================ sidecar_text_repair
def sidecar(text, **kw):
    rec = {"doi": "10.1000/x", "title": "Baro\ufb02ex\u00a0control", "year": "2020", "authors": ["A B"],
           "text": text, "extractor": "pdfminer.six", "extracted_from_pdf": True,
           "figures": [{"label": "Fig 1", "caption": "E\ufb03cacy\u2009test"}], "n_formulas": 0}
    rec.update(kw)
    return rec


def plant_sidecars(e):
    lib = e.lib
    recs = {
        "2020_A_Pdf.fulltext.json": sidecar("The baro\ufb02ex at 10\u00a0mg and\u200b more\ncore-\nbody\n12\n"
                                            "\u201cquoted\u201d \u2013 dash"),
        "2020_B_Jats.fulltext.json": {"pmcid": "PMC1", "title": "J", "abstract": "Dose 5\u00a0mg",
                                      "sections": [{"title": "Intro", "text": "Heat\u00a0stress"}],
                                      "text": "# J\n\nDose 5\u00a0mg"},
        "2020_C_Flagged.fulltext.json": sidecar("\ufb01ne", identity="FLAG"),
        "2020_D_FlagFile.fulltext.json": sidecar("\ufb01ne"),
        "2020_E_Clean.fulltext.json": {"title": "Clean", "text": "nothing to do"},
        "2021_Ünal_Café au lait.fulltext.json": sidecar("e\ufb00ort"),
    }
    for name, rec in recs.items():
        (lib / name).write_bytes((json.dumps(rec, indent=2, ensure_ascii=False)).encode("utf-8"))
    (lib / "2020_D_FlagFile.identity.json").write_text(json.dumps({"identity": "FLAG"}), encoding="utf-8")
    (lib / "2020_F_Broken.fulltext.json").write_text("{not json", encoding="utf-8")
    (lib / "data.json").write_text(json.dumps({"text": "\ufb01"}), encoding="utf-8")
    (lib / "2020_A_Pdf.ris").write_bytes((DAMAGED).encode("utf-8"))
    return recs


SC_TOUCHED = {"2020_A_Pdf.fulltext.json", "2020_B_Jats.fulltext.json", "2021_Ünal_Café au lait.fulltext.json"}


def test_sidecar_text_dry_run_then_commit_then_idempotent(env):
    recs = plant_sidecars(env)
    before = digest_tree(env.root)
    dry = sidecar_text_repair.run(lib_dirs=[env.lib])
    assert digest_tree(env.root) == before
    assert dry["changed_files"] == len(SC_TOUCHED) and dry["errors"] == 1 and dry["exit_code"] == 2
    assert dry["skipped_flag"] == 2
    originals = {n: (env.lib / n).read_bytes() for n in os.listdir(env.lib)}
    res = sidecar_text_repair.run(lib_dirs=[env.lib], commit=True)
    for n, data in originals.items():
        now = (env.lib / n).read_bytes()
        if n in SC_TOUCHED:
            assert now != data
            assert (env.lib / (n + ".bak-w5b")).read_bytes() == data
        else:
            assert now == data, n
    new = json.loads((env.lib / "2020_A_Pdf.fulltext.json").read_text(encoding="utf-8"))
    old = recs["2020_A_Pdf.fulltext.json"]
    # only the non-destructive rules: no de-hyphenation, page-number or typography pass
    assert new["text"] == "The baroflex at 10 mg and more\ncore-\nbody\n12\n“quoted” – dash"
    assert new["title"] == "Baroflex control" and new["figures"][0]["caption"] == "Efficacy test"
    for k in old:                                   # every other key keeps its value
        if k not in ("text", "title", "figures"):
            assert new[k] == old[k]
    assert new["extractor"] == "pdfminer.six" and "clean_pdf_text" in new["repaired_by"]
    # the layout (indent 2, UTF-8 text) is kept
    assert (env.lib / "2020_A_Pdf.fulltext.json").read_text(encoding="utf-8") == \
        json.dumps(new, indent=2, ensure_ascii=False)
    jats = json.loads((env.lib / "2020_B_Jats.fulltext.json").read_text(encoding="utf-8"))
    assert jats["sections"][0]["text"] == "Heat stress" and jats["abstract"] == "Dose 5 mg"
    after = digest_tree(env.root)
    again = sidecar_text_repair.run(lib_dirs=[env.lib], commit=True)
    assert again["changed_files"] == 0 and digest_tree(env.root) == after


def test_sidecar_json_layout_round_trip():
    for kw in ({"indent": 2, "ensure_ascii": False}, {"indent": None, "ensure_ascii": True},
               {"indent": 4, "ensure_ascii": True}):
        rec = {"a": "é\u00a0x", "b": [1, {"c": None}], "d": 1.5}
        raw = json.dumps(rec, **kw)
        lay = sidecar_text_repair.json_layout(raw)
        assert sidecar_text_repair.dump(rec, lay) == raw, kw


# ================================================================ sidecar_doi_from_ris
def ris_with(doi, title="Heat stress responses in endurance athletes"):
    return f"TY  - JOUR\nAU  - Doe, Jane\nTI  - {title}\nPY  - 2020\nDO  - {doi}\nER  - \n"


def plant_doi_cases(e):
    lib = e.lib
    filler = " ".join(["Body text about thermoregulation and sweat rate."] * 20)
    cases = {
        "2020_A_DoiInText": ({"doi": "", "title": "", "text": f"Journal X\ndoi: 10.1000/abc.1\n{filler}"},
                             ris_with("10.1000/abc.1")),
        "2020_B_TitleMatch": ({"title": "", "text": f"Heat stress responses in endurance athletes\n{filler}"},
                              ris_with("10.1000/abc.2")),
        "2020_C_Unrelated": ({"doi": "", "title": "", "text": f"A study of plant roots\n{filler}"},
                             ris_with("10.1000/abc.3")),
        "2020_D_Flagged": ({"doi": "", "identity": "FLAG", "text": "doi: 10.1000/abc.4"}, ris_with("10.1000/abc.4")),
        "2020_E_HasDoi": ({"doi": "10.1000/keep", "text": "doi: 10.1000/abc.5"}, ris_with("10.1000/abc.5")),
        "2020_F_Placeholder": ({"doi": "", "text": "doi: 10.1145/nnnnnnn.nnnnnnn"}, ris_with("10.1145/nnnnnnn.nnnnnnn")),
        "2020_G_LetterTail": ({"doi": "", "text": f"doi:10.1088/2053-1591/acdecd {filler}"},
                              ris_with("10.1088/2053-1591/acdecd")),
        "2021_Ünal_Café au lait": ({"doi": None, "text": f"https://doi.org/10.1000/abc.6\n{filler}"},
                                   ris_with("10.1000/abc.6")),
    }
    for stem, (rec, ris) in cases.items():
        (lib / f"{stem}.fulltext.json").write_bytes((json.dumps(rec, indent=2, ensure_ascii=False)).encode("utf-8"))
        (lib / f"{stem}.ris").write_bytes((ris).encode("utf-8"))
    (lib / "2020_H_NoRis.fulltext.json").write_text(json.dumps({"doi": "", "text": "x"}), encoding="utf-8")
    return cases


DOI_TOUCHED = {"2020_A_DoiInText.fulltext.json", "2020_B_TitleMatch.fulltext.json",
               "2020_G_LetterTail.fulltext.json", "2021_Ünal_Café au lait.fulltext.json"}


def test_sidecar_doi_fill_exactly_the_passing_sidecars(env):
    cases = plant_doi_cases(env)
    before = digest_tree(env.root)
    dry = sidecar_doi_from_ris.run(lib_dirs=[env.lib])
    assert digest_tree(env.root) == before
    assert dry["changed_files"] == len(DOI_TOUCHED)
    assert dry["identity_mismatch"] == 1 and dry["skipped_flag"] == 1 and dry["bad_do"] == 1
    originals = {n: (env.lib / n).read_bytes() for n in os.listdir(env.lib)}
    sidecar_doi_from_ris.run(lib_dirs=[env.lib], commit=True)
    for n, data in originals.items():
        if n in DOI_TOUCHED:
            assert (env.lib / n).read_bytes() != data
            assert (env.lib / (n + ".bak-w5b")).read_bytes() == data
        else:
            assert (env.lib / n).read_bytes() == data, n
    got = {n: json.loads((env.lib / n).read_text(encoding="utf-8"))["doi"] for n in DOI_TOUCHED}
    assert got["2020_A_DoiInText.fulltext.json"] == "10.1000/abc.1"
    assert got["2020_B_TitleMatch.fulltext.json"] == "10.1000/abc.2"
    assert got["2020_G_LetterTail.fulltext.json"] == "10.1088/2053-1591/acdecd"   # structured: kept whole
    rec = json.loads((env.lib / "2020_B_TitleMatch.fulltext.json").read_text(encoding="utf-8"))
    assert "TITLE_MATCH" in rec["repaired_by"] and rec["text"] == cases["2020_B_TitleMatch"][0]["text"]
    after = digest_tree(env.root)
    assert sidecar_doi_from_ris.run(lib_dirs=[env.lib], commit=True)["changed_files"] == 0
    assert digest_tree(env.root) == after


# ================================================================ abstract_cleanup
def make_db(path, rows):
    import duckdb
    con = duckdb.connect(str(path))
    con.execute("CREATE TABLE paper_metadata (doi VARCHAR PRIMARY KEY, year INTEGER, lastname VARCHAR, "
                "title VARCHAR, venue VARCHAR, authors VARCHAR, abstract VARCHAR, "
                "abstract_attempted_at TIMESTAMP, refreshed_at TIMESTAMP)")
    for d, ab in rows:
        con.execute("INSERT INTO paper_metadata (doi, abstract) VALUES (?, ?)", [d, ab])
    con.close()


ABSTRACTS = [("10.1/a", "<jats:p>Heat &amp; cold, \"quoted\"\nand a comma</jats:p>"),
             ("10.1/b", "Abstract Background: fine."),
             ("10.1/c", "Plain text."),
             ("10.1/d", "Abstract"),
             ("10.1/e", None),
             ("10.1/f", "p &amp;amp;lt; 0.05")]


def read_abstracts(path):
    import duckdb
    con = duckdb.connect(str(path), read_only=True)
    try:
        return dict(con.execute("SELECT doi, abstract FROM paper_metadata").fetchall())
    finally:
        con.close()


def test_abstract_cleanup_dry_run_commit_idempotent(env):
    db = env.tmp / "copy" / "index copy.duckdb"
    db.parent.mkdir()
    make_db(db, ABSTRACTS)
    data = db.read_bytes()
    assert abstract_cleanup.run()["exit_code"] == 1                       # --db is required
    dry = abstract_cleanup.run(db=str(db))
    assert dry["exit_code"] == 0 and dry["changed"] == 4 and dry["emptied"] == 1
    assert db.read_bytes() == data
    res = abstract_cleanup.run(db=str(db), commit=True)
    assert res["exit_code"] == 0 and res["updated"] == 4 and res["left_after_update"] == 0
    assert Path(res["backup"]).read_bytes() == data
    got = read_abstracts(db)
    assert got["10.1/a"] == 'Heat & cold, "quoted" and a comma'
    assert got["10.1/b"] == "Background: fine." and got["10.1/c"] == "Plain text."
    assert got["10.1/d"] == "" and got["10.1/e"] is None and got["10.1/f"] == "p < 0.05"
    again = abstract_cleanup.run(db=str(db), commit=True, backup=False)
    assert again["changed"] == 0 and read_abstracts(db) == got


def test_abstract_cleanup_dry_run_opens_read_only(env, monkeypatch):
    import duckdb
    db = env.tmp / "a.duckdb"
    make_db(db, ABSTRACTS)
    seen = []
    real = duckdb.connect

    def spy(path, read_only=False, **kw):
        seen.append(read_only)
        return real(path, read_only=read_only, **kw)
    monkeypatch.setattr(duckdb, "connect", spy)
    assert abstract_cleanup.run(db=str(db))["exit_code"] == 0
    assert seen == [True]


def test_abstract_cleanup_refuses_the_live_db_without_a_copy(env, capsys):
    live = env.tmp / "db" / "portfolio.duckdb"                           # the registry's db_dir
    live.parent.mkdir()
    make_db(live, ABSTRACTS)
    data = live.read_bytes()
    res = abstract_cleanup.run(db=str(live), commit=True)
    assert res["exit_code"] == 1 and live.read_bytes() == data
    err = capsys.readouterr().err
    assert "Make a copy first" in err and "portfolio.pre-w5b.duckdb" in err
    assert abstract_cleanup.run(db=str(live))["exit_code"] == 0          # a read-only dry run is fine
    ok = abstract_cleanup.run(db=str(live), commit=True, i_made_a_copy=True, backup=False)
    assert ok["exit_code"] == 0 and ok["updated"] == 4


# ================================================================ email_scrub
def plant_reports(e):
    root = e.root / "teaching_alpha"
    enc = TEST_EMAIL.replace("@", "%40")
    files = {
        root / "lit_pull_queue.2026-09-01.unpaywall.csv":
            f"doi,error\n10.1/x,\"HTTP_422 https://api.unpaywall.org/v2/10.1/x?email={enc}\"\n"
            f"10.1/y,\"retry email={TEST_EMAIL}&x=1\"\n10.1/z,ok\n",
        root / "lit_pull_queue_residuals.md":
            f"- 10.1/x: contact mailto:{TEST_EMAIL}\n- by {TEST_EMAIL.upper()}\n- other@example.net stays\n",
        root / "_archive" / "2026-09-30" / "lit_sweep_exhaust" / "sweep_report_1.csv":
            f"a,b\n1,\"https://h/x?email={TEST_EMAIL}\"\n",
        e.root / "research_parent" / "_archive" / "old" / "_downloads_import_2026.csv":
            f"f,err\nx.pdf,\"mailto:{TEST_EMAIL}\"\n",
        root / "notes.md": f"Contact {TEST_EMAIL}\n",
        root / "CLAUDE.md": f"owner {TEST_EMAIL}\n",
        root / "participants.csv": "name,mail\nA,a@uni.edu\n",
        root / "literature" / "x.ris": f"TY  - JOUR\nN1  - mailto:{TEST_EMAIL}\nER  - \n",
        root / ".git" / "lit_pull_queue.csv": f"email={TEST_EMAIL}\n",
        root / "_archive" / ".git" / "x_report.csv": f"email={TEST_EMAIL}\n",
        root / "lit_pull_queue.2026-09-02.normalized.csv": "doi\n10.1/q other@example.net\n",
        e.lib / "_archive" / "lit_runs" / "lit_pull_queue.2026-05-21.unpaywall.csv":
            f"doi,error\n10.1/w,\"x?email={enc}\"\n",
        e.lib / "_backfill_report.csv": f"pdf,detail\nx.pdf,\"email={TEST_EMAIL}\"\n",
    }
    for p, text in files.items():
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(text.encode("utf-8"))
    return files


def test_email_scrub_replaces_leak_forms_in_pipeline_reports_only(env, capsys):
    files = plant_reports(env)
    touched = {p for p in files if p.name in ("lit_pull_queue.2026-09-01.unpaywall.csv",
                                              "lit_pull_queue_residuals.md", "sweep_report_1.csv",
                                              "_downloads_import_2026.csv",
                                              "lit_pull_queue.2026-05-21.unpaywall.csv")}
    before = digest_tree(env.root)
    dry = email_scrub.run()
    out = capsys.readouterr()
    assert digest_tree(env.root) == before
    assert dry["files_changed"] == 5 and dry["occurrences"] == 7
    report = Path(dry["report"]).read_text(encoding="utf-8")
    for text in (out.out, out.err, report):
        assert TEST_EMAIL not in text.lower() and "tester" not in text.lower()
    assert "t***@litpipe-test.org" in report
    originals = {p: p.read_bytes() for p in files}
    res = email_scrub.run(commit=True)
    for p, data in originals.items():
        if p in touched:
            assert p.read_bytes() != data
            assert Path(str(p) + ".bak-w5b").read_bytes() == data
        else:
            assert p.read_bytes() == data, p
    csv_text = (env.root / "teaching_alpha" / "lit_pull_queue.2026-09-01.unpaywall.csv").read_text(encoding="utf-8")
    assert "[EMAIL-REDACTED]" in csv_text and "&x=1" in csv_text and "10.1/z,ok" in csv_text
    md = (env.root / "teaching_alpha" / "lit_pull_queue_residuals.md").read_text(encoding="utf-8")
    assert "[MAILTO-REDACTED]" in md and "by REDACTED" in md and "other@example.net stays" in md
    assert res["backups"]
    for b in res["backups"]:
        os.remove(b)
    after = digest_tree(env.root)
    again = email_scrub.run(commit=True)
    assert again["files_changed"] == 0 and digest_tree(env.root) == after


def test_email_scrub_name_filter():
    yes = ("lit_pull_queue.csv", "lit_pull_queue.2026-09-01.unpaywall.csv", "LIT_PULL_QUEUE_residuals.md",
           "pmc_fetch_report_2.csv", "_downloads_import_2026-09-01.csv", "sweep_report.csv")
    no = ("notes.md", "CLAUDE.md", "data.csv", "x.ris", "x.fulltext.json", "projects.json",
          "lit_pull_queue.json", "report.csv", "x_report.json")
    assert all(email_scrub.name_matches(n) for n in yes)
    assert not any(email_scrub.name_matches(n) for n in no)


def test_email_scrub_uses_redact_tokens():
    from litpipe import ledger
    assert email_scrub.scrub_line("a?email=x%40y.org&b", __import__("collections").Counter()) == \
        f"a?{ledger.EMAIL_TOKEN}&b"


# ================================================================ mismatch_restore
def make_pdf(path, text):
    import fitz
    d = fitz.open()
    p = d.new_page()
    lines = []
    for para in text.split("\n"):
        lines.extend(textwrap.wrap(para, 90) or [""])
    p.insert_text((40, 50), "\n".join(lines), fontsize=8)
    d.save(str(path))
    d.close()


FILLER = " ".join(["Participants completed a graded exercise test in a hot room."] * 12)


def plant_mismatch(e):
    lib, mm = e.lib, e.lib / "_mismatch"
    mm.mkdir()
    pdfs = {
        "2020_Unknown_HeatAcclimationTrial.pdf":
            f"Journal of Example 2020\nhttps://doi.org/10.1000/right.1\nHeat acclimation trial\n{FILLER}",
        "2019_Smith_Café au lait study.pdf":
            f"Other header doi:10.1000/other.9\nA cafe study\ndoi: 10.1000/right.2\n{FILLER}",
        "2018_Wrong_NoLongerPasses.pdf": f"Completely different paper about plants\n{FILLER}",
        "2017_Dup_Copy.pdf": f"doi 10.1000/dup.1\n{FILLER}",
        "2016_Held_Copy.pdf": f"doi 10.1000/held.1\n{FILLER}",
        "2015_Medium_Restore.pdf": f"doi 10.1000/med.1\n{FILLER}",
        "2014_Keep_Quarantined.pdf": f"doi 10.1000/keep.1\n{FILLER}",
        "2013_New_Unproposed.pdf": f"doi 10.1000/new.1\n{FILLER}",
        "2012_Taken_Destination.pdf": f"doi 10.1000/taken.1\n{FILLER}",
    }
    for name, text in pdfs.items():
        make_pdf(mm / name, text)
    (lib / "2012_Taken_Destination.pdf").write_bytes(b"%PDF-1.4 another file already here")
    rows = [
        ("teaching_alpha", "2020_Unknown_HeatAcclimationTrial.pdf", "10.1000/right.1", "RESTORE", "HIGH", ""),
        ("teaching_alpha", "2019_Smith_Café au lait study.pdf", "10.1000/right.2", "RESTORE", "HIGH", ""),
        ("teaching_alpha", "2018_Wrong_NoLongerPasses.pdf", "10.1000/gone.9", "RESTORE", "HIGH", ""),
        ("teaching_alpha", "2017_Dup_Copy.pdf", "10.1000/dup.1", "DUPLICATE_COPY", "HIGH", "same as x"),
        ("teaching_alpha", "2016_Held_Copy.pdf", "10.1000/held.1", "ALREADY_HELD", "HIGH", "held as y"),
        ("teaching_alpha", "2015_Medium_Restore.pdf", "10.1000/med.1", "RESTORE", "MEDIUM", ""),
        ("teaching_alpha", "2014_Keep_Quarantined.pdf", "10.1000/keep.1", "KEEP_QUARANTINED", "LOW", ""),
        ("teaching_alpha", "2011_Gone_Already.pdf", "10.1000/g.1", "RESTORE", "HIGH", ""),
        ("teaching_alpha", "2012_Taken_Destination.pdf", "10.1000/taken.1", "RESTORE", "HIGH", ""),
    ]
    prop = e.tmp / "proposal.csv"
    import csv
    with open(prop, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["project", "path", "queue_doi", "report", "verdict", "proposal", "confidence",
                    "pdf_doi_primary", "evidence"])
        for proj, name, doi, action, conf, ev in rows:
            w.writerow([proj, f"C:\\elsewhere\\_mismatch\\{name}", doi, "", "OK", action, conf, "", ev])
    return prop


def test_mismatch_restore_dry_run_moves_nothing(env):
    prop = plant_mismatch(env)
    before = digest_tree(env.root)
    res = mismatch_restore.run(proposal=str(prop), use_db=False)
    assert digest_tree(env.root) == before
    assert res["exit_code"] == 0
    assert res["would_restore"] == 3 and res["skipped"] == 1               # the identity that fails now
    assert res["delete_candidates"] == 2 and res["not_high"] == 2 and res["unproposed"] == 1
    assert res["gone"] == 1 and res["mismatch_pdfs"] == 9
    text = Path(res["report"]).read_text(encoding="utf-8")
    assert "identity no longer passes" in text and "DOI-hash suffix" in text and "UNPROPOSED" in text


def test_mismatch_restore_commit_moves_exactly_the_passing_rows(env):
    prop = plant_mismatch(env)
    mm = env.lib / "_mismatch"
    originals = {n: (mm / n).read_bytes() for n in os.listdir(mm)}
    taken = (env.lib / "2012_Taken_Destination.pdf").read_bytes()
    res = mismatch_restore.run(proposal=str(prop), use_db=False, commit=True)
    assert res["restored"] == 3
    hashed = [p.name for p in env.lib.glob("2012_Taken_Destination_*.pdf")]
    assert len(hashed) == 1                                               # the taken stem: a DOI-hash name
    restored = {"2020_Unknown_HeatAcclimationTrial.pdf": "2020_Unknown_HeatAcclimationTrial.pdf",
                "2019_Smith_Café au lait study.pdf": "2019_Smith_Café au lait study.pdf",
                "2012_Taken_Destination.pdf": hashed[0]}
    for n, data in originals.items():
        if n in restored:
            dest = env.lib / restored[n]
            assert not (mm / n).exists()
            assert dest.read_bytes() == data                              # moved, bytes unchanged
            ids = json.loads(dest.with_suffix(".identity.json").read_text(encoding="utf-8"))
            assert ids["identity"] == "OK" and ids["restored_from"] == f"_mismatch/{n}"
            assert ids["source"] == "mismatch_restore" and ids["pdf"] == dest.name
        else:
            assert (mm / n).read_bytes() == data, n                       # nothing else moved or deleted
    assert (env.lib / "2012_Taken_Destination.pdf").read_bytes() == taken
    after = digest_tree(env.root)
    again = mismatch_restore.run(proposal=str(prop), use_db=False, commit=True)
    assert again["restored"] == 0 and digest_tree(env.root) == after


def test_mismatch_restore_dec14_name_from_the_index_and_write_ris(env, monkeypatch):
    prop = plant_mismatch(env)
    import duckdb
    db = env.tmp / "meta.duckdb"
    con = duckdb.connect(str(db))
    con.execute("CREATE TABLE paper_metadata (doi VARCHAR PRIMARY KEY, year INTEGER, lastname VARCHAR, "
                "title VARCHAR, authors VARCHAR)")
    con.execute("INSERT INTO paper_metadata VALUES ('10.1000/right.1', 2020, '', "
                "'Heat acclimation trial', 'Ana van der Walt; B. Jones')")
    # the index's title for this DOI is a parsed reference string: the file keeps its name
    con.execute("INSERT INTO paper_metadata VALUES ('10.1000/right.2', 2019, '', "
                "'10945768 https doi org 10', 'C. Writer')")
    con.close()
    calls = []

    def fake_emit(doi, pdf_path, overwrite=False):
        calls.append((doi, Path(pdf_path).name))
        Path(pdf_path).with_suffix(".ris").write_text("TY  - JOUR\nER  - \n", encoding="utf-8")
        return "OK", str(Path(pdf_path).with_suffix(".ris"))
    monkeypatch.setattr(R, "emit_ris_for_pdf", fake_emit)
    assert mismatch_restore.run(proposal=str(prop), db=str(db), write_ris=True)["exit_code"] == 1  # needs --commit
    res = mismatch_restore.run(proposal=str(prop), db=str(db), commit=True, write_ris=True)
    assert res["restored"] == 3
    assert (env.lib / "2020_vanderWalt_HeatAcclimationTrial.pdf").exists()
    assert ("10.1000/right.1", "2020_vanderWalt_HeatAcclimationTrial.pdf") in calls
    assert ("10.1000/right.2", "2019_Smith_Café au lait study.pdf") in calls
    assert "slug ratio" in Path(res["report"]).read_text(encoding="utf-8")


def test_mismatch_dest_name_guard():
    import unpaywall_fetch_v2 as uf
    pdf = Path("2026_Unknown_NetworkEdgeInferenceLargeLanguageModels.pdf")
    name, how = mismatch_restore.dest_name(pdf, {"title": "Network Edge Inference for Large Language Models",
                                                 "authors": "Zhixiong Chen; B. Zhu"}, uf)
    assert name == "2026_Chen_NetworkEdgeInferenceLargeLanguageModels.pdf" and "DEC-14" in how
    for meta in ({"title": "Neck circumference screening for elevated blood pressure", "authors": "Anon"},
                 {"title": "6 https doi org 10 1002", "authors": "A. Khudairy"}, {"title": "x", "authors": ""}):
        name, how = mismatch_restore.dest_name(Path("2026_Recio_NeckCircumferenceScreeningElevated.pdf"), meta, uf)
        assert name == "2026_Recio_NeckCircumferenceScreeningElevated.pdf" and how.startswith("kept name")


# ================================================================ CLI shape
@pytest.mark.parametrize("name", SCRIPTS)
def test_help_from_a_foreign_cwd(name, tmp_path):
    r = subprocess.run([sys.executable, str(REPO / "backfills" / f"{name}.py"), "--help"],
                       capture_output=True, text=True, cwd=str(tmp_path), timeout=120)
    assert r.returncode == 0, r.stderr
    assert "--commit" in r.stdout


@pytest.mark.parametrize("mod", [mismatch_restore, ris_text_repair, sidecar_text_repair, abstract_cleanup,
                                 sidecar_doi_from_ris, email_scrub])
def test_house_cli_shape(mod):
    assert callable(mod.run) and callable(mod.main)
    assert "--commit" in (mod.__doc__ or "") and "dry run" in (mod.__doc__ or "").lower()


def test_main_reads_argv(env, monkeypatch):
    (env.lib / "a.ris").write_bytes((DAMAGED).encode("utf-8"))
    monkeypatch.setattr(sys, "argv", ["ris_text_repair.py", "--lib-dir", str(env.lib)])
    assert ris_text_repair.main() == 0
    assert (env.lib / "a.ris").read_text(encoding="utf-8") == DAMAGED


def test_address_mask_never_leaks():
    for s in (TEST_EMAIL, TEST_EMAIL.replace("@", "%40"), TEST_EMAIL.replace("@", "%2540"),
              f"x?email={TEST_EMAIL}&y"):
        for mod in (ris_text_repair, sidecar_text_repair, sidecar_doi_from_ris, abstract_cleanup,
                    mismatch_restore, email_scrub):
            m = mod.mask(s)
            assert "tester" not in m and "litpipe-test.org" in m
