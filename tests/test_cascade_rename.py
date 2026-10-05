"""B6/A2+F3+D3: audit_filenames cascade_rename hardening.

A2 -- cascade_rename is all-or-nothing: a mid-cascade rename failure rolls back the already-done
      renames so the library never lands half-renamed.
F3 -- the sidecar image_path rewrite is atomic (no .tmp residue) and happens after every rename.
D3 -- the dry-run collision detector (seen_canonical) reflects proposed renames in BOTH modes, so a
      dry-run reports the same collisions --execute would.
"""
import csv
import json
import os
import sys

import pytest

import audit_filenames as A


def test_cascade_rename_happy_path_updates_image_path_atomically(tmp_path):
    lib = tmp_path / "lib"; lib.mkdir()
    (lib / "old.pdf").write_bytes(b"%PDF-1.4\n")
    (lib / "old.ris").write_text("DO  - 10.1/x\n", encoding="utf-8")
    (lib / "old.fulltext.json").write_text(
        json.dumps({"doi": "10.1/x", "figures": [{"image_path": "old.fig1.png"}]}), encoding="utf-8")
    (lib / "old.fig1.png").write_bytes(b"img")

    A.cascade_rename(str(lib), "old.pdf", "new.pdf")

    assert {p.name for p in lib.iterdir()} == {
        "new.pdf", "new.ris", "new.fulltext.json", "new.fig1.png"}   # every companion renamed
    d = json.loads((lib / "new.fulltext.json").read_text(encoding="utf-8"))
    assert d["figures"][0]["image_path"] == "new.fig1.png"           # F3: image_path rewritten
    assert not list(lib.glob("*.tmp"))                               # F3: atomic, no tmp residue


def test_cascade_rename_rolls_back_on_failure(tmp_path, monkeypatch):
    """A2: force the 2nd rename (the .ris) to fail -> the already-done .pdf rename is undone."""
    lib = tmp_path / "lib"; lib.mkdir()
    (lib / "old.pdf").write_bytes(b"x")
    (lib / "old.ris").write_text("DO  - 10.1/x\n", encoding="utf-8")

    real_rename = os.rename
    calls = {"n": 0}
    def flaky(src, dst):
        calls["n"] += 1
        if calls["n"] == 2:                     # 1 = pdf (ok), 2 = ris (boom)
            raise OSError("disk full")
        return real_rename(src, dst)
    monkeypatch.setattr(A.os, "rename", flaky)

    with pytest.raises(OSError):
        A.cascade_rename(str(lib), "old.pdf", "new.pdf")

    # rolled back: originals restored, no new-* names, no partial state
    assert {p.name for p in lib.iterdir()} == {"old.pdf", "old.ris"}


def test_cascade_rename_rolls_back_full_companion_set(tmp_path, monkeypatch):
    """A2: with the FULL companion set present, force the LAST rename (a figure) to fail -> the
    multi-element reverse-replay restores every companion and (F3 barrier) never rewrote image_path.
    Exercises the multi-element rollback loop the single-file test above can't reach."""
    lib = tmp_path / "lib"; lib.mkdir()
    (lib / "old.pdf").write_bytes(b"x")
    (lib / "old.fulltext.json").write_text(
        json.dumps({"figures": [{"image_path": "old.fig1.png"}]}), encoding="utf-8")
    (lib / "old.ris").write_text("DO  - 10.1/x\n", encoding="utf-8")
    (lib / "old.fig1.png").write_bytes(b"i1")
    (lib / "old.fig2.png").write_bytes(b"i2")

    real_rename = os.rename
    def fail_on_last_figure(src, dst):
        if os.path.basename(src) == "old.fig2.png":   # the LAST rename in the cascade
            raise OSError("boom")
        return real_rename(src, dst)
    monkeypatch.setattr(A.os, "rename", fail_on_last_figure)

    with pytest.raises(OSError):
        A.cascade_rename(str(lib), "old.pdf", "new.pdf")

    # every companion restored, no new-* names remain
    assert {p.name for p in lib.iterdir()} == {
        "old.pdf", "old.fulltext.json", "old.ris", "old.fig1.png", "old.fig2.png"}
    # F3 barrier: the image_path rewrite is AFTER the rename barrier, so a rolled-back cascade
    # never leaves an edited sidecar
    d = json.loads((lib / "old.fulltext.json").read_text(encoding="utf-8"))
    assert d["figures"][0]["image_path"] == "old.fig1.png"


def _run_audit(monkeypatch, lib, report, doi_map, cr_map, execute=False):
    monkeypatch.setattr(A, "doi_from_sidecar", lambda p: "")
    monkeypatch.setattr(A, "extract_doi_from_pdf", lambda p: doi_map.get(os.path.basename(p), ""))
    monkeypatch.setattr(A, "crossref", lambda doi: cr_map.get(doi))
    monkeypatch.setattr(A.time, "sleep", lambda *_a, **_k: None)
    argv = ["audit_filenames.py", "--lib-dir", str(lib), "--report", str(report)]
    if execute:
        argv.append("--execute")
    monkeypatch.setattr(sys, "argv", argv)
    A.main()
    with open(report, encoding="utf-8") as fh:
        return {r["current"]: r["status"] for r in csv.DictReader(fh)}


def test_d3_dry_run_detects_two_files_proposing_same_name(tmp_path, monkeypatch):
    """D3: two files whose canonical name is identical -> the second is WOULD_COLLIDE even in
    dry-run. Pre-D3 the dry-run never added proposed names to seen_canonical, so it missed this."""
    lib = tmp_path / "lib"; lib.mkdir()
    # non-year-prefixed names so the cur-year!=cr-year safety filter doesn't intercept
    (lib / "paperA.pdf").write_bytes(b"%PDF-1.4\n")
    (lib / "paperB.pdf").write_bytes(b"%PDF-1.4\n")
    same_cr = {"lastname": "Smith", "year": "2001", "title": "Same Title"}
    statuses = _run_audit(
        monkeypatch, lib, lib / "r.csv",
        doi_map={"paperA.pdf": "10.1/a", "paperB.pdf": "10.1/b"},
        cr_map={"10.1/a": same_cr, "10.1/b": same_cr})
    # paperA (sorted first) proposes the canonical name; paperB then collides with it.
    assert statuses["paperA.pdf"] == "WOULD_RENAME"
    assert statuses["paperB.pdf"] == "WOULD_COLLIDE"


# ---------------------------------------------------------------- W3-D2: identity record + DEC-29 manifest
import ris_emit  # noqa: E402

RIS_TEXT = "TY  - JOUR\nTI  - A pipeline-written record\nDO  - 10.1234/x.1\nER  - \n"


@pytest.fixture
def real_state(tmp_path, monkeypatch):
    """The real litpipe.state on a temp file (tests/test_w2e1_manifest.py pattern)."""
    import litpipe.state as real
    monkeypatch.setattr(real, "DB_PATH", tmp_path / "state" / "litpipe_state.sqlite")
    monkeypatch.setattr(ris_emit, "STATE", real)
    return real


def test_identity_record_and_xml_move_with_the_pdf(tmp_path):
    lib = tmp_path / "lib"; lib.mkdir()
    (lib / "old.pdf").write_bytes(b"x")
    (lib / "old.identity.json").write_text(json.dumps({"pdf": "old.pdf", "identity": "OK", "queue_doi": "10.1234/x.1"}),
                                           encoding="utf-8")
    (lib / "old.xml").write_text("<article/>", encoding="utf-8")
    A.cascade_rename(str(lib), "old.pdf", "new.pdf")
    assert {p.name for p in lib.iterdir()} == {"new.pdf", "new.identity.json", "new.xml"}
    d = json.loads((lib / "new.identity.json").read_text(encoding="utf-8"))
    assert d["pdf"] == "new.pdf" and d["identity"] == "OK"


def test_rename_carries_the_manifest_through_the_real_state(tmp_path, real_state):
    """DEC-29: a renamed pipeline-written .ris stays pipeline-owned (refreshable); without the
    carry it would read as unrecorded, i.e. curated, forever."""
    lib = tmp_path / "lib"; lib.mkdir()
    (lib / "old.pdf").write_bytes(b"x")
    old_ris = lib / "old.ris"
    assert ris_emit.write_ris(str(old_ris), RIS_TEXT) is True
    old_key = ris_emit.manifest_key(old_ris)
    rec = real_state.kv_get("ris", old_key)
    assert rec
    A.cascade_rename(str(lib), "old.pdf", "new.pdf")
    new_ris = lib / "new.ris"
    assert ris_emit.ris_owner(str(new_ris)) == "pipeline"
    assert real_state.kv_get("ris", ris_emit.manifest_key(new_ris)) == rec
    assert real_state.kv_get("ris", old_key) is None                   # blanked: no stale record
    assert ris_emit.write_ris(str(new_ris), RIS_TEXT.replace("A pipeline", "The refreshed"), overwrite=True) is True


def test_a_curated_ris_stays_curated_after_a_rename(tmp_path, real_state):
    lib = tmp_path / "lib"; lib.mkdir()
    (lib / "old.pdf").write_bytes(b"x")
    (lib / "old.ris").write_text(RIS_TEXT, encoding="utf-8")          # no manifest record: curated
    A.cascade_rename(str(lib), "old.pdf", "new.pdf")
    assert ris_emit.ris_owner(str(lib / "new.ris")) == "unrecorded"


def test_rename_failure_leaves_files_and_manifest_as_they_were(tmp_path, real_state, monkeypatch):
    lib = tmp_path / "lib"; lib.mkdir()
    (lib / "old.pdf").write_bytes(b"x")
    ris_emit.write_ris(str(lib / "old.ris"), RIS_TEXT)
    old_key = ris_emit.manifest_key(lib / "old.ris")
    rec = real_state.kv_get("ris", old_key)
    real_rename = os.rename

    def flaky(src, dst):
        if os.path.basename(src) == "old.ris":
            raise OSError("disk full")
        return real_rename(src, dst)
    monkeypatch.setattr(A.os, "rename", flaky)
    with pytest.raises(OSError):
        A.cascade_rename(str(lib), "old.pdf", "new.pdf")
    assert {p.name for p in lib.iterdir()} == {"old.pdf", "old.ris"}
    assert real_state.kv_get("ris", old_key) == rec and ris_emit.ris_owner(str(lib / "old.ris")) == "pipeline"


def test_a_failed_manifest_write_rolls_back_files_and_restores_both_keys(tmp_path, real_state, monkeypatch):
    """The carry is part of the unit: if the state cannot take the change, the renames are undone
    and the old record is back in place (the library and the manifest never disagree)."""
    lib = tmp_path / "lib"; lib.mkdir()
    (lib / "old.pdf").write_bytes(b"x")
    ris_emit.write_ris(str(lib / "old.ris"), RIS_TEXT)
    old_key = ris_emit.manifest_key(lib / "old.ris")
    rec = real_state.kv_get("ris", old_key)
    real_set = ris_emit._kv_set
    fail = {"on": True}

    def failing(ns, key, value, ttl_s=None):
        if value is None and fail["on"]:          # blanking the old key fails once
            fail["on"] = False
            return False
        return real_set(ns, key, value, ttl_s=ttl_s)
    monkeypatch.setattr(ris_emit, "_kv_set", failing)
    with pytest.raises(OSError):
        A.cascade_rename(str(lib), "old.pdf", "new.pdf")
    assert {p.name for p in lib.iterdir()} == {"old.pdf", "old.ris"}
    assert real_state.kv_get("ris", old_key) == rec
    assert not real_state.kv_get("ris", ris_emit.manifest_key(lib / "new.ris"))
    assert ris_emit.ris_owner(str(lib / "old.ris")) == "pipeline"


def test_a_case_only_rename_moves_and_keeps_the_record(tmp_path, real_state):
    """`2019_SMITH_X` to `2019_Smith_X`: on a case-insensitive disk the target "exists" (it is the
    same file), which must not stop the cascade; the record follows the new on-disk case."""
    lib = tmp_path / "lib"; lib.mkdir()
    (lib / "2019_SMITH_X.pdf").write_bytes(b"x")
    ris_emit.write_ris(str(lib / "2019_SMITH_X.ris"), RIS_TEXT)
    rec = real_state.kv_get("ris", ris_emit.manifest_key(lib / "2019_SMITH_X.ris"))
    A.cascade_rename(str(lib), "2019_SMITH_X.pdf", "2019_Smith_X.pdf")
    assert sorted(p.name for p in lib.iterdir()) == ["2019_Smith_X.pdf", "2019_Smith_X.ris"]
    assert real_state.kv_get("ris", ris_emit.manifest_key(lib / "2019_Smith_X.ris")) == rec
    assert ris_emit.ris_owner(str(lib / "2019_Smith_X.ris")) == "pipeline"


def test_an_existing_target_stops_the_cascade_before_anything_moves(tmp_path):
    """os.rename replaces an existing file silently on POSIX: every target is checked first."""
    lib = tmp_path / "lib"; lib.mkdir()
    (lib / "old.pdf").write_bytes(b"x")
    (lib / "old.ris").write_text("DO  - 10.1234/x.1\n", encoding="utf-8")
    (lib / "new.ris").write_text("an orphan that belongs to someone else\n", encoding="utf-8")
    with pytest.raises(FileExistsError):
        A.cascade_rename(str(lib), "old.pdf", "new.pdf")
    assert {p.name for p in lib.iterdir()} == {"old.pdf", "old.ris", "new.ris"}
    assert (lib / "new.ris").read_text(encoding="utf-8").startswith("an orphan")


def test_execute_end_to_end_keeps_a_pipeline_ris_refreshable(tmp_path, real_state, monkeypatch):
    """audit --execute through the real state on a temp file: the renamed .ris is still the
    pipeline's, so a later refresh (write_ris overwrite=True) replaces it."""
    import fitz
    lib = tmp_path / "lib"; lib.mkdir()
    fn = "2020_Unknown_HeatAcclimation.pdf"
    doc = fitz.open(); doc.new_page(); (lib / fn).write_bytes(doc.tobytes()); doc.close()
    ris_emit.write_ris(str(lib / (fn[:-4] + ".ris")), RIS_TEXT)
    (lib / (fn[:-4] + ".identity.json")).write_text(json.dumps({"pdf": fn, "identity": "OK"}), encoding="utf-8")
    monkeypatch.setattr(A, "crossref", lambda doi: {"title": "Heat acclimation in athletes and soldiers",
                                                    "year": "2020", "lastname": "Periard", "authors": []})
    report = tmp_path / "r.csv"
    assert A.main(["--lib-dir", str(lib), "--execute", "--report", str(report)]) == 0
    new = "2020_Periard_HeatAcclimationAthletesSoldiers"
    assert sorted(p.name for p in lib.iterdir()) == [new + ".identity.json", new + ".pdf", new + ".ris"]
    assert json.loads((lib / (new + ".identity.json")).read_text(encoding="utf-8"))["pdf"] == new + ".pdf"
    assert ris_emit.ris_owner(str(lib / (new + ".ris"))) == "pipeline"
    assert ris_emit.write_ris(str(lib / (new + ".ris")), RIS_TEXT + "\n", overwrite=True) is True
