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
