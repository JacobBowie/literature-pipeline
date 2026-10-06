"""W3b seam lock (dispatcher, 2026-10-06): the scoped chain with the real modules end to end.

W3-A's scoped walk writes `_<scope>_forward_citations.csv`; index_portfolio ingests it into
scoped_candidates; the W3-C2 seeder's `--scope` draft reads it back. Builders in separate trees
tested each half against the written contract; this drives both halves together.
"""
import csv
import json

import forward_citations as fc
import index_portfolio as I
import lit_util
import seed_queue_from_top_candidates as seeder
from tests.test_w3b_forward_scoped import seed_list
from tests.test_walk_forward import make_lib, s2_citer, world  # noqa: F401  (fixture)

SEED_A, SEED_B = "10.5555/sa0001", "10.5555/sb0002"   # a digit-free suffix is a placeholder


def read_draft(path):
    lines = [ln for ln in path.read_text(encoding="utf-8").splitlines() if not ln.startswith("#")]
    return list(csv.DictReader(lines))


def test_a_scoped_harvest_reaches_the_seeders_scope_draft(world, tmp_path, monkeypatch, capsys):
    rows_a = [s2_citer(SEED_A, 0), s2_citer(SEED_A, 1)]
    rows_b = [s2_citer(SEED_A, 0), s2_citer(SEED_B, 5)]            # one citer shared by both seeds
    world.add(SEED_A, len(rows_a), rows=rows_a).add(SEED_B, len(rows_b), rows=rows_b)
    root = tmp_path / "root"
    lib = make_lib(root / "teaching_a", [])
    lst = seed_list(tmp_path / "ch09.csv", [{"doi": SEED_A, "authors": "Seedwright, S", "year": "1994"},
                                            {"doi": SEED_B, "authors": "Other, O", "year": "1995"}])
    walked = fc.run(lib_dir=str(lib), seeds_from=[f"{lst}::Ch09"], scope="teach")
    assert walked["exit_code"] == 0
    with (lib / "_teach_forward_citations.csv").open(encoding="utf-8", newline="") as fh:
        scoped = {r["citing_doi"] for r in csv.DictReader(fh) if r["citing_doi"]}
    shared = "10.9999/sa0001.c0"
    assert shared in scoped and len(scoped) == 3

    cfg = tmp_path / "projects.json"
    cfg.write_text(json.dumps({"state_dir": str(tmp_path / "state"),
                               "projects": {"teaching_a": {"lib_dir": "literature"}}}), encoding="utf-8")
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    monkeypatch.setattr(I, "CONFIG_PATH", cfg)
    monkeypatch.setattr(seeder, "CONFIG_PATH", cfg)
    db = tmp_path / "refs" / "portfolio.duckdb"
    assert I.main(["--db", str(db), "--project", "teaching_a"]) == 0

    out = tmp_path / "draft.csv"
    capsys.readouterr()
    rc = seeder.main(["--project", "teaching_a", "--scope", "teach", "--db", str(db), "--output", str(out),
                      "--year-min", "0"])
    assert rc == 0, capsys.readouterr().out
    draft = read_draft(out)
    assert {r["doi"] for r in draft} == scoped                      # every scoped DOI, nothing else
    assert draft[0]["doi"] == shared                                # cited by both seeds: first
    assert all("scope=teach" in r["notes"] for r in draft)
    assert all(r["destination"] == "literature/" for r in draft)
