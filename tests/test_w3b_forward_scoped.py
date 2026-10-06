"""Scoped mode of the forward walk (dispatch W3-A step 3, DEC-30): DOI lists as seeds, the consumer's
seed filter, the first list claiming a DOI, `_<scope>_forward_citations.csv` and
`_<scope>_descendants.csv`, the publish rule, the overwrite rule, the ranking against the consumer's
gate (`(-n_seeds, -cited_by, -year)` before its topic filters and quotas), OA first, held DOIs, and
the REAL index_portfolio ingesting a scoped CSV with an OpenAlex row into scoped_candidates.
Offline: the World stub of tests/test_walk_forward.py."""
import csv
import json
import shutil
from collections import defaultdict
from pathlib import Path

import duckdb
import pytest

import forward_citations as fc
import index_portfolio as I
import lit_util
from litpipe import openalex
from tests.test_walk_forward import make_lib, pid_of, read_csv, s2_citer, sha, world  # noqa: F401  (fixture)

FIX = Path(__file__).parent / "fixtures" / "W3-A"
CONSUMER_HEADER = ["seed_doi", "seed_label", "seed_chapter", "citing_paper_id", "citing_doi", "citing_title",
                   "citing_year", "citing_authors", "citing_venue", "citing_cited_by", "citing_abstract"]


def seed_list(path, rows, header=("doi", "title", "authors", "year")):
    path = Path(path)
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write("# REVIEW BEFORE SWEEP: a draft header comment\n")
        w = csv.DictWriter(f, fieldnames=list(header), lineterminator="\n")
        w.writeheader()
        for r in rows:
            w.writerow(r)
    return path


def header_of(path):
    with open(path, encoding="utf-8", newline="") as f:
        return next(csv.reader(f))


# ================================================================ flags and seed lists
def test_scoped_flags_go_together_and_the_scope_name_is_the_index_pattern(world, tmp_path):
    lib = make_lib(tmp_path, [])
    lst = seed_list(tmp_path / "ch01.csv", [{"doi": "10.5555/sc.1"}])
    assert fc.run(lib_dir=str(lib), seeds_from=[str(lst)])["exit_code"] == 1
    assert fc.run(lib_dir=str(lib), scope="teach")["exit_code"] == 1
    assert fc.run(seeds_from=[str(lst)], scope="teach")["exit_code"] == 1                 # no output library
    for bad in ("Teach", "_teach", "te ach", "teach.x", ""):
        assert fc.run(lib_dir=str(lib), seeds_from=[str(lst)], scope=bad or None)["exit_code"] == 1
    assert fc.run(lib_dir=str(lib), seeds_from=[str(lst)], scope="teach", report=str(tmp_path / "r.csv"))["exit_code"] == 1
    for good in ("teach", "ch15", "body-comp_2", "0a"):
        assert fc.SCOPE_RE.match(good) and I.SCOPED_FORWARD_RE.match(f"_{good}_forward_citations.csv")
    assert world.sent == []


def test_seed_lists_use_the_consumer_filter_and_the_first_list_claims_a_doi(tmp_path):
    a = seed_list(tmp_path / "lit_pull_queue.ch12_refs.draft.csv", [
        {"doi": "https://doi.org/10.5555/SC.1", "authors": "Alpha, A; Beta, B", "year": "2001", "status": "HIGH",
         "bookish": "", "n_cites": "2", "chapter": "Ch12"},
        {"doi": "10.5555/sc.2", "title": "A book guessed a DOI", "status": "HIGH", "bookish": "true", "n_cites": "9"},
        {"doi": "10.5555/sc.3", "status": "LOW", "n_cites": "1"},
        {"doi": "not a doi", "status": "HIGH"},
        {"doi": "10.1145/nnnnnnn.nnnnnnn", "status": "HIGH"},
        {"doi": "10.5555/sc.4", "title": "No author or year here", "status": "HIGH", "n_cites": "1"},
    ], header=("doi", "title", "authors", "year", "status", "bookish", "n_cites", "chapter"))
    drive_spec = f"{tmp_path / 'ch13.csv'}::Ch13"                                   # C:\...::Ch13 on Windows
    seed_list(tmp_path / "ch13.csv", [{"doi": "10.5555/sc.1", "authors": "Other", "year": "1999", "n_cites": "7"},
                                      {"doi": "10.5555/sc.5", "authors": "Gamma, G", "year": "2010"}],
              header=("doi", "title", "authors", "year", "n_cites"))
    seeds, st = fc.read_seed_lists([str(a), drive_spec])
    by = {s.doi: s for s in seeds}
    assert list(by) == ["10.5555/sc.1", "10.5555/sc.4", "10.5555/sc.5"]
    assert (by["10.5555/sc.1"].label, by["10.5555/sc.1"].chapter, by["10.5555/sc.1"].n_cites) == ("Alpha, A 2001", "Ch12", 7)
    assert by["10.5555/sc.4"].label == "No author or year here"
    assert (by["10.5555/sc.4"].chapter, by["10.5555/sc.5"].chapter) == ("lit_pull_queue.ch12_refs.draft", "Ch13")
    assert st["dropped"] == {"bookish": 1, "status_LOW": 1, "no_valid_doi": 1, "placeholder": 1, "duplicate": 1}
    assert fc._split_spec(r"C:\data\x.csv") == (r"C:\data\x.csv", "")
    assert fc._split_spec(r"C:\data\x.csv::Ch 9") == (r"C:\data\x.csv", "Ch 9")
    with pytest.raises(fc.SeedListError):
        fc.read_seed_lists([str(tmp_path / "missing.csv")])


# ================================================================ outputs and the publish rule
def scoped_world(world, tmp_path, n=3, counts=(3, 2, 4)):
    ds = [f"10.5555/sc.{i}" for i in range(n)]
    for d, c in zip(ds, counts):
        world.add(d, c)
    lib = make_lib(tmp_path, [])
    lst = seed_list(tmp_path / "ch09.csv", [{"doi": d, "authors": f"Author{i}", "year": "2000"} for i, d in enumerate(ds)])
    return ds, lib, lst


def test_scoped_outputs_never_touch_the_library_report(world, tmp_path):
    ds, lib, lst = scoped_world(world, tmp_path)
    lib_report = lib / "_forward_citations.csv"
    lib_report.write_text("seed_pdf,seed_doi\nx.pdf,10.5555/kept\n", encoding="utf-8")
    before = sha(lib_report)
    res = fc.run(lib_dir=str(lib), seeds_from=[f"{lst}::Ch09"], scope="teach")
    out, desc = lib / "_teach_forward_citations.csv", lib / "_teach_descendants.csv"
    assert res["exit_code"] == 0 and res["mode"] == "scoped" and out.exists() and desc.exists()
    assert sha(lib_report) == before and not (lib / "_forward_citations_unique_dois.csv").exists()
    assert header_of(out) == CONSUMER_HEADER + ["source", "citing_oa", "seed_n_cites"]
    assert header_of(desc)[-1] == "seed_n_cites" and header_of(desc)[:3] == ["rank", "doi", "title"]
    rows = read_csv(out)
    assert len(rows) == 9 and {r["seed_chapter"] for r in rows} == {"Ch09"} and rows[0]["seed_label"] == "Author0 2000"
    assert I.SCOPED_FORWARD_RE.match(out.name) and not I.SCOPED_FORWARD_RE.match(desc.name)


def test_a_consumer_written_scoped_csv_is_never_replaced_without_force(world, tmp_path, capsys):
    ds, lib, lst = scoped_world(world, tmp_path)
    out = lib / "_resp_forward_citations.csv"
    shutil.copyfile(FIX / "consumer_scoped_header.csv", out)
    before = sha(out)
    res = fc.run(lib_dir=str(lib), seeds_from=[str(lst)], scope="resp")
    assert res["exit_code"] == 1 and "--force" in capsys.readouterr().err
    assert sha(out) == before and world.sent == [] and not (lib / "_resp_descendants.csv").exists()
    res = fc.run(lib_dir=str(lib), seeds_from=[str(lst)], scope="resp", force=True)
    assert res["exit_code"] == 0 and "source" in header_of(out)


def test_scoped_outputs_follow_the_publish_rule(world, tmp_path):
    ds, lib, lst = scoped_world(world, tmp_path)
    assert fc.run(lib_dir=str(lib), seeds_from=[str(lst)], scope="teach")["exit_code"] == 0
    out = lib / "_teach_forward_citations.csv"
    before, desc_before = sha(out), sha(lib / "_teach_descendants.csv")
    world.papers[ds[1]]["count"] = 0                     # a seed with citers now has none
    res = fc.run(lib_dir=str(lib), seeds_from=[str(lst)], scope="teach")
    assert res["exit_code"] == 2 and "fewer than the published 3" in res["reasons"][0]
    assert sha(out) == before and sha(lib / "_teach_descendants.csv") == desc_before
    assert fc.degraded_path(out).name == "_teach_forward_citations.degraded.csv" and fc.degraded_path(out).exists()
    assert not I.SCOPED_FORWARD_RE.match(fc.degraded_path(out).name)
    last = res
    assert last["published"] is False


def test_duplicate_seeds_take_the_largest_n_cites_in_both_outputs(world, tmp_path):
    world.add("10.5555/sc.dup1", 2).add("10.5555/sc.one1", 2)
    lib = make_lib(tmp_path, [])
    a = seed_list(tmp_path / "a.csv", [{"doi": "10.5555/sc.dup1", "n_cites": "2", "chapter": "Ch01"},
                                        {"doi": "10.5555/sc.one1", "n_cites": "1", "chapter": "Ch01"}],
                  header=("doi", "n_cites", "chapter"))
    b = seed_list(tmp_path / "b.csv", [{"doi": "10.5555/sc.dup1", "n_cites": "5"}], header=("doi", "n_cites"))
    assert fc.run(lib_dir=str(lib), seeds_from=[str(a), f"{b}::Ch02"], scope="teach")["exit_code"] == 0
    rows = read_csv(lib / "_teach_forward_citations.csv")
    assert {(r["seed_doi"], r["seed_chapter"], r["seed_n_cites"]) for r in rows} == {
        ("10.5555/sc.dup1", "Ch01", "5"), ("10.5555/sc.one1", "Ch01", "1")}
    desc = read_csv(lib / "_teach_descendants.csv")
    assert {d["seed_n_cites"] for d in desc} == {"5", "1"}


# ================================================================ the ranking
def gate_order(rows, year_min=None):
    """The consumer gate's ranking before its topic filters, exclusions, held/staged drops and quotas
    (its pool build): one record per normalised citing DOI, metadata from the first
    row, n_seeds = distinct `seed_label or seed_doi`, sorted by (-n_seeds, -cited_by, -year)."""
    def norm(d):
        d = (d or "").strip().lower()
        for p in ("https://doi.org/", "http://doi.org/", "https://dx.doi.org/", "http://dx.doi.org/", "doi:"):
            if d.startswith(p):
                d = d[len(p):].strip()
        return d
    by_doi, seeds_of = {}, defaultdict(set)
    for r in rows:
        doi = norm(r.get("citing_doi", ""))
        if not doi:
            continue
        try:
            year = int(r.get("citing_year", ""))
        except (TypeError, ValueError):
            year = 0
        if year_min is not None and year < year_min:
            continue
        seeds_of[doi].add(r.get("seed_label", "") or r.get("seed_doi", ""))
        if doi not in by_doi:
            by_doi[doi] = {"doi": doi, "year": year, "cited_by": int(r.get("citing_cited_by") or 0)}
    kept = list(by_doi.values())
    for rec in kept:
        rec["n_seeds"] = len(seeds_of[rec["doi"]])
    kept.sort(key=lambda r: (-r["n_seeds"], -r["cited_by"], -r["year"]))
    return [r["doi"] for r in kept]


def test_with_oa_unknown_the_scoped_order_is_the_consumer_gates_order():
    rows = read_csv(FIX / "teaching_resp_forward_citations.csv")
    assert not any(r.get("citing_oa") for r in rows)                                  # OA unknown on every row
    ranked, stats = fc.rank_descendants(rows)
    mine = [r["doi"] for r in ranked]
    assert mine == gate_order(rows) and len(mine) == 851
    assert max(stats["tiers"]) == 8 and sum(stats["tiers"].values()) == 851
    years = {r["doi"]: r["year"] for r in ranked}
    assert [d for d in mine if (years[d] or 0) >= 2015] == gate_order(rows, year_min=2015)   # its era filter too
    assert [r["rank"] for r in ranked] == list(range(1, 852))


def test_oa_first_within_a_tier():
    rows = read_csv(FIX / "teaching_oa_forward_citations.csv")
    ranked, stats = fc.rank_descendants(rows)
    assert [r["doi"].rsplit(".", 1)[1] for r in ranked] == ["c2", "c6", "c3", "c1", "c4", "c5"]
    assert gate_order(rows) != [r["doi"] for r in ranked]
    by = {r["doi"].rsplit(".", 1)[1]: r for r in ranked}
    assert (by["c6"]["oa"], by["c3"]["oa"], by["c1"]["oa"]) == ("true", "", "false")
    assert (by["c1"]["seed_n_cites"], by["c6"]["seed_n_cites"], by["c5"]["seed_n_cites"]) == (4, 3, "")
    assert stats["no_doi"] == 1 and stats["tiers"] == {2: 4, 1: 2}


def test_alias_seeds_count_once_in_n_seeds(world, tmp_path):
    shared = pid_of("one-paper")
    pre, pub, other = "10.31234/osf.io/xpre12", "10.5555/sc.pub.2024", "10.5555/sc.other9"
    world.add(pre, 2, pid=shared).add(pub, 2, pid=shared)
    world.add(other, 1, rows=[s2_citer(pre, 0)])          # the other seed shares one citer with the pair
    lib = make_lib(tmp_path, [])
    lst = seed_list(tmp_path / "s.csv", [{"doi": pre}, {"doi": pub}, {"doi": other}], header=("doi",))
    res = fc.run(lib_dir=str(lib), seeds_from=[str(lst)], scope="teach")
    assert res["exit_code"] == 0 and res["aliases"] == 2 and len(world.nested_calls()) == 1
    assert len(world.nested_calls()[0]["json"]["ids"]) == 2                           # one id per S2 paper
    desc = {d["doi"]: d for d in read_csv(lib / "_teach_descendants.csv")}
    shared_citer = s2_citer(pre, 0)["externalIds"]["DOI"]
    assert desc[shared_citer]["n_seeds"] == "2"          # the alias pair is one seed, the other seed the second
    assert len(desc[shared_citer]["seed_dois"].split(";")) == 3
    assert desc[s2_citer(pre, 1)["externalIds"]["DOI"]]["n_seeds"] == "1"


def test_held_dois_are_dropped_and_a_flagged_file_is_not_a_holding(world, tmp_path, monkeypatch):
    root = tmp_path / "root"
    seed = "10.5555/sc.held9"
    c = [s2_citer(seed, i)["externalIds"]["DOI"] for i in range(3)]
    held_lib = root / "research_b" / "literature"
    held_lib.mkdir(parents=True)
    (held_lib / "2020_Held.pdf").write_bytes(b"%PDF-1.4 stub")
    (held_lib / "2020_Held.ris").write_text(f"TY  - JOUR\nDO  - {c[0].upper()}\nER  - \n", encoding="utf-8")
    (held_lib / "2020_Flag.fulltext.json").write_text(json.dumps(
        {"doi": c[1], "text": "wrong paper", "identity": "FLAG", "has_pdf": False}), encoding="utf-8")
    world.env.write_config(projects={"research_b": {"lib_dir": "literature"}, "teaching_a": {"lib_dir": "literature"}})
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    world.add(seed, 3)
    lib = make_lib(root / "teaching_a", [])
    lst = seed_list(tmp_path / "s.csv", [{"doi": seed}], header=("doi",))
    res = fc.run(lib_dir=str(lib), seeds_from=[str(lst)], scope="teach")
    assert res["exit_code"] == 0 and res["descendants"]["held_dropped"] == 1
    desc = {d["doi"] for d in read_csv(lib / "_teach_descendants.csv")}
    assert desc == {c[1], c[2]}                                                        # c0 held; the FLAG is not
    assert len(read_csv(lib / "_teach_forward_citations.csv")) == 3                    # the forward CSV keeps all
    assert (world.env.cfg_path.parent / "state" / "holdings_cache.json").exists()      # the temp state_dir


# ================================================================ the REAL index on a temp DB
def test_the_real_index_ingests_a_scoped_csv_with_an_openalex_row(world, tmp_path, monkeypatch):
    monkeypatch.setenv(openalex.KEY_ENV, "oa-test-key-not-real")
    root = tmp_path / "root"
    lib = make_lib(root / "teaching_a", ["10.5555/sc.held.1"])
    world.add("10.5555/sc.oa.1", 1, oa=2).add("10.5555/sc.held.1", 1)
    lst = seed_list(tmp_path / "ch09.csv", [{"doi": "10.5555/sc.oa.1", "authors": "Seedwright, S", "year": "1994",
                                             "chapter": "Ch09"}], header=("doi", "authors", "year", "chapter"))
    res = fc.run(lib_dir=str(lib), seeds_from=[str(lst)], scope="teach", source="openalex")
    assert res["exit_code"] == 0 and res["openalex_walked"] == 1
    reg = {"state_dir": str(tmp_path / "state"), "projects": {"teaching_a": {"lib_dir": "literature"}}}
    cfg = tmp_path / "projects.json"
    cfg.write_text(json.dumps(reg), encoding="utf-8")
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    monkeypatch.setattr(I, "CONFIG_PATH", cfg)
    db = tmp_path / "refs" / "portfolio.duckdb"
    assert I.main(["--db", str(db), "--project", "teaching_a"]) == 0
    con = duckdb.connect(str(db), read_only=True)
    try:
        cols = [r[0] for r in con.execute("DESCRIBE scoped_candidates").fetchall()]
        got = [dict(zip(cols, r)) for r in con.execute("SELECT * FROM scoped_candidates ORDER BY doi").fetchall()]
        n_core = con.execute("SELECT COUNT(*) FROM candidates").fetchone()[0]
    finally:
        con.close()
    assert len(got) == 2 and n_core == 0
    row = got[0]
    assert row["doi"] == "10.7777/oa.c.oa.1.0" and row["scope"] == "teach" and row["project"] == "teaching_a"
    assert (row["source_type"], row["source_seed_doi"], row["seed_label"], row["seed_chapter"]) == (
        "forward", "10.5555/sc.oa.1", "Seedwright, S 1994", "Ch09")
    assert (row["title"], row["year"], row["authors"], row["venue"]) == ("Open citer 0", 2020, "Bea Open; Cy Access", "J Open")
    assert (row["citing_cited_by"], got[1]["citing_cited_by"]) == (5, 6)               # cited_by_count, not 0
    for r in got:
        for k, v in r.items():
            assert v not in (None, "", 0), k                                           # every column filled
    out = read_csv(lib / "_teach_forward_citations.csv")
    assert {r["source"] for r in out} == {"openalex"} and {r["citing_oa"] for r in out} == {"true", "false"}
