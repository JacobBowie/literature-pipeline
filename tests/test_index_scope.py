"""W3-C1 / DEC-30: scoped harvests live in scoped_candidates + scoped_cites, keyed by scope.

A consumer's per-chapter forward harvests (<lib>/_<scope>_forward_citations.csv, header
seed_doi, seed_label, seed_chapter, citing_paper_id, citing_doi, ...; no seed_pdf) are ingested by
(project, scope). Two scopes stay apart, a scope whose CSV is gone loses its rows, and scoped
ingest adds no row to candidates, cites or paper_metadata (so top_candidates and the public build
never see it). `_forward_citations_unique_dois.csv`, `*.degraded.csv` and `*.partial.jsonl` are
never ingested.
"""
import json

import duckdb
import pytest

import index_portfolio as I
import lit_util

SCOPED_HDR = ("seed_doi,seed_label,seed_chapter,citing_paper_id,citing_doi,citing_title,citing_year,"
              "citing_authors,citing_venue,citing_cited_by,citing_abstract\n")
FWD_HDR = "seed_pdf,seed_doi,citing_paper_id,citing_doi,citing_title,citing_year,citing_authors,citing_venue,citing_cited_by\n"


def scoped_csv(rows):
    return SCOPED_HDR + "".join(
        f"{s},Label {s[-4:]},{ch},pid{i},{c},Title {c[-4:]},2020,Au,Ve,{i},\"multi\nline abstract\"\n"
        for i, (s, ch, c) in enumerate(rows))


def make_lib(root, project, seeds):
    lib = root / project / "literature"
    lib.mkdir(parents=True, exist_ok=True)
    for i, d in enumerate(seeds):
        (lib / f"2020_Seed{i}.pdf").write_bytes(b"%PDF-1.4 stub")
        (lib / f"2020_Seed{i}.ris").write_text(f"TY  - JOUR\nPY  - 2020\nDO  - {d}\nER  - \n", encoding="utf-8")
    return lib


@pytest.fixture
def env(tmp_path, monkeypatch):
    root = tmp_path / "root"
    for proj in ("teaching_a", "teaching_b"):      # both libraries exist (a missing one is a skip)
        (root / proj / "literature").mkdir(parents=True)
    reg = {"state_dir": str(tmp_path / "state"),
           "projects": {"teaching_a": {"lib_dir": "literature"}, "teaching_b": {"lib_dir": "literature"}}}
    cfg = tmp_path / "projects.json"
    cfg.write_text(json.dumps(reg), encoding="utf-8")
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    monkeypatch.setattr(I, "CONFIG_PATH", cfg)
    db = tmp_path / "refs" / "portfolio.duckdb"

    def index(*extra):
        return I.main(["--db", str(db), *extra])

    def q(sql, params=()):
        con = duckdb.connect(str(db), read_only=True)
        try:
            return con.execute(sql, list(params)).fetchall()
        finally:
            con.close()
    return root, index, q


def core_tables(q):
    return {
        "candidates": sorted(q("SELECT doi, source_type, source_seed_doi, source_project, citing_cited_by FROM candidates")),
        "cites": sorted(q("SELECT * FROM cites")),
        "paper_metadata": sorted(q("SELECT doi, year, lastname, title, venue, authors, abstract FROM paper_metadata")),
        "top_candidates": sorted(q("SELECT doi, n_seeds_pointing, max_cited_by, via_projects FROM top_candidates")),
    }


@pytest.mark.parametrize("name,scope", [
    ("_resp_forward_citations.csv", "resp"),
    ("_ch15_forward_citations.csv", "ch15"),
    ("_neuromuscular_forward_citations.csv", "neuromuscular"),
    ("_body-comp_2_forward_citations.csv", "body-comp_2"),
])
def test_scope_pattern_matches_consumer_harvests(name, scope):
    m = I.SCOPED_FORWARD_RE.match(name)
    assert m and m.group(1) == scope


@pytest.mark.parametrize("name", [
    "_forward_citations.csv", "_forward_citations_unique_dois.csv", "_forward_citations.degraded.csv",
    "_resp_forward_citations.degraded.csv", "_resp_forward_citations.partial.jsonl",
    "_resp_forward_citations_unique_dois.csv", "_Resp_forward_citations.csv", "resp_forward_citations.csv",
    "__forward_citations.csv", "_resp_forward_seeds.json", "_reverse_citations_parsed.csv",
])
def test_scope_pattern_rejects_everything_else(name):
    assert I.SCOPED_FORWARD_RE.match(name) is None


def test_two_scopes_stay_apart_and_core_tables_are_untouched(env):
    root, index, q = env
    seeds = ["10.5555/seed.0001", "10.5555/seed.0002"]
    lib = make_lib(root, "teaching_a", seeds)
    (lib / "_forward_citations.csv").write_text(
        FWD_HDR + "2020_Seed0.pdf,10.5555/seed.0001,p1,10.6666/core.0001,Core,2019,A,V,5\n", encoding="utf-8")
    assert index() == 0
    before = core_tables(q)

    (lib / "_resp_forward_citations.csv").write_text(scoped_csv([
        ("10.5555/seed.0001", "Ch09", "10.7777/shared.0001"),
        ("10.5555/seed.0001", "Ch09", "10.7777/resp.0002"),
        ("10.9999/external.0003", "Ch09", "10.7777/resp.0003"),      # a seed this library does not hold
    ]), encoding="utf-8")
    (lib / "_ch15_forward_citations.csv").write_text(scoped_csv([
        ("10.5555/seed.0002", "Ch15", "10.7777/shared.0001"),
        ("10.5555/seed.0002", "Ch15", "10.7777/ch15.0002"),
    ]), encoding="utf-8")
    # never ingested: a degraded walk result, its journal, the unique-DOI side output
    (lib / "_resp_forward_citations.degraded.csv").write_text(scoped_csv([
        ("10.5555/seed.0001", "Ch09", "10.7777/degraded.0001")]), encoding="utf-8")
    (lib / "_forward_citations.degraded.csv").write_text(
        FWD_HDR + "2020_Seed0.pdf,10.5555/seed.0001,p9,10.6666/degraded.0002,D,2019,A,V,5\n", encoding="utf-8")
    (lib / "_resp_forward_citations.partial.jsonl").write_text('{"seed":"10.5555/seed.0001"}\n', encoding="utf-8")
    (lib / "_forward_citations_unique_dois.csv").write_text("doi\n10.6666/unique.0003\n", encoding="utf-8")
    assert index() == 0

    rows = q("SELECT scope, doi, source_seed_doi, seed_label, seed_chapter, year FROM scoped_candidates "
             "WHERE project = 'teaching_a' ORDER BY scope, doi")
    assert [(r[0], r[1], r[2]) for r in rows] == [
        ("ch15", "10.7777/ch15.0002", "10.5555/seed.0002"),
        ("ch15", "10.7777/shared.0001", "10.5555/seed.0002"),
        ("resp", "10.7777/resp.0002", "10.5555/seed.0001"),
        ("resp", "10.7777/resp.0003", "10.9999/external.0003"),
        ("resp", "10.7777/shared.0001", "10.5555/seed.0001"),
    ]
    assert rows[0][3:] == ("Label 0002", "Ch15", 2020)
    assert sorted(q("SELECT scope, citing_doi, cited_doi FROM scoped_cites")) == sorted(
        (r[0], r[1], r[2]) for r in rows)
    assert core_tables(q) == before                                  # no row added anywhere else
    every = " ".join(str(r) for t in ("candidates", "cites", "paper_metadata", "scoped_candidates", "scoped_cites")
                     for r in q(f"SELECT * FROM {t}"))
    for never in ("degraded.0001", "degraded.0002", "unique.0003"):
        assert never not in every

    # a scope whose CSV is gone loses its rows; the other scope is untouched
    (lib / "_ch15_forward_citations.csv").unlink()
    assert index() == 0
    assert q("SELECT DISTINCT scope FROM scoped_candidates") == [("resp",)]
    assert q("SELECT DISTINCT scope FROM scoped_cites") == [("resp",)]
    assert q("SELECT COUNT(*) FROM scoped_candidates")[0][0] == 3
    assert core_tables(q) == before

    # a shrunk scope is rewritten, not appended to
    (lib / "_resp_forward_citations.csv").write_text(scoped_csv([
        ("10.5555/seed.0001", "Ch09", "10.7777/resp.0002")]), encoding="utf-8")
    assert index("--project", "teaching_a") == 0
    assert q("SELECT doi FROM scoped_candidates") == [("10.7777/resp.0002",)]


def test_same_scope_name_in_two_projects_stays_apart(env):
    root, index, q = env
    for proj, seed, cited in (("teaching_a", "10.5555/seed.0001", "10.7777/a.0001"),
                              ("teaching_b", "10.5555/seed.0002", "10.7777/b.0001")):
        lib = make_lib(root, proj, [seed])
        (lib / "_resp_forward_citations.csv").write_text(scoped_csv([(seed, "Ch09", cited)]), encoding="utf-8")
    assert index() == 0
    assert q("SELECT project, scope, doi FROM scoped_candidates ORDER BY project") == [
        ("teaching_a", "resp", "10.7777/a.0001"), ("teaching_b", "resp", "10.7777/b.0001")]
    (root / "teaching_b" / "literature" / "_resp_forward_citations.csv").unlink()
    assert index("--project", "teaching_b") == 0
    assert q("SELECT project, scope, doi FROM scoped_candidates") == [("teaching_a", "resp", "10.7777/a.0001")]


def test_scoped_dois_are_normalised_and_placeholders_dropped(env):
    root, index, q = env
    lib = make_lib(root, "teaching_a", ["10.5555/seed.0001"])
    (lib / "_resp_forward_citations.csv").write_text(scoped_csv([
        ("https://doi.org/10.5555/SEED.0001", "Ch09", "10.7777/UPPER.0001"),
        ("10.5555/seed.0001", "Ch09", "10.1145/nnnnnnn.nnnnnnn"),     # ACM template placeholder
        ("10.5555/seed.0001", "Ch09", "10.1089/ther.2017.29031.mkb"),  # a real letter-tail DOI, kept whole
    ]), encoding="utf-8")
    assert index() == 0
    assert sorted(q("SELECT doi, source_seed_doi FROM scoped_candidates")) == [
        ("10.1089/ther.2017.29031.mkb", "10.5555/seed.0001"), ("10.7777/upper.0001", "10.5555/seed.0001")]


def test_no_citations_leaves_scoped_rows_alone(env):
    root, index, q = env
    lib = make_lib(root, "teaching_a", ["10.5555/seed.0001"])
    (lib / "_resp_forward_citations.csv").write_text(scoped_csv([
        ("10.5555/seed.0001", "Ch09", "10.7777/resp.0001")]), encoding="utf-8")
    assert index() == 0
    (lib / "_resp_forward_citations.csv").unlink()
    assert index("--no-citations") == 0
    assert q("SELECT COUNT(*) FROM scoped_candidates")[0][0] == 1
