"""W3b verifier J: the seeder (W3-C2) where it meets the rest of the pipeline.

- A seeder draft (every header line, the --pmc-check columns), renamed into a queue, through the REAL
  `sweep.run` and the REAL migrate (stage scripts stubbed by the W1 verifier's Stages), for a
  subproject: sweep takes the destination, the stages get the registry library, the extra columns
  ride through the residual, and the draft name itself is never swept.
- Ranking against an independent oracle computed in Python from the raw index tables (candidates,
  paper_metadata, paper_locations, cites, scoped_candidates) of an index built by the REAL
  index_portfolio: four projects (one a subproject) sharing candidates, a text-only holding, an
  identity-flagged file, undated rows (year 0 from the walker CSVs, NULL in a scope), ties.
- run_daily's auto-stage driven in-process (the seeder answered by its real main, sweep stubbed):
  the volume the new default stages, how run_daily reads the seeder's exit 1, and the auto_stage gate.
- --pmc-check through the REAL lit_net.doi_to_pmcid against MockServers (idconv live or refused, a
  refused Europe PMC host, 429, transport failures, a crash mid-batch); pmc.ncbi.nlm.nih.gov and the
  arXiv hosts are refused in every state these tests create.
Two tests lock fixes handed back to the dispatcher; they fail on 50789ae and pass since the fix commit.
"""
import contextlib
import csv
import datetime
import io
import json
import re
import types
from collections import defaultdict
from pathlib import Path

import duckdb
import pytest

import forward_citations as fc
import lit_net
import lit_util
import migrate_closed_to_md as mig
import run_daily
import seed_queue_from_top_candidates as S
import sweep
from litpipe import net
from tests.netmock import Reply
from tests.test_seeder import World, notes_of, ris, summary_of
from tests.test_w1_verify_b import Stages, _FrozenDate, read_csv

DAY = "2026-10-06"
ARXIV = ("export.arxiv.org", "arxiv.org", "www.arxiv.org")
JSON_H = {"Content-Type": "application/json"}
EMAIL = "tester@litpipe-test.org"            # net_env's LITPIPE_EMAIL
REVERSE_FIELDS = ["seed", "first_author", "year", "title_snippet", "doi", "raw", "seed_doi", "source"]
FORWARD_FIELDS = ["seed_pdf", "seed_doi", "citing_doi", "citing_title", "citing_year", "citing_authors",
                  "citing_venue", "citing_cited_by"]


def write_csv(path, fields, rows):
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def reverse_rows(edges, years=None, titles=None):
    years, titles = years or {}, titles or {}
    return [{"seed": f"{s}.pdf", "first_author": "Lee", "year": years.get(d, 2020),
             "title_snippet": titles.get(d, f"Paper {d}"), "doi": d, "raw": "ref", "seed_doi": s,
             "source": "s2"} for s, d in edges]


def forward_rows(edges, years=None, cites=None):
    years, cites = years or {}, cites or {}
    return [{"seed_pdf": "s.pdf", "seed_doi": s, "citing_doi": d, "citing_title": f"Citing {d}",
             "citing_year": years.get(d, 2021), "citing_authors": "Avery, Kim", "citing_venue": "V",
             "citing_cited_by": cites.get(d, 5)} for s, d in edges]


def flag_file(w, key, doi):
    """An identity-flagged PDF whose .ris names `doi`: a review item, never a holding."""
    stem = "2019_Flagged0001"
    lib = w.lib(key)
    (lib / f"{stem}.pdf").write_bytes(b"%PDF-1.4 stub")
    (lib / f"{stem}.ris").write_text(ris(doi, title="A flagged paper"), encoding="utf-8")
    (lib / f"{stem}.identity.json").write_text(json.dumps({"doi": doi, "identity": "FLAG", "doc_kind": "ARTICLE"}),
                                               encoding="utf-8")


def draft_rows(path):
    head = [ln for ln in Path(path).read_text(encoding="utf-8").splitlines() if ln.startswith("#")]
    fields, rows = sweep.read_queue(path)
    return head, fields, rows


# ================================================================ ranking against an oracle
D = "10.5555/"
BIG = [f"{D}big.{i:04d}" for i in range(6)]
PAR = [f"{D}par.{i:04d}" for i in range(3)]
SUB = [f"{D}sub.{i:04d}" for i in range(3)]
SM = [f"{D}sm.{i:04d}" for i in range(2)]
SHARED, PARSUB, SUBONLY = f"{D}shared.0001", f"{D}parsub.0002", f"{D}subonly.0003"
TIE_A, TIE_B, TIE_C, TIE_D = f"{D}tiea.0004", f"{D}tieb.0005", f"{D}tiec.0006", f"{D}tied.0007"
OLD, UNDATED, DIGIT, GREEK = f"{D}old.0008", f"{D}undated.0009", f"{D}digit.0010", f"{D}greek.0011"
HELD, TEXT, FLAGGED = f"{D}heldpdf.0012", f"{D}textonly.0013", f"{D}flagged.0014"
FW_A, FW_B, FW_C = f"{D}fwa.0015", f"{D}fwb.0016", f"{D}fwc.0017"
SC_X, SC_Y, SC_Z, SC_OTHER, SC_SUB = (f"{D}scx.0018", f"{D}scy.0019", f"{D}scz.0020", f"{D}scother.0021",
                                      f"{D}scsub.0022")
SC_W = f"{D}scw.0023"           # sorts before SC_Y by DOI, after it by year
KEYS = ("research_big", "Parent", "Parent/sub", "teaching_small")


def build_rank_world(w):
    w.register("research_big", "literature")
    w.register("Parent", "docs/literature")
    w.register("Parent/sub", "sub/literature", parent="Parent")
    w.register("teaching_small", "literature")
    w.hold("research_big", *BIG, HELD)
    w.hold("Parent", *PAR)
    w.hold("Parent/sub", *SUB)
    w.hold("teaching_small", *SM)
    w.text_only("teaching_small", TEXT)
    flag_file(w, "teaching_small", FLAGGED)
    years = {OLD: 2008, UNDATED: "", SHARED: 2018, PARSUB: 2020, SUBONLY: 2021, TIE_A: 2017, TIE_B: 2017,
             TIE_C: 2019, TIE_D: 2019, FLAGGED: 2015}
    titles = {DIGIT: "3D printing of heat sensors", GREEK: "β-Alanine and heat"}
    rev = {
        "research_big": [(s, SHARED) for s in BIG] + [(BIG[0], TIE_B), (BIG[1], FLAGGED)]
                        + [(s, TEXT) for s in BIG[:3]],
        "Parent": [(s, SHARED) for s in PAR[:2]] + [(s, PARSUB) for s in PAR],
        "Parent/sub": [(SUB[0], SHARED)] + [(s, PARSUB) for s in SUB[:2]] + [(s, SUBONLY) for s in SUB],
        "teaching_small": [(SM[0], SHARED)] + [(s, d) for s in SM for d in (TIE_A, TIE_B, UNDATED, DIGIT,
                                                                             HELD, FLAGGED)]
                          + [(SM[0], TIE_C), (SM[1], TIE_D), (SM[0], OLD), (SM[0], GREEK), (SM[0], TEXT)],
    }
    for key, edges in rev.items():
        write_csv(w.lib(key) / "_reverse_citations_parsed.csv", REVERSE_FIELDS, reverse_rows(edges, years, titles))
    write_csv(w.lib("teaching_small") / "_forward_citations.csv", FORWARD_FIELDS,
              forward_rows([(SM[0], FW_A), (SM[0], FW_B), (SM[1], FW_B), (SM[1], FW_C)],
                           years={FW_A: 2022, FW_B: 2016, FW_C: ""}, cites={FW_A: 50, FW_B: 7, FW_C: 7}))
    write_csv(w.lib("research_big") / "_forward_citations.csv", FORWARD_FIELDS,
              forward_rows([(BIG[0], FW_A), (BIG[2], FW_C)], years={FW_A: 2022, FW_C: ""},
                           cites={FW_A: 50, FW_C: 7}))
    # scoped harvests in W3-A's column shape (forward_citations.SCOPED_FIELDS)
    def sc(seed, doi, year, cites, source="s2", wid="", title=None, authors="Avery, Kim"):
        return {"seed_doi": seed, "seed_label": "Ch1", "seed_chapter": "1", "citing_paper_id": wid,
                "citing_doi": doi, "citing_title": title or f"Scoped {doi}", "citing_year": year,
                "citing_authors": authors, "citing_venue": "V", "citing_cited_by": cites,
                "citing_abstract": "", "source": source, "citing_oa": "", "seed_n_cites": "3"}
    write_csv(w.lib("teaching_small") / "_ch01_forward_citations.csv", fc.SCOPED_FIELDS, [
        sc(SM[0], SC_X, 2019, 10), sc(SM[1], SC_X, 2019, 10),
        sc(SM[0], SC_Y, 2023, 30, source="openalex", wid="W4321000", title="OpenAlex walked paper",
           authors="Quinn, R"),
        sc(SM[1], SC_Z, "", 30), sc(SM[1], SC_W, 2020, 30),
        sc(SM[0], HELD, 2019, 99), sc(SM[1], TEXT, 2019, 99),          # held as a PDF / text-only: never drafted
    ])
    write_csv(w.lib("teaching_small") / "_ch02_forward_citations.csv", fc.SCOPED_FIELDS,
              [sc(SM[0], SC_OTHER, 2020, 1)])
    write_csv(w.lib("Parent/sub") / "_ch01_forward_citations.csv", fc.SCOPED_FIELDS,
              [sc(SUB[0], SC_SUB, 2020, 1)])
    w.index()


def _tables(db, key, scope):
    con = duckdb.connect(str(db), read_only=True)
    try:
        q = lambda sql, p=(): con.execute(sql, list(p)).fetchall()  # noqa: E731
        return {
            "cands": q("SELECT doi, source_seed_doi, source_project, citing_cited_by FROM candidates"),
            "meta": {d: (y, t, a) for d, y, t, a in q("SELECT doi, year, title, authors FROM paper_metadata")},
            "locs": q("SELECT doi, project FROM paper_locations"),
            "cites": q("SELECT citing_doi, cited_doi FROM cites"),
            "scoped": q("SELECT doi, source_seed_doi, citing_cited_by, year, title, authors FROM scoped_candidates "
                        "WHERE project = ? AND scope = ?", [key, scope]) if scope else [],
        }
    finally:
        con.close()


def _desc_nulls_last(v):
    return (v is None, -(v or 0))


def oracle(db, *, key, mode, scope=None, year_min=2010, min_seeds=None, min_cites=0, limit=100,
           recent_first=False, include_undated=False):
    """(dois in order, total matching, undated count, {doi: (n_own, n_port)}): an independent reading of
    the W3-C2 spec (amendments 2-4) over the raw tables."""
    t = _tables(db, key, scope)
    held = {d for d, _ in t["locs"]}
    own, port, via, mc = defaultdict(set), defaultdict(set), defaultdict(set), {}
    for d, s, p, c in t["cands"]:
        port[d].add(s)
        via[d].add(p)
        if p == key:
            own[d].add(s)
        if c is not None:
            mc[d] = max(mc.get(d, c), c)
    rows = []
    if mode == "project":
        for d in own:
            y, ti, _ = t["meta"].get(d, (None, None, None))
            rows.append({"doi": d, "year": y, "title": ti, "rank": len(own[d]), "cites": mc.get(d)})
        order = lambda r: (-r["rank"], _desc_nulls_last(r["cites"]), -len(port[r["doi"]]), r["doi"])  # noqa: E731
    elif mode == "portfolio":
        for d in port:
            if key in via[d] and d not in held:
                y, ti, _ = t["meta"].get(d, (None, None, None))
                rows.append({"doi": d, "year": y, "title": ti, "rank": len(port[d]), "cites": mc.get(d)})
        order = lambda r: (-r["rank"], _desc_nulls_last(r["cites"]), r["doi"])  # noqa: E731
    elif mode == "cocitation":
        mine = {d for d, p in t["locs"] if p == key}
        citing = defaultdict(set)
        for ci, cd in t["cites"]:
            if ci in mine and cd != ci and cd not in mine:
                citing[cd].add(ci)
        for d, cs in citing.items():
            y, ti, _ = t["meta"].get(d, (None, None, None))
            rows.append({"doi": d, "year": y, "title": ti, "rank": len(cs), "cites": mc.get(d) or 0})
        order = lambda r: (-r["rank"], r["doi"])  # noqa: E731
    else:
        g = defaultdict(lambda: {"seeds": set(), "cites": None, "year": None, "title": None})
        for d, s, c, y, ti, _ in t["scoped"]:
            r = g[d]
            r["seeds"].add(s)
            if c is not None:
                r["cites"] = c if r["cites"] is None else max(r["cites"], c)
            if y:
                r["year"] = y if r["year"] is None else max(r["year"], y)
            if ti:
                r["title"] = ti if r["title"] is None else max(r["title"], ti)
        for d, r in g.items():
            my, mt, _ = t["meta"].get(d, (None, None, None))
            rows.append({"doi": d, "year": r["year"] or (my or None), "title": r["title"] or mt,
                         "rank": len(r["seeds"]), "cites": r["cites"]})
        order = lambda r: (-r["rank"], _desc_nulls_last(r["cites"]), _desc_nulls_last(r["year"]), r["doi"])  # noqa: E731
    floor = S.DEFAULT_MIN_SEEDS[mode] if min_seeds is None else min_seeds

    def base_ok(r):
        return (r["doi"] not in held and r["title"] is not None and re.fullmatch("[A-Za-z]", r["title"][:1])
                and r["rank"] >= floor and (r["cites"] or 0) >= min_cites)

    def dated(r):
        return (r["year"] or 0) > 0

    passing = [r for r in rows if base_ok(r)]
    undated = sum(1 for r in passing if not dated(r))
    kept = [r for r in passing if (dated(r) and r["year"] >= year_min) or (include_undated and not dated(r))]
    if recent_first:
        kept.sort(key=lambda r: (not dated(r), _desc_nulls_last(r["year"]), order(r)))
    else:
        kept.sort(key=order)
    return [r["doi"] for r in kept[:limit]], len(kept), undated, {d: (len(own[d]), len(port[d])) for d in port}


OPTION_SETS = [
    {}, {"min_seeds": 2}, {"year_min": 2018}, {"min_cites": 6}, {"include_undated": True},
    {"recent_first": True}, {"recent_first": True, "include_undated": True}, {"limit": 2}, {"year_min": 0},
]


def _argv(key, mode, scope, o):
    argv = ["--project", key]
    argv += ["--scope", scope] if mode == "scope" else (["--rank", mode] if mode != "project" else [])
    for k, flag in (("min_seeds", "--min-seeds"), ("year_min", "--year-min"), ("min_cites", "--min-cites"),
                    ("limit", "--limit")):
        if k in o:
            argv += [flag, str(o[k])]
    argv += ["--include-undated"] if o.get("include_undated") else []
    argv += ["--recent-first"] if o.get("recent_first") else []
    return argv


def _compare(w, key, mode, scope=None):
    seen = []
    for o in OPTION_SETS:
        out = w.tmp / "drafts" / f"{len(seen)}.csv"
        rc, so, err = w.seed(*_argv(key, mode, scope, o), "--output", str(out))
        assert rc == 0, (o, err)
        _, _, rows = draft_rows(out)
        want, total, undated, counts = oracle(w.db, key=key, mode=mode, scope=scope, **o)
        got = [r["doi"] for r in rows]
        assert got == want, (key, mode, o)
        s = summary_of(so)
        assert (s["total_matching"], s["undated"]) == (total, undated), (key, mode, o)
        for r in rows:
            n = notes_of(r)
            n_own, n_port = counts.get(r["doi"], (0, 0))
            assert (int(n["n_seeds_project"]), int(n["n_seeds_portfolio"])) == (n_own, n_port), (r["doi"], o)
        seen.append(got)
    return seen


@pytest.fixture
def rank_world(tmp_path, monkeypatch, capsys):
    w = World(tmp_path, monkeypatch, capsys)
    build_rank_world(w)
    return w


def test_the_fixture_index_has_the_shapes_the_oracle_needs(rank_world):
    w = rank_world
    locs = dict(w.q("SELECT doi, has_pdf FROM paper_locations"))
    assert locs[TEXT] is False and locs[HELD] is True and FLAGGED not in locs      # text-only held; flagged not
    years = dict(w.q("SELECT doi, year FROM paper_metadata"))
    assert years[UNDATED] == 0 and years[FW_C] == 0                                # the walker CSVs give 0
    assert w.q("SELECT year FROM scoped_candidates WHERE doi = ?", [SC_Z]) == [(None,)]   # a scope gives NULL
    projs = {p for (p,) in w.q("SELECT DISTINCT source_project FROM candidates")}
    assert projs == set(KEYS)


@pytest.mark.parametrize("key", KEYS)
def test_project_rank_matches_the_oracle(rank_world, key):
    seen = _compare(rank_world, key, "project")
    if key == "teaching_small":
        assert FLAGGED in seen[0] and TEXT not in seen[0] and HELD not in seen[0]
        assert seen[0].index(TIE_B) < seen[0].index(TIE_A)        # equal own seeds and cites: portfolio count
        assert seen[0].index(TIE_C) < seen[0].index(TIE_D)        # then the DOI


@pytest.mark.parametrize("key", KEYS)
def test_portfolio_rank_matches_the_oracle(rank_world, key):
    _compare(rank_world, key, "portfolio")


@pytest.mark.parametrize("key", KEYS)
def test_cocitation_rank_matches_the_oracle(rank_world, key):
    _compare(rank_world, key, "cocitation")


def test_scope_matches_the_oracle_and_reads_only_its_scope(rank_world):
    seen = _compare(rank_world, "teaching_small", "scope", scope="ch01")
    allrows = set().union(*map(set, seen))
    assert SC_OTHER not in allrows and SC_SUB not in allrows      # another scope, another project
    assert HELD not in allrows and TEXT not in allrows            # a scoped citer the portfolio holds


def test_a_parent_and_its_subproject_count_only_their_own_seeds(rank_world):
    w = rank_world
    got = {}
    for key in ("Parent", "Parent/sub"):
        out = w.tmp / f"{key.replace('/', '_')}.csv"
        rc, _, err = w.seed("--project", key, "--output", str(out))
        assert rc == 0, err
        got[key] = {r["doi"]: notes_of(r) for r in draft_rows(out)[2]}
    assert got["Parent"][PARSUB]["n_seeds_project"] == "3"          # the subproject's 2 seeds excluded
    assert got["Parent/sub"][PARSUB]["n_seeds_project"] == "2"      # the parent's 3 seeds excluded
    assert got["Parent"][SHARED]["n_seeds_project"] == "2" and got["Parent/sub"][SHARED]["n_seeds_project"] == "1"
    assert SUBONLY in got["Parent/sub"] and SUBONLY not in got["Parent"]
    assert got["Parent"][PARSUB]["n_seeds_portfolio"] == got["Parent/sub"][PARSUB]["n_seeds_portfolio"] == "5"


def test_the_openalex_walked_scoped_row_reaches_the_scope_draft_with_its_metadata(rank_world):
    w = rank_world
    out = w.tmp / "scope.csv"
    rc, so, err = w.seed("--project", "teaching_small", "--scope", "ch01", "--include-undated", "--output", str(out))
    assert rc == 0, err
    head, fields, rows = draft_rows(out)
    by = {r["doi"]: r for r in rows}
    assert (by[SC_Y]["title"], by[SC_Y]["authors"], by[SC_Y]["year"]) == ("OpenAlex walked paper", "Quinn, R", "2023")
    assert [r["doi"] for r in rows] == [SC_X, SC_Y, SC_W, SC_Z]   # 2 seeds; then the cites-30 tie by year, undated last
    assert by[SC_Z]["year"] == ""
    assert all(r["destination"] == "literature/" for r in rows)
    src = next(ln for ln in head if ln.startswith("# Source:"))
    assert "differs from the walker's _ch01_descendants.csv only by OA" in src
    assert next(ln for ln in head if ln.startswith("# Order:")) == f"# Order: {S.ORDER_TEXT['scope']}"


# ================================================================ the draft through the REAL sweep
def wire_sweep(w, monkeypatch):
    """sweep and migrate on the World's registry and root: stage scripts answered by the W1 verifier's
    Stages (CLOSED / NO_PMCID / NO_MATCH), migrate by its real main in-process, metadata lookups fail,
    migrate's date frozen."""
    for mod in (sweep, mig):
        monkeypatch.setattr(mod, "CONFIG_PATH", w.cfg)
    monkeypatch.setenv("USERPROFILE", str(w.tmp / "home"))
    monkeypatch.setenv("HOME", str(w.tmp / "home"))
    monkeypatch.setattr(sweep, "_resolve_meta", lambda doi: (_ for _ in ()).throw(RuntimeError("no metadata")))
    st = Stages()
    monkeypatch.setattr(sweep.subprocess, "run", st)
    monkeypatch.setattr(mig, "datetime", types.SimpleNamespace(date=_FrozenDate, timedelta=datetime.timedelta))
    _FrozenDate.frozen = datetime.date.fromisoformat(DAY)
    return st


def epmc_body(hits):
    return json.dumps({"version": "6.9", "hitCount": len(hits), "request": {},
                       "resultList": {"result": [{"doi": d, "pmcid": p, "isOpenAccess": "Y"} for d, p in hits]}})


def esearch_none():
    return json.dumps({"esearchresult": {"count": "0", "retmax": "0", "idlist": []}})


@pytest.mark.parametrize("tag", [None, "teach"])
def test_a_subproject_draft_with_pmc_columns_sweeps_into_the_registry_library(
        tag, tmp_path, monkeypatch, capsys, net_env, mock_server):
    (tmp_path / "w").mkdir()
    w = World(tmp_path / "w", monkeypatch, capsys)
    srv = mock_server()
    monkeypatch.setattr(lit_net, "IDCONV", srv.url("/idconv/"))
    monkeypatch.setattr(lit_net, "EPMC_SEARCH", srv.url("/epmc/search"))
    monkeypatch.setattr(lit_net, "EUTILS", srv.url("/eutils"))
    for h in ARXIV + ("pmc.ncbi.nlm.nih.gov",):
        net_env.state.refuse(h, "manual: test", persistence="manual")
    key = "Teach/teaching_sub"
    w.register("Teach", "literature")
    w.register(key, "teaching_sub/literature", parent="Teach")
    seeds = [f"{D}tsub.{i:04d}" for i in range(2)]
    w.hold(key, *seeds)
    cands = [f"{D}tcand.{i:04d}" for i in range(3)]
    write_csv(w.lib(key) / "_forward_citations.csv", FORWARD_FIELDS,
              forward_rows([(s, c) for s in seeds for c in cands[:2]] + [(seeds[0], cands[2])]))
    w.index()
    srv.script("/epmc/search", Reply(200, epmc_body([(cands[0], "PMC7000001")]), JSON_H))
    srv.script("/eutils/esearch.fcgi", Reply(200, esearch_none(), JSON_H))
    rc, out, err = w.seed("--project", key, "--pmc-check", *(["--tag", tag] if tag else []))
    assert rc == 0, err
    proot, lib = w.proot(key), w.lib(key)
    draft = proot / S.draft_name(tag)
    head, fields, rows = draft_rows(draft)
    assert fields == S.BASE_COLUMNS + S.PMC_COLUMNS and len(rows) == 3
    assert [ln.split(":")[0] for ln in head] == ["# REVIEW BEFORE SWEEP -- drop irrelevant rows, then `mv " + draft.name
                                                  + " " + S.final_name(tag) + "`", "# Source", "# Order", "# Filters",
                                                  "# Undated", "# PMC check", "# Total rows"]
    assert {r["destination"] for r in rows} == {"literature/"}
    assert EMAIL not in draft.read_text(encoding="utf-8") and "email=" not in draft.read_text(encoding="utf-8")

    st = wire_sweep(w, monkeypatch)
    capsys.readouterr()
    res = sweep.run(project=key, date=DAY, migrate=True, loose_ends=False)       # the draft alone: no queue
    assert res["exit_code"] == sweep.EXIT_NO_QUEUE and st.calls == [] and draft.exists()

    queue = draft.rename(proot / S.final_name(tag))
    res = sweep.run(project=key, date=DAY, migrate=True, loose_ends=False)
    assert res["exit_code"] == sweep.EXIT_OK, capsys.readouterr().out
    stage_libs = {Path(c[c.index("--lib-dir") + 1]).resolve() for s, c in st.calls if "--lib-dir" in c}
    assert stage_libs == {lib.resolve()}                                      # every stage got the registry library
    assert not (proot / "teaching_sub").exists()                              # no shadow library
    stem = f"lit_pull_queue{'.' + tag if tag else ''}.{DAY}"
    triaged = read_csv(proot / f"{stem}.unpaywall.csv")                      # written from the triage CSV
    assert [r["doi"] for r in triaged] == [r["doi"] for r in rows]            # header lines skipped
    assert [r["doi"] for r in sweep.read_queue(proot / f"{stem}.processed.csv")[1]] == [r["doi"] for r in rows]
    residual = read_csv(proot / f"{stem}.residual.csv")
    assert {r["doi"] for r in residual} == {r["doi"] for r in rows}
    assert {r["doi"]: (r["pmcid"], r["pmc_status"]) for r in residual} == {
        r["doi"]: (r["pmcid"], r["pmc_status"]) for r in rows}                 # the extra columns ride along
    assert all(r["residual_class"] == "TERMINAL_CLOSED" for r in residual)
    ill = (proot / mig.ILL_NAME).read_text(encoding="utf-8")
    assert all(r["doi"] in ill for r in rows)                                 # routed at the subproject root
    assert not queue.exists()                                                 # the queue retired
    assert not any("#" in r["doi"] for r in residual)


def test_no_queue_reader_takes_a_draft_name(tmp_path):
    import backfills.pmc_recovery as pr
    root = tmp_path / "p"
    root.mkdir()
    body = "# REVIEW BEFORE SWEEP\ndoi,title,authors,year,destination,notes,pmcid,pmc_status\n" \
           "10.5555/q.0001,T,A,2020,literature/,n,,NO_PMCID\n"
    for name in (S.draft_name(None), S.draft_name("teach"), S.draft_name("ch07")):
        (root / name).write_text(body, encoding="utf-8")
        assert sweep.queue_tag(name) is None and sweep.parse_artifact(name) is None
    assert sweep.discover_queues(root) == [] and pr.live_queue_dois(root) == set()
    assert sweep.run_ids_in_use([root]) == set()
    (root / S.final_name("teach")).write_text(body, encoding="utf-8")
    assert [p.name for p in sweep.discover_queues(root)] == [S.final_name("teach")]
    assert pr.live_queue_dois(root) == {"10.5555/q.0001"}


# ================================================================ run_daily's auto-stage
class DailyStages:
    """run_daily's subprocess.run: the seeder answered by its REAL main(argv) in-process with exactly
    run_daily's argv; sweep and migrate recorded and answered 0."""

    def __init__(self):
        self.calls = []

    def __call__(self, cmd, **kw):
        cmd = [str(c) for c in cmd]
        script = Path(cmd[1]).name
        self.calls.append((script, cmd[2:]))
        if script == "seed_queue_from_top_candidates.py":
            out, err = io.StringIO(), io.StringIO()
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                rc = S.main(cmd[2:])
            return types.SimpleNamespace(returncode=rc, stdout=out.getvalue(), stderr=err.getvalue())
        return types.SimpleNamespace(returncode=0, stdout="", stderr="")

    def scripts(self):
        return [s for s, _ in self.calls]


SMALL = "Teach/teaching_small"


def build_daily_world(w, small_extra=None, key=SMALL, lib_dir="teaching_small/literature"):
    """A big library (40 seeds) and a small subproject library shaped like the live small ones: 6 seeds,
    each citing 20 references of its own and 10 shared by all six; the big library cites 5 of the small
    library's references from 3 seeds each. Own-seed candidates: 6 x 20 + 10 = 130. Portfolio count >= 3
    (the pre-W3-C2 default): the 10 shared (6) and the 5 the big library also cites (1 + 3)."""
    w.top["db_dir"] = str(w.db.parent)
    w.register("research_big", "literature")
    w.register("Teach", "literature")
    w.register(key, lib_dir, parent="Teach", **(small_extra or {}))
    big = [f"{D}dbig.{i:04d}" for i in range(40)]
    small = [f"{D}dsm.{i:04d}" for i in range(6)]
    w.hold("research_big", *big)
    w.hold(key, *small)
    shared = [f"{D}dshared.{j:04d}" for j in range(10)]
    own = {s: [f"{D}dref.{i:02d}{j:02d}" for j in range(20)] for i, s in enumerate(small)}
    edges = [(s, d) for s in small for d in shared] + [(s, d) for s in small for d in own[s]]
    write_csv(w.lib(key) / "_reverse_citations_parsed.csv", REVERSE_FIELDS, reverse_rows(edges))
    co = [own[small[0]][j] for j in range(5)]
    write_csv(w.lib("research_big") / "_reverse_citations_parsed.csv", REVERSE_FIELDS,
              reverse_rows([(b, d) for d in co for b in big[:3]] + [(b, f"{D}dbigonly.{i:04d}")
                                                                     for i, b in enumerate(big)]))
    w.index()


@pytest.fixture
def daily(tmp_path, monkeypatch, capsys):
    w = World(tmp_path, monkeypatch, capsys)
    monkeypatch.setattr(run_daily, "CONFIG_PATH", w.cfg)
    st = DailyStages()
    monkeypatch.setattr(run_daily.subprocess, "run", st)
    return w, st


@pytest.mark.parametrize("small_key,lib_dir", [(SMALL, "teaching_small/literature"), ("sm", "sm/literature")])
def test_run_daily_stages_the_new_default_draft_at_the_subproject_root(daily, small_key, lib_dir):
    """Measurement: the new default (own seeds, --min-seeds 1) drafts 100 of 130 own-seed candidates for
    the small library; the pre-W3-C2 order and filters (--rank portfolio, --min-seeds 3) give 15. run_daily
    stages all 100 and sweeps them. The seeder's draft lands where run_daily looks (project_root): were the
    output path PROJECTS_ROOT / key again, run_daily would print 'no draft produced' and stage nothing."""
    w, st = daily
    build_daily_world(w, small_extra={"auto_stage": True}, key=small_key, lib_dir=lib_dir)   # passes with the gate too
    old_out = w.tmp / "old.csv"
    rc, so, err = w.seed("--project", small_key, "--rank", "portfolio", "--output", str(old_out))
    assert rc == 0, err
    n_old = len(draft_rows(old_out)[2])
    assert n_old == 15
    ok = run_daily.pipeline_one(small_key, run_daily.load_projects(), False, False, DAY)
    assert ok is True
    seed_argv = [a for s, a in st.calls if s == "seed_queue_from_top_candidates.py"]
    assert seed_argv == [["--project", small_key]]                       # no --pmc-check: exit 2 cannot arise
    proot = w.proot(small_key)
    assert not (proot / "lit_pull_queue.draft.csv").exists()         # consumed by the stager
    fields, staged = sweep.read_queue(proot / "lit_pull_queue.csv")
    assert fields == S.BASE_COLUMNS and len(staged) == 100           # 100 / 15 = 6.7x this fixture's old draft
    assert {r["destination"] for r in staged} == {"literature/"}
    assert not (proot / "lit_pull_queue.csv").read_text(encoding="utf-8").startswith("#")
    assert st.scripts() == ["seed_queue_from_top_candidates.py", "sweep.py", "migrate_closed_to_md.py"]
    assert not (w.root / small_key).exists() or w.root / small_key == proot


def test_run_daily_reads_the_seeders_exit_1_as_a_failed_project(daily):
    w, st = daily
    build_daily_world(w)
    w.register("Teach/outside", "literature", parent="Teach", auto_stage=True)   # library outside its root
    ok = run_daily.pipeline_one("Teach/outside", run_daily.load_projects(), False, False, DAY)
    assert ok is False                                               # StepError: the project failed, not "0 rows"
    assert st.scripts() == ["seed_queue_from_top_candidates.py"]     # nothing staged, nothing swept
    assert not (w.proot("Teach/outside") / "lit_pull_queue.csv").exists()


def test_run_daily_stages_only_auto_stage_projects(daily):
    """The registry contract (litpipe.config: auto_stage, default false: may the runner stage drafts
    itself) and W4-A's acceptance ("an auto_stage: false project is never staged"). run_daily.py:120-163
    seeds and stages every READY project. A tagged queue must still be swept."""
    w, st = daily
    build_daily_world(w)
    proot = w.proot(SMALL)
    ok = run_daily.pipeline_one(SMALL, run_daily.load_projects(), False, False, DAY)
    assert ok is True
    assert "seed_queue_from_top_candidates.py" not in st.scripts() and not (proot / "lit_pull_queue.csv").exists()
    assert "sweep.py" not in st.scripts()                            # nothing waiting: nothing to sweep
    (proot / "lit_pull_queue.teach.csv").write_text(
        "doi,title,authors,year,destination,notes\n10.5555/x.0001,T,A,2020,literature/,n\n", encoding="utf-8")
    st.calls.clear()
    assert run_daily.pipeline_one(SMALL, run_daily.load_projects(), False, False, DAY) is True
    assert st.scripts() == ["sweep.py", "migrate_closed_to_md.py"]   # the tagged queue is still swept
    w.projects[SMALL]["auto_stage"] = True
    w._write()
    st.calls.clear()
    (proot / "lit_pull_queue.teach.csv").unlink()
    assert run_daily.pipeline_one(SMALL, run_daily.load_projects(), False, False, DAY) is True
    assert st.scripts()[0] == "seed_queue_from_top_candidates.py" and (proot / "lit_pull_queue.csv").exists()


# ================================================================ --pmc-check on mocks
OK_D, EMB_D, NONE_D, ABSENT_D = f"{D}pok.0001", f"{D}pemb.0002", f"{D}pnone.0003", f"{D}pabsent.0004"
PMC_DOIS = (OK_D, EMB_D, NONE_D, ABSENT_D)


def idconv_body(records):
    return json.dumps({"status": "ok", "responseDate": "2026-10-06", "request": "", "records": records})


IDCONV_ANSWERS = [{"requested-id": OK_D, "doi": OK_D, "pmcid": "PMC7100001", "pmid": "1"},
                  {"requested-id": EMB_D, "doi": EMB_D, "pmcid": "PMC7100002", "live": False,
                   "release-date": "2027-01-15"},
                  {"requested-id": NONE_D, "errmsg": "Identifier not found in PMC"}]


@pytest.fixture
def pmcw(tmp_path, monkeypatch, capsys, net_env, mock_server):
    """research_a: four candidates; idconv and E-utilities on 127.0.0.1, Europe PMC on 127.0.0.2.
    pmc.ncbi.nlm.nih.gov and the arXiv hosts are refused in the state. `live_idconv()` points the
    seeder's idconv refusal check at a mock host name, so the mock idconv is reachable while the real
    host stays refused."""
    (tmp_path / "w").mkdir()
    w = World(tmp_path / "w", monkeypatch, capsys)
    w.srv, w.epmc, w.net_env = mock_server(), mock_server("127.0.0.2"), net_env
    monkeypatch.setattr(lit_net, "IDCONV", w.srv.url("/idconv/"))
    monkeypatch.setattr(lit_net, "EUTILS", w.srv.url("/eutils"))
    monkeypatch.setattr(lit_net, "EPMC_SEARCH", w.epmc.url("/epmc/search"))
    for h in ARXIV + ("pmc.ncbi.nlm.nih.gov",):
        net_env.state.refuse(h, "manual: test", persistence="manual")
    w.live_idconv = lambda: monkeypatch.setattr(lit_net, "IDCONV_HOST", "idconv.mock.invalid")
    w.register("research_a", "literature")
    seeds = [f"{D}pseed.{i:04d}" for i in range(4)]
    w.hold("research_a", *seeds)
    write_csv(w.lib("research_a") / "_reverse_citations_parsed.csv", REVERSE_FIELDS,
              reverse_rows([(s, d) for i, d in enumerate(PMC_DOIS) for s in seeds[:4 - i]]))
    w.index()
    return w


def _pmc_run(w, *extra):
    rc, out, err = w.seed("--project", "research_a", "--pmc-check", *extra)
    head, fields, rows = draft_rows(w.proot("research_a") / "lit_pull_queue.draft.csv")
    return rc, out, head, {r["doi"]: (r["pmcid"], r["pmc_status"]) for r in rows}


def _no_forbidden_host(w):
    hosts = {str(ln.get("host")) for ln in w.net_env.ledger_lines()}
    assert "pmc.ncbi.nlm.nih.gov" not in hosts and not any("arxiv" in h for h in hosts)
    assert EMAIL not in w.net_env.ledger_text()


def test_idconv_live_answers_found_embargoed_and_not_found_definitively(pmcw):
    w = pmcw
    w.live_idconv()
    w.srv.script("/idconv/", Reply(200, idconv_body(IDCONV_ANSWERS), JSON_H))
    w.epmc.script("/epmc/search", Reply(200, epmc_body([(ABSENT_D, "PMC7100004")]), JSON_H))
    rc, out, head, got = _pmc_run(w)
    assert rc == 0
    assert got == {OK_D: ("PMC7100001", "OK"), EMB_D: ("PMC7100002", "EMBARGOED until 2027-01-15"),
                   NONE_D: ("", "NO_PMCID"), ABSENT_D: ("PMC7100004", "OK")}
    assert len(w.srv.hits_for("/idconv/")) == 1 and not w.srv.hits_for("/eutils/esearch.fcgi")
    epmc = w.epmc.hits_for("/epmc/search")
    assert len(epmc) == 1 and ABSENT_D in epmc[0].query["query"][0] and OK_D not in epmc[0].query["query"][0]
    s = summary_of(out)
    assert s["exit_code"] == 0 and s["pmc"]["failed"] == 0 and s["reasons"] == []
    assert any(ln.startswith("# PMC check: 4 of 4 answered; 0 lookup(s) failed") for ln in head)
    _no_forbidden_host(w)


def test_idconv_refused_still_gets_europe_pmc_answers(pmcw):
    w = pmcw
    w.epmc.script("/epmc/search", Reply(200, epmc_body([(d, f"PMC72{i:05d}") for i, d in enumerate(PMC_DOIS)]),
                                        JSON_H))
    rc, out, head, got = _pmc_run(w)
    assert rc == 0 and all(v[1] == "OK" and v[0] for v in got.values())
    assert w.srv.hits == []                                                   # idconv refused: never called
    _no_forbidden_host(w)


def test_a_refused_europe_pmc_host_and_a_429_from_eutils_are_never_no_pmcid(pmcw):
    w = pmcw
    w.live_idconv()
    w.srv.script("/idconv/", Reply(200, idconv_body([]), JSON_H))            # idconv places none of them
    w.net_env.state.refuse("127.0.0.2", "manual: test", persistence="manual")
    w.srv.script("/eutils/esearch.fcgi", *[Reply(429, "", {"Retry-After": "1"})] * 8)
    rc, out, head, got = _pmc_run(w)
    assert rc == 2
    assert w.epmc.hits == []                                                  # refused: never sent
    for d, (pmcid, status) in got.items():
        assert pmcid == "" and status != "NO_PMCID" and status.startswith("REFUSED"), (d, status)
    last = out.rstrip("\n").splitlines()[-1]
    assert last.startswith(S.SUMMARY_MARKER)
    s = summary_of(out)
    assert s["exit_code"] == 2 and s["pmc"]["failed"] == 4 and s["reasons"][0].startswith("pmc-check: 4 of 4")
    _no_forbidden_host(w)


def test_a_429_from_every_fallback_is_a_failure_not_no_pmcid(pmcw):
    w = pmcw
    w.live_idconv()
    w.srv.script("/idconv/", Reply(200, idconv_body([]), JSON_H))
    w.epmc.script("/epmc/search", *[Reply(429, "", {"Retry-After": "1"})] * 8)
    w.srv.script("/eutils/esearch.fcgi", *[Reply(429, "", {"Retry-After": "1"})] * 8)
    rc, out, head, got = _pmc_run(w)
    assert rc == 2
    statuses = {v[1] for v in got.values()}
    assert "NO_PMCID" not in statuses and all(v[0] == "" for v in got.values())
    assert statuses == {"HTTP_429"}                           # Europe PMC's final 429: the first failure
    assert len(w.epmc.hits_for("/epmc/search")) > 1                       # retried, then given up
    assert summary_of(out)["reasons"] == ["pmc-check: 4 of 4 lookup(s) failed (HTTP_429: 4)"]
    assert summary_of(out)["pmc"]["failed"] == 4
    _no_forbidden_host(w)


def test_transport_failures_are_counted_and_redacted(pmcw):
    w = pmcw
    w.live_idconv()
    w.srv.script("/idconv/", Reply(200, idconv_body([]), JSON_H))
    w.epmc.script("/epmc/search", *[Reply(close=True)] * 8)
    w.srv.script("/eutils/esearch.fcgi", *[Reply(close=True)] * 8)
    rc, out, head, got = _pmc_run(w)
    assert rc == 2
    assert all(v[1].startswith("TRANSPORT") for v in got.values()), got
    s = summary_of(out)
    assert s["transport_failures"] == 4 and s["pmc"]["transport"] == 4
    text = (w.proot("research_a") / "lit_pull_queue.draft.csv").read_text(encoding="utf-8")
    assert EMAIL not in text and "email=" not in text and "tester%40" not in text
    _no_forbidden_host(w)


def test_transport_failures_behind_a_refused_idconv_are_still_counted(pmcw):
    """lit_net's chain reports the FIRST failure as a DOI's kind; with idconv refused that is REFUSED for
    every DOI, so a network outage on Europe PMC and E-utilities reads transport_failures == 0 in the
    seeder's [step-summary] (the runner's signal that the network, not the service, failed)."""
    w = pmcw
    w.epmc.script("/epmc/search", *[Reply(close=True)] * 8)
    w.srv.script("/eutils/esearch.fcgi", *[Reply(close=True)] * 8)
    rc, out, head, got = _pmc_run(w)
    assert rc == 2 and all("epmc_search:TRANSPORT" in v[1] for v in got.values())
    assert summary_of(out)["transport_failures"] == 4


def test_a_crash_mid_batch_marks_every_row_and_never_no_pmcid(pmcw, monkeypatch):
    w = pmcw
    w.live_idconv()
    w.srv.script("/idconv/", Reply(200, idconv_body(IDCONV_ANSWERS), JSON_H))
    real_get, n = net.get, []

    def flaky(url, **kw):
        n.append(url)
        if len(n) == 2:                                       # idconv answered; Europe PMC's call blows up
            raise RuntimeError("socket exploded")
        return real_get(url, **kw)
    monkeypatch.setattr(net, "get", flaky)
    rc, out, head, got = _pmc_run(w)
    assert rc == 2 and len(n) == 2
    assert {v for v in got.values()} == {("", "ERROR: RuntimeError: socket exploded")}
    assert summary_of(out)["pmc"]["failed"] == 4 and out.rstrip("\n").splitlines()[-1].startswith(S.SUMMARY_MARKER)


def test_no_request_without_the_flag_even_with_idconv_reachable(pmcw):
    w = pmcw
    w.live_idconv()
    w.srv.script("/idconv/", Reply(200, idconv_body(IDCONV_ANSWERS), JSON_H))
    rc, out, err = w.seed("--project", "research_a")
    assert rc == 0, err
    assert w.srv.hits == [] and w.epmc.hits == [] and w.net_env.ledger_lines() == []
    _, fields, _ = draft_rows(w.proot("research_a") / "lit_pull_queue.draft.csv")
    assert fields == S.BASE_COLUMNS


def test_the_column_literal_and_the_draft_header_names_the_order():
    import ast
    src = Path(S.__file__).read_text(encoding="utf-8")
    lits = [n for n in ast.walk(ast.parse(src)) if isinstance(n, ast.List)
            and [getattr(e, "value", None) for e in n.elts] == ["doi", "title", "authors", "year", "destination", "notes"]]
    assert len(lits) == 1 and S.BASE_COLUMNS == list(sweep.QUEUE_COLUMNS)
