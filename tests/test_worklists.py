"""W4-D: litpipe.worklists on fixture lists, pools and temp indexes built by the REAL index_portfolio.

- oa_blocked parses the lines migrate_closed_to_md.render_oa_blocked_block writes (the format
  contract), plus hand edits; a checked-off line is done, never open; group_by_host groups by the
  host after `via`, else by the publisher of the DOI prefix; the render has one-click links.
- ill_list ranks the open ILL rows by co-citation count, then n_seeds_pointing, ties by DOI, with
  every requesting project; seed_coverage warns below 80 %.
- Pool drawdown: disjoint batches, a simulated kill, an edited pool, seed_from idempotent.
- The CLI is dry by default; --write PATH writes the worklist.
"""
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

import index_portfolio as I
import lit_util
import migrate_closed_to_md as M
from litpipe import config, holdings
from litpipe import worklists as WL
from litpipe.outcomes import Kind
from tests import test_w4d_world as W

REPO = Path(__file__).resolve().parent.parent
REVERSE_FIELDS = ["seed", "first_author", "year", "title_snippet", "doi", "raw", "seed_doi", "source"]
SICI = "10.1002/(SICI)1097-4598(199709)20:9<1097::AID-MUS3>3.0.CO;2-X"


# ================================================================ a temp portfolio
class Portfolio:
    def __init__(self, tmp_path, monkeypatch, capsys, projects):
        spec = {"projects": projects, "held": {}, "forward": {}, "residuals": {}}
        self.w = W.build(tmp_path, spec)
        self.capsys = capsys
        self.registry = json.loads(self.w.cfg_path.read_text(encoding="utf-8"))
        monkeypatch.setattr(lit_util, "PROJECTS_ROOT", self.w.root)
        monkeypatch.setattr(config, "CONFIG_PATH", self.w.cfg_path)
        monkeypatch.setattr(I, "CONFIG_PATH", self.w.cfg_path)
        monkeypatch.setattr(WL, "CONFIG_PATH", self.w.cfg_path)

    def proot(self, key):
        return self.w.project_dir(key)

    def lib(self, key):
        return self.w.lib(key)

    def hold(self, key, stem, doi):
        lib = self.lib(key)
        (lib / f"{stem}.pdf").write_bytes(b"%PDF-1.4 stub\n")
        (lib / f"{stem}.ris").write_text(W.ris_text(doi, stem), encoding="utf-8")

    def reverse(self, key, edges):
        """edges: (seed stem, seed doi, cited doi)."""
        rows = [{"seed": f"{s}.pdf", "first_author": "Lee", "year": 2015, "title_snippet": f"Paper {d}",
                 "doi": d, "raw": "ref", "seed_doi": sd, "source": "s2"} for s, sd, d in edges]
        W.write_csv(self.lib(key) / "_reverse_citations_parsed.csv", REVERSE_FIELDS, rows)

    def forward(self, key, edges):
        rows = [{"seed_pdf": "s.pdf", "seed_doi": s, "citing_doi": d, "citing_title": f"Paper {d}",
                 "citing_year": 2021, "citing_authors": "Avery, Kim", "citing_venue": "V", "citing_cited_by": 5}
                for s, d in edges]
        W.write_csv(self.lib(key) / "_forward_citations.csv", W.FORWARD_FIELDS, rows)

    def index(self):
        assert I.main(["--db", str(self.w.db)]) == 0
        self.capsys.readouterr()

    def con(self):
        import duckdb
        return duckdb.connect(str(self.w.db), read_only=True)


def _sig(stage, kind, where, detail):
    return M._sig(stage, kind, where, detail)


def write_oa(path, project, rows, date="2026-10-01"):
    path.write_text(M._HEADERS[M.OA_BLOCKED_NAME](project) + M.render_oa_blocked_block(date, rows),
                    encoding="utf-8")


def check_off(path, doi):
    lines = path.read_text(encoding="utf-8").splitlines()
    hit = [i for i, ln in enumerate(lines) if f"[{doi}]" in ln or f"`{doi}`" in ln]
    assert len(hit) == 1, (doi, hit)
    lines[hit[0]] = lines[hit[0]].replace("- [ ]", "- [x]", 1)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


@pytest.fixture
def oa_world(tmp_path, monkeypatch, capsys):
    pf = Portfolio(tmp_path, monkeypatch, capsys, {"teaching_alpha": {"lib_dir": "literature"},
                                                   "research_beta": {"lib_dir": "literature"}})
    ta = pf.proot("teaching_alpha") / WL.OA_BLOCKED_NAME
    rb = pf.proot("research_beta") / WL.OA_BLOCKED_NAME
    write_oa(ta, "teaching_alpha", [
        {"doi": "10.1016/j.x.2020.01.001", "title": "An Elsevier paper", "year": "2020",
         "signals": [_sig("unpaywall", Kind.REFUSED, "download", "publisher/publishedVersion/HTTP_403")]},
        {"doi": "10.31219/osf.io/abc12", "title": "An OSF preprint", "year": "2021", "landing_url": "https://osf.io/abc12/",
         "signals": [_sig("preprint", Kind.SKIPPED, "download", "manual_preprint:osf")]},
        {"doi": "10.1152/japplphysiol.00001.2019", "title": "An APS paper", "year": "2019", "reason": "unpaywall: HTTP_403"},
        {"doi": "10.9999/unlisted.2020.5", "title": "An unlisted prefix", "reason": "unpaywall: HTTP_403"},
        {"doi": "10.1016/j.y.2018.02.002", "title": "A PMC page refused", "year": "2018", "pmcid": "PMC555",
         "signals": [_sig("pmc", Kind.REFUSED, "download", "europepmc/HTTP_403")]},
        {"doi": SICI, "title": "A title with (parens) and [brackets]", "year": "1997", "reason": "unpaywall: HTTP_403"},
    ])
    with open(ta, "a", encoding="utf-8") as f:
        f.write("- [ ] hand note: see 10.1123/ijspp.2019-0001 later\n- [ ] a note with no DOI at all\n")
    write_oa(rb, "research_beta", [
        {"doi": "10.1016/j.x.2020.01.001", "title": "An Elsevier paper", "year": "2020", "reason": "unpaywall: HTTP_403"},
        {"doi": "10.1016/j.z.2017.03.003", "title": "Fetched by hand", "year": "2017", "reason": "unpaywall: HTTP_403"},
        {"doi": "10.1080/abc.2016.123", "title": "A Taylor and Francis paper", "year": "2016", "reason": "unpaywall: HTTP_403"},
        {"doi": "10.1152/japplphysiol.00001.2019", "title": "An APS paper", "year": "2019", "reason": "unpaywall: HTTP_403"},
    ])
    check_off(rb, "10.1016/j.z.2017.03.003")
    check_off(ta, "10.1152/japplphysiol.00001.2019")
    return pf


# ================================================================ oa_blocked
def test_oa_blocked_parses_the_renderer_and_hand_edits(oa_world):
    rows = WL.oa_blocked(oa_world.registry)
    by = {(r["project"], r["key"]): r for r in rows}
    assert len(rows) == 11
    e = by[("teaching_alpha", "10.1016/j.x.2020.01.001")]
    assert (e["title"], e["year"], e["cause"], e["via"], e["host"], e["publisher"], e["checked"], e["parsed"]) == \
        ("An Elsevier paper", "2020", "HTTP_403", "unpaywall:publisher", "", "Elsevier", False, True)
    assert e["link"] == "https://doi.org/10.1016/j.x.2020.01.001"
    osf = by[("teaching_alpha", "10.31219/osf.io/abc12")]
    assert (osf["link"], osf["cause"], osf["via"], osf["host"]) == \
        ("https://osf.io/abc12/", "manual_preprint", "preprint:osf", "osf")
    pmc = by[("teaching_alpha", "10.1016/j.y.2018.02.002")]
    assert (pmc["link"], pmc["host"]) == ("https://pmc.ncbi.nlm.nih.gov/articles/PMC555/", "europepmc")
    sici = by[("teaching_alpha", holdings.doi_key(SICI))]
    assert sici["title"] == "A title with (parens) and [brackets]" and sici["doi"] == SICI
    assert "%3C1097" in sici["link"] and sici["publisher"] == "Wiley"
    assert by[("teaching_alpha", "10.9999/unlisted.2020.5")]["year"] == ""
    hand = by[("teaching_alpha", "10.1123/ijspp.2019-0001")]
    assert hand["parsed"] is False and hand["publisher"] == "Human Kinetics"
    assert by[("teaching_alpha", "10.1152/japplphysiol.00001.2019")]["checked"] is True
    assert by[("research_beta", "10.1016/j.z.2017.03.003")]["checked"] is True
    assert by[("research_beta", "10.1152/japplphysiol.00001.2019")]["checked"] is False


def test_oa_blocked_order_is_deterministic(oa_world):
    rows = WL.oa_blocked(oa_world.registry)
    assert [r["project"] for r in rows] == ["teaching_alpha"] * 7 + ["research_beta"] * 4
    for proj in ("teaching_alpha", "research_beta"):
        keys = [r["key"] for r in rows if r["project"] == proj]
        assert keys == sorted(keys)
    assert rows == WL.oa_blocked(oa_world.registry)
    only = WL.oa_blocked(oa_world.registry, projects=["research_beta"])
    assert {r["project"] for r in only} == {"research_beta"} and len(only) == 4
    with pytest.raises(WL.WorklistError):
        WL.oa_blocked(oa_world.registry, projects=["nobody"])


def test_group_by_host_and_publisher(oa_world):
    groups = WL.group_by_host(WL.oa_blocked(oa_world.registry))
    # one open DOI each, so by label; the group with nothing open is last
    assert list(groups) == ["10.9999", "Elsevier", "europepmc", "Human Kinetics", "osf", "Taylor & Francis",
                            "Wiley", "American Physiological Society"]
    assert {r["key"] for r in groups["Elsevier"]} == {"10.1016/j.x.2020.01.001", "10.1016/j.z.2017.03.003"}
    assert [r["project"] for r in groups["Elsevier"]] == ["research_beta", "teaching_alpha", "research_beta"]
    assert groups == WL.group_by_host(list(reversed(WL.oa_blocked(oa_world.registry))))


def test_render_lists_open_rows_once_with_one_click_links(oa_world):
    text = WL.render_oa_worklist(WL.group_by_host(WL.oa_blocked(oa_world.registry)), "2026-10-06")
    assert text.startswith("# Open-access worklist, 2026-10-06\n")
    assert "7 open papers in 7 groups" in text and "2 done" in text
    assert "## Elsevier: 1 open, 1 done" in text
    # one line per DOI; its cause and via are the first listing's (by project), every project is named
    assert ("- [ ] **An Elsevier paper** (2020) [10.1016/j.x.2020.01.001](https://doi.org/10.1016/j.x.2020.01.001) "
            "cause `HTTP_403` via `unpaywall` for research_beta, teaching_alpha") in text
    assert text.count("[10.1016/j.x.2020.01.001]") == 1
    assert text.index("## 10.9999: 1 open") < text.index("## Elsevier") < text.index("## Wiley")
    assert "[10.31219/osf.io/abc12](https://osf.io/abc12/)" in text
    assert "(https://pmc.ncbi.nlm.nih.gov/articles/PMC555/)" in text
    assert "10.1016/j.z.2017.03.003" not in text                       # checked off: done
    assert "10.1152/japplphysiol.00001.2019" not in text               # checked off in one project: done
    assert "American Physiological Society" not in text                # a group with nothing open
    assert "[10.1123/ijspp.2019-0001](https://doi.org/10.1123/ijspp.2019-0001)" in text
    for bad in ("—", "–", "→"):
        assert bad not in text


def test_held_rows_are_done_with_a_holdmap(oa_world):
    oa_world.hold("research_beta", "2016_Held", "10.1080/abc.2016.123")
    hm = holdings.build(oa_world.registry, use_cache=False, write_cache=False)
    rows = WL.oa_blocked(oa_world.registry, holdmap=hm)
    assert [r["held"] for r in rows if r["key"] == "10.1080/abc.2016.123"] == [True]
    text = WL.render_oa_worklist(WL.group_by_host(rows), "2026-10-06")
    assert "10.1080/abc.2016.123" not in text and "6 open papers" in text


def test_parse_oa_line_tolerates_a_link_with_parentheses():
    body = ("**A title** (1972) [10.1016/0021-9681(72)90027-6](https://doi.org/10.1016/0021-9681(72)90027-6) "
            "cause `HTTP_403` via `unpaywall`")
    r = WL.parse_oa_line(body)
    assert r["parsed"] and r["link"] == "https://doi.org/10.1016/0021-9681(72)90027-6"
    assert r["doi"] == "10.1016/0021-9681(72)90027-6" and r["host"] == ""


# ================================================================ ILL list
@pytest.fixture
def ill_world(tmp_path, monkeypatch, capsys):
    pf = Portfolio(tmp_path, monkeypatch, capsys, {"teaching_alpha": {"lib_dir": "literature"},
                                                   "research_beta": {"lib_dir": "literature"}})
    pf.hold("teaching_alpha", "S1", "10.1000/ta.2019.001")
    pf.hold("teaching_alpha", "S2", "10.1000/ta.2019.002")
    pf.hold("research_beta", "S4", "10.2000/rb.2018.001")
    c1, c2, c3, c4 = "10.5000/c1.2010.1", "10.5000/c2.2011.1", "10.5000/c3.2012.1", "10.5000/c4.2013.1"
    pf.reverse("teaching_alpha", [("S1", "10.1000/ta.2019.001", c1), ("S1", "10.1000/ta.2019.001", c2),
                                  ("S1", "10.1000/ta.2019.001", c3), ("S2", "10.1000/ta.2019.002", c1),
                                  ("S2", "10.1000/ta.2019.002", c2)])
    pf.reverse("research_beta", [("S4", "10.2000/rb.2018.001", c2), ("S4", "10.2000/rb.2018.001", c4)])
    pf.forward("research_beta", [("10.2000/rb.2018.001", c3)])
    # c4 cites two teaching seeds: three seeds point to it, yet only one held paper cites it, so
    # co-citation and seed order disagree (c1 before c4 only when co-citation ranks first)
    pf.forward("teaching_alpha", [("10.1000/ta.2019.001", c4), ("10.1000/ta.2019.002", c4)])
    pf.index()
    ill = M._HEADERS[M.ILL_NAME]
    ta = [{"doi": d, "title": f"Closed {d}", "year": "2010"} for d in (c1, c2, c3, "10.5000/c9.2015.9")]
    ta.append({"doi": "10.2000/RB.2018.001", "title": "Now held elsewhere", "year": "2018"})
    (pf.proot("teaching_alpha") / WL.ILL_NAME).write_text(
        ill("teaching_alpha") + M.render_md_block("teaching_alpha", "2026-09-01", ta), encoding="utf-8")
    rb = [{"doi": d, "title": f"Closed {d}", "year": "2011"} for d in (c2, c4)]
    (pf.proot("research_beta") / WL.ILL_NAME).write_text(
        ill("research_beta") + M.render_md_block("research_beta", "2026-09-02", rb)
        + "- [ ] **Hand line** (2015) [https://doi.org/10.5000/c6.2015.1](https://doi.org/10.5000/c6.2015.1)\n"
        + "- [ ] **A note with no DOI** (ask the librarian)\n"
        + "- [x] **Got it by ILL** (2014) — DOI `10.5000/c7.2014.1` — closed\n", encoding="utf-8")
    pf.c = (c1, c2, c3, c4)
    return pf


def test_ill_list_ranks_by_cocitation_then_seeds_then_doi(ill_world):
    c1, c2, c3, c4 = ill_world.c
    con = ill_world.con()
    try:
        rows = WL.ill_list(ill_world.registry, con=con)
        again = WL.ill_list(ill_world.registry, con=con)
        with_held = WL.ill_list(ill_world.registry, con=con, include_held=True)
    finally:
        con.close()
    assert rows == again
    got = [(r["doi"], r["n_own_citing"], r["n_seeds_pointing"], r["projects"]) for r in rows]
    assert got == [
        (c2, 2, 3, ["research_beta", "teaching_alpha"]),
        (c1, 2, 2, ["teaching_alpha"]),
        (c4, 1, 3, ["research_beta"]),
        (c3, 1, 2, ["teaching_alpha"]),
        ("10.5000/c6.2015.1", 0, 0, ["research_beta"]),
        ("10.5000/c9.2015.9", 0, 0, ["teaching_alpha"]),
    ]
    assert rows[0]["n_own_citing_by_project"] == {"research_beta": 1, "teaching_alpha": 2}
    assert rows[4]["link"] == "https://doi.org/10.5000/c6.2015.1" and rows[4]["title"] == "Hand line"
    assert all("c7.2014" not in r["key"] for r in with_held)                   # checked off
    held = [r for r in with_held if r["key"] == "10.2000/rb.2018.001"]
    assert len(held) == 1 and held[0]["held_anywhere"] is True
    assert len(with_held) == len(rows) + 1


def test_ill_list_without_the_view_is_an_error(ill_world, tmp_path):
    import duckdb
    db = tmp_path / "old.duckdb"
    con = duckdb.connect(str(db))
    con.execute("CREATE TABLE top_candidates (doi VARCHAR, n_seeds_pointing INTEGER)")
    con.close()
    con = duckdb.connect(str(db), read_only=True)
    try:
        with pytest.raises(WL.WorklistError, match="project_cocitations"):
            WL.ill_list(ill_world.registry, con=con)
    finally:
        con.close()


def test_render_ill_list(ill_world):
    con = ill_world.con()
    try:
        rows = WL.ill_list(ill_world.registry, con=con)
    finally:
        con.close()
    text = WL.render_ill_list(rows, "2026-10-06")
    assert text.startswith("# ILL worklist, 2026-10-06\n")
    assert ("1. **Closed 10.5000/c2.2011.1** (2010) [10.5000/c2.2011.1](https://doi.org/10.5000/c2.2011.1): "
            "co-cited 2, seeds 3; for research_beta, teaching_alpha") in text
    assert "—" not in text


# ================================================================ seed coverage
def test_seed_coverage(tmp_path, monkeypatch, capsys):
    pf = Portfolio(tmp_path, monkeypatch, capsys, {"teaching_alpha": {"lib_dir": "literature"},
                                                   "research_beta": {"lib_dir": "literature"}})
    lib = pf.lib("teaching_alpha")
    for name in ("a.pdf", "B.pdf", "c.PDF", "d.pdf", "e.pdf"):
        (lib / name).write_bytes(b"%PDF-1.4 stub\n")
    rows = [{"seed": s, "doi": "10.5000/x.2010.1"} for s in ("a.pdf", "b.pdf", "d.txt", "zzz.pdf", "a.pdf")]
    W.write_csv(lib / WL.REVERSE_PARSED, REVERSE_FIELDS, rows)
    cov = WL.seed_coverage("teaching_alpha", pf.registry)
    assert (cov["n_pdfs"], cov["n_parsed"], cov["share"], cov["warn"], cov["unparsed"]) == (5, 3, 0.6, True, ["c", "e"])
    W.write_csv(lib / WL.REVERSE_PARSED, REVERSE_FIELDS, rows + [{"seed": "C.pdf", "doi": "10.5000/x.2010.2"}])
    cov = WL.seed_coverage("teaching_alpha", pf.registry)
    assert (cov["share"], cov["warn"], cov["unparsed"]) == (0.8, False, ["e"])     # 80 % does not warn
    (lib / WL.REVERSE_PARSED).unlink()
    cov = WL.seed_coverage("teaching_alpha", pf.registry)
    assert (cov["n_parsed"], cov["share"], cov["warn"], cov["csv_exists"]) == (0, 0.0, True, False)
    cov = WL.seed_coverage("research_beta", pf.registry)
    assert (cov["n_pdfs"], cov["share"], cov["warn"]) == (0, None, False)
    with pytest.raises(WL.WorklistError):
        WL.seed_coverage("nobody", pf.registry)


# ================================================================ residual CSVs
def test_residual_csvs_attribution_and_skips(tmp_path, monkeypatch, capsys):
    pf = Portfolio(tmp_path, monkeypatch, capsys, {
        "research_beta": {"lib_dir": "literature"},
        "research_beta/sub_study": {"parent": "research_beta", "lib_dir": "sub_study/literature"}})
    fields = ["doi", "residual_class"]
    base, sub = pf.proot("research_beta"), pf.proot("research_beta/sub_study")
    W.write_csv(base / "lit_pull_queue.2026-09-01.residual.csv", fields, [{"doi": "10.5000/a.2010.1", "residual_class": "TERMINAL_CLOSED"}])
    W.write_csv(base / "runs" / "lit_pull_queue.2026-09-02.residual.csv", fields, [{"doi": "10.5000/b.2010.1", "residual_class": "TRANSIENT"}])
    W.write_csv(base / "Archive" / "lit_pull_queue.2026-01-01.residual.csv", fields, [{"doi": "10.5000/c.2010.1"}])
    W.write_csv(base / "_archive" / "lit_pull_queue.2026-01-02.residual.csv", fields, [{"doi": "10.5000/c.2010.2"}])
    W.write_csv(sub / "lit_pull_queue.2026-09-03.residual.csv", ["doi", "title"], [{"doi": "10.5000/d.2010.1"}])
    W.write_csv(sub / "deeper" / "x" / "lit_pull_queue.2026-09-04.residual.csv", fields, [{"doi": "10.5000/e.2010.1"}])
    W.write_csv(base / "lit_pull_queue.2026-09-01.report.csv", fields, [{"doi": "10.5000/f.2010.1"}])
    got = [(k, p.relative_to(pf.w.root).as_posix()) for k, p in WL.residual_csvs(pf.registry)]
    assert got == [
        ("research_beta", "research_beta/lit_pull_queue.2026-09-01.residual.csv"),
        ("research_beta", "research_beta/runs/lit_pull_queue.2026-09-02.residual.csv"),
        ("research_beta/sub_study", "research_beta/sub_study/lit_pull_queue.2026-09-03.residual.csv"),
    ]
    rows, st = WL.read_residuals(WL.residual_csvs(pf.registry))
    assert [(p, r["doi"]) for p, r in rows] == [("research_beta", "10.5000/a.2010.1"),
                                                ("research_beta/sub_study", "10.5000/d.2010.1")]
    assert st == {"csvs": 3, "legacy_csvs": 1, "rows_read": 3, "rows_kept": 2, "excluded": {"TRANSIENT": 1},
                  "unreadable": []}


# ================================================================ pool drawdown
POOL_FIELDS = ["doi", "title", "score"]


def make_pool(path, dois):
    W.write_csv(path, POOL_FIELDS, [{"doi": d, "title": f"T {d}", "score": 100 - i} for i, d in enumerate(dois)])
    return path


SIX = [f"10.5000/p.2020.{i}" for i in range(1, 7)]


def test_three_batches_of_two_are_disjoint(tmp_path):
    pool = WL.Pool(make_pool(tmp_path / "pool.csv", SIX), holdings=holdings.HoldMap())
    batches = []
    for n in range(3):
        b = [r["doi"] for r in pool.next_batch(2)]
        pool.mark_staged(b, f"2026-10-0{n + 1}", str(tmp_path / f"batch{n}.csv"))
        batches.append(b)
    assert batches == [SIX[0:2], SIX[2:4], SIX[4:6]]
    assert pool.next_batch(2) == []
    assert pool.status()["pending"] == 6 and pool.status()["remaining"] == 0


def test_a_kill_after_mark_staged_leaves_the_batch_pending(tmp_path):
    p = make_pool(tmp_path / "pool.csv", SIX)
    batch = tmp_path / "batch1.csv"
    batch.write_text("doi\n", encoding="utf-8")
    first = WL.Pool(p, holdings=holdings.HoldMap())
    rows = first.next_batch(2)
    assert first.next_batch(2) == rows                          # pure: nothing recorded yet
    first.mark_staged([r["doi"] for r in rows], "2026-10-06", str(batch))
    del first                                                   # the kill
    second = WL.Pool(p, holdings=holdings.HoldMap())
    assert second.pending() == [{"run_id": "2026-10-06", "batch_path": str(batch), "dois": SIX[:2], "exists": True}]
    assert [r["doi"] for r in second.next_batch(2)] == SIX[2:4]
    second.mark_swept(SIX[:2], "2026-10-06", {SIX[0]: "fetched", SIX[1].upper(): "TERMINAL_CLOSED"})
    assert second.pending() == []
    third = WL.Pool(p, holdings=holdings.HoldMap())
    assert third.status()["classes"] == {"TERMINAL_CLOSED": 1, "fetched": 1}
    assert [r["doi"] for r in third.next_batch(10)] == SIX[2:]


def test_two_pools_on_one_file_share_state_after_a_write(tmp_path):
    p = make_pool(tmp_path / "pool.csv", SIX)
    a, b = WL.Pool(p, holdings=holdings.HoldMap()), WL.Pool(p, holdings=holdings.HoldMap())
    a.mark_staged([SIX[0]], "r1", "b1.csv")
    assert [r["doi"] for r in b.next_batch(1)] == [SIX[1]]
    b.mark_staged([SIX[1]], "r1", "b1.csv")
    assert [x["dois"] for x in a.pending()] == [[SIX[0], SIX[1]]]
    assert a.pending()[0]["exists"] is False


def test_an_edited_pool_keeps_per_doi_state(tmp_path):
    p = make_pool(tmp_path / "pool.csv", SIX)
    pool = WL.Pool(p, holdings=holdings.HoldMap())
    pool.mark_staged(SIX[:2], "r1", "b1.csv")
    pool.mark_swept([SIX[0]], "r1", {SIX[0]: "fetched"})
    assert pool.status()["pool_changed"] is False
    new = "10.5000/p.2020.9"
    make_pool(p, [new, SIX[5], SIX[1].upper(), SIX[4], SIX[0], SIX[3], SIX[2]])   # reordered, one added
    st = pool.status()
    assert st["pool_changed"] is True and st["rows"] == 7
    assert [r["doi"] for r in pool.next_batch(3)] == [new, SIX[5], SIX[4]]
    assert pool.pending()[0]["dois"] == [SIX[1]]
    pool.mark_staged([new], "r2", "b2.csv")
    assert pool.status()["pool_changed"] is False                     # the hash is re-recorded on a write


def test_seed_from_imports_once_and_never_pends(tmp_path):
    p = make_pool(tmp_path / "pool.csv", SIX)
    vap = tmp_path / "state.json"
    vap.write_text(json.dumps({"consumed": 3, "batches": [["x"]], "staged_dois": [SIX[0], SIX[2].upper(), "", SIX[4]]}),
                   encoding="utf-8")
    pool = WL.Pool(p, holdings=holdings.HoldMap())
    pool.mark_staged([SIX[4]], "r1", "b1.csv")
    dry = pool.seed_from(vap, dry_run=True)
    assert (dry["imported"], dry["already_tracked"], dry["dry_run"]) == (2, 1, True)
    assert pool.status()["seeded_from"] == []
    first = pool.seed_from(vap)
    assert (first["imported"], first["already_tracked"], first["already_seeded"]) == (2, 1, False)
    second = WL.Pool(p, holdings=holdings.HoldMap()).seed_from(vap)
    assert (second["imported"], second["already_seeded"]) == (0, True)
    st = pool.status()
    assert st["seeded_from"] == [first["sha256"]] and st["classes"] == {"imported": 2}
    assert [x["dois"] for x in pool.pending()] == [[SIX[4]]]
    assert [r["doi"] for r in pool.next_batch(10)] == [SIX[1], SIX[3], SIX[5]]


def test_held_rows_are_skipped_through_holdings(tmp_path, monkeypatch, capsys):
    pf = Portfolio(tmp_path, monkeypatch, capsys, {"teaching_alpha": {"lib_dir": "literature"}})
    pf.hold("teaching_alpha", "held", SIX[1].upper())
    p = make_pool(tmp_path / "pool.csv", SIX)
    assert [r["doi"] for r in WL.Pool(p, registry=pf.registry).next_batch(2)] == [SIX[0], SIX[2]]
    hm = holdings.build(pf.registry, use_cache=False, write_cache=False)
    assert [r["doi"] for r in WL.Pool(p, holdings=hm).next_batch(2)] == [SIX[0], SIX[2]]
    assert [r["doi"] for r in WL.Pool(p).next_batch(2, exclude_held=False)] == SIX[:2]
    with pytest.raises(WL.WorklistError, match="registry= or holdings="):
        WL.Pool(p).next_batch(2)


def test_pool_state_file_and_errors(tmp_path):
    p = make_pool(tmp_path / "teaching_pool.csv", SIX)
    pool = WL.Pool(p, holdings=holdings.HoldMap())
    assert pool.state_path == tmp_path / "teaching_pool.drawdown.json"
    pool.mark_staged([SIX[0]], "r1", "b1.csv")
    raw = pool.state_path.read_bytes()
    assert b"\r\n" not in raw
    st = json.loads(raw)
    assert st["pool_sha256"] and st["seeded_from"] == [] and st["dois"]["10.5000/p.2020.1"]["staged"]["run_id"] == "r1"
    with pytest.raises(WL.WorklistError, match="no class"):
        pool.mark_swept([SIX[0], SIX[1]], "r1", {SIX[0]: "fetched"})
    with pytest.raises(WL.WorklistError):
        pool.mark_staged([SIX[1]], "", "b.csv")
    pool.state_path.write_text("{not json", encoding="utf-8")
    with pytest.raises(WL.PoolStateError):
        pool.next_batch(1)
    W.write_csv(tmp_path / "nodoi.csv", ["title"], [{"title": "x"}])
    with pytest.raises(WL.WorklistError, match="no `doi` column"):
        WL.Pool(tmp_path / "nodoi.csv").next_batch(1, exclude_held=False)


# ================================================================ links
def test_doi_link():
    assert WL.doi_link("10.1000/abc<def") == "https://doi.org/10.1000/abc%3Cdef"
    assert WL.doi_link("10.1088/2053-1591/acdecd") == "https://doi.org/10.1088/2053-1591/acdecd"
    assert WL.doi_link("https://doi.org/10.1234/ABC.2020.1") == "https://doi.org/10.1234/abc.2020.1"
    assert WL.doi_link(SICI) == M.doi_url(SICI)
    assert WL.publisher("10.1016/j.x.2020.01.001") == "Elsevier" and WL.publisher("10.77777/x.1") == "10.77777"
    assert WL.publisher("not a doi") == "unknown"


# ================================================================ CLI
def test_cli_oa_blocked_is_dry_by_default(oa_world, tmp_path, capsys):
    assert WL.main(["oa-blocked", "--date", "2026-10-06"]) == 0
    out = capsys.readouterr().out
    assert "7 open, 2 done, 8 groups" in out and "dry run: nothing written" in out
    target = tmp_path / "out" / "oa_worklist.md"
    assert not target.exists()
    assert WL.main(["oa-blocked", "--date", "2026-10-06", "--write", str(target)]) == 0
    text = target.read_text(encoding="utf-8")
    assert text == WL.render_oa_worklist(WL.group_by_host(WL.oa_blocked(
        oa_world.registry, holdmap=holdings.build(oa_world.registry, use_cache=False, write_cache=False))), "2026-10-06")
    assert b"\r\n" not in target.read_bytes()
    assert not (Path(oa_world.registry["state_dir"]) / holdings.CACHE_NAME).exists()   # holdings cache untouched


def test_cli_ill_coverage_and_errors(ill_world, tmp_path, capsys):
    assert WL.main(["ill", "--db", str(ill_world.w.db), "--date", "2026-10-06"]) == 0
    out = capsys.readouterr().out
    assert "6 open ILL rows" in out and "dry run" in out
    target = tmp_path / "ill.md"
    assert WL.main(["ill", "--db", str(ill_world.w.db), "--write", str(target), "--limit", "2"]) == 0
    assert target.read_text(encoding="utf-8").count("\n1. ") == 1 and "\n3. " not in target.read_text(encoding="utf-8")
    assert WL.main(["ill", "--db", str(tmp_path / "missing.duckdb")]) == 1
    assert "no index at" in capsys.readouterr().err
    assert WL.main(["coverage"]) == 0
    out = capsys.readouterr().out
    assert "[WARN] teaching_alpha: 0/2 PDFs parsed" not in out and "teaching_alpha: 2/2" in out
    assert WL.main(["oa-blocked", "--project", "nobody"]) == 1
    assert "nobody" in capsys.readouterr().err


def test_cli_missing_registry_exits_1(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(WL, "CONFIG_PATH", tmp_path / "none" / "projects.json")
    assert WL.main(["coverage"]) == 1
    assert "projects.json not found" in capsys.readouterr().err


def test_cli_pool_status_seed_state(tmp_path, capsys):
    p = make_pool(tmp_path / "pool.csv", SIX)
    vap = tmp_path / "vap_state.json"
    vap.write_text(json.dumps({"staged_dois": SIX[:3]}), encoding="utf-8")
    assert WL.main(["pool-status", "--pool", str(p), "--seed-state", str(vap)]) == 0
    assert "would import 3 DOIs" in capsys.readouterr().out
    assert not WL.state_path_for(p).exists()
    assert WL.main(["pool-status", "--pool", str(p), "--seed-state", str(vap), "--write"]) == 0
    assert "imported 3 DOIs" in capsys.readouterr().out
    assert WL.main(["pool-status", "--pool", str(p), "--seed-state", str(vap), "--write"]) == 0
    out = capsys.readouterr().out
    assert "already imported" in out and "remaining: 3" in out
    assert WL.main(["pool-status", "--pool", str(tmp_path / "missing.csv")]) == 1


def test_cli_help_exits_0():
    r = subprocess.run([sys.executable, "-m", "litpipe.worklists", "--help"], cwd=str(REPO),
                       capture_output=True, text=True, timeout=60)
    assert r.returncode == 0 and "oa-blocked" in r.stdout
