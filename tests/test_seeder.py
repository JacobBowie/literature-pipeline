"""W3-C2: the seeder (seed_queue_from_top_candidates) on temp indexes built by the REAL index_portfolio.

- Destination (REG-I07): the registry library relative to lit_util.project_root, for every key
  shape (every live subproject shape, a dot, a space, a nested tail, lib_dir "."); sweep's own
  destination check accepts it; a library outside its project root exits 1 unless --destination.
  The draft lands at lit_util.project_root, not PROJECTS_ROOT / key.
- Ranking (REG-I29): --rank project (default) counts the project's own seeds (equality on
  source_project, so a parent's count excludes its subprojects); --rank portfolio is the old
  order; --rank cocitation reproduces index_portfolio.project_cocitations without held_anywhere
  rows; --scope reads only that (project, scope). Held DOIs (text-only included) leave every rank.
- Undated candidates are dropped and counted, or kept with --include-undated (last under
  --recent-first). Draft names never match sweep's queue discovery; the renamed ones do.
- Registry and DB: exit 1 for a missing registry, an unknown key, no index, or an index without
  the view/table a rank needs; the DB resolves at call time (never the live one under a patched
  root); the last stdout line is "[step-summary] {json}".
"""
import csv
import json
import shutil
import sys
from pathlib import Path

import duckdb
import pytest

import index_portfolio as I
import lit_util
import seed_queue_from_top_candidates as S
import sweep
from litpipe import config

FIX = Path(__file__).resolve().parent / "fixtures" / "W3-C2"
SHAPES = json.loads((FIX / "registry_shapes.json").read_text(encoding="utf-8"))["projects"]

REVERSE_FIELDS = ["seed", "first_author", "year", "title_snippet", "doi", "raw", "seed_doi", "source"]
FORWARD_FIELDS = ["seed_pdf", "seed_doi", "citing_doi", "citing_title", "citing_year", "citing_authors",
                  "citing_venue", "citing_cited_by"]
SCOPED_FIELDS = ["seed_doi", "seed_label", "seed_chapter", "citing_doi", "citing_title", "citing_year",
                 "citing_authors", "citing_venue", "citing_cited_by"]


def ris(doi, title="A held paper", py="2019"):
    return f"TY  - JOUR\nAU  - Smith, Jane\nPY  - {py}\nTI  - {title}\nJO  - A Journal\nDO  - {doi}\nER  - \n"


def jats_sidecar(doi):
    """The pre-W2a JATS text-only shape (no has_pdf key, not extracted from a PDF)."""
    return json.dumps({"pmcid": "PMC100001", "doi": doi, "title": "Text only paper", "year": "2016",
                       "journal": "A Journal", "authors": ["Morgan Avery-Lee"], "text": "Body text " * 30})


def summary_of(out):
    last = out.rstrip("\n").splitlines()[-1]
    assert last.startswith(S.SUMMARY_MARKER), last
    return json.loads(last[len(S.SUMMARY_MARKER):])


class World:
    """A temp projects root, registry and index. Libraries hold PDFs with a .ris each; walker CSVs
    are written where index_portfolio reads them, and index() runs the real CLI in-process."""

    def __init__(self, tmp_path, monkeypatch, capsys):
        self.tmp, self.capsys = tmp_path, capsys
        self.root = tmp_path / "root"
        self.root.mkdir()
        self.cfg = tmp_path / "projects.json"
        self.db = tmp_path / "refs" / "portfolio.duckdb"
        self.projects = {}
        self.top = {}
        self.n = 0
        monkeypatch.setattr(lit_util, "PROJECTS_ROOT", self.root)
        monkeypatch.setattr(I, "CONFIG_PATH", self.cfg)
        monkeypatch.setattr(S, "CONFIG_PATH", self.cfg)
        monkeypatch.setattr(config, "CONFIG_PATH", self.cfg)
        self._write()

    def _write(self):
        self.cfg.write_text(json.dumps({"state_dir": str(self.tmp / "state"), "projects": self.projects,
                                        **self.top}), encoding="utf-8")

    def register(self, key, lib_dir="literature", parent=None, **extra):
        entry = {"lib_dir": lib_dir, **({"parent": parent} if parent else {}), **extra}
        self.projects[key] = entry
        self._write()
        lib = lit_util.lib_paths(key, entry)[1]
        lib.mkdir(parents=True, exist_ok=True)
        return lib

    def lib(self, key):
        return lit_util.lib_paths(key, self.projects[key])[1]

    def proot(self, key):
        return lit_util.project_root(key, self.projects[key])

    def hold(self, key, *dois):
        for d in dois:
            self.n += 1
            stem = f"2019_Held{self.n:04d}"
            (self.lib(key) / f"{stem}.pdf").write_bytes(b"%PDF-1.4 stub")
            (self.lib(key) / f"{stem}.ris").write_text(ris(d), encoding="utf-8")

    def text_only(self, key, doi):
        self.n += 1
        (self.lib(key) / f"2016_Text{self.n:04d}.fulltext.json").write_text(jats_sidecar(doi), encoding="utf-8")

    def _csv(self, path, fields, rows):
        with open(path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(rows)

    def reverse(self, key, edges, years=None, titles=None):
        """edges: (seed_doi, cited_doi) pairs; the seeds should be held by `key`."""
        years, titles = years or {}, titles or {}
        rows = [{"seed": f"{s}.pdf", "first_author": "Lee", "year": years.get(d, 2020),
                 "title_snippet": titles.get(d, f"Paper {d}"), "doi": d, "raw": "ref", "seed_doi": s,
                 "source": "s2"} for s, d in edges]
        self._csv(self.lib(key) / "_reverse_citations_parsed.csv", REVERSE_FIELDS, rows)

    def forward(self, key, edges, years=None, titles=None, cites=None):
        """edges: (seed_doi, citing_doi) pairs."""
        years, titles, cites = years or {}, titles or {}, cites or {}
        rows = [{"seed_pdf": "s.pdf", "seed_doi": s, "citing_doi": d, "citing_title": titles.get(d, f"Paper {d}"),
                 "citing_year": years.get(d, 2021), "citing_authors": "Avery, Kim", "citing_venue": "V",
                 "citing_cited_by": cites.get(d, 5)} for s, d in edges]
        self._csv(self.lib(key) / "_forward_citations.csv", FORWARD_FIELDS, rows)

    def scoped(self, key, scope, edges, years=None, cites=None):
        years, cites = years or {}, cites or {}
        rows = [{"seed_doi": s, "seed_label": "L", "seed_chapter": "1", "citing_doi": d,
                 "citing_title": f"Scoped {d}", "citing_year": years.get(d, 2022), "citing_authors": "Avery, Kim",
                 "citing_venue": "V", "citing_cited_by": cites.get(d, 1)} for s, d in edges]
        self._csv(self.lib(key) / f"_{scope}_forward_citations.csv", SCOPED_FIELDS, rows)

    def index(self, db=None):
        rc = I.main(["--db", str(db or self.db)])
        out = self.capsys.readouterr().out
        assert rc == 0, out
        return out

    def seed(self, *argv, db=True):
        args = (["--db", str(self.db)] if db else []) + list(argv)
        rc = S.main(args)
        cap = self.capsys.readouterr()
        return rc, cap.out, cap.err

    def draft(self, path):
        text = Path(path).read_text(encoding="utf-8")
        head = [ln for ln in text.splitlines() if ln.startswith("#")]
        fields, rows = sweep.read_queue(path)
        return head, fields, rows

    def q(self, sql, params=()):
        con = duckdb.connect(str(self.db), read_only=True)
        try:
            return con.execute(sql, list(params)).fetchall()
        finally:
            con.close()


@pytest.fixture
def world(tmp_path, monkeypatch, capsys):
    return World(tmp_path, monkeypatch, capsys)


def dois_of(rows):
    return [r["doi"] for r in rows]


def notes_of(row):
    return dict(p.split("=", 1) for p in row["notes"].split("; "))


# ================================================================ destination (REG-I07)
@pytest.mark.parametrize("key", sorted(SHAPES))
def test_destination_for_every_key_shape_matches_sweeps_check(tmp_path, monkeypatch, key):
    root = tmp_path / "root"
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    registry = {k: {kk: vv for kk, vv in v.items() if kk != "expect"} for k, v in SHAPES.items()}
    entry = registry[key]
    dest, why = S.destination_for(key, entry)
    assert dest == SHAPES[key]["expect"], why
    proot = lit_util.project_root(key, entry)
    lib = lit_util.lib_paths(key, entry)[1]
    if dest is None:
        assert "outside the project root" in why
        return
    assert dest.endswith("/") and "\\" not in dest
    assert (proot / dest).resolve() == lib.resolve()
    (proot / dest).resolve().relative_to(proot.resolve())          # inside the project root
    # sweep's own check (run_pipeline, dry run: nothing written, nothing sent) accepts it
    lib.mkdir(parents=True, exist_ok=True)
    proot.mkdir(parents=True, exist_ok=True)
    q = proot / "lit_pull_queue.csv"
    q.write_text(f"doi,title,authors,year,destination,notes\n10.5555/x.0001,T,A,2020,{dest},n\n", encoding="utf-8")
    res = sweep.run_pipeline(proot, q, dry_run=True, run_date="2026-10-06", key=key, registry=registry)
    assert res is not None and Path(res["destination"]) == lib.resolve()


def test_the_old_destination_doubled_a_subproject_tail_and_sweep_refuses_it(tmp_path, monkeypatch):
    """The pre-W3-C2 seeder wrote lib_dir + "/" for every key: under a subproject's root that is a
    shadow library, which sweep's REG-I07 backstop refuses."""
    root = tmp_path / "root"
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    key, entry = "Research_Data/sub_a", {"lib_dir": "sub_a/literature", "parent": "Research_Data"}
    proot = lit_util.project_root(key, entry)
    proot.mkdir(parents=True)
    q = proot / "lit_pull_queue.csv"
    q.write_text("doi,title,authors,year,destination,notes\n10.5555/x.0001,T,A,2020,sub_a/literature/,n\n",
                 encoding="utf-8")
    assert sweep.run_pipeline(proot, q, dry_run=True, key=key, registry={key: entry}) is None
    assert S.destination_for(key, entry)[0] == "literature/"


def test_subproject_draft_has_the_registry_destination_and_sweep_takes_it(world):
    world.register("Research_Data", "docs/literature")
    world.register("Research_Data/sub_a", "sub_a/literature", parent="Research_Data")
    world.hold("Research_Data/sub_a", "10.5555/seed.0001")
    world.reverse("Research_Data/sub_a", [("10.5555/seed.0001", "10.5555/cand.0001")])
    world.hold("Research_Data", "10.5555/seed.0100")
    world.index()
    rc, out, err = world.seed("--project", "Research_Data/sub_a")
    assert rc == 0, err
    draft = world.proot("Research_Data/sub_a") / "lit_pull_queue.draft.csv"
    assert summary_of(out)["output"] == str(draft) and draft.exists()
    head, fields, rows = world.draft(draft)
    assert dois_of(rows) == ["10.5555/cand.0001"] and {r["destination"] for r in rows} == {"literature/"}
    staged = draft.with_name("lit_pull_queue.csv")
    shutil.copy(draft, staged)
    projects = json.loads(world.cfg.read_text(encoding="utf-8"))["projects"]
    res = sweep.run_pipeline(world.proot("Research_Data/sub_a"), staged, dry_run=True,
                             key="Research_Data/sub_a", registry=projects)
    assert res is not None and Path(res["destination"]) == world.lib("Research_Data/sub_a").resolve()
    assert not (world.proot("Research_Data/sub_a") / "sub_a").exists()          # no shadow library


def test_output_path_comes_from_project_root_not_projects_root_slash_key(world):
    """A subproject key that does not repeat its parent resolves under the parent (project_root);
    PROJECTS_ROOT / key would be a directory no tool reads."""
    world.register("Teach", "literature")
    world.register("sub", "sub/literature", parent="Teach")
    world.hold("sub", "10.5555/seed.0001")
    world.reverse("sub", [("10.5555/seed.0001", "10.5555/cand.0001")])
    world.hold("Teach", "10.5555/seed.0100")
    world.index()
    rc, out, err = world.seed("--project", "sub")
    assert rc == 0, err
    assert world.proot("sub") == world.root / "Teach" / "sub"
    assert (world.root / "Teach" / "sub" / "lit_pull_queue.draft.csv").exists()
    assert not (world.root / "sub").exists()
    _, _, rows = world.draft(world.root / "Teach" / "sub" / "lit_pull_queue.draft.csv")
    assert {r["destination"] for r in rows} == {"literature/"}


def test_library_outside_its_project_root_exits_1_unless_destination(world):
    world.register("Teach", "literature")
    world.register("Teach/outside_sub", "literature", parent="Teach")     # the parent's library
    world.hold("Teach", "10.5555/seed.0001")
    world.index()
    rc, out, err = world.seed("--project", "Teach/outside_sub")
    assert rc == 1 and "outside the project root" in err and "--destination" in err
    assert summary_of(out)["exit_code"] == 1
    assert not (world.proot("Teach/outside_sub") / "lit_pull_queue.draft.csv").exists()
    rc, out, err = world.seed("--project", "Teach/outside_sub", "--destination", "../literature")
    assert rc == 0, err
    _, _, rows = world.draft(world.proot("Teach/outside_sub") / "lit_pull_queue.draft.csv")
    assert rows == []                                                    # it owns no walk; the draft is empty


# ================================================================ ranking (REG-I29)
def _big_and_small(world):
    """research_big: 40 seeds cite X. teaching_small: one seed cites X, three cite Y."""
    world.register("research_big", "literature")
    world.register("teaching_small", "literature")
    big = [f"10.5555/big.{i:04d}" for i in range(40)]
    small = [f"10.5555/small.{i:04d}" for i in range(3)]
    world.hold("research_big", *big)
    world.hold("teaching_small", *small)
    world.reverse("research_big", [(s, "10.5555/x.0040") for s in big])
    world.reverse("teaching_small", [(small[0], "10.5555/x.0040")] + [(s, "10.5555/y.0003") for s in small])
    world.index()


def test_small_project_ranks_by_its_own_seeds(world):
    _big_and_small(world)
    rc, out, err = world.seed("--project", "teaching_small", "--min-seeds", "1")
    assert rc == 0, err
    head, fields, rows = world.draft(world.proot("teaching_small") / "lit_pull_queue.draft.csv")
    assert dois_of(rows) == ["10.5555/y.0003", "10.5555/x.0040"]           # 3 own seeds above 1 own + 40 big
    x = notes_of(rows[1])
    assert (x["n_seeds_project"], x["n_seeds_portfolio"]) == ("1", "41")
    assert notes_of(rows[0])["n_seeds_project"] == "3"
    assert any(ln.startswith("# Order: n_seeds_project desc") for ln in head)
    # the default rank is project, with --min-seeds 1
    rc, out, err = world.seed("--project", "teaching_small")
    assert rc == 0 and summary_of(out)["rank"] == "project"
    _, _, rows2 = world.draft(world.proot("teaching_small") / "lit_pull_queue.draft.csv")
    assert dois_of(rows2) == dois_of(rows)
    # --min-seeds counts own seeds under --rank project: X (1 own, 41 portfolio) leaves
    rc, out, err = world.seed("--project", "teaching_small", "--min-seeds", "2")
    _, _, rows3 = world.draft(world.proot("teaching_small") / "lit_pull_queue.draft.csv")
    assert dois_of(rows3) == ["10.5555/y.0003"]


def test_portfolio_rank_is_the_old_order(world):
    _big_and_small(world)
    rc, out, err = world.seed("--project", "teaching_small", "--rank", "portfolio", "--min-seeds", "1")
    assert rc == 0, err
    _, _, rows = world.draft(world.proot("teaching_small") / "lit_pull_queue.draft.csv")
    assert dois_of(rows) == ["10.5555/x.0040", "10.5555/y.0003"]
    # default --min-seeds 3 under --rank portfolio; both pass (41 and 3)
    rc, out, err = world.seed("--project", "teaching_small", "--rank", "portfolio")
    assert summary_of(out)["min_seeds"] == 3
    rc, out, err = world.seed("--project", "teaching_small", "--rank", "portfolio", "--min-seeds", "4")
    _, _, rows = world.draft(world.proot("teaching_small") / "lit_pull_queue.draft.csv")
    assert dois_of(rows) == ["10.5555/x.0040"]


def test_portfolio_rank_reproduces_the_pre_w3c2_query(world):
    world.register("research_a", "literature")
    world.register("research_b", "literature")
    seeds_a = [f"10.5555/sa.{i:04d}" for i in range(6)]
    seeds_b = [f"10.5555/sb.{i:04d}" for i in range(6)]
    world.hold("research_a", *seeds_a)
    world.hold("research_b", *seeds_b)
    cands = [f"10.5555/c.{i:04d}" for i in range(12)]
    edges_a = [(seeds_a[j], c) for i, c in enumerate(cands) for j in range(i % 6 + 1)]
    edges_b = [(seeds_b[j], c) for i, c in enumerate(cands[:6]) for j in range(i % 3 + 1)]
    world.reverse("research_a", edges_a, years={cands[3]: 2005})
    world.forward("research_b", [(seeds_b[0], c) for c in cands[6:]], cites={c: i for i, c in enumerate(cands)})
    world.reverse("research_b", edges_b)
    world.index()
    rc, out, err = world.seed("--project", "research_a", "--rank", "portfolio", "--min-seeds", "2")
    assert rc == 0, err
    _, _, rows = world.draft(world.proot("research_a") / "lit_pull_queue.draft.csv")
    old = world.q("""
        SELECT t.doi FROM top_candidates t LEFT JOIN paper_metadata m ON m.doi = t.doi
        WHERE (',' || t.via_projects || ',') LIKE ? AND t.year >= ? AND t.n_seeds_pointing >= ?
          AND t.max_cited_by >= ? AND t.title IS NOT NULL AND substr(t.title, 1, 1) ~ '[A-Za-z]'
        ORDER BY t.n_seeds_pointing DESC, t.max_cited_by DESC, t.doi LIMIT ?""",
        ["%,research_a,%", 2010, 2, 0, 100])
    assert dois_of(rows) == [r[0] for r in old] and len(old) >= 8
    assert cands[3] not in dois_of(rows)                                # 2005 < --year-min


def test_parent_count_excludes_its_subprojects(world):
    world.register("Research_Data", "docs/literature")
    world.register("Research_Data/sub_a", "sub_a/literature", parent="Research_Data")
    world.hold("Research_Data", "10.5555/p.0001")
    subs = [f"10.5555/s.{i:04d}" for i in range(3)]
    world.hold("Research_Data/sub_a", *subs)
    world.reverse("Research_Data", [("10.5555/p.0001", "10.5555/z.0001")])
    world.reverse("Research_Data/sub_a", [(s, "10.5555/z.0001") for s in subs])
    world.index()
    rc, out, err = world.seed("--project", "Research_Data")
    assert rc == 0, err
    _, _, rows = world.draft(world.proot("Research_Data") / "lit_pull_queue.draft.csv")
    n = notes_of(rows[0])
    assert dois_of(rows) == ["10.5555/z.0001"] and (n["n_seeds_project"], n["n_seeds_portfolio"]) == ("1", "4")
    rc, out, err = world.seed("--project", "Research_Data/sub_a")
    _, _, rows = world.draft(world.proot("Research_Data/sub_a") / "lit_pull_queue.draft.csv")
    assert notes_of(rows[0])["n_seeds_project"] == "3"


def _cocitation_world(world):
    world.register("research_a", "literature")
    world.register("research_b", "literature")
    a = [f"10.5555/a.{i:04d}" for i in range(4)]
    world.hold("research_a", *a)
    world.hold("research_b", "10.5555/held.0001")
    edges = ([(s, "10.5555/x.0001") for s in a] + [(s, "10.5555/y.0001") for s in a[:3]]
             + [(a[0], "10.5555/w.0001"), (a[1], "10.5555/v.0001"), (a[2], "10.5555/u.0001")]
             + [(s, "10.5555/held.0001") for s in a]                       # held by research_b
             + [(a[0], a[1])])                                             # its own holding
    world.reverse("research_a", edges, years={"10.5555/u.0001": 2001})
    world.index()


def test_cocitation_rank_reproduces_project_cocitations_without_held_anywhere(world):
    _cocitation_world(world)
    rc, out, err = world.seed("--project", "research_a", "--rank", "cocitation", "--year-min", "1900")
    assert rc == 0, err
    head, _, rows = world.draft(world.proot("research_a") / "lit_pull_queue.draft.csv")
    con = duckdb.connect(str(world.db), read_only=True)
    try:
        view = I.project_cocitations(con, "research_a")
    finally:
        con.close()
    assert any(held for *_, held in view)                                 # the fixture has a held row
    assert dois_of(rows) == [d for _, d, n, held in view if not held]
    assert dois_of(rows) == ["10.5555/x.0001", "10.5555/y.0001", "10.5555/u.0001", "10.5555/v.0001",
                             "10.5555/w.0001"]
    assert notes_of(rows[0])["n_own_citing"] == "4"
    assert any("project_cocitations" in ln for ln in head)
    # the shared filters apply: --year-min drops the 2001 paper from the same order
    rc, out, err = world.seed("--project", "research_a", "--rank", "cocitation")
    _, _, rows = world.draft(world.proot("research_a") / "lit_pull_queue.draft.csv")
    assert "10.5555/u.0001" not in dois_of(rows)


def test_scope_reads_only_that_scope_in_its_order(world):
    world.register("teaching_a", "literature")
    world.register("teaching_b", "literature")
    world.hold("teaching_a", "10.5555/sa.0001", "10.5555/sa.0002", "10.5555/held.0009")
    world.hold("teaching_b", "10.5555/sb.0001")
    s1, s2 = "10.5555/sa.0001", "10.5555/sa.0002"
    world.scoped("teaching_a", "ch01",
                 [(s1, "10.5555/k.0001"), (s2, "10.5555/k.0001"),                  # 2 scope seeds
                  (s1, "10.5555/k.0002"), (s1, "10.5555/k.0003"), (s1, "10.5555/k.0004"),
                  (s1, "10.5555/held.0009")],
                 years={"10.5555/k.0002": 2019, "10.5555/k.0003": 2024, "10.5555/k.0004": 2024},
                 cites={"10.5555/k.0002": 50, "10.5555/k.0003": 3, "10.5555/k.0004": 3})
    world.scoped("teaching_a", "ch02", [(s1, "10.5555/other.0001")])
    world.scoped("teaching_b", "ch01", [("10.5555/sb.0001", "10.5555/bscope.0001")])
    world.forward("teaching_a", [(s1, "10.5555/plain.0001")])            # the unscoped walk
    world.index()
    rc, out, err = world.seed("--project", "teaching_a", "--scope", "ch01")
    assert rc == 0, err
    head, _, rows = world.draft(world.proot("teaching_a") / "lit_pull_queue.draft.csv")
    # (-n_seeds, -citing_cited_by, -year, doi); the held DOI and every other scope/project are absent
    assert dois_of(rows) == ["10.5555/k.0001", "10.5555/k.0002", "10.5555/k.0003", "10.5555/k.0004"]
    n = notes_of(rows[0])
    assert (n["scope"], n["n_seeds_scope"], n["src"]) == ("ch01", "2", "scope:ch01")
    assert any("by OA and tie-breaks" in ln for ln in head)
    assert summary_of(out)["scope"] == "ch01"


def test_scope_with_rank_and_unknown_scope_exit_1(world):
    world.register("teaching_a", "literature")
    world.hold("teaching_a", "10.5555/sa.0001")
    world.scoped("teaching_a", "ch01", [("10.5555/sa.0001", "10.5555/k.0001")])
    world.index()
    rc, out, err = world.seed("--project", "teaching_a", "--scope", "ch01", "--rank", "project")
    assert rc == 1 and "--rank does not apply" in err
    rc, out, err = world.seed("--project", "teaching_a", "--scope", "ch09")
    assert rc == 1 and "no scope 'ch09'" in err and "known: ch01" in err


def test_text_only_holding_appears_in_no_rank(world):
    """Amendment 8: a DOI held only as a text-only sidecar (has_pdf=false) is held."""
    world.register("research_a", "literature")
    world.register("research_b", "literature")
    a = [f"10.5555/a.{i:04d}" for i in range(3)]
    world.hold("research_a", *a)
    world.hold("research_b", "10.5555/b.0001")
    world.text_only("research_b", "10.5555/text.0001")
    world.reverse("research_a", [(s, "10.5555/text.0001") for s in a] + [(a[0], "10.5555/free.0001")])
    world.forward("research_a", [(a[1], "10.5555/text.0001")])
    world.scoped("research_a", "ch01", [(a[0], "10.5555/text.0001"), (a[0], "10.5555/free.0002")])
    world.index()
    assert world.q("SELECT project, has_pdf FROM paper_locations WHERE doi = '10.5555/text.0001'") == [
        ("research_b", False)]
    assert world.q("SELECT COUNT(*) FROM candidates WHERE doi = '10.5555/text.0001'")[0][0] == 4
    for argv, other in ((["--rank", "project"], "10.5555/free.0001"),
                        (["--rank", "portfolio", "--min-seeds", "1"], "10.5555/free.0001"),
                        (["--rank", "cocitation"], "10.5555/free.0001"),
                        (["--scope", "ch01"], "10.5555/free.0002")):
        rc, out, err = world.seed("--project", "research_a", *argv)
        assert rc == 0, (argv, err)
        _, _, rows = world.draft(world.proot("research_a") / "lit_pull_queue.draft.csv")
        assert other in dois_of(rows) and "10.5555/text.0001" not in dois_of(rows), argv


# ================================================================ undated, recent-first
def _dated_world(world):
    world.register("research_a", "literature")
    a = [f"10.5555/a.{i:04d}" for i in range(3)]
    world.hold("research_a", *a)
    edges = [(s, "10.5555/old.0001") for s in a] + [(a[0], "10.5555/new.0001"),
                                                    (a[0], "10.5555/undated.0001"),
                                                    (a[1], "10.5555/undated.0001"),
                                                    (a[0], "10.5555/mid.0001"), (a[1], "10.5555/mid.0001")]
    world.reverse("research_a", edges, years={"10.5555/old.0001": 2012, "10.5555/new.0001": 2025,
                                              "10.5555/undated.0001": "", "10.5555/mid.0001": 2018})
    world.index()


def test_undated_candidates_are_dropped_and_counted_then_kept_with_the_flag(world):
    _dated_world(world)
    assert world.q("SELECT year FROM paper_metadata WHERE doi = '10.5555/undated.0001'") == [(0,)]
    rc, out, err = world.seed("--project", "research_a")
    assert rc == 0, err
    head, _, rows = world.draft(world.proot("research_a") / "lit_pull_queue.draft.csv")
    assert "10.5555/undated.0001" not in dois_of(rows)
    assert "# Undated: 1 candidate(s) with no year dropped (--include-undated keeps them)" in head
    assert "undated: 1 candidate(s) with no year dropped" in out and summary_of(out)["undated"] == 1
    rc, out, err = world.seed("--project", "research_a", "--include-undated")
    head, _, rows = world.draft(world.proot("research_a") / "lit_pull_queue.draft.csv")
    assert dois_of(rows) == ["10.5555/old.0001", "10.5555/mid.0001", "10.5555/undated.0001", "10.5555/new.0001"]
    assert [r["year"] for r in rows if r["doi"] == "10.5555/undated.0001"] == [""]
    assert "# Undated: 1 candidate(s) with no year kept" in head


def test_recent_first_sorts_by_year_then_rank_with_undated_last(world):
    _dated_world(world)
    rc, out, err = world.seed("--project", "research_a", "--recent-first", "--include-undated")
    assert rc == 0, err
    head, _, rows = world.draft(world.proot("research_a") / "lit_pull_queue.draft.csv")
    assert dois_of(rows) == ["10.5555/new.0001", "10.5555/mid.0001", "10.5555/old.0001", "10.5555/undated.0001"]
    assert "# Order: year desc (undated last), then n_seeds_project desc, max_cited_by desc, " \
           "n_seeds_portfolio desc, doi" in head
    rc, out, err = world.seed("--project", "research_a")                    # off by default
    head, _, rows = world.draft(world.proot("research_a") / "lit_pull_queue.draft.csv")
    assert dois_of(rows)[0] == "10.5555/old.0001" and not any("year desc" in ln for ln in head)


# ================================================================ names, header, columns
def test_draft_names_never_match_sweep_discovery_and_renamed_ones_do(world):
    _big_and_small(world)
    proot = world.proot("teaching_small")
    rc, out, err = world.seed("--project", "teaching_small")
    rc2, out2, err2 = world.seed("--project", "teaching_small", "--tag", "batch_01")
    assert rc == rc2 == 0, err + err2
    names = sorted(p.name for p in proot.glob("lit_pull_queue*.csv"))
    assert names == ["lit_pull_queue.batch_01.draft.csv", "lit_pull_queue.draft.csv"]
    assert sweep.discover_queues(proot) == []
    assert [sweep.queue_tag(n) for n in names] == [None, None]
    head, _, _ = world.draft(proot / "lit_pull_queue.batch_01.draft.csv")
    assert head[0] == ("# REVIEW BEFORE SWEEP -- drop irrelevant rows, then "
                       "`mv lit_pull_queue.batch_01.draft.csv lit_pull_queue.batch_01.csv`")
    head, _, _ = world.draft(proot / "lit_pull_queue.draft.csv")
    assert head[0].endswith("`mv lit_pull_queue.draft.csv lit_pull_queue.csv`")
    (proot / "lit_pull_queue.batch_01.draft.csv").rename(proot / "lit_pull_queue.batch_01.csv")
    (proot / "lit_pull_queue.draft.csv").rename(proot / "lit_pull_queue.csv")
    assert [p.name for p in sweep.discover_queues(proot)] == ["lit_pull_queue.csv", "lit_pull_queue.batch_01.csv"]
    assert sweep.queue_tag("lit_pull_queue.batch_01.csv") == "batch_01"


@pytest.mark.parametrize("tag", ["draft", "2026-10-06", "Upper", "retry_later", "a.b", ""])
def test_invalid_tag_exits_1(world, tag):
    _big_and_small(world)
    rc, out, err = world.seed("--project", "teaching_small", "--tag", tag)
    assert rc == 1 and "not a valid queue tag" in err
    assert not list(world.proot("teaching_small").glob("lit_pull_queue*.csv"))


def test_columns_header_and_notes(world):
    _big_and_small(world)
    rc, out, err = world.seed("--project", "teaching_small")
    path = world.proot("teaching_small") / "lit_pull_queue.draft.csv"
    head, fields, rows = world.draft(path)
    assert fields == S.BASE_COLUMNS == ["doi", "title", "authors", "year", "destination", "notes"]
    assert head[0].startswith("# REVIEW BEFORE SWEEP")
    assert head[-1] == "# Total rows: 2 (of 2 matching)"
    assert list(notes_of(rows[0])) == ["n_seeds_project", "n_seeds_portfolio", "cites", "src"]
    assert rows[0]["title"] == "Paper 10.5555/y.0003" and rows[0]["year"] == "2020"
    assert out.splitlines()[0] == f"Wrote 2 draft rows to {path}"           # run_daily prints line 1
    assert b"\r\n" not in path.read_bytes()


def test_limit_truncation_warns_and_counts(world):
    _big_and_small(world)
    rc, out, err = world.seed("--project", "teaching_small", "--limit", "1")
    head, _, rows = world.draft(world.proot("teaching_small") / "lit_pull_queue.draft.csv")
    assert dois_of(rows) == ["10.5555/y.0003"] and "--limit=1 truncated" in err
    assert head[-1] == "# Total rows: 1 (of 2 matching)"


# ================================================================ registry, DB, exits
def test_missing_registry_exits_1_not_2(world, monkeypatch, tmp_path):
    monkeypatch.setattr(S, "CONFIG_PATH", tmp_path / "nowhere" / "projects.json")
    rc, out, err = world.seed("--project", "research_a")
    assert rc == 1 and "projects.json not found" in err
    s = summary_of(out)
    assert s["exit_code"] == 1 and s["reasons"] and s["aborted"] is None


def test_unknown_project_exits_1(world):
    world.register("research_a")
    rc, out, err = world.seed("--project", "research_zz")
    assert rc == 1 and "'research_zz' not in projects.json" in err


def test_missing_index_exits_1_and_creates_nothing(world):
    world.register("research_a")
    rc, out, err = world.seed("--project", "research_a")
    assert rc == 1 and "no index at" in err and "index_portfolio.py --project research_a" in err
    assert not world.db.exists() and not world.db.parent.exists()


def test_db_resolves_at_call_time_under_the_patched_root(world, monkeypatch):
    """Never the live DB: with PROJECTS_ROOT patched and no --db, the seeder opens
    <tmp root>/_references/portfolio.duckdb (config.db_dir at call time)."""
    assert S.DB_PATH is None
    src = Path(S.__file__).read_text(encoding="utf-8")
    assert "PROJECTS_ROOT / \"_references\"" not in src
    world.register("research_a")
    world.hold("research_a", "10.5555/a.0001")
    world.reverse("research_a", [("10.5555/a.0001", "10.5555/c.0001")])
    default_db = world.root / "_references" / "portfolio.duckdb"
    world.index(db=default_db)
    opened = []
    real = lit_util.connect_db
    monkeypatch.setattr(lit_util, "connect_db", lambda p, *a, **k: (opened.append((p, k)), real(p, *a, **k))[1])
    rc, out, err = world.seed("--project", "research_a", db=False)
    assert rc == 0, err
    assert opened == [(str(default_db), {"read_only": True})]
    assert summary_of(out)["output"].startswith(str(world.root))
    # a registry db_dir is honoured the same way
    world.top["db_dir"] = str(world.tmp / "elsewhere")
    world._write()
    rc, out, err = world.seed("--project", "research_a", db=False)
    assert rc == 1 and str(world.tmp / "elsewhere" / "portfolio.duckdb") in err


def _drop_new_relations(db):
    con = duckdb.connect(str(db))
    try:
        con.execute("DROP VIEW project_cocitations")
        con.execute("DROP TABLE scoped_cites")
        con.execute("DROP TABLE scoped_candidates")
    finally:
        con.close()


def test_index_built_before_w3c1_exits_1_for_cocitation_and_scope(world):
    _big_and_small(world)
    _drop_new_relations(world.db)
    for argv, what in ((["--rank", "cocitation"], "project_cocitations"), (["--scope", "ch01"], "scoped_candidates")):
        rc, out, err = world.seed("--project", "teaching_small", *argv)
        assert rc == 1, argv
        msg = [ln for ln in err.splitlines() if ln.startswith("[ERR]")]
        assert len(msg) == 1 and what in msg[0] and "python index_portfolio.py --project teaching_small" in msg[0]
        assert "Traceback" not in err
    rc, out, err = world.seed("--project", "teaching_small")                # project rank needs neither
    assert rc == 0, err


def test_run_returns_a_dict_and_main_reads_sys_argv(world, monkeypatch):
    _big_and_small(world)
    res = S.run(project="teaching_small", db=str(world.db))
    world.capsys.readouterr()
    assert res["exit_code"] == 0 and res["dois"] == ["10.5555/y.0003", "10.5555/x.0040"]
    assert res["destination"] == "literature/" and res["pmc"] is None
    monkeypatch.setattr(sys, "argv", ["seed", "--project", "teaching_small", "--db", str(world.db)])
    assert S.main() == 0


def test_cfg_dict_replaces_the_registry_file(world, tmp_path, monkeypatch):
    _big_and_small(world)
    cfg = json.loads(world.cfg.read_text(encoding="utf-8"))
    monkeypatch.setattr(S, "CONFIG_PATH", tmp_path / "missing.json")
    res = S.run(project="teaching_small", db=str(world.db), cfg=cfg)
    assert res["exit_code"] == 0
