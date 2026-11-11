"""Candidate titles (librarian brief 2026-11-10, `_portfolio/briefs/2026-11-10_litpipe_index_titles_fix.md`).

- litpipe.titles.junk_kind / clean_title: reference fragments, URLs, author lists and empty strings
  are junk (stored as NULL); real titles, including ones starting with "(" or "[" and tag-shaped
  words such as <NA>, are kept in display form with real markup stripped.
- Lookups: Crossref (404 is not_found, an outage an error) and arXiv (50 ids per call, version
  suffix stripped), with litpipe.net stubbed; nothing is sent.
- index_portfolio: a reverse-citation fragment is stored as NULL, and a later real title fills it
  (paper_metadata stays insert-if-absent for everything else).
- The seeder drops a row without a real title (counted in the header); --lookup-titles looks it
  up instead, and a failed lookup makes the exit 2.
- The refresh (python -m litpipe.titles): a dry run writes nothing; --commit fills, cleans markup,
  NULLs unanswered junk, leaves a transient failure for the next run; scoped by --project.
"""
import json
from types import SimpleNamespace

import duckdb
import pytest

import index_portfolio as I
import seed_queue_from_top_candidates as S
from litpipe import titles as T
from litpipe.outcomes import Kind, Outcome
from tests.test_seeder import World, summary_of


# ================================================================ classifier
@pytest.mark.parametrize("raw,kind", [
    ("", "empty"), (None, "empty"), ("   ", "empty"),
    ("; 41(1): 3-12", "reference_fragment"),
    (";76:1–15. doi:10.1016/j.envint.2014.11.016.", "reference_fragment"),
    ("doi: 10.48550/arXiv.2210.17323.", "reference_fragment"),
    ("57:418. doi: 10.3390/medicina57050418", "reference_fragment"),
    ("Some paper title [PubMed] [Google Scholar]", "reference_fragment"),
    ("[Online]", "reference_fragment"),
    ("vol. 52, no 4, pp. 869–873.", "reference_fragment"),
    ("Nov 30;12(12):3692", "reference_fragment"),
    ("URL: http://arxiv.org/abs/2310.01557.", "reference_fragment"),
    ("02.021 (2019).", "reference_fragment"),
    ("(2017).", "reference_fragment"),
    ("–2019: a systematic analysis for the Global Burden of Disease Study 2019", "reference_fragment"),
    ("J Clin Oncol", "reference_fragment"),
    ("Am J Physiol Regul Integr Comp Physiol", "reference_fragment"),
    ("Med Sci Sports Exerc 2013;45:824–31.", "reference_fragment"),
    ("Nucleic Acids Res. 47, D1005–D1012 (2019).", "reference_fragment"),
    ("https://doi.org/10.3389/fphys.2011.00086", "url"),
    ("www.example.org/paper", "url"),
    ("Kim, S., Mangalam, K., Moon, S., Malik, J., Mahoney, M", "author_list"),
    ("Izacard G, Lewis PSH, Lomeli M, Hosseini L", "author_list"),
])
def test_junk_kinds(raw, kind):
    assert T.junk_kind(raw) == kind
    assert T.clean_title(raw) is None


@pytest.mark.parametrize("raw,clean", [
    ("Heat Stroke", "Heat Stroke"),
    ("(-)-Epicatechin improves mitochondrial function", "(-)-Epicatechin improves mitochondrial function"),
    ("[Heat stroke in athletes]", "[Heat stroke in athletes]"),
    ("β-alanine supplementation and exercise", "β-alanine supplementation and exercise"),
    ("The HSP70 chaperone machinery: J proteins as drivers of functional specificity",
     "The HSP70 chaperone machinery: J proteins as drivers of functional specificity"),
    ("Effects of training on VO<sub>2max</sub>", "Effects of training on VO2max"),
    ("<i>In vivo</i> heat &amp; cold", "In vivo heat & cold"),
    ("Values were <NA> in R", "Values were <NA> in R"),
    ("Mixtral of Experts", "Mixtral of Experts"),
    ("Fourth universal definition of myocardial infarction (2018)",
     "Fourth universal definition of myocardial infarction (2018)"),
])
def test_real_titles_are_kept_in_display_form(raw, clean):
    assert T.junk_kind(raw) is None
    assert T.clean_title(raw) == clean


# ================================================================ lookups (litpipe.net stubbed)
class FakeNet:
    """Stands in for litpipe.net.get: answers from `routes` (url substring -> callable(params))."""

    def __init__(self, routes):
        self.routes, self.calls = routes, []

    def get(self, url, params=None, **kw):
        self.calls.append((url, params))
        for frag, fn in self.routes.items():
            if frag in url:
                return fn(url, params)
        raise AssertionError(f"unexpected request {url}")


def _json(status, body):
    payload = SimpleNamespace(json=lambda: body, content=json.dumps(body).encode())
    kind = Kind.OK if status == 200 else Kind.NO_MATCH if status == 404 else Kind.OUTAGE
    return Outcome(kind, status=status, payload=payload if status == 200 else None)


def _atom(entries):
    body = "".join(f"<entry><id>http://arxiv.org/abs/{i}</id><title>{t}</title></entry>" for i, t in entries)
    xml = f'<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom">{body}</feed>'
    return Outcome(Kind.OK, status=200, payload=SimpleNamespace(content=xml.encode()))


def crossref_route(records):
    def fn(url, _params):
        doi = url.split("/works/", 1)[1].replace("%2F", "/")
        rec = records.get(doi)
        if rec == "500":
            return _json(500, None)
        if rec is None:
            return _json(404, None)
        return _json(200, {"message": {"title": [rec] if rec else []}})
    return fn


def test_crossref_title_statuses(monkeypatch):
    fake = FakeNet({"api.crossref.org": crossref_route(
        {"10.5555/t.0001": "A <i>Real</i> Title", "10.5555/t.0002": "", "10.5555/t.0003": "500"})})
    monkeypatch.setattr(T.net, "get", fake.get)
    assert T.crossref_title("10.5555/t.0001") == ("A Real Title", "ok")
    assert T.crossref_title("10.5555/t.0002") == (None, "no_title")
    assert T.crossref_title("10.5555/t.0004") == (None, "not_found")
    assert T.crossref_title("10.5555/t.0003")[1].startswith("error")


def test_arxiv_titles_batch_50_and_strip_versions(monkeypatch):
    dois = [f"10.48550/arXiv.2401.{n:05d}" for n in range(60)]

    def fn(_url, params):
        ids = params["id_list"].split(",")
        return _atom([(f"{i}v2", f"Paper {i}") for i in ids])
    fake = FakeNet({"export.arxiv.org": fn})
    monkeypatch.setattr(T.net, "get", fake.get)
    found, failed = T.arxiv_titles(dois)
    assert len(fake.calls) == 2 and len(fake.calls[0][1]["id_list"].split(",")) == 50
    assert failed == [] and len(found) == 60
    assert found["10.48550/arXiv.2401.00007"] == "Paper 2401.00007"


def test_lookup_routes_arxiv_and_crossref(monkeypatch):
    fake = FakeNet({"export.arxiv.org": lambda _u, p: _atom([("2401.04088v1", "Mixtral of Experts")]),
                    "api.crossref.org": crossref_route({"10.5555/t.0001": "Crossref Title"})})
    monkeypatch.setattr(T.net, "get", fake.get)
    res = T.lookup(["10.48550/arxiv.2401.04088", "10.5555/t.0001", "10.5555/t.0005"])
    assert res == {"10.48550/arxiv.2401.04088": ("Mixtral of Experts", "ok"),
                   "10.5555/t.0001": ("Crossref Title", "ok"), "10.5555/t.0005": (None, "not_found")}
    assert not any("api.crossref.org" in u and "48550" in u for u, _ in fake.calls)


# ================================================================ index_portfolio
@pytest.fixture
def world(tmp_path, monkeypatch, capsys):
    return World(tmp_path, monkeypatch, capsys)


SEED_A, SEED_B = "10.5555/seed.0001", "10.5555/seed.0002"


def test_index_stores_null_for_a_fragment_and_a_later_real_title_fills_it(world):
    world.register("p")
    world.hold("p", SEED_A)
    world.reverse("p", [(SEED_A, "10.5555/c.0001"), (SEED_A, "10.5555/c.0002"), (SEED_A, "10.5555/c.0003")],
                  titles={"10.5555/c.0001": "; 41(1): 3-12", "10.5555/c.0002": "Effects of <i>heat</i>",
                          "10.5555/c.0003": "https://doi.org/10.5555/c.0003"})
    world.index()
    got = dict(world.q("SELECT doi, title FROM paper_metadata WHERE doi LIKE '10.5555/c.%'"))
    assert got == {"10.5555/c.0001": None, "10.5555/c.0002": "Effects of heat", "10.5555/c.0003": None}
    # a forward walk later knows c.0001's real title: it fills the NULL; c.0002's title is not overwritten
    world.forward("p", [(SEED_A, "10.5555/c.0001"), (SEED_A, "10.5555/c.0002")],
                  titles={"10.5555/c.0001": "The Real Title", "10.5555/c.0002": "Other Title"})
    world.index()
    got = dict(world.q("SELECT doi, title FROM paper_metadata WHERE doi LIKE '10.5555/c.%'"))
    assert got["10.5555/c.0001"] == "The Real Title"
    assert got["10.5555/c.0002"] == "Effects of heat"


def test_insert_new_metadata_keeps_abstract_and_real_title(tmp_path):
    con = duckdb.connect(str(tmp_path / "x.duckdb"))
    con.execute(I.SCHEMA)
    con.execute("INSERT INTO paper_metadata (doi, title, abstract) VALUES "
                "('10.1/a', NULL, 'kept abstract'), ('10.1/b', 'Real', NULL), ('10.1/c', '', NULL)")
    now = "2026-11-11 00:00:00"
    I.insert_new_metadata(con, [("10.1/a", 2020, "Lee", "Filled Title", "", "", now),
                                ("10.1/b", 2020, "Lee", "Not This", "", "", now),
                                ("10.1/c", 2020, "Lee", "J Clin Oncol", "", "", now),
                                ("10.1/d", 2020, "Lee", "doi: 10.1/d", "", "", now)])
    got = {r[0]: r[1:] for r in con.execute("SELECT doi, title, abstract FROM paper_metadata").fetchall()}
    assert got["10.1/a"] == ("Filled Title", "kept abstract")
    assert got["10.1/b"] == ("Real", None)
    assert got["10.1/c"] == ("", None)          # junk never fills
    assert got["10.1/d"] == (None, None)        # new row, junk staged as NULL


# ================================================================ seeder
def _seed_world(world):
    world.register("p")
    world.hold("p", SEED_A, SEED_B)
    edges = [(s, d) for s in (SEED_A, SEED_B) for d in ("10.5555/y.0001", "10.5555/y.0002", "10.5555/y.0003")]
    world.reverse("p", edges, titles={"10.5555/y.0001": "A Fine Title", "10.5555/y.0002": "J Clin Oncol",
                                      "10.5555/y.0003": ""})
    world.index()


def test_seeder_drops_rows_without_a_real_title_and_says_so(world):
    _seed_world(world)
    rc, out, _err = world.seed("--project", "p")
    assert rc == 0
    head, _f, rows = world.draft(world.proot("p") / "lit_pull_queue.draft.csv")
    assert [r["doi"] for r in rows] == ["10.5555/y.0001"]
    assert any("a real title (litpipe.titles)" in h for h in head)
    assert summary_of(out)["titles"] == {"dropped": 0, "looked_up": 0, "found": 0, "failed": 0}


def test_seeder_lookup_titles_fills_from_lookup_and_degrades_on_failure(world, monkeypatch):
    _seed_world(world)
    seen = []

    def fake_lookup(dois):
        seen.extend(dois)
        return {"10.5555/y.0002": ("Looked Up Title", "ok"), "10.5555/y.0003": (None, "error:outage")}
    monkeypatch.setattr(S._titles, "lookup", fake_lookup)
    rc, out, _err = world.seed("--project", "p", "--lookup-titles")
    assert sorted(seen) == ["10.5555/y.0002", "10.5555/y.0003"]
    _head, _f, rows = world.draft(world.proot("p") / "lit_pull_queue.draft.csv")
    assert {r["doi"]: r["title"] for r in rows} == {"10.5555/y.0001": "A Fine Title",
                                                    "10.5555/y.0002": "Looked Up Title"}
    s = summary_of(out)
    assert rc == 2 and s["titles"] == {"dropped": 1, "looked_up": 2, "found": 1, "failed": 1}
    assert any("lookup-titles" in r for r in s["reasons"])


def test_fix_titles_without_lookup_sends_nothing(monkeypatch):
    monkeypatch.setattr(S._titles, "lookup", lambda d: pytest.fail("lookup without --lookup-titles"))
    rows = [{"doi": "10.1/a", "title": "Good <b>One</b>"}, {"doi": "10.1/b", "title": ";12(3):4"}]
    kept, stats = S.fix_titles(rows)
    assert [r["title"] for r in kept] == ["Good One"] and stats["dropped"] == 1


# ================================================================ refresh
def _refresh_world(world):
    world.register("p")
    world.register("q")
    world.hold("p", SEED_A, SEED_B)
    world.hold("q", "10.5555/seed.0003", "10.5555/seed.0004")
    pe = [(s, d) for s in (SEED_A, SEED_B) for d in
          ("10.5555/r.0001", "10.5555/r.0002", "10.5555/r.0003", "10.5555/r.0004", "10.5555/r.0005")]
    world.reverse("p", pe, titles={"10.5555/r.0001": "; 41(1): 3-12", "10.5555/r.0002": "Fine Title",
                                   "10.5555/r.0003": "", "10.5555/r.0004": "J Clin Oncol",
                                   "10.5555/r.0005": "doi: 10.5555/r.0005"})
    world.reverse("q", [(s, "10.5555/q.0001") for s in ("10.5555/seed.0003", "10.5555/seed.0004")],
                  titles={"10.5555/q.0001": ""})
    world.index()
    con = duckdb.connect(str(world.db))
    # markup that predates the cleaner (titles were stored raw before 2026-11-11)
    con.execute("UPDATE paper_metadata SET title = 'Fine <i>Title</i>' WHERE doi = '10.5555/r.0002'")
    con.close()


def test_refresh_dry_run_counts_and_writes_nothing(world, monkeypatch):
    _refresh_world(world)
    monkeypatch.setattr(T, "lookup", lambda d: pytest.fail("dry run must not look up"))
    before = world.q("SELECT doi, title FROM paper_metadata ORDER BY doi")
    con = duckdb.connect(str(world.db), read_only=True)
    res = T.refresh(con, ["p"], log=lambda *_: None)
    con.close()
    assert res["targets"] == 5 and res["by_kind"] == {"empty": 4, "markup": 1}
    assert world.q("SELECT doi, title FROM paper_metadata ORDER BY doi") == before


def test_refresh_commit_fills_cleans_nulls_and_leaves_failures(world, monkeypatch):
    _refresh_world(world)
    answers = {"10.5555/r.0001": ("Found One", "ok"), "10.5555/r.0003": (None, "not_found"),
               "10.5555/r.0004": (None, "error:outage"), "10.5555/r.0005": (None, "no_title")}
    monkeypatch.setattr(T, "lookup", lambda dois: {d: answers[d] for d in dois})
    con = duckdb.connect(str(world.db))
    res = T.refresh(con, ["p"], commit=True, log=lambda *_: None)
    con.close()
    got = dict(world.q("SELECT doi, title FROM paper_metadata WHERE doi LIKE '10.5555/r.%'"))
    assert got["10.5555/r.0001"] == "Found One"
    assert got["10.5555/r.0002"] == "Fine Title"
    assert got["10.5555/r.0003"] is None and got["10.5555/r.0005"] is None
    assert res["filled"] == 1 and res["cleaned"] == 1 and res["failed"] == 1
    assert res["not_found"] == ["10.5555/r.0003"]
    # q's candidate is out of scope: untouched; a second run targets only what is still junk
    assert world.q("SELECT title FROM paper_metadata WHERE doi = '10.5555/q.0001'") == [(None,)]
    con = duckdb.connect(str(world.db), read_only=True)
    again = T.refresh(con, ["p"], log=lambda *_: None)
    con.close()
    assert again["by_kind"] == {"empty": 3}
