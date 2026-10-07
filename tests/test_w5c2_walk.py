"""W5-C2 step 1: the OpenAlex walk's length check and the count gate for a seed S2 holds no count for.

- The length check counts the rows OpenAlex RETURNED (before W-id de-duplication) against the first
  or the last page's meta.count; a W-id repeated across cursor pages is dropped from the result,
  counted and named, and the seed is complete (it failed every run before: 119 against 120).
- A seed with no S2 count (source "openalex") is gated on OpenAlex's free singleton cited_by_count:
  walked again when it moved since the walk that stored it, kept otherwise.
Offline: the World transport of tests/test_walk_forward.py, with the singleton answering
cited_by_count and scripted cursor pages."""
import json

import duckdb
import pytest

from litpipe import openalex, walk
from tests.test_walk_forward import World, _raw, RL, oa_work, world  # noqa: F401  (fixture)

SEED = "10.5555/oaseed.0001"


class PagedWorld(World):
    """OpenAlex singletons with cited_by_count, and cites: pages scripted as lists of work indexes
    with a meta.count per page."""

    def __init__(self):
        super().__init__()
        self.cited_by = {}
        self.pages = {}

    def script(self, doi, pages, counts, cited_by=None):
        self.add(doi, None, 0, oa=0)
        self.papers[doi]["pages"] = pages
        self.papers[doi]["counts"] = counts
        self.cited_by[doi] = cited_by
        return self

    def _openalex(self, path, q):
        if path.startswith("/works/doi:"):
            doi = path[len("/works/doi:"):].lower()
            p = self.papers.get(doi)
            if p is None:
                return _raw(404, {"error": "not found"}, {**RL, "X-RateLimit-Credits-Used": "0"})
            body = {"id": f"https://openalex.org/{p['wid']}"}
            if "cited_by_count" in q.get("select", "") and self.cited_by.get(doi) is not None:
                body["cited_by_count"] = self.cited_by[doi]
            return _raw(200, body, {**RL, "X-RateLimit-Credits-Used": "0"})
        if path == "/works" and q.get("filter", "").startswith("cites:"):
            wid = q["filter"][len("cites:"):]
            p = next(x for x in self.papers.values() if x["wid"] == wid)
            if "pages" not in p:
                return super()._openalex(path, q)
            cur = q.get("cursor", "*")
            k = 0 if cur == "*" else int(cur[1:])
            works = [oa_work(p["doi"], i) for i in p["pages"][k]]
            nxt = f"c{k + 1}" if k + 1 < len(p["pages"]) else None
            return _raw(200, {"meta": {"count": p["counts"][k], "next_cursor": nxt}, "results": works}, RL)
        return super()._openalex(path, q)


@pytest.fixture
def pworld(world, monkeypatch):
    from litpipe import net
    w = PagedWorld()
    w.env = world.env
    monkeypatch.setitem(net._TRANSPORTS, "requests", w)
    monkeypatch.setitem(net._TRANSPORTS, "urllib", w)
    return w


def _walk(doi):
    return walk.walk_openalex(doi, session=openalex.Session())


# ------------------------------------------------------------------ the length check
def test_a_wid_on_two_cursor_pages_is_complete_with_the_unique_rows(pworld):
    # 120 rows returned over two pages (meta.count 120); work 99 comes back again on page 2.
    pworld.script(SEED, [list(range(100)), [99] + list(range(100, 119))], [120, 120])
    res = _walk(SEED)
    assert res.state == "complete" and not res.failed and not res.mismatch
    assert len(res.rows) == 119
    assert len({r["citing_paper_id"] for r in res.rows}) == 119
    assert res.duplicates == 1 and "W9000099" in res.reason
    assert res.expected == 120


def test_the_last_pages_count_is_accepted_when_the_count_moved_while_paging(pworld):
    pworld.script(SEED, [list(range(100)), list(range(100, 121))], [120, 121])
    res = _walk(SEED)
    assert res.state == "complete" and len(res.rows) == 121


def test_the_first_pages_count_is_accepted_too(pworld):
    pworld.script(SEED, [list(range(100)), list(range(100, 120))], [120, 121])
    res = _walk(SEED)
    assert res.state == "complete" and len(res.rows) == 120


def test_a_short_list_against_both_counts_still_fails(pworld):
    pworld.script(SEED, [list(range(100)), list(range(100, 118))], [120, 121])
    res = _walk(SEED)
    assert res.failed and res.mismatch and res.kind == "COUNT_MISMATCH"
    assert "118 rows" in res.reason and "120 (first page), 121 (last page)" in res.reason


def test_a_duplicate_does_not_hide_a_short_list(pworld):
    # 119 returned with one repeat (118 unique) against meta.count 120: short, failed.
    pworld.script(SEED, [list(range(100)), [5] + list(range(100, 118))], [120, 120])
    res = _walk(SEED)
    assert res.failed and res.mismatch and "119 rows (118 unique)" in res.reason


def test_the_walk_carries_the_singletons_cited_by_count(pworld):
    pworld.script(SEED, [list(range(3))], [3], cited_by=4)
    res = _walk(SEED)
    assert res.state == "complete" and res.oa_cited_by == 4
    singleton = [r for r in pworld.oa_calls() if r["path"].startswith("/works/doi:")]
    assert len(singleton) == 1 and "cited_by_count" in singleton[0]["params"]["select"]


# ------------------------------------------------------------------ the count gate (count None)
def _cached(oa_cited_by, state="complete"):
    return {"doi": SEED, "source": "openalex", "state": state, "count_at_walk": None, "n_rows": 3,
            "oa_cited_by": oa_cited_by}


def test_a_count_none_seed_is_walked_again_when_cited_by_count_moved():
    assert walk.needs_walk(None, _cached(5), oa_count=6)


def test_a_count_none_seed_is_kept_when_cited_by_count_did_not_move():
    assert not walk.needs_walk(None, _cached(5), oa_count=5)


def test_without_an_openalex_count_the_gate_is_unchanged():
    assert not walk.needs_walk(None, _cached(5))
    assert walk.needs_walk(None, _cached(5), refresh=True)
    assert walk.needs_walk(None, _cached(5, state="failed"), oa_count=5)
    # an S2 count still decides for a seed S2 knows
    assert not walk.needs_walk(7, {**_cached(5), "count_at_walk": 7}, oa_count=9)


def test_a_seed_walked_before_the_column_existed_is_walked_once_to_store_it():
    assert walk.needs_walk(None, _cached(None), oa_count=5)


def test_the_planner_applies_the_same_gate():
    cached = {(SEED, "openalex"): _cached(5)}
    kept = walk.plan({SEED: None}, cached, source="openalex", oa_counts={SEED: 5})
    moved = walk.plan({SEED: None}, cached, source="openalex", oa_counts={SEED: 6})
    assert kept["kept"] == 1 and kept["walk"] == 0
    assert moved["walk"] == 1 and moved["kept"] == 0


def test_openalex_counts_are_free_singletons_and_a_failed_lookup_is_none(pworld):
    pworld.script(SEED, [[0]], [1], cited_by=11)
    pworld.script("10.5555/oa.0002", [[0]], [1], cited_by=None)      # no count in the answer
    got = walk.openalex_counts([SEED, "10.5555/oa.0002", "10.5555/missing.0003"], session=openalex.Session())
    assert got == {SEED: 11, "10.5555/oa.0002": None, "10.5555/missing.0003": None}
    assert all(r["path"].startswith("/works/doi:") for r in pworld.oa_calls())       # no list call
    assert len(pworld.oa_calls()) == 3


def test_cited_by_count_outcomes(pworld):
    pworld.script(SEED, [[0]], [1], cited_by=11)
    ok = openalex.cited_by_count(SEED, session=openalex.Session())
    assert ok.ok and ok.payload == 11
    miss = openalex.cited_by_count("10.5555/missing.0003", session=openalex.Session())
    assert miss.kind.name == "NO_MATCH" and miss.payload is None
    skip = openalex.cited_by_count("not a doi", session=openalex.Session())
    assert skip.kind.name == "SKIPPED"


# ------------------------------------------------------------------ the cache column
def test_the_cache_stores_and_keeps_oa_cited_by(tmp_path):
    path = tmp_path / "c.duckdb"
    with walk.Cache(path) as c:
        c.record(SEED, "openalex", state="complete", rows=[], oa_cited_by=7)
        assert c.states()[(SEED, "openalex")]["oa_cited_by"] == 7
        c.record(SEED, "openalex", state="failed", reason="page 2 failed")          # state only, kept
        assert c.states()[(SEED, "openalex")]["oa_cited_by"] == 7
        c.record(SEED, "openalex", state="complete", rows=[], oa_cited_by=9)
        assert c.states()[(SEED, "openalex")]["oa_cited_by"] == 9


def test_a_cache_written_before_the_column_gains_it_on_open(tmp_path):
    path = tmp_path / "old.duckdb"
    con = duckdb.connect(str(path))
    con.execute("""CREATE TABLE seed_state (doi VARCHAR NOT NULL, source VARCHAR NOT NULL, paper_id VARCHAR,
        count_at_walk BIGINT, n_rows BIGINT, state VARCHAR NOT NULL, kind VARCHAR, reason VARCHAR,
        n_unreachable BIGINT, walked_at TIMESTAMPTZ NOT NULL, rows_at TIMESTAMPTZ, PRIMARY KEY (doi, source))""")
    con.execute("INSERT INTO seed_state VALUES ('10.5555/old', 'openalex', NULL, NULL, 3, 'complete', NULL, '', "
                "NULL, TIMESTAMPTZ '2026-10-01 00:00:00+00', TIMESTAMPTZ '2026-10-01 00:00:00+00')")
    con.close()
    with walk.Cache(path) as c:
        st = c.states()[("10.5555/old", "openalex")]
        assert st["oa_cited_by"] is None and st["n_rows"] == 3
        c.record("10.5555/old", "openalex", state="complete", rows=[], oa_cited_by=3)
        assert c.states()[("10.5555/old", "openalex")]["oa_cited_by"] == 3
