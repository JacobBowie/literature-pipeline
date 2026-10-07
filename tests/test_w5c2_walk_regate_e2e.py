"""W5-C2 step 1, end to end through forward_citations (needs the forwarded forward_citations.py
patch): under --source openalex a seed S2 holds no count for is walked again when OpenAlex's
cited_by_count moved since its cached walk, and kept (no cites: list call) when it did not."""
import pytest

import forward_citations as fc
from litpipe import openalex, walk
from tests.test_walk_forward import _raw, make_lib, world  # noqa: F401  (fixture)
from tests.test_w5c2_walk import PagedWorld

SEED = "10.5555/oaseed.0001"


class S2UnknownWorld(PagedWorld):
    """S2's metadata pass answers null for the seed (no S2 record), as for a seed S2 does not hold."""

    def __call__(self, method, url, hdrs, body, timeout, max_bytes):
        if "/paper/batch" in url and body:
            import json
            js = json.loads(body)
            self.sent.append({"method": method, "host": "api.semanticscholar.org", "path": "/graph/v1/paper/batch",
                              "params": {}, "json": js})
            return _raw(200, [None for _ in js["ids"]])
        return super().__call__(method, url, hdrs, body, timeout, max_bytes)


@pytest.fixture
def uworld(world, monkeypatch):
    from litpipe import net
    w = S2UnknownWorld()
    w.env = world.env
    monkeypatch.setitem(net._TRANSPORTS, "requests", w)
    monkeypatch.setitem(net._TRANSPORTS, "urllib", w)
    return w


def _cites_calls(w):
    return [r for r in w.oa_calls() if r["path"] == "/works"]


def test_a_count_none_seed_is_rewalked_only_when_cited_by_count_moved(uworld, tmp_path, monkeypatch):
    monkeypatch.setenv(openalex.KEY_ENV, "oa-test-key-not-real")
    uworld.script(SEED, [[0, 1, 2]], [3], cited_by=3)
    lib = make_lib(tmp_path, [SEED])
    res = fc.run(lib_dir=str(lib), source="openalex")
    assert res["exit_code"] == 0 and res["openalex_walked"] == 1
    with walk.Cache(walk.CACHE_PATH) as c:
        st = c.states()[(SEED, "openalex")]
    assert st["count_at_walk"] is None and st["oa_cited_by"] == 3

    uworld.sent.clear()                                   # unchanged: kept, one free singleton, no list call
    res = fc.run(lib_dir=str(lib), source="openalex")
    assert res["exit_code"] == 0 and _cites_calls(uworld) == []
    assert [r["path"] for r in uworld.oa_calls()] == [f"/works/doi:{SEED}"]

    uworld.sent.clear()                                   # moved: walked again
    uworld.script(SEED, [[0, 1, 2, 3]], [4], cited_by=4)
    res = fc.run(lib_dir=str(lib), source="openalex")
    assert res["exit_code"] == 0 and len(_cites_calls(uworld)) == 1
    with walk.Cache(walk.CACHE_PATH) as c:
        assert c.states()[(SEED, "openalex")]["oa_cited_by"] == 4
        assert len(c.rows(SEED, "openalex")) == 4
