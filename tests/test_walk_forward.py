"""The forward walk on the cache (dispatch W3-A: K2, REG-I39, O2): the count gate, routing, replace-
merge, placeholder and text-only seeds, aliases, OpenAlex for seeds over 9,999, kills and the cache
lock. Offline: World stubs litpipe.net's transports and answers like Semantic Scholar (POST
/paper/batch for the metadata pass and nested batches, paged GET /paper/{id}/citations with
publicationDateOrYear windows) and OpenAlex (/works/doi: singletons, cites: cursor pages). Every
cache lives under tmp (conftest sets litpipe.walk.CACHE_PATH; tests that need a named file pass
cache_path=)."""
import csv
import hashlib
import json
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

import pytest
from requests.structures import CaseInsensitiveDict

import forward_citations as fc
import lit_util
from litpipe import hosts, net, openalex, s2, walk

REPO = Path(__file__).resolve().parent.parent
REAL_STATE_DIR = Path.home() / ".local" / "db" / "literature_pipeline"
BATCH = "/graph/v1/paper/batch"
RL = {"X-RateLimit-Limit": "10000", "X-RateLimit-Remaining": "9000", "X-RateLimit-Credits-Used": "1",
      "X-RateLimit-Reset": "3600"}


def _raw(status, body, headers=None):
    data = body if isinstance(body, bytes) else json.dumps(body).encode("utf-8")
    h = CaseInsensitiveDict({"Content-Type": "application/json", **(headers or {})})
    return net._Raw(status, h, data, data[:net.CHUNK], len(data))


def pid_of(x):
    return hashlib.sha1(x.encode()).hexdigest()


def s2_citer(seed, i, *, dated=True, doi=True):
    d = date(2000, 1, 1) + timedelta(days=i % 9000)
    return {"paperId": pid_of(f"{seed}/c{i}"), "externalIds": {"DOI": f"10.9999/{seed[-6:]}.c{i}"} if doi else {},
            "title": f"Citer {i}", "year": d.year, "publicationDate": d.isoformat() if dated else None,
            "authors": [{"name": "Ann Author"}], "venue": "J Test", "citationCount": i % 50}


def oa_work(seed, i):
    return {"id": f"https://openalex.org/W{9000000 + i}", "doi": f"https://doi.org/10.7777/oa.{seed[-6:]}.{i}",
            "display_name": f"Open citer {i}", "publication_year": 2020 + i % 5, "cited_by_count": 5 + i % 30,
            "authorships": [{"author": {"display_name": "Bea Open"}}, {"author": {"display_name": "Cy Access"}}],
            "primary_location": {"source": {"display_name": "J Open"}}, "open_access": {"is_oa": i % 2 == 0}}


class World:
    """Semantic Scholar and OpenAlex on one stub transport.

    add(doi, count, rows=None, ...): an S2 paper; rows is a count (generated lazily, `undated` of them
    without a publicationDate) or a list of citer dicts. `s2_doi` is the externalIds.DOI S2 answers
    (default the seed DOI), `pid` its paperId (two seeds may share one), `oa` the number of OpenAlex
    citers (None: not in OpenAlex). fail(doi, status) fails that seed's paged GETs (a nested batch
    then lists it short, so the client re-fetches it); nested_null makes a nested row null
    (not_found); interrupt_on (a paperId) raises KeyboardInterrupt at its GET; oa_fail_at = (page,
    status, headers) fails one OpenAlex cites page. `violations` records any GET with
    offset+limit >= 10000."""

    def __init__(self):
        self.papers = {}
        self.sent = []
        self.failing = {}
        self.nested_null = set()
        self.interrupt_on = None
        self.oa_fail_at = None
        self.violations = []

    def add(self, doi, count, rows=None, *, s2_doi=None, pid=None, year=1999, oa=None, undated=0):
        n = count if rows is None else rows
        self.papers[doi] = {"doi": doi, "count": count, "rows": n, "pid": pid or pid_of(doi), "year": year,
                            "s2_doi": s2_doi or doi, "oa": oa, "undated": undated, "wid": f"W{100 + len(self.papers)}"}
        return self

    def fail(self, doi, status):
        self.failing[self.papers[doi]["pid"]] = status
        return self

    # -- views
    def by_pid(self, pid):
        return next((p for p in self.papers.values() if p["pid"] == pid), None)

    def n_rows(self, p):
        return p["rows"] if isinstance(p["rows"], int) else len(p["rows"])

    def all_rows(self, p):
        if isinstance(p["rows"], int):
            n = p["rows"]
            return [s2_citer(p["doi"], i, dated=i >= p["undated"]) for i in range(n)]
        return list(p["rows"])

    def metadata_calls(self):
        return [r for r in self.sent if r["path"] == BATCH and "citations." not in r["params"].get("fields", "")]

    def nested_calls(self):
        return [r for r in self.sent if r["path"] == BATCH and "citations." in r["params"].get("fields", "")]

    def get_calls(self, doi=None):
        pid = self.papers[doi]["pid"] if doi else None
        return [r for r in self.sent if r["path"].endswith("/citations") and (pid is None or f"/{pid}/" in r["path"])]

    def oa_calls(self):
        return [r for r in self.sent if r["host"] == "api.openalex.org"]

    def s2_calls(self):
        return [r for r in self.sent if r["host"] == "api.semanticscholar.org"]

    # -- the transport
    def __call__(self, method, url, hdrs, body, timeout, max_bytes):
        parts = urlsplit(url)
        path = unquote(parts.path)
        q = {k: v[-1] for k, v in parse_qs(parts.query).items()}
        js = json.loads(body) if body else None
        self.sent.append({"method": method, "host": parts.hostname, "path": path, "params": q, "json": js})
        if parts.hostname == "api.openalex.org":
            return self._openalex(path, q)
        if path == BATCH:
            if "citations." in q.get("fields", ""):
                return self._nested(js["ids"])
            out = []
            for i in js["ids"]:
                p = self.papers.get(i[len("DOI:"):].lower())
                out.append(None if p is None else {"paperId": p["pid"], "citationCount": p["count"],
                                                   "externalIds": {"DOI": p["s2_doi"]}, "year": p["year"]})
            return _raw(200, out)
        if path.endswith("/citations"):
            return self._get(path.split("/")[-2], q)
        return _raw(404, {"error": f"no route {path}"})

    def _nested(self, ids):
        out = []
        for i in ids:
            p = self.by_pid(i) or self.papers.get(i[len("DOI:"):].lower())
            if p is None or p["pid"] in self.nested_null:
                out.append(None)
                continue
            short = p["pid"] in self.failing
            out.append({"paperId": p["pid"], "citationCount": p["count"],
                        "citations": [] if short else self.all_rows(p)})
        return _raw(200, out)

    def _get(self, pid, q):
        if self.interrupt_on == pid:
            raise KeyboardInterrupt("scripted kill")
        off, lim = int(q["offset"]), int(q["limit"])
        if off + lim >= 10000 or lim > 1000:
            self.violations.append((pid, off, lim))
            return _raw(400, {"error": "offset + limit must be < 10000"})
        if pid in self.failing:
            return _raw(self.failing[pid], {"message": "scripted failure"})
        p = self.by_pid(pid)
        if p is None:
            return _raw(404, {"error": "Paper not found"})
        rows = self.all_rows(p)
        win = q.get("publicationDateOrYear")
        if win:
            a, b = win.split(":")
            rows = [r for r in rows if r.get("publicationDate") and a <= r["publicationDate"] <= b]
        if q.get("fields") == "paperId":
            rows = [{"paperId": r["paperId"]} for r in rows]
        page = {"offset": off, "data": [{"citingPaper": r} for r in rows[off:off + lim]]}
        if off + lim < len(rows):
            page["next"] = off + lim
        return _raw(200, page)

    def _openalex(self, path, q):
        if path.startswith("/works/doi:"):
            doi = path[len("/works/doi:"):].lower()
            p = self.papers.get(doi)
            if p is None or p["oa"] is None:
                return _raw(404, {"error": "not found"}, {**RL, "X-RateLimit-Credits-Used": "0"})
            return _raw(200, {"id": f"https://openalex.org/{p['wid']}"}, {**RL, "X-RateLimit-Credits-Used": "0"})
        if path == "/works" and q.get("filter", "").startswith("cites:"):
            wid = q["filter"][len("cites:"):]
            p = next(x for x in self.papers.values() if x["wid"] == wid)
            n = p["oa"]
            cur = q.get("cursor", "*")
            k = 0 if cur == "*" else int(cur[1:])
            if self.oa_fail_at and self.oa_fail_at[0] == k:
                st, hd = self.oa_fail_at[1], self.oa_fail_at[2]
                return _raw(st, {"error": "scripted"}, hd)
            per = int(q.get("per_page", 25))
            works = [oa_work(p["doi"], i) for i in range(k * per, min(n, (k + 1) * per))]
            if q.get("select"):                          # OpenAlex honours select (top-level fields only)
                keep = q["select"].split(",")
                works = [{f: w[f] for f in keep if f in w} for w in works]
            nxt = f"c{k + 1}" if (k + 1) * per < n else None
            return _raw(200, {"meta": {"count": n, "next_cursor": nxt}, "results": works}, RL)
        return _raw(404, {"error": f"no route {path}"})


# ------------------------------------------------------------------------------ fixtures
@pytest.fixture
def world(net_env, monkeypatch):
    monkeypatch.delenv(s2.KEY_ENV, raising=False)
    monkeypatch.delenv(openalex.KEY_ENV, raising=False)
    monkeypatch.setattr(s2, "BASE", "https://api.semanticscholar.org")
    monkeypatch.setattr(openalex, "BASE", "https://api.openalex.org")
    s2.reset_default_session()
    openalex.reset_default_session()
    w = World()
    monkeypatch.setitem(net._TRANSPORTS, "requests", w)
    monkeypatch.setitem(net._TRANSPORTS, "urllib", w)
    w.env = net_env
    yield w
    s2.reset_default_session()
    openalex.reset_default_session()
    hosts.reset()


def make_lib(root, dois, name="literature", ris=True):
    lib = Path(root) / name
    lib.mkdir(parents=True, exist_ok=True)
    for i, d in enumerate(dois):
        (lib / f"2020_Seed{i:04d}.pdf").write_bytes(b"%PDF-1.4 stub")
        if d and ris:
            (lib / f"2020_Seed{i:04d}.ris").write_text(f"TY  - JOUR\nDO  - {d}\nER  - \n", encoding="utf-8")
    return lib


def dois(n, prefix="10.5555/wk"):
    return [f"{prefix}.{i:04d}" for i in range(n)]


def read_csv(path):
    with open(path, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def cache_states():
    with walk.Cache(walk.CACHE_PATH) as c:
        return c.states()


def cache_rows(doi, source="s2"):
    with walk.Cache(walk.CACHE_PATH) as c:
        return c.rows(doi, source)


# ================================================================ K2 acceptance (stubbed)
def test_a_failed_seed_keeps_its_prior_rows_from_the_cache(world, tmp_path):
    ds = dois(25)
    for d in ds:
        world.add(d, 4)
    lib = make_lib(tmp_path, ds)
    assert fc.run(lib_dir=str(lib))["exit_code"] == 0
    (lib / "_forward_citations.csv").unlink()            # so the kept rows can only come from the cache
    for d in ds:                                         # every seed re-walked (I-1: kept seeds do not dilute)
        world.papers[d]["count"] = world.papers[d]["rows"] = 5
    world.papers[ds[3]]["count"] = 5                    # a new citer: the gate re-walks it ...
    world.papers[ds[3]]["rows"] = 5
    world.fail(ds[3], 503)                               # ... and the walk fails
    res = fc.run(lib_dir=str(lib))
    assert res["exit_code"] == 0 and res["failed"] == 1 and res["kept"] == 0      # 1 of 25 walked = 4 %
    rows = [r for r in read_csv(lib / "_forward_citations.csv") if r["seed_doi"] == ds[3]]
    assert len(rows) == 4 and {r["citing_title"] for r in rows} == {f"Citer {i}" for i in range(4)}
    st = cache_states()[(ds[3], "s2")]
    assert st["state"] == "failed" and st["n_rows"] == 4 and st["rows_at"] is not None
    assert len(cache_rows(ds[3])) == 4


def test_a_seed_of_1011_citers_takes_2_pages_and_gives_1011_rows(world, tmp_path):
    ds = dois(1)
    world.add(ds[0], 1011)
    lib = make_lib(tmp_path, ds)
    res = fc.run(lib_dir=str(lib))
    gets = world.get_calls(ds[0])
    assert [(g["params"]["offset"], g["params"]["limit"]) for g in gets] == [("0", "1000"), ("1000", "1000")]
    assert res["exit_code"] == 0 and res["total_rows"] == 1011
    assert len(read_csv(lib / "_forward_citations.csv")) == 1011
    assert cache_states()[(ds[0], "s2")]["n_rows"] == 1011


def test_the_loop_never_sends_offset_plus_limit_of_10000(world, tmp_path):
    ds = dois(2)
    world.add(ds[0], 9999)                               # paged to the last reachable row
    world.add(ds[1], 12000, undated=40)                  # over the cap: S2 year windows (no OpenAlex key)
    lib = make_lib(tmp_path, ds)
    res = fc.run(lib_dir=str(lib))
    assert world.violations == []
    for g in world.get_calls():
        assert int(g["params"]["offset"]) + int(g["params"]["limit"]) < 10000
    last = world.get_calls(ds[0])[-1]["params"]
    assert (last["offset"], last["limit"]) == ("9000", "999")
    assert res["exit_code"] == 0 and res["capped"] == 1


def test_a_second_run_with_unchanged_counts_makes_only_metadata_calls(world, tmp_path):
    ds = dois(6)
    for d, n in zip(ds, (0, 3, 700, 1500, 9999, 12000)):
        world.add(d, n)
    lib = make_lib(tmp_path, ds)
    first = fc.run(lib_dir=str(lib))
    assert first["exit_code"] == 0
    before = sha(lib / "_forward_citations.csv")
    world.sent.clear()
    second = fc.run(lib_dir=str(lib))
    assert second["exit_code"] == 0 and second["kept"] == 6 and second["walked"] == 0
    assert len(world.sent) == len(world.metadata_calls()) == 1 == second["s2"]["calls"]
    assert second["plan"]["s2_calls"] == 1
    assert sha(lib / "_forward_citations.csv") == before          # regenerated from the cache, unchanged


def test_more_than_5_percent_failed_seeds_exits_2_and_publishes_nothing(world, tmp_path):
    ds = dois(10)
    for d in ds:
        world.add(d, 2)
    world.fail(ds[6], 500)
    lib = make_lib(tmp_path, ds)
    res = fc.run(lib_dir=str(lib))
    assert res["exit_code"] == 2 and res["failed"] == 1 and not res["published"]
    assert not (lib / "_forward_citations.csv").exists() and fc.degraded_path(lib / "_forward_citations.csv").exists()


def test_a_run_counts_metadata_plus_nested_bins_plus_pages_and_nothing_else(world, tmp_path):
    ds = dois(6)
    for d, n in zip(ds, (0, 3, 4, 900, 1500, 2500)):
        world.add(d, n)
    res = fc.run(lib_dir=str(make_lib(tmp_path, ds)))
    assert len(world.metadata_calls()) == 1 and len(world.nested_calls()) == 1        # 3 + 4 + 900 in one bin
    assert len(world.get_calls()) == 2 + 3                                            # 1,500 and 2,500 paged
    assert res["s2"]["calls"] == len(world.sent) == 1 + 1 + 5 == res["plan"]["s2_calls"]
    assert world.nested_calls()[0]["json"]["ids"] == [pid_of(ds[3]), pid_of(ds[2]), pid_of(ds[1])]  # FFD order
    fields = world.nested_calls()[0]["params"]["fields"].split(",")
    assert fields[0] == "citationCount" and "citations.abstract" not in fields                      # the lean set
    assert all("abstract" not in g["params"]["fields"] for g in world.get_calls())


# ================================================================ seeds over 9,999 (O2)
def test_over_9999_goes_to_openalex_with_a_key_every_citer_source_openalex(world, tmp_path, monkeypatch):
    monkeypatch.setenv(openalex.KEY_ENV, "oa-test-key-not-real")
    ds = dois(2)
    world.add(ds[0], 10500, oa=10620).add(ds[1], 2)
    lib = make_lib(tmp_path, ds)
    res = fc.run(lib_dir=str(lib))
    assert res["exit_code"] == 0 and res["openalex_walked"] == 1 and res["capped"] == 0
    assert world.get_calls(ds[0]) == []                                                # no S2 page for it
    rows = [r for r in read_csv(lib / "_forward_citations.csv") if r["seed_doi"] == ds[0]]
    assert len(rows) == 10620 and {r["source"] for r in rows} == {"openalex"}
    r0 = rows[0]
    assert r0["citing_paper_id"] == "W9000000" and r0["citing_authors"] == "Bea Open; Cy Access"
    assert r0["citing_venue"] == "J Open" and r0["citing_oa"] == "true" and r0["citing_doi"].startswith("10.7777/oa.")
    st = cache_states()[(ds[0], "openalex")]
    assert st["state"] == "complete" and st["n_rows"] == 10620 and st["count_at_walk"] == 10500   # S2's count
    assert len(world.oa_calls()) == 1 + 107                                            # singleton + pages of 100
    assert all("Authorization" not in json.dumps(r["params"]) for r in world.oa_calls())
    pages = [r for r in world.oa_calls() if r["path"] == "/works"]
    assert {r["params"]["select"] for r in pages} == {",".join(walk.OA_SELECT)}


def test_over_9999_without_a_key_takes_s2_windows_and_records_capped(world, tmp_path):
    ds = dois(1)
    world.add(ds[0], 12000, undated=37)                  # 37 citers carry no publicationDate
    lib = make_lib(tmp_path, ds)
    res = fc.run(lib_dir=str(lib))
    assert res["exit_code"] == 0 and res["capped"] == 1 and world.oa_calls() == []
    st = cache_states()[(ds[0], "s2")]
    assert st["state"] == "capped_9999" and st["n_rows"] == 12000 - 37 and st["n_unreachable"] == 37
    gets = world.get_calls()
    assert all("publicationDateOrYear" in g["params"] for g in gets)                  # windows only
    assert gets[0]["params"]["publicationDateOrYear"].startswith("1999-01-01:")       # from the seed's year
    assert all(int(g["params"]["offset"]) + int(g["params"]["limit"]) < 10000 for g in world.get_calls())
    world.sent.clear()
    assert fc.run(lib_dir=str(lib))["kept"] == 1 and len(world.sent) == 1   # capped is not re-walked: count unchanged


def test_a_seed_openalex_does_not_hold_falls_back_to_s2_windows(world, tmp_path, monkeypatch):
    monkeypatch.setenv(openalex.KEY_ENV, "oa-test-key-not-real")
    ds = dois(1)
    world.add(ds[0], 10300, oa=None, undated=3)           # the OpenAlex singleton answers 404
    res = fc.run(lib_dir=str(make_lib(tmp_path, ds)))
    assert res["exit_code"] == 0 and res["capped"] == 1 and res["not_found"] == 0
    st = cache_states()
    assert st[(ds[0], "s2")]["state"] == "capped_9999" and (ds[0], "openalex") not in st
    assert len(world.oa_calls()) == 1


def test_openalex_stopping_mid_run_sends_the_rest_to_s2_windows(world, tmp_path, monkeypatch):
    monkeypatch.setenv(openalex.KEY_ENV, "oa-test-key-not-real")
    ds = dois(2)
    world.add(ds[0], 10100, oa=10100).add(ds[1], 10200, oa=10200, undated=5)
    world.oa_fail_at = (1, 429, {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "600"})
    lib = make_lib(tmp_path, ds)
    res = fc.run(lib_dir=str(lib))
    e0 = cache_states()[(ds[0], "openalex")]
    assert e0["state"] == "failed" and e0["kind"] == "DEFERRED"
    assert cache_states()[(ds[1], "s2")]["state"] == "capped_9999"                     # the fallback
    assert sum(1 for r in world.oa_calls() if r["params"].get("cursor") == "c1") == 1  # a 429 is never retried
    assert res["exit_code"] == 2 and res["aborted"] is None                            # 1 of 2 failed; S2 ran on


# ================================================================ replace-merge, gate states
def test_replace_merge_drops_a_citer_that_left_s2s_list(world, tmp_path):
    ds = dois(1)
    world.add(ds[0], 5)
    lib = make_lib(tmp_path, ds)
    fc.run(lib_dir=str(lib))
    assert {r["citing_title"] for r in cache_rows(ds[0])} == {f"Citer {i}" for i in range(5)}
    gone = s2_citer(ds[0], 4)
    world.papers[ds[0]]["rows"] = [s2_citer(ds[0], i) for i in (0, 1, 2, 3)]
    world.papers[ds[0]]["count"] = 4                     # citer 4 left the list
    res = fc.run(lib_dir=str(lib))
    assert res["exit_code"] == 0
    titles = [r["citing_title"] for r in cache_rows(ds[0])]
    assert titles == ["Citer 0", "Citer 1", "Citer 2", "Citer 3"]
    assert gone["externalIds"]["DOI"] not in {r["citing_doi"] for r in read_csv(lib / "_forward_citations.csv")}


def test_count_zero_is_empty_with_no_call_and_replaces_the_rows(world, tmp_path):
    ds = dois(2)
    world.add(ds[0], 3).add(ds[1], 2)
    lib = make_lib(tmp_path, ds)
    fc.run(lib_dir=str(lib))
    world.papers[ds[0]]["count"] = 0
    world.sent.clear()
    res = fc.run(lib_dir=str(lib), force=True)
    assert world.nested_calls() == [] and world.get_calls() == []
    assert cache_states()[(ds[0], "s2")]["state"] == "empty" and cache_rows(ds[0]) is None
    assert res["zero_citers"] == 1 and res["exit_code"] == 0


def test_not_found_is_never_zero_citers_and_is_walked_again(world, tmp_path):
    ds = dois(2)
    world.add(ds[0], 3).add(ds[1], 2)
    lib = make_lib(tmp_path, ds)
    fc.run(lib_dir=str(lib))
    world.nested_null.add(pid_of(ds[0]))
    world.papers[ds[0]]["count"] = 4
    res = fc.run(lib_dir=str(lib))
    assert res["not_found"] == 1 and res["zero_citers"] == 0
    assert len([r for r in read_csv(lib / "_forward_citations.csv") if r["seed_doi"] == ds[0]]) == 3  # prior kept
    world.sent.clear()
    fc.run(lib_dir=str(lib))                             # same count, state not_found: walked again
    assert [c["json"]["ids"] for c in world.nested_calls()] == [[pid_of(ds[0])]]


def test_unresolved_seed_is_recorded_and_retried_next_run(world, tmp_path):
    ds = dois(2)
    world.add(ds[0], 2)                                  # ds[1]: S2 has no record
    lib = make_lib(tmp_path, ds)
    res = fc.run(lib_dir=str(lib))
    assert res["unresolved"] == 1 and res["exit_code"] == 0
    assert cache_states()[(ds[1], "s2")]["state"] == "unresolved"
    world.add(ds[1], 1)
    world.sent.clear()
    res = fc.run(lib_dir=str(lib))
    assert res["walked"] == 1 and [c["json"]["ids"] for c in world.nested_calls()] == [[pid_of(ds[1])]]


def test_refresh_walks_every_seed(world, tmp_path):
    ds = dois(3)
    for d in ds:
        world.add(d, 2)
    lib = make_lib(tmp_path, ds)
    fc.run(lib_dir=str(lib))
    world.sent.clear()
    res = fc.main(["--lib-dir", str(lib), "--refresh"])
    assert res == 0 and len(world.nested_calls()) == 1 and len(world.nested_calls()[0]["json"]["ids"]) == 3


# ================================================================ seeds
def test_a_placeholder_seed_is_never_walked_counted_apart_and_does_not_trip_the_guard(world, tmp_path, monkeypatch):
    ds = dois(2)
    for d in ds:
        world.add(d, 2)
    lib = make_lib(tmp_path, ds + ["10.1145/nnnnnnn.nnnnnnn"])
    (lib / "2020_Seed0002.fulltext.json").write_text(json.dumps(
        {"doi": "10.1145/nnnnnnn.nnnnnnn", "text": "body", "has_pdf": True}), encoding="utf-8")
    out = lib / "_forward_citations.csv"
    with open(out, "w", encoding="utf-8", newline="") as f:          # the research-library shape: published placeholder rows
        w = csv.DictWriter(f, fieldnames=fc.FIELDS[:10])
        w.writeheader()
        for k, d in enumerate(ds + ["10.1145/nnnnnnn.nnnnnnn"]):
            for i in range(6):
                w.writerow({"seed_pdf": f"2020_Seed{k:04d}.pdf", "seed_doi": d, "citing_paper_id": f"old{i}",
                            "citing_doi": f"10.7777/old.{k}.{i}"})
    texts = []
    monkeypatch.setattr(fc, "doi_from_pdf", lambda p, **k: texts.append(p) or "10.5555/cited.in.references")
    res = fc.run(lib_dir=str(lib))
    assert res["placeholder"] == 1 and res["no_doi"] == 0 and texts == []           # no PDF-text fallback
    assert all("nnnn" not in i for c in world.metadata_calls() for i in c["json"]["ids"])
    assert res["exit_code"] == 0 and res["prior_seeds_with_citers"] == 2 and res["seeds_with_citers"] == 2
    assert {r["seed_doi"] for r in read_csv(out)} == set(ds)                        # the 6 placeholder rows are gone


def test_a_placeholder_found_only_in_the_pdf_text_is_counted_apart(world, tmp_path):
    """The research-library shape (2026-10-06): a PDF with no .ris whose first page prints the ACM
    template DOI. The text rule drops it; the seed is counted as placeholder, not no_doi."""
    fitz = pytest.importorskip("fitz")
    ds = dois(1)
    world.add(ds[0], 1)
    lib = make_lib(tmp_path, ds)
    doc = fitz.open()
    doc.new_page().insert_text((72, 72), "ACM Reference Format. https://doi.org/10.1145/nnnnnnn.nnnnnnn")
    doc.save(str(lib / "2025_Unknown_TemplatePreprint.pdf"))
    doc.close()
    res = fc.run(lib_dir=str(lib))
    assert res["placeholder"] == 1 and res["no_doi"] == 0 and res["exit_code"] == 0
    assert world.metadata_calls()[0]["json"]["ids"] == [f"DOI:{ds[0]}"]


def test_text_only_holdings_are_seeds_and_flagged_files_are_not(world, tmp_path):
    ds = dois(5)
    for d in ds:
        world.add(d, 2)
    lib = make_lib(tmp_path, ds[:2])
    (lib / "2021_Text_Only.fulltext.json").write_text(json.dumps(
        {"doi": "ignored-the-ris-wins", "text": "jats body", "has_pdf": False}), encoding="utf-8")
    (lib / "2021_Text_Only.ris").write_text(f"TY  - JOUR\nDO  - {ds[2]}\nER  - \n", encoding="utf-8")
    (lib / "2019_Legacy_Jats.fulltext.json").write_text(json.dumps({"doi": ds[3], "text": "legacy"}), encoding="utf-8")
    (lib / "2018_Flagged.fulltext.json").write_text(json.dumps(
        {"doi": ds[4], "text": "x", "identity": "FLAG", "has_pdf": False}), encoding="utf-8")
    (lib / "2017_Wrong.pdf").write_bytes(b"%PDF-1.4 stub")
    (lib / "2017_Wrong.ris").write_text(f"TY  - JOUR\nDO  - {ds[4]}\nER  - \n", encoding="utf-8")
    (lib / "2017_Wrong.identity.json").write_text(json.dumps({"identity": "FLAG"}), encoding="utf-8")
    (lib / "2016_Orphan.fulltext.json").write_text(json.dumps(
        {"doi": "10.5555/orphan.1", "text": "pdf text", "extracted_from_pdf": True}), encoding="utf-8")
    res = fc.run(lib_dir=str(lib))
    assert res["exit_code"] == 0 and res["text_only_seeds"] == 2 and res["flagged"] == 2
    ids = {i for c in world.metadata_calls() for i in c["json"]["ids"]}
    assert ids == {f"DOI:{d}" for d in ds[:4]}
    by_file = {}
    for r in read_csv(lib / "_forward_citations.csv"):
        by_file.setdefault(r["seed_pdf"], set()).add(r["seed_doi"])
    assert by_file["2021_Text_Only.fulltext.json"] == {ds[2]} and by_file["2019_Legacy_Jats.fulltext.json"] == {ds[3]}
    assert "2017_Wrong.pdf" not in by_file and "2018_Flagged.fulltext.json" not in by_file


def test_a_text_only_seed_whose_doi_went_unreadable_keeps_the_guard(world, tmp_path):
    """F-4 with sidecars: the on_disk set includes text-only sidecar names."""
    ds = dois(3)
    for d in ds:
        world.add(d, 2)
    lib = make_lib(tmp_path, ds[:2])
    (lib / "2021_Text_Only.fulltext.json").write_text(json.dumps({"text": "body", "has_pdf": False}), encoding="utf-8")
    out = lib / "_forward_citations.csv"
    with open(out, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fc.FIELDS)
        w.writeheader()
        for name, d in (("2020_Seed0000.pdf", ds[0]), ("2020_Seed0001.pdf", ds[1]), ("2021_Text_Only.fulltext.json", ds[2])):
            w.writerow({"seed_pdf": name, "seed_doi": d, "citing_paper_id": "old", "citing_doi": "10.7777/x"})
    before = sha(out)
    res = fc.run(lib_dir=str(lib))
    assert res["no_doi"] == 1 and res["prior_seeds_with_citers"] == 3
    assert res["exit_code"] == 2 and sha(out) == before


def test_a_doi_held_by_two_files_is_walked_once_with_rows_for_each_pdfs_first(world, tmp_path):
    ds = dois(1)
    world.add(ds[0], 3)
    lib = make_lib(tmp_path, ds)
    (lib / "2022_Same_Paper.fulltext.json").write_text(json.dumps({"doi": ds[0], "text": "t", "has_pdf": False}),
                                                         encoding="utf-8")
    res = fc.run(lib_dir=str(lib))
    assert res["seed_walks"] == 1 and len(world.metadata_calls()[0]["json"]["ids"]) == 1
    files = [r["seed_pdf"] for r in read_csv(lib / "_forward_citations.csv")]
    assert files == ["2020_Seed0000.pdf"] * 3 + ["2022_Same_Paper.fulltext.json"] * 3


def test_structured_doi_forms_are_kept_whole(world, tmp_path):
    seed = "10.1088/2053-1591/acdecd"                    # the text rule would cut this to 10.1088/2053-1591
    citer = dict(s2_citer("x", 1), externalIds={"DOI": "10.1089/THER.2017.29031.MKB"})
    world.add(seed, 1, rows=[citer])
    lib = tmp_path / "literature"
    lib.mkdir()
    (lib / "2023_Letter.pdf").write_bytes(b"%PDF-1.4 stub")
    (lib / "2023_Letter.ris").write_text("TY  - JOUR\nDO  - https://doi.org/10.1088/2053-1591/ACDECD\nER  - \n",
                                         encoding="utf-8")
    res = fc.run(lib_dir=str(lib))
    assert res["exit_code"] == 0
    assert world.metadata_calls()[0]["json"]["ids"] == [f"DOI:{seed}"]
    row = read_csv(lib / "_forward_citations.csv")[0]
    assert row["seed_doi"] == seed and row["citing_doi"] == "10.1089/ther.2017.29031.mkb"
    assert fc.doi_from_ris(lib / "2023_Letter.ris") == seed


# ================================================================ aliases
def test_two_seeds_on_one_paper_are_walked_once_and_reported(world, tmp_path, capsys):
    pre, pub = "10.31234/osf.io/abcde", "10.5555/wk.published.2024"
    shared = pid_of("one-paper")
    world.add(pub, 3, pid=shared).add(pre, 3, pid=shared, s2_doi=pub)
    lib = make_lib(tmp_path, [pub, pre])
    res = fc.run(lib_dir=str(lib))
    assert res["exit_code"] == 0 and res["aliases"] == 2
    assert [c["json"]["ids"] for c in world.nested_calls()] == [[shared]]               # one paperId, one walk
    rows = read_csv(lib / "_forward_citations.csv")
    assert sum(r["seed_doi"] == pre for r in rows) == 3 and sum(r["seed_doi"] == pub for r in rows) == 3
    out = capsys.readouterr().out
    assert "[aliases]" in out and "same S2 paper as" in out and "S2 lists it as" in out
    assert cache_states()[(pre, "s2")]["n_rows"] == 3


# ================================================================ kills, isolation, the lock
def test_a_killed_run_keeps_every_committed_seed_in_the_cache(world, tmp_path):
    ds = dois(5)
    for d in ds:
        world.add(d, 1001)                               # paged: one walk per seed
    lib = make_lib(tmp_path, ds)
    world.interrupt_on = pid_of(ds[3])
    with pytest.raises(KeyboardInterrupt):
        fc.run(lib_dir=str(lib))
    st = cache_states()
    assert all(st[(d, "s2")]["state"] == "complete" for d in ds[:3]) and (ds[3], "s2") not in st
    world.interrupt_on = None
    world.sent.clear()
    res = fc.run(lib_dir=str(lib), restart=True)         # without the journal: the cache alone
    walked = {g["path"].split("/")[-2] for g in world.get_calls()}
    assert walked == {pid_of(ds[3]), pid_of(ds[4])}                                    # the one in flight again
    assert res["kept"] == 3 and res["exit_code"] == 0 and res["total_rows"] == 5 * 1001


def test_a_registry_without_state_dir_and_cache_path_set_leaves_the_real_state_dir_absent(world, tmp_path, monkeypatch):
    assert walk.CACHE_PATH is not None                   # conftest
    root = tmp_path / "root"
    ds = dois(2)
    for d in ds:
        world.add(d, 2)
    make_lib(root / "teaching_x", ds)
    reg = tmp_path / "registry.json"
    reg.write_text(json.dumps({"projects": {"teaching_x": {"lib_dir": "literature"}}}), encoding="utf-8")
    monkeypatch.setattr(fc, "CONFIG_PATH", reg)
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    # the machine's real state dir exists and is live since the cutover, so the check is on the DEFAULT
    # state dir under a temp home: the walk must not fall back to it
    home = tmp_path / "default_home"
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("USERPROFILE", str(home))
    default_state = Path.home() / ".local" / "db" / "literature_pipeline"
    assert str(default_state).startswith(str(home))
    res = fc.run(project="teaching_x")
    assert res["exit_code"] == 0 and Path(res["cache"]) == Path(walk.CACHE_PATH) and Path(walk.CACHE_PATH).exists()
    assert not default_state.exists()


def test_cache_path_resolves_state_dir_without_creating_it(tmp_path, monkeypatch):
    monkeypatch.setattr(walk, "CACHE_PATH", None)
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path / "home"))
    p = walk.cache_path({"projects": {}})
    assert p == tmp_path / "home" / ".local" / "db" / "literature_pipeline" / "s2_cache.duckdb"
    assert not (tmp_path / "home").exists()
    assert walk.cache_path({"state_dir": str(tmp_path / "s")}) == tmp_path / "s" / "s2_cache.duckdb"
    assert walk.cache_path(None, tmp_path / "x.duckdb") == tmp_path / "x.duckdb"
    c = walk.Cache(tmp_path / "deep" / "dir" / "c.duckdb")                          # reads never create it
    assert c.states() == {} and c.rows_many([("a", "s2")]) == {} and not (tmp_path / "deep").exists()
    c.record("10.1/a", "s2", state="complete", count=1, rows=[{"citing_paper_id": "p1", "citing_title": "t"}])
    assert (tmp_path / "deep" / "dir" / "c.duckdb").exists()                       # the first write makes it
    c.close()


def test_the_cache_kwarg_overrides_the_module_path(world, tmp_path):
    ds = dois(1)
    world.add(ds[0], 1)
    mine = tmp_path / "elsewhere" / "w.duckdb"
    res = fc.run(lib_dir=str(make_lib(tmp_path, ds)), cache_path=mine)
    assert res["exit_code"] == 0 and mine.exists() and not Path(walk.CACHE_PATH).exists()


def test_a_cache_held_by_another_process_exits_3_after_the_summary(world, tmp_path, capsys):
    ds = dois(2)
    for d in ds:
        world.add(d, 2)
    lib = make_lib(tmp_path, ds)
    cpath = tmp_path / "held.duckdb"
    walk.Cache(cpath).record("10.1/x", "s2", state="empty", count=0, rows=[])
    holder = subprocess.Popen([sys.executable, "-c",
                               "import duckdb, sys, time; c = duckdb.connect(sys.argv[1]); print('held', flush=True); "
                               "time.sleep(60)", str(cpath)], stdout=subprocess.PIPE, text=True)
    try:
        assert holder.stdout.readline().strip() == "held"
        capsys.readouterr()
        res = fc.run(lib_dir=str(lib), cache_path=cpath)
    finally:
        holder.kill()
        holder.wait()
    assert res["exit_code"] == 3 and res["aborted"] == "cache locked"
    assert world.sent == [] and not (lib / "_forward_citations.csv").exists()
    assert not fc.degraded_path(lib / "_forward_citations.csv").exists()
    last = capsys.readouterr().out.strip().splitlines()[-1]
    assert last.startswith(fc.SUMMARY_MARKER) and json.loads(last[len(fc.SUMMARY_MARKER):])["aborted"] == "cache locked"


def test_a_failed_cache_statement_rolls_back_and_fails_the_seed(world, tmp_path, monkeypatch):
    ds = dois(25)
    for d in ds:
        world.add(d, 3)
    lib = make_lib(tmp_path, ds)
    fc.run(lib_dir=str(lib))
    real = walk._citer_frame

    def poisoned(doi, source, rows):
        df = real(doi, source, rows)
        if doi == ds[5]:
            df.loc[0, "citing_id"] = None                # NOT NULL breaks the INSERT after the DELETE
        return df
    monkeypatch.setattr(walk, "_citer_frame", poisoned)
    for d in ds:                                         # every seed re-walked (I-1: kept seeds do not dilute)
        world.papers[d]["count"] = world.papers[d]["rows"] = 4
    res = fc.run(lib_dir=str(lib))
    assert res["failed_cache"] == 1 and res["failed"] == 1 and res["exit_code"] == 0     # 1 of 25
    assert len(cache_rows(ds[5])) == 3                                                  # rolled back, rows kept
    st = cache_states()[(ds[5], "s2")]
    assert st["state"] == "failed" and st["kind"] == "CACHE" and st["count_at_walk"] == 4
    assert sum(r["seed_doi"] == ds[5] for r in read_csv(lib / "_forward_citations.csv")) == 3


# ================================================================ --source openalex
def test_source_openalex_walks_every_seed_and_a_429_defers_exit_3_never_retried(world, tmp_path, monkeypatch):
    monkeypatch.setenv(openalex.KEY_ENV, "oa-test-key-not-real")
    ds = dois(3)
    world.add(ds[0], 2, oa=150).add(ds[1], 2, oa=40).add(ds[2], 2, oa=10)
    world.papers[ds[2]]["count"] = 2
    lib = make_lib(tmp_path, ds)
    world.oa_fail_at = None
    res = fc.run(lib_dir=str(lib), source="openalex")
    assert res["exit_code"] == 0 and res["openalex_walked"] == 3 and world.nested_calls() == []
    assert {r["source"] for r in read_csv(lib / "_forward_citations.csv")} == {"openalex"}
    world.sent.clear()
    for d in ds:
        world.papers[d]["count"] += 1                    # the S2 count gate wants all three again
    world.oa_fail_at = (1, 429, {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "900"})
    res = fc.run(lib_dir=str(lib), source="openalex")
    assert res["exit_code"] == 3 and res["aborted"] == "openalex budget" and not res["published"]
    assert sum(1 for r in world.oa_calls() if r["params"].get("cursor") == "c1") == 1  # never retried
    assert res["not_walked"] == 2


def test_source_openalex_citers_without_a_doi_count_but_are_not_candidates(world, tmp_path, monkeypatch):
    monkeypatch.setenv(openalex.KEY_ENV, "oa-test-key-not-real")
    ds = dois(1)
    world.add(ds[0], 1, oa=3)
    real = world._openalex

    def no_doi(path, q):
        raw = real(path, q)
        if path == "/works":
            body = json.loads(raw.content)
            body["results"][1]["doi"] = None
            return _raw(200, body, RL)
        return raw
    world._openalex = no_doi
    lib = make_lib(tmp_path, ds)
    res = fc.run(lib_dir=str(lib), source="openalex")
    rows = read_csv(lib / "_forward_citations.csv")
    assert res["exit_code"] == 0 and len(rows) == 3 and sum(1 for r in rows if not r["citing_doi"]) == 1
    assert res["unique_dois"] == 2


# ================================================================ the planner and the CLI shape
def test_the_planner_gives_602_then_11_on_the_stored_count_vector():
    counts = json.loads((REPO / "tests/fixtures/W3-A/library_s2_counts.json").read_text())["citationCount"]
    c = dict(enumerate(counts))
    first = walk.plan(c)
    assert (first["metadata"], first["nested_bins"], first["pages"] + first["windows"]) == (11, 35, 556)
    assert first["s2_calls"] == 602 and first["openalex_calls"] == 0
    cached = {}
    for i, n in c.items():
        if n is not None:
            r = walk.route(n)
            cached[(i, walk.route_source(r))] = {"state": {"empty": "empty", "windows": "capped_9999"}.get(r, "complete"),
                                                 "count_at_walk": n}
    assert walk.plan(c, cached)["s2_calls"] == 11
    keyed = walk.plan(c, openalex_key=True)
    assert keyed["windows"] == 0 and keyed["openalex_singletons"] == 8


def test_the_plan_is_printed_on_every_real_run(world, tmp_path, capsys):
    ds = dois(2)
    world.add(ds[0], 2).add(ds[1], 1200)
    fc.run(lib_dir=str(make_lib(tmp_path, ds)))
    out = capsys.readouterr().out
    assert "[plan] S2 calls 4 = metadata 1 + nested bins 1 + pages 2 + window calls 0" in out


def test_main_keeps_every_flag_and_adds_the_new_ones(world, tmp_path, capsys):
    ds = dois(2)
    world.add(ds[0], 1).add(ds[1], 1)
    lib = make_lib(tmp_path, ds)
    assert fc.main(["--lib-dir", str(lib), "--limit", "1", "--sleep", "0", "--force", "--restart"]) == 0
    assert fc.main(["--lib-dir", str(lib), "--refresh", "--source", "s2", "--report", str(tmp_path / "r.csv")]) == 0
    assert (tmp_path / "r.csv").exists()
    assert fc.main(["--lib-dir", str(lib), "--seeds-from", str(tmp_path / "x.csv")]) == 1      # needs --scope
    assert fc.main(["--lib-dir", str(lib), "--scope", "teach"]) == 1                          # needs --seeds-from
    with pytest.raises(SystemExit):
        fc.main(["--lib-dir", str(lib), "--source", "crossref"])
    proc = subprocess.run([sys.executable, str(REPO / "forward_citations.py"), "--help"], cwd=tmp_path,
                          capture_output=True, text=True, encoding="utf-8", timeout=120)
    assert proc.returncode == 0
    for flag in ("--refresh", "--seeds-from", "--scope", "--source", "--restart", "--force", "--limit", "--report"):
        assert flag in proc.stdout
    assert "--dry-run" not in proc.stdout


def test_snowball_still_imports_doi_from_ris(tmp_path):
    import snowball
    lib = tmp_path / "lib"
    lib.mkdir()
    (lib / "a.pdf").write_bytes(b"%PDF")
    (lib / "a.ris").write_text("TY  - JOUR\nDO  - 10.5555/FP.1\nER  - \n", encoding="utf-8")
    (lib / "b.ris").write_text("TY  - JOUR\nDO  - 10.1145/nnnnnnn.nnnnnnn\nER  - \n", encoding="utf-8")
    assert snowball.library_fingerprint(lib) == (("a.pdf",), ("10.5555/fp.1",), ())   # W4b: + text-only sidecars
