"""Verifier I (W3b): adversarial locks on the forward walk (W3-A: forward_citations, litpipe.walk).

What these add to the builder's tests:
  - every path to the real state dir, with the conftest CACHE_PATH line removed (Path.home pointed
    at a temp home, so "the real state dir" is a temp path and nothing real is touched);
  - the REAL forward_citations.run on the REAL litpipe.s2, litpipe.walk cache and litpipe.state
    (temp files) behind tests/netmock.py's MockServer (real HTTP on loopback): clean, throttled,
    resume, unchanged, changed and killed runs;
  - the cache under a lock that appears mid-run (a second real process) and a failed statement,
    with every statement logged (no COMMIT after a failed statement);
  - the 5 % failure denominator with gate-kept seeds (the defect: fails on 50789ae, fixed since);
  - a seed whose OpenAlex walk is stopped mid-seed by the run budget (the defect: fails on 50789ae, fixed since);
  - snowball's reading of every exit through the REAL forward_citations.main in-process;
  - the REAL index_portfolio ingesting the regenerated CSV (OpenAlex W-ids, text-only seeds,
    letter-tail DOIs, abstracts kept, scoped rows only in scoped_*);
  - the planner against the calls a mixed run actually makes.
Offline: World (tests/test_walk_forward.py) answers like S2 and OpenAlex; HttpWorld serves it over
loopback HTTP."""
import contextlib
import csv
import io
import json
import os
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlencode

import duckdb
import pytest

import forward_citations as fc
import index_portfolio as I
import lit_util
import snowball
from litpipe import config, hosts, net, openalex, s2, walk
from litpipe import state as lstate
from tests import netmock
from tests.test_walk_forward import (World, dois, make_lib, oa_work, read_csv, s2_citer, sha,  # noqa: F401
                                     world)

REPO = Path(__file__).resolve().parent.parent
EVIDENCE = Path(os.environ.get("LITPIPE_VERIFY_EVIDENCE", "")) if os.environ.get("LITPIPE_VERIFY_EVIDENCE") else None
OA_KEY = "oa-secret-KEY-9f8e7d6c"
ARXIV = ("export.arxiv.org", "arxiv.org", "www.arxiv.org")


def evidence(name, obj):
    """Write a data file to the verifier's scratch folder when LITPIPE_VERIFY_EVIDENCE is set."""
    if EVIDENCE is not None:
        EVIDENCE.mkdir(parents=True, exist_ok=True)
        (EVIDENCE / name).write_text(json.dumps(obj, indent=1, default=str), encoding="utf-8")


def write_registry(path, projects, **top):
    path.write_text(json.dumps({"projects": projects, **top}), encoding="utf-8")
    return path


def last_line(text):
    lines = [ln for ln in text.strip().splitlines() if ln.strip()]
    return lines[-1] if lines else ""


def summary_of(text):
    ln = last_line(text)
    assert ln.startswith(fc.SUMMARY_MARKER), f"last line is not the summary: {ln[:120]!r}"
    return json.loads(ln[len(fc.SUMMARY_MARKER):])


# ================================================================ item 1: the real state dir
@pytest.fixture
def fake_home(tmp_path, monkeypatch):
    """Path.home() -> a temp home, and the conftest CACHE_PATH line removed: "the real state dir"
    is <temp home>/.local/db/literature_pipeline for this test."""
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: home))
    monkeypatch.setattr(walk, "CACHE_PATH", None)
    return home / ".local" / "db" / "literature_pipeline"


def _production_shaped(monkeypatch):
    """No test overrides left for state, ledger or cache: what a live run resolves."""
    monkeypatch.setattr(net, "STATE", None)
    monkeypatch.setattr(lstate, "DB_PATH", None)
    from litpipe import ledger
    monkeypatch.setattr(ledger, "LEDGER_DIR", None)


def test_sd_cache_path_and_a_seedless_run_never_create_the_state_dir(world, fake_home, tmp_path, monkeypatch):
    reg_path = write_registry(tmp_path / "reg.json", {"research_a": {"lib_dir": "literature"}})
    monkeypatch.setattr(config, "CONFIG_PATH", reg_path)
    monkeypatch.setattr(fc, "CONFIG_PATH", reg_path)
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", tmp_path / "root")
    assert walk.cache_path(config.load()) == fake_home / walk.CACHE_NAME and not fake_home.exists()
    _production_shaped(monkeypatch)
    make_lib(tmp_path / "root" / "research_a", [None, None], ris=False)      # PDFs without a DOI
    res = fc.run(project="research_a")
    assert res["exit_code"] == 0 and res["seed_walks"] == 0 and world.sent == []
    assert not fake_home.exists()                                           # nothing sent, nothing created
    bad = fc.run(project="nope")
    assert bad["exit_code"] == 1 and not fake_home.exists()


def test_sd_a_live_shaped_run_creates_the_state_dir_at_its_first_write(world, fake_home, tmp_path, monkeypatch):
    """The meant creation: a live run on a registry without state_dir makes ~/.local/db/... (cache,
    state and ledger) once it sends and writes."""
    reg_path = write_registry(tmp_path / "reg.json", {"research_a": {"lib_dir": "literature"}})
    monkeypatch.setattr(config, "CONFIG_PATH", reg_path)
    monkeypatch.setattr(fc, "CONFIG_PATH", reg_path)
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", tmp_path / "root")
    _production_shaped(monkeypatch)
    monkeypatch.setattr(lstate, "_sleep", lambda s: None)
    ds = dois(2)
    for d in ds:
        world.add(d, 2)
    make_lib(tmp_path / "root" / "research_a", ds)
    assert not fake_home.exists()
    res = fc.run(project="research_a")
    assert res["exit_code"] == 0
    created = sorted(p.name for p in fake_home.iterdir())
    evidence("sd_live_shaped_run.json", {"created": created})
    assert walk.CACHE_NAME in created and "litpipe_state.sqlite" in created


def test_sd_lib_dir_mode_reads_litpipe_config_not_the_module_config_path(world, fake_home, tmp_path, monkeypatch):
    """--lib-dir resolves the cache through litpipe.config.CONFIG_PATH (config.load()), whatever
    forward_citations.CONFIG_PATH says (spec amendment 1). Both default to the repo's projects.json."""
    with_sd = write_registry(tmp_path / "with.json", {}, state_dir=str(tmp_path / "sd"))
    without = write_registry(tmp_path / "without.json", {})
    monkeypatch.setattr(fc, "CONFIG_PATH", with_sd)
    monkeypatch.setattr(config, "CONFIG_PATH", without)
    ds = dois(1)
    world.add(ds[0], 1)
    res = fc.run(lib_dir=str(make_lib(tmp_path, ds)))
    assert res["exit_code"] == 0 and Path(res["cache"]) == fake_home / walk.CACHE_NAME
    assert (fake_home / walk.CACHE_NAME).exists() and not (tmp_path / "sd").exists()


def test_sd_scoped_holdings_cache_sits_beside_an_overridden_walk_cache(world, fake_home, tmp_path, monkeypatch):
    """A scoped clean run with cache_path= (W4) and a registry without state_dir: the holdings cache
    goes beside the walk cache, never to the real state dir."""
    root = tmp_path / "root"
    held_lib = make_lib(root / "research_b", [])
    reg_path = write_registry(tmp_path / "reg.json", {"research_b": {"lib_dir": "literature"}})
    monkeypatch.setattr(config, "CONFIG_PATH", reg_path)
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    seed = "10.5555/sc.0001"
    world.add(seed, 2)
    lst = tmp_path / "list.csv"
    lst.write_text(f"doi\n{seed}\n", encoding="utf-8")
    cpath = tmp_path / "w4" / "s2_cache.duckdb"
    res = fc.run(lib_dir=str(held_lib), seeds_from=[str(lst)], scope="sc", cache_path=cpath)
    assert res["exit_code"] == 0 and res["descendants"]["rows"] == 2
    from litpipe import holdings
    assert not fake_home.exists()
    assert (cpath.parent / walk.CACHE_NAME).exists() and (cpath.parent / holdings.CACHE_NAME).exists()


def test_sd_help_and_import_create_nothing_from_a_foreign_cwd(tmp_path):
    home = tmp_path / "home"
    home.mkdir()
    env = {**os.environ, "USERPROFILE": str(home), "HOME": str(home), "PYTHONUTF8": "1"}
    for script in ("forward_citations.py", "snowball.py"):
        p = subprocess.run([sys.executable, str(REPO / script), "--help"], cwd=tmp_path, env=env,
                           capture_output=True, text=True, timeout=120)
        assert p.returncode == 0, p.stderr[-400:]
    assert not (home / ".local").exists()


def test_sd_an_unreadable_cache_is_a_clean_config_error_not_a_crash(world, tmp_path):
    """A cache file DuckDB cannot open (corrupt, or not a database) must end the run as a config
    error (exit 1, a message naming the file), never an uncaught traceback, and send nothing."""
    ds = dois(1)
    world.add(ds[0], 1)
    cpath = tmp_path / "bad.duckdb"
    cpath.write_bytes(b"this is not a duckdb file" * 200)
    try:
        res = fc.run(lib_dir=str(make_lib(tmp_path, ds)), cache_path=cpath)
    except Exception as e:                                   # noqa: BLE001
        pytest.fail(f"uncaught {type(e).__name__}: {str(e)[:160]}")
    assert res["exit_code"] == 1 and world.sent == []


# ================================================================ item 2: real modules over HTTP
class HttpWorld(netmock.MockServer):
    """A MockServer that answers every request through a World (real HTTP on loopback). `throttle`
    answers 429 to every S2 citations request (nested POST and paged GET)."""

    def __init__(self, w, upstream, host):
        self.world, self.upstream, self.throttle = w, upstream, False
        super().__init__(host)

    def _next(self, path):
        hit = self.hits[-1]
        q = {k: v[-1] for k, v in hit.query.items()}
        if self.throttle and ("citations" in q.get("fields", "") or path.endswith("/citations")):
            return netmock.Reply(429, b'{"message": "Too Many Requests"}', {"Content-Type": "application/json"})
        url = f"https://{self.upstream}{path}" + (f"?{urlencode(q)}" if q else "")
        raw = self.world(hit.method, url, hit.headers, hit.body or None, None, None)
        return netmock.Reply(raw.status, raw.content, {k: v for k, v in raw.headers.items()})


@pytest.fixture
def http(net_env, monkeypatch, tmp_path):
    """World behind two loopback servers (S2 127.0.0.1, OpenAlex 127.0.0.2), the REAL litpipe.state
    on a temp file (conftest's DB_PATH) with its clock virtual, arXiv refused manually first."""
    monkeypatch.delenv(s2.KEY_ENV, raising=False)
    monkeypatch.delenv(openalex.KEY_ENV, raising=False)
    w = World()
    s2srv = HttpWorld(w, "api.semanticscholar.org", "127.0.0.1")
    oasrv = HttpWorld(w, "api.openalex.org", "127.0.0.2")
    monkeypatch.setattr(s2, "BASE", s2srv.url("").rstrip("/"))
    monkeypatch.setattr(openalex, "BASE", oasrv.url("").rstrip("/"))
    monkeypatch.setattr(net, "STATE", None)                 # -> the real litpipe.state module
    monkeypatch.setattr(lstate, "_time", net_env.clock.time)
    monkeypatch.setattr(lstate, "_sleep", net_env.clock.sleep)
    for h in ARXIV:
        lstate.refuse(h, "manual: refused since 2026-09-25", persistence="manual")
    s2.reset_default_session()
    openalex.reset_default_session()
    w.s2srv, w.oasrv, w.env = s2srv, oasrv, net_env
    yield w
    s2srv.stop()
    oasrv.stop()
    s2.reset_default_session()
    openalex.reset_default_session()
    hosts.reset()


def _hits(srv, kind):
    out = []
    for h in srv.hits:
        q = {k: v[-1] for k, v in h.query.items()}
        if kind == "meta" and h.path.endswith("/paper/batch") and "citations." not in q.get("fields", ""):
            out.append(h)
        elif kind == "nested" and h.path.endswith("/paper/batch") and "citations." in q.get("fields", ""):
            out.append(h)
        elif kind == "get" and h.path.endswith("/citations"):
            out.append(h)
    return out


def test_e2e_real_modules_clean_unchanged_changed_and_the_key_never_persisted(http, tmp_path, monkeypatch, capsys):
    monkeypatch.setenv(openalex.KEY_ENV, OA_KEY)
    ds = ["10.5555/e2e.0001", "10.5555/e2e.0002", "10.5555/e2e.0003", "10.5555/e2e.0004"]
    rows3 = [s2_citer(ds[1], i) for i in range(3)]
    http.add(ds[0], 0).add(ds[1], 3, rows=rows3).add(ds[2], 1500).add(ds[3], 12000, oa=150)
    lib = make_lib(tmp_path, ds)
    out = lib / "_forward_citations.csv"
    first = fc.run(lib_dir=str(lib))
    assert first["exit_code"] == 0, first["reasons"]
    assert len(_hits(http.s2srv, "meta")) == 1 and len(_hits(http.s2srv, "nested")) == 1
    assert len(_hits(http.s2srv, "get")) == 2                               # 1,500 citers: 2 pages
    csv1 = read_csv(out)
    assert list(csv1[0].keys()) == fc.FIELDS
    oa_rows = [r for r in csv1 if r["seed_doi"] == ds[3]]
    assert len(oa_rows) == 150 and all(r["source"] == "openalex" and r["citing_paper_id"].startswith("W")
                                       for r in oa_rows)
    assert {r["citing_oa"] for r in oa_rows} == {"true", "false"} and all(r["citing_abstract"] == "" for r in csv1)
    # the key: only in the Authorization header, never in a URL or anything persisted
    oa_hits = http.oasrv.hits
    assert oa_hits and all(h.headers.get("Authorization") == f"Bearer {OA_KEY}" for h in oa_hits)
    assert not any(OA_KEY in h.path or OA_KEY in json.dumps(h.query) for h in oa_hits)
    with walk.Cache(walk.CACHE_PATH) as c:
        dump = json.dumps([list(c.states().items()),
                           list(c.rows_many((d, s) for d in ds for s in walk.SOURCES).items())], default=str)
    persisted = {"stdout": capsys.readouterr().out, "csv": out.read_text(encoding="utf-8"),
                 "ledger": http.env.ledger_text(), "cache": dump, "result": json.dumps(first, default=str)}
    assert {k for k, v in persisted.items() if OA_KEY in v} == set()
    email = os.environ["LITPIPE_EMAIL"]                                     # net_env's test contact address
    assert {k for k, v in persisted.items() if email in v} == set()

    # an unchanged second run: metadata only
    n_s2, n_oa = len(http.s2srv.hits), len(http.oasrv.hits)
    before = sha(out)
    second = fc.run(lib_dir=str(lib))
    assert second["exit_code"] == 0 and second["kept"] == 4 and second["walked"] == 0
    assert len(http.s2srv.hits) - n_s2 == 1 and len(http.oasrv.hits) == n_oa and sha(out) == before

    # a changed count: re-walked, and the citer that left is gone (replace-merge)
    http.papers[ds[1]]["rows"] = rows3[1:] + [s2_citer(ds[1], 7), s2_citer(ds[1], 8)]
    http.papers[ds[1]]["count"] = 4
    third = fc.run(lib_dir=str(lib))
    assert third["exit_code"] == 0 and third["walked"] == 1 and third["kept"] == 3
    titles = {r["citing_title"] for r in read_csv(out) if r["seed_doi"] == ds[1]}
    assert titles == {"Citer 1", "Citer 2", "Citer 7", "Citer 8"}
    evidence("e2e_clean_unchanged_changed.json", {"first_s2": first["s2"], "second_s2": second["s2"],
                                                  "third_s2": third["s2"], "first_plan": first["plan"]})


def test_e2e_throttled_run_exits_3_prior_csv_byte_identical_then_resumes(http, tmp_path, capsys):
    ds = dois(5, "10.5555/thr")
    for d in ds:
        http.add(d, 1100)                                        # paged: one GET per seed per page
    lib = make_lib(tmp_path, ds)
    out = lib / "_forward_citations.csv"
    assert fc.run(lib_dir=str(lib))["exit_code"] == 0
    before = sha(out)
    for d in ds[1:]:
        http.papers[d]["count"] = http.papers[d]["rows"] = 1101  # four seeds need a walk
    http.s2srv.throttle = True                                   # every citations request: 429
    capsys.readouterr()
    res = fc.run(lib_dir=str(lib))
    text = capsys.readouterr().out
    assert res["exit_code"] == 3 and res["aborted"] == "breaker", res["reasons"]
    assert summary_of(text)["exit_code"] == 3 and res["not_walked"] == 1 and res["failed"] == 3
    assert sha(out) == before and fc.journal_path(out).exists()
    http.s2srv.throttle = False
    # The final 429 refused the host for the refusing run (litpipe.state, persistence "run", keyed to this
    # pid): a rerun is a new process, whose run the refusal does not reach. In-process, end it by hand.
    assert lstate.is_refused("127.0.0.1")
    lstate.clear_refusal("127.0.0.1")
    n_meta = len(_hits(http.s2srv, "meta"))
    again = fc.run(lib_dir=str(lib))
    assert again["exit_code"] == 0 and again["resumed"] == 1              # the kept seed
    resumed_meta = _hits(http.s2srv, "meta")[n_meta:]
    sent_ids = {i[len("DOI:"):] for h in resumed_meta for i in json.loads(h.body)["ids"]}
    assert sent_ids == set(ds[1:])                                        # answered seeds not asked again
    assert {r["seed_doi"] for r in read_csv(out) if r["citing_title"] == "Citer 1100"} == set(ds[1:])
    evidence("e2e_throttled.json", {"throttled": {k: res[k] for k in ("exit_code", "aborted", "reasons", "s2")},
                                    "resumed": {k: again[k] for k in ("exit_code", "resumed", "walked", "kept")}})


def test_e2e_a_429_on_the_one_nested_call_is_degraded_not_aborted(http, tmp_path):
    """One failed nested POST is one failed call: below the breaker (3), so its seeds fail (exit 2,
    DEGRADED) and the prior report stays byte-identical."""
    ds = dois(5, "10.5555/thn")
    for d in ds:
        http.add(d, 2)
    lib = make_lib(tmp_path, ds)
    out = lib / "_forward_citations.csv"
    assert fc.run(lib_dir=str(lib))["exit_code"] == 0
    before = sha(out)
    for d in ds[2:]:
        http.papers[d]["count"] = http.papers[d]["rows"] = 3
    http.s2srv.throttle = True
    res = fc.run(lib_dir=str(lib))
    assert res["exit_code"] == 2 and res["failed"] == 3 and res["transport_failures"] == 3 and res["aborted"] is None
    assert sha(out) == before


def test_e2e_a_kill_mid_bin_keeps_committed_seeds_and_rewalks_the_rest(http, tmp_path, monkeypatch):
    ds = dois(6, "10.5555/kil")
    for d in ds:
        http.add(d, 2)
    lib = make_lib(tmp_path, ds)
    real = walk.Cache.record
    calls = {"n": 0}

    def dying(self, *a, **k):
        calls["n"] += 1
        if calls["n"] == 3:
            raise KeyboardInterrupt("killed mid-bin")
        return real(self, *a, **k)
    monkeypatch.setattr(walk.Cache, "record", dying)
    with pytest.raises(KeyboardInterrupt):
        fc.run(lib_dir=str(lib))
    monkeypatch.setattr(walk.Cache, "record", real)
    with walk.Cache(walk.CACHE_PATH) as c:
        committed = {d for (d, _s), st in c.states().items() if st["rows_at"] is not None}
    assert committed == set(ds[:2])
    n_nested = len(_hits(http.s2srv, "nested"))
    res = fc.run(lib_dir=str(lib))
    assert res["exit_code"] == 0 and res["resumed"] == 2
    nested = _hits(http.s2srv, "nested")[n_nested:]
    assert len(nested) == 1 and len(json.loads(nested[0].body)["ids"]) == 4
    assert len(read_csv(lib / "_forward_citations.csv")) == 12


# ================================================================ item 3: the cache under contention
class _LoggingCon:
    """Forwards to a DuckDB connection and logs (statement, ok) for every execute."""

    def __init__(self, con, log):
        self._con, self._log = con, log

    def execute(self, sql, *a, **k):
        try:
            r = self._con.execute(sql, *a, **k)
        except Exception:
            self._log.append((" ".join(str(sql).split())[:60], False))
            raise
        self._log.append((" ".join(str(sql).split())[:60], True))
        return r

    def __getattr__(self, name):
        return getattr(self._con, name)


def test_no_commit_follows_a_failed_statement_and_the_prior_rows_survive(world, tmp_path, monkeypatch):
    ds = dois(3, "10.5555/rb")
    for d in ds:
        world.add(d, 3)
    lib = make_lib(tmp_path, ds)
    assert fc.run(lib_dir=str(lib))["exit_code"] == 0
    log = []
    real_connect = duckdb.connect
    monkeypatch.setattr(duckdb, "connect", lambda *a, **k: _LoggingCon(real_connect(*a, **k), log))
    real_frame = walk._citer_frame

    def poisoned(doi, source, rows):
        df = real_frame(doi, source, rows)
        if doi == ds[1]:
            df.loc[1, "citing_id"] = None                   # NOT NULL: the INSERT fails after the DELETE
        return df
    monkeypatch.setattr(walk, "_citer_frame", poisoned)
    world.papers[ds[1]]["count"] = world.papers[ds[1]]["rows"] = 4
    res = fc.run(lib_dir=str(lib))
    assert res["failed_cache"] == 1
    for i, (sql, ok) in enumerate(log):
        if not ok:
            assert log[i + 1][0].startswith("ROLLBACK"), log[i - 2:i + 3]
    tx, failed_in_tx = False, False
    for sql, ok in log:                                     # no COMMIT inside a transaction that failed
        if sql.startswith("BEGIN"):
            tx, failed_in_tx = True, False
        elif sql.startswith("ROLLBACK"):
            tx = failed_in_tx = False
        elif sql.startswith("COMMIT"):
            assert not failed_in_tx, log
            tx = False
        elif tx and not ok:
            failed_in_tx = True
    with walk.Cache(walk.CACHE_PATH) as c:
        assert len(c.rows(ds[1], "s2")) == 3 and c.states()[(ds[1], "s2")]["state"] == "failed"
    evidence("cache_statement_log.json", log)


def test_a_lock_taken_mid_run_by_a_second_process_aborts_exit_3_and_resumes(world, tmp_path, monkeypatch, capsys):
    """The cache file does not exist when the run starts; another real process creates and holds it
    before the first write. Exit 3 after the summary line; the next run resumes."""
    ds = dois(4, "10.5555/lk")
    for d in ds:
        world.add(d, 2)
    lib = make_lib(tmp_path, ds)
    cpath = tmp_path / "late" / "s2_cache.duckdb"
    cpath.parent.mkdir()
    real_ensure = walk.Cache._ensure
    holder = {}

    def ensure(self):
        if self.con is None and "p" not in holder:
            holder["p"] = subprocess.Popen(
                [sys.executable, "-c", "import duckdb, sys, time; c = duckdb.connect(sys.argv[1]); "
                 "print('held', flush=True); time.sleep(60)", str(cpath)], stdout=subprocess.PIPE, text=True)
            assert holder["p"].stdout.readline().strip() == "held"
        return real_ensure(self)
    monkeypatch.setattr(walk.Cache, "_ensure", ensure)
    capsys.readouterr()
    try:
        res = fc.run(lib_dir=str(lib), cache_path=cpath)
    finally:
        holder["p"].kill()
        holder["p"].wait()
    text = capsys.readouterr().out
    assert res["exit_code"] == 3 and res["aborted"] == fc.CACHE_LOCKED and not res["published"]
    assert summary_of(text)["aborted"] == fc.CACHE_LOCKED
    assert not (lib / "_forward_citations.csv").exists()
    monkeypatch.setattr(walk.Cache, "_ensure", real_ensure)
    again = fc.run(lib_dir=str(lib), cache_path=cpath)
    assert again["exit_code"] == 0 and len(read_csv(lib / "_forward_citations.csv")) == 8


# ================================================================ item 4: the failure denominator
def test_verdict_with_the_dispatch_numbers_hides_a_half_failed_walk_in_the_whole_run():
    """1,000 gate-kept + 60 walked, 30 failed: 2.8 % of every seed, 50 % of the walked ones."""
    assert fc.verdict(1060, 30, 1060, 1060) == (0, [])                     # the call site's denominator today
    assert fc.verdict(60, 30, 1060, 1060)[0] == 2                          # the walked seeds' denominator


def test_half_the_walked_seeds_failing_beside_many_kept_ones_is_degraded(world, tmp_path):
    world.env.write_config(s2={"breaker": 1000})
    ds = dois(212, "10.5555/den")
    for d in ds:
        world.add(d, 1)
    lib = make_lib(tmp_path, ds)
    assert fc.run(lib_dir=str(lib))["exit_code"] == 0
    walked = ds[200:]                                      # 12 change their count; 6 of them fail
    for d in walked:
        world.papers[d]["count"] = world.papers[d]["rows"] = 2
    for d in walked[::2]:
        world.fail(d, 500)
    res = fc.run(lib_dir=str(lib))
    evidence("denominator_case.json", {k: res[k] for k in ("exit_code", "failed", "kept", "walked", "seed_walks",
                                                           "reasons", "aborted")})
    assert res["kept"] == 200 and res["failed"] == 6 and res["aborted"] is None
    assert res["exit_code"] == 2 and not res["published"]


# ================================================================ item 8: OpenAlex stopped mid-seed
def test_a_seed_whose_openalex_walk_is_stopped_mid_seed_is_never_reported_fine(world, tmp_path, monkeypatch):
    monkeypatch.setenv(openalex.KEY_ENV, OA_KEY)
    ds = ["10.5555/oab.0001", "10.5555/oab.0002"]
    world.add(ds[0], 10100, oa=250).add(ds[1], 10200, oa=50, undated=3)
    lib = make_lib(tmp_path, ds)
    oa = openalex.Session(budget=2)                       # the singleton and page 1 of 3; page 2 not sent
    res = fc.run(lib_dir=str(lib), oa_session=oa)
    evidence("openalex_mid_seed_budget.json", {k: res.get(k) for k in ("exit_code", "not_walked", "failed", "capped",
                                                                       "openalex_walked", "published", "reasons")})
    assert res["not_walked"] == 0                         # every seed has an answer or a failure
    with walk.Cache(walk.CACHE_PATH) as c:
        st = c.states()
    assert any(d == ds[0] for d, _s in st), "the seed in flight left no trace in the cache"
    if res["exit_code"] == 0:
        assert st[(ds[0], "s2")]["state"] == "capped_9999"     # the fix: it falls back to S2 windows


def test_an_openalex_count_mismatch_fails_the_seed_and_keeps_its_rows(world, tmp_path, monkeypatch):
    """The open question, pinned as built: no recount; the seed is failed, its prior rows kept, and the
    gate walks it again next run."""
    monkeypatch.setenv(openalex.KEY_ENV, OA_KEY)
    d = "10.5555/oam.0001"
    world.add(d, 10100, oa=120)
    lib = make_lib(tmp_path, [d])
    assert fc.run(lib_dir=str(lib))["exit_code"] == 0
    world.papers[d]["count"] = 10101
    real = walk.openalex_citing
    monkeypatch.setattr(walk, "openalex_citing",
                        lambda w: {**real(w), "citing_paper_id": "W1"} if w.get("id", "").endswith("05") else real(w))
    res = fc.run(lib_dir=str(lib))                        # two works share a W-id: 119 rows against meta.count 120
    with walk.Cache(walk.CACHE_PATH) as c:
        st = c.states()[(d, "openalex")]
        assert st["state"] == "failed" and st["kind"] == "COUNT_MISMATCH" and len(c.rows(d, "openalex")) == 120
    assert res["failed"] == 1 and res["recounted"] == 0 and res["exit_code"] == 2


# ================================================================ item 5: snowball reads every exit
def _snowball_env(tmp_path, monkeypatch, projects, **top):
    root = tmp_path / "root"
    reg = write_registry(tmp_path / "reg.json", projects, state_dir=str(tmp_path / "sd"), **top)
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    monkeypatch.setattr(fc, "CONFIG_PATH", reg)
    monkeypatch.setattr(config, "CONFIG_PATH", reg)
    monkeypatch.setattr(snowball, "CONFIG_PATH", reg)
    monkeypatch.setattr(snowball, "DB_PATH", tmp_path / "refs" / "portfolio.duckdb")
    monkeypatch.setattr(snowball, "LOG_PATH", tmp_path / "refs" / "convergence_log.csv")
    return root


def _inproc_runner(captured, extra=()):
    def runner(cmd, label):
        if label != "forward_citations":
            return snowball.StepResult(label, cmd, 0)
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = fc.main([str(c) for c in cmd[2:]] + list(extra))
        text = buf.getvalue()
        summ = None
        for ln in text.splitlines():
            if ln.startswith(snowball.SUMMARY_MARKER):
                summ = snowball._summary(ln) or summ
        captured.append((rc, text))
        return snowball.StepResult(label, cmd, rc, summ, len(text.splitlines()))
    return runner


def _one_snowball(world, tmp_path, monkeypatch, *, n=4, count=2, extra=(), top=None, setup=None):
    root = _snowball_env(tmp_path, monkeypatch, {"research_a": {"lib_dir": "literature"}}, **(top or {}))
    ds = dois(n, "10.5555/snb")
    for d in ds:
        world.add(d, count)
    make_lib(root / "research_a", ds)
    if setup:
        setup(ds)
    captured = []
    res = snowball.run(project="research_a", step_runner=_inproc_runner(captured, extra))
    rc, text = captured[0]
    it = res["projects"][0]["iterations"][0]
    return rc, text, it["status"], it["reason"]


SNOWBALL_CASES = {
    "cache_locked": dict(setup="locked"),
    "s2_budget": dict(top={"s2": {"max_requests_per_run": 1}}),
    "over_5_percent": dict(setup="fail_one"),
    "openalex_budget": dict(extra=("--source", "openalex"), top={"openalex": {"max_requests_per_run": 2}},
                            setup="oa"),
}


@pytest.mark.parametrize("case", sorted(SNOWBALL_CASES))
def test_snowball_reads_every_exit_2_and_3_as_degraded(world, tmp_path, monkeypatch, case):
    spec = dict(SNOWBALL_CASES[case])
    kind = spec.pop("setup", None)

    def setup(ds):
        if kind == "locked":
            def locked(self, path):
                raise walk.CacheLocked("s2_cache.duckdb is held by another process")
            monkeypatch.setattr(walk.Cache, "__init__", locked)
        elif kind == "fail_one":
            world.fail(ds[1], 500)
        elif kind == "oa":
            monkeypatch.setenv(openalex.KEY_ENV, OA_KEY)
            for d in ds:
                world.papers[d]["oa"] = 2
    rc, text, status, reason = _one_snowball(world, tmp_path, monkeypatch, setup=setup, **spec)
    evidence(f"snowball_{case}.json", {"rc": rc, "status": status, "reason": reason,
                                       "summary_last": last_line(text)[:400]})
    assert rc in (2, 3)
    summ = summary_of(text)
    assert summ["exit_code"] == rc and status == "DEGRADED", reason
    assert OA_KEY not in text and OA_KEY not in reason


def test_snowball_reads_a_walker_config_error_as_failed(world, tmp_path, monkeypatch):
    root = _snowball_env(tmp_path, monkeypatch, {"research_a": {"lib_dir": "literature"}})
    (root / "research_a").mkdir(parents=True)               # registered, but the library is missing
    captured = []
    res = snowball.run(project="research_a", step_runner=_inproc_runner(captured))
    rc, text = captured[0]
    assert rc == 1 and fc.SUMMARY_MARKER not in text
    assert res["projects"][0]["iterations"][0]["status"] == "FAILED" and res["exit_code"] == 1


# ================================================================ items 6, 7: the index reads the regenerated CSV
LETTER_SEED, LETTER_CITER = "10.1088/2053-1591/acdecd", "10.1088/2053-1591/abcdef"


def test_the_real_index_ingests_the_regenerated_csv(world, tmp_path, monkeypatch):
    monkeypatch.setenv(openalex.KEY_ENV, OA_KEY)
    root = tmp_path / "root"
    lib = make_lib(root / "research_a", [LETTER_SEED, "10.5555/big.0001"])
    text_doi = "10.5555/txt.0001"
    (lib / "2019_TextOnly.fulltext.json").write_text(json.dumps(
        {"doi": text_doi, "text": "Body text of a JATS-only holding. " * 40, "has_pdf": False}), encoding="utf-8")
    letter_rows = [s2_citer(LETTER_SEED, 0), {**s2_citer(LETTER_SEED, 1), "externalIds": {"DOI": LETTER_CITER.upper()}}]
    world.add(LETTER_SEED, 2, rows=letter_rows).add("10.5555/big.0001", 12000, oa=3).add(text_doi, 2)
    reg = write_registry(tmp_path / "reg.json", {"research_a": {"lib_dir": "literature"}},
                         state_dir=str(tmp_path / "sd"))
    for mod in (fc, I):
        monkeypatch.setattr(mod, "CONFIG_PATH", reg)
    monkeypatch.setattr(config, "CONFIG_PATH", reg)
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    res = fc.run(project="research_a")
    assert res["exit_code"] == 0 and res["text_only_seeds"] == 1
    rows = read_csv(lib / "_forward_citations.csv")
    assert {r["seed_pdf"] for r in rows if r["seed_doi"] == text_doi} == {"2019_TextOnly.fulltext.json"}
    assert LETTER_CITER in {r["citing_doi"] for r in rows if r["seed_doi"] == LETTER_SEED}
    oa_row = next(r for r in rows if r["source"] == "openalex")

    # a scoped harvest in the same library: its citers must stay out of candidates
    world.add("10.5555/scoped.0001", 1, rows=[s2_citer("10.5555/scoped.0001", 3)])
    lst = tmp_path / "list.csv"
    lst.write_text("doi\n10.5555/scoped.0001\n", encoding="utf-8")
    assert fc.run(project="research_a", seeds_from=[str(lst)], scope="side")["exit_code"] == 0
    scoped_doi = read_csv(lib / "_side_forward_citations.csv")[0]["citing_doi"]

    db = tmp_path / "refs" / "portfolio.duckdb"
    assert I.main(["--db", str(db), "--project", "research_a"]) == 0
    con = duckdb.connect(str(db))
    try:
        con.execute("UPDATE paper_metadata SET abstract = 'kept abstract' WHERE doi = ?", [oa_row["citing_doi"]])
        n_abs = con.execute("SELECT count(*) FROM paper_metadata WHERE abstract IS NOT NULL").fetchone()[0]
    finally:
        con.close()
    assert fc.run(project="research_a", refresh=True)["exit_code"] == 0      # regenerated
    assert I.main(["--db", str(db), "--project", "research_a"]) == 0
    con = duckdb.connect(str(db), read_only=True)
    try:
        cand = con.execute("SELECT doi, source_seed_doi FROM candidates WHERE source_type = 'forward'").fetchall()
        meta = con.execute("SELECT title, venue, authors, year FROM paper_metadata WHERE doi = ?",
                           [oa_row["citing_doi"]]).fetchone()
        n_abs2 = con.execute("SELECT count(*) FROM paper_metadata WHERE abstract IS NOT NULL").fetchone()[0]
        scoped = {r[0] for r in con.execute("SELECT doi FROM scoped_candidates WHERE scope = 'side'").fetchall()}
    finally:
        con.close()
    by_seed = {}
    for doi, seed in cand:
        by_seed.setdefault(seed, set()).add(doi)
    assert by_seed[LETTER_SEED] == {r["citing_doi"] for r in rows if r["seed_doi"] == LETTER_SEED}
    assert LETTER_CITER in by_seed[LETTER_SEED]                             # the letter tail kept whole
    assert by_seed[text_doi] and by_seed["10.5555/big.0001"] == {r["citing_doi"] for r in rows if r["source"] == "openalex"}
    assert meta == (oa_row["citing_title"], oa_row["citing_venue"], oa_row["citing_authors"], int(oa_row["citing_year"]))
    assert n_abs2 == n_abs == 1
    assert scoped == {scoped_doi} and scoped_doi not in {d for d, _ in cand}
    evidence("index_ingest.json", {"candidates_by_seed": {k: sorted(v) for k, v in by_seed.items()},
                                   "oa_row_metadata": meta, "abstracts": [n_abs, n_abs2], "scoped": sorted(scoped)})


# ================================================================ item 10: the planner against actual calls
def test_the_plan_against_the_calls_a_mixed_run_makes(world, tmp_path, monkeypatch):
    monkeypatch.setenv(openalex.KEY_ENV, OA_KEY)
    ds = dois(7, "10.5555/pln")
    for d, n in zip(ds, (0, 3, 900, 1500, 9999, 12000, 14000)):
        world.add(d, n, undated=20 if n == 14000 else 0)
    world.papers[ds[5]]["oa"] = 12500                     # OpenAlex holds more than S2 counts
    world.nested_null.add(world.papers[ds[2]]["pid"])     # a nested row null: not_found, no extra call
    lib = make_lib(tmp_path, ds)
    oa = openalex.Session(budget=None)
    res = fc.run(lib_dir=str(lib), oa_session=oa)
    p = res["plan"]
    actual = {"metadata": len(world.metadata_calls()), "nested_bins": len(world.nested_calls()),
              "pages_and_windows": len(world.get_calls()),
              "openalex_singletons": sum(1 for r in world.oa_calls() if r["path"].startswith("/works/doi:")),
              "openalex_lists": sum(1 for r in world.oa_calls() if r["path"] == "/works")}
    planned = {"metadata": p["metadata"], "nested_bins": p["nested_bins"], "pages_and_windows": p["pages"] + p["windows"],
               "openalex_singletons": p["openalex_singletons"], "openalex_lists": p["openalex_lists"]}
    evidence("planner_vs_actual.json", {"planned": planned, "actual": actual, "plan": p,
                                        "result": {k: res[k] for k in ("exit_code", "capped", "openalex_walked",
                                                                       "not_found", "failed")}})
    assert res["exit_code"] == 0
    for k in ("metadata", "nested_bins", "openalex_singletons"):
        assert actual[k] == planned[k], (k, actual, planned)
    # the gaps, pinned: OpenAlex lists are estimated from S2's count (120 + 140 planned; 125 sent, and the
    # 14,000 seed OpenAlex does not hold falls back to S2 windows the plan never counted)
    assert planned["openalex_lists"] == 260 and actual["openalex_lists"] == 125
    assert actual["pages_and_windows"] > planned["pages_and_windows"] == 12
