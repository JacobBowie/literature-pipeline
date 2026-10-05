"""Enrichment feeds (dispatch W3-E: T1, K6, K7, DEC-18): enrich_recommendations.py (the recent feed)
and litpipe/enrich_s2.py (library abstracts, OA URLs and counts from the S2 paper batch).

Offline: requests go to FeedS2, a stub litpipe.net transport answering /recommendations/v1 and
/graph/v1/paper/batch like Semantic Scholar. Every DB is a temp DuckDB built from the real
index_portfolio.SCHEMA. tests/fixtures/W3-E/p4_batch_k6.json is a trimmed slice of the recorded P4
batch (2026-09-23): a null, a publisher-elided abstract in publisher casing, a closed paper with
an abstract, an "ABSTRACT" heading with an OA URL, and a plain abstract."""
import ast
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

import duckdb
import pytest
from requests.structures import CaseInsensitiveDict

import enrich_recommendations as er
import index_portfolio
import lit_util
import snowball
from litpipe import enrich_s2 as es
from litpipe import hosts, net, s2

REPO = Path(__file__).resolve().parent.parent
FIX = Path(__file__).resolve().parent / "fixtures" / "W3-E"
SECRET = "s2-TESTKEY-w3e-9f8e7d6c5b4a"
T0 = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)


# ------------------------------------------------------------------------------ the S2 stub
def _raw(status, body):
    data = body if isinstance(body, bytes) else json.dumps(body).encode("utf-8")
    h = CaseInsensitiveDict({"Content-Type": "application/json"})
    return net._Raw(status, h, data, data[:net.CHUNK], len(data))


class HardKill(BaseException):
    """Stands in for a SIGKILL: nothing in the stage may catch it."""


class FeedS2:
    """recs: seed DOI -> list of recommendedPapers, an HTTP status (int), "transport" or "kill".
    batch: "DOI:<doi>" (lower case) -> record dict or None (S2's positional null); `batch_status`
    answers every batch call with that status instead."""

    def __init__(self):
        self.recs, self.batch, self.batch_status = {}, {}, None
        self.sent = []

    def rec_calls(self):
        return [r for r in self.sent if "/recommendations/" in r["path"]]

    def batch_calls(self):
        return [r for r in self.sent if r["path"].endswith("/paper/batch")]

    def __call__(self, method, url, hdrs, body, timeout, max_bytes):
        parts = urlsplit(url)
        path = unquote(parts.path)
        q = {k: v[-1] for k, v in parse_qs(parts.query).items()}
        js = json.loads(body) if body else None
        self.sent.append({"method": method, "path": path, "params": q, "headers": dict(hdrs), "json": js})
        if path.startswith("/recommendations/v1/papers/forpaper/DOI:"):
            seed = path[len("/recommendations/v1/papers/forpaper/DOI:"):]
            a = self.recs.get(seed, [])
            if a == "kill":
                raise HardKill(seed)
            if a == "transport":
                return net._Raw(error="ConnectionError: scripted")
            if isinstance(a, int):
                return _raw(a, {"error": "Input papers not found" if a == 404 else f"scripted {a}"})
            return _raw(200, {"recommendedPapers": a[:int(q.get("limit", 100))]})
        if path == "/graph/v1/paper/batch" and method == "POST":
            if self.batch_status:
                return _raw(self.batch_status, {"error": f"scripted {self.batch_status}"})
            return _raw(200, [self.batch.get(i.lower()) for i in js["ids"]])
        return _raw(404, {"error": f"FeedS2: no route for {path}"})


def rec(doi, title="A recent paper", year=2026, authors=("Ada Lovelace",)):
    return {"paperId": "f" * 40, "externalIds": {"DOI": doi} if doi else {}, "title": title, "year": year,
            "authors": [{"authorId": "1", "name": a} for a in authors]}


# ------------------------------------------------------------------------------ DB and env
def make_db(path, library, metadata=None):
    """A temp index from the real SCHEMA. library: [(doi, project)]; metadata: {doi: abstract or
    None}, default: every library DOI with an abstract."""
    con = duckdb.connect(str(path))
    con.execute(index_portfolio.SCHEMA)
    for doi, proj in library:
        con.execute("INSERT INTO paper_locations (doi, project, has_pdf) VALUES (?, ?, TRUE)", [doi, proj])
    meta = metadata if metadata is not None else {d: f"Local abstract of {d}" for d, _ in library}
    for doi, abstract in meta.items():
        con.execute("INSERT INTO paper_metadata (doi, year, title, abstract) VALUES (?, 2020, ?, ?)",
                    [doi, f"Local title {doi}", abstract])
    con.close()
    return path


def q(db, sql, args=()):
    con = duckdb.connect(str(db), read_only=True)
    try:
        return con.execute(sql, list(args)).fetchall()
    finally:
        con.close()


def abstracts(db):
    return q(db, "SELECT count(*) FROM paper_metadata WHERE abstract IS NOT NULL AND abstract <> ''")[0][0]


def summary_of(out):
    last = out.strip().splitlines()[-1]
    assert last.startswith(er.SUMMARY_MARKER), last
    return json.loads(last[len(er.SUMMARY_MARKER):])


@pytest.fixture
def feed(net_env, monkeypatch, tmp_path):
    """net_env plus FeedS2 on both transports, no key, both modules' CONFIG_PATH on the temp
    registry (projects research_a and teaching_b), and an index with five research_a seeds."""
    monkeypatch.delenv(s2.KEY_ENV, raising=False)
    monkeypatch.setattr(s2, "BASE", "https://api.semanticscholar.org")
    s2.reset_default_session()
    fake = FeedS2()
    monkeypatch.setitem(net._TRANSPORTS, "requests", fake)
    monkeypatch.setitem(net._TRANSPORTS, "urllib", fake)
    net_env.write_config(projects={"research_a": {"lib_dir": "lit"}, "teaching_b": {"lib_dir": "lit"}})
    monkeypatch.setattr(er, "CONFIG_PATH", net_env.cfg_path)
    monkeypatch.setattr(es, "CONFIG_PATH", net_env.cfg_path)
    net_env.fake = fake
    net_env.seeds = [f"10.1000/seed.{i}" for i in range(1, 6)]
    net_env.db = make_db(tmp_path / "portfolio.duckdb", [(d, "research_a") for d in net_env.seeds])
    yield net_env
    s2.reset_default_session()
    hosts.reset()


def run_recs(env, **kw):
    kw.setdefault("db", str(env.db))
    kw.setdefault("recent_feed", True)
    kw.setdefault("allow_unkeyed", True)
    kw.setdefault("now", T0)
    return er.run(**kw)


# ================================================================ DEC-18 gates
def test_no_flag_sends_nothing_prints_one_line_and_exits_0(feed, capsys, tmp_path):
    missing = tmp_path / "nowhere" / "portfolio.duckdb"
    assert er.main(["--db", str(missing), "--project", "not_registered"]) == 0
    out, err = capsys.readouterr()
    assert out.splitlines() == [er.OFF_LINE] and err == ""
    assert feed.fake.sent == [] and not missing.exists()


def test_no_flag_from_sys_argv_too(feed, capsys, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["enrich_recommendations.py"])
    assert er.main() == 0
    assert capsys.readouterr().out.splitlines() == [er.OFF_LINE]
    assert feed.fake.sent == []


def test_recent_feed_without_key_sends_nothing_unless_allow_unkeyed(feed, capsys):
    assert er.main(["--recent-feed", "--db", str(feed.db)]) == 0
    assert capsys.readouterr().out.splitlines() == [er.NO_KEY_LINE]
    assert feed.fake.sent == []
    assert q(feed.db, "SELECT count(*) FROM information_schema.tables WHERE table_name = 'recent_feed'")[0][0] == 0
    assert er.main(["--recent-feed", "--allow-unkeyed", "--db", str(feed.db)]) == 0
    assert len(feed.fake.rec_calls()) == 5


def test_run_daily_and_snowball_bare_calls_make_no_request(feed, capsys):
    """run_daily.py ~224 calls the script with no flag, snowball --with-recs with only --db: both
    are gated until they pass --recent-feed (forwarded to W4-A and snowball's owner)."""
    assert er.main([]) == 0
    assert er.main(["--db", str(feed.db)]) == 0
    assert feed.fake.sent == []


def test_s2_key_flag_reaches_the_header_only_and_is_never_printed(feed, capsys, monkeypatch):
    assert er.main(["--recent-feed", "--s2-key", SECRET, "--db", str(feed.db), "--limit", "2"]) == 0
    out, err = capsys.readouterr()
    assert SECRET not in out + err and "[deprecated] --s2-key" in out
    sent = feed.fake.rec_calls()
    assert len(sent) == 2 and all(r["headers"].get("x-api-key") == SECRET for r in sent)
    assert SECRET not in feed.ledger_text()
    assert os.environ.get(s2.KEY_ENV) is None                # restored after the run


def test_env_key_is_enough_and_never_printed(feed, capsys, monkeypatch):
    monkeypatch.setenv(s2.KEY_ENV, SECRET)
    assert er.main(["--recent-feed", "--db", str(feed.db), "--limit", "1"]) == 0
    out, err = capsys.readouterr()
    assert SECRET not in out + err and "key: present" in out
    assert SECRET not in feed.ledger_text()


# ================================================================ T1 / K7: writes
def test_feed_recommendations_metadata_and_ledger_are_written(feed, capsys):
    s1, s2_, s3 = feed.seeds[:3]
    feed.db = make_db(feed.db.with_name("p2.duckdb"), [(s1, "research_a"), (s2_, "research_a"), (s3, "research_a")],
                      {s1: "Local abstract", s2_: None, s3: "", "10.2000/known.1": "Known abstract"})
    feed.fake.recs = {s1: [rec("10.2000/NEW.1", title="New &amp; <i>hot</i>"), rec(None), rec("10.2000/known.1")],
                      s2_: [rec("10.2000/new.1")], s3: []}
    before = abstracts(feed.db)
    res = run_recs(feed)
    assert res["exit_code"] == 0 and res["ok"] == 3 and res["new_pairs"] == 3
    rows = q(feed.db, "SELECT seed_doi, recommended_doi, pool, rank, epoch(first_seen_at), epoch(last_seen_at) "
                      "FROM recent_feed ORDER BY 1, 2")
    assert [r[:4] for r in rows] == [(s1, "10.2000/known.1", "recent", 3), (s1, "10.2000/new.1", "recent", 1),
                                     (s2_, "10.2000/new.1", "recent", 1)]
    assert all(r[4] == r[5] == T0.timestamp() for r in rows)
    assert q(feed.db, "SELECT count(*) FROM recommendations")[0][0] == 3
    meta = dict(q(feed.db, "SELECT doi, title FROM paper_metadata"))
    assert meta["10.2000/new.1"] == "New & hot"                       # display_field
    assert meta["10.2000/known.1"] == "Local title 10.2000/known.1"        # existing row untouched
    assert q(feed.db, "SELECT abstract FROM paper_metadata WHERE doi = '10.2000/known.1'")[0][0] == "Known abstract"
    assert abstracts(feed.db) == before
    att = dict(q(feed.db, "SELECT seed_doi, outcome FROM rec_attempts"))
    assert att == {s1: "OK", s2_: "OK", s3: "OK"}


def test_publisher_casing_does_not_duplicate_metadata(feed):
    s = feed.seeds[0]
    feed.db = make_db(feed.db.with_name("p3.duckdb"), [(s, "research_a")], {s: "x", "10.1007/s00421-021-04802-5": "y"})
    feed.fake.recs = {s: [rec("10.1007/S00421-021-04802-5")]}
    res = run_recs(feed)
    assert res["metadata_inserted"] == 0
    assert q(feed.db, "SELECT recommended_doi FROM recent_feed") == [("10.1007/s00421-021-04802-5",)]
    assert q(feed.db, "SELECT count(*) FROM paper_metadata")[0][0] == 2


def test_second_run_within_the_window_makes_no_calls(feed):
    feed.fake.recs = {d: [rec(f"10.2000/r.{i}")] for i, d in enumerate(feed.seeds)}
    assert run_recs(feed)["asked"] == 5
    n = len(feed.fake.sent)
    res = run_recs(feed, now=T0 + timedelta(days=3))
    assert len(feed.fake.sent) == n and res["asked"] == 0 and res["answered_recently"] == 5
    res = run_recs(feed, now=T0 + timedelta(days=er.RETRY_AFTER_DAYS + 1))
    assert res["asked"] == 5                                            # due again after the window


def test_rerun_appends_only_new_pairs_and_deletes_nothing(feed):
    s = feed.seeds[0]
    feed.fake.recs = {s: [rec("10.2000/a.1"), rec("10.2000/b.1")]}
    run_recs(feed, limit=1)
    feed.fake.recs = {s: [rec("10.2000/b.1"), rec("10.2000/c.1")]}
    later = T0 + timedelta(days=10)
    res = run_recs(feed, limit=1, refresh=True, now=later)
    assert res["new_pairs"] == 1 and res["seen_again"] == 1
    rows = {r[0]: r[1:] for r in q(feed.db, "SELECT recommended_doi, rank, epoch(first_seen_at), epoch(last_seen_at) "
                                            "FROM recent_feed")}
    assert rows == {"10.2000/a.1": (1, T0.timestamp(), T0.timestamp()),
                    "10.2000/b.1": (1, T0.timestamp(), later.timestamp()),
                    "10.2000/c.1": (2, later.timestamp(), later.timestamp())}
    assert {r[0] for r in q(feed.db, "SELECT recommended_doi FROM recommendations")} == {"10.2000/a.1", "10.2000/b.1",
                                                                                     "10.2000/c.1"}


def test_404_is_not_found_and_not_asked_again(feed):
    feed.fake.recs = {feed.seeds[0]: 404}
    res = run_recs(feed, limit=1)
    assert res["exit_code"] == 0 and res["not_found"] == 1 and res["failed"] == 0
    assert q(feed.db, "SELECT outcome, status FROM rec_attempts") == [("NO_MATCH", "404")]
    n = len(feed.fake.sent)
    run_recs(feed, limit=1, now=T0 + timedelta(days=1))
    assert feed.fake.rec_calls()[-1]["path"].endswith(feed.seeds[1])     # the 404 seed was skipped
    assert len(feed.fake.sent) == n + 1


def test_a_failed_list_keeps_the_prior_rows_and_is_asked_again(feed, capsys):
    s = feed.seeds[0]
    feed.fake.recs = {s: [rec("10.2000/a.1"), rec("10.2000/b.1")]}
    run_recs(feed, limit=1)
    feed.fake.recs = {s: 500}
    res = run_recs(feed, limit=1, refresh=True, now=T0 + timedelta(days=5))
    assert res["failed"] == 1 and res["ok"] == 0
    assert q(feed.db, "SELECT count(*) FROM recent_feed")[0][0] == 2
    assert q(feed.db, "SELECT count(*) FROM recommendations")[0][0] == 2
    assert q(feed.db, "SELECT outcome, status FROM rec_attempts") == [("OUTAGE", "500")]
    feed.fake.recs = {s: [rec("10.2000/a.1")]}
    res = run_recs(feed, limit=1, now=T0 + timedelta(days=6))            # no --refresh: still due
    assert res["asked"] == 1 and res["ok"] == 1


def test_ask_types_a_failure_and_never_returns_an_empty_success(feed):
    sess = s2.Session(cfg=json.loads(feed.cfg_path.read_text()))
    feed.fake.recs = {"10.1000/x.1": "transport"}
    outcome, status, rows, meta, detail, attempts = er.ask("10.1000/x.1", 20, sess)
    assert outcome == "TRANSPORT" and attempts >= 1 and outcome not in er.ANSWERED
    assert er.ask("not a doi", 20, sess)[0] == "SKIPPED"


def test_degraded_over_5_percent_exits_2_with_the_summary_line(feed, capsys, tmp_path):
    seeds = [f"10.1000/many.{i:02d}" for i in range(10)]
    feed.db = make_db(tmp_path / "many.duckdb", [(d, "research_a") for d in seeds])
    feed.fake.recs = {d: [rec("10.2000/z.1")] for d in seeds}
    feed.fake.recs[seeds[4]] = 503
    res = run_recs(feed)
    out = capsys.readouterr().out
    assert res["exit_code"] == 2 and res["failed"] == 1
    summ = summary_of(out)
    assert summ["reasons"] and summ["aborted"] is None and summ["transport_failures"] == 1
    assert snowball.degraded_exit(2, summ)


def test_breaker_aborts_with_exit_3_and_a_rerun_resumes(feed, capsys):
    feed.fake.recs = {d: 500 for d in feed.seeds}
    res = run_recs(feed)
    summ = summary_of(capsys.readouterr().out)
    assert res["exit_code"] == 3 and summ["aborted"] == "breaker" and res["asked"] == 3
    assert snowball.degraded_exit(3, summ)
    s2.reset_default_session()
    feed.fake.recs = {d: [rec("10.2000/ok.1")] for d in feed.seeds}
    res = run_recs(feed, now=T0 + timedelta(hours=1))
    assert res["exit_code"] == 0 and res["asked"] == 5


def test_hard_kill_loses_at_most_one_batch_and_a_rerun_resumes(feed):
    feed.fake.recs = {d: [rec(f"10.2000/k.{i}")] for i, d in enumerate(feed.seeds)}
    feed.fake.recs[feed.seeds[3]] = "kill"
    before = abstracts(feed.db)
    with pytest.raises(HardKill):
        run_recs(feed, commit_every=2)
    assert {r[0] for r in q(feed.db, "SELECT seed_doi FROM rec_attempts")} == set(feed.seeds[:2])
    assert q(feed.db, "SELECT count(*) FROM recent_feed")[0][0] == 2
    assert abstracts(feed.db) == before
    feed.fake.recs[feed.seeds[3]] = [rec("10.2000/k.3")]
    n = len(feed.fake.rec_calls())
    res = run_recs(feed, commit_every=2, now=T0 + timedelta(minutes=5))
    asked = [r["path"].rsplit(":", 1)[-1] for r in feed.fake.rec_calls()[n:]]
    assert asked == feed.seeds[2:]                     # seed 3's batch redone, nothing earlier
    assert res["exit_code"] == 0 and abstracts(feed.db) == before


def test_interrupt_commits_the_answered_seeds(feed, monkeypatch):
    feed.fake.recs = {d: [rec(f"10.2000/i.{i}")] for i, d in enumerate(feed.seeds)}
    real = er.ask
    calls = []

    def ask(seed, top_n, session):
        calls.append(seed)
        if len(calls) == 3:
            raise KeyboardInterrupt
        return real(seed, top_n, session)

    monkeypatch.setattr(er, "ask", ask)
    res = run_recs(feed, commit_every=50)
    assert res["interrupted"] and res["batches"] == 1 and res["exit_code"] == 130
    assert {r[0] for r in q(feed.db, "SELECT seed_doi FROM rec_attempts")} == set(feed.seeds[:2])


def test_a_failed_batch_write_rolls_back_whole_and_the_abstract_count_holds(feed, monkeypatch, capsys):
    feed.fake.recs = {d: [rec(f"10.2000/w.{i}")] for i, d in enumerate(feed.seeds)}
    real = es.upsert
    n = []

    def flaky(con, table, *a, **k):
        n.append(table)
        if table == "recommendations":
            raise duckdb.IOException("disk went away")
        return real(con, table, *a, **k)

    monkeypatch.setattr(es, "upsert", flaky)
    before = abstracts(feed.db)
    res = run_recs(feed, commit_every=2)
    summ = summary_of(capsys.readouterr().out)
    assert res["exit_code"] == 3 and summ["aborted"] == "write_rolled_back"
    assert n[:2] == ["recent_feed", "recommendations"]
    for t in ("recent_feed", "rec_attempts", "recommendations"):
        assert q(feed.db, f"SELECT count(*) FROM {t}")[0][0] == 0, t
    assert abstracts(feed.db) == before
    assert res["asked"] == 2                                   # stopped after the failed batch


def test_the_abstract_guard_rolls_back_a_batch_that_would_drop_an_abstract(feed, monkeypatch):
    feed.fake.recs = {d: [rec("10.2000/g.1")] for d in feed.seeds}
    real = es.upsert

    def bad(con, table, *a, **k):
        if table == "rec_attempts":
            con.execute("UPDATE paper_metadata SET abstract = NULL WHERE doi = ?", [feed.seeds[0]])
        return real(con, table, *a, **k)

    monkeypatch.setattr(es, "upsert", bad)
    before = abstracts(feed.db)
    res = run_recs(feed)
    assert res["exit_code"] == 3 and "AbstractCountDropped" in res["write_error"]
    assert abstracts(feed.db) == before
    assert q(feed.db, "SELECT count(*) FROM recent_feed")[0][0] == 0


def test_project_restricts_the_seeds(feed, tmp_path):
    feed.db = make_db(tmp_path / "two.duckdb", [("10.1000/a.1", "research_a"), ("10.1000/b.1", "teaching_b"),
                                                ("10.1000/c.1", "teaching_b")])
    res = run_recs(feed, project="teaching_b")
    assert res["seeds"] == 2
    assert sorted(r["path"].rsplit(":", 1)[-1] for r in feed.fake.rec_calls()) == ["10.1000/b.1", "10.1000/c.1"]


@pytest.mark.parametrize("case", ["unknown_project", "no_registry", "no_db", "bad_top_n"])
def test_config_errors_exit_1_and_send_nothing(feed, case, tmp_path, monkeypatch):
    kw = {}
    if case == "unknown_project":
        kw["project"] = "nope"
    elif case == "no_registry":
        monkeypatch.setattr(er, "CONFIG_PATH", tmp_path / "absent" / "projects.json")
        kw["project"] = "research_a"
    elif case == "no_db":
        kw["db"] = str(tmp_path / "absent.duckdb")
    else:
        kw["top_n"] = 501
    res = run_recs(feed, **kw)
    assert res["exit_code"] == 1 and feed.fake.sent == []
    assert not (tmp_path / "absent.duckdb").exists()


def test_writes_are_set_based_and_fast_on_a_large_index(tmp_path):
    """T1 acceptance at test scale: 200 seeds x 20 recs into a 100k-row paper_metadata. The old
    per-row executemany took 10.8 s for 5,000 rows on 100k (DuckDB 1.5.5); the set-based batch
    takes well under a second."""
    pd = pytest.importorskip("pandas")
    db = tmp_path / "big.duckdb"
    con = duckdb.connect(str(db))
    con.execute(index_portfolio.SCHEMA)
    con.execute(er.FEED_DDL)
    con.execute(er.ATTEMPTS_DDL)
    big = pd.DataFrame({"doi": [f"10.3000/m.{i}" for i in range(100_000)],
                        "abstract": [("text" if i % 2 else None) for i in range(100_000)]})
    con.register("big", big)
    con.execute("INSERT INTO paper_metadata (doi, abstract) SELECT doi, abstract FROM big")
    con.unregister("big")
    feed_rows = [(f"10.1000/s.{s}", f"10.4000/r.{s}.{r}", "recent", r) for s in range(200) for r in range(1, 21)]
    meta = [(d, 2026, "t", "a") for _, d, _, _ in feed_rows]
    att = [(f"10.1000/s.{s}", "OK", "200") for s in range(200)]
    t0 = time.perf_counter()
    got = er.write_batch(con, feed_rows, meta, att, T0)
    took = time.perf_counter() - t0
    con.close()
    assert got["new_pairs"] == 4000 and got["metadata_inserted"] == 4000
    assert took < 10, f"write_batch took {took:.1f} s"


def test_no_executemany_in_either_module():
    for path in (REPO / "enrich_recommendations.py", REPO / "litpipe" / "enrich_s2.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        names = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
        assert "executemany" not in names, path.name


def test_module_binds_no_email_or_ua_and_keeps_its_flags():
    assert not hasattr(er, "EMAIL") and not hasattr(er, "UA")
    helps = subprocess.run([sys.executable, str(REPO / "enrich_recommendations.py"), "--help"],
                           capture_output=True, text=True, timeout=120, cwd=str(REPO))
    assert helps.returncode == 0, helps.stderr
    for flag in ("--db", "--limit", "--sleep", "--s2-key", "--top-n", "--project", "--recent-feed",
                 "--allow-unkeyed", "--refresh", "--commit-every", "--retry-after-days"):
        assert flag in helps.stdout, flag


def test_deprecated_sleep_is_accepted_and_ignored(feed, capsys):
    assert er.main(["--recent-feed", "--allow-unkeyed", "--sleep", "0.1", "--db", str(feed.db), "--limit", "1"]) == 0
    assert "[deprecated] --sleep is ignored" in capsys.readouterr().out


# ================================================================ K6: litpipe.enrich_s2
def fx():
    return json.loads((FIX / "p4_batch_k6.json").read_text(encoding="utf-8"))


@pytest.fixture
def k6(feed, tmp_path):
    """The fixture's five DOIs as a research_a library: the elided one (publisher casing in S2)
    and the closed and heading rows have no local abstract; the plain one has its own."""
    f = fx()
    dois = [i[len("DOI:"):].lower() for i in f["ids"]]
    feed.k6 = dict(zip(f["picked"], dois))
    feed.fake.batch = {i.lower(): row for i, row in zip(f["ids"], f["body"])}
    d = feed.k6
    feed.db = make_db(tmp_path / "k6.duckdb", [(x, "research_a") for x in dois] + [(d["markup"], "teaching_b")],
                      {d["null"]: None, d["elided_upper"]: None, d["closed_abstract"]: "",
                       d["heading_oa"]: None, d["markup"]: "Local abstract kept"})
    return feed


def test_k6_dry_run_writes_nothing_opens_read_only_and_prints_the_commit_command(k6, capsys, monkeypatch):
    opened = []
    real = lit_util.connect_db
    monkeypatch.setattr(lit_util, "connect_db", lambda *a, **k: (opened.append(k.get("read_only")), real(*a, **k))[1])
    before = abstracts(k6.db)
    res = es.run(db=str(k6.db), now=T0)
    out = capsys.readouterr().out
    assert res["exit_code"] == 0 and res["dry_run"] and opened == [True]
    assert res["would_fill"] == 2 and res["elided"] == 1 and res["not_in_s2"] == 1
    assert abstracts(k6.db) == before
    assert q(k6.db, "SELECT count(*) FROM information_schema.tables WHERE table_name = 's2_enrichment'")[0][0] == 0
    assert "python -m litpipe.enrich_s2 --commit" in out
    assert len(k6.fake.batch_calls()) == 1
    assert res["per_project"]["research_a"]["would_fill"] == 2
    assert res["per_project"]["teaching_b"]["requested"] == 1


def test_k6_commit_fills_only_empty_abstracts_skips_elided_and_stores_counts(k6, capsys):
    d = k6.k6
    before = abstracts(k6.db)
    res = es.run(db=str(k6.db), commit=True, now=T0)
    assert res["exit_code"] == 0 and res["abstracts_filled"] == 2
    assert abstracts(k6.db) == before + 2
    ab = dict(q(k6.db, "SELECT doi, abstract FROM paper_metadata"))
    assert ab[d["elided_upper"]] is None                                  # elided: never filled
    assert ab[d["markup"]] == "Local abstract kept"                        # never overwritten
    assert ab[d["heading_oa"]].startswith("Rationale Industry guidelines")  # heading dropped, cleaned
    assert ab[d["closed_abstract"]].startswith("This Viewpoint discusses")
    rows = {r[0]: r[1:] for r in q(k6.db, "SELECT doi, oa_url, citation_count, reference_count, abstract_elided, "
                                          "epoch(fetched_at) FROM s2_enrichment")}
    assert rows[d["elided_upper"]] == (None, 308, 30, True, T0.timestamp())   # lower-case key
    assert rows[d["heading_oa"]][0].startswith("https://onlinelibrary.wiley.com/")
    assert rows[d["null"]] == (None, None, None, None, T0.timestamp())
    assert all(k == k.lower() for k in rows)


def test_k6_elided_and_fresh_dois_are_not_requested_again(k6):
    es.run(db=str(k6.db), commit=True, now=T0)
    n = len(k6.fake.batch_calls())
    res = es.run(db=str(k6.db), commit=True, now=T0 + timedelta(days=1))
    assert len(k6.fake.batch_calls()) == n and res["requested"] == 0      # fresh: 0 calls
    res = es.run(db=str(k6.db), commit=True, refresh=True, now=T0 + timedelta(days=2))
    sent = k6.fake.batch_calls()[-1]["json"]["ids"]
    assert "DOI:" + k6.k6["elided_upper"] not in sent and len(sent) == 4 and res["skipped_elided"] == 1
    es.run(db=str(k6.db), commit=True, refresh=True, recheck_elided=True, now=T0 + timedelta(days=3))
    assert len(k6.fake.batch_calls()[-1]["json"]["ids"]) == 5


def test_k6_a_failed_batch_keeps_the_prior_rows(k6, capsys):
    es.run(db=str(k6.db), commit=True, now=T0)
    snap = q(k6.db, "SELECT * FROM s2_enrichment ORDER BY doi")
    before = abstracts(k6.db)
    k6.fake.batch_status = 500
    res = es.run(db=str(k6.db), commit=True, refresh=True, now=T0 + timedelta(days=40))
    summ = summary_of(capsys.readouterr().out)
    assert res["exit_code"] == 2 and res["failed"] == 4 and summ["transport_failures"] == 4
    assert q(k6.db, "SELECT * FROM s2_enrichment ORDER BY doi") == snap
    assert abstracts(k6.db) == before


def test_k6_a_row_answered_for_another_doi_is_not_used(k6):
    d = k6.k6
    k6.fake.batch["doi:" + d["closed_abstract"]] = dict(k6.fake.batch["doi:" + d["heading_oa"]])
    res = es.run(db=str(k6.db), commit=True, now=T0)
    assert res["exit_code"] == 2 and res["mismatch"] == 1
    assert q(k6.db, "SELECT abstract FROM paper_metadata WHERE doi = ?", [d["closed_abstract"]]) == [("",)]
    assert q(k6.db, "SELECT count(*) FROM s2_enrichment WHERE doi = ?", [d["closed_abstract"]])[0][0] == 0


def test_k6_an_interrupted_write_rolls_back_and_the_abstract_count_never_drops(k6, monkeypatch):
    real = es.upsert

    def boom(con, table, *a, **k):
        out = real(con, table, *a, **k)
        raise duckdb.IOException("killed mid-transaction")

    monkeypatch.setattr(es, "upsert", boom)
    before = abstracts(k6.db)
    res = es.run(db=str(k6.db), commit=True, now=T0)
    assert res["exit_code"] == 3 and res["aborted"] == "write_rolled_back"
    assert abstracts(k6.db) == before
    assert q(k6.db, "SELECT count(*) FROM s2_enrichment")[0][0] == 0


def test_k6_guard_rolls_back_a_write_that_would_drop_an_abstract(k6, monkeypatch):
    real = es.upsert

    def bad(con, table, *a, **k):
        con.execute("UPDATE paper_metadata SET abstract = '' WHERE doi = ?", [k6.k6["markup"]])
        return real(con, table, *a, **k)

    monkeypatch.setattr(es, "upsert", bad)
    before = abstracts(k6.db)
    res = es.run(db=str(k6.db), commit=True, now=T0)
    assert res["exit_code"] == 3 and abstracts(k6.db) == before
    assert q(k6.db, "SELECT abstract FROM paper_metadata WHERE doi = ?", [k6.k6["markup"]]) == [("Local abstract kept",)]


def test_k6_project_scope_and_key_never_printed(k6, capsys, monkeypatch):
    monkeypatch.setenv(s2.KEY_ENV, SECRET)
    res = es.run(db=str(k6.db), project="teaching_b", now=T0)
    out, err = capsys.readouterr()
    assert res["requested"] == 1 and "--project teaching_b" in out
    assert SECRET not in out + err and SECRET not in k6.ledger_text()
    assert k6.fake.batch_calls()[0]["headers"].get("x-api-key") == SECRET


@pytest.mark.parametrize("case", ["unknown_project", "no_db"])
def test_k6_config_errors_exit_1(k6, case, tmp_path):
    kw = {"project": "nope"} if case == "unknown_project" else {"db": str(tmp_path / "absent.duckdb")}
    kw.setdefault("db", str(k6.db))
    assert es.run(now=T0, **kw)["exit_code"] == 1
    assert k6.fake.sent == [] and not (tmp_path / "absent.duckdb").exists()


def test_k6_elision_marker_parsing():
    assert es.abstract_elided({"openAccessPdf": {"disclaimer": "Notice: The following paper fields have been "
                                                               "elided by the publisher: {'abstract'}. Paper"}})
    assert not es.abstract_elided({"openAccessPdf": {"disclaimer": "Notice: The following paper fields have been "
                                                                   "elided by the publisher: {'references'}."}})
    assert not es.abstract_elided({"openAccessPdf": {"disclaimer": "Notice: Paper or abstract available at x"}})
    assert not es.abstract_elided({"openAccessPdf": None}) and not es.abstract_elided(None)


def test_k6_an_elided_row_never_yields_an_abstract_and_markup_is_cleaned():
    elided = {"abstract": "Should not be used", "citationCount": 3, "openAccessPdf": {
        "url": "", "disclaimer": "Notice: The following paper fields have been elided by the publisher: {'abstract'}."}}
    r = es.parse_row(elided)
    assert r["abstract"] == "" and r["abstract_elided"] is True and r["oa_url"] is None
    r = es.parse_row({"abstract": "<jats:p>Heat &amp; exercise</jats:p>", "openAccessPdf": {"url": " https://x.org/a.pdf "}})
    assert r["abstract"] == "Heat & exercise" and r["oa_url"] == "https://x.org/a.pdf" and r["abstract_elided"] is False


def test_k6_help_from_the_repo_root():
    p = subprocess.run([sys.executable, "-m", "litpipe.enrich_s2", "--help"], capture_output=True, text=True,
                       timeout=120, cwd=str(REPO))
    assert p.returncode == 0 and "--commit" in p.stdout, p.stderr


# ================================================================ the index SCHEMA still runs
def test_index_schema_and_views_still_execute_after_these_tables_exist(k6):
    k6.fake.recs = {d: [rec("10.2000/v.1")] for d in k6.k6.values()}
    assert es.run(db=str(k6.db), commit=True, now=T0)["exit_code"] == 0
    assert run_recs(k6)["exit_code"] == 0
    con = duckdb.connect(str(k6.db))
    try:
        con.execute(index_portfolio.SCHEMA)
        assert con.execute("SELECT count(*) FROM papers").fetchone()[0] == 5
        con.execute("SELECT * FROM top_candidates").fetchall()
        tables = {r[0] for r in con.execute("SELECT table_name FROM information_schema.tables "
                                            "WHERE table_type = 'BASE TABLE'").fetchall()}
        assert {"recent_feed", "rec_attempts", "s2_enrichment"} <= tables
        cols = {r[0] for r in con.execute("SELECT column_name FROM information_schema.columns "
                                          "WHERE table_name = 'paper_metadata'").fetchall()}
        assert cols == {"doi", "year", "lastname", "title", "venue", "authors", "abstract",
                        "abstract_attempted_at", "refreshed_at"}           # no column added
    finally:
        con.close()


def test_upsert_works_on_a_copy_without_its_primary_key(tmp_path):
    """A --rebuild that carries recent_feed over with CREATE TABLE AS (no PK) must still take
    the next run's writes (UPDATE ... FROM / INSERT ... WHERE NOT EXISTS need no constraint)."""
    pd = pytest.importorskip("pandas")
    con = duckdb.connect(str(tmp_path / "c.duckdb"))
    con.execute(er.FEED_DDL)
    con.execute("CREATE TABLE copy AS SELECT * FROM recent_feed")
    src = pd.DataFrame([("s", "r", "recent", 1, es.utc_iso(T0))], columns=["seed_doi", "recommended_doi", "pool",
                                                                          "rank", "ts"])
    with es.staged(con, "src", src):
        cols = {"seed_doi": "s.seed_doi", "recommended_doi": "s.recommended_doi", "pool": "s.pool",
                "rank": "s.rank", "first_seen_at": "CAST(s.ts AS TIMESTAMPTZ)",
                "last_seen_at": "CAST(s.ts AS TIMESTAMPTZ)"}
        assert es.upsert(con, "copy", "src", ["seed_doi", "recommended_doi", "pool"], {"rank": "s.rank"}, cols) == (0, 1)
        assert es.upsert(con, "copy", "src", ["seed_doi", "recommended_doi", "pool"], {"rank": "s.rank"}, cols) == (1, 0)
    assert con.execute("SELECT count(*) FROM copy").fetchone()[0] == 1
    con.close()
