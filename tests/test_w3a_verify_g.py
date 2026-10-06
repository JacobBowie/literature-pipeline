"""Verifier G for W3a (database side): the walk-to-index seam, W3-E's history tables through
--rebuild and the metadata GC, index_runs read by the instruments, every exit 2/3 read by the real
snowball, the OpenAlex client's key and URL budget, and the set-based writes.

Every module runs for real and in-process: reverse_citations on WalkStub (tests/test_walk_backward.py),
enrich_recommendations and litpipe.enrich_s2 on FeedS2 (tests/test_enrich_feeds.py), litpipe.openalex
on OAStub (tests/test_w3b_openalex.py), index_portfolio on temp DuckDB files. No live network; every
registry, root, ledger, state and DB is a temp one, and arXiv is refused first in every state.

The locks of APPLY items G-1..G-5 fail on e67241a (they were strict expected failures there)
and pass since the fix commit, which dropped their markers."""
import contextlib
import csv
import datetime as dt
import io
import json
from datetime import timedelta
from pathlib import Path

import duckdb
import pytest

import audit_portfolio
import enrich_recommendations as er
import index_portfolio as ip
import lit_util
import reverse_citations as rc
import snowball
from litpipe import doi as D
from litpipe import enrich_s2 as es
from litpipe import hosts, net, s2
from litpipe import openalex as oa
from litpipe.outcomes import Kind
from tests.test_enrich_feeds import T0, FeedS2, rec
from tests.test_w3b_openalex import SECRET as OA_TEST_KEY
from tests.test_w3b_openalex import OAStub, oaenv, work  # noqa: F401  (oaenv is a fixture)
from tests.test_walk_backward import (_publish_core, add_seed, cited, env, outputs,  # noqa: F401
                                      rows_of, sha, stub)

ARXIV = ("export.arxiv.org", "arxiv.org", "www.arxiv.org")
HIST = ("abstract_attempts", "recent_feed", "rec_attempts", "s2_enrichment")
LETTER_TAIL_SEED = "10.1089/ther.2017.29031.mkb"
LETTER_TAIL_REFS = ["10.1088/2053-1591/acdecd", "10.1088/2053-1591/abfeed", "10.1149/2.0381907jss"]


def refuse_arxiv(state):
    for h in ARXIV:
        state.refuse(h, "manual: refused since 2026-09-25", persistence="manual")


def dbq(db, sql, args=()):
    con = duckdb.connect(str(db), read_only=True)
    try:
        return con.execute(sql, list(args)).fetchall()
    finally:
        con.close()


def n_abstracts(db):
    return dbq(db, "SELECT count(*) FROM paper_metadata WHERE abstract IS NOT NULL AND abstract <> ''")[0][0]


MAINS = {"reverse_citations": rc.main, "index_portfolio": ip.main, "enrich_recommendations": er.main}


def inproc(cmd, label, main=None):
    """snowball's step runner, in-process: the real main(argv), stdout captured, the last line
    read as the [step-summary] the way snowball.run_step reads a child's output."""
    main = main or MAINS[Path(cmd[1]).stem]
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = main([str(c) for c in cmd[2:]])
    lines = [ln for ln in out.getvalue().splitlines() if ln.strip()]
    summ = snowball._summary(lines[-1]) if lines and lines[-1].startswith(snowball.SUMMARY_MARKER) else None
    return snowball.StepResult(label, list(cmd), code, summ)


def step(script, *args):
    return [snowball.py(), str(snowball.HERE / script), *args]


def reading(r):
    """How the real snowball reads one step."""
    return snowball.classify(10, 10, [r])[0]


# ================================================================ seam 1: the walk to the index
@pytest.fixture
def idx(env, monkeypatch, tmp_path):
    monkeypatch.setattr(ip, "CONFIG_PATH", env.cfg_path)
    refuse_arxiv(env.state)
    env.db = tmp_path / "g_portfolio.duckdb"
    return env


def index(env, **kw):
    kw.setdefault("project", "research_a")
    return ip.run(db=str(env.db), **kw)


def reverse_edges(db):
    return set(dbq(db, "SELECT citing_doi, cited_doi FROM cites WHERE source_pipeline = 'reverse' "
                       "AND source_project = 'research_a'"))


def held(db):
    return {r[0] for r in dbq(db, "SELECT doi FROM paper_locations WHERE project = 'research_a'")}


def test_g_seam1_real_walk_to_real_index_edges_survive_a_seed_rename(stub, idx):
    seeds = {"10.5555/seed.a001": "2020_Alpha", "10.5555/seed.b002": "2021_Beta"}
    for i, (doi, stem) in enumerate(seeds.items()):
        add_seed(idx, stem, doi)
        stub.s2[doi] = {"count": 2, "refs": [cited(1, f"10.5555/r.{i}.1"), cited(2, f"10.5555/r.{i}.2")]}
    res = rc.run(project="research_a")
    assert res["exit_code"] == 0, res["reasons"]
    rows = rows_of(outputs(idx)["parsed"])
    assert {r["seed_doi"] for r in rows} == set(seeds) and {r["source"] for r in rows} == {"s2"}
    assert index(idx)["exit_code"] == 0
    before = reverse_edges(idx.db)
    assert len(before) == 4 and {c for c, _ in before} == set(seeds) <= held(idx.db)
    for ext in (".pdf", ".ris"):                      # a rename of the seed; the walker is not rerun
        (idx.lib / f"2020_Alpha{ext}").rename(idx.lib / f"2020_Alpha_Renamed{ext}")
    assert index(idx)["exit_code"] == 0
    assert reverse_edges(idx.db) == before            # the seed comes from the seed_doi column
    assert {c for c, _ in reverse_edges(idx.db)} <= held(idx.db)


def test_g_seam1_index_normalises_regex_dois_and_keeps_structured_ones_whole(idx):
    add_seed(idx, "2020_Alpha", "10.5555/seed.a001")
    junk, whole = "10.1016/j.jtherbio.2020.102345.url", "10.1149/2.0381907jss"
    with open(idx.lib / "_reverse_citations_parsed.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(rc.FIELDS)
        w.writerow(["2020_Alpha.pdf", "Ann", "2020", "t", junk, "raw", "10.5555/seed.a001", "regex"])
        w.writerow(["2020_Alpha.pdf", "Ann", "2020", "t", whole, "raw", "10.5555/seed.a001", "s2"])
    assert index(idx)["exit_code"] == 0
    assert reverse_edges(idx.db) == {("10.5555/seed.a001", D.normalise(junk)), ("10.5555/seed.a001", whole)}
    assert D.normalise(junk) != junk.lower()


def test_g_seam1_legacy_csv_ingests_and_loses_only_its_edges_on_a_rename(idx):
    """A pre-W3-B six-column CSV: its rows still ingest, keyed through the seed file's .ris; after a
    rename of the seed the candidates stay but the edges and the seed attribution are lost until
    the walker reruns (W3-C1 open question 3)."""
    add_seed(idx, "2020_Leg", "10.5555/leg.0001")
    with open(idx.lib / "_reverse_citations_parsed.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(rc.LEGACY_FIELDS)
        w.writerow(["2020_Leg.pdf", "Ann", "2001", "t", "10.5555/old.ref.1", "raw"])
        w.writerow(["2020_Leg.pdf", "Bo", "2002", "t", "10.5555/old.ref.2", "raw"])
    assert index(idx)["exit_code"] == 0
    assert reverse_edges(idx.db) == {("10.5555/leg.0001", "10.5555/old.ref.1"), ("10.5555/leg.0001", "10.5555/old.ref.2")}
    for ext in (".pdf", ".ris"):
        (idx.lib / f"2020_Leg{ext}").rename(idx.lib / f"2020_Leg_Renamed{ext}")
    assert index(idx)["exit_code"] == 0
    assert reverse_edges(idx.db) == set()
    cands = set(dbq(idx.db, "SELECT doi, source_seed_doi FROM candidates WHERE source_type = 'reverse'"))
    assert cands == {("10.5555/old.ref.1", ""), ("10.5555/old.ref.2", "")}


def _tables(db):
    return {"cites": sorted(dbq(db, "SELECT * FROM cites")),
            "candidates": sorted(dbq(db, "SELECT doi, source_type, source_seed_doi, source_project FROM candidates")),
            "meta": sorted(r[0] for r in dbq(db, "SELECT doi FROM paper_metadata")),
            "scoped": dbq(db, "SELECT count(*) FROM scoped_candidates")[0][0]}


def test_g_seam1_a_degraded_walk_reaches_no_table(stub, idx):
    published = _publish_core(stub, idx, n=20)
    assert index(idx)["exit_code"] == 0
    snap = _tables(idx.db)
    stub.s2_status = 429
    res = rc.run(project="research_a", refresh=True)
    assert res["exit_code"] == 2 and not res["published"]
    assert {k: sha(v) for k, v in outputs(idx).items() if k != "degraded"} == published
    only = "10.5555/degraded.only"
    with open(outputs(idx)["degraded"], "a", encoding="utf-8", newline="") as f:
        csv.writer(f).writerow(["2020_Seed0000.pdf", "Only", "2020", "t", only, "raw", "10.5555/seed.0000", "s2"])
    (idx.lib / "_reverse_citations.partial.jsonl").write_text(json.dumps({"doi": only}) + "\n", encoding="utf-8")
    for name in ("_forward_citations.degraded.csv", "_teach_forward_citations.degraded.csv"):
        (idx.lib / name).write_text(f"seed_doi,citing_doi\n10.5555/seed.0000,{only}\n", encoding="utf-8")
    assert index(idx)["exit_code"] == 0
    assert _tables(idx.db) == snap
    assert not dbq(idx.db, "SELECT 1 FROM paper_metadata WHERE doi = ? UNION ALL SELECT 1 FROM candidates "
                           "WHERE doi = ? UNION ALL SELECT 1 FROM cites WHERE cited_doi = ? OR citing_doi = ? "
                           "UNION ALL SELECT 1 FROM scoped_candidates WHERE doi = ?", [only] * 5)


def test_g_seam1_structured_dois_reach_the_index_whole(stub, idx):
    add_seed(idx, "2020_Tail", LETTER_TAIL_SEED)
    refs = [cited(i, d) for i, d in enumerate(LETTER_TAIL_REFS)] + [cited(9, "10.5555/plain.0009")]
    stub.s2[LETTER_TAIL_SEED] = {"count": 4, "refs": refs}
    assert rc.run(project="research_a")["exit_code"] == 0
    rows = rows_of(outputs(idx)["parsed"])
    assert {r["seed_doi"] for r in rows} == {LETTER_TAIL_SEED}
    assert sorted(r["doi"] for r in rows) == sorted(LETTER_TAIL_REFS + ["10.5555/plain.0009"])
    assert index(idx)["exit_code"] == 0
    assert LETTER_TAIL_SEED in held(idx.db)
    assert reverse_edges(idx.db) == {(LETTER_TAIL_SEED, d) for d in LETTER_TAIL_REFS + ["10.5555/plain.0009"]}


@pytest.mark.parametrize("d", [LETTER_TAIL_SEED, *LETTER_TAIL_REFS])
def test_g_walker_keeps_structured_dois_whole(d, tmp_path):
    for src in ("s2", "openalex", "crossref", "sidecar"):
        assert rc._final({"doi": d, "source": src}, "")[0]["doi"] == d
    pdf = tmp_path / "2020_X.pdf"
    pdf.write_bytes(b"%PDF-1.4 stub")
    (tmp_path / "2020_X.ris").write_text(f"TY  - JOUR\nDO  - {d}\nER  - \n", encoding="utf-8")
    assert rc.seed_doi(pdf) == d == ip.norm_doi(d, True)        # the form paper_locations holds


def test_g_walker_still_normalises_regex_dois():
    """The G-1 fix must stay off reference text: a regex row's junk tail is still peeled."""
    junk = "10.1016/j.jtherbio.2020.102345.url"
    assert rc._final({"doi": junk, "source": "regex"}, "")[0]["doi"] == D.normalise(junk) != junk


# ================================================================ seam 2: history tables, rebuild, GC
@pytest.fixture
def g2(net_env, monkeypatch, tmp_path):
    """A temp index built by the real index_portfolio over six seeds (three hold an abstract, one
    abstract_attempts row), FeedS2 answering recommendations and paper_batch for every seed."""
    import enrich_abstracts
    monkeypatch.delenv(s2.KEY_ENV, raising=False)
    monkeypatch.setattr(s2, "BASE", "https://api.semanticscholar.org")
    s2.reset_default_session()
    hosts.reset()
    fake = FeedS2()
    monkeypatch.setitem(net._TRANSPORTS, "requests", fake)
    monkeypatch.setitem(net._TRANSPORTS, "urllib", fake)
    root = tmp_path / "root"
    lib = root / "research_a" / "lit"
    lib.mkdir(parents=True)
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    net_env.write_config(projects={"research_a": {"lib_dir": "lit"}})
    for m in (ip, er, es):
        monkeypatch.setattr(m, "CONFIG_PATH", net_env.cfg_path)
    refuse_arxiv(net_env.state)
    seeds = [f"10.1000/seed.{i}" for i in range(1, 7)]
    for i, d in enumerate(seeds):
        (lib / f"2020_S{i}.pdf").write_bytes(b"%PDF-1.4 stub")
        (lib / f"2020_S{i}.ris").write_text(f"TY  - JOUR\nTI  - Seed {i}\nDO  - {d}\nER  - \n", encoding="utf-8")
    db = tmp_path / "g2.duckdb"
    assert ip.run(db=str(db), project="research_a")["exit_code"] == 0
    con = duckdb.connect(str(db))
    for d in seeds[:3]:
        con.execute("UPDATE paper_metadata SET abstract = ? WHERE doi = ?", [f"Local abstract of {d}", d])
    con.execute(enrich_abstracts.ATTEMPTS_DDL)
    con.execute("INSERT INTO abstract_attempts (doi, outcome, status, detail, attempted_at) "
                "VALUES (?, 'OK', '200', '', TIMESTAMP '2026-09-30 10:00:00')", [seeds[0]])
    con.close()
    for i, d in enumerate(seeds):
        fake.recs[d] = [rec(f"10.2000/rec.{i}.{k}") for k in range(3)]
        fake.batch[f"doi:{d}"] = {"paperId": "p" * 40, "externalIds": {"DOI": d}, "abstract": f"S2 abstract of {d}",
                                  "openAccessPdf": {"url": f"https://oa.example.org/{i}.pdf"},
                                  "citationCount": i, "referenceCount": 10 + i}
    net_env.fake, net_env.db, net_env.seeds, net_env.lib = fake, db, seeds, lib
    yield net_env
    s2.reset_default_session()
    hosts.reset()


def feed(env, now=T0, **kw):
    return er.run(db=str(env.db), recent_feed=True, allow_unkeyed=True, now=now, **kw)


def enrich(env, now=T0, **kw):
    return es.run(db=str(env.db), commit=True, now=now, **kw)


def schema_of(db, tables=HIST):
    con = duckdb.connect(str(db), read_only=True)
    try:
        ddl = dict(con.execute("SELECT table_name, sql FROM duckdb_tables() "
                               "WHERE database_name = current_database()").fetchall())
        cons = sorted((t, c, tuple(cols)) for t, c, cols in con.execute(
            "SELECT table_name, constraint_type, constraint_column_names FROM duckdb_constraints() "
            "WHERE database_name = current_database()").fetchall() if t in tables)
        rows = {t: sorted(map(repr, con.execute(f'SELECT * FROM "{t}"').fetchall())) for t in tables if t in ddl}
    finally:
        con.close()
    return {t: ddl.get(t) for t in tables}, cons, rows


def test_g_seam2_rebuild_carries_every_history_row_with_its_keys_and_reruns_upsert(g2):
    r = feed(g2)
    assert r["exit_code"] == 0 and r["new_pairs"] == 18
    e = enrich(g2)
    assert e["exit_code"] == 0 and e["abstracts_filled"] == 3
    ddl0, cons0, rows0 = schema_of(g2.db)
    assert all(ddl0.values()), ddl0
    assert {t for t, c, _ in cons0 if c == "PRIMARY KEY"} >= {"recent_feed", "rec_attempts", "s2_enrichment"}
    res = ip.run(db=str(g2.db), project="research_a", rebuild=True)
    assert res["exit_code"] == 0, res["reasons"]
    ddl1, cons1, rows1 = schema_of(g2.db)
    assert ddl1 == ddl0 and cons1 == cons0 and rows1 == rows0
    con = duckdb.connect(str(g2.db))
    try:
        with pytest.raises(duckdb.ConstraintException):        # NOT NULL on the key survived
            con.execute("INSERT INTO recent_feed (seed_doi, recommended_doi, pool) VALUES (NULL, 'x', 'recent')")
        with pytest.raises(duckdb.ConstraintException):        # the PRIMARY KEY survived
            con.execute("INSERT INTO rec_attempts (seed_doi) SELECT seed_doi FROM rec_attempts LIMIT 1")
        with pytest.raises(duckdb.ConstraintException):
            con.execute("INSERT INTO s2_enrichment (doi) SELECT doi FROM s2_enrichment LIMIT 1")
    finally:
        con.close()
    later = T0 + timedelta(days=1)
    assert feed(g2, now=later, refresh=True)["exit_code"] == 0
    assert enrich(g2, now=later, refresh=True)["exit_code"] == 0
    _, _, rows2 = schema_of(g2.db)
    assert {t: len(v) for t, v in rows2.items()} == {t: len(v) for t, v in rows0.items()}


def test_g_seam2_gc_spares_feed_recommendation_and_scoped_dois(g2):
    con = duckdb.connect(str(g2.db))
    try:
        con.execute(er.FEED_DDL)
        refs = {k: f"10.3000/{k}.1" for k in ("feed_seed", "feed_rec", "rec_seed", "rec_rec", "sc_doi", "sc_seed",
                                               "sx_citing", "sx_cited", "orphan")}
        for d in refs.values():
            con.execute("INSERT INTO paper_metadata (doi, title) VALUES (?, 't')", [d])
        con.execute("INSERT INTO recent_feed VALUES (?, ?, 'recent', 1, now(), now())", [refs["feed_seed"], refs["feed_rec"]])
        con.execute("INSERT INTO recommendations (seed_doi, recommended_doi, rank) VALUES (?, ?, 1)",
                    [refs["rec_seed"], refs["rec_rec"]])
        con.execute("INSERT INTO scoped_candidates (project, scope, doi, source_type, source_seed_doi) "
                    "VALUES ('research_a', 'resp', ?, 'forward', ?)", [refs["sc_doi"], refs["sc_seed"]])
        con.execute("INSERT INTO scoped_cites (project, scope, citing_doi, cited_doi, source_pipeline) "
                    "VALUES ('research_a', 'resp', ?, ?, 'forward')", [refs["sx_citing"], refs["sx_cited"]])
        n = ip.gc_orphan_metadata(con)
        left = {r[0] for r in con.execute("SELECT doi FROM paper_metadata").fetchall()}
    finally:
        con.close()
    assert n >= 1 and refs["orphan"] not in left
    assert set(refs.values()) - {refs["orphan"]} <= left
    assert set(g2.seeds) <= left


def test_g_seam2_abstract_count_never_falls_and_no_writer_commits_after_a_failed_statement(g2, monkeypatch, capsys):
    counts = [n_abstracts(g2.db)]
    assert enrich(g2)["exit_code"] == 0
    counts.append(n_abstracts(g2.db))
    assert feed(g2)["exit_code"] == 0
    counts.append(n_abstracts(g2.db))
    assert ip.run(db=str(g2.db), project="research_a")["exit_code"] == 0      # a reindex
    counts.append(n_abstracts(g2.db))
    assert counts == [3, 6, 6, 6]
    stamp0 = dbq(g2.db, "SELECT list_sort(list(fetched_at)) FROM s2_enrichment")[0][0]

    real_upsert = es.upsert

    def upsert_then_fail(con, *a, **k):                # a real DuckDB failure mid-batch (NOT NULL key)
        out = real_upsert(con, *a, **k)
        con.execute("INSERT INTO paper_metadata (doi) VALUES (NULL)")
        return out

    monkeypatch.setattr(es, "upsert", upsert_then_fail)
    capsys.readouterr()
    res = enrich(g2, now=T0 + timedelta(days=2), refresh=True)
    assert res["exit_code"] == 3 and res["aborted"] == "write_rolled_back"
    assert capsys.readouterr().out.strip().splitlines()[-1].startswith(es.SUMMARY_MARKER)
    assert dbq(g2.db, "SELECT list_sort(list(fetched_at)) FROM s2_enrichment")[0][0] == stamp0   # nothing landed
    assert n_abstracts(g2.db) == 6
    monkeypatch.setattr(es, "upsert", real_upsert)

    n_loc = dbq(g2.db, "SELECT count(*) FROM paper_locations")[0][0]
    n_runs = dbq(g2.db, "SELECT count(*) FROM index_runs")[0][0]
    (g2.lib / "2020_S5.pdf").unlink()                  # a committed reindex would drop its location

    def failing_record(con, *a, **k):
        con.execute("INSERT INTO paper_metadata (doi) VALUES (NULL)")

    monkeypatch.setattr(ip, "record_index_run", failing_record)
    with pytest.raises(duckdb.ConstraintException):
        ip.run(db=str(g2.db), project="research_a")
    assert dbq(g2.db, "SELECT count(*) FROM paper_locations")[0][0] == n_loc
    assert dbq(g2.db, "SELECT count(*) FROM index_runs")[0][0] == n_runs
    assert n_abstracts(g2.db) == 6


def test_g_seam2_after_a_rebuild_enrich_s2_refills_the_discarded_abstracts(g2):
    assert enrich(g2)["abstracts_filled"] == 3
    assert ip.run(db=str(g2.db), project="research_a", rebuild=True)["exit_code"] == 0
    assert n_abstracts(g2.db) == 0                     # the rebuild's documented discard
    res = enrich(g2, now=T0 + timedelta(days=1))
    assert res["exit_code"] == 0 and res["abstracts_after"] == 6


def test_g_seam2_recommendations_survive_a_rebuild(g2):
    assert feed(g2)["exit_code"] == 0
    n = dbq(g2.db, "SELECT count(*) FROM recommendations")[0][0]
    assert n == 18
    assert ip.run(db=str(g2.db), project="research_a", rebuild=True)["exit_code"] == 0
    assert feed(g2, now=T0 + timedelta(days=1))["exit_code"] == 0
    assert dbq(g2.db, "SELECT count(*) FROM recommendations")[0][0] == n


# ================================================================ seam 3: index_runs and the instruments
def test_g_seam3_index_runs_is_aware_utc_and_the_instruments_read_it_in_any_session_zone(g2, monkeypatch):
    (g2.lib / "2019_Text.fulltext.json").write_text(json.dumps({"doi": "10.1000/text.1", "text": "body " * 50}),
                                                    encoding="utf-8")            # text-only (pre-W2a JATS shape)
    (g2.lib / "2019_Flag.fulltext.json").write_text(json.dumps({"doi": "10.1000/flag.1", "text": "x " * 50,
                                                                "has_pdf": False, "identity": "FLAG"}),
                                                    encoding="utf-8")            # flagged: counted nowhere
    (g2.lib / "2019_NoDoi.pdf").write_bytes(b"%PDF-1.4 stub")                    # no .ris: counted
    t0 = dt.datetime.now(dt.timezone.utc)
    assert ip.run(db=str(g2.db), project="research_a")["exit_code"] == 0
    last = "SELECT {} FROM index_runs WHERE project = 'research_a' ORDER BY finished_at DESC LIMIT 1"
    for tz in ("UTC", "America/New_York", "Asia/Tokyo"):
        con = duckdb.connect(str(g2.db), read_only=True)
        try:
            con.execute(f"SET TimeZone = '{tz}'")
            v, epoch, n_files = con.execute(last.format("finished_at, epoch(finished_at), n_files")).fetchone()
        finally:
            con.close()
        assert v.tzinfo is not None and abs(v.timestamp() - t0.timestamp()) < 120
        assert abs(epoch - t0.timestamp()) < 120
        assert n_files == 6 + 1 + 1                     # PDFs (no-DOI included) plus the text-only sidecar
    scan = audit_portfolio.scan_library(g2.lib)
    real = duckdb.connect

    def tokyo(*a, **k):
        c = real(*a, **k)
        c.execute("SET TimeZone = 'Asia/Tokyo'")
        return c

    for connect in (real, tokyo):
        monkeypatch.setattr(duckdb, "connect", connect)
        st = audit_portfolio.index_status(g2.db, [("research_a", scan)])["projects"]["research_a"]
        assert st["source"] == "index_runs" and st["stale"] is None and st["count_differs"] is False
        assert st["n_index"] == st["n_disk"] == 8
        assert abs(dt.datetime.fromisoformat(st["stamp"]).timestamp() - t0.timestamp()) < 120   # local naive
    monkeypatch.setattr(duckdb, "connect", real)


# ================================================================ exit codes read by the real snowball
def test_g_exit_reverse_degraded_and_config_paths(stub, idx):
    _publish_core(stub, idx, n=20)
    stub.s2_status = 429
    r2 = inproc(step("reverse_citations.py", "--project", "research_a", "--refresh"), "reverse_citations")
    assert r2.rc == 2 and {"reasons", "aborted", "transport_failures"} <= set(r2.summary)
    assert r2.summary["transport_failures"] == 20 and reading(r2) == "DEGRADED"
    r1 = inproc(step("reverse_citations.py", "--project", "not_registered"), "reverse_citations")
    assert r1.rc == 1 and reading(r1) == "FAILED"
    r1b = inproc(step("reverse_citations.py", "--project", "research_a", "--sources", "s2,bogus"), "reverse_citations")
    assert r1b.rc == 1 and reading(r1b) == "FAILED"


def test_g_exit_reverse_aborted_reads_degraded(stub, idx):
    for i in range(3):
        add_seed(idx, f"2020_El{i}", f"10.5555/el.{i:04d}")
        stub.s2[f"10.5555/el.{i:04d}"] = {"count": 5, "refs": None}
    stub.oa.headers = {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "900"}
    stub.oa.add(work(1, "10.5555/el.0000", refs=[2]), work(2, "10.5555/x.2"))
    r3 = inproc(step("reverse_citations.py", "--project", "research_a"), "reverse_citations")
    assert r3.rc == 3 and "openalex" in r3.summary["aborted"] and reading(r3) == "DEGRADED"


def test_g_exit_index_degraded_and_config_paths(stub, idx, monkeypatch, tmp_path):
    add_seed(idx, "2020_A", "10.5555/a.0001")
    idx.write_config(projects={"research_a": {"lib_dir": "lit", "data_dir": "data"},
                               "research_gone": {"lib_dir": "missing"}})
    r2 = inproc(step("index_portfolio.py", "--db", str(idx.db)), "index_portfolio")
    assert r2.rc == 2 and r2.summary["reasons"] and reading(r2) == "DEGRADED"
    r1 = inproc(step("index_portfolio.py", "--project", "nope", "--db", str(idx.db)), "index_portfolio")
    assert r1.rc == 1 and reading(r1) == "FAILED"
    monkeypatch.setattr(ip, "CONFIG_PATH", tmp_path / "absent.json")
    fresh = tmp_path / "never.duckdb"
    r1c = inproc(step("index_portfolio.py", "--db", str(fresh)), "index_portfolio")
    assert r1c.rc == 1 and reading(r1c) == "FAILED" and not fresh.exists()


def test_g_exit_enrich_paths(g2, monkeypatch, tmp_path):
    db = str(g2.db)
    g2.fake.recs[g2.seeds[0]] = 500                     # 1 of 6 seeds fails: over 5 %
    r2 = inproc(step("enrich_recommendations.py", "--db", db, "--recent-feed", "--allow-unkeyed"), "recs")
    assert r2.rc == 2 and reading(r2) == "DEGRADED", r2.summary
    for d in g2.seeds:
        g2.fake.recs[d] = 500
    r3 = inproc(step("enrich_recommendations.py", "--db", db, "--recent-feed", "--allow-unkeyed", "--refresh"), "recs")
    assert r3.rc == 3 and r3.summary["aborted"] and reading(r3) == "DEGRADED"
    r1 = inproc(step("enrich_recommendations.py", "--db", db, "--recent-feed", "--allow-unkeyed", "--top-n", "0"), "recs")
    assert r1.rc == 1 and reading(r1) == "FAILED"
    off = inproc(step("enrich_recommendations.py", "--db", db), "recs")
    assert off.rc == 0 and reading(off) == "ok"

    g2.fake.batch_status = 500
    k = inproc(["py", "enrich_s2", "--db", db, "--commit"], "enrich_s2", main=es.main)
    assert k.rc in (2, 3) and {"reasons", "aborted", "transport_failures"} <= set(k.summary)
    assert reading(k) == "DEGRADED"
    g2.fake.batch_status = None

    def fail(con, *a, **kw):
        raise duckdb.ConstraintException("scripted")

    monkeypatch.setattr(es, "upsert", fail)
    k3 = inproc(["py", "enrich_s2", "--db", db, "--commit", "--refresh"], "enrich_s2", main=es.main)
    assert k3.rc == 3 and k3.summary["aborted"] == "write_rolled_back" and reading(k3) == "DEGRADED"
    k1 = inproc(["py", "enrich_s2", "--db", str(tmp_path / "missing.duckdb")], "enrich_s2", main=es.main)
    assert k1.rc == 1 and reading(k1) == "FAILED"


def test_g_unkeyed_reverse_step_keeps_prior_s2_rows_and_snowball_reads_ok(stub, idx, monkeypatch):
    monkeypatch.setenv(s2.KEY_ENV, "s2-TESTKEY-verify-g")
    _publish_core(stub, idx, n=10)                     # an earlier keyed run published S2 rows
    published = sorted((r["seed"], r["doi"], r["source"]) for r in rows_of(outputs(idx)["parsed"]))
    assert {s for _, _, s in published} == {"s2"}
    monkeypatch.delenv(s2.KEY_ENV)
    monkeypatch.delenv(oa.KEY_ENV)
    monkeypatch.setattr(snowball, "DB_PATH", idx.db)
    monkeypatch.setattr(snowball, "LOG_PATH", idx.db.parent / "g_convergence_log.csv")
    monkeypatch.setattr(snowball, "CONFIG_PATH", idx.cfg_path)
    cmds = []

    def runner(cmd, label):
        cmds.append(list(cmd))
        return inproc(cmd, label)

    n_s2 = stub.hosts()["api.semanticscholar.org"]
    res = snowball.run(project="research_a", skip_forward=True, skip_abstracts=True, step_runner=runner)
    assert res["exit_code"] == 0, res
    rev = next(c for c in cmds if Path(c[1]).stem == "reverse_citations")
    assert rev[-2:] == ["--sources", "openalex,crossref,regex"]
    assert stub.hosts()["api.semanticscholar.org"] == n_s2 and stub.hosts()["api.openalex.org"] == 0
    assert sorted((r["seed"], r["doi"], r["source"]) for r in rows_of(outputs(idx)["parsed"])) == published


# ================================================================ the OpenAlex client
@pytest.fixture
def oastub(oaenv, monkeypatch):
    st = OAStub()
    monkeypatch.setitem(net._TRANSPORTS, "requests", st)
    monkeypatch.setitem(net._TRANSPORTS, "urllib", st)
    return st


def test_g_openalex_key_is_never_persisted_on_retries_or_a_401_that_echoes_it(oastub, oaenv, capsys):
    oastub.script(2, 503).script(1, 401, body={"error": "Invalid or missing API key",
                                                "message": f"API key {OA_TEST_KEY} not found"})
    got = oa.works_by_doi(["10.5555/a.0001"])["10.5555/a.0001"]
    assert got.kind is Kind.CONFIG
    out, err = capsys.readouterr()
    blobs = [got.detail or "", oaenv.ledger_text(), out, err, *(s["url"] for s in oastub.sent),
             json.dumps([s["params"] for s in oastub.sent])]
    assert all(OA_TEST_KEY not in b for b in blobs)
    assert len(oastub.sent) == 3
    assert all(s["headers"].get("Authorization") == f"Bearer {OA_TEST_KEY}" for s in oastub.sent)


def test_g_openalex_chunks_as_sent_stay_under_7500_bytes_with_sici_dois(oastub):
    dois = [f"10.1002/(sici)1097-4636(1996{i:04d})30:1<{i}::aid-jbm{i}>3.0.co;2-t" for i in range(250)]
    want = {D.normalise(d) for d in dois} - {None}
    assert len(want) >= 200
    oa.works_by_doi(dois)
    calls = oastub.list_calls()
    sent = [v for c in calls for v in c["params"]["filter"].split(":", 1)[1].split("|")]
    assert set(sent) == want and len(calls) >= 3
    assert all(oa.url_bytes(c["url"]) <= oa.URL_SAFE_BYTES for c in calls)
    assert all(len(c["url"].encode("utf-8")) <= oa.URL_LIMIT_BYTES for c in calls)
    assert all(len(c["params"]["filter"].split("|")) <= oa.OR_MAX for c in calls)


def test_g_openalex_letter_tail_dois_of_one_journal_do_not_collapse(oastub):
    a, b = LETTER_TAIL_REFS[0], LETTER_TAIL_REFS[1]
    oastub.add(work(1, a), work(2, b))
    got = oa.works_by_doi([a, b])
    assert got[a].ok and got[b].ok
    assert oa.work_id(got[a].payload["id"]) == "W1" and oa.work_id(got[b].payload["id"]) == "W2"


def test_g_openalex_host_note_states_the_documented_budget():
    """help.openalex.org/access/example-costs (read 2026-10-05): a free key gives $1 of usage a
    day (10,000 credits at 1 credit = $0.0001), keyless $0.10 (1,000 credits); a list call costs 1
    credit, a singleton 0. The note must not read as if keyless and keyed were different units."""
    note = oa.host_policy().note
    assert "10,000 credits/day" in note and "1,000" in note


# ================================================================ DOIs, writes, rename
@pytest.mark.parametrize("raw, want", [
    ("10.1519/1533-4295(2006)28[44:msastr]2.0.co;2", "10.1519/1533-4295(2006)28[44:msastr]2.0.co;2"),
    ("10.1519/1533-4295(2005)027[0048:rtftoa]2.0.co;2", "10.1519/1533-4295(2005)027[0048:rtftoa]2.0.co;2"),
    ("10.1519/1533-4295(2005)027\\[0050:seastt\\]2.0.co;2", "10.1519/1533-4295(2005)027[0050:seastt]2.0.co;2"),
    ("see [10.1519/1533-4295(2007)29[24:ptpotc]2.0.co;2].", "10.1519/1533-4295(2007)29[24:ptpotc]2.0.co;2"),
])
def test_g_sici_dois_with_square_brackets_stay_whole(raw, want):
    assert D.normalise(raw) == want
    assert ip.norm_doi(raw, True) == want


def _old_upsert(con, rows):
    """ce0479b index_portfolio.py:255-278 (the per-row path), logic verbatim."""
    dedup = ip._dedup_first(rows)
    dois = [r[0] for r in dedup]
    existing = {r[0] for r in con.execute(
        f"SELECT doi FROM paper_metadata WHERE doi IN ({','.join('?' * len(dois))})", dois).fetchall()}
    upd = [(r[1], r[2], r[3], r[4], r[5], r[6], r[0]) for r in dedup if r[0] in existing]
    if upd:
        con.executemany("UPDATE paper_metadata SET year=?, lastname=?, title=?, venue=?, authors=?, "
                        "refreshed_at=? WHERE doi=?", upd)
    ip._bulk_insert(con, "paper_metadata", ip._META_COLS, [r for r in dedup if r[0] not in existing],
                    int_cols=["year"])


def test_g_set_based_metadata_refresh_equals_the_old_per_row_path(tmp_path):
    now = "2026-10-05T12:00:00"
    existing = [("10.1/a.1", 2001, "Smith", "T", "V", "A", "keep me"), ("10.1/b.2", None, None, None, None, None, None),
                ("10.1/c.3", 2003, "Lee", "", "", "", ""), ("10.1/e.5", 2005, "Ng", "T5", "V5", "A5", "abs 5")]
    incoming = [("10.1/a.1", None, None, None, None, None, now), ("10.1/b.2", 2002, "Ng", "T2", "V2", "A2", now),
                ("10.1/c.3", 2003, "Lee", "", "", "", now), ("10.1/d.4", None, "", None, "", "", now),
                ("10.1/a.1", 1999, "Dup", "x", "x", "x", now), ("10.1/e.5", 2005, "Ng", "T5", "V5", "A5", now)]
    finals = []
    for k, path in enumerate((_old_upsert, ip.upsert_library_metadata)):
        con = duckdb.connect(str(tmp_path / f"m{k}.duckdb"))
        con.execute(ip.SCHEMA)
        for d, y, ln, t, v, a, ab in existing:
            con.execute("INSERT INTO paper_metadata (doi, year, lastname, title, venue, authors, abstract, refreshed_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, TIMESTAMP '2026-01-01 00:00:00')", [d, y, ln, t, v, a, ab])
        path(con, incoming)
        finals.append(con.execute("SELECT * FROM paper_metadata ORDER BY doi").fetchall())
        con.close()
    assert finals[0] == finals[1]
    assert len(finals[1]) == 5 and dict((r[0], r) for r in finals[1])["10.1/a.1"][1] is None


def test_g_rename_dry_run_is_read_only_and_execute_moves_every_project_column_at_once(g2, monkeypatch):
    con = duckdb.connect(str(g2.db))
    try:
        con.execute(er.FEED_DDL)
        con.execute(er.ATTEMPTS_DDL)
        con.execute(es.DDL)
        con.execute("INSERT INTO scoped_candidates (project, scope, doi, source_type, source_seed_doi) "
                    "VALUES ('research_a', 'resp', '10.4000/sc.1', 'forward', '10.1000/seed.1')")
        con.execute("INSERT INTO scoped_cites (project, scope, citing_doi, cited_doi, source_pipeline) "
                    "VALUES ('research_a', 'resp', '10.4000/sc.1', '10.1000/seed.1', 'forward')")
        con.execute("INSERT INTO papers_no_doi (pdf_filename, project, lib_path, reason) "
                    "VALUES ('x.pdf', 'research_a', 'lib', 'no_ris')")
        cols = ip.project_columns(con)
        before = {f"{t}.{c}": con.execute(f'SELECT count(*) FROM "{t}" WHERE "{c}" = ?', ["research_a"]).fetchone()[0]
                  for t, c in cols}
    finally:
        con.close()
    tables = {t for t, _ in cols}
    assert {"paper_locations", "papers_no_doi", "scoped_candidates", "scoped_cites", "index_runs"} <= tables
    assert not tables & set(HIST)                      # W3-E's tables carry no project key
    assert all(before[k] for k in ("paper_locations.project", "scoped_candidates.project", "index_runs.project"))

    opened = []
    real_connect = lit_util.connect_db

    def spy(*a, **k):
        opened.append(k.get("read_only"))
        return real_connect(*a, **k)

    monkeypatch.setattr(lit_util, "connect_db", spy)

    def counts(name):
        return {f"{t}.{c}": dbq(g2.db, f'SELECT count(*) FROM "{t}" WHERE "{c}" = ?', [name])[0][0] for t, c in cols}

    assert ip.run(db=str(g2.db), rename_project=("research_a", "research_b"))["exit_code"] == 0
    assert opened[-1] is True and counts("research_a") == before

    real_pk = ip._primary_key

    def pk_fails_last(con, table):
        if table == cols[-1][0]:
            raise RuntimeError("scripted failure on the last table")
        return real_pk(con, table)

    monkeypatch.setattr(ip, "_primary_key", pk_fails_last)
    with pytest.raises(RuntimeError):
        ip.run(db=str(g2.db), rename_project=("research_a", "research_b"), execute=True)
    assert counts("research_a") == before               # one transaction: nothing moved
    monkeypatch.setattr(ip, "_primary_key", real_pk)

    assert ip.run(db=str(g2.db), rename_project=("research_a", "research_b"), execute=True)["exit_code"] == 0
    assert opened[-1] is False
    assert not any(counts("research_a").values()) and counts("research_b") == before
