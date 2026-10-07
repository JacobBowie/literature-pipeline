"""W2b verifier F: the degraded-walk guard, abstracts, backfill_ris and the instruments, end to end
with the REAL modules, and the gaps the builders' own tests could not see.

tests/test_w2d2_forward.py drives forward_citations through a stub transport on a FakeState. Here
forward_citations.run drives the REAL litpipe.s2 and the REAL litpipe.net into a loopback MockServer
answering like Semantic Scholar, against the REAL litpipe.state in a temp state_dir (its clock and
sleep virtual, so 6.5 s pacing costs nothing). snowball.main runs in-process with its children
stubbed at the subprocess boundary; the forward step's child is the real forward_citations.main
in-process, so snowball parses the walker's real `[step-summary]` line. backfill_ris writes text-only
`.ris` files that the real pipeline_check and audit_portfolio then read (the W2-E2/W2-F seam).

Tests commented `lock for APPLY F-<n>` lock an APPLY item of the verifier's report: each failed on
the code at 61eb10b (checked with --runxfail) and passes with the fix.
"""
import contextlib
import csv
import hashlib
import io
import json
import os
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

import audit_portfolio
import backfill_ris
import enrich_abstracts as ea
import forward_citations as fc
import lit_util
import pipeline_check
import snowball
from litpipe import config, holdings, hosts, ledger, net, s2, state
from litpipe import text as T
from tests.netmock import Reply

REPO = Path(__file__).resolve().parent.parent
SECRET = "s2-VERIFYF-key-0a1b2c3d4e5f6a7b"
EMAIL = "tester@litpipe-test.org"          # net_env's LITPIPE_EMAIL
BATCH = "/graph/v1/paper/batch"
JSON_H = {"Content-Type": "application/json"}


# ================================================================ helpers
def pid_of(doi):
    return hashlib.sha1(doi.encode()).hexdigest()


def cites_path(doi):
    return f"/graph/v1/paper/{pid_of(doi)}/citations"


def citer(i, seed):
    return {"citingPaper": {"paperId": pid_of(f"{seed}/c{i}"), "externalIds": {"DOI": f"10.9999/{seed[-4:]}.c{i}"},
                            "title": f"Citer {i}", "abstract": None, "year": 2021, "venue": "J Test",
                            "authors": [{"authorId": "1", "name": "Ann Author"}], "citationCount": i}}


def batch_reply(counts):
    """counts: list of int (citationCount) or None (no S2 record), in request order."""
    body = [None if n is None else {"paperId": pid_of(d), "citationCount": n} for d, n in counts]
    return Reply(200, json.dumps(body), JSON_H)


def page_reply(doi, n):
    return Reply(200, json.dumps({"offset": 0, "data": [citer(i, doi) for i in range(n)]}), JSON_H)


def nested_reply(rows):
    """A nested POST /paper/batch answer (W3-A: seeds of <= 1,000 citers): rows is a list of
    (doi, citationCount, n listed) in the order the client packs them (first-fit decreasing by count,
    stable). Listing fewer than the count is S2's silent nested truncation; the client re-fetches
    that seed by paged GET."""
    body = [{"paperId": pid_of(d), "citationCount": c, "citations": [citer(i, d)["citingPaper"] for i in range(n)]}
            for d, c, n in rows]
    return Reply(200, json.dumps(body), JSON_H)


def pages_1001(doi):
    """The two paged GET answers of a 1,001-citer seed (W3-A: seeds over 1,000 are paged)."""
    return (Reply(200, json.dumps({"offset": 0, "next": 1000, "data": [citer(i, doi) for i in range(1000)]}), JSON_H),
            Reply(200, json.dumps({"offset": 1000, "data": [citer(1000, doi)]}), JSON_H))


def make_lib(root, dois, name="literature"):
    lib = Path(root) / name
    lib.mkdir(parents=True, exist_ok=True)
    for i, d in enumerate(dois):
        (lib / f"2020_Seed{i:04d}.pdf").write_bytes(b"%PDF-1.4 stub")
        (lib / f"2020_Seed{i:04d}.ris").write_text(f"TY  - JOUR\nDO  - {d}\nER  - \n", encoding="utf-8")
    return lib


def write_prior(lib, rows_by_doi):
    out = lib / "_forward_citations.csv"
    with open(out, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fc.FIELDS)
        w.writeheader()
        for k, (doi, n) in enumerate(rows_by_doi.items()):
            for i in range(n):
                w.writerow({"seed_pdf": f"2020_Seed{k:04d}.pdf", "seed_doi": doi, "citing_paper_id": f"old{i}",
                            "citing_doi": f"10.7777/old.{k}.{i}", "citing_title": "Old citer"})
    return out


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read_csv(path):
    with open(path, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def all_bytes(*dirs):
    """Every file's bytes under the given dirs (for redaction scans)."""
    out = {}
    for d in dirs:
        d = Path(d)
        if d.is_dir():
            for p in d.rglob("*"):
                if p.is_file():
                    out[str(p)] = p.read_bytes()
    return out


def tree(*dirs):
    out = set()
    for d in dirs:
        d = Path(d)
        if d.exists():
            out |= {str(p) for p in d.rglob("*")}
    return out


# ================================================================ the real S2 world
@pytest.fixture
def world(net_env, mock_server, monkeypatch, tmp_path):
    """MockServer as api.semanticscholar.org; the REAL litpipe.state in net_env's temp state_dir
    (net.STATE None, so litpipe.s2 falls through to the module); virtual pacing clock; arXiv refused
    in that state first (dispatch shared section). Yields (server, env, state_dir)."""
    srv = mock_server()
    monkeypatch.setattr(s2, "BASE", srv.url("").rstrip("/"))
    monkeypatch.delenv(s2.KEY_ENV, raising=False)
    monkeypatch.setattr(net, "STATE", None)
    monkeypatch.setattr(state, "DB_PATH", None)
    monkeypatch.setattr(state, "_time", net_env.clock.time)
    monkeypatch.setattr(state, "_sleep", net_env.clock.sleep)
    s2.reset_default_session()
    sdir = net_env.cfg_path.parent / "state"
    assert not sdir.exists()
    for h in ("export.arxiv.org", "arxiv.org", "www.arxiv.org"):
        state.refuse(h, "manual: refused since 2026-09-25", persistence="manual")
    assert (sdir / state.DB_NAME).exists()          # the real state file, in the temp state_dir
    yield srv, net_env, sdir
    s2.reset_default_session()
    hosts.reset()
    assert not any("arxiv" in ln for ln in net_env.ledger_text().splitlines()
                   if '"host"' in ln and "arxiv" in ln.split('"host"')[1][:40])


def walk(lib, **kw):
    return fc.run(lib_dir=str(lib), **kw)


# ================================================================ W2-D2: forward walk, real modules
def test_real_clean_walk_publishes_zero_citer_and_unresolved_are_not_failures(world, tmp_path):
    srv, env, sdir = world
    ds = ["10.5555/vf.c001", "10.5555/vf.c002", "10.5555/vf.c003", "10.5555/vf.c004"]
    lib = make_lib(tmp_path / "root", ds)
    srv.script(BATCH, batch_reply([(ds[0], 2), (ds[1], 0), (ds[2], 1), (ds[3], None)]),
               nested_reply([(ds[0], 2, 2), (ds[2], 1, 1)]))            # W3-A: the nested batch
    srv.script(cites_path(ds[0]), page_reply(ds[0], 2))
    srv.script(cites_path(ds[2]), page_reply(ds[2], 1))
    res = walk(lib)
    assert res["exit_code"] == 0 and res["published"]
    assert (res["failed"], res["zero_citers"], res["unresolved"]) == (0, 1, 1)
    assert not srv.hits_for(cites_path(ds[1]))                        # count 0: no call at all
    rows = read_csv(lib / "_forward_citations.csv")
    assert sorted({r["seed_doi"] for r in rows}) == [ds[0], ds[2]] and len(rows) == 3
    assert not fc.journal_path(lib / "_forward_citations.csv").exists()
    assert not fc.degraded_path(lib / "_forward_citations.csv").exists()
    # the real state counted every attempt for the mock host (W3-A: 1 metadata batch + 1 nested batch)
    assert state.day_count("127.0.0.1") == 2 == len(srv.hits)


def test_real_throttled_walk_exits_3_keeps_prior_byte_identical_and_the_journal(world, tmp_path):
    srv, env, sdir = world
    ds = [f"10.5555/vf.t{i}" for i in range(4)]
    lib = make_lib(tmp_path / "root", ds)
    prior = write_prior(lib, {d: 2 for d in ds})
    before = sha(prior)
    srv.script(BATCH, batch_reply([(d, 1001) for d in ds]))           # W3-A: paged seeds (3 now go nested)
    for d in ds:
        srv.script(cites_path(d), Reply(429, json.dumps({"message": "Too Many Requests"}), JSON_H))
    res = walk(lib)
    assert res["exit_code"] == 3 and res["status"] == "aborted" and not res["published"]
    assert sha(prior) == before                                        # byte-identical
    deg = fc.degraded_path(prior)
    assert deg.exists()
    # every seed keeps its published rows in the degraded result (a failed seed is never 0 citers)
    assert {r["seed_doi"] for r in read_csv(deg)} == set(ds)
    jp = fc.journal_path(prior)
    assert jp.exists() and res["journal"] == str(jp)
    assert res["transport_failures"] >= 1 and res["failed_citations"] >= 1
    assert not (lib / "_forward_citations_unique_dois.csv").exists()


def test_real_resume_rerequests_nothing_finished(world, tmp_path):
    srv, env, sdir = world
    ds = [f"10.5555/vf.r{i}" for i in range(5)]
    lib = make_lib(tmp_path / "root", ds)
    # W3-A: the nested batch truncates every list (S2's silent nested truncation), so each seed is
    # re-fetched by one paged GET: metadata + nested + 2 seeds = budget 4 (was batch + 2 seeds = 3).
    # The second run's seeds are over 1,000 citers (paged), so its only batch is the metadata pass.
    srv.script(BATCH, batch_reply([(d, 1) for d in ds]), nested_reply([(d, 1, 0) for d in ds]),
               batch_reply([(d, 1001) for d in ds[2:]]))
    for d in ds[:2]:
        srv.script(cites_path(d), page_reply(d, 1))
    for d in ds[2:]:
        srv.script(cites_path(d), *pages_1001(d))
    first = walk(lib, session=s2.Session(budget=4))                    # batch + nested + 2 seeds, then spent
    assert first["exit_code"] == 3 and first["aborted"] == "budget"
    assert fc.journal_path(lib / "_forward_citations.csv").exists()
    assert not (lib / "_forward_citations.csv").exists()
    n_hits_first = len(srv.hits)
    second = walk(lib)
    assert second["exit_code"] == 0 and second["resumed"] == 2
    later = srv.hits[n_hits_first:]
    batch_ids = [json.loads(h.body)["ids"] for h in later if h.path == BATCH]
    assert batch_ids == [[f"DOI:{d}" for d in ds[2:]]]                 # finished seeds not resolved again
    for d in ds[:2]:
        assert len(srv.hits_for(cites_path(d))) == 1                   # nor walked again
    assert {r["seed_doi"] for r in read_csv(lib / "_forward_citations.csv")} == set(ds)


def test_real_count_drift_is_recounted_once_and_published(world, tmp_path):
    srv, env, sdir = world
    ds = ["10.5555/vf.m0", "10.5555/vf.m1"]
    lib = make_lib(tmp_path / "root", ds)
    srv.script(BATCH, batch_reply([(ds[0], 3), (ds[1], 1)]),
               nested_reply([(ds[0], 3, 2), (ds[1], 1, 1)]),            # W3-A: nested, ds[0] short
               batch_reply([(ds[0], 2)]))
    srv.script(cites_path(ds[0]), page_reply(ds[0], 2))               # 2 rows against a count of 3
    srv.script(cites_path(ds[1]), page_reply(ds[1], 1))
    res = walk(lib)
    assert res["exit_code"] == 0 and res["recounted"] == 1 and res["count_mismatch"] == 0
    # W3-A: metadata + nested + recount on the batch path (was 2: no nested batch)
    assert len(srv.hits_for(BATCH)) == 3 and len(srv.hits_for(cites_path(ds[0]))) == 2
    rows = read_csv(lib / "_forward_citations.csv")
    assert sum(r["seed_doi"] == ds[0] for r in rows) == 2


def test_real_persistent_mismatch_stays_failed_and_is_not_rewalked(world, tmp_path):
    srv, env, sdir = world
    ds = ["10.5555/vf.p0", "10.5555/vf.p1"]
    lib = make_lib(tmp_path / "root", ds)
    prior = write_prior(lib, {ds[0]: 4, ds[1]: 1})
    srv.script(BATCH, batch_reply([(ds[0], 3), (ds[1], 1)]),
               nested_reply([(ds[0], 3, 2), (ds[1], 1, 1)]),            # W3-A: nested, ds[0] short
               batch_reply([(ds[0], 3)]))
    srv.script(cites_path(ds[0]), page_reply(ds[0], 2))
    srv.script(cites_path(ds[1]), page_reply(ds[1], 1))
    before = sha(prior)
    res = walk(lib)
    assert res["exit_code"] == 2 and res["count_mismatch"] == 1 and res["transport_failures"] == 0
    # W3-A: metadata + nested + recount on the batch path (was 2: no nested batch)
    assert len(srv.hits_for(BATCH)) == 3 and len(srv.hits_for(cites_path(ds[0]))) == 1
    assert sha(prior) == before
    deg = read_csv(fc.degraded_path(prior))
    assert sum(r["seed_doi"] == ds[0] for r in deg) == 4               # the prior rows, not the partial 2


# lock for APPLY F-4: a seed whose PDF is still in the library but whose DOI could not be read this run
# (its .ris gone or unreadable, no DOI in the PDF text) is not a removed seed: publishing drops its
# published citers, the 09-16 shape (a worse CSV replaces a better one) by another road.
# lock for APPLY F-4 (fixed in the dispatcher commit after 61eb10b)
def test_a_seed_whose_doi_went_unreadable_does_not_publish_away_its_rows(world, tmp_path):
    srv, env, sdir = world
    ds = [f"10.5555/vf.n{i}" for i in range(3)]
    lib = make_lib(tmp_path / "root", ds)
    prior = write_prior(lib, {d: 2 for d in ds})
    before = sha(prior)
    (lib / "2020_Seed0001.ris").unlink()                               # the PDF stays; its DOI is unreadable
    srv.script(BATCH, batch_reply([(ds[0], 2), (ds[2], 2)]),
               nested_reply([(ds[0], 2, 2), (ds[2], 2, 2)]))            # W3-A: the nested batch
    srv.script(cites_path(ds[0]), page_reply(ds[0], 2))
    srv.script(cites_path(ds[2]), page_reply(ds[2], 2))
    res = walk(lib)
    assert res["no_doi"] == 1
    assert res["exit_code"] == 2 and sha(prior) == before


def test_real_s2_key_flag_reaches_the_header_and_nothing_persisted_or_printed(world, tmp_path, capsys):
    srv, env, sdir = world
    ds = ["10.5555/vf.k0", "10.5555/vf.k1"]
    lib = make_lib(tmp_path / "root", ds)
    srv.script(BATCH, batch_reply([(ds[0], 1), (ds[1], 1)]),
               nested_reply([(ds[0], 1, 0), (ds[1], 1, 1)]))            # W3-A: ds[0] short -> paged GET
    srv.script(cites_path(ds[0]), Reply(500, json.dumps({"message": "boom"}), JSON_H))   # a failure detail path
    srv.script(cites_path(ds[1]), page_reply(ds[1], 1))
    res = walk(lib, s2_key=SECRET)
    assert os.environ.get(s2.KEY_ENV) is None                          # restored after the run
    assert all(h.headers.get("x-api-key") == SECRET for h in srv.hits)  # the key worked
    out = capsys.readouterr()
    blobs = all_bytes(lib, sdir, env.cfg_path.parent / "ledger")
    blobs["stdout"] = (out.out + out.err).encode("utf-8")
    blobs["result"] = json.dumps(res, default=str).encode("utf-8")
    for where, b in blobs.items():
        assert SECRET.encode() not in b, where
        assert EMAIL.encode() not in b, where


# ================================================================ snowball over the real walker
class FakeProc:
    def __init__(self, text, rc):
        self.stdout = io.StringIO(text)
        self._rc = rc

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def wait(self):
        return self._rc


@pytest.fixture
def snow(world, tmp_path, monkeypatch):
    """snowball and forward_citations on one temp registry and root; children stubbed at Popen,
    forward_citations answered by the real fc.main in-process."""
    srv, env, sdir = world
    root = tmp_path / "root"
    env.write_config(projects={"research_a": {"lib_dir": "literature"}})
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    for mod in (snowball, fc):
        monkeypatch.setattr(mod, "CONFIG_PATH", env.cfg_path)
    refs = tmp_path / "refs"
    monkeypatch.setattr(snowball, "LOG_PATH", refs / "convergence_log.csv")
    monkeypatch.setattr(snowball, "DB_PATH", refs / "portfolio.duckdb")
    cmds = []

    def popen(cmd, **kw):
        cmds.append(list(cmd))
        assert kw.get("env", {}).get("PYTHONUNBUFFERED") == "1"
        if Path(cmd[1]).name == "forward_citations.py":
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                rc = fc.main(cmd[2:])
            return FakeProc(buf.getvalue(), rc)
        return FakeProc("child ok\n", 0)

    monkeypatch.setattr(snowball.subprocess, "Popen", popen)
    return srv, root, cmds


def test_snowball_reads_the_real_walkers_summary_and_logs_degraded(snow, capsys):
    srv, root, cmds = snow
    ds = [f"10.5555/vf.s{i}" for i in range(3)]
    make_lib(root / "research_a", ds)
    srv.script(BATCH, batch_reply([(d, 1001) for d in ds]))           # W3-A: paged seeds (2 now go nested)
    for d in ds:
        srv.script(cites_path(d), Reply(429, json.dumps({"message": "Too Many Requests"}), JSON_H))
    assert snowball.main(["--project", "research_a"]) == 2
    rows = snowball.read_log()
    assert len(rows) == 1 and rows[0]["reason"].startswith("DEGRADED: forward_citations exit 3 (aborted: ")
    assert not any("enrich_recommendations" in " ".join(c) for c in cmds)   # 0 recommendation calls
    assert sum("enrich_abstracts" in " ".join(c) for c in cmds) == 1        # once, after the loop


def test_snowball_degrades_on_one_transport_failure_under_the_threshold(snow):
    srv, root, cmds = snow
    ds = [f"10.5555/vf.u{i:02d}" for i in range(40)]
    make_lib(root / "research_a", ds)
    srv.script(BATCH, batch_reply([(d, 1) for d in ds]),             # W3-A: nested, ds[7] short -> GET
               nested_reply([(d, 1, 0 if d == ds[7] else 1) for d in ds]))
    for d in ds:
        srv.script(cites_path(d), page_reply(d, 1))
    srv.script(cites_path(ds[7]), Reply(503, json.dumps({"message": "unavailable"}), JSON_H))
    res = snowball.run(project="research_a", skip_abstracts=True)
    it = res["projects"][0]["iterations"][0]
    fwd = it["steps"][0]
    assert fwd["rc"] == 0 and fwd["summary"]["transport_failures"] == 1   # walker published (1 of 40)
    assert it["status"] == "DEGRADED" and "recorded 1 transport failure" in it["reason"]
    assert res["exit_code"] == 2


# lock for APPLY F-1: a child's usage or configuration exit 2 (argparse, ris_emit.load_projects_config,
# reverse_citations' config errors) is not a degraded walk; only the walker's verdict is.
# lock for APPLY F-1 (fixed in the dispatcher commit after 61eb10b)
@pytest.mark.parametrize("label", ["reverse_citations", "index_portfolio"])
def test_a_child_usage_exit_2_without_a_step_summary_is_failed_not_degraded(snow, monkeypatch, label):
    srv, root, cmds = snow
    make_lib(root / "research_a", ["10.5555/vf.x0"])

    def runner(cmd, lab):
        rc = 2 if Path(cmd[1]).stem == label else 0
        summ = {"exit_code": 0, "transport_failures": 0} if Path(cmd[1]).stem == "forward_citations" else None
        return snowball.StepResult(lab, list(cmd), rc, summ)

    monkeypatch.setattr(snowball, "candidate_count", lambda p: 5)
    res = snowball.run(project="research_a", skip_abstracts=True, step_runner=runner)
    assert res["projects"][0]["status"] == "FAILED"
    assert res["exit_code"] == 1


# ================================================================ the convergence log upgrade
OLD_LOG_CRLF = (b"date,project,iter,n_before,n_after,growth_pct\r\n"
                b"2026-05-06,research_a,1,9810,17248,75.82\r\n"
                b"2026-05-06,research_a,2,17248,17248,0.00\r\n"
                b"2026-08-19,teaching_b,2,36767,36694,-0.20\r\n"
                b"2026-09-16,\"teaching_b/sub, x\",1,23187,36767,58.57\r\n")


def test_old_log_upgrade_survives_a_killed_write_and_keeps_rows_and_crlf(snow, monkeypatch):
    log = snowball.LOG_PATH
    log.parent.mkdir(parents=True, exist_ok=True)
    log.write_bytes(OLD_LOG_CRLF)

    def killed(src, dst):
        raise KeyboardInterrupt("killed between write and replace")

    monkeypatch.setattr(lit_util, "_replace_with_retry", killed)
    with pytest.raises(KeyboardInterrupt):
        snowball.log_iteration("research_a", 1, 10, 12, 20.0, "ok; stop: single iteration")
    assert log.read_bytes() == OLD_LOG_CRLF                            # untouched
    assert sorted(p.name for p in log.parent.iterdir()) == [log.name]  # no temp file left behind
    monkeypatch.undo()
    monkeypatch.setattr(snowball, "LOG_PATH", log)
    snowball.log_iteration("research_a", 1, 10, 12, 20.0, "ok; stop: single iteration")
    raw = log.read_bytes()
    lines = raw.split(b"\r\n")
    assert lines[-1] == b"" and b"\n" not in b"".join(lines)          # every line CRLF, none bare LF
    old = list(csv.reader(io.StringIO(OLD_LOG_CRLF.decode(), newline="")))
    new = list(csv.reader(io.StringIO(raw.decode(), newline="")))
    assert new[0] == old[0] + ["reason"]
    assert new[1:len(old)] == [r + [""] for r in old[1:]]             # every old row, values kept
    assert new[-1][-1] == "ok; stop: single iteration" and len(new) == len(old) + 1
    assert [r["growth_pct"] for r in snowball.read_log()][:4] == ["75.82", "0.00", "-0.20", "58.57"]


# ================================================================ W2-E2: abstracts on the real index
CR = "/works/"


@pytest.fixture
def indexed(net_env, mock_server, monkeypatch, tmp_path):
    """A temp portfolio.duckdb built by the REAL index_portfolio over a 3-PDF library; Crossref on a
    MockServer through the real litpipe.net."""
    import index_portfolio
    srv = mock_server()
    root = tmp_path / "root"
    ds = ["10.5555/ab.0", "10.5555/ab.1", "10.5555/ab.2"]
    lib = make_lib(root / "research_a", ds)
    reg = {"state_dir": str(tmp_path / "state"), "projects": {"research_a": {"lib_dir": "literature"}}}
    net_env.cfg_path.write_text(json.dumps(reg), encoding="utf-8")
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    monkeypatch.setattr(index_portfolio, "CONFIG_PATH", net_env.cfg_path)
    db = tmp_path / "refs" / "portfolio.duckdb"
    db.parent.mkdir()

    def index():
        monkeypatch.setattr(sys, "argv", ["index_portfolio.py", "--project", "research_a", "--db", str(db)])
        index_portfolio.main()

    index()
    monkeypatch.setattr(ea, "CROSSREF", srv.url("/works/{doi}"))
    return srv, db, ds, index, lib


def test_abstracts_on_a_real_index_db_keep_the_view_and_never_drop(indexed):
    srv, db, ds, index, lib = indexed
    srv.script(CR + ds[0], Reply(200, json.dumps({"message": {"abstract":
               "<jats:p>&lt;b&gt;Purpose&lt;/b&gt; Heat &amp;agr; tolerance</jats:p>"}}), JSON_H))
    srv.script(CR + ds[1], Reply(404, "Resource not found."))
    srv.script(CR + ds[2], Reply(503, "down"))
    res = ea.run(db=str(db), commit_every=2)
    assert (res["hits"], res["errors_permanent"], res["errors_transient"]) == (1, 1, 1)
    con = duckdb.connect(str(db), read_only=True)
    try:
        got = dict(con.execute("SELECT doi, abstract FROM paper_metadata").fetchall())
        att = {r[0]: r[1:] for r in con.execute(
            "SELECT doi, outcome, status, attempted_at IS NOT NULL FROM abstract_attempts").fetchall()}
        marked = dict(con.execute("SELECT doi, abstract_attempted_at IS NOT NULL FROM paper_metadata").fetchall())
        view = dict(con.execute("SELECT doi, abstract FROM papers").fetchall())
    finally:
        con.close()
    assert got[ds[0]] == "Purpose Heat α tolerance" and got[ds[1]] is None and got[ds[2]] is None
    assert view[ds[0]] == got[ds[0]]
    assert att[ds[0]][:2] == ("OK", "200") and att[ds[1]][0] == "NO_MATCH" and att[ds[2]][0] == "OUTAGE"
    assert marked == {ds[0]: True, ds[1]: True, ds[2]: False}         # transient: retried next run
    # a re-index (the real code) keeps the abstract and the attempts table; the view still reads
    index()
    con = duckdb.connect(str(db), read_only=True)
    try:
        assert dict(con.execute("SELECT doi, abstract FROM papers").fetchall())[ds[0]] == got[ds[0]]
        assert con.execute("SELECT COUNT(*) FROM abstract_attempts").fetchone()[0] == 3
    finally:
        con.close()
    # an existing abstract is never a target again, and never overwritten
    srv.script(CR + ds[0], Reply(200, json.dumps({"message": {"abstract": "OVERWRITTEN"}}), JSON_H))
    ea.run(db=str(db), retry_after_days=0)
    assert not [h for h in srv.hits if h.path == CR + ds[0]][1:]
    blobs = all_bytes(db.parent)
    assert all(EMAIL.encode() not in b for b in blobs.values())


def test_duckdb_commit_after_a_failed_statement_silently_rolls_back_so_the_writer_must_not(tmp_path, monkeypatch):
    """W2-E2's premise, probed: commit() after a failed statement returns normally and keeps nothing.
    The writer never reaches commit() after a failure (the exception leaves the batch first), and
    its row-by-row rewrite keeps every good row of a batch a real constraint broke."""
    p = str(tmp_path / "probe.duckdb")
    con = duckdb.connect(p)
    con.execute("CREATE TABLE t (k VARCHAR PRIMARY KEY, v VARCHAR CHECK (v IS NULL OR v <> 'BAD'))")
    con.executemany("INSERT INTO t VALUES (?, NULL)", [("a",), ("b",)])
    con.begin()
    con.execute("UPDATE t SET v = 'good' WHERE k = 'a'")
    with pytest.raises(duckdb.ConstraintException):
        con.execute("UPDATE t SET v = 'BAD' WHERE k = 'b'")
    con.commit()                                                        # no exception...
    assert con.execute("SELECT v FROM t WHERE k = 'a'").fetchone()[0] is None   # ...and nothing kept
    con.close()
    # the real writer on a real constraint failure (no monkeypatched _write)
    db = str(tmp_path / "t.duckdb")
    con = duckdb.connect(db)
    con.execute("CREATE TABLE paper_metadata (doi VARCHAR PRIMARY KEY, "
                "abstract VARCHAR CHECK (abstract IS NULL OR abstract <> 'POISON'))")
    con.executemany("INSERT INTO paper_metadata VALUES (?, NULL)", [(f"10.1/{i}",) for i in range(5)])
    con.close()
    answers = {f"10.1/{i}": f"text {i}" for i in range(5)}
    answers["10.1/2"] = "POISON"
    monkeypatch.setattr(ea, "crossref_abstract", lambda doi, timeout=15: answers[doi])
    res = ea.run(db=db, commit_every=5)
    con = duckdb.connect(db, read_only=True)
    got = dict(con.execute("SELECT doi, abstract FROM paper_metadata").fetchall())
    n_att = con.execute("SELECT COUNT(*) FROM abstract_attempts").fetchone()[0]
    con.close()
    assert got == {"10.1/0": "text 0", "10.1/1": "text 1", "10.1/2": None, "10.1/3": "text 3", "10.1/4": "text 4"}
    assert [d for d, _ in res["write_failures"]] == ["10.1/2"] and n_att == 4


# ================================================================ W2-E2 / W2-F seam
def _pdf(p, n=12_000):
    p.write_bytes(b"%PDF-1.4\n" + b"0" * n)


@pytest.fixture
def lib_world(net_env, tmp_path, monkeypatch):
    """A registered library: one PDF holding (pdf, .ris, sidecar), a W2-A2 text-only sidecar, a
    legacy (no has_pdf key) text-only sidecar and a FLAG text-only sidecar. Real litpipe.state in a
    temp state_dir that does not exist yet. Since W5 backfill_ris asks the registration agency for a
    text-only holding too: no source holds these DOIs, so each falls back to its sidecar."""
    import ris_emit
    monkeypatch.setattr(ris_emit, "resolve_meta", lambda doi: ({}, "none"))
    root = tmp_path / "root"
    lib = root / "research_a" / "literature"
    lib.mkdir(parents=True)
    reg = {"state_dir": str(tmp_path / "state"), "projects": {"research_a": {"lib_dir": "literature", "tier": 2}}}
    net_env.cfg_path.write_text(json.dumps(reg), encoding="utf-8")
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    for mod in (audit_portfolio, pipeline_check):
        monkeypatch.setattr(mod, "CONFIG_PATH", net_env.cfg_path)
    monkeypatch.setattr(net, "STATE", None)
    monkeypatch.setattr(state, "DB_PATH", None)
    _pdf(lib / "2020_Alpha_HeatPaper.pdf")
    (lib / "2020_Alpha_HeatPaper.ris").write_text(
        "TY  - JOUR\nAU  - Alpha, Ann\nTI  - Heat paper\nPY  - 2020\nDO  - 10.5555/seam.0001\nER  - \n",
        encoding="utf-8")
    (lib / "2020_Alpha_HeatPaper.fulltext.json").write_text(json.dumps(
        {"doi": "10.5555/seam.0001", "text": "body", "extracted_from_pdf": True, "has_pdf": True}), encoding="utf-8")
    (lib / "2021_Beta_TextOnly.fulltext.json").write_text(json.dumps(
        {"doi": "10.5555/seam.0002", "title": "Text only holding", "authors": ["Beta Bo", "Gamma Cy"],
         "year": 2021, "journal": "J Seam", "volume": "3", "issue": "2", "pages": "10-20",
         "abstract": "Abstract &lt;b&gt;Purpose&lt;/b&gt; seam test", "text": "full text body",
         "has_pdf": False, "source": "pmc_jats"}), encoding="utf-8")
    (lib / "2019_Gamma_Legacy.fulltext.json").write_text(json.dumps(
        {"doi": "10.5555/seam.0003", "title": "Legacy jats record", "text": "legacy body"}), encoding="utf-8")
    (lib / "2018_Delta_Flagged.fulltext.json").write_text(json.dumps(
        {"doi": "10.5555/seam.0004", "title": "Wrong paper", "text": "x", "identity": "FLAG",
         "has_pdf": False}), encoding="utf-8")
    return root, lib, tmp_path / "state"


def test_instruments_on_a_fresh_world_create_nothing(lib_world, tmp_path, capsys):
    root, lib, sdir = lib_world
    before = tree(tmp_path)
    out = tmp_path / "audit.json"
    pipeline_check.main(["--project", "research_a"])
    audit_portfolio.main(["--project", "research_a", "--json", str(out)])
    assert tree(tmp_path) - before == {str(out)}                      # only the --json file
    assert not sdir.exists()                                            # the state dir is not created


def test_backfill_text_only_ris_is_owned_by_its_holding_in_both_instruments(lib_world, tmp_path, capsys):
    root, lib, sdir = lib_world
    assert backfill_ris.main(["--lib-dir", str(lib), "--commit", "--include-text-only"]) == 0
    assert sorted(p.name for p in lib.glob("*.ris")) == [
        "2019_Gamma_Legacy.ris", "2020_Alpha_HeatPaper.ris", "2021_Beta_TextOnly.ris"]   # none for the FLAG
    capsys.readouterr()
    pc = pipeline_check.run(project="research_a")
    ap = audit_portfolio.run(project="research_a", json_path=str(tmp_path / "a.json"))
    text = capsys.readouterr().out
    scan = ap["projects"][0]["audit"]
    assert scan["orphan_ris"] == [] and scan["orphan_sidecars"] == []
    assert sorted(scan["text_only"]) == ["2019_Gamma_Legacy.fulltext.json", "2021_Beta_TextOnly.fulltext.json"]
    assert scan["text_only_with_ris"] == 2 and len(scan["flags"]) == 1
    assert pc["exit_code"] == 0 and ap["exit_code"] == 0, pc["issues"]
    assert not any("orphan" in i.lower() for i in pc["issues"])
    assert "[FAIL]" not in text or "orphan" not in text.split("[FAIL]", 1)[1].split("\n", 1)[0].lower()


# lock for APPLY F-2: a sidecar extracted from a PDF that is no longer beside it is an orphan in the
# instruments (audit_portfolio.is_text_only_sidecar), so backfill_ris must not give it a .ris (which
# pipeline_check then FAILs as an orphan .ris).
# lock for APPLY F-2 (fixed in the dispatcher commit after 61eb10b)
@pytest.mark.parametrize("extra", [{"has_pdf": True, "extracted_from_pdf": True}, {"extracted_from_pdf": True}])
def test_backfill_text_only_agrees_with_the_instruments_on_orphan_sidecars(lib_world, tmp_path, capsys, extra):
    root, lib, sdir = lib_world
    (lib / "2017_Eps_Renamed.fulltext.json").write_text(json.dumps(
        {"doi": "10.5555/seam.0005", "title": "Renamed PDF's text", "text": "pdf text", **extra}), encoding="utf-8")
    assert not audit_portfolio.is_text_only_sidecar(json.loads((lib / "2017_Eps_Renamed.fulltext.json").read_text()))
    backfill_ris.main(["--lib-dir", str(lib), "--commit", "--include-text-only"])
    assert not (lib / "2017_Eps_Renamed.ris").exists()
    pc = pipeline_check.run(project="research_a")
    assert not any(".ris" in i and "orphan" in i.lower() for i in pc["issues"])


# lock for APPLY F-3: the .ris AB line of a text-only holding gets the abstract cleaner (dispatcher
# ruling a902abf: abstracts take litpipe.text.abstract_field), like every ris_emit abstract site.
# lock for APPLY F-3 (fixed in the dispatcher commit after 61eb10b)
def test_text_only_ris_abstract_uses_the_abstract_cleaner(lib_world):
    root, lib, sdir = lib_world
    backfill_ris.main(["--lib-dir", str(lib), "--commit", "--include-text-only"])
    ab = [ln for ln in (lib / "2021_Beta_TextOnly.ris").read_text(encoding="utf-8").splitlines() if ln.startswith("AB  -")]
    assert ab == ["AB  - Purpose seam test"]


@pytest.mark.parametrize("shape, holding", [
    ({"has_pdf": False}, True),
    ({}, True),                                                        # legacy JATS: no key
    ({"has_pdf": False, "identity": "FLAG"}, False),
])
def test_text_only_predicates_agree_where_they_should(tmp_path, monkeypatch, shape, holding):
    """holdings (W1-D2), audit_portfolio/pipeline_check (W2-F) and backfill_ris (W2-E2) on the shapes
    where all three agree. The orphan shapes (has_pdf true / extracted_from_pdf true, PDF gone) are
    the F-2 lock above and a forward for litpipe.holdings."""
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", tmp_path)
    lib = tmp_path / "p" / "lib"
    lib.mkdir(parents=True)
    sc = {"doi": "10.5555/pred.1", "title": "T", "text": "body", **shape}
    (lib / "2020_X_Pred.fulltext.json").write_text(json.dumps(sc), encoding="utf-8")
    reg = {"projects": {"p": {"lib_dir": "lib"}}}
    hm = holdings.build(reg, use_cache=False, write_cache=False)
    in_holdings = hm.text_only("10.5555/pred.1")
    in_audit = audit_portfolio.is_text_only_sidecar(sc)
    in_backfill = (backfill_ris.text_only_sidecars(lib) != []
                   and not backfill_ris._flagged_record(sc))
    assert (in_holdings, in_audit, in_backfill) == (holding, holding, holding)


# ================================================================ CI parity: --help from a foreign cwd
@pytest.mark.parametrize("script", ["forward_citations.py", "snowball.py", "enrich_abstracts.py",
                                    "backfill_ris.py", "audit_portfolio.py", "pipeline_check.py"])
def test_help_exits_0_from_a_foreign_cwd(script, tmp_path):
    proc = subprocess.run([sys.executable, str(REPO / script), "--help"], cwd=tmp_path,
                          capture_output=True, text=True, encoding="utf-8", timeout=120)
    assert proc.returncode == 0, proc.stderr[-400:]
    assert "usage" in proc.stdout.lower()
