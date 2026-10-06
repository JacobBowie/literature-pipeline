"""litpipe.canaries local phase (W4-B): 0-request checks over what a run wrote. Temp projects,
temp ledger (conftest), temp DuckDB; nothing here resolves the real registry, state or DB."""
import csv
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from litpipe import canaries, ledger, net
from litpipe.outcomes import Kind

RUN = "20261006T120000Z-runner-4242-abc123"
SID = "2026-10-06"


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def boom(*a, **k):
        raise AssertionError("a local check sent a request")
    monkeypatch.setattr(net, "request", boom)


class Proj:
    def __init__(self, tmp_path, key="teaching_x", sources=("unpaywall", "pmc"), stages=("unpaywall", "pmc")):
        self.key = key
        self.root = tmp_path / key
        self.lib = self.root / "lib"
        self.art = self.root / "runs"
        for d in (self.lib, self.art):
            d.mkdir(parents=True, exist_ok=True)
        self.sources, self.stages = list(sources), list(stages)
        self.now = datetime.now(timezone.utc)
        self.since = self.now - timedelta(hours=1)
        self.db = tmp_path / "portfolio.duckdb"

    def context(self, **over):
        p = {"key": self.key, "root": str(self.root), "lib_dir": "lib", "sources": self.sources,
             "artifact_dir": "runs", "sweep_run_ids": [SID], "stages": self.stages}
        p.update(over)
        return {"run_id": RUN, "since": self.since.isoformat().replace("+00:00", "Z"),
                "db_path": str(self.db), "projects": [p]}

    def report(self, stage, fields, rows, tag=None, where=None):
        name = f"lit_pull_queue{'.' + tag if tag else ''}.{SID}.{stage}.csv"
        path = (where or self.art) / name
        with open(path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            w.writeheader()
            for r in rows:
                w.writerow(r)
        return path

    def run(self, **over):
        return canaries.run("every_run", phase="local", context=self.context(**over), now=self.now)


def by(outs, cid, host=None):
    return [o for o in outs if o.detail == cid and (host is None or o.host == host)]


def one(outs, cid, host=None):
    (o,) = by(outs, cid, host)
    return o


def pmc_rows(n, downloaded):
    return [{"doi": f"10.1000/x{i}", "pmcid": f"PMC{i + 1}", "downloaded": i < downloaded, "skipped": False,
             "route": "s3", "first_status": "200"} for i in range(n)]


def pmc_fields():
    import pmc_fetch
    return pmc_fetch.REPORT_FIELDS


def upw_fields():
    import unpaywall_fetch_v2
    return unpaywall_fetch_v2.REPORT_FIELDS


def old(path, since):
    t = (since - timedelta(days=1)).timestamp()
    os.utime(path, (t, t))


# ------------------------------------------------------------------------------ stage yields
@pytest.mark.parametrize("n,dl,alarm", [(25, 7, True),     # 0.28 on 25: alarm
                                        (25, 8, True),     # 0.32 (0.3) on 25: alarm
                                        (25, 15, False),   # 0.6 on 25
                                        (15, 4, False),    # 0.27 on 15: under 20 rows, no floor
                                        (20, 6, True),     # 0.30 on exactly 20
                                        (20, 10, False)])  # 0.50 is the floor itself
def test_pmc_yield_floor(tmp_path, n, dl, alarm):
    pr = Proj(tmp_path)
    pr.report("pmc", pmc_fields(), pmc_rows(n, dl))
    o = one(pr.run(), "yield_pmc")
    assert o.payload["status"] == (canaries.ALARM if alarm else canaries.PASS), o.payload
    assert f"{dl}/{n}" in o.payload["observed"]


def test_pmc_yield_ignores_rows_without_pmcid_and_skipped_rows(tmp_path):
    pr = Proj(tmp_path)
    rows = pmc_rows(25, 25)
    rows += [{"doi": f"10.1000/n{i}", "pmcid": "", "downloaded": False} for i in range(40)]
    rows += [{"doi": f"10.1000/s{i}", "pmcid": f"PMC9{i}", "downloaded": False, "skipped": True} for i in range(40)]
    pr.report("pmc", pmc_fields(), rows)
    o = one(pr.run(), "yield_pmc")
    assert o.payload["status"] == canaries.PASS and "25/25" in o.payload["observed"]


def test_reports_found_through_sweep_run_ids_in_the_artifact_dir_any_tag(tmp_path):
    pr = Proj(tmp_path)
    pr.report("pmc", pmc_fields(), pmc_rows(12, 2), tag="retry")
    pr.report("pmc", pmc_fields(), pmc_rows(13, 2))
    # another run's report (a different sweep run id) is not this run's
    with open(pr.art / "lit_pull_queue.2026-10-05.pmc.csv", "w", encoding="utf-8", newline="") as f:
        csv.writer(f).writerows([pmc_fields()] + [["10.1/y", "", "PMC5", "True"]] * 50)
    o = one(pr.run(), "yield_pmc")
    assert o.payload["status"] == canaries.ALARM and "4/25" in o.payload["observed"]


def test_epmc_403_in_the_pmc_report_alarms(tmp_path):
    pr = Proj(tmp_path)
    rows = pmc_rows(3, 3) + [{"doi": "10.1/e", "pmcid": "PMC77", "downloaded": False,
                              "route": "epmc_fulltextxml", "first_status": "403", "outcome": "REFUSED"}]
    pr.report("pmc", pmc_fields(), rows)
    assert one(pr.run(), "epmc_403").payload["status"] == canaries.ALARM


def test_legacy_europepmc_403_token_alarms(tmp_path):
    pr = Proj(tmp_path)
    pr.report("pmc", pmc_fields(), [{"doi": "10.1/e", "pmcid": "PMC7", "attempts": "europepmc/HTTP_403 | s3/OK"}])
    assert one(pr.run(), "epmc_403").payload["status"] == canaries.ALARM


def test_no_epmc_403_passes(tmp_path):
    pr = Proj(tmp_path)
    pr.report("pmc", pmc_fields(), pmc_rows(3, 1))
    assert one(pr.run(), "epmc_403").payload["status"] == canaries.PASS


def _upw_rows(n403, nok, refused=0):
    toks = ["publisher/publishedVersion/HTTP_403"] * n403 + ["repository/acceptedVersion/OK"] * nok \
        + ["publisher/publishedVersion/HOST_REFUSED"] * refused
    return [{"doi": f"10.1000/u{i}", "oa_status": "OA", "attempts": t, "downloaded": t.endswith("OK")}
            for i, t in enumerate(toks)]


@pytest.mark.parametrize("n403,nok,refused,alarm", [(12, 13, 0, True),     # 0.48 of 25
                                                    (10, 15, 0, False),    # 0.40
                                                    (12, 13, 30, True),    # HOST_REFUSED was not sent:
                                                                           # still 12/25, not 12/55
                                                    (9, 9, 0, False)])     # 0.5 of 18: under 20 attempts
def test_unpaywall_403_share(tmp_path, n403, nok, refused, alarm):
    pr = Proj(tmp_path)
    pr.report("unpaywall", upw_fields(), _upw_rows(n403, nok, refused))
    o = one(pr.run(), "unpaywall_403")
    assert o.payload["status"] == (canaries.ALARM if alarm else canaries.PASS), o.payload


@pytest.mark.parametrize("n,dl,alarm", [(10, 5, True), (10, 6, False), (9, 0, False)])
def test_mdpi_bmc_share(tmp_path, n, dl, alarm):
    pr = Proj(tmp_path)
    rows = [{"doi": f"10.{'3390' if i % 2 else '1186'}/m{i}", "oa_status": "OA", "downloaded": i < dl}
            for i in range(n)]
    rows += [{"doi": f"10.3390/c{i}", "oa_status": "CLOSED", "downloaded": False} for i in range(20)]
    rows += [{"doi": f"10.1016/o{i}", "oa_status": "OA", "downloaded": False} for i in range(20)]
    pr.report("unpaywall", upw_fields(), rows)
    o = one(pr.run(), "yield_mdpi_bmc")
    assert o.payload["status"] == (canaries.ALARM if alarm else canaries.PASS), o.payload


def test_preprint_check_only_for_projects_with_a_preprint_source(tmp_path):
    import preprint_fetch
    bio = Proj(tmp_path)
    bio.report("preprint", preprint_fetch.REPORT_FIELDS, [{"doi": "10.48550/arxiv.1", "outcome": "REFUSED"}])
    assert by(bio.run(), "preprint_outcomes") == []
    ppr = Proj(tmp_path, key="research_x", sources=("unpaywall", "osf"))
    ppr.report("preprint", preprint_fetch.REPORT_FIELDS, [{"doi": "10.31234/a", "outcome": "OK"},
                                                          {"doi": "10.31234/b", "outcome": "NO_MATCH"}])
    o = one(ppr.run(), "preprint_outcomes")
    assert o.payload["status"] == canaries.PASS and "OK 1" in o.payload["observed"]


def test_a_source_the_project_does_not_use_gets_no_check(tmp_path):
    pr = Proj(tmp_path, sources=("unpaywall",))
    pr.report("pmc", pmc_fields(), pmc_rows(25, 0))
    outs = pr.run()
    assert by(outs, "yield_pmc") == [] and by(outs, "epmc_403") == []


def test_missing_report_is_skipped(tmp_path):
    pr = Proj(tmp_path)
    o = one(pr.run(), "yield_pmc")
    assert o.payload["status"] == canaries.SKIPPED and o.kind is Kind.SKIPPED


# ------------------------------------------------------------------------------ lost artifacts
def test_lost_artifact_is_reported(tmp_path):
    pr = Proj(tmp_path, stages=("unpaywall", "pmc", "residual"))
    pr.report("unpaywall", upw_fields(), [])
    pr.report("pmc", pmc_fields(), [], where=pr.root)        # the project root counts too
    o = one(pr.run(), "lost_artifacts")
    assert o.payload["status"] == canaries.ALARM
    assert f"{SID}.residual" in o.payload["observed"] and "pmc" not in o.payload["observed"]


def test_all_artifacts_present_passes(tmp_path):
    pr = Proj(tmp_path)
    pr.report("unpaywall", upw_fields(), [], tag="retry")
    pr.report("pmc", pmc_fields(), [])
    assert one(pr.run(), "lost_artifacts").payload["status"] == canaries.PASS


# ------------------------------------------------------------------------------ email grep
def test_email_grep_finds_planted_values_and_reports_no_value(tmp_path, monkeypatch):
    addr = "owner@litpipe-test.org"
    monkeypatch.setenv("LITPIPE_EMAIL", addr)
    pr = Proj(tmp_path)
    p = pr.report("unpaywall", ["doi", "error"], [
        {"doi": "10.1/a", "error": "ok"},
        {"doi": "10.1/b", "error": "HTTP_422 https://api.example/v2/x?email=someone%40host.test"},
        {"doi": "10.1/c", "error": f"contact {addr} failed"},
    ])
    older = pr.report("pmc", ["doi", "error"], [{"doi": "10.1/d", "error": "mailto:legacy@host.test"}])
    old(older, pr.since)
    outs = pr.run()
    o = one(outs, "email", pr.key)
    assert o.payload["status"] == canaries.ALARM
    obs = o.payload["observed"]
    assert f"{p.name}:3" in obs and f"{p.name}:4" in obs and older.name not in obs
    rep = json.dumps(canaries.report(outs))
    for leak in (addr, "someone", "legacy@", "email=", "mailto:"):
        assert leak not in rep and leak not in obs


def test_email_grep_reads_the_runs_ledger_lines(tmp_path):
    pr = Proj(tmp_path)
    d = Path(ledger.LEDGER_DIR)
    d.mkdir(parents=True, exist_ok=True)
    rec_ok = {"ts": pr.now.isoformat().replace("+00:00", "Z"), "run_id": RUN, "host": "h", "url": "https://h/x",
              "attempt": 1, "hop": 0, "status": 200, "kind": "OK", "decision": "final", "purpose": "p"}
    leak = dict(rec_ok, url="https://h/x?email=x%40y.test")
    other_run = dict(leak, run_id="another-run")
    path = d / f"{pr.now:%Y-%m-%d}.jsonl"
    path.write_text("".join(json.dumps(r) + "\n" for r in (rec_ok, leak, other_run)), encoding="utf-8")
    o = one(pr.run(), "email", "ledger")
    assert o.payload["status"] == canaries.ALARM and f"{path.name}:2" in o.payload["observed"]
    assert f"{path.name}:3" not in o.payload["observed"]


def test_clean_reports_pass_the_email_grep(tmp_path):
    pr = Proj(tmp_path)
    pr.report("unpaywall", ["doi", "error"], [{"doi": "10.1/a", "error": "[EMAIL-REDACTED] [MAILTO-REDACTED]"}])
    assert one(pr.run(), "email", pr.key).payload["status"] == canaries.PASS


# ------------------------------------------------------------------------------ first attempts
def _ledger(pr, recs):
    d = Path(ledger.LEDGER_DIR)
    d.mkdir(parents=True, exist_ok=True)
    base = {"ts": pr.now.isoformat().replace("+00:00", "Z"), "run_id": RUN, "hop": 0, "purpose": "stage"}
    (d / f"{pr.now:%Y-%m-%d}.jsonl").write_text("".join(json.dumps({**base, **r}) + "\n" for r in recs),
                                                encoding="utf-8")


def test_not_sent_and_canary_lines_do_not_alarm(tmp_path):
    pr = Proj(tmp_path)
    recs = [{"host": "export.arxiv.org", "attempt": 0, "decision": "not_sent", "kind": "REFUSED"}] * 30
    recs += [{"host": "export.arxiv.org", "attempt": 1, "decision": "final", "kind": "REFUSED", "status": 406,
              "purpose": "canary:arxiv"}] * 25
    recs += [{"host": "api.crossref.org", "attempt": 1, "decision": "final", "kind": "OK", "status": 200}] * 25
    _ledger(pr, recs)
    o = one(pr.run(), "first_attempts")
    assert o.payload["status"] == canaries.PASS, o.payload
    assert set(o.payload["observed"]["hosts"]) == {"api.crossref.org"}


def test_refused_share_alarms_on_a_busy_host(tmp_path):
    pr = Proj(tmp_path)
    recs = [{"host": "pmc-oa-opendata.s3.amazonaws.com", "attempt": 1, "decision": "final", "kind": "OK",
             "status": 200}] * 19
    recs += [{"host": "pmc-oa-opendata.s3.amazonaws.com", "attempt": 1, "decision": "final", "kind": "REFUSED",
              "status": 403}]
    recs += [{"host": "pmc-oa-opendata.s3.amazonaws.com", "attempt": 2, "decision": "final", "kind": "OK",
              "status": 200}] * 50                       # retries are not first attempts
    recs += [{"host": "few.example", "attempt": 1, "decision": "final", "kind": "REFUSED", "status": 403}] * 5
    _ledger(pr, recs)
    o = one(pr.run(), "first_attempts")
    assert o.payload["status"] == canaries.ALARM
    assert "pmc-oa-opendata.s3.amazonaws.com REFUSED 1/20" in o.payload["observed"]["summary"]
    assert "few.example" not in o.payload["observed"]["summary"]    # under 20 first attempts


def test_a_blocked_redirect_is_not_a_refusal(tmp_path):
    pr = Proj(tmp_path)
    recs = [{"host": "doi.org", "attempt": 1, "decision": "redirect_blocked", "kind": "REFUSED", "status": 302}] * 25
    _ledger(pr, recs)
    assert one(pr.run(), "first_attempts").payload["status"] == canaries.PASS


def test_any_europe_pmc_403_first_attempt_alarms(tmp_path):
    pr = Proj(tmp_path)
    _ledger(pr, [{"host": "www.ebi.ac.uk", "attempt": 1, "decision": "final", "kind": "REFUSED", "status": 403}])
    o = one(pr.run(), "first_attempts")
    assert o.payload["status"] == canaries.ALARM and "www.ebi.ac.uk 403" in o.payload["observed"]["summary"]


def test_other_runs_lines_are_ignored(tmp_path):
    pr = Proj(tmp_path)
    _ledger(pr, [{"host": "www.ebi.ac.uk", "attempt": 1, "decision": "final", "kind": "REFUSED", "status": 403,
                  "run_id": "someone-else"}])
    assert one(pr.run(), "first_attempts").payload["status"] == canaries.PASS


# ------------------------------------------------------------------------------ library checks
def test_mismatch_growth(tmp_path):
    pr = Proj(tmp_path)
    mm = pr.lib / "_mismatch"
    mm.mkdir()
    old_f = mm / "old.pdf"
    old_f.write_bytes(b"%PDF")
    old(old_f, pr.since)
    assert one(pr.run(), "mismatch_growth").payload["status"] == canaries.PASS
    (mm / "sub").mkdir()
    (mm / "sub" / "new.pdf").write_bytes(b"%PDF")
    o = one(pr.run(), "mismatch_growth")
    assert o.payload["status"] == canaries.ALARM and "new.pdf" in o.payload["observed"]


def test_markup_and_entities_in_new_files(tmp_path):
    pr = Proj(tmp_path)
    (pr.lib / "a.ris").write_text("TY  - JOUR\nTI  - Heat &amp; exercise\nDO  - 10.1/a\nER  - \n", encoding="utf-8")
    (pr.lib / "b.ris").write_text("TY  - JOUR\nTI  - Clean title p < 0.05\nDO  - 10.1002/(sici)1;2-<385::x>\nER  - \n",
                                  encoding="utf-8")
    (pr.lib / "c.fulltext.json").write_text(json.dumps({"title": "<i>In vivo</i> heat", "text": "&amp;"}),
                                            encoding="utf-8")
    (pr.lib / "d.fulltext.json").write_text(json.dumps({"title": "Clean", "authors": ["Mündel T"],
                                                        "text": "body &amp; is not read"}), encoding="utf-8")
    stale = pr.lib / "e.ris"
    stale.write_text("TI  - Old &lt;b&gt;\n", encoding="utf-8")
    old(stale, pr.since)
    o = one(pr.run(), "markup")
    assert o.payload["status"] == canaries.ALARM
    ob = o.payload["observed"]
    assert ob["files_checked"] == 4 and ob["with_markup"] == 2 and ob["rate"] == 0.5
    assert "a.ris" in ob["summary"] and "c.fulltext.json" in ob["summary"] and "e.ris" not in ob["summary"]


def test_clean_new_files_pass_markup(tmp_path):
    pr = Proj(tmp_path)
    (pr.lib / "a.ris").write_text("TY  - JOUR\nTI  - Heat & exercise; p < 0.05\nER  - \n", encoding="utf-8")
    assert one(pr.run(), "markup").payload["status"] == canaries.PASS


# ------------------------------------------------------------------------------ DOI fixtures
def test_doi_fixtures_pass_as_built(tmp_path):
    o = one(Proj(tmp_path).run(), "doi_fixtures")
    assert o.payload["status"] == canaries.PASS


def test_doi_fixture_mismatch_alarms(tmp_path, monkeypatch):
    monkeypatch.setattr(canaries, "DOI_FIXTURES", canaries.DOI_FIXTURES + (("10.1/abc", "normalise", "10.1/xyz"),))
    o = one(Proj(tmp_path).run(), "doi_fixtures")
    assert o.payload["status"] == canaries.ALARM and "10.1/xyz" in o.payload["observed"]


def test_the_sici_hash_fixture_is_none():
    from litpipe import doi as _doi
    assert (canaries.SICI_HASH_DOI, "normalise", None) in canaries.DOI_FIXTURES
    assert _doi.normalise(canaries.SICI_HASH_DOI) is None


# ------------------------------------------------------------------------------ index freshness
def _db(path, rows):
    import duckdb
    con = duckdb.connect(str(path))
    con.execute("SET TimeZone = 'UTC'")
    con.execute("CREATE TABLE index_runs (project VARCHAR, finished_at TIMESTAMPTZ, n_files INTEGER, db_path VARCHAR)")
    for key, when in rows:
        con.execute("INSERT INTO index_runs VALUES (?, ?, 1, ?)", [key, when, str(path)])
    con.close()


def test_index_stale_by_more_than_a_day_alarms_and_the_db_is_closed(tmp_path, monkeypatch):
    import duckdb
    import lit_util
    pr = Proj(tmp_path)
    (pr.lib / "x.pdf").write_bytes(b"%PDF")
    _db(pr.db, [(pr.key, pr.now - timedelta(days=3)), (pr.key, pr.now - timedelta(days=2))])
    opened = []
    real = lit_util.connect_db

    class Spy:
        def __init__(self, con):
            self.con, self.closed = con, False

        def execute(self, *a, **k):
            return self.con.execute(*a, **k)

        def close(self):
            self.closed = True
            self.con.close()

    def spy_connect(path, **kw):
        assert kw.get("read_only") is True and kw.get("tries") == 1
        opened.append(Spy(real(path, **kw)))
        return opened[-1]
    monkeypatch.setattr(lit_util, "connect_db", spy_connect)
    o = one(pr.run(), "index_freshness")
    assert o.payload["status"] == canaries.ALARM and "h newer" in o.payload["observed"]
    assert len(opened) == 1 and opened[0].closed      # closed before returning (read-only, tries=1)
    duckdb.connect(str(pr.db)).close()                # a writer can open it


def test_fresh_index_passes(tmp_path):
    pr = Proj(tmp_path)
    (pr.lib / "x.pdf").write_bytes(b"%PDF")
    _db(pr.db, [(pr.key, pr.now - timedelta(hours=2))])
    assert one(pr.run(), "index_freshness").payload["status"] == canaries.PASS


def test_never_indexed_project_alarms(tmp_path):
    pr = Proj(tmp_path)
    (pr.lib / "x.fulltext.json").write_text("{}", encoding="utf-8")
    _db(pr.db, [("other", pr.now)])
    assert one(pr.run(), "index_freshness").payload["status"] == canaries.ALARM


def test_index_missing_db_or_table_or_lock_is_skipped(tmp_path):
    import duckdb
    pr = Proj(tmp_path)
    assert one(pr.run(), "index_freshness").payload["status"] == canaries.SKIPPED      # no DB
    con = duckdb.connect(str(pr.db))
    con.execute("CREATE TABLE other (x INTEGER)")
    con.close()
    o = one(pr.run(), "index_freshness")
    assert o.payload["status"] == canaries.SKIPPED and "index_runs" in o.payload["observed"]
    holder = duckdb.connect(str(pr.db))           # held read-write: a read-only open fails
    try:
        o = one(pr.run(), "index_freshness")
        assert o.payload["status"] == canaries.SKIPPED and "could not open" in o.payload["observed"]
    finally:
        holder.close()


# ------------------------------------------------------------------------------ the CLI default context
def test_without_context_the_registry_and_todays_artifacts_are_used(tmp_path, monkeypatch, net_env):
    import lit_util
    root = tmp_path / "root"
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    net_env.write_config(db_dir=str(tmp_path / "db"),
                         projects={"teaching_x": {"lib_dir": "lib", "active": True},
                                   "teaching_off": {"lib_dir": "lib", "active": False}})
    proj = root / "teaching_x"
    (proj / "lib").mkdir(parents=True)
    (proj / "runs").mkdir()
    today = datetime.now(timezone.utc)
    with open(proj / "runs" / f"lit_pull_queue.{today:%Y-%m-%d}.2.pmc.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=pmc_fields(), extrasaction="ignore")
        w.writeheader()
        for r in pmc_rows(25, 2):
            w.writerow(r)
    outs = canaries.run("every_run", phase="local", now=today)
    o = one(outs, "yield_pmc", "teaching_x")
    assert o.payload["status"] == canaries.ALARM and "2/25" in o.payload["observed"]
    assert one(outs, "lost_artifacts", "teaching_x").payload["status"] == canaries.SKIPPED
    assert not [x for x in outs if x.host == "teaching_off"]
    assert one(outs, "index_freshness").payload["status"] == canaries.SKIPPED      # temp db_dir, no DB


def test_local_checks_send_nothing_and_report_their_phase(tmp_path):
    pr = Proj(tmp_path)
    outs = pr.run()
    assert outs and all(o.payload["phase"] == "local" and o.attempts == 0 for o in outs)
    assert {o.payload["cadence"] for o in outs} == {"every_run"}
