"""W2-G: sweep and migrate route rows on the typed report columns (dispatch 0.5 residual table).

The fixtures (tests/fixtures/W2-G/) are three stage reports in the shapes the stages write after
W2a (unpaywall_fetch_v2.REPORT_FIELDS, pmc_fetch.REPORT_FIELDS) and the W2b preprint contract
(legacy columns plus outcome, first_status, route, detail, identity, release_date, landing_url):
the W2a verifier's 26 Unpaywall row shapes, and PMC and preprint rows for every Kind and special
case (FLAG with OK and with ERROR, SUPPLEMENT, EMBARGOED with and without a date, both SKIPPED
details, REFUSED on a download host and on a lookup, NOT_AT_RA, CONFIG). expected.csv names the
class of every row, read typed and read from the legacy columns alone.

The integration tests run the REAL sweep.run(...) with migrate=True on a temp root and a temp
registry: the stage subprocesses are answered from the fixture reports, the migrate subprocess by
the REAL migrate_closed_to_md.main(argv) in-process (the tests/test_w1_verify_b.py harness), and
the hold check is the REAL litpipe.holdings.build over temp libraries. No network, no subprocess.
"""
import contextlib
import csv
import dataclasses
import datetime
import io
import json
import types
from pathlib import Path

import pytest

import lit_util
import migrate_closed_to_md as mig
import pmc_fetch
import ris_emit
import sweep
import unpaywall_fetch_v2 as U
from litpipe import config
from litpipe.outcomes import Kind

FIX = Path(__file__).parent / "fixtures" / "W2-G"
DAY, DAY2, DAY3 = "2026-10-01", "2026-10-02", "2026-10-03"
LEAKS = ("email=", "mailto:", "fixture.user", "%40example", "@example")
LANDING = "https://www.biorxiv.org/content/10.1101/2020.06.01.000001v1"
PPR_LEGACY = ["doi", "title", "year", "preprint_filename", "found", "source", "match_id", "similarity",
              "downloaded", "skipped", "status"]
PPR_TYPED = PPR_LEGACY + ["first_status", "route", "outcome", "detail", "identity", "release_date",
                          "landing_url"]
UPW_LEGACY = U.REPORT_FIELDS[:U.REPORT_FIELDS.index("error") + 1]
FIELDS = {"unpaywall": (list(U.REPORT_FIELDS), UPW_LEGACY),
          "pmc": (list(pmc_fetch.REPORT_FIELDS), list(pmc_fetch.LEGACY_FIELDS)),
          "preprint": (PPR_TYPED, PPR_LEGACY)}
CONFIG_ROWS = ("config_email_unset", "upw_422")
HELD, UNAVAIL, RETRY_OK, NOTFOUND, INVALID = (
    "10.1000/w2g.held9", "10.1000/w2g.nometa_unavail9", "10.1000/w2g.nometa_retry_ok9",
    "10.1000/w2g.nometa_notfound9", "NO_DOI_doe2020")


def read_csv(path):
    with open(path, encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path, rows, fields):
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore", restval="", lineterminator="\n")
        w.writeheader()
        w.writerows(rows)


def legacy(row, stage):
    """The row as a W1-era report has it: the legacy columns only."""
    return {k: v for k, v in row.items() if k in FIELDS[stage][1]}


def fixture(stage):
    return {r["doi"]: r for r in read_csv(FIX / f"typed.{stage}.csv")}


EXPECTED = {r["name"]: r for r in read_csv(FIX / "expected.csv")}
VERDICT = {"unpaywall": sweep.unpaywall_verdict, "pmc": sweep.pmc_verdict,
           "preprint": sweep.preprint_verdict}


# ---------------------------------------------------------------- the harness
class _FrozenDate(datetime.date):
    frozen = None

    @classmethod
    def today(cls):
        return cls.frozen


DEFAULT = {   # every enabled stage found nothing: CLOSED, NO_PMCID, NO_MATCH (typed)
    "unpaywall": {"oa_status": "CLOSED", "downloaded": "False", "error": "", "route": "api",
                  "outcome": "NOT_AVAILABLE"},
    "pmc": {"downloaded": "False", "skipped": "False", "sidecar": "False",
            "error": "NO_PMCID: not in Europe PMC", "route": "europepmc_search", "outcome": "NO_MATCH"},
    "preprint": {"downloaded": "False", "skipped": "False", "found": "False", "status": "NO_MATCH",
                 "route": "europepmc", "outcome": "NO_MATCH"},
}


class Stages:
    """subprocess.run stand-in: each stage script writes its report from the W2-G fixture rows
    (typed, or the legacy columns only), the default row for any other DOI; migrate runs the REAL
    main() in-process. `exit[stage]` overrides a stage's exit code; `stop_after` makes Unpaywall
    stop after that DOI (its CONFIG abort: the rows after it are not in the report)."""

    def __init__(self, typed=True):
        self.typed = typed
        self.rows = {s: fixture(s) for s in FIELDS}
        self.calls, self.migrations, self.exit, self.stop_after = [], [], {}, None

    def _row(self, stage, d, base):
        r = dict(base)
        r.update(DEFAULT[stage])
        r.update(self.rows[stage].get(d, {}))
        r["doi"] = d
        return r if self.typed else legacy(r, stage)

    def _write(self, path, stage, rows):
        write_csv(path, rows, FIELDS[stage][0] if self.typed else FIELDS[stage][1])

    def __call__(self, cmd, **kw):
        cmd = [str(c) for c in cmd]
        script = next(Path(c).name for c in cmd if c.endswith(".py"))
        self.calls.append((script, cmd))
        arg = lambda flag: cmd[cmd.index(flag) + 1]  # noqa: E731
        if script == "migrate_closed_to_md.py":
            out, err = io.StringIO(), io.StringIO()
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                rc = mig.main(cmd[2:])
            self.migrations.append((cmd[2:], rc, out.getvalue(), err.getvalue()))
            return types.SimpleNamespace(returncode=rc, stdout=out.getvalue(), stderr=err.getvalue())
        rc = 0
        if script == "unpaywall_fetch_v2.py":
            rows = []
            for q in read_csv(arg("--triage")):
                d = q["doi"].strip().lower()
                rows.append(self._row("unpaywall", d, {"title": q.get("title", "")}))
                if d == self.stop_after:
                    rc = 2
                    break
            self._write(arg("--report"), "unpaywall", rows)
        elif script == "pmc_fetch.py":
            rows = [self._row("pmc", r["doi"].strip().lower(), {})
                    for r in read_csv(arg("--report-in"))
                    if r["downloaded"] != "True" and r["oa_status"] != "SKIP_EXISTS"]
            self._write(arg("--report-out"), "pmc", rows)
        elif script == "preprint_fetch.py":
            # read at call time: sweep reuses its triage path for the residual CSV afterwards
            self.preprint_triage = [r["doi"] for r in read_csv(arg("--triage"))]
            rows = [self._row("preprint", d.strip().lower(), {}) for d in self.preprint_triage]
            self._write(arg("--report"), "preprint", rows)
        stage = {"unpaywall_fetch_v2.py": "unpaywall", "pmc_fetch.py": "pmc",
                 "preprint_fetch.py": "preprint"}.get(script)
        rc = self.exit.get(stage, rc)
        return types.SimpleNamespace(returncode=rc, stdout="", stderr="")

    def cmd(self, script):
        return next(c for s, c in self.calls if s == script)


class World:
    def __init__(self, tmp_path, monkeypatch, sources=("unpaywall", "pmc", "osf", "biorxiv"), typed=True):
        self.tmp, self.root = tmp_path, tmp_path / "Projects"
        self.state = tmp_path / "state"
        self.cfg_path = tmp_path / "projects.json"
        p = {"lib_dir": "lit"}
        if sources is not None:
            p["sources"] = list(sources)
        self.cfg = {"root": str(self.root), "state_dir": str(self.state), "loose_ends": "Ops/LOOSE_ENDS.md",
                    "projects": {"P": p, "Q": {"lib_dir": "qlib"}}}
        self.save()
        self.proj, self.lib, self.qlib = self.root / "P", self.root / "P" / "lit", self.root / "Q" / "qlib"
        for d in (self.lib, self.qlib):
            d.mkdir(parents=True)
        for mod in (sweep, mig, config):
            monkeypatch.setattr(mod, "CONFIG_PATH", self.cfg_path)
        monkeypatch.setattr(lit_util, "PROJECTS_ROOT", self.root)
        monkeypatch.setenv("USERPROFILE", str(tmp_path / "home"))
        monkeypatch.setenv("HOME", str(tmp_path / "home"))
        self.meta_calls = []
        monkeypatch.setattr(sweep, "_resolve_meta", self.resolver)
        self.stages = Stages(typed=typed)
        monkeypatch.setattr(sweep.subprocess, "run", self.stages)
        monkeypatch.setattr(mig, "datetime", types.SimpleNamespace(date=_FrozenDate,
                                                                     timedelta=datetime.timedelta))
        _FrozenDate.frozen = datetime.date.fromisoformat(DAY)
        (self.qlib / "2019_Held_Paper.pdf").write_bytes(b"%PDF-1.4\n")
        (self.qlib / "2019_Held_Paper.ris").write_text(f"TY  - JOUR\nDO  - {HELD.upper()}\nER  - \n",
                                                       encoding="utf-8")

    def save(self):
        self.cfg_path.write_text(json.dumps(self.cfg), encoding="utf-8")

    def resolver(self, doi):
        self.meta_calls.append(doi)
        if doi == UNAVAIL or (doi == RETRY_OK and self.meta_calls.count(doi) == 1):
            raise ris_emit.MetadataUnavailable("crossref", detail="HTTP 503 ?email=fixture.user%40example.org")
        if doi == RETRY_OK:
            return {"title": "Recovered title", "year": 2020, "authors": [{"family": "Doe", "given": "J"}]}, "crossref"
        if doi == NOTFOUND:
            return {}, "none"
        raise AssertionError(f"unexpected metadata lookup for {doi}")

    def queue(self, dois, name="lit_pull_queue.csv", blank=()):
        rows = [{"doi": d, "title": "" if d in blank else f"A study of {d.rsplit('.', 1)[-1]}",
                 "authors": "" if d in blank else "Doe J", "year": "2020", "destination": "lit", "notes": ""}
                for d in dois]
        write_csv(self.proj / name, rows, list(sweep.QUEUE_COLUMNS))
        return self.proj / name

    def sweep(self, day=DAY, **kw):
        _FrozenDate.frozen = datetime.date.fromisoformat(day)
        with contextlib.redirect_stdout(io.StringIO()):
            return sweep.run(project="P", date=day, migrate=True, **kw)

    def residual(self, run_id=DAY, tag=""):
        return {r["doi"]: r for r in read_csv(self.proj / sweep.artifact_name(tag, run_id, "residual"))}

    def md(self, name):
        p = self.proj / name
        return p.read_text(encoding="utf-8") if p.exists() else ""

    def retry(self):
        return {r["doi"]: r for r in mig.read_retry_later(self.proj)[1]}

    def written_by_us(self):
        return [p for p in self.root.rglob("*") if p.is_file() and p.suffix in (".csv", ".md")
                and not p.name.endswith((".unpaywall.csv", ".pmc.csv", ".preprint.csv"))]


def main_queue():
    """Every fixture DOI but the two CONFIG rows (they abort the run), plus the rows settled
    before any fetch: held elsewhere, blank metadata (three ways), an invalid DOI."""
    return ([r["doi"] for n, r in EXPECTED.items() if n not in CONFIG_ROWS]
            + [HELD, UNAVAIL, RETRY_OK, NOTFOUND, INVALID])


SETTLED = {HELD: "HELD_ELSEWHERE", UNAVAIL: "NO_METADATA", NOTFOUND: "NO_METADATA", INVALID: "INVALID_DOI",
           RETRY_OK: "TERMINAL_CLOSED"}


def doi_of(name):
    return EXPECTED[name]["doi"]


# ---------------------------------------------------------------- the acceptance run
def test_the_fixture_reports_carry_every_kind_and_special_case():
    outcomes = {r["outcome"] for s in FIELDS for r in fixture(s).values()}
    assert outcomes == {k.value for k in Kind}
    ppr = list(fixture("preprint").values())
    assert {r["detail"] for r in ppr if r["outcome"] == "SKIPPED"} >= {"manual_preprint", "source_excluded:medrxiv"}
    pmc = list(fixture("pmc").values())
    assert any(r["outcome"] == "OK" and r["identity"] == "FLAG" for r in pmc)              # FLAG with OK
    assert any(r["outcome"] == "ERROR" and r["identity"] == "FLAG" for r in fixture("unpaywall").values())
    assert any(r["doc_kind"] == "SUPPLEMENT" for r in fixture("unpaywall").values())
    assert {bool(r["release_date"]) for r in pmc if r["outcome"] == "EMBARGOED"} == {True, False}
    for name in fixture("unpaywall"):    # the trailing digit keeps the shared normaliser's hands off
        assert sweep._dkey(name) == name


def test_synthetic_typed_run_routes_every_row_by_the_table(tmp_path, monkeypatch):
    w = World(tmp_path, monkeypatch)
    w.queue(main_queue(), blank=(UNAVAIL, RETRY_OK, NOTFOUND))
    res = w.sweep()
    assert res["exit_code"] == sweep.EXIT_OK
    p = res["projects"]["P"]
    assert p["results"][0]["retired"] is True
    args, rc, out, err = w.stages.migrations[-1]
    assert rc == 0, (out, err)

    # ---- sweep's classes: every row where expected.csv (the 0.5 table) puts it
    resid = w.residual()
    got = {d: resid[d]["residual_class"] if d in resid else "fetched" for d in main_queue()}
    want = {r["doi"]: r["class_typed"] for n, r in EXPECTED.items() if n not in CONFIG_ROWS}
    want.update(SETTLED)
    assert got == want

    # ---- the special cases' detail
    assert resid[UNAVAIL]["reason"] == "metadata unavailable"
    assert resid[NOTFOUND]["reason"] == "blank title; metadata not found"
    assert w.meta_calls.count(UNAVAIL) == 2 and w.meta_calls.count(RETRY_OK) == 2
    assert w.meta_calls.count(NOTFOUND) == 1                          # a not-found is not retried
    assert resid[HELD]["held_at"].endswith("2019_Held_Paper.pdf")
    dated, undated = resid[doi_of("pmc_embargo_dated")], resid[doi_of("pmc_embargo_undated")]
    assert dated["not_before"] == "2027-03-11" and "EMBARGOED until 2027-03-11" in dated["reason"]
    assert undated["not_before"] == "" and "release date unknown" in undated["reason"]
    for name, path in (("identity_flag", "2020_Doe_UpwFlag.pdf"), ("supplement", "2020_Doe_Supplement.pdf"),
                       ("pmc_flag_ok", "2020_Doe_PmcFlag.pdf"),
                       ("ppr_flag_supplement", "2020_Doe_Supp_preprint.pdf")):
        assert resid[doi_of(name)]["flagged_path"] == str(w.lib / path), name
    assert resid[doi_of("ppr_manual")]["landing_url"] == LANDING
    assert "preprint" in resid[doi_of("ppr_excluded")]["skipped_sources"].split(";")
    assert "outcome OK but no file" in resid[doi_of("typed_ok_no_file")]["reason"]
    assert resid[doi_of("ppr_arxiv_refused")]["reason"].startswith("preprint refused:")
    report = {(r["section"], r["name"]): r for r in read_csv(w.proj / f"lit_pull_queue.{DAY}.report.csv")}
    assert report[("metadata", "retried")]["count"] == "2"

    # ---- the preprint stage knows its project (W2-C selects servers from it)
    cmd = w.stages.cmd("preprint_fetch.py")
    assert cmd[cmd.index("--project") + 1] == "P"

    # ---- migrate: each class where it goes
    ill, oab, rev = (w.md(mig.ILL_NAME), w.md(mig.OA_BLOCKED_NAME), w.md(mig.REVIEW_NAME))
    by_class = {}
    for d, c in got.items():
        by_class.setdefault(c, set()).add(d)
    for d in main_queue():
        assert (d in ill) == (d in by_class["TERMINAL_CLOSED"]), d
        assert (f"[{d}]" in oab) == (d in by_class["OA_BLOCKED"]), d
        assert (f"DOI `{d}`" in rev) == (d in by_class["IDENTITY_FLAG"]), d
    assert f"[{doi_of('ppr_manual')}]({LANDING}) cause `manual_preprint` via `preprint:biorxiv`" in oab
    lines = {d: next(x for x in rev.splitlines() if f"DOI `{d}`" in x) for d in by_class["IDENTITY_FLAG"]}
    assert f"evidence `{w.lib / '2020_Doe_UpwFlag.identity.json'}`" in lines[doi_of("identity_flag")]
    assert f"file `{w.lib / '2020_Doe_PmcFlag.pdf'}`" in lines[doi_of("pmc_flag_ok")]
    assert (f"evidence `{w.lib / '2020_Doe_PmcFlag.fulltext.json'} (identity_* fields)`"
            in lines[doi_of("pmc_flag_ok")])
    assert f"evidence `{w.lib / '2020_Doe_Supp_preprint.identity.json'}`" in lines[doi_of("ppr_flag_supplement")]
    rl = w.retry()
    assert set(rl) == by_class["TRANSIENT"] | by_class["OA_BLOCKED"]
    for d, r in rl.items():
        want_nb = {doi_of("pmc_embargo_dated"): "2027-03-11", doi_of("pmc_embargo_undated"): "2026-10-08"}.get(
            d, "2026-10-04" if d in by_class["OA_BLOCKED"] else DAY2)
        assert r["not_before"] == want_nb, (d, r["not_before"])
    routing = {r["doi"]: r for r in read_csv(w.proj / f"lit_pull_queue.{DAY}.routing.csv")}
    assert routing[doi_of("ppr_manual")]["landing_url"] == LANDING
    assert routing[doi_of("pmc_flag_ok")]["flagged_path"] == str(w.lib / "2020_Doe_PmcFlag.pdf")

    # ---- nothing sweep or migrate wrote carries an email
    for f in w.written_by_us():
        text = f.read_text(encoding="utf-8")
        for leak in LEAKS:
            assert leak not in text, (f.name, leak)
    # ---- no request at all (so none to arXiv): the per-test ledger is empty
    from litpipe import ledger
    assert not [p for p in Path(ledger.LEDGER_DIR).rglob("*") if p.is_file()]


def test_the_same_run_with_legacy_only_reports_routes_as_before(tmp_path, monkeypatch):
    """W1-era reports (no typed columns): every row lands where the legacy strings say. The only
    row that differs from the typed run is the OA-class text-only sidecar: without pmc_class a
    legacy reader cannot tell it from an author manuscript (TEXT_ONLY, as before)."""
    w = World(tmp_path, monkeypatch, typed=False)
    w.queue(main_queue(), blank=(UNAVAIL, RETRY_OK, NOTFOUND))
    assert w.sweep()["exit_code"] == sweep.EXIT_OK
    header = read_csv(w.proj / f"lit_pull_queue.{DAY}.unpaywall.csv")[0].keys()
    assert "outcome" not in header
    resid = w.residual()
    got = {d: resid[d]["residual_class"] if d in resid else "fetched" for d in main_queue()}
    want = {r["doi"]: r["class_legacy"] for n, r in EXPECTED.items() if n not in CONFIG_ROWS}
    want.update(SETTLED)
    assert got == want
    diff = {r["name"] for r in EXPECTED.values() if r["class_legacy"] != r["class_typed"]}
    assert diff == {"pmc_oa_text"}
    assert resid[doi_of("pmc_embargo_dated")]["not_before"] == "2027-03-11"   # parsed from the legacy string
    assert resid[doi_of("pmc_flag_ok")]["flagged_path"] == str(w.lib / "2020_Doe_PmcFlag.pdf")
    assert resid[doi_of("ppr_manual")]["landing_url"] == ""
    assert f"[{doi_of('ppr_manual')}](https://doi.org/{doi_of('ppr_manual')})" in w.md(mig.OA_BLOCKED_NAME)


# ---------------------------------------------------------------- CONFIG aborts the run
def test_a_config_row_aborts_and_the_rows_after_it_are_pending(tmp_path, monkeypatch):
    """Unpaywall's CONFIG stops the stage (exit 2) after the CONFIG row: the rows after it have no
    verdict and are PENDING, never SKIPPED_SOURCE; the run exits 2, the queue stays, migrate routes
    nothing."""
    w = World(tmp_path, monkeypatch)
    first, cfg, after = doi_of("closed"), doi_of("config_email_unset"), [doi_of("dl_403"), doi_of("upw_404")]
    q = w.queue([first, cfg, *after])
    w.stages.stop_after = cfg
    res = w.sweep()
    assert res["exit_code"] == sweep.EXIT_USAGE
    assert q.exists()
    assert [s for s, _ in w.stages.calls] == ["unpaywall_fetch_v2.py"]
    resid = w.residual()
    assert {d: resid[d]["residual_class"] for d in after} == {d: "PENDING" for d in after}
    assert all("unpaywall config" in resid[d]["reason"] for d in after)
    assert "SKIPPED_SOURCE" not in {r["residual_class"] for r in resid.values()}
    assert w.stages.migrations == []                  # CONFIG aborts the run: nothing is routed
    assert res["projects"]["P"]["migrate"]            # the command is still recorded
    for name in (mig.ILL_NAME, mig.OA_BLOCKED_NAME, mig.REVIEW_NAME, mig.RETRY_LATER_NAME):
        assert not (w.proj / name).exists(), name


def test_a_config_row_from_a_completed_stage_aborts(tmp_path, monkeypatch):
    w = World(tmp_path, monkeypatch)
    q = w.queue([doi_of("upw_422"), doi_of("closed")])
    res = w.sweep()
    assert res["exit_code"] == sweep.EXIT_USAGE and q.exists()
    assert w.residual()[doi_of("upw_422")]["residual_class"] == "CONFIG"


def test_a_stage_exiting_2_aborts_even_without_a_config_row(tmp_path, monkeypatch):
    w = World(tmp_path, monkeypatch)
    q = w.queue([doi_of("closed"), doi_of("dl_403")])
    w.stages.exit["pmc"] = 2
    res = w.sweep()
    assert res["exit_code"] == sweep.EXIT_USAGE and q.exists()
    r = res["projects"]["P"]["results"][0]
    assert r["stages"]["pmc"] == "config" and r["config_error"] and r["stages"]["preprint"] == "not_run"
    assert {x["residual_class"] for x in w.residual().values()} == {"PENDING"}   # preprint never ran
    assert all("preprint not_run" in x["reason"] for x in w.residual().values())
    assert w.stages.migrations == [] and not (w.proj / mig.ILL_NAME).exists()


# ---------------------------------------------------------------- attempts across a fresh re-queue
def test_an_error_row_closes_on_its_third_run_even_when_requeued_fresh(tmp_path, monkeypatch):
    """Run 1 (day 1): ERROR, attempts 1, retry_later until day 2. Run 2 (day 1, later): the DOI is
    re-queued fresh in lit_pull_queue.csv (not through `retry`): attempts 2, read from retry_later.
    Run 3 (day 2): re-admitted, attempts 3: TERMINAL_CLOSED with the reason."""
    w = World(tmp_path, monkeypatch)
    e = doi_of("too_small")
    w.queue([e])
    assert w.sweep()["exit_code"] == 0
    assert w.retry()[e]["attempts"] == "1" and w.retry()[e]["not_before"] == DAY2
    w.queue([e])                                              # fresh: no attempts column
    assert w.sweep()["exit_code"] == 0
    assert w.residual(f"{DAY}.2")[e]["attempts"] == "2"
    assert w.residual(f"{DAY}.2")[e]["residual_class"] == "TRANSIENT"
    res = w.sweep(DAY2)
    assert res["admitted"]["P"]["admitted"] == 1
    got = w.residual(DAY2, tag="retry")[e]
    assert got["residual_class"] == "TERMINAL_CLOSED" and got["attempts"] == "3"
    assert "error in 3 runs" in got["reason"]
    assert e in w.md(mig.ILL_NAME) and e not in w.retry()


def test_retry_history_reads_retry_later_by_doi(tmp_path):
    write_csv(tmp_path / sweep.RETRY_LATER_FILE, [
        {"doi": "https://doi.org/10.1000/W2G.A", "attempts": "2"}, {"doi": "10.1000/w2g.a", "attempts": "1"},
        {"doi": "10.1000/w2g.b", "attempts": ""}], ["doi", "attempts"])
    assert sweep.retry_history(tmp_path) == {"10.1000/w2g.a": 2, "10.1000/w2g.b": 0}
    assert sweep.retry_history(tmp_path / "missing") == {}


# ---------------------------------------------------------------- DEC-31: which projects run the preprint stage
ARXIV = "10.48550/arxiv.2101.00001"


def test_no_preprint_sources_skips_the_stage(tmp_path, monkeypatch):
    w = World(tmp_path, monkeypatch, sources=None)            # no `sources` key: unpaywall + pmc
    w.queue([doi_of("closed"), doi_of("dl_403")])
    res = w.sweep()
    assert res["exit_code"] == 0
    assert "preprint_fetch.py" not in [s for s, _ in w.stages.calls]
    r = res["projects"]["P"]["results"][0]
    assert r["stages"]["preprint"] == "skipped" and r["retired"]
    assert w.residual()[doi_of("closed")]["skipped_sources"] == "preprint"
    args = w.stages.migrations[-1][0]
    assert "--skip-preprint" in args


def test_an_arxiv_doi_runs_the_stage_for_that_row_only(tmp_path, monkeypatch):
    w = World(tmp_path, monkeypatch, sources=["unpaywall", "pmc"])
    w.queue([doi_of("closed"), ARXIV.upper().replace("ARXIV", "arXiv")])
    res = w.sweep()
    assert res["exit_code"] == 0
    cmd = w.stages.cmd("preprint_fetch.py")
    assert cmd[cmd.index("--project") + 1] == "P"
    assert w.stages.preprint_triage == [ARXIV]
    r = res["projects"]["P"]["results"][0]
    assert r["stages"]["preprint"] == "completed" and r["retired"]
    resid = w.residual()
    assert resid[doi_of("closed")]["skipped_sources"] == "preprint"      # kept from the stage
    assert resid[ARXIV]["residual_class"] == "TRANSIENT"                 # arXiv refused: not NO_MATCH
    detail = {(x["section"], x["name"]): x for x in read_csv(w.proj / f"lit_pull_queue.{DAY}.report.csv")}
    assert "10.48550/ row(s) only" in detail[("stage", "preprint_fetch")]["detail"]


def test_a_preprint_source_runs_the_stage_for_every_row(tmp_path, monkeypatch):
    w = World(tmp_path, monkeypatch, sources=["unpaywall", "pmc", "osf"])
    w.queue([doi_of("closed"), ARXIV])
    w.sweep()
    assert sorted(w.stages.preprint_triage) == sorted([doi_of("closed"), ARXIV])


def test_skip_preprint_still_skips_an_arxiv_row(tmp_path, monkeypatch):
    w = World(tmp_path, monkeypatch, sources=None)
    w.queue([ARXIV])
    res = w.sweep(skip_preprint=True)
    assert res["exit_code"] == 0 and "preprint_fetch.py" not in [s for s, _ in w.stages.calls]
    assert res["projects"]["P"]["results"][0]["stages"]["preprint"] == "skipped"


@pytest.mark.parametrize("sources,excluded", [
    (None, True), (["unpaywall", "pmc"], True), (["unpaywall", "pmc", "arxiv"], False),
    (["europepmc_preprints"], False), (["sportrxiv"], False), (["openalex_content", "pmc"], True)])
def test_preprint_excluded_follows_dec31(sources, excluded):
    entry = {"lib_dir": "lit"}
    if sources is not None:
        entry["sources"] = sources
    assert sweep._preprint_excluded("P", {"projects": {"P": entry}}) is excluded


# ---------------------------------------------------------------- MetadataUnavailable
def test_fill_metadata_keeps_its_return_values(monkeypatch):
    monkeypatch.setattr(sweep, "_resolve_meta", lambda d: (_ for _ in ()).throw(ris_emit.MetadataUnavailable("x")))
    assert sweep.fill_metadata({"title": ""}, "10.1/x") == "error:MetadataUnavailable"
    assert sweep._fill({"title": ""}, "10.1/x") == ("error:MetadataUnavailable", True)
    monkeypatch.setattr(sweep, "_resolve_meta", lambda d: (_ for _ in ()).throw(RuntimeError("boom")))
    assert sweep._fill({"title": ""}, "10.1/x") == ("error:RuntimeError", False)
    monkeypatch.setattr(sweep, "_resolve_meta", lambda d: ({}, "none"))
    assert sweep.fill_metadata({"title": ""}, "10.1/x") == "unavailable"


def test_a_metadata_retry_keeps_the_queue_order(tmp_path, monkeypatch):
    calls = []

    def resolver(doi):
        calls.append(doi)
        if doi.endswith("b1") and calls.count(doi) == 1:
            raise ris_emit.MetadataUnavailable("crossref")
        return {"title": f"T {doi}"}, "crossref"

    monkeypatch.setattr(sweep, "_resolve_meta", resolver)
    norm = tmp_path / "n.csv"
    write_csv(norm, [{"doi": f"10.1000/{x}", "title": "", "authors": "A"} for x in ("a1", "b1", "c1")],
              ["doi", "title", "authors"])
    _f, to_fetch, settled, meta = sweep.prepare_rows(norm, tmp_path)
    assert [r["doi"] for r in to_fetch] == ["10.1000/a1", "10.1000/b1", "10.1000/c1"] and not settled
    assert calls == ["10.1000/a1", "10.1000/b1", "10.1000/c1", "10.1000/b1"]
    assert meta["retried"] == 1 and meta["filled"] == 3


# ---------------------------------------------------------------- verdicts, row by row
def _strip(row, stage):
    return legacy(row, stage)


@pytest.mark.parametrize("name", [n for n, r in EXPECTED.items() if r["led_by"] == "unpaywall"])
def test_unpaywall_typed_and_legacy_readings_agree_on_every_row_shape(name):
    """The W2a verifier's table (and two W2-G rows): the typed `outcome` and the legacy columns
    name the same Kind and give the same class, at run 1 and at run 3."""
    row = fixture("unpaywall")[doi_of(name)]
    typed, old = sweep.unpaywall_verdict(row), sweep.unpaywall_verdict(_strip(row, "unpaywall"))
    assert typed.typed or sweep.fetched_by(typed)
    assert not old.typed
    assert typed.kind is old.kind, (name, typed.kind, old.kind)
    want = EXPECTED[name]
    for attempts, col in ((1, "class_typed"), (3, "class_run3")):
        assert sweep.classify([typed], attempts)[0] == sweep.classify([old], attempts)[0] == want[col], (
            name, attempts)


def test_a_flagged_row_is_never_ok_or_text_only():
    pmc = fixture("pmc")[doi_of("pmc_flag_ok")]
    v = sweep.pmc_verdict(pmc)
    assert v.identity and v.kind is not Kind.OK and not v.text_only and v.flagged_file == "2020_Doe_PmcFlag.pdf"
    assert sweep.classify([v])[0] == "IDENTITY_FLAG"
    # whatever downloaded says: a flagged file is not the paper
    for row, f in ((dict(pmc, downloaded="True"), sweep.pmc_verdict),
                   (dict(fixture("unpaywall")[doi_of("identity_flag")], downloaded="True"), sweep.unpaywall_verdict),
                   (dict(fixture("preprint")[doi_of("ppr_flag_supplement")], downloaded="True", skipped="True"),
                    sweep.preprint_verdict)):
        assert not sweep.fetched_by(f(row))
    sup = sweep.unpaywall_verdict(fixture("unpaywall")[doi_of("supplement")])
    assert sup.identity and sweep.classify([sup])[0] == "IDENTITY_FLAG"
    assert sweep.is_flagged({"doc_kind": "SUPPLEMENT", "identity": "OK", "error": ""})   # the column alone
    assert sweep.is_flagged({"identity": "FLAG", "status": ""}, "status")
    assert not sweep.is_flagged({"identity": "TITLE_MATCH", "doc_kind": "ARTICLE", "error": "HTTP_403"})
    # another stage's genuine OK still fetches the row; a transient or blocked one outranks the flag
    ok, out, blocked = (sweep.Verdict("pmc", Kind.OK, downloaded=True), sweep.Verdict("pmc", Kind.OUTAGE),
                        sweep.Verdict("pmc", Kind.REFUSED, download_host=True))
    assert sweep.classify([v, ok])[0] == "fetched"
    assert sweep.classify([v, out])[0] == "TRANSIENT"
    assert sweep.classify([v, blocked])[0] == "OA_BLOCKED"


def test_text_only_is_exactly_the_w2b_definition():
    am = fixture("pmc")[doi_of("pmc_am_text")]
    assert sweep.pmc_verdict(am).text_only
    assert sweep.pmc_verdict(dict(am, pmc_class="AM (from S3 metadata)")).text_only
    for change in ({"pmc_class": "OA"}, {"pmc_class": "NONE"}, {"identity": "OK"}, {"sidecar": "False"},
                   {"sidecar_status": "IDENTITY_ONLY"}, {"sidecar_status": "OUTAGE"}):
        assert not sweep.pmc_verdict(dict(am, **change)).text_only, change
    assert sweep.pmc_verdict(dict(am, sidecar_status="EXISTS")).text_only
    assert sweep.pmc_verdict(_strip(dict(am, pmc_class="OA"), "pmc")).text_only   # legacy: no class to read


def test_embargo_dates_and_the_not_before_extra():
    dated = sweep.pmc_verdict(fixture("pmc")[doi_of("pmc_embargo_dated")])
    undated = sweep.pmc_verdict(fixture("pmc")[doi_of("pmc_embargo_undated")])
    assert (dated.kind, dated.release_date) == (Kind.EMBARGOED, "2027-03-11")
    assert (undated.kind, undated.release_date) == (Kind.EMBARGOED, "")
    later = dataclasses.replace(dated, release_date="2027-06-01", stage="preprint")
    assert sweep.classify([later, dated]) == ("TRANSIENT", "pmc: EMBARGOED until 2027-03-11")
    assert sweep.residual_extras([later, dated], "TRANSIENT")["not_before"] == "2027-03-11"
    assert sweep.residual_extras([undated], "TRANSIENT")["not_before"] == ""
    assert sweep.residual_extras([dated], "fetched")["not_before"] == ""


@pytest.mark.parametrize("name,download_host,cls", [
    ("pmc_refused_dl", True, "OA_BLOCKED"), ("pmc_refused_lookup", False, "TRANSIENT"),
    ("ppr_refused_dl", True, "OA_BLOCKED"), ("ppr_arxiv_refused", False, "TRANSIENT"),
    ("dl_403", True, "OA_BLOCKED"), ("upw_host_refused", False, "TRANSIENT")])
def test_refused_is_oa_blocked_only_on_a_download_host(name, download_host, cls):
    stage = EXPECTED[name]["led_by"]
    v = VERDICT[stage](fixture(stage)[doi_of(name)])
    assert v.kind is Kind.REFUSED and v.download_host is download_host
    assert sweep.classify([v])[0] == cls


def test_an_unpaywall_refusal_on_the_api_route_is_not_a_download_host():
    row = dict(fixture("unpaywall")[doi_of("dl_403")], route="api")
    assert not sweep.unpaywall_verdict(row).download_host
    assert sweep.unpaywall_verdict(_strip(row, "unpaywall")).download_host    # legacy: oa_status OA


def test_skipped_details():
    rows = fixture("preprint")
    manual = sweep.preprint_verdict(rows[doi_of("ppr_manual")])
    assert manual.manual_preprint and manual.landing_url == LANDING
    assert sweep.classify([manual]) == ("OA_BLOCKED", "preprint: MANUAL_PREPRINT")
    excl = sweep.preprint_verdict(rows[doi_of("ppr_excluded")])
    assert excl.excluded_source == "medrxiv" and excl.kind is Kind.SKIPPED
    assert sweep.classify([excl])[0] == "SKIPPED_SOURCE"            # no enabled stage's verdict remains
    closed = sweep.Verdict("unpaywall", Kind.NOT_AVAILABLE, "CLOSED")
    assert sweep.classify([closed, excl])[0] == "TERMINAL_CLOSED"   # the remaining stages decide
    assert sweep.classify([sweep.Verdict("pmc", Kind.ERROR, "x"), excl])[0] == "TRANSIENT"
    other = sweep.preprint_verdict(rows[doi_of("ppr_skipped_other")])   # reads through `status`
    assert (other.typed, other.kind) == (False, Kind.NO_MATCH)
    assert sweep.preprint_verdict(_strip(rows[doi_of("ppr_excluded")], "preprint")).excluded_source == "medrxiv"


def test_not_at_ra_is_terminal_only_when_every_stage_is():
    ra = sweep.unpaywall_verdict(fixture("unpaywall")[doi_of("not_at_ra")])
    assert ra.kind is Kind.NOT_AT_RA
    assert sweep.classify([ra, sweep.Verdict("pmc", Kind.NO_MATCH)])[0] == "TERMINAL_CLOSED"
    assert sweep.classify([ra, sweep.Verdict("pmc", Kind.OUTAGE)])[0] == "TRANSIENT"
    assert sweep.classify([ra, sweep.Verdict("pmc", Kind.ERROR)], 1)[0] == "TRANSIENT"


@pytest.mark.parametrize("raw,kind", [
    ("HTTP_404", Kind.NOT_AVAILABLE), ("europepmc/HTTP_404", Kind.NOT_AVAILABLE),
    ("HTTP 404", Kind.NO_MATCH), ("BOILERPLATE:springer", Kind.ERROR), ("TOO_LARGE", Kind.ERROR),
    ("HTTP_4040", Kind.ERROR)])
def test_a_legacy_download_404_is_not_available(raw, kind):
    assert sweep.unpaywall_verdict({"doi": "10.1/x", "oa_status": "OA", "error": raw}).kind is kind
    if raw != "HTTP 404":
        assert sweep.pmc_verdict({"doi": "10.1/x", "pmcid": "PMC1", "error": raw}).kind is kind


def test_blank_or_unknown_typed_columns_fall_back_to_the_legacy_ones():
    row = {"doi": "10.1/x", "oa_status": "OA", "error": "HTTP_403", "outcome": "", "route": "publisher"}
    v = sweep.unpaywall_verdict(row)
    assert (v.typed, v.kind) == (False, Kind.REFUSED)
    v = sweep.unpaywall_verdict(dict(row, outcome="NOT_A_KIND"))
    assert (v.typed, v.kind) == (False, Kind.REFUSED)
    v = sweep.unpaywall_verdict(dict(row, outcome="outage"))
    assert (v.typed, v.kind) == (True, Kind.OUTAGE)


def test_the_typed_outcome_wins_over_a_disagreeing_legacy_string():
    row = {"doi": "10.1/x", "oa_status": "", "error": "HTTP 403", "outcome": "DEFERRED", "route": "api"}
    assert sweep.unpaywall_verdict(row).kind is Kind.DEFERRED
    assert sweep.unpaywall_verdict(_strip(row, "unpaywall")).kind is Kind.REFUSED


def test_aliased_takes_the_error_path():
    v = sweep.preprint_verdict(fixture("preprint")[doi_of("ppr_aliased")])
    assert v.kind is Kind.ALIASED
    assert sweep.classify([v], 1)[0] == "TRANSIENT" and sweep.classify([v], 3)[0] == "TERMINAL_CLOSED"


# ---------------------------------------------------------------- migrate on the new columns
def _typed_residual(proj, rows, run_id=DAY):
    fields = (list(sweep.QUEUE_COLUMNS) + ["citation_count", "residual_class", "reason", "stages", "held_at",
                                            "skipped_sources", "attempts", "run_id"]
              + list(sweep.RESIDUAL_EXTRA_FIELDS))
    base = {"title": "T", "authors": "Doe J", "year": "2020", "destination": "lit", "attempts": "1",
            "run_id": run_id}
    write_csv(mig.artifact_path(proj, run_id, "residual"), [{**base, **r} for r in rows], fields)


def test_migrate_routes_the_typed_extras(tmp_path, monkeypatch):
    w = World(tmp_path, monkeypatch)
    _typed_residual(w.proj, [
        {"doi": "10.1000/w2g.m1", "residual_class": "IDENTITY_FLAG",
         "reason": "unpaywall: DOI_MISMATCH:pdf_doi=10.9/x;identity=FLAG", "flagged_path": str(w.lib / "a.pdf")},
        {"doi": "10.1000/w2g.m2", "residual_class": "IDENTITY_FLAG",
         "reason": "pmc: DOI_MISMATCH:identity_flag", "flagged_path": str(w.lib / "b.pdf")},
        {"doi": "10.1000/w2g.m3", "residual_class": "TRANSIENT", "reason": "pmc: EMBARGOED until 2027-03-11",
         "not_before": "2027-03-11"},
        {"doi": "10.1000/w2g.m4", "residual_class": "TRANSIENT",
         "reason": "pmc: EMBARGOED (release date unknown)"},
        {"doi": "10.1000/w2g.m5", "residual_class": "OA_BLOCKED", "reason": "preprint: MANUAL_PREPRINT",
         "landing_url": LANDING},
        {"doi": "10.1000/w2g.m6", "residual_class": "TRANSIENT", "reason": "unpaywall: HTTP 503",
         "not_before": "2026-09-01"},
    ])
    with contextlib.redirect_stdout(io.StringIO()):
        assert mig.main(["--project", "P", "--date", DAY, "--no-holdings"]) == 0
    rev = w.md(mig.REVIEW_NAME)
    assert f"file `{w.lib / 'a.pdf'}` evidence `{w.lib / 'a.identity.json'}`" in rev
    assert f"file `{w.lib / 'b.pdf'}` evidence `{w.lib / 'b.fulltext.json'} (identity_* fields)`" in rev
    rl = w.retry()
    assert rl["10.1000/w2g.m3"]["not_before"] == "2027-03-11"
    assert rl["10.1000/w2g.m4"]["not_before"] == "2026-10-08"       # an undated embargo: 7 days
    assert rl["10.1000/w2g.m5"]["not_before"] == "2026-10-04"
    assert rl["10.1000/w2g.m6"]["not_before"] == DAY2                # a stale date is ignored
    assert f"[10.1000/w2g.m5]({LANDING}) cause `manual_preprint` via `preprint`" in w.md(mig.OA_BLOCKED_NAME)


SICI = "10.1002/(sici)1097-4598(199709)20:9<1097::aid-mus3>3.0.co;2-x"


def test_a_worklist_link_percent_encodes_the_doi(tmp_path, monkeypatch):
    """DOI Handbook 2025 4.7: a DOI in a URL path is percent-encoded (litpipe.doi.encode_path). The
    SICI `<` and `>` would otherwise break the markdown link and the browser click; the visible
    DOI text stays as it is, and the list still dedups on it."""
    w = World(tmp_path, monkeypatch)
    _typed_residual(w.proj, [{"doi": SICI, "residual_class": "OA_BLOCKED", "reason": "unpaywall: HTTP_403"},
                             {"doi": "https://doi.org/10.1000/W2G.Link9", "residual_class": "OA_BLOCKED",
                              "reason": "unpaywall: HTTP_403"}])
    for _ in range(2):
        with contextlib.redirect_stdout(io.StringIO()):
            assert mig.main(["--project", "P", "--date", DAY, "--no-holdings"]) == 0
    oab = w.md(mig.OA_BLOCKED_NAME)
    assert (f"[{SICI}](https://doi.org/10.1002/(sici)1097-4598(199709)20:9%3C1097::aid-mus3%3E3.0.co;2-x) "
            f"cause `HTTP_403`") in oab
    assert "[https://doi.org/10.1000/W2G.Link9](https://doi.org/10.1000/w2g.link9)" in oab
    assert oab.count(f"[{SICI}]") == 1                                   # the re-run added nothing
    assert mig.doi_url("10.1000/456#789") == "https://doi.org/10.1000/456"   # normalised: no fragment
    assert mig.doi_url("not a doi") == "https://doi.org/not%20a%20doi"


def test_migrate_reads_a_typed_chain_without_a_residual(tmp_path, monkeypatch):
    """A chain whose sweep left no residual CSV is classified here from the typed reports, the way
    sweep reads them: a flagged PMC file with a sidecar is IDENTITY_FLAG (never TEXT_ONLY), an
    embargo waits for its date, a source the project excludes is no verdict."""
    w = World(tmp_path, monkeypatch)
    names = ["pmc_flag_ok", "pmc_embargo_dated", "ppr_excluded", "ppr_manual", "pmc_am_text"]
    stages = Stages()
    upw = [stages._row("unpaywall", doi_of(n), {"title": "T"}) for n in names]
    write_csv(mig.artifact_path(w.proj, DAY, "unpaywall"), upw, FIELDS["unpaywall"][0])
    write_csv(mig.artifact_path(w.proj, DAY, "pmc"), [stages._row("pmc", doi_of(n), {}) for n in names],
              FIELDS["pmc"][0])
    write_csv(mig.artifact_path(w.proj, DAY, "preprint"),
              [stages._row("preprint", doi_of(n), {}) for n in names], FIELDS["preprint"][0])
    rows = {r["doi"]: r for r in mig.read_report_chain(w.proj, DAY)}
    for r in rows.values():
        mig.classify_row(r, sources={"unpaywall", "pmc", "osf"})
    got = {n: rows[doi_of(n)]["residual_class"] for n in names}
    assert got == {"pmc_flag_ok": "IDENTITY_FLAG", "pmc_embargo_dated": "TRANSIENT",
                   "ppr_excluded": "TERMINAL_CLOSED", "ppr_manual": "OA_BLOCKED", "pmc_am_text": "TEXT_ONLY"}
    assert rows[doi_of("pmc_embargo_dated")]["not_before"] == "2027-03-11"
    assert rows[doi_of("ppr_manual")]["landing_url"] == LANDING
    assert rows[doi_of("pmc_flag_ok")]["flagged_path"] == ["2020_Doe_PmcFlag.pdf"]    # no queue: no library


def test_migrate_dry_run_on_a_typed_run_writes_nothing(tmp_path, monkeypatch):
    w = World(tmp_path, monkeypatch)
    w.queue(main_queue(), blank=(UNAVAIL, RETRY_OK, NOTFOUND))
    with contextlib.redirect_stdout(io.StringIO()):
        sweep.run(project="P", date=DAY, migrate=False)

    def tree():
        return {str(p): p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}

    before = tree()
    with contextlib.redirect_stdout(io.StringIO()):
        assert mig.main(["--project", "P", "--date", DAY, "--dry-run"]) == 0
    assert tree() == before


def test_the_residual_keeps_its_columns_and_appends_the_extras(tmp_path, monkeypatch):
    w = World(tmp_path, monkeypatch)
    w.queue([doi_of("closed")])
    w.sweep()
    with open(w.proj / sweep.artifact_name("", DAY, "residual"), encoding="utf-8") as f:
        header = next(csv.reader(f))
    w1 = ["doi", "title", "authors", "year", "destination", "notes", "citation_count", "residual_class",
          "reason", "stages", "held_at", "skipped_sources", "attempts", "run_id"]
    assert header == w1 + ["not_before", "flagged_path", "landing_url"]
