"""W1-D1: sweep runs that keep their records and retire honestly.

Every test runs sweep against a temp projects root and a temp registry; the stage scripts are
replaced by FakeStages (a subprocess.run stand-in that writes stage reports in the real column
shapes, replaying rows copied from live reports in tests/fixtures/W1-D1/). No network: the
metadata resolver and the holdings map are fakes too.
"""
import csv
import datetime
import inspect
import json
import subprocess
import sys
import types
from pathlib import Path

import pytest

import lit_util
import sweep

FIX = Path(__file__).parent / "fixtures" / "W1-D1"
DAY = "2026-09-30"
UNPW_FIELDS = ["rank", "doi", "year", "cites", "filename", "title", "oa_status", "n_locations",
               "downloaded", "winning_host", "winning_url", "attempts", "error"]
PMC_FIELDS = ["doi", "filename", "pmcid", "downloaded", "skipped", "winning_source", "attempts",
              "error", "sidecar", "sidecar_status"]
PPR_FIELDS = ["doi", "title", "year", "preprint_filename", "found", "source", "match_id",
              "similarity", "downloaded", "skipped", "status"]
SCRIPTS = {"unpaywall_fetch_v2.py": "unpaywall", "pmc_fetch.py": "pmc",
           "preprint_fetch.py": "preprint", "extract_pdf_fulltext.py": "extract",
           "migrate_closed_to_md.py": "migrate"}


def read_csv(path):
    with open(path, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path, rows, fields):
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        w.writeheader()
        w.writerows(rows)


def fixture_rows(prefix, stage):
    return {r["doi"].strip().lower(): r for r in read_csv(FIX / f"{prefix}.{stage}.csv")}


class FakeStages:
    """subprocess.run stand-in for the stage scripts. `spec[doi][stage]` is the report row that
    stage writes for that DOI (defaults: CLOSED, NO_PMCID, NO_MATCH); `crash` lists stages that
    exit 1 without a report. PMC reads the unpaywall report exactly as pmc_fetch does (skips
    downloaded and SKIP_EXISTS rows); preprint reads its --triage."""

    def __init__(self):
        self.spec, self.crash, self.calls, self.triage = {}, set(), [], {}

    def stages_called(self):
        return [s for s, _cmd, _kw in self.calls]

    def triage_dois(self, stage="unpaywall"):
        rows = self.triage.get(stage)
        return None if rows is None else [r["doi"] for r in rows]

    def __call__(self, cmd, **kw):
        cmd = [str(c) for c in cmd]
        script = next(Path(c).name for c in cmd if c.endswith(".py"))
        stage = SCRIPTS[script]
        self.calls.append((stage, cmd, kw))
        if stage in self.crash:
            return types.SimpleNamespace(returncode=1, stdout="", stderr="boom")
        arg = lambda flag: cmd[cmd.index(flag) + 1]
        if "--triage" in cmd:   # read at call time: a retired run deletes the normalized file
            self.triage.setdefault(stage, read_csv(arg("--triage")))
        if stage == "unpaywall":
            rows = []
            for r in read_csv(arg("--triage")):
                d = r["doi"].strip().lower()
                row = {"doi": d, "title": r.get("title", ""), "oa_status": "CLOSED",
                       "downloaded": "False", "error": ""}
                row.update(self.spec.get(d, {}).get("unpaywall", {}))
                rows.append(row)
            write_csv(arg("--report"), rows, UNPW_FIELDS)
        elif stage == "pmc":
            rows = []
            for r in read_csv(arg("--report-in")):
                if r["downloaded"] == "True" or r["oa_status"] == "SKIP_EXISTS":
                    continue
                d = r["doi"].strip().lower()
                row = {"doi": d, "downloaded": "False", "skipped": "False", "error": "NO_PMCID",
                       "sidecar": "False"}
                row.update(self.spec.get(d, {}).get("pmc", {}))
                rows.append(row)
            write_csv(arg("--report-out"), rows, PMC_FIELDS)
        elif stage == "preprint":
            rows = []
            for r in read_csv(arg("--triage")):
                d = r["doi"].strip().lower()
                row = {"doi": d, "downloaded": "False", "skipped": "False", "status": "NO_MATCH"}
                row.update(self.spec.get(d, {}).get("preprint", {}))
                rows.append(row)
            write_csv(arg("--report"), rows, PPR_FIELDS)
        return types.SimpleNamespace(returncode=0, stdout="  DOWNLOADED:  0\n", stderr="")


class FakeHold:
    def __init__(self, held=None):
        self.held = {k.lower(): [Path(p) for p in v] for k, v in (held or {}).items()}

    def where(self, doi):
        return self.held.get(doi.lower(), [])

    def records(self, doi):   # a .pdf path is a PDF holding; anything else is a sidecar
        from types import SimpleNamespace
        return [SimpleNamespace(path=p, has_pdf=p.suffix.lower() == ".pdf")
                for p in self.held.get(doi.lower(), [])]


def _no_network(doi):
    raise AssertionError(f"metadata lookup for {doi} in a test that expected none")


class Env:
    def __init__(self, tmp_path, monkeypatch):
        self.tmp = tmp_path
        self.root = tmp_path / "Projects"
        self.root.mkdir()
        self.cfg_path = tmp_path / "projects.json"
        self.loose = self.root / "Ops" / "LOOSE_ENDS.md"
        monkeypatch.setattr(lit_util, "PROJECTS_ROOT", self.root)
        monkeypatch.setattr(sweep, "CONFIG_PATH", self.cfg_path)
        monkeypatch.setenv("HOME", str(tmp_path / "home"))
        monkeypatch.setenv("USERPROFILE", str(tmp_path / "home"))
        self.holdings = FakeHold()
        monkeypatch.setattr(sweep, "_load_holdings", lambda registry: (self.holdings, None))
        monkeypatch.setattr(sweep, "_resolve_meta", _no_network)
        self.stages = FakeStages()
        monkeypatch.setattr(sweep.subprocess, "run", self.stages)
        self.register({"P": {"lib_dir": "lit"}})

    def register(self, projects, loose_ends=True):
        cfg = {"projects": projects}
        if loose_ends:
            cfg["loose_ends"] = "Ops/LOOSE_ENDS.md"
        self.cfg_path.write_text(json.dumps(cfg), encoding="utf-8")

    def pdir(self, key="P"):
        d = lit_util.project_root(key, json.loads(self.cfg_path.read_text())["projects"].get(key, {}))
        d.mkdir(parents=True, exist_ok=True)
        return d

    def queue(self, dois, key="P", name="lit_pull_queue.csv", dest="lit", head="", extra=None):
        rows = [{"doi": d, "title": f"Title {i}", "authors": "Smith J", "year": "2020",
                 "destination": dest, "notes": ""} for i, d in enumerate(dois)]
        for r in rows:
            r.update((extra or {}).get(r["doi"], {}))
        p = self.pdir(key) / name
        with open(p, "w", encoding="utf-8", newline="") as f:
            f.write(head)
            w = csv.DictWriter(f, fieldnames=list(sweep.QUEUE_COLUMNS), lineterminator="\n")
            w.writeheader()
            w.writerows(rows)
        return p

    def sweep(self, *args, date=DAY):
        return sweep.main(["--date", date, *args])

    def art(self, stage, run_id=DAY, key="P", tag=""):
        return self.pdir(key) / sweep.artifact_name(tag, run_id, stage)

    def report(self, run_id=DAY, key="P", tag=""):
        return {(r["section"], r["name"]): r for r in read_csv(self.art("report", run_id, key, tag))}

    def residual(self, run_id=DAY, key="P", tag=""):
        return {r["doi"]: r for r in read_csv(self.art("residual", run_id, key, tag))}

    def loose_lines(self):
        return self.loose.read_text(encoding="utf-8").splitlines() if self.loose.exists() else []


@pytest.fixture
def env(tmp_path, monkeypatch):
    return Env(tmp_path, monkeypatch)


# ---------------------------------------------------------------- run ids and names
def test_parse_artifact_reads_current_and_legacy_names():
    P = sweep.parse_artifact
    assert P("lit_pull_queue.2026-09-15.report.csv") == sweep.ArtifactName("", "2026-09-15", "report")
    assert P("lit_pull_queue.2026-09-15.processed.2.csv") == sweep.ArtifactName(
        "", "2026-09-15", "processed", 2)
    assert P("lit_pull_queue.2026-09-15.3.unpaywall.csv") == sweep.ArtifactName(
        "", "2026-09-15.3", "unpaywall")
    assert P("lit_pull_queue.retry.2026-09-15.2.residual.csv") == sweep.ArtifactName(
        "retry", "2026-09-15.2", "residual")
    for not_artifact in ("lit_pull_queue.csv", "lit_pull_queue.alpha.csv",
                         "lit_pull_queue.retry_later.csv", "lit_pull_queue.ch15_b01.draft.csv",
                         "lit_pull_queue.snapshot.4501bc_b01.unpaywall.csv",
                         "lit_pull_queue.2026-09-15.report.2.csv"):
        assert P(not_artifact) is None, not_artifact


def test_choose_run_id_counts_legacy_artifacts(tmp_path):
    assert sweep.choose_run_id([tmp_path], DAY) == DAY
    (tmp_path / f"lit_pull_queue.{DAY}.processed.2.csv").write_text("x")  # legacy same-day suffix
    assert sweep.choose_run_id([tmp_path], DAY) == f"{DAY}.2"
    (tmp_path / f"lit_pull_queue.alpha.{DAY}.2.report.csv").write_text("x")  # any tag counts
    assert sweep.choose_run_id([tmp_path], DAY) == f"{DAY}.3"
    (tmp_path / f"lit_pull_queue.{DAY}.candidates_all.csv").write_text("x")  # not a sweep stage
    assert sweep.choose_run_id([tmp_path / "missing", tmp_path], DAY) == f"{DAY}.3"


def test_first_run_of_the_day_keeps_the_legacy_names(env):
    """A consumer's finalize step reads lit_pull_queue.<date>.<stage>.csv: the first run of a day must
    still write exactly those names."""
    env.queue(["10.1000/a1"])
    assert env.sweep() == 0
    for stage in ("unpaywall", "pmc", "residual", "report", "processed"):
        assert env.art(stage).exists(), stage
    assert not env.art("normalized").exists()   # retired: normalized removed, as before


def test_two_same_day_sweeps_keep_two_complete_artifact_sets(env):
    env.queue(["10.1000/first"])
    assert env.sweep() == 0
    env.queue(["10.1000/second"])
    assert env.sweep() == 0
    for run_id, doi in ((DAY, "10.1000/first"), (f"{DAY}.2", "10.1000/second")):
        for stage in ("unpaywall", "pmc", "preprint", "residual", "report", "processed"):
            assert env.art(stage, run_id).exists(), (run_id, stage)
        assert [r["doi"] for r in read_csv(env.art("unpaywall", run_id))] == [doi]
        assert [r["doi"] for r in read_csv(env.art("processed", run_id))] == [doi]
        assert env.report(run_id)[("run", "run_id")]["detail"] == run_id


def test_two_tagged_queues_in_one_run_keep_separate_artifacts(env):
    env.queue(["10.1000/plain"])
    env.queue(["10.1000/a1", "10.1000/a2"], name="lit_pull_queue.alpha.csv")
    env.queue(["10.1000/b1"], name="lit_pull_queue.beta.csv")
    assert env.sweep() == 0
    for tag, dois in (("", ["10.1000/plain"]), ("alpha", ["10.1000/a1", "10.1000/a2"]),
                      ("beta", ["10.1000/b1"])):
        assert [r["doi"] for r in read_csv(env.art("unpaywall", tag=tag))] == dois
        assert env.report(tag=tag)[("total", "rows")]["count"] == str(len(dois))
        assert env.art("processed", tag=tag).exists()
    assert not list(env.pdir().glob("lit_pull_queue.*2026-09-30.2*"))   # one run id per run


@pytest.mark.parametrize("tag,ok", [
    ("alpha", True), ("s09_fwd", True), ("a-b", True), ("retry", True),
    ("Alpha", False), ("4501_pool", False), ("rerun-2026-08-25", False), ("draft", False),
    ("retry_later", False), ("report", False), ("a" * 33, False), ("", False)])
def test_tag_rules(tag, ok):
    assert sweep.is_valid_tag(tag) is ok


def test_tag_shaped_pool_without_queue_columns_is_left_alone(env):
    """A consumer keeps lit_pull_queue.<tag>_pool.csv (a candidate pool, no destination column) beside
    its batches; the tag rule alone would sweep 1,000 rows of it."""
    pool = env.pdir() / "lit_pull_queue.ch15_pool.csv"
    pool.write_text("doi,title,year,venue,authors,cited_by,n_seeds,verdict,on_disk,facets\n"
                    "10.1000/p1,T,2020,V,A,1,1,ON,0,x\n", encoding="utf-8")
    env.queue(["10.1000/a1"])
    assert env.sweep() == 0
    assert pool.exists() and env.stages.triage_dois() == ["10.1000/a1"]
    assert not list(env.pdir().glob("lit_pull_queue.ch15_pool.2026*"))


# ---------------------------------------------------------------- exit codes
def test_failed_stage_exits_nonzero_and_keeps_the_queue(env):
    q = env.queue(["10.1000/a1"])
    env.stages.crash.add("pmc")
    assert env.sweep() == sweep.EXIT_STAGE_FAILED
    assert q.exists() and not env.art("processed").exists()
    rep = env.report()
    assert rep[("queue", "kept")]["detail"] == "pmc failed"
    assert rep[("class", "PENDING")]["count"] == "1"


def test_unpaywall_failure_exits_nonzero_and_keeps_the_queue(env):
    q = env.queue(["10.1000/a1"])
    env.stages.crash.add("unpaywall")
    assert env.sweep() == sweep.EXIT_STAGE_FAILED
    assert q.exists()
    assert env.stages.stages_called() == ["unpaywall"]
    assert env.art("report").exists() and env.art("normalized").exists()


def test_extract_failure_sets_the_exit_code_but_the_queue_retires(env):
    env.queue(["10.1000/a1"])
    env.stages.crash.add("extract")
    assert env.sweep() == sweep.EXIT_STAGE_FAILED
    assert env.art("processed").exists()


def test_every_stage_completed_with_nothing_fetched_exits_zero(env):
    env.queue(["10.1000/a1", "10.1000/a2"])
    assert env.sweep() == 0
    assert env.report()[("total", "downloaded")]["count"] == "0"


def test_help_lists_the_exit_codes():
    r = subprocess.run([sys.executable, str(Path(sweep.__file__)), "--help"],
                       capture_output=True, text=True, encoding="utf-8", cwd=str(Path.home()))
    assert r.returncode == 0
    for code in ("0  every stage", "1  --project", "2  usage", "3  a stage failed", "4  a queue"):
        assert code in r.stdout, code


# ---------------------------------------------------------------- retirement and classes
def test_skip_preprint_retires_the_queue_and_writes_no_partial_line(env):
    env.queue(["10.1000/closed", "10.1000/got"])
    env.stages.spec["10.1000/got"] = {"unpaywall": {"oa_status": "OA", "downloaded": "True"}}
    assert env.sweep("--skip-preprint") == 0
    assert "preprint" not in env.stages.stages_called()
    assert env.art("processed").exists()
    res = env.residual()
    assert res["10.1000/closed"]["residual_class"] == "TERMINAL_CLOSED"
    assert res["10.1000/closed"]["skipped_sources"] == "preprint"
    lines = env.loose_lines()
    assert len(lines) == 1 and lines[0].startswith(sweep.LOOSE_DONE)
    assert not any(sweep.LOOSE_PARTIAL in ln for ln in lines)
    assert env.report()[("stage", "preprint_fetch")]["detail"].startswith("skipped")


def test_a_queue_of_permanent_failures_retires_on_its_first_run(env):
    dois = ["10.1000/c1", "10.1000/c2", "10.1000/c3"]
    env.queue(dois)
    env.stages.spec["10.1000/c3"] = {"unpaywall": {"oa_status": "", "error": "HTTP 404"}}
    assert env.sweep() == 0
    assert env.art("processed").exists() and not (env.pdir() / "lit_pull_queue.csv").exists()
    assert {r["residual_class"] for r in env.residual().values()} == {"TERMINAL_CLOSED"}


def test_real_report_rows_classify_per_the_residual_table(env):
    """Replay the live teaching-project 2026-09-29 rows (fixtures) through a sweep."""
    queue = read_csv(FIX / "teaching_2026-09-29.processed.csv")
    unpw = fixture_rows("teaching_2026-09-29", "unpaywall")
    pmc = fixture_rows("teaching_2026-09-29", "pmc")
    ppr = fixture_rows("teaching_2026-09-29", "preprint")
    for d in unpw:
        env.stages.spec[d] = {"unpaywall": unpw[d], "pmc": pmc.get(d, {}), "preprint": ppr.get(d, {})}
    for r in queue:
        r["destination"] = "lit"
    p = env.pdir() / "lit_pull_queue.csv"
    write_csv(p, queue, list(sweep.QUEUE_COLUMNS))
    assert env.sweep() == 0
    res = env.residual()
    got = {d: res[d]["residual_class"] if d in res else "fetched" for d in unpw}
    expect = {
        "10.1002/cphy.c150028": "fetched", "10.1038/s41368-020-0074-x": "fetched",
        "10.1006/cbmr.1998.1504": "TERMINAL_CLOSED",                  # CLOSED, NO_PMCID, NO_MATCH
        "10.1164/arrd.1985.131.5.672": "TERMINAL_CLOSED",             # Unpaywall API 404
        "10.1055/s-2003-39084": "TRANSIENT",                          # download HTTP_500
        "10.1002/andp.18561751008": "OA_BLOCKED",                     # publisher HTTP_403
        "10.1016/j.jaci.2010.05.016": "OA_BLOCKED",                   # PMC HTML wall, sidecar 500
        "10.1007/s00418-018-1747-9": "TEXT_ONLY",                     # PMC HTML wall, sidecar OK
    }
    for d, cls in expect.items():
        assert got[d] == cls, (d, got[d])
    text_only = [d for d, r in pmc.items() if r["sidecar"] == "True"]
    assert text_only and all(got[d] == "TEXT_ONLY" for d in text_only)
    rep = env.report()
    assert rep[("class", "fetched")]["count"] == "2"
    assert env.art("processed").exists()


def test_report_counts_skip_exists_apart_from_downloads(env):
    env.queue(["10.1000/held", "10.1000/new", "10.1000/pmc"])
    env.stages.spec["10.1000/held"] = {"unpaywall": {"oa_status": "SKIP_EXISTS"}}
    env.stages.spec["10.1000/new"] = {"unpaywall": {"oa_status": "OA", "downloaded": "True"}}
    env.stages.spec["10.1000/pmc"] = {"pmc": {"skipped": "True", "winning_source": "ALREADY_EXISTS",
                                              "error": ""}}
    assert env.sweep() == 0
    rep = env.report()
    assert rep[("total", "downloaded")]["count"] == "1"
    assert rep[("total", "skip_exists")]["count"] == "2"
    assert rep[("skip_exists", "unpaywall_v2")]["count"] == "1"
    assert rep[("skip_exists", "pmc_fetch")]["count"] == "1"
    assert rep[("class", "fetched")]["count"] == "3"


def test_config_outcome_aborts_the_run(env):
    q = env.queue(["10.1000/a1"])
    env.stages.spec["10.1000/a1"] = {"unpaywall": {"oa_status": "", "error": "HTTP 422"}}
    assert env.sweep() == sweep.EXIT_USAGE
    assert q.exists()


@pytest.mark.parametrize("verdicts,attempts,expected", [
    ([("pmc", sweep.Kind.ERROR, {"identity": True, "raw": "DOI_MISMATCH:pdf_doi=10.9/x"}),
      ("unpaywall", sweep.Kind.NOT_AVAILABLE, {})], 1, "IDENTITY_FLAG"),
    ([("unpaywall", sweep.Kind.ERROR, {"raw": "HTTP_404"})], 1, "TRANSIENT"),
    ([("unpaywall", sweep.Kind.ERROR, {"raw": "HTTP_404"})], 3, "TERMINAL_CLOSED"),
    ([("preprint", sweep.Kind.SKIPPED, {"manual_preprint": True})], 1, "OA_BLOCKED"),
    ([("unpaywall", sweep.Kind.REFUSED, {"raw": "HTTP 403"})], 1, "TRANSIENT"),      # the API
    ([("unpaywall", sweep.Kind.REFUSED, {"download_host": True})], 1, "OA_BLOCKED"),
    ([("unpaywall", sweep.Kind.OUTAGE, {}), ("pmc", sweep.Kind.REFUSED, {"download_host": True})],
     1, "OA_BLOCKED"),
    ([("unpaywall", sweep.Kind.EMBARGOED, {})], 1, "TRANSIENT"),
    ([("pmc", sweep.Kind.NO_MATCH, {"text_only": True})], 1, "TEXT_ONLY"),
    ([], 1, "SKIPPED_SOURCE"),
])
def test_classify_follows_the_table_order(verdicts, attempts, expected):
    vs = [sweep.Verdict(stage, kind, **kw) for stage, kind, kw in verdicts]
    assert sweep.classify(vs, attempts=attempts)[0] == expected


# ---------------------------------------------------------------- rows settled before fetching
def test_a_row_held_in_another_library_is_skipped_with_its_path(env):
    other = env.root / "Other" / "lit" / "2020_Smith_Title.pdf"
    text_only = env.root / "Other" / "lit" / "2021_Jones_Title.fulltext.json"
    own = env.pdir() / "lit" / "x.fulltext.json"
    env.holdings.held = {"10.1000/elsewhere": [other], "10.1000/own": [own],
                         "10.1000/textonly": [text_only]}
    env.queue(["10.1000/elsewhere", "10.1000/own", "10.1000/new", "10.1000/textonly"])
    assert env.sweep() == 0
    # held elsewhere as a PDF: not fetched; held elsewhere only as text: the PDF is still fetched
    assert env.stages.triage_dois() == ["10.1000/own", "10.1000/new", "10.1000/textonly"]
    row = env.residual()["10.1000/elsewhere"]
    assert row["residual_class"] == "HELD_ELSEWHERE" and row["held_at"] == str(other)


def test_invalid_and_placeholder_dois_are_skipped_and_reported(env):
    env.queue(["NO_DOI_smith2020", "not-a-doi", "10.1000/ok"])
    assert env.sweep() == 0
    assert env.stages.triage_dois() == ["10.1000/ok"]
    res = env.residual()
    assert res["NO_DOI_smith2020"]["residual_class"] == "INVALID_DOI"
    assert res["not-a-doi"]["residual_class"] == "INVALID_DOI"
    assert env.report()[("class", "INVALID_DOI")]["count"] == "2"


def test_a_blank_title_row_is_filled(env, monkeypatch):
    env.queue(["10.1000/blank"], extra={"10.1000/blank": {"title": "", "authors": "", "year": ""}})
    monkeypatch.setattr(sweep, "_resolve_meta", lambda doi: (
        {"title": "Heat <i>strain</i> model", "year": "1972",
         "authors": [{"family": "Givoni", "given": "B."}, {"family": "Goldman", "given": "Ralph F"}]},
        "crossref"))
    assert env.sweep() == 0
    row = env.stages.triage["unpaywall"][0]
    assert (row["title"], row["authors"], row["year"]) == (
        "Heat strain model", "Givoni B; Goldman RF", "1972")
    assert env.report()[("metadata", "filled")]["count"] == "1"


@pytest.mark.parametrize("resolver", [
    lambda doi: ({}, "none"),
    lambda doi: (_ for _ in ()).throw(RuntimeError("https://api.x/?email=me@uconn.edu")),
])
def test_an_unfillable_blank_title_row_is_marked_not_fetched(env, monkeypatch, resolver):
    env.queue(["10.1000/blank", "10.1000/ok"], extra={"10.1000/blank": {"title": "", "authors": ""}})
    monkeypatch.setattr(sweep, "_resolve_meta", resolver)
    assert env.sweep() == 0
    assert env.stages.triage_dois() == ["10.1000/ok"]
    row = env.residual()["10.1000/blank"]
    assert row["residual_class"] == "NO_METADATA"
    assert "@" not in env.art("residual").read_text(encoding="utf-8")


def test_a_draft_with_hash_lines_sweeps(env):
    env.queue(["10.1000/a1", "10.1000/a2"],
              head="# seeded 2026-09-30 by seed_queue_from_top_candidates\n# rank by cites\n")
    assert sweep.first_destination(env.pdir() / "lit_pull_queue.csv") == "lit"
    assert env.sweep() == 0
    assert env.stages.triage_dois() == ["10.1000/a1", "10.1000/a2"]
    assert env.art("processed").exists()


# ---------------------------------------------------------------- retry_later
def test_a_due_retry_later_row_is_swept_and_a_future_one_is_not(env):
    d = env.pdir()
    write_csv(d / sweep.RETRY_LATER_FILE, [
        {"doi": "10.1000/due", "title": "Due", "authors": "A B", "year": "2020",
         "destination": "", "notes": "", "not_before": "2026-09-29", "attempts": "1"},
        {"doi": "10.1000/today", "title": "Today", "authors": "A B", "year": "2020",
         "destination": "lit", "notes": "", "not_before": DAY, "attempts": "0"},
        {"doi": "10.1000/later", "title": "Later", "authors": "A B", "year": "2020",
         "destination": "lit", "notes": "", "not_before": "2026-10-03", "attempts": "0"},
    ], list(sweep.QUEUE_COLUMNS) + ["not_before", "attempts"])
    assert env.sweep() == 0
    assert sorted(r["doi"] for r in read_csv(env.art("unpaywall", tag="retry"))) == [
        "10.1000/due", "10.1000/today"]
    assert [r["doi"] for r in read_csv(d / sweep.RETRY_LATER_FILE)] == ["10.1000/later"]
    assert env.art("processed", tag="retry").exists()
    assert env.residual(tag="retry")["10.1000/due"]["attempts"] == "2"


def test_retry_admission_survives_a_crash_between_writes(tmp_path):
    """The retry queue is written first; re-admitting the same due rows de-duplicates."""
    write_csv(tmp_path / sweep.RETRY_LATER_FILE,
              [{"doi": "10.1000/due", "destination": "lit", "not_before": ""}],
              ["doi", "destination", "not_before"])
    assert sweep.admit_retries(tmp_path, DAY)["admitted"] == 1
    write_csv(tmp_path / sweep.RETRY_LATER_FILE,   # as if the second write never happened
              [{"doi": "10.1000/DUE", "destination": "lit", "not_before": ""}],
              ["doi", "destination", "not_before"])
    sweep.admit_retries(tmp_path, DAY)
    assert [r["doi"] for r in read_csv(tmp_path / "lit_pull_queue.retry.csv")] == ["10.1000/due"]


# ---------------------------------------------------------------- dry run, destination, env
def _tree(root):
    return {str(p.relative_to(root)): (p.read_bytes() if p.is_file() else None)
            for p in sorted(root.rglob("*"))}


def test_a_dry_run_writes_nothing(env, monkeypatch, capsys):
    env.register({"P": {"lib_dir": "lit"}, "Q": {"lib_dir": "papers"}})
    env.queue(["10.1000/a1", "NO_DOI_x"], extra={"10.1000/a1": {"title": ""}})
    env.queue(["10.1000/t1"], name="lit_pull_queue.alpha.csv")
    env.queue(["10.1000/q1"], key="Q", dest="wrong/")
    write_csv(env.pdir() / sweep.RETRY_LATER_FILE,
              [{"doi": "10.1000/due", "destination": "lit", "not_before": "2026-01-01"}],
              ["doi", "destination", "not_before"])
    env.loose.parent.mkdir(parents=True)
    env.loose.write_text("x\n", encoding="utf-8")

    def boom(*a, **k):
        raise AssertionError("a dry run must not call this")
    monkeypatch.setattr(sweep.subprocess, "run", boom)
    monkeypatch.setattr(sweep, "_load_holdings", boom)
    before = _tree(env.tmp)
    assert env.sweep("--dry-run") == sweep.EXIT_QUEUE_REFUSED   # Q's destination is refused
    assert _tree(env.tmp) == before
    out = capsys.readouterr().out
    assert "would admit 1 due retry_later row" in out and "DRY would process 2 rows" in out


def test_a_doubled_subproject_tail_is_refused_unless_allowed(env):
    env.register({"Par/Sub": {"parent": "Par", "lib_dir": "Sub/literature"}})
    q = env.queue(["10.1000/a1"], key="Par/Sub", dest="Sub/literature/")
    assert env.sweep() == sweep.EXIT_QUEUE_REFUSED
    assert q.exists() and env.stages.calls == []
    assert env.sweep("--allow-destination") == 0
    assert (env.pdir("Par/Sub") / "Sub" / "literature").is_dir()


def test_the_registry_destination_is_accepted(env):
    env.register({"Par/Sub": {"parent": "Par", "lib_dir": "Sub/literature"}})
    env.queue(["10.1000/a1"], key="Par/Sub", dest="literature/")
    assert env.sweep() == 0


def test_stage_subprocesses_run_unbuffered(env, monkeypatch):
    monkeypatch.delenv("PYTHONUNBUFFERED", raising=False)   # sweep must set it, not inherit it
    env.queue(["10.1000/a1"])
    assert env.sweep() == 0
    assert env.stages.calls
    assert all(kw["env"]["PYTHONUNBUFFERED"] == "1" for _s, _c, kw in env.stages.calls)


def test_a_sources_list_without_preprint_servers_skips_the_stage(env):
    env.register({"P": {"lib_dir": "lit", "sources": ["unpaywall", "pmc"]}})
    env.queue(["10.1000/a1"])
    assert env.sweep() == 0
    assert "preprint" not in env.stages.stages_called()
    assert env.art("processed").exists()


def test_an_invalid_sources_list_is_a_config_error(env):
    env.register({"P": {"lib_dir": "lit", "sources": ["nope"]}})
    env.queue(["10.1000/a1"])
    assert env.sweep() == sweep.EXIT_USAGE
    assert env.stages.calls == []


# ---------------------------------------------------------------- LOOSE_ENDS, migrate, run()
def test_the_same_partial_state_is_not_logged_twice_in_a_row(env):
    env.queue(["10.1000/a1"])
    env.stages.crash.add("pmc")
    assert env.sweep() == 3
    assert env.sweep() == 3
    lines = env.loose_lines()
    assert len(lines) == 1 and lines[0].startswith(sweep.LOOSE_PARTIAL)
    env.stages.crash.clear()
    assert env.sweep() == 0
    assert [ln[:len(sweep.LOOSE_DONE)] for ln in env.loose_lines()] == [
        sweep.LOOSE_PARTIAL[:len(sweep.LOOSE_DONE)], sweep.LOOSE_DONE]


def test_no_loose_ends_flag_writes_no_line(env):
    env.queue(["10.1000/a1"])
    assert env.sweep("--no-loose-ends") == 0
    assert env.loose_lines() == []


def test_migrate_runs_for_this_run_id_or_is_printed(env, capsys):
    env.queue(["10.1000/a1"])
    assert env.sweep() == 0
    assert "migrate" not in env.stages.stages_called()
    out = capsys.readouterr().out
    assert f"--project P --date {DAY}" in out
    env.queue(["10.1000/a2"])
    assert env.sweep("--migrate") == 0
    cmd = next(c for s, c, _ in env.stages.calls if s == "migrate")
    assert cmd[cmd.index("--date") + 1] == f"{DAY}.2" and cmd[cmd.index("--project") + 1] == "P"


def test_run_wraps_run_pipeline_and_keeps_its_signature(env):
    params = list(inspect.signature(sweep.run_pipeline).parameters)
    assert params[:5] == ["project_dir", "queue_csv", "dry_run", "run_date", "skip_preprint"]
    env.queue(["10.1000/a1"])
    res = sweep.run(project="P", date=DAY)
    assert res["exit_code"] == 0
    assert res["projects"]["P"]["run_id"] == DAY
    assert res["projects"]["P"]["results"][0]["retired"] is True


def test_the_run_id_is_printed_for_wrappers(env, capsys):
    env.queue(["10.1000/a1"])
    env.sweep()
    env.queue(["10.1000/a2"])
    env.sweep()
    out = capsys.readouterr().out
    assert f"[sweep] run_id={DAY} project=P" in out
    assert f"[sweep] run_id={DAY}.2 project=P" in out


def test_artifact_dir_holds_the_run_and_counts_for_run_ids(env):
    """REG-I34: --artifact-dir keeps dated byproducts out of the project root; run ids stay
    unique across both places."""
    (env.pdir() / f"lit_pull_queue.{DAY}.report.csv").write_text("legacy", encoding="utf-8")
    env.queue(["10.1000/a1"])
    assert env.sweep("--artifact-dir", "lit_runs") == 0
    runs = env.pdir() / "lit_runs"
    assert (runs / f"lit_pull_queue.{DAY}.2.processed.csv").exists()
    assert (runs / f"lit_pull_queue.{DAY}.2.report.csv").exists()
    assert not list(env.pdir().glob(f"lit_pull_queue.{DAY}.2.*"))
    assert env.sweep("--artifact-dir", str(env.tmp / "abs")) == sweep.EXIT_USAGE   # needs --project


def test_bad_date_is_a_usage_error(env):
    env.queue(["10.1000/a1"])
    assert env.sweep(date="2026-9-3") == sweep.EXIT_USAGE
    assert datetime.date.fromisoformat(DAY)


# ---------------------------------------------------------------- W1 integration (dispatcher)
def test_migrate_command_carries_a_deliberate_preprint_skip():
    """A skipped preprint stage has no report; migrate must be told, or routing blocks on it."""
    import sweep as sw
    assert "--skip-preprint" in sw.migrate_command("K", "2026-09-30", None, True)
    assert "--skip-preprint" not in sw.migrate_command("K", "2026-09-30", None, False)
    cmd = sw.migrate_command("K", "2026-09-30.2", "arts", True)
    assert cmd[cmd.index("--artifact-dir") + 1] == "arts" and cmd[cmd.index("--date") + 1] == "2026-09-30.2"


def test_held_elsewhere_counts_only_pdf_holdings(tmp_path):
    """A DOI held in another library only as a text-only sidecar is still fetched (the PDF adds
    value); one held there as a PDF is HELD_ELSEWHERE."""
    import sweep as sw
    from litpipe import holdings as H

    class _Rec:
        def __init__(self, path, pdf):
            self.path, self.has_pdf = path, pdf

    class _Map:
        def __init__(self, recs):
            self._recs = recs
        def records(self, doi):
            return self._recs
        def where(self, doi):   # the old call site; must not be what decides
            return [r.path for r in self._recs]

    lib = tmp_path / "mine"
    lib.mkdir()
    other_sidecar = tmp_path / "other" / "a.fulltext.json"
    other_pdf = tmp_path / "other" / "a.pdf"
    assert sw._held_elsewhere(_Map([_Rec(other_sidecar, False)]), "10.1/a", lib) == []
    assert sw._held_elsewhere(_Map([_Rec(other_pdf, True)]), "10.1/a", lib) == [str(other_pdf)]
    assert H.HoldMap is not None
