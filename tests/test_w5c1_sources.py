"""W5-C1 item 2: DEC-31 `sources` gate every sweep stage (decision A1, M073): Unpaywall and PMC as
well as the preprint servers, a one-run --sources override in sweep, migrate and the runner, and a
sources set with no sweep stage refused before fetching."""
import csv
import json
import types
from pathlib import Path

import pytest

import lit_util
import migrate_closed_to_md as mig
import sweep
from litpipe import canaries, config, runner

from tests.test_sweep_runs import DAY, Env, read_csv

REAL_CANARIES_RUN = canaries.run
FIX = Path(__file__).parent / "fixtures" / "W4-A"


@pytest.fixture
def env(tmp_path, monkeypatch):
    return Env(tmp_path, monkeypatch)


def hold(lib, doi, year, authors, title):
    """A PDF already in the library under its DEC-14 name, its sidecar naming the DOI."""
    import unpaywall_fetch_v2 as U
    fn = U.build_filename(year, authors, title)
    lib.mkdir(parents=True, exist_ok=True)
    (lib / fn).write_bytes(b"%PDF-1.4 held")
    (lib / (fn[:-4] + ".fulltext.json")).write_text(json.dumps({"doi": doi, "text": "x"}), encoding="utf-8")
    return fn


def arg(cmd, flag):
    return cmd[cmd.index(flag) + 1]


# ================================================================ sweep
def test_a_pmc_only_project_sends_nothing_to_unpaywall_and_names_pmc_files_by_dec14(env):
    import pmc_fetch
    import unpaywall_fetch_v2 as U
    env.register({"P": {"lib_dir": "lit", "sources": ["pmc"]}})
    held, new = "10.1000/held1", "10.1000/new1"
    env.queue([held, new])                                  # titles "Title 0" / "Title 1", Smith J, 2020
    hold(env.pdir() / "lit", held, "2020", "Smith J", "Title 0")
    env.stages.spec[new] = {"pmc": {"downloaded": "True", "skipped": "False", "error": ""}}
    assert env.sweep() == 0
    assert env.stages.stages_called() == ["pmc", "extract"]          # 0 Unpaywall requests: never launched
    pmc_cmd = next(c for s, c, _ in env.stages.calls if s == "pmc")
    pmc_in = Path(arg(pmc_cmd, "--report-in"))
    assert pmc_in.name == sweep.artifact_name("", DAY, "pmc_input")
    rows = {r["doi"]: r for r in read_csv(pmc_in)}
    assert rows[held]["oa_status"] == "SKIP_EXISTS"                   # held: PMC never re-fetches it
    assert rows[new]["filename"] == U.build_filename("2020", "Smith J", "Title 1")   # the DEC-14 name
    assert [r["doi"] for r in pmc_fetch.read_rows(str(pmc_in))] == [new]             # what the real stage reads
    rep = env.report()
    assert rep[("stage", "unpaywall_v2")]["detail"] == "skipped (the project's sources omit unpaywall)"
    assert rep[("skip_exists", "pmc_fetch")]["count"] == "1" and rep[("class", "fetched")]["count"] == "2"
    assert rep[("queue", "retired")]["detail"]                        # a skipped stage blocks nothing


def test_an_unpaywall_only_project_sends_nothing_to_ncbi_or_europe_pmc(env):
    env.register({"P": {"lib_dir": "lit", "sources": ["unpaywall"]}})
    env.queue(["10.1000/c1"])
    assert env.sweep() == 0
    assert env.stages.stages_called() == ["unpaywall", "extract"]     # no pmc_fetch, no preprint stage
    rep = env.report()
    assert rep[("stage", "pmc_fetch")]["detail"] == "skipped (the project's sources omit pmc)"
    res = env.residual()["10.1000/c1"]
    assert res["residual_class"] == "TERMINAL_CLOSED" and "pmc" in res["skipped_sources"].split(";")
    assert env.art("processed").exists()


def test_the_sources_override_wins_for_one_run(env, capsys):
    env.register({"P": {"lib_dir": "lit", "sources": ["pmc"]}})
    env.queue(["10.1000/o1"])
    assert env.sweep("--sources", "unpaywall,pmc") == 0
    assert env.stages.stages_called() == ["unpaywall", "pmc", "extract"]
    assert "--sources pmc,unpaywall" in capsys.readouterr().out        # migrate is told the run's sources
    env.stages.calls.clear()
    env.queue(["10.1000/o2"])
    assert env.sweep() == 0
    assert env.stages.stages_called() == ["pmc", "extract"]            # the next run: the project's own
    assert sweep.migrate_command("P", DAY, sources={"pmc"})[-2:] == ["--sources", "pmc"]
    assert "--sources" not in sweep.migrate_command("P", DAY)


@pytest.mark.parametrize("sources", [[], ["openalex_content"]])
def test_sources_with_no_sweep_stage_keep_the_queue_before_fetching(env, sources):
    env.register({"P": {"lib_dir": "lit", "sources": sources}})
    q = env.queue(["10.1000/k1"])
    rl = env.pdir() / sweep.RETRY_LATER_FILE
    rl.write_text("doi,title,authors,year,destination,notes,not_before\n10.1000/d9,T,A,2020,lit,,2026-01-01\n",
                  encoding="utf-8")
    before = rl.read_bytes()
    assert env.sweep() == sweep.EXIT_QUEUE_REFUSED
    assert env.stages.calls == [] and q.exists() and rl.read_bytes() == before
    assert not list(env.pdir().glob(f"lit_pull_queue.{DAY}.*"))


def test_a_dry_run_reports_the_refusal_and_writes_nothing(env, capsys):
    env.register({"P": {"lib_dir": "lit", "sources": []}})
    env.queue(["10.1000/k2"])
    before = {p.name: p.stat().st_mtime_ns for p in env.pdir().iterdir()}
    assert env.sweep("--dry-run") == sweep.EXIT_QUEUE_REFUSED
    assert "name no sweep stage" in capsys.readouterr().out
    assert {p.name: p.stat().st_mtime_ns for p in env.pdir().iterdir()} == before and env.stages.calls == []


def test_an_invalid_sources_override_is_a_usage_error_before_anything_runs(env):
    env.queue(["10.1000/u1"])
    assert env.sweep("--sources", "unpaywall,nope") == sweep.EXIT_USAGE
    assert env.stages.calls == [] and env.pdir().joinpath("lit_pull_queue.csv").exists()


def test_a_project_without_a_sources_key_keeps_unpaywall_and_pmc(env):
    env.queue(["10.1000/d1"])
    assert env.sweep() == 0
    assert env.stages.stages_called() == ["unpaywall", "pmc", "extract"]


# ================================================================ migrate
def _legacy_row(doi="10.1/x", **missing):
    return {"doi": doi, "doi_norm": doi, "title": "T", "authors": "A", "signals": [], "missing": dict(missing)}


def test_migrate_gates_unpaywall_as_it_gates_pmc():
    r = _legacy_row(unpaywall="report_absent", pmc="report_absent")
    assert mig.classify_row(dict(r), sources={"unpaywall", "pmc"}) == mig.PENDING
    assert mig.classify_row(dict(r), sources={"pmc"}) == mig.PENDING               # pmc still missing
    assert mig.classify_row(dict(r), sources={"unpaywall"}) == mig.PENDING         # unpaywall still missing
    assert mig.classify_row(dict(r), sources={"osf"}) != mig.PENDING               # neither is enabled


def test_migrate_sources_override_reaches_the_legacy_classifier(tmp_path, monkeypatch):
    root = tmp_path / "Projects"
    proj = root / "P"
    proj.mkdir(parents=True)
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    cfg = {"projects": {"P": {"lib_dir": "lit"}}, "state_dir": str(tmp_path / "state")}
    with open(proj / f"lit_pull_queue.{DAY}.unpaywall.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["doi", "title", "oa_status", "downloaded", "error"])
        w.writerow(["10.1000/m1", "T", "CLOSED", "False", ""])
    # no PMC report: with pmc enabled the row is PENDING; with --sources unpaywall it routes (ILL)
    res = mig.run("P", run_id=DAY, cfg=cfg, use_holdings=False, dry_run=True)
    assert res["counts"] == {"PENDING": 1}
    res = mig.run("P", run_id=DAY, cfg=cfg, use_holdings=False, dry_run=True, sources="unpaywall")
    assert res["counts"] == {"TERMINAL_CLOSED": 1}
    bad = mig.run("P", run_id=DAY, cfg=cfg, use_holdings=False, dry_run=True, sources="nope")
    assert bad["status"] == "config"


# ================================================================ the runner
@pytest.fixture
def w(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(FIX))
    from w4a_world import World
    return World(tmp_path, monkeypatch)


DOIS = ["10.5555/src.0001", "10.5555/src.0002"]


def test_the_runner_passes_a_sources_override_to_sweep_and_route(w):
    w.register("research_a")
    w.queue("research_a", DOIS)
    w.set_fake("sweep", "research_a", classes={DOIS[1]: "TERMINAL_CLOSED"})
    assert runner.main(["run", "--profile", "every_run", "--sources", "pmc"]) == 0
    assert w.calls("sweep")[0]["sources"] == "pmc" and w.calls("route")[0]["sources"] == "pmc"


def test_an_invalid_runner_sources_list_exits_1_before_anything_runs(w):
    w.register("research_a")
    w.queue("research_a", DOIS)
    assert runner.main(["run", "--profile", "every_run", "--sources", "unpaywall,bogus"]) == 1
    assert runner.main(["batch", "--project", "research_a", "--pool", "x.csv", "--sources", "bogus"]) == 1
    assert w.calls() == [] and not (w.state_dir / "runner").exists()
    assert [r for r in w.runs() if r["kind"] == "runner"] == []          # nothing registered


def test_the_local_context_expects_unpaywall_only_when_it_ran(w):
    w.register("research_a", sources=["pmc"])
    cfg = json.loads(w.cfg_path.read_text(encoding="utf-8"))
    r = runner._ScheduledRun(cfg, ["research_a"])
    p = r.projects[0]
    res = [{"tag": "", "run_id": DAY, "retired": True,
            "stages": {"unpaywall": "skipped", "pmc": "completed", "preprint": "skipped"}}]
    (p.root / sweep.artifact_name("", DAY, "pmc")).write_text("doi\n", encoding="utf-8")
    p.sweep = {"exit": 0, "status": "OK", "run_id": DAY, "results": res, "refused": []}
    p.route_status = "ok"
    stages = r._local_ctx()["projects"][0]["stages"]
    assert "unpaywall" not in stages and stages[:3] == ["residual", "report", "pmc"]
    res[0]["stages"]["unpaywall"] = "failed"
    assert r._local_ctx()["projects"][0]["stages"][0] == "unpaywall"


def test_a_pmc_only_night_passes_lost_artifacts_with_the_real_sweep_and_migrate(w, monkeypatch):
    monkeypatch.setitem(runner.STAGE_MODULES, "sweep", "sweep")
    monkeypatch.setitem(runner.STAGE_MODULES, "route", "migrate_closed_to_md")

    def canaries_local_real(profile, *, phase="all", context=None, cfg=None, **kw):
        w.canary_calls.append({"phase": phase, "context": context})
        return REAL_CANARIES_RUN(profile, phase=phase, context=context, cfg=cfg) if phase == "local" else []
    monkeypatch.setattr(canaries, "run", canaries_local_real)
    w.register("research_a", sources=["pmc"])
    w.queue("research_a", DOIS)
    seen = []

    def stage(cmd):
        cmd = [str(c) for c in cmd]
        script = Path(cmd[1]).name
        seen.append(script)
        if script == "pmc_fetch.py":
            with open(arg(cmd, "--report-in"), encoding="utf-8") as f:
                rows = [r for r in csv.DictReader(f) if r["oa_status"] != "SKIP_EXISTS"]
            Path(arg(cmd, "--report-out")).write_text(
                "doi,downloaded,error,pmcid,filename\n" + f"{rows[0]['doi']},True,,PMC1,{rows[0]['filename']}\n"
                + "".join(f"{r['doi']},False,NO_PMCID,,{r['filename']}\n" for r in rows[1:]), encoding="utf-8")
        return types.SimpleNamespace(returncode=0, stdout="", stderr="")
    monkeypatch.setattr(sweep, "_run_stage", stage)
    code = runner.main(["run", "--profile", "every_run"])
    s = w.summaries()[-1]
    assert seen == ["pmc_fetch.py", "extract_pdf_fulltext.py"]          # no Unpaywall stage
    loc = next(c for c in w.canary_calls if c["phase"] == "local")["context"]["projects"][0]
    assert "unpaywall" not in loc["stages"] and "routing" in loc["stages"]
    lost = [c for c in s["canaries"]["checks"] if c["id"] == "lost_artifacts"]
    assert lost and all(c["status"] == "PASS" for c in lost), lost
    assert code == 0, s["reasons"]
