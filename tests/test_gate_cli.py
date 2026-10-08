"""litpipe.gate at its edges: the CLI and run(), run folders, determinism, the dry run, promote, the
drawn set, holdings (injected snapshot and a temp-registry HoldMap), zero-input refusals, reports and
the worked example spec. Everything lives under tmp_path with a temp registry."""
import csv
import json
import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest

import lit_util
from litpipe import config, gate, worklists

FIX = Path(__file__).parent / "fixtures" / "gate"
EXAMPLE = Path(__file__).resolve().parent.parent / "examples" / "gate_spec.example.json"
PROJECT = "teaching_demo"


def hold(lib, stem, doi):
    """A content holding: a .ris with a DO line and the PDF beside it."""
    (lib / f"{stem}.ris").write_text(f"TY  - JOUR\nTI  - Fixture\nDO  - {doi}\nER  - \n", encoding="utf-8")
    (lib / f"{stem}.pdf").write_bytes(b"%PDF-1.4\n% fixture\n")


@pytest.fixture
def proj(tmp_path, monkeypatch):
    root = tmp_path / "Projects"
    lib = root / PROJECT / "literature"
    other = root / "research_other" / "literature"
    lib.mkdir(parents=True)
    other.mkdir(parents=True)
    shutil.copy(FIX / "harvest_small.csv", lib / "_demo_forward_citations.csv")
    shutil.copy(FIX / "seeds_small.json", lib / "_demo_forward_seeds.json")
    reg = {"state_dir": str(tmp_path / "state"),
           "projects": {PROJECT: {"lib_dir": "literature"}, "research_other": {"lib_dir": "literature"}}}
    cfgp = tmp_path / "projects.json"
    cfgp.write_text(json.dumps(reg), encoding="utf-8")
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    monkeypatch.setattr(config, "CONFIG_PATH", cfgp)
    for i in range(1, 4):
        hold(lib, f"held_{i}", f"10.5555/held.{i}")
    hold(lib, "frost_4", "10.5555/frost.4")
    hold(other, "drought_2", "10.5555/drought.2")
    snap = tmp_path / "held_as_of.txt"
    snap.write_text("# held at a past run (a comment: the whole line starts with #)\n10.5555/frost.4\n"
                    "  10.5555/rain.4  \n10.1002/(sici)1098-240x(200002)23:1#\n\n", encoding="utf-8")
    return SimpleNamespace(root=root, lib=lib, other=other, reg=reg, tmp=tmp_path, snap=snap)


def run(command="plan", spec=EXAMPLE, **kw):
    kw.setdefault("quiet", True)
    return gate.run(command, project=PROJECT, spec=str(spec) if isinstance(spec, Path) else spec, **kw)


def example(**over):
    d = json.loads(EXAMPLE.read_text(encoding="utf-8"))
    d.update(over)
    return d


def tree(path):
    return sorted((str(p.relative_to(path)), p.stat().st_size) for p in Path(path).rglob("*"))


def dois(rows):
    return [r["doi"] for r in rows]


# ---------------------------------------------------------------- the worked example
def test_example_spec_runs_on_the_fixture_harvest_with_live_project_holdings(proj):
    res = run(registry=proj.reg)
    assert res["exit_code"] == 0, res["error"]
    rows, rep = res["rows"], res["report"]
    assert dois(rows["selection"]) == ["10.5555/drought.2", "10.5555/rain.1", "10.5555/frost.1", "10.5555/mixed.2",
                                       "10.5555/rain.2", "10.5555/frost.3"]
    assert [r["assigned_topic"] for r in rows["selection"]] == ["drought", "rain", "frost"] * 2
    assert dois(rows["lanes"]["older"]) == ["10.5555/frost.old", "10.5555/frost.old2"]
    assert rep["holdings"]["mode"] == "live" and rep["holdings"]["scope"] == "project"
    assert rep["holdings"]["controls"]["dois_in_scope"] == 4
    assert rep["funnel"]["reasons"]["held"] == 1 and rep["funnel"]["candidates"] == 12
    assert rep["funnel"]["rows_no_doi"] == 1 and rep["funnel"]["reasons"]["excluded:lab_only"] == 1
    assert rep["chapters"]["budgets"] == {"Unit1": {"budget": 4, "taken": 4, "short": 0},
                                          "Unit2": {"budget": 2, "taken": 2, "short": 0}}
    assert rep["direction_splits"][0]["counts"] == {"harms": 0, "helps": 1, "unstated": 1}
    assert any("direction split drought" in w for w in rep["warnings"])
    assert rep["seeds"]["counts"] == {"OK": 2, "S2_RESOLVE_FAIL": 1, "TRUNCATED": 1}
    per = {s["seed"]: s for s in rep["seeds"]["per_seed"]}
    assert per["10.5555/seed.2"]["status"] == "TRUNCATED" and per["10.5555/seed.4"]["rows"] == 0
    assert rep["coverage"]["main"]["held_now"] == 0 and rep["coverage"]["older"]["rows"] == 2
    assert rows["selection"][0]["notes"] == "gate=demo; lane=main; topic=drought; n_seeds=2; ch=Unit1;Unit2"


def test_scope_portfolio_counts_another_project_library_as_held(proj):
    spec = example(holdings={"filter": True, "scope": "portfolio"})
    res = run(spec=spec, registry=proj.reg)
    assert res["exit_code"] == 0, res["error"]
    assert "10.5555/drought.2" not in dois(res["rows"]["selection"])
    assert res["report"]["funnel"]["reasons"]["held"] == 2 and res["report"]["holdings"]["n"] == 5


def test_as_of_snapshot_is_exact_string_with_whole_line_comments(proj):
    assert gate.read_snapshot(proj.snap) == {"10.5555/frost.4", "10.5555/rain.4",
                                             "10.1002/(sici)1098-240x(200002)23:1#"}
    res = run(holdings_as_of=str(proj.snap))
    assert res["exit_code"] == 0, res["error"]
    rep = res["report"]
    assert rep["holdings"]["mode"] == "as_of" and rep["holdings"]["n"] == 3
    assert rep["funnel"]["reasons"]["held"] == 2 and rep["coverage"] == {"note": "no live holdings in this run"}


def test_holdings_filter_false_ignores_a_snapshot(proj):
    res = run(spec=example(holdings={"filter": False}), holdings_as_of=str(proj.snap))
    assert res["report"]["holdings"]["mode"] == "off" and "held" not in res["report"]["funnel"]["reasons"]


# ---------------------------------------------------------------- dry run, write, determinism
def test_the_dry_run_writes_nothing(proj, capsys):
    before = tree(proj.tmp)
    res = run(holdings_as_of=str(proj.snap), quiet=False)
    assert res["exit_code"] == 0 and res["run_dir"] is None
    assert tree(proj.tmp) == before
    assert "dry run: nothing written" in capsys.readouterr().out


def test_write_creates_an_immutable_run_folder(proj):
    res = run(holdings_as_of=str(proj.snap), write=True)
    rd = Path(res["run_dir"])
    assert rd.parent == proj.lib / "_gate" / "demo"
    assert sorted(p.name for p in rd.iterdir()) == ["drops.csv", "lane_older.csv", "manifest.json", "pool.csv",
                                                    "report.json", "report.md", "selection.csv"]
    for p in rd.iterdir():
        b = p.read_bytes()
        assert not b.startswith(b"\xef\xbb\xbf") and b"\r\n" not in b
    with open(rd / "selection.csv", encoding="utf-8", newline="") as f:
        sel = list(csv.DictReader(f))
    assert list(sel[0])[0] == "doi" and len(sel) == 6
    man = json.loads((rd / "manifest.json").read_text(encoding="utf-8"))
    assert man["holdings"]["mode"] == "as_of" and len(man["holdings"]["sha256"]) == 64
    assert man["inputs"]["harvest"]["rows"] == 24 and man["drawn"]["n"] == 0
    assert man["files"]["selection.csv"] and man["outputs"] == {"selection": "selection.csv",
                                                                "lanes": {"older": "lane_older.csv"}}
    assert man["spec_sha256"] == gate.load_spec(EXAMPLE).sha256 and man["engine_version"] == gate.ENGINE_VERSION
    drops = (rd / "drops.csv").read_text(encoding="utf-8")
    assert "excluded:lab_only" in drops and "over_cap:" in drops and ",plan\n" in drops


def test_two_runs_write_byte_identical_files(proj):
    a = Path(run(holdings_as_of=str(proj.snap), write=True, argv=["plan"])["run_dir"])
    b = Path(run(holdings_as_of=str(proj.snap), write=True, argv=["plan"])["run_dir"])
    assert a != b
    for p in sorted(a.iterdir()):
        assert p.read_bytes() == (b / p.name).read_bytes(), p.name


def test_out_root_and_json(proj):
    out = proj.tmp / "elsewhere"
    js = proj.tmp / "rep" / "report.json"
    res = run(holdings_as_of=str(proj.snap), write=True, out_root=str(out), json_path=str(js))
    assert Path(res["run_dir"]).parent == out / "_gate" / "demo" and not (proj.lib / "_gate").exists()
    assert json.loads(js.read_text(encoding="utf-8"))["plan"]["selected"] == 6


def test_build_writes_no_selection(proj):
    res = run("build", holdings_as_of=str(proj.snap), write=True)
    names = sorted(p.name for p in Path(res["run_dir"]).iterdir())
    assert names == ["drops.csv", "manifest.json", "pool.csv", "report.json", "report.md"]
    assert "plan" not in res["report"]


# ---------------------------------------------------------------- refusals and errors
def test_zero_input_refusals_exit_2_with_a_summary_line(proj, capsys):
    (proj.lib / "_demo_forward_citations.csv").write_text(
        (FIX / "harvest_small.csv").read_text(encoding="utf-8").splitlines()[0] + "\n", encoding="utf-8")
    res = run(holdings_as_of=str(proj.snap), quiet=False)
    assert res["exit_code"] == 2 and res["reasons"] == ["zero_input:harvest"]
    assert '[step-summary] {"aborted": null, "reasons": ["zero_input:harvest"]' in capsys.readouterr().out


def test_zero_input_snapshot_and_live_holdings(proj):
    empty = proj.tmp / "empty.txt"
    empty.write_text("# only a comment\n\n", encoding="utf-8")
    res = run(holdings_as_of=str(empty))
    assert res["exit_code"] == 2 and res["reasons"] == ["zero_input:snapshot"]
    for p in proj.lib.glob("held_*"):
        p.unlink()
    res = run(registry=proj.reg)
    assert res["exit_code"] == 2 and res["reasons"] == ["zero_input:holdings"]


def test_live_holdings_controls(proj):
    from litpipe import holdings
    # litpipe.doi treats the fabricated DOI as a placeholder, so no .ris can make the map list it;
    # a broken map (injected) is what this control guards against
    hm = holdings.build(proj.reg, use_cache=False, write_cache=False)
    recs = [h for v in hm._by_doi.values() for h in v]
    broken = holdings.HoldMap(recs + [holdings.Holding(gate.ABSENT_DOI, proj.lib / "x.pdf", PROJECT, proj.lib,
                                                       holdings.PDF)])
    res = run(registry=proj.reg, holdmap=broken)
    assert res["exit_code"] == 2 and "fabricated DOI" in res["error"]
    (proj.lib / "aa_unlisted.ris").write_text("DO  - 10.5555/not.in.the.map\n", encoding="utf-8")
    res = run(registry=proj.reg, holdmap=holdings.HoldMap(recs))
    assert res["exit_code"] == 2 and "aa_unlisted.ris names" in res["error"]
    (proj.lib / "aa_unlisted.ris").unlink()
    for p in proj.lib.glob("*.ris"):
        p.write_text("TY  - JOUR\nER  - \n", encoding="utf-8")
    for i in range(4):
        hold(proj.other, f"o{i}", f"10.5555/other.{i}")
    res = run(spec=example(holdings={"filter": True, "scope": "portfolio"}), registry=proj.reg)
    assert res["exit_code"] == 2 and "with a DO line, fewer than 4" in res["error"]


def test_controls_failure_exits_2(proj):
    spec = example()
    spec["topics"][0]["controls"]["hit"].append("Snowfall")
    res = run(spec=spec, holdings_as_of=str(proj.snap))
    assert res["exit_code"] == 2 and res["reasons"] == ["controls"]


@pytest.mark.parametrize("change, msg", [
    (lambda p: (p.lib / "_demo_forward_citations.csv").unlink(), "cannot read the harvest"),
    (lambda p: (p.lib / "_demo_forward_citations.csv").write_text("citing_doi,citing_title\n10.1/a,x\n",
                                                                  encoding="utf-8"), "lacks column"),
    (lambda p: (p.lib / "_demo_forward_citations.csv").write_text(
        "citing_doi,citing_title,citing_year,citing_cited_by\n10.1/a,x,2020,1\n", encoding="utf-8"),
     "none of the seed columns"),
])
def test_input_errors_exit_1(proj, change, msg):
    change(proj)
    res = run(holdings_as_of=str(proj.snap))
    assert res["exit_code"] == 1 and msg in res["error"]


def test_registry_and_spec_errors_exit_1(proj, tmp_path):
    res = gate.run("plan", project="research_unknown", spec=str(EXAMPLE), registry=proj.reg, quiet=True)
    assert res["exit_code"] == 1 and "not registered" in res["error"]
    bad = tmp_path / "bad.json"
    bad.write_text('{"schema": "litpipe.gate/1", "name": "x", "name": "y"}', encoding="utf-8")
    res = run(spec=bad)
    assert res["exit_code"] == 1 and "duplicate JSON key" in res["error"]
    res = gate.run("plan", project=PROJECT, spec=None, quiet=True)
    assert res["exit_code"] == 1


def test_lib_dir_replaces_the_registry_library(proj, tmp_path):
    other = tmp_path / "loose_lib"
    other.mkdir()
    shutil.copy(FIX / "harvest_small.csv", other / "_demo_forward_citations.csv")
    res = gate.run("plan", project="research_unregistered", spec=str(EXAMPLE), lib_dir=str(other),
                   holdings_as_of=str(proj.snap), registry={"projects": {}}, quiet=True)
    assert res["exit_code"] == 0, res["error"]
    assert res["report"]["seeds"]["status"] == "UNKNOWN"


# ---------------------------------------------------------------- annotate and seeds manifests
def test_annotate_reads_the_listed_dois_skipping_comment_lines(proj):
    (proj.lib.parent / "wiki_list.csv").write_text(
        "# a comment line\n# another\ndoi,title\n10.5555/RAIN.1,x\n10.5555/frost.old,y\n", encoding="utf-8")
    spec = example(annotate=[{"column": "in_list", "dois_from": "../wiki_list.csv"}])
    res = run(spec=spec, holdings_as_of=str(proj.snap))
    assert res["report"]["annotations"]["in_list"] == {"records": 2, "candidates": 1, "selected": 1}
    lane = {r["doi"]: r["in_list"] for r in res["rows"]["lanes"]["older"]}
    assert lane["10.5555/frost.old"] == "y"
    bad = example(annotate=[{"column": "in_list", "dois_from": "../wiki_list.csv", "doi_column": "id"}])
    assert run(spec=bad, holdings_as_of=str(proj.snap))["exit_code"] == 1


def test_seeds_manifest_shapes(tmp_path):
    assert gate.read_seeds_manifest(None)["status"] == "UNKNOWN"
    assert gate.read_seeds_manifest(tmp_path / "missing.json")["status"] == "UNKNOWN"
    j = tmp_path / "walk.partial.jsonl"
    j.write_text(json.dumps({"journal": "forward_citations", "version": 1}) + "\n"
                 + json.dumps({"doi": "10.5555/seed.1", "state": "walked"}) + "\n"
                 + json.dumps({"doi": "10.5555/seed.2", "state": "failed"}) + "\n{torn", encoding="utf-8")
    got = gate.read_seeds_manifest(j)
    assert got["status"] == "READ" and got["counts"] == {"FAILED": 1, "WALKED": 1}
    other = tmp_path / "x.json"
    other.write_text("[1, 2]", encoding="utf-8")
    assert gate.read_seeds_manifest(other)["status"] == "UNKNOWN"


# ---------------------------------------------------------------- plan --pool
def test_plan_from_a_written_pool_matches_and_skips_year_lanes(proj):
    res = run(holdings_as_of=str(proj.snap), write=True)
    pool = Path(res["run_dir"]) / "pool.csv"
    again = run(pool=str(pool), holdings_as_of=str(proj.snap))
    assert again["exit_code"] == 0, again["error"]
    assert dois(again["rows"]["selection"]) == dois(res["rows"]["selection"])
    assert again["report"]["holdings"]["mode"] == "not_applied" and again["report"]["funnel"]["from_pool"]
    assert any("lane older" in w for w in again["warnings"]) and again["rows"]["lanes"] == {}
    bad = proj.tmp / "bad_pool.csv"
    bad.write_text("doi,title\n10.1/a,x\n", encoding="utf-8")
    assert run(pool=str(bad))["exit_code"] == 1


# ---------------------------------------------------------------- the drawn set and promote
def _promoted(proj, **kw):
    res = run(holdings_as_of=str(proj.snap), write=True)
    rid = Path(res["run_dir"]).name
    return res, rid, gate.run("promote", project=PROJECT, spec=str(EXAMPLE), run_id=rid, quiet=True, **kw)


def test_promote_copies_the_selection_and_lanes_to_the_stable_paths(proj):
    res, rid, pr = _promoted(proj)
    assert pr["exit_code"] == 0, pr["error"]
    sel, lane = proj.lib / "_demo_gate_selection.csv", proj.lib / "_demo_gate_lane_older.csv"
    rd = Path(res["run_dir"])
    assert sel.read_bytes() == (rd / "selection.csv").read_bytes()
    assert lane.read_bytes() == (rd / "lane_older.csv").read_bytes()
    assert [r["doi"] for r in worklists.Pool(sel).rows()] == dois(res["rows"]["selection"])


def test_promote_refuses_pending_batches_unless_forced(proj):
    res, rid, pr = _promoted(proj)
    sel = proj.lib / "_demo_gate_selection.csv"
    worklists.Pool(sel).mark_staged([res["rows"]["selection"][0]["doi"]], "run-1", str(proj.tmp / "b.csv"))
    again = gate.run("promote", project=PROJECT, spec=str(EXAMPLE), run_id=rid, quiet=True)
    assert again["exit_code"] == 2 and "pending batches" in again["error"]
    forced = gate.run("promote", project=PROJECT, spec=str(EXAMPLE), run_id=rid, force=True, quiet=True)
    assert forced["exit_code"] == 0 and forced["pending_overridden"]


def test_promote_refuses_files_that_would_share_a_runner_batch_tag(proj):
    # one gate's own names clash at load since the batch-tag check (verifier R-4); two gates of one
    # project whose long names truncate to one tag are still caught at promote
    first, second = example(name="a" * 30, lanes=[]), example(name="a" * 31, lanes=[])
    for spec in (first, second):
        res = run(spec=spec, holdings_as_of=str(proj.snap), write=True)
        pr = gate.run("promote", project=PROJECT, spec=spec, run_id=Path(res["run_dir"]).name, quiet=True)
    assert pr["exit_code"] == 1 and "share a runner batch tag" in pr["error"]
    assert (proj.lib / f"_{'a' * 30}_gate_selection.csv").exists()
    assert not (proj.lib / f"_{'a' * 31}_gate_selection.csv").exists()


def test_promote_of_an_unknown_run_exits_1(proj):
    pr = gate.run("promote", project=PROJECT, spec=str(EXAMPLE), run_id="19990101T000000Z-x", quiet=True)
    assert pr["exit_code"] == 1 and "manifest.json" in pr["error"]


def test_rebuild_excludes_the_drawn_set_from_the_selection_and_the_lanes(proj):
    res, rid, pr = _promoted(proj)
    sel, lane = proj.lib / "_demo_gate_selection.csv", proj.lib / "_demo_gate_lane_older.csv"
    first, second = res["rows"]["selection"][0]["doi"], res["rows"]["selection"][1]["doi"]
    pool = worklists.Pool(sel)
    pool.mark_staged([first, second], "run-1", str(proj.tmp / "b.csv"))
    pool.mark_swept([first], "run-1", {first: "fetched"})
    worklists.Pool(lane).mark_staged(["10.5555/frost.old"], "run-2", str(proj.tmp / "c.csv"))
    again = run(holdings_as_of=str(proj.snap))
    assert again["exit_code"] == 0, again["error"]
    picked = set(dois(again["rows"]["selection"])) | set(dois(again["rows"]["lanes"]["older"]))
    assert not picked & {first, second, "10.5555/frost.old"}
    rep = again["report"]
    assert rep["drawn"]["n"] == 3 and rep["drawn"]["swept_classes"] == {"fetched": 1}
    # frost.old is a main-build record too (dropped there for its year), so all three carry `drawn`
    assert rep["drawn"]["staged_unswept"] == 2 and rep["funnel"]["reasons"]["drawn"] == 3
    assert dois(again["rows"]["lanes"]["older"]) == ["10.5555/frost.old2", "10.5555/rain.old"]


def test_a_promoted_pool_stays_byte_identical_and_seeded_dois_count_as_drawn(proj):
    """The stable path is immutable under drawdown (Pool writes only its .drawdown.json), and a
    state imported with Pool.seed_from (a consumer's staged_dois list) is drawn at the next build."""
    from litpipe import holdings
    res, rid, pr = _promoted(proj)
    sel = proj.lib / "_demo_gate_selection.csv"
    before = sel.read_bytes()
    legacy = proj.tmp / "legacy_state.json"
    legacy.write_text(json.dumps({"staged_dois": ["10.5555/rain.1", "10.5555/frost.1"]}), encoding="utf-8")
    pool = worklists.Pool(sel, holdings=holdings.HoldMap())
    assert pool.seed_from(legacy)["imported"] == 2
    batch = pool.next_batch(2)
    pool.mark_staged([r["doi"] for r in batch], "run-1", str(proj.tmp / "b.csv"))
    pool.mark_swept([batch[0]["doi"]], "run-1", {batch[0]["doi"]: "fetched"})
    assert sel.read_bytes() == before
    again = run(holdings_as_of=str(proj.snap))
    assert again["report"]["drawn"]["swept_classes"] == {"fetched": 1, "imported": 2}
    assert not set(dois(again["rows"]["selection"])) & ({"10.5555/rain.1", "10.5555/frost.1"}
                                                        | {r["doi"] for r in batch})


def test_an_unreadable_or_foreign_pool_state_exits_1(proj):
    st = worklists.state_path_for(proj.lib / "_demo_gate_selection.csv")
    st.write_text(json.dumps({"version": 99, "dois": {}}), encoding="utf-8")
    res = run(holdings_as_of=str(proj.snap))
    assert res["exit_code"] == 1 and "version 99" in res["error"]
    st.write_text("{not json", encoding="utf-8")
    assert run(holdings_as_of=str(proj.snap))["exit_code"] == 1


def test_injected_drawn_set(proj):
    res = run(holdings_as_of=str(proj.snap), drawn={"https://doi.org/10.5555/DROUGHT.2"})
    assert "10.5555/drought.2" not in dois(res["rows"]["selection"])
    assert res["report"]["drawn"]["injected"] is True


# ---------------------------------------------------------------- main()
def test_main_usage_errors_exit_1_and_help_exits_0(proj, capsys):
    assert gate.main(["plan", "--project", PROJECT]) == 1
    assert gate.main(["bogus"]) == 1
    assert gate.main(["--help"]) == 0
    capsys.readouterr()


def test_main_report_prints_markdown_and_plan_prints_the_funnel(proj, capsys):
    assert gate.main(["report", "--project", PROJECT, "--spec", str(EXAMPLE), "--holdings-as-of", str(proj.snap)]) == 0
    out = capsys.readouterr().out
    assert "# Gate report: demo (report)" in out and "| rain |" in out
    assert gate.main(["plan", "--project", PROJECT, "--spec", str(EXAMPLE), "--holdings-as-of", str(proj.snap)]) == 0
    out = capsys.readouterr().out
    assert "gate demo (plan): 24 harvest rows" in out and "lane older: 2 of 3 candidates" in out


def test_main_reads_sys_argv(proj, monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["gate", "build", "--project", PROJECT, "--spec", str(EXAMPLE),
                                     "--holdings-as-of", str(proj.snap)])
    assert gate.main() == 0
    assert "candidates" in capsys.readouterr().out


def test_main_promote_round_trip(proj):
    res = run(holdings_as_of=str(proj.snap), write=True)
    rid = Path(res["run_dir"]).name
    assert gate.main(["promote", "--project", PROJECT, "--spec", str(EXAMPLE), "--run", rid]) == 0
    assert (proj.lib / "_demo_gate_selection.csv").exists()


def test_minimum_none_prints_a_warning(proj, capsys):
    spec = example(controls={"minimum": "none", "match_none": ["A survey of tractor maintenance"]})
    for t in spec["topics"]:
        t.pop("controls", None)
    spec["exclude"][0].pop("controls")
    res = run(spec=spec, holdings_as_of=str(proj.snap), quiet=False)
    assert res["exit_code"] == 0
    assert "WARNING: controls.minimum is 'none'" in capsys.readouterr().out
