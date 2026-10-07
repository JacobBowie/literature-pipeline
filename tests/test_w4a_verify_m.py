"""W4a verifier M: locks for the pool drawdown, the worklists over the real migrate lists, the paywall
queue and W4-0 (the walk verdict, now read by litpipe.runner; the 30-day wait for a host
refused until cleared).

The tests whose names follow an APPLY item (M-1 to M-5) pinned a defect found in verification: each
failed on 964c26f and passes with its fix, landed in the same commit. Temp roots and temp registries
only; nothing is sent.
"""
import csv
import datetime
import json
import sys
from pathlib import Path

import pytest

import build_priority_paywall_queue as bppq
import lit_util
import migrate_closed_to_md as mig
import sweep
from litpipe import config, holdings
from litpipe import worklists as WL
from tests.test_migrate_routing import chain, row

SIX = [f"10.7000/v.2021.{i}" for i in range(1, 7)]


def make_pool(path, dois):
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["doi", "title"])
        for d in dois:
            w.writerow([d, f"T {d}"])
    return path


def new_pool(path):
    return WL.Pool(path, holdings=holdings.HoldMap())


# ---------------------------------------------------------------- the pool under a kill at every step
STEPS = ["next_batch", "write_file", "mark_staged", "sweep", "mark_swept"]


class Killed(Exception):
    pass


def _batch(pool, run_id, d, staged, swept, kill=None):
    rows = pool.next_batch(2, exclude_held=False)
    if not rows:
        return False
    dois = [r["doi"] for r in rows]
    if kill == "next_batch":
        raise Killed
    bp = make_pool(d / f"batch_{run_id}.csv", dois)
    if kill == "write_file":
        raise Killed
    pool.mark_staged(dois, run_id, str(bp))
    staged.extend(dois)
    if kill == "mark_staged":
        raise Killed
    swept.extend(dois)
    if kill == "sweep":
        raise Killed
    pool.mark_swept(dois, run_id, {x: "fetched" for x in dois})
    if kill == "mark_swept":
        raise Killed
    return True


@pytest.mark.parametrize("kill", STEPS)
def test_a_kill_at_any_step_neither_restages_nor_loses_a_row(tmp_path, kill):
    p = make_pool(tmp_path / "pool.csv", SIX)
    staged, swept, n = [], [], iter(range(1, 99))
    pool = new_pool(p)
    _batch(pool, f"r{next(n)}", tmp_path, staged, swept)
    with pytest.raises(Killed):
        _batch(pool, f"r{next(n)}", tmp_path, staged, swept, kill=kill)
    resumed = new_pool(p)                                   # a restart: a NEW Pool, as the docstring says
    pend = resumed.pending()
    assert [len(b["dois"]) for b in pend] == ([2] if kill in ("mark_staged", "sweep") else [])
    for b in pend:
        assert b["exists"]
        swept.extend(b["dois"])
        resumed.mark_swept(b["dois"], b["run_id"], {x: "fetched" for x in b["dois"]})
    while _batch(resumed, f"r{next(n)}", tmp_path, staged, swept):
        pass
    assert sorted(set(staged)) == sorted(SIX) and len(staged) == len(SIX)       # never staged twice
    assert set(swept) == set(SIX)                                               # nothing lost
    st = resumed.status()
    assert (st["pending"], st["remaining"], st["swept"]) == (0, 0, 6)


def test_a_batch_whose_file_was_deleted_is_still_pending_with_exists_false(tmp_path):
    p = make_pool(tmp_path / "pool.csv", SIX)
    pool = new_pool(p)
    dois = [r["doi"] for r in pool.next_batch(2, exclude_held=False)]
    bp = make_pool(tmp_path / "b1.csv", dois)
    pool.mark_staged(dois, "r1", str(bp))
    bp.unlink()
    pend = new_pool(p).pending()
    assert pend == [{"run_id": "r1", "batch_path": str(bp), "dois": dois, "exists": False}]
    assert [r["doi"] for r in new_pool(p).next_batch(2, exclude_held=False)] == SIX[2:4]


def test_an_edited_pool_keeps_state_by_doi_case_and_url_form(tmp_path):
    p = make_pool(tmp_path / "pool.csv", SIX)
    pool = new_pool(p)
    pool.mark_staged(SIX[:2], "r1", str(tmp_path / "b1.csv"))
    pool.mark_swept(SIX[:1], "r1", {SIX[0]: "fetched"})
    # reordered, SIX[1] (pending) removed, a new top row, SIX[0] as an upper-case doi.org URL
    make_pool(p, ["10.7000/new.0", "https://doi.org/" + SIX[0].upper(), SIX[5], SIX[4], SIX[3], SIX[2]])
    again = new_pool(p)
    assert [r["doi"] for r in again.next_batch(3, exclude_held=False)] == ["10.7000/new.0", SIX[5], SIX[4]]
    assert [b["dois"] for b in again.pending()] == [[SIX[1]]]            # a removed row is still owed a sweep
    st = again.status()
    assert (st["pool_changed"], st["not_in_pool"], st["swept"]) == (True, 1, 1)


def test_seed_from_a_state_of_the_consumers_real_shape(tmp_path):
    """The consumer's state (recorded 2026-10-07 from a scratch copy): consumed int, batches of
    {file, n, pool_from, pool_to, rows: int}, staged_dois a list of str, plus build counters."""
    staged = [f"10.7100/s.{i}" for i in range(1, 12)]
    state = {"consumed": 10, "pool_builds": 4, "pool_rows": 20, "selection_rows": 10, "staged_dois": staged,
             "batches": [{"file": "b1.csv", "n": 1, "pool_from": 1, "pool_to": 10, "rows": 10}]}
    src = tmp_path / "_resp_pool_state.json"
    src.write_text(json.dumps(state), encoding="utf-8")
    p = make_pool(tmp_path / "pool.csv", staged[:3] + SIX[:2])
    pool = new_pool(p)
    pool.mark_staged([staged[0]], "pre", str(tmp_path / "pre.csv"))      # pending before the import
    first, again = pool.seed_from(src), pool.seed_from(src)
    assert (first["imported"], first["already_tracked"], again["already_seeded"]) == (10, 1, True)
    assert [b["dois"] for b in pool.pending()] == [[staged[0]]]          # the import never hides a pending row
    assert [r["doi"] for r in pool.next_batch(5, exclude_held=False)] == SIX[:2]


@pytest.mark.parametrize("content", ["{not json", "", "[]", '{"version": 1}'])
def test_an_unreadable_state_raises_and_is_never_reset(tmp_path, content):
    p = make_pool(tmp_path / "pool.csv", SIX)
    WL.state_path_for(p).write_text(content, encoding="utf-8")
    for call in (lambda q: q.next_batch(1, exclude_held=False), lambda q: q.pending(),
                 lambda q: q.mark_staged([SIX[0]], "r", "b")):
        with pytest.raises(WL.PoolStateError):
            call(new_pool(p))
    assert WL.state_path_for(p).read_text(encoding="utf-8") == content


def test_a_state_record_that_is_not_an_object_raises_pool_state_error(tmp_path):
    p = make_pool(tmp_path / "pool.csv", SIX)
    WL.state_path_for(p).write_text(json.dumps({"dois": {holdings.doi_key(SIX[0]): "staged"}}), encoding="utf-8")
    with pytest.raises(WL.PoolStateError):
        new_pool(p).next_batch(1, exclude_held=False)


def test_a_state_with_a_bom_loads(tmp_path):
    p = make_pool(tmp_path / "pool.csv", SIX)
    pool = new_pool(p)
    pool.mark_staged(SIX[:1], "r1", "b1.csv")
    sp = WL.state_path_for(p)
    sp.write_bytes(b"\xef\xbb\xbf" + sp.read_bytes())
    assert [r["doi"] for r in new_pool(p).next_batch(1, exclude_held=False)] == [SIX[1]]


def test_next_batch_writes_no_holdings_cache(tmp_path, monkeypatch):
    root = tmp_path / "root"
    lib = root / "teaching_a" / "literature"
    lib.mkdir(parents=True)
    (lib / "x.ris").write_text("TY  - JOUR\nDO  - 10.7000/v.2021.3\nER  - \n", encoding="utf-8")
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    state = tmp_path / "state"
    reg = {"state_dir": str(state), "projects": {"teaching_a": {"lib_dir": "literature"}}}
    p = make_pool(tmp_path / "pool.csv", SIX)
    assert [r["doi"] for r in WL.Pool(p, registry=reg).next_batch(2)] == SIX[:2]
    assert not (state / holdings.CACHE_NAME).exists()
    assert not state.exists()


def test_the_contract_marks_a_batch_staged_before_writing_its_file():
    doc = WL.__doc__
    assert doc.index("pool.mark_staged(") < doc.index("write the batch queue file from rows")


# ---------------------------------------------------------------- worklists over the real migrate lists
OA_LINE = "- [{m}] **A title** (2020) [{doi}](https://doi.org/{doi}) cause `{cause}` via `{via}`\n"
X = "10.1016/j.x.2020.01"


def _two_projects(tmp_path, monkeypatch, p_mark, q_mark):
    root = tmp_path / "root"
    for k in ("teaching_p", "research_q"):
        (root / k).mkdir(parents=True)
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    (root / "teaching_p" / WL.OA_BLOCKED_NAME).write_text(
        "# OA blocked\n\n" + OA_LINE.format(m=p_mark, doi=X, cause="HTTP_403", via="unpaywall:publisher"), encoding="utf-8")
    (root / "research_q" / WL.OA_BLOCKED_NAME).write_text(
        "# OA blocked\n\n" + OA_LINE.format(m=q_mark, doi=X, cause="HTML", via="pmc:ncbi-page"), encoding="utf-8")
    return {"projects": {"teaching_p": {"lib_dir": "literature"}, "research_q": {"lib_dir": "literature"}}}


def _open_everywhere(groups):
    return [r["doi"] for rows in groups.values() for r in WL._merge(rows)[0]]


def test_a_doi_blocked_in_two_projects_under_two_hosts_is_listed_once(tmp_path, monkeypatch):
    reg = _two_projects(tmp_path, monkeypatch, " ", " ")
    groups = WL.group_by_host(WL.oa_blocked(reg))
    assert _open_everywhere(groups) == [X]
    assert WL.render_oa_worklist(groups, "2026-10-07").count(f"[{X}]") == 1


def test_a_doi_checked_off_in_one_project_is_done_in_every_group(tmp_path, monkeypatch):
    reg = _two_projects(tmp_path, monkeypatch, "x", " ")
    groups = WL.group_by_host(WL.oa_blocked(reg))
    assert _open_everywhere(groups) == []


def test_worklists_read_the_real_migrate_lists_across_runs_a_tag_and_hand_checks(tmp_path, monkeypatch):
    root = tmp_path / "Projects"
    for k in ("P", "Q"):
        (root / k / "literature").mkdir(parents=True)
    cfg = {"state_dir": str(tmp_path / "state"),
           "projects": {"P": {"lib_dir": "literature"}, "Q": {"lib_dir": "literature"}}}
    cfgp = tmp_path / "projects.json"
    cfgp.write_text(json.dumps(cfg), encoding="utf-8")
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    monkeypatch.setattr(config, "CONFIG_PATH", cfgp)
    monkeypatch.setattr(mig, "CONFIG_PATH", cfgp)
    Y, Z, C1, C2, C3 = "10.1002/y.2019.02", "10.3390/z.2021.03", "10.1080/c.2018.1", "10.1080/c.2018.2", "10.1152/c.2018.3"
    u403 = {"oa_status": "OA", "attempts": "publisher/publishedVersion/HTTP_403", "error": "HTTP_403"}
    r403 = {"oa_status": "OA", "attempts": "repository/acceptedVersion/HTTP_403", "error": "HTTP_403"}

    def check_off(path, doi):
        path.write_text("\n".join(l.replace("- [ ]", "- [x]", 1) if doi in l else l
                                  for l in path.read_text(encoding="utf-8").split("\n")), encoding="utf-8")

    d1, d2 = datetime.date(2026, 9, 30), datetime.date(2026, 10, 1)
    chain(root / "P", "2026-09-30", [row(X, u=u403), row(Y, u=r403), row(C1)])
    mig.run("P", cfg=cfg, today=d1)
    check_off(root / "P" / mig.OA_BLOCKED_NAME, X)
    check_off(root / "P" / mig.ILL_NAME, C1)
    chain(root / "P", "2026-10-01", [row(X, u=u403), row(Z, u=u403), row(C1), row(C2)])
    mig.run("P", cfg=cfg, today=d2)
    chain(root / "P", "2026-10-01", [row(Y, u=r403), row(C3)], tag="extra")
    mig.run("P", cfg=cfg, today=d2, tags=["extra"])
    chain(root / "Q", "2026-10-01", [row(C2)])
    mig.run("Q", cfg=cfg, today=d2)

    rows = WL.oa_blocked(cfg)
    assert all(r["parsed"] for r in rows)
    groups = WL.group_by_host(rows)
    assert sorted(_open_everywhere(groups)) == sorted([Y, Z])
    assert [r["doi"] for rs in groups.values() for r in WL._merge(rs)[1]] == [X]
    import duckdb
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE project_cocitations(project VARCHAR, doi VARCHAR, n_own_citing INT)")
    con.execute("CREATE TABLE top_candidates(doi VARCHAR, n_seeds_pointing INT)")
    con.execute("CREATE TABLE paper_locations(doi VARCHAR)")
    con.executemany("INSERT INTO project_cocitations VALUES (?,?,?)", [("P", C2, 2), ("Q", C2, 5), ("P", C3, 3)])
    ill = WL.ill_list(cfg, con=con)
    con.close()
    assert [(r["doi"], r["projects"], r["n_own_citing"]) for r in ill] == [(C2, ["P", "Q"], 5), (C3, ["P"], 3)]


# ---------------------------------------------------------------- the paywall queue
def test_an_unreadable_residual_csv_is_not_silently_dropped(tmp_path, monkeypatch, capsys):
    root = tmp_path / "root"
    (root / "teaching_a" / "literature").mkdir(parents=True)
    bad = root / "teaching_a" / "lit_pull_queue.2026-09-01.residual.csv"
    bad.write_text("doi,title,year\n10.7000/closed.1,T,2020\n", encoding="utf-8")
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    real_open = open

    def locked(path, *a, **kw):
        if Path(path).name == bad.name:
            raise PermissionError(13, "The process cannot access the file", str(path))
        return real_open(path, *a, **kw)

    monkeypatch.setattr(WL, "open", locked, raising=False)
    reg = {"state_dir": str(tmp_path / "state"), "projects": {"teaching_a": {"lib_dir": "literature"}}}
    res = bppq.run(date="2026-10-07", db=str(tmp_path / "none.duckdb"), out_dir=str(tmp_path / "out"), registry=reg)
    assert res["residual"]["unreadable"] == [str(bad)]
    assert res["exit_code"] == 2


# ---------------------------------------------------------------- W4-0's walk verdict, on the runner (W4-A)
# run_daily no longer runs snowball: since W4-A it wraps litpipe.runner, which reads each stage child
# itself. The two W4-0 locks, moved onto that reader: a flood of undecodable output never stops it,
# and an exit without the stage's result (argparse's usage exit 2 included) is ERROR, never DEGRADED.
_FLOOD = ("import sys\n"
          "o, e = sys.stdout.buffer, sys.stderr.buffer\n"
          "for i in range(50000):\n"
          "    o.write(b'out ' + str(i).encode() + bytes([0xff, 0xfe, 0x80, 10]))\n"
          "    e.write(b'err ' + str(i).encode() + bytes([0xc3, 0x28, 10]))\n"
          "o.flush(); e.flush()\n"
          "print('# snowball: 1 project(s); P DEGRADED; exit 2', flush=True)\n"
          "sys.exit(2)\n")


def test_the_runners_log_reader_takes_a_flood_of_undecodable_output_without_a_deadlock(tmp_path):
    import subprocess
    from litpipe import runner
    p = subprocess.Popen([sys.executable, "-c", _FLOOD], stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                         stdin=subprocess.DEVNULL)
    pump = runner._Pump(p.stdout, tmp_path / "flood.log").start()
    assert p.wait(timeout=120) == 2
    pump.thread.join(60)
    pump.close()
    p.stdout.close()
    assert not pump.thread.is_alive() and pump.lines >= 100000
    assert "# snowball: 1 project(s); P DEGRADED; exit 2" in (tmp_path / "flood.log").read_text(encoding="utf-8")


@pytest.mark.parametrize("rc", [2, 1, 3], ids=["usage-exit-2", "exit-1", "exit-3"])
def test_an_exit_without_the_stages_result_is_error_never_degraded(rc):
    from litpipe import runner
    status, reason, _ = runner.classify("walk", runner.StageRun(rc, None, problem="no result file"))
    assert status == "ERROR" and f"shim exit {rc}" in reason


# ---------------------------------------------------------------- W4-0: the 30-day wait
ARXIV = "10.48550/arxiv.2101.00001"
NOT_IN_UPW = {"oa_status": "", "error": "NOT_IN_UNPAYWALL"}


@pytest.fixture
def one_project(tmp_path, monkeypatch):
    root = tmp_path / "Projects"
    (root / "P" / "literature").mkdir(parents=True)
    cfg = {"state_dir": str(tmp_path / "state"), "projects": {"P": {"lib_dir": "literature"}}}
    cfgp = tmp_path / "projects.json"
    cfgp.write_text(json.dumps(cfg), encoding="utf-8")
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    monkeypatch.setattr(config, "CONFIG_PATH", cfgp)
    monkeypatch.setattr(mig, "CONFIG_PATH", cfgp)
    return root / "P", cfg


def _route(proj, cfg, u, r):
    chain(proj, "2026-09-30", [row(ARXIV, u=u, r={"source": "arxiv", **r})], preprint=True)
    mig.run("P", cfg=cfg, today=datetime.date(2026, 9, 30))
    return {x["doi"]: x for x in mig.read_retry_later(proj)[1]}[ARXIV]


@pytest.mark.parametrize("u,r,want", [
    # an Unpaywall arXiv PDF location is PROHIBITED (terminal), so only the refused export API is open
    ({"oa_status": "OA", "attempts": "repository/submittedVersion/PROHIBITED", "error": "PROHIBITED"},
     {"status": "HOST_REFUSED:export.arxiv.org"}, "2026-10-30"),
    (NOT_IN_UPW, {"status": "HOST_REFUSED:WWW.ARXIV.ORG"}, "2026-10-30"),          # host case does not matter
    ({"oa_status": "", "error": "HTTP 503"}, {"status": "HOST_REFUSED:export.arxiv.org"}, "2026-10-03"),
], ids=["prohibited-arxiv-pdf", "upper-case-host", "unpaywall-outage-keeps-3-days"])
def test_the_manual_refusal_wait_by_row_shape(one_project, u, r, want):
    proj, cfg = one_project
    assert _route(proj, cfg, u, r)["not_before"] == want


def test_sweep_readmits_a_parked_row_on_its_30th_day_and_not_before(one_project):
    proj, cfg = one_project
    assert _route(proj, cfg, NOT_IN_UPW, {"status": "HOST_REFUSED:export.arxiv.org"})["not_before"] == "2026-10-30"
    assert sweep.admit_retries(proj, "2026-10-29")["admitted"] == 0
    assert sweep.admit_retries(proj, "2026-10-30")["admitted"] == 1
    assert mig.read_retry_later(proj)[1] == []
    with open(proj / "lit_pull_queue.retry.csv", encoding="utf-8", newline="") as f:
        assert [x["doi"] for x in csv.DictReader(f)] == [ARXIV]
