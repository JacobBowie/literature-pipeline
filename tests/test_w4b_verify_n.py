"""W4b verifier N: the runner's core (litpipe/runner.py at af45c48): isolation, one runner at a
time, the heartbeat and stage timeouts, the batch under a real kill at every point, the local
canaries' context, and the process tree.

Every real process here is either a stage child started by runner.subprocess_launcher with a temp
registry (the child runs a probe module written into tmp_path; the probes send nothing), or the
builder's harness tests/fixtures/W4-A/run_runner.py (temp registry, loopback-only transports,
stubbed preflight and canaries), wrapped to kill itself at a batch checkpoint. Nothing starts
`python -m litpipe.runner run|batch` bare.

The tests named for findings N-1 to N-5 lock them: each failed on af45c48 and passes with its fix,
landed in the same commit."""
import csv
import json
import os
import subprocess
import sys
import threading
import time as _rt
import types
from pathlib import Path

import pytest

FIX = Path(__file__).resolve().parent / "fixtures" / "W4-A"
sys.path.insert(0, str(FIX))

from litpipe import canaries, preflight, runner, state, worklists  # noqa: E402
from w4a_world import REPO, World, stand_in_overrides  # noqa: E402

REAL_CANARIES_RUN = canaries.run          # captured at import: World replaces it per test
REAL_PREFLIGHT_RUN = preflight.run
REAL_STATE_DIR = Path.home() / ".local" / "db" / "literature_pipeline"
RUN_ID = "20261007T010000Z-runner-1-abcdef"
STAGE_MODULE_NAMES = ("sweep", "migrate_closed_to_md", "forward_citations", "reverse_citations",
                      "index_portfolio", "enrich_abstracts", "enrich_recommendations", "audit_portfolio",
                      "seed_queue_from_top_candidates", "snowball", "extract_pdf_fulltext",
                      "unpaywall_fetch_v2", "pmc_fetch", "preprint_fetch", "litpipe.enrich_s2",
                      "build_priority_paywall_queue")


# ------------------------------------------------------------------------------ helpers
def wait_for(pred, timeout=60.0, step=0.05):
    end = _rt.time() + timeout
    while _rt.time() < end:
        if pred():
            return True
        _rt.sleep(step)
    return False


def same(a, b):
    return os.path.normcase(os.path.abspath(str(a))) == os.path.normcase(os.path.abspath(str(b)))


def write_mods(tmp_path, monkeypatch, **mods):
    """Write probe modules into tmp_path/mods and put that folder on the children's PYTHONPATH."""
    d = tmp_path / "mods"
    d.mkdir(exist_ok=True)
    for name, src in mods.items():
        (d / f"{name}.py").write_text(src, encoding="utf-8")
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join([str(d), str(FIX)] + [
        p for p in os.environ.get("PYTHONPATH", "").split(os.pathsep) if p]))
    if str(d) not in sys.path:
        monkeypatch.syspath_prepend(str(d))
    return d


def temp_registry(tmp_path):
    reg = tmp_path / "reg" / "projects.json"
    reg.parent.mkdir(parents=True, exist_ok=True)
    (tmp_path / "Projects").mkdir(exist_ok=True)
    reg.write_text(json.dumps({"root": str(tmp_path / "Projects"), "state_dir": str(tmp_path / "state"),
                               "db_dir": str(tmp_path / "refs"),
                               "loose_ends": str(tmp_path / "notes" / "LOOSE_ENDS.md"), "projects": {}}),
                   encoding="utf-8")
    return reg


def make_step(tmp_path, reg, job, module, kwargs, *, timeout_s=120.0, hb=None):
    d = tmp_path / "run"
    return runner.Step(job, module, kwargs, None, float(timeout_s), d / "logs" / f"{module}.log",
                       d / "stages" / f"{module}.kwargs.json", d / "stages" / f"{module}.result.json", reg,
                       RUN_ID, hb)


class JumpClock:
    """time.time() plus an offset a test moves forward: what a process sees when the machine wakes
    from sleep (or the clock steps). Every other attribute is the real time module's."""

    def __init__(self):
        self.offset = 0.0

    def time(self):
        return _rt.time() + self.offset

    def __getattr__(self, name):
        return getattr(_rt, name)


# ================================================================ 1. isolation
def test_import_runner_in_a_fresh_interpreter_loads_no_stage_module():
    code = "import sys, json; import litpipe.runner; print(json.dumps(sorted(sys.modules)))"
    out = subprocess.run([sys.executable, "-c", code], cwd=str(REPO), capture_output=True, text=True,
                         timeout=180, env={**os.environ, "PYTHONUTF8": "1"})
    assert out.returncode == 0, out.stderr
    mods = set(json.loads(out.stdout.strip().splitlines()[-1]))
    assert not mods & set(STAGE_MODULE_NAMES), sorted(mods & set(STAGE_MODULE_NAMES))
    assert not REAL_STATE_DIR.exists()


PROBE = '''
import os
from pathlib import Path
CONFIG_PATH = Path("unbound")


def run(**kw):
    import lit_util
    from litpipe import config, ledger, state, walk
    import index_portfolio
    import snowball
    led = ledger.write({"host": "probe.invalid", "purpose": "probe", "method": "GET"})
    return {"exit_code": 0, "config": str(config.CONFIG_PATH), "own_config": str(CONFIG_PATH),
            "root": str(lit_util.PROJECTS_ROOT), "state_db": str(state.DB_PATH),
            "state_db_resolved": str(state.db_path(create=False)), "ledger_dir": str(ledger.LEDGER_DIR),
            "ledger_file": str(led), "state_dir": str(config.state_dir(create=False)),
            "db_dir": str(config.db_dir()), "walk_cache": str(walk.cache_path()),
            "index_db": str(index_portfolio.DB_PATH), "snowball_db": str(snowball.DB_PATH),
            "snowball_log": str(snowball.LOG_PATH), "run_env": os.environ.get("LITPIPE_RUN_ID"),
            "current_run": state.current_run(), "utf8": os.environ.get("PYTHONUTF8"),
            "unbuffered": os.environ.get("PYTHONUNBUFFERED"), "cwd": os.getcwd()}
'''


def test_the_shim_child_resolves_only_the_temp_registry_paths(tmp_path, monkeypatch):
    """A real child through the production launcher: every path the shim binds, and the module
    constants bound at import from lit_util.PROJECTS_ROOT, resolve under the temp registry."""
    write_mods(tmp_path, monkeypatch, w4bn_probe=PROBE)
    reg = temp_registry(tmp_path)
    sr = runner.subprocess_launcher(make_step(tmp_path, reg, "index", "w4bn_probe", {}))
    assert sr.result is not None, (sr.problem, Path(sr.log).read_text(encoding="utf-8"))
    r = sr.result
    sd = tmp_path / "state"
    assert same(r["config"], reg) and same(r["own_config"], reg)
    assert same(r["root"], tmp_path / "Projects")
    assert same(r["state_db"], sd / state.DB_NAME) and same(r["state_db_resolved"], sd / state.DB_NAME)
    assert same(r["ledger_dir"], sd / "ledger") and Path(r["ledger_file"]).parent == Path(r["ledger_dir"])
    assert same(r["state_dir"], sd) and same(r["db_dir"], tmp_path / "refs")
    assert same(r["walk_cache"], sd / "s2_cache.duckdb")
    for k in ("index_db", "snowball_db", "snowball_log"):          # bound at import from PROJECTS_ROOT
        assert Path(r[k]).is_relative_to(tmp_path / "Projects"), (k, r[k])
    assert r["run_env"] == RUN_ID and r["current_run"] == RUN_ID
    assert (r["utf8"], r["unbuffered"]) == ("1", "1") and same(r["cwd"], REPO)
    assert not REAL_STATE_DIR.exists()


# ================================================================ 2. one runner at a time
@pytest.fixture
def wr(tmp_path, monkeypatch):
    """A World whose harness processes run the stand-in stages in real children."""
    world = World(tmp_path, monkeypatch)
    world.overrides(**stand_in_overrides())
    return world


@pytest.mark.parametrize("pair", range(int(os.environ.get("W4BN_PAIRS", "1"))))
def test_two_real_runners_on_a_barrier_exactly_one_proceeds(wr, pair):
    w = wr
    w.register("research_a")
    w.queue("research_a", ["10.5555/w4bn.0001"])
    w.set_fake("sweep", "research_a", sleep=4)
    barrier = w.tmp / "barrier"
    env = {"LITPIPE_TEST_BARRIER": str(barrier)}
    procs = [w.spawn("run", "--profile", "every_run", env=env) for _ in range(2)]
    try:
        assert wait_for(lambda: len(list(w.tmp.glob("barrier.*.ready"))) == 2, timeout=120), w.harness_log()
        barrier.write_text("go", encoding="utf-8")
        codes = sorted(p.wait(timeout=180) for p in procs)
    finally:
        for p in procs:
            if p.poll() is None:
                p.kill()
    assert codes == [0, 3], w.harness_log()
    runs = [r for r in w.runs() if r["kind"] == "runner"]
    loser = [r for r in runs if r["status"].startswith("refused")]
    winner = [r for r in runs if r["status"] == "ok"]
    assert len(loser) == 1 and len(winner) == 1, runs
    pf = [j for s in w.summaries() for j in s["jobs"] if j["job"] == "preflight"]
    assert [j["status"] for j in pf] == ["OK"]                     # the loser ran no preflight
    assert len(w.calls("sweep")) == 1
    assert not REAL_STATE_DIR.exists()


LATE_RUNNER = '''
import sys
import time
from pathlib import Path
sys.path.insert(0, sys.argv[4])
from litpipe import state
state.DB_PATH = Path(sys.argv[1])
rid = state.register_run("runner", writer=True)          # a second runner, registered after the winner
Path(sys.argv[2]).write_text(rid, encoding="utf-8")
end = time.time() + 60
while not Path(sys.argv[3]).exists() and time.time() < end:  # still between register_run and finish_run
    time.sleep(0.02)
state.finish_run(rid, status="refused: an earlier runner is live")
'''


def test_a_runner_registered_after_the_winner_never_fails_its_preflight(tmp_path, monkeypatch):
    w = World(tmp_path, monkeypatch)
    w.register("research_a")
    helper = tmp_path / "late_runner.py"
    helper.write_text(LATE_RUNNER, encoding="utf-8")
    ready, release = tmp_path / "late.ready", tmp_path / "late.release"
    procs = []

    def late_runner_then_the_real_preflight(**kw):
        procs.append(subprocess.Popen([sys.executable, str(helper), str(w.state_dir / state.DB_NAME), str(ready),
                                       str(release), str(REPO)], cwd=str(REPO), stdin=subprocess.DEVNULL))
        assert wait_for(ready.exists, 120)
        return REAL_PREFLIGHT_RUN(network=False, env={"LITPIPE_EMAIL": "runner.probe@university.edu"}, **kw)
    monkeypatch.setattr(preflight, "run", late_runner_then_the_real_preflight)
    try:
        code = runner.main(["run", "--profile", "every_run"])
    finally:
        release.write_text("go", encoding="utf-8")
        for p in procs:
            p.wait(timeout=60)
    late = ready.read_text(encoding="utf-8")
    assert [r["status"] for r in w.runs() if r["run_id"] == late] == ["refused: an earlier runner is live"]
    pf = [j for j in w.summaries()[-1]["jobs"] if j["job"] == "preflight"]
    assert pf and pf[0]["status"] == "OK", pf
    assert code == 0


# ================================================================ 3. the heartbeat and stage timeouts
SLEEPER = '''
import time


def run(sleep=6, **kw):
    time.sleep(float(sleep))
    return {"exit_code": 0}
'''


def test_a_wall_clock_jump_while_a_real_child_runs_is_not_a_timeout(tmp_path, monkeypatch):
    """The machine sleeps for two hours while a stage runs. On wake the launcher's poll returns within
    POLL_S; the heartbeat thread is still inside its slice (up to HEARTBEAT_SLICE_S), so the jump is
    not yet in hb.jumped() and elapsed() reads two hours."""
    write_mods(tmp_path, monkeypatch, w4bn_sleeper=SLEEPER)
    reg = temp_registry(tmp_path)
    clock = JumpClock()
    monkeypatch.setattr(runner, "time", clock)
    hb = runner.Heartbeat(RUN_ID, beat=lambda rid: True).start()
    step = make_step(tmp_path, reg, "index", "w4bn_sleeper", {"sleep": 6}, timeout_s=3600, hb=hb)

    def lid_closed_for_two_hours():
        _rt.sleep(2.5)
        clock.offset += 2 * 3600
    t = threading.Thread(target=lid_closed_for_two_hours, daemon=True)
    t.start()
    try:
        sr = runner.subprocess_launcher(step)
    finally:
        t.join(10)
        hb.stop()
    assert sr.killed == "", (sr.killed, sr.kill_method, sr.elapsed_s)
    assert sr.result is not None and sr.result["exit_code"] == 0
    assert sr.elapsed_s < 600


TREE = '''
import os
import subprocess
import sys
import time
from pathlib import Path

LEAF = "import os, sys, time; from pathlib import Path; Path(sys.argv[1]).write_text(str(os.getpid())); time.sleep(600)"
MID = ("import os, subprocess, sys, time; from pathlib import Path; "
       "Path(sys.argv[1] + '.g').write_text(str(os.getpid())); "
       "p = subprocess.Popen([sys.executable, '-c', sys.argv[2], sys.argv[1] + '.gg']); "
       "Path(sys.argv[1] + '.gp').write_text(str(p.pid)); time.sleep(600)")


def run(pids=None, **kw):
    Path(pids + ".c").write_text(str(os.getpid()))
    p = subprocess.Popen([sys.executable, "-c", MID, pids, LEAF])
    Path(pids + ".cp").write_text(str(p.pid))
    time.sleep(600)
    return {"exit_code": 0}
'''


def test_a_timeout_kills_a_grandchild_of_a_grandchild(tmp_path, monkeypatch):
    """stage child -> its child -> that child's child (each a python started with sys.executable, a
    launcher in a venv): the timeout kills every level (the job object on Windows, the process group
    on POSIX)."""
    write_mods(tmp_path, monkeypatch, w4bn_tree=TREE)
    reg = temp_registry(tmp_path)
    base = str(tmp_path / "pids")
    step = make_step(tmp_path, reg, "index", "w4bn_tree", {"pids": base}, timeout_s=20)
    sr = runner.subprocess_launcher(step)
    assert sr.killed == "timeout", (sr.killed, sr.problem)
    pids = {s: int(Path(base + s).read_text()) for s in (".c", ".cp", ".g", ".gp", ".gg")}   # all started
    assert wait_for(lambda: not any(state.pid_alive(p) for p in pids.values()), timeout=30), \
        {s: state.pid_alive(p) for s, p in pids.items()}
    assert sr.kill_method in ("job object", "process group (SIGTERM)", "process group (SIGTERM, then SIGKILL)")


def test_the_cron_line_names_the_sigkill_limit():
    """POSIX: each stage runs in its own session, so a runner killed with SIGKILL (the OOM killer,
    kill -9) cannot kill it. systemd's unit cgroup covers this; cron does not, so its line says so."""
    text = runner.schedule_text("linux", checkout="/opt/litpipe")
    cron = text[text.index("# crontab fallback"):]
    assert "SIGKILL" in cron


# ================================================================ 4. the batch under a real kill at every point
BKEY = "teaching_a"
BPOOL = [f"10.5555/pool.{i:04d}" for i in range(1, 7)]
KILL_WRAPPER = '''
import json
import os
import sys
from pathlib import Path
sys.path.insert(0, os.environ["W4BN_FIX"])
import run_runner                                   # the builder's harness: path setup at import only
from litpipe import runner, worklists

marks = Path(os.environ["W4BN_MARKS"])
real = worklists.Pool.mark_swept


def mark_swept(self, dois, run_id, classes):
    with open(marks, "a", encoding="utf-8") as f:
        f.write(json.dumps({"dois": list(dois), "run_id": run_id, "pid": os.getpid()}) + "\\n")
    return real(self, dois, run_id, classes)


worklists.Pool.mark_swept = mark_swept
point, nth, seen = os.environ.get("W4BN_KILL_AT"), int(os.environ.get("W4BN_KILL_NTH", "2")), []


def hook(name):
    if name == point:
        seen.append(name)
        if len(seen) == nth:
            sys.stdout.flush()
            os._exit(137)                           # a kill: no finally, no finish_run, no summary


runner.KILL_HOOK = hook
sys.argv = [run_runner.__file__] + sys.argv[1:]
sys.exit(run_runner.main())
'''


def spawn_wrapper(w, wrapper, argv, env):
    e = {**os.environ, "PYTHONUTF8": "1", **env}
    e.pop("LITPIPE_RUN_ID", None)
    out = open(w.tmp / f"harness.{len(list(w.tmp.glob('harness.*.log')))}.log", "wb")
    return subprocess.Popen([sys.executable, str(wrapper), str(w.cfg_path), *argv], cwd=str(REPO), env=e,
                            stdout=out, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)


@pytest.mark.parametrize("point", ["after_next_batch", "after_mark_staged", "after_write", "mid_sweep",
                                   "after_sweep", "after_route", "after_mark_swept"])
def test_a_real_kill_at_each_batch_point_then_a_fresh_process_resumes(wr, point):
    w = wr
    w.register(BKEY)
    pool = w.proot(BKEY) / "lit_pull_queue.unit3_pool.csv"
    pool.write_bytes(("doi,title,authors,year\n" + "".join(
        f"{d},Pool title {i},Author P,2021\n" for i, d in enumerate(BPOOL))).encode("utf-8"))
    wrapper = w.tmp / "kill_wrapper.py"
    wrapper.write_text(KILL_WRAPPER, encoding="utf-8")
    marks = w.tmp / "marks.jsonl"
    env = {"W4BN_FIX": str(FIX), "W4BN_MARKS": str(marks)}
    argv = ["batch", "--project", BKEY, "--pool", str(pool), "--size", "2", "--batches", "3"]
    # Windows: TerminateProcess (the job objects take the stage child). POSIX: SIGTERM, the signal a
    # runner can act on (a SIGKILLed runner cannot kill its child: see test_the_cron_line_names_...).
    hard_kill = point != "mid_sweep" or sys.platform == "win32"
    if point == "mid_sweep":
        started, pidf = w.tmp / "sweep.started", w.tmp / "sweep.pid"
        w.set_fake("sweep", BKEY, sleep=120, started_marker=str(started), pid_file=str(pidf))
        p = spawn_wrapper(w, wrapper, argv, env)
        try:
            assert wait_for(started.exists, timeout=180), w.harness_log()
        finally:
            p.kill() if hard_kill else p.terminate()
            p.wait(timeout=60)
        child = int(pidf.read_text(encoding="utf-8"))
        assert wait_for(lambda: not state.pid_alive(child), timeout=30)       # the sweep child died with it
        w.set_fake("sweep", BKEY)
    else:
        p = spawn_wrapper(w, wrapper, argv, {**env, "W4BN_KILL_AT": point, "W4BN_KILL_NTH": "2"})
        assert p.wait(timeout=300) == 137, w.harness_log()
    killed = [r for r in w.runs() if r["kind"] == "runner"]
    assert len(killed) == 1 and (killed[0]["finished"] is None) == hard_kill   # a hard kill finishes nothing
    assert spawn_wrapper(w, wrapper, argv, env).wait(timeout=300) == 0, w.harness_log()
    rows = [json.loads(x) for x in marks.read_text(encoding="utf-8").splitlines()] if marks.is_file() else []
    per = {}
    for r in rows:
        for d in r["dois"]:
            per[d] = per.get(d, 0) + 1
    assert per == {d: 1 for d in BPOOL}, rows                              # once per DOI, none lost
    cfg = json.loads(w.cfg_path.read_text(encoding="utf-8"))
    st = worklists.Pool(pool, registry=cfg).status()
    assert (st["swept"], st["remaining"], st["pending"]) == (6, 0, 0)
    sweeps = w.calls("sweep")
    assert len(sweeps) == (4 if point == "mid_sweep" else 3), sweeps        # only a surviving file again
    routes = w.calls("route")
    assert len(routes) == 3 and len({c["run_id"] for c in routes}) == 3
    statuses = sorted(r["status"] for r in w.runs() if r["kind"] == "runner")
    first = "abandoned" if hard_kill else "aborted: terminated (SIGTERM)"
    assert statuses == sorted([first, "ok"]), statuses                      # the killed run, then the resume
    assert pool.read_bytes().startswith(b"doi,title,authors,year\n")       # the pool is never written
    assert not REAL_STATE_DIR.exists()


def test_a_retired_batch_whose_sweep_log_holds_a_traceback_stops_the_loop(tmp_path, monkeypatch):
    """The consumer's loop fail-stops on a traceback in the sweep log even after exit 0 (exit 0 is
    not every step ran). The retired batch is still marked swept; no further batch is drawn."""
    w = World(tmp_path, monkeypatch)
    w.register(BKEY)
    pool = w.proot(BKEY) / "lit_pull_queue.unit3_pool.csv"
    pool.write_bytes(("doi,title,authors,year\n" + "".join(
        f"{d},Pool title {i},Author P,2021\n" for i, d in enumerate(BPOOL))).encode("utf-8"))
    w.set_fake("sweep", BKEY, traceback=True)                 # retires its queue, prints a traceback
    code = runner.main(["batch", "--project", BKEY, "--pool", str(pool), "--size", "2", "--batches", "3"])
    s = w.summaries()[-1]
    sw = [j for j in s["jobs"] if j["job"] == "sweep"]
    assert sw[0]["status"] == "DEGRADED" and "traceback" in sw[0]["reason"]
    cfg = json.loads(w.cfg_path.read_text(encoding="utf-8"))
    st = worklists.Pool(pool, registry=cfg).status()
    assert (st["swept"], st["pending"]) == (2, 0)               # the retired batch is marked swept
    assert len(w.calls("sweep")) == 1, w.calls("sweep")          # and the loop stopped before batch 2
    assert code == 2


def test_the_batch_files_destination_passes_sweeps_own_check_for_a_subproject(tmp_path, monkeypatch):
    import sweep
    w = World(tmp_path, monkeypatch)
    key = "research_p/sub_c"
    w.register(key, lib_dir="refs/papers")
    pool = w.proot(key) / "_ranked_pool.csv"
    pool.write_bytes(("doi,title,authors,year\n" + "".join(
        f"{d},T {i},A,2021\n" for i, d in enumerate(BPOOL[:2]))).encode("utf-8"))
    assert runner.main(["batch", "--project", key, "--pool", str(pool), "--size", "2", "--stage-only"]) == 0
    bfile = w.proot(key) / f"lit_pull_queue.{runner.batch_tag(pool.name)}.csv"
    assert bfile.is_file()
    cfg = json.loads(w.cfg_path.read_text(encoding="utf-8"))
    res = sweep.run_pipeline(w.proot(key), bfile, dry_run=True, key=key, registry=cfg["projects"])
    assert res is not None                                                 # None: refused (destination)


# ================================================================ 7. the local canaries' context
def _two_queue_stage(monkeypatch):
    """sweep's fetch stages replaced (as tests/test_batch1_fixes.py does): DOIs with 'fetch' in
    them download, the rest are CLOSED with no PMCID. Nothing runs and nothing is sent."""
    import sweep

    def stage(cmd):
        cmd = [str(c) for c in cmd]
        script = Path(cmd[1]).name

        def arg(flag):
            return cmd[cmd.index(flag) + 1]
        if script == "unpaywall_fetch_v2.py":
            with open(arg("--triage"), encoding="utf-8", newline="") as f:
                dois = [r["doi"] for r in csv.DictReader(f)]
            Path(arg("--report")).write_text("doi,downloaded,oa_status,error\n" + "".join(
                f"{d},True,OA,\n" if "fetch" in d else f"{d},False,CLOSED,CLOSED\n" for d in dois),
                encoding="utf-8")
        elif script == "pmc_fetch.py":
            with open(arg("--report-in"), encoding="utf-8", newline="") as f:
                left = [r["doi"] for r in csv.DictReader(f) if r["downloaded"] != "True"]
            Path(arg("--report-out")).write_text("doi,downloaded,error,pmcid\n" + "".join(
                f"{d},False,NO_PMCID,\n" for d in left), encoding="utf-8")
        return types.SimpleNamespace(returncode=0, stdout="", stderr="")
    monkeypatch.setattr(sweep, "_run_stage", stage)


def test_two_queues_one_wholly_fetched_raise_no_lost_artifacts_alarm(tmp_path, monkeypatch):
    w = World(tmp_path, monkeypatch)
    monkeypatch.setitem(runner.STAGE_MODULES, "sweep", "sweep")
    monkeypatch.setitem(runner.STAGE_MODULES, "route", "migrate_closed_to_md")

    def canaries_local_real(profile, *, phase="all", context=None, cfg=None, **kw):
        w.canary_calls.append({"phase": phase, "context": context})
        return REAL_CANARIES_RUN(profile, phase=phase, context=context, cfg=cfg) if phase == "local" else []
    monkeypatch.setattr(canaries, "run", canaries_local_real)
    w.register("research_a")
    w.queue("research_a", ["10.5555/fetch.0001", "10.5555/closed.0001"])
    w.queue("research_a", ["10.5555/fetch.0002"], tag="b-extra")            # every row fetched
    _two_queue_stage(monkeypatch)
    code = runner.main(["run", "--profile", "every_run"])
    s = w.summaries()[-1]
    sw = [j for j in s["jobs"] if j["job"] == "sweep"][0]
    assert sw["status"] == "OK" and sw["counts"]["retired"] == 2, sw
    rid = sw["counts"]["run_id"]
    root = w.proot("research_a")
    assert (root / f"lit_pull_queue.{rid}.routing.csv").is_file()
    lost = [c for c in s["canaries"]["checks"] if c["id"] == "lost_artifacts"]
    assert lost and all(c["status"] == "PASS" for c in lost), lost
    assert code == 0, s["reasons"]


def test_two_queues_one_with_preprint_skipped_raise_no_lost_artifacts_alarm(tmp_path, monkeypatch):
    """The brief's case: one queue's preprint stage ran, the other's was skipped. The context leaves
    preprint out (it is not on disk for every chain), so the real canary finds nothing missing."""
    import sweep
    w = World(tmp_path, monkeypatch)
    w.register("research_a")
    cfg = json.loads(w.cfg_path.read_text(encoding="utf-8"))
    r = runner._ScheduledRun(cfg, ["research_a"])
    r.run_id = RUN_ID
    p = r.projects[0]
    sid = "2026-10-07"
    results = []
    for tag, pre in (("", "completed"), ("b-extra", "skipped")):
        stages = {"unpaywall": "completed", "pmc": "completed", "preprint": pre}
        # W5-C1: migrate writes a (header-only) routing CSV for every chain, so `routing` is expected too
        for s in ("unpaywall", "pmc", "residual", "report", "processed", "routing") + (
                ("preprint",) if pre == "completed" else ()):
            (p.root / sweep.artifact_name(tag, sid, s)).write_text("doi\n", encoding="utf-8")
        results.append({"tag": tag, "run_id": sid, "retired": True, "stages": stages})
    p.sweep = {"exit": 0, "status": "OK", "run_id": sid, "results": results, "refused": []}
    p.route_status = "nothing"
    ctx = r._local_ctx()
    assert ctx["projects"][0]["stages"] == ["unpaywall", "residual", "report", "pmc", "processed", "routing"]
    outs = REAL_CANARIES_RUN("every_run", phase="local", context=ctx, cfg=cfg)
    lost = [o.payload for o in outs if (o.payload or {}).get("id") == "lost_artifacts"]
    assert lost and all(x["status"] == "PASS" for x in lost), lost


def test_a_malformed_local_context_is_reported_not_a_crash(tmp_path, monkeypatch):
    w = World(tmp_path, monkeypatch)
    w.register("research_a")
    monkeypatch.setattr(runner._ScheduledRun, "_local_ctx",
                        lambda self: {"run_id": self.run_id, "since": "not a time", "projects": "research_a"})
    code = runner.main(["run", "--profile", "every_run"])
    s = w.summaries()[-1]
    loc = [j for j in s["jobs"] if j["job"] == "canaries_local"]
    assert loc and loc[0]["status"] == "ERROR" and "ConfigError" in loc[0]["reason"], loc
    assert code == 2 and s["exit_code"] == 2 and s["aborted"] is None
    assert [r["status"] for r in w.runs() if r["kind"] == "runner"] == ["completed with failures"]


# ================================================================ 6. outputs: redaction of the configured address
LEAKY_SWEEP = '''
import os
import w4a_fake_sweep


def run(**kw):
    e = os.environ["LITPIPE_EMAIL"]
    print(f"contact {e} or mailto:{e}", flush=True)
    res = w4a_fake_sweep.run(**kw)
    for p in (res.get("projects") or {}).values():
        p["refused"] = [f"lit_pull_queue.{e}.csv"]
        for r in p.get("results") or []:
            r["keep_reason"] = f"ask {e}"
    return res
'''


def test_the_configured_address_never_reaches_a_runner_output(tmp_path, monkeypatch):
    email = "runner.probe@university.edu"
    monkeypatch.setenv("LITPIPE_EMAIL", email)
    w = World(tmp_path, monkeypatch)
    write_mods(tmp_path, monkeypatch, w4bn_leaky_sweep=LEAKY_SWEEP)
    monkeypatch.setitem(runner.STAGE_MODULES, "sweep", "w4bn_leaky_sweep")
    w.register("research_a")
    w.queue("research_a", ["10.5555/w4bn.0001"])
    runner.main(["run", "--profile", "every_run"])
    logs = list((w.state_dir / "runner").rglob("*.log"))
    assert logs and any("REDACTED" in p.read_text(encoding="utf-8") for p in logs)   # the line was logged
    blobs = [p.read_bytes() for p in (w.state_dir / "runner").rglob("*") if p.is_file()]
    blobs += [w.loose.read_bytes()] if w.loose.is_file() else []
    import sqlite3
    con = sqlite3.connect(str(w.state_dir / state.DB_NAME))
    try:
        blobs += [json.dumps(con.execute("SELECT * FROM kv").fetchall()).encode("utf-8"),
                  json.dumps(con.execute("SELECT * FROM runs").fetchall()).encode("utf-8")]
    finally:
        con.close()
    assert not [b for b in blobs if email.encode() in b or b"university.edu" in b]
