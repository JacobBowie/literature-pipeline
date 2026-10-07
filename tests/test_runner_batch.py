"""python -m litpipe.runner `batch` (amendment 11): a pool drawn down in batches on the drawdown
contract of litpipe.worklists.Pool (W4a verifier M), resumable after a kill at every point.

The kill points are simulated in-process through runner.KILL_HOOK (a BaseException the runner does
not catch, like a kill); the mid-sweep kill is a real process (tests/test_runner_procs.py). The sweep
is the stand-in of tests/fixtures/W4-A (no fetch stage runs)."""
import csv
import io
import json
import sys
from pathlib import Path

import pytest

FIX = Path(__file__).resolve().parent / "fixtures" / "W4-A"
sys.path.insert(0, str(FIX))

from litpipe import runner, worklists  # noqa: E402
from w4a_world import World  # noqa: E402

KEY = "teaching_a"
POOL_DOIS = [f"10.5555/pool.{i:04d}" for i in range(1, 7)]


class Killed(BaseException):
    pass


@pytest.fixture
def w(tmp_path, monkeypatch):
    world = World(tmp_path, monkeypatch)
    world.register(KEY)
    world.marks = []
    real = worklists.Pool.mark_swept

    def mark_swept(self, dois, run_id, classes):
        world.marks.append((list(dois), run_id, dict(classes)))
        return real(self, dois, run_id, classes)
    monkeypatch.setattr(worklists.Pool, "mark_swept", mark_swept)
    return world


def make_pool(w, name="lit_pull_queue.ch15_pool.csv", dois=POOL_DOIS, where=None, destination=False):
    d = where or w.proot(KEY)
    d.mkdir(parents=True, exist_ok=True)
    p = d / name
    head = "doi,title,authors,year" + (",destination" if destination else "") + "\n"
    rows = "".join(f"{x},Pool title {i},Author P,2021" + (",literature/" if destination else "") + "\n"
                   for i, x in enumerate(dois))
    p.write_bytes((head + rows).encode("utf-8"))
    return p


def batch(pool, *extra):
    return runner.main(["batch", "--project", KEY, "--pool", str(pool), *extra])


def swept_dois(w):
    """{doi: number of mark_swept calls that named it} (curated_out included)."""
    out = {}
    for dois, _, _ in w.marks:
        for d in dois:
            out[d] = out.get(d, 0) + 1
    return out


def pool_status(pool, w):
    import json as _j
    cfg = _j.loads(w.cfg_path.read_text(encoding="utf-8"))
    return worklists.Pool(pool, registry=cfg).status()


# ================================================================ the tag
@pytest.mark.parametrize("name,tag", [
    ("lit_pull_queue.ch15_pool.csv", "b-ch15_pool"),
    ("4501_bodycomp_pool.csv", "b-4501_bodycomp_pool"),
    ("_resp_ranked_pool.csv", "b-resp_ranked_pool"),
    ("Mixed Case Pool!.csv", "b-mixed-case-pool-"),
    ("lit_pull_queue." + "x" * 40 + ".csv", "b-" + "x" * 30),
])
def test_batch_tag(name, tag):
    assert runner.batch_tag(name) == tag and len(tag) <= 32


@pytest.mark.parametrize("name", ["lit_pull_queue.ch15_pool.csv", "4501_bodycomp_pool.csv", "_resp_ranked_pool.csv"])
def test_the_consumers_real_pool_names_give_valid_tags(w, name):
    import sweep
    assert sweep.is_valid_tag(runner.batch_tag(name))
    pool = make_pool(w, name, where=w.lib(KEY) if name.startswith("_") else None)
    assert batch(pool, "--size", "2") == 0
    assert not (w.proot(KEY) / f"lit_pull_queue.{runner.batch_tag(name)}.csv").exists()     # retired
    assert len(swept_dois(w)) == 2 and pool.is_file()


def test_a_tag_that_is_not_a_valid_queue_tag_exits_1_before_any_write(w):
    pool = make_pool(w, "2026-09-13.candidates_all.csv")
    before = w.listing()
    assert batch(pool) == 1
    assert w.listing() == before


# ================================================================ exit 1 before writing anything
def test_the_batch_queue_never_overwrites_the_pool(w):
    pool = make_pool(w)
    before = w.listing()
    assert batch(pool, "--tag", "ch15_pool") == 1             # lit_pull_queue.ch15_pool.csv IS the pool
    assert w.listing() == before


def test_a_pool_that_sweep_would_sweep_whole_exits_1(w, capsys):
    pool = make_pool(w, destination=True)                       # doi and destination: a queue to sweep
    before = w.listing()
    assert batch(pool) == 1
    assert w.listing() == before
    assert "sweep would sweep the pool" in capsys.readouterr().err      # its own guard, not the next one


def test_another_staged_queue_exits_1_but_retry_does_not(w):
    pool = make_pool(w)
    w.queue(KEY, ["10.5555/other.0001"])
    before = w.listing()
    assert batch(pool) == 1
    assert w.listing() == before
    (w.proot(KEY) / "lit_pull_queue.csv").unlink()
    w.queue(KEY, ["10.5555/retry.0001"], tag="retry")
    assert batch(pool, "--size", "2") == 0


def test_an_untracked_file_at_the_batch_path_is_never_written_over(w):
    pool = make_pool(w)
    stray = w.proot(KEY) / "lit_pull_queue.b-ch15_pool.csv"
    stray.write_text("doi,title\n10.5555/hand.0001,Hand\n", encoding="utf-8")   # no destination: not a queue
    assert batch(pool) == 1
    assert stray.read_text(encoding="utf-8") == "doi,title\n10.5555/hand.0001,Hand\n"


# ================================================================ a 3 x 2-row batch
def test_three_batches_of_two_draw_the_pool_down(w):
    pool = make_pool(w)
    w.set_fake("sweep", KEY, classes={POOL_DOIS[1]: "TERMINAL_CLOSED"})
    assert batch(pool, "--size", "2", "--batches", "3") == 0
    assert swept_dois(w) == {d: 1 for d in POOL_DOIS}
    st = pool_status(pool, w)
    assert (st["swept"], st["remaining"], st["pending"]) == (6, 0, 0)
    assert st["classes"] == {"TERMINAL_CLOSED": 1, "fetched": 5}
    sweeps = w.calls("sweep")
    assert len(sweeps) == 3 and all(c["loose_ends"] is False for c in sweeps)
    q = list(w.proot(KEY).glob("lit_pull_queue.b-ch15_pool.*.processed.csv"))
    assert len(q) == 3
    rows = list(csv.DictReader(io.StringIO(q[0].read_text(encoding="utf-8"))))
    assert list(rows[0]) == ["doi", "title", "authors", "year", "destination", "notes"]
    assert {r["destination"] for r in rows} == {"literature"}
    assert len(w.calls("route")) == 3
    s = w.summaries()[-1]
    assert s["kind"] == "runner batch" and len(s["batch"]["batches"]) == 3
    assert batch(pool, "--size", "2") == 0                       # drawn down: nothing left
    assert len(w.calls("sweep")) == 3


KILL_POINTS = ["after_next_batch", "after_mark_staged", "after_write", "after_sweep", "after_route",
               "after_mark_swept"]


@pytest.mark.parametrize("point", KILL_POINTS)
def test_a_kill_at_each_point_resumes_without_loss_or_a_second_mark(w, monkeypatch, point):
    pool = make_pool(w)
    seen = []

    def hook(name):
        if name == point:
            seen.append(name)
            if len(seen) == 2:                      # the second batch dies at this point
                raise Killed(point)
    monkeypatch.setattr(runner, "KILL_HOOK", hook)
    with pytest.raises(Killed):
        batch(pool, "--size", "2", "--batches", "3")
    monkeypatch.setattr(runner, "KILL_HOOK", None)
    assert batch(pool, "--size", "2", "--batches", "3") == 0
    assert swept_dois(w) == {d: 1 for d in POOL_DOIS}               # mark_swept once per DOI, none lost
    st = pool_status(pool, w)
    assert (st["swept"], st["remaining"], st["pending"]) == (6, 0, 0)
    assert len(w.calls("sweep")) == 3                                 # only a surviving file is swept again
    routed = {c["run_id"] for c in w.calls("route")}
    assert len(routed) == 3


def test_after_the_sweep_the_resume_routes_and_never_resweeps(w, monkeypatch):
    pool = make_pool(w)
    monkeypatch.setattr(runner, "KILL_HOOK", lambda n: (_ for _ in ()).throw(Killed(n)) if n == "after_sweep" else None)
    with pytest.raises(Killed):
        batch(pool, "--size", "2")
    assert w.calls("route") == [] and len(w.calls("sweep")) == 1
    monkeypatch.setattr(runner, "KILL_HOOK", None)
    assert batch(pool, "--size", "2", "--batches", "1") == 0
    assert len(w.calls("sweep")) == 2                      # the resume swept the NEXT batch, not this one again
    assert w.calls("route")[0]["run_id"] == w.marks[0][1]  # the killed run was routed, then marked


def test_a_batch_not_retired_marks_nothing_stops_and_is_left_for_the_next_run(w):
    pool = make_pool(w)
    w.set_fake("sweep", KEY, retire=False, keep_reason="pmc failed", exit_code=3, stages={"pmc": "failed"})
    assert batch(pool, "--size", "2", "--batches", "3") == 2
    assert w.marks == [] and len(w.calls("sweep")) == 1
    st = pool_status(pool, w)
    assert st["pending"] == 2 and st["swept"] == 0
    assert "left for the next runner batch" in json.dumps(w.summaries()[-1]["jobs"])
    w.set_fake("sweep", KEY)
    assert batch(pool, "--size", "2", "--batches", "3") == 0
    assert swept_dois(w) == {d: 1 for d in POOL_DOIS}
    assert len(w.calls("sweep")) == 4


# ================================================================ --stage-only and curation
def test_stage_only_then_a_hand_curated_file_marks_the_removed_rows_curated_out(w):
    pool = make_pool(w)
    assert batch(pool, "--size", "3", "--stage-only") == 0
    bfile = w.proot(KEY) / "lit_pull_queue.b-ch15_pool.csv"
    assert bfile.is_file() and w.calls("sweep") == [] and pool_status(pool, w)["pending"] == 3
    assert batch(pool, "--size", "3", "--stage-only") == 0          # pending: nothing new staged
    assert pool_status(pool, w)["pending"] == 3 and w.calls("sweep") == []
    lines = bfile.read_text(encoding="utf-8").splitlines()
    bfile.write_text("\n".join(lines[:1] + lines[2:]) + "\n", encoding="utf-8")   # curate the first row out
    assert batch(pool, "--size", "3") == 0
    marks = swept_dois(w)
    assert marks == {d: 1 for d in POOL_DOIS[:3]}
    assert w.marks[0] == ([POOL_DOIS[0]], w.marks[0][1], {POOL_DOIS[0]: "curated_out"})
    assert pool_status(pool, w)["classes"] == {"curated_out": 1, "fetched": 2}
    assert batch(pool, "--size", "3") == 0                          # the curated row is never drawn again
    assert set(swept_dois(w)) == set(POOL_DOIS) and swept_dois(w)[POOL_DOIS[0]] == 1


def test_a_doi_the_batch_did_not_stage_exits_1_naming_it(w, capsys):
    pool = make_pool(w)
    batch(pool, "--size", "2", "--stage-only")
    bfile = w.proot(KEY) / "lit_pull_queue.b-ch15_pool.csv"
    with open(bfile, "a", encoding="utf-8") as f:
        f.write("10.5555/sneaked.0099,Sneaked,X,2020,literature,n\n")
    assert batch(pool, "--size", "2") == 1
    assert "10.5555/sneaked.0099" in capsys.readouterr().err
    assert w.calls("sweep") == [] and w.marks == []


def test_stage_only_sends_nothing(w, monkeypatch):
    pool = make_pool(w)
    monkeypatch.setattr("litpipe.preflight.run", lambda **kw: (_ for _ in ()).throw(AssertionError("sent")))
    assert batch(pool, "--size", "2", "--stage-only") == 0
    assert [c["phase"] for c in w.canary_calls] == []


# ================================================================ the dry run and pass-throughs
def test_batch_dry_run_writes_nothing(w, capsys):
    pool = make_pool(w)
    before = w.listing()
    assert batch(pool, "--size", "2", "--dry-run") == 0
    assert w.listing() == before
    out = capsys.readouterr().out
    assert "next batch: 2 DOI(s)" in out and "lit_pull_queue.b-ch15_pool.csv" in out
    assert not (pool.parent / "lit_pull_queue.ch15_pool.drawdown.json").exists()


def test_skip_preprint_passes_through_to_sweep_and_route(w):
    pool = make_pool(w)
    assert batch(pool, "--size", "2", "--skip-preprint") == 0
    assert w.calls("sweep")[0]["skip_preprint"] is True and w.calls("route")[0]["skip_preprint"] is True


def test_batch_registers_one_runner_run_and_runs_preflight_and_the_every_run_canaries(w):
    pool = make_pool(w)
    assert batch(pool, "--size", "2") == 0
    runs = [r for r in w.runs() if r["kind"] == "runner"]
    assert len(runs) == 1 and runs[0]["status"] == "ok"
    assert [(c["profile"], c["phase"]) for c in w.canary_calls] == [("every_run", "network"), ("every_run", "local")]
    assert len(w.summaries()) == 1
