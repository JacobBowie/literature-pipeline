"""Sibling fixes the dispatcher added with the W5 verifier fixes (2026-10-08): defects of the same
pattern as a verifier finding, found by sweeping for it. Each test fails on `03114a9`.

- lock release (P-1/P-6 pattern, a check then an act): release() read the lock and then deleted it;
  a breaker that moved our stale-looking lock aside and took a fresh one in between lost its lock.
- the worklist CSV import (P's dry-run note, live since decision A2 = retry): a review row whose DOI
  is already on the ILL or the browser list was queued for retry, so the next sweep swept it again."""
from pathlib import Path

from litpipe import lockfile
from tests.test_w5c1_import import REVIEW, World, row, run_import, seed_listed

import pytest


def _forget(lk):
    """Drop `lk` from this process's held table, so another Lock here behaves as another process."""
    with lockfile._HELD_GUARD:
        lockfile._HELD.pop(lockfile._key(lk.path), None)


# ================================================================ lock release
def test_release_never_deletes_a_lock_a_breaker_took_between_its_read_and_its_delete(tmp_path, monkeypatch):
    t = {"now": 1_000_000.0}
    monkeypatch.setattr(lockfile, "_time", lambda: t["now"])
    old = lockfile.Lock(tmp_path, tool="sweep", run_id="old-holder", stale_s=100, heartbeat=False).acquire()
    _forget(old)
    real_inspect = lockfile._inspect
    racer = {}

    def inspect_then_race(path):
        got = real_inspect(path)
        if not racer and Path(path) == old.path:
            # release() has read its own record; the machine had slept, so another process judges
            # the lock stale by its heartbeat age and tries to break it and take it
            t["now"] += 101
            racer["lk"] = lockfile.Lock(tmp_path, tool="runner", run_id="new-holder", stale_s=100,
                                        heartbeat=False)
            try:
                racer["lk"].acquire()
            except lockfile.LockHeld:
                pass
        return got

    monkeypatch.setattr(lockfile, "_inspect", inspect_then_race)
    old.release()
    monkeypatch.setattr(lockfile, "_inspect", real_inspect)
    if racer["lk"].held:                               # 03114a9: it took the lock and release deleted it
        assert (lockfile.read(tmp_path) or {}).get("run_id") == "new-holder"
    else:                                              # fixed: the breaker file kept it out; ours is gone
        assert lockfile.read(tmp_path) is None
    assert not (tmp_path / (lockfile.LOCK_NAME + lockfile.BREAK_SUFFIX)).exists()


def test_release_still_deletes_its_own_lock_past_a_crashed_breakers_file(tmp_path):
    lk = lockfile.Lock(tmp_path, tool="sweep", run_id="me", heartbeat=False).acquire()
    brk = tmp_path / (lockfile.LOCK_NAME + lockfile.BREAK_SUFFIX)
    brk.write_text("4242", encoding="ascii")          # fresh: a breaker that crashed seconds ago
    assert lk.release() is True
    assert lockfile.read(tmp_path) is None
    assert brk.exists()                                # not ours to remove; a breaker removes it when old


# ================================================================ the import's review dedup
@pytest.fixture
def w(tmp_path, monkeypatch):
    return World(tmp_path, monkeypatch)


def test_a_review_row_already_on_a_worklist_is_listed_not_queued_for_retry(w):
    seed_listed(w)                                     # 10.1000/LISTED.1 on the ILL list
    root = w.proot("Courses/teaching_a")
    (root / "lit_pull_queue.oa_blocked.md").write_text(
        "# Browser worklist\n\n- [ ] **B** [10.1000/oa.7](https://doi.org/10.1000/oa.7) cause `403`\n",
        encoding="utf-8")
    src = w.csv([row("teaching_a", "10.1000/listed.1", REVIEW),           # on the ILL list
                 row("teaching_a", "10.1000/oa.7", REVIEW),               # on the browser list
                 row("teaching_a", "10.1000/both.1", "ill"),              # in this CSV as ill and review
                 row("teaching_a", "10.1000/both.1", REVIEW),
                 row("teaching_a", "10.1000/rev.77", REVIEW)])
    res = run_import(w, src, commit=True)
    rev = res["projects"]["Courses/teaching_a"]["review"]
    assert rev["write"] == 1 and rev["listed"] == 3
    import migrate_closed_to_md as mig
    assert [r["doi"] for r in mig.read_retry_later(root)[1]] == ["10.1000/rev.77"]
