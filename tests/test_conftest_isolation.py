"""The suite's own isolation (W3a pre-dispatch review, C1): a direct litpipe.state call resolves a temp
state file, never the real state dir, and the session guard fails a run that creates the real dir."""
from types import SimpleNamespace

from litpipe import state
from tests import conftest


def test_a_direct_state_call_uses_a_temp_file():
    p = state.db_path(create=False)
    assert conftest.REAL_STATE_DIR not in p.parents
    state.kv_set("isolation", "k", 1)
    assert state.kv_get("isolation", "k") == 1
    assert p.exists() and conftest.REAL_STATE_DIR not in p.parents


def test_the_session_guard_fails_a_run_that_creates_the_real_state_dir(tmp_path, monkeypatch):
    fake_real = tmp_path / "home" / ".local" / "db" / "literature_pipeline"
    monkeypatch.setattr(conftest, "REAL_STATE_DIR", fake_real)
    session = SimpleNamespace(config=SimpleNamespace(), exitstatus=0)
    conftest.pytest_sessionstart(session)
    conftest.pytest_sessionfinish(session, 0)
    assert session.exitstatus == 0                    # absent before and after: fine
    session = SimpleNamespace(config=SimpleNamespace(), exitstatus=0)
    conftest.pytest_sessionstart(session)
    fake_real.mkdir(parents=True)
    conftest.pytest_sessionfinish(session, 0)
    assert session.exitstatus == 1                    # created during the run: the run fails
    session = SimpleNamespace(config=SimpleNamespace(), exitstatus=0)
    conftest.pytest_sessionstart(session)             # existed before (after cutover): not flagged
    conftest.pytest_sessionfinish(session, 0)
    assert session.exitstatus == 0
