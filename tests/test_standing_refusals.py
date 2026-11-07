"""Item 4 (handoff D(a), week-1 health): a manual refusal older than 24 h is a HEALTH ALARM, and
the standing refusals are listed in the health summary.

A manual refusal lasts until someone runs `python -m litpipe.state --clear-refusal HOST`.
export.arxiv.org was refused that way on 2026-10-13 after one 429 and was still refused a week
later, although arXiv answered 200: nothing in the morning report said so.
"""
from datetime import datetime, timezone

import pytest

import litpipe.state as st
from litpipe import canaries

NOW = datetime(2026, 11, 7, 6, 0, tzinfo=timezone.utc)
HOUR = 3600.0


@pytest.fixture
def at(monkeypatch):
    """Refuse a host as of `hours` before NOW (the state reads its clock through _time)."""
    def refuse(host, reason, persistence, hours):
        monkeypatch.setattr(st, "_time", lambda: NOW.timestamp() - hours * HOUR)
        st.refuse(host, reason, persistence=persistence)
        monkeypatch.setattr(st, "_time", lambda: NOW.timestamp())
    return refuse


def check(state=st):
    rctx = canaries._RunCtx(context={}, state=state, request=None, cfg=None, now=NOW)
    [o] = canaries._run_local(canaries.CHECKS_BY_ID["standing_refusals"], canaries._LocalCtx(rctx))
    return o.payload


def test_no_refusals_passes():
    p = check()
    assert p["status"] == canaries.PASS and p["observed"] == "none"


def test_a_manual_refusal_older_than_a_day_is_an_alarm(at):
    at("export.arxiv.org", "manual: one 429", "manual", hours=25 * 24)
    p = check()
    assert p["status"] == canaries.ALARM
    assert "export.arxiv.org (manual, 25.0 d" in p["observed"]
    assert "--clear-refusal" in p["observed"]


def test_a_fresh_manual_refusal_is_listed_not_an_alarm(at):
    at("www.mdpi.com", "manual: bot wall", "manual", hours=3)
    p = check()
    assert p["status"] == canaries.PASS and "www.mdpi.com (manual, 3.0 h" in p["observed"]


def test_a_run_refusal_is_never_an_alarm_by_age(at, monkeypatch):
    """A run refusal ends with its run; one that still holds belongs to a live run."""
    monkeypatch.setattr(st, "pid_alive", lambda pid: True)
    at("onlinelibrary.wiley.com", "HTTP 403", "run", hours=30)
    p = check()
    assert p["status"] == canaries.PASS and "onlinelibrary.wiley.com (run," in p["observed"]


def test_a_dead_runs_refusal_is_not_standing(at, monkeypatch):
    monkeypatch.setattr(st, "pid_alive", lambda pid: False)
    at("figshare.com", "HTTP 403", "run", hours=30)
    assert check()["observed"] == "none"


def test_the_summary_lists_standing_refusals_on_a_pass():
    rep = {"checks": [{"id": "standing_refusals", "status": canaries.PASS, "host": "state",
                       "observed": "1 standing: www.mdpi.com (manual, 3.0 h: bot wall)"}],
           "requests": 0}
    s = canaries.summary(rep)
    assert s.splitlines()[0].startswith("HEALTH PASS")
    assert "standing refusals: 1 standing: www.mdpi.com (manual, 3.0 h: bot wall)" in s


def test_no_state_file_is_not_created(tmp_path, monkeypatch):
    monkeypatch.setattr(st, "DB_PATH", tmp_path / "absent.sqlite")
    assert check()["observed"] == "none"
    assert not (tmp_path / "absent.sqlite").exists()


def test_a_stand_in_state_lists_without_ages():
    class Stand:
        refused = {"export.arxiv.org": ("manual: one 429", "manual")}

        def is_refused(self, host):
            return host in self.refused
    p = check(Stand())
    assert p["status"] == canaries.PASS and "export.arxiv.org (manual, age unknown" in p["observed"]
