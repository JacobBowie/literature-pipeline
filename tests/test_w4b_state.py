"""litpipe.state, the three W4b edits (amendment 12); each lock fails on 56c8392.

1. kv_set redacts every string in the value, nested ones included (litpipe.ledger.redact_obj);
   sha hex, RA names, cadence dicts and numbers are unchanged.
2. live_runs() orders by (started, rowid) and each entry carries `seq` (the runs rowid), so two runs
   registered in the same second are ordered by registration.
3. LEASE_S covers a slow 80 MB stream: litpipe.net holds one lease around the whole transfer."""
import json
import sqlite3

import pytest

from litpipe import ledger, net, state

ADDR = "someone@university.edu"


@pytest.fixture
def real_state(tmp_path, monkeypatch):
    monkeypatch.setattr(state, "DB_PATH", tmp_path / "state" / state.DB_NAME)
    monkeypatch.setattr(state, "_current_run", None)
    monkeypatch.delenv("LITPIPE_RUN_ID", raising=False)
    return state


# ================================================================ 1. kv redaction
def test_kv_set_redacts_strings_nested_ones_included(real_state):
    state.kv_set("runner", "x", {"line": f"contact {ADDR}", "list": [f"mailto:{ADDR}", 3, None],
                                 "deep": {"url": f"https://h.example/x?email={ADDR}&api_key=SECRETKEY123"}})
    got = state.kv_get("runner", "x")
    text = json.dumps(got)
    assert ADDR not in text and "SECRETKEY123" not in text
    assert got["list"][1:] == [3, None]
    with sqlite3.connect(str(state.DB_PATH)) as con:                     # what is persisted, not just read
        raw = con.execute("SELECT value FROM kv WHERE ns='runner' AND key='x'").fetchone()[0]
    assert ADDR not in raw


@pytest.mark.parametrize("value", [
    "3f786850e387550fdab836ed7e6dc881de23001b8c5f8c0e8c8e3a2d5a6b1c2d",       # a sha256 (the .ris manifest)
    "Crossref", {"walked_at": 1_800_000_000.5, "pdf_sha": "ab12"}, 7, 2.5, True, None,
    ["repository", "publisher"], "2026-10-07.2",
])
def test_kv_set_leaves_every_existing_kind_of_value_unchanged(real_state, value):
    state.kv_set("ns", "k", value)
    assert state.kv_get("ns", "k") == value


# ================================================================ 2. live_runs order and seq
def test_live_runs_carry_seq_in_registration_order_within_one_second(real_state, monkeypatch):
    monkeypatch.setattr(state, "_time", lambda: 1_800_000_000.0)      # both registrations in one second
    a = state.register_run("runner")
    b = state.register_run("runner")
    live = state.live_runs()
    assert [r["run_id"] for r in live] == [a, b]
    assert live[0]["seq"] < live[1]["seq"] and live[0]["started"] == live[1]["started"]


def test_seq_is_the_runs_rowid(real_state):
    a = state.register_run("runner")
    with sqlite3.connect(str(state.DB_PATH)) as con:
        rowid = con.execute("SELECT rowid FROM runs WHERE run_id=?", (a,)).fetchone()[0]
    assert state.live_runs()[0]["seq"] == rowid


# ================================================================ 3. the lease
def test_the_lease_outlasts_a_slow_80_mb_stream(real_state, monkeypatch):
    """One lease covers a whole transfer (net.request). At 133 KB/s an 80 MB stream takes 600 s: with
    the old 300 s lease the slot expired mid-transfer and a second claim got in past concurrency 1."""
    t = [1_800_000_000.0]
    monkeypatch.setattr(state, "_time", lambda: t[0])
    first = state._try_claim("slow.example", 0.0, None, 1, state.LEASE_S, None)
    assert isinstance(first, state.Slot)
    t[0] += net.DEFAULT_STREAM_CAP / 133_000                           # about 601 s into the stream
    second = state._try_claim("slow.example", 0.0, None, 1, state.LEASE_S, None)
    assert not isinstance(second, state.Slot)                          # still held: the caller waits


def test_lease_length_bound():
    assert state.LEASE_S >= net.DEFAULT_STREAM_CAP / 45_000           # 80 MB at 45 KB/s within one lease
    assert state.LEASE_S <= state.HEARTBEAT_STALE_S                    # a dead holder's run is reaped by then


def test_ledger_redact_obj_is_what_kv_uses(real_state, monkeypatch):
    seen = []
    real = ledger.redact_obj
    monkeypatch.setattr(ledger, "redact_obj", lambda x: (seen.append(x), real(x))[1])
    state.kv_set("ns", "k", {"a": "b"})
    assert seen and seen[0] == {"a": "b"}
