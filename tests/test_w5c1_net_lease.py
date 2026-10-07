"""W5-C1 item 10 (the W4-A forward): litpipe.net renews the attempt's state lease at least every
LEASE_S / 3 while it drains a body (state.renew), so a slow stream never outlives its lease. A fake
clock that advances on every read stands in for a slow stream; the server is the loopback mock."""
import pytest

from litpipe import hosts, net, state
from litpipe.hosts import HostPolicy
from tests import netmock
from tests.netmock import Reply

A = "127.0.0.1"
BODY = b"%PDF-1.4\n" + b"x" * (net.CHUNK * 6)          # seven chunks


class StepClock(netmock.FakeClock):
    """monotonic() advances `step` seconds on every call (one call per drained chunk)."""

    def __init__(self, step):
        super().__init__()
        self.step = step

    def monotonic(self):
        self.t += self.step
        return self.t


class RenewingState(netmock.FakeState):
    LEASE_S = 1800.0

    def __init__(self, clock):
        super().__init__(clock)
        self.renewed = []

    def renew(self, slot):
        self.renewed.append(slot)
        return True


@pytest.mark.parametrize("transport", ["requests", "urllib"])
def test_a_long_stream_renews_its_lease_every_third_of_lease_s(net_env, mock_server, monkeypatch, transport):
    hosts.register(HostPolicy(A, min_interval_s=0.0, transport=transport))
    clock = StepClock(step=RenewingState.LEASE_S / 3 + 1)          # each chunk "takes" a third of the lease
    st = RenewingState(clock)
    monkeypatch.setattr(net, "CLOCK", clock)
    s = mock_server().script("/pdf", Reply(200, BODY, {"Content-Type": "application/pdf"}))
    o = net.request("GET", s.url("/pdf"), stream=True, state=st)
    assert o.ok and o.payload.content == BODY
    assert len(st.renewed) >= 6                                       # every chunk after the first
    assert ("release", A, True) in st.calls


def test_a_quick_transfer_renews_nothing(net_env, mock_server, monkeypatch):
    hosts.register(HostPolicy(A, min_interval_s=0.0))
    clock = StepClock(step=10.0)                                      # 7 chunks x 10 s: far under 600 s
    st = RenewingState(clock)
    monkeypatch.setattr(net, "CLOCK", clock)
    s = mock_server().script("/pdf", Reply(200, BODY, {"Content-Type": "application/pdf"}))
    assert net.request("GET", s.url("/pdf"), stream=True, state=st).ok
    assert st.renewed == []


class NoRenewState(netmock.FakeState):
    renew = None                      # a state with no renew (net reads it through getattr)


def test_a_state_without_renew_is_simply_not_renewed(net_env, mock_server, monkeypatch):
    hosts.register(HostPolicy(A, min_interval_s=0.0))
    clock = StepClock(step=10_000.0)
    monkeypatch.setattr(net, "CLOCK", clock)
    s = mock_server().script("/pdf", Reply(200, BODY, {"Content-Type": "application/pdf"}))
    assert net.request("GET", s.url("/pdf"), stream=True, state=NoRenewState(clock)).ok


def test_state_renew_pushes_the_lease_expiry(tmp_path, monkeypatch):
    monkeypatch.setattr(state, "DB_PATH", tmp_path / "s.sqlite")
    t = {"now": 1_000_000.0}
    monkeypatch.setattr(state, "_time", lambda: t["now"])
    slot = state.acquire("lease.test", interval=0, budget=None, concurrency=1)

    def expires():
        with state._read() as con:
            return con.execute("SELECT expires FROM leases WHERE id=?", (slot.lease_id,)).fetchone()
    assert expires() == (1_000_000.0 + state.LEASE_S,)
    t["now"] += 1500
    assert state.renew(slot) is True and expires() == (t["now"] + state.LEASE_S,)
    state.release("lease.test", slot=slot)
    assert state.renew(slot) is False                                  # released: nothing to renew
