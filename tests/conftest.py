"""Make the top-level pipeline scripts importable as modules from tests/, and give every test a
network layer that cannot touch live state (dispatch 0.2)."""
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Hosts a net_env test may reach: loopback mocks, and a name whose DNS failure a test simulates.
OFFLINE_HOSTS = frozenset({"127.0.0.1", "127.0.0.2", "127.0.0.3", "dns-fail.test"})


@pytest.fixture(autouse=True)
def _litpipe_net_isolation(tmp_path_factory, monkeypatch):
    """Safety net for every test: litpipe.net writes its ledger under a temp directory and uses an
    in-memory FakeState, never the real <state_dir>/litpipe_state.sqlite or ledger/. A test that
    wants real state passes `state=` to net.request (or monkeypatches net.STATE) itself. It does not
    block the network: tests use MockServer (tests/netmock.py) for that."""
    try:
        from litpipe import ledger, net
        from tests import netmock
    except ImportError:          # a broken litpipe.net shows up in its own tests, not in all of them
        yield
        return
    monkeypatch.setattr(ledger, "LEDGER_DIR", tmp_path_factory.mktemp("ledger"))
    monkeypatch.setattr(net, "STATE", netmock.FakeState(netmock.FakeClock()))
    # No live network (dispatch 0.2), with or without net_env: a transport asked for any host but
    # the loopback mocks fails the test instead of sending. Tests that stub a transport replace
    # these entries and never reach the guard.
    for name, fn in list(net._TRANSPORTS.items()):
        def guarded(method, url, *a, _fn=fn, **k):
            host = urlsplit(url).hostname
            if host not in OFFLINE_HOSTS:
                raise AssertionError(f"live network attempted: {method} {host}")
            return _fn(method, url, *a, **k)
        monkeypatch.setitem(net._TRANSPORTS, name, guarded)
    yield


@dataclass
class NetEnv:
    clock: object
    state: object
    ledger_dir: Path
    cfg_path: Path

    def ledger_lines(self):
        from litpipe import ledger
        return ledger.read("*")

    def ledger_text(self):
        return "".join(p.read_text(encoding="utf-8") for p in sorted(self.ledger_dir.glob("*.jsonl")))

    def write_config(self, **top):
        """Rewrite the temp projects.json with extra top-level keys (e.g. hosts={...})."""
        cfg = {"state_dir": str(self.cfg_path.parent / "state"), "projects": {}, **top}
        self.cfg_path.write_text(json.dumps(cfg), encoding="utf-8")


@pytest.fixture
def net_env(tmp_path, monkeypatch):
    """litpipe.net wired to a virtual clock (retry waits recorded, not slept), a FakeState, a temp
    ledger and a temp projects.json (state_dir under tmp_path); LITPIPE_EMAIL set to a test
    address; jitter 0; the host table reset before and after. Mock hosts 127.0.0.1 and 127.0.0.2
    get a 0 s interval so pacing never hides a backoff wait."""
    from litpipe import config, hosts, ledger, net
    from tests import netmock

    clock = netmock.FakeClock()
    state = netmock.FakeState(clock)
    cfg_path = tmp_path / "projects.json"
    ledger_dir = tmp_path / "ledger"
    env = NetEnv(clock, state, ledger_dir, cfg_path)
    env.write_config()
    monkeypatch.setattr(config, "CONFIG_PATH", cfg_path)
    monkeypatch.setattr(ledger, "LEDGER_DIR", ledger_dir)
    monkeypatch.setattr(ledger, "RUN_ID", None)
    monkeypatch.delenv("LITPIPE_RUN_ID", raising=False)
    monkeypatch.setattr(net, "CLOCK", clock)
    monkeypatch.setattr(net, "STATE", state)
    monkeypatch.setattr(net, "RANDOM", lambda: 0.0)
    monkeypatch.setenv("LITPIPE_EMAIL", "tester@litpipe-test.org")
    for var in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("NO_PROXY", "*")
    monkeypatch.setattr(net, "_OPENER", None)     # rebuilt without proxy handlers
    # No live network: a transport asked for any host but the loopback mocks (or the DNS-failure
    # test name, whose resolution the test patches) fails the test instead of sending.
    for name, fn in list(net._TRANSPORTS.items()):
        def guarded(method, url, *a, _fn=fn, **k):
            host = urlsplit(url).hostname
            if host not in OFFLINE_HOSTS:
                raise AssertionError(f"live network attempted: {method} {host}")
            return _fn(method, url, *a, **k)
        monkeypatch.setitem(net._TRANSPORTS, name, guarded)
    hosts.reset()
    for h in ("127.0.0.1", "127.0.0.2"):
        hosts.register(hosts.HostPolicy(h, min_interval_s=0.0))
    yield env
    hosts.reset()


@pytest.fixture
def mock_server():
    """Factory: mock_server(host="127.0.0.1") -> a running netmock.MockServer, stopped at teardown."""
    from tests import netmock
    servers = []

    def make(host="127.0.0.1"):
        s = netmock.MockServer(host)
        servers.append(s)
        return s

    yield make
    for s in servers:
        s.stop()
