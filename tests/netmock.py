"""netmock: offline doubles for litpipe.net tests (dispatch W1-A1 step 4).

  MockServer   a localhost http.server in a thread serving scripted Reply sequences per path
               (status, headers, body, delay, or a dropped connection); the last reply of a path
               repeats; every request is recorded as a Hit.
  FakeClock    virtual monotonic/time/sleep: retry waits advance virtual time and are recorded, so a
               6-retry backoff test takes milliseconds.
  FakeState    the documented litpipe.state API (dispatch 0.5) in memory: pacing on the virtual
               clock (spacing counted from the end of the previous attempt, the interval from
               litpipe.hosts), daily budgets, deferrals, run/manual refusals, kv, run registry.

Fixtures that wire these into litpipe.net live in tests/conftest.py (`net_env`, `mock_server`).
Use a second loopback address (127.0.0.2) for a "different host" without DNS.
"""
from __future__ import annotations

import socket
import socketserver
import threading
import time
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from litpipe import hosts


@dataclass
class Reply:
    status: int = 200
    body: bytes | str = b""
    headers: dict = field(default_factory=dict)
    delay: float = 0.0        # real seconds before answering (timeout tests)
    close: bool = False       # drop the connection without any response (transport failure)


@dataclass
class Hit:
    server: str
    method: str
    path: str
    query: dict
    headers: dict
    body: bytes


class _Server(ThreadingHTTPServer):
    daemon_threads = True
    handler_errors = 0

    def server_bind(self):
        # HTTPServer.server_bind calls socket.getfqdn(host), a reverse DNS lookup: it took 4.9 s for
        # 127.0.0.2 on Windows (probe 2026-09-30) and would put DNS traffic into the tests.
        socketserver.TCPServer.server_bind(self)
        host, port = self.server_address[:2]
        self.server_name, self.server_port = host, port

    def handle_error(self, request, client_address):
        # a client that stops reading at its byte cap resets the socket mid-write: expected
        type(self).handler_errors += 1


class MockServer:
    def __init__(self, host="127.0.0.1"):
        self.host = host
        self.hits: list[Hit] = []
        self._scripts: dict[str, list[Reply]] = {}
        self._lock = threading.Lock()
        self._srv = _Server((host, 0), self._handler())
        self.port = self._srv.server_address[1]
        self._thread = threading.Thread(target=self._srv.serve_forever, kwargs={"poll_interval": 0.05},
                                        daemon=True)
        self._thread.start()

    def url(self, path="/"):
        return f"http://{self.host}:{self.port}{path}"

    def script(self, path, *replies: Reply):
        with self._lock:
            self._scripts[path] = list(replies)
        return self

    def hits_for(self, path=None):
        return [h for h in self.hits if path is None or h.path == path]

    def stop(self):
        self._srv.shutdown()
        self._srv.server_close()

    def _next(self, path) -> Reply:
        with self._lock:
            seq = self._scripts.get(path)
            if not seq:
                return Reply(599, f"netmock: no script for {path}")
            return seq.pop(0) if len(seq) > 1 else seq[0]

    def _handler(self):
        mock = self

        class H(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.0"

            def log_message(self, *a):
                pass

            def _handle(self):
                parts = urlsplit(self.path)
                n = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(n) if n else b""
                mock.hits.append(Hit(mock.host, self.command, parts.path, parse_qs(parts.query),
                                     dict(self.headers.items()), body))
                r = mock._next(parts.path)
                if r.delay:
                    time.sleep(r.delay)
                if r.close:
                    self.close_connection = True
                    try:
                        self.connection.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                    return
                data = r.body.encode("utf-8") if isinstance(r.body, str) else r.body
                self.send_response(r.status)
                for k, v in r.headers.items():
                    self.send_header(k, v)
                if not any(k.lower() == "content-length" for k in r.headers):
                    self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                if self.command != "HEAD" and data:
                    self.wfile.write(data)

            do_GET = do_POST = do_HEAD = do_PUT = _handle

        return H


class FakeClock:
    def __init__(self, start=1_800_000_000.0):
        self.t = float(start)
        self.sleeps: list[float] = []

    def monotonic(self):
        return self.t

    def time(self):
        return self.t

    def sleep(self, s):
        if s < 0:
            raise ValueError(f"negative sleep {s}")
        self.sleeps.append(s)
        self.t += s


class BudgetExhausted(Exception):
    def __init__(self, host, retry_after=None):
        self.host, self.retry_after = host, retry_after
        super().__init__(f"{host}: " + ("deferred" if retry_after is not None else "daily budget spent"))


class FakeState:
    """In-memory litpipe.state (dispatch 0.5 API). `calls` logs every API call in order."""
    BudgetExhausted = BudgetExhausted

    def __init__(self, clock=None, budgets=None, interval=None):
        self.clock = clock or FakeClock()
        self.budgets = dict(budgets or {})
        self.interval = interval            # override every host's min_interval_s
        self.refused: dict[str, tuple] = {}
        self.deferred: dict[str, float] = {}
        self.counts: dict[str, int] = {}
        self.next_allowed: dict[str, float] = {}
        self.kv: dict[tuple, object] = {}
        self.acquired: list[tuple] = []     # (host, virtual time the slot was granted)
        self.released: list[tuple] = []     # (host, ok)
        self.pacing_waits: list[float] = []
        self.calls: list[tuple] = []
        self.runs: dict[str, dict] = {}

    def _interval(self, host):
        return self.interval if self.interval is not None else hosts.policy(host).min_interval_s

    def acquire(self, host):
        self.calls.append(("acquire", host))
        now = self.clock.time()
        until = self.deferred.get(host)
        if until is not None and until > now:
            raise BudgetExhausted(host, retry_after=until - now)
        budget = self.budgets.get(host, hosts.policy(host).daily_budget)
        if budget is not None and self.counts.get(host, 0) >= budget:
            raise BudgetExhausted(host)
        wait = self.next_allowed.get(host, 0.0) - now
        if wait > 0:                        # pacing: advance virtual time, kept apart from clock.sleeps
            self.pacing_waits.append(wait)
            self.clock.t += wait
        self.counts[host] = self.counts.get(host, 0) + 1
        self.acquired.append((host, self.clock.time()))

    def release(self, host, ok):
        self.calls.append(("release", host, ok))
        self.released.append((host, ok))
        self.next_allowed[host] = self.clock.time() + self._interval(host)   # from the END of the attempt

    LEASE_S = 1800.0                        # litpipe.state's lease length (W5-C1: net renews during long bodies)

    def renew(self, slot):
        self.calls.append(("renew", slot))
        return True

    def defer(self, host, until):
        self.calls.append(("defer", host, until))
        self.deferred[host] = until

    def refuse(self, host, reason, persistence="run"):
        self.calls.append(("refuse", host, reason, persistence))
        self.refused[host] = (reason, persistence)

    def is_refused(self, host):
        return host in self.refused

    def clear_refusal(self, host):
        self.refused.pop(host, None)

    def day_count(self, host):
        return self.counts.get(host, 0)

    def kv_get(self, ns, key):
        return self.kv.get((ns, key))

    def kv_set(self, ns, key, value, ttl_s=None):
        self.kv[(ns, key)] = value

    def register_run(self, kind):
        run_id = f"run{len(self.runs) + 1}"
        self.runs[run_id] = {"kind": kind, "status": None}
        return run_id

    def heartbeat(self, run_id):
        pass

    def finish_run(self, run_id, status):
        self.runs.setdefault(run_id, {})["status"] = status
        self.refused = {h: v for h, v in self.refused.items() if v[1] == "manual"}

    def live_runs(self):
        return [dict(run_id=k, **v) for k, v in self.runs.items() if v.get("status") is None]
