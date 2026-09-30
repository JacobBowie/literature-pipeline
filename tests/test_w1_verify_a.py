"""W1 verifier A: litpipe.net against the REAL litpipe.state, redaction edge cases, the prohibited
check, the ledger lock and the host table against the refactor scope section 2.2.

The W1-A1 net tests run against tests/netmock.FakeState and the W1-A2 state tests never call
litpipe.net, so the net-to-state contract (dispatch 0.5) was only checked by the suite passing.
Section 1 drives the real litpipe.net.request against the real litpipe.state (a temp sqlite file,
one virtual clock shared by both) and a loopback netmock server. Offline: no live network.
"""
import dataclasses
import json
import os
import subprocess
import sys
import threading
import textwrap
from pathlib import Path
from urllib.parse import quote

import pytest
import requests

from litpipe import hosts, ledger, net, preflight
from litpipe import state as real_state
from litpipe.hosts import HostPolicy, ProhibitedHost, RetryPolicy
from litpipe.outcomes import Kind
from tests.netmock import Reply

REPO = Path(__file__).resolve().parent.parent
A = "127.0.0.1"
B = "127.0.0.2"


# ================================================================ 1. real net x real state
@dataclasses.dataclass
class Real:
    state: object
    clock: object
    pacing: list
    env: object


@pytest.fixture
def real(net_env, tmp_path, monkeypatch):
    """net_env (virtual clock, temp ledger, temp projects.json, loopback-only transports) with
    litpipe.net wired to the real litpipe.state on a temp sqlite file. State and net share the
    virtual clock; state's pacing sleeps are recorded apart from net's retry sleeps."""
    clock = net_env.clock
    pacing = []

    def pace(s):
        pacing.append(round(s, 6))
        clock.t += max(s, 0.0)

    monkeypatch.setattr(real_state, "DB_PATH", tmp_path / "state" / real_state.DB_NAME)
    monkeypatch.setattr(real_state, "_current_run", None)
    monkeypatch.setattr(real_state, "_time", clock.time)
    monkeypatch.setattr(real_state, "_sleep", pace)
    monkeypatch.setattr(net, "STATE", real_state)
    return Real(real_state, clock, pacing, net_env)


def host_row(host):
    return next(h for h in real_state.status()["hosts"] if h["host"] == host)


def test_real_pacing_counts_from_the_end_of_a_slow_attempt(real, mock_server, monkeypatch):
    hosts.register(HostPolicy(A, min_interval_s=2.0))
    s = mock_server().script("/x", Reply(200, b"{}"))
    for name, fn in list(net._TRANSPORTS.items()):          # every attempt takes 5 virtual seconds
        def slow(method, url, *a, _fn=fn, **k):
            r = _fn(method, url, *a, **k)
            real.clock.t += 5.0
            return r
        monkeypatch.setitem(net._TRANSPORTS, name, slow)
    t0 = real.clock.time()
    assert net.request("GET", s.url("/x")).ok
    assert net.request("GET", s.url("/x")).ok
    # claim at t0, attempt ends at t0+5: the next slot is t0+7 (end + interval), not t0+2
    assert real.pacing == [2.0]
    assert real.clock.time() == pytest.approx(t0 + 12.0)
    assert real_state.day_count(A) == 2 and len(s.hits) == 2
    assert real_state.status()["leases"] == []               # every attempt released its lease
    assert net.CLOCK is real.clock and real.env.clock.sleeps == []   # no retry sleeps involved


def test_real_403_refuses_for_the_run_short_circuits_and_clears_at_finish_run(real, mock_server):
    run_id = real_state.register_run("verify")
    s = mock_server().script("/a", Reply(403)).script("/b", Reply(200, b"{}"))
    o = net.request("GET", s.url("/a"))
    assert o.kind is Kind.REFUSED and o.status == 403 and o.attempts == 1
    assert real_state.is_refused(A) and host_row(A)["refused"] == "run"
    assert real_state.kv_get("net.consecutive_403", f"{run_id}:{A}") == 1
    o2 = net.request("GET", s.url("/b"))                   # the refusal survives into the next call
    assert o2.kind is Kind.REFUSED and o2.attempts == 0 and s.hits_for("/b") == []
    real_state.finish_run(run_id, "ok")
    assert not real_state.is_refused(A)
    o3 = net.request("GET", s.url("/b"))
    assert o3.ok and len(s.hits_for("/b")) == 1
    assert [r["run_id"] for r in real.env.ledger_lines()][:2] == [run_id, run_id]


def test_real_403_counter_threshold_3_lives_in_state_kv_and_resets(real, mock_server):
    run_id = real_state.register_run("verify")
    hosts.register(HostPolicy(A, min_interval_s=0.0, refuse_after_consecutive_403=3))
    s = mock_server().script("/a", Reply(403)).script("/ok", Reply(200, b"{}"))
    key = f"{run_id}:{A}"
    net.request("GET", s.url("/a"))
    net.request("GET", s.url("/a"))
    assert real_state.kv_get("net.consecutive_403", key) == 2 and not real_state.is_refused(A)
    assert net.request("GET", s.url("/ok")).ok
    assert real_state.kv_get("net.consecutive_403", key) == 0
    for _ in range(3):
        net.request("GET", s.url("/a"))
    assert real_state.is_refused(A)
    real_state.finish_run(run_id, "ok")


def test_real_403_without_a_registered_run_holds_for_this_process(real, mock_server):
    s = mock_server().script("/a", Reply(403))
    net.request("GET", s.url("/a"))
    assert real_state.kv_get("net.consecutive_403", f"-:{A}") == 1
    assert real_state.is_refused(A) and host_row(A)["refused"] == "run"
    assert net.request("GET", s.url("/a")).attempts == 0


def test_real_retry_after_600_defers_the_host_and_the_next_call_sends_nothing(real, mock_server):
    s = mock_server().script("/x", Reply(429, headers={"Retry-After": "600"}), Reply(200, b"{}"))
    t0 = real.clock.time()
    o = net.request("GET", s.url("/x"))
    assert o.kind is Kind.DEFERRED and o.retry_after == 600 and o.attempts == 1
    assert real_state.deferred_until(A) == pytest.approx(t0 + 600)
    o2 = net.request("GET", s.url("/x"))                   # HostDeferred -> DEFERRED, nothing sent
    assert o2.kind is Kind.DEFERRED and o2.attempts == 0 and o2.retry_after == pytest.approx(600)
    assert len(s.hits) == 1 and real.env.clock.sleeps == []
    real.clock.t += 601
    assert net.request("GET", s.url("/x")).ok and len(s.hits) == 2


def test_real_budget_exhaustion_is_deferred_until_utc_midnight(real, mock_server):
    hosts.register(HostPolicy(A, min_interval_s=0.0, daily_budget=3))
    s = mock_server().script("/x", Reply(500), Reply(200, b"{}"))
    assert net.request("GET", s.url("/x")).attempts == 2    # a retried attempt counts too
    assert net.request("GET", s.url("/x")).ok
    assert real_state.day_count(A) == 3
    o = net.request("GET", s.url("/x"))
    now = real.clock.time()
    assert o.kind is Kind.DEFERRED and o.attempts == 0 and len(s.hits) == 3
    assert o.retry_after == pytest.approx(real_state._next_midnight(now) - now, abs=0.01)


def test_real_manual_refusal_short_circuits_with_zero_requests(real, mock_server):
    s = mock_server().script("/x", Reply(200, b"{}"))
    real_state.refuse(A, "arXiv 406 (manual)", persistence="manual")
    run_id = real_state.register_run("verify")
    o = net.request("GET", s.url("/x"))
    assert o.kind is Kind.REFUSED and o.attempts == 0 and s.hits == []
    real_state.finish_run(run_id, "ok")
    assert real_state.is_refused(A)                         # manual survives the run
    assert real_state.main(["--clear-refusal", A]) == 0
    assert net.request("GET", s.url("/x")).ok and len(s.hits) == 1


def test_real_manual_persistence_flows_from_the_host_row(real, mock_server):
    hosts.register(dataclasses.replace(hosts.policy("export.arxiv.org"), host=A, min_interval_s=0.0))
    run_id = real_state.register_run("verify")
    s = mock_server().script("/api/query", Reply(406))
    assert net.request("GET", s.url("/api/query")).kind is Kind.REFUSED
    assert host_row(A)["refused"] == "manual"
    real_state.finish_run(run_id, "ok")
    assert real_state.is_refused(A)


def test_real_pmc_ncbi_first_429_refuses_for_the_run(real, mock_server):
    hosts.register(dataclasses.replace(hosts.policy("pmc.ncbi.nlm.nih.gov"), host=A, min_interval_s=0.0))
    s = mock_server().script("/tools/idconv/api/v1/articles/", Reply(429, "<html>429</html>"))
    o = net.request("GET", s.url("/tools/idconv/api/v1/articles/"), params={"ids": "10.1/x"})
    assert o.kind is Kind.REFUSED and o.attempts == 1 and real.env.clock.sleeps == []
    assert host_row(A)["refused"] == "run"
    assert net.request("GET", s.url("/tools/idconv/api/v1/articles/")).attempts == 0


def test_real_release_ok_flag_per_attempt_and_hop(real, mock_server, monkeypatch):
    seen = []
    orig = real_state.release

    def spy(host, ok=True, slot=None):
        seen.append((host, ok))
        return orig(host, ok, slot)
    monkeypatch.setattr(real_state, "release", spy)
    hosts.register(HostPolicy(A, min_interval_s=0.0, redirect_allow=(B,)))
    a, b = mock_server(A), mock_server(B)
    b.script("/gone", Reply(404))
    a.script("/r", Reply(302, headers={"Location": b.url("/gone")}))
    o = net.request("GET", a.url("/r"))
    assert o.kind is Kind.NO_MATCH and o.attempts == 2
    assert seen == [(A, True), (B, False)]                  # 3xx counts as ok, 404 does not
    assert host_row(B)["consecutive_fail"] == 1 and real_state.day_count(B) == 1
    a.script("/drop", Reply(close=True))
    hosts.register(HostPolicy(A, min_interval_s=0.0, retry=RetryPolicy(transport_retries=0)))
    assert net.request("GET", a.url("/drop")).kind is Kind.TRANSPORT
    assert seen[-1] == (A, False)
    a.script("/ok", Reply(200, b"{}"))
    assert net.request("GET", a.url("/ok")).ok and seen[-1] == (A, True)
    assert real_state.status()["leases"] == []


def test_real_lease_is_released_when_the_transport_raises(real, monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("transport bug")
    monkeypatch.setitem(net._TRANSPORTS, "requests", boom)
    with pytest.raises(RuntimeError):
        net.request("GET", f"http://{A}:9/x")
    assert real_state.status()["leases"] == []


def test_real_ledger_and_state_hold_no_email_through_the_ncbi_identity(real, mock_server):
    run_id = real_state.register_run("verify")
    hosts.register(HostPolicy(A, min_interval_s=0.0, identity="ncbi"))
    s = mock_server()
    email = os.environ["LITPIPE_EMAIL"]
    s.script("/r", Reply(301, headers={"Location": f"/t?tool=x&email={quote(email, safe='')}"}))
    s.script("/t", Reply(429, headers={"Retry-After": "900"}))
    o = net.request("GET", s.url("/r"))
    assert o.kind is Kind.DEFERRED and s.hits_for("/r")[0].query["email"] == [email]
    real_state.finish_run(run_id, f"done for {email}")
    dump = json.dumps(real_state.status()) + real.env.ledger_text() + o.detail
    for needle in (email, quote(email, safe=""), "litpipe-test.org", "email=", "mailto:"):
        assert needle not in dump, needle


def test_preflight_through_real_net_and_state_reads_a_real_response(real, mock_server, monkeypatch):
    """The preflight tests hand it dict payloads; litpipe.net returns a net.Response object."""
    up, cr = mock_server(A), mock_server(B)
    hosts.register(dataclasses.replace(hosts.policy("api.unpaywall.org"), host=A, min_interval_s=0.0))
    hosts.register(dataclasses.replace(hosts.policy("api.crossref.org"), host=B, min_interval_s=0.0))
    doi = preflight.PREFLIGHT_DOI
    up.script(f"/v2/{doi}", Reply(200, json.dumps({"doi": doi, "is_oa": True}),
                                  headers={"Content-Type": "application/json"}))
    cr.script(f"/works/{doi}", Reply(200, b'{"status":"ok"}', headers={
        "Content-Type": "application/json", "x-api-pool": "polite-single", "x-rate-limit-limit": "10",
        "x-rate-limit-interval": "1s", "x-concurrency-limit": "3"}))

    def via_mock(method, url, **kw):
        url = url.replace(f"https://{preflight.UNPAYWALL_HOST}", up.url("")).replace(
            f"https://{preflight.CROSSREF_HOST}", cr.url(""))
        return net.request(method, url, **kw)
    outs = preflight.run(env=os.environ, request=via_mock)
    assert [(o.payload["check"], o.kind) for o in outs if o.kind is not Kind.OK] == []
    email = os.environ["LITPIPE_EMAIL"]
    assert up.hits[0].query["email"] == [email]                       # identity by litpipe.net
    assert f"mailto:{email}" in cr.hits[0].headers["User-Agent"]
    assert email not in real.env.ledger_text()


# ================================================================ 2. the prohibited check
@pytest.mark.parametrize("url", [
    "https://pmc.ncbi.nlm.nih.gov/./articles/PMC9817969/",
    "https://pmc.ncbi.nlm.nih.gov/tools/../articles/PMC9817969/",
    "https://pmc.ncbi.nlm.nih.gov/%61rticles/PMC9817969/",
    "https://www.ncbi.nlm.nih.gov/pmc/./articles/PMC9817969/",
])
def test_prohibited_check_sees_the_path_requests_will_send(net_env, monkeypatch, url):
    """requests normalises dot segments and unreserved %-escapes before sending (probed), so the
    rule must match the normalised path, not the raw string."""
    sent_path = requests.Request("GET", url).prepare().path_url
    assert any(sent_path.startswith(r.path_prefix) for r in hosts.PROHIBITED
               if r.host == hosts.host_of(url))                 # what would really go out
    sent = []
    monkeypatch.setitem(net._TRANSPORTS, "requests", lambda *a, **k: sent.append(a) or net._Raw(200))
    monkeypatch.setitem(net._TRANSPORTS, "urllib", lambda *a, **k: sent.append(a) or net._Raw(200))
    with pytest.raises(ProhibitedHost):
        net.request("GET", url)
    assert sent == []


@pytest.mark.parametrize("url", [
    "https://pmc.ncbi.nlm.nih.gov./articles/PMC9817969/",
    "https://europepmc.org./articles/PMC9817969?pdf=render",
    "https://cdn.ncbi.nlm.nih.gov./pmc/blobs/x.pdf",
])
def test_prohibited_check_and_policy_ignore_a_trailing_dot_host(net_env, url):
    """An absolute FQDN resolves to the same host; litpipe.state already strips the dot
    (_norm_host), litpipe.hosts does not, so the rule and the host row both miss."""
    assert hosts.prohibited(url) is not None
    bare = url.split("/")[2].rstrip(".")
    assert hosts.policy(url).host == bare
    assert hosts.policy(url).known == (bare in hosts.rows())   # pmc.ncbi keeps its urllib row


# ================================================================ 3. Retry-After on a redirect
def test_retry_after_on_a_3xx_delays_the_redirected_request(net_env, mock_server):
    """RFC 9110 section 10.2.3: 'When sent with any 3xx (Redirection) response, Retry-After
    indicates the minimum time that the user agent is asked to wait before issuing the redirected
    request.'"""
    s = mock_server().script("/r", Reply(301, headers={"Location": "/t", "Retry-After": "10"}))
    s.script("/t", Reply(200, b"{}"))
    o = net.request("GET", s.url("/r"))
    assert o.ok and o.attempts == 2
    assert sum(net_env.clock.sleeps) >= 10


# ================================================================ 4. redaction
def test_redact_json_shaped_api_key():
    out = ledger.redact('{"api_key": "SEKRIT123", "rows": 1}')
    assert "SEKRIT123" not in out


def test_redact_triple_encoded_configured_email(monkeypatch):
    e = "jane.doe@uni-test.edu"
    monkeypatch.setenv("LITPIPE_EMAIL", e)
    t3 = quote(quote(quote(e, safe=""), safe=""), safe="")      # jane.doe%252540uni-test.edu
    out = ledger.redact(f"Location: /login?next=%2Fsso%253Fnext%253D%25252Fq%25253Femail%25253D{t3}")
    assert "jane.doe" not in out and "uni-test" not in out


@pytest.mark.parametrize("text", [
    "https://h/x?contact_email=jane.doe@uni.edu",
    "Mailto: jane.doe@uni.edu",
    "mailto: jane.doe@uni.edu",
])
def test_redact_leaves_no_email_eq_or_mailto_colon_for_the_canary(text):
    """Dispatcher convention: no persisted string contains `email=` or `mailto:`; the W4-B canary
    is a plain grep, so a redacted value behind one of these still reads as a leak."""
    out = ledger.redact(text).lower()
    assert "jane.doe" not in out and "email=" not in out and "mailto:" not in out


@pytest.mark.parametrize("text,configured", [
    ("jane.doe%252Btag%2540uni.edu", None),                 # %-encoded plus, double-encoded
    ("echo: jane.doe lit@uni-test.edu", "jane.doe+lit@uni-test.edu"),   # a '+' form-decoded to ' '
])
def test_redact_takes_the_whole_local_part(monkeypatch, text, configured):
    if configured:
        monkeypatch.setenv("LITPIPE_EMAIL", configured)
    assert "jane.doe" not in ledger.redact(text)


# ================================================================ 5. the ledger lock
def test_ledger_append_waits_for_the_file_lock(tmp_path, monkeypatch):
    monkeypatch.setattr(ledger, "LEDGER_DIR", tmp_path / "led")
    d = ledger.ledger_dir()
    done = threading.Event()

    def writer():
        ledger.write({"host": "x", "note": "thread"}, strict=True)
        done.set()
    with ledger._file_lock(d / ".ledger.lock"):
        t = threading.Thread(target=writer)
        t.start()
        assert not done.wait(0.5), "the append did not wait for the lock"
    t.join(10)
    assert done.is_set() and len(ledger.read("*")) == 1


def test_ledger_appends_from_processes_never_interleave(tmp_path):
    code = textwrap.dedent("""
        import sys
        from pathlib import Path
        sys.path.insert(0, sys.argv[1])
        from litpipe import ledger
        ledger.LEDGER_DIR = Path(sys.argv[2])
        for i in range(150):
            ledger.write({"host": "h", "who": sys.argv[3], "i": i, "pad": "x" * 3000}, strict=True)
    """)
    env = {k: v for k, v in os.environ.items() if k != "LITPIPE_RUN_ID"}
    procs = [subprocess.Popen([sys.executable, "-c", code, str(REPO), str(tmp_path / "led"), str(n)],
                              env=env, stderr=subprocess.PIPE, text=True) for n in range(3)]
    for p in procs:
        _, err = p.communicate(timeout=120)
        assert p.returncode == 0, err
    lines = [ln for f in (tmp_path / "led").glob("*.jsonl") for ln in f.read_text("utf-8").splitlines()]
    recs = [json.loads(ln) for ln in lines]                  # a torn line fails to parse
    assert len(recs) == 450 and {(r["who"], r["i"]) for r in recs} == {
        (str(n), i) for n in range(3) for i in range(150)}


# ================================================================ 6. the no-live-network guard
def test_autouse_guard_blocks_a_live_host_without_net_env(tmp_path, monkeypatch):
    """Dispatch 0.2: no live network in tests. The conftest autouse fixture swaps the ledger and
    the state for every test, but the loopback-only transport guard lives in `net_env` alone, so a
    W2 test that calls litpipe.net without that fixture would reach the real host."""
    from litpipe import config
    cfg = tmp_path / "projects.json"
    cfg.write_text(json.dumps({"state_dir": str(tmp_path / "state"), "projects": {}}), encoding="utf-8")
    monkeypatch.setattr(config, "CONFIG_PATH", cfg)
    reached = []

    def would_send(*a, **k):
        reached.append(a)
        raise RuntimeError("stopped before the socket")
    monkeypatch.setattr(requests, "request", would_send)
    with pytest.raises(AssertionError, match="live network"):
        net.request("GET", "https://api.crossref.org/works/10.1371/journal.pone.0012033")
    assert reached == []


# ================================================================ 7. host table vs scope 2.2
def test_host_table_matches_refactor_scope_2_2():
    p = hosts.policy
    expect = {   # host: (min_interval_s, identity, transport); scope 2.2 runner settings
        "api.crossref.org": (0.34, "ua", "requests"),
        "api.datacite.org": (0.5, "ua", "requests"),
        "doi.org": (1.0, "ua", "requests"),
        "api.unpaywall.org": (1.0, "email_param", "requests"),
        "pmc.ncbi.nlm.nih.gov": (1.0, "ncbi", "urllib"),
        "eutils.ncbi.nlm.nih.gov": (0.4, "ncbi", "requests"),
        "pmc-oa-opendata.s3.amazonaws.com": (0.25, "anonymous", "requests"),
        "www.ncbi.nlm.nih.gov": (0.5, "ncbi", "requests"),
        "www.ebi.ac.uk": (1.0, "ua", "requests"),
        "export.arxiv.org": (3.5, "ua", "requests"),
        "api.osf.io": (36.0, "ua", "requests"),
        "sportrxiv.org": (3.0, "ua", "requests"),
        "api.biorxiv.org": (1.0, "ua", "requests"),
        "api.semanticscholar.org": (6.5, "ua", "requests"),
    }
    for h, (iv, ident, tr) in expect.items():
        row = p(h)
        assert row.known and (row.min_interval_s, row.identity, row.transport) == (iv, ident, tr), h
        assert row.concurrency == 1, h
    assert p("api.unpaywall.org").status_kinds == {422: Kind.CONFIG, 410: Kind.CONFIG}
    assert p("export.arxiv.org").refusal_persistence == "manual"
    for h in ("export.arxiv.org", "pmc.ncbi.nlm.nih.gov"):
        assert 429 not in p(h).retry.statuses and 429 in p(h).refuse_host_statuses, h
    assert {406, 429} <= p("export.arxiv.org").refuse_host_statuses
    assert p("doi.org").https_only and not p("doi.org").allows_redirect_to("159.226.100.1")
    unknown = p("https://unknown-publisher.test/x.pdf")
    assert (unknown.known, unknown.min_interval_s, unknown.concurrency) == (False, 2.0, 1)


def test_never_automated_rules_are_exactly_the_dispatch_list():
    got = {(r.host, r.path_prefix, r.unless_switch) for r in hosts.PROHIBITED}
    assert got == {
        ("europepmc.org", "/", None), ("www.europepmc.org", "/", None),
        ("pmc.ncbi.nlm.nih.gov", "/articles/", None),
        ("cdn.ncbi.nlm.nih.gov", "/", None),
        ("www.ncbi.nlm.nih.gov", "/pmc/articles/", None),
        ("www.biorxiv.org", "/", "biorxiv_pdf_allowed"), ("biorxiv.org", "/", "biorxiv_pdf_allowed"),
        ("www.medrxiv.org", "/", "biorxiv_pdf_allowed"), ("medrxiv.org", "/", "biorxiv_pdf_allowed"),
        ("arxiv.org", "/pdf/", "arxiv_pdf_allowed"), ("www.arxiv.org", "/pdf/", "arxiv_pdf_allowed"),
    }
    assert hosts.prohibited("https://pmc.ncbi.nlm.nih.gov/tools/idconv/api/v1/articles/?ids=1") is None
    assert hosts.prohibited("https://pmc.ncbi.nlm.nih.gov/articles/PMC1/") is not None
