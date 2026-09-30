"""litpipe.net / litpipe.hosts / litpipe.ledger (dispatch W1-A1). Offline: every request goes to a
tests/netmock.MockServer on loopback, or to a stubbed transport; retry waits run on a virtual clock."""
import dataclasses
import json
import os
import socket
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from pathlib import Path

import pytest
from requests.structures import CaseInsensitiveDict

from litpipe import __version__, hosts, ledger, net
from litpipe.hosts import HostPolicy, ProhibitedHost, RetryPolicy
from litpipe.outcomes import Kind
from tests.netmock import Reply

REPO = Path(__file__).resolve().parent.parent
A = "127.0.0.1"
B = "127.0.0.2"
TRANSPORTS = ("requests", "urllib")


def pol(host=A, **kw):
    """Register a test policy (0 s pacing unless given)."""
    kw.setdefault("min_interval_s", 0.0)
    return hosts.register(HostPolicy(host, **kw))


def clone(row_host, onto=A, **kw):
    """Register a copy of a real table row onto a loopback mock host."""
    return hosts.register(dataclasses.replace(hosts.policy(row_host), host=onto, min_interval_s=0.0, **kw))


def gaps(state, host=A):
    ts = [t for h, t in state.acquired if h == host]
    return [round(b - a, 6) for a, b in zip(ts, ts[1:])]


def stub_transport(monkeypatch, status=200, body=b'{"ok": true}', headers=None):
    """Replace both transports; returns the list of (method, url, headers) actually 'sent'."""
    sent = []

    def fake(method, url, hdrs, body_, timeout, max_bytes):
        sent.append((method, url, dict(hdrs)))
        h = CaseInsensitiveDict(headers or {"Content-Type": "application/json"})
        return net._Raw(status, h, body, body[:net.CHUNK], len(body))

    monkeypatch.setitem(net._TRANSPORTS, "requests", fake)
    monkeypatch.setitem(net._TRANSPORTS, "urllib", fake)
    return sent


# ================================================================ acceptance (dispatch W1-A1)
@pytest.mark.parametrize("transport", TRANSPORTS)
def test_429_without_retry_after_waits_at_least_2s(net_env, mock_server, transport):
    pol(transport=transport)
    s = mock_server().script("/x", Reply(429), Reply(200, b"{}"))
    o = net.request("GET", s.url("/x"))
    assert o.ok and o.attempts == 2 and len(s.hits_for("/x")) == 2
    assert net_env.clock.sleeps == [2.0]
    assert gaps(net_env.state) == [2.0]          # virtual send-to-send gap
    assert net_env.ledger_lines()[1]["wait_s"] >= 2.0


def test_first_wait_with_full_jitter_stays_in_bounds(net_env, mock_server, monkeypatch):
    monkeypatch.setattr(net, "RANDOM", lambda: 0.999)
    s = mock_server().script("/x", Reply(429), Reply(429), Reply(200))
    assert net.request("GET", s.url("/x")).ok
    w1, w2 = net_env.clock.sleeps
    assert 2.0 <= w1 < 2.5 and 4.0 <= w2 < 4.5


@pytest.mark.parametrize("transport", TRANSPORTS)
def test_500_is_retried(net_env, mock_server, transport):
    pol(transport=transport)
    s = mock_server().script("/x", Reply(500), Reply(200, b"{}"))
    o = net.request("GET", s.url("/x"))
    assert o.ok and o.attempts == 2 and net_env.clock.sleeps == [2.0]


def test_retry_after_600_defers_host_and_returns_deferred(net_env, mock_server):
    s = mock_server().script("/x", Reply(429, headers={"Retry-After": "600"}), Reply(200))
    t0 = net_env.clock.time()
    o = net.request("GET", s.url("/x"))
    assert o.kind is Kind.DEFERRED and o.retry_after == 600 and o.status == 429 and o.attempts == 1
    assert net_env.state.deferred[A] == pytest.approx(t0 + 600)
    assert net_env.clock.sleeps == []            # never sleeps inside the row
    o2 = net.request("GET", s.url("/x"))         # the state now refuses a slot: nothing sent
    assert o2.kind is Kind.DEFERRED and o2.attempts == 0 and o2.retry_after == pytest.approx(600)
    assert len(s.hits_for("/x")) == 1
    assert [r["decision"] for r in net_env.ledger_lines()] == ["deferred", "not_sent"]


def test_first_403_refuses_host_for_run_and_next_call_sends_nothing(net_env, mock_server):
    s = mock_server().script("/a", Reply(403)).script("/b", Reply(200))
    o = net.request("GET", s.url("/a"))
    assert o.kind is Kind.REFUSED and o.status == 403 and o.attempts == 1
    assert net_env.state.refused[A][1] == "run"
    o2 = net.request("GET", s.url("/b"))
    assert o2.kind is Kind.REFUSED and o2.attempts == 0
    assert s.hits_for("/b") == []


def test_403_threshold_3_refuses_on_the_third(net_env, mock_server):
    pol(refuse_after_consecutive_403=3)
    s = mock_server().script("/a", Reply(403)).script("/b", Reply(200))
    for i in (1, 2):
        o = net.request("GET", s.url("/a"))
        assert o.kind is Kind.REFUSED and A not in net_env.state.refused, i
    o3 = net.request("GET", s.url("/a"))
    assert o3.kind is Kind.REFUSED and A in net_env.state.refused
    assert net.request("GET", s.url("/b")).attempts == 0 and s.hits_for("/b") == []


def test_consecutive_403_counter_resets_on_a_success(net_env, mock_server):
    pol(refuse_after_consecutive_403=3)
    s = mock_server().script("/a", Reply(403)).script("/ok", Reply(200))
    net.request("GET", s.url("/a"))
    net.request("GET", s.url("/a"))
    assert net.request("GET", s.url("/ok")).ok
    net.request("GET", s.url("/a"))
    assert A not in net_env.state.refused


def test_idconv_allowed_while_articles_raise(net_env, monkeypatch):
    sent = stub_transport(monkeypatch, body=b'{"records": []}')
    o = net.request("GET", "https://pmc.ncbi.nlm.nih.gov/tools/idconv/api/v1/articles/",
                    params={"ids": "10.1/x", "idtype": "doi", "format": "json"})
    assert o.ok and len(sent) == 1
    with pytest.raises(ProhibitedHost):
        net.request("GET", "https://pmc.ncbi.nlm.nih.gov/articles/PMC9817969/")
    assert len(sent) == 1                        # raised before anything was sent
    assert net_env.ledger_lines()[-1]["decision"] == "prohibited"


@pytest.mark.parametrize("validator,body,ctype", [
    (net.expect_json, b"", "application/json"),
    (net.expect_json, b"   ", "application/json"),
    (net.expect_json, b"<html><body>maintenance</body></html>", "text/html"),
    (net.expect_json, b"not json", "application/json"),
    (lambda p: bool(p.content), b"", "application/json"),
])
def test_empty_200_or_html_with_validator_is_outage(net_env, mock_server, validator, body, ctype):
    s = mock_server().script("/x", Reply(200, body, headers={"Content-Type": ctype}))
    o = net.request("GET", s.url("/x"), validate=validator)
    assert o.kind is Kind.OUTAGE and o.status == 200 and o.attempts == 1


def test_valid_json_passes_the_validator(net_env, mock_server):
    s = mock_server().script("/x", Reply(200, b'{"data": []}', headers={"Content-Type": "application/json"}))
    o = net.request("GET", s.url("/x"), validate=net.expect_json)
    assert o.ok and o.payload.json() == {"data": []}   # an empty list is data, not a failure


@pytest.mark.parametrize("transport", TRANSPORTS)
def test_dns_failure_is_transport(net_env, monkeypatch, transport):
    real = socket.getaddrinfo

    def fake(host, *a, **k):
        if host == "dns-fail.test":
            raise socket.gaierror(11001, "getaddrinfo failed")
        return real(host, *a, **k)

    monkeypatch.setattr(socket, "getaddrinfo", fake)
    pol("dns-fail.test", transport=transport)
    o = net.request("GET", "http://dns-fail.test/x")
    assert o.kind is Kind.TRANSPORT and o.status is None and o.payload is None
    assert o.attempts == 3 and net_env.clock.sleeps == [2.0, 4.0]   # 2 transport retries
    assert "getaddrinfo" in o.detail or "NameResolution" in o.detail


def test_ledger_never_holds_email_key_or_authorization(net_env, mock_server, monkeypatch):
    email = "probe.person+lit@uni-test.edu"
    monkeypatch.setenv("LITPIPE_EMAIL", email)
    pol(identity="ncbi")                          # the email rides in the URL as email=
    s = mock_server()
    loc = "/next?tool=x&email=a%40b.c&contact=a%2540b.c&api_key=SECRETKEY123"
    s.script("/start", Reply(301, headers={"Location": loc, "Link": "<mailto:a@b.c>; rel=author"}))
    s.script("/next", Reply(500), Reply(200, b"{}"))
    o = net.request("GET", s.url("/start"), params={"api_key": "SECRETKEY123"},
                    headers={"x-api-key": "SECRETKEY123", "Authorization": "Bearer TOKENVALUE456"})
    assert o.ok and o.attempts == 3
    # an exception text that carries the URL (requests: "Max retries exceeded with url: /x?...email=")
    real = socket.getaddrinfo
    monkeypatch.setattr(socket, "getaddrinfo", lambda h, *a, **k: (_ for _ in ()).throw(
        socket.gaierror(11001, "getaddrinfo failed")) if h == "dns-fail.test" else real(h, *a, **k))
    pol("dns-fail.test", identity="ncbi", retry=RetryPolicy(transport_retries=0))
    f = net.request("GET", "http://dns-fail.test/q", params={"api_key": "SECRETKEY123"})
    assert f.kind is Kind.TRANSPORT and "REDACTED" in f.detail and "@" not in f.detail
    assert s.hits_for("/start")[0].query["email"] == [email]     # it really was sent
    assert s.hits_for("/start")[0].headers["x-api-key"] == "SECRETKEY123"
    assert "uni-test.edu" not in o.payload.url and "SECRETKEY123" not in o.payload.url
    text = net_env.ledger_text()
    for needle in (email, email.replace("@", "%40"), "probe.person%2Blit%40uni-test.edu", "%2540",
                   "a@b.c", "a%40b.c", "a%2540b.c", "SECRETKEY123", "TOKENVALUE456", "uni-test.edu"):
        assert needle not in text, needle
    assert "@" not in text and "%40" not in text
    locs = [r["resp_meta"].get("location") for r in net_env.ledger_lines() if r["resp_meta"].get("location")]
    assert locs and all("REDACTED" in x for x in locs)
    hop_urls = [r["url"] for r in net_env.ledger_lines() if r["hop"] == 1]
    assert hop_urls and all("[EMAIL-REDACTED]" in u and "api_key=REDACTED" in u and "email=" not in u for u in hop_urls)


@pytest.mark.parametrize("transport", TRANSPORTS)
def test_stream_keeps_first_chunk_and_the_byte_cap(net_env, mock_server, transport):
    pol(transport=transport)
    big = b"%PDF-1.7\n" + b"x" * 300_000
    s = mock_server().script("/big", Reply(200, big, headers={"Content-Type": "application/pdf"}))
    s.script("/small", Reply(200, big[:50_000], headers={"Content-Type": "application/pdf"}))
    o = net.request("GET", s.url("/big"), stream=True, max_bytes=100_000, validate=net.expect_pdf)
    assert o.kind is Kind.ERROR and o.detail.startswith("too_large")
    p = o.payload
    assert p.truncated and p.content is None and p.first_chunk.startswith(b"%PDF")
    assert 100_000 < p.total_bytes <= 100_000 + net.CHUNK
    ok = net.request("GET", s.url("/small"), stream=True, max_bytes=100_000, validate=net.expect_pdf)
    assert ok.ok and ok.payload.content == big[:50_000] and ok.payload.first_chunk.startswith(b"%PDF")
    assert not ok.payload.truncated and ok.payload.total_bytes == 50_000


# ================================================================ spec amendments (W0, 2026-09-30)
def test_pmc_ncbi_429_refuses_the_host_for_the_run_without_retry(net_env, mock_server):
    row = hosts.policy("pmc.ncbi.nlm.nih.gov")
    assert row.transport == "urllib" and row.identity == "ncbi" and 429 not in row.retry.statuses
    clone("pmc.ncbi.nlm.nih.gov")
    body = "<html><head><title>429</title></head><body>429 Too Many Requests</body></html>"
    s = mock_server().script("/tools/idconv/api/v1/articles/",
                             Reply(429, body, headers={"Content-Type": "text/html"}))
    o = net.request("GET", s.url("/tools/idconv/api/v1/articles/"), params={"ids": "10.1/x"})
    assert o.kind is Kind.REFUSED and o.status == 429 and o.attempts == 1
    assert net_env.clock.sleeps == []            # no retry into a longer ban
    assert net_env.state.refused[A][1] == "run"
    assert net.request("GET", s.url("/tools/idconv/api/v1/articles/")).attempts == 0
    assert len(s.hits) == 1


def test_pmc_ncbi_429_with_retry_after_still_refuses(net_env, mock_server):
    clone("pmc.ncbi.nlm.nih.gov")
    s = mock_server().script("/x", Reply(429, headers={"Retry-After": "5"}))
    o = net.request("GET", s.url("/x"))
    assert o.kind is Kind.REFUSED and A in net_env.state.refused and net_env.clock.sleeps == []


def test_export_arxiv_refusal_is_manual(net_env, mock_server):
    clone("export.arxiv.org")
    s = mock_server().script("/api/query", Reply(429))
    o = net.request("GET", s.url("/api/query"))
    assert o.kind is Kind.REFUSED and o.attempts == 1
    assert net_env.state.refused[A][1] == "manual"
    net_env.state.finish_run("run1", "ok")       # a run refusal would clear here; manual stays
    assert net_env.state.is_refused(A)


@pytest.mark.parametrize("text,secrets", [
    ("https://h/x?tool=t&email=jane.doe@uni.edu&ids=1", ["jane.doe@uni.edu"]),
    ("Location: /v1.0/?email=jane.doe%40uni.edu&ids=1", ["jane.doe%40uni.edu", "jane.doe"]),
    ("next=%2Fq%3Femail%3Djane.doe%2540uni.edu%26ids%3D1", ["jane.doe%2540uni.edu", "jane.doe"]),
    ("User-Agent: literature-pipeline/0.2 (mailto:jane.doe@uni.edu)", ["jane.doe@uni.edu"]),
    ("mailto%3Ajane.doe%40uni.edu", ["jane.doe"]),
    ("GET /graph?api_key=abc123SECRET&fields=title", ["abc123SECRET"]),
    ("GET /graph?apikey=abc123SECRET", ["abc123SECRET"]),
    ("{'x-api-key': 'abc123SECRET', 'User-Agent': 'x'}", ["abc123SECRET"]),
    ('{"Authorization": "Bearer tok.en-VALUE"}', ["tok.en-VALUE", "Bearer tok"]),
    ("Authorization: Basic dXNlcjpwYXNz\r\nAccept: */*", ["dXNlcjpwYXNz"]),
    ("HTTPSConnectionPool(host='api.unpaywall.org'): Max retries exceeded with url: "
     "/v2/10.1/x?email=jane.doe%2Blit%40uni.edu (Caused by X)", ["jane.doe", "uni.edu"]),
])
def test_redact_strips_every_form(text, secrets):
    out = ledger.redact(text)
    for sec in secrets:
        assert sec not in out, (sec, out)
    assert "REDACTED" in out


def test_redact_configured_email_literal_forms(monkeypatch):
    e = "Odd.Name+x@Example-Lab.org"
    monkeypatch.setenv("LITPIPE_EMAIL", e)
    from urllib.parse import quote, quote_plus
    for form in (e, e.lower(), quote(e, safe=""), quote(quote(e, safe=""), safe=""), quote_plus(e)):
        assert "Example-Lab".lower() not in ledger.redact(f"x {form} y").lower(), form


def test_redact_keeps_harmless_text():
    s = "GET https://api.crossref.org/works/10.1152/japplphysiol.00752.2018 x-api-key present: no"
    assert ledger.redact(s) == s
    assert ledger.redact(None) is None
    assert ledger.redact_headers({"Authorization": "Bearer x", "Location": "/?email=a@b.c",
                                  "Content-Type": "text/html"}) == {
        "Authorization": "REDACTED", "Location": "/?[EMAIL-REDACTED]", "Content-Type": "text/html"}


@pytest.mark.parametrize("transport", TRANSPORTS)
def test_redirect_to_a_host_not_on_the_allow_list_is_not_followed(net_env, mock_server, transport):
    pol(transport=transport, redirect_allow=())
    a, b = mock_server(A), mock_server(B)
    b.script("/t", Reply(200, b"{}"))
    a.script("/r", Reply(302, headers={"Location": b.url("/t")}))
    o = net.request("GET", a.url("/r"))
    assert o.kind is Kind.REFUSED and o.status == 302 and "allow-list" in o.detail
    assert o.attempts == 1 and len(a.hits) == 1
    assert b.hits == []                            # the target server received nothing
    assert net_env.ledger_lines()[-1]["decision"] == "redirect_blocked"


@pytest.mark.parametrize("transport", TRANSPORTS)
def test_allow_listed_redirect_is_followed_counted_and_ledgered(net_env, mock_server, transport):
    pol(transport=transport, redirect_allow=(B,))
    a, b = mock_server(A), mock_server(B)
    b.script("/t", Reply(200, b'{"ok": 1}'))
    a.script("/r", Reply(302, headers={"Location": b.url("/t")}))
    o = net.request("GET", a.url("/r"))
    assert o.ok and o.host == B and o.attempts == 2 and o.payload.url == b.url("/t")
    assert [h for h, _ in net_env.state.acquired] == [A, B]      # each hop paced and counted
    assert [r["decision"] for r in net_env.ledger_lines()] == ["redirect", "final"]
    assert [r["hop"] for r in net_env.ledger_lines()] == [0, 1]


def test_redirect_to_a_prohibited_route_is_not_followed(net_env, mock_server):
    pol(redirect_allow=(hosts.ANY_HOST,))
    a = mock_server()
    a.script("/r", Reply(302, headers={"Location": "https://pmc.ncbi.nlm.nih.gov/articles/PMC1/"}))
    o = net.request("GET", a.url("/r"))         # no exception: the server chose the hop, not us
    assert o.kind is Kind.REFUSED and "prohibited" in o.detail and o.attempts == 1


def test_redirect_to_a_refused_host_is_not_followed(net_env, mock_server):
    pol(redirect_allow=(B,))
    a, b = mock_server(A), mock_server(B)
    a.script("/r", Reply(302, headers={"Location": b.url("/t")}))
    net_env.state.refuse(B, "test")
    o = net.request("GET", a.url("/r"))
    assert o.kind is Kind.REFUSED and b.hits == []


def test_https_to_http_downgrade_is_not_followed(net_env):
    raw = net._Raw(302, CaseInsensitiveDict({"Location": "http://doi.org/10.1/x"}))
    act = net._redirect(raw, hosts.policy("doi.org"), "https://doi.org/10.1/x", net_env.state, None)
    assert act.decision == "redirect_blocked" and "downgrade" in act.detail
    raw = net._Raw(302, CaseInsensitiveDict({"Location": "http://122.115.55.36:8000/istic"}))
    act = net._redirect(raw, hosts.policy("doi.org"), "https://doi.org/10.1/x", net_env.state, None)
    assert act.decision == "redirect_blocked"          # the ISTIC raw-IP hop
    raw = net._Raw(302, CaseInsensitiveDict({"Location": "https://data.crosscite.org/x"}))
    act = net._redirect(raw, hosts.policy("doi.org"), "https://doi.org/10.1/x", net_env.state, None)
    assert act.decision == "redirect" and act.target_policy.host == "data.crosscite.org"


def test_credentials_and_mailto_do_not_follow_a_cross_host_redirect(net_env, mock_server):
    pol(redirect_allow=(B,), identity="ua")
    pol(B, identity="download")
    a, b = mock_server(A), mock_server(B)
    b.script("/t", Reply(200))
    a.script("/r", Reply(307, headers={"Location": b.url("/t")}))
    assert net.request("GET", a.url("/r"), headers={"x-api-key": "K1", "Authorization": "Bearer T"}).ok
    ha, hb = a.hits[0].headers, b.hits[0].headers
    assert ha["x-api-key"] == "K1" and ha["Authorization"] == "Bearer T" and "mailto:" in ha["User-Agent"]
    low = {k.lower() for k in hb}
    assert "x-api-key" not in low and "authorization" not in low
    assert "mailto" not in hb["User-Agent"]


def test_too_many_redirects_is_an_error(net_env, mock_server):
    a = mock_server().script("/loop", Reply(302, headers={"Location": "/loop"}))
    o = net.request("GET", a.url("/loop"))
    assert o.kind is Kind.ERROR and o.attempts == net.MAX_REDIRECTS + 1


def test_303_turns_post_into_get(net_env, mock_server):
    a = mock_server().script("/p", Reply(303, headers={"Location": "/g"})).script("/g", Reply(200))
    assert net.request("POST", a.url("/p"), json={"ids": [1]}).ok
    assert [h.method for h in a.hits] == ["POST", "GET"] and a.hits[1].body == b""
    assert [r["method"] for r in net_env.ledger_lines()] == ["POST", "GET"]


@pytest.mark.parametrize("value,expect", [
    ("120", 120.0), ("0", 0.0), ("-5", 0.0), ("soon", None), ("", None), (None, None), ("1.5", None),
])
def test_retry_after_delay_seconds(value, expect):
    assert net.retry_after_seconds(value, now=0.0) == expect


def test_retry_after_http_date_forms():
    now = datetime(1994, 11, 6, 8, 48, 37, tzinfo=timezone.utc).timestamp()
    for s in ("Sun, 06 Nov 1994 08:49:37 GMT", "Sunday, 06-Nov-94 08:49:37 GMT", "Sun Nov  6 08:49:37 1994"):
        assert net.retry_after_seconds(s, now=now) == pytest.approx(60.0), s
    assert net.retry_after_seconds("Sun, 06 Nov 1994 08:49:37 GMT", now=now + 3600) == 0.0   # past: floor 0


def test_retry_after_http_date_is_honoured_inline(net_env, mock_server):
    when = datetime.fromtimestamp(net_env.clock.time() + 10, tz=timezone.utc)
    s = mock_server().script("/x", Reply(503, headers={"Retry-After": format_datetime(when, usegmt=True)}),
                             Reply(200))
    assert net.request("GET", s.url("/x")).ok
    assert net_env.clock.sleeps == [pytest.approx(10.0)]


def test_retry_after_http_date_over_the_cap_defers(net_env, mock_server):
    when = datetime.fromtimestamp(net_env.clock.time(), tz=timezone.utc) + timedelta(hours=2)
    s = mock_server().script("/x", Reply(503, headers={"Retry-After": format_datetime(when, usegmt=True)}))
    o = net.request("GET", s.url("/x"))
    assert o.kind is Kind.DEFERRED and o.retry_after == pytest.approx(7200) and A in net_env.state.deferred


def test_retry_after_zero_is_floored_at_the_backoff(net_env, mock_server):
    s = mock_server().script("/x", Reply(503, headers={"Retry-After": "0"}), Reply(200))
    assert net.request("GET", s.url("/x")).ok and net_env.clock.sleeps == [2.0]


def test_retry_after_under_the_cap_replaces_a_shorter_backoff(net_env, mock_server):
    s = mock_server().script("/x", Reply(429, headers={"Retry-After": "7"}), Reply(200))
    assert net.request("GET", s.url("/x")).ok and net_env.clock.sleeps == [7.0]


@pytest.mark.parametrize("value", [None, "", "   "])
def test_no_email_sends_no_mailto_at_all(net_env, mock_server, monkeypatch, value):
    if value is None:
        monkeypatch.delenv("LITPIPE_EMAIL", raising=False)
    else:
        monkeypatch.setenv("LITPIPE_EMAIL", value)
    pol(identity="ncbi")
    s = mock_server().script("/x", Reply(200))
    assert net.request("GET", s.url("/x"), params={"email": "caller@x.org"}).ok
    h = s.hits[0]
    assert h.headers["User-Agent"] == f"literature-pipeline/{__version__}"
    assert "None" not in h.headers["User-Agent"]
    assert h.query == {"tool": ["literature-pipeline"]}       # no email= at all


def test_identity_user_agent_and_ncbi_params(net_env, mock_server):
    pol(identity="ncbi")
    s = mock_server().script("/x", Reply(200))
    assert net.request("GET", s.url("/x"), params={"ids": "1", "email": "old@x.org", "tool": "GETPAID"},
                       headers={"User-Agent": "GETPAID-x/1.0"}).ok
    h = s.hits[0]
    assert h.headers["User-Agent"] == f"literature-pipeline/{__version__} (mailto:tester@litpipe-test.org)"
    assert h.query == {"ids": ["1"], "tool": ["literature-pipeline"], "email": ["tester@litpipe-test.org"]}


def test_identity_modes_email_param_anonymous_download(net_env, mock_server):
    s = mock_server()
    s.script("/x", Reply(200))
    pol(identity="email_param")
    net.request("GET", s.url("/x"))
    assert s.hits[-1].query == {"email": ["tester@litpipe-test.org"]}
    pol(identity="anonymous")
    net.request("GET", s.url("/x"), headers={"User-Agent": "Mozilla/5.0"})
    assert s.hits[-1].headers["User-Agent"] == f"literature-pipeline/{__version__}"
    pol(identity="download")
    net.request("GET", s.url("/x"), headers={"User-Agent": "Mozilla/5.0"})
    assert s.hits[-1].headers["User-Agent"] == "Mozilla/5.0"
    net.request("GET", s.url("/x"))
    assert s.hits[-1].headers["User-Agent"] == f"literature-pipeline/{__version__}"   # no mailto to strangers


def test_budget_exhausted_returns_deferred_and_sends_nothing(net_env, mock_server):
    pol(daily_budget=1)
    s = mock_server().script("/x", Reply(200))
    assert net.request("GET", s.url("/x")).ok
    o = net.request("GET", s.url("/x"))
    assert o.kind is Kind.DEFERRED and o.attempts == 0 and len(s.hits) == 1


def test_refused_host_short_circuits(net_env, mock_server):
    s = mock_server().script("/x", Reply(200))
    net_env.state.refuse(A, "earlier 406", persistence="run")
    o = net.request("GET", s.url("/x"))
    assert o.kind is Kind.REFUSED and o.attempts == 0 and s.hits == []
    assert net_env.state.calls == [("refuse", A, "earlier 406", "run")]   # not even an acquire


def test_406_refuses_the_host(net_env, mock_server):
    s = mock_server().script("/x", Reply(406))
    o = net.request("GET", s.url("/x"))
    assert o.kind is Kind.REFUSED and A in net_env.state.refused and o.attempts == 1


def test_final_429_after_six_retries_refuses_the_host(net_env, mock_server):
    s = mock_server().script("/x", Reply(429))
    o = net.request("GET", s.url("/x"))
    assert o.kind is Kind.REFUSED and o.attempts == 7 and A in net_env.state.refused
    assert net_env.clock.sleeps == [2.0, 4.0, 8.0, 16.0, 32.0, 64.0]   # no wait after the last attempt


def test_exhausted_503_is_outage_not_refusal(net_env, mock_server):
    s = mock_server().script("/x", Reply(503))
    o = net.request("GET", s.url("/x"))
    assert o.kind is Kind.OUTAGE and o.attempts == 7 and A not in net_env.state.refused
    assert len(net_env.clock.sleeps) == 6


def test_acquire_and_release_around_every_attempt(net_env, mock_server):
    s = mock_server().script("/x", Reply(500), Reply(502), Reply(200))
    o = net.request("GET", s.url("/x"))
    seq = [c[0] for c in net_env.state.calls if c[0] in ("acquire", "release")]
    assert o.attempts == 3 and seq == ["acquire", "release"] * 3
    assert [ok for _, ok in net_env.state.released] == [False, False, True]


def test_pacing_counts_from_the_end_of_the_previous_attempt(net_env, mock_server):
    pol(min_interval_s=5.0)
    s = mock_server().script("/x", Reply(200))
    net.request("GET", s.url("/x"))
    net.request("GET", s.url("/x"))
    assert gaps(net_env.state) == [5.0] and net_env.state.pacing_waits == [5.0]


def test_unknown_host_gets_the_default_policy_and_a_ledger_note(net_env, mock_server):
    p = hosts.policy("https://www.some-publisher.example/pdf/1")
    assert (p.known, p.min_interval_s, p.concurrency, p.identity, p.host) == \
        (False, 2.0, 1, "download", "www.some-publisher.example")
    s = mock_server("127.0.0.3").script("/x", Reply(200))
    assert net.request("GET", s.url("/x")).ok
    assert "unknown host" in net_env.ledger_lines()[-1]["note"]


@pytest.mark.parametrize("status,kind", [(404, Kind.NO_MATCH), (410, Kind.NO_MATCH), (400, Kind.ERROR),
                                         (401, Kind.ERROR), (202, Kind.REFUSED), (501, Kind.OUTAGE)])
def test_final_status_mapping(net_env, mock_server, status, kind):
    s = mock_server().script("/x", Reply(status, b"body"))
    o = net.request("GET", s.url("/x"))
    assert o.kind is kind and o.status == status and o.attempts == 1
    assert o.payload.status == status and o.payload.content == b"body"   # kept for diagnosis


def test_unpaywall_422_is_config(net_env, mock_server):
    clone("api.unpaywall.org")
    s = mock_server().script("/v2/10.1/x", Reply(422, b'{"error": true}'))
    assert net.request("GET", s.url("/v2/10.1/x")).kind is Kind.CONFIG


def test_validator_kind_and_expect_pdf(net_env, mock_server):
    s = mock_server().script("/html", Reply(200, b"<!DOCTYPE html><html>Just a moment...</html>"))
    s.script("/empty", Reply(200, b""))
    assert net.request("GET", s.url("/html"), validate=net.expect_pdf).kind is Kind.REFUSED
    assert net.request("GET", s.url("/empty"), validate=net.expect_pdf).kind is Kind.OUTAGE
    assert net.request("GET", s.url("/html"), validate=lambda p: Kind.NO_MATCH).kind is Kind.NO_MATCH
    boom = net.request("GET", s.url("/html"), validate=lambda p: 1 / 0)
    assert boom.kind is Kind.ERROR and "ZeroDivisionError" in boom.detail


@pytest.mark.parametrize("transport", TRANSPORTS)
def test_dropped_connection_is_retried_then_ok(net_env, mock_server, transport):
    pol(transport=transport)
    s = mock_server().script("/x", Reply(close=True), Reply(200, b"{}"))
    o = net.request("GET", s.url("/x"))
    assert o.ok and o.attempts == 2 and net_env.clock.sleeps == [2.0]


@pytest.mark.parametrize("transport", TRANSPORTS)
def test_read_timeout_is_transport(net_env, mock_server, transport):
    pol(transport=transport, retry=RetryPolicy(transport_retries=0))
    s = mock_server().script("/x", Reply(200, b"late", delay=1.0))
    o = net.request("GET", s.url("/x"), timeout=(2, 0.2))
    assert o.kind is Kind.TRANSPORT and o.attempts == 1


@pytest.mark.parametrize("transport", TRANSPORTS)
def test_post_json_body(net_env, mock_server, transport):
    pol(transport=transport)
    s = mock_server().script("/batch", Reply(200, b"[]"))
    assert net.request("POST", s.url("/batch"), json={"ids": ["a", "b"]}).ok
    h = s.hits[0]
    assert h.method == "POST" and json.loads(h.body) == {"ids": ["a", "b"]}
    assert h.headers["Content-Type"] == "application/json"


# ================================================================ host table
@pytest.mark.parametrize("url", [
    "https://europepmc.org/articles/PMC9817969?pdf=render",
    "https://europepmc.org/backend/ptpmcrender.fcgi?accid=PMC1&blobtype=pdf",
    "https://www.europepmc.org/abstract/MED/1",
    "https://pmc.ncbi.nlm.nih.gov/articles/PMC9817969/pdf/",
    "https://cdn.ncbi.nlm.nih.gov/pmc/blobs/abc/file.pdf",
    "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC9817969/",
    "https://www.biorxiv.org/content/10.1101/2024.01.01.1v1.full.pdf",
    "https://www.medrxiv.org/content/10.1101/2024.01.01.1v1",
    "https://arxiv.org/pdf/2605.29559",
])
def test_prohibited_routes_raise_before_sending(net_env, monkeypatch, url):
    sent = stub_transport(monkeypatch)
    with pytest.raises(ProhibitedHost):
        net.request("GET", url)
    assert sent == []


@pytest.mark.parametrize("url", [
    "https://pmc.ncbi.nlm.nih.gov/tools/idconv/api/v1/articles/?ids=10.1/x",
    "https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=DOI:%2210.1/x%22",
    "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pmc",
    "https://www.ncbi.nlm.nih.gov/research/bionlp/RESTful/pmcoa.cgi/BioC_json/PMC1/unicode",
    "https://export.arxiv.org/api/query?id_list=2605.29559",
    "https://arxiv.org/abs/2605.29559",
    "https://api.biorxiv.org/details/biorxiv/10.1101/x",
])
def test_allowed_routes(net_env, url):
    assert hosts.prohibited(url) is None


def test_switches_lift_the_pdf_rules(net_env):
    assert hosts.prohibited("https://arxiv.org/pdf/2605.29559") is not None
    assert hosts.prohibited("https://www.biorxiv.org/content/x.full.pdf") is not None
    net_env.write_config(hosts={"arxiv_pdf_allowed": True, "biorxiv_pdf_allowed": True})
    assert hosts.prohibited("https://arxiv.org/pdf/2605.29559") is None
    assert hosts.prohibited("https://www.biorxiv.org/content/x.full.pdf") is None
    assert hosts.policy("https://arxiv.org/pdf/2605.29559").min_interval_s == 15.0
    assert hosts.prohibited("https://pmc.ncbi.nlm.nih.gov/articles/PMC1/") is not None   # no switch


def test_host_table_rows():
    p = hosts.policy
    assert {h for h in hosts.rows()} >= {
        "api.crossref.org", "api.datacite.org", "doi.org", "api.unpaywall.org", "api.openalex.org",
        "pmc.ncbi.nlm.nih.gov", "eutils.ncbi.nlm.nih.gov", "pmc-oa-opendata.s3.amazonaws.com",
        "www.ncbi.nlm.nih.gov", "www.ebi.ac.uk", "export.arxiv.org", "api.osf.io", "osf.io", ".osf.io",
        "storage.googleapis.com", "sportrxiv.org", "api.biorxiv.org", "api.semanticscholar.org"}
    assert p("api.semanticscholar.org").min_interval_s == 6.5
    assert p("api.semanticscholar.org").retry_after_cap_s == 600
    ax = p("https://export.arxiv.org/api/query")
    assert ax.min_interval_s == 3.5 and ax.refusal_persistence == "manual" and 429 not in ax.retry.statuses
    assert p("eutils.ncbi.nlm.nih.gov").min_interval_s == 0.4 and p("eutils.ncbi.nlm.nih.gov").identity == "ncbi"
    assert p("www.ebi.ac.uk").min_interval_s == 1.0
    up = p("api.unpaywall.org")
    assert up.identity == "email_param" and up.status_kinds == {422: Kind.CONFIG, 410: Kind.CONFIG}
    assert p("files.de-1.osf.io").host == "files.de-1.osf.io" and p("files.de-1.osf.io").known
    assert p("api.osf.io").allows_redirect_to("files.de-1.osf.io")
    assert p("osf.io").allows_redirect_to("storage.googleapis.com")
    assert not p("api.crossref.org").allows_redirect_to("evil.example")
    assert p("doi.org").allows_redirect_to("data.crosscite.org")
    for h, row in hosts.rows().items():
        assert row.refuse_after_consecutive_403 == 1 and row.concurrency == 1, h
        assert row.retry.first_wait_s >= 2.0 and row.retry.max_retries <= 6, h


def test_register_replaces_a_row_and_reset_restores_it():
    try:
        hosts.register(dataclasses.replace(hosts.policy("api.semanticscholar.org"), min_interval_s=1.1))
        assert hosts.policy("https://api.semanticscholar.org/graph/v1/paper/x").min_interval_s == 1.1
    finally:
        hosts.reset()
    assert hosts.policy("api.semanticscholar.org").min_interval_s == 6.5


def test_policy_validation():
    with pytest.raises(ValueError):
        HostPolicy("x.org", identity="browser")
    with pytest.raises(ValueError):
        HostPolicy("x.org", refuse_after_consecutive_403=0)
    with pytest.raises(ValueError):
        net.request("GET", "ftp://x.org/file")


# ================================================================ ledger and packaging
def test_ledger_line_fields(net_env, mock_server):
    ledger.set_run_id("2026-09-30.2")
    try:
        s = mock_server().script("/x", Reply(200, b"{}", headers={"X-RateLimit-Remaining": "7"}))
        net.request("GET", s.url("/x"), headers={"x-api-key": "K9"}, purpose="unit")
    finally:
        ledger.set_run_id(None)
    rec = net_env.ledger_lines()[-1]
    for k in ("ts", "run_id", "pid", "host", "method", "url", "purpose", "attempt", "hop", "transport",
              "status", "decision", "kind", "elapsed_ms", "bytes", "retry_after", "wait_s",
              "req_headers", "resp_headers", "resp_meta", "error", "note"):
        assert k in rec, k
    assert rec["run_id"] == "2026-09-30.2" and rec["purpose"] == "unit" and rec["status"] == 200
    assert rec["kind"] == "OK" and rec["decision"] == "final" and rec["bytes"] == 2
    assert "x-api-key" in rec["req_headers"] and "K9" not in json.dumps(rec)   # names, never values
    assert rec["resp_meta"]["x-ratelimit-remaining"] == "7"
    assert list(net_env.ledger_dir.glob("*.jsonl"))[0].name == rec["ts"][:10] + ".jsonl"


def test_ledger_default_dir_is_under_state_dir(tmp_path, monkeypatch):
    from litpipe import config
    cfg = tmp_path / "projects.json"
    cfg.write_text(json.dumps({"state_dir": str(tmp_path / "st")}), encoding="utf-8")
    monkeypatch.setattr(config, "CONFIG_PATH", cfg)
    monkeypatch.setattr(ledger, "LEDGER_DIR", None)
    path = ledger.write({"host": "h", "url": "https://h/?email=a@b.c"})
    assert path.parent == tmp_path / "st" / "ledger"
    assert "a@b.c" not in path.read_text(encoding="utf-8")


def test_importing_litpipe_net_needs_no_state_module(tmp_path):
    code = ("import sys; sys.path.insert(0, sys.argv[1]); import litpipe.net, litpipe.hosts, litpipe.ledger; "
            "print('litpipe.state' in sys.modules)")
    r = subprocess.run([sys.executable, "-c", code, str(REPO)], cwd=tmp_path, capture_output=True,
                       text=True, timeout=60, env={**os.environ, "PYTHONUTF8": "1"})
    assert r.returncode == 0, r.stderr
    assert r.stdout.strip() == "False"
