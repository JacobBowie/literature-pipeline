"""lit_net.get retry contract (B1) -- offline, via monkeypatched requests.get + time.sleep.

Characterization pin (written before the 8 metadata GETs were routed through it): locks the
drop-in semantics -- 200/404 pass straight through, 429/5xx retry with Retry-After honored+capped,
exhaustion returns the final response (not raise), network errors retry then re-raise.
"""
import pytest
import requests

import lit_net


class FakeResp:
    def __init__(self, status_code, headers=None):
        self.status_code = status_code
        self.headers = headers or {}


def _patch(monkeypatch, items, rec):
    """items: list of FakeResp or Exception, returned/raised in order per requests.get call."""
    seq = list(items)

    def fake_get(url, **kwargs):
        rec["calls"] += 1
        rec["last_kwargs"] = kwargs
        item = seq.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    monkeypatch.setattr(lit_net.requests, "get", fake_get)
    monkeypatch.setattr(lit_net.time, "sleep", lambda s: rec["sleeps"].append(s))


def test_200_passes_through_without_retry(monkeypatch):
    rec = {"calls": 0, "sleeps": []}
    _patch(monkeypatch, [FakeResp(200)], rec)
    r = lit_net.get("http://x")
    assert r.status_code == 200 and rec["calls"] == 1 and rec["sleeps"] == []


def test_404_is_terminal_no_retry(monkeypatch):
    rec = {"calls": 0, "sleeps": []}
    _patch(monkeypatch, [FakeResp(404)], rec)
    r = lit_net.get("http://x")
    assert r.status_code == 404 and rec["calls"] == 1 and rec["sleeps"] == []


def test_429_retried_then_success(monkeypatch):
    rec = {"calls": 0, "sleeps": []}
    _patch(monkeypatch, [FakeResp(429), FakeResp(200)], rec)
    r = lit_net.get("http://x")
    assert r.status_code == 200 and rec["calls"] == 2 and len(rec["sleeps"]) == 1


def test_5xx_retried_across_attempts(monkeypatch):
    rec = {"calls": 0, "sleeps": []}
    _patch(monkeypatch, [FakeResp(503), FakeResp(500), FakeResp(200)], rec)
    r = lit_net.get("http://x", retries=3)
    assert r.status_code == 200 and rec["calls"] == 3


def test_retry_after_header_honored_and_capped(monkeypatch):
    rec = {"calls": 0, "sleeps": []}
    _patch(monkeypatch, [FakeResp(429, {"Retry-After": "5"}), FakeResp(200)], rec)
    lit_net.get("http://x")
    assert rec["sleeps"] == [5.0]

    rec2 = {"calls": 0, "sleeps": []}
    _patch(monkeypatch, [FakeResp(503, {"Retry-After": "9999"}), FakeResp(200)], rec2)
    lit_net.get("http://x")
    assert rec2["sleeps"] == [lit_net.MAX_RETRY_AFTER]


def test_negative_retry_after_floored(monkeypatch):
    """A non-conformant negative Retry-After must floor to 0, not produce time.sleep(<0)
    (which raises ValueError -- uncaught at a narrow-except caller)."""
    rec = {"calls": 0, "sleeps": []}
    _patch(monkeypatch, [FakeResp(503, {"Retry-After": "-5"}), FakeResp(200)], rec)
    r = lit_net.get("http://x")
    assert r.status_code == 200 and rec["sleeps"] == [0.0]


def test_exhaustion_returns_final_5xx_not_raise(monkeypatch):
    rec = {"calls": 0, "sleeps": []}
    _patch(monkeypatch, [FakeResp(503), FakeResp(503), FakeResp(503)], rec)
    r = lit_net.get("http://x", retries=3)
    # returns the final response so the caller can distinguish transient-exhaustion from a 404
    assert r.status_code == 503 and rec["calls"] == 3


def test_network_error_retried_then_reraised(monkeypatch):
    rec = {"calls": 0, "sleeps": []}
    boom = requests.exceptions.ConnectionError("boom")
    _patch(monkeypatch, [boom, boom, boom], rec)
    with pytest.raises(requests.exceptions.ConnectionError):
        lit_net.get("http://x", retries=3)
    assert rec["calls"] == 3


def test_network_error_then_success(monkeypatch):
    rec = {"calls": 0, "sleeps": []}
    _patch(monkeypatch, [requests.exceptions.Timeout("t"), FakeResp(200)], rec)
    r = lit_net.get("http://x")
    assert r.status_code == 200 and rec["calls"] == 2


def test_kwargs_pass_through(monkeypatch):
    rec = {"calls": 0, "sleeps": []}
    _patch(monkeypatch, [FakeResp(200)], rec)
    lit_net.get("http://x", params={"email": "a@b.c"}, headers={"User-Agent": "UA"}, timeout=15)
    kw = dict(rec["last_kwargs"])
    assert kw.pop("hooks") == {"response": lit_net._redirect_guard}       # V-A7: every hop is checked
    assert kw == {"params": {"email": "a@b.c"}, "headers": {"User-Agent": "UA"}, "timeout": 15}


def test_prohibited_pmc_article_page_is_refused_before_sending(monkeypatch):
    """W2-A1: the PMC article pages are never automated. get() used to route that host through a
    urllib detour; it now returns a status-0 failure and sends nothing."""
    rec = {"calls": 0, "sleeps": []}
    _patch(monkeypatch, [FakeResp(200)], rec)
    r = lit_net.get("https://pmc.ncbi.nlm.nih.gov/articles/PMC123/")
    assert r.status_code == 0 and r.error.startswith("PROHIBITED") and rec["calls"] == 0
    assert r.text == "" and rec["sleeps"] == []


def test_non_prohibited_ncbi_path_still_uses_requests(monkeypatch):
    rec = {"calls": 0, "sleeps": []}
    _patch(monkeypatch, [FakeResp(200)], rec)
    assert lit_net.get("https://www.ncbi.nlm.nih.gov/research/bionlp/x").status_code == 200
    assert rec["calls"] == 1
