"""idconv consolidation (c8) + is_valid_doi tightening (B2).

Offline: monkeypatch lit_net.get (the shared retry GET the consolidated doi_to_pmcid_batch calls)
+ lit_net.time.sleep. Guards the B2 hardening: poisoned-chunk 1-DOI fallback, the empty-key guard,
the up-front validity gate, and transport-error skip.
"""
import requests

import lit_net
from lit_util import is_valid_doi


class FakeJson:
    def __init__(self, status_code, data):
        self.status_code = status_code
        self.headers = {}
        self._data = data

    def json(self):
        return self._data


def _rec(doi, pmcid):
    return {"requested-id": doi, "pmcid": pmcid}


def test_batch_resolves_records(monkeypatch):
    def fake_get(url, headers=None, params=None, timeout=None):
        ids = params["ids"].split(",")
        return FakeJson(200, {"records": [_rec(i, "PMC" + i[-1]) for i in ids]})
    monkeypatch.setattr(lit_net, "get", fake_get)
    monkeypatch.setattr(lit_net.time, "sleep", lambda s: None)
    out = lit_net.doi_to_pmcid_batch(["10.1234/a", "10.5678/b"], ua="UA", email="e@x")
    assert out == {"10.1234/a": "PMCa", "10.5678/b": "PMCb"}


def test_poisoned_chunk_falls_back_to_single(monkeypatch):
    """B2: one bad id 400s the whole batch -> retry one at a time so the rest still resolve."""
    calls = []

    def fake_get(url, headers=None, params=None, timeout=None):
        ids = params["ids"].split(",")
        calls.append(len(ids))
        if len(ids) > 1:
            return FakeJson(400, {"status": "error", "records": []})
        return FakeJson(200, {"records": [_rec(ids[0], "PMC" + ids[0][-1])]})
    monkeypatch.setattr(lit_net, "get", fake_get)
    monkeypatch.setattr(lit_net.time, "sleep", lambda s: None)
    out = lit_net.doi_to_pmcid_batch(["10.1234/a", "10.5678/b"], ua="UA", email="e@x")
    assert out == {"10.1234/a": "PMCa", "10.5678/b": "PMCb"}
    assert calls[0] == 2 and calls[1:] == [1, 1]  # batch 400s, then two single-id retries


def test_empty_key_guard(monkeypatch):
    """A record with a pmcid but no doi/requested-id must NOT write out[''] (recheck_pmc bug)."""
    def fake_get(url, headers=None, params=None, timeout=None):
        return FakeJson(200, {"records": [{"pmcid": "PMC999"},
                                          {"requested-id": "10.1234/x", "pmcid": "PMC1"}]})
    monkeypatch.setattr(lit_net, "get", fake_get)
    monkeypatch.setattr(lit_net.time, "sleep", lambda s: None)
    out = lit_net.doi_to_pmcid_batch(["10.1234/x"], ua="UA", email="e@x")
    assert "" not in out and out == {"10.1234/x": "PMC1"}


def test_invalid_dois_dropped_before_api(monkeypatch):
    """B2: malformed DOIs never reach idconv (would 400 the whole chunk)."""
    seen = []

    def fake_get(url, headers=None, params=None, timeout=None):
        seen.append(params["ids"])
        return FakeJson(200, {"records": [_rec("10.1234/ok", "PMC1")]})
    monkeypatch.setattr(lit_net, "get", fake_get)
    monkeypatch.setattr(lit_net.time, "sleep", lambda s: None)
    out = lit_net.doi_to_pmcid_batch(
        ["10.1234/ok", "10.1234/ab–cd", "10.1234/ab—cd"], ua="UA", email="e@x")
    assert seen == ["10.1234/ok"]          # only the valid DOI reached idconv; dash artifacts dropped
    assert out == {"10.1234/ok": "PMC1"}


def test_transport_error_skips_chunk(monkeypatch):
    def fake_get(url, headers=None, params=None, timeout=None):
        raise requests.exceptions.ConnectionError("down")
    monkeypatch.setattr(lit_net, "get", fake_get)
    monkeypatch.setattr(lit_net.time, "sleep", lambda s: None)
    out = lit_net.doi_to_pmcid_batch(["10.1234/a"], ua="UA", email="e@x")
    assert out == {}  # logged + skipped, no crash


def test_is_valid_doi_rejects_dash_artifacts_keeps_sici():
    assert is_valid_doi("10.1234/abc.123")
    assert is_valid_doi("10.1234/ab-cd")            # ASCII hyphen stays valid
    assert not is_valid_doi("10.1234/ab–cd")   # en dash (PDF artifact)
    assert not is_valid_doi("10.1234/ab−cd")   # minus sign (PDF artifact)
    # legit SICI-format DOIs carry literal angle brackets -- must NOT be rejected: is_valid_doi gates
    # the citation graph + index, and these are real registered DOIs (old Wiley/Blackwell).
    assert is_valid_doi("10.1002/(SICI)1097-0258(19970228)16:4<385::AID-SIM380>3.0.CO;2-3")
    assert not is_valid_doi("")
    assert not is_valid_doi("10.1/x")               # <4-digit registrant
