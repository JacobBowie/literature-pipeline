"""litpipe.s2 (dispatch W2-D1, K1). Offline: requests go to FakeS2 (a stub litpipe.net transport that
plays Semantic Scholar's paging, batch and error rules), to a tests/netmock.MockServer on loopback, or
through the real litpipe.state on a temp database. Retry waits run on the net_env virtual clock.

Fixtures under tests/fixtures/W2-D1/ are trimmed S2 responses from the 2026-09-23 probes and the
2026-09-30 re-probe. The s2probe self-tests (notes/2026-09-23_s2_probes/setup_selftest.py, T0-T16) are
ported here as test_selftest_* against the client that replaces the probe harness."""
import ast
import json
import os
import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

import pytest
from requests.structures import CaseInsensitiveDict

from litpipe import config, hosts, ledger, net, s2
from litpipe.outcomes import Kind, Outcome
from litpipe.s2 import WalkState
from tests.netmock import Reply

REPO = Path(__file__).resolve().parent.parent
FIX = Path(__file__).resolve().parent / "fixtures" / "W2-D1"
SECRET = "s2-TESTKEY-0a1b2c3d4e5f6a7b8c9d"
S2 = "api.semanticscholar.org"


def fixture(name):
    return json.loads((FIX / name).read_text(encoding="utf-8"))


# ------------------------------------------------------------------------------ FakeS2
def _json_raw(status, body, headers=None):
    data = body if isinstance(body, bytes) else json.dumps(body).encode("utf-8")
    h = CaseInsensitiveDict({"Content-Type": "application/json", **(headers or {})})
    return net._Raw(status, h, data, data[:net.CHUNK], len(data))


class FakeS2:
    """A stub for litpipe.net's transports that answers like Semantic Scholar:
    paging with offset/limit/next and a 400 for offset + limit >= 10000 or limit > 1000; the
    publicationDateOrYear filter dropping undated citers (P3-C1); `data: null` for elided references;
    POST /paper/batch aligned with nulls and the 9,999 nested cap truncating silently; 404 bodies.
    `script(path, (status, headers, body), ...)` answers a path from a queue first."""

    def __init__(self):
        self.papers = {}
        self.scripts = {}
        self.sent = []
        self.nested_cap = 9999
        self.truncate = {}          # pid -> nested list length served (a silent truncation)

    def add(self, pid, *, citations=(), references=(), **meta):
        self.papers[pid] = {"citations": list(citations), "references": references if references is None
                            else list(references), **meta}
        return self

    def script(self, path, *replies):
        self.scripts.setdefault(path, []).extend(replies)
        return self

    def sent_paths(self, suffix=""):
        return [r for r in self.sent if r["path"].endswith(suffix)]

    def __call__(self, method, url, hdrs, body, timeout, max_bytes):
        parts = urlsplit(url)
        path = unquote(parts.path)
        q = {k: v[-1] for k, v in parse_qs(parts.query, keep_blank_values=True).items()}
        js = json.loads(body) if body else None
        self.sent.append({"method": method, "path": path, "raw_path": parts.path, "params": q,
                          "headers": dict(hdrs), "json": js, "timeout": timeout})
        queue = self.scripts.get(path)
        if queue:
            status, headers, b = queue.pop(0) if len(queue) > 1 else queue[0]
            if b == "transport":
                return net._Raw(error="ConnectionError: scripted")
            return _json_raw(status, b if b is not None else {"error": f"scripted {status}"}, headers)
        if path == "/graph/v1/paper/batch" and method == "POST":
            return self._batch(q, js)
        if path.startswith("/graph/v1/paper/") and path.endswith(("/citations", "/references")):
            return self._list(path, q)
        return _json_raw(404, {"error": f"FakeS2: no route for {path}"})

    def _list(self, path, q):
        rel = "citations" if path.endswith("/citations") else "references"
        pid = path[len("/graph/v1/paper/"):-len("/" + rel)]
        offset, limit = int(q.get("offset", 0)), int(q.get("limit", 100))
        if limit > 1000:
            return _json_raw(400, {"error": "limit must be <= 1000"})
        if offset + limit >= 10000:
            return _json_raw(400, {"error": "offset + limit must be < 10000"})
        paper = self.papers.get(pid)
        if paper is None:
            return _json_raw(404, {"error": f"Paper with id {pid} not found"})
        lst = paper[rel]
        if lst is None:
            return _json_raw(200, {"data": None, "citingPaperInfo": {"openAccessPdf": {"disclaimer":
                             "Notice: The following paper fields have been elided by the publisher: {'references'}."}}})
        win = q.get("publicationDateOrYear")
        if win:
            lo, hi = win.split(":")
            lst = [it for it in lst if it.get("citingPaper", {}).get("publicationDate")
                   and lo <= it["citingPaper"]["publicationDate"] <= hi]
        page = lst[offset:offset + limit]
        out = {"offset": offset, "data": page}
        if offset + limit < len(lst):
            out["next"] = offset + limit
        return _json_raw(200, out)

    def _batch(self, q, js):
        fields = q.get("fields", "").split(",")
        nested = [f for f in fields if "." in f]
        rel = nested[0].split(".")[0] if nested else None
        rows, served = [], 0
        for pid in js["ids"]:
            p = self.papers.get(pid)
            if p is None:
                rows.append(None)
                continue
            row = {"paperId": p.get("paperId", pid[-40:])}
            for f in fields:
                if "." not in f and f in p:
                    row[f] = p[f]
            if rel:
                lst = p[rel]
                if lst is None:
                    row[rel] = None
                    row["openAccessPdf"] = {"disclaimer": "Notice: The following paper fields have been elided "
                                                          "by the publisher: {'references'}."}
                else:
                    items = [it.get("citingPaper") or it.get("citedPaper") or it for it in lst]
                    items = items[:self.truncate.get(pid, len(items))]
                    room = max(0, self.nested_cap - served)
                    row[rel] = items[:room]
                    served += len(row[rel])
            rows.append(row)
        return _json_raw(200, rows)


def citers(n, start=0, *, dated=True, year0=2000):
    out = []
    for i in range(start, start + n):
        d = date(year0, 1, 1) + timedelta(days=(i * 3) % 9000)
        out.append({"citingPaper": {"paperId": f"{i:040x}", "year": d.year,
                                    "publicationDate": d.isoformat() if dated else None}})
    return out


# ------------------------------------------------------------------------------ fixtures
@pytest.fixture
def s2env(net_env, monkeypatch):
    """net_env (virtual clock, FakeState, temp ledger and projects.json) with no key, a fresh
    default session, and the host table restored afterwards."""
    monkeypatch.delenv(s2.KEY_ENV, raising=False)
    monkeypatch.setattr(s2, "BASE", "https://api.semanticscholar.org")
    s2.reset_default_session()
    yield net_env
    s2.reset_default_session()
    hosts.reset()


@pytest.fixture
def fake(s2env, monkeypatch):
    f = FakeS2()
    monkeypatch.setitem(net._TRANSPORTS, "requests", f)
    monkeypatch.setitem(net._TRANSPORTS, "urllib", f)
    return f


def acquired_gaps(state, host=S2):
    ts = [t for h, t in state.acquired if h == host]
    return [round(b - a, 6) for a, b in zip(ts, ts[1:])]


A = "DOI:10.1152/japplphysiol.00752.2018"
CIT = f"/graph/v1/paper/{A}/citations"
REF = f"/graph/v1/paper/{A}/references"


# ================================================================ K1 acceptance: retry and spacing
def test_first_retry_waits_2s_never_0_and_doubles(fake, s2env):
    fake.add(A, citations=citers(3)).script(CIT, (429, {}, None), (429, {}, None), (200, {}, {"offset": 0, "data": citers(3)}))
    w = s2.citations(A, expected=3)
    assert w.state is WalkState.COMPLETE and w.n_rows == 3 and w.attempts == 3
    assert s2env.clock.sleeps == [2.0, 4.0]                 # backoff, jitter pinned to 0 by net_env
    assert min(acquired_gaps(s2env.state)) >= 6.5           # unkeyed spacing holds across retries


def test_backoff_ladder_on_persistent_429_then_the_host_is_refused(fake, s2env):
    fake.add(A, citations=citers(3)).script(CIT, (429, {}, {"message": "Too Many Requests"}))
    w = s2.citations(A, expected=3)
    assert w.state is WalkState.FAILED and w.kind is Kind.REFUSED and w.status == 429
    assert w.attempts == 7 and s2env.clock.sleeps == [2.0, 4.0, 8.0, 16.0, 32.0, 64.0]
    assert w.rows is None and w.n_rows is None
    n = len(fake.sent)
    again = s2.citations(A, expected=3)                     # refused for the run: nothing sent
    assert again.failed and again.kind is Kind.REFUSED and again.attempts == 0 and len(fake.sent) == n


@pytest.mark.parametrize("status", [500, 502, 503, 504])
def test_5xx_is_retried(fake, status):
    fake.add(A, citations=citers(2)).script(CIT, (status, {}, None), (200, {}, {"offset": 0, "data": citers(2)}))
    w = s2.citations(A, expected=2)
    assert w.state is WalkState.COMPLETE and w.attempts == 2


def test_500_exhausted_is_failed_outage_not_empty(fake, s2env):
    fake.add(A, citations=citers(2)).script(CIT, (500, {}, None))
    w = s2.citations(A, expected=2)
    assert w.failed and w.kind is Kind.OUTAGE and w.status == 500 and w.attempts == 7 and w.rows is None


def test_retry_after_is_honoured_up_to_600s(fake, s2env):
    fake.add(A, citations=citers(1)).script(CIT, (429, {"Retry-After": "600"}, None),
                                            (200, {}, {"offset": 0, "data": citers(1)}))
    assert s2.citations(A, expected=1).state is WalkState.COMPLETE
    assert s2env.clock.sleeps == [600.0]


def test_retry_after_3_replaces_a_shorter_backoff(fake, s2env):
    fake.add(A, citations=citers(1)).script(CIT, (429, {"Retry-After": "3"}, None),
                                            (200, {}, {"offset": 0, "data": citers(1)}))
    assert s2.citations(A, expected=1).state is WalkState.COMPLETE
    assert s2env.clock.sleeps == [3.0]


def test_retry_after_over_600_defers_the_host_and_is_failed_deferred(fake, s2env):
    fake.add(A, citations=citers(1)).script(CIT, (429, {"Retry-After": "3600"}, None))
    w = s2.citations(A, expected=1)
    assert w.failed and w.kind is Kind.DEFERRED and w.retry_after == 3600 and w.attempts == 1
    assert s2env.clock.sleeps == []                         # never parked in the row
    n = len(fake.sent)
    later = s2.citations(A, expected=1)
    assert later.failed and later.kind is Kind.DEFERRED and len(fake.sent) == n


def test_404_and_400_are_not_retried(fake, s2env):
    w = s2.citations("DOI:10.1016/j.cell.2020.04.043", expected=None)
    assert w.state is WalkState.NOT_FOUND and w.attempts == 1 and "not found" in w.reason
    fake.add(A, citations=citers(1)).script(CIT, (400, {}, fixture("p2_offsetcap_400.json")["body"]))
    w = s2.citations(A, expected=1)
    assert w.failed and w.kind is Kind.ERROR and w.status == 400 and w.attempts == 1
    assert "offset + limit must be < 10000" in w.reason       # S2's error text kept for diagnosis


def test_transport_failure_is_retried_then_failed_transport(fake, s2env):
    fake.add(A, citations=citers(1)).script(CIT, (0, {}, "transport"))
    w = s2.citations(A, expected=1)
    assert w.failed and w.kind is Kind.TRANSPORT and w.attempts == 3   # net caps transport retries at 2
    assert s2env.clock.sleeps == [2.0, 4.0]


def test_keyed_spacing_is_1_1s_and_retries_stay_2s_apart(fake, s2env, monkeypatch):
    monkeypatch.setenv(s2.KEY_ENV, SECRET)
    fake.add(A, citations=citers(2)).script(CIT, (429, {}, None), (200, {}, {"offset": 0, "data": citers(2)}))
    s2.citations(A, expected=2)
    s2.citations(A, expected=2)
    assert hosts.policy(S2).min_interval_s == 1.1
    gaps = acquired_gaps(s2env.state)
    assert gaps[0] >= 2.0 and min(gaps) >= 1.1              # retry gap 2 s, call gap 1.1 s
    assert s2env.clock.sleeps[0] == 2.0


def test_s2_row_carries_the_pinned_retry_and_the_600s_cap(fake):
    fake.add(A, citations=citers(1))
    s2.citations(A, expected=1)
    row = hosts.policy(S2)
    assert row.retry == s2.S2_RETRY and row.retry.first_wait_s == 2.0 and 500 in row.retry.statuses
    assert row.retry_after_cap_s == 600 and row.min_interval_s == 6.5


# ================================================================ K1 acceptance: failed vs empty
def test_exhausted_429_and_a_real_empty_list_are_distinguishable(fake):
    fake.add(A, citations=[])
    empty = s2.citations(A, expected=None)
    fake.script(CIT, (429, {}, None))
    failed = s2.citations(A, expected=None)
    assert empty.state is WalkState.EMPTY and empty.rows == [] and empty.n_rows == 0 and empty.answered
    assert failed.state is WalkState.FAILED and failed.rows is None and failed.n_rows is None
    assert not failed.answered and failed.kind is Kind.REFUSED


def test_empty_list_while_a_count_is_expected_is_failed_not_empty(fake):
    fake.add(A, references=[])
    w = s2.references(A, expected=5)
    assert w.failed and w.reason.startswith("count_mismatch") and w.rows is None


def test_count_zero_is_empty_with_no_request(fake):
    w = s2.citations(A, expected=0)
    assert w.state is WalkState.EMPTY and w.rows == [] and w.attempts == 0 and fake.sent == []


def test_count_mismatch_keeps_the_rows_as_partial_only(fake):
    fake.add(A, citations=citers(4))
    w = s2.citations(A, expected=5)
    assert w.failed and w.rows is None and len(w.partial) == 4 and w.count_at_walk == 5


def test_a_failed_page_after_a_good_one_fails_the_walk(fake):
    fake.add(A, citations=citers(1500)).script(
        CIT, (200, {}, {"offset": 0, "next": 1000, "data": citers(1000)}), (503, {}, None))
    w = s2.citations(A, expected=1500)
    assert w.failed and w.kind is Kind.OUTAGE and w.rows is None and len(w.partial) == 1000


# ================================================================ K1 acceptance: paging and the 10,000 cap
def test_1011_citers_take_two_pages(fake):
    fake.add(A, citations=citers(1011))
    w = s2.citations(A, expected=1011)
    assert w.state is WalkState.COMPLETE and w.n_rows == 1011
    pages = [(int(r["params"]["offset"]), int(r["params"]["limit"])) for r in fake.sent]
    assert pages == [(0, 1000), (1000, 1000)]
    assert w.rows[0]["paperId"] == f"{0:040x}"              # unwrapped from citingPaper


def test_recorded_1011_citer_pages_replay_to_a_complete_walk(fake):
    fx = fixture("p2_pages_1011.json")
    doi = "DOI:" + fx["pages"][0]["request"]["path"].split("DOI:")[1].rsplit("/citations", 1)[0]
    path = f"/graph/v1/paper/{doi}/citations"
    fake.script(path, *[(p["status"], {}, p["body"]) for p in fx["pages"]])
    w = s2.citations(doi, ("paperId", "year"), expected=fx["citationCount"])
    assert w.state is WalkState.COMPLETE and w.n_rows == 1011 and w.attempts == 2
    assert [r["params"]["offset"] for r in fake.sent] == ["0", "1000"]


def test_paging_never_sends_offset_plus_limit_at_or_over_10000(fake):
    fake.add(A, citations=citers(12_000))
    w = s2.citations(A, expected=12_000)
    assert w.state is WalkState.CAPPED and w.n_rows == 9999 and w.n_unreachable_est == 2001
    sent = [(int(r["params"]["offset"]), int(r["params"]["limit"])) for r in fake.sent]
    assert all(o + lim < 10_000 for o, lim in sent) and sent[-1] == (9000, 999) and len(sent) == 10
    assert not w.failed and w.answered                        # capped is typed, not a silent truncation


def test_exactly_9999_citers_is_complete(fake):
    fake.add(A, citations=citers(9999))
    w = s2.citations(A, expected=9999)
    assert w.state is WalkState.COMPLETE and w.n_rows == 9999


def test_page_params_refuse_to_build_a_request_past_the_cap():
    assert s2._page_params(9000, 999) == {"offset": 9000, "limit": 999}
    for off, lim in ((9000, 1000), (9999, 1), (0, 10_000)):
        with pytest.raises(ValueError):
            s2._page_params(off, lim)


def test_windowed_citations_union_is_capped_with_the_undated_unreachable(fake):
    dated = citers(12_000)
    fake.add(A, citations=dated + citers(50, start=20_000, dated=False))
    w = s2.citations_windowed(A, ("paperId", "publicationDate"), expected=12_050, year_from=2000, year_to=2025)
    assert w.state is WalkState.CAPPED and w.n_rows == 12_000 and w.n_unreachable_est == 50
    assert len({r["paperId"] for r in w.rows}) == 12_000
    sent = [(int(r["params"]["offset"]), int(r["params"]["limit"])) for r in fake.sent]
    assert all(o + lim < 10_000 for o, lim in sent)
    assert any(r["params"].get("publicationDateOrYear") for r in fake.sent)


def test_windowed_citations_fail_on_a_failed_window(fake):
    fake.add(A, citations=citers(10)).script(CIT, (200, {}, {"offset": 8999, "data": []}), (500, {}, None))
    w = s2.citations_windowed(A, expected=10, year_from=2000, year_to=2001)
    assert w.failed and w.kind is Kind.OUTAGE and w.rows is None


def test_live_2026_09_30_answers_replay_to_the_same_states(fake):
    """The 2026-09-30 re-probe (7 unkeyed calls), replayed: the states the live run produced."""
    calls = fixture("live_2026-09-30.json")["calls"]
    for c in calls[:5]:
        fake.script(c["path"], (c["status"], {}, c["body"]))
    batch, cit, elided, refs, missing = calls[:5]
    out = s2.paper_batch([i.split(":", 1)[1] for i in batch["json"]["ids"]], batch["params"]["fields"])
    assert out.ok and [r is None for r in out.payload] == [False, False, True]
    cc, rc = out.payload[0]["citationCount"], out.payload[0]["referenceCount"]
    doi = batch["json"]["ids"][0]
    w = s2.citations(doi, cit["params"]["fields"], expected=cc)
    assert w.state is WalkState.COMPLETE and w.n_rows == cc == len(cit["body"]["data"])
    e = s2.references(batch["json"]["ids"][1], elided["params"]["fields"], expected=out.payload[1]["referenceCount"])
    assert e.state is WalkState.ELIDED and "elided by the publisher" in e.reason
    r = s2.references(doi, refs["params"]["fields"], expected=rc)
    assert r.state is WalkState.COMPLETE and r.n_rows == rc
    nf = s2.citations(batch["json"]["ids"][2], ("paperId",), expected=None)
    assert nf.state is WalkState.NOT_FOUND and "not found" in nf.reason
    assert [x["params"] for x in fake.sent[1:]] == [{k: str(v) for k, v in c["params"].items()}
                                                    for c in calls[1:5]]              # the same requests


def test_live_offset_cap_boundary_matches_the_client_rule():
    """Live 2026-09-30: offset 9999 + limit 1 is a 400, offset 9998 + limit 1 a 200 that still
    advertises next=9999; the client's rule (offset + limit < 10000, stop at offset 9999) matches."""
    calls = fixture("live_2026-09-30.json")["calls"]
    over, under = calls[5], calls[6]
    assert over["status"] == 400 and over["body"]["error"] == "offset + limit must be < 10000"
    assert under["status"] == 200 and under["body"]["next"] == 9999
    with pytest.raises(ValueError):
        s2._page_params(over["params"]["offset"], over["params"]["limit"])
    assert s2._page_params(under["params"]["offset"], under["params"]["limit"])["offset"] == 9998
    assert s2.REACHABLE == under["body"]["next"]


# ================================================================ references: elided, empty, page
def test_elided_references_from_the_recorded_marker(fake):
    fx = fixture("p6_ref_elided.json")
    fake.script(REF, (fx["status"], {}, fx["body"]))
    w = s2.references(A, expected=87)
    assert w.state is WalkState.ELIDED and w.rows is None and "elided by the publisher" in w.reason
    assert w.answered and not w.failed


def test_recorded_empty_references_are_empty(fake):
    fx = fixture("p6_ref_empty.json")
    fake.script(REF, (fx["status"], {}, fx["body"]))
    w = s2.references(A, expected=None)
    assert w.state is WalkState.EMPTY and w.rows == []


def test_recorded_reference_page_is_complete_and_unwrapped(fake):
    fx = fixture("p6_ref_page.json")
    fake.script(REF, (fx["status"], {}, fx["body"]))
    w = s2.references(A, expected=len(fx["body"]["data"]))
    assert w.state is WalkState.COMPLETE and w.n_rows == 63
    assert all("citedPaper" not in r for r in w.rows) and any(r.get("title") for r in w.rows)


def test_data_null_after_the_first_page_is_failed(fake):
    fake.script(REF, (200, {}, {"offset": 0, "next": 1000, "data": [{"citedPaper": {"paperId": "x"}}] * 1000}),
                (200, {}, {"data": None}))
    w = s2.references(A, expected=None)
    assert w.failed and "null" in w.reason and len(w.partial) == 1000


def test_malformed_pages_are_failed(fake):
    fake.script(REF, (200, {}, {"offset": 0, "next": 0, "data": [{"citedPaper": {}}]}))
    assert s2.references(A, expected=None).failed
    fake.scripts.clear()
    fake.script(REF, (200, {}, {"data": []}))                 # [] without offset: not a list answer
    assert s2.references(A, expected=None).failed


# ================================================================ paper_batch
def test_paper_batch_is_aligned_with_nulls_from_the_recorded_chunk(fake):
    fx = fixture("p4_batch_null.json")
    fake.script("/graph/v1/paper/batch", (200, {}, fx["body"]))
    out = s2.paper_batch(fx["ids"], fx["fields"])
    assert out.ok and len(out.payload) == 3
    assert [r is None for r in out.payload] == [r is None for r in fx["body"]]
    assert fake.sent[0]["json"]["ids"] == ["DOI:" + i.split(":", 1)[1].lower() for i in fx["ids"]]


def test_paper_batch_chunks_of_500_keep_order(fake):
    ids = [f"10.5555/x{i}" for i in range(1001)]
    for i in range(1001):
        if i % 7:
            fake.add(f"DOI:10.5555/x{i}", citationCount=i)
    out = s2.paper_batch(ids, ("citationCount",))
    assert out.ok and [len(r["json"]["ids"]) for r in fake.sent] == [500, 500, 1]
    assert [None if r is None else r["citationCount"] for r in out.payload] == \
        [None if i % 7 == 0 else i for i in range(1001)]


def test_paper_batch_failed_chunk_is_typed_in_place(fake):
    ids = [f"10.5555/x{i}" for i in range(1001)]
    for i in range(1001):
        fake.add(f"DOI:10.5555/x{i}", citationCount=1)
    fake.script("/graph/v1/paper/batch", (200, {}, [{"citationCount": 1}] * 500), (503, {}, None))
    out = s2.paper_batch(ids, ("citationCount",))
    assert out.kind is Kind.OUTAGE and not out.ok and len(out.payload) == 1001
    assert all(isinstance(r, dict) for r in out.payload[:500])
    assert all(isinstance(r, Outcome) and r.kind is Kind.OUTAGE for r in out.payload[500:])


def test_paper_batch_skips_invalid_ids_without_sending_them(fake):
    fake.add("DOI:10.5555/ok1", citationCount=3)
    out = s2.paper_batch(["10.5555/ok1", "not a doi", "10.1145/nnnnnnn.nnnnnnn"], ("citationCount",))
    assert out.ok and out.payload[0]["citationCount"] == 3
    assert all(isinstance(r, Outcome) and r.kind is Kind.SKIPPED for r in out.payload[1:])
    assert fake.sent[0]["json"]["ids"] == ["DOI:10.5555/ok1"] and "skipped" in out.detail


def test_paper_batch_wrong_length_answer_is_an_error(fake):
    fake.script("/graph/v1/paper/batch", (200, {}, [None]))
    out = s2.paper_batch(["10.5555/a1", "10.5555/b1"], ("title",))
    assert out.kind is Kind.ERROR and all(isinstance(r, Outcome) for r in out.payload)


def test_paper_batch_of_nothing_sends_nothing(fake):
    out = s2.paper_batch([], ("title",))
    assert out.ok and out.payload == [] and fake.sent == []


# ================================================================ batch_nested
def test_batch_nested_verifies_each_id_and_refetches_truncated_lists(fake):
    fake.add("DOI:10.5555/a1", citations=citers(300), citationCount=300)
    fake.add("DOI:10.5555/b1", citations=citers(200, start=1000), citationCount=200)
    fake.add("DOI:10.5555/z1", citations=[], citationCount=0)
    fake.add("DOI:10.5555/big1", citations=citers(1200, start=5000), citationCount=1200)
    fake.truncate["DOI:10.5555/b1"] = 150                    # served short: must be re-fetched by paged GET
    ids = ["10.5555/a1", "10.5555/b1", "10.5555/z1", "10.5555/big1", "10.5555/none1"]
    out = s2.batch_nested(ids, "citations", ("paperId",), cap=1000)
    assert out["10.5555/a1"].state is WalkState.COMPLETE and out["10.5555/a1"].n_rows == 300
    assert out["10.5555/b1"].state is WalkState.COMPLETE and out["10.5555/b1"].n_rows == 200
    assert out["10.5555/z1"].state is WalkState.EMPTY
    assert out["10.5555/big1"].state is WalkState.COMPLETE and out["10.5555/big1"].n_rows == 1200
    assert out["10.5555/none1"].state is WalkState.NOT_FOUND
    gets = [r["path"] for r in fake.sent if r["method"] == "GET"]
    assert sorted(set(gets)) == ["/graph/v1/paper/DOI:10.5555/b1/citations",
                                 "/graph/v1/paper/DOI:10.5555/big1/citations"]
    nested = [r for r in fake.sent if r["method"] == "POST" and "citations.paperId" in r["params"]["fields"]]
    assert len(nested) == 1 and nested[0]["params"]["fields"].startswith("citationCount,")
    assert sorted(nested[0]["json"]["ids"]) == ["DOI:10.5555/a1", "DOI:10.5555/b1"]


def test_batch_nested_over_the_nested_cap_is_caught_by_the_count_check(fake):
    fake.nested_cap = 500                                    # S2 truncates silently, not from the tail
    fake.add("DOI:10.5555/a1", citations=citers(400), citationCount=400)
    fake.add("DOI:10.5555/b1", citations=citers(300, start=400), citationCount=300)
    out = s2.batch_nested(["10.5555/a1", "10.5555/b1"], "citations", ("paperId",),
                          counts={"10.5555/a1": 400, "10.5555/b1": 300})
    assert all(w.state is WalkState.COMPLETE for w in out.values())
    assert [w.n_rows for w in out.values()] == [400, 300]


def test_batch_nested_references_from_the_recorded_batch(fake):
    fx = fixture("p6_batch_refs.json")
    for i, r in zip(fx["ids"], fx["body"]):              # FakeS2 serves the recorded rows in request order
        refs = None if r["references"] is None else [{"citedPaper": x} for x in r["references"]]
        fake.add("DOI:" + i.split(":", 1)[1].lower(), references=refs, referenceCount=r["referenceCount"],
                 paperId=r["paperId"])
    counts = {i: r["referenceCount"] for i, r in zip(fx["ids"], fx["body"])}
    out = s2.batch_nested(fx["ids"], "references", ("paperId",), counts=counts)
    states = {i: w.state for i, w in out.items()}
    elided = [i for i, r in zip(fx["ids"], fx["body"]) if r["references"] is None]
    assert elided and all(states[i] is WalkState.ELIDED for i in elided)
    assert all(out[i].reason.startswith("Notice: The following paper fields have been elided") for i in elided)
    for i, r in zip(fx["ids"], fx["body"]):
        if r["references"] is not None:
            want = WalkState.EMPTY if r["referenceCount"] == 0 else WalkState.COMPLETE
            assert states[i] is want and out[i].n_rows == r["referenceCount"], i
    zero = [i for i, n in counts.items() if n == 0]
    sent_ids = fake.sent[0]["json"]["ids"]
    assert len(fake.sent) == 1 and len(sent_ids) == len(counts) - len(zero)
    assert zero and not any("DOI:" + z.split(":", 1)[1].lower() in sent_ids for z in zero)   # count 0: no call


def test_batch_nested_failed_call_fails_every_id_in_it(fake):
    fake.add("DOI:10.5555/a1", citations=citers(3), citationCount=3)
    fake.script("/graph/v1/paper/batch", (502, {}, None))
    out = s2.batch_nested(["10.5555/a1"], "citations", ("paperId",), counts={"10.5555/a1": 3})
    w = out["10.5555/a1"]
    assert w.failed and w.kind is Kind.OUTAGE and w.rows is None


def test_batch_nested_without_counts_makes_one_metadata_pass(fake):
    fake.add("DOI:10.5555/a1", citations=citers(3), citationCount=3)
    out = s2.batch_nested(["10.5555/a1"], "citations", ("paperId",))
    assert out["10.5555/a1"].state is WalkState.COMPLETE
    assert [r["params"]["fields"] for r in fake.sent] == ["citationCount", "citationCount,citations.paperId"]


def test_pack_ffd_respects_both_caps():
    bins = s2.pack_ffd({f"i{k}": n for k, n in enumerate([900, 800, 300, 200, 100, 100])}, cap=1000, max_ids=2)
    assert all(len(b) <= 2 for b in bins)
    sizes = {f"i{k}": n for k, n in enumerate([900, 800, 300, 200, 100, 100])}
    assert all(sum(sizes[i] for i in b) <= 1000 for b in bins)
    assert sorted(i for b in bins for i in b) == sorted(sizes)


# ================================================================ match_title and recommendations
def test_match_title_accepts_the_recorded_exact_hit(fake):
    fx = fixture("p5_match_hit.json")
    fake.script("/graph/v1/paper/search/match", (fx["status"], {}, fx["body"]))
    m = s2.match_title(fx["query"])
    assert isinstance(m, s2.Match) and m.decision == "accept" and m.similarity >= 0.95
    assert fake.sent[0]["params"]["query"] == fx["query"] and "year" not in fake.sent[0]["params"]


def test_match_title_404_is_not_found_meaning_unresolved(fake):
    fx = fixture("p5_match_404.json")
    fake.script("/graph/v1/paper/search/match", (fx["status"], {}, fx["body"]))
    nf = s2.match_title(fx["query"])
    assert isinstance(nf, s2.NotFound) and nf.attempts == 1


def test_match_title_failure_is_failed_not_not_found(fake):
    fake.script("/graph/v1/paper/search/match", (429, {}, None))
    f = s2.match_title("Heat acclimation in trained runners")
    assert isinstance(f, s2.Failed) and f.kind is Kind.REFUSED and f.status == 429


def _match_body(title, year=2016, author="Rachel Vanscoy"):
    return {"data": [{"paperId": "p" * 40, "title": title, "year": year, "authors": [{"name": author}]}]}


def test_match_title_near_band_needs_year_and_first_author(fake):
    got = "Does the shortened environmental symptoms questionnaire represent heat acclimation adaptations"
    q = "Does the shortened environmental symptoms questionnaire accurately represent heat acclimation adaptation"
    assert 0.80 <= s2.title_similarity(q, got) < 0.95
    fake.script("/graph/v1/paper/search/match", (200, {}, _match_body(got)))
    assert s2.match_title(q, year=2017, first_author="vanscoy").decision == "accept"
    assert s2.match_title(q, year=2019, first_author="vanscoy").decision == "review"
    assert s2.match_title(q).decision == "review"


def test_match_title_low_similarity_needs_tokens_and_text_head(fake):
    got = "Heat strain in firefighters during live-fire training: a field study"
    fake.script("/graph/v1/paper/search/match", (200, {}, _match_body(got)))
    head = "Journal of X. " + got + ". Abstract. We measured core temperature."
    assert s2.match_title("heat strain firefighters", text_head=head).decision == "accept"
    assert s2.match_title("heat strain firefighters").decision == "review"


def test_match_title_refuses_a_blank_title(fake):
    with pytest.raises(ValueError):
        s2.match_title("   ")
    assert fake.sent == []


def test_recommend_for_paper_ok_empty_and_not_found(fake):
    fx = fixture("p8_forpaper.json")
    doi = "10.1152/jappl.1964.19.3.531"
    path = f"/recommendations/v1/papers/forpaper/DOI:{doi}"
    fake.script(path, (200, {}, fx["body"]), (200, {}, {"recommendedPapers": []}), (404, {}, {"error": "Input papers not found"}))
    ok = s2.recommend_for_paper(doi)
    assert ok.ok and len(ok.payload) == 3
    assert fake.sent[0]["raw_path"].endswith("DOI:10.1152%2Fjappl.1964.19.3.531")     # quote(safe=""), P8-C5
    assert fake.sent[0]["params"]["from"] == "recent"
    empty = s2.recommend_for_paper(doi)
    assert empty.ok and empty.payload == []
    nf = s2.recommend_for_paper(doi)
    assert nf.kind is Kind.NO_MATCH and not nf.ok


def test_recommend_post_and_malformed_answer(fake):
    fake.script("/recommendations/v1/papers", (200, {}, {"recommendedPapers": [{"paperId": "a"}]}), (200, {}, {"x": 1}))
    out = s2.recommend(["a" * 40], ["b" * 40], limit=10)
    assert out.ok and out.payload == [{"paperId": "a"}]
    assert fake.sent[0]["json"] == {"positivePaperIds": ["a" * 40], "negativePaperIds": ["b" * 40]}
    assert s2.recommend(["a" * 40]).kind is Kind.ERROR


def test_graph_paths_keep_the_doi_slash_and_encode_the_rest(fake):
    doi = "10.1002/(sici)1097-4636(199706)35:4<485::aid-jbm8>3.0.co;2-c"
    s2.citations(doi, expected=None)
    raw = fake.sent[0]["raw_path"]
    assert raw.startswith("/graph/v1/paper/DOI:10.1002/%28sici%29") and "%3C485%3A%3Aaid" in raw


# ================================================================ key, ledger, budget, breaker
def test_ledger_never_holds_the_key_and_logs_its_header_name(fake, s2env, monkeypatch):
    monkeypatch.setenv(s2.KEY_ENV, SECRET)
    fake.add(A, citations=citers(2))
    fake.script("/graph/v1/paper/batch", (200, {}, [None]))
    s2.citations(A, expected=2)
    s2.paper_batch(["10.5555/q1"], ("title",))
    assert all(r["headers"].get("x-api-key") == SECRET for r in fake.sent)   # sent on the wire
    text = s2env.ledger_text()
    assert SECRET not in text and SECRET[-12:] not in text
    lines = s2env.ledger_lines()
    assert lines and all("x-api-key" in rec["req_headers"] for rec in lines)
    assert all(rec["purpose"].startswith("s2 ") for rec in lines)


def test_no_key_header_when_the_key_is_absent(fake):
    fake.add(A, citations=citers(1))
    s2.citations(A, expected=1)
    assert "x-api-key" not in {k.lower() for k in fake.sent[0]["headers"]}
    assert s2.key_status() == "key: absent"


def test_status_cli_prints_presence_never_the_key(s2env, monkeypatch, capsys):
    monkeypatch.setenv(s2.KEY_ENV, SECRET)
    assert s2.main(["--status"]) == 0
    out = capsys.readouterr().out
    assert "key: present" in out and SECRET not in out and "spacing=1.1s" in out
    r = subprocess.run([sys.executable, "-m", "litpipe.s2", "--help"], cwd=REPO, capture_output=True, text=True,
                       env={**os.environ, s2.KEY_ENV: SECRET}, timeout=60)
    assert r.returncode == 0 and SECRET not in r.stdout + r.stderr


def test_run_budget_is_enforced_mid_retry_and_aborts_the_session(fake, s2env):
    fake.add(A, citations=citers(1)).script(CIT, (429, {}, None))
    sess = s2.Session(budget=3)
    w = s2.citations(A, expected=1, session=sess)
    assert len(fake.sent) == 3 and sess.attempts == 3                # the 4th attempt was never sent
    assert w.failed and w.kind is Kind.DEFERRED and sess.aborted == "budget"
    again = s2.citations(A, expected=1, session=sess)
    assert again.failed and again.kind is Kind.DEFERRED and len(fake.sent) == 3


def test_breaker_trips_after_consecutive_failed_calls(fake):
    fake.add(A, citations=citers(1)).script(CIT, (400, {}, {"error": "bad"}))
    sess = s2.Session(breaker=3)
    for _ in range(3):
        assert s2.citations(A, expected=1, session=sess).failed
    assert sess.aborted == "breaker" and len(fake.sent) == 3
    w = s2.citations(A, expected=1, session=sess)
    assert w.failed and w.kind is Kind.DEFERRED and len(fake.sent) == 3
    assert "ABORTED=breaker" in sess.summary_line()


def test_not_found_does_not_count_toward_the_breaker(fake):
    sess = s2.Session(breaker=2)
    for _ in range(4):
        assert s2.citations("10.5555/none1", expected=None, session=sess).state is WalkState.NOT_FOUND
    assert sess.aborted is None


def test_session_summary_counts(fake):
    fake.add(A, citations=citers(1011), references=None)
    sess = s2.Session()
    s2.citations(A, expected=1011, session=sess)
    s2.references(A, expected=5, session=sess)
    sm = sess.summary()
    assert sm["calls"] == 3 and sm["attempts"] == 3 and sm["walks"] == {"complete": 1, "elided": 1}
    assert sm["bytes"] > 0 and sm["key"] == "key: absent"


def test_settings_from_the_s2_block_and_validation(s2env):
    s2env.write_config(s2={"spacing_s": 8.0, "max_requests_per_run": 50, "breaker": 5})
    st = s2.settings()
    assert (st.spacing_s, st.spacing_keyed_s, st.max_requests_per_run, st.breaker) == (8.0, 1.1, 50, 5)
    assert s2.Session().budget == 50
    s2env.write_config(s2={"max_requests_per_run": None})
    assert s2.Session().budget is None
    for bad in ({"spacing_s": 0.5}, {"spacing_keyed_s": 1.0}, {"max_requests_per_run": 0},
                {"max_requests_per_run": "10"}, {"breaker": 0}, {"spacing_s": True}):
        s2env.write_config(s2=bad)
        with pytest.raises(config.ConfigError):
            s2.settings()


def test_config_spacing_reaches_the_pacing(fake, s2env):
    s2env.write_config(s2={"spacing_s": 9.0})
    fake.add(A, citations=citers(1))
    s2.reset_default_session()
    s2.citations(A, expected=1)
    s2.citations(A, expected=1)
    assert acquired_gaps(s2env.state) == [9.0]


def test_paper_id_forms():
    assert s2.paper_id("https://doi.org/10.1152/JAPPL.1964.19.3.531") == "DOI:10.1152/jappl.1964.19.3.531"
    assert s2.paper_id("DOI:10.1152/x.1") == "DOI:10.1152/x.1"
    assert s2.paper_id("CorpusId:19633341") == "CorpusId:19633341"
    assert s2.paper_id("f2b028a5d0040dde261b2e1ecea5ca6ef83e4632") == "f2b028a5d0040dde261b2e1ecea5ca6ef83e4632"
    for bad in ("", "hello", "10.1145/nnnnnnn.nnnnnnn"):
        with pytest.raises(ValueError):
            s2.paper_id(bad)


def test_importing_s2_leaves_the_host_table_alone_even_with_a_key():
    code = ("import os; os.environ['S2_API_KEY'] = 'k'; from litpipe import hosts, s2; "
            "print(hosts.policy('api.semanticscholar.org').min_interval_s)")
    r = subprocess.run([sys.executable, "-c", code], cwd=REPO, capture_output=True, text=True, timeout=60)
    assert r.returncode == 0 and r.stdout.strip() == "6.5", r.stderr


def test_no_or_empty_fallback_on_any_s2_list_in_the_source():
    """K1: no `x or []` (or `or ()`, `or {}`) anywhere in litpipe/s2.py: that idiom is how a null
    (elided) or failed list became an empty one (09-16; V_P6_P8 method issue 2)."""
    tree = ast.parse((REPO / "litpipe" / "s2.py").read_text(encoding="utf-8"))
    hits = []
    for node in ast.walk(tree):
        if isinstance(node, ast.BoolOp) and isinstance(node.op, ast.Or):
            last = node.values[-1]
            if isinstance(last, (ast.List, ast.Tuple, ast.Dict)) and not getattr(last, "elts", getattr(last, "keys", [1])):
                hits.append(node.lineno)
    assert hits == [], f"`or []`-style fallbacks at lines {hits}"


# ================================================================ self-tests ported (s2probe T0-T16)
@pytest.fixture
def mock_s2(s2env, mock_server, monkeypatch):
    """A loopback MockServer standing in for S2: BASE points at it and the S2 row is copied onto it."""
    srv = mock_server()
    monkeypatch.setattr(s2, "BASE", srv.url(""))
    return srv


def test_selftest_T0_guards_refuse_before_sending(s2env, monkeypatch):
    sent = []
    monkeypatch.setitem(net._TRANSPORTS, "requests", lambda *a, **k: sent.append(a) or net._Raw(200))
    with pytest.raises(ValueError):
        s2.citations("not an id", expected=None)            # an id that is not a DOI or S2 id: nothing sent
    with pytest.raises(ValueError):
        s2.batch_nested(["10.5555/a1"], "cites", ("paperId",))
    with pytest.raises(ValueError):
        s2.recommend_for_paper("10.5555/a1", pool="everything")
    assert sent == []


def test_selftest_T1_basic_get_ledger_line_and_identity(mock_s2, s2env):
    mock_s2.script(CIT, Reply(200, json.dumps({"offset": 0, "data": citers(1)}), {"Content-Type": "application/json"}))
    w = s2.citations(A, ("title", "year"), expected=1)
    assert w.state is WalkState.COMPLETE
    hit = mock_s2.hits_for(CIT)[0]
    assert hit.headers["User-Agent"].startswith("literature-pipeline/") and "x-api-key" not in {
        k.lower() for k in hit.headers}
    (rec,) = s2env.ledger_lines()
    for key in ("ts", "host", "method", "url", "status", "decision", "elapsed_ms", "bytes", "req_headers",
                "resp_headers", "purpose"):
        assert key in rec
    assert rec["status"] == 200 and rec["decision"] == "final" and rec["purpose"] == "s2 citations offset=0"
    assert "tester" not in json.dumps(rec)                  # the contact email never reaches the ledger


def test_selftest_T2_spacing_from_the_end_of_the_previous_attempt(mock_s2, s2env):
    mock_s2.script(CIT, Reply(200, json.dumps({"offset": 0, "data": []}), {"Content-Type": "application/json"}))
    for _ in range(4):
        s2.citations(A, expected=None)
    assert acquired_gaps(s2env.state, "127.0.0.1") == [6.5, 6.5, 6.5]
    assert hosts.policy("127.0.0.1").min_interval_s == 6.5


def test_selftest_T3_T4_T8_every_attempt_is_ledgered_through_exhaustion(mock_s2, s2env):
    mock_s2.script(CIT, Reply(429, "{}", {"Content-Type": "application/json"}))
    w = s2.citations(A, expected=None)
    assert w.failed and w.attempts == 7 and len(mock_s2.hits_for(CIT)) == 7
    decisions = [r["decision"] for r in s2env.ledger_lines()]
    assert decisions == ["retry"] * 6 + ["final"] and s2env.clock.sleeps == [2.0, 4.0, 8.0, 16.0, 32.0, 64.0]


def test_selftest_T5_T6_retry_after(mock_s2, s2env):
    j = {"Content-Type": "application/json"}
    mock_s2.script(CIT, Reply(429, "{}", {"Retry-After": "3", **j}), Reply(200, '{"offset": 0, "data": []}', j))
    assert s2.citations(A, expected=None).state is WalkState.EMPTY and s2env.clock.sleeps == [3.0]
    mock_s2.script(REF, Reply(503, "{}", {"Retry-After": "601", **j}))
    w = s2.references(A, expected=None)
    assert w.failed and w.kind is Kind.DEFERRED and w.retry_after == 601


def test_selftest_T7_5xx_retried_404_not(mock_s2):
    j = {"Content-Type": "application/json"}
    mock_s2.script(CIT, Reply(502, "{}", j), Reply(504, "{}", j), Reply(500, "{}", j),
                   Reply(200, '{"offset": 0, "data": []}', j))
    assert s2.citations(A, expected=None).attempts == 4
    mock_s2.script(REF, Reply(404, '{"error": "Paper with id x not found"}', j))
    w = s2.references(A, expected=None)
    assert w.state is WalkState.NOT_FOUND and len(mock_s2.hits_for(REF)) == 1


def test_selftest_T9_post_batch_retried_on_503(mock_s2):
    j = {"Content-Type": "application/json"}
    mock_s2.script("/graph/v1/paper/batch", Reply(503, "{}", j), Reply(200, "[null, null, null]", j))
    out = s2.paper_batch(["10.5555/a1", "10.5555/b1", "10.5555/c1"], ("title",))
    hits = mock_s2.hits_for("/graph/v1/paper/batch")
    assert out.ok and out.payload == [None, None, None] and len(hits) == 2
    assert all(json.loads(h.body)["ids"] == ["DOI:10.5555/a1", "DOI:10.5555/b1", "DOI:10.5555/c1"] for h in hits)


def test_selftest_T10_read_timeout_is_an_attempt_and_retried(mock_s2, monkeypatch):
    monkeypatch.setattr(s2, "TIMEOUT_GET", (2, 0.3))
    j = {"Content-Type": "application/json"}
    mock_s2.script(CIT, Reply(200, '{"offset": 0, "data": []}', j, delay=1.0), Reply(200, '{"offset": 0, "data": []}', j))
    w = s2.citations(A, expected=None)
    assert w.state is WalkState.EMPTY and w.attempts == 2


def test_selftest_T11_connection_refused_is_transport(s2env, monkeypatch):
    import socket
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()                                             # nothing listens on this port
    monkeypatch.setattr(s2, "BASE", f"http://127.0.0.1:{port}")
    monkeypatch.setattr(s2, "TIMEOUT_GET", (0.3, 1))         # Windows retries a refused SYN for ~2 s
    w = s2.citations(A, expected=None)
    assert w.failed and w.kind is Kind.TRANSPORT and w.attempts == 3 and w.rows is None


def test_selftest_T12_T13_budget_before_send_and_mid_retry(mock_s2):
    j = {"Content-Type": "application/json"}
    mock_s2.script(CIT, Reply(429, "{}", j), Reply(429, "{}", j), Reply(200, '{"offset": 0, "data": []}', j))
    sess = s2.Session(budget=2)
    w = s2.citations(A, expected=None, session=sess)
    assert w.failed and w.kind is Kind.DEFERRED and len(mock_s2.hits_for(CIT)) == 2
    assert s2.citations(A, expected=None, session=sess).kind is Kind.DEFERRED and len(mock_s2.hits_for(CIT)) == 2


def test_selftest_T15_mock_hits_reconcile_with_ledger_attempts(mock_s2, s2env):
    j = {"Content-Type": "application/json"}
    mock_s2.script(CIT, Reply(503, "{}", j), Reply(200, json.dumps({"offset": 0, "next": 1000, "data": citers(1000)}), j),
                   Reply(200, json.dumps({"offset": 1000, "data": citers(5, 1000)}), j))
    mock_s2.script(REF, Reply(404, "{}", j))
    s2.citations(A, expected=1005)
    s2.references(A, expected=None)
    sent_lines = [r for r in s2env.ledger_lines() if r["status"] is not None]
    assert len(sent_lines) == len(mock_s2.hits) == 4


def test_selftest_T16_live_state_untouched(s2env):
    assert ledger.LEDGER_DIR == s2env.ledger_dir and isinstance(net.STATE, type(s2env.state))
    assert str(config.CONFIG_PATH) == str(s2env.cfg_path)


# ------------------------------------------------------------------------------ real litpipe.state
@pytest.fixture
def real_state(tmp_path, monkeypatch, net_env):
    """The real litpipe.state on a temp database and the real clock (no retries in these tests)."""
    import litpipe.state as st
    monkeypatch.setattr(st, "DB_PATH", tmp_path / "state.sqlite")
    monkeypatch.setattr(net, "STATE", None)
    monkeypatch.setattr(net, "CLOCK", net.SystemClock())
    monkeypatch.delenv(s2.KEY_ENV, raising=False)
    net_env.write_config(s2={"spacing_s": 1.1})
    s2.reset_default_session()
    yield st
    s2.reset_default_session()
    hosts.reset()


def test_real_state_paces_and_the_run_budget_tuple_is_caught(real_state, mock_server, monkeypatch):
    srv = mock_server()
    monkeypatch.setattr(s2, "BASE", srv.url(""))
    srv.script(CIT, Reply(200, '{"offset": 0, "data": []}', {"Content-Type": "application/json"}))
    sess = s2.Session(budget=2)
    t0 = time.monotonic()
    assert s2.citations(A, expected=None, session=sess).state is WalkState.EMPTY
    assert s2.citations(A, expected=None, session=sess).state is WalkState.EMPTY
    assert time.monotonic() - t0 >= 1.0                       # the second call waited for the 1.1 s spacing
    w = s2.citations(A, expected=None, session=sess)           # budget spent: RunBudgetSpent caught by net
    assert w.failed and w.kind is Kind.DEFERRED and len(srv.hits) == 2
    assert real_state.day_count("127.0.0.1") == 2


_CHILD = r"""
import json, sys, time
sys.path.insert(0, sys.argv[1])
from litpipe import config, net, s2
import litpipe.state as st
config.CONFIG_PATH = __import__("pathlib").Path(sys.argv[2])
s2.BASE = sys.argv[3]
out = []
for _ in range(2):
    w = s2.citations("DOI:10.5555/a1", expected=None)
    out.append(w.state.value)
print(json.dumps(out))
"""


def test_selftest_T14_two_processes_share_the_pacing(tmp_path, mock_server, monkeypatch):
    """Two processes x 2 calls on one temp state database: every arrival at least the spacing
    after the previous one ended (s2probe T14, now through litpipe.state)."""
    srv = mock_server()
    path = "/graph/v1/paper/DOI:10.5555/a1/citations"
    srv.script(path, Reply(200, '{"offset": 0, "data": []}', {"Content-Type": "application/json"}))
    cfg = tmp_path / "projects.json"
    cfg.write_text(json.dumps({"state_dir": str(tmp_path / "state"), "projects": {}, "s2": {"spacing_s": 1.1}}),
                   encoding="utf-8")
    # Create the state database first: two processes initialising a brand-new one at the same
    # moment can hit "database is locked" in litpipe.state._init (forwarded to its owner); this
    # test is about S2 pacing across processes, not first use.
    import litpipe.state as st
    monkeypatch.setattr(config, "CONFIG_PATH", cfg)
    monkeypatch.setattr(st, "DB_PATH", None)
    assert st.day_count("127.0.0.1") == 0 and st.db_path().is_relative_to(tmp_path)
    script = tmp_path / "child.py"
    script.write_text(_CHILD, encoding="utf-8")
    env = {k: v for k, v in os.environ.items() if k not in (s2.KEY_ENV, "LITPIPE_RUN_ID")}
    env.update(NO_PROXY="*", PYTHONUTF8="1")
    procs = [subprocess.Popen([sys.executable, str(script), str(REPO), str(cfg), srv.url("")], env=env,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) for _ in range(2)]
    outs = [p.communicate(timeout=120) for p in procs]
    assert all(p.returncode == 0 for p in procs), [o[1][-500:] for o in outs]
    assert all(json.loads(o[0].strip().splitlines()[-1]) == ["empty", "empty"] for o in outs)
    assert len(srv.hits_for(path)) == 4
    led = []
    for f in sorted((tmp_path / "state" / "ledger").glob("*.jsonl")):
        led += [json.loads(line) for line in f.read_text(encoding="utf-8").splitlines() if line.strip()]
    from datetime import datetime
    spans = sorted((datetime.fromisoformat(r["ts"].replace("Z", "+00:00")).timestamp(), r["elapsed_ms"] / 1000)
                   for r in led)
    gaps = [b[0] - (a[0] + a[1]) for a, b in zip(spans, spans[1:])]
    assert len(spans) == 4 and min(gaps) >= 1.0, gaps
