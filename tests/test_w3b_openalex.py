"""litpipe.openalex offline (dispatch 0.5 contract, W3-B): batching under the URL limit, typed
answers, the dangling IDs, a count of 0 as unknown, cursor paging, the Bearer key, the
X-RateLimit budget, and content_pdf's paid-call gates. Every request hits OAStub, a stub of
litpipe.net's transport answering like api.openalex.org (and, for content_pdf only,
content.openalex.org); nothing is sent anywhere. The recorded V2 answers for the 15 core papers
are in tests/fixtures/W3-B/."""
import json
import statistics
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

import pytest
from requests.structures import CaseInsensitiveDict

from litpipe import config, hosts, ledger, net
from litpipe import openalex as oa
from litpipe.outcomes import Kind

FIX = Path(__file__).parent / "fixtures" / "W3-B"
SECRET = "oa-TESTKEY-w3b-0f1e2d3c4b5a"
RL = {"X-RateLimit-Limit": "10000", "X-RateLimit-Remaining": "9000", "X-RateLimit-Credits-Used": "1",
      "X-RateLimit-Reset": "3600", "X-RateLimit-Cost-USD": "0.0001"}


def _raw(status, body, headers=None):
    data = body if isinstance(body, bytes) else json.dumps(body).encode("utf-8")
    h = CaseInsensitiveDict({"Content-Type": "application/json", **(headers or {})})
    return net._Raw(status, h, data, data[:net.CHUNK], len(data))


def short(x):
    return str(x).rsplit("/", 1)[-1].upper()


def bare(d):
    return (d or "").replace("https://doi.org/", "").lower()


class OAStub:
    """api.openalex.org on a stub transport: /works list calls (filter doi:, openalex:, cites: with
    a cursor), /works/doi:{doi} and /works/W... singletons; `select` honoured. `script(n, status,
    headers=..., body=...)` answers the next n calls with that status instead. Every call is
    recorded in `sent` (url, params, headers)."""

    def __init__(self, headers=None):
        self.works = {}
        self.cites = {}
        self.sent = []
        self.headers = dict(RL if headers is None else headers)
        self.queue = []
        self.page_size_seen = []

    def add(self, *works):
        """Add works; a work added twice (a seed that another seed also cites) keeps every field."""
        for w in works:
            k = short(w["id"])
            self.works[k] = {**self.works.get(k, {}), **w}
        return self

    def script(self, n, status, headers=None, body=None):
        for _ in range(n):
            self.queue.append((status, headers if headers is not None else {}, body))
        return self

    def by_doi(self, d):
        return next((w for w in self.works.values() if bare(w.get("doi")) == d), None)

    @staticmethod
    def _sel(w, q):
        sel = q.get("select")
        return {k: v for k, v in w.items() if k in sel.split(",")} if sel else dict(w)

    def __call__(self, method, url, hdrs, body, timeout, max_bytes):
        parts = urlsplit(url)
        q = {k: v[-1] for k, v in parse_qs(parts.query).items()}
        path = unquote(parts.path)
        self.sent.append({"url": url, "host": parts.hostname, "path": path, "params": q, "headers": dict(hdrs)})
        if self.queue:
            status, h, b = self.queue.pop(0)
            return _raw(status, b if b is not None else {"error": "scripted", "message": f"HTTP {status}"}, h)
        if parts.hostname == "content.openalex.org":
            return _raw(200, b"%PDF-1.7 cached copy", {"Content-Type": "application/pdf", **self.headers})
        if path == "/works":
            field, _, vals = q["filter"].partition(":")
            if field == "doi":
                res = [w for v in vals.split("|") if (w := self.by_doi(bare(v)))]
            elif field == "openalex":
                res = [self.works[short(v)] for v in vals.split("|") if short(v) in self.works]
            elif field == "cites":
                allw = self.cites.get(short(vals), [])
                per = int(q.get("per_page", 25))
                start = 0 if q.get("cursor") == "*" else int(q["cursor"][1:])
                page = allw[start:start + per]
                nxt = f"c{start + per}" if start + per < len(allw) else None
                return _raw(200, {"meta": {"count": len(allw), "next_cursor": nxt},
                                  "results": [self._sel(w, q) for w in page]}, self.headers)
            else:
                return _raw(400, {"error": "Invalid query parameters error.", "message": f"bad filter {field}"})
            return _raw(200, {"meta": {"count": len(res)}, "results": [self._sel(w, q) for w in res]}, self.headers)
        if path.startswith("/works/doi:"):
            w = self.by_doi(bare(path[len("/works/doi:"):]))
        elif path.startswith("/works/"):
            w = self.works.get(short(path[len("/works/"):]))
        else:
            w = None
        if w is None:
            return _raw(404, b"<!doctype html><title>404 Not Found</title>",
                        {"Content-Type": "text/html; charset=utf-8", **self.headers})
        return _raw(200, self._sel(w, q), {**self.headers, "X-RateLimit-Credits-Used": "0"})

    def list_calls(self):
        return [s for s in self.sent if s["path"] == "/works"]


@pytest.fixture
def oaenv(net_env, monkeypatch):
    monkeypatch.setenv(oa.KEY_ENV, SECRET)
    oa.reset_default_session()
    hosts.reset()
    yield net_env
    oa.reset_default_session()
    hosts.reset()


@pytest.fixture
def stub(oaenv, monkeypatch):
    st = OAStub()
    monkeypatch.setitem(net._TRANSPORTS, "requests", st)
    monkeypatch.setitem(net._TRANSPORTS, "urllib", st)
    return st


def work(i, doi=None, refs=None, **kw):
    w = {"id": f"https://openalex.org/W{i}", "doi": f"https://doi.org/{doi}" if doi else None,
         "display_name": f"Work {i}", "publication_year": 2000 + i % 20}
    if refs is not None:
        w["referenced_works"] = [f"https://openalex.org/W{r}" for r in refs]
        w["referenced_works_count"] = len(refs)
    w.update(kw)
    return w


# ================================================================ batching and the URL limit
def test_works_by_doi_batches_at_most_100_per_call(stub):
    dois = [f"10.5555/item.{i:05d}" for i in range(250)]
    stub.add(*[work(i, d) for i, d in enumerate(dois)])
    res = oa.works_by_doi(dois)
    calls = stub.list_calls()
    assert len(calls) == 3
    assert [len(c["params"]["filter"].split("|")) for c in calls] == [100, 100, 50]
    assert all(r.ok and bare(r.payload["doi"]) == d for d, r in res.items())
    assert list(res) == dois


def test_long_dois_close_a_chunk_on_url_bytes_before_100(stub):
    dois = [f"10.5555/{'x' * 110}.{i:04d}" for i in range(120)]
    stub.add(*[work(i, d) for i, d in enumerate(dois)])
    res = oa.works_by_doi(dois)
    calls = stub.list_calls()
    assert len(calls) >= 3 and all(r.ok for r in res.values())
    for c in calls:
        assert oa.url_bytes(c["url"]) <= oa.URL_SAFE_BYTES < oa.URL_LIMIT_BYTES
        assert len(c["url"].encode()) < oa.URL_LIMIT_BYTES
        assert len(c["params"]["filter"].split("|")) < 100


def test_url_bytes_counts_slashes_twice_as_the_server_does():
    url = "https://api.openalex.org/works?filter=doi:10.1/a|10.2/b"
    assert oa.url_bytes(url) == len(url) + 2 * url.count("/")


def test_chunks_refuse_a_value_that_alone_overflows():
    with pytest.raises(ValueError):
        oa.chunks(["10.5555/" + "y" * 8000], "doi", {"select": "id"})


def test_dois_go_in_the_query_through_params_and_are_normalised(stub):
    stub.add(work(1, "10.1152/japplphysiol.00775.2024"))
    res = oa.works_by_doi(["https://doi.org/10.1152/JAPPLPHYSIOL.00775.2024"])
    (only,) = res.values()
    assert only.ok
    c = stub.list_calls()[0]
    assert c["params"]["filter"] == "doi:10.1152/japplphysiol.00775.2024"
    assert "%2F" in c["url"] and "%7C" not in c["url"]          # one value: no separator


# ================================================================ typed answers
def test_missing_doi_is_no_match_and_a_non_doi_is_skipped_unsent(stub):
    stub.add(work(1, "10.5555/a.1"))
    res = oa.works_by_doi(["10.5555/a.1", "10.5555/a.2", "not a doi"])
    assert res["10.5555/a.1"].ok
    assert res["10.5555/a.2"].kind is Kind.NO_MATCH and res["10.5555/a.2"].payload is None
    assert res["not a doi"].kind is Kind.SKIPPED
    assert stub.list_calls()[0]["params"]["filter"] == "doi:10.5555/a.1|10.5555/a.2"


def test_a_failed_batch_gives_every_doi_its_failure_never_an_empty_answer(stub, oaenv):
    stub.script(4, 503)                                            # first try plus 3 retries
    res = oa.works_by_doi(["10.5555/a.1", "10.5555/a.2"])
    assert {r.kind for r in res.values()} == {Kind.OUTAGE}
    assert all(r.payload is None or not isinstance(r.payload, (list, dict)) for r in res.values())
    assert oaenv.clock.sleeps[:3] == [1.0, 2.0, 4.0]               # docs: "wait 1s, 2s, 4s"


def test_a_truncated_page_is_error_not_no_match(stub, monkeypatch):
    stub.add(work(1, "10.5555/a.1"))
    real = stub.__call__

    def short_page(method, url, *a):
        r = real(method, url, *a)
        body = json.loads(r.content)
        body["meta"]["count"] = 5
        return _raw(200, body, RL)
    monkeypatch.setitem(net._TRANSPORTS, "requests", short_page)
    res = oa.works_by_doi(["10.5555/a.1", "10.5555/a.2"])
    assert res["10.5555/a.1"].ok and res["10.5555/a.2"].kind is Kind.ERROR


def test_works_by_id_reports_dangling_ids_as_no_match(stub):
    stub.add(work(1, "10.5555/a.1"), work(2))
    res = oa.works_by_id(["W1", "https://openalex.org/W2", "W3", "nope"])
    assert res["W1"].ok and res["https://openalex.org/W2"].ok
    assert res["W3"].kind is Kind.NO_MATCH and "dangling" in res["W3"].detail
    assert res["nope"].kind is Kind.SKIPPED
    assert stub.list_calls()[0]["params"]["filter"] == "openalex:W1|W2|W3"


# ================================================================ referenced_works
def test_referenced_works_payload_is_a_list_of_dois_with_dangling_reported(stub):
    stub.add(work(10, "10.5555/seed.1", refs=[1, 2, 3, 4]), work(1, "10.5555/r.1"), work(2, "10.5555/r.2"),
             work(3))                                              # W4 dangling, W3 without a DOI
    out = oa.referenced_works("10.5555/SEED.1")
    assert out.ok and isinstance(out.payload, list) and isinstance(out.payload, oa.RefList)
    assert list(out.payload) == ["10.5555/r.1", "10.5555/r.2"]
    assert out.payload.dangling == ["W4"] and len(out.payload.no_doi) == 1
    assert out.payload.count == 4 and out.payload.work_id == "W10"
    assert stub.sent[0]["path"] == "/works/doi:10.5555/seed.1"       # the free singleton first
    assert "1 dangling" in out.detail


def test_a_count_of_0_is_unknown_never_an_empty_list(stub):
    stub.add(work(10, "10.1111/sms.70236", refs=[]))
    out = oa.referenced_works("10.1111/sms.70236")
    assert out.kind is Kind.NOT_AVAILABLE and out.payload is None
    assert "unknown" in out.detail and "V2-N8" in out.detail
    many = oa.referenced_works_many(["10.1111/sms.70236"])
    assert many["10.1111/sms.70236"].kind is Kind.NOT_AVAILABLE and many["10.1111/sms.70236"].payload is None


def test_a_404_html_singleton_is_no_match(stub):
    out = oa.referenced_works("10.5555/absent.1")
    assert out.kind is Kind.NO_MATCH and out.payload is None


def test_a_failed_resolve_fails_the_seed_rather_than_answering_a_partial_list(stub, monkeypatch):
    stub.add(work(10, "10.5555/seed.1", refs=[1, 2]), work(1, "10.5555/r.1"), work(2, "10.5555/r.2"))
    real = stub.__call__
    calls = {"n": 0}

    def fail_resolve(method, url, *a):
        if "openalex:" in unquote(url):
            calls["n"] += 1
            return _raw(500, {"error": "boom"}, {})
        return real(method, url, *a)
    monkeypatch.setitem(net._TRANSPORTS, "requests", fail_resolve)
    out = oa.referenced_works("10.5555/seed.1")
    assert out.kind is Kind.OUTAGE and out.payload is None and calls["n"] == 4
    many = oa.referenced_works_many(["10.5555/seed.1"])["10.5555/seed.1"]
    assert many.kind is Kind.OUTAGE and many.payload is None


# ================================================================ acceptance: the 15 V2 core papers
def _core15_stub(stub):
    seeds = json.loads((FIX / "oa_core15_seeds.json").read_text(encoding="utf-8"))
    resolved = json.loads((FIX / "oa_core15_resolved.json").read_text(encoding="utf-8"))
    stub.add(*seeds["results"], *resolved["results"])
    return seeds, resolved, json.loads((FIX / "core15_metrics.json").read_text(encoding="utf-8"))


def test_core15_openalex_coverage_median_is_1_as_audited(stub):
    seeds, resolved, metrics = _core15_stub(stub)
    dois = list(metrics["per_paper"])
    res = oa.referenced_works_many(dois)
    cov_ids, cov_doi, dangling = [], [], 0
    for d in dois:
        out, m = res[d], metrics["per_paper"][d]
        assert out.ok, (d, out)
        refs = out.payload
        n_ids = len(refs.works) + len(refs.dangling)
        with_doi = len(refs.works) - len(refs.no_doi)
        assert n_ids == m["n_openalex"] == refs.count
        assert len(refs.dangling) == m["n_openalex_dangling"]
        assert with_doi == m["n_openalex_with_doi"]
        assert d not in refs                                     # the seed's own DOI never appears
        cov_ids.append(n_ids / m["n_printed_used"])
        cov_doi.append(with_doi / m["n_printed_used"])
        dangling += len(refs.dangling)
    agg = metrics["aggregates"]
    assert statistics.median(cov_ids) == pytest.approx(agg["median_cov_oa"], abs=0.005) == pytest.approx(1.0, abs=0.005)
    assert statistics.median(cov_doi) == pytest.approx(agg["median_cov_oa_doi"], abs=0.01)
    assert dangling == agg["sum_dangling"] == 16
    # one seed batch and the shared resolve calls, all list calls under the URL budget
    calls = stub.list_calls()
    assert calls[0]["params"]["filter"].startswith("doi:") and len(calls) == 1 + -(-len(set(
        w for s in seeds["results"] for w in s["referenced_works"])) // 100)
    assert all(oa.url_bytes(c["url"]) <= oa.URL_SAFE_BYTES for c in calls)


# ================================================================ citing works
def test_citing_works_follows_the_cursor_to_the_end(stub):
    stub.add(work(10, "10.5555/seed.1"))
    stub.cites["W10"] = [work(100 + i, f"10.5555/c.{i}") for i in range(230)]
    pages = list(oa.citing_works("10.5555/seed.1"))
    assert [len(p.payload) for p in pages] == [100, 100, 30]
    assert all(p.ok and p.payload.count == 230 for p in pages)
    assert pages[-1].payload.next_cursor is None
    cursors = [c["params"]["cursor"] for c in stub.list_calls()]
    assert cursors == ["*", "c100", "c200"]
    assert stub.sent[0]["path"] == "/works/doi:10.5555/seed.1"


def test_citing_works_yields_a_failure_and_stops(stub):
    stub.cites["W10"] = [work(100 + i) for i in range(250)]
    it = oa.citing_works("W10")
    first = next(it)
    assert first.ok
    stub.script(4, 502)
    second = next(it)
    assert second.kind is Kind.OUTAGE and second.payload is None
    assert list(it) == []


def test_citing_works_for_an_unknown_doi_is_one_no_match(stub):
    assert [o.kind for o in oa.citing_works("10.5555/none.1")] == [Kind.NO_MATCH]


# ================================================================ the key
def test_the_key_goes_in_the_bearer_header_never_the_url_or_the_ledger(stub, oaenv):
    stub.add(work(10, "10.5555/seed.1", refs=[1]), work(1, "10.5555/r.1"))
    out = oa.referenced_works("10.5555/seed.1")
    assert out.ok
    for s in stub.sent:
        assert s["headers"].get("Authorization") == f"Bearer {SECRET}"
        assert SECRET not in s["url"] and "api_key" not in s["url"]
    text = oaenv.ledger_text()
    assert text and SECRET not in text and "Authorization" in text      # the header NAME only
    assert SECRET not in repr(out)


def test_no_key_sends_no_authorization_header(stub, monkeypatch):
    monkeypatch.delenv(oa.KEY_ENV)
    stub.add(work(1, "10.5555/a.1"))
    assert oa.works_by_doi(["10.5555/a.1"])["10.5555/a.1"].ok
    assert "Authorization" not in stub.sent[0]["headers"] and oa.key_status() == "key: absent"


def test_redact_scrubs_an_api_key_query_parameter_and_the_literal_key(monkeypatch):
    monkeypatch.setenv(oa.KEY_ENV, SECRET)
    url = f"https://content.openalex.org/works/W2741809807.pdf?api_key={SECRET}"
    out = ledger.redact(url)
    assert "api_key=REDACTED" in out and SECRET not in out
    assert SECRET not in ledger.redact(f"failed: Bearer {SECRET} rejected")


# ================================================================ budget and host rows
def test_remaining_0_defers_the_host_until_reset_and_stops_the_session(stub, oaenv):
    stub.headers = {**RL, "X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "5000"}
    stub.add(work(1, "10.5555/a.1"), work(2, "10.5555/a.2"))
    s = oa.Session()
    first = oa.works_by_doi(["10.5555/a.1"], session=s)["10.5555/a.1"]
    assert first.ok                                                # the call that spent the last credit
    assert s.aborted == "budget" and s.remaining == 0
    assert oaenv.state.deferred["api.openalex.org"] == pytest.approx(oaenv.clock.t + 5000, abs=1)
    n = len(stub.sent)
    second = oa.works_by_doi(["10.5555/a.2"], session=s)["10.5555/a.2"]
    assert second.kind is Kind.DEFERRED and len(stub.sent) == n   # nothing sent
    assert second.retry_after == pytest.approx(5000, abs=5)


def test_a_429_is_deferred_not_retried_and_never_refuses_the_host(stub, oaenv):
    stub.script(1, 429, headers={"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "7200"})
    s = oa.Session()
    out = oa.works_by_doi(["10.5555/a.1"], session=s)["10.5555/a.1"]
    assert out.kind is Kind.DEFERRED and out.status == 429 and len(stub.sent) == 1
    assert oaenv.clock.sleeps == []                                # no backoff into a spent budget
    assert not oaenv.state.is_refused("api.openalex.org")
    assert oaenv.state.deferred["api.openalex.org"] == pytest.approx(oaenv.clock.t + 7200, abs=1)
    assert s.aborted == "budget"


def test_missing_rate_limit_headers_are_tolerated(stub):
    stub.headers = {}
    stub.add(work(1, "10.5555/a.1"))
    s = oa.Session()
    assert oa.works_by_doi(["10.5555/a.1"], session=s)["10.5555/a.1"].ok
    assert s.remaining is None and s.aborted is None
    assert oa.rate_limit(CaseInsensitiveDict({"X-RateLimit-Remaining": "n/a"})) == {}


def test_a_401_is_config_and_stops_the_session(stub):
    stub.script(1, 401, body={"error": "Unauthorized", "message": "invalid api key"})
    s = oa.Session()
    out = oa.works_by_doi(["10.5555/a.1"], session=s)["10.5555/a.1"]
    assert out.kind is Kind.CONFIG and s.aborted == "config"


def test_breaker_and_run_budget(stub):
    stub.script(12, 500)
    s = oa.Session(breaker=3)
    for i in range(3):
        oa.works_by_doi([f"10.5555/b.{i}"], session=s)
    assert s.aborted == "breaker"
    n = len(stub.sent)
    assert oa.works_by_doi(["10.5555/b.9"], session=s)["10.5555/b.9"].kind is Kind.DEFERRED
    assert len(stub.sent) == n
    stub.queue.clear()
    stub.add(work(1, "10.5555/a.1"))
    s2 = oa.Session(budget=1)
    assert oa.works_by_doi(["10.5555/a.1"], session=s2)["10.5555/a.1"].ok
    assert oa.works_by_doi(["10.5555/a.1"], session=s2)["10.5555/a.1"].kind is Kind.DEFERRED
    assert s2.aborted == "budget" and s2.attempts == 1


def test_host_rows_are_registered_per_call_not_at_import(stub):
    import importlib
    hosts.reset()
    importlib.reload(oa)
    assert hosts.policy("api.openalex.org") == hosts._rows()[[r.host for r in hosts._rows()].index("api.openalex.org")]
    assert not hosts.policy("content.openalex.org").known
    oa.works_by_doi(["10.5555/a.1"])
    api, content = hosts.policy("api.openalex.org"), hosts.policy("content.openalex.org")
    assert 429 not in api.retry.statuses and 429 not in api.refuse_host_statuses
    assert api.status_kinds[429] is Kind.DEFERRED and api.status_kinds[401] is Kind.CONFIG
    assert content.known and content.redirect_allow == () and 429 not in content.retry.statuses


def test_settings_validate_the_openalex_block(oaenv):
    oaenv.write_config(openalex={"max_requests_per_run": None, "breaker": 5, "content_max_per_run": 2})
    st = oa.settings()
    assert st.max_requests_per_run is None and st.breaker == 5 and st.content_max_per_run == 2
    oaenv.write_config(openalex={"content_max_per_run": -1})
    with pytest.raises(config.ConfigError):
        oa.settings()


# ================================================================ content_pdf (offline only)
def _paid_cfg(oaenv, sources=("unpaywall", "openalex_content"), cap=1):
    oaenv.write_config(openalex={"content_max_per_run": cap},
                       projects={"research_a": {"lib_dir": "lit", "sources": list(sources)}})
    return config.load()


def test_content_pdf_is_skipped_unless_allowed_sourced_and_capped(stub, oaenv):
    cfg = _paid_cfg(oaenv, cap=1)
    s = oa.Session(cfg=cfg)
    unpaid = oa.content_pdf("W1", project="research_a", cfg=cfg, session=s)            # everything but allow_paid
    assert unpaid.kind is Kind.SKIPPED and "allow_paid" in unpaid.detail and s.content_calls == 0
    assert oa.content_pdf("W1", allow_paid=True, session=s).kind is Kind.SKIPPED       # no project
    cfg_no = _paid_cfg(oaenv, sources=("unpaywall",), cap=1)
    assert oa.content_pdf("W1", allow_paid=True, project="research_a", cfg=cfg_no,
                          session=oa.Session(cfg=cfg_no)).kind is Kind.SKIPPED        # not a DEC-31 source
    assert stub.sent == []
    cfg = _paid_cfg(oaenv, cap=1)
    s = oa.Session(cfg=cfg)
    out = oa.content_pdf("W1", allow_paid=True, project="research_a", cfg=cfg, session=s)
    assert out.ok and out.payload.content.startswith(b"%PDF")
    assert stub.sent[0]["host"] == "content.openalex.org" and stub.sent[0]["path"] == "/works/W1.pdf"
    assert stub.sent[0]["headers"]["Authorization"] == f"Bearer {SECRET}" and SECRET not in stub.sent[0]["url"]
    again = oa.content_pdf("W2", allow_paid=True, project="research_a", cfg=cfg, session=s)
    assert again.kind is Kind.SKIPPED and "cap" in again.detail and len(stub.sent) == 1


def test_content_pdf_default_cap_is_0_and_needs_a_key(stub, oaenv, monkeypatch):
    oaenv.write_config(projects={"research_a": {"lib_dir": "lit", "sources": ["openalex_content"]}})
    cfg = config.load()
    out = oa.content_pdf("W1", allow_paid=True, project="research_a", cfg=cfg, session=oa.Session(cfg=cfg))
    assert out.kind is Kind.SKIPPED and "cap" in out.detail
    cfg = _paid_cfg(oaenv, cap=5)
    monkeypatch.delenv(oa.KEY_ENV)
    out = oa.content_pdf("W1", allow_paid=True, project="research_a", cfg=cfg, session=oa.Session(cfg=cfg))
    assert out.kind is Kind.SKIPPED and oa.KEY_ENV in out.detail and stub.sent == []


def test_content_pdf_html_is_refused_not_ok(stub, oaenv):
    cfg = _paid_cfg(oaenv, cap=2)
    stub.script(1, 200, headers={"Content-Type": "text/html"}, body=b"<html>login</html>")
    out = oa.content_pdf("W1", allow_paid=True, project="research_a", cfg=cfg, session=oa.Session(cfg=cfg))
    assert out.kind is Kind.REFUSED


def test_status_cli_never_prints_the_key(capsys, oaenv):
    assert oa.main(["--status"]) == 0
    out = capsys.readouterr().out
    assert "key: present" in out and SECRET not in out
