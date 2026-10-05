"""The Unpaywall stage on litpipe.net (dispatch W2-B; refactor scope 3.2). Offline.

Every test drives the real stage through the real litpipe.net (host table, identity, refusal and
403 rules, ledger, redaction) against the FakeState of `net_env`; only the transport is replaced
by `Web`, which answers from canned replies (tests/fixtures/W2-B: live probes of 2026-09-30 and
the V3 audit, trimmed) and fails the test on any URL it was not given. PDFs are generated with
pymupdf so the identity check reads real text.
"""
import csv
import json
import os
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

import pytest
from requests.structures import CaseInsensitiveDict

import unpaywall_fetch_v2 as U
from litpipe import hosts, net
from litpipe.outcomes import Kind

FIX = Path(__file__).parent / "fixtures" / "W2-B"
TEST_EMAIL = "tester@litpipe-test.org"          # the address net_env sets
RA = {"10.48550": "DataCite", "10.3305": "mEDRA", "10.2903": "OP", "10.5281": "DataCite"}
NOT_EXIST = {"10.99999"}


def fixture_bytes(name):
    return (FIX / name).read_bytes()


def fixture_json(name):
    return json.loads((FIX / name).read_text(encoding="utf-8"))


def make_pdf(first_page_text, pages=3, pad=True):
    """A real PDF: page 1 carries `first_page_text`; padded past MIN_PDF_BYTES via metadata."""
    import fitz
    doc = fitz.open()
    filler = ("Exercise in the heat raises core temperature and heart rate; acclimation lowers "
              "both at a given work rate. ") * 10
    for i in range(pages):
        page = doc.new_page()
        page.insert_textbox(fitz.Rect(40, 40, 560, 800), first_page_text if i == 0 else filler,
                            fontsize=8)
    if pad:
        doc.set_metadata({"subject": "pad " * 3000})
    data = doc.tobytes()
    doc.close()
    return data


def article_pdf(doi, title="Heat acclimation and athletic performance"):
    return make_pdf(f"Journal of Testing 2020\n{title}\nA. Author, B. Author\n"
                    f"https://doi.org/{doi}\n© 2020 The Authors. Published by Test Press\n")


class Web:
    """litpipe.net transport double. Routes are keyed by URL without the query string; a value is
    a list of (status, headers, body) replies (the last one repeats) or a callable(url) returning
    one. Every request that reaches the transport is recorded in `sent`."""

    def __init__(self):
        self.routes = {}
        self.sent = []
        self.ra = dict(RA)

    def add(self, url, *replies):
        self.routes[url] = [(r if isinstance(r, tuple) else (200, {}, r)) for r in replies]
        return self

    def to(self, host):
        return [s for s in self.sent if urlsplit(s[1]).hostname == host]

    def _doira(self, url):
        prefixes = unquote(urlsplit(url).path[len("/doiRA/"):]).split(",")
        out = []
        for p in prefixes:
            if p in NOT_EXIST:
                out.append({"DOI": p, "status": "DOI does not exist"})
            else:
                out.append({"DOI": p, "RA": self.ra.get(p, "Crossref")})
        return 200, {"Content-Type": "application/json;charset=UTF-8"}, json.dumps(out).encode()

    def __call__(self, method, url, headers, body, timeout, max_bytes):
        self.sent.append((method, url, dict(headers)))
        key = url.split("?", 1)[0]
        if key.startswith("https://doi.org/doiRA/") and key not in self.routes:
            status, hdrs, data = self._doira(url)
        else:
            seq = self.routes.get(key)
            if seq is None:
                raise AssertionError(f"unrouted request (no live network in tests): {method} {url}")
            r = seq.pop(0) if len(seq) > 1 else seq[0]
            status, hdrs, data = r(url) if callable(r) else r
        if isinstance(data, str):
            data = data.encode("utf-8")
        cap = net._cap(status, max_bytes)
        trunc = cap is not None and len(data) > cap
        return net._Raw(status, CaseInsensitiveDict(hdrs), data, data[:65536], len(data), trunc)


@pytest.fixture
def web(net_env, monkeypatch):
    w = Web()
    monkeypatch.setitem(net._TRANSPORTS, "requests", w)
    monkeypatch.setitem(net._TRANSPORTS, "urllib", w)
    w.env = net_env
    return w


@pytest.fixture
def ris_calls(monkeypatch):
    calls = []

    def fake_emit(doi, pdf_path, overwrite=False):
        calls.append((doi, pdf_path))
        return ("OK", os.path.splitext(pdf_path)[0] + ".ris")

    monkeypatch.setattr(U._R, "emit_ris_for_pdf", fake_emit)
    return calls


def upw_url(doi):
    return f"{U.UNPAYWALL}/{doi}"


def upw_record(doi, locations, title="Heat acclimation and athletic performance", year=2020,
               z_authors=None, is_oa=True):
    return {"doi": doi, "is_oa": is_oa, "oa_status": "gold" if is_oa else "closed", "title": title,
            "year": year, "z_authors": z_authors or [{"author_position": "first",
                                                      "raw_author_name": "Alex Author"}],
            "oa_locations": locations,
            "best_oa_location": locations[0] if locations else None}


def loc(url_for_pdf=None, url=None, host_type="publisher", version="publishedVersion"):
    return {"host_type": host_type, "version": version, "url_for_pdf": url_for_pdf,
            "url": url or url_for_pdf}


JSON = {"Content-Type": "application/json"}
HTML = {"Content-Type": "text/html; charset=utf-8"}
PDF = {"Content-Type": "application/pdf"}


def add_upw(w, doi, record):
    w.add(upw_url(doi), (200, JSON, json.dumps(record)))


def run_stage(tmp_path, rows, **kw):
    """Write a triage CSV, run the stage, return (result, report rows by doi, library dir)."""
    tri = tmp_path / "triage.csv"
    with open(tri, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["doi", "title", "authors", "year", "citation_count"])
        w.writeheader()
        for r in rows:
            w.writerow({"citation_count": "0", "authors": "Author A", "year": "2020",
                        "title": "Heat acclimation and athletic performance", **r})
    lib = tmp_path / "lib"
    rep = tmp_path / "report.csv"
    kw.setdefault("no_write_ris", True)
    res = U.run(top_n=100, base_dir=str(tmp_path), triage=str(tri), lib_dir=str(lib),
                report=str(rep), **kw)
    out = {}
    if rep.exists():
        with open(rep, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                out[r["doi"]] = r
    return res, out, lib


# ------------------------------------------------------------------ registration agency routing
def test_a_datacite_doi_makes_no_unpaywall_request(web, tmp_path):
    res, rep, _ = run_stage(tmp_path, [{"doi": "10.48550/arxiv.2402.05741"}])
    row = rep["10.48550/arxiv.2402.05741"]
    assert row["outcome"] == "NOT_AT_RA" and row["route"] == "ra" and row["ra"] == "DataCite"
    assert row["error"].startswith("NOT_IN_UNPAYWALL")          # legacy readers: NO_MATCH, terminal
    assert web.to("api.unpaywall.org") == []
    assert len(web.to("doi.org")) == 1


@pytest.mark.parametrize("doi, ra", [("10.3305/nh.2015.32.1.8883", "mEDRA"),
                                     ("10.2903/j.efsa.2024.9045", "OP"),
                                     ("10.5281/zenodo.123456", "DataCite")])
def test_every_non_crossref_agency_skips_unpaywall(web, tmp_path, doi, ra):
    _, rep, _ = run_stage(tmp_path, [{"doi": doi}])
    assert rep[doi]["outcome"] == "NOT_AT_RA" and rep[doi]["ra"] == ra
    assert web.to("api.unpaywall.org") == []


def test_ra_is_one_batched_call_and_cached_per_prefix(web, tmp_path):
    for d in ("10.1152/a.1", "10.1152/a.2", "10.1186/b.1"):
        web.add(upw_url(d), (404, HTML, fixture_bytes("unpaywall_404.html")))
    rows = [{"doi": "10.1152/a.1"}, {"doi": "10.1152/a.2"}, {"doi": "10.1186/b.1"},
            {"doi": "10.48550/arxiv.1"}]
    run_stage(tmp_path, rows)
    ra_calls = web.to("doi.org")
    assert len(ra_calls) == 1
    asked = unquote(urlsplit(ra_calls[0][1]).path).rsplit("/", 1)[-1].split(",")
    assert sorted(asked) == ["10.1152", "10.1186", "10.48550"]
    assert web.env.state.kv[(U.RA_KV_NS, "10.48550")] == "DataCite"
    web.sent.clear()
    run_stage(tmp_path, rows)                                    # second run: cache, no doi.org call
    assert web.to("doi.org") == []
    assert len(web.to("api.unpaywall.org")) == 3


def test_ra_lookup_failure_falls_through_to_unpaywall(web, tmp_path):
    web.add("https://doi.org/doiRA/10.1152", (503, {}, b"down"))
    web.add(upw_url("10.1152/x.1"), (404, HTML, fixture_bytes("unpaywall_404.html")))
    _, rep, _ = run_stage(tmp_path, [{"doi": "10.1152/x.1"}])
    assert len(web.to("api.unpaywall.org")) == 1                 # failed lookup never blocks the row
    assert rep["10.1152/x.1"]["ra"] == "" and rep["10.1152/x.1"]["outcome"] == "NO_MATCH"
    assert (U.RA_KV_NS, "10.1152") not in web.env.state.kv      # a failure is not cached


def test_ra_does_not_exist_is_not_cached_and_falls_through(web, tmp_path):
    web.add(upw_url("10.99999/nope"), (404, HTML, fixture_bytes("unpaywall_404.html")))
    _, rep, _ = run_stage(tmp_path, [{"doi": "10.99999/nope"}])
    assert rep["10.99999/nope"]["outcome"] == "NO_MATCH"
    assert (U.RA_KV_NS, "10.99999") not in web.env.state.kv


def test_live_doira_fixture_parses():
    """The 2026-09-30 /doiRA/ answer: RA names and a {"status": "DOI does not exist"} entry."""
    body = fixture_json("doira_batch.json")
    by = {i["DOI"]: i.get("RA") or i.get("status") for i in body}
    assert by["10.48550"] == "DataCite" and by["10.1152"] == "Crossref" and by["10.3305"] == "mEDRA"
    assert by["10.99999"] == "DOI does not exist"


# ------------------------------------------------------------------ Unpaywall status handling
def test_unpaywall_404_is_no_match(web, tmp_path):
    web.add(upw_url("10.1152/litpipe.probe.nonexistent.2026"),
            (404, HTML, fixture_bytes("unpaywall_404.html")))
    _, rep, _ = run_stage(tmp_path, [{"doi": "10.1152/litpipe.probe.nonexistent.2026"}])
    row = rep["10.1152/litpipe.probe.nonexistent.2026"]
    assert row["outcome"] == "NO_MATCH" and row["error"] == "HTTP 404" and row["first_status"] == "404"
    from litpipe.outcomes import from_legacy
    assert from_legacy(row["error"], "unpaywall") is Kind.NO_MATCH   # legacy readers agree


def test_unpaywall_422_aborts_the_stage_as_config(web, tmp_path):
    web.add(upw_url("10.1152/a.1"), (422, JSON, fixture_bytes("unpaywall_422.json")))
    res, rep, _ = run_stage(tmp_path, [{"doi": "10.1152/a.1"}, {"doi": "10.1152/a.2"}])
    assert res["exit_code"] == U.EXIT_CONFIG and res["aborted"]
    assert rep["10.1152/a.1"]["outcome"] == "CONFIG" and rep["10.1152/a.1"]["error"] == "HTTP 422"
    assert "10.1152/a.2" not in rep                              # nothing after the abort
    assert len(web.to("api.unpaywall.org")) == 1


def test_main_exits_2_on_config(web, tmp_path, monkeypatch):
    web.add(upw_url("10.1152/a.1"), (410, JSON, b'{"error":"gone"}'))
    tri = tmp_path / "t.csv"
    tri.write_text("doi,title,authors,year,citation_count\n10.1152/a.1,T,A B,2020,0\n", encoding="utf-8")
    monkeypatch.setattr("sys.argv", ["unpaywall_fetch_v2.py", "--triage", str(tri), "--lib-dir",
                                     str(tmp_path / "lib"), "--report", str(tmp_path / "r.csv")])
    with pytest.raises(SystemExit) as e:
        U.main()
    assert e.value.code == 2


@pytest.mark.parametrize("value", [None, "", "   ", "you@example.com", "someone@example.org",
                                   "x@host.test"])
def test_missing_or_placeholder_email_is_config_before_any_send(web, tmp_path, monkeypatch, value):
    if value is None:
        monkeypatch.delenv("LITPIPE_EMAIL", raising=False)
    else:
        monkeypatch.setenv("LITPIPE_EMAIL", value)
    res, rep, _ = run_stage(tmp_path, [{"doi": "10.1152/a.1"}, {"doi": "10.1152/a.2"}])
    assert res["exit_code"] == U.EXIT_CONFIG
    assert rep["10.1152/a.1"]["outcome"] == "CONFIG"
    assert web.to("api.unpaywall.org") == []                     # never sent: no email=None, no placeholder


def test_email_is_injected_once_by_net_never_by_the_stage(web, tmp_path):
    add_upw(web, "10.1152/a.1", upw_record("10.1152/a.1", [], is_oa=False))
    run_stage(tmp_path, [{"doi": "10.1152/a.1"}])
    (_, url, headers), = web.to("api.unpaywall.org")
    q = parse_qs(urlsplit(url).query)
    assert q == {"email": [TEST_EMAIL]}                          # exactly one, net's
    for _, u, h in web.sent:
        assert "email=None" not in u and "mailto:None" not in json.dumps(h)
        if urlsplit(u).hostname != "api.unpaywall.org":
            assert TEST_EMAIL not in u                           # the email goes to Unpaywall only


def test_unpaywall_url_is_normalised_and_path_encoded(web, tmp_path):
    """V3-N3 / I47: normalise first (a `#sa3` fragment is cut, never encoded into a 404), then
    encode per DOI Handbook 4.7 (SICI angle brackets become %3C / %3E)."""
    sici = "10.1002/(sici)1097-4636(199601)30:1<1::aid-jbm1>3.0.co;2-x"
    enc = "10.1002/(sici)1097-4636(199601)30:1%3C1::aid-jbm1%3E3.0.co;2-x"
    web.add(f"{U.UNPAYWALL}/{enc}", (404, HTML, fixture_bytes("unpaywall_404.html")))
    web.add(f"{U.UNPAYWALL}/10.1056/nejmc1113675", (404, HTML, fixture_bytes("unpaywall_404.html")))
    run_stage(tmp_path, [{"doi": sici}, {"doi": "10.1056/NEJMc1113675#sa3"}])
    sent = [u.split("?")[0] for _, u, _ in web.to("api.unpaywall.org")]
    assert sent == [f"{U.UNPAYWALL}/{enc}", f"{U.UNPAYWALL}/10.1056/nejmc1113675"]


def test_no_report_line_contains_the_email(web, tmp_path):
    leak = f"ConnectionError: HTTPSConnectionPool(host='api.unpaywall.org'): Max retries exceeded " \
           f"with url: /v2/10.1152/a.1?email={TEST_EMAIL.replace('@', '%40')}"

    def boom(method, url, headers, body, timeout, max_bytes):
        web.sent.append((method, url, dict(headers)))
        if "api.unpaywall.org/v2/10.1152/a.1" in url:
            return net._Raw(error=leak)
        return Web.__call__(web, method, url, headers, body, timeout, max_bytes)

    net._TRANSPORTS["requests"] = boom
    # a candidate whose URL carries the address (a tracking link) and wins: winning_url, attempts
    tracked = f"https://x.example.org/a2.pdf?src=upw&email={TEST_EMAIL.replace('@', '%40')}"
    add_upw(web, "10.1152/a.2", upw_record("10.1152/a.2", [loc(tracked)]))
    web.add("https://x.example.org/a2.pdf", (200, PDF, article_pdf("10.1152/a.2")))
    res, rep, _ = run_stage(tmp_path, [{"doi": "10.1152/a.1"}, {"doi": "10.1152/a.2"}])
    text = (tmp_path / "report.csv").read_text(encoding="utf-8")
    assert rep["10.1152/a.1"]["outcome"] == "TRANSPORT"
    assert rep["10.1152/a.2"]["downloaded"] == "True" and rep["10.1152/a.2"]["winning_url"]
    ids = next((tmp_path / "lib").glob("*.identity.json")).read_text(encoding="utf-8")
    text += ids                                                  # the sidecar persists the URL too
    low = text.lower()
    for needle in ("email=", "mailto:", TEST_EMAIL, TEST_EMAIL.replace("@", "%40"),
                   TEST_EMAIL.replace("@", "%2540")):
        assert needle.lower() not in low, needle


# ------------------------------------------------------------------ candidates and order
def test_candidate_order_repository_default_and_publisher_first():
    rec = {"oa_locations": [loc("https://pub.example.net/a.pdf"),
                            loc("https://repo.example.edu/a.pdf", host_type="repository",
                                version="acceptedVersion"),
                            loc(None, "https://repo.example.edu/landing", host_type="repository",
                                version="submittedVersion")]}
    assert [c[0] for c in U.candidate_urls(rec)] == ["repository", "repository", "publisher"]
    assert U.candidate_urls(rec)[0][2] == "https://repo.example.edu/a.pdf"
    assert [c[0] for c in U.candidate_urls(rec, "publisher")][0] == "publisher"
    with pytest.raises(ValueError):
        U.candidate_urls(rec, "random")


def test_candidate_order_flag_reaches_the_downloads(web, tmp_path):
    rec = upw_record("10.1152/a.1", [loc("https://pub.example.net/a.pdf"),
                                     loc("https://repo.example.edu/a.pdf", host_type="repository")])
    add_upw(web, "10.1152/a.1", rec)
    web.add("https://pub.example.net/a.pdf", (200, PDF, article_pdf("10.1152/a.1")))
    web.add("https://repo.example.edu/a.pdf", (200, PDF, article_pdf("10.1152/a.1")))
    _, rep, _ = run_stage(tmp_path, [{"doi": "10.1152/a.1"}], candidate_order="publisher")
    downloads = [s for s in web.sent if "example" in s[1]]
    assert urlsplit(downloads[0][1]).hostname == "pub.example.net"
    assert rep["10.1152/a.1"]["route"] == "publisher"


def test_cli_accepts_candidate_order(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["unpaywall_fetch_v2.py", "--help"])
    with pytest.raises(SystemExit) as e:
        U.main()
    assert e.value.code == 0 and "--candidate-order" in capsys.readouterr().out
    monkeypatch.setattr("sys.argv", ["unpaywall_fetch_v2.py", "--candidate-order", "random"])
    with pytest.raises(SystemExit) as e:
        U.main()
    assert e.value.code == 2


# ------------------------------------------------------------------ 403s, walls and refusals
@pytest.mark.parametrize("threshold", [1, 2])
def test_configured_consecutive_403s_refuse_the_host_and_the_next_row_sends_nothing(
        web, tmp_path, threshold):
    host = "blocked.example.net"
    hosts.register(hosts.HostPolicy(host, min_interval_s=0.0, identity="download",
                                    redirect_allow=(hosts.ANY_HOST,),
                                    refuse_after_consecutive_403=threshold))
    rows = []
    for n in range(threshold + 1):
        d = f"10.1152/b.{n}"
        add_upw(web, d, upw_record(d, [loc(f"https://{host}/{n}.pdf")]))
        web.add(f"https://{host}/{n}.pdf", (403, HTML, fixture_bytes("mdpi_403_access_denied.html")))
        rows.append({"doi": d})
    _, rep, _ = run_stage(tmp_path, rows)
    assert len(web.to(host)) == threshold                        # the row after the threshold: nothing sent
    last = rep[f"10.1152/b.{threshold}"]
    assert last["outcome"] == "REFUSED" and last["error"] == "HOST_REFUSED" and last["first_status"] == ""
    for n in range(threshold):
        assert rep[f"10.1152/b.{n}"]["outcome"] == "REFUSED"
        assert rep[f"10.1152/b.{n}"]["first_status"] == "403"
    assert host in web.env.state.refused
    import sweep
    v = sweep.Verdict("unpaywall", Kind(last["outcome"]), last["error"], download_host=True)
    assert sweep.classify([v]) == ("OA_BLOCKED", "unpaywall: HOST_REFUSED")   # retry_later + worklist


def test_mdpi_live_403_refuses_www_mdpi_com_and_skips_prohibited_pmc_pages(web, tmp_path):
    rec = fixture_json("unpaywall_mdpi_record.json")
    doi = rec["doi"]
    add_upw(web, doi, rec)
    pdf_url = next(l["url_for_pdf"] for l in rec["oa_locations"] if l["host_type"] == "publisher")
    web.add(pdf_url.split("?")[0], (403, {"Content-Type": "text/html"},
                                    fixture_bytes("mdpi_403_access_denied.html")))
    web.add("https://doaj.org/article/7146995628f6430cbdef43facb38df2b",
            (200, HTML, b"<html><head><title>DOAJ</title></head><body>record page</body></html>"))
    _, rep, _ = run_stage(tmp_path, [{"doi": doi}])
    row = rep[doi]
    assert "www.mdpi.com" in web.env.state.refused
    assert not any("ncbi.nlm.nih.gov/pmc/articles" in s[1] for s in web.sent)   # never automated
    assert "PROHIBITED" in row["attempts"]
    assert row["outcome"] == "REFUSED" and row["first_status"] == "403"


def test_interstitial_page_refuses_the_host_for_the_run(web, tmp_path):
    host = "journal.example.com"
    rows = []
    for n in range(2):
        d = f"10.1186/c.{n}"
        add_upw(web, d, upw_record(d, [loc(None, f"https://{host}/articles/{n}")]))
        web.add(f"https://{host}/articles/{n}", (200, HTML, fixture_bytes("bmc_client_challenge.html")))
        rows.append({"doi": d})
    _, rep, _ = run_stage(tmp_path, rows)
    assert len(web.to(host)) == 1
    assert host in web.env.state.refused and "Client Challenge" in web.env.state.refused[host][0]
    assert rep["10.1186/c.0"]["outcome"] == "REFUSED" and "interstitial" in rep["10.1186/c.0"]["detail"]
    assert rep["10.1186/c.1"]["error"] == "HOST_REFUSED"


def test_identical_size_non_pdf_body_for_two_dois_refuses_the_host(web, tmp_path):
    host = "canned.example.org"
    page = b"<html><body>" + b"x" * 5000 + b"</body></html>"          # same bytes, no wall phrase
    rows = []
    for n in range(3):
        d = f"10.1152/d.{n}"
        add_upw(web, d, upw_record(d, [loc(f"https://{host}/{n}.pdf")]))
        web.add(f"https://{host}/{n}.pdf", (200, HTML, page))
        rows.append({"doi": d})
    _, rep, _ = run_stage(tmp_path, rows)
    assert len(web.to(host)) == 2                                # the third row sends nothing
    assert "identical" in web.env.state.refused[host][0]
    assert rep["10.1152/d.2"]["error"] == "HOST_REFUSED"


def test_same_size_landing_pages_that_name_their_dois_are_not_a_signature(web, tmp_path):
    """Two real landing pages (each names its own DOI) can share a byte count; a canned page
    names neither."""
    host = "repo.example.edu"
    rows = []
    for n in range(3):
        d = f"10.1152/z.{n}"
        page = f"<html><body>record for doi {d}: see the publisher</body></html>".encode()
        add_upw(web, d, upw_record(d, [loc(None, f"https://{host}/rec/{n}")]))
        web.add(f"https://{host}/rec/{n}", (200, HTML, page))
        rows.append({"doi": d})
    run_stage(tmp_path, rows)
    assert len(web.to(host)) == 3 and host not in web.env.state.refused


def test_same_size_body_for_the_same_doi_is_not_a_signature(web, tmp_path):
    host = "canned.example.org"
    page = b"<html><body>" + b"y" * 3000 + b"</body></html>"
    add_upw(web, "10.1152/e.1", upw_record("10.1152/e.1", [loc(f"https://{host}/1.pdf"),
                                                           loc(f"https://{host}/2.pdf")]))
    web.add(f"https://{host}/1.pdf", (200, HTML, page))
    web.add(f"https://{host}/2.pdf", (200, HTML, page))
    run_stage(tmp_path, [{"doi": "10.1152/e.1"}])
    assert host not in web.env.state.refused


def test_landing_page_is_followed_to_its_citation_pdf_url(web, tmp_path):
    d = "10.3389/f.1"
    add_upw(web, d, upw_record(d, [loc(None, "https://www.frontiers-like.org/articles/f1/full")]))
    web.add("https://www.frontiers-like.org/articles/f1/full",
            (200, HTML, b'<html><head><meta name="citation_pdf_url" content="/articles/f1/pdf">'
                        b'<title>Article</title></head><body>full text</body></html>'))
    web.add("https://www.frontiers-like.org/articles/f1/pdf", (200, PDF, article_pdf(d)))
    _, rep, lib = run_stage(tmp_path, [{"doi": d}])
    row = rep[d]
    assert row["downloaded"] == "True" and row["route"] == "html-fallback" and row["outcome"] == "OK"
    assert (lib / row["filename"]).exists()


def test_doi_org_landing_url_goes_through_the_handle_api(web, tmp_path):
    rec = fixture_json("unpaywall_bmc_record.json")
    doi = rec["doi"]
    add_upw(web, doi, rec)
    web.add(f"https://doi.org/api/handles/{doi}", (200, JSON, json.dumps(fixture_json("doi_handle_bmc.json"))))
    landing = "https://cardiab.biomedcentral.com/articles/" + doi
    web.add(landing, (200, HTML, fixture_bytes("bmc_client_challenge.html")))
    web.add("https://doaj.org/article/8d15d09ea0264bf28fb0af888e11e986",
            (200, HTML, b"<html><head><title>DOAJ</title></head><body>record</body></html>"))
    _, rep, _ = run_stage(tmp_path, [{"doi": doi}])
    assert not any(s[1].startswith("https://doi.org/10.") for s in web.sent)   # never the redirecting URL
    assert any(s[1].startswith(landing) for s in web.sent)
    assert "cardiab.biomedcentral.com" in web.env.state.refused              # today's challenge page
    assert rep[doi]["outcome"] == "REFUSED"


def test_html_answer_to_a_pdf_url_without_links_is_refused_row_level(web, tmp_path):
    d = "10.1152/g.1"
    add_upw(web, d, upw_record(d, [loc("https://viewer.example.org/g1.pdf")]))
    web.add("https://viewer.example.org/g1.pdf", (200, HTML, b"<html><body>js viewer</body></html>"))
    _, rep, _ = run_stage(tmp_path, [{"doi": d}])
    assert rep[d]["error"] == "HTML" and rep[d]["outcome"] == "REFUSED"
    assert "viewer.example.org" not in web.env.state.refused     # one plain HTML page is not a wall


def test_dead_oa_link_is_not_available_and_transport_is_transient(web, tmp_path):
    add_upw(web, "10.1152/h.1", upw_record("10.1152/h.1", [loc("https://gone.example.org/h1.pdf")]))
    web.add("https://gone.example.org/h1.pdf", (404, HTML, b"<html>not found</html>"))
    add_upw(web, "10.1152/h.2", upw_record("10.1152/h.2", [loc("https://flaky.example.org/h2.pdf")]))
    real = net._TRANSPORTS["requests"]

    def flaky(method, url, headers, body, timeout, max_bytes):
        if "flaky.example.org" in url:
            web.sent.append((method, url, dict(headers)))
            return net._Raw(error="ConnectionError: reset by peer")
        return real(method, url, headers, body, timeout, max_bytes)

    net._TRANSPORTS["requests"] = flaky
    _, rep, _ = run_stage(tmp_path, [{"doi": "10.1152/h.1"}, {"doi": "10.1152/h.2"}])
    assert rep["10.1152/h.1"]["outcome"] == "NOT_AVAILABLE" and rep["10.1152/h.1"]["error"].startswith("HTTP_404")
    assert rep["10.1152/h.2"]["outcome"] == "TRANSPORT"


def test_too_small_and_boilerplate_never_reach_the_library(web, tmp_path, monkeypatch):
    add_upw(web, "10.1152/i.1", upw_record("10.1152/i.1", [loc("https://a.example.org/i1.pdf")]))
    web.add("https://a.example.org/i1.pdf", (200, PDF, b"%PDF-1.4 tiny"))
    bp = article_pdf("10.1152/i.2")
    import hashlib
    monkeypatch.setitem(U.KNOWN_BOILERPLATE_MD5, hashlib.md5(bp).hexdigest(), "test_template")
    add_upw(web, "10.1152/i.2", upw_record("10.1152/i.2", [loc("https://b.example.org/i2.pdf")]))
    web.add("https://b.example.org/i2.pdf", (200, PDF, bp))
    _, rep, lib = run_stage(tmp_path, [{"doi": "10.1152/i.1"}, {"doi": "10.1152/i.2"}])
    assert rep["10.1152/i.1"]["error"] == "TOO_SMALL" and rep["10.1152/i.2"]["error"] == "BOILERPLATE"
    assert list(lib.iterdir()) == []


# ------------------------------------------------------------------ identity replaces the quarantine
def _snapshot(lib):
    return {p.name: p.read_bytes() for p in lib.iterdir()} if lib.exists() else {}


def test_right_paper_downloads_with_identity_sidecar_and_ris(web, tmp_path, ris_calls):
    d = "10.1152/j.1"
    add_upw(web, d, upw_record(d, [loc("https://j.example.org/j1.pdf")]))
    web.add("https://j.example.org/j1.pdf", (200, PDF, article_pdf(d)))
    _, rep, lib = run_stage(tmp_path, [{"doi": d, "authors": "Periard JD; Casa DJ"}],
                            no_write_ris=False)
    row = rep[d]
    assert row["downloaded"] == "True" and row["identity"] == "OK" and row["outcome"] == "OK"
    pdf = lib / row["filename"]
    ids = json.loads((lib / (pdf.name[:-4] + ".identity.json")).read_text(encoding="utf-8"))
    assert ids["queue_doi"] == d and ids["identity"] == "OK" and ids["pdf"] == pdf.name
    assert ids["doc_kind"] == "VOR" and ids["source"] == "unpaywall"
    assert ris_calls == [(d, str(pdf))]


def test_wrong_paper_is_flagged_kept_in_place_and_nothing_moves(web, tmp_path, ris_calls):
    lib = tmp_path / "lib"
    lib.mkdir()
    (lib / "2019_Other_KeptPaper.pdf").write_bytes(article_pdf("10.1016/j.kept.2019.1", "Kept paper"))
    before = _snapshot(lib)
    d = "10.1152/k.1"
    add_upw(web, d, upw_record(d, [loc("https://k.example.org/k1.pdf")]))
    web.add("https://k.example.org/k1.pdf",
            (200, PDF, article_pdf("10.1016/j.jsams.2019.04.012", "An unrelated study of cycling cadence")))
    _, rep, lib = run_stage(tmp_path, [{"doi": d}], no_write_ris=False)
    row = rep[d]
    assert row["downloaded"] == "False" and row["identity"] == "FLAG" and row["outcome"] == "ERROR"
    assert row["error"].startswith("DOI_MISMATCH:pdf_doi=10.1016/j.jsams.2019.04.012")
    after = _snapshot(lib)
    assert all(after.get(k) == v for k, v in before.items())    # nothing left the library
    assert (lib / row["filename"]).exists()                      # the flagged file stays where written
    assert not (lib / "_mismatch").exists()
    ids = json.loads((lib / (row["filename"][:-4] + ".identity.json")).read_text(encoding="utf-8"))
    assert ids["identity"] == "FLAG" and ids["identity_evidence"]["pdf_dois"] == ["10.1016/j.jsams.2019.04.012"]
    assert ris_calls == []                                       # a flagged file gets no .ris
    import sweep
    v = sweep.unpaywall_verdict(row)
    assert v.identity and sweep.classify([v])[0] == "IDENTITY_FLAG"


def test_flagged_file_never_reads_as_the_queued_doi(web, tmp_path):
    """REG-I11 for the sibling stages: pmc_fetch/preprint_fetch ask _doi_of_existing."""
    d = "10.1152/k.2"
    add_upw(web, d, upw_record(d, [loc("https://k.example.org/k2.pdf")]))
    web.add("https://k.example.org/k2.pdf", (200, PDF, make_pdf("A scanned page with no identifiers")))
    _, rep, lib = run_stage(tmp_path, [{"doi": d, "title": "Heat acclimation in soldiers"}])
    pdf = str(lib / rep[d]["filename"])
    got = U._doi_of_existing(pdf)
    assert got and got != d                                      # never '' (reads as "same") nor d


def test_title_match_counts_as_the_right_paper(web, tmp_path):
    d = "10.1152/l.1"
    title = "Cardiovascular strain during uncompensable heat stress in firefighters"
    add_upw(web, d, upw_record(d, [loc("https://l.example.org/l1.pdf")], title=title))
    web.add("https://l.example.org/l1.pdf", (200, PDF, make_pdf(f"Research article\n{title}\nC. Author")))
    _, rep, _ = run_stage(tmp_path, [{"doi": d, "title": title}])
    assert rep[d]["identity"] == "TITLE_MATCH" and rep[d]["downloaded"] == "True"


def test_a_supplement_is_flagged_even_when_it_prints_the_doi(web, tmp_path):
    d = "10.1152/m.1"
    add_upw(web, d, upw_record(d, [loc("https://m.example.org/m1.pdf")]))
    web.add("https://m.example.org/m1.pdf",
            (200, PDF, make_pdf(f"Supplementary material for: Heat acclimation and athletic performance\n"
                                f"https://doi.org/{d}\n")))
    _, rep, _ = run_stage(tmp_path, [{"doi": d}])
    assert rep[d]["doc_kind"] == "SUPPLEMENT" and rep[d]["downloaded"] == "False"
    assert "doc_kind=SUPPLEMENT" in rep[d]["error"]


# ------------------------------------------------------------------ REG-I11: names and sidecars
def test_sidecars_follow_the_final_pdf_name_on_a_collision(web, tmp_path, ris_calls):
    lib = tmp_path / "lib"
    lib.mkdir()
    fn = U.build_filename("2020", "Author A", "Heat acclimation and athletic performance")
    (lib / fn).write_bytes(article_pdf("10.1016/j.other.2020.101", "Another paper on heat"))
    (lib / (fn[:-4] + ".ris")).write_text("TY  - JOUR\nDO  - 10.1016/j.other.2020.101\nER  - \n",
                                          encoding="utf-8")
    d = "10.1152/n.1"
    add_upw(web, d, upw_record(d, [loc("https://n.example.org/n1.pdf")]))
    web.add("https://n.example.org/n1.pdf", (200, PDF, article_pdf(d)))
    _, rep, lib = run_stage(tmp_path, [{"doi": d}], no_write_ris=False)
    final = rep[d]["filename"]
    assert final != fn and final.startswith(fn[:-4] + "_") and (lib / final).exists()
    assert (lib / (final[:-4] + ".identity.json")).exists()
    assert not (lib / (fn[:-4] + ".identity.json")).exists()
    assert ris_calls == [(d, str(lib / final))]
    assert (lib / (fn[:-4] + ".ris")).read_text(encoding="utf-8").count("10.1016/j.other.2020.101") == 1


@pytest.mark.parametrize("orphan_doi, expect_hashed", [("10.1016/j.someone.2020.1", True),
                                                      ("10.1152/o.1", False)])
def test_an_orphan_sidecar_occupies_its_stem(web, tmp_path, orphan_doi, expect_hashed):
    lib = tmp_path / "lib"
    lib.mkdir()
    fn = U.build_filename("2020", "Author A", "Heat acclimation and athletic performance")
    (lib / (fn[:-4] + ".fulltext.json")).write_text(json.dumps({"doi": orphan_doi, "text": "t"}),
                                                    encoding="utf-8")
    d = "10.1152/o.1"
    add_upw(web, d, upw_record(d, [loc("https://o.example.org/o1.pdf")]))
    web.add("https://o.example.org/o1.pdf", (200, PDF, article_pdf(d)))
    _, rep, _ = run_stage(tmp_path, [{"doi": d}])
    assert (rep[d]["filename"] != fn) is expect_hashed           # same DOI: the PDF joins its sidecar


def test_two_blank_title_rows_get_two_pdfs_with_their_own_sidecars(web, tmp_path):
    rows = []
    for n in range(2):
        d = f"10.1152/p.{n}"
        add_upw(web, d, upw_record(d, [loc(f"https://p.example.org/{n}.pdf")], title=""))
        web.add(f"https://p.example.org/{n}.pdf", (200, PDF, article_pdf(d, "Untitled work")))
        rows.append({"doi": d, "title": "", "authors": "", "year": ""})
    _, rep, lib = run_stage(tmp_path, rows)
    names = {rep[f"10.1152/p.{n}"]["filename"] for n in range(2)}
    assert len(names) == 2
    for n in range(2):
        name = rep[f"10.1152/p.{n}"]["filename"]
        ids = json.loads((lib / (name[:-4] + ".identity.json")).read_text(encoding="utf-8"))
        assert ids["queue_doi"] == f"10.1152/p.{n}"


def test_an_existing_file_with_no_doi_does_not_make_an_unrelated_row_skip(web, tmp_path):
    lib = tmp_path / "lib"
    lib.mkdir()
    fn = U.build_filename("2020", "Author A", "Heat acclimation and athletic performance")
    (lib / fn).write_bytes(make_pdf("A different paper about sleep and shift work\nno identifiers"))
    d = "10.1152/q.1"
    add_upw(web, d, upw_record(d, [loc("https://q.example.org/q1.pdf")]))
    web.add("https://q.example.org/q1.pdf", (200, PDF, article_pdf(d)))
    _, rep, lib = run_stage(tmp_path, [{"doi": d}])
    assert rep[d]["oa_status"] != "SKIP_EXISTS" and rep[d]["downloaded"] == "True"
    assert rep[d]["filename"] != fn and (lib / fn).exists()      # the unknown file is untouched


def test_same_paper_on_disk_skips_by_name_by_legacy_name_and_by_ris_doi(web, tmp_path):
    lib = tmp_path / "lib"
    lib.mkdir()
    authors, title = "van der Walt JHA; Smith B", "The effects of heat on the heart"
    new, old = U.build_filename("2020", authors, title), U.legacy_build_filename("2020", authors, title)
    assert new != old                                            # DEC-14/15 changed this name
    (lib / old).write_bytes(article_pdf("10.1152/r.1", title))   # held under the legacy name
    (lib / "2021_Renamed_ByHand.pdf").write_bytes(b"%PDF-1.4 whatever")
    (lib / "2021_Renamed_ByHand.ris").write_text("DO  - 10.1152/r.2\n", encoding="utf-8")
    fn3 = U.build_filename("2020", "Author A", "Heat acclimation and athletic performance")
    (lib / fn3).write_bytes(article_pdf("10.1152/r.3"))
    rows = [{"doi": "10.1152/r.1", "authors": authors, "title": title},
            {"doi": "10.1152/r.2"}, {"doi": "10.1152/r.3"}]
    _, rep, _ = run_stage(tmp_path, rows)
    assert rep["10.1152/r.1"]["oa_status"] == "SKIP_EXISTS" and rep["10.1152/r.1"]["filename"] == old
    assert rep["10.1152/r.2"]["filename"] == "2021_Renamed_ByHand.pdf"
    assert rep["10.1152/r.3"]["oa_status"] == "SKIP_EXISTS"
    assert web.to("api.unpaywall.org") == []
    assert all(rep[k]["outcome"] == "OK" and rep[k]["route"] == "exists" for k in rep)


def test_resolve_dest_never_clobbers_a_file_it_cannot_identify(tmp_path):
    lib = tmp_path
    (lib / "a.pdf").write_bytes(b"%PDF-1.4 unknown")
    dest, collided = U.resolve_dest(str(lib), "a.pdf", "10.1152/x.1", set())
    assert collided and dest != str(lib / "a.pdf")
    (lib / "b.ris").write_text("DO  - 10.1152/x.1\n", encoding="utf-8")    # orphan for the same DOI
    assert U.resolve_dest(str(lib), "b.pdf", "10.1152/x.1", set()) == (str(lib / "b.pdf"), False)
    (lib / "c.ris").write_text("DO  - 10.1152/y.2\n", encoding="utf-8")    # orphan for another DOI
    assert U.resolve_dest(str(lib), "c.pdf", "10.1152/x.1", set())[1] is True


def test_doi_of_existing_reads_the_identity_sidecar(tmp_path):
    pdf = tmp_path / "x.pdf"
    pdf.write_bytes(b"%PDF-1.4")
    side = tmp_path / "x.identity.json"
    side.write_text(json.dumps({"queue_doi": "10.1152/ok.1", "identity": "OK", "doc_kind": "VOR"}),
                    encoding="utf-8")
    assert U._doi_of_existing(str(pdf)) == "10.1152/ok.1"
    side.write_text(json.dumps({"queue_doi": "10.1152/ok.1", "identity": "FLAG",
                                "identity_evidence": {"pdf_dois": []}}), encoding="utf-8")
    assert U._doi_of_existing(str(pdf)) == "unverified:10.1152/ok.1"
    side.write_text(json.dumps({"queue_doi": "10.1152/ok.1", "identity": "FLAG",
                                "identity_evidence": {"pdf_dois": ["10.1016/j.x.2020.1"]}}), encoding="utf-8")
    assert U._doi_of_existing(str(pdf)) == "10.1016/j.x.2020.1"


# ------------------------------------------------------------------ DEC-14 author fallback
def test_unknown_queue_author_takes_the_records_first_author(web, tmp_path):
    rec = fixture_json("unpaywall_crossref_record.json")          # live shape: z_authors[].raw_author_name
    d = rec["doi"]
    rec["oa_locations"] = [loc("https://s.example.org/s1.pdf")]
    add_upw(web, d, rec)
    web.add("https://s.example.org/s1.pdf", (200, PDF, article_pdf(d, rec["title"])))
    _, rep, lib = run_stage(tmp_path, [{"doi": d, "authors": "", "title": rec["title"], "year": "2025"}])
    first = U.first_author_of(rec)
    assert first and U.last_name(first) != "Unknown"
    assert rep[d]["filename"].startswith(f"2025_{U.last_name(first)}_")
    assert (lib / rep[d]["filename"]).exists()


@pytest.mark.parametrize("rec, want", [
    ({"z_authors": [{"author_position": "middle", "raw_author_name": "B Two"},
                    {"author_position": "first", "raw_author_name": "Hugo De Vries"}]}, "Hugo De Vries"),
    ({"z_authors": [{"family": "van der Walt", "given": "J"}]}, "van der Walt, J"),
    ({"z_authors": None}, ""), ({}, ""), ({"z_authors": ["junk"]}, ""),
])
def test_first_author_of_record_shapes(rec, want):
    assert U.first_author_of(rec) == want


# ------------------------------------------------------------------ report contract
def test_report_keeps_legacy_columns_and_adds_typed_ones(web, tmp_path):
    add_upw(web, "10.1152/t.1", upw_record("10.1152/t.1", [], is_oa=False))
    run_stage(tmp_path, [{"doi": "10.1152/t.1"}, {"doi": "10.48550/arxiv.9"}])
    with open(tmp_path / "report.csv", encoding="utf-8") as f:
        header = next(csv.reader(f))
    legacy = ["rank", "doi", "year", "cites", "filename", "title", "oa_status", "n_locations",
              "downloaded", "winning_host", "winning_url", "attempts", "error"]
    assert header[:len(legacy)] == legacy                        # sweep, pmc_fetch, consumers read these
    assert {"first_status", "route", "outcome"} <= set(header)
    with open(tmp_path / "report.csv", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            Kind(r["outcome"])                                   # every outcome is a Kind value


def test_closed_record_is_not_available(web, tmp_path):
    add_upw(web, "10.1152/u.1", upw_record("10.1152/u.1", [], is_oa=False))
    _, rep, _ = run_stage(tmp_path, [{"doi": "10.1152/u.1"}])
    assert rep["10.1152/u.1"]["oa_status"] == "CLOSED" and rep["10.1152/u.1"]["outcome"] == "NOT_AVAILABLE"


def test_dry_run_writes_nothing_to_the_library(web, tmp_path):
    add_upw(web, "10.1152/v.1", upw_record("10.1152/v.1", [loc("https://v.example.org/v1.pdf")]))
    res, rep, lib = run_stage(tmp_path, [{"doi": "10.1152/v.1"}], dry_run=True)
    assert rep["10.1152/v.1"]["outcome"] == "SKIPPED" and rep["10.1152/v.1"]["winning_url"]
    assert list(lib.iterdir()) == [] and web.to("v.example.org") == []
    assert res["exit_code"] == 0


def test_try_download_legacy_adapter(web, tmp_path):
    web.add("https://w.example.org/ok.pdf", (200, PDF, article_pdf("10.1/w")))
    web.add("https://w.example.org/landing", (200, HTML, b"<html>landing</html>"))
    web.add("https://w.example.org/gone.pdf", (404, HTML, b"<html>gone</html>"))
    dest = tmp_path / "a.pdf"
    st, msg = U.try_download("https://w.example.org/ok.pdf", str(dest))
    assert st == "OK" and dest.exists() and int(msg) == dest.stat().st_size
    assert U.try_download("https://w.example.org/landing", str(tmp_path / "b.pdf")) == ("HTML", b"<html>landing</html>")
    assert U.try_download("https://w.example.org/gone.pdf", str(tmp_path / "c.pdf")) == ("HTTP_404", "")
