"""The preprint stage on litpipe.net, routed per project (dispatch W2-C; refactor scope 3.3). Offline.

Every test drives the real stage (`preprint_fetch.run`) through the real litpipe.net (host table,
pacing, budgets, refusals, prohibited routes, redirects, ledger) against the FakeState of `net_env`
(or, where a refusal must outlive a run, the REAL litpipe.state on a temp file); only the transport
is replaced by `Web`, which answers from canned replies (tests/fixtures/W2-C: the 2026-10-05 W2-C
probes and the 2026-09-25 V4 audit, trimmed; PROVENANCE.json says which) and fails the test on any
URL it was not given. PDFs are generated with pymupdf so the identity check reads real text.
"""
import csv
import json
import os
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest
from requests.structures import CaseInsensitiveDict

import lit_util
import preprint_fetch as P
from litpipe import net
from litpipe import state as real_state
from litpipe.outcomes import Kind, from_legacy

FIX = Path(__file__).parent / "fixtures" / "W2-C"
EPMC = P.EPMC_SEARCH
ARXIV = P.ARXIV_API
OAI = P.SPORTRXIV_OAI
BALLET = "The Demands of a Professional Ballet Schedule: A Five-Season Analysis"
PAPER_64898 = ("Molecular Transducers of Physical Activity Consortium: Initial Insights into the "
           "Dynamic Human Responses to Exercise")
DOI_64898 = "10.64898/2026.03.02.705347"
SRX_1079 = ("Sustained Use and Discontinuation in Digital Fitness Applications: A Systematic Review of "
            "Retention Patterns")
SRX_1095 = "IMPROVING THE EDUCATIONAL PROCESS OF SPORTS COACHES THROUGH THE SYSTEMATIC USE OF MOBILE APPLICATIONS"
FORBIDDEN = {"arxiv.org", "www.arxiv.org", "www.biorxiv.org", "www.medrxiv.org", "biorxiv.org", "medrxiv.org",
             "europepmc.org", "www.europepmc.org", "pmc.ncbi.nlm.nih.gov"}


def fx(name):
    return (FIX / name).read_bytes()


def fxj(name):
    return json.loads((FIX / name).read_text(encoding="utf-8"))


def make_pdf(first_page_text, pages=3):
    """A real PDF: page 1 carries `first_page_text`; padded past MIN_PDF_BYTES via metadata."""
    import fitz
    doc = fitz.open()
    filler = ("Rehearsal load, performance count and season structure were recorded for each "
              "dancer over five seasons. ") * 10
    for i in range(pages):
        page = doc.new_page()
        page.insert_textbox(fitz.Rect(40, 40, 560, 800), first_page_text if i == 0 else filler, fontsize=8)
    doc.set_metadata({"subject": "pad " * 3000})
    data = doc.tobytes()
    doc.close()
    return data


def preprint_pdf(title, doi=""):
    return make_pdf(f"Preprint, not peer reviewed\n{title}\nA. Author, B. Author\n"
                    + (f"doi: https://doi.org/{doi}\n" if doi else ""))


class Unrouted(BaseException):
    """Not an Exception: the stage's per-row guard must not swallow a test's missing route."""


class Web:
    """litpipe.net transport double. Routes are keyed by URL without the query string; a value is
    a list of (status, headers, body) replies (the last one repeats) or a callable(url) returning
    one. Every request that reaches the transport is recorded in `sent`; `advance` adds virtual
    seconds to the clock per request."""

    def __init__(self, clock):
        self.routes, self.sent, self.clock, self.advance = {}, [], clock, {}

    def add(self, url, *replies):
        self.routes[url] = [r if (isinstance(r, tuple) or callable(r)) else (200, {}, r) for r in replies]
        return self

    def hosts(self):
        return [urlsplit(u).hostname for _, u, _ in self.sent]

    def to(self, host):
        return [u for _, u, _ in self.sent if urlsplit(u).hostname == host]

    def __call__(self, method, url, headers, body, timeout, max_bytes):
        self.sent.append((method, url, dict(headers)))
        key = url.split("?", 1)[0]
        seq = self.routes.get(key)
        if seq is None:
            raise Unrouted(f"unrouted request (no live network in tests): {method} {url}")
        r = seq.pop(0) if len(seq) > 1 else seq[0]
        status, hdrs, data = r(url) if callable(r) else r
        if isinstance(data, str):
            data = data.encode("utf-8")
        self.clock.t += self.advance.get(urlsplit(url).hostname, 0.0)
        cap = net._cap(status, max_bytes)
        trunc = cap is not None and len(data) > cap
        return net._Raw(status, CaseInsensitiveDict(hdrs), data, data[:65536], len(data), trunc)


JSON = {"Content-Type": "application/json"}
PDF = {"Content-Type": "application/pdf"}


def q(url, key):
    return (parse_qs(urlsplit(url).query).get(key) or [""])[0]


def epmc_body(results):
    return json.dumps({"version": "6.9", "hitCount": len(results), "resultList": {"result": results}})


def epmc_record(name, idx=0):
    return fxj(name)["resultList"]["result"][idx]


class StageEnv:
    def __init__(self, net_env, web, tmp_path, ris_calls):
        self.net_env, self.web, self.tmp, self.ris_calls = net_env, web, tmp_path, ris_calls
        self.root = tmp_path / "root"
        self.lib = self.root / "research_alpha" / "literature"
        self.lib.mkdir(parents=True)
        self.cfg = {"state_dir": str(tmp_path / "state"),
                    "projects": {"research_alpha": {"lib_dir": "literature"},
                                 "teaching_beta": {"lib_dir": "lit"}}}
        self.n = 0

    @property
    def state(self):
        return self.net_env.state

    def sources(self, *names, key="research_alpha"):
        self.cfg["projects"][key]["sources"] = ["unpaywall", "pmc", *names]
        return self

    def switches(self, **kw):
        self.cfg["hosts"] = kw
        return self

    def run(self, rows, project="research_alpha", **kw):
        self.n += 1
        triage = self.tmp / f"triage{self.n}.csv"
        with open(triage, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["doi", "title", "year", "authors"])
            w.writeheader()
            for r in rows:
                w.writerow({"doi": r.get("doi", ""), "title": r.get("title", ""), "year": r.get("year", ""),
                            "authors": r.get("authors", "")})
        report = self.tmp / f"report{self.n}.csv"
        res = P.run(triage=str(triage), lib_dir=str(self.lib), report=str(report), project=project,
                    cfg=self.cfg, **kw)
        out = list(csv.DictReader(open(report, encoding="utf-8"))) if report.exists() else []
        self.report_path = report
        return res, out


@pytest.fixture
def web(net_env, monkeypatch):
    w = Web(net_env.clock)
    monkeypatch.setitem(net._TRANSPORTS, "requests", w)
    monkeypatch.setitem(net._TRANSPORTS, "urllib", w)
    return w


@pytest.fixture
def ris_calls(monkeypatch):
    calls = []

    def fake_emit(doi, pdf_path, overwrite=False):
        calls.append((doi, pdf_path))
        return ("OK", os.path.splitext(pdf_path)[0] + ".ris")

    monkeypatch.setattr(P._R, "emit_ris_for_pdf", fake_emit)
    return calls


@pytest.fixture
def env(net_env, web, tmp_path, monkeypatch, ris_calls):
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", tmp_path / "root")
    return StageEnv(net_env, web, tmp_path, ris_calls)


def arxiv_406(url):
    d = fxj("arxiv_406_response.json")
    hdrs = {k: v for k, v in d["headers"].items() if k.lower() != "transfer-encoding"}
    return 406, hdrs, b""


def route_osf_ballet(web, pdf=None):
    web.add("https://api.osf.io/v2/preprints/", (200, {"Content-Type": "application/vnd.api+json"},
                                                fx("osf_preprints_ballet.json")))
    web.add("https://api.osf.io/v2/files/61112f14e38013042f960b92/",
            (200, {"Content-Type": "application/vnd.api+json"}, fx("osf_file_record.json")))
    web.add("https://osf.io/download/f6yza/",
            (302, {"Location": "https://files.osf.io/v1/resources/fkdby_v1/providers/osfstorage/61112f14"}, b""))
    web.add("https://files.osf.io/v1/resources/fkdby_v1/providers/osfstorage/61112f14",
            (302, {"Location": "https://storage.googleapis.com/cos-osf-prod-files-us-east1/blob?X-Goog-Signature=abc"}, b""))
    web.add("https://storage.googleapis.com/cos-osf-prod-files-us-east1/blob",
            (200, {"Content-Type": "application/octet-stream"}, pdf if pdf is not None else preprint_pdf(BALLET, "10.31236/osf.io/fkdby")))


def assert_contract(rows):
    """The typed-column contract on every row (dispatch W2b, shared section)."""
    for r in rows:
        for col in P.TYPED_FIELDS + P.LEGACY_FIELDS:
            assert col in r, col
        kind = Kind(r["outcome"])
        if not r["detail"].startswith("source_excluded:"):
            assert from_legacy(r["status"], "preprint") is kind, (r["status"], kind)
        else:
            assert kind is Kind.SKIPPED and r["status"].startswith("SOURCE_EXCLUDED:")
        assert (r["skipped"] == "True") == (r["status"] == "ALREADY_EXISTS"), r
        assert (r["downloaded"] == "True") == (kind is Kind.OK and r["status"] == "OK"), r
        if r["identity"] == "FLAG":
            assert r["downloaded"] == "False" and r["skipped"] == "False" and r["status"].startswith("DOI_MISMATCH:")
        if kind is Kind.SKIPPED and r["status"] == "MANUAL_PREPRINT":
            assert r["detail"] == "manual_preprint" and r["landing_url"].startswith("https://")
        assert "email=" not in json.dumps(r) and "mailto:" not in json.dumps(r)


# ================================================================ arXiv: the 09-25 refusal replayed
def test_arxiv_406_replay_one_request_then_refused_rows_never_no_match(env, monkeypatch):
    env.sources("arxiv")
    monkeypatch.setattr(P, "ARXIV_BATCH", 1)              # two id batches: the second must not be sent
    env.web.add(ARXIV, arxiv_406)
    rows = [{"doi": "10.48550/arXiv.2103.00020", "title": "Learning Transferable Visual Models"},
            {"doi": "10.48550/arXiv.1511.07289", "title": "Fast and Accurate Deep Network Learning"},
            {"doi": "10.1234/journal.1", "title": "Multi-Electron Production at High Transverse Momenta"},
            {"doi": "10.1234/journal.2", "title": "Why Should I Trust You? Explaining Classifiers"}]
    res, out = env.run(rows)
    assert len(env.web.to("export.arxiv.org")) == 1                     # after the first 406: zero
    assert env.state.refused["export.arxiv.org"][1] == "manual"         # net, from the host row
    assert [r["outcome"] for r in out] == ["REFUSED"] * 4
    assert all(r["detail"] == "host_refused:export.arxiv.org" for r in out)
    assert all(r["found"] == "False" for r in out)
    assert not any(r["status"] == "NO_MATCH" or r["outcome"] == "NO_MATCH" for r in out)
    assert out[0]["status"] == "HTTP_406" and out[0]["first_status"] == "406"
    assert out[1]["status"] == "HOST_REFUSED:export.arxiv.org" and out[1]["first_status"] == ""
    assert res["refused_hosts"] == ["export.arxiv.org"]
    assert_contract(out)


def test_arxiv_refused_beforehand_sends_nothing(env):
    env.sources("arxiv")
    env.state.refuse("export.arxiv.org", "manual: refused since 2026-09-25", persistence="manual")
    res, out = env.run([{"doi": "10.48550/arXiv.2103.00020", "title": "T one"},
                        {"doi": "10.1234/x.1", "title": "Some other paper title"}])
    assert env.web.sent == []
    # not even a not_sent ledger line: the stage itself skips a refused host
    assert not [ln for ln in env.net_env.ledger_lines() if "arxiv" in ln["host"]]
    assert [r["outcome"] for r in out] == ["REFUSED", "REFUSED"]
    assert_contract(out)


def test_arxiv_refusal_is_the_root_cause_over_another_sources_no_match(env):
    env.sources("arxiv", "osf")
    env.state.refuse("export.arxiv.org", "manual", persistence="manual")
    env.web.add("https://api.osf.io/v2/preprints/", (200, JSON, json.dumps({"data": [], "links": {"meta": {"total": 0}}})))
    _, out = env.run([{"doi": "10.1234/x.1", "title": "A paper nobody posted anywhere at all"}])
    assert out[0]["outcome"] == "REFUSED" and out[0]["detail"] == "host_refused:export.arxiv.org"
    assert_contract(out)


def test_arxiv_rate_exceeded_200_refuses_manually(env):
    env.web.add(ARXIV, (200, {"Content-Type": "text/plain"}, b"Rate exceeded."))
    _, out = env.run([{"doi": "10.48550/arXiv.2103.00020", "title": "T"}], project=None)
    assert out[0]["outcome"] == "REFUSED"
    assert env.state.refused["export.arxiv.org"][1] == "manual"
    assert_contract(out)


@pytest.fixture
def real(net_env, tmp_path, monkeypatch):
    """net_env with litpipe.net wired to the REAL litpipe.state on a temp sqlite file (virtual clock)."""
    clock = net_env.clock

    def pace(s):
        clock.t += max(s, 0.0)

    monkeypatch.setattr(real_state, "DB_PATH", tmp_path / "state" / real_state.DB_NAME)
    monkeypatch.setattr(real_state, "_current_run", None)
    monkeypatch.setattr(real_state, "_time", clock.time)
    monkeypatch.setattr(real_state, "_sleep", pace)
    monkeypatch.setattr(net, "STATE", real_state)
    return real_state


def test_arxiv_manual_refusal_survives_a_new_run_on_the_real_state(env, real):
    env.web.add(ARXIV, arxiv_406)
    row = [{"doi": "10.48550/arXiv.2103.00020", "title": "Learning Transferable Visual Models"}]
    run1 = real.register_run("sweep")
    _, out1 = env.run(row, project=None)
    assert out1[0]["outcome"] == "REFUSED" and len(env.web.to("export.arxiv.org")) == 1
    real.refuse("api.osf.io", "control: a run refusal", persistence="run")
    real.finish_run(run1)
    real.register_run("sweep")                                   # a new run
    assert real.is_refused("export.arxiv.org") and not real.is_refused("api.osf.io")
    _, out2 = env.run(row, project=None)
    assert len(env.web.to("export.arxiv.org")) == 1                # still the one request of run 1
    assert out2[0]["outcome"] == "REFUSED" and out2[0]["status"] == "HOST_REFUSED:export.arxiv.org"
    real.clear_refusal("export.arxiv.org")                         # what `--clear-refusal` does
    env.run(row, project=None)
    assert len(env.web.to("export.arxiv.org")) == 2


# ================================================================ arXiv: ids, parsing, PDFs
def test_arxiv_ids_batched_on_https_and_pdf_switch_off_is_a_manual_preprint(env):
    env.web.add(ARXIV, (200, {"Content-Type": "application/atom+xml"}, fx("arxiv_atom_docs_example.xml")))
    rows = [{"doi": "10.48550/arXiv.2103.00020", "title": "Learning Transferable Visual Models From Natural Language Supervision"},
            {"doi": "arXiv:hep-ex/0307015", "title": "Multi-Electron Production at High Transverse Momenta in ep Collisions at HERA"}]
    _, out = env.run(rows, project=None)
    sent = env.web.to("export.arxiv.org")
    assert len(sent) == 1 and sent[0].startswith("https://export.arxiv.org/api/query")
    assert q(sent[0], "id_list") == "2103.00020,hep-ex/0307015"
    assert [r["status"] for r in out] == ["MANUAL_PREPRINT", "MANUAL_PREPRINT"]
    assert out[1]["match_id"] == "hep-ex/0307015v1"                   # old-style id kept whole (N-D3)
    assert out[1]["landing_url"] == "https://arxiv.org/abs/hep-ex/0307015v1"
    assert "arxiv.org" not in env.web.hosts()                         # no PDF without the switch
    assert_contract(out)


def test_arxiv_pdf_switch_on_fetches_arxiv_org_pdf_paced_15s(env):
    env.switches(arxiv_pdf_allowed=True)
    env.web.add(ARXIV, (200, {}, fx("arxiv_atom_docs_example.xml")))
    title = "Learning Transferable Visual Models From Natural Language Supervision"
    env.web.add("https://arxiv.org/pdf/2103.00020v1", (200, PDF, make_pdf(f"arXiv:2103.00020v1\n{title}\nA. Radford")))
    _, out = env.run([{"doi": "10.48550/arXiv.2103.00020", "title": title, "year": "2021", "authors": "Radford A"}],
                     project=None)
    assert out[0]["downloaded"] == "True" and out[0]["identity"] == "TITLE_MATCH"
    assert env.web.to("arxiv.org") == ["https://arxiv.org/pdf/2103.00020v1"]
    assert_contract(out)


def test_parse_arxiv_feed_old_style_id_and_placeholder_filter():
    entries = P.parse_arxiv_feed(fx("arxiv_atom_docs_example.xml"))
    assert [e["id"] for e in entries] == ["2103.00020v1", "hep-ex/0307015v1"]
    assert entries[0]["journal_doi"] == ""                           # 10.1145/nnnnnnn.nnnnnnn dropped
    assert entries[1]["journal_doi"] == "10.1140/epjc/s2003-01326-x"
    assert entries[0]["title"] == "Learning Transferable Visual Models From Natural Language Supervision"
    with pytest.raises(ValueError):
        P.parse_arxiv_feed(b"Rate exceeded.")


# ================================================================ DEC-31: which project, which sources
def test_a_project_without_preprint_sources_sends_nothing_for_ordinary_rows(env):
    rows = [{"doi": "10.1249/mss.0000000000001234", "title": "Heat acclimation and endurance performance"},
            {"doi": DOI_64898, "title": PAPER_64898},
            {"doi": "10.51224/SportRxiv.997", "title": "Goalkeeper GPS load profiles"},
            {"doi": "10.31236/osf.io/fkdby", "title": BALLET}]
    res, out = env.run(rows)
    assert env.web.sent == []
    assert [r["detail"] for r in out] == ["source_excluded:europepmc_preprints", "source_excluded:biorxiv",
                                          "source_excluded:sportrxiv", "source_excluded:osf"]
    assert all(r["outcome"] == "SKIPPED" and r["skipped"] == "False" and r["found"] == "False" for r in out)
    assert res["skipped_source"] == 4
    assert_contract(out)


def test_with_arxiv_refused_a_default_project_sends_nothing_at_all(env):
    env.state.refuse("export.arxiv.org", "manual", persistence="manual")
    rows = [{"doi": "10.1249/mss.1", "title": "Heat acclimation"},
            {"doi": "10.48550/arXiv.2103.00020", "title": "Learning Transferable Visual Models"}]
    _, out = env.run(rows)
    assert env.web.sent == [] and env.net_env.ledger_lines() == []
    assert [r["outcome"] for r in out] == ["SKIPPED", "REFUSED"]
    assert_contract(out)


def test_project_from_lib_dir_and_the_default_line(env, capsys):
    env.sources("osf")
    route_osf_ballet(env.web)
    _, out = env.run([{"doi": "10.1123/ijspp.1", "title": BALLET, "year": "2021", "authors": "Shaw J"}], project=None)
    assert "project research_alpha (from --lib-dir); preprint sources: osf" in capsys.readouterr().out
    assert out[0]["downloaded"] == "True"
    env.lib = env.tmp / "unregistered_lib"
    env.lib.mkdir()
    env.web.sent.clear()
    _, out = env.run([{"doi": "10.1123/ijspp.1", "title": BALLET}], project=None)
    printed = capsys.readouterr().out
    assert "no registered project matches --lib-dir" in printed and "default sources (unpaywall, pmc)" in printed
    assert env.web.sent == [] and out[0]["detail"] == "source_excluded:europepmc_preprints"


def test_sources_override_replaces_the_projects_list(env):
    env.sources("osf")
    env.web.add(ARXIV, (200, {}, b'<feed xmlns="http://www.w3.org/2005/Atom"></feed>'))
    _, out = env.run([{"doi": "10.1123/x.1", "title": BALLET}], sources="arxiv")
    assert out[0]["outcome"] == "NO_MATCH" and out[0]["route"] == "arxiv_search"
    sent = env.web.to("export.arxiv.org")
    assert len(sent) == 1 and q(sent[0], "search_query").startswith('ti:"The Demands of a Professional')
    assert env.web.to("api.osf.io") == []                            # the project's own list is replaced
    assert_contract(out)


def test_an_unregistered_project_or_a_bad_source_is_config(env):
    res, _ = env.run([{"doi": "10.1/x", "title": "T"}], project="no_such_project")
    assert res["exit_code"] == P.EXIT_CONFIG
    res, _ = env.run([{"doi": "10.1/x", "title": "T"}], sources="arxiv,notaserver")
    assert res["exit_code"] == P.EXIT_CONFIG
    assert env.web.sent == []


# ================================================================ OSF
def test_osf_route_yields_pdf_through_the_redirect_hops(env):
    env.sources("osf")
    route_osf_ballet(env.web)
    _, out = env.run([{"doi": "10.1123/ijspp.2021.1", "title": BALLET, "year": "2021", "authors": "Shaw J"}])
    r = out[0]
    assert r["downloaded"] == "True" and r["outcome"] == "OK" and r["route"] == "osf_download"
    assert r["source"] == "osf" and r["match_id"] == "fkdby_v1"
    assert env.web.hosts() == ["api.osf.io", "api.osf.io", "osf.io", "files.osf.io", "storage.googleapis.com"]
    pdf = env.lib / r["preprint_filename"]
    assert pdf.read_bytes()[:4] == b"%PDF"
    ids = json.loads(pdf.with_suffix(".identity.json").read_text(encoding="utf-8"))
    assert ids["source"] == "preprint" and ids["identity"] in ("OK", "TITLE_MATCH")
    assert "X-Goog-Signature" not in json.dumps(ids)                # the signed blob URL is not kept
    assert any(w >= 35.9 for w in env.state.pacing_waits)            # api.osf.io paced at 36 s
    assert env.ris_calls == [("10.1123/ijspp.2021.1", str(pdf))]
    assert_contract(out)


def test_osf_daily_budget_spent_is_deferred(env):
    env.sources("osf")
    env.state.budgets["api.osf.io"] = 0
    _, out = env.run([{"doi": "10.1123/x.1", "title": BALLET}])
    assert out[0]["outcome"] == "DEFERRED" and out[0]["status"] == "DEFERRED" and out[0]["found"] == "False"
    assert env.web.sent == []
    assert_contract(out)


def test_a_dead_osf_file_is_not_available_and_a_blocked_hop_refused(env):
    env.sources("osf")
    route_osf_ballet(env.web)
    env.web.add("https://osf.io/download/f6yza/", (404, {}, b""))
    _, out = env.run([{"doi": "10.1123/x.1", "title": BALLET}])
    assert out[0]["outcome"] == "NOT_AVAILABLE" and out[0]["status"] == "HTTP_404 [NOT_AVAILABLE]"
    assert out[0]["found"] == "True"
    assert_contract(out)
    env.web.add("https://osf.io/download/f6yza/", (302, {"Location": "https://evil.example.net/x.pdf"}, b""))
    _, out = env.run([{"doi": "10.1123/x.1", "title": BALLET}])
    assert out[0]["outcome"] == "REFUSED" and out[0]["found"] == "True"     # a download host: OA_BLOCKED
    assert "evil.example.net" not in env.web.hosts()
    assert_contract(out)


def test_row_deadline_defers_the_row(env):
    env.sources("osf")
    route_osf_ballet(env.web)
    env.web.advance["api.osf.io"] = 100.0                            # a slow search
    _, out = env.run([{"doi": "10.1123/x.1", "title": BALLET}], row_deadline=30)
    assert out[0]["outcome"] == "DEFERRED" and out[0]["detail"] == "row_deadline:30s"
    assert out[0]["status"] == "DEFERRED:row_deadline:30s"
    assert len(env.web.to("api.osf.io")) == 1                        # the file record was not asked
    assert_contract(out)


# ================================================================ bioRxiv / medRxiv
def route_epmc(web, by_doi=None, keyword=None):
    def answer(url):
        query = q(url, "query")
        if query.startswith('DOI:"'):
            d = query[5:].split('"', 1)[0]
            return 200, JSON, epmc_body([r for r in (by_doi or {}).get(d, [])])
        return 200, JSON, epmc_body(keyword or [])
    web.add(EPMC, answer)


def test_a_64898_doi_routes_to_details_and_ends_a_manual_preprint(env):
    env.sources("biorxiv", "medrxiv")
    route_epmc(env.web, by_doi={DOI_64898: [epmc_record("epmc_search_64898_biorxiv.json")]})
    env.web.add(f"https://api.biorxiv.org/details/biorxiv/{DOI_64898}/na/json", (200, JSON, fx("biorxiv_details_64898.json")))
    _, out = env.run([{"doi": DOI_64898, "title": PAPER_64898, "year": "2026"}])
    r = out[0]
    assert env.web.to("api.biorxiv.org") == [f"https://api.biorxiv.org/details/biorxiv/{DOI_64898}/na/json"]
    assert r["outcome"] == "SKIPPED" and r["detail"] == "manual_preprint" and r["status"] == "MANUAL_PREPRINT"
    assert r["landing_url"] == f"https://doi.org/{DOI_64898}" and r["found"] == "True"
    assert not set(env.web.hosts()) & FORBIDDEN
    assert_contract(out)


@pytest.mark.parametrize("body", [b"", fx("biorxiv_details_no_posts.json")], ids=["empty_body", "empty_collection"])
def test_an_empty_details_answer_is_outage(env, body):
    env.sources("biorxiv")
    route_epmc(env.web, by_doi={DOI_64898: [epmc_record("epmc_search_64898_biorxiv.json")]})
    env.web.add(f"https://api.biorxiv.org/details/biorxiv/{DOI_64898}/na/json", (200, JSON, body))
    _, out = env.run([{"doi": DOI_64898, "title": PAPER_64898}])
    assert out[0]["outcome"] == "OUTAGE" and out[0]["first_status"] == "200"
    assert_contract(out)


def test_an_unknown_server_tries_the_other_enabled_server(env):
    env.sources("biorxiv", "medrxiv")
    route_epmc(env.web, by_doi={})                                   # Europe PMC does not hold it
    env.web.add(f"https://api.biorxiv.org/details/biorxiv/{DOI_64898}/na/json", (200, JSON, fx("biorxiv_details_no_posts.json")))
    env.web.add(f"https://api.biorxiv.org/details/medrxiv/{DOI_64898}/na/json", (200, JSON, fx("biorxiv_details_64898.json")))
    _, out = env.run([{"doi": DOI_64898, "title": PAPER_64898}])
    assert len(env.web.to("api.biorxiv.org")) == 2
    assert out[0]["status"] == "MANUAL_PREPRINT" and out[0]["source"] == "medrxiv"
    assert_contract(out)


def test_server_comes_from_europe_pmc_publisher_not_the_prefix(env):
    env.sources("biorxiv", "medrxiv")
    rec = dict(epmc_record("epmc_search_doi_biorxiv_oa.json"))
    rec["bookOrReportDetails"] = {"publisher": "medRxiv"}
    rec["isOpenAccess"], rec["fullTextIdList"] = "N", None
    title = rec["title"]
    route_epmc(env.web, keyword=[rec])
    env.web.add("https://api.biorxiv.org/details/medrxiv/10.1101/2025.11.04.686534/na/json",
                (200, JSON, fx("biorxiv_details_64898.json")))
    _, out = env.run([{"doi": "10.1152/japplphysiol.1", "title": title, "year": "2025"}])
    assert env.web.to("api.biorxiv.org")[0].startswith("https://api.biorxiv.org/details/medrxiv/")
    assert out[0]["source"] == "medrxiv"


def test_a_preprint_on_a_server_the_project_excludes_is_source_excluded(env):
    env.sources("biorxiv")
    rec = dict(epmc_record("epmc_search_doi_biorxiv_oa.json"))
    rec["bookOrReportDetails"] = {"publisher": "medRxiv"}
    route_epmc(env.web, keyword=[rec])
    _, out = env.run([{"doi": "10.1152/japplphysiol.1", "title": rec["title"]}])
    assert out[0]["outcome"] == "SKIPPED" and out[0]["detail"] == "source_excluded:medrxiv"
    assert env.web.to("api.biorxiv.org") == []
    assert_contract(out)


def test_europe_pmc_preprint_full_text_is_a_text_only_sidecar_500_sent_once(env):
    env.sources("biorxiv")
    rec = epmc_record("epmc_search_doi_biorxiv_oa.json")
    doi = rec["doi"]
    route_epmc(env.web, by_doi={doi: [rec]})
    env.web.add(f"https://api.biorxiv.org/details/biorxiv/{doi}/na/json", (200, JSON, fx("biorxiv_details_64898.json")))
    jats = (f'<article><front><article-meta><article-id pub-id-type="doi">{doi}</article-id><title-group>'
            f'<article-title>{rec["title"]}</article-title></title-group></article-meta></front>'
            '<body><sec><title>Results</title><p>Fibre types adapt.</p></sec></body></article>')
    env.web.add(f"https://www.ebi.ac.uk/europepmc/webservices/rest/{rec['id']}/fullTextXML",
                (200, {"Content-Type": "application/xml"}, jats))
    _, out = env.run([{"doi": doi, "title": rec["title"], "year": "2025", "authors": "Dilbaz S"}])
    r = out[0]
    assert r["sidecar"] == "True" and r["sidecar_status"] == "OK" and r["downloaded"] == "False"
    assert r["outcome"] == "NOT_AVAILABLE" and r["route"] == "epmc_fulltext" and r["identity"] == ""
    sc = json.loads((env.lib / (r["preprint_filename"][:-4] + ".fulltext.json")).read_text(encoding="utf-8"))
    assert sc["has_pdf"] is False and sc["doi"] == doi and sc["ppr"] == rec["id"]
    assert_contract(out)
    # the probe's answer (2026-10-05: PPR1114009 fullTextXML 500 despite inEPMC=Y): sent once, manual
    env.web.add(f"https://www.ebi.ac.uk/europepmc/webservices/rest/{rec['id']}/fullTextXML",
                (500, JSON, b'{"status":500,"error":"Internal Server Error"}'))
    (env.lib / (r["preprint_filename"][:-4] + ".fulltext.json")).unlink()
    before = len(env.web.to("www.ebi.ac.uk"))
    _, out = env.run([{"doi": doi, "title": rec["title"], "year": "2025", "authors": "Dilbaz S"}])
    assert out[0]["status"] == "MANUAL_PREPRINT"
    assert len(env.web.to("www.ebi.ac.uk")) - before == 2               # the DOI search + one fullTextXML


def test_biorxiv_pdf_switch_on_fetches_the_versioned_pdf(env):
    env.sources("biorxiv").switches(biorxiv_pdf_allowed=True)
    route_epmc(env.web, by_doi={DOI_64898: [epmc_record("epmc_search_64898_biorxiv.json")]})
    env.web.add(f"https://api.biorxiv.org/details/biorxiv/{DOI_64898}/na/json", (200, JSON, fx("biorxiv_details_64898.json")))
    url = f"https://www.biorxiv.org/content/{DOI_64898}v2.full.pdf"
    env.web.add(url, (200, PDF, preprint_pdf(PAPER_64898, DOI_64898)))
    _, out = env.run([{"doi": DOI_64898, "title": PAPER_64898, "year": "2026", "authors": "Consortium Study Group"}])
    assert env.web.to("www.biorxiv.org") == [url]
    assert out[0]["downloaded"] == "True" and out[0]["identity"] == "OK"
    assert_contract(out)


# ================================================================ Europe PMC
def test_europe_pmc_errcode_in_a_200_is_error_not_no_match(env):
    env.sources("europepmc_preprints")
    env.web.add(EPMC, (200, JSON, fx("epmc_errcode_200.json")))
    _, out = env.run([{"doi": "10.1/x", "title": "Heat acclimation and endurance performance"}])
    assert out[0]["outcome"] == "ERROR" and "errCode" in out[0]["detail"]
    assert_contract(out)


def test_no_email_reaches_the_report(env):
    env.sources("europepmc_preprints")
    env.web.add(EPMC, (200, JSON, json.dumps({"errCode": 400, "errMsg": "rejected email=someone@example.org"})))
    _, out = env.run([{"doi": "10.1/x", "title": "Heat acclimation and endurance performance"}])
    assert out[0]["outcome"] == "ERROR"
    text = env.report_path.read_text(encoding="utf-8")
    assert "someone@example.org" not in text and "email=" not in text


def test_an_empty_keyword_group_is_never_sent(env):
    env.sources("europepmc_preprints")
    _, out = env.run([{"doi": "10.1/x", "title": "The Of And On"}])
    assert env.web.sent == []
    assert out[0]["outcome"] == "NO_MATCH"
    assert P.epmc_keyword_query("The Of And") == ""
    assert all(len(P._keywordize("word " * 300 + "x" * 9)) <= 200 for _ in [0])


# ================================================================ SportRxiv
def listrecords_without_1080_galley():
    """The ListRecords fixture with record 1080's galley relation removed (a record with no PDF)."""
    return fx("sportrxiv_oai_listrecords.xml").replace(
        b"\t<dc:relation>https://sportrxiv.org/index.php/server/preprint/view/1080/2331</dc:relation>\n", b"")


def route_oai(web, getrecord=None, listrecords=None):
    def answer(url):
        verb = q(url, "verb")
        if verb == "ListRecords":
            return 200, {"Content-Type": "text/xml"}, listrecords or fx("sportrxiv_oai_listrecords.xml")
        if verb == "GetRecord":
            return getrecord(url) if getrecord else (200, {}, fx("sportrxiv_oai_getrecord.xml"))
        raise Unrouted(url)
    web.add(OAI, answer)


def test_sportrxiv_daily_oai_cache_and_the_galley_route(env):
    env.sources("sportrxiv")
    body = listrecords_without_1080_galley()
    assert b"view/1080/2331" not in body
    route_oai(env.web, listrecords=body)
    env.web.add("https://sportrxiv.org/index.php/server/preprint/download/1079/2330", (200, PDF, preprint_pdf(SRX_1079)))
    _, out = env.run([{"doi": "10.1016/j.x.1", "title": SRX_1079, "year": "2026", "authors": "Tsimachidis K"}])
    assert out[0]["downloaded"] == "True" and out[0]["source"] == "sportrxiv" and out[0]["route"] == "sportrxiv_galley"
    oai = [u for u in env.web.to("sportrxiv.org") if "/oai" in u]
    assert len(oai) == 1 and q(oai[0], "verb") == "ListRecords" and q(oai[0], "from")
    cache = Path(env.cfg["state_dir"]) / "cache" / "sportrxiv_oai.json"
    assert "10.51224/sportrxiv.1079" in json.loads(cache.read_text(encoding="utf-8"))["records"]
    # the same UTC day: no second harvest; a record without a galley is a manual preprint
    title_1080 = json.loads(cache.read_text(encoding="utf-8"))["records"]["10.51224/sportrxiv.1080"]["title"]
    _, out = env.run([{"doi": "10.1016/j.x.2", "title": title_1080}])
    assert len([u for u in env.web.to("sportrxiv.org") if "ListRecords" in u]) == 1
    assert not [u for u in env.web.to("sportrxiv.org") if "GetRecord" in u]
    assert out[0]["status"] == "MANUAL_PREPRINT" and "sportrxiv.org" in out[0]["landing_url"]
    assert_contract(out)


def test_sportrxiv_crossref_lookup_then_getrecord_galley(env):
    env.sources("sportrxiv")
    route_oai(env.web, getrecord=lambda url: (200, {}, fx("sportrxiv_oai_getrecord.xml").replace(b"1079", b"1095")))
    env.web.add(P.CROSSREF_51224_WORKS, (200, JSON, fx("crossref_prefix_51224_works.json")))
    env.web.add("https://sportrxiv.org/index.php/server/preprint/download/1095/2330", (200, PDF, preprint_pdf(SRX_1095)))
    _, out = env.run([{"doi": "10.1016/j.x.3", "title": SRX_1095, "year": "2026"}])
    assert out[0]["downloaded"] == "True" and out[0]["match_id"] == "10.51224/sportrxiv.1095"
    getrec = [u for u in env.web.to("sportrxiv.org") if "GetRecord" in u]
    assert len(getrec) == 1 and q(getrec[0], "identifier") == "oai:ojs.scholarsportal.info:preprint/1095"
    assert_contract(out)


def test_a_51224_doi_is_looked_up_on_crossref_works(env):
    env.sources("sportrxiv")
    route_oai(env.web, getrecord=lambda url: (200, {}, fx("sportrxiv_oai_getrecord.xml").replace(b"1079", b"997")))
    env.web.add("https://api.crossref.org/works/10.51224/sportrxiv.997", (200, JSON, fx("crossref_work_sportrxiv_997.json")))
    title = fxj("crossref_work_sportrxiv_997.json")["message"]["title"][0]
    env.web.add("https://sportrxiv.org/index.php/server/preprint/download/997/2330", (200, PDF, preprint_pdf(title)))
    # the cache has no 997: Crossref, then GetRecord, then the galley
    _, out = env.run([{"doi": "10.51224/SportRxiv.997", "title": title, "year": "2026"}])
    assert out[0]["downloaded"] == "True" and out[0]["similarity"] == "1.00"
    assert_contract(out)


# ================================================================ identity, holdings, files
def test_a_held_file_with_no_readable_doi_does_not_block_the_fetch(env):
    env.sources("osf")
    route_osf_ballet(env.web)
    name = P.slug_filename("2021", "Shaw J", BALLET)
    squatter = make_pdf("Lecture notes on a different topic entirely\nno identifier here")
    (env.lib / name).write_bytes(squatter)
    _, out = env.run([{"doi": "10.1123/ijspp.2021.1", "title": BALLET, "year": "2021", "authors": "Shaw J"}])
    assert out[0]["downloaded"] == "True" and out[0]["skipped"] == "False"
    assert out[0]["preprint_filename"] != name                       # a disambiguated name
    assert (env.lib / name).read_bytes() == squatter                 # the squatter is untouched
    assert_contract(out)


@pytest.mark.parametrize("namer", [P.slug_filename, P.legacy_slug_filename], ids=["dec15_name", "legacy_name"])
def test_a_held_file_for_this_doi_is_already_exists_with_no_request(env, namer):
    env.sources("osf", "arxiv", "europepmc_preprints")
    name = namer("2021", "van der Walt JHA", BALLET)
    (env.lib / name).write_bytes(make_pdf(BALLET))
    (env.lib / (name[:-4] + ".ris")).write_text("TY  - JOUR\nDO  - 10.1123/ijspp.2021.1\nER  - \n", encoding="utf-8")
    _, out = env.run([{"doi": "10.1123/ijspp.2021.1", "title": BALLET, "year": "2021", "authors": "van der Walt JHA"}])
    assert env.web.sent == []
    assert out[0]["status"] == "ALREADY_EXISTS" and out[0]["skipped"] == "True" and out[0]["outcome"] == "OK"
    assert out[0]["preprint_filename"] == name
    assert_contract(out)


def test_a_wrong_paper_is_flagged_in_place_with_no_ris(env):
    env.sources("osf")
    route_osf_ballet(env.web, pdf=make_pdf("Journal of Other Things\nAn unrelated study of soil microbes\n"
                                           "https://doi.org/10.9999/other.42"))
    _, out = env.run([{"doi": "10.1123/ijspp.2021.1", "title": BALLET, "year": "2021", "authors": "Shaw J"}])
    r = out[0]
    assert r["identity"] == "FLAG" and r["outcome"] == "ERROR" and r["downloaded"] == "False"
    assert r["status"].startswith("DOI_MISMATCH:pdf_doi=10.9999/other.42")
    pdf = env.lib / r["preprint_filename"]
    assert pdf.exists() and json.loads(pdf.with_suffix(".identity.json").read_text(encoding="utf-8"))["identity"] == "FLAG"
    assert env.ris_calls == [] and not (env.lib / "_mismatch").exists()
    assert_contract(out)


def test_a_supplement_is_flagged_too(env):
    env.sources("osf")
    route_osf_ballet(env.web, pdf=make_pdf(f"Supplementary material for\n{BALLET}\nhttps://doi.org/10.31236/osf.io/fkdby"))
    _, out = env.run([{"doi": "10.1123/ijspp.2021.1", "title": BALLET, "year": "2021", "authors": "Shaw J"}])
    assert out[0]["identity"] == "FLAG" and out[0]["doc_kind"] == "SUPPLEMENT"
    assert out[0]["status"].endswith("doc_kind=SUPPLEMENT")
    assert env.ris_calls == []
    assert_contract(out)


def test_no_file_leaves_the_library(env):
    env.sources("osf")
    route_osf_ballet(env.web, pdf=make_pdf("Journal of Other Things\nUnrelated\nhttps://doi.org/10.9999/other.1"))
    keep = {"a.pdf": make_pdf("Lecture notes"), P.slug_filename("2021", "Shaw J", BALLET): make_pdf("Squatter")}
    for n, b in keep.items():
        (env.lib / n).write_bytes(b)
    env.run([{"doi": "10.1123/ijspp.2021.1", "title": BALLET, "year": "2021", "authors": "Shaw J"}])
    env.run([{"doi": "10.1123/ijspp.2021.1", "title": BALLET, "year": "2021", "authors": "Shaw J"}])   # a rerun
    for n, b in keep.items():
        assert (env.lib / n).read_bytes() == b
    assert not (env.lib / "_mismatch").exists()
    flagged = [p for p in env.lib.glob("*.identity.json")]
    assert len(flagged) == 1                                         # rewritten in place, no pile-up


def test_the_quarantine_helpers_are_not_imported():
    assert not hasattr(P, "quarantine_mismatch") and not hasattr(P, "pdf_doi_disagrees")
    src = Path(P.__file__).read_text(encoding="utf-8")
    assert "quarantine_mismatch" not in src and "pdf_doi_disagrees" not in src
    assert not hasattr(P, "EMAIL") and not hasattr(P, "UA")


# ================================================================ prohibited routes, encoding, report
def test_prohibited_routes_are_never_sent(env):
    for url in ("https://europepmc.org/articles/PMC123?pdf=render",
                "https://europepmc.org/api/fulltextRepo?pprId=PPR1114009&type=FILE",
                "https://arxiv.org/pdf/2103.00020", "https://www.biorxiv.org/content/10.1101/x.full.pdf",
                "https://www.medrxiv.org/content/10.1101/x.full.pdf"):
        o = P._download(url, state=None, cfg=env.cfg, purpose="test")
        assert o.kind is Kind.NOT_AVAILABLE and o.detail.startswith("PROHIBITED")
    assert env.web.sent == []
    src = Path(P.__file__).read_text(encoding="utf-8")
    assert "pdf=render" not in src.replace("`?pdf=render`", "")


def test_dois_in_urls_are_percent_encoded():
    """Dispatcher amendment (DOI Handbook 2025 4.7): every DOI in a URL path goes through
    litpipe.doi.encode_path (normalised first, so a fragment is cut, then percent-encoded)."""
    sici = "10.1002/(SICI)1097-4636(199601)30:1<1::AID-JBM1>3.0.CO;2-T"
    assert P._doi_url(sici) == "https://doi.org/10.1002/(sici)1097-4636(199601)30:1%3C1::aid-jbm1%3E3.0.co;2-t"
    assert P._doi_url("10.1056/nejmc1113675#sa3") == "https://doi.org/10.1056/nejmc1113675"
    assert P._doi_url("not a doi") == ""


SICI_PATH = "10.1002/(sici)1097-4636(199601)30:1%3C1::aid-jbm1%3E3.0.co;2-t"


def test_details_and_pdf_urls_encode_the_doi(env):
    sici = "10.1002/(SICI)1097-4636(199601)30:1<1::AID-JBM1>3.0.CO;2-T"
    env.web.add(f"https://api.biorxiv.org/details/biorxiv/{SICI_PATH}/na/json", (200, JSON, fx("biorxiv_details_64898.json")))
    o = P.biorxiv_details("biorxiv", sici, cfg=env.cfg)
    assert o.ok and env.web.sent[0][1] == f"https://api.biorxiv.org/details/biorxiv/{SICI_PATH}/na/json"
    # the switch-gated PDF and the landing link encode it too
    env.sources("biorxiv").switches(biorxiv_pdf_allowed=True)
    rec = dict(epmc_record("epmc_search_64898_biorxiv.json"), doi=sici)
    route_epmc(env.web, keyword=[rec])
    env.web.add(f"https://www.biorxiv.org/content/{SICI_PATH}v2.full.pdf", (200, PDF, preprint_pdf(PAPER_64898)))
    _, out = env.run([{"doi": "10.1152/x.1", "title": PAPER_64898, "year": "2026"}])
    assert f"https://www.biorxiv.org/content/{SICI_PATH}v2.full.pdf" in [u for _, u, _ in env.web.sent]
    assert out[0]["landing_url"] == f"https://doi.org/{SICI_PATH}"


def test_every_row_path_holds_the_contract_in_one_report(env, monkeypatch):
    """OK, ALREADY_EXISTS, FLAG, SKIPPED (both details), REFUSED, NO_MATCH, OUTAGE, DEFERRED, ERROR."""
    env.sources("osf", "biorxiv", "europepmc_preprints")
    env.state.refuse("export.arxiv.org", "manual", persistence="manual")
    route_osf_ballet(env.web)
    rec = epmc_record("epmc_search_64898_biorxiv.json")
    route_epmc(env.web, by_doi={DOI_64898: [rec]})
    env.web.add(f"https://api.biorxiv.org/details/biorxiv/{DOI_64898}/na/json", (200, JSON, b""))
    held = P.slug_filename("2020", "Smith J", "A held paper about heat")
    (env.lib / held).write_bytes(make_pdf("A held paper about heat"))
    (env.lib / (held[:-4] + ".ris")).write_text("DO  - 10.1/held\n", encoding="utf-8")
    rows = [{"doi": "10.1123/ijspp.2021.1", "title": BALLET, "year": "2021", "authors": "Shaw J"},   # OK
            {"doi": "10.1/held", "title": "A held paper about heat", "year": "2020", "authors": "Smith J"},
            {"doi": DOI_64898, "title": PAPER_64898},                                       # OUTAGE
            {"doi": "10.51224/SportRxiv.997", "title": "Goalkeeper"},                     # source_excluded
            {"doi": "10.48550/arXiv.2103.00020", "title": "T"},                           # REFUSED
            {"doi": "10.1/none", "title": ""},                                            # ERROR (no title)
            ]
    _, out = env.run(rows)
    assert [r["outcome"] for r in out] == ["OK", "OK", "OUTAGE", "SKIPPED", "REFUSED", "ERROR"]
    assert_contract(out)
    text = env.report_path.read_text(encoding="utf-8")
    assert "email=" not in text
    assert text.splitlines()[0].split(",") == P.REPORT_FIELDS


def test_help_works_from_a_foreign_cwd(tmp_path):
    """The one subprocess this stage's tests run (--help only: no registry, state or network)."""
    import subprocess
    import sys
    p = subprocess.run([sys.executable, str(Path(P.__file__).resolve()), "--help"], cwd=str(tmp_path),
                       capture_output=True, text=True, timeout=120)
    assert p.returncode == 0, p.stderr
    for flag in ("--triage", "--lib-dir", "--min-similarity", "--limit", "--dry-run", "--report",
                 "--no-write-ris", "--project", "--sources", "--row-deadline"):
        assert flag in p.stdout


def test_main_in_process_uses_the_registry_file(env, net_env, capsys):
    env.sources("osf")
    route_osf_ballet(env.web)
    net_env.cfg_path.write_text(json.dumps(env.cfg), encoding="utf-8")
    triage = env.tmp / "t.csv"
    triage.write_text("doi,title,year,authors\n10.1123/x.9," + BALLET.replace(",", "") + ",2021,Shaw J\n",
                      encoding="utf-8")
    report = env.tmp / "r.csv"
    code = P.main(["--triage", str(triage), "--lib-dir", str(env.lib), "--report", str(report),
                   "--project", "research_alpha", "--no-write-ris", "--row-deadline", "0"])
    assert code == 0
    rows = list(csv.DictReader(open(report, encoding="utf-8")))
    assert rows[0]["downloaded"] == "True" and env.ris_calls == []
    assert P.main(["--triage", str(env.tmp / "missing.csv"), "--lib-dir", str(env.lib),
                   "--report", str(report), "--project", "research_alpha"]) == P.EXIT_NO_TRIAGE
