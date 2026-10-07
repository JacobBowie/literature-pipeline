"""The backward walk (reverse_citations, W3-B): the References parser fixes (V2-N1, V2-N3, V2-N4),
the union of S2, OpenAlex, Crossref, the JATS sidecar and the regex, the seed's own DOI, arXiv IDs,
typed failures that keep prior rows, the degraded and aborted exits, the cadence, the W3-C1 output
seam, and `--sources regex` as the offline run it always was.

Offline: WalkStub answers like api.semanticscholar.org, api.openalex.org and api.crossref.org on
litpipe.net's stub transport (net_env: a virtual clock, a FakeState, a temp ledger and registry).
Libraries are temp folders of stub PDFs with `.ris` DOIs; text dumps come from
tests/fixtures/W3-B/regex_lib/ (synthetic lists), the OpenAlex and Crossref answers from the
recorded V2 bodies in tests/fixtures/W3-B/."""
import csv
import hashlib
import io
import json
import shutil
import statistics
from collections import Counter
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

import pytest

import lit_util
import reverse_citations as rc
from litpipe import config, hosts, net, s2
from litpipe import openalex as oa
from tests.test_w3b_openalex import OAStub, _raw

FIX = Path(__file__).parent / "fixtures" / "W3-B"
LIB_FIX = FIX / "regex_lib"
S2_SECRET = "s2-TESTKEY-w3b-1a2b3c4d5e6f"
OA_SECRET = "oa-TESTKEY-w3b-6f5e4d3c2b1a"
MANIFEST = json.loads((LIB_FIX / "manifest.json").read_text(encoding="utf-8"))


# ------------------------------------------------------------------------------ the stub
def pid(doi):
    return hashlib.sha1(doi.encode()).hexdigest()


def cited(i, doi=None, arxiv=None, title=None):
    ext = {}
    if doi:
        ext["DOI"] = doi
    if arxiv:
        ext["ArXiv"] = arxiv
    return {"paperId": pid(f"ref{i}{doi}"), "externalIds": ext, "title": title or f"Cited paper {i}",
            "year": 2001 + i % 20, "authors": [{"authorId": "1", "name": f"Ann Writer{i}"}]}


class WalkStub:
    """S2 (`s2[doi] = {"count": n, "refs": list | None}`; None is the elision marker), OpenAlex
    (`oa`, an OAStub) and Crossref (`cr[doi] = item`). `s2_status` / `cr_status` fail every call of
    that host; `s2_get_fail` = {paperId: status} fails one seed's paged re-fetch."""

    def __init__(self):
        self.oa = OAStub()
        self.s2 = {}
        self.cr = {}
        self.sent = []
        self.s2_status = None
        self.cr_status = None
        self.s2_get_fail = {}

    def hosts(self):
        return Counter(h for h, *_ in self.sent)

    def __call__(self, method, url, hdrs, body, timeout, max_bytes):
        parts = urlsplit(url)
        host = parts.hostname
        self.sent.append((host, method, url, dict(hdrs)))
        if host == "api.semanticscholar.org":
            return self._s2(method, parts, body)
        if host in ("api.openalex.org", "content.openalex.org"):
            return self.oa(method, url, hdrs, body, timeout, max_bytes)
        if host == "api.crossref.org":
            return self._cr(parts)
        raise AssertionError(f"unexpected host {host}")

    def _s2(self, method, parts, body):
        if self.s2_status:
            return _raw(self.s2_status, {"message": "Too Many Requests"})
        q = {k: v[-1] for k, v in parse_qs(parts.query).items()}
        path = unquote(parts.path)
        if path == "/graph/v1/paper/batch":
            ids = json.loads(body)["ids"]
            out = []
            for i in ids:
                p = self.s2.get(i[len("DOI:"):].lower())
                if p is None:
                    out.append(None)
                elif q["fields"] == "referenceCount":
                    out.append({"paperId": pid(i), "referenceCount": p["count"]})
                else:
                    out.append({"paperId": pid(i), "referenceCount": p["count"], "references": p["refs"]})
            return _raw(200, out)
        if path.endswith("/references"):
            key = path[len("/graph/v1/paper/"):-len("/references")]     # a DOI keeps its '/'
            doi = key[len("DOI:"):].lower() if key.startswith("DOI:") else key
            if pid("DOI:" + doi) in self.s2_get_fail:
                return _raw(self.s2_get_fail[pid("DOI:" + doi)], {"message": "scripted"})
            p = self.s2[doi]
            off, lim = int(q["offset"]), int(q["limit"])
            rows = p.get("full", p["refs"]) or []
            page = {"offset": off, "data": [{"citedPaper": r} for r in rows[off:off + lim]]}
            if off + lim < len(rows):
                page["next"] = off + lim
            return _raw(200, page)
        return _raw(404, {"error": "no route"})

    def _cr(self, parts):
        if self.cr_status:
            return _raw(self.cr_status, {"status": "error"})
        q = {k: v[-1] for k, v in parse_qs(parts.query).items()}
        dois = [f[len("doi:"):].lower() for f in q["filter"].split(",")]
        items = [self.cr[d] for d in dois if d in self.cr]
        return _raw(200, {"status": "ok", "message-type": "work-list",
                          "message": {"total-results": len(items), "items": items}})


# ------------------------------------------------------------------------------ fixtures
@pytest.fixture
def env(net_env, monkeypatch, tmp_path):
    monkeypatch.setenv(oa.KEY_ENV, OA_SECRET)
    monkeypatch.delenv(s2.KEY_ENV, raising=False)
    monkeypatch.setattr(s2, "BASE", "https://api.semanticscholar.org")
    s2.reset_default_session()
    oa.reset_default_session()
    hosts.reset()
    root = tmp_path / "root"
    (root / "research_a" / "lit").mkdir(parents=True)
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    net_env.write_config(projects={"research_a": {"lib_dir": "lit", "data_dir": "data"}})
    monkeypatch.setattr(rc, "CONFIG_PATH", net_env.cfg_path)
    net_env.root = root
    net_env.lib = root / "research_a" / "lit"
    net_env.text = root / "research_a" / "data" / "text"
    yield net_env
    s2.reset_default_session()
    oa.reset_default_session()
    hosts.reset()


@pytest.fixture
def stub(env, monkeypatch):
    st = WalkStub()
    monkeypatch.setitem(net._TRANSPORTS, "requests", st)
    monkeypatch.setitem(net._TRANSPORTS, "urllib", st)
    return st


def add_seed(env, stem, doi=None, text=None, sidecar=None, pdf=b"%PDF-1.4 stub"):
    (env.lib / f"{stem}.pdf").write_bytes(pdf)
    if doi:
        (env.lib / f"{stem}.ris").write_text(f"TY  - JOUR\nDO  - {doi}\nER  - \n", encoding="utf-8")
    if sidecar:
        (env.lib / f"{stem}.fulltext.json").write_text(json.dumps(sidecar), encoding="utf-8")
    if text is not None:
        env.text.mkdir(parents=True, exist_ok=True)
        (env.text / f"{stem}.txt").write_text(text, encoding="utf-8")


def regex_lib(env):
    for stem, m in MANIFEST.items():
        text_file = LIB_FIX / "text" / f"{stem}.txt"
        add_seed(env, stem, m["doi"], text=text_file.read_text(encoding="utf-8") if text_file.exists() else None,
                 sidecar=m.get("sidecar"))


def rows_of(path):
    with open(path, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def outputs(env):
    return rc.output_paths(env.lib / "_reverse_citations")


def sha(p):
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def summary_line(out):
    lines = [ln for ln in out.splitlines() if ln.strip()]
    assert lines[-1].startswith(rc.SUMMARY_MARKER), lines[-3:]
    return json.loads(lines[-1][len(rc.SUMMARY_MARKER):])


# ================================================================ the parser (V2-N1, V2-N3, V2-N4)
@pytest.mark.parametrize("stem", sorted(s for s in MANIFEST if (LIB_FIX / "text" / f"{s}.txt").exists()))
def test_parser_fixtures_give_the_printed_count(stem):
    text = (LIB_FIX / "text" / f"{stem}.txt").read_text(encoding="utf-8")
    chunks = rc.split_refs(rc.locate_refs(text))
    assert len(chunks) == MANIFEST[stem]["printed"], [c[:40] for c in chunks]


def test_v2_n1_the_first_numbered_reference_is_kept():
    for stem, first in (("2020_NumDot", "Author1 A"), ("2020_NumBracket", "Author1 A")):
        chunks = rc.split_refs(rc.locate_refs((LIB_FIX / "text" / f"{stem}.txt").read_text(encoding="utf-8")))
        assert chunks[0].startswith(first) and chunks[-1].startswith("Author8 A")
    # V2 raw/regex_first_ref_drop_test.txt: the blob starts at item 1, with no newline before it
    blob = "".join(f"{i}. Author{i} A, Other B. A title about thing number {i}. J Test.\n" for i in range(1, 9))
    assert len(rc.split_refs(blob)) == 8
    bracket = "".join(f"[{i}] Author{i} A. A title about thing number {i}. J Test.\n" for i in range(1, 9))
    assert len(rc.split_refs(bracket)) == 8


def test_v2_n3_a_header_inside_a_cited_title_is_not_a_header():
    text = (LIB_FIX / "text" / "2020_TitleHeader.txt").read_text(encoding="utf-8")
    hits = [m.group(0).strip() for m in rc.REFS_HDR.finditer(text)]
    assert hits == ["References", "References"]                   # the contents line and the real one
    assert not rc.REFS_HDR.search("Disease of the heart, with special reference\nto congenital lesions")
    blob = rc.locate_refs(text)
    assert blob.lstrip().startswith("1. Author1 A")              # the real header, past the middle
    assert rc.REFS_HDR.search("SDC 1 Table REFERENCES \n") and rc.REFS_HDR.search("in conclusion. References\n")


def test_v2_n4_author_year_wrapped_names_are_one_reference():
    text = (LIB_FIX / "text" / "2020_JPhys.txt").read_text(encoding="utf-8")
    chunks = rc.split_refs(rc.locate_refs(text))
    assert chunks[1].startswith("Ames H") and "Zane PS" in chunks[1]   # "Garcia\nL, Mouse E" is no start
    assert chunks[-1].startswith("O'Brien TJ & van der Berg K.")
    ln = rc.split_refs(rc.locate_refs((LIB_FIX / "text" / "2020_LineNum.txt").read_text(encoding="utf-8")))
    assert not any(c.strip().isdigit() for c in ln) and "709" not in ln[0]


def test_author_year_split_is_linear_on_a_hostile_line():
    hostile = "Smith AB, " * 400 + "and no year at all\n"
    import time
    t0 = time.perf_counter()
    rc.split_author_year(hostile * 20)
    assert time.perf_counter() - t0 < 2.0


def test_arxiv_ids_map_to_10_48550_dois():
    assert rc.arxiv_doi("Writer E. Attention. arXiv:1706.03762v5, 2017.") == "10.48550/arxiv.1706.03762"
    assert rc.arxiv_doi("see https://arxiv.org/abs/2305.14152v2") == "10.48550/arxiv.2305.14152"
    assert rc.arxiv_doi("arXiv preprint hep-th/9901001 (1999)") == "10.48550/arxiv.hep-th/9901001"
    assert rc.arxiv_doi("CoRR abs/1412.6980") == "10.48550/arxiv.1412.6980"
    assert rc.arxiv_doi("Physiol Rev 2004;84:1-2.") == ""


# ================================================================ --sources regex: the offline run
def test_sources_regex_sends_nothing_and_matches_the_ce0479b_output(stub, env):
    regex_lib(env)
    res = rc.run(project="research_a", sources="regex")
    assert res["exit_code"] == 0 and res["published"]
    assert stub.sent == [] and env.state.calls == [] and not env.ledger_lines()
    p = outputs(env)
    with open(p["parsed"], encoding="utf-8", newline="") as f:
        header = next(csv.reader(f))
    assert header == rc.LEGACY_FIELDS + ["seed_doi", "source"] == rc.FIELDS
    new = rows_of(p["parsed"])
    old = rows_of(FIX / "regex_lib_golden_ce0479b.csv")
    legacy = lambda rs, seed: [{k: r[k] for k in rc.LEGACY_FIELDS} for r in rs if r["seed"] == seed]  # noqa: E731
    for seed in ("2020_Blank.pdf", "2020_Sidecar.pdf"):           # untouched by the fixes: identical
        assert legacy(new, seed) == legacy(old, seed)
    nb = legacy(new, "2020_NumBracket.pdf")
    assert nb[1:] == legacy(old, "2020_NumBracket.pdf") and nb[0]["raw"].startswith("Author1 A")   # V2-N1
    for seed, m in MANIFEST.items():
        got = [r for r in new if r["seed"] == f"{seed}.pdf"]
        assert len(got) == m["printed"]
        assert all(r["seed_doi"] == m["doi"] for r in got)
        assert {r["source"] for r in got} == ({"sidecar"} if m.get("sidecar") else {"regex"})
    assert sum(1 for r in old) < len(new)


def test_the_seed_doi_never_appears_and_arxiv_references_get_dois(stub, env):
    regex_lib(env)
    rc.run(project="research_a", sources="regex")
    p = outputs(env)
    rows = rows_of(p["parsed"])
    seeds = {m["doi"] for m in MANIFEST.values()}
    assert not any(r["doi"] == r["seed_doi"] for r in rows)
    numdot = [r for r in rows if r["seed"] == "2020_NumDot.pdf"]
    assert [r["doi"] for r in numdot] == ["", "", "10.5555/ref.0003", "", "10.48550/arxiv.1706.03762",
                                          "10.48550/arxiv.hep-th/9901001", "", ""]
    unique = {r["doi"] for r in rows_of(p["unique"])}
    assert not unique & seeds and "10.48550/arxiv.1706.03762" in unique


# ================================================================ the network legs
def _core15(stub, env):
    seeds = json.loads((FIX / "oa_core15_seeds.json").read_text(encoding="utf-8"))
    resolved = json.loads((FIX / "oa_core15_resolved.json").read_text(encoding="utf-8"))
    metrics = json.loads((FIX / "core15_metrics.json").read_text(encoding="utf-8"))
    stub.oa.add(*seeds["results"], *resolved["results"])
    for i, (doi, m) in enumerate(metrics["per_paper"].items()):
        add_seed(env, f"2020_Core{i:02d}", doi)
        stub.s2[doi] = {"count": m["s2_referenceCount"], "refs": None}       # elided by the publisher
    return metrics


def test_core15_elided_seeds_get_openalex_references_at_the_audited_coverage(stub, env):
    metrics = _core15(stub, env)
    res = rc.run(project="research_a")
    assert res["exit_code"] == 0, res["reasons"]
    assert res["answered"] == {"s2": 0, "openalex": 15, "crossref": 0}
    assert stub.hosts()["api.crossref.org"] == 0
    rows = rows_of(outputs(env)["parsed"])
    assert {r["source"] for r in rows} == {"openalex"}
    cov = []
    for i, (doi, m) in enumerate(metrics["per_paper"].items()):
        mine = [r for r in rows if r["seed"] == f"2020_Core{i:02d}.pdf"]
        assert all(r["seed_doi"] == doi and r["doi"] != doi for r in mine)
        cov.append((len(mine) + m["n_openalex_dangling"]) / m["n_printed_used"])
    assert statistics.median(cov) == pytest.approx(1.0, abs=0.03)
    assert res["openalex"]["calls"] >= 2 and res["s2"]["calls"] == 2       # one count batch, one nested


def test_a_count_of_0_falls_through_to_crossref_deduplicated(stub, env):
    seeds = json.loads((FIX / "oa_core15_seeds.json").read_text(encoding="utf-8"))
    cr = json.loads((FIX / "cr_refs_trimmed.json").read_text(encoding="utf-8"))["items"]
    stub.oa.add(seeds["sms_count0"])                              # referenced_works_count 0 (V2-N8)
    stub.cr = {it["DOI"].lower(): it for it in cr}
    for i, doi in enumerate(("10.1111/sms.70236", "10.1123/jpah.2018-0627", "10.1123/ijspp.2018-0399",
                             "10.1152/jappl.1973.35.2.236")):
        add_seed(env, f"2020_Fall{i}", doi)
        stub.s2[doi] = {"count": 40, "refs": None}
    res = rc.run(project="research_a")
    assert res["exit_code"] == 0, res["reasons"]
    rows = rows_of(outputs(env)["parsed"])
    by = Counter(r["seed"] for r in rows)
    assert {r["source"] for r in rows} == {"crossref"}
    assert by["2020_Fall0.pdf"] == 47                             # OpenAlex says 0: Crossref has 47
    assert by["2020_Fall1.pdf"] == 25 and by["2020_Fall2.pdf"] == 34  # HK: 50 and 68 entries, each twice
    assert by["2020_Fall3.pdf"] == 0                              # no deposit, no OpenAlex: api_empty
    assert res["api_empty"] == 1 and res["answered"]["crossref"] == 3
    assert all(r["doi"] != r["seed_doi"] for r in rows)


def test_s2_rows_union_the_regex_dois_they_lack(stub, env):
    seed = "10.5555/seed.0100"
    text = (LIB_FIX / "text" / "2020_NumDot.txt").read_text(encoding="utf-8").replace("10.5555/seed.0001", seed)
    add_seed(env, "2020_Union", seed, text=text)
    refs = [cited(1, "10.5555/ref.0003"), cited(2, "10.5555/S2ONLY.0002"), cited(3, seed),
            cited(4, arxiv="2305.14152"), cited(5)]
    stub.s2[seed] = {"count": 5, "refs": refs}
    res = rc.run(project="research_a")
    assert res["exit_code"] == 0 and res["answered"]["s2"] == 1
    rows = rows_of(outputs(env)["parsed"])
    got = [(r["source"], r["doi"]) for r in rows]
    assert ("s2", "10.5555/ref.0003") in got and ("s2", "10.5555/s2only.0002") in got
    assert ("s2", "10.48550/arxiv.2305.14152") in got and ("s2", "") in got
    assert seed not in {d for _, d in got}                         # S2's self-reference dropped
    assert ("regex", "10.5555/ref.0003") not in got               # the same reference once
    assert ("regex", "10.48550/arxiv.1706.03762") in got           # a DOI S2 lacks is added
    assert not any(s == "regex" and d == "" for s, d in got)      # undoied regex chunks only count
    assert stub.hosts()["api.openalex.org"] == 0 and stub.hosts()["api.crossref.org"] == 0


# ================================================================ failures, exits, outputs
def _publish_core(stub, env, n=30):
    for i in range(n):
        doi = f"10.5555/seed.{i:04d}"
        add_seed(env, f"2020_Seed{i:04d}", doi)
        stub.s2[doi] = {"count": 2, "refs": [cited(1, f"10.5555/r.{i}.1"), cited(2, f"10.5555/r.{i}.2")]}
    res = rc.run(project="research_a")
    assert res["exit_code"] == 0
    return {k: sha(v) for k, v in outputs(env).items() if k != "degraded"}


def test_a_throttled_walk_keeps_the_outputs_byte_identical_and_writes_degraded(stub, env, capsys):
    before = _publish_core(stub, env)
    capsys.readouterr()
    stub.s2_status = 429
    n_oa = stub.hosts()["api.openalex.org"]
    res = rc.run(project="research_a", refresh=True)
    assert res["exit_code"] == 2 and res["failed"] == 30 and res["transport_failures"] == 30
    assert {k: sha(v) for k, v in outputs(env).items() if k != "degraded"} == before
    deg = rows_of(outputs(env)["degraded"])
    assert len(deg) == 60 and {r["source"] for r in deg} == {"s2"}      # the failed seeds kept their rows
    assert stub.hosts()["api.openalex.org"] == n_oa                     # no fallback for a failed S2 walk
    summ = summary_line(capsys.readouterr().out)
    assert summ["exit_code"] == 2 and summ["reasons"] and summ["aborted"] is None
    import snowball
    assert snowball.degraded_exit(2, summ)


def test_a_failed_seed_under_5_percent_keeps_its_published_rows(stub, env):
    _publish_core(stub, env, n=30)
    bad = "10.5555/seed.0007"
    stub.s2[bad] = {"count": 3, "refs": [cited(9, "10.5555/new.1")], "full": None}
    stub.s2_get_fail[pid("DOI:" + bad)] = 500                     # the re-fetch of the short list fails
    res = rc.run(project="research_a", refresh=True)
    assert res["exit_code"] == 0 and res["failed"] == 1 and res["transport_failures"] == 1
    rows = rows_of(outputs(env)["parsed"])
    kept = sorted(r["doi"] for r in rows if r["seed_doi"] == bad)
    assert kept == ["10.5555/r.7.1", "10.5555/r.7.2"]               # prior rows, never an empty list


def test_openalex_budget_spent_aborts_with_exit_3(stub, env, capsys):
    for i in range(3):
        doi = f"10.5555/el.{i:04d}"
        add_seed(env, f"2020_El{i}", doi)
        stub.s2[doi] = {"count": 5, "refs": None}
    stub.oa.headers = {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "900"}
    stub.oa.add({"id": "https://openalex.org/W1", "doi": "https://doi.org/10.5555/el.0000",
                 "referenced_works": ["https://openalex.org/W2"], "referenced_works_count": 1},
                {"id": "https://openalex.org/W2", "doi": "https://doi.org/10.5555/x.2"})
    (env.lib / "_reverse_citations_parsed.csv").write_text("seed,first_author,year,title_snippet,doi,raw\n",
                                                           encoding="utf-8")
    before = sha(env.lib / "_reverse_citations_parsed.csv")
    res = rc.run(project="research_a")
    assert res["exit_code"] == 3 and "openalex:budget" in res["aborted"]
    assert sha(env.lib / "_reverse_citations_parsed.csv") == before
    assert outputs(env)["degraded"].exists()
    assert env.state.deferred["api.openalex.org"] == pytest.approx(env.clock.t + 900, abs=5)
    summ = summary_line(capsys.readouterr().out)
    assert summ["aborted"] and "transport_failures" in summ and summ["reasons"]


def test_fewer_seeds_with_references_is_degraded_unless_forced(stub, env):
    _publish_core(stub, env, n=4)
    for i in range(4):
        stub.s2[f"10.5555/seed.{i:04d}"] = {"count": 0, "refs": []}
    stub.oa.add()                                                  # nothing in OpenAlex either
    res = rc.run(project="research_a", refresh=True)
    assert res["exit_code"] == 2 and "fewer than the published" in res["reasons"][0]
    assert rc.run(project="research_a", refresh=True, force=True)["exit_code"] == 0


def test_limit_walks_the_first_seeds_and_carries_the_rest(stub, env):
    _publish_core(stub, env, n=3)
    stub.s2["10.5555/seed.0000"]["refs"] = [cited(7, "10.5555/changed.1")]
    stub.s2["10.5555/seed.0000"]["count"] = 1
    stub.s2["10.5555/seed.0002"]["refs"] = [cited(8, "10.5555/changed.2")]
    res = rc.run(project="research_a", limit=1, refresh=True)
    assert res["exit_code"] == 0 and res["walked_pdfs"] == 1
    rows = rows_of(outputs(env)["parsed"])
    assert sorted(r["doi"] for r in rows if r["seed"] == "2020_Seed0000.pdf") == ["10.5555/changed.1"]
    assert sorted(r["doi"] for r in rows if r["seed"] == "2020_Seed0002.pdf") == ["10.5555/r.2.1", "10.5555/r.2.2"]


# ================================================================ keys, cadence, sources
def test_no_openalex_key_skips_the_leg_with_one_notice_and_never_exits_1(stub, env, monkeypatch, capsys):
    monkeypatch.delenv(oa.KEY_ENV)
    for i in range(2):
        add_seed(env, f"2020_El{i}", f"10.5555/el.{i:04d}")
        stub.s2[f"10.5555/el.{i:04d}"] = {"count": 5, "refs": None}
    res = rc.run(project="research_a")
    out = capsys.readouterr().out
    assert res["exit_code"] == 0
    assert out.count("OPENALEX_API_KEY is not set") == 1
    assert stub.hosts()["api.openalex.org"] == 0 and stub.hosts()["api.crossref.org"] == 1
    assert res["skipped"] == 2 and res["api_empty"] == 0               # a skipped source is not "empty"


def test_a_lower_source_never_replaces_a_higher_one_that_could_not_answer(stub, env, monkeypatch):
    doi = "10.5555/el.0001"
    add_seed(env, "2020_El", doi)
    stub.s2[doi] = {"count": 5, "refs": None}
    stub.oa.add({"id": "https://openalex.org/W1", "doi": f"https://doi.org/{doi}",
                 "referenced_works": ["https://openalex.org/W2"], "referenced_works_count": 1},
                {"id": "https://openalex.org/W2", "doi": "https://doi.org/10.5555/oa.2"})
    assert rc.run(project="research_a")["answered"]["openalex"] == 1
    monkeypatch.delenv(oa.KEY_ENV)
    stub.cr[doi] = {"DOI": doi, "reference": [{"key": "r1", "DOI": "10.5555/cr.1"}]}
    res = rc.run(project="research_a", refresh=True)
    assert res["exit_code"] == 0 and res["kept"] == 1
    rows = rows_of(outputs(env)["parsed"])
    assert [(r["source"], r["doi"]) for r in rows] == [("openalex", "10.5555/oa.2")]


def test_cadence_skips_unchanged_seeds_and_walks_changed_or_stale_ones(stub, env):
    env.write_config(projects={"research_a": {"lib_dir": "lit", "data_dir": "data", "walk_cadence_days": 7}})
    for i in range(2):
        doi = f"10.5555/seed.{i:04d}"
        add_seed(env, f"2020_Seed{i:04d}", doi)
        stub.s2[doi] = {"count": 1, "refs": [cited(i, f"10.5555/r.{i}")]}
    assert rc.run(project="research_a")["exit_code"] == 0
    n = len(stub.sent)
    res = rc.run(project="research_a")
    assert res["exit_code"] == 0 and res["cadence_skipped"] == 2 and len(stub.sent) == n
    assert len(rows_of(outputs(env)["parsed"])) == 2
    (env.lib / "2020_Seed0001.pdf").write_bytes(b"%PDF-1.4 a new version of the file")
    res = rc.run(project="research_a")
    assert res["cadence_skipped"] == 1 and res["network_walks"] == 1
    env.clock.t += 8 * 86400                                          # past walk_cadence_days
    assert rc.run(project="research_a")["cadence_skipped"] == 0
    assert rc.run(project="research_a", refresh=True)["cadence_skipped"] == 0


def test_cadence_default_is_30_days_for_a_lib_dir_run(stub, env):
    add_seed(env, "2020_Seed0000", "10.5555/seed.0000")
    stub.s2["10.5555/seed.0000"] = {"count": 1, "refs": [cited(1, "10.5555/r.1")]}
    rc.run(lib_dir=str(env.lib))
    env.clock.t += 29 * 86400
    assert rc.run(lib_dir=str(env.lib))["cadence_skipped"] == 1
    env.clock.t += 2 * 86400
    assert rc.run(lib_dir=str(env.lib))["cadence_skipped"] == 0


def test_no_key_is_written_anywhere(stub, env, monkeypatch, capsys):
    monkeypatch.setenv(s2.KEY_ENV, S2_SECRET)
    _core15(stub, env)
    res = rc.run(project="research_a")
    assert res["exit_code"] == 0
    out = capsys.readouterr()
    texts = [out.out, out.err, env.ledger_text()] + [p.read_text(encoding="utf-8", errors="replace")
                                                     for p in env.lib.iterdir() if p.is_file()]
    for t in texts:
        assert S2_SECRET not in t and OA_SECRET not in t
    for host, _, url, hdrs in stub.sent:
        assert S2_SECRET not in url and OA_SECRET not in url and "api_key" not in url
    assert any(h.get("Authorization") == f"Bearer {OA_SECRET}" for host, _, _, h in stub.sent if host == "api.openalex.org")


def test_s2_runs_unkeyed_at_6_5_s_on_one_session(stub, env, monkeypatch):
    made = []
    real = s2.Session

    class Counting(real):
        def __init__(self, *a, **k):
            made.append(1)
            super().__init__(*a, **k)
    monkeypatch.setattr(s2, "Session", Counting)
    _publish_core(stub, env, n=3)
    assert made == [1]
    assert hosts.policy("api.semanticscholar.org").min_interval_s == 6.5


# ================================================================ config errors, paths, the seam
def test_config_errors_exit_1_never_2(stub, env, monkeypatch, capsys, tmp_path):
    assert rc.main([]) == 1
    assert rc.main(["--project", "research_b"]) == 1
    assert rc.main(["--lib-dir", str(tmp_path / "missing")]) == 1
    assert rc.main(["--project", "research_a", "--sources", "s2,bogus"]) == 1
    env.write_config(projects={"research_a": {"lib_dir": "lit"}}, s2={"breaker": 0})
    assert rc.main(["--project", "research_a"]) == 1
    monkeypatch.setattr(rc, "CONFIG_PATH", tmp_path / "nowhere" / "projects.json")
    assert rc.main(["--project", "research_a"]) == 1
    assert rc.SUMMARY_MARKER not in capsys.readouterr().out
    assert stub.sent == []


def test_a_rejected_openalex_key_is_a_config_error(stub, env):
    add_seed(env, "2020_El", "10.5555/el.0001")
    stub.s2["10.5555/el.0001"] = {"count": 5, "refs": None}
    stub.oa.script(1, 401, body={"error": "Unauthorized", "message": "invalid api key"})
    res = rc.run(project="research_a")
    assert res["exit_code"] == 1 and res["reasons"][0].startswith("configuration: openalex")
    assert outputs(env)["degraded"].exists() and not outputs(env)["parsed"].exists()


def test_a_dotted_out_prefix_keeps_its_name_for_every_output(stub, env):
    regex_lib(env)
    prefix = env.root / "out.v2" / "reverse.v2"
    assert rc.run(project="research_a", sources="regex", out_prefix=str(prefix))["exit_code"] == 0
    names = sorted(p.name for p in prefix.parent.iterdir())
    assert names == ["reverse.v2.jsonl", "reverse.v2_parsed.csv", "reverse.v2_unique.csv"]
    assert rc.output_paths(prefix)["degraded"].name == "reverse.v2_parsed.degraded.csv"
    line = json.loads((prefix.parent / "reverse.v2.jsonl").read_text(encoding="utf-8").splitlines()[0])
    assert {"seed", "raw", "doi", "seed_doi", "source", "kept"} <= set(line)


def test_a_legacy_six_column_csv_is_read_and_its_rows_carried(stub, env):
    add_seed(env, "2020_Old", "10.5555/old.0001")
    (env.lib / "_reverse_citations_parsed.csv").write_text(
        "seed,first_author,year,title_snippet,doi,raw\r\n2020_Old.pdf,Smith,2001,T,10.5555/x.1,raw\r\n",
        encoding="utf-8", newline="")
    stub.s2_status = 503                                             # S2 fails: the legacy row is carried
    res = rc.run(project="research_a", sources="s2")
    assert res["failed"] == 1 and res["exit_code"] == 2
    deg = rows_of(outputs(env)["degraded"])
    assert deg == [{"seed": "2020_Old.pdf", "first_author": "Smith", "year": "2001", "title_snippet": "T",
                    "doi": "10.5555/x.1", "raw": "raw", "seed_doi": "10.5555/old.0001", "source": "regex"}]


def test_index_portfolio_ingests_the_new_parsed_csv(stub, env, tmp_path):
    import duckdb
    import index_portfolio as I
    regex_lib(env)
    rc.run(project="research_a", sources="regex")
    con = duckdb.connect(str(tmp_path / "t.duckdb"))
    con.execute(I.SCHEMA)
    I.ingest_reverse(con, "research_a", outputs(env)["parsed"], env.lib)
    got = {r[0] for r in con.execute("SELECT doi FROM candidates WHERE source_project='research_a'").fetchall()}
    assert "10.48550/arxiv.1706.03762" in got and "10.5555/side.0001" in got
    assert not got & {m["doi"] for m in MANIFEST.values()}
    con.close()


def test_help_runs():
    import subprocess
    import sys
    repo = Path(rc.__file__).resolve().parent
    for script in ("reverse_citations.py", "backfills/placeholder_edges.py"):
        r = subprocess.run([sys.executable, str(repo / script), "--help"], capture_output=True, text=True,
                           encoding="utf-8", timeout=60)
        assert r.returncode == 0 and "usage" in r.stdout.lower()
