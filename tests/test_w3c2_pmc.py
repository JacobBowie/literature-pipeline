"""W3-C2: the seeder's --pmc-check (skill hard rule 5) through the REAL lit_net.doi_to_pmcid.

lit_net.IDCONV, EPMC_SEARCH and EUTILS point at a MockServer; pmc.ncbi.nlm.nih.gov is refused
manually in the temp state (with the arXiv hosts), so idconv is never called and Europe PMC answers
(the recorded 2026-10-06 probe answer); E-utilities decides what Europe PMC did not place.
- pmc_status is OK when a PMCID was found, else lit_net.lookup_status: a refused or failed lookup
  is never NO_PMCID, and makes the run exit 2 with "[step-summary]" as the last stdout line.
- Without the flag nothing is sent and the draft has the six contract columns only.
"""
import json
from pathlib import Path

import pytest

import lit_net
import seed_queue_from_top_candidates as S
from litpipe.outcomes import Kind, Outcome
from tests.netmock import Reply
from tests.test_seeder import World, dois_of, summary_of

FIX = Path(__file__).resolve().parent / "fixtures" / "W3-C2"
EPMC_BODY = (FIX / "epmc_search_lite_2026-10-06.json").read_text(encoding="utf-8")
JSON = {"Content-Type": "application/json"}

OA_DOI, OA_PMC = "10.3390/nu8060377", "PMC4924218"
AM_DOI, AM_PMC = "10.1016/j.resp.2021.103638", "PMC7983342"
NEW_DOI, NEW_PMC = "10.21037/jss-20262-05", "PMC13598418"
MISS_DOI = "10.1007/s40279-015-0365-0"                      # Europe PMC lists it without a PMCID
ARXIV = ("export.arxiv.org", "arxiv.org", "www.arxiv.org")


@pytest.fixture
def pmc_world(tmp_path, monkeypatch, capsys, net_env, mock_server):
    (tmp_path / "w").mkdir()
    w = World(tmp_path / "w", monkeypatch, capsys)
    srv = mock_server()
    monkeypatch.setattr(lit_net, "IDCONV", srv.url("/idconv/"))
    monkeypatch.setattr(lit_net, "EPMC_SEARCH", srv.url("/epmc/search"))
    monkeypatch.setattr(lit_net, "EUTILS", srv.url("/eutils"))
    for h in ARXIV:
        net_env.state.refuse(h, "manual: refused since 2026-09-25", persistence="manual")
    net_env.state.refuse("pmc.ncbi.nlm.nih.gov", "manual: test", persistence="manual")
    srv.script("/epmc/search", Reply(200, EPMC_BODY, JSON))
    srv.script("/eutils/esearch.fcgi", Reply(200, json.dumps({"esearchresult": {"count": "0", "idlist": []}}), JSON))
    w.srv, w.net_env = srv, net_env
    w.register("research_a", "literature")
    seeds = [f"10.5555/a.{i:04d}" for i in range(3)]
    w.hold("research_a", *seeds)
    # three own seeds cite the OA paper, two the AM, one each the new and the missing paper
    edges = ([(s, OA_DOI) for s in seeds] + [(s, AM_DOI) for s in seeds[:2]]
             + [(seeds[0], NEW_DOI), (seeds[0], MISS_DOI)])
    w.reverse("research_a", edges)
    w.index()
    return w


def _draft(w):
    return w.draft(w.proot("research_a") / "lit_pull_queue.draft.csv")


def test_pmc_check_with_idconv_refused_gets_europe_pmc_answers(pmc_world):
    w = pmc_world
    rc, out, err = w.seed("--project", "research_a", "--pmc-check")
    assert rc == 0, err
    head, fields, rows = _draft(w)
    assert fields == S.BASE_COLUMNS + ["pmcid", "pmc_status"]
    got = {r["doi"]: (r["pmcid"], r["pmc_status"]) for r in rows}
    assert got == {OA_DOI: (OA_PMC, "OK"), AM_DOI: (AM_PMC, "OK"), NEW_DOI: (NEW_PMC, "OK"),
                   MISS_DOI: ("", "NO_PMCID")}                              # E-utilities answered: not in PMC
    assert dois_of(rows) == [OA_DOI, AM_DOI, MISS_DOI, NEW_DOI]            # rank order kept
    assert not w.srv.hits_for("/idconv/")                                  # refused: never called
    assert len(w.srv.hits_for("/epmc/search")) == 1                        # one batched call
    esearch = w.srv.hits_for("/eutils/esearch.fcgi")
    assert len(esearch) == 1 and MISS_DOI in esearch[0].query["term"][0] and OA_DOI not in esearch[0].query["term"][0]
    s = summary_of(out)
    assert s["exit_code"] == 0 and s["pmc"]["failed"] == 0 and s["transport_failures"] == 0
    assert any(ln.startswith("# PMC check: 4 of 4 answered") for ln in head)
    hosts = {ln.get("host") for ln in w.net_env.ledger_lines()}
    assert not any("arxiv" in str(h) for h in hosts) and "pmc.ncbi.nlm.nih.gov" not in hosts


def test_failed_lookup_is_not_no_pmcid_and_exits_2_with_the_summary_last(pmc_world):
    w = pmc_world
    w.srv.script("/epmc/search", Reply(503, ""))
    w.srv.script("/eutils/esearch.fcgi", Reply(503, ""))
    rc, out, err = w.seed("--project", "research_a", "--pmc-check")
    assert rc == 2
    _, _, rows = _draft(w)                                                  # the draft is still written
    assert len(rows) == 4
    for r in rows:
        assert r["pmcid"] == "" and r["pmc_status"] != "NO_PMCID" and r["pmc_status"].startswith("REFUSED")
    s = summary_of(out)                                                     # the last stdout line
    assert s["exit_code"] == 2 and s["aborted"] is None and isinstance(s["transport_failures"], int)
    assert s["reasons"] == ["pmc-check: 4 of 4 lookup(s) failed (REFUSED: 4)"]
    assert not w.srv.hits_for("/idconv/")


def test_a_partial_failure_keeps_the_answers_and_names_the_failures(pmc_world):
    w = pmc_world
    w.srv.script("/eutils/esearch.fcgi", Reply(503, ""))
    rc, out, err = w.seed("--project", "research_a", "--pmc-check")
    assert rc == 2
    _, _, rows = _draft(w)
    got = {r["doi"]: r["pmc_status"] for r in rows}
    assert got[OA_DOI] == got[AM_DOI] == got[NEW_DOI] == "OK"
    assert got[MISS_DOI].startswith("REFUSED") and "epmc_search:no PMCID" in got[MISS_DOI]
    assert summary_of(out)["reasons"] == ["pmc-check: 1 of 4 lookup(s) failed (REFUSED: 1)"]


def test_no_network_without_the_flag(pmc_world):
    w = pmc_world
    rc, out, err = w.seed("--project", "research_a")
    assert rc == 0, err
    _, fields, rows = _draft(w)
    assert fields == S.BASE_COLUMNS and len(rows) == 4
    assert w.srv.hits == [] and w.net_env.ledger_lines() == []
    assert summary_of(out)["pmc"] is None


def _fake_chain(monkeypatch, outcomes):
    calls = []

    def fake(dois, *, state=None, cfg=None, **k):
        calls.append(list(dois))
        return {d.lower(): outcomes[d] for d in dois}
    monkeypatch.setattr(lit_net, "doi_to_pmcid", fake)
    return calls


def test_status_tokens_and_transport_count(pmc_world, monkeypatch):
    w = pmc_world
    hit = lambda d, p=None, rel=None: lit_net.PmcidHit(d, p, rel, "epmc_search")  # noqa: E731
    calls = _fake_chain(monkeypatch, {
        OA_DOI: Outcome(Kind.OK, status=200, host="x", payload=hit(OA_DOI, OA_PMC)),
        AM_DOI: Outcome(Kind.EMBARGOED, status=200, host="x", payload=hit(AM_DOI, AM_PMC, "2027-03-11")),
        NEW_DOI: Outcome(Kind.TRANSPORT, host="x", detail="ConnectionError: reset", payload=hit(NEW_DOI)),
        MISS_DOI: Outcome(Kind.DEFERRED, host="x", detail="budget", payload=hit(MISS_DOI)),
    })
    rc, out, err = w.seed("--project", "research_a", "--pmc-check")
    assert rc == 2 and len(calls) == 1 and sorted(calls[0]) == sorted([OA_DOI, AM_DOI, NEW_DOI, MISS_DOI])
    _, _, rows = _draft(w)
    got = {r["doi"]: (r["pmcid"], r["pmc_status"]) for r in rows}
    assert got[OA_DOI] == (OA_PMC, "OK")
    assert got[AM_DOI] == (AM_PMC, "EMBARGOED until 2027-03-11")           # answered, not a failure
    assert got[NEW_DOI][1].startswith("TRANSPORT") and got[MISS_DOI][1] == "DEFERRED"
    s = summary_of(out)
    assert s["transport_failures"] == 1 and s["pmc"]["failed"] == 2


def test_a_crashed_lookup_marks_every_row_and_exits_2(pmc_world, monkeypatch):
    w = pmc_world

    def boom(dois, **k):
        raise RuntimeError("state locked")
    monkeypatch.setattr(lit_net, "doi_to_pmcid", boom)
    rc, out, err = w.seed("--project", "research_a", "--pmc-check")
    assert rc == 2
    _, _, rows = _draft(w)
    assert {r["pmc_status"] for r in rows} == {"ERROR: RuntimeError: state locked"}
