"""W4a verifier K: litpipe.canaries (W4-B, 5f5645a) against the REAL network layer, the REAL state
module, the REAL sweep and migrate, and the record shapes they actually write.

Every network check goes to a MockServer on loopback (the builder's `env` harness from
tests/test_canaries.py). The tests named for K-1 to K-9 lock the APPLY items handed back to the
dispatcher: each failed on 964c26f and passes with its fix, landed in the same commit.
Lines printed as `K-EVIDENCE {json}` are the verifier's measurements (run with -s to collect them).
"""
import csv
import dataclasses
import json
import os
import re
import subprocess
import sys
import time
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import lit_net
import preprint_fetch
import sweep
from litpipe import canaries, config, hosts, ledger, net
from litpipe.outcomes import Kind
from tests.netmock import Reply
from tests.test_canaries import (CTX_ARXIV, CTX_PLAIN, JSON, KEY, NOW, P, env, fx, fxj,  # noqa: F401
                                 script_all, status)
from tests.test_w1_verify_b import World, read_csv, write_csv

# The real constants, captured before any test points them at a mock: a send to these hosts would
# trip the conftest guard, so "nothing sent" is proven by the guard and the ledger together.
REAL_ARXIV_API = preprint_fetch.ARXIV_API
REAL_IDCONV = lit_net.IDCONV
FORBIDDEN = frozenset(canaries.ARXIV_HOSTS) | {"pmc.ncbi.nlm.nih.gov"}
ADDR = "tester@litpipe-test.org"          # conftest net_env's LITPIPE_EMAIL
NETWORK_IDS = [c.id for c in canaries.CHECKS if c.phase == "network"]
REAL_ORIGINS = {c.id: canaries._origin_host(c) for c in canaries.CHECKS if c.phase == "network"}


def ev(name, **data):
    print("K-EVIDENCE " + json.dumps({"name": name, **data}, default=str))


def by_id(outs, cid):
    return [o for o in outs if o.detail == cid]


def refused_in_status(st):
    return {h["host"]: h["refused"] for h in st.status()["hosts"] if h.get("refused")}


# ================================================================ 1. refusal semantics, REAL net + REAL state
@pytest.fixture
def real(env):
    import litpipe.state as st
    env.st = st
    return env


def test_1a_single_503_then_200_refuses_nothing_real_state(real):
    real.srv.script(P["datacite"], Reply(503, b"blip"), Reply(200, fx("datacite_arxiv_2605_29559.json"), JSON))
    o = real.one("datacite", state=real.st)
    assert status(o) == canaries.PASS and o.payload["action"] == canaries.ACTION_NONE
    assert refused_in_status(real.st) == {}
    assert len(real.hits()) == 2 == o.attempts          # one send per round: no status retries
    ev("1a_503_then_200", hits=len(real.hits()), planned=1)


def test_1b_two_5xx_refuse_the_origin_for_the_run_real_state(real):
    real.srv.script(P["datacite"], Reply(502, b"bad gateway"))
    o = real.one("datacite", state=real.st)
    assert status(o) == canaries.ALARM and o.payload["action"] == canaries.ACTION_REFUSED
    assert refused_in_status(real.st) == {"127.0.0.1": "run"}
    assert len(real.hits()) == 2                          # retry_statuses=() really stops 502 retries
    ev("1b_5xx_twice", hits=len(real.hits()), worst=canaries.worst_case("datacite"))


def test_1c_a_403_refuses_at_once_real_state(real):
    real.srv.script(P["doira"], Reply(403, b"forbidden"))
    o = real.one("doira", state=real.st)
    assert status(o) == canaries.ALARM and o.payload["action"] == canaries.ACTION_REFUSED
    assert refused_in_status(real.st) == {"127.0.0.1": "run"}
    assert len(real.hits()) == 1 and real.clock.sleeps.count(canaries.RECHECK_S) == 0
    ev("1c_403", hits=len(real.hits()))


def test_1d_a_failing_redirect_target_refuses_neither_host_real_state(real):
    real.srv2.script(P["cnt1"], Reply(503, b"down"))
    o = real.one("content_negotiation", state=real.st)
    assert status(o) == canaries.ALARM and o.payload["action"] == canaries.ACTION_NONE
    assert refused_in_status(real.st) == {}
    assert len(real.hits(P["cn1"])) == 2 and len(real.hits(P["cnt1"])) == 2   # 2 per round, 2 rounds
    ev("1d_cn_target_503", hits=len(real.hits()), worst=canaries.worst_case("content_negotiation"))


def test_1e_what_litpipe_net_does_to_a_redirect_target_on_403(real):
    """Not a canary refusal: litpipe.net refuses a host that answers 403 (DEC-06, host row
    refuse_after_consecutive_403=1), redirect target or not. Recorded for the report."""
    real.srv2.script(P["cnt1"], Reply(403, b"forbidden"))
    o = real.one("content_negotiation", state=real.st)
    assert o.payload["action"] == canaries.ACTION_NONE
    assert refused_in_status(real.st) == {"127.0.0.2": "run"}       # by litpipe.net, not the canary
    assert "127.0.0.1" not in refused_in_status(real.st)
    ev("1e_cn_target_403", net_refused=refused_in_status(real.st), observed=o.payload["observed"])


def test_1f_drift_refuses_nothing_real_state(real):
    body = fxj("datacite_arxiv_2605_29559.json")
    body["data"]["attributes"]["publisher"] = {"name": "arXiv"}
    real.srv.script(P["datacite"], Reply(200, json.dumps(body), JSON))
    o = real.one("datacite", state=real.st)
    assert status(o) == canaries.ALARM and o.payload["action"] == canaries.ACTION_NONE
    assert refused_in_status(real.st) == {} and len(real.hits()) == 1


def test_1g_a_manual_refusal_is_never_downgraded_by_the_canary(real):
    """A host whose row refuses "manual" (as arXiv's): litpipe.net refuses it manual on the 403,
    then the canary's refuse(..., "run") must leave it manual."""
    hosts.register(hosts.HostPolicy("127.0.0.1", min_interval_s=0.0, redirect_allow=("127.0.0.2",),
                                    refusal_persistence="manual"))
    real.srv.script(P["datacite"], Reply(403, b"forbidden"))
    o = real.one("datacite", state=real.st)
    assert status(o) == canaries.ALARM
    assert refused_in_status(real.st) == {"127.0.0.1": "manual"}
    ev("1g_manual_kept", action=o.payload["action"], state=refused_in_status(real.st))


def test_1h_transport_worst_case_on_the_last_osf_hop(env):
    env.srv2.script(P["blob"], Reply(close=True))
    o = env.one("osf")
    assert status(o) == canaries.ALARM and o.kind is Kind.TRANSPORT
    per_round = len(env.hits()) // 2
    assert per_round == 1 + 3 + 2                      # record, two hops, the blob hop and 2 transport retries
    assert o.attempts == len(env.hits()) <= canaries.worst_case("osf")
    assert env.state.refused == {}                      # a transport failure on a hop refuses nothing
    ev("1h_osf_blob_transport", hits=len(env.hits()), worst=canaries.worst_case("osf"))


def test_1i_unplanned_redirect_hops_are_outside_the_documented_worst_case(env):
    """Evidence only: a self-redirecting answer then a dropped connection. litpipe.net follows up to
    MAX_REDIRECTS hops per request, which worst_case() does not count (a LOW docstring item)."""
    env.srv.script(P["datacite"], *([Reply(301, b"", {"Location": env.srv.url(P["datacite"])})] * 5),
                   Reply(close=True))
    o = env.one("datacite")
    ev("1i_redirect_loop_then_transport", hits=len(env.hits()), worst=canaries.worst_case("datacite"),
       status=status(o))
    assert len(env.hits()) > canaries.worst_case("datacite")


def test_k1_a_single_429_does_not_turn_a_source_off_for_the_night(env):
    cr = fxj("crossref_refs_2.json")
    env.srv.script(P["crossref"], Reply(429, b"slow down", {"Retry-After": "1"}),
                   Reply(200, json.dumps(cr["body"]), {**JSON, **cr["headers"]}))
    o = env.one("crossref_refs")
    assert env.state.refused == {}
    assert status(o) == canaries.PASS


def test_k1_a_single_429_on_a_redirect_target_never_refuses_it(env):
    env.srv2.script(P["cnt1"], Reply(429, b"", {"Retry-After": "1"}),
                    Reply(200, fx("cn_crossref_csl.json"), {"Content-Type": "application/vnd.citationstyles.csl+json"}))
    o = env.one("content_negotiation")
    assert "127.0.0.2" not in env.state.refused
    assert status(o) == canaries.PASS


def test_k1_measured_current_429_behaviour(env):
    """Evidence for K-1: the canary's verdict on one 429 is recorded (today: the origin refused);
    the same 429 through litpipe.net with the host row's own retries is waited out (asserted)."""
    env.srv.script(P["datacite"], Reply(429, b"", {"Retry-After": "1"}),
                   Reply(200, fx("datacite_arxiv_2605_29559.json"), JSON))
    o = env.one("datacite")
    canary_refused = dict(env.state.refused)
    env.state.refused.clear()
    env.srv.script(P["datacite"], Reply(429, b"", {"Retry-After": "1"}),
                   Reply(200, fx("datacite_arxiv_2605_29559.json"), JSON))
    import ris_emit
    from litpipe import doi as _doi
    stage = net.get(ris_emit.DATACITE_WORK.format(doi=_doi.encode_path(canaries.DATACITE_DOI)), purpose="stage")
    ev("k1_429", canary_status=status(o), canary_refused=list(canary_refused),
       stage_kind=str(stage.kind), stage_attempts=stage.attempts, stage_refused=list(env.state.refused))
    assert stage.kind is Kind.OK and stage.attempts == 2 and env.state.refused == {}


def test_k2_the_report_does_not_claim_nothing_was_refused(env):
    env.srv2.script(P["cnt1"], Reply(403, b"forbidden"))
    o = env.one("content_negotiation")
    assert "127.0.0.2" in env.state.refused
    assert "nothing refused" not in o.payload["observed"]


# ================================================================ 2. never send to a refused or forbidden host
@pytest.fixture
def guarded(env, monkeypatch):
    """env with every transport attempt recorded (on top of conftest's non-loopback guard)."""
    env.sent_hosts = []
    for name, fn in list(net._TRANSPORTS.items()):
        def rec(method, url, *a, _fn=fn, **k):
            from urllib.parse import urlsplit
            env.sent_hosts.append(urlsplit(url).hostname)
            return _fn(method, url, *a, **k)
        monkeypatch.setitem(net._TRANSPORTS, name, rec)
    return env


CASES = ([(h, "manual") for h in canaries.ARXIV_HOSTS] + [(h, "run") for h in canaries.ARXIV_HOSTS]
         + [("pmc.ncbi.nlm.nih.gov", "run"), ("pmc.ncbi.nlm.nih.gov", "manual")])


@pytest.mark.parametrize("host,persistence", CASES)
def test_2a_a_refused_host_gets_no_line_in_any_profile_or_phase(guarded, host, persistence, tmp_path, monkeypatch):
    import litpipe.state as st
    if host == "pmc.ncbi.nlm.nih.gov":
        monkeypatch.setattr(lit_net, "IDCONV", REAL_IDCONV)
    else:
        monkeypatch.setattr(preprint_fetch, "ARXIV_API", REAL_ARXIV_API)
    st.refuse(host, f"{persistence}: test", persistence=persistence)
    ctx = {**CTX_ARXIV, "db_path": str(tmp_path / "none.duckdb"), "since": NOW.isoformat()}
    seen = []
    for profile in canaries.PROFILES:
        for phase in ("network", "all"):          # "local" alone never sends (test_2c)
            script_all(guarded.srv, guarded.srv2)
            outs = canaries.run(profile, phase=phase, context=ctx, state=st, now=NOW)
            assert not [o for o in outs if o.payload["status"] == canaries.ERROR], [o.payload for o in outs]
            for cid in ("arxiv", "idconv"):
                for o in by_id(outs, cid):
                    if (cid == "arxiv") == (host != "pmc.ncbi.nlm.nih.gov"):
                        assert status(o) == canaries.SKIPPED and o.attempts == 0
            seen.append((profile, phase, len(outs)))
    lines = [x for x in guarded.net_env.ledger_lines() if x.get("host") in FORBIDDEN]
    assert lines == []                                              # not even a not_sent line
    assert set(guarded.sent_hosts) <= {"127.0.0.1", "127.0.0.2"}
    assert refused_in_status(st)[host] == persistence               # nothing downgraded or cleared


def test_2b_nothing_refused_and_no_project_lists_arxiv_sends_no_arxiv(guarded, tmp_path, monkeypatch):
    import lit_util
    monkeypatch.setattr(preprint_fetch, "ARXIV_API", REAL_ARXIV_API)
    for ctx in (CTX_PLAIN, None):
        if ctx is None:     # the CLI default: the registry's active projects, none with arxiv
            guarded.net_env.write_config(db_dir=str(tmp_path / "db"), root=str(tmp_path / "root"),
                                         projects={"teaching_x": {"lib_dir": "lib", "sources": ["unpaywall", "pmc"]}})
            monkeypatch.setattr(lit_util, "PROJECTS_ROOT", tmp_path / "root")
        outs = canaries.run("monthly", phase="network", context=ctx, now=NOW)
        a = by_id(outs, "arxiv")[0]
        assert status(a) == canaries.SKIPPED and "no scheduled project" in a.payload["observed"]
    assert not [x for x in guarded.net_env.ledger_lines() if x.get("host") in FORBIDDEN]
    assert set(guarded.sent_hosts) <= {"127.0.0.1", "127.0.0.2"}


def test_2c_the_local_phase_sends_nothing(env, monkeypatch, tmp_path):
    def boom(*a, **k):
        raise AssertionError("the local phase sent a request")
    monkeypatch.setattr(net, "request", boom)
    outs = canaries.run("monthly", phase="local", context={"db_path": str(tmp_path / "x.duckdb"), "projects": []},
                        request=boom)
    assert outs and all(o.payload["phase"] == "local" for o in outs) and env.hits() == []


def test_2d_redirect_reach_of_every_network_check(env):
    """Evidence: the hosts a check can reach by a redirect (its origin row's allow-list); none is an
    arXiv host or pmc.ncbi.nlm.nih.gov."""
    hosts.reset()
    reach = {cid: (origin, list(hosts.policy(origin).redirect_allow)) for cid, origin in REAL_ORIGINS.items()}
    assert not set(REAL_ORIGINS.values()) & FORBIDDEN - {"export.arxiv.org", "pmc.ncbi.nlm.nih.gov"}
    ev("2d_redirect_reach", reach=reach, default_redirect_allow=list(hosts.DEFAULT.redirect_allow))
    allowed = {h for _, al in reach.values() for h in al}
    assert not (allowed & FORBIDDEN) and hosts.ANY_HOST not in allowed


# ================================================================ 3. the plan and the cost
@pytest.mark.parametrize("profile,expected", [("every_run", 1), ("daily", 18), ("weekly", 27), ("monthly", 27)])
def test_3a_server_hits_of_a_passing_run_equal_the_plan(env, profile, expected):
    planned = canaries.planned_requests(profile, context=CTX_ARXIV)
    outs = canaries.run(profile, phase="network", context=CTX_ARXIV, now=NOW)
    assert all(status(o) == canaries.PASS for o in outs), [o.payload for o in outs if status(o) != canaries.PASS]
    hits = len(env.hits())
    assert hits == sum(planned.values()) == expected
    per ={o.detail: o.attempts for o in outs}
    assert per == planned
    ev("3a_plan", profile=profile, planned=sum(planned.values()), measured=hits, per_check=per)


def test_3b_dry_run_total_matches_the_measured_plan(env, tmp_path, monkeypatch, capsys):
    import lit_util
    env.net_env.write_config(db_dir=str(tmp_path / "db"), root=str(tmp_path / "root"),
                             projects={"research_x": {"lib_dir": "lib", "sources": ["unpaywall", "pmc", "arxiv"]}})
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", tmp_path / "root")
    totals = {}
    for profile in canaries.PROFILES:
        capsys.readouterr()
        assert canaries.main(["--profile", profile, "--dry-run"]) == 0
        last = capsys.readouterr().out.strip().splitlines()[-1]
        totals[profile] = int(re.search(r": (\d+) planned requests", last).group(1))
    assert totals == {"every_run": 1, "daily": 18, "weekly": 27, "monthly": 27}
    assert env.hits() == []
    ev("3b_dry_run", totals=totals)


def test_3c_request_count_table_per_check_and_failure_case(env):
    first = {"idconv": P["idconv"], "epmc_fulltextxml": P["epmc_am"], "efetch": P["efetch"], "s3": P["s3_list"],
             "bioc": P["bioc"], "biorxiv": P["details"], "osf": P["osf_rec"], "sportrxiv": P["oai"],
             "arxiv": P["arxiv"], "datacite": P["datacite"], "doira": P["doira"], "openalex": P["openalex"],
             "crossref_refs": P["crossref"], "content_negotiation": P["cn1"], "crossref_alias": P["alias"],
             "sici": P["sici"]}
    assert set(first) == set(NETWORK_IDS)
    table = []
    for cid in NETWORK_IDS:
        for case in ("pass", "503_then_pass", "5xx_twice", "403", "transport"):
            env.srv.hits.clear(), env.srv2.hits.clear()
            env.state.refused.clear(), env.state.kv.clear(), env.state.deferred.clear()
            script_all(env.srv, env.srv2)
            path = first[cid]
            ok = env.srv._scripts[path][0]
            fail = {"pass": None, "503_then_pass": [Reply(503, b"x"), ok], "5xx_twice": [Reply(503, b"x")],
                    "403": [Reply(403, b"x")], "transport": [Reply(close=True)]}[case]
            if fail:
                env.srv.script(path, *fail)
            o = env.one(cid)
            n = len(env.hits())
            row = {"check": cid, "case": case, "planned": canaries.CHECKS_BY_ID[cid].requests,
                   "worst": canaries.worst_case(cid), "measured": n, "status": status(o),
                   "action": o.payload["action"]}
            table.append(row)
            assert n == o.attempts <= canaries.worst_case(cid), row
            if case == "pass":
                assert n == row["planned"] and status(o) == canaries.PASS, row
            if case == "503_then_pass":
                assert n == 1 + row["planned"] and status(o) == canaries.PASS and row["action"] == "none", row
            if case in ("5xx_twice", "403"):
                assert row["action"] == canaries.ACTION_REFUSED, row
    ev("3c_table", table=table)


# ================================================================ 4. local checks on a real sweep
DAY = "2026-10-01"
STAGES = ["normalized", "unpaywall", "pmc", "preprint", "residual", "report", "processed", "routing"]


def _swept_world(tmp_path, monkeypatch, capsys):
    w = World(tmp_path, monkeypatch)
    w.cfg["projects"] = {"P": {"lib_dir": "lit", "sources": ["unpaywall", "pmc", "osf"]}}
    w.cfg_path.write_text(json.dumps(w.cfg), encoding="utf-8")
    s = w.stages.spec
    for i in range(25):     # 25 rows with a PMCID, 7 downloaded: 0.28
        s[f"10.1000/pmc.{i}"] = {"pmc": {"pmcid": f"PMC{1000 + i}", "downloaded": str(i < 7),
                                         "error": "" if i < 7 else "NO_PDF"}}
    for i in range(3):      # already held: skipped, not counted
        s[f"10.1000/held.{i}"] = {"pmc": {"pmcid": f"PMC{2000 + i}", "skipped": "True"}}
    for i in range(10):     # MDPI OA: 5 downloaded
        ok = i < 5
        s[f"10.3390/mdpi.{i}"] = {"unpaywall": {
            "oa_status": "OA", "downloaded": str(ok), "filename": f"mdpi_{i}.pdf" if ok else "",
            "attempts": "publisher/publishedVersion/OK" if ok else
            "publisher/publishedVersion/HTTP_403 | repository/acceptedVersion/HTTP_403 | repository/acceptedVersion/HOST_REFUSED"}}
    for i in range(5):      # BMC OA: none downloaded
        s[f"10.1186/bmc.{i}"] = {"unpaywall": {"oa_status": "OA", "downloaded": "False",
                                               "attempts": "publisher/publishedVersion/HTTP_403 | publisher/publishedVersion/DEFERRED"}}
    for i in range(5):
        (w.lib / f"mdpi_{i}.pdf").write_bytes(b"%PDF-1.4 test")
    w.queue(list(s))
    w.queue(["10.1000/tagged.1", "10.1000/tagged.2"], name="lit_pull_queue.tagx.csv")
    since = datetime.now(timezone.utc) - timedelta(seconds=2)
    capsys.readouterr()
    out1 = w.sweep(DAY)
    w.queue(["10.1000/second.1"])
    out2 = w.sweep(DAY)
    printed = capsys.readouterr().out
    run_ids = re.findall(r"\[sweep\] run_id=(\S+) project=P", printed)
    arts = sorted(p.name for p in w.proj.glob("lit_pull_queue*.csv"))
    ev("4_sweep_world", exit=[out1.get("exit_code"), out2.get("exit_code")], run_ids=run_ids, artifacts=arts)
    ctx = {"run_id": "20261001T010000Z-run-1-abc", "since": since.isoformat().replace("+00:00", "Z"),
           "db_path": str(tmp_path / "no.duckdb"),
           "projects": [{"key": "P", "root": str(w.proj), "lib_dir": "lit", "sources": ["unpaywall", "pmc", "osf"],
                         "artifact_dir": None, "sweep_run_ids": run_ids, "stages": KEPT}]}
    return w, ctx, since


KEPT = [s for s in STAGES if s != "normalized"]       # sweep.py:1188 deletes `normalized` when a queue retires


def test_k8_a_completed_sweep_with_the_documented_stage_list_has_no_lost_artifact(tmp_path, monkeypatch, capsys):
    w, ctx, since = _swept_world(tmp_path, monkeypatch, capsys)
    ctx["projects"][0]["stages"] = STAGES                 # exactly the module docstring's list
    lost = [o.payload for o in canaries.run("every_run", phase="local", context=ctx) if o.detail == "lost_artifacts"]
    assert lost[0]["status"] == canaries.PASS, lost[0]["observed"]


def test_k9_a_lost_untagged_artifact_is_not_masked_by_a_tagged_queue(tmp_path, monkeypatch, capsys):
    w, ctx, since = _swept_world(tmp_path, monkeypatch, capsys)
    (w.proj / f"lit_pull_queue.{DAY}.pmc.csv").unlink()  # lit_pull_queue.tagx.<DAY>.pmc.csv is still there
    lost = [o.payload for o in canaries.run("every_run", phase="local", context=ctx) if o.detail == "lost_artifacts"]
    assert lost[0]["status"] == canaries.ALARM and f"{DAY}.pmc" in lost[0]["observed"]


def test_4a_yields_tags_second_run_and_lost_artifacts_on_a_real_sweep(tmp_path, monkeypatch, capsys):
    w, ctx, since = _swept_world(tmp_path, monkeypatch, capsys)
    assert ctx["projects"][0]["sweep_run_ids"] == [DAY, f"{DAY}.2"]
    lc = canaries._LocalCtx(canaries._RunCtx(context=ctx, state=None, request=None, cfg=None, now=None))
    reps = lc.projects[0].reports()
    names = {p.name for ps in reps.values() for p in ps}
    assert f"lit_pull_queue.tagx.{DAY}.unpaywall.csv" in names            # a tagged queue's artifacts
    assert f"lit_pull_queue.{DAY}.2.unpaywall.csv" in names                # the second run of the day
    outs = canaries.run("every_run", phase="local", context=ctx)
    capsys.readouterr()
    got = {o.detail: o.payload for o in outs if o.host == "P"}
    ev("4a_local", checks={k: (v["status"], v["observed"]) for k, v in got.items()})
    assert got["yield_pmc"]["status"] == canaries.ALARM and "7/25" in got["yield_pmc"]["observed"]
    assert got["unpaywall_403"]["status"] == canaries.ALARM and "15/20" in got["unpaywall_403"]["observed"]
    assert got["yield_mdpi_bmc"]["status"] == canaries.ALARM and "5/15" in got["yield_mdpi_bmc"]["observed"]
    assert got["lost_artifacts"]["status"] == canaries.PASS, got["lost_artifacts"]
    assert got["email"]["status"] == canaries.PASS and got["mismatch_growth"]["status"] == canaries.PASS
    # a lost artifact is named; the present ones are not (run .2 had no tagged queue)
    (w.proj / f"lit_pull_queue.{DAY}.2.pmc.csv").unlink()
    outs = canaries.run("every_run", phase="local", context=ctx)
    lost = [o.payload for o in outs if o.detail == "lost_artifacts"][0]
    assert lost["status"] == canaries.ALARM and lost["observed"] == f"missing artifacts: {DAY}.2.pmc"


def test_4b_files_older_than_since_are_ignored(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("LITPIPE_EMAIL", ADDR)
    w, ctx, since = _swept_world(tmp_path, monkeypatch, capsys)
    old = since.timestamp() - 3600
    resid = w.proj / f"lit_pull_queue.{DAY}.residual.csv"
    with open(resid, "a", encoding="utf-8", newline="") as f:
        f.write(f"10.1000/x,contact {ADDR},,,,\n")
    mm = w.lib / "_mismatch"
    mm.mkdir()
    (mm / "old.pdf").write_bytes(b"%PDF")
    (w.lib / "old.ris").write_text("TY  - JOUR\nTI  - A &amp; B <i>x</i>\nER  - \n", encoding="utf-8")
    for p in (mm / "old.pdf", w.lib / "old.ris"):
        os.utime(p, (old, old))

    def run():
        return {o.detail: o.payload for o in canaries.run("every_run", phase="local", context=ctx) if o.host == "P"}
    got = run()
    assert got["email"]["status"] == canaries.ALARM and resid.name in got["email"]["observed"]
    assert ADDR not in json.dumps(got)
    assert got["mismatch_growth"]["status"] == canaries.PASS
    assert "markup" not in got or got["markup"]["observed"]["files_checked"] >= 0
    os.utime(resid, (old, old))
    (w.lib / "new.ris").write_text("TY  - JOUR\nTI  - A <i>study</i>\nER  - \n", encoding="utf-8")
    (mm / "new.pdf").write_bytes(b"%PDF")
    got = run()
    assert got["email"]["status"] == canaries.PASS
    assert got["mismatch_growth"]["status"] == canaries.ALARM and "new.pdf" in got["mismatch_growth"]["observed"]
    assert got["markup"]["status"] == canaries.ALARM
    assert "old.ris" not in got["markup"]["observed"]["summary"]


def test_4c_no_context_defaults_do_not_scan_history(env, tmp_path, monkeypatch):
    """The CLI shape: today's run ids only, today's ledger file only, new files only."""
    import lit_util
    root = tmp_path / "root"
    proj, lib = root / "teaching_x", root / "teaching_x" / "lib"
    lib.mkdir(parents=True)
    env.net_env.write_config(db_dir=str(tmp_path / "db"), root=str(root),
                             projects={"teaching_x": {"lib_dir": "lib", "sources": ["unpaywall", "pmc"]}})
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    now = datetime.now(timezone.utc)
    today, yday = now.strftime("%Y-%m-%d"), (now - timedelta(days=1)).strftime("%Y-%m-%d")
    old = (now - timedelta(days=1)).timestamp()
    write_csv(proj / f"lit_pull_queue.{today}.unpaywall.csv", [{"doi": "10.1/a"}], ["doi"])
    write_csv(proj / f"lit_pull_queue.{yday}.unpaywall.csv", [{"doi": f"10.1/b email={ADDR}"}], ["doi"])
    os.utime(proj / f"lit_pull_queue.{yday}.unpaywall.csv", (old, old))
    (lib / "_mismatch").mkdir()
    (lib / "_mismatch" / "old.pdf").write_bytes(b"%PDF")
    (lib / "old.ris").write_text("TI  - <b>x</b>\n", encoding="utf-8")
    for p in (lib / "_mismatch" / "old.pdf", lib / "old.ris"):
        os.utime(p, (old, old))
    Path(ledger.LEDGER_DIR).mkdir(parents=True, exist_ok=True)
    (Path(ledger.LEDGER_DIR) / f"{yday}.jsonl").write_text(json.dumps(
        {"ts": f"{yday}T12:00:00Z", "run_id": "r", "host": "x", "attempt": 1, "hop": 0, "status": 403,
         "kind": "REFUSED", "note": f"mailto:{ADDR}"}) + "\n", encoding="utf-8")
    outs = canaries.run("every_run", phase="local")
    got = {(o.detail, o.host): o.payload for o in outs}
    ev("4c_no_context", checks={f"{k[0]}@{k[1]}": (v["status"], v.get("target")) for k, v in got.items()})
    assert got[("email", "teaching_x")]["status"] == canaries.PASS
    assert got[("email", "ledger")]["status"] == canaries.PASS
    tgt = got[("lost_artifacts", "teaching_x")]["target"]
    assert tgt == f"teaching_x run {today}"                  # today's run ids only, not yesterday's
    assert got[("mismatch_growth", "teaching_x")]["status"] == canaries.PASS
    assert ("markup", "teaching_x") not in got
    assert got[("lost_artifacts", "teaching_x")]["status"] == canaries.SKIPPED
    assert got[("first_attempts", "ledger")]["status"] == canaries.PASS


# ================================================================ 5. the first-attempt histogram, real ledger lines
RID, OTHER = "20261007T010000Z-run-1-abc", "20261007T020000Z-run-2-def"


@pytest.fixture
def hist(net_env, mock_server, monkeypatch, tmp_path):
    srv, srv2 = mock_server(), mock_server("127.0.0.2")
    hosts.register(hosts.HostPolicy("127.0.0.1", min_interval_s=0.0, redirect_allow=("127.0.0.2",),
                                    refuse_after_consecutive_403=10_000))
    srv.script("/ok", Reply(200, b"{}", JSON))
    srv.script("/f403", Reply(403, b"no"))
    srv.script("/redir", Reply(302, b"", {"Location": srv2.url("/t403")}))
    srv2.script("/t403", Reply(403, b"no"))
    srv2.script("/ok", Reply(200, b"{}", JSON))
    rule = types.SimpleNamespace(host="127.0.0.1", path_prefix="/forbidden-route", reason="test")
    real_prohibited = hosts.prohibited
    monkeypatch.setattr(hosts, "prohibited",
                        lambda url, cfg=None: rule if "/forbidden-route" in url else real_prohibited(url, cfg))

    def noise():
        monkeypatch.setattr(ledger, "RUN_ID", RID)
        for _ in range(5):
            net.get(srv.url("/f403"), purpose="canary:x")                     # canary lines
        for _ in range(2):
            net.get(srv.url("/redir"), purpose="stage")                       # 2 first attempts; hop lines
        for _ in range(30):
            net.get(srv2.url("/ok"), purpose="stage")                         # not_sent (refused above)
        for _ in range(3):
            with pytest.raises(hosts.ProhibitedHost):
                net.get(srv.url("/forbidden-route"), purpose="stage")         # prohibited lines
        monkeypatch.setattr(ledger, "RUN_ID", OTHER)
        for _ in range(10):
            net.get(srv.url("/f403"), purpose="stage")                        # another run
        monkeypatch.setattr(ledger, "RUN_ID", RID)

    def send(n_ok, n_403, extra=None):
        noise()
        for _ in range(n_ok):
            net.get(srv.url("/ok"), purpose="stage")
        for _ in range(n_403):
            net.get(srv.url("/f403"), purpose="stage")
        if extra:
            extra(srv)
        ctx = {"run_id": RID, "since": (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(),
               "db_path": str(tmp_path / "none.duckdb"), "projects": []}
        o = by_id(canaries.run("every_run", phase="local", context=ctx), "first_attempts")[0]
        return o.payload, net_env.ledger_lines()
    return send


@pytest.mark.parametrize("n_ok,expect", [(17, canaries.ALARM), (18, canaries.PASS), (16, canaries.PASS)],
                         ids=["20_attempts_1_refused_5pct", "21_attempts_4.8pct", "19_attempts_under_minimum"])
def test_5a_refused_share_boundaries_on_real_ledger_lines(hist, n_ok, expect):
    payload, lines = hist(n_ok, 1)
    h = payload["observed"]["hosts"]
    assert set(h) == {"127.0.0.1"}, h                          # 127.0.0.2: only hops and not_sent lines
    assert h["127.0.0.1"]["n"] == n_ok + 2 + 1                 # ok + the 2 redirects + the 403
    assert payload["status"] == expect, payload
    kinds = {(x.get("purpose"), x.get("decision"), x.get("host"), x.get("hop")) for x in lines}
    assert ("stage", "not_sent", "127.0.0.2", 0) in kinds and ("stage", "prohibited", "127.0.0.1", 0) in kinds
    assert any(x.get("hop") == 1 and x.get("host") == "127.0.0.2" for x in lines)
    ev("5a", n=h["127.0.0.1"]["n"], share=h["127.0.0.1"]["refused_share"], status=payload["status"])


def test_k3_a_retried_then_served_429_is_not_refused(hist):
    def rl(srv):
        srv.script("/rl", Reply(429, b"", {"Retry-After": "1"}), Reply(200, b"{}", JSON))
        assert net.get(srv.url("/rl"), purpose="stage").kind is Kind.OK
    payload, _ = hist(17, 0, extra=rl)
    assert payload["observed"]["hosts"]["127.0.0.1"]["n"] == 20
    assert payload["status"] == canaries.PASS, payload["observed"]["summary"]


# ================================================================ 6. index freshness and the DB
def _db(path, finished):
    import duckdb
    con = duckdb.connect(str(path))
    con.execute("SET TimeZone = 'UTC'")
    con.execute("CREATE TABLE IF NOT EXISTS index_runs (project VARCHAR, finished_at TIMESTAMPTZ, n_files INTEGER, "
                "db_path VARCHAR)")
    con.execute("INSERT INTO index_runs VALUES ('P', ?, 1, ?)", [finished, str(path)])
    con.close()


def _ctx_lib(tmp_path, db):
    lib = tmp_path / "P" / "lit"
    lib.mkdir(parents=True, exist_ok=True)
    (lib / "a.pdf").write_bytes(b"%PDF")
    return {"db_path": str(db), "projects": [{"key": "P", "root": str(tmp_path / "P"), "lib_dir": "lit",
                                              "sources": ["pmc"], "sweep_run_ids": [], "stages": []}]}


def test_6a_index_check_closes_its_connection(tmp_path):
    import duckdb
    db = tmp_path / "portfolio.duckdb"
    _db(db, datetime.now(timezone.utc) - timedelta(days=3))
    outs = canaries.run("every_run", phase="local", context=_ctx_lib(tmp_path, db))
    idx = by_id(outs, "index_freshness")[0]
    assert status(idx) == canaries.ALARM and "h newer" in idx.payload["observed"]
    w = duckdb.connect(str(db))                     # a writer right after: the read-only handle is closed
    w.execute("INSERT INTO index_runs VALUES ('P', now(), 2, 'x')")
    w.close()
    outs = canaries.run("every_run", phase="local", context=_ctx_lib(tmp_path, db))
    assert status(by_id(outs, "index_freshness")[0]) == canaries.PASS


def test_6b_a_locked_db_is_skipped_at_once(tmp_path):
    import duckdb
    db = tmp_path / "portfolio.duckdb"
    _db(db, datetime.now(timezone.utc))
    holder = duckdb.connect(str(db))                # a read-write handle: a read-only open conflicts
    try:
        t0 = time.monotonic()
        outs = canaries.run("every_run", phase="local", context=_ctx_lib(tmp_path, db))
        took = time.monotonic() - t0
    finally:
        holder.close()
    idx = by_id(outs, "index_freshness")[0]
    assert status(idx) == canaries.SKIPPED and "could not open read-only" in idx.payload["observed"]
    assert took < 1.9                                # tries=1: no 2 s connect_db back-off
    ev("6b_locked", observed=idx.payload["observed"], seconds=round(took, 2))


# ================================================================ 7. redaction
def test_7_planted_email_never_reaches_report_summary_or_stdout(env, tmp_path, monkeypatch, capsys):
    """The address planted in a URL query (a transport error quoting it), in an exception the check
    raises, in a report CSV and in a raw ledger line: none reaches the report, the summary, the
    --json file or stdout. The key never appears either."""
    import lit_util
    import ris_emit
    clean = canaries.report([env.one("openalex")])        # the keyed check, passing: no header in the report
    assert status(env.one("openalex")) == canaries.PASS
    rid = "20261007T030000Z-run-3-aaa"
    import socket
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    dead = s.getsockname()[1]
    s.close()                                                          # a refused connection quotes the URL
    monkeypatch.setattr(ris_emit, "DATACITE_WORK",
                        f"http://127.0.0.1:{dead}/datacite/dois/{{doi}}?email={ADDR}&contact=mailto:{ADDR}")

    def boom(p, rctx):
        raise RuntimeError(f"failed for {ADDR} at https://x.example/?email={ADDR} mailto:{ADDR} "
                           f"Authorization: Bearer {KEY}")
    chk = dataclasses.replace(canaries.CHECKS_BY_ID["idconv"], fn=boom)
    monkeypatch.setattr(canaries, "CHECKS", tuple(chk if c.id == "idconv" else c for c in canaries.CHECKS))
    monkeypatch.setitem(canaries.CHECKS_BY_ID, "idconv", chk)
    root = tmp_path / "root"
    proj = root / "teaching_x"
    proj.mkdir(parents=True)
    env.net_env.write_config(db_dir=str(tmp_path / "db"), root=str(root),
                             projects={"teaching_x": {"lib_dir": "lib", "sources": ["unpaywall"]}})
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    write_csv(proj / f"lit_pull_queue.{today}.unpaywall.csv",
              [{"doi": "10.1/a", "error": f"lookup failed ?email={ADDR}"}, {"doi": "10.1/b", "error": ADDR}],
              ["doi", "error"])
    ldir = Path(ledger.LEDGER_DIR)
    ldir.mkdir(parents=True, exist_ok=True)
    with open(ldir / f"{today}.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps({"ts": f"{today}T00:00:01Z", "run_id": rid, "host": "h", "attempt": 1, "hop": 0,
                            "note": f"raw {ADDR}"}) + "\n")
    ctx = {"run_id": rid, "since": f"{today}T00:00:00Z", "db_path": str(tmp_path / "none.duckdb"),
           "projects": [{"key": "teaching_x", "root": str(proj), "sources": ["unpaywall"],
                         "sweep_run_ids": [today], "stages": ["unpaywall"]}]}
    outs = canaries.run("weekly", phase="all", context=ctx)
    rep = canaries.report(outs, run_id=rid, profile="weekly")
    js = tmp_path / "out" / "health.json"
    env.state.refused.clear()                              # main() sends the same failing checks again
    capsys.readouterr()
    rc = canaries.main(["--profile", "weekly", "--json", str(js)])
    stdout = capsys.readouterr()
    text = "\n".join((json.dumps(rep), canaries.summary(rep), stdout.out, stdout.err,
                      js.read_text(encoding="utf-8")))
    leaks = [s for s in (ADDR, "tester%40", "litpipe-test.org", KEY) if s in text]
    assert leaks == [] and rc == 2
    em = {o.host: o.payload for o in outs if o.detail == "email"}
    assert em["teaching_x"]["status"] == canaries.ALARM
    assert f"lit_pull_queue.{today}.unpaywall.csv:2" in em["teaching_x"]["observed"]
    assert em["ledger"]["status"] == canaries.ALARM
    assert by_id(outs, "idconv")[0].payload["status"] == canaries.ERROR
    assert "idconv" in stdout.out and "[step-summary]" in stdout.out
    ev("7_redaction", tokens_left=[t for t in ("email=", "mailto:", "Authorization", "Bearer") if t in text],
       datacite=by_id(outs, "datacite")[0].payload["observed"], idconv=by_id(outs, "idconv")[0].payload["observed"],
       openalex_clean_has_authorization="Authorization" in json.dumps(clean))
    assert "Authorization" not in json.dumps(clean) and KEY not in json.dumps(clean)


def test_7b_no_report_carries_the_email_tokens(tmp_path):
    """Lock-in: redaction keeps the literal tokens out of every report (the email grep stays a plain grep)."""
    outs = canaries.run("every_run", phase="local", context={"db_path": str(tmp_path / "x"), "projects": []})
    text = json.dumps(canaries.report(outs))
    assert "email=" not in text and "mailto:" not in text


def test_k7_every_check_text_survives_redaction():
    for c in canaries.CHECKS:
        assert ledger.redact(c.expected) == c.expected and ledger.redact(c.target) == c.target, c.id


# ================================================================ 8. the W4-A contract
def test_k4_a_non_dict_project_is_a_config_error(env):
    with pytest.raises(config.ConfigError):
        canaries.run("daily", phase="network", context={"projects": ["research_x"]}, now=NOW)


@pytest.mark.parametrize("field,value", [("sweep_run_ids", "2026-10-07"), ("stages", "pmc"), ("sources", "arxiv")])
def test_k4_a_string_list_field_is_a_config_error(env, tmp_path, field, value):
    p = {"key": "P", "root": str(tmp_path), "sources": ["pmc"], "sweep_run_ids": [], "stages": [], field: value}
    with pytest.raises(config.ConfigError):
        canaries.run("every_run", phase="local", context={"db_path": str(tmp_path / "x"), "projects": [p]})


def test_k5_an_unreadable_report_is_not_a_pass(tmp_path):
    rows = [{"doi": f"10.1/{i}", "pmcid": f"PMC{i}", "downloaded": str(i < 5), "skipped": "False"} for i in range(25)]
    p = tmp_path / "lit_pull_queue.2026-10-07.pmc.csv"
    write_csv(p, rows, ["doi", "pmcid", "downloaded", "skipped"])
    p.write_bytes(p.read_bytes().replace(b"10.1/3,", b"10.1/3\xe9,"))      # one cp1252 byte
    ctx = {"db_path": str(tmp_path / "x"), "projects": [{"key": "P", "root": str(tmp_path), "sources": ["pmc"],
                                                         "sweep_run_ids": ["2026-10-07"], "stages": []}]}
    o = by_id(canaries.run("every_run", phase="local", context=ctx), "yield_pmc")[0]
    assert status(o) != canaries.PASS, o.payload["observed"]


def test_k6_a_mismatch_file_written_at_since_is_new(tmp_path):
    lib = tmp_path / "lit" / "_mismatch"
    lib.mkdir(parents=True)
    f = lib / "x.pdf"
    f.write_bytes(b"%PDF")
    since = datetime(2026, 10, 7, tzinfo=timezone.utc)
    os.utime(f, (since.timestamp(), since.timestamp()))
    ctx = {"since": "2026-10-07T00:00:00Z", "db_path": str(tmp_path / "x"),
           "projects": [{"key": "P", "root": str(tmp_path), "lib_dir": "lit", "sources": ["pmc"],
                         "sweep_run_ids": [], "stages": []}]}
    o = by_id(canaries.run("every_run", phase="local", context=ctx), "mismatch_growth")[0]
    assert status(o) == canaries.ALARM


def test_8_status_kinds_and_refused_hosts_are_distinguishable(env):
    """What W4-A reads: payload status tells PASS / ALARM / SKIPPED / ERROR apart, `action` and
    `refused_hosts` the canary's own refusals. (Every mock check shares 127.0.0.1, so a refusal
    skips the checks after it, as a real refusal skips the checks on that host.)"""
    env.srv.script(P["efetch"], Reply(200, b"<pmc-articleset></pmc-articleset>", {"Content-Type": "text/xml"}))
    env.srv.script(P["datacite"], Reply(503, b"down"))
    outs = canaries.run("daily", phase="network", context=CTX_PLAIN, now=NOW)
    rep = canaries.report(outs, run_id="r", profile="daily")
    st = {c["id"]: (c["status"], c["kind"], c["action"]) for c in rep["checks"]}
    assert st["datacite"] == (canaries.ALARM, "OUTAGE", canaries.ACTION_REFUSED)
    assert st["efetch"][0] == canaries.ALARM and st["efetch"][2] == canaries.ACTION_NONE
    assert st["doira"][0] == canaries.SKIPPED and st["arxiv"][0] == canaries.SKIPPED
    assert rep["refused_hosts"] == ["127.0.0.1"]
    assert len(canaries.summary(rep).splitlines()) <= canaries.SUMMARY_LINES


# ================================================================ 10. CLI from a foreign cwd
def test_10_help_from_a_foreign_cwd(tmp_path):
    root = Path(canaries.__file__).resolve().parents[1]
    envv = {**os.environ, "PYTHONPATH": str(root)}
    p = subprocess.run([sys.executable, "-m", "litpipe.canaries", "--help"], cwd=str(tmp_path),
                       capture_output=True, text=True, timeout=120, env=envv)
    assert p.returncode == 0 and "--profile" in p.stdout, p.stderr
