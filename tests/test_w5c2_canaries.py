"""W5-C2 step 8 (item 5; C116): the canaries' K-5 siblings read a failed read as ERROR, never PASS.

- `_l_email`: an unreadable report is ERROR (it was skipped, and the project passed);
- `_newest_lib_mtime`: an unlistable library is ERROR for its project (it read as "no PDF", PASS);
- `_file_has_markup`: an unreadable or unparsable new file is ERROR (it read as clean);
- `epmc_403`: verified that it cannot match an `epmc_fulltextxml` REFUSED row without a 403 (a host
  refused for the run sends nothing, first_status blank); locked here, no narrowing needed;
- a canary ALARM on an origin litpipe.net already refused until cleared (a manual-policy host) says
  "refused until cleared", not "refused for the run".
"""
import json
from datetime import timedelta

import pytest

from litpipe import canaries, hosts
from tests.netmock import Reply
from tests.test_canaries import P, env  # noqa: F401  (fixture)
from tests.test_w4b_local import Proj, _db, one, pmc_fields, pmc_rows, upw_fields


# ------------------------------------------------------------------ email: an unreadable report
def test_an_unreadable_report_is_error_not_pass(tmp_path, monkeypatch):
    pr = Proj(tmp_path)
    good = pr.report("unpaywall", upw_fields(), [{"doi": "10.1/a"}])
    bad = pr.report("pmc", pmc_fields(), pmc_rows(1, 1))
    real_open = open

    def fake_open(path, *a, **k):
        if str(path) == str(bad):
            raise PermissionError("locked by a sync client")
        return real_open(path, *a, **k)
    monkeypatch.setattr("builtins.open", fake_open)
    rows = [o for o in canaries.run("every_run", phase="local", context=pr.context(), now=pr.now)
            if o.detail == "email" and o.host == pr.key]
    assert len(rows) == 1
    p = rows[0].payload
    assert p["status"] == canaries.ERROR and bad.name in p["observed"] and "PermissionError" in p["observed"]
    assert good.exists()


def test_email_hits_still_alarm_beside_an_unreadable_report(tmp_path, monkeypatch):
    pr = Proj(tmp_path)
    pr.report("unpaywall", upw_fields(), [{"doi": "10.1/a", "error": "GET ?email=someone%40x.org"}])
    bad = pr.report("pmc", pmc_fields(), pmc_rows(1, 1))
    real_open = open
    monkeypatch.setattr("builtins.open", lambda path, *a, **k: (_ for _ in ()).throw(OSError("gone"))
                        if str(path) == str(bad) else real_open(path, *a, **k))
    rows = [o for o in canaries.run("every_run", phase="local", context=pr.context(), now=pr.now)
            if o.detail == "email" and o.host == pr.key]
    assert rows[0].payload["status"] == canaries.ALARM and "unreadable report" in rows[0].payload["observed"]


# ------------------------------------------------------------------ index freshness: an unlistable library
def test_an_unlistable_library_is_error_not_no_pdf(tmp_path, monkeypatch):
    pr = Proj(tmp_path)
    (pr.lib / "x.pdf").write_bytes(b"%PDF")
    _db(pr.db, [(pr.key, pr.now - timedelta(hours=2))])
    real = canaries.os.scandir

    def scandir(p):
        if str(p) == str(pr.lib):
            raise PermissionError("access denied")
        return real(p)
    monkeypatch.setattr(canaries.os, "scandir", scandir)
    o = one(pr.run(), "index_freshness")
    assert o.payload["status"] == canaries.ERROR and "library unreadable" in o.payload["observed"]


def test_newest_lib_mtime_raises_on_a_listing_failure_and_reads_none_for_an_empty_library(tmp_path):
    with pytest.raises(canaries._Unreadable):
        canaries._newest_lib_mtime(tmp_path / "missing")
    assert canaries._newest_lib_mtime(tmp_path) is None


# ------------------------------------------------------------------ markup: an unreadable new file
def test_an_unparsable_sidecar_is_error_not_clean(tmp_path):
    pr = Proj(tmp_path)
    (pr.lib / "a.ris").write_text("TY  - JOUR\nTI  - Clean title\nER  - \n", encoding="utf-8")
    (pr.lib / "b.fulltext.json").write_bytes(b"{not json")
    o = one(pr.run(), "markup")
    ob = o.payload["observed"]
    assert o.payload["status"] == canaries.ERROR and ob["unreadable"] == 1 and "b.fulltext.json" in ob["summary"]
    assert ob["files_checked"] == 1 and ob["with_markup"] == 0


def test_markup_still_alarms_beside_an_unreadable_file(tmp_path):
    pr = Proj(tmp_path)
    (pr.lib / "a.ris").write_text("TY  - JOUR\nTI  - Heat &amp; exercise\nER  - \n", encoding="utf-8")
    (pr.lib / "b.fulltext.json").write_bytes(b"\xff\xfe not utf-8 json")
    o = one(pr.run(), "markup")
    assert o.payload["status"] == canaries.ALARM and o.payload["observed"]["unreadable"] == 1


def test_file_has_markup_raises_on_a_read_error(tmp_path):
    with pytest.raises(canaries._Unreadable):
        canaries._file_has_markup(tmp_path / "gone.fulltext.json")


# ------------------------------------------------------------------ epmc_403 cannot match a REFUSED row without a 403
def test_epmc_403_does_not_match_a_refused_fulltextxml_row_with_no_403(tmp_path):
    pr = Proj(tmp_path)
    rows = pmc_rows(3, 3) + [
        {"doi": "10.1/r", "pmcid": "PMC70", "downloaded": False, "route": "epmc_fulltextxml",
         "first_status": "", "outcome": "REFUSED", "attempts": "epmc_fulltextxml/REFUSED",
         "error": "epmc_fulltextxml/REFUSED: host www.ebi.ac.uk is refused; nothing sent"},
        {"doi": "10.1/s", "pmcid": "PMC71", "downloaded": False, "route": "epmc_fulltextxml",
         "first_status": "500", "outcome": "NOT_AVAILABLE", "attempts": "epmc_fulltextxml/NOT_AVAILABLE"}]
    pr.report("pmc", pmc_fields(), rows)
    assert one(pr.run(), "epmc_403").payload["status"] == canaries.PASS


# ------------------------------------------------------------------ the manual refusal's wording
def test_an_origin_refused_until_cleared_says_so_and_is_not_downgraded(env):
    # the origin's host row refuses until cleared (arXiv's policy); litpipe.net refuses it on the 406
    hosts.register(hosts.HostPolicy("127.0.0.1", min_interval_s=0.0, redirect_allow=("127.0.0.2",),
                                    refusal_persistence="manual"))
    env.srv.script(P["arxiv"], Reply(406, b""))
    o = env.one("arxiv")
    assert o.payload["status"] == canaries.ALARM and o.payload["action"] == canaries.ACTION_REFUSED_MANUAL
    assert env.state.refused["127.0.0.1"][1] == "manual"                 # the canary did not downgrade it
    rep = canaries.report([o], run_id="r", profile="daily", started=None)
    assert rep["refused_hosts"] == ["127.0.0.1"]
    text = canaries.summary(rep)
    assert "[refused until cleared]" in text and "refused for the run" not in text


def test_a_run_refusal_keeps_its_wording(env):
    env.srv.script(P["arxiv"], Reply(406, b""))
    o = env.one("arxiv")
    assert o.payload["action"] == canaries.ACTION_REFUSED
    assert "[refused for the run]" in canaries.summary(canaries.report([o], run_id="r", profile="daily"))
