"""TRANSIENT retry cap (2026-11-09, Jacob: fix the retry limit).

FRED's Shang 2022 (10.71465/ajainn3446) answered unpaywall HTTP 500 on 27 nights from 10-15 on and
was re-admitted every day: TRANSIENT had no attempt count, unlike OA_BLOCKED (3) and ERROR (3). At
transient_max_attempts sweeps (default 10) a TRANSIENT row closes as TERMINAL_CLOSED onto the ILL
list and leaves retry_later. An embargoed row is exempt: it waits for a date, not on a failing host."""
import contextlib
import csv
import datetime
import io
import json

import pytest

import lit_util
import migrate_closed_to_md as mig
import sweep
from litpipe import config

RUN, TAG = "2026-11-09", "retry"
FIELDS = list(sweep.QUEUE_COLUMNS) + ["residual_class", "reason", "stages", "held_at", "skipped_sources",
                                      "attempts", "run_id", "not_before"]


def _world(tmp_path, monkeypatch, **top):
    root = tmp_path / "Projects"
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    cfg = {"state_dir": str(tmp_path / "state"), "projects": {"P": {"lib_dir": "literature"}}, **top}
    proj = lit_util.project_root("P", cfg["projects"]["P"])
    proj.mkdir(parents=True)
    p = tmp_path / "projects.json"
    p.write_text(json.dumps(cfg), encoding="utf-8")
    monkeypatch.setattr(mig, "CONFIG_PATH", p)
    return cfg, proj


def _residual(proj, rows):
    """rows: (doi, class, attempts, reason)."""
    with open(proj / f"lit_pull_queue.{TAG}.{RUN}.residual.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        w.writeheader()
        for doi, cls, attempts, reason in rows:
            w.writerow({"doi": doi, "title": f"T {doi}", "authors": "A", "destination": "literature",
                        "residual_class": cls, "reason": reason, "attempts": str(attempts), "run_id": RUN})


def _route(cfg):
    with contextlib.redirect_stdout(io.StringIO()):
        return mig.run("P", run_id=RUN, tags=[TAG], cfg=cfg, use_holdings=False,
                       today=datetime.date(2026, 11, 9))


def _retry(proj):
    p = proj / mig.RETRY_LATER_NAME
    return {r["doi"]: r for r in csv.DictReader(open(p, encoding="utf-8"))} if p.exists() else {}


def _routing(proj):
    return {r["doi"]: r for r in csv.DictReader(
        open(proj / f"lit_pull_queue.{TAG}.{RUN}.routing.csv", encoding="utf-8"))}


def test_transient_at_the_cap_closes_onto_the_ill_list_and_leaves_retry_later(tmp_path, monkeypatch):
    cfg, proj = _world(tmp_path, monkeypatch)
    # Shang's shape: already waiting in retry_later at 26, re-admitted, now on its 27th sweep
    with open(proj / mig.RETRY_LATER_NAME, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=mig.RETRY_FIELDS)
        w.writeheader()
        w.writerow({"doi": "10.71465/ajainn3446", "residual_class": "TRANSIENT", "attempts": "26",
                    "not_before": "2026-11-09", "last_run": "2026-11-08:retry"})
        w.writerow({"doi": "10.1000/other", "residual_class": "TRANSIENT", "attempts": "1",
                    "not_before": "2026-11-12"})
    _residual(proj, [("10.71465/ajainn3446", mig.TRANSIENT, 27, "unpaywall: HTTP_500"),
                     ("10.1000/nine", mig.TRANSIENT, 9, "unpaywall: TRANSPORT"),
                     ("10.1000/ten", mig.TRANSIENT, 10, "unpaywall: TRANSPORT")])
    res = _route(cfg)
    assert res["transient_capped"] == 2
    assert set(_retry(proj)) == {"10.1000/nine", "10.1000/other"}
    assert "1 dropped at the retry cap" in res["written"][mig.RETRY_LATER_NAME]
    ill = (proj / mig.ILL_NAME).read_text(encoding="utf-8")
    assert "10.71465/ajainn3446" in ill and "10.1000/ten" in ill and "10.1000/nine" not in ill
    routing = _routing(proj)
    assert routing["10.1000/ten"]["residual_class"] == mig.TERMINAL_CLOSED
    assert routing["10.1000/ten"]["route"] == "ill (retry cap)"
    assert routing["10.1000/ten"]["reason"] == "unpaywall: TRANSPORT; transient in 10 runs, retry cap 10 reached"
    assert routing["10.1000/nine"]["route"] == "retry_later"
    assert res["counts"].get(mig.TERMINAL_CLOSED) == 2


def test_an_embargo_is_never_capped(tmp_path, monkeypatch):
    cfg, proj = _world(tmp_path, monkeypatch)
    _residual(proj, [("10.1000/emb", mig.TRANSIENT, 40, "unpaywall: EMBARGOED until 2027-03-01")])
    res = _route(cfg)
    assert res["transient_capped"] == 0
    assert _retry(proj)["10.1000/emb"]["residual_class"] == mig.TRANSIENT
    assert _routing(proj)["10.1000/emb"]["route"] == "retry_later"
    assert not (proj / mig.ILL_NAME).exists()


def test_the_oa_blocked_cap_is_unchanged(tmp_path, monkeypatch):
    cfg, proj = _world(tmp_path, monkeypatch)
    _residual(proj, [("10.1000/oab", mig.OA_BLOCKED, 12, "unpaywall: HTTP_403")])
    res = _route(cfg)
    assert res["transient_capped"] == 0 and res["oa_blocked_capped"] == 1
    assert _routing(proj)["10.1000/oab"]["route"] == "oa_blocked (retry cap)"


@pytest.mark.parametrize("where", ["top", "project"])
def test_the_cap_is_configurable(tmp_path, monkeypatch, where):
    cfg, proj = _world(tmp_path, monkeypatch, **({"transient_max_attempts": 4} if where == "top" else {}))
    if where == "project":
        cfg["projects"]["P"]["transient_max_attempts"] = 4
    _residual(proj, [("10.1000/three", mig.TRANSIENT, 3, "pmc: OUTAGE"),
                     ("10.1000/four", mig.TRANSIENT, 4, "pmc: OUTAGE")])
    res = _route(cfg)
    assert res["transient_capped"] == 1 and set(_retry(proj)) == {"10.1000/three"}


@pytest.mark.parametrize("bad", [0, -1, True, "10", 2.5])
def test_a_bad_cap_is_a_config_error(tmp_path, monkeypatch, bad):
    cfg, _ = _world(tmp_path, monkeypatch, transient_max_attempts=bad)
    with pytest.raises(config.ConfigError):
        config.transient_max_attempts("P", cfg=cfg)
    assert _route(cfg)["status"] == "config"


def test_default_cap_is_ten(tmp_path, monkeypatch):
    cfg, _ = _world(tmp_path, monkeypatch)
    assert config.transient_max_attempts("P", cfg=cfg) == 10
