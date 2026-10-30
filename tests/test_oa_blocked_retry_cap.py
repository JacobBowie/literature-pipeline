"""OA_BLOCKED retry cap (2026-10-30, Jacob: "OA blocked should stop after N attempts", N = 3).

From 10-23 on, the same 8 FRED rows (publishers refusing scripted downloads) re-ran every third
night with no end. At oa_blocked_max_attempts sweeps a row leaves retry_later and stays on the
browser worklist only, which the headed browser works from."""
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

RUN, TAG = "2026-10-29", "retry"
FIELDS = list(sweep.QUEUE_COLUMNS) + ["residual_class", "reason", "stages", "held_at", "skipped_sources",
                                      "attempts", "run_id"]


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
    with open(proj / f"lit_pull_queue.{TAG}.{RUN}.residual.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        w.writeheader()
        for doi, cls, attempts in rows:
            w.writerow({"doi": doi, "title": f"T {doi}", "authors": "A", "destination": "literature",
                        "residual_class": cls, "reason": "unpaywall: HTTP_403", "attempts": str(attempts),
                        "run_id": RUN})


def _route(cfg):
    with contextlib.redirect_stdout(io.StringIO()):
        return mig.run("P", run_id=RUN, tags=[TAG], cfg=cfg, use_holdings=False,
                       today=datetime.date(2026, 10, 29))


def _retry_dois(proj):
    p = proj / mig.RETRY_LATER_NAME
    return {r["doi"] for r in csv.DictReader(open(p, encoding="utf-8"))} if p.exists() else set()


def test_oa_blocked_at_the_cap_leaves_retry_later_and_stays_on_the_worklist(tmp_path, monkeypatch):
    cfg, proj = _world(tmp_path, monkeypatch)
    # a capped row already waiting in retry_later (FRED's 8 sat there at 6 attempts) is dropped too
    with open(proj / mig.RETRY_LATER_NAME, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=mig.RETRY_FIELDS)
        w.writeheader()
        w.writerow({"doi": "10.1000/six", "residual_class": "OA_BLOCKED", "attempts": "5",
                    "not_before": "2026-11-01"})
        w.writerow({"doi": "10.1000/other", "residual_class": "TRANSIENT", "attempts": "1",
                    "not_before": "2026-10-30"})
    _residual(proj, [("10.1000/two", mig.OA_BLOCKED, 2), ("10.1000/three", mig.OA_BLOCKED, 3),
                     ("10.1000/six", mig.OA_BLOCKED, 6), ("10.1000/t", mig.TRANSIENT, 5)])
    res = _route(cfg)
    assert res["oa_blocked_capped"] == 2
    assert _retry_dois(proj) == {"10.1000/two", "10.1000/t", "10.1000/other"}   # below cap and TRANSIENT keep retrying
    assert "1 dropped at the retry cap" in res["written"][mig.RETRY_LATER_NAME]
    oab = (proj / mig.OA_BLOCKED_NAME).read_text(encoding="utf-8")
    assert all(d in oab for d in ("10.1000/two", "10.1000/three", "10.1000/six"))   # browser worklist keeps all
    routing = {r["doi"]: r for r in csv.DictReader(
        open(proj / f"lit_pull_queue.{TAG}.{RUN}.routing.csv", encoding="utf-8"))}
    assert routing["10.1000/three"]["route"] == "oa_blocked (retry cap)"
    assert "retry cap 3 reached" in routing["10.1000/three"]["reason"]
    assert routing["10.1000/two"]["route"] == "oa_blocked+retry_later"


@pytest.mark.parametrize("where", ["top", "project"])
def test_the_cap_is_configurable(tmp_path, monkeypatch, where):
    cfg, proj = _world(tmp_path, monkeypatch, **({"oa_blocked_max_attempts": 5} if where == "top" else {}))
    if where == "project":
        cfg["projects"]["P"]["oa_blocked_max_attempts"] = 5
    _residual(proj, [("10.1000/three", mig.OA_BLOCKED, 3), ("10.1000/five", mig.OA_BLOCKED, 5)])
    res = _route(cfg)
    assert res["oa_blocked_capped"] == 1 and _retry_dois(proj) == {"10.1000/three"}


@pytest.mark.parametrize("bad", [0, -1, True, "3", 2.5])
def test_a_bad_cap_is_a_config_error(tmp_path, monkeypatch, bad):
    cfg, _ = _world(tmp_path, monkeypatch, oa_blocked_max_attempts=bad)
    with pytest.raises(config.ConfigError):
        config.oa_blocked_max_attempts("P", cfg=cfg)
    assert _route(cfg)["status"] == "config"


def test_default_cap_is_three(tmp_path, monkeypatch):
    cfg, _ = _world(tmp_path, monkeypatch)
    assert config.oa_blocked_max_attempts("P", cfg=cfg) == 3
