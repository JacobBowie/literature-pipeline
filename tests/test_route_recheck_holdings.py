"""Routing re-checks holdings on a typed chain (2026-10-29).

VAP/KINS4500_ExPhys chain h1harvest.2026-10-09.2 was swept 10-09 and routed 10-26. Its typed
residual kept sweep's classes, and route only ran the holdings check for legacy chains, so 14 ILL
and 5 browser rows that were held with a PDF by 10-26 still reached the worklists."""
import contextlib
import csv
import datetime
import io
import json
import types
from pathlib import Path

import lit_util
import migrate_closed_to_md as mig
import sweep

RUN, TAG = "2026-10-09.2", "h1harvest"
FIELDS = list(sweep.QUEUE_COLUMNS) + ["residual_class", "reason", "stages", "held_at", "skipped_sources",
                                      "attempts", "run_id"]


class _HoldMap:
    def __init__(self, held):
        self.held = {d.lower() for d in held}

    def content(self, doi):
        return [types.SimpleNamespace(has_pdf=True, path=Path(f"lib/{doi.replace('/', '_')}.pdf"))] \
            if str(doi).lower() in self.held else []


def _world(tmp_path, monkeypatch):
    root = tmp_path / "Projects"
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    cfg = {"state_dir": str(tmp_path / "state"), "projects": {"P": {"lib_dir": "literature"}}}
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
        for doi, cls, reason in rows:
            w.writerow({"doi": doi, "title": f"T {doi}", "authors": "A", "destination": "literature",
                        "residual_class": cls, "reason": reason, "attempts": "1", "run_id": RUN})


def _route(cfg, held):
    with contextlib.redirect_stdout(io.StringIO()):
        return mig.run("P", run_id=RUN, tags=[TAG], cfg=cfg, holdmap=_HoldMap(held),
                       today=datetime.date(2026, 10, 26))


def test_typed_rows_held_at_routing_time_leave_the_worklists(tmp_path, monkeypatch):
    cfg, proj = _world(tmp_path, monkeypatch)
    ill_held = [f"10.1000/ill.held.{i}" for i in range(14)]
    ill_open = [f"10.1000/ill.open.{i}" for i in range(3)]
    oab_held = [f"10.1000/oab.held.{i}" for i in range(5)]
    oab_open = [f"10.1000/oab.open.{i}" for i in range(2)]
    _residual(proj, [(d, mig.TERMINAL_CLOSED, "unpaywall: NOT_AVAILABLE") for d in ill_held + ill_open]
              + [(d, mig.OA_BLOCKED, "unpaywall: HTTP_403") for d in oab_held + oab_open])

    res = _route(cfg, ill_held + oab_held)
    assert res["status"] == "ok" and res["held_at_routing"] == 19
    assert res["counts"].get(mig.HELD_ELSEWHERE) == 19
    assert res["counts"].get(mig.TERMINAL_CLOSED) == 3 and res["counts"].get(mig.OA_BLOCKED) == 2

    ill = (proj / mig.ILL_NAME).read_text(encoding="utf-8").lower()
    oab = (proj / mig.OA_BLOCKED_NAME).read_text(encoding="utf-8").lower()
    assert not any(d in ill for d in ill_held) and all(d in ill for d in ill_open)       # control stays ILL
    assert not any(d in oab for d in oab_held) and all(d in oab for d in oab_open)       # control stays browser
    retry = (proj / mig.RETRY_LATER_NAME).read_text(encoding="utf-8").lower()
    assert not any(d in retry for d in oab_held) and all(d in retry for d in oab_open)

    routing = {r["doi"]: r for r in csv.DictReader(
        open(proj / f"lit_pull_queue.{TAG}.{RUN}.routing.csv", encoding="utf-8"))}
    r = routing[ill_held[0]]
    assert r["residual_class"] == mig.HELD_ELSEWHERE and r["route"] == "none"
    assert "swept as TERMINAL_CLOSED" in r["reason"] and r["held_paths"].endswith(".pdf")
    assert routing[ill_open[0]]["residual_class"] == mig.TERMINAL_CLOSED


def test_no_holdings_map_keeps_sweeps_classes(tmp_path, monkeypatch):
    cfg, proj = _world(tmp_path, monkeypatch)
    _residual(proj, [("10.1000/a", mig.TERMINAL_CLOSED, "unpaywall: NOT_AVAILABLE")])
    with contextlib.redirect_stdout(io.StringIO()):
        res = mig.run("P", run_id=RUN, tags=[TAG], cfg=cfg, use_holdings=False,
                      today=datetime.date(2026, 10, 26))
    assert res["held_at_routing"] == 0 and res["counts"].get(mig.TERMINAL_CLOSED) == 1
