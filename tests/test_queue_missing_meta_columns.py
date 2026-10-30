"""A queue without title/authors/year columns is a queue with them blank (2026-10-30).

The README says only `doi` and `destination` are required. A `doi,destination` queue (the new
"Single project, no runner" path; repro DOI 10.1371/journal.pmed.1003845) got its metadata looked
up, but prepare_rows rewrote the normalized queue with the queue's own header, so the filled
title/authors/year were dropped and the stages named the file Unknown_<Lastname>_Untitled."""
import csv

import pytest

import sweep
import unpaywall_fetch_v2
from tests.test_w1_verify_b import World, read_csv, write_csv  # the end-to-end sweep world

DOI = "10.1371/journal.pmed.1003845"
META = {"title": "Global burden of heat", "authors": [{"family": "Smith", "given": "Jane"}], "year": 2021}


@pytest.fixture
def world(tmp_path, monkeypatch):
    w = World(tmp_path, monkeypatch)
    calls = []

    def meta(doi):
        calls.append(doi)
        return dict(META), "crossref"

    monkeypatch.setattr(sweep, "_resolve_meta", meta)
    w.meta_calls, w.triage = calls, []
    stages = w.stages

    def run(cmd, **kw):   # the triage CSV is deleted when the queue retires: read it while it exists
        cmd = [str(c) for c in cmd]
        if any(c.endswith("unpaywall_fetch_v2.py") for c in cmd):
            w.triage.extend(read_csv(cmd[cmd.index("--triage") + 1]))
        return stages(cmd, **kw)

    monkeypatch.setattr(sweep.subprocess, "run", run)
    return w


def _queue(w, fields, row):
    write_csv(w.proj / "lit_pull_queue.csv", [row], fields)


def _triage_title(w):
    (row,) = w.triage
    return row


def test_a_doi_destination_only_queue_gets_its_metadata_filled(world):
    _queue(world, ["doi", "destination"], {"doi": DOI, "destination": "lit"})
    world.sweep("2026-10-30")
    assert world.meta_calls == [DOI]
    row = _triage_title(world)
    assert (row["title"], row["authors"], row["year"]) == ("Global burden of heat", "Smith J", "2021")
    name = unpaywall_fetch_v2.build_filename(row["year"], row["authors"], row["title"])
    assert name.startswith("2021_Smith_") and "Untitled" not in name and "Unknown" not in name


def test_a_present_and_blank_queue_behaves_as_before(world):
    _queue(world, list(sweep.QUEUE_COLUMNS), {"doi": DOI, "title": "", "authors": "", "year": "",
                                              "destination": "lit", "notes": ""})
    world.sweep("2026-10-30")
    row = _triage_title(world)
    assert (row["title"], row["year"]) == ("Global burden of heat", "2021")


def test_a_present_and_filled_queue_is_not_overwritten(world):
    _queue(world, list(sweep.QUEUE_COLUMNS), {"doi": DOI, "title": "My own title", "authors": "Doe J",
                                              "year": "2020", "destination": "lit", "notes": ""})
    world.sweep("2026-10-30")
    assert world.meta_calls == []                          # nothing blank: no lookup
    row = _triage_title(world)
    assert (row["title"], row["authors"], row["year"]) == ("My own title", "Doe J", "2020")


def test_the_residual_of_a_doi_destination_queue_carries_the_filled_title(world):
    _queue(world, ["doi", "destination"], {"doi": DOI, "destination": "lit"})
    world.sweep("2026-10-30")
    (res,) = [p for p in world.proj.glob("lit_pull_queue.2026-10-30*.residual.csv")]
    (row,) = list(csv.DictReader(open(res, encoding="utf-8")))
    assert row["title"] == "Global burden of heat" and row["year"] == "2021"
