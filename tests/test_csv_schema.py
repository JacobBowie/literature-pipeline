"""Schema tests for the lit_pull_queue CSV contract.

The pipeline reads/writes `lit_pull_queue.csv` as the user-facing queue.
This contract is documented in README §The lit_pull_queue contract:

  doi,title,authors,year,destination,notes

These tests verify:
  - `sweep.first_destination()` finds the first `destination` value in a queue
  - sweep handles missing/blank destination gracefully
  - the path-traversal guard rejects destinations that escape the project root
  - CSV with unicode (accented authors) round-trips cleanly through utf-8 IO

Run-mode: pure stdlib + the pipeline's helpers, no network.
"""
from __future__ import annotations
import csv
from pathlib import Path

import pytest

import sweep


REQUIRED_COLUMNS = {"doi", "title", "authors", "year", "destination", "notes"}


def _write_queue(path: Path, rows: list[dict]) -> Path:
    """Write a lit_pull_queue.csv with the documented schema."""
    with open(path, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(REQUIRED_COLUMNS))
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in REQUIRED_COLUMNS})
    return path


@pytest.fixture
def sample_row():
    return {
        "doi":         "10.1152/jappl.1972.32.6.812",
        "title":       "Predicting rectal temperature",
        "authors":     "Givoni B; Goldman R",
        "year":        "1972",
        "destination": "literature/",
        "notes":       "baseline",
    }


class TestFirstDestination:
    def test_returns_first_destination(self, tmp_path, sample_row):
        q = _write_queue(tmp_path / "q.csv", [sample_row])
        assert sweep.first_destination(q) == "literature/"

    def test_empty_queue_returns_none_or_blank(self, tmp_path):
        q = _write_queue(tmp_path / "q.csv", [])
        out = sweep.first_destination(q)
        assert not out, f"expected falsy for empty queue, got {out!r}"

    def test_blank_destination_returns_blank(self, tmp_path, sample_row):
        sample_row["destination"] = ""
        q = _write_queue(tmp_path / "q.csv", [sample_row])
        out = sweep.first_destination(q)
        assert not out


class TestUnicodeRoundTrip:
    def test_accented_authors_survive(self, tmp_path):
        """The pipeline reads queues as utf-8 — accented names must round-trip."""
        row = {
            "doi":         "10.1038/example",
            "title":       "Heat acclimation in Périard cohorts",
            "authors":     "Périard JD; Mølmen Ø",
            "year":        "2023",
            "destination": "literature/",
            "notes":       "Müller correspondence",
        }
        q = _write_queue(tmp_path / "q.csv", [row])
        with open(q, encoding="utf-8") as fh:
            parsed = list(csv.DictReader(fh))
        assert len(parsed) == 1
        assert parsed[0]["authors"] == "Périard JD; Mølmen Ø"
        assert "Müller" in parsed[0]["notes"]


class TestPathTraversalGuard:
    """sweep.run_pipeline should reject destinations that escape project_root.

    The guard was added on 2026-05-12 after the red-team flagged that a
    malicious or mistyped queue could write outside the intended directory.
    """

    def test_traversal_destination_is_rejected(self, tmp_path, sample_row, capsys):
        project_dir = tmp_path / "my_project"
        project_dir.mkdir()
        sample_row["destination"] = "../../escape/"
        queue = _write_queue(project_dir / "lit_pull_queue.csv", [sample_row])

        result = sweep.run_pipeline(project_dir, queue, dry_run=True)
        assert result is None, "traversal destination should refuse to plan"

        err = capsys.readouterr().err
        assert "escapes project root" in err or "escape" in err.lower(), \
            f"expected a traversal-rejection message on stderr, got: {err!r}"

    def test_normal_destination_is_accepted(self, tmp_path, sample_row):
        project_dir = tmp_path / "my_project"
        project_dir.mkdir()
        sample_row["destination"] = "literature/"
        queue = _write_queue(project_dir / "lit_pull_queue.csv", [sample_row])

        result = sweep.run_pipeline(project_dir, queue, dry_run=True)
        # dry_run returns the planned paths; not None
        assert result is not None
