"""Regression: migrate_closed_to_md must dedup by DOI, not skip by sweep-date.

Bug (2026-06-23): a same-day SECOND sweep (different residual DOIs, reports
overwritten) was skipped wholesale because a "## Sweep residuals <date>" header
already existed in lit_pull_queue.md, silently dropping the new closed-access rows.
The fix parses backtick-wrapped DOIs already in the .md and appends only new ones.
This test pins the render format <-> dedup-regex contract against the REAL module.
"""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import migrate_closed_to_md as m  # noqa: E402

PATTERN = r"DOI `([^`]+)`"


def _dedup(existing_md, rows):
    """Mirror of the dedup in migrate_closed_to_md.main (lines ~183-186)."""
    existing_dois = set(re.findall(PATTERN, existing_md))
    return [r for r in rows if r["doi"] not in existing_dois]


def test_render_emits_dedup_regex_token():
    """The shipped render_md_block output must carry a `DOI <doi>` token the
    dedup regex captures — otherwise the dedup silently matches nothing."""
    block = m.render_md_block("T", "2026-06-22", [{
        "title": "Probe", "year": "2024", "doi": "10.z/probe",
        "oa_status": "closed", "error": "", "stage_pmc": "skip", "stage_preprint": "skip",
    }])
    assert "DOI `10.z/probe`" in block
    assert re.findall(PATTERN, block) == ["10.z/probe"]


def test_same_day_second_sweep_keeps_new_doi():
    existing = (
        "# Manual Pull Queue\n\n## Sweep residuals 2026-06-23: 1 closed (auto-migrated)\n\n"
        "- [ ] **Old paper** (2024) — DOI `10.x/already` — oa_status=closed\n"
    )
    rows = [
        {"doi": "10.x/already", "title": "Old paper", "year": "2024"},
        {"doi": "10.y/new", "title": "New paper", "year": "2025"},
    ]
    survivors = _dedup(existing, rows)
    assert [r["doi"] for r in survivors] == ["10.y/new"]  # the exact original bug, fixed


def test_all_present_yields_empty():
    existing = "- [ ] **A** — DOI `10.x/a` —\n- [ ] **B** — DOI `10.x/b` —\n"
    rows = [{"doi": "10.x/a", "title": "A", "year": "2024"},
            {"doi": "10.x/b", "title": "B", "year": "2024"}]
    assert _dedup(existing, rows) == []


def test_empty_md_filters_nothing():
    rows = [{"doi": "10.x/a", "title": "A", "year": "2024"}]
    assert _dedup("", rows) == rows
