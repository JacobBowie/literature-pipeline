"""migrate_closed_to_md dedups by DOI in any form, not by sweep date and not only in backticks.

History. 2026-06-23: a same-day SECOND sweep was skipped wholesale because a "## Sweep residuals
<date>" header already existed, silently dropping its new rows; the fix parsed backtick-wrapped
DOIs. REG-I25 (W1-D2): the curated lists also carry DOIs as doi.org links, backtick-in-link and bare,
in any case, so a backtick-only dedup re-appended them. These tests run the REAL module against real
queue lines (tests/fixtures/W1-D2/md_*.md, copied read-only from consumer projects).
"""
import re
from pathlib import Path

import pytest

import migrate_closed_to_md as m
from litpipe import holdings
from tests.test_migrate_routing import RUN, chain, env, route, row, text  # noqa: F401 (env is a fixture)

FIX = Path(__file__).parent / "fixtures" / "W1-D2"
PATTERN = r"DOI `([^`]+)`"


def test_render_emits_the_backtick_token():
    """The ILL line still carries the `DOI \\`<doi>\\`` token (consumers and old tooling read it)."""
    block = m.render_md_block("T", "2026-06-22", [{
        "title": "Probe", "year": "2024", "doi": "10.z/probe",
        "oa_status": "closed", "error": "", "stage_pmc": "skip", "stage_preprint": "skip",
    }])
    assert "DOI `10.z/probe`" in block
    assert re.findall(PATTERN, block) == ["10.z/probe"]


@pytest.mark.parametrize("fixture", ["md_link_form.md", "md_backtick_link.md", "md_ill_backtick.md",
                                     "md_blocked_oa.md", "md_residuals_email.md"])
def test_existing_dois_reads_every_real_form(fixture):
    body = (FIX / fixture).read_text(encoding="utf-8")
    found = m.existing_dois(body)
    assert all(x == x.lower() for x in found)
    for line in body.splitlines():
        (d,) = set(holdings.extract_dois(line))
        assert d in found


@pytest.mark.parametrize("fixture", ["md_link_form.md", "md_backtick_link.md", "md_ill_backtick.md"])
def test_run_does_not_reappend_a_doi_listed_in_another_form(env, fixture):
    """REG-I25 end to end: residual rows whose DOIs already sit in lit_pull_queue.md (as links,
    backtick-in-link or backtick, any case) are not appended again; a genuinely new DOI is, once."""
    body = (FIX / fixture).read_text(encoding="utf-8")
    (env.proj / m.ILL_NAME).write_text("# Manual Pull Queue\n\n" + body, encoding="utf-8")
    listed = [holdings.extract_dois(l)[0] for l in body.splitlines()]
    rows = [row(("https://doi.org/" + d.upper()) if i % 2 else d.upper()) for i, d in enumerate(listed)]
    chain(env.proj, RUN, rows + [row("10.1234/genuinely-new9")])
    route(env)
    after = (env.proj / m.ILL_NAME).read_text(encoding="utf-8")
    assert after.startswith("# Manual Pull Queue\n\n" + body)          # curated lines untouched
    added = after[len("# Manual Pull Queue\n\n" + body):]
    assert holdings.extract_dois(added) == ["10.1234/genuinely-new9"]


def test_same_day_second_sweep_keeps_new_doi(env):
    """The 2026-06-23 bug, through the real run(): a second run the same day with new residuals."""
    (env.proj / m.ILL_NAME).write_text(
        "# Manual Pull Queue\n\n## Sweep residuals 2026-09-30: 1 closed (auto-migrated)\n\n"
        "- [ ] **Old paper** (2024) — DOI `10.1234/already9` — oa_status=closed\n", encoding="utf-8")
    chain(env.proj, "2026-09-30.2", [row("10.1234/already9"), row("10.1234/new9")])
    route(env, run_id="2026-09-30.2")
    body = text(env.proj / m.ILL_NAME)
    assert body.count("10.1234/already9") == 1 and body.count("10.1234/new9") == 1


def test_a_doi_in_two_chains_of_one_run_is_listed_once(env):
    chain(env.proj, RUN, [row("10.1234/twice9")])
    chain(env.proj, RUN, [row("10.1234/TWICE9")], tag="ch15")
    route(env)
    assert holdings.extract_dois(text(env.proj / m.ILL_NAME)) == ["10.1234/twice9"]


def test_oa_blocked_worklist_dedups_against_the_vap_form(env):
    body = (FIX / "md_blocked_oa.md").read_text(encoding="utf-8")
    (env.proj / m.OA_BLOCKED_NAME).write_text("# P: open access, blocked\n\n" + body, encoding="utf-8")
    listed = [holdings.extract_dois(l)[0] for l in body.splitlines()]
    blocked = {"oa_status": "OA", "attempts": "publisher/publishedVersion/HTTP_403", "error": "HTTP_403"}
    chain(env.proj, RUN, [row(listed[0], u=blocked), row("10.1234/new403", u=blocked)])
    route(env)
    added = text(env.proj / m.OA_BLOCKED_NAME)[len("# P: open access, blocked\n\n" + body):]
    assert holdings.extract_dois(added) == ["10.1234/new403"]


def test_a_malformed_doi_this_tool_listed_is_not_listed_again(env):
    """A non-DOI-shaped token written as `DOI \\`...\\`` still dedups (the pre-REG-I25 contract)."""
    assert m.existing_dois("- [ ] **A** — DOI `10.x/a` —") == {"10.x/a"}
    assert m.existing_dois("") == set()
