"""W3a seam locks (dispatcher, 2026-10-05): builders in separate trees against a written contract."""
from pathlib import Path

import pytest

import enrich_recommendations
import index_portfolio
import reverse_citations
import snowball


def _record(calls):
    def runner(cmd, label):
        calls.append(list(cmd))
        return snowball.StepResult(label, list(cmd), 0, None)
    return runner


def _cmd(calls, stem):
    return next(c for c in calls if Path(c[1]).stem == stem)


def test_with_recs_asks_for_the_recent_feed(monkeypatch):
    """W3-E made enrich_recommendations do nothing without --recent-feed, so snowball --with-recs
    must pass it, and the flag must be one the real parser accepts."""
    calls = []
    opts = snowball.Options(with_recs=True, skip_abstracts=True)
    snowball.post_loop(opts, _record(calls))
    cmd = _cmd(calls, "enrich_recommendations")
    assert "--recent-feed" in cmd
    seen = {}
    monkeypatch.setattr(enrich_recommendations, "run", lambda **kw: seen.update(kw) or {"exit_code": 0})
    assert enrich_recommendations.main(cmd[2:]) == 0
    assert seen.get("recent_feed") is True


@pytest.mark.parametrize("key, expect", [(None, ["--sources", "openalex,crossref,regex"]), ("k-test", [])])
def test_reverse_step_leaves_out_s2_without_a_key(monkeypatch, key, expect):
    """Unkeyed S2 answers 429 at once, which would make every iteration DEGRADED (W3-B forward 2):
    without S2_API_KEY the reverse step walks OpenAlex, Crossref and the local parse only."""
    if key is None:
        monkeypatch.delenv("S2_API_KEY", raising=False)
    else:
        monkeypatch.setenv("S2_API_KEY", key)
    calls = []
    snowball.one_iteration("teaching_a", skip_forward=True, skip_reverse=False, step_runner=_record(calls))
    cmd = _cmd(calls, "reverse_citations")
    tail = cmd[cmd.index("teaching_a") + 1:]
    assert tail == expect
    if expect:
        assert reverse_citations.parse_sources(expect[1]) == {"openalex", "crossref", "regex"}


def test_every_structured_reverse_source_is_one_the_index_trusts():
    """W3-B writes `source` per row of _reverse_citations_parsed.csv; W3-C1 trusts the structured ones
    (kept whole by its DOI guard) and treats `regex` as text-derived. A new source in one module and
    not the other would silently change how its DOIs are normalised."""
    assert set(reverse_citations.ROW_SOURCES) - {"regex"} == set(index_portfolio.STRUCTURED_SOURCES)
    assert "regex" not in index_portfolio.STRUCTURED_SOURCES
