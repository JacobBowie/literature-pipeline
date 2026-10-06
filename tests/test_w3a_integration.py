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


def test_a_library_that_lists_empty_keeps_its_index_rows(tmp_path, monkeypatch, capsys):
    """Verifier G's observation: the DOI-keyed GC deletes every row of a project whose library lists
    nothing (the old prune skipped an empty listing). A synced folder can list empty for a moment,
    so that is a skip (exit 2, rows kept); --allow-empty-library accepts a library emptied on purpose."""
    import json
    import duckdb
    import lit_util
    root = tmp_path / "root"
    lib = root / "research_a" / "literature"
    lib.mkdir(parents=True)
    ris = "TY  - JOUR\nAU  - Smith, Jane\nPY  - 2019\nTI  - Title\nDO  - 10.5555/held.2019.001\nER  - \n"
    (lib / "2019_Smith_Title.pdf").write_bytes(b"%PDF-1.4 stub")
    (lib / "2019_Smith_Title.ris").write_text(ris, encoding="utf-8")
    cfg = tmp_path / "projects.json"
    cfg.write_text(json.dumps({"state_dir": str(tmp_path / "state"),
                               "projects": {"research_a": {"lib_dir": "literature"}}}), encoding="utf-8")
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    monkeypatch.setattr(index_portfolio, "CONFIG_PATH", cfg)
    db = tmp_path / "refs" / "portfolio.duckdb"

    def rows():
        con = duckdb.connect(str(db), read_only=True)
        try:
            return con.execute("SELECT COUNT(*) FROM paper_locations WHERE project = 'research_a'").fetchone()[0]
        finally:
            con.close()

    assert index_portfolio.main(["--db", str(db)]) == 0 and rows() == 1
    for f in lib.iterdir():
        f.unlink()
    capsys.readouterr()
    assert index_portfolio.main(["--db", str(db)]) == 2
    out = capsys.readouterr().out
    assert rows() == 1 and "lists no PDF, sidecar or .ris" in out
    assert out.strip().splitlines()[-1].startswith("[step-summary] ")
    assert index_portfolio.main(["--db", str(db), "--allow-empty-library"]) == 0 and rows() == 0


LINE_START_ADDRESS = "Corresponding author:\njo.author@uni.example\nReceived 2024"


def _read_json(path):
    import json
    return json.loads(Path(path).read_text(encoding="utf-8"))


def test_redact_obj_keeps_the_record_valid_and_the_address_out():
    """Verifier H (H1): redacting the JSON encoding turned "\\njo@..." into "\\REDACTED", an invalid
    escape; redact_obj redacts the decoded values instead."""
    from litpipe import ledger
    out = ledger.redact_obj({"text": LINE_START_ADDRESS, "pages": [LINE_START_ADDRESS], "n": 3})
    assert "jo.author@uni.example" not in str(out) and out["n"] == 3
    assert out["text"].startswith("Corresponding author:\n") and out["text"].endswith("\nReceived 2024")


def test_preprint_text_sidecar_survives_an_address_after_a_line_break(tmp_path):
    """Sibling of H1 (preprint_fetch.py, the Europe PMC preprint text-only sidecar): full text puts an
    author address at a line start often, and the old write raised JSONDecodeError."""
    from types import SimpleNamespace
    import preprint_fetch
    ctx = SimpleNamespace(lib_dir=str(tmp_path))
    row = SimpleNamespace(doi="10.5555/p.1")
    c = SimpleNamespace(doi="10.5555/p.1", ppr="PPR1", server="medrxiv")
    got = SimpleNamespace(text={"text": LINE_START_ADDRESS})
    assert preprint_fetch._write_text_sidecar(ctx, row, c, "2024_Author_Title.pdf", got) == "OK"
    rec = _read_json(tmp_path / "2024_Author_Title.fulltext.json")
    assert "jo.author@uni.example" not in rec["text"] and rec["has_pdf"] is False


def test_identity_sidecar_survives_an_address_after_a_line_break(tmp_path):
    """Sibling of H1 (unpaywall_fetch_v2.write_identity_sidecar): the verdict's evidence is free text."""
    from types import SimpleNamespace
    import unpaywall_fetch_v2 as U
    verdict = SimpleNamespace(as_dict=lambda: {"identity": "OK", "identity_evidence": LINE_START_ADDRESS})
    attempt = SimpleNamespace(url="https://pub.example/a.pdf", host="pub.example", host_type="publisher",
                              version="publishedVersion")
    pdf = tmp_path / "2024_Author_Title.pdf"
    path = U.write_identity_sidecar(str(pdf), "10.5555/p.1", verdict, "ARTICLE", 9, attempt)
    rec = _read_json(path)
    assert "jo.author@uni.example" not in rec["identity_evidence"] and rec["identity"] == "OK"


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
