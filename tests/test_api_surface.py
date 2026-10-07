"""The external API surface other projects import (dispatch 0.6). Pinned from W1 on.

Consumers are live: other projects import the flat modules, some drive unpaywall_fetch_v2
through sys.argv, and a citation-network build
reads portfolio.duckdb. A name may gain parameters or change inside; it may not disappear.
tests/test_imports.py (every script imports cleanly, twice) stays alongside this file.
"""
import importlib
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent

# module -> (callables, other attributes)
SURFACE = {
    "lit_util": (["atomic_write_text", "atomic_write_csv", "normalize_doi", "is_valid_doi",
                  "is_suspicious_doi", "extract_doi_from_text", "load_projects_config",
                  "project_root", "utf8_stdout", "coerce_int"],
                 ["PROJECTS_ROOT", "DEFAULT_EMAIL"]),
    "ris_emit": (["load_projects_config", "build_ris", "write_ris", "crossref_meta",
                  "crossref_by_doi", "crossref_by_title", "resolve_meta", "canonical_stem",
                  "emit_ris_for_pdf", "warn_if_default_email"],
                 ["_RIS_TYPE"]),
    "unpaywall_fetch_v2": (["build_filename", "last_name", "slug_title", "main"], []),
    "lit_net": (["get", "stream_download", "doi_to_pmcid_batch"], ["IDCONV"]),
    "litpipe.outcomes": (["from_legacy", "legacy_outcome"], ["Kind", "Outcome"]),
    "litpipe.config": (["load", "db_dir", "state_dir", "sources", "auto_stage",
                        "walk_cadence_days", "hosts", "s2", "openalex"],
                       ["CONFIG_PATH", "ConfigError"]),
    "litpipe.state": (["acquire", "release", "defer", "refuse", "is_refused", "clear_refusal",
                       "day_count", "kv_get", "kv_set", "register_run", "heartbeat",
                       "finish_run", "live_runs", "main"],
                      ["BudgetExhausted", "HostDeferred"]),
    "litpipe.preflight": (["run", "ok", "exit_code", "main"], []),
    # W1 integration: the 0.5 contracts later waves build against
    "litpipe.net": (["request", "get", "post", "user_agent", "retry_after_seconds",
                     "expect_json", "expect_pdf"], ["Response"]),
    "litpipe.hosts": (["policy", "register", "check", "prohibited"],
                      ["HostPolicy", "RetryPolicy", "ProhibitedHost"]),
    "litpipe.ledger": (["redact", "redact_headers", "write", "read", "set_run_id",
                        "current_run_id"], []),
    "litpipe.doi": (["candidates", "iter_candidates", "normalise", "encode_path", "resolve_first",
                     "is_placeholder"], ["ResolverUnavailable"]),
    "litpipe.text": (["strip_tags", "unescape", "clean_field", "normalise_title"], []),
    "litpipe.identity": (["check", "doc_kind", "suspect_file", "text_stats"],
                         ["Verdict", "SuspectCheck"]),
    "litpipe.holdings": (["build"], ["HoldMap", "Holding"]),
    # W3a: the OpenAlex client W3-A builds on
    "litpipe.openalex": (["works_by_doi", "works_by_id", "referenced_works", "referenced_works_many",
                          "citing_works", "content_pdf", "key_present", "apply_host_policy"],
                         ["RefList", "Page", "Session"]),
    # W3b: the forward walk (snowball and the W4 runner call it) and its cache module
    "forward_citations": (["run", "main", "verdict", "doi_from_ris", "read_report", "library_seeds"],
                          ["FIELDS", "SUMMARY_MARKER", "CONFIG_PATH"]),
    "litpipe.walk": (["cache_path", "needs_walk", "route", "plan"],
                     ["Cache", "CacheLocked", "CacheWriteError", "CACHE_PATH", "CACHE_NAME"]),
    # W4a: the canaries and the worklists the W4 runner calls in-process, and the paywall queue paywall_pull imports
    "litpipe.canaries": (["run", "report", "summary", "planned_requests", "checks", "worst_case", "main"],
                         ["PROFILES", "CHECKS", "Check"]),
    "litpipe.worklists": (["oa_blocked", "group_by_host", "render_oa_worklist", "ill_list", "render_ill_list",
                           "seed_coverage", "residual_csvs", "read_residuals", "doi_link", "run", "main"],
                          ["Pool", "WorklistError", "PoolStateError"]),
    # W4b: the scheduled runner and run_daily as its wrapper
    "litpipe.runner": (["run", "batch", "status", "schedule_text", "schedule_print", "main", "shim",
                        "read_result", "classify", "earlier_runner", "batch_tag", "subprocess_launcher",
                        "inprocess_launcher"],
                       ["RESULT_TABLE", "STAGE_MODULES", "ProcessTree"]),
    "run_daily": (["run", "main", "exit_code", "queue_data_rows"], []),
    "build_priority_paywall_queue": (["lib_dois", "load_libs", "run", "main", "portfolio_dir"],
                                     ["LIBS", "ROOT", "CONFIG_PATH"]),
    # W5: the pool-selection gate (the VAP fold-in's engine) and paywall_pull's library-facing surface
    "litpipe.gate": (["run", "main", "load_spec", "gate", "plan", "promote", "read_snapshot", "doi_key_fn",
                      "compile_pattern"],
                     ["SCHEMA", "ENGINE_VERSION", "SpecError", "GateAbort", "GateError"]),
    "paywall_pull": (["run", "main", "build_url", "plan_ingest"], []),
}

CASES = [(mod, name, True) for mod, (fns, _) in SURFACE.items() for name in fns] + \
        [(mod, name, False) for mod, (_, attrs) in SURFACE.items() for name in attrs]


@pytest.mark.parametrize("mod, name, must_call", CASES,
                         ids=[f"{m}.{n}" for m, n, _ in CASES])
def test_name_is_importable(mod, name, must_call):
    m = importlib.import_module(mod)
    assert hasattr(m, name), f"{mod}.{name} is part of the external API (dispatch 0.6)"
    if must_call:
        assert callable(getattr(m, name))


def test_default_email_name_stays_whatever_its_value():
    import lit_util
    # DEC-13 may set it to None; consumers still import the name.
    assert "DEFAULT_EMAIL" in vars(lit_util)
    assert lit_util.DEFAULT_EMAIL is None or isinstance(lit_util.DEFAULT_EMAIL, str)


def test_ris_emit_load_projects_config_still_exits_2_when_missing(tmp_path, capsys):
    import ris_emit
    with pytest.raises(SystemExit) as e:
        ris_emit.load_projects_config(tmp_path / "projects.json")
    assert e.value.code == 2
    assert "projects.json.template" in capsys.readouterr().err


def test_unpaywall_fetch_v2_imports_and_runs_under_the_argv_hack():
    # a consumer sets sys.argv, imports the module, then calls main(): the import must not parse
    # argv and main() must read sys.argv. A fresh interpreter, so no module here is reloaded
    # (a reload would split function identities other tests pin).
    code = ("import sys; sys.path.insert(0, sys.argv[1]); "
            "sys.argv = ['unpaywall_fetch_v2.py', '--no-such-flag', 'junk']; "
            "import unpaywall_fetch_v2 as m; "
            "sys.argv = ['unpaywall_fetch_v2.py', '--help']; m.main()")
    p = subprocess.run([sys.executable, "-c", code, str(REPO)], capture_output=True, text=True,
                       timeout=120)
    assert p.returncode == 0, p.stderr
    assert "--top-n" in p.stdout


PORTFOLIO_TABLES = {
    "paper_metadata": ({"doi", "year", "lastname", "title", "venue", "authors", "abstract",
                        "abstract_attempted_at", "refreshed_at"}, ["doi"]),
    "candidates": ({"doi", "source_type", "source_seed_doi", "source_project", "citing_cited_by",
                    "refreshed_at"}, ["doi", "source_type", "source_seed_doi", "source_project"]),
    "cites": ({"citing_doi", "cited_doi", "source_pipeline", "source_project"},
              ["citing_doi", "cited_doi", "source_project"]),
}


@pytest.mark.parametrize("table", sorted(PORTFOLIO_TABLES))
def test_portfolio_tables_keep_their_columns_and_keys(table):
    duckdb = pytest.importorskip("duckdb")
    import index_portfolio
    con = duckdb.connect(":memory:")
    try:
        con.execute(index_portfolio.SCHEMA)
        cols = {r[0] for r in con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
            [table]).fetchall()}
        pk = con.execute(
            "SELECT constraint_column_names FROM duckdb_constraints() "
            "WHERE table_name = ? AND constraint_type = 'PRIMARY KEY'", [table]).fetchone()
    finally:
        con.close()
    want_cols, want_pk = PORTFOLIO_TABLES[table]
    assert want_cols <= cols, f"{table} lost columns {sorted(want_cols - cols)}"  # new ones are fine
    assert pk is not None and list(pk[0]) == want_pk


def _source_text():
    files = list(REPO.glob("*.py")) + list((REPO / "litpipe").glob("*.py"))
    return "\n".join(f.read_text(encoding="utf-8", errors="replace") for f in files)


@pytest.mark.parametrize("literal", [
    "✅ Lit pull done:",
    "⏸️ Lit pull PARTIAL:",
])
def test_loose_ends_prefixes_are_still_written(literal):
    # A tripwire, not a behaviour test (test_sweep_loose_ends.py owns that): consumers grep
    # LOOSE_ENDS.md for these exact prefixes.
    assert literal in _source_text()


def test_queue_columns_are_still_the_contract():
    # Tripwire: the documented header (sweep.py) and the seeder's writer, the one tool that
    # creates queues. test_csv_schema.py owns the reader side.
    import seed_queue_from_top_candidates as seeder
    assert "doi,title,authors,year,destination,notes" in _source_text()
    src = Path(seeder.__file__).read_text(encoding="utf-8")
    assert '["doi", "title", "authors", "year", "destination", "notes"]' in src


@pytest.mark.skipif(not (REPO / ".venv").exists(), reason="no .venv in this checkout (CI, worktree)")
def test_consumer_venv_python_path_exists():
    # a consumer project calls this repo's interpreter by absolute path.
    exe = REPO / ".venv" / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")
    assert exe.exists()
