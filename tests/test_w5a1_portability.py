"""W5-A1 portability locks: the preflight fix hint names both platform families (PA01), harvest
and table extraction resolve their folders from the registry instead of one user's layout (Pb07,
Pd11), the legacy data_dir fallbacks of the index still work under their generic label (P03,
Pd13), and the DB messages assume no particular sync client (P09)."""
import json
import re
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))


# ------------------------------------------------------------------ PA01: the email fix hint
def test_email_fix_names_a_posix_and_a_windows_form():
    from litpipe import preflight
    fix = preflight.EMAIL_FIX
    assert "export LITPIPE_EMAIL=" in fix
    assert "setx LITPIPE_EMAIL" in fix
    assert "\n" not in fix


def test_check_email_detail_carries_both_forms():
    from litpipe import preflight
    o = preflight.check_email({})
    assert o.kind.name == "CONFIG"
    assert "export LITPIPE_EMAIL=" in o.detail and "setx LITPIPE_EMAIL" in o.detail


def test_a_long_unpaywall_message_never_cuts_the_fix():
    from litpipe import preflight
    from litpipe.outcomes import Kind, Outcome

    body = json.dumps({"message": "x" * 500}).encode()

    def request(method, url, **kw):
        return Outcome(Kind.CONFIG, status=422, host=preflight.UNPAYWALL_HOST, detail="422",
                       payload={"status": 422, "headers": {}, "body": body})

    o = preflight.check_unpaywall(request)
    assert o.kind is Kind.CONFIG
    assert o.detail.endswith(preflight.EMAIL_FIX)


# ------------------------------------------------------------------ Pb07: harvest_citations folders
def _registry(tmp_path, **top):
    reg = tmp_path / "projects.json"
    reg.write_text(json.dumps({"root": str(tmp_path / "root"), "projects": {}, **top}), encoding="utf-8")
    return reg


def test_harvest_output_defaults_to_db_dir_citations(tmp_path, monkeypatch, capsys):
    import harvest_citations
    db = tmp_path / "localdb"
    monkeypatch.setattr(harvest_citations, "CONFIG_PATH", _registry(tmp_path, db_dir=str(db)))
    assert harvest_citations.default_out_dir() == db / "citations"
    src = tmp_path / "src"
    src.mkdir()
    harvest_citations.main(["--source-dir", str(src)])          # dry run: nothing written
    out = capsys.readouterr().out
    assert f"out-dir:  {db / 'citations'}" in out
    assert not (db / "citations").exists()


def test_harvest_output_without_db_dir_is_the_projects_root_default(tmp_path, monkeypatch):
    import harvest_citations
    import lit_util
    monkeypatch.setattr(harvest_citations, "CONFIG_PATH", _registry(tmp_path))
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", tmp_path / "root")
    assert harvest_citations.default_out_dir() == tmp_path / "root" / "_references" / "citations"


def test_harvest_help_prints_no_resolved_home_path(capsys):
    import harvest_citations
    with pytest.raises(SystemExit) as e:
        harvest_citations.main(["--help"])
    assert e.value.code == 0
    out = capsys.readouterr().out
    assert str(Path.home()) not in out
    assert "<db_dir>/citations" in out


def test_harvest_bad_db_dir_is_a_clean_error(tmp_path, monkeypatch, capsys):
    import harvest_citations
    monkeypatch.setattr(harvest_citations, "CONFIG_PATH", _registry(tmp_path, db_dir=""))
    src = tmp_path / "src"
    src.mkdir()
    with pytest.raises(SystemExit) as e:
        harvest_citations.main(["--source-dir", str(src)])
    assert e.value.code == 2
    assert "--out-dir" in capsys.readouterr().err


# ------------------------------------------------------------------ Pd11: extract_tables folders
def _project_registry(tmp_path, entry):
    root = tmp_path / "root"
    return {"root": str(root), "projects": {"research_tables": entry}}, root


def test_extract_tables_project_resolves_library_and_data_dir(tmp_path, monkeypatch, capsys):
    import extract_tables
    import lit_util
    cfg, root = _project_registry(tmp_path, {"tier": 1, "lib_dir": "papers", "data_dir": "build"})
    (root / "research_tables" / "papers").mkdir(parents=True)
    reg = tmp_path / "projects.json"
    reg.write_text(json.dumps(cfg), encoding="utf-8")
    monkeypatch.setattr(extract_tables, "CONFIG_PATH", reg)
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    monkeypatch.chdir(tmp_path)                       # the cwd-relative defaults must not be used
    extract_tables.main(["--project", "research_tables"])
    report = root / "research_tables" / "build" / "tables" / "_extraction_report.csv"
    assert report.is_file()
    assert not (tmp_path / "data").exists()
    assert str(root / "research_tables" / "papers") in capsys.readouterr().out


def test_extract_tables_project_without_data_dir_needs_out_dir(tmp_path, monkeypatch):
    import extract_tables
    import lit_util
    cfg, root = _project_registry(tmp_path, {"tier": 2, "lib_dir": "papers"})
    (root / "research_tables" / "papers").mkdir(parents=True)
    reg = tmp_path / "projects.json"
    reg.write_text(json.dumps(cfg), encoding="utf-8")
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    monkeypatch.setattr(extract_tables, "CONFIG_PATH", reg)
    monkeypatch.chdir(tmp_path)                       # a regression must not write into the checkout
    with pytest.raises(SystemExit) as e:
        extract_tables.main(["--project", "research_tables"])
    assert "data_dir" in str(e.value.code) and "--out-dir" in str(e.value.code)
    out = tmp_path / "explicit"
    extract_tables.main(["--project", "research_tables", "--out-dir", str(out)])
    assert (out / "_extraction_report.csv").is_file()


def test_extract_tables_unregistered_project_exits(tmp_path, monkeypatch):
    import extract_tables
    reg = tmp_path / "projects.json"
    reg.write_text(json.dumps({"projects": {}}), encoding="utf-8")
    monkeypatch.setattr(extract_tables, "CONFIG_PATH", reg)
    with pytest.raises(SystemExit) as e:
        extract_tables.main(["--project", "research_missing"])
    assert "not registered" in str(e.value.code)


# ------------------------------------------------------------------ P03/Pd13: legacy data_dir fallbacks
def test_legacy_data_dir_fallbacks_still_found_and_library_file_wins(tmp_path):
    import index_portfolio as ip
    lib, data = tmp_path / "lib", tmp_path / "data"
    (data / "discovered").mkdir(parents=True)
    (data / "references").mkdir(parents=True)
    lib.mkdir()
    legacy_fwd = data / "discovered" / "s2_forward_citations_v2.csv"
    legacy_rev = data / "references" / "parsed_references.csv"
    legacy_fwd.write_text("x\n", encoding="utf-8")
    legacy_rev.write_text("x\n", encoding="utf-8")
    assert ip.find_forward_csv(lib, data) == legacy_fwd
    assert ip.find_reverse_csv(lib, data) == legacy_rev
    (lib / "_forward_citations.csv").write_text("x\n", encoding="utf-8")
    (lib / "_reverse_citations_parsed.csv").write_text("x\n", encoding="utf-8")
    assert ip.find_forward_csv(lib, data) == lib / "_forward_citations.csv"
    assert ip.find_reverse_csv(lib, data) == lib / "_reverse_citations_parsed.csv"
    assert ip.find_forward_csv(lib, None) == lib / "_forward_citations.csv"


def test_legacy_fallbacks_are_labelled_by_layout():
    src = (REPO / "index_portfolio.py").read_text(encoding="utf-8")
    assert src.count("legacy data_dir layout") >= 4


# ------------------------------------------------------------------ P09: no sync client assumed
def test_enrich_s2_dry_run_message_names_no_particular_sync_client(capsys):
    from litpipe import enrich_s2
    res = {"requested": 0, "skipped_fresh": 0, "skipped_elided": 0, "s2_records": 0, "not_in_s2": 0,
           "failed": 0, "mismatch": 0, "elided": 0, "oa_urls": 0, "would_fill": 0,
           "abstracts_before": 0, "status": "ok", "reasons": [], "aborted": None,
           "s2": {"key": "key: absent"}}
    enrich_s2._print_summary(res, False, None, Path("portfolio.duckdb"), 0, False, enrich_s2.MAX_AGE_DAYS, False)
    out = capsys.readouterr().out
    assert "sync client" in out
    assert "Google" not in out and "Drive" not in out


# ------------------------------------------------------------------ the public surface of this task
PUBLIC_DOCS = ["README.md", "ROADMAP.md", "projects.json.template", "lit_pull_queue.template.csv",
               ".github/workflows/ci.yml", "vendor/VENDORED.md"]
EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+")
OK_EMAIL = re.compile(r"(?i)@(?:your|users\.noreply\.github\.com)")
HOME = re.compile(r"(?i)(?:[A-Z]:[\\/]+Users[\\/]+|/home/)(?!<|you\b|name\b|runner\b)[A-Za-z0-9._-]+")
COURSE = re.compile(r"(?<![A-Za-z0-9])[A-Z]{3,4}\d{4}(?![0-9])")
STANDARD = re.compile(r"^(?:RFC|ISO|UTF|ISSN|ISBN)\d+$")


@pytest.mark.parametrize("rel", PUBLIC_DOCS)
def test_public_docs_carry_no_personal_paths_addresses_or_course_codes(rel):
    text = (REPO / rel).read_text(encoding="utf-8")
    assert [m for m in EMAIL.findall(text) if not OK_EMAIL.search(m)] == []
    assert HOME.findall(text) == []
    assert [c for c in COURSE.findall(text) if not STANDARD.match(c)] == []
    assert "](../" not in text, "a link that leaves the repository"


@pytest.mark.parametrize("rel", ["README.md", "vendor/VENDORED.md"])   # rewritten in W5; ROADMAP only scrubbed
def test_markdown_uses_no_dash_or_arrow_characters(rel):
    text = (REPO / rel).read_text(encoding="utf-8")
    bad = sorted({c for c in text if c in "–—→←↔⇒"})
    assert bad == []


# ------------------------------------------------------------------ .gitignore keeps private files out
PRIVATE = ["projects.json", "projects.json.bak-20260101", "notes/x.md", "reference/x.md", "CURRENT_STATE.md",
           "LOOSE_ENDS.md", ".claude/session.txt", "litpipe_state.sqlite", "portfolio.duckdb",
           "s2_cache.duckdb", "lit_pull_queue.csv", "lit_pull_queue.2026-01-01.report.csv",
           "lit_pull_queue.oa_blocked.md", "lit_pull_queue.lock", "sweep.py.bak-20260101"]
PUBLIC = ["lit_pull_queue.template.csv", "projects.json.template", "README.md",
          "tests/fixtures/W1-D2/chain_real/lit_pull_queue.2026-09-22.pmc.csv"]


def _ignored(paths):
    import shutil
    import subprocess
    if not shutil.which("git") or not (REPO / ".git").exists():
        pytest.skip("not a git checkout")
    r = subprocess.run(["git", "check-ignore", "--no-index", *paths], cwd=REPO, capture_output=True, text=True)
    return set(r.stdout.split())


def test_gitignore_keeps_private_files_out():
    assert _ignored(PRIVATE) == set(PRIVATE)


def test_gitignore_keeps_the_public_files_and_tracked_fixtures_in():
    assert _ignored(PUBLIC) == set()
