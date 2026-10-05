"""Regression tests for the 2026-06-25-audit sibling-sweep fixes (post Batch-2 verification).

The adversarial sibling sweep that ran after the initial Batch-2 fixes landed found
real instances of the same bug-classes that the first pass missed. These lock them:
  - lit_util.coerce_int: shared int()-on-messy-CSV guard (T5d + the build_priority year-sort
    crash on a non-numeric residual-CSV year).
  - migrate_closed_to_md.read_report_chain: an ALREADY_EXISTS/skipped paper (PDF already on
    disk) must NOT be mis-routed to the manual-pull/ILL queue as closed-access (T5b's parallel
    oracle -- the exact harm T3 guards).
  - pipeline_check Stage 4b: the .ris-coverage check is a real per-project predicate now, not a
    hardcoded True no-op (the audit's prescribed 'delete all .ris -> assert exit 1' test).
"""
import csv
import importlib
import os
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

import lit_util

REPO = Path(__file__).resolve().parent.parent


# ---------- lit_util.coerce_int ----------

@pytest.mark.parametrize("raw,expected", [
    ("1,234", 1234), ("  12 ", 12), ("2020.0", 2020), ("42", 42),
    ("in press", 0), ("2020a", 0), ("n/a", 0), ("", 0), (None, 0),
])
def test_coerce_int(raw, expected):
    assert lit_util.coerce_int(raw) == expected


def test_coerce_int_custom_default():
    assert lit_util.coerce_int("not-a-number", default=-1) == -1


def test_coerce_int_year_sort_key_no_crash():
    """The exact build_priority sort-key shape that used to ValueError on a non-numeric year."""
    rows = [{"score": 5, "year": "in press"}, {"score": 5, "year": "2020"}]
    rows.sort(key=lambda r: (-r["score"], -lit_util.coerce_int(r["year"])))  # must not raise
    assert [r["year"] for r in rows] == ["2020", "in press"]  # numeric year sorts ahead


# ---------- migrate_closed_to_md: ALREADY_EXISTS not mis-routed to ILL ----------

def _write_csv(path, fieldnames, rows):
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def _seed_unpaywall(root, date, doi):
    _write_csv(root / f"lit_pull_queue.{date}.unpaywall.csv",
               ["doi", "downloaded", "oa_status", "winning_host", "error", "title", "year"],
               [{"doi": doi, "downloaded": "False", "oa_status": "CLOSED",
                 "winning_host": "", "error": "", "title": "T", "year": "2020"}])


def test_migrate_pmc_already_exists_not_closed_access(tmp_path):
    migrate = importlib.import_module("migrate_closed_to_md")
    date = "2026-06-29"
    doi = "10.1234/already-pmc"
    _seed_unpaywall(tmp_path, date, doi)
    _write_csv(tmp_path / f"lit_pull_queue.{date}.pmc.csv",
               ["doi", "downloaded", "skipped", "winning_source", "pmcid"],
               [{"doi": doi, "downloaded": "False", "skipped": "True",
                 "winning_source": "ALREADY_EXISTS", "pmcid": ""}])
    out = migrate.read_report_chain(tmp_path, date)
    assert doi not in [r["doi"] for r in out]  # resolved (on disk), NOT closed-access


def test_migrate_preprint_already_exists_not_closed_access(tmp_path):
    migrate = importlib.import_module("migrate_closed_to_md")
    date = "2026-06-29"
    doi = "10.1234/already-ppr"
    _seed_unpaywall(tmp_path, date, doi)
    # PMC genuinely fails (no pmcid), preprint reports ALREADY_EXISTS
    _write_csv(tmp_path / f"lit_pull_queue.{date}.pmc.csv",
               ["doi", "downloaded", "skipped", "winning_source", "pmcid"],
               [{"doi": doi, "downloaded": "False", "skipped": "", "winning_source": "", "pmcid": ""}])
    _write_csv(tmp_path / f"lit_pull_queue.{date}.preprint.csv",
               ["doi", "downloaded", "skipped", "status"],
               [{"doi": doi, "downloaded": "False", "skipped": "True", "status": "ALREADY_EXISTS"}])
    out = migrate.read_report_chain(tmp_path, date)
    assert doi not in [r["doi"] for r in out]


def test_migrate_genuine_failure_still_reported(tmp_path):
    """Control: a DOI that genuinely failed every stage MUST still be reported as closed-access."""
    migrate = importlib.import_module("migrate_closed_to_md")
    date = "2026-06-29"
    doi = "10.1234/genuinely-closed"
    _seed_unpaywall(tmp_path, date, doi)
    _write_csv(tmp_path / f"lit_pull_queue.{date}.pmc.csv",
               ["doi", "downloaded", "skipped", "winning_source", "pmcid"],
               [{"doi": doi, "downloaded": "False", "skipped": "", "winning_source": "", "pmcid": ""}])
    out = migrate.read_report_chain(tmp_path, date)
    assert doi in [r["doi"] for r in out]


# ---------- pipeline_check Stage 4b: real predicate (the audit's prescribed test) ----------

def _make_pdf(path):
    path.write_bytes(b"%PDF-1.4\n" + b"x" * 200)  # valid magic byte, >0 size


def test_pipeline_check_stage4b_fails_on_zero_ris(tmp_path):
    """2 PDFs, 0 .ris (0% < 90% default floor) -> Stage 4b FAILs and pipeline_check exits 1.
    Before the fix this was check(..., True) and could never fail."""
    lib = tmp_path / "lib"
    lib.mkdir()
    _make_pdf(lib / "2020_Smith_Heat.pdf")
    _make_pdf(lib / "2021_Jones_Cold.pdf")
    proc = subprocess.run(
        [sys.executable, str(REPO / "pipeline_check.py"),
         "--base-dir", str(tmp_path), "--lib-dir", "lib", "--tier", "2"],
        capture_output=True, text=True)
    assert proc.returncode == 1, proc.stdout
    assert "below 90%" in proc.stdout  # the FAIL-branch detail string (proves the predicate fired)


def test_pipeline_check_stage4b_passes_empty_lib(tmp_path):
    """Empty lib (0 PDFs) -> the (not pdfs) guard keeps Stage 4b green (no false-fail)."""
    lib = tmp_path / "lib"
    lib.mkdir()
    proc = subprocess.run(
        [sys.executable, str(REPO / "pipeline_check.py"),
         "--base-dir", str(tmp_path), "--lib-dir", "lib", "--tier", "2"],
        capture_output=True, text=True)
    assert "below" not in proc.stdout  # Stage 4b did not fire a coverage failure


# ---------- c12: DEFAULT_EMAIL single-source + atomic_write_csv (Stage 3 DRY) ----------

# Every fetcher module that binds a module-level EMAIL fallback; jats_to_text is excluded
# (its mailto is inline in _smoke_test, not a module constant).
_EMAIL_MODULES = [
    "audit_filenames",
    "enrich_recommendations", "fill_missing_dois",
    # left in W2a: pmc_fetch (W2-A1), backfill_fulltext, harvest_citations, recheck_pmc (W2-A2),
    # unpaywall_fetch_v2 (W2-B) and ris_emit (W2-E1); in W2b: backfill_ris, enrich_abstracts (W2-E2), forward_citations (W2-D2), preprint_fetch (W2-C).
    # They bind no EMAIL; litpipe.net injects identity (DEC-13; pinned below and by test_w2e1_net.py,
    # test_unpaywall_stage_has_no_email_constant). Modules move to _REWIRED as W2/W3 rewire them onto litpipe.net.
]
_REWIRED = ["backfill_ris", "enrich_abstracts", "forward_citations", "preprint_fetch", "import_downloads"]


@pytest.mark.parametrize("modname", _REWIRED)
def test_rewired_module_binds_no_email_constant(modname):
    """DEC-13: a module on litpipe.net keeps no EMAIL / UA of its own (no `mailto:None` to leak)."""
    m = importlib.import_module(modname)
    assert not hasattr(m, "EMAIL") and not hasattr(m, "UA")


@pytest.mark.parametrize("modname", _EMAIL_MODULES)
def test_email_is_single_sourced(modname):
    """Each fetcher's EMAIL is the env override or lit_util.DEFAULT_EMAIL -- never a
    divergent literal. The env-unset fallback routes through the one constant."""
    m = importlib.import_module(modname)
    assert m.EMAIL == os.environ.get("LITPIPE_EMAIL", lit_util.DEFAULT_EMAIL)


def test_default_email_has_no_code_default_and_no_literal():
    """DEC-13: no code default; the name stays importable (dispatch 0.6), and the retired
    maintainer address is hardcoded nowhere (the c12 sweep's 14 sites stay at 0)."""
    assert lit_util.DEFAULT_EMAIL is None
    retired = "JacobBowie@users.noreply.github.com"
    offenders = [py.name for py in REPO.glob("*.py") if retired in py.read_text(encoding="utf-8")]
    assert offenders == [], f"retired email literal hardcoded: {offenders}"


def test_atomic_write_csv_lf_and_roundtrip(tmp_path):
    """LF-only line endings (not the csv \\r\\n default), and a clean DictReader roundtrip
    including a field that needs quoting."""
    p = tmp_path / "r.csv"
    rows = [{"a": "1", "b": "x"}, {"a": "2", "b": "y,z"}]  # embedded comma -> quoted
    lit_util.atomic_write_csv(str(p), rows, fieldnames=["a", "b"])
    raw = p.read_bytes()
    assert b"\r\n" not in raw and raw.endswith(b"\n")
    assert list(csv.DictReader(p.open(newline=""))) == rows
    assert not list(tmp_path.glob("*.tmp"))  # tmp cleaned up


def test_atomic_write_csv_replaces_not_appends(tmp_path):
    """A second write fully replaces the first via os.replace -- never appends/interleaves."""
    p = tmp_path / "r.csv"
    lit_util.atomic_write_csv(str(p), [{"a": "1"}], fieldnames=["a"])
    lit_util.atomic_write_csv(str(p), [{"a": "2"}], fieldnames=["a"])
    assert p.read_text(encoding="utf-8") == "a\n2\n"


# ---------- c9: lit_util.connect_db (shared RC10 Drive-lock-tolerant open) ----------

def test_connect_db_opens_and_queries(tmp_path):
    con = lit_util.connect_db(str(tmp_path / "t.duckdb"))
    con.execute("CREATE TABLE t (x INTEGER)")
    con.execute("INSERT INTO t VALUES (1)")
    assert con.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 1
    con.close()


def test_connect_db_read_only_rejects_writes(tmp_path):
    db = str(tmp_path / "t.duckdb")
    con = lit_util.connect_db(db)
    con.execute("CREATE TABLE t (x INTEGER)"); con.execute("INSERT INTO t VALUES (7)")
    con.close()
    ro = lit_util.connect_db(db, read_only=True)
    assert ro.execute("SELECT x FROM t").fetchone()[0] == 7   # reads work
    with pytest.raises(duckdb.Error):
        ro.execute("INSERT INTO t VALUES (8)")                # writes rejected
    ro.close()


def test_connect_db_retries_then_succeeds(tmp_path, monkeypatch):
    """A transient failure is retried; a subsequent success returns the connection."""
    real = duckdb.connect
    calls = {"n": 0}
    def flaky(path, **kw):
        calls["n"] += 1
        if calls["n"] < 2:
            raise RuntimeError("locked (transient)")
        return real(path, **kw)
    monkeypatch.setattr(duckdb, "connect", flaky)
    con = lit_util.connect_db(str(tmp_path / "t.duckdb"), tries=3, delays=(0,))
    assert calls["n"] == 2                                     # failed once, then succeeded
    con.close()


def test_connect_db_on_fail_exit_raises_systemexit(tmp_path, monkeypatch):
    """on_fail='exit' aborts with SystemExit after exhausting `tries` (enrich_* CLI behavior)."""
    calls = {"n": 0}
    def always_fail(path, **kw):
        calls["n"] += 1; raise RuntimeError("locked")
    monkeypatch.setattr(duckdb, "connect", always_fail)
    with pytest.raises(SystemExit):
        lit_util.connect_db(str(tmp_path / "t.duckdb"), on_fail="exit", tries=2, delays=(0,))
    assert calls["n"] == 2                                     # both attempts made


def test_connect_db_on_fail_raise_reraises_last(tmp_path, monkeypatch):
    """on_fail='raise' (default) re-raises the LAST underlying error (not the first), NOT
    SystemExit (the index rebuild wants the real traceback). Distinct error per attempt so
    the test actually distinguishes last-vs-first."""
    errs = [RuntimeError("attempt-1"), RuntimeError("attempt-2")]
    seq = iter(errs)
    monkeypatch.setattr(duckdb, "connect", lambda path, **kw: (_ for _ in ()).throw(next(seq)))
    with pytest.raises(RuntimeError) as ei:
        lit_util.connect_db(str(tmp_path / "t.duckdb"), on_fail="raise", tries=2, delays=(0,))
    assert ei.value is errs[-1]                               # the LAST error, not the first


def test_connect_db_delay_envelope_and_clamp(tmp_path, monkeypatch):
    """Lock the load-bearing delays clamp AND both production retry envelopes (the c9
    behavior-preservation claim): enrich (tries=5, delays=(3,)) -> [3,3,3,3]=12s via the
    delays[min(attempt-1, len-1)] clamp; index (tries=3, delays=(2,4)) -> [2,4]=6s. A
    regression to delays[attempt-1] would IndexError enrich on attempt 2 -- this catches it."""
    waits = []
    monkeypatch.setattr(lit_util.time, "sleep", lambda s: waits.append(s))
    monkeypatch.setattr(duckdb, "connect",
                        lambda path, **kw: (_ for _ in ()).throw(RuntimeError("locked")))
    # enrich production config: a 1-element delays tuple must clamp to a constant 3s for all 4 sleeps
    with pytest.raises(SystemExit):
        lit_util.connect_db(str(tmp_path / "t.duckdb"), on_fail="exit", tries=5, delays=(3,))
    assert waits == [3, 3, 3, 3]                              # 4 inter-attempt sleeps, all clamped
    # index production config: 2s then 4s
    waits.clear()
    with pytest.raises(RuntimeError):
        lit_util.connect_db(str(tmp_path / "t.duckdb"), tries=3, delays=(2, 4))
    assert waits == [2, 4]


def test_connect_db_rejects_tries_below_one(tmp_path):
    """The shared helper fails fast on a computed tries<1 rather than `raise None` -> TypeError."""
    with pytest.raises(ValueError):
        lit_util.connect_db(str(tmp_path / "t.duckdb"), tries=0)


def test_unpaywall_stage_has_no_email_constant():
    import unpaywall_fetch_v2 as m   # DEC-13: identity comes from litpipe.net (W2-B)
    assert not hasattr(m, "EMAIL") and not hasattr(m, "API_UA")
