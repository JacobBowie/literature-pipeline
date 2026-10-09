"""W2-F: the instruments tell the truth (audit_portfolio.py and pipeline_check.py).

Every test runs on a temp projects root, a temp registry (its state_dir and db_dir under tmp_path)
and, where the index is involved, a temp DuckDB built here. No test opens the live index or the
real state file, and none may create the registry's state_dir.
"""
import csv
import datetime as dt
import hashlib
import json
import os
import time
from pathlib import Path

import duckdb
import pytest

import audit_portfolio as ap
import lit_util
import pipeline_check as pc
from litpipe import config as litconfig
from litpipe import state

PDF_BODY = b"%PDF-1.4\n" + b"x" * 12_000   # real magic, above the 10 KB tiny threshold
PROJECT = "research_alpha"
LIB_REL = "references/literature"


# ---------------------------------------------------------------- fixtures and builders
def _ris(title, au="Smith, A.", py="2020", doi="10.1234/x"):
    return f"TY  - JOUR\nTI  - {title}\nAU  - {au}\nPY  - {py}\nDO  - {doi}\nER  - \n"


def _write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _json(path, obj):
    return _write(path, json.dumps(obj, ensure_ascii=False))


def _pdf(path, body=PDF_BODY):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)
    return path


def _csv(path, fields, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    return path


def make_acceptance_lib(lib):
    """The acceptance library: three clean PDF holdings (one with a ligature sidecar, one with an
    entity .ris), one text-only holding with its .ris, one Unpaywall .identity.json FLAG and one
    PMC .fulltext.json FLAG."""
    clean = "Heat tolerance and thermoregulation in trained athletes. " * 30
    _pdf(lib / "2020_Smith_HeatTolerance.pdf")
    _write(lib / "2020_Smith_HeatTolerance.ris", _ris("Heat tolerance in athletes", doi="10.1234/a1"))
    _json(lib / "2020_Smith_HeatTolerance.fulltext.json",
          {"doi": "10.1234/a1", "text": clean, "extracted_from_pdf": True})
    _pdf(lib / "2021_Jones_Reflex.pdf")
    _write(lib / "2021_Jones_Reflex.ris", _ris("The reflex arc", au="Jones, B.", py="2021", doi="10.1234/a2"))
    _json(lib / "2021_Jones_Reflex.fulltext.json",
          {"doi": "10.1234/a2", "text": clean + " the baro\N{LATIN SMALL LIGATURE FL}ex arc", "extracted_from_pdf": True})
    _pdf(lib / "2022_Brown_Humidity.pdf")
    _write(lib / "2022_Brown_Humidity.ris",
           _ris("Heat &amp; humidity", au="Brown, C.", py="2022", doi="10.1234/a3"))
    _json(lib / "2022_Brown_Humidity.fulltext.json",
          {"doi": "10.1234/a3", "text": clean, "extracted_from_pdf": True})
    _json(lib / "2023_Green_JatsOnly.fulltext.json",
          {"doi": "10.1234/a4", "pmcid": "PMC1", "text": clean, "has_pdf": False,
           "extracted_from_pdf": False, "extractor": "s3_jats"})
    _write(lib / "2023_Green_JatsOnly.ris", _ris("Jats only paper", au="Green, D.", py="2023", doi="10.1234/a4"))
    _pdf(lib / "2024_White_Flagged.pdf")
    _json(lib / "2024_White_Flagged.identity.json",
          {"queue_doi": "10.1234/a5", "pdf": "2024_White_Flagged.pdf", "source": "unpaywall",
           "doc_kind": "VOR", "identity": "FLAG", "identity_score": 0.2, "identity_evidence": {}})
    _json(lib / "2024_White_Flagged.fulltext.json",
          {"doi": "", "text": clean, "extracted_from_pdf": True})
    _pdf(lib / "2025_Black_PmcFlag.pdf")
    _json(lib / "2025_Black_PmcFlag.fulltext.json",
          {"doi": "10.1234/a6", "pmcid": "PMC2", "text": "", "has_pdf": True, "extracted_from_pdf": False,
           "extractor": "identity_only", "identity": "FLAG", "identity_score": 0.1, "identity_evidence": {}})


class World:
    def __init__(self, tmp_path, monkeypatch):
        self.tmp = tmp_path
        self.root = tmp_path / "root"
        self.root.mkdir()
        self.state_dir = tmp_path / "state"
        self.db_dir = tmp_path / "db"
        self.cfg_path = tmp_path / "registry" / "projects.json"
        self.projects = {}
        monkeypatch.setattr(lit_util, "PROJECTS_ROOT", self.root)
        monkeypatch.setattr(ap, "CONFIG_PATH", self.cfg_path)
        monkeypatch.setattr(pc, "CONFIG_PATH", self.cfg_path)
        monkeypatch.setattr(litconfig, "CONFIG_PATH", self.cfg_path)
        monkeypatch.setattr(state, "DB_PATH", None)
        self.add(PROJECT, LIB_REL)

    def add(self, key, lib_dir, **extra):
        self.projects[key] = {"tier": 2, "lib_dir": lib_dir, "active": True, **extra}
        self.save()

    def save(self):
        _json(self.cfg_path, {"state_dir": str(self.state_dir), "db_dir": str(self.db_dir),
                              "projects": self.projects})

    def lib(self, key=PROJECT):
        return lit_util.lib_paths(key, self.projects[key])[1]

    def proj(self, key=PROJECT):
        return lit_util.project_root(key, self.projects[key])

    @property
    def db(self):
        return self.db_dir / "portfolio.duckdb"


@pytest.fixture
def world(tmp_path, monkeypatch):
    return World(tmp_path, monkeypatch)


def _tree(root):
    """{relative path: (size, mtime_ns)} of every file and directory under root."""
    out = {}
    for dirpath, dirs, files in os.walk(root):
        for n in dirs + files:
            p = Path(dirpath) / n
            st = p.stat()
            out[str(p.relative_to(root))] = (st.st_size if p.is_file() else -1, st.st_mtime_ns)
    return out


def _where(scan, name):
    """Every list in a scan that names `name` (the 'reported exactly once' check)."""
    hits = []
    for key, val in scan.items():
        if key.startswith("_") or key in ("pdf_names",):
            continue
        if key == "damage":
            for kf, kinds in val.items():
                for k, names in kinds.items():
                    hits += [f"damage.{kf}.{k}"] * names.count(name)
        elif key == "flags":
            hits += ["flags"] * sum(1 for f in val if f["file"] == name)
        elif isinstance(val, list):
            hits += [key] * sum(1 for v in val if v == name or (isinstance(v, (tuple, list)) and v and v[0] == name))
    return hits


# ---------------------------------------------------------------- the acceptance library
def test_acceptance_library_scan_reports_each_item_once(world):
    lib = world.lib()
    make_acceptance_lib(lib)
    s = ap.scan_library(lib)
    assert _where(s, "2021_Jones_Reflex.fulltext.json") == ["damage.sidecar.ligature"]
    assert _where(s, "2022_Brown_Humidity.ris") == ["damage.ris.entity"]
    assert _where(s, "2023_Green_JatsOnly.fulltext.json") == ["text_only"]
    assert _where(s, "2023_Green_JatsOnly.ris") == []          # belongs to the text-only holding
    assert s["text_only_with_ris"] == 1
    assert _where(s, "2024_White_Flagged.pdf") == ["flags"]
    assert _where(s, "2025_Black_PmcFlag.pdf") == ["flags"]
    assert [f["reason"] for f in s["flags"]] == ["identity=FLAG", "identity=FLAG"]
    assert s["pdf_holdings"] == 3                              # flags are not holdings
    for key in ("orphan_sidecars", "orphan_ris", "orphan_identity", "pdfs_no_ris", "pdfs_no_sidecar",
                "empty_sidecars", "bad_sidecars", "fake_pdfs", "tiny_pdfs"):
        assert s[key] == [], key
    assert sum(len(v) for kinds in s["damage"].values() for v in kinds.values()) == 2


def test_audit_portfolio_prints_each_acceptance_item_once_in_its_severity(world, capsys):
    make_acceptance_lib(world.lib())
    rc = ap.main(["--no-holdings"])
    out = capsys.readouterr().out
    assert rc == 0, out
    assert "  WARN sidecar text damage: ligature: 1\n     2021_Jones_Reflex.fulltext.json" in out
    assert "  WARN ris text damage: entity: 1\n     2022_Brown_Humidity.ris" in out
    assert "  INFO TEXT_ONLY holdings (text, no PDF; 1 with .ris): 1\n     2023_Green_JatsOnly.fulltext.json" in out
    assert "  WARN identity flags (review; not holdings): 2" in out
    assert out.count("2023_Green_JatsOnly") == 1
    assert out.count("2021_Jones_Reflex.fulltext.json") == 2      # project block + portfolio rollup
    assert "orphan" not in out.split("PORTFOLIO SUMMARY")[0]


def test_pipeline_check_prints_each_acceptance_item_once_in_its_severity(world, capsys):
    make_acceptance_lib(world.lib())
    rc = pc.main(["--project", PROJECT])
    out = capsys.readouterr().out
    assert rc == 0, out
    assert "  [WARN] sidecar text damage: ligature: 1\n      2021_Jones_Reflex.fulltext.json" in out
    assert "  [WARN] ris text damage: entity: 1\n      2022_Brown_Humidity.ris" in out
    assert "  [INFO] TEXT_ONLY holdings (text, no PDF; 1 with .ris): 1\n      2023_Green_JatsOnly.fulltext.json" in out
    assert "  [WARN] identity flags (review; not holdings): 2" in out
    assert "[OK] no orphan sidecars (no PDF)" in out
    assert "[OK] no orphan .ris (no PDF)" in out
    assert "[OK] 3/3 PDFs have .ris (100%, floor 90%)" in out   # the flagged PDFs are outside it
    assert "[OK] all 5 PDFs are real PDFs (%PDF magic)" in out
    assert out.count("2023_Green_JatsOnly") == 1


# ---------------------------------------------------------------- orphans, text-only, all PDFs
@pytest.mark.parametrize("sidecar,text_only", [
    ({"text": "jats body", "pmcid": "PMC9"}, True),                         # pre-W2a JATS sidecar
    ({"text": "jats body", "has_pdf": False}, True),                        # W2a text-only
    ({"text": "pdf text", "extracted_from_pdf": True}, False),              # its PDF is gone
    ({"text": "pdf text", "has_pdf": True}, False),                         # its PDF is gone
    ({"text": "", "has_pdf": False}, False),                                # no content
])
def test_text_only_predicate(sidecar, text_only):
    assert ap.is_text_only_sidecar(sidecar) is text_only


def test_text_only_holding_and_its_ris_are_not_orphan_fails(world, capsys):
    lib = world.lib()
    _pdf(lib / "2020_Smith_A.pdf")
    _write(lib / "2020_Smith_A.ris", _ris("A"))
    _json(lib / "2020_Smith_A.fulltext.json", {"text": "a", "extracted_from_pdf": True})
    _json(lib / "2019_Lake_Jats.fulltext.json", {"text": "jats body", "pmcid": "PMC9"})
    _write(lib / "2019_Lake_Jats.ris", _ris("Jats"))
    assert pc.main(["--project", PROJECT]) == 0
    out = capsys.readouterr().out
    assert "[OK] no orphan sidecars (no PDF)" in out and "[OK] no orphan .ris (no PDF)" in out


def test_sidecar_of_a_deleted_pdf_is_still_an_orphan_fail(world, capsys):
    lib = world.lib()
    _json(lib / "2020_Gone_Pdf.fulltext.json", {"text": "pdf text", "extracted_from_pdf": True})
    _write(lib / "2020_Gone_Pdf.ris", _ris("Gone"))
    assert pc.main(["--project", PROJECT]) == 1
    out = capsys.readouterr().out
    assert "[FAIL] no orphan sidecars (no PDF)" in out
    assert "[FAIL] no orphan .ris (no PDF)" in out


def test_every_pdf_is_checked_for_its_magic_not_the_first_three(world, capsys):
    lib = world.lib()
    for i in range(5):
        _pdf(lib / f"202{i}_Author{i}_Title.pdf")
        _write(lib / f"202{i}_Author{i}_Title.ris", _ris("T"))
    _pdf(lib / "2024_Author4_Title.pdf", b"<html>not a pdf</html>" + b"x" * 12_000)
    assert pc.main(["--project", PROJECT]) == 1
    out = capsys.readouterr().out
    assert "[FAIL] all 5 PDFs are real PDFs (%PDF magic)" in out and "2024_Author4_Title.pdf" in out
    assert ap.main(["--no-holdings"]) == 1                   # a fake PDF is a FAIL there too
    assert "FAIL fake PDFs (no %PDF magic): 1" in capsys.readouterr().out


def test_mismatch_folder_is_a_warn_with_its_count(world, capsys):
    lib = world.lib()
    _pdf(lib / "2020_Smith_A.pdf")
    _write(lib / "2020_Smith_A.ris", _ris("A"))
    _pdf(lib / "_mismatch" / "2019_Other_Paper.pdf")
    _write(lib / "_mismatch" / "2019_Other_Paper.ris", _ris("Other"))
    assert pc.main(["--project", PROJECT]) == 0
    assert "[WARN] _mismatch/ quarantine (1 PDFs; restoring is a separate step): 2" in capsys.readouterr().out
    assert ap.main(["--no-holdings"]) == 0
    assert "WARN _mismatch/ quarantine (1 PDFs): 2" in capsys.readouterr().out


def test_item_lists_cap_at_twenty_and_full_prints_all(world, capsys):
    lib = world.lib()
    for i in range(25):
        _write(lib / f"2020_Orphan_{i:02d}.ris", _ris(f"Orphan {i}"))
    pc.main(["--project", PROJECT])
    out = capsys.readouterr().out
    assert "2020_Orphan_19.ris" in out and "2020_Orphan_20.ris" not in out
    assert "... +5 more (--full prints all)" in out
    pc.main(["--project", PROJECT, "--full"])
    assert "2020_Orphan_24.ris" in capsys.readouterr().out


# ---------------------------------------------------------------- missing library, modes, exit codes
def test_missing_library_exits_non_zero_in_both_tools(world, capsys):
    make_acceptance_lib(world.lib())
    world.add("teaching_beta", "lit")              # registered, active, never created
    assert ap.main(["--no-holdings"]) == 1
    out = capsys.readouterr().out
    assert "FAIL registered but missing: 1" in out and "teaching_beta" in out
    assert pc.main(["--project", "teaching_beta"]) == 1
    assert "[FAIL] library exists" in capsys.readouterr().out
    assert pc.main(["--all"]) == 1
    out = capsys.readouterr().out
    assert "[FAIL] every active registered library exists — registered but missing: 1" in out


def test_inactive_missing_library_is_not_a_fail(world, capsys):
    make_acceptance_lib(world.lib())
    world.add("teaching_beta", "lit", active=False)
    assert ap.main(["--no-holdings"]) == 0
    assert pc.main(["--all"]) == 0
    assert "inactive (skipped): 1" in capsys.readouterr().out


def test_pipeline_check_requires_a_mode(world, capsys):
    assert pc.main([]) == 2
    assert "pass --project NAME, --all, or --base-dir" in capsys.readouterr().err


def test_unknown_project_is_exit_two_not_an_exception(world, capsys):
    assert pc.run(project="research_nonexistent")["exit_code"] == 2
    assert "not in projects.json" in capsys.readouterr().err
    assert ap.main(["--project", "research_nonexistent"]) == 2


def test_pipeline_check_all_prints_one_table_and_one_exit_code(world, capsys):
    make_acceptance_lib(world.lib())
    world.add("research_gamma", "lit")
    lib2 = world.lib("research_gamma")
    _pdf(lib2 / "2020_Smith_A.pdf")                 # 0 % .ris coverage: a FAIL
    rc = pc.main(["--all"])
    out = capsys.readouterr().out
    assert rc == 1
    assert "SUMMARY (2 projects)" in out
    table = out.split("SUMMARY (2 projects)")[1]
    assert f"{PROJECT:<44}    0    0" in table
    assert f"{'research_gamma':<44}    1    1" in table
    assert "research_gamma: 0/1 PDFs have .ris (0%, floor 90%)" in out


def test_run_returns_a_dict_and_main_returns_its_exit_code(world, capsys):
    make_acceptance_lib(world.lib())
    r = ap.run(holdings=False)
    assert r["exit_code"] == 0 and r["projects"][0]["audit"]["pdf_holdings"] == 3
    assert "_records" not in r["projects"][0]["audit"]
    r = pc.run(project=PROJECT)
    assert r["exit_code"] == 0 and r["projects"][0]["counts"]["text_only"] == 1


# ---------------------------------------------------------------- nothing written but --json
def test_neither_tool_writes_anything_but_json_nor_creates_state(world, tmp_path, capsys):
    make_acceptance_lib(world.lib())
    _db_with_rows(world, refreshed="2020-01-01 00:00:00")
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    before = _tree(world.root), _tree(world.db_dir), _tree(world.cfg_path.parent)
    digest = hashlib.sha256(world.db.read_bytes()).hexdigest()
    assert ap.main(["--json", str(out_dir / "audit.json")]) == 0      # holdings map included
    assert pc.main(["--project", PROJECT, "--json", str(out_dir / "check.json")]) == 0
    assert pc.main(["--all", "--json", str(out_dir / "all.json")]) == 0
    after = _tree(world.root), _tree(world.db_dir), _tree(world.cfg_path.parent)
    assert after == before
    assert hashlib.sha256(world.db.read_bytes()).hexdigest() == digest
    assert sorted(p.name for p in out_dir.iterdir()) == ["all.json", "audit.json", "check.json"]
    assert not world.state_dir.exists()
    audit = json.loads((out_dir / "audit.json").read_text(encoding="utf-8"))
    assert audit["live_runs"]["exists"] is False
    assert audit["holdings"]["dois"] == 4 and audit["holdings"]["dois_text_only"] == 1
    check = json.loads((out_dir / "check.json").read_text(encoding="utf-8"))
    assert check["exit_code"] == 0 and check["projects"][0]["index"]["stale"]


# ---------------------------------------------------------------- index freshness and reconcile
def _db_with_rows(world, refreshed, pdfs=None, extra_rows=(), index_runs=None):
    """A temp index DB with the columns the instruments read (never the live one)."""
    world.db_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(world.db))
    con.execute("CREATE TABLE paper_locations (doi VARCHAR, project VARCHAR, lib_path VARCHAR, "
                "pdf_filename VARCHAR, has_pdf BOOLEAN DEFAULT FALSE, has_sidecar BOOLEAN, "
                "has_ris BOOLEAN, sidecar_text_len INTEGER, refreshed_at TIMESTAMP, "
                "PRIMARY KEY (doi, project))")
    con.execute("CREATE TABLE papers_no_doi (pdf_filename VARCHAR, project VARCHAR, lib_path VARCHAR, "
                "reason VARCHAR, PRIMARY KEY (pdf_filename, project))")
    names = pdfs if pdfs is not None else sorted(p.name for p in world.lib().glob("*.pdf"))
    for i, n in enumerate(list(names) + list(extra_rows)):
        con.execute("INSERT INTO paper_locations VALUES (?, ?, ?, ?, TRUE, TRUE, TRUE, 10, ?)",
                    [f"10.1234/db{i}", PROJECT, str(world.lib()), n, refreshed])
    if index_runs is not None:
        con.execute("CREATE TABLE index_runs (project VARCHAR, finished_at TIMESTAMP, n_files INTEGER)")
        for row in index_runs:
            con.execute("INSERT INTO index_runs VALUES (?, ?, ?)", list(row))
    con.close()


def _now_plus(hours):
    return (dt.datetime.now() + dt.timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")


def test_an_old_index_stamp_warns_in_both_tools(world, capsys):
    make_acceptance_lib(world.lib())
    _db_with_rows(world, refreshed="2020-01-01 00:00:00")
    assert pc.main(["--project", PROJECT]) == 0
    out = capsys.readouterr().out
    assert "[WARN] index stale: 1" in out and "is newer than the index stamp 2020-01-01 00:00 (refreshed_at)" in out
    assert ap.main(["--no-holdings"]) == 0
    out = capsys.readouterr().out
    assert "WARN index stale: newest library file" in out
    assert "WARN stale or miscounted index entries (max(refreshed_at);" in out


def test_a_current_index_does_not_warn(world, capsys):
    make_acceptance_lib(world.lib())
    _db_with_rows(world, refreshed=_now_plus(1))
    assert pc.main(["--project", PROJECT]) == 0
    out = capsys.readouterr().out
    assert "[OK] index current (5 rows" in out and "index stale" not in out


def test_db_versus_disk_reconcile_counts(world, capsys):
    make_acceptance_lib(world.lib())
    on_disk = sorted(p.name for p in world.lib().glob("*.pdf"))
    _db_with_rows(world, refreshed=_now_plus(1), pdfs=on_disk[1:], extra_rows=["2019_Gone_Paper.pdf"])
    r = pc.run(project=PROJECT)
    idx = r["projects"][0]["index"]
    assert idx["not_indexed"] == [on_disk[0]] and idx["stale_rows"] == ["2019_Gone_Paper.pdf"]
    out = capsys.readouterr().out
    assert "[WARN] PDFs on disk not in the index: 1" in out
    assert "[WARN] index rows with no file on disk: 1" in out
    ap.main(["--no-holdings"])
    assert f"{PROJECT}: 1, 1" in capsys.readouterr().out


def test_index_runs_table_is_preferred_when_present(world, capsys):
    make_acceptance_lib(world.lib())
    _db_with_rows(world, refreshed=_now_plus(1),
                  index_runs=[(PROJECT, "2020-01-01 00:00:00", 6), ("other", _now_plus(1), 1)])
    r = pc.run(project=PROJECT)
    idx = r["projects"][0]["index"]
    assert idx["source"] == "index_runs" and idx["stale"] and not idx["count_differs"]  # 5 PDFs + 1 text-only
    capsys.readouterr()


def test_index_is_opened_read_only(world, monkeypatch, capsys):
    make_acceptance_lib(world.lib())
    _db_with_rows(world, refreshed=_now_plus(1))
    seen = []
    real = duckdb.connect

    def spy(path, *a, **k):
        seen.append(k.get("read_only"))
        return real(path, *a, **k)
    monkeypatch.setattr(duckdb, "connect", spy)
    pc.main(["--project", PROJECT])
    ap.main(["--no-holdings"])
    capsys.readouterr()
    assert seen == [True, True]


def test_a_missing_index_is_info_and_never_created(world, capsys):
    make_acceptance_lib(world.lib())
    assert pc.main(["--project", PROJECT]) == 0
    assert "index DB not found" in capsys.readouterr().out
    assert not world.db_dir.exists()


# ---------------------------------------------------------------- live runs
def test_no_state_file_is_info_and_the_state_dir_is_not_created(world, capsys):
    make_acceptance_lib(world.lib())
    pc.main(["--project", PROJECT])
    assert f"live runs: no state file ({world.state_dir / 'litpipe_state.sqlite'})" in capsys.readouterr().out
    assert not world.state_dir.exists()


def test_a_live_run_older_than_six_hours_is_named(world, monkeypatch, tmp_path, capsys):
    make_acceptance_lib(world.lib())
    monkeypatch.setattr(state, "DB_PATH", tmp_path / "live_state.sqlite")
    monkeypatch.setattr(state, "_current_run", None)
    real_time = state._time
    monkeypatch.setattr(state, "_time", lambda: real_time() - 7 * 3600)
    run_id = state.register_run("sweep")
    monkeypatch.setattr(state, "_time", real_time)
    assert state.heartbeat(run_id)
    assert pc.main(["--project", PROJECT]) == 0
    out = capsys.readouterr().out
    assert "[WARN] live runs older than 6 h: 1" in out and run_id in out
    r = ap.run(holdings=False)
    assert [x["run_id"] for x in r["live_runs"]["stale"]] == [run_id]
    capsys.readouterr()


# ---------------------------------------------------------------- queue history (sweep's classes)
REPORT_HEAD = ["section", "name", "count", "detail"]
RESIDUAL_FIELDS = ["doi", "title", "authors", "year", "destination", "notes", "residual_class",
                   "reason", "stages", "held_at", "skipped_sources", "attempts", "run_id"]
UNPW_LEGACY = ["rank", "doi", "year", "cites", "filename", "title", "oa_status", "n_locations",
               "downloaded", "winning_host", "winning_url", "attempts", "error"]
UNPW_TYPED = UNPW_LEGACY + ["first_status", "route", "outcome", "detail", "ra", "identity", "doc_kind"]
PMC_LEGACY = ["doi", "filename", "pmcid", "downloaded", "skipped", "winning_source", "attempts",
              "error", "sidecar", "sidecar_status"]
PPR_LEGACY = ["doi", "title", "year", "preprint_filename", "found", "source", "match_id", "similarity",
              "downloaded", "skipped", "status"]


def _run_report(path, run_id, lib, classes, downloaded):
    rows = [["run", "run_id", "", run_id], ["run", "queue", "", "lit_pull_queue.csv"],
            ["run", "destination", "", str(lib)], ["total", "downloaded", str(downloaded), ""]]
    rows += [["class", c, str(n), ""] for c, n in classes.items()]
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(REPORT_HEAD)
        w.writerows(rows)


def _residual(path, rows):
    _csv(path, RESIDUAL_FIELDS, [{"doi": d, "title": f"Title {d}", "residual_class": c, "reason": why}
                                 for d, c, why in rows])


def _legacy_report(path, n):
    _csv(path, ["stage", "downloaded"], [{"stage": "unpaywall_v2", "downloaded": n},
                                          {"stage": "pmc_fetch", "downloaded": 0},
                                          {"stage": "total", "downloaded": n}])


def test_run_scoped_report_counts_come_from_sweep_and_a_blank_class_is_unknown(world):
    proj = world.proj()
    _run_report(proj / "lit_pull_queue.2026-10-02.report.csv", "2026-10-02", world.lib(),
                {"fetched": 2, "HELD_ELSEWHERE": 1, "OA_BLOCKED": 1, "TERMINAL_CLOSED": 1, "TEXT_ONLY": 0}, 2)
    _residual(proj / "lit_pull_queue.2026-10-02.residual.csv",
              [("10.1/h", "HELD_ELSEWHERE", "held in another library"),
               ("10.1/o", "OA_BLOCKED", "refused on a download host"),
               ("10.1/t", "TERMINAL_CLOSED", "closed"), ("10.1/b", "", "")])
    q = ap.audit_queue(proj, entry=world.projects[PROJECT], name=PROJECT, projects=world.projects,
                       lib=world.lib())
    s = q["latest_runs"][0]
    assert s["format"] == "run" and s["class_source"] == "report"
    assert s["classes"]["fetched"] == 2 and s["classes"]["OA_BLOCKED"] == 1
    assert [i["class"] for i in s["items"]] == ["HELD_ELSEWHERE", "OA_BLOCKED", "TERMINAL_CLOSED", "UNKNOWN"]
    assert s["unknown"] == 1 and s["disagreement"] == {"UNKNOWN": {"report": 0, "residual": 1}}
    assert [f["doi"] for f in q["failures"]] == ["10.1/b"]           # blank is never success
    assert q["runs"] == 1 and q["reports"] == 1


def test_legacy_dated_report_counts_from_its_residual_with_stage_detail(world):
    proj = world.proj()
    _legacy_report(proj / "lit_pull_queue.2026-09-14.report.csv", 1)
    _csv(proj / "lit_pull_queue.2026-09-14.residual.csv",
         ["doi", "title", "authors", "year", "destination", "notes", "citation_count"],
         [{"doi": "10.1/x", "title": "X"}, {"doi": "10.1/y", "title": "Y"}])
    _csv(proj / "lit_pull_queue.2026-09-14.unpaywall.csv", UNPW_LEGACY,
         [{"doi": "10.1/x", "downloaded": "False", "oa_status": "OA", "error": "HTML"},
          {"doi": "10.1/y", "downloaded": "False", "oa_status": "CLOSED", "error": ""},
          {"doi": "10.1/z", "downloaded": "True", "oa_status": "OA"}])
    _csv(proj / "lit_pull_queue.2026-09-14.pmc.csv", PMC_LEGACY,
         [{"doi": "10.1/x", "downloaded": "False", "error": "NO_PMCID"},
          {"doi": "10.1/y", "downloaded": "False", "error": "NO_PMCID"}])
    q = ap.audit_queue(proj, entry=world.projects[PROJECT], name=PROJECT, projects=world.projects)
    s = q["latest_runs"][0]
    assert s["format"] == "legacy" and s["class_source"] == "residual" and s["downloaded"] == 1
    assert s["classes"] == {"UNKNOWN": 2} and s["residual_has_class"] is False
    assert s["items"][0]["detail"] == "unpaywall=REFUSED; pmc=NO_MATCH"
    assert s["items"][1]["detail"] == "unpaywall=NOT_AVAILABLE; pmc=NO_MATCH"


def test_legacy_run_without_residual_falls_back_to_stage_reports(world):
    proj = world.proj()
    _legacy_report(proj / "lit_pull_queue.2026-08-01.report.csv", 1)
    _csv(proj / "lit_pull_queue.2026-08-01.unpaywall.csv", UNPW_TYPED, [
        {"doi": "10.1/ok", "downloaded": "True", "outcome": "OK"},
        {"doi": "10.1/typed", "downloaded": "False", "error": "HTTP_500", "outcome": "EMBARGOED"},
        {"doi": "10.1/legacy", "downloaded": "False", "error": "HTTP_403"},
        {"doi": "10.1/blank", "downloaded": "False", "error": "", "oa_status": ""},
        {"doi": "10.1/flag", "downloaded": "False", "error": "DOI_MISMATCH:pdf_doi=none", "outcome": "ERROR",
         "identity": "FLAG"}])
    _csv(proj / "lit_pull_queue.2026-08-01.pmc.csv", PMC_LEGACY + ["outcome", "identity"], [
        {"doi": "10.1/flag2", "downloaded": "False", "outcome": "OK", "identity": "FLAG"}])
    q = ap.audit_queue(proj, entry=world.projects[PROJECT], name=PROJECT, projects=world.projects)
    s = q["latest_runs"][0]
    assert s["class_source"] == "stages"
    assert s["classes"] == {"fetched": 1, "EMBARGOED": 1, "REFUSED": 1, "UNKNOWN": 1, "IDENTITY_FLAG": 2}
    assert s["unknown"] == 1
    assert {f["doi"] for f in q["failures"]} == {"10.1/blank"}


def test_run_ids_tags_and_latest_per_queue(world):
    proj, lib = world.proj(), world.lib()
    _run_report(proj / "lit_pull_queue.2026-10-02.report.csv", "2026-10-02", lib, {"fetched": 1}, 1)
    _run_report(proj / "lit_pull_queue.2026-10-02.2.report.csv", "2026-10-02.2", lib, {"fetched": 7}, 7)
    _run_report(proj / "lit_pull_queue.batch_a.2026-10-01.report.csv", "2026-10-01", lib,
                {"fetched": 3}, 3)
    _write(proj / "lit_pull_queue.2026-10-02.processed.csv", "doi\n")
    _write(proj / "lit_pull_queue.2026-09-25.processed.10.csv", "doi\n")        # legacy same-day suffix
    _write(proj / "lit_pull_queue.snapshot.b02.report.csv", "stage,downloaded\n")  # not a sweep name
    q = ap.audit_queue(proj, entry=world.projects[PROJECT], name=PROJECT, projects=world.projects, lib=lib)
    assert q["runs"] == 4 and q["reports"] == 3
    by_tag = {s["tag"]: s for s in q["latest_runs"]}
    assert by_tag[""]["run_id"] == "2026-10-02.2" and by_tag[""]["classes"]["fetched"] == 7
    assert by_tag["batch_a"]["classes"]["fetched"] == 3
    assert q["latest"] == "lit_pull_queue.2026-10-02.2.report.csv"


def test_subproject_reports_live_in_the_subproject_dir(world, capsys):
    """T2 step 1: a subproject's queue history comes from its own dir, not the parent's."""
    world.add("teaching_course/unit_a", "unit_a/literature", parent="teaching_course")
    make_acceptance_lib(world.lib("teaching_course/unit_a"))
    sub = world.proj("teaching_course/unit_a")
    assert sub == world.root / "teaching_course" / "unit_a"
    _legacy_report(sub / "lit_pull_queue.2026-10-01.report.csv", 31)
    ap.main(["--project", "unit_a", "--no-holdings"])
    out = capsys.readouterr().out
    assert "queue history: 1 run(s), 1 report(s)" in out and "latest=lit_pull_queue.2026-10-01.report.csv" in out


def _archive(world, sub_lib, sibling_lib):
    """An archive folder in a consumer's own layout (any layout: the audit names none): two runs
    of this project (one legacy, one naming its library), a sibling's run, and an unnamed run."""
    arch = world.tmp / "any archive é" / "2026-09-30"
    _legacy_report(arch / "unit_a" / "lit_pull_queue.2026-09-04.report.csv", 2)        # names no library
    _run_report(arch / "lit_pull_queue.2026-09-29.report.csv", "2026-09-29", sub_lib, {"fetched": 6}, 6)
    _run_report(arch / "lit_pull_queue.2026-09-28.report.csv", "2026-09-28", sibling_lib, {"fetched": 8}, 8)
    return arch


def test_artifact_dir_and_report_dir_folders_are_searched(world):
    """W5-C3 (P08): no consumer archive layout in the code. Archived reports are read where
    --report-dir names them; a run there counts when its report names this library, and with
    --project (claim_unnamed) a run that names none counts too."""
    world.add("teaching_course/unit_a", "unit_a/literature", parent="teaching_course")
    world.add("teaching_course/unit_b", "unit_b/literature", parent="teaching_course")
    key = "teaching_course/unit_a"
    lib = world.lib(key)
    lib.mkdir(parents=True)
    sub = world.proj(key)
    _run_report(sub / "sweep_out" / "lit_pull_queue.2026-10-03.report.csv", "2026-10-03", lib, {"fetched": 4}, 4)
    arch = _archive(world, lib, world.lib("teaching_course/unit_b"))
    q = ap.audit_queue(sub, entry=world.projects[key], name=key, projects=world.projects,
                       artifact_dir="sweep_out", lib=lib, extra_dirs=[str(arch.parent)])
    assert sorted(r["run_id"] for r in q["run_list"]) == ["2026-09-29", "2026-10-03"]
    assert [u["run_id"] for u in q["unattributed"]] == ["2026-09-04"]
    assert q["latest"] == "lit_pull_queue.2026-10-03.report.csv"
    qp = ap.audit_queue(sub, entry=world.projects[key], name=key, projects=world.projects, lib=lib,
                        extra_dirs=[str(arch.parent)], claim_unnamed=True)
    assert sorted(r["run_id"] for r in qp["run_list"]) == ["2026-09-04", "2026-09-29"]
    assert qp["unattributed"] == []
    q2 = ap.audit_queue(sub, entry=world.projects[key], name=key, projects=world.projects, lib=lib)
    assert q2["run_list"] == []                 # neither the artifact dir nor the archive by default


def test_an_archive_layout_on_disk_is_not_read_without_report_dir(world):
    """The layout one consumer used (`_archive/*/lit_sweep_exhaust/`) is no longer built in."""
    world.add("research_delta", "lit")
    ex = world.root / "research_delta" / "_archive" / "2026-09-01" / "lit_sweep_exhaust"
    _legacy_report(ex / "lit_pull_queue.2026-08-01.report.csv", 1)
    q = ap.audit_queue(world.proj("research_delta"), entry=world.projects["research_delta"],
                       name="research_delta", projects=world.projects)
    assert q["run_list"] == []
    src = Path(ap.__file__).read_text(encoding="utf-8")
    assert "lit_sweep_exhaust" not in src and "_archive/*" not in src


def test_report_dir_on_the_cli_with_project(world, capsys):
    world.add("teaching_course/unit_a", "unit_a/literature", parent="teaching_course")
    world.add("teaching_course/unit_b", "unit_b/literature", parent="teaching_course")
    key = "teaching_course/unit_a"
    lib = world.lib(key)
    make_acceptance_lib(lib)
    arch = _archive(world, lib, world.lib("teaching_course/unit_b"))
    res = ap.run(project=key, report_dirs=[str(arch.parent)], holdings=False)
    q = res["projects"][0]["queue"]
    assert sorted(r["run_id"] for r in q["run_list"]) == ["2026-09-04", "2026-09-29"]
    ap.main(["--project", key, "--no-holdings", "--report-dir", str(arch.parent)])
    assert "queue history: 2 run(s)" in capsys.readouterr().out


@pytest.mark.parametrize("has_accessor", [True, False])
def test_the_registry_artifact_dir_is_read_through_litpipe_config(world, monkeypatch, has_accessor):
    """W5-C1's litpipe.config.artifact_dir(key, cfg) when it exists, else the project root."""
    world.add("research_alpha2", "lit")
    lib = world.lib("research_alpha2")
    lib.mkdir(parents=True)
    art = world.tmp / "artifacts"
    _run_report(art / "research_alpha2" / "lit_pull_queue.2026-10-04.report.csv", "2026-10-04", lib,
                {"fetched": 3}, 3)
    _run_report(world.proj("research_alpha2") / "lit_pull_queue.2026-10-01.report.csv", "2026-10-01", lib,
                {"fetched": 1}, 1)
    seen = []

    def artifact_dir(key, cfg=None):
        seen.append((key, sorted((cfg or {}).get("projects") or {})))
        return art / key
    if has_accessor:
        monkeypatch.setattr(litconfig, "artifact_dir", artifact_dir, raising=False)
    else:
        monkeypatch.delattr(litconfig, "artifact_dir", raising=False)
    q = ap.audit_queue(world.proj("research_alpha2"), entry=world.projects["research_alpha2"],
                       name="research_alpha2", projects=world.projects, lib=lib)
    ids = sorted(r["run_id"] for r in q["run_list"])
    if has_accessor:
        assert ids == ["2026-10-01", "2026-10-04"] and seen and seen[0][0] == "research_alpha2"
        assert "research_alpha2" in seen[0][1]          # the whole registry as {"projects": ...}
    else:
        assert ids == ["2026-10-01"]


def test_an_unusable_artifact_dir_is_reported_and_the_project_root_still_read(world, monkeypatch, capsys):
    world.add("research_alpha3", "lit")
    lib = world.lib("research_alpha3")
    lib.mkdir(parents=True)
    _run_report(world.proj("research_alpha3") / "lit_pull_queue.2026-10-01.report.csv", "2026-10-01", lib,
                {"fetched": 1}, 1)

    def bad(key, cfg=None):
        raise litconfig.ConfigError("artifact_dir must be a path string")
    monkeypatch.setattr(litconfig, "artifact_dir", bad, raising=False)
    q = ap.audit_queue(world.proj("research_alpha3"), entry=world.projects["research_alpha3"],
                       name="research_alpha3", projects=world.projects, lib=lib)
    assert [r["run_id"] for r in q["run_list"]] == ["2026-10-01"]
    assert "artifact_dir unusable" in capsys.readouterr().err


# ---------------------------------------------------------------- fetched titles against the .ris
def test_fetched_titles_unlike_their_ris_are_listed(world):
    proj, lib = world.proj(), world.lib()
    _pdf(lib / "2020_Smith_Heat.pdf")
    _write(lib / "2020_Smith_Heat.ris", _ris("Heat tolerance in trained athletes"))
    _pdf(lib / "2021_Jones_Wrong.pdf")
    _write(lib / "2021_Jones_Wrong.ris", _ris("Quantum chromodynamics of gluon plasma"))
    _csv(proj / "lit_pull_queue.2026-10-02.processed.csv", ["doi", "title"],
         [{"doi": "10.1/a", "title": "Heat Tolerance in Trained Athletes."},
          {"doi": "10.1/b", "title": "Sweat sodium losses during exercise in the heat"}])
    _csv(proj / "lit_pull_queue.2026-10-02.unpaywall.csv", UNPW_LEGACY, [
        {"doi": "10.1/a", "filename": "2020_Smith_Heat.pdf", "downloaded": "True", "title": "cut"},
        {"doi": "10.1/b", "filename": "2021_Jones_Wrong.pdf", "downloaded": "True", "title": "cut"},
        {"doi": "10.1/c", "filename": "2022_Gone_File.pdf", "downloaded": "True", "title": "Gone"}])
    q = ap.audit_queue(proj, entry=world.projects[PROJECT], name=PROJECT, projects=world.projects,
                       lib=lib, scan=ap.scan_library(lib))
    t = q["titles"]
    assert t["checked"] == 2 and [x["file"] for x in t["low"]] == ["2021_Jones_Wrong.pdf"]
    assert t["not_in_library"] == ["2022_Gone_File.pdf"]


# ---------------------------------------------------------------- filename regex (T2 step 2)
@pytest.mark.parametrize("fname,au,flagged", [
    ("2023_Chen_PunicaMultiTenantLoraServing_preprint.pdf", "Chen, Lequn", False),
    ("2023_Alizadeh_LlmFlashEfficientLargeLanguageModel.pdf", "Alizadeh, Keivan", False),
    ("2022_Periard_HeatAcclimation.pdf", "Périard, Julien", False),
    ("2023_Berkeley_MemgptTowardsLlmsOperatingSystems.pdf", "Packer, Charles", True),
])
def test_filename_lastname_capture_stops_at_the_first_underscore(world, fname, au, flagged):
    lib = world.lib()
    _pdf(lib / fname)
    _write(lib / (fname[:-4] + ".ris"), _ris("T", au=au, py=fname[:4]))
    deep = ap.deep_audit_lib(lib)
    assert bool(deep["fn_misalign"]) is flagged, deep["fn_misalign"]


@pytest.mark.parametrize("file_last,ris_au,match", [
    ("Périard", "Périard", True),           # non-ASCII filename: both sides folded alike
    ("Periard", "Périard", True),
    ("Keijzer", "de Keijzer", True),                  # particle
    ("Snyder", "Ploutz-Snyder", True),                # compound
    ("GuimaraesFerreira", "Guimarães-Ferreira", True),
    ("Fox", "L. Fox", True),                          # AU written "First Last" (no comma)
    ("StPierre", "St. Pierre", True),
    ("Ponce", "Cebri\N{LATIN SMALL LETTER A WITH ACUTE}n\N{HYPHEN}Ponce", True),   # U+2010 splits the parts
    ("Unknown", "Park", False),                       # real disagreements stay flagged
    ("Microsoft", "Wang", False),
    ("AG", "Thiago A.G.", False),                     # a 2-letter part is not a surname match
    ("Brer", "Bröer", False),                    # a letter dropped by an old ascii-ignore name
])
def test_lastname_comparison(file_last, ris_au, match):
    assert ap.lastname_matches(file_last, ris_au) is match


def test_non_ascii_filename_is_not_a_mismatch(world):
    lib = world.lib()
    _pdf(lib / "2021_Périard_ExerciseUnderHeatStress.pdf")
    _write(lib / "2021_Périard_ExerciseUnderHeatStress.ris", _ris("T", au="Périard, Julien D.", py="2021"))
    assert ap.deep_audit_lib(lib)["fn_misalign"] == []


def test_text_damage_definitions():
    assert ap.text_damage("baro\N{LATIN SMALL LIGATURE FL}ex") == ["ligature"]
    assert ap.text_damage("heat\N{NO-BREAK SPACE}stress") == ["nbsp"]
    assert ap.text_damage("Heat &amp; humidity") == ["entity"]
    assert ap.text_damage("A <i>study</i>") == ["tag"]
    assert ap.text_damage("p<0.05 & x > y; Taylor & Francis; &OV0312;") == []
