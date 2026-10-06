"""W4-D: build_priority_paywall_queue rebased on litpipe.worklists and the shared code.

- Golden: on tests/fixtures/W4-D/golden_world.json the queue files equal the ones the base
  (280f592) wrote, compared with normalised line endings; the new files are LF.
- paywall_pull keeps importing lib_dois, LIBS and ROOT, and lib_dois() returns the base's set.
- Importing the module opens no DB (a guard: the base passed it too).
- The second fixture (pins_world.json) pins each fix; every pin failed at the base:
  a non-closed typed row is excluded; a subproject's and an artifact subdirectory's residuals are
  read (archive/ is not); a `<` DOI links with %3C and a `;2-#` SICI DOI links without an
  exception; a DOI named only by an identity-FLAG sidecar or a .ris N1 note line is listed;
  run() honours lit_util.PROJECTS_ROOT and CONFIG_PATH patched after import.
"""
import importlib
import json
import sys
from pathlib import Path

import pytest

import build_priority_paywall_queue as B
import index_portfolio as I
import lit_util
from litpipe import config
from tests import test_w4d_world as W

GOLDEN = W.FIX / "golden"
GOLDEN_SPEC = W.load_spec("golden_world.json")
PINS_SPEC = W.load_spec("pins_world.json")


def _norm(b):
    return b.replace(b"\r\n", b"\n")


class Env:
    def __init__(self, tmp_path, monkeypatch, capsys, spec):
        self.world = W.build(tmp_path, spec)
        self.capsys = capsys
        monkeypatch.setattr(lit_util, "PROJECTS_ROOT", self.world.root)
        monkeypatch.setattr(config, "CONFIG_PATH", self.world.cfg_path)
        monkeypatch.setattr(I, "CONFIG_PATH", self.world.cfg_path)
        monkeypatch.setattr(B, "CONFIG_PATH", str(self.world.cfg_path))

    def index(self, db=None):
        rc = I.main(["--db", str(db or self.world.db)])
        out = self.capsys.readouterr().out
        assert rc == 0, out
        return self

    def queue(self, date, *argv, db=True):
        args = ["--date", date, *argv] + (["--db", str(self.world.db)] if db else [])
        rc = B.main(args)
        cap = self.capsys.readouterr()
        out = self.world.root / "_portfolio"
        return rc, out / f"{date}_priority_paywall_queue.csv", out / f"{date}_priority_paywall_queue.md", cap


@pytest.fixture
def golden(tmp_path, monkeypatch, capsys):
    return Env(tmp_path, monkeypatch, capsys, GOLDEN_SPEC).index()


@pytest.fixture
def pins(tmp_path, monkeypatch, capsys):
    env = Env(tmp_path, monkeypatch, capsys, PINS_SPEC).index()
    rc, csv_p, md_p, cap = env.queue(PINS_SPEC["date"])
    assert rc == 0, cap.out + cap.err
    env.rows = {r["doi"]: r for r in W.read_rows(csv_p)}
    env.md = md_p.read_text(encoding="utf-8")
    env.out = cap.out
    return env


# ================================================================ golden
@pytest.mark.parametrize("run", GOLDEN_SPEC["runs"], ids=lambda r: r["date"])
def test_golden_queue_files_match_the_base(golden, run):
    rc, csv_p, md_p, cap = golden.queue(run["date"], *run["argv"])
    assert rc == 0, cap.out + cap.err
    for got in (csv_p, md_p):
        want = GOLDEN / got.name
        assert _norm(got.read_bytes()) == _norm(want.read_bytes()), got.name


def test_new_queue_files_are_lf(golden):
    rc, csv_p, md_p, _ = golden.queue("2026-10-06")
    assert rc == 0
    for p in (csv_p, md_p):
        data = p.read_bytes()
        assert b"\r\n" not in data and data.count(b"\n") > 3, p.name


def test_out_dir_and_db_flags(golden, tmp_path):
    out = tmp_path / "elsewhere"
    rc, _, _, cap = golden.queue("2026-10-06", "--out-dir", str(out))
    assert rc == 0, cap.err
    assert (out / "2026-10-06_priority_paywall_queue.csv").exists()
    assert not (golden.world.root / "_portfolio").exists()      # created only when it is the target


def test_missing_index_still_writes_the_queue_and_exits_2(golden, tmp_path):
    rc, csv_p, _, cap = golden.queue("2026-10-06", "--db", str(tmp_path / "nope.duckdb"), db=False)
    assert rc == 2
    assert cap.out.rstrip().splitlines()[-1].startswith("[step-summary] ")
    rows = W.read_rows(csv_p)
    assert len(rows) == 7 and {r["seeds_pointing"] for r in rows} == {"0"}


def test_missing_registry_exits_1(golden, monkeypatch, tmp_path):
    monkeypatch.setattr(B, "CONFIG_PATH", str(tmp_path / "missing" / "projects.json"))
    rc, csv_p, _, cap = golden.queue("2026-10-06")
    assert rc == 1 and "projects.json not found" in cap.err
    assert not csv_p.exists()


# ================================================================ paywall_pull keeps its imports
def test_paywall_pull_imports_and_lib_dois_match_the_base(golden, monkeypatch):
    import paywall_pull
    from build_priority_paywall_queue import LIBS, ROOT, lib_dois   # paywall_pull.py:43, verbatim
    assert callable(lib_dois) and isinstance(LIBS, dict) and isinstance(ROOT, str)
    assert paywall_pull.lib_dois.__module__ == B.__name__ and paywall_pull.lib_dois.__name__ == "lib_dois"
    assert paywall_pull.LIBS == B.LIBS and paywall_pull.ROOT == B.ROOT
    monkeypatch.setattr(B, "ROOT", str(golden.world.root))
    monkeypatch.setattr(B, "LIBS", {k: lit_util.lib_rel(k, p) for k, p in GOLDEN_SPEC["projects"].items()})
    want = set(json.loads((GOLDEN / "lib_dois.json").read_text(encoding="utf-8")))
    assert B.lib_dois() == want


def test_import_opens_no_db(monkeypatch):
    """A guard (the base passed it too): nothing at import opens a DuckDB file."""
    import duckdb

    def refuse(*a, **k):
        raise AssertionError("a DuckDB file was opened at import")

    monkeypatch.setattr(duckdb, "connect", refuse)
    monkeypatch.setattr(lit_util, "connect_db", refuse)
    for name in ("build_priority_paywall_queue", "litpipe.worklists"):
        monkeypatch.delitem(sys.modules, name, raising=False)
    mod = importlib.import_module("build_priority_paywall_queue")
    importlib.import_module("litpipe.worklists")
    assert callable(mod.run) and callable(mod.lib_dois)


# ================================================================ the pins (each failed at the base)
def test_pin_a_non_closed_typed_row_is_excluded(pins):
    assert "10.4000/x.2020.303" not in pins.rows    # TRANSIENT
    assert "10.4000/x.2020.304" not in pins.rows    # OA_BLOCKED
    assert "10.4000/x.2020.307" in pins.rows        # a legacy CSV keeps every row
    assert "excluded by class=2" in pins.out


def test_pin_subproject_and_artifact_dir_residuals_are_read(pins):
    assert pins.rows["10.4000/x.2020.306"]["projects"] == "research_beta/sub_study"
    assert pins.rows["10.4000/x.2020.305"]["projects"] == "teaching_alpha"
    assert "10.4000/x.2020.399" not in pins.rows    # archive/ is skipped


def test_pin_links_are_percent_encoded(pins):
    angle = pins.rows["10.1175/1520-0477(1998)079<0001:atdsow>2.0.co;2"]
    assert angle["url"] == "https://doi.org/10.1175/1520-0477(1998)079%3C0001:atdsow%3E2.0.co;2"
    sici = [r for d, r in pins.rows.items() if d.endswith(";2-#")]
    assert len(sici) == 1
    assert sici[0]["url"] == ("https://doi.org/10.1002/%28sici%291097-4636%28199601%2930%3A1%3C1%3A%3A"
                              "aid-jbm1%3E3.0.co%3B2-%23")
    assert "(https://doi.org/10.1175/1520-0477(1998)079%3C0001:atdsow%3E2.0.co;2)" in pins.md


def test_guard_letter_tail_doi_keeps_its_whole_link(pins):
    """Not a pin (the base was right): the text rule would shorten this DOI; the link must not."""
    assert pins.rows["10.1088/2053-1591/acdecd"]["url"] == "https://doi.org/10.1088/2053-1591/acdecd"


def test_pin_a_doi_named_only_by_a_flagged_sidecar_is_listed(pins):
    assert "10.4000/x.2020.302" in pins.rows


def test_pin_a_doi_on_a_ris_note_line_only_is_listed(pins):
    row = pins.rows["10.4000/x.2020.301"]
    assert (row["seeds_pointing"], row["cited_by"], row["score"]) == ("1", "30", "40")
    assert "10.2000/rb.2018.009" not in pins.rows


def test_pin_run_honours_root_and_config_patched_after_import(tmp_path, monkeypatch, capsys):
    """Patch only lit_util.PROJECTS_ROOT and this module's CONFIG_PATH (never litpipe.config's), put
    the index at <root>/_references, and pass no --db / --out-dir: everything resolves at call time."""
    world = W.build(tmp_path, PINS_SPEC)
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", world.root)
    monkeypatch.setattr(I, "CONFIG_PATH", world.cfg_path)
    monkeypatch.setattr(B, "CONFIG_PATH", str(world.cfg_path))
    db = world.root / "_references" / "portfolio.duckdb"
    assert I.main(["--db", str(db)]) == 0
    capsys.readouterr()
    res = B.run(date="2026-10-09")
    assert res["exit_code"] == 0, res
    assert Path(res["db"]) == db
    assert Path(res["csv"]).parent == world.root / "_portfolio"
    rows = {r["doi"]: r for r in W.read_rows(res["csv"])}
    assert rows["10.4000/x.2020.301"]["score"] == "40"
    assert "10.4000/x.2020.306" in rows
