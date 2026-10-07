"""W5-C3: paywall_pull and build_priority_paywall_queue (item 8; C109 F-2, M302, M125, M114; PA07,
Pb01, Pb02, Pb06; N1).

- "Done" comes from litpipe.holdings keys, not a regex over `.ris` text: a DOI on a `.ris` note
  line, or named only by an identity-FLAG sidecar, is still pending; a text-only holding is done.
- The opened link is the queue's encoded `url` column, else litpipe.doi.encode_path.
- The EZproxy host has no default and no environment variable: projects.json "ezproxy_host" or
  --ezproxy-host, else exit 1 saying how to set it.
- The queue folder is projects.json "portfolio_dir" (global); unset and no --queue/--out-dir: exit 1.
- --finish never files a file without %PDF in its first 1,024 bytes.
- build_priority_paywall_queue.lib_dois reads `.ris` DO lines as structured DOIs and closes its files.
Every run is on a temp registry, temp libraries and a temp drop folder; no subprocess, no browser."""
import builtins
import csv
import json
import os
from pathlib import Path

import pymupdf
import pytest

import build_priority_paywall_queue as B
import lit_util
import paywall_pull as PP
from litpipe import config

FIELDS = ["rank", "doi", "url", "title", "year", "score", "seeds_pointing", "cited_by", "projects"]
HELD_TEXT = "10.4000/held.text.1"
NOTE_ONLY = "10.4000/note.only.2"
FLAGGED = "10.4000/flagged.3"
PLAIN = "10.4000/plain.4"
ANGLE = "10.1175/1520-0477(1998)079<0001:atdsow>2.0.co;2"


def pdf(path, text="A paper"):
    d = pymupdf.open()
    d.new_page().insert_text((50, 50), text)
    d.save(str(path))
    d.close()
    return Path(path)


class World:
    def __init__(self, tmp, monkeypatch, *, portfolio="_portfolio", ezproxy=None):
        self.tmp = tmp
        self.root = tmp / "root"
        self.root.mkdir()
        monkeypatch.setattr(lit_util, "PROJECTS_ROOT", self.root)
        self.reg = {"state_dir": str(tmp / "state"),
                    "projects": {"teaching_alpha": {"lib_dir": "literature"},
                                 "research_beta": {"lib_dir": "literature"}}}
        if portfolio is not None:
            self.reg["portfolio_dir"] = portfolio
        if ezproxy is not None:
            self.reg["ezproxy_host"] = ezproxy
        self.cfg_path = tmp / "projects.json"
        self.write()
        monkeypatch.setattr(config, "CONFIG_PATH", self.cfg_path)
        monkeypatch.setattr(B, "CONFIG_PATH", str(self.cfg_path))
        self.alpha = self.root / "teaching_alpha" / "literature"
        self.beta = self.root / "research_beta" / "literature"
        self.alpha.mkdir(parents=True)
        self.beta.mkdir(parents=True)
        self.drop = tmp / "drop folder é"
        self.drop.mkdir()
        self.port = self.root / "_portfolio"
        self.opened, self.children, self.ris = [], [], []
        monkeypatch.setattr(PP, "emit_ris_for_pdf", lambda doi, dest, overwrite=False: (
            self.ris.append((doi, dest)) or ("OK", dest)))

    def write(self):
        self.cfg_path.write_text(json.dumps(self.reg), encoding="utf-8")

    def queue(self, rows, folder=None, name="2026-10-07_priority_paywall_queue.csv"):
        folder = Path(folder or self.port)
        folder.mkdir(parents=True, exist_ok=True)
        p = folder / name
        with open(p, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=FIELDS, lineterminator="\n")
            w.writeheader()
            for i, r in enumerate(rows, 1):
                w.writerow({"rank": i, "title": f"Title {i}", "year": "2020", "score": 10 - i,
                            "seeds_pointing": 1, "cited_by": 1, "projects": "teaching_alpha", **r})
        return p

    def run(self, **kw):
        kw.setdefault("opener", self.opened.append)
        kw.setdefault("pause", 0)
        kw.setdefault("child", lambda cmd: self.children.append(cmd) or 0)
        return PP.run(**kw)


@pytest.fixture
def world(tmp_path, monkeypatch):
    return World(tmp_path, monkeypatch)


def held_world(w):
    """A text-only holding, a DOI only on a .ris N1 note line, and a DOI only an identity-FLAG pmc
    sidecar names (both of the last two are NOT holdings)."""
    (w.beta / "2019_Avery_Text.fulltext.json").write_text(json.dumps(
        {"doi": HELD_TEXT, "text": "JATS body text", "has_pdf": False}), encoding="utf-8")
    pdf(w.alpha / "2018_Lee_Other.pdf")
    (w.alpha / "2018_Lee_Other.ris").write_text(
        "TY  - JOUR\nTI  - Other\nDO  - 10.4000/other.9\nN1  - erratum: https://doi.org/" + NOTE_ONLY +
        "\nER  - \n", encoding="utf-8")
    pdf(w.alpha / "2017_Kim_Flag.pdf")
    (w.alpha / "2017_Kim_Flag.fulltext.json").write_text(json.dumps(
        {"doi": FLAGGED, "text": "x", "identity": "FLAG", "has_pdf": True}), encoding="utf-8")
    return w.queue([{"doi": d} for d in (HELD_TEXT, NOTE_ONLY, FLAGGED, PLAIN)])


# ================================================================ done-detection on holdings keys
def test_status_counts_done_from_holdings_not_a_text_regex(world, capsys):
    held_world(world)
    res = world.run()
    assert res["command"] == "status" and res["exit_code"] == 0
    assert (res["total"], res["done"], res["pending"]) == (4, 1, 3)


def test_open_skips_held_rows_and_opens_note_line_and_flagged_dois(world):
    held_world(world)
    res = world.run(open_n=8)
    joined = " ".join(world.opened)
    assert HELD_TEXT not in joined
    assert NOTE_ONLY in joined and FLAGGED in joined and PLAIN in joined
    ledger = json.loads((world.port / PP.LEDGER_NAME).read_text(encoding="utf-8"))
    assert sorted(ledger) == sorted([NOTE_ONLY, FLAGGED, PLAIN])
    assert res["ledger"] == str(world.port / PP.LEDGER_NAME)
    assert world.run(open_n=8)["opened"] == []          # the ledger advances the next call


# ================================================================ encoded links (M302)
def test_the_opened_link_is_the_encoded_url_column_or_encode_path(world):
    world.queue([{"doi": ANGLE, "url": ""}, {"doi": PLAIN, "url": "https://doi.org/10.4000/plain.4"}])
    world.run(open_n=2)
    assert world.opened[0] == "https://doi.org/10.1175/1520-0477(1998)079%3C0001:atdsow%3E2.0.co;2"
    assert world.opened[1] == "https://doi.org/10.4000/plain.4"
    assert all("<" not in u and ">" not in u for u in world.opened)


def test_build_url_encodes_without_a_url_column():
    assert PP.build_url(ANGLE, "doi", "") == ("https://doi.org/10.1175/1520-0477(1998)079%3C0001:"
                                              "atdsow%3E2.0.co;2")
    assert PP.build_url("10.1000/a b", "doi", "").endswith("/10.1000/a%20b")


# ================================================================ the EZproxy host (PA07, Pb01)
def test_ezproxy_without_a_host_exits_1_saying_how_to_set_it(world, capsys):
    world.queue([{"doi": PLAIN}])
    assert PP.main(["--open", "1", "--access", "ezproxy"]) == 1
    err = capsys.readouterr().err.strip().splitlines()
    assert len(err) == 1 and '"ezproxy_host"' in err[0] and "--ezproxy-host" in err[0]
    assert world.opened == []


def test_ezproxy_host_from_projects_json_and_the_flag_overrides(tmp_path, monkeypatch):
    w = World(tmp_path, monkeypatch, ezproxy="https://proxy.example.edu/")
    w.queue([{"doi": ANGLE}])
    w.run(open_n=1, access="ezproxy")
    assert w.opened == ["https://doi-org.proxy.example.edu/10.1175/1520-0477(1998)079%3C0001:atdsow%3E2.0.co;2"]
    w.run(open_n=1, access="ezproxy", ezproxy_host_arg="other-proxy.example.org", reset_opened=True)
    assert w.opened[-1].startswith("https://doi-org.other-proxy.example.org/10.1175/")


def test_no_environment_variable_and_no_built_in_host():
    src = Path(PP.__file__).read_text(encoding="utf-8")
    assert "os.environ" not in src and "getenv" not in src
    parser_default = [a for a in PP.main.__code__.co_consts if isinstance(a, str) and "ezproxy." in a]
    assert all("example" in a for a in parser_default)


def test_the_doi_access_needs_no_host(world):
    world.queue([{"doi": PLAIN}])
    assert world.run(open_n=1)["opened"] == ["https://doi.org/10.4000/plain.4"]


# ================================================================ portfolio_dir (Pb06)
def test_no_portfolio_dir_and_no_queue_exits_1_saying_how_to_set_it(tmp_path, monkeypatch, capsys):
    World(tmp_path, monkeypatch, portfolio=None)
    assert PP.main([]) == 1
    err = capsys.readouterr().err
    assert '"portfolio_dir"' in err and "--queue" in err
    assert not (tmp_path / "root" / "_portfolio").exists()


def test_an_empty_portfolio_dir_exits_1_naming_the_builder(world, capsys):
    assert PP.main(["--open", "2"]) == 1
    err = capsys.readouterr().err
    assert "no priority paywall queue found" in err and "build_priority_paywall_queue.py" in err
    assert world.opened == []


def test_with_queue_and_no_portfolio_dir_the_ledger_sits_beside_the_queue(tmp_path, monkeypatch):
    w = World(tmp_path, monkeypatch, portfolio=None)
    q = w.queue([{"doi": PLAIN}], folder=tmp_path / "queues")
    res = w.run(queue=str(q), open_n=1)
    assert res["ledger"] == str(tmp_path / "queues" / PP.LEDGER_NAME)
    assert not (tmp_path / "root" / "_portfolio").exists()


def test_an_absolute_portfolio_dir_is_used_as_given(tmp_path, monkeypatch):
    w = World(tmp_path, monkeypatch, portfolio=str(tmp_path / "elsewhere"))
    w.queue([{"doi": PLAIN}], folder=tmp_path / "elsewhere")
    assert w.run()["queue"] == str(tmp_path / "elsewhere" / "2026-10-07_priority_paywall_queue.csv")


def test_a_bad_portfolio_dir_value_exits_1(tmp_path, monkeypatch, capsys):
    w = World(tmp_path, monkeypatch)
    w.reg["portfolio_dir"] = 7
    w.write()
    assert PP.main([]) == 1 and "portfolio_dir" in capsys.readouterr().err


def test_the_queue_builder_needs_portfolio_dir_or_out_dir(tmp_path, monkeypatch, capsys):
    w = World(tmp_path, monkeypatch, portfolio=None)
    res = B.run(date="2026-10-07", db=str(tmp_path / "none.duckdb"))
    assert res["exit_code"] == 1 and '"portfolio_dir"' in res["error"] and "--out-dir" in res["error"]
    assert not list(tmp_path.rglob("*_priority_paywall_queue.*"))
    assert B.main(["--date", "2026-10-07", "--db", str(tmp_path / "none.duckdb")]) == 1
    w.reg["portfolio_dir"] = str(tmp_path / "abs_port")
    w.write()
    res = B.run(date="2026-10-07", db=str(tmp_path / "none.duckdb"))
    assert Path(res["csv"]).parent == tmp_path / "abs_port"
    res = B.run(date="2026-10-08", db=str(tmp_path / "none.duckdb"), out_dir=str(tmp_path / "flag"))
    assert Path(res["csv"]).parent == tmp_path / "flag"


# ================================================================ --finish (N1)
def html(path, title, doi):
    path.write_bytes(f"<!DOCTYPE html><html><head><title>{title}</title></head><body><h1>{title}</h1>"
                     f"<p>https://doi.org/{doi}</p></body></html>".encode("utf-8"))
    return path


def test_finish_never_files_an_html_page_saved_as_pdf(world):
    world.queue([{"doi": "10.4000/landing.page.5", "title": "A landing page title"},
                 {"doi": PLAIN}])
    page = html(world.drop / "landing.page.5.pdf", "A landing page title", "10.4000/landing.page.5")
    named = html(world.drop / "teaching_alpha_saved.pdf", "Another", "10.4000/x.6")   # prefix route too
    real = pdf(world.drop / "plain.4.pdf", f"https://doi.org/{PLAIN}")
    res = world.run(finish=True, drop_dir=str(world.drop), apply=True)
    assert sorted(res["not_pdf"]) == sorted([str(page), str(named)])
    assert page.exists() and named.exists()
    assert [Path(p) for p in res["moved"]] == [world.alpha / "plain.4.pdf"] and not real.exists()
    assert [(d, Path(p)) for d, p in world.ris] == [(PLAIN, world.alpha / "plain.4.pdf")]
    assert [c[1].endswith("backfill_ris.py") for c in world.children].count(True) == 1


def test_finish_dry_run_moves_nothing(world):
    world.queue([{"doi": PLAIN}])
    real = pdf(world.drop / "plain.4.pdf")
    res = world.run(finish=True, drop_dir=str(world.drop))
    assert res["to_file"] == [str(real)] and res["moved"] == [] and real.exists()
    assert world.children == [] and world.ris == []


def test_finish_after_apply_clears_the_ledger_for_newly_held_dois(world):
    world.queue([{"doi": PLAIN}])
    world.run(open_n=1)
    pdf(world.drop / "plain.4.pdf")

    def child(cmd):          # stands in for backfill_ris: the .ris that makes the DOI held
        world.children.append(cmd)
        if cmd[1].endswith("backfill_ris.py"):
            (world.alpha / "plain.4.ris").write_text(f"TY  - JOUR\nDO  - {PLAIN}\nER  - \n", encoding="utf-8")
        return 0
    res = world.run(finish=True, drop_dir=str(world.drop), apply=True, child=child)
    assert res["newly"] == 1 and res["remaining"] == 0
    assert json.loads((world.port / PP.LEDGER_NAME).read_text(encoding="utf-8")) == []


def test_prefix_route_docstring_and_logic_are_neutral(world):
    libs = PP.libraries(world.reg)
    assert PP.prefix_route("research_beta_some_paper", libs)[0] == "research_beta"
    assert PP.prefix_route("teaching_alpha_x", libs)[0] == "teaching_alpha"


def test_main_reads_sys_argv_when_argv_is_none(world, monkeypatch):
    world.queue([{"doi": PLAIN}])
    monkeypatch.setattr("sys.argv", ["paywall_pull.py"])
    assert PP.main() == 0


# ================================================================ lib_dois (M125, M114)
def test_lib_dois_reads_do_lines_as_structured_dois_and_closes_its_files(world, monkeypatch):
    held_world(world)
    (world.alpha / "2016_Ng_Tail.ris").write_text("TY  - JOUR\nDO  - 10.1088/2053-1591/ACDECD\nER  - \n",
                                                 encoding="utf-8")
    monkeypatch.setattr(B, "ROOT", str(world.root))
    monkeypatch.setattr(B, "LIBS", {"teaching_alpha": "teaching_alpha/literature",
                                    "research_beta": "research_beta/literature"})
    opened = []
    real_open = builtins.open

    def tracking(*a, **k):
        f = real_open(*a, **k)
        opened.append(f)
        return f
    monkeypatch.setattr(builtins, "open", tracking)
    got = B.lib_dois()
    monkeypatch.setattr(builtins, "open", real_open)
    assert NOTE_ONLY not in got                         # a note line is not the record's DOI
    assert "10.4000/other.9" in got and HELD_TEXT in got
    assert "10.1088/2053-1591/acdecd" in got            # the whole registered form
    assert opened and all(f.closed for f in opened)


def test_the_builder_module_has_no_default_portfolio_folder():
    src = Path(B.__file__).read_text(encoding="utf-8")
    assert '"_portfolio"' not in src and "'_portfolio'" not in src
    src = Path(PP.__file__).read_text(encoding="utf-8")
    assert '"_portfolio"' not in src and "'_portfolio'" not in src
