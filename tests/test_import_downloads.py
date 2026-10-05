"""W3-D1: import_downloads becomes a safe shared tool (T10 plus).

No default library (--lib-dir or --project, refusing with neither); no email, user agent or personal
path in the module; dry run by default and writing nothing but its _DRYRUN report; DOIs through
litpipe.doi with cover sheets and boilerplate set aside; identity through litpipe.identity (a DOI's
record must have its title printed, a boilerplate record never counts); dedup against the holdings
map and within the run; a text-only holding takes its PDF; names through build_filename with
display-clean metadata; sidecars through clean_pdf_text; the .ris written and never overwritten."""
import csv
import hashlib
import importlib
import json
import os
import re
import subprocess
import sys
import textwrap
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import fitz
import pytest

import import_downloads as ID
import lit_util
import ris_emit as R
from litpipe import config, holdings, net
from litpipe.outcomes import Kind
from tests.netmock import Reply

FIX = Path(__file__).resolve().parent / "fixtures" / "W3-D1"
REPO = Path(__file__).resolve().parent.parent
RECORDS = json.loads((FIX / "crossref_records.json").read_text(encoding="utf-8"))
COVERS = json.loads((FIX / "cover_pages.json").read_text(encoding="utf-8"))
LIGATURES = tuple(chr(c) for c in range(0xFB00, 0xFB07))
JSON = {"Content-Type": "application/json"}
FILLER = ("Methods. Participants completed a graded exercise test on a cycle ergometer while heart "
          "rate, blood pressure and oxygen uptake were recorded continuously, and the results were "
          "analysed with linear mixed models adjusted for age, sex and training status. ") * 4


def meta(name):
    return R.crossref_meta(RECORDS[name])


def title_of(name):
    return meta(name)["title"]


def doi_of(name):
    return meta(name)["doi"]


def page(title="", doi="", header="Journal of Example Physiology 2020; 1: 1-10", body=FILLER,
         authors="A. Writer, B. Writer", pre=""):
    parts = [p for p in (pre, header, f"https://doi.org/{doi}" if doi else "", title, authors, body) if p]
    return "\n".join(parts)


def make_pdf(path, pages, fontfile=None):
    d = fitz.open()
    for text in pages:
        p = d.new_page()
        lines = []
        for para in text.split("\n"):
            lines.extend(textwrap.wrap(para, 95) or [""])
        kw = {}
        if fontfile:
            p.insert_font(fontname="F0", fontfile=fontfile)
            kw = {"fontname": "F0"}
        p.insert_text((40, 50), "\n".join(lines), fontsize=8, **kw)
    d.save(str(path))
    d.close()
    return Path(path)


def tree(*roots):
    """{path: (size, mtime_ns, sha256)} for every file under the roots."""
    out = {}
    for r in roots:
        for p in Path(r).rglob("*"):
            if p.is_file():
                st = p.stat()
                out[str(p)] = (st.st_size, st.st_mtime_ns, hashlib.sha256(p.read_bytes()).hexdigest())
    return out


def assert_no_ligatures(*libs):
    for lib in libs:
        for sc in Path(lib).glob("*.fulltext.json"):
            raw = sc.read_text(encoding="utf-8")
            assert not any(ch in raw for ch in LIGATURES), sc.name


@pytest.fixture
def env(tmp_path, monkeypatch):
    """A temp projects root and registry, a teaching library, a temp Downloads, and doubles for
    ris_emit.resolve_meta and the Crossref search (answers keyed by DOI; calls logged)."""
    root = tmp_path / "root"
    root.mkdir()
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    reg = {"state_dir": str(tmp_path / "state"), "projects": {}}
    reg_path = tmp_path / "projects.json"

    def write_reg():
        reg_path.write_text(json.dumps(reg), encoding="utf-8")
    write_reg()
    monkeypatch.setattr(config, "CONFIG_PATH", reg_path)
    lib = root / "teaching_course" / "literature"
    lib.mkdir(parents=True)
    dl = tmp_path / "downloads"
    dl.mkdir()
    answers, resolved, searches = {}, [], []
    search = SimpleNamespace(results=[])

    def fake_resolve(doi):
        resolved.append(doi)
        a = answers.get(str(doi).lower(), ({}, "none"))
        if isinstance(a, BaseException):
            raise a
        return a

    def fake_search(query):
        searches.append(query)
        if isinstance(search.results, BaseException):
            raise search.results
        return list(search.results)
    monkeypatch.setattr(R, "resolve_meta", fake_resolve)
    monkeypatch.setattr(ID, "crossref_search", fake_search)

    def add(*names):
        for n in names:
            m = meta(n)
            answers[m["doi"]] = (m, "crossref")

    def run(**kw):
        kw.setdefault("downloads", str(dl))
        if "project" not in kw:
            kw.setdefault("lib_dir", str(lib))
        return ID.run(**kw)

    return SimpleNamespace(root=root, reg=reg, write_reg=write_reg, reg_path=reg_path, lib=lib, dl=dl,
                           answers=answers, resolved=resolved, searches=searches, search=search, add=add,
                           run=run, tmp=tmp_path)


def by_source(res):
    return {r["source"]: r for r in res["rows"]}


# ---------------------------------------------------------------- library, flags, refusals
def test_no_library_refuses_with_one_line_and_exit_1(env, capsys):
    assert ID.main(["--downloads", str(env.dl)]) == 1
    err = capsys.readouterr().err.strip().splitlines()
    assert len(err) == 1 and "--lib-dir" in err[0] and "--project" in err[0]


def test_main_reads_sys_argv_when_argv_is_none(env, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["import_downloads.py", "--downloads", str(env.dl)])
    assert ID.main() == 1
    assert "--lib-dir" in capsys.readouterr().err


@pytest.mark.parametrize("argv, needle", [
    (["--lib-dir", "x", "--project", "y"], "not both"),
    (["--project", "research_nowhere"], "not registered"),
    (["--lib-dir", "no/such/dir"], "library not found"),
    (["--execute", "--dry-run", "--lib-dir", "x"], "not both"),
    (["--cutoff", "yesterday-ish"], "--cutoff"),
])
def test_usage_errors_exit_1_with_one_line(env, capsys, argv, needle):
    if "--cutoff" in argv:
        argv = argv + ["--lib-dir", str(env.lib), "--downloads", str(env.dl)]
    assert ID.main(argv) == 1
    err = capsys.readouterr().err.strip().splitlines()
    assert len(err) == 1 and needle in err[0]


def test_missing_registry_exits_1(env, monkeypatch, capsys):
    monkeypatch.setattr(config, "CONFIG_PATH", env.tmp / "absent" / "projects.json")
    assert ID.main(["--project", "teaching_course", "--downloads", str(env.dl)]) == 1
    assert "not registered" in capsys.readouterr().err


def test_missing_downloads_or_report_dir_exits_1(env, capsys):
    assert ID.main(["--lib-dir", str(env.lib), "--downloads", str(env.tmp / "nope")]) == 1
    assert ID.main(["--lib-dir", str(env.lib), "--downloads", str(env.dl),
                    "--report-dir", str(env.tmp / "nope")]) == 1
    assert capsys.readouterr().err.count("does not exist") == 2


def test_project_is_tail_aware_and_reports_into_the_project_folder(env):
    env.reg["projects"]["teaching_parent/course_a"] = {"parent": "teaching_parent",
                                                       "lib_dir": "course_a/literature"}
    env.write_reg()
    lib = env.root / "teaching_parent" / "course_a" / "literature"
    lib.mkdir(parents=True)
    env.add("stroke_volume")
    make_pdf(env.dl / "a.pdf", [page(title_of("stroke_volume"), doi_of("stroke_volume"))])
    res = env.run(project="teaching_parent/course_a")
    assert Path(res["lib_dir"]) == lib
    assert Path(res["report"]).parent == env.root / "teaching_parent" / "course_a"
    assert res["rows"][0]["action"] == "WOULD_MOVE"


def test_documented_form_lib_dir_cutoff_execute_without_report_dir(env, monkeypatch):
    """The consumer's run instructions: `--lib-dir <lib> --cutoff <t> --execute`, with neither
    --report-dir nor --downloads (the default is ~/Downloads)."""
    home = env.tmp / "home"
    (home / "Downloads").mkdir(parents=True)
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: home))
    env.add("stroke_volume")
    make_pdf(home / "Downloads" / "190.full.pdf", [page(title_of("stroke_volume"), doi_of("stroke_volume"))])
    cutoff = (datetime.now() - timedelta(minutes=5)).isoformat(timespec="minutes")
    assert ID.main(["--lib-dir", str(env.lib), "--cutoff", cutoff, "--execute"]) == 0
    report = env.lib.parent / f"_downloads_import_{date.today():%Y-%m-%d}.csv"
    rows = list(csv.DictReader(report.open(encoding="utf-8")))
    assert [r["action"] for r in rows] == ["MOVED"]
    name = rows[0]["new_name"]
    assert (env.lib / name).exists() and not (home / "Downloads" / "190.full.pdf").exists()
    assert (env.lib / name).with_suffix(".ris").exists()
    assert lit_util.companion_path(env.lib / name, ".fulltext.json").exists()


def test_cutoff_defaults_to_today_at_local_midnight(env):
    env.add("stroke_volume", "shuttle_run")
    old = make_pdf(env.dl / "old.pdf", [page(title_of("shuttle_run"), doi_of("shuttle_run"))])
    make_pdf(env.dl / "new.pdf", [page(title_of("stroke_volume"), doi_of("stroke_volume"))])
    yesterday = datetime.combine(date.today(), datetime.min.time()) - timedelta(minutes=1)
    os.utime(old, (yesterday.timestamp(), yesterday.timestamp()))
    res = env.run()
    assert [r["source"] for r in res["rows"]] == ["new.pdf"]
    assert res["cutoff"] == datetime.combine(date.today(), datetime.min.time()).isoformat()


def test_cutoff_accepts_a_timezone_aware_string(env):
    make_pdf(env.dl / "a.pdf", [page("Some title of a paper", "")])
    res = env.run(cutoff=(datetime.now().astimezone() - timedelta(hours=1)).isoformat())
    assert len(res["rows"]) == 1


def test_report_keeps_the_legacy_columns_first():
    assert ID.REPORT_FIELDS[:10] == ["source", "size_kb", "action", "doi", "year", "first_au", "title",
                                     "new_name", "dup_match", "note"]


# ---------------------------------------------------------------- dry run
def test_dry_run_moves_and_writes_nothing_but_its_report(env):
    env.add("stroke_volume", "autonomic")
    (env.lib / "2015_Quill_Autonomic.fulltext.json").write_text(json.dumps(
        {"doi": doi_of("autonomic"), "title": title_of("autonomic"), "text": "JATS body text"}), encoding="utf-8")
    make_pdf(env.dl / "a.pdf", [page(title_of("stroke_volume"), doi_of("stroke_volume"))])
    make_pdf(env.dl / "b.pdf", [page(title_of("autonomic"), doi_of("autonomic"))])
    before = tree(env.root, env.dl)
    kv_before = dict(net.STATE.kv)
    res = env.run()
    after = tree(env.root, env.dl)
    report = env.lib.parent / f"_downloads_import_{date.today():%Y-%m-%d}_DRYRUN.csv"
    assert set(after) - set(before) == {str(report)}
    assert {k: after[k] for k in before} == before
    assert net.STATE.kv == kv_before and R.STATE is None
    assert {r["action"] for r in res["rows"]} == {"WOULD_MOVE"}
    assert {r["ris"] for r in res["rows"]} == {"WOULD_WRITE"}


def test_dry_run_drops_ris_emit_cache_writes_and_execute_keeps_them(env, monkeypatch):
    """resolve_meta caches a DOI's agency in the state kv; a dry run must leave the state as it was."""
    real_meta = meta("stroke_volume")

    def resolving(doi):
        R._kv_set("doi_ra", "10.1136", "crossref")
        return real_meta, "crossref"
    monkeypatch.setattr(R, "resolve_meta", resolving)
    make_pdf(env.dl / "a.pdf", [page(title_of("stroke_volume"), doi_of("stroke_volume"))])
    env.run()
    assert ("doi_ra", "10.1136") not in net.STATE.kv
    env.run(execute=True)
    assert net.STATE.kv[("doi_ra", "10.1136")] == "crossref"


# ---------------------------------------------------------------- cover sheets and boilerplate
@pytest.mark.parametrize("name, tag", [("ill_cover_1", "ill"), ("ill_cover_2", "ill"),
                                       ("ill_notice_only", "ill"), ("tandf_cover", "tandf"),
                                       ("jstor_cover", "jstor"), ("ebsco_notice", "copyright_notice")])
def test_cover_pages_are_recognised(name, tag):
    assert ID.cover_tag(COVERS[name]) == tag


def test_an_article_page_is_not_a_cover_and_loses_its_boilerplate_passages():
    p = page("A Paper", "10.1234/x.1") + "\n" + COVERS["ebsco_notice"] + "\nDownloaded from example.org on May 1"
    assert ID.cover_tag(p) == ""
    cleaned = ID.strip_boilerplate(p)
    assert "is the property of" not in cleaned and "Downloaded from" not in cleaned
    assert "A Paper" in cleaned and "10.1234/x.1" in cleaned


@pytest.mark.parametrize("name, bad", [("boilerplate_copyright_notice", True), ("copyright_law_paper", False),
                                       ("stroke_volume", False)])
def test_boilerplate_records(name, bad):
    assert ID.is_boilerplate_record(meta(name)) is bad


def _consumer_case(case):
    with (FIX / "consumer_report_cases.csv").open(encoding="utf-8") as f:
        return next(r for r in csv.DictReader(f) if r["case"] == case)


def test_consumer_case_copyright_notice_record_from_cover_text_is_rejected(env):
    """Old: an EBSCO download with no DOI was filed as `2022_Unknown_Copyright2022By...` because a
    title search matched a Crossref record whose title is a copyright notice."""
    case = _consumer_case("boilerplate_cover_search")
    notice = "Copyright \u00a92022 by European Association for Signal Processing (EURASIP) All rights reserved"
    make_pdf(env.dl / case["source"], [page("Exercise Blood Pressure in Untrained Adults", "",
                                            pre=COVERS["ebsco_notice"], body=FILLER + notice)])
    env.search.results = [RECORDS["boilerplate_copyright_notice"], RECORDS["copyright_law_paper"]]
    res = env.run(execute=True)
    row = by_source(res)[case["source"]]
    assert row["action"] == "UNKNOWN_LEAVE" and row["doi"] == "" and row["new_name"] == ""
    assert (env.dl / case["source"]).exists() and not list(env.lib.iterdir())
    assert env.searches and all("is the property of" not in q for q in env.searches)
    assert case["old_action"] == "WOULD_MOVE"                   # the evidence this test reverses


def test_consumer_case_ill_notice_search_match_is_rejected(env):
    """Old: an ILL scan's Title 17 notice went to the title search, which matched a paper about
    copyright law and filed it under that DOI."""
    case = _consumer_case("boilerplate_ill_search")
    make_pdf(env.dl / case["source"], [COVERS["ill_notice_only"],
                                       page("Exercise Blood Pressure in Untrained Adults", "")])
    env.search.results = [RECORDS["copyright_law_paper"]]
    res = env.run()
    row = by_source(res)[case["source"]]
    assert row["action"] == "UNKNOWN_LEAVE" and row["doi"] == ""
    assert "cover pages skipped: ill" in row["detail"]
    assert env.searches and all("copyright law" not in q.lower() for q in env.searches)


def test_a_printed_doi_of_a_boilerplate_record_is_never_accepted(env):
    env.add("boilerplate_copyright_notice")
    notice = "Copyright \u00a92022 by European Association for Signal Processing (EURASIP) All rights reserved"
    make_pdf(env.dl / "a.pdf", [page(notice, doi_of("boilerplate_copyright_notice"))])
    row = env.run()["rows"][0]
    assert row["action"] in ("UNKNOWN_LEAVE", "IDENTITY_FLAG") and row["action"] != "WOULD_MOVE"
    assert "boilerplate record" in row["detail"]


def test_two_page_ill_cover_is_skipped_and_the_doi_read_from_page_3(env):
    env.add("autonomic")
    make_pdf(env.dl / "ill.pdf", [COVERS["ill_cover_1"], COVERS["ill_cover_2"],
                                  page(title_of("autonomic"), doi_of("autonomic"))])
    row = env.run()["rows"][0]
    assert row["action"] == "WOULD_MOVE" and row["doi"] == doi_of("autonomic")
    assert "cover pages skipped: ill" in row["detail"]
    assert row["new_name"].startswith("2015_Quill_Autonomic")


def test_tandf_cover_doi_is_used_when_the_article_prints_its_title(env):
    env.add("shuttle_run")
    make_pdf(env.dl / "tf.pdf", [COVERS["tandf_cover"],
                                 page(title_of("shuttle_run"), "", header="JOURNAL OF SPORTS SCIENCES, 1988, 6, 93-101")])
    row = env.run()["rows"][0]
    assert row["action"] == "WOULD_MOVE" and row["doi"] == doi_of("shuttle_run")
    assert "doi_cover" in row["detail"]


def test_known_misfetch_fingerprint_is_boilerplate(env):
    make_pdf(env.dl / "lww.pdf", [page("Lippincott Journal Portfolio", "", body="Author Permission Guidelines " * 20)])
    row = env.run()["rows"][0]
    assert row["action"] == "BOILERPLATE" and (env.dl / "lww.pdf").exists()


# ---------------------------------------------------------------- identity
def test_correction_notice_doi_loses_to_the_title_confirmed_doi(env):
    env.add("correction_notice", "corrected_article")
    pre = f"This online publication has been corrected. See https://doi.org/{doi_of('correction_notice')}"
    make_pdf(env.dl / "c.pdf", [page(title_of("corrected_article"), doi_of("corrected_article"), pre=pre)])
    row = env.run()["rows"][0]
    assert row["action"] == "WOULD_MOVE" and row["doi"] == doi_of("corrected_article")
    assert env.resolved[0] == doi_of("correction_notice")      # tried first, and refused on its title


def test_reference_list_dois_are_not_taken_for_the_paper(env):
    env.answers["10.1234/ref.1001"] = ({**meta("stroke_volume"), "doi": "10.1234/ref.1001",
                                       "title": "Cardiac output during prolonged cycling"}, "crossref")
    env.answers["10.1234/ref.2002"] = ({**meta("stroke_volume"), "doi": "10.1234/ref.2002",
                                       "title": "Venous return and posture in older adults"}, "crossref")
    refs = "References\n1. Someone A. Paper one. https://doi.org/10.1234/ref.1001\n2. Other B. Paper two. doi:10.1234/ref.2002"
    make_pdf(env.dl / "r.pdf", [page("Body mass and mortality across four continents", ""),
                                page("", "", header="Discussion", body=FILLER + refs)])
    row = env.run(execute=True)["rows"][0]
    assert row["action"] == "IDENTITY_FLAG" and row["identity"] == "FLAG"
    assert "10.1234/ref.1001" in row["detail"] and (env.dl / "r.pdf").exists()


def test_a_single_reference_doi_after_page_1_is_not_taken_either(env):
    env.answers["10.1234/ref.1001"] = ({**meta("stroke_volume"), "doi": "10.1234/ref.1001",
                                       "title": "Cardiac output during prolonged cycling"}, "crossref")
    make_pdf(env.dl / "r.pdf", [page("Body mass and mortality across four continents", ""),
                                page("", "", header="References", body="1. A. Paper. https://doi.org/10.1234/ref.1001")])
    assert env.run()["rows"][0]["action"] == "IDENTITY_FLAG"


def test_the_only_doi_printed_on_page_1_is_taken_when_the_title_is_not_in_the_text(env):
    """A scanned back issue: the publisher's DOI stamp is the only DOI, the title block has no text."""
    env.add("stroke_volume")
    make_pdf(env.dl / "s.pdf", [page("", doi_of("stroke_volume"))])
    row = env.run()["rows"][0]
    assert row["action"] == "WOULD_MOVE" and row["identity"] == "OK" and "sole_doi" in row["detail"]


def test_title_search_match_is_accepted_only_when_its_title_is_printed(env):
    make_pdf(env.dl / "a.pdf", [page(title_of("stroke_volume"), "")])
    env.search.results = [RECORDS["copyright_law_paper"], RECORDS["stroke_volume"]]
    row = env.run()["rows"][0]
    assert row["action"] == "WOULD_MOVE" and row["doi"] == doi_of("stroke_volume")
    assert row["identity"] == "TITLE_MATCH" and row["meta_source"] == "crossref_search"


def test_no_text_layer_is_unknown_with_a_hint(env):
    d = fitz.open()
    d.new_page()
    d.save(str(env.dl / "scan.pdf"))
    d.close()
    row = env.run()["rows"][0]
    assert row["action"] == "UNKNOWN_LEAVE" and "--doi" in row["detail"] and not env.searches


def test_supplement_is_flagged(env):
    env.add("stroke_volume")
    text = ("Supplementary appendix\nThis appendix has been provided by the authors to give readers "
            "additional information about their work.\n" + page(title_of("stroke_volume"), doi_of("stroke_volume")))
    make_pdf(env.dl / "supp.pdf", [text])
    row = env.run()["rows"][0]
    assert row["action"] == "IDENTITY_FLAG" and row["doc_kind"] == "SUPPLEMENT"


def test_unreadable_pdf_is_err_read(env):
    (env.dl / "broken.pdf").write_bytes(b"%PDF-1.4 not really")
    row = env.run()["rows"][0]
    assert row["action"] == "ERR_READ" and (env.dl / "broken.pdf").exists()


def test_metadata_unavailable_is_counted_and_the_file_stays(env):
    out = SimpleNamespace(kind=Kind.OUTAGE, status=503, detail="HTTP 503")
    env.answers[doi_of("stroke_volume")] = R.MetadataUnavailable("crossref", out)
    make_pdf(env.dl / "a.pdf", [page(title_of("stroke_volume"), doi_of("stroke_volume"))])
    res = env.run(execute=True)
    row = res["rows"][0]
    assert row["action"] == "META_UNAVAILABLE" and row["outcome"] == Kind.OUTAGE.value
    assert res["counts"] == {"META_UNAVAILABLE": 1} and (env.dl / "a.pdf").exists()
    assert not env.searches                       # a failed call is not a "no": no fallback guess


def test_doi_override_needs_exactly_one_pdf(env, capsys):
    env.add("stroke_volume")
    make_pdf(env.dl / "a.pdf", [page("x", "")])
    make_pdf(env.dl / "b.pdf", [page("y", "")])
    assert ID.main(["--lib-dir", str(env.lib), "--downloads", str(env.dl), "--doi", doi_of("stroke_volume")]) == 1
    assert "exactly one" in capsys.readouterr().err


def test_doi_override_that_does_not_resolve_exits_1(env, capsys):
    make_pdf(env.dl / "a.pdf", [page("x", "")])
    assert ID.main(["--lib-dir", str(env.lib), "--downloads", str(env.dl), "--doi", "10.1234/none.1"]) == 1
    assert "did not resolve" in capsys.readouterr().err


def test_doi_override_files_an_unidentifiable_pdf(env):
    env.add("stroke_volume")
    make_pdf(env.dl / "a.pdf", [page("", "")])
    res = env.run(doi=doi_of("stroke_volume"), execute=True)
    row = res["rows"][0]
    assert row["action"] == "MOVED" and row["doi"] == doi_of("stroke_volume")
    sc = json.loads(lit_util.companion_path(env.lib / row["new_name"], ".fulltext.json").read_text(encoding="utf-8"))
    assert sc["identity_method"] == "--doi"


# ---------------------------------------------------------------- dedup
def test_same_doi_twice_in_one_run_is_flagged_not_imported_twice(env):
    a, b = _consumer_case("same_doi_twice_a"), _consumer_case("same_doi_twice_b")
    env.add("stroke_volume")
    for case in (a, b):
        make_pdf(env.dl / case["source"], [page(title_of("stroke_volume"), doi_of("stroke_volume"))])
    dry = by_source(env.run())
    assert dry[a["source"]]["action"] == "WOULD_MOVE"
    assert dry[b["source"]]["action"] == "DUP_IN_RUN" and dry[b["source"]]["dup_match"] == a["source"]
    res = by_source(env.run(execute=True))
    assert res[a["source"]]["action"] == "MOVED" and res[b["source"]]["action"] == "DUP_IN_RUN"
    assert len(list(env.lib.glob("*.pdf"))) == 1 and (env.dl / b["source"]).exists()


def test_a_pdf_holding_in_the_library_is_dup_doi(env):
    env.add("stroke_volume")
    make_pdf(env.lib / "2005_Penrose_Held.pdf", [page("held")])
    (env.lib / "2005_Penrose_Held.fulltext.json").write_text(json.dumps(
        {"doi": doi_of("stroke_volume"), "text": "x", "has_pdf": True}), encoding="utf-8")
    make_pdf(env.dl / "a.pdf", [page(title_of("stroke_volume"), doi_of("stroke_volume"))])
    row = env.run(execute=True)["rows"][0]
    assert row["action"] == "DUP_DOI" and row["dup_match"] == "2005_Penrose_Held"
    assert (env.dl / "a.pdf").exists()


def test_an_identity_flagged_pdf_is_not_a_holding(env):
    env.add("stroke_volume")
    make_pdf(env.lib / "2005_Penrose_Other.pdf", [page("another work")])
    (env.lib / "2005_Penrose_Other.ris").write_text(f"TY  - JOUR\nDO  - {doi_of('stroke_volume')}\nER  - \n",
                                                    encoding="utf-8")
    (env.lib / "2005_Penrose_Other.identity.json").write_text(json.dumps(
        {"identity": "FLAG", "queue_doi": doi_of("stroke_volume")}), encoding="utf-8")
    make_pdf(env.dl / "a.pdf", [page(title_of("stroke_volume"), doi_of("stroke_volume"))])
    assert env.run()["rows"][0]["action"] == "WOULD_MOVE"


def test_text_only_holding_takes_the_pdf(env):
    """A pre-W2a JATS sidecar (no has_pdf key) is text-only: the PDF takes its stem and has_pdf."""
    env.add("stroke_volume")
    stem = "2005_Penrose_StrokeVolumeJats"
    sc = env.lib / f"{stem}.fulltext.json"
    jats = {"doi": doi_of("stroke_volume"), "pmcid": "PMC1000001", "title": title_of("stroke_volume"),
            "text": "JATS body text, kept as the better text."}
    sc.write_text(json.dumps(jats), encoding="utf-8")
    make_pdf(env.dl / "a.pdf", [page(title_of("stroke_volume"), doi_of("stroke_volume"))])
    dry = env.run()["rows"][0]
    assert dry["action"] == "WOULD_MOVE" and dry["new_name"] == f"{stem}.pdf" and dry["dup_match"] == stem
    row = env.run(execute=True)["rows"][0]
    assert row["action"] == "MOVED" and (env.lib / f"{stem}.pdf").exists()
    rec = json.loads(sc.read_text(encoding="utf-8"))
    assert rec["has_pdf"] is True and rec["text"] == jats["text"] and rec["pmcid"] == "PMC1000001"
    assert f"DO  - {doi_of('stroke_volume')}" in (env.lib / f"{stem}.ris").read_text(encoding="utf-8")
    hm = holdings.build({"projects": {"teaching_course": {"lib_dir": "literature"}}},
                        use_cache=False, write_cache=False)
    assert hm.has_pdf(doi_of("stroke_volume")) and not hm.text_only(doi_of("stroke_volume"))


def test_text_only_holding_keeps_its_curated_ris(env):
    env.add("stroke_volume")
    stem = "2005_Penrose_StrokeVolumeJats"
    (env.lib / f"{stem}.fulltext.json").write_text(json.dumps(
        {"doi": doi_of("stroke_volume"), "text": "JATS"}), encoding="utf-8")
    curated = f"TY  - JOUR\nTI  - Curated by hand\nDO  - {doi_of('stroke_volume')}\nER  - \n"
    (env.lib / f"{stem}.ris").write_text(curated, encoding="utf-8")
    make_pdf(env.dl / "a.pdf", [page(title_of("stroke_volume"), doi_of("stroke_volume"))])
    row = env.run(execute=True)["rows"][0]
    assert row["action"] == "MOVED" and row["ris"] == "EXISTS"
    assert (env.lib / f"{stem}.ris").read_text(encoding="utf-8") == curated


def test_a_holding_elsewhere_is_reported_and_the_file_still_imported(env):
    env.reg["projects"]["teaching_course"] = {"lib_dir": "literature"}
    env.reg["projects"]["research_other"] = {"lib_dir": "literature"}
    env.write_reg()
    other = env.root / "research_other" / "literature"
    other.mkdir(parents=True)
    make_pdf(other / "2005_Penrose_Elsewhere.pdf", [page("x")])
    (other / "2005_Penrose_Elsewhere.fulltext.json").write_text(json.dumps(
        {"doi": doi_of("stroke_volume"), "text": "x", "has_pdf": True}), encoding="utf-8")
    env.add("stroke_volume")
    make_pdf(env.dl / "a.pdf", [page(title_of("stroke_volume"), doi_of("stroke_volume"))])
    row = env.run(execute=True)["rows"][0]
    assert row["action"] == "MOVED" and row["held_elsewhere"].endswith("2005_Penrose_Elsewhere.pdf")
    assert (env.lib / row["new_name"]).exists()


def test_an_unregistered_lib_dir_is_still_checked_for_holdings(env):
    lib = env.tmp / "loose" / "literature"
    lib.mkdir(parents=True)
    make_pdf(lib / "2005_Penrose_Held.pdf", [page("held")])
    (lib / "2005_Penrose_Held.ris").write_text(f"TY  - JOUR\nDO  - {doi_of('stroke_volume')}\nER  - \n",
                                               encoding="utf-8")
    env.add("stroke_volume")
    make_pdf(env.dl / "a.pdf", [page(title_of("stroke_volume"), doi_of("stroke_volume"))])
    assert env.run(lib_dir=str(lib))["rows"][0]["action"] == "DUP_DOI"


@pytest.mark.parametrize("held_year, action", [("2005", "DUP_TITLE"), ("1995", "WOULD_MOVE")])
def test_title_dedup_only_against_doi_less_pdfs_of_the_same_edition(env, held_year, action):
    env.add("stroke_volume")
    make_pdf(env.lib / "Penrose_NoDoi.pdf", [page("x")])
    (env.lib / "Penrose_NoDoi.fulltext.json").write_text(json.dumps(
        {"title": title_of("stroke_volume"), "year": held_year, "text": "x"}), encoding="utf-8")
    make_pdf(env.dl / "a.pdf", [page(title_of("stroke_volume"), doi_of("stroke_volume"))])
    assert env.run()["rows"][0]["action"] == action


def test_a_held_pdf_with_another_doi_and_the_same_title_is_not_a_dup(env):
    env.add("stroke_volume")
    make_pdf(env.lib / "2001_Penrose_Earlier.pdf", [page("x")])
    (env.lib / "2001_Penrose_Earlier.fulltext.json").write_text(json.dumps(
        {"doi": "10.1234/earlier.2001", "title": title_of("stroke_volume"), "year": "2005", "text": "x"}),
        encoding="utf-8")
    make_pdf(env.dl / "a.pdf", [page(title_of("stroke_volume"), doi_of("stroke_volume"))])
    assert env.run()["rows"][0]["action"] == "WOULD_MOVE"


def test_a_taken_name_is_suffixed_never_overwritten(env):
    env.add("stroke_volume")
    name = ID.proposed_name(meta("stroke_volume"))[0]
    make_pdf(env.lib / name, [page("an unrelated file without sidecars")])
    before = (env.lib / name).read_bytes()
    make_pdf(env.dl / "a.pdf", [page(title_of("stroke_volume"), doi_of("stroke_volume"))])
    row = env.run(execute=True)["rows"][0]
    assert row["action"] == "MOVED" and row["new_name"] != name and "suffixed" in row["note"]
    assert (env.lib / name).read_bytes() == before


# ---------------------------------------------------------------- names, text, sidecars, .ris
def test_group_first_author_does_not_give_unknown(env):
    case = _consumer_case("unknown_author")
    env.add("group_first_author")
    make_pdf(env.dl / case["source"], [page(title_of("group_first_author"), doi_of("group_first_author"))])
    row = env.run()["rows"][0]
    assert "Unknown" in case["old_new_name"]
    assert row["action"] == "WOULD_MOVE" and row["new_name"].startswith("2025_Rowan_")
    assert row["first_au"] == "Rowan"


@pytest.mark.parametrize("authors, token", [
    ([{"family": "Writing Committee Members*", "given": ""}, {"family": "Rowan", "given": "D"}], "Rowan"),
    ([{"family": "Example Heart Association", "given": ""}], "Association"),
    ([{"family": "Holm", "given": ""}, {"family": "Varga", "given": "B"}], "Holm"),
    ([{"family": "van der Walt", "given": "J"}], "vanderWalt"),
    ([], "Unknown"),
])
def test_filename_author(authors, token):
    m = {**meta("stroke_volume"), "authors": authors}
    assert ID.proposed_name(m)[0].split("_")[1] == token


def test_markup_title_gives_a_clean_name_sidecar_and_report(env):
    case = _consumer_case("markup_in_name")
    env.add("sup_markup_title")
    make_pdf(env.dl / case["source"], [page(title_of("sup_markup_title"), doi_of("sup_markup_title"))])
    row = env.run(execute=True)["rows"][0]
    assert "Sup31sup" in case["old_new_name"]
    assert row["action"] == "MOVED" and "sup" not in row["new_name"].lower() and "<" not in row["new_name"]
    assert row["new_name"].startswith("2003_Holm_31")
    assert "<" not in row["title"]
    sc = json.loads(lit_util.companion_path(env.lib / row["new_name"], ".fulltext.json").read_text(encoding="utf-8"))
    assert sc["title"].startswith("31P-MRS") and "<" not in json.dumps(sc["authors"])
    assert "<sup>" not in (env.lib / row["new_name"]).with_suffix(".ris").read_text(encoding="utf-8")


def test_markup_from_any_metadata_source_never_reaches_name_report_or_sidecar(env):
    """Whatever a source returns (here a raw JATS title and journal), this module displays it clean."""
    raw = {**meta("sup_markup_title"), "title": RECORDS["sup_markup_title"]["title"][0],
           "container": "Int J <i>Sports</i> Med", "authors": [{"family": "Holm", "given": "L."}]}
    env.answers[raw["doi"]] = (raw, "cn:medra")
    make_pdf(env.dl / "a.pdf", [page(title_of("sup_markup_title"), raw["doi"])])
    row = env.run(execute=True)["rows"][0]
    assert row["action"] == "MOVED" and "sup" not in row["new_name"].lower()
    assert "<" not in row["title"] and row["title"].startswith("31P-MRS")
    sc = json.loads(lit_util.companion_path(env.lib / row["new_name"], ".fulltext.json").read_text(encoding="utf-8"))
    assert "<" not in sc["title"] and "<" not in sc["journal"]


def test_new_sidecar_shape_and_no_ligatures(env, monkeypatch):
    """The text layer carries U+FB00..U+FB06 (stubbed here, a real font below): none reaches a sidecar."""
    env.add("stroke_volume")
    make_pdf(env.dl / "a.pdf", [page(title_of("stroke_volume"), doi_of("stroke_volume"))])
    real = fitz.Page.get_text
    monkeypatch.setattr(fitz.Page, "get_text", lambda self, *a, **k: real(self, *a, **k) + " barore\ufb02ex \ufb01rst e\ufb00ect")
    row = env.run(execute=True)["rows"][0]
    sc = json.loads(lit_util.companion_path(env.lib / row["new_name"], ".fulltext.json").read_text(encoding="utf-8"))
    assert sc["has_pdf"] is True and sc["extracted_from_pdf"] is True and sc["text_cleaned"] is True
    assert sc["doi"] == doi_of("stroke_volume") and sc["identity"] == "OK" and sc["doc_kind"]
    assert "baroreflex" in sc["text"] and "first" in sc["text"]
    assert_no_ligatures(env.lib)


def _ligature_font():
    for f in (Path(os.environ.get("WINDIR", "C:/Windows")) / "Fonts" / "arial.ttf",
              Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
              Path("/Library/Fonts/Arial.ttf")):
        if f.exists():
            return str(f)
    return None


@pytest.mark.skipif(_ligature_font() is None, reason="no system TrueType font with ligature glyphs")
def test_real_pdf_with_ligatures_gives_a_clean_sidecar(env):
    env.add("stroke_volume")
    pdf = make_pdf(env.dl / "a.pdf", [page(title_of("stroke_volume"), doi_of("stroke_volume"),
                                           body=FILLER + " The barore\ufb02ex re\ufb02ex \ufb01rst e\ufb00ect.")],
                   fontfile=_ligature_font())
    with fitz.open(str(pdf)) as d:
        assert "\ufb02" in d[0].get_text()                     # the fixture really carries ligatures
    row = env.run(execute=True)["rows"][0]
    assert row["action"] == "MOVED"
    text = json.loads(lit_util.companion_path(env.lib / row["new_name"], ".fulltext.json")
                      .read_text(encoding="utf-8"))["text"]
    assert "baroreflex" in text
    assert_no_ligatures(env.lib)


def test_make_sidecar_cleans_raw_text():
    ident = ID.Ident("OK", "10.1234/x.1", meta("stroke_volume"), "crossref", None, "doi_article")
    rec = ID.make_sidecar("barore\ufb02ex \ufb03x", meta("sup_markup_title"), "crossref", "10.1234/x.1", "a.pdf",
                          ident, "VOR")
    assert rec["text"] == "baroreflex ffix" and "<" not in rec["title"]


def test_an_orphan_sidecar_of_the_same_doi_is_kept_and_gains_has_pdf(env):
    env.add("stroke_volume")
    name = ID.proposed_name(meta("stroke_volume"))[0]
    orphan = lit_util.companion_path(env.lib / name, ".fulltext.json")
    orphan.write_text(json.dumps({"doi": doi_of("stroke_volume"), "text": "old extraction",
                                  "extracted_from_pdf": True}), encoding="utf-8")
    make_pdf(env.dl / "a.pdf", [page(title_of("stroke_volume"), doi_of("stroke_volume"))])
    row = env.run(execute=True)["rows"][0]
    assert row["action"] == "MOVED" and row["new_name"] == name
    rec = json.loads(orphan.read_text(encoding="utf-8"))
    assert rec["has_pdf"] is True and rec["text"] == "old extraction"


# ---------------------------------------------------------------- the real ris_emit chain on litpipe.net
def test_real_net_chain_writes_the_ris_and_records_its_manifest(net_env, mock_server, monkeypatch, tmp_path):
    s = mock_server("127.0.0.1")
    for attr, path in (("CROSSREF_WORK", "/works/{doi}"), ("CROSSREF_SEARCH", "/works"),
                       ("DATACITE_WORK", "/dois/{doi}"), ("DOI_RA", "/doiRA/{doi}"), ("DOI_CN", "/cn/{doi}")):
        monkeypatch.setattr(R, attr, s.url(path))
    root = tmp_path / "root"
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    lib = root / "teaching_course" / "literature"
    lib.mkdir(parents=True)
    dl = tmp_path / "downloads"
    dl.mkdir()
    s.script("/works/10.1055/s-2003-39085", Reply(200, json.dumps({"message": RECORDS["sup_markup_title"]}), JSON))
    s.script("/works/10.1234/down.1", Reply(503))
    make_pdf(dl / "a.pdf", [page(title_of("sup_markup_title"), doi_of("sup_markup_title"))])
    make_pdf(dl / "b.pdf", [page("A paper whose metadata host is down", "10.1234/down.1")])
    res = ID.run(downloads=str(dl), lib_dir=str(lib), execute=True)
    rows = by_source(res)
    assert rows["b.pdf"]["action"] == "META_UNAVAILABLE" and (dl / "b.pdf").exists()
    a = rows["a.pdf"]
    assert a["action"] == "MOVED" and a["ris"] == "WROTE" and a["meta_source"] == "crossref"
    ris = (lib / a["new_name"]).with_suffix(".ris")
    assert "TI  - 31P-MRS Characterization" in ris.read_text(encoding="utf-8")
    assert R.ris_owner(str(ris)) == "pipeline"                      # DEC-29 manifest recorded
    text = Path(res["report"]).read_text(encoding="utf-8") + net_env.ledger_text()
    assert "tester@litpipe-test.org" not in Path(res["report"]).read_text(encoding="utf-8")
    assert "mailto:tester" not in text
    assert all(h.path.startswith(("/works/", "/dois/", "/doiRA/", "/cn/")) for h in s.hits)


# ---------------------------------------------------------------- module hygiene
def test_module_binds_no_email_user_agent_or_personal_path():
    assert not hasattr(ID, "EMAIL") and not hasattr(ID, "UA")
    assert not hasattr(ID, "LIB") and not hasattr(ID, "DOWNLOADS") and not hasattr(ID, "REPORT_DIR")
    src = (REPO / "import_downloads.py").read_text(encoding="utf-8")
    assert not re.search(r"[\w.+-]+@[\w-]+\.[A-Za-z]{2,}", src)            # no email literal
    assert not re.search(r"[A-Za-z]:[\\/]+Users[\\/]", src)                 # no home path literal
    assert "urllib.request" not in src and "requests." not in src           # HTTP only via litpipe.net


def test_module_imports_twice():
    importlib.reload(importlib.import_module("import_downloads"))


def test_help_works_from_a_foreign_cwd(tmp_path):
    p = subprocess.run([sys.executable, str(REPO / "import_downloads.py"), "--help"], cwd=tmp_path,
                       capture_output=True, text=True, timeout=120)
    assert p.returncode == 0, p.stderr
    for flag in ("--execute", "--dry-run", "--cutoff", "--downloads", "--lib-dir", "--project",
                 "--report-dir", "--doi"):
        assert flag in p.stdout
