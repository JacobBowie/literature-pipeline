"""W5-C3: import_downloads files only PDFs, writes its companions first, strips interlibrary-loan
cover pages, and never names a paper by a cover sheet's own DOI.

- N1: a file without `%PDF` in its first 1,024 bytes (an HTML landing page saved as `.pdf`) is
  NOT_PDF and never moved, even when it prints a resolvable DOI and its own title.
- Item 24 (decision C7): the sidecar and `.ris` are written before the PDF is moved; a failed write
  leaves the PDF where it was, removes what the import wrote, and reports ERR_WRITE.
- N2: with --execute the filed PDF loses its `ill` / `tandf` / `jstor` cover pages (by content), the
  untouched original goes to `<lib>/_archive/originals/<new name>`, and no cover text reaches the
  sidecar or the `.ris` (Lock B, a synthetic courier slip with a PATRON line). A cover sheet's own
  DOI is never taken unless its record's title is printed on the article pages (Lock A).
- C062: a same-day second run writes `_downloads_import_<date>.2.csv`, never over the first.
Every run is on a temp library and a temp Downloads folder."""
import csv
import hashlib
import json
from datetime import date

import pymupdf
import pytest

import import_downloads as ID
import lit_util
import ris_emit as R
from litpipe import net
from tests.test_import_downloads import (COVERS, FILLER, RECORDS, by_source, doi_of, env, make_pdf,  # noqa: F401
                                         meta, page, title_of)

# A synthetic courier slip (invented names and hosts; never a real one).
SLIP = ("Interlibrary Loan Request\n"
        "ILLiad TN: 900001\n"
        "PATRON: Quentin Exampleperson\n"
        "PATRON STATUS: Faculty\n"
        "ARTICLE TITLE: Heat Balance During Prolonged Marching in Hot Climates\n"
        "JOURNAL TITLE: Proceedings of the Example Conference\n"
        "YEAR: 1985\n"
        "LENDER: Example Research Library, illiad.example.edu\n"
        "NOTICE: This material may be protected by copyright law (Title 17 U.S. Code)")
SLIP_TEXT_MARKERS = ("Exampleperson", "PATRON", "Title 17", "example.edu", "ILLiad", "900001",
                     "protected by copyright law")
COVER_TEXT_MARKERS = ("Rapid #", "RapidX", "Title 17", "ILLiad", "Patron", "BORROWER", "LENDER")

CONF_TITLE = "Heat Balance During Prolonged Marching in Hot Climates"
CONF_DOI = "10.1234/conf.1985.5"
LAW_DOI = "10.1234/law.2012.77"
LAW_META = {"doi": LAW_DOI, "title": "Statutory Remedies for Infringement in Digital Library Lending", "year": "2012",
            "container": "Example Law Review", "authors": [{"family": "Lawton", "given": "P"}],
            "type": "journal-article"}
CONF_ITEM = {"DOI": CONF_DOI, "type": "proceedings-article", "title": [CONF_TITLE],
             "container-title": ["Proceedings of the Example Conference"],
             "published-print": {"date-parts": [[1985]]},
             "author": [{"given": "R.", "family": "Marlow", "sequence": "first"}]}


def sha(p):
    return hashlib.sha256(p.read_bytes()).hexdigest()


def pages_of(pdf):
    with pymupdf.open(str(pdf)) as d:
        return [pg.get_text() for pg in d]


def sidecar_of(lib, name):
    return lit_util.companion_path(lib / name, ".fulltext.json")


def html_landing(path, title, doi):
    path.write_bytes(("<!DOCTYPE html><html><head><title>" + title + "</title></head><body><h1>" + title +
                      "</h1><p>https://doi.org/" + doi + "</p><p>" + FILLER + "</p></body></html>")
                     .encode("utf-8"))
    return path


# ================================================================ N1: the %PDF gate
def test_an_html_page_with_a_resolvable_doi_and_its_title_is_never_moved(env):
    env.add("stroke_volume")
    html_landing(env.dl / "landing.pdf", title_of("stroke_volume"), doi_of("stroke_volume"))
    before = sha(env.dl / "landing.pdf")
    res = env.run(execute=True)
    row = res["rows"][0]
    assert row["action"] == "NOT_PDF" and row["doi"] == "" and row["new_name"] == ""
    assert (env.dl / "landing.pdf").exists() and sha(env.dl / "landing.pdf") == before
    assert not list(env.lib.iterdir())
    assert env.resolved == []                       # refused before any identity lookup
    assert "<!DOCTYPE" in row["detail"]


def test_the_magic_may_sit_after_leading_junk_within_1024_bytes(env):
    env.add("stroke_volume")
    pdf = make_pdf(env.tmp / "x.pdf", [page(title_of("stroke_volume"), doi_of("stroke_volume"))])
    (env.dl / "junk_first.pdf").write_bytes(b"\r\n" * 40 + pdf.read_bytes())
    assert env.run()["rows"][0]["action"] == "WOULD_MOVE"
    (env.dl / "junk_first.pdf").write_bytes(b" " * 1100 + pdf.read_bytes())
    assert env.run()["rows"][0]["action"] == "NOT_PDF"


def test_is_pdf_file():
    assert ID.PDF_MAGIC_WINDOW == 1024


# ================================================================ item 24: companions before the PDF
def _log_calls(monkeypatch, log):
    real_json, real_ris, real_move = lit_util.atomic_write_json, R.write_ris, ID.shutil.move

    def j(path, *a, **k):
        log.append(("sidecar", str(path)))
        return real_json(path, *a, **k)

    def r(path, *a, **k):
        log.append(("ris", str(path)))
        return real_ris(path, *a, **k)

    def m(src, dst, *a, **k):
        log.append(("move", str(dst)))
        return real_move(src, dst, *a, **k)
    monkeypatch.setattr(lit_util, "atomic_write_json", j)
    monkeypatch.setattr(R, "write_ris", r)
    monkeypatch.setattr(ID.shutil, "move", m)


def test_the_sidecar_and_ris_are_written_before_the_pdf_is_moved(env, monkeypatch):
    env.add("stroke_volume")
    make_pdf(env.dl / "a.pdf", [page(title_of("stroke_volume"), doi_of("stroke_volume"))])
    log = []
    _log_calls(monkeypatch, log)
    row = env.run(execute=True)["rows"][0]
    assert row["action"] == "MOVED" and row["ris"] == "WROTE"
    kinds = [k for k, p in log if not p.endswith(".csv")]
    assert kinds == ["sidecar", "ris", "move"], log


def _fail(*a, **k):
    raise OSError("disk full (injected)")


def test_a_failed_pdf_move_leaves_no_companion_and_the_pdf_in_downloads(env, monkeypatch):
    env.add("stroke_volume")
    make_pdf(env.dl / "a.pdf", [page(title_of("stroke_volume"), doi_of("stroke_volume"))])
    before = sha(env.dl / "a.pdf")
    monkeypatch.setattr(ID.shutil, "move", _fail)
    row = env.run(execute=True)["rows"][0]
    assert row["action"] == "ERR_WRITE" and row["outcome"] == "ERROR" and "disk full" in row["detail"]
    assert sha(env.dl / "a.pdf") == before
    assert not list(env.lib.iterdir())                                    # no orphan sidecar or .ris
    ris = env.lib / row["new_name"].replace(".pdf", ".ris")
    assert R._kv_get(R.RIS_NS, R.manifest_key(str(ris))) is None          # the manifest record too


def test_a_failed_sidecar_write_files_nothing(env, monkeypatch):
    env.add("stroke_volume")
    make_pdf(env.dl / "a.pdf", [page(title_of("stroke_volume"), doi_of("stroke_volume"))])
    monkeypatch.setattr(lit_util, "atomic_write_json", _fail)
    row = env.run(execute=True)["rows"][0]
    assert row["action"] == "ERR_WRITE" and "sidecar" in row["detail"]
    assert (env.dl / "a.pdf").exists() and not list(env.lib.iterdir())


def test_a_failed_ris_write_removes_the_new_sidecar(env, monkeypatch):
    env.add("stroke_volume")
    make_pdf(env.dl / "a.pdf", [page(title_of("stroke_volume"), doi_of("stroke_volume"))])
    monkeypatch.setattr(R, "write_ris", _fail)
    row = env.run(execute=True)["rows"][0]
    assert row["action"] == "ERR_WRITE" and ".ris" in row["detail"]
    assert (env.dl / "a.pdf").exists() and not list(env.lib.iterdir())


def test_a_failed_move_restores_a_text_only_sidecar_byte_for_byte(env, monkeypatch):
    env.add("stroke_volume")
    stem = "2005_Penrose_StrokeVolumeJats"
    sc = env.lib / f"{stem}.fulltext.json"
    sc.write_text(json.dumps({"doi": doi_of("stroke_volume"), "text": "JATS body text"}), encoding="utf-8")
    before = sc.read_bytes()
    make_pdf(env.dl / "a.pdf", [page(title_of("stroke_volume"), doi_of("stroke_volume"))])
    monkeypatch.setattr(ID.shutil, "move", _fail)
    row = env.run(execute=True)["rows"][0]
    assert row["action"] == "ERR_WRITE"
    assert sc.read_bytes() == before
    assert sorted(p.name for p in env.lib.iterdir()) == [sc.name]
    assert (env.dl / "a.pdf").exists()


def test_after_a_failure_the_next_run_imports_the_file(env, monkeypatch):
    env.add("stroke_volume")
    make_pdf(env.dl / "a.pdf", [page(title_of("stroke_volume"), doi_of("stroke_volume"))])
    with monkeypatch.context() as m:
        m.setattr(ID.shutil, "move", _fail)
        assert env.run(execute=True)["rows"][0]["action"] == "ERR_WRITE"
    row = env.run(execute=True)["rows"][0]
    assert row["action"] == "MOVED" and row["ris"] == "WROTE"
    assert (env.lib / row["new_name"]).exists()


# ================================================================ N2: cover pages
def _ill_pdf(env, name="ill.pdf", article_pages=2):
    env.add("autonomic")
    body = [page(title_of("autonomic"), doi_of("autonomic"))] + \
           [page("", "", header="Results", body=FILLER) for _ in range(article_pages - 1)]
    return make_pdf(env.dl / name, [COVERS["ill_cover_1"], COVERS["ill_cover_2"]] + body)


def test_a_two_page_ill_cover_is_stripped_and_the_original_archived(env):
    src = _ill_pdf(env, article_pages=2)
    n_before, orig = len(pages_of(src)), sha(src)
    res = env.run(execute=True)
    row = res["rows"][0]
    assert row["action"] == "MOVED" and row["covers_stripped"] == "2"
    filed = env.lib / row["new_name"]
    texts = pages_of(filed)
    assert len(texts) == n_before - 2
    assert not any(m in t for t in texts for m in COVER_TEXT_MARKERS)
    assert title_of("autonomic").split()[0] in texts[0]
    archived = env.lib / "_archive" / "originals" / row["new_name"]
    assert row["archived_original"] == str(archived)
    assert sha(archived) == orig and len(pages_of(archived)) == n_before
    assert not src.exists()
    sc = json.loads(sidecar_of(env.lib, row["new_name"]).read_text(encoding="utf-8"))
    assert not any(m in sc["text"] for m in COVER_TEXT_MARKERS)
    assert "Results" in sc["text"]
    assert not list(env.lib.glob("*.tmp"))


def test_dry_run_reports_the_strip_and_writes_nothing(env):
    _ill_pdf(env)
    row = env.run()["rows"][0]
    assert row["action"] == "WOULD_MOVE" and row["covers_stripped"] == "2"
    assert "would strip 2 cover page(s) (ill)" in row["note"]
    assert not list(env.lib.iterdir())


def test_keep_covers_files_the_pdf_whole_and_still_keeps_cover_text_out_of_the_sidecar(env):
    src = _ill_pdf(env)
    orig = sha(src)
    row = env.run(execute=True, keep_covers=True)["rows"][0]
    assert row["action"] == "MOVED" and row["covers_stripped"] == "" and row["archived_original"] == ""
    filed = env.lib / row["new_name"]
    assert sha(filed) == orig
    assert not (env.lib / "_archive").exists()
    sc = json.loads(sidecar_of(env.lib, row["new_name"]).read_text(encoding="utf-8"))
    assert not any(m in sc["text"] for m in COVER_TEXT_MARKERS)
    assert "--keep-covers" in row["note"]


def test_keep_covers_on_the_cli(env):
    src = _ill_pdf(env)
    orig = sha(src)
    assert ID.main(["--lib-dir", str(env.lib), "--downloads", str(env.dl), "--execute", "--keep-covers"]) == 0
    filed = next(env.lib.glob("*.pdf"))
    assert sha(filed) == orig


def test_covers_are_found_by_content_not_by_position(env):
    """An ILL slip in the middle is stripped; the pages around it stay, in order."""
    env.add("autonomic")
    src = make_pdf(env.dl / "mid.pdf", [page(title_of("autonomic"), doi_of("autonomic")), SLIP,
                                        page("", "", header="Discussion", body=FILLER)])
    row = env.run(execute=True)["rows"][0]
    assert row["action"] == "MOVED" and row["covers_stripped"] == "1"
    texts = pages_of(env.lib / row["new_name"])
    assert len(texts) == 2 and "Autonomic" in texts[0] and "Discussion" in texts[1]
    assert not src.exists()


@pytest.mark.parametrize("cover, tag", [("tandf_cover", "tandf"), ("jstor_cover", "jstor")])
def test_publisher_download_notices_are_stripped(env, cover, tag):
    env.add("shuttle_run")
    make_pdf(env.dl / "p.pdf", [COVERS[cover], page(title_of("shuttle_run"), doi_of("shuttle_run")),
                                page("", "", header="Methods", body=FILLER)])
    row = env.run(execute=True)["rows"][0]
    assert row["action"] == "MOVED" and row["covers_stripped"] == "1" and f"({tag})" in row["note"]
    assert len(pages_of(env.lib / row["new_name"])) == 2


def test_a_copyright_notice_page_is_not_stripped(env):
    env.add("stroke_volume")
    make_pdf(env.dl / "e.pdf", [page(title_of("stroke_volume"), doi_of("stroke_volume")),
                                page("", "", header="Results", body=FILLER), COVERS["ebsco_notice"]])
    row = env.run(execute=True)["rows"][0]
    assert row["action"] == "MOVED" and row["covers_stripped"] == ""
    assert len(pages_of(env.lib / row["new_name"])) == 3
    assert not (env.lib / "_archive").exists()


def test_nothing_is_stripped_when_every_scanned_page_looks_like_a_cover(env):
    src = make_pdf(env.dl / "allcover.pdf", [COVERS["ill_cover_1"], COVERS["ill_cover_2"]])
    scan = ID.scan_pdf(src)
    assert set(scan.covers.values()) == {"ill"} and scan.strip_pages() == []
    env.add("autonomic")
    row = env.run(execute=True, doi=doi_of("autonomic"))["rows"][0]
    assert row["action"] == "MOVED" and row["covers_stripped"] == ""
    assert len(pages_of(env.lib / row["new_name"])) == 2


def test_nothing_is_stripped_when_the_scanned_pages_are_all_covers_and_more_pages_follow(env, monkeypatch):
    """Every page the scan read looked like a cover, though the document goes on past the scan:
    the covers are not trusted, nothing is stripped."""
    monkeypatch.setattr(ID, "SCAN_PAGES", 2)
    src = make_pdf(env.dl / "long.pdf", [COVERS["ill_cover_1"], COVERS["ill_cover_2"],
                                         page(title_of("autonomic"), doi_of("autonomic"))])
    assert ID.scan_pdf(src).strip_pages() == []
    env.add("autonomic")
    row = env.run(execute=True, doi=doi_of("autonomic"))["rows"][0]
    assert row["action"] == "MOVED" and row["covers_stripped"] == ""
    assert len(pages_of(env.lib / row["new_name"])) == 3


def test_a_failed_stripped_write_returns_the_original_and_removes_everything(env, monkeypatch):
    src = _ill_pdf(env)
    orig = sha(src)
    real_replace = ID.os.replace

    def replace(a, b):
        if str(a).endswith(".importing.tmp"):
            raise OSError("locked (injected)")
        return real_replace(a, b)
    monkeypatch.setattr(ID.os, "replace", replace)
    row = env.run(execute=True)["rows"][0]
    assert row["action"] == "ERR_WRITE" and "locked" in row["detail"]
    assert src.exists() and sha(src) == orig
    assert not list(env.lib.glob("*.*")) and not list((env.lib / "_archive" / "originals").iterdir())


def test_an_archived_original_never_overwrites_an_earlier_one(env):
    src = _ill_pdf(env)
    name = ID.proposed_name(meta("autonomic"))[0]
    (env.lib / "_archive" / "originals").mkdir(parents=True)
    old = env.lib / "_archive" / "originals" / name
    old.write_bytes(b"%PDF-1.4 an earlier original")
    row = env.run(execute=True)["rows"][0]
    assert row["action"] == "MOVED" and old.read_bytes() == b"%PDF-1.4 an earlier original"
    assert row["archived_original"].endswith(name[:-4] + ".2.pdf")
    assert not src.exists()


def test_spaces_and_non_ascii_names_in_downloads_and_library(env):
    lib = env.tmp / "Bibliothèque de test" / "lit érature"
    lib.mkdir(parents=True)
    dl = env.tmp / "Mes Téléchargements"
    dl.mkdir()
    env.add("autonomic", "stroke_volume")
    make_pdf(dl / "Prêt entre bibliothèques (1).pdf", [COVERS["ill_cover_1"], COVERS["ill_cover_2"],
                                                       page(title_of("autonomic"), doi_of("autonomic"))])
    make_pdf(dl / "Ünïcode paper ß.pdf", [page(title_of("stroke_volume"), doi_of("stroke_volume"))])
    res = ID.run(downloads=str(dl), lib_dir=str(lib), execute=True)
    rows = by_source(res)
    a, b = rows["Prêt entre bibliothèques (1).pdf"], rows["Ünïcode paper ß.pdf"]
    assert a["action"] == b["action"] == "MOVED"
    assert len(pages_of(lib / a["new_name"])) == 1
    assert (lib / "_archive" / "originals" / a["new_name"]).exists()
    assert (lib / b["new_name"]).exists() and not list(dl.iterdir())


# ---------------------------------------------------------------- Lock B: no cover text anywhere
def test_lock_b_no_slip_text_in_the_sidecar_any_field_or_the_ris(env):
    env.answers[CONF_DOI] = (R.crossref_meta(CONF_ITEM), "crossref")
    make_pdf(env.dl / "slip.pdf", [SLIP, page(CONF_TITLE, CONF_DOI, header="Proceedings of the Example "
                                              "Conference 1985, pp. 12-20")])
    row = env.run(execute=True)["rows"][0]
    assert row["action"] == "MOVED" and row["doi"] == CONF_DOI and row["covers_stripped"] == "1"
    sc_raw = sidecar_of(env.lib, row["new_name"]).read_text(encoding="utf-8")
    ris_raw = (env.lib / row["new_name"]).with_suffix(".ris").read_text(encoding="utf-8")
    for m in SLIP_TEXT_MARKERS:
        assert m not in sc_raw, m                 # text, identity evidence and every other field
        assert m not in ris_raw, m                # every line, an N1 note included
    assert not any(m in t for t in pages_of(env.lib / row["new_name"]) for m in SLIP_TEXT_MARKERS)


# ---------------------------------------------------------------- Lock A: a cover sheet's own DOI
def _slip_with_unrelated_doi(env):
    """An ILL cover printing an unrelated, resolvable DOI, and a pre-DOI article with no DOI."""
    env.answers[LAW_DOI] = (LAW_META, "crossref")
    cover = SLIP + f"\nCITED DOI: https://doi.org/{LAW_DOI}"
    make_pdf(env.dl / "pre_doi.pdf", [cover, page(CONF_TITLE, "", header="Proceedings of the Example "
                                                  "Conference 1985, pp. 12-20")])


def test_lock_a_a_cover_doi_whose_title_is_not_printed_is_never_taken(env):
    _slip_with_unrelated_doi(env)
    res = env.run(execute=True)
    row = res["rows"][0]
    assert LAW_DOI in env.resolved                       # it was looked up ...
    assert row["action"] == "UNKNOWN_LEAVE" and row["doi"] == ""   # ... and never taken
    assert "cover-sheet DOI" in row["detail"]
    assert (env.dl / "pre_doi.pdf").exists() and not list(env.lib.iterdir())


def test_lock_a_the_title_search_finds_the_articles_own_record(env):
    _slip_with_unrelated_doi(env)
    env.search.results = [CONF_ITEM]
    row = env.run()["rows"][0]
    assert row["action"] == "WOULD_MOVE" and row["doi"] == CONF_DOI and row["doi"] != LAW_DOI
    assert row["meta_source"] == "crossref_search" and row["new_name"].startswith("1985_Marlow_")


def test_lock_a_holds_when_the_cover_doi_is_the_only_doi_and_its_title_is_absent(env):
    """Even as the only DOI in the file, and with the article's text unusable for a search."""
    env.answers[LAW_DOI] = (LAW_META, "crossref")
    make_pdf(env.dl / "only.pdf", [SLIP + f"\nDOI: {LAW_DOI}", page("", "", header="", body=FILLER)])
    row = env.run()["rows"][0]
    assert row["action"] in ("UNKNOWN_LEAVE",) and row["doi"] == ""


# ================================================================ C062: run-id report names
def test_a_same_day_second_execute_never_overwrites_the_first_report(env):
    env.add("stroke_volume", "autonomic")
    make_pdf(env.dl / "a.pdf", [page(title_of("stroke_volume"), doi_of("stroke_volume"))])
    first = env.run(execute=True)
    stamp = f"{date.today():%Y-%m-%d}"
    assert first["report"].endswith(f"_downloads_import_{stamp}.csv")
    first_bytes = open(first["report"], "rb").read()
    make_pdf(env.dl / "b.pdf", [page(title_of("autonomic"), doi_of("autonomic"))])
    second = env.run(execute=True)
    assert second["report"].endswith(f"_downloads_import_{stamp}.2.csv")
    assert open(first["report"], "rb").read() == first_bytes
    third = env.run(execute=True)
    assert third["report"].endswith(f"_downloads_import_{stamp}.3.csv")
    rows = list(csv.DictReader(open(second["report"], encoding="utf-8")))
    assert [r["source"] for r in rows] == ["b.pdf"]


def test_dry_runs_number_their_own_reports(env):
    a = env.run()
    b = env.run()
    stamp = f"{date.today():%Y-%m-%d}"
    assert a["report"].endswith(f"_{stamp}_DRYRUN.csv") and b["report"].endswith(f"_{stamp}.2_DRYRUN.csv")


def test_the_report_keeps_its_columns_and_adds_two_at_the_end():
    assert ID.REPORT_FIELDS[:17] == ["source", "size_kb", "action", "doi", "year", "first_au", "title",
                                     "new_name", "dup_match", "note", "identity", "doc_kind", "meta_source",
                                     "held_elsewhere", "ris", "outcome", "detail"]
    assert ID.REPORT_FIELDS[17:] == ["covers_stripped", "archived_original"]


def test_module_imports_pymupdf_not_fitz():
    src = open(ID.__file__, encoding="utf-8").read()
    assert "import fitz" not in src and "import pymupdf" in src


def test_net_state_untouched_by_a_dry_run_with_covers(env):
    _ill_pdf(env)
    before = dict(net.STATE.kv)
    env.run()
    assert net.STATE.kv == before
