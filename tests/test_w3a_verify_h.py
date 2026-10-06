"""W3a verifier H: lock-ins for import_downloads (W3-D1) and the rename / DOI-fill tools (W3-D2).

import_downloads runs in-process through the REAL ris_emit.resolve_meta and litpipe.net against a
loopback MockServer, with the DEC-29 manifest in a real litpipe.state file under tmp_path."""
import csv
import hashlib
import json
import os
import sys
import textwrap
from datetime import date
from pathlib import Path

import fitz
import pytest

import audit_filenames as AF
import fill_missing_dois as FMD
import import_downloads as ID
import lit_util
import ris_emit as R
from litpipe import config, holdings
from litpipe import doi as _doi
from tests.netmock import Reply

LIGATURES = tuple(chr(c) for c in range(0xFB00, 0xFB07))
JSON_H = {"Content-Type": "application/json"}
FONT = Path(os.environ.get("WINDIR", "C:/Windows")) / "Fonts" / "arial.ttf"
BODY = ("Participants completed a graded exercise test on a cycle ergometer while heart rate, blood "
        "pressure and oxygen uptake were recorded, and the results were analysed with linear mixed "
        "models adjusted for age, sex and training status. ") * 3
TODAY = date(2026, 10, 5)


def rec(doi, title, family="Writer", given="Alex", year=2020, typ="journal-article"):
    return {"DOI": doi, "title": [title], "type": typ, "container-title": ["Journal of Example Physiology"],
            "author": [{"family": family, "given": given, "sequence": "first"}],
            "published-print": {"date-parts": [[year]]}}


def make_pdf(path, pages, font=None):
    d = fitz.open()
    for text in pages:
        p = d.new_page()
        lines = []
        for para in text.split("\n"):
            lines.extend(textwrap.wrap(para, 95) or [""])
        kw = {}
        if font:
            p.insert_font(fontname="F0", fontfile=str(font))
            kw = {"fontname": "F0"}
        p.insert_text((40, 50), "\n".join(lines), fontsize=8, **kw)
    d.save(str(path))
    d.close()
    return Path(path)


def article(title, doi="", extra="", authors="Alex Writer, Blair Writer"):
    head = "Journal of Example Physiology 2020; 12: 1-10"
    return "\n".join(x for x in (head, f"https://doi.org/{doi}" if doi else "", title, authors,
                                 "Abstract. " + BODY, extra) if x)


def tree(*roots):
    out = {}
    for r in roots:
        for p in Path(r).rglob("*"):
            if p.is_file():
                out[str(p)] = hashlib.sha256(p.read_bytes()).hexdigest()
    return out


@pytest.fixture
def lab(tmp_path, monkeypatch, net_env, mock_server):
    """A temp root and registry, one teaching library, a temp Downloads, a real litpipe.state file
    for ris_emit (the DEC-29 manifest), and Crossref / DataCite / doi.org routed to a MockServer."""
    import litpipe.state as real
    monkeypatch.setattr(real, "DB_PATH", tmp_path / "state" / "litpipe_state.sqlite")
    monkeypatch.setattr(R, "STATE", real)
    for h in ("export.arxiv.org", "arxiv.org", "www.arxiv.org"):
        real.refuse(h, "manual: refused since 2026-09-25", persistence="manual")
    root = tmp_path / "root"
    lib = root / "teaching_course" / "literature"
    lib.mkdir(parents=True)
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    reg = json.loads(Path(config.CONFIG_PATH).read_text(encoding="utf-8")) if Path(config.CONFIG_PATH).exists() else {}
    reg["state_dir"] = str(tmp_path / "state")
    reg["projects"] = {"teaching_course": {"lib_dir": "literature"},
                       "teaching_course/unit2": {"parent": "teaching_course", "lib_dir": "unit2/literature"}}
    Path(config.CONFIG_PATH).write_text(json.dumps(reg), encoding="utf-8")
    (root / "teaching_course" / "unit2" / "literature").mkdir(parents=True)
    dl = tmp_path / "downloads"
    dl.mkdir()
    rep = tmp_path / "reports"
    rep.mkdir()
    srv = mock_server()
    monkeypatch.setattr(R, "CROSSREF_WORK", srv.url("/works/") + "{doi}")
    monkeypatch.setattr(R, "CROSSREF_SEARCH", srv.url("/works"))
    monkeypatch.setattr(R, "DATACITE_WORK", srv.url("/dc/") + "{doi}")
    monkeypatch.setattr(R, "DOI_RA", srv.url("/ra/") + "{doi}")
    monkeypatch.setattr(R, "DOI_CN", srv.url("/cn/") + "{doi}")
    srv.script("/works", Reply(200, json.dumps({"message": {"items": []}}), JSON_H))

    class Lab:
        pass
    L = Lab()
    L.root, L.lib, L.dl, L.rep, L.srv, L.state, L.tmp = root, lib, dl, rep, srv, real, tmp_path

    def crossref(*records, missing=()):
        for r in records:
            srv.script("/works/" + _doi.encode_path(r["DOI"]), Reply(200, json.dumps({"message": r}), JSON_H))
        for d in missing:
            enc = _doi.encode_path(d)
            srv.script("/works/" + enc, Reply(404, "not found"))
            srv.script("/dc/" + enc, Reply(404, "not found"))
            srv.script("/ra/" + enc, Reply(200, json.dumps([{"DOI": d, "RA": "Crossref"}]), JSON_H))
    L.crossref = crossref
    return L


def _report(path):
    with open(path, encoding="utf-8", newline="") as f:
        return {r["source"]: r for r in csv.DictReader(f)}


# ---------------------------------------------------------------------------- import_downloads e2e
T = {
    "a": "Plasma volume expansion after ten days of heat acclimation in trained endurance cyclists",
    "b": "Sweat sodium concentration across repeated exercise heat stress trials in young adults",
    "c": "Core temperature responses to firefighter protective clothing during treadmill walking",
    "d": "Cutaneous vasodilation and sweating onset thresholds in older women exposed to heat",
    "e": "Cardiovascular drift during prolonged cycling in hot and humid laboratory conditions",
    "f": "Heat shock protein expression in skeletal muscle after repeated passive heating sessions",
    "g": "Thermal sensation and comfort votes during intermittent sprint exercise in the heat",
    "j": "Hydration status and cognitive performance in adolescent athletes during summer camps",
    "k": "Electrolyte replacement strategies during ultra endurance events in tropical climates",
}
H_TITLE_REC = "<sup>31</sup>P magnetic resonance spectroscopy of muscle e\ufb03ciency during exercise in the heat"
H_TITLE_PDF = "31P magnetic resonance spectroscopy of muscle efficiency during exercise in the heat"
ILL1 = ("NOTICE: This material may be protected by copyright law (Title 17 U.S. Code)\nILLiad TN: 1234567\n"
        "Lending String: *ABC,DEF\nPatron: Reader, A\nCall #: QP1 .J6\nMax Cost: 0.00 USD")
ILL2 = "Borrower: XYZ\nTransaction Date: 10/1/2026\nDocument Delivery Service\nOdyssey: 10.0.0.1"
TANDF = ("This article was downloaded by: [Example University]\nOn: 01 October 2026\nPublisher: Taylor & "
         "Francis\nPlease scroll down for article\nTo cite this article: Writer A (2020), DOI: 10.5555/d4")
EBSCO = ("Copyright of Journal of Example Physiology is the property of Example Publisher and its content "
         "may not be copied or emailed to multiple sites or posted to a listserv without the copyright "
         "holder's express written permission. However, users may print, download, or email articles for "
         "individual use.")


def _build_scenario(L):
    recs = [rec("10.5555/a1", T["a"], "Ames"), rec("10.5555/b2", T["b"], "Baird"),
            rec("10.5555/c3", T["c"], "Cole"), rec("10.5555/d4", T["d"], "Dunn"),
            rec("10.5555/e5", T["e"], "Eves"), rec("10.5555/f6", T["f"], "Ford"),
            rec("10.5555/g7", T["g"], "Gray"), rec("10.5555/j10", T["j"], "Jory"),
            rec("10.5555/k11", T["k"], "King"),
            rec("10.5555/h8", H_TITLE_REC, "O\ufb03cer", "Sam")]
    L.crossref(*recs)
    dl = L.dl
    make_pdf(dl / "a.pdf", [article(T["a"], "10.5555/a1")])
    make_pdf(dl / "b_ill.pdf", [ILL1, article(T["b"], "10.5555/b2")])
    make_pdf(dl / "c_ill2.pdf", [ILL1, ILL2, article(T["c"], "10.5555/c3")])
    make_pdf(dl / "d_tandf.pdf", [TANDF, article(T["d"], "10.5555/d4")])
    make_pdf(dl / "e_ebsco.pdf", [article(T["e"], "10.5555/e5", extra=EBSCO)])
    make_pdf(dl / "f_supp.pdf", ["Supplementary Material\n" + f"Supplementary data for {T['f']}\n"
                                 "https://doi.org/10.5555/f6\nTable S1. Participant characteristics. " + BODY])
    make_pdf(dl / "x.pdf", [article(T["g"], "10.5555/g7")])
    make_pdf(dl / "x (1).pdf", [article(T["g"], "10.5555/g7")])
    font = FONT if FONT.exists() else None
    h_text = article(H_TITLE_PDF, "10.5555/h8", extra="The e\ufb03ciency of o\ufb00-transient \ufb01ts.") \
        if font else article(H_TITLE_PDF, "10.5555/h8")
    make_pdf(dl / "h_lig.pdf", [h_text], font=font)
    (dl / "i_corrupt.pdf").write_bytes(b"%PDF-1.4\n%garbage, not a pdf body\n")
    make_pdf(dl / "j_textonly.pdf", [article(T["j"], "10.5555/j10")])
    make_pdf(dl / "k_dup.pdf", [article(T["k"], "10.5555/k11")])
    # The library: a text-only holding of j10 (pmc shape, has_pdf false) and a PDF holding of k11.
    lib = L.lib
    (lib / "2019_Jory_HydrationStatus.fulltext.json").write_text(json.dumps(
        {"doi": "10.5555/j10", "title": T["j"], "text": "JATS body text " * 50, "has_pdf": False}), encoding="utf-8")
    (lib / "2019_Jory_HydrationStatus.ris").write_text("TY  - JOUR\nTI  - curated\nDO  - 10.5555/j10\nER  - \n",
                                                     encoding="utf-8")
    make_pdf(lib / "2018_King_Electrolyte.pdf", [article(T["k"], "10.5555/k11")])
    (lib / "2018_King_Electrolyte.ris").write_text("TY  - JOUR\nTI  - curated k\nDO  - 10.5555/k11\nER  - \n",
                                                 encoding="utf-8")


def _kv(L):
    return {ns: dict(L.state.kv_items(ns)) for ns in ("ris", "doi_ra")}


def test_import_e2e_dry_run_writes_only_its_report_then_execute_places_every_file(lab):
    L = lab
    _build_scenario(L)
    before, kv0 = tree(L.lib, L.dl), _kv(L)
    res = ID.run(execute=False, cutoff="2000-01-01", downloads=str(L.dl), lib_dir=str(L.lib),
                 report_dir=str(L.rep), today=TODAY)
    assert tree(L.lib, L.dl) == before and _kv(L) == kv0
    assert sorted(os.listdir(L.rep)) == ["_downloads_import_2026-10-05_DRYRUN.csv"]
    rows = {r["source"]: r for r in res["rows"]}
    act = {k: v["action"] for k, v in rows.items()}
    assert act["a.pdf"] == act["b_ill.pdf"] == act["c_ill2.pdf"] == act["d_tandf.pdf"] == "WOULD_MOVE"
    assert act["e_ebsco.pdf"] == act["h_lig.pdf"] == act["j_textonly.pdf"] == "WOULD_MOVE"
    assert act["f_supp.pdf"] == "IDENTITY_FLAG"
    assert act["i_corrupt.pdf"] == "ERR_READ"
    assert act["k_dup.pdf"] == "DUP_DOI"
    assert sorted([act["x.pdf"], act["x (1).pdf"]]) == ["DUP_IN_RUN", "WOULD_MOVE"]
    assert rows["j_textonly.pdf"]["new_name"] == "2019_Jory_HydrationStatus.pdf"
    assert rows["b_ill.pdf"]["doi"] == "10.5555/b2" and rows["c_ill2.pdf"]["doi"] == "10.5555/c3"

    res = ID.run(execute=True, cutoff="2000-01-01", downloads=str(L.dl), lib_dir=str(L.lib),
                 report_dir=str(L.rep), today=TODAY)
    rows = {r["source"]: r for r in res["rows"]}
    left = sorted(os.listdir(L.dl))
    assert {"f_supp.pdf", "i_corrupt.pdf", "k_dup.pdf"} <= set(left)
    assert len({"x.pdf", "x (1).pdf"} & set(left)) == 1
    for src in ("a.pdf", "b_ill.pdf", "c_ill2.pdf", "d_tandf.pdf", "e_ebsco.pdf", "h_lig.pdf"):
        r = rows[src]
        assert r["action"] == "MOVED" and r["ris"] == "WROTE", (src, r)
        pdf = L.lib / r["new_name"]
        ris = lit_util.companion_path(pdf, ".ris")
        assert pdf.exists() and ris.exists()
        assert R.ris_owner(str(ris)) == "pipeline"            # recorded in the real manifest
        sc = json.loads(lit_util.companion_path(pdf, ".fulltext.json").read_text(encoding="utf-8"))
        assert sc["has_pdf"] is True and sc["extracted_from_pdf"] is True and sc["doi"] == r["doi"]
    # curated .ris files untouched
    assert "curated k" in (L.lib / "2018_King_Electrolyte.ris").read_text(encoding="utf-8")
    assert "curated" in (L.lib / "2019_Jory_HydrationStatus.ris").read_text(encoding="utf-8")
    assert rows["j_textonly.pdf"]["ris"] == "EXISTS"
    # the text-only holding took its PDF and is now a PDF holding
    assert (L.lib / "2019_Jory_HydrationStatus.pdf").exists()
    hm = holdings.build({"state_dir": str(L.tmp / "state"), "projects": {"t": {"lib_dir": "literature",
                                                                            "parent": "teaching_course"}}},
                        use_cache=False, write_cache=False)
    assert [h.kind for h in hm.records("10.5555/j10")] == [holdings.PDF]
    # no markup or ligature in any name, report cell or sidecar
    for p in list(L.lib.iterdir()) + list(L.rep.iterdir()):
        assert "<" not in p.name and "sup" not in p.name.lower() and not any(c in p.name for c in LIGATURES)
        if p.suffix in (".json", ".csv", ".ris"):
            raw = p.read_text(encoding="utf-8")
            assert not any(c in raw for c in LIGATURES), p.name
            assert "<sup>" not in raw, p.name
    assert "Officer" in rows["h_lig.pdf"]["new_name"] or "Officer" in rows["h_lig.pdf"]["first_au"]
    # nothing reached arXiv
    assert not [h for h in L.srv.hits if "arxiv" in h.path]


def test_documented_consumer_form_without_report_dir_and_a_subproject_key(lab, monkeypatch):
    L = lab
    L.crossref(rec("10.5555/a1", T["a"], "Ames"))
    home = L.tmp / "home"
    (home / "Downloads").mkdir(parents=True)
    make_pdf(home / "Downloads" / "a.pdf", [article(T["a"], "10.5555/a1")])
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: home))
    assert ID.main(["--lib-dir", str(L.lib), "--cutoff", "2000-01-01", "--execute"]) == 0
    assert (L.lib.parent / "_downloads_import_{}.csv".format(date.today().isoformat())).exists()
    lib, report = ID.resolve_library(project="teaching_course/unit2")
    assert lib == L.root / "teaching_course" / "unit2" / "literature"
    assert report == L.root / "teaching_course" / "unit2"


@pytest.mark.skipif(os.name != "nt", reason="Windows drive-letter and UNC joins")
@pytest.mark.parametrize("lib", [r"D:\libs\teaching\literature", r"\\server\share\dir\literature",
                                 r"\\server\share\literature", "D:\\literature"])
def test_unregistered_destination_resolves_to_itself(lab, lib):
    reg = ID._registry_with_destination(None, lib)
    got = {os.path.normcase(str(p)) for _k, p, _a in holdings.libraries(reg)}
    assert os.path.normcase(os.path.abspath(lib)) in got


# ---------------------------------------------------------------------------- import_downloads defects
def test_an_email_after_a_line_break_does_not_break_the_sidecar():
    """H1: redact() over the JSON text matched 'n' of an escaped newline into the address and left an
    invalid '\\R' escape; json.loads raised and --execute aborted without a report."""
    ident = ID.Ident("OK", "10.5555/a1", {}, "crossref", None, "doi_article", "")
    for txt in ("Correspondence\nj.smith@univ.example.edu\nBody", "Tel\tj.smith@univ.example.edu"):
        rec_ = ID.make_sidecar(txt, {"title": "T", "authors": []}, "crossref", "10.5555/a1", "a.pdf", ident, "VOR")
        assert rec_["doi"] == "10.5555/a1" and "Body" in rec_["text"] or "Tel" in rec_["text"]


def test_execute_survives_an_email_at_the_start_of_a_line(lab):
    L = lab
    L.crossref(rec("10.5555/a1", T["a"], "Ames"))
    make_pdf(L.dl / "a.pdf", [article(T["a"], "10.5555/a1", extra="Correspondence:\nalex.writer@univ.example.edu")])
    res = ID.run(execute=True, cutoff="2000-01-01", downloads=str(L.dl), lib_dir=str(L.lib),
                 report_dir=str(L.rep), today=TODAY)
    assert res["rows"][0]["action"] == "MOVED"
    assert Path(res["report"]).exists()


def test_a_reference_list_doi_is_never_taken_as_the_article(lab):
    """H2: the article prints no DOI of its own; page 2 cites another paper with its title and DOI.
    That cited DOI must not name the file."""
    L = lab
    cited = "Effects of heat acclimation on plasma volume in trained cyclists and runners"
    L.crossref(rec("10.5555/cited1", cited, "Smith"))
    make_pdf(L.dl / "letter.pdf", [article(T["a"]),
                                   "Discussion. " + BODY + "\nReferences\n1. Smith J, Jones K. " + cited +
                                   ". J Appl Physiol. 2010;108:1-10. doi:10.5555/cited1"])
    res = ID.run(execute=False, cutoff="2000-01-01", downloads=str(L.dl), lib_dir=str(L.lib),
                 report_dir=str(L.rep), today=TODAY)
    row = res["rows"][0]
    assert not (row["action"] == "WOULD_MOVE" and row["doi"] == "10.5555/cited1"), row


def test_a_short_search_title_found_in_the_text_is_not_an_identity(lab):
    """H3: the bibliographic search returns a record whose title is two words that the article
    happens to contain; a non-specific title is not evidence of identity."""
    L = lab
    L.srv.script("/works", Reply(200, json.dumps({"message": {"items": [
        rec("10.5555/short", "Heat stroke", "Other", "Pat", 1999)]}}), JSON_H))
    make_pdf(L.dl / "case.pdf", [article("Exertional collapse in a marathon runner: a case of heat stroke "
                                         "treated with cold water immersion")])
    res = ID.run(execute=False, cutoff="2000-01-01", downloads=str(L.dl), lib_dir=str(L.lib),
                 report_dir=str(L.rep), today=TODAY)
    row = res["rows"][0]
    assert not (row["action"] == "WOULD_MOVE" and row["doi"] == "10.5555/short"), row


# ---------------------------------------------------------------------------- cascade_rename
def _pipeline_ris(path, text="TY  - JOUR\nTI  - one\nDO  - 10.5555/r1\nER  - \n"):
    assert R.write_ris(str(path), text) is True


def test_cascade_rename_carries_every_companion_and_the_manifest(lab):
    lib = lab.lib
    make_pdf(lib / "Unknown_Ames_Plasma.pdf", ["x"])
    (lib / "Unknown_Ames_Plasma.fulltext.json").write_text(json.dumps({"doi": "10.5555/r1", "text": "t"}), encoding="utf-8")
    (lib / "Unknown_Ames_Plasma.identity.json").write_text(json.dumps({"pdf": "Unknown_Ames_Plasma.pdf",
                                                                       "identity": "OK"}), encoding="utf-8")
    (lib / "Unknown_Ames_Plasma.xml").write_text("<x/>", encoding="utf-8")
    _pipeline_ris(lib / "Unknown_Ames_Plasma.ris")
    old_key = R.manifest_key(lib / "Unknown_Ames_Plasma.ris")
    AF.cascade_rename(str(lib), "Unknown_Ames_Plasma.pdf", "2020_Ames_Plasma.pdf")
    assert sorted(os.listdir(lib)) == sorted(["2020_Ames_Plasma" + e for e in
                                              (".pdf", ".fulltext.json", ".identity.json", ".xml", ".ris")])
    assert json.loads((lib / "2020_Ames_Plasma.identity.json").read_text(encoding="utf-8"))["pdf"] == "2020_Ames_Plasma.pdf"
    assert R._kv_get(R.RIS_NS, old_key) is None
    assert R.ris_owner(str(lib / "2020_Ames_Plasma.ris")) == "pipeline"
    assert R.write_ris(str(lib / "2020_Ames_Plasma.ris"), "TY  - JOUR\nTI  - two\nER  - \n", overwrite=True) is True


def test_cascade_rename_refuses_an_orphan_companion_at_the_new_stem(lab):
    """H4: the old file has no .ris; another paper's orphan .ris sits at the new stem. Renaming would
    pair this PDF with that record (holdings then reads the orphan's DOI as this file's)."""
    lib = lab.lib
    make_pdf(lib / "Unknown_Ames_Plasma.pdf", ["x"])
    (lib / "2020_Ames_Plasma.ris").write_text("TY  - JOUR\nDO  - 10.5555/other\nER  - \n", encoding="utf-8")
    with pytest.raises(FileExistsError):
        AF.cascade_rename(str(lib), "Unknown_Ames_Plasma.pdf", "2020_Ames_Plasma.pdf")
    assert (lib / "Unknown_Ames_Plasma.pdf").exists() and not (lib / "2020_Ames_Plasma.pdf").exists()


def test_case_only_rename_works_and_a_case_twin_of_another_file_is_refused(lab):
    lib = lab.lib
    make_pdf(lib / "2020_ames_Plasma.pdf", ["x"])
    _pipeline_ris(lib / "2020_ames_Plasma.ris")
    AF.cascade_rename(str(lib), "2020_ames_Plasma.pdf", "2020_Ames_Plasma.pdf")
    assert "2020_Ames_Plasma.pdf" in os.listdir(lib) and "2020_Ames_Plasma.ris" in os.listdir(lib)
    assert R.ris_owner(str(lib / "2020_Ames_Plasma.ris")) == "pipeline"
    make_pdf(lib / "2021_Baird_Sweat.pdf", ["y"])
    if not (lib / "2021_baird_sweat.pdf").exists():
        pytest.skip("case-sensitive filesystem")
    with pytest.raises(FileExistsError):
        AF.cascade_rename(str(lib), "2020_Ames_Plasma.pdf", "2021_baird_sweat.pdf")
    assert sorted(p for p in os.listdir(lib) if p.endswith(".pdf")) == ["2020_Ames_Plasma.pdf", "2021_Baird_Sweat.pdf"]


def test_audit_offline_sends_nothing(lab, capsys):
    lib = lab.lib
    make_pdf(lib / "Unknown_Ames_Plasma.pdf", ["x"])
    (lib / "Unknown_Ames_Plasma.ris").write_text(
        "TY  - JOUR\nAU  - Ames, Alex\nTI  - " + T["a"] + "\nPY  - 2020\nDO  - 10.5555/a1\nER  - \n", encoding="utf-8")
    res = AF.run(lib_dir=str(lib), offline=True)
    assert [r["status"] for r in res["rows"]] == ["WOULD_RENAME"] and lab.srv.hits == []
    assert res["report"] is None and sorted(os.listdir(lib)) == ["Unknown_Ames_Plasma.pdf", "Unknown_Ames_Plasma.ris"]


# ---------------------------------------------------------------------------- fill_missing_dois consumers
def _consumer_items():
    title = "Heat acclimation decay and re-induction in endurance trained cyclists"
    return title, [dict(rec("10.5555/t1", title, "Daanen", "Hein", 2018), score=60.0),
                   dict(rec("10.5555/t2", "Unrelated renal physiology review of sodium handling",
                            "Other", "Pat", 2011), score=20.0)]


def test_consumer_call_form_still_imports_and_says_why_nothing_is_high(capsys):
    """The two consumer scripts do `from fill_missing_dois import CROSSREF, extract_metadata,
    filter_by_type, score_match` and call score_match(year, surname, ranked, raw). Without
    evidence= nothing can be HIGH any more; the module must say so instead of degrading silently."""
    assert FMD.CROSSREF.endswith("/works")
    title, items = _consumer_items()
    ranked, _demoted = FMD.filter_by_type(items)
    status, meta, s1, _s2 = FMD.score_match("2018", "Daanen", ranked, items)
    assert status != "HIGH" and meta["doi"] == "10.5555/t1" and s1 == 60.0
    assert "evidence" in capsys.readouterr().err
    ref = f"Daanen HAM, Racinais S, Periard JD. {title}. Sports Med. 2018;48:409-430."
    status, *_ = FMD.score_match("2018", "Daanen", ranked, items, evidence=ref, query=ref)
    assert status == "HIGH"
    m = FMD.extract_metadata(items[0])
    assert {"doi", "title", "authors", "year", "type", "first_family"} <= set(m)
