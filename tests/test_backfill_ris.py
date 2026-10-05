"""W2-E2: backfill_ris. No default library (--lib-dir or --project through the registry); per-row
statuses (META_UNAVAILABLE counted and the run continues, KEPT_CURATED when write_ris keeps a
curated file, PDF_ERROR not NO_DOI, NO_DOI with the path, IDENTITY_FLAG, the resolve_meta source);
DEC-29 (--overwrite keeps an edited .ris, --force replaces it); --include-text-only writes one .ris
per text-only holding from the sidecar's own fields, none for a FLAG sidecar, with no request."""
import csv
import json
import shutil
import subprocess
import sys
from pathlib import Path

import fitz
import pytest

import backfill_ris as B
import lit_util
import ris_emit as R
from litpipe import config
from litpipe.outcomes import Kind
from tests.netmock import Reply

FIX = Path(__file__).resolve().parent / "fixtures" / "W2-E2"
REPO = Path(__file__).resolve().parent.parent
JSON = {"Content-Type": "application/json"}


def make_pdf(path, text):
    d = fitz.open()
    d.new_page().insert_text((72, 72), text)
    d.save(str(path))
    d.close()


def meta_for(doi, title="A Title"):
    return {"doi": doi, "title": title, "year": "2020", "date": "2020", "lastname": "Smith",
            "authors": [{"family": "Smith", "given": "Jane"}], "container": "Journal of Tests",
            "volume": "1", "issue": "2", "page": "3-4", "issn": "", "abstract": "",
            "url": f"https://doi.org/{doi}", "type": "journal-article"}


@pytest.fixture
def lib(tmp_path):
    d = tmp_path / "library"
    d.mkdir()
    return d


@pytest.fixture
def resolver(monkeypatch):
    """resolve_meta double: answers[doi] is (meta, source) or an exception; calls are logged."""
    answers, calls = {}, []

    def fake(doi):
        calls.append(doi)
        a = answers.get(doi, ({}, "none"))
        if isinstance(a, BaseException):
            raise a
        return a
    monkeypatch.setattr(R, "resolve_meta", fake)
    fake.answers, fake.calls = answers, calls
    return fake


def rows_by_name(res):
    return {Path(r["path"]).name: r for r in res["rows"]}


# ---------------------------------------------------------------- library selection
def test_no_library_exits_2_with_one_line(capsys):
    assert B.main([]) == 2
    err = capsys.readouterr().err.strip().splitlines()
    assert len(err) == 1 and "--lib-dir" in err[0] and "--project" in err[0]


@pytest.mark.parametrize("argv, needle", [
    (["--lib-dir", "x", "--project", "y"], "not both"),
    (["--project", "research_nowhere"], "not registered"),
    (["--lib-dir", "no/such/dir"], "not found"),
])
def test_bad_library_args_exit_2_with_one_line(argv, needle, capsys, tmp_path, monkeypatch):
    reg = tmp_path / "projects.json"
    reg.write_text(json.dumps({"projects": {}}), encoding="utf-8")
    monkeypatch.setattr(config, "CONFIG_PATH", reg)
    monkeypatch.chdir(tmp_path)
    assert B.main(argv) == 2
    err = capsys.readouterr().err.strip().splitlines()
    assert len(err) == 1 and needle in err[0]


def test_default_lib_is_gone():
    assert not hasattr(B, "DEFAULT_LIB")
    assert not hasattr(B, "EMAIL") and not hasattr(B, "UA")


def test_project_resolves_through_the_registry(tmp_path, monkeypatch, resolver):
    root = tmp_path / "root"
    reg = tmp_path / "projects.json"
    reg.write_text(json.dumps({"projects": {
        "research_alpha": {"lib_dir": "literature"},
        "teaching_parent/teaching_child": {"parent": "teaching_parent", "lib_dir": "teaching_child/literature"},
    }}), encoding="utf-8")
    monkeypatch.setattr(config, "CONFIG_PATH", reg)
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    (root / "research_alpha" / "literature").mkdir(parents=True)
    (root / "teaching_parent" / "teaching_child" / "literature").mkdir(parents=True)
    assert B.run(project="research_alpha")["lib_dir"] == str(root / "research_alpha" / "literature")
    assert B.run(project="teaching_parent/teaching_child")["lib_dir"] == str(
        root / "teaching_parent" / "teaching_child" / "literature")


# ---------------------------------------------------------------- per-row statuses (PDFs)
def test_pdf_rows_statuses_sources_and_counts(lib, resolver, capsys):
    make_pdf(lib / "2020_A_Crossref.pdf", "doi: 10.1234/crossref.1")
    make_pdf(lib / "2020_B_Jalc.pdf", "doi: 10.1234/jalc.2")
    make_pdf(lib / "2020_C_Down.pdf", "doi: 10.1234/down.3")
    make_pdf(lib / "2020_D_Istic.pdf", "doi: 10.1234/istic.4")
    make_pdf(lib / "2020_E_None.pdf", "doi: 10.1234/none.5")
    make_pdf(lib / "2020_F_NoDoi.pdf", "No identifier on this page at all.")
    (lib / "2020_G_Broken.pdf").write_bytes(b"not a pdf at all")
    resolver.answers.update({
        "10.1234/crossref.1": (meta_for("10.1234/crossref.1"), "crossref"),
        "10.1234/jalc.2": (meta_for("10.1234/jalc.2"), "cn:jalc"),
        "10.1234/down.3": R.MetadataUnavailable("crossref", detail="HTTP 503 after 6 retries"),
        "10.1234/istic.4": ({}, "unsupported:istic"),
    })
    res = B.run(lib_dir=str(lib), commit=True)
    rows = rows_by_name(res)
    assert rows["2020_A_Crossref.pdf"]["status"] == "WROTE" and rows["2020_A_Crossref.pdf"]["source"] == "crossref"
    assert rows["2020_B_Jalc.pdf"]["status"] == "WROTE" and rows["2020_B_Jalc.pdf"]["source"] == "cn:jalc"
    down = rows["2020_C_Down.pdf"]
    assert down["status"] == "META_UNAVAILABLE" and down["source"] == "crossref" and "503" in down["detail"]
    assert rows["2020_D_Istic.pdf"]["status"] == "UNSUPPORTED_RA"
    assert rows["2020_D_Istic.pdf"]["source"] == "unsupported:istic"
    assert rows["2020_E_None.pdf"]["status"] == "RESOLVE_FAIL" and rows["2020_E_None.pdf"]["source"] == "none"
    nodoi = rows["2020_F_NoDoi.pdf"]
    assert nodoi["status"] == "NO_DOI" and nodoi["path"] == str(lib / "2020_F_NoDoi.pdf")
    broken = rows["2020_G_Broken.pdf"]
    assert broken["status"] == "PDF_ERROR" and broken["detail"]
    # the run continued past the unavailable row and wrote the rest
    assert (lib / "2020_A_Crossref.ris").exists() and (lib / "2020_B_Jalc.ris").exists()
    assert not (lib / "2020_C_Down.ris").exists()
    s = res["summary"]
    assert (s["wrote"], s["meta_unavailable"], s["no_doi"], s["pdf_error"], s["crossref_fail"],
            s["unsupported_ra"]) == (2, 1, 1, 1, 1, 1)
    assert res["sources"] == {"crossref": 1, "cn:jalc": 1, "unavailable:crossref": 1,
                              "unsupported:istic": 1, "none": 1}
    out = capsys.readouterr().out
    assert str(lib / "2020_F_NoDoi.pdf") in out and "cn:jalc=1" in out
    # the report: legacy columns first, the new ones after
    with open(lib / B.REPORT_NAME, encoding="utf-8", newline="") as f:
        rep = list(csv.DictReader(f))
    assert list(rep[0].keys()) == B.REPORT_FIELDS and B.REPORT_FIELDS[:4] == ["pdf", "doi", "status", "out"]
    assert {r["status"] for r in rep} >= {"WROTE", "META_UNAVAILABLE", "NO_DOI", "PDF_ERROR"}


def test_pdf_error_is_not_counted_as_no_doi(lib, resolver):
    (lib / "2020_X_Empty.pdf").write_bytes(b"")
    res = B.run(lib_dir=str(lib))
    assert res["rows"][0]["status"] == "PDF_ERROR" and res["summary"]["no_doi"] == 0


def test_sidecar_doi_wins_and_is_normalised(lib, resolver):
    make_pdf(lib / "2020_S_Side.pdf", "doi: 10.9999/wrong.1")
    (lib / "2020_S_Side.fulltext.json").write_text(json.dumps({"doi": "https://doi.org/10.1234/SIDE.1"}),
                                                   encoding="utf-8")
    resolver.answers["10.1234/side.1"] = (meta_for("10.1234/side.1"), "crossref")
    row = B.run(lib_dir=str(lib))["rows"][0]
    assert row["doi"] == "10.1234/side.1" and row["doi_from"] == "sidecar" and row["status"] == "DRY:sidecar"
    assert resolver.calls == ["10.1234/side.1"]


@pytest.mark.parametrize("companion, rec", [
    (".identity.json", {"queue_doi": "10.1234/q.1", "identity": "FLAG", "doc_kind": "ARTICLE"}),
    (".identity.json", {"queue_doi": "10.1234/q.1", "identity": "OK", "doc_kind": "SUPPLEMENT"}),
    (".fulltext.json", {"doi": "10.1234/q.1", "identity": "FLAG", "text": "another work"}),
])
def test_flagged_pdf_gets_no_ris(lib, resolver, companion, rec):
    make_pdf(lib / "2020_Q_Flag.pdf", "doi: 10.1234/q.1")
    (lib / f"2020_Q_Flag{companion}").write_text(json.dumps(rec), encoding="utf-8")
    res = B.run(lib_dir=str(lib), commit=True)
    assert res["rows"][0]["status"] == "IDENTITY_FLAG" and resolver.calls == []
    assert not (lib / "2020_Q_Flag.ris").exists()


# ---------------------------------------------------------------- DEC-29: --overwrite and --force
def test_edited_ris_survives_overwrite_and_force_replaces_it(lib, resolver):
    make_pdf(lib / "2020_E_Edit.pdf", "doi: 10.1234/edit.1")
    resolver.answers["10.1234/edit.1"] = (meta_for("10.1234/edit.1"), "crossref")
    ris = lib / "2020_E_Edit.ris"
    assert B.run(lib_dir=str(lib), commit=True)["rows"][0]["status"] == "WROTE"
    fresh = ris.read_text(encoding="utf-8")
    # unedited: --overwrite may replace it (here with the same record)
    assert B.run(lib_dir=str(lib), commit=True, overwrite=True)["rows"][0]["status"] == "WROTE"
    # a curator edits it in EndNote
    edited = fresh.replace("TI  - A Title", "TI  - A Title, corrected by hand")
    ris.write_text(edited, encoding="utf-8")
    assert B.run(lib_dir=str(lib), commit=False, overwrite=True)["rows"][0]["status"] == "DRY:KEPT_CURATED"
    res = B.run(lib_dir=str(lib), commit=True, overwrite=True)
    assert res["rows"][0]["status"] == "KEPT_CURATED" and res["summary"]["kept_curated"] == 1
    assert ris.read_text(encoding="utf-8") == edited
    # without --overwrite the file is not even looked at
    assert B.run(lib_dir=str(lib), commit=True)["rows"][0]["status"] == "EXISTS_SKIP"
    res = B.run(lib_dir=str(lib), commit=True, force=True)
    assert res["rows"][0]["status"] == "WROTE" and ris.read_text(encoding="utf-8") == fresh


def test_unrecorded_curated_ris_is_kept_on_overwrite(lib, resolver):
    make_pdf(lib / "2020_C_Cur.pdf", "doi: 10.1234/cur.1")
    (lib / "2020_C_Cur.ris").write_text("TY  - JOUR\nTI  - Curated\nER  - \n", encoding="utf-8")
    resolver.answers["10.1234/cur.1"] = (meta_for("10.1234/cur.1"), "crossref")
    assert B.run(lib_dir=str(lib), commit=True, overwrite=True)["rows"][0]["status"] == "KEPT_CURATED"
    assert "Curated" in (lib / "2020_C_Cur.ris").read_text(encoding="utf-8")


def test_write_ris_false_is_counted_kept_curated(lib, resolver, monkeypatch):
    make_pdf(lib / "2020_K_Kept.pdf", "doi: 10.1234/kept.1")
    resolver.answers["10.1234/kept.1"] = (meta_for("10.1234/kept.1"), "crossref")
    monkeypatch.setattr(R, "write_ris", lambda *a, **k: False)
    res = B.run(lib_dir=str(lib), commit=True)
    assert res["rows"][0]["status"] == "KEPT_CURATED" and res["summary"]["wrote"] == 0


# ---------------------------------------------------------------- --include-text-only
@pytest.fixture
def text_lib(lib):
    shutil.copy(FIX / "textonly_w2a2_sidecar.json", lib / "2021_Berg_HeatAcclimation.fulltext.json")
    shutil.copy(FIX / "textonly_legacy_sidecar.json", lib / "2014_Field_SweatSodium.fulltext.json")
    shutil.copy(FIX / "textonly_flag_sidecar.json", lib / "2020_Smith_Other.fulltext.json")
    (lib / "2019_Empty_NoText.fulltext.json").write_text(json.dumps({"doi": "10.1234/empty.1", "text": ""}),
                                                         encoding="utf-8")
    # a PDF with its own sidecar: not a text-only holding
    make_pdf(lib / "2022_Pdf_Holding.PDF", "doi: 10.1234/pdf.1")
    (lib / "2022_Pdf_Holding.fulltext.json").write_text(json.dumps({"doi": "10.1234/pdf.1", "text": "x"}),
                                                        encoding="utf-8")
    return lib


def test_include_text_only_writes_one_ris_per_text_only_holding(text_lib, resolver):
    res = B.run(lib_dir=str(text_lib), commit=True, include_text_only=True)
    rows = rows_by_name(res)
    assert rows["2021_Berg_HeatAcclimation.fulltext.json"]["status"] == "WROTE"
    assert rows["2014_Field_SweatSodium.fulltext.json"]["status"] == "WROTE"
    assert rows["2020_Smith_Other.fulltext.json"]["status"] == "IDENTITY_FLAG"
    assert rows["2019_Empty_NoText.fulltext.json"]["status"] == "NO_TEXT"
    written = sorted(p.name for p in text_lib.glob("*.ris"))
    assert written == ["2014_Field_SweatSodium.ris", "2021_Berg_HeatAcclimation.ris"]   # + none for the PDF
    assert resolver.calls == ["10.1234/pdf.1"]          # text-only rows ask no metadata source
    assert rows["2022_Pdf_Holding.PDF"]["kind"] == "pdf"
    assert rows["2021_Berg_HeatAcclimation.fulltext.json"]["kind"] == "text_only"
    assert rows["2021_Berg_HeatAcclimation.fulltext.json"]["source"] == "sidecar"


def test_text_only_ris_carries_the_sidecar_fields(text_lib, resolver):
    B.run(lib_dir=str(text_lib), commit=True, include_text_only=True)
    lines = (text_lib / "2021_Berg_HeatAcclimation.ris").read_text(encoding="utf-8").splitlines()
    assert lines[0] == "TY  - JOUR" and lines[-1] == "ER  - "
    assert "TI  - Heat acclimation and VO₂max in trained runners: a randomised trial" in lines
    assert [ln for ln in lines if ln.startswith("AU  - ")] == [
        "AU  - van der Berg, Hanna", "AU  - Stone, River B.", "AU  - Hill-Moss, Ada E O", "AU  - Le, Van Thanh"]
    for want in ("PY  - 2021", "JO  - Journal of Applied Testing", "VL  - 6", "IS  - 2", "SP  - 123", "EP  - 130",
                 "DO  - 10.1234/textonly.0001", "UR  - https://doi.org/10.1234/textonly.0001",
                 "AB  - Heat acclimation raised VO₂max by 5 µg per kg. No adverse events occurred."):
        assert want in lines, want
    legacy = (text_lib / "2014_Field_SweatSodium.ris").read_text(encoding="utf-8").splitlines()
    assert "AU  - Field, Ira" in legacy and "DO  - 10.1234/textonly.0002" in legacy
    assert not any(ln.startswith(("VL", "IS", "SP")) for ln in legacy)
    # the record is the one ris_emit would build, and the manifest owns it (DEC-29)
    sc = json.loads((FIX / "textonly_w2a2_sidecar.json").read_text(encoding="utf-8"))
    assert "\n".join(lines) + "\n" == R.build_ris(B.sidecar_meta(sc))
    assert R.ris_owner(str(text_lib / "2021_Berg_HeatAcclimation.ris")) == "pipeline"


def test_text_only_ur_line_percent_encodes_a_sici_doi(lib, resolver):
    sc = json.loads((FIX / "textonly_legacy_sidecar.json").read_text(encoding="utf-8"))
    sc["doi"] = "10.1002/(SICI)1097-4636(199601)30:1<1::AID-JBM1>3.0.CO;2-N"
    (lib / "1996_Sici_Paper.fulltext.json").write_text(json.dumps(sc), encoding="utf-8")
    B.run(lib_dir=str(lib), commit=True, include_text_only=True)
    lines = (lib / "1996_Sici_Paper.ris").read_text(encoding="utf-8").splitlines()
    assert "DO  - 10.1002/(sici)1097-4636(199601)30:1<1::aid-jbm1>3.0.co;2-n" in lines
    assert "UR  - https://doi.org/10.1002/(sici)1097-4636(199601)30:1%3C1::aid-jbm1%3E3.0.co;2-n" in lines


def test_text_only_needs_the_flag(text_lib, resolver):
    B.run(lib_dir=str(text_lib), commit=True)
    assert sorted(p.name for p in text_lib.glob("*.ris")) == []     # only the PDF was looked at (NONE)


def test_text_only_rows_send_no_request(text_lib, monkeypatch, resolver):
    from litpipe import net
    monkeypatch.setattr(net, "request", lambda *a, **k: pytest.fail("a text-only row made a request"))
    (text_lib / "2022_Pdf_Holding.PDF").unlink()
    (text_lib / "2022_Pdf_Holding.fulltext.json").unlink()
    monkeypatch.setattr(R, "resolve_meta", lambda d: pytest.fail("resolve_meta called"))
    res = B.run(lib_dir=str(text_lib), commit=True, include_text_only=True)
    assert res["summary"]["wrote"] == 2


def test_text_only_ris_survives_overwrite_once_edited(text_lib, resolver):
    B.run(lib_dir=str(text_lib), commit=True, include_text_only=True)
    ris = text_lib / "2014_Field_SweatSodium.ris"
    ris.write_text(ris.read_text(encoding="utf-8") + "N1  - curator note\n", encoding="utf-8")
    rows = rows_by_name(B.run(lib_dir=str(text_lib), commit=True, include_text_only=True, overwrite=True))
    assert rows["2014_Field_SweatSodium.fulltext.json"]["status"] == "KEPT_CURATED"
    assert "curator note" in ris.read_text(encoding="utf-8")
    rows = rows_by_name(B.run(lib_dir=str(text_lib), commit=True, include_text_only=True, force=True))
    assert rows["2014_Field_SweatSodium.fulltext.json"]["status"] == "WROTE"
    assert "curator note" not in ris.read_text(encoding="utf-8")


@pytest.mark.parametrize("raw, want", [
    ("Stone River B.", {"family": "Stone", "given": "River B."}),
    ("van der Berg Hanna", {"family": "van der Berg", "given": "Hanna"}),
    ("Le Van Thanh", {"family": "Le", "given": "Van Thanh"}),
    ("Smith", {"family": "Smith", "given": ""}),
    ("Smith, Jane A", {"family": "Smith", "given": "Jane A"}),
    ({"family": "O´Neil", "given": "Ann"}, {"family": "O´Neil", "given": "Ann"}),
    ("", None),
])
def test_split_author(raw, want):
    assert B.split_author(raw) == want


# ---------------------------------------------------------------- real ris_emit + litpipe.net
def test_metadata_unavailable_through_real_net_is_counted_and_the_run_continues(net_env, mock_server,
                                                                               monkeypatch, lib):
    s = mock_server("127.0.0.1")
    monkeypatch.setattr(R, "CROSSREF_WORK", s.url("/works/{doi}"))
    monkeypatch.setattr(R, "DATACITE_WORK", s.url("/dois/{doi}"))
    monkeypatch.setattr(R, "DOI_RA", s.url("/doiRA/{doi}"))
    msg = {"DOI": "10.1234/ok.2", "title": ["Second paper"], "type": "journal-article",
           "author": [{"family": "Smith", "given": "Jane"}], "published-print": {"date-parts": [[2020]]}}
    s.script("/works/10.1234/down.1", Reply(503))
    s.script("/works/10.1234/ok.2", Reply(200, json.dumps({"message": msg}), JSON))
    make_pdf(lib / "2020_A_Down.pdf", "doi: 10.1234/down.1")
    make_pdf(lib / "2020_B_Ok.pdf", "doi: 10.1234/ok.2")
    res = B.run(lib_dir=str(lib), commit=True)
    rows = rows_by_name(res)
    assert rows["2020_A_Down.pdf"]["status"] == "META_UNAVAILABLE"
    assert str(Kind.OUTAGE) in rows["2020_A_Down.pdf"]["detail"]
    assert rows["2020_B_Ok.pdf"]["status"] == "WROTE" and rows["2020_B_Ok.pdf"]["source"] == "crossref"
    report = (lib / B.REPORT_NAME).read_text(encoding="utf-8")
    assert "tester@litpipe-test.org" not in report and "email=" not in report and "mailto:" not in report


# ---------------------------------------------------------------- CLI
@pytest.mark.parametrize("script", ["backfill_ris.py", "enrich_abstracts.py"])
def test_help_exits_0(script):
    r = subprocess.run([sys.executable, str(REPO / script), "--help"], capture_output=True, text=True,
                       cwd=str(REPO), timeout=120)
    assert r.returncode == 0, r.stderr
    if script == "backfill_ris.py":
        for flag in ("--lib-dir", "--project", "--commit", "--overwrite", "--force", "--include-text-only",
                     "--limit", "--sleep"):
            assert flag in r.stdout
    else:
        for flag in ("--db", "--limit", "--sleep", "--only-papers", "--retry-after-days", "--commit-every"):
            assert flag in r.stdout


def test_main_in_process_with_project_flag(tmp_path, monkeypatch, resolver, capsys):
    root = tmp_path / "root"
    reg = tmp_path / "projects.json"
    reg.write_text(json.dumps({"projects": {"research_beta": {"lib_dir": "lit"}}}), encoding="utf-8")
    monkeypatch.setattr(config, "CONFIG_PATH", reg)
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    lib = root / "research_beta" / "lit"
    lib.mkdir(parents=True)
    shutil.copy(FIX / "textonly_legacy_sidecar.json", lib / "2014_Field_SweatSodium.fulltext.json")
    assert B.main(["--project", "research_beta", "--commit", "--include-text-only"]) == 0
    assert (lib / "2014_Field_SweatSodium.ris").exists() and (lib / B.REPORT_NAME).exists()
