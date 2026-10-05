"""W3-D2 (T9, I16): audit_filenames reads the .ris DOI first, resolves through ris_emit
(resolve_meta, crossref_meta: decoded names, print-first year, encoded DOI path), proposes nothing
it cannot stand behind (AMBIGUOUS, IDENTITY_FLAG, REVIEW_AUTHOR), maps legacy names and run-id
artifacts through --queue-history, reports errors on stdout and exits 2 with a step summary.
The metadata seam `crossref` is stubbed, or ris_emit is served by MockServer."""
import csv
import json
import sys
from pathlib import Path

import fitz
import pytest

import audit_filenames as A
import ris_emit
from litpipe.outcomes import Kind, Outcome
from tests import netmock


def pdf(path, text="a scan with no identifiers"):
    doc = fitz.open()
    doc.new_page().insert_text((72, 72), text[:90])
    path.write_bytes(doc.tobytes())
    doc.close()


def ris(path, doi=None, au=(), ti="", py=""):
    lines = ["TY  - JOUR"] + [f"AU  - {a}" for a in au]
    if ti:
        lines.append(f"TI  - {ti}")
    if py:
        lines.append(f"PY  - {py}")
    if doi:
        lines.append(f"DO  - {doi}")
    path.write_text("\n".join(lines + ["ER  - ", ""]), encoding="utf-8")


def meta(title, year, family, authors=None):
    return {"title": title, "year": year, "lastname": family,
            "authors": authors or [{"family": family, "given": ""}]}


@pytest.fixture
def lib(tmp_path):
    d = tmp_path / "lib"
    d.mkdir()
    return d


def audit(monkeypatch, lib, argv=(), cr=None, report=True):
    calls = []
    if cr is not None:
        def fake(doi):
            calls.append(doi)
            v = cr.get(doi) if isinstance(cr, dict) else cr(doi)
            if isinstance(v, Exception):
                raise v
            return v
        monkeypatch.setattr(A, "crossref", fake)
    rep = lib.parent / "audit.csv"
    args = ["--lib-dir", str(lib)] + (["--report", str(rep)] if report else []) + list(argv)
    code = A.main(args)
    rows = {}
    if report and rep.exists():
        with open(rep, encoding="utf-8") as fh:
            rows = {r["current"]: r for r in csv.DictReader(fh)}
    return code, rows, calls


TITLE = "Heat acclimation and athletic performance in the heat"


# ---------------------------------------------------------------- DOI order: the .ris first
def test_ris_doi_is_read_first(lib, monkeypatch):
    fn = "2020_Periard_HeatAcclimation.pdf"
    pdf(lib / fn, "doi:10.9999/text.doi.1")
    ris(lib / (fn[:-4] + ".ris"), "10.1123/ijspp.2019-0123")
    code, rows, calls = audit(monkeypatch, lib, cr={"10.1123/ijspp.2019-0123": meta(TITLE, "2020", "Periard")})
    assert calls == ["10.1123/ijspp.2019-0123"] and code == 0
    assert rows[fn]["status"] == "WOULD_RENAME"
    assert rows[fn]["proposed"] == "2020_Periard_HeatAcclimationAthleticPerformanceHeat.pdf"


def test_sidecar_disagreeing_with_the_ris_is_ambiguous(lib, monkeypatch):
    fn = "2020_Periard_HeatAcclimation.pdf"
    pdf(lib / fn)
    ris(lib / (fn[:-4] + ".ris"), "10.1123/ijspp.2019-0123")
    (lib / (fn[:-4] + ".fulltext.json")).write_text(json.dumps({"doi": "10.1016/j.other.2020.1"}), encoding="utf-8")
    code, rows, calls = audit(monkeypatch, lib, cr={})
    assert rows[fn]["status"] == "AMBIGUOUS" and calls == [] and code == 0
    assert rows[fn]["doi"] == "10.1123/ijspp.2019-0123;10.1016/j.other.2020.1"


# ---------------------------------------------------------------- queue history
def write_queue(path, rows, comment=False):
    with open(path, "w", encoding="utf-8", newline="") as f:
        if comment:
            f.write("# a comment line the sweep leaves\n")
        w = csv.DictWriter(f, fieldnames=["doi", "title", "authors", "year", "destination", "notes"])
        w.writeheader()
        for r in rows:
            w.writerow({"destination": "lib", "notes": "", **r})


def test_legacy_named_file_still_maps_through_queue_history(lib, tmp_path, monkeypatch):
    """163 files on disk carry pre-DEC-14/15 names; the map holds both conventions."""
    from unpaywall_fetch_v2 import build_filename, legacy_build_filename
    row = {"doi": "10.1234/heart.2019.7", "title": "Is the effect of heat on the heart large",
           "authors": "van der Walt JHA; Smith B", "year": "2019"}
    legacy = legacy_build_filename(row["year"], row["authors"], row["title"])
    assert legacy == "2019_van_IsEffectHeatHeartLarge.pdf" != build_filename(row["year"], row["authors"], row["title"])
    pdf(lib / legacy)
    q = tmp_path / "lit_pull_queue.2026-09-01.processed.csv"
    write_queue(q, [row])
    m = meta(row["title"], "2019", "van der Walt", [{"family": "van der Walt", "given": "J"}])
    code, rows, calls = audit(monkeypatch, lib, ["--queue-history", str(tmp_path / "lit_pull_queue.*processed*.csv")],
                              cr={row["doi"]: m})
    assert calls == [row["doi"]]
    assert rows[legacy]["status"] == "WOULD_RENAME_QH"
    assert rows[legacy]["proposed"] == "2019_vanderWalt_EffectHeatHeartLarge.pdf"


def test_run_id_and_legacy_artifact_names_are_read(tmp_path):
    q = tmp_path / "proj"
    q.mkdir()
    write_queue(q / "lit_pull_queue.teaching.2026-10-01.2.processed.csv",
                [{"doi": "10.1234/a.1", "title": "Alpha heat paper title", "authors": "Smith J", "year": "2020"}], comment=True)
    write_queue(q / "lit_pull_queue.2026-09-01.processed.2.csv",
                [{"doi": "10.1234/b.1", "title": "Beta heat paper title", "authors": "Jones K", "year": "2019"}])
    write_queue(q / "lit_pull_queue.2026-10-01.unpaywall.csv",
                [{"doi": "10.1234/c.1", "title": "Gamma", "authors": "Lee", "year": "2018"}])
    by_dir, paths = A.load_queue_history(str(q))
    assert sorted(Path(p).name for p in paths) == ["lit_pull_queue.2026-09-01.processed.2.csv",
                                                   "lit_pull_queue.teaching.2026-10-01.2.processed.csv"]
    assert sorted(by_dir.values()) == ["10.1234/a.1", "10.1234/b.1"]
    by_glob, _ = A.load_queue_history(str(q / "lit_pull_queue.*processed*.csv"))
    assert by_glob == by_dir
    assert A.QUEUE_ARTIFACT_RE.match("lit_pull_queue.teaching.2026-10-01.2.processed.csv").group("tag") == "teaching"


def test_two_unknown_files_are_ambiguous_not_a_proposal(lib, tmp_path, monkeypatch):
    """T9 acceptance (I11 shape): rows with neither title nor authors all build one name; the
    library's two Unknown_ files get AMBIGUOUS, never a confident rename."""
    write_queue(tmp_path / "lit_pull_queue.2026-09-03.processed.csv",
                [{"doi": "10.1234/kokkinos.1", "title": "", "authors": "", "year": "2020"},
                 {"doi": "10.1234/macdonald.2", "title": "", "authors": "", "year": "2020"}])
    for fn in ("2020_Unknown_Untitled.pdf", "2020_Unknown_Untitled_3f2a1b.pdf"):
        pdf(lib / fn)
    code, rows, calls = audit(monkeypatch, lib, ["--queue-history", str(tmp_path / "lit_pull_queue.*processed*.csv")],
                              cr={})
    assert rows["2020_Unknown_Untitled.pdf"]["status"] == "AMBIGUOUS_QH"
    assert rows["2020_Unknown_Untitled.pdf"]["doi"] == "10.1234/kokkinos.1;10.1234/macdonald.2"
    assert rows["2020_Unknown_Untitled_3f2a1b.pdf"]["status"] == "NO_DOI"
    assert calls == [] and not any(r["status"].startswith("WOULD_RENAME") for r in rows.values())


def test_a_single_blank_row_identifies_nothing(lib, tmp_path, monkeypatch):
    write_queue(tmp_path / "lit_pull_queue.2026-09-03.processed.csv",
                [{"doi": "10.1234/kokkinos.1", "title": "", "authors": "", "year": "2020"}])
    pdf(lib / "2020_Unknown_Untitled.pdf")
    _code, rows, calls = audit(monkeypatch, lib, ["--queue-history", str(tmp_path / "*.csv")], cr={})
    assert rows["2020_Unknown_Untitled.pdf"]["status"] == "AMBIGUOUS_QH" and calls == []


def test_queue_history_disagreeing_with_the_ris_is_ambiguous(lib, tmp_path, monkeypatch):
    row = {"doi": "10.1234/queued.1", "title": "Heat acclimation and athletic performance", "authors": "Periard JD",
           "year": "2020"}
    fn = "2020_Periard_HeatAcclimationAthleticPerformance.pdf"
    pdf(lib / fn)
    ris(lib / (fn[:-4] + ".ris"), "10.1234/other.9")
    write_queue(tmp_path / "lit_pull_queue.2026-09-01.processed.csv", [row])
    _code, rows, calls = audit(monkeypatch, lib, ["--queue-history", str(tmp_path / "*.csv")], cr={})
    assert rows[fn]["status"] == "AMBIGUOUS" and calls == []


def test_a_doi_carried_by_two_files_is_ambiguous_for_both(lib, monkeypatch):
    for fn in ("2020_Unknown_Untitled.pdf", "2020_Unknown_Untitled_3f2a1b.pdf"):
        pdf(lib / fn)
        ris(lib / (fn[:-4] + ".ris"), "10.1234/macdonald.2")
    _code, rows, calls = audit(monkeypatch, lib, cr={})
    assert {r["status"] for r in rows.values()} == {"AMBIGUOUS"} and calls == []


# ---------------------------------------------------------------- identity flags
@pytest.mark.parametrize("ext,rec", [(".identity.json", {"identity": "FLAG"}),
                                     (".identity.json", {"identity": "OK", "doc_kind": "SUPPLEMENT"}),
                                     (".fulltext.json", {"doi": "10.1234/queue.1", "identity": "FLAG"})])
def test_identity_flagged_file_is_never_renamed(ext, rec, lib, monkeypatch):
    fn = "2020_Unknown_SomethingFetched.pdf"
    pdf(lib / fn)
    ris(lib / (fn[:-4] + ".ris"), "10.1234/queue.1")
    (lib / (fn[:-4] + ext)).write_text(json.dumps(rec), encoding="utf-8")
    code, rows, calls = audit(monkeypatch, lib, ["--execute"], cr={"10.1234/queue.1": meta(TITLE, "2020", "Periard")})
    assert rows[fn]["status"] == "IDENTITY_FLAG" and calls == [] and code == 0
    assert (lib / fn).exists()


# ---------------------------------------------------------------- names: decoded, normalised
def test_muumlndel_becomes_mundel_through_the_ris_emit_path(lib, monkeypatch):
    """The seam is ris_emit's: crossref_by_doi + crossref_meta decode `M&uuml;ndel` and read the
    print year first (the old path read published-online and kept the entity)."""
    fn = "2008_Muumlndel_ExerciseHeatStressMetabolism.pdf"
    pdf(lib / fn)
    ris(lib / (fn[:-4] + ".ris"), "10.1159/000151554")
    msg = {"DOI": "10.1159/000151554", "type": "book-chapter", "title": ["Exercise Heat Stress and Metabolism"],
           "author": [{"family": "M&uuml;ndel", "given": "Toby"}],
           "published-online": {"date-parts": [[2007, 12]]}, "published-print": {"date-parts": [[2008]]}}
    seen = []
    monkeypatch.setattr(ris_emit, "crossref_by_doi", lambda d, timeout=15: seen.append(d) or msg)
    _code, rows, _ = audit(monkeypatch, lib)
    assert seen == ["10.1159/000151554"]
    assert rows[fn]["proposed"] == "2008_Mundel_ExerciseHeatStressMetabolism.pdf"
    assert rows[fn]["status"] == "WOULD_RENAME"
    assert "Muumlndel" not in A.canonical_filename("2008", "M&uuml;ndel", "x")


def test_crossref_seam_reaches_datacite_through_resolve_meta(lib, monkeypatch):
    fn = "2024_Unknown_AgentsMemory_preprint.pdf"
    pdf(lib / fn)
    ris(lib / (fn[:-4] + ".ris"), "10.48550/arxiv.2401.00001")
    monkeypatch.setattr(ris_emit, "resolve_meta", lambda d: (
        {"doi": d, "title": "Agents with memory: a survey of long-horizon methods", "year": "2024",
         "lastname": "Zhang", "authors": [{"family": "Zhang", "given": "Wei"}]}, "datacite"))
    _code, rows, _ = audit(monkeypatch, lib)
    assert rows[fn]["proposed"] == "2024_Zhang_AgentsMemorySurveyLong-horizonMethods_preprint.pdf"   # suffix kept


@pytest.mark.parametrize("title", ["<i>In vivo</i> heat acclimation &amp; cold tolerance",
                                   "V&#775;O<sub>2max</sub> responses to heat",
                                   "<mml:math><mml:mi>V</mml:mi></mml:math>O2 kinetics in the heat",
                                   "Cardiovascular Drift in Périard Athletes"])
def test_canonical_filename_normalises_the_title_and_agrees_with_the_writer(title):
    from unpaywall_fetch_v2 import build_filename
    name = A.canonical_filename("2021", "Périard", title)
    assert name == build_filename("2021", "Périard J", title)
    assert name.isascii() and not any(junk in name for junk in ("<", "Amp", "Sub", "Mml", "775"))


def test_doi_in_the_crossref_url_is_percent_encoded(net_env, mock_server, lib, monkeypatch):
    """Every DOI placed in a URL goes through litpipe.doi.encode_path (DOI Handbook 4.7); the old
    `api.crossref.org/works/{doi}` was raw."""
    srv = mock_server()
    doi = "10.1002/(sici)1097-0142(20000915)89:6<1234::aid-cncr5>3.0.co;2-2"
    path = "/works/10.1002/(sici)1097-0142(20000915)89:6%3C1234::aid-cncr5%3E3.0.co;2-2"
    body = {"message": {"DOI": doi, "type": "journal-article", "title": ["Cancer heat therapy outcomes in adults"],
                        "author": [{"family": "Smith", "given": "A"}], "published-print": {"date-parts": [[2000]]}}}
    srv.script(path, netmock.Reply(200, json.dumps(body), {"Content-Type": "application/json"}))
    monkeypatch.setattr(ris_emit, "CROSSREF_WORK", srv.url("/works/") + "{doi}")
    fn = "2000_Smith_CancerHeat.pdf"
    pdf(lib / fn)
    ris(lib / (fn[:-4] + ".ris"), doi)
    _code, rows, _ = audit(monkeypatch, lib)
    assert [h.path for h in srv.hits] == [path]
    assert rows[fn]["proposed"] == "2000_Smith_CancerHeatTherapyOutcomesAdults.pdf"


# ---------------------------------------------------------------- safety checks
@pytest.mark.parametrize("fn,m,status", [
    # a different author and a different title: the DOI may be another paper's
    ("2020_Smith_MuscleGlycogenRecovery.pdf", meta(TITLE, "2020", "Jones"), "REVIEW_AUTHOR"),
    # author order slip: the file names the second author
    ("2020_Smith_MuscleGlycogenRecovery.pdf",
     meta(TITLE, "2020", "Jones", [{"family": "Jones", "given": "A"}, {"family": "Smith", "given": "B"}]), "WOULD_RENAME"),
    # the given name as the surname: the file or the deposit swapped them (both occur): review
    ("2023_Zirui_KiviPlugPlay.pdf",
     meta(TITLE, "2023", "Liu", [{"family": "Liu", "given": "Zirui"}]), "REVIEW_GIVEN_NAME"),
    # a swapped deposit ("Tongwu, Yu"): a co-author "Yuxiong" must not prefix-match "Yu"
    ("2025_Yu_SupramaximalIntervalTraining.pdf",
     meta(TITLE, "2025", "Tongwu", [{"family": "Tongwu", "given": "Yu"}, {"family": "Yuxiong", "given": "Xu"}]),
     "REVIEW_GIVEN_NAME"),
    # a second given name ("Wu, Xi Vivien"), a hyphenated one ("Podolski, Anne-Sophie")
    ("2025_Vivien_IntergenerationalDance.pdf", meta(TITLE, "2025", "Wu", [{"family": "Wu", "given": "Xi Vivien"}]),
     "REVIEW_GIVEN_NAME"),
    ("2023_Sophie_DanceMovement.pdf", meta(TITLE, "2023", "Podolski", [{"family": "Podolski", "given": "Anne-Sophie"}]),
     "REVIEW_GIVEN_NAME"),
    # a deposit with given and family swapped: an initial, or a given name with initials
    ("2017_Czuba_IntermittentHypoxicTraining.pdf",
     meta(TITLE, "2017", "M", [{"family": "M", "given": "Czuba"}]), "REVIEW_RECORD_AUTHOR"),
    ("2019_ApkarianMarc_AcuteHeartRate.pdf", meta(TITLE, "2019", "Marc R"), "REVIEW_RECORD_AUTHOR"),
    # named after an organisation, but the title slug agrees
    ("2022_PrincetonGoogle_HeatAcclimationAthleticPerformanceHeat.pdf", meta(TITLE, "2022", "Yao"), "WOULD_RENAME"),
    # letters dropped by the old ascii-ignore naming
    ("2019_Mller_SomethingElse.pdf", meta(TITLE, "2019", "Müller"), "WOULD_RENAME"),
    # year 0000 is unknown, not a contradiction
    ("0000_Periard_HeatAcclimation.pdf", meta(TITLE, "2020", "Periard"), "WOULD_RENAME"),
    # a real year difference is still a review item
    ("2019_Periard_HeatAcclimation.pdf", meta(TITLE, "2020", "Periard"), "SKIP_YEAR_DIFF_2019_vs_2020"),
])
def test_author_and_year_safety(fn, m, status, lib, monkeypatch):
    pdf(lib / fn)
    ris(lib / (fn[:-4] + ".ris"), "10.1234/x.1")
    _code, rows, _ = audit(monkeypatch, lib, cr={"10.1234/x.1": m})
    assert rows[fn]["status"] == status


# ---------------------------------------------------------------- errors: stdout, exit 2
def test_rename_error_goes_to_stdout_and_exits_2(lib, monkeypatch, capsys):
    fn = "2020_Unknown_HeatAcclimation.pdf"
    pdf(lib / fn)
    ris(lib / (fn[:-4] + ".ris"), "10.1234/x.1")

    def boom(*a, **k):
        raise PermissionError(13, "file in use")
    monkeypatch.setattr(A, "cascade_rename", boom)
    code, rows, _ = audit(monkeypatch, lib, ["--execute"], cr={"10.1234/x.1": meta(TITLE, "2020", "Periard")})
    out, err = capsys.readouterr()
    assert code == 2 and rows[fn]["status"].startswith("RENAME_ERROR")
    assert "RENAME_ERROR" in out and "RENAME_ERROR" not in err
    last = out.strip().splitlines()[-1]
    assert last.startswith(A.SUMMARY_MARKER)
    s = json.loads(last[len(A.SUMMARY_MARKER):])
    assert s["reasons"] == ["RENAME_ERROR: 1 row(s)"] and s["aborted"] is None and s["transport_failures"] == 0


def test_unreadable_pdf_is_pdf_error_not_no_doi(lib, monkeypatch, capsys):
    """W2b forward: PyMuPDF errors were swallowed as "no DOI" (`audit_filenames.py:55-63`)."""
    (lib / "2020_Smith_Broken.pdf").write_bytes(b"%PDF-1.4\n")
    pdf(lib / "2020_Smith_Fine.pdf")
    code, rows, _ = audit(monkeypatch, lib, cr={})
    assert rows["2020_Smith_Broken.pdf"]["status"] == "PDF_ERROR"
    assert rows["2020_Smith_Fine.pdf"]["status"] == "NO_DOI"
    assert code == 2 and "PDF_ERROR" in capsys.readouterr().out
    with pytest.raises(A.PdfReadError):
        A.extract_doi_from_pdf(str(lib / "2020_Smith_Broken.pdf"))


def test_metadata_unavailable_is_an_error_not_no_record(lib, monkeypatch, capsys):
    fn = "2020_Smith_Heat.pdf"
    pdf(lib / fn)
    ris(lib / (fn[:-4] + ".ris"), "10.1234/x.1")
    err = ris_emit.MetadataUnavailable("crossref", Outcome(Kind.OUTAGE, 503, "api.crossref.org", "HTTP 503", 1, 1.0))
    code, rows, _ = audit(monkeypatch, lib, cr={"10.1234/x.1": err})
    assert rows[fn]["status"] == "META_UNAVAILABLE" and code == 2
    assert '"transport_failures": 1' in capsys.readouterr().out.strip().splitlines()[-1]


def test_usage_and_config_errors_exit_1(tmp_path, monkeypatch):
    assert A.main(["--lib-dir", str(tmp_path / "missing")]) == 1
    with pytest.raises(SystemExit) as e:
        A.main([])
    assert e.value.code == 1
    (tmp_path / "lib").mkdir()
    pdf(tmp_path / "lib" / "2020_A_B.pdf")
    assert A.main(["--lib-dir", str(tmp_path / "lib"), "--only", "2020_Not_There.pdf"]) == 1


# ---------------------------------------------------------------- --only, dry run, offline
def test_only_limits_the_audit_to_the_named_files(lib, monkeypatch):
    for fn in ("2020_Unknown_A.pdf", "2020_Unknown_B.pdf", "2020_Unknown_C.pdf"):
        pdf(lib / fn)
        ris(lib / (fn[:-4] + ".ris"), f"10.1234/{fn[13].lower()}.1")
    cr = {f"10.1234/{c}.1": meta(f"{c} heat title words here", "2020", "Periard") for c in "abc"}
    _code, rows, calls = audit(monkeypatch, lib, ["--only", "2020_Unknown_A.pdf", "2020_Unknown_C.pdf"], cr=cr)
    assert sorted(rows) == ["2020_Unknown_A.pdf", "2020_Unknown_C.pdf"] and calls == ["10.1234/a.1", "10.1234/c.1"]


def test_dry_run_writes_nothing(lib, monkeypatch, capsys):
    fn = "2020_Unknown_HeatAcclimation.pdf"
    pdf(lib / fn)
    ris(lib / (fn[:-4] + ".ris"), "10.1234/x.1")
    before = sorted(p.name for p in lib.iterdir())
    code, rows, _ = audit(monkeypatch, lib, cr={"10.1234/x.1": meta(TITLE, "2020", "Periard")}, report=False)
    assert code == 0 and sorted(p.name for p in lib.iterdir()) == before
    assert "PROPOSE" in capsys.readouterr().out


def test_execute_writes_the_default_report_in_the_library(lib, monkeypatch, st_manifest):
    fn = "2020_Unknown_HeatAcclimation.pdf"
    pdf(lib / fn)
    ris(lib / (fn[:-4] + ".ris"), "10.1234/x.1")
    code, _rows, _ = audit(monkeypatch, lib, ["--execute"], cr={"10.1234/x.1": meta(TITLE, "2020", "Periard")}, report=False)
    assert code == 0 and (lib / A.DEFAULT_REPORT).exists()
    assert (lib / "2020_Periard_HeatAcclimationAthleticPerformanceHeat.pdf").exists()


def test_offline_proposes_from_the_ris_without_a_request(lib, monkeypatch):
    fn = "2021_Unknown_LactateThresholds.pdf"
    pdf(lib / fn)
    ris(lib / (fn[:-4] + ".ris"), "10.1234/w.1", au=("Wackerhage, Henning", "Smith, A"),
        ti="Lactate thresholds and the simulation of human energy metabolism", py="2021")
    monkeypatch.setattr(A, "crossref", lambda d: (_ for _ in ()).throw(AssertionError("network")))
    _code, rows, _ = audit(monkeypatch, lib, ["--offline"])
    assert rows[fn]["proposed"] == "2021_Wackerhage_LactateThresholdsSimulationHumanEnergyMetabolism.pdf"
    assert rows[fn]["status"] == "WOULD_RENAME"


def test_module_binds_no_email_and_no_raw_crossref_url():
    import inspect
    src = inspect.getsource(A)
    assert not hasattr(A, "EMAIL") and not hasattr(A, "UA") and "requests.get" not in src
    assert "api.crossref.org/works/{" not in src


@pytest.fixture
def st_manifest(monkeypatch):
    s = netmock.FakeState()
    monkeypatch.setattr(ris_emit, "STATE", s)
    return s


def test_help_exits_0():
    import subprocess
    p = subprocess.run([sys.executable, str(Path(A.__file__)), "--help"], capture_output=True, text=True)
    assert p.returncode == 0 and "--only" in p.stdout and "--offline" in p.stdout
