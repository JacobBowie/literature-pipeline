"""W3-D2 (T9, refactor scope 3.6, DEC-20): fill_missing_dois accepts a DOI only from the front
matter or a strict title + author + year match, never promotes a near twin, never falls back to a
demoted or excluded record, and never overwrites a `.ris` DOI. Network stubbed (crossref_query,
ris_emit.resolve_meta) or served by MockServer; state is the conftest FakeState or a temp file."""
import copy
import csv
import hashlib
import json
from pathlib import Path

import fitz
import pytest

import fill_missing_dois as F
import lit_util
import ris_emit
from litpipe.outcomes import Kind, Outcome
from tests import netmock

FIX = Path(__file__).parent / "fixtures" / "W3-D2"
NEAR = json.loads((FIX / "near_twins.json").read_text(encoding="utf-8"))
WRONG = json.loads((FIX / "wrong_0904.json").read_text(encoding="utf-8"))["rows"]


@pytest.fixture
def st(monkeypatch):
    s = netmock.FakeState()
    monkeypatch.setattr(ris_emit, "STATE", s)
    return s


def pdf(path, text="a test file"):
    doc = fitz.open()
    doc.new_page().insert_text((72, 72), text[:90])
    path.write_bytes(doc.tobytes())
    doc.close()


def orphan(lib, fn, text=None, sidecar=True, extra=None):
    pdf(lib / fn)
    sc = lib / (fn[:-4] + ".fulltext.json")
    if sidecar:
        d = {"doi": ""}
        if text is not None:
            d["text"] = text
        d.update(extra or {})
        sc.write_text(json.dumps(d), encoding="utf-8")
    return str(sc)


def work(doi, title, family="", year="", score=50.0, typ="journal-article", given="", **kw):
    it = {"DOI": doi, "title": [title], "type": typ, "score": score}
    if family:
        it["author"] = [{"family": family, "given": given}]
    if year:
        it["published-print"] = {"date-parts": [[int(year)]]}
    it.update(kw)
    return it


def stub_search(monkeypatch, items, calls=None):
    def fake(query_str, top_n=F.ROWS, retries=3):
        if calls is not None:
            calls.append((query_str, top_n))
        return copy.deepcopy(items), "OK"
    monkeypatch.setattr(F, "crossref_query", fake)


def no_front_matter(monkeypatch):
    def boom(doi):
        raise AssertionError(f"resolve_meta called for {doi}")
    monkeypatch.setattr(ris_emit, "resolve_meta", boom)


# ---------------------------------------------------------------- V3-N2 near twins -> AMBIG
@pytest.mark.parametrize("case", NEAR["cases"], ids=lambda c: c["file"].split("_")[1])
def test_v3_n2_near_twin_fixtures_go_to_ambig(case, tmp_path, monkeypatch):
    """limma (F1000 v1/v2, ratio 1.0002) and Impellizzeri (Part 2 over Part I, 1.0114): the 09-04
    run wrote one of them at HIGH. The right title is in the file's text, the author and the year
    match: still AMBIG, and nothing is written with --execute."""
    right_title = {"Law": "RNA-seq analysis is easy as 1-2-3 with limma, Glimma and edgeR",
                   "Impellizzeri": "Training Load and Its Role in Injury Prevention, Part I: Back to the Future"}
    author = case["file"].split("_")[1]
    lib = tmp_path / "lib"
    lib.mkdir()
    sc = orphan(lib, case["file"], text=f"{right_title[author]}\n{author} et al.\n")
    calls = []
    stub_search(monkeypatch, case["items"], calls)
    no_front_matter(monkeypatch)
    row, meta = F.process_orphan(case["file"], sc, json.loads(Path(sc).read_text(encoding="utf-8")))
    assert row["status"] == "AMBIG"
    assert calls == [(case["query"], 8)]                      # rows=8 (2026-08-17 gotchas)
    before = Path(sc).read_bytes()
    t = F.run_project("research_x", lib, type("A", (), {"execute": True, "limit": None, "report_dir": str(tmp_path / "r")})())
    assert t["applied"] == 0 and Path(sc).read_bytes() == before
    assert not (lib / (case["file"][:-4] + ".ris")).exists()


LIVE = json.loads((FIX / "near_twins_live.json").read_text(encoding="utf-8"))


@pytest.mark.parametrize("case", LIVE["cases"], ids=lambda c: c["query"].split()[0])
def test_v3_n2_live_responses_go_to_ambig(case):
    """The same two searches live on 2026-10-05 (rows=8). limma's order flipped again (v2 over v1;
    v1 was first on 09-25), and F1000 v2/v3 carry `update-to` of type new_version: a version is not
    a notice, so it stays a candidate and the tie stays AMBIG."""
    author = case["query"].split()[0]
    year = case["query"].split()[-1]
    title = {"Law": "RNA-seq analysis is easy as 1-2-3 with limma, Glimma and edgeR",
             "Impellizzeri": "Training Load and Its Role in Injury Prevention, Part I: Back to the Future"}[author]
    pref, _ = F.filter_by_type(case["items"])
    label, m, s1, s2 = F.score_match(year, author, pref, case["items"], evidence=title, query=case["query"])
    assert label == "AMBIG" and s1 / s2 < F.TIE_MARGIN
    assert int(case["rows"]) == F.ROWS
    reasons = {it["DOI"]: F.excluded_reason(it, case["query"]) for it in case["items"]}
    if author == "Law":
        assert reasons["10.12688/f1000research.9005.2"] == ""                 # new_version: kept
        assert all(r == "type:peer-review" for d, r in reasons.items() if d.startswith("10.5256/"))
    else:
        assert reasons["10.4085/1062-6050-1006-21"].startswith("title:corrigendum")


def test_live_muendel_record_still_carries_the_entity_and_decodes():
    """Crossref still serves `M&uuml;ndel` (live 2026-10-05): crossref_meta decodes it."""
    m = ris_emit.crossref_meta(LIVE["muendel"])
    assert LIVE["muendel"]["author"][0]["family"] == "M&uuml;ndel"
    assert m["lastname"] == "Mündel" and m["year"] == "2008"


def test_impellizzeri_corrigendum_is_excluded_not_a_candidate():
    case = next(c for c in NEAR["cases"] if "Impellizzeri" in c["file"])
    corr = NEAR["impellizzeri_corrigendum"]
    assert F.excluded_reason(corr, case["query"]).startswith("title:corrigendum")


def test_near_twin_promotion_is_gone_even_for_a_distant_runner_up():
    """A sibling outside the tie margin does not block HIGH; one inside it always does."""
    t = "Training load and its role in injury prevention part one back to the future"
    items = [work("10.1/p1", t, "Impellizzeri", "2020", 60.0), work("10.1/p2", t + " two", "Impellizzeri", "2020", 40.0)]
    assert F.score_match("2020", "Impellizzeri", items, items, evidence=t)[0] == "HIGH"
    items[1]["score"] = 58.0
    assert F.score_match("2020", "Impellizzeri", items, items, evidence=t)[0] == "AMBIG"


# ---------------------------------------------------------------- I15: generic titles rejected
NASA_TITLE = "National Aeronautics and Space Administration"


@pytest.mark.parametrize("fn", ["2019_NASA_HumanResearchProgramEvidenceReport.pdf",
                                "2015_NASA_RiskInjuryCompromisedPerformanceDue.pdf",
                                "2021_NASA_SpaceflightAssociatedNeuroOcularSyndrome.pdf"])
def test_nasa_reports_are_rejected(fn, tmp_path, monkeypatch, st):
    """09-06 (I15): three NASA reports matched 10.1086/108312, titled "National Aeronautics and
    Space Administration", a string in every NASA document. Five words: not strict, so never HIGH
    (and not MED_TITLE_STRONG), even with the year matching and no competitor."""
    year = fn[:4]
    lib = tmp_path / "lib"
    lib.mkdir()
    sc = orphan(lib, fn, text=f"{NASA_TITLE}\nLyndon B. Johnson Space Center\nHouston, Texas\n{year}\n")
    stub_search(monkeypatch, [work("10.1086/108312", NASA_TITLE, year=year, score=80.0)])
    no_front_matter(monkeypatch)
    row, _ = F.process_orphan(fn, sc, json.loads(Path(sc).read_text(encoding="utf-8")))
    assert row["status"] == "LOW_TITLE_ONLY"
    t = F.run_project("research_x", lib, type("A", (), {"execute": True, "limit": None, "report_dir": str(tmp_path / "r")})())
    assert t["applied"] == 0
    assert json.loads(Path(sc).read_text(encoding="utf-8"))["doi"] == ""


def test_strength_and_conditioning_chapter_is_rejected(tmp_path, monkeypatch, st):
    """09-06: a Carpinelli article matched a book chapter titled "Strength and Conditioning". The
    hardest form: the chapter's author and year match the file and the phrase is in its text (the
    journal name). Too short to be strict: review only."""
    fn = "2011_Carpinelli_AssessmentOneRepetitionMaximum.pdf"
    lib = tmp_path / "lib"
    lib.mkdir()
    sc = orphan(lib, fn, text="Journal of Strength and Conditioning Research\nAssessment of one repetition maximum\n")
    stub_search(monkeypatch, [work("10.1007/978-0-387-00000-0_1", "Strength and Conditioning", "Carpinelli", "2011",
                                   90.0, typ="book-chapter")])
    no_front_matter(monkeypatch)
    row, _ = F.process_orphan(fn, sc, json.loads(Path(sc).read_text(encoding="utf-8")))
    assert row["status"] not in F.WRITABLE and row["status"] == "MED_AUTHOR_ONLY"
    assert F.strict_title("Strength and Conditioning") == ""
    assert F.strict_title(NASA_TITLE) == ""                  # 45 characters but 5 words
    assert F.strict_title("RNA-seq analysis is easy as 1-2-3 with limma, Glimma and edgeR")


# ---------------------------------------------------------------- exclusions (08-17 gotchas)
def test_faculty_opinions_records_are_skipped_and_the_article_wins():
    """Subramanian 2017: three Faculty Opinions records at ranks 0-2, the article at rank 3."""
    t = "A Next Generation Connectivity Map: L1000 Platform and the First 1,000,000 Profiles"
    items = [work(f"10.3410/f.73219433{i}.79358509{i}", "Faculty Opinions recommendation of " + t, score=60 - i)
             for i in range(3)] + [work("10.1016/j.cell.2017.10.049", t, "Subramanian", "2017", 50.0)]
    pref, _ = F.filter_by_type(items)
    label, m, _, _ = F.score_match("2017", "Subramanian", pref, items, evidence=t, query="Subramanian L1000 2017")
    assert (label, m["doi"]) == ("HIGH", "10.1016/j.cell.2017.10.049")


def test_every_hit_excluded_is_excluded_only_never_a_fallback():
    t = "A Next Generation Connectivity Map: L1000 Platform and the First 1,000,000 Profiles"
    items = [work("10.3410/f.1.2", "Faculty Opinions recommendation of " + t, score=60)]
    label, m, _, _ = F.score_match("2017", "Subramanian", items, items, evidence=t)
    assert label == "EXCLUDED_ONLY" and m is None


@pytest.mark.parametrize("item,why", [
    (work("10.5256/f1000research.9688.r14439", "Peer Review Report For: x", typ="peer-review"), "type:peer-review"),
    (work("10.1/n", "Notice", update_to=None), ""),
    (work("10.3410/f.732194331.793585095", "A next generation connectivity map"), "prefix:10.3410"),
    (dict(work("10.1/c", "Some title"), **{"update-to": [{"type": "correction", "DOI": "10.1/x"}]}), "update-notice:correction"),
    (dict(work("10.1/v", "Some title"), **{"update-to": [{"type": "new_version", "DOI": "10.1/x"}]}), ""),
    (dict(work("10.1/r", "Some title"), relation={"is-comment-on": [{"id": "10.1/x"}]}), "relation:is-comment-on"),
    (dict(work("10.1/h", "Some title"), relation={"has-review": [{"id": "10.1/x"}]}), ""),
    (work("10.1/e", "Erratum to: heat acclimation"), "title:erratum"),
    (work("10.1/k", "Comment on: Sports dietitians"), "title:comment on"),
])
def test_excluded_reason(item, why):
    item.pop("update_to", None)
    assert F.excluded_reason(item) == why


def test_title_guard_yields_when_the_query_asks_for_the_phrase():
    item = work("10.1/k", "Comment on: Sports dietitians and ultra sports science")
    assert F.excluded_reason(item, query="Smith Comment on Sports dietitians")== ""


# ---------------------------------------------------------------- the strict rule
STRICT = "Heat acclimation decay and re-induction: a systematic review and meta-analysis"


def test_strict_title_author_year_is_high_and_short_or_absent_title_is_not():
    items = [work("10.1/a", STRICT, "Daanen", "2018", 70.0), work("10.1/b", "Other heat paper", "Smith", "2017", 30.0)]
    assert F.score_match("2018", "Daanen", items, items, evidence=f"Sports Med\n{STRICT}\nDaanen HAM")[0] == "HIGH"
    assert F.score_match("2018", "Daanen", items, items, evidence="unrelated text")[0] == "MED_AUTHOR_ONLY"
    assert F.score_match("2017", "Daanen", items, items, evidence=STRICT)[0] == "MED_AUTHOR_ONLY"   # year off
    assert F.score_match("2018", "Smith", items, items, evidence=STRICT)[0] == "MED_TITLE_STRONG"   # author off


def test_year_comes_from_crossref_meta_print_first():
    """The year is ris_emit.crossref_meta's (CROSSREF_DATE_ORDER): the issue's print date beats an
    online-first date; the old loop read published-online first when print was absent."""
    it = {"DOI": "10.1/y", "title": [STRICT], "type": "journal-article", "score": 10,
          "author": [{"family": "Daanen"}], "published-online": {"date-parts": [[2017, 11]]},
          "journal-issue": {"published-print": {"date-parts": [[2018, 3]]}}}
    assert F.extract_metadata(it)["year"] == "2018"


def test_family_name_is_decoded_and_the_stem_is_ascii_mundel(tmp_path, st):
    """`M&uuml;ndel` from Crossref: the sidecar, the emitted .ris and any stem read Mündel / Mundel,
    never Muumlndel (2026-09-17 intake)."""
    it = work("10.1159/000151554", "Exercise heat stress: thermoregulation and hydration in the heat",
              "M&uuml;ndel", "2008", given="Toby")
    m = F.extract_metadata(it)
    assert m["first_family"] == "Mündel" and m["authors"][0]["surname"] == "Mündel"
    assert ris_emit.canonical_stem(m["year"], m["first_family"], m["title"]).startswith("2008_Mundel_")
    lib = tmp_path / "lib"
    lib.mkdir()
    sc = orphan(lib, "2008_Mundel_ExerciseHeatStress.pdf")
    sd = json.loads(Path(sc).read_text(encoding="utf-8"))
    assert F.apply_match(str(lib / "2008_Mundel_ExerciseHeatStress.pdf"), sc, sd, m) == (True, "sidecar+ris")
    ris = (lib / "2008_Mundel_ExerciseHeatStress.ris").read_text(encoding="utf-8")
    assert "AU  - Mündel, Toby" in ris and "uuml" not in ris
    assert json.loads(Path(sc).read_text(encoding="utf-8"))["authors"][0]["surname"] == "Mündel"


# ---------------------------------------------------------------- DEC-20: never overwrite a .ris DOI
def _sha(p):
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def _accepted(doi="10.1234/match.1"):
    return F.extract_metadata(work(doi, STRICT, "Daanen", "2018"))


@pytest.mark.parametrize("ris_doi,key", [("10.1080/23328940.2019.1666624", "doi_unverified_match"),
                                          ("10.1101/2022.05.09.491211", "doi_published"),
                                          ("10.21203/rs.3.rs-4548154/v1", "doi_published"),
                                          ("10.1101/gr.123456.111", "doi_unverified_match")])
def test_a_different_ris_doi_is_kept_and_the_match_stored_aside(ris_doi, key, tmp_path, st):
    lib = tmp_path / "lib"
    lib.mkdir()
    fn = "2018_Daanen_HeatAcclimationDecay.pdf"
    sc = orphan(lib, fn)
    ris = lib / (fn[:-4] + ".ris")
    ris.write_text(f"TY  - JOUR\nTI  - Curated\nDO  - {ris_doi}\nER  - \n", encoding="utf-8")
    before = _sha(ris)
    sd = json.loads(Path(sc).read_text(encoding="utf-8"))
    assert F.apply_match(str(lib / fn), sc, sd, _accepted()) == (True, key)
    after = json.loads(Path(sc).read_text(encoding="utf-8"))
    assert _sha(ris) == before
    assert after["doi"] == ris_doi and after[key] == "10.1234/match.1" and "DEC-20" in after["doi_note"]
    assert not after.get("title")                    # the other record's fields are not copied in


def test_same_or_no_ris_doi_fills_the_sidecar_and_keeps_the_ris(tmp_path, st):
    lib = tmp_path / "lib"
    lib.mkdir()
    for fn, line in (("2018_A_X.pdf", "DO  - 10.1234/match.1\n"), ("2018_B_X.pdf", "")):
        sc = orphan(lib, fn)
        ris = lib / (fn[:-4] + ".ris")
        ris.write_text(f"TY  - JOUR\nTI  - Curated\n{line}ER  - \n", encoding="utf-8")
        before = _sha(ris)
        written, action = F.apply_match(str(lib / fn), sc, json.loads(Path(sc).read_text(encoding="utf-8")), _accepted())
        assert written and _sha(ris) == before
        assert json.loads(Path(sc).read_text(encoding="utf-8"))["doi"] == "10.1234/match.1"
        assert action == ("sidecar" if line else "sidecar; the .ris has no DOI and is kept")


def test_no_ris_emits_one_through_write_ris_and_records_it(tmp_path, st):
    lib = tmp_path / "lib"
    lib.mkdir()
    fn = "2018_Daanen_HeatAcclimationDecay.pdf"
    sc = orphan(lib, fn)
    assert F.apply_match(str(lib / fn), sc, json.loads(Path(sc).read_text(encoding="utf-8")), _accepted()) == \
        (True, "sidecar+ris")
    ris = lib / (fn[:-4] + ".ris")
    assert "DO  - 10.1234/match.1" in ris.read_text(encoding="utf-8")
    assert ris_emit.ris_owner(str(ris)) == "pipeline"            # DEC-29 manifest


def test_replay_of_the_0904_wrong_matches_overwrites_no_ris(tmp_path, monkeypatch, st):
    """I15 / T9 acceptance: the 11 wrong 09-04 matches replayed through --execute. Each file keeps its
    .ris byte for byte, its sidecar DOI is the .ris DOI, and the match is stored aside."""
    lib = tmp_path / "lib"
    lib.mkdir()
    by_file = {}
    for r in WRONG:
        fn = r["filename"]
        year, author, _t, _k = F.parse_filename_hints(fn)
        title = r["ris_title"] if len(F.strict_title(r["ris_title"])) else STRICT
        orphan(lib, fn, text=f"{title}\n{author}\n")
        (lib / (fn[:-4] + ".ris")).write_text(f"TY  - JOUR\nTI  - {r['ris_title']}\nDO  - {r['ris_doi']}\nER  - \n",
                                             encoding="utf-8")
        by_file[fn] = (work(r["match_doi"], title, author, year or "2020", 100.0), r)

    def fake(query_str, top_n=F.ROWS, retries=3):
        for fn, (item, _r) in by_file.items():
            if F.build_query_string(*F.parse_filename_hints(fn)[:3]) == query_str:
                return [copy.deepcopy(item)], "OK"
        raise AssertionError(query_str)
    monkeypatch.setattr(F, "crossref_query", fake)
    no_front_matter(monkeypatch)
    hashes = {p.name: _sha(p) for p in lib.glob("*.ris")}
    t = F.run_project("research_x", lib, type("A", (), {"execute": True, "limit": None, "report_dir": str(tmp_path / "r")})())
    assert t["attempted"] == len(WRONG) and t["applied"] >= 1
    assert {p.name: _sha(p) for p in lib.glob("*.ris")} == hashes          # 0 .ris overwrites
    for fn, (_item, r) in by_file.items():
        sd = json.loads((lib / (fn[:-4] + ".fulltext.json")).read_text(encoding="utf-8"))
        if sd["doi"]:
            assert sd["doi"] == r["ris_doi"]
            key = "doi_published" if F.is_preprint_doi(r["ris_doi"]) else "doi_unverified_match"
            assert sd[key] == r["match_doi"]


# ---------------------------------------------------------------- front matter first
def test_front_matter_doi_is_taken_first(tmp_path, monkeypatch, st):
    fn = "2018_Daanen_HeatAcclimationDecay.pdf"
    lib = tmp_path / "lib"
    lib.mkdir()
    sc = orphan(lib, fn, text=f"Sports Med (2018) 48:409\nhttps://doi.org/10.1007/s40279-017-0808-x\n{STRICT}\n")
    seen = []

    def resolve(doi):
        seen.append(doi)
        return ({"doi": doi, "title": STRICT, "year": "2018", "lastname": "Daanen",
                 "authors": [{"family": "Daanen", "given": "Hein"}], "container": "Sports Med",
                 "type": "journal-article"}, "crossref")
    monkeypatch.setattr(ris_emit, "resolve_meta", resolve)
    monkeypatch.setattr(F, "crossref_query", lambda *a, **k: (_ for _ in ()).throw(AssertionError("searched")))
    row, m = F.process_orphan(fn, sc, json.loads(Path(sc).read_text(encoding="utf-8")))
    assert (row["status"], row["basis"], m["doi"]) == ("HIGH", "front_matter_doi", "10.1007/s40279-017-0808-x")
    assert seen == ["10.1007/s40279-017-0808-x"]


def test_a_contradicted_front_matter_doi_falls_through_to_the_search(tmp_path, monkeypatch, st):
    fn = "2018_Daanen_HeatAcclimationDecay.pdf"
    lib = tmp_path / "lib"
    lib.mkdir()
    sc = orphan(lib, fn, text=f"doi:10.1016/j.other.2009.01.001\n{STRICT}\nDaanen\n")
    monkeypatch.setattr(ris_emit, "resolve_meta", lambda d: ({"doi": d, "title": "Some other paper", "year": "2009",
                                                              "lastname": "Other", "authors": [{"family": "Other"}]},
                                                             "crossref"))
    stub_search(monkeypatch, [work("10.1/a", STRICT, "Daanen", "2018", 70.0)])
    row, m = F.process_orphan(fn, sc, json.loads(Path(sc).read_text(encoding="utf-8")))
    assert row["status"] == "HIGH" and row["basis"].startswith("strict_title") and m["doi"] == "10.1/a"
    assert row["front_matter_doi"] == "10.1016/j.other.2009.01.001" and "unconfirmed" in row["note"]


def test_metadata_unavailable_is_an_error_row_not_no_match(tmp_path, monkeypatch, st):
    fn = "2018_Daanen_HeatAcclimationDecay.pdf"
    lib = tmp_path / "lib"
    lib.mkdir()
    sc = orphan(lib, fn, text="doi:10.1007/s40279-017-0808-x\n")

    def down(doi):
        raise ris_emit.MetadataUnavailable("crossref", Outcome(Kind.OUTAGE, 503, "api.crossref.org", "HTTP 503", 1, 1.0))
    monkeypatch.setattr(ris_emit, "resolve_meta", down)
    row, m = F.process_orphan(fn, sc, json.loads(Path(sc).read_text(encoding="utf-8")))
    assert row["status"].startswith("ERR_META_UNAVAILABLE") and m is None


# ---------------------------------------------------------------- identity flags
@pytest.mark.parametrize("where", ["identity", "fulltext"])
def test_identity_flagged_orphan_is_skipped(where, tmp_path, monkeypatch, st):
    """A flagged file's name and hints describe the paper that was asked for; filling it would
    write that paper's DOI onto another work."""
    fn = "2018_Daanen_HeatAcclimationDecay.pdf"
    lib = tmp_path / "lib"
    lib.mkdir()
    extra = {"identity": "FLAG"} if where == "fulltext" else None
    sc = orphan(lib, fn, text=STRICT, extra=extra)
    if where == "identity":
        (lib / (fn[:-4] + ".identity.json")).write_text(json.dumps({"identity": "FLAG"}), encoding="utf-8")
    stub_search(monkeypatch, [work("10.1/a", STRICT, "Daanen", "2018")])
    no_front_matter(monkeypatch)
    row, m = F.process_orphan(fn, sc, json.loads(Path(sc).read_text(encoding="utf-8")))
    assert row["status"] == "SKIP_IDENTITY_FLAG" and m is None


# ---------------------------------------------------------------- network: litpipe.net, rows=8
def test_crossref_query_goes_through_litpipe_net(net_env, mock_server, monkeypatch):
    srv = mock_server()
    srv.script("/works", netmock.Reply(200, json.dumps({"message": {"items": [work("10.1/a", STRICT)]}}),
                                       {"Content-Type": "application/json"}))
    monkeypatch.setattr(F, "CROSSREF", srv.url("/works"))
    items, status = F.crossref_query("Daanen heat acclimation 2018")
    assert status == "OK" and items[0]["DOI"] == "10.1/a"
    hit = srv.hits[0]
    assert hit.query["query.bibliographic"] == ["Daanen heat acclimation 2018"] and hit.query["rows"] == ["8"]
    assert hit.headers.get("User-Agent", "").startswith("literature-pipeline/")
    assert not hasattr(F, "EMAIL") and not hasattr(F, "UA")       # DEC-13


def test_a_failed_search_is_an_error_status_never_an_empty_ok(net_env, mock_server, monkeypatch):
    srv = mock_server()
    srv.script("/works", netmock.Reply(503, "down"))
    monkeypatch.setattr(F, "CROSSREF", srv.url("/works"))
    items, status = F.crossref_query("x y z")
    assert items == [] and status.startswith("ERR_OUTAGE")


# ---------------------------------------------------------------- the CLI: dry run, reports, exits
@pytest.fixture
def registry(tmp_path, monkeypatch):
    root = tmp_path / "Projects"
    lib = root / "research_x" / "literature"
    lib.mkdir(parents=True)
    cfg = {"root": str(root), "state_dir": str(tmp_path / "state"),
           "projects": {"research_x": {"lib_dir": "literature", "active": True}}}
    path = tmp_path / "projects.json"
    path.write_text(json.dumps(cfg), encoding="utf-8")
    monkeypatch.setattr(F, "CONFIG_PATH", path)
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    return lib


def test_dry_run_writes_nothing(registry, tmp_path, monkeypatch, st, capsys):
    fn = "2018_Daanen_HeatAcclimationDecay.pdf"
    sc = orphan(registry, fn, text=STRICT)
    stub_search(monkeypatch, [work("10.1/a", STRICT, "Daanen", "2018")])
    no_front_matter(monkeypatch)
    before = sorted(p.name for p in registry.iterdir()), Path(sc).read_bytes()
    assert F.main(["--project", "research_x"]) == 0
    assert (sorted(p.name for p in registry.iterdir()), Path(sc).read_bytes()) == before
    assert st.kv == {}
    assert "none written (dry run" in capsys.readouterr().out


def test_report_dir_uses_run_id_names(registry, tmp_path, monkeypatch, st):
    orphan(registry, "2018_Daanen_HeatAcclimationDecay.pdf", text=STRICT)
    stub_search(monkeypatch, [work("10.1/a", STRICT, "Daanen", "2018")])
    no_front_matter(monkeypatch)
    out = tmp_path / "reports"
    assert F.main(["--project", "research_x", "--report-dir", str(out)]) == 0
    assert F.main(["--project", "research_x", "--report-dir", str(out)]) == 0
    names = sorted(p.name for p in out.iterdir() if p.name.startswith("_doi_fill_report."))
    first = names[-1]
    assert len(names) == 2 and names[0] == first[:-4] + ".2.csv"   # a same-day re-run never replaces one
    with open(out / first, encoding="utf-8") as fh:
        row = next(csv.DictReader(fh))
    assert row["status"] == "HIGH" and row["basis"] == "strict_title+author+year"


def test_execute_writes_high_and_the_ris(registry, tmp_path, monkeypatch, st):
    fn = "2018_Daanen_HeatAcclimationDecay.pdf"
    sc = orphan(registry, fn, text=STRICT)
    stub_search(monkeypatch, [work("10.1234/heat.2018.1", STRICT, "Daanen", "2018")])
    no_front_matter(monkeypatch)
    assert F.main(["--project", "research_x", "--execute"]) == 0
    assert json.loads(Path(sc).read_text(encoding="utf-8"))["doi"] == "10.1234/heat.2018.1"
    assert (registry / (fn[:-4] + ".ris")).exists()
    assert list(registry.glob("_doi_fill_applied.*.csv"))


def test_min_confidence_below_high_is_retired(registry, tmp_path, monkeypatch, st, capsys):
    fn = "2018_Daanen_HeatAcclimationDecay.pdf"
    sc = orphan(registry, fn, text="no title here")
    stub_search(monkeypatch, [work("10.1/a", STRICT, "Daanen", "2018")])          # MED_AUTHOR_ONLY
    no_front_matter(monkeypatch)
    assert F.main(["--project", "research_x", "--execute", "--min-confidence", "MED_AUTHOR_ONLY"]) == 0
    assert json.loads(Path(sc).read_text(encoding="utf-8"))["doi"] == ""
    assert "--min-confidence MED_AUTHOR_ONLY is retired" in capsys.readouterr().out


def test_config_errors_exit_1(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(F, "CONFIG_PATH", tmp_path / "missing.json")
    assert F.main(["--project", "research_x"]) == 1
    (tmp_path / "p.json").write_text(json.dumps({"projects": {}}), encoding="utf-8")
    monkeypatch.setattr(F, "CONFIG_PATH", tmp_path / "p.json")
    assert F.main(["--project", "nope"]) == 1
    with pytest.raises(SystemExit) as e:
        F.main([])
    assert e.value.code == 1                                       # a usage error exits 1


def test_a_failed_lookup_exits_2_with_a_step_summary(registry, monkeypatch, st, capsys):
    orphan(registry, "2018_Daanen_HeatAcclimationDecay.pdf", text=STRICT)
    monkeypatch.setattr(F, "crossref_query", lambda *a, **k: ([], "ERR_OUTAGE: HTTP 503"))
    no_front_matter(monkeypatch)
    assert F.main(["--project", "research_x"]) == 2
    last = capsys.readouterr().out.strip().splitlines()[-1]
    assert last.startswith(F.SUMMARY_MARKER)
    s = json.loads(last[len(F.SUMMARY_MARKER):])
    assert s["reasons"] and s["aborted"] is None and s["transport_failures"] == 1


def test_the_hint_names_force_with_the_curated_caveat():
    """W2b forward: `backfill_ris --overwrite` no longer refreshes a pre-DEC-29 (unrecorded) .ris."""
    import inspect
    doc = F.__doc__
    src = inspect.getsource(F.run_project)
    assert "--overwrite" not in doc and "--overwrite" not in src
    assert "--force" in doc and "curated" in doc and "--force" in src and "EndNote" in src
