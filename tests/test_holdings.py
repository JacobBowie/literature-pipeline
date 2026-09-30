"""litpipe.holdings: the portfolio holdings map (dispatch 0.5, W1-D2).

Fixtures in tests/fixtures/W1-D2/lib_real/ are real library files (a PMC text-only sidecar with its
text trimmed, and a pipeline-written .ris), copied read-only from a consumer library. Every test
runs against a temp projects root and a temp state_dir; nothing here touches the live registry.
"""
import json
import os
import shutil
from pathlib import Path

import pytest

import lit_util
from litpipe import config, holdings

FIX = Path(__file__).parent / "fixtures" / "W1-D2"
SIDECAR = "2001_Unknown_AlveolarEpithelialTypeIiCellDefender.fulltext.json"
SIDECAR_DOI = "10.1186/rr36"
RIS = "1972_Keys_IndicesRelativeWeightObesity.ris"
RIS_DOI = "10.1016/0021-9681(72)90027-6"


@pytest.fixture(autouse=True)
def root(tmp_path, monkeypatch):
    """A temp projects root and a temp state_dir (via a temp CONFIG_PATH)."""
    r = tmp_path / "Projects"
    r.mkdir()
    cfgp = tmp_path / "projects.json"
    cfgp.write_text(json.dumps({"state_dir": str(tmp_path / "state"), "projects": {}}), encoding="utf-8")
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", r)
    monkeypatch.setattr(config, "CONFIG_PATH", cfgp)
    return r


def _reg(tmp_path, **libs):
    return {"state_dir": str(tmp_path / "state"),
            "projects": {k: {"lib_dir": v} for k, v in libs.items()}}


def _lib(root, rel):
    p = root / rel
    p.mkdir(parents=True, exist_ok=True)
    return p


def _pdf(path):
    path.write_bytes(b"%PDF-1.4\n% fixture\n")
    return path


def _copy(name, lib, new_name=None):
    return Path(shutil.copy2(FIX / "lib_real" / name, lib / (new_name or name)))


def _sidecar(path, doi, text="Body text.", pmcid="PMC1"):
    path.write_text(json.dumps({"pmcid": pmcid, "doi": doi, "title": "T", "text": text}), encoding="utf-8")
    return path


# ---------------------------------------------------------------- DOI normalisation (REG-I25)
@pytest.mark.parametrize("raw", [
    "10.1186/rr36", "10.1186/RR36", "`10.1186/rr36`", "doi:10.1186/rr36", "DOI: 10.1186/rr36",
    "https://doi.org/10.1186/rr36", "http://dx.doi.org/10.1186/RR36", "https://www.doi.org/10.1186/rr36",
    "https://doi.org/10.1186%2Frr36", "10.1186/rr36.", "10.1186/rr36;", "<https://doi.org/10.1186/rr36>",
])
def test_normalise_doi_any_form(raw):
    assert holdings.normalise_doi(raw) == SIDECAR_DOI


def test_normalise_keeps_balanced_parentheses_and_drops_a_link_closer():
    assert holdings.normalise_doi(RIS_DOI.upper()) == RIS_DOI
    assert holdings.normalise_doi(RIS_DOI + ")") == RIS_DOI  # a markdown link's closing paren
    assert holdings.normalise_doi("10.1016/s2213-8587(18)30137-2") == "10.1016/s2213-8587(18)30137-2"


@pytest.mark.parametrize("raw", ["", None, "not a doi", "10.x/a", "PMC12345", "https://example.org/x"])
def test_normalise_rejects_non_dois(raw):
    assert holdings.normalise_doi(raw) == ""


def test_extract_dois_stops_at_a_url_query():
    """The real Unpaywall error text puts `?email=` right after the DOI; the email is never part of
    an extracted DOI (found against consumer residual lines, 2026-09-30)."""
    line = (FIX / "md_residuals_email.md").read_text(encoding="utf-8").splitlines()[0]
    assert "?email=" in line
    dois = holdings.extract_dois(line)
    assert dois and all("?" not in d and "email" not in d and "example" not in d for d in dois)
    assert len(set(dois)) == 1   # the link, the link text and the URL path are the same DOI


@pytest.mark.parametrize("fixture,expected", [
    ("md_link_form.md", 4), ("md_backtick_link.md", 4), ("md_ill_backtick.md", 5), ("md_blocked_oa.md", 4),
])
def test_extract_dois_one_per_real_queue_line(fixture, expected):
    """Real queue .md lines in every form the portfolio uses (link, backtick-in-link, backtick,
    a consumer's worklist form): each line yields exactly one DOI, even when it appears twice."""
    lines = (FIX / fixture).read_text(encoding="utf-8").splitlines()
    per_line = [holdings.extract_dois(l) for l in lines]
    assert len(per_line) == expected
    assert all(len(d) == 1 for d in per_line), per_line
    assert all(d[0] == d[0].lower() and not d[0].endswith((")", ".", "`")) for d in per_line)


# ---------------------------------------------------------------- the map
def test_text_only_sidecar_is_a_holding(tmp_path, root):
    """Acceptance: a DOI held only as a text-only sidecar (no PDF) is found."""
    lib = _lib(root, "A/lib")
    sc = _copy(SIDECAR, lib)
    hm = holdings.build(_reg(tmp_path, A="lib"))
    assert hm.where(SIDECAR_DOI) == [sc]
    assert hm.where("https://doi.org/10.1186/RR36") == [sc]
    assert SIDECAR_DOI in hm and hm.text_only(SIDECAR_DOI) and not hm.has_pdf(SIDECAR_DOI)
    (rec,) = hm.records(SIDECAR_DOI)
    assert rec.kind == holdings.TEXT_ONLY and rec.project == "A" and rec.library == root / "A" / "lib"
    assert hm.stats["dois_text_only"] == 1


def test_pdf_held_through_its_ris(tmp_path, root):
    lib = _lib(root, "A/lib")
    _copy(RIS, lib)
    pdf = _pdf(lib / RIS.replace(".ris", ".pdf"))
    hm = holdings.build(_reg(tmp_path, A="lib"))
    assert hm.where(RIS_DOI) == [pdf]
    assert hm.has_pdf(RIS_DOI) and not hm.text_only(RIS_DOI)


def test_pdf_beats_sidecar_on_the_same_base(tmp_path, root):
    lib = _lib(root, "A/lib")
    _copy(SIDECAR, lib)
    pdf = _pdf(lib / SIDECAR.replace(".fulltext.json", ".PDF"))  # upper-case suffix too
    hm = holdings.build(_reg(tmp_path, A="lib"))
    assert hm.where(SIDECAR_DOI) == [pdf]
    assert [r.kind for r in hm.records(SIDECAR_DOI)] == [holdings.PDF]


def test_empty_sidecar_and_orphan_ris_are_not_content(tmp_path, root):
    lib = _lib(root, "A/lib")
    _sidecar(lib / "x.fulltext.json", "10.1000/empty9", text="   ")
    _copy(RIS, lib)  # no PDF, no sidecar
    hm = holdings.build(_reg(tmp_path, A="lib"))
    assert hm.where("10.1000/empty9") == [] and hm.where(RIS_DOI) == []
    assert "10.1000/empty9" not in hm and len(hm) == 0
    assert [r.kind for r in hm.records("10.1000/empty9")] == [holdings.EMPTY_SIDECAR]
    assert [r.kind for r in hm.records(RIS_DOI)] == [holdings.RIS_ONLY]
    assert hm.stats["empty_sidecars"] == 1 and hm.stats["orphan_ris"] == 1


def test_multi_library_pdf_first_and_counted(tmp_path, root):
    a, b = _lib(root, "A/lib"), _lib(root, "B/papers")
    sc = _copy(SIDECAR, a)
    _sidecar(b / "held.fulltext.json", SIDECAR_DOI.upper())
    pdf = _pdf(b / "held.pdf")
    hm = holdings.build(_reg(tmp_path, A="lib", B="papers"))
    assert hm.where(SIDECAR_DOI) == [pdf, sc]
    assert hm.has_pdf(SIDECAR_DOI) and not hm.text_only(SIDECAR_DOI)
    assert hm.stats["dois_multi_library"] == 1


def test_mismatch_quarantine_and_subdirs_are_not_holdings(tmp_path, root):
    lib = _lib(root, "A/lib")
    q = lib / "_mismatch"
    q.mkdir()
    _copy(RIS, q)
    _pdf(q / RIS.replace(".ris", ".pdf"))
    hm = holdings.build(_reg(tmp_path, A="lib"))
    assert hm.records(RIS_DOI) == [] and hm.stats["files_scanned"] == 0


def test_ris_and_sidecar_disagreement_maps_both_and_is_reported(tmp_path, root):
    lib = _lib(root, "A/lib")
    _copy(RIS, lib, "base.ris")
    _sidecar(lib / "base.fulltext.json", "10.1000/other9")
    _pdf(lib / "base.pdf")
    hm = holdings.build(_reg(tmp_path, A="lib"))
    assert hm.where(RIS_DOI) == hm.where("10.1000/other9") == [lib / "base.pdf"]
    assert hm.stats["ris_sidecar_doi_disagreements"][0]["ris"] == RIS_DOI


def test_registry_forms_subprojects_duplicates_and_missing_libraries(tmp_path, root):
    _lib(root, "Parent/Sub/literature")
    _copy(SIDECAR, root / "Parent" / "Sub" / "literature")
    full = {"state_dir": str(tmp_path / "state"), "projects": {
        "Parent/Sub": {"parent": "Parent", "lib_dir": "Sub/literature"},
        "Dup": {"lib_dir": "../Parent/Sub/literature"},   # the same directory registered twice
        "Gone": {"lib_dir": "nowhere", "active": False},
        "NoLib": {"tier": 2},
    }}
    libs = holdings.libraries(full)
    assert [k for k, _, _ in libs] == ["Parent/Sub", "Gone"]
    assert libs[1][2] is False
    for reg in (full, full["projects"]):   # the whole file, or just its projects mapping
        hm = holdings.build(reg)
        assert hm.where(SIDECAR_DOI) and hm.records(SIDECAR_DOI)[0].project == "Parent/Sub"
    assert [l["exists"] for l in hm.stats["libraries"]] == [True, False]


def test_build_with_no_registry_reads_config_path(tmp_path, root):
    lib = _lib(root, "A/lib")
    _copy(SIDECAR, lib)
    config.CONFIG_PATH.write_text(json.dumps(_reg(tmp_path, A="lib")), encoding="utf-8")
    assert holdings.build().where(SIDECAR_DOI) == [lib / SIDECAR]


# ---------------------------------------------------------------- the cache (warm builds stat, not read)
def test_cache_lives_under_state_dir(tmp_path, root):
    _copy(SIDECAR, _lib(root, "A/lib"))
    hm = holdings.build(_reg(tmp_path, A="lib"))
    expected = tmp_path / "state" / holdings.CACHE_NAME
    assert Path(hm.stats["cache_path"]) == expected and expected.exists()
    assert hm.stats["cache_written"]


def test_warm_build_reads_no_unchanged_file(tmp_path, root):
    lib = _lib(root, "A/lib")
    _copy(SIDECAR, lib)
    _copy(RIS, lib)
    _pdf(lib / RIS.replace(".ris", ".pdf"))
    reg = _reg(tmp_path, A="lib")
    cold = holdings.build(reg)
    assert cold.stats["files_read"] == 2 and cold.stats["cache_hits"] == 0
    warm = holdings.build(reg)
    assert warm.stats["files_read"] == 0 and warm.stats["cache_hits"] == 2
    assert not warm.stats["cache_written"]           # nothing changed, nothing rewritten
    assert warm.where(SIDECAR_DOI) == cold.where(SIDECAR_DOI)
    assert warm.where(RIS_DOI) == cold.where(RIS_DOI)


def test_warm_build_rereads_a_changed_file(tmp_path, root):
    lib = _lib(root, "A/lib")
    sc = _sidecar(lib / "x.fulltext.json", "10.1000/old9")
    reg = _reg(tmp_path, A="lib")
    holdings.build(reg)
    _sidecar(sc, "10.1000/new-and-longer9")
    st = sc.stat()
    os.utime(sc, ns=(st.st_atime_ns, st.st_mtime_ns + 5_000_000_000))
    warm = holdings.build(reg)
    assert warm.stats["files_read"] == 1
    assert warm.where("10.1000/new-and-longer9") == [sc] and warm.where("10.1000/old9") == []


def test_warm_build_sees_a_new_pdf_without_rereading(tmp_path, root):
    """PDF presence is never cached: a PDF that arrives turns a text-only holding into a PDF one."""
    lib = _lib(root, "A/lib")
    _copy(SIDECAR, lib)
    reg = _reg(tmp_path, A="lib")
    assert holdings.build(reg).text_only(SIDECAR_DOI)
    pdf = _pdf(lib / SIDECAR.replace(".fulltext.json", ".pdf"))
    warm = holdings.build(reg)
    assert warm.stats["files_read"] == 0 and warm.where(SIDECAR_DOI) == [pdf]


def test_deleted_file_leaves_the_map_and_the_cache(tmp_path, root):
    lib = _lib(root, "A/lib")
    sc = _copy(SIDECAR, lib)
    reg = _reg(tmp_path, A="lib")
    holdings.build(reg)
    sc.unlink()
    warm = holdings.build(reg)
    assert warm.where(SIDECAR_DOI) == [] and warm.stats["cache_written"]
    cache = json.loads((tmp_path / "state" / holdings.CACHE_NAME).read_text(encoding="utf-8"))
    assert all(not v["files"] for v in cache["libraries"].values())


def test_unreadable_files_are_counted_and_retried_not_cached(tmp_path, root):
    lib = _lib(root, "A/lib")
    (lib / "bad.fulltext.json").write_text("{not json", encoding="utf-8")
    (lib / "list.fulltext.json").write_text("[1, 2]", encoding="utf-8")
    reg = _reg(tmp_path, A="lib")
    first = holdings.build(reg)
    assert len(first.stats["unreadable"]) == 2
    second = holdings.build(reg)
    assert len(second.stats["unreadable"]) == 2 and second.stats["cache_hits"] == 0


def test_corrupt_or_foreign_cache_means_a_cold_build(tmp_path, root):
    _copy(SIDECAR, _lib(root, "A/lib"))
    reg = _reg(tmp_path, A="lib")
    (tmp_path / "state").mkdir()
    cache = tmp_path / "state" / holdings.CACHE_NAME
    for junk in ("{garbage", json.dumps({"version": 999, "libraries": {}}), "[]"):
        cache.write_text(junk, encoding="utf-8")
        hm = holdings.build(reg)
        assert hm.stats["files_read"] == 1 and hm.where(SIDECAR_DOI)


def test_cache_write_failure_is_recorded_not_raised(tmp_path, root):
    _copy(SIDECAR, _lib(root, "A/lib"))
    blocker = tmp_path / "not_a_dir"
    blocker.write_text("x", encoding="utf-8")
    hm = holdings.build(_reg(tmp_path, A="lib"), cache_dir=blocker)
    assert hm.where(SIDECAR_DOI) and hm.stats["cache_error"]


def test_no_library_touches_no_state_dir(tmp_path, root, monkeypatch):
    def boom(*a, **k):
        raise AssertionError("state_dir must not be resolved when there is nothing to scan")
    monkeypatch.setattr(config, "state_dir", boom)
    hm = holdings.build({"projects": {"P": {}}})
    assert len(hm) == 0 and hm.stats["cache_path"] is None


def test_use_cache_false_reads_everything_and_writes_nothing(tmp_path, root):
    _copy(SIDECAR, _lib(root, "A/lib"))
    reg = _reg(tmp_path, A="lib")
    hm = holdings.build(reg, use_cache=False)
    assert hm.stats["files_read"] == 1 and not (tmp_path / "state").exists()


def test_holdmap_from_records_for_fakes():
    h = holdings.Holding("10.1/X", Path("a.pdf"), "P", Path("."), holdings.PDF)
    hm = holdings.HoldMap([h])
    assert hm.where("10.1/x") == [Path("a.pdf")] and len(hm) == 1
