"""An identity FLAG is a review item, never a holding (W2a verifier A, V-A2). The PMC stage records a
FLAG verdict in the PDF's `.fulltext.json` together with the queue DOI; the portfolio holdings map
and the Unpaywall stage's sidecar-DOI reader must not read that DOI as "this paper is held", or the
flagged row turns into HELD_ELSEWHERE / ALREADY_EXISTS on the next run and nobody reviews it."""
import json

import pytest

import lit_util
import unpaywall_fetch_v2 as U
from litpipe import config, holdings

DOI = "10.1234/flagged.5"


@pytest.fixture(autouse=True)
def root(tmp_path, monkeypatch):
    r = tmp_path / "Projects"
    r.mkdir()
    cfgp = tmp_path / "projects.json"
    cfgp.write_text(json.dumps({"state_dir": str(tmp_path / "state"), "projects": {}}), encoding="utf-8")
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", r)
    monkeypatch.setattr(config, "CONFIG_PATH", cfgp)
    return r


def _held(lib, base, *, identity=None, doi=DOI):
    (lib / f"{base}.pdf").write_bytes(b"%PDF-1.4\n%held\n")
    rec = {"doi": doi, "pmcid": "PMC1", "text": "Body text of the article.", "has_pdf": True}
    if identity:
        rec["identity"] = identity
    (lib / f"{base}.fulltext.json").write_text(json.dumps(rec), encoding="utf-8")
    return lib / f"{base}.pdf"


def _registry(tmp_path):
    return {"state_dir": str(tmp_path / "state"), "projects": {"teaching_a": {"lib_dir": "lib"}}}


def test_holdings_does_not_count_a_flagged_sidecar(tmp_path, root):
    lib = root / "teaching_a" / "lib"
    lib.mkdir(parents=True)
    _held(lib, "2020_Author_Flagged", identity="FLAG")
    _held(lib, "2021_Author_Verified", identity="OK", doi="10.1234/verified.6")
    for _ in range(2):                                  # the second build reads the stat-keyed cache
        hm = holdings.build(_registry(tmp_path))
        assert DOI not in hm and not hm.has_pdf(DOI)
        assert "10.1234/verified.6" in hm and hm.has_pdf("10.1234/verified.6")


def test_holdings_cache_version_moved_past_the_flag_blind_reading():
    assert holdings.CACHE_VERSION >= 3


def test_unpaywall_sidecar_doi_ignores_a_flagged_fulltext_record(tmp_path):
    flagged = _held(tmp_path, "2020_Author_Flagged", identity="FLAG")
    verified = _held(tmp_path, "2021_Author_Verified", identity="OK")
    plain = _held(tmp_path, "2022_Author_Plain")         # sidecars written before identity existed
    assert U._sidecar_doi(str(flagged)) == ""
    assert U._sidecar_doi(str(verified)) == DOI
    assert U._sidecar_doi(str(plain)) == DOI


def test_unpaywall_existing_holds_is_not_same_for_a_flagged_pmc_copy(tmp_path):
    flagged = _held(tmp_path, "2020_Author_Flagged", identity="FLAG")
    assert U.existing_holds(str(flagged), DOI, "A title") != "same"
