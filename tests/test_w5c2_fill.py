"""W5-C2 step 7 (item 4; C062, C102): fill_missing_dois tries the sidecar's `doi_candidate` (the text
DOI extract_pdf_fulltext records, never as `doi`) first, verified as any candidate is; a `--dry-run`
writes nothing, the `doi_ra` cache included (ris_emit's kv writes are dropped for its duration)."""
import json
from pathlib import Path

import pytest

import fill_missing_dois as F
import ris_emit
from tests import netmock
from tests.test_rename_fill import STRICT, orphan, st, stub_search, work  # noqa: F401  (fixture)

FN = "2018_Daanen_HeatAcclimationDecay.pdf"
CAND = "10.1007/s40279-017-0808-x"


def _meta(doi, title=STRICT, year="2018", family="Daanen"):
    return ({"doi": doi, "title": title, "year": year, "lastname": family,
             "authors": [{"family": family, "given": "Hein"}], "container": "Sports Med",
             "type": "journal-article"}, "crossref")


def _orphan(tmp_path, text, cand=CAND):
    lib = tmp_path / "lib"
    lib.mkdir(exist_ok=True)
    extra = {"doi_candidate": cand, "doi_source": "text"} if cand else None
    sc = orphan(lib, FN, text=text, extra=extra)
    return lib, sc, json.loads(Path(sc).read_text(encoding="utf-8"))


def test_the_doi_candidate_is_tried_first_and_accepted_when_verified(tmp_path, monkeypatch, st):
    # the text names ANOTHER DOI first; the sidecar's candidate is asked before it, and wins
    _lib, sc, sd = _orphan(tmp_path, f"doi:10.1016/j.other.2009.01.001\n{STRICT}\n")
    seen = []

    def resolve(doi):
        seen.append(doi)
        return _meta(doi)
    monkeypatch.setattr(ris_emit, "resolve_meta", resolve)
    monkeypatch.setattr(F, "crossref_query", lambda *a, **k: (_ for _ in ()).throw(AssertionError("searched")))
    row, m = F.process_orphan(FN, sc, sd)
    assert (row["status"], row["basis"], m["doi"]) == ("HIGH", "doi_candidate", CAND)
    assert seen == [CAND] and row["doi_candidate"] == CAND


def test_a_contradicted_doi_candidate_falls_through_and_is_not_asked_twice(tmp_path, monkeypatch, st):
    _lib, sc, sd = _orphan(tmp_path, f"https://doi.org/{CAND}\n{STRICT}\n")
    seen = []

    def resolve(doi):
        seen.append(doi)
        return _meta(doi, title="Some other paper", year="2009", family="Other")
    monkeypatch.setattr(ris_emit, "resolve_meta", resolve)
    stub_search(monkeypatch, [work("10.1/a", STRICT, "Daanen", "2018", 70.0)])
    row, m = F.process_orphan(FN, sc, sd)
    assert row["status"] == "HIGH" and row["basis"].startswith("strict_title") and m["doi"] == "10.1/a"
    assert seen == [CAND]                                   # the same front-matter DOI is not looked up again
    assert f"doi_candidate {CAND} unconfirmed" in row["note"]


def test_a_sidecar_without_a_candidate_reads_the_front_matter_as_before(tmp_path, monkeypatch, st):
    _lib, sc, sd = _orphan(tmp_path, f"https://doi.org/{CAND}\n{STRICT}\n", cand=None)
    monkeypatch.setattr(ris_emit, "resolve_meta", lambda d: _meta(d))
    row, m = F.process_orphan(FN, sc, sd)
    assert (row["status"], row["basis"]) == ("HIGH", "front_matter_doi") and row["doi_candidate"] == ""


def test_doi_candidate_is_the_last_report_column():
    assert F.FIELDNAMES[-1] == "doi_candidate" and F.FIELDNAMES[:5] == ["filename", "parse_class", "parsed_year",
                                                                       "parsed_author", "parsed_title"]


# ------------------------------------------------------------------ the dry run writes no state
@pytest.mark.parametrize("execute", [False, True])
def test_a_dry_run_writes_no_doi_ra_cache(tmp_path, monkeypatch, execute):
    lib, sc, _sd = _orphan(tmp_path, f"https://doi.org/{CAND}\n{STRICT}\n")
    real = netmock.FakeState()
    monkeypatch.setattr(ris_emit, "STATE", real)

    def resolve(doi):
        ris_emit._kv_set("doi_ra", doi.split("/", 1)[0], "crossref")      # what doi_ra() does
        assert ris_emit._kv_get("doi_ra", "10.1007") in (None, "crossref")
        return _meta(doi)
    monkeypatch.setattr(ris_emit, "resolve_meta", resolve)
    monkeypatch.setattr(ris_emit, "write_ris", lambda *a, **k: True)
    before = Path(sc).read_bytes()
    args = type("A", (), {"execute": execute, "limit": None, "report_dir": None})()
    t = F.run_project("research_x", lib, args)
    assert t["high"] == 1
    assert ris_emit.STATE is real                                          # restored
    cached = real.kv_get("doi_ra", "10.1007")
    if execute:
        assert cached == "crossref" and Path(sc).read_bytes() != before
    else:
        assert cached is None and Path(sc).read_bytes() == before
        assert sorted(p.name for p in lib.iterdir()) == sorted([FN, Path(sc).name])   # nothing new


def test_the_wrapper_passes_reads_through():
    inner = netmock.FakeState()
    inner.kv_set("doi_ra", "10.1152", "crossref")
    w = F._NoKVWrites(inner)
    assert w.kv_get("doi_ra", "10.1152") == "crossref"
    w.kv_set("doi_ra", "10.1186", "crossref")
    assert inner.kv_get("doi_ra", "10.1186") is None
