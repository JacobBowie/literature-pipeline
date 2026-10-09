"""An abstract-only sidecar is no holding anywhere (librarian, 2026-10-09, on live a109534).

d1eb1d5 stopped the fetch stages from reporting an abstract-only sidecar as a text-only result, but the
holdings map still classed any non-empty sidecar TEXT_ONLY. So a requeued abstract-only DOI was
skipped as already held before a stage could see it, and the instruments, the index and the walk
seeds counted it as held text. The route step (migrate) re-read the sidecar's text on disk and routed
the row TEXT_ONLY as well. Now:
- holdings has an ABSTRACT_ONLY kind, which is not content (cache version 5 re-reads old rows);
- the shared predicate (audit_portfolio.is_text_only_sidecar) excludes it and is_abstract_only_sidecar
  names it; the audit lists such sidecars in their own INFO bucket, not as orphans;
- migrate's sidecar-on-disk test ignores it;
- a PDF that arrives for it still fills it, as for a text-only holding."""
import json
from pathlib import Path

import pytest

import audit_portfolio as ap
import import_downloads as ID
import lit_util
import migrate_closed_to_md as mig
from litpipe import config, holdings
from tests.test_abstract_only_text import MARTIN

ABS_DOI, FULL_DOI = "10.1161/cir.0000000000001303", "10.5555/full.0001"
BODY = "The cohort was followed for ten years and outcomes were adjudicated blind. " * 30


@pytest.fixture
def lib(tmp_path, monkeypatch):
    root = tmp_path / "Projects"
    lib = root / "A" / "lib"
    lib.mkdir(parents=True)
    cfg = tmp_path / "projects.json"
    cfg.write_text(json.dumps({"state_dir": str(tmp_path / "state"), "projects": {}}), encoding="utf-8")
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    monkeypatch.setattr(config, "CONFIG_PATH", cfg)
    (lib / "2025_Martin_Stats.fulltext.json").write_text(
        json.dumps(dict(MARTIN, doi=ABS_DOI, pmcid="PMC12256702", has_pdf=False)), encoding="utf-8")
    (lib / "2025_Martin_Stats.ris").write_text(f"TY  - JOUR\nDO  - {ABS_DOI}\nER  - \n", encoding="utf-8")
    (lib / "2020_Full_Text.fulltext.json").write_text(json.dumps(
        {"doi": FULL_DOI, "pmcid": "PMC1", "has_pdf": False, "title": "T", "abstract": "An abstract.",
         "sections": [{"title": "Methods", "text": BODY}], "text": "# T\n\n## Methods\n\n" + BODY}), encoding="utf-8")
    return SimpleLib(tmp_path, lib)


class SimpleLib:
    def __init__(self, tmp, path):
        self.tmp, self.path = tmp, path
        self.reg = {"state_dir": str(tmp / "state"), "projects": {"A": {"lib_dir": "lib"}}}


def test_the_holdings_map_does_not_count_an_abstract_only_sidecar_as_held(lib):
    hm = holdings.build(lib.reg, cache_dir=lib.tmp / "cache")
    assert hm.where(ABS_DOI) == [] and not hm.text_only(ABS_DOI)                # not content
    (rec,) = hm.records(ABS_DOI)
    assert rec.kind == holdings.ABSTRACT_ONLY and rec.path.name == "2025_Martin_Stats.fulltext.json"
    assert hm.text_only(FULL_DOI) and [h.kind for h in hm.records(FULL_DOI)] == [holdings.TEXT_ONLY]
    assert hm.stats["abstract_only_sidecars"] == 1
    warm = holdings.build(lib.reg, cache_dir=lib.tmp / "cache")                  # a warm (cached) build
    assert warm.stats["cache_hits"] > 0 and [h.kind for h in warm.records(ABS_DOI)] == [holdings.ABSTRACT_ONLY]


def test_the_instruments_list_it_on_its_own_never_as_a_holding_or_an_orphan(lib):
    s = ap.scan_library(lib.path)
    assert s["abstract_only"] == ["2025_Martin_Stats.fulltext.json"]
    assert "2025_Martin_Stats.fulltext.json" not in s["text_only"] and not s["orphan_sidecars"]
    assert "2025_Martin_Stats.ris" not in s["orphan_ris"]
    assert s["text_only"] == ["2020_Full_Text.fulltext.json"]
    rec = json.loads((lib.path / "2025_Martin_Stats.fulltext.json").read_text(encoding="utf-8"))
    assert ap.is_abstract_only_sidecar(rec) and not ap.is_text_only_sidecar(rec)


def test_the_route_step_does_not_read_it_as_text_on_disk(lib):
    assert not mig._sidecar_text_on_disk({"pmc_filename": "2025_Martin_Stats.pdf"}, lib.path)
    assert mig._sidecar_text_on_disk({"pmc_filename": "2020_Full_Text.pdf"}, lib.path)


def test_a_pdf_arriving_for_it_still_fills_it(lib):
    hm = holdings.build(lib.reg, use_cache=False, write_cache=False)
    got = ID.Destination(lib.path, hm).text_only(ABS_DOI)
    assert got is not None and Path(got[0]).name == "2025_Martin_Stats.fulltext.json"
