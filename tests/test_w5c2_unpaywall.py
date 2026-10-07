"""W5-C2 step 5: the Unpaywall report's `best_oa_url` (items 2, C131) and a named refused host
(item 3, C109 F-1). Offline, on the real stage through the real litpipe.net (tests/test_unpaywall_stage's
Web transport).

- best_oa_url, appended last: the record's best_oa_location (url_for_pdf, else url) from the SAME
  response, on every row Unpaywall answered with an OA location (failed, flagged, dry run or
  downloaded); empty for a closed record, a lookup failure and a held file.
- A download attempt on a refused host names it (`host_refused:<host>` in the detail;
  `HOST_REFUSED:<host>` in `error`, preprint_fetch's legacy form), so an arXiv-copy row whose only
  open source is a host refused until cleared waits 30 days through sweep's verdict reader and
  migrate's router, end to end.
"""
import csv
import datetime
import json

import lit_util
import migrate_closed_to_md as mig
import sweep
import unpaywall_fetch_v2 as U
from litpipe import config
from litpipe.outcomes import Kind
from tests.test_unpaywall_stage import (HTML, PDF, add_upw, article_pdf, loc, run_stage, upw_record,  # noqa: F401
                                        upw_url, web)

D_FAIL, D_OK, D_CLOSED, D_DRY = "10.1152/w5.0001", "10.1152/w5.0002", "10.1152/w5.0003", "10.1152/w5.0004"
ARXIV_DOI = "10.1152/w5.0099"


def test_best_oa_url_is_the_last_report_column():
    assert U.REPORT_FIELDS[-1] == "best_oa_url"


def test_best_oa_url_prefers_url_for_pdf_then_url():
    assert U.best_oa_url({"best_oa_location": {"url_for_pdf": "https://a/x.pdf", "url": "https://a/x"}}) \
        == "https://a/x.pdf"
    assert U.best_oa_url({"best_oa_location": {"url_for_pdf": None, "url": "https://a/landing"}}) \
        == "https://a/landing"
    assert U.best_oa_url({"best_oa_location": None}) == "" and U.best_oa_url({}) == ""
    assert U.best_oa_url({"best_oa_location": {"url_for_pdf": "", "url": ""}}) == ""


def test_best_oa_url_is_written_on_every_row_with_an_oa_location(web, tmp_path):
    best = loc("https://pub.example.org/best.pdf", "https://pub.example.org/landing")
    other = loc("https://repo.example.org/aam.pdf", host_type="repository", version="acceptedVersion")
    add_upw(web, D_FAIL, upw_record(D_FAIL, [best, other]))
    web.add("https://pub.example.org/best.pdf", (404, HTML, b"<html>gone</html>"))
    web.add("https://repo.example.org/aam.pdf", (404, HTML, b"<html>gone</html>"))
    web.add("https://pub.example.org/landing", (404, HTML, b"<html>gone</html>"))
    ok_loc = loc("https://pub.example.org/ok.pdf")
    add_upw(web, D_OK, upw_record(D_OK, [ok_loc]))
    web.add("https://pub.example.org/ok.pdf", (200, PDF, article_pdf(D_OK)))
    add_upw(web, D_CLOSED, upw_record(D_CLOSED, [], is_oa=False))
    _, rep, _ = run_stage(tmp_path, [{"doi": D_FAIL}, {"doi": D_OK},
                                     {"doi": D_CLOSED, "title": "Cold water immersion after sprint training"}])
    assert rep[D_FAIL]["downloaded"] == "False" and rep[D_FAIL]["winning_url"] == ""
    assert rep[D_FAIL]["best_oa_url"] == "https://pub.example.org/best.pdf"     # the same response's best
    assert rep[D_OK]["downloaded"] == "True" and rep[D_OK]["best_oa_url"] == "https://pub.example.org/ok.pdf"
    assert rep[D_CLOSED]["best_oa_url"] == ""
    assert len(web.to("api.unpaywall.org")) == 3                                  # one lookup per row, no re-query


def test_best_oa_url_on_a_dry_run_and_a_landing_only_location(web, tmp_path):
    add_upw(web, D_DRY, upw_record(D_DRY, [loc(None, "https://pub.example.org/article/4")]))
    _, rep, _ = run_stage(tmp_path, [{"doi": D_DRY}], dry_run=True)
    assert rep[D_DRY]["best_oa_url"] == "https://pub.example.org/article/4"


def test_a_lookup_failure_leaves_best_oa_url_empty(web, tmp_path):
    web.add(upw_url(D_FAIL), (404, HTML, b"<html>not found</html>"))
    _, rep, _ = run_stage(tmp_path, [{"doi": D_FAIL}])
    assert rep[D_FAIL]["best_oa_url"] == ""


# ------------------------------------------------------------------ the refused host, end to end
def _arxiv_row(web, tmp_path):
    web.env.write_config(hosts={"arxiv_pdf_allowed": True})       # the copy is reachable only with the switch on
    web.env.state.refuse("arxiv.org", "manual: refused since 2026-09-25", persistence="manual")
    arxiv = loc("https://arxiv.org/pdf/2101.00001", host_type="repository", version="submittedVersion")
    add_upw(web, ARXIV_DOI, upw_record(ARXIV_DOI, [arxiv]))
    _, rep, _ = run_stage(tmp_path, [{"doi": ARXIV_DOI}])
    return rep[ARXIV_DOI]


def test_a_refused_repository_attempt_names_its_host(web, tmp_path):
    row = _arxiv_row(web, tmp_path)
    assert web.to("arxiv.org") == []                                              # nothing sent
    assert row["outcome"] == "REFUSED" and row["attempts"] == "repository/submittedVersion/HOST_REFUSED"
    assert row["error"] == "HOST_REFUSED:arxiv.org"
    assert "host_refused:arxiv.org" in row["detail"]
    assert row["best_oa_url"] == "https://arxiv.org/pdf/2101.00001"
    v = sweep.unpaywall_verdict(row)
    assert v.kind is Kind.REFUSED and v.download_host and "host_refused:arxiv.org" in v.raw.lower()


def test_the_arxiv_copy_row_waits_30_days_through_sweep_and_migrate(web, tmp_path, monkeypatch):
    (tmp_path / "stage").mkdir()
    row = _arxiv_row(web, tmp_path / "stage")
    root = tmp_path / "Projects"
    proj = root / "P"
    (proj / "literature").mkdir(parents=True)
    cfgp = tmp_path / "mig_projects.json"
    cfgp.write_text(json.dumps({"state_dir": str(tmp_path / "state"),
                                "projects": {"P": {"lib_dir": "literature"}}}), encoding="utf-8")
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    monkeypatch.setattr(config, "CONFIG_PATH", cfgp)
    monkeypatch.setattr(mig, "CONFIG_PATH", cfgp)
    run_id, today = "2026-09-30", datetime.date(2026, 9, 30)

    def write(stage, fields, rows):
        with open(mig.artifact_path(proj, run_id, stage), "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore", restval="", lineterminator="\n")
            w.writeheader()
            w.writerows(rows)

    write("unpaywall", U.REPORT_FIELDS, [row])
    write("pmc", ["doi", "filename", "pmcid", "downloaded", "skipped", "winning_source", "attempts", "error",
                  "sidecar", "sidecar_status"],
          [{"doi": ARXIV_DOI, "pmcid": "", "downloaded": "False", "skipped": "False", "error": "NO_PMCID",
            "sidecar": "False"}])
    write("normalized", ["doi", "title", "authors", "year", "destination", "notes", "citation_count"],
          [{"doi": ARXIV_DOI, "title": row["title"], "authors": "Author A", "year": "2020",
            "destination": "literature/", "citation_count": "0"}])
    mig.run("P", cfg=json.loads(cfgp.read_text(encoding="utf-8")), today=today)
    retry = {r["doi"]: r for r in mig.read_retry_later(proj)[1]}
    assert retry[ARXIV_DOI]["not_before"] == (today + datetime.timedelta(days=30)).isoformat()
