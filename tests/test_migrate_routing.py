"""migrate_closed_to_md: residual routing by class (dispatch 0.5, W1-D2).

Stage reports are written with the fetchers' real column lists (unpaywall_fetch_v2, pmc_fetch,
preprint_fetch, sweep's normalized queue); the DNS error string is the real Unpaywall exception text
from a consumer report (tests/fixtures/W1-D2/chain_real), with the email replaced by a synthetic
address in its plain, %40 and %2540 forms. Everything runs under a temp projects root, a temp
registry and a temp state_dir.
"""
import csv
import datetime
import json
import re
import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest

import lit_util
import migrate_closed_to_md as mig
from litpipe import config, holdings

FIX = Path(__file__).parent / "fixtures" / "W1-D2"
TODAY = datetime.date(2026, 9, 30)
RUN = "2026-09-30"

UNPAYWALL = ["rank", "doi", "year", "cites", "filename", "title", "oa_status", "n_locations",
             "downloaded", "winning_host", "winning_url", "attempts", "error"]
PMC = ["doi", "filename", "pmcid", "downloaded", "skipped", "winning_source", "attempts", "error",
       "sidecar", "sidecar_status"]
PREPRINT = ["doi", "title", "year", "preprint_filename", "found", "source", "match_id", "similarity",
            "downloaded", "skipped", "status"]
NORMALIZED = ["doi", "title", "authors", "year", "destination", "notes", "citation_count"]

SIDECAR = "2001_Unknown_AlveolarEpithelialTypeIiCellDefender.fulltext.json"
SIDECAR_DOI = "10.1186/rr36"


def _real_dns_error():
    with open(FIX / "chain_real" / "lit_pull_queue.2026-09-22.unpaywall.csv", encoding="utf-8") as f:
        return next(r["error"] for r in csv.DictReader(f) if "NameResolution" in r["error"])


DNS_PCT40 = _real_dns_error()
assert "email=fixture.user%40example.org" in DNS_PCT40
DNS_FORMS = {"plain": DNS_PCT40.replace("%40", "@"), "pct40": DNS_PCT40,
             "pct2540": DNS_PCT40.replace("%40", "%2540")}
EMAIL_NEEDLES = ("email=", "fixture.user", "example.org", "mailto:")


# ---------------------------------------------------------------- helpers
def write_csv(path, fields, rows):
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore", restval="", lineterminator="\n")
        w.writeheader()
        w.writerows(rows)


def row(doi, title="A title", year="2020", u=None, p=None, r=None, authors="Doe J", notes="", pmc=True,
        attempts=""):
    fn = f"{year}_Doe_{re.sub(r'[^A-Za-z0-9]+', '_', doi)}.pdf"
    return {
        "doi": doi, "title": title, "year": year, "authors": authors, "notes": notes, "filename": fn,
        "attempts": attempts,
        "u": {"doi": doi, "title": title, "year": year, "filename": fn, "oa_status": "CLOSED",
              "downloaded": "False", **(u or {})},
        "p": None if not pmc else {"doi": doi, "filename": fn, "pmcid": "", "downloaded": "False",
                                   "skipped": "False", "error": "NO_PMCID", "sidecar": "False", **(p or {})},
        "r": None if r is None else {"doi": doi, "title": title, "year": year, "status": "NO_MATCH",
                                     "downloaded": "False", "skipped": "False", **r},
    }


def chain(proj, run_id, rows, tag=None, pmc=True, preprint=False, normalized=True, dest="literature/"):
    """Write one run chain with the fetchers' real columns (legacy dated names when run_id is a date)."""
    art = lambda stage: mig.artifact_path(proj, run_id, stage, tag)  # noqa: E731
    write_csv(art("unpaywall"), UNPAYWALL, [x["u"] for x in rows])
    if pmc:
        write_csv(art("pmc"), PMC, [x["p"] for x in rows if x["p"] is not None])
    if preprint:
        write_csv(art("preprint"), PREPRINT, [x["r"] for x in rows if x["r"] is not None])
    if normalized:
        # A re-admitted retry queue carries the retry_later columns (sweep's admit_retries), and
        # sweep's normalizer passes every column through; `attempts` is the one routing reads.
        extra = ["attempts"] if any(x["attempts"] != "" for x in rows) else []
        write_csv(art("normalized"), NORMALIZED + extra, [
            {"doi": x["doi"], "title": x["title"], "authors": x["authors"], "year": x["year"],
             "destination": dest, "notes": x["notes"], "citation_count": "0", "attempts": x["attempts"]}
            for x in rows])


@pytest.fixture
def env(tmp_path, monkeypatch):
    root = tmp_path / "Projects"
    lib = root / "P" / "literature"
    lib.mkdir(parents=True)
    cfgp = tmp_path / "projects.json"
    e = SimpleNamespace(root=root, proj=root / "P", lib=lib, cfgp=cfgp, tmp=tmp_path,
                        cfg={"state_dir": str(tmp_path / "state"), "projects": {"P": {"lib_dir": "literature"}}})
    e.save = lambda: cfgp.write_text(json.dumps(e.cfg), encoding="utf-8")
    e.save()
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    monkeypatch.setattr(config, "CONFIG_PATH", cfgp)
    monkeypatch.setattr(mig, "CONFIG_PATH", cfgp)
    return e


def route(env, **kw):
    kw.setdefault("today", TODAY)
    return mig.run("P", cfg=json.loads(env.cfgp.read_text(encoding="utf-8")), **kw)


def retry_rows(env):
    return mig.read_retry_later(env.proj)[1]


def routing(env, run_id=RUN, tag=None):
    with open(mig.artifact_path(env.proj, run_id, "routing", tag), encoding="utf-8") as f:
        return {r["doi"]: r for r in csv.DictReader(f)}


def text(path):
    return path.read_text(encoding="utf-8") if path.exists() else ""


def acceptance_rows(sidecar_flag=True):
    r403 = row("10.1234/r403", u={"oa_status": "OA", "attempts": "publisher/publishedVersion/HTTP_403",
                                  "error": "HTTP_403"})
    rdns = row("10.1234/rdns9", u={"oa_status": "", "error": DNS_FORMS["plain"]})
    r500 = row(SIDECAR_DOI, title="Alveolar epithelial type II cell", year="2001",
               p={"pmcid": "PMC59567", "attempts": "europepmc/HTTP_500", "error": "HTTP_500",
                  "sidecar": str(sidecar_flag), "sidecar_status": "OK" if sidecar_flag else "HTTP_500"})
    r500["u"]["filename"] = r500["p"]["filename"] = SIDECAR.replace(".fulltext.json", ".pdf")
    return r403, rdns, r500


# ---------------------------------------------------------------- acceptance
def test_acceptance_403_dns_and_500_with_a_sidecar(env):
    """403 -> OA_BLOCKED (worklist + retry_later); DNS -> TRANSIENT; europepmc/HTTP_500 with a
    text sidecar on disk -> TEXT_ONLY. Nothing reaches the ILL list."""
    r403, rdns, r500 = acceptance_rows()
    shutil.copy2(FIX / "lib_real" / SIDECAR, env.lib / SIDECAR)
    chain(env.proj, RUN, [r403, rdns, r500])
    res = route(env)
    assert res["counts"] == {"OA_BLOCKED": 1, "TRANSIENT": 1, "TEXT_ONLY": 1}
    classes = {d: r["residual_class"] for d, r in routing(env).items()}
    assert classes == {"10.1234/r403": "OA_BLOCKED", "10.1234/rdns9": "TRANSIENT", SIDECAR_DOI: "TEXT_ONLY"}
    assert not (env.proj / mig.ILL_NAME).exists()
    oa = text(env.proj / mig.OA_BLOCKED_NAME)
    assert holdings.extract_dois(oa) == ["10.1234/r403"]
    assert "cause `HTTP_403` via `unpaywall:publisher`" in oa
    rl = {r["doi"]: r for r in retry_rows(env)}
    assert set(rl) == {"10.1234/r403", "10.1234/rdns9"}
    assert rl["10.1234/r403"]["residual_class"] == "OA_BLOCKED" and rl["10.1234/r403"]["not_before"] == "2026-10-03"
    assert rl["10.1234/rdns9"]["residual_class"] == "TRANSIENT" and rl["10.1234/rdns9"]["not_before"] == "2026-10-01"
    assert "TRANSPORT" in rl["10.1234/rdns9"]["reason"]


def test_acceptance_500_without_a_sidecar_is_terminal_closed(env):
    r403, rdns, r500 = acceptance_rows(sidecar_flag=False)
    chain(env.proj, RUN, [r403, rdns, r500])
    route(env)
    ill = text(env.proj / mig.ILL_NAME)
    assert holdings.extract_dois(ill) == [SIDECAR_DOI]      # only the 500 row is an ILL candidate
    assert routing(env)[SIDECAR_DOI]["residual_class"] == "TERMINAL_CLOSED"


def test_sidecar_flag_without_the_file_on_disk_is_not_text_only(env):
    r500 = acceptance_rows(sidecar_flag=True)[2]      # the report says OK; the file is not there
    chain(env.proj, RUN, [r500])
    route(env)
    assert routing(env)[SIDECAR_DOI]["residual_class"] == "TERMINAL_CLOSED"


def test_sidecar_found_by_the_holdings_map_under_another_stem(env):
    r500 = acceptance_rows(sidecar_flag=False)[2]
    shutil.copy2(FIX / "lib_real" / SIDECAR, env.lib / "renamed_by_audit.fulltext.json")
    chain(env.proj, RUN, [r500])
    route(env)
    got = routing(env)[SIDECAR_DOI]
    assert got["residual_class"] == "TEXT_ONLY" and got["held_paths"].endswith("renamed_by_audit.fulltext.json")


@pytest.mark.parametrize("form", sorted(DNS_FORMS))
def test_no_written_or_printed_line_carries_an_email(env, capsys, form):
    """I20: the Unpaywall exception text carries `email=`; no report, list or log line may."""
    rows = [
        row("10.1234/dns9", u={"oa_status": "", "error": DNS_FORMS[form]}),
        row("10.1234/closed9", u={"error": DNS_FORMS[form].replace("NameResolutionError", "SSLError")}),
        row("10.1234/sc9", p={"pmcid": "PMC1", "attempts": "europepmc/HTTP_404", "error": "HTTP_404",
                             "sidecar_status": "ERROR_Read timed out; UA litpipe (mailto:fixture.user@example.org)"}),
    ]
    chain(env.proj, RUN, rows)
    route(env)
    written = [p for p in env.proj.iterdir() if p.is_file() and "normalized" not in p.name
               and not p.name.endswith((".unpaywall.csv", ".pmc.csv", ".preprint.csv"))]
    assert {p.name for p in written} >= {mig.RETRY_LATER_NAME, f"lit_pull_queue.{RUN}.routing.csv"}
    for p in written:
        body = p.read_text(encoding="utf-8")
        for needle in EMAIL_NEEDLES:
            assert needle not in body, (p.name, needle)
    out = capsys.readouterr()
    for needle in EMAIL_NEEDLES:
        assert needle not in out.out + out.err


def test_ill_line_with_an_email_in_its_trace_is_redacted(env):
    """A legacy ILL row whose `error` carries the email (a consumer residuals shape) is written clean."""
    bad = DNS_FORMS["pct2540"]
    rows = [{"doi": "10.1234/legacy9", "title": "T", "year": "2020", "oa_status": "CLOSED",
             "error": bad, "stage_pmc": "no_pmcid", "stage_preprint": "no_match",
             "signals": [], "missing": {}}]
    mig.classify_row(rows[0])
    block = mig.render_md_block("P", RUN, rows)
    assert "unpaywall_err=" in block and not any(n in block for n in EMAIL_NEEDLES)


@pytest.mark.parametrize("source", [
    "unpaywall_rows_pct40_email.csv", "chain_real/lit_pull_queue.2026-09-22.unpaywall.csv",
])
def test_redact_scrubs_the_real_report_rows(source):
    with open(FIX / source, encoding="utf-8") as f:
        errors = [r["error"] for r in csv.DictReader(f) if "email=" in r["error"]]
    assert errors
    for e in errors:
        for variant in (e, e.replace("%40", "@"), e.replace("%40", "%2540")):
            clean = mig.redact(variant)
            assert not any(n in clean for n in EMAIL_NEEDLES), clean
            assert "NameResolutionError" in clean or "Max retries" in clean  # the diagnosis survives


def test_redact_scrubs_the_real_md_lines():
    for line in (FIX / "md_residuals_email.md").read_text(encoding="utf-8").splitlines():
        clean = mig.redact(line)
        assert not any(n in clean for n in EMAIL_NEEDLES)
        assert holdings.extract_dois(clean) == holdings.extract_dois(line)  # DOIs untouched


@pytest.mark.parametrize("s", [
    "x?email=a.b%40c.org&tool=y", "x?email=a.b%2540c.org", "EMAIL=a.b@c.org", "email%3Da.b%2540c.org",
    "User-Agent: litpipe/1.0 (mailto:a.b@c.org)", "mailto%3Aa.b%40c.org", "contact a.b@c.co.uk now",
])
def test_redact_forms(s):
    """mig.redact is litpipe.ledger.redact (the fallback was removed at W1 integration)."""
    out = mig.redact(s)
    assert "a.b" not in out and "email=" not in out.lower() and "mailto:" not in out.lower()


# ---------------------------------------------------------------- the other classes
def test_held_elsewhere_with_a_pdf_or_as_text_only(env):
    other = env.root / "Q" / "papers"
    other.mkdir(parents=True)
    shutil.copy2(FIX / "lib_real" / "1972_Keys_IndicesRelativeWeightObesity.ris", other / "keys.ris")
    (other / "keys.pdf").write_bytes(b"%PDF-1.4\n")
    shutil.copy2(FIX / "lib_real" / SIDECAR, other / SIDECAR)
    env.cfg["projects"]["Q"] = {"lib_dir": "papers"}
    env.save()
    chain(env.proj, RUN, [row("10.1016/0021-9681(72)90027-6"), row("https://doi.org/10.1186/RR36")])
    route(env)
    got = routing(env)
    pdf_row = got["10.1016/0021-9681(72)90027-6"]
    assert pdf_row["residual_class"] == "HELD_ELSEWHERE" and pdf_row["held_paths"] == str(other / "keys.pdf")
    txt_row = got["https://doi.org/10.1186/RR36"]
    assert txt_row["residual_class"] == "HELD_ELSEWHERE" and txt_row["held_paths"].endswith(SIDECAR)
    assert not (env.proj / mig.ILL_NAME).exists() and not retry_rows(env)


def test_manual_preprint_is_oa_blocked(env):
    chain(env.proj, RUN, [row("10.1234/mp9", r={"status": "MANUAL_PREPRINT", "source": "osf"})], preprint=True)
    route(env)
    assert routing(env)["10.1234/mp9"]["residual_class"] == "OA_BLOCKED"
    assert "cause `manual_preprint` via `preprint:osf`" in text(env.proj / mig.OA_BLOCKED_NAME)


def test_pmc_refusal_links_the_pmc_article(env):
    chain(env.proj, RUN, [row("10.1234/pmc9", p={"pmcid": "PMC555", "error": "HTML",
                                                "attempts": "europepmc/HTTP_403 | ncbi-page/HTML"})])
    route(env)
    assert "(https://pmc.ncbi.nlm.nih.gov/articles/PMC555/) cause `HTTP_403` via `pmc:europepmc`" in \
        text(env.proj / mig.OA_BLOCKED_NAME)


def test_api_refusal_is_transient_not_oa_blocked(env):
    chain(env.proj, RUN, [row("10.1234/api429", u={"oa_status": "", "error": "HTTP 429"})])
    route(env)
    assert routing(env)["10.1234/api429"]["residual_class"] == "TRANSIENT"
    assert not (env.proj / mig.OA_BLOCKED_NAME).exists()


def test_doi_mismatch_goes_to_review_not_ill_and_is_not_a_fetch(env):
    chain(env.proj, RUN, [row("10.1234/mm9", u={
        "oa_status": "OA", "attempts": "repository/acceptedVersion/OK",
        "error": "DOI_MISMATCH:pdf_doi=10.9999/other9"})], preprint=False)
    route(env)
    assert routing(env)["10.1234/mm9"]["residual_class"] == "IDENTITY_FLAG"
    review = text(env.proj / mig.REVIEW_NAME)
    assert "DOI `10.1234/mm9` flag `DOI_MISMATCH:pdf_doi=10.9999/other9` stage `unpaywall`" in review
    assert not (env.proj / mig.ILL_NAME).exists()


def test_an_outranked_identity_flag_stays_in_the_reason(env):
    chain(env.proj, RUN, [row("10.1234/mm403", u={
        "oa_status": "OA", "attempts": "publisher/publishedVersion/HTTP_403 | repository/acceptedVersion/OK",
        "error": "DOI_MISMATCH:pdf_doi=10.9999/other9"})])
    route(env)
    got = routing(env)["10.1234/mm403"]
    assert got["residual_class"] == "OA_BLOCKED" and "identity: DOI_MISMATCH" in got["reason"]


def test_blank_metadata_is_no_metadata(env):
    chain(env.proj, RUN, [row("10.1234/blank9", title="", authors="")])
    route(env)
    assert routing(env)["10.1234/blank9"]["residual_class"] == "NO_METADATA"
    assert not (env.proj / mig.ILL_NAME).exists()


def test_config_outcome_aborts_and_writes_nothing(env, monkeypatch):
    chain(env.proj, RUN, [row("10.1234/cfg9", u={"oa_status": "", "error": "HTTP 422"}),
                          row("10.1234/ok403", u={"oa_status": "OA", "attempts": "p/v/HTTP_403"})])
    before = sorted(p.name for p in env.proj.iterdir())
    assert route(env)["status"] == "config"
    assert sorted(p.name for p in env.proj.iterdir()) == before
    monkeypatch.setattr("sys.argv", ["migrate_closed_to_md.py", "--project", "P", "--date", RUN])
    assert mig.main() == 2
    assert sorted(p.name for p in env.proj.iterdir()) == before


# ---------------------------------------------------------------- stage reports that are missing
def test_missing_pmc_report_is_pending_unless_pmc_is_excluded(env):
    """T3: a missing report is not 'PMC found nothing'. The row is PENDING (sweep keeps the queue),
    never an ILL line and never a retry_later row (the kept queue re-sweeps it)."""
    chain(env.proj, RUN, [row("10.1234/nopmc9")], pmc=False)
    route(env)
    got = routing(env)["10.1234/nopmc9"]
    assert got["residual_class"] == "PENDING" and "report_absent" in got["reason"]
    assert not (env.proj / mig.ILL_NAME).exists() and not retry_rows(env)
    env.cfg["projects"]["P"]["sources"] = ["unpaywall"]
    env.save()
    route(env)
    assert routing(env)["10.1234/nopmc9"]["residual_class"] == "TERMINAL_CLOSED"


def test_missing_preprint_report_blocks_only_an_enabled_preprint_stage(env):
    chain(env.proj, RUN, [row("10.1234/nopre9")])
    assert route(env, dry_run=True)["counts"] == {"TERMINAL_CLOSED": 1}   # default sources: not enabled
    env.cfg["projects"]["P"]["sources"] = ["unpaywall", "pmc", "biorxiv"]
    env.save()
    assert route(env, dry_run=True)["counts"] == {"PENDING": 1}
    assert route(env, dry_run=True, skip_preprint=True)["counts"] == {"TERMINAL_CLOSED": 1}


def test_a_row_missing_from_a_present_pmc_report_is_not_closed(env):
    x = row("10.1234/absent9")
    x["p"] = None
    chain(env.proj, RUN, [x, row("10.1234/present9")])
    route(env)
    got = routing(env)
    assert got["10.1234/absent9"]["residual_class"] == "PENDING"
    assert got["10.1234/present9"]["residual_class"] == "TERMINAL_CLOSED"


# ---------------------------------------------------------------- retry_later
def test_retry_later_appends_updates_in_place_and_keeps_other_rows(env):
    rl = env.proj / mig.RETRY_LATER_NAME
    write_csv(rl, mig.RETRY_FIELDS + ["jacob_note"], [
        {"doi": "10.5555/keep-me", "title": "Kept", "residual_class": "TRANSIENT", "not_before": "2026-09-01",
         "first_seen": "2026-08-01", "jacob_note": "hand note"},
        {"doi": "https://doi.org/10.1234/R403", "title": "Old title", "first_seen": "2026-09-01",
         "residual_class": "TRANSIENT", "not_before": "2026-09-02"},
    ])
    r403, rdns, _ = acceptance_rows()
    chain(env.proj, RUN, [r403, rdns])
    route(env)
    rows = retry_rows(env)
    assert [holdings.doi_key(r["doi"]) for r in rows] == ["10.5555/keep-me", "10.1234/r403", "10.1234/rdns9"]
    assert rows[0]["jacob_note"] == "hand note" and rows[0]["not_before"] == "2026-09-01"
    assert rows[1]["residual_class"] == "OA_BLOCKED" and rows[1]["first_seen"] == "2026-09-01"
    assert rows[1]["last_seen"] == TODAY.isoformat() and rows[1]["not_before"] == "2026-10-03"
    route(env)   # idempotent: a second pass adds nothing
    assert len(retry_rows(env)) == 3


def test_error_is_counted_across_runs_then_closed(env):
    """`attempts` counts sweeps of the row (W1-D1 semantics); an ERROR row closes on the third."""
    err = lambda: [row("10.1234/e404", u={"oa_status": "OA", "attempts": "publisher/publishedVersion/HTTP_404",  # noqa: E731
                                          "error": "HTTP_404"})]
    for i, run_id in enumerate(("2026-09-28", "2026-09-29", "2026-09-30"), start=1):
        chain(env.proj, run_id, err())
        route(env, run_id=run_id)
        route(env, run_id=run_id)          # a re-run of the same run id does not count twice
        got = routing(env, run_id)["10.1234/e404"]
        assert got["attempts"] == str(i)
        if i < 3:
            assert got["residual_class"] == "TRANSIENT"
            assert retry_rows(env)[0]["attempts"] == str(i)
        else:
            assert got["residual_class"] == "TERMINAL_CLOSED" and "error in 3 runs" in got["reason"]
    assert "error in 3 runs" in text(env.proj / mig.ILL_NAME)


def test_attempts_survive_readmission_through_the_retry_queue(env):
    """sweep's admit_retries moves a due retry_later row (every column, `attempts` included) into
    the `retry` queue and deletes it from retry_later; the count comes back via the normalized CSV."""
    x = row("10.1234/e404", notes="s4 carryover", attempts="2",
            u={"oa_status": "OA", "attempts": "publisher/publishedVersion/HTTP_404", "error": "HTTP_404"})
    chain(env.proj, "2026-09-30.2", [x], tag="retry")
    route(env, run_id="2026-09-30.2")
    got = routing(env, "2026-09-30.2", "retry")["10.1234/e404"]
    assert got["residual_class"] == "TERMINAL_CLOSED" and got["attempts"] == "3"


# ---------------------------------------------------------------- run ids, tags, CLI
def test_run_ids_and_tagged_chains(env):
    chain(env.proj, "2026-09-29.10", [row("10.1234/old9")])
    chain(env.proj, "2026-09-30", [row("10.1234/first9")])
    chain(env.proj, "2026-09-30.2", [row("10.1234/untagged9")])
    chain(env.proj, "2026-09-30.2", [row("10.1234/tagged9")], tag="retry")
    assert mig.latest_sweep_date(env.proj) == "2026-09-30.2"
    assert mig.find_tags(env.proj, "2026-09-30.2") == ["retry"]
    res = route(env)
    assert res["run_id"] == "2026-09-30.2" and res["chains"] == ["", "retry"]
    ill = text(env.proj / mig.ILL_NAME)
    assert set(holdings.extract_dois(ill)) == {"10.1234/untagged9", "10.1234/tagged9"}
    assert "## Sweep residuals 2026-09-30.2 [retry]: 1 closed-access" in ill
    assert "`lit_pull_queue.retry.2026-09-30.2.*.csv`" in ill
    assert set(routing(env, "2026-09-30.2", "retry")) == {"10.1234/tagged9"}
    chain(env.proj, "2026-09-30.10", [row("10.1234/tenth9")])
    assert mig.latest_sweep_date(env.proj) == "2026-09-30.10"   # numeric, not lexical


def test_tag_flag_routes_only_that_chain(env, monkeypatch):
    chain(env.proj, "2026-09-30.2", [row("10.1234/untagged9")])
    chain(env.proj, "2026-09-30.2", [row("10.1234/tagged9")], tag="ch15")
    monkeypatch.setattr("sys.argv", ["migrate_closed_to_md.py", "--project", "P", "--run-id", "2026-09-30.2",
                                     "--tag", "ch15", "--no-holdings"])
    assert mig.main() == 0
    assert holdings.extract_dois(text(env.proj / mig.ILL_NAME)) == ["10.1234/tagged9"]


def test_dry_run_writes_nothing(env):
    r403, rdns, r500 = acceptance_rows(sidecar_flag=False)
    chain(env.proj, RUN, [r403, rdns, r500])
    before = sorted(p.name for p in env.proj.iterdir())
    res = route(env, dry_run=True)
    assert res["counts"] == {"OA_BLOCKED": 1, "TRANSIENT": 1, "TERMINAL_CLOSED": 1}
    assert sorted(p.name for p in env.proj.iterdir()) == before


def test_cli_help_and_bad_arguments(env, capsys):
    with pytest.raises(SystemExit) as e:
        mig.main(["--help"])
    assert e.value.code == 0
    assert mig.main(["--project", "P", "--date", "2026-9-30"]) == 2
    assert mig.main(["--project", "P", "--tag", "Bad Tag"]) == 2
    assert mig.main(["--project", "nope"]) == 2


def test_no_artifacts_is_a_clean_no_op(env):
    assert route(env)["status"] == "nothing"
    assert list(env.proj.iterdir()) == [env.lib]


# ---------------------------------------------------------------- W1-D1's typed residual CSV
# sweep (W1-D1) writes lit_pull_queue[.<tag>].<run_id>.residual.csv: the normalized queue columns
# plus residual_class, reason, stages, held_at, skipped_sources, attempts, run_id, for every row not
# fetched (rows settled before fetching included). migrate routes by that class.
TYPED = NORMALIZED + ["residual_class", "reason", "stages", "held_at", "skipped_sources", "attempts", "run_id"]


def typed(doi, cls, reason="", run_id=RUN, **kw):
    return {"doi": doi, "title": "A title", "authors": "Doe J", "year": "2020",
            "destination": "literature/", "notes": "", "citation_count": "0", "residual_class": cls,
            "reason": reason, "stages": "", "held_at": "", "skipped_sources": "", "attempts": "1",
            "run_id": run_id, **kw}


def typed_residual(folder, rows, run_id=RUN, tag=None, extra=()):
    write_csv(mig.artifact_path(folder, run_id, "residual", tag), TYPED + list(extra), rows)


def test_typed_residual_routes_by_sweeps_class(env):
    # The reports alone would make T1 OA_BLOCKED; sweep's typed class (TERMINAL_CLOSED) wins.
    chain(env.proj, RUN, [
        row("10.1234/t1", u={"oa_status": "OA", "attempts": "publisher/publishedVersion/HTTP_403", "error": "HTTP_403"}),
        row("10.1234/t2", p={"pmcid": "PMC777", "attempts": "europepmc/HTTP_403 | ncbi-page/HTML", "error": "HTML"}),
    ])
    typed_residual(env.proj, [
        typed("10.1234/t1", "TERMINAL_CLOSED", "unpaywall: CLOSED; pmc: NO_PMCID", stages="unpaywall=CLOSED; pmc=NO_PMCID"),
        typed("10.1234/t2", "OA_BLOCKED", "pmc: HTML", stages="unpaywall=CLOSED; pmc=HTML"),
        typed("10.1234/t3", "OA_BLOCKED", "unpaywall: HTTP_403"),
        typed("10.1234/t4", "TRANSIENT", "unpaywall: " + DNS_FORMS["plain"], attempts="2"),
        typed("10.1234/t5", "IDENTITY_FLAG", "unpaywall: DOI_MISMATCH:pdf_doi=10.9999/x9"),
        typed("10.1234/t6", "HELD_ELSEWHERE", "held in another library", held_at="C:/x/a.pdf; C:/y/b.pdf"),
        typed("10.1234/t7", "TEXT_ONLY", "text sidecar, no PDF"),
        typed("10.1234/t8", "NO_METADATA", "blank title; metadata unavailable", title=""),
        typed("NO_DOI_123", "INVALID_DOI", "invalid or placeholder DOI 'NO_DOI_123'"),
        typed("10.1234/t10", "PENDING", "pmc failed"),
        typed("10.1234/t11", "SKIPPED_SOURCE", "no enabled stage covers this row"),
    ])
    res = route(env)
    assert res["counts"] == {"TERMINAL_CLOSED": 1, "OA_BLOCKED": 2, "TRANSIENT": 1, "IDENTITY_FLAG": 1,
                             "HELD_ELSEWHERE": 1, "TEXT_ONLY": 1, "NO_METADATA": 1, "INVALID_DOI": 1,
                             "PENDING": 1, "SKIPPED_SOURCE": 1}
    ill = text(env.proj / mig.ILL_NAME)
    assert holdings.extract_dois(ill) == ["10.1234/t1"] and "unpaywall=CLOSED; pmc=NO_PMCID" in ill
    oa = text(env.proj / mig.OA_BLOCKED_NAME)
    assert set(holdings.extract_dois(oa)) == {"10.1234/t2", "10.1234/t3"}
    assert "(https://pmc.ncbi.nlm.nih.gov/articles/PMC777/) cause `HTTP_403` via `pmc:europepmc`" in oa
    assert "[10.1234/t3](https://doi.org/10.1234/t3) cause `HTTP_403` via `unpaywall`" in oa
    rl = {r["doi"]: r for r in retry_rows(env)}
    assert set(rl) == {"10.1234/t2", "10.1234/t3", "10.1234/t4"}
    assert rl["10.1234/t4"]["attempts"] == "2" and rl["10.1234/t4"]["not_before"] == "2026-10-01"
    assert rl["10.1234/t2"]["not_before"] == "2026-10-03"
    assert "DOI `10.1234/t5` flag `DOI_MISMATCH:pdf_doi=10.9999/x9` stage `unpaywall`" in text(env.proj / mig.REVIEW_NAME)
    got = routing(env)
    assert got["10.1234/t6"]["held_paths"] == "C:/x/a.pdf | C:/y/b.pdf"
    assert got["10.1234/t10"]["route"].startswith("none") and got["NO_DOI_123"]["route"] == "none"
    for p in env.proj.iterdir():
        if p.is_file() and not p.name.endswith((".unpaywall.csv", ".residual.csv")):
            assert not any(n in p.read_text(encoding="utf-8") for n in EMAIL_NEEDLES), p.name


def test_typed_residual_not_before_stale_ignored_future_kept(env):
    typed_residual(env.proj, [
        typed("10.1234/stale9", "TRANSIENT", "unpaywall: HTTP_503", not_before="2026-09-01"),
        typed("10.1234/embargo9", "TRANSIENT", "embargoed", not_before="2026-12-01"),
    ], extra=["not_before"])
    route(env)
    rl = {r["doi"]: r for r in retry_rows(env)}
    assert rl["10.1234/stale9"]["not_before"] == "2026-10-01"      # a re-admitted past date is stale
    assert rl["10.1234/embargo9"]["not_before"] == "2026-12-01"    # a later date the row carries wins


@pytest.mark.parametrize("absolute", [False, True])
def test_artifact_dir(env, monkeypatch, absolute):
    runs = (env.tmp / "elsewhere") if absolute else (env.proj / "runs")
    runs.mkdir()
    chain(runs, RUN, [row("10.1234/closed9")])
    arg = str(runs) if absolute else "runs"
    monkeypatch.setattr("sys.argv", ["migrate_closed_to_md.py", "--project", "P", "--date", RUN,
                                     "--artifact-dir", arg, "--no-holdings"])
    assert mig.main() == 0
    assert holdings.extract_dois(text(env.proj / mig.ILL_NAME)) == ["10.1234/closed9"]   # list: project root
    assert mig.artifact_path(runs, RUN, "routing").exists()                             # report: artifact dir
    assert not mig.artifact_path(env.proj, RUN, "routing").exists()
    assert mig.latest_sweep_date(runs) == RUN and mig.latest_sweep_date(env.proj) is None


def test_a_typed_residual_alone_is_found(env):
    """A run whose Unpaywall stage failed writes only the typed residual (every row PENDING)."""
    typed_residual(env.proj, [typed("10.1234/p9", "PENDING", "unpaywall failed")], run_id="2026-09-30.3", tag="ch15")
    assert mig.latest_sweep_date(env.proj) == "2026-09-30.3"
    assert mig.find_tags(env.proj, "2026-09-30.3") == ["ch15"]
    assert route(env)["counts"] == {"PENDING": 1}


def test_legacy_untyped_residual_falls_back_to_the_reports(env):
    chain(env.proj, RUN, [row("10.1234/r403", u={"oa_status": "OA", "attempts": "p/v/HTTP_403", "error": "HTTP_403"})])
    write_csv(mig.artifact_path(env.proj, RUN, "residual"), NORMALIZED,
              [{"doi": "10.1234/r403", "title": "A title", "destination": "literature/"}])
    assert route(env, dry_run=True)["counts"] == {"OA_BLOCKED": 1}


def test_typed_config_row_aborts(env):
    typed_residual(env.proj, [typed("10.1234/cfg9", "CONFIG", "configuration refused by a source"),
                              typed("10.1234/closed9", "TERMINAL_CLOSED", "unpaywall: CLOSED")])
    before = sorted(p.name for p in env.proj.iterdir())
    assert route(env)["status"] == "config"
    assert sorted(p.name for p in env.proj.iterdir()) == before
