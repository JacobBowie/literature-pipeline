"""W2b verifier E: the preprint stage (W2-C), sweep and migrate on typed outcomes (W2-G), the
integration commit, and the seam where the instruments read sweep's artifacts.

Every test here drives REAL modules: `preprint_fetch.run`/`main` through the REAL litpipe.net (only
the transport is the W2-C `Web` double, which fails on any URL it was not given), `sweep.run` with
the stage subprocesses answered in-process (the REAL preprint stage, or its REAL report rows
replayed), the REAL `migrate_closed_to_md.main(argv)`, and the REAL `audit_portfolio.audit_queue`.
arXiv is refused in every state these tests create. No live network, no subprocess but --help.

Tests marked APPLY in their docstring fail on 61eb10b and lock a fix handed back to the dispatcher.
"""
import contextlib
import csv
import io
import json
import subprocess
import sys
import types
from pathlib import Path

import pytest

import audit_portfolio
import lit_util
import migrate_closed_to_md as mig
import preprint_fetch as P
import sweep
from litpipe import config, ledger, net
from litpipe import doi as D
from litpipe.outcomes import Kind
# The W2-C stage harness (Web transport, StageEnv, canned routes) and the W1 verifier's sweep
# harness (World, Stages); the fixtures are imported by name so pytest finds them here.
from tests.test_preprint_stage import (BALLET, DOI_64898, EPMC, JSON, PAPER_64898, Web,  # noqa: F401
                                       env, epmc_record, fx, make_pdf, ris_calls, route_epmc,
                                       route_osf_ballet, web)
from tests.test_w1_verify_b import UNPW_FIELDS, Stages, World, read_csv, write_csv

DAY, DAY2 = "2026-10-01", "2026-10-02"
ARXIV_HOSTS = ("export.arxiv.org", "arxiv.org", "www.arxiv.org")
LEAKS = ("email=", "mailto:", "tester@litpipe-test.org", "tester%40litpipe-test.org", "fixture.user",
         "SECRETSIG", "SECRETCRED", "Authorization: Bearer")
TYPED_BLANK = dict.fromkeys(P.TYPED_FIELDS, "")


def refuse_arxiv(state):
    for h in ARXIV_HOSTS:
        state.refuse(h, "manual: HTTP 406 since 2026-09-25", persistence="manual")


class RealPreprint(Stages):
    """The W1 harness's stage stand-in, with preprint_fetch.py answered by the REAL stage in-process
    (`preprint_fetch.main(argv)` with exactly the argv sweep built), or, when `replay` holds rows,
    by those rows (real stage output collected beforehand) for the DOIs in the triage CSV.
    `exit_for` forces a script's exit code for a queue whose name contains the key."""

    def __init__(self):
        super().__init__()
        self.ppr, self.replay, self.exit_for = [], None, {}

    def __call__(self, cmd, **kw):
        cmd = [str(c) for c in cmd]
        script = next(Path(c).name for c in cmd if c.endswith(".py"))
        for (scr, needle), rc in self.exit_for.items():
            if scr == script and any(needle in c for c in cmd):
                self.calls.append((script, cmd))
                return types.SimpleNamespace(returncode=rc, stdout="", stderr="CONFIG: forced")
        if script != "preprint_fetch.py":
            return super().__call__(cmd, **kw)
        self.calls.append((script, cmd))
        argv = cmd[2:]
        if self.replay is not None:
            arg = lambda flag: argv[argv.index(flag) + 1]
            dois = [r["doi"].strip().lower() for r in read_csv(arg("--triage"))]
            rows = [self.replay[d] for d in dois if d in self.replay]
            write_csv(arg("--report"), rows, P.REPORT_FIELDS)
            self.ppr.append((argv, 0, "", ""))
            return types.SimpleNamespace(returncode=0, stdout="", stderr="")
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            rc = P.main(argv)
        self.ppr.append((argv, rc, out.getvalue(), err.getvalue()))
        return types.SimpleNamespace(returncode=rc, stdout=out.getvalue(), stderr=err.getvalue())


def make_world(tmp_path, monkeypatch, projects):
    """The W1 World (temp root, registry, CONFIG_PATH of sweep/migrate/config, frozen migrate date)
    with `projects` as the registry and the RealPreprint stand-in; arXiv refused in net.STATE."""
    w = World(tmp_path, monkeypatch)
    w.cfg["projects"] = projects
    w.cfg_path.write_text(json.dumps(w.cfg), encoding="utf-8")
    w.stages = RealPreprint()
    monkeypatch.setattr(sweep.subprocess, "run", w.stages)
    refuse_arxiv(net.STATE)
    return w


def queue_at(w, project_dir, rows, dest, name="lit_pull_queue.csv"):
    project_dir.mkdir(parents=True, exist_ok=True)
    out = []
    for r in rows:
        if isinstance(r, str):
            r = {"doi": r}
        out.append({"title": "A study of heat", "authors": "Smith J", "year": "2020",
                    "destination": dest, "notes": "", **r})
    write_csv(project_dir / name, out, list(sweep.QUEUE_COLUMNS))


def residual_of(w, project_dir, run_id=DAY, tag=""):
    name = f"lit_pull_queue{('.' + tag) if tag else ''}.{run_id}.residual.csv"
    return {r["doi"]: r for r in read_csv(project_dir / name)}


def md(project_dir, name):
    p = project_dir / name
    return p.read_text(encoding="utf-8") if p.exists() else ""


OSF_SOURCES = ["unpaywall", "pmc", "osf"]


def no_osf_match(web):
    web.add("https://api.osf.io/v2/preprints/", (200, {"Content-Type": "application/vnd.api+json"},
                                                json.dumps({"data": []})))


# ================================================================ 1. every key form sweep can pass
KEY_FORMS = {
    "subproject": ("Parent/Sub", {"parent": "Parent", "lib_dir": "Sub/literature", "sources": OSF_SOURCES},
                   "literature"),
    "space_and_dot": ("Dot.Key With Space", {"lib_dir": "lit", "sources": OSF_SOURCES}, "lit"),
    "inactive": ("Old", {"lib_dir": "lit", "active": False, "sources": OSF_SOURCES}, "lit"),
}


@pytest.mark.parametrize("form", sorted(KEY_FORMS))
def test_every_registered_key_form_reaches_the_real_preprint_stage(form, tmp_path, monkeypatch, web):
    """Sweep passes `--project <key>`; a stage exit 2 is CONFIG and aborts the whole sweep. The REAL
    preprint stage must accept every registered key form sweep can pass (subproject tail, a space
    and a dot, an inactive key named explicitly) through its REAL argv parser."""
    key, entry, dest = KEY_FORMS[form]
    w = make_world(tmp_path, monkeypatch, {"Parent": {"lib_dir": "literature"}, key: entry})
    no_osf_match(web)
    pdir = lit_util.project_root(key, entry)
    queue_at(w, pdir, ["10.1000/keyform.1"], dest)
    out = sweep.run(project=key, date=DAY, migrate=True)
    assert len(w.stages.ppr) == 1
    argv, rc, _, err = w.stages.ppr[0]
    assert argv[argv.index("--project") + 1] == key and rc == 0, err
    assert out["exit_code"] == sweep.EXIT_OK
    res = residual_of(w, pdir)
    assert res["10.1000/keyform.1"]["residual_class"] == "TERMINAL_CLOSED"
    assert "10.1000/keyform.1" in md(pdir, mig.ILL_NAME)
    assert [u for _, u, _ in web.sent] and set(web.hosts()) == {"api.osf.io"}


def test_a_bare_sweep_passes_every_active_registry_key(tmp_path, monkeypatch, web):
    projects = {"Parent": {"lib_dir": "literature"}}
    for form in ("subproject", "space_and_dot", "inactive"):
        key, entry, _ = KEY_FORMS[form]
        projects[key] = entry
    w = make_world(tmp_path, monkeypatch, projects)
    no_osf_match(web)
    for i, form in enumerate(("subproject", "space_and_dot", "inactive")):
        key, entry, dest = KEY_FORMS[form]
        queue_at(w, lit_util.project_root(key, entry), [f"10.1000/bare.{i}"], dest)
    out = sweep.run(date=DAY, migrate=True)
    assert out["exit_code"] == sweep.EXIT_OK
    passed = sorted(a[a.index("--project") + 1] for a, rc, _, _ in w.stages.ppr if rc == 0)
    assert passed == ["Dot.Key With Space", "Parent/Sub"]           # the inactive key is not swept


def test_an_unregistered_project_dir_is_not_a_config_abort(tmp_path, monkeypatch, web):
    """APPLY (sweep.py `_preprint_excluded`, ~1280): find_queues still sweeps an explicit
    unregistered --project as a top-level dir (its docstring: backward compatible), but W2-G's
    DEC-31 gate now calls litpipe.config.sources() on it, which raises ConfigError, and the whole
    sweep exits 2 before any stage runs. At 2188c44 an unregistered key read as no `sources` key."""
    w = make_world(tmp_path, monkeypatch, {"P": {"lib_dir": "lit"}})
    queue_at(w, w.root / "Loose", ["10.1000/loose.1", "10.48550/arxiv.2103.00020"], "lit")
    out = sweep.run(project="Loose", date=DAY, migrate=False)
    assert out["exit_code"] != sweep.EXIT_USAGE
    assert "unpaywall_fetch_v2.py" in w.stages.scripts()
    for argv, rc, _, err in w.stages.ppr:                           # the arXiv row only, never CONFIG
        assert rc != P.EXIT_CONFIG, err
    assert web.sent == []                                           # arXiv refused: nothing sent


# ================================================================ 1b. exit 2 means CONFIG, only
def _write_cfg(tmp_path, monkeypatch, **top):
    cfg = {"state_dir": str(tmp_path / "state"),
           "projects": {"Parent": {"lib_dir": "literature"},
                        "Parent/Sub": {"parent": "Parent", "lib_dir": "Sub/literature"}}, **top}
    p = tmp_path / "projects.json"
    p.write_text(json.dumps(cfg), encoding="utf-8")
    monkeypatch.setattr(config, "CONFIG_PATH", p)
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", tmp_path / "root")
    return cfg


def _main(argv):
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        rc = P.main(argv)
    return rc, out.getvalue() + err.getvalue()


def test_the_preprint_stage_exits_2_only_on_configuration(tmp_path, monkeypatch, web):
    _write_cfg(tmp_path, monkeypatch)
    refuse_arxiv(net.STATE)
    lib = tmp_path / "root" / "Parent" / "Sub" / "literature"      # not created: the stage makes it
    t = tmp_path / "t.csv"
    rep = str(tmp_path / "r.csv")
    cases = {
        "header only": "doi,title,year,authors\n",
        "doi column only": "doi\n10.1000/x.1\n",
        "an arXiv row, arXiv refused": "doi,title\n10.48550/arxiv.2103.00020,T\n",
        "odd characters": 'doi,title\n"10.1002/(sici)1097-4636(199601)30:1<1::aid-jbm1>3.0.co;2-t","A, ""quoted"" title"\n',
    }
    for why, text in cases.items():
        t.write_text(text, encoding="utf-8")
        for extra in (["--project", "Parent/Sub"], [], ["--sources", "unpaywall,pmc"]):
            rc, log = _main(["--triage", str(t), "--lib-dir", str(lib), "--report", rep, *extra])
            assert rc == P.EXIT_OK, (why, extra, log)
    # a bug inside one row is that row's ERROR, never the stage's exit code
    monkeypatch.setattr(P, "discover", lambda ctx, row: (_ for _ in ()).throw(RuntimeError("boom")))
    t.write_text("doi,title\n10.1000/x.2,Some title\n", encoding="utf-8")
    rc, _ = _main(["--triage", str(t), "--lib-dir", str(lib), "--report", rep, "--project", "Parent/Sub"])
    assert rc == P.EXIT_OK and read_csv(rep)[0]["outcome"] == "ERROR"
    assert _main(["--triage", str(tmp_path / "missing.csv"), "--lib-dir", str(lib), "--report", rep])[0] == 1
    # exit 2 is configuration only
    assert _main(["--triage", str(t), "--lib-dir", str(lib), "--report", rep, "--project", "Sub"])[0] == 2
    assert _main(["--triage", str(t), "--lib-dir", str(lib), "--report", rep, "--sources", "scihub"])[0] == 2
    _write_cfg(tmp_path, monkeypatch, hosts={"arxiv_pdf_allowed": "yes"})
    assert _main(["--triage", str(t), "--lib-dir", str(lib), "--report", rep])[0] == 2
    assert web.sent == []


def test_stage_dois_round_trip_to_sweeps_index():
    """Sweep looks a preprint row up by lit_util.normalize_doi of the triage DOI (already
    litpipe.doi-normalised by prepare_rows); the stage writes `_doi.normalise` of it again. A
    non-idempotent normaliser would leave the row PENDING and the queue unretired."""
    for raw in ("10.1002/(SICI)1097-4636(199601)30:1<1::AID-JBM1>3.0.CO;2-T", "https://doi.org/10.1056/NEJMc1113675#sa3",
                "10.48550/arXiv.2103.00020v2", "10.64898/2026.03.02.705347", "10.1371/journal.pone.0123456.",
                "doi:10.31236/osf.io/fkdby", "10.1000/a%2Fb"):
        n = D.normalise(raw)
        if n is None:
            continue
        row = P._Row({"doi": n, "title": "t"}, 0)
        assert lit_util.normalize_doi(row.doi_col) == lit_util.normalize_doi(n), raw


# ================================================================ 2. the seam, every preprint row shape
UNKNOWN_10_1101 = "10.1101/gr.277335.122"     # a Genome Research (CSHL Press) article, not a preprint


def collect_real_rows(env, monkeypatch):
    """{shape: report row} from the REAL preprint stage, one run per shape (the W2-C routes)."""
    got = {}

    def one(shape, rows, project="research_alpha", **kw):
        _, out = env.run(rows, project=project, **kw)
        assert len(out) == 1, (shape, out)
        got[shape] = out[0]

    refuse_arxiv(env.state)
    env.sources("osf")
    route_osf_ballet(env.web)
    one("OK", [{"doi": "10.1123/w2bve.ok1", "title": BALLET, "year": "2021", "authors": "Shaw J"}])
    for shape, namer, doi, year, author, title in (
            ("ALREADY_EXISTS_dec15", P.slug_filename, "10.1000/e.held1", "2020", "Smith J", "A held paper about heat"),
            ("ALREADY_EXISTS_legacy", P.legacy_slug_filename, "10.1000/e.held2", "2019", "Jones K", "Another held paper on cold")):
        name = namer(year, author, title)
        (env.lib / name).write_bytes(make_pdf(title))
        (env.lib / (name[:-4] + ".ris")).write_text(f"TY  - JOUR\nDO  - {doi}\nER  - \n", encoding="utf-8")
        one(shape, [{"doi": doi, "title": title, "year": year, "authors": author}])
    def clear_ballet():          # the OK row's preprint IS this paper ("same"): a later row would be held
        for p in env.lib.glob("*Ballet*"):
            p.unlink()

    clear_ballet()
    route_osf_ballet(env.web, pdf=make_pdf("Journal of Other Things\nAn unrelated study of soil microbes\n"
                                           "https://doi.org/10.9999/other.42"))
    one("FLAG", [{"doi": "10.1123/w2bve.flag1", "title": BALLET, "year": "2021", "authors": "Shaw J"}])
    clear_ballet()
    route_osf_ballet(env.web, pdf=make_pdf(f"Supplementary material for\n{BALLET}\nhttps://doi.org/10.31236/osf.io/fkdby"))
    one("SUPPLEMENT", [{"doi": "10.1123/w2bve.supp1", "title": BALLET, "year": "2021", "authors": "Shaw J"}])
    assert got["FLAG"]["identity"] == got["SUPPLEMENT"]["identity"] == "FLAG"
    route_osf_ballet(env.web)
    env.web.add("https://osf.io/download/f6yza/", (302, {"Location": "https://evil.example.net/x.pdf"}, b""))
    one("REFUSED_download_host", [{"doi": "10.1123/w2bve.blocked1", "title": BALLET, "year": "2021"}])
    env.web.add("https://osf.io/download/f6yza/", (404, {}, b""))
    one("NOT_AVAILABLE_dead_file", [{"doi": "10.1123/w2bve.dead1", "title": BALLET, "year": "2021"}])
    route_osf_ballet(env.web)
    env.web.advance["api.osf.io"] = 100.0
    one("DEFERRED_deadline", [{"doi": "10.1123/w2bve.deadline1", "title": BALLET}], row_deadline=30)
    env.web.advance.clear()
    env.state.budgets["api.osf.io"] = 0
    one("DEFERRED_osf_budget", [{"doi": "10.1123/w2bve.budget1", "title": BALLET}])
    del env.state.budgets["api.osf.io"]
    # bioRxiv / medRxiv and Europe PMC
    env.sources("biorxiv", "medrxiv", "europepmc_preprints")
    rec = epmc_record("epmc_search_doi_biorxiv_oa.json")
    outage_doi = "10.64898/2026.01.01.000001"
    route_epmc(env.web, by_doi={rec["doi"]: [rec], DOI_64898: [epmc_record("epmc_search_64898_biorxiv.json")],
                                outage_doi: [dict(epmc_record("epmc_search_64898_biorxiv.json"), doi=outage_doi)]})
    env.web.add(f"https://api.biorxiv.org/details/biorxiv/{rec['doi']}/na/json", (200, JSON, fx("biorxiv_details_64898.json")))
    jats = (f'<article><front><article-meta><article-id pub-id-type="doi">{rec["doi"]}</article-id><title-group>'
            f'<article-title>{rec["title"]}</article-title></title-group></article-meta></front>'
            '<body><sec><title>Results</title><p>' + "Fibre types adapt to the training load. " * 60 +
            '</p></sec></body></article>')               # a body, not only an abstract (is_abstract_only)
    env.web.add(f"https://www.ebi.ac.uk/europepmc/webservices/rest/{rec['id']}/fullTextXML",
                (200, {"Content-Type": "application/xml"}, jats))
    one("TEXT_ONLY_sidecar", [{"doi": rec["doi"], "title": rec["title"], "year": "2025", "authors": "Dilbaz S"}])
    env.web.add(f"https://api.biorxiv.org/details/biorxiv/{DOI_64898}/na/json", (200, JSON, fx("biorxiv_details_64898.json")))
    one("SKIPPED_manual_preprint", [{"doi": DOI_64898, "title": PAPER_64898, "year": "2026"}])
    env.web.add(f"https://api.biorxiv.org/details/biorxiv/{outage_doi}/na/json", (200, JSON, b""))
    one("OUTAGE_empty_details", [{"doi": outage_doi, "title": PAPER_64898}])
    one("NO_MATCH", [{"doi": "10.1123/w2bve.nomatch1", "title": "Heat acclimation and endurance in trained cyclists"}])
    env.web.add(EPMC, (200, JSON, json.dumps({"errCode": 503, "errMsg": "search unavailable"})))
    one("ERROR_epmc_errcode", [{"doi": "10.1123/w2bve.err1", "title": "Cold water immersion after sprint training"}])
    # a project with no preprint sources: excluded rows and the refused arXiv row
    one("SKIPPED_source_excluded", [{"doi": "10.51224/sportrxiv.e1", "title": "Goalkeeper loads"}],
        project="teaching_beta")
    one("REFUSED_arxiv", [{"doi": "10.48550/arxiv.2103.00020", "title": "T"}], project="teaching_beta")
    legacy = {k: v for k, v in got["SKIPPED_source_excluded"].items() if k not in P.TYPED_FIELDS}
    legacy["doi"] = "10.51224/sportrxiv.e2"
    got["SKIPPED_source_excluded_legacy_only"] = legacy
    return got


# shape -> (class, list entries the row must appear in)
ROUTES = {
    "OK": ("fetched", ()),
    "ALREADY_EXISTS_dec15": ("fetched", ()),
    "ALREADY_EXISTS_legacy": ("fetched", ()),
    "FLAG": ("IDENTITY_FLAG", (mig.REVIEW_NAME,)),
    "SUPPLEMENT": ("IDENTITY_FLAG", (mig.REVIEW_NAME,)),
    "TEXT_ONLY_sidecar": ("TEXT_ONLY", ()),
    "SKIPPED_source_excluded": ("TERMINAL_CLOSED", (mig.ILL_NAME,)),
    "SKIPPED_source_excluded_legacy_only": ("TERMINAL_CLOSED", (mig.ILL_NAME,)),
    "SKIPPED_manual_preprint": ("OA_BLOCKED", (mig.OA_BLOCKED_NAME, mig.RETRY_LATER_NAME)),
    "REFUSED_arxiv": ("TRANSIENT", (mig.RETRY_LATER_NAME,)),
    "REFUSED_download_host": ("OA_BLOCKED", (mig.OA_BLOCKED_NAME, mig.RETRY_LATER_NAME)),
    "DEFERRED_deadline": ("TRANSIENT", (mig.RETRY_LATER_NAME,)),
    "DEFERRED_osf_budget": ("TRANSIENT", (mig.RETRY_LATER_NAME,)),
    "OUTAGE_empty_details": ("TRANSIENT", (mig.RETRY_LATER_NAME,)),
    "NO_MATCH": ("TERMINAL_CLOSED", (mig.ILL_NAME,)),
    "ERROR_epmc_errcode": ("TRANSIENT", (mig.RETRY_LATER_NAME,)),
    "NOT_AVAILABLE_dead_file": ("TERMINAL_CLOSED", (mig.ILL_NAME,)),
}
LISTS = (mig.ILL_NAME, mig.OA_BLOCKED_NAME, mig.REVIEW_NAME, mig.RETRY_LATER_NAME)


def test_every_real_preprint_row_shape_routes_through_sweep_and_migrate(env, net_env, tmp_path, monkeypatch):
    rows = collect_real_rows(env, monkeypatch)
    assert set(rows) == set(ROUTES)
    shape_of = {r["doi"]: s for s, r in rows.items()}
    w = make_world(tmp_path, monkeypatch, {"P": {"lib_dir": "lit", "sources": OSF_SOURCES}})
    w.stages.replay = {r["doi"]: r for r in rows.values()}
    w.queue(list(shape_of))
    out = sweep.run(project="P", date=DAY, migrate=True)
    assert out["exit_code"] == sweep.EXIT_OK, out
    res = residual_of(w, w.proj)
    lists = {n: md(w.proj, n) for n in LISTS}
    table = []
    for doi, shape in shape_of.items():
        cls, where = ROUTES[shape]
        got = res[doi]["residual_class"] if doi in res else "fetched"
        table.append((shape, got))
        assert got == cls, (shape, got, res.get(doi, {}).get("reason"))
        for n in LISTS:
            assert (doi in lists[n]) == (n in where), (shape, n)
        v = sweep.preprint_verdict(w.stages.replay[doi])
        assert v.typed == (shape != "SKIPPED_source_excluded_legacy_only") or v.kind is Kind.OK, shape
    # the list entries carry what a person needs
    ppr = rows["SKIPPED_manual_preprint"]
    assert f"https://doi.org/{DOI_64898}" in res[DOI_64898]["landing_url"]
    assert "10.64898" in lists[mig.OA_BLOCKED_NAME]
    for shape in ("FLAG", "SUPPLEMENT"):
        r = res[rows[shape]["doi"]]
        assert r["flagged_path"].endswith(rows[shape]["preprint_filename"]), shape
    assert ppr["found"] == "True" and rows["REFUSED_arxiv"]["found"] == "False"
    # nothing persisted carries a leak
    for p in list(w.root.rglob("*")) + list(env.tmp.rglob("*.csv")):
        if p.is_file() and p.suffix in (".csv", ".md", ".json", ".jsonl"):
            text = p.read_text(encoding="utf-8", errors="replace")
            for leak in LEAKS:
                assert leak.lower() not in text.lower(), (p.name, leak)


def test_a_10_1101_doi_on_no_preprint_server_closes_not_retries_forever(env):
    """APPLY (preprint_fetch.py acquire ~1114 / biorxiv_details ~572): a 10.1101 DOI Europe PMC does
    not hold as a preprint (a CSHL Press journal article, or a DOI on neither server) gets "no posts
    found" from /details on every enabled server; W2-C's own live probe B2 shows that answer means
    "not on this server". It is written OUTAGE, so the row is TRANSIENT on every run, forever (no
    cap: only ERROR closes on the third run), and never reaches the ILL list."""
    env.sources("biorxiv", "medrxiv")
    route_epmc(env.web, by_doi={})
    for s in ("biorxiv", "medrxiv"):
        env.web.add(f"https://api.biorxiv.org/details/{s}/{UNKNOWN_10_1101}/na/json",
                    (200, JSON, fx("biorxiv_details_no_posts.json")))
    _, out = env.run([{"doi": UNKNOWN_10_1101, "title": "Chromatin loops in a mouse genome"}])
    assert out[0]["outcome"] in ("NO_MATCH", "NOT_AVAILABLE"), out[0]
    v = [sweep.unpaywall_verdict({"doi": UNKNOWN_10_1101, "downloaded": "False", "oa_status": "CLOSED",
                                  "error": "", "outcome": "NOT_AVAILABLE", "route": "api"}),
         sweep.pmc_verdict({"doi": UNKNOWN_10_1101, "downloaded": "False", "error": "NO_PMCID",
                            "outcome": "NO_MATCH", "pmcid": ""}),
         sweep.preprint_verdict(out[0])]
    assert sweep.classify(v, attempts=1)[0] == "TERMINAL_CLOSED"


# ================================================================ 3. DEC-31 with the real stage
def test_dec31_gate_with_the_real_stage_and_arxiv_refused(tmp_path, monkeypatch, web):
    w = make_world(tmp_path, monkeypatch, {"P": {"lib_dir": "lit"}})          # no `sources`: default
    w.queue(["10.1000/plain.1", "10.48550/arXiv.2103.00020"])
    out = sweep.run(project="P", date=DAY, migrate=True)
    assert out["exit_code"] == sweep.EXIT_OK
    (argv, rc, _, _), = w.stages.ppr
    assert rc == 0 and argv[argv.index("--project") + 1] == "P"
    triage = read_csv(argv[argv.index("--triage") + 1].replace(".residual.", ".residual."))
    assert web.sent == []                                              # arXiv refused: 0 requests
    res = residual_of(w, w.proj)
    assert res["10.48550/arxiv.2103.00020"]["residual_class"] == "TRANSIENT"
    assert res["10.1000/plain.1"]["residual_class"] == "TERMINAL_CLOSED"
    assert "preprint" in res["10.1000/plain.1"]["skipped_sources"]
    assert "10.48550/arxiv.2103.00020" in {r["doi"] for r in mig.read_retry_later(w.proj)[1]}
    assert triage                                                       # (overwritten by the residual)
    # no arXiv row: the stage is not run at all; --skip-preprint skips even the arXiv row
    w.queue(["10.1000/plain.2"], name="lit_pull_queue.b2.csv")
    w.queue(["10.48550/arXiv.2104.00001"], name="lit_pull_queue.b3.csv")
    sweep.run(project="P", date=DAY2, migrate=True, skip_preprint=True)
    w.queue(["10.1000/plain.3"])
    n = len(w.stages.ppr)
    sweep.run(project="P", date="2026-10-03", migrate=True)
    assert len(w.stages.ppr) == n == 1


# ================================================================ 5. signed URLs in the ledger
SIGNED_GCS = ("https://storage.googleapis.com/cos-osf-prod-files-us-east1/blob?response-content-disposition="
              "attachment%3B%20filename%3D%22x.pdf%22&GoogleAccessId=files-us%40cos-osf-prod.iam.gserviceaccount.com"
              "&Expires=1759700000&Signature=SECRETSIG%2Babc%3D")
SIGNED_V4 = ("https://storage.googleapis.com/cos-osf-prod-files-us-east1/blob?X-Goog-Algorithm=GOOG4-RSA-SHA256"
             "&X-Goog-Credential=SECRETCRED%2F20261005%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20261005T000000Z"
             "&X-Goog-Expires=60&X-Goog-SignedHeaders=host&X-Goog-Signature=SECRETSIG0123abcd")


@pytest.mark.parametrize("signed", [SIGNED_GCS, SIGNED_V4], ids=["v2_signature", "v4_x_goog"])
def test_the_osf_blob_hop_is_ledgered_without_its_signature(env, net_env, signed):
    """APPLY (litpipe/ledger.py redact, ~116): the OSF download's last hop is a signed, expiring
    storage.googleapis.com URL; net ledgers every hop's URL through redact(), which strips keys and
    emails but not Signature / X-Goog-Signature / X-Goog-Credential, so a bearer-equivalent download
    link lands in <state_dir>/ledger/*.jsonl."""
    env.sources("osf")
    route_osf_ballet(env.web)
    env.web.add("https://files.osf.io/v1/resources/fkdby_v1/providers/osfstorage/61112f14",
                (302, {"Location": signed}, b""))
    _, out = env.run([{"doi": "10.1123/w2bve.sig1", "title": BALLET, "year": "2021", "authors": "Shaw J"}])
    assert out[0]["downloaded"] == "True"
    assert any(u.startswith("https://storage.googleapis.com/") for _, u, _ in env.web.sent)
    text = net_env.ledger_text()
    assert "storage.googleapis.com" in text
    assert "SECRETSIG" not in text and "SECRETCRED" not in text
    assert "SECRETSIG" not in env.report_path.read_text(encoding="utf-8")
    for p in env.lib.glob("*.identity.json"):
        assert "SECRETSIG" not in p.read_text(encoding="utf-8")


@pytest.mark.parametrize("url", [
    SIGNED_GCS, SIGNED_V4,
    "https://b.s3.amazonaws.com/k.pdf?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=SECRETCRED%2F20261005"
    "&X-Amz-Security-Token=SECRETSIG-token&X-Amz-Signature=SECRETSIG99",
    "https://b.s3.amazonaws.com/k.pdf?AWSAccessKeyId=SECRETCRED&Expires=1&Signature=SECRETSIG%2F1%3D",
    "https%3A%2F%2Fstorage.googleapis.com%2Fb%3FX-Goog-Signature%3DSECRETSIG77%26X-Goog-Date%3D1",
    '{"url": "https://storage.googleapis.com/b?Signature=SECRETSIG5&Expires=9"}',
])
def test_redact_strips_signed_url_credentials(url):
    """APPLY (litpipe/ledger.py redact): the one redaction implementation strips signed-URL
    credentials (GCS v2 and v4, S3 SigV2 and SigV4) at any percent-encoding depth, keeping the
    parameter names and the non-secret ones (Expires, X-Goog-Date) so the ledger stays readable."""
    out = ledger.redact(url)
    assert "SECRETSIG" not in out and "SECRETCRED" not in out, out
    assert "Signature" in out or "signature" in out.lower()
    assert ledger.redact("Signature verification failed") == "Signature verification failed"


# ================================================================ 6/7. CONFIG abort, report rows, instruments
def test_a_config_abort_still_routes_the_queues_it_already_retired(tmp_path, monkeypatch, web):
    """APPLY (sweep.py run ~1437-1443 with migrate_closed_to_md.run ~1028): a project's first queue
    completes and is retired (renamed .processed); its second queue hits a stage exit 2 (CONFIG).
    W2-G now skips migrate for the whole project, so the first queue's TERMINAL_CLOSED row never
    reaches the ILL list, and the next run's migrate (a new run id) never routes it either. At
    2188c44 migrate still ran (and itself refused only a run whose rows carry CONFIG)."""
    w = make_world(tmp_path, monkeypatch, {"P": {"lib_dir": "lit", "sources": OSF_SOURCES}})
    no_osf_match(web)
    w.queue(["10.1000/first.1"])
    w.queue(["10.1000/second.1"], name="lit_pull_queue.b2.csv")
    w.stages.exit_for[("pmc_fetch.py", ".b2.")] = 2
    out = sweep.run(project="P", date=DAY, migrate=True)
    assert out["exit_code"] == sweep.EXIT_USAGE                       # the run aborts: right
    assert not (w.proj / "lit_pull_queue.csv").exists()                # the first queue retired
    assert (w.proj / "lit_pull_queue.b2.csv").exists()                 # the CONFIG queue stays
    w.stages.exit_for.clear()
    sweep.run(project="P", date=DAY2, migrate=True)                     # configuration fixed
    assert "10.1000/second.1" in md(w.proj, mig.ILL_NAME)
    assert "10.1000/first.1" in md(w.proj, mig.ILL_NAME), "the retired queue's closed row was never routed"


def test_every_run_writes_every_class_row_and_the_instruments_read_it(tmp_path, monkeypatch, web):
    """Sweep's report has a `section=class` row for EVERY class (zeros included) and the
    `run,destination` row; no residual row has a blank class; the REAL audit_portfolio.audit_queue
    reads the run with no UNKNOWN class and no disagreement between report and residual."""
    w = make_world(tmp_path, monkeypatch, {"P": {"lib_dir": "lit", "sources": OSF_SOURCES},
                                           "Q": {"lib_dir": "qlib"}})
    no_osf_match(web)
    s = w.stages.spec
    s["10.1000/blk.1"] = {"unpaywall": {"oa_status": "OA", "error": "HTTP_403"}}
    s["10.1000/dns.1"] = {"unpaywall": {"oa_status": "", "error": "ConnectionError: [Errno 11001] getaddrinfo failed"}}
    w.queue(["10.1000/closed.1", "10.1000/blk.1", "10.1000/dns.1", "NO_DOI_smith2020",
             {"doi": "10.1000/nometa.1", "title": "", "authors": ""}])
    out = sweep.run(project="P", date=DAY, migrate=True)
    assert out["exit_code"] == sweep.EXIT_OK
    rep = read_csv(w.proj / f"lit_pull_queue.{DAY}.processed.csv".replace(".processed.", ".report."))
    classes = {r["name"]: r["count"] for r in rep if r["section"] == "class"}
    assert list(classes) == list(sweep.REPORT_CLASSES)
    assert all(c.isdigit() for c in classes.values())
    dest = [r for r in rep if r["section"] == "run" and r["name"] == "destination"]
    assert len(dest) == 1 and Path(dest[0]["detail"]) == (w.proj / "lit").resolve()
    resid = read_csv(w.proj / f"lit_pull_queue.{DAY}.residual.csv")
    assert resid and all(r["residual_class"].strip() for r in resid)
    a = audit_portfolio.audit_queue(w.proj)
    assert a["runs"] == 1 and len(a["latest_runs"]) == 1
    run = a["latest_runs"][0]
    assert run["unknown"] == 0 and run["disagreement"] == {}, run
    assert run["classes"] == {k: int(v) for k, v in classes.items()}
    assert {i["class"] for i in run["items"]} <= set(sweep.REPORT_CLASSES)
    assert all("UNKNOWN" not in f["class"] for f in a["failures"])


# ================================================================ 4. prohibited routes behind a redirect
PROHIBITED_TARGETS = ("https://www.biorxiv.org/content/10.1101/2020.01.01.000001v1.full.pdf",
                      "https://arxiv.org/pdf/2103.00020", "https://europepmc.org/articles/PMC1?pdf=render",
                      "https://www.medrxiv.org/content/10.1101/x.full.pdf")


@pytest.mark.parametrize("switches", [{}, {"biorxiv_pdf_allowed": True, "arxiv_pdf_allowed": True}],
                         ids=["switches_off", "switches_on"])
def test_an_osf_or_sportrxiv_hop_never_lands_on_a_prohibited_host(env, switches):
    """Every hop is checked: a prohibited route (switch off) or a host outside the issuing host's
    allow-list (switch on: OSF's list is osf.io, .osf.io, storage.googleapis.com; SportRxiv's is its
    own host) is never sent; the row is REFUSED with found=True (OA_BLOCKED: a person opens it)."""
    env.sources("osf").switches(**switches)
    for target in PROHIBITED_TARGETS:
        route_osf_ballet(env.web)
        env.web.add("https://files.osf.io/v1/resources/fkdby_v1/providers/osfstorage/61112f14",
                    (302, {"Location": target}, b""))
        _, out = env.run([{"doi": "10.1123/w2bve.hop1", "title": BALLET, "year": "2021"}])
        assert out[0]["outcome"] == "REFUSED" and out[0]["found"] == "True", (target, out[0])
        dl = P.SPORTRXIV_DOWNLOAD.format(id="1079", galley="2185")
        env.web.add(dl, (302, {"Location": target}, b""))
        o = P._download(dl, state=None, cfg=env.cfg, purpose="test")
        assert o.kind is Kind.REFUSED and o.status == 302, (target, o)
    sent = {u.split("?")[0] for _, u, _ in env.web.sent}
    assert not sent & {t.split("?")[0] for t in PROHIBITED_TARGETS}


def test_the_crossref_work_url_encodes_the_doi(monkeypatch):
    """DOI Handbook 4.7 on the one path W2-C's encoding test does not reach: /works/{doi}."""
    seen = []
    monkeypatch.setattr(P.net, "get", lambda url, **kw: seen.append(url) or P.Outcome(Kind.NO_MATCH))
    P.crossref_sportrxiv_work("10.1002/(SICI)1097-4636(199601)30:1<1::AID-JBM1>3.0.CO;2-T")
    assert seen == ["https://api.crossref.org/works/10.1002/(sici)1097-4636(199601)30:1%3C1::aid-jbm1%3E3.0.co;2-t"]


# ================================================================ W2-G open questions (recommendations)
# W2-G open question 1: recommendation adopted by the dispatcher (2026-10-05)
@pytest.mark.parametrize("klass,outcome", [("OA", "NOT_AVAILABLE"), ("NONE", "NOT_AVAILABLE")])
def test_open_q1_an_oa_or_none_class_text_sidecar_is_not_sent_to_ill(klass, outcome):
    pmc = {"doi": "10.1000/q1", "downloaded": "False", "skipped": "False", "pmcid": "PMC1", "error": "NOT_AVAILABLE",
           "sidecar": "True", "sidecar_status": "OK", "outcome": outcome, "identity": "", "pmc_class": klass}
    upw = {"doi": "10.1000/q1", "downloaded": "False", "oa_status": "CLOSED", "error": "", "outcome": "NOT_AVAILABLE",
           "route": "api"}
    assert sweep.classify([sweep.unpaywall_verdict(upw), sweep.pmc_verdict(pmc)], attempts=1)[0] == "TEXT_ONLY"


# W2-G open question 2: recommendation adopted by the dispatcher (2026-10-05)
def test_open_q2_metadata_unavailable_after_an_outage_is_retried(tmp_path, monkeypatch, web):
    import ris_emit
    w = make_world(tmp_path, monkeypatch, {"P": {"lib_dir": "lit"}})
    monkeypatch.setattr(sweep, "_resolve_meta", lambda doi: (_ for _ in ()).throw(ris_emit.MetadataUnavailable("crossref 503")))
    w.queue([{"doi": "10.1000/blank.1", "title": "", "authors": ""}])
    sweep.run(project="P", date=DAY, migrate=True)
    assert residual_of(w, w.proj)["10.1000/blank.1"]["reason"] == sweep.NO_METADATA_UNAVAILABLE
    assert "10.1000/blank.1" in {r["doi"] for r in mig.read_retry_later(w.proj)[1]}


# ================================================================ 9. CI parity: --help from a foreign cwd
@pytest.mark.parametrize("script", ["preprint_fetch.py", "sweep.py", "migrate_closed_to_md.py"])
def test_help_from_a_foreign_cwd(script, tmp_path):
    p = subprocess.run([sys.executable, str(Path(sweep.__file__).resolve().parent / script), "--help"],
                       cwd=str(tmp_path), capture_output=True, text=True, timeout=120)
    assert p.returncode == 0, p.stderr
