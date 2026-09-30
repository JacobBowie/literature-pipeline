"""W1 verifier B: the sweep -> migrate seam end to end, and the gaps the builders could not test.

The integration tests run the REAL `sweep.run(...)` with `migrate=True` on a temp root and a temp
registry. Only the stage scripts are stubbed (a subprocess.run stand-in writing stage reports in the
live column shapes of tests/fixtures/W1-D1 and W1-D2); the migrate subprocess is answered by the
REAL `migrate_closed_to_md.main(argv)` with exactly the argv sweep built, and the hold check uses
the REAL `litpipe.holdings.build` over temp libraries. No network: the metadata resolver fails.

The six APPLY tests (settling resolver forms and placeholders, the shared DOI normaliser in
holdings, a wrapped 5+ digit article number, migrate --dry-run writing nothing, run ids unique
across --artifact-dir, not_before from the run date) failed on e7924aa and lock the fixes that
landed after it.
"""
import contextlib
import csv
import datetime
import io
import json
import types
from pathlib import Path

import pytest

import lit_util
import migrate_closed_to_md as mig
import sweep
from litpipe import config, holdings
from litpipe import doi as D
from litpipe.ledger import redact

FIX = Path(__file__).parent / "fixtures"
DAY1, DAY2, DAY3 = "2026-10-01", "2026-10-02", "2026-10-03"
UNPW_FIELDS = ["rank", "doi", "year", "cites", "filename", "title", "oa_status", "n_locations",
               "downloaded", "winning_host", "winning_url", "attempts", "error"]
PMC_FIELDS = ["doi", "filename", "pmcid", "downloaded", "skipped", "winning_source", "attempts",
              "error", "sidecar", "sidecar_status"]
PPR_FIELDS = ["doi", "title", "year", "preprint_filename", "found", "source", "match_id",
              "similarity", "downloaded", "skipped", "status"]
LEAKS = ("email=", "mailto:", "fixture.user", "%40example", "@example")


def _dns_error(doi):
    """The live Unpaywall DNS failure (W1-D2 fixture), email included, re-pointed at `doi`."""
    with open(FIX / "W1-D2" / "chain_real" / "lit_pull_queue.2026-09-22.unpaywall.csv",
              encoding="utf-8", newline="") as f:
        r = next(csv.DictReader(f))
    return r["error"].replace(r["doi"], doi)


def read_csv(path):
    with open(path, encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path, rows, fields):
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        w.writeheader()
        w.writerows(rows)


class _FrozenDate(datetime.date):
    frozen = None

    @classmethod
    def today(cls):
        return cls.frozen


class Stages:
    """subprocess.run stand-in. Stage scripts write reports from `spec[doi][stage]` (defaults
    CLOSED / NO_PMCID / NO_MATCH); migrate_closed_to_md.py runs the real main() in-process."""

    def __init__(self):
        self.spec, self.calls, self.migrations = {}, [], []

    def __call__(self, cmd, **kw):
        cmd = [str(c) for c in cmd]
        script = next(Path(c).name for c in cmd if c.endswith(".py"))
        self.calls.append((script, cmd))
        arg = lambda flag: cmd[cmd.index(flag) + 1]
        if script == "migrate_closed_to_md.py":
            out, err = io.StringIO(), io.StringIO()
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                rc = mig.main(cmd[2:])
            self.migrations.append((cmd[2:], rc, out.getvalue(), err.getvalue()))
            return types.SimpleNamespace(returncode=rc, stdout=out.getvalue(), stderr=err.getvalue())
        if script == "unpaywall_fetch_v2.py":
            rows = []
            for r in read_csv(arg("--triage")):
                d = r["doi"].strip().lower()
                row = {"doi": d, "title": r.get("title", ""), "oa_status": "CLOSED",
                       "downloaded": "False", "error": ""}
                row.update(self.spec.get(d, {}).get("unpaywall", {}))
                rows.append(row)
            write_csv(arg("--report"), rows, UNPW_FIELDS)
        elif script == "pmc_fetch.py":
            rows = []
            for r in read_csv(arg("--report-in")):
                if r["downloaded"] == "True" or r["oa_status"] == "SKIP_EXISTS":
                    continue
                d = r["doi"].strip().lower()
                row = {"doi": d, "downloaded": "False", "skipped": "False", "error": "NO_PMCID",
                       "sidecar": "False"}
                row.update(self.spec.get(d, {}).get("pmc", {}))
                rows.append(row)
            write_csv(arg("--report-out"), rows, PMC_FIELDS)
        elif script == "preprint_fetch.py":
            rows = []
            for r in read_csv(arg("--triage")):
                d = r["doi"].strip().lower()
                row = {"doi": d, "downloaded": "False", "skipped": "False", "status": "NO_MATCH"}
                row.update(self.spec.get(d, {}).get("preprint", {}))
                rows.append(row)
            write_csv(arg("--report"), rows, PPR_FIELDS)
        return types.SimpleNamespace(returncode=0, stdout="", stderr="")

    def scripts(self):
        return [s for s, _ in self.calls]


class World:
    def __init__(self, tmp_path, monkeypatch):
        self.tmp = tmp_path
        self.root = tmp_path / "Projects"
        self.state = tmp_path / "state"
        self.cfg_path = tmp_path / "projects.json"
        self.cfg = {"root": str(self.root), "state_dir": str(self.state),
                    "loose_ends": "Ops/LOOSE_ENDS.md",
                    "projects": {"P": {"lib_dir": "lit"}, "Q": {"lib_dir": "qlib"}}}
        self.cfg_path.write_text(json.dumps(self.cfg), encoding="utf-8")
        self.proj = self.root / "P"
        self.lib = self.proj / "lit"
        self.qlib = self.root / "Q" / "qlib"
        for d in (self.lib, self.qlib):
            d.mkdir(parents=True)
        for mod in (sweep, mig, config):
            monkeypatch.setattr(mod, "CONFIG_PATH", self.cfg_path)
        monkeypatch.setattr(lit_util, "PROJECTS_ROOT", self.root)
        monkeypatch.setenv("USERPROFILE", str(tmp_path / "home"))
        monkeypatch.setenv("HOME", str(tmp_path / "home"))
        monkeypatch.setattr(sweep, "_resolve_meta", self._no_meta)
        self.stages = Stages()
        monkeypatch.setattr(sweep.subprocess, "run", self.stages)
        monkeypatch.setattr(mig, "datetime", types.SimpleNamespace(
            date=_FrozenDate, timedelta=datetime.timedelta))
        _FrozenDate.frozen = datetime.date.today()   # no leak from an earlier test

    @staticmethod
    def _no_meta(doi):
        raise RuntimeError(f"lookup failed for {doi} ?email=fixture.user%40example.org")

    def queue(self, rows, name="lit_pull_queue.csv"):
        out = []
        for r in rows:
            if isinstance(r, str):
                r = {"doi": r, "title": "A study of heat", "authors": "Smith J"}
            out.append({"year": "2020", "destination": "lit", "notes": "", **r})
        write_csv(self.proj / name, out, list(sweep.QUEUE_COLUMNS))

    def sweep(self, day, **kw):
        _FrozenDate.frozen = datetime.date.fromisoformat(day)
        return sweep.run(project="P", date=day, migrate=True, **kw)

    def md(self, name):
        p = self.proj / name
        return p.read_text(encoding="utf-8") if p.exists() else ""

    def retry(self):
        return {r["doi"]: r for r in mig.read_retry_later(self.proj)[1]}

    def written_by_us(self):
        """Every file sweep or migrate wrote (stage reports are the stages' own files)."""
        return [p for p in self.root.rglob("*") if p.is_file()
                and not p.name.endswith((".unpaywall.csv", ".pmc.csv", ".preprint.csv"))
                and p.suffix in (".csv", ".md")]


@pytest.fixture
def world(tmp_path, monkeypatch):
    return World(tmp_path, monkeypatch)


A, B, C, Dm, E, F, G, H, I, J = (
    "10.1000/closed1", "10.1000/blocked1", "10.1000/dns1", "10.1000/mismatch1", "10.1000/held1",
    "10.1000/textonly1", "10.1000/sidecar1", "NO_DOI_smith2020", "10.1000/nometa1", "10.1000/err1")
K, L, M, N = "10.1000/alphaclosed1", "10.1000/alphablocked1", "10.1000/closed2", "10.1000/dns2"
BLOCK = {"oa_status": "OA", "attempts": "publisher/publishedVersion/HTTP_403", "error": "HTTP_403"}
ERR404 = {"oa_status": "OA", "attempts": "publisher/publishedVersion/HTTP_404", "error": "HTTP_404"}


def _libraries(w):
    (w.qlib / "2019_Held_Paper.pdf").write_bytes(b"%PDF-1.4\n")
    (w.qlib / "2019_Held_Paper.ris").write_text(
        "TY  - JOUR\nDO  - https://doi.org/10.1000/HELD1\nER  - \n", encoding="utf-8")
    (w.qlib / "2020_Text_Only.fulltext.json").write_text(
        json.dumps({"doi": "10.1000/TEXTONLY1", "text": "Body text of the article."}), encoding="utf-8")


def _day1_spec(w):
    s = w.stages.spec
    s[B] = s[L] = {"unpaywall": BLOCK}
    s[C] = {"unpaywall": {"oa_status": "", "error": _dns_error(C)}}
    s[N] = {"unpaywall": {"oa_status": "", "error": _dns_error(N)}}
    s[Dm] = {"unpaywall": {"oa_status": "OA", "attempts": "publisher/publishedVersion/OK",
                           "error": "DOI_MISMATCH: the file names 10.9999/other"}}
    s[F] = {"unpaywall": {"oa_status": "OA", "downloaded": "True", "winning_host": "publisher"}}
    s[G] = {"pmc": {"pmcid": "PMC1234567", "error": "HTML", "filename": "2020_X_Sidecar.pdf",
                    "attempts": "europepmc/HTTP_500 | ncbi-page/HTML", "sidecar": "True",
                    "sidecar_status": "OK"}}
    s[J] = {"unpaywall": ERR404}


def test_sweep_into_migrate_end_to_end(world):
    """Every residual class lands where dispatch 0.5 says, across two same-day runs (the second
    with --skip-preprint and --artifact-dir), a tagged queue, and three days of retry_later
    re-admission: attempts count up, an ERROR row closes on its third run, a transport failure
    never closes, and nothing sweep or migrate wrote carries an email."""
    w = world
    _libraries(w)
    _day1_spec(w)
    assert D.normalise("x") is None   # the module under test imports cleanly

    # ---- day 1, run 1: untagged + tagged queue
    w.queue([A, B, C, Dm, E, F, G, H, {"doi": I, "title": "", "authors": ""}, J])
    w.queue([K, L], name="lit_pull_queue.alpha.csv")
    res = w.sweep(DAY1)
    assert res["exit_code"] == sweep.EXIT_OK
    p = res["projects"]["P"]
    assert p["run_id"] == DAY1
    assert [r["retired"] for r in p["results"]] == [True, True]
    args, rc, out, err = w.stages.migrations[-1]
    assert rc == 0, (out, err)
    assert args[args.index("--date") + 1] == DAY1 and "--skip-preprint" not in args
    # the real holdings map: a PDF elsewhere is held, a text-only sidecar elsewhere is fetched
    unpw = {r["doi"] for r in read_csv(w.proj / f"lit_pull_queue.{DAY1}.unpaywall.csv")}
    assert E not in unpw and F in unpw and I not in unpw
    residual = {r["doi"]: r for r in read_csv(w.proj / f"lit_pull_queue.{DAY1}.residual.csv")}
    assert {d: r["residual_class"] for d, r in residual.items()} == {
        A: "TERMINAL_CLOSED", B: "OA_BLOCKED", C: "TRANSIENT", Dm: "IDENTITY_FLAG",
        E: "HELD_ELSEWHERE", G: "TEXT_ONLY", H: "INVALID_DOI", I: "NO_METADATA", J: "TRANSIENT"}
    assert residual[E]["held_at"].endswith("2019_Held_Paper.pdf")

    ill, oab, rev = (w.md("lit_pull_queue.md"), w.md("lit_pull_queue.oa_blocked.md"),
                     w.md("lit_pull_queue.review.md"))
    assert A in ill and K in ill and "[alpha]" in ill
    for d in (B, C, Dm, E, F, G, I, J, L):
        assert d not in ill, d
    assert B in oab and L in oab and all(d not in oab for d in (A, C, Dm, E, G, J))
    assert Dm in rev and all(d not in rev for d in (A, B, C, E, G, J))
    rl = w.retry()
    assert set(rl) == {B, C, J, L}
    assert rl[B]["residual_class"] == "OA_BLOCKED" and rl[B]["not_before"] == "2026-10-04"
    assert rl[C]["not_before"] == DAY2 and rl[J]["not_before"] == DAY2
    assert {rl[d]["attempts"] for d in rl} == {"1"}
    routing = {r["doi"]: r for r in read_csv(w.proj / f"lit_pull_queue.{DAY1}.routing.csv")}
    assert routing[E]["route"] == "none" and "2019_Held_Paper.pdf" in routing[E]["held_paths"]
    assert (w.proj / f"lit_pull_queue.alpha.{DAY1}.routing.csv").exists()

    # ---- day 1, run 2: same day, --skip-preprint, --artifact-dir
    w.queue([A, M, N])
    res = w.sweep(DAY1, skip_preprint=True, artifact_dir="runs")
    assert res["exit_code"] == sweep.EXIT_OK
    p = res["projects"]["P"]
    assert p["run_id"] == f"{DAY1}.2" and p["results"][0]["retired"]
    args, rc, out, err = w.stages.migrations[-1]
    assert rc == 0, (out, err)
    assert args[args.index("--date") + 1] == f"{DAY1}.2"
    assert args[args.index("--artifact-dir") + 1] == "runs" and "--skip-preprint" in args
    runs = w.proj / "runs"
    for stage in ("unpaywall", "pmc", "residual", "report", "processed", "routing"):
        assert (runs / f"lit_pull_queue.{DAY1}.2.{stage}.csv").exists(), stage
    assert not (runs / f"lit_pull_queue.{DAY1}.2.preprint.csv").exists()
    assert not list(w.proj.glob(f"lit_pull_queue.{DAY1}.2.*"))
    ill = w.md("lit_pull_queue.md")
    assert M in ill and f"{DAY1}.2" in ill and ill.count(A) == 1   # deduped across runs
    assert w.retry()[N]["not_before"] == DAY2
    lines = [x for x in (w.root / "Ops" / "LOOSE_ENDS.md").read_text(encoding="utf-8").splitlines()
             if x.strip()]
    assert lines and not any(x.startswith(sweep.LOOSE_PARTIAL) for x in lines)

    # ---- day 2: C, J, N are due; B and L wait until 10-04
    w.stages.spec[C] = {"unpaywall": {"oa_status": "OA", "downloaded": "True"}}
    res = w.sweep(DAY2)
    assert res["exit_code"] == sweep.EXIT_OK
    assert res["admitted"]["P"]["admitted"] == 3 and res["admitted"]["P"]["waiting"] == 2
    assert (w.proj / f"lit_pull_queue.retry.{DAY2}.processed.csv").exists()
    assert not (w.proj / "lit_pull_queue.retry.csv").exists()
    rl = w.retry()
    assert set(rl) == {B, L, J, N}
    assert rl[J]["attempts"] == "2" and rl[J]["not_before"] == DAY3
    assert rl[N]["attempts"] == "2" and rl[B]["attempts"] == "1"

    # ---- day 3: J closes on its third run; N (transport) stays transient
    res = w.sweep(DAY3)
    assert res["exit_code"] == sweep.EXIT_OK
    rl = w.retry()
    assert set(rl) == {B, L, N} and rl[N]["attempts"] == "3"
    ill = w.md("lit_pull_queue.md")
    j_line = next(x for x in ill.splitlines() if J in x)
    assert "error in 3 runs" in j_line
    assert sum(B in x for x in w.md("lit_pull_queue.oa_blocked.md").splitlines()) == 1

    # ---- nothing sweep or migrate persisted carries an email or a mailto
    files = w.written_by_us()
    assert any(f.name == "lit_pull_queue.retry_later.csv" for f in files)
    for f in files:
        text = f.read_text(encoding="utf-8")
        for leak in LEAKS:
            assert leak not in text, (f.name, leak)


def test_migrate_reroute_of_the_same_run_is_idempotent(world):
    """Re-running migrate for a run already routed (a wrapper retry) adds nothing and does not
    count the run twice."""
    w = world
    _libraries(w)
    _day1_spec(w)
    w.queue([A, B, J])
    assert w.sweep(DAY1)["exit_code"] == 0
    before = {n: w.md(n) for n in ("lit_pull_queue.md", "lit_pull_queue.oa_blocked.md")}
    rl_before = w.retry()
    with contextlib.redirect_stdout(io.StringIO()):
        assert mig.main(["--project", "P", "--date", DAY1]) == 0
    assert {n: w.md(n) for n in before} == before
    assert {d: (r["attempts"], r["not_before"]) for d, r in w.retry().items()} == \
        {d: (r["attempts"], r["not_before"]) for d, r in rl_before.items()}


def test_every_persisting_redactor_is_the_ledger_one():
    """The interim redactors were removed (dispatcher integration): sweep and migrate must give
    the ledger's output, and no form of an email survives any of them."""
    samples = ["/v2/10.1/x?email=a.b%40c.org", "UA mailto:a@b.org", "email=a%2540b.c&x=1",
               "Location: https://x/?tool=t&email=a%40b.c"]
    for s in samples:
        assert sweep._redact(s) == redact(s) == mig.redact(s)
        for leak in ("email=", "mailto:", "%40b", "@b.org"):
            assert leak not in redact(s), (s, leak)


# ---------------------------------------------------------------- defects reported as APPLY
def test_sweep_settles_placeholders_and_fetches_resolver_forms(world):
    """A placeholder or truncated DOI must not be fetched and then listed for ILL; a DOI given as
    an http:// or dx link, %2F-encoded, or with a PDF hyphen must be fetched, not dropped as
    INVALID_DOI (litpipe.doi.normalise is the one normaliser)."""
    w = world
    w.queue(["10.1145/nnnnnnn.nnnnnnn", "10.1002/cphy", "http://doi.org/10.1000/ok1",
             "https://dx.doi.org/10.1000/ok2", "10.1000%2Fok3", "10.1000/ok‐4"])
    w.sweep(DAY1)
    triage = {r["doi"] for r in read_csv(w.proj / f"lit_pull_queue.{DAY1}.unpaywall.csv")}
    assert triage == {"10.1000/ok1", "10.1000/ok2", "10.1000/ok3", "10.1000/ok-4"}
    assert "nnnnnnn" not in w.md("lit_pull_queue.md") and "10.1002/cphy" not in w.md("lit_pull_queue.md")


def test_holdings_keys_a_sidecar_doi_the_way_sweep_keys_the_queue(tmp_path, monkeypatch):
    """A sidecar DOI with a PDF hyphen (U+2010) or a glued running head, and a .ris DOI with a
    URL fragment, name papers the queue asks for by their clean DOI; the hold check must see
    them, or sweep downloads a second copy."""
    root = tmp_path / "Projects"
    lib = root / "Q" / "qlib"
    lib.mkdir(parents=True)
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    for i, raw in enumerate(("10.1000/abc‐1", "10.1177/0022146515298293Stearns")):
        (lib / f"p{i}.pdf").write_bytes(b"%PDF")
        (lib / f"p{i}.fulltext.json").write_text(json.dumps({"doi": raw, "text": "t"}), encoding="utf-8")
    (lib / "p9.pdf").write_bytes(b"%PDF")
    (lib / "p9.ris").write_text("TY  - JOUR\nDO  - 10.1056/NEJMc1113675#sa3\nER  - \n", encoding="utf-8")
    hm = holdings.build({"Q": {"lib_dir": "qlib"}}, cache_dir=tmp_path / "c")
    for queue_doi in ("10.1000/abc-1", "10.1177/0022146515298293", "10.1056/nejmc1113675"):
        assert hm.has_pdf(lit_util.normalize_doi(queue_doi)), queue_doi


def test_an_article_number_wrapped_after_the_year_is_rejoined():
    text = "J Therm Biol 84 (2019). https://doi.org/10.1016/j.jtherbio.2019.\n102456 Received 3 May"
    assert lit_util.extract_doi_from_text(text) == "10.1016/j.jtherbio.2019.102456"
    # the V2-N2 line-number and year cases stay refused
    assert D.normalise("doi: 10.1002/cphy.c100082.\n834 Next") == "10.1002/cphy.c100082"
    assert D.normalise("10.1371/journal.pone.0055660.\n2013 Another") == "10.1371/journal.pone.0055660"


def test_migrate_dry_run_writes_nothing_anywhere(world):
    """`--dry-run` says "write nothing"; a legacy chain makes migrate build the holdings map, whose
    cache lands under state_dir."""
    w = world
    write_csv(w.proj / f"lit_pull_queue.{DAY1}.unpaywall.csv",
              [{"doi": A, "title": "T", "oa_status": "CLOSED", "downloaded": "False", "error": ""}],
              UNPW_FIELDS)
    write_csv(w.proj / f"lit_pull_queue.{DAY1}.pmc.csv",
              [{"doi": A, "downloaded": "False", "error": "NO_PMCID"}], PMC_FIELDS)
    _FrozenDate.frozen = datetime.date.fromisoformat(DAY1)
    with contextlib.redirect_stdout(io.StringIO()):
        assert mig.main(["--project", "P", "--date", DAY1, "--dry-run"]) == 0
    assert not w.state.exists() or not any(w.state.rglob("*"))


def test_run_ids_stay_unique_when_artifact_dir_is_used_on_one_run_only(world):
    w = world
    w.queue([A])
    assert w.sweep(DAY1, artifact_dir="runs")["projects"]["P"]["run_id"] == DAY1
    w.queue([M])
    assert w.sweep(DAY1)["projects"]["P"]["run_id"] == f"{DAY1}.2"


def test_not_before_follows_the_run_date_when_it_is_ahead_of_the_clock(world):
    """sweep --date D --migrate admits retry rows by D, but migrate sets not_before from the wall
    clock, so a run dated ahead of the clock (a runner run date, a test) re-admits a TRANSIENT row
    in the same day's next run (seen in the CLI drive: dns1 swept twice on 2026-10-01)."""
    w = world
    _FrozenDate.frozen = datetime.date(2026, 9, 30)          # the wall clock
    fields = list(sweep.QUEUE_COLUMNS) + ["residual_class", "reason", "stages", "held_at",
                                          "skipped_sources", "attempts", "run_id"]
    write_csv(w.proj / "lit_pull_queue.2026-10-05.residual.csv",
              [{"doi": C, "title": "T", "authors": "A", "destination": "lit",
                "residual_class": "TRANSIENT", "reason": "unpaywall: TRANSPORT", "attempts": "1",
                "run_id": "2026-10-05"}], fields)
    with contextlib.redirect_stdout(io.StringIO()):
        assert mig.main(["--project", "P", "--date", "2026-10-05"]) == 0
    assert w.retry()[C]["not_before"] == "2026-10-06"
