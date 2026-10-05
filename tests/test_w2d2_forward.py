"""forward_citations on litpipe.s2: the degraded-walk guard (dispatch W2-D2, K3).

Offline: S2Stub replaces litpipe.net's transports and answers like Semantic Scholar's
POST /paper/batch and paged GET /paper/{id}/citations (offset, limit, next); retry waits and
pacing run on the net_env virtual clock. Libraries are temp folders of empty PDFs whose DOI
comes from a `.ris` sidecar."""
import ast
import csv
import hashlib
import json
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

import pytest
from requests.structures import CaseInsensitiveDict

import forward_citations as fc
from litpipe import hosts, net, s2

REPO = Path(__file__).resolve().parent.parent
SECRET = "s2-TESTKEY-w2d2-9f8e7d6c5b4a"


# ------------------------------------------------------------------------------ the S2 stub
def _raw(status, body, headers=None):
    data = body if isinstance(body, bytes) else json.dumps(body).encode("utf-8")
    h = CaseInsensitiveDict({"Content-Type": "application/json", **(headers or {})})
    return net._Raw(status, h, data, data[:net.CHUNK], len(data))


def pid_of(doi):
    return hashlib.sha1(doi.encode()).hexdigest()


def citer(i, seed):
    return {"citingPaper": {"paperId": pid_of(f"{seed}/c{i}"), "externalIds": {"DOI": f"10.9999/{seed[-6:]}.c{i}"},
                            "title": f"Citer {i}", "abstract": None, "year": 2020 + i % 5,
                            "authors": [{"authorId": "1", "name": "Ann Author"}, {"name": None}],
                            "citationCount": i, "venue": "J Test"}}


class S2Stub:
    """Semantic Scholar on a stub transport. papers: DOI -> {"count": int, "rows": n or list}.
    `fail(doi, status, times=None)` makes that seed's citations answer `status` (`times` times,
    else always); `fail_after = (n, status)` makes every citations call after the n-th answer it;
    `batch_status` fails POST /paper/batch; `interrupt_on` (a paperId) raises KeyboardInterrupt
    mid-run, as a kill would."""

    def __init__(self):
        self.papers = {}
        self.sent = []
        self.failing = {}
        self.fail_after = None
        self.batch_status = None
        self.interrupt_on = None
        self.n_citation_calls = 0

    def add(self, doi, count, rows=None):
        rows = count if rows is None else rows
        self.papers[doi] = {"count": count, "rows": [citer(i, doi) for i in range(rows)] if isinstance(rows, int)
                            else rows, "pid": pid_of(doi)}
        return self

    def fail(self, doi, status, times=None):
        self.failing[pid_of(doi)] = [status, times]
        return self

    def by_pid(self, pid):
        return next((p for p in self.papers.values() if p["pid"] == pid), None)

    def citation_paths(self):
        return [r["path"] for r in self.sent if r["path"].endswith("/citations")]

    def __call__(self, method, url, hdrs, body, timeout, max_bytes):
        parts = urlsplit(url)
        path = unquote(parts.path)
        q = {k: v[-1] for k, v in parse_qs(parts.query).items()}
        js = json.loads(body) if body else None
        self.sent.append({"method": method, "path": path, "params": q, "headers": dict(hdrs), "json": js})
        if path == "/graph/v1/paper/batch":
            if self.batch_status:
                return _raw(self.batch_status, {"message": "scripted"})
            out = []
            for i in js["ids"]:
                p = self.papers.get(i[len("DOI:"):])
                out.append(None if p is None else {"paperId": p["pid"], "citationCount": p["count"]})
            return _raw(200, out)
        if path.endswith("/citations"):
            pid = path.split("/")[-2]
            self.n_citation_calls += 1
            if self.interrupt_on == pid:
                raise KeyboardInterrupt("scripted kill")
            if self.fail_after is not None and self.n_citation_calls > self.fail_after[0]:
                return _raw(self.fail_after[1], {"message": "Too Many Requests"})
            f = self.failing.get(pid)
            if f and (f[1] is None or f[1] > 0):
                if f[1] is not None:
                    f[1] -= 1
                return _raw(f[0], {"message": "scripted failure"})
            p = self.by_pid(pid)
            if p is None:
                return _raw(404, {"error": "Paper not found"})
            off, lim = int(q["offset"]), int(q["limit"])
            if lim > 1000 or off + lim >= 10000:
                return _raw(400, {"error": "offset + limit must be < 10000"})
            page = {"offset": off, "data": p["rows"][off:off + lim]}
            if off + lim < len(p["rows"]):
                page["next"] = off + lim
            return _raw(200, page)
        return _raw(404, {"error": f"no route {path}"})


@pytest.fixture
def s2env(net_env, monkeypatch):
    monkeypatch.delenv(s2.KEY_ENV, raising=False)
    monkeypatch.setattr(s2, "BASE", "https://api.semanticscholar.org")
    s2.reset_default_session()
    yield net_env
    s2.reset_default_session()
    hosts.reset()


@pytest.fixture
def stub(s2env, monkeypatch):
    st = S2Stub()
    monkeypatch.setitem(net._TRANSPORTS, "requests", st)
    monkeypatch.setitem(net._TRANSPORTS, "urllib", st)
    return st


def make_lib(tmp_path, dois, name="literature"):
    lib = tmp_path / name
    lib.mkdir(parents=True, exist_ok=True)
    for i, d in enumerate(dois):
        pdf = lib / f"2020_Seed{i:04d}.pdf"
        pdf.write_bytes(b"%PDF-1.4 stub")
        if d:
            (lib / f"2020_Seed{i:04d}.ris").write_text(f"TY  - JOUR\nDO  - {d}\nER  - \n", encoding="utf-8")
    return lib


def dois(n, prefix="10.5555/seed"):
    return [f"{prefix}.{i:04d}" for i in range(n)]


def read_csv(path):
    with open(path, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def write_prior(lib, rows_by_doi):
    """A published report with `n` rows for each DOI (old-walker shape: CRLF rows)."""
    out = lib / "_forward_citations.csv"
    with open(out, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fc.FIELDS)
        w.writeheader()
        for k, (doi, n) in enumerate(rows_by_doi.items()):
            for i in range(n):
                w.writerow({"seed_pdf": f"2020_Seed{k:04d}.pdf", "seed_doi": doi, "citing_paper_id": f"old{i}",
                            "citing_doi": f"10.7777/old.{k}.{i}", "citing_title": "Old citer"})
    return out


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


# ================================================================ clean walk
def test_clean_walk_pages_past_1000_and_publishes(stub, tmp_path):
    ds = dois(3)
    stub.add(ds[0], 1011).add(ds[1], 0).add(ds[2], 5)
    lib = make_lib(tmp_path, ds)
    res = fc.run(lib_dir=str(lib))
    assert res["exit_code"] == 0 and res["published"] and res["status"] == "ok"
    rows = read_csv(lib / "_forward_citations.csv")
    assert len(rows) == 1016 and res["total_rows"] == 1016
    assert sum(1 for r in rows if r["seed_doi"] == ds[0]) == 1011          # 2 pages, not truncated at 1,000
    assert res["zero_citers"] == 1 and res["failed"] == 0
    assert not any(pid_of(ds[1]) in p for p in stub.citation_paths())       # count 0: no call
    assert (lib / "_forward_citations_unique_dois.csv").exists()
    assert not fc.journal_path(lib / "_forward_citations.csv").exists()
    assert not fc.degraded_path(lib / "_forward_citations.csv").exists()
    r = next(r for r in rows if r["seed_doi"] == ds[2])
    assert r["citing_authors"] == "Ann Author" and r["citing_title"].startswith("Citer")


def test_one_session_counts_every_request(stub, tmp_path):
    ds = dois(4)
    for d in ds:
        stub.add(d, 3)
    res = fc.run(lib_dir=str(make_lib(tmp_path, ds)))
    assert res["s2"]["calls"] == len(stub.sent) == 1 + 4                    # one batch, one page per seed
    assert res["s2"]["attempts"] == 5


def test_seeds_are_resolved_by_batch_not_one_call_each(stub, tmp_path):
    ds = dois(12)
    for d in ds:
        stub.add(d, 1)
    fc.run(lib_dir=str(make_lib(tmp_path, ds)))
    batches = [r for r in stub.sent if r["path"] == "/graph/v1/paper/batch"]
    assert len(batches) == 1 and len(batches[0]["json"]["ids"]) == 12
    assert batches[0]["params"]["fields"] == "paperId,citationCount"


# ================================================================ zero citers and unresolved
def test_true_zero_citer_seed_is_not_failed(stub, tmp_path):
    ds = dois(20)
    for d in ds:
        stub.add(d, 0)
    res = fc.run(lib_dir=str(make_lib(tmp_path, ds)))
    assert res["exit_code"] == 0 and res["zero_citers"] == 20 and res["failed"] == 0
    assert res["answered"] == 20 and res["published"]
    assert res["transport_failures"] == 0 and stub.citation_paths() == []


def test_unresolved_doi_is_not_a_failure(stub, tmp_path):
    ds = dois(3)
    stub.add(ds[0], 2).add(ds[1], 2)            # ds[2]: batch answers null
    res = fc.run(lib_dir=str(make_lib(tmp_path, ds)))
    assert res["exit_code"] == 0 and res["unresolved"] == 1 and res["failed"] == 0


def test_a_failed_citations_call_is_failed_never_zero_citers(stub, tmp_path):
    ds = dois(2)
    stub.add(ds[0], 4).add(ds[1], 4).fail(ds[1], 500)
    res = fc.run(lib_dir=str(make_lib(tmp_path, ds)))
    assert res["failed_citations"] == 1 and res["zero_citers"] == 0 and res["transport_failures"] == 1
    assert res["exit_code"] == 2                                            # 1 of 2 is over 5 %


def test_a_failed_resolve_is_counted_apart_from_zero_citers(stub, tmp_path):
    ds = dois(5)
    for d in ds:
        stub.add(d, 0)
    stub.batch_status = 503
    res = fc.run(lib_dir=str(make_lib(tmp_path, ds)))
    assert res["failed_resolve"] == 5 and res["zero_citers"] == 0 and res["transport_failures"] == 5
    assert res["exit_code"] == 2 and not res["published"]


# ================================================================ exit codes 0, 2, 3
def test_exit_0_under_5_percent_failed_and_the_failed_seed_keeps_its_rows(stub, tmp_path):
    ds = dois(25)
    for d in ds:
        stub.add(d, 2)
    stub.fail(ds[7], 503)
    lib = make_lib(tmp_path, ds)
    write_prior(lib, {d: 3 for d in ds})
    res = fc.run(lib_dir=str(lib))
    assert res["exit_code"] == 0 and res["failed"] == 1 and res["transport_failures"] == 1   # 4 % <= 5 %
    rows = read_csv(lib / "_forward_citations.csv")
    kept = [r for r in rows if r["seed_doi"] == ds[7]]
    assert len(kept) == 3 and all(r["citing_paper_id"].startswith("old") for r in kept)
    assert sum(1 for r in rows if r["seed_doi"] == ds[0]) == 2             # answered: replaced


def test_exit_2_over_5_percent_failed_leaves_the_report_byte_identical(stub, tmp_path):
    ds = dois(10)
    for d in ds:
        stub.add(d, 2)
    stub.fail(ds[2], 500)                       # 1 of 10 = 10 %
    lib = make_lib(tmp_path, ds)
    prior = write_prior(lib, {d: 1 for d in ds})
    before = sha(prior)
    res = fc.run(lib_dir=str(lib))
    assert res["exit_code"] == 2 and res["status"] == "degraded"
    assert sha(prior) == before
    deg = read_csv(fc.degraded_path(prior))
    assert {r["seed_doi"] for r in deg} == set(ds)
    assert fc.journal_path(prior).exists()                                  # kept: retry the failed seed


def test_throttled_walk_429s_exhausted_aborts_keeps_report_and_writes_degraded(stub, s2env, tmp_path):
    """The K3 acceptance: 429s exhausted -> the host is refused for the run -> the breaker
    trips -> exit 3; the published CSV is byte-identical and the result goes to .degraded.csv."""
    ds = dois(8)
    for d in ds:
        stub.add(d, 3)
    stub.fail_after = (0, 429)
    lib = make_lib(tmp_path, ds)
    prior = write_prior(lib, {d: 2 for d in ds})
    before = sha(prior)
    res = fc.run(lib_dir=str(lib))
    assert res["exit_code"] == 3 and res["aborted"] == "breaker" and res["status"] == "aborted"
    assert sha(prior) == before
    assert fc.degraded_path(prior).exists()
    deg = read_csv(fc.degraded_path(prior))
    assert len(deg) == 16 and all(r["citing_paper_id"].startswith("old") for r in deg)  # prior rows kept
    assert res["transport_failures"] == 3 and res["not_walked"] == 5
    assert len(stub.citation_paths()) == 7                                  # 1 + 6 retries, then nothing sent
    assert s2env.clock.sleeps == [2.0, 4.0, 8.0, 16.0, 32.0, 64.0]
    assert fc.journal_path(prior).exists()


def test_budget_spent_is_exit_3(stub, s2env, tmp_path):
    s2env.write_config(s2={"max_requests_per_run": 3})
    ds = dois(6)
    for d in ds:
        stub.add(d, 1)
    res = fc.run(lib_dir=str(make_lib(tmp_path, ds)))
    assert res["exit_code"] == 3 and res["aborted"] == "budget"
    assert len(stub.sent) == 3 and res["s2"]["attempts"] == 3


def test_a_library_bigger_than_the_budget_is_walked_over_two_runs(stub, s2env, tmp_path):
    s2env.write_config(s2={"max_requests_per_run": 3})
    ds = dois(4)
    for d in ds:
        stub.add(d, 2)
    lib = make_lib(tmp_path, ds)
    first = fc.run(lib_dir=str(lib))
    assert first["exit_code"] == 3 and first["answered"] == 2 and not (lib / "_forward_citations.csv").exists()
    stub.sent.clear()
    second = fc.run(lib_dir=str(lib))
    assert second["exit_code"] == 0 and second["resumed"] == 2 and len(stub.sent) == 3
    assert len(read_csv(lib / "_forward_citations.csv")) == 8


def test_main_returns_the_exit_code(stub, tmp_path):
    ds = dois(2)
    stub.add(ds[0], 1).add(ds[1], 1)
    lib = make_lib(tmp_path, ds)
    assert fc.main(["--lib-dir", str(lib)]) == 0
    stub.fail_after = (0, 429)
    assert fc.main(["--lib-dir", str(lib), "--restart"]) == 2               # both failed: 100 % (no abort at 2)
    assert fc.main([]) == 1
    assert fc.main(["--lib-dir", str(tmp_path / "missing")]) == 1


# ================================================================ fewer seeds with citers
def test_fewer_seeds_with_citers_writes_degraded_and_force_publishes(stub, tmp_path):
    ds = dois(3)
    stub.add(ds[0], 2).add(ds[1], 2).add(ds[2], 0)     # ds[2] had citers; S2 now says 0
    lib = make_lib(tmp_path, ds)
    prior = write_prior(lib, {d: 1 for d in ds})
    before = sha(prior)
    res = fc.run(lib_dir=str(lib))
    assert res["exit_code"] == 2 and res["seeds_with_citers"] == 2 and res["prior_seeds_with_citers"] == 3
    assert sha(prior) == before and fc.degraded_path(prior).exists()
    assert not fc.journal_path(prior).exists()          # nothing failed: re-walk next time
    res = fc.run(lib_dir=str(lib), force=True)
    assert res["exit_code"] == 0 and res["published"] and sha(prior) != before


def test_a_seed_removed_from_the_library_is_not_a_degradation(stub, tmp_path):
    ds = dois(3)
    stub.add(ds[0], 2).add(ds[1], 2)
    lib = make_lib(tmp_path, ds[:2])
    write_prior(lib, {ds[0]: 1, ds[1]: 1, ds[2]: 4})   # ds[2]'s PDF has left the library
    res = fc.run(lib_dir=str(lib))
    assert res["exit_code"] == 0 and res["prior_seeds_with_citers"] == 2
    assert {r["seed_doi"] for r in read_csv(lib / "_forward_citations.csv")} == set(ds[:2])


def test_limit_walks_the_first_seeds_and_keeps_the_others_published_rows(stub, tmp_path):
    ds = dois(4)
    for d in ds:
        stub.add(d, 2)
    lib = make_lib(tmp_path, ds)
    write_prior(lib, {d: 1 for d in ds})
    res = fc.run(lib_dir=str(lib), limit=2)
    assert res["exit_code"] == 0 and res["seed_walks"] == 2
    rows = read_csv(lib / "_forward_citations.csv")
    by = {d: [r for r in rows if r["seed_doi"] == d] for d in ds}
    assert len(by[ds[0]]) == 2 and len(by[ds[3]]) == 1 and by[ds[3]][0]["citing_paper_id"] == "old0"


# ================================================================ resume
def test_killed_walk_resumes_from_the_last_complete_seed(stub, tmp_path):
    ds = dois(6)
    for d in ds:
        stub.add(d, 2)
    lib = make_lib(tmp_path, ds)
    out = lib / "_forward_citations.csv"
    stub.interrupt_on = pid_of(ds[3])
    with pytest.raises(KeyboardInterrupt):
        fc.run(lib_dir=str(lib))
    assert not out.exists()
    j = fc.journal_path(out)
    done = [json.loads(line)["doi"] for line in j.read_text(encoding="utf-8").splitlines()[1:]]
    assert done == ds[:3]
    with open(j, "a", encoding="utf-8") as f:
        f.write('{"doi": "10.5555/seed.0003", "state": "comp')               # a torn line from the kill
    stub.interrupt_on = None
    stub.sent.clear()
    res = fc.run(lib_dir=str(lib))
    assert res["exit_code"] == 0 and res["resumed"] == 3
    batch = [r for r in stub.sent if r["path"] == "/graph/v1/paper/batch"]
    assert batch[0]["json"]["ids"] == [f"DOI:{d}" for d in ds[3:]]          # finished seeds not re-resolved
    walked = stub.citation_paths()
    assert not any(pid_of(d) in p for d in ds[:3] for p in walked)          # nor re-walked
    assert len(walked) == 3
    assert len(read_csv(out)) == 12 and not j.exists()


def test_resume_retries_failed_seeds_only(stub, tmp_path):
    ds = dois(10)
    for d in ds:
        stub.add(d, 1)
    stub.fail(ds[4], 500, times=7)                # fails the first run, answers the second
    lib = make_lib(tmp_path, ds)
    assert fc.run(lib_dir=str(lib))["exit_code"] == 2
    stub.sent.clear()
    res = fc.run(lib_dir=str(lib))
    assert res["exit_code"] == 0 and res["resumed"] == 9
    assert [p for p in stub.citation_paths()] == [f"/graph/v1/paper/{pid_of(ds[4])}/citations"]


def test_foreign_journals_and_restart_are_not_resumed_an_old_one_is(stub, tmp_path, capsys):
    """A journal is always newer than the published report (publishing deletes it), so its age
    alone never discards it: a library bigger than one run budget is walked over several runs."""
    ds = dois(2)
    stub.add(ds[0], 1).add(ds[1], 1)
    lib = make_lib(tmp_path, ds)
    out = lib / "_forward_citations.csv"
    j = fc.journal_path(out)
    entry = {"doi": ds[0], "stage": "citations", "state": "complete", "rows": [], "n_rows": 0}

    def journal(**h):
        head = {"journal": "forward_citations", "version": 1, "lib": str(lib.resolve()),
                "started_at": "2026-10-05T10:00:00+00:00", **h}
        j.write_text(json.dumps(head) + "\n" + json.dumps(entry) + "\n", encoding="utf-8")
    for foreign in ({"lib": str(tmp_path / "other")}, {"version": 0}, {"journal": "other"}):
        journal(**foreign)
        assert fc.run(lib_dir=str(lib))["resumed"] == 0
    journal(started_at="2026-06-01T00:00:00+00:00")
    capsys.readouterr()
    assert fc.run(lib_dir=str(lib))["resumed"] == 1
    assert "started 2026-06-01T00:00:00+00:00" in capsys.readouterr().out
    journal()
    assert fc.run(lib_dir=str(lib), restart=True)["resumed"] == 0


# ================================================================ count mismatches
def test_count_mismatch_is_failed_counted_apart_and_keeps_prior_rows(stub, tmp_path):
    ds = dois(2)
    stub.add(ds[0], 5, rows=4).add(ds[1], 2)
    lib = make_lib(tmp_path, ds)
    write_prior(lib, {ds[0]: 3, ds[1]: 1})
    res = fc.run(lib_dir=str(lib))
    assert res["count_mismatch"] == 1 and res["failed_citations"] == 1 and res["transport_failures"] == 0
    assert res["recounted"] == 0                                           # count unchanged: no re-walk
    assert [r["path"] for r in stub.sent].count("/graph/v1/paper/batch") == 2  # the one recount batch
    assert res["exit_code"] == 2                                            # 1 of 2 seeds
    deg = read_csv(fc.degraded_path(lib / "_forward_citations.csv"))
    assert sum(1 for r in deg if r["seed_doi"] == ds[0]) == 3              # partial rows never used


def test_count_drift_during_the_run_is_rechecked_and_rewalked(stub, tmp_path):
    ds = dois(1)
    stub.add(ds[0], 5, rows=6)                     # one citer arrived after the batch read 5
    calls = {"n": 0}
    real = stub.__call__

    def drifting(method, url, *a, **k):
        if urlsplit(url).path == "/graph/v1/paper/batch":
            calls["n"] += 1
            stub.papers[ds[0]]["count"] = 5 if calls["n"] == 1 else 6
        return real(method, url, *a, **k)
    net._TRANSPORTS["requests"] = net._TRANSPORTS["urllib"] = drifting
    res = fc.run(lib_dir=str(make_lib(tmp_path, ds)))
    assert res["exit_code"] == 0 and res["count_mismatch"] == 0 and res["recounted"] == 1
    assert res["total_rows"] == 6


# ================================================================ flags, key and source rules
def test_sleep_is_accepted_ignored_and_spacing_comes_from_the_client(stub, s2env, tmp_path, capsys):
    ds = dois(3)
    for d in ds:
        stub.add(d, 1)
    assert fc.main(["--lib-dir", str(make_lib(tmp_path, ds)), "--sleep", "0"]) == 0
    assert "--sleep is ignored" in capsys.readouterr().out
    ts = [t for h, t in s2env.state.acquired if h == "api.semanticscholar.org"]
    assert min(b - a for a, b in zip(ts, ts[1:])) >= 6.5 - 1e-6


def test_s2_key_flag_sets_the_env_for_this_run_only_and_is_never_printed(stub, s2env, tmp_path, capsys,
                                                                          monkeypatch):
    monkeypatch.delenv(s2.KEY_ENV, raising=False)
    ds = dois(2)
    stub.add(ds[0], 1).add(ds[1], 0)
    lib = make_lib(tmp_path, ds)
    assert fc.main(["--lib-dir", str(lib), "--s2-key", SECRET]) == 0
    assert all(r["headers"].get("x-api-key") == SECRET for r in stub.sent)
    out = capsys.readouterr()
    assert "--s2-key" in out.out and SECRET not in out.out + out.err
    assert "key: present" in out.out
    assert s2.KEY_ENV not in __import__("os").environ                     # restored after the run
    assert SECRET not in s2env.ledger_text()
    for p in lib.iterdir():
        assert SECRET not in p.read_text(encoding="utf-8", errors="replace")
    ts = [t for h, t in s2env.state.acquired if h == "api.semanticscholar.org"]
    assert min(b - a for a, b in zip(ts, ts[1:])) >= 1.1 - 1e-6            # keyed spacing (virtual-clock float)


def test_project_resolution_and_config_errors(stub, tmp_path, monkeypatch):
    import lit_util
    root = tmp_path / "root"
    ds = dois(1)
    stub.add(ds[0], 1)
    make_lib(root / "teaching_x", ds)
    reg = tmp_path / "registry.json"
    reg.write_text(json.dumps({"projects": {"teaching_x": {"lib_dir": "literature"}}}), encoding="utf-8")
    monkeypatch.setattr(fc, "CONFIG_PATH", reg)
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    assert fc.run(project="teaching_x")["exit_code"] == 0
    assert (root / "teaching_x" / "literature" / "_forward_citations.csv").exists()
    assert fc.run(project="teaching_unknown")["exit_code"] == 1


def test_the_walker_binds_no_email_and_sends_nothing_outside_litpipe():
    src = (REPO / "forward_citations.py").read_text(encoding="utf-8")
    assert not hasattr(fc, "EMAIL") and not hasattr(fc, "UA") and not hasattr(fc, "s2_get")
    tree = ast.parse(src)
    imported = {a.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, (ast.Import, ast.ImportFrom))
                for a in (n.names if isinstance(n, ast.Import) else [ast.alias(n.module or "")])}
    assert not imported & {"requests", "urllib", "http", "lit_net"}


def test_no_or_empty_list_on_any_s2_list():
    """`x or []` reads a failed or missing list as an answered empty one (the 09-16 defect class)."""
    tree = ast.parse((REPO / "forward_citations.py").read_text(encoding="utf-8"))
    bad = [n.lineno for n in ast.walk(tree) if isinstance(n, ast.BoolOp) and isinstance(n.op, ast.Or)
           and isinstance(n.values[-1], (ast.List, ast.Dict)) and not getattr(n.values[-1], "elts", None)
           and not getattr(n.values[-1], "keys", None)]
    assert bad == []
    assert "cites.get(\"data\", [])" not in (REPO / "forward_citations.py").read_text(encoding="utf-8")


def test_summary_marker_is_the_last_line_and_parses(stub, tmp_path, capsys):
    ds = dois(2)
    stub.add(ds[0], 1).add(ds[1], 1).fail(ds[1], 503)
    fc.run(lib_dir=str(make_lib(tmp_path, ds)))
    last = capsys.readouterr().out.strip().splitlines()[-1]
    assert last.startswith(fc.SUMMARY_MARKER)
    summ = json.loads(last[len(fc.SUMMARY_MARKER):])
    assert summ["exit_code"] == 2 and summ["transport_failures"] == 1 and "key" not in json.dumps(summ["s2"])


# ================================================================ the 09-16 replay, end to end
def test_replay_0916_throttled_second_walk_is_degraded_and_publishes_nothing(stub, tmp_path):
    """09-16: 237 seeds, the published report had 222 seeds with citers; the second walk was
    throttled part-way and the old walker wrote 141. Now the first exhausted 429 refuses the host,
    the breaker trips, nothing is published and the report keeps its 222 seeds."""
    ds = dois(237)
    for i, d in enumerate(ds):
        stub.add(d, 0 if i >= 222 else 3)
    lib = make_lib(tmp_path, ds)
    prior = write_prior(lib, {d: 2 for d in ds[:222]})
    before = sha(prior)
    stub.fail_after = (141, 429)
    res = fc.run(lib_dir=str(lib))
    assert res["exit_code"] == 3 and sha(prior) == before
    assert res["seeds_with_citers"] == 222                                  # the degraded file loses nothing
    assert len({r["seed_doi"] for r in read_csv(fc.degraded_path(prior))}) == 222
    import snowball
    step = snowball.StepResult("forward_citations", [], res["exit_code"], res)
    assert snowball.classify(36767, 36767, [step])[0] == "DEGRADED"
