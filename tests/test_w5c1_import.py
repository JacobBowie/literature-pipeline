"""W5-C1 item 1 (decision A3): the DOI-keyed CSV import into the worklists,
`migrate_closed_to_md.py --import-csv PATH [--project KEY ...] [--project-map TAIL=KEY ...]`.
A synthetic CSV with bare project names, the long `review (...)` value and an email in last_error."""
import contextlib
import csv
import datetime
import io
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import lit_util
import migrate_closed_to_md as mig
from litpipe import lockfile

TODAY = datetime.date(2026, 10, 8)
EMAIL = "someone.private@example.edu"
REVIEW = "review (never reached Unpaywall; settled or partial run)"
ERR = (f"HTTPSConnectionPool(host='api.unpaywall.org', port=443): Max retries exceeded with url: "
       f"/v2/10.1/x?email={EMAIL}")


class FakeHoldMap:
    def __init__(self, held):
        self.held = {d.lower(): pdf for d, pdf in held.items()}

    def content(self, doi):
        pdf = self.held.get(str(doi).lower())
        return [] if pdf is None else [SimpleNamespace(has_pdf=pdf, path=Path("x"))]


class World:
    def __init__(self, tmp, monkeypatch):
        self.tmp = Path(tmp)
        self.root = self.tmp / "Projects"
        self.root.mkdir()
        monkeypatch.setattr(lit_util, "PROJECTS_ROOT", self.root)
        self.cfg = {"state_dir": str(self.tmp / "state"), "projects": {
            "Courses": {"lib_dir": "lib"},
            "Courses/teaching_a": {"parent": "Courses", "lib_dir": "teaching_a/literature"},
            "teaching_b": {"lib_dir": "literature"},
            "GroupA/shared": {"parent": "GroupA", "lib_dir": "shared/lit"},
            "GroupB/shared": {"parent": "GroupB", "lib_dir": "shared/lit"}}}
        self.cfg_path = self.tmp / "projects.json"
        self.cfg_path.write_text(json.dumps(self.cfg), encoding="utf-8")
        monkeypatch.setattr(mig, "CONFIG_PATH", self.cfg_path)
        for k in ("Courses/teaching_a", "teaching_b"):
            self.proot(k).mkdir(parents=True, exist_ok=True)

    def proot(self, key):
        return lit_util.project_root(key, self.cfg["projects"][key])

    def csv(self, rows, name="swept.csv", fields=("project", "doi", "pool", "first_swept", "last_oa_status",
                                                  "last_error", "suggested_worklist", "title")):
        p = self.tmp / name
        with open(p, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(fields), extrasaction="ignore", lineterminator="\n")
            w.writeheader()
            w.writerows(rows)
        return p

    def snapshot(self):
        return {str(p.relative_to(self.tmp)): p.read_bytes() for p in self.tmp.rglob("*") if p.is_file()}


def row(project, doi, sw, **kw):
    return {"project": project, "doi": doi, "suggested_worklist": sw, "first_swept": "2026-09-20",
            "last_oa_status": {"ill": "CLOSED", "oa-blocked": "OA"}.get(sw, "(no unpaywall row)"),
            "last_error": ERR, **kw}


ROWS = [
    row("teaching_a", "10.1000/ill.1", "ill", title=f"Closed one, write to {EMAIL}"),
    row("teaching_a", "10.1000/ill.2", "ill"),
    row("teaching_a", "10.1000/ill.2", "ill"),                                   # repeated in the CSV
    row("teaching_a", "10.1000/listed.1", "ill"),                                 # already on the ILL list
    row("teaching_a", "10.1000/held.1", "ill"),                                   # held now
    row("teaching_a", "10.1000/oa.1", "oa-blocked"),
    row("teaching_a", "10.1000/rev.1", REVIEW),
    row("teaching_a", "10.1000/rev.2", REVIEW),
    row("teaching_b", "10.1000/b.1", "oa-blocked"),
    row("Courses/teaching_a", "10.1000/full.1", REVIEW),                          # a full registry key
    row("Unknown_Course", "10.1000/u.1", "ill"),                                  # unregistered
    row("shared", "10.1000/amb.1", "ill"),                                        # two registered tails
    row("teaching_a", "NO_DOI_7", "ill"),                                         # malformed
    row("teaching_a", "10.1000/bad.1", "maybe later"),                            # malformed
    row("", "10.1000/blank.1", "ill"),                                            # malformed
]


@pytest.fixture
def w(tmp_path, monkeypatch):
    return World(tmp_path, monkeypatch)


def run_import(w, path, **kw):
    kw.setdefault("cfg", w.cfg)
    kw.setdefault("holdmap", FakeHoldMap({"10.1000/held.1": True}))
    kw.setdefault("today", TODAY)
    return mig.import_csv(path, **kw)


def seed_listed(w):
    ill = w.proot("Courses/teaching_a") / mig.ILL_NAME
    ill.write_text("# Manual Pull Queue\n\n- [ ] **Old** — DOI `10.1000/LISTED.1` — earlier\n", encoding="utf-8")


# ================================================================ resolution
def test_project_values_resolve_by_key_then_by_one_tail():
    keys = ["Courses", "Courses/teaching_a", "teaching_b", "GroupA/shared", "GroupB/shared"]
    assert mig.resolve_import_project("teaching_a", keys) == ("Courses/teaching_a", "")
    assert mig.resolve_import_project("Courses/teaching_a", keys) == ("Courses/teaching_a", "")
    assert mig.resolve_import_project("teaching_b", keys) == ("teaching_b", "")
    assert mig.resolve_import_project("shared", keys) == (None, "ambiguous")
    assert mig.resolve_import_project("nowhere", keys) == (None, "unregistered")
    assert mig.resolve_import_project("  ", keys) == (None, "blank")
    assert mig.resolve_import_project("shared", keys, {"shared": "GroupB/shared"}) == ("GroupB/shared", "")
    assert mig.resolve_import_project("teaching_a", keys, {"teaching_a": "teaching_b"}) == ("teaching_b", "")


@pytest.mark.parametrize("value,target", [("ill", "ill"), ("oa-blocked", "oa-blocked"), (REVIEW, "review"),
                                          ("Review", "review"), ("ILL (closed)", "ill"), ("maybe", None),
                                          ("", None)])
def test_suggested_worklist_is_read_by_its_first_word(value, target):
    assert mig._import_target(value) == target


# ================================================================ the dry run
def test_the_dry_run_counts_every_target_and_skip_and_writes_nothing(w):
    seed_listed(w)
    src = w.csv(ROWS)
    before = w.snapshot()
    res = run_import(w, src)
    assert w.snapshot() == before                                    # the input and every list untouched
    a = res["projects"]["Courses/teaching_a"]
    assert a["ill"] == {"write": 2, "held": 1, "held_text_only": 0, "listed": 2}   # ill.2 repeated + listed.1
    assert a["oa-blocked"]["write"] == 1 and a["review"]["write"] == 3   # rev.1, rev.2 and the full key's row
    assert res["projects"]["teaching_b"]["oa-blocked"]["write"] == 1
    assert res["unregistered"] == {"Unknown_Course": 1, "shared (ambiguous)": 1}
    assert len(res["malformed"]) == 3 and res["rows"] == len(ROWS)
    assert res["by_target"] == {"ill": 7, "oa-blocked": 2, "review": 3}


def test_the_report_never_prints_last_error_or_an_address(w, capsys):
    src = w.csv(ROWS)
    res = run_import(w, src)
    mig.print_import(res)
    out = capsys.readouterr().out
    assert EMAIL not in out and "HTTPSConnectionPool" not in out and "would write" in out
    assert "Courses/teaching_a" in out and "unregistered: 2 row(s)" in out and "DRY RUN" in out


# ================================================================ the commit
def test_commit_writes_each_target_in_its_lists_format_and_never_the_review_list(w):
    seed_listed(w)
    src = w.csv(ROWS)
    src_bytes = src.read_bytes()
    res = run_import(w, src, commit=True)
    root = w.proot("Courses/teaching_a")
    ill = (root / mig.ILL_NAME).read_text(encoding="utf-8")
    assert "DOI `10.1000/ill.1` — imported from swept.csv; last oa_status CLOSED" in ill
    assert ill.count("10.1000/ill.2") == 1 and "10.1000/held.1" not in ill
    assert mig.existing_dois(ill) >= {"10.1000/ill.1", "10.1000/ill.2", "10.1000/listed.1"}
    oa = (root / mig.OA_BLOCKED_NAME).read_text(encoding="utf-8")
    assert "[10.1000/oa.1](https://doi.org/10.1000/oa.1) cause `imported from swept.csv` via `import:OA`" in oa
    _, retry = mig.read_retry_later(root)
    by = {r["doi"]: r for r in retry}
    assert set(by) == {"10.1000/rev.1", "10.1000/rev.2", "10.1000/full.1"}
    assert all(r["not_before"] == TODAY.isoformat() and r["attempts"] == "0" for r in retry)
    assert by["10.1000/rev.1"]["destination"] == "literature" and by["10.1000/rev.1"]["first_seen"] == "2026-09-20"
    assert not (root / mig.REVIEW_NAME).exists()                       # the identity-flag list: never
    everything = "".join(p.read_text(encoding="utf-8") for p in root.iterdir() if p.is_file())
    assert EMAIL not in everything and "HTTPSConnectionPool" not in everything
    assert src.read_bytes() == src_bytes                               # the input is only read
    assert lockfile.read(root) is None                                 # the lock was taken and released
    assert res["written"] == {f"Courses/teaching_a/{mig.ILL_NAME}": 2,
                              f"Courses/teaching_a/{mig.OA_BLOCKED_NAME}": 1,
                              f"Courses/teaching_a/{mig.RETRY_LATER_NAME}": 3,
                              f"teaching_b/{mig.OA_BLOCKED_NAME}": 1}


def test_a_second_commit_writes_nothing(w):
    src = w.csv(ROWS)
    run_import(w, src, commit=True)
    before = w.snapshot()
    res = run_import(w, src, commit=True)
    assert w.snapshot() == before and res["written"] == {}
    a = res["projects"]["Courses/teaching_a"]
    assert a["ill"]["write"] == a["oa-blocked"]["write"] == a["review"]["write"] == 0
    assert a["review"]["listed"] == 3


def test_a_review_row_admitted_by_the_next_sweep_still_counts_as_listed(w):
    import sweep
    src = w.csv([row("teaching_a", "10.1000/rev.9", REVIEW)])
    run_import(w, src, commit=True)
    root = w.proot("Courses/teaching_a")
    assert sweep.admit_retries(root, TODAY.isoformat())["admitted"] == 1   # due at once
    assert run_import(w, src, commit=True)["written"] == {}


def test_review_as_list_only_writes_no_review_row(w):
    src = w.csv(ROWS)
    res = run_import(w, src, commit=True, review_as="list-only")
    root = w.proot("Courses/teaching_a")
    assert not (root / mig.RETRY_LATER_NAME).exists()
    assert res["projects"]["Courses/teaching_a"]["review"]["list_only"] == 3


def test_a_held_lock_skips_that_project_and_the_cli_exits_4(w, capsys):
    root = w.proot("Courses/teaching_a")
    (root / lockfile.LOCK_NAME).write_text(json.dumps({
        "host": "laptop.example", "pid": 1, "run_id": "other", "tool": "sweep",
        "started": lockfile._iso(lockfile._time()), "heartbeat": lockfile._iso(lockfile._time())}), encoding="utf-8")
    src = w.csv(ROWS)
    w.cfg_path.write_text(json.dumps(w.cfg), encoding="utf-8")
    code = mig.main(["--import-csv", str(src), "--commit", "--no-holdings"])
    assert code == mig.EXIT_IMPORT_LOCKED
    assert not (root / mig.ILL_NAME).exists() and not (root / mig.RETRY_LATER_NAME).exists()
    assert (w.proot("teaching_b") / mig.OA_BLOCKED_NAME).exists()      # the other project is written
    assert "laptop.example" in capsys.readouterr().out


def test_project_filter_and_map(w):
    src = w.csv(ROWS)
    res = run_import(w, src, projects=["teaching_b"])
    assert list(res["projects"]) == ["teaching_b"] and res["not_selected"] == 9
    res = run_import(w, src, project_map={"teaching_a": "teaching_b", "shared": "GroupA/shared"})
    assert res["projects"]["teaching_b"]["review"]["write"] == 2 and res["unregistered"] == {"Unknown_Course": 1}


@pytest.mark.parametrize("argv,needle", [
    (["--project-map", "teaching_a"], "TAIL=KEY"),
    (["--project-map", "teaching_a=Nope"], "unregistered"),
    (["--project", "Nope"], "not in projects.json"),
    (["--review-as", "list-only", "--project", "teaching_b"], None),
])
def test_cli_usage(w, capsys, argv, needle):
    src = w.csv(ROWS)
    with contextlib.redirect_stdout(io.StringIO()):
        code = mig.main(["--import-csv", str(src), "--no-holdings", *argv])
    err = capsys.readouterr().err
    if needle is None:
        assert code == 0
    else:
        assert code == 2 and needle in err


def test_a_csv_without_the_required_columns_is_a_usage_error(w, capsys):
    src = w.csv([{"doi": "10.1/x"}], fields=("doi", "title"))
    assert mig.main(["--import-csv", str(src), "--no-holdings"]) == 2
    assert "lacks the column" in capsys.readouterr().err


def test_routing_still_takes_exactly_one_project(w, capsys):
    assert mig.main([]) == 2
    assert mig.main(["--project", "teaching_b", "--project", "Courses/teaching_a"]) == 2
