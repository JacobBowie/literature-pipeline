"""W5 verifier R: the pool-selection gate (litpipe.gate) beyond its recorded-run parity.

The schema as a new user meets it (every loader key documented, malformed specs named), the two
supported plan modes with both orders on small harvests whose selections are worked out by hand in
the test, the trim (half-to-even at an exact .5, the undershoot top-up, the protected abort),
determinism (another working directory, other PYTHONHASHSEED values, rows shuffled inside rank-tie
groups), live holdings over a temp registry, the as-of snapshot, the drawn set, invariant 12 end to
end, promote and `runner batch` through tests/fixtures/W4-A (temp registry, stubbed transports), the
zero-input and sanity refusals (exit 2, nothing written), report arithmetic and portability.
Tests that lock defects this review found (R-1 to R-4) were strict xfails at `7002ee9`; the fixes
landed on 2026-10-08 and they are plain tests now."""
import copy
import csv
import io
import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

import lit_util
from litpipe import config, gate, holdings, runner, worklists

REPO = Path(__file__).resolve().parent.parent
W4A = Path(__file__).resolve().parent / "fixtures" / "W4-A"
if str(W4A) not in sys.path:
    sys.path.insert(0, str(W4A))

from w4a_world import World, stand_in_overrides  # noqa: E402

K = "teaching_verify"
HDR = ["seed_doi", "seed_chapter", "citing_doi", "citing_title", "citing_year", "citing_authors", "citing_venue",
       "citing_cited_by"]
TOPICS = [
    {"name": "alpha", "substrings": ["alpha"], "controls": {"hit": ["alpha study"], "miss": ["beta study"]}},
    {"name": "beta", "substrings": ["beta"]},
    {"name": "gamma", "substrings": ["gamma"]},
]
B1_DECLARED = [{"topic": "alpha", "cap": 2}, {"topic": "beta", "cap": "take_all"}, {"topic": "gamma", "cap": 1}]


def D(s):
    return "10.5555/" + s


def rec(doi, title, *, n=1, cb=1, year="2020", chapters=None):
    """The harvest rows of one record: one row per seed (n distinct seeds), contiguous."""
    ch = list(chapters or [""])
    return [{"seed_doi": f"10.9/seed{i}", "seed_chapter": ch[(i - 1) % len(ch)], "citing_doi": doi,
             "citing_title": title, "citing_year": str(year), "citing_authors": "Author A",
             "citing_venue": "Venue V", "citing_cited_by": str(cb)} for i in range(1, n + 1)]


def harvest(*recs):
    return [r for rs in recs for r in rs]


def spec(**over):
    d = {"schema": gate.SCHEMA, "name": "verify", "input": {"harvest": "_verify_h.csv"},
         "topics": copy.deepcopy(TOPICS), "controls": {"match_none": ["an unrelated title"]},
         "holdings": {"filter": False},
         "quotas": {"target": 6, "declared": copy.deepcopy(B1_DECLARED)}}
    d.update(copy.deepcopy(over))
    return d


def keys(recs):
    return [r.key for r in recs]


def write_harvest(path, rows):
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=HDR, lineterminator="\n")
    w.writeheader()
    w.writerows(rows)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_bytes(buf.getvalue().encode("utf-8"))


def mklib(tmp, rows, name="_verify_h.csv"):
    lib = Path(tmp) / "lib"
    lib.mkdir(parents=True, exist_ok=True)
    write_harvest(lib / name, rows)
    return lib


def files_under(p):
    p = Path(p)
    return sorted((str(x.relative_to(p)), x.stat().st_size) for x in p.rglob("*")) if p.exists() else []


def b1_rows():
    """rarest_match + declared, worked by hand. Rank (n_seeds, cited_by desc): ab, a1, b1, a2, a3, b2, g1.
    Tags over the pool: alpha 4 (a1 a2 a3 ab), beta 3 (b1 b2 ab), gamma 1, so ab goes to beta."""
    return harvest(rec(D("a1"), "alpha one", cb=90), rec(D("a2"), "alpha two", cb=80),
                   rec(D("a3"), "alpha three", cb=70), rec(D("b1"), "beta one", cb=85),
                   rec(D("b2"), "beta two", cb=60), rec(D("g1"), "gamma one", cb=50),
                   rec(D("ab"), "alpha beta both", n=2, cb=10), rec(D("off"), "an unrelated title", cb=99))


# ================================================================ 1. the schema as a new user meets it
FULL = {
    "schema": gate.SCHEMA, "name": "full-spec", "description": "every documented field", "provenance": {"a": 1},
    "recorded_run": {"b": 2},
    "input": {"harvest": {"scope": "full"}, "seeds_manifest": "_full_forward_seeds.json",
              "columns": {"doi": "citing_doi", "title": "citing_title", "year": "citing_year",
                          "authors": "citing_authors", "venue": "citing_venue", "cited_by": "citing_cited_by",
                          "seed": ["seed_label", "seed_doi"], "seed_chapter": None},
              "doi_key": "lower_unresolve_rstrip"},
    "collapse": {"record_row": "first", "fill_empty": ["title", "authors", "venue", "year"], "cited_by": "record_row",
                 "text": ["html_unescape"]},
    "years": {"min": 2000, "max": 2030, "unparsed": "keep", "scope": "row"},
    "topics": [{"name": "alpha", "chapter": "U1", "substrings": ["alpha"], "phrases": ["alpha beta"],
                "regex": [r"\balpha\b"], "provenance": {"x": 1},
                "controls": {"hit": ["alpha study", {"title": "alpha x", "source": "s"}], "miss": ["beta study"]}},
               {"name": "beta", "chapter": None, "substrings": ["beta"]}],
    "require": [{"name": "study", "substrings": ["study"], "controls": {"hit": ["a study"], "miss": ["an essay"]}}],
    "exclude": [{"name": "retracted", "phrases": ["retracted"], "controls": {"hit": ["retracted study"]}}],
    "controls": {"match_none": ["an unrelated study"], "exclude_none": ["alpha study"], "admit": ["alpha study"],
                 "contentless": True, "minimum": "gate", "require_offtopic_drops": True,
                 "require_nonempty_pool": True},
    "holdings": {"filter": True, "scope": "project"},
    "rank": [{"field": "year", "order": "asc"}, {"field": "n_seeds"}],
    "quotas": {"target": 3, "tier_take_all": {"min_seeds": 3}, "assign": "fill_order_claim", "caps": "equal_share"},
    "chapter_budgets": {"U1": 2},
    "order": {"method": "round_robin", "bucket_order": "size_desc"},
    "lanes": [{"name": "older", "topics": ["alpha"], "size": 1, "years": {"min": 1990, "max": 1999},
               "rank": [{"field": "cited_by", "order": "desc"}]}],
    "annotate": [{"column": "in_list", "dois_from": "list.csv", "doi_column": "doi", "comment_prefix": "#"}],
    "reports": {"direction_split": [{"topic": "alpha", "sides": [{"name": "up", "words": ["rise"]},
                                                                 {"name": "down", "words": ["fall"]}],
                                     "match": "substring"}]},
    "output": {"notes_template": "gate={gate}; t={assigned_topic}; ts={topics}; n={n_seeds}; cb={cited_by}; "
                                 "y={year}; tc={topic_chapter}; r={pool_rank}; o={order}; l={lane}; ch={chapters}"},
}


def full_declared():
    d = copy.deepcopy(FULL)
    d["quotas"] = {"target": 3, "assign": "rarest_match", "caps": "declared",
                   "declared": [{"topic": "alpha", "cap": "take_all"}], "default_cap": 1, "allocation_order": "cap_desc"}
    d["order"] = {"method": "rank"}
    return d


def test_every_key_the_loader_accepts_is_documented_and_a_spec_using_every_documented_field_loads(monkeypatch):
    seen = {}
    real = gate._obj

    def spy(v, where, allowed, required=()):
        if where != "chapter_budgets":          # its keys are the user's chapter names
            seen.setdefault(re.sub(r"\[\d+\]", "[]", where), set()).update(allowed)
        return real(v, where, allowed, required)
    monkeypatch.setattr(gate, "_obj", spy)
    a, b = gate.load_spec(copy.deepcopy(FULL)), gate.load_spec(full_declared())
    assert a.eff["quotas"]["tier_take_all"] == {"min_seeds": 3} and b.eff["quotas"]["default_cap"] == 1
    assert {"spec", "input", "input.columns", "collapse", "years", "topics[]", "topics[].controls", "require[]",
            "exclude[]", "controls", "holdings", "rank[]", "quotas", "order", "lanes[]", "annotate[]", "reports",
            "reports.direction_split[]", "reports.direction_split[].sides[]", "output"} <= set(seen)
    doc = gate.__doc__
    undocumented = sorted({k for ks in seen.values() for k in ks
                           if not re.search(rf"(?<![A-Za-z_]){re.escape(k)}(?![A-Za-z_])", doc)})
    assert undocumented == []


@pytest.mark.parametrize("mutate, msg", [
    (lambda d: d["topics"][1].pop("substrings"), "topics[1] (beta) has no pattern"),
    (lambda d: d.update(topics=[]), "topics needs at least 1 entry"),
    (lambda d: d["quotas"].update(target=0), "quotas.target must be >= 1, got 0"),
    (lambda d: d["quotas"].update(target=-3), "quotas.target must be >= 1, got -3"),
    (lambda d: d.update(lanes=[{"name": "older", "topics": ["delta"], "size": 2}]),
     "lanes[0].topics[0] 'delta' is not a topic"),
    (lambda d: d["topics"][0].update(regex=["alpha("]), "topics[0] (alpha): pattern 'alpha(' does not compile"),
    (lambda d: d["topics"][0].update(regex=["x*"]), "pattern 'x*' matches the empty title"),
    (lambda d: d.update(require=[{"name": "everything", "regex": ["^"]}]), "matches the empty title"),
    (lambda d: d["quotas"]["declared"].append({"topic": "delta", "cap": 1}), "quotas.declared[3].topic 'delta'"),
    (lambda d: d["quotas"]["declared"][0].update(cap=-1), "quotas.declared[0].cap must be >= 0"),
    (lambda d: d["quotas"].update(declared=[{"topic": "alpha", "cap": 2}]), "does not cap beta, gamma"),
    (lambda d: d.update(lanes=[{"name": "main", "topics": ["alpha"], "size": 1}]), "reserved"),
    (lambda d: d.update(lanes=[{"name": "x", "topics": ["alpha"], "size": 0}]), "lanes[0].size must be >= 1"),
    (lambda d: d["quotas"].update(target=2.5), "quotas.target must be an integer"),
])
def test_malformed_specs_a_user_would_write_are_refused_naming_field_and_problem(mutate, msg):
    d = spec()
    mutate(d)
    with pytest.raises(gate.SpecError) as ei:
        gate.load_spec(d)
    assert msg in str(ei.value)


def test_a_cap_larger_than_the_target_is_accepted_and_trimmed_half_to_even():
    # 5 alpha + 5 beta allocated for a target of 3: over 7, each share round(3.5) = 4 (even), keep 1 + 1,
    # then the undershoot top-up restores alpha's next row (tie on loss: first in allocation order)
    sp = spec(quotas={"target": 3, "declared": [{"topic": "alpha", "cap": 10}, {"topic": "beta", "cap": 10},
                                                {"topic": "gamma", "cap": 10}]})
    res = gate.gate(gate.load_spec(sp), pair_rows())
    assert keys(res.plan.order) == [D("b1"), D("a1"), D("a2")]
    assert res.plan.trim["buckets"] == {"alpha": {"before": 5, "after": 2}, "beta": {"before": 5, "after": 1}}
    assert res.plan.trim["restored"] == 1


def test_a_require_facet_that_matches_everything_is_refused_by_the_controls_and_by_the_sanity_check():
    d = spec(require=[{"name": "anything", "regex": ["."]}])
    with pytest.raises(gate.GateAbort) as ei:
        gate.gate(gate.load_spec(d), b1_rows())
    assert "contentless control passes inclusion" in str(ei.value)
    d["controls"]["contentless"] = False
    with pytest.raises(gate.GateAbort) as ei:
        gate.gate(gate.load_spec(d), b1_rows())
    assert "no record failed inclusion" in str(ei.value)


def test_minimum_none_warning_reaches_stdout_report_md_and_report_json(tmp_path, capsys):
    lib = mklib(tmp_path, b1_rows())
    d = spec(controls={"minimum": "none"})
    for t in d["topics"]:
        t.pop("controls", None)
    sp = tmp_path / "spec.json"
    sp.write_text(json.dumps(d), encoding="utf-8")
    out = tmp_path / "out"
    code = gate.main(["plan", "--project", K, "--spec", str(sp), "--lib-dir", str(lib), "--write",
                      "--out-root", str(out)])
    assert code == 0
    assert "WARNING: controls.minimum is 'none'" in capsys.readouterr().out
    (rd,) = list((out / "_gate" / "verify").iterdir())
    assert "controls.minimum is 'none'" in (rd / "report.md").read_text(encoding="utf-8")
    assert any("controls.minimum is 'none'" in w
               for w in json.loads((rd / "report.json").read_text(encoding="utf-8"))["warnings"])


# ---------------------------------------------------------------- R-1, R-2: loader defects (locks)
@pytest.mark.parametrize("tpl", ["{chapters[0]}", "{gate.upper}", "{n_seeds[0]}", "{topics[0]}"])
def test_notes_template_takes_plain_field_names_only(tpl):
    with pytest.raises(gate.SpecError, match="plain field names"):
        gate.load_spec(spec(output={"notes_template": tpl}))


def test_notes_template_with_format_specs_still_loads_and_fills_the_notes():
    res = gate.gate(gate.load_spec(spec(output={"notes_template": "o={order:03d}; t={assigned_topic}"})), b1_rows())
    assert [r["notes"] for r in gate.output_rows(res)["selection"]][:2] == ["o=001; t=gamma", "o=002; t=alpha"]


@pytest.mark.parametrize("value", [5, True, 1.5])
def test_chapter_budgets_must_be_an_object_and_the_error_names_it(value):
    with pytest.raises(gate.SpecError, match="chapter_budgets must be an object"):
        gate.load_spec(spec(chapter_budgets=value))


# ================================================================ 2. the supported combinations by hand
@pytest.mark.parametrize("order, want", [
    ({"method": "round_robin", "bucket_order": "size_asc"}, ["g1", "a1", "ab", "a2", "b1", "b2"]),
    ({"method": "round_robin", "bucket_order": "size_desc"}, ["ab", "a1", "g1", "b1", "a2", "b2"]),
    ({"method": "rank"}, ["ab", "a1", "b1", "a2", "b2", "g1"]),
])
def test_rarest_match_declared_with_take_all_by_hand(order, want):
    res = gate.gate(gate.load_spec(spec(order=order)), b1_rows())
    assert keys(res.candidates) == [D(x) for x in ("ab", "a1", "b1", "a2", "a3", "b2", "g1")]
    assert keys(res.plan.order) == [D(x) for x in want]
    assert res.plan.allocation == ["beta", "alpha", "gamma"]          # bucket_size_desc, tie by first appearance
    assert res.plan.assigned == {D("ab"): "beta", D("b1"): "beta", D("b2"): "beta", D("a1"): "alpha",
                                 D("a2"): "alpha", D("g1"): "gamma"}
    assert res.plan.reasons == {D("a3"): "over_cap:alpha"}
    assert res.records[D("off")].reasons == ["no_topic"]


def test_rarest_match_trim_never_cuts_the_take_all_bucket_by_hand():
    # target 5: allocated 6 (beta take_all 3, alpha 2, gamma 1); capped alpha+gamma = 3 absorb the overflow 1:
    # alpha share round(2/3) = 1, gamma round(1/3) = 0
    d = spec()
    d["quotas"]["target"] = 5
    res = gate.gate(gate.load_spec(d), b1_rows())
    assert keys(res.plan.order) == [D("a1"), D("g1"), D("ab"), D("b1"), D("b2")]
    assert res.plan.reasons == {D("a3"): "over_cap:alpha", D("a2"): "trimmed:alpha"}
    assert res.plan.trim == {"overflow": 1, "allocated": 6, "dropped_largest": 0, "restored": 0,
                             "buckets": {"alpha": {"before": 2, "after": 1}, "gamma": {"before": 1, "after": 1}}}


def claim_rows():
    """fill_order_claim + equal_share + tier, worked by hand. Tier (n_seeds >= 2): t2, t1. P in rank order:
    p1..p7. avail: alpha p1 p2 p5 (3), beta p2 p3 p6 (3), gamma p4 (1). Fill order gamma, alpha, beta
    (scarcest first, ties in spec order); room 4: caps gamma 1, alpha min(3, 3//2) = 1, beta 2. Claims:
    gamma p4; alpha p1; beta p2 (not claimed by alpha), p3."""
    return harvest(rec(D("t1"), "alpha beta study tier", n=2, cb=5, chapters=["U2", "U1"]),
                   rec(D("t2"), "gamma study tier", n=3, cb=1, chapters=["U3", "U1", "U2"]),
                   rec(D("p1"), "alpha study one", cb=90), rec(D("p2"), "alpha beta study two", cb=80),
                   rec(D("p3"), "beta study three", cb=70), rec(D("p4"), "gamma study four", cb=60),
                   rec(D("p5"), "alpha study five", cb=50), rec(D("p6"), "beta study six", cb=40),
                   rec(D("p7"), "a study of nothing", cb=30), rec(D("off"), "an unrelated title", cb=99))


def claim_spec(order, **over):
    return spec(require=[{"name": "study", "substrings": ["study"]}],
                quotas={"target": 6, "tier_take_all": {"min_seeds": 2}, "assign": "fill_order_claim",
                        "caps": "equal_share"}, order=order, **over)


@pytest.mark.parametrize("order, want", [
    ({"method": "rank"}, ["t2", "t1", "p1", "p2", "p3", "p4"]),
    ({"method": "round_robin", "bucket_order": "size_desc"}, ["t2", "p2", "p4", "p1", "t1", "p3"]),
    ({"method": "round_robin", "bucket_order": "size_asc"}, ["p4", "p1", "t2", "p2", "t1", "p3"]),
])
def test_fill_order_claim_equal_share_with_the_seed_tier_by_hand(order, want):
    res = gate.gate(gate.load_spec(claim_spec(order)), claim_rows())
    assert keys(res.plan.order) == [D(x) for x in want]
    assert res.plan.allocation == ["_tier", "gamma", "alpha", "beta"]
    assert res.plan.caps == {"gamma": 1, "alpha": 1, "beta": 2}
    assert res.plan.assigned == {D("t2"): "_tier", D("t1"): "_tier", D("p4"): "gamma", D("p1"): "alpha",
                                 D("p2"): "beta", D("p3"): "beta"}
    assert res.plan.reasons == {D("p5"): "quota_full:alpha", D("p6"): "quota_full:beta", D("p7"): "no_quota_topic"}
    assert res.records[D("off")].reasons == ["missing:study"]      # with require, inclusion is the facets


def pair_rows():
    """Five alpha and five beta records, alpha first in rank order (a1 198, b1 197, a2 196, ...)."""
    out = []
    for i in range(1, 6):
        out += rec(D(f"a{i}"), f"alpha {i}", cb=200 - 2 * i) + rec(D(f"b{i}"), f"beta {i}", cb=199 - 2 * i)
    return out + rec(D("off"), "an unrelated title", cb=1)


def pair_spec(target):
    return spec(quotas={"target": target, "declared": [{"topic": "alpha", "cap": 5}, {"topic": "beta", "cap": 5},
                                                       {"topic": "gamma", "cap": 0}]})


def test_trim_half_to_even_at_an_exact_half_by_hand():
    # over 5 of 10, each share round(5/2) = 2 (half-to-even; half-up would give 3 and then top up alpha):
    # keep 3 + 3 = 6 > 5, so the largest bucket drops one (tie: first in allocation order, alpha)
    res = gate.gate(gate.load_spec(pair_spec(5)), pair_rows())
    assert keys(res.plan.order) == [D("a1"), D("b1"), D("a2"), D("b2"), D("b3")]
    assert res.plan.trim["buckets"] == {"alpha": {"before": 5, "after": 2}, "beta": {"before": 5, "after": 3}}
    assert (res.plan.trim["dropped_largest"], res.plan.trim["restored"]) == (1, 0)
    assert {k: v for k, v in res.plan.reasons.items()} == {D("a3"): "trimmed:alpha", D("a4"): "trimmed:alpha",
                                                           D("a5"): "trimmed:alpha", D("b4"): "trimmed:beta",
                                                           D("b5"): "trimmed:beta"}


def test_trim_undershoot_top_up_by_hand():
    # target 7: over 3, each share round(3/2) = 2, keep 3 + 3 = 6 < 7, restore alpha's a4 (both lost 2;
    # tie: first in allocation order). Floor shares (1 + 1) would keep 4 + 4 and drop alpha's a4 instead.
    res = gate.gate(gate.load_spec(pair_spec(7)), pair_rows())
    assert keys(res.plan.order) == [D(x) for x in ("b1", "a1", "b2", "a2", "b3", "a3", "a4")]
    assert res.plan.trim["buckets"] == {"alpha": {"before": 5, "after": 4}, "beta": {"before": 5, "after": 3}}
    assert (res.plan.trim["dropped_largest"], res.plan.trim["restored"]) == (0, 1)
    assert res.plan.table["alpha"]["trimmed"] == 1 and res.plan.table["beta"]["trimmed"] == 2


def test_protected_trim_aborts_when_take_all_rows_alone_exceed_what_the_capped_rows_can_absorb(tmp_path):
    d = spec(quotas={"target": 4, "declared": [{"topic": "alpha", "cap": "take_all"}, {"topic": "beta", "cap": 2},
                                               {"topic": "gamma", "cap": 0}]})
    with pytest.raises(gate.GateAbort) as ei:
        gate.gate(gate.load_spec(d), pair_rows())
    assert ei.value.reasons == ["trim:impossible"] and "impossible trim: 7 rows allocated for a target of 4" in str(ei.value)
    lib, out = mklib(tmp_path, pair_rows()), tmp_path / "out"
    res = gate.run("plan", project=K, spec=d, lib_dir=str(lib), write=True, out_root=str(out), quiet=True)
    assert res["exit_code"] == 2 and not out.exists()


@pytest.mark.parametrize("quotas, order, named", [
    ({"target": 6, "assign": "rarest_match", "caps": "equal_share"}, None,
     "quotas.assign 'rarest_match' with quotas.caps 'equal_share' without a tier"),
    ({"target": 6, "assign": "fill_order_claim", "caps": "declared", "declared": B1_DECLARED}, None,
     "quotas.assign 'fill_order_claim' with quotas.caps 'declared' without a tier"),
    ({"target": 6, "tier_take_all": {"min_seeds": 2}, "declared": B1_DECLARED}, None,
     "quotas.assign 'rarest_match' with quotas.caps 'declared' and a tier_take_all"),
    ({"target": 6, "assign": "fill_order_claim", "caps": "equal_share", "default_cap": 3}, None,
     "quotas.default_cap does not apply to quotas.caps 'equal_share'"),
    ({"target": 6, "declared": B1_DECLARED}, {"method": "rank", "bucket_order": "size_asc"},
     "order.bucket_order applies to order.method 'round_robin' only"),
])
def test_every_unsupported_combination_is_refused_by_name(quotas, order, named):
    d = spec(quotas=quotas)
    if order:
        d["order"] = order
    with pytest.raises(gate.SpecError) as ei:
        gate.load_spec(d)
    assert "unsupported combination" in str(ei.value) and named in str(ei.value)


# ================================================================ 3. determinism
CHILD = """
import sys, os
sys.path.insert(0, sys.argv[1])
os.chdir(sys.argv[2])
from litpipe import gate
res = gate.run("plan", project="teaching_verify", spec=sys.argv[3], lib_dir=sys.argv[4], write=True,
               out_root=sys.argv[5], run_id="fixed", holdings_as_of=sys.argv[6], quiet=True)
print("EXIT", res["exit_code"], res["error"])
"""


def det_world(tmp_path):
    lib = mklib(tmp_path, claim_rows())
    (lib / "listed.csv").write_bytes(b"# a comment line\ndoi\n10.5555/P3\n10.5555/t1\n")
    d = claim_spec({"method": "round_robin", "bucket_order": "size_desc"}, holdings={"filter": True},
                   lanes=[{"name": "extra", "topics": ["alpha", "beta"], "size": 2}],
                   annotate=[{"column": "listed", "dois_from": "listed.csv"}], chapter_budgets={"U1": 3})
    for i, t in enumerate(d["topics"]):
        t["chapter"] = ["U1", "U1", "U2"][i]
    sp = tmp_path / "spec.json"
    sp.write_text(json.dumps(d), encoding="utf-8")
    snap = tmp_path / "held.txt"
    snap.write_text("# as of a past run\n10.5555/p6\n", encoding="utf-8")
    return lib, sp, snap


def run_bytes(folder):
    return {p.name: p.read_bytes() for p in sorted(Path(folder).iterdir())}


def test_runs_in_another_cwd_and_under_other_hash_seeds_write_byte_identical_files(tmp_path, monkeypatch):
    lib, sp, snap = det_world(tmp_path)
    outs = []
    for cwd in ("a", "b"):
        (tmp_path / cwd).mkdir()
        monkeypatch.chdir(tmp_path / cwd)
        res = gate.run("plan", project=K, spec=str(sp), lib_dir=str(lib), write=True,
                       out_root=str(tmp_path / f"out_{cwd}"), run_id="fixed", holdings_as_of=str(snap), quiet=True)
        assert res["exit_code"] == 0, res["error"]
        outs.append(run_bytes(res["run_dir"]))
    assert outs[0] == outs[1]
    sel = outs[0]["selection.csv"].decode("utf-8")
    assert "U1;U2;U3" in sel                                   # the seed-chapter set, sorted
    for seed in ("0", "4242", "987654321"):
        (tmp_path / f"cwd_{seed}").mkdir()
        env = {**os.environ, "PYTHONHASHSEED": seed, "PYTHONUTF8": "1"}
        p = subprocess.run([sys.executable, "-c", CHILD, str(REPO), str(tmp_path / f"cwd_{seed}"), str(sp), str(lib),
                            str(tmp_path / f"out_seed_{seed}"), str(snap)], env=env, capture_output=True, text=True,
                           timeout=180, cwd=str(REPO))
        assert "EXIT 0 None" in p.stdout, p.stdout + p.stderr
        assert run_bytes(tmp_path / f"out_seed_{seed}" / "_gate" / "verify" / "fixed") == outs[0]


def test_rows_shuffled_inside_rank_tie_groups_reorder_only_by_first_appearance():
    d = spec(quotas={"target": 3, "declared": [{"topic": "alpha", "cap": 3}, {"topic": "beta", "cap": 0},
                                               {"topic": "gamma", "cap": 0}]}, order={"method": "rank"})
    x = {k: rec(D(k), f"alpha {k}", cb=50) for k in ("x1", "x2", "x3")}       # tie on every rank key
    y = {k: rec(D(k), f"alpha {k}", cb=40) for k in ("y1", "y2")}             # a second tie group
    off = rec(D("off"), "an unrelated title", cb=1)
    first = gate.gate(gate.load_spec(d), harvest(x["x1"], x["x2"], x["x3"], y["y1"], y["y2"], off))
    again = gate.gate(gate.load_spec(d), harvest(x["x3"], x["x1"], x["x2"], y["y2"], y["y1"], off))
    assert keys(first.candidates) == [D(k) for k in ("x1", "x2", "x3", "y1", "y2")]
    assert keys(again.candidates) == [D(k) for k in ("x3", "x1", "x2", "y2", "y1")]
    assert keys(first.plan.order) == [D(k) for k in ("x1", "x2", "x3")]
    assert keys(again.plan.order) == [D(k) for k in ("x3", "x1", "x2")]
    assert first.plan.reasons == again.plan.reasons == {D("y1"): "over_cap:alpha", D("y2"): "over_cap:alpha"}


# ================================================================ 4. holdings and the drawn set
def content(lib, stem, doi):
    (lib / f"{stem}.ris").write_text(f"TY  - JOUR\nTI  - Fixture\nDO  - {doi}\nER  - \n", encoding="utf-8")
    (lib / f"{stem}.pdf").write_bytes(b"%PDF-1.4\n% fixture\n")


@pytest.fixture
def hreg(tmp_path, monkeypatch):
    root = tmp_path / "Projects"
    lib, other = root / K / "literature", root / "research_other" / "literature"
    lib.mkdir(parents=True)
    other.mkdir(parents=True)
    reg = {"state_dir": str(tmp_path / "state"),
           "projects": {K: {"lib_dir": "literature"}, "research_other": {"lib_dir": "literature"}}}
    cfgp = tmp_path / "projects.json"
    cfgp.write_text(json.dumps(reg), encoding="utf-8")
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    monkeypatch.setattr(config, "CONFIG_PATH", cfgp)
    for i in range(1, 5):
        content(lib, f"held_{i}", D(f"h{i}"))
    # (the DOIs carry a digit: litpipe.doi.normalise refuses an all-letter suffix such as 10.5555/txt)
    (lib / "ronly.ris").write_text(f"TY  - JOUR\nDO  - {D('r7o')}\nER  - \n", encoding="utf-8")     # .ris only
    (lib / "flag.pdf").write_bytes(b"%PDF-1.4\n% another work\n")                                     # FLAG verdict
    (lib / "flag.fulltext.json").write_text(json.dumps({"doi": D("f7g"), "identity": "FLAG", "has_pdf": True,
                                                        "text": "x " * 200}), encoding="utf-8")
    (lib / "txt.fulltext.json").write_text(json.dumps({"doi": D("t7x"), "has_pdf": False, "text": "y " * 200}),
                                           encoding="utf-8")                                          # text-only
    content(other, "oth", D("o7h"))
    write_harvest(lib / "_verify_h.csv", harvest(*[rec(D(k), f"alpha {k}", cb=90 - i) for i, k in
                                                   enumerate(("h1", "r7o", "f7g", "t7x", "o7h", "f1"))],
                                                 rec(D("off"), "an unrelated title")))
    return reg


@pytest.mark.parametrize("scope, held, in_scope", [
    ("project", {"h1", "t7x"}, 5),
    ("portfolio", {"h1", "t7x", "o7h"}, 6),
])
def test_live_holdings_count_content_only_and_scope_project_filters(hreg, scope, held, in_scope):
    d = spec(holdings={"filter": True, "scope": scope},
             quotas={"target": 10, "declared": [{"topic": "alpha", "cap": 10}, {"topic": "beta", "cap": 0},
                                                {"topic": "gamma", "cap": 0}]})
    res = gate.run("plan", project=K, spec=d, registry=hreg, quiet=True)
    assert res["exit_code"] == 0, res["error"]
    rep = res["report"]
    assert rep["holdings"]["mode"] == "live" and rep["holdings"]["controls"]["dois_in_scope"] == in_scope
    dropped = {r["doi"] for r in res["rows"]["drops"] if "held" in r["reasons"].split(";")}
    assert dropped == {D(k) for k in held}
    sel = {r["doi"] for r in res["rows"]["selection"]}
    assert {D("r7o"), D("f7g"), D("f1")} <= sel                # .ris-only and FLAG files are not holdings


def test_as_of_snapshot_with_bom_crlf_whole_line_comments_and_a_sici_doi_ending_in_hash(tmp_path):
    sici = "10.1002/(sici)0000-0000(200001)1:1<1::aid-x1>3.0.co;2-#"
    p = tmp_path / "snap.txt"
    p.write_bytes(b"\xef\xbb\xbf# header\r\n10.5555/a1\r\n  10.5555/b1  \r\n#10.5555/g1\r\n" + sici.encode() + b"\r\n\r\n")
    snap = gate.read_snapshot(p)
    assert snap == {D("a1"), D("b1"), sici}
    rows = b1_rows() + rec(sici, "alpha sici record", cb=1)
    res = gate.gate(gate.load_spec(spec(holdings={"filter": True})), rows, held=snap)
    held = {k for k, r in res.records.items() if "held" in r.reasons}
    assert held == {D("a1"), D("b1"), sici}


def state_file(path, dois):
    path.write_text(json.dumps({"version": worklists.STATE_VERSION, "dois": dois}), encoding="utf-8")


def test_the_drawn_set_comes_from_the_stable_pools_staged_and_swept_and_a_removed_lanes_state(tmp_path):
    lib = mklib(tmp_path, b1_rows())
    sp = gate.load_spec(spec())
    assert gate.read_drawn(lib, sp) == (set(), {"n": 0, "sha256": gate._set_hash(set()), "files": [],
                                                "swept_classes": {}, "staged_unswept": 0})
    st = {"run_id": "r1", "batch_path": "b.csv", "at": "t"}
    state_file(lib / "_verify_gate_selection.drawdown.json", {
        D("a1"): {"doi": D("a1"), "staged": st},
        D("b1"): {"doi": D("b1"), "staged": st, "swept": {"run_id": "r1", "class": "fetched", "at": "t"}},
        D("a2"): {"doi": D("a2")}})
    state_file(lib / "_verify_gate_lane_gone.drawdown.json",
               {D("g1"): {"doi": D("g1"), "swept": {"run_id": "r0", "class": "curated_out", "at": "t"}}})
    got, info = gate.read_drawn(lib, sp)
    assert got == {D("a1"), D("b1"), D("g1")}
    assert info["swept_classes"] == {"curated_out": 1, "fetched": 1} and info["staged_unswept"] == 1
    assert info["files"] == ["_verify_gate_lane_gone.drawdown.json", "_verify_gate_selection.drawdown.json"]
    res = gate.run("plan", project=K, spec=spec(), lib_dir=str(lib), quiet=True)
    assert res["exit_code"] == 0, res["error"]
    assert [r["doi"] for r in res["rows"]["selection"]] == [D("ab"), D("a2"), D("b2"), D("a3")]
    assert {r["doi"] for r in res["rows"]["drops"] if "drawn" in r["reasons"]} == {D("a1"), D("b1"), D("g1")}


@pytest.mark.parametrize("text, msg", [
    ("{not json", "is unreadable"),
    (json.dumps({"version": 99, "dois": {}}), "is not 1"),
    (json.dumps({"version": 1, "dois": {D("a1"): "staged"}}), "is not an object"),
])
def test_a_malformed_pool_state_exits_1_and_writes_nothing(tmp_path, text, msg):
    lib, out = mklib(tmp_path, b1_rows()), tmp_path / "out"
    (lib / "_verify_gate_selection.drawdown.json").write_text(text, encoding="utf-8")
    res = gate.run("plan", project=K, spec=spec(), lib_dir=str(lib), write=True, out_root=str(out), quiet=True)
    assert res["exit_code"] == 1 and msg in res["error"] and not out.exists()


@pytest.mark.parametrize("swept", [True, "2026-10-01"])
def test_a_swept_value_that_is_not_an_object_is_a_pool_state_error(tmp_path, swept):
    lib = mklib(tmp_path, b1_rows())
    state_file(lib / "_verify_gate_selection.drawdown.json", {D("a1"): {"doi": D("a1"), "swept": swept}})
    res = gate.run("plan", project=K, spec=spec(), lib_dir=str(lib), quiet=True)
    assert res["exit_code"] == 1 and "swept" in res["error"]


def test_invariant_12_end_to_end_keeps_the_first_row_and_pool_rows_sees_every_selected_row(tmp_path, monkeypatch):
    # holdings.doi_key folds a `.dup` suffix here, standing in for a normaliser that merges two citing DOIs
    real = holdings.doi_key
    monkeypatch.setattr(holdings, "doi_key", lambda raw: real(raw).removesuffix(".dup"))
    rows = b1_rows() + rec(D("a1") + ".dup", "alpha one again", cb=95)
    lib, out = mklib(tmp_path, rows), tmp_path / "out"
    d = spec()
    d["quotas"]["declared"][0]["cap"] = 3
    res = gate.run("plan", project=K, spec=d, lib_dir=str(lib), write=True, out_root=str(out), run_id="r", quiet=True)
    assert res["exit_code"] == 0, res["error"]
    sel = [r["doi"] for r in res["rows"]["selection"]]
    assert D("a1") + ".dup" in sel and D("a1") not in sel           # the first in draw order is kept
    col = [r for r in res["rows"]["drops"] if r["primary_reason"] == "pool_key_collision"]
    assert col == [{"doi": D("a1"), "title": "alpha one", "year": "2020", "n_seeds": 1,
                    "primary_reason": "pool_key_collision", "reasons": f"pool_key_collision:{D('a1')}.dup",
                    "stage": "invariant"}]
    rep = res["report"]
    assert len(rep["invariants"]["collisions"]) == 1
    assert rep["pool_key_collisions"] == [{"holdings_key": D("a1"), "keys": [D("a1") + ".dup", D("a1")]}]
    path = Path(res["run_dir"]) / "selection.csv"
    assert [r["doi"] for r in worklists.Pool(path).rows()] == sel    # Pool.rows() drops nothing silently


# ================================================================ 5. promote and runner batch, end to end
@pytest.fixture
def w(tmp_path, monkeypatch):
    world = World(tmp_path, monkeypatch)
    world.overrides(**stand_in_overrides())
    return world


def e2e_spec():
    return spec(lanes=[{"name": "extra", "topics": ["alpha"], "size": 1}])


def test_build_write_then_promote_is_refused_as_not_a_plan_run(w, tmp_path):
    w.register(K)
    write_harvest(w.lib(K) / "_verify_h.csv", b1_rows())
    b = gate.run("build", project=K, spec=e2e_spec(), write=True, quiet=True)
    assert b["exit_code"] == 0 and not (Path(b["run_dir"]) / "selection.csv").exists()
    pr = gate.run("promote", project=K, spec=e2e_spec(), run_id=Path(b["run_dir"]).name, quiet=True)
    assert pr["exit_code"] == 1 and "is not a plan run" in pr["error"]
    assert not (w.lib(K) / "_verify_gate_selection.csv").exists()


def test_promote_then_runner_batch_then_rebuild_end_to_end(w, tmp_path):
    w.register(K)
    lib, root = w.lib(K), w.proot(K)
    write_harvest(lib / "_verify_h.csv", b1_rows())
    plan = gate.run("plan", project=K, spec=e2e_spec(), write=True, quiet=True)
    assert plan["exit_code"] == 0, plan["error"]
    rid = Path(plan["run_dir"]).name
    pr = gate.run("promote", project=K, spec=e2e_spec(), run_id=rid, quiet=True)
    assert pr["exit_code"] == 0, pr["error"]
    sel, lane = lib / "_verify_gate_selection.csv", lib / "_verify_gate_lane_extra.csv"
    promoted = sel.read_bytes()
    assert promoted == (Path(plan["run_dir"]) / "selection.csv").read_bytes() and lane.is_file()
    tags = [runner.batch_tag(p.name) for p in sorted(lib.glob("_*_gate_*.csv"))]
    assert len(set(tags)) == len(tags) == 2                        # one project, no shared batch tag
    for p in (sel, lane, Path(plan["run_dir"]) / "report.json"):
        b = p.read_bytes()
        assert b"\r\n" not in b and not b.startswith(b"\xef\xbb\xbf")

    # a real runner process, through the W4-A harness: stage one batch of 2 (no sweep yet)
    code = w.harness("batch", "--project", K, "--pool", str(sel), "--size", "2", "--stage-only")
    assert code == 0, w.harness_log()
    bfile = root / "lit_pull_queue.b-verify_gate_selection.csv"
    with open(bfile, encoding="utf-8", newline="") as f:
        brows = list(csv.DictReader(f))
    selrows = plan["rows"]["selection"]
    assert [r["doi"] for r in brows] == [r["doi"] for r in selrows[:2]]
    assert [r["notes"] for r in brows] == [r["notes"] for r in selrows[:2]]
    assert brows[0]["notes"] == "gate=verify; lane=main; topic=gamma; n_seeds=1; ch="
    assert {r["destination"] for r in brows} == {os.path.relpath(lib, root).replace(os.sep, "/")}
    cfg = json.loads(w.cfg_path.read_text(encoding="utf-8"))
    assert worklists.Pool(sel, registry=cfg).status()["pending"] == 2

    # a second promote while that batch is pending is refused (exit 2), and --force overrides
    again = gate.run("promote", project=K, spec=e2e_spec(), run_id=rid, quiet=True)
    assert again["exit_code"] == 2 and "pending batches" in again["error"] and sel.read_bytes() == promoted
    forced = gate.run("promote", project=K, spec=e2e_spec(), run_id=rid, force=True, quiet=True)
    assert forced["exit_code"] == 0 and forced["pending_overridden"] == [
        "_verify_gate_selection.csv: 2 DOI(s) in 1 batch(es)"]

    # sweep the staged batch (in-process runner, stand-in sweep), then rebuild: drawn rows stay out
    assert runner.main(["batch", "--project", K, "--pool", str(sel), "--size", "2", "--batches", "1"]) == 0
    st = worklists.Pool(sel, registry=cfg).status()
    assert st["swept"] >= 2 and st["pending"] == 0
    drawn = {d for d, r in json.loads(worklists.state_path_for(sel).read_text(encoding="utf-8"))["dois"].items()
             if r.get("staged") or r.get("swept")}
    assert {r["doi"] for r in selrows[:2]} <= drawn
    re_plan = gate.run("plan", project=K, spec=e2e_spec(), quiet=True)
    assert re_plan["exit_code"] == 0, re_plan["error"]
    picked = {r["doi"] for r in re_plan["rows"]["selection"]} | {r["doi"] for r in re_plan["rows"]["lanes"]["extra"]}
    assert not picked & drawn
    assert {r["doi"] for r in re_plan["rows"]["drops"] if "drawn" in r["reasons"]} == drawn
    assert re_plan["report"]["drawn"]["n"] == len(drawn)


@pytest.mark.parametrize("name, lanes", [
    ("a" * 24, ["x"]),
    ("teaching_unit_three", ["t01", "t02"]),
    ("pool-2026-10-07", []),
    ("pool", ["2026-10-07"]),
])
def test_the_loader_refuses_names_whose_stable_paths_give_no_usable_runner_batch_tag(name, lanes):
    with pytest.raises(gate.SpecError, match="runner batch tag"):
        gate.load_spec(spec(name=name, lanes=[{"name": ln, "topics": ["alpha"], "size": 1} for ln in lanes]))


def test_names_of_the_recorded_shape_give_distinct_valid_tags():
    import sweep
    for name, lanes in (("verify", ["extra"]), ("demo", ["older"]), ("unit3_pool", ["t01", "t02", "t03"])):
        paths = [f"_{name}_gate_selection.csv"] + [f"_{name}_gate_lane_{ln}.csv" for ln in lanes]
        tags = [runner.batch_tag(p) for p in paths]
        assert len(set(tags)) == len(tags) and all(sweep.is_valid_tag(t) for t in tags)


# ================================================================ 6. zero inputs and sanity aborts
def _zero_case(case, tmp_path, monkeypatch):
    """(run kwargs, expected message, folder that must stay unchanged)."""
    on_topic = [r for r in b1_rows() if r["citing_doi"] != D("off")]
    if case == "live_fewer_than_4":
        root = tmp_path / "Projects"
        lib = root / K / "literature"
        lib.mkdir(parents=True)
        for i in range(1, 4):
            content(lib, f"held_{i}", D(f"h{i}"))
        write_harvest(lib / "_verify_h.csv", b1_rows())
        reg = {"state_dir": str(tmp_path / "state"), "projects": {K: {"lib_dir": "literature"}}}
        monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
        return {"spec": spec(holdings={"filter": True, "scope": "project"}), "registry": reg}, "fewer than 4", lib
    rows = {"zero_rows": [], "zero_records": [dict(r, citing_doi="") for r in b1_rows()],
            "no_offtopic_drops": on_topic}.get(case, b1_rows())
    lib = mklib(tmp_path, rows)
    snap = tmp_path / "snap.txt"
    kw = {"spec": spec(), "lib_dir": str(lib)}
    if case == "empty_snapshot":
        snap.write_text("# nothing held at that run\n\n", encoding="utf-8")
        kw.update(spec=spec(holdings={"filter": True}), holdings_as_of=str(snap))
    if case == "zero_candidates":
        snap.write_text("\n".join(r["citing_doi"] for r in on_topic) + "\n", encoding="utf-8")
        kw.update(spec=spec(holdings={"filter": True}), holdings_as_of=str(snap))
    msg = {"zero_rows": "the harvest has 0 rows", "zero_records": "0 records survive the collapse",
           "empty_snapshot": "holds 0 DOIs", "zero_candidates": "zero candidates",
           "no_offtopic_drops": "no record failed inclusion"}[case]
    return kw, msg, lib


@pytest.mark.parametrize("case", ["zero_rows", "zero_records", "empty_snapshot", "live_fewer_than_4",
                                  "zero_candidates", "no_offtopic_drops"])
def test_every_refusal_exits_2_with_a_message_and_a_summary_line_and_writes_nothing(tmp_path, monkeypatch, capsys,
                                                                                 case):
    kw, msg, watched = _zero_case(case, tmp_path, monkeypatch)
    before = files_under(watched)
    out, js = tmp_path / "out", tmp_path / "json" / "report.json"
    spec_d = kw.pop("spec")
    res = gate.run("plan", project=K, spec=spec_d, write=True, out_root=str(out), json_path=str(js), quiet=False,
                   **kw)
    assert res["exit_code"] == 2 and msg in res["error"], res["error"]
    assert not out.exists() and not js.exists() and files_under(watched) == before
    captured = capsys.readouterr()
    last = captured.out.strip().splitlines()[-1]
    assert last.startswith(gate.SUMMARY_MARKER) and json.loads(last[len(gate.SUMMARY_MARKER):])["reasons"]
    assert "[REFUSED]" in captured.err


# ================================================================ 7. reports add up
def report_rows():
    return b1_rows() + harvest(rec("", "alpha blank"), rec("10.5555/A1", "alpha one"),
                               rec(D("x1"), "alpha retracted", cb=3), rec(D("o1"), "alpha old", year=1990),
                               rec(D("nt"), ""))


def test_funnel_drop_log_topic_and_chapter_tables_add_up(tmp_path):
    rows = report_rows()
    lib, out = mklib(tmp_path, rows), tmp_path / "out"
    snap = tmp_path / "snap.txt"
    snap.write_text(D("b2") + "\n", encoding="utf-8")
    d = spec(years={"min": 2000}, exclude=[{"name": "retracted", "substrings": ["retracted"]}],
             holdings={"filter": True}, chapter_budgets={"U1": 4, "U2": 2})
    for t, ch in zip(d["topics"], ("U1", "U1", "U2")):
        t["chapter"] = ch
    res = gate.run("plan", project=K, spec=d, lib_dir=str(lib), holdings_as_of=str(snap), write=True,
                   out_root=str(out), run_id="r", quiet=True)
    assert res["exit_code"] == 0, res["error"]
    rep, out_rows = res["report"], res["rows"]
    f = rep["funnel"]
    distinct = {r["citing_doi"].strip().lower() for r in rows if r["citing_doi"].strip()}
    assert (f["harvest_rows"], f["rows_no_doi"], f["records"]) == (len(rows), 1, len(distinct)) == (14, 1, 11)
    build = [r for r in out_rows["drops"] if r["stage"] == "build"]
    plan_d = [r for r in out_rows["drops"] if r["stage"] == "plan"]
    assert len(build) + f["candidates"] == f["records"] and len({r["doi"] for r in build}) == len(build)
    assert sum(f["primary_reasons"].values()) == len(build)
    per_reason = {}
    for r in build:
        for x in r["reasons"].split(";"):
            per_reason[x] = per_reason.get(x, 0) + 1
    assert per_reason == f["reasons"] == {"excluded:retracted": 1, "held": 1, "no_title": 1, "no_topic": 2,
                                          "year": 1}
    assert f["inclusion_failures"] == 2
    p = rep["plan"]
    assert p["selected"] == len(out_rows["selection"]) == 5
    assert f["candidates"] == p["selected"] + len(plan_d)
    assert sum(t["taken"] for t in p["topics"].values()) + p["tier"] == p["selected"]
    assert all(t["taken"] <= t["cap"] for t in p["topics"].values() if isinstance(t["cap"], int))
    assert sum(rep["chapters"]["by_topic_chapter"].values()) == p["selected"]
    assert rep["chapters"]["budgets"] == {"U1": {"budget": 4, "taken": 4, "short": 0},
                                          "U2": {"budget": 2, "taken": 1, "short": 1}}
    rd = Path(res["run_dir"])
    with open(rd / "pool.csv", encoding="utf-8", newline="") as fh:
        assert len(list(csv.DictReader(fh))) == f["candidates"]
    with open(rd / "drops.csv", encoding="utf-8", newline="") as fh:
        assert len(list(csv.DictReader(fh))) == len(build) + len(plan_d)
    md = (rd / "report.md").read_text(encoding="utf-8")
    assert f"| candidates | {f['candidates']} |" in md and f"| records | {f['records']} |" in md


# ================================================================ 8. portability
EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+")
HOME = re.compile(r"(?i)(?:[A-Z]:[\\/]+Users[\\/]+|/home/)[A-Za-z0-9._-]+")
COURSE = re.compile(r"(?<![A-Za-z0-9])[A-Z]{3,4}\d{4}(?![0-9])")


@pytest.mark.parametrize("rel", ["litpipe/gate.py", "tests/test_gate.py", "tests/test_gate_cli.py",
                                 "tests/fixtures/gate/harvest_small.csv", "tests/fixtures/gate/seeds_small.json",
                                 "examples/gate_spec.example.json", "tests/test_w5_verify_r.py"])
def test_gate_files_carry_no_personal_paths_addresses_or_course_codes(rel):
    text = (REPO / rel).read_text(encoding="utf-8")
    if rel.endswith("test_w5_verify_r.py"):
        text = text.split("# ================================================================ 8. portability")[0]
    assert EMAIL.findall(text) == [] and HOME.findall(text) == [] and COURSE.findall(text) == []


def test_spec_paths_resolve_against_the_library_and_forward_slashes_work_everywhere(tmp_path):
    lib = tmp_path / "lib"
    write_harvest(lib / "sub" / "h.csv", b1_rows())
    (lib / "sub" / "seeds.json").write_text(json.dumps({"10.9/seed1": {"status": "OK"}}), encoding="utf-8")
    d = spec(input={"harvest": "sub/h.csv", "seeds_manifest": "sub/seeds.json"})
    res = gate.run("plan", project=K, spec=d, lib_dir=str(lib), quiet=True)
    assert res["exit_code"] == 0, res["error"]
    assert res["report"]["seeds"]["status"] == "READ"
    d["input"]["harvest"] = str((lib / "sub" / "h.csv").resolve())          # absolute
    assert gate.run("plan", project=K, spec=d, lib_dir=str(lib), quiet=True)["exit_code"] == 0
    if os.name == "nt":
        d["input"]["harvest"] = "sub\\h.csv"
        assert gate.run("plan", project=K, spec=d, lib_dir=str(lib), quiet=True)["exit_code"] == 0


def test_every_written_file_uses_lf_and_no_bom(tmp_path):
    lib, sp, snap = det_world(tmp_path)
    js = tmp_path / "j" / "report.json"
    res = gate.run("plan", project=K, spec=str(sp), lib_dir=str(lib), write=True, out_root=str(tmp_path / "out"),
                   holdings_as_of=str(snap), json_path=str(js), quiet=True)
    assert res["exit_code"] == 0, res["error"]
    files = sorted(Path(res["run_dir"]).iterdir()) + [js]
    assert {p.name for p in files} >= {"pool.csv", "selection.csv", "lane_extra.csv", "drops.csv", "report.json",
                                       "report.md", "manifest.json"}
    for p in files:
        b = p.read_bytes()
        assert b"\r" not in b and not b.startswith(b"\xef\xbb\xbf"), p.name
