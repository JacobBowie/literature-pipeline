"""litpipe.gate: the pure core (schema loader, matchers, controls, collapse, evaluate, rank, plan, trim,
order, lanes, invariants). Synthetic, neutral fixtures only; no network, no files outside tmp_path."""
import copy
import json

import pytest

from litpipe import gate
from litpipe.gate import GateAbort, SpecError


# ---------------------------------------------------------------- helpers
def base(**over):
    spec = {
        "schema": "litpipe.gate/1", "name": "teaching_demo",
        "input": {"harvest": "_demo_forward_citations.csv"},
        "topics": [
            {"name": "rain", "substrings": ["rainfall"], "controls": {"hit": ["Rainfall timing and crop yield"]}},
            {"name": "drought", "phrases": ["drought"], "controls": {"miss": ["Droughtmaster cattle"]}},
            {"name": "frost", "regex": [r"\bfrost\w*"]},
        ],
        "controls": {"match_none": ["A survey of tractor maintenance"]},
        "quotas": {"target": 10, "declared": [{"topic": "rain", "cap": 3}, {"topic": "drought", "cap": 3},
                                              {"topic": "frost", "cap": 3}]},
    }
    for k, v in over.items():
        spec[k] = v
    return spec


def load(spec):
    return gate.load_spec(spec)


def row(doi, title, year="2020", seed="s1", cited="0", chapter="", label="", authors="A. Author", venue="J"):
    return {"citing_doi": doi, "citing_title": title, "citing_year": str(year), "seed_doi": seed,
            "seed_label": label, "seed_chapter": chapter, "citing_cited_by": str(cited), "citing_authors": authors,
            "citing_venue": venue}


OFF = row("10.1/off", "A survey of tractor maintenance")


def run_core(spec, rows, **kw):
    return gate.gate(load(spec) if isinstance(spec, dict) else spec, rows, **kw)


def keys(recs):
    return [r.key for r in recs]


def set_at(d, path, value):
    d = copy.deepcopy(d)
    cur = d
    parts = path.split(".")
    for p in parts[:-1]:
        cur = cur[int(p)] if isinstance(cur, list) else cur.setdefault(p, {})
    last = parts[-1]
    if isinstance(cur, list):
        cur[int(last)] = value
    else:
        cur[last] = value
    return d


# ---------------------------------------------------------------- loader: defaults and strictness
def test_minimal_spec_fills_every_default():
    e = load(base()).eff
    assert e["description"] == "" and e["provenance"] == {} and e["recorded_run"] == {}
    assert e["input"]["columns"] == {"doi": "citing_doi", "title": "citing_title", "year": "citing_year",
                                     "authors": "citing_authors", "venue": "citing_venue",
                                     "cited_by": "citing_cited_by", "seed": ["seed_doi"],
                                     "seed_chapter": "seed_chapter"}
    assert e["input"]["doi_key"] == "lower" and e["input"]["seeds_manifest"] is None
    assert e["collapse"] == {"record_row": "first_with_title", "fill_empty": [], "cited_by": "max",
                             "text": ["html_unescape", "strip_tags", "collapse_whitespace"]}
    assert e["years"] is None and e["require"] == [] and e["exclude"] == []
    assert e["controls"]["contentless"] is True and e["controls"]["minimum"] == "gate"
    assert e["controls"]["require_offtopic_drops"] is True and e["controls"]["require_nonempty_pool"] is True
    assert e["holdings"] == {"filter": True, "scope": "portfolio"}
    assert e["rank"] == gate.DEFAULT_RANK
    q = e["quotas"]
    assert (q["tier_take_all"], q["assign"], q["caps"], q["default_cap"], q["allocation_order"]) == \
        (None, "rarest_match", "declared", None, "bucket_size_desc")
    assert e["chapter_budgets"] is None and e["lanes"] == [] and e["annotate"] == []
    assert e["order"] == {"method": "round_robin", "bucket_order": "size_asc"}
    assert e["reports"] == {"direction_split": []}
    assert e["output"]["notes_template"] == gate.DEFAULT_NOTES


def test_years_defaults_when_set():
    e = load(base(years={"min": 2015})).eff
    assert e["years"] == {"min": 2015, "max": None, "unparsed": "drop", "scope": "record"}


def test_duplicate_json_keys_are_refused_at_every_level():
    text = json.dumps(base())
    with pytest.raises(SpecError, match="duplicate JSON key 'name'"):
        gate.load_spec(text[:-1] + ', "name": "other"}')
    nested = text.replace('"quotas": {', '"quotas": {"target": 5, ', 1)
    with pytest.raises(SpecError, match="duplicate JSON key 'target'"):
        gate.load_spec(nested)


def test_nan_and_non_object_specs_are_refused():
    with pytest.raises(SpecError, match="constant NaN"):
        gate.load_spec(json.dumps(base())[:-1] + ', "provenance": {"x": NaN}}')
    with pytest.raises(SpecError, match="must be a JSON object"):
        gate.parse_spec_text("[1, 2]")
    with pytest.raises(SpecError, match="not valid JSON"):
        gate.parse_spec_text("{")


@pytest.mark.parametrize("path, value", [
    ("bogus", 1), ("input.bogus", 1), ("input.columns.bogus", "x"), ("collapse.bogus", 1),
    ("years", {"min": 2000, "bogus": 1}), ("topics.0.bogus", 1), ("topics.0.controls.bogus", []),
    ("controls.bogus", 1), ("holdings.bogus", 1), ("rank", [{"field": "n_seeds", "bogus": 1}]),
    ("quotas.bogus", 1), ("order.bogus", 1), ("lanes", [{"name": "x", "topics": ["rain"], "size": 1, "bogus": 1}]),
    ("annotate", [{"column": "flag", "dois_from": "f.csv", "bogus": 1}]), ("reports.bogus", []),
    ("output.bogus", 1), ("input.harvest", {"scope": "demo", "bogus": 1}),
    ("controls.match_none", [{"title": "x", "bogus": 1}]),
])
def test_unknown_keys_are_refused(path, value):
    with pytest.raises(SpecError, match="unknown key"):
        load(set_at(base(), path, value))


def test_output_destination_was_dropped():
    with pytest.raises(SpecError, match="destination was dropped"):
        load(base(output={"destination": "literature/"}))


@pytest.mark.parametrize("path, value", [
    ("input.doi_key", "upper"), ("collapse.record_row", "last"), ("collapse.cited_by", "min"),
    ("collapse.text", ["lowercase"]), ("collapse.fill_empty", ["abstract"]), ("years", {"unparsed": "zero"}),
    ("years", {"scope": "doi"}), ("controls.minimum", "topic"), ("holdings.scope", "machine"),
    ("rank", [{"field": "oa", "order": "desc"}]), ("rank", [{"field": "year", "order": "down"}]),
    ("quotas.assign", "first_match"), ("quotas.caps", "proportional"), ("quotas.allocation_order", "name"),
    ("order.method", "proportional"), ("order.bucket_order", "random"),
    ("reports", {"direction_split": [{"topic": "rain", "match": "letters",
                                      "sides": [{"name": "a", "words": ["x"]}, {"name": "b", "words": ["y"]}]}]}),
])
def test_bad_enums_are_refused(path, value):
    with pytest.raises(SpecError, match="must be one of"):
        load(set_at(base(), path, value))


@pytest.mark.parametrize("path, value, msg", [
    ("schema", "litpipe.gate/2", "schema must be"),
    ("name", "Bad Name", "must match"),
    ("name", "", "must not be empty"),
    ("description", 3, "must be a string"),
    ("provenance", [], "must be an object"),
    ("topics", [], "at least 1"),
    ("topics.0.name", "Rain", "must match"),
    ("topics.0.substrings", [""], "must not be empty"),
    ("topics.0", {"name": "rain"}, "has no pattern"),
    ("topics.2.regex", ["(unclosed"], "does not compile"),
    ("topics.2.regex", ["a*"], "matches the empty title"),
    ("topics.1.name", "rain", "more than once"),
    ("topics.0.chapter", 5, "must be a string"),
    ("years", {"min": 2020, "max": 2010}, "above max"),
    ("years", {"min": "2015"}, "must be an integer"),
    ("years", {"min": True}, "must be an integer"),
    ("rank", [{"field": "year"}, {"field": "year"}], "twice"),
    ("quotas.target", 0, ">= 1"),
    ("quotas.target", "10", "must be an integer"),
    ("quotas.declared", [{"topic": "rain", "cap": -1}, {"topic": "drought", "cap": 1}, {"topic": "frost", "cap": 1}],
     ">= 0"),
    ("quotas.declared", [{"topic": "rain", "cap": "all"}], "must be an integer"),
    ("quotas.declared", [{"topic": "snow", "cap": 1}], "is not a topic"),
    ("quotas.declared", [{"topic": "rain", "cap": 1}, {"topic": "rain", "cap": 2}], "twice"),
    ("quotas.declared", [{"topic": "rain", "cap": 1}], "does not cap drought, frost"),
    ("quotas.default_cap", -2, ">= 0"),
    ("lanes", [{"name": "main", "topics": ["rain"], "size": 1}], "reserved"),
    ("lanes", [{"name": "x", "topics": ["rain"], "size": 1}, {"name": "x", "topics": ["rain"], "size": 1}], "twice"),
    ("lanes", [{"name": "x", "topics": ["snow"], "size": 1}], "is not a topic"),
    ("lanes", [{"name": "x", "topics": [], "size": 1}], "at least 1"),
    ("lanes", [{"name": "x", "topics": ["rain"], "size": 0}], ">= 1"),
    ("annotate", [{"column": "doi", "dois_from": "f.csv"}], "collides"),
    ("annotate", [{"column": "Flag", "dois_from": "f.csv"}], "must match"),
    ("reports", {"direction_split": [{"topic": "snow", "sides": []}]}, "is not a topic"),
    ("reports", {"direction_split": [{"topic": "rain", "sides": [{"name": "a", "words": ["x"]}]}]}, "at least 2"),
    ("reports", {"direction_split": [{"topic": "rain", "sides": [{"name": "a", "words": ["x"]},
                                                                {"name": "a", "words": ["y"]}]}]}, "twice"),
    ("output.notes_template", "doi={doi}", "allowed"),
    ("output.notes_template", "n={n_seeds", "not a valid format"),
    ("chapter_budgets", {"U1": -1}, ">= 0"),
    ("input.harvest", {"scope": "Bad Scope"}, "must match"),
    ("input.columns.seed", [], "at least 1"),
    ("controls.contentless", "yes", "true or false"),
    ("holdings.filter", 1, "true or false"),
    ("quotas.tier_take_all", {"min_seeds": 0}, ">= 1"),
])
def test_bad_values_are_refused(path, value, msg):
    with pytest.raises(SpecError, match=msg):
        load(set_at(base(), path, value))


def test_template_accepts_numeric_format_specs():
    assert load(base(output={"notes_template": "n={n_seeds:03d}; {topics}"})).eff["output"]["notes_template"]


# ---------------------------------------------------------------- supported combinations
CLAIM = {"target": 10, "assign": "fill_order_claim", "caps": "equal_share"}


@pytest.mark.parametrize("quotas, order", [
    ({"target": 10, "declared": [{"topic": t, "cap": 2} for t in ("rain", "drought", "frost")]}, {}),
    ({"target": 10, "declared": [{"topic": t, "cap": 2} for t in ("rain", "drought", "frost")]}, {"method": "rank"}),
    (CLAIM, {"method": "rank"}), (CLAIM, {}),
    ({**CLAIM, "tier_take_all": {"min_seeds": 2}}, {"method": "rank"}),
    ({**CLAIM, "tier_take_all": {"min_seeds": 2}}, {"method": "round_robin", "bucket_order": "size_desc"}),
])
def test_supported_combinations_load(quotas, order):
    load(base(quotas=quotas, order=order))


@pytest.mark.parametrize("quotas, msg", [
    ({"target": 10, "assign": "rarest_match", "caps": "equal_share"}, "'rarest_match' with quotas.caps 'equal_share'"),
    ({"target": 10, "assign": "fill_order_claim", "caps": "declared", "declared": []},
     "'fill_order_claim' with quotas.caps 'declared'"),
    ({"target": 10, "tier_take_all": {"min_seeds": 2}, "declared": [{"topic": "rain", "cap": 1}],
      "default_cap": 1}, "and a tier_take_all"),
    ({**CLAIM, "declared": []}, "quotas.declared does not apply"),
    ({**CLAIM, "default_cap": 3}, "quotas.default_cap does not apply"),
    ({**CLAIM, "allocation_order": "cap_desc"}, "quotas.allocation_order does not apply"),
    ({"target": 10}, "quotas.declared is required"),
])
def test_unsupported_combinations_are_refused_by_name(quotas, msg):
    with pytest.raises(SpecError, match="unsupported combination|required"):
        load(base(quotas=quotas))
    with pytest.raises(SpecError) as e:
        load(base(quotas=quotas))
    assert msg in str(e.value)


def test_bucket_order_with_rank_is_refused():
    with pytest.raises(SpecError, match="bucket_order applies to order.method 'round_robin' only"):
        load(base(order={"method": "rank", "bucket_order": "size_asc"}))


def test_load_from_path_and_text_record_the_sha(tmp_path):
    p = tmp_path / "spec.json"
    p.write_text(json.dumps(base()), encoding="utf-8")
    a, b = gate.load_spec(p), gate.load_spec(str(p))
    assert a.sha256 == b.sha256 and len(a.sha256) == 64
    with pytest.raises(SpecError, match="cannot read the spec"):
        gate.load_spec(tmp_path / "missing.json")


# ---------------------------------------------------------------- matchers
def test_substring_is_an_unanchored_stem():
    rx = gate.compile_pattern("substrings", "precipitat")
    assert rx.search("Precipitation in uplands") and rx.search("PRECIPITATING clouds")
    assert gate.compile_pattern("substrings", "hail").search("Hailey and the weather")


def test_phrase_is_letter_bounded_and_allows_digits():
    rx = gate.compile_pattern("phrases", "soil moisture")
    assert rx.search("Soil moisture in fields") and rx.search("Plot 3soil moisture2 readings")
    assert not rx.search("Topsoil moisture") and not rx.search("soil moistures")
    assert gate.compile_pattern("phrases", "c-band").search("The C-band radar")


def test_regex_is_compiled_as_given_ignorecase():
    rx = gate.compile_pattern("regex", r"\bfrost\w*")
    assert rx.search("FROSTED blossoms") and not rx.search("Defrosting freezers")
    guarded = gate.compile_pattern("regex", r"hail(?:s)?\b")
    assert guarded.search("Hails in spring storms") and not guarded.search("Hailey and the weather")


def test_the_engine_never_rewrites_a_pattern():
    sp = load(base())
    kinds = [(k, src) for m in sp.topics for k, src, _ in m.patterns]
    assert kinds == [("substrings", "rainfall"), ("phrases", "drought"), ("regex", r"\bfrost\w*")]


# ---------------------------------------------------------------- controls
def _controls(spec):
    return gate.run_controls(load(spec))


def test_controls_pass_and_count():
    c = _controls(base())
    assert c["failures"] == [] and c["positives"] == 1 and c["negatives"] == 2 and c["checked"] == 4


@pytest.mark.parametrize("change, fails_with", [
    (("topics.0.controls.hit", ["Snowfall records"]), "hit control does not match"),
    (("topics.1.controls.miss", ["Drought stress"]), "miss control matches"),
    (("controls.match_none", ["Rainfall and soil"]), "match_none control matches"),
    (("controls.admit", ["A survey of tractor maintenance"]), "admit control is not admitted"),
])
def test_each_control_kind_fails(change, fails_with):
    c = _controls(set_at(base(), *change))
    assert any(fails_with in f for f in c["failures"])


def test_exclude_controls_and_exclude_none():
    spec = base(exclude=[{"name": "lab_only", "phrases": ["in vitro"],
                          "controls": {"hit": ["Genes in vitro"], "miss": ["Genes in vivo"]}}],
                controls={"match_none": ["A survey of tractor maintenance"], "exclude_none": ["Drought in vitro?"]})
    c = _controls(spec)
    assert any("exclude_none control matches exclude(s) ['lab_only']" in f for f in c["failures"])
    spec["controls"]["exclude_none"] = ["Drought in the field"]
    assert _controls(spec)["failures"] == []


def test_admit_with_require_needs_every_facet_and_no_exclude():
    spec = base(require=[{"name": "field", "phrases": ["field"]}],
                exclude=[{"name": "lab_only", "phrases": ["in vitro"]}],
                controls={"match_none": ["A survey of tractor maintenance"],
                          "admit": ["Rainfall field trial", "Rainfall field trial in vitro", "Rainfall alone"]})
    fails = _controls(spec)["failures"]
    assert sum("admit control is not admitted" in f for f in fails) == 2


def test_contentless_control_and_its_switch():
    spec = base(topics=[{"name": "rain", "substrings": ["rainfall", "investigation"]}],
                quotas={"target": 5, "declared": [{"topic": "rain", "cap": 5}]},
                controls={"match_none": ["A survey of tractor maintenance"], "admit": ["Rainfall"]})
    assert any("contentless" in f for f in _controls(spec)["failures"])
    spec["controls"]["contentless"] = False
    assert _controls(spec)["failures"] == []


def test_minimum_gate_needs_a_positive_and_a_negative():
    no_neg = base(topics=[{"name": "rain", "substrings": ["rainfall"], "controls": {"hit": ["Rainfall"]}}],
                  controls={}, quotas={"target": 5, "declared": [{"topic": "rain", "cap": 5}]})
    assert any("no negative control" in f for f in _controls(no_neg)["failures"])
    no_pos = base(topics=[{"name": "rain", "substrings": ["rainfall"]}], controls={"match_none": ["Snow"]},
                  quotas={"target": 5, "declared": [{"topic": "rain", "cap": 5}]})
    assert any("no positive control" in f for f in _controls(no_pos)["failures"])
    no_pos["controls"]["minimum"] = "none"
    assert _controls(no_pos)["failures"] == []


def test_require_miss_counts_as_a_negative_and_require_hit_as_a_positive():
    spec = base(topics=[{"name": "rain", "substrings": ["rainfall"]}], controls={},
                require=[{"name": "field", "phrases": ["field"], "controls": {"hit": ["A field trial"],
                                                                            "miss": ["A lab trial"]}}],
                quotas={"target": 5, "declared": [{"topic": "rain", "cap": 5}]})
    c = _controls(spec)
    assert c["failures"] == [] and c["positives"] == 1 and c["negatives"] == 1


def test_minimum_none_runs_with_a_warning():
    spec = base(topics=[{"name": "rain", "substrings": ["rainfall"]}], controls={"minimum": "none"},
                quotas={"target": 5, "declared": [{"topic": "rain", "cap": 5}]})
    res = run_core(spec, [row("10.1/a", "Rainfall one"), OFF])
    assert any("minimum is 'none'" in w for w in res.warnings)


def test_controls_failure_refuses_the_run():
    with pytest.raises(GateAbort) as e:
        run_core(set_at(base(), "topics.0.controls.hit", ["Snowfall"]), [row("10.1/a", "Rainfall"), OFF])
    assert e.value.exit_code == 2 and e.value.reasons == ["controls"]


def test_regression_a_bare_substring_in_place_of_a_guarded_regex_fails_the_controls():
    """Replacing a guarded regex with a bare substring must make the controls exit 2 (the guard's
    `miss` control is what catches the prefix collision)."""
    guarded = base(topics=[{"name": "storms", "regex": [r"hail(?:s)?\b"], "substrings": ["thunderstorm"],
                            "controls": {"hit": ["Hails in spring storms"],
                                         "miss": ["Hailey and the weather station"]}}],
                   quotas={"target": 5, "declared": [{"topic": "storms", "cap": 5}]})
    assert _controls(guarded)["failures"] == []
    bare = copy.deepcopy(guarded)
    bare["topics"][0]["regex"] = []
    bare["topics"][0]["substrings"].append("hail")
    with pytest.raises(GateAbort, match="miss control matches"):
        run_core(bare, [row("10.1/a", "Thunderstorm frequency"), OFF])


# ---------------------------------------------------------------- collapse switches
def _recs(spec, rows, years=None):
    sp = load(spec)
    recs, counts, seed_rows = gate.collapse(sp, rows, sp.eff["years"] if years is None else years)
    return recs, counts, seed_rows


def test_record_row_first_vs_first_with_title():
    rows = [row("10.1/a", "", year="2001", cited="1", authors="X"), row("10.1/a", "Rainfall", year="2002", cited="5",
                                                                      authors="Y")]
    first, _, _ = _recs(base(collapse={"record_row": "first", "cited_by": "record_row"}), rows)
    titled, _, _ = _recs(base(collapse={"record_row": "first_with_title", "cited_by": "record_row"}), rows)
    a, b = first["10.1/a"], titled["10.1/a"]
    assert (a.title, a.year, a.authors, a.cited_by) == ("", "2001", "X", 1)
    assert (b.title, b.year, b.authors, b.cited_by) == ("Rainfall", "2002", "Y", 5)


def test_first_with_title_keeps_the_first_row_when_no_row_has_a_title():
    recs, _, _ = _recs(base(), [row("10.1/a", "", year="2001"), row("10.1/a", "", year="2002")])
    assert recs["10.1/a"].year == "2001" and recs["10.1/a"].title == ""


def test_fill_empty_fills_from_the_first_later_row_with_a_value():
    rows = [row("10.1/a", "", year="", authors="", venue=""),
            row("10.1/a", "Rainfall", year="2010", authors="B", venue=""),
            row("10.1/a", "Other", year="2011", authors="C", venue="V")]
    recs, _, _ = _recs(base(collapse={"record_row": "first", "fill_empty": ["title", "authors", "venue", "year"]}),
                       rows)
    r = recs["10.1/a"]
    assert (r.title, r.authors, r.venue, r.year) == ("Rainfall", "B", "V", "2010")
    none, _, _ = _recs(base(collapse={"record_row": "first"}), rows)
    assert none["10.1/a"].title == ""


def test_cited_by_max_skips_non_integers_and_record_row_reads_the_record_row():
    rows = [row("10.1/a", "Rainfall", cited=""), row("10.1/a", "Rainfall", cited="n/a"),
            row("10.1/a", "Rainfall", cited="1,234"), row("10.1/a", "Rainfall", cited="inf"),
            row("10.1/a", "Rainfall", cited="12")]
    mx, _, _ = _recs(base(collapse={"cited_by": "max"}), rows)
    assert mx["10.1/a"].cited_by == 1234
    rr, _, _ = _recs(base(collapse={"cited_by": "record_row", "record_row": "first"}), rows)
    assert rr["10.1/a"].cited_by == 0


@pytest.mark.parametrize("ops, want", [
    ([], "&amp; <i>Rain</i>   fall"),
    (["html_unescape"], "& <i>Rain</i>   fall"),
    (["strip_tags"], "&amp; Rain   fall"),
    (["collapse_whitespace"], "&amp; <i>Rain</i> fall"),
    (["html_unescape", "strip_tags", "collapse_whitespace"], "& Rain fall"),
])
def test_text_cleanups_and_strip(ops, want):
    recs, _, _ = _recs(base(collapse={"text": ops}), [row("10.1/a", "  &amp; <i>Rain</i>   fall  ")])
    assert recs["10.1/a"].title == want


def test_strip_tags_runs_before_unescape():
    recs, _, _ = _recs(base(collapse={"text": ["html_unescape", "strip_tags"]}),
                       [row("10.1/a", "Escaped &lt;b&gt; stays")])
    assert recs["10.1/a"].title == "Escaped <b> stays"


@pytest.mark.parametrize("mode, raw, key", [
    ("lower", " 10.1/ABC. ", "10.1/abc."),
    ("lower", "https://doi.org/10.1/ABC", "https://doi.org/10.1/abc"),
    ("lower_unresolve_rstrip", "https://doi.org/10.1/ABC.;", "10.1/abc"),
    ("lower_unresolve_rstrip", "dx.doi.org/10.1/x, ", "10.1/x"),
    ("lower_unresolve_rstrip", "10.1002/(sici)1098-240x(200002)23:1#", "10.1002/(sici)1098-240x(200002)23:1#"),
])
def test_doi_key_modes(mode, raw, key):
    assert gate.doi_key_fn(mode)(raw) == key


def test_seed_key_first_non_empty_column_blank_seeds_and_chapters():
    spec = base(input={"harvest": "h.csv", "columns": {"seed": ["seed_label", "seed_doi"]}})
    rows = [row("10.1/a", "Rainfall", seed="10.9/s1", label="Label One", chapter="U1"),
            row("10.1/a", "Rainfall", seed="10.9/s1", label="", chapter="U2"),
            row("10.1/a", "Rainfall", seed="", label="", chapter=""),
            row("", "Rainfall no doi")]
    recs, counts, seed_rows = _recs(spec, rows)
    r = recs["10.1/a"]
    assert r.seeds == {"Label One", "10.9/s1"} and r.n_seeds == 2 and r.chapters == {"U1", "U2"}
    assert counts["rows_blank_seed"] == 1 and counts["rows_no_doi"] == 1 and counts["harvest_rows"] == 4
    assert seed_rows == {"Label One": 1, "10.9/s1": 1} and r.n_rows == 3


# ---------------------------------------------------------------- years
@pytest.mark.parametrize("years, kept", [
    ({"min": 2015, "unparsed": "drop", "scope": "record"}, {"10.1/new"}),
    ({"min": 2015, "unparsed": "keep", "scope": "record"}, {"10.1/new", "10.1/none"}),
    ({"min": 2010, "max": 2014, "unparsed": "drop", "scope": "record"}, {"10.1/old"}),
    ({"min": 2015, "max": 2015, "scope": "record"}, set()),
])
def test_record_scope_years(years, kept):
    rows = [row("10.1/new", "Rainfall a", year="2016"), row("10.1/none", "Rainfall b", year=""),
            row("10.1/old", "Rainfall c", year="2014"), OFF]
    spec = base(years=years, controls={"match_none": ["A survey of tractor maintenance"],
                                       "require_nonempty_pool": False})
    res = run_core(spec, rows, command="build")
    assert {r.key for r in res.candidates} == kept
    reasons = {r.key: r.reasons for r in res.records.values()}
    if "10.1/none" not in kept:
        assert reasons["10.1/none"][0] == "year_unparsed"
    if "10.1/old" not in kept:
        assert reasons["10.1/old"][0] == "year"


def test_row_scope_years_drop_rows_before_the_collapse():
    rows = [row("10.1/a", "Rainfall early", year="2010", seed="s1"),
            row("10.1/a", "Rainfall late", year="2016", seed="s2"),
            row("10.1/b", "Rainfall none", year=""), OFF]
    drop = run_core(base(years={"min": 2015, "scope": "row"}), rows, command="build")
    a = drop.records["10.1/a"]
    assert a.title == "Rainfall late" and a.n_seeds == 1 and "10.1/b" not in drop.records
    assert drop.counts["rows_year_dropped"] == 2
    keep = run_core(base(years={"min": 2015, "scope": "row", "unparsed": "keep"}), rows, command="build")
    assert "10.1/b" in keep.records and keep.counts["rows_year_dropped"] == 1


# ---------------------------------------------------------------- reasons, sanity, zero inputs
def test_every_build_reason_is_recorded_on_one_record():
    spec = base(years={"min": 2015}, require=[{"name": "field", "phrases": ["field"]}],
                exclude=[{"name": "lab_only", "phrases": ["in vitro"]}, {"name": "pot", "phrases": ["pot"]}],
                controls={"match_none": ["A survey of tractor maintenance"]})
    rows = [row("10.1/a", "Rainfall pot study in vitro", year="2001"),
            row("10.1/b", "Rainfall field study"), row("10.1/c", "", year="2020"), OFF]
    res = run_core(spec, rows, held={"10.1/a"}, drawn={"10.1/a"}, command="build")
    assert res.records["10.1/a"].reasons == ["year", "missing:field", "excluded:lab_only", "excluded:pot", "held",
                                             "drawn"]
    assert res.records["10.1/c"].reasons == ["no_title", "missing:field"]
    no_req = run_core(base(), [row("10.1/c", ""), row("10.1/d", "Rainfall")], command="build")
    assert no_req.records["10.1/c"].reasons == ["no_title", "no_topic"]


def test_sanity_aborts_and_their_switches():
    with pytest.raises(GateAbort, match="zero candidates"):
        run_core(base(), [OFF, row("10.1/b", "Rainfall")], held={"10.1/b"}, command="build")
    with pytest.raises(GateAbort, match="no record failed inclusion"):
        run_core(base(), [row("10.1/a", "Rainfall")], command="build")
    ok = base(controls={"match_none": ["A survey of tractor maintenance"], "require_nonempty_pool": False,
                        "require_offtopic_drops": False})
    run_core(ok, [row("10.1/a", "Rainfall")], command="build")
    run_core(ok, [OFF], command="build")


def test_zero_input_refusals_in_the_core():
    with pytest.raises(GateAbort) as e:
        run_core(base(), [], command="build")
    assert e.value.reasons == ["zero_input:harvest"]
    with pytest.raises(GateAbort) as e:
        run_core(base(), [row("", "Rainfall"), row(" ", "Rainfall")], command="build")
    assert e.value.reasons == ["zero_input:records"]
    with pytest.raises(GateAbort) as e:
        run_core(base(years={"min": 2015, "scope": "row"}), [row("10.1/a", "Rainfall", year="1990")], command="build")
    assert e.value.reasons == ["zero_input:records"]


def test_holdings_filter_false_ignores_held():
    res = run_core(base(holdings={"filter": False}), [row("10.1/a", "Rainfall"), OFF], held={"10.1/a"}, command="build")
    assert keys(res.candidates) == ["10.1/a"]


def test_held_as_a_callable_and_as_exact_string_membership():
    res = run_core(base(), [row("10.1/A", "Rainfall a"), row("10.1/b", "Rainfall b"), OFF],
                   held={"10.1/A", "10.1/b"}, command="build")
    assert keys(res.candidates) == ["10.1/a"]
    res = run_core(base(), [row("10.1/a", "Rainfall a"), row("10.1/b", "Rainfall b"), OFF],
                   held=lambda k: k.endswith("a"), command="build")
    assert keys(res.candidates) == ["10.1/b"]


def test_drawn_dois_are_dropped_by_holdings_key():
    res = run_core(base(), [row("10.5555/rain.1", "Rainfall a"), row("10.5555/rain.2", "Rainfall b"), OFF],
                   drawn={"https://doi.org/10.5555/RAIN.1."}, command="build")
    assert keys(res.candidates) == ["10.5555/rain.2"] and res.records["10.5555/rain.1"].reasons == ["drawn"]


# ---------------------------------------------------------------- rank
def test_rank_ties_are_broken_by_first_appearance_not_doi():
    rows = [row("10.1/z", "Rainfall z", cited="5"), row("10.1/a", "Rainfall a", cited="5"),
            row("10.1/m", "Rainfall m", cited="9"), OFF]
    res = run_core(base(), rows, command="build")
    assert keys(res.candidates) == ["10.1/m", "10.1/z", "10.1/a"]
    assert [r.pool_rank for r in res.candidates] == [1, 2, 3]


def test_rank_keys_orders_and_unparsed_year_as_zero():
    rows = [row("10.1/a", "Rainfall a", year="2010"), row("10.1/b", "Rainfall b", year=""),
            row("10.1/c", "Rainfall c", year="2020"), OFF]
    asc = run_core(base(rank=[{"field": "year", "order": "asc"}]), rows, command="build")
    assert keys(asc.candidates) == ["10.1/b", "10.1/a", "10.1/c"]
    desc = run_core(base(rank=[{"field": "year"}]), rows, command="build")
    assert keys(desc.candidates) == ["10.1/c", "10.1/a", "10.1/b"]
    seeds = run_core(base(rank=[{"field": "n_seeds"}]),
                     [row("10.1/a", "Rainfall a"), row("10.1/b", "Rainfall b", seed="s1"),
                      row("10.1/b", "Rainfall b", seed="s2"), OFF], command="build")
    assert keys(seeds.candidates) == ["10.1/b", "10.1/a"]


# ---------------------------------------------------------------- plan: rarest_match + declared
def _pool(*specs):
    """Rows from (doi, title, cited) with descending cited_by so rank order is the given order."""
    return [row(d, t, cited=str(1000 - i)) for i, (d, t) in enumerate(specs)] + [OFF]


def test_rarest_match_assigns_the_rarest_topic_ties_by_name():
    rows = _pool(("10.1/a", "Rainfall and drought"), ("10.1/b", "Rainfall"), ("10.1/c", "Drought frost"),
                 ("10.1/d", "Frost"), ("10.1/e", "Frost and rainfall"))
    res = run_core(base(rank=[{"field": "cited_by"}]), rows)
    # freq: rain 3, drought 2, frost 3 -> a: drought; c: drought; e: tie frost/rain at 3 -> frost (name)
    assert {k: res.plan.assigned[k] for k in ("10.1/a", "10.1/c", "10.1/e")} == \
        {"10.1/a": "drought", "10.1/c": "drought", "10.1/e": "frost"}


def test_take_all_default_cap_no_cap_and_over_cap():
    spec = base(rank=[{"field": "cited_by"}],
                quotas={"target": 20, "declared": [{"topic": "rain", "cap": "take_all"}, {"topic": "drought", "cap": 0}],
                        "default_cap": 1})
    rows = _pool(("10.1/r1", "Rainfall"), ("10.1/r2", "Rainfall"), ("10.1/d1", "Drought"), ("10.1/f1", "Frost"),
                 ("10.1/f2", "Frost"))
    res = run_core(spec, rows)
    assert sorted(keys(res.plan.order)) == ["10.1/f1", "10.1/r1", "10.1/r2"]
    assert res.plan.reasons == {"10.1/d1": "no_cap:drought", "10.1/f2": "over_cap:frost"}
    t = res.plan.table
    assert (t["rain"]["cap"], t["drought"]["cap"], t["frost"]["cap"]) == ("take_all", 0, 1)
    assert t["frost"]["available"] == 2 and t["frost"]["taken"] == 1 and t["drought"]["short"] == 0


def test_rarest_without_a_topic_is_no_quota_topic():
    spec = base(require=[{"name": "field", "phrases": ["field"]}], rank=[{"field": "cited_by"}],
                controls={"match_none": ["A survey of tractor maintenance"]})
    res = run_core(spec, _pool(("10.1/a", "Rainfall field"), ("10.1/b", "Plain field notes")))
    assert res.plan.reasons == {"10.1/b": "no_quota_topic"}


def test_allocation_order_bucket_size_desc_ties_by_first_appearance():
    spec = base(rank=[{"field": "cited_by"}], order={"method": "rank"},
                quotas={"target": 20, "default_cap": 5, "declared": []})
    rows = _pool(("10.1/f1", "Frost"), ("10.1/r1", "Rainfall"), ("10.1/r2", "Rainfall"), ("10.1/f2", "Frost"),
                 ("10.1/d1", "Drought"))
    assert run_core(spec, rows).plan.allocation == ["frost", "rain", "drought"]


def test_allocation_order_cap_desc_take_all_first_stable_on_declared_order():
    spec = base(rank=[{"field": "cited_by"}],
                quotas={"target": 20, "allocation_order": "cap_desc",
                        "declared": [{"topic": "frost", "cap": 2}, {"topic": "drought", "cap": "take_all"},
                                     {"topic": "rain", "cap": 2}]})
    rows = _pool(("10.1/r1", "Rainfall"), ("10.1/r2", "Rainfall"), ("10.1/f1", "Frost"), ("10.1/d1", "Drought"))
    assert run_core(spec, rows).plan.allocation == ["drought", "frost", "rain"]


# ---------------------------------------------------------------- plan: fill_order_claim + equal_share
def _claim_spec(target=10, tier=None, order=None):
    q = {"target": target, "assign": "fill_order_claim", "caps": "equal_share"}
    if tier:
        q["tier_take_all"] = {"min_seeds": tier}
    return base(quotas=q, rank=[{"field": "n_seeds"}, {"field": "cited_by"}], order=order or {"method": "rank"})


def test_equal_share_caps_scarcest_first_with_carry_forward():
    # avail: frost 1, drought 5, rain 9 (spec order rain, drought, frost) -> caps frost 1, drought 4, rain 5
    rows = [row(f"10.1/f{i}", "Frost", cited=str(900 - i)) for i in range(1)]
    rows += [row(f"10.1/d{i}", "Drought", cited=str(800 - i)) for i in range(5)]
    rows += [row(f"10.1/r{i}", "Rainfall", cited=str(700 - i)) for i in range(9)] + [OFF]
    res = run_core(_claim_spec(target=10), rows)
    assert res.plan.caps == {"frost": 1, "drought": 4, "rain": 5}
    assert res.plan.allocation == ["frost", "drought", "rain"] and len(res.plan.order) == 10
    assert res.plan.reasons["10.1/d4"] == "quota_full:drought"


def test_claimed_rows_are_skipped_and_do_not_use_up_a_later_cap():
    rows = [row("10.1/x", "Frost and rainfall", cited="99"), row("10.1/r1", "Rainfall", cited="50"),
            row("10.1/r2", "Rainfall", cited="40"), row("10.1/r3", "Rainfall", cited="30"),
            row("10.1/u", "Drought at a dry site", cited="20"), OFF]
    res = run_core(base(require=[{"name": "weather", "phrases": ["frost", "rainfall", "drought"]}], quotas={
        "target": 3, "assign": "fill_order_claim", "caps": "equal_share"}, order={"method": "rank"},
        rank=[{"field": "cited_by"}], controls={"match_none": ["Snow"], "contentless": False}), rows)
    # O: drought(1), frost(1), rain(4); caps 1, 1, 1; frost claims x; rain's walk skips x and takes r1
    assert res.plan.assigned == {"10.1/u": "drought", "10.1/x": "frost", "10.1/r1": "rain"}
    assert res.plan.reasons["10.1/r2"] == "quota_full:rain"


def test_tier_takes_all_and_rows_below_with_no_topic_get_no_quota_topic():
    rows = [row("10.1/t1", "Field notes", seed="s1"), row("10.1/t1", "Field notes", seed="s2"),
            row("10.1/r1", "Rainfall", cited="5"), row("10.1/n1", "Field notes only", cited="9"), OFF]
    spec = _claim_spec(target=5, tier=2)
    spec["require"] = [{"name": "field", "substrings": ["field", "rainfall"]}]
    res = run_core(spec, rows)
    assert keys(res.plan.tier) == ["10.1/t1"] and res.plan.assigned["10.1/t1"] == gate.TIER_BUCKET
    assert res.plan.reasons == {"10.1/n1": "no_quota_topic"}
    assert keys(res.plan.order) == ["10.1/t1", "10.1/r1"]


def test_a_tier_above_the_target_is_an_impossible_trim():
    rows = []
    for i in range(3):
        rows += [row(f"10.1/t{i}", "Rainfall", seed="s1"), row(f"10.1/t{i}", "Rainfall", seed="s2")]
    with pytest.raises(GateAbort) as e:
        run_core(_claim_spec(target=2, tier=2), rows + [OFF])
    assert e.value.reasons == ["trim:impossible"]


def test_rank_order_keeps_allocation_order_inside_ties():
    """Rank mode is a stable sort of the allocation order by the rank keys ONLY: tied rows keep claim
    order (frost claims before rain), never first appearance."""
    rows = [row("10.1/r_first", "Rainfall", cited="5"), row("10.1/f_later", "Frost", cited="5"),
            row("10.1/r2", "Rainfall", cited="4"), OFF]
    res = run_core(_claim_spec(target=10), rows)
    assert res.plan.allocation.index("frost") < res.plan.allocation.index("rain")
    assert keys(res.plan.order) == ["10.1/f_later", "10.1/r_first", "10.1/r2"]


# ---------------------------------------------------------------- trim
def _trim_rows(n_take_all, sizes):
    """Take-all topic 'frost' rows plus capped buckets rain/drought with given sizes, ranked by cited."""
    rows, c = [], 10000
    for i in range(n_take_all):
        rows.append(row(f"10.1/f{i}", "Frost", cited=str(c)))
        c -= 1
    for name, title, n in (("r", "Rainfall", sizes[0]), ("d", "Drought", sizes[1])):
        for i in range(n):
            rows.append(row(f"10.1/{name}{i}", title, cited=str(c)))
            c -= 1
    return rows + [OFF]


def _trim_spec(target, alloc="bucket_size_desc"):
    return base(rank=[{"field": "cited_by"}], quotas={"target": target, "allocation_order": alloc, "declared": [
        {"topic": "frost", "cap": "take_all"}, {"topic": "rain", "cap": 100}, {"topic": "drought", "cap": 100}]})


def test_trim_rounds_half_to_even_then_drops_from_the_largest():
    # 2 take-all + rain 4 + drought 4 = 10, target 5: overflow 5, shares 5*4/8 = 2.5 -> 2 each (half-even)
    # keep 2 + 2, still one over: drop the last row of the largest (tie: first in allocation order, rain)
    res = run_core(_trim_spec(5, "cap_desc"), _trim_rows(2, (4, 4)))
    taken = {t: res.plan.table[t]["taken"] for t in ("frost", "rain", "drought")}
    assert taken == {"frost": 2, "rain": 1, "drought": 2}
    assert res.plan.trim["dropped_largest"] == 1 and res.plan.trim["restored"] == 0
    assert res.plan.reasons["10.1/r1"] == "trimmed:rain" and res.plan.table["rain"]["trimmed"] == 3


def test_trim_top_up_restores_an_undershoot():
    # rain 3, drought 3, take-all 0; target 4: overflow 2, shares round(1) each -> 2 + 2 = 4 (exact)
    exact = run_core(_trim_spec(4), _trim_rows(0, (3, 3)))
    assert len(exact.plan.order) == 4 and exact.plan.trim["restored"] == 0
    # rain 3, drought 1 (+ 1 take-all), target 3: overflow 2 over capped 4: rain round(1.5)=2, drought round(0.5)=0
    under = run_core(_trim_spec(3), _trim_rows(1, (3, 1)))
    assert len(under.plan.order) == 3
    assert under.plan.trim["restored"] == 0 and under.plan.trim["dropped_largest"] == 0


def test_trim_undershoot_is_topped_up_from_the_bucket_that_lost_most():
    # rain 5, drought 5, target 3 with no take-all: over 7, shares round(3.5)=4 each -> keep 1 + 1 = 2 < 3
    res = run_core(_trim_spec(3), _trim_rows(0, (5, 5)))
    assert len(res.plan.order) == 3 and res.plan.trim["restored"] == 1
    assert res.plan.table["rain"]["taken"] == 2 and res.plan.table["drought"]["taken"] == 1
    assert "10.1/r1" not in res.plan.reasons and res.plan.reasons["10.1/r2"] == "trimmed:rain"


def test_trim_never_cuts_take_all_rows_and_aborts_when_it_must():
    res = run_core(_trim_spec(4), _trim_rows(3, (5, 5)))
    assert {k for k in keys(res.plan.order) if k.startswith("10.1/f")} == {"10.1/f0", "10.1/f1", "10.1/f2"}
    with pytest.raises(GateAbort, match="impossible trim"):
        run_core(_trim_spec(2), _trim_rows(3, (1, 1)))


# ---------------------------------------------------------------- order
def _order_rows():
    return _pool(("10.1/r1", "Rainfall"), ("10.1/r2", "Rainfall"), ("10.1/r3", "Rainfall"),
                 ("10.1/d1", "Drought"), ("10.1/d2", "Drought"), ("10.1/f1", "Frost"))


def test_round_robin_size_asc_and_size_desc():
    spec = base(rank=[{"field": "cited_by"}], order={"method": "round_robin", "bucket_order": "size_asc"})
    asc = run_core(spec, _order_rows())
    assert keys(asc.plan.order) == ["10.1/f1", "10.1/d1", "10.1/r1", "10.1/d2", "10.1/r2", "10.1/r3"]
    spec["order"]["bucket_order"] = "size_desc"
    desc = run_core(spec, _order_rows())
    assert keys(desc.plan.order) == ["10.1/r1", "10.1/d1", "10.1/f1", "10.1/r2", "10.1/d2", "10.1/r3"]


def test_round_robin_ties_keep_allocation_order():
    spec = base(rank=[{"field": "cited_by"}], quotas={"target": 10, "allocation_order": "cap_desc", "declared": [
        {"topic": "frost", "cap": 1}, {"topic": "rain", "cap": 3}, {"topic": "drought", "cap": 1}]})
    res = run_core(spec, _order_rows())
    assert res.plan.allocation == ["rain", "frost", "drought"]
    assert keys(res.plan.order)[:3] == ["10.1/f1", "10.1/d1", "10.1/r1"]


def test_rank_order_method():
    spec = base(rank=[{"field": "cited_by"}], order={"method": "rank"})
    assert keys(run_core(spec, _order_rows()).plan.order) == \
        ["10.1/r1", "10.1/r2", "10.1/r3", "10.1/d1", "10.1/d2", "10.1/f1"]


# ---------------------------------------------------------------- lanes
def test_lane_with_its_own_years_rank_and_exclusions():
    spec = base(years={"min": 2015, "scope": "row"}, rank=[{"field": "cited_by"}],
                quotas={"target": 10, "declared": [{"topic": "rain", "cap": 1}, {"topic": "drought", "cap": 1},
                                                   {"topic": "frost", "cap": 1}]},
                lanes=[{"name": "older", "topics": ["frost", "rain"], "size": 2,
                        "years": {"min": 2000, "max": 2014, "scope": "row"},
                        "rank": [{"field": "year", "order": "desc"}]},
                       {"name": "recent", "topics": ["rain"], "size": 5}])
    rows = [row("10.1/new.r1", "Rainfall", year="2020", cited="9"), row("10.1/new.r2", "Rainfall", year="2021", cited="8"),
            row("10.1/new.r3", "Rainfall", year="2022", cited="7"), row("10.1/drawn.r", "Rainfall", year="2022", cited="6"),
            row("10.1/old.f1", "Frost", year="2005"), row("10.1/old.r1", "Rainfall", year="2012"),
            row("10.1/old.r2", "Rainfall", year="2009"), row("10.1/old.d1", "Drought", year="2013"), OFF]
    res = run_core(spec, rows, drawn={"10.1/drawn.r"})
    lanes = dict(res.lanes)
    assert keys(res.plan.order) == ["10.1/new.r1"]
    assert keys(lanes["older"]) == ["10.1/old.r1", "10.1/old.r2"]
    assert keys(lanes["recent"]) == ["10.1/new.r2", "10.1/new.r3"]
    assert res.lane_info["older"]["candidates"] == 3 and res.lane_info["recent"]["candidates"] == 2


def test_lane_excludes_earlier_lanes_and_warns_when_empty():
    spec = base(rank=[{"field": "cited_by"}], quotas={"target": 10, "declared": [
        {"topic": "rain", "cap": 1}, {"topic": "drought", "cap": 0}, {"topic": "frost", "cap": 0}]},
        lanes=[{"name": "one", "topics": ["rain"], "size": 1}, {"name": "two", "topics": ["rain"], "size": 1},
               {"name": "three", "topics": ["frost"], "size": 1}])
    res = run_core(spec, _pool(("10.1/r1", "Rainfall"), ("10.1/r2", "Rainfall"), ("10.1/r3", "Rainfall"),
                               ("10.1/d1", "Drought")))
    lanes = dict(res.lanes)
    assert keys(lanes["one"]) == ["10.1/r2"] and keys(lanes["two"]) == ["10.1/r3"] and lanes["three"] == []
    assert any("lane three" in w for w in res.warnings)


def test_lane_years_window_with_no_candidates_is_refused():
    spec = base(lanes=[{"name": "older", "topics": ["rain"], "size": 1, "years": {"min": 1990, "max": 1999,
                                                                                 "scope": "row"}}])
    with pytest.raises(GateAbort) as e:
        run_core(spec, [row("10.1/a", "Rainfall", year="2020"), OFF])
    assert e.value.reasons == ["zero_input:records"]
    spec["lanes"][0]["years"] = {"min": 1990, "max": 1999, "scope": "record"}
    with pytest.raises(GateAbort, match="lane older"):
        run_core(spec, [row("10.1/a", "Rainfall", year="2020"), OFF])


# ---------------------------------------------------------------- invariants
def _plan_for_invariants():
    spec = load(base(rank=[{"field": "cited_by"}], quotas={"target": 3, "declared": [
        {"topic": "rain", "cap": "take_all"}, {"topic": "drought", "cap": 2}, {"topic": "frost", "cap": 1}]}))
    res = gate.gate(spec, _pool(("10.1/r1", "Rainfall"), ("10.1/d1", "Drought"), ("10.1/d2", "Drought"),
                                ("10.1/f1", "Frost")))
    return spec, res


def test_invariants_pass_and_list_their_checks():
    _, res = _plan_for_invariants()
    assert res.checks == ["permutation", "take_all_and_tier_present", "taken_le_cap", "selection_le_target",
                          "distinct_spec_keys", "distinct_holdings_keys"] and res.collisions == []


@pytest.mark.parametrize("breaker, msg", [
    (lambda pl: pl.order.pop(), "not a permutation"),
    (lambda pl: (pl.order.remove(pl.protected[0]), pl.alloc_rows.remove(pl.protected[0])), "take-all or tier"),
    (lambda pl: pl.caps.__setitem__("drought", 0), "above cap"),
    (lambda pl: pl.caps.__setitem__("frost", 0), "above cap"),
])
def test_each_invariant_failure_refuses(breaker, msg):
    spec, res = _plan_for_invariants()
    breaker(res.plan)
    with pytest.raises(GateAbort, match=msg) as e:
        gate.check_invariants(spec, res.plan, [])
    assert e.value.exit_code == 2


def test_invariant_selection_above_target_and_duplicate_keys():
    spec, res = _plan_for_invariants()
    spec.eff["quotas"]["target"] = 2
    with pytest.raises(GateAbort, match="above target"):
        gate.check_invariants(spec, res.plan, [])
    spec, res = _plan_for_invariants()
    with pytest.raises(GateAbort, match="appears twice"):
        gate.check_invariants(spec, res.plan, [("lane", [res.plan.order[0]])])


def test_holdings_key_collision_keeps_the_first_row_and_logs_it():
    rows = _pool(("10.5555/x.1", "Rainfall one"), ("10.5555/x.1.", "Rainfall two"), ("10.5555/y", "Rainfall three"))
    res = run_core(base(rank=[{"field": "cited_by"}]), rows)
    assert keys(res.plan.order) == ["10.5555/x.1", "10.5555/y"]
    assert res.collisions == [{"kept": "10.5555/x.1", "kept_in": "main", "dropped": "10.5555/x.1.",
                               "dropped_from": "main", "holdings_key": "10.5555/x.1"}]
    assert res.pool_collisions == [{"holdings_key": "10.5555/x.1", "keys": ["10.5555/x.1", "10.5555/x.1."]}]
    out = gate.output_rows(res)
    assert [d for d in out["drops"] if d["stage"] == "invariant"][0]["primary_reason"] == "pool_key_collision"


# ---------------------------------------------------------------- annotate, direction split, report, outputs
def test_annotate_and_direction_split_and_warning():
    spec = base(rank=[{"field": "cited_by"}], annotate=[{"column": "in_list", "dois_from": "x.csv"}],
                reports={"direction_split": [{"topic": "drought", "sides": [
                    {"name": "harms", "words": ["stress", "loss"]}, {"name": "helps", "words": ["tolerance"]}]}]})
    rows = _pool(("10.1/d1", "Drought stress and loss"), ("10.1/d2", "Drought tolerance under stress"),
                 ("10.1/d3", "Drought stress"), ("10.1/r1", "Rainfall"))
    res = run_core(spec, rows, annotations={"in_list": {"10.1/d1", "10.1/r1"}})
    rep = gate.build_report(res)
    assert rep["annotations"]["in_list"] == {"records": 2, "candidates": 2, "selected": 2}
    ds = rep["direction_splits"][0]
    assert ds["counts"] == {"harms": 2, "helps": 0, "unstated": 1} and ds["empty_sides"] == ["helps"]
    assert any("direction split drought" in w for w in rep["warnings"])
    sel = gate.output_rows(res)["selection"]
    assert {r["doi"]: r["in_list"] for r in sel}["10.1/d1"] == "y"


def test_selection_rows_are_pool_csvs_with_notes():
    spec = base(rank=[{"field": "cited_by"}], topics=[
        {"name": "rain", "substrings": ["rainfall"], "chapter": "U1", "controls": {"hit": ["Rainfall"]}},
        {"name": "drought", "phrases": ["drought"]}],
        quotas={"target": 5, "declared": [{"topic": "rain", "cap": 5}, {"topic": "drought", "cap": 5}]},
        output={"notes_template": "{gate}|{lane}|{assigned_topic}|{topic_chapter}|{order}|{n_seeds:02d}"})
    res = run_core(spec, [row("10.1/a", "Rainfall", chapter="C2", cited="3"), OFF])
    sel = gate.output_rows(res)["selection"]
    assert list(sel[0])[:2] == ["doi", "order"] and sel[0]["notes"] == "teaching_demo|main|rain|U1|1|01"
    assert sel[0]["chapters"] == "C2" and sel[0]["lane"] == "main"


def test_report_shares_baseline_take_all_share_and_tiers():
    spec = base(rank=[{"field": "cited_by"}], chapter_budgets={"U1": 2},
                quotas={"target": 2, "declared": [{"topic": "rain", "cap": "take_all"}, {"topic": "drought", "cap": 1},
                                                  {"topic": "frost", "cap": 1}]})
    rows = _pool(("10.1/r1", "Rainfall"), ("10.1/d1", "Drought"), ("10.1/f1", "Frost"))
    res = run_core(spec, rows)
    rep = gate.build_report(res)
    assert rep["take_all_share"]["rows"] == 1
    assert rep["shares"]["baseline"]["frost"]["rows"] == 0 and rep["shares"]["baseline"]["rain"]["rows"] == 1
    assert rep["tiers"]["candidates"]["n"] == 3 and rep["chapters"]["budgets"]["U1"]["short"] == 2
    assert gate.render_markdown(rep).startswith("# Gate report: teaching_demo")


def test_infix_lint_counts_tags_carried_only_by_mid_word_hits():
    spec = base(topics=[{"name": "soil", "substrings": ["soil moisture"], "controls": {"hit": ["x soil moisture"]}},
                        {"name": "rain", "substrings": ["rainfall"]}],
                quotas={"target": 5, "declared": [{"topic": "soil", "cap": 5}, {"topic": "rain", "cap": 5}]})
    res = run_core(spec, [row("10.1/a", "Topsoil moisture in orchards"), row("10.1/b", "Soil moisture sensors"),
                          OFF], command="build")
    assert gate.build_report(res)["lint"]["infix_only"]["soil"]["rows"] == 1


def test_plan_from_a_recorded_pool():
    spec = load(base(rank=[{"field": "cited_by"}]))
    pool = [{"doi": "10.1/b", "n_seeds": "2", "topics": "rain", "cited_by": "1"},
            {"doi": "10.1/a", "n_seeds": "1", "topics": "drought;frost", "cited_by": "9"},
            {"doi": "10.1/c", "n_seeds": "1", "topics": "", "cited_by": "0"}]
    res = gate.gate(spec, pool_rows=pool)
    assert keys(res.candidates) == ["10.1/b", "10.1/a", "10.1/c"] and res.from_pool
    assert res.plan.reasons == {"10.1/c": "no_quota_topic"} and res.records["10.1/b"].n_seeds == 2
    with pytest.raises(gate.GateError, match="does not define: snow"):
        gate.gate(spec, pool_rows=[{"doi": "10.1/x", "n_seeds": "1", "topics": "snow"}])
    with pytest.raises(GateAbort, match="the pool has 0 rows"):
        gate.gate(spec, pool_rows=[])
