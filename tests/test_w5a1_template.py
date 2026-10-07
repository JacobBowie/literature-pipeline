"""W5-A1: projects.json.template documents every registry key the pipeline reads, parses as JSON,
and its example values pass the pipeline's own validators.

The key list is EXPLICIT on purpose: litpipe/config.py alone does not read the runner's, the
clients' or the library tools' keys. It holds the keys litpipe/config.py, litpipe/runner.py,
lit_util.py, sweep.py, pipeline_check.py, litpipe/s2.py and litpipe/openalex.py read at 53ca3e6,
plus the keys W5-C1 and W5-C3 add (artifact_dir, the runner's lock_stale_s, timeouts,
schedule_time and ris_limit, ezproxy_host, portfolio_dir)."""
import json
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
TEMPLATE = REPO / "projects.json.template"

CONFIG_PY = ["db_dir", "state_dir", "hosts", "hosts.arxiv_pdf_allowed", "hosts.biorxiv_pdf_allowed",
             "s2", "openalex", "projects", "projects.<key>.sources", "projects.<key>.auto_stage",
             "projects.<key>.walk_cadence_days"]
RUNNER_PY = ["runner", "runner.unattended_db_writes", "runner.candidate_order", "loose_ends",
             "projects.<key>.active", "projects.<key>.lib_dir", "projects.<key>.parent"]
REGISTRY = ["root", "projects.<key>.tier", "projects.<key>.data_dir", "projects.<key>.ris_threshold"]
CLIENTS = ["s2.spacing_s", "s2.spacing_keyed_s", "s2.max_requests_per_run", "s2.breaker",
           "openalex.max_requests_per_run", "openalex.breaker", "openalex.content_max_per_run"]
CONCURRENT = ["artifact_dir", "projects.<key>.artifact_dir", "runner.lock_stale_s", "runner.timeouts",
              "runner.schedule_time", "runner.ris_limit", "ezproxy_host", "portfolio_dir"]
ALL_KEYS = CONFIG_PY + RUNNER_PY + REGISTRY + CLIENTS + CONCURRENT


def _no_duplicates(pairs):
    keys = [k for k, _ in pairs]
    dup = {k for k in keys if keys.count(k) > 1}
    assert not dup, f"duplicate keys {dup}"
    return dict(pairs)


@pytest.fixture(scope="module")
def tpl():
    return json.loads(TEMPLATE.read_text(encoding="utf-8"), object_pairs_hook=_no_duplicates)


def test_the_key_list_has_no_repeats():
    assert len(ALL_KEYS) == len(set(ALL_KEYS))


@pytest.mark.parametrize("key", ALL_KEYS)
def test_every_key_is_documented(tpl, key):
    doc = tpl["_schema"].get(key)
    assert isinstance(doc, str) and len(doc) > 20, f"{key} is not documented in _schema"


def test_schema_documents_no_key_the_pipeline_does_not_read(tpl):
    extra = set(tpl["_schema"]) - set(ALL_KEYS) - {"_reserved"}
    assert extra == set()


def _body_keys(tpl):
    out = set()
    for k, v in tpl.items():
        if k.startswith("_"):
            continue
        out.add(k)
        if k == "projects":
            for entry in v.values():
                out |= {f"projects.<key>.{kk}" for kk in entry}
        elif isinstance(v, dict):
            out |= {f"{k}.{kk}" for kk in v}
    return out


def test_every_example_value_is_documented(tpl):
    assert _body_keys(tpl) - set(tpl["_schema"]) == set()


def test_example_projects_use_neutral_keys(tpl):
    assert tpl["projects"]
    assert all(re.fullmatch(r"my-[a-z]+", k) for k in tpl["projects"])


def test_the_example_registry_passes_the_pipeline_validators(tpl, tmp_path, monkeypatch):
    import lit_util
    from litpipe import config, openalex, runner, s2
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", tmp_path / "root")
    assert config.db_dir(tpl) == tmp_path / "root" / "_references"          # the documented default
    assert config.state_dir(tpl, create=False) == Path.home() / ".local" / "db" / "literature_pipeline"
    assert config.hosts(tpl) == config.HOST_SWITCH_DEFAULTS
    for key in tpl["projects"]:
        config.sources(key, cfg=tpl)
        config.auto_stage(key, cfg=tpl)
        config.walk_cadence_days(key, cfg=tpl)
    assert runner.runner_block(tpl)["unattended_db_writes"] is False
    assert s2.settings(tpl) == s2.Settings()                                 # the code defaults
    assert openalex.settings(tpl) == openalex.Settings()
    assert lit_util._resolve_projects_root(tpl) == Path.home() / "Projects"


def test_documented_client_defaults_match_the_code(tpl):
    from litpipe import openalex, s2
    sch = tpl["_schema"]
    for key, value in [("s2.spacing_s", s2.Settings().spacing_s),
                       ("s2.spacing_keyed_s", s2.Settings().spacing_keyed_s),
                       ("s2.max_requests_per_run", s2.Settings().max_requests_per_run),
                       ("s2.breaker", s2.Settings().breaker),
                       ("openalex.max_requests_per_run", openalex.Settings().max_requests_per_run),
                       ("openalex.breaker", openalex.Settings().breaker),
                       ("openalex.content_max_per_run", openalex.Settings().content_max_per_run)]:
        m = re.match(r"Default ([0-9.]+)", sch[key])
        assert m and float(m.group(1)) == float(value), key


def test_documented_runner_timeouts_match_the_code(tpl):
    from litpipe import runner
    doc = tpl["_schema"]["runner.timeouts"]
    assert runner.TIMEOUTS["sweep"] == 4 * 3600 and "sweep 4 h" in doc
    assert runner.TIMEOUTS["walk"] == runner.TIMEOUTS["reverse"] == 6 * 3600 and "walk and reverse 6 h" in doc
    assert runner.TIMEOUTS["index"] == 3600 and "index 1 h" in doc
    assert runner.TIMEOUTS["abstracts"] == runner.TIMEOUTS["recommendations"] == 3 * 3600
    assert runner.DEFAULT_TIMEOUT_S == 1800 and "anything else 30 min" in doc


def test_documented_source_names_are_the_allowed_ones(tpl):
    from litpipe import config
    doc = tpl["_schema"]["projects.<key>.sources"]
    for name in config.ALLOWED_SOURCES:
        assert re.search(rf"\b{name}\b", doc), name
    assert sorted(json.loads(re.search(r"Default (\[.*?\])", doc).group(1))) == sorted(config.DEFAULT_SOURCES)
