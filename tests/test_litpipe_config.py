"""litpipe.config: projects.json accessors for db_dir, state_dir, sources and the client blocks (W0).

Every test passes a temp registry dict or points CONFIG_PATH at a temp file; none reads the live
projects.json, and the default state_dir is only computed (create=False), never created.
"""
import json
from pathlib import Path

import pytest

import lit_util
from litpipe import config
from litpipe.config import ConfigError


def reg(**top):
    projects = top.pop("projects", {"K": {"tier": 2, "lib_dir": "lit"}})
    return {"projects": projects, **top}


# ---------------------------------------------------------------- db_dir
def test_db_dir_defaults_to_references_under_the_projects_root(tmp_path, monkeypatch):
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", tmp_path)
    assert config.db_dir(reg()) == tmp_path / "_references"


def test_db_dir_absolute_verbatim_relative_to_root_tilde_to_home(tmp_path, monkeypatch):
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", tmp_path / "root")
    assert config.db_dir(reg(db_dir=str(tmp_path / "db"))) == tmp_path / "db"
    assert config.db_dir(reg(db_dir="shared/db")) == tmp_path / "root" / "shared" / "db"
    assert config.db_dir(reg(db_dir="~/dbs")) == Path.home() / "dbs"


@pytest.mark.parametrize("bad", [7, "", "   ", ["a"]])
def test_db_dir_rejects_non_path_values(bad):
    with pytest.raises(ConfigError):
        config.db_dir(reg(db_dir=bad))


# ---------------------------------------------------------------- state_dir
def test_state_dir_default_is_off_the_mirror_and_not_created_when_asked(tmp_path, monkeypatch):
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", tmp_path)
    d = config.state_dir(reg(), create=False)
    assert d == Path.home() / ".local" / "db" / "literature_pipeline"
    assert tmp_path not in d.parents


def test_state_dir_is_created_on_first_use(tmp_path):
    target = tmp_path / "state" / "deep"
    assert config.state_dir(reg(state_dir=str(target))) == target and target.is_dir()


def test_state_dir_relative_anchors_to_home_never_to_the_projects_root(tmp_path, monkeypatch):
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", tmp_path)
    assert config.state_dir(reg(state_dir="lp_state"), create=False) == Path.home() / "lp_state"


def test_state_dir_rejects_non_path_values():
    with pytest.raises(ConfigError):
        config.state_dir(reg(state_dir=3), create=False)


# ---------------------------------------------------------------- sources
def test_sources_default_is_unpaywall_and_pmc_only():
    assert config.sources("K", cfg=reg()) == {"unpaywall", "pmc"}


def test_project_sources_replace_the_default():
    r = reg(projects={"research_a": {"lib_dir": "x", "sources": ["unpaywall", "pmc", "arxiv", "osf"]}})
    assert config.sources("research_a", cfg=r) == {"unpaywall", "pmc", "arxiv", "osf"}


def test_override_replaces_both_and_accepts_a_cli_string():
    r = reg(projects={"research_a": {"lib_dir": "x", "sources": ["arxiv"]}})
    assert config.sources("research_a", override=["pmc"], cfg=r) == {"pmc"}
    assert config.sources("research_a", override="unpaywall, PMC", cfg=r) == {"unpaywall", "pmc"}


def test_empty_sources_list_is_honoured():
    assert config.sources("K", cfg=reg(projects={"K": {"lib_dir": "x", "sources": []}})) == set()


def test_default_set_is_not_shared_state():
    s = config.sources("K", cfg=reg())
    s.add("arxiv")
    assert config.sources("K", cfg=reg()) == {"unpaywall", "pmc"}


@pytest.mark.parametrize("bad", [["unpaywall", "scihub"], ["europe_pmc"], {"a": 1}, 5])
def test_unknown_or_malformed_sources_raise(bad):
    with pytest.raises(ConfigError):
        config.sources("K", cfg=reg(projects={"K": {"lib_dir": "x", "sources": bad}}))


def test_unknown_override_raises():
    with pytest.raises(ConfigError, match="scihub"):
        config.sources("K", override="pmc,scihub", cfg=reg())


def test_every_allowed_source_is_accepted():
    r = reg(projects={"K": {"lib_dir": "x", "sources": sorted(config.ALLOWED_SOURCES)}})
    assert config.sources("K", cfg=r) == set(config.ALLOWED_SOURCES)
    assert len(config.ALLOWED_SOURCES) == 9


def test_unregistered_project_raises():
    with pytest.raises(ConfigError, match="not registered"):
        config.sources("nope", cfg=reg())


# ---------------------------------------------------------------- auto_stage, walk cadence
def test_auto_stage_defaults_false_and_must_be_a_bool():
    assert config.auto_stage("K", cfg=reg()) is False
    assert config.auto_stage("K", cfg=reg(projects={"K": {"auto_stage": True}})) is True
    with pytest.raises(ConfigError):
        config.auto_stage("K", cfg=reg(projects={"K": {"auto_stage": "yes"}}))


def test_walk_cadence_days():
    assert config.walk_cadence_days("K", cfg=reg()) is None
    assert config.walk_cadence_days("K", cfg=reg(projects={"K": {"walk_cadence_days": 7}})) == 7
    for bad in (0, -3, True, "7", 1.5):
        with pytest.raises(ConfigError):
            config.walk_cadence_days("K", cfg=reg(projects={"K": {"walk_cadence_days": bad}}))


# ---------------------------------------------------------------- hosts, s2, openalex
def test_host_switches_default_off_and_overrides_merge():
    assert config.hosts(reg()) == {"arxiv_pdf_allowed": False, "biorxiv_pdf_allowed": False}
    h = config.hosts(reg(hosts={"arxiv_pdf_allowed": True, "extra": {"x": 1}}))
    assert h == {"arxiv_pdf_allowed": True, "biorxiv_pdf_allowed": False, "extra": {"x": 1}}


def test_host_switch_must_be_a_bool():
    with pytest.raises(ConfigError):
        config.hosts(reg(hosts={"biorxiv_pdf_allowed": "true"}))


def test_client_blocks_default_empty_and_are_copies():
    r = reg(s2={"daily_budget": 500})
    got = config.s2(r)
    got["daily_budget"] = 1
    assert config.s2(r) == {"daily_budget": 500}
    assert config.openalex(reg()) == {}
    with pytest.raises(ConfigError):
        config.openalex(reg(openalex=["key"]))


# ---------------------------------------------------------------- the file path
def test_reads_config_path_when_no_dict_is_passed(tmp_path, monkeypatch):
    f = tmp_path / "projects.json"
    f.write_text(json.dumps(reg(projects={"P": {"lib_dir": "x", "sources": ["osf"]}})),
                 encoding="utf-8")
    monkeypatch.setattr(config, "CONFIG_PATH", f)
    assert config.sources("P") == {"osf"}


def test_missing_projects_json_is_an_empty_registry(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "CONFIG_PATH", tmp_path / "absent.json")
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", tmp_path)
    assert config.db_dir() == tmp_path / "_references"
    with pytest.raises(ConfigError):
        config.sources("any")


def test_config_path_is_the_repo_registry():
    assert config.CONFIG_PATH == Path(lit_util.__file__).resolve().parent / "projects.json"
