"""Pipeline configuration read from projects.json (dispatch 0.5; config-only, no new env vars).

Top-level keys (all optional):
  db_dir     where portfolio.duckdb lives. Default <root>/_references. '~' expands; an absolute
             path is used verbatim; a bare-relative path is anchored to the projects root, like
             the default.
  state_dir  the pipeline's own write-heavy state: litpipe_state.sqlite, s2_cache.duckdb,
             ledger/, source caches. Default ~/.local/db/literature_pipeline, on a local disk
             (SQLite WAL needs one; a synced folder must not hold it). A bare-relative path is
             anchored to HOME, never to the projects root, so it cannot land in a synced
             projects tree by accident.
  hosts      overrides of the host policy table; the switches default off
             ({"arxiv_pdf_allowed": false, "biorxiv_pdf_allowed": false}).
  s2, openalex   client blocks (budget, spacing), passed through as dicts.
  artifact_dir   where sweep's run artifacts go (artifact_dir() below); unset: each project root.
  runner     the scheduled runner's block (litpipe.runner.runner_block validates it).
  portfolio_dir  the folder for portfolio-level worklists, read by
             build_priority_paywall_queue.portfolio_dir() and paywall_pull; no default (those
             tools resolve it themselves; no accessor here).
  ezproxy_host   the library proxy host read by `paywall_pull --access ezproxy`; no default
             (resolved by paywall_pull itself; no accessor here).

Per-project keys (under "projects": {key: {...}}):
  sources            list of fetch sources; replaces the default {"unpaywall", "pmc"}.
  auto_stage         bool, default false: may the runner stage top_candidates drafts itself.
  walk_cadence_days  positive int, or absent for no scheduled walk.
  artifact_dir       this project's artifact directory; wins over the top-level key.

Every accessor takes an optional `cfg` dict (a loaded projects.json) so tests pass a temp
registry; without it the file at CONFIG_PATH is read on each call. Tests may also monkeypatch
CONFIG_PATH by attribute. A value of the wrong type or an unknown source raises ConfigError
(the CONFIG outcome: the run aborts rather than guessing).
"""
import os
from pathlib import Path

import lit_util

CONFIG_PATH = Path(__file__).resolve().parent.parent / "projects.json"

ALLOWED_SOURCES = frozenset({
    "unpaywall", "pmc", "europepmc_preprints", "biorxiv", "medrxiv", "osf", "sportrxiv",
    "arxiv", "openalex_content",
})
DEFAULT_SOURCES = frozenset({"unpaywall", "pmc"})
HOST_SWITCH_DEFAULTS = {"arxiv_pdf_allowed": False, "biorxiv_pdf_allowed": False}


class ConfigError(ValueError):
    """projects.json holds a value the pipeline cannot use."""


def load(cfg=None):
    """The loaded projects.json dict: `cfg` when given, else CONFIG_PATH ({} when absent)."""
    if cfg is not None:
        return cfg
    return lit_util.load_projects_config(CONFIG_PATH, missing_ok=True)


def _path(raw, anchor):
    p = Path(raw).expanduser()
    return p if p.is_absolute() else anchor / p


def db_dir(cfg=None) -> Path:
    raw = load(cfg).get("db_dir")
    if raw is not None and (not isinstance(raw, str) or not raw.strip()):
        raise ConfigError(f"db_dir must be a non-empty path string, got {raw!r}")
    # PROJECTS_ROOT by attribute at call time, so a test's monkeypatch of the anchor applies.
    return _path(raw, lit_util.PROJECTS_ROOT) if raw else lit_util.PROJECTS_ROOT / "_references"


def state_dir(cfg=None, create=True) -> Path:
    """The state directory, created on first use unless create=False."""
    raw = load(cfg).get("state_dir")
    if raw is not None and (not isinstance(raw, str) or not raw.strip()):
        raise ConfigError(f"state_dir must be a non-empty path string, got {raw!r}")
    d = _path(raw, Path.home()) if raw else Path.home() / ".local" / "db" / "literature_pipeline"
    if create:
        d.mkdir(parents=True, exist_ok=True)
    return d


def _project(key, cfg):
    projects = load(cfg).get("projects") or {}
    if key not in projects:
        raise ConfigError(f"project {key!r} is not registered in projects.json")
    return projects[key] or {}


def _source_set(values, where):
    if isinstance(values, str):  # a --sources CLI value; never iterated character by character
        values = values.split(",")
    if not isinstance(values, (list, tuple, set, frozenset)):
        raise ConfigError(f"{where} must be a list of source names, got {values!r}")
    out = {str(v).strip().lower() for v in values if str(v).strip()}
    unknown = out - ALLOWED_SOURCES
    if unknown:
        raise ConfigError(f"{where}: unknown source(s) {sorted(unknown)}; "
                          f"allowed: {sorted(ALLOWED_SOURCES)}")
    return out


def sources(project_key, override=None, cfg=None) -> set[str]:
    """The fetch sources for a project. `override` (a --sources value: a list or a
    comma-separated string) replaces everything; otherwise the project's own `sources` list
    replaces the default {"unpaywall", "pmc"}. An empty list is honoured (fetch nothing)."""
    if override is not None:
        return _source_set(override, "--sources")
    p = _project(project_key, cfg)
    if "sources" not in p:
        return set(DEFAULT_SOURCES)
    return _source_set(p["sources"], f"projects[{project_key!r}].sources")


def auto_stage(project_key, cfg=None) -> bool:
    v = _project(project_key, cfg).get("auto_stage", False)
    if not isinstance(v, bool):
        raise ConfigError(f"projects[{project_key!r}].auto_stage must be true or false, got {v!r}")
    return v


def walk_cadence_days(project_key, cfg=None) -> int | None:
    v = _project(project_key, cfg).get("walk_cadence_days")
    if v is None:
        return None
    if isinstance(v, bool) or not isinstance(v, int) or v <= 0:
        raise ConfigError(f"projects[{project_key!r}].walk_cadence_days must be a positive "
                          f"integer, got {v!r}")
    return v


def _artifact_raw(value, where):
    if value is not None and (not isinstance(value, str) or not value.strip()):
        raise ConfigError(f"{where} must be a non-empty path string, got {value!r}")
    return value


def _artifact_dir_of(key, entry, global_raw):
    """(directory, configured) for one registered project (see artifact_dir)."""
    root = lit_util.project_root(key, entry)
    own = _artifact_raw(entry.get("artifact_dir"), f"projects[{key!r}].artifact_dir")
    if own:
        p = Path(own).expanduser()
        return (p if p.is_absolute() else root / p), True
    if global_raw:
        p = Path(global_raw).expanduser()
        # sweep's artifact names carry no project, so one absolute directory holds a subfolder per key
        return (p / key if p.is_absolute() else root / p), True
    return root, False


def artifact_dir(key, cfg=None) -> Path:
    """Where sweep, migrate and the runner keep a project's run artifacts
    (lit_pull_queue[.<tag>].<run_id>.<stage>.csv): projects.json `artifact_dir`, the project's own
    value winning over the top-level one. Unset: the project root, exactly as before. A relative
    path is under the project root; a top-level ABSOLUTE path holds one subfolder per project,
    `<it>/<project key>`; a per-project absolute path is used as given. An unregistered key returns
    its project root. Two projects resolving to one directory (where either has the key set) is a
    ConfigError. sweep --artifact-dir still wins for one run. The queue files, retry_later and the
    worklist .md files stay in the project root."""
    c = load(cfg)
    projects = c.get("projects") or {}
    global_raw = _artifact_raw(c.get("artifact_dir"), "artifact_dir")
    if key not in projects:
        return lit_util.project_root(key, {})
    mine, configured = _artifact_dir_of(key, projects[key] or {}, global_raw)
    if configured or global_raw or any(isinstance(e, dict) and e.get("artifact_dir")
                                       for e in projects.values()):
        want = os.path.normcase(os.path.abspath(mine))
        for other, entry in projects.items():
            if other == key:
                continue
            d, conf = _artifact_dir_of(other, entry or {}, global_raw)
            if (conf or configured) and os.path.normcase(os.path.abspath(d)) == want:
                raise ConfigError(f"artifact_dir: projects {key!r} and {other!r} both resolve to {mine}; "
                                  f"each project needs its own artifact directory")
    return mine


def _block(name, cfg):
    v = load(cfg).get(name) or {}
    if not isinstance(v, dict):
        raise ConfigError(f"{name} must be an object, got {v!r}")
    return dict(v)


def hosts(cfg=None) -> dict:
    """Host-policy overrides, with the PDF switches defaulting off. A switch must be a bool."""
    out = {**HOST_SWITCH_DEFAULTS, **_block("hosts", cfg)}
    for k in HOST_SWITCH_DEFAULTS:
        if not isinstance(out[k], bool):
            raise ConfigError(f"hosts.{k} must be true or false, got {out[k]!r}")
    return out


def s2(cfg=None) -> dict:
    return _block("s2", cfg)


def openalex(cfg=None) -> dict:
    return _block("openalex", cfg)
