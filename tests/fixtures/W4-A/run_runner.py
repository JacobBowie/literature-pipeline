"""Real-process harness for the runner tests (W4-A amendment 16).

    python tests/fixtures/W4-A/run_runner.py <temp registry> <runner argv ...>

Never start `python -m litpipe.runner run|batch` bare in a test: it resolves the real registry and
state dir, and a winning run would send preflight and the idconv canary live. Before importing
anything else from the repo, this script:
  * binds every path the stage shim binds to the temp registry (litpipe.config.CONFIG_PATH,
    lit_util.PROJECTS_ROOT from its root, litpipe.state.DB_PATH and litpipe.ledger.LEDGER_DIR from
    its state_dir), plus sweep.CONFIG_PATH and litpipe.walk.CACHE_PATH; the registry must carry a
    temp state_dir, root, db_dir and loose_ends, and its state_dir may not be the real one;
  * replaces each litpipe.net._TRANSPORTS entry with a loopback-only guard;
  * replaces litpipe.preflight.run with an all-OK offline stub and litpipe.canaries.run with one
    that returns [];
  * waits on a barrier file when LITPIPE_TEST_BARRIER names one (after writing
    <barrier>.<pid>.ready), so two runners start together;
  * applies <registry dir>/runner_overrides.json ({"runner": {attr: value}, "state": {...}});
  * puts this folder on PYTHONPATH, so the runner's stage children import the stand-in modules;
then calls litpipe.runner.main(argv) and exits with its code."""
import json
import os
import sys
import time
from pathlib import Path
from urllib.parse import urlsplit

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(HERE))
os.environ["PYTHONPATH"] = os.pathsep.join([str(HERE)] + [p for p in os.environ.get("PYTHONPATH", "").split(os.pathsep) if p])
LOOPBACK = frozenset({"127.0.0.1", "127.0.0.2", "127.0.0.3"})


def main():
    registry = Path(sys.argv[1]).resolve()
    argv = sys.argv[2:]
    with open(registry, encoding="utf-8") as f:
        cfg = json.load(f)
    missing = [k for k in ("state_dir", "root", "db_dir", "loose_ends") if not cfg.get(k)]
    if missing:
        print(f"harness: the temp registry needs {missing}", file=sys.stderr)
        return 97
    import lit_util
    from litpipe import config, ledger, state
    sd = config.state_dir(cfg, create=False)
    real = Path.home() / ".local" / "db" / "literature_pipeline"
    if os.path.normcase(os.path.abspath(sd)) == os.path.normcase(os.path.abspath(real)):
        print("harness: the registry's state_dir is the real one", file=sys.stderr)
        return 98
    config.CONFIG_PATH = registry
    lit_util.PROJECTS_ROOT = lit_util._resolve_projects_root(cfg)
    state.DB_PATH = sd / state.DB_NAME
    ledger.LEDGER_DIR = sd / "ledger"
    import sweep
    sweep.CONFIG_PATH = registry
    from litpipe import walk
    walk.CACHE_PATH = sd / "s2_cache.duckdb"

    from litpipe import net
    for name, fn in list(net._TRANSPORTS.items()):
        def guarded(method, url, *a, _fn=fn, **k):
            host = urlsplit(url).hostname
            if host not in LOOPBACK:
                raise AssertionError(f"live network attempted: {method} {host}")
            return _fn(method, url, *a, **k)
        net._TRANSPORTS[name] = guarded

    from litpipe import canaries, preflight
    from litpipe.outcomes import Kind, Outcome
    preflight.run = lambda **kw: [Outcome(Kind.OK, host="stub", detail="harness: offline preflight stub",
                                          payload={"check": "stub"})]
    canaries.run = lambda *a, **kw: []

    barrier = os.environ.get("LITPIPE_TEST_BARRIER")
    if barrier:
        Path(f"{barrier}.{os.getpid()}.ready").write_text("ready", encoding="utf-8")
        deadline = time.time() + 120
        while not Path(barrier).exists():
            if time.time() > deadline:
                print("harness: barrier timeout", file=sys.stderr)
                return 99
            time.sleep(0.005)

    from litpipe import runner
    ov = registry.parent / "runner_overrides.json"
    if ov.is_file():
        with open(ov, encoding="utf-8") as f:
            o = json.load(f)
        for k, v in (o.get("runner") or {}).items():
            cur = getattr(runner, k)
            if isinstance(cur, dict):
                cur.update(v)
            else:
                setattr(runner, k, v)
        for k, v in (o.get("state") or {}).items():
            setattr(state, k, v)
    return runner.main(argv)


if __name__ == "__main__":
    sys.exit(main())
