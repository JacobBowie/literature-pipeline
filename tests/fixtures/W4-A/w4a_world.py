"""A temp world for the runner tests: a temp root, a temp registry (state_dir, root, db_dir and
loose_ends all temp), the stand-in stage modules, stubbed preflight and canaries, and the runner's
launcher set to run the shim in-process (amendment 16). Every module path is set with monkeypatch,
never by assignment (conftest restores neither lit_util.PROJECTS_ROOT nor config.CONFIG_PATH)."""
import csv
import io
import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import lit_util
from litpipe import canaries, config, ledger, net, preflight, runner, state, walk
from litpipe.outcomes import Kind, Outcome

FIX = Path(__file__).resolve().parent
HARNESS = FIX / "run_runner.py"
REPO = FIX.parent.parent.parent
STANDINS = {"seed": "w4a_fake_seed", "sweep": "w4a_fake_sweep", "route": "w4a_fake_route",
            "walk": "w4a_fake_walk", "reverse": "w4a_fake_reverse", "index": "w4a_fake_index",
            "abstracts": "w4a_fake_abstracts", "recommendations": "w4a_fake_recs", "audit": "w4a_fake_audit"}
ARXIV_HOSTS = ("export.arxiv.org", "arxiv.org", "www.arxiv.org")
QUEUE_HEADER = "doi,title,authors,year,destination,notes\n"


def ok_preflight(**kw):
    return [Outcome(Kind.OK, host="stub", detail="test preflight", payload={"check": "stub"})]


class World:
    def __init__(self, tmp, monkeypatch, *, seed_state=True, inprocess=True):
        self.tmp = Path(tmp)
        self.mp = monkeypatch
        self.root = self.tmp / "Projects"
        self.state_dir = self.tmp / "state"
        self.db_dir = self.tmp / "refs"
        self.loose = self.tmp / "notes" / "LOOSE_ENDS.md"
        self.cfg_path = self.tmp / "reg" / "projects.json"
        self.cfg_path.parent.mkdir(parents=True)
        self.root.mkdir()
        self.projects = {}
        self.top = {"root": str(self.root), "state_dir": str(self.state_dir), "db_dir": str(self.db_dir),
                    "loose_ends": str(self.loose)}
        self.fake = {}
        self.local_outs = []
        self.canary_calls = []
        import sweep
        monkeypatch.setattr(config, "CONFIG_PATH", self.cfg_path)
        monkeypatch.setattr(lit_util, "PROJECTS_ROOT", self.root)
        monkeypatch.setattr(state, "DB_PATH", self.state_dir / state.DB_NAME)
        monkeypatch.setattr(ledger, "LEDGER_DIR", self.state_dir / "ledger")
        monkeypatch.setattr(ledger, "RUN_ID", None)
        monkeypatch.setattr(walk, "CACHE_PATH", self.state_dir / "s2_cache.duckdb")
        monkeypatch.setattr(sweep, "CONFIG_PATH", self.cfg_path)
        monkeypatch.setattr(net, "STATE", None)        # refusals live in litpipe.state (amendment 16)
        monkeypatch.setattr(state, "_current_run", None)
        monkeypatch.delenv("LITPIPE_RUN_ID", raising=False)
        monkeypatch.delenv("S2_API_KEY", raising=False)
        for job, mod in STANDINS.items():
            monkeypatch.setitem(runner.STAGE_MODULES, job, mod)
        monkeypatch.setattr(preflight, "run", ok_preflight)
        monkeypatch.setattr(canaries, "run", self._canaries)
        if inprocess:
            monkeypatch.setattr(runner, "subprocess_launcher", runner.inprocess_launcher)
        if str(FIX) not in sys.path:
            monkeypatch.syspath_prepend(str(FIX))
        self.write()
        if seed_state:
            for h in ARXIV_HOSTS:
                state.refuse(h, "manual: refused since 2026-09-25", persistence="manual")

    # -- stubs
    def _canaries(self, profile, *, phase="all", context=None, state=None, request=None, cfg=None, now=None):
        canaries._check_context(context)          # the real contract check (W4a verifier K-4)
        self.canary_calls.append({"profile": profile, "phase": phase, "context": context, "now": now})
        return list(self.local_outs) if phase == "local" else []

    # -- the registry
    def register(self, key, lib_dir="literature", parent=None, **extra):
        e = {"lib_dir": lib_dir, **extra}
        if parent:
            e["parent"] = parent
        self.projects[key] = e
        self.lib(key).mkdir(parents=True, exist_ok=True)
        self.proot(key).mkdir(parents=True, exist_ok=True)
        self.write()
        return self.proot(key)

    def write(self):
        self.cfg_path.write_bytes(json.dumps({**self.top, "projects": self.projects}, indent=1).encode("utf-8"))
        (self.cfg_path.parent / "fake_stages.json").write_bytes(json.dumps(self.fake, indent=1).encode("utf-8"))

    def set_fake(self, stage, project, **spec):
        self.fake.setdefault(stage, {})[project or "_portfolio"] = spec
        self.write()

    def proot(self, key):
        return lit_util.project_root(key, self.projects[key])

    def lib(self, key):
        return lit_util.lib_paths(key, self.projects[key])[1]

    def dest(self, key):
        return os.path.relpath(self.lib(key), self.proot(key)).replace(os.sep, "/") + "/"

    def queue(self, key, dois, tag=None, name=None):
        p = self.proot(key) / (name or (f"lit_pull_queue.{tag}.csv" if tag else "lit_pull_queue.csv"))
        body = QUEUE_HEADER + "".join(f"{d},Title {i},Author A,2020,{self.dest(key)},n\n" for i, d in enumerate(dois))
        p.write_bytes(body.encode("utf-8"))
        return p

    # -- reading the results
    def calls(self, stage=None):
        p = self.cfg_path.parent / "calls.jsonl"
        if not p.is_file():
            return []
        rows = [json.loads(line) for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]
        return [r for r in rows if stage is None or r["stage"] == stage]

    def summaries(self):
        base = self.state_dir / "runner"
        if not base.is_dir():
            return []
        files = [d / "summary.json" for d in base.iterdir() if (d / "summary.json").is_file()]
        files.sort(key=lambda p: p.stat().st_mtime_ns)       # run ids in one second sort by a random suffix
        return [json.loads(p.read_text(encoding="utf-8")) for p in files]

    def runs(self):
        p = self.state_dir / state.DB_NAME
        if not p.exists():
            return []
        con = sqlite3.connect(str(p))
        try:
            return [dict(zip(("run_id", "kind", "status", "finished"), r)) for r in
                    con.execute("SELECT run_id, kind, status, finished FROM runs ORDER BY rowid").fetchall()]
        finally:
            con.close()

    def loose_lines(self):
        if not self.loose.is_file():
            return []
        return [ln for ln in self.loose.read_text(encoding="utf-8").splitlines() if ln.strip()]

    def jobs(self, summ, job=None, project=None):
        return [j for j in summ["jobs"] if (job is None or j["job"] == job)
                and (project is None or j["project"] == project)]

    def ledger_arxiv_lines(self):
        d = self.state_dir / "ledger"
        n = 0
        for f in (d.glob("*.jsonl") if d.is_dir() else []):
            for line in f.read_text(encoding="utf-8").splitlines():
                if "arxiv" in (json.loads(line).get("host") or ""):
                    n += 1
        return n

    def listing(self):
        """Every path under the temp dir with its size and mtime (the dry-run check)."""
        out = {}
        for p in sorted(self.tmp.rglob("*")):
            s = p.stat()
            out[str(p.relative_to(self.tmp))] = (p.is_dir(), s.st_size if p.is_file() else 0,
                                                 s.st_mtime_ns if p.is_file() else 0)
        return out

    # -- a real process (amendment 16)
    def overrides(self, **runner_attrs):
        state_attrs = runner_attrs.pop("state", {})
        (self.cfg_path.parent / "runner_overrides.json").write_bytes(
            json.dumps({"runner": runner_attrs, "state": state_attrs}).encode("utf-8"))

    def spawn(self, *argv, env=None):
        e = {**os.environ, "PYTHONUTF8": "1", **(env or {})}
        e.pop("LITPIPE_RUN_ID", None)
        out = open(self.tmp / f"harness.{len(list(self.tmp.glob('harness.*.log')))}.log", "wb")
        return subprocess.Popen([sys.executable, str(HARNESS), str(self.cfg_path), *argv], cwd=str(REPO), env=e,
                                stdout=out, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)

    def harness(self, *argv, timeout=180, env=None):
        p = self.spawn(*argv, env=env)
        try:
            return p.wait(timeout=timeout)
        finally:
            if p.poll() is None:
                p.kill()

    def harness_log(self):
        return "".join(p.read_text(encoding="utf-8", errors="replace") for p in sorted(self.tmp.glob("harness.*.log")))


def stand_in_overrides():
    return dict(STAGE_MODULES=dict(STANDINS))


def read_csv(path):
    with open(path, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(io.StringIO(f.read())))
