"""Shared helpers for the W4-A stand-in stage modules.

A stand-in reads its behaviour from `fake_stages.json` beside the registry the shim bound
(litpipe.config.CONFIG_PATH), keyed by stage and project, and appends one JSON line per call to
`calls.jsonl` there. It never sends a request. Behaviours that end the process (no_result,
malformed) only make sense in a child process; in-process they raise instead."""
import json
import os
import subprocess
import sys
import time
from pathlib import Path


def here():
    from litpipe import config
    return Path(config.CONFIG_PATH).parent


def registry():
    from litpipe import config
    with open(config.CONFIG_PATH, encoding="utf-8") as f:
        return json.load(f)


def spec(stage, project=None):
    p = here() / "fake_stages.json"
    if not p.is_file():
        return {}
    with open(p, encoding="utf-8") as f:
        data = json.load(f)
    block = data.get(stage) or {}
    return dict(block.get(project or "_portfolio") or block.get("*") or {})


def log(stage, **kw):
    rec = {"stage": stage, "pid": os.getpid(), "run_env": os.environ.get("LITPIPE_RUN_ID"), **kw}
    with open(here() / "calls.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, default=str) + "\n")


def in_child():
    return "_stage" in sys.argv


def _out_path():
    return sys.argv[sys.argv.index("--out") + 1]


def behave(s):
    """Apply the generic behaviours of a spec block, in order."""
    if s.get("pid_file"):
        Path(s["pid_file"]).write_text(str(os.getpid()), encoding="utf-8")
    if s.get("grandchild"):
        gc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(600)"],
                              stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        Path(s["grandchild"]).write_text(str(gc.pid), encoding="utf-8")
    if s.get("refuse"):
        from litpipe import state
        for host in s["refuse"]:
            state.refuse(host, "stand-in: refused for the run", persistence="run")
    if s.get("ledger"):                   # ledger lines as litpipe.net writes them, for this run
        from litpipe import ledger
        for rec in s["ledger"]:
            ledger.write(dict(rec))
    if s.get("flood"):
        out = sys.stdout.buffer if hasattr(sys.stdout, "buffer") else None
        for i in range(int(s["flood"])):
            if out is not None:
                out.write(b"line " + str(i).encode() + bytes([0xff, 0xfe, 0x80]) + b"\n")
            else:
                print(f"line {i} �")
        if out is not None:
            out.flush()
    if s.get("traceback"):
        print("Traceback (most recent call last):\n  File \"x.py\", line 1, in <module>\nRuntimeError: swallowed",
              flush=True)
    if s.get("started_marker"):
        Path(s["started_marker"]).write_text("started", encoding="utf-8")
    if s.get("sleep"):
        end = time.time() + float(s["sleep"])
        while time.time() < end:
            time.sleep(0.25)
            if s.get("observe"):
                observe(s["observe"])
    elif s.get("observe"):
        observe(s["observe"])
    mode = s.get("mode")
    if mode == "crash":
        raise RuntimeError("stand-in crash")
    if mode == "exit2":
        raise SystemExit(2)
    if mode == "no_result":
        if not in_child():
            raise RuntimeError("no_result needs a child process")
        sys.stdout.flush()
        os._exit(0)
    if mode == "malformed":
        if not in_child():
            raise RuntimeError("malformed needs a child process")
        Path(_out_path()).write_text("{not json", encoding="utf-8")
        sys.stdout.flush()
        os._exit(0)


def observe(path):
    """Record what this process sees of the run right now: live runs and the S2 refusal."""
    from litpipe import state
    rid = os.environ.get("LITPIPE_RUN_ID") or state.current_run()
    live = [r["run_id"] for r in state.live_runs()]
    rec = {"t": time.time(), "run": rid, "live": rid in live,
           "s2_refused": state.is_refused("api.semanticscholar.org")}
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec) + "\n")
