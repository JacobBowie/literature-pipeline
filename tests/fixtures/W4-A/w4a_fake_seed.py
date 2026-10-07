"""Stand-in for seed_queue_from_top_candidates.run (spec "seed" -> project): writes the draft
lit_pull_queue.draft.csv at the project root (lit_util.project_root), as the seeder does.

Spec: dois [list] (default two rows), exit_code (default 0), no_draft (write nothing)."""
import w4a_fake_common as common

HEAD = "# REVIEW BEFORE SWEEP: stand-in draft\n"


def run(project, **kw):
    common.log("seed", project=project, kw=kw)
    s = common.spec("seed", project)
    common.behave(s)
    import lit_util
    entry = (common.registry().get("projects") or {}).get(project) or {}
    root = lit_util.project_root(project, entry)
    code = s.get("exit_code", 0)
    if code == 1:
        return {"step": "seed_queue", "exit_code": 1, "status": "error", "error": "stand-in config error",
                "reasons": ["stand-in config error"], "aborted": None, "transport_failures": 0}
    dois = s.get("dois", ["10.5555/w4a.seed1", "10.5555/w4a.seed2"])
    out = root / "lit_pull_queue.draft.csv"
    if not s.get("no_draft"):
        dest = s.get("destination", "literature/")
        body = HEAD + "doi,title,authors,year,destination,notes\n" + "".join(
            f"{d},Title {i},Author A,2020,{dest},seeded\n" for i, d in enumerate(dois))
        root.mkdir(parents=True, exist_ok=True)
        with open(out, "w", encoding="utf-8", newline="\n") as f:
            f.write(body)
    return {"step": "seed_queue", "exit_code": code, "status": "ok", "reasons": [], "aborted": None,
            "transport_failures": 0, "project": project, "output": str(out), "rows": len(dois)}
