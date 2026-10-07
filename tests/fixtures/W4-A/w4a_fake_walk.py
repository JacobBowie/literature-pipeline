"""Stand-in for forward_citations.run (and, through `stage`, any exit-convention step).

Spec "walk" -> project: result {extra keys}, exit_code (default 0), reasons, aborted,
fail_seeds [dois recorded as failed in the walk cache], plus
the generic behaviours (sleep, grandchild, flood, traceback, mode crash / no_result / malformed /
no_keys, refuse [hosts], observe)."""
import w4a_fake_common as common


def result(stage, project, s):
    if s.get("mode") == "no_keys":
        return {"step": stage, "note": "a dict without the result table's keys"}
    code = s.get("exit_code", 0)
    res = {"step": stage, "exit_code": code, "status": "ok" if code == 0 else "degraded",
           "reasons": list(s.get("reasons") or ([] if code == 0 else [f"{stage} stand-in exit {code}"])),
           "aborted": s.get("aborted"), "transport_failures": s.get("transport_failures", 0),
           "seeds": s.get("seeds", 3), "failed": len(s.get("fail_seeds") or [])}
    res.update(s.get("result") or {})
    return res


def run(project=None, cache_path=None, **kw):
    common.log("walk", project=project, cache_path=cache_path, kw=kw)
    s = common.spec("walk", project)
    common.behave(s)
    if s.get("fail_seeds"):
        from litpipe import walk
        c = walk.Cache(cache_path or walk.cache_path())
        try:
            for d in s["fail_seeds"]:
                c.record(d, "s2", state="failed", kind="TRANSPORT", reason="stand-in failure")
        finally:
            c.close()
    return result("forward_citations", project, s)
