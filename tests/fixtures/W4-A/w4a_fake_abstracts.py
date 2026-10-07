"""Stand-in for enrich_abstracts.run (spec "abstracts" -> "_portfolio"): result keys only."""
import w4a_fake_common as common


def run(db=None, **kw):
    common.log("abstracts", db=db)
    s = common.spec("abstracts")
    common.behave(s)
    res = {"db": db, "targets": 3, "attempted": 3, "hits": 2, "misses": 1, "errors_permanent": 0,
           "errors_transient": 0, "commits": 1, "rows_written": 3, "write_failures": [], "interrupted": False,
           "stopped_early": False, "stop_reason": ""}
    res.update(s.get("result") or {})
    return res
