"""Stand-in for enrich_recommendations.run (spec "recommendations" -> "_portfolio")."""
import w4a_fake_common as common


def run(db=None, recent_feed=False, **kw):
    common.log("recommendations", db=db, recent_feed=recent_feed)
    s = common.spec("recommendations")
    common.behave(s)
    if not recent_feed:
        return {"step": "enrich_recommendations", "exit_code": 0, "status": "off", "sent": 0}
    res = {"step": "enrich_recommendations", "exit_code": 0, "status": "ok", "asked": 2, "ok": 2, "failed": 0,
           "transport_failures": 0, "reasons": [], "aborted": None}
    res.update(s.get("result") or {})
    return res
