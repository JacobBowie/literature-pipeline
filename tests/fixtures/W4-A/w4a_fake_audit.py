"""Stand-in for audit_portfolio.run (spec "audit" -> "_portfolio"): writes json_path when given."""
import json

import w4a_fake_common as common


def run(project=None, json_path=None, holdings=True, **kw):
    common.log("audit", project=project, json_path=json_path, holdings=holdings)
    s = common.spec("audit")
    common.behave(s)
    res = {"exit_code": s.get("exit_code", 0), "projects": [], "summary": {"fail": 0}}
    if json_path:
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(res, f)
    return res
