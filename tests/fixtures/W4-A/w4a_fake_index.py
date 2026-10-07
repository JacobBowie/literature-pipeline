"""Stand-in for index_portfolio.run (spec "index" -> project): never opens a database."""
import w4a_fake_common as common
from w4a_fake_walk import result


def run(project=None, db=None, **kw):
    common.log("index", project=project, db=db)
    s = common.spec("index", project)
    common.behave(s)
    res = result("index_portfolio", project, s)
    res.setdefault("indexed", [project] if res["exit_code"] == 0 else [])
    return res
