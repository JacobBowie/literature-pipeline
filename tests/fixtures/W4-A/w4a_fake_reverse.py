"""Stand-in for reverse_citations.run (spec "reverse" -> project; see w4a_fake_walk.result)."""
import w4a_fake_common as common
from w4a_fake_walk import result


def run(project=None, sources=None, **kw):
    common.log("reverse", project=project, sources=sources)
    s = common.spec("reverse", project)
    common.behave(s)
    return result("reverse_citations", project, s)
