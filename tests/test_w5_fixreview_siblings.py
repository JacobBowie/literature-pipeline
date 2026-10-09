"""Fix review, sibling sweep of e69e299's finite check: other numeric settings that accept NaN or Infinity."""
import json

import pytest


@pytest.mark.parametrize("raw", ["NaN", "Infinity"])
@pytest.mark.parametrize("key", ["spacing_s", "spacing_keyed_s"])
def test_fr7_a_non_finite_s2_spacing_is_a_config_error(raw, key):
    from litpipe import config, s2
    cfg = json.loads('{"projects": {}, "s2": {"%s": %s}}' % (key, raw))
    with pytest.raises(config.ConfigError):
        s2.settings(cfg)


@pytest.mark.parametrize("secs", ["nan", "inf"])
def test_fr8_a_non_finite_runner_timeout_is_a_usage_error(secs):
    from litpipe import runner
    with pytest.raises(runner._Usage):
        runner._timeouts([f"sweep={secs}"])


def test_fr7_8_finite_values_still_pass():
    from litpipe import runner, s2
    assert runner._timeouts(["sweep=90"]) == {"sweep": 90.0}
    s = s2.settings(json.loads('{"projects": {}, "s2": {"spacing_s": 5}}'))
    assert s.spacing_s == 5.0
