"""warn_if_default_email (dispatch 0.6 name; W2-E1 amendment 3): warns when LITPIPE_EMAIL is unset,
empty, not an address, or an RFC 2606 reserved domain; names the consequences (Unpaywall 422,
Crossref blocking); never prints an address or `None`; fires once."""
import pytest

import ris_emit as R


@pytest.fixture(autouse=True)
def fresh(monkeypatch):
    monkeypatch.setattr(R, "_email_warned", False)


@pytest.mark.parametrize("value", [None, "", "   ", "you@example.com", "someone@example.org",
                                   "a@lab.example.net", "me@host.test", "not-an-address"])
def test_warns_for_unusable_addresses(monkeypatch, capsys, value):
    if value is None:
        monkeypatch.delenv("LITPIPE_EMAIL", raising=False)
    else:
        monkeypatch.setenv("LITPIPE_EMAIL", value)
    R.warn_if_default_email()
    err = capsys.readouterr().err
    assert "LITPIPE_EMAIL" in err and "422" in err and "Unpaywall" in err and "Crossref" in err
    assert "None" not in err and "maintainer" not in err
    if value and "@" in value:
        assert value not in err                                  # the address is never echoed


def test_silent_for_a_real_address(monkeypatch, capsys):
    monkeypatch.setenv("LITPIPE_EMAIL", "tester@litpipe-test.org")
    R.warn_if_default_email()
    assert capsys.readouterr().err == ""


def test_fires_once(monkeypatch, capsys):
    monkeypatch.delenv("LITPIPE_EMAIL", raising=False)
    R.warn_if_default_email()
    R.warn_if_default_email()
    assert capsys.readouterr().err.count("[litpipe]") == 1
