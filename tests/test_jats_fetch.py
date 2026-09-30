"""c13: fetch_jats_xml shared JATS GET + recheck 404 -> NOT_AVAILABLE.

fetch_jats_xml consolidates the ~7-line Europe PMC JATS GET that pmc_fetch,
backfill_fulltext, recheck_pmc, and jats_to_text._smoke_test each carried. It
classifies HTTP-level outcomes and returns (content|None, status); transport
errors PROPAGATE so each caller keeps its own except-clause + error-label
format. recheck now labels a 404 NOT_AVAILABLE (previously a bare HTTP_404).

Offline: the lazy `import requests` inside fetch_jats_xml resolves to the cached
module, so patching requests.get on the real module reaches it.
"""
import json

import pytest
import requests

import jats_to_text
import pmc_fetch
import recheck_pmc
import backfill_fulltext


class FakeResp:
    def __init__(self, status_code, content=b""):
        self.status_code = status_code
        self.content = content


def _patch_get(monkeypatch, resp=None, exc=None):
    def fake_get(url, **kwargs):
        if exc is not None:
            raise exc
        return resp
    monkeypatch.setattr(requests, "get", fake_get)


# ---------- fetch_jats_xml status classification ----------

@pytest.mark.parametrize("status_code,content,expect", [
    (200, b"<article>x</article>", "OK"),
    (200, b"   <article/>",        "OK"),              # leading ws stripped before '<' check
    (404, b"",                     "NOT_AVAILABLE"),
    (500, b"oops",                 "HTTP_500"),
    (403, b"",                     "HTTP_403"),
    (200, b"",                     "EMPTY_OR_NON_XML"),  # empty body
    (200, b"not xml at all",       "EMPTY_OR_NON_XML"),  # 200 but not XML
])
def test_fetch_jats_xml_status(monkeypatch, status_code, content, expect):
    _patch_get(monkeypatch, FakeResp(status_code, content))
    body, status = jats_to_text.fetch_jats_xml("PMC1", "ua/1.0")
    assert status == expect
    assert (body == content) if expect == "OK" else (body is None)


def test_fetch_jats_xml_propagates_transport_error(monkeypatch):
    """Transport failures are NOT swallowed -- they propagate to the caller's own
    except so each caller keeps its established error-label format."""
    _patch_get(monkeypatch, exc=requests.exceptions.ConnectionError("boom"))
    with pytest.raises(requests.exceptions.ConnectionError):
        jats_to_text.fetch_jats_xml("PMC1", "ua/1.0")


# ---------- recheck gains 404 -> NOT_AVAILABLE (the one intended behavior change) ----------

def test_recheck_404_is_not_available(monkeypatch, tmp_path):
    _patch_get(monkeypatch, FakeResp(404))
    sidecar = tmp_path / "p.fulltext.json"
    ok, status = recheck_pmc.fetch_jats_sidecar("PMC404", str(sidecar))
    assert (ok, status) == (False, "NOT_AVAILABLE")
    assert not sidecar.exists()  # nothing written on a miss


# ---------- per-caller error-label FORMAT is preserved (the key design decision) ----------


def test_recheck_transport_error_label_is_bare(monkeypatch, tmp_path):
    """recheck no longer imports requests, yet the propagated transport error is
    still caught by its bare `except Exception` with the same label format."""
    _patch_get(monkeypatch, exc=requests.exceptions.ConnectionError("boom"))
    ok, status = recheck_pmc.fetch_jats_sidecar("PMC1", str(tmp_path / "p.json"))
    assert ok is False and status == "ERROR_boom"


def test_backfill_transport_error_label_has_type(monkeypatch, tmp_path):
    """backfill catches specific types -> 'ERROR_<type>:<msg>'; it keeps `import
    requests` precisely because its except references requests.RequestException."""
    _patch_get(monkeypatch, exc=requests.exceptions.ConnectionError("boom"))
    ok, status = backfill_fulltext.fetch_sidecar("PMC1", str(tmp_path / "p.json"))
    assert ok is False and status.startswith("ERROR_ConnectionError:")


# ---------- 200-OK happy path THROUGH each caller (closes the c13-review coverage gap) ----------
# The isolation tests above prove fetch_jats_xml RETURNS content on OK; these prove each caller USES
# it correctly -- the exact lines c13 rewired (r.content -> content; `if content is None` guard).
# A regression like a leftover `parse_jats(r.content)` (NameError, r deleted) would be swallowed by
# the except and silently stop full-text production; every one of these would go red.
JATS_OK = (b"<article><front><article-meta>"
           b"<article-title>Test Heat Title</article-title>"
           b"</article-meta></front></article>")


def test_recheck_ok_writes_sidecar(monkeypatch, tmp_path):
    """Default pdf_head_text='' -> the title-sanity check is skipped; sidecar is written
    with the _recheck_source_doi provenance annotation."""
    _patch_get(monkeypatch, FakeResp(200, JATS_OK))
    sidecar = tmp_path / "p.fulltext.json"
    ok, status = recheck_pmc.fetch_jats_sidecar("PMC1", str(sidecar), doi="10.1/x")
    assert (ok, status) == (True, "OK")
    data = json.loads(sidecar.read_text(encoding="utf-8"))
    assert data["title"] == "Test Heat Title" and data["_recheck_source_doi"] == "10.1/x"


def test_recheck_title_mismatch_refuses_sidecar(monkeypatch, tmp_path):
    """The RC3 title-sanity branch (kept intact by c13, right after the rewired lines): a JATS
    title absent from the PDF head signals a wrong-paper attachment -> refuse to write."""
    _patch_get(monkeypatch, FakeResp(200, JATS_OK))
    sidecar = tmp_path / "p.fulltext.json"
    ok, status = recheck_pmc.fetch_jats_sidecar(
        "PMC1", str(sidecar), pdf_head_text="totally unrelated words with no overlap at all")
    assert ok is False and status.startswith("TITLE_MISMATCH")
    assert not sidecar.exists()


def test_backfill_ok_writes_sidecar(monkeypatch, tmp_path):
    _patch_get(monkeypatch, FakeResp(200, JATS_OK))
    sidecar = tmp_path / "p.fulltext.json"
    ok, status = backfill_fulltext.fetch_sidecar("PMC1", str(sidecar))
    assert (ok, status) == (True, "OK")
    assert json.loads(sidecar.read_text(encoding="utf-8"))["title"] == "Test Heat Title"


def test_backfill_ok_merges_existing_sidecar(monkeypatch, tmp_path):
    """RC5 branch (right after the c13-rewired parse line): a re-fetch over an existing sidecar
    keeps enriched fields (doi/figures) the minimal fresh JATS parse lacks."""
    _patch_get(monkeypatch, FakeResp(200, JATS_OK))
    sidecar = tmp_path / "p.fulltext.json"
    sidecar.write_text(json.dumps({"title": "old", "doi": "10.1/x", "figures": [{"id": "f1"}]}),
                       encoding="utf-8")
    ok, status = backfill_fulltext.fetch_sidecar("PMC1", str(sidecar))
    assert (ok, status) == (True, "OK")
    data = json.loads(sidecar.read_text(encoding="utf-8"))
    assert data["title"] == "Test Heat Title"      # fresh parse wins for a field it populated
    assert data["doi"] == "10.1/x"                   # RC5: enriched doi preserved
    assert data["figures"] == [{"id": "f1"}]         # RC5: figures preserved


def test_smoke_test_return_code(monkeypatch):
    """The 4th routed caller: _smoke_test returns 0 on OK, 1 on a miss."""
    _patch_get(monkeypatch, FakeResp(200, JATS_OK))
    assert jats_to_text._smoke_test("PMC1", dump=False) == 0
    _patch_get(monkeypatch, FakeResp(404))
    assert jats_to_text._smoke_test("PMC1", dump=False) == 1
