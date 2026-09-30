"""DEC-29: the pipeline replaces only the .ris files it wrote and nobody has edited since (W2-E1).
write_ris records sha256 per real path in the state kv (namespace "ris"); an edited or unrecorded
file is curated and kept unless force=True. Tests use an in-memory FakeState or a temp sqlite state,
never the real one."""
import hashlib
import os
import sys

import pytest

import ris_emit as R
from tests import netmock

RIS = "TY  - JOUR\nTI  - A\nER  - \n"
RIS2 = "TY  - JOUR\nTI  - B\nER  - \n"


@pytest.fixture
def st(monkeypatch):
    s = netmock.FakeState()
    monkeypatch.setattr(R, "STATE", s)
    return s


def sha(p):
    return hashlib.sha256(p.read_bytes()).hexdigest()


def test_new_file_is_written_and_recorded(tmp_path, st):
    p = tmp_path / "lib" / "2020_X_A.ris"
    assert R.write_ris(str(p), RIS) is True
    assert st.kv[("ris", R.manifest_key(p))] == sha(p)
    assert R.ris_owner(str(p)) == "pipeline"


def test_pipeline_owned_file_is_replaced_and_rerecorded(tmp_path, st):
    p = tmp_path / "a.ris"
    R.write_ris(str(p), RIS)
    assert R.write_ris(str(p), RIS2, overwrite=True) is True
    assert p.read_text(encoding="utf-8") == RIS2
    assert st.kv[("ris", R.manifest_key(p))] == sha(p)


def test_edited_file_is_not_overwritten(tmp_path, st, capsys):
    p = tmp_path / "a.ris"
    R.write_ris(str(p), RIS)
    p.write_text(RIS.replace("TI  - A", "TI  - A: the curated subtitle"), encoding="utf-8")   # EndNote edit
    edited = p.read_bytes()
    assert R.ris_owner(str(p)) == "edited"
    assert R.write_ris(str(p), RIS2, overwrite=True) is False
    assert p.read_bytes() == edited
    assert "edited since the pipeline wrote it" in capsys.readouterr().err


def test_unrecorded_file_is_treated_as_curated(tmp_path, st, capsys):
    p = tmp_path / "a.ris"
    p.write_text(RIS, encoding="utf-8")
    assert R.ris_owner(str(p)) == "unrecorded"
    assert R.write_ris(str(p), RIS2, overwrite=True) is False
    assert p.read_text(encoding="utf-8") == RIS
    assert "no pipeline record" in capsys.readouterr().err


def test_force_replaces_a_curated_file_and_records_it(tmp_path, st):
    p = tmp_path / "a.ris"
    p.write_text(RIS, encoding="utf-8")
    assert R.write_ris(str(p), RIS2, overwrite=True, force=True) is True
    assert p.read_text(encoding="utf-8") == RIS2 and R.ris_owner(str(p)) == "pipeline"


def test_overwrite_false_skips_even_a_pipeline_file(tmp_path, st):
    p = tmp_path / "a.ris"
    R.write_ris(str(p), RIS)
    assert R.write_ris(str(p), RIS2, overwrite=False) is False
    assert p.read_text(encoding="utf-8") == RIS


def test_identical_unrecorded_file_is_adopted_not_rewritten(tmp_path, st):
    p = tmp_path / "a.ris"
    p.write_bytes(RIS.encode("utf-8"))
    before = os.stat(p).st_mtime_ns
    assert R.write_ris(str(p), RIS, overwrite=True) is True
    assert os.stat(p).st_mtime_ns == before
    assert R.ris_owner(str(p)) == "pipeline"


def test_every_spelling_of_a_path_shares_one_record(tmp_path, st, monkeypatch):
    lib = tmp_path / "Lib"
    p = lib / "a.ris"
    R.write_ris(str(p), RIS)
    monkeypatch.chdir(tmp_path)
    assert R.manifest_key(os.path.join("Lib", ".", "a.ris")) == R.manifest_key(p)
    if sys.platform == "win32":
        assert R.manifest_key(str(p).upper()) == R.manifest_key(p)
    assert R.write_ris(os.path.join("Lib", "a.ris"), RIS2, overwrite=True) is True


def test_emit_ris_for_pdf_keeps_an_edited_ris(tmp_path, st, monkeypatch):
    monkeypatch.setattr(R, "resolve_meta", lambda doi: ({"title": "New", "doi": doi, "type": "journal-article"}, "crossref"))
    pdf = tmp_path / "2020_X_T.pdf"
    ris = tmp_path / "2020_X_T.ris"
    assert R.emit_ris_for_pdf("10.1234/abc.1", str(pdf)) == ("OK", str(ris))
    assert R.emit_ris_for_pdf("10.1234/abc.1", str(pdf)) == ("EXISTS_SKIP", str(ris))
    ris.write_text("TY  - JOUR\nTI  - Curated\nER  - \n", encoding="utf-8")
    assert R.emit_ris_for_pdf("10.1234/abc.1", str(pdf), overwrite=True) == ("EXISTS_KEPT", str(ris))
    assert "Curated" in ris.read_text(encoding="utf-8")


def test_manifest_round_trips_through_the_real_state_module(tmp_path, monkeypatch):
    import litpipe.state as real
    monkeypatch.setattr(real, "DB_PATH", tmp_path / "state" / "litpipe_state.sqlite")
    monkeypatch.setattr(R, "STATE", real)
    p = tmp_path / "a.ris"
    assert R.write_ris(str(p), RIS) is True
    assert real.kv_get("ris", R.manifest_key(p)) == sha(p)
    assert R.write_ris(str(p), RIS2, overwrite=True) is True
    p.write_text("edited\n", encoding="utf-8")
    assert R.write_ris(str(p), RIS, overwrite=True) is False


def test_broken_state_protects_the_file(tmp_path, monkeypatch, capsys):
    class Broken:
        def kv_get(self, ns, key):
            raise OSError("locked")

        def kv_set(self, ns, key, value, ttl_s=None):
            raise OSError("locked")
    monkeypatch.setattr(R, "STATE", Broken())
    p = tmp_path / "a.ris"
    assert R.write_ris(str(p), RIS) is True                     # a new file is still written
    assert "state write failed" in capsys.readouterr().err
    assert R.write_ris(str(p), RIS2, overwrite=True) is False   # unrecorded: kept
    assert p.read_text(encoding="utf-8") == RIS
