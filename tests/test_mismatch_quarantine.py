"""F5: wrong-DOI PDF quarantine helper -- offline, filesystem-only.

The mismatch RESTRUCTURE (commit download state only in the non-mismatch branch) lives inside the
fetchers' main() loops and is integration-level; this pins the extracted helper + that all three
fetchers share it, which is the load-bearing piece a report consumer depends on.
"""
import unpaywall_fetch_v2 as unpw


def test_quarantine_moves_pdf_to_mismatch_dir(tmp_path):
    lib = tmp_path / "lib"
    lib.mkdir()
    dest = lib / "2024_Smith.pdf"
    dest.write_bytes(b"%PDF-1.4 wrong paper")
    assert unpw.quarantine_mismatch(str(dest), str(lib)) is True
    assert not dest.exists()                       # removed from the canonical slot
    q = lib / "_mismatch" / "2024_Smith.pdf"
    assert q.exists() and q.read_bytes() == b"%PDF-1.4 wrong paper"   # preserved, not deleted


def test_quarantine_disambiguates_on_collision(tmp_path):
    """Two different wrong PDFs sharing a canonical basename must BOTH be preserved (no clobber)."""
    lib = tmp_path / "lib"
    lib.mkdir()
    d1 = lib / "2024_Smith.pdf"
    d1.write_bytes(b"%PDF wrong A")
    assert unpw.quarantine_mismatch(str(d1), str(lib)) is True
    d2 = lib / "2024_Smith.pdf"                      # same canonical name, different bytes
    d2.write_bytes(b"%PDF wrong B")
    assert unpw.quarantine_mismatch(str(d2), str(lib)) is True
    mm = lib / "_mismatch"
    assert sorted(p.name for p in mm.iterdir()) == ["2024_Smith.pdf", "2024_Smith_1.pdf"]
    assert (mm / "2024_Smith.pdf").read_bytes() == b"%PDF wrong A"      # first preserved
    assert (mm / "2024_Smith_1.pdf").read_bytes() == b"%PDF wrong B"    # second not clobbered


def test_quarantine_missing_dest_returns_false(tmp_path):
    lib = tmp_path / "lib"
    lib.mkdir()
    assert unpw.quarantine_mismatch(str(lib / "nope.pdf"), str(lib)) is False


def test_quarantine_helper_shared_across_fetchers():
    """All 3 fetchers must route mismatches through the shared unpaywall helper -- not a local
    re-implementation. (__module__, not `is`: pytest's import machinery can duplicate a module
    object across the full collection, but the function's defining module is stable.)"""
    import preprint_fetch   # pmc_fetch (W2-A1) and the Unpaywall stage (W2-B) use the identity check
    for mod in (unpw, preprint_fetch):
        qm = getattr(mod, "quarantine_mismatch", None)
        assert callable(qm), f"{mod.__name__} is missing quarantine_mismatch"
        assert qm.__module__ == "unpaywall_fetch_v2", f"{mod.__name__} uses a non-shared copy"
