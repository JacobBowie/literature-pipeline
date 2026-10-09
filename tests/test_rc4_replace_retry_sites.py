"""Every library writer's final move retries a transient Windows denial (lit_util RC4).

A scanner, indexer or sync client can hold a just-written file for a moment, and the move into place
then fails with WinError 5. The fetch stages, the import, the figure fetcher and the mismatch restore
move through lit_util._replace_with_retry, as atomic_write_text does; one denied attempt is retried."""
import os

import pytest

import fetch_figures
import import_downloads
import lit_util
import pmc_fetch
import preprint_fetch
import unpaywall_fetch_v2

WRITERS = [
    ("unpaywall_fetch_v2._write_bytes", lambda dest: unpaywall_fetch_v2._write_bytes(dest, b"%PDF-1.4 x")),
    ("pmc_fetch._write_pdf", lambda dest: pmc_fetch._write_pdf(dest, b"%PDF-1.4 x")),
    ("preprint_fetch._write_bytes", lambda dest: preprint_fetch._write_bytes(dest, b"%PDF-1.4 x")),
    ("import_downloads._restore_bytes", lambda dest: import_downloads._restore_bytes(dest, b"%PDF-1.4 x")),
    ("fetch_figures._write_bytes", lambda dest: fetch_figures._write_bytes(dest, b"\x89PNG x")),
]


@pytest.mark.parametrize("name,write", WRITERS, ids=[w[0] for w in WRITERS])
def test_one_denied_move_into_place_is_retried(tmp_path, monkeypatch, name, write):
    real, denied = os.replace, []

    def replace(src, dst, *a, **k):
        if not denied:
            denied.append(str(dst))
            raise PermissionError(13, "Access is denied (injected)", str(dst))
        return real(src, dst, *a, **k)
    monkeypatch.setattr(os, "replace", replace)
    monkeypatch.setattr(lit_util, "_REPLACE_BACKOFF", (0, 0, 0, 0))
    dest = tmp_path / "2020_Smith_Title.pdf"
    write(str(dest))
    assert denied == [str(dest)] and dest.read_bytes().endswith(b" x")
    assert [p.name for p in tmp_path.iterdir()] == [dest.name]       # no temp file left behind
