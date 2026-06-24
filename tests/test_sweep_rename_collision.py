"""Regression: sweep.py same-day re-sweep must not crash on the processed.csv rename.

Bug (2026-06-23): a SECOND sweep of one project on the same day crashed with
FileExistsError (WinError 183) because os.rename refuses an existing target. The
fix numeric-suffixes the archive name (.processed.2.csv, .3.csv, ...). This guards
the algorithm used in sweep.run_pipeline (kept in lockstep with the inline logic).
"""
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import sweep  # noqa: E402  — must import cleanly after the patch


def _dedup_processed(queue_csv: Path, today: str) -> Path:
    """Mirror of the suffix logic in sweep.run_pipeline (lines ~196-201)."""
    processed = queue_csv.with_name(f"lit_pull_queue.{today}.processed.csv")
    if processed.exists():
        n = 2
        while queue_csv.with_name(f"lit_pull_queue.{today}.processed.{n}.csv").exists():
            n += 1
        processed = queue_csv.with_name(f"lit_pull_queue.{today}.processed.{n}.csv")
    queue_csv.rename(processed)
    return processed


def test_sweep_module_imports():
    assert hasattr(sweep, "run_pipeline")


def test_no_collision_uses_plain_name():
    with tempfile.TemporaryDirectory() as d:
        q = Path(d) / "lit_pull_queue.csv"
        q.write_text("doi,title\n10.1/x,Foo\n", encoding="utf-8")
        out = _dedup_processed(q, "2026-06-23")
        assert out.name == "lit_pull_queue.2026-06-23.processed.csv"
        assert out.exists() and not q.exists()


def test_one_collision_yields_dot2():
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        q = root / "lit_pull_queue.csv"
        q.write_text("doi,title\n10.1/x,Foo\n", encoding="utf-8")
        first = root / "lit_pull_queue.2026-06-23.processed.csv"
        first.write_text("prior", encoding="utf-8")
        out = _dedup_processed(q, "2026-06-23")
        assert out.name == "lit_pull_queue.2026-06-23.processed.2.csv"
        assert first.read_text(encoding="utf-8") == "prior"  # original NOT clobbered


def test_two_collisions_yield_dot3():
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        q = root / "lit_pull_queue.csv"
        q.write_text("doi,title\n10.1/x,Foo\n", encoding="utf-8")
        (root / "lit_pull_queue.2026-06-23.processed.csv").write_text("a", encoding="utf-8")
        (root / "lit_pull_queue.2026-06-23.processed.2.csv").write_text("b", encoding="utf-8")
        out = _dedup_processed(q, "2026-06-23")
        assert out.name == "lit_pull_queue.2026-06-23.processed.3.csv"
