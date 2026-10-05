"""Remove citation edges whose seed DOI is a template placeholder from a forward-citations CSV.

One research project's `_forward_citations.csv` carries 660 rows whose `seed_doi` is
`10.1145/nnnnnnn.nnnnnnn`, the ACM acmart template default that a seed PDF printed as its own DOI
(V4-N3; refactor scope 3.4 "Data cleanup"). Every such row is a spurious edge in the citation graph.

Dry run by default: list the rows whose `seed_doi` is non-empty and `litpipe.doi.is_placeholder`
calls a placeholder (or a truncated DOI), with their line numbers, and the counts by seed DOI.
An empty `seed_doi` is never listed or removed (`is_placeholder("")` is True: it means "unknown",
not "placeholder"). Nothing is written, and the `--commit` command is printed for the owner.

`--commit` removes exactly those rows. It first copies the file to `<name>.bak` (`.bak.1`,
`.bak.2`, ... when one exists; a backup is never overwritten), then rewrites the CSV atomically.
Every kept record keeps its original bytes (quoting, line endings, embedded newlines): the file is
filtered record by record, never re-serialised.

Usage:
  python backfills/fred_placeholder_edges.py PATH/_forward_citations.csv            # dry run
  python backfills/fred_placeholder_edges.py PATH/_forward_citations.csv --commit   # owner only
  [--list-out CSV]  write the listed rows (line, seed_pdf, seed_doi, citing_doi) to a CSV
  [--show N]        print the first N listed rows (default 20)

Exit codes: 0 done (a dry run or a commit), 1 usage error (no such file, no seed_doi column).
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import os
import shutil
import sys
import tempfile
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import lit_util  # noqa: E402
from litpipe import doi as _doi  # noqa: E402

lit_util.utf8_stdout()

SEED_COLUMN = "seed_doi"
STEP = "placeholder_edges"
LIST_COLUMNS = ["line", "seed_pdf", "seed_doi", "citing_doi"]


def is_placeholder_seed(value) -> bool:
    """True for a non-empty seed DOI that litpipe.doi.is_placeholder flags; never for ''."""
    v = (value or "").strip()
    return bool(v) and _doi.is_placeholder(v.lower())


def records(text: str):
    """Yield (first physical line number, raw record text, parsed fields) for each CSV record of
    `text`, the raw text exactly as it appears (its line ending included), so a record that holds a
    quoted newline stays one record."""
    # Split on "\n" only (str.splitlines would also split on U+2028, \x0c, ... inside a field).
    parts = text.split("\n")
    lines = [p + "\n" for p in parts[:-1]] + ([parts[-1]] if parts[-1] else [])
    consumed: list[str] = []
    pos = {"i": 0}

    def feed():
        while pos["i"] < len(lines):
            line = lines[pos["i"]]
            pos["i"] += 1
            consumed.append(line)
            yield line

    start = 1
    for fields in csv.reader(feed()):
        raw = "".join(consumed)
        consumed.clear()
        yield start, raw, fields
        start = pos["i"] + 1


def _backup_path(path: Path) -> Path:
    b = path.with_name(path.name + ".bak")
    n = 0
    while b.exists():
        n += 1
        b = path.with_name(f"{path.name}.bak.{n}")
    return b


def run(*, csv_path, commit=False, list_out=None, show=20) -> dict:
    path = Path(csv_path)
    if not path.is_file():
        print(f"[ERR] not a file: {path}", file=sys.stderr)
        return {"step": STEP, "exit_code": 1, "error": f"not a file: {path}"}
    data = path.read_bytes()
    bom = data.startswith(b"\xef\xbb\xbf")
    text = data[3:].decode("utf-8") if bom else data.decode("utf-8")
    it = records(text)
    try:
        _, header_raw, header = next(it)
    except StopIteration:
        print(f"[ERR] empty file: {path}", file=sys.stderr)
        return {"step": STEP, "exit_code": 1, "error": "empty file"}
    if SEED_COLUMN not in header:
        print(f"[ERR] {path.name} has no {SEED_COLUMN} column", file=sys.stderr)
        return {"step": STEP, "exit_code": 1, "error": f"no {SEED_COLUMN} column"}
    col = header.index(SEED_COLUMN)
    pdf_col = header.index("seed_pdf") if "seed_pdf" in header else None
    cite_col = header.index("citing_doi") if "citing_doi" in header else None

    kept_parts = [header_raw]
    listed, by_seed = [], Counter()
    n_rows = n_empty = 0
    for line, raw, fields in it:
        n_rows += 1
        seed = fields[col] if col < len(fields) else ""
        if not seed.strip():
            n_empty += 1
        if is_placeholder_seed(seed):
            by_seed[seed.strip()] += 1
            listed.append({"line": line, "seed_pdf": fields[pdf_col] if pdf_col is not None and pdf_col < len(fields) else "",
                           "seed_doi": seed, "citing_doi": fields[cite_col] if cite_col is not None and cite_col < len(fields) else ""})
            continue
        kept_parts.append(raw)

    print(f"file:            {path}")
    print(f"rows:            {n_rows} ({n_empty} with an empty seed_doi: kept, never removed)")
    print(f"placeholder:     {len(listed)} rows to remove, {n_rows - len(listed)} kept")
    for seed, n in by_seed.most_common():
        pdfs = sorted({r["seed_pdf"] for r in listed if r["seed_doi"].strip() == seed})
        print(f"  {seed!r}: {n} rows from {len(pdfs)} seed PDF(s): {', '.join(pdfs[:5])}"
              + (" ..." if len(pdfs) > 5 else ""))
    for r in listed[:max(0, int(show))]:
        print(f"  line {r['line']:>7}  {r['seed_pdf'][:60]:<60}  {r['citing_doi']}")
    if len(listed) > show:
        print(f"  ... {len(listed) - show} more")
    if list_out:
        lit_util.atomic_write_csv(str(list_out), listed, LIST_COLUMNS)
        print(f"listed rows:     {list_out}")

    res = {"step": STEP, "exit_code": 0, "file": str(path), "rows": n_rows,
           "empty_seed_rows": n_empty, "placeholder_rows": len(listed),
           "by_seed": dict(by_seed), "committed": False, "backup": None}
    if not commit:
        print("\nDry run: nothing written. To remove these rows (writes a .bak first), the owner runs:")
        print(f'  uv run --project "{REPO}" python "{Path(__file__).resolve()}" "{path}" --commit')
        print("[step-summary] " + json.dumps({k: v for k, v in res.items() if k != "by_seed"}))
        return res
    if not listed:
        print("\nNothing to remove; the file is unchanged.")
        print("[step-summary] " + json.dumps({k: v for k, v in res.items() if k != "by_seed"}))
        return res
    backup = _backup_path(path)
    shutil.copy2(path, backup)
    out = ("﻿" if bom else "") + "".join(kept_parts)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(out.encode("utf-8"))
        lit_util._replace_with_retry(tmp, str(path))
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)
    res.update(committed=True, backup=str(backup))
    print(f"\nRemoved {len(listed)} rows; backup: {backup}")
    print("[step-summary] " + json.dumps({k: v for k, v in res.items() if k != "by_seed"}))
    return res


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("csv_path", help="a forward-citations CSV with a seed_doi column")
    ap.add_argument("--commit", action="store_true",
                    help="remove the listed rows (a .bak copy is written first); default: dry run")
    ap.add_argument("--list-out", default=None, help="write the listed rows to this CSV")
    ap.add_argument("--show", type=int, default=20, help="print the first N listed rows (default 20)")
    args = ap.parse_args(argv)
    return run(csv_path=args.csv_path, commit=args.commit, list_out=args.list_out, show=args.show)["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
