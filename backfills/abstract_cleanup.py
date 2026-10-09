"""Clean the abstracts stored in an index database with the current abstract cleaner. Dry run by default.
(Maintainer refs: W5-B; issue rows M282, C046.)

`enrich_abstracts` stored Crossref abstracts with an older cleaner that decoded three character
references only, so `paper_metadata.abstract` still holds JATS markup (`<jats:p>`), escaped text
(`&lt;`, `&amp;amp;lt;`) and leading "Abstract" headings. `litpipe.text.abstract_field` (W2-E2) is
the one cleaner now (the enrich step and every `.ris` AB line use it); this script runs it over the
stored rows. `top_candidates` and `papers` are views over `paper_metadata`, so they follow.

The database is named with `--db` (required; there is no default). The dry run opens it with
`read_only=True` and writes only the diff report. `--commit`:
  * refuses the configured live database (`litpipe.config.db_dir()/portfolio.duckdb`) unless
    `--i-made-a-copy` is passed, and prints the command that makes the copy; the intended use is to
    run it on a copy first, check the report, then on the live file;
  * refuses a database with a write-ahead log beside it (`<db>.wal`: another process has it open,
    or it was not closed cleanly);
  * copies the file byte for byte to `<db>.bak-w5b` first unless `--no-backup`;
  * updates the changed rows in one transaction, by DOI, from a temporary table loaded with
    read_csv (not executemany); an abstract the cleaner empties (a lone heading) is stored as '',
    which the enrich step reads as missing.

The diff report (path, field = `paper_metadata.abstract[<doi>]`, before excerpt, after excerpt,
rule) goes to `--report` (default `abstract_cleanup_<date>.csv` in the current directory).

Usage:
  python backfills/abstract_cleanup.py --db PATH [--commit [--i-made-a-copy]] [--report CSV]
                                       [--no-backup] [--show N]
Exit codes: 0 done; 1 usage or configuration (no --db, a missing file, the live file without
--i-made-a-copy, a .wal beside it); 2 the database could not be opened or written.
"""
from __future__ import annotations

import argparse
import csv
import datetime as _dt
import json
import os
import re
import shutil
import sys
import tempfile
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import lit_util  # noqa: E402
from litpipe import config  # noqa: E402
from litpipe import text as _text  # noqa: E402

STEP = "abstract_cleanup"
DB_NAME = "portfolio.duckdb"
BACKUP_SUFFIX = ".bak-w5b"
REPORT_COLUMNS = ["path", "field", "before", "after", "rule"]
WINDOW = 160


# ---------------------------------------------------------------- shared helpers (one copy per W5-B script)
_ADDRESS = re.compile(r"(?i)([A-Za-z0-9._+-])[A-Za-z0-9._+-]*(@|%(?:25)*40)"
                      r"([A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]+)")


def mask(s) -> str:
    """`s` with every email address shown as its first character, `***` and its domain."""
    return _ADDRESS.sub(lambda m: m.group(1) + "***" + m.group(2) + m.group(3), str(s or ""))


def excerpts(before, after, width=WINDOW):
    """(before, after): addresses masked, then a window of `width` characters starting a little
    before the first difference; line breaks shown as \\n."""
    b, a = mask(before), mask(after)
    i, n = 0, min(len(b), len(a))
    while i < n and b[i] == a[i]:
        i += 1
    start = max(0, i - 40)

    def cut(s):
        w = s[start:start + width].replace("\r", "\\r").replace("\n", "\\n")
        return ("..." if start else "") + w + ("..." if start + width < len(s) else "")
    return cut(b), cut(a)


def report_path(report, step):
    if report:
        return Path(report)
    base = Path.cwd() / f"{step}_{_dt.date.today().isoformat()}"
    p, n = base.with_name(base.name + ".csv"), 1
    while p.exists():
        n += 1
        p = base.with_name(f"{base.name}.{n}.csv")
    return p


def write_report(path, rows):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    lit_util.atomic_write_csv(str(path), rows, REPORT_COLUMNS)


def backup_path(path) -> Path:
    path = Path(path)
    b, n = path.with_name(path.name + BACKUP_SUFFIX), 0
    while b.exists():
        n += 1
        b = path.with_name(f"{path.name}{BACKUP_SUFFIX}.{n}")
    return b


def summary_line(res) -> str:
    keep = {k: v for k, v in res.items() if isinstance(v, (int, float, str, bool)) or v is None}
    return "[step-summary] " + json.dumps(keep, ensure_ascii=False)


# ---------------------------------------------------------------- the database
def live_db_path(cfg=None) -> Path:
    return config.db_dir(cfg) / DB_NAME


def same_file(a, b) -> bool:
    try:
        return os.path.samefile(a, b)
    except OSError:
        return os.path.normcase(os.path.abspath(a)) == os.path.normcase(os.path.abspath(b))


def rule_for(before: str) -> str:
    from audit_portfolio import text_damage
    kinds = text_damage(before)
    return "abstract_field: " + ("+".join(kinds) if kinds else "other (heading, spacing or NFC)")


def changes(con):
    """[(doi, before, after)] for every stored abstract the cleaner changes."""
    out = []
    for doi, ab in con.execute("SELECT doi, abstract FROM paper_metadata "
                               "WHERE abstract IS NOT NULL AND abstract <> '' ORDER BY doi").fetchall():
        new = _text.abstract_field(ab)
        if new != ab:
            out.append((doi, ab, new))
    return out


def _apply(con, rows) -> int:
    """Update `rows` [(doi, before, after)] in one transaction via a temp table loaded from a CSV."""
    fd, tmp = tempfile.mkstemp(suffix=".csv", prefix="w5b-abstracts-")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(["doi", "abstract"])
            w.writerows((d, a) for d, _b, a in rows)
        con.execute("BEGIN TRANSACTION")
        try:
            con.execute("CREATE TEMP TABLE _w5b_abstract AS SELECT * FROM read_csv(?, header=true, "
                        "delim=',', quote='\"', escape='\"', "
                        "columns={'doi': 'VARCHAR', 'abstract': 'VARCHAR'})", [tmp])
            n_tmp = con.execute("SELECT COUNT(*) FROM _w5b_abstract").fetchone()[0]
            if n_tmp != len(rows):
                raise RuntimeError(f"temp table holds {n_tmp} rows, expected {len(rows)}")
            con.execute("UPDATE paper_metadata SET abstract = coalesce(t.abstract, '') FROM _w5b_abstract t "
                        "WHERE paper_metadata.doi = t.doi")
            con.execute("DROP TABLE _w5b_abstract")
            con.execute("COMMIT")
        except BaseException:
            con.execute("ROLLBACK")
            raise
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)
    return len(rows)


def run(*, db=None, commit=False, i_made_a_copy=False, report=None, backup=True, show=10, cfg=None) -> dict:
    res = {"step": STEP, "exit_code": 0, "commit": bool(commit), "db": str(db) if db else None,
           "abstracts": 0, "changed": 0, "emptied": 0, "updated": 0, "backup": None, "report": None,
           "rules": {}}

    def fail(code, msg):
        print(f"[ERR] {msg}", file=sys.stderr)
        res.update(exit_code=code, error=msg)
        return res

    if not db:
        return fail(1, "--db is required (there is no default database)")
    db = Path(db)
    if not db.is_file():
        return fail(1, f"no database file at {db}")
    rpath = report_path(report, STEP)
    live = live_db_path(cfg)
    is_live = live.exists() and same_file(db, live)
    res["live"] = is_live
    if commit:
        if is_live and not i_made_a_copy:
            copy = db.with_name(db.stem + ".pre-w5b" + db.suffix)
            print("[ERR] --commit names the live index database. Make a copy first, run this on the copy, "
                  "check the report, then pass --i-made-a-copy to run it on the live file:", file=sys.stderr)
            print(f'  copy "{db}" "{copy}"   (Windows)' if os.name == "nt" else f'  cp "{db}" "{copy}"',
                  file=sys.stderr)
            res.update(exit_code=1, error="live database without --i-made-a-copy", copy_command=str(copy))
            return res
        wal = Path(str(db) + ".wal")
        if wal.exists():
            return fail(1, f"{wal} exists: another process has the database open, or it was not closed "
                           "cleanly; close it (or let the index finish) and run again")
    import duckdb
    try:
        con = duckdb.connect(str(db), read_only=not commit)
    except Exception as e:                       # duckdb's lock and IO errors have no stable class
        return fail(2, f"cannot open {db}: {type(e).__name__}: {mask(e)}")
    try:
        res["abstracts"] = con.execute("SELECT COUNT(*) FROM paper_metadata "
                                       "WHERE abstract IS NOT NULL AND abstract <> ''").fetchone()[0]
        rows = changes(con)
        res["changed"] = len(rows)
        res["emptied"] = sum(1 for _d, _b, a in rows if not a)
        rules = Counter()
        report_rows = []
        for d, b, a in rows:
            rule = rule_for(b) + ("; emptied (a heading only)" if not a else "")
            rules[rule.split(";")[0]] += 1
            eb, ea = excerpts(b, a)
            report_rows.append({"path": str(db), "field": f"paper_metadata.abstract[{d}]", "before": eb,
                                "after": ea, "rule": rule})
        res["rules"] = dict(rules)
        write_report(rpath, report_rows)
        res["report"] = str(rpath)
        for row in report_rows[:max(0, int(show))]:
            print(f"  {row['field']}: {row['before'][:70]!r} => {row['after'][:70]!r}")
        if commit and rows:
            con.close()
            con = None
            if backup:
                bak = backup_path(db)
                shutil.copy2(db, bak)
                res["backup"] = str(bak)
            con = duckdb.connect(str(db), read_only=False)
            res["updated"] = _apply(con, rows)
            left = changes(con)
            res["left_after_update"] = len(left)
    except Exception as e:
        return fail(2, f"{db}: {type(e).__name__}: {mask(e)}")
    finally:
        if con is not None:
            con.close()
    _print(res)
    return res


def _print(res):
    print(f"database: {res['db']}{' (the configured live index)' if res.get('live') else ''}")
    print(f"stored abstracts: {res['abstracts']}; changed by abstract_field: {res['changed']} "
          f"({res['emptied']} emptied: a heading only)")
    for rule, n in sorted(res["rules"].items(), key=lambda kv: -kv[1]):
        print(f"  {n:>7}  {rule}")
    if res["commit"]:
        print(f"updated {res['updated']} rows" + (f"; backup {res['backup']}" if res["backup"] else ""))
    else:
        print("Dry run: the database was opened read-only; nothing written but the report.")
    print(f"report: {res['report']}")
    print(summary_line(res))


def main(argv=None) -> int:
    lit_util.utf8_stdout()
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--db", default=None, help="the DuckDB index file (required; no default)")
    ap.add_argument("--commit", action="store_true", help="write the cleaned abstracts; default: dry run")
    ap.add_argument("--i-made-a-copy", action="store_true",
                    help="allow --commit on the configured live portfolio.duckdb (after a copy was made)")
    ap.add_argument("--report", default=None,
                    help="the diff report CSV (default: abstract_cleanup_<date>.csv in the current directory)")
    ap.add_argument("--no-backup", action="store_true", help="do not copy the database to <db>.bak-w5b first")
    ap.add_argument("--show", type=int, default=10, help="print the first N changes (default 10)")
    a = ap.parse_args(argv)
    return run(db=a.db, commit=a.commit, i_made_a_copy=a.i_made_a_copy, report=a.report,
               backup=not a.no_backup, show=a.show)["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
