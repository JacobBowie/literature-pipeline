"""Scrub the contact address out of the reports the pipeline wrote before every persisted string
went through `litpipe.ledger.redact` (W5-B; issue rows M020, M221, decision D-e). Dry run by default.

Before W1, Unpaywall error strings carried the request URL, `email=` included, into the stage
reports and from there into a residual `.md` worklist. This script replaces only the LEAK forms,
with the tokens `redact` uses:
  * an `email=` query value (the key goes with it): `[EMAIL-REDACTED]`;
  * a `mailto:` value: `[MAILTO-REDACTED]`;
  * the configured contact address (LITPIPE_EMAIL): `REDACTED`;
in every encoding `redact` handles (percent-encoded at any depth, `+` as a space, any case), using
`litpipe.ledger`'s own patterns. Any other address is left alone: consumer notes, CLAUDE.md files
and data CSVs legitimately hold addresses.

Which files: PIPELINE-WRITTEN reports only, selected by name and never by content:
`lit_pull_queue*.csv`, `lit_pull_queue*.md`, `*_report*.csv` and `_downloads_import_*.csv`, in each
selected project's root (top level) and anywhere under its `_archive/`, its library's `_archive/`
(one project keeps its old runs there) and, for a subproject, its parent's `_archive/` (where a
consumer keeps the dated sweep exhaust). A file not matched by name is never opened for writing;
nothing under `.git`, no library file (PDF, `.ris`, sidecar, any `.json`), nothing in a library's
top level, and never the registry.

The dry run prints files and occurrence counts only, never an address; the diff report (path, field
= `line N`, before excerpt, after excerpt, rule) masks every address to its first character and its
domain. `--commit` replaces each changed file atomically (a temp file beside it, then os.replace),
keeping the bytes otherwise identical (encoding, byte-order mark, line ends). A byte-identical
`<name>.bak-w5b` copy is kept beside it unless `--no-backup`; that copy still holds the address, so
delete it once the scrub is checked (git keeps the original of a tracked file anyway).

Usage:
  python backfills/email_scrub.py [--project KEY ...] [--commit] [--report CSV] [--no-backup]
Exit codes: 0 done; 1 usage or configuration; 2 done, but a file could not be read or written.
"""
from __future__ import annotations

import argparse
import datetime as _dt
import fnmatch
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
from litpipe import config, ledger  # noqa: E402

STEP = "email_scrub"
BACKUP_SUFFIX = ".bak-w5b"
REPORT_COLUMNS = ["path", "field", "before", "after", "rule"]
WINDOW = 160
NAME_PATTERNS = ("lit_pull_queue*.csv", "lit_pull_queue*.md", "*_report*.csv", "_downloads_import_*.csv")
NEVER_SUFFIXES = (".pdf", ".ris", ".json", ".xml")
RULE_PARAM, RULE_MAILTO, RULE_CONFIGURED = "email= value", "mailto: value", "configured address"


# ---------------------------------------------------------------- shared helpers (one copy per W5-B script)
_ADDRESS = re.compile(r"(?i)([A-Za-z0-9._+-])[A-Za-z0-9._+-]*(@|%(?:25)*40)"
                      r"([A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]+)")


def configured_emails() -> list:
    return list(ledger._configured_emails())


def mask(s) -> str:
    """`s` with the configured address (in each form redact knows) and every other email address
    shown as its first character, `***` and its domain."""
    s = str(s or "")
    for email in configured_emails():
        local, _, domain = email.partition("@")
        shown = (local[:1] + "***@" + domain) if domain else "***"
        for form in ledger._literal_forms(email):
            s = re.sub(re.escape(form), shown, s, flags=re.IGNORECASE)
    return _ADDRESS.sub(lambda m: m.group(1) + "***" + m.group(2) + m.group(3), s)


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


def _inside(path, directory) -> bool:
    p = os.path.normcase(os.path.abspath(path))
    d = os.path.normcase(os.path.abspath(directory))
    return p == d or p.startswith(d.rstrip("\\/") + os.sep)


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


def replace_bytes(path, data: bytes, backup=True):
    """Replace `path` with `data` atomically; with `backup`, first copy it byte for byte beside
    itself. Returns the backup path or None."""
    path = Path(path)
    bak = None
    if backup:
        bak = backup_path(path)
        shutil.copy2(path, bak)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=".w5b-", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        lit_util._replace_with_retry(tmp, str(path))
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)
    return str(bak) if bak else None


def summary_line(res) -> str:
    keep = {k: v for k, v in res.items() if isinstance(v, (int, float, str, bool)) or v is None}
    return "[step-summary] " + json.dumps(keep, ensure_ascii=False)


# ---------------------------------------------------------------- selection
def name_matches(name: str) -> bool:
    low = name.lower()
    if low.endswith(NEVER_SUFFIXES) or low == "projects.json":
        return False
    return any(fnmatch.fnmatchcase(low, pat) for pat in NAME_PATTERNS)


def select_projects(projects=None, cfg=None):
    reg = config.load(cfg)
    known = reg.get("projects") or {}
    if projects:
        unknown = [k for k in projects if k not in known]
        if unknown:
            raise config.ConfigError(f"not registered in projects.json: {', '.join(unknown)}")
        keys = list(dict.fromkeys(projects))
    else:
        keys = [k for k, p in known.items() if isinstance(p, dict) and p.get("active", True)]
    return [(k, known[k] if isinstance(known[k], dict) else {}) for k in keys], known


def scan_dirs(selected):
    """[(directory, recursive)]: each project root (top level); its `_archive/` and its library's
    `_archive/` (recursive: one project archives its old runs under `<library>/_archive/`); and a
    subproject's parent `_archive/` (recursive); each directory once."""
    out, seen = [], set()

    def add(d, rec):
        k = (os.path.normcase(os.path.abspath(d)), rec)
        if k not in seen:
            seen.add(k)
            out.append((Path(d), rec))
    for key, p in selected:
        root = lit_util.project_root(key, p)
        add(root, False)
        add(root / "_archive", True)
        if p.get("lib_dir"):
            add(lit_util.lib_paths(key, p)[1] / "_archive", True)
        if p.get("parent"):
            add(lit_util.PROJECTS_ROOT / p["parent"] / "_archive", True)
    return out


def candidate_files(dirs, libraries):
    """Report files matched by name in `dirs`; never a file in a library's top level (where the
    PDFs, sidecars and `.ris` live), never under `.git`."""
    seen = set()
    lib_keys = {os.path.normcase(os.path.abspath(lib)) for lib in libraries}
    for d, recursive in dirs:
        if not d.is_dir():
            continue
        if recursive:
            walker = os.walk(d)
        else:
            try:
                walker = [(str(d), [], [e.name for e in os.scandir(d) if e.is_file()])]
            except OSError:
                continue
        for dirpath, dirnames, filenames in walker:
            dirnames[:] = [x for x in dirnames if x != ".git"]
            if ".git" in Path(dirpath).parts:
                continue
            for fn in filenames:
                if not name_matches(fn):
                    continue
                p = Path(dirpath) / fn
                if os.path.normcase(os.path.abspath(p.parent)) in lib_keys:
                    continue
                k = os.path.normcase(os.path.abspath(p))
                if k not in seen:
                    seen.add(k)
                    yield p


# ---------------------------------------------------------------- the scrub
def scrub_line(line: str, counts: Counter) -> str:
    def param(m):
        counts[RULE_PARAM] += 1
        return ledger.EMAIL_TOKEN

    def mailto(m):
        counts[RULE_MAILTO] += 1
        return ledger.MAILTO_TOKEN
    s = ledger._EMAIL_PARAM_RE.sub(param, line)
    s = ledger._MAILTO_RE.sub(mailto, s)
    for email in configured_emails():
        for form in ledger._literal_forms(email):
            s, n = re.subn(re.escape(form), ledger.PLACEHOLDER, s, flags=re.IGNORECASE)
            counts[RULE_CONFIGURED] += n
    return s


def scrub_file(path: Path, commit=False, backup=True):
    res = {"path": str(path), "status": "clean", "rows": [], "counts": Counter(), "reason": "", "backup": None}
    try:
        raw = path.read_bytes()
    except OSError as e:
        res.update(status="error", reason=f"unreadable: {type(e).__name__}")
        return res
    bom = raw.startswith(b"\xef\xbb\xbf")
    try:
        text = raw[3:].decode("utf-8") if bom else raw.decode("utf-8")
    except UnicodeDecodeError:
        res.update(status="error", reason="not UTF-8 (left alone)")
        return res
    out_lines = []
    for n, line in enumerate(re.split(r"(?<=\n)", text), start=1):
        if not line:
            continue
        c = Counter()
        new = scrub_line(line, c)
        out_lines.append(new)
        if new != line:
            res["counts"].update(c)
            b, a = excerpts(line, new)
            res["rows"].append({"path": str(path), "field": f"line {n}", "before": b, "after": a,
                                "rule": "+".join(f"{k} x{v}" for k, v in sorted(c.items()) if v)})
    if not res["rows"]:
        return res
    data = (b"\xef\xbb\xbf" if bom else b"") + "".join(out_lines).encode("utf-8")
    res["status"] = "changed"
    if commit:
        try:
            res["backup"] = replace_bytes(path, data, backup=backup)
        except OSError as e:
            res.update(status="error", reason=f"write failed: {type(e).__name__}: {e}")
    return res


def run(*, projects=None, commit=False, report=None, backup=True, cfg=None) -> dict:
    res = {"step": STEP, "exit_code": 0, "commit": bool(commit), "configured_address": False,
           "dirs": 0, "files_matched": 0, "files_changed": 0, "occurrences": 0, "lines": 0, "errors": 0,
           "report": None, "by_rule": {}, "files": [], "backups": [], "error_list": []}
    try:
        selected, known = select_projects(projects, cfg)
        if not selected:
            raise config.ConfigError("no project selected (the registry lists no active project)")
        libraries = [lit_util.lib_paths(k, p)[1] for k, p in known.items()
                     if isinstance(p, dict) and p.get("lib_dir")]
        rpath = report_path(report, STEP)
        for lib in libraries:
            if _inside(rpath, lib):
                raise config.ConfigError(f"the report {rpath} would be written inside the library {lib}")
    except config.ConfigError as e:
        print(f"[ERR] {e}", file=sys.stderr)
        res.update(exit_code=1, error=str(e))
        return res
    res["configured_address"] = bool(configured_emails())
    if not res["configured_address"]:
        print("  [note] LITPIPE_EMAIL is not set: only email= and mailto: values are replaced", file=sys.stderr)
    dirs = scan_dirs(selected)
    res["dirs"] = sum(1 for d, _r in dirs if d.is_dir())
    rows, by_rule = [], Counter()
    for p in candidate_files(dirs, libraries):
        res["files_matched"] += 1
        r = scrub_file(p, commit=commit, backup=backup)
        if r["status"] == "error":
            res["errors"] += 1
            res["error_list"].append(f"{p}: {r['reason']}")
            continue
        if r["status"] != "changed":
            continue
        res["files_changed"] += 1
        rows.extend(r["rows"])
        by_rule.update(r["counts"])
        n = sum(r["counts"].values())
        res["files"].append({"path": str(p), "occurrences": n, "lines": len(r["rows"]),
                             **{k: v for k, v in r["counts"].items()}})
        if r["backup"]:
            res["backups"].append(r["backup"])
    res["by_rule"] = dict(by_rule)
    res["occurrences"] = sum(by_rule.values())
    res["lines"] = len(rows)
    write_report(rpath, rows)
    res["report"] = str(rpath)
    if res["errors"]:
        res["exit_code"] = 2
    _print(res)
    return res


def _print(res):
    for f in res["files"]:
        parts = ", ".join(f"{k} {f[k]}" for k in (RULE_PARAM, RULE_MAILTO, RULE_CONFIGURED) if f.get(k))
        print(f"  {f['path']}: {f['occurrences']} occurrence(s) on {f['lines']} line(s) ({parts})")
    print(f"total: {res['files_matched']} report file(s) matched by name in {res['dirs']} folder(s); "
          f"{res['files_changed']} {'scrubbed' if res['commit'] else 'to scrub'}: {res['occurrences']} "
          f"occurrence(s) on {res['lines']} line(s) "
          f"({', '.join(f'{k} {v}' for k, v in sorted(res['by_rule'].items())) or 'none'})")
    for e in res["error_list"][:20]:
        print(f"  [error] {e}")
    if res["backups"]:
        print(f"  {len(res['backups'])} .bak-w5b backup(s) still hold the address: delete them once the scrub "
              "is checked")
    print(f"report (addresses masked): {res['report']}")
    if not res["commit"]:
        print("Dry run: nothing written but the report. Add --commit to scrub.")
    print(summary_line({k: v for k, v in res.items() if k not in ("files", "backups")}))


def main(argv=None) -> int:
    lit_util.utf8_stdout()
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--project", action="append", default=None, metavar="KEY",
                    help="a registered project (repeatable); default: every active project")
    ap.add_argument("--commit", action="store_true", help="write the scrubbed files; default: dry run")
    ap.add_argument("--report", default=None,
                    help="the diff report CSV (default: email_scrub_<date>.csv in the current directory)")
    ap.add_argument("--no-backup", action="store_true",
                    help="do not keep <name>.bak-w5b copies (they still hold the address)")
    a = ap.parse_args(argv)
    return run(projects=a.project, commit=a.commit, report=a.report, backup=not a.no_backup)["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
