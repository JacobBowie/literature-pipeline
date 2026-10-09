"""Normalise the text stored in `.fulltext.json` sidecars with the extractor's current rules. Dry run by default.
(Maintainer refs: W5-B; issue row M151.)

The PDF extractor and import_downloads now pass every text through
`pdf_text_clean.clean_pdf_text` (ligatures U+FB00 to U+FB06 expanded; no-break, figure, thin and
narrow no-break spaces made plain; the zero-width space dropped). Sidecars written before that, and
the import_downloads bypass (37 sidecars on 2026-09-22), still hold `\\ufb01` and U+00A0, which
defeat a search for "reflex" or "10 mg" in the index. This script applies exactly that function,
with its default switches (the destructive options stay off), to the text fields only:
  * top level: title, subtitle, abstract, text, body;
  * nested: sections[].title and .text, figures[].caption, tables[].caption and .text.
Every other key keeps its value (the extractor, provenance, OCR and identity fields included), and
a `repaired_by` note is added (appended after "; " when one exists). The layout of the JSON is kept
as far as json.dumps allows (indent, ASCII escaping, the final newline, CRLF): untouched keys keep
their values exactly. Tags (`<i>`, 341 sidecars on 2026-10-07) are out of scope: clean_pdf_text
does not strip them, and a JATS text can carry a literal "<".

Skipped: a sidecar whose identity verdict is FLAG or a SUPPLEMENT (itself, or `<stem>.identity.json`;
audit_portfolio.identity_flag): a review item, not a holding. An unreadable sidecar is listed and
counted as an error.

Writes: `--commit` replaces each changed file atomically (a temp file beside it, then os.replace)
after copying it byte for byte to `<name>.bak-w5b` (`.bak-w5b.1`, ... when one exists) unless
`--no-backup`. The diff report (path, field, before excerpt, after excerpt, rule) goes to
`--report` (default `sidecar_text_repair_<date>.csv` in the current directory; refused inside a
library).

Usage:
  python backfills/sidecar_text_repair.py [--project KEY ...] [--lib-dir DIR ...] [--commit]
                                          [--report CSV] [--no-backup] [--show N]
Exit codes: 0 done; 1 usage or configuration; 2 done, but a file could not be read or written.
"""
from __future__ import annotations

import argparse
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
from pdf_text_clean import LIGATURES, SPACES, clean_pdf_text  # noqa: E402

STEP = "sidecar_text_repair"
SIDECAR = ".fulltext.json"
BACKUP_SUFFIX = ".bak-w5b"
REPORT_COLUMNS = ["path", "field", "before", "after", "rule"]
WINDOW = 160
TOP_FIELDS = ("title", "subtitle", "abstract", "text", "body")
NESTED_FIELDS = {"sections": ("title", "text"), "figures": ("caption",), "tables": ("caption", "text")}
# audit_portfolio's census keys (its _SIDECAR_TEXT_KEYS): the damage counts compare with its numbers.
CENSUS_FIELDS = ("title", "abstract", "text", "body")
NOTE = ("sidecar_text_repair {date}: pdf_text_clean.clean_pdf_text (ligatures U+FB00-FB06; no-break, "
        "figure, thin and narrow no-break spaces; zero-width space) on the text fields")
_LIG = frozenset(chr(c) for c in LIGATURES)
_SPACE_KINDS = {"\u00a0": "nbsp", "\u2007": "space", "\u2009": "space", "\u202f": "space", "\u200b": "zero-width"}


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
        w = "".join(f"<U+{ord(c):04X}>" if c in _LIG or c in _SPACE_KINDS else c for c in w)
        return ("..." if start else "") + w + ("..." if start + width < len(s) else "")
    return cut(b), cut(a)


def select_libraries(projects=None, lib_dirs=None, cfg=None):
    """[(label, library Path)]: each --lib-dir, each --project (registry keys), else every active
    registered project with a lib_dir. ConfigError for an unknown project."""
    out = [(str(Path(d)), Path(d)) for d in (lib_dirs or ())]
    if lib_dirs and not projects:
        return out
    reg = config.load(cfg)
    known = reg.get("projects") or {}
    if projects:
        unknown = [k for k in projects if k not in known]
        if unknown:
            raise config.ConfigError(f"not registered in projects.json: {', '.join(unknown)}")
        keys = list(dict.fromkeys(projects))
    else:
        keys = [k for k, p in known.items() if isinstance(p, dict) and p.get("active", True)]
    for k in keys:
        p = known.get(k)
        if isinstance(p, dict) and p.get("lib_dir"):
            out.append((k, lit_util.lib_paths(k, p)[1]))
    return out


def _inside(path, directory) -> bool:
    p = os.path.normcase(os.path.abspath(path))
    d = os.path.normcase(os.path.abspath(directory))
    return p == d or p.startswith(d.rstrip("\\/") + os.sep)


def report_path(report, step, libs):
    """The report path: `report`, else `<cwd>/<step>_<date>.csv` (`.2`, `.3`, ... when taken).
    ConfigError when it would land inside a library."""
    if report:
        p = Path(report)
    else:
        base = Path.cwd() / f"{step}_{_dt.date.today().isoformat()}"
        p, n = base.with_name(base.name + ".csv"), 1
        while p.exists():
            n += 1
            p = base.with_name(f"{base.name}.{n}.csv")
    for _label, lib in libs:
        if _inside(p, lib):
            raise config.ConfigError(f"the report {p} would be written inside the library {lib}; "
                                     "pass --report with a path outside every library")
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


# ---------------------------------------------------------------- JSON layout
def json_layout(raw_text: str) -> dict:
    """The json.dumps arguments that reproduce a file's layout as closely as possible."""
    m = re.match(r"\{\r?\n([ \t]+)\"", raw_text)
    indent = (m.group(1) if "\t" in m.group(1) else len(m.group(1))) if m else None
    non_ascii = any(ord(c) > 127 for c in raw_text)
    return {"indent": indent, "ensure_ascii": (not non_ascii) and "\\u" in raw_text,
            "crlf": "\r\n" in raw_text, "final_newline": raw_text.endswith("\n")}


def dump(obj, layout) -> str:
    s = json.dumps(obj, indent=layout["indent"], ensure_ascii=layout["ensure_ascii"])
    if layout["final_newline"]:
        s += "\n"
    if layout["crlf"]:
        s = s.replace("\n", "\r\n")
    return s


def add_note(rec: dict, note: str) -> None:
    old = rec.get("repaired_by")
    rec["repaired_by"] = f"{old}; {note}" if isinstance(old, str) and old.strip() else note


# ---------------------------------------------------------------- the repair
def _kinds(s: str) -> list:
    out = []
    if any(c in _LIG for c in s):
        out.append("ligature")
    for ch, k in _SPACE_KINDS.items():
        if ch in s and k not in out:
            out.append(k)
    return out


def text_fields(rec: dict):
    """Yield (field label, getter, setter) for every string text field of a sidecar dict."""
    for k in TOP_FIELDS:
        if isinstance(rec.get(k), str):
            yield k, (lambda k=k: rec[k]), (lambda v, k=k: rec.__setitem__(k, v))
    for list_key, keys in NESTED_FIELDS.items():
        items = rec.get(list_key)
        if not isinstance(items, list):
            continue
        for i, item in enumerate(items):
            if not isinstance(item, dict):
                continue
            for k in keys:
                if isinstance(item.get(k), str):
                    yield (f"{list_key}[{i}].{k}", (lambda it=item, k=k: it[k]),
                           (lambda v, it=item, k=k: it.__setitem__(k, v)))


def census(rec: dict) -> list:
    """audit_portfolio's damage kinds that this repair addresses (ligature, nbsp) in its census keys."""
    s = "\n".join(rec[k] for k in CENSUS_FIELDS if isinstance(rec.get(k), str))
    out = []
    if any(c in _LIG for c in s):
        out.append("ligature")
    if "\u00a0" in s:
        out.append("nbsp")
    return out


def _flag(path: Path, rec) -> str:
    from audit_portfolio import identity_flag, read_json
    why = identity_flag(rec)
    if why:
        return f"{path.name}: {why}"
    ids = path.with_name(path.name[: -len(SIDECAR)] + ".identity.json")
    if ids.is_file():
        d, _err = read_json(ids)
        why = identity_flag(d)
        if why:
            return f"{ids.name}: {why}"
    return ""


def repair_file(path: Path, commit=False, backup=True, today=None):
    res = {"path": str(path), "status": "unchanged", "rows": [], "reason": "", "census": [], "backup": None,
           "layout_kept": None}
    try:
        raw = path.read_bytes()
        raw_text = raw.decode("utf-8-sig")
        rec = json.loads(raw_text)
    except (OSError, ValueError) as e:
        res.update(status="error", reason=f"unreadable: {type(e).__name__}")
        return res
    if not isinstance(rec, dict):
        res.update(status="error", reason="not a JSON object")
        return res
    res["census"] = census(rec)
    flag = _flag(path, rec)
    if flag:
        res.update(status="skipped", reason=f"identity flag ({flag})")
        return res
    layout = json_layout(raw_text)
    new = json.loads(raw_text)                     # an independent copy to edit
    for field, get, put in text_fields(new):
        before = get()
        after = clean_pdf_text(before)
        if after == before:
            continue
        put(after)
        b, a = excerpts(before, after)
        res["rows"].append({"path": str(path), "field": field, "before": b, "after": a,
                            "rule": "clean_pdf_text: " + "+".join(_kinds(before))})
    if not res["rows"]:
        return res
    add_note(new, NOTE.format(date=(today or _dt.date.today()).isoformat()))
    res["layout_kept"] = dump(rec, layout).encode("utf-8") == raw.removeprefix(b"\xef\xbb\xbf")
    out = dump(new, layout).encode("utf-8")
    if raw.startswith(b"\xef\xbb\xbf"):
        out = b"\xef\xbb\xbf" + out
    res["status"] = "changed"
    if commit:
        try:
            res["backup"] = replace_bytes(path, out, backup=backup)
        except OSError as e:
            res.update(status="error", reason=f"write failed: {type(e).__name__}: {e}")
    return res


def run(*, projects=None, lib_dirs=None, commit=False, report=None, backup=True, show=20, cfg=None) -> dict:
    res = {"step": STEP, "exit_code": 0, "commit": bool(commit), "libraries": 0, "sidecars": 0,
           "census_ligature": 0, "census_nbsp": 0, "changed_files": 0, "changed_fields": 0,
           "skipped_flag": 0, "layout_changed": 0, "errors": 0, "report": None,
           "per_library": {}, "kinds": {}, "error_list": []}
    try:
        libs = select_libraries(projects, lib_dirs, cfg)
        if not libs:
            raise config.ConfigError("no library selected (no --lib-dir, and no active project with a lib_dir)")
        rpath = report_path(report, STEP, libs)
    except config.ConfigError as e:
        print(f"[ERR] {e}", file=sys.stderr)
        res.update(exit_code=1, error=str(e))
        return res
    rows, kinds, shown = [], Counter(), 0
    for label, lib in libs:
        per = Counter()
        if not lib.is_dir():
            print(f"  [skip] {label}: no library at {lib}", file=sys.stderr)
            res["per_library"][label] = {"missing": 1}
            continue
        res["libraries"] += 1
        try:
            names = sorted(e.name for e in os.scandir(lib) if e.is_file() and e.name.lower().endswith(SIDECAR))
        except OSError as e:
            res["errors"] += 1
            res["error_list"].append(f"{lib}: {type(e).__name__}")
            continue
        for name in names:
            r = repair_file(lib / name, commit=commit, backup=backup)
            per["sidecars"] += 1
            per["census_ligature"] += "ligature" in r["census"]
            per["census_nbsp"] += "nbsp" in r["census"]
            if r["status"] == "changed":
                per["changed"] += 1
                per["layout_changed"] += r["layout_kept"] is False
                rows.extend(r["rows"])
                for row in r["rows"]:
                    for k in row["rule"].split(": ", 1)[1].split("+"):
                        kinds[k] += 1
                if shown < show:
                    shown += 1
                    print(f"  {'fixed' if commit else 'would fix'} {label}: {name} ({len(r['rows'])} field(s))")
            elif r["status"] == "skipped":
                per["skipped_flag"] += 1
            elif r["status"] == "error":
                per["errors"] += 1
                res["error_list"].append(f"{lib / name}: {r['reason']}")
        res["per_library"][label] = dict(per)
        for k_res, k_per in (("sidecars", "sidecars"), ("census_ligature", "census_ligature"),
                             ("census_nbsp", "census_nbsp"), ("changed_files", "changed"),
                             ("skipped_flag", "skipped_flag"), ("layout_changed", "layout_changed"),
                             ("errors", "errors")):
            res[k_res] += per[k_per]
    res["changed_fields"] = len(rows)
    res["kinds"] = dict(kinds)
    write_report(rpath, rows)
    res["report"] = str(rpath)
    if res["errors"]:
        res["exit_code"] = 2
    _print(res)
    return res


def _print(res):
    print(f"\n{'project':<40} {'sidecars':>8} {'lig':>5} {'nbsp':>5} {'change':>6} {'flag':>5} {'error':>5}")
    for label, p in res["per_library"].items():
        if p.get("missing"):
            print(f"{label[:40]:<40} (no library)")
            continue
        print(f"{label[:40]:<40} {p.get('sidecars', 0):>8} {p.get('census_ligature', 0):>5} "
              f"{p.get('census_nbsp', 0):>5} {p.get('changed', 0):>6} {p.get('skipped_flag', 0):>5} "
              f"{p.get('errors', 0):>5}")
    print(f"total: {res['sidecars']} sidecars; census keys (title, abstract, text, body): "
          f"{res['census_ligature']} with a ligature, {res['census_nbsp']} with a no-break space; "
          f"{res['changed_files']} {'repaired' if res['commit'] else 'to repair'} ({res['changed_fields']} "
          f"fields: {', '.join(f'{k} {v}' for k, v in sorted(res['kinds'].items()))}); "
          f"{res['skipped_flag']} identity-flagged skipped; {res['layout_changed']} whose JSON layout "
          "json.dumps cannot reproduce (values kept)")
    for e in res["error_list"][:20]:
        print(f"  [error] {e}")
    print(f"report: {res['report']}")
    if not res["commit"]:
        print("Dry run: nothing written but the report. Add --commit to repair (a .bak-w5b copy of "
              "each file is kept beside it unless --no-backup).")
    print(summary_line(res))


def main(argv=None) -> int:
    lit_util.utf8_stdout()
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--project", action="append", default=None, metavar="KEY",
                    help="a registered project (repeatable); default: every active project")
    ap.add_argument("--lib-dir", action="append", default=None, metavar="DIR",
                    help="a library directory (repeatable), instead of or besides --project")
    ap.add_argument("--commit", action="store_true", help="write the repairs; default: dry run")
    ap.add_argument("--report", default=None,
                    help="the diff report CSV (default: sidecar_text_repair_<date>.csv in the current directory)")
    ap.add_argument("--no-backup", action="store_true", help="do not keep <name>.bak-w5b copies")
    ap.add_argument("--show", type=int, default=20, help="print the first N files (default 20)")
    a = ap.parse_args(argv)
    return run(projects=a.project, lib_dirs=a.lib_dir, commit=a.commit, report=a.report,
               backup=not a.no_backup, show=a.show)["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
