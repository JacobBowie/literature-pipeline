"""Give a `.fulltext.json` sidecar with no DOI the DOI its same-stem `.ris` records (W5-B; issue row
M222, "Gap2"). Dry run by default.

Since W4-C the extractor seeds a new sidecar's DOI from its `.ris` (extract_pdf_fulltext
finish_record); the sidecars written before that still have an empty `doi` while the `.ris` beside
them has a DO line (689 on 2026-09-23). For each top-level `<stem>.fulltext.json` whose `doi` is
missing or empty and whose `<stem>.ris` has a DO line:
  1. the DOI is `litpipe.doi.normalise_structured(DO)` (the structured-field rule: a registered
     letter-tail DOI is kept whole); a placeholder or malformed DO is skipped;
  2. a sidecar whose identity verdict is FLAG or a SUPPLEMENT (itself or `<stem>.identity.json`;
     audit_portfolio.identity_flag) is skipped: it is a review item, and its `.ris` may describe
     the queued paper rather than the file;
  3. `litpipe.identity.check` must pass: the DOI printed in the sidecar's first pages (the text up
     to its third form feed, else its first 12,000 characters) is OK; else the sidecar's `title`
     (queue title) or the `.ris` TI (display form) matching that text at 0.85 is TITLE_MATCH; FLAG
     is skipped and reported with the check's reason;
  4. the record is rebuilt through `lit_util.merge_sidecar(old, new)` (the pipeline's non-clobbering
     merge; every enriched field kept) with `doi` set and a `repaired_by` note, and written in the
     file's own JSON layout. Nothing else changes.

Writes: `--commit` replaces each changed sidecar atomically (a temp file beside it, then
os.replace) after copying it byte for byte to `<name>.bak-w5b` (`.bak-w5b.1`, ... when one exists)
unless `--no-backup`. The diff report (path, field, before, after, rule) goes to `--report`
(default `sidecar_doi_from_ris_<date>.csv` in the current directory; refused inside a library).

Usage:
  python backfills/sidecar_doi_from_ris.py [--project KEY ...] [--lib-dir DIR ...] [--commit]
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
from litpipe import config, identity  # noqa: E402
from litpipe import doi as _doi  # noqa: E402
from litpipe import text as _text  # noqa: E402

STEP = "sidecar_doi_from_ris"
SIDECAR = ".fulltext.json"
BACKUP_SUFFIX = ".bak-w5b"
REPORT_COLUMNS = ["path", "field", "before", "after", "rule"]
WINDOW = 160
FIRST_PAGES = 3
HEAD_CHARS = 12_000
NOTE = "sidecar_doi_from_ris {date}: doi from the same-stem .ris DO line (identity {verdict})"
_RIS_DO = re.compile(r"^DO\s{2}-\s?(.*)$", re.M)
_RIS_TI = re.compile(r"^(?:TI|T1)\s{2}-\s?(.*)$", re.M)


# ---------------------------------------------------------------- shared helpers (one copy per W5-B script)
_ADDRESS = re.compile(r"(?i)([A-Za-z0-9._+-])[A-Za-z0-9._+-]*(@|%(?:25)*40)"
                      r"([A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]+)")


def mask(s) -> str:
    """`s` with every email address shown as its first character, `***` and its domain."""
    return _ADDRESS.sub(lambda m: m.group(1) + "***" + m.group(2) + m.group(3), str(s or ""))


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
def has_doi(rec: dict) -> bool:
    v = rec.get("doi")
    return isinstance(v, str) and bool(v.strip()) or (v is not None and not isinstance(v, str))


def first_pages(text: str) -> str:
    if not isinstance(text, str):
        return ""
    if "\f" in text:
        return "\f".join(text.split("\f")[:FIRST_PAGES])
    return text[:HEAD_CHARS]


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


def repair_file(path: Path, ris: Path | None, commit=False, backup=True, today=None):
    """One sidecar. Status: has_doi, no_ris, no_do, bad_do, skipped (flag), mismatch (identity
    FLAG), changed, error."""
    res = {"path": str(path), "status": "", "rows": [], "reason": "", "doi": "", "verdict": "",
           "backup": None}
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
    if has_doi(rec):
        res["status"] = "has_doi"
        return res
    if ris is None:
        res["status"] = "no_ris"
        return res
    try:
        ris_text = ris.read_text(encoding="utf-8", errors="replace")
    except OSError as e:
        res.update(status="error", reason=f"{ris.name} unreadable: {type(e).__name__}")
        return res
    m = _RIS_DO.search(ris_text)
    if not m or not m.group(1).strip():
        res["status"] = "no_do"
        return res
    d = _doi.normalise_structured(m.group(1).strip())
    if not d:
        res.update(status="bad_do", reason=f"DO {m.group(1).strip()[:80]!r} is a placeholder or malformed")
        return res
    res["doi"] = d
    flag = _flag(path, rec)
    if flag:
        res.update(status="skipped", reason=f"identity flag ({flag})")
        return res
    ti = _RIS_TI.search(ris_text)
    ris_title = _text.display_field(ti.group(1)) if ti else ""
    sc_title = rec.get("title") if isinstance(rec.get("title"), str) else ""
    verdict = identity.check(first_pages(rec.get("text")), d, queue_title=sc_title or None,
                             ris_title=ris_title or None)
    res["verdict"] = str(verdict.decision)
    if not verdict.ok:
        why = verdict.evidence.get("reason", "")
        res.update(status="mismatch", reason=f"identity {verdict.decision}: {why}")
        return res
    new = json.loads(raw_text)
    new["doi"] = d
    add_note(new, NOTE.format(date=(today or _dt.date.today()).isoformat(), verdict=verdict.decision))
    new = lit_util.merge_sidecar(rec, new)
    out = dump(new, json_layout(raw_text)).encode("utf-8")
    if raw.startswith(b"\xef\xbb\xbf"):
        out = b"\xef\xbb\xbf" + out
    before = "" if rec.get("doi") is None else str(rec.get("doi"))
    rule = f"doi from .ris DO (identity {verdict.decision}"
    cand = str(rec.get("doi_candidate") or "").strip().lower()
    if cand and cand != d:
        rule += f"; doi_candidate {cand} differs"
    res["rows"].append({"path": str(path), "field": "doi", "before": mask(before) or "(empty)",
                        "after": mask(d), "rule": rule + ")"})
    res["status"] = "changed"
    if commit:
        try:
            res["backup"] = replace_bytes(path, out, backup=backup)
        except OSError as e:
            res.update(status="error", reason=f"write failed: {type(e).__name__}: {e}")
    return res


def run(*, projects=None, lib_dirs=None, commit=False, report=None, backup=True, show=20, cfg=None) -> dict:
    res = {"step": STEP, "exit_code": 0, "commit": bool(commit), "libraries": 0, "sidecars": 0,
           "no_doi": 0, "no_doi_with_ris_do": 0, "changed_files": 0, "skipped_flag": 0,
           "identity_mismatch": 0, "bad_do": 0, "errors": 0, "report": None, "per_library": {},
           "verdicts": {}, "error_list": []}
    try:
        libs = select_libraries(projects, lib_dirs, cfg)
        if not libs:
            raise config.ConfigError("no library selected (no --lib-dir, and no active project with a lib_dir)")
        rpath = report_path(report, STEP, libs)
    except config.ConfigError as e:
        print(f"[ERR] {e}", file=sys.stderr)
        res.update(exit_code=1, error=str(e))
        return res
    rows, verdicts, shown = [], Counter(), 0
    for label, lib in libs:
        per = Counter()
        if not lib.is_dir():
            print(f"  [skip] {label}: no library at {lib}", file=sys.stderr)
            res["per_library"][label] = {"missing": 1}
            continue
        res["libraries"] += 1
        try:
            entries = [e.name for e in os.scandir(lib) if e.is_file()]
        except OSError as e:
            res["errors"] += 1
            res["error_list"].append(f"{lib}: {type(e).__name__}")
            continue
        ris_by_stem = {n[:-4].casefold(): n for n in entries if n.lower().endswith(".ris")}
        for name in sorted(n for n in entries if n.lower().endswith(SIDECAR)):
            stem = name[: -len(SIDECAR)]
            rn = ris_by_stem.get(stem.casefold())
            r = repair_file(lib / name, lib / rn if rn else None, commit=commit, backup=backup)
            per["sidecars"] += 1
            st = r["status"]
            if st not in ("has_doi", "error"):
                per["no_doi"] += 1
            if st in ("changed", "skipped", "mismatch", "bad_do"):
                per["no_doi_with_ris_do"] += 1
            if r["verdict"]:
                verdicts[r["verdict"]] += 1
            if st == "changed":
                per["changed"] += 1
                rows.extend(r["rows"])
                if shown < show:
                    shown += 1
                    print(f"  {'filled' if commit else 'would fill'} {label}: {name} ({r['doi']}, "
                          f"identity {r['verdict']})")
            elif st == "skipped":
                per["skipped_flag"] += 1
                rows.append({"path": str(lib / name), "field": "doi", "before": "(empty)", "after": "(kept empty)",
                             "rule": f"skipped: {r['reason']}"})
            elif st == "mismatch":
                per["identity_mismatch"] += 1
                rows.append({"path": str(lib / name), "field": "doi", "before": "(empty)", "after": "(kept empty)",
                             "rule": f"skipped: .ris DO {r['doi']} fails the {r['reason']}"})
            elif st == "bad_do":
                per["bad_do"] += 1
                rows.append({"path": str(lib / name), "field": "doi", "before": "(empty)", "after": "(kept empty)",
                             "rule": f"skipped: {mask(r['reason'])}"})
            elif st == "error":
                per["errors"] += 1
                res["error_list"].append(f"{lib / name}: {r['reason']}")
        res["per_library"][label] = dict(per)
        for k_res, k_per in (("sidecars", "sidecars"), ("no_doi", "no_doi"),
                             ("no_doi_with_ris_do", "no_doi_with_ris_do"), ("changed_files", "changed"),
                             ("skipped_flag", "skipped_flag"), ("identity_mismatch", "identity_mismatch"),
                             ("bad_do", "bad_do"), ("errors", "errors")):
            res[k_res] += per[k_per]
    res["verdicts"] = dict(verdicts)
    write_report(rpath, rows)
    res["report"] = str(rpath)
    if res["errors"]:
        res["exit_code"] = 2
    _print(res)
    return res


def _print(res):
    print(f"\n{'project':<40} {'sidecars':>8} {'no doi':>6} {'+DO':>5} {'fill':>5} {'flag':>5} {'fail':>5}")
    for label, p in res["per_library"].items():
        if p.get("missing"):
            print(f"{label[:40]:<40} (no library)")
            continue
        print(f"{label[:40]:<40} {p.get('sidecars', 0):>8} {p.get('no_doi', 0):>6} "
              f"{p.get('no_doi_with_ris_do', 0):>5} {p.get('changed', 0):>5} {p.get('skipped_flag', 0):>5} "
              f"{p.get('identity_mismatch', 0) + p.get('bad_do', 0):>5}")
    print(f"total: {res['sidecars']} sidecars, {res['no_doi']} with no doi, {res['no_doi_with_ris_do']} of them "
          f"with a .ris DO line; {res['changed_files']} {'filled' if res['commit'] else 'to fill'} "
          f"(identity {', '.join(f'{k} {v}' for k, v in sorted(res['verdicts'].items()))}); "
          f"{res['skipped_flag']} identity-flagged, {res['identity_mismatch']} failing the identity check, "
          f"{res['bad_do']} with an unusable DO (all kept empty)")
    for e in res["error_list"][:20]:
        print(f"  [error] {e}")
    print(f"report: {res['report']}")
    if not res["commit"]:
        print("Dry run: nothing written but the report. Add --commit to fill (a .bak-w5b copy of each "
              "file is kept beside it unless --no-backup).")
    print(summary_line(res))


def main(argv=None) -> int:
    lit_util.utf8_stdout()
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--project", action="append", default=None, metavar="KEY",
                    help="a registered project (repeatable); default: every active project")
    ap.add_argument("--lib-dir", action="append", default=None, metavar="DIR",
                    help="a library directory (repeatable), instead of or besides --project")
    ap.add_argument("--commit", action="store_true", help="write the DOIs; default: dry run")
    ap.add_argument("--report", default=None,
                    help="the diff report CSV (default: sidecar_doi_from_ris_<date>.csv in the current directory)")
    ap.add_argument("--no-backup", action="store_true", help="do not keep <name>.bak-w5b copies")
    ap.add_argument("--show", type=int, default=20, help="print the first N files (default 20)")
    a = ap.parse_args(argv)
    return run(projects=a.project, lib_dirs=a.lib_dir, commit=a.commit, report=a.report,
               backup=not a.no_backup, show=a.show)["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
