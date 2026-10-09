"""Repair character references, markup and line-wrapped fields in the `.ris` files the pipeline
wrote (W5-B; issue rows M148, M282). Dry run by default.

Older writers put Crossref text into `.ris` files as it came: `Physiology &amp; Behavior`,
`<i>in vitro</i>`, `P &lt; 0.05` in an AB line, and (before `ris_emit._ris_val`) raw newlines that
leave untagged continuation lines (F6). The writer now cleans every field (ris_emit.build_ris with
litpipe.text); this script brings the existing files to the same form:
  * TI, T2, JO and AU values: `litpipe.text.display_field`, taken again until it stops changing (at
    most four rounds), so a title deposited as escaped markup (`&lt;b&gt;Title&lt;/b&gt;`, seven
    live files on 2026-10-07) loses the markup instead of keeping `<b>` and changing again on the
    next run;
  * AB values: `litpipe.text.abstract_field` (already iterates; drops a leading "Abstract");
  * a field written over several lines becomes one line (its lines joined by the cleaners'
    whitespace collapse); a field the cleaner empties is dropped, as build_ris drops empty fields.
Every other line is kept byte for byte. No entity table of its own: only the W2-E2 cleaners. A
file whose changes are all cosmetic (a dropped "Abstract" heading, spacing, NFC; no character
reference, tag, ligature, no-break space or wrapped line) is repaired too, so the file reads as the
current writer would write it; `--damage-only` leaves those alone and lists them.

Which files are the pipeline's (DEC-29): a file whose sha256 the `.ris` manifest records (state kv
namespace "ris", key `ris_emit.manifest_key`) and that still has that hash; or, with no record (no
file has one before the cutover), a file of the pipeline's shape:
  1. UTF-8 without a byte-order mark, LF line ends only (no CR anywhere), ending in a newline;
  2. the first line `TY  - <type>` with a type build_ris writes (JOUR CPAPER BOOK CHAP RPRT UNPD DATA
     THES EDBOOK) and the last line exactly `ER  - `;
  3. every tagged line `XX  - value` with a tag build_ris writes, in build_ris's order
     (TY AU TI JO PY DA VL IS SP EP DO SN UR AB ER), each at most once except AU;
  4. an untagged line (a blank one included) only after TI, JO, AU or AB, where older writers left
     raw newlines.
An EndNote or Zotero export fails (CRLF, other tags such as T2, N1, DB, ID, PB, a blank line after
ER). A file with a record and another hash (edited since), or with no record and another shape, is
curated and never touched. A file whose identity verdict is FLAG (`<stem>.identity.json`, or a pmc
`<stem>.fulltext.json`; audit_portfolio.identity_flag) is a review item and is skipped. After a
repair the manifest record is updated when one exists (none is created).

Subtitles are not added: the subtitle is not on disk in a form the pipeline trusts (sidecar
`subtitle` values include page numbers and report codes), so a subtitle repair needs the metadata
source (backfill_ris --overwrite after the cutover, DEC-29). Filename stems that still carry entity
residue are REPORTED, never renamed (rows with the field "filename stem", for audit_filenames
review): a `.ris` whose TI or first AU still holds an entity whose name the stem keeps, and any
`.pdf` or `.ris` stem with a glued entity name (`Muumlndel`, `Peacuteriard`, `...AmpBehavior`).

Writes: `--commit` replaces each changed file atomically (a temp file beside it, then os.replace)
after copying it byte for byte to `<name>.bak-w5b` (`.bak-w5b.1`, ... when one exists; never
overwritten) unless `--no-backup`. The diff report (one row per changed field: path, field, before
excerpt, after excerpt, rule) goes to `--report` (default `ris_text_repair_<date>.csv` in the
current directory; refused inside a library). A dry run writes only the report and never creates
the state file.

Usage:
  python backfills/ris_text_repair.py [--project KEY ...] [--lib-dir DIR ...] [--commit]
                                      [--damage-only] [--report CSV] [--no-backup] [--show N]
Exit codes: 0 done; 1 usage or configuration (unknown project, no library, a report path inside a
library); 2 done, but a file could not be read or written (listed).
"""
from __future__ import annotations

import argparse
import datetime as _dt
import hashlib
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
import ris_emit as R  # noqa: E402
from litpipe import config  # noqa: E402
from litpipe import text as _text  # noqa: E402

STEP = "ris_text_repair"
BACKUP_SUFFIX = ".bak-w5b"
REPORT_COLUMNS = ["path", "field", "before", "after", "rule"]
WINDOW = 160

# ---------------------------------------------------------------- the pipeline's shape (build_ris)
PIPELINE_ORDER = ("TY", "AU", "TI", "JO", "PY", "DA", "VL", "IS", "SP", "EP", "DO", "SN", "UR", "AB", "ER")
_RANK = {t: i for i, t in enumerate(PIPELINE_ORDER)}
PIPELINE_TYPES = frozenset({"JOUR", "CPAPER", "BOOK", "CHAP", "RPRT", "UNPD", "DATA", "THES", "EDBOOK"})
CONTINUED_TAGS = frozenset({"TI", "JO", "AU", "AB"})
DISPLAY_TAGS = frozenset({"TI", "T2", "JO", "AU"})
ABSTRACT_TAGS = frozenset({"AB"})
_TAG_LINE = re.compile(r"^([A-Z][A-Z0-9])  - (.*)$")
_REF = re.compile(r"&(#[0-9]+|#[xX][0-9A-Fa-f]+|[A-Za-z][A-Za-z0-9]*);")
_PY = re.compile(r"^(\d{4})")


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


# ---------------------------------------------------------------- shape and parsing
def pipeline_shape(raw: bytes):
    """(entries, "") when `raw` has the shape build_ris writes, else (None, the reason). An entry is
    [tag, value lines] (the first line's value, then any continuation lines)."""
    if raw.startswith(b"\xef\xbb\xbf"):
        return None, "byte-order mark"
    if b"\r" in raw:
        return None, "CR line ends"
    try:
        t = raw.decode("utf-8")
    except UnicodeDecodeError:
        return None, "not UTF-8"
    if not t.endswith("\n"):
        return None, "no final newline"
    entries, prev = [], -1
    for i, line in enumerate(t[:-1].split("\n")):
        m = _TAG_LINE.match(line)
        if not m:
            if entries and entries[-1][0] in CONTINUED_TAGS:
                entries[-1][1].append(line)
                continue
            return None, f"untagged line after {entries[-1][0] if entries else 'nothing'}"
        tag, val = m.group(1), m.group(2)
        if tag not in _RANK:
            return None, f"tag {tag} (build_ris never writes it)"
        r = _RANK[tag]
        if i == 0 and tag != "TY":
            return None, "first line is not TY"
        if r < prev or (r == prev and tag != "AU"):
            return None, f"tag {tag} out of build_ris order"
        prev = r
        entries.append([tag, [val]])
    if not entries or entries[0][0] != "TY" or entries[0][1][0] not in PIPELINE_TYPES:
        return None, "TY type build_ris never writes"
    if entries[-1][0] != "ER" or entries[-1][1] != [""]:
        return None, "last line is not 'ER  - '"
    return entries, ""


def display_fixed(value) -> tuple[str, int]:
    """display_field taken until it stops changing (at most four rounds): (text, rounds)."""
    s = _text.display_field(value)
    rounds = 1
    for _ in range(3):
        t = _text.display_field(s)
        if t == s:
            break
        s, rounds = t, rounds + 1
    return s, rounds


def _damage(s) -> list:
    from audit_portfolio import text_damage
    return text_damage(s)


def clean_entry(tag, lines):
    """(new value or None when unchanged, the rule label, damaged). `damaged`: the field shows text
    damage (audit_portfolio.text_damage: a character reference, a tag, a ligature, a no-break
    space) or spans several lines; otherwise the change is cosmetic (a dropped "Abstract" heading,
    spacing, NFC)."""
    raw = "\n".join(lines)
    multiline = len(lines) > 1
    if tag in DISPLAY_TAGS:
        new, rounds = display_fixed(raw)
        cleaner = "display_field" + (f" x{rounds}" if rounds > 1 else "")
    elif tag in ABSTRACT_TAGS:
        new, cleaner = _text.abstract_field(raw), "abstract_field"
    else:
        return None, "", False
    if new == raw and not multiline:
        return None, "", False
    kinds = (["continuation"] if multiline else []) + _damage(raw)
    damaged = bool(kinds)
    if new == "":
        kinds.append("emptied")
    return new, f"{cleaner}: {'+'.join(kinds) or COSMETIC}", damaged


COSMETIC = "other (heading, spacing or NFC)"
# Entity names glued into a filename stem by the old namer: `M&uuml;ndel` gave `Muumlndel`,
# `P&eacute;riard` gave `Peacuteriard`, `Physiology &amp; Behavior` gave `PhysiologyAmpBehavior`.
STEM_RESIDUE = re.compile(
    r"(?:[aeiouyn](?:uml|acute|grave|tilde|cedil|slash)|szlig)(?=[a-z])"
    r"|(?<=[a-z0-9])(?:Amp|Lt|Gt|Quot|Apos|Nbsp|Ndash|Mdash|Rsquo|Lsquo|Rdquo|Ldquo|Hellip|Thinsp)(?=[A-Z0-9_]|$)")


def rebuild(entries, changes) -> str:
    out = []
    for i, (tag, lines) in enumerate(entries):
        if i in changes:
            new = changes[i]
            if new:
                out.append(f"{tag}  - {new}")
        else:
            out.append(f"{tag}  - {lines[0]}")
            out.extend(lines[1:])
    return "\n".join(out) + "\n"


# ---------------------------------------------------------------- ownership
def _state_absent() -> bool:
    """True when the manifest lives in litpipe.state and its file does not exist: nothing is
    recorded, and reading would create it."""
    st = R._state()
    import litpipe.state as S
    return st is S and not S.db_path(create=False).exists()


def manifest_record(path):
    """The DEC-29 record for `path`, or None. Read through the state ris_emit uses (its STATE, else
    litpipe.net's, else litpipe.state), but NOT through ris_emit._kv_get, which reads a failed read
    as "no record": here a failed read must stop the file (it may be a curated edit), so the error
    propagates to the caller."""
    if _state_absent():
        return None
    return R._state().kv_get(R.RIS_NS, R.manifest_key(path))


def _flag(lib: Path, stem: str) -> str:
    from audit_portfolio import identity_flag, read_json
    for ext in (".identity.json", ".fulltext.json"):
        p = lib / (stem + ext)
        if p.is_file():
            d, _err = read_json(p)
            why = identity_flag(d)
            if why:
                return f"{p.name}: {why}"
    return ""


def stem_residue(path: Path, raw_text: str):
    """(suggested stem, entity names) when the file name keeps more of an entity name from the raw
    TI/AU values than the stem the current writer would build; else None. Report only."""
    fields = {}
    for line in raw_text.splitlines():
        m = _TAG_LINE.match(line)
        if m and m.group(1) not in fields:
            fields[m.group(1)] = m.group(2)
    raw_ti, raw_au = fields.get("TI", ""), fields.get("AU", "")
    names = {m.group(1).lstrip("#").casefold() for m in _REF.finditer(raw_ti + " " + raw_au)}
    if not names:
        return None
    family = display_fixed(raw_au.split(",", 1)[0])[0] if raw_au else ""
    py = _PY.match(fields.get("PY", ""))
    new = R.canonical_stem(py.group(1) if py else "", family, display_fixed(raw_ti)[0])
    stem, new_f = path.stem.casefold(), new.casefold()
    left = sorted(n for n in names if stem.count(n) > new_f.count(n))
    return (new, left) if left else None


# ---------------------------------------------------------------- the run
def repair_file(path: Path, lib: Path, commit=False, backup=True, damage_only=False):
    """One `.ris`: {"status", "rows", "owner", ...}. Status: changed, unchanged, cosmetic (only
    cosmetic changes, left alone under damage_only), skipped, error."""
    res = {"path": str(path), "status": "unchanged", "rows": [], "reason": "", "owner": "",
           "residue": None, "backup": None, "entity": False, "recorded": False}
    try:
        raw = path.read_bytes()
    except OSError as e:
        res.update(status="error", reason=f"{type(e).__name__}: {e}")
        return res
    text = raw.decode("utf-8", errors="replace")
    res["entity"] = "entity" in _damage(text)
    res["residue"] = stem_residue(path, text)
    flag = _flag(lib, path.stem)
    if flag:
        res.update(status="skipped", reason=f"identity flag ({flag})")
        return res
    try:
        rec = manifest_record(path)
    except Exception as e:                      # a broken state file: nothing is known
        res.update(status="error", reason=f"manifest read failed: {type(e).__name__}")
        return res
    sha = hashlib.sha256(raw).hexdigest()
    if rec and rec != sha:
        res.update(status="skipped", reason="curated: edited since the pipeline wrote it")
        return res
    entries, why = pipeline_shape(raw)
    if entries is None:
        res.update(status="skipped", reason=f"curated or foreign: {why}")
        return res
    res["owner"] = "manifest" if rec else "shape"
    res["recorded"] = bool(rec)
    changes, au, damaged = {}, 0, False
    for i, (tag, lines) in enumerate(entries):
        field = tag
        if tag == "AU":
            au += 1
            field = f"AU[{au}]"
        new, rule, hurt = clean_entry(tag, lines)
        if new is None:
            continue
        changes[i] = new
        damaged = damaged or hurt
        b, a = excerpts("\n".join(lines), new)
        res["rows"].append({"path": str(path), "field": field, "before": b, "after": a, "rule": rule})
    if not changes:
        return res
    out = rebuild(entries, changes).encode("utf-8")
    if out == raw:
        res["rows"] = []
        return res
    if damage_only and not damaged:
        res["status"] = "cosmetic"
        for row in res["rows"]:
            row["rule"] += " (left alone: --damage-only)"
        return res
    res["status"] = "changed"
    if commit:
        try:
            res["backup"] = replace_bytes(path, out, backup=backup)
        except OSError as e:
            res.update(status="error", reason=f"write failed: {type(e).__name__}: {e}")
            return res
        if rec and not R._kv_set(R.RIS_NS, R.manifest_key(path), hashlib.sha256(out).hexdigest()):
            res.update(status="error", reason="repaired, but the manifest record could not be updated: the "
                                              "file now reads as edited (curated) to write_ris")
    return res


def stem_pattern_rows(lib: Path, names, reported: set):
    """Report rows for library stems (of `.pdf` and `.ris` files) whose name carries an entity name
    the old namer glued in (STEM_RESIDUE), other than those `reported` already."""
    rows, seen = [], set(reported)
    for name in names:
        low = name.lower()
        if not (low.endswith(".pdf") or low.endswith(".ris")):
            continue
        stem = name[:-4]
        if stem.casefold() in seen:
            continue
        m = STEM_RESIDUE.search(stem)
        if m:
            seen.add(stem.casefold())
            rows.append({"path": str(lib / name), "field": "filename stem", "before": stem, "after": "",
                         "rule": f"stem entity residue ('{m.group(0)}' in the name): report only, for "
                                 "audit_filenames review"})
    return rows


def run(*, projects=None, lib_dirs=None, commit=False, report=None, backup=True, show=20, cfg=None,
        damage_only=False) -> dict:
    res = {"step": STEP, "exit_code": 0, "commit": bool(commit), "damage_only": bool(damage_only),
           "libraries": 0, "ris_files": 0, "entity_files": 0, "pipeline_files": 0, "recorded_files": 0,
           "changed_files": 0, "changed_fields": 0, "cosmetic_files": 0, "skipped_curated": 0,
           "skipped_flag": 0, "errors": 0, "stem_residue": 0, "report": None, "per_library": {}, "rules": {},
           "error_list": []}
    try:
        libs = select_libraries(projects, lib_dirs, cfg)
        if not libs:
            raise config.ConfigError("no library selected (no --lib-dir, and no active project with a lib_dir)")
        rpath = report_path(report, STEP, libs)
    except config.ConfigError as e:
        print(f"[ERR] {e}", file=sys.stderr)
        res.update(exit_code=1, error=str(e))
        return res
    rows, rules, shown = [], Counter(), 0
    for label, lib in libs:
        per = Counter()
        if not lib.is_dir():
            print(f"  [skip] {label}: no library at {lib}", file=sys.stderr)
            per["missing"] = 1
            res["per_library"][label] = dict(per)
            continue
        res["libraries"] += 1
        try:
            listing = sorted(e.name for e in os.scandir(lib) if e.is_file())
        except OSError as e:
            res["errors"] += 1
            res["error_list"].append(f"{lib}: {type(e).__name__}")
            continue
        names = [n for n in listing if n.lower().endswith(".ris")]
        residue_stems = set()
        for name in names:
            r = repair_file(lib / name, lib, commit=commit, backup=backup, damage_only=damage_only)
            per["ris"] += 1
            per["entity"] += r["entity"]
            if r["owner"]:
                per["pipeline"] += 1
                per["recorded"] += r["recorded"]
            if r["status"] == "changed":
                per["changed"] += 1
                rows.extend(r["rows"])
                for row in r["rows"]:
                    rules[row["rule"].split(":")[0] + ":" + row["field"].split("[")[0]] += 1
                if shown < show:
                    shown += 1
                    print(f"  {'fixed' if commit else 'would fix'} {label}: {name} "
                          f"({', '.join(x['field'] for x in r['rows'])})")
            elif r["status"] == "cosmetic":
                per["cosmetic"] += 1
                rows.extend(r["rows"])
            elif r["status"] == "skipped":
                per["skipped_flag" if r["reason"].startswith("identity flag") else "skipped_curated"] += 1
            elif r["status"] == "error":
                per["errors"] += 1
                res["error_list"].append(f"{lib / name}: {r['reason']}")
            if r["residue"]:
                per["stem_residue"] += 1
                residue_stems.add(Path(name).stem.casefold())
                new, names_left = r["residue"]
                rows.append({"path": str(lib / name), "field": "filename stem", "before": Path(name).stem,
                             "after": new, "rule": "stem entity residue (" + ",".join(names_left)
                             + " in the .ris TI/AU): report only, for audit_filenames review"})
        pattern_rows = stem_pattern_rows(lib, listing, residue_stems)
        per["stem_residue"] += len(pattern_rows)
        rows.extend(pattern_rows)
        res["per_library"][label] = dict(per)
        for k_res, k_per in (("ris_files", "ris"), ("entity_files", "entity"), ("pipeline_files", "pipeline"),
                             ("recorded_files", "recorded"), ("changed_files", "changed"),
                             ("cosmetic_files", "cosmetic"), ("skipped_curated", "skipped_curated"),
                             ("skipped_flag", "skipped_flag"), ("errors", "errors"),
                             ("stem_residue", "stem_residue")):
            res[k_res] += per[k_per]
    res["changed_fields"] = sum(1 for r in rows if r["field"] != "filename stem"
                                and not r["rule"].endswith("--damage-only)"))
    res["rules"] = dict(rules)
    write_report(rpath, rows)
    res["report"] = str(rpath)
    if res["errors"]:
        res["exit_code"] = 2
    _print(res)
    return res


def _print(res):
    print(f"\n{'project':<40} {'.ris':>6} {'entity':>6} {'pipe':>6} {'change':>6} {'curated':>7} {'flag':>4} {'stem':>4}")
    for label, p in res["per_library"].items():
        if p.get("missing"):
            print(f"{label[:40]:<40} (no library)")
            continue
        print(f"{label[:40]:<40} {p.get('ris', 0):>6} {p.get('entity', 0):>6} {p.get('pipeline', 0):>6} "
              f"{p.get('changed', 0):>6} {p.get('skipped_curated', 0):>7} {p.get('skipped_flag', 0):>4} "
              f"{p.get('stem_residue', 0):>4}")
    print(f"total: {res['ris_files']} .ris, {res['entity_files']} with a character reference (audit "
          f"definition), {res['pipeline_files']} pipeline-written, {res['changed_files']} "
          f"{'repaired' if res['commit'] else 'to repair'} ({res['changed_fields']} fields), "
          + (f"{res['cosmetic_files']} with cosmetic changes only left alone (--damage-only), "
             if res["damage_only"] else "")
          + f"{res['skipped_curated']} curated or foreign, {res['skipped_flag']} identity-flagged, "
          f"{res['stem_residue']} stems with entity residue (report only)")
    if res["rules"]:
        print("fields by cleaner and kind: " + ", ".join(f"{k} {v}" for k, v in sorted(res["rules"].items())))
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
                    help="the diff report CSV (default: ris_text_repair_<date>.csv in the current directory)")
    ap.add_argument("--no-backup", action="store_true", help="do not keep <name>.bak-w5b copies")
    ap.add_argument("--damage-only", action="store_true",
                    help="repair only files with text damage (a character reference, a tag, a ligature, a "
                         "no-break space, a line-wrapped field); list the cosmetic-only ones")
    ap.add_argument("--show", type=int, default=20, help="print the first N files (default 20)")
    a = ap.parse_args(argv)
    return run(projects=a.project, lib_dirs=a.lib_dir, commit=a.commit, report=a.report,
               backup=not a.no_backup, show=a.show, damage_only=a.damage_only)["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
