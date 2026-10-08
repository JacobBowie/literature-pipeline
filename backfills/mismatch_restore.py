"""Restore the right papers the old DOI-mismatch guard moved into `_mismatch/` (W5-B; issue rows
M081, C046; plan DEC-07). Dry run by default.

Until W2a a fetch stage moved a PDF to `<library>/_mismatch/` when the first DOI printed in it was
not the queued DOI; the identity check (W1-B) found most of them were the right paper and proposed
what to do with each, as a CSV given here as `--proposal` (columns project, path, queue_doi,
report, proposal, confidence, pdf_doi_primary, evidence; proposal RESTORE, DUPLICATE_COPY,
ALREADY_HELD or KEEP_QUARANTINED, confidence HIGH, MEDIUM or LOW). DEC-07: only HIGH rows act.
  * RESTORE (HIGH): the identity is checked again now (`unpaywall_fetch_v2.judge_identity`, i.e.
    `litpipe.identity.check` on the first three pages, against the queued DOI, or for a row with no
    surviving report row the PDF's own DOI, and the best title on hand: the index's
    `paper_metadata.title` for that DOI, read-only, else the stage report's title). A row that no
    longer passes (FLAG, or a SUPPLEMENT) is skipped and reported. One that passes moves back to its
    library under the DEC-14 name (`unpaywall_fetch_v2.build_filename` from the index's first author
    and title, the year from the file name) when the index's title is this file's title and its
    author gives a surname (see dest_name), else under the name it had; a stem taken by another
    paper gets the DOI-hash suffix (`resolve_dest`); an existing file is never replaced.
    A `.ris`, `.fulltext.json`, `.identity.json` or `.xml` with the same stem inside `_mismatch/`
    moves with it (none did on 2026-10-07: the old guard moved only the PDF). Then
    `<stem>.identity.json` records the verdict, as the fetch stages write it, plus `restored_from`.
  * DUPLICATE_COPY and ALREADY_HELD (HIGH): listed as deletion candidates; nothing is deleted (the
    owner's call).
  * KEEP_QUARANTINED, and every MEDIUM or LOW row: reported, left in place.
  * A PDF in `_mismatch/` that the proposal does not list (the folder grew from 73 to 77 after the
    proposal) is reported as UNPROPOSED; a proposal row whose file is gone, as GONE.

A restored PDF has no `.ris` (the old guard skipped it), and its first printed DOI may be another
paper's, which is why it was quarantined: `backfill_ris` would read that DOI from the text.
`--write-ris` (with `--commit`) writes the `.ris` for the restored DOI through
`ris_emit.emit_ris_for_pdf` (a live metadata lookup through litpipe.net; needs LITPIPE_EMAIL).

The file location comes from the registry (`<library>/_mismatch/<name>`), else the proposal's
`path`. The diff report (path, field, before, after, rule; one row per proposal row and per
unproposed file) goes to `--report` (default `mismatch_restore_<date>.csv` in the current
directory; refused inside a library). A moved file keeps its bytes; no backup is needed for a move,
and an `.identity.json` being replaced is first copied to `<name>.bak-w5b` unless `--no-backup`.

Usage:
  python backfills/mismatch_restore.py --proposal CSV [--project KEY ...] [--lib-dir DIR ...]
         [--db PATH | --no-db] [--commit [--write-ris]] [--report CSV] [--no-backup]
Exit codes: 0 done; 1 usage or configuration (no proposal, an unknown project); 2 done, but a file
could not be read, checked or moved.
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
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import lit_util  # noqa: E402
from litpipe import config, ledger  # noqa: E402
from litpipe import doi as _doi  # noqa: E402

STEP = "mismatch_restore"
MISMATCH = "_mismatch"
DB_NAME = "portfolio.duckdb"
BACKUP_SUFFIX = ".bak-w5b"
REPORT_COLUMNS = ["path", "field", "before", "after", "rule"]
COMPANIONS = (".ris", ".fulltext.json", ".identity.json", ".xml")
_YEAR = re.compile(r"^(\d{4})_")


# ---------------------------------------------------------------- shared helpers (one copy per W5-B script)
_ADDRESS = re.compile(r"(?i)([A-Za-z0-9._+-])[A-Za-z0-9._+-]*(@|%(?:25)*40)"
                      r"([A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]+)")


def mask(s) -> str:
    """`s` with every email address shown as its first character, `***` and its domain."""
    return _ADDRESS.sub(lambda m: m.group(1) + "***" + m.group(2) + m.group(3), str(s or ""))


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
    for lib in libs:
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


def summary_line(res) -> str:
    keep = {k: v for k, v in res.items() if isinstance(v, (int, float, str, bool)) or v is None}
    return "[step-summary] " + json.dumps(keep, ensure_ascii=False)


# ---------------------------------------------------------------- inputs
def read_proposal(path):
    with open(path, encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    need = {"project", "path", "queue_doi", "proposal", "confidence"}
    if rows and not need <= set(rows[0]):
        raise config.ConfigError(f"{path} lacks columns {sorted(need - set(rows[0]))}")
    return rows


def library_of(project, row_path, known):
    p = known.get(project)
    if isinstance(p, dict) and p.get("lib_dir"):
        return lit_util.lib_paths(project, p)[1]
    return Path(row_path).parent.parent


def locate(row, lib):
    """The PDF's current path: `<lib>/_mismatch/<name>` (registry-based, portable), else the
    proposal's own path; None when neither exists."""
    name = re.split(r"[\\/]", row.get("path") or "")[-1]
    for cand in (lib / MISMATCH / name, Path(row.get("path") or "")):
        if name and cand.is_file():
            return cand
    return None


class Metadata:
    """Title, year and authors by DOI from the index (read-only), else from the stage report row."""

    def __init__(self, db=None):
        self.con = None
        self.note = "no index"
        self._reports = {}
        if db and Path(db).is_file():
            try:
                import duckdb
                self.con = duckdb.connect(str(db), read_only=True)
                self.note = f"index {db} (read-only)"
            except Exception as e:                # a lock held by the indexer: go on without it
                self.note = f"index unavailable ({type(e).__name__})"

    def close(self):
        if self.con is not None:
            self.con.close()
            self.con = None

    def index(self, doi):
        if self.con is None or not doi:
            return {}
        try:
            r = self.con.execute("SELECT year, lastname, title, authors FROM paper_metadata WHERE doi = ?",
                                 [doi]).fetchone()
        except Exception:
            return {}
        if not r:
            return {}
        return {"year": str(r[0] or ""), "lastname": r[1] or "", "title": r[2] or "", "authors": r[3] or ""}

    def report_title(self, report_rel, doi):
        """The stage report row's title: <projects root>/<report>, else the same file name under
        `<projects root>/<first folder>/_archive/`."""
        if not report_rel or not doi:
            return ""
        parts = [p for p in re.split(r"[\\/]", report_rel) if p]
        if not parts:
            return ""
        if report_rel not in self._reports:
            path = lit_util.PROJECTS_ROOT.joinpath(*parts)
            if not path.is_file():
                arch = lit_util.PROJECTS_ROOT / parts[0] / "_archive"
                path = next(iter(sorted(arch.rglob(parts[-1]))), None) if arch.is_dir() else None
            rows = {}
            if path is not None:
                try:
                    with open(path, encoding="utf-8-sig", newline="") as f:
                        for r in csv.DictReader(f):
                            d = _doi.normalise(r.get("doi") or "")
                            if d and d not in rows:
                                rows[d] = (r.get("title") or "").strip()
                except (OSError, csv.Error, UnicodeDecodeError):
                    rows = {}
            self._reports[report_rel] = rows
        return self._reports[report_rel].get(doi, "")


# ---------------------------------------------------------------- one row
def _row(path, field, before, after, rule):
    return {"path": str(path), "field": field, "before": mask(before), "after": mask(after), "rule": mask(rule)}


def _slug_key(s) -> str:
    return re.sub(r"[^a-z0-9]", "", lit_util.safe_ascii(s or "").lower())


def dest_name(pdf: Path, meta: dict, uf):
    """(file name, how): the DEC-14 name (unpaywall_fetch_v2.build_filename) when the index gives an
    author and a title that is this file's title, else the quarantined file's own name. The index
    also holds titles parsed from reference lists (`10945768 https doi org ...`) and placeholder
    authors (`Anon`), so its title must reproduce the file name's title slug (difflib ratio 0.8 or
    more on letters and digits) and its first author must give a surname. The year stays the file
    name's (the queue's year when it was fetched) unless that is `Unknown`."""
    from difflib import SequenceMatcher
    authors = meta.get("authors") or meta.get("lastname") or ""
    title = meta.get("title") or ""
    m = _YEAR.match(pdf.name)
    year = m.group(1) if m else (meta.get("year") or "")
    if not (title and authors):
        return pdf.name, "kept name (the index has no author or title for this DOI)"
    surname = uf.last_name(authors)
    if surname in ("Unknown", "Anon", "Anonymous") or not re.search(r"[A-Za-z]{2}", surname):
        return pdf.name, f"kept name (the index author {surname!r} is not a surname)"
    old_slug = pdf.name[:-4].split("_", 2)[2] if pdf.name.count("_") >= 2 else pdf.name[:-4]
    new = uf.build_filename(year, authors, title)
    new_slug = new[:-4].split("_", 2)[2] if new.count("_") >= 2 else new[:-4]
    ratio = SequenceMatcher(None, _slug_key(old_slug), _slug_key(new_slug)).ratio()
    if ratio < 0.8:
        return pdf.name, f"kept name (the index title is not this file's title: slug ratio {ratio:.2f})"
    return new, f"DEC-14 name (index author and title, slug ratio {ratio:.2f})"


def write_identity(dest: Path, doi, verdict, kind, n_pages, restored_from, backup=True):
    rec = {"queue_doi": _doi.normalise(doi) or doi, "pdf": dest.name, "source": "mismatch_restore",
           "source_url": "", "host": "", "host_type": "", "version": "",
           "checked_at": ledger.now_iso(), "n_pages": n_pages, "doc_kind": str(kind),
           "restored_from": restored_from,
           "restored_by": "mismatch_restore (DEC-07: a HIGH RESTORE row of the W1-B proposal, identity re-checked)"}
    rec.update(verdict.as_dict())
    path = lit_util.companion_path(dest, ".identity.json")
    if path.exists() and backup:
        shutil.copy2(path, backup_path(path))
    lit_util.atomic_write_json(str(path), ledger.redact_obj(rec))
    return path


def restore_row(row, pdf: Path, lib: Path, meta_src: Metadata, commit=False, write_ris=False, backup=True):
    """Act on one HIGH RESTORE row. Returns (status, report rows, detail)."""
    import unpaywall_fetch_v2 as uf
    doi = _doi.normalise(row.get("queue_doi") or "") or _doi.normalise(row.get("pdf_doi_primary") or "")
    if not doi:
        return "SKIPPED", [_row(pdf, "file", pdf.name, "(kept in _mismatch)", "RESTORE HIGH: no DOI to check")], {}
    meta = meta_src.index(doi)
    title = meta.get("title") or meta_src.report_title(row.get("report") or "", doi)
    title_src = "index" if meta.get("title") else ("stage report" if title else "none")
    verdict, kind, n_pages = uf.judge_identity(str(pdf), doi, title)
    if uf._flagged(verdict, kind):
        why = verdict.evidence.get("reason") or f"doc_kind {kind}"
        return "SKIPPED", [_row(pdf, "file", pdf.name, "(kept in _mismatch)",
                                f"RESTORE HIGH: identity no longer passes ({verdict.decision}, {kind}: {why}; "
                                f"title from {title_src})")], {"doi": doi}
    fn, how = dest_name(pdf, meta, uf)
    dest, collided = uf.resolve_dest(str(lib), fn, doi, set())
    dest = Path(dest)
    rule = (f"RESTORE HIGH: identity {verdict.decision} ({kind}, title from {title_src}); {how}"
            + ("; stem taken, DOI-hash suffix" if collided else ""))
    if dest.exists():
        return "SKIPPED", [_row(pdf, "file", pdf.name, str(dest), f"{rule}; destination exists: nothing moved")], {}
    stem = pdf.name[:-4]
    companions = [(pdf.with_name(stem + ext), lit_util.companion_path(dest, ext)) for ext in COMPANIONS
                  if pdf.with_name(stem + ext).is_file()]
    rows = [_row(pdf, "file", str(pdf.relative_to(lib)) if _inside(pdf, lib) else str(pdf), dest.name, rule)]
    for src, dst in companions:
        rows.append(_row(src, "companion", src.name, dst.name,
                         "moves with its PDF" + ("; destination exists: kept in _mismatch" if dst.exists() else "")))
    detail = {"doi": doi, "dest": str(dest)}
    if not commit:
        return "WOULD_RESTORE", rows, detail
    if dest.exists():                             # checked above; a file that appeared since is kept
        raise FileExistsError(str(dest))
    os.replace(pdf, dest)
    for src, dst in companions:
        if not dst.exists():
            os.replace(src, dst)                  # a moved .identity.json is backed up, then rewritten
    write_identity(dest, doi, verdict, kind, n_pages, f"{MISMATCH}/{pdf.name}", backup=backup)
    if write_ris:
        import ris_emit as R
        status, _p = R.emit_ris_for_pdf(doi, str(dest))
        detail["ris"] = status
        rows.append(_row(dest, "ris", "", lit_util.companion_path(dest, ".ris").name,
                         f"ris_emit.emit_ris_for_pdf: {status}"))
    return "RESTORED", rows, detail


# ---------------------------------------------------------------- the run
def run(*, proposal=None, projects=None, lib_dirs=None, commit=False, report=None, backup=True,
        db=None, use_db=True, write_ris=False, show=40, cfg=None) -> dict:
    res = {"step": STEP, "exit_code": 0, "commit": bool(commit), "proposal_rows": 0, "mismatch_pdfs": 0,
           "restore_high": 0, "restored": 0, "would_restore": 0, "skipped": 0, "delete_candidates": 0,
           "kept": 0, "not_high": 0, "unproposed": 0, "gone": 0, "errors": 0, "report": None,
           "metadata": "", "statuses": {}, "error_list": [], "restored_files": []}
    try:
        if not proposal:
            raise config.ConfigError("--proposal CSV is required (W1-B's mismatch_restore_proposal.csv)")
        if write_ris and not commit:
            raise config.ConfigError("--write-ris needs --commit")
        rows_in = read_proposal(proposal)
        reg = config.load(cfg)
        known = reg.get("projects") or {}
        if projects:
            unknown = [k for k in projects if k not in known]
            if unknown:
                raise config.ConfigError(f"not registered in projects.json: {', '.join(unknown)}")
        libs = {}                                  # label -> library Path
        for lib in (lib_dirs or ()):
            libs[str(lib)] = Path(lib)
        if projects or not lib_dirs:
            keys = list(dict.fromkeys(projects)) if projects else \
                [k for k, p in known.items() if isinstance(p, dict) and p.get("active", True)]
            for k in keys:
                if isinstance(known.get(k), dict) and known[k].get("lib_dir"):
                    libs[k] = lit_util.lib_paths(k, known[k])[1]
            if not projects and not lib_dirs:      # the proposal's projects even when inactive
                for r in rows_in:
                    k = r.get("project") or ""
                    if k not in libs:
                        libs[k] = library_of(k, r.get("path") or "", known)
        rpath = report_path(report, STEP, libs.values())
    except (config.ConfigError, OSError, csv.Error) as e:
        print(f"[ERR] {e}", file=sys.stderr)
        res.update(exit_code=1, error=str(e))
        return res

    lib_keys = {os.path.normcase(os.path.abspath(p)) for p in libs.values()}
    meta_src = Metadata(db if use_db else None)
    res["metadata"] = meta_src.note
    statuses, out_rows, seen_pdfs = Counter(), [], set()
    try:
        for row in rows_in:
            lib = library_of(row.get("project") or "", row.get("path") or "", known)
            if os.path.normcase(os.path.abspath(lib)) not in lib_keys:
                continue
            res["proposal_rows"] += 1
            pdf = locate(row, lib)
            prop, conf = (row.get("proposal") or "").upper(), (row.get("confidence") or "").upper()
            label = f"{prop} {conf}"
            if pdf is None:
                res["gone"] += 1
                statuses["GONE"] += 1
                out_rows.append(_row(row.get("path"), "file", row.get("path"), "(not found)", f"{label}: GONE"))
                continue
            seen_pdfs.add(os.path.normcase(os.path.abspath(pdf)))
            if conf != "HIGH":
                res["not_high"] += 1
                statuses[f"NOT_HIGH {prop}"] += 1
                out_rows.append(_row(pdf, "file", pdf.name, "(kept in _mismatch)", f"{label}: not HIGH, kept (DEC-07)"))
                continue
            if prop in ("DUPLICATE_COPY", "ALREADY_HELD"):
                res["delete_candidates"] += 1
                statuses[prop] += 1
                out_rows.append(_row(pdf, "file", pdf.name, "(delete candidate; not deleted)",
                                     f"{label}: {row.get('evidence') or ''}"[:400]))
                continue
            if prop != "RESTORE":
                res["kept"] += 1
                statuses[prop or "UNKNOWN"] += 1
                out_rows.append(_row(pdf, "file", pdf.name, "(kept in _mismatch)", f"{label}: kept"))
                continue
            res["restore_high"] += 1
            try:
                status, rows, detail = restore_row(row, pdf, lib, meta_src, commit=commit,
                                                   write_ris=write_ris, backup=backup)
            except Exception as e:                # an unreadable PDF or a failed move: listed, run goes on
                res["errors"] += 1
                res["error_list"].append(f"{pdf}: {type(e).__name__}: {mask(e)}")
                statuses["ERROR"] += 1
                out_rows.append(_row(pdf, "file", pdf.name, "(kept in _mismatch)", f"{label}: ERROR {type(e).__name__}"))
                continue
            statuses[status] += 1
            out_rows.extend(rows)
            if status == "RESTORED":
                res["restored"] += 1
                res["restored_files"].append(detail.get("dest"))
            elif status == "WOULD_RESTORE":
                res["would_restore"] += 1
            else:
                res["skipped"] += 1
        # what _mismatch/ holds now that the proposal does not list
        for lib in sorted(set(libs.values()), key=str):
            mm = lib / MISMATCH
            if not mm.is_dir():
                continue
            for dirpath, _dirs, files in os.walk(mm):
                for fn in sorted(files):
                    if not fn.lower().endswith(".pdf"):
                        continue
                    p = Path(dirpath) / fn
                    res["mismatch_pdfs"] += 1
                    if os.path.normcase(os.path.abspath(p)) in seen_pdfs:
                        continue
                    res["unproposed"] += 1
                    statuses["UNPROPOSED"] += 1
                    out_rows.append(_row(p, "file", fn, "(kept in _mismatch)",
                                         "UNPROPOSED: not in the W1-B proposal; judge it by hand or re-run the proposal"))
    finally:
        meta_src.close()
    if commit:
        res["mismatch_pdfs_after"] = sum(1 for lib in set(libs.values()) for _d, _s, fs in os.walk(lib / MISMATCH)
                                         for f in fs if f.lower().endswith(".pdf"))
    res["statuses"] = dict(statuses)
    write_report(rpath, out_rows)
    res["report"] = str(rpath)
    if res["errors"]:
        res["exit_code"] = 2
    _print(res, out_rows, show)
    return res


def _print(res, rows, show):
    for r in [r for r in rows if r["rule"].startswith("RESTORE")][:max(0, int(show))]:
        print(f"  {r['before'][:70]:<70} => {r['after'][:60]}  [{r['rule'][:90]}]")
    print(f"proposal rows in scope: {res['proposal_rows']}; PDFs in _mismatch/ now: {res['mismatch_pdfs']}")
    print(f"RESTORE HIGH: {res['restore_high']} ({res['restored']} restored, {res['would_restore']} would "
          f"restore, {res['skipped']} skipped); delete candidates (DUPLICATE_COPY, ALREADY_HELD HIGH; not "
          f"deleted): {res['delete_candidates']}; kept: {res['kept']}; not HIGH: {res['not_high']}; "
          f"unproposed: {res['unproposed']}; gone: {res['gone']}")
    print(f"statuses: {', '.join(f'{k} {v}' for k, v in sorted(res['statuses'].items()))}")
    print(f"metadata: {res['metadata']}")
    for e in res["error_list"][:20]:
        print(f"  [error] {e}")
    print(f"report: {res['report']}")
    if not res["commit"]:
        print("Dry run: nothing moved or written but the report. Add --commit (and --write-ris for the "
              "restored files' .ris) to restore.")
    print(summary_line(res))


def main(argv=None) -> int:
    lit_util.utf8_stdout()
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--proposal", default=None, help="W1-B's mismatch_restore_proposal.csv (required)")
    ap.add_argument("--project", action="append", default=None, metavar="KEY",
                    help="only this registered project's rows (repeatable); default: every project in the proposal")
    ap.add_argument("--lib-dir", action="append", default=None, metavar="DIR",
                    help="only rows whose library is this directory (repeatable)")
    ap.add_argument("--db", default=None,
                    help="index database read (read-only) for titles and authors; default: the configured "
                         "portfolio.duckdb when it exists")
    ap.add_argument("--no-db", action="store_true", help="read no index database")
    ap.add_argument("--commit", action="store_true", help="move the files and write the verdicts; default: dry run")
    ap.add_argument("--write-ris", action="store_true",
                    help="with --commit: write each restored file's .ris (a live metadata lookup)")
    ap.add_argument("--report", default=None,
                    help="the report CSV (default: mismatch_restore_<date>.csv in the current directory)")
    ap.add_argument("--no-backup", action="store_true",
                    help="do not keep a .bak-w5b copy of an .identity.json being replaced")
    ap.add_argument("--show", type=int, default=40, help="print the first N restore rows (default 40)")
    a = ap.parse_args(argv)
    db = a.db
    if db is None and not a.no_db:
        try:
            cand = config.db_dir() / DB_NAME
            db = str(cand) if cand.is_file() else None
        except config.ConfigError:
            db = None
    return run(proposal=a.proposal, projects=a.project, lib_dirs=a.lib_dir, commit=a.commit,
               report=a.report, backup=not a.no_backup, db=db, use_db=not a.no_db,
               write_ris=a.write_ris, show=a.show)["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
