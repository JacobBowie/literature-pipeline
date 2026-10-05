"""Forward-citation walker (Semantic Scholar, through litpipe.s2).

For every PDF in a project's library, find the papers that CITE it. Promoted
from an older per-project enrichment tool; each PDF's `.ris` sidecar carries the
canonical DOI, so titles are not re-extracted from PDF first pages.

Reads DOI from each PDF's `.ris` sidecar (preferred), then falls back to
.fulltext.json sidecar, then extracts from PDF text.

Semantic Scholar traffic goes through litpipe.s2 on ONE Session per run (run budget and
circuit breaker from the projects.json "s2" block; typed walks). Seeds are resolved by
POST /paper/batch (500 DOIs a call: paperId, citationCount); each seed's citers are then
paged (1,000 a page, up to the 9,999 rows S2 lets anyone reach) and checked against that
citationCount. Spacing comes from the client: 6.5 s unkeyed, 1.1 s with a key in the
S2_API_KEY environment variable. `--sleep` is ignored and `--s2-key` is deprecated.

Degraded-walk guard (K3):
  - A failed resolve or citations call is FAILED, never "0 citers". A seed whose
    citationCount is 0 is a true zero-citer seed (no call) and is not a failure; a DOI S2
    has no record of is "unresolved", not a failure.
  - A seed's rows are replaced only when S2 answered it; a failed seed keeps the rows the
    published CSV already holds for it.
  - Each seed is appended to `<report stem>.partial.jsonl` as it finishes. A killed,
    aborted or degraded run resumes from it: answered seeds are not requested again,
    failed ones are retried (`--restart` starts over). Publishing deletes the journal, so
    a journal is always newer than the published report; a library larger than one run
    budget is walked over several runs.
  - The run replaces <report> only when it is clean. Otherwise it writes
    `<report stem>.degraded.csv` and leaves <report> byte-identical.

Exit codes:
  0  clean: the report was written.
  2  degraded: more than 5 % of seed walks failed, or the result has fewer seeds with
     citers than the published report (`--force` accepts that one). Nothing published.
  3  aborted: the run budget is spent or the circuit breaker tripped. Nothing published;
     rerun the same command to resume.
  1  usage or configuration error.
The last stdout line is "[step-summary] {json}" for snowball.py.

Usage:
  # By project name (recommended)
  python forward_citations.py --project research_a

  # Explicit paths
  python forward_citations.py --lib-dir /path/to/library

  # First N seeds for testing (the other seeds keep their published rows)
  python forward_citations.py --project research_a --limit 5

Outputs (default location: <lib-dir>/_forward_citations.csv):
  Columns: seed_pdf, seed_doi, citing_paper_id, citing_doi, citing_title,
           citing_year, citing_authors, citing_venue, citing_cited_by, citing_abstract
"""
import argparse
import contextlib
import csv
import json
import os
import re
import sys
from pathlib import Path

import lit_util
from litpipe import config, ledger, s2
from litpipe.outcomes import Kind, Outcome

lit_util.utf8_stdout()

CONFIG_PATH = Path(__file__).parent / "projects.json"

FIELDS = ["seed_pdf", "seed_doi", "citing_paper_id", "citing_doi", "citing_title",
          "citing_year", "citing_authors", "citing_venue", "citing_cited_by",
          "citing_abstract"]
# The citing-paper fields behind FIELDS (the set this walker has always requested; the
# lean set without `abstract` is the K2 rebuild's call, W3-A).
CITING_FIELDS = ("paperId", "externalIds", "title", "abstract", "year", "authors",
                 "citationCount", "venue")
RESOLVE_FIELDS = ("paperId", "citationCount")

FAIL_THRESHOLD = 0.05            # more than 5 % of seed walks failed: degraded (exit 2)
JOURNAL_VERSION = 1
SUMMARY_MARKER = "[step-summary] "
# Failure kinds that are the network's doing (snowball reads any of these as DEGRADED).
TRANSPORT_KINDS = frozenset({"TRANSPORT", "OUTAGE", "REFUSED", "DEFERRED"})
EXIT_OK, EXIT_ERROR, EXIT_DEGRADED, EXIT_ABORTED = 0, 1, 2, 3


class ProjectError(ValueError):
    """The library cannot be located (unregistered project, missing directory)."""


def _gate_doi(doi: str) -> str:
    """RC1: normalize then drop malformed/suspicious (truncated) DOIs."""
    d = lit_util.normalize_doi(doi)
    if not lit_util.is_valid_doi(d) or lit_util.is_suspicious_doi(d):
        return ""
    return d


def doi_from_ris(ris_path: Path) -> str:
    if not ris_path.exists(): return ""
    try:
        with open(ris_path, encoding="utf-8") as f:
            for line in f:
                m = re.match(r"^DO\s{2}-\s?(.+)$", line)
                if m: return _gate_doi(m.group(1))
    except OSError: pass
    return ""


def doi_from_sidecar(sc_path: Path) -> str:
    if not sc_path.exists(): return ""
    try:
        with open(sc_path, encoding="utf-8") as f:
            d = json.load(f)
        return _gate_doi(d.get("doi") or "")
    except (OSError, ValueError): return ""


def doi_from_pdf(pdf_path: Path, max_chars=5000) -> str:
    try:
        import fitz
    except ImportError:
        return ""
    text = ""
    try:
        doc = fitz.open(str(pdf_path))
        try:
            for p in doc:
                text += p.get_text()
                if len(text) >= max_chars: break
        finally:
            doc.close()
    except Exception: return ""
    # RC1: re-joins line-wrapped DOIs and drops truncated/suspicious ones.
    return lit_util.extract_doi_from_text(text, max_chars=max_chars)


def get_doi(pdf: Path) -> str:
    return (doi_from_ris(lit_util.companion_path(pdf, ".ris"))
         or doi_from_sidecar(lit_util.companion_path(pdf, ".fulltext.json"))
         or doi_from_pdf(pdf))


def resolve_project(name: str) -> Path:
    """The library directory of a registered project (ProjectError when it is not registered)."""
    cfg = lit_util.load_projects_config(CONFIG_PATH, missing_ok=True).get("projects", {})
    if name not in cfg:
        raise ProjectError(f"'{name}' not in projects.json")
    p = cfg[name]
    base = lit_util.PROJECTS_ROOT / (p.get("parent") or name)
    return base / p["lib_dir"]


# ------------------------------------------------------------------------------ paths
def degraded_path(out: Path) -> Path:
    return out.with_name(out.stem + ".degraded.csv")


def journal_path(out: Path) -> Path:
    return out.with_name(out.stem + ".partial.jsonl")


def unique_path(out: Path) -> Path:
    return out.with_name(out.stem + "_unique_dois.csv")


# ------------------------------------------------------------------------------ rows
def _text(v) -> str:
    return v if isinstance(v, str) else ("" if v is None else str(v))


def citing_row(paper) -> dict:
    """The citing-paper columns of one S2 citingPaper object. Missing or null fields write
    blank; nothing is invented (a null-paperId stub row still counts toward citationCount)."""
    p = paper if isinstance(paper, dict) else {}
    ext = p.get("externalIds")
    raw_doi = ext.get("DOI") if isinstance(ext, dict) else None
    authors = p.get("authors")
    names = ([a.get("name") for a in authors if isinstance(a, dict) and isinstance(a.get("name"), str)]
             if isinstance(authors, list) else [])
    year, cited_by = p.get("year"), p.get("citationCount")
    return {
        "citing_paper_id": _text(p.get("paperId")),
        # RC1: gate the S2-supplied citing DOI so malformed values don't reach the CSV /
        # unique-DOI list that feeds sweep.py.
        "citing_doi":      _gate_doi(raw_doi) if isinstance(raw_doi, str) else "",
        "citing_title":    _text(p.get("title")),
        "citing_year":     year if isinstance(year, int) else "",
        "citing_authors":  "; ".join(names),
        "citing_venue":    _text(p.get("venue")),
        "citing_cited_by": cited_by if isinstance(cited_by, int) else "",
        "citing_abstract": _text(p.get("abstract"))[:1500],
    }


def read_report(path: Path):
    """{seed_doi: [rows]} of a published report, or None when there is none. Rows of one seed
    come from the first seed_pdf listed for it (a second PDF of the same DOI repeats them)."""
    if not path.exists():
        return None
    by_doi, first_pdf = {}, {}
    with open(path, encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f):
            d = (r.get("seed_doi") or "").strip().lower()
            if not d:
                continue
            pdf = r.get("seed_pdf") or ""
            if first_pdf.setdefault(d, pdf) != pdf:
                continue
            by_doi.setdefault(d, []).append({k: r.get(k, "") for k in FIELDS})
    return by_doi


# ------------------------------------------------------------------------------ journal
class Journal:
    """Per-seed append log, `<report stem>.partial.jsonl`: a header line, then one JSON line per
    finished seed (the last line for a DOI wins). Lines are flushed and fsynced as written, so a
    kill loses at most the seed in flight; a torn last line is dropped on load."""

    def __init__(self, path: Path, lib: Path):
        self.path = path
        self.lib = str(Path(lib).resolve())
        self.started_at = None
        self._fh = None

    def load(self, restart=False) -> dict:
        """The entries of a resumable journal ({doi: entry}); {} (and a fresh start) otherwise."""
        if restart or not self.path.exists():
            if restart and self.path.exists():
                print(f"[info] --restart: discarding {self.path.name}")
            return {}
        try:
            lines = self.path.read_text(encoding="utf-8").splitlines()
        except OSError as e:
            print(f"[warn] cannot read {self.path.name} ({ledger.redact(e)}); starting over")
            return {}
        header, entries = None, {}
        for i, line in enumerate(lines):
            try:
                obj = json.loads(line)
            except ValueError:
                continue                                   # a torn line from a kill
            if i == 0:
                header = obj
                continue
            if isinstance(obj, dict) and isinstance(obj.get("doi"), str):
                entries[obj["doi"]] = obj
        why = self._foreign(header)
        if why:
            print(f"[info] not resuming from {self.path.name}: {why}; starting over")
            return {}
        self.started_at = header["started_at"]
        return entries

    def _foreign(self, header):
        if not isinstance(header, dict) or header.get("journal") != "forward_citations":
            return "no journal header"
        if header.get("version") != JOURNAL_VERSION:
            return f"journal version {header.get('version')!r}"
        if header.get("lib") != self.lib:
            return "written for another library"
        if not isinstance(header.get("started_at"), str):
            return "no start time"
        return None

    def open(self, entries: dict):
        """Rewrite the journal compactly (header and the last entry per DOI), then append to it."""
        self.started_at = self.started_at or ledger.now_iso()
        header = {"journal": "forward_citations", "version": JOURNAL_VERSION, "lib": self.lib,
                  "started_at": self.started_at}
        text = "".join(json.dumps(o, ensure_ascii=False) + "\n" for o in [header, *entries.values()])
        self.path.parent.mkdir(parents=True, exist_ok=True)
        lit_util.atomic_write_text(str(self.path), text)
        self._fh = open(self.path, "a", encoding="utf-8", newline="\n")

    def append(self, entry: dict):
        self._fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
        self._fh.flush()
        os.fsync(self._fh.fileno())

    def close(self):
        if self._fh is not None:
            self._fh.close()
            self._fh = None

    def remove(self):
        self.close()
        with contextlib.suppress(FileNotFoundError):
            self.path.unlink()


def _answered(entry) -> bool:
    return bool(entry) and entry.get("state") != "failed"


def _entry(doi, stage, state, *, rows=None, count=None, kind=None, status=None, reason="",
           attempts=0, mismatch=False) -> dict:
    return {"doi": doi, "stage": stage, "state": state, "count": count,
            "n_rows": len(rows) if rows is not None else None, "rows": rows,
            "kind": kind, "status": status, "reason": ledger.redact(reason or "")[:300],
            "count_mismatch": mismatch, "attempts": attempts, "walked_at": ledger.now_iso()}


def _walk_entry(doi, w: s2.Walk, count) -> dict:
    """A journal entry for one citations Walk. A count mismatch is FAILED (s2 keeps the fetched
    rows in .partial, which is diagnosis, never a result)."""
    if w.failed:
        mismatch = w.reason.startswith("count_mismatch")
        return _entry(doi, "citations", "failed", count=count,
                      kind="COUNT_MISMATCH" if mismatch else str(w.kind or Kind.ERROR),
                      status=w.status, reason=w.reason, attempts=w.attempts, mismatch=mismatch)
    rows = [citing_row(p) for p in w.rows] if w.rows is not None else None
    return _entry(doi, "citations", str(w.state), rows=rows, count=count, status=w.status,
                  reason=w.reason, attempts=w.attempts)


def _walk_id(paper_id, doi):
    return paper_id if isinstance(paper_id, str) and re.fullmatch(r"[0-9a-f]{40}", paper_id) else doi


def _resolved(slot):
    """(paperId, citationCount) of a batch record, or None when the record has no usable count."""
    if not isinstance(slot, dict):
        return None
    n = slot.get("citationCount")
    if isinstance(n, bool) or not isinstance(n, int) or n < 0:
        return None
    return slot.get("paperId"), n


def _label(e) -> str:
    st = e["state"]
    if st == "failed":
        return f"FAILED {e['stage']} {e['kind']}: {e['reason'][:70]}"
    if st == "unresolved":
        return "S2_UNRESOLVED (no S2 record)"
    if e["rows"] is None:
        return st.upper()
    tag = {"empty": " (zero citers)", "capped_9999": " (capped at 9,999)"}.get(st, "")
    return f"{e['n_rows']:>5} citing{tag}"


# ------------------------------------------------------------------------------ key and flags
@contextlib.contextmanager
def _key_env(value):
    """Deprecated --s2-key: put the value in S2_API_KEY for this run only (litpipe.s2 reads
    nothing else), restore the previous environment afterwards, never print it."""
    if not value:
        yield
        return
    print("[deprecated] --s2-key: set the S2_API_KEY environment variable instead (a key on the "
          "command line lands in shell history); using it for this run only")
    old = os.environ.get(s2.KEY_ENV)
    os.environ[s2.KEY_ENV] = value
    try:
        yield
    finally:
        if old is None:
            os.environ.pop(s2.KEY_ENV, None)
        else:
            os.environ[s2.KEY_ENV] = old


# ------------------------------------------------------------------------------ the walk
def run(*, project=None, lib_dir=None, report=None, limit=0, sleep=None, s2_key=None,
        force=False, restart=False, session=None) -> dict:
    """One forward walk. Returns the summary dict (its `exit_code` is the CLI's exit code)."""
    if sleep is not None:
        print("[deprecated] --sleep is ignored: spacing comes from litpipe.s2 (6.5 s unkeyed, "
              "1.1 s keyed; projects.json \"s2\" block)")
    with _key_env(s2_key):
        return _run(project, lib_dir, report, limit, force, restart, session)


def _error(msg) -> dict:
    print(f"[ERR] {msg}", file=sys.stderr)
    return {"step": "forward_citations", "exit_code": EXIT_ERROR, "status": "error", "error": msg}


def _run(project, lib_dir, report, limit, force, restart, session) -> dict:
    try:
        if project:
            lib = resolve_project(project)
        elif lib_dir:
            lib = Path(lib_dir)
        else:
            return _error("Pass --project or --lib-dir")
    except ProjectError as e:
        return _error(str(e))
    if not lib.is_dir():
        return _error(f"not a directory: {lib}")
    try:
        sess = session or s2.Session()
    except config.ConfigError as e:
        return _error(f"projects.json s2 block: {ledger.redact(e)}")

    pdfs = sorted(lib.glob("*.pdf"))
    walk_n = min(limit, len(pdfs)) if limit else len(pdfs)
    out = Path(report) if report else (lib / "_forward_citations.csv")
    print(f"library:  {lib}")
    print(f"PDFs:     {len(pdfs)}" + (f" (walking the first {walk_n})" if limit else ""))
    print(f"output:   {out}")
    print(f"{s2.key_status()}  run budget={sess.budget} attempts  breaker={sess.breaker}\n")

    seeds = []                                     # (pdf, doi) for every PDF, sorted
    for i, pdf in enumerate(pdfs, 1):
        doi = get_doi(pdf)
        seeds.append((pdf, doi))
        if not doi and i <= walk_n:
            print(f"  [{i:>4}/{len(pdfs)}] {pdf.name[:55]:<55}  NO_DOI")
    first_pdf = {}
    for pdf, doi in seeds[:walk_n]:
        if doi:
            first_pdf.setdefault(doi, pdf.name)
    walk_dois = list(first_pdf)                    # distinct, library order

    prior = read_report(out)
    journal = Journal(journal_path(out), lib)
    entries = {d: e for d, e in journal.load(restart).items() if d in first_pdf}
    resumed = sum(1 for d in walk_dois if _answered(entries.get(d)))
    if resumed:
        print(f"[resume] {journal.path.name} (started {journal.started_at}): {resumed} answered "
              f"seed(s) kept, not requested again")
    todo = [d for d in walk_dois if not _answered(entries.get(d))]
    journal.open(entries)
    try:
        _walk(todo, walk_dois, first_pdf, entries, journal, sess)
    finally:
        journal.close()
    return _finish(seeds, walk_n, walk_dois, entries, prior, out, journal, sess, force, resumed)


def _record(journal, entries, entry, i, n, first_pdf):
    journal.append(entry)
    entries[entry["doi"]] = entry
    print(f"  [{i:>4}/{n}] {first_pdf[entry['doi']][:50]:<50} {entry['doi'][:30]:<30} {_label(entry)}",
          flush=True)


def _walk(todo, walk_dois, first_pdf, entries, journal, sess):
    pos = {d: i for i, d in enumerate(walk_dois, 1)}
    n = len(walk_dois)
    if not todo:
        return
    batch = s2.paper_batch(todo, RESOLVE_FIELDS, session=sess)
    plan = []                                      # (doi, walk id, count)
    for doi, slot in zip(todo, batch.payload):
        got = _resolved(slot)
        if got is not None:
            plan.append((doi, _walk_id(got[0], doi), got[1]))
        elif slot is None:
            _record(journal, entries, _entry(doi, "resolve", "unresolved", reason="no S2 record (batch null)"),
                    pos[doi], n, first_pdf)
        elif isinstance(slot, Outcome) and slot.kind is Kind.SKIPPED:
            _record(journal, entries, _entry(doi, "resolve", "unresolved", kind=str(slot.kind),
                                             reason=slot.detail), pos[doi], n, first_pdf)
        elif isinstance(slot, Outcome):
            if sess.aborted and slot.kind is Kind.DEFERRED and not slot.attempts:
                continue                           # never sent: the run stopped first
            _record(journal, entries, _entry(doi, "resolve", "failed", kind=str(slot.kind), status=slot.status,
                                             reason=slot.detail, attempts=slot.attempts), pos[doi], n, first_pdf)
        else:
            _record(journal, entries, _entry(doi, "resolve", "failed", kind=str(Kind.ERROR),
                                             reason="unexpected S2 response: no citationCount"),
                    pos[doi], n, first_pdf)
    for doi, wid, count in plan:
        if sess.aborted:
            break
        w = s2.citations(wid, CITING_FIELDS, expected=count, session=sess)
        _record(journal, entries, _walk_entry(doi, w, count), pos[doi], n, first_pdf)
    _recount(plan, entries, journal, sess, pos, n, first_pdf)


def _recount(plan, entries, journal, sess, pos, n, first_pdf):
    """A count mismatch from count drift during a long run (citationCount read at the start,
    citers paged hours later) is re-checked once: one batch call for the mismatched seeds, and a
    seed whose count changed is walked again against the new count. A mismatch with an unchanged
    count stays FAILED."""
    mism = [(d, wid) for d, wid, _ in plan if entries.get(d, {}).get("count_mismatch")]
    if not mism or sess.aborted:
        return
    print(f"\n[recount] {len(mism)} count mismatch(es): re-reading citationCount once")
    batch = s2.paper_batch([d for d, _ in mism], RESOLVE_FIELDS, session=sess)
    for (doi, wid), slot in zip(mism, batch.payload):
        if sess.aborted:
            break
        got = _resolved(slot)
        if got is None or got[1] == entries[doi]["count"]:
            continue
        w = s2.citations(wid, CITING_FIELDS, expected=got[1], session=sess)
        e = _walk_entry(doi, w, got[1])
        e["recount"] = True
        _record(journal, entries, e, pos[doi], n, first_pdf)


def verdict(seed_walks, failed, with_citers, prior_with_citers=None, aborted=None, force=False,
            not_walked=0) -> tuple:
    """(exit code, reasons) of a finished walk. Exit 3 when the session aborted; exit 2 when more
    than 5 % of seed walks failed, or when the result has fewer seeds with citers than the
    published report (unless `force`); else 0. Pure, so recorded runs can be replayed."""
    reasons = []
    if aborted:
        reasons.append(f"aborted ({aborted}); {not_walked} seed(s) not walked")
    if failed > FAIL_THRESHOLD * seed_walks:
        reasons.append(f"{failed} of {seed_walks} seed walks failed (over 5 %)")
    if prior_with_citers is not None and with_citers < prior_with_citers and not force:
        reasons.append(f"{with_citers} seeds with citers, fewer than the published {prior_with_citers}")
    code = EXIT_ABORTED if aborted else (EXIT_DEGRADED if reasons else EXIT_OK)
    return code, reasons


def _finish(seeds, walk_n, walk_dois, entries, prior, out, journal, sess, force, resumed) -> dict:
    final = [entries.get(d) for d in walk_dois]
    failed = [e for e in final if e and e["state"] == "failed"]
    mismatch = [e for e in failed if e.get("count_mismatch")]
    transport = [e for e in failed if e.get("kind") in TRANSPORT_KINDS]
    not_walked = sum(1 for e in final if e is None)

    rows = []                                      # every PDF: answered rows, else published rows
    for pdf, doi in seeds:
        if not doi:
            continue
        e = entries.get(doi)
        if _answered(e) and e["rows"] is not None:
            cites = e["rows"]
        elif prior is not None and doi in prior:
            cites = prior[doi]
        else:
            cites = ()
        rows.extend({**c, "seed_pdf": pdf.name, "seed_doi": doi} for c in cites)
    current = {doi for _, doi in seeds if doi}
    on_disk = {pdf.name for pdf, _ in seeds}
    with_citers = len({r["seed_doi"] for r in rows})
    # A published seed still counts while its PDF is in the library, even when its DOI could not be
    # read this run: a removed PDF is a removed seed, an unreadable .ris is not.
    prior_with = (len({d for d, rs in prior.items()
                       if d in current or any(r.get("seed_pdf") in on_disk for r in rs)})
                  if prior is not None else None)

    aborted = sess.aborted
    code, reasons = verdict(len(walk_dois), len(failed), with_citers, prior_with, aborted, force, not_walked)
    too_many = len(failed) > FAIL_THRESHOLD * len(walk_dois)

    out.parent.mkdir(parents=True, exist_ok=True)
    uniq = sorted({r["citing_doi"] for r in rows if r["citing_doi"]})
    written = None
    if reasons:
        dpath = degraded_path(out)
        lit_util.atomic_write_csv(str(dpath), rows, FIELDS)
        written = str(dpath)
        if aborted or too_many:
            journal.close()                        # kept: the next run resumes from it
        else:
            journal.remove()                       # nothing failed: re-walk next time
    else:
        lit_util.atomic_write_csv(str(out), rows, FIELDS)
        lit_util.atomic_write_csv(str(unique_path(out)), [{"doi": d} for d in uniq], ["doi"])
        journal.remove()
        written = str(out)

    res = {
        "step": "forward_citations", "exit_code": code,
        "status": "aborted" if aborted else ("degraded" if reasons else "ok"),
        "reasons": reasons,
        "seeds": len(seeds), "walked_pdfs": walk_n,
        "with_doi": sum(1 for _, d in seeds[:walk_n] if d),
        "no_doi": sum(1 for _, d in seeds[:walk_n] if not d),
        "seed_walks": len(walk_dois), "resumed": resumed, "not_walked": not_walked,
        "answered": sum(1 for e in final if _answered(e)),
        "failed": len(failed),
        "failed_resolve": sum(1 for e in failed if e["stage"] == "resolve"),
        "failed_citations": sum(1 for e in failed if e["stage"] == "citations"),
        "count_mismatch": len(mismatch),
        "recounted": sum(1 for e in final if e and e.get("recount")),
        "transport_failures": len(transport),
        "unresolved": sum(1 for e in final if e and e["state"] == "unresolved"),
        "zero_citers": sum(1 for e in final if e and e["state"] == "empty"),
        "seeds_with_citers": with_citers, "prior_seeds_with_citers": prior_with,
        "total_rows": len(rows), "unique_dois": len(uniq),
        "written": written, "published": code == EXIT_OK,
        "journal": str(journal.path) if journal.path.exists() else None,
        "aborted": aborted, "s2": sess.summary(),
    }
    _print_summary(res, out)
    return res


def _print_summary(res, out):
    print()
    print("=== summary ===")
    print(f"  seeds:              {res['seeds']}" + (f" ({res['walked_pdfs']} walked)"
                                                     if res["walked_pdfs"] != res["seeds"] else ""))
    print(f"  with DOI:           {res['with_doi']}")
    print(f"  no DOI:             {res['no_doi']}")
    print(f"  seed walks:         {res['seed_walks']} distinct DOIs ({res['resumed']} resumed)")
    print(f"  S2 resolve failed:  {res['failed_resolve']}")
    print(f"  citations failed:   {res['failed_citations']} (count mismatch {res['count_mismatch']}, "
          f"transport {res['transport_failures']})")
    print(f"  S2 unresolved:      {res['unresolved']} (no S2 record; not a failure)")
    print(f"  zero citers:        {res['zero_citers']} (not a failure)")
    if res["not_walked"]:
        print(f"  not walked:         {res['not_walked']} (run stopped)")
    print(f"  seeds with citers:  {res['seeds_with_citers']}"
          + (f" (published: {res['prior_seeds_with_citers']})" if res["prior_seeds_with_citers"] is not None else ""))
    print(f"  total citing rows:  {res['total_rows']}")
    print(f"  unique citing DOIs: {res['unique_dois']}")
    if res["published"]:
        print(f"  report:             {out}")
        print(f"  unique DOIs:        {unique_path(out)}")
    else:
        status = "ABORTED" if res["status"] == "aborted" else "DEGRADED"
        print(f"  [{status}] {'; '.join(res['reasons'])}")
        print(f"  NOT published: {out.name} left as it was; this run's result is in {res['written']}")
        if res["journal"]:
            print(f"  resume: rerun the same command (answered seeds are kept in {Path(res['journal']).name})")
        if any("fewer than the published" in r for r in res["reasons"]):
            print("  if the drop is real, rerun with --force to publish it")
    print(f"  {s2_line(res)}")
    small = {k: v for k, v in res.items() if k != "s2"}
    small["s2"] = {k: res["s2"].get(k) for k in ("calls", "attempts", "attempts_not_ok", "budget", "aborted")}
    print(SUMMARY_MARKER + json.dumps(small, ensure_ascii=False), flush=True)


def s2_line(res) -> str:
    s = res["s2"]
    return (f"[s2] {s.get('key')} calls={s.get('calls')} attempts={s.get('attempts')} "
            f"not_ok_attempts={s.get('attempts_not_ok')} budget={s.get('budget')}"
            + (f" ABORTED={s.get('aborted')}" if s.get("aborted") else ""))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    ap.add_argument("--project", default=None,
                     help="Project name from projects.json (e.g. 'research_a').")
    ap.add_argument("--lib-dir", default=None,
                     help="Explicit library path (legacy).")
    ap.add_argument("--report",  default=None,
                     help="Output CSV (default: <lib-dir>/_forward_citations.csv).")
    ap.add_argument("--limit",   type=int, default=0,
                     help="Walk the first N seeds only (testing); the others keep their published rows.")
    ap.add_argument("--sleep",   type=float, default=None,
                     help="Deprecated and ignored: spacing comes from litpipe.s2 (projects.json s2 block).")
    ap.add_argument("--s2-key",  default=None,
                     help="Deprecated: set the S2_API_KEY environment variable instead.")
    ap.add_argument("--force",   action="store_true",
                     help="Publish even when the result has fewer seeds with citers than the published report.")
    ap.add_argument("--restart", action="store_true",
                     help="Ignore a resumable <report>.partial.jsonl and walk every seed again.")
    args = ap.parse_args(argv)
    res = run(project=args.project, lib_dir=args.lib_dir, report=args.report, limit=args.limit,
              sleep=args.sleep, s2_key=args.s2_key, force=args.force, restart=args.restart)
    return res["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
