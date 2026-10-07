"""Worklists, pools and curation helpers (W4-D): pure readers over the files the pipeline writes.

Every function here reads files (and, for `ill_list`, a read-only DuckDB connection the caller
opened) and returns rows; none writes, except `Pool`'s state file and the CLI's `--write PATH`.

Worklists
  oa_blocked(registry, *, projects=None, holdmap=None) -> list[dict]
      Every row of every project's `lit_pull_queue.oa_blocked.md` (migrate_closed_to_md writes it;
      this module only reads it). Keys: doi, title, year, link, cause, via, host, publisher, project,
      checked, plus key (the holdings DOI key), parsed and, with a holdmap, held. A checked-off line
      (`- [x]`) is done: it is reported, never listed as open.
  group_by_host(rows) -> {label: [rows]}
      By the host after `via \\`<stage>:<host>\\``; a line without one (or with only Unpaywall's
      host type, `publisher` / `repository`) goes under the publisher its DOI prefix names
      (PUBLISHERS), else under the prefix itself. Groups with more open rows come first.
  render_oa_worklist(groups, date) -> str
      Markdown, one section per host or publisher; one line per DOI with a link that opens in one
      click (the row's own link, else migrate_closed_to_md.doi_url).
  ill_list(registry, *, con, projects=None, include_held=False) -> list[dict]
      The open rows of every project's ILL list (`lit_pull_queue.md`; migrate routes only
      TERMINAL_CLOSED rows there), one row per DOI with every requesting project, ranked by
      co-citation count (`project_cocitations.n_own_citing` for the requesting projects, the
      largest) and then by `top_candidates.n_seeds_pointing`, ties broken by DOI.
  seed_coverage(project, registry) -> dict
      The share of the library's top-level PDFs named as a `seed` in
      `<lib>/_reverse_citations_parsed.csv`; `warn` below SEED_COVERAGE_WARN (0.80); the
      unparsed PDFs' stems.
  residual_csvs(registry) / read_residuals(csvs)
      Sweep's residual CSVs in each registered project root and its direct subdirectories
      (`archive` and `_archive` skipped), and their rows filtered on `residual_class`.

Pool drawdown (the contract `python -m litpipe.runner batch` is built on, W4-A)
  A pool is a CSV with at least a `doi` column, ranked top to bottom. Its state lives beside it in
  `<pool stem>.drawdown.json`, rewritten atomically on every change and re-read by every call, so
  state survives a restart and two Pool objects on the same file see the same state after a write.
  State is keyed by DOI (litpipe.holdings.doi_key), never by row number: a pool CSV edited since
  the last write keeps every DOI's state (`pool_changed` in status() says the file changed).

    pool = Pool(csv_path, registry=reg)              # or holdings=<HoldMap>
    for b in pool.pending():                         # 1. staged before a kill, never swept
        if not b["exists"]:                          # killed between mark_staged and the write
            write the batch queue file at b["batch_path"] from b["dois"]
        sweep b["batch_path"] with run id b["run_id"]
        pool.mark_swept(b["dois"], b["run_id"], classes)
    rows = pool.next_batch(size)                     # 2. pure: top rows neither staged, swept nor held
    pool.mark_staged([r["doi"] for r in rows], run_id, batch_path)   # before the file is written
    write the batch queue file from rows
    sweep the batch with run_id                      # 3.
    pool.mark_swept(dois, run_id, classes)           # classes: {doi: residual class or "fetched"}
    archive the staged set; route                    # 4, 5.

  A kill after mark_staged leaves the batch in pending() (exists false when its file was never
  written) and next_batch() skips its rows, so the runner never re-stages them; a kill before
  mark_staged loses nothing and leaves no untracked queue file (the rows are drawn again). A kill
  after the sweep but before mark_swept sweeps that batch again on resume unless the runner takes
  its classes from the run's routing CSV.
  seed_from(path) imports a VAP-style state file (`staged_dois`): each DOI is marked staged AND
  swept with class "imported", so it is never pending and never drawn; the import is recorded by
  the file's sha256 in `seeded_from`, so importing the same file twice imports once.

CLI (dry by default; a worklist .md is written only with --write PATH):
  python -m litpipe.worklists oa-blocked [--project KEY ...] [--date D] [--no-holdings] [--show] [--write PATH]
  python -m litpipe.worklists ill [--project KEY ...] [--db PATH] [--limit N] [--include-held] [--write PATH]
  python -m litpipe.worklists coverage [--project KEY ...]
  python -m litpipe.worklists pool-status --pool CSV [--seed-state PATH [--write]]
Exit codes: 0 done, 1 usage, registry or index error.
"""
import argparse
import csv
import datetime as _dt
import hashlib
import json
import os
import re
import sys
from pathlib import Path
from urllib.parse import quote

import lit_util
import migrate_closed_to_md as _migrate
from litpipe import config
from litpipe import doi as _doi
from litpipe import holdings as _holdings

CONFIG_PATH = Path(__file__).resolve().parent.parent / "projects.json"
DB_NAME = "portfolio.duckdb"

OA_BLOCKED_NAME = _migrate.OA_BLOCKED_NAME
ILL_NAME = _migrate.ILL_NAME
TERMINAL_CLOSED = _migrate.TERMINAL_CLOSED
RESIDUAL_GLOB = "lit_pull_queue.*.residual.csv"
SKIP_DIRS = frozenset({"archive", "_archive"})
REVERSE_PARSED = "_reverse_citations_parsed.csv"
SEED_COVERAGE_WARN = 0.80
STATE_SUFFIX = ".drawdown.json"
STATE_VERSION = 1
IMPORTED = "imported"

# Unpaywall's host types (`via \`unpaywall:publisher\``): they name a kind of host, not a host.
HOST_TYPES = frozenset({"publisher", "repository"})

# The publisher a DOI prefix names, for a blocked row whose line names no host. A small table of
# the prefixes these libraries meet; an unlisted prefix is its own group.
PUBLISHERS = {
    "10.1001": "American Medical Association",
    "10.1002": "Wiley",
    "10.1007": "Springer",
    "10.1016": "Elsevier",
    "10.1017": "Cambridge University Press",
    "10.1021": "American Chemical Society",
    "10.1038": "Springer Nature",
    "10.1055": "Thieme",
    "10.1056": "Massachusetts Medical Society",
    "10.1073": "PNAS",
    "10.1080": "Taylor & Francis",
    "10.1088": "IOP Publishing",
    "10.1089": "Mary Ann Liebert",
    "10.1093": "Oxford University Press",
    "10.1097": "Wolters Kluwer",
    "10.1101": "Cold Spring Harbor Laboratory",
    "10.1109": "IEEE",
    "10.1111": "Wiley",
    "10.1113": "Wiley",
    "10.1123": "Human Kinetics",
    "10.1126": "AAAS",
    "10.1136": "BMJ",
    "10.1139": "Canadian Science Publishing",
    "10.1152": "American Physiological Society",
    "10.1161": "American Heart Association",
    "10.1177": "SAGE",
    "10.1186": "BioMed Central",
    "10.1210": "Endocrine Society",
    "10.1249": "Wolters Kluwer",
    "10.1371": "PLOS",
    "10.1519": "Wolters Kluwer",
    "10.2165": "Springer",
    "10.3389": "Frontiers",
    "10.3390": "MDPI",
}


class WorklistError(ValueError):
    """A usage, registry or index problem: the CLI exits 1 with a one-line message."""


class PoolStateError(WorklistError):
    """A pool's state file (or an imported one) cannot be read: never silently reset."""


# ---------------------------------------------------------------- registry
def _projects(registry):
    """The `projects` mapping of a loaded projects.json, or of a bare projects mapping."""
    if registry is None:
        raise WorklistError("a registry (a loaded projects.json) is required")
    if isinstance(registry, dict) and isinstance(registry.get("projects"), dict):
        return registry["projects"]
    if isinstance(registry, dict):
        return registry
    raise WorklistError(f"the registry must be a dict, got {type(registry).__name__}")


def _select(registry, projects=None):
    """[(key, entry)] in registry order, optionally only `projects` (an unknown key raises)."""
    reg = _projects(registry)
    if projects is not None:
        unknown = [k for k in projects if k not in reg]
        if unknown:
            raise WorklistError(f"not in projects.json: {', '.join(map(repr, unknown))}")
        wanted = set(projects)
        return [(k, reg[k] or {}) for k in reg if k in wanted]
    return [(k, p or {}) for k, p in reg.items() if isinstance(p, dict) or p is None]


def load_registry(cfg=None):
    """projects.json through litpipe.config.load: `cfg` when given, else this module's CONFIG_PATH.
    WorklistError (exit 1) when the file is missing or unreadable."""
    if cfg is not None:
        return config.load(cfg)
    p = Path(CONFIG_PATH)
    if not p.exists():
        raise WorklistError(f"projects.json not found at {p} (copy projects.json.template)")
    try:
        reg = config.load(lit_util.load_projects_config(p))
    except (OSError, ValueError) as e:
        raise WorklistError(f"projects.json at {p} is unreadable: {type(e).__name__}: {e}") from None
    if not isinstance(reg, dict) or not isinstance(reg.get("projects") or {}, dict):
        raise WorklistError("projects.json has no `projects` object")
    return reg


# ---------------------------------------------------------------- DOIs and links
def doi_key(raw):
    return _holdings.doi_key(raw)


def doi_prefix(doi):
    """`10.1016` from any DOI form; "" when there is none."""
    d = _doi.normalise_structured(doi) or str(doi or "").strip().lower()
    m = re.match(r"(10\.\d{4,9})/", d)
    return m.group(1) if m else ""


def publisher(doi):
    """The publisher a DOI's prefix names (PUBLISHERS), else the prefix, else "unknown"."""
    p = doi_prefix(doi)
    return PUBLISHERS.get(p, p or "unknown")


def doi_link(doi):
    """A doi.org link for a DOI, percent-encoded for a URL path (DOI Handbook 2025, 4.7).

    migrate_closed_to_md.doi_url builds it, which survives a DOI that litpipe.doi.encode_path
    rejects (a SICI `;2-#`). The one exception is a registered DOI that the free-text rule
    shortens (`10.1088/2053-1591/acdecd`): its whole structured form is encoded with the same
    4.7 set, so the link never points at a journal-level DOI (W5 unifies the two rules)."""
    s = _doi.normalise_structured(doi)
    if s and s != _doi.normalise(doi):
        prefix, suffix = s.split("/", 1)
        safe = _doi._PATH_SAFE
        return "https://doi.org/" + quote(prefix, safe=safe) + "/" + quote(suffix, safe=safe + "/")
    return _migrate.doi_url(doi)


# ---------------------------------------------------------------- markdown line parsing
_CHECKBOX = re.compile(r"^\s*[-*]\s+\[(?P<mark>[ xX])\]\s+(?P<body>.*?)\s*$")
_OA_LINE = re.compile(
    r"^\*\*(?P<title>.*?)\*\*(?:\s+\((?P<year>[^()]*)\))?\s+\[(?P<doi>.+)\]\((?P<link>\S+)\)\s+"
    r"cause\s+`(?P<cause>[^`]*)`\s+via\s+`(?P<via>[^`]*)`$")
_MD_LINK = re.compile(r"\[(?P<text>[^\]]+)\]\((?P<link>[^)\s]+(?:\([^)\s]*\)[^)\s]*)*)\)")
_BOLD = re.compile(r"\*\*(?P<title>.*?)\*\*(?:\s+\((?P<year>[^()]*)\))?")
_DOI_TICK = re.compile(r"DOI `([^`]+)`")


def _read_lines(path):
    try:
        return Path(path).read_text(encoding="utf-8", errors="replace").splitlines()
    except FileNotFoundError:
        return None


def _checkbox_lines(path):
    """(line number, checked, body) of every checkbox line of a markdown list."""
    out = []
    for i, line in enumerate(_read_lines(path) or (), 1):
        m = _CHECKBOX.match(line)
        if m:
            out.append((i, m.group("mark") != " ", m.group("body")))
    return out


def _host_of(via):
    _stage, _, host = (via or "").partition(":")
    host = host.strip()
    return "" if host.lower() in HOST_TYPES else host


def parse_oa_line(body):
    """One OA-blocked line body (after the checkbox) as a dict, or a best-effort dict with
    parsed=False when it does not have the renderer's shape (a hand edit)."""
    m = _OA_LINE.match(body)
    if m:
        via = m.group("via")
        return {"doi": m.group("doi").strip(), "title": m.group("title").strip(),
                "year": (m.group("year") or "").strip(), "link": m.group("link"),
                "cause": m.group("cause"), "via": via, "host": _host_of(via), "parsed": True}
    link = _MD_LINK.search(body)
    bold = _BOLD.search(body)
    doi = link.group("text").strip() if link else ""
    if not _doi.normalise(doi):
        found = _holdings.extract_dois(body)
        doi = found[0] if found else doi
    return {"doi": doi, "title": bold.group("title").strip() if bold else "",
            "year": ((bold.group("year") if bold else "") or "").strip(),
            "link": link.group("link") if link else "", "cause": "", "via": "", "host": "", "parsed": False}


def oa_blocked(registry, *, projects=None, holdmap=None):
    """Every row of every project's OA-blocked list, ordered by project (registry order), DOI key,
    then line. Lines with no DOI at all are skipped. With `holdmap`, each row says whether the DOI
    is now held anywhere (`held`)."""
    out = []
    for key, entry in _select(registry, projects):
        path = lit_util.project_root(key, entry) / OA_BLOCKED_NAME
        rows = []
        for line_no, checked, body in _checkbox_lines(path):
            r = parse_oa_line(body)
            if not r["doi"]:
                continue
            r.update(key=doi_key(r["doi"]), publisher=publisher(r["doi"]), project=key,
                     checked=checked, line=line_no)
            if holdmap is not None:
                r["held"] = bool(holdmap.where(r["doi"]))
            rows.append(r)
        rows.sort(key=lambda r: (r["key"], r["line"]))
        out.extend(rows)
    return out


def group_label(row):
    return row.get("host") or row.get("publisher") or publisher(row.get("doi"))


def _done(row):
    return bool(row.get("checked") or row.get("held"))


def group_by_host(rows):
    """{label: [rows]}: by host when the line names one, else by the DOI prefix's publisher. Rows
    in a group are ordered by DOI key, then project; groups by open rows (desc), then label."""
    def order(r):
        return (r.get("key") or doi_key(r.get("doi")), r.get("project", ""), r.get("line", 0))

    label = {}   # every row of one DOI goes to the group of its first row: listed once, done once
    for r in sorted(rows, key=order):
        label.setdefault(order(r)[0], group_label(r))
    groups = {}
    for r in rows:
        groups.setdefault(label[order(r)[0]], []).append(r)
    for rs in groups.values():
        rs.sort(key=order)

    def n_open(rs):
        return len(_merge(rs)[0])

    return {k: groups[k] for k in sorted(groups, key=lambda k: (-n_open(groups[k]), k.lower(), k))}


def _merge(rows):
    """(open, done) lists of merged rows, one per DOI key: a DOI checked off (or held) in any
    project is done everywhere; the projects that list it are joined."""
    by_key = {}
    for r in rows:
        by_key.setdefault(r.get("key") or doi_key(r.get("doi")), []).append(r)
    open_, done = [], []
    for k in sorted(by_key):
        rs = by_key[k]
        first = rs[0]
        merged = {**first, "projects": sorted({r.get("project", "") for r in rs} - {""})}
        (done if any(_done(r) for r in rs) else open_).append(merged)
    return open_, done


def _one_click(row):
    link = (row.get("link") or "").strip()
    return link if link.lower().startswith(("http://", "https://")) else doi_link(row.get("doi"))


def _plain(s):
    return re.sub(r"\s+", " ", str(s or "").replace("**", "")).strip()


def render_oa_worklist(groups, date):
    """The portfolio OA-blocked worklist as Markdown: one section per host or publisher."""
    total_open = total_done = 0
    sections = []
    for label, rows in groups.items():
        open_, done = _merge(rows)
        total_open += len(open_)
        total_done += len(done)
        if not open_:
            continue
        lines = ["", f"## {label}: {len(open_)} open" + (f", {len(done)} done" if done else ""), ""]
        for r in open_:
            year = f" ({r['year']})" if r.get("year") else ""
            cause = f" cause `{r['cause']}`" if r.get("cause") else ""
            via = f" via `{r['via']}`" if r.get("via") else ""
            lines.append(f"- [ ] **{_plain(r.get('title')) or '(no title)'}**{year} "
                         f"[{r['doi']}]({_one_click(r)}){cause}{via} for {', '.join(r['projects'])}")
        sections.append("\n".join(lines))
    n_groups = sum(1 for rows in groups.values() if _merge(rows)[0])
    head = [f"# Open-access worklist, {date}", "",
            f"{total_open} open papers in {n_groups} groups, by blocked host, else by the publisher "
            f"the DOI prefix names. Each link opens the paper in one click; save the PDF into the "
            f"library of a project it is listed for, then check the line off in that project's "
            f"`{OA_BLOCKED_NAME}`. {total_done} done (checked off, or held) are not listed."]
    return "\n".join(head) + "\n" + "\n".join(sections) + ("\n" if sections else "")


# ---------------------------------------------------------------- ILL list
def parse_ill_line(body):
    """(doi, title, year) of an ILL list line: the `DOI \\`...\\`` token migrate writes, else a
    doi.org link or any DOI in the line. ("", ...) when the line names no DOI (a hand note)."""
    m = _DOI_TICK.search(body)
    doi = m.group(1).strip() if m else ""
    if not doi:
        found = _holdings.extract_dois(body)
        doi = found[0] if found else ""
    bold = _BOLD.search(body)
    title = bold.group("title").strip() if bold else ""
    year = ((bold.group("year") if bold else "") or "").strip()
    return doi, title, year


def _chunks(seq, n=500):
    seq = list(seq)
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def _db_form(doi):
    return _doi.normalise_structured(doi) or doi_key(doi)


def _query(con, sql, params, what):
    try:
        return con.execute(sql, params).fetchall()
    except Exception as e:   # duckdb's error classes are not a stable public API
        raise WorklistError(f"the index cannot answer {what} ({type(e).__name__}: {str(e)[:160]}); "
                            f"re-run index_portfolio.py") from None


def ill_list(registry, *, con, projects=None, include_held=False):
    """The open ILL rows of every project, one per DOI, ranked (see the module docstring). Each row:
    doi, key, title, year, projects (every requesting project), n_own_citing (the largest over
    them), n_own_citing_by_project, n_seeds_pointing, held_anywhere, link. A DOI the index holds
    anywhere is left out unless include_held."""
    by_key = {}
    for key, entry in _select(registry, projects):
        path = lit_util.project_root(key, entry) / ILL_NAME
        for _line, checked, body in _checkbox_lines(path):
            if checked:
                continue
            doi, title, year = parse_ill_line(body)
            if not doi:
                continue
            k = doi_key(doi)
            e = by_key.setdefault(k, {"doi": doi, "key": k, "title": "", "year": "", "projects": set()})
            e["projects"].add(key)
            if not e["title"] and title:
                e["title"] = title
            if not e["year"] and year:
                e["year"] = year
    if not by_key:
        return []
    forms = {}
    for k, e in by_key.items():
        for f in {_db_form(e["doi"]), k}:
            forms.setdefault(f, set()).add(k)
    req = sorted({p for e in by_key.values() for p in e["projects"]})
    cocit, seeds, held = {}, {}, set()
    for chunk in _chunks(sorted(forms)):
        marks = ",".join("?" * len(chunk))
        for proj, d, n in _query(con, f"SELECT project, doi, n_own_citing FROM project_cocitations "
                                      f"WHERE project IN ({','.join('?' * len(req))}) AND doi IN ({marks})",
                                 req + chunk, "project_cocitations"):
            for k in forms.get(d, ()):
                cocit.setdefault(k, {})[proj] = max(int(n or 0), cocit.get(k, {}).get(proj, 0))
        for d, n in _query(con, f"SELECT doi, n_seeds_pointing FROM top_candidates WHERE doi IN ({marks})",
                           chunk, "top_candidates"):
            for k in forms.get(d, ()):
                seeds[k] = max(int(n or 0), seeds.get(k, 0))
        for (d,) in _query(con, f"SELECT DISTINCT doi FROM paper_locations WHERE doi IN ({marks})",
                           chunk, "paper_locations"):
            held.update(forms.get(d, ()))
    rows = []
    for k, e in by_key.items():
        if k in held and not include_held:
            continue
        by_proj = {p: cocit.get(k, {}).get(p, 0) for p in sorted(e["projects"])}
        rows.append({"doi": e["doi"], "key": k, "title": e["title"], "year": e["year"],
                     "projects": sorted(e["projects"]), "n_own_citing": max(by_proj.values(), default=0),
                     "n_own_citing_by_project": by_proj, "n_seeds_pointing": seeds.get(k, 0),
                     "held_anywhere": k in held, "link": doi_link(e["doi"])})
    rows.sort(key=lambda r: (-r["n_own_citing"], -r["n_seeds_pointing"], r["key"]))
    return rows


def render_ill_list(rows, date):
    lines = [f"# ILL worklist, {date}", "",
             f"{len(rows)} closed-access papers from the projects' `{ILL_NAME}` lists, ranked by "
             f"co-citation count (how many papers a requesting project holds cite it), then by how "
             f"many seed papers point to it.", ""]
    for i, r in enumerate(rows, 1):
        year = f" ({r['year']})" if r.get("year") else ""
        lines.append(f"{i}. **{_plain(r.get('title')) or '(no title)'}**{year} [{r['doi']}]({r['link']}): "
                     f"co-cited {r['n_own_citing']}, seeds {r['n_seeds_pointing']}; "
                     f"for {', '.join(r['projects'])}")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------- seed coverage
def _seed_stem(name):
    s = str(name or "").strip()
    low = s.lower()
    for ext in (".pdf", ".txt"):
        if low.endswith(ext):
            s = s[:-len(ext)]
            break
    return s.casefold()


def seed_coverage(project, registry):
    """{project, library, csv, n_pdfs, n_parsed, share, warn, unparsed}: the share of the library's
    top-level PDFs (the reverse walker's seeds) named in the `seed` column of
    `<lib>/_reverse_citations_parsed.csv`. share is None for a library with no PDF; warn is true
    below SEED_COVERAGE_WARN. A seed that yielded no reference row is not named there, so it counts
    as unparsed."""
    (key, entry), = _select(registry, [project])
    if not entry.get("lib_dir"):
        raise WorklistError(f"projects.json gives {key!r} no lib_dir")
    lib = lit_util.lib_paths(key, entry)[1]
    try:
        pdfs = sorted((e.name[:-4] for e in os.scandir(lib) if e.is_file() and e.name.lower().endswith(".pdf")),
                      key=lambda s: (s.casefold(), s))
    except FileNotFoundError:
        raise WorklistError(f"library not found for {key!r}: {lib}") from None
    parsed_csv = lib / REVERSE_PARSED
    seeds = set()
    if parsed_csv.exists():
        with open(parsed_csv, encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                if row.get("seed"):
                    seeds.add(_seed_stem(row["seed"]))
    unparsed = [s for s in pdfs if s.casefold() not in seeds]
    n_parsed = len(pdfs) - len(unparsed)
    share = (n_parsed / len(pdfs)) if pdfs else None
    return {"project": key, "library": str(lib), "csv": str(parsed_csv), "csv_exists": parsed_csv.exists(),
            "n_pdfs": len(pdfs), "n_parsed": n_parsed, "share": share,
            "warn": share is not None and share < SEED_COVERAGE_WARN, "unparsed": unparsed}


# ---------------------------------------------------------------- residual CSVs
def residual_csvs(registry, *, projects=None):
    """[(project key, path)] of every residual CSV (`lit_pull_queue.*.residual.csv`) in each
    registered project's root (lit_util.project_root) and its direct subdirectories (sweep's
    relative --artifact-dir), skipping `archive` and `_archive`. A file reachable from two projects
    (a subproject root is also its parent's subdirectory) belongs to the one whose root holds it.
    Ordered by path."""
    found = {}
    for order, (key, entry) in enumerate(_select(registry, projects)):
        root = lit_util.project_root(key, entry)
        if not root.is_dir():
            continue
        dirs = [(0, root)]
        try:
            dirs += [(1, Path(e.path)) for e in os.scandir(root)
                     if e.is_dir() and e.name.lower() not in SKIP_DIRS]
        except OSError:
            pass
        for depth, d in dirs:
            for p in d.glob(RESIDUAL_GLOB):
                if not p.is_file():
                    continue
                norm = os.path.normcase(os.path.abspath(p))
                cand = (depth, order, key, p)
                if norm not in found or cand[:2] < found[norm][:2]:
                    found[norm] = cand
    return [(found[n][2], found[n][3]) for n in sorted(found)]


def read_residuals(csvs, *, keep=frozenset({TERMINAL_CLOSED})):
    """(rows, stats) from residual CSVs: [(project, row)] keeping a typed row only when its
    `residual_class` is in `keep`, and every row of a legacy CSV (no `residual_class` column).
    stats: csvs, legacy_csvs, rows_read, rows_kept, excluded {class: n}, unreadable [paths]."""
    rows = []
    stats = {"csvs": 0, "legacy_csvs": 0, "rows_read": 0, "rows_kept": 0, "excluded": {}, "unreadable": []}
    for proj, path in csvs:
        try:
            with open(path, encoding="utf-8-sig", errors="replace", newline="") as f:
                rd = csv.DictReader(f)
                typed = "residual_class" in (rd.fieldnames or [])
                got = list(rd)
        except OSError:
            stats["unreadable"].append(str(path))
            continue
        stats["csvs"] += 1
        stats["legacy_csvs"] += not typed
        for row in got:
            stats["rows_read"] += 1
            cls = (row.get("residual_class") or "").strip() if typed else ""
            if typed and cls not in keep:
                stats["excluded"][cls or "(blank)"] = stats["excluded"].get(cls or "(blank)", 0) + 1
                continue
            stats["rows_kept"] += 1
            rows.append((proj, row))
    return rows, stats


# ---------------------------------------------------------------- pool drawdown
def _now():
    return _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")


def _sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def state_path_for(pool_path):
    p = Path(pool_path)
    return p.with_name(p.stem + STATE_SUFFIX)


class Pool:
    """A ranked pool drawn down in batches; see the module docstring for the contract."""

    def __init__(self, path, *, registry=None, holdings=None, cache_dir=None):
        self.path = Path(path)
        self.cache_dir = cache_dir
        self.state_path = state_path_for(self.path)
        self.registry = registry
        self._holdmap = holdings

    # -- files
    def rows(self):
        """The pool's rows in rank order, one per DOI key (the first wins); blank DOIs skipped."""
        try:
            with open(self.path, encoding="utf-8-sig", newline="") as f:
                rd = csv.DictReader(f)
                col = next((c for c in rd.fieldnames or [] if str(c or "").strip().lower() == "doi"), None)
                if col is None:
                    raise WorklistError(f"pool {self.path.name} has no `doi` column")
                out, seen = [], set()
                for row in rd:
                    raw = (row.get(col) or "").strip()
                    if not raw:
                        continue
                    k = doi_key(raw)
                    if k in seen:
                        continue
                    seen.add(k)
                    out.append(dict(row) if col == "doi" else {**row, "doi": raw})
                return out
        except FileNotFoundError:
            raise WorklistError(f"pool not found: {self.path}") from None

    def _empty(self):
        return {"version": STATE_VERSION, "pool": self.path.name, "pool_sha256": None,
                "seeded_from": [], "imports": [], "updated_at": None, "dois": {}}

    def _load(self):
        if not self.state_path.exists():
            return self._empty()
        try:
            st = json.loads(self.state_path.read_text(encoding="utf-8-sig"))
        except (OSError, ValueError) as e:
            raise PoolStateError(f"pool state {self.state_path} is unreadable ({type(e).__name__}: {e}); "
                                 f"fix or move it, it is never reset silently") from None
        if not isinstance(st, dict) or not isinstance(st.get("dois"), dict):
            raise PoolStateError(f"pool state {self.state_path} has no `dois` object")
        bad = next((k for k, v in st["dois"].items() if not isinstance(v, dict)), None)
        if bad is not None:
            raise PoolStateError(f"pool state {self.state_path}: the record for {bad!r} is not an object")
        for k, v in self._empty().items():
            st.setdefault(k, v)
        return st

    def _save(self, st):
        st["pool"] = self.path.name
        st["pool_sha256"] = _sha256(self.path) if self.path.exists() else st.get("pool_sha256")
        st["updated_at"] = _now()
        lit_util.atomic_write_text(str(self.state_path),
                                   json.dumps(st, indent=1, sort_keys=True, ensure_ascii=False) + "\n")

    def _held(self):
        if self._holdmap is None:
            if self.registry is None:
                raise WorklistError("Pool needs registry= or holdings= to leave held rows out "
                                    "(or call next_batch(exclude_held=False))")
            # next_batch is pure: read a warm holdings cache, write one only into an explicit
            # cache_dir (the runner's), never the state dir implicitly.
            self._holdmap = _holdings.build(self.registry, cache_dir=self.cache_dir,
                                            write_cache=self.cache_dir is not None)
        return self._holdmap

    # -- the drawdown API
    def next_batch(self, size, *, exclude_held=True):
        """The top `size` rows that are neither staged nor swept nor (by default) held anywhere.
        Pure: reads the pool and its state, writes nothing."""
        if size is None or int(size) < 1:
            return []
        st = self._load()["dois"]
        hm = self._held() if exclude_held else None
        out = []
        for row in self.rows():
            k = doi_key(row["doi"])
            rec = st.get(k) or {}
            if rec.get("staged") or rec.get("swept"):
                continue
            if hm is not None and hm.where(row["doi"]):
                continue
            out.append(row)
            if len(out) >= int(size):
                break
        return out

    def mark_staged(self, dois, run_id, batch_path):
        """Record that `dois` were staged in `batch_path` for `run_id` (a re-stage clears `swept`)."""
        if not str(run_id or "").strip():
            raise WorklistError("mark_staged needs a run id")
        st = self._load()
        at, n = _now(), 0
        for d in dois:
            raw = str(d or "").strip()
            if not raw:
                continue
            rec = st["dois"].setdefault(doi_key(raw), {})
            rec["doi"] = rec.get("doi") or raw
            rec["staged"] = {"run_id": str(run_id), "batch_path": str(batch_path), "at": at}
            rec.pop("swept", None)
            n += 1
        self._save(st)
        return n

    def mark_swept(self, dois, run_id, classes):
        """Record the sweep of `dois` under `run_id`; `classes` maps each DOI (any form) to its
        residual class or "fetched". A DOI with no class raises: the caller must say."""
        cls = {doi_key(k): str(v) for k, v in (classes or {}).items()}
        dois = [str(d or "").strip() for d in dois if str(d or "").strip()]
        missing = [d for d in dois if doi_key(d) not in cls]
        if missing:
            raise WorklistError(f"mark_swept: no class for {len(missing)} DOI(s), first {missing[0]!r}")
        st = self._load()
        at = _now()
        for d in dois:
            k = doi_key(d)
            rec = st["dois"].setdefault(k, {})
            rec["doi"] = rec.get("doi") or d
            rec["swept"] = {"run_id": str(run_id), "class": cls[k], "at": at}
        self._save(st)
        return len(dois)

    def pending(self):
        """[{run_id, batch_path, dois, exists}]: rows staged and never swept, grouped by batch,
        oldest batch first; DOIs in pool rank order."""
        st = self._load()["dois"]
        try:
            rank = {doi_key(r["doi"]): i for i, r in enumerate(self.rows())}
        except WorklistError:
            rank = {}
        groups = {}
        for k, rec in st.items():
            s = rec.get("staged")
            if not s or rec.get("swept"):
                continue
            g = groups.setdefault((s.get("run_id", ""), s.get("batch_path", "")), {"ats": [], "keys": []})
            g["ats"].append(s.get("at") or "")
            g["keys"].append(k)
        out = []
        for (run_id, batch), g in sorted(groups.items(), key=lambda kv: (min(kv[1]["ats"]), kv[0])):
            keys = sorted(g["keys"], key=lambda k: (rank.get(k, len(rank)), k))
            out.append({"run_id": run_id, "batch_path": batch, "dois": [st[k].get("doi") or k for k in keys],
                        "exists": bool(batch) and Path(batch).exists()})
        return out

    def status(self):
        st = self._load()
        recs = st["dois"]
        rows = self.rows()
        keys = [doi_key(r["doi"]) for r in rows]
        classes = {}
        for rec in recs.values():
            if rec.get("swept"):
                c = rec["swept"].get("class", "")
                classes[c] = classes.get(c, 0) + 1
        pend = self.pending()
        out = {"pool": str(self.path), "state": str(self.state_path), "state_exists": self.state_path.exists(),
               "rows": len(rows),
               "staged": sum(1 for r in recs.values() if r.get("staged")),
               "swept": sum(1 for r in recs.values() if r.get("swept")),
               "pending": sum(len(b["dois"]) for b in pend), "pending_batches": len(pend),
               "remaining": sum(1 for k in keys if not (recs.get(k, {}).get("staged") or recs.get(k, {}).get("swept"))),
               "classes": dict(sorted(classes.items())),
               "not_in_pool": len(set(recs) - set(keys)),
               "pool_changed": bool(st.get("pool_sha256")) and st["pool_sha256"] != _sha256(self.path),
               "seeded_from": list(st.get("seeded_from") or [])}
        return out

    def seed_from(self, path, *, dry_run=False):
        """Import a VAP-style state file: every DOI in its `staged_dois` list is marked staged and
        swept with class "imported" (never pending, never drawn). DOIs this pool already tracks are
        left as they are. Idempotent through `seeded_from` (the file's sha256)."""
        p = Path(path)
        try:
            data = p.read_bytes()
            src = json.loads(data.decode("utf-8-sig"))
        except (OSError, ValueError) as e:
            raise PoolStateError(f"cannot read the state to import, {p}: {type(e).__name__}: {e}") from None
        staged = src.get("staged_dois") if isinstance(src, dict) else None
        if not isinstance(staged, list):
            raise PoolStateError(f"{p.name} has no `staged_dois` list")
        sha = hashlib.sha256(data).hexdigest()
        st = self._load()
        if sha in (st.get("seeded_from") or []):
            return {"imported": 0, "already_tracked": 0, "already_seeded": True, "sha256": sha, "dry_run": dry_run}
        at, n_new, n_old, seen = _now(), 0, 0, set()
        for item in staged:
            raw = str((item.get("doi") if isinstance(item, dict) else item) or "").strip()
            if not raw or doi_key(raw) in seen:
                continue
            k = doi_key(raw)
            seen.add(k)
            if k in st["dois"]:
                n_old += 1
                continue
            st["dois"][k] = {"doi": raw,
                             "staged": {"run_id": IMPORTED, "batch_path": str(p), "at": at},
                             "swept": {"run_id": IMPORTED, "class": IMPORTED, "at": at}}
            n_new += 1
        res = {"imported": n_new, "already_tracked": n_old, "already_seeded": False, "sha256": sha,
               "dry_run": dry_run}
        if not dry_run:
            st["seeded_from"] = list(st.get("seeded_from") or []) + [sha]
            st["imports"] = list(st.get("imports") or []) + [{"sha256": sha, "file": p.name, "at": at,
                                                              "imported": n_new, "already_tracked": n_old}]
            self._save(st)
        return res


# ---------------------------------------------------------------- CLI
def _today():
    return _dt.date.today().isoformat()


def _default_db(registry):
    return config.db_dir(registry) / DB_NAME


def run(command, *, projects=None, date=None, write=None, db=None, limit=None, include_held=False,
        pool=None, seed_state=None, no_holdings=False, show=False, registry=None) -> dict:
    """One CLI command; returns {"exit_code", "command", ...}. Writes only with `write`."""
    res = {"command": command, "exit_code": 0, "written": None}
    date = date or _today()
    try:
        if command == "pool-status":
            if not pool:
                raise WorklistError("pool-status needs --pool CSV")
            pl = Pool(pool, registry=registry)
            if seed_state:
                res["seed"] = pl.seed_from(seed_state, dry_run=not write)
            res["status"] = pl.status()
            return res
        reg = registry if registry is not None else load_registry()
        if command == "oa-blocked":
            hm = None if no_holdings else _holdings.build(reg, write_cache=False)
            rows = oa_blocked(reg, projects=projects, holdmap=hm)
            groups = group_by_host(rows)
            text = render_oa_worklist(groups, date)
            open_ = {lbl: len(_merge(rs)[0]) for lbl, rs in groups.items()}
            res.update(rows=len(rows), open=sum(open_.values()), groups=open_,
                       done=sum(len(_merge(rs)[1]) for rs in groups.values()), markdown=text)
        elif command == "ill":
            import duckdb
            dbp = Path(db) if db else _default_db(reg)
            if not dbp.exists():
                raise WorklistError(f"no index at {dbp} (run index_portfolio.py, or pass --db)")
            try:
                con = duckdb.connect(str(dbp), read_only=True)
            except Exception as e:   # a lock (Drive sync, a writer) or a damaged file
                raise WorklistError(f"cannot open the index {dbp} read-only: {type(e).__name__}: {e}") from None
            try:
                rows = ill_list(reg, con=con, projects=projects, include_held=include_held)
            finally:
                con.close()
            if limit:
                rows = rows[:int(limit)]
            text = render_ill_list(rows, date)
            res.update(db=str(dbp), rows=len(rows), top=rows[:10], markdown=text)
        elif command == "coverage":
            keys = projects or [k for k, p in _select(reg) if p.get("lib_dir")]
            res["coverage"] = [seed_coverage(k, reg) for k in keys]
            text = None
        else:
            raise WorklistError(f"unknown command {command!r}")
        if write and text is not None:
            out = Path(write)
            out.parent.mkdir(parents=True, exist_ok=True)
            lit_util.atomic_write_text(str(out), text)
            res["written"] = str(out)
    except (WorklistError, config.ConfigError) as e:
        res.update(exit_code=1, error=str(e))
    return res


def _print(res, show):
    cmd = res["command"]
    if res["exit_code"]:
        print(f"[ERR] {res.get('error')}", file=sys.stderr)
        return
    if cmd == "oa-blocked":
        print(f"{res['open']} open, {res['done']} done, {len(res['groups'])} groups")
        for lbl, n in res["groups"].items():
            if n:
                print(f"  {n:>5}  {lbl}")
    elif cmd == "ill":
        print(f"DB: {res['db']}\n{res['rows']} open ILL rows")
        for r in res["top"]:
            print(f"  co-cited {r['n_own_citing']:>3}  seeds {r['n_seeds_pointing']:>3}  {r['doi']}  "
                  f"({', '.join(r['projects'])})")
    elif cmd == "coverage":
        for c in res["coverage"]:
            share = "n/a" if c["share"] is None else f"{c['share']:.0%}"
            tag = "[WARN]" if c["warn"] else "[OK]"
            print(f"{tag} {c['project']}: {c['n_parsed']}/{c['n_pdfs']} PDFs parsed as seeds ({share})"
                  + (f", {len(c['unparsed'])} unparsed" if c["unparsed"] else ""))
    elif cmd == "pool-status":
        if "seed" in res:
            s = res["seed"]
            verb = "would import" if s["dry_run"] else "imported"
            print("seed state already imported (same sha256)" if s["already_seeded"]
                  else f"{verb} {s['imported']} DOIs ({s['already_tracked']} already tracked)")
        for k, v in res["status"].items():
            print(f"  {k}: {v}")
    if show and res.get("markdown"):
        print(res["markdown"])
    if res.get("written"):
        print(f"wrote {res['written']}")
    elif res.get("markdown") is not None:
        print("dry run: nothing written (pass --write PATH)")


def main(argv=None) -> int:
    lit_util.utf8_stdout()
    ap = argparse.ArgumentParser(prog="python -m litpipe.worklists",
                                 description="Worklists, pools and curation helpers (dry by default).")
    sub = ap.add_subparsers(dest="command", required=True)
    oa = sub.add_parser("oa-blocked", help="the portfolio OA-blocked browser worklist, grouped by host")
    ill = sub.add_parser("ill", help="the ILL list ranked by co-citation count and n_seeds_pointing")
    cov = sub.add_parser("coverage", help="share of each library's PDFs parsed as reverse-walk seeds")
    ps = sub.add_parser("pool-status", help="a pool's drawdown state; --seed-state imports a VAP state")
    for p in (oa, ill, cov):
        p.add_argument("--project", action="append", default=None, help="only this project (repeatable)")
    for p in (oa, ill):
        p.add_argument("--date", default=None, help="the date in the heading (default: today)")
        p.add_argument("--write", default=None, metavar="PATH", help="write the worklist .md here")
        p.add_argument("--show", action="store_true", help="print the worklist markdown")
    oa.add_argument("--no-holdings", action="store_true", help="do not mark DOIs held anywhere as done")
    ill.add_argument("--db", default=None, help="index (default: <db_dir>/portfolio.duckdb), opened read-only")
    ill.add_argument("--limit", type=int, default=None)
    ill.add_argument("--include-held", action="store_true", help="keep DOIs the index holds somewhere")
    ps.add_argument("--pool", required=True, help="the pool CSV")
    ps.add_argument("--seed-state", default=None, metavar="PATH", help="a VAP-style state (staged_dois) to import")
    ps.add_argument("--write", action="store_true", help="with --seed-state: import it (default: dry run)")
    args = ap.parse_args(sys.argv[1:] if argv is None else argv)
    kw = {"projects": getattr(args, "project", None), "date": getattr(args, "date", None),
          "write": getattr(args, "write", None), "db": getattr(args, "db", None),
          "limit": getattr(args, "limit", None), "include_held": getattr(args, "include_held", False),
          "no_holdings": getattr(args, "no_holdings", False), "show": getattr(args, "show", False)}
    if args.command == "pool-status":
        kw.update(pool=args.pool, seed_state=args.seed_state, write=args.write)
    res = run(args.command, **kw)
    _print(res, kw["show"])
    return res["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
