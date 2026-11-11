"""Candidate titles: tell a real title from a parsed-reference fragment, and look real ones up.

Problem (librarian, 2026-11-10, `notes/2026-11-10.md`): reverse-citation parsing stored the
reference string, or nothing, as `paper_metadata.title`, and `paper_metadata` is insert-if-absent,
so a fragment that landed first was never replaced. 19,975 of 110,981 seedable candidates had an
empty title, a reference fragment (`; 41(1): 3-12`, `doi: 10.48550/...`, `[PubMed]`), a URL, an
author list, or markup.

  clean_title(raw)       the display form (tags stripped, references decoded) of a real title, or
                         None when it is empty or one of the junk kinds below. Writers store None
                         (NULL) so a real source can fill the row later.
  junk_kind(raw)         'empty' | 'url' | 'reference_fragment' | 'author_list' | None (a title).
  crossref_title(doi)    (title or None, status), status 'ok' | 'no_title' | 'not_found' | an error.
  arxiv_titles(dois)     {doi: title} for 10.48550/arXiv.* DOIs, 50 ids per export.arxiv.org call.
  lookup(dois)           {doi: (title or None, status)}: arXiv DOIs through arXiv, the rest Crossref.

Refresh (CLI): python -m litpipe.titles --project KEY [--project KEY2] [--commit] [--limit N]
  Targets: the project's seedable candidates (top_candidates rows whose via_projects names the
  project, n_seeds_pointing >= --min-seeds (2), not held anywhere) whose stored title is junk or
  carries markup. Without --commit it is a dry run: it counts by kind, prints examples, sends
  nothing and writes nothing. With --commit it looks titles up (Crossref 0.34 s, arXiv 3.5 s per
  call, both paced by litpipe.net), writes a found title, cleans a markup-only title in place,
  and sets a junk title with no lookup answer to NULL. A Crossref 404 is reported as `not_found`
  (often a truncated DOI from reference parsing, e.g. `10.1007/s1257`); nothing else is done to it.
  Re-runnable: a second run targets only what is still junk. Exit 0 clean, 1 config/DB error,
  2 when lookups failed (transient; the next run retries them).
"""
from __future__ import annotations

import argparse
import re
import sys
import time
from xml.etree import ElementTree as ET

from litpipe import doi as _doi
from litpipe import hosts, net
from litpipe.text import display_field, has_markup

CROSSREF = "https://api.crossref.org/works/{doi}"
ARXIV_API = "https://export.arxiv.org/api/query"
ARXIV_BATCH = 50
_ATOM = "{http://www.w3.org/2005/Atom}"
_ARXIV_DOI = re.compile(r"^10\.48550/arxiv\.(.+)$", re.IGNORECASE)

_URL = re.compile(r"^\s*(?:https?://|www\.|(?:dx\.)?doi\.org/)", re.IGNORECASE)
# A reference fragment: starts with list punctuation or a volume(issue)/volume: page run, or carries
# a reference-list marker anywhere. A title starting with "(" or "[" is NOT flagged: "(-)-Epicatechin
# ..." and PubMed's bracketed translated titles ("[Heat stroke in athletes]") are real.
_FRAGMENT = re.compile(
    r"^\s*[;,.:]"
    r"|^\s*\d+\s*\(\s*\d+\s*\)"            # 41(1)
    r"|^\s*\d+\s*[:;]\s*\d"                # 41: 3-12
    r"|\d+\s*\(\s*\d+\s*\)\s*:\s*e?\d"      # Nov 30;12(12):3692, 6(7): e1000097 anywhere
    r"|\b(?:1[89]|20)\d\d\s*;\s*\d+\s*[:(]"  # Med Sci Sports Exerc 2013;45:824-31.
    r"|\b[A-Z]?\d+\s*[-–]\s*[A-Z]?\d+\s*\((?:1[89]|20)\d\d\)"   # 47, D1005–D1012 (2019).
    r"|^\s*[-–]\s*\d"                      # –2019: a systematic analysis ... (a cut title)
    r"|^\s*vol\.?\s*\d|\bpp\.\s*\d+\s*[-–]\s*\d"   # vol. 52, no 4, pp. 869–873.
    r"|^\s*URL\b|\bURL:\s*https?://|\bhttps?://\S*doi\.org"
    r"|^\s*\?"                             # ? A study of the 2006 heat wave
    r"|\[PubMed\]|\bGoogle Scholar\b|\bPMID\b|\[DOI\b|\bdoi:\s*10\.|\[(?:Online|Internet|cited)\b",
    re.IGNORECASE)
# Two or more "Surname, I." / "Surname I," items from the start: an author list, not a title.
_AUTHORS = re.compile(
    r"^(?:[A-Z][\w'\-]+,?\s+(?:[A-Z]\.?\s?){1,3}[,;&]\s*(?:and\s+|&\s*)?){2,}"
    r"|^(?:[A-Z][\w'\-]+\s+[A-Z]{1,3},\s*){2,}")
# No run of three letters (any script): `02.021 (2019).`, `(2017).`, `–2020`, `100, 1400–1409 (2006).`
_WORDY = re.compile(r"[^\W\d_]{3}")
# A journal abbreviation standing alone (at most seven words with a bare "J"): `J Clin Oncol`,
# `Am J Physiol Regul Integr Comp Physiol`.
_JOURNAL = re.compile(r"^(?=(?:\S+\s+){0,6}\S+$).*(?:^|\s)J\.?(?:\s|$)")


def junk_kind(raw) -> str | None:
    """The junk kind of a stored title, None when it reads as a real title (after cleaning)."""
    t = display_field(raw or "")
    if not t:
        return "empty"
    if _URL.search(t):
        return "url"
    if _FRAGMENT.search(t) or not _WORDY.search(t) or _JOURNAL.search(t):
        return "reference_fragment"
    if _AUTHORS.search(t):
        return "author_list"
    return None


def clean_title(raw) -> str | None:
    """The display form of a real title, or None for empty and junk (see junk_kind)."""
    if junk_kind(raw):
        return None
    return display_field(raw)


# ------------------------------------------------------------------------------ lookups
def crossref_title(doi, timeout=15):
    """(title or None, status) from Crossref's record for `doi`."""
    try:
        path = _doi.encode_path(doi)
    except ValueError:
        return None, "not_a_doi"
    out = net.get(CROSSREF.format(doi=path), timeout=timeout, validate=net.expect_json,
                  purpose="titles")
    if not out.ok:
        if out.status in (404, 410):
            return None, "not_found"
        return None, f"error:{out.kind}"
    try:
        msg = out.payload.json().get("message") or {}
    except (ValueError, AttributeError):
        return None, "error:bad_json"
    titles = msg.get("title") or []
    title = clean_title(titles[0] if isinstance(titles, list) and titles else titles if isinstance(titles, str) else "")
    return (title, "ok") if title else (None, "no_title")


def arxiv_id(doi) -> str | None:
    m = _ARXIV_DOI.match((doi or "").strip())
    return m.group(1).lower() if m else None


def _strip_version(i):
    return re.sub(r"v\d+$", "", i)


def arxiv_titles(dois, timeout=30):
    """({doi: title}, failed_dois) for arXiv DOIs, ARXIV_BATCH ids per export.arxiv.org call."""
    ids = {arxiv_id(d): d for d in dois if arxiv_id(d)}
    found, failed = {}, []
    keys = list(ids)
    for i in range(0, len(keys), ARXIV_BATCH):
        chunk = keys[i:i + ARXIV_BATCH]
        out = net.get(ARXIV_API, params={"id_list": ",".join(chunk), "max_results": str(len(chunk))},
                      timeout=timeout, purpose="titles")
        if not out.ok or out.payload is None:
            failed.extend(ids[k] for k in chunk)
            continue
        try:
            root = ET.fromstring(out.payload.content or b"")
        except ET.ParseError:
            failed.extend(ids[k] for k in chunk)
            continue
        for entry in root.iter(f"{_ATOM}entry"):
            eid = (entry.findtext(f"{_ATOM}id") or "").rsplit("/abs/", 1)[-1].strip().lower()
            key = _strip_version(eid)
            title = clean_title(" ".join((entry.findtext(f"{_ATOM}title") or "").split()))
            if key in ids and title and title.lower() != "error":
                found[ids[key]] = title
    return found, failed


def lookup(dois):
    """{doi: (title or None, status)} for every DOI given."""
    dois = list(dict.fromkeys(dois))
    res = {}
    ax = [d for d in dois if arxiv_id(d)]
    if ax:
        found, failed = arxiv_titles(ax)
        failed = set(failed)
        for d in ax:
            res[d] = (found[d], "ok") if d in found else (None, "error:arxiv" if d in failed else "no_title")
    for d in dois:
        if d not in res:
            res[d] = crossref_title(d)
    return res


# ------------------------------------------------------------------------------ refresh
_TARGETS_SQL = """
SELECT t.doi, m.title
FROM top_candidates t
LEFT JOIN paper_metadata m ON m.doi = t.doi
WHERE (',' || t.via_projects || ',') LIKE ?
  AND t.n_seeds_pointing >= ?
  AND NOT EXISTS (SELECT 1 FROM paper_locations l WHERE l.doi = t.doi)
ORDER BY t.n_seeds_pointing DESC, t.doi
"""


def targets(con, projects, min_seeds=2):
    """[(doi, stored title, kind)] where kind is a junk kind or 'markup' (a real title with tags)."""
    out, seen = [], set()
    for p in projects:
        for doi, title in con.execute(_TARGETS_SQL, [f"%,{p},%", min_seeds]).fetchall():
            if doi in seen:
                continue
            seen.add(doi)
            k = junk_kind(title)
            if k is None and has_markup(title or ""):
                k = "markup"
            if k:
                out.append((doi, title, k))
    return out


def _write(con, doi, title):
    con.execute("UPDATE paper_metadata SET title = ? WHERE doi = ?", [title, doi])


def refresh(con, projects, *, commit=False, min_seeds=2, limit=0, every=200, log=print):
    """Dry run (commit=False): counts only. Commit: look up, write, NULL unanswered junk."""
    rows = targets(con, projects, min_seeds)
    if limit:
        rows = rows[:limit]
    by_kind = {}
    for _d, _t, k in rows:
        by_kind[k] = by_kind.get(k, 0) + 1
    res = {"projects": list(projects), "targets": len(rows), "by_kind": by_kind, "commit": commit,
           "filled": 0, "cleaned": 0, "nulled": 0, "not_found": [], "failed": 0}
    log(f"targets: {len(rows)} {by_kind}")
    for d, t, k in rows[:8]:
        log(f"  {k:<18} {d}  {(t or '')[:90]!r}")
    if not commit:
        return res
    markup = [(d, t) for d, t, k in rows if k == "markup"]
    junk = [d for d, _t, k in rows if k != "markup"]
    t0 = time.monotonic()
    pace = hosts.policy(CROSSREF).min_interval_s
    log(f"lookups: {len(junk)} (ETA ~{len(junk) * pace / 60:.1f} min); markup cleaned in place: {len(markup)}")
    con.execute("BEGIN")
    try:
        for d, t in markup:
            _write(con, d, display_field(t))
            res["cleaned"] += 1
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    for i in range(0, len(junk), every):
        chunk = junk[i:i + every]
        answers = lookup(chunk)
        con.execute("BEGIN")
        try:
            for d in chunk:
                title, status = answers[d]
                if title:
                    _write(con, d, title)
                    res["filled"] += 1
                elif status.startswith("error"):
                    res["failed"] += 1          # transient: leave the row for the next run
                else:
                    _write(con, d, None)
                    res["nulled"] += 1
                    if status == "not_found":
                        res["not_found"].append(d)
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        log(f"  [{min(i + every, len(junk))}/{len(junk)}] filled={res['filled']} nulled={res['nulled']} "
            f"failed={res['failed']} ({time.monotonic() - t0:.0f} s)")
    if res["not_found"]:
        log(f"not_found at Crossref ({len(res['not_found'])}, often truncated DOIs): "
            + ", ".join(res["not_found"][:10]))
    return res


def main(argv=None) -> int:
    import duckdb
    from litpipe import config
    ap = argparse.ArgumentParser(description="Refresh junk candidate titles for one or more projects.")
    ap.add_argument("--project", action="append", required=True, metavar="KEY")
    ap.add_argument("--commit", action="store_true", help="Look up and write (default: dry run).")
    ap.add_argument("--min-seeds", type=int, default=2)
    ap.add_argument("--limit", type=int, default=0, help="First N targets only.")
    ap.add_argument("--db", default=None, help="Default: portfolio.duckdb in projects.json db_dir.")
    args = ap.parse_args(argv)
    try:
        reg = config.load(None)
        unknown = [p for p in args.project if p not in (reg.get("projects") or {})]
        if unknown:
            print(f"not in projects.json: {', '.join(unknown)}", file=sys.stderr)
            return 1
        db = args.db or str(config.db_dir(reg) / "portfolio.duckdb")
        con = duckdb.connect(db, read_only=not args.commit)
    except (config.ConfigError, duckdb.Error) as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    try:
        res = refresh(con, args.project, commit=args.commit, min_seeds=args.min_seeds, limit=args.limit)
    finally:
        con.close()
    print(f"summary: {res}" if not args.commit else
          f"summary: targets={res['targets']} filled={res['filled']} cleaned={res['cleaned']} "
          f"nulled={res['nulled']} not_found={len(res['not_found'])} failed={res['failed']}")
    return 2 if res["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
