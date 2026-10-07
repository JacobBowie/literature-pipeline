"""Browser paywall pull: the manual library-access session over the priority paywall queue.

The pipeline fetches what is open access. What remains is paywalled to anonymous traffic and can
only be pulled by hand through an authenticated library session. `build_priority_paywall_queue.py`
ranks those papers into `<portfolio_dir>/<date>_priority_paywall_queue.csv`; this script drives the
session over that queue:

  1. opens the next batch of still-missing papers in your default browser (where you are signed in
     to your library's access),
  2. shows which project library each PDF goes into (the queue's `projects` column decides it),
  3. after you have saved the PDFs, files them (--finish, a dry run; --apply moves them), runs
     backfill_ris and index_portfolio for each library it touched, and reports what landed.

"Done" is read from the libraries themselves through litpipe.holdings (a DOI held anywhere, as a
PDF or a text-only holding, drops off the queue; an identity-flagged file is not a holding), so the
session resumes across days with no bookkeeping. The only state kept is a small ledger of DOIs
opened but not yet saved (`_paywall_pull_opened.json` in the portfolio folder, else beside the
queue), so consecutive --open calls advance instead of re-opening the same tabs.

Configuration (projects.json, global keys; there is no environment variable and no default):
  portfolio_dir  the folder that holds the queue files and the ledger (build_priority_paywall_queue
                 writes there). '~' expands; a relative path is under the projects root. Unset,
                 pass --queue; with neither the tool exits 1.
  ezproxy_host   your library's EZproxy host, for --access ezproxy; --ezproxy-host overrides it.

Usage:
  python paywall_pull.py                            # status: how many left, what's next
  python paywall_pull.py --open 8                   # open the next 8 in the browser
  python paywall_pull.py --open 8 --access ezproxy --ezproxy-host ezproxy.example.edu
  python paywall_pull.py --finish                   # dry run: what would be filed where
  python paywall_pull.py --finish --apply           # file them, backfill .ris, reindex
  python paywall_pull.py --reset-opened             # clear the opened-not-saved ledger

Library access:
  --access doi (the default) opens the queue's `url` column, the percent-encoded
  https://doi.org/<DOI> link (litpipe.doi.encode_path when a row has none). It reaches full text
  when your browser has a library session or a link-resolver extension. --access ezproxy routes
  through an EZproxy that proxies by hostname: https://doi-org.<host>/<DOI>, so the whole redirect
  chain (doi.org, then the publisher, each host rewritten with dashes) stays behind the proxy
  login. This opens only the front door you are entitled to; it bypasses nothing.

Filing (--finish): a PDF in --drop-dir (default: the Downloads folder in your home directory, one
workflow; name any folder) matches a queue row by the DOI suffix in its file name, else by the
DOI in its first two pages, else by an explicit `<project key>_` file-name prefix. A file without
`%PDF` in its first 1,024 bytes (a web page saved as `.pdf`) is never filed, and neither is a
known misfetch page (unpaywall_fetch_v2.is_known_boilerplate; an interlibrary-loan candidate).

Exit codes: 0 done; 1 usage or configuration (no queue found, neither "portfolio_dir" nor --queue,
--access ezproxy with no host, an unreadable registry).
"""
import argparse
import csv
import glob
import json
import os
import shutil
import subprocess
import sys
import time
import webbrowser
from pathlib import Path

import lit_util
lit_util.utf8_stdout()

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from build_priority_paywall_queue import portfolio_dir  # noqa: E402  (the shared "portfolio_dir" key)
from litpipe import config, holdings  # noqa: E402
from litpipe import doi as _doi  # noqa: E402

# Optional helpers reused for the --finish ingest stage (all degrade gracefully):
try:
    from unpaywall_fetch_v2 import is_known_boilerplate   # publisher permission-page detector
except Exception:
    is_known_boilerplate = None
try:
    import pymupdf  # for pulling the DOI out of a downloaded PDF
except Exception:
    pymupdf = None
try:
    from lit_util import extract_doi_from_text
except Exception:
    extract_doi_from_text = None
try:
    from ris_emit import emit_ris_for_pdf   # enh-ii: write .ris from a KNOWN queue DOI on move
except Exception:
    emit_ris_for_pdf = None

LEDGER_NAME = "_paywall_pull_opened.json"
QUEUE_GLOB = "*_priority_paywall_queue.csv"
PDF_MAGIC_WINDOW = 1024     # extract_pdf_fulltext's rule: %PDF within the first 1,024 bytes
PY = sys.executable
NO_QUEUE = ('paywall_pull: no queue folder: set "portfolio_dir" (global) in projects.json, the folder '
            'build_priority_paywall_queue.py writes to, or pass --queue PATH')
NO_EZPROXY = ('paywall_pull: --access ezproxy needs your library\'s EZproxy host: set "ezproxy_host" '
              '(global) in projects.json, or pass --ezproxy-host HOST')


class UsageError(ValueError):
    """A usage or configuration problem: main prints one line and exits 1."""


def norm(d):
    """The holdings key of a DOI (litpipe.holdings.doi_key), so done-detection matches holdings."""
    return holdings.doi_key(d)


def libraries(registry):
    """{project key: library Path} for every active registered project with a lib_dir."""
    out = {}
    for key, p in ((registry or {}).get("projects") or {}).items():
        if isinstance(p, dict) and p.get("active", True) and p.get("lib_dir"):
            out[key] = lit_util.lib_paths(key, p)[1]
    return out


def ezproxy_host(registry, override=None):
    """--ezproxy-host, else projects.json "ezproxy_host", as a bare host name; "" when neither."""
    raw = override if override else (registry or {}).get("ezproxy_host")
    if raw is None or raw == "":
        return ""
    if not isinstance(raw, str):
        raise UsageError(f"paywall_pull: ezproxy_host must be a host name string, got {raw!r}")
    host = raw.strip()
    for scheme in ("https://", "http://"):
        if host.lower().startswith(scheme):
            host = host[len(scheme):]
    return host.strip("/")


def find_latest_queue(folder):
    cands = sorted(glob.glob(os.path.join(str(folder), QUEUE_GLOB)))
    return cands[-1] if cands else None


def load_queue(path):
    rows = []
    with open(path, encoding="utf-8-sig", newline="") as f:
        for r in csv.DictReader(f):
            r["doi_raw"] = (r.get("doi") or "").strip()      # for the URL (don't over-strip)
            r["doi_norm"] = norm(r.get("doi"))               # for matching against libs
            try:
                r["rank_int"] = int(r.get("rank") or 0)
            except ValueError:
                r["rank_int"] = 10**9
            rows.append(r)
    return rows


def load_ledger(path):
    try:
        with open(path, encoding="utf-8") as f:
            return set(json.load(f))
    except (OSError, ValueError, TypeError):
        return set()


def save_ledger(path, s):
    os.makedirs(os.path.dirname(str(path)) or ".", exist_ok=True)
    lit_util.atomic_write_text(str(path), json.dumps(sorted(s), indent=0))


def primary_lib(projects_field, libs):
    """First project in the queue's `projects` list that has a known library."""
    for p in [x.strip() for x in (projects_field or "").split(",") if x.strip()]:
        if p in libs:
            return p, str(libs[p]).replace("\\", "/")
    return None, None


def prefix_route(stem, libs):
    """Route a file by an explicit '<project>_' filename prefix (a manual convention). Matches the
    longest project key or subproject tail, so keys that contain underscores themselves
    ('research_alpha_*', 'teaching_course_b_*') resolve to the right project."""
    s = stem.lower()
    best = None
    for key in libs:
        for cand in (key.lower(), key.split("/")[-1].lower()):
            if s.startswith(cand + "_") and (best is None or len(cand) > len(best[1])):
                best = (key, cand)
    if best:
        return best[0], str(libs[best[0]]).replace("\\", "/")
    return None, None


def doi_url(doi_raw, url=None):
    """The queue row's encoded doi.org link (`url` column), else one built with
    litpipe.doi.encode_path (DOI Handbook 4.7)."""
    if url and url.lower().startswith("https://doi.org/"):
        return url
    try:
        return "https://doi.org/" + _doi.encode_path(doi_raw)
    except ValueError:
        from urllib.parse import quote
        return "https://doi.org/" + quote((doi_raw or "").strip(), safe="/")


def build_url(doi_raw, access, ezhost, url=None):
    base = doi_url(doi_raw, url)
    if access == "ezproxy" and ezhost:
        # An EZproxy that proxies by hostname rewrites each host (dots to dashes) under its own
        # domain rather than taking a login?url= starting point. Opening the proxied doi.org
        # resolver routes the whole redirect chain through the proxy:
        #   doi-org.<host>/<doi> -> <publisher-host-with-dashes>.<host>/...
        return f"https://doi-org.{ezhost}/" + base[len("https://doi.org/"):]
    return base


def cmd_status(rows, have, queue_path, ledger, libs):
    done = [r for r in rows if r["doi_norm"] in have]
    pending = [r for r in rows if r["doi_norm"] not in have]
    opened = load_ledger(ledger) & {r["doi_norm"] for r in pending}
    print(f"Queue: {os.path.basename(queue_path)}")
    print(f"  total: {len(rows)}   in-library (done): {len(done)}   pending: {len(pending)}")
    print(f"  opened, not yet saved: {len(opened)}")
    nxt = sorted((r for r in pending if r["doi_norm"] not in opened),
                 key=lambda r: r["rank_int"])[:8]
    if nxt:
        print("\nNext up:")
        for r in nxt:
            proj, _ = primary_lib(r.get("projects"), libs)
            print(f"  [{r['rank']:>3}] {(r.get('title') or '')[:68]:<68}  -> {proj or '?'}")
    print("\nNext: python paywall_pull.py --open 8")
    return {"total": len(rows), "done": len(done), "pending": len(pending), "opened": len(opened)}


def cmd_open(rows, have, n, access, ezhost, ledger, libs, opener=None, pause=1.2):
    opener = opener or webbrowser.open
    opened = load_ledger(ledger)
    pending = sorted((r for r in rows
                      if r["doi_norm"] not in have and r["doi_norm"] not in opened),
                     key=lambda r: r["rank_int"])
    batch = pending[:n]
    if not batch:
        print("Nothing new to open (all pending are already opened or in a library).")
        print("Run --finish after you have saved them, or --reset-opened to re-open.")
        return {"opened": []}
    print(f"Opening {len(batch)} papers in your default browser ({access}).")
    print("Save each PDF into the folder shown, then: python paywall_pull.py --finish\n")
    urls = []
    for r in batch:
        proj, libpath = primary_lib(r.get("projects"), libs)
        url = build_url(r["doi_raw"], access, ezhost, r.get("url"))
        others = [x.strip() for x in (r.get("projects") or "").split(",")
                  if x.strip() and x.strip() != proj]
        tag = f"   (also wanted by: {', '.join(others)})" if others else ""
        print(f"[{r['rank']:>3}] {r.get('title') or '(title pending)'} ({r.get('year') or 'n.d.'})")
        print(f"      save to: {libpath or '(unknown lib -- pick manually)'}{tag}")
        print(f"      {url}")
        opener(url)
        urls.append(url)
        opened.add(r["doi_norm"])
        if pause:
            time.sleep(pause)   # let the browser settle between tabs
    save_ledger(ledger, opened)
    print(f"\nOpened {len(batch)}. Ledger now tracks {len(opened)} opened-not-saved DOIs.")
    return {"opened": urls}


def is_pdf_file(path):
    """True when `%PDF` is in the file's first 1,024 bytes (False when it cannot be read)."""
    try:
        with open(path, "rb") as fh:
            return b"%PDF" in fh.read(PDF_MAGIC_WINDOW)
    except OSError:
        return False


def _doi_from_pdf(path):
    """Pull a DOI from a downloaded PDF (first 2 pages) for filing when the filename
    doesn't carry it (e.g. ScienceDirect PII-named files)."""
    if pymupdf is None:
        return None
    try:
        with pymupdf.open(path) as doc:
            text = "".join(doc[i].get_text() for i in range(min(2, doc.page_count)))
    except Exception:
        return None
    if extract_doi_from_text:
        return extract_doi_from_text(text)
    ds = holdings.extract_dois(text or "")
    return ds[0] if ds else None


def plan_ingest(rows, drop_dir, libs):
    """Match PDFs in drop_dir to queue rows. Returns (to_file, boilerplate, unmatched, dups, not_pdf).

    A file without %PDF in its first 1,024 bytes is not a PDF (a saved landing page) and is never
    filed. Match order: (1) the publisher filename often IS the DOI suffix (s40279-...-z.pdf),
    (2) else pull the DOI from the PDF text, (3) else an explicit '<project>_' prefix. Each
    candidate is run through the boilerplate detector before filing (the 2026-05-22 trap), so a
    saved permissions doc is rejected to ILL instead of polluting a library."""
    suffix_map = {}
    for r in rows:
        suf = r["doi_norm"].split("/", 1)[-1]
        if suf:
            suffix_map[suf] = r
    by_norm = {r["doi_norm"]: r for r in rows}
    to_file, boiler, unmatched, not_pdf = [], [], [], []
    for pdf in sorted(glob.glob(os.path.join(str(drop_dir), "*.pdf"))):
        if not is_pdf_file(pdf):
            not_pdf.append(pdf)
            continue
        stem = os.path.splitext(os.path.basename(pdf))[0].lower()
        row = None
        for suf in sorted(suffix_map, key=len, reverse=True):
            if len(suf) >= 6 and suf in stem:
                row = suffix_map[suf]
                break
        if row is None:
            found = _doi_from_pdf(pdf)
            row = by_norm.get(norm(found)) if found else None
        if row is None:
            # Fallback: honor an explicit "<project>_" filename prefix (user convention).
            pproj, plib = prefix_route(stem, libs)
            if plib:
                to_file.append((pdf, {"doi_norm": "", "projects": pproj}, pproj, plib))
                continue
            unmatched.append(pdf)
            continue
        if is_known_boilerplate is not None:
            try:
                bad, tag = is_known_boilerplate(pdf)
            except Exception:
                bad, tag = False, None
            if bad:
                boiler.append((pdf, row, tag or "boilerplate"))
                continue
        proj, lib = primary_lib(row.get("projects"), libs)
        if lib is None:   # no known library for this row: cannot file it; route to manual
            unmatched.append(pdf)
            continue
        to_file.append((pdf, row, proj, lib))
    # Dedup by DOI: a paper downloaded twice (foo.pdf + "foo (1).pdf") files once.
    # Keep the shortest basename (the clean original, not the " (1)" copy).
    best, dups = {}, []
    for item in to_file:
        d = item[1].get("doi_norm") or ("file::" + os.path.basename(item[0]))
        if d not in best:
            best[d] = item
        elif len(os.path.basename(item[0])) < len(os.path.basename(best[d][0])):
            dups.append(best[d])
            best[d] = item
        else:
            dups.append(item)
    return list(best.values()), boiler, unmatched, dups, not_pdf


def _run_child(cmd):
    return subprocess.run(cmd, encoding="utf-8", errors="replace").returncode


def cmd_finish(rows, have_before, drop_dir, apply, *, libs, ledger, have_after_fn, child=None):
    child = child or _run_child
    print(f"Scanning {drop_dir} for downloaded PDFs that match the queue...\n")
    to_file, boiler, unmatched, dups, not_pdf = plan_ingest(rows, drop_dir, libs)
    print(f"  ready to file: {len(to_file)}   duplicate copies: {len(dups)}   "
          f"boilerplate (-> ILL): {len(boiler)}   not a PDF: {len(not_pdf)}   unmatched: {len(unmatched)}\n")
    for pdf, row, proj, lib in to_file:
        print(f"  FILE  {os.path.basename(pdf)}")
        print(f"          -> {lib}/   [{proj}]  {row.get('doi_norm') or '(by filename prefix)'}")
    for pdf, row, proj, lib in dups:
        print(f"  DUP   {os.path.basename(pdf)}  (same DOI as a kept file, skipped)")
    for pdf, row, tag in boiler:
        print(f"  SKIP  {os.path.basename(pdf)}  (boilerplate: {tag}; {row['doi_norm']} -> ILL)")
    for pdf in not_pdf:
        print(f"  SKIP  {os.path.basename(pdf)}  (not a PDF: no %PDF in its first 1,024 bytes, a saved "
              f"web page? left in place; download the PDF itself)")
    for pdf in unmatched:
        print(f"  ??    {os.path.basename(pdf)}  (no DOI match -- not in this queue, left in place)")
    res = {"to_file": [p for p, *_ in to_file], "dups": [p for p, *_ in dups],
           "boilerplate": [p for p, *_ in boiler], "not_pdf": list(not_pdf), "unmatched": list(unmatched),
           "moved": [], "failed_projects": []}
    if not apply:
        print("\nDRY RUN -- nothing moved. Re-run with --apply to file these, then "
              "backfill .ris + reindex.")
        return res

    moved, touched, failed = 0, set(), set()
    ris_w = ris_skip = ris_fail = 0
    for pdf, row, proj, lib in to_file:
        os.makedirs(lib, exist_ok=True)
        dest = os.path.join(lib, os.path.basename(pdf))
        if os.path.exists(dest):
            print(f"  [skip] already present: {dest}")
            # enh-ii self-heal: an already-present file may still LACK a .ris (e.g. a prior
            # round's CrossRef failure on an LWW PDF, whose DOI isn't in extractable text).
            # Re-attempt the known-DOI .ris (EXISTS_SKIP-idempotent) and re-touch the project so
            # the safety-net backfill + reindex run for it instead of leaving it stranded.
            doi0 = (row.get("doi_raw") or row.get("doi_norm") or "").strip()
            if doi0 and emit_ris_for_pdf is not None:
                try:
                    emit_ris_for_pdf(doi0, dest, overwrite=False)
                except Exception:
                    pass
            touched.add(proj)
            continue
        shutil.move(pdf, dest)
        moved += 1
        res["moved"].append(dest)
        touched.add(proj)
        # enh-ii (2026-06-25): when the file matched a queue DOI, write its .ris straight from
        # that KNOWN DOI (CrossRef). LWW/paywalled PDFs don't expose their DOI in extractable
        # text, so the generic backfill below NO_DOIs them; this fixes the LWW strand and makes
        # backfill a safety net only (for prefix-routed / no-DOI files).
        doi = (row.get("doi_raw") or row.get("doi_norm") or "").strip()
        if doi and emit_ris_for_pdf is not None:
            try:
                st, _ = emit_ris_for_pdf(doi, dest, overwrite=False)
                if st == "OK":
                    ris_w += 1
                elif st == "EXISTS_SKIP":
                    ris_skip += 1
                else:
                    ris_fail += 1
                    print(f"  [ris {st}] {os.path.basename(dest)} ({doi})")
            except Exception as e:
                ris_fail += 1
                print(f"  [ris ERR] {os.path.basename(dest)}: {e}")
    print(f"\nMoved {moved} files into {len(touched)} libs "
          f"(.ris from known DOI: {ris_w} written, {ris_skip} existed, {ris_fail} failed).")
    print("Backfilling any remaining .ris (safety net) + reindexing...\n")

    # T3 (2026-06-25): check returncodes. A crashed backfill leaves a project .ris-less and
    # invisible to the index; do NOT clear its DOIs from the ledger, skip its reindex, and tell
    # the operator to re-run backfill (paywall_pull does not auto-retry -- the PDF is already
    # moved out of the drop dir, so the next --finish cannot re-find it).
    for p in sorted(touched):
        lib = str(libs[p]).replace("\\", "/")
        rc = child([PY, os.path.join(HERE, "backfill_ris.py"), "--lib-dir", lib, "--commit"])
        if rc != 0:
            print(f"  [ERR] backfill_ris failed for {p} (exit {rc}); skipping its reindex. Re-run "
                  f"`backfill_ris.py --lib-dir {lib} --commit` then `index_portfolio.py --project {p}`.")
            failed.add(p)
    for p in sorted(touched):
        if p in failed:
            continue
        rc = child([PY, os.path.join(HERE, "index_portfolio.py"), "--project", p])
        if rc != 0:
            print(f"  [ERR] index_portfolio failed for {p} (exit {rc})")
            failed.add(p)

    have_after = have_after_fn()
    newly = [r for r in rows if r["doi_norm"] in have_after and r["doi_norm"] not in have_before]
    # T3: clear the ledger only for DOIs whose project did NOT fail this round. A failed project's
    # files are already moved into its lib but may lack a .ris, so leave their DOIs un-cleared
    # (status keeps showing them pending) until backfill is re-run for that lib.
    cleared = {r["doi_norm"] for r in rows
               if r["doi_norm"] in have_after and primary_lib(r.get("projects"), libs)[0] not in failed}
    save_ledger(ledger, load_ledger(ledger) - cleared)
    print(f"\nNewly in libraries this round: {len(newly)}")
    if failed:
        print(f"FAILED projects (re-run backfill_ris --commit + reindex for these): {', '.join(sorted(failed))}")
    remaining = [r for r in rows if r["doi_norm"] not in have_after]
    print(f"Remaining pending: {len(remaining)} / {len(rows)}")
    res.update(failed_projects=sorted(failed), newly=len(newly), remaining=len(remaining))
    return res


def _held(registry, rows):
    """The holdings keys of the queue rows held anywhere (litpipe.holdings; the cache is read when
    present and never written)."""
    hm = holdings.build(registry, write_cache=False)
    return {r["doi_norm"] for r in rows if r["doi_norm"] and hm.where(r["doi_raw"] or r["doi_norm"])}


def run(*, queue=None, open_n=None, finish=False, drop_dir=None, apply=False, access="doi",
        ezproxy_host_arg=None, reset_opened=False, cfg=None, child=None, opener=None, pause=1.2) -> dict:
    """One paywall-pull command (status, --open, --finish, --reset-opened; module docstring).
    Raises UsageError for a usage or configuration problem."""
    try:
        registry = config.load(cfg)
        folder = portfolio_dir(registry)
    except (OSError, ValueError) as e:      # config.ConfigError is a ValueError
        raise UsageError(f"paywall_pull: {e}") from None
    host = ezproxy_host(registry, ezproxy_host_arg)
    if access == "ezproxy" and not host:
        raise UsageError(NO_EZPROXY)
    if queue:
        queue_path = str(queue)
    elif folder is None:
        raise UsageError(NO_QUEUE)
    else:
        queue_path = find_latest_queue(folder)
    if not queue_path or not os.path.isfile(queue_path):
        where = queue_path or f"{folder}{os.sep}{QUEUE_GLOB}"
        raise UsageError(f"paywall_pull: no priority paywall queue found ({where}); "
                         "run build_priority_paywall_queue.py first")
    ledger = (Path(folder) if folder is not None else Path(queue_path).parent) / LEDGER_NAME
    libs = libraries(registry)
    rows = load_queue(queue_path)
    res = {"exit_code": 0, "queue": queue_path, "ledger": str(ledger), "rows": len(rows)}

    if reset_opened:
        save_ledger(ledger, set())
        print("Cleared opened ledger.")
    have = _held(registry, rows)
    if open_n:
        res.update(command="open", **cmd_open(rows, have, open_n, access, host, ledger, libs,
                                              opener=opener, pause=pause))
    elif finish:
        drop = drop_dir or os.path.expanduser("~/Downloads")
        res.update(command="finish", **cmd_finish(rows, have, drop, apply, libs=libs, ledger=ledger,
                                                  have_after_fn=lambda: _held(registry, rows), child=child))
    else:
        res.update(command="status", **cmd_status(rows, have, queue_path, ledger, libs))
    return res


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--queue", default=None,
                    help='queue CSV (default: the latest in projects.json "portfolio_dir")')
    ap.add_argument("--open", type=int, metavar="N", help="open the next N pending in the browser")
    ap.add_argument("--finish", action="store_true",
                    help="ingest downloaded PDFs from the drop dir, backfill .ris + reindex")
    ap.add_argument("--drop-dir", default=os.path.expanduser("~/Downloads"),
                    help="where the browser saved PDFs (default: the Downloads folder in your home "
                         "directory, one workflow; name any folder)")
    ap.add_argument("--apply", action="store_true",
                    help="with --finish: actually move files + reindex (default is a dry run)")
    ap.add_argument("--access", choices=["doi", "ezproxy"], default="doi",
                    help="doi (default, the doi.org link) or ezproxy (through your library's EZproxy)")
    ap.add_argument("--ezproxy-host", default=None,
                    help='your library\'s EZproxy host for --access ezproxy (default: projects.json '
                         '"ezproxy_host"; there is no built-in host)')
    ap.add_argument("--reset-opened", action="store_true", help="clear the opened-not-saved ledger")
    args = ap.parse_args(sys.argv[1:] if argv is None else argv)
    try:
        res = run(queue=args.queue, open_n=args.open, finish=args.finish, drop_dir=args.drop_dir,
                  apply=args.apply, access=args.access, ezproxy_host_arg=args.ezproxy_host,
                  reset_opened=args.reset_opened)
    except UsageError as e:
        print(str(e), file=sys.stderr)
        return 1
    return res["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
