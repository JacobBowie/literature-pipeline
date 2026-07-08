"""Browser paywall pull -- launcher for the manual institutional-access fetch session.

The pipeline fetches everything open-access (Unpaywall -> PMC -> preprint). The residual
~5-15% is genuinely paywalled to anonymous traffic and can only be pulled by hand through
an authenticated library session. `build_priority_paywall_queue.py` ranks those into
`_portfolio/<date>_priority_paywall_queue.csv`. This script drives the pull session over
that queue:

  1. opens the next batch of still-missing DOIs in your default browser (where you are
     already signed into UConn library access),
  2. shows which project lib_dir to save each PDF into (the `projects` column decides it),
  3. after you have dropped the PDFs, wires the existing backfill_ris + index_portfolio
     finish and reports what newly landed.

"Done" is derived from the libraries themselves -- a DOI that is now present in any lib
drops off the queue automatically -- so the session is fully resumable across days with no
manual bookkeeping. The only state kept is a tiny "opened but not yet saved" ledger so
consecutive --open calls advance instead of re-opening the same tabs.

Usage:
  python paywall_pull.py                            # status: how many left, what's next
  python paywall_pull.py --open 8                   # open next 8 in the browser
  python paywall_pull.py --open 8 --access ezproxy --ezproxy-host ezproxy.lib.uconn.edu
  python paywall_pull.py --finish                   # backfill .ris + reindex, report newly pulled
  python paywall_pull.py --reset-opened             # clear the opened-not-saved ledger

Institutional access:
  Default opens https://doi.org/<doi>. That resolves to full text IF your browser has an
  active library session OR a link-resolver extension (Lean Library / EndNote Click). If
  not, pass --access ezproxy --ezproxy-host <your EZproxy host> (or set $UCONN_EZPROXY_HOST)
  to route through the proxy login page. No paywall bypass -- this only opens the front door
  you are already entitled to walk through.
"""
import argparse, csv, glob, json, os, shutil, sys, time, subprocess, webbrowser

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
# Reuse the canonical lib-DOI scan + project->lib_dir map so this stays interop-clean
# with the queue builder (single source of truth for what we already have and where).
from build_priority_paywall_queue import lib_dois, LIBS, ROOT

# Optional helpers reused for the --finish ingest stage (all degrade gracefully):
try:
    from unpaywall_fetch_v2 import is_known_boilerplate   # LWW/Lippincott boilerplate detector
except Exception:
    is_known_boilerplate = None
try:
    import fitz  # pymupdf, for pulling the DOI out of a downloaded PDF
except Exception:
    fitz = None
try:
    from lit_util import extract_doi_from_text
except Exception:
    extract_doi_from_text = None
try:
    from ris_emit import emit_ris_for_pdf   # enh-ii: write .ris from a KNOWN queue DOI on move
except Exception:
    emit_ris_for_pdf = None

PORTFOLIO = os.path.join(ROOT, "_portfolio")
LEDGER = os.path.join(PORTFOLIO, "_paywall_pull_opened.json")
PY = sys.executable


def norm(d):
    """Match the queue builder's DOI normalization so done-detection lines up exactly."""
    return (d or "").strip().lower().rstrip(".").rstrip(")")


def find_latest_queue():
    cands = sorted(glob.glob(os.path.join(PORTFOLIO, "*_priority_paywall_queue.csv")))
    return cands[-1] if cands else None


def load_queue(path):
    rows = []
    with open(path, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            r["doi_raw"] = (r.get("doi") or "").strip()      # for the URL (don't over-strip)
            r["doi_norm"] = norm(r.get("doi"))               # for matching against libs
            try:
                r["rank_int"] = int(r.get("rank") or 0)
            except ValueError:
                r["rank_int"] = 10**9
            rows.append(r)
    return rows


def load_ledger():
    try:
        return set(json.load(open(LEDGER, encoding="utf-8")))
    except (OSError, json.JSONDecodeError):
        return set()


def save_ledger(s):
    with open(LEDGER, "w", encoding="utf-8") as f:
        json.dump(sorted(s), f, indent=0)


def primary_lib(projects_field):
    """First project in the queue's `projects` list that has a known lib_dir."""
    for p in [x.strip() for x in (projects_field or "").split(",") if x.strip()]:
        if p in LIBS:
            return p, os.path.join(ROOT, LIBS[p]).replace("\\", "/")
    return None, None


def prefix_route(stem):
    """Route a file by an explicit '<project>_' filename prefix (manual user convention).
    Matches the longest project key/tail so 'Physiological_Data_*' and 'Genova_Diagnostics_*'
    (which themselves contain underscores) resolve correctly."""
    s = stem.lower()
    best = None
    for key in LIBS:
        for cand in (key.lower(), key.split("/")[-1].lower()):
            if s.startswith(cand + "_") and (best is None or len(cand) > len(best[1])):
                best = (key, cand)
    if best:
        return best[0], os.path.join(ROOT, LIBS[best[0]]).replace("\\", "/")
    return None, None


def build_url(doi_raw, access, ezhost):
    base = f"https://doi.org/{doi_raw}"
    if access == "ezproxy" and ezhost:
        # UConn EZproxy rewrites HOSTNAMES (dots -> dashes) rather than using a
        # login?url= starting point. Prepending the proxied doi.org resolver routes
        # the whole redirect chain through the proxy:
        #   doi.org -> doi-org.<ezhost>/<doi> -> <publisher-with-dashes>.<ezhost>/...
        # Confirmed 2026-06-24 (journals.lww.com -> journals-lww-com.ezproxy.lib.uconn.edu).
        proxied_resolver = "doi-org." + ezhost
        return f"https://{proxied_resolver}/{doi_raw}"
    return base


def cmd_status(rows, have, queue_path):
    done = [r for r in rows if r["doi_norm"] in have]
    pending = [r for r in rows if r["doi_norm"] not in have]
    opened = load_ledger() & {r["doi_norm"] for r in pending}
    print(f"Queue: {os.path.basename(queue_path)}")
    print(f"  total: {len(rows)}   in-library (done): {len(done)}   pending: {len(pending)}")
    print(f"  opened, not yet saved: {len(opened)}")
    nxt = sorted((r for r in pending if r["doi_norm"] not in opened),
                 key=lambda r: r["rank_int"])[:8]
    if nxt:
        print("\nNext up:")
        for r in nxt:
            proj, _ = primary_lib(r.get("projects"))
            print(f"  [{r['rank']:>3}] {(r.get('title') or '')[:68]:<68}  -> {proj or '?'}")
    print("\nNext: python paywall_pull.py --open 8")


def cmd_open(rows, have, n, access, ezhost):
    opened = load_ledger()
    pending = sorted((r for r in rows
                      if r["doi_norm"] not in have and r["doi_norm"] not in opened),
                     key=lambda r: r["rank_int"])
    batch = pending[:n]
    if not batch:
        print("Nothing new to open (all pending are already opened or in a library).")
        print("Run --finish after you have saved them, or --reset-opened to re-open.")
        return
    print(f"Opening {len(batch)} papers in your default browser ({access}).")
    print("Save each PDF into the folder shown, then: python paywall_pull.py --finish\n")
    for r in batch:
        proj, libpath = primary_lib(r.get("projects"))
        url = build_url(r["doi_raw"], access, ezhost)
        others = [x.strip() for x in (r.get("projects") or "").split(",")
                  if x.strip() and x.strip() != proj]
        tag = f"   (also wanted by: {', '.join(others)})" if others else ""
        print(f"[{r['rank']:>3}] {r.get('title') or '(title pending)'} ({r.get('year') or 'n.d.'})")
        print(f"      save to: {libpath or '(unknown lib -- pick manually)'}{tag}")
        print(f"      {url}")
        webbrowser.open(url)
        opened.add(r["doi_norm"])
        time.sleep(1.2)   # let the browser settle between tabs
    save_ledger(opened)
    print(f"\nOpened {len(batch)}. Ledger now tracks {len(opened)} opened-not-saved DOIs.")


def _doi_from_pdf(path):
    """Pull a DOI from a downloaded PDF (first 2 pages) for filing when the filename
    doesn't carry it (e.g. ScienceDirect PII-named files)."""
    if fitz is None:
        return None
    try:
        doc = fitz.open(path)
        text = "".join(doc[i].get_text() for i in range(min(2, doc.page_count)))
        doc.close()
    except Exception:
        return None
    if extract_doi_from_text:
        return extract_doi_from_text(text)
    import re
    m = re.search(r"10\.\d{4,9}/[-._;()/:A-Za-z0-9]+", text or "")
    return m.group(0) if m else None


def plan_ingest(rows, drop_dir):
    """Match PDFs in drop_dir to queue rows. Returns (to_file, boilerplate, unmatched).

    Match order: (1) the publisher filename often IS the DOI suffix (s40279-...-z.pdf),
    (2) else pull the DOI from the PDF text. Each candidate is run through the LWW/Lippincott
    boilerplate detector before filing (the 2026-05-22 trap), so a saved permissions doc is
    rejected to ILL instead of polluting a library."""
    suffix_map = {}
    for r in rows:
        suf = r["doi_norm"].split("/", 1)[-1]
        if suf:
            suffix_map[suf] = r
    by_norm = {r["doi_norm"]: r for r in rows}
    to_file, boiler, unmatched = [], [], []
    for pdf in sorted(glob.glob(os.path.join(drop_dir, "*.pdf"))):
        stem = os.path.splitext(os.path.basename(pdf))[0].lower()
        row = None
        for suf in sorted(suffix_map, key=len, reverse=True):
            if len(suf) >= 6 and suf in stem:
                row = suffix_map[suf]
                break
        if row is None:
            row = by_norm.get(norm(_doi_from_pdf(pdf)))
        if row is None:
            # Fallback: honor an explicit "<project>_" filename prefix (user convention).
            pproj, plib = prefix_route(stem)
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
        proj, lib = primary_lib(row.get("projects"))
        if lib is None:   # no LIBS-known project for this row -> cannot file it; route to manual
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
            dups.append(best[d]); best[d] = item
        else:
            dups.append(item)
    return list(best.values()), boiler, unmatched, dups


def cmd_finish(rows, have_before, drop_dir, apply):
    print(f"Scanning {drop_dir} for downloaded PDFs that match the queue...\n")
    to_file, boiler, unmatched, dups = plan_ingest(rows, drop_dir)
    print(f"  ready to file: {len(to_file)}   duplicate copies: {len(dups)}   "
          f"boilerplate (-> ILL): {len(boiler)}   unmatched: {len(unmatched)}\n")
    for pdf, row, proj, lib in to_file:
        print(f"  FILE  {os.path.basename(pdf)}")
        print(f"          -> {lib}/   [{proj}]  {row.get('doi_norm') or '(by filename prefix)'}")
    for pdf, row, proj, lib in dups:
        print(f"  DUP   {os.path.basename(pdf)}  (same DOI as a kept file, skipped)")
    for pdf, row, tag in boiler:
        print(f"  SKIP  {os.path.basename(pdf)}  (boilerplate: {tag}; {row['doi_norm']} -> ILL)")
    for pdf in unmatched:
        print(f"  ??    {os.path.basename(pdf)}  (no DOI match -- not in this queue, left in place)")

    if not apply:
        print("\nDRY RUN -- nothing moved. Re-run with --apply to file these, then "
              "backfill .ris + reindex.")
        return

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
            doi0 = (row.get("doi_norm") or "").strip()
            if doi0 and emit_ris_for_pdf is not None:
                try: emit_ris_for_pdf(doi0, dest, overwrite=False)
                except Exception: pass
            touched.add(proj)
            continue
        shutil.move(pdf, dest)
        moved += 1
        touched.add(proj)
        # enh-ii (2026-06-25): when the file matched a queue DOI, write its .ris straight from
        # that KNOWN DOI (CrossRef). LWW/paywalled PDFs don't expose their DOI in extractable
        # text, so the generic backfill below NO_DOIs them; this fixes the LWW strand and makes
        # backfill a safety net only (for prefix-routed / no-DOI files).
        doi = (row.get("doi_norm") or "").strip()
        if doi and emit_ris_for_pdf is not None:
            try:
                st, _ = emit_ris_for_pdf(doi, dest, overwrite=False)
                if st == "OK": ris_w += 1
                elif st == "EXISTS_SKIP": ris_skip += 1
                else: ris_fail += 1; print(f"  [ris {st}] {os.path.basename(dest)} ({doi})")
            except Exception as e:
                ris_fail += 1; print(f"  [ris ERR] {os.path.basename(dest)}: {e}")
    print(f"\nMoved {moved} files into {len(touched)} libs "
          f"(.ris from known DOI: {ris_w} written, {ris_skip} existed, {ris_fail} failed).")
    print("Backfilling any remaining .ris (safety net) + reindexing...\n")

    # T3 (2026-06-25): check returncodes. A crashed backfill leaves a project .ris-less and
    # invisible to the index; do NOT clear its DOIs from the ledger, skip its reindex, and tell
    # the operator to re-run backfill (paywall_pull does not auto-retry -- the PDF is already
    # moved out of the drop dir, so the next --finish cannot re-find it).
    for p in sorted(touched):
        lib = os.path.join(ROOT, LIBS[p]).replace("\\", "/")
        rc = subprocess.run([PY, os.path.join(HERE, "backfill_ris.py"), "--lib-dir", lib, "--commit"],
                            encoding="utf-8", errors="replace").returncode
        if rc != 0:
            print(f"  [ERR] backfill_ris failed for {p} (exit {rc}); skipping its reindex. Re-run "
                  f"`backfill_ris.py --lib-dir {lib} --commit` then `index_portfolio.py --project {p}`.")
            failed.add(p)
    for p in sorted(touched):
        if p in failed:
            continue
        rc = subprocess.run([PY, os.path.join(HERE, "index_portfolio.py"), "--project", p],
                            encoding="utf-8", errors="replace").returncode
        if rc != 0:
            print(f"  [ERR] index_portfolio failed for {p} (exit {rc})")
            failed.add(p)

    have_after = lib_dois()
    newly = [r for r in rows if r["doi_norm"] in have_after and r["doi_norm"] not in have_before]
    # T3: clear the ledger only for DOIs whose project did NOT fail this round. A failed project's
    # files are already moved into its lib but may lack a .ris, so leave their DOIs un-cleared
    # (status keeps showing them pending) until backfill is re-run for that lib.
    cleared = {r["doi_norm"] for r in rows
               if r["doi_norm"] in have_after and primary_lib(r.get("projects"))[0] not in failed}
    save_ledger(load_ledger() - cleared)
    print(f"\nNewly in libraries this round: {len(newly)}")
    if failed:
        print(f"FAILED projects (re-run backfill_ris --commit + reindex for these): {', '.join(sorted(failed))}")
    remaining = [r for r in rows if r["doi_norm"] not in have_after]
    print(f"Remaining pending: {len(remaining)} / {len(rows)}")


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--queue", default=None, help="queue CSV (default: latest in _portfolio/)")
    ap.add_argument("--open", type=int, metavar="N", help="open the next N pending in the browser")
    ap.add_argument("--finish", action="store_true",
                    help="ingest downloaded PDFs from the drop dir, backfill .ris + reindex")
    ap.add_argument("--drop-dir", default=os.path.expanduser("~/Downloads"),
                    help="where the browser saved PDFs (default: ~/Downloads)")
    ap.add_argument("--apply", action="store_true",
                    help="with --finish: actually move files + reindex (default is a dry run)")
    ap.add_argument("--access", choices=["doi", "ezproxy"], default="doi",
                    help="doi (default, bare doi.org) or ezproxy (route through proxy login)")
    ap.add_argument("--ezproxy-host", default=os.environ.get("UCONN_EZPROXY_HOST", "ezproxy.lib.uconn.edu"),
                    help="EZproxy host for --access ezproxy (default: ezproxy.lib.uconn.edu; or set $UCONN_EZPROXY_HOST)")
    ap.add_argument("--reset-opened", action="store_true", help="clear the opened-not-saved ledger")
    args = ap.parse_args()

    queue_path = args.queue or find_latest_queue()
    if not queue_path or not os.path.isfile(queue_path):
        sys.exit("No priority paywall queue found in _portfolio/. "
                 "Run build_priority_paywall_queue.py first.")
    rows = load_queue(queue_path)

    if args.reset_opened:
        save_ledger(set())
        print("Cleared opened ledger.")

    if args.access == "ezproxy" and not args.ezproxy_host:
        sys.exit("--access ezproxy needs --ezproxy-host (e.g. ezproxy.lib.uconn.edu) "
                 "or set $UCONN_EZPROXY_HOST.")

    have = lib_dois()
    if args.open:
        cmd_open(rows, have, args.open, args.access, args.ezproxy_host)
    elif args.finish:
        cmd_finish(rows, have, args.drop_dir, args.apply)
    else:
        cmd_status(rows, have, queue_path)


if __name__ == "__main__":
    main()
