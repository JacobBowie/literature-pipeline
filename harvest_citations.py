"""Harvest citation files from a directory (default: ~/Downloads) into a
canonical RIS-only library at Projects/_references/citations/.

Per-file flow:
  1. Parse .ris / .enw / .nbib → extract DOI (or PMID for nbib: PMID → DOI through E-utilities
     esummary; a failed lookup is counted, never read as "no DOI") + fallback metadata
  2. CrossRef lookup:
       - DOI present     → /works/{doi}
       - DOI absent      → /works?query.title=...&query.author=... (Scholar files)
  3. If no confident CrossRef match → use the source file's own metadata
  4. Build canonical .ris and write to <out-dir>/<year>_<Lastname>_<Slug>.ris
  5. Dedupe by DOI (case-insensitive); first wins, dupes logged. A distinct paper whose canonical
     stem is already taken (this run or on disk) gets `<stem>_<6-hex hash>.ris`, never a skip

Source files in --source-dir are NEVER moved or deleted. After verifying the
inbox, the user can manually clear Downloads.

Usage:
  # Default dry-run (reports what would happen, writes nothing)
  python harvest_citations.py

  # Commit (actually write the .ris files + index.csv)
  python harvest_citations.py --commit

  # Custom source / output
  python harvest_citations.py --source-dir ~/somewhere --out-dir /tmp/citations --commit

  # Limit to first N files for testing
  python harvest_citations.py --limit 5
"""
import os, sys, re, csv, time, argparse, hashlib
from pathlib import Path

import lit_util
lit_util.utf8_stdout()

# Local module
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ris_emit as R
from litpipe import net
from litpipe.outcomes import Kind, Outcome

# PMID -> DOI through E-utilities esummary (db=pubmed): the DOI is the `articleids` entry with
# idtype "doi" (V1 P6: all 5 PubMed-only PMIDs carried it; re-probed 2026-09-30). idconv, used before,
# "will only return related IDs if the article is in PubMed Central" (N-B6), and its host refuses this
# pipeline since 2026-09-30. litpipe.net adds the NCBI tool/email identity and paces the host.
ESUMMARY = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"

DEFAULT_SOURCE = os.path.expanduser("~/Downloads")
DEFAULT_OUT    = str(lit_util.PROJECTS_ROOT / "_references" / "citations")

EXTS = {".ris", ".enw", ".nbib"}


# ---------- per-format parsers ----------

# parse_ris was promoted to lit_util (Stage 3 c10); re-exported so detect_and_parse's `parse_ris(path)`
# keeps working. The union returns `authors_raw` (alias of authors) so this module's consumer
# (parsed["authors_raw"]) is unchanged, and canonical_stem tolerates the now-int year. parse_enw /
# parse_nbib below stay bespoke (their own tag grammars) and still return the authors_raw shape.
parse_ris = lit_util.parse_ris


def parse_enw(path):
    """EndNote tagged text: %T %A %D %R %U etc."""
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            text = f.read()
    except OSError:
        return {}
    fields = {}
    for line in text.splitlines():
        m = re.match(r"^%([A-Z0-9])\s+(.*)$", line)
        if not m: continue
        tag, val = m.group(1), m.group(2).strip()
        fields.setdefault(tag, []).append(val)
    doi = (fields.get("R", [""])[0] or "").lower()
    if not doi:
        for url in fields.get("U", []):
            d = R.extract_doi_from_text(url)
            if d: doi = d; break
    title = (fields.get("T", []) + [""])[0]
    year  = (fields.get("D", []) + [""])[0][:4]
    authors_raw = fields.get("A", [])
    lastname = ""
    if authors_raw:
        first = (authors_raw[0] or "").strip()
        if first:
            if "," in first:
                lastname = first.split(",")[0].strip()
            else:
                parts = first.split()
                lastname = parts[0].strip() if parts else ""
    return {"doi": doi, "title": title, "year": year, "lastname": lastname,
            "authors_raw": authors_raw}


def parse_nbib(path):
    """PubMed nbib format. The PMID feeds the esummary PMID -> DOI fallback (pmid_to_doi)."""
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            text = f.read()
    except OSError:
        return {}
    fields = {}
    cur_tag = None
    for line in text.splitlines():
        m = re.match(r"^([A-Z]{2,4})\s*-\s?(.*)$", line)
        if m:
            cur_tag = m.group(1)
            fields.setdefault(cur_tag, []).append(m.group(2).rstrip())
        elif line.startswith("      ") and cur_tag:
            # continuation of previous field
            fields[cur_tag][-1] += " " + line.strip()
    # DOI is in LID line ending with [doi], or AID line
    doi = ""
    for src in ("LID", "AID"):
        for v in fields.get(src, []):
            if v.lower().endswith("[doi]"):
                d = v.split("[doi]")[0].strip()
                if d.startswith("10."):
                    doi = d.lower(); break
        if doi: break
    pmid = (fields.get("PMID", [""])[0] or "").strip()
    title = (fields.get("TI", [""])[0] or "").strip()
    year  = (fields.get("DP", [""])[0] or "")[:4]
    authors_raw = fields.get("FAU", []) or fields.get("AU", [])
    lastname = ""
    if authors_raw:
        first = (authors_raw[0] or "").strip()
        if first:
            if "," in first:
                lastname = first.split(",")[0].strip()
            else:
                parts = first.split()
                lastname = parts[0].strip() if parts else ""
    return {"doi": doi, "pmid": pmid, "title": title, "year": year,
            "lastname": lastname, "authors_raw": authors_raw}


def _doi_from_summary(doc):
    """The DOI of one esummary document: its `articleids` doi, else a `doi:` elocationid."""
    for a in doc.get("articleids") or []:
        if (a.get("idtype") or "").lower() == "doi" and (a.get("value") or "").strip():
            return a["value"].strip()
    m = re.search(r"doi:\s*(10\.\S+)", doc.get("elocationid") or "", re.IGNORECASE)
    return m.group(1) if m else ""


def pmid_to_doi(pmid: str) -> Outcome:
    """PMID -> DOI via E-utilities esummary (db=pubmed). OK carries the lower-cased DOI as payload;
    NO_MATCH means PubMed answered and has no DOI for the PMID (or no such PMID). Any other kind is
    a failed lookup the caller counts (TRANSPORT, REFUSED, OUTAGE, DEFERRED, ERROR): a failure is
    never an empty DOI (REG-I46)."""
    pmid = (pmid or "").strip()
    if not pmid:
        return Outcome(Kind.SKIPPED, detail="no pmid")
    if not pmid.isdigit():
        return Outcome(Kind.NO_MATCH, detail=f"not a PMID: {pmid!r}")
    o = net.get(ESUMMARY, params={"db": "pubmed", "id": pmid, "retmode": "json"},
                timeout=(10, 15), purpose="pmid_to_doi", validate=net.expect_json)
    if not o.ok:
        return Outcome(o.kind, status=o.status, host=o.host, detail=o.detail, attempts=o.attempts,
                       elapsed_ms=o.elapsed_ms, retry_after=o.retry_after)
    base = dict(status=o.status, host=o.host, attempts=o.attempts, elapsed_ms=o.elapsed_ms)
    try:
        body = o.payload.json()
    except ValueError as e:
        return Outcome(Kind.ERROR, detail=f"esummary body is not JSON: {e}", **base)
    res = body.get("result") if isinstance(body, dict) else None
    if not isinstance(res, dict):
        err = body.get("error") if isinstance(body, dict) else type(body).__name__
        return Outcome(Kind.ERROR, detail=f"esummary body without a result: {err}", **base)
    doc = res.get(pmid)
    if not isinstance(doc, dict) or not doc or doc.get("error"):
        why = doc.get("error") if isinstance(doc, dict) and doc.get("error") else "no document"
        return Outcome(Kind.NO_MATCH, detail=f"PubMed: {why}", **base)
    doi = _doi_from_summary(doc)
    if not doi:
        return Outcome(Kind.NO_MATCH, detail="PubMed record carries no DOI", **base)
    return Outcome(Kind.OK, payload=doi.lower(), **base)


def parse_any(path):
    ext = Path(path).suffix.lower()
    if ext == ".ris":  return ("ris",  parse_ris(path))
    if ext == ".enw":  return ("enw",  parse_enw(path))
    if ext == ".nbib": return ("nbib", parse_nbib(path))
    return (ext.lstrip("."), {})


# ---------- main harvest ----------

def fallback_meta_from_file(parsed: dict) -> dict:
    """If CrossRef lookup fails, build a meta dict from the source file's own fields."""
    authors = []
    for a in parsed.get("authors_raw", []):
        if "," in a:
            fam, giv = a.split(",", 1)
            authors.append({"family": fam.strip(), "given": giv.strip()})
        else:
            parts = a.split()
            if len(parts) >= 2:
                authors.append({"family": parts[-1], "given": " ".join(parts[:-1])})
            elif parts:
                authors.append({"family": parts[0], "given": ""})
    return {
        "doi":      parsed.get("doi", ""),
        "title":    parsed.get("title", ""),
        "year":     parsed.get("year", ""),
        "date":     parsed.get("year", ""),
        "lastname": parsed.get("lastname", ""),
        "authors":  authors,
        "container": "", "volume": "", "issue": "", "page": "",
        "issn": "", "abstract": "",
        "url":      f"https://doi.org/{parsed['doi']}" if parsed.get("doi") else "",
        "type":     "journal-article",
    }


def _crossref(fn, args, stats, what):
    """A ris_emit Crossref lookup whose typed failure (MetadataUnavailable, REG-I46) is counted and
    reported instead of ending the harvest; the row then falls back to the file's own metadata.
    The class is looked up at call time, so a reloaded ris_emit (tests reload it) is still caught."""
    try:
        return fn(*args)
    except R.MetadataUnavailable as e:
        stats["metadata_lookup_failed"] += 1
        print(f"  [crossref] {what}: lookup failed: {e}", file=sys.stderr)
        return None


# ---------- output names: one file per paper ----------
# canonical_stem is year + first-author surname + the first six title words, so two distinct papers
# can share it (a paper and its follow-up by the same author in one year). The second used to be
# EXISTS_SKIP (or, with --overwrite, written over the first). A stem already taken by another paper,
# in this run or on disk, gets a short hash of the paper's DOI (else its title).

def _identity(meta):
    return ((meta.get("doi") or "").strip().lower(), R.normalize_title(meta.get("title") or ""))


def _same_paper(a, b):
    """DOIs decide when both have one; otherwise the full normalised titles (both blank: same)."""
    if a[0] and b[0]:
        return a[0] == b[0]
    return a[1] == b[1]


def _ris_identity(path):
    try:
        m = lit_util.parse_ris(str(path))
    except OSError:
        return None
    return ((m.get("doi") or "").strip().lower(), R.normalize_title(m.get("title") or ""))


def _choose_name(stem, ident, emitted, outdir):
    name = f"{stem}.ris"
    holder = emitted.get(name.casefold())
    if holder is None and (Path(outdir) / name).exists():
        holder = _ris_identity(Path(outdir) / name)     # written by an earlier run
    if holder is None or _same_paper(holder, ident):
        return name
    tag = hashlib.sha1((ident[0] or ident[1]).encode("utf-8")).hexdigest()[:6]
    return f"{stem}_{tag}.ris"


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    ap.add_argument("--source-dir", default=DEFAULT_SOURCE,
                    help=f"Directory of .ris/.enw/.nbib files to harvest (default: {DEFAULT_SOURCE}).")
    ap.add_argument("--out-dir",    default=DEFAULT_OUT,
                    help=f"Canonical RIS output directory (default: {DEFAULT_OUT}).")
    ap.add_argument("--commit",     action="store_true",
                    help="Actually write files. Default is dry-run.")
    ap.add_argument("--limit",      type=int, default=0,
                    help="Limit to first N files (for testing).")
    ap.add_argument("--sleep",      type=float, default=0.6,
                    help="Seconds between CrossRef calls (politeness).")
    ap.add_argument("--no-search",  action="store_true",
                    help="Skip title-based CrossRef search for files w/o DOI.")
    ap.add_argument("--overwrite",  action="store_true",
                    help="Overwrite existing .ris files in out-dir.")
    args = ap.parse_args()

    source = Path(args.source_dir)
    outdir = Path(args.out_dir)
    if not source.exists():
        print(f"[ERR] source not found: {source}", file=sys.stderr); sys.exit(2)
    if args.commit:
        outdir.mkdir(parents=True, exist_ok=True)

    files = sorted([p for p in source.iterdir()
                     if p.is_file() and p.suffix.lower() in EXTS])
    if args.limit: files = files[:args.limit]

    mode = "COMMIT" if args.commit else "DRY-RUN"
    print(f"== citation harvest [{mode}] ==")
    print(f"  source:   {source}")
    print(f"  out-dir:  {outdir}")
    print(f"  found:    {len(files)} files (.ris/.enw/.nbib)")
    print()

    rows = []
    seen_doi = {}        # doi -> first canonical filename
    emitted = {}         # out_name.casefold() -> (doi, title) of the paper this run gave it
    stats = {"crossref_doi": 0, "crossref_search": 0, "fallback": 0,
             "no_metadata": 0, "dup_skip": 0, "wrote": 0, "pmid_lookup_failed": 0,
             "metadata_lookup_failed": 0, "stem_collision": 0, "kept_curated": 0}

    for i, p in enumerate(files, 1):
        fmt, parsed = parse_any(p)
        if not parsed:
            stats["no_metadata"] += 1
            rows.append({"src": p.name, "fmt": fmt, "doi": "", "status": "PARSE_FAIL",
                         "out": "", "title": "", "year": ""})
            print(f"  [{i}/{len(files)}] {p.name:<40} → PARSE_FAIL")
            continue

        doi = parsed.get("doi", "")
        # PMID → DOI fallback for nbib files w/o LID-doi
        if not doi and fmt == "nbib" and parsed.get("pmid"):
            res = pmid_to_doi(parsed["pmid"])
            if res.ok:
                doi = parsed["doi"] = res.payload
            elif res.kind is not Kind.NO_MATCH:
                # A failed lookup is not "no DOI": count it and say so (REG-I22).
                stats["pmid_lookup_failed"] += 1
                print(f"  [pmid_to_doi] pmid {parsed['pmid']}: {res.kind} "
                      f"{res.status or ''} {res.detail}".rstrip(), file=sys.stderr)
            time.sleep(args.sleep)

        meta = None; source_kind = ""
        if doi:
            msg = _crossref(R.crossref_by_doi, (doi,), stats, f"doi {doi}")
            if msg:
                meta = R.crossref_meta(msg); source_kind = "crossref-doi"
                stats["crossref_doi"] += 1
            time.sleep(args.sleep)

        if not meta and not args.no_search and parsed.get("title"):
            msg = _crossref(R.crossref_by_title,
                            (parsed["title"], parsed.get("lastname",""), parsed.get("year","")),
                            stats, f"title of {p.name}")
            if msg:
                meta = R.crossref_meta(msg); source_kind = "crossref-search"
                stats["crossref_search"] += 1
            time.sleep(args.sleep)

        if not meta:
            meta = fallback_meta_from_file(parsed); source_kind = f"fallback-{fmt}"
            stats["fallback"] += 1

        # Need at least *some* identifying info
        if not meta.get("title") and not meta.get("lastname"):
            stats["no_metadata"] += 1
            rows.append({"src": p.name, "fmt": fmt, "doi": doi, "status": "NO_METADATA",
                         "out": "", "title": parsed.get("title",""),
                         "year": parsed.get("year","")})
            print(f"  [{i}/{len(files)}] {p.name:<40} → NO_METADATA")
            continue

        # Dedup
        d_key = (meta.get("doi") or "").lower()
        if d_key and d_key in seen_doi:
            stats["dup_skip"] += 1
            rows.append({"src": p.name, "fmt": fmt, "doi": d_key, "status": "DUP_SKIP",
                         "out": seen_doi[d_key], "title": meta.get("title",""),
                         "year": meta.get("year","")})
            print(f"  [{i}/{len(files)}] {p.name:<40} → DUP_SKIP (doi seen: {seen_doi[d_key]})")
            continue

        stem = R.canonical_stem(meta.get("year"), meta.get("lastname"), meta.get("title"))
        ident = _identity(meta)
        out_name = _choose_name(stem, ident, emitted, outdir)
        if out_name != f"{stem}.ris":
            stats["stem_collision"] += 1
        emitted[out_name.casefold()] = ident
        out_path = outdir / out_name
        if d_key: seen_doi[d_key] = out_name

        ris_text = R.build_ris(meta)
        wrote = False
        if args.commit:
            # Don't overwrite by default
            if out_path.exists() and not args.overwrite:
                status = "EXISTS_SKIP"
            elif R.write_ris(str(out_path), ris_text, overwrite=True):
                wrote = True; stats["wrote"] += 1
                status = "WROTE"
            else:
                # DEC-29: an edited or unrecorded file is kept as curated (ris_emit says why on stderr)
                stats["kept_curated"] += 1
                status = "KEPT_CURATED"
        else:
            status = f"DRY:{source_kind}"

        rows.append({"src": p.name, "fmt": fmt, "doi": d_key, "status": status,
                     "out": out_name, "title": meta.get("title","")[:80],
                     "year": meta.get("year","")})
        marker = "WROTE" if wrote else status
        print(f"  [{i}/{len(files)}] {p.name:<40} → {marker:<14} {out_name}")

    # Index CSV
    idx_path = outdir / "_index.csv"
    if args.commit:
        with open(idx_path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["src","fmt","doi","status","out","year","title"])
            w.writeheader(); w.writerows(rows)

    print()
    print("== summary ==")
    for k, v in stats.items(): print(f"  {k:<18} {v}")
    print(f"  total inputs       {len(files)}")
    print(f"  unique DOIs        {len(seen_doi)}")
    if args.commit:
        print(f"  index CSV          {idx_path}")
    else:
        print("  (dry-run; pass --commit to actually write)")


if __name__ == "__main__":
    main()
