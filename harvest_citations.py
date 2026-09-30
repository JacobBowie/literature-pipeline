"""Harvest citation files from a directory (default: ~/Downloads) into a
canonical RIS-only library at Projects/_references/citations/.

Per-file flow:
  1. Parse .ris / .enw / .nbib → extract DOI (or PMID for nbib) + fallback metadata
  2. CrossRef lookup:
       - DOI present     → /works/{doi}
       - DOI absent      → /works?query.title=...&query.author=... (Scholar files)
  3. If no confident CrossRef match → use the source file's own metadata
  4. Build canonical .ris and write to <out-dir>/<year>_<Lastname>_<Slug>.ris
  5. Dedupe by DOI (case-insensitive); first wins, dupes logged

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
import os, sys, re, csv, time, argparse
from pathlib import Path

import lit_util
lit_util.utf8_stdout()

# Local module
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ris_emit as R
import requests

EMAIL  = os.environ.get("LITPIPE_EMAIL", lit_util.DEFAULT_EMAIL)
UA     = f"GETPAID-harvest/1.0 (mailto:{EMAIL})"
# 2026-09-01: this module carried its own copy of the retired idconv URL and its own
# requests call, so it missed the c8 consolidation and kept hitting a dead endpoint.
# Reuse lit_net's constant + stdlib-urllib transport (that host 403s requests). The
# direction here is pmid -> doi, so doi_to_pmcid_batch does not apply; the transport does.
import lit_net
from litpipe.outcomes import Kind, Outcome, from_legacy
IDCONV = lit_net.IDCONV

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
    """PubMed nbib format. PMID needed for ID-converter fallback."""
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


def pmid_to_doi(pmid: str) -> Outcome:
    """PMID -> DOI via idconv. OK carries the lower-cased DOI as payload; NO_MATCH means idconv
    answered and has no DOI for it. Any other kind is a failed lookup the caller counts: it used
    to be swallowed into "" and look exactly like "no DOI" (REG-I22)."""
    if not pmid:
        return Outcome(Kind.SKIPPED, detail="no pmid")
    params = {"tool": "GETPAID", "email": EMAIL, "ids": pmid,
              "idtype": "pmid", "format": "json"}
    r = lit_net._idconv_get(IDCONV, params, UA, timeout=15)
    if r.status_code == 0:
        return Outcome(Kind.TRANSPORT, host="pmc.ncbi.nlm.nih.gov", detail=r.error, attempts=1)
    if r.status_code != 200:
        return Outcome(from_legacy(f"HTTP_{r.status_code}"), status=r.status_code,
                       host="pmc.ncbi.nlm.nih.gov", attempts=1)
    try:
        records = r.json().get("records") or []
    except (ValueError, AttributeError) as e:
        return Outcome(Kind.ERROR, status=200, host="pmc.ncbi.nlm.nih.gov",
                       detail=f"non-JSON idconv body: {e}", attempts=1)
    for rec in records:
        if rec.get("doi"):
            return Outcome(Kind.OK, status=200, attempts=1, payload=rec["doi"].lower())
    return Outcome(Kind.NO_MATCH, status=200, attempts=1)


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
    stats = {"crossref_doi": 0, "crossref_search": 0, "fallback": 0,
             "no_metadata": 0, "dup_skip": 0, "wrote": 0, "pmid_lookup_failed": 0}

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
            msg = R.crossref_by_doi(doi)
            if msg:
                meta = R.crossref_meta(msg); source_kind = "crossref-doi"
                stats["crossref_doi"] += 1
            time.sleep(args.sleep)

        if not meta and not args.no_search and parsed.get("title"):
            msg = R.crossref_by_title(parsed["title"], parsed.get("lastname",""),
                                       parsed.get("year",""))
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

        stem = R.canonical_stem(meta.get("year"), meta.get("lastname"), meta.get("title"))
        out_name = f"{stem}.ris"
        out_path = outdir / out_name

        # Dedup
        d_key = (meta.get("doi") or "").lower()
        if d_key and d_key in seen_doi:
            stats["dup_skip"] += 1
            rows.append({"src": p.name, "fmt": fmt, "doi": d_key, "status": "DUP_SKIP",
                         "out": seen_doi[d_key], "title": meta.get("title",""),
                         "year": meta.get("year","")})
            print(f"  [{i}/{len(files)}] {p.name:<40} → DUP_SKIP (doi seen: {seen_doi[d_key]})")
            continue
        if d_key: seen_doi[d_key] = out_name

        ris_text = R.build_ris(meta)
        wrote = False
        if args.commit:
            # Don't overwrite by default
            if out_path.exists() and not args.overwrite:
                status = "EXISTS_SKIP"
            else:
                R.write_ris(str(out_path), ris_text, overwrite=True)
                wrote = True; stats["wrote"] += 1
                status = "WROTE"
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
