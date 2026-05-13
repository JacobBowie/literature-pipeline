"""Smarter Unpaywall fetcher.

Improvements over v1:
  1. Iterates ALL oa_locations with url_for_pdf, not just best_oa_location.
  2. Tries repository hosts BEFORE publisher hosts (publishers more likely to 403).
  3. Browser-like User-Agent for downloads (publisher PDFs often block scientific clients).
  4. HTML-response fallback: extracts <embed>/<iframe>/<a href*=.pdf> from landing pages.
  5. Retries each candidate location until one succeeds.

Usage:
  python tools/unpaywall_fetch_v2.py [--top-n 100] [--dry-run]
"""
import os, sys, io, time, csv, re, argparse
import requests
try:
    if getattr(sys.stdout, "encoding", "").lower() != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, OSError):
    pass

# Local module
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ris_emit as _R

EMAIL      = os.environ.get("LITPIPE_EMAIL", "jacob.bowie2@gmail.com")
UNPAYWALL  = "https://api.unpaywall.org/v2"

# Browser-ish UA for *download* GETs (publishers block "GETPAID-bot")
BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
API_UA     = f"GETPAID-lib-builder/1.0 (mailto:{EMAIL})"

DEFAULT_TRIAGE = "data/prior_art/discovered/triage_not_in_library.csv"
DEFAULT_LIB    = "references/literature"
DEFAULT_REPORT = "data/prior_art/discovered/unpaywall_fetch_report_v2.csv"

# ---------- filename synthesis (same as v1) ----------

def slug_title(title, max_words=6):
    from ris_emit import safe_ascii
    t = re.sub(r"<[^>]+>", "", title)
    t = safe_ascii(t)
    t = re.sub(r"[^\w\s\-]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    skip = {"a","an","the","of","in","on","and","to","for","at","from","with","by","as"}
    words = [w for w in t.split() if w.lower() not in skip]
    slug = "".join(w.capitalize() for w in words[:max_words])
    return slug or "Untitled"

def last_name(authors_str):
    """Extract the first author's family name.

    Handles common formats:
      - "J Smith; K Jones"             → "Smith"   (Initial + LastName)
      - "Smith J; Jones K"             → "Smith"   (LastName + Initial)
      - "Cramer MN, Jay O"             → "Cramer"  (comma-separated authors)
      - "Smith, John; Doe, Jane"       → "Smith"   ("Last, First" via ;)
      - "Hoffman GE; Roussos P"        → "Hoffman" (LastName + multi-letter Initials)
      - "Malchaire J, Piette A, et al" → "Malchaire" (et al stripped)
      - "T. Gabbett"                   → "Gabbett"
    """
    if not authors_str: return "Unknown"
    from ris_emit import safe_ascii
    # NFKD-normalize first so the initials-detection regex (ASCII-only) matches
    # uppercase letters that came from non-ASCII chars (Ø → O, Å → A, Ñ → N).
    s = safe_ascii(authors_str.strip())
    # Strip trailing "et al" / "et al."
    s = re.sub(r",?\s*et\s+al\.?\s*$", "", s, flags=re.IGNORECASE).strip()
    # Take first author: split on ; first (multi-author separator), then on ,
    first = s.split(";")[0].strip()
    if "," in first: first = first.split(",")[0].strip()
    parts = first.split()
    if not parts: return "Unknown"
    cand = parts[-1]
    # If trailing token looks like initials (e.g. "P", "GE", "J.M."), the
    # author name is in "LastName Initial" order — use the first token.
    if len(parts) > 1 and re.fullmatch(r"[A-Z]{1,3}\.?", cand):
        cand = parts[0]
    return re.sub(r"[^A-Za-z0-9\-]", "", cand) or "Unknown"

def build_filename(year, authors, title):
    yr = year if year and re.match(r'^\d{4}$', str(year)) else "Unknown"
    return f"{yr}_{last_name(authors)}_{slug_title(title)}.pdf"

# ---------- Unpaywall query ----------

def unpaywall_lookup(doi, timeout=15):
    try:
        r = requests.get(f"{UNPAYWALL}/{doi}",
                         params={"email": EMAIL},
                         headers={"User-Agent": API_UA},
                         timeout=timeout)
        if r.status_code == 200:
            return r.json()
        return {"error": f"HTTP {r.status_code}"}
    except Exception as e:
        return {"error": str(e)}

def candidate_urls(upw):
    """Return ordered list of (host_type, version, url) candidates.

    Order: repositories first (less likely to 403), then publisher.
    Within each tier: locations with url_for_pdf first, then url/landing_page.
    """
    out = []
    locs = upw.get("oa_locations") or []
    # Bucket by host_type
    repos = [l for l in locs if l.get("host_type") == "repository"]
    pubs  = [l for l in locs if l.get("host_type") == "publisher"]
    other = [l for l in locs if l.get("host_type") not in ("repository","publisher")]

    for tier in (repos, pubs, other):
        # Within tier, prefer publishedVersion > acceptedVersion > submittedVersion
        ver_rank = {"publishedVersion":0, "acceptedVersion":1, "submittedVersion":2}
        tier_sorted = sorted(tier, key=lambda l: ver_rank.get(l.get("version") or "submittedVersion", 3))
        for l in tier_sorted:
            for url_field in ("url_for_pdf", "url"):
                u = l.get(url_field)
                if u:
                    out.append((l.get("host_type"), l.get("version"), u, url_field))
    # De-dupe URLs
    seen = set(); uniq = []
    for tup in out:
        if tup[2] not in seen:
            seen.add(tup[2]); uniq.append(tup)
    return uniq

# ---------- download with browser UA + HTML fallback ----------

def looks_like_pdf(blob):
    return blob[:4] == b"%PDF"

def extract_pdf_links_from_html(html_bytes, base_url):
    """Find PDF URLs embedded in an HTML landing page (citation_pdf_url meta,
    embed/iframe src, or a-tag href with .pdf)."""
    try:
        html = html_bytes.decode("utf-8", errors="replace")
    except Exception:
        return []
    candidates = []

    # Highly reliable: <meta name="citation_pdf_url" content="...">
    for m in re.finditer(r'<meta[^>]+name=["\']citation_pdf_url["\'][^>]+content=["\']([^"\']+)["\']',
                          html, re.IGNORECASE):
        candidates.append(m.group(1))
    # <embed src=...> with PDF mime
    for m in re.finditer(r'<embed[^>]+src=["\']([^"\']+\.pdf[^"\']*)["\']', html, re.IGNORECASE):
        candidates.append(m.group(1))
    for m in re.finditer(r'<iframe[^>]+src=["\']([^"\']+\.pdf[^"\']*)["\']', html, re.IGNORECASE):
        candidates.append(m.group(1))
    # <a href=...pdf>
    for m in re.finditer(r'<a[^>]+href=["\']([^"\']+\.pdf[^"\']*)["\']', html, re.IGNORECASE):
        candidates.append(m.group(1))

    # Resolve relative URLs
    from urllib.parse import urljoin
    return [urljoin(base_url, c) for c in candidates]

def try_download(url, dest, timeout=30):
    """Returns (status, msg). status in {OK, HTML, HTTP_xxx, ERROR, TOO_SMALL}."""
    try:
        r = requests.get(url, headers={"User-Agent": BROWSER_UA, "Accept": "application/pdf,*/*"},
                          timeout=timeout, stream=True, allow_redirects=True)
        if r.status_code != 200:
            return f"HTTP_{r.status_code}", ""
        first_chunk = b""
        chunks = []
        total = 0
        for chunk in r.iter_content(chunk_size=8192):
            if chunk:
                if not first_chunk:
                    first_chunk = chunk
                chunks.append(chunk)
                total += len(chunk)
                if total > 50_000_000:  # 50MB cap
                    break
        if not first_chunk:
            return "EMPTY", ""
        if looks_like_pdf(first_chunk):
            with open(dest, "wb") as f:
                for c in chunks: f.write(c)
            size = os.path.getsize(dest)
            if size < 10_000:
                os.remove(dest)
                return "TOO_SMALL", f"{size}B"
            return "OK", f"{size}"
        # HTML fallback
        return "HTML", b"".join(chunks)
    except Exception as e:
        return "ERROR", str(e)

def download_with_fallback(candidates, dest, timeout=30):
    """Try each candidate URL. If we get HTML, try to extract PDF link and follow once."""
    attempts = []
    for host, version, url, field in candidates:
        status, msg = try_download(url, dest, timeout)
        attempts.append((host, version, url, status, str(msg)[:100] if isinstance(msg, str) else f"<{len(msg)}B HTML>"))
        if status == "OK":
            return True, attempts
        if status == "HTML":
            # msg is HTML bytes; extract PDF links
            pdf_links = extract_pdf_links_from_html(msg, url)
            for pl in pdf_links[:3]:
                s2, m2 = try_download(pl, dest, timeout)
                attempts.append(("html-fallback", version, pl, s2,
                                  str(m2)[:100] if isinstance(m2, str) else f"<{len(m2)}B HTML>"))
                if s2 == "OK":
                    return True, attempts
        time.sleep(0.5)
    return False, attempts

# ---------- main ----------

def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    ap.add_argument("--top-n", type=int, default=100,
                     help="Maximum number of triage rows to attempt (default: 100).")
    ap.add_argument("--dry-run", action="store_true",
                     help="Look up Unpaywall metadata but do not download PDFs.")
    ap.add_argument("--min-cites", type=int, default=0,
                     help="Skip rows with citation_count below this threshold.")
    ap.add_argument("--base-dir", default=os.getcwd(),
                     help="Project root. Default: CWD.")
    ap.add_argument("--triage", default=None,
                     help=f"Triage CSV path (default: <base-dir>/{DEFAULT_TRIAGE})")
    ap.add_argument("--lib-dir", default=None,
                     help=f"PDF destination (default: <base-dir>/{DEFAULT_LIB})")
    ap.add_argument("--report", default=None,
                     help=f"Output report CSV (default: <base-dir>/{DEFAULT_REPORT})")
    ap.add_argument("--no-write-ris", action="store_true",
                     help="Skip writing .ris sidecar next to each successfully fetched PDF.")
    args = ap.parse_args()

    base = os.path.abspath(args.base_dir)
    triage_csv = args.triage or os.path.join(base, DEFAULT_TRIAGE)
    lib_dir    = args.lib_dir or os.path.join(base, DEFAULT_LIB)
    out_report = args.report  or os.path.join(base, DEFAULT_REPORT)

    if not os.path.exists(triage_csv):
        print(f"ERR: triage CSV not found: {triage_csv}", file=sys.stderr)
        sys.exit(1)

    with open(triage_csv, encoding="utf-8") as f:
        triage = list(csv.DictReader(f))
    triage_top = [r for r in triage if int(r.get("citation_count") or 0) >= args.min_cites][:args.top_n]
    print(f"Project: {base}")
    print(f"Triage:  {triage_csv}")
    print(f"Library: {lib_dir}")
    print(f"Report:  {out_report}")
    print(f"Attempting top {len(triage_top)} (min_cites={args.min_cites}){' [DRY RUN]' if args.dry_run else ''}\n")

    os.makedirs(lib_dir, exist_ok=True)
    report_dir = os.path.dirname(out_report)
    if report_dir: os.makedirs(report_dir, exist_ok=True)
    existing = set(os.listdir(lib_dir))
    results = []
    n_oa = n_dl = n_skip = n_no_oa = n_oa_no_url = n_fail = 0

    for i, row in enumerate(triage_top, 1):
        doi = (row.get("doi") or "").strip().lower()
        if not doi: continue
        title = row.get("title", ""); year = row.get("year", "")
        authors = row.get("authors", ""); cites = row.get("citation_count", 0)
        fn = build_filename(year, authors, title)
        dest = os.path.join(lib_dir, fn)

        out = {"rank": i, "doi": doi, "year": year, "cites": cites,
               "filename": fn, "title": title[:120],
               "oa_status": "", "n_locations": 0, "downloaded": False,
               "winning_host": "", "winning_url": "",
               "attempts": "", "error": ""}

        if fn in existing:
            n_skip += 1
            out["oa_status"] = "SKIP_EXISTS"
            results.append(out)
            print(f"  [{i:>3}] {fn[:75]:<75} SKIP")
            continue

        upw = unpaywall_lookup(doi)
        time.sleep(1.0)

        if "error" in upw:
            n_fail += 1
            out["error"] = upw["error"]
            results.append(out)
            print(f"  [{i:>3}] {doi[:55]:<55} API ERR: {upw['error']}")
            continue

        is_oa = upw.get("is_oa", False)
        cands = candidate_urls(upw) if is_oa else []
        out["oa_status"] = "OA" if is_oa else "CLOSED"
        out["n_locations"] = len(cands)

        if not is_oa:
            n_no_oa += 1
            results.append(out)
            print(f"  [{i:>3}] {doi[:55]:<55} CLOSED")
            continue
        n_oa += 1
        if not cands:
            n_oa_no_url += 1
            results.append(out)
            print(f"  [{i:>3}] {doi[:55]:<55} OA-no-URL")
            continue

        if args.dry_run:
            out["winning_url"] = cands[0][2]
            results.append(out)
            print(f"  [{i:>3}] {fn[:60]:<60} OA: {len(cands)} cand, [0]={cands[0][2][:60]}")
            continue

        ok, attempts = download_with_fallback(cands, dest)
        out["attempts"] = " | ".join(f"{h}/{v}/{s}" for h,v,_,s,_ in attempts)
        if ok:
            n_dl += 1
            out["downloaded"] = True
            winning = next((a for a in attempts if a[3] == "OK"), None)
            if winning:
                out["winning_host"] = winning[0]; out["winning_url"] = winning[2]
            print(f"  [{i:>3}] {fn[:65]:<65} DL ({out['winning_host']}, {len(attempts)} tries)")
            if not args.no_write_ris:
                ris_status, _ = _R.emit_ris_for_pdf(doi, dest)
                print(f"        ris: {ris_status}")
        else:
            n_fail += 1
            out["error"] = attempts[-1][3] if attempts else "no candidates"
            results.append(out)
            print(f"  [{i:>3}] {fn[:65]:<65} FAIL ({len(attempts)} tries: {out['error']})")
            continue
        results.append(out)
        time.sleep(1.0)

    with open(out_report, "w", encoding="utf-8", newline="") as f:
        fields = ["rank","doi","year","cites","filename","title","oa_status",
                   "n_locations","downloaded","winning_host","winning_url","attempts","error"]
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(results)

    print(f"\n=== Summary ===")
    print(f"  Attempted:   {len(triage_top)}")
    print(f"  Skipped:     {n_skip} (already in library)")
    print(f"  Closed:      {n_no_oa}")
    print(f"  OA-no-URL:   {n_oa_no_url}")
    print(f"  OA usable:   {n_oa - n_oa_no_url}")
    print(f"  DOWNLOADED:  {n_dl}")
    print(f"  Failed:      {n_fail}")
    print(f"\nReport: {out_report}")

if __name__ == "__main__":
    main()
