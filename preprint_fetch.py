"""Tertiary fetcher — find preprint companions on arXiv / Europe PMC preprints / OSF.

Why tertiary: Unpaywall + PMC handle published OA. This catches:
  (a) papers with arXiv preprints we want alongside the published version
  (b) papers where the published version is paywalled but a preprint is free
  (c) papers from sportRxiv / bioRxiv / medRxiv

Strategy per source:
  arXiv      — Atom API title search. Reliable. PDF at arxiv.org/pdf/{id}.pdf.
  Europe PMC — search?query=TITLE:"..." AND SRC:PPR. Covers bioRxiv, medRxiv, ChemRxiv,
               and others Europe PMC indexes as preprints. PDF via getPdf endpoint.
  OSF        — api.osf.io/v2/preprints with title filter. Covers sportRxiv (provider:sportrxiv)
               + ~25 other OSF-hosted servers. PDF via 'primary_file' relationship.

Match scoring:
  - Normalize titles (lowercase, alpha+digit only)
  - SequenceMatcher ratio between query and candidate
  - Threshold 0.80 default; tunable via --min-similarity
  - Tie-break by year proximity when available

Saves found PDFs with `_preprint` suffix in the same library directory.

Usage:
  python tools/preprint_fetch.py \\
      --triage data/prior_art/discovered/triage_not_in_library.csv \\
      --lib-dir references/literature
"""
import os, sys, io, csv, re, time, json, argparse
import requests
from difflib import SequenceMatcher
from urllib.parse import urlencode
from xml.etree import ElementTree as ET

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lit_util  # RC2/RC3/RC4 audit-remediation helpers
# T5a (2026-06-25 audit): reject known publisher boilerplate in this stage too (OSF/biorxiv mirrors).
from unpaywall_fetch_v2 import is_known_boilerplate
# RC2/RC3: reuse the collision-safe dest + DOI<->content helpers (single source of truth).
from unpaywall_fetch_v2 import (resolve_dest, pdf_doi_disagrees,
                                doi_from_pdf_bytes, _doi_of_existing)

try:
    if getattr(sys.stdout, "encoding", "").lower() != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, OSError):
    pass

EMAIL  = os.environ.get("LITPIPE_EMAIL", "jacob.bowie2@gmail.com")
UA     = f"GETPAID-preprint-fetch/1.0 (mailto:{EMAIL})"
BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

ARXIV_API   = "http://export.arxiv.org/api/query"
EPMC_SEARCH = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
EPMC_PDF    = "https://europepmc.org/articles/{pmcid}?pdf=render"  # for PMC preprints
OSF_API     = "https://api.osf.io/v2/preprints/"

ARXIV_NS = {"a": "http://www.w3.org/2005/Atom", "ax": "http://arxiv.org/schemas/atom"}


# ---------- title normalization + scoring ----------

def norm_title(t):
    t = re.sub(r"<[^>]+>", "", t or "")
    t = re.sub(r"[^a-zA-Z0-9 ]+", " ", t).lower()
    return re.sub(r"\s+", " ", t).strip()


def title_similarity(a, b):
    return SequenceMatcher(None, norm_title(a), norm_title(b)).ratio()


def slug_filename(year, author, title):
    # Reuse the robust lastname extractor + NFKD normalizer from unpaywall_fetch_v2
    # (handles ; and , separators, "et al" stripping, LastName-Initial vs
    # Initial-LastName; safe_ascii NFKD-normalizes accents before ASCII strip).
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from unpaywall_fetch_v2 import last_name
    from ris_emit import safe_ascii
    title = re.sub(r"<[^>]+>", "", title or "")
    title = safe_ascii(title)
    title = re.sub(r"[^\w\s\-]", " ", title)
    title = re.sub(r"\s+", " ", title).strip()
    skip = {"a","an","the","of","in","on","and","to","for","at","from","with","by","as"}
    words = [w for w in title.split() if w.lower() not in skip][:6]
    slug = "".join(w.capitalize() for w in words) or "Untitled"
    last = safe_ascii(last_name(author)) if author else "Unknown"
    return f"{year or 'Unknown'}_{last}_{slug}_preprint.pdf"


# ---------- arXiv ----------

def search_arxiv(title, n=3):
    """Return list of {id, title, year, pdf_url, authors}."""
    try:
        r = requests.get(ARXIV_API,
                          params={"search_query": f'ti:"{title[:200]}"', "max_results": n},
                          headers={"User-Agent": UA}, timeout=20)
        if r.status_code != 200: return []
        root = ET.fromstring(r.content)
        out = []
        for entry in root.findall("a:entry", ARXIV_NS):
            arx_id_el = entry.find("a:id", ARXIV_NS)
            tit_el    = entry.find("a:title", ARXIV_NS)
            pub_el    = entry.find("a:published", ARXIV_NS)
            if arx_id_el is None or tit_el is None: continue
            arx_id = arx_id_el.text.rsplit("/", 1)[-1]
            year   = pub_el.text[:4] if pub_el is not None and pub_el.text else ""
            authors = [a.findtext("a:name", "", ARXIV_NS) for a in entry.findall("a:author", ARXIV_NS)]
            out.append({
                "source": "arxiv", "id": arx_id, "title": (tit_el.text or "").strip(),
                "year": year, "authors": "; ".join(authors),
                "pdf_url": f"https://arxiv.org/pdf/{arx_id}.pdf",
            })
        return out
    except Exception as e:
        print(f"    arxiv error: {e}"); return []


# ---------- Europe PMC preprints ----------

def _keywordize(title, n=8):
    """Extract significant title words for a non-phrase keyword query."""
    skip = {"a","an","the","of","in","on","and","or","to","for","at","from","with","by","as",
             "is","are","was","were","be","been","this","that","these","those"}
    words = re.findall(r"[a-zA-Z0-9]{3,}", title.lower())
    return " ".join(w for w in words if w not in skip)[:200]


def _preprint_pdf_url(doi):
    """Map a preprint DOI to a direct PDF URL by server-prefix convention.

    Known patterns:
      10.1101/        → bioRxiv or medRxiv. Both serve {url}.full.pdf
      10.21203/       → Research Square. Landing page only via DOI; needs scrape.
      10.31219/       → OSF preprints (sportRxiv, PsyArXiv, etc.). Needs scrape.
      10.20944/       → Preprints.org. Landing page; needs scrape.
      10.31234/       → PsyArXiv (subset of OSF).
    Returns (pdf_url, scrape_required:bool).
    """
    if not doi: return None, False
    d = doi.lower()
    if d.startswith("10.1101/"):
        # Try bioRxiv first; medRxiv shares 10.1101 prefix though
        return f"https://www.biorxiv.org/content/{doi}.full.pdf", False
    # For other prefixes, return the DOI URL and let caller scrape
    return f"https://doi.org/{doi}", True


def search_epmc_preprints(title, n=5):
    """EPMC preprint search.

    Returns records with `pdf_url` for downloads we can do directly (currently
    bioRxiv/medRxiv pattern), and a `landing_url` flag for ones requiring scrape.
    """
    try:
        kws = _keywordize(title, n=10)
        q = f'({kws}) AND SRC:PPR'
        r = requests.get(EPMC_SEARCH,
                          params={"query": q, "format": "json", "pageSize": n,
                                  "resultType": "core"},
                          headers={"User-Agent": UA}, timeout=20)
        if r.status_code != 200: return []
        data = r.json()
        out = []
        for rec in data.get("resultList", {}).get("result", []):
            pmcid = rec.get("pmcid", "")
            doi   = rec.get("doi", "")
            ppr_id = rec.get("id", "")
            # PMC-deposited preprint (rare; gives clean PDF)
            if pmcid:
                out.append({
                    "source": "epmc-preprint-pmc", "id": pmcid,
                    "title": rec.get("title", ""),
                    "year": str(rec.get("pubYear", "")),
                    "authors": rec.get("authorString", ""),
                    "pdf_url": EPMC_PDF.format(pmcid=pmcid), "doi": doi,
                    "needs_scrape": False,
                })
                continue
            # DOI-based preprint server
            if doi:
                pdf_url, scrape_required = _preprint_pdf_url(doi)
                if pdf_url is None: continue
                # tag source by DOI prefix for clarity
                prefix = doi.split("/")[0] if "/" in doi else "epmc"
                tag = {"10.1101":"biorxiv-medrxiv", "10.21203":"researchsquare",
                       "10.31219":"osf-preprints", "10.20944":"preprints.org",
                       "10.31234":"psyarxiv"}.get(prefix, f"epmc:{prefix}")
                out.append({
                    "source": tag, "id": ppr_id,
                    "title": rec.get("title", ""),
                    "year": str(rec.get("pubYear", "")),
                    "authors": rec.get("authorString", ""),
                    "pdf_url": pdf_url, "doi": doi,
                    "needs_scrape": scrape_required,
                })
        return out
    except Exception as e:
        print(f"    epmc error: {e}"); return []


# ---------- OSF (covers sportRxiv, ChemRxiv, EngrXiv, etc.) ----------

def search_osf_preprints(title, n=3):
    try:
        params = {"filter[title]": title[:200], "page[size]": n}
        r = requests.get(OSF_API, params=params, headers={"User-Agent": UA}, timeout=20)
        if r.status_code != 200: return []
        data = r.json()
        out = []
        for rec in data.get("data", [])[:n]:
            attrs = rec.get("attributes", {})
            rel   = rec.get("relationships", {})
            pf    = (rel.get("primary_file") or {}).get("data") or {}
            if not pf.get("id"): continue
            # Need to resolve primary_file → download URL
            file_id = pf["id"]
            download_url = f"https://api.osf.io/v2/files/{file_id}/?action=download"
            # Provider hint
            provider = (rel.get("provider") or {}).get("data", {}).get("id", "osf")
            out.append({
                "source": f"osf:{provider}", "id": rec.get("id",""),
                "title": attrs.get("title",""),
                "year": (attrs.get("date_published") or "")[:4],
                "authors": "",  # OSF returns these via separate endpoint
                "doi": attrs.get("doi") or "",
                "pdf_url": download_url,
            })
        return out
    except Exception as e:
        print(f"    osf error: {e}"); return []


# ---------- aggregate find_preprint ----------

def find_preprint(title, year, min_similarity=0.80, prefer_direct_pdf=True,
                   max_year_delta=2):
    """Search all 3 sources, return best match meeting threshold (or None).

    prefer_direct_pdf: when True, downrank candidates that require landing-page scraping.
    max_year_delta: reject matches more than N years apart from the published-paper year.
                    Prevents false-positive matches with similar-titled later/earlier work.
    """
    cands = []
    cands += search_arxiv(title)
    time.sleep(0.4)
    cands += search_epmc_preprints(title)
    time.sleep(0.4)
    cands += search_osf_preprints(title)
    time.sleep(0.4)
    if not cands: return None

    try:
        target_year = int(str(year)) if year else None
    except (ValueError, TypeError):
        target_year = None

    for c in cands:
        c["sim"] = title_similarity(title, c["title"])
        try:
            cy = int(str(c.get("year",""))) if c.get("year") else None
        except (ValueError, TypeError):
            cy = None
        c["year_match"] = (cy == target_year) if target_year else False
        c["year_delta"] = abs(cy - target_year) if (cy and target_year) else 999
        c.setdefault("needs_scrape", False)

    # Reject matches with year-delta > max_year_delta when both years known
    cands = [c for c in cands if c["year_delta"] <= max_year_delta or c["year_delta"] == 999]
    if not cands: return None

    # rank: similarity > year_match > direct_pdf
    cands.sort(
        key=lambda c: (c["sim"], c["year_match"],
                        not c["needs_scrape"] if prefer_direct_pdf else True),
        reverse=True
    )
    best = cands[0]
    if best["sim"] >= min_similarity:
        return best
    return None


# ---------- download ----------

def is_pdf(b): return b[:4] == b"%PDF"

def fetch_pdf(url, dest, timeout=30):
    try:
        r = requests.get(url, headers={"User-Agent": BROWSER_UA, "Accept":"application/pdf,*/*"},
                          timeout=timeout, stream=True, allow_redirects=True)
        if r.status_code != 200: return False, f"HTTP_{r.status_code}"
        first=b""; chunks=[]; total=0
        for c in r.iter_content(8192):
            if not c: continue
            if not first: first=c
            chunks.append(c); total+=len(c)
            if total > 60_000_000: break
        if not is_pdf(first): return False, "NOT_PDF"
        with open(dest, "wb") as f:
            for c in chunks: f.write(c)
        sz = os.path.getsize(dest)
        if sz < 10_000:
            os.remove(dest); return False, f"TOO_SMALL_{sz}B"
        # T5a (2026-06-25 audit): reject known publisher boilerplate that passes %PDF + size.
        is_bp, tag = is_known_boilerplate(dest)
        if is_bp:
            os.remove(dest); return False, f"BOILERPLATE_{tag or ''}"
        return True, f"OK_{sz}B"
    except Exception as e:
        return False, f"ERR_{str(e)[:80]}"


# ---------- main ----------

def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    ap.add_argument("--triage", required=True,
                     help="CSV with columns: doi,title,year,authors")
    ap.add_argument("--lib-dir", required=True,
                     help="Directory to save fetched preprints")
    ap.add_argument("--min-similarity", type=float, default=0.80,
                     help="Minimum normalized-title SequenceMatcher ratio to accept a match (default: 0.80).")
    ap.add_argument("--limit", type=int, default=0,
                     help="Process only first N rows (0 = all)")
    ap.add_argument("--dry-run", action="store_true",
                     help="Search preprint sources and log matches, but do not download PDFs.")
    ap.add_argument("--report", default=None,
                     help="Output CSV (default: data/prior_art/discovered/preprint_fetch_report.csv)")
    ap.add_argument("--no-write-ris", action="store_true",
                     help="Skip writing .ris sidecar next to each successfully fetched PDF.")
    args = ap.parse_args()
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import ris_emit as _R

    rows = []
    with open(args.triage, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append(r)
    if args.limit: rows = rows[:args.limit]

    os.makedirs(args.lib_dir, exist_ok=True)
    existing = set(os.listdir(args.lib_dir))
    written_this_run = set()  # RC2: PDFs written this run; never clobber them

    report = []
    n_found = n_dl = n_skip = n_no_match = n_fail = 0
    n_mismatch = 0

    for i, row in enumerate(rows, 1):
        title = (row.get("title") or "").strip()
        year = (row.get("year") or "").strip()
        authors = (row.get("authors") or "").strip()
        doi = (row.get("doi") or "").strip().lower()
        if not title:
            continue
        fn = slug_filename(year, authors, title)
        dest = os.path.join(args.lib_dir, fn)

        rec = {"doi": doi, "title": title[:80], "year": year,
               "preprint_filename": fn, "found": False, "source": "",
               "match_id": "", "similarity": "", "downloaded": False,
               "skipped": False, "status": ""}

        # RC2: skip only when the on-disk preprint is THIS doi (or carries no DOI to
        # contradict it); a same-name file for a DIFFERENT doi is a collision.
        if fn in existing and ((not _doi_of_existing(dest))
                                or _doi_of_existing(dest) == lit_util.normalize_doi(doi)):
            n_skip += 1
            # RC2 (sweep dedupe): ALREADY_EXISTS is a skip, not a fresh download.
            rec["status"] = "ALREADY_EXISTS"; rec["skipped"] = True
            report.append(rec)
            print(f"  [{i:>3}/{len(rows)}] SKIP {fn[:75]}")
            continue

        match = find_preprint(title, year, min_similarity=args.min_similarity)
        if not match:
            n_no_match += 1
            rec["status"] = "NO_MATCH"
            report.append(rec)
            print(f"  [{i:>3}/{len(rows)}] --   no preprint for: {title[:70]}")
            continue

        n_found += 1
        rec["found"] = True; rec["source"] = match["source"]
        rec["match_id"] = match["id"]; rec["similarity"] = f"{match['sim']:.2f}"

        if args.dry_run:
            rec["status"] = "DRY"
            report.append(rec)
            scrape_tag = " (needs-scrape)" if match.get("needs_scrape") else ""
            print(f"  [{i:>3}/{len(rows)}] DRY  {match['source']:<22} sim={match['sim']:.2f}{scrape_tag} {match['title'][:50]}")
            continue

        if match.get("needs_scrape"):
            # Don't try to fetch — DOI URL would just save the HTML landing page.
            # Log the find so the user can click through manually.
            rec["status"] = "MANUAL_PREPRINT"
            rec["downloaded"] = False
            print(f"  [{i:>3}/{len(rows)}] MANUAL {match['source']:<22} sim={match['sim']:.2f} -> {match['pdf_url']}")
            report.append(rec)
            time.sleep(0.4)
            continue

        # RC2: collision-safe destination. Disambiguate on the preprint's own DOI when
        # known (the artifact's identity), else the queue DOI.
        preprint_doi = (match.get("doi") or "").strip().lower()
        dest, collided = resolve_dest(args.lib_dir, fn, preprint_doi or doi, written_this_run)
        if collided:
            fn = os.path.basename(dest)
            rec["preprint_filename"] = fn

        ok, st = fetch_pdf(match["pdf_url"], dest)
        rec["downloaded"] = ok; rec["status"] = st
        if ok:
            n_dl += 1
            written_this_run.add(dest)
            existing.add(fn)
            print(f"  [{i:>3}/{len(rows)}] DL   {match['source']:<14} sim={match['sim']:.2f} -> {fn[:55]}")
            # RC3: a preprint legitimately carries a DIFFERENT DOI from the published
            # (queue) paper, so a queue-DOI disagreement alone is NOT a wrong-paper signal.
            # Only flag when the PDF's DOI matches NEITHER the queue DOI NOR the matched
            # preprint's own DOI -- i.e. the bytes are some third, unrelated paper.
            found_pdf_doi = doi_from_pdf_bytes(dest)
            mismatch = bool(found_pdf_doi) and pdf_doi_disagrees(dest, doi) and (
                (not preprint_doi) or found_pdf_doi != lit_util.normalize_doi(preprint_doi))
            if mismatch:
                n_mismatch += 1
                rec["status"] = f"DOI_MISMATCH:pdf_doi={found_pdf_doi}"
                print(f"        DOI_MISMATCH: pdf DOI {found_pdf_doi} != queue/preprint DOI; skipping .ris")
            elif not args.no_write_ris and doi:
                ris_status, _ = _R.emit_ris_for_pdf(doi, dest)
                print(f"        ris: {ris_status}")
        else:
            n_fail += 1
            print(f"  [{i:>3}/{len(rows)}] FAIL {match['source']:<14} sim={match['sim']:.2f} ({st})")
        report.append(rec)
        time.sleep(0.6)

    # Write report (RC4: build in memory, write atomically).
    out_csv = args.report or "data/prior_art/discovered/preprint_fetch_report.csv"
    out_dir = os.path.dirname(out_csv)
    if out_dir: os.makedirs(out_dir, exist_ok=True)
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=["doi","title","year","preprint_filename",
                                          "found","source","match_id","similarity",
                                          "downloaded","skipped","status"],
                       extrasaction="ignore", lineterminator="\n")
    w.writeheader(); w.writerows(report)
    lit_util.atomic_write_text(out_csv, buf.getvalue())

    print(f"\n=== Summary ===")
    print(f"  Rows processed:    {len(rows)}")
    print(f"  Already had file:  {n_skip}")
    print(f"  Preprints found:   {n_found}")
    print(f"  Downloaded:        {n_dl}")
    print(f"  DOI mismatch:      {n_mismatch} (PDF DOI unrelated; .ris skipped)")
    print(f"  Download failed:   {n_fail}")
    print(f"  No match:          {n_no_match}")
    print(f"\nReport: {out_csv}")


if __name__ == "__main__":
    main()
