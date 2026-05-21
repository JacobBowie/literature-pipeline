"""Recover DOIs for orphan PDFs via CrossRef title-match from filename hints.

An "orphan" here = a PDF whose sidecar (.fulltext.json) has an empty or
missing `doi` field. We parse year+author+title hints from the filename,
optionally augment from sidecar `title`/`text`, query CrossRef, filter the
results by document type, score the top match, label it by confidence, and
optionally write the resolved DOI/title/year/authors/journal back to the
sidecar.

Spec: _tools/literature_pipeline/notes/2026-05-21_scope_fill_missing_dois.md
Empirical basis: 21-sample test in crossref_titlematch_test.py (~70% recovery
across the 317 portfolio orphans).

Usage:
  python fill_missing_dois.py --project getpaid                    # dry-run report
  python fill_missing_dois.py --project getpaid --limit 20         # smoke test
  python fill_missing_dois.py --project getpaid --execute          # apply HIGH-confidence
  python fill_missing_dois.py --project getpaid --execute --min-confidence MED
  python fill_missing_dois.py --all                                # dry-run all active projects
  python fill_missing_dois.py --all --execute                      # apply across all
"""
import argparse
import csv
import json
import os
import re
import sys
import time
import unicodedata
from datetime import date
from pathlib import Path

import requests

try:
    if getattr(sys.stdout, "encoding", "").lower() != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, OSError):
    pass

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ris_emit import load_projects_config
from audit_filenames import safe_ascii

EMAIL = os.environ.get("LITPIPE_EMAIL", "jacob.bowie2@gmail.com")
UA = f"GETPAID-doi-fill/1.0 (mailto:{EMAIL})"
CROSSREF = "https://api.crossref.org/works"
PROJECTS_ROOT = Path(os.path.expanduser("~/Projects"))
CONFIG_PATH = Path(__file__).parent / "projects.json"

CANONICAL_RE = re.compile(r"^(\d{4})_([A-Z][A-Za-z\-']+)_([A-Z][A-Za-z0-9\-]+)\.pdf$")
CANONICAL_LOOSE_RE = re.compile(r"^(\d{4})_([A-Z][A-Za-z\-']+)_(.+)\.pdf$")
LEGACY_RE = re.compile(r"^([a-z]+)_(\d{4})_([a-z0-9_]+)\.pdf$")
BUNDLE_RE = re.compile(r"(?i)(?<![A-Za-z0-9])(vol\d|edition\d|issue\d|volume\d)(?![A-Za-z0-9])")
YEAR_RE = re.compile(r"(?<!\d)(?:19|20)\d{2}(?!\d)")

PREFERRED_TYPES = {"journal-article", "proceedings-article", "book-chapter",
                   "report", "monograph", "reference-entry", "review", "letter",
                   "editorial", "other"}
DEMOTED_TYPES = {"dataset", "peer-review", "component", "grant", "book",
                 "book-set", "book-series", "journal-issue", "journal-volume",
                 "journal", "posted-content", "standard"}

CONFIDENCE_RANK = {"HIGH": 4, "MED_NO_YEAR": 3, "MED_AUTHOR_ONLY": 3,
                   "MED_TITLE_STRONG": 3,
                   "MED_AUTHOR_YEAR_MISMATCH": 2, "MED_TYPE_MISMATCH": 2,
                   "LOW_TITLE_ONLY": 1, "AMBIG": 0, "NO_RESULT": 0,
                   "ERROR": 0, "SKIP_BUNDLED_ISSUE": 0, "SKIP_NO_HINTS": 0}


def parse_filename_hints(fn):
    """Return (year, author, title_hint, parse_class). year='' means unknown."""
    m = CANONICAL_RE.match(fn)
    if m:
        year, author, title_slug = m.groups()
        title_words = re.findall(r"[A-Z][a-z0-9\-']*", title_slug)
        title = " ".join(title_words)
        if year == "0000":
            year = ""
        return year, author, title, "canonical"
    m = CANONICAL_LOOSE_RE.match(fn)
    if m:
        year, author, rest = m.groups()
        parts = []
        for seg in rest.split("_"):
            words = re.findall(r"[A-Z][a-z0-9\-']*|[A-Z]+(?=[A-Z][a-z])|[A-Z]+|[a-z0-9]+", seg)
            parts.extend(w for w in words if w)
        title = " ".join(parts).strip()
        if year == "0000":
            year = ""
        return year, author, title, "canonical-loose"
    m = LEGACY_RE.match(fn)
    if m:
        author, year, title_slug = m.groups()
        title = title_slug.replace("_", " ")
        return year, author.title(), title, "legacy"
    ym = YEAR_RE.search(fn)
    year = ym.group(0) if ym else ""
    base = fn[:-4] if fn.lower().endswith(".pdf") else fn
    if year:
        base = base.replace(year, "")
    title = re.sub(r"_+", " ", base).strip()
    return year, None, title, "gibberish"


def normalize_for_match(s):
    """Lowercase + ASCII-fold for robust comparison.

    Reuses audit_filenames.safe_ascii which handles non-decomposable specials
    (ø→o, æ→ae, ß→ss, ł→l) that pure NFKD leaves intact. Keeping a single
    canonical fold across the pipeline ensures `2020_Molmen_*.pdf` (filename
    written via safe_ascii) round-trips against CrossRef's "Mølmen" record.
    """
    return safe_ascii(s).lower() if s else ""


def is_bundled_issue(fn, title_hint):
    """Detect journal-issue bundle patterns (`2007_IJCSS_Vol6_Edition2.pdf`)."""
    return bool(BUNDLE_RE.search(fn) or BUNDLE_RE.search(title_hint or ""))


def load_sidecar(sidecar_path):
    try:
        with open(sidecar_path, encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return None


def sidecar_title_hint(sidecar):
    """Pull a useful title hint from the sidecar when filename hint is weak.

    Order: sidecar.title field (if present) → first non-blank line of
    sidecar.text (often the cover title).
    """
    if not sidecar:
        return ""
    t = (sidecar.get("title") or "").strip()
    if t and len(t.split()) >= 3:
        return t
    text = sidecar.get("text") or ""
    for line in text.splitlines()[:30]:
        line = line.strip()
        if len(line.split()) >= 4 and not re.match(r"^[\d\W]+$", line):
            return line
    return t


def crossref_query(query_str, top_n=5, retries=3):
    """CrossRef bibliographic query. Returns (items, status)."""
    params = [("query.bibliographic", query_str), ("rows", str(top_n))]
    headers = {"User-Agent": UA}
    for attempt in range(retries):
        try:
            r = requests.get(CROSSREF, params=params, headers=headers, timeout=20)
            if r.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            r.raise_for_status()
            return r.json().get("message", {}).get("items", []), "OK"
        except requests.RequestException as e:
            if attempt == retries - 1:
                return [], f"ERR_{type(e).__name__}:{str(e)[:80]}"
            time.sleep(2 ** attempt)
    return [], "ERR_RETRIES_EXHAUSTED"


def extract_metadata(item):
    """Pull (score, doi, type, title, first_author_family, year, journal, authors_list)."""
    score = item.get("score", 0)
    doi = (item.get("DOI") or "").lower()
    typ = item.get("type") or ""
    title = " ".join(item.get("title") or []) or ""
    authors = item.get("author") or []
    first_family = (authors[0].get("family") or "").strip() if authors else ""
    year = ""
    for k in ("published-print", "published-online", "issued", "created"):
        if k in item and item[k].get("date-parts"):
            parts = item[k]["date-parts"]
            if parts and parts[0]:
                year = str(parts[0][0])
                break
    journal = ""
    cn = item.get("container-title") or []
    if cn:
        journal = cn[0]
    authors_list = []
    for a in authors:
        fam = a.get("family") or ""
        giv = a.get("given") or ""
        if fam:
            authors_list.append({"surname": fam, "given": giv, "source": "crossref"})
    return {
        "score": score, "doi": doi, "type": typ, "title": title,
        "first_family": first_family, "year": year, "journal": journal,
        "authors": authors_list,
    }


def filter_by_type(items):
    """Partition into (preferred, demoted, unknown). Preferred sort first."""
    preferred, demoted, unknown = [], [], []
    for it in items:
        t = (it.get("type") or "").lower()
        if t in PREFERRED_TYPES:
            preferred.append(it)
        elif t in DEMOTED_TYPES:
            demoted.append(it)
        else:
            unknown.append(it)
    return preferred + unknown, demoted


def score_match(year_hint, author_hint, ranked_items, raw_items):
    """Score the top filtered item against hints.

    ranked_items = type-preferred items (may be subset of raw_items).
    raw_items = original CrossRef hit list (for type-mismatch detection).
    Returns (status, chosen_item_dict_or_None, top1_score, top2_score).
    """
    if not ranked_items and not raw_items:
        return "NO_RESULT", None, 0, 0

    if not ranked_items and raw_items:
        meta = extract_metadata(raw_items[0])
        top2 = raw_items[1].get("score", 0) if len(raw_items) > 1 else 0
        return "MED_TYPE_MISMATCH", meta, meta["score"], top2

    top = extract_metadata(ranked_items[0])
    top2 = ranked_items[1].get("score", 0) if len(ranked_items) > 1 else 0
    runner = extract_metadata(ranked_items[1]) if len(ranked_items) > 1 else None

    # Collapse near-twin duplicates: same first_family + same year between top1/top2
    # means CrossRef returned essentially the same record twice (reprint records,
    # JSCR-style mirrors). Don't penalize as AMBIG.
    near_twin = False
    if runner and top["first_family"] and runner["first_family"]:
        if (normalize_for_match(top["first_family"]) == normalize_for_match(runner["first_family"])
                and top["year"] == runner["year"]):
            near_twin = True

    author_match = False
    if author_hint and top["first_family"]:
        ah = normalize_for_match(author_hint)
        rh = normalize_for_match(top["first_family"])
        if ah and rh and (ah in rh or rh in ah):
            author_match = True

    year_match = bool(year_hint and top["year"] and year_hint == top["year"])

    score_ratio = (top["score"] / top2) if top2 > 0 else float("inf")
    is_ambig = (score_ratio < 1.10) and not near_twin

    if is_ambig:
        return "AMBIG", top, top["score"], top2

    if author_match and year_match:
        return "HIGH", top, top["score"], top2

    if author_match and not year_hint:
        return "MED_NO_YEAR", top, top["score"], top2

    if author_match and year_hint and top["year"]:
        try:
            if abs(int(year_hint) - int(top["year"])) >= 2:
                return "MED_AUTHOR_YEAR_MISMATCH", top, top["score"], top2
        except ValueError:
            pass
        return "MED_AUTHOR_ONLY", top, top["score"], top2

    if author_match:
        return "MED_AUTHOR_ONLY", top, top["score"], top2

    # Title-strong fallback: author hint missing OR CrossRef record has no
    # author, BUT top1 is well-separated from top2 (ratio >= 1.20) AND year
    # matches (when known). Common with Elsevier abstract-records that drop
    # authors but keep title.
    if score_ratio >= 1.20 and (year_match or not year_hint):
        return "MED_TITLE_STRONG", top, top["score"], top2

    if year_match:
        return "LOW_TITLE_ONLY", top, top["score"], top2

    return "LOW_TITLE_ONLY", top, top["score"], top2


def update_sidecar(sidecar_path, sidecar_dict, match_meta):
    """Apply CrossRef match metadata to sidecar. Preserves existing fields."""
    sd = sidecar_dict
    sd["doi"] = match_meta["doi"]
    if not sd.get("title"):
        sd["title"] = match_meta["title"]
    if not sd.get("year"):
        sd["year"] = match_meta["year"]
    if not sd.get("journal"):
        sd["journal"] = match_meta["journal"]
    if not sd.get("authors") and match_meta["authors"]:
        sd["authors"] = match_meta["authors"]
    with open(sidecar_path, "w", encoding="utf-8") as fh:
        json.dump(sd, fh, indent=2, ensure_ascii=False)


def discover_orphans(lib_dir):
    """Yield (pdf_filename, sidecar_path, sidecar_dict) for PDFs whose sidecar
    has an empty/missing DOI."""
    if not os.path.isdir(lib_dir):
        return
    for fn in sorted(os.listdir(lib_dir)):
        if not fn.lower().endswith(".pdf"):
            continue
        sidecar = os.path.join(lib_dir, fn[:-4] + ".fulltext.json")
        if not os.path.isfile(sidecar):
            yield fn, sidecar, None
            continue
        sd = load_sidecar(sidecar)
        if sd is None:
            continue
        if not (sd.get("doi") or "").strip():
            yield fn, sidecar, sd


def discover_projects(arg_project, arg_all):
    """Return [(name, lib_dir_path)] from projects.json registry."""
    cfg = load_projects_config(CONFIG_PATH).get("projects", {})
    out = []
    for name, p in cfg.items():
        if not p.get("active", True):
            continue
        parent = p.get("parent")
        root = PROJECTS_ROOT / (parent if parent else name)
        lib = root / p["lib_dir"]
        if not lib.is_dir():
            continue
        out.append((name, lib))
    if arg_all:
        return out
    if arg_project:
        out = [(n, l) for n, l in out if n == arg_project]
        if not out:
            sys.exit(f"[fill_missing_dois] project '{arg_project}' not found in projects.json")
        return out
    sys.exit("[fill_missing_dois] must specify --project NAME or --all")


def build_query_string(year, author, title):
    """Build CrossRef query.bibliographic string from hints."""
    parts = []
    if author:
        parts.append(author)
    if title:
        parts.append(title)
    if year:
        parts.append(year)
    return " ".join(parts).strip()


def process_orphan(fn, sidecar_path, sidecar, polite_sleep=0.4):
    """Run one orphan through the full pipeline. Returns row dict."""
    year, author, title_hint, klass = parse_filename_hints(fn)

    row = {
        "filename": fn,
        "parse_class": klass,
        "parsed_year": year or "",
        "parsed_author": author or "",
        "parsed_title": title_hint or "",
        "status": "",
        "crossref_doi": "",
        "crossref_type": "",
        "crossref_title": "",
        "crossref_year": "",
        "crossref_first_author": "",
        "crossref_journal": "",
        "top1_score": "",
        "top2_score": "",
        "applied": False,
    }

    if is_bundled_issue(fn, title_hint):
        row["status"] = "SKIP_BUNDLED_ISSUE"
        return row, None

    augmented_title = title_hint
    if sidecar and (not title_hint or len(title_hint.split()) < 4):
        sct = sidecar_title_hint(sidecar)
        if sct and len(sct.split()) > len((title_hint or "").split()):
            augmented_title = sct
            row["parsed_title"] = augmented_title

    if not author and not year and (not augmented_title or len(augmented_title.split()) < 3):
        row["status"] = "SKIP_NO_HINTS"
        return row, None

    query_str = build_query_string(year, author, augmented_title)
    if not query_str:
        row["status"] = "SKIP_NO_HINTS"
        return row, None

    items, status = crossref_query(query_str, top_n=5)
    time.sleep(polite_sleep)
    if status != "OK":
        row["status"] = status
        return row, None
    if not items:
        row["status"] = "NO_RESULT"
        return row, None

    preferred, _demoted = filter_by_type(items)
    label, top_meta, top1, top2 = score_match(year, author, preferred, items)

    row["status"] = label
    row["top1_score"] = f"{top1:.2f}" if top1 else ""
    row["top2_score"] = f"{top2:.2f}" if top2 else ""
    if top_meta:
        row["crossref_doi"] = top_meta["doi"]
        row["crossref_type"] = top_meta["type"]
        row["crossref_title"] = top_meta["title"]
        row["crossref_year"] = top_meta["year"]
        row["crossref_first_author"] = top_meta["first_family"]
        row["crossref_journal"] = top_meta["journal"]
    return row, top_meta


def write_report(rows, path, fieldnames):
    with open(path, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})


def run_project(name, lib_dir, args):
    today = date.today().isoformat()
    print(f"\n=== Project: {name} ===")
    print(f"  Library: {lib_dir}")
    orphans = list(discover_orphans(str(lib_dir)))
    print(f"  Orphans (sidecar.doi empty): {len(orphans)}")
    if args.limit:
        orphans = orphans[:args.limit]
        print(f"  Limited to first {len(orphans)} orphans")

    min_rank = CONFIDENCE_RANK.get(args.min_confidence, 4)

    rows = []
    applied_rows = []
    review_rows = []
    skip_rows = []
    n_high = n_med = n_amb = n_low = n_err = n_skip = 0

    t0 = time.time()
    for i, (fn, sidecar_path, sidecar) in enumerate(orphans, 1):
        row, top_meta = process_orphan(fn, sidecar_path, sidecar)
        status = row["status"]

        if status.startswith("SKIP_"):
            n_skip += 1
            skip_rows.append(row)
        elif status in ("NO_RESULT",) or status.startswith("ERR_"):
            n_err += 1
        elif status == "HIGH":
            n_high += 1
        elif status.startswith("MED"):
            n_med += 1
            review_rows.append(row)
        elif status == "AMBIG":
            n_amb += 1
            review_rows.append(row)
        elif status.startswith("LOW"):
            n_low += 1
            review_rows.append(row)

        if (args.execute
            and top_meta
            and sidecar is not None
            and CONFIDENCE_RANK.get(status, 0) >= min_rank
            and CONFIDENCE_RANK[status] >= CONFIDENCE_RANK[args.min_confidence]):
            try:
                update_sidecar(sidecar_path, sidecar, top_meta)
                row["applied"] = True
                applied_rows.append(row)
                print(f"  [{i}/{len(orphans)}] APPLIED {status:25s} {fn[:60]} -> {top_meta['doi']}")
            except OSError as e:
                row["status"] = f"WRITE_ERROR_{e}"
                print(f"  [{i}/{len(orphans)}] WRITE-ERR {fn[:60]} ({e})", file=sys.stderr)
        else:
            tag = "DRY" if not args.execute else "SKIP"
            print(f"  [{i}/{len(orphans)}] {tag:5s} {status:25s} {fn[:50]:50s}"
                  f" {(top_meta['doi'] if top_meta else ''):<35}")

        rows.append(row)

    elapsed = time.time() - t0
    fieldnames = ["filename", "parse_class", "parsed_year", "parsed_author",
                  "parsed_title", "status", "crossref_doi", "crossref_type",
                  "crossref_title", "crossref_year", "crossref_first_author",
                  "crossref_journal", "top1_score", "top2_score", "applied"]
    report = lib_dir / f"_doi_fill_report.{today}.csv"
    applied = lib_dir / f"_doi_fill_applied.{today}.csv"
    review = lib_dir / f"_doi_fill_review.{today}.csv"
    skipped = lib_dir / f"_doi_fill_skipped.{today}.csv"

    write_report(rows, report, fieldnames)
    if applied_rows:
        write_report(applied_rows, applied, fieldnames)
    if review_rows:
        write_report(review_rows, review, fieldnames)
    if skip_rows:
        write_report(skip_rows, skipped, fieldnames)

    print(f"\n  --- Summary ({name}) ---")
    print(f"    Attempted:        {len(rows)}")
    print(f"    HIGH:             {n_high}")
    print(f"    MED:              {n_med}")
    print(f"    AMBIG:            {n_amb}")
    print(f"    LOW:              {n_low}")
    print(f"    SKIPPED:          {n_skip}")
    print(f"    NO_RESULT/ERR:    {n_err}")
    print(f"    Applied:          {len(applied_rows)}")
    print(f"    Wall time:        {elapsed:.1f}s")
    print(f"    Report:           {report}")
    if applied_rows: print(f"    Applied:          {applied}")
    if review_rows: print(f"    Review queue:     {review}")
    if skip_rows: print(f"    Skip log:         {skipped}")

    return {"name": name, "attempted": len(rows), "high": n_high, "med": n_med,
            "amb": n_amb, "low": n_low, "skip": n_skip, "err": n_err,
            "applied": len(applied_rows)}


def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--project", help="Project name from projects.json")
    g.add_argument("--all", action="store_true", help="All active projects")
    ap.add_argument("--execute", action="store_true",
                    help="Write resolved DOIs to sidecars (default: dry-run)")
    ap.add_argument("--min-confidence", default="HIGH",
                    choices=["HIGH", "MED_NO_YEAR", "MED_AUTHOR_ONLY",
                             "MED_TITLE_STRONG",
                             "MED_AUTHOR_YEAR_MISMATCH", "MED_TYPE_MISMATCH"],
                    help="Minimum confidence label required to write sidecar (default: HIGH)")
    ap.add_argument("--limit", type=int, default=None,
                    help="Process only the first N orphans (smoke-test)")
    args = ap.parse_args()

    print(f"User-Agent: {UA}")
    print(f"Mode: {'EXECUTE' if args.execute else 'DRY RUN'}")
    print(f"Min confidence to write: {args.min_confidence}")

    projects = discover_projects(args.project, args.all)
    print(f"Projects to process: {len(projects)}  ({', '.join(n for n, _ in projects)})")

    totals = []
    for name, lib in projects:
        totals.append(run_project(name, lib, args))

    print("\n=== Portfolio summary ===")
    print(f"  {'project':22s} {'attempted':>10s} {'HIGH':>5s} {'MED':>5s}"
          f" {'AMBIG':>6s} {'LOW':>5s} {'SKIP':>5s} {'ERR':>5s} {'applied':>8s}")
    for t in totals:
        print(f"  {t['name']:22s} {t['attempted']:>10d} {t['high']:>5d} {t['med']:>5d}"
              f" {t['amb']:>6d} {t['low']:>5d} {t['skip']:>5d} {t['err']:>5d}"
              f" {t['applied']:>8d}")


if __name__ == "__main__":
    main()
