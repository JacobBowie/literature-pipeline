"""Build a searchable text + metadata library from a project's PDF collection.

Output layout (relative to --out-dir):
  text/{filename}.txt        — per-PDF text dump (pymupdf, cleaned via pdf_text_clean)
  metadata.csv               — tabular index (year, author, title, pages, chars, venue, issues)
  abstracts.md               — markdown dump of extracted first-page abstract paragraphs
  library_report.md          — human-readable summary

Usage:
  python build_pdf_library.py                          # CWD-relative defaults
  python build_pdf_library.py --base-dir /path/to/project
  python build_pdf_library.py --lib-dir references/literature \\
                              --out-dir data/prior_art
"""
import os, re, sys, io, csv, argparse
try:
    if getattr(sys.stdout, "encoding", "").lower() != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, OSError):
    pass

if 'TESSDATA_PREFIX' not in os.environ:
    default_tessdata = os.path.expanduser(r'~\AppData\Local\miniconda3\share\tessdata')
    if os.path.isdir(default_tessdata):
        os.environ['TESSDATA_PREFIX'] = default_tessdata

import fitz
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pdf_text_clean import clean_pdf_text, report_issues


# ---------- filename parsing ----------

# Convention: Year_LastName_TitleSnippet.pdf (underscores throughout, only author last name)
FN_RE = re.compile(r'^(?P<year>[12]\d{3}|Unknown)_(?P<author>[A-Za-z\-]+?)_(?P<title>.+?)\.pdf$')

def parse_filename(fn):
    m = FN_RE.match(fn)
    if not m:
        return {"year": "?", "author": "?", "title_from_fn": fn[:-4]}
    d = m.groupdict()
    d["title_from_fn"] = d.pop("title").replace("_", " ")
    return d


# ---------- heuristic abstract extraction ----------

def extract_abstract(text):
    """Find 'Abstract' keyword and return the next ~300-600 chars of body text."""
    m = re.search(r'\b(?:Abstract|ABSTRACT|A B S T R A C T)\b[\.:]?\s*(.{150,1500}?)(?:\n\s*\n|\b(?:Keywords|KEYWORDS|Key words|Introduction|INTRODUCTION|1\s*\.\s*Introduction|1\.\s+Introduction)\b)',
                  text, re.DOTALL)
    if m:
        para = re.sub(r'\s+', ' ', m.group(1)).strip()
        return para[:800]
    paras = [p.strip() for p in re.split(r'\n\s*\n', text) if len(p.strip()) > 200]
    return re.sub(r'\s+', ' ', paras[0])[:800] if paras else ""


# ---------- venue heuristic ----------

VENUE_PATTERNS = [
    ("Scientific Reports",      r'Sci(?:entific)?\s*Rep(?:orts)?|scientific reports'),
    ("Med Sci Sports Exerc",    r'Med\.?\s*Sci\.?\s*Sports?\s*Exerc|Medicine\s*(?:&|and)\s*Science\s*in\s*Sports?\s*(?:&|and)\s*Exercise'),
    ("J Sports Sci",            r'J(?:ournal)?\s*(?:of\s*)?Sports?\s*Sci(?:ences?)?'),
    ("Sports Med",              r'Sports?\s*Med(?:icine)?(?!\s*Sci)'),
    ("Eur J Appl Physiol",      r'Eur(?:opean)?\s*J(?:ournal)?\s*(?:of\s*)?Appl(?:ied)?\s*Physiol'),
    ("Int J Perf Anal Sport",   r'Int(?:ernational)?\s*J(?:ournal)?\s*(?:of\s*)?Perf(?:ormance)?\s*Anal(?:ysis)?\s*in\s*Sport'),
    ("PLOS One/CompBio",        r'PL(?:o|O)S\s*(?:ONE|One|Comput|Computational)'),
    ("IEEE SMC",                r'IEEE\s*Trans(?:actions)?\s*Syst(?:ems)?\s*Man\s*Cybern'),
    ("Bull Math Biol",          r'Bull(?:etin)?\s*Math(?:ematical)?\s*Biol(?:ogy)?'),
    ("Aust J Sports Med",       r'Aust(?:ralian)?\s*J(?:ournal)?\s*(?:of\s*)?Sports?\s*Med(?:icine)?'),
    ("Eur J Sport Sci",         r'Eur(?:opean)?\s*J(?:ournal)?\s*(?:of\s*)?Sport\s*Sci(?:ence)?'),
    ("Springer (proceedings)",  r'Springer\s*(?:Nature|Heidelberg|Berlin|Boston|Cham)'),
]

def infer_venue(text):
    head = text[:3000]
    for name, pat in VENUE_PATTERNS:
        if re.search(pat, head, re.IGNORECASE):
            return name
    return ""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-dir", default=os.getcwd())
    ap.add_argument("--lib-dir", default="references/literature",
                     help="PDF directory, relative to --base-dir")
    ap.add_argument("--out-dir", default="data/prior_art",
                     help="Output directory for text/, metadata.csv, etc., relative to --base-dir")
    args = ap.parse_args()

    base = os.path.abspath(args.base_dir)
    lib_dir = os.path.join(base, args.lib_dir)
    out_dir = os.path.join(base, args.out_dir)
    text_dir = os.path.join(out_dir, "text")
    os.makedirs(text_dir, exist_ok=True)

    if not os.path.isdir(lib_dir):
        print(f"ERR: --lib-dir not a directory: {lib_dir}", file=sys.stderr); sys.exit(1)

    rows = []
    abstracts_md = ["# Abstracts — auto-extracted\n"]

    pdfs = sorted(f for f in os.listdir(lib_dir) if f.endswith(".pdf"))
    print(f"Project: {base}")
    print(f"Library: {lib_dir}")
    print(f"Output:  {out_dir}")
    print(f"Processing {len(pdfs)} PDFs...\n")

    for fn in pdfs:
        src = os.path.join(lib_dir, fn)
        meta = parse_filename(fn)
        try:
            with fitz.open(src) as doc:
                raw_text = "\n\n".join(p.get_text(sort=True) for p in doc)
                n_pages = len(doc)

            pre_issues = report_issues(raw_text)
            full_text = clean_pdf_text(raw_text)

            n_chars = len(full_text)
            cpp = n_chars / n_pages if n_pages else 0
            abstract = extract_abstract(full_text)
            venue = infer_venue(full_text)

            txt_path = os.path.join(text_dir, os.path.splitext(fn)[0] + ".txt")
            with open(txt_path, "w", encoding="utf-8") as f:
                f.write(full_text)

            row = {
                "filename": fn,
                "year": meta["year"],
                "author": meta["author"],
                "title_from_fn": meta["title_from_fn"],
                "pages": n_pages,
                "chars": n_chars,
                "chars_per_page": int(cpp),
                "venue_hint": venue,
                "abstract_snippet": abstract[:400].replace("\n", " "),
                "ligatures_fixed": pre_issues["ligatures"],
                "hyphens_rejoined": pre_issues["linebreak_hyphens"],
                "page_nums_stripped": pre_issues["bare_page_numbers"],
            }
            rows.append(row)
            abstracts_md.append(f"## {fn}\n\n"
                                 f"**Year**: {meta['year']}  |  **Author**: {meta['author']}  |  "
                                 f"**Venue hint**: {venue or '(not detected)'}  |  "
                                 f"**{n_pages}pp, {cpp:.0f}cpp**\n\n"
                                 f"{abstract or '_(no abstract extracted)_'}\n\n---\n")
            print(f"  {fn[:55]:<55} {meta['year']:>6} {cpp:>6.0f}cpp  {venue[:28]}")
        except Exception as e:
            print(f"  ERROR on {fn}: {e}")
            rows.append({"filename": fn, "year": meta["year"], "author": meta["author"],
                          "title_from_fn": meta["title_from_fn"], "pages": 0, "chars": 0,
                          "chars_per_page": 0, "venue_hint": "", "abstract_snippet": f"ERROR: {e}",
                          "ligatures_fixed": 0, "hyphens_rejoined": 0, "page_nums_stripped": 0})

    csv_path = os.path.join(out_dir, "metadata.csv")
    if not rows:
        print(f"ERR: no PDFs processed in {lib_dir}", file=sys.stderr); sys.exit(1)
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader(); w.writerows(rows)

    ab_path = os.path.join(out_dir, "abstracts.md")
    with open(ab_path, "w", encoding="utf-8") as f:
        f.write("\n".join(abstracts_md))

    rep_path = os.path.join(out_dir, "library_report.md")
    year_counts = {}
    for r in rows:
        year_counts[r["year"]] = year_counts.get(r["year"], 0) + 1
    with open(rep_path, "w", encoding="utf-8") as f:
        f.write(f"# Prior-art library — {len(rows)} PDFs\n\n")
        f.write(f"Built by `build_pdf_library.py` at {__import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n")
        f.write(f"Project: `{base}`\n\n")
        f.write(f"## By year\n\n")
        for y in sorted(year_counts.keys()):
            f.write(f"- {y}: {year_counts[y]}\n")
        f.write(f"\n## Text-layer quality\n\n")
        scan = [r for r in rows if r["chars_per_page"] < 500]
        f.write(f"- Clean text layer (>=500 cpp): {len(rows) - len(scan)}\n")
        f.write(f"- Sparse / scan (<500 cpp): {len(scan)}\n")
        if scan:
            f.write(f"\nPDFs needing OCR: {', '.join(r['filename'] for r in scan)}\n")
        f.write(f"\n## Venue distribution\n\n")
        venue_counts = {}
        for r in rows:
            v = r["venue_hint"] or "(not detected)"
            venue_counts[v] = venue_counts.get(v, 0) + 1
        for v, c in sorted(venue_counts.items(), key=lambda x: -x[1]):
            f.write(f"- {v}: {c}\n")
        f.write(f"\n## Cleaning applied (pdf_text_clean.py)\n\n")
        tot_lig = sum(r.get("ligatures_fixed", 0) for r in rows)
        tot_hy  = sum(r.get("hyphens_rejoined", 0) for r in rows)
        tot_pg  = sum(r.get("page_nums_stripped", 0) for r in rows)
        n_lig_papers = sum(1 for r in rows if r.get("ligatures_fixed", 0) > 0)
        f.write(f"- Ligatures expanded: **{tot_lig}** across {n_lig_papers} papers\n")
        f.write(f"- Soft-hyphens rejoined: **{tot_hy}**\n")
        f.write(f"- Bare page-number lines stripped: **{tot_pg}**\n")
        if rows:
            worst_lig = sorted(rows, key=lambda r: -r.get("ligatures_fixed", 0))[:3]
            f.write(f"\nMost ligatures (top 3):\n")
            for r in worst_lig:
                if r.get("ligatures_fixed", 0):
                    f.write(f"- {r['filename']}: {r['ligatures_fixed']}\n")
        f.write(f"\n## Artifacts\n\n")
        f.write(f"- Full-text corpus: `{text_dir}/` ({len(rows)} .txt files)\n")
        f.write(f"- Metadata CSV: `{csv_path}`\n")
        f.write(f"- Abstracts dump: `{ab_path}`\n")

    print(f"\nBuilt library:")
    print(f"  Metadata: {csv_path}")
    print(f"  Text corpus: {text_dir}/ ({len(rows)} files)")
    print(f"  Abstracts: {ab_path}")
    print(f"  Report: {rep_path}")


if __name__ == "__main__":
    main()
