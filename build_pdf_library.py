"""Build a searchable text + metadata library from a project's PDF collection.

Output layout (relative to --out-dir):
  text/{filename}.txt        — per-PDF text dump (pymupdf, cleaned via pdf_text_clean)
  metadata.csv               — tabular index (year, author, title, pages, chars, venue, issues)
  abstracts.md               — markdown dump of extracted first-page abstract paragraphs
  library_report.md          — human-readable summary

The text-validity gate (extract_pdf_fulltext.text_validity, W4-C) runs on every PDF: a scan with a
bad OCR layer, an image-only file or a mojibake layer gets an empty .txt dump, no abstract, and
`text_gate = fail: <reasons>` in metadata.csv (OCR it with extract_pdf_fulltext --ocr).

Two PDFs whose stems are equal up to case (Windows file names are not case-sensitive) get two
dumps: the first in sorted order keeps `<stem>.txt`, a later one gets `<stem>__2.txt` (then
`__3`, ...); the `txt_file` column names each.

Error-rate gate: when every PDF, or more than half, raised or failed the text gate, none of
metadata.csv, abstracts.md, library_report.md and the text/ dumps is written; the rows go to
metadata.errors.csv and the run exits 2 after a `[step-summary] {json}` line. The dumps are
written only after the gate decides. Exit 1: a usage
error, a missing --lib-dir, or no PDFs.

Usage:
  python build_pdf_library.py                          # CWD-relative defaults
  python build_pdf_library.py --base-dir /path/to/project
  python build_pdf_library.py --lib-dir references/literature \\
                              --out-dir data/prior_art
"""
import os, re, sys, csv, json, argparse
import lit_util
lit_util.utf8_stdout()

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import extract_pdf_fulltext as _xf  # noqa: E402  (the text-validity gate; TESSDATA_PREFIX default)

# TESSDATA_PREFIX for OCR: an inherited value wins on every platform; else, on Windows only and only
# when the folder exists, a per-user %LOCALAPPDATA%\Tesseract-OCR\tessdata (use-case-only: a
# winget install bundles only eng and osd). See extract_pdf_fulltext.ensure_tessdata_prefix.
_xf.ensure_tessdata_prefix()

import pymupdf  # noqa: E402
from pdf_text_clean import clean_pdf_text, report_issues  # noqa: E402

SUMMARY_MARKER = "[step-summary] "
EXIT_OK, EXIT_USAGE, EXIT_DEGRADED = 0, 1, 2
MAX_BAD_SHARE = 0.5          # more than this share raised or failed the gate: nothing is replaced


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


# ---------- .txt dump names ----------

def txt_dump_names(pdfs):
    """{pdf file name: .txt dump name}. The stem plus `.txt`, except that a stem equal to an
    earlier one (sorted order) up to case gets `__2`, `__3`, ...: the first suffix whose name is
    still free up to case. Deterministic, so a rebuild writes the same names."""
    out, used = {}, set()
    order = sorted(pdfs)
    for fn in order:
        stem = os.path.splitext(fn)[0]
        if stem.casefold() not in used:
            used.add(stem.casefold())
            out[fn] = stem + ".txt"
    for fn in order:
        if fn in out:
            continue
        stem = os.path.splitext(fn)[0]
        n = 2
        while f"{stem}__{n}".casefold() in used:
            n += 1
        used.add(f"{stem}__{n}".casefold())
        out[fn] = f"{stem}__{n}.txt"
    return out


# ---------- the run ----------

class _Parser(argparse.ArgumentParser):
    """Usage errors exit 1 (argparse's 2 would read as DEGRADED to a caller)."""
    def error(self, message):
        self.print_usage(sys.stderr)
        self.exit(EXIT_USAGE, f"{self.prog}: error: {message}\n")


def _error_row(fn, meta, e):
    return {"filename": fn, "year": meta["year"], "author": meta["author"],
            "title_from_fn": meta["title_from_fn"], "pages": 0, "chars": 0,
            "chars_per_page": 0, "venue_hint": "", "abstract_snippet": f"ERROR: {e}",
            "ligatures_fixed": 0, "hyphens_rejoined": 0, "page_nums_stripped": 0,
            "text_gate": "error", "txt_file": ""}


FIELDS = ["filename", "year", "author", "title_from_fn", "pages", "chars", "chars_per_page",
          "venue_hint", "abstract_snippet", "ligatures_fixed", "hyphens_rejoined",
          "page_nums_stripped", "text_gate", "txt_file"]


def run(*, base_dir=None, lib_dir="references/literature", out_dir="data/prior_art",
        **_ignored) -> dict:
    """Build the library (see the module docstring). Returns {"exit", "pdfs", "ok",
    "gate_failed", "errors", "reasons", "metadata", "errors_csv"}."""
    res = {"exit": EXIT_OK, "pdfs": 0, "ok": 0, "gate_failed": 0, "errors": 0, "reasons": [],
           "metadata": "", "errors_csv": ""}
    base = os.path.abspath(base_dir or os.getcwd())
    lib_dir = os.path.join(base, lib_dir)
    out_dir = os.path.join(base, out_dir)
    text_dir = os.path.join(out_dir, "text")

    if not os.path.isdir(lib_dir):
        print(f"ERR: --lib-dir not a directory: {lib_dir}", file=sys.stderr)
        res["exit"] = EXIT_USAGE
        return res

    rows = []
    abstracts_md = ["# Abstracts — auto-extracted\n"]
    gate_failed = []
    dumps = {}                  # txt name -> text of a gate-passed PDF, written after the error-rate gate

    pdfs = sorted(f for f in os.listdir(lib_dir) if f.endswith(".pdf"))
    txt_names = txt_dump_names(pdfs)
    print(f"Project: {base}")
    print(f"Library: {lib_dir}")
    print(f"Output:  {out_dir}")
    print(f"Processing {len(pdfs)} PDFs...\n")

    for fn in pdfs:
        src = os.path.join(lib_dir, fn)
        meta = parse_filename(fn)
        try:
            with pymupdf.open(src) as doc:
                pages = [p.get_text(sort=True) for p in doc]
                n_pages = len(doc)
            raw_text = "\n\n".join(pages)

            pre_issues = report_issues(raw_text)
            full_text = clean_pdf_text(raw_text)
            gate = _xf.text_validity(full_text, pages, n_pages)

            n_chars = len(full_text)
            cpp = n_chars / n_pages if n_pages else 0
            if gate["ok"]:
                abstract = extract_abstract(full_text)
                venue = infer_venue(full_text)
                dumps[txt_names[fn]] = full_text
            else:
                abstract, venue = "", ""
                gate_failed.append(fn)

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
                "text_gate": "pass" if gate["ok"] else "fail: " + ";".join(gate["reasons"]),
                "txt_file": txt_names[fn],
            }
            rows.append(row)
            if gate["ok"]:
                body = abstract or '_(no abstract extracted)_'
            else:
                body = (f"_(text layer failed the validity gate: {', '.join(gate['reasons'])}; "
                        f"needs OCR)_")
            abstracts_md.append(f"## {fn}\n\n"
                                 f"**Year**: {meta['year']}  |  **Author**: {meta['author']}  |  "
                                 f"**Venue hint**: {venue or '(not detected)'}  |  "
                                 f"**{n_pages}pp, {cpp:.0f}cpp**\n\n"
                                 f"{body}\n\n---\n")
            flag = "" if gate["ok"] else "  GATE: " + ",".join(gate["reasons"])
            print(f"  {fn[:55]:<55} {meta['year']:>6} {cpp:>6.0f}cpp  {venue[:28]}{flag}")
        except Exception as e:
            print(f"  ERROR on {fn}: {e}")
            rows.append(_error_row(fn, meta, e))

    csv_path = os.path.join(out_dir, "metadata.csv")
    res["pdfs"] = len(pdfs)
    if not rows:
        print(f"ERR: no PDFs processed in {lib_dir}", file=sys.stderr)
        res["exit"] = EXIT_USAGE
        return res

    n_err = sum(1 for r in rows if r["text_gate"] == "error")
    n_bad = n_err + len(gate_failed)
    res.update(ok=len(rows) - n_bad, gate_failed=len(gate_failed), errors=n_err)
    if n_bad == len(rows) or n_bad > len(rows) * MAX_BAD_SHARE:
        os.makedirs(out_dir, exist_ok=True)
        err_path = os.path.join(out_dir, "metadata.errors.csv")
        lit_util.atomic_write_csv(err_path, rows, FIELDS, newline="\r\n")
        res["errors_csv"] = err_path
        res["reasons"].append(f"{n_bad} of {len(rows)} PDFs raised ({n_err}) or failed the text "
                              f"gate ({len(gate_failed)}): metadata.csv, abstracts.md, "
                              f"library_report.md and the text/ dumps left as they were")
        print(f"\nERR: {n_bad} of {len(rows)} PDFs raised ({n_err}) or failed the text-validity "
              f"gate ({len(gate_failed)}).\n  metadata.csv, abstracts.md and library_report.md were NOT "
              f"overwritten and no text/ dump was written; the rows are in {err_path}", file=sys.stderr)
        res["exit"] = EXIT_DEGRADED
        print(SUMMARY_MARKER + json.dumps({"reasons": res["reasons"], "aborted": None,
                                           "transport_failures": 0}, ensure_ascii=False),
              flush=True)
        return res

    # Only now, the error-rate gate passed: the dumps (W5-C2, C114; a degraded run writes none).
    # A failed text layer leaves an empty dump: the text corpus keeps one file per PDF, and no
    # earlier dump of the same bad layer survives.
    os.makedirs(text_dir, exist_ok=True)
    for name, text in dumps.items():
        lit_util.atomic_write_text(os.path.join(text_dir, name), text)
    for fn in gate_failed:
        lit_util.atomic_write_text(os.path.join(text_dir, txt_names[fn]), "")

    lit_util.atomic_write_csv(csv_path, rows, FIELDS, newline="\r\n")
    res["metadata"] = csv_path

    ab_path = os.path.join(out_dir, "abstracts.md")
    lit_util.atomic_write_text(ab_path, "\n".join(abstracts_md), newline=None)

    rep_path = os.path.join(out_dir, "library_report.md")
    year_counts = {}
    for r in rows:
        year_counts[r["year"]] = year_counts.get(r["year"], 0) + 1
    rep = []
    w = rep.append
    w(f"# Prior-art library — {len(rows)} PDFs\n\n")
    w(f"Built by `build_pdf_library.py` at {__import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n")
    w(f"Project: `{base}`\n\n")
    w(f"## By year\n\n")
    for y in sorted(year_counts.keys()):
        w(f"- {y}: {year_counts[y]}\n")
    w(f"\n## Text-layer quality\n\n")
    scan = [r for r in rows if r["chars_per_page"] < 500]
    w(f"- Clean text layer (>=500 cpp): {len(rows) - len(scan)}\n")
    w(f"- Sparse / scan (<500 cpp): {len(scan)}\n")
    if scan:
        w(f"\nPDFs needing OCR: {', '.join(r['filename'] for r in scan)}\n")
    w(f"\n## Text-validity gate\n\n")
    w(f"- Passed: {len(rows) - n_bad}\n")
    w(f"- Failed (empty .txt dump; OCR with extract_pdf_fulltext --ocr): {len(gate_failed)}\n")
    w(f"- Raised: {n_err}\n")
    for r in rows:
        if r["text_gate"].startswith("fail"):
            w(f"  - {r['filename']}: {r['text_gate'][len('fail: '):]}\n")
    w(f"\n## Venue distribution\n\n")
    venue_counts = {}
    for r in rows:
        v = r["venue_hint"] or "(not detected)"
        venue_counts[v] = venue_counts.get(v, 0) + 1
    for v, c in sorted(venue_counts.items(), key=lambda x: -x[1]):
        w(f"- {v}: {c}\n")
    w(f"\n## Cleaning applied (pdf_text_clean.py)\n\n")
    tot_lig = sum(r.get("ligatures_fixed", 0) for r in rows)
    tot_hy  = sum(r.get("hyphens_rejoined", 0) for r in rows)
    tot_pg  = sum(r.get("page_nums_stripped", 0) for r in rows)
    n_lig_papers = sum(1 for r in rows if r.get("ligatures_fixed", 0) > 0)
    w(f"- Ligatures expanded: **{tot_lig}** across {n_lig_papers} papers\n")
    w(f"- Soft-hyphens rejoined: **{tot_hy}**\n")
    w(f"- Bare page-number lines stripped: **{tot_pg}**\n")
    if rows:
        worst_lig = sorted(rows, key=lambda r: -r.get("ligatures_fixed", 0))[:3]
        w(f"\nMost ligatures (top 3):\n")
        for r in worst_lig:
            if r.get("ligatures_fixed", 0):
                w(f"- {r['filename']}: {r['ligatures_fixed']}\n")
    w(f"\n## Artifacts\n\n")
    w(f"- Full-text corpus: `{text_dir}/` ({len(rows) - n_err} .txt files)\n")
    w(f"- Metadata CSV: `{csv_path}`\n")
    w(f"- Abstracts dump: `{ab_path}`\n")
    lit_util.atomic_write_text(rep_path, "".join(rep), newline=None)

    print(f"\nBuilt library:")
    print(f"  Metadata: {csv_path}")
    print(f"  Text corpus: {text_dir}/ ({len(rows) - n_err} files)")
    print(f"  Abstracts: {ab_path}")
    print(f"  Report: {rep_path}")
    print(f"  Text gate: {len(rows) - n_bad} passed, {len(gate_failed)} failed (needs OCR), "
          f"{n_err} raised")
    return res


def main(argv=None) -> int:
    ap = _Parser()
    ap.add_argument("--base-dir", default=os.getcwd())
    ap.add_argument("--lib-dir", default="references/literature",
                     help="PDF directory, relative to --base-dir")
    ap.add_argument("--out-dir", default="data/prior_art",
                     help="Output directory for text/, metadata.csv, etc., relative to --base-dir")
    args = ap.parse_args(argv)
    return run(base_dir=args.base_dir, lib_dir=args.lib_dir, out_dir=args.out_dir)["exit"]


if __name__ == "__main__":
    sys.exit(main())
