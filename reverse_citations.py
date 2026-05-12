"""Reverse-citation extractor (parses what each PDF cites).

For every PDF in a project's library, extract the References section and dump
each individual reference. Promoted from getpaid/tools/extract_references.py
and made portfolio-aware.

Source priority per PDF:
  1. <stem>.fulltext.json's `references` field (PMC papers — pre-structured)
  2. <data_dir>/text/<stem>.txt        (Tier 1 — pre-cleaned text dumps)
  3. PDF first-pass text extraction    (Tier 2 fallback — no dumps available)

Outputs (at <lib>/_reverse_citations*):
  _reverse_citations.jsonl       — one JSON object per (seed_pdf, raw_reference_text)
  _reverse_citations_parsed.csv  — best-effort (first_author, year, title_snippet, doi)
  _reverse_citations_unique.csv  — deduplicated DOIs (queue these for sweep.py)

Usage:
  # By project name
  python reverse_citations.py --project Physiological_Data
  python reverse_citations.py --project getpaid

  # Explicit
  python reverse_citations.py --lib-dir /path/to/library [--text-dir /path/to/text]
"""
import os, sys, io, re, csv, json, time, argparse
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

PROJECTS_ROOT = Path(os.path.expanduser("~/Projects"))
CONFIG_PATH   = Path(__file__).parent / "projects.json"

EMAIL = os.environ.get("LITPIPE_EMAIL", "jacob.bowie2@gmail.com")

# ---------- references-section locator ----------

REFS_HDR = re.compile(
    r'(?:^|[\s\.\)])\s*'
    r'(?:R\s*E\s*F\s*E\s*R\s*E\s*N\s*C\s*E\s*S|References?|REFERENCES?|'
    r'Bibliography|BIBLIOGRAPHY|Literature\s*Cited|Works\s+Cited)'
    r'\s*[\n\r]',
    re.IGNORECASE | re.MULTILINE
)

NUM_BRACKET = re.compile(r'\n\s*\[\s*\d{1,3}\s*\]\s*')
NUM_DOT     = re.compile(r'\n\s*\d{1,3}\.\s+')
AUTHOR_YEAR_HANG = re.compile(
    r'\n(?=[A-Z][a-z]+(?:,\s*[A-Z]\.?|,?\s+[A-Z]\.)(?:[^.\n]{0,200}?\(\d{4})\b)',
    re.MULTILINE
)
YEAR_RE = re.compile(r'\b(19[5-9]\d|20[0-2]\d)\b')
DOI_RE  = re.compile(r'\b10\.\d{4,9}/[-._;()/:A-Z0-9]+', re.IGNORECASE)


def locate_refs(text: str) -> str:
    matches = list(REFS_HDR.finditer(text))
    if not matches: return ""
    start = matches[-1].end()
    tail_cut = re.search(
        r'\n\s*(?:Appendix|APPENDIX|Supplementary|SUPPLEMENTARY|Acknowledg|'
        r'ACKNOWLEDG|Author contributions|Competing interests)\s',
        text[start:], re.IGNORECASE)
    end = start + tail_cut.start() if tail_cut else len(text)
    return text[start:end]


def split_refs(refs_text: str):
    chunks = NUM_BRACKET.split(refs_text)
    if len(chunks) > 5: return [c.strip() for c in chunks[1:] if len(c.strip()) > 30]
    chunks = NUM_DOT.split(refs_text)
    if len(chunks) > 5: return [c.strip() for c in chunks[1:] if len(c.strip()) > 30]
    chunks = AUTHOR_YEAR_HANG.split(refs_text)
    if len(chunks) > 5: return [c.strip() for c in chunks if len(c.strip()) > 30]
    chunks = re.split(r'\n\s*\n', refs_text)
    return [c.strip() for c in chunks if len(c.strip()) > 30]


def parse_one(raw: str) -> dict:
    raw_clean = re.sub(r'\s+', ' ', raw).strip()
    year = (m.group(1) if (m := YEAR_RE.search(raw_clean)) else "")
    doi  = (m.group(0).rstrip('.,;') if (m := DOI_RE.search(raw_clean)) else "")
    first_author = ""
    m = re.match(r'^([A-Z][A-Za-z\-\']+)(?:,\s*[A-Z]|\s+[A-Z]\.)', raw_clean)
    if m: first_author = m.group(1)
    elif (m := re.match(r'^([A-Z][A-Za-z\-\']+)', raw_clean)): first_author = m.group(1)
    title = ""
    if (ym := YEAR_RE.search(raw_clean)):
        after = re.sub(r'^[\.\)\,\s]+', '', raw_clean[ym.end():])
        title = (m.group(1) if (m := re.match(r'(.+?)\.\s+(?=[A-Z])', after))
                 else after[:150])
    return {"first_author": first_author, "year": year,
            "title_snippet": title[:200].strip(),
            "doi": doi.lower() if doi else "",
            "raw": raw_clean[:500]}


# ---------- source-priority text retrieval ----------

def text_from_sidecar_refs(sc_path: Path):
    """If JATS sidecar has structured references, return list of dicts directly."""
    if not sc_path.exists(): return None
    try:
        with open(sc_path, encoding="utf-8") as f:
            d = json.load(f)
        refs = d.get("references") or []
        if not refs: return None
        out = []
        for r in refs:
            if isinstance(r, dict):
                doi = (r.get("doi") or "").lower()
                out.append({
                    "first_author": r.get("first_author") or "",
                    "year":         str(r.get("year") or ""),
                    "title_snippet": (r.get("title") or "")[:200],
                    "doi":          doi,
                    "raw":          (r.get("text") or json.dumps(r))[:500],
                })
            else:
                out.append(parse_one(str(r)))
        return out
    except (OSError, ValueError): return None


def text_from_dump(text_dir: Path, stem: str):
    p = text_dir / (stem + ".txt") if text_dir else None
    if p and p.exists():
        try:
            with open(p, encoding="utf-8") as f:
                return f.read()
        except OSError: return None
    return None


def text_from_pdf(pdf_path: Path):
    try:
        import fitz
    except ImportError:
        return None
    try:
        doc = fitz.open(str(pdf_path))
        try:
            return "\n".join(p.get_text() for p in doc)
        finally:
            doc.close()
    except Exception:
        return None


# ---------- project resolution ----------

def resolve_project(name: str):
    with open(CONFIG_PATH, encoding="utf-8") as f:
        cfg = json.load(f).get("projects", {})
    if name not in cfg:
        print(f"[ERR] '{name}' not in projects.json", file=sys.stderr); sys.exit(2)
    p = cfg[name]
    base = PROJECTS_ROOT / (p.get("parent") or name)
    lib = base / p["lib_dir"]
    text_dir = (base / p["data_dir"] / "text") if p.get("data_dir") else None
    return lib, text_dir


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    ap.add_argument("--project",  default=None,
                     help="Project name from projects.json (e.g. 'Physiological_Data').")
    ap.add_argument("--lib-dir",  default=None,
                     help="Explicit library path (legacy).")
    ap.add_argument("--text-dir", default=None,
                     help="Tier 1 text-dump dir (default: <data_dir>/text from projects.json).")
    ap.add_argument("--out-prefix", default=None,
                     help="Output file prefix (default: <lib>/_reverse_citations).")
    ap.add_argument("--limit", type=int, default=0,
                     help="Process first N PDFs only (testing).")
    args = ap.parse_args()

    if args.project:
        lib, text_dir = resolve_project(args.project)
    elif args.lib_dir:
        lib = Path(args.lib_dir)
        text_dir = Path(args.text_dir) if args.text_dir else None
    else:
        print("[ERR] Pass --project or --lib-dir", file=sys.stderr); sys.exit(2)

    if not lib.is_dir():
        print(f"[ERR] not a directory: {lib}", file=sys.stderr); sys.exit(2)

    pdfs = sorted(lib.glob("*.pdf"))
    if args.limit: pdfs = pdfs[:args.limit]

    out_prefix = Path(args.out_prefix) if args.out_prefix else (lib / "_reverse_citations")
    print(f"library:   {lib}")
    print(f"text-dir:  {text_dir}  (used if no JATS refs)")
    print(f"PDFs:      {len(pdfs)}")
    print(f"outputs:   {out_prefix}.{{jsonl, _parsed.csv, _unique.csv}}\n")

    raw_records = []
    parsed_records = []
    sources_used = {"sidecar": 0, "text_dump": 0, "pdf": 0, "no_refs": 0}
    total_refs = 0

    for i, pdf in enumerate(pdfs, 1):
        stem = pdf.stem
        sc_path = pdf.with_suffix(".fulltext.json")

        # Source 1: JATS sidecar's structured references (cleanest)
        sidecar_refs = text_from_sidecar_refs(sc_path)
        if sidecar_refs:
            sources_used["sidecar"] += 1
            for j, r in enumerate(sidecar_refs):
                r["seed"] = pdf.name
                parsed_records.append(r)
                raw_records.append({"seed": pdf.name, "raw": r.get("raw","")[:1000]})
            total_refs += len(sidecar_refs)
            print(f"  [{i:>3}/{len(pdfs)}] {pdf.name[:55]:<55} sidecar  {len(sidecar_refs):>3} refs")
            continue

        # Source 2: text dump (Tier 1)
        text = text_from_dump(text_dir, stem) if text_dir else None
        source_label = "text"
        if not text:
            # Source 3: PDF parse (Tier 2 fallback)
            text = text_from_pdf(pdf)
            source_label = "pdf"
        if not text:
            sources_used["no_refs"] += 1
            print(f"  [{i:>3}/{len(pdfs)}] {pdf.name[:55]:<55} NO_TEXT")
            continue

        refs_blob = locate_refs(text)
        if not refs_blob:
            sources_used["no_refs"] += 1
            print(f"  [{i:>3}/{len(pdfs)}] {pdf.name[:55]:<55} NO_REFS_SECTION")
            continue

        chunks = split_refs(refs_blob)
        sources_used["text_dump" if source_label == "text" else "pdf"] += 1
        total_refs += len(chunks)
        print(f"  [{i:>3}/{len(pdfs)}] {pdf.name[:55]:<55} {source_label:<7} {len(chunks):>3} refs")
        for c in chunks:
            raw_records.append({"seed": pdf.name, "raw": c[:1000]})
            parsed = parse_one(c)
            parsed["seed"] = pdf.name
            parsed_records.append(parsed)

    # Write outputs
    out_prefix.parent.mkdir(parents=True, exist_ok=True)
    jsonl = out_prefix.with_suffix(".jsonl")
    with open(jsonl, "w", encoding="utf-8") as f:
        for r in raw_records: f.write(json.dumps(r) + "\n")

    parsed_csv = out_prefix.parent / (out_prefix.name + "_parsed.csv")
    with open(parsed_csv, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["seed","first_author","year","title_snippet","doi","raw"])
        w.writeheader(); w.writerows(parsed_records)

    dois = sorted({r["doi"] for r in parsed_records if r.get("doi")})
    uniq_csv = out_prefix.parent / (out_prefix.name + "_unique.csv")
    with open(uniq_csv, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f); w.writerow(["doi"])
        for d in dois: w.writerow([d])

    print(f"\n=== summary ===")
    print(f"  PDFs:                  {len(pdfs)}")
    for k, v in sources_used.items(): print(f"  {k:<22} {v}")
    print(f"  total refs:            {total_refs}")
    print(f"  unique DOIs:           {len(dois)}")
    print(f"  raw:                   {jsonl}")
    print(f"  parsed:                {parsed_csv}")
    print(f"  unique DOIs:           {uniq_csv}")


if __name__ == "__main__":
    main()
