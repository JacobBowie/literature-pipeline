"""Parse Europe PMC JATS XML into a structured sidecar dict.

Sidecar shape (written as JSON):
{
  "pmcid": "PMC4977162",
  "pmid": "27583291",
  "doi": "10.4161/temp.29752",
  "title": "...",
  "year": "2014",
  "authors": ["Ketko I", ...],
  "abstract": "Plain text of abstract",
  "sections": [{"title": "Introduction", "text": "..."}, ...],
  "figures":  [{"label": "Fig 1", "caption": "..."}, ...],
  "tables":   [{"label": "Table 1", "caption": "...", "text": "..."}, ...],
  "formulas": [{"label": "(1)", "latex": "\\frac{T_rec}{HR}"}, ...],
  "n_formulas": 3,
  "text": "Concatenated plain text (abstract + sections + LaTeX formulas)."
}

Limitations:
- Math: MathML is converted to LaTeX via mathml-to-latex when available; falls back to
  "[FORMULA]" placeholders if the lib isn't installed or conversion fails. LaTeX is
  embedded inline in section text as $...$ (display) or $...$ (inline).
- Tables are flattened to space-joined cell text. Structure is lost.
- Inline citations and cross-refs are stripped.
"""
import os, re, sys
from xml.etree import ElementTree as ET

# Prefer the vendored copy (see vendor/VENDORED.md) for reproducibility.
# Falls back to the pip-installed package if vendor/ isn't found.
# Search both layouts: ./vendor/ (elevated _tools location) and ../vendor/ (legacy getpaid layout).
_HERE = os.path.dirname(os.path.abspath(__file__))
_VENDOR_CANDIDATES = [
    os.path.normpath(os.path.join(_HERE, "vendor")),
    os.path.normpath(os.path.join(_HERE, "..", "vendor")),
]
_VENDOR_DIR = next((p for p in _VENDOR_CANDIDATES if os.path.isdir(p)), None)
if _VENDOR_DIR and _VENDOR_DIR not in sys.path:
    sys.path.insert(0, _VENDOR_DIR)
try:
    from mathml_to_latex.converter import MathMLToLaTeX
    _MML_CONV = MathMLToLaTeX()
    _MATH_AVAILABLE = True
    _MML_SOURCE = "vendor" if _VENDOR_DIR else "pip"
except ImportError:
    _MML_CONV = None
    _MATH_AVAILABLE = False
    _MML_SOURCE = "missing"


def _itertext(elem, skip_tags=()):
    """Yield text from elem and its descendants, skipping subtrees rooted at skip_tags."""
    if elem.tag in skip_tags:
        return
    if elem.text:
        yield elem.text
    for child in elem:
        if child.tag in skip_tags:
            if child.tail:
                yield child.tail
            continue
        yield from _itertext(child, skip_tags)
        if child.tail:
            yield child.tail


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


# JATS uses the MathML namespace. Element tags arrive as Clark-notation strings
# like "{http://www.w3.org/1998/Math/MathML}math".
_MML_NS = "http://www.w3.org/1998/Math/MathML"


def _formula_latex(formula_elem):
    """Convert a JATS <disp-formula> or <inline-formula> to a LaTeX string.

    Returns (latex, status, mathml_source) where:
      status: "ok" | "no-mathml" | "no-converter" | "conv-error:<msg>" | "empty-output"
      mathml_source: the MathML XML we attempted to convert (empty when no <math> found),
                     useful for debugging which formula failed.
    """
    if not _MATH_AVAILABLE:
        return "", "no-converter", ""

    math_el = None
    # Accept either a JATS formula wrapper (containing <math> as a descendant)
    # or a bare <math> element passed directly.
    if formula_elem.tag in (f"{{{_MML_NS}}}math", "math"):
        math_el = formula_elem
    else:
        for tag in (f"{{{_MML_NS}}}math", "math"):
            math_el = formula_elem.find(f".//{tag}")
            if math_el is not None: break
    if math_el is None:
        return "", "no-mathml", ""

    # Strip namespace prefixes; mathml-to-latex prefers default namespace.
    def strip_ns(elem):
        if isinstance(elem.tag, str) and "}" in elem.tag:
            elem.tag = elem.tag.split("}", 1)[1]
        for child in elem:
            strip_ns(child)
    import copy
    math_clean = copy.deepcopy(math_el)
    strip_ns(math_clean)
    math_clean.set("xmlns", _MML_NS)
    xml_str = ET.tostring(math_clean, encoding="unicode")

    # Pre-flight: detect mtable (matrix) input. Audited 2026-04-28: the upstream
    # converter emits `\left(\right. ... \left.\right)` without a `\begin{matrix}`
    # wrapper, producing pdflatex "Misplaced alignment tab" errors. Flag rather
    # than silently emit broken output.
    has_mtable = math_clean.find(".//mtable") is not None

    try:
        latex = _MML_CONV.convert(xml_str)
        if not latex or not latex.strip():
            return "", "empty-output", xml_str
        # Strip U+2061 FUNCTION APPLICATION (audit 2026-04-28: pdflatex chokes on
        # this; emitted by the converter for <mi>log</mi>, <mi>sin</mi>, etc.).
        # We just remove it; downstream consumers can regex-detect "log" / "sin"
        # / "cos" identifiers and prefix \ if they want proper LaTeX operators.
        latex = latex.replace("⁡", "")
        # Collapse "T r e c" / "H R" runs of single-char tokens.
        def _join_braced(m):
            inner = m.group(1)
            if re.fullmatch(r"(?:[A-Za-z]\s)+[A-Za-z]", inner):
                return "{" + inner.replace(" ", "") + "}"
            return m.group(0)
        latex = re.sub(r"\{([^{}]+)\}", _join_braced, latex)
        latex = re.sub(r"\b((?:[A-Za-z]\s){2,}[A-Za-z])\b",
                       lambda m: m.group(0).replace(" ", ""), latex)
        if has_mtable:
            return latex.strip(), "ok-but-matrix-likely-broken", xml_str
        return latex.strip(), "ok", xml_str
    except Exception as e:
        return "", f"conv-error:{type(e).__name__}:{str(e)[:80]}", xml_str


def _section_text(sec):
    """Concatenate <p> text inside a section, embedding LaTeX for formulas."""
    parts = []
    for child in sec:
        tag = child.tag
        if tag == "title":
            continue  # handled by caller
        if tag == "sec":
            sub_title = child.findtext("title", default="").strip()
            sub_text = _section_text(child)
            if sub_title or sub_text:
                parts.append(f"\n## {sub_title}\n{sub_text}")
        elif tag == "p":
            # Walk children and assemble; convert formulas inline as $...$.
            buf = []
            if child.text: buf.append(child.text)
            for c in child:
                if c.tag in ("xref",):
                    pass  # drop cross-refs
                elif c.tag == "inline-formula":
                    latex, _, _ = _formula_latex(c)
                    buf.append(f"${latex}$" if latex else "[FORMULA]")
                elif c.tag == "disp-formula":
                    latex, _, _ = _formula_latex(c)
                    buf.append(f"$${latex}$$" if latex else "[FORMULA]")
                elif c.tag in ("fig", "table-wrap"):
                    pass  # collected separately at top level
                else:
                    buf.append(" ".join(_itertext(c, skip_tags=("xref",))))
                if c.tail: buf.append(c.tail)
            parts.append(_clean(" ".join(buf)))
        elif tag == "disp-formula":
            latex, _, _ = _formula_latex(child)
            parts.append(f"$${latex}$$" if latex else "[FORMULA]")
        elif tag in ("fig", "table-wrap"):
            continue
        elif tag == "list":
            for li in child.iter("list-item"):
                bullet = " ".join(_itertext(li, skip_tags=("xref",)))
                parts.append(f"  - {_clean(bullet)}")
    return "\n\n".join(p for p in parts if p)


def parse_jats(xml_bytes: bytes) -> dict:
    root = ET.fromstring(xml_bytes)
    front_el = root.find("front")
    front = front_el if front_el is not None else root
    am_el = front.find(".//article-meta")
    article_meta = am_el if am_el is not None else front

    def aid(t):
        for x in article_meta.findall("article-id"):
            if x.get("pub-id-type") == t:
                return (x.text or "").strip()
        return ""

    pmcid = aid("pmcid"); pmid = aid("pmid"); doi = aid("doi")
    if pmcid and not pmcid.startswith("PMC"):
        pmcid = "PMC" + pmcid

    title = _clean(" ".join(_itertext(article_meta.find(".//article-title"), skip_tags=("xref",))) \
                    if article_meta.find(".//article-title") is not None else "")
    subtitle_el = article_meta.find(".//subtitle")
    subtitle = _clean(subtitle_el.text if subtitle_el is not None else "")

    journal_el = front.find(".//journal-title")
    journal = _clean(journal_el.text if journal_el is not None else "")

    year_el = article_meta.find(".//pub-date/year")
    year = (year_el.text or "").strip() if year_el is not None else ""

    authors = []
    for c in article_meta.findall(".//contrib[@contrib-type='author']") or article_meta.findall(".//contrib"):
        sur = c.findtext(".//surname", default="").strip()
        giv = c.findtext(".//given-names", default="").strip()
        if sur:
            authors.append(f"{sur} {giv}".strip())

    # Abstract
    abstract_parts = []
    abs_el = article_meta.find("abstract")
    if abs_el is not None:
        for p in abs_el.iter("p"):
            abstract_parts.append(_clean(" ".join(_itertext(p, skip_tags=("xref",)))))
    abstract = "\n\n".join(p for p in abstract_parts if p)

    # Body sections
    body = root.find("body")
    sections = []
    if body is not None:
        for sec in body.findall("sec"):
            sec_title = sec.findtext("title", default="").strip()
            if sec_title.lower() == "references":
                continue
            sec_text = _section_text(sec)
            sections.append({"title": sec_title, "text": sec_text})

    scope = body if body is not None else root

    # Figures: label, caption, and graphic href (the bare filename JATS gives us;
    # full CDN URL gets resolved later by tools/fetch_figures.py).
    figures = []
    for fig in scope.iter("fig"):
        label = fig.findtext("label", default="").strip()
        caption_el = fig.find("caption")
        cap_text = ""
        if caption_el is not None:
            cap_text = _clean(" ".join(_itertext(caption_el, skip_tags=("xref",))))
        # JATS uses the xlink namespace for href on <graphic>; ElementTree exposes
        # it as Clark-notation. Walk children, prefer first non-empty href.
        graphic_href = ""
        for g in fig.iter():
            tag = g.tag.split("}", 1)[-1] if "}" in g.tag else g.tag
            if tag != "graphic": continue
            for k, v in g.attrib.items():
                key = k.split("}", 1)[-1] if "}" in k else k
                if key == "href" and v:
                    graphic_href = v
                    break
            if graphic_href: break
        figures.append({"label": label, "caption": cap_text,
                         "graphic_href": graphic_href,
                         "image_path": "", "image_url": ""})

    # Tables (caption + flattened cell text)
    tables = []
    for tw in scope.iter("table-wrap"):
        label = tw.findtext("label", default="").strip()
        caption_el = tw.find("caption")
        cap_text = ""
        if caption_el is not None:
            cap_text = _clean(" ".join(_itertext(caption_el, skip_tags=("xref",))))
        # flatten table cells
        cell_text = " | ".join(
            _clean(" ".join(_itertext(td, skip_tags=("xref",))))
            for td in tw.iter("td")
        )
        if not cell_text:
            cell_text = " | ".join(
                _clean(" ".join(_itertext(th, skip_tags=("xref",))))
                for th in tw.iter("th")
            )
        tables.append({"label": label, "caption": cap_text, "text": cell_text})

    # Collect all formulas as a top-level list (in addition to embedding them
    # inline in section text). Useful for parameter-inventory work that wants
    # to enumerate equations without parsing the markdown.
    #
    # When conversion fails, the offending MathML is captured in `mathml_input`
    # so the failure can be diagnosed without re-fetching the JATS XML.
    # When conversion succeeds, mathml_input is omitted to keep sidecars small.
    formulas = []
    formula_failures = {}
    def _record(elem, kind):
        latex, status, mml_src = _formula_latex(elem)
        label = elem.findtext("label", default="").strip() if kind == "display" else ""
        rec = {"kind": kind, "label": label, "latex": latex, "status": status}
        # Capture offending MathML for any non-clean status (failures and warnings)
        # so the converter's mistakes can be diagnosed without re-fetching JATS.
        if status != "ok":
            rec["mathml_input"] = mml_src
            key = ("conv-error" if status.startswith("conv-error")
                   else status)
            formula_failures[key] = formula_failures.get(key, 0) + 1
        formulas.append(rec)
    for f in scope.iter("disp-formula"):
        _record(f, "display")
    for f in scope.iter("inline-formula"):
        _record(f, "inline")
    n_formulas = len(formulas)

    # Concatenated plain text for grep/pypdf comparison
    text_parts = []
    if title:    text_parts.append(f"# {title}")
    if subtitle: text_parts.append(subtitle)
    if abstract: text_parts.append(f"\n## Abstract\n\n{abstract}")
    for s in sections:
        text_parts.append(f"\n## {s['title']}\n\n{s['text']}")
    for fig in figures:
        if fig["caption"]: text_parts.append(f"\n[{fig['label']}] {fig['caption']}")
    for tab in tables:
        if tab["caption"] or tab["text"]:
            text_parts.append(f"\n[{tab['label']}] {tab['caption']}\n{tab['text']}")
    text = "\n".join(text_parts).strip()

    return {
        "pmcid": pmcid, "pmid": pmid, "doi": doi,
        "title": title, "subtitle": subtitle, "year": year, "journal": journal,
        "authors": authors,
        "abstract": abstract,
        "sections": sections,
        "figures": figures,
        "tables": tables,
        "formulas": formulas,
        "n_formulas": n_formulas,
        "formula_failures": {k: v for k, v in formula_failures.items() if v > 0},
        "text": text,
    }


def _smoke_test(pmcid: str, dump: bool) -> int:
    """Fetch a JATS-XML article from Europe PMC and dump the parsed structure."""
    import requests
    UA = f"litpipe-jats-smoke/1.0 (mailto:{os.environ.get('LITPIPE_EMAIL', 'jacob.bowie2@gmail.com')})"
    r = requests.get(f"https://www.ebi.ac.uk/europepmc/webservices/rest/{pmcid}/fullTextXML",
                      headers={"User-Agent": UA}, timeout=30)
    print(f"HTTP {r.status_code} ({len(r.content)} bytes)")
    if r.status_code != 200:
        return 1
    out = parse_jats(r.content)
    print(f"Title:    {out['title'][:80]}")
    print(f"DOI:      {out['doi']}")
    print(f"Authors:  {len(out['authors'])} ({', '.join(out['authors'][:3])}...)")
    print(f"Sections: {len(out['sections'])} ({', '.join(s['title'] for s in out['sections'][:5])}...)")
    print(f"Figures:  {len(out['figures'])}")
    print(f"Tables:   {len(out['tables'])}")
    print(f"Formulas: {out['n_formulas']}  failures: {out['formula_failures']}")
    if out['formulas'][:3]:
        print("Sample formulas:")
        for f in out['formulas'][:3]:
            print(f"  [{f['kind']}] {f['label']:<6} {f['latex'][:80]}")
    print(f"Text len: {len(out['text'])} chars")
    if dump:
        print("\n--- TEXT ---\n")
        print(out["text"])
    return 0


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(
        description="Parse Europe PMC JATS XML into a structured sidecar dict (library + dev smoke-test).")
    ap.add_argument("--smoke-test", metavar="PMCID", default=None,
                    help="Fetch this PMCID from Europe PMC and dump the parsed structure. "
                         "Example: --smoke-test PMC4977162.")
    ap.add_argument("--dump", action="store_true",
                    help="With --smoke-test, also dump the full plain-text body.")
    args = ap.parse_args()
    if args.smoke_test:
        sys.exit(_smoke_test(args.smoke_test, args.dump))
    print("jats_to_text is a library module; import parse_jats from it, or run with --smoke-test PMCID for a dev probe.",
          file=sys.stderr)
    sys.exit(0)
