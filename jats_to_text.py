"""Parse JATS XML (Europe PMC fullTextXML, the PMC Cloud Service xml_url) or BioC JSON into a
structured sidecar dict, and fetch both through litpipe.net.

Sidecar shape (written as JSON):
{
  "pmcid": "PMC4977162",
  "pmid": "27583291",
  "doi": "10.4161/temp.29752",
  "title": "...",
  "year": "2014",
  "journal": "...", "volume": "6", "issue": "2", "pages": "123-130",
  "authors": ["Ketko I", ...],                       # authors only, never editors
  "abstract": "Plain text of abstract",
  "sections": [{"title": "Introduction", "text": "..."}, ...],
  "figures":  [{"label": "Fig 1", "caption": "...", "graphic_href": "x-g001.jpg",
                "image_path": "", "image_url": ""}, ...],   # + "graphic_hrefs" for multi-panel
  "tables":   [{"label": "Table 1", "caption": "...", "text": "..."}, ...],
  "formulas": [{"label": "(1)", "latex": "\\frac{T_rec}{HR}"}, ...],
  "n_formulas": 3,
  "text": "Concatenated plain text (abstract + sections + LaTeX formulas)."
}

Entry points: parse_jats(xml_bytes), parse_bioc(bioc), fetch_jats_xml(pmcid) and fetch_bioc(pmcid)
((payload | None, status), never raising for a network failure), their Outcome forms
fetch_jats_outcome / fetch_bioc_outcome, and fetch_fulltext(pmcid) (JATS, then BioC for an
article Europe PMC does not hold).

Limitations:
- Math: MathML is converted to LaTeX via mathml-to-latex when available; falls back to
  "[FORMULA]" placeholders if the lib isn't installed or conversion fails. LaTeX is
  embedded inline in section text as $...$ (display) or $...$ (inline). BioC has no MathML.
- Tables are flattened to space-joined cell text. Structure is lost.
- Inline citations and cross-refs are stripped.
"""
import json, os, re, sys
from xml.etree import ElementTree as ET

from litpipe import hosts, net
from litpipe.outcomes import Kind, Outcome

# Prefer the vendored copy (see vendor/VENDORED.md) for reproducibility.
# Falls back to the pip-installed package if vendor/ isn't found.
# Search both layouts: ./vendor/ (elevated _tools location) and ../vendor/ (legacy nested-tools layout).
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
except ImportError:
    _MML_CONV = None
    _MATH_AVAILABLE = False


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


_NOT_AUTHOR_GROUP = ("editor", "reviewer", "translator")


def _author_contribs(article_meta):
    """The <contrib> elements that are authors, in document order.

    `contrib-type="author"` (case-insensitive) when any contrib carries it; otherwise only contribs
    with NO contrib-type, never an editor, translator or reviewer (the old fallback took every
    contrib, so an article whose authors were untyped listed its editors as authors). A contrib in a
    contrib-group whose content-type names editors or reviewers is not an author either."""
    rows = []
    seen = set()
    for cg in article_meta.iter("contrib-group"):
        gtype = (cg.get("content-type") or "").strip().lower()
        for c in cg.findall("contrib"):
            seen.add(id(c))
            rows.append((c, (c.get("contrib-type") or "").strip().lower(), gtype))
    for c in article_meta.iter("contrib"):          # a contrib outside any contrib-group (lenient)
        if id(c) not in seen:
            rows.append((c, (c.get("contrib-type") or "").strip().lower(), ""))
    typed = [c for c, ctype, _ in rows if ctype == "author"]
    if typed:
        return typed
    return [c for c, ctype, gtype in rows
            if not ctype and not any(w in gtype for w in _NOT_AUTHOR_GROUP)]


def _float_scopes(root):
    """Where figures and tables live: <body>, <back> (appendices) and <floats-group>. PMC's JATS
    puts most floats of many articles in <floats-group>, outside <body> (V1-N3, settled offline on
    the 2026-09-25 efetch record of an OA article: 2 of its 8 figures sat in <body>, 6 in
    <floats-group>). Falls back to the whole document when none of the three exists."""
    scopes = [e for e in (root.find("body"), root.find("back"), root.find("floats-group")) if e is not None]
    return scopes or [root]


def _local(tag):
    return tag.split("}", 1)[-1] if isinstance(tag, str) else ""


def _graphic_hrefs(fig):
    """Every <graphic> xlink:href in the figure, in document order, without duplicates: a
    multi-panel figure has one <graphic> per panel (the old parser kept only the first)."""
    out = []
    for g in fig.iter():
        if _local(g.tag) != "graphic":
            continue
        for k, v in g.attrib.items():
            if _local(k) == "href" and v and v not in out:
                out.append(v)
                break
    return out


def _label_order(items):
    """Figures/tables from <body> come before those in <floats-group>, so Figure 1 can follow
    Figure 3 in document order. When every label carries a distinct number, order by it (so the
    Nth entry, and fetch_figures' `.figN` file, is Figure N); otherwise keep document order."""
    nums = []
    for it in items:
        m = re.search(r"\d+", it.get("label") or "")
        if not m:
            return items
        nums.append(int(m.group()))
    if len(set(nums)) != len(nums):
        return items
    return [it for _, it in sorted(zip(nums, items), key=lambda t: t[0])]


def _pages(fpage, lpage, elocation):
    if fpage and lpage and lpage != fpage:
        return f"{fpage}-{lpage}"
    return fpage or elocation or ""


def _assemble_text(title, subtitle, abstract, sections, figures, tables):
    """Concatenated plain text for grep/pypdf comparison (shared by the JATS and BioC parsers)."""
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
    return "\n".join(text_parts).strip()


def parse_jats(xml_bytes: bytes) -> dict:
    """JATS XML (Europe PMC fullTextXML, the PMC Cloud Service xml_url, or an efetch record) to the
    sidecar dict described in the module docstring."""
    root = ET.fromstring(xml_bytes)
    if root.find("front") is None and root.find("article") is not None:   # a <pmc-articleset> wrapper
        root = root.find("article")
    front_el = root.find("front")
    front = front_el if front_el is not None else root
    am_el = front.find(".//article-meta")
    article_meta = am_el if am_el is not None else front

    def aid(t):
        for x in article_meta.findall("article-id"):
            if x.get("pub-id-type") == t:
                return (x.text or "").strip()
        return ""

    pmcid = aid("pmcid") or aid("pmc"); pmid = aid("pmid"); doi = aid("doi")
    if pmcid and not pmcid.upper().startswith("PMC"):
        pmcid = "PMC" + pmcid

    title = _clean(" ".join(_itertext(article_meta.find(".//article-title"), skip_tags=("xref",))) \
                    if article_meta.find(".//article-title") is not None else "")
    subtitle_el = article_meta.find(".//subtitle")
    subtitle = _clean(subtitle_el.text if subtitle_el is not None else "")

    journal_el = front.find(".//journal-title")
    journal = _clean(journal_el.text if journal_el is not None else "")

    year_el = article_meta.find(".//pub-date/year")
    year = (year_el.text or "").strip() if year_el is not None else ""

    volume = _clean(article_meta.findtext("volume", default=""))
    issue = _clean(article_meta.findtext("issue", default=""))
    pages = _pages(_clean(article_meta.findtext("fpage", default="")),
                   _clean(article_meta.findtext("lpage", default="")),
                   _clean(article_meta.findtext("elocation-id", default="")))

    authors = []
    for c in _author_contribs(article_meta):
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
        # Paragraphs placed straight in <body> (a comment, a letter, an editorial, or an article's
        # untitled opening) form an untitled section where they stand; reading only <sec> dropped
        # every one of them. ElementTree has no parent links, so the container leaves <body> intact.
        loose = ET.Element("sec")

        def flush():
            text = _section_text(loose)
            if text:
                sections.append({"title": "", "text": text})
            loose.clear()
        for child in body:
            if child.tag != "sec":
                loose.append(child)
                continue
            flush()
            sec_title = child.findtext("title", default="").strip()
            if sec_title.lower() == "references":
                continue
            sec_text = _section_text(child)
            sections.append({"title": sec_title, "text": sec_text})
        flush()

    scope = body if body is not None else root
    floats = _float_scopes(root)

    # Figures: label, caption, and graphic hrefs (the bare filenames JATS gives; fetch_figures
    # matches them to the PMC Cloud Service media list). graphic_href stays the first panel.
    figures = []
    for fig in (f for sc in floats for f in sc.iter("fig")):
        label = fig.findtext("label", default="").strip()
        caption_el = fig.find("caption")
        cap_text = ""
        if caption_el is not None:
            cap_text = _clean(" ".join(_itertext(caption_el, skip_tags=("xref",))))
        hrefs = _graphic_hrefs(fig)
        rec = {"label": label, "caption": cap_text,
               "graphic_href": hrefs[0] if hrefs else "",
               "image_path": "", "image_url": ""}
        if len(hrefs) > 1:
            rec["graphic_hrefs"] = hrefs
        figures.append(rec)

    # Tables (caption + flattened cell text)
    tables = []
    for tw in (t for sc in floats for t in sc.iter("table-wrap")):
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
    figures, tables = _label_order(figures), _label_order(tables)

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

    text = _assemble_text(title, subtitle, abstract, sections, figures, tables)

    return {
        "pmcid": pmcid, "pmid": pmid, "doi": doi,
        "title": title, "subtitle": subtitle, "year": year, "journal": journal,
        "volume": volume, "issue": issue, "pages": pages,
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



# ------------------------------------------------------------------------------ BioC JSON
# BioC API for PMC Open Access (https://www.ncbi.nlm.nih.gov/research/bionlp/APIs/BioC-PMC/, read
# 2026-09-30): "Articles available from this service are in the PMC Open Access Subset and the PMC
# Author Manuscript Collection." Template ".../RESTful/pmcoa.cgi/BioC_[format]/[ID]/[encoding]",
# format xml or json, encoding unicode or ascii ("no Unicode to ASCII translation is perfect"), so
# we ask for unicode: sidecars are UTF-8 JSON. A PMCID outside both collections is a 200 text/html
# page "[Error] : No result can be found." (V1 P1d, re-probed 2026-09-30), not an HTTP error.
BIOC_JSON = "https://www.ncbi.nlm.nih.gov/research/bionlp/RESTful/pmcoa.cgi/BioC_json/{pmcid}/{encoding}"

# BioC-PMC section_type values that are not body text (the JATS parser reads <body> <sec> only).
_BIOC_NOT_BODY = frozenset({"TITLE", "ABSTRACT", "FIG", "TABLE", "REF", "ACK_FUND", "AUTH_CONT",
                            "COMP_INT", "ABBR", "SUPPL", "REVIEW_INFO", "KEYWORD", "APPENDIX"})


def _bioc_document(bioc):
    """The first BioC document in a collection list, a collection or a document (dict or JSON)."""
    if isinstance(bioc, (bytes, bytearray)):
        bioc = bioc.decode("utf-8", "replace")
    if isinstance(bioc, str):
        s = bioc.lstrip()
        if not s.startswith(("[", "{")):
            raise ValueError(f"not a BioC JSON document: {s[:60]!r}")
        bioc = json.loads(s)
    if isinstance(bioc, list):
        if not bioc:
            raise ValueError("empty BioC collection list")
        bioc = bioc[0]
    if not isinstance(bioc, dict):
        raise ValueError(f"not a BioC document: {type(bioc).__name__}")
    if "passages" in bioc:
        return bioc
    docs = bioc.get("documents") or []
    if not docs:
        raise ValueError("BioC collection has no documents")
    return docs[0]


def _bioc_name(value):
    """BioC-PMC author infon "surname:Spencer;given-names:Matthew D." as "Spencer Matthew D."."""
    parts = {}
    for piece in (value or "").split(";"):
        k, _, v = piece.partition(":")
        parts[k.strip().lower()] = v.strip()
    sur = parts.get("surname", "")
    return f"{sur} {parts.get('given-names', '')}".strip() if sur else ""


def _bioc_table_text(s):
    return " | ".join(c.strip() for c in (s or "").split("\t") if c.strip())


def _bioc_label(kind, ident):
    """BioC has no figure/table labels: "F1" / "entropy-26-00970-f002" -> "Figure 1" / "Figure 2"."""
    m = re.search(r"(\d+)$", ident or "")
    return f"{kind} {int(m.group(1))}" if m else (ident or "")


def parse_bioc(bioc, *, authors=None) -> dict:
    """A BioC JSON document (bytes, str, the parsed list/dict) to the same sidecar shape as
    parse_jats. For author manuscripts, where no JATS comes from Europe PMC; the caller sets
    `has_pdf: false` for a text-only sidecar. BioC carries no MathML (formulas are empty), no
    journal title and no figure labels (derived from the figure id: "Figure 2").

    Authors: BioC's name_N infons carry no role, and they include editors (probe 2026-09-30: an OA
    article with 1 author and 3 editors lists all 4). An author manuscript's list (document licence
    "author_manuscript") has been authors only in every probe; any other document's list is kept
    with `authors_may_include_editors: true` unless the caller passes `authors=` (for example
    from the article's JATS or PubMed record). A multi-panel figure's BioC `file` is its LAST panel."""
    doc = _bioc_document(bioc)
    passages = doc.get("passages") or []
    front = next((p for p in passages if (p.get("infons") or {}).get("type") == "front"),
                 passages[0] if passages else {})
    fi = front.get("infons") or {}

    pmcid = str(fi.get("article-id_pmc") or doc.get("id") or "").strip()
    if pmcid and not pmcid.upper().startswith("PMC"):
        pmcid = "PMC" + pmcid
    names = sorted((k for k in fi if re.fullmatch(r"name_\d+", k)), key=lambda k: int(k[5:]))
    is_am = str((doc.get("infons") or {}).get("license") or "").strip().lower() == "author_manuscript"
    unverified = authors is None and not is_am and bool(names)
    if authors is None:
        authors = [a for a in (_bioc_name(fi[k]) for k in names) if a]

    abstract_parts, sections = [], []
    figs, tabs = {}, {}
    cur = None
    cur_type = None
    for p in passages:
        inf = p.get("infons") or {}
        st = (inf.get("section_type") or "").upper()
        ty = (inf.get("type") or "").lower()
        text = _clean(p.get("text") or "")
        if st == "ABSTRACT":
            if text and not ty.startswith(("title", "abstract_title")):
                abstract_parts.append(text)
        elif st == "FIG":
            rec = figs.setdefault(inf.get("id") or f"fig{len(figs) + 1}",
                                  {"label": _bioc_label("Figure", inf.get("id")), "caption": "",
                                   "graphic_href": inf.get("file") or "", "image_path": "", "image_url": ""})
            if text and "caption" in ty:
                rec["caption"] = f"{rec['caption']} {text}".strip()
        elif st == "TABLE":
            rec = tabs.setdefault(inf.get("id") or f"table{len(tabs) + 1}",
                                  {"label": _bioc_label("Table", inf.get("id")), "caption": "", "text": ""})
            if ty == "table":
                rec["text"] = _bioc_table_text(p.get("text") or "")
            elif "caption" in ty and text:
                rec["caption"] = f"{rec['caption']} {text}".strip()
        elif st and st not in _BIOC_NOT_BODY:
            if ty == "title_1" or cur is None or (st != cur_type and not ty.startswith("title")):
                cur = {"title": text if ty == "title_1" else "", "parts": []}
                sections.append(cur)
                cur_type = st
                if ty == "title_1":
                    continue
            if not text or ty == "footnote":
                continue
            if ty.startswith("title"):
                cur["parts"].append(f"\n## {text}")
            else:
                cur["parts"].append(text)
    sections = [{"title": s["title"], "text": "\n\n".join(s["parts"]).strip()} for s in sections]
    abstract = "\n\n".join(abstract_parts)
    title = _clean(front.get("text") or "") if fi.get("type") == "front" else ""
    figures, tables = _label_order(list(figs.values())), _label_order(list(tabs.values()))
    out = {
        "pmcid": pmcid,
        "pmid": str(fi.get("article-id_pmid") or "").strip(),
        "doi": str(fi.get("article-id_doi") or "").strip(),
        "title": title, "subtitle": "", "year": str(fi.get("year") or "").strip(), "journal": "",
        "volume": str(fi.get("volume") or "").strip(), "issue": str(fi.get("issue") or "").strip(),
        "pages": _pages(str(fi.get("fpage") or "").strip(), str(fi.get("lpage") or "").strip(),
                        str(fi.get("elocation-id") or "").strip()),
        "authors": list(authors),
        "abstract": abstract,
        "sections": sections,
        "figures": figures,
        "tables": tables,
        "formulas": [],
        "n_formulas": 0,
        "formula_failures": {},
        "text": _assemble_text(title, "", abstract, sections, figures, tables),
    }
    if unverified:
        out["authors_may_include_editors"] = True
    return out


# ------------------------------------------------------------------------------ fetching
# c13: single home for the Europe PMC JATS full-text GET (pmc_fetch, backfill_fulltext, recheck_pmc
# and _smoke_test); every fetch goes through litpipe.net, which injects the one identity.
EPMC_JATS_XML = "https://www.ebi.ac.uk/europepmc/webservices/rest/{pmcid}/fullTextXML"

# Europe PMC reference guide 6.9.0 (doc 1.51), getFullTextXML: "The full text XML is available only
# for the full-text OA subset of the Europe PMC database." Since 2026-09-16 an article outside that
# set answers 500 with a JSON body, deterministically (V1 P3: 10/10 unchanged after 31.9 min); it
# answered 404 before. Both mean NOT_AVAILABLE, and the 500 is never retried: the call passes its own
# retry statuses to litpipe.net (the www.ebi.ac.uk row keeps retrying 500 for the REST search).


def _retry_without(url, statuses):
    """The host row's retry statuses minus `statuses`, for one litpipe.net call."""
    return hosts.policy(url).retry.statuses - frozenset(statuses)


def _failure_status(o) -> str:
    """The legacy-shaped status string of a failed litpipe.net Outcome. A caller that routes on
    kinds should use the Outcome forms (fetch_jats_outcome, fetch_bioc_outcome) instead."""
    if o.kind is Kind.TRANSPORT:
        return f"TRANSPORT: {o.detail}"
    if o.kind is Kind.DEFERRED:
        return "DEFERRED"
    if o.status:
        return f"HTTP_{o.status}"
    return f"{o.kind}: {o.detail}".strip()


def fetch_jats_outcome(pmcid, timeout=30) -> Outcome:
    """GET Europe PMC fullTextXML for a PMCID through litpipe.net. OK carries the XML bytes as
    payload; a 500 or 404/410 is NOT_AVAILABLE; a 200 that is empty or not XML is OUTAGE; every
    other failure keeps litpipe.net's kind (TRANSPORT, REFUSED, OUTAGE, DEFERRED, ERROR)."""
    url = EPMC_JATS_XML.format(pmcid=pmcid)
    host = hosts.host_of(url)
    o = net.get(url, timeout=(10, timeout), purpose="fullTextXML", validate=_expect_xml,
                retry_statuses=_retry_without(url, {500}))
    if o.ok:
        return Outcome(Kind.OK, status=o.status, host=host, attempts=o.attempts,
                       elapsed_ms=o.elapsed_ms, payload=o.payload.content)
    if o.status in (404, 410, 500):
        return Outcome(Kind.NOT_AVAILABLE, status=o.status, host=host, attempts=o.attempts,
                       elapsed_ms=o.elapsed_ms,
                       detail=f"HTTP {o.status}: not in Europe PMC's open-access full-text set")
    return o


def _expect_xml(p):
    body = (p.content or b"").strip()
    if not body or not body.startswith(b"<"):
        return (Kind.OUTAGE, "EMPTY_OR_NON_XML")
    return None


def fetch_jats_xml(pmcid, ua=None, timeout=30):
    """GET a Europe PMC JATS full-text XML document for a PMCID (through litpipe.net).

    Returns (content_bytes, "OK") for a 200 whose body is XML. Otherwise (None, status):
      - "NOT_AVAILABLE"      500 (since 2026-09-16) or 404: not in Europe PMC's OA full-text set
      - "EMPTY_OR_NON_XML"   200 but the body is empty or does not start with '<'
      - "TRANSPORT: <why>"   DNS, timeout or connection failure after litpipe.net's retries
      - "HTTP_<code>"        any other status (403/429 refused, 502/503/504 after retries)
      - "DEFERRED"           the host is deferred or its daily budget is spent; nothing sent
      - "REFUSED: <why>"     the host is refused for the run; nothing sent
    Never raises for a network failure. `ua` is accepted for old callers and ignored: litpipe.net
    sends the pipeline's one identity."""
    o = fetch_jats_outcome(pmcid, timeout=timeout)
    if o.ok:
        return o.payload, "OK"
    if o.kind is Kind.NOT_AVAILABLE:
        return None, "NOT_AVAILABLE"
    if o.kind is Kind.OUTAGE and o.status == 200:
        return None, "EMPTY_OR_NON_XML"
    return None, _failure_status(o)


def _is_bioc_absent(p) -> bool:
    body = (p.content or b"")[:400].lstrip()
    return "html" in p.content_type or body.startswith(b"[Error]") or b"No result can be found" in body


def fetch_bioc_outcome(pmcid, encoding="unicode", timeout=30) -> Outcome:
    """GET the BioC JSON for a PMCID through litpipe.net. OK carries the parsed JSON as payload;
    the 200 text/html "[Error] : No result can be found." page is NOT_AVAILABLE (keyed on the
    Content-Type and the body, never the status); a 429 is not retried (it refuses the host for the
    run: this host answered 429 to a third request 0.65 s after the second on 2026-09-30)."""
    url = BIOC_JSON.format(pmcid=pmcid, encoding=encoding)
    host = hosts.host_of(url)
    o = net.get(url, timeout=(10, timeout), purpose="bioc", retry_statuses=_retry_without(url, {429}))
    if not o.ok:
        return o
    p = o.payload
    if _is_bioc_absent(p):
        return Outcome(Kind.NOT_AVAILABLE, status=o.status, host=host, attempts=o.attempts,
                       elapsed_ms=o.elapsed_ms,
                       detail="not in the PMC Open Access Subset or Author Manuscript Collection")
    try:
        data = json.loads(p.text)
        _bioc_document(data)
    except (ValueError, KeyError, IndexError, TypeError) as e:
        return Outcome(Kind.OUTAGE, status=o.status, host=host, attempts=o.attempts,
                       elapsed_ms=o.elapsed_ms, detail=f"BioC body is not a BioC document: {e}")
    return Outcome(Kind.OK, status=o.status, host=host, attempts=o.attempts,
                   elapsed_ms=o.elapsed_ms, payload=data)


def fetch_bioc(pmcid, encoding="unicode", timeout=30):
    """(bioc_json | None, status) with the statuses of fetch_jats_xml ("OK", "NOT_AVAILABLE",
    "TRANSPORT: ...", "HTTP_<code>", "DEFERRED", "REFUSED: ..."; a 200 that is not BioC JSON is
    "EMPTY_OR_NON_JSON"). Never raises for a network failure."""
    o = fetch_bioc_outcome(pmcid, encoding=encoding, timeout=timeout)
    if o.ok:
        return o.payload, "OK"
    if o.kind is Kind.NOT_AVAILABLE:
        return None, "NOT_AVAILABLE"
    if o.kind is Kind.OUTAGE and o.status == 200:
        return None, "EMPTY_OR_NON_JSON"
    return None, _failure_status(o)


def fetch_fulltext(pmcid, timeout=30):
    """Europe PMC JATS first, then BioC when Europe PMC does not hold the article (author
    manuscripts: N-B4). Returns (sidecar_dict | None, status, source) with source "jats",
    "bioc" or "". A JATS failure other than NOT_AVAILABLE is returned as is (no BioC call: the
    article may well be OA and the failure transient). Raises nothing for a network failure;
    an unparseable document raises ET.ParseError / ValueError to the caller."""
    content, status = fetch_jats_xml(pmcid, timeout=timeout)
    if content is not None:
        return parse_jats(content), "OK", "jats"
    if status != "NOT_AVAILABLE":
        return None, status, ""
    bioc, bstatus = fetch_bioc(pmcid, timeout=timeout)
    if bioc is None:
        return None, bstatus, ""
    return parse_bioc(bioc), "OK", "bioc"


def _smoke_test(pmcid: str, dump: bool, bioc: bool = False) -> int:
    """Fetch one article (Europe PMC JATS, or BioC with --bioc) and dump the parsed structure."""
    if bioc:
        data, status = fetch_bioc(pmcid)
        print(f"BioC fetch: {status}")
        if data is None:
            return 1
        out = parse_bioc(data)
    else:
        content, status = fetch_jats_xml(pmcid)
        print(f"JATS fetch: {status}" + (f" ({len(content)} bytes)" if content else ""))
        if content is None:
            return 1
        out = parse_jats(content)
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
        description="Parse JATS XML (or BioC JSON) into a structured sidecar dict (library + dev smoke-test).")
    ap.add_argument("--smoke-test", metavar="PMCID", default=None,
                    help="Fetch this PMCID from Europe PMC and dump the parsed structure. "
                         "Example: --smoke-test PMC4977162.")
    ap.add_argument("--bioc", action="store_true",
                    help="With --smoke-test, fetch the BioC JSON (open access and author manuscripts) instead.")
    ap.add_argument("--dump", action="store_true",
                    help="With --smoke-test, also dump the full plain-text body.")
    args = ap.parse_args()
    if args.smoke_test:
        sys.exit(_smoke_test(args.smoke_test, args.dump, bioc=args.bioc))
    print("jats_to_text is a library module; import parse_jats / parse_bioc from it, or run with "
          "--smoke-test PMCID for a dev probe.", file=sys.stderr)
    sys.exit(0)
