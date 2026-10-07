"""PMC stage: PDFs and full-text sidecars from NCBI's sanctioned routes, for the DOIs Unpaywall
could not deliver (W2-A1, refactor scope §3.1).

"The PMC Cloud Service, PMC OAI-PMH Service, E-Utilities and BioC API are the only services that may
be used for automated retrieval of PMC content" (PMC For Developers, Aug 2026). Every request goes
through litpipe.net (host policy, pacing, refusals, ledger; identity injected once, DEC-13).

Per row (rows come from the Unpaywall report: downloaded=False and not SKIP_EXISTS):
  0. a PDF already in the library for this DOI is ALREADY_EXISTS (RC2), before any request;
  1. DOI -> PMCID: lit_net.doi_to_pmcid (idconv while pmc.ncbi.nlm.nih.gov is not refused, then
     Europe PMC REST search, then E-utilities esearch + esummary). EMBARGOED carries the release
     date; a lookup that no service could answer keeps its first failure (TRANSIENT), never NO_PMCID;
  2. classify the PMCIDs with one `efetch db=pmc` per 30 IDs (custom-meta flags; V1-P13 matched the
     S3 class 30/30): EMBARGOED (pmc-status-embargo=yes or pmc-status-live=no), OA
     (pmc-prop-open-access=yes), AM (pmc-prop-manuscript=yes), or NONE;
  3. OA and AM: PMC Cloud on S3 (anonymous): ListObjectsV2 for the versions (do not assume `.1`),
     the latest version's metadata JSON, then the PDF when the metadata lists one and the JATS XML.
     Every object is checked against the md5 in its metadata URL (`?md5=`), never the ETag (a
     multipart ETag is not an md5, V1-N6). An AM has no PDF on any sanctioned route: its sidecar is
     text-only (`has_pdf: false`), from the S3 XML or else the BioC JSON. Figures come from the S3
     media list (fetch_figures.figures_from_s3, W2-A2);
  4. NONE (publisher copyright, free to read on the website only): Europe PMC fullTextXML only when
     Europe PMC flagged the article OA; else NOT_AVAILABLE with nothing sent. A fullTextXML 500 is
     NOT_AVAILABLE (deterministic since 2026-09-16, V1-P3);
  EMBARGOED rows fetch nothing. When the classification itself failed, S3 decides (its metadata
  says OA or AM); an S3 miss then keeps the classification failure as the root cause.

Retired (prohibited routes): Europe PMC `?pdf=render`, the NCBI article page and its
`citation_pdf_url`. The DOI-mismatch quarantine is gone too: litpipe.identity.check reads the PDF's
first pages; a FLAG leaves the file where it is and is recorded in the sidecar (and the report's
`identity` column), never moved.

Report (one row per input row; legacy columns kept): doi, filename, pmcid, downloaded, skipped,
winning_source, attempts, error, sidecar, sidecar_status, plus first_status, route, outcome (a
litpipe Kind: the FIRST root cause, not the last fallback, V1-N1), identity, release_date, license,
pmc_class, figures. `error` stays a legacy string that litpipe.outcomes.from_legacy maps to the
same Kind wherever a legacy token exists (EMBARGOED, DEFERRED and a not-sent refusal have none yet).

Usage:
  python pmc_fetch.py [--dry-run] [--only-doi DOI ...] [--no-sidecar] [--no-write-ris]
                      [--no-figures] [--base-dir DIR] [--report-in CSV] [--lib-dir DIR] [--report-out CSV]
"""
import argparse
import csv
import dataclasses
import datetime as _dt
import hashlib
import io
import json
import os
import re
import sys
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import jats_to_text  # parse_jats; parse_bioc and the litpipe.net fetch_jats_xml come with W2-A2
import lit_net
import lit_util
import ris_emit as _R
# RC2: the collision-safe destination and the on-disk DOI reader stay shared with the Unpaywall
# stage; T5a: so does the boilerplate fingerprint. The quarantine helpers are not imported (W2-A1).
from unpaywall_fetch_v2 import existing_holds, is_known_boilerplate, resolve_dest
from litpipe import doi as _doi
from litpipe import identity as _identity
from litpipe import net
from litpipe.ledger import redact
from litpipe.outcomes import Kind, Outcome

lit_util.utf8_stdout()

S3_HOST = "pmc-oa-opendata.s3.amazonaws.com"
S3_BASE = f"https://{S3_HOST}"
EFETCH = f"{lit_net.EUTILS}/efetch.fcgi"
EFETCH_BATCH = 30
EFETCH_MAX_BYTES = 200_000_000     # 30 articles at up to ~1.9 MB each (V1-P13: 5.7 MB for 30)
XML_MAX_BYTES = 60_000_000
BIOC = "https://www.ncbi.nlm.nih.gov/research/bionlp/RESTful/pmcoa.cgi/BioC_json/{pmcid}/unicode"

ROUTE_EFETCH = "efetch"
ROUTE_S3 = "s3"
ROUTE_BIOC = "bioc"
ROUTE_FULLTEXT = "epmc_fulltextxml"
ROUTE_FS = "library"

OA, AM, NONE, EMBARGOED, UNKNOWN = "OA", "AM", "NONE", "EMBARGOED", "UNKNOWN"

DEFAULT_REPORT_IN = "data/prior_art/discovered/unpaywall_fetch_report_v2.csv"
DEFAULT_LIB = "references/literature"
DEFAULT_REPORT_OUT = "data/prior_art/discovered/pmc_fetch_report.csv"

LEGACY_FIELDS = ["doi", "filename", "pmcid", "downloaded", "skipped", "winning_source", "attempts",
                 "error", "sidecar", "sidecar_status"]
REPORT_FIELDS = LEGACY_FIELDS + ["first_status", "route", "outcome", "identity", "release_date",
                                 "license", "pmc_class", "figures"]


def safe_filename(name: str) -> str:
    # Strip non-ASCII (matches v2 naming convention which is ASCII-only).
    return name.encode("ascii", "ignore").decode("ascii")


# ============================================================================ classification
@dataclass(frozen=True)
class PmcClass:
    """Outcome.payload of classify_pmcids: one PMCID's class from efetch db=pmc."""
    pmcid: str
    klass: str                       # OA | AM | NONE | EMBARGOED
    license: str = ""
    flags: dict = field(default_factory=dict)


_META_RE = re.compile(r"<meta-name>\s*([^<]+?)\s*</meta-name>\s*<meta-value>\s*([^<]*?)\s*</meta-value>")


def _pmc_num(pmcid) -> str:
    return re.sub(r"(?i)^pmc", "", str(pmcid or "").strip()).split(".")[0]


def parse_efetch(body: str) -> dict:
    """{PMCID: PmcClass} from an efetch db=pmc XML answer. Flags are read from <article-meta>
    only: <journal-meta> carries its own custom-meta group (collection flags)."""
    out = {}
    for art in re.split(r"(?=<article[\s>])", body or "")[1:]:
        a0, a1 = art.find("<article-meta"), art.find("</article-meta>")
        meta = art[a0:a1] if a0 >= 0 else ""
        m = re.search(r'<article-id pub-id-type="pmcid">\s*(PMC\d+)', meta)
        if not m:
            m2 = re.search(r'<article-id pub-id-type="pmcaid">\s*(\d+)', meta)
            if not m2:
                continue
            pmcid = f"PMC{m2.group(1)}"
        else:
            pmcid = m.group(1)
        flags = {k: v.lower() for k, v in _META_RE.findall(meta) if k.startswith("pmc-")}
        if flags.get("pmc-status-embargo") == "yes" or flags.get("pmc-status-live") == "no":
            klass = EMBARGOED
        elif flags.get("pmc-prop-open-access") == "yes":
            klass = OA
        elif flags.get("pmc-prop-manuscript") == "yes":
            klass = AM
        else:
            klass = NONE
        lic = dict(_META_RE.findall(meta)).get("pmc-license-ref", "")
        out[pmcid.upper()] = PmcClass(pmcid.upper(), klass, lic, flags)
    return out


def classify_pmcids(pmcids, *, state=None, cfg=None, batch=EFETCH_BATCH) -> dict:
    """{PMCID: Outcome} with payload PmcClass on OK. A failed efetch call gives every PMCID of its
    batch that call's outcome; a PMCID missing from a good answer is ERROR."""
    ids = []
    for p in pmcids:
        n = _pmc_num(p)
        if n.isdigit() and f"PMC{n}" not in ids:
            ids.append(f"PMC{n}")
    out = {}
    for i in range(0, len(ids), batch):
        chunk = ids[i:i + batch]
        o = net.get(EFETCH, params={"db": "pmc", "id": ",".join(_pmc_num(p) for p in chunk), "retmode": "xml"},
                    max_bytes=EFETCH_MAX_BYTES, timeout=(10, 120), purpose="classify PMCIDs (efetch db=pmc)",
                    state=state, cfg=cfg)
        parsed = {}
        if o.ok:
            text = o.payload.text
            parsed = parse_efetch(text)
            if not parsed:
                err = re.search(r"<ERROR>(.*?)</ERROR>", text, re.S)
                o = Outcome(Kind.ERROR if err else Kind.OUTAGE, status=o.status, host=o.host, attempts=o.attempts,
                            detail=(f"efetch error: {err.group(1).strip()}" if err
                                    else "efetch answered without any article")[:200])
        for p in chunk:
            if not o.ok:
                out[p] = o
            elif p in parsed:
                out[p] = Outcome(Kind.OK, status=o.status, host=o.host, attempts=o.attempts, payload=parsed[p])
            else:
                out[p] = Outcome(Kind.ERROR, status=o.status, host=o.host, attempts=o.attempts,
                                 detail="PMCID absent from the efetch answer")
    return out


# ============================================================================ PMC Cloud (S3)
def s3_https(s3_url):
    """'s3://pmc-oa-opendata/KEY?md5=X' -> ('https://<bucket host>/KEY', 'X' or None); (None, None)
    for anything else."""
    m = re.match(r"s3://pmc-oa-opendata/([^?\s]+)(?:\?md5=([0-9A-Fa-f]{32}))?", str(s3_url or "").strip())
    if not m:
        return None, None
    return f"{S3_BASE}/{m.group(1)}", (m.group(2) or "").lower() or None


def _expect_s3_list(p):
    body = (p.content or b"")[:4096]
    return None if b"ListBucketResult" in body else "not an S3 ListBucketResult"


def _expect_xml(p):
    body = (p.content or b"").lstrip()
    if not body:
        return "empty body"
    return None if body[:1] == b"<" else "body is not XML"


def s3_versions(pmcid, *, state=None, cfg=None) -> Outcome:
    """OK with payload [version prefix, ...] oldest first (an empty list: not in the datasets)."""
    o = net.get(f"{S3_BASE}/", params={"list-type": "2", "prefix": f"{pmcid}.", "delimiter": "/"},
                validate=_expect_s3_list, purpose="PMC Cloud versions", state=state, cfg=cfg)
    if not o.ok:
        return o
    found = re.findall(r"<CommonPrefixes>\s*<Prefix>([^<]+?)/?</Prefix>", o.payload.text)
    vs = []
    for v in found:
        m = re.fullmatch(r"(PMC\d+)\.(\d+)", v.strip())
        if m and m.group(1).upper() == pmcid.upper():
            vs.append((int(m.group(2)), f"{m.group(1)}.{m.group(2)}"))
    return dataclasses.replace(o, payload=[v for _, v in sorted(vs)])


def s3_metadata(version, *, state=None, cfg=None) -> Outcome:
    """OK with payload the metadata dict of one article version (`metadata/<version>.json`)."""
    o = net.get(f"{S3_BASE}/metadata/{version}.json", validate=net.expect_json,
                purpose="PMC Cloud metadata", state=state, cfg=cfg)
    if not o.ok:
        return o
    try:
        meta = o.payload.json()
    except ValueError as e:
        return Outcome(Kind.OUTAGE, status=o.status, host=o.host, attempts=o.attempts, detail=f"metadata JSON: {e}")
    if not isinstance(meta, dict):
        return Outcome(Kind.OUTAGE, status=o.status, host=o.host, attempts=o.attempts,
                       detail="metadata JSON is not an object")
    return dataclasses.replace(o, payload=meta)


def s3_object(s3_url, *, pdf=False, state=None, cfg=None) -> Outcome:
    """GET one S3 object named by a metadata `s3://...?md5=` URL. OK carries the body bytes; the
    body's md5 must equal the metadata md5 (ERROR otherwise). The ETag is never compared."""
    url, md5 = s3_https(s3_url)
    if not url:
        return Outcome(Kind.ERROR, host=S3_HOST, detail=f"not a PMC Cloud URL: {str(s3_url)[:80]}")
    if pdf:
        o = net.get(url, stream=True, max_bytes=lit_net.MAX_PDF_BYTES, validate=net.expect_pdf,
                    timeout=(10, 120), purpose="PMC Cloud PDF", state=state, cfg=cfg)
    else:
        o = net.get(url, max_bytes=XML_MAX_BYTES, validate=_expect_xml, timeout=(10, 60),
                    purpose="PMC Cloud JATS XML", state=state, cfg=cfg)
    if not o.ok:
        return o
    body = o.payload.content or b""
    got = hashlib.md5(body).hexdigest()
    if md5 and got != md5:
        return Outcome(Kind.ERROR, status=o.status, host=o.host, attempts=o.attempts,
                       detail=f"md5 mismatch: body {got}, metadata {md5}")
    return dataclasses.replace(o, payload=body)


# ============================================================================ other text routes
def bioc_json(pmcid, *, state=None, cfg=None) -> Outcome:
    """BioC JSON for an OA or AM article. 'Absent' is a 200 text/html '[Error] : No result can be
    found.' (V1-P1d), which is NOT_AVAILABLE, not OK."""
    o = net.get(BIOC.format(pmcid=pmcid), purpose="BioC text", state=state, cfg=cfg)
    if not o.ok:
        return o
    body = (o.payload.content or b"").strip()
    if "html" in o.payload.content_type or body[:7] == b"[Error]" or body[:1] not in (b"{", b"["):
        return Outcome(Kind.NOT_AVAILABLE, status=o.status, host=o.host, attempts=o.attempts,
                       detail="BioC has no record (200 with an HTML error page)")
    return dataclasses.replace(o, payload=body)


def _fulltextxml_outcome(pmcid) -> Outcome:
    """Europe PMC fullTextXML through jats_to_text.fetch_jats_xml (W2-A2 owns the fetch)."""
    try:
        content, status = jats_to_text.fetch_jats_xml(pmcid, None)
    except Exception as e:   # the pre-W2-A2 function lets transport errors propagate
        return Outcome(Kind.TRANSPORT, host="www.ebi.ac.uk", detail=f"{type(e).__name__}: {e}"[:200])
    if content is not None:
        return Outcome(Kind.OK, status=200, host="www.ebi.ac.uk", payload=content)
    st = str(status or "")
    m = re.search(r"(\d{3})", st)
    code = int(m.group(1)) if m else None
    if st == "NOT_AVAILABLE" or code in (404, 500):
        kind = Kind.NOT_AVAILABLE    # the OA-only endpoint's "not in the subset" (500 since 2026-09-16)
    elif st.startswith("EMPTY"):
        kind = Kind.OUTAGE
    else:
        from litpipe.outcomes import from_legacy
        kind = from_legacy(st, "fulltext")
    return Outcome(kind, status=code, host="www.ebi.ac.uk", detail=st)


def _parse_bioc_fn():
    return getattr(jats_to_text, "parse_bioc", None)


def _figures_fn():
    try:
        import fetch_figures
    except Exception:
        return None
    return getattr(fetch_figures, "figures_from_s3", None)


# ============================================================================ identity
def first_pages_text(path, pages=2) -> str:
    """Text of the first `pages` pages (pymupdf), '' when the file cannot be read."""
    try:
        import pymupdf as fitz
        with fitz.open(path) as doc:
            return "\n".join(doc[i].get_text() for i in range(min(pages, doc.page_count)))
    except Exception:
        return ""


# ============================================================================ report strings
def legacy_error(route, o: Outcome, release_date=None) -> str:
    """The `error` column: `route/TOKEN: detail`, with the token chosen so that
    litpipe.outcomes.from_legacy(error, "pmc") gives o.kind wherever a legacy token exists."""
    k = o.kind
    d = redact(o.detail or "")[:240]

    def w(s):
        return f"{s}: {d}" if d else s
    if k is Kind.OK:
        return ""
    if k is Kind.NO_MATCH:
        return w("NO_PMCID") if (route in lit_net.MAP_ROUTES or not route) else w(f"{route}/NOT_FOUND")
    if k is Kind.NOT_AVAILABLE:
        return w(f"{route}/CLOSED")
    if k is Kind.EMBARGOED:
        return f"{route}/EMBARGOED " + (f"until {release_date}" if release_date else "(release date unknown)")
    if k is Kind.TRANSPORT:
        return f"{route}/TRANSPORT: {d or 'no response'} (timeout or connection failure)"
    if k in (Kind.REFUSED, Kind.OUTAGE) and o.status:
        if 200 <= o.status < 300 and o.status != 202:
            return w(f"{route}/{'NOT_PDF' if k is Kind.REFUSED else 'EMPTY'}")
        return w(f"{route}/HTTP_{o.status}")
    return w(f"{route}/{k}")


# ============================================================================ one row
@dataclass
class _Ctx:
    lib_dir: str
    no_sidecar: bool = False
    no_write_ris: bool = False
    no_figures: bool = False
    state: object = None
    cfg: object = None
    written_this_run: set = field(default_factory=set)
    existing: set = field(default_factory=set)


class _Row:
    def __init__(self, r, fn):
        self.doi = r["doi"].strip().lower()
        self.title = (r.get("title") or "").strip()
        self.fn = fn
        self.rec = {k: "" for k in REPORT_FIELDS}
        self.rec.update({"doi": self.doi, "filename": fn, "downloaded": False, "skipped": False, "sidecar": False})
        self.steps = []                  # (route, Outcome) in order
        self.map_steps = ()
        self.pdf_written = None          # dest path when a PDF was written
        self.verdict = None

    def add(self, route, o):
        self.steps.append((route, o))
        return o

    def finish(self, release_date=None):
        rec = self.rec
        # the mapping's own steps come from its PmcidHit; a mapping outcome in self.steps is the
        # row-level summary of those, not another attempt
        rec["attempts"] = " | ".join(list(self.map_steps) + [f"{r}/{o.kind}" for r, o in self.steps
                                                              if r not in lit_net.MAP_ROUTES and r])
        if self.pdf_written:
            rec["route"], rec["outcome"], rec["first_status"] = ROUTE_S3, str(Kind.OK), "200"
            if self.verdict is not None and not self.verdict.ok:
                rec["error"] = ("DOI_MISMATCH:identity_flag (PDF kept in place; verdict in the sidecar) "
                                + redact(self.verdict.evidence.get("reason", ""))[:120]).strip()
            return rec
        root = next(((r, o) for r, o in self.steps if not o.ok), None)
        if root is None:
            return rec
        route, o = root
        rec["route"], rec["outcome"] = route, str(o.kind)
        rec["first_status"] = str(o.status) if o.status else ""
        rec["error"] = legacy_error(route, o, release_date)
        return rec


def _holds(dest, doi, title) -> bool:
    """REG-I11: the file at `dest` is THIS paper. A file whose identity verdict is FLAG (this stage
    records it in the .fulltext.json; the Unpaywall stage in .identity.json) is never the paper,
    whatever DOI its sidecar names; otherwise unpaywall_fetch_v2.existing_holds decides ("same"
    only: a file with no DOI and no title match is "unknown", not held)."""
    for ext in (".identity.json", ".fulltext.json"):
        try:
            with open(lit_util.companion_path(dest, ext), encoding="utf-8") as f:
                rec = json.load(f)
        except (OSError, ValueError):
            continue
        if isinstance(rec, dict) and rec.get("identity") == "FLAG":
            return False
    return existing_holds(dest, doi, title) == "same"


def _write_pdf(dest, body):
    tmp = dest + ".part"
    with open(tmp, "wb") as f:
        f.write(body)
    os.replace(tmp, dest)


def _sidecar_record(parsed, *, row, pmcid, meta, extractor, has_pdf, verdict):
    sc = dict(parsed or {})
    if not sc.get("doi"):
        sc["doi"] = row.doi
    if not sc.get("pmcid"):
        sc["pmcid"] = pmcid
    sc.update({
        "has_pdf": bool(has_pdf),
        "extracted_from_pdf": False,
        "extractor": extractor,
        "pmc_version": (meta or {}).get("version") or "",
        "license": (meta or {}).get("license_code") or row.rec.get("license") or "",
        "fetched_utc": _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    })
    if verdict is not None:
        sc.update(verdict.as_dict())
    return sc


def _merge_into_sidecar(path, extra) -> bool:
    """Add `extra` keys to an existing sidecar (a PDF that arrived after its text). False when the
    file is not a JSON object (left untouched)."""
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError):
        return False
    if not isinstance(data, dict):
        return False
    data.update(extra)
    lit_util.atomic_write_json(path, data)
    return True


def _text_from_s3_or_fallback(row, ctx, pmcid, klass, meta):
    """(parsed sidecar dict, extractor) or (None, None); every attempt is added to row.steps."""
    if meta and meta.get("xml_url"):
        xo = row.add(ROUTE_S3, s3_object(meta["xml_url"], state=ctx.state, cfg=ctx.cfg))
        if xo.ok:
            try:
                return jats_to_text.parse_jats(xo.payload), "pmc_s3_jats"
            except Exception as e:
                row.add(ROUTE_S3, Outcome(Kind.ERROR, host=S3_HOST, detail=f"parse_jats: {e}"[:200]))
    is_am = klass == AM or bool((meta or {}).get("is_manuscript"))
    if is_am:
        parse_bioc = _parse_bioc_fn()
        if parse_bioc is None:
            row.add(ROUTE_BIOC, Outcome(Kind.SKIPPED, detail="jats_to_text.parse_bioc not available (W2-A2)"))
            return None, None
        bo = row.add(ROUTE_BIOC, bioc_json(pmcid, state=ctx.state, cfg=ctx.cfg))
        if bo.ok:
            try:
                return parse_bioc(bo.payload), "pmc_bioc"
            except Exception as e:
                row.add(ROUTE_BIOC, Outcome(Kind.ERROR, host=bo.host, detail=f"parse_bioc: {e}"[:200]))
        return None, None
    fo = row.add(ROUTE_FULLTEXT, _fulltextxml_outcome(pmcid))
    if fo.ok:
        try:
            return jats_to_text.parse_jats(fo.payload), "europepmc_fulltextxml"
        except Exception as e:
            row.add(ROUTE_FULLTEXT, Outcome(Kind.ERROR, host=fo.host, detail=f"parse_jats: {e}"[:200]))
    return None, None


def _fetch_pdf(row, ctx, meta):
    """Download the metadata's PDF into the library; returns the Outcome (added to row.steps)."""
    po = s3_object(meta["pdf_url"], pdf=True, state=ctx.state, cfg=ctx.cfg)
    if not po.ok:
        return row.add(ROUTE_S3, po)
    dest, collided = resolve_dest(ctx.lib_dir, row.fn, row.doi, ctx.written_this_run)
    if collided:
        row.fn = os.path.basename(dest)
        row.rec["filename"] = row.fn
    _write_pdf(dest, po.payload)
    is_bp, tag = is_known_boilerplate(dest)
    if is_bp:
        os.remove(dest)
        return row.add(ROUTE_S3, Outcome(Kind.ERROR, status=po.status, host=po.host,
                                         detail=f"BOILERPLATE:{tag or ''}"))
    row.pdf_written = dest
    ctx.written_this_run.add(dest)
    ctx.existing.add(row.fn)
    row.verdict = _identity.check(first_pages_text(dest), row.doi, queue_title=row.title or None,
                                  ris_title=(meta.get("title") or None))
    row.rec["identity"] = str(row.verdict.decision)
    return row.add(ROUTE_S3, dataclasses.replace(po, payload=None))


def _write_text(row, ctx, pmcid, klass, meta):
    """The sidecar step: write (or update) <stem>.fulltext.json; returns the sidecar path or None."""
    sidecar_path = os.path.join(ctx.lib_dir, row.fn[:-4] + ".fulltext.json")
    extra_identity = row.verdict.as_dict() if row.verdict is not None else {}
    if os.path.exists(sidecar_path):
        row.rec["sidecar"], row.rec["sidecar_status"] = True, "EXISTS"
        if row.pdf_written:
            _merge_into_sidecar(sidecar_path, {"has_pdf": True, **extra_identity})
        return sidecar_path
    if ctx.no_sidecar:
        if row.pdf_written and row.verdict is not None and not row.verdict.ok:
            # the verdict must live on the artifact even without a text sidecar
            lit_util.atomic_write_json(sidecar_path, _sidecar_record({}, row=row, pmcid=pmcid, meta=meta,
                                                                     extractor="identity_only", has_pdf=True,
                                                                     verdict=row.verdict))
            row.rec["sidecar"], row.rec["sidecar_status"] = True, "IDENTITY_ONLY"
            return sidecar_path
        return None
    parsed, extractor = _text_from_s3_or_fallback(row, ctx, pmcid, klass, meta)
    if parsed is None:
        last = row.steps[-1][1] if row.steps else None
        row.rec["sidecar_status"] = str(last.kind) if last is not None else "NOT_AVAILABLE"
        if row.pdf_written and row.verdict is not None and not row.verdict.ok:
            lit_util.atomic_write_json(sidecar_path, _sidecar_record({}, row=row, pmcid=pmcid, meta=meta,
                                                                     extractor="identity_only", has_pdf=True,
                                                                     verdict=row.verdict))
            row.rec["sidecar"] = True
            return sidecar_path
        return None
    lit_util.atomic_write_json(sidecar_path, _sidecar_record(parsed, row=row, pmcid=pmcid, meta=meta,
                                                             extractor=extractor, has_pdf=bool(row.pdf_written),
                                                             verdict=row.verdict))
    row.rec["sidecar"], row.rec["sidecar_status"] = True, "OK"
    return sidecar_path


def _figures(row, ctx, meta, sidecar_path):
    if ctx.no_figures or not sidecar_path or not (meta or {}).get("media_urls"):
        return
    fn = _figures_fn()
    if fn is None:
        row.rec["figures"] = "pending (fetch_figures.figures_from_s3 not available)"
        return
    try:
        figs = fn(meta, sidecar_path, lib_dir=ctx.lib_dir) or []
        row.rec["figures"] = str(len(figs))
    except Exception as e:   # the contract says never raises; a bug there must not fail the row
        row.rec["figures"] = redact(f"ERROR {type(e).__name__}: {e}")[:120]


def fetch_row(row, ctx, mapping: Outcome, cls: Outcome | None):
    """Fetch one row whose DOI mapped to a PMCID; fills row.rec (finish() is the caller's)."""
    hit = mapping.payload
    pmcid = f"PMC{_pmc_num(hit.pmcid)}"
    row.rec["pmcid"] = pmcid
    klass = cls.payload.klass if (cls is not None and cls.ok) else UNKNOWN
    row.rec["pmc_class"] = klass
    cls_failure = None
    if cls is not None and cls.ok:
        row.rec["license"] = cls.payload.license
    else:
        cls_failure = cls if cls is not None else Outcome(Kind.ERROR, detail="PMCID not classified")
    if klass == EMBARGOED:
        row.add(ROUTE_EFETCH, Outcome(Kind.EMBARGOED, status=cls.status, host=cls.host,
                                      detail="pmc-status-embargo=yes (only metadata until the release date)"))
        return
    if klass == NONE:
        if hit.is_open_access == "Y" and not ctx.no_sidecar:
            # Europe PMC flags it OA while PMC does not: its fullTextXML may hold the text
            row.add(ROUTE_EFETCH, Outcome(Kind.NOT_AVAILABLE, status=cls.status, host=cls.host,
                                          detail="publisher copyright: no PDF on a sanctioned route"))
            _write_text(row, ctx, pmcid, klass, None)
            return
        row.add(ROUTE_EFETCH, Outcome(Kind.NOT_AVAILABLE, status=cls.status, host=cls.host,
                                      detail="publisher copyright: no sanctioned full-text route "
                                             "(not open access, not an author manuscript); nothing sent"))
        return

    # OA, AM, or UNKNOWN (the classification failed): the PMC Cloud decides. When it cannot, the
    # classification failure came first and is the root cause.
    vo = s3_versions(pmcid, state=ctx.state, cfg=ctx.cfg)
    if not vo.ok or not vo.payload:
        if cls_failure is not None:
            row.add(ROUTE_EFETCH, cls_failure)
        if not vo.ok:
            row.add(ROUTE_S3, vo)
        else:
            row.add(ROUTE_S3, Outcome(Kind.NOT_AVAILABLE, status=vo.status, host=vo.host,
                                      detail=(f"{klass} per efetch but " if klass != UNKNOWN else "")
                                      + "not in the PMC Cloud datasets"))
        if klass in (OA, AM):
            _write_text(row, ctx, pmcid, klass, None)
        return
    latest = vo.payload[-1]
    mo = s3_metadata(latest, state=ctx.state, cfg=ctx.cfg)
    if not mo.ok:
        if cls_failure is not None:
            row.add(ROUTE_EFETCH, cls_failure)
        row.add(ROUTE_S3, mo)
        if klass in (OA, AM):
            _write_text(row, ctx, pmcid, klass, None)
        return
    meta = mo.payload
    row.rec["license"] = meta.get("license_code") or row.rec["license"]
    if klass == UNKNOWN:
        klass = OA if meta.get("is_pmc_openaccess") else (AM if meta.get("is_manuscript") else NONE)
        row.rec["pmc_class"] = f"{klass} (from S3 metadata)"
    raw_mdoi = str(meta.get("doi") or "").strip()
    mdoi = (_doi.normalise(raw_mdoi) or raw_mdoi.lower()) if raw_mdoi else None
    if mdoi and mdoi != (_doi.normalise(row.doi) or row.doi):
        row.add(ROUTE_S3, Outcome(Kind.ERROR, status=mo.status, host=mo.host,
                                  detail=f"S3 metadata DOI {mdoi} is not the queue DOI; nothing fetched"))
        return
    if meta.get("pdf_url"):
        _fetch_pdf(row, ctx, meta)
    else:
        why = ("author manuscript: no PDF on a sanctioned route" if (klass == AM or meta.get("is_manuscript"))
               else "no PDF in the PMC Cloud datasets")
        row.add(ROUTE_S3, Outcome(Kind.NOT_AVAILABLE, status=mo.status, host=mo.host, detail=why))
    sidecar = _write_text(row, ctx, pmcid, klass, meta)
    _figures(row, ctx, meta, sidecar)
    if row.pdf_written and not ctx.no_write_ris and row.verdict is not None and row.verdict.ok:
        try:
            ris_status, _ = _R.emit_ris_for_pdf(row.doi, row.pdf_written)
            print(f"       ris: {ris_status}")
        except Exception as e:
            print(f"       ris: ERROR {redact(str(e))[:80]}")


# ============================================================================ the stage
def read_rows(report_in, only_doi=None):
    only = {d.strip().lower() for d in only_doi} if only_doi else None
    rows = []
    with open(report_in, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r.get("downloaded") == "True":
                continue
            if r.get("oa_status") == "SKIP_EXISTS":
                continue
            d = (r.get("doi") or "").strip().lower()
            if not d:
                continue
            if only is not None and d not in only:
                continue
            rows.append(r)
    return rows


def run(*, base_dir=None, report_in=None, lib_dir=None, report_out=None, dry_run=False, only_doi=None,
        no_sidecar=False, no_write_ris=False, no_figures=False, state=None, cfg=None) -> dict:
    """The PMC stage (dispatch 0.5 stage function). Returns the counts and the report path."""
    base = os.path.abspath(base_dir or os.getcwd())
    report_in = report_in or os.path.join(base, DEFAULT_REPORT_IN)
    lib_dir = lib_dir or os.path.join(base, DEFAULT_LIB)
    report_out = report_out or os.path.join(base, DEFAULT_REPORT_OUT)
    rows = read_rows(report_in, only_doi)
    print(f"Project: {base}")
    print(f"Library: {lib_dir}")

    if not dry_run:
        os.makedirs(lib_dir, exist_ok=True)
    report_dir = os.path.dirname(report_out)
    if report_dir:
        os.makedirs(report_dir, exist_ok=True)
    ctx = _Ctx(lib_dir, no_sidecar, no_write_ris, no_figures, state, cfg,
               existing=set(os.listdir(lib_dir)) if os.path.isdir(lib_dir) else set())

    todo, out_rows = [], []
    for r in rows:
        fn = safe_filename(r.get("filename") or "")
        doi = r["doi"].strip().lower()
        if not fn.lower().endswith(".pdf"):
            fn = re.sub(r"[^A-Za-z0-9._-]+", "_", doi).strip("_") + ".pdf"
        row = _Row(r, fn)
        dest = os.path.join(lib_dir, fn)
        # RC2 / REG-I11: skip only when the on-disk file is THIS paper (a file with no readable
        # DOI is not; a FLAGged file is not)
        if fn in ctx.existing and _holds(dest, doi, row.title):
            row.rec["skipped"] = True
            row.rec["winning_source"] = "ALREADY_EXISTS"
            row.rec["outcome"], row.rec["route"] = str(Kind.OK), ROUTE_FS
            out_rows.append(row.rec)
            print(f"  SKIP {fn[:75]}")
            continue
        todo.append(row)

    print(f"Looking up {len(todo)} DOIs in PMC...")
    mapping = lit_net.doi_to_pmcid([row.doi for row in todo], state=state, cfg=cfg) if todo else {}
    by_kind = {}
    for o in mapping.values():
        by_kind[str(o.kind)] = by_kind.get(str(o.kind), 0) + 1
    n_found = by_kind.get("OK", 0)
    print(f"  {n_found}/{len(todo)} have a live PMCID ({100 * n_found / max(1, len(todo)):.0f}%); "
          + ", ".join(f"{k} {v}" for k, v in sorted(by_kind.items()) if k != "OK"))

    live = [mapping[row.doi].payload.pmcid for row in todo if mapping[row.doi].kind is Kind.OK]
    classes = classify_pmcids(live, state=state, cfg=cfg) if (live and not dry_run) else {}

    counts = {"rows": len(rows), "skipped_existing": len(rows) - len(todo), "downloaded": 0, "identity_flag": 0,
              "text_only": 0, "sidecar_new": 0, "sidecar_exists": 0, "no_pmcid": 0, "embargoed": 0,
              "lookup_failed": 0, "not_available": 0, "failed": 0}
    for row in todo:
        m = mapping[row.doi]
        hit = m.payload
        row.map_steps = tuple(s.replace(":", "/", 1) for s in (hit.steps if hit else ()))
        if m.kind is not Kind.OK:
            row.rec["pmcid"] = (hit.pmcid or "") if hit else ""
            row.rec["release_date"] = (hit.release_date or "") if hit else ""
            row.add(hit.source if hit else "", m)
            rec = row.finish(release_date=row.rec["release_date"])
            key = {"NO_MATCH": "no_pmcid", "EMBARGOED": "embargoed"}.get(str(m.kind), "lookup_failed")
            counts[key] += 1
            out_rows.append(rec)
            print(f"  --   {row.doi[:55]:<55} {rec['error'][:70]}")
            continue
        if dry_run:
            row.rec["pmcid"] = hit.pmcid
            row.add("dry_run", Outcome(Kind.SKIPPED, detail="dry run"))
            rec = row.finish()
            rec["error"] = "DRY"
            out_rows.append(rec)
            print(f"  DRY  {hit.pmcid:<12} -> {row.fn[:60]}")
            continue
        try:
            fetch_row(row, ctx, m, classes.get(f"PMC{_pmc_num(hit.pmcid)}"))
        except Exception as e:    # a bug in one row must not lose the stage report
            row.add("pmc_fetch", Outcome(Kind.ERROR, detail=f"{type(e).__name__}: {e}"[:200]))
        rec = row.finish()
        out_rows.append(rec)
        if row.pdf_written:
            if row.verdict is not None and not row.verdict.ok:
                counts["identity_flag"] += 1
                rec["downloaded"] = False
                rec["winning_source"] = "pmc_s3"
                print(f"  FLAG {hit.pmcid:<12} -> {row.fn[:55]} (identity FLAG; kept in place)")
            else:
                counts["downloaded"] += 1
                rec["downloaded"] = True
                rec["winning_source"] = "pmc_s3"
                print(f"  DL   {hit.pmcid:<12} -> {row.fn[:60]} (pmc_s3, {rec['identity']})")
        elif rec["sidecar"] and rec["sidecar_status"] == "OK":
            counts["text_only"] += 1
            print(f"  TEXT {hit.pmcid:<12} -> {row.fn[:-4][:55]}.fulltext.json ({rec['outcome']})")
        else:
            counts["not_available" if rec["outcome"] == str(Kind.NOT_AVAILABLE) else "failed"] += 1
            print(f"  FAIL {hit.pmcid:<12} -> {row.fn[:55]} ({rec['error'][:70]})")
        if rec["sidecar_status"] == "OK":
            counts["sidecar_new"] += 1
        elif rec["sidecar_status"] == "EXISTS":
            counts["sidecar_exists"] += 1

    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=REPORT_FIELDS, extrasaction="ignore", lineterminator="\n")
    w.writeheader()
    for rec in out_rows:
        w.writerow({k: redact(v) if isinstance(v, str) else v for k, v in rec.items()})
    lit_util.atomic_write_text(report_out, buf.getvalue())

    print("\n=== Summary ===")
    print(f"  Total candidates:    {len(rows)}")
    print(f"  Skipped (existing):  {counts['skipped_existing']}")
    print(f"  No PMCID found:      {counts['no_pmcid']}")
    print(f"  Embargoed:           {counts['embargoed']}")
    print(f"  Lookup failed:       {counts['lookup_failed']} (retry later; not 'no PMCID')")
    print(f"  PDFs DOWNLOADED:     {counts['downloaded']}")
    print(f"  Identity FLAG:       {counts['identity_flag']} (kept in place; verdict in the sidecar)")
    print(f"  Text only (no PDF):  {counts['text_only']}")
    print(f"  Not available:       {counts['not_available']}")
    print(f"  Failed:              {counts['failed']}")
    if not no_sidecar:
        print(f"  Sidecars NEW:        {counts['sidecar_new']}")
        print(f"  Sidecars existed:    {counts['sidecar_exists']}")
    print(f"\nReport: {report_out}")
    return {**counts, "report": report_out}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    ap.add_argument("--dry-run", action="store_true",
                    help="Resolve PMCIDs but do not classify, download PDFs or write sidecars.")
    ap.add_argument("--only-doi", nargs="*", default=None,
                    help="Only attempt these DOIs (compared lower-case).")
    ap.add_argument("--no-sidecar", action="store_true",
                    help="Skip the .fulltext.json sidecar (an identity FLAG is still recorded on the PDF's sidecar).")
    ap.add_argument("--no-write-ris", action="store_true",
                    help="Skip writing .ris sidecar next to each successfully fetched PDF.")
    ap.add_argument("--no-figures", action="store_true",
                    help="Skip the figure images listed in the PMC Cloud metadata.")
    ap.add_argument("--base-dir", default=os.getcwd(),
                    help="Project root. Default: CWD.")
    ap.add_argument("--report-in", default=None,
                    help=f"Input fetch report (default: <base-dir>/{DEFAULT_REPORT_IN})")
    ap.add_argument("--lib-dir", default=None,
                    help=f"PDF + sidecar destination (default: <base-dir>/{DEFAULT_LIB})")
    ap.add_argument("--report-out", default=None,
                    help=f"Output report CSV (default: <base-dir>/{DEFAULT_REPORT_OUT})")
    args = ap.parse_args(argv)
    run(base_dir=args.base_dir, report_in=args.report_in, lib_dir=args.lib_dir, report_out=args.report_out,
        dry_run=args.dry_run, only_doi=args.only_doi, no_sidecar=args.no_sidecar,
        no_write_ris=args.no_write_ris, no_figures=args.no_figures)
    return 0


if __name__ == "__main__":
    sys.exit(main())
