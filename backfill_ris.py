"""Backfill .ris records next to the holdings of one literature library.

Library (one is required; there is no default library since W2-E2):
  --lib-dir PATH   the library directory itself;
  --project KEY    a project registered in projects.json, resolved with lit_util.lib_paths.

For each <stem>.pdf in the library's top level, and with --include-text-only for each text-only
holding (a <stem>.fulltext.json that carries text and has no PDF beside it; DEC-08: PMC JATS, BioC
and author-manuscript records are first-class holdings; the rule is litpipe.holdings'):
  1. an existing <stem>.ris is kept (EXISTS_SKIP) unless --overwrite or --force;
  2. a file whose identity verdict is FLAG (its <stem>.identity.json or <stem>.fulltext.json; an
     Unpaywall supplement too) is a review item, not a holding: IDENTITY_FLAG, nothing written;
  3. DOI: the sidecar's `doi` (litpipe.doi.normalise), else the first ~5,000 characters of the PDF
     text (PyMuPDF); a PDF PyMuPDF cannot read is PDF_ERROR, not NO_DOI;
  4. metadata: ris_emit.resolve_meta (Crossref, DataCite, then content negotiation for mEDRA,
     JaLC, KISTI, OP; its `source` is reported per row), for a PDF and for a text-only holding
     alike (a sidecar's flat "Family Given" author strings cannot be split reliably). Only when
     no source holds a text-only holding's DOI does it fall back to the sidecar's own fields
     (title and subtitle, year, journal, volume, issue, pages, authors, abstract; display form
     litpipe.text.display_field, NFC; source "sidecar");
  5. ris_emit.build_ris and write_ris. DEC-29: --overwrite replaces only a .ris the pipeline wrote
     and nobody has edited since (its sha256 is in the state manifest); an edited or unrecorded
     (curated) file is kept and counted KEPT_CURATED. --force replaces any file (implies
     --overwrite).
Default is a dry run (DRY:<doi source>, or DRY:KEPT_CURATED when write_ris would keep the file).
--commit writes and saves <lib>/_ris_backfill_report.csv.

Row statuses: WROTE, KEPT_CURATED, EXISTS_SKIP, NO_DOI (the row carries the file's path),
PDF_ERROR, IDENTITY_FLAG, META_UNAVAILABLE (a metadata source could not answer:
ris_emit.MetadataUnavailable; try again later; the run continues), RESOLVE_FAIL (no source holds
the DOI, or a text-only sidecar without a title), UNSUPPORTED_RA (the DOI's agency has no https
route, source unsupported:<agency>), SIDECAR_ERROR (unreadable sidecar), NO_TEXT (a PDF-less
sidecar without text: not a holding), DRY:*.
Report columns: pdf, doi, status, out (legacy), then kind (pdf | text_only), path, source,
doi_from, detail (redacted).

Every request goes through litpipe.net via ris_emit (one identity, per-host pacing, ledger); a
text-only holding costs one metadata request, as a PDF does. Exit codes: 0 the run completed (row failures are in the report);
2 usage or configuration (no library given, an unknown project, a missing directory).

Usage:
  python backfill_ris.py --lib-dir <path>                         # dry run
  python backfill_ris.py --project <KEY> --commit                 # write missing .ris
  python backfill_ris.py --project <KEY> --commit --include-text-only
  python backfill_ris.py --lib-dir <path> --commit --overwrite    # refresh pipeline-owned .ris
  python backfill_ris.py --lib-dir <path> --commit --force        # replace curated .ris too
"""
import argparse
import collections
import hashlib
import json
import os
import re
import sys
import time
from pathlib import Path

import lit_util
lit_util.utf8_stdout()

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ris_emit as R  # noqa: E402
from litpipe import config  # noqa: E402
from litpipe import doi as _doi  # noqa: E402
from litpipe.ledger import redact  # noqa: E402
from litpipe.text import abstract_field, display_field  # noqa: E402

SIDECAR = ".fulltext.json"
REPORT_NAME = "_ris_backfill_report.csv"
REPORT_FIELDS = ["pdf", "doi", "status", "out", "kind", "path", "source", "doi_from", "detail"]
NO_LIBRARY = ("backfill_ris: no library given; pass --lib-dir PATH or --project KEY "
              "(there is no default library)")


class UsageError(ValueError):
    """No usable library: the CLI prints one line and exits 2."""


class PdfError(Exception):
    """PyMuPDF could not read the PDF (or is not installed)."""


# ------------------------------------------------------------------------------ library selection
def resolve_library(lib_dir=None, project=None, cfg=None) -> Path:
    """The library directory from --lib-dir or --project (exactly one). Raises UsageError."""
    if lib_dir and project:
        raise UsageError("backfill_ris: pass --lib-dir or --project, not both")
    if not lib_dir and not project:
        raise UsageError(NO_LIBRARY)
    if project:
        projects = config.load(cfg).get("projects") or {}
        p = projects.get(project)
        if not isinstance(p, dict):
            raise UsageError(f"backfill_ris: project {project!r} is not registered in projects.json")
        if not p.get("lib_dir"):
            raise UsageError(f"backfill_ris: project {project!r} declares no lib_dir")
        lib = lit_util.lib_paths(project, p)[1]
    else:
        lib = Path(lib_dir)
    if not lib.is_dir():
        raise UsageError(f"backfill_ris: lib-dir not found: {lib}")
    return lib


# ------------------------------------------------------------------------------ sidecars and PDFs
def _read_json(path):
    with open(path, encoding="utf-8") as f:
        d = json.load(f)
    if not isinstance(d, dict):
        raise ValueError("not a JSON object")
    return d


def _flagged_record(d) -> bool:
    return isinstance(d, dict) and (d.get("identity") == "FLAG" or d.get("doc_kind") == "SUPPLEMENT")


def doi_from_sidecar(sidecar_path: Path) -> str:
    """The normalised `doi` of a .fulltext.json; '' when absent, unreadable, not a DOI, or when the
    sidecar's identity verdict is FLAG (the PMC stage records the queue DOI of a PDF judged to be
    another work there)."""
    try:
        d = _read_json(sidecar_path)
    except (OSError, ValueError):
        return ""
    if d.get("identity") == "FLAG":
        return ""
    return _doi.normalise(d.get("doi") or "") or ""


def identity_flag(pdf_path: Path) -> str:
    """Why the file is flagged ('' when it is not): an identity verdict of FLAG in its
    .identity.json (Unpaywall, preprint) or .fulltext.json (PMC), or an Unpaywall SUPPLEMENT."""
    for ext in (".identity.json", SIDECAR):
        p = lit_util.companion_path(pdf_path, ext)
        try:
            d = _read_json(p)
        except (OSError, ValueError):
            continue
        if _flagged_record(d):
            return f"{p.name}: identity={d.get('identity', '')} doc_kind={d.get('doc_kind', '')}".strip()
    return ""


def doi_from_pdf(pdf_path: Path, max_chars=5000) -> str:
    """DOI from the first ~5,000 characters of the PDF text ('' when the text has none). Raises
    PdfError when PyMuPDF is missing or cannot read the file (counted PDF_ERROR, never NO_DOI)."""
    try:
        import pymupdf
    except ImportError as e:
        raise PdfError(f"PyMuPDF not installed: {e}") from None
    text = ""
    try:
        doc = pymupdf.open(str(pdf_path))
        try:
            for p in doc:
                text += p.get_text()
                if len(text) >= max_chars:
                    break
        finally:
            doc.close()
    except Exception as e:
        raise PdfError(f"{type(e).__name__}: {e}") from None
    return R.extract_doi_from_text(text[:max_chars])


# ------------------------------------------------------------------------------ text-only metadata
# Lower-case surname particles kept with the surname when a sidecar author ("Surname Given...", the
# PMC stage's form) is split. The sidecar does not record the boundary, so the first token is the
# surname, plus any leading lower-case particles ("van der Berg Hans"). A capitalised "Le", "De" or
# "Van" is not a particle here: "Le Van Thanh" is surname "Le".
_PARTICLES = frozenset({"van", "von", "der", "den", "de", "del", "della", "di", "da", "dos", "das",
                        "du", "la", "le", "ter", "ten", "zu", "op", "af", "bin", "ibn", "al", "el"})


def split_author(a):
    """{'family', 'given'} from a sidecar author: a dict (family/given or surname/given-names), a
    "Family, Given" string, or the PMC stage's "Family Given" string. None when empty."""
    if isinstance(a, dict):
        fam = display_field(a.get("family") or a.get("surname") or a.get("name") or "")
        giv = display_field(a.get("given") or a.get("given-names") or "")
        return {"family": fam, "given": giv} if fam else None
    s = display_field(a if isinstance(a, str) else "")
    if not s:
        return None
    if "," in s:
        fam, giv = (x.strip() for x in s.split(",", 1))
        return {"family": fam, "given": giv} if fam else None
    toks = s.split()
    k = 0
    while k < len(toks) - 2 and toks[k] in _PARTICLES:
        k += 1
    return {"family": " ".join(toks[:k + 1]), "given": " ".join(toks[k + 1:])}


def sidecar_meta(sc: dict) -> dict:
    """A text-only sidecar as the dict ris_emit.build_ris consumes (the crossref_meta shape)."""
    d = _doi.normalise(sc.get("doi") or "") or ""
    authors = [x for x in (split_author(a) for a in (sc.get("authors") or [])) if x]
    year = str(sc.get("year") or "").strip()
    return {
        "doi": d,
        "title": R.join_title(sc.get("title") or "", sc.get("subtitle") or ""),
        "year": year if re.fullmatch(r"\d{4}", year) else "",
        "date": "",
        "lastname": authors[0]["family"] if authors else "",
        "authors": authors,
        "container": display_field(sc.get("journal")),
        "volume": display_field(sc.get("volume")),
        "issue": display_field(sc.get("issue")),
        "page": display_field(sc.get("pages")),
        "issn": "",
        "abstract": abstract_field(sc.get("abstract")),
        "url": "https://doi.org/" + _doi.encode_path(d) if d else "",   # DOI Handbook 4.7 (build_ris re-encodes too)
        "type": "journal-article",
    }


def text_only_sidecars(lib: Path) -> list:
    """The PDF-less `.fulltext.json` files in the library's top level (litpipe.holdings' rule: no
    `<stem>.pdf` beside it, compared case-insensitively). Text, identity and has_pdf are judged per
    row: sidecars written before W2-A1 carry no `has_pdf` key at all."""
    pdfs, sidecars = set(), []
    for e in os.scandir(lib):
        try:
            if not e.is_file():
                continue
        except OSError:
            continue
        low = e.name.lower()
        if low.endswith(SIDECAR):
            sidecars.append(Path(e.path))
        elif low.endswith(".pdf"):
            pdfs.add(e.name[:-4].casefold())
    return sorted(p for p in sidecars if p.name[:-len(SIDECAR)].casefold() not in pdfs)


# ------------------------------------------------------------------------------ writing
def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def would_keep(ris_path, ris_text, overwrite, force) -> bool:
    """write_ris's decision without writing (the dry run's KEPT_CURATED prediction)."""
    p = str(ris_path)
    if not os.path.exists(p) or force:
        return False
    if not overwrite:
        return True
    if R.ris_owner(p) == "pipeline":
        return False
    with open(p, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest() != _sha(ris_text)


# ------------------------------------------------------------------------------ the stage
def _row(kind, path, status, *, doi="", out="", source="", doi_from="", detail=""):
    return {"pdf": path.name if kind == "pdf" else "", "doi": doi, "status": status, "out": out,
            "kind": kind, "path": str(path), "source": source, "doi_from": doi_from,
            "detail": redact(detail)[:300] if detail else ""}


def _emit(row, ris_out, ris_text, commit, overwrite, force, stats):
    """Write (or, in a dry run, predict) one record; sets the row's status."""
    if commit:
        if R.write_ris(str(ris_out), ris_text, overwrite=overwrite or force, force=force):
            row["status"] = "WROTE"
        else:
            row["status"] = "KEPT_CURATED"          # DEC-29: edited or unrecorded (ris_emit says why)
    else:
        row["status"] = ("DRY:KEPT_CURATED" if would_keep(ris_out, ris_text, overwrite, force)
                         else f"DRY:{row['doi_from'] or 'sidecar'}")
    stats[row["status"]] += 1


def _do_pdf(pdf, commit, overwrite, force, stats, sources):
    ris_out = lit_util.companion_path(pdf, ".ris")
    if ris_out.exists() and not (overwrite or force):
        return _row("pdf", pdf, "EXISTS_SKIP", out=ris_out.name)
    flag = identity_flag(pdf)
    if flag:
        return _row("pdf", pdf, "IDENTITY_FLAG", detail=flag)
    doi = doi_from_sidecar(lit_util.companion_path(pdf, SIDECAR))
    doi_from = "sidecar" if doi else ""
    if not doi:
        try:
            doi = _doi.normalise(doi_from_pdf(pdf) or "") or ""
        except PdfError as e:
            return _row("pdf", pdf, "PDF_ERROR", detail=str(e))
        doi_from = "pdf" if doi else ""
    if not doi:
        return _row("pdf", pdf, "NO_DOI")
    stats[f"doi_{doi_from}"] += 1
    try:
        meta, source = R.resolve_meta(doi)
    except R.MetadataUnavailable as e:
        sources[f"unavailable:{e.source}"] += 1
        return _row("pdf", pdf, "META_UNAVAILABLE", doi=doi, source=e.source, doi_from=doi_from,
                    detail=str(e))
    sources[source] += 1
    if not meta:
        status = "UNSUPPORTED_RA" if source.startswith("unsupported:") else "RESOLVE_FAIL"
        return _row("pdf", pdf, status, doi=doi, source=source, doi_from=doi_from)
    row = _row("pdf", pdf, "", doi=doi, out=ris_out.name, source=source, doi_from=doi_from)
    _emit(row, ris_out, R.build_ris(meta), commit, overwrite, force, stats)
    return row


def _do_text_only(sc_path, commit, overwrite, force, stats, sources):
    base = sc_path.name[:-len(SIDECAR)]
    ris_out = sc_path.with_name(base + ".ris")
    if ris_out.exists() and not (overwrite or force):
        return _row("text_only", sc_path, "EXISTS_SKIP", out=ris_out.name)
    try:
        sc = _read_json(sc_path)
    except (OSError, ValueError) as e:
        return _row("text_only", sc_path, "SIDECAR_ERROR", detail=f"{type(e).__name__}: {e}")
    if _flagged_record(sc):
        return _row("text_only", sc_path, "IDENTITY_FLAG", detail=f"identity={sc.get('identity', '')}")
    if sc.get("has_pdf") is True or ("has_pdf" not in sc and sc.get("extracted_from_pdf") is True):
        return _row("text_only", sc_path, "ORPHAN_SIDECAR",
                    detail="extracted from a PDF that is not beside it (renamed or deleted): not a holding")
    txt = sc.get("text")
    if not (isinstance(txt, str) and txt.strip()):
        return _row("text_only", sc_path, "NO_TEXT", detail="sidecar without text: not a holding")
    meta = sidecar_meta(sc)
    if not meta["doi"]:
        return _row("text_only", sc_path, "NO_DOI")
    stats["doi_sidecar"] += 1
    # The registration agency's record first, as for a PDF: a sidecar stores authors as flat
    # "Family Given" strings with no boundary, so split_author mis-splits compound surnames
    # ("Soler Artigas María" -> "Soler, Artigas María"; about 1 paper in 10 on a 2026-10-07 sample).
    # A source that cannot answer now is META_UNAVAILABLE (tried again later): never write a .ris
    # from the sidecar's guess when a structured record may exist. Only a DOI no source holds falls
    # back to the sidecar's own fields.
    try:
        rmeta, source = R.resolve_meta(meta["doi"])
    except R.MetadataUnavailable as e:
        sources[f"unavailable:{e.source}"] += 1
        return _row("text_only", sc_path, "META_UNAVAILABLE", doi=meta["doi"], source=e.source,
                    doi_from="sidecar", detail=str(e))
    if rmeta:
        sources[source] += 1
        # the record wins; the sidecar fills what the record lacks (Crossref often has no abstract,
        # the JATS sidecar usually does)
        meta = {**rmeta, **{k: meta[k] for k in ("abstract", "title") if meta.get(k) and not rmeta.get(k)}}
    else:
        source = "sidecar"
        sources["sidecar"] += 1
        if not meta["title"]:
            return _row("text_only", sc_path, "RESOLVE_FAIL", doi=meta["doi"], source="sidecar",
                        doi_from="sidecar", detail="no source holds the DOI and the sidecar has no title")
    row = _row("text_only", sc_path, "", doi=meta["doi"] or "", out=ris_out.name, source=source,
               doi_from="sidecar")
    _emit(row, ris_out, R.build_ris(meta), commit, overwrite, force, stats)
    return row


def run(*, lib_dir=None, project=None, commit=False, overwrite=False, force=False, limit=0,
        sleep=0.0, include_text_only=False, cfg=None) -> dict:
    """Backfill one library (stage function). Raises UsageError for a missing or unknown library.
    Returns the library, the mode, the per-row records, the status counts, the metadata sources
    and the report path (None in a dry run)."""
    lib = resolve_library(lib_dir, project, cfg)
    items = [("pdf", p) for p in lib.iterdir() if p.is_file() and p.suffix.lower() == ".pdf"]
    if include_text_only:
        items += [("text_only", p) for p in text_only_sidecars(lib)]
    items.sort(key=lambda kp: kp[1].name.casefold())
    if limit:
        items = items[:limit]

    mode = "COMMIT" if commit else "DRY-RUN"
    n_pdf = sum(1 for k, _ in items if k == "pdf")
    print(f"== ris backfill [{mode}] ==")
    print(f"  lib-dir:  {lib}")
    print(f"  PDFs:     {n_pdf}")
    if include_text_only:
        print(f"  text-only holdings: {len(items) - n_pdf}")
    if force:
        print("  --force: curated and edited .ris files are replaced too (DEC-29 override)")
    print()

    rows = []
    stats = collections.Counter()
    sources = collections.Counter()
    for i, (kind, path) in enumerate(items, 1):
        if kind == "pdf":
            row = _do_pdf(path, commit, overwrite, force, stats, sources)
            if sleep and row["source"] and row["status"] != "EXISTS_SKIP":
                time.sleep(sleep)
        else:
            row = _do_text_only(path, commit, overwrite, force, stats, sources)
        if not row["status"].startswith("DRY:") and row["status"] not in ("WROTE", "KEPT_CURATED"):
            stats[row["status"]] += 1
        rows.append(row)
        extra = f" ({row['source'] or row['doi_from']})" if row["source"] or row["doi_from"] else ""
        where = f"  {row['path']}" if row["status"] == "NO_DOI" else ""
        why = f"  {row['detail']}" if row["detail"] and row["status"] != "WROTE" else ""
        print(f"  [{i:3d}/{len(items)}] {path.name[:60]:<60} -> {row['status']:<16}{extra}{where}{why}")

    report = None
    if commit:
        report = lib / REPORT_NAME
        lit_util.atomic_write_csv(str(report), rows, fieldnames=REPORT_FIELDS)
        print(f"\n  report: {report}")

    # legacy summary keys first, then every status and source seen
    summary = {"skip_existing": stats["EXISTS_SKIP"], "wrote": stats["WROTE"],
               "kept_curated": stats["KEPT_CURATED"], "doi_sidecar": stats["doi_sidecar"],
               "doi_pdf": stats["doi_pdf"], "no_doi": stats["NO_DOI"],
               "crossref_fail": stats["RESOLVE_FAIL"], "meta_unavailable": stats["META_UNAVAILABLE"],
               "pdf_error": stats["PDF_ERROR"], "identity_flag": stats["IDENTITY_FLAG"],
               "unsupported_ra": stats["UNSUPPORTED_RA"]}
    print("\n== summary ==")
    for k, v in summary.items():
        print(f"  {k:<18} {v}")
    for k in sorted(stats):
        if k not in ("EXISTS_SKIP", "WROTE", "KEPT_CURATED", "NO_DOI", "RESOLVE_FAIL", "META_UNAVAILABLE",
                     "PDF_ERROR", "IDENTITY_FLAG", "UNSUPPORTED_RA", "doi_sidecar", "doi_pdf"):
            print(f"  {k:<18} {stats[k]}")
    if sources:
        print("  sources:           " + ", ".join(f"{k}={v}" for k, v in sorted(sources.items())))
    return {"lib_dir": str(lib), "mode": mode, "rows": rows, "stats": dict(stats), "summary": summary,
            "sources": dict(sources), "report": str(report) if report else None}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    ap.add_argument("--lib-dir", default=None,
                    help="Library directory (PDFs and sidecars). This or --project is required.")
    ap.add_argument("--project", default=None,
                    help="A project key registered in projects.json; its library is resolved with "
                         "lit_util.lib_paths. This or --lib-dir is required.")
    ap.add_argument("--commit", action="store_true",
                    help="Actually write .ris files. Default is dry-run.")
    ap.add_argument("--overwrite", action="store_true",
                    help="Replace existing .ris files the pipeline wrote and nobody edited since "
                         "(DEC-29); curated or edited files are kept and counted KEPT_CURATED.")
    ap.add_argument("--force", action="store_true",
                    help="Replace any existing .ris, curated or edited ones too (implies --overwrite).")
    ap.add_argument("--include-text-only", action="store_true",
                    help="Also write a .ris from the sidecar's own metadata for each text-only holding "
                         "(a .fulltext.json with text and no PDF; DEC-08).")
    ap.add_argument("--limit", type=int, default=0,
                    help="Process first N holdings only (testing).")
    ap.add_argument("--sleep", type=float, default=0.0,
                    help="Extra seconds after each PDF's metadata lookup, on top of litpipe.net's "
                         "per-host pacing. Default 0.")
    args = ap.parse_args(argv)
    try:
        run(lib_dir=args.lib_dir, project=args.project, commit=args.commit, overwrite=args.overwrite,
            force=args.force, limit=args.limit, sleep=args.sleep, include_text_only=args.include_text_only)
    except UsageError as e:
        print(str(e), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
