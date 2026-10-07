"""Stand-in for backfill_ris.run: the same result shape, no lookup, nothing written unless the spec says.

Spec "ris" -> project: summary {key: n} and stats {status: n} merged into the result, plus the
generic behaviours."""
import w4a_fake_common as common


def run(*, lib_dir=None, project=None, commit=False, overwrite=False, force=False, limit=0, sleep=0.0,
        include_text_only=False, cfg=None):
    common.log("ris", project=project, commit=commit, include_text_only=include_text_only, limit=limit)
    s = common.spec("ris", project)
    common.behave(s)
    summary = {"skip_existing": 0, "wrote": 0, "kept_curated": 0, "doi_sidecar": 0, "doi_pdf": 0, "no_doi": 0,
               "crossref_fail": 0, "meta_unavailable": 0, "pdf_error": 0, "identity_flag": 0, "unsupported_ra": 0,
               **(s.get("summary") or {})}
    return {"lib_dir": str(lib_dir or ""), "mode": "COMMIT" if commit else "DRY-RUN", "rows": [],
            "stats": dict(s.get("stats") or {}), "summary": summary, "sources": {}, "report": None}
