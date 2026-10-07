"""Stand-in for sweep.run: the same result shape and run artifacts, no fetch stage, no request.

Spec (fake_stages.json "sweep" -> project): classes {doi: residual class} (default_class
"fetched"), retire (default true), keep_reason, exit_code (default 0), refused [queue names],
stages {stage: status}, plus the generic behaviours of w4a_fake_common.behave (sleep,
started_marker, ...), applied before any artifact is written."""
import csv
import io
from collections import Counter

import w4a_fake_common as common

FIELDS = ["doi", "title", "authors", "year", "destination", "notes", "residual_class", "reason", "run_id"]


def _write(path, rows, fields):
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=fields, lineterminator="\n", extrasaction="ignore")
    w.writeheader()
    w.writerows(rows)
    path.write_text(buf.getvalue(), encoding="utf-8")


def run(project=None, dry_run=False, skip_preprint=False, date=None, loose_ends=True, migrate=False,
        artifact_dir=None, allow_destination=False, candidate_order=None, sources=None):
    common.log("sweep", project=project, date=date, loose_ends=loose_ends, candidate_order=candidate_order,
               skip_preprint=skip_preprint, sources=sources)
    s = common.spec("sweep", project)
    common.behave(s)
    import lit_util
    import sweep as S
    entry = (common.registry().get("projects") or {}).get(project) or {}
    root = lit_util.project_root(project, entry)
    queues = S.discover_queues(root)
    if not queues:
        return {"exit_code": 1, "projects": {}, "ignored": [], "admitted": {}}
    # the run's artifacts go where the real sweep puts them (projects.json artifact_dir, W5)
    adir = S.project_artifact_dir(project, root, artifact_dir, common.registry())
    adir.mkdir(parents=True, exist_ok=True)
    run_id = S.choose_run_id(S.run_id_dirs(root, adir), date)
    classes_of = {k.lower(): v for k, v in (s.get("classes") or {}).items()}
    default = s.get("default_class", "fetched")
    retire = s.get("retire", True)
    srcs = S.project_sources(project, common.registry(), sources)    # DEC-31, as the real sweep reads it
    stages = {"unpaywall": "completed" if "unpaywall" in srcs else "skipped",
              "pmc": "completed" if "pmc" in srcs else "skipped", "preprint": "skipped", "extract": "completed",
              **(s.get("stages") or {})}
    results = []
    for q in queues:
        tag = S.queue_tag(q.name) or ""
        _, rows = S.read_queue(q)

        def art(stage):
            return adir / S.artifact_name(tag, run_id, stage)
        counts = Counter()
        residual = []
        upw = []
        for r in rows:
            cls = classes_of.get((r.get("doi") or "").strip().lower(), default)
            counts[cls] += 1
            upw.append({"doi": r.get("doi"), "downloaded": str(cls == "fetched"),
                        "oa_status": "OA" if cls == "fetched" else "CLOSED"})
            if cls != "fetched":
                residual.append({**r, "residual_class": cls, "reason": "stand-in", "run_id": run_id})
        if stages.get("unpaywall") in ("completed", "failed"):
            _write(art("unpaywall"), upw, ["doi", "downloaded", "oa_status"])
        if stages.get("pmc") in ("completed", "failed"):
            _write(art("pmc"), [{"doi": u["doi"], "downloaded": "False"} for u in upw], ["doi", "downloaded"])
        _write(art("residual"), residual, FIELDS)
        _write(art("report"), [{"section": "class", "name": c, "count": n} for c, n in counts.items()],
               ["section", "name", "count"])
        n_fetched = counts.get("fetched", 0)
        res = {"rows": len(rows), "downloaded": n_fetched, "unpaywall": n_fetched, "pmc": 0, "preprint": 0,
               "skip_exists": 0, "report": str(art("report")), "residual": str(art("residual")),
               "processed": None, "partial": not retire, "retired": retire, "run_id": run_id, "tag": tag,
               "queue": q.name, "stages": dict(stages),
               "failed_stages": [k for k, v in stages.items() if v == "failed"],
               "classes": dict(counts), "config_error": False,
               "keep_reason": "" if retire else s.get("keep_reason", "pmc failed")}
        if retire:
            q.rename(art("processed"))
            res["processed"] = str(art("processed"))
        results.append(res)
    return {"exit_code": s.get("exit_code", 0),
            "projects": {project: {"run_id": run_id, "results": results, "refused": list(s.get("refused") or []),
                                   "loose_end": None, "migrate": None}},
            "ignored": [], "admitted": {}}
