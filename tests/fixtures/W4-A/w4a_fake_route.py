"""Stand-in for migrate_closed_to_md.run: writes the run's routing CSV per chain, sends nothing.

Spec "route" -> project: status (default: ok when a residual chain exists, else nothing), plus
the generic behaviours."""
import csv
import io

import w4a_fake_common as common


def run(project, run_id=None, tags=None, dry_run=False, skip_preprint=False, use_holdings=True, holdmap=None,
        cfg=None, today=None, artifact_dir=None):
    common.log("route", project=project, run_id=run_id, tags=tags, skip_preprint=skip_preprint)
    s = common.spec("route", project)
    common.behave(s)
    import lit_util
    import sweep as S
    entry = (common.registry().get("projects") or {}).get(project) or {}
    root = lit_util.project_root(project, entry)
    written, n = {}, 0
    for p in sorted(root.glob("lit_pull_queue*.residual.csv")):
        a = S.parse_artifact(p.name)
        if not a or a.run_id != run_id or (tags and a.tag not in tags):
            continue
        with open(p, encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f))
        out = root / S.artifact_name(a.tag, run_id, "routing")
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=["doi", "residual_class", "route"], lineterminator="\n",
                           extrasaction="ignore")
        w.writeheader()
        w.writerows({"doi": r.get("doi"), "residual_class": r.get("residual_class"), "route": "stand-in"}
                    for r in rows)
        out.write_text(buf.getvalue(), encoding="utf-8")
        written[out.name] = len(rows)
        n += len(rows)
    status = s.get("status") or ("ok" if written else "nothing")
    return {"status": status, "project": project, "run_id": run_id, "chains": [], "counts": {}, "written": written,
            "rows": n, "dry_run": dry_run, **({"error": s["error"]} if s.get("error") else {})}
