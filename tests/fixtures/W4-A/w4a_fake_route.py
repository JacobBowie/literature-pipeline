"""Stand-in for migrate_closed_to_md.run. By default it IS the real migrate (W4b verifier N-3: a
stand-in route that wrote routing CSVs only for chains with rows hid a false lost_artifacts alarm):
migrate_closed_to_md.run on the temp registry, holdings off, so it reads the stand-in sweep's typed
residual CSVs, writes the worklists and a routing CSV per chain (header-only for a chain with nothing
to route), and sends nothing.

Spec "route" -> project: a forced `status` (and `error`) replaces the real migrate with the old
stand-in (a routing CSV per residual chain, that status); plus the generic behaviours."""
import csv
import io

import w4a_fake_common as common


def run(project, run_id=None, tags=None, dry_run=False, skip_preprint=False, use_holdings=True, holdmap=None,
        cfg=None, today=None, artifact_dir=None, sources=None):
    common.log("route", project=project, run_id=run_id, tags=tags, skip_preprint=skip_preprint, sources=sources)
    s = common.spec("route", project)
    common.behave(s)
    if not (s.get("status") or s.get("error")):
        import migrate_closed_to_md as M
        return M.run(project, run_id=run_id, tags=tags, dry_run=dry_run, skip_preprint=skip_preprint,
                     use_holdings=False, cfg=common.registry(), today=today, artifact_dir=artifact_dir,
                     sources=sources)
    import lit_util
    import sweep as S
    entry = (common.registry().get("projects") or {}).get(project) or {}
    root = lit_util.project_root(project, entry)
    adir = S.project_artifact_dir(project, root, artifact_dir, common.registry())
    written, n = {}, 0
    for p in sorted(adir.glob("lit_pull_queue*.residual.csv")):
        a = S.parse_artifact(p.name)
        if not a or a.run_id != run_id or (tags and a.tag not in tags):
            continue
        with open(p, encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f))
        out = adir / S.artifact_name(a.tag, run_id, "routing")
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
