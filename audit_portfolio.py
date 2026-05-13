"""Portfolio-wide audit of literature-pipeline outputs.

Walks each project under Projects/ that has a recognizable literature
directory and produces a per-project report card. Surfaces symptoms of
breakage (not just config-conformance):

  - PDF integrity: count, %PDF magic-byte check, suspiciously small files (<10KB)
  - Sidecar integrity: parse JSON, check for content (text or abstract present)
  - Pairing: orphan sidecars (no matching PDF), PDFs without sidecars
  - .ris coverage: how many PDFs have a .ris sidecar (after our recent backfill)
  - Filename quality: count "_al_" antipattern (name-extraction bug from
    "Smith et al." misparsing as authors)
  - Queue history: failed-fetch rows in lit_pull_queue.*.report.csv that
    were never retried successfully

Usage:
  python audit_portfolio.py                  # Audit all projects under ~/Projects
  python audit_portfolio.py --project getpaid  # Single project
  python audit_portfolio.py --json out.json    # Machine-readable output
"""
import os, sys, io, csv, json, argparse, re
from pathlib import Path

try:
    if getattr(sys.stdout, "encoding", "").lower() != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, OSError):
    pass

PROJECTS_ROOT = Path(os.path.expanduser("~/Projects"))
CONFIG_PATH = Path(__file__).parent / "projects.json"


def load_config():
    """Load projects.json. Returns dict[name -> {tier, lib_dir, data_dir?, parent?, active}]."""
    from ris_emit import load_projects_config
    return load_projects_config(CONFIG_PATH).get("projects", {})


def discover_libs():
    """Read projects.json and return [(project_name, lib_path, tier, data_path|None)]."""
    cfg = load_config()
    libs = []
    for name, p in cfg.items():
        if not p.get("active", True): continue
        # Subproject? Use parent as the project root, append lib_dir to that.
        parent = p.get("parent")
        if parent:
            root = PROJECTS_ROOT / parent
        else:
            root = PROJECTS_ROOT / name
        lp = root / p["lib_dir"]
        if not lp.is_dir():
            print(f"[WARN] {name}: lib_dir not found ({lp}); skipping")
            continue
        dp = (root / p["data_dir"]) if p.get("data_dir") else None
        libs.append((name, lp, p.get("tier", 2), dp))
    return libs


def audit_lib(name: str, lib: Path) -> dict:
    pdfs     = sorted(p for p in lib.iterdir() if p.is_file() and p.suffix.lower() == ".pdf")
    sidecars = sorted(p for p in lib.iterdir() if p.is_file() and p.name.endswith(".fulltext.json"))
    risfiles = sorted(p for p in lib.iterdir() if p.is_file() and p.suffix.lower() == ".ris")

    pdf_stems     = {p.stem for p in pdfs}
    sidecar_stems = {p.name[:-len(".fulltext.json")] for p in sidecars}
    ris_stems     = {p.stem for p in risfiles}

    # PDF integrity
    fake_pdfs = []
    tiny_pdfs = []
    for p in pdfs:
        try:
            with open(p, "rb") as f: magic = f.read(4)
            if magic != b"%PDF": fake_pdfs.append(p.name)
            elif p.stat().st_size < 10_000: tiny_pdfs.append((p.name, p.stat().st_size))
        except OSError as e:
            fake_pdfs.append(f"{p.name} ({e})")

    # Sidecar integrity
    bad_sidecars = []  # (name, reason)
    empty_sidecars = []
    for s in sidecars:
        try:
            with open(s, encoding="utf-8") as fh:
                d = json.load(fh)
            if not (d.get("text") or d.get("abstract") or d.get("body")):
                empty_sidecars.append(s.name)
        except (OSError, json.JSONDecodeError, UnicodeDecodeError) as e:
            bad_sidecars.append((s.name, str(e)[:80]))

    # Pairing
    orphan_sidecars = sidecar_stems - pdf_stems
    pdfs_no_sidecar = pdf_stems - sidecar_stems
    pdfs_no_ris     = pdf_stems - ris_stems
    orphan_ris      = ris_stems - pdf_stems

    # Filename quality
    al_antipattern = sorted(p.name for p in pdfs if re.search(r"_al_", p.name))
    unknown_year   = sorted(p.name for p in pdfs if p.name.startswith("Unknown_"))
    upper_lastname = sorted(p.name for p in pdfs
                            if re.match(r"^\d{4}_[A-Z]{2,}_", p.name))[:5]  # cap

    return {
        "project":       name,
        "lib":           str(lib),
        "n_pdfs":        len(pdfs),
        "n_sidecars":    len(sidecars),
        "n_ris":         len(risfiles),
        "fake_pdfs":     fake_pdfs,
        "tiny_pdfs":     tiny_pdfs,
        "bad_sidecars":  bad_sidecars,
        "empty_sidecars": empty_sidecars,
        "orphan_sidecars": sorted(orphan_sidecars),
        "pdfs_no_sidecar": len(pdfs_no_sidecar),
        "pdfs_no_ris":   len(pdfs_no_ris),
        "orphan_ris":    sorted(orphan_ris),
        "al_antipattern": al_antipattern,
        "unknown_year":  unknown_year,
        "upper_lastname_sample": upper_lastname,
    }


def audit_queue(proj: Path) -> dict:
    """Look at processed lit_pull_queue reports for failed fetches."""
    reports = sorted(proj.glob("lit_pull_queue.*.report.csv"))
    if not reports:
        return {"reports": 0, "failures": [], "latest": None}
    # Use latest (lexicographic = chronological since dates are ISO)
    latest = reports[-1]
    failures = []
    try:
        with open(latest, encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                status = (r.get("status") or "").lower()
                if status not in ("ok", "skip_exists", "downloaded", "true", ""):
                    failures.append({"doi": r.get("doi", ""),
                                      "status": status,
                                      "title": (r.get("title") or "")[:60]})
    except (OSError, csv.Error, UnicodeDecodeError) as e:
        return {"reports": len(reports), "failures": [],
                "latest": latest.name, "error": str(e)[:80]}
    return {"reports": len(reports), "failures": failures, "latest": latest.name}


def doi_from_sources(stem: Path, pdf: Path) -> dict:
    """Get DOI from sidecar, ris, and PDF text. Return all three for comparison."""
    out = {"sidecar": "", "ris": "", "pdf": ""}
    sc = stem.with_suffix(".fulltext.json")
    if sc.exists():
        try:
            with open(sc, encoding="utf-8") as fh:
                d = json.load(fh)
            out["sidecar"] = (d.get("doi") or "").lower()
        except (OSError, json.JSONDecodeError, UnicodeDecodeError): pass
    rp = stem.with_suffix(".ris")
    if rp.exists():
        try:
            with open(rp, encoding="utf-8") as fh:
                for line in fh:
                    m = re.match(r"^DO\s{2}-\s?(.*)$", line)
                    if m: out["ris"] = m.group(1).strip().lower(); break
        except (OSError, UnicodeDecodeError): pass
    return out


def deep_audit_lib(lib: Path) -> dict:
    """Deeper checks: DOI consistency + sidecar text length + filename alignment."""
    pdfs = sorted(p for p in lib.iterdir() if p.is_file() and p.suffix.lower() == ".pdf")
    sidecar_lens = []        # (stem, len)
    doi_mismatch = []        # (stem, sources)
    fn_misalign = []         # (filename, expected_lastname, expected_year)
    doi_to_files = {}        # doi -> [(project_lib, filename)]

    for pdf in pdfs:
        stem = pdf.with_suffix("")
        # Sidecar text length
        sc = stem.with_suffix(".fulltext.json")
        if sc.exists():
            try:
                with open(sc, encoding="utf-8") as fh:
                    d = json.load(fh)
                txt = (d.get("text") or "") + (d.get("body") or "") + (d.get("abstract") or "")
                sidecar_lens.append((pdf.name, len(txt)))
            except (OSError, json.JSONDecodeError, UnicodeDecodeError): pass

        # DOI consistency across sidecar + ris
        dois = doi_from_sources(stem, pdf)
        non_empty = {k: v for k, v in dois.items() if v}
        if len(set(non_empty.values())) > 1:
            doi_mismatch.append((pdf.name, dois))
        # Track DOI for cross-project overlap
        canonical_doi = next(iter(non_empty.values()), "")
        if canonical_doi:
            doi_to_files.setdefault(canonical_doi, []).append(pdf.name)

        # Filename alignment with sidecar/ris metadata
        m = re.match(r"^(\d{4})_([A-Za-z][\w\-]*)_", pdf.name)
        if m and (sc.exists() or stem.with_suffix(".ris").exists()):
            fn_year, fn_last = m.group(1), m.group(2)
            rp = stem.with_suffix(".ris")
            if rp.exists():
                with open(rp, encoding="utf-8") as fh:
                    ris = fh.read()
                py = re.search(r"^PY\s{2}-\s?(\d{4})", ris, re.M)
                au = re.search(r"^AU\s{2}-\s?([^,\n]+)", ris, re.M)
                exp_year = py.group(1) if py else ""
                exp_last_raw = au.group(1).strip() if au else ""
                # Compare against ASCII-normalized RIS lastname so canonical
                # filenames (Periard, Luthi, Kukic) don't false-alarm against
                # their Unicode source forms (Périard, Lüthi, Kukić).
                # Also strip non-word chars (St. Pierre → StPierre).
                from ris_emit import safe_ascii
                exp_last = re.sub(r"[^\w\-]", "", safe_ascii(exp_last_raw))
                if exp_year and fn_year and abs(int(exp_year) - int(fn_year)) > 1:
                    fn_misalign.append((pdf.name, "year", f"file={fn_year} ris={exp_year}"))
                elif exp_last and fn_last and fn_last.lower() != exp_last.lower():
                    if not exp_last.lower().startswith(fn_last.lower()):
                        fn_misalign.append((pdf.name, "lastname", f"file={fn_last} ris={exp_last_raw}"))

    return {
        "sidecar_lens":  sidecar_lens,
        "doi_mismatch":  doi_mismatch,
        "fn_misalign":   fn_misalign,
        "doi_to_files":  doi_to_files,
    }


def fmt_report(audit: dict, queue: dict) -> str:
    lines = []
    name = audit["project"]; tier = audit.get("tier", "?")
    lines.append(f"\n{'='*72}")
    lines.append(f"  {name}  [Tier {tier}]")
    lines.append(f"  lib: {audit['lib']}")
    lines.append('='*72)

    # Headline counts
    n_pdf = audit["n_pdfs"]; n_sc = audit["n_sidecars"]; n_ris = audit["n_ris"]
    pct_sc  = (100*n_sc/n_pdf) if n_pdf else 0
    pct_ris = (100*n_ris/n_pdf) if n_pdf else 0
    lines.append(f"  PDFs:     {n_pdf}")
    lines.append(f"  sidecars: {n_sc} ({pct_sc:.0f}%)")
    lines.append(f"  .ris:     {n_ris} ({pct_ris:.0f}%)")

    # PDF integrity
    if audit["fake_pdfs"]:
        lines.append(f"  FAIL fake PDFs (no %PDF magic): {len(audit['fake_pdfs'])}")
        for f in audit["fake_pdfs"][:3]: lines.append(f"     {f}")
    if audit["tiny_pdfs"]:
        lines.append(f"  WARN tiny PDFs (<10KB): {len(audit['tiny_pdfs'])}")
        for f, sz in audit["tiny_pdfs"][:3]: lines.append(f"     {f} ({sz}B)")

    # Sidecars
    if audit["bad_sidecars"]:
        lines.append(f"  FAIL unparseable sidecars: {len(audit['bad_sidecars'])}")
        for n, e in audit["bad_sidecars"][:3]: lines.append(f"     {n}: {e}")
    if audit["empty_sidecars"]:
        lines.append(f"  WARN empty-content sidecars: {len(audit['empty_sidecars'])}")
        for n in audit["empty_sidecars"][:3]: lines.append(f"     {n}")
    if audit["orphan_sidecars"]:
        lines.append(f"  WARN orphan sidecars (no PDF): {len(audit['orphan_sidecars'])}")
        for n in audit["orphan_sidecars"][:3]: lines.append(f"     {n}")
    if audit["pdfs_no_sidecar"]:
        lines.append(f"  INFO PDFs without sidecar: {audit['pdfs_no_sidecar']}")
    if audit["pdfs_no_ris"]:
        lines.append(f"  INFO PDFs without .ris:    {audit['pdfs_no_ris']}")
    if audit["orphan_ris"]:
        lines.append(f"  WARN orphan .ris (no PDF): {len(audit['orphan_ris'])}")
        for n in audit["orphan_ris"][:3]: lines.append(f"     {n}")

    # Filename quality
    if audit["al_antipattern"]:
        lines.append(f"  WARN '_al_' antipattern: {len(audit['al_antipattern'])} (audit_filenames.py can fix)")
        for n in audit["al_antipattern"][:3]: lines.append(f"     {n}")
    if audit["unknown_year"]:
        lines.append(f"  WARN Unknown_ prefix: {len(audit['unknown_year'])}")
    if audit["upper_lastname_sample"]:
        lines.append(f"  INFO ALLCAPS lastnames (CrossRef quirk): sample = {len(audit['upper_lastname_sample'])}")
        for n in audit["upper_lastname_sample"]: lines.append(f"     {n}")

    # Queue
    if queue["reports"]:
        lines.append(f"  queue history: {queue['reports']} processed report(s); latest={queue['latest']}")
        if queue["failures"]:
            lines.append(f"  WARN queue failures (latest report): {len(queue['failures'])}")
            for f in queue["failures"][:5]:
                lines.append(f"     {f['status']:<14} {f['doi']:<40} {f['title']}")
    else:
        lines.append(f"  queue history: none (project has lib but no lit_pull_queue used)")

    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default=None,
                    help="Audit only this project (name).")
    ap.add_argument("--json", default=None,
                    help="Write machine-readable output to this path.")
    args = ap.parse_args()

    libs = discover_libs()
    if args.project:
        libs = [t for t in libs if t[0] == args.project or t[0].endswith("/"+args.project)]

    print(f"Auditing {len(libs)} libraries (config: {CONFIG_PATH.name})\n")
    out = []
    portfolio_doi = {}  # doi -> [(project, filename)]
    for name, lib, tier, data_dir in libs:
        a = audit_lib(name, lib)
        a["tier"] = tier
        deep = deep_audit_lib(lib)
        a["deep"] = deep
        for doi, files in deep["doi_to_files"].items():
            for fn in files:
                portfolio_doi.setdefault(doi, []).append((name, fn))
        proj_root = PROJECTS_ROOT / name.split("/")[0]
        q = audit_queue(proj_root)
        out.append({"audit": a, "queue": q})
        print(fmt_report(a, q))

    # Portfolio summary
    print(f"\n{'='*72}\n  PORTFOLIO SUMMARY\n{'='*72}")
    total_pdfs = sum(o["audit"]["n_pdfs"] for o in out)
    total_sc   = sum(o["audit"]["n_sidecars"] for o in out)
    total_ris  = sum(o["audit"]["n_ris"] for o in out)
    print(f"  total libraries: {len(out)}")
    print(f"  total PDFs:      {total_pdfs}")
    print(f"  total sidecars:  {total_sc} ({100*total_sc/max(1,total_pdfs):.0f}%)")
    print(f"  total .ris:      {total_ris} ({100*total_ris/max(1,total_pdfs):.0f}%)")
    flags = []
    for o in out:
        a = o["audit"]
        if a["fake_pdfs"] or a["bad_sidecars"]:
            flags.append(f"BREAK: {a['project']}")
        elif (a["empty_sidecars"] or a["orphan_sidecars"] or a["tiny_pdfs"]
              or a["al_antipattern"] or a["unknown_year"]):
            flags.append(f"WARN:  {a['project']}")
    if flags:
        print(f"  flagged projects:")
        for f in flags: print(f"    {f}")
    else:
        print(f"  no projects flagged")

    # Deep findings rollup
    print(f"\n=== DEEP CHECKS ===")
    # 1. Cross-project DOI overlap
    overlaps = {d: locs for d, locs in portfolio_doi.items()
                if len({p for p,_ in locs}) > 1}
    print(f"  cross-project DOI overlap: {len(overlaps)} DOIs in >1 project")
    for d, locs in sorted(overlaps.items())[:10]:
        print(f"     {d}")
        for p, fn in locs: print(f"       [{p}] {fn}")
    if len(overlaps) > 10: print(f"     ... +{len(overlaps)-10} more")

    # 2. Same-project filename duplicates (different stems, same DOI)
    same_proj_dups = {}
    for d, locs in portfolio_doi.items():
        by_proj = {}
        for p, fn in locs: by_proj.setdefault(p, []).append(fn)
        for p, fns in by_proj.items():
            if len(fns) > 1:
                same_proj_dups[(d, p)] = fns
    print(f"\n  same-project DOI dupes: {len(same_proj_dups)}")
    for (d, p), fns in list(same_proj_dups.items())[:5]:
        print(f"     {d} in [{p}]: {fns}")

    # 3. Sidecar length distribution
    all_lens = [(o["audit"]["project"], n, l) for o in out
                for n, l in o["audit"]["deep"]["sidecar_lens"]]
    if all_lens:
        all_lens_sorted = sorted(all_lens, key=lambda x: x[2])
        very_short = [t for t in all_lens if t[2] < 1000]
        print(f"\n  sidecar text-length distribution (n={len(all_lens)}):")
        if all_lens_sorted:
            print(f"     min={all_lens_sorted[0][2]} median={all_lens_sorted[len(all_lens)//2][2]} max={all_lens_sorted[-1][2]}")
        print(f"     <1000 chars: {len(very_short)} (likely empty/stub)")
        for proj, fn, l in very_short[:5]:
            print(f"       [{proj}] {fn}: {l} chars")

    # 4. Filename misalignments (file year/lastname disagrees with ris metadata)
    all_misalign = [(o["audit"]["project"], *m) for o in out
                    for m in o["audit"]["deep"]["fn_misalign"]]
    print(f"\n  filename ↔ metadata mismatches: {len(all_misalign)}")
    for proj, fn, kind, detail in all_misalign[:10]:
        print(f"     [{proj}] [{kind}] {fn}: {detail}")
    if len(all_misalign) > 10: print(f"     ... +{len(all_misalign)-10} more")

    # 5. DOI mismatches within a single PDF's metadata (sidecar vs ris)
    all_doi_mis = [(o["audit"]["project"], *m) for o in out
                   for m in o["audit"]["deep"]["doi_mismatch"]]
    print(f"\n  intra-file DOI mismatches (sidecar≠ris): {len(all_doi_mis)}")
    for proj, fn, dois in all_doi_mis[:5]:
        print(f"     [{proj}] {fn}: {dois}")

    if args.json:
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2, default=str, ensure_ascii=False)
        print(f"\n  JSON written: {args.json}")


if __name__ == "__main__":
    main()
