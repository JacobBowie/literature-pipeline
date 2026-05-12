"""End-to-end fault check for the literature pipeline.

Two tiers (declared in projects.json):
  - Tier 1 (full pipeline): discovery → fetch → tables → text → reports.
    Used by systematic-review projects with `data_dir` set (e.g. getpaid).
  - Tier 2 (library-only):  just PDFs + .fulltext.json + .ris sidecars.
    Used by projects that just need a place to drop related papers.

Stages run by tier:
  Tier 1: 1 (discovery) → 7 (cross-checks) + tools sanity. All checks active.
  Tier 2: 3 (library state) + 4 (sidecar integrity) + 7-light (orphan sidecars,
          orphan .ris) + tools sanity. Skips discovery/triage/tables/text checks.

Usage:
  # By project name (loads layout from projects.json)
  python pipeline_check.py --project getpaid
  python pipeline_check.py --project Physiological_Data

  # Explicit paths (legacy mode, useful for projects not yet in registry)
  python pipeline_check.py --base-dir /path/to/proj --lib-dir docs/literature
"""
import sys, io, os, csv, json, argparse
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')


PROJECTS_ROOT = Path(os.path.expanduser("~/Projects"))
CONFIG_PATH   = Path(__file__).parent / "projects.json"


def resolve_from_config(name: str):
    """Look up project in projects.json. Returns (base, lib, data_or_None, tier)."""
    if not CONFIG_PATH.exists():
        print(f"[ERR] projects.json not found at {CONFIG_PATH}", file=sys.stderr)
        sys.exit(2)
    with open(CONFIG_PATH, encoding="utf-8") as f:
        cfg = json.load(f).get("projects", {})
    if name not in cfg:
        print(f"[ERR] project '{name}' not in projects.json. "
              f"Known: {', '.join(sorted(cfg.keys()))}", file=sys.stderr)
        sys.exit(2)
    p = cfg[name]
    parent = p.get("parent")
    base = PROJECTS_ROOT / (parent or name)
    lib  = base / p["lib_dir"]
    data = (base / p["data_dir"]) if p.get("data_dir") else None
    tier = p.get("tier", 2)
    return base, lib, data, tier


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default=None,
                     help="Project name from projects.json (e.g. 'getpaid', 'Physiological_Data').")
    ap.add_argument("--base-dir", default=None,
                     help="(legacy) Project root. Used if --project not given.")
    ap.add_argument("--lib-dir", default="references/literature",
                     help="(legacy) PDF + sidecar directory, relative to --base-dir.")
    ap.add_argument("--data-dir", default="data/prior_art",
                     help="(legacy) Build-artifact directory, relative to --base-dir.")
    ap.add_argument("--tier", type=int, default=None,
                     help="(legacy) Override tier (1 or 2). Inferred from --project if used.")
    args = ap.parse_args()

    if args.project:
        base, lib, data, tier = resolve_from_config(args.project)
    else:
        base = Path(args.base_dir or os.getcwd()).resolve()
        lib  = base / args.lib_dir
        data = base / args.data_dir
        tier = args.tier or 1   # legacy default: assume full pipeline

    print(f"Project base: {base}")
    print(f"Tier:         {tier}")
    print(f"Library:      {lib}")
    if data: print(f"Data dir:     {data}")
    print()

    issues = []
    def check(label, ok, detail=""):
        icon = "OK" if ok else "FAIL"
        print(f"  [{icon}] {label}{(' — '+detail) if detail else ''}")
        if not ok: issues.append(label)

    # ---------- Stage 1: Discovery (Tier 1 only) ----------
    if tier == 1 and data:
        print("\n=== Stage 1: Discovery ===")
        triage = data / "discovered/triage_not_in_library.csv"
        check("triage_not_in_library.csv exists", triage.exists())
        if triage.exists():
            with open(triage, encoding="utf-8") as fh:
                n_rows = sum(1 for _ in csv.DictReader(fh))
            with open(triage, encoding="utf-8") as fh:
                n_with_doi = sum(1 for r in csv.DictReader(fh) if r.get("doi"))
            check("triage parses + has DOIs", n_rows > 0 and n_with_doi > 0,
                  detail=f"{n_rows} rows, {n_with_doi} with DOI")

        # ---------- Stage 2: Fetch reports ----------
        print("\n=== Stage 2: Fetch reports ===")
        unpw = data / "discovered/unpaywall_fetch_report_v2.csv"
        pmc  = data / "discovered/pmc_fetch_report.csv"
        ppr  = data / "discovered/preprint_fetch_report.csv"
        for p, label in [(unpw, "unpaywall_v2"), (pmc, "pmc"), (ppr, "preprint")]:
            if p.exists():
                with open(p, encoding="utf-8") as fh:
                    rows = list(csv.DictReader(fh))
                n_dl = sum(1 for r in rows if r.get("downloaded", "").lower() == "true")
                check(f"{label}: {len(rows)} rows, {n_dl} downloaded", len(rows) > 0)
            else:
                check(f"{label} report exists", False)
    elif tier == 2:
        print("\n[Tier 2: skipping Stages 1-2 (discovery, fetch reports)]")

    # ---------- Stage 3: Library state ----------
    print("\n=== Stage 3: Library state ===")
    pdfs = sorted(p.name for p in lib.glob("*.pdf")) if lib.is_dir() else []
    sidecars = sorted(p.name for p in lib.glob("*.fulltext.json")) if lib.is_dir() else []
    check("library exists", lib.is_dir(),
          detail=f"{len(pdfs)} PDFs, {len(sidecars)} sidecars")
    for p in pdfs[:3]:
        with open(lib / p, "rb") as f:
            magic = f.read(4)
        check(f"  {p[:55]} is real PDF", magic == b"%PDF")

    # ---------- Stage 4: Sidecar integrity ----------
    print("\n=== Stage 4: Sidecar integrity ===")
    bad_sidecars = []
    for s in sidecars:
        try:
            with open(lib / s, encoding="utf-8") as fh:
                d = json.load(fh)
            if not d.get("text") and not d.get("abstract"):
                bad_sidecars.append((s, "empty text+abstract"))
        except (OSError, json.JSONDecodeError, UnicodeDecodeError) as e:
            bad_sidecars.append((s, str(e)[:50]))
    check(f"all {len(sidecars)} sidecars parse + have content", not bad_sidecars,
          detail="" if not bad_sidecars else f"{len(bad_sidecars)} bad")
    for s, err in bad_sidecars[:3]:
        print(f"      bad: {s} ({err})")

    # ---------- Stage 4b: .ris coverage (informational, all tiers) ----------
    print("\n=== Stage 4b: .ris coverage ===")
    ris_files = sorted(p.name for p in lib.glob("*.ris")) if lib.is_dir() else []
    pdf_stems_set = {n[:-4] for n in pdfs}
    ris_stems = {n[:-4] for n in ris_files}
    pdfs_with_ris = pdf_stems_set & ris_stems
    pct = (100*len(pdfs_with_ris)/len(pdfs)) if pdfs else 0
    check(f"{len(pdfs_with_ris)}/{len(pdfs)} PDFs have .ris ({pct:.0f}%)", True)
    orphan_ris = ris_stems - pdf_stems_set
    check("no orphan .ris (no PDF)", not orphan_ris,
          detail="" if not orphan_ris else f"{len(orphan_ris)} orphans")

    # ---------- Stage 5-6: Tables + build artifacts (Tier 1 only) ----------
    text_files = []
    if tier == 1 and data:
        print("\n=== Stage 5: Tables ===")
        tables_dir = data / "tables"
        table_report = tables_dir / "_extraction_report.csv"
        check("tables/_extraction_report.csv exists", table_report.exists())
        if table_report.exists():
            with open(table_report, encoding="utf-8") as fh:
                rows = list(csv.DictReader(fh))
            n_with_t = sum(1 for r in rows if int(r.get("n_tables", "0") or 0) > 0)
            n_total_t = sum(int(r.get("n_tables", "0") or 0) for r in rows)
            check(f"  {n_with_t}/{len(rows)} papers with tables, {n_total_t} total",
                  len(rows) > 0)

        print("\n=== Stage 6: Library build artifacts ===")
        text_dir = data / "text"
        meta = data / "metadata.csv"
        abst = data / "abstracts.md"
        rep  = data / "library_report.md"
        check("text/ dir exists", text_dir.is_dir())
        text_files = list(text_dir.glob("*.txt")) if text_dir.is_dir() else []
        check("text files count matches PDFs", len(text_files) == len(pdfs),
              detail=f"{len(text_files)} txt vs {len(pdfs)} pdf")
        check("metadata.csv exists", meta.exists())
        check("abstracts.md exists", abst.exists())
        check("library_report.md exists", rep.exists())
        if rep.exists():
            rep_text = rep.read_text(encoding="utf-8")
            check("library_report includes cleaning section",
                  "Cleaning applied" in rep_text)
    else:
        print("\n[Tier 2: skipping Stages 5-6 (tables, build artifacts)]")

    # ---------- Stage 7: Cross-checks ----------
    print("\n=== Stage 7: Cross-checks ===")
    pdf_stems = set(p[:-4] for p in pdfs)
    sidecar_stems = set(s[:-len(".fulltext.json")] for s in sidecars)
    orphan_sidecars = sidecar_stems - pdf_stems
    check("no orphan sidecars (no PDF)", not orphan_sidecars,
          detail="" if not orphan_sidecars else f"{len(orphan_sidecars)} orphans")

    if tier == 1 and data:
        txt_stems = set(p.name[:-4] for p in text_files)
        missing_txt = pdf_stems - txt_stems
        check("every PDF has a text dump", not missing_txt,
              detail="" if not missing_txt else f"{len(missing_txt)} missing")

        pmc = data / "discovered/pmc_fetch_report.csv"
        if pmc.exists():
            with open(pmc, encoding="utf-8") as fh:
                pmc_rows = list(csv.DictReader(fh))
            dl_rows = [r for r in pmc_rows if r.get("downloaded", "").lower() == "true"]
            missing_pdfs = []
            for r in dl_rows:
                fn = r.get("filename", "")
                if fn and fn not in pdfs:
                    ascii_fn = fn.encode("ascii", "ignore").decode("ascii")
                    if ascii_fn not in pdfs:
                        missing_pdfs.append(fn)
            check("every PMC-downloaded filename is in the library", not missing_pdfs,
                  detail="" if not missing_pdfs else f"{len(missing_pdfs)} missing")

    # ---------- Tools sanity ----------
    # Check tools are co-located with this script (the elevated location).
    print("\n=== Tools sanity ===")
    here = Path(__file__).parent
    expected_tools = [
        "unpaywall_fetch_v2.py", "pmc_fetch.py", "backfill_fulltext.py",
        "extract_tables.py", "preprint_fetch.py", "pdf_text_clean.py",
        "jats_to_text.py", "build_pdf_library.py", "pipeline_check.py",
    ]
    for t in expected_tools:
        check(t, (here / t).exists())
    # Also vendor
    check("vendor/mathml_to_latex/", (here / "vendor" / "mathml_to_latex").is_dir())

    # ---------- Result ----------
    print("\n=== Result ===")
    if issues:
        print(f"  {len(issues)} issue(s) found:")
        for i in issues:
            print(f"    - {i}")
        sys.exit(1)
    else:
        print("  All stages OK.")
        sys.exit(0)


if __name__ == "__main__":
    main()
