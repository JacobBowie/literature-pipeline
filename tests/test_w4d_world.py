"""W4-D fixture worlds, shared by the golden recording and the tests (not a test module).

`build(tmp, spec)` materialises a JSON world spec (tests/fixtures/W4-D/*.json) under `tmp`: a
projects root, a registry file with a temp state_dir, libraries (PDF + .ris + sidecar per held
DOI), forward-citation CSVs and residual CSVs. It writes files only; indexing needs the caller's
patches (lit_util.PROJECTS_ROOT, index_portfolio.CONFIG_PATH, litpipe.config.CONFIG_PATH)."""
import csv
import json
from pathlib import Path

FIX = Path(__file__).resolve().parent / "fixtures" / "W4-D"

FORWARD_FIELDS = ["seed_pdf", "seed_doi", "citing_doi", "citing_title", "citing_year", "citing_authors",
                  "citing_venue", "citing_cited_by"]


def load_spec(name):
    return json.loads((FIX / name).read_text(encoding="utf-8"))


def ris_text(doi, title, year="2019"):
    """A .ris whose only DOI is its DO line."""
    return f"TY  - JOUR\nAU  - Avery, Kim\nPY  - {year}\nTI  - {title}\nJO  - A Journal\nDO  - {doi}\nER  - \n"


def sidecar_text(doi, title):
    return json.dumps({"doi": doi, "title": title, "text": "Body text of the paper. " * 20,
                       "has_pdf": True, "extracted_from_pdf": True})


def write_csv(path, fields, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, lineterminator="\n")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})


class World:
    def __init__(self, tmp, spec):
        self.tmp = Path(tmp)
        self.spec = spec
        self.root = self.tmp / "root"
        self.cfg_path = self.tmp / "projects.json"
        self.db = self.tmp / "refs" / "portfolio.duckdb"
        self.registry = {"state_dir": str(self.tmp / "state"), "projects": spec["projects"]}

    def project_dir(self, key):
        p = self.spec["projects"][key]
        parent = p.get("parent")
        if parent:
            return self.root / parent / (key[len(parent):].lstrip("/") or key)
        return self.root / key

    def lib(self, key):
        p = self.spec["projects"][key]
        return self.root / (p.get("parent") or key) / p["lib_dir"]

    def build(self):
        self.root.mkdir(parents=True, exist_ok=True)
        self.cfg_path.write_text(json.dumps(self.registry, indent=1), encoding="utf-8")
        stems = {}
        for key in self.spec["projects"]:
            self.lib(key).mkdir(parents=True, exist_ok=True)
        for key, items in self.spec.get("held", {}).items():
            lib = self.lib(key)
            for h in items:
                stems[h["doi"]] = h["stem"]
                (lib / f"{h['stem']}.pdf").write_bytes(b"%PDF-1.4 stub\n")
                (lib / f"{h['stem']}.ris").write_text(ris_text(h["doi"], h["title"]), encoding="utf-8")
                (lib / f"{h['stem']}.fulltext.json").write_text(sidecar_text(h["doi"], h["title"]),
                                                               encoding="utf-8")
        for x in self.spec.get("extra_files", ()):
            p = self.lib(x["project"]) / x["name"]
            p.write_text(json.dumps(x["json"]) if "json" in x else x["text"], encoding="utf-8")
        for key, edges in self.spec.get("forward", {}).items():
            rows = [{"seed_pdf": stems.get(e["seed_doi"], "seed") + ".pdf", "citing_authors": "Avery, Kim",
                     "citing_venue": "A Venue", **e} for e in edges]
            write_csv(self.lib(key) / "_forward_citations.csv", FORWARD_FIELDS, rows)
        for key, files in self.spec.get("residuals", {}).items():
            for f in files:
                where = self.project_dir(key) / f.get("subdir", "") / f["name"]
                write_csv(where, f["fields"], f["rows"])
        return self


def build(tmp, spec):
    return World(tmp, spec).build()


def read_rows(path):
    with open(path, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))
