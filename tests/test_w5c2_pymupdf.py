"""W5-C2 step 9 (item 6; M223): PyMuPDF's module name is `pymupdf` since 1.24.3 ("The Python module is
now called `pymupdf`. `fitz` is still supported for backwards compatibility"); 1.28.2 warns when the
legacy `fitz` module is imported (pymupdf.readthedocs.io changes.html, read 2026-10-07). No module
below imports `fitz`; where only the import line may change (W5 ownership), it is `import pymupdf as
fitz`, which the docs name as the alternative. W5-C3's modules join MODULES once they land."""
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
MODULES = ["unpaywall_fetch_v2.py", "fill_missing_dois.py", "forward_citations.py", "reverse_citations.py",
           "pmc_fetch.py", "recheck_pmc.py", "extract_pdf_fulltext.py", "build_pdf_library.py",
           "pdf_text_clean.py", "snowball.py", "litpipe/text.py", "litpipe/identity.py", "litpipe/doi.py",
           "litpipe/walk.py", "litpipe/openalex.py", "litpipe/canaries.py", "litpipe/worklists.py",
           # W5-C3's modules (landed in ac8e535; added by the dispatcher at the C2 merge, forward F8)
           "audit_filenames.py", "backfill_ris.py", "import_downloads.py", "paywall_pull.py"]
LEGACY = re.compile(r"^\s*(?:import\s+fitz\b(?!\S)|from\s+fitz\b)", re.M)


@pytest.mark.parametrize("name", MODULES)
def test_no_module_imports_the_legacy_fitz_name(name):
    src = (REPO / name).read_text(encoding="utf-8")
    hits = [m.group(0).strip() for m in LEGACY.finditer(src)]
    assert hits == [], f"{name} imports the legacy name: {hits}"


def test_the_pattern_tells_the_alias_from_the_legacy_import():
    assert LEGACY.search("    import fitz\n") and LEGACY.search("import fitz  # pymupdf\n")
    assert LEGACY.search("from fitz import open\n")
    assert not LEGACY.search("    import pymupdf as fitz\n") and not LEGACY.search("import pymupdf\n")


def test_the_pymupdf_name_imports():
    import pymupdf
    assert callable(pymupdf.open)
