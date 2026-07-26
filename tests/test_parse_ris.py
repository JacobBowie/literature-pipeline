"""c10/c11: unified lit_util.parse_ris (unions the index_portfolio + harvest_citations copies).

Covers int-or-None year (paper_metadata.year INTEGER invariant), authors + authors_raw alias,
venue, continuation-line joining, AU/A1 - PY/Y1 - TI/T1 tag aliases, the UR-only-DOI fallback, and
that both former homes now re-export the SAME function object. Also adds the enw/nbib fallback tests
the handoff flagged as zero-coverage (harvest_citations.parse_enw / parse_nbib).
"""
import lit_util
import index_portfolio
import harvest_citations
import ris_emit


def _ris(tmp_path, text):
    p = tmp_path / "x.ris"
    p.write_text(text, encoding="utf-8")
    return p


def test_parse_ris_basic_fields(tmp_path):
    m = lit_util.parse_ris(_ris(tmp_path,
        "TY  - JOUR\nDO  - 10.1234/ABC\nPY  - 2020\nTI  - Heat Title\n"
        "JO  - J Therm\nAU  - Smith, John\nAU  - Doe, Jane\nER  - \n"))
    assert m["doi"] == "10.1234/abc"                          # lowercased
    assert m["year"] == 2020 and isinstance(m["year"], int)   # INT, not str (ingest INTEGER)
    assert m["title"] == "Heat Title"
    assert m["venue"] == "J Therm"
    assert m["authors"] == ["Smith, John", "Doe, Jane"]
    assert m["authors_raw"] == m["authors"]                   # alias for harvest-family consumers
    assert m["lastname"] == "Smith"


def test_parse_ris_year_is_int_or_none(tmp_path):
    assert lit_util.parse_ris(_ris(tmp_path, "TI  - No Year\nER  - \n"))["year"] is None
    assert lit_util.parse_ris(_ris(tmp_path, "PY  - n.d.\nER  - \n"))["year"] is None   # non-digit -> None
    assert lit_util.parse_ris(_ris(tmp_path, "PY  - 1998/03/01\nER  - \n"))["year"] == 1998  # [:4]


def test_parse_ris_continuation_line_joined_not_truncated(tmp_path):
    m = lit_util.parse_ris(_ris(tmp_path,
        "TI  - A Very Long Title That Wraps\n      Across Two Lines\nDO  - 10.1/x\nER  - \n"))
    assert m["title"] == "A Very Long Title That Wraps Across Two Lines"


def test_parse_ris_ur_only_doi_fallback(tmp_path):
    m = lit_util.parse_ris(_ris(tmp_path, "TI  - No DO Tag\nUR  - https://doi.org/10.5678/xyz9\nER  - \n"))
    assert m["doi"] == "10.5678/xyz9"


def test_parse_ris_tag_aliases(tmp_path):
    """A1/Y1/T1 accepted as AU/PY/TI aliases (the harvest-side behavior, now unified in)."""
    m = lit_util.parse_ris(_ris(tmp_path, "T1  - Alias Title\nY1  - 2019\nA1  - Roe, Rae\nER  - \n"))
    assert m["title"] == "Alias Title" and m["year"] == 2019 and m["authors"] == ["Roe, Rae"]


def test_parse_ris_missing_file_returns_empty_shape(tmp_path):
    m = lit_util.parse_ris(tmp_path / "nope.ris")
    assert m["doi"] == "" and m["year"] is None and m["authors"] == [] and m["authors_raw"] == []


def test_parse_ris_reexports_are_same_object():
    """index_portfolio.parse_ris (test_index_prune calls it) and harvest_citations.parse_ris
    are both the promoted lit_util function -- one implementation, no divergence."""
    assert index_portfolio.parse_ris is lit_util.parse_ris
    assert harvest_citations.parse_ris is lit_util.parse_ris


def test_canonical_stem_tolerates_int_and_str_year():
    """THE invariant that made the year-type unification safe: harvest's .ris now yields an int
    year while its .enw/.nbib siblings still yield a str year, and both flow to the SAME consumer
    (ris_emit.canonical_stem via the CrossRef-miss fallback filename path). Lock that canonical_stem
    str-coerces the year, so int and str produce IDENTICAL stems and None -> 'Unknown'. A future
    canonical_stem edit assuming a str year (year[:4], year.strip()) would silently degrade harvest
    fallback filenames with no other guard -- this catches it."""
    assert ris_emit.canonical_stem(2020, "Smith", "A Heat Title").startswith("2020_Smith")
    assert ris_emit.canonical_stem(None, "Smith", "T").startswith("Unknown_")
    # int (parse_ris) and str (parse_enw/parse_nbib) years must yield the SAME canonical stem
    assert ris_emit.canonical_stem(2020, "Smith", "A Heat Title") == \
           ris_emit.canonical_stem("2020", "Smith", "A Heat Title")


# ---------- enw / nbib fallback (harvest siblings) -- previously zero coverage (handoff) ----------

def test_parse_enw_basic_and_url_fallback(tmp_path):
    p = tmp_path / "a.enw"
    p.write_text("%T ENW Title\n%D 2021\n%A Nordman, Nils\n%R 10.9/enw1\n", encoding="utf-8")
    m = harvest_citations.parse_enw(str(p))
    assert m["doi"] == "10.9/enw1" and m["title"] == "ENW Title"
    assert m["year"] == "2021" and m["lastname"] == "Nordman" and m["authors_raw"] == ["Nordman, Nils"]
    # %R absent -> DOI recovered from a %U doi.org URL (fallback runs through extract_doi_from_text,
    # which requires a real 4-9 digit registrant, so use a well-formed DOI here).
    q = tmp_path / "b.enw"
    q.write_text("%T No R Tag\n%U https://doi.org/10.9999/enw.42\n", encoding="utf-8")
    assert harvest_citations.parse_enw(str(q))["doi"] == "10.9999/enw.42"


def test_parse_nbib_basic(tmp_path):
    p = tmp_path / "c.nbib"
    p.write_text("PMID- 12345678\nTI  - NBIB Title\nDP  - 2018 Jun\n"
                 "FAU - Vasquez, Vera\nAID - 10.1/nbib1 [doi]\n", encoding="utf-8")
    m = harvest_citations.parse_nbib(str(p))
    assert m["doi"] == "10.1/nbib1" and m["pmid"] == "12345678"
    assert m["title"] == "NBIB Title" and m["year"] == "2018"
    assert m["lastname"] == "Vasquez" and m["authors_raw"] == ["Vasquez, Vera"]
