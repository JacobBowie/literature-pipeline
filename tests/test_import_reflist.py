"""A PDF whose first page lists many works is not one of them (librarian, 2026-10-09).

A 16-page product bibliography (a table of studies: year, author, institute, title, journal) printed
11 distinct DOIs on its first page, and identify() filed it as the first listed study whose record
title matched page 1. Past REFLIST_PAGE1_DOIS distinct DOIs on page 1, every candidate is read
before one is taken, and two matching titles mean a list: FLAG, left in Downloads. Below it, the first
match still wins (a "Comment on: X" prints its own and X's DOI and titles on page 1)."""
import import_downloads as ID
from tests.test_import_downloads import FILLER, env, make_pdf, meta, page  # noqa: F401  (env is a fixture)

STUDIES = [(f"10.5555/bib.{i}", t) for i, t in enumerate([
    "Body composition using air displacement plethysmography in critically ill adults",
    "Visceral adipose tissue and cardiometabolic risk in adolescents with obesity",
    "Fat mass estimation by plethysmography in patients receiving haemodialysis",
    "Comparison of four body composition methods in elite rugby players",
    "Changes in fat-free mass during a twelve-week resistance training programme",
    "Agreement between plethysmography and dual-energy absorptiometry in older women"], start=1)]


def _answer(env, doi, title):
    env.answers[doi] = ({**meta("stroke_volume"), "doi": doi, "title": title}, "crossref")


def test_a_bibliography_page_is_flagged_not_filed_as_its_first_entry(env):
    for d, t in STUDIES:
        _answer(env, d, t)
    rows = "\n".join(f"Product | 2020 | Author {i} | Institute | {t} | https://doi.org/{d} | Journal"
                     for i, (d, t) in enumerate(STUDIES))
    make_pdf(env.dl / "bib.pdf", [f"Product Bibliography\n{rows}", f"Product Bibliography (cont.)\n{rows}"])
    row = env.run(execute=True)["rows"][0]
    assert row["action"] == "IDENTITY_FLAG" and row["identity"] == "FLAG"
    assert "reference list or bibliography" in row["detail"] and (env.dl / "bib.pdf").exists()
    assert "check it by hand before any --doi" in row["note"] and "resolves it" not in row["note"]
    assert not list(env.lib.glob("*.pdf"))                              # nothing filed
    assert env.searches == []                                           # no title search past the gate


def test_a_comment_printing_its_target_on_page_1_still_takes_its_own_doi(env):
    own, target = STUDIES[0], STUDIES[1]
    _answer(env, *own)
    _answer(env, *target)
    # the usual layout: its own DOI in the header, the commented paper cited below its title. (Known
    # limit, unchanged here: below the gate the first matching DOI wins, so a target DOI printed above
    # the comment's own would be taken.)
    first = page(own[1], own[0], body=f"Comment on: {target[1]} https://doi.org/{target[0]}\n{FILLER}")
    make_pdf(env.dl / "c.pdf", [first, page("", "", header="Results", body=FILLER)])
    row = env.run()["rows"][0]
    assert row["doi"] == own[0] and row["action"] != "IDENTITY_FLAG"     # two DOIs: below the gate


def test_a_dense_first_page_whose_own_title_is_the_only_match_is_taken(env):
    own = STUDIES[0]
    _answer(env, *own)
    for d, _ in STUDIES[1:5]:                                           # cited DOIs whose titles are not printed
        _answer(env, d, "An unrelated cited work on renal physiology and sodium balance")
    cites = " ".join(f"[{i}] https://doi.org/{d}" for i, (d, _) in enumerate(STUDIES[1:5], start=1))
    make_pdf(env.dl / "a.pdf", [page(own[1], own[0], body=FILLER + " " + cites),
                                page("", "", header="Results", body=FILLER)])
    row = env.run()["rows"][0]
    assert len(set(ID.holdings.extract_dois(ID.scan_pdf(env.dl / "a.pdf").first_article_text))) >= ID.REFLIST_PAGE1_DOIS
    assert row["doi"] == own[0] and row["action"] != "IDENTITY_FLAG"
