"""Fix review of 6c772de (import_downloads in-place filing): reproductions written at 5487c3c."""
from pathlib import Path

import pytest

import import_downloads as ID
from tests.test_import_downloads import COVERS, FILLER, doi_of, env, make_pdf, meta, page, title_of  # noqa: F401
from tests.test_w5c3_import import pages_of


# ---------------------------------------------------------------- FR-4: a case variant of the canonical name
def test_fr4_a_case_variant_of_its_canonical_name_is_not_reported_as_a_suffixed_duplicate(env):
    env.add("autonomic")
    name = ID.proposed_name(meta("autonomic"))[0]
    variant = name.lower()
    assert variant != name
    body = [page(title_of("autonomic"), doi_of("autonomic")), page("", "", header="Results", body=FILLER)]
    make_pdf(env.lib / variant, body)
    row = env.run(downloads=str(env.lib), execute=True)["rows"][0]
    print(f"\nFR-4 row: action={row['action']} new_name={row['new_name']} note={row['note']!r}")
    assert row["action"] == "MOVED"
    assert [p.name for p in env.lib.glob("*.pdf")] == [variant]          # filed in place, not renamed
    assert "suffixed" not in row["note"] and "duplicate" not in row["note"]


# ---------------------------------------------------------------- FR-5: the library reached through a junction
def test_fr5_downloads_that_is_the_library_through_a_junction_files_in_place(env):
    _winapi = pytest.importorskip("_winapi")
    from tests.test_import_downloads import RECORDS
    link = env.tmp / "dl_via_junction"
    _winapi.CreateJunction(str(env.lib), str(link))
    name = ID.proposed_name(meta("stroke_volume"))[0]
    body = [page(title_of("stroke_volume"), ""), page("", "", header="Results", body=FILLER)]
    src = make_pdf(env.lib / name, [COVERS["ill_cover_1"], COVERS["ill_cover_2"]] + body)
    n = len(pages_of(src))
    env.search.results = [RECORDS["stroke_volume"]]
    row = env.run(downloads=str(link), execute=True)["rows"][0]
    print(f"\nFR-5 row: action={row['action']} new_name={row['new_name']} note={row['note']!r}")
    assert row["new_name"] == name and "suffixed" not in row["note"]
    assert [p.name for p in env.lib.glob("*.pdf")] == [name]
    assert len(pages_of(env.lib / name)) == n - 2
