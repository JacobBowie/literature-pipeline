"""W5 verifier Q: the library side of 53ca3e6..7002ee9 (W5-C2, W5-C3, merge forwards F1-F5).

Locks added by an adversarial review: the comparison fold never reaches a stored or displayed value,
the SICI and month-range DOI rules over live census shapes, the walk cache's schema-2 migration,
import_downloads' failure paths and edge folders, and the NOT_PDF boundary. The locks for the
defects it found (Q-2 to Q-6) were strict xfails at `7002ee9`; the fixes landed on 2026-10-08 and
they are plain tests now."""
import json
from pathlib import Path

import pymupdf
import pytest

import import_downloads as ID
import ris_emit as R
from litpipe import doi as D, holdings as H, text as T
from tests.test_import_downloads import COVERS, FILLER, doi_of, env, make_pdf, meta, page, title_of  # noqa: F401
from tests.test_w5c3_import import COVER_TEXT_MARKERS, pages_of, sha, sidecar_of


# ================================================================ 1. the fold never reaches a stored value
@pytest.mark.parametrize("title, subtitle, want", [
    ("ACSM’s guidelines for exercise testing", "", "ACSM’s guidelines for exercise testing"),
    ("β-Alanine supplementation", "a meta–analysis", "β-Alanine supplementation: a meta–analysis"),
    ("Effects of β-alanine", "", "Effects of β-alanine"),
])
def test_join_title_writes_the_display_text_never_the_folded_form(title, subtitle, want):
    assert R.join_title(title, subtitle) == want


def test_join_title_drops_a_subtitle_the_title_already_ends_with_modulo_the_fold():
    """The fold reaches join_title's comparison only: a subtitle repeated in another spelling is not
    appended twice, and the title kept is the record's own display text."""
    assert R.join_title("Training at altitude: β-adrenergic responses", "beta-adrenergic responses") == \
        "Training at altitude: β-adrenergic responses"


def test_filename_title_is_byte_for_byte_the_pre_fold_normalise_title():
    """F2: canonical filenames are built from filename_title, which must equal the 53ca3e6
    normalise_title (verifier census: 0 of 6,289 library canonical names change)."""
    old = lambda s: " ".join(T._SPACE_AFTER.sub(r"\1", T._SPACE_BEFORE.sub(r"\1", T.clean_field(s).lower())).split())
    for s in ("ACSM’s guidelines", "β-Alanine – a review", "Heat <i>stress</i> ( Part 1 ) ,", "Périard 1990–2000",
              "&bgr;-alanine", "µg and μg", "5′-AMP", ""):
        assert T.filename_title(s) == old(s)


def test_harvest_identity_key_stays_unfolded():
    """harvest_citations keys identity and a collision hash on ris_emit.normalize_title: unfolded."""
    import harvest_citations as HC
    assert R.normalize_title("β-blockers in heat") == "blockers in heat"
    assert HC._identity({"doi": "", "title": "β-blockers in heat"}) == ("", "blockers in heat")
    assert HC._identity({"title": "beta-blockers in heat"}) != HC._identity({"title": "β-blockers in heat"})


# ================================================================ 3. SICI and month-range DOI rules (live census shapes)
SICI_HASH = "10.1002/(sici)1097-0177(199809)213:1<147::aid-aja15>3.0.co;2-#"      # a live harvest citing_doi
MONTH_RANGE = ["10.1002/1520-6300(200102/03)13:2<162::aid-ajhb1025>3.0.co;2-t",     # three live harvest DOIs
               "10.1002/1520-6300(200102/03)13:2<173::aid-ajhb1026>3.0.co;2-m",     # 53ca3e6 merged them into
               "10.1002/1520-6300(200102/03)13:2<180::aid-ajhb1027>3.0.co;2-r"]     # one key: ...13:2


def test_the_live_sici_hash_doi_has_one_key_in_every_rule():
    assert D.normalise(SICI_HASH) == D.normalise_structured(SICI_HASH) == H.doi_key(SICI_HASH) == SICI_HASH
    assert D.normalise("10.1002/(SICI)1097-0177(199809)213:1<147::AID-AJA15>3.0.CO;2-#") == SICI_HASH
    assert D.encode_path(SICI_HASH).endswith(";2-%23")


def test_the_three_month_range_dois_are_three_keys_again():
    for fn in (D.normalise, D.normalise_structured, H.doi_key):
        keys = [fn(d) for d in MONTH_RANGE]
        assert keys == MONTH_RANGE and len(set(keys)) == 3


def test_a_sici_hash_doi_in_a_ris_do_line_is_a_holding(tmp_path, monkeypatch):
    import lit_util
    root = tmp_path / "root"
    lib = root / "teaching_a" / "literature"
    lib.mkdir(parents=True)
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", root)
    (lib / "1998_Example_Sici.pdf").write_bytes(b"%PDF-1.4 stub")
    (lib / "1998_Example_Sici.ris").write_text(
        "TY  - JOUR\nDO  - 10.1002/(SICI)1097-0177(199809)213:1<147::AID-AJA15>3.0.CO;2-#\nER  - \n", encoding="utf-8")
    reg = {"state_dir": str(tmp_path / "state"), "projects": {"teaching_a": {"lib_dir": "literature"}}}
    hm = H.build(reg, use_cache=False, write_cache=False)
    assert [Path(p).name for p in hm.where(SICI_HASH)] == ["1998_Example_Sici.pdf"]


@pytest.mark.parametrize("wrapped", [
    f"`{SICI_HASH}`",                       # a markdown code span (migrate writes DOI `...` lines)
    f"\"{SICI_HASH}\"",                     # a quoted string (an HTML attribute, JSON text)
    f"'{SICI_HASH}'",
    f"**{SICI_HASH}**",                     # markdown bold
    f"“{SICI_HASH}”",                       # curly quotes
])
def test_a_sici_hash_doi_inside_quotes_or_backticks_keeps_its_check_character(wrapped):
    assert D.normalise(wrapped) == SICI_HASH
    assert H.extract_dois(f"see {wrapped} here") == [SICI_HASH]


def test_a_non_sici_hash_is_still_a_fragment():
    assert D.normalise("10.1056/NEJMc1113675#sa3") == "10.1056/nejmc1113675"
    assert D.normalise("`10.1056/NEJMc1113675#sa3`") == "10.1056/nejmc1113675"
    assert D.normalise("\"10.1056/NEJMc1113675#sa3\"") == "10.1056/nejmc1113675"


# ================================================================ 4. the walk: re-gate scope and the cache migration
from litpipe import openalex, walk  # noqa: E402
from tests.test_walk_forward import make_lib, world  # noqa: E402,F401  (fixture)
from tests.test_w5c2_walk import pworld  # noqa: E402,F401  (fixture)

OA_SEED = "10.5555/oaseed.0001"
S2_SEED = "10.5555/s2seed.0002"


def test_the_regate_singleton_goes_only_to_count_none_seeds_and_only_under_source_openalex(pworld, tmp_path,
                                                                                          monkeypatch):
    import forward_citations as fc
    monkeypatch.setenv(openalex.KEY_ENV, "oa-test-key-not-real")
    pworld.script(OA_SEED, [[0, 1, 2]], [3], cited_by=3)       # S2 citationCount null
    pworld.add(S2_SEED, 2, oa=2)                                # S2 counts it
    lib = make_lib(tmp_path, [OA_SEED, S2_SEED])
    assert fc.run(lib_dir=str(lib), source="openalex")["exit_code"] == 0
    pworld.sent.clear()
    assert fc.run(lib_dir=str(lib), source="openalex")["exit_code"] == 0
    assert [r["path"] for r in pworld.oa_calls()] == [f"/works/doi:{OA_SEED}"]   # one, for the count-None seed
    pworld.sent.clear()
    fc.run(lib_dir=str(lib), source="s2")                        # another source: no OpenAlex call at all
    assert pworld.oa_calls() == []
    pworld.sent.clear()
    fc.run(lib_dir=str(lib), source="openalex", refresh=True)    # --refresh walks; no gate singleton first
    singletons = [r["path"] for r in pworld.oa_calls() if r["path"].startswith("/works/doi:")]
    assert sorted(singletons) == sorted([f"/works/doi:{OA_SEED}", f"/works/doi:{S2_SEED}"])   # the walks' own


V1_SCHEMA = """
CREATE TABLE cache_meta (key VARCHAR PRIMARY KEY, value VARCHAR);
CREATE TABLE seed_state (doi VARCHAR NOT NULL, source VARCHAR NOT NULL, paper_id VARCHAR, count_at_walk BIGINT,
  n_rows BIGINT, state VARCHAR NOT NULL, kind VARCHAR, reason VARCHAR, n_unreachable BIGINT,
  walked_at TIMESTAMPTZ NOT NULL, rows_at TIMESTAMPTZ, PRIMARY KEY (doi, source));
CREATE TABLE citers (seed_doi VARCHAR NOT NULL, source VARCHAR NOT NULL, citing_id VARCHAR NOT NULL,
  pos INTEGER NOT NULL, citing_paper_id VARCHAR, citing_doi VARCHAR, citing_title VARCHAR, citing_year INTEGER,
  citing_authors VARCHAR, citing_venue VARCHAR, citing_cited_by BIGINT, citing_oa BOOLEAN,
  PRIMARY KEY (seed_doi, source, citing_id));
INSERT INTO cache_meta VALUES ('schema_version', '1');
"""


def _v1_cache(path, n_seeds=5, n_citers=4):
    import duckdb
    con = duckdb.connect(str(path))
    con.execute(V1_SCHEMA)
    for s in range(n_seeds):
        src = "openalex" if s % 2 else "s2"
        con.execute("INSERT INTO seed_state VALUES (?, ?, ?, ?, ?, 'complete', NULL, '', NULL, "
                    "TIMESTAMPTZ '2026-10-01 00:00:00+00', TIMESTAMPTZ '2026-10-01 00:00:00+00')",
                    [f"10.5555/v1.{s}", src, f"p{s}", None if src == "openalex" else n_citers, n_citers])
        for c in range(n_citers):
            con.execute("INSERT INTO citers VALUES (?, ?, ?, ?, ?, ?, 'T', 2001, 'A', 'V', 0, false)",
                        [f"10.5555/v1.{s}", src, f"W{s}{c}", c, f"W{s}{c}", f"10.5555/c.{s}.{c}"])
    con.close()


def test_a_schema_1_cache_keeps_every_row_through_the_migration_and_takes_new_writes(tmp_path):
    path = tmp_path / "s2_cache.duckdb"
    _v1_cache(path)
    with walk.Cache(path) as c:
        st = c.states()
        assert len(st) == 5 and all(v["oa_cited_by"] is None for v in st.values())
        rows = c.rows_many(list(st))
        assert sum(len(v) for v in rows.values()) == 20
        c.record("10.5555/v1.1", "openalex", state="complete", rows=rows[("10.5555/v1.1", "openalex")][:3],
                 oa_cited_by=3)
    with walk.Cache(path) as c:                                   # re-opened: the column is not re-added
        st = c.states()
        assert len(st) == 5 and st[("10.5555/v1.1", "openalex")]["oa_cited_by"] == 3
        assert sum(len(v) for v in c.rows_many(list(st)).values()) == 19
    # an old seed with no S2 count and no stored oa_cited_by is walked once to store it
    assert walk.needs_walk(None, st[("10.5555/v1.3", "openalex")], oa_count=4) is True
    assert walk.needs_walk(None, st[("10.5555/v1.1", "openalex")], oa_count=3) is False


def test_a_migrated_cache_records_its_schema_version(tmp_path):
    import duckdb
    path = tmp_path / "s2_cache.duckdb"
    _v1_cache(path, 1, 1)
    with walk.Cache(path):
        pass
    con = duckdb.connect(str(path), read_only=True)
    try:
        assert con.execute("SELECT value FROM cache_meta WHERE key='schema_version'").fetchone()[0] == \
            str(walk.SCHEMA_VERSION)
    finally:
        con.close()


# ================================================================ 6. import_downloads beyond the librarian's exercise
from tests.test_w5c3_import import SLIP, SLIP_TEXT_MARKERS, CONF_DOI, CONF_ITEM, CONF_TITLE  # noqa: E402


def _ill(env, where, name="ill.pdf"):
    env.add("autonomic")
    body = [page(title_of("autonomic"), doi_of("autonomic")), page("", "", header="Results", body=FILLER)]
    return make_pdf(where / name, [COVERS["ill_cover_1"], COVERS["ill_cover_2"]] + body)


def test_a_zero_byte_file_is_not_pdf_and_stays(env):
    (env.dl / "empty.pdf").write_bytes(b"")
    row = env.run(execute=True)["rows"][0]
    assert row["action"] == "NOT_PDF" and (env.dl / "empty.pdf").exists() and not list(env.lib.iterdir())
    assert env.resolved == []


@pytest.mark.parametrize("offset, action", [(1020, "WOULD_MOVE"), (1021, "NOT_PDF"), (1024, "NOT_PDF")])
def test_the_pdf_magic_boundary_is_the_first_1024_bytes(env, offset, action):
    env.add("stroke_volume")
    pdf = make_pdf(env.tmp / "x.pdf", [page(title_of("stroke_volume"), doi_of("stroke_volume"))])
    (env.dl / "padded.pdf").write_bytes(b" " * offset + pdf.read_bytes())    # %PDF at byte offset+1
    assert env.run()["rows"][0]["action"] == action


def _fail_once(label):
    def f(*a, **k):
        raise OSError(f"{label} (injected)")
    return f


def test_a_failed_cover_strip_write_files_nothing(env, monkeypatch):
    src = _ill(env, env.dl)
    orig = sha(src)
    monkeypatch.setattr(ID, "write_without_pages", _fail_once("strip failed"))
    row = env.run(execute=True)["rows"][0]
    assert row["action"] == "ERR_WRITE" and "strip failed" in row["detail"]
    assert sha(src) == orig
    assert not [p for p in env.lib.rglob("*") if p.is_file()]                # no companion, no tmp, no archive
    ris = env.lib / row["new_name"].replace(".pdf", ".ris")
    assert R._kv_get(R.RIS_NS, R.manifest_key(str(ris))) is None


def test_a_failed_archive_move_files_nothing_and_keeps_the_original(env, monkeypatch):
    src = _ill(env, env.dl)
    orig = sha(src)
    real = ID.shutil.move

    def move(a, b, *x, **k):
        if "_archive" in str(b):
            raise OSError("archive unwritable (injected)")
        return real(a, b, *x, **k)
    monkeypatch.setattr(ID.shutil, "move", move)
    row = env.run(execute=True)["rows"][0]
    assert row["action"] == "ERR_WRITE" and "archive" in row["detail"]
    assert src.exists() and sha(src) == orig
    assert not [p for p in env.lib.rglob("*") if p.is_file()]


def test_keep_covers_keeps_slip_text_out_of_every_sidecar_field_and_the_ris_and_files_the_pdf_whole(env):
    env.answers[CONF_DOI] = (R.crossref_meta(CONF_ITEM), "crossref")
    src = make_pdf(env.dl / "slip.pdf", [SLIP, page(CONF_TITLE, CONF_DOI, header="Proceedings of the Example "
                                                    "Conference 1985, pp. 12-20")])
    orig = sha(src)
    res = env.run(execute=True, keep_covers=True)
    row = res["rows"][0]
    assert row["action"] == "MOVED" and row["covers_stripped"] == "" and row["archived_original"] == ""
    filed = env.lib / row["new_name"]
    assert sha(filed) == orig                                                 # the PDF whole
    sc_raw = sidecar_of(env.lib, row["new_name"]).read_text(encoding="utf-8")
    ris_raw = filed.with_suffix(".ris").read_text(encoding="utf-8")
    for m in SLIP_TEXT_MARKERS:
        assert m not in sc_raw and m not in ris_raw, m
    report = Path(res["report"]).read_text(encoding="utf-8")
    assert "Exampleperson" not in report                                      # no patron in the report either


def test_a_downloads_folder_that_is_the_library_moves_within_it_and_strips(env):
    src = _ill(env, env.lib)
    orig = sha(src)
    row = env.run(downloads=str(env.lib), execute=True)["rows"][0]
    assert row["action"] == "MOVED" and row["covers_stripped"] == "2"
    filed = env.lib / row["new_name"]
    assert len(pages_of(filed)) == 2 and not src.exists()
    assert sha(env.lib / "_archive" / "originals" / row["new_name"]) == orig


def test_a_canonically_named_ill_pdf_in_the_library_is_never_reported_stripped_while_whole(env):
    name = ID.proposed_name(meta("autonomic"))[0]
    src = _ill(env, env.lib, name=name)
    n = len(pages_of(src))
    row = env.run(downloads=str(env.lib), execute=True)["rows"][0]
    assert row["action"] == "MOVED"
    filed = env.lib / row["new_name"]
    stripped = len(pages_of(filed)) == n - 2
    assert (row["covers_stripped"] == "2") == stripped
    assert not stripped or row["archived_original"]
    assert ("stripped 2 cover page(s)" in row["note"]) == stripped


def test_a_title_identified_covered_pdf_at_its_canonical_name_is_filed_in_place(env):
    """Q-3 sibling (librarian projects-41, reproduced on two real ILL deliveries, 2026-10-08): with the
    Downloads folder being the library, a file already at its canonical name that prints no DOI in its
    first 5,000 characters (an ILL cover, a title-identified paper) read as ANOTHER paper occupying its
    own name, so it was moved to a suffixed name with a "check for a duplicate" note pointing at
    itself. It is filed in place: canonical name kept, covers stripped, the original archived."""
    from tests.test_import_downloads import RECORDS
    name = ID.proposed_name(meta("stroke_volume"))[0]
    body = [page(title_of("stroke_volume"), ""), page("", "", header="Results", body=FILLER)]
    src = make_pdf(env.lib / name, [COVERS["ill_cover_1"], COVERS["ill_cover_2"]] + body)
    n = len(pages_of(src))
    env.search.results = [RECORDS["stroke_volume"]]
    row = env.run(downloads=str(env.lib), execute=True)["rows"][0]
    assert row["identity"] == "TITLE_MATCH"
    assert row["new_name"] == name and "suffixed" not in row["note"]
    assert [p.name for p in env.lib.glob("*.pdf")] == [name]
    assert len(pages_of(env.lib / name)) == n - 2 and row["covers_stripped"] == "2"
    assert row["archived_original"] and Path(row["archived_original"]).name == name


def test_a_ris_write_that_fails_after_the_file_exists_leaves_no_orphan_ris(env, monkeypatch):
    env.add("stroke_volume")
    make_pdf(env.dl / "a.pdf", [page(title_of("stroke_volume"), doi_of("stroke_volume"))])
    monkeypatch.setattr(R, "_sha256_file", _fail_once("sharing violation reading the new .ris"))
    row = env.run(execute=True)["rows"][0]
    assert row["action"] == "ERR_WRITE" and (env.dl / "a.pdf").exists()
    assert not list(env.lib.iterdir())


def test_a_ris_another_writer_put_there_survives_a_later_failure(env, monkeypatch):
    """Dispatcher lock for the Q-6 fix (2026-10-08): with the undo registered before write_ris, a
    False return (the .ris appeared meanwhile: not ours) disarms it, so a later failure of the PDF
    step never deletes another writer's .ris."""
    import shutil
    env.add("stroke_volume")
    make_pdf(env.dl / "a.pdf", [page(title_of("stroke_volume"), doi_of("stroke_volume"))])
    foreign = b"TY  - JOUR\nTI  - curated by hand\nER  - \n"
    seen = {}

    def someone_else_wrote_it(path, *a, **k):
        Path(path).write_bytes(foreign)                  # appeared between the exists check and the write
        seen["ris"] = Path(path)
        return False
    monkeypatch.setattr(R, "write_ris", someone_else_wrote_it)
    monkeypatch.setattr(shutil, "move", _fail_once("moving the PDF"))
    row = env.run(execute=True)["rows"][0]
    assert row["action"] == "ERR_WRITE" and (env.dl / "a.pdf").exists()
    assert seen["ris"].read_bytes() == foreign


# ================================================================ 9. Unpaywall's legacy error, fill_missing_dois on real state
def test_a_named_refused_host_in_the_legacy_error_still_maps_to_refused():
    from litpipe.outcomes import Kind, from_legacy
    assert from_legacy("HOST_REFUSED:arxiv.org") is Kind.REFUSED          # from_legacy returns the Kind
    assert from_legacy("HOST_REFUSED") is Kind.REFUSED


def test_a_fill_dry_run_leaves_the_real_state_module_without_a_doi_ra_entry(tmp_path, monkeypatch):
    import fill_missing_dois as F
    from litpipe import state
    from tests.test_w5c2_fill import CAND, _meta, _orphan
    from tests.test_rename_fill import STRICT
    monkeypatch.setattr(state, "DB_PATH", tmp_path / "state" / "litpipe_state.sqlite")
    monkeypatch.setattr(R, "STATE", state)                       # the real module, on a temp file

    def resolve(doi):
        R._kv_set("doi_ra", doi.split("/", 1)[0], "crossref")   # what doi_ra() does on a cache miss
        return _meta(doi)
    monkeypatch.setattr(R, "resolve_meta", resolve)
    monkeypatch.setattr(R, "write_ris", lambda *a, **k: True)
    lib, sc, _ = _orphan(tmp_path, f"https://doi.org/{CAND}\n{STRICT}\n")
    args = type("A", (), {"execute": False, "limit": None, "report_dir": None})()
    assert F.run_project("research_x", lib, args)["high"] == 1
    assert R.STATE is state
    assert state.kv_get("doi_ra", "10.1007") is None


# ================================================================ 10. pymupdf
_LEGACY_FITZ = __import__("re").compile(r"^\s*(?:import\s+fitz\b(?!\S)|from\s+fitz\b)", __import__("re").M)
_REPO = Path(__file__).resolve().parent.parent


def _repo_py_files():
    """The repo's .py files: in a git checkout, tracked plus untracked-not-ignored (a new module counts
    before it is committed; a gitignored local scratch folder does not). Otherwise every .py file."""
    import subprocess
    try:
        out = subprocess.run(["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard", "--", "*.py"],
                             cwd=_REPO, capture_output=True, check=True, timeout=60).stdout
    except (OSError, subprocess.SubprocessError):
        return list(_REPO.rglob("*.py"))
    return [_REPO / r for r in sorted(set(out.decode("utf-8").split("\0"))) if r and (_REPO / r).is_file()]


def test_no_non_test_module_anywhere_in_the_repo_imports_fitz():
    """tests/test_w5c2_pymupdf.py pins a list of 21 modules (every PyMuPDF user today); this scans
    every non-test module, so a module outside the list (preprint_fetch, lit_util, backfills/) that
    starts using PyMuPDF under the legacy name is caught too."""
    hits = []
    for p in _repo_py_files():
        rel = p.relative_to(_REPO).as_posix()
        if rel.startswith(("tests/", ".venv/", "vendor/", ".claude/")) or "/site-packages/" in rel:
            continue
        if _LEGACY_FITZ.search(p.read_text(encoding="utf-8", errors="replace")):
            hits.append(rel)
    assert hits == []


def test_the_pymupdf_floor_is_the_release_that_introduced_the_module_name():
    import re
    text = (_REPO / "pyproject.toml").read_text(encoding="utf-8")
    line = next(ln for ln in text.splitlines() if re.match(r'\s*"pymupdf', ln))
    floor = re.search(r">=\s*([\d.]+)", line).group(1)
    assert tuple(int(x) for x in floor.split(".")) >= (1, 24, 3)
    assert "fitz" not in line
