"""backfills/placeholder_edges.py (W3-B step 3): a dry run lists the rows whose seed DOI is a
template placeholder (never an empty seed DOI); --commit removes exactly those rows after writing a
.bak, and every kept record keeps its bytes. The CSV path is an argument; a temp file here."""
import csv
import hashlib
import io
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "backfills"))
import placeholder_edges as fpe  # noqa: E402

HEADER = "seed_pdf,seed_doi,citing_paper_id,citing_doi,citing_title,citing_year,citing_authors,citing_venue,citing_cited_by,citing_abstract\n"


def make_csv(tmp_path, crlf=False):
    rows = [
        'A.pdf,10.5555/seed.0001,p1,10.5555/c.1,Title one,2020,Ann,V,1,"An abstract, with a comma"',
        'B.pdf,10.1145/nnnnnnn.nnnnnnn,p2,10.5555/c.2,Title two,2021,Bob,V,2,"Line one\nline two of a quoted abstract"',
        'B.pdf,10.1145/nnnnnnn.nnnnnnn,p3,10.5555/c.3,Title three,2022,Cy,V,3,',
        'C.pdf,,p4,10.5555/c.4,Title four with an empty seed,2022,Di,V,4,',
        'D.pdf,10.1016/j.amepre,p5,10.5555/c.5,A truncated seed DOI,2023,Ed,V,5,',
        'E.pdf,10.5555/seed.0002,p6,10.5555/c.6,"Quoted, ""title""   with a line separator",2024,Flo,V,6,',
    ]
    nl = "\r\n" if crlf else "\n"
    text = HEADER.replace("\n", nl) + nl.join(rows) + nl
    p = tmp_path / "_forward_citations.csv"
    p.write_bytes(text.encode("utf-8"))
    return p


def sha(p):
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def test_dry_run_lists_placeholders_never_empty_seeds_and_writes_nothing(tmp_path, capsys):
    p = make_csv(tmp_path)
    before = sha(p)
    res = fpe.run(csv_path=p)
    assert res["exit_code"] == 0 and not res["committed"]
    assert res["rows"] == 6 and res["empty_seed_rows"] == 1
    assert res["placeholder_rows"] == 3                      # two template rows and one truncated DOI
    assert res["by_seed"] == {"10.1145/nnnnnnn.nnnnnnn": 2, "10.1016/j.amepre": 1}
    assert sha(p) == before and sorted(x.name for x in tmp_path.iterdir()) == ["_forward_citations.csv"]
    out = capsys.readouterr().out
    assert "--commit" in out and "nothing written" in out.lower()


def test_is_placeholder_seed_never_flags_an_empty_value():
    assert not fpe.is_placeholder_seed("") and not fpe.is_placeholder_seed("   ")
    assert fpe.is_placeholder_seed("10.1145/NNNNNNN.NNNNNNN")
    assert not fpe.is_placeholder_seed("10.5555/seed.0001")


def test_list_out_writes_the_listed_rows(tmp_path):
    p = make_csv(tmp_path)
    out = tmp_path / "listed.csv"
    fpe.run(csv_path=p, list_out=out)
    rows = list(csv.DictReader(open(out, encoding="utf-8")))
    assert [r["line"] for r in rows] == ["3", "5", "7"]       # physical line numbers; row B spans two lines
    assert {r["seed_doi"] for r in rows} == {"10.1145/nnnnnnn.nnnnnnn", "10.1016/j.amepre"}


def test_commit_removes_exactly_those_rows_after_a_backup_and_keeps_bytes(tmp_path):
    for crlf in (False, True):
        d = tmp_path / ("crlf" if crlf else "lf")
        d.mkdir()
        p = make_csv(d, crlf=crlf)
        original = p.read_bytes()
        res = fpe.run(csv_path=p, commit=True)
        assert res["committed"] and res["placeholder_rows"] == 3
        assert Path(res["backup"]).name == "_forward_citations.csv.bak"
        assert Path(res["backup"]).read_bytes() == original
        text = p.read_bytes().decode("utf-8")
        rows = list(csv.DictReader(io.StringIO(text)))
        assert [r["citing_paper_id"] for r in rows] == ["p1", "p4", "p6"]
        assert rows[2]["citing_title"] == 'Quoted, "title"   with a line separator'
        recs = list(fpe.records(original.decode("utf-8")))
        expect = "".join(raw for _, raw, f in recs if f[2] in ("citing_paper_id", "p1", "p4", "p6"))
        assert text == expect                                               # original bytes, in order
        assert ("\r\n" in text) == crlf
        again = fpe.run(csv_path=p, commit=True)
        assert again["placeholder_rows"] == 0 and not again["committed"]
        assert not (d / "_forward_citations.csv.bak.1").exists()


def test_a_second_backup_never_overwrites_the_first(tmp_path):
    p = make_csv(tmp_path)
    (tmp_path / "_forward_citations.csv.bak").write_text("an older backup", encoding="utf-8")
    res = fpe.run(csv_path=p, commit=True)
    assert Path(res["backup"]).name == "_forward_citations.csv.bak.1"
    assert (tmp_path / "_forward_citations.csv.bak").read_text(encoding="utf-8") == "an older backup"


def test_usage_errors_exit_1(tmp_path):
    assert fpe.main([str(tmp_path / "missing.csv")]) == 1
    p = tmp_path / "x.csv"
    p.write_text("a,b\n1,2\n", encoding="utf-8")
    assert fpe.main([str(p)]) == 1
    p.write_text("", encoding="utf-8")
    assert fpe.main([str(p)]) == 1


def test_a_bom_is_kept(tmp_path):
    p = make_csv(tmp_path)
    p.write_bytes(b"\xef\xbb\xbf" + p.read_bytes())
    fpe.run(csv_path=p, commit=True)
    assert p.read_bytes().startswith(b"\xef\xbb\xbfseed_pdf,seed_doi")
