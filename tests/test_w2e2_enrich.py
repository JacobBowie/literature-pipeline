"""W2-E2: enrich_abstracts. The abstract cleaner fixes the four V0 shapes (heading, ISO Greek
entities, tags, JATS residue) on live Crossref fixtures (2026-10-05); Crossref is reached only
through litpipe.net (typed failures, host refusal stops the run); writes are batched with a commit
at the end and on an interrupt, and an abstract is never dropped or overwritten."""
import json
from pathlib import Path

import duckdb
import pytest

import enrich_abstracts as ea
import lit_util
from litpipe import config
from litpipe import text as T
from litpipe.outcomes import Kind
from tests.netmock import Reply

FIX = Path(__file__).resolve().parent / "fixtures" / "W2-E2"
JSON = {"Content-Type": "application/json"}
LIVE = ["crossref_escaped_tags.json", "crossref_abstract_heading.json", "crossref_double_escaped_jats.json"]


def fixture(name):
    return json.loads((FIX / name).read_text(encoding="utf-8"))


def raw_abstract(name):
    return fixture(name)["body"]["message"]["abstract"]


# ---------------------------------------------------------------- the cleaner: four V0 shapes
@pytest.mark.parametrize("raw, want", [
    # 1. a leftover heading (33 % of stored rows)
    ("<jats:title>Abstract</jats:title><jats:p>Background Heat stress.</jats:p>", "Background Heat stress."),
    ("<jats:title>ABSTRACT</jats:title>\n   <jats:p>In addition, rats.</jats:p>", "In addition, rats."),
    ("ABSTRACT: While performing design tasks.", "While performing design tasks."),
    ("Abstract. The aim was.", "The aim was."),
    ("AbstractBackground and study aim: kabaddi.", "Background and study aim: kabaddi."),
    ("ABSTRACTCigarette addiction.", "Cigarette addiction."),
    ("ABSTRACTA systematic review.", "A systematic review."),
    ("Abstract d-Psicose 3-epimerase.", "d-Psicose 3-epimerase."),
    ("<jats:title>Abstract</jats:title>", ""),
    # 2. ISO Greek entities (isogrk1, not in HTML5)
    ("<jats:p>&agr;-actinin and &bgr;-cells raise &Dgr;T</jats:p>", "α-actinin and β-cells raise ΔT"),
    # 3. tags, including escaped markup the old cleaner decoded after stripping
    ("<jats:p>VO<jats:sub>2</jats:sub>max rose</jats:p>", "VO2max rose"),
    ("<jats:p>&lt;b&gt;&lt;i&gt;Purpose:&lt;/i&gt;&lt;/b&gt; We evaluated.</jats:p>", "Purpose: We evaluated."),
    ("<p>&lt;sub&gt;2&lt;/sub&gt; and <i>in vivo</i></p>", "2 and in vivo"),
    # 4. JATS residue: CR references, no-break spaces, double and triple escaping
    ("<jats:p>First line&#x0D;second&nbsp;line</jats:p>", "First line second line"),
    ("<jats:p>&amp;lt;jats:p&amp;gt;Aim: p&amp;amp;lt;0,05 and 92,6&amp;plusmn;4,2%&amp;lt;/jats:p&amp;gt;</jats:p>",
     "Aim: p<0,05 and 92,6±4,2%"),
])
def test_clean_abstract_fixes_the_four_v0_shapes(raw, want):
    assert ea.clean_abstract(raw) == want


@pytest.mark.parametrize("s", ["Abstracts of the annual meeting were read.", "Abstraction of the signal.",
                               "ABSTRACTION matters.", "P < 0.05 and x<y held", "Heat; abstract reasoning"])
def test_clean_abstract_keeps_text_that_is_not_a_heading_or_markup(s):
    assert ea.clean_abstract(s) == s


def test_clean_abstract_keeps_compatibility_characters_nfc_not_nfkc():
    s = "<jats:p>VO₂max rose by 5 µg in m² (D´Amico)</jats:p>"
    assert ea.clean_abstract(s) == "VO₂max rose by 5 µg in m² (D´Amico)"


def test_clean_abstract_is_the_display_form_plus_heading_and_escaped_markup():
    """No heading and no escaped markup: exactly litpipe.text.display_field (ris_emit's AB form)."""
    s = "<jats:p>Heat &amp; humidity, M&uuml;ndel, baroreﬂex</jats:p>"
    assert ea.clean_abstract(s) == T.display_field(s) == "Heat & humidity, Mündel, baroreflex"


@pytest.mark.parametrize("name", LIVE)
def test_live_crossref_abstracts_come_out_clean(name):
    out = ea.clean_abstract(raw_abstract(name))
    assert out and "&" not in out.replace("R&D", "")
    assert not T._TAG.search(out)                  # no markup tag survives
    assert not out.lower().startswith("abstract")
    assert ea.clean_abstract(out) == out           # idempotent: safe on stored rows (W5-B)


def test_live_fixture_specifics():
    assert ea.clean_abstract(raw_abstract("crossref_escaped_tags.json")).startswith(
        "Purpose: We evaluated the incidence of acute kidney injury")
    heading = ea.clean_abstract(raw_abstract("crossref_abstract_heading.json"))
    assert heading.startswith("In addition to its health benefits") and "(P<0.001)" in heading
    jats = ea.clean_abstract(raw_abstract("crossref_double_escaped_jats.json"))
    assert jats.startswith("Introduction/Aim:") and "p<0,05" in jats and "92,6±4,2%" in jats
    assert "jats:p" not in jats and "&lt;" not in jats


@pytest.mark.parametrize("stored, want", [
    ("ABSTRACT Background The aim.", "Background The aim."),
    ("<b><i>Background:</i></b> Fermentable carbohydrates.", "Background: Fermentable carbohydrates."),
    ("&lt;jats:p&gt;Introduction/Aim: heat stress", "Introduction/Aim: heat stress"),
    ("Importance P &lt; .001 for trend", "Importance P < .001 for trend"),
])
def test_clean_abstract_on_stored_rows_shapes(stored, want):
    """The shapes the old cleaner left in paper_metadata (read-only census 2026-10-05)."""
    assert ea.clean_abstract(stored) == want


# ---------------------------------------------------------------- Crossref through litpipe.net
@pytest.fixture
def crossref(net_env, mock_server, monkeypatch):
    s = mock_server("127.0.0.1")
    monkeypatch.setattr(ea, "CROSSREF", s.url("/works/{doi}"))
    return s


def test_crossref_abstract_cleans_a_live_shape_through_net(crossref):
    crossref.script("/works/10.1242/jeb.242543",
                    Reply(200, json.dumps(fixture("crossref_abstract_heading.json")["body"]), JSON))
    out = ea.crossref_abstract("10.1242/JEB.242543")
    assert out.startswith("In addition to its health benefits")
    hit = crossref.hits_for("/works/10.1242/jeb.242543")[0]
    assert hit.headers["User-Agent"].startswith("literature-pipeline/")   # litpipe.net's identity


@pytest.mark.parametrize("reply, kind, permanent", [
    (Reply(404, "Resource not found.", {"Content-Type": "text/plain"}), Kind.NO_MATCH, True),
    (Reply(410), Kind.NO_MATCH, True),
    (Reply(400, "bad"), Kind.ERROR, True),
    (Reply(503), Kind.OUTAGE, False),
    (Reply(close=True), Kind.TRANSPORT, False),
    (Reply(200, "<html>maintenance</html>", {"Content-Type": "text/html"}), Kind.OUTAGE, False),
    (Reply(200, '{"status": "ok"}', JSON), Kind.OUTAGE, False),
])
def test_crossref_failures_are_typed_never_no_abstract(crossref, reply, kind, permanent):
    crossref.script("/works/10.1234/x.1", reply)
    with pytest.raises(ea.CrossRefError) as e:
        ea.crossref_abstract("10.1234/x.1")
    assert e.value.kind is kind and e.value.permanent is permanent and not e.value.host_blocked


def test_a_403_refuses_crossref_for_the_run(crossref, net_env):
    crossref.script("/works/10.1234/x.2", Reply(403))
    with pytest.raises(ea.CrossRefError) as e:
        ea.crossref_abstract("10.1234/x.2")
    assert e.value.kind is Kind.REFUSED and e.value.host_blocked and not e.value.permanent


@pytest.mark.parametrize("doi, path", [
    # SICI: '<' and '>' percent-encoded in the path (DOI Handbook 2025 4.7, RFC 3986), lower-cased
    ("10.1002/(SICI)1097-4636(199601)30:1<1::AID-JBM1>3.0.CO;2-N",
     "/works/10.1002/(sici)1097-4636(199601)30:1%3C1::aid-jbm1%3E3.0.co;2-n"),
    # a fragment is not part of the DOI: cut by normalising, never sent as %23
    ("10.1056/NEJMc1113675#sa3", "/works/10.1056/nejmc1113675"),
])
def test_doi_is_normalised_then_percent_encoded_in_the_path(crossref, doi, path, monkeypatch):
    from litpipe import doi as D
    from litpipe import net
    assert path == "/works/" + D.encode_path(doi)
    crossref.script(path, Reply(200, json.dumps({"message": {"abstract": "<jats:p>Text.</jats:p>"}}), JSON))
    sent, real_get = [], net.get
    monkeypatch.setattr(net, "get", lambda url, **kw: (sent.append(url), real_get(url, **kw))[1])
    assert ea.crossref_abstract(doi) == "Text."
    # the URL this module builds is already encoded (the transport would also quote '<', hiding a
    # raw DOI on the wire), and the server sees exactly that path
    assert [u[u.index("/works/"):] for u in sent] == [path]
    assert [h.path for h in crossref.hits] == [path]


def test_not_a_doi_is_permanent_and_nothing_is_sent(crossref):
    with pytest.raises(ea.CrossRefError) as e:
        ea.crossref_abstract("not-a-doi")
    assert e.value.permanent and e.value.kind is Kind.NO_MATCH
    assert crossref.hits == []


def test_record_without_abstract_is_empty_string(crossref):
    crossref.script("/works/10.1234/x.3", Reply(200, json.dumps({"message": {"DOI": "10.1234/x.3"}}), JSON))
    assert ea.crossref_abstract("10.1234/x.3") == ""


def test_module_binds_no_email_ua_or_legacy_transport():
    src = Path(ea.__file__).read_text(encoding="utf-8")
    assert not hasattr(ea, "EMAIL") and not hasattr(ea, "UA")
    assert "import lit_net" not in src and "import requests" not in src
    assert "~50 req/s" not in src and "by email address" in src     # the docstring's rate claim


# ---------------------------------------------------------------- run(): batches, attempt state
def make_db(tmp_path, rows):
    p = str(tmp_path / "t.duckdb")
    con = duckdb.connect(p)
    con.execute("CREATE TABLE paper_metadata (doi VARCHAR PRIMARY KEY, abstract VARCHAR)")
    con.executemany("INSERT INTO paper_metadata (doi, abstract) VALUES (?, ?)", rows)
    con.close()
    return p


def read(db, sql):
    con = duckdb.connect(db, read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def fake(results, log=None):
    """crossref_abstract double: results[doi] is a string, an exception instance, or a callable."""
    def f(doi, timeout=15):
        if log is not None:
            log.append(doi)
        r = results[doi]
        if isinstance(r, BaseException):
            raise r
        return r() if callable(r) else r
    return f


def test_batches_commit_per_n_rows_and_at_the_end(tmp_path, monkeypatch):
    db = make_db(tmp_path, [(f"10.1/{i}", None) for i in range(5)])
    monkeypatch.setattr(ea, "crossref_abstract", fake({f"10.1/{i}": f"text {i}" for i in range(5)}))
    res = ea.run(db=db, commit_every=2)
    assert res["commits"] == 3 and res["rows_written"] == 5 and res["hits"] == 5
    assert read(db, "SELECT count(*) FROM paper_metadata WHERE abstract LIKE 'text %'")[0][0] == 5


def test_typed_attempt_state_is_recorded(tmp_path, monkeypatch):
    db = make_db(tmp_path, [("10.1/hit", None), ("10.2/miss", None), ("10.3/gone", None),
                            ("10.4/down", None), ("10.5/waf", None)])
    monkeypatch.setattr(ea, "crossref_abstract", fake({
        "10.1/hit": "An abstract", "10.2/miss": "",
        "10.3/gone": ea.CrossRefError("HTTP 404", status=404),
        "10.4/down": ea.CrossRefError("HTTP 503 after 6 retries", status=503, kind=Kind.OUTAGE),
        "10.5/waf": ea.CrossRefError("HTTP 403", status=403)}))
    ea.run(db=db)
    got = {d: (o, s) for d, o, s in read(db, "SELECT doi, outcome, status FROM abstract_attempts")}
    assert got == {"10.1/hit": ("OK", "200"), "10.2/miss": ("NOT_AVAILABLE", "200"),
                   "10.3/gone": ("NO_MATCH", "404"), "10.4/down": ("OUTAGE", "503"),
                   "10.5/waf": ("REFUSED", "403")}
    marked = {d for d, m in read(db, "SELECT doi, abstract_attempted_at IS NOT NULL FROM paper_metadata") if m}
    assert marked == {"10.1/hit", "10.2/miss", "10.3/gone"}       # transient ones retried next run


def test_existing_abstracts_are_never_targets_or_overwritten(tmp_path, monkeypatch):
    db = make_db(tmp_path, [("10.1/has", "Kept abstract"), ("10.2/empty", ""), ("10.3/null", None)])
    log = []
    monkeypatch.setattr(ea, "crossref_abstract", fake({
        "10.2/empty": ea.CrossRefError("HTTP 503", status=503), "10.3/null": "New text"}, log))
    ea.run(db=db, retry_after_days=0)
    assert sorted(log) == ["10.2/empty", "10.3/null"]
    got = dict(read(db, "SELECT doi, abstract FROM paper_metadata"))
    assert got == {"10.1/has": "Kept abstract", "10.2/empty": "", "10.3/null": "New text"}


def test_a_hit_never_overwrites_an_abstract_written_meanwhile(tmp_path):
    db = make_db(tmp_path, [("10.1/x", "Written by another tool")])
    con = duckdb.connect(db)
    con.execute("ALTER TABLE paper_metadata ADD COLUMN IF NOT EXISTS abstract_attempted_at TIMESTAMP")
    con.execute(ea.ATTEMPTS_DDL)
    w = ea._Writer(con, 10)
    w.add("10.1/x", "Fetched text", True, "OK", "200", "")
    w.finish()
    con.close()
    assert read(db, "SELECT abstract FROM paper_metadata")[0][0] == "Written by another tool"


def test_interrupt_commits_the_buffered_batch(tmp_path, monkeypatch):
    db = make_db(tmp_path, [(f"10.1/{i}", None) for i in range(6)])
    results = {f"10.1/{i}": f"text {i}" for i in range(3)}
    results["10.1/3"] = KeyboardInterrupt()
    results.update({"10.1/4": "never", "10.1/5": "never"})
    monkeypatch.setattr(ea, "crossref_abstract", fake(results))
    monkeypatch.setattr("sys.argv", ["enrich_abstracts.py", "--db", db, "--commit-every", "100"])
    assert ea.main() == 130
    got = dict(read(db, "SELECT doi, abstract FROM paper_metadata"))
    assert got == {"10.1/0": "text 0", "10.1/1": "text 1", "10.1/2": "text 2",
                   "10.1/3": None, "10.1/4": None, "10.1/5": None}


def test_interrupt_during_a_commit_still_keeps_the_batch(tmp_path, monkeypatch):
    db = make_db(tmp_path, [(f"10.1/{i}", None) for i in range(4)])
    monkeypatch.setattr(ea, "crossref_abstract", fake({f"10.1/{i}": f"text {i}" for i in range(4)}))
    real = ea._Writer._write
    calls = {"n": 0}

    def flaky(self, row):
        calls["n"] += 1
        if calls["n"] == 2:                 # Ctrl-C inside the first batch's transaction
            raise KeyboardInterrupt
        return real(self, row)

    monkeypatch.setattr(ea._Writer, "_write", flaky)
    res = ea.run(db=db, commit_every=2)
    assert res["interrupted"]
    got = dict(read(db, "SELECT doi, abstract FROM paper_metadata"))
    assert got["10.1/0"] == "text 0" and got["10.1/1"] == "text 1"   # rolled back, then rewritten


def test_a_failing_row_does_not_drop_its_batch(tmp_path, monkeypatch):
    db = make_db(tmp_path, [(f"10.1/{i}", None) for i in range(4)])
    monkeypatch.setattr(ea, "crossref_abstract", fake({f"10.1/{i}": f"text {i}" for i in range(4)}))
    real = ea._Writer._write

    def bad_row(self, row):
        if row[0] == "10.1/2":
            raise duckdb.ConstraintException("simulated")
        return real(self, row)

    monkeypatch.setattr(ea._Writer, "_write", bad_row)
    monkeypatch.setattr("sys.argv", ["enrich_abstracts.py", "--db", db, "--commit-every", "4"])
    assert ea.main() == 1                                       # a write failure is not a clean run
    got = dict(read(db, "SELECT doi, abstract FROM paper_metadata"))
    assert got == {"10.1/0": "text 0", "10.1/1": "text 1", "10.1/2": None, "10.1/3": "text 3"}


def test_a_blocked_host_stops_the_run_and_leaves_the_rest(tmp_path, monkeypatch):
    db = make_db(tmp_path, [(f"10.1/{i}", None) for i in range(4)])
    log = []
    monkeypatch.setattr(ea, "crossref_abstract", fake({
        "10.1/0": "text 0",
        "10.1/1": ea.CrossRefError("HTTP 403 (1 consecutive); host refused for the run", status=403,
                                   kind=Kind.REFUSED, host_blocked=True),
        "10.1/2": "never", "10.1/3": "never"}, log))
    res = ea.run(db=db)
    assert log == ["10.1/0", "10.1/1"] and res["stopped_early"] and res["attempted"] == 2
    att = dict(read(db, "SELECT doi, outcome FROM abstract_attempts"))
    assert att == {"10.1/0": "OK", "10.1/1": "REFUSED"}
    assert read(db, "SELECT abstract FROM paper_metadata WHERE doi='10.1/0'")[0][0] == "text 0"


def test_run_through_real_net_stops_after_a_403_and_sends_nothing_more(crossref, tmp_path, capsys):
    db = make_db(tmp_path, [("10.1234/a.1", None), ("10.1234/a.2", None), ("10.1234/a.3", None)])
    crossref.script("/works/10.1234/a.1", Reply(200, json.dumps(fixture("crossref_escaped_tags.json")["body"]), JSON))
    crossref.script("/works/10.1234/a.2", Reply(403))
    crossref.script("/works/10.1234/a.3", Reply(200, "{}", JSON))
    res = ea.run(db=db)
    assert res["hits"] == 1 and res["stopped_early"]
    assert crossref.hits_for("/works/10.1234/a.3") == []
    assert read(db, "SELECT abstract FROM paper_metadata WHERE doi='10.1234/a.1'")[0][0].startswith("Purpose: We")
    out = capsys.readouterr()
    for text in (out.out, out.err, json.dumps(read(db, "SELECT * FROM abstract_attempts"), default=str)):
        assert "tester@litpipe-test.org" not in text and "email=" not in text and "mailto:" not in text


def test_default_db_comes_from_config_db_dir(tmp_path, monkeypatch):
    reg = tmp_path / "projects.json"
    reg.write_text(json.dumps({"db_dir": str(tmp_path / "dbs"), "projects": {}}), encoding="utf-8")
    monkeypatch.setattr(config, "CONFIG_PATH", reg)
    assert ea.default_db() == str(tmp_path / "dbs" / "portfolio.duckdb")
    reg.write_text(json.dumps({"projects": {}}), encoding="utf-8")
    monkeypatch.setattr(lit_util, "PROJECTS_ROOT", tmp_path / "root")
    assert ea.default_db() == str(tmp_path / "root" / "_references" / "portfolio.duckdb")


def test_an_explicit_db_is_the_only_db_opened(tmp_path, monkeypatch):
    db = make_db(tmp_path, [("10.1/x", None)])
    opened = []
    real = lit_util.connect_db
    monkeypatch.setattr(lit_util, "connect_db", lambda p, **k: (opened.append(p), real(p, **k))[1])
    monkeypatch.setattr(ea, "default_db", lambda: pytest.fail("default DB resolved"))
    monkeypatch.setattr(ea, "crossref_abstract", fake({"10.1/x": ""}))
    ea.run(db=db)
    assert opened == [db]
