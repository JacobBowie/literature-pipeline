"""W2-A2: jats_to_text. The JATS parser (authors exclude editors, floats outside <body> (V1-N3),
multi-panel figures, citation fields for a .ris from the sidecar), the BioC parser (same sidecar
shape, for author manuscripts: N-B4) and the BioC fetch (absent is a 200 text/html page; a 429 is
never retried). Offline: fixtures in tests/fixtures/W2-A2/, transports stubbed."""
import json
from pathlib import Path

import pytest
from requests.structures import CaseInsensitiveDict

import jats_to_text as J
from litpipe import hosts, net
from litpipe.outcomes import Kind

FIX = Path(__file__).resolve().parent / "fixtures" / "W2-A2"
JSONH = {"Content-Type": "application/json"}
HTML = {"Content-Type": "text/html"}


def stub(monkeypatch, *replies):
    sent = []

    def fake(method, url, hdrs, body, timeout, max_bytes):
        sent.append((method, url, dict(hdrs)))
        r = replies[min(len(sent), len(replies)) - 1]
        if isinstance(r, Exception):
            return net._Raw(error=f"{type(r).__name__}: {r}")
        status, data, h = (tuple(r) + (None,))[:3]
        data = data if isinstance(data, bytes) else data.encode("utf-8")
        return net._Raw(status, CaseInsensitiveDict(h or JSONH), data, data[:net.CHUNK], len(data))

    monkeypatch.setitem(net._TRANSPORTS, "requests", fake)
    monkeypatch.setitem(net._TRANSPORTS, "urllib", fake)
    return sent


def jats(front_meta="", body="", floats="", back="", wrap=False):
    x = (f'<article xmlns:xlink="http://www.w3.org/1999/xlink"><front><journal-meta><journal-title>J Test'
         f'</journal-title></journal-meta><article-meta>{front_meta}</article-meta></front>'
         f'<body>{body}</body>{back}{floats}</article>')
    return (f"<pmc-articleset>{x}</pmc-articleset>" if wrap else x).encode("utf-8")


def contrib(ctype, sur, giv="A"):
    t = f' contrib-type="{ctype}"' if ctype is not None else ""
    return f"<contrib{t}><name><surname>{sur}</surname><given-names>{giv}</given-names></name></contrib>"


def fig(fid, label, *hrefs):
    g = "".join(f'<graphic xlink:href="{h}"/>' for h in hrefs)
    return f'<fig id="{fid}"><label>{label}</label><caption><p>{label} caption</p></caption>{g}</fig>'


# ------------------------------------------------------------------ authors exclude editors
def test_editors_are_not_authors_on_the_real_record():
    """The 2026-09-25 efetch record of PMC11592912: 1 author, 3 editors (contrib-type="editor")."""
    out = J.parse_jats((FIX / "jats_PMC11592912_efetch.xml").read_bytes())
    assert out["authors"] == ["Williams Nick"]


def test_untyped_contribs_fall_back_but_editors_never_do():
    """The old fallback (no author-typed contrib) took EVERY contrib, editors included."""
    meta = (f"<contrib-group>{contrib(None, 'Alpha')}{contrib(None, 'Beta')}</contrib-group>"
            f"<contrib-group>{contrib('editor', 'Ed')}{contrib('translator', 'Tr')}</contrib-group>")
    assert J.parse_jats(jats(meta))["authors"] == ["Alpha A", "Beta A"]


def test_untyped_contribs_in_an_editor_group_are_not_authors():
    meta = (f"<contrib-group>{contrib(None, 'Alpha')}</contrib-group>"
            f'<contrib-group content-type="editors">{contrib(None, "Ed")}</contrib-group>')
    assert J.parse_jats(jats(meta))["authors"] == ["Alpha A"]


def test_author_type_is_case_insensitive_and_wins():
    meta = f"<contrib-group>{contrib('Author', 'Alpha')}{contrib(None, 'Untyped')}{contrib('editor', 'Ed')}</contrib-group>"
    assert J.parse_jats(jats(meta))["authors"] == ["Alpha A"]


def test_only_editors_gives_no_authors():
    meta = f"<contrib-group>{contrib('editor', 'Ed')}</contrib-group>"
    assert J.parse_jats(jats(meta))["authors"] == []


# ------------------------------------------------------------------ figures and tables (V1-N3)
def test_all_figures_of_the_real_record_are_found_in_label_order():
    """V1-N3 settled: the sidecar listed 2 figures (those in <body>); 6 sat in <floats-group>."""
    out = J.parse_jats((FIX / "jats_PMC11592912_efetch.xml").read_bytes())
    assert [f["label"] for f in out["figures"]] == [f"Figure {i}" for i in range(1, 9)]
    f4 = out["figures"][3]
    assert f4["graphic_href"] == "entropy-26-00970-g004a.jpg"
    assert f4["graphic_hrefs"] == [f"entropy-26-00970-g004{c}.jpg" for c in "abcd"]
    assert "graphic_hrefs" not in out["figures"][0]            # single-panel figures keep the old shape
    n_panels = sum(len(f.get("graphic_hrefs") or [f["graphic_href"]]) for f in out["figures"])
    assert n_panels == 18                                       # = the 18 S3 media images


def test_floats_group_and_back_tables_are_read():
    floats = f"<floats-group>{fig('f2', 'Figure 2', 'g2.jpg')}<table-wrap><label>Table 1</label><caption><p>T</p></caption><table><tr><td>a</td><td>b</td></tr></table></table-wrap></floats-group>"
    back = f"<back><app-group><app>{fig('fa', 'Figure 3', 'g3.jpg')}</app></app-group></back>"
    body = f"<sec><title>Results</title><p>x</p>{fig('f1', 'Figure 1', 'g1.jpg')}</sec>"
    out = J.parse_jats(jats(body=body, floats=floats, back=back))
    assert [f["graphic_href"] for f in out["figures"]] == ["g1.jpg", "g2.jpg", "g3.jpg"]
    assert [t["label"] for t in out["tables"]] == ["Table 1"] and out["tables"][0]["text"] == "a | b"
    assert "[Figure 2] Figure 2 caption" in out["text"]


def test_unnumbered_labels_keep_document_order():
    body = f"<sec><title>R</title>{fig('a', 'Graphical abstract', 'ga.jpg')}{fig('b', 'Figure 1', 'g1.jpg')}</sec>"
    out = J.parse_jats(jats(body=body))
    assert [f["graphic_href"] for f in out["figures"]] == ["ga.jpg", "g1.jpg"]


def test_articleset_wrapper_is_unwrapped():
    out = J.parse_jats(jats('<article-id pub-id-type="pmcid">PMC9</article-id><article-title>T</article-title>', wrap=True))
    assert out["pmcid"] == "PMC9" and out["title"] == "T"


# ------------------------------------------------------------------ citation fields (REG-I04 writer side)
def test_citation_fields_for_a_ris_from_the_sidecar():
    meta = ('<article-id pub-id-type="pmc">123</article-id><article-id pub-id-type="doi">10.1/x</article-id>'
            "<article-title>T</article-title><pub-date><year>2021</year></pub-date>"
            "<volume>12</volume><issue>3</issue><fpage>100</fpage><lpage>110</lpage>")
    out = J.parse_jats(jats(meta))
    assert (out["pmcid"], out["doi"], out["journal"], out["year"]) == ("PMC123", "10.1/x", "J Test", "2021")
    assert (out["volume"], out["issue"], out["pages"]) == ("12", "3", "100-110")
    out = J.parse_jats(jats("<article-title>T</article-title><elocation-id>e0123</elocation-id>"))
    assert out["pages"] == "e0123"


# ------------------------------------------------------------------ parse_bioc
SHAPE = {"pmcid", "pmid", "doi", "title", "subtitle", "year", "journal", "volume", "issue", "pages",
         "authors", "abstract", "sections", "figures", "tables", "formulas", "n_formulas",
         "formula_failures", "text"}


def test_bioc_author_manuscript_builds_the_jats_shape():
    out = J.parse_bioc((FIX / "bioc_am_PMC7983342.json").read_bytes())
    assert SHAPE <= set(out) and set(J.parse_jats(jats()).keys()) <= set(out)
    assert (out["pmcid"], out["pmid"], out["doi"]) == ("PMC7983342", "33581294", "10.1016/j.resp.2021.103638")
    assert out["title"].startswith("Dyspnea during exercise")
    assert out["authors"][:2] == ["Spencer Matthew D.", "Balmain Bryce N."]
    assert "authors_may_include_editors" not in out          # an author manuscript lists authors only
    assert out["abstract"].startswith("Temporal responses")
    assert [s["title"] for s in out["sections"]][:2] == ["Introduction", "Methods"]
    assert "\n## Participants:" in out["sections"][1]["text"]   # a title_2 becomes a sub-heading
    assert out["figures"][0]["label"] == "Figure 1" and out["figures"][0]["graphic_href"].endswith("f0001.jpg")
    assert out["tables"][0]["label"] == "Table 1" and " | " in out["tables"][0]["text"]
    assert out["formulas"] == [] and out["n_formulas"] == 0
    assert out["text"].startswith("# Dyspnea") and "## Abstract" in out["text"]
    assert "References" not in [s["title"] for s in out["sections"]]


def test_bioc_accepts_str_dict_list_and_document():
    raw = (FIX / "bioc_am_PMC7983342.json").read_text(encoding="utf-8")
    coll = json.loads(raw)
    a = J.parse_bioc(raw)
    assert J.parse_bioc(coll) == a == J.parse_bioc(coll[0]) == J.parse_bioc(coll[0]["documents"][0])


def test_bioc_names_include_editors_so_they_are_flagged():
    """Probe 2026-09-30: BioC name_N lists the article's 1 author AND its 3 editors, without roles."""
    out = J.parse_bioc((FIX / "bioc_oa_editors_PMC11592912.json").read_bytes())
    assert out["authors"][0] == "Williams Nick" and len(out["authors"]) == 4
    assert out["authors_may_include_editors"] is True
    out = J.parse_bioc((FIX / "bioc_oa_editors_PMC11592912.json").read_bytes(), authors=["Williams Nick"])
    assert out["authors"] == ["Williams Nick"] and "authors_may_include_editors" not in out
    assert out["pages"] == "970"                                    # elocation-id


def test_bioc_error_page_is_not_a_document():
    with pytest.raises(ValueError):
        J.parse_bioc((FIX / "bioc_absent.html").read_bytes())
    with pytest.raises(ValueError):
        J.parse_bioc([])


# ------------------------------------------------------------------ fetch_bioc
@pytest.fixture
def env(net_env):
    return net_env


def test_bioc_json_ok(env, monkeypatch):
    sent = stub(monkeypatch, (200, (FIX / "bioc_am_PMC7983342.json").read_bytes(), JSONH))
    data, status = J.fetch_bioc("PMC7983342")
    assert status == "OK" and J.parse_bioc(data)["pmcid"] == "PMC7983342"
    url = sent[0][1]
    assert url.startswith("https://www.ncbi.nlm.nih.gov/research/bionlp/RESTful/pmcoa.cgi/BioC_json/PMC7983342/unicode")
    assert hosts.prohibited(url) is None


def test_bioc_absent_is_a_200_html_page(env, monkeypatch):
    stub(monkeypatch, (200, (FIX / "bioc_absent.html").read_bytes(), HTML))
    assert J.fetch_bioc("PMC8029826") == (None, "NOT_AVAILABLE")
    assert J.fetch_bioc_outcome("PMC8029826").kind is Kind.NOT_AVAILABLE


def test_bioc_429_is_not_retried_and_refuses_the_host(env, monkeypatch):
    """Probe 2026-09-30: a third request 0.65 s after the second got 429 (HTML, empty Retry-After)."""
    sent = stub(monkeypatch, (429, (FIX / "bioc_429.html").read_bytes(), {"Content-Type": "text/html", "Retry-After": ""}))
    data, status = J.fetch_bioc("PMC11592912")
    assert data is None and status == "HTTP_429" and len(sent) == 1
    assert env.state.is_refused("www.ncbi.nlm.nih.gov")
    assert J.fetch_bioc("PMC1")[1].startswith("REFUSED") and len(sent) == 1   # nothing more sent
    assert 429 in hosts.policy("www.ncbi.nlm.nih.gov").retry.statuses        # the override was scoped


def test_bioc_transport_failure(env, monkeypatch):
    stub(monkeypatch, ConnectionError("reset"))
    data, status = J.fetch_bioc("PMC1")
    assert data is None and status.startswith("TRANSPORT: ")


def test_bioc_json_that_is_not_bioc(env, monkeypatch):
    stub(monkeypatch, (200, b'{"hello": 1}', JSONH))
    assert J.fetch_bioc("PMC1") == (None, "EMPTY_OR_NON_JSON")


# ------------------------------------------------------------------ fetch_fulltext
def test_fetch_fulltext_prefers_jats(env, monkeypatch):
    sent = stub(monkeypatch, (200, jats("<article-title>From JATS</article-title>"), {"Content-Type": "application/xml"}))
    parsed, status, source = J.fetch_fulltext("PMC1")
    assert (status, source, parsed["title"]) == ("OK", "jats", "From JATS") and len(sent) == 1


def test_fetch_fulltext_falls_back_to_bioc_on_not_available(env, monkeypatch):
    stub(monkeypatch, (500, (FIX / "epmc_fulltextxml_500.json").read_bytes(), JSONH),
         (200, (FIX / "bioc_am_PMC7983342.json").read_bytes(), JSONH))
    parsed, status, source = J.fetch_fulltext("PMC7983342")
    assert (status, source) == ("OK", "bioc") and parsed["doi"] == "10.1016/j.resp.2021.103638"


def test_fetch_fulltext_does_not_mask_an_outage(env, monkeypatch):
    sent = stub(monkeypatch, (503, b"busy", {"Content-Type": "text/plain"}))
    parsed, status, source = J.fetch_fulltext("PMC1")
    assert parsed is None and status == "HTTP_503" and source == ""
    assert {u.split("/")[2] for _, u, _ in sent} == {"www.ebi.ac.uk"}
