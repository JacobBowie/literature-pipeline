"""Item 3 (week-1 health F1, 2026-11-03 identity review): wrong-content documents.

22 held "papers" were repository boilerplate (a PURE privacy notice, an ANID open-access policy,
a citation guide, a legal notice, a terms-of-use page), and the 11-03 review found 4 more (a BMJ
self-archiving policy, an NFHS survey, a Universidade de Evora regulation; on 11-04 a CMS code
list). The Teesside privacy notice came back on 10-14 and 11-02 after being archived on 06-23.
"""
import unpaywall_fetch_v2 as U
from litpipe import boilerplate as bp

# First-page text as pdftotext or pymupdf gives it: line breaks, en dashes, accents.
DOCS = {
    "pure_research_portal_privacy_notice":
        "Legal & Governance Services\nResearch Portal – PURE Software System\nThis privacy notice is to be read",
    "anid_open_access_policy":
        "Política de acceso abierto a la información\ncientífica y a datos de investigación",
    "uchile_citation_guide":
        "Redacción de Citas Bibliográficas\nGuía y ejemplos\nUniversidad de Chile",
    "aviso_legal_privacy_policy":
        "Aviso Legal y Política de Privacidad\n1. DATOS IDENTIFICATIVOS:",
    "bmj_self_archiving_policy":
        "BMJ Authors Self-Archiving Policy, September 2018\nNon Open Access Articles",
    "nfhs_participation_survey":
        "HIGH SCHOOL ATHLETICS\nPARTICIPATION SURVEY\nConducted By THE NATIONAL FEDERATION",
    "cms_therapy_code_list":
        "Therapy Code List: 2026 Annual Update\nRelated CR Release Date: September 25, 2025\nMLN Matters Number: MM14250",
}


def test_each_new_document_is_boilerplate_by_text():
    for tag, text in DOCS.items():
        assert U.boilerplate_of(b"%PDF-1.4 not these bytes", text) == tag, tag


def test_a_phrase_split_across_lines_still_matches():
    assert U.boilerplate_of(b"", "Lippincott Journal\nPortfolio") == "lww_author_permission_guidelines"


def test_the_observed_bytes_are_listed():
    """The exact files seen (md5 read on FRED); the Chapman page and the scanned Evora regulation
    are listed by bytes only."""
    for md5, tag in {"bbb20e478ed231ccc6575eab698103ad": "pure_research_portal_privacy_notice",
                     "0de56e6fe8e4916e06cf436c17c60cdc": "chapman_rights_terms_of_use",
                     "f82beb0b0087a5c295463019502802d7": "uevora_repository_regulation",
                     "4d0cd1dae38074391f11dbddbb52e9ff": "cms_therapy_code_list"}.items():
        assert U.KNOWN_BOILERPLATE_MD5[md5] == tag


def test_a_repository_cover_page_alone_is_not_boilerplate():
    """A cover page can sit in front of a real paper, so its text is never a phrase."""
    cover = ("Rights and Terms of Use\nAll material in Chapman University Digital Commons, unless "
             "stated otherwise, is freely available")
    assert U.boilerplate_of(b"", cover) is None


def test_an_article_is_not_boilerplate():
    text = ("ORIGINAL RESEARCH\nConsumption of an Anthocyanin-Rich Extract Made From New Zealand "
            "Blackcurrants Prior to Exercise May Assist Recovery From Oxidative Stress")
    assert U.boilerplate_of(b"", text) is None


def test_is_known_boilerplate_reads_the_text_of_a_file(tmp_path, monkeypatch):
    """The file path (pmc_fetch, preprint_fetch, paywall_pull): pdftotext's text is matched the same way."""
    import subprocess
    f = tmp_path / "x.pdf"
    f.write_bytes(b"%PDF-1.4 unique bytes")
    monkeypatch.setattr("shutil.which", lambda name: "/usr/bin/pdftotext")

    class R:
        returncode = 0
        stdout = DOCS["bmj_self_archiving_policy"].encode("utf-8")
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: R())
    assert U.is_known_boilerplate(str(f)) == (True, "bmj_self_archiving_policy")


def test_one_list():
    """The fetchers and the audit read the same tables."""
    assert U.KNOWN_BOILERPLATE_MD5 is bp.MD5
    assert U.KNOWN_BOILERPLATE_TEXT is bp.TEXT


# ---------------------------------------------------------------- audit_portfolio
import hashlib  # noqa: E402
import json  # noqa: E402
import os  # noqa: E402

import audit_portfolio as AP  # noqa: E402


def _held(lib, stem, content, doi, text="An article body " * 20, identity=None, sc_extra=None):
    lib.mkdir(parents=True, exist_ok=True)
    (lib / f"{stem}.pdf").write_bytes(content)
    sc = {"doi": doi, "title": stem, "text": text}
    sc.update(sc_extra or {})
    (lib / f"{stem}.fulltext.json").write_text(json.dumps(sc), encoding="utf-8")
    (lib / f"{stem}.ris").write_text(f"TY  - JOUR\nTI  - {stem}\nDO  - {doi}\nER  - \n", encoding="utf-8")
    if identity is not None:
        (lib / f"{stem}.identity.json").write_text(json.dumps(identity), encoding="utf-8")


def _pdf_bytes(tag):
    return b"%PDF-1.4\n" + tag.encode() * 2000


def test_audit_lists_a_held_file_whose_text_is_boilerplate(tmp_path):
    lib = tmp_path / "lib"
    _held(lib, "2023_Corbett_X", _pdf_bytes("corbett"), "10.1186/s12576-023-00882-4",
          text=DOCS["pure_research_portal_privacy_notice"])
    _held(lib, "2019_Hurst_Real", _pdf_bytes("hurst"), "10.3389/fnut.2019.00073")
    a = AP.scan_library(lib)
    assert [(b["file"], b["tag"]) for b in a["boilerplate"]] == [
        ("2023_Corbett_X.pdf", "pure_research_portal_privacy_notice")]


def test_audit_lists_a_held_file_whose_bytes_are_boilerplate(tmp_path, monkeypatch):
    """A scanned notice has no usable text: its bytes are enough (Evora, Chapman)."""
    content = _pdf_bytes("evora")
    monkeypatch.setitem(bp.MD5, hashlib.md5(content).hexdigest(), "test_regulation")
    monkeypatch.setattr(bp, "SIZES", bp.SIZES | {len(content)})
    lib = tmp_path / "lib"
    _held(lib, "2025_Russo_X", content, "10.1080/10833196.2025.2564026", text="DESPACHO z6lzot4 Reposit6rio")
    a = AP.scan_library(lib)
    assert [(b["file"], b["tag"]) for b in a["boilerplate"]] == [("2025_Russo_X.pdf", "test_regulation")]


def test_identical_bytes_under_different_dois_are_one_group(tmp_path):
    """Chen holds Phipson's paper; the Ashworth pair sits in two projects under two DOIs."""
    same = _pdf_bytes("phipson")
    _held(tmp_path / "athena", "2016_Chen_Reads", same, "10.12688/f1000research.8987.2")
    _held(tmp_path / "athena", "2016_Phipson_Robust", same, "10.1214/16-aoas920")
    _held(tmp_path / "athena", "2017_Other", _pdf_bytes("other"), "10.1/other")
    pair = _pdf_bytes("ashworth")
    _held(tmp_path / "phys", "2024_Ashworth_A", pair, "10.18276/cej.2024.3-06")
    _held(tmp_path / "thermal", "2024_Ashworth_B", pair, "10.1080/23328940.2019.1666624")
    copy = _pdf_bytes("copy")                       # the same paper in two projects: not a finding
    _held(tmp_path / "phys", "2020_Copy", copy, "10.1/copy")
    _held(tmp_path / "thermal", "2020_Copy", copy, "10.1/copy")
    audits = []
    for key in ("athena", "phys", "thermal"):
        a = AP.scan_library(tmp_path / key)
        a["project"] = key
        audits.append(a)
    groups = AP.content_dupes(audits)
    got = sorted(sorted((f["project"], f["doi"]) for f in g["files"]) for g in groups)
    assert got == [
        [("athena", "10.1214/16-aoas920"), ("athena", "10.12688/f1000research.8987.2")],
        [("phys", "10.18276/cej.2024.3-06"), ("thermal", "10.1080/23328940.2019.1666624")],
    ]


def test_a_flag_older_than_its_pdf_is_stale(tmp_path):
    """Cohen and Seron: Unpaywall's wrong file got FLAG, then PMC replaced the PDF; the
    .identity.json still says FLAG."""
    lib = tmp_path / "lib"
    flag = {"queue_doi": "10.2196/43883", "identity": "FLAG", "checked_at": "2026-11-04T13:45:00+00:00",
            "source": "unpaywall"}
    _held(lib, "2023_Cohen_X", _pdf_bytes("cohen"), "10.2196/43883", identity=flag,
          sc_extra={"identity": "OK"})
    _held(lib, "2020_Ashton_X", _pdf_bytes("ashton"), "10.1136/bjsports-2017-098970",
          identity=dict(flag, queue_doi="10.1136/bjsports-2017-098970"))
    old = 1_700_000_000                                 # 2023: the Ashton PDF predates its verdict
    os.utime(lib / "2020_Ashton_X.pdf", (old, old))
    a = AP.scan_library(lib)
    assert [s["file"] for s in a["stale_flags"]] == ["2023_Cohen_X.pdf"]
    assert {f["file"] for f in a["flags"]} == {"2023_Cohen_X.pdf", "2020_Ashton_X.pdf"}   # still review items


def test_the_report_shows_the_new_checks(tmp_path):
    lib = tmp_path / "lib"
    _held(lib, "2018_Naci_X", _pdf_bytes("naci"), "10.1136/bjsports-2018-099921",
          text=DOCS["bmj_self_archiving_policy"])
    a = AP.scan_library(lib)
    a.update(project="VAP", tier=1)
    out = AP.fmt_report(a, {"runs": 0, "dirs": []})
    assert "WARN known wrong-content documents (boilerplate): 1" in out
    assert "2018_Naci_X.pdf [bmj_self_archiving_policy]" in out
