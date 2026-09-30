"""W2-A2: harvest_citations. pmid_to_doi through E-utilities esummary (N-B6), typed failures
counted (REG-I46), the canonical_stem collision that skipped a distinct paper, and the shared
identity (DEC-13). Offline: litpipe.net runs against a stubbed transport; fixtures under
tests/fixtures/W2-A2/ are trimmed, redacted probe and endpoint-audit responses."""
import csv
import json
import sys
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest
from requests.structures import CaseInsensitiveDict

import harvest_citations as H
import lit_util
import ris_emit as R
from litpipe import hosts, net
from litpipe.outcomes import Kind

FIX = Path(__file__).resolve().parent / "fixtures" / "W2-A2"
JSON = {"Content-Type": "application/json; charset=UTF-8"}


def stub(monkeypatch, *replies):
    """Replace both transports; each call takes the next (status, body, headers) reply, the last
    repeats; an Exception instance is returned as a transport failure. Returns the sent list."""
    sent = []

    def fake(method, url, hdrs, body, timeout, max_bytes):
        sent.append((method, url, dict(hdrs)))
        r = replies[min(len(sent), len(replies)) - 1]
        if isinstance(r, Exception):
            return net._Raw(error=f"{type(r).__name__}: {r}")
        status, data, h = (tuple(r) + ({},))[:3]
        data = data if isinstance(data, bytes) else data.encode("utf-8")
        return net._Raw(status, CaseInsensitiveDict(h or JSON), data, data[:net.CHUNK], len(data))

    monkeypatch.setitem(net._TRANSPORTS, "requests", fake)
    monkeypatch.setitem(net._TRANSPORTS, "urllib", fake)
    return sent


@pytest.fixture
def quiet_net(net_env):
    """net_env with the real host table (net_env registers loopback rows only)."""
    return net_env


# ------------------------------------------------------------------ pmid_to_doi via esummary
def test_pubmed_only_pmid_resolves_through_esummary(quiet_net, monkeypatch):
    """N-B6: a PubMed-only PMID (not in PMC, so idconv could never answer) gets its DOI from the
    esummary articleids. Fixture: the 2026-09-30 probe response, trimmed."""
    body = (FIX / "esummary_pubmed.json").read_text(encoding="utf-8")
    sent = stub(monkeypatch, (200, body))
    res = H.pmid_to_doi("35393042")
    assert res.kind is Kind.OK and res.payload == "10.1016/j.jtherbio.2021.103173"
    method, url, hdrs = sent[0]
    parts = urlsplit(url)
    q = parse_qs(parts.query)
    assert parts.netloc == "eutils.ncbi.nlm.nih.gov" and parts.path.endswith("/esummary.fcgi")
    assert q["db"] == ["pubmed"] and q["id"] == ["35393042"] and q["retmode"] == ["json"]
    assert q["tool"] == ["literature-pipeline"]              # the one identity, injected by net
    assert "pmc.ncbi.nlm.nih.gov" not in url and "idconv" not in url


def test_pmc_pmid_resolves_too(quiet_net, monkeypatch):
    stub(monkeypatch, (200, (FIX / "esummary_pubmed.json").read_text(encoding="utf-8")))
    assert H.pmid_to_doi("39593914").payload == "10.3390/e26110970"


def test_doi_is_lowercased_and_elocationid_is_the_fallback(quiet_net, monkeypatch):
    doc = {"uid": "1", "articleids": [{"idtype": "pubmed", "value": "1"}],
           "elocationid": "doi: 10.1000/ABC.Def"}
    stub(monkeypatch, (200, json.dumps({"result": {"uids": ["1"], "1": doc}})))
    res = H.pmid_to_doi("1")
    assert res.kind is Kind.OK and res.payload == "10.1000/abc.def"


def test_invalid_uid_is_no_match(quiet_net, monkeypatch):
    """The live shape of an unknown PMID (probe 2026-09-30): 200 with a per-uid error."""
    stub(monkeypatch, (200, (FIX / "esummary_invalid_uid.json").read_text(encoding="utf-8")))
    res = H.pmid_to_doi("99999999999")
    assert res.kind is Kind.NO_MATCH and "cannot get document summary" in res.detail
    assert res.payload is None


def test_record_without_doi_is_no_match(quiet_net, monkeypatch):
    doc = {"uid": "7", "articleids": [{"idtype": "pubmed", "value": "7"}], "elocationid": ""}
    stub(monkeypatch, (200, json.dumps({"result": {"uids": ["7"], "7": doc}})))
    assert H.pmid_to_doi("7").kind is Kind.NO_MATCH


@pytest.mark.parametrize("reply,kind", [
    (ConnectionError("getaddrinfo failed"), Kind.TRANSPORT),
    ((403, "{}"), Kind.REFUSED),
    ((503, "{}"), Kind.OUTAGE),
    ((200, "<html>maintenance</html>", {"Content-Type": "text/html"}), Kind.OUTAGE),
    ((200, '{"error": "API rate limit exceeded"}'), Kind.ERROR),
])
def test_failures_are_typed_never_an_empty_doi(quiet_net, monkeypatch, reply, kind):
    stub(monkeypatch, reply)
    res = H.pmid_to_doi("12345678")
    assert res.kind is kind and res.payload is None


def test_no_pmid_and_non_numeric_are_not_sent(quiet_net, monkeypatch):
    sent = stub(monkeypatch, (200, "{}"))
    assert H.pmid_to_doi("").kind is Kind.SKIPPED
    assert H.pmid_to_doi("PMC123").kind is Kind.NO_MATCH
    assert sent == []


def test_email_param_never_none_when_unset(quiet_net, monkeypatch):
    """DEC-13: with LITPIPE_EMAIL unset nothing says email=None or mailto:None."""
    monkeypatch.delenv("LITPIPE_EMAIL", raising=False)
    sent = stub(monkeypatch, (200, (FIX / "esummary_pubmed.json").read_text(encoding="utf-8")))
    H.pmid_to_doi("39593914")
    _, url, hdrs = sent[0]
    assert "email=" not in url and "None" not in url
    assert "None" not in hdrs.get("User-Agent", "") and "mailto" not in hdrs.get("User-Agent", "")


def test_module_builds_no_identity_of_its_own():
    """DEC-13: no module-level EMAIL / UA / idconv constants remain to leak mailto:None."""
    for name in ("EMAIL", "UA", "IDCONV"):
        assert not hasattr(H, name), name


# ------------------------------------------------------------------ main(): counting failures
def _nbib(path, pmid, title, year="2018", author="Vasquez, Vera"):
    path.write_text(f"PMID- {pmid}\nTI  - {title}\nDP  - {year} Jun\nFAU - {author}\n", encoding="utf-8")


def _run(monkeypatch, src, out, *extra):
    monkeypatch.setattr(sys, "argv", ["harvest_citations.py", "--source-dir", str(src),
                                      "--out-dir", str(out), "--no-search", "--sleep", "0", *extra])
    H.main()


def test_main_counts_a_transport_failure(quiet_net, monkeypatch, tmp_path, capsys):
    src = tmp_path / "in"; src.mkdir()
    _nbib(src / "a.nbib", "12345678", "NBIB Title")
    stub(monkeypatch, ConnectionError("getaddrinfo failed"))
    monkeypatch.setattr(R, "crossref_by_doi", lambda doi, **k: None)
    _run(monkeypatch, src, tmp_path / "out")
    out = capsys.readouterr()
    assert "pmid_lookup_failed 1" in " ".join(out.out.split())
    assert "TRANSPORT" in out.err
    assert not (tmp_path / "out").exists()     # a dry run writes nothing


def test_main_uses_the_esummary_doi(quiet_net, monkeypatch, tmp_path, capsys):
    src = tmp_path / "in"; src.mkdir()
    _nbib(src / "a.nbib", "35393042", "Thermal thing")
    stub(monkeypatch, (200, (FIX / "esummary_pubmed.json").read_text(encoding="utf-8")))
    seen = []
    monkeypatch.setattr(R, "crossref_by_doi", lambda doi, **k: seen.append(doi))
    _run(monkeypatch, src, tmp_path / "out")
    assert seen == ["10.1016/j.jtherbio.2021.103173"]
    assert "pmid_lookup_failed 0" in " ".join(capsys.readouterr().out.split())


def test_main_counts_a_metadata_failure_instead_of_crashing(monkeypatch, tmp_path, capsys):
    """REG-I46 on the Crossref side: once ris_emit raises MetadataUnavailable for a failed lookup,
    the harvest counts it and falls back to the file's own metadata instead of dying."""
    class MetadataUnavailable(Exception):
        pass
    monkeypatch.setattr(H, "_META_UNAVAILABLE", (MetadataUnavailable,))

    def boom(doi, **k):
        raise MetadataUnavailable("Crossref 503")
    monkeypatch.setattr(R, "crossref_by_doi", boom)
    src = tmp_path / "in"; src.mkdir()
    (src / "a.ris").write_text("TY  - JOUR\nTI  - A title\nAU  - Doe, J\nPY  - 2020\nDO  - 10.1/a\nER  - \n",
                               encoding="utf-8")
    _run(monkeypatch, src, tmp_path / "out")
    out = " ".join(capsys.readouterr().out.split())
    assert "metadata_lookup_failed 1" in out and "fallback 1" in out


# ------------------------------------------------------------------ the canonical_stem collision
def _ris(path, doi, title, year="2020", author="Smith, Jane"):
    path.write_text(f"TY  - JOUR\nTI  - {title}\nAU  - {author}\nPY  - {year}\nDO  - {doi}\nER  - \n",
                    encoding="utf-8")


TITLE_A = "Heat acclimation improves exercise capacity in trained cyclists: a randomized trial"
TITLE_B = "Heat acclimation improves exercise capacity in trained cyclists: a follow-up in women"


def test_stems_collide_for_the_two_titles():
    """Precondition of the regression: both papers map to one canonical stem."""
    assert R.canonical_stem("2020", "Smith", TITLE_A) == R.canonical_stem("2020", "Smith", TITLE_B)


def test_distinct_papers_sharing_a_stem_are_both_written(monkeypatch, tmp_path, capsys):
    """The collision (code review 2026-07-02): year + lastname + first 6 title words gave both
    papers one file name, so the second was EXISTS_SKIP (or, with --overwrite, clobbered the
    first). Both must be written, each under its own name with its own DOI."""
    monkeypatch.setattr(R, "crossref_by_doi", lambda doi, **k: None)
    src = tmp_path / "in"; src.mkdir()
    _ris(src / "a.ris", "10.1000/aaa", TITLE_A)
    _ris(src / "b.ris", "10.1000/bbb", TITLE_B)
    out = tmp_path / "out"
    _run(monkeypatch, src, out, "--commit")
    files = sorted(out.glob("*.ris"))
    assert len(files) == 2, [f.name for f in files]
    dois = sorted(lit_util.parse_ris(str(f))["doi"] for f in files)
    assert dois == ["10.1000/aaa", "10.1000/bbb"]
    rows = list(csv.DictReader(open(out / "_index.csv", encoding="utf-8")))
    assert sorted(r["status"] for r in rows) == ["WROTE", "WROTE"]
    assert len({r["out"] for r in rows}) == 2


def test_overwrite_does_not_clobber_a_distinct_paper(monkeypatch, tmp_path):
    monkeypatch.setattr(R, "crossref_by_doi", lambda doi, **k: None)
    src = tmp_path / "in"; src.mkdir()
    _ris(src / "a.ris", "10.1000/aaa", TITLE_A)
    _ris(src / "b.ris", "10.1000/bbb", TITLE_B)
    out = tmp_path / "out"
    _run(monkeypatch, src, out, "--commit", "--overwrite")
    assert sorted(lit_util.parse_ris(str(f))["doi"] for f in out.glob("*.ris")) == ["10.1000/aaa", "10.1000/bbb"]


def test_a_file_from_an_earlier_run_for_another_paper_is_not_a_skip(monkeypatch, tmp_path):
    """Second run, new paper: the stem's file on disk belongs to a different DOI."""
    monkeypatch.setattr(R, "crossref_by_doi", lambda doi, **k: None)
    src = tmp_path / "in"; src.mkdir()
    out = tmp_path / "out"
    _ris(src / "a.ris", "10.1000/aaa", TITLE_A)
    _run(monkeypatch, src, out, "--commit")
    (src / "a.ris").unlink()
    _ris(src / "b.ris", "10.1000/bbb", TITLE_B)
    _run(monkeypatch, src, out, "--commit")
    assert sorted(lit_util.parse_ris(str(f))["doi"] for f in out.glob("*.ris")) == ["10.1000/aaa", "10.1000/bbb"]


def test_the_same_paper_again_is_still_exists_skip(monkeypatch, tmp_path):
    """No regression: a rerun over the same paper keeps EXISTS_SKIP and writes no second file."""
    monkeypatch.setattr(R, "crossref_by_doi", lambda doi, **k: None)
    src = tmp_path / "in"; src.mkdir()
    out = tmp_path / "out"
    _ris(src / "a.ris", "10.1000/aaa", TITLE_A)
    _run(monkeypatch, src, out, "--commit")
    _run(monkeypatch, src, out, "--commit")
    assert len(list(out.glob("*.ris"))) == 1
    rows = list(csv.DictReader(open(out / "_index.csv", encoding="utf-8")))
    assert [r["status"] for r in rows] == ["EXISTS_SKIP"]


def test_same_doi_twice_is_still_dup_skip(monkeypatch, tmp_path):
    monkeypatch.setattr(R, "crossref_by_doi", lambda doi, **k: None)
    src = tmp_path / "in"; src.mkdir()
    _ris(src / "a.ris", "10.1000/aaa", TITLE_A)
    _ris(src / "b.ris", "10.1000/AAA", TITLE_A)
    out = tmp_path / "out"
    _run(monkeypatch, src, out, "--commit")
    rows = list(csv.DictReader(open(out / "_index.csv", encoding="utf-8")))
    assert sorted(r["status"] for r in rows) == ["DUP_SKIP", "WROTE"]
    assert len(list(out.glob("*.ris"))) == 1
