"""W2-A2: recheck_pmc and backfill_fulltext end to end (temp library, stubbed transports).

DEC-13: both used to build `mailto:{EMAIL}` User-Agents and pass `ua=`/`email=` to the idconv batch
(`email=None` once lit_util.DEFAULT_EMAIL became None). Now the DOI -> PMCID route and its identity
are lit_net's (W2-A1) and every full-text fetch is litpipe.net's, so neither module passes an
identity anywhere. N-B3/N-B4: a Europe PMC 500 is NOT_AVAILABLE and BioC is tried next."""
import csv
import json
import sys
from pathlib import Path

import pytest
from requests.structures import CaseInsensitiveDict

import backfill_fulltext
import lit_net
import recheck_pmc
from litpipe import net

FIX = Path(__file__).resolve().parent / "fixtures" / "W2-A2"
JATS_OK = (b"<article><front><article-meta><article-id pub-id-type='doi'>10.1000/hx.2020.017</article-id>"
           b"<article-title>Heat acclimation and plasma volume</article-title>"
           b"</article-meta></front></article>")


def stub_by_host(monkeypatch, table):
    sent = []

    def fake(method, url, hdrs, body, timeout, max_bytes):
        sent.append((url, dict(hdrs)))
        host = url.split("/")[2]
        status, data, h = (tuple(table[host]) + (None,))[:3]
        return net._Raw(status, CaseInsensitiveDict(h or {"Content-Type": "application/xml"}), data,
                        data[:net.CHUNK], len(data))

    monkeypatch.setitem(net._TRANSPORTS, "requests", fake)
    monkeypatch.setitem(net._TRANSPORTS, "urllib", fake)
    return sent


def record_idconv(monkeypatch, mapping):
    calls = []

    from litpipe.outcomes import Kind, Outcome

    def fake(dois, *a, **k):
        calls.append((list(dois), a, k))
        return {d: (Outcome(Kind.OK, payload=lit_net.PmcidHit(d, mapping[d], source="idconv")) if d in mapping
                    else Outcome(Kind.NO_MATCH, payload=lit_net.PmcidHit(d))) for d in dois}

    monkeypatch.setattr(lit_net, "doi_to_pmcid", fake)
    return calls


def test_backfill_main_end_to_end(net_env, monkeypatch, tmp_path, capsys):
    lib = tmp_path / "lib"; lib.mkdir()
    (lib / "2020_Doe_Heat.pdf").write_bytes(b"%PDF-1.4 stub")
    (lib / "2021_Roe_Absent.pdf").write_bytes(b"%PDF-1.4 stub")
    src = tmp_path / "dois.csv"
    src.write_text("filename,doi\n2020_Doe_Heat.pdf,10.1000/hx.2020.017\n2021_Roe_Absent.pdf,10.1000/abs\n", encoding="utf-8")
    calls = record_idconv(monkeypatch, {"10.1000/hx.2020.017": "PMC1", "10.1000/abs": "PMC2"})
    # PMC1: JATS 200; PMC2: Europe PMC 500 then BioC "absent" page.
    replies = {}

    def fake(method, url, hdrs, body, timeout, max_bytes):
        if "PMC1/fullTextXML" in url:
            r = (200, JATS_OK, {"Content-Type": "application/xml"})
        elif "fullTextXML" in url:
            r = (500, (FIX / "epmc_fulltextxml_500.json").read_bytes(), {"Content-Type": "application/json"})
        else:
            r = (200, (FIX / "bioc_absent.html").read_bytes(), {"Content-Type": "text/html"})
        replies.setdefault("urls", []).append(url)
        return net._Raw(r[0], CaseInsensitiveDict(r[2]), r[1], r[1][:net.CHUNK], len(r[1]))

    monkeypatch.setitem(net._TRANSPORTS, "requests", fake)
    monkeypatch.setitem(net._TRANSPORTS, "urllib", fake)
    monkeypatch.setattr(sys, "argv", ["backfill_fulltext.py", "--lib-dir", str(lib), "--doi-source", str(src)])
    backfill_fulltext.main()
    assert calls and all(a == () and k == {} for _, a, k in calls)       # no ua=, no email=
    assert json.loads((lib / "2020_Doe_Heat.fulltext.json").read_text(encoding="utf-8"))["doi"] == "10.1000/hx.2020.017"
    assert not (lib / "2021_Roe_Absent.fulltext.json").exists()
    rows = {r["filename"]: r for r in csv.DictReader(open(lib / "_fulltext_backfill_report.csv", encoding="utf-8"))}
    assert rows["2020_Doe_Heat.pdf"]["status"] == "OK"
    assert rows["2021_Roe_Absent.pdf"]["status"] == "NOT_AVAILABLE"
    out = capsys.readouterr().out
    assert "Sidecars unavailable:   1" in out and "Errors:                 0" in out
    assert not any("pmc.ncbi" in u or "europepmc.org/" in u.split("//")[1][:15] for u in replies["urls"])


def _pdf_with_text(path, text):
    import fitz
    doc = fitz.open()
    page = doc.new_page()
    page.insert_text((72, 72), text)
    doc.save(str(path))
    doc.close()


def test_recheck_main_end_to_end(net_env, monkeypatch, tmp_path):
    lib = tmp_path / "lib"; lib.mkdir()
    _pdf_with_text(lib / "2020_Doe_Heat.pdf",
                   "Heat acclimation and plasma volume\nhttps://doi.org/10.1000/hx.2020.017\n")
    calls = record_idconv(monkeypatch, {"10.1000/hx.2020.017": "PMC1"})
    sent = stub_by_host(monkeypatch, {"www.ebi.ac.uk": (200, JATS_OK)})
    monkeypatch.setattr(sys, "argv", ["recheck_pmc.py", "--lib-dir", str(lib)])
    recheck_pmc.main()
    assert calls == [(["10.1000/hx.2020.017"], (), {})]                           # no ua=, no email=
    sc = json.loads((lib / "2020_Doe_Heat.fulltext.json").read_text(encoding="utf-8"))
    assert sc["title"] == "Heat acclimation and plasma volume" and sc["_recheck_source_doi"] == "10.1000/hx.2020.017"
    (url, hdrs), = sent
    assert url.endswith("/PMC1/fullTextXML") and "None" not in hdrs["User-Agent"]
    rows = list(csv.DictReader(open(lib / "_pmc_recheck_report.csv", encoding="utf-8")))
    assert rows[0]["status"] == "OK"


@pytest.mark.parametrize("script", ["recheck_pmc.py", "backfill_fulltext.py", "harvest_citations.py",
                                    "fetch_figures.py", "jats_to_text.py"])
def test_help_runs(script):
    import subprocess
    repo = Path(__file__).resolve().parent.parent
    r = subprocess.run([sys.executable, str(repo / script), "--help"], capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, r.stderr
