"""W2a verifier B (Unpaywall stage, metadata writer, S2 client, outcomes, hosts/state/doi fixes).

Offline. The Unpaywall cases drive the REAL stage (`unpaywall_fetch_v2.run`) through the REAL
litpipe.net (host table, refusal and 403 rules, retry, ledger, redaction) with only the transport
replaced (the `Web` double of tests/test_unpaywall_stage.py, extended with transport errors).

The central table: for every row shape the stage can write, the legacy columns (what
`sweep.unpaywall_verdict` and `migrate_closed_to_md` read through `from_legacy`) and the typed
`outcome` column (what W2-G will read) must name the same Kind, and so the same residual class.
A mismatch sends a row to the wrong class; an ERROR closes it after three runs.
"""
import dataclasses
import json
import os
from pathlib import Path

import pytest

import unpaywall_fetch_v2 as U
from litpipe import hosts, net
from litpipe.outcomes import Kind, from_legacy
from tests.test_unpaywall_stage import (HTML, JSON, PDF, Web, add_upw, article_pdf, fixture_bytes,
                                        loc, make_pdf, run_stage, upw_record, upw_url)

TEST_EMAIL = "tester@litpipe-test.org"


class ErrWeb(Web):
    """Web plus transport failures: `errors[url_without_query] = "ExcName: text"`."""

    def __init__(self):
        super().__init__()
        self.errors = {}

    def __call__(self, method, url, headers, body, timeout, max_bytes):
        key = url.split("?", 1)[0]
        if key in self.errors:
            self.sent.append((method, url, dict(headers)))
            return net._Raw(error=self.errors[key])
        return super().__call__(method, url, headers, body, timeout, max_bytes)


@pytest.fixture
def web(net_env, monkeypatch):
    w = ErrWeb()
    monkeypatch.setitem(net._TRANSPORTS, "requests", w)
    monkeypatch.setitem(net._TRANSPORTS, "urllib", w)
    w.env = net_env
    return w


def _dl_host(host):
    hosts.register(hosts.HostPolicy(host, min_interval_s=0.0, identity="download",
                                    redirect_allow=(hosts.ANY_HOST,)))


# ------------------------------------------------------------------ scenarios (one row each)
class _Dois:
    """Stable numeric test DOIs by name (litpipe.doi.normalise peels word-only tails, so a DOI
    such as 10.1152/vb.flag would not survive as a DOI at all)."""

    def __init__(self):
        self.n = {}

    def format(self, name):
        return f"10.1152/vb.{1000 + self.n.setdefault(name, len(self.n))}"


D = _Dois()


def sc_skip_exists(w, tmp_path, mp):
    d = D.format("skip")
    lib = tmp_path / "lib"
    lib.mkdir(exist_ok=True)
    fn = U.build_filename("2020", "Author A", "Heat acclimation and athletic performance")
    (lib / fn).write_bytes(article_pdf(d))
    (lib / (fn[:-4] + ".ris")).write_text(f"TY  - JOUR\nDO  - {d}\nER  - \n", encoding="utf-8")
    return d


def sc_downloaded(w, tmp_path, mp):
    d = D.format("ok")
    _dl_host("ok.example.org")
    add_upw(w, d, upw_record(d, [loc("https://ok.example.org/a.pdf")]))
    w.add("https://ok.example.org/a.pdf", (200, PDF, article_pdf(d)))
    return d


def sc_not_at_ra(w, tmp_path, mp):
    return "10.48550/arxiv.2402.05741"


def sc_config_email_unset(w, tmp_path, mp):
    mp.delenv("LITPIPE_EMAIL", raising=False)
    return D.format("cfg")


def sc_upw_404(w, tmp_path, mp):
    d = D.format("u404")
    w.add(upw_url(d), (404, HTML, fixture_bytes("unpaywall_404.html")))
    return d


def sc_upw_422(w, tmp_path, mp):
    d = D.format("u422")
    w.add(upw_url(d), (422, JSON, fixture_bytes("unpaywall_422.json")))
    return d


def sc_upw_503(w, tmp_path, mp):
    d = D.format("u503")
    w.add(upw_url(d), (503, JSON, b'{"error": "unavailable"}'))
    return d


def sc_upw_transport(w, tmp_path, mp):
    d = D.format("utr")
    w.errors[upw_url(d)] = "ConnectionError: [WinError 10054] reset by peer"
    return d


def sc_upw_html_200(w, tmp_path, mp):
    d = D.format("uhtml")
    w.add(upw_url(d), (200, HTML, b"<html><title>Maintenance</title>back soon</html>"))
    return d


def sc_upw_not_a_record(w, tmp_path, mp):
    d = D.format("ulist")
    w.add(upw_url(d), (200, JSON, b"[]"))
    return d


def sc_upw_host_refused(w, tmp_path, mp):
    w.env.state.refuse("api.unpaywall.org", "refused earlier in the run", persistence="run")
    return D.format("uref")


def sc_upw_budget_spent(w, tmp_path, mp):
    w.env.state.budgets["api.unpaywall.org"] = 0
    return D.format("ubud")


def sc_upw_429_long_retry_after(w, tmp_path, mp):
    d = D.format("u429")
    w.add(upw_url(d), (429, {"Retry-After": "3600", **JSON}, b"{}"))
    return d


def sc_closed(w, tmp_path, mp):
    d = D.format("closed")
    add_upw(w, d, upw_record(d, [], is_oa=False))
    return d


def sc_oa_no_url(w, tmp_path, mp):
    d = D.format("nourl")
    add_upw(w, d, upw_record(d, []))
    return d


def sc_dl_404(w, tmp_path, mp):
    d = D.format("d404")
    _dl_host("gone.example.org")
    add_upw(w, d, upw_record(d, [loc("https://gone.example.org/a.pdf")]))
    w.add("https://gone.example.org/a.pdf", (404, HTML, b"<html>not found</html>"))
    return d


def sc_dl_403(w, tmp_path, mp):
    d = D.format("d403")
    add_upw(w, d, upw_record(d, [loc("https://wall.example.org/a.pdf")]))
    hosts.register(hosts.HostPolicy("wall.example.org", min_interval_s=0.0, identity="download",
                                    refuse_after_consecutive_403=5))
    w.add("https://wall.example.org/a.pdf", (403, HTML, b"<html>forbidden</html>"))
    return d


def sc_dl_503(w, tmp_path, mp):
    d = D.format("d503")
    _dl_host("down.example.org")
    add_upw(w, d, upw_record(d, [loc("https://down.example.org/a.pdf")]))
    w.add("https://down.example.org/a.pdf", (503, HTML, b"<html>down</html>"))
    return d


def sc_dl_transport(w, tmp_path, mp):
    d = D.format("dtr")
    _dl_host("flaky.example.org")
    add_upw(w, d, upw_record(d, [loc("https://flaky.example.org/a.pdf")]))
    w.errors["https://flaky.example.org/a.pdf"] = "ConnectionError: reset by peer"
    return d


def sc_dl_deferred(w, tmp_path, mp):
    d = D.format("ddef")
    _dl_host("later.example.org")
    add_upw(w, d, upw_record(d, [loc("https://later.example.org/a.pdf")]))
    w.env.state.defer("later.example.org", 1e12)
    return d


def sc_dl_403_then_404(w, tmp_path, mp):
    """Repository first (DEC-11 default): a 403 wall, then the publisher's dead link. The row's
    root cause is the refusal (OA_BLOCKED); the legacy column must not say otherwise."""
    d = D.format("mix")
    hosts.register(hosts.HostPolicy("repo.example.org", min_interval_s=0.0, identity="download",
                                    refuse_after_consecutive_403=5))
    _dl_host("pub.example.org")
    add_upw(w, d, upw_record(d, [loc("https://pub.example.org/a.pdf", host_type="publisher"),
                                 loc("https://repo.example.org/a.pdf", host_type="repository",
                                     version="acceptedVersion")]))
    w.add("https://repo.example.org/a.pdf", (403, HTML, b"<html>forbidden</html>"))
    w.add("https://pub.example.org/a.pdf", (404, HTML, b"<html>not found</html>"))
    return d


def sc_handle_outage(w, tmp_path, mp):
    d = D.format("handle")
    add_upw(w, d, upw_record(d, [loc(url=f"https://doi.org/{d}")]))
    w.add(U.DOI_HANDLES + U._doi.encode_path(d), (503, JSON, b"{}"))
    return d


def sc_interstitial(w, tmp_path, mp):
    d = D.format("wall")
    _dl_host("cc.example.org")
    add_upw(w, d, upw_record(d, [loc("https://cc.example.org/a.pdf")]))
    w.add("https://cc.example.org/a.pdf", (200, HTML, fixture_bytes("bmc_client_challenge.html")))
    return d


def sc_prohibited_only(w, tmp_path, mp):
    d = D.format("proh")
    add_upw(w, d, upw_record(d, [loc("https://europepmc.org/articles/PMC123456?pdf=render",
                                     host_type="repository")]))
    return d


def sc_too_small(w, tmp_path, mp):
    d = D.format("small")
    _dl_host("tiny.example.org")
    add_upw(w, d, upw_record(d, [loc("https://tiny.example.org/a.pdf")]))
    w.add("https://tiny.example.org/a.pdf", (200, PDF, make_pdf(f"doi {d}", pages=1, pad=False)))
    return d


def sc_identity_flag(w, tmp_path, mp):
    d = D.format("flag")
    _dl_host("k.example.org")
    add_upw(w, d, upw_record(d, [loc("https://k.example.org/a.pdf")]))
    w.add("https://k.example.org/a.pdf",
          (200, PDF, article_pdf("10.1016/j.jsams.2019.04.012", "An unrelated study of cycling cadence")))
    return d


SCENARIOS = {f.__name__[3:]: f for f in (
    sc_skip_exists, sc_downloaded, sc_not_at_ra, sc_config_email_unset, sc_upw_404, sc_upw_422,
    sc_upw_503, sc_upw_transport, sc_upw_html_200, sc_upw_not_a_record, sc_upw_host_refused,
    sc_upw_budget_spent, sc_upw_429_long_retry_after, sc_closed, sc_oa_no_url, sc_dl_404, sc_dl_403,
    sc_dl_503, sc_dl_transport, sc_dl_deferred, sc_dl_403_then_404, sc_handle_outage, sc_interstitial,
    sc_prohibited_only, sc_too_small, sc_identity_flag)}


def _row(web, tmp_path, monkeypatch, name):
    d = SCENARIOS[name](web, tmp_path, monkeypatch)
    _, rep, _ = run_stage(tmp_path, [{"doi": d}])
    return rep[U._doi.normalise(d) or d]


def _verdicts(row):
    import sweep
    legacy = sweep.unpaywall_verdict(row)
    typed = dataclasses.replace(legacy, kind=Kind(row["outcome"]))
    return legacy, typed


@pytest.mark.parametrize("name", sorted(SCENARIOS))
def test_legacy_columns_and_typed_outcome_route_to_the_same_residual_class(web, tmp_path, monkeypatch, name):
    """The router contract across the cutover: on the third run (where ERROR turns TERMINAL_CLOSED)
    the legacy columns and the typed outcome must give the same residual class."""
    import sweep
    row = _row(web, tmp_path, monkeypatch, name)
    legacy, typed = _verdicts(row)
    n = sweep.ERROR_RUNS_TO_TERMINAL
    assert sweep.classify([legacy], n)[0] == sweep.classify([typed], n)[0], (
        f"{name}: error={row['error']!r} oa_status={row['oa_status']!r} outcome={row['outcome']} "
        f"-> legacy {legacy.kind} / class {sweep.classify([legacy], n)[0]} vs typed {typed.kind} / "
        f"class {sweep.classify([typed], n)[0]}")


@pytest.mark.parametrize("name", sorted(SCENARIOS))
def test_legacy_columns_and_typed_outcome_name_the_same_kind(web, tmp_path, monkeypatch, name):
    """Stricter: the same Kind (W2-G's counts and the LOOSE_ENDS reasons read the kind)."""
    row = _row(web, tmp_path, monkeypatch, name)
    legacy, typed = _verdicts(row)
    assert legacy.kind is typed.kind, (
        f"{name}: error={row['error']!r} oa_status={row['oa_status']!r}: from_legacy -> {legacy.kind}, "
        f"outcome column -> {typed.kind}")


def test_identity_flag_and_supplement_rows_pair_outcome_with_identity(web, tmp_path):
    """W2-G input contract: a flag is outcome ERROR plus identity FLAG; a supplement is outcome
    ERROR with identity OK and doc_kind SUPPLEMENT. Both carry the DOI_MISMATCH legacy prefix, so
    a router keyed on identity == FLAG alone would miss the supplement."""
    d1 = sc_identity_flag(web, tmp_path, None)
    d2 = D.format("supp")
    add_upw(web, d2, upw_record(d2, [loc("https://k.example.org/s.pdf")]))
    web.add("https://k.example.org/s.pdf",
            (200, PDF, make_pdf(f"Supplementary material for: Heat acclimation and athletic performance\n"
                                f"https://doi.org/{d2}\n")))
    _, rep, _ = run_stage(tmp_path, [{"doi": d1}, {"doi": d2}])
    flag, supp = rep[d1], rep[d2]
    assert (flag["outcome"], flag["identity"]) == ("ERROR", "FLAG")
    assert (supp["outcome"], supp["identity"], supp["doc_kind"]) == ("ERROR", "OK", "SUPPLEMENT")
    assert flag["error"].startswith("DOI_MISMATCH:") and supp["error"].startswith("DOI_MISMATCH:")
    import sweep
    for r in (flag, supp):
        assert sweep.classify([sweep.unpaywall_verdict(r)])[0] == "IDENTITY_FLAG"


def test_a_flagged_row_rerun_does_not_pile_up_copies_in_the_library(web, tmp_path):
    """A flagged row comes back (retry_later when another stage was transient, or the queue keeps
    it until reviewed). Re-running it must not add another copy of the wrong PDF each run:
    nothing leaves the library, and nothing should multiply in it either."""
    d = sc_identity_flag(web, tmp_path, None)
    for _ in range(3):
        _, rep, lib = run_stage(tmp_path, [{"doi": d}])
        assert rep[d]["identity"] == "FLAG"
    pdfs = sorted(p.name for p in lib.glob("*.pdf"))
    assert len(pdfs) == 1, f"flagged PDF written {len(pdfs)} times: {pdfs}"


# ------------------------------------------------------------------ from_legacy: old tokens, old order
@pytest.mark.parametrize("s, stage, want", [
    ("europepmc/HTTP_500", "pmc", Kind.NOT_AVAILABLE),           # rule 1
    ("HTTP_500", "fulltext", Kind.NOT_AVAILABLE),
    ("HTTP_406", None, Kind.REFUSED), ("HTTP_429", None, Kind.REFUSED),   # rule 2
    ("HTTP_403", "unpaywall", Kind.REFUSED),                      # rule 3
    ("HTTP_503", "unpaywall", Kind.OUTAGE), ("EMPTY", "unpaywall", Kind.OUTAGE),   # rule 4
    ("HTTPSConnectionPool(host='x', port=443): Max retries exceeded (NameResolutionError)", "pmc",
     Kind.TRANSPORT),                                             # rule 5
    ("NO_PMCID", "pmc", Kind.NO_MATCH), ("HTTP 404", "unpaywall", Kind.NO_MATCH),  # rule 6
    ("CLOSED", "unpaywall", Kind.NOT_AVAILABLE),                  # rule 7
    ("HTML", "unpaywall", Kind.REFUSED), ("NOT_PDF", "unpaywall", Kind.REFUSED),
    ("HTTP_202", "pmc", Kind.REFUSED),                            # rule 8
    ("HTTP 422", "unpaywall", Kind.CONFIG), ("HTTP 410", "unpaywall", Kind.CONFIG),  # rule 9
    ("MANUAL_PREPRINT", "preprint", Kind.SKIPPED),                # rule 10
    ("TOO_SMALL", "unpaywall", Kind.ERROR),
    # order: an earlier rule wins over a later one in the same string
    ("HTTP_403 HTML", "unpaywall", Kind.REFUSED), ("HTTP_503 NO_PMCID", "pmc", Kind.OUTAGE),
    ("europepmc/HTTP_500 timed out", "pmc", Kind.NOT_AVAILABLE), ("HTTP_429 timed out", None, Kind.REFUSED),
    # lower-case prose never triggers a typed token
    ("Connection refused", "pmc", Kind.TRANSPORT), ("host x is refused; nothing sent", None, Kind.ERROR),
])
def test_from_legacy_keeps_the_dispatch_order_for_old_tokens(s, stage, want):
    assert from_legacy(s, stage) is want


# ------------------------------------------------------------------ W2-B acceptance (independent locks)
@pytest.mark.parametrize("authors, want", [
    ("Durnin JVGA; Womersley J", "Durnin"), ("van der Walt JHA; Smith B", "vanderWalt"),
    ("De Vries H", "DeVries"), ("Garcia-Lopez J", "Garcia-Lopez")])
def test_dec14_particles(authors, want):
    assert U.last_name(authors) == want


def test_lookup_without_email_is_config_and_nothing_carries_none(web, tmp_path, monkeypatch):
    """DEC-13 with LITPIPE_EMAIL unset: Unpaywall is never asked; no request anywhere carries
    email= or a mailto (least of all 'None')."""
    monkeypatch.delenv("LITPIPE_EMAIL", raising=False)
    res, rep, _ = run_stage(tmp_path, [{"doi": "10.1152/vb.9001"}])
    assert res["exit_code"] == U.EXIT_CONFIG and rep["10.1152/vb.9001"]["outcome"] == "CONFIG"
    assert web.to("api.unpaywall.org") == []
    for _, url, hdrs in web.sent:
        blob = url + json.dumps(hdrs)
        assert "email=" not in blob and "mailto" not in blob and "None" not in blob


def test_no_persisted_artifact_holds_the_email_in_any_form(web, tmp_path, monkeypatch):
    """Grep every file an end-to-end run wrote (report, identity sidecars, ledger) for the address
    in plain, %40 and %2540 forms, and for a raw email= / mailto: value."""
    for name in ("downloaded", "dl_404", "upw_transport", "identity_flag", "upw_html_200"):
        SCENARIOS[name](web, tmp_path, monkeypatch)
    rows = [{"doi": d} for d in (D.format("ok"), D.format("d404"), D.format("utr"), D.format("flag"),
                                 D.format("uhtml"))]
    run_stage(tmp_path, rows)
    forms = (TEST_EMAIL, TEST_EMAIL.replace("@", "%40"), TEST_EMAIL.replace("@", "%2540"))
    files = [p for p in tmp_path.rglob("*") if p.is_file() and p.suffix in (".csv", ".json", ".jsonl", ".ris")]
    assert any(p.suffix == ".jsonl" for p in files)            # the ledger was written and is checked
    for p in files:
        text = p.read_text(encoding="utf-8", errors="replace")
        for f in forms:
            assert f.lower() not in text.lower(), f"{f} in {p.name}"
        assert "email=" not in text.replace("[EMAIL-REDACTED]", ""), p.name


# ------------------------------------------------------------------ W2-E1: metadata writer
def test_content_negotiation_406_is_no_metadata_not_unavailable(web):
    """d16149d made a doi.org CN 406 a per-DOI NOT_AVAILABLE. The metadata writer must read it as
    "this agency has no CSL-JSON for this DOI" (None / source 'none'), not as MetadataUnavailable
    (try again later, forever), and doi.org must stay usable for the next DOI."""
    import ris_emit as R
    web.env.state.kv_set("doi_ra", "10.3305", "medra")
    bad, good = "10.3305/vb.9406", "10.3305/vb.9200"
    web.add(R.DOI_CN.format(doi=U._doi.encode_path(bad)), (406, HTML, b"<html>Not Acceptable</html>"))
    web.add(R.DOI_CN.format(doi=U._doi.encode_path(good)),
            (200, {"Content-Type": R.CSL_JSON}, json.dumps({"DOI": good, "title": "A mEDRA record",
                                                            "issued": {"date-parts": [[2019]]},
                                                            "type": "article-journal"})))
    assert R.resolve_meta(bad) == ({}, "none")
    assert not web.env.state.is_refused("doi.org")
    meta, src = R.resolve_meta(good)
    assert src == "cn:medra" and meta["title"] == "A mEDRA record"


def _spellings(p: Path):
    s = str(p)
    out = {"native": s, "forward": s.replace("\\", "/"), "dotdot": str(p.parent / ".." / p.parent.name / p.name)}
    if os.name == "nt":
        out.update(upper=s.upper(), lower=s.lower())
    return out


def test_dec29_manifest_key_is_stable_across_path_spellings(net_env, tmp_path, monkeypatch):
    """A drifting key turns every pipeline-written .ris into 'unrecorded' (kept as curated, never
    refreshed). Case, slashes, '..' and a relative path must all reach one record."""
    import ris_emit as R
    lib = tmp_path / "Lib Dir"
    lib.mkdir()
    p = lib / "2020_Author_Paper.ris"
    assert R.write_ris(str(p), "TY  - JOUR\nTI  - One\nER  - \n")
    monkeypatch.chdir(lib)
    sp = {**_spellings(p), "relative": p.name}
    for name, s in sp.items():
        assert R.manifest_key(s) == R.manifest_key(str(p)), name
        assert R.ris_owner(s) == "pipeline", name
    assert R.write_ris(sp["forward"], "TY  - JOUR\nTI  - Two\nER  - \n")     # pipeline-owned: replaced
    p.write_text("TY  - JOUR\nTI  - Two (edited by hand)\nER  - \n", encoding="utf-8")
    for name, s in sp.items():
        assert R.ris_owner(s) == "edited", name
        assert R.write_ris(s, "TY  - JOUR\nTI  - Three\nER  - \n") is False, name
    assert "edited by hand" in p.read_text(encoding="utf-8")
    assert R.write_ris(sp["relative"], "TY  - JOUR\nTI  - Three\nER  - \n", force=True)


def test_resolve_meta_shape_and_typed_failure(web):
    import ris_emit as R
    d = "10.1249/vb.9503"
    web.add(R.CROSSREF_WORK.format(doi=U._doi.encode_path(d)), (503, JSON, b"{}"))
    with pytest.raises(R.MetadataUnavailable):
        R.resolve_meta(d)
    assert web.to("api.datacite.org") == []                       # an outage never falls through
    nf = "10.1249/vb.9404"
    web.add(R.CROSSREF_WORK.format(doi=U._doi.encode_path(nf)), (404, JSON, b"Resource not found."))
    web.add(R.DATACITE_WORK.format(doi=U._doi.encode_path(nf)), (404, JSON, b'{"errors":[]}'))
    web.ra["10.1249"] = "Crossref"
    out = R.resolve_meta(nf)
    assert isinstance(out, tuple) and out == ({}, "none")


# ------------------------------------------------------------------ S2: no key in a persisted string
def test_s2_key_never_reaches_outcome_detail_or_walk_reason(net_env, monkeypatch):
    """S2 error bodies are appended to Outcome.detail (and so to Walk.reason, reports, logs). A key
    echoed in a body must not survive: redact only knows `x-api-key:`/`api_key=` shapes, not the
    bare value."""
    from litpipe import s2
    key = "Zq7vKEYvalue0123456789abcdef"
    monkeypatch.setenv("S2_API_KEY", key)

    def transport(method, url, headers, body, timeout, max_bytes):
        msg = json.dumps({"message": f"API key {key} is not valid for this endpoint"}).encode()
        return net._Raw(403, net.CaseInsensitiveDict({"Content-Type": "application/json"}), msg, msg[:65536], len(msg))

    monkeypatch.setitem(net._TRANSPORTS, "requests", transport)
    monkeypatch.setitem(net._TRANSPORTS, "urllib", transport)
    w = s2.citations("10.1249/jsr.0b013e31825615cc", expected=5, session=s2.Session())
    assert w.failed
    assert key not in (w.reason or "")
    assert key not in net_env.ledger_text()


# ------------------------------------------------------------------ 4948606: SICI and trailing junk
@pytest.mark.parametrize("doi", [
    "10.1002/1097-0142(20000915)89:6<1345::aid-cncr19>3.0.co;2-7",   # Wiley SICI, 8-digit date, no marker
    "10.1002/1097-4636(1996)30:1<1::aid-jbm1>3.0.co;2-x",
    "10.1519/1533-4287(1990)004<0047:rbrasp>2.3.co;2",
    "10.1002/(sici)1097-4636(199601)30:1<1::aid-jbm1>3.0.co;2-x",
])
def test_sici_dois_stay_whole_for_every_date_width(doi):
    assert U._doi.normalise(doi) == doi


@pytest.mark.parametrize("text, want", [
    ("doi:10.1519/1533-4287(1990)004<0047:RBRASP>2.3.CO;2</p>", "10.1519/1533-4287(1990)004<0047:rbrasp>2.3.co;2"),
    ("https://doi.org/10.1002/(SICI)1097-4636(199601)30:1<1::AID-JBM1>3.0.CO;2-X</td>",
     "10.1002/(sici)1097-4636(199601)30:1<1::aid-jbm1>3.0.co;2-x"),
])
def test_a_closing_html_tag_is_not_part_of_a_sici_doi(text, want):
    assert U._doi.normalise(text) == want


@pytest.mark.parametrize("raw, want", [
    ("10.1016/j.jsams.2019.04.012.", "10.1016/j.jsams.2019.04.012"),
    ("10.1016/j.jsams.2019.04.012)", "10.1016/j.jsams.2019.04.012"),
    ("10.1371/journal.pone.0123456.pdf", "10.1371/journal.pone.0123456"),
    ("10.1152/japplphysiol.00001.2020Smith", "10.1152/japplphysiol.00001.2020"),
    ("10.1096/fj.201900106RRR", "10.1096/fj.201900106rrr"),
])
def test_trailing_junk_is_still_stripped_and_revision_tails_kept(raw, want):
    assert U._doi.normalise(raw) == want
