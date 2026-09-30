"""W2-A2: fetch_figures. Figure images come from the PMC Cloud Service media list (never the PMC
article page or its CDN), each figure carries the article licence mapped to the consumers' four
triage categories, and a network failure is recorded, never raised. Offline: transports stubbed
by URL; S3 metadata fixtures from the 2026-09-25 endpoint audit (tests/fixtures/W2-A2/)."""
import hashlib
import json
from pathlib import Path
from urllib.parse import urlsplit

import pytest
from requests.structures import CaseInsensitiveDict

import fetch_figures as FF
import jats_to_text as J
from litpipe import hosts, net
from litpipe.outcomes import Kind

FIX = Path(__file__).resolve().parent / "fixtures" / "W2-A2"
S3 = "pmc-oa-opendata.s3.amazonaws.com"


def fake_jpeg(name):
    return b"\xff\xd8\xff\xe0" + name.encode("utf-8") + b"\x00" * 64


def md5(b):
    return hashlib.md5(b).hexdigest()


def route(monkeypatch, table, default=(404, b"<Error><Code>NoSuchKey</Code></Error>", {"Content-Type": "application/xml"})):
    """Transport stub answering by URL path (query ignored): table[path] -> reply or Exception."""
    sent = []

    def fake(method, url, hdrs, body, timeout, max_bytes):
        sent.append(url)
        r = table.get(urlsplit(url).path, default)
        if isinstance(r, Exception):
            return net._Raw(error=f"{type(r).__name__}: {r}")
        status, data, h = (tuple(r) + (None,))[:3]
        h = CaseInsensitiveDict(h or {"Content-Type": "binary/octet-stream"})
        cap = max_bytes if (max_bytes and 200 <= status < 300) else None
        if cap is not None and len(data) > cap:
            return net._Raw(status, h, data[:cap + 1], data[:net.CHUNK], cap + 1, True)
        return net._Raw(status, h, data, data[:net.CHUNK], len(data))

    monkeypatch.setitem(net._TRANSPORTS, "requests", fake)
    monkeypatch.setitem(net._TRANSPORTS, "urllib", fake)
    return sent


@pytest.fixture
def env(net_env):
    return net_env


def s3_article(fixture="s3_meta_PMC11592912.1.json"):
    """The fixture's metadata with each media md5 replaced by the md5 of our fake bytes, and the
    route table serving those bytes."""
    meta = json.loads((FIX / fixture).read_text(encoding="utf-8"))
    table, urls = {}, []
    for u in meta.get("media_urls") or []:
        key = u.split("s3://pmc-oa-opendata/", 1)[1].split("?", 1)[0]
        body = fake_jpeg(key)
        table["/" + key] = (200, body)
        urls.append(f"s3://pmc-oa-opendata/{key}?md5={md5(body)}")
    if "media_urls" in meta:
        meta["media_urls"] = urls
    return meta, table


def write_sidecar(tmp_path, parsed, name="2024_Williams_Complexity.fulltext.json"):
    p = tmp_path / name
    p.write_text(json.dumps(parsed), encoding="utf-8")
    return p


def real_sidecar(tmp_path):
    parsed = J.parse_jats((FIX / "jats_PMC11592912_efetch.xml").read_bytes())
    parsed["doi"] = "10.3390/e26110970"
    return write_sidecar(tmp_path, parsed)


# ------------------------------------------------------------------ licence categories
@pytest.mark.parametrize("code,oa,cat", [
    ("CC BY", True, "OK_WITH_CREDIT"),
    ("CC BY-SA", True, "OK_WITH_CREDIT"),
    ("CC0", True, "OK_WITH_CREDIT"),
    ("cc by", "Y", "OK_WITH_CREDIT"),
    ("CC BY", False, "NOT_CLEARED"),             # a licence string without the OA flag is not cleared
    ("CC BY-NC", True, "CHECK_NC"),
    ("CC BY-NC-SA", True, "CHECK_NC"),
    ("CC BY-NC-ND", True, "CHECK_ND"),           # ND wins over NC: a redraw is a derivative
    ("CC BY-ND", True, "CHECK_ND"),
    ("TDM", False, "NOT_CLEARED"),               # author manuscripts
    ("", True, "NOT_CLEARED"),
    (None, None, "NOT_CLEARED"),
])
def test_licence_category(code, oa, cat):
    assert FF.licence_category(code, oa) == cat


def test_s3_https():
    assert FF.s3_https("s3://pmc-oa-opendata/PMC1.1/x-g001.jpg?md5=ABCDEF0123456789abcdef0123456789") == (
        "https://pmc-oa-opendata.s3.amazonaws.com/PMC1.1/x-g001.jpg", "abcdef0123456789abcdef0123456789")
    assert FF.s3_https("s3://pmc-oa-opendata/PMC1.1/x.jpg") == ("https://pmc-oa-opendata.s3.amazonaws.com/PMC1.1/x.jpg", None)
    assert FF.s3_https("https://cdn.ncbi.nlm.nih.gov/pmc/blobs/x.jpg") == (None, None)


# ------------------------------------------------------------------ figures_from_s3
def test_s3_media_fixture_yields_figures_with_licences(env, monkeypatch, tmp_path):
    """Acceptance: an S3 media fixture yields figures with licences, written beside the sidecar."""
    meta, table = s3_article()
    sent = route(monkeypatch, table)
    sc = real_sidecar(tmp_path)
    figs = FF.figures_from_s3(meta, sc)
    assert len(figs) == 8
    for i, f in enumerate(figs, 1):
        assert f["licence"] == "CC BY" and f["licence_category"] == "OK_WITH_CREDIT"
        assert f["source"] == "pmc_s3" and f["image_status"] == "OK"
        assert f["image_path"].startswith(f"2024_Williams_Complexity.fig{i}")
        assert (tmp_path / f["image_path"]).read_bytes()[:3] == b"\xff\xd8\xff"
    f4 = figs[3]
    assert f4["image_paths"] == [f"2024_Williams_Complexity.fig4{c}.jpg" for c in "abcd"]
    assert f4["image_url"] == "https://pmc-oa-opendata.s3.amazonaws.com/PMC11592912.1/entropy-26-00970-g004a.jpg"
    assert len(sent) == 18 and {urlsplit(u).netloc for u in sent} == {S3}
    on_disk = json.loads(sc.read_text(encoding="utf-8"))
    assert on_disk["figures"] == figs and on_disk["doi"] == "10.3390/e26110970"
    assert on_disk["title"].startswith("Complexity and Variation")   # nothing dropped
    assert "figure_media_unmatched" not in on_disk


def test_ncnd_article_is_check_nd(env, monkeypatch, tmp_path):
    meta, table = s3_article("s3_meta_PMC12876392.1.json")
    route(monkeypatch, table)
    figs_in = [{"label": f"Figure {i}", "caption": "c", "graphic_href": f"gr{i}.jpg", "image_path": "", "image_url": ""}
               for i in range(1, 6)]
    sc = write_sidecar(tmp_path, {"pmcid": "PMC12876392", "doi": "10.1/x", "figures": figs_in})
    figs = FF.figures_from_s3(meta, sc)
    assert {f["licence_category"] for f in figs} == {"CHECK_ND"} and all(f["image_path"] for f in figs)


def test_author_manuscript_has_no_media_but_gets_its_licence(env, monkeypatch, tmp_path):
    meta = json.loads((FIX / "s3_meta_PMC10041407.1.json").read_text(encoding="utf-8"))
    sent = route(monkeypatch, {})
    sc = write_sidecar(tmp_path, {"pmcid": "PMC10041407", "figures": [
        {"label": "Figure 1", "caption": "c", "graphic_href": "nihms-f0001.jpg", "image_path": "", "image_url": ""}]})
    figs = FF.figures_from_s3(meta, sc)
    assert sent == []
    assert figs[0]["licence"] == "TDM" and figs[0]["licence_category"] == "NOT_CLEARED"
    assert figs[0]["image_status"] == "NO_MEDIA" and not figs[0]["image_path"]


def test_a_network_failure_is_recorded_not_raised(env, monkeypatch, tmp_path):
    meta, table = s3_article()
    first = "/PMC11592912.1/entropy-26-00970-g001.jpg"
    table[first] = ConnectionError("connection reset")
    route(monkeypatch, table)
    sc = real_sidecar(tmp_path)
    figs = FF.figures_from_s3(meta, sc)
    assert figs[0]["image_status"].startswith("ERR_TRANSPORT") and not figs[0]["image_path"]
    assert figs[0]["licence_category"] == "OK_WITH_CREDIT"
    assert all(f["image_status"] == "OK" for f in figs[1:])
    assert not list(tmp_path.glob("*.fig1.jpg"))


def test_a_refused_host_is_recorded_and_nothing_more_is_sent(env, monkeypatch, tmp_path):
    meta, table = s3_article()
    table = {k: (403, b"AccessDenied", {"Content-Type": "application/xml"}) for k in table}
    sent = route(monkeypatch, table)
    figs = FF.figures_from_s3(meta, real_sidecar(tmp_path))
    assert len(sent) == 1                                   # the first 403 refuses the host for the run
    assert figs[0]["image_status"] == "HTTP_403"
    assert all(f["image_status"].startswith(("ERR_REFUSED", "HTTP_403")) for f in figs)


def test_md5_mismatch_is_not_written(env, monkeypatch, tmp_path):
    meta, table = s3_article()
    table["/PMC11592912.1/entropy-26-00970-g001.jpg"] = (200, fake_jpeg("something else"))
    route(monkeypatch, table)
    figs = FF.figures_from_s3(meta, real_sidecar(tmp_path))
    assert figs[0]["image_status"] == "MD5_MISMATCH" and not figs[0]["image_path"]


def test_a_non_image_body_is_not_written(env, monkeypatch, tmp_path):
    meta, table = s3_article()
    table["/PMC11592912.1/entropy-26-00970-g001.jpg"] = (200, b"<html>not an image</html>")
    route(monkeypatch, table)
    figs = FF.figures_from_s3(meta, real_sidecar(tmp_path))
    assert figs[0]["image_status"] == "NOT_IMAGE"


def test_existing_image_with_the_right_md5_is_not_downloaded_again(env, monkeypatch, tmp_path):
    meta, table = s3_article()
    sc = real_sidecar(tmp_path)
    (tmp_path / "2024_Williams_Complexity.fig1.jpg").write_bytes(table["/PMC11592912.1/entropy-26-00970-g001.jpg"][1])
    sent = route(monkeypatch, table)
    figs = FF.figures_from_s3(meta, sc)
    assert figs[0]["image_status"] == "EXISTS" and figs[0]["image_path"] == "2024_Williams_Complexity.fig1.jpg"
    assert len(sent) == 17


def test_a_name_taken_by_another_image_is_not_overwritten(env, monkeypatch, tmp_path):
    meta, table = s3_article()
    sc = real_sidecar(tmp_path)
    other = tmp_path / "2024_Williams_Complexity.fig1.jpg"
    other.write_bytes(b"\xff\xd8\xff old image from another source")
    route(monkeypatch, table)
    figs = FF.figures_from_s3(meta, sc)
    assert figs[0]["image_status"] == "NAME_TAKEN" and other.read_bytes().endswith(b"another source")


def test_an_older_fetch_the_sidecar_names_is_kept_unverified(env, monkeypatch, tmp_path):
    """A sidecar fetched from the old CDN route names its own .fig1.jpg; the S3 bytes differ. The
    file is kept, the licence is added, and nothing claims it came from S3."""
    meta, table = s3_article()
    parsed = J.parse_jats((FIX / "jats_PMC11592912_efetch.xml").read_bytes())
    parsed["figures"][0].update(image_path="2024_Williams_Complexity.fig1.jpg", image_url="https://old/x.jpg")
    sc = write_sidecar(tmp_path, parsed)
    (tmp_path / "2024_Williams_Complexity.fig1.jpg").write_bytes(b"\xff\xd8\xff older encoding")
    sent = route(monkeypatch, table)
    f1 = FF.figures_from_s3(meta, sc)[0]
    assert f1["image_status"] == "EXISTS_UNVERIFIED" and f1["image_path"] == "2024_Williams_Complexity.fig1.jpg"
    assert f1["image_url"] == "https://old/x.jpg" and "source" not in f1
    assert f1["licence_category"] == "OK_WITH_CREDIT" and len(sent) == 17


def test_old_image_fields_survive_a_failed_refetch(env, monkeypatch, tmp_path):
    meta, table = s3_article()
    table = {k: ConnectionError("down") for k in table}
    route(monkeypatch, table)
    parsed = J.parse_jats((FIX / "jats_PMC11592912_efetch.xml").read_bytes())
    parsed["figures"][0]["image_path"] = "old.fig1.jpg"
    sc = write_sidecar(tmp_path, parsed)
    figs = FF.figures_from_s3(meta, sc, force=True)
    assert figs[0]["image_path"] == "old.fig1.jpg"           # never dropped
    assert json.loads(sc.read_text(encoding="utf-8"))["figures"][0]["image_path"] == "old.fig1.jpg"


def test_media_no_figure_names_are_listed(env, monkeypatch, tmp_path):
    """An old sidecar (parsed before the V1-N3 fix) names 2 of 8 figures: the rest is made visible."""
    meta, table = s3_article()
    route(monkeypatch, table)
    parsed = J.parse_jats((FIX / "jats_PMC11592912_efetch.xml").read_bytes())
    parsed["figures"] = [f for f in parsed["figures"] if f["label"] in ("Figure 2", "Figure 3")]
    sc = write_sidecar(tmp_path, parsed)
    FF.figures_from_s3(meta, sc)
    unmatched = json.loads(sc.read_text(encoding="utf-8"))["figure_media_unmatched"]
    assert len(unmatched) == 16 and "entropy-26-00970-g001.jpg" in unmatched


def test_lib_dir_places_the_images(env, monkeypatch, tmp_path):
    meta, table = s3_article()
    route(monkeypatch, table)
    out = tmp_path / "images"
    out.mkdir()
    figs = FF.figures_from_s3(meta, real_sidecar(tmp_path), lib_dir=out)
    assert (out / figs[0]["image_path"]).exists() and not (tmp_path / figs[0]["image_path"]).exists()


def test_missing_sidecar_sends_nothing_and_creates_nothing(env, monkeypatch, tmp_path):
    meta, table = s3_article()
    sent = route(monkeypatch, table)
    lib = tmp_path / "lib"
    lib.mkdir()
    assert FF.figures_from_s3(meta, lib / "absent.fulltext.json") == []
    assert sent == [] and list(lib.iterdir()) == []


# ------------------------------------------------------------------ the prohibited page scrape
def test_fetch_pmc_html_refuses_without_sending(env, monkeypatch, capsys):
    sent = route(monkeypatch, {})
    assert FF.fetch_pmc_html("PMC4977162") is None
    assert sent == [] and "prohibited" in capsys.readouterr().err


def test_no_code_path_names_a_prohibited_route():
    src = Path(FF.__file__).read_text(encoding="utf-8")
    assert "cdn.ncbi.nlm.nih.gov" not in src and "lit_net" not in src and "requests" not in src.split('"""', 2)[2]


# ------------------------------------------------------------------ S3 metadata and the CLI
def test_s3_article_meta_lists_versions_and_prefers_the_highest(env, monkeypatch):
    v1 = (FIX / "s3_meta_PMC7988253.1.json").read_bytes()
    v2 = (FIX / "s3_meta_PMC7988253.2.json").read_bytes()
    sent = route(monkeypatch, {"/": (200, (FIX / "s3_list_two_versions.xml").read_bytes(), {"Content-Type": "application/xml"}),
                               "/metadata/PMC7988253.1.json": (200, v1),
                               "/metadata/PMC7988253.2.json": (200, v2)})
    o = FF.s3_article_meta("PMC7988253")
    assert o.ok and o.payload["version"] == 2
    assert "list-type=2" in sent[0] and "prefix=PMC7988253." in sent[0]


def test_s3_article_meta_empty_list_is_no_match(env, monkeypatch):
    route(monkeypatch, {"/": (200, (FIX / "s3_list_empty.xml").read_bytes(), {"Content-Type": "application/xml"})})
    o = FF.s3_article_meta("PMC2300466")
    assert o.kind is Kind.NO_MATCH and o.payload is None


def test_s3_article_meta_prefers_a_published_version(env, monkeypatch):
    am = dict(json.loads((FIX / "s3_meta_PMC7988253.2.json").read_text()), is_manuscript=True, version=2)
    listing = (FIX / "s3_list_two_versions.xml").read_bytes()
    route(monkeypatch, {"/": (200, listing, {"Content-Type": "application/xml"}),
                        "/metadata/PMC7988253.1.json": (200, (FIX / "s3_meta_PMC7988253.1.json").read_bytes()),
                        "/metadata/PMC7988253.2.json": (200, json.dumps(am).encode())})
    assert FF.s3_article_meta("PMC7988253").payload["version"] == 1


def test_cli_sidecar_run_end_to_end(env, monkeypatch, tmp_path, capsys):
    """--sidecar: reads the pmcid, fetches the S3 list and metadata, then the media."""
    meta, table = s3_article()
    listing = (b'<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
               b"<KeyCount>1</KeyCount><CommonPrefixes><Prefix>PMC11592912.1/</Prefix></CommonPrefixes></ListBucketResult>")
    table["/"] = (200, listing, {"Content-Type": "application/xml"})
    table["/metadata/PMC11592912.1.json"] = (200, json.dumps(meta).encode())
    route(monkeypatch, table)
    sc = real_sidecar(tmp_path)
    assert FF.main(["--sidecar", str(sc)]) == 0
    out = capsys.readouterr().out
    assert "OK" in out and "8/8 figures" in out
    status, saved, total = FF.fetch_figures_for_sidecar(str(sc))
    assert status == "already-fetched" and saved == total == 8


def test_cli_statuses(env, monkeypatch, tmp_path):
    route(monkeypatch, {"/": ConnectionError("down")})
    sc = real_sidecar(tmp_path)
    assert FF.fetch_figures_for_sidecar(str(sc))[0] == "s3-unavailable"
    route(monkeypatch, {"/": (200, (FIX / "s3_list_empty.xml").read_bytes(), {"Content-Type": "application/xml"})})
    assert FF.fetch_figures_for_sidecar(str(sc))[0] == "not-in-s3"
    nopmc = write_sidecar(tmp_path, {"figures": [{"graphic_href": "a.jpg"}]}, "x.fulltext.json")
    assert FF.fetch_figures_for_sidecar(str(nopmc))[0] == "no-pmcid"
    stale = write_sidecar(tmp_path, {"pmcid": "PMC1", "figures": [{"label": "F"}]}, "y.fulltext.json")
    assert FF.fetch_figures_for_sidecar(str(stale))[0] == "no-graphic-href"


# ------------------------------------------------------------------ download_image (legacy adapter)
def test_download_image_statuses(env, monkeypatch, tmp_path):
    jpg = fake_jpeg("a")
    route(monkeypatch, {"/ok.jpg": (200, jpg), "/nope.jpg": (200, b"nope"),
                        "/big.jpg": (200, b"\xff\xd8\xff" + b"x" * (FF.MAX_IMAGE_BYTES + 10))})
    base = "https://" + S3
    assert FF.download_image(base + "/ok.jpg", str(tmp_path / "a.jpg")) == (True, "OK", len(jpg))
    assert FF.download_image(base + "/nope.jpg", str(tmp_path / "b.jpg")) == (False, "NOT_IMAGE", 4)
    ok, st, size = FF.download_image(base + "/big.jpg", str(tmp_path / "c.jpg"))
    assert (ok, st) == (False, "TOO_LARGE") and size > FF.MAX_IMAGE_BYTES
    assert FF.download_image(base + "/missing.jpg", str(tmp_path / "d.jpg"))[:2] == (False, "HTTP_404")
    assert not (tmp_path / "b.jpg").exists() and not (tmp_path / "c.jpg").exists()
