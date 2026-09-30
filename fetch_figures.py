"""Figure images for PMC sidecars, from the PMC Cloud Service (the `pmc-oa-opendata` S3 bucket),
each tagged with the article's reuse licence.

Source. The PMC article pages and their CDN blobs, which this tool used to scrape, are prohibited
routes (litpipe.hosts: NCBI bars scripting its web pages, and pmc.ncbi.nlm.nih.gov refuses this
pipeline's traffic since 2026-09-30). The Cloud Service README
(https://pmc-oa-opendata.s3.amazonaws.com/README.txt, read 2026-09-30) lists per article version a
JSON metadata object with `license_code`, `is_pmc_openaccess`, `is_manuscript` and
`media_urls` ("a list of S3 URL to images and supplementary data files, if available"); media are
present only "when permissible by the publishers' licenses", and "All S3 URLs in the JSON include
the MD5 digest of the object in form of a URL parameter, md5." The media file names equal the JATS
`<graphic>` hrefs (V1 P10: 13 of 13), so a figure is matched by name.

Licence. Every figure carries the article's `license_code` and a triage category, the four used
by the consumers' figure-licence tooling (advisory, not legal advice):
    OK_WITH_CREDIT   cc0 / cc by / cc by-sa, and the version is in the open-access subset
    CHECK_NC         a non-commercial term (cc by-nc, cc by-nc-sa)
    CHECK_ND         any no-derivatives term (a redraw is a derivative)
    NOT_CLEARED      a record came back and it carries no reusable licence (TDM, none)

Sidecar shape after a fetch (merged into the existing sidecar, never dropping a filled field):

    "figures": [
      {
        "label": "Figure 1", "caption": "...",
        "graphic_href": "x-g001.jpg",                  # bare filename from JATS
        "image_path": "2014_Ketko_TCR.fig1.jpg",       # relative to the library dir
        "image_url":  "https://pmc-oa-opendata.s3.amazonaws.com/PMC4977162.1/x-g001.jpg",
        "licence": "CC BY", "licence_category": "OK_WITH_CREDIT",
        "source": "pmc_s3", "image_status": "OK"
      }, ...
    ]
A multi-panel figure also gets "image_paths" (".fig4a.jpg", ".fig4b.jpg", ...). A failed download
is recorded in "image_status" (a network failure never raises).

Usage:
  python fetch_figures.py --lib-dir /path/to/literature/            # all sidecars in dir
  python fetch_figures.py --sidecar /path/to/foo.fulltext.json      # one sidecar
  python fetch_figures.py --lib-dir DIR --pmcid PMC4977162          # filter to one paper
"""
import argparse
import hashlib
import json
import os
import re
import sys
import tempfile
import time
from xml.etree import ElementTree as ET

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lit_util
from litpipe import hosts, net
from litpipe.outcomes import Kind, Outcome

lit_util.utf8_stdout()

S3_BUCKET = "pmc-oa-opendata"
S3_BASE = f"https://{S3_BUCKET}.s3.amazonaws.com"
_S3_URL = re.compile(r"^s3://" + re.escape(S3_BUCKET) + r"/([^?#]+)(?:\?(?:.*&)?md5=([0-9a-fA-F]{32}))?")
_S3_NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
IMAGE_EXTS = frozenset({"jpg", "jpeg", "png", "gif", "tif", "tiff", "webp"})
MAX_IMAGE_BYTES = 30_000_000  # 30MB cap, no figure should be this big
SOURCE = "pmc_s3"


def fetch_pmc_html(pmcid, timeout=20):
    """Retired, kept importable. PMC article pages are a prohibited route (litpipe.hosts), so this
    returns None without sending anything; figures come from the PMC Cloud Service
    (figures_from_s3)."""
    print(f"  [fetch_figures] {pmcid}: PMC article pages are not fetched (prohibited route); "
          f"figures come from the PMC Cloud Service", file=sys.stderr)
    return None


# ------------------------------------------------------------------------------ licence
def licence_category(license_code, is_oa) -> str:
    """The triage category of an article licence (see the module docstring). `is_oa` is the
    version's is_pmc_openaccess (True/False, or "Y"/"N")."""
    lic = (license_code or "").strip().lower()
    if not lic:
        return "NOT_CLEARED"
    words = lic.replace("-", " ").split()
    if "nd" in words:
        return "CHECK_ND"
    if "nc" in words:
        return "CHECK_NC"
    oa = is_oa is True or str(is_oa).strip().upper() in ("Y", "YES", "TRUE")
    if lic.startswith("cc") or lic in {"cc0", "public domain"}:
        return "OK_WITH_CREDIT" if oa else "NOT_CLEARED"
    return "NOT_CLEARED"


# ------------------------------------------------------------------------------ S3 helpers
def s3_https(s3_url):
    """"s3://pmc-oa-opendata/KEY?md5=X" -> ("https://pmc-oa-opendata.s3.amazonaws.com/KEY", "x"),
    or (None, None) for anything outside the bucket. The md5 query is ours to check, not S3's."""
    m = _S3_URL.match((s3_url or "").strip())
    if not m:
        return None, None
    return f"{S3_BASE}/{m.group(1)}", (m.group(2) or "").lower() or None


def _media_images(article_meta):
    """[(basename, https_url, md5)] for the image files in media_urls, in listed order."""
    out = []
    for u in article_meta.get("media_urls") or []:
        https, md5 = s3_https(u)
        if not https:
            continue
        base = https.rsplit("/", 1)[-1]
        if base.rsplit(".", 1)[-1].lower() in IMAGE_EXTS:
            out.append((base, https, md5))
    return out


def s3_article_meta(pmcid, timeout=30) -> Outcome:
    """The Cloud Service metadata JSON for a PMCID (payload: the dict), through litpipe.net.
    Lists the article's version prefixes (versions may be 2 without 1; do not assume `.1`), reads
    each version's metadata, and prefers a published version (is_manuscript false) that lists media,
    then the highest version. NO_MATCH when the bucket holds no version."""
    pmcid = (pmcid or "").strip().upper()
    if not pmcid.startswith("PMC"):
        pmcid = "PMC" + pmcid
    o = net.get(S3_BASE + "/", params={"list-type": "2", "prefix": f"{pmcid}.", "delimiter": "/"},
                timeout=(10, timeout), purpose="s3 list versions")
    if not o.ok:
        return o
    try:
        root = ET.fromstring(o.payload.content or b"")
    except ET.ParseError as e:
        return Outcome(Kind.OUTAGE, status=o.status, host=o.host, attempts=o.attempts,
                       detail=f"S3 list body is not XML: {e}")
    versions = []
    for p in root.findall("s3:CommonPrefixes/s3:Prefix", _S3_NS):
        m = re.fullmatch(re.escape(pmcid) + r"\.(\d+)/", (p.text or "").strip())
        if m:
            versions.append(int(m.group(1)))
    if not versions:
        return Outcome(Kind.NO_MATCH, status=o.status, host=o.host, attempts=o.attempts,
                       detail=f"{pmcid}: not in the PMC Cloud Service datasets")
    metas, last_fail, attempts = [], None, o.attempts
    for v in sorted(versions, reverse=True):
        mo = net.get(f"{S3_BASE}/metadata/{pmcid}.{v}.json", timeout=(10, timeout),
                     purpose="s3 metadata", validate=net.expect_json)
        attempts += mo.attempts
        if mo.ok:
            metas.append(mo.payload.json())
        else:
            last_fail = mo
    if not metas:
        return last_fail
    metas.sort(key=lambda d: (not d.get("is_manuscript"), bool(_media_images(d)), int(d.get("version") or 0)),
               reverse=True)
    return Outcome(Kind.OK, status=200, host=o.host, attempts=attempts, payload=metas[0])


# ------------------------------------------------------------------------------ images
def _is_image(head: bytes) -> bool:
    return (head[:3] == b"\xff\xd8\xff" or head[:4] == b"\x89PNG" or head[:3] == b"GIF"
            or head[:4] in (b"II*\x00", b"MM\x00*") or (head[:4] == b"RIFF" and head[8:12] == b"WEBP"))


def _expect_image(p):
    return None if _is_image(p.first_chunk or b"") else (Kind.ERROR, "NOT_IMAGE")


def _get_image(url, timeout=30, md5=None):
    """(bytes | None, status, size) for one image through litpipe.net. Status "OK", "NOT_IMAGE",
    "TOO_LARGE", "MD5_MISMATCH", "HTTP_<code>", or "ERR_<detail>" (transport, refused or deferred
    before a response). Never raises for a network failure."""
    try:
        o = net.get(url, timeout=(10, timeout), max_bytes=MAX_IMAGE_BYTES, purpose="figure image",
                    validate=_expect_image)
    except hosts.ProhibitedHost as e:
        return None, f"ERR_{str(e)[:60]}", 0
    p = o.payload
    size = p.total_bytes if p is not None else 0
    if o.ok:
        body = p.content or b""
        if md5 and hashlib.md5(body).hexdigest() != md5:
            return None, "MD5_MISMATCH", size
        return body, "OK", size
    if o.detail.startswith("too_large"):
        return None, "TOO_LARGE", size
    if o.detail == "NOT_IMAGE":
        return None, "NOT_IMAGE", size
    if o.status and o.status != 200:
        return None, f"HTTP_{o.status}", size
    return None, f"ERR_{o.kind}: {o.detail}"[:80], size


def _write_bytes(path, data):
    d = os.path.dirname(path) or "."
    fd, tmp = tempfile.mkstemp(dir=d, suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def download_image(url, dest, timeout=30, md5=None):
    """Download one image to `dest` through litpipe.net. Returns (ok, status, size_bytes)."""
    body, status, size = _get_image(url, timeout=timeout, md5=md5)
    if body is None:
        return False, status, size
    try:
        _write_bytes(dest, body)
    except OSError as e:
        return False, f"ERR_{str(e)[:60]}", size
    return True, "OK", size


def _file_md5(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# ------------------------------------------------------------------------------ merging
_IMAGE_FIELDS = ("image_path", "image_paths", "image_url", "licence", "licence_category", "source",
                 "image_status")


def merge_figures(old_figs, new_figs):
    """`new_figs` with each figure's image and licence fields carried over from the matching figure
    of `old_figs` (by graphic_href, else by a unique label) where the new one leaves them empty:
    lit_util.merge_sidecar keeps a whole old `figures` list only when the new list is empty, so a
    re-parse used to drop every fetched image_path."""
    old_figs = old_figs or []
    by_href = {f.get("graphic_href"): f for f in old_figs if f.get("graphic_href")}
    labels = [f.get("label") for f in old_figs if f.get("label")]
    by_label = {f.get("label"): f for f in old_figs if f.get("label") and labels.count(f.get("label")) == 1}
    out = []
    for nf in new_figs or []:
        of = by_href.get(nf.get("graphic_href")) or (by_label.get(nf.get("label")) if nf.get("label") else None)
        merged = dict(nf)
        if of:
            for k in _IMAGE_FIELDS:
                if not merged.get(k) and of.get(k):
                    merged[k] = of[k]
        out.append(merged)
    return out


def _read_sidecar(path):
    try:
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
        return d if isinstance(d, dict) else None
    except (OSError, ValueError):
        return None


# ------------------------------------------------------------------------------ the S3 path
def figures_from_s3(article_meta: dict, sidecar_path, *, lib_dir=None, force=False) -> list:
    """Download the sidecar's figures from the PMC Cloud Service and record them in the sidecar.

    `article_meta` is the parsed S3 metadata JSON of the article version (the object at
    `metadata/PMC<id>.<v>.json`). Each sidecar figure is matched by its graphic href(s) to an image
    in `media_urls`, downloaded through litpipe.net (md5 checked against the URL's `md5=`), and
    written as `<stem>.figN.<ext>` (panels `<stem>.figNa.<ext>`, ...) in `lib_dir` (default: the
    sidecar's directory), N being the figure's 1-based position in the sidecar. Every figure gets
    the article's `licence` and `licence_category`; the figures are merged into the sidecar (an
    existing image or licence field is never dropped) and returned. A network failure is recorded
    in the figure's `image_status`, never raised. Media images no figure names are listed under
    the sidecar's `figure_media_unmatched`. A missing or unreadable sidecar returns [] (this never
    creates a sidecar: a sidecar with a doi counts as a holding)."""
    sidecar_path = str(sidecar_path)
    sc = _read_sidecar(sidecar_path)
    if sc is None:
        print(f"  [fetch_figures] no readable sidecar at {sidecar_path}; nothing fetched", file=sys.stderr)
        return []
    lib_dir = str(lib_dir) if lib_dir else (os.path.dirname(sidecar_path) or ".")
    name = os.path.basename(sidecar_path)
    stem = name[:-len(".fulltext.json")] if name.endswith(".fulltext.json") else os.path.splitext(name)[0]
    lic = (article_meta.get("license_code") or "").strip()
    category = licence_category(lic, article_meta.get("is_pmc_openaccess"))

    media = _media_images(article_meta)
    by_name = {b.lower(): (b, u, m) for b, u, m in media}
    stems = {}
    for b, u, m in media:
        stems.setdefault(b.rsplit(".", 1)[0].lower(), []).append((b, u, m))
    used = set()

    old_figs = sc.get("figures") or []
    figs = []
    for idx, f0 in enumerate(old_figs, 1):
        f = dict(f0)
        f["licence"], f["licence_category"] = lic, category
        hrefs = f.get("graphic_hrefs") or ([f["graphic_href"]] if f.get("graphic_href") else [])
        panels = []
        for h in hrefs:
            base = os.path.basename(h).lower()
            hit = by_name.get(base)
            if hit is None and len(stems.get(base.rsplit(".", 1)[0], [])) == 1:
                hit = stems[base.rsplit(".", 1)[0]][0]
            if hit is not None and hit[0] not in used:
                panels.append(hit)
                used.add(hit[0])
        if not panels:
            if not f.get("image_path"):
                f["image_status"] = "NO_MEDIA"
            figs.append(f)
            continue
        paths, statuses, first_url = [], [], panels[0][1]
        for pi, (base, url, md5) in enumerate(panels):
            ext = base.rsplit(".", 1)[-1].lower()
            suffix = chr(ord("a") + pi) if len(panels) > 1 and pi < 26 else ""
            out_fn = f"{stem}.fig{idx}{suffix}.{ext}"
            out_path = os.path.join(lib_dir, out_fn)
            if os.path.exists(out_path) and not force:
                if md5 and _file_md5(out_path) != md5:
                    if out_fn in (f0.get("image_path"), *(f0.get("image_paths") or ())):
                        # the sidecar already names this file as this figure's image (an earlier
                        # fetch from another source): keep it, unverified against the S3 md5
                        paths.append(out_fn)
                        statuses.append("EXISTS_UNVERIFIED")
                    else:
                        statuses.append("NAME_TAKEN")  # another image already has this name
                    continue
                paths.append(out_fn)
                statuses.append("EXISTS")
                continue
            ok, st, _ = download_image(url, out_path, md5=md5)
            statuses.append(st)
            if ok:
                paths.append(out_fn)
        if paths:
            f["image_path"] = paths[0]
            if len(panels) > 1:
                f["image_paths"] = paths
            if any(s in ("OK", "EXISTS") for s in statuses):   # bytes verified against the S3 md5
                f["image_url"] = first_url
                f["source"] = SOURCE
        bad = [s for s in statuses if s not in ("OK", "EXISTS")]
        f["image_status"] = bad[0] if bad else ("EXISTS" if all(s == "EXISTS" for s in statuses) else "OK")
        figs.append(f)

    unmatched = [b for b, _, _ in media if b not in used]
    new = dict(sc)
    new["figures"] = figs          # built from the sidecar's own figures: nothing already there is lost
    if unmatched:
        new["figure_media_unmatched"] = unmatched
    else:
        new.pop("figure_media_unmatched", None)
    new = lit_util.merge_sidecar(sc, new)
    if new != sc:
        lit_util.atomic_write_json(sidecar_path, new)  # RC4: crash-safe rewrite
    return figs


def fetch_figures_for_sidecar(sidecar_path, force=False, sleep_after=0.0):
    """Fetch images (and licences) for one sidecar. Returns (status, n_saved, n_total).

    status in {"ok", "no-pmcid", "no-figs", "no-graphic-href", "already-fetched", "not-in-s3",
    "s3-unavailable"}; n_saved counts figures that have an image_path."""
    sc = _read_sidecar(sidecar_path)
    if sc is None:
        return "no-figs", 0, 0
    pmcid = (sc.get("pmcid") or "").strip()
    figs = sc.get("figures") or []
    if not pmcid:
        return "no-pmcid", 0, len(figs)
    if not figs:
        return "no-figs", 0, 0
    if not any(f.get("graphic_href") for f in figs):
        return "no-graphic-href", 0, len(figs)       # older sidecars: re-parse first
    have = [f for f in figs if f.get("graphic_href")]
    if not force and all(f.get("image_path") and f.get("licence_category") for f in have):
        return "already-fetched", sum(1 for f in figs if f.get("image_path")), len(figs)
    o = s3_article_meta(pmcid)
    if sleep_after:
        time.sleep(sleep_after)
    if o.kind is Kind.NO_MATCH:
        return "not-in-s3", 0, len(figs)
    if not o.ok:
        print(f"  [fetch_figures] {pmcid}: S3 metadata {o.kind} {o.status or ''} {o.detail}".rstrip(),
              file=sys.stderr)
        return "s3-unavailable", 0, len(figs)
    out = figures_from_s3(o.payload, sidecar_path, force=force)
    return "ok", sum(1 for f in out if f.get("image_path")), len(out)


def main(argv=None):
    ap = argparse.ArgumentParser(description="Fetch figure images and their reuse licence for PMC "
                                             "sidecars from the PMC Cloud Service.")
    ap.add_argument("--lib-dir", help="Walk all *.fulltext.json sidecars in this dir")
    ap.add_argument("--sidecar", help="Process one specific sidecar JSON path")
    ap.add_argument("--pmcid", help="Filter to one paper by PMCID (with --lib-dir)")
    ap.add_argument("--force", action="store_true",
                    help="Re-fetch even if image_path is already set")
    ap.add_argument("--sleep", type=float, default=0.0,
                    help="Extra pause between papers in seconds (default 0; host pacing is litpipe.net's)")
    args = ap.parse_args(argv)

    if args.sidecar:
        targets = [args.sidecar]
    elif args.lib_dir:
        targets = sorted(os.path.join(args.lib_dir, f)
                         for f in os.listdir(args.lib_dir)
                         if f.endswith(".fulltext.json"))
    else:
        ap.error("provide --sidecar or --lib-dir")

    counts = {}
    total_saved = total_figs = 0
    unavailable = []
    for path in targets:
        stem = os.path.basename(path)[:-len(".fulltext.json")]
        if args.pmcid:
            sc = _read_sidecar(path) or {}
            if (sc.get("pmcid") or "").upper() != args.pmcid.upper():
                continue
        status, saved, total = fetch_figures_for_sidecar(path, force=args.force, sleep_after=args.sleep)
        counts[status] = counts.get(status, 0) + 1
        total_saved += saved; total_figs += total
        if status == "ok":
            print(f"  OK    {stem[:60]:<60} {saved}/{total} figures")
        elif status == "already-fetched":
            print(f"  SKIP  {stem[:60]:<60} {saved}/{total} (already)")
        elif status == "s3-unavailable":
            unavailable.append(stem)
            print(f"  RETRY {stem[:60]:<60} (S3 unavailable; will need retry)")
        elif status == "not-in-s3":
            print(f"  --    {stem[:60]:<60} not in the PMC Cloud Service")
        elif status == "no-graphic-href":
            print(f"  STALE {stem[:60]:<60} (sidecar has no graphic_href; re-parse first)")

    print("\n=== Summary ===")
    for label, key in (("Sidecars fetched", "ok"), ("Already had images", "already-fetched"),
                       ("S3 unavailable", "s3-unavailable"), ("Not in Cloud Service", "not-in-s3"),
                       ("No PMCID", "no-pmcid"), ("No figures", "no-figs"),
                       ("Stale (no href)", "no-graphic-href")):
        print(f"  {label + ':':<21}{counts.get(key, 0)}")
    print(f"  Total images saved:  {total_saved} / {total_figs} figures")
    if unavailable:
        print("\nS3 unavailable (rerun later):")
        for s in unavailable[:10]: print(f"  - {s}")
        if len(unavailable) > 10: print(f"  ... +{len(unavailable) - 10} more")
    return 0


if __name__ == "__main__":
    sys.exit(main())
