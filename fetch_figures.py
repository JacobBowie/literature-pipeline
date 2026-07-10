"""Fetch figure images for each PMC sidecar in a library.

Strategy: scrape NCBI's rendered article HTML for `<img src="cdn.ncbi.nlm.nih.gov/pmc/blobs/...">` URLs,
match by filename to JATS `graphic_href`, download images, update sidecar.

Why scrape and not use a JATS-only path: PMC's CDN paths are hash-based per blob
(e.g. `/blobs/8fed/4977162/383ea7fc0246/...`) and only some JATS files include the
`<?cloudpmc-path?>` processing instruction with the hashed path. The rendered HTML
always exposes them in `<img src>` and is the most reliable source.

Sidecar shape after fetch:

    "figures": [
      {
        "label": "Figure 1",
        "caption": "...",
        "graphic_href": "ktmp-01-02-10929752-g001.jpg",   # bare filename from JATS
        "image_path": "2014_Ketko_TCR.fig1.jpg",          # relative to library dir
        "image_url":  "https://cdn.ncbi.nlm.nih.gov/pmc/blobs/...g001.jpg"
      },
      ...
    ]

Use case: feed `<paper>.fig1.jpg` to a Claude session as an image attachment to
validate / improve the JATS-extracted caption, or to read approximate values off
plots. For exact numerical extraction from plots, hand the image to WebPlotDigitizer.

Usage:
  python fetch_figures.py --lib-dir /path/to/literature/    # all sidecars in dir
  python fetch_figures.py --sidecar /path/to/foo.fulltext.json  # one sidecar
  python fetch_figures.py --lib-dir DIR --pmcid PMC4977162  # filter to one paper
"""
import os, sys, io, json, re, time, argparse
import requests

try:
    if getattr(sys.stdout, "encoding", "").lower() != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, OSError):
    pass
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lit_util

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
NCBI_ARTICLE = "https://pmc.ncbi.nlm.nih.gov/articles/{pmcid}/"

# Match <img src="https://cdn.ncbi.nlm.nih.gov/pmc/blobs/.../*.jpg|png|gif"
CDN_IMG_RE = re.compile(
    r'src="(https://cdn\.ncbi\.nlm\.nih\.gov/pmc/blobs/[^"]+\.(?:jpg|png|gif|jpeg))"',
    re.IGNORECASE,
)


def fetch_pmc_html(pmcid, timeout=20):
    """Return rendered HTML for a PMC article, or None on rate-limit/error."""
    try:
        r = requests.get(NCBI_ARTICLE.format(pmcid=pmcid),
                          headers={"User-Agent": UA}, timeout=timeout)
        if r.status_code != 200: return None
        # Heuristic: real article pages are >50KB; the anti-bot interstitial is ~20KB
        if len(r.content) < 30_000:
            return None
        return r.text
    except requests.RequestException:
        return None


def parse_cdn_image_urls(html):
    """Return {filename: url} of all CDN-hosted figure images in the page."""
    out = {}
    for url in CDN_IMG_RE.findall(html):
        fn = url.rsplit("/", 1)[-1]
        if fn not in out:
            out[fn] = url
    return out


def download_image(url, dest, timeout=30):
    """Stream-download an image. Returns (ok, status, size_bytes)."""
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=timeout, stream=True)
        if r.status_code != 200:
            return False, f"HTTP_{r.status_code}", 0
        first = b""
        chunks = []
        total = 0
        truncated = False
        for c in r.iter_content(8192):
            if not c: continue
            if not first: first = c
            chunks.append(c); total += len(c)
            if total > 30_000_000:
                truncated = True; break  # 30MB cap, no figure should be this big
        if truncated:  # over cap: don't write a truncated image
            return False, "TOO_LARGE", total
        # Validate magic bytes (JPG/PNG/GIF)
        if not (first[:3] == b"\xff\xd8\xff" or first[:3] == b"\x89PN" or first[:3] == b"GIF"):
            return False, "NOT_IMAGE", total
        with open(dest, "wb") as f:
            for c in chunks: f.write(c)
        return True, "OK", total
    except (requests.RequestException, OSError) as e:
        return False, f"ERR_{str(e)[:60]}", 0


def fetch_figures_for_sidecar(sidecar_path, force=False, sleep_after=1.0):
    """Fetch images for one sidecar. Returns (status, n_saved, n_total).

    status in {"ok", "no-pmcid", "no-figs", "rate-limited", "no-cdn-match"}.
    """
    with open(sidecar_path, encoding="utf-8") as f:
        sc = json.load(f)
    pmcid = sc.get("pmcid", "")
    figs = sc.get("figures", [])

    if not pmcid:
        return "no-pmcid", 0, len(figs)
    if not figs:
        return "no-figs", 0, 0
    if not any(f.get("graphic_href") for f in figs):
        # Older sidecars without graphic_href; they need re-parsing first
        return "no-graphic-href", 0, len(figs)

    # Skip if already fetched (unless --force)
    if not force and all(f.get("image_path") for f in figs if f.get("graphic_href")):
        return "already-fetched", sum(1 for f in figs if f.get("image_path")), len(figs)

    html = fetch_pmc_html(pmcid)
    time.sleep(sleep_after)
    if html is None:
        return "rate-limited", 0, len(figs)

    cdn_map = parse_cdn_image_urls(html)
    if not cdn_map:
        return "no-cdn-match", 0, len(figs)

    lib_dir = os.path.dirname(sidecar_path)
    paper_stem = os.path.basename(sidecar_path)[:-len(".fulltext.json")]
    saved = 0
    for i, fig in enumerate(figs):
        href = fig.get("graphic_href", "")
        if not href: continue
        url = cdn_map.get(href)
        if not url:
            continue
        ext = href.rsplit(".", 1)[-1].lower()
        out_fn = f"{paper_stem}.fig{i+1}.{ext}"
        out_path = os.path.join(lib_dir, out_fn)
        if os.path.exists(out_path) and not force:
            fig["image_path"] = out_fn
            fig["image_url"] = url
            saved += 1
            continue
        ok, _, _ = download_image(url, out_path)
        if ok:
            fig["image_path"] = out_fn
            fig["image_url"] = url
            saved += 1
        time.sleep(0.4)

    if saved:
        lit_util.atomic_write_json(sidecar_path, sc)  # RC4: crash-safe rewrite

    return "ok", saved, len(figs)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lib-dir", help="Walk all *.fulltext.json sidecars in this dir")
    ap.add_argument("--sidecar", help="Process one specific sidecar JSON path")
    ap.add_argument("--pmcid", help="Filter to one paper by PMCID (with --lib-dir)")
    ap.add_argument("--force", action="store_true",
                     help="Re-fetch even if image_path is already set")
    ap.add_argument("--sleep", type=float, default=1.5,
                     help="Sleep between papers (NCBI rate-limit guard, default 1.5s)")
    args = ap.parse_args()

    if args.sidecar:
        targets = [args.sidecar]
    elif args.lib_dir:
        targets = sorted(os.path.join(args.lib_dir, f)
                          for f in os.listdir(args.lib_dir)
                          if f.endswith(".fulltext.json"))
    else:
        ap.error("provide --sidecar or --lib-dir")

    n_ok = n_skip = n_rl = n_no_pmc = n_no_figs = n_no_href = 0
    total_saved = total_figs = 0
    rate_limited = []
    for path in targets:
        stem = os.path.basename(path)[:-len(".fulltext.json")]
        # Optional PMCID filter
        if args.pmcid:
            with open(path, encoding="utf-8") as f:
                if json.load(f).get("pmcid","") != args.pmcid:
                    continue
        status, saved, total = fetch_figures_for_sidecar(path, force=args.force,
                                                          sleep_after=args.sleep)
        total_saved += saved; total_figs += total
        if status == "ok":
            n_ok += 1
            print(f"  OK    {stem[:60]:<60} {saved}/{total} figures")
        elif status == "already-fetched":
            n_skip += 1
            print(f"  SKIP  {stem[:60]:<60} {saved}/{total} (already)")
        elif status == "rate-limited":
            n_rl += 1
            rate_limited.append(stem)
            print(f"  RL    {stem[:60]:<60} (rate-limited; will need retry)")
        elif status == "no-pmcid":
            n_no_pmc += 1
        elif status == "no-figs":
            n_no_figs += 1
        elif status == "no-graphic-href":
            n_no_href += 1
            print(f"  STALE {stem[:60]:<60} (sidecar has no graphic_href; re-parse first)")
        elif status == "no-cdn-match":
            print(f"  --    {stem[:60]:<60} no matching CDN URLs in HTML")

    print(f"\n=== Summary ===")
    print(f"  Sidecars fetched:    {n_ok}")
    print(f"  Already had images:  {n_skip}")
    print(f"  Rate-limited:        {n_rl}")
    print(f"  No PMCID:            {n_no_pmc}")
    print(f"  No figures:          {n_no_figs}")
    print(f"  Stale (no href):     {n_no_href}")
    print(f"  Total images saved:  {total_saved} / {total_figs} figures")
    if rate_limited:
        print(f"\nRate-limited papers (rerun later):")
        for s in rate_limited[:10]: print(f"  - {s}")
        if len(rate_limited) > 10: print(f"  ... +{len(rate_limited)-10} more")


if __name__ == "__main__":
    main()
