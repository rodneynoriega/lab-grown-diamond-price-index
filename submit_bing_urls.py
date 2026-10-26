#!/usr/bin/env python3
"""submit_bing_urls.py: tell Bing which rings.com research pages changed.

Why: Shopify only pings Bing (IndexNow) for product pages, and the last ping
was in April. Our index, price and cost pages change monthly, and freshness
drives AI citations (Bing AI Performance report, 2026-10-12).

Why not IndexNow: IndexNow needs a plain-text key file at the site root, and
Shopify cannot serve one (no root files, redirect scope missing). Bing's own
Webmaster API (SubmitUrlBatch) needs no key file, only an API key.

Usage:
  python3 submit_bing_urls.py --since 2026-10-05            # DRY RUN: list URLs
  python3 submit_bing_urls.py --urls-file urls.txt          # DRY RUN: explicit list
  BING_WEBMASTER_API_KEY=... python3 submit_bing_urls.py --since 2026-10-05 --yes

URLs come from the live sitemap (lastmod >= --since), research pages only
(/pages/ and /blogs/), never product or stone pages. Dry run by default: this
sends data to an external service, so --yes is required to submit.
Get the key in Bing Webmaster Tools > Settings > API access > API Key.
"""
import argparse, json, os, re, sys, urllib.request

SITE = "https://rings.com"
SITEMAP = SITE + "/sitemap.xml"
API = "https://ssl.bing.com/webmaster/api.svc/json/SubmitUrlBatch?apikey={key}"
RESEARCH = re.compile(r"^https://rings\.com/(pages|blogs)/")


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    return urllib.request.urlopen(req, timeout=30).read().decode("utf-8", "replace")


def urls_since(since):
    out = {}
    index = fetch(SITEMAP)
    for sm in re.findall(r"<loc>([^<]+)</loc>", index):
        if "sitemap_pages" not in sm and "sitemap_blogs" not in sm:
            continue  # skip products, collections
        xml = fetch(sm.replace("&amp;", "&"))
        for blk in re.findall(r"<url>(.*?)</url>", xml, re.S):
            loc = re.search(r"<loc>([^<]+)</loc>", blk)
            mod = re.search(r"<lastmod>([^<]+)</lastmod>", blk)
            if loc and mod and mod.group(1)[:10] >= since and RESEARCH.match(loc.group(1)):
                out[loc.group(1)] = mod.group(1)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", help="YYYY-MM-DD: submit pages with sitemap lastmod on/after this date")
    ap.add_argument("--urls-file", help="text file, one rings.com URL per line")
    ap.add_argument("--yes", action="store_true", help="actually submit to Bing")
    a = ap.parse_args()
    if bool(a.since) == bool(a.urls_file):
        sys.exit("give exactly one of --since or --urls-file")
    if a.since:
        found = urls_since(a.since)
        urls = sorted(found)
    else:
        urls = [u.strip() for u in open(a.urls_file) if u.strip()]
    bad = [u for u in urls if not u.startswith(SITE + "/")]
    if bad:
        sys.exit(f"refusing non-rings.com URLs: {bad[:3]}")
    print(f"{len(urls)} URLs:")
    for u in urls:
        print("  ", u)
    if not urls:
        return
    if not a.yes:
        print("DRY RUN: nothing sent. Add --yes (and BING_WEBMASTER_API_KEY) to submit.")
        return
    key = os.environ.get("BING_WEBMASTER_API_KEY")
    if not key:
        sys.exit("BING_WEBMASTER_API_KEY not set")
    body = json.dumps({"siteUrl": SITE, "urlList": urls}).encode()
    req = urllib.request.Request(API.format(key=key), data=body,
                                 headers={"Content-Type": "application/json; charset=utf-8"})
    try:
        resp = urllib.request.urlopen(req, timeout=30)
        print("Bing responded", resp.status, resp.read().decode()[:300])
    except urllib.error.HTTPError as e:
        sys.exit(f"Bing rejected the batch: {e.code} {e.read().decode()[:300]} (check the daily URL quota in Bing Webmaster Tools)")


if __name__ == "__main__":
    main()
