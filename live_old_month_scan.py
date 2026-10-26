#!/usr/bin/env python3
"""live_old_month_scan.py: read-only scan of the live site for an old edition month.

Monthly cycle close (MEMORY/project_monthly_cycle_checklist.md): grep every
sitemap page, llms.txt and agents.md for the OLD month so nothing stays on it
(September 2026 left three surfaces on August until a scan found them).

Fetches https://rings.com/sitemap.xml -> sitemap_pages / sitemap_blogs, then each
page, with a cache-busting query. Reports pages whose visible text contains the
old month token. Archive pages (handles ending -YYYY-MM) and the Previous
editions lists legitimately mention old months: hits on archive handles are
listed separately and a hit that is only "<Month> 2026 edition" inside a
Previous editions link is ignored.

Local curl can pin a stale cache variant on this site (publish-gate caveat), so
any page this scan reports must be re-checked in a clean browser before it is
called a fail. Cache-busting query is added; propagation can take 10-30 minutes.

Usage:
  python3 live_old_month_scan.py --old "September 2026" [--new "October 2026"] [--workers 6]
Prints a summary; writes the full result JSON to --out (default: scratch path given).
"""
import argparse
import concurrent.futures as cf
import json
import re
import sys
import time
import urllib.request

SITE = "https://rings.com"
ARCHIVE = re.compile(r"-20\d\d-\d\d$")
PREV_EDITION_LINK = re.compile(r"<a [^>]*>[A-Za-z]+ 2026 edition</a>", re.I)


def fetch(url, tries=3):
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (lgd-scan)"})
            return urllib.request.urlopen(req, timeout=30).read().decode("utf-8", "replace")
        except Exception as e:  # noqa: BLE001
            err = str(e)
            time.sleep(1.5 * (i + 1))
    return "FETCHERROR " + err


def sitemap_urls():
    idx = fetch(SITE + "/sitemap.xml")
    urls = []
    for sm in re.findall(r"<loc>([^<]+)</loc>", idx):
        if "sitemap_pages" in sm or "sitemap_blogs" in sm:
            urls += re.findall(r"<loc>([^<]+)</loc>", fetch(sm))
    return sorted(set(urls))


def scan(url, old, new):
    h = fetch(url + ("&" if "?" in url else "?") + f"scan={int(time.time())}")
    if h.startswith("FETCHERROR"):
        return {"url": url, "error": h[:120]}
    body = re.sub(r"<script\b[^>]*ld\+json[^>]*>.*?</script>", "", h, flags=re.S)
    body = PREV_EDITION_LINK.sub("", body)
    text = re.sub(r"<[^>]+>", " ", body)
    tok = old.lower()
    n_old = text.lower().count(tok)
    return {"url": url, "old": n_old, "new": text.lower().count(new.lower()) if new else None}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--old", required=True)
    ap.add_argument("--new", default=None)
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--out", default="live_old_month_scan.json")
    ap.add_argument("--extra", nargs="*", default=[SITE + "/llms.txt", SITE + "/agents.md", SITE + "/llms-full.txt"])
    a = ap.parse_args()
    urls = [u for u in sitemap_urls() if "/pages/" in u or "/blogs/" in u] + a.extra
    print(len(urls), "urls", file=sys.stderr)
    res = []
    with cf.ThreadPoolExecutor(a.workers) as ex:
        for r in ex.map(lambda u: scan(u, a.old, a.new), urls):
            res.append(r)
            time.sleep(0.05)
    err = [r for r in res if "error" in r]
    hits = [r for r in res if r.get("old")]
    arch = [r for r in hits if ARCHIVE.search(r["url"].rstrip("/").split("?")[0])]
    live = [r for r in hits if r not in arch]
    json.dump(res, open(a.out, "w"), indent=1)
    print(f"scanned {len(res)}; fetch errors {len(err)}; pages with '{a.old}': {len(hits)} "
          f"(archive pages {len(arch)}, non-archive {len(live)})")
    for r in live[:60]:
        print("  STALE?", r["url"], "old=", r["old"], "new=", r.get("new"))
    if len(live) > 60:
        print("  ... and", len(live) - 60, "more (see JSON)")
    for r in err[:10]:
        print("  ERROR", r["url"], r["error"])


if __name__ == "__main__":
    main()
