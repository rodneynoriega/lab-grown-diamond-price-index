"""Clean Origin lab-grown diamond scraper — full inventory pull.

Two-phase scrape:
  Phase 1 — listing: paginate the Magento AJAX endpoint to collect
             shape/carat/cut/color/clarity/price for all stones.
  Phase 2 — detail enrichment: fetch one product detail page per stone
             to get polish, symmetry, fluorescence, certificate_lab,
             and certificate_number. Uses a url-keyed cache (populated
             by the orchestrator from prior-day raw CSVs) to skip stones
             whose detail data was already fetched on a previous run.

HTTP layer: Playwright (real Chrome) + page.evaluate() fetch.
  - context.request.get() gets 403 on the AJAX endpoint (WAF detects it
    as non-browser). page.evaluate() runs the fetch from inside the live
    browser page, sharing cookies and passing WAF checks transparently.
  - Warm-up: navigate to LIST_URL to establish session/cookies, then keep
    the page open as the fetch host for all subsequent requests.
"""

from __future__ import annotations

import re
import time
import urllib.parse

from playwright.sync_api import sync_playwright

from .base import Diamond, normalize_shape

SITE_ROOT = "https://www.cleanorigin.com"
LIST_URL = f"{SITE_ROOT}/diamonds/"
RETAILER = "Clean Origin"

_SHAPE_MAP: dict[str, str] = {
    "round": "Round",
    "oval": "Oval",
    "pear": "Pear",
    "cushion": "Cushion",
    "princess": "Princess",
    "emerald": "Emerald",
    "radiant": "Radiant",
    "marquise": "Marquise",
    "asscher": "Asscher",
    "heart": "Heart",
}

ALLOWED_CUTS = {"Ideal", "Excellent"}
ALLOWED_COLORS = {"D", "E", "F", "G", "H", "I", "J", "K"}
ALLOWED_CLARITIES = {"FL", "IF", "VVS1", "VVS2", "VS1", "VS2", "SI1", "SI2", "I1"}
PAGE_SIZE = 40


# ---------------------------------------------------------------------------
# In-browser fetch helper
# ---------------------------------------------------------------------------

_FETCH_JS = """
    async ({url, headers}) => {
        const resp = await fetch(url, {
            headers: headers,
            credentials: 'include'
        });
        return {status: resp.status, text: await resp.text()};
    }
"""


def _page_fetch(page, url: str, headers: dict) -> tuple[int, str]:
    """Run a fetch() call from inside the Playwright page. Returns (status, html)."""
    result = page.evaluate(_FETCH_JS, {"url": url, "headers": headers})
    return result["status"], result["text"]


def _ajax_url(page_num: int, co_shape: str, weight_param: str) -> str:
    params = {
        "isAjax": "1",
        "diamond_shape[]": co_shape,
        "diamond_weight": weight_param,
        "product_list_order": "price",
        "p": page_num,
    }
    return LIST_URL + "?" + urllib.parse.urlencode(params)


# ---------------------------------------------------------------------------
# Listing page helpers (unchanged)
# ---------------------------------------------------------------------------

def _cell(row_html: str, attr: str) -> str | None:
    m = re.search(
        rf'data-attr="{re.escape(attr)}"[^>]*>.*?<a[^>]*>\s*(.*?)\s*</a>',
        row_html, re.S,
    )
    return m.group(1).strip() if m else None


def _parse_listing_rows(html: str) -> list[dict]:
    rows = re.findall(
        r'<tr class="diamonds-table-row[^"]*"[^>]*>(.*?)</tr>', html, re.S
    )
    results: list[dict] = []
    for row in rows:
        url_m = re.search(
            r'<a href="(https://www\.cleanorigin\.com/diamonds/[^"]+)"', row
        )
        price_m = re.search(
            r'data-price-amount="([^"]+)"[^>]*data-price-type="finalPrice"', row
        )
        results.append({
            "shape":   _cell(row, "diamond_shape"),
            "carat":   _cell(row, "diamond_weight"),
            "cut":     _cell(row, "diamond_cut_grade"),
            "color":   _cell(row, "diamond_color"),
            "clarity": _cell(row, "diamond_clarity"),
            "price":   price_m.group(1) if price_m else None,
            "url":     url_m.group(1) if url_m else None,
        })
    return results


def _total_count(html: str) -> int | None:
    m = re.search(r'<span class="toolbar-number">(\d+)</span>', html)
    return int(m.group(1)) if m else None


# ---------------------------------------------------------------------------
# Detail page helper
# ---------------------------------------------------------------------------

_DETAIL_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,*/*",
    "Referer": LIST_URL,
}

def _fetch_detail(page, url: str) -> dict:
    """Fetch a CO product detail page via in-browser fetch."""
    try:
        status, html = _page_fetch(page, url, _DETAIL_HEADERS)
        if status >= 400:
            return {}
    except Exception:
        return {}

    attr_pairs = re.findall(
        r'<div class="attr-label">([^<]+)</div>\s*<div class="attr-value">([^<]+)</div>',
        html, re.S,
    )
    attrs = {label.strip(): val.strip() for label, val in attr_pairs}

    lab_m = re.search(r'"diamond_lab"\s*:\s*"([^"]+)"', html)
    cert_img_m = re.search(r'"certImage"\s*:\s*"([^"\\]+)', html)
    cert_number = None
    if cert_img_m:
        cn_m = re.search(r'/([A-Z0-9]+)\.pdf', cert_img_m.group(1))
        cert_number = cn_m.group(1) if cn_m else None

    return {
        "polish":       attrs.get("Polish"),
        "symmetry":     attrs.get("Symmetry"),
        "fluorescence": attrs.get("Fluorescence"),
        "cert_lab":     lab_m.group(1) if lab_m else None,
        "cert_number":  cert_number,
    }


# ---------------------------------------------------------------------------
# Public scrape function
# ---------------------------------------------------------------------------

_LIST_HEADERS = {
    "Accept": "text/html, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
}

def scrape(
    shapes: list[str],
    min_carat: float,
    max_carat: float,
    *,
    req_delay: float = 1.0,
    detail_cache: dict[str, dict] | None = None,
) -> list[Diamond]:
    """Scrape Clean Origin in two phases (listing then detail enrichment)."""

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            channel="chrome",
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        )
        context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

        # Warm up: full page load to establish cookies. Keep page open as
        # the fetch host for all subsequent page.evaluate() calls.
        page = context.new_page()
        page.goto(LIST_URL, wait_until="commit", timeout=60000)
        time.sleep(req_delay)

        # ------------------------------------------------------------------
        # Phase 1: collect listing data
        # ------------------------------------------------------------------
        listing_items: list[dict] = []
        seen_urls: set[str] = set()

        for raw_shape in shapes:
            co_shape = _SHAPE_MAP.get(raw_shape.lower())
            if co_shape is None:
                print(f"  [clean_origin] shape '{raw_shape}' not supported, skipping")
                continue

            weight_param = f"{min_carat:.2f}-{max_carat:.2f}"
            page_num = 1
            shape_seen_count = 0

            while True:
                url = _ajax_url(page_num, co_shape, weight_param)
                status, html = _page_fetch(page, url, _LIST_HEADERS)
                if status >= 400:
                    raise Exception(f"HTTP Error {status}")

                if page_num == 1:
                    total = _total_count(html)
                    print(
                        f"  [clean_origin] {co_shape} {weight_param}ct: "
                        f"{total or '?'} total listings"
                    )

                rows = _parse_listing_rows(html)
                if not rows:
                    break

                for r in rows:
                    url_item = r.get("url")
                    if not url_item or url_item in seen_urls:
                        continue
                    seen_urls.add(url_item)
                    shape_seen_count += 1

                    try:
                        carat = float(r["carat"])
                    except (TypeError, ValueError):
                        continue
                    if not (min_carat <= carat <= max_carat):
                        continue
                    if r.get("cut") not in ALLOWED_CUTS:
                        continue
                    if r.get("color") not in ALLOWED_COLORS:
                        continue
                    if r.get("clarity") not in ALLOWED_CLARITIES:
                        continue
                    try:
                        price = float(r["price"])
                    except (TypeError, ValueError):
                        continue

                    listing_items.append({
                        "shape": r.get("shape") or raw_shape,
                        "carat": carat,
                        "cut": r.get("cut"),
                        "color": r.get("color"),
                        "clarity": r.get("clarity"),
                        "price": price,
                        "url": url_item,
                    })

                total_count = _total_count(html)
                if total_count is not None and shape_seen_count >= total_count:
                    break
                if len(rows) < PAGE_SIZE:
                    break

                page_num += 1
                time.sleep(req_delay)

        print(f"  [clean_origin] listing phase complete: {len(listing_items)} qualifying stones")

        # ------------------------------------------------------------------
        # Phase 2: detail enrichment
        # ------------------------------------------------------------------
        cache = detail_cache or {}
        uncached = [item for item in listing_items if item["url"] not in cache]
        cached_count = len(listing_items) - len(uncached)
        print(
            f"  [clean_origin] detail pages: {cached_count} cached, "
            f"{len(uncached)} to fetch"
        )

        fetched_details: dict[str, dict] = {}
        for i, item in enumerate(uncached, 1):
            if i % 500 == 0:
                print(f"  [clean_origin] detail fetch progress: {i}/{len(uncached)}")
            fetched_details[item["url"]] = _fetch_detail(page, item["url"])
            time.sleep(req_delay)

        browser.close()

    # ------------------------------------------------------------------
    # Phase 3: build Diamond objects
    # ------------------------------------------------------------------
    all_diamonds: list[Diamond] = []
    for item in listing_items:
        url = item["url"]
        detail = cache.get(url) or fetched_details.get(url) or {}
        all_diamonds.append(
            Diamond.build(
                retailer=RETAILER,
                shape=item["shape"],
                carat=item["carat"],
                color=item["color"],
                clarity=item["clarity"],
                cut=item["cut"],
                polish=detail.get("polish"),
                symmetry=detail.get("symmetry"),
                fluorescence=detail.get("fluorescence"),
                certificate_lab=detail.get("cert_lab"),
                certificate_number=detail.get("cert_number"),
                price_usd=item["price"],
                product_url=url,
            )
        )

    print(f"  [clean_origin] collected {len(all_diamonds)} diamonds")
    return all_diamonds
