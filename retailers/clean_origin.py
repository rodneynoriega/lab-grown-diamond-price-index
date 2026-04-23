"""Clean Origin lab-grown diamond scraper — full inventory pull.

Two-phase scrape:
  Phase 1 — listing: paginate the Magento AJAX endpoint to collect
             shape/carat/cut/color/clarity/price for all stones.
  Phase 2 — detail enrichment: fetch one product detail page per stone
             to get polish, symmetry, fluorescence, certificate_lab,
             and certificate_number. Uses a url-keyed cache (populated
             by the orchestrator from prior-day raw CSVs) to skip stones
             whose detail data was already fetched on a previous run.

Field coverage after enrichment:
  shape, carat, cut, color, clarity, price_usd (listing)
  polish, symmetry, fluorescence, certificate_lab, certificate_number (detail page)
  Missing: none.
"""

from __future__ import annotations

import re
import time

from curl_cffi import requests as cr

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

_LIST_HEADERS = {
    "Accept": "text/html, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": LIST_URL,
}
_DETAIL_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,*/*",
    "Referer": LIST_URL,
}


# ---------------------------------------------------------------------------
# Listing page helpers
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

def _fetch_detail(session: cr.Session, url: str) -> dict:
    """Fetch a CO product detail page. Returns a dict with polish, symmetry,
    fluorescence, cert_lab, cert_number. Returns empty dict on failure."""
    try:
        resp = session.get(url, headers=_DETAIL_HEADERS, timeout=30)
        resp.raise_for_status()
        html = resp.text
    except Exception:
        return {}

    # <div class="attr-item"><div class="attr-label">X</div><div class="attr-value">Y</div></div>
    attr_pairs = re.findall(
        r'<div class="attr-label">([^<]+)</div>\s*<div class="attr-value">([^<]+)</div>',
        html, re.S,
    )
    attrs = {label.strip(): val.strip() for label, val in attr_pairs}

    # {"diamond_lab":"IGI"} and {"certImage":"https://labcerts.../LG123.pdf"}
    lab_m = re.search(r'"diamond_lab"\s*:\s*"([^"]+)"', html)
    cert_img_m = re.search(r'"certImage"\s*:\s*"([^"\\]+)', html)
    cert_number = None
    if cert_img_m:
        cn_m = re.search(r'/([A-Z0-9]+)\.pdf', cert_img_m.group(1))
        cert_number = cn_m.group(1) if cn_m else None

    return {
        "polish":     attrs.get("Polish"),
        "symmetry":   attrs.get("Symmetry"),
        "fluorescence": attrs.get("Fluorescence"),
        "cert_lab":   lab_m.group(1) if lab_m else None,
        "cert_number": cert_number,
    }


# ---------------------------------------------------------------------------
# Public scrape function
# ---------------------------------------------------------------------------

def scrape(
    shapes: list[str],
    min_carat: float,
    max_carat: float,
    *,
    req_delay: float = 1.0,
    detail_cache: dict[str, dict] | None = None,
) -> list[Diamond]:
    """Scrape Clean Origin in two phases (listing then detail enrichment).

    detail_cache: mapping of product_url -> detail dict from prior runs.
      Pass the result of the orchestrator's _load_co_detail_cache() call.
      Stones whose URL is already in the cache skip the detail page fetch.
    """
    session = cr.Session(impersonate="chrome")
    session.get(LIST_URL, timeout=30)
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
        page = 1
        shape_seen_count = 0  # per-shape count for total_count comparison

        while True:
            params = {
                "isAjax": "1",
                "diamond_shape[]": co_shape,
                "diamond_weight": weight_param,
                "product_list_order": "price",
                "p": page,
            }
            resp = session.get(LIST_URL, params=params, headers=_LIST_HEADERS, timeout=30)
            resp.raise_for_status()
            html = resp.text

            if page == 1:
                total = _total_count(html)
                print(
                    f"  [clean_origin] {co_shape} {weight_param}ct: "
                    f"{total or '?'} total listings"
                )

            rows = _parse_listing_rows(html)
            if not rows:
                break

            for r in rows:
                url = r.get("url")
                if not url or url in seen_urls:
                    continue
                seen_urls.add(url)
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
                    "url": url,
                })

            total_count = _total_count(html)
            if total_count is not None and shape_seen_count >= total_count:
                break
            if len(rows) < PAGE_SIZE:
                break

            page += 1
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
        fetched_details[item["url"]] = _fetch_detail(session, item["url"])
        time.sleep(req_delay)

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
