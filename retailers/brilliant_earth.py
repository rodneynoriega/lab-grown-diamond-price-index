"""Brilliant Earth lab-grown diamond scraper — full inventory pull.

Uses BE's internal product-listing API (same endpoint as their PLP).
Must be run from a US IP. Uses curl_cffi to pass Cloudflare's TLS check.

Cloudflare notes:
  BE's API blocks broad pagination (full caret range, page 2+). Requesting
  >50 results in one query triggers CF bot detection. Fix: shard by color
  AND 0.10ct caret windows, with a fresh curl_cffi session per color. Each
  shard returns ≤ 300 stones (6 pages), which looks like normal browsing.

Field coverage (from API response):
  shape, carat, cut, color, clarity, polish, symmetry, fluorescence,
  certificate_lab (field: "report"), certificate_number, price_usd.

Valid grades for lab diamonds at BE:
  Colors: D-J (K is not a valid choice → returns error + 0 results)
  Clarities: FL-SI2 (I1 is not a valid choice → same)
"""

from __future__ import annotations

import time
from decimal import Decimal

from curl_cffi import requests as cr

from .base import Diamond

RETAILER = "Brilliant Earth"
SITE_ROOT = "https://www.brilliantearth.com"
API_URL = f"{SITE_ROOT}/api/v1/plp/products/"
DETAIL_URL_FMT = f"{SITE_ROOT}/lab-diamonds-search/view_detail/{{id}}/"

PAGE_SIZE = 50   # BE silently caps display at 50; using 100 causes early loop exit

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

HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Referer": f"{SITE_ROOT}/lab-grown-diamonds/",
    "X-Requested-With": "XMLHttpRequest",
}

# D-J, FL-SI2: full valid lab diamond grade ranges at BE.
ALL_COLORS = ["D", "E", "F", "G", "H", "I", "J"]
ALL_CLARITIES = "FL,IF,VVS1,VVS2,VS1,VS2,SI1,SI2"

# 0.10ct caret windows — each shard has ≤ 300 stones (6 pages max).
def _caret_windows(min_caret: float, max_caret: float) -> list[tuple[float, float]]:
    windows: list[tuple[float, float]] = []
    lo = Decimal(str(min_caret))
    step = Decimal("0.10")
    while lo <= Decimal(str(max_caret)):
        hi = min(lo + step - Decimal("0.01"), Decimal(str(max_caret)))
        windows.append((float(lo), float(hi)))
        lo += step
    return windows


def _scrape_color_window(
    session: cr.Session,
    be_shape: str,
    color: str,
    c_from: float,
    c_to: float,
    req_delay: float,
) -> list[dict]:
    """Fetch all pages for one (shape, color, caret_window) shard."""
    all_products: list[dict] = []
    for page in range(1, 500):
        params = {
            "currency": "USD",
            "product_class": "Lab Created Colorless Diamonds",
            "shapes": be_shape,
            "colors": color,
            "clarities": ALL_CLARITIES,
            "min_carat": f"{c_from:.2f}",
            "max_carat": f"{c_to:.2f}",
            "order_by": "price",
            "order_method": "asc",
            "display": PAGE_SIZE,
            "page": page,
        }
        try:
            resp = session.get(
                API_URL, params=params, headers=HEADERS, timeout=(15, 30)
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"  [brilliant_earth] request failed ({color} {c_from:.2f}-{c_to:.2f}ct p{page}): {e}")
            break

        msg = data.get("message")
        if msg:
            print(f"  [brilliant_earth] API message ({color} {c_from:.2f}): {msg}")
            break

        products = data.get("products") or []
        if not products:
            break

        if page == 1:
            total = data.get("total", "?")
            if total and int(total) > 0:
                pass  # normal, don't print every window

        all_products.extend(products)
        total = data.get("total")
        if total is not None and len(all_products) >= int(total):
            break
        if len(products) < PAGE_SIZE:
            break

        time.sleep(req_delay)

    return all_products


def scrape(
    shapes: list[str],
    min_carat: float,
    max_carat: float,
    *,
    req_delay: float = 1.0,
) -> list[Diamond]:
    all_diamonds: list[Diamond] = []
    seen_ids: set[str] = set()
    windows = _caret_windows(min_carat, max_carat)

    for raw_shape in shapes:
        be_shape = _SHAPE_MAP.get(raw_shape.lower())
        if be_shape is None:
            print(f"  [brilliant_earth] shape '{raw_shape}' not in shape map, skipping")
            continue

        shape_count = 0

        for color in ALL_COLORS:
            # Fresh session per color to avoid Cloudflare session tracking.
            session = cr.Session(impersonate="chrome")
            try:
                session.get(SITE_ROOT + "/", timeout=(10, 20))
            except Exception:
                pass  # warmup failure is non-fatal
            time.sleep(req_delay)

            color_count = 0
            for c_from, c_to in windows:
                products = _scrape_color_window(
                    session, be_shape, color, c_from, c_to, req_delay
                )
                for p in products:
                    pid = str(p.get("id", ""))
                    if not pid or pid in seen_ids:
                        continue
                    if p.get("origin") != "Lab Created":
                        continue
                    try:
                        carat = float(p["carat"])
                    except (KeyError, ValueError, TypeError):
                        continue
                    price = p.get("price")
                    if price is None:
                        continue
                    seen_ids.add(pid)
                    color_count += 1
                    all_diamonds.append(
                        Diamond.build(
                            retailer=RETAILER,
                            shape=p.get("shape"),
                            carat=carat,
                            color=p.get("color"),
                            clarity=p.get("clarity"),
                            cut=p.get("cut"),
                            polish=p.get("polish"),
                            symmetry=p.get("symmetry"),
                            fluorescence=p.get("fluorescence"),
                            certificate_lab=p.get("report"),
                            certificate_number=str(p["certificate_number"])
                                if p.get("certificate_number") else None,
                            price_usd=float(price),
                            product_url=DETAIL_URL_FMT.format(id=pid),
                        )
                    )
                time.sleep(req_delay)

            shape_count += color_count
            print(f"  [brilliant_earth] {be_shape} {color}: {color_count} diamonds")
            time.sleep(req_delay)

        print(f"  [brilliant_earth] {be_shape} total: {shape_count} diamonds")

    print(f"  [brilliant_earth] collected {len(all_diamonds)} diamonds")
    return all_diamonds
