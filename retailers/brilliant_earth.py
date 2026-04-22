"""Brilliant Earth lab-grown diamond scraper.

Calls BE's internal product-listing API directly. Must be run from a US IP
(GitHub-hosted runners satisfy this). Uses curl_cffi to impersonate Chrome's
TLS fingerprint so Cloudflare returns real responses.
"""

from __future__ import annotations

from curl_cffi import requests as cr

from .base import Benchmark, Match

RETAILER = "Brilliant Earth"
SITE_ROOT = "https://www.brilliantearth.com"
API_URL = f"{SITE_ROOT}/api/v1/plp/products/"
DETAIL_URL_FMT = f"{SITE_ROOT}/lab-diamonds-search/view_detail/{{id}}/"

# "Excellent cut" for round lab diamonds at BE = top two cut tiers.
ALLOWED_CUTS = ("Super Ideal", "Ideal")

# Clarity uses "VS2 or better" as the floor (index-wide rule so retailer cells
# are always filled; VS2 is still almost always the cheapest grade that comes
# back because cleaner stones cost more).
COMMON_PARAMS = {
    "currency": "USD",
    "product_class": "Lab Created Colorless Diamonds",
    "shapes": "Round",
    "cuts": "Super Ideal,Ideal",
    "colors": "F,G",
    "clarities": "FL,IF,VVS1,VVS2,VS1,VS2",
    "polishes": "Excellent",
    "symmetries": "Excellent",
    "order_by": "price",
    "order_method": "asc",
    "display": 10,
    "page": 1,
}

ALLOWED_CLARITIES = ("FL", "IF", "VVS1", "VVS2", "VS1", "VS2")

HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Referer": f"{SITE_ROOT}/lab-grown-diamonds/",
    "X-Requested-With": "XMLHttpRequest",
}


def scrape(bench: Benchmark) -> Match | None:
    session = cr.Session(impersonate="chrome")
    session.get(SITE_ROOT + "/", timeout=30)  # warm up CF cookies

    params = {
        **COMMON_PARAMS,
        "min_carat": f"{bench.min_carat:.2f}",
        "max_carat": f"{bench.max_carat:.2f}",
    }
    resp = session.get(API_URL, params=params, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    products = data.get("products") or []
    for product in products:
        if product.get("origin") != "Lab Created":
            continue
        if product.get("shape") != "Round":
            continue
        if product.get("cut") not in ALLOWED_CUTS:
            continue
        if product.get("color") not in ("F", "G"):
            continue
        if product.get("clarity") not in ALLOWED_CLARITIES:
            continue
        if product.get("polish") != "Excellent" or product.get("symmetry") != "Excellent":
            continue
        carat = product.get("carat")
        if carat is None or not (bench.min_carat <= float(carat) <= bench.max_carat):
            continue
        price = product.get("price")
        if price is None:
            continue
        return Match(
            price_usd=float(price),
            url=DETAIL_URL_FMT.format(id=product["id"]),
            actual_carat=float(carat),
            cut=product.get("cut"),
            color=product.get("color"),
            clarity=product.get("clarity"),
            total_matches=data.get("total"),
        )

    return None
