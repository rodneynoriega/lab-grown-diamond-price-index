"""With Clarity lab-grown diamond scraper.

Calls With Clarity's internal Shopify diamond-listing API directly.
The API lives on a separate server (vportalwithclarity.com) with a
self-signed / incomplete certificate chain, so SSL verification is
intentionally disabled.  Prices returned are the displayed retail
prices (total_discounted_sales_price includes any active discounts).
"""

from __future__ import annotations

from curl_cffi import requests as cr

from .base import Benchmark, Match

SITE_ROOT = "https://www.withclarity.com"
API_URL = "https://vportalwithclarity.com/fetchdirectdiamond/"
DETAIL_URL_FMT = f"{SITE_ROOT}/products/diamonds?sku={{sku}}"

ALLOWED_CUTS = {"Excellent", "Ideal"}
ALLOWED_CLARITIES = {"FL", "IF", "VVS1", "VVS2", "VS1", "VS2"}

HEADERS = {
    "Content-Type": "application/json",
    "Referer": f"{SITE_ROOT}/collections/lab-diamonds",
    "Origin": SITE_ROOT,
}


def _build_filter(bench: Benchmark, page: int = 1) -> list:
    # Filter array shape is positional and mirrors the JS filterArray in diamonds.js.
    # Integer codes for cut/color/clarity are opaque; we request all values and
    # filter locally on the string grade fields in the response.
    return [
        {"shapes": ["Round"]},
        {"cuts": [0, 1, 2, 3]},
        {"colors": [1, 2, 3, 4, 5, 6, 7, 8, 9]},
        {"claritys": [1, 2, 3, 4, 5, 6, 7, 8, 9]},
        {"labs": []},
        {"polish": [0, 1, 2, 3]},
        {"symmetrys": [0, 1, 2, 3]},
        {"price": "100,700000"},
        {"carat": f"{bench.min_carat},{bench.max_carat}"},
        {"page": page},
        {"orderBy": "Price"},
        {"sortBy": "ASC"},
        {"lwratio": "0.9,2.75"},
        {"fluorescences": [0, 1, 2, 3]},
        {"sku": ""},
        {"table": "40,90"},
        {"depth": "40,90"},
        {"type": ""},
        {"diamond_type": "lab"},
        {"cert_num": ""},
        {"quick_ship_diamonds": "N"},
        {"Appointment": ""},
        {"VaultDiscount": "No"},
        {"reports": ""},
        {"country": "US"},
        {"color_intensity": []},
    ]


def scrape(bench: Benchmark) -> Match | None:
    session = cr.Session(impersonate="chrome")
    session.get(SITE_ROOT + "/", timeout=30)  # warm up CF cookies

    for page in range(1, 6):  # cap at 5 pages (100 stones) before giving up
        resp = session.post(
            API_URL,
            json={"filter": _build_filter(bench, page)},
            headers=HEADERS,
            timeout=30,
            verify=False,
        )
        resp.raise_for_status()
        payload = resp.json()
        diamonds = payload["data"]["liveDiamondData"]["diamond"]
        total = payload["data"]["liveDiamondData"]["dataCount"]

        if not diamonds:
            break

        for d in diamonds:
            carat = float(d["size"])
            if not (bench.min_carat <= carat <= bench.max_carat):
                continue
            if d.get("cut") not in ALLOWED_CUTS:
                continue
            if d.get("color") not in ("F", "G"):
                continue
            if d.get("clarity") not in ALLOWED_CLARITIES:
                continue
            if d.get("polish") != "Excellent":
                continue
            if d.get("symmetry") != "Excellent":
                continue
            price = d.get("total_discounted_sales_price")
            if price is None:
                continue
            return Match(
                price_usd=float(price),
                url=DETAIL_URL_FMT.format(sku=d["cert_num"]),
                actual_carat=carat,
                cut=d["cut"],
                color=d["color"],
                clarity=d["clarity"],
                total_matches=total,
            )

    return None
