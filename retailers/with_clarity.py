"""With Clarity lab-grown diamond scraper — full inventory pull.

Calls With Clarity's internal Shopify diamond-listing API directly.
The API lives on a separate server (vportalwithclarity.com) with a
self-signed / incomplete certificate chain, so SSL verification is
intentionally disabled. Prices returned are the displayed retail prices
(total_discounted_sales_price includes any active discounts).

Field coverage (from API response):
  shape, carat, cut, color, clarity, polish, symmetry,
  fluorescence (field: fluor_intensity), certificate_lab (field: lab),
  certificate_number (field: cert_num), price_usd.
"""

from __future__ import annotations

import time

from curl_cffi import requests as cr

from .base import Diamond

RETAILER = "With Clarity"
SITE_ROOT = "https://www.withclarity.com"
API_URL = "https://vportalwithclarity.com/fetchdirectdiamond/"
DETAIL_URL_FMT = f"{SITE_ROOT}/products/diamonds?sku={{sku}}"

PAGE_SIZE = 20   # WC API returns 20 stones per page

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
    "Content-Type": "application/json",
    "Referer": f"{SITE_ROOT}/collections/lab-diamonds",
    "Origin": SITE_ROOT,
}


def _build_filter(shape_str: str, min_carat: float, max_carat: float, page: int) -> list:
    # Filter array shape is positional. Integer codes are opaque; passing all
    # codes for cut/color/clarity fetches the full inventory. We filter locally
    # on string grade fields to normalize via Diamond.build().
    return [
        {"shapes": [shape_str]},
        {"cuts": [0, 1, 2, 3, 4, 5]},
        {"colors": [1, 2, 3, 4, 5, 6, 7, 8, 9]},
        {"claritys": [1, 2, 3, 4, 5, 6, 7, 8, 9]},
        {"labs": []},
        {"polish": [0, 1, 2, 3]},
        {"symmetrys": [0, 1, 2, 3]},
        {"price": "100,700000"},
        {"carat": f"{min_carat:.2f},{max_carat:.2f}"},
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


def scrape(
    shapes: list[str],
    min_carat: float,
    max_carat: float,
    *,
    req_delay: float = 1.0,
) -> list[Diamond]:
    session = cr.Session(impersonate="chrome")
    session.get(SITE_ROOT + "/", timeout=30)
    time.sleep(req_delay)

    all_diamonds: list[Diamond] = []
    seen_ids: set[str] = set()

    for raw_shape in shapes:
        wc_shape = _SHAPE_MAP.get(raw_shape.lower())
        if wc_shape is None:
            print(f"  [with_clarity] shape '{raw_shape}' not in shape map, skipping")
            continue

        page = 1
        shape_total: int | None = None

        while True:
            resp = session.post(
                API_URL,
                json={"filter": _build_filter(wc_shape, min_carat, max_carat, page)},
                headers=HEADERS,
                timeout=30,
                verify=False,
            )
            resp.raise_for_status()
            payload = resp.json()

            ld = payload["data"]["liveDiamondData"]
            diamonds_raw = ld.get("diamond") or []
            total = ld.get("dataCount", 0)

            if not diamonds_raw:
                break

            if page == 1:
                shape_total = total
                print(
                    f"  [with_clarity] {wc_shape} {min_carat:.2f}-{max_carat:.2f}ct: "
                    f"{shape_total or '?'} total diamonds"
                )

            for d in diamonds_raw:
                did = str(d.get("diamond_id") or d.get("cert_num") or "")
                if not did or did in seen_ids:
                    continue
                try:
                    carat = float(d["size"])
                except (KeyError, ValueError, TypeError):
                    continue
                price = d.get("total_discounted_sales_price")
                if price is None:
                    continue
                sku = d.get("cert_num") or did
                seen_ids.add(did)
                all_diamonds.append(
                    Diamond.build(
                        retailer=RETAILER,
                        shape=d.get("shape"),
                        carat=carat,
                        color=d.get("color"),
                        clarity=d.get("clarity"),
                        cut=d.get("cut"),
                        polish=d.get("polish"),
                        symmetry=d.get("symmetry"),
                        fluorescence=d.get("fluor_intensity"),
                        certificate_lab=d.get("lab"),
                        certificate_number=str(sku) if sku else None,
                        price_usd=float(price),
                        product_url=DETAIL_URL_FMT.format(sku=sku),
                    )
                )

            fetched_so_far = (page - 1) * PAGE_SIZE + len(diamonds_raw)
            if shape_total is not None and fetched_so_far >= shape_total:
                break
            if len(diamonds_raw) < PAGE_SIZE:
                break

            page += 1
            time.sleep(req_delay)

    print(f"  [with_clarity] collected {len(all_diamonds)} diamonds")
    return all_diamonds
