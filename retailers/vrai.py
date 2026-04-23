"""VRAI lab-grown diamond scraper — full inventory pull.

Calls VRAI's internal Next.js API route (/api/diamonds) directly.
VRAI's inventory is fully lab-grown (Diamond Foundry grown, "Cut for You"
model). All stocked rounds are Super Ideal cut; color grades available
include D through H (~20 stones/color, ~124 G-color stones total).

Oval support: VRAI does not carry oval lab-grown diamonds (confirmed:
`diamondType=oval-brilliant` returns 0 results).

Field coverage (from API response):
  shape, carat, cut ("Super Ideal" normalized to "Excellent"), color,
  clarity, polish, symmetry, price_usd, product_url.
  Missing: fluorescence, certificate_lab, certificate_number.
    -> VRAI uses internal Diamond Foundry grading (dfCertificateUrl).
    -> No GIA/IGI/GCAL third-party certification in their catalog.

Fetches each color separately because the API silently drops
colors beyond the first when multiple are requested as a comma-list.
"""

from __future__ import annotations

import time

from curl_cffi import requests as cr

from .base import Diamond

RETAILER = "VRAI"
SITE_ROOT = "https://www.vrai.com"
API_URL = f"{SITE_ROOT}/api/diamonds"
DETAIL_URL_FMT = f"{SITE_ROOT}/diamonds/{{lot_id}}"

HEADERS = {
    "Accept": "application/json",
    "Referer": f"{SITE_ROOT}/diamonds/lab-grown",
}

# VRAI offers D-H in their Cut for You line.
VRAI_COLORS = ["D", "E", "F", "G", "H"]

# VRAI only sells round brilliants. Map shape names to API diamondType values.
_DIAMOND_TYPE_MAP: dict[str, str] = {
    "round": "round-brilliant",
}


def _fetch_color(
    session: cr.Session,
    diamond_type: str,
    color: str,
    min_carat: float,
    max_carat: float,
    req_delay: float,
) -> list[dict]:
    items: list[dict] = []
    for page in range(1, 30):
        resp = session.get(
            API_URL,
            params={
                "diamondType": diamond_type,
                "color": color,
                "sortBy": "price",
                "sortOrder": "asc",
                "page": page,
            },
            headers=HEADERS,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        batch = [
            d for d in (data.get("items") or [])
            if d.get("availableForSale", True)
            and min_carat <= float(d.get("carat", 0)) <= max_carat
        ]
        items.extend(batch)
        paginator = data.get("paginator", {})
        total = paginator.get("itemCount", 0)
        if not data.get("items") or len(items) >= total:
            break
        time.sleep(req_delay)
    return items


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
    seen_lot_ids: set[str] = set()

    for raw_shape in shapes:
        diamond_type = _DIAMOND_TYPE_MAP.get(raw_shape.lower())
        if diamond_type is None:
            print(f"  [vrai] '{raw_shape}' not supported (VRAI only carries rounds), skipping")
            continue

        shape_count = 0
        for color in VRAI_COLORS:
            items = _fetch_color(session, diamond_type, color, min_carat, max_carat, req_delay)
            for d in items:
                lot_id = d.get("lotId") or d.get("_id") or ""
                if not lot_id or lot_id in seen_lot_ids:
                    continue
                try:
                    carat = float(d["carat"])
                except (KeyError, ValueError, TypeError):
                    continue
                price_cents = d.get("price")
                if price_cents is None:
                    continue
                seen_lot_ids.add(lot_id)
                shape_count += 1
                all_diamonds.append(
                    Diamond.build(
                        retailer=RETAILER,
                        shape=d.get("diamondType", raw_shape),
                        carat=carat,
                        color=d.get("color"),
                        clarity=d.get("clarity"),
                        cut=d.get("cut") or d.get("real_cut"),
                        polish=d.get("polish"),
                        symmetry=d.get("symmetry"),
                        fluorescence=None,
                        certificate_lab=None,
                        certificate_number=None,
                        price_usd=price_cents / 100,
                        product_url=DETAIL_URL_FMT.format(lot_id=lot_id),
                    )
                )
            time.sleep(req_delay)

        print(
            f"  [vrai] {raw_shape} {min_carat:.2f}-{max_carat:.2f}ct: "
            f"{shape_count} diamonds across {len(VRAI_COLORS)} colors"
        )

    print(f"  [vrai] collected {len(all_diamonds)} diamonds")
    return all_diamonds
