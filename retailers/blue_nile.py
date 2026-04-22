"""Blue Nile lab-grown diamond scraper.

Calls Blue Nile's internal GraphQL product API
(POST /service-api/bn-product-api/diamond/v/2/).

Blue Nile uses integer codes for color, clarity, cut grades. Mappings below
discovered via API introspection. Query filters by lab-grown only
(isLabDiamond=True), round shape (shapeID=1), and the current carat bucket
plus F-G color, VS2-or-better clarity, and top-two cut tiers.
"""

from __future__ import annotations

from curl_cffi import requests as cr

from .base import Benchmark, Match

SITE_ROOT = "https://www.bluenile.com"
API_URL = f"{SITE_ROOT}/service-api/bn-product-api/diamond/v/2/"
SEARCH_PAGE = f"{SITE_ROOT}/diamonds/lab-grown"

# Integer codes used by Blue Nile's GraphQL schema.
# Color: D=1, E=2, F=3, G=4, H=5, I=6, J=7, K=8 (lower id = better).
# Clarity: FL=1, IF=2, VVS1=3, VVS2=4, VS1=5, VS2=6, SI1=7, SI2=8.
# Cut: Ideal=1, Excellent=2 (BN uses only these two top tiers for lab rounds).
# Shape: Round=1.
SHAPE_ROUND = 1
COLOR_F, COLOR_G = 3, 4
CLARITY_FL, CLARITY_VS2 = 1, 6     # VS2-or-better range
CUT_IDEAL, CUT_EXCELLENT = 1, 2    # top two cut tiers

ALLOWED_CUT_NAMES = ("Ideal", "Excellent")
ALLOWED_POLISH_SYMMETRY = ("EX", "Excellent")
ALLOWED_CLARITIES = ("FL", "IF", "VVS1", "VVS2", "VS1", "VS2")
ALLOWED_COLORS = ("F", "G")

GRAPHQL_QUERY = """query (
  $isLabDiamond: Boolean,
  $shapeID: [Int],
  $carat: floatRange,
  $color: intRange,
  $clarity: intRange,
  $cut: intRange,
  $page: pager,
  $sort: sortBy
) {
  searchByIDs(
    isLabDiamond: $isLabDiamond,
    shapeID: $shapeID,
    carat: $carat,
    color: $color,
    clarity: $clarity,
    cut: $cut,
    page: $page,
    sort: $sort
  ) {
    total
    hits
    items {
      productID
      sku
      price
      title
      url
      stone {
        carat
        shape { name }
        color { name }
        clarity { name }
        cut { name }
        polish { name }
        symmetry { name }
        isLabDiamond
      }
    }
  }
}"""

HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Referer": SEARCH_PAGE,
    "Origin": SITE_ROOT,
}


def _flatten_items(items_raw):
    """BN returns items as a list containing a single list of products."""
    flat = []
    for el in items_raw:
        if isinstance(el, list):
            flat.extend(el)
        else:
            flat.append(el)
    return flat


def scrape(bench: Benchmark) -> Match | None:
    session = cr.Session(impersonate="chrome")
    session.get(SEARCH_PAGE, timeout=30)  # warm up cookies / edge cache

    variables = {
        "isLabDiamond": True,
        "shapeID": [SHAPE_ROUND],
        "carat": {"from": bench.min_carat, "to": bench.max_carat},
        "color": {"from": COLOR_F, "to": COLOR_G},
        "clarity": {"from": CLARITY_FL, "to": CLARITY_VS2},
        "cut": {"from": CUT_IDEAL, "to": CUT_EXCELLENT},
        "page": {"number": 1, "size": 20},
        "sort": "PriceAsc",
    }

    resp = session.post(
        API_URL,
        json={"query": GRAPHQL_QUERY, "variables": variables},
        headers=HEADERS,
        timeout=30,
    )
    resp.raise_for_status()
    payload = resp.json()

    if payload.get("errors"):
        raise RuntimeError(f"Blue Nile GraphQL errors: {payload['errors']}")

    data = payload["data"]["searchByIDs"]
    items = _flatten_items(data.get("items") or [])

    for item in items:
        stone = item.get("stone") or {}
        if not stone.get("isLabDiamond"):
            continue
        if (stone.get("shape") or {}).get("name") != "round":
            continue
        if (stone.get("color") or {}).get("name") not in ALLOWED_COLORS:
            continue
        if (stone.get("clarity") or {}).get("name") not in ALLOWED_CLARITIES:
            continue
        if (stone.get("cut") or {}).get("name") not in ALLOWED_CUT_NAMES:
            continue
        if (stone.get("polish") or {}).get("name") not in ALLOWED_POLISH_SYMMETRY:
            continue
        if (stone.get("symmetry") or {}).get("name") not in ALLOWED_POLISH_SYMMETRY:
            continue
        carat = stone.get("carat")
        if carat is None or not (bench.min_carat <= float(carat) <= bench.max_carat):
            continue
        price = item.get("price")
        if price is None:
            continue

        url_path = item.get("url") or f"diamond-details/{item.get('sku')}"
        if url_path.startswith("http"):
            product_url = url_path
        else:
            product_url = f"{SITE_ROOT}/{url_path.lstrip('/')}"

        return Match(
            price_usd=float(price),
            url=product_url,
            actual_carat=float(carat),
            cut=stone["cut"]["name"],
            color=stone["color"]["name"],
            clarity=stone["clarity"]["name"],
            total_matches=data.get("hits"),
        )

    return None
