"""Blue Nile lab-grown diamond scraper — full inventory pull.

Calls Blue Nile's internal GraphQL product API
(POST /service-api/bn-product-api/diamond/v/2/).

Pagination constraint: BN's API hard-limits to 25 pages per query.
With page_size=100 that is 2,500 stones max per query. Colors D and E
have 8,000+ and 7,000+ stones in 0.90-2.50ct, exceeding that limit.
Strategy: query each color separately; for colors with > 2,500 stones,
shard by 0.01ct caret windows (each window has < 400 stones, well under
the 25-page limit).

Shape codes: Round=1, Oval=6 (confirmed via API).
Color codes: D=1, E=2, F=3, G=4, H=5, I=6, J=7, K=8.
Clarity codes: FL=1, IF=2, VVS1=3, VVS2=4, VS1=5, VS2=6, SI1=7, SI2=8.

Field coverage (from GraphQL response):
  shape, carat, cut, color, clarity, polish, symmetry, price_usd.
  certificate_lab: extracted from product title ("IGI 1.06 Carat...").
  Missing: fluorescence, certificate_number (not in GraphQL schema;
    cert_number not found in static detail HTML either).
"""

from __future__ import annotations

import time
from decimal import Decimal

from curl_cffi import requests as cr

from .base import Diamond

RETAILER = "Blue Nile"
SITE_ROOT = "https://www.bluenile.com"
API_URL = f"{SITE_ROOT}/service-api/bn-product-api/diamond/v/2/"
SEARCH_PAGE = f"{SITE_ROOT}/diamonds/lab-grown"

SHAPE_IDS: dict[str, int] = {
    "round":    1,
    "princess": 2,
    "radiant":  3,
    "emerald":  4,
    "marquise": 5,
    "oval":     6,
    "pear":     7,
    "cushion":  8,
    "asscher":  9,
    "heart":    10,
}

# Color codes D-K (BN lab inventory confirmed through K)
COLORS: list[tuple[int, str]] = [
    (1, "D"), (2, "E"), (3, "F"), (4, "G"),
    (5, "H"), (6, "I"), (7, "J"), (8, "K"),
]

PAGE_SIZE = 100
MAX_PAGE = 25                         # BN hard limit; page 26+ returns error
MAX_RESULTS_PER_QUERY = MAX_PAGE * PAGE_SIZE   # 2,500

GRAPHQL_QUERY = """query (
  $isLabDiamond: Boolean,
  $shapeID: [Int],
  $carat: floatRange,
  $color: intRange,
  $clarity: intRange,
  $page: pager,
  $sort: sortBy
) {
  searchByIDs(
    isLabDiamond: $isLabDiamond,
    shapeID: $shapeID,
    carat: $carat,
    color: $color,
    clarity: $clarity,
    page: $page,
    sort: $sort
  ) {
    total hits
    items {
      productID sku price title url
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


def _flatten_items(items_raw: list) -> list[dict]:
    flat: list[dict] = []
    for el in items_raw:
        if isinstance(el, list):
            flat.extend(el)
        else:
            flat.append(el)
    return flat


def _cert_lab_from_title(title: str | None) -> str | None:
    if not title:
        return None
    upper = title.upper()
    for lab in ("IGI", "GIA", "GCAL"):
        if lab in upper:
            return lab
    return None


def _query_page(
    session: cr.Session,
    shape_id: int,
    color_code: int,
    caret_from: float,
    caret_to: float,
    page_num: int,
) -> tuple[list[dict], int | None, bool]:
    """Return (items, hits_total, is_too_many_pages)."""
    variables = {
        "isLabDiamond": True,
        "shapeID": [shape_id],
        "carat": {"from": caret_from, "to": caret_to},
        "color": {"from": color_code, "to": color_code},
        "clarity": {"from": 1, "to": 9},
        "page": {"number": page_num, "size": PAGE_SIZE},
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
        if any("Too many pages" in str(e) for e in payload["errors"]):
            return [], None, True
        raise RuntimeError(f"Blue Nile GraphQL error: {payload['errors']}")
    data = payload["data"]["searchByIDs"]
    items = _flatten_items(data.get("items") or [])
    hits = data.get("hits") or data.get("total")
    return items, hits, False


def _paginate_range(
    session: cr.Session,
    shape_id: int,
    color_code: int,
    caret_from: float,
    caret_to: float,
    req_delay: float,
) -> list[dict]:
    """Paginate one (color, caret_range) combination. Assumes total <= 2,500."""
    all_items: list[dict] = []
    for page_num in range(1, MAX_PAGE + 1):
        items, hits, too_many = _query_page(
            session, shape_id, color_code, caret_from, caret_to, page_num
        )
        if too_many:
            break
        all_items.extend(items)
        if hits is not None and len(all_items) >= hits:
            break
        if len(items) < PAGE_SIZE:
            break
        time.sleep(req_delay)
    return all_items


def _caret_windows(min_caret: float, max_caret: float) -> list[tuple[float, float]]:
    """Generate 0.01ct windows from min_caret to max_caret inclusive."""
    windows: list[tuple[float, float]] = []
    c = Decimal(str(min_caret))
    end = Decimal(str(max_caret))
    step = Decimal("0.01")
    while c <= end:
        cf = float(c)
        windows.append((cf, cf))
        c += step
    return windows


def _build_diamond(item: dict, retailer: str) -> Diamond | None:
    stone = item.get("stone") or {}
    if not stone.get("isLabDiamond"):
        return None
    try:
        carat = float(stone["carat"])
    except (KeyError, ValueError, TypeError):
        return None
    price = item.get("price")
    if price is None:
        return None
    sku = str(item.get("sku") or item.get("productID") or "")
    url_path = item.get("url") or f"diamond-details/{sku}"
    product_url = (
        url_path if url_path.startswith("http")
        else f"{SITE_ROOT}/{url_path.lstrip('/')}"
    )
    cert_lab = _cert_lab_from_title(item.get("title") or "")
    return Diamond.build(
        retailer=retailer,
        shape=(stone.get("shape") or {}).get("name"),
        carat=carat,
        color=(stone.get("color") or {}).get("name"),
        clarity=(stone.get("clarity") or {}).get("name"),
        cut=(stone.get("cut") or {}).get("name"),
        polish=(stone.get("polish") or {}).get("name"),
        symmetry=(stone.get("symmetry") or {}).get("name"),
        fluorescence=None,
        certificate_lab=cert_lab,
        certificate_number=None,
        price_usd=float(price),
        product_url=product_url,
    )


def scrape(
    shapes: list[str],
    min_carat: float,
    max_carat: float,
    *,
    req_delay: float = 1.0,
) -> list[Diamond]:
    session = cr.Session(impersonate="chrome")
    session.get(SEARCH_PAGE, timeout=30)
    time.sleep(req_delay)

    all_diamonds: list[Diamond] = []
    seen_skus: set[str] = set()

    for raw_shape in shapes:
        shape_id = SHAPE_IDS.get(raw_shape.lower())
        if shape_id is None:
            print(f"  [blue_nile] shape '{raw_shape}' not in shape map, skipping")
            continue

        shape_diamonds = 0

        for color_code, color_name in COLORS:
            # Check total for this color across the full caret range.
            probe_items, probe_hits, too_many = _query_page(
                session, shape_id, color_code, min_carat, max_carat, 1
            )
            if too_many or probe_hits is None:
                probe_hits = MAX_RESULTS_PER_QUERY + 1  # force bucketing

            if probe_hits <= MAX_RESULTS_PER_QUERY:
                # Single range fits within the 25-page limit — paginate through all pages.
                # probe_items already has page 1; continue from page 2.
                raw_items = probe_items[:]
                if probe_hits > PAGE_SIZE:
                    for page_num in range(2, MAX_PAGE + 1):
                        items2, _, too_many2 = _query_page(
                            session, shape_id, color_code, min_carat, max_carat, page_num
                        )
                        if too_many2 or not items2:
                            break
                        raw_items.extend(items2)
                        if len(raw_items) >= probe_hits:
                            break
                        time.sleep(req_delay)
            else:
                # Shard by 0.01ct windows (max ~400 stones per window for D color).
                raw_items = []
                for c_from, c_to in _caret_windows(min_carat, max_carat):
                    window_items = _paginate_range(
                        session, shape_id, color_code, c_from, c_to, req_delay
                    )
                    raw_items.extend(window_items)
                    time.sleep(req_delay)

            # Build Diamond objects.
            for item in raw_items:
                sku = str(item.get("sku") or item.get("productID") or "")
                if not sku or sku in seen_skus:
                    continue
                d = _build_diamond(item, RETAILER)
                if d is None:
                    continue
                seen_skus.add(sku)
                all_diamonds.append(d)
                shape_diamonds += 1

            time.sleep(req_delay)

        print(
            f"  [blue_nile] {raw_shape} {min_carat:.2f}-{max_carat:.2f}ct: "
            f"{shape_diamonds} diamonds collected"
        )

    print(f"  [blue_nile] total collected: {len(all_diamonds)} diamonds")
    return all_diamonds
