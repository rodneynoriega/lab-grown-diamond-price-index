"""James Allen lab-grown diamond scraper — full inventory pull.

Uses JA's internal GraphQL product API (POST /service-api/ja-product-api/diamond/v/2/).
Must be run from a US IP.

Pagination quirk: the API returns a "sliding window" of 10 inner pages per call.
With size=100, each call returns 10×100=1000 items. To advance non-overlapping
windows, step the `number` param by 10 (not 1).

Shape IDs: Round=1, Oval=6 (same as Blue Nile).

Field coverage:
  shape, carat, color, clarity, cut, polish, symmetry, fluorescence (flour),
  certificate_lab (lab.name), price_usd.
  Missing: certificate_number (certNumber is null on the listing API for most stones).
"""

from __future__ import annotations

import time

from curl_cffi import requests as cr

from .base import Diamond

RETAILER = "James Allen"
SITE_ROOT = "https://www.jamesallen.com"
API_URL = f"{SITE_ROOT}/service-api/ja-product-api/diamond/v/2/"
SEARCH_PAGE = f"{SITE_ROOT}/loose-diamonds/all-diamonds/lab-grown-diamond-search/"

INNER_PAGES_PER_CALL = 10   # API always returns 10 inner pages per call
PAGE_STEP = INNER_PAGES_PER_CALL  # step outer `number` by this to avoid overlap
PAGE_SIZE = 100             # items per inner page → 10×100=1000 per call

SHAPE_IDS: dict[str, int] = {
    "round": 1,
    "oval": 6,
    "pear": 7,
    "cushion": 8,
    "princess": 2,
    "emerald": 4,
    "radiant": 3,
    "marquise": 5,
    "asscher": 9,
    "heart": 10,
}

# cut name abbreviations from JA (stone.cut.name values)
_CUT_MAP: dict[str, str] = {
    "ideal": "Excellent",
    "excellent": "Excellent",
    "very good": "Very Good",
    "good": "Good",
    "fair": "Fair",
    "poor": "Poor",
}

# polish/symmetry abbreviations
_GRADE_MAP: dict[str, str] = {
    "ex": "Excellent",
    "excellent": "Excellent",
    "vg": "Very Good",
    "very good": "Very Good",
    "g": "Good",
    "good": "Good",
    "f": "Fair",
    "fair": "Fair",
    "p": "Poor",
    "poor": "Poor",
}

# fluorescence (flour.name) abbreviations
_FLOUR_MAP: dict[str, str] = {
    "nn": "None",
    "none": "None",
    "n": "None",
    "sl": "Faint",
    "faint": "Faint",
    "f": "Faint",
    "m": "Medium",
    "med": "Medium",
    "medium": "Medium",
    "s": "Strong",
    "st": "Strong",
    "strong": "Strong",
    "vs": "Very Strong",
    "vst": "Very Strong",
    "very strong": "Very Strong",
}

GRAPHQL_QUERY = """
query(
  $carat: floatRange,
  $color: intRange,
  $shapeID: [Int],
  $isLabDiamond: Boolean,
  $page: pager,
  $sort: sortBy
) {
  searchByIDs(
    carat: $carat,
    color: $color,
    shapeID: $shapeID,
    isLabDiamond: $isLabDiamond,
    page: $page,
    sort: $sort
  ) {
    hits
    items {
      productID
      price
      url
      stone {
        carat
        certNumber
        isLabDiamond
        shape  { id name }
        color  { name }
        clarity { name }
        cut    { name }
        polish { name }
        symmetry { name }
        flour  { name }
        lab    { name }
      }
    }
  }
}"""

HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Origin": SITE_ROOT,
    "Referer": SEARCH_PAGE,
}


def _flatten_items(raw: list) -> list[dict]:
    flat: list[dict] = []
    for el in raw:
        if isinstance(el, list):
            flat.extend(el)
        else:
            flat.append(el)
    return flat


def _map_cut(raw: str | None) -> str | None:
    if raw is None:
        return None
    return _CUT_MAP.get(raw.strip().lower(), raw.strip())


def _map_grade(raw: str | None) -> str | None:
    if raw is None:
        return None
    return _GRADE_MAP.get(raw.strip().lower(), raw.strip())


def _map_flour(raw: str | None) -> str | None:
    if raw is None:
        return None
    return _FLOUR_MAP.get(raw.strip().lower(), raw.strip())


def _query_page(
    session: cr.Session,
    shape_id: int,
    min_carat: float,
    max_carat: float,
    page_number: int,
    color_code: int | None = None,
) -> tuple[list[dict], int | None, bool]:
    """Return (flat_items, hits_total, is_too_many_pages)."""
    variables: dict = {
        "isLabDiamond": True,
        "shapeID": [shape_id],
        "carat": {"from": min_carat, "to": max_carat},
        "page": {"number": page_number, "size": PAGE_SIZE},
        "sort": "PriceAsc",
    }
    if color_code is not None:
        variables["color"] = {"from": color_code, "to": color_code}
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
        raise RuntimeError(f"James Allen GraphQL error: {payload['errors']}")
    data = payload["data"]["searchByIDs"]
    flat = _flatten_items(data.get("items") or [])
    hits = data.get("hits")
    return flat, hits, False


def _build_diamond(item: dict) -> Diamond | None:
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
    pid = str(item.get("productID") or "")
    url_path = item.get("url") or ""
    product_url = (
        url_path if url_path.startswith("http")
        else f"{SITE_ROOT}/{url_path.lstrip('/')}"
    )
    cert_lab_raw = (stone.get("lab") or {}).get("name")
    return Diamond.build(
        retailer=RETAILER,
        shape=(stone.get("shape") or {}).get("name"),
        carat=carat,
        color=(stone.get("color") or {}).get("name"),
        clarity=(stone.get("clarity") or {}).get("name"),
        cut=_map_cut((stone.get("cut") or {}).get("name")),
        polish=_map_grade((stone.get("polish") or {}).get("name")),
        symmetry=_map_grade((stone.get("symmetry") or {}).get("name")),
        fluorescence=_map_flour((stone.get("flour") or {}).get("name")),
        certificate_lab=cert_lab_raw,
        certificate_number=stone.get("certNumber") or None,
        price_usd=float(price),
        product_url=product_url,
    )


# Color codes D-K (JA uses same IDs as Blue Nile)
COLORS: list[tuple[int, str]] = [
    (1, "D"), (2, "E"), (3, "F"), (4, "G"),
    (5, "H"), (6, "I"), (7, "J"), (8, "K"),
]

# Max items retrievable per color shard: 3 effective page calls × 1000 items/call = 3000
# (page numbers 1, 11, 21 → n=31 errors with "Too many pages")
_MAX_PAGE_NUMBER = 30
MAX_ITEMS_PER_SHARD = (_MAX_PAGE_NUMBER // PAGE_STEP) * PAGE_SIZE * INNER_PAGES_PER_CALL


def _carat_windows(min_c: float, max_c: float, step: float = 0.10) -> list[tuple[float, float]]:
    """0.10ct windows from min to max."""
    from decimal import Decimal
    windows: list[tuple[float, float]] = []
    lo = Decimal(str(min_c))
    s = Decimal(str(step))
    while lo <= Decimal(str(max_c)):
        hi = min(lo + s - Decimal("0.01"), Decimal(str(max_c)))
        windows.append((float(lo), float(hi)))
        lo += s
    return windows


def _paginate_shard(
    session: cr.Session,
    shape_id: int,
    c_from: float,
    c_to: float,
    color_code: int,
    req_delay: float,
    seen_pids: set[str],
) -> list[Diamond]:
    """Paginate one (shape, color, carat_range) shard. Returns new Diamond objects."""
    results: list[Diamond] = []
    page_num = 1
    hits_total: int | None = None

    while True:
        try:
            items, hits, too_many = _query_page(
                session, shape_id, c_from, c_to, page_num, color_code
            )
        except Exception as e:
            print(f"  [james_allen] request failed (n={page_num}): {e}")
            break

        if too_many:
            break

        if hits_total is None and hits is not None:
            hits_total = hits

        if not items:
            break

        new_count = 0
        for item in items:
            pid = str(item.get("productID") or "")
            if not pid or pid in seen_pids:
                continue
            d = _build_diamond(item)
            if d is None:
                continue
            seen_pids.add(pid)
            results.append(d)
            new_count += 1

        if new_count == 0:
            break
        if hits_total is not None and len(results) >= hits_total:
            break

        page_num += PAGE_STEP
        time.sleep(req_delay)

    return results


def scrape(
    shapes: list[str],
    min_carat: float,
    max_carat: float,
    *,
    req_delay: float = 1.0,
) -> list[Diamond]:
    session = cr.Session(impersonate="chrome")
    try:
        session.get(SEARCH_PAGE, timeout=20)
    except Exception:
        pass
    time.sleep(req_delay)

    all_diamonds: list[Diamond] = []
    seen_pids: set[str] = set()

    for raw_shape in shapes:
        shape_id = SHAPE_IDS.get(raw_shape.lower())
        if shape_id is None:
            print(f"  [james_allen] shape '{raw_shape}' not in shape map, skipping")
            continue

        shape_count_before = len(all_diamonds)

        for color_code, color_name in COLORS:
            # Probe first page to get hit count
            try:
                probe_items, probe_hits, too_many = _query_page(
                    session, shape_id, min_carat, max_carat, 1, color_code
                )
            except Exception as e:
                print(f"  [james_allen] probe failed ({raw_shape} {color_name}): {e}")
                time.sleep(req_delay)
                continue

            if too_many or probe_hits is None:
                probe_hits = MAX_ITEMS_PER_SHARD + 1

            if probe_hits <= MAX_ITEMS_PER_SHARD:
                # Fits in one shard — paginate directly
                # First page already fetched; collect it, then continue
                for item in probe_items:
                    pid = str(item.get("productID") or "")
                    if not pid or pid in seen_pids:
                        continue
                    d = _build_diamond(item)
                    if d is None:
                        continue
                    seen_pids.add(pid)
                    all_diamonds.append(d)

                if probe_hits > len(probe_items):
                    page_num = 1 + PAGE_STEP
                    while True:
                        try:
                            items2, _, too_many2 = _query_page(
                                session, shape_id, min_carat, max_carat, page_num, color_code
                            )
                        except Exception:
                            break
                        if too_many2 or not items2:
                            break
                        new = 0
                        for item in items2:
                            pid = str(item.get("productID") or "")
                            if pid and pid not in seen_pids:
                                d = _build_diamond(item)
                                if d:
                                    seen_pids.add(pid)
                                    all_diamonds.append(d)
                                    new += 1
                        if new == 0:
                            break
                        page_num += PAGE_STEP
                        time.sleep(req_delay)
            else:
                # Too many items for one shard — sub-shard by 0.10ct windows
                for c_from, c_to in _carat_windows(min_carat, max_carat):
                    new_diamonds = _paginate_shard(
                        session, shape_id, c_from, c_to, color_code, req_delay, seen_pids
                    )
                    all_diamonds.extend(new_diamonds)
                    time.sleep(req_delay)

            time.sleep(req_delay)

        shape_count = len(all_diamonds) - shape_count_before
        print(f"  [james_allen] {raw_shape} collected: {shape_count} diamonds")

    print(f"  [james_allen] total collected: {len(all_diamonds)} diamonds")
    return all_diamonds
