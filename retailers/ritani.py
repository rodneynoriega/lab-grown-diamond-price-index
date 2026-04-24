"""Ritani lab-grown diamond scraper — full inventory pull.

Uses the newecx.com REST search API that backs Ritani's Next.js storefront.

API endpoint: GET https://api-server.newecx.com/api/search/{handle}
API key embedded in the JS app bundle (same-origin public key).

Filter format: shape=RD&color=D&carat>=0.9&carat<=0.99 appended to the URL.
The >= and <= operators are literal (not URL-encoded). color= is a single letter.

Hard limit: 5 pages max per request (page 6+ returns HTTP 400).
Sharding strategy:
  1. Shard by color (D-K).
  2. For each color shard, if total_pages > 5 (>500 items), further shard by
     0.01ct carat windows. Each 0.01ct window should fit within 5 pages.
  3. Deduplicate by handle across all shards.

Field coverage: all 16 CSV fields available in the item response.
"""

from __future__ import annotations

import time
from decimal import Decimal

from curl_cffi import requests as cr

from .base import Diamond

RETAILER = "Ritani"
SITE_ROOT = "https://www.ritani.com"
API_BASE = "https://api-server.newecx.com/api"
COLLECTION_HANDLE = "lab-grown-diamonds"
API_KEY = "pREC5nOg4c1kiJ0u5wybbqX0fVL:nkTXhhcihOd4Xq9NJyhqZ5K7amJ"

PAGE_SIZE = 100
MAX_PAGE = 5   # Hard API limit — page 6+ returns HTTP 400

COLORS = ["D", "E", "F", "G", "H", "I", "J", "K"]

_SHAPE_CODES: dict[str, str] = {
    "round": "RD",
    "oval": "OV",
    "pear": "PS",
    "cushion": "CU",
    "princess": "PR",
    "emerald": "EM",
    "radiant": "RA",
    "marquise": "MQ",
    "asscher": "AS",
    "heart": "HS",
}

HEADERS = {
    "Accept": "application/json",
    "Origin": SITE_ROOT,
    "Referer": f"{SITE_ROOT}/collections/{COLLECTION_HANDLE}",
}


def _carat_windows(min_c: float, max_c: float, step: float = 0.01) -> list[tuple[float, float]]:
    windows: list[tuple[float, float]] = []
    lo = Decimal(str(min_c))
    s = Decimal(str(step))
    while lo <= Decimal(str(max_c)):
        hi = lo + s - Decimal("0.001")
        windows.append((float(lo), float(min(hi, Decimal(str(max_c))))))
        lo += s
    return windows


def _get_page(
    session: cr.Session,
    shape_code: str,
    color: str | None,
    min_carat: float,
    max_carat: float,
    page_no: int,
) -> dict | None:
    """Return raw API response dict, or None on unrecoverable error."""
    filter_flag = 1 if page_no == 1 else 0
    # Build filter query string with literal >= and <=
    filter_parts = [f"shape={shape_code}"]
    if color:
        filter_parts.append(f"color={color}")
    filter_parts += [f"carat>={min_carat:.2f}", f"carat<={max_carat:.2f}"]
    filter_qs = "&".join(filter_parts)

    url = (
        f"{API_BASE}/search/{COLLECTION_HANDLE}"
        f"?api_key={API_KEY}"
        f"&page_size={PAGE_SIZE}"
        f"&page_no={page_no}"
        f"&filter={filter_flag}"
        f"&{filter_qs}"
    )
    try:
        resp = session.get(url, headers=HEADERS, timeout=30)
        if resp.status_code == 400:
            return None   # Page limit hit
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        raise


def _build_diamond(item: dict) -> Diamond | None:
    try:
        carat = float(item["carat"])
    except (KeyError, ValueError, TypeError):
        return None
    price = item.get("price")
    if price is None:
        return None
    handle = item.get("handle") or item.get("variant_handle") or ""
    product_url = f"{SITE_ROOT}/products/{handle}" if handle else ""

    shape_code = (item.get("shape") or "").upper()
    shape_name_map = {v: k for k, v in _SHAPE_CODES.items()}
    shape = shape_name_map.get(shape_code, shape_code.lower() or None)

    return Diamond.build(
        retailer=RETAILER,
        shape=shape,
        carat=carat,
        color=item.get("color"),
        clarity=item.get("clarity"),
        cut=item.get("cut"),
        polish=item.get("polish"),
        symmetry=item.get("symmetry"),
        fluorescence=item.get("fluorescence") or None,
        certificate_lab=item.get("certificate_lab"),
        certificate_number=str(item["certificate_number"])
            if item.get("certificate_number") else None,
        price_usd=float(price),
        product_url=product_url,
    )


def _paginate_shard(
    session: cr.Session,
    shape_code: str,
    color: str,
    min_carat: float,
    max_carat: float,
    req_delay: float,
    seen_handles: set[str],
) -> list[Diamond]:
    """Paginate one (color, carat_window) shard. Returns new Diamond objects."""
    results: list[Diamond] = []
    for page_no in range(1, MAX_PAGE + 1):
        try:
            data = _get_page(session, shape_code, color, min_carat, max_carat, page_no)
        except Exception as e:
            print(f"  [ritani] request failed ({shape_code} {color} {min_carat:.2f}-{max_carat:.2f} p{page_no}): {e}")
            break

        if data is None:   # 400 = page limit
            break

        items = data.get("items") or []
        if not items:
            break

        total_pages = data.get("total_pages", 1)
        for item in items:
            handle = item.get("handle") or item.get("variant_handle") or ""
            if not handle or handle in seen_handles:
                continue
            d = _build_diamond(item)
            if d is None:
                continue
            seen_handles.add(handle)
            results.append(d)

        if page_no >= total_pages:
            break

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
        session.get(f"{SITE_ROOT}/collections/{COLLECTION_HANDLE}", timeout=20)
    except Exception:
        pass
    time.sleep(req_delay)

    all_diamonds: list[Diamond] = []
    seen_handles: set[str] = set()

    for raw_shape in shapes:
        shape_code = _SHAPE_CODES.get(raw_shape.lower())
        if shape_code is None:
            print(f"  [ritani] shape '{raw_shape}' not in shape map, skipping")
            continue

        shape_count_before = len(all_diamonds)

        for color in COLORS:
            # Probe page 1 to get total_pages
            try:
                probe = _get_page(session, shape_code, color, min_carat, max_carat, 1)
            except Exception as e:
                print(f"  [ritani] probe failed ({raw_shape} {color}): {e}")
                time.sleep(req_delay)
                continue

            if probe is None:
                time.sleep(req_delay)
                continue

            total_pages = probe.get("total_pages", 1)
            total_items = probe.get("total", "?")

            if total_pages <= MAX_PAGE:
                # Fits in 5 pages — paginate directly
                first_items = probe.get("items") or []
                for item in first_items:
                    handle = item.get("handle") or item.get("variant_handle") or ""
                    if handle and handle not in seen_handles:
                        d = _build_diamond(item)
                        if d:
                            seen_handles.add(handle)
                            all_diamonds.append(d)

                for page_no in range(2, min(total_pages + 1, MAX_PAGE + 1)):
                    try:
                        data = _get_page(session, shape_code, color, min_carat, max_carat, page_no)
                    except Exception as e:
                        print(f"  [ritani] failed ({raw_shape} {color} p{page_no}): {e}")
                        break
                    if data is None:
                        break
                    for item in (data.get("items") or []):
                        handle = item.get("handle") or item.get("variant_handle") or ""
                        if handle and handle not in seen_handles:
                            d = _build_diamond(item)
                            if d:
                                seen_handles.add(handle)
                                all_diamonds.append(d)
                    time.sleep(req_delay)
            else:
                # Too many pages — shard by 0.01ct windows
                windows = _carat_windows(min_carat, max_carat, step=0.01)
                for c_from, c_to in windows:
                    new_items = _paginate_shard(
                        session, shape_code, color, c_from, c_to, req_delay, seen_handles
                    )
                    all_diamonds.extend(new_items)
                    time.sleep(req_delay)

                if len(all_diamonds) - shape_count_before > 0 and (len(all_diamonds) - shape_count_before) % 10000 == 0:
                    print(f"  [ritani] {raw_shape} {color} progress: {len(all_diamonds) - shape_count_before} so far")

            time.sleep(req_delay)

        shape_count = len(all_diamonds) - shape_count_before
        print(f"  [ritani] {raw_shape} collected: {shape_count} diamonds")

    print(f"  [ritani] total collected: {len(all_diamonds)} diamonds")
    return all_diamonds
