"""Grown Brilliance lab-grown diamond scraper — full inventory pull.

Uses GB's AJAX diamond search endpoint (POST /diamond_search_result_ajax).
This endpoint requires a CSRF token obtained by first loading the search page.
It returns paginated HTML tables; we parse them for diamond data.

Field coverage from listing:
  shape, carat, color, clarity, cut, polish, symmetry,
  certificate_lab, certificate_number, price_usd, product_url.
  Missing: fluorescence (not exposed in the listing table; would need detail page).

Shape IDs (from GB JS):
  Round=1, Oval=2, Princess=3, Pear=4, Marquise=5, Emerald=6, Asscher=7,
  Cushion=8, Heart=9, Radiant=10.
"""

from __future__ import annotations

import re
import time

from curl_cffi import requests as cr

from .base import Diamond

RETAILER = "Grown Brilliance"
SITE_ROOT = "https://www.grownbrilliance.com"
SEARCH_PAGE = f"{SITE_ROOT}/lab-grown-diamonds-search"
AJAX_URL = f"{SITE_ROOT}/diamond_search_result_ajax"

_SHAPE_IDS: dict[str, str] = {
    "round": "1",
    "oval": "2",
    "princess": "3",
    "pear": "4",
    "marquise": "5",
    "emerald": "6",
    "asscher": "7",
    "cushion": "8",
    "heart": "9",
    "radiant": "10",
}

_AJAX_HEADERS = {
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Referer": SEARCH_PAGE,
}


def _get_csrf(session: cr.Session) -> str:
    resp = session.get(SEARCH_PAGE, timeout=30)
    resp.raise_for_status()
    m = re.search(r'<meta name="csrf-token" content="([^"]+)"', resp.text)
    if not m:
        raise RuntimeError("Could not find CSRF token on Grown Brilliance search page")
    return m.group(1)


def _post_page(
    session: cr.Session,
    shape_id: str,
    min_carat: float,
    max_carat: float,
    page: int,
    csrf_token: str,
) -> dict:
    data = {
        "is_ajax_call": "1",
        "shapes": shape_id,
        "fromCarat": f"{min_carat:.2f}",
        "toCarat": f"{max_carat:.2f}",
        "page": str(page),
        "orignal_update_slider": "0",
        "_token": csrf_token,
    }
    resp = session.post(AJAX_URL, data=data, headers=_AJAX_HEADERS, timeout=60)
    resp.raise_for_status()
    return resp.json()


def _parse_cert_popup(row_html: str) -> tuple[str | None, str | None]:
    """Extract cert_lab and cert_number from view_reportPopUp onclick."""
    m = re.search(r"view_reportPopUp\('([^']+)',\s*'([^']*)'\)", row_html)
    if m:
        return m.group(1).strip() or None, m.group(2).strip() or None
    return None, None


def _parse_price(row_html: str) -> float | None:
    m = re.search(r'class="dmd-price price"[^>]*>\s*\$([0-9,]+)', row_html)
    if not m:
        m = re.search(r'\$\s*([0-9,]+)', row_html)
    if m:
        try:
            return float(m.group(1).replace(",", ""))
        except ValueError:
            pass
    return None


def _td_text(row_html: str, cls: str) -> str | None:
    """Extract text content of a <td class="cls"> cell."""
    m = re.search(
        rf'<td[^>]*class="[^"]*\b{re.escape(cls)}\b[^"]*"[^>]*>(.*?)</td>',
        row_html, re.S,
    )
    if not m:
        return None
    raw = re.sub(r"<[^>]+>", "", m.group(1)).strip()
    # Remove trailing " ct" for carat cells
    raw = re.sub(r"\s+ct\s*$", "", raw, flags=re.I).strip()
    return raw or None


def _product_url(row_html: str) -> str | None:
    m = re.search(
        r'href="(https://www\.grownbrilliance\.com/[^"]+)"',
        row_html,
    )
    return m.group(1) if m else None


def _parse_rows(html: str) -> list[dict]:
    rows = re.findall(
        r'<tr[^>]*class="[^"]*ds_rtable_row[^"]*"[^>]*>(.*?)</tr>',
        html, re.S,
    )
    results: list[dict] = []
    for row in rows:
        url = _product_url(row)
        if not url:
            continue
        cert_lab, cert_num = _parse_cert_popup(row)
        price = _parse_price(row)
        shape_text = _td_text(row, "shape-ds")
        if not shape_text:
            # fallback: extract from <span class="text">
            sm = re.search(r'class="text">([^<]+)<', row)
            shape_text = sm.group(1).strip() if sm else None

        results.append({
            "url": url,
            "shape": shape_text,
            "carat": _td_text(row, "carat"),
            "color": _td_text(row, "color"),
            "clarity": _td_text(row, "clarity"),
            "cut": _td_text(row, "cut"),
            "polish": _td_text(row, "polish"),
            "symmetry": _td_text(row, "symmetry"),
            "price": price,
            "cert_lab": cert_lab,
            "cert_num": cert_num,
        })
    return results


def scrape(
    shapes: list[str],
    min_carat: float,
    max_carat: float,
    *,
    req_delay: float = 1.0,
) -> list[Diamond]:
    session = cr.Session(impersonate="chrome")

    # Warm up and get CSRF token
    csrf_token = _get_csrf(session)
    time.sleep(req_delay)

    all_diamonds: list[Diamond] = []
    seen_urls: set[str] = set()

    for raw_shape in shapes:
        shape_id = _SHAPE_IDS.get(raw_shape.lower())
        if shape_id is None:
            print(f"  [grown_brilliance] shape '{raw_shape}' not supported, skipping")
            continue

        shape_count = 0
        page = 0
        total_pages: int | None = None

        while True:
            try:
                payload = _post_page(
                    session, shape_id, min_carat, max_carat, page, csrf_token
                )
            except Exception as e:
                print(f"  [grown_brilliance] request failed (shape={raw_shape} page={page}): {e}")
                # Refresh CSRF token and retry once
                try:
                    csrf_token = _get_csrf(session)
                    time.sleep(req_delay * 2)
                    payload = _post_page(
                        session, shape_id, min_carat, max_carat, page, csrf_token
                    )
                except Exception as e2:
                    print(f"  [grown_brilliance] retry also failed: {e2}")
                    break

            total_raw = payload.get("totalDiamond", "0")
            try:
                total_count = int(str(total_raw).replace(",", ""))
            except ValueError:
                total_count = 0

            last_page = payload.get("lastPage", 1)
            next_page = payload.get("nextPage", "")

            if total_pages is None:
                total_pages = int(last_page) if last_page else 1
                print(
                    f"  [grown_brilliance] {raw_shape} {min_carat:.2f}-{max_carat:.2f}ct: "
                    f"{total_count} diamonds, {total_pages} pages"
                )

            html = payload.get("html", "")
            rows = _parse_rows(html)

            if not rows:
                break

            for r in rows:
                url = r.get("url")
                if not url or url in seen_urls:
                    continue
                seen_urls.add(url)

                carat_str = r.get("carat")
                try:
                    carat = float(carat_str)
                except (TypeError, ValueError):
                    continue
                if not (min_carat <= carat <= max_carat):
                    continue
                price = r.get("price")
                if price is None:
                    continue

                all_diamonds.append(
                    Diamond.build(
                        retailer=RETAILER,
                        shape=r.get("shape"),
                        carat=carat,
                        color=r.get("color"),
                        clarity=r.get("clarity"),
                        cut=r.get("cut"),
                        polish=r.get("polish"),
                        symmetry=r.get("symmetry"),
                        fluorescence=None,  # not in listing HTML
                        certificate_lab=r.get("cert_lab"),
                        certificate_number=r.get("cert_num"),
                        price_usd=price,
                        product_url=url,
                    )
                )
                shape_count += 1

            if not next_page or str(next_page) == "" or page >= total_pages:
                break

            if shape_count % 2000 == 0:
                print(
                    f"  [grown_brilliance] {raw_shape} progress: {shape_count} diamonds"
                )

            page = int(next_page)
            time.sleep(req_delay)

        print(f"  [grown_brilliance] {raw_shape} collected: {shape_count} diamonds")

    print(f"  [grown_brilliance] total collected: {len(all_diamonds)} diamonds")
    return all_diamonds
