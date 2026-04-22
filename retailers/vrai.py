"""VRAI lab-grown diamond scraper.

Calls VRAI's internal Next.js API route (/api/diamonds) directly.
VRAI's inventory is fully lab-grown (Diamond Foundry grown, "Cut for You"
model). All stocked rounds are Super Ideal cut; color grades available
include D through H. Prices are returned in cents (divide by 100).

Filter approach: fetch all pages for F-color then G-color round brilliants
(~124 stones each, 7 pages), combine, and pick the cheapest stone that
falls in the benchmark carat window and meets the VS2-or-better floor.
Fetching both colors is necessary because inventory coverage by carat
varies; F has no 2ct stones, G fills that gap.
"""

from __future__ import annotations

from curl_cffi import requests as cr

from .base import Benchmark, Match

SITE_ROOT = "https://www.vrai.com"
API_URL = f"{SITE_ROOT}/api/diamonds"
DETAIL_URL_FMT = f"{SITE_ROOT}/diamonds/{{lot_id}}"

ALLOWED_CLARITIES = {"FL", "IF", "VVS1", "VVS2", "VS1", "VS2"}

HEADERS = {
    "Accept": "application/json",
    "Referer": f"{SITE_ROOT}/diamonds/lab-grown",
}


def _fetch_color(session: cr.Session, color: str) -> list[dict]:
    items: list[dict] = []
    for page in range(1, 20):
        resp = session.get(
            API_URL,
            params={
                "diamondType": "round-brilliant",
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
        batch = data.get("items", [])
        items.extend(batch)
        total = data.get("paginator", {}).get("itemCount", 0)
        if not batch or len(items) >= total:
            break
    return items


def scrape(bench: Benchmark) -> Match | None:
    session = cr.Session(impersonate="chrome")
    session.get(SITE_ROOT + "/", timeout=30)  # warm up cookies

    candidates: list[dict] = []
    for color in ("F", "G"):
        for d in _fetch_color(session, color):
            carat = float(d["carat"])
            if not (bench.min_carat <= carat <= bench.max_carat):
                continue
            if d.get("clarity") not in ALLOWED_CLARITIES:
                continue
            if not d.get("availableForSale", True):
                continue
            candidates.append(d)

    if not candidates:
        return None

    candidates.sort(key=lambda d: d["price"])
    best = candidates[0]
    return Match(
        price_usd=best["price"] / 100,
        url=DETAIL_URL_FMT.format(lot_id=best["lotId"]),
        actual_carat=float(best["carat"]),
        cut=best.get("cut", "Super Ideal"),
        color=best["color"],
        clarity=best["clarity"],
        total_matches=len(candidates),
    )
