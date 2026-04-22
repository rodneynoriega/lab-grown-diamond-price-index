"""
Brilliant Earth lab-grown diamond price scraper.

Scrapes benchmark stones for the Rings.com Lab-Grown Diamond Price Index:
round cut, VS2 clarity, F-G color, excellent cut, at 1.0ct / 1.5ct / 2.0ct.

Calls Brilliant Earth's internal product-listing API directly
(/api/v1/plp/products/). For each benchmark it records the lowest-priced
matching stone (the realistic market floor for that spec) and appends one row
per benchmark to data/prices.csv.

Requirements:
  - Must be run from a US IP. Brilliant Earth geo-routes non-US traffic to
    their Canadian site with CAD pricing; we need USD.
  - curl_cffi is used to impersonate Chrome's TLS fingerprint so Brilliant
    Earth's Cloudflare shield returns real responses instead of challenge pages.
"""

from __future__ import annotations

import csv
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from curl_cffi import requests as cr

RETAILER = "Brilliant Earth"
SITE_ROOT = "https://www.brilliantearth.com"
API_URL = f"{SITE_ROOT}/api/v1/plp/products/"
DETAIL_URL_FMT = f"{SITE_ROOT}/lab-diamonds-search/view_detail/{{id}}/"

OUTPUT_CSV = Path(__file__).parent / "data" / "prices.csv"
CSV_FIELDS = ["date", "retailer", "carat_weight", "price_usd", "url"]


@dataclass(frozen=True)
class Benchmark:
    label_carat: float
    min_carat: float
    max_carat: float


# Commercial carat buckets: a 1.03ct stone is sold as "1 carat".
BENCHMARKS = [
    Benchmark(1.0, 1.00, 1.09),
    Benchmark(1.5, 1.50, 1.59),
    Benchmark(2.0, 2.00, 2.19),
]

# Filters shared across all benchmark queries. "Excellent cut" for round
# lab diamonds at Brilliant Earth maps to Super Ideal + Ideal (the top two
# cut grades). Polish and symmetry pinned to Excellent.
COMMON_PARAMS = {
    "currency": "USD",
    "product_class": "Lab Created Colorless Diamonds",
    "shapes": "Round",
    "cuts": "Super Ideal,Ideal",
    "colors": "F,G",
    "clarities": "VS2",
    "polishes": "Excellent",
    "symmetries": "Excellent",
    "order_by": "price",
    "order_method": "asc",
    "display": 5,
    "page": 1,
}

HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Referer": f"{SITE_ROOT}/lab-grown-diamonds/",
    "X-Requested-With": "XMLHttpRequest",
}


def fetch_cheapest_match(session, bench: Benchmark) -> dict | None:
    params = {
        **COMMON_PARAMS,
        "min_carat": f"{bench.min_carat:.2f}",
        "max_carat": f"{bench.max_carat:.2f}",
    }
    resp = session.get(API_URL, params=params, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    products = data.get("products") or []
    if not products:
        return None

    # Results are price-ascending via order_by=price. Defensively re-verify
    # each stone meets the spec, then pick the cheapest.
    for product in products:
        if product.get("origin") != "Lab Created":
            continue
        if product.get("shape") != "Round":
            continue
        if product.get("cut") not in ("Super Ideal", "Ideal"):
            continue
        if product.get("color") not in ("F", "G"):
            continue
        if product.get("clarity") != "VS2":
            continue
        if product.get("polish") != "Excellent" or product.get("symmetry") != "Excellent":
            continue
        carat = product.get("carat")
        if carat is None or not (bench.min_carat <= float(carat) <= bench.max_carat):
            continue
        price = product.get("price")
        if price is None:
            continue
        return {
            "price_usd": float(price),
            "url": DETAIL_URL_FMT.format(id=product["id"]),
            "actual_carat": float(carat),
            "color": product.get("color"),
            "cut": product.get("cut"),
            "total_matches": data.get("total"),
        }

    return None


def append_rows(rows: list[dict]) -> None:
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    new_file = not OUTPUT_CSV.exists()
    with OUTPUT_CSV.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if new_file:
            writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    today = date.today().isoformat()
    rows: list[dict] = []

    session = cr.Session(impersonate="chrome")
    # Warm up: fetch homepage so Cloudflare issues any session cookies it wants.
    session.get(SITE_ROOT + "/", timeout=30)

    for bench in BENCHMARKS:
        try:
            match = fetch_cheapest_match(session, bench)
        except Exception as e:
            print(f"  {bench.label_carat}ct: error {e}", file=sys.stderr)
            continue
        if match is None:
            print(f"  {bench.label_carat}ct: no matching stone", file=sys.stderr)
            continue
        rows.append({
            "date": today,
            "retailer": RETAILER,
            "carat_weight": bench.label_carat,
            "price_usd": match["price_usd"],
            "url": match["url"],
        })
        print(f"  {bench.label_carat}ct  ${match['price_usd']:>7,.0f}  "
              f"actual={match['actual_carat']}ct  {match['cut']} {match['color']} VS2  "
              f"({match['total_matches']} stones match spec)  {match['url']}")

    if not rows:
        print("No rows collected. Nothing written.", file=sys.stderr)
        return 1

    append_rows(rows)
    print(f"\nAppended {len(rows)} row(s) to {OUTPUT_CSV}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
