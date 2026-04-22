"""Orchestrator: scrape every retailer at every benchmark, append to CSV."""

from __future__ import annotations

import csv
import sys
from datetime import date
from pathlib import Path

from retailers import blue_nile, brilliant_earth
from retailers.base import BENCHMARKS, Benchmark, Match

OUTPUT_CSV = Path(__file__).parent / "data" / "prices.csv"
CSV_FIELDS = ["date", "retailer", "carat_weight", "price_usd", "url"]

# Clean Origin, With Clarity, and VRAI stubs live in retailers/ but are not
# yet wired into the weekly run; they will be added once implemented.
RETAILERS = [
    ("Brilliant Earth", brilliant_earth.scrape),
    ("Blue Nile",       blue_nile.scrape),
]


def main() -> int:
    today = date.today().isoformat()
    rows: list[dict] = []

    for retailer_name, scrape_fn in RETAILERS:
        print(f"\n== {retailer_name} ==")
        for bench in BENCHMARKS:
            try:
                match: Match | None = scrape_fn(bench)
            except Exception as e:
                print(f"  {bench.label_carat}ct: error {e}", file=sys.stderr)
                continue
            if match is None:
                print(f"  {bench.label_carat}ct: no matching stone", file=sys.stderr)
                continue
            rows.append({
                "date": today,
                "retailer": retailer_name,
                "carat_weight": bench.label_carat,
                "price_usd": match.price_usd,
                "url": match.url,
            })
            matches_note = f" ({match.total_matches} stones match)" if match.total_matches else ""
            print(f"  {bench.label_carat}ct  ${match.price_usd:>7,.0f}  "
                  f"actual={match.actual_carat}ct  {match.cut} {match.color} {match.clarity}"
                  f"{matches_note}")

    if not rows:
        print("No rows collected. Nothing written.", file=sys.stderr)
        return 1

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    new_file = not OUTPUT_CSV.exists()
    with OUTPUT_CSV.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if new_file:
            writer.writeheader()
        writer.writerows(rows)
    print(f"\nAppended {len(rows)} row(s) to {OUTPUT_CSV}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
