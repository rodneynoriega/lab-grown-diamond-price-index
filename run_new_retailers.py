"""Standalone runner for the three new retailer scrapers.

Produces:
  data/raw/james_allen_2026-04-23.csv
  data/raw/ritani_2026-04-23.csv
  data/raw/grown_brilliance_2026-04-23.csv
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from retailers import james_allen, ritani, grown_brilliance
from retailers.base import CSV_FIELDS, TARGET_SHAPES, MIN_CARAT, MAX_CARAT, diamond_to_row

RAW_DIR = Path(__file__).parent / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

DATE = "2026-04-23"
DELAY = 1.0

RETAILERS = [
    ("James Allen",    "james_allen",    james_allen.scrape),
    ("Ritani",         "ritani",         ritani.scrape),
    ("Grown Brilliance","grown_brilliance", grown_brilliance.scrape),
]


def run(name: str, slug: str, scrape_fn) -> None:
    out_path = RAW_DIR / f"{slug}_{DATE}.csv"
    if out_path.exists():
        print(f"[{name}] already exists ({out_path.name}), skipping")
        return
    print(f"\n== {name} ==")
    try:
        diamonds = scrape_fn(TARGET_SHAPES, MIN_CARAT, MAX_CARAT, req_delay=DELAY)
    except Exception as e:
        print(f"  ERROR: {e}", file=sys.stderr)
        return
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows([diamond_to_row(d) for d in diamonds])
    print(f"  Saved {len(diamonds)} rows -> {out_path.name}")


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "all"
    for name, slug, fn in RETAILERS:
        if target == "all" or target == slug:
            run(name, slug, fn)
