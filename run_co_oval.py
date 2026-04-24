"""Standalone runner for Clean Origin oval diamonds.

Scrapes oval shapes only (round complete from April 22 run).
Uses req_delay=2.0 to avoid tarpitting, and loads the April 22 detail cache.

Output: data/raw/clean_origin_oval_2026-04-23.csv
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from retailers.clean_origin import scrape
from retailers.base import CSV_FIELDS, MIN_CARAT, MAX_CARAT, diamond_to_row

RAW_DIR = Path(__file__).parent / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

OUT_PATH = RAW_DIR / "clean_origin_oval_2026-04-23.csv"
REQ_DELAY = 2.0  # conservative to avoid tarpit after yesterday's round scrape


def _load_co_detail_cache() -> dict[str, dict]:
    cache: dict[str, dict] = {}
    for csv_path in sorted(RAW_DIR.glob("clean_origin_*.csv")):
        try:
            with csv_path.open(newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    url = row.get("product_url", "").strip()
                    if not url or url in cache:
                        continue
                    detail = {
                        "polish":       row.get("polish") or None,
                        "symmetry":     row.get("symmetry") or None,
                        "fluorescence": row.get("fluorescence") or None,
                        "cert_lab":     row.get("certificate_lab") or None,
                        "cert_number":  row.get("certificate_number") or None,
                    }
                    if any(v is not None for v in detail.values()):
                        cache[url] = detail
        except Exception:
            continue
    print(f"[clean_origin] detail cache loaded: {len(cache)} entries")
    return cache


if __name__ == "__main__":
    if OUT_PATH.exists():
        print(f"Already exists: {OUT_PATH.name}, skipping")
        sys.exit(0)

    detail_cache = _load_co_detail_cache()

    print("\n== Clean Origin (oval only) ==")
    diamonds = scrape(
        ["oval"],
        MIN_CARAT,
        MAX_CARAT,
        req_delay=REQ_DELAY,
        detail_cache=detail_cache,
    )

    with OUT_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows([diamond_to_row(d) for d in diamonds])
    print(f"Saved {len(diamonds)} rows -> {OUT_PATH.name}")
