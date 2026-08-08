"""One-off resume: fetch the remaining With Clarity benchmark scope and merge
with the already-complete 1.00-1.09ct Round/G rows from the first run.

Remaining scope:
  - Round: 1.40-1.59ct (refetched in full — the prior partial can't cleanly
    resume mid-band) + 1.90-2.09ct
  - Oval: all three bands (never attempted yet)

Same G-color server-side filter as the first run, same req_delay.
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from retailers import with_clarity_pw
from retailers.base import CSV_FIELDS, diamond_to_row

DATE = "2026-07-23"
REQ_DELAY = 1.0
RAW_DIR = Path(__file__).parent / "data" / "raw"
OUT_PATH = RAW_DIR / f"with_clarity_{DATE}.csv"
G = [with_clarity_pw.COLOR_CODE["G"]]


def main() -> int:
    with OUT_PATH.open(newline="", encoding="utf-8") as f:
        existing_rows = list(csv.DictReader(f))
    keep = [r for r in existing_rows if r["shape"] == "round" and 1.00 <= float(r["carat"]) <= 1.09]
    print(f"[resume] preserving {len(keep)} already-complete round 1.00-1.09ct rows")

    all_new_diamonds = []

    print("[resume] fetching round 1.40-1.59 + 1.90-2.09 (G only)...")
    d1 = with_clarity_pw.scrape(
        ["round"], min_carat=1.40, max_carat=2.09, req_delay=REQ_DELAY,
        carat_bands=[(1.40, 1.59), (1.90, 2.09)], colors=G,
    )
    all_new_diamonds.extend(d1)

    print("[resume] fetching oval 1.00-1.09 + 1.40-1.59 + 1.90-2.09 (G only)...")
    d2 = with_clarity_pw.scrape(
        ["oval"], min_carat=1.00, max_carat=2.09, req_delay=REQ_DELAY,
        carat_bands=[(1.00, 1.09), (1.40, 1.59), (1.90, 2.09)], colors=G,
    )
    all_new_diamonds.extend(d2)

    for d in all_new_diamonds:
        d.date = DATE
        d.scraped_at = f"{DATE}T12:00:00Z"

    new_rows = [diamond_to_row(d) for d in all_new_diamonds]

    with OUT_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(keep)
        writer.writerows(new_rows)

    print(f"[resume] wrote {len(keep) + len(new_rows)} total rows "
          f"({len(keep)} preserved + {len(new_rows)} newly fetched) -> {OUT_PATH.name}")
    return 0


if __name__ == "__main__":
    _code = main()
    from snapshot import sync as _snapshot_sync
    _snapshot_sync()
    sys.exit(_code)
