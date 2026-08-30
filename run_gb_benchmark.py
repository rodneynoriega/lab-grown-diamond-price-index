"""Grown Brilliance — benchmark-bands-only refresh.

Same lesson as With Clarity, Clean Origin, and Ritani: a full 0.90-2.50ct
census is a multi-hour job here, and GB is the worst-behaved retailer in the
panel for this — MEMORY/project_retailer_scraping.md documents the
diamond_search_result_ajax endpoint holding connections open without
responding, causing a 7.5-hour hang at 0% CPU on a single stalled connection
during a prior overnight run, despite grown_brilliance.py's existing
timeout=60 on the POST call (a timeout param alone apparently doesn't always
bound a true TCP-level stall). That memory note's own recommendation:
"set a short per-request timeout (~30s) + retry ... and/or scrape only
benchmark carat bands." This script does the latter (no retailer-module
code changes) — same 3 bands as every other retailer's validation cells
(retailers/base.py VALIDATION_CELLS), one band at a time, so a stall shows
up quickly against a small expected job instead of silently eating hours.

Usage: python3 run_gb_benchmark.py <band_index 1-3>
  1 -> 0.95-1.09ct   2 -> 1.40-1.59ct   3 -> 1.90-2.09ct

Band 1 starts at 0.95ct, not 1.00ct: the published 1ct cell is the E VS1
0.95-1.05ct window, and a scrape band starting at 1.00ct silently truncates
that window to its top half every month. Widening by 0.05ct adds negligible
volume (confirmed 2026-07-26: a live re-scrape of this 0.95-1.09ct band
returned the identical 70-stone set as the archived 1.00-1.09ct band, same
color/clarity mix, same single E VS1 Excellent IGI stone at 1.01ct/$500 —
so GB's 1ct thinness this cycle is real inventory scarcity, not a
truncated scrape, and the wider band costs nothing to keep).
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from retailers import grown_brilliance
from retailers.base import CSV_FIELDS, TARGET_SHAPES, diamond_to_row

import os as _os
from datetime import date as _date
# Cycle date: LGD_DATE env var, else argv[2], else today. (Was hardcoded per cycle before 2026-08-29.)
DATE = _os.environ.get("LGD_DATE") or (sys.argv[2] if len(sys.argv) > 2 else _date.today().isoformat())
REQ_DELAY = 1.0
BANDS = [(0.95, 1.09), (1.40, 1.59), (1.90, 2.09)]
RAW_DIR = Path(__file__).parent / "data" / "raw"


def main() -> int:
    band_idx = int(sys.argv[1]) - 1 if len(sys.argv) > 1 else 0
    lo, hi = BANDS[band_idx]
    out_path = RAW_DIR / f"grown_brilliance_{DATE}_band{band_idx + 1}.csv"
    if out_path.exists():
        print(f"[gb-benchmark] already exists ({out_path.name}), skipping")
        return 0

    print(f"[gb-benchmark] band {band_idx + 1}/3: {lo:.2f}-{hi:.2f}ct, "
          f"shapes={TARGET_SHAPES} -> {out_path.name}")

    diamonds = grown_brilliance.scrape(TARGET_SHAPES, lo, hi, req_delay=REQ_DELAY)

    for d in diamonds:
        d.date = DATE
        d.scraped_at = f"{DATE}T12:00:00Z"

    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows([diamond_to_row(d) for d in diamonds])

    print(f"[gb-benchmark] wrote {len(diamonds)} rows -> {out_path.name}")
    return 0


if __name__ == "__main__":
    _code = main()
    from snapshot import sync as _snapshot_sync
    _snapshot_sync()
    sys.exit(_code)
