"""Ritani — benchmark-bands-only refresh.

Same lesson as With Clarity and Clean Origin: a full 0.90-2.50ct x 8-color
census is a multi-hour job (April full run was ~350K rows; June 2026 full
census kept hitting curl(28) connection-saturation timeouts and a 43% partial
was used instead — see MEMORY/project_retailer_scraping.md). The published
index only needs the same 6 benchmark cells every other retailer is measured
against (retailers/base.py VALIDATION_CELLS: G VS1 IGI/GIA at 1.00-1.09 /
1.40-1.59 / 1.90-2.09ct, round + one oval cell).

ritani.py doesn't expose a color filter (it always loops all 8 colors
internally), so this can't be scoped as tightly as With Clarity was. But
narrowing the carat range up front means far fewer color shards need the
expensive 0.01ct sub-sharding (Ritani's own hard-limit workaround), which is
where a full-range run spends most of its time. Runs one band at a time so
volume/pace can be checked before committing to the rest.

Usage: python3 run_ritani_benchmark.py <band_index 1-3>
  1 -> 0.95-1.09ct   2 -> 1.40-1.59ct   3 -> 1.90-2.09ct

Band 1 starts at 0.95ct, not 1.00ct: the published 1ct cell is the E VS1
0.95-1.05ct window, and a scrape band starting at 1.00ct silently truncates
that window to its top half every month. Widening by 0.05ct adds negligible
volume (confirmed 2026-07-26: a live re-scrape of Grown Brilliance's
0.95-1.09ct band returned the identical stone set as its 1.00-1.09ct band,
so the extra range costs nothing and can recover missed stones elsewhere).
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from retailers import ritani
from retailers.base import CSV_FIELDS, TARGET_SHAPES, diamond_to_row

DATE = "2026-07-23"
REQ_DELAY = 1.0
BANDS = [(0.95, 1.09), (1.40, 1.59), (1.90, 2.09)]
RAW_DIR = Path(__file__).parent / "data" / "raw"


def main() -> int:
    band_idx = int(sys.argv[1]) - 1 if len(sys.argv) > 1 else 0
    lo, hi = BANDS[band_idx]
    out_path = RAW_DIR / f"ritani_{DATE}_band{band_idx + 1}.csv"
    if out_path.exists():
        print(f"[ritani-benchmark] already exists ({out_path.name}), skipping")
        return 0

    print(f"[ritani-benchmark] band {band_idx + 1}/3: {lo:.2f}-{hi:.2f}ct, "
          f"shapes={TARGET_SHAPES} -> {out_path.name}")

    diamonds = ritani.scrape(TARGET_SHAPES, lo, hi, req_delay=REQ_DELAY)

    for d in diamonds:
        d.date = DATE
        d.scraped_at = f"{DATE}T12:00:00Z"

    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows([diamond_to_row(d) for d in diamonds])

    print(f"[ritani-benchmark] wrote {len(diamonds)} rows -> {out_path.name}")
    return 0


if __name__ == "__main__":
    _code = main()
    from snapshot import sync as _snapshot_sync
    _snapshot_sync()
    sys.exit(_code)
