"""Standalone runner for the With Clarity Playwright scraper.

Use when the curl_cffi WC path is IP-blocked (403) and you're on a clean
residential IP (cooled-down home connection or a phone hotspot). Saves to the
same path/format as the normal pipeline so scrape.py --validate-only and the
combine step pick it up.

  python3 run_wc_pw.py                              # today, all shapes, full range
  python3 run_wc_pw.py 2026-06-27 round 0.5        # round-only, full range
  python3 run_wc_pw.py 2026-06-27 round 0.6 bands  # round, benchmark bands only

The `bands` mode pulls only the three benchmark carat windows (0.95-1.05,
1.45-1.55, 1.95-2.05ct) instead of the full 0.90-2.50 range. Use it when WC
is blocking an IP on request volume — far fewer pages, finishes before the
block trips. Grades are filtered downstream, so the index medians are unaffected.
"""

from __future__ import annotations

import csv
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from retailers import with_clarity_pw
from retailers.base import CSV_FIELDS, TARGET_SHAPES, MIN_CARAT, MAX_CARAT, diamond_to_row

RAW_DIR = Path(__file__).parent / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# Benchmark carat windows = index cell_ranges (0.95-1.05 / 1.45-1.55 / 1.95-2.05).
BENCHMARK_BANDS = [(0.95, 1.05), (1.45, 1.55), (1.95, 2.05)]

DATE = sys.argv[1] if len(sys.argv) > 1 else date.today().isoformat()
SHAPES = sys.argv[2].split(",") if len(sys.argv) > 2 else TARGET_SHAPES
DELAY = float(sys.argv[3]) if len(sys.argv) > 3 else 1.0
USE_BANDS = len(sys.argv) > 4 and sys.argv[4] == "bands"
# argv[5]: 1-based band indices into BENCHMARK_BANDS, e.g. "2" or "2,3". Default all.
BAND_SEL = [int(i) for i in sys.argv[5].split(",")] if len(sys.argv) > 5 else None
# argv[6]: output filename suffix, e.g. "15ct" -> with_clarity_{DATE}_15ct.csv.
SUFFIX = sys.argv[6] if len(sys.argv) > 6 else None


def main() -> None:
    name = f"with_clarity_{DATE}_{SUFFIX}.csv" if SUFFIX else f"with_clarity_{DATE}.csv"
    out_path = RAW_DIR / name
    if out_path.exists():
        print(f"[With Clarity] already exists ({out_path.name}), skipping")
        return
    if USE_BANDS:
        bands = [BENCHMARK_BANDS[i - 1] for i in BAND_SEL] if BAND_SEL else BENCHMARK_BANDS
    else:
        bands = None
    # In bands mode, also filter color server-side to E (the US benchmark color)
    # to keep request volume low and dodge WC's per-IP block. E = code 7.
    colors = [with_clarity_pw.COLOR_CODE["E"]] if USE_BANDS else None
    print(f"== With Clarity (Playwright) {SHAPES} delay={DELAY} "
          f"bands={'benchmark' if USE_BANDS else 'full-range'} "
          f"colors={'E-only' if colors else 'all'} -> {out_path.name} ==")
    diamonds = with_clarity_pw.scrape(
        SHAPES, MIN_CARAT, MAX_CARAT, req_delay=DELAY, carat_bands=bands, colors=colors
    )
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows([diamond_to_row(d) for d in diamonds])
    print(f"  Saved {len(diamonds)} rows -> {out_path.name}")


if __name__ == "__main__":
    main()
    from snapshot import sync as _snapshot_sync
    _snapshot_sync()
