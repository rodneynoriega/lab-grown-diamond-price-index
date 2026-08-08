"""With Clarity — benchmark-bands-only pull via the Playwright fallback.

Per the coordinator: the raw `python3 -m retailers.with_clarity_pw scrape` CLI
mode does a full 0.90-2.50ct round+oval pull ("thousands of pages" per its own
docstring, likely to retrip the per-IP block). It ignores the `carat_bands`
parameter that scrape() already supports specifically as the IP-block
workaround. This wrapper calls scrape() directly with carat_bands narrowed to
the actual live benchmark cells this index publishes — NOT with_clarity_pw's
own stale BENCHMARK_BANDS constant (0.95-1.05/1.45-1.55/1.95-2.05), which is
left over from the old E VS1 spec. The current spec (retailers/base.py
VALIDATION_CELLS) uses G VS1 IGI/GIA at 1.00-1.09 / 1.40-1.59 / 1.90-2.09ct,
so that's what every other retailer is actually validated against — With
Clarity needs to match those same windows, not the outdated ones.

UPDATE after a live probe: with colors=None (full range), just the round
1.00-1.09ct band alone returned 25,874 stones (~1,294 pages, ~22 minutes at
this req_delay) — every current VALIDATION_CELLS row in retailers/base.py
requires color="G" specifically (all six cells), so pulling the other ~8
color grades server-side would only be thrown away downstream anyway. Added
a server-side color=G filter (with_clarity_pw.COLOR_CODE["G"]) to cut that
dead weight — this changes nothing about which stones end up published,
only how many get fetched and discarded to get there. Clarity/cut are left
unfiltered (server-side codes for those aren't hardcoded in with_clarity_pw
yet, and reverse-engineering them isn't worth the risk under a time-boxed
hotspot window) — downstream validation in scrape.py still filters those as
normal, same as every other retailer.

Usage: python3 run_wc_benchmark.py [DATE]   # DATE defaults to 2026-07-23
                                              # (this refresh cycle), not today,
                                              # so the file lines up with the
                                              # rest of this cycle's raw CSVs.
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from retailers import with_clarity_pw
from retailers.base import CSV_FIELDS, TARGET_SHAPES, diamond_to_row

# Matches retailers/base.py VALIDATION_CELLS exactly (G VS1 IGI/GIA cells) —
# NOT with_clarity_pw.BENCHMARK_BANDS, which is the stale E VS1 spec.
CURRENT_BENCHMARK_BANDS = [(1.00, 1.09), (1.40, 1.59), (1.90, 2.09)]

DATE = sys.argv[1] if len(sys.argv) > 1 else "2026-07-23"
REQ_DELAY = 1.0  # same politeness delay as every other scraper — not reduced

RAW_DIR = Path(__file__).parent / "data" / "raw"
OUT_PATH = RAW_DIR / f"with_clarity_{DATE}.csv"


def main() -> int:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    if OUT_PATH.exists():
        print(f"[wc-benchmark] already exists ({OUT_PATH.name}), skipping")
        return 0

    g_color = [with_clarity_pw.COLOR_CODE["G"]]
    print(f"[wc-benchmark] bands={CURRENT_BENCHMARK_BANDS} shapes={TARGET_SHAPES} "
          f"colors=G-only cuts=all claritys=all -> {OUT_PATH.name}")

    diamonds = with_clarity_pw.scrape(
        TARGET_SHAPES,
        min_carat=CURRENT_BENCHMARK_BANDS[0][0],   # unused when carat_bands is given, but required
        max_carat=CURRENT_BENCHMARK_BANDS[-1][1],
        req_delay=REQ_DELAY,
        carat_bands=CURRENT_BENCHMARK_BANDS,
        colors=g_color,
        cuts=None,
        claritys=None,
    )

    # with_clarity_pw.scrape()'s internal Diamond.build() calls don't pass
    # date/scraped_at, so it defaults to real build-time (same class of bug
    # already caught and fixed in run_co_chunked.py's finalize). Normalize
    # here instead of touching the shared retailer module.
    for d in diamonds:
        d.date = DATE
        d.scraped_at = f"{DATE}T12:00:00Z"

    with OUT_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows([diamond_to_row(d) for d in diamonds])

    print(f"[wc-benchmark] wrote {len(diamonds)} rows -> {OUT_PATH.name}")
    return 0


if __name__ == "__main__":
    _code = main()
    from snapshot import sync as _snapshot_sync
    _snapshot_sync()
    sys.exit(_code)
