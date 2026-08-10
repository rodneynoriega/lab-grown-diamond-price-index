"""Carat-extension pull (shape-widening pilot, Phase 1) — approved 2026-08-09.

Collects the never-before-scraped range above 2.50ct to answer the 3ct page's
data gate (cells must clear n>=30 at >=3 retailers; With Clarity excluded from
published pages per the 2026-08-09 scope amendment, so it is not pulled here).

Scope per the approved plan (OUTBOX/2026-08-09-shape-widening-pilot-plan.md):
- Brilliant Earth / Blue Nile (full-inventory scrapers): round+oval 2.50-3.10ct,
  the delta above their existing 0.90-2.50 monthly coverage.
- Ritani / Grown Brilliance (band scrapers): one NEW band 2.90-3.10ct, their
  native mode (full pulls tarpit; see run_gb_benchmark.py header).
The established benchmark bands and cadence are untouched; this is additive.

Usage: python3 run_carat_extension.py [retailer_slug|all]
Idempotent: skips any retailer whose output CSV already exists.
"""

from __future__ import annotations

import csv
import sys
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from retailers import blue_nile, brilliant_earth, grown_brilliance, ritani
from retailers.base import CSV_FIELDS, TARGET_SHAPES, diamond_to_row

DATE = "2026-08-09"
RAW_DIR = Path(__file__).parent / "data" / "raw"

# slug -> (module.scrape, lo, hi, req_delay)
JOBS = {
    "brilliant_earth":  (brilliant_earth.scrape,  2.50, 3.10, 1.5),
    "blue_nile":        (blue_nile.scrape,        2.50, 3.10, 1.0),
    "grown_brilliance": (grown_brilliance.scrape, 2.90, 3.10, 1.0),
    "ritani":           (ritani.scrape,           2.90, 3.10, 8.0),  # 429s -> long delay
}


def run_one(slug: str) -> tuple[str, int | None]:
    fn, lo, hi, delay = JOBS[slug]
    out_path = RAW_DIR / f"{slug}_{DATE}_ext3ct.csv"
    if out_path.exists():
        print(f"[carat-ext] {slug}: already exists ({out_path.name}), skipping")
        return ("skipped", None)
    print(f"[carat-ext] {slug}: {lo:.2f}-{hi:.2f}ct shapes={TARGET_SHAPES} "
          f"req_delay={delay}s -> {out_path.name}", flush=True)
    try:
        diamonds = fn(TARGET_SHAPES, lo, hi, req_delay=delay)
    except Exception:
        print(f"[carat-ext] {slug}: FAILED", file=sys.stderr)
        traceback.print_exc()
        return ("failed", None)

    for d in diamonds:
        d.date = DATE
        d.scraped_at = f"{DATE}T12:00:00Z"

    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows([diamond_to_row(d) for d in diamonds])
    print(f"[carat-ext] {slug}: wrote {len(diamonds)} rows -> {out_path.name}",
          flush=True)
    return ("ok", len(diamonds))


def main() -> int:
    target = sys.argv[1] if len(sys.argv) > 1 else "all"
    slugs = list(JOBS) if target == "all" else [target]
    results: dict[str, tuple[str, int | None]] = {}
    for slug in slugs:
        results[slug] = run_one(slug)

    print("\n[carat-ext] summary:")
    for slug, (status, n) in results.items():
        print(f"  {slug}: {status}" + (f" ({n} rows)" if n is not None else ""))
    return 1 if any(s == "failed" for s, _ in results.values()) else 0


if __name__ == "__main__":
    _code = main()
    from snapshot import sync as _snapshot_sync
    _snapshot_sync()
    sys.exit(_code)
