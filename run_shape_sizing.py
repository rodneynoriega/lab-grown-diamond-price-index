"""Shape sizing pull (shape-widening pilot, Phase 2) — approved 2026-08-09.

Sizes the 6 candidate NEW shapes (cushion, princess, emerald, pear, marquise,
radiant) at BE + BN, 0.90-3.10ct, full inventory, to build the shape x carat x
retailer cell-count matrix (Phase 3 report). Round + oval are deliberately NOT
re-pulled: their 0.90-2.50 coverage is the July cycle and their 2.50-3.10
coverage is the 2026-08-09 ext3ct pull; re-pulling would double the footprint
on both sites for no new information (plan risk #2: widened pulls change block
posture). The Phase 3 matrix derives round/oval cells from those existing files
and states the date basis per cell.

Scope per OUTBOX/2026-08-09-shape-widening-pilot-plan.md (Phase 2):
- BE + BN only (the two full-inventory scrapers). Sequential, never parallel.
- Stop-on-block: a failed shape pull aborts that retailer's REMAINING shapes
  (benchmark scope stays safe); the other retailer still runs.

Usage: python3 run_shape_sizing.py [brilliant_earth|blue_nile|all]
Idempotent + resumable: one output CSV per retailer x shape; existing files
are skipped, so a crashed run continues where it left off.
"""

from __future__ import annotations

import csv
import sys
import traceback
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from retailers import blue_nile, brilliant_earth
from retailers.base import CSV_FIELDS, diamond_to_row

RAW_DIR = Path(__file__).parent / "data" / "raw"

NEW_SHAPES = ["cushion", "princess", "emerald", "pear", "marquise", "radiant"]
LO, HI = 0.90, 3.10

# slug -> (module.scrape, req_delay)
JOBS = {
    "brilliant_earth": (brilliant_earth.scrape, 1.5),
    "blue_nile":       (blue_nile.scrape,       1.0),
}


def run_retailer(slug: str) -> dict[str, tuple[str, int | None]]:
    fn, delay = JOBS[slug]
    results: dict[str, tuple[str, int | None]] = {}
    for shape in NEW_SHAPES:
        run_date = date.today().isoformat()
        out_path = RAW_DIR / f"{slug}_{run_date}_shape_{shape}.csv"
        # Resumability check must also match a file started on an earlier date.
        existing = sorted(RAW_DIR.glob(f"{slug}_*_shape_{shape}.csv"))
        if existing:
            print(f"[shape-sizing] {slug}/{shape}: already exists "
                  f"({existing[-1].name}), skipping", flush=True)
            results[shape] = ("skipped", None)
            continue
        print(f"[shape-sizing] {slug}/{shape}: {LO:.2f}-{HI:.2f}ct "
              f"req_delay={delay}s -> {out_path.name}", flush=True)
        try:
            diamonds = fn([shape], LO, HI, req_delay=delay)
        except Exception:
            print(f"[shape-sizing] {slug}/{shape}: FAILED — aborting this "
                  f"retailer's remaining shapes (stop-on-block rule)",
                  file=sys.stderr, flush=True)
            traceback.print_exc()
            results[shape] = ("failed", None)
            return results

        for d in diamonds:
            d.date = run_date
            d.scraped_at = f"{run_date}T12:00:00Z"

        with out_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            writer.writeheader()
            writer.writerows([diamond_to_row(d) for d in diamonds])
        print(f"[shape-sizing] {slug}/{shape}: wrote {len(diamonds)} rows",
              flush=True)
        results[shape] = ("ok", len(diamonds))
    return results


def main() -> int:
    target = sys.argv[1] if len(sys.argv) > 1 else "all"
    slugs = list(JOBS) if target == "all" else [target]
    if any(s not in JOBS for s in slugs):
        print(f"unknown retailer '{target}'; choose from {list(JOBS)} or all",
              file=sys.stderr)
        return 2

    all_results: dict[str, dict[str, tuple[str, int | None]]] = {}
    for slug in slugs:
        all_results[slug] = run_retailer(slug)

    print("\n[shape-sizing] summary:")
    failed = False
    for slug, shapes in all_results.items():
        for shape, (status, n) in shapes.items():
            print(f"  {slug}/{shape}: {status}"
                  + (f" ({n} rows)" if n is not None else ""))
            failed = failed or status == "failed"
        missing = [s for s in NEW_SHAPES if s not in shapes]
        if missing:
            print(f"  {slug}: NOT ATTEMPTED (aborted earlier): "
                  f"{', '.join(missing)}")
    return 1 if failed else 0


if __name__ == "__main__":
    _code = main()
    from snapshot import sync as _snapshot_sync
    _snapshot_sync()
    sys.exit(_code)
