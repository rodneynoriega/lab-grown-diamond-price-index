"""Run benchmark validation across all retailer CSVs for a given date.

Usage:
    python3 validate_all.py                    # uses today's date
    python3 validate_all.py --date 2026-04-23  # specific date
    python3 validate_all.py --date 2026-04-23 --round-only
"""

from __future__ import annotations

import csv
import statistics
import sys
from datetime import date
from pathlib import Path

from retailers.base import (
    CSV_FIELDS, VALIDATION_CELLS, ValidationCell,
    Diamond, diamond_to_row,
)

DATA_DIR = Path(__file__).parent / "data"
RAW_DIR = DATA_DIR / "raw"
VALIDATION_DIR = DATA_DIR / "validation"

THIN_THRESHOLD = 30

SLUG_TO_DISPLAY = {
    "blue_nile":        "Blue Nile",
    "brilliant_earth":  "Brilliant Earth",
    "clean_origin":     "Clean Origin",
    "grown_brilliance": "Grown Brilliance",
    "james_allen":      "James Allen",
    "ritani":           "Ritani",
    "vrai":             "VRAI",
    "with_clarity":     "With Clarity",
}


def load_csv(path: Path, display_name: str) -> list[Diamond]:
    diamonds: list[Diamond] = []
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                diamonds.append(Diamond.build(
                    retailer=display_name,
                    shape=row["shape"],
                    carat=float(row["carat"]),
                    color=row["color"] or None,
                    clarity=row["clarity"] or None,
                    cut=row["cut"] or None,
                    polish=row["polish"] or None,
                    symmetry=row["symmetry"] or None,
                    fluorescence=row["fluorescence"] or None,
                    certificate_lab=row["certificate_lab"] or None,
                    certificate_number=row["certificate_number"] or None,
                    price_usd=float(row["price_usd"]),
                    product_url=row["product_url"],
                    date=row["date"],
                    scraped_at=row["scraped_at"],
                ))
            except (KeyError, ValueError):
                continue
    return diamonds


def _cell_stones(diamonds: list[Diamond], cell: ValidationCell) -> list[Diamond]:
    return [
        d for d in diamonds
        if d.shape == cell.shape
        and cell.min_carat <= d.carat <= cell.max_carat
        and d.color == cell.color
        and d.clarity == cell.clarity
        and d.cut == cell.cut
        and (cell.cert_lab is None or d.certificate_lab == cell.cert_lab)
    ]


def build_validation(all_diamonds: list[Diamond], target_date: str, round_only: bool = False) -> str:
    VALIDATION_DIR.mkdir(parents=True, exist_ok=True)
    out_path = VALIDATION_DIR / f"cell_counts_{target_date}.md"

    cells = [c for c in VALIDATION_CELLS if c.shape == "round"] if round_only else VALIDATION_CELLS
    retailers = sorted({d.retailer for d in all_diamonds})

    lines = [
        f"# Benchmark Cell Validation — {target_date}",
        "",
        "**Thin threshold:** fewer than 30 stones = 'thin — do not publish'",
        "",
        "| retailer | cell | stone_count | median_price_usd | median_price_per_carat | p25 | p75 | status |",
        "|---|---|---|---|---|---|---|---|",
    ]

    for cell in cells:
        for retailer in retailers:
            stones = _cell_stones([d for d in all_diamonds if d.retailer == retailer], cell)
            n = len(stones)
            if n == 0:
                lines.append(f"| {retailer} | {cell.label} | 0 | — | — | — | — | no data |")
                continue
            prices = sorted(d.price_usd for d in stones)
            ppcs   = sorted(d.price_per_carat for d in stones)
            med_p  = statistics.median(prices)
            med_ppc= statistics.median(ppcs)
            p25_p  = prices[max(0, int(n * 0.25) - 1)]
            p75_p  = prices[min(n - 1, int(n * 0.75))]
            status = "OK" if n >= THIN_THRESHOLD else "thin — do not publish"
            lines.append(
                f"| {retailer} | {cell.label} | {n} "
                f"| ${med_p:,.0f} | ${med_ppc:,.0f} "
                f"| ${p25_p:,.0f} | ${p75_p:,.0f} | {status} |"
            )

    table = "\n".join(lines)
    with out_path.open("w", encoding="utf-8") as f:
        f.write(table + "\n")
    return table


def main() -> int:
    date_idx = next((i for i, a in enumerate(sys.argv) if a == "--date"), None)
    target_date = sys.argv[date_idx + 1] if date_idx is not None else date.today().isoformat()
    round_only = "--round-only" in sys.argv

    all_diamonds: list[Diamond] = []
    loaded: list[str] = []
    missing: list[str] = []

    for slug, display in sorted(SLUG_TO_DISPLAY.items()):
        path = RAW_DIR / f"{slug}_{target_date}.csv"
        if not path.exists():
            missing.append(slug)
            continue
        stones = load_csv(path, display)
        all_diamonds.extend(stones)
        loaded.append(f"  {display}: {len(stones):,} stones")

    print(f"Loaded CSVs for {target_date}:")
    print("\n".join(loaded))
    if missing:
        print(f"No CSV found for: {', '.join(missing)}")

    if not all_diamonds:
        print("No data loaded.", file=sys.stderr)
        return 1

    table = build_validation(all_diamonds, target_date, round_only=round_only)
    print("\n" + "=" * 70)
    print(table)
    return 0


if __name__ == "__main__":
    sys.exit(main())
