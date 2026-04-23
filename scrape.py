"""Orchestrator: scrape full inventory per retailer, save CSVs, run validation.

Usage:
    python3 scrape.py                  # scrape all retailers
    python3 scrape.py --validate-only  # skip scraping, rerun validation on today's raw files

Output:
    data/raw/{retailer}_{YYYY-MM-DD}.csv         -- one stone per row, per retailer per day
    data/processed/index_{YYYY-MM-DD}.csv        -- combined, deduplicated
    data/validation/cell_counts_{YYYY-MM-DD}.md  -- benchmark cell validation table
"""

from __future__ import annotations

import csv
import statistics
import sys
import time
from datetime import date
from pathlib import Path

from retailers import brilliant_earth, blue_nile, clean_origin, vrai, with_clarity

from retailers.base import (
    CSV_FIELDS, VALIDATION_CELLS, ValidationCell,
    TARGET_SHAPES, MIN_CARAT, MAX_CARAT,
    Diamond, diamond_to_row,
)

DATA_DIR = Path(__file__).parent / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
VALIDATION_DIR = DATA_DIR / "validation"

# Polite request delay between pages (seconds). Override via --delay N.
DEFAULT_DELAY = 1.0

# Minimum stone count for a cell to be considered publishable.
THIN_THRESHOLD = 30

RETAILERS: list[tuple[str, str, object]] = [
    # (display_name, slug_for_filename, scrape_fn)
    ("Brilliant Earth", "brilliant_earth", brilliant_earth.scrape),
    ("Blue Nile",       "blue_nile",       blue_nile.scrape),
    ("With Clarity",    "with_clarity",    with_clarity.scrape),
    ("VRAI",            "vrai",            vrai.scrape),
    ("Clean Origin",    "clean_origin",    clean_origin.scrape),
]


# ---------------------------------------------------------------------------
# Scrape phase
# ---------------------------------------------------------------------------

def _raw_path(slug: str, today: str) -> Path:
    return RAW_DIR / f"{slug}_{today}.csv"


def _load_raw(slug: str, today: str) -> list[Diamond] | None:
    """Return diamonds from today's raw file if it exists, else None."""
    p = _raw_path(slug, today)
    if not p.exists():
        return None
    diamonds: list[Diamond] = []
    with p.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                diamonds.append(
                    Diamond.build(
                        retailer=row["retailer"],
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
                    )
                )
            except (KeyError, ValueError):
                continue
    return diamonds


def _load_co_detail_cache() -> dict[str, dict]:
    """Build a url-keyed cache of Clean Origin detail fields from all prior raw CSVs.

    Avoids re-fetching detail pages for stones already scraped on a prior day.
    Only carries forward rows where at least one detail field is non-null.
    """
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
                    # Only cache rows that actually have detail data.
                    if any(v is not None for v in detail.values()):
                        cache[url] = detail
        except Exception:
            continue
    print(f"  [clean_origin] detail cache loaded: {len(cache)} entries from prior runs")
    return cache


def run_scrape(
    today: str,
    req_delay: float = DEFAULT_DELAY,
    retailer_filter: set[str] | None = None,
) -> dict[str, list[Diamond]]:
    """Scrape retailers and save raw CSVs. Returns {slug: [Diamond, ...]}.

    retailer_filter: if provided, only scrape slugs in this set.
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    # Load CO detail cache once before the retailer loop.
    co_detail_cache: dict[str, dict] = _load_co_detail_cache()

    results: dict[str, list[Diamond]] = {}

    for display_name, slug, scrape_fn in RETAILERS:
        if retailer_filter and slug not in retailer_filter:
            continue
        existing = _load_raw(slug, today)
        if existing is not None:
            print(f"[{display_name}] already scraped today ({len(existing)} stones), skipping")
            results[slug] = existing
            continue

        print(f"\n== {display_name} ==")
        try:
            if slug == "clean_origin":
                diamonds = scrape_fn(
                    TARGET_SHAPES, MIN_CARAT, MAX_CARAT,
                    req_delay=req_delay,
                    detail_cache=co_detail_cache,
                )
            else:
                diamonds = scrape_fn(TARGET_SHAPES, MIN_CARAT, MAX_CARAT, req_delay=req_delay)
        except Exception as e:
            print(f"  ERROR: {e}", file=sys.stderr)
            results[slug] = []
            # Let the OS network stack recover after a timeout or connection error
            # before starting the next retailer.
            print("  Waiting 30s for network recovery before next retailer...")
            time.sleep(30)
            continue

        p = _raw_path(slug, today)
        with p.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            writer.writeheader()
            writer.writerows([diamond_to_row(d) for d in diamonds])
        print(f"  Saved {len(diamonds)} rows -> {p.name}")
        results[slug] = diamonds

    return results


# ---------------------------------------------------------------------------
# Processed index
# ---------------------------------------------------------------------------

def build_processed(all_diamonds: list[Diamond], today: str) -> Path:
    """Combine all retailers into one deduplicated CSV sorted by retailer + price."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    out = PROCESSED_DIR / f"index_{today}.csv"

    seen: set[str] = set()
    rows: list[dict] = []
    for d in sorted(all_diamonds, key=lambda x: (x.retailer, x.price_usd)):
        key = f"{d.retailer}|{d.product_url}"
        if key in seen:
            continue
        seen.add(key)
        rows.append(diamond_to_row(d))

    with out.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nProcessed index: {len(rows)} stones -> {out.name}")
    return out


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def _cut_matches(stone_cut: str | None, cell_cut: str) -> bool:
    """A stone's cut matches the cell if stone_cut == cell_cut OR
    stone_cut is 'Excellent' (which covers Super Ideal / Ideal as normalized)."""
    if stone_cut is None:
        return False
    return stone_cut == cell_cut


def _cell_stones(diamonds: list[Diamond], cell: ValidationCell) -> list[Diamond]:
    matches: list[Diamond] = []
    for d in diamonds:
        if d.shape != cell.shape:
            continue
        if not (cell.min_carat <= d.carat <= cell.max_carat):
            continue
        if d.color != cell.color:
            continue
        if d.clarity != cell.clarity:
            continue
        if not _cut_matches(d.cut, cell.cut):
            continue
        if cell.cert_lab is not None and d.certificate_lab != cell.cert_lab:
            continue
        matches.append(d)
    return matches


def build_validation(all_diamonds: list[Diamond], today: str) -> str:
    """Compute cell stats and return the markdown table as a string."""
    VALIDATION_DIR.mkdir(parents=True, exist_ok=True)
    out_path = VALIDATION_DIR / f"cell_counts_{today}.md"

    retailers = sorted({d.retailer for d in all_diamonds})

    lines: list[str] = [
        f"# Benchmark Cell Validation — {today}",
        "",
        "**Thin threshold:** fewer than 30 stones = 'thin — do not publish'",
        "",
    ]

    header = "| retailer | cell | stone_count | median_price_usd | median_price_per_carat | p25 | p75 | status |"
    sep    = "|---|---|---|---|---|---|---|---|"
    lines += [header, sep]

    for cell in VALIDATION_CELLS:
        for retailer in retailers:
            retailer_diamonds = [d for d in all_diamonds if d.retailer == retailer]
            stones = _cell_stones(retailer_diamonds, cell)
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
    print(f"\nValidation table: {out_path.name}")
    return table


# ---------------------------------------------------------------------------
# Field coverage report
# ---------------------------------------------------------------------------

def field_coverage(all_diamonds: list[Diamond]) -> str:
    fields = ["shape", "carat", "color", "clarity", "cut", "polish", "symmetry",
              "fluorescence", "certificate_lab", "certificate_number"]
    retailers = sorted({d.retailer for d in all_diamonds})
    lines = ["| field | " + " | ".join(retailers) + " |",
             "|---|" + "---|" * len(retailers)]
    for f in fields:
        row = [f]
        for r in retailers:
            stones = [d for d in all_diamonds if d.retailer == r]
            have = sum(1 for d in stones if getattr(d, f if f != "carat" else "carat") is not None)
            pct = int(100 * have / len(stones)) if stones else 0
            row.append(f"{pct}%" if pct < 100 else "YES")
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    validate_only = "--validate-only" in sys.argv
    delay_idx = next((i for i, a in enumerate(sys.argv) if a == "--delay"), None)
    req_delay = float(sys.argv[delay_idx + 1]) if delay_idx is not None else DEFAULT_DELAY

    # --retailers slug1,slug2  — restrict scraping/loading to named slugs
    retailers_idx = next((i for i, a in enumerate(sys.argv) if a == "--retailers"), None)
    retailer_filter: set[str] | None = None
    if retailers_idx is not None:
        retailer_filter = {s.strip() for s in sys.argv[retailers_idx + 1].split(",")}

    # --date YYYY-MM-DD  — override the date used for file paths
    date_idx = next((i for i, a in enumerate(sys.argv) if a == "--date"), None)
    today = sys.argv[date_idx + 1] if date_idx is not None else date.today().isoformat()

    if not validate_only:
        raw_by_slug = run_scrape(today, req_delay=req_delay, retailer_filter=retailer_filter)
        all_diamonds: list[Diamond] = [d for stones in raw_by_slug.values() for d in stones]
    else:
        all_diamonds = []
        for _, slug, _ in RETAILERS:
            if retailer_filter and slug not in retailer_filter:
                continue
            existing = _load_raw(slug, today)
            if existing:
                all_diamonds.extend(existing)
            else:
                print(f"[{slug}] no raw file for {today}, skipping", file=sys.stderr)

    if not all_diamonds:
        print("No diamonds loaded.", file=sys.stderr)
        return 1

    build_processed(all_diamonds, today)
    table = build_validation(all_diamonds, today)

    print("\n" + "=" * 70)
    print(table)

    print("\n--- Field Coverage ---")
    print(field_coverage(all_diamonds))

    return 0


if __name__ == "__main__":
    sys.exit(main())
