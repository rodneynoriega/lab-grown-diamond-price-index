"""Phase 3 aggregation for the shape-widening pilot (approved 2026-08-09).

Builds the shape x carat-point x retailer cell-count matrix from:
- NEW shapes (cushion/princess/emerald/pear/marquise/radiant): the Phase 2
  per-shape pulls, data/raw/{slug}_*_shape_{shape}.csv (BE + BN, 0.90-3.10ct).
- round/oval 0.90-2.50: the July cycle files ({slug}_2026-07-23.csv).
- round/oval 2.50-3.10: the Phase 1 ext3ct files ({slug}_2026-08-09_ext3ct.csv).

Cell definition matches the aggregate pages: carat point +/-0.09. Note this is
a hair narrower than Phase 1's 2.90-3.10 band at the 3ct point; stated in the
report. Counts are reported pooled (all colors/clarities) and strict
(E / VS1 / cut Excellent / IGI), mirroring the Phase 1 report.

Usage: python3 aggregate_shape_sizing.py > <report-body.md>
"""

from __future__ import annotations

import csv
import sys
from collections import defaultdict
from pathlib import Path

RAW = Path(__file__).parent / "data" / "raw"

NEW_SHAPES = ["cushion", "princess", "emerald", "pear", "marquise", "radiant"]
ALL_SHAPES = ["round", "oval"] + NEW_SHAPES
CARAT_POINTS = [1.00, 1.50, 2.00, 2.50, 3.00]
BAND = 0.09
N_RULE = 30

SOURCES = {
    "brilliant_earth": "BE",
    "blue_nile": "BN",
}


def load_rows(slug: str) -> tuple[list[dict], list[str]]:
    files: list[Path] = []
    files += sorted(RAW.glob(f"{slug}_*_shape_*.csv"))
    files += [RAW / f"{slug}_2026-07-23.csv", RAW / f"{slug}_2026-08-09_ext3ct.csv"]
    rows: list[dict] = []
    used: list[str] = []
    for path in files:
        if not path.exists():
            print(f"WARNING: missing {path.name}", file=sys.stderr)
            continue
        used.append(path.name)
        with path.open(newline="", encoding="utf-8") as f:
            rows.extend(csv.DictReader(f))
    return rows, used


def is_strict(r: dict) -> bool:
    return (r["color"] == "E" and r["clarity"] == "VS1"
            and r["cut"] == "Excellent" and r["certificate_lab"] == "IGI")


def main() -> None:
    # counts[(shape, point)][retailer] = [pooled, strict]
    counts: dict[tuple[str, float], dict[str, list[int]]] = defaultdict(
        lambda: defaultdict(lambda: [0, 0]))
    sources_used: dict[str, list[str]] = {}

    for slug, label in SOURCES.items():
        rows, used = load_rows(slug)
        sources_used[label] = used
        # ext3ct and July files overlap new-shape pulls only if a new-shape
        # pull ever re-ran; dedupe on (cert_lab, cert_number) where present,
        # else on product_url, within each retailer.
        seen: set[str] = set()
        for r in rows:
            shape = r["shape"].strip().lower()
            if shape not in ALL_SHAPES:
                continue
            try:
                carat = float(r["carat"])
            except ValueError:
                continue
            key = (r.get("certificate_number") or "").strip()
            key = f"{r.get('certificate_lab','')}:{key}" if key else r.get("product_url", "")
            if key and key in seen:
                continue
            if key:
                seen.add(key)
            for pt in CARAT_POINTS:
                if pt - BAND <= carat <= pt + BAND:
                    cell = counts[(shape, pt)][label]
                    cell[0] += 1
                    if is_strict(r):
                        cell[1] += 1

    print("### Source files used\n")
    for label, used in sources_used.items():
        print(f"- **{label}:** {', '.join(used)}")
    print()

    for mode, idx in (("pooled (all colors/clarities)", 0),
                      ("strict (E VS1 Excellent IGI)", 1)):
        print(f"### Cell counts, {mode}\n")
        print("| Shape | " + " | ".join(f"{p:.2f}ct" for p in CARAT_POINTS)
              + " |")
        print("|---" * (len(CARAT_POINTS) + 1) + "|")
        for shape in ALL_SHAPES:
            cells = []
            for pt in CARAT_POINTS:
                c = counts[(shape, pt)]
                be, bn = c["BE"][idx], c["BN"][idx]
                both = "BOTH" if be >= N_RULE and bn >= N_RULE else "no"
                cells.append(f"BE {be} / BN {bn} ({both})")
            print(f"| {shape} | " + " | ".join(cells) + " |")
        print()

    print("### Shapes ranked by cells where BE and BN BOTH clear n>=30\n")
    print("| Shape | Pooled cells (of 5) | Strict cells (of 5) |")
    print("|---|---|---|")
    ranked = []
    for shape in ALL_SHAPES:
        pooled = sum(1 for pt in CARAT_POINTS
                     if all(counts[(shape, pt)][l][0] >= N_RULE for l in ("BE", "BN")))
        strict = sum(1 for pt in CARAT_POINTS
                     if all(counts[(shape, pt)][l][1] >= N_RULE for l in ("BE", "BN")))
        ranked.append((shape, pooled, strict))
    for shape, pooled, strict in sorted(ranked, key=lambda t: (-t[1], -t[2])):
        print(f"| {shape} | {pooled} | {strict} |")
    print()


if __name__ == "__main__":
    main()
