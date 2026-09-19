"""Shape benchmark-matrix driver for the Ring Advisor KB (lab shape pricing).

Purpose: pull each panel retailer across all 8 shapes (0.90-2.50ct), cache the
raw stones, then filter to the KB benchmark spec (E / VS1 / IGI / Excellent) and
compute the median $/ct per shape at the 1 / 1.5 / 2ct bands. Output feeds the
"lab pricing by shape" delta into the Ring Advisor knowledge base.

Design notes:
- One broad pull per retailer returns ALL colors/clarities, so the benchmark
  filter runs in-driver — re-tune the spec without re-scraping.
- Raw stones are cached to JSON in the scratchpad so a crash/partial run is
  resumable and the aggregation step never needs a live network.
- Run sequentially, VPN off. Ritani 429s -> give it req_delay ~8s.

Usage:
    python3 shape_matrix_driver.py <retailer_slug|all> [req_delay]
    python3 shape_matrix_driver.py aggregate      # re-aggregate cached raw only
"""

from __future__ import annotations

import json
import statistics
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from retailers import (
    blue_nile,
    ritani,
    with_clarity,
    brilliant_earth,
    grown_brilliance,
    clean_origin,
)
from retailers.base import diamond_to_row

# --- Output location: scratchpad (data), driver itself stays in the git repo ---
OUT_DIR = Path(
    "/private/tmp/claude-501/-Users-rodneynoriega-My-Agents-Ring-Advisor"
    "/eba770b1-4fae-41ac-8525-e10d37f92c88/scratchpad/shape_matrix"
)
OUT_DIR.mkdir(parents=True, exist_ok=True)

ALL_8_SHAPES = [
    "round", "oval", "cushion", "princess",
    "emerald", "pear", "marquise", "radiant",
]

MIN_CARAT = 0.90
MAX_CARAT = 2.50

# Panel: slug -> (display name, scrape fn, recommended req_delay)
PANEL = {
    "blue_nile":        ("Blue Nile",        blue_nile.scrape,        1.0),
    "with_clarity":     ("With Clarity",     with_clarity.scrape,     1.5),
    "brilliant_earth":  ("Brilliant Earth",  brilliant_earth.scrape,  1.5),
    "grown_brilliance": ("Grown Brilliance", grown_brilliance.scrape, 1.5),
    "clean_origin":     ("Clean Origin",     clean_origin.scrape,     1.5),
    "ritani":           ("Ritani",           ritani.scrape,           8.0),  # 429s -> backoff
}

# --- KB benchmark spec (matches the live index: 1ct E VS1 Excellent IGI round) ---
SPEC_COLOR = "E"
SPEC_CLARITY = "VS1"
SPEC_CUT = "Excellent"
SPEC_CERT = "IGI"

# Carat bands (match base.py BENCHMARKS convention)
BANDS = {
    "1ct":   (1.00, 1.09),
    "1.5ct": (1.50, 1.59),
    "2ct":   (2.00, 2.19),
}


def raw_path(slug: str) -> Path:
    return OUT_DIR / f"raw_{slug}.json"


def _is_number(v) -> bool:
    try:
        float(v)
        return True
    except (ValueError, TypeError):
        return False


def _in_band(carat_val, band: tuple[float, float]) -> bool:
    try:
        c = float(carat_val)
    except (ValueError, TypeError):
        return False
    return band[0] <= c <= band[1]


def pull(slug: str, req_delay: float | None = None, only_bands: list[str] | None = None) -> int:
    name, fn, default_delay = PANEL[slug]
    delay = req_delay if req_delay is not None else default_delay
    bands = only_bands or list(BANDS)
    print(f"\n== {name} (req_delay={delay}s, bands={bands}) ==")
    # Pull only the three narrow benchmark bands, not the full 0.90-2.50 range.
    # Narrow bands keep every color slice under Blue Nile's 2,500-stone sharding
    # threshold (no 0.01ct windowing) and cut request volume ~10x panel-wide.
    out = raw_path(slug)
    # Merge mode: keep any existing cached rows OUTSIDE the bands we're re-pulling,
    # so a targeted re-pull of one flaky band (e.g. BN 2ct) folds into prior bands.
    rows: list[dict] = []
    if out.exists() and only_bands:
        existing = json.loads(out.read_text(encoding="utf-8"))
        target_ranges = [BANDS[b] for b in bands]
        rows = [r for r in existing
                if not any(_in_band(r.get("carat"), rng) for rng in target_ranges)]
        print(f"  merge: kept {len(rows)} existing rows outside {bands}")
    for band_name in bands:
        lo, hi = BANDS[band_name]
        try:
            diamonds = fn(ALL_8_SHAPES, lo, hi, req_delay=delay)
        except Exception as e:  # noqa: BLE001 - keep the run going per band/retailer
            print(f"  ERROR ({band_name} {lo}-{hi}): {e}", file=sys.stderr)
            continue
        band_rows = [diamond_to_row(d) for d in diamonds]
        print(f"  [{band_name}] {lo:.2f}-{hi:.2f}ct: {len(band_rows)} stones")
        rows.extend(band_rows)
        # Persist after EACH band so a later band failing (e.g. laptop sleep /
        # network drop) never discards bands already collected.
        out.write_text(json.dumps(rows, indent=None), encoding="utf-8")
    print(f"  cached {len(rows)} stones total -> {out.name}")
    return len(rows)


def _matches_spec(row: dict, band: tuple[float, float], loosen_clarity: bool) -> bool:
    if (row.get("color") or "").upper() != SPEC_COLOR:
        return False
    clarity = (row.get("clarity") or "").upper()
    if loosen_clarity:
        if clarity not in ("VS1", "VS2"):
            return False
    elif clarity != SPEC_CLARITY:
        return False
    if (row.get("cut") or "") != SPEC_CUT:
        return False
    if (row.get("certificate_lab") or "").upper() != SPEC_CERT:
        return False
    try:
        carat = float(row["carat"])
    except (KeyError, ValueError, TypeError):
        return False
    return band[0] <= carat <= band[1]


def _load_all_raw() -> dict[str, list[dict]]:
    data: dict[str, list[dict]] = {}
    for slug in PANEL:
        p = raw_path(slug)
        if p.exists():
            data[slug] = json.loads(p.read_text(encoding="utf-8"))
    return data


def aggregate(loosen_clarity: bool = False) -> dict:
    data = _load_all_raw()
    if not data:
        print("No cached raw pulls found. Run a pull first.", file=sys.stderr)
        return {}

    tag = "VS1+VS2" if loosen_clarity else "VS1"
    print(f"\n### Benchmark medians (spec: {SPEC_COLOR} / {tag} / {SPEC_CUT} / {SPEC_CERT}) ###")
    print(f"Cached retailers: {', '.join(sorted(data))}\n")

    result: dict = {"spec": f"{SPEC_COLOR}/{tag}/{SPEC_CUT}/{SPEC_CERT}", "shapes": {}}

    hdr = f"{'shape':<9} " + " ".join(f"{b:>22}" for b in BANDS)
    print(hdr)
    print("-" * len(hdr))

    for shape in ALL_8_SHAPES:
        result["shapes"][shape] = {}
        cells = []
        for band_name, band in BANDS.items():
            # Compute a median per RETAILER first, then take the median of those
            # retailer-medians. Retailers differ ~2x in price level (Ritani floor
            # vs Brilliant Earth ceiling) and have different inventory per shape,
            # so pooling raw stones lets whichever retailer has more listings in a
            # shape drag that shape's number toward its own price level — a fake
            # cross-shape signal. Equal-weighting retailers isolates the shape effect.
            retailer_meds: dict[str, int] = {}
            retailer_n: dict[str, int] = {}
            for slug, rows in data.items():
                ppc = [float(row["price_per_carat"]) for row in rows
                       if (row.get("shape") or "").lower() == shape
                       and _matches_spec(row, band, loosen_clarity)
                       and _is_number(row.get("price_per_carat"))]
                if ppc:
                    retailer_meds[slug] = round(statistics.median(ppc))
                    retailer_n[slug] = len(ppc)
            if retailer_meds:
                med = round(statistics.median(list(retailer_meds.values())))
                result["shapes"][shape][band_name] = {
                    "panel_median_ppc": med,
                    "retailers": len(retailer_meds),
                    "per_retailer_median": retailer_meds,
                    "per_retailer_n": retailer_n,
                }
                cells.append(f"${med:>6,}/ct r={len(retailer_meds)}")
            else:
                result["shapes"][shape][band_name] = None
                cells.append(f"{'-- no data --':>18}")
        print(f"{shape:<9} " + " ".join(f"{c:>18}" for c in cells))

    out = OUT_DIR / f"benchmark_medians_{'loose' if loosen_clarity else 'strict'}.json"
    out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"\nSaved -> {out}")
    return result


if __name__ == "__main__":
    arg = sys.argv[1] if len(sys.argv) > 1 else "all"
    # Remaining args: floats -> req_delay, band names (1ct/1.5ct/2ct) -> only_bands.
    delay_arg: float | None = None
    band_args: list[str] = []
    for extra in sys.argv[2:]:
        if extra in BANDS:
            band_args.append(extra)
        else:
            try:
                delay_arg = float(extra)
            except ValueError:
                print(f"Ignoring unknown arg '{extra}'", file=sys.stderr)
    only_bands = band_args or None

    if arg == "aggregate":
        aggregate(loosen_clarity=False)
        aggregate(loosen_clarity=True)
    elif arg == "all":
        for slug in PANEL:
            pull(slug)
        aggregate()
    elif arg in PANEL:
        pull(arg, delay_arg, only_bands)
    else:
        print(f"Unknown target '{arg}'. Options: {', '.join(PANEL)}, all, aggregate")
        sys.exit(1)
