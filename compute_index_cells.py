#!/usr/bin/env python3
"""compute_index_cells.py: the published E VS1 benchmark cells, from raw CSVs.

Reproduces exactly how the monthly index-data.json `retailers[]` cells and
`history[]` entry are computed (verified 2026-08-29 against every July 2026
published figure, n and $/ct, all seven retailers):

  IGI retailers: round, E, VS1, cut Excellent (normalized), certificate_lab
    IGI, carat within the published band (1ct 0.95-1.05, 1.5ct 1.45-1.55,
    2ct 1.95-2.05); median of price_usd/carat, rounded.
  VRAI (non-IGI, in-house grading): shape round or round-brilliant, all
    clarities, windows 1ct 1.0-1.09 (D-F), 1.5ct 1.6-1.9 (D-G), 2ct 2.1-2.45
    (D-G); median $/ct plus the raw median total (median_total_price is
    used when the window's median weight diverges from the label).
  status: "ok" at n >= 30, else "thin" (n > 0) or "no_data".
  total_listings: rows in each retailer's MAIN cycle file, deduped by
    product_url within the retailer, summed (July: 208,903).

Usage:
  python3 compute_index_cells.py --cycle-date 2026-08-29 [--month "August 2026"]
    [--wc-files a.csv,b.csv]   # override With Clarity's file list (top-ups)
Prints a JSON fragment: {"total_listings", "retailers": [...], "history_entry"}.
Editorial fields (notes, key_findings, methodology) are NOT produced here.
"""
import argparse
import sys
import csv
import json
import math
import statistics
from pathlib import Path

RAW = Path(__file__).resolve().parent / "data" / "raw"
BANDS = {"1ct": (0.95, 1.05, 1.0), "1.5ct": (1.45, 1.55, 1.5), "2ct": (1.95, 2.05, 2.0)}
VRAI_WINDOWS = {"1ct": (1.0, 1.09, {"D", "E", "F"}, "1.0–1.09ct"),
                "1.5ct": (1.6, 1.9, {"D", "E", "F", "G"}, "1.6–1.9ct"),
                "2ct": (2.1, 2.45, {"D", "E", "F", "G"}, "2.1–2.45ct")}
PANEL = [("Ritani", "ritani"), ("Clean Origin", "clean_origin"),
         ("Grown Brilliance", "grown_brilliance"), ("With Clarity", "with_clarity"),
         ("Blue Nile", "blue_nile"), ("Brilliant Earth", "brilliant_earth"),
         ("VRAI", "vrai")]
THIN = 30


def load(files):
    rows = {}
    for fn in files:
        with open(fn, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                rows[r["product_url"]] = r
    return list(rows.values())


def med(v):
    return math.floor(statistics.median(v) + 0.5) if v else None  # half-up, matches the page JS


def cells_for(slug, rows):
    out = {}
    for band, (lo, hi, wt) in BANDS.items():
        if slug == "vrai":
            vlo, vhi, cols, rng = VRAI_WINDOWS[band]
            sel = [r for r in rows if r["shape"] in ("round", "round-brilliant")
                   and vlo <= float(r["carat"]) <= vhi and r["color"] in cols]
        else:
            sel = [r for r in rows if r["shape"] == "round" and lo <= float(r["carat"]) <= hi
                   and r["color"] == "E" and r["clarity"] == "VS1"
                   and r["cut"] == "Excellent" and r["certificate_lab"] == "IGI"]
        n = len(sel)
        ppc = med([float(r["price_usd"]) / float(r["carat"]) for r in sel])
        c = {"status": "ok" if n >= THIN else ("thin" if n else "no_data"), "stone_count": n}
        if n:
            c["median_price_per_carat"] = ppc
            c["_median_total_raw"] = med([float(r["price_usd"]) for r in sel])
            c["_median_weight"] = round(statistics.median([float(r["carat"]) for r in sel]), 2)
            c["_ppc_x_weight"] = math.floor(ppc * wt + 0.5)   # half-up, matches the page JS
        if slug == "vrai":
            c["actual_range"] = VRAI_WINDOWS[band][3]
        out[band] = c
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cycle-date", required=True)
    ap.add_argument("--month", default=None)
    ap.add_argument("--wc-files", default=None)
    args = ap.parse_args()
    d = args.cycle_date
    retailers, hist, total = [], {}, 0
    for name, slug in PANEL:
        if slug == "with_clarity" and args.wc_files:
            files = [RAW / f for f in args.wc_files.split(",")]
        else:
            files = [RAW / f"{slug}_{d}.csv"]
        missing = [str(f) for f in files if not f.exists()]
        if missing:
            print(f"MISSING {name}: {missing}", file=sys.stderr)
            continue
        rows = load(files)
        main_rows = load([RAW / f"{slug}_{d}.csv"]) if (RAW / f"{slug}_{d}.csv").exists() else rows
        total += len(main_rows)
        cells = cells_for(slug, rows)
        entry = {"name": name, "slug": slug, "cells": cells, "_rows": len(rows)}
        if slug == "vrai":
            entry["non_igi"] = True
            entry["approximate_windows"] = True
        retailers.append(entry)
        h = {b: (c.get("median_price_per_carat") if c["status"] == "ok" else None) for b, c in cells.items()}
        if slug == "vrai":
            h["non_igi"] = True
        hist[slug] = h
    print(json.dumps({"month": args.month, "collection_date": d, "total_listings": total,
                      "retailers": retailers,
                      "history_entry": {"month": args.month, "total_listings": total, "retailers": hist}},
                     indent=1, ensure_ascii=False))


if __name__ == "__main__":
    main()
