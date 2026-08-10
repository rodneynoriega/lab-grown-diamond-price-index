#!/usr/bin/env python3
"""page_pipeline: deterministic pilot page generation (work-queue item #6).

Pipeline: raw scrape CSVs (data/raw) -> per-stone parquet with normalized
lab+cert identity (data/processed) -> Jinja2 templates (templates/) ->
page records: HTML bodies + QA manifest (publish_gate.py docstring format)
+ keyword log + canary rollback spec. This script NEVER touches the
Shopify API; drafts are created separately (canary_create_drafts.py) and
publishing exists nowhere in this pipeline.

Binding scope rules implemented here (2026-08-09 decisions):
  - WC-BLIND: With Clarity appears in NO displayed data, NO selection
    input, NO ranking input (accepted-risk memo condition 1). WC rows are
    kept in the internal parquet (private, gitignored) but every selection
    and every rendered figure uses displayed retailers only.
  - Stone selection: original tier rule (4-retailer stones + 3-retailer/
    4-cycle stones, July-listed) INTERSECT >= MIN_DISPLAYED displayed
    retailers holding current-cycle prices (the strict 1,927 pool of the
    2026-08-09 WC-exclusion recompute), ranked by displayed-retailer
    price spread (WC-blind), top TARGET_STONE_PAGES.
  - Aggregate pages: approved titling scheme families A, B, D, E only.
    Family C (brand-in-title) is HELD BACK (condition 2) and is not
    implemented. The 3ct page is data-gated and not built.
  - Titles/H1s/metas exactly per the approved scheme
    (OUTBOX/2026-08-09-retitle-proposal-and-a5-first-response.md);
    "Price Index" appears only as the in-page brand line.
  - Cert-matched cross-retailer comparison only on stone pages; loose
    stones only, no blended ring-total pricing anywhere.
  - Per-retailer honest delist-window labels on every page.
  - James Allen appears nowhere (Block 3 deferred; claim omitted).
  - Aggregate cells state their exact carat band; the pooled-cell
    statistic is explicitly distinguished from the E VS1 benchmark.

Usage:
  python3 page_pipeline.py build [--out page_output] [--canary N]

Outputs (all under --out, gitignored: page bodies carry listing-level data):
  pages/<handle>.html            rendered Shopify body_html fragments
  qa-manifest.json               all pages, publish_gate record format
  qa-manifest-canary.json        canary subset
  canary-rollback-spec.json      rollback_manifest.py snapshot spec
  canary-meta.json               per-page SEO metafields for the drafts
  keyword-log.csv                machine-readable title->keyword log (all)
  keyword-log-aggregate.csv      aggregate-page subset (committable: no
                                 stone/cert rows)
  selection-report.json          pool sizes, ranking stats, page counts

Determinism: fixed input file lists, stable sorts everywhere, prices
rounded once. Re-running on the same raw files reproduces byte-identical
records (rendered HTML embeds no clock time).
"""

import argparse
import csv
import json
import re
import statistics
from collections import defaultdict
from pathlib import Path

import pandas as pd
from jinja2 import Environment, FileSystemLoader, select_autoescape

HERE = Path(__file__).resolve().parent
RAW = HERE / "data" / "raw"
PROCESSED = HERE / "data" / "processed"
TEMPLATES = HERE / "templates"

# ----------------------------------------------------------------- cycle
CYCLE = "2026-07"
CYCLE_LABEL = "July 2026"
DATA_TIMESTAMP = "2026-07-23"      # capture date of every July source file
ALL_CYCLES = ["2026-04", "2026-05", "2026-06", "2026-07"]
CYCLE_LABELS = {"2026-04": "April 2026", "2026-05": "May 2026",
                "2026-06": "June 2026", "2026-07": "July 2026"}

# One file per retailer per cycle (verified: July band files are exact
# subsets of the July main files). James Allen deliberately absent
# (retired May 2026; Block 3 deferred; JA appears nowhere on pilot pages).
CYCLE_FILES = {
    "2026-04": {
        "Brilliant Earth": "brilliant_earth_2026-04-22.csv",
        "Blue Nile": "blue_nile_2026-04-22.csv",
        "Clean Origin": "clean_origin_2026-04-22.csv",
        "VRAI": "vrai_2026-04-22.csv",
        "Ritani": "ritani_2026-04-23.csv",
        "Grown Brilliance": "grown_brilliance_2026-04-23.csv",
        "With Clarity": "with_clarity_2026-04-22.csv",
    },
    "2026-05": {
        "Brilliant Earth": "brilliant_earth_2026-05-20.csv",
        "Blue Nile": "blue_nile_2026-05-20.csv",
        "Clean Origin": "clean_origin_2026-05-20.csv",
        "VRAI": "vrai_2026-05-20.csv",
        "Ritani": "ritani_2026-05-18.csv",
        "Grown Brilliance": "grown_brilliance_2026-05-18.csv",
        "With Clarity": "with_clarity_2026-05-18.csv",
    },
    "2026-06": {
        "Brilliant Earth": "brilliant_earth_2026-06-27.csv",
        "Blue Nile": "blue_nile_2026-06-27.csv",
        "Clean Origin": "clean_origin_2026-06-27.csv",
        "VRAI": "vrai_2026-06-27.csv",
        "Ritani": "ritani_2026-06-27_partial.csv",
        "Grown Brilliance": "grown_brilliance_2026-06-27.csv",
        "With Clarity": "with_clarity_2026-06-27.csv",
    },
    "2026-07": {
        "Brilliant Earth": "brilliant_earth_2026-07-23.csv",
        "Blue Nile": "blue_nile_2026-07-23.csv",
        "Clean Origin": "clean_origin_2026-07-23.csv",
        "VRAI": "vrai_2026-07-23.csv",
        "Ritani": "ritani_2026-07-23.csv",
        "Grown Brilliance": "grown_brilliance_2026-07-23.csv",
        "With Clarity": "with_clarity_2026-07-23.csv",
    },
}

# Additional cert-retailer files fed ONLY into the per-stone table, so
# stone identity/cycle coverage matches the 2026-08-09 recompute exactly
# (which globbed every non-quarantined file). Aggregates deliberately do
# NOT read these: the one-file-per-retailer-per-cycle map above is the
# double-count-safe basis for pooled statistics. July band files are
# verified exact subsets of the July main files (harmless; kept for
# completeness). The 2026-08-09 ext3ct files are deliberately excluded
# (Phase 1 carat extension, a different cycle, still running).
STONE_EXTRA_FILES = {
    "2026-05": {
        "Brilliant Earth": ["brilliant_earth_2026-05-18.csv"],
        "Grown Brilliance": ["grown_brilliance_2026-05-18_partial.csv"],
    },
    "2026-06": {
        "Grown Brilliance": ["grown_brilliance_2026-06-27_partial.csv",
                             "grown_brilliance_2026-06-27_partial2.csv"],
        "With Clarity": ["with_clarity_2026-06-27_15ct.csv",
                         "with_clarity_2026-06-27_1ct.csv",
                         "with_clarity_2026-06-27_2ct.csv"],
    },
    "2026-07": {
        "Ritani": ["ritani_2026-07-23_band1.csv",
                   "ritani_2026-07-23_band2.csv",
                   "ritani_2026-07-23_band3.csv"],
        "Grown Brilliance": ["grown_brilliance_2026-07-23_band1.csv",
                             "grown_brilliance_2026-07-23_band2.csv",
                             "grown_brilliance_2026-07-23_band3.csv"],
        "With Clarity": ["with_clarity_2026-07-25_15ct.csv",
                         "with_clarity_2026-07-25_15ct_partial1.csv"],
    },
}

# ------------------------------------------------------------- retailers
# Condition 1 (accepted-risk memo, signed 2026-08-09): With Clarity is
# EXCLUDED from all displayed data. It exists below ONLY so the internal
# parquet retains its rows; nothing rendered or ranked ever reads them.
WC = "With Clarity"

# Retailers whose scrape rows carry third-party cert numbers (stone pages).
CERT_RETAILERS = ["Brilliant Earth", "Grown Brilliance", "Ritani", WC]
# Displayed stone-page panel: cert-capable minus WC. Clean Origin joins
# automatically once a cycle file carries cert numbers (fixed forward
# from 2026-08-08; the July file predates the fix).
STONE_DISPLAY = ["Brilliant Earth", "Clean Origin", "Grown Brilliance",
                 "Ritani"]
# Aggregate-page panel: everything scraped minus WC.
AGG_DISPLAY = ["Blue Nile", "Brilliant Earth", "Clean Origin",
               "Grown Brilliance", "Ritani", "VRAI"]

MIN_DISPLAYED = 2          # stone page needs >= 2 displayed current prices
MIN_CELL_N = 30            # aggregate n-rule (per page and per retailer row)
MIN_CELL_RETAILERS = 3     # aggregate cell needs >= 3 retailers at n >= 30
TARGET_STONE_PAGES = 1000

CARAT_BAND = 0.09          # aggregate cell = carat point +/- 0.09 (stated
                           # in-page; widest non-overlapping band at the
                           # audit's 0.25ct point spacing)

INDEX_URL = "/pages/lab-grown-diamond-price-index"
BRAND_LINE = ("Source: the Rings.com Lab-Grown Diamond Price Index. "
              "Methodology and monthly benchmark")

# Approved Family A/B page grid (2026-08-09 titling scheme; 3ct page
# data-gated, round 1.25 / oval 1.75 failed the coverage audit).
FAMILY_A_POINTS = [1.0, 1.5, 2.0]
FAMILY_B_CELLS = [("round", c) for c in (1.0, 1.5, 1.75, 2.0, 2.25, 2.5)] + \
                 [("oval", c) for c in (1.0, 1.25, 1.5, 2.0, 2.25, 2.5)]

FAMILY_A_VOLUME = {1.0: "1,600/mo", 1.5: "320/mo", 2.0: "1,000/mo"}


# ------------------------------------------------------------ formatting

def usd(x):
    return "${:,.0f}".format(round(x))


def carat_str(c):
    s = ("%.2f" % c).rstrip("0").rstrip(".")
    return s


def carat_slug(c):
    return carat_str(c).replace(".", "-")


def norm_cert(lab, num):
    lab = (lab if isinstance(lab, str) else "").strip().upper()
    num = (num if isinstance(num, str) else "").strip().upper()
    if not num:
        return None
    if num.startswith("LG"):
        num = num[2:]
    num = num.strip()
    if not num or not re.search(r"\d", num):
        return None
    return lab, num


# ------------------------------------------------------------- data load

def load_frame():
    """All cycles, one file per retailer per cycle, deduped by
    (retailer, product_url) keeping the last row (latest scraped_at in
    file order)."""
    parts = []
    for cycle, files in CYCLE_FILES.items():
        for retailer, fn in files.items():
            p = RAW / fn
            if not p.exists():
                raise SystemExit(f"missing source file: {p}")
            df = pd.read_csv(p, dtype=str)
            df["cycle"] = cycle
            df["retailer"] = retailer     # normalize retailer naming
            df["source_file"] = f"data/raw/{fn}"
            parts.append(df)
    f = pd.concat(parts, ignore_index=True)
    f["carat"] = pd.to_numeric(f["carat"], errors="coerce")
    f["price_usd"] = pd.to_numeric(f["price_usd"], errors="coerce")
    f = f[(f["price_usd"] > 0) & (f["carat"] > 0)]
    f["shape"] = f["shape"].fillna("").str.strip().str.lower()
    f = f.drop_duplicates(subset=["retailer", "cycle", "product_url"],
                          keep="last")
    return f.reset_index(drop=True)


def build_stone_parquet(frame):
    """Long-format per-stone table for cert retailers, normalized lab+cert
    identity, one row per (cert, retailer, cycle): latest scraped_at row,
    tiebreak lowest price. Includes WC rows (internal only; private dir)."""
    extras = []
    for cycle, by_ret in STONE_EXTRA_FILES.items():
        for retailer, fns in by_ret.items():
            for fn in fns:
                p = RAW / fn
                if not p.exists():
                    raise SystemExit(f"missing source file: {p}")
                df = pd.read_csv(p, dtype=str)
                df["cycle"] = cycle
                df["retailer"] = retailer
                df["source_file"] = f"data/raw/{fn}"
                extras.append(df)
    ex = pd.concat(extras, ignore_index=True)
    ex["carat"] = pd.to_numeric(ex["carat"], errors="coerce")
    ex["price_usd"] = pd.to_numeric(ex["price_usd"], errors="coerce")
    ex = ex[(ex["price_usd"] > 0) & (ex["carat"] > 0)]
    ex["shape"] = ex["shape"].fillna("").str.strip().str.lower()
    f = pd.concat([frame[frame["retailer"].isin(CERT_RETAILERS)], ex],
                  ignore_index=True)
    keys = f.apply(lambda r: norm_cert(r.get("certificate_lab"),
                                       r.get("certificate_number")), axis=1)
    f = f[keys.notna()].copy()
    f["cert_lab"] = [k[0] for k in keys.dropna()]
    f["cert_number"] = [k[1] for k in keys.dropna()]
    f["cert_key"] = f["cert_lab"] + ":" + f["cert_number"]
    f = f.sort_values(["cert_key", "retailer", "cycle", "scraped_at",
                       "price_usd"],
                      ascending=[True, True, True, True, False])
    f = f.drop_duplicates(subset=["cert_key", "retailer", "cycle"],
                          keep="last")
    cols = ["cert_key", "cert_lab", "cert_number", "retailer", "cycle",
            "carat", "shape", "color", "clarity", "cut", "polish",
            "symmetry", "fluorescence", "price_usd", "price_per_carat",
            "product_url", "scraped_at", "source_file"]
    out = f[cols].sort_values(["cert_key", "retailer", "cycle"]) \
                 .reset_index(drop=True)
    PROCESSED.mkdir(parents=True, exist_ok=True)
    pq = PROCESSED / f"stones_pilot_{CYCLE}.parquet"
    out.to_parquet(pq, index=False)
    return out, pq


# -------------------------------------------------------- stone selection

def select_stones(stones):
    """Strict pool per the 2026-08-09 WC-exclusion recompute, ranked
    WC-blind by displayed-retailer July price spread."""
    by_key = defaultdict(dict)      # cert_key -> retailer -> row (July)
    cycles = defaultdict(set)
    retailers_ever = defaultdict(set)
    for r in stones.itertuples():
        cycles[r.cert_key].add(r.cycle)
        retailers_ever[r.cert_key].add(r.retailer)
        if r.cycle == CYCLE:
            by_key[r.cert_key][r.retailer] = r

    allc = set(ALL_CYCLES)
    tier = [k for k in by_key
            if len(retailers_ever[k]) == 4
            or (len(retailers_ever[k]) == 3 and allc <= cycles[k])]
    strict, mismatched = [], []
    for k in sorted(tier):
        disp = {ret: row for ret, row in by_key[k].items()
                if ret in STONE_DISPLAY}
        if len(disp) < MIN_DISPLAYED:
            continue
        # Spec-consistency guard: a "same stone" page is only honest if
        # the displayed retailers agree on the certificate-backed specs.
        # Spread ranking adversely selects for cert-merge mistakes (a
        # wrong merge shows a huge fake spread), so disagreement on
        # shape/carat/color/clarity EXCLUDES the stone from selection.
        # Retailer-assigned fields (cut/polish/symmetry, not always
        # cert-backed on fancy shapes) may differ; handled at render.
        rows = list(disp.values())
        def _s(v):
            return str(v).strip().upper() if isinstance(v, str) else ""
        agree = (len({_s(r.shape) for r in rows}) == 1
                 and len({_s(r.color) for r in rows}) == 1
                 and len({_s(r.clarity) for r in rows}) == 1
                 and max(r.carat for r in rows)
                 - min(r.carat for r in rows) <= 0.011)
        if not agree:
            mismatched.append(k)
            continue
        prices = sorted(row.price_usd for row in disp.values())
        spread = prices[-1] - prices[0]
        pct = spread / prices[0] if prices[0] else 0.0
        strict.append({"cert_key": k, "displayed": disp,
                       "spread": spread, "spread_pct": pct,
                       "cycles": sorted(cycles[k]),
                       "n_displayed": len(disp)})
    strict.sort(key=lambda s: (-s["spread"], -s["spread_pct"],
                               s["cert_key"]))
    return strict, mismatched


# ----------------------------------------------------------- aggregates

def july_agg_frame(frame):
    f = frame[(frame["cycle"] == CYCLE)
              & (frame["retailer"].isin(AGG_DISPLAY))]
    return f


def cell_stats(f, shape, point):
    lo, hi = round(point - CARAT_BAND, 2), round(point + CARAT_BAND, 2)
    cell = f[(f["carat"] >= lo) & (f["carat"] <= hi)]
    if shape:
        cell = cell[cell["shape"] == shape]
    rows = []
    for ret in AGG_DISPLAY:
        sub = cell[cell["retailer"] == ret]["price_usd"]
        rows.append({"retailer": ret, "n": int(len(sub)),
                     "median": float(sub.median()) if len(sub) else None})
    shown = [r for r in rows if r["n"] >= MIN_CELL_N]
    # Page-level stats pool ONLY retailers clearing the n-rule, so the
    # stated "n listings across m retailers" is exactly the displayed set.
    pooled = cell[cell["retailer"].isin([r["retailer"] for r in shown])]
    prices = pooled["price_usd"]
    return {
        "shape": shape, "point": point, "lo": lo, "hi": hi,
        "n": int(len(pooled)),
        "median": float(prices.median()) if len(pooled) else None,
        "p10": float(prices.quantile(0.10)) if len(pooled) else None,
        "p90": float(prices.quantile(0.90)) if len(pooled) else None,
        "median_ppc": float((prices / pooled["carat"]).median())
        if len(pooled) else None,
        "retailer_rows": shown,
        "below_min": [r["retailer"] for r in rows if 0 < r["n"] < MIN_CELL_N],
        "shown_retailers": [r["retailer"] for r in shown],
        "passes": len(shown) >= MIN_CELL_RETAILERS and len(pooled) >= MIN_CELL_N,
    }


def monthly_history(frame, shape, point):
    lo, hi = round(point - CARAT_BAND, 2), round(point + CARAT_BAND, 2)
    hist = []
    for cyc in ALL_CYCLES:
        f = frame[(frame["cycle"] == cyc)
                  & (frame["retailer"].isin(AGG_DISPLAY))
                  & (frame["carat"] >= lo) & (frame["carat"] <= hi)]
        if shape:
            f = f[f["shape"] == shape]
        n = int(len(f))
        hist.append({
            "cycle": cyc, "label": CYCLE_LABELS[cyc], "n": n,
            "median": float(f["price_usd"].median()) if n else None,
            "retailers": int(f["retailer"].nunique()),
        })
    return hist


def repricing_stats(stones):
    """Same-stone listing-price change April -> July, displayed retailers
    only (composition-free: identical certificate, identical retailer)."""
    f = stones[stones["retailer"].isin(STONE_DISPLAY)]
    apr = f[f["cycle"] == "2026-04"].set_index(["cert_key", "retailer"])
    jul = f[f["cycle"] == CYCLE].set_index(["cert_key", "retailer"])
    both = apr.join(jul, how="inner", lsuffix="_apr", rsuffix="_jul")
    changes = ((both["price_usd_jul"] - both["price_usd_apr"])
               / both["price_usd_apr"]).dropna()
    apr_keys = set(apr.index.get_level_values(0))
    jul_keys = set(jul.index.get_level_values(0))
    gone = apr_keys - jul_keys
    return {
        "n_pairs": int(len(changes)),
        "median_change_pct": float(changes.median() * 100)
        if len(changes) else None,
        "share_down_pct": float((changes < 0).mean() * 100)
        if len(changes) else None,
        "n_april_stones": len(apr_keys),
        "delisted_share_pct": (100.0 * len(gone) / len(apr_keys))
        if apr_keys else None,
    }


# ------------------------------------------------------------- rendering

def jenv():
    env = Environment(loader=FileSystemLoader(str(TEMPLATES)),
                      autoescape=select_autoescape(["html"]),
                      trim_blocks=True, lstrip_blocks=True)
    env.filters["usd"] = usd
    env.filters["carat"] = carat_str
    return env


def dataset_ld(name, desc):
    return {"@context": "https://schema.org", "@type": "Dataset",
            "name": f"{name} ({CYCLE_LABEL})", "description": desc,
            "temporalCoverage": CYCLE,
            "creator": {"@type": "Organization", "name": "Rings.com",
                        "url": "https://rings.com"}}


def base_ctx():
    return {
        "cycle_label": CYCLE_LABEL,
        "data_date": DATA_TIMESTAMP,
        "data_date_h": "July 23, 2026",
        "index_url": INDEX_URL,
        "brand_line": BRAND_LINE,
    }


def stone_page(env, sel, spec_row):
    lab, num = spec_row.cert_lab, spec_row.cert_number
    k = len(sel["displayed"])
    displayed = [sel["displayed"][r] for r in STONE_DISPLAY
                 if r in sel["displayed"]]
    prices = sorted((row.price_usd, row.retailer) for row in displayed)
    lo_p, lo_r = prices[0]
    hi_p, hi_r = prices[-1]
    cs, shape = carat_str(spec_row.carat), (spec_row.shape or "").title()
    title = (f"{cs} Carat {shape} Lab Grown Diamond {spec_row.color} "
             f"{spec_row.clarity} {lab} {num}: Price at {k} Retailers "
             f"– {CYCLE_LABEL}")
    h1 = (f"{cs} Carat {shape} Lab Grown Diamond "
          f"({spec_row.color} {spec_row.clarity}, {lab} {num}) Prices")
    meta = (f"{lab} {num}: {cs} ct {shape.lower()} lab grown diamond "
            f"({spec_row.color} {spec_row.clarity}) listed "
            f"{usd(lo_p)}–{usd(hi_p)} at {k} US retailers, "
            f"{CYCLE_LABEL}. Certificate-matched comparison.")
    handle = f"lab-diamond-{lab.lower()}-{num.lower()}"
    keyword = f"{lab.lower()} {num.lower()}"

    history = defaultdict(dict)
    hist_files = set()
    for r in stone_rows_for(sel["all_rows"]):
        if r.retailer in STONE_DISPLAY:
            history[r.cycle][r.retailer] = r.price_usd
            hist_files.add(r.source_file)
    hist_cycles = [c for c in ALL_CYCLES if c in history]
    retailer_order = [r for r in STONE_DISPLAY
                      if any(r in history[c] for c in hist_cycles)]

    def consensus(field):
        vals = set()
        for r in displayed:
            v = getattr(r, field, None)
            if isinstance(v, str) and v.strip() and v.strip().lower() \
                    not in ("nan", "none"):
                vals.add(v.strip())
        return vals.pop() if len(vals) == 1 else None
    ret_specs = {f: consensus(f) for f in ("cut", "polish", "symmetry")}

    json_ld = {
        "@context": "https://schema.org", "@type": "Product",
        "name": f"{cs} Carat {shape} Lab Grown Diamond ({lab} {num})",
        "sku": f"{lab}-{num}", "description": meta,
        "offers": {
            "@type": "AggregateOffer", "priceCurrency": "USD",
            "lowPrice": round(lo_p), "highPrice": round(hi_p),
            "offerCount": k,
            "offers": [{"@type": "Offer", "priceCurrency": "USD",
                        "price": round(row.price_usd),
                        "url": row.product_url,
                        "seller": {"@type": "Organization",
                                   "name": row.retailer}}
                       for row in displayed]}}
    body = env.get_template("stone.html.j2").render(
        **base_ctx(), spec=spec_row, cs=cs, shape=shape, lab=lab, num=num,
        displayed=displayed, k=k, lo_p=lo_p, lo_r=lo_r, hi_p=hi_p,
        hi_r=hi_r, spread=sel["spread"], spread_pct=sel["spread_pct"] * 100,
        history=history, hist_cycles=hist_cycles,
        retailer_order=retailer_order, ret_specs=ret_specs,
        cycle_labels=CYCLE_LABELS, h1=h1, meta=meta, json_ld=json_ld)
    src = sorted({row.source_file for row in displayed} | hist_files)
    record = {
        "handle": handle, "title": title, "h1": h1,
        "target_keyword": keyword, "page_type": "stone",
        "data_timestamp": DATA_TIMESTAMP, "cycle": CYCLE,
        "source_files": src, "sample_size": k,
        "retailer_set": [row.retailer for row in displayed],
        "cert": {"lab": lab, "number": num},
        "scope": {"cert_matched_only": True, "ring_total_present": False,
                  "ring_total_separated": False},
    }
    return handle, title, h1, meta, keyword, body, record, "stone"


def stone_rows_for(rows):
    return rows


def agg_record(handle, title, h1, keyword, n, retailer_set, src):
    return {
        "handle": handle, "title": title, "h1": h1,
        "target_keyword": keyword, "page_type": "aggregate",
        "data_timestamp": DATA_TIMESTAMP, "cycle": CYCLE,
        "source_files": sorted(src), "sample_size": int(n),
        "retailer_set": retailer_set,
        "scope": {"cert_matched_only": False, "ring_total_present": False,
                  "ring_total_separated": False},
    }


def july_sources(retailers):
    return [f"data/raw/{CYCLE_FILES[CYCLE][r]}" for r in sorted(retailers)]


def all_cycle_sources(retailers):
    out = []
    for cyc in ALL_CYCLES:
        for r in sorted(retailers):
            out.append(f"data/raw/{CYCLE_FILES[cyc][r]}")
    return out


def build(out_dir, canary_n):
    frame = load_frame()
    stones, pq = build_stone_parquet(frame)
    print(f"parquet: {pq} ({len(stones)} rows)")

    # attach full row history per cert for stone pages
    rows_by_key = defaultdict(list)
    for r in stones.itertuples():
        rows_by_key[r.cert_key].append(r)

    ranked, mismatched = select_stones(stones)
    pool = len(ranked)
    dist = defaultdict(int)
    for s in ranked:
        dist[s["n_displayed"]] += 1
    picked = ranked[:TARGET_STONE_PAGES]
    for s in picked:
        s["all_rows"] = rows_by_key[s["cert_key"]]
    print(f"strict pool: {pool} after spec-consistency guard "
          f"(excluded {len(mismatched)} spec-mismatched certs; "
          f"displayed-count dist {dict(dist)}); selected {len(picked)}")

    env = jenv()
    pages_dir = out_dir / "pages"
    pages_dir.mkdir(parents=True, exist_ok=True)
    for stale in pages_dir.glob("*.html"):   # regenerate-from-zero: no
        stale.unlink()                       # stale selections survive

    pages = []          # (handle, title, h1, meta, keyword, record, family)

    # ---- stone pages ----
    for sel in picked:
        spec_row = next(r for r in
                        (sel["displayed"].get(x) for x in
                         ["Brilliant Earth", "Ritani", "Grown Brilliance",
                          "Clean Origin"]) if r is not None)
        handle, title, h1, meta, kw, body, record, fam = stone_page(
            env, sel, spec_row)
        (pages_dir / f"{handle}.html").write_text(body)
        pages.append((handle, title, h1, meta, kw, record, "stone"))

    jf = july_agg_frame(frame)
    n_agg_retailers = int(jf["retailer"].nunique())
    skipped_cells = []

    # ---- Family A: carat-price pages ----
    for point in FAMILY_A_POINTS:
        st = cell_stats(jf, None, point)
        if not st["passes"]:
            skipped_cells.append(("A", None, point))
            continue
        cs = carat_str(point)
        title = (f"{cs} Carat Lab Grown Diamond Price – "
                 f"{CYCLE_LABEL} Data")
        h1 = f"{cs} Carat Lab Grown Diamond Price"
        m = len(st["shown_retailers"])
        meta = (f"The median {cs} carat lab grown diamond lists for "
                f"{usd(st['median'])} ({CYCLE_LABEL}), from "
                f"{st['n']:,} listings across {m} US retailers. "
                f"Per-retailer medians and monthly price history.")
        handle = f"{carat_slug(point)}-carat-lab-grown-diamond-price"
        kw = f"{cs} carat lab grown diamond price"
        shapes = [cell_stats(jf, s, point) for s in ("round", "oval")]
        hist = monthly_history(frame, None, point)
        body = env.get_template("aggregate_carat.html.j2").render(
            **base_ctx(), st=st, cs=cs, h1=h1, meta=meta, shapes=shapes,
            history=hist, volume_note=FAMILY_A_VOLUME.get(point),
            json_ld=dataset_ld(h1, meta))
        (pages_dir / f"{handle}.html").write_text(body)
        pages.append((handle, title, h1, meta, kw,
                      agg_record(handle, title, h1, kw, st["n"],
                                 st["shown_retailers"],
                                 july_sources(AGG_DISPLAY)
                                 + [s for s in all_cycle_sources(AGG_DISPLAY)
                                    if CYCLE not in s]), "A"))

    # ---- Family B: shape x carat pages ----
    for shape, point in FAMILY_B_CELLS:
        st = cell_stats(jf, shape, point)
        if not st["passes"]:
            skipped_cells.append(("B", shape, point))
            continue
        cs = carat_str(point)
        sh = shape.title()
        title = (f"{cs} Carat {sh} Lab Grown Diamond Price – "
                 f"{CYCLE_LABEL}")
        h1 = f"{cs} Carat {sh} Lab Grown Diamond Price"
        m = len(st["shown_retailers"])
        meta = (f"The median {cs} carat {shape} lab grown diamond lists "
                f"for {usd(st['median'])} ({CYCLE_LABEL}), from "
                f"{st['n']:,} listings across {m} US retailers. "
                f"Per-retailer medians and monthly history.")
        handle = (f"{carat_slug(point)}-carat-{shape}-lab-grown-"
                  f"diamond-price")
        kw = f"{cs} carat {shape} lab grown diamond price"
        hist = monthly_history(frame, shape, point)
        body = env.get_template("aggregate_shape_carat.html.j2").render(
            **base_ctx(), st=st, cs=cs, sh=sh, h1=h1, meta=meta,
            history=hist, json_ld=dataset_ld(h1, meta))
        (pages_dir / f"{handle}.html").write_text(body)
        pages.append((handle, title, h1, meta, kw,
                      agg_record(handle, title, h1, kw, st["n"],
                                 st["shown_retailers"],
                                 july_sources(AGG_DISPLAY)
                                 + [s for s in all_cycle_sources(AGG_DISPLAY)
                                    if CYCLE not in s]), "B"))

    # ---- Family D1: cost FAQ ----
    cells = {p: cell_stats(jf, None, p) for p in FAMILY_A_POINTS}
    total_n = int(len(jf))
    title = (f"How Much Does a Lab Grown Diamond Cost? "
             f"({CYCLE_LABEL} Data)")
    h1 = "How Much Does a Lab Grown Diamond Cost?"
    meta = (f"{CYCLE_LABEL}: the median 1 carat lab grown diamond lists "
            f"for {usd(cells[1.0]['median'])}, 1.5 carat "
            f"{usd(cells[1.5]['median'])}, 2 carat "
            f"{usd(cells[2.0]['median'])}. From {total_n:,} listings "
            f"across {n_agg_retailers} US retailers.")
    handle = "how-much-does-a-lab-grown-diamond-cost"
    kw = "how much does a lab grown diamond cost"
    faq_ld = {
        "@context": "https://schema.org", "@type": "FAQPage",
        "mainEntity": [
            {"@type": "Question",
             "name": "How much does a 1 carat lab grown diamond cost?",
             "acceptedAnswer": {"@type": "Answer", "text":
                 f"The median 1 carat lab grown diamond listed for "
                 f"{usd(cells[1.0]['median'])} in {CYCLE_LABEL}, with most "
                 f"listings between {usd(cells[1.0]['p10'])} and "
                 f"{usd(cells[1.0]['p90'])} depending on color, clarity, "
                 f"and cut."}},
            {"@type": "Question",
             "name": "How much does a 2 carat lab grown diamond cost?",
             "acceptedAnswer": {"@type": "Answer", "text":
                 f"The median 2 carat lab grown diamond listed for "
                 f"{usd(cells[2.0]['median'])} in {CYCLE_LABEL}, with most "
                 f"listings between {usd(cells[2.0]['p10'])} and "
                 f"{usd(cells[2.0]['p90'])}."}},
        ]}
    body = env.get_template("aggregate_cost_faq.html.j2").render(
        **base_ctx(), cells=cells, total_n=total_n,
        n_retailers=n_agg_retailers, h1=h1, meta=meta,
        carat_points=FAMILY_A_POINTS,
        retailer_names=sorted(jf["retailer"].unique()), json_ld=faq_ld)
    (pages_dir / f"{handle}.html").write_text(body)
    pages.append((handle, title, h1, meta, kw,
                  agg_record(handle, title, h1, kw, total_n,
                             sorted(jf["retailer"].unique()),
                             july_sources(AGG_DISPLAY)), "D"))

    # ---- Family D2: resale value ----
    rep = repricing_stats(stones)
    hist_1ct = monthly_history(frame, "round", 1.0)
    title = "Lab Grown Diamond Resale Value: What Price History Shows"
    h1 = "Lab Grown Diamond Resale Value: What Price History Shows"
    meta = (f"Listing-price history for lab grown diamonds: the same "
            f"certified stones repriced a median "
            f"{rep['median_change_pct']:+.1f}% April–July 2026 "
            f"({rep['n_pairs']:,} matched listings). What that means "
            f"for resale value.")
    handle = "lab-grown-diamond-resale-value"
    kw = "lab grown diamond resale value"
    body = env.get_template("aggregate_resale.html.j2").render(
        **base_ctx(), rep=rep, hist=hist_1ct, h1=h1, meta=meta,
        retailer_names=AGG_DISPLAY, json_ld=dataset_ld(h1, meta))
    (pages_dir / f"{handle}.html").write_text(body)
    pages.append((handle, title, h1, meta, kw,
                  agg_record(handle, title, h1, kw, rep["n_pairs"],
                             AGG_DISPLAY,
                             all_cycle_sources(AGG_DISPLAY)), "D"))

    # ---- Family E: comparison hub ----
    grid = {p: cell_stats(jf, None, p) for p in FAMILY_A_POINTS}
    title = (f"Lab Grown Diamond Price Comparison: {n_agg_retailers} US "
             f"Retailers, Updated Monthly")
    h1 = "Lab Grown Diamond Price Comparison"
    meta = (f"Median lab grown diamond prices at 1, 1.5, and 2 carats "
            f"compared across {n_agg_retailers} US retailers "
            f"({CYCLE_LABEL}). Publicly listed prices, "
            f"{total_n:,} listings, refreshed monthly.")
    handle = "lab-grown-diamond-price-comparison"
    kw = "lab grown diamond price comparison"
    body = env.get_template("aggregate_hub.html.j2").render(
        **base_ctx(), grid=grid, carat_points=FAMILY_A_POINTS,
        retailers=AGG_DISPLAY, n_retailers=n_agg_retailers, h1=h1,
        meta=meta, total_n=total_n, json_ld=dataset_ld(h1, meta))
    (pages_dir / f"{handle}.html").write_text(body)
    pages.append((handle, title, h1, meta, kw,
                  agg_record(handle, title, h1, kw, total_n,
                             sorted(jf["retailer"].unique()),
                             july_sources(AGG_DISPLAY)), "E"))

    # ------------------------------------------------ records + manifests
    qa = [rec for (_, _, _, _, _, rec, _) in pages]
    (out_dir / "qa-manifest.json").write_text(
        json.dumps({"pages": qa}, indent=1) + "\n")

    with open(out_dir / "keyword-log.csv", "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["handle", "page_type", "family", "target_keyword",
                    "title", "h1", "meta_description"])
        for (handle, title, h1, meta, kw, rec, fam) in pages:
            w.writerow([handle, rec["page_type"], fam, kw, title, h1, meta])
    with open(out_dir / "keyword-log-aggregate.csv", "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["handle", "page_type", "family", "target_keyword",
                    "title", "h1", "meta_description"])
        for (handle, title, h1, meta, kw, rec, fam) in pages:
            if rec["page_type"] == "aggregate":
                w.writerow([handle, rec["page_type"], fam, kw, title, h1,
                            meta])

    # ---- canary: every aggregate page + top stone pages (3-retailer
    # stones first, then widest 2-retailer spreads) ----
    agg_handles = [h for (h, *_rest) in pages
                   if _rest[-2]["page_type"] == "aggregate"]
    stone_pages = [p for p in pages if p[5]["page_type"] == "stone"]
    three_r = [p for p in stone_pages if p[5]["sample_size"] >= 3]
    two_r = [p for p in stone_pages if p[5]["sample_size"] == 2]
    n_stone_canary = max(0, canary_n - len(agg_handles))
    canary_stones = (three_r[:3] + two_r)[:n_stone_canary]
    canary = [p for p in pages
              if p[5]["page_type"] == "aggregate"] + canary_stones

    (out_dir / "qa-manifest-canary.json").write_text(
        json.dumps({"pages": [p[5] for p in canary]}, indent=1) + "\n")
    spec = {"pages": [{
        "handle": p[0], "title": p[1],
        "body_file": f"pages/{p[0]}.html",
        "source": {"cycle": CYCLE, "data_timestamp": DATA_TIMESTAMP,
                   "source_files": p[5]["source_files"],
                   "sample_size": p[5]["sample_size"],
                   "retailer_set": p[5]["retailer_set"]},
        "target_keyword": p[4],
        "note": f"batch-1 canary; {p[5]['page_type']} page; "
                f"meta in canary-meta.json",
    } for p in canary]}
    (out_dir / "canary-rollback-spec.json").write_text(
        json.dumps(spec, indent=1) + "\n")
    (out_dir / "canary-meta.json").write_text(json.dumps({
        p[0]: {"title_tag": p[1], "description_tag": p[3]}
        for p in canary}, indent=1) + "\n")

    report = {
        "generated_for_cycle": CYCLE,
        "strict_pool_before_guard": pool + len(mismatched),
        "spec_mismatch_excluded": len(mismatched),
        "spec_mismatch_examples": mismatched[:10],
        "strict_pool": pool,
        "displayed_count_distribution": {str(k): v for k, v
                                         in sorted(dist.items())},
        "stone_pages": len(stone_pages),
        "aggregate_pages": len(pages) - len(stone_pages),
        "aggregate_by_family": {
            fam: sum(1 for p in pages if p[6] == fam)
            for fam in ("A", "B", "D", "E")},
        "skipped_cells": [f"{f}:{s or 'both'}:{p}"
                          for (f, s, p) in skipped_cells],
        "canary_pages": len(canary),
        "canary_aggregate": len(agg_handles),
        "canary_stone": len(canary_stones),
        "not_built": [
            "Family C retailer roundup pages (6): HELD BACK, binding "
            "condition 2 (ships only after Block 2 closes + quiet "
            "period clean)",
            "3 Carat page: data-gated (no data above 2.50ct in the "
            "July cycle; carat extension running)",
        ],
        "parquet": str(pq),
        "top_spread": {"cert_key": picked[0]["cert_key"],
                       "spread": picked[0]["spread"]} if picked else None,
    }
    (out_dir / "selection-report.json").write_text(
        json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build")
    b.add_argument("--out", default=str(HERE / "page_output"))
    b.add_argument("--canary", type=int, default=25)
    args = ap.parse_args()
    if args.cmd == "build":
        build(Path(args.out), args.canary)


if __name__ == "__main__":
    main()
