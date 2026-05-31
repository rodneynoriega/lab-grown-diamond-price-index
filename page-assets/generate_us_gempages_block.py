#!/usr/bin/env python3
"""
generate_us_gempages_block.py
Single source of truth for the US price index GemPages custom code block.

Reads index-data.json and produces both:
  1. The pre-rendered static price table HTML (for no-JS / crawler visibility)
  2. The Dataset schema.org JSON-LD (variableMeasured min/max derived from the same data)

Both are written as a single HTML file, ready to paste into the GemPages custom code block.

Usage:
    python generate_us_gempages_block.py [path/to/index-data.json]

Output:
    OUTBOX/us-index-gempages-YYYY-MM-DD.html
    (date taken from data["last_updated"])

Monthly update workflow:
    1. Update index-data.json with new scrape results
    2. Run this script
    3. Paste the output file into GemPages -> Custom Code block
    4. The JSON-LD dateModified, variableMeasured min/max, and table all update automatically
"""

import json
import math
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DEFAULT_JSON = os.path.join(PROJECT_ROOT, "PROJECTS", "diamond-price-index", "index-data.json")

BANDS = [
    ("1ct",   1.0, "0.95-1.05ct"),
    ("1.5ct", 1.5, "1.45-1.55ct"),
    ("2ct",   2.0, "1.95-2.05ct"),
]

MONTH_NAMES = [
    "January","February","March","April","May","June",
    "July","August","September","October","November","December",
]
MONTH_MAP = {m: f"{i+1:02d}" for i, m in enumerate(MONTH_NAMES)}


def load_data(path=None):
    with open(path or DEFAULT_JSON) as f:
        return json.load(f)


def total_price(ppc, weight):
    # Use JS Math.round semantics (always rounds 0.5 up, not banker's rounding)
    return math.floor(ppc * weight + 0.5)


def median_of(values):
    if not values:
        return None
    s = sorted(values)
    m = len(s) // 2
    return round((s[m-1] + s[m]) / 2) if len(s) % 2 == 0 else s[m]


def band_ranges(data):
    """Return {band_key: {"min": int, "max": int}} for IGI retailers only."""
    result = {}
    for key, wt, _ in BANDS:
        prices = []
        for r in data["retailers"]:
            if r.get("non_igi"):
                continue
            cell = r["cells"].get(key)
            if cell and cell.get("status") == "ok":
                prices.append(total_price(cell["median_price_per_carat"], wt))
        if prices:
            result[key] = {"min": min(prices), "max": max(prices)}
    return result


def pub_display(last_updated):
    """'2026-05-21' -> 'May 21, 2026'"""
    y, mo, d = last_updated.split("-")
    return f"{MONTH_NAMES[int(mo)-1]} {int(d)}, {y}"


def temporal_coverage(month):
    """'May 2026' -> '2026-05'"""
    parts = (month or "").split()
    if len(parts) == 2 and parts[0] in MONTH_MAP:
        return f"{parts[1]}-{MONTH_MAP[parts[0]]}"
    return month


def retailer_list(retailers):
    names = [r["name"] for r in retailers]
    return ", ".join(names[:-1]) + ", and " + names[-1]


def total_listings_for_month(data):
    month = data.get("month", "")
    if "history" in data:
        for h in reversed(data["history"]):
            if h.get("month") == month:
                return h.get("total_listings")
    return None


def generate_jsonld(data, ranges, pub_date):
    month = data.get("month", "")
    listings = total_listings_for_month(data)
    listings_str = f"{listings:,}" if listings else "N/A"

    vm = []
    for key, _, band_range in BANDS:
        if key in ranges:
            r = ranges[key]
            vm.append({
                "@type": "PropertyValue",
                "name": f"Median total stone price, {key} cell ({band_range}), IGI retailers",
                "unitCode": "USD",
                "minValue": r["min"],
                "maxValue": r["max"],
            })

    obj = {
        "@context": "https://schema.org",
        "@type": "Dataset",
        "name": f"Rings.com Lab-Grown Diamond Price Index, {month}",
        "description": (
            f"Monthly benchmark prices for lab-grown diamonds (E VS1 Round Excellent IGI-certified) "
            f"across major U.S. online retailers. {month} edition covers {listings_str} listings "
            f"across {len(data['retailers'])} retailers: {retailer_list(data['retailers'])}."
        ),
        "url": "https://rings.com/pages/lab-grown-diamond-price-index",
        "datePublished": pub_date,
        "dateModified": pub_date,
        "publisher": {"@type": "Organization", "name": "Rings.com", "url": "https://rings.com"},
        "creator":   {"@type": "Organization", "name": "Rings.com", "url": "https://rings.com"},
        "temporalCoverage": temporal_coverage(month),
        "isAccessibleForFree": True,
        "keywords": [
            "lab-grown diamond prices",
            "lab diamond price index USA",
            "diamond price comparison",
            "IGI certified diamonds",
            "E VS1 round diamond price",
            "US lab diamond retailer comparison",
        ],
        "measurementTechnique": (
            "Median total stone price of publicly listed lab-grown diamond prices, filtered to "
            "E color, VS1 clarity, round brilliant cut, Excellent cut grade, IGI-certified, "
            "within plus or minus 0.05ct of benchmark weight. Minimum 30 qualifying listings "
            "required per published cell. All prices in USD, excluding tax and shipping."
        ),
        "variableMeasured": vm,
        "spatialCoverage": {"@type": "Place", "name": "United States"},
        "distribution": {
            "@type": "DataDownload",
            "encodingFormat": "application/json",
            "contentUrl": "https://raw.githubusercontent.com/rodneynoriega/lab-grown-diamond-price-index/main/index-data.json",
        },
        "license": "https://rings.com/pages/terms-of-service",
    }
    return json.dumps(obj, indent=2, ensure_ascii=False)


def cell_td(cell, bg, weight):
    base = f'text-align:right;padding:10px 12px;background:{bg};'
    if not cell or cell.get("status") != "ok":
        return f'<td style="{base}color:#aaa;">&#8212;</td>'
    tp = total_price(cell["median_price_per_carat"], weight)
    return f'<td style="{base}font-variant-numeric:tabular-nums;">${tp:,}</td>'


def generate_html(data):
    pub_date = data.get("last_updated", "")
    pub_disp = pub_display(pub_date)
    month    = data.get("month", "")
    retailers = data["retailers"]

    # Dollar-gap callout sentence (IGI only, status == ok)
    igi_1ct = [(r["name"], r["cells"]["1ct"]["median_price_per_carat"])
               for r in retailers
               if not r.get("non_igi") and r["cells"].get("1ct", {}).get("status") == "ok"]
    igi_2ct = [total_price(r["cells"]["2ct"]["median_price_per_carat"], 2.0)
               for r in retailers
               if not r.get("non_igi") and r["cells"].get("2ct", {}).get("status") == "ok"]
    min1_name, min1 = min(igi_1ct, key=lambda x: x[1])
    max1_name, max1 = max(igi_1ct, key=lambda x: x[1])
    min2, max2 = min(igi_2ct), max(igi_2ct)

    callout = (
        f'<p style="font-size:0.95rem;color:#333;margin:0 0 20px;line-height:1.6;">'
        f'At 1ct, prices range from <strong>${round(min1):,} to ${round(max1):,}</strong> '
        f'depending on where you buy. At 2ct, the gap widens to '
        f'<strong>${min2:,} to ${max2:,}</strong>. '
        f'Same certification. Same grade. Different retailer.</p>'
    )

    # Market medians (IGI only, per-carat then total)
    medians = {}
    for key, wt, _ in BANDS:
        prices = [r["cells"][key]["median_price_per_carat"]
                  for r in retailers
                  if not r.get("non_igi") and r["cells"].get(key, {}).get("status") == "ok"]
        med_ppc = median_of(prices)
        medians[key] = total_price(med_ppc, wt) if med_ppc is not None else None

    # Build retailer rows
    rows = []
    for i, r in enumerate(retailers):
        is_vrai = r.get("non_igi", False)
        bg = "#f5f5f5" if is_vrai else ("#fafafa" if i % 2 == 0 else "#ffffff")
        row_style = ' style="border-top:2px solid #e0e0e0;"' if is_vrai else ""
        name = r["name"]
        if is_vrai:
            name += ' <span style="font-size:0.75rem;font-weight:400;color:#999;">(non-IGI)</span>'
        if r["slug"] == "ritani":
            name += " *"

        tds = "".join(cell_td(r["cells"].get(key), bg, wt) for key, wt, _ in BANDS)
        rows.append(
            f'        <tr{row_style}>'
            f'<td style="padding:10px 12px;font-weight:700;white-space:nowrap;background:{bg};color:#1a1a1a;">{name}</td>'
            f'{tds}</tr>'
        )

    med_tds = []
    for key, wt, _ in BANDS:
        med = medians.get(key)
        if med:
            med_tds.append(
                f'<td style="text-align:right;padding:10px 12px;background:#f0f0f0;'
                f'color:#555;font-style:italic;font-variant-numeric:tabular-nums;">${med:,}</td>'
            )
        else:
            med_tds.append(
                '<td style="text-align:right;padding:10px 12px;background:#f0f0f0;color:#aaa;">&#8212;</td>'
            )

    rows_html  = "\n".join(rows)
    med_td_html = "".join(med_tds)

    # Key Findings — pull first bullet numbers from computed values
    # (same data as table and JSON-LD)
    ranges = band_ranges(data)
    min1ct = ranges["1ct"]["min"] if "1ct" in ranges else 0
    max1ct = ranges["1ct"]["max"] if "1ct" in ranges else 0
    min2ct = ranges["2ct"]["min"] if "2ct" in ranges else 0
    max2ct = ranges["2ct"]["max"] if "2ct" in ranges else 0
    listings = total_listings_for_month(data)
    listings_str = f"{listings:,}" if listings else ""
    n_retailers = len(retailers)

    # Detect Ritani move for Key Findings (if history has >= 2 months)
    ritani_note = ""
    if "history" in data and len(data["history"]) >= 2:
        prev = data["history"][-2]["retailers"].get("ritani", {})
        curr = data["history"][-1]["retailers"].get("ritani", {})
        if prev.get("1ct") and curr.get("1ct"):
            pct = round((curr["1ct"] - prev["1ct"]) / prev["1ct"] * 100)
            ritani_note = f"Ritani posted the largest month-over-month move: {pct:+d}% at 1ct."

    jsonld_str = generate_jsonld(data, ranges, pub_date)

    html = f"""\
<div id="lgd-index" style="font-family:system-ui,-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#1a1a1a;max-width:860px;margin:0 auto;padding:0 16px 24px;">

  <div style="padding:14px 0;margin-bottom:24px;border-bottom:1px solid #e5e5e5;display:flex;justify-content:space-between;align-items:center;">
    <a href="/" style="font-size:0.75rem;font-weight:700;letter-spacing:0.1em;text-transform:uppercase;color:#1a1a1a;text-decoration:none;">Rings.com</a>
    <span style="font-size:0.75rem;color:#999;letter-spacing:0.05em;text-transform:uppercase;">Research &amp; Data</span>
  </div>

  <div style="border-top:3px solid #1a1a1a;padding-top:20px;margin-bottom:32px;">
    <p style="font-size:0.7rem;font-weight:700;letter-spacing:0.12em;text-transform:uppercase;color:#888;margin:0 0 14px;">Rings.com Research</p>
    <h1 style="font-size:clamp(1.6rem,4.5vw,2.4rem);font-weight:700;margin:0 0 10px;line-height:1.15;letter-spacing:-0.01em;">
      Lab-Grown Diamond Price Index
    </h1>
    <p style="font-size:1rem;color:#555;margin:0 0 16px;line-height:1.55;max-width:560px;">
      Monthly benchmark prices for lab-grown diamonds across major U.S. online retailers.
    </p>
    <p id="lgd-published-date" style="font-size:0.78rem;color:#999;margin:0;padding-top:14px;border-top:1px solid #ebebeb;">
      Published {pub_disp} &nbsp;&middot;&nbsp; E&nbsp;VS1 Round Excellent IGI &nbsp;&middot;&nbsp; USD
    </p>
  </div>

  <!--
    lgd-table-wrap: pre-rendered static table for crawlers and no-JS environments.
    The theme.liquid script replaces this with the interactive version (tooltips, share
    buttons) when JS runs. Content is identical -- same prices, same structure.
  -->
  <div id="lgd-table-wrap" style="overflow-x:auto;margin-bottom:32px;">

    {callout}

    <table style="width:100%;border-collapse:collapse;font-size:0.95rem;">
      <thead>
        <tr>
          <th style="text-align:left;padding:10px 12px;border-bottom:2px solid #1a1a1a;font-weight:700;white-space:nowrap;color:#1a1a1a;background:#fff;">Retailer</th>
          <th style="text-align:right;padding:10px 12px;border-bottom:2px solid #1a1a1a;font-weight:700;white-space:nowrap;color:#1a1a1a;background:#fff;">1ct</th>
          <th style="text-align:right;padding:10px 12px;border-bottom:2px solid #1a1a1a;font-weight:700;white-space:nowrap;color:#1a1a1a;background:#fff;">1.5ct</th>
          <th style="text-align:right;padding:10px 12px;border-bottom:2px solid #1a1a1a;font-weight:700;white-space:nowrap;color:#1a1a1a;background:#fff;">2ct</th>
        </tr>
      </thead>
      <tbody>
{rows_html}
        <tr style="border-top:2px solid #ccc;">
          <td style="padding:10px 12px;font-weight:600;font-style:italic;background:#f0f0f0;color:#555;">Market Median (IGI retailers)</td>
          {med_td_html}
        </tr>
      </tbody>
    </table>

    <p style="font-size:0.8rem;color:#666;margin:8px 0 0;line-height:1.4;">
      Total stone price. E VS1 Round Excellent IGI. Median. All prices USD. VRAI: proprietary Diamond Foundry grading, not matched to E VS1 IGI specification. * Largest month-over-month move. See Key Findings.
    </p>

  </div>

  <!--
    lgd-callout-wrap: pre-rendered key findings for crawlers.
    JS replaces this with the interactive version.
  -->
  <div id="lgd-callout-wrap" style="margin-bottom:32px;">
    <div style="background:#1a1a1a;color:#ffffff;border-radius:6px;padding:20px 24px;line-height:1.7;font-size:0.95rem;">
      <strong style="display:block;margin-bottom:10px;font-size:1rem;color:#ffffff;">Key Findings: {month}</strong>
      <p style="margin:0 0 14px;color:#c8c8c8;font-size:0.9rem;line-height:1.6;">{n_retailers} retailers. {listings_str} listings. E VS1 Round Excellent IGI.</p>
      <ul style="margin:0;padding:0 0 0 18px;color:#ffffff;line-height:1.65;">
        <li style="margin:0 0 9px;">If you're shopping for a 1ct E VS1 IGI round, prices range from ${min1ct:,} to ${max1ct:,} depending on where you buy. That gap is not a quality difference. It's a retailer difference.</li>
        <li style="margin:0 0 9px;">At 2ct, the gap widens to ${min2ct:,} to ${max2ct:,}.</li>
      </ul>
    </div>
  </div>

  <!--
    lgd-method-wrap: pre-rendered methodology for crawlers.
    JS replaces this with the interactive version.
  -->
  <div id="lgd-method-wrap" style="border-top:1px solid #ddd;padding-top:24px;margin-bottom:24px;">
    <h2 style="font-size:1rem;font-weight:700;margin:0 0 12px;color:#1a1a1a;">Methodology</h2>
    <div style="font-size:0.9rem;color:#333;line-height:1.7;">
      <p style="margin:0 0 12px;"><strong>Scope.</strong> This index tracks listed retail prices (not transaction prices) across major U.S. online lab-grown diamond retailers. The {month} edition analyzed {listings_str} lab-grown diamond listings across {n_retailers} retailers.</p>
      <p style="margin:0 0 12px;"><strong>Benchmark specification.</strong> E color, VS1 clarity, round brilliant, Excellent cut, IGI-certified. Three weight cells: 1ct (0.95-1.05ct), 1.5ct (1.45-1.55ct), and 2ct (1.95-2.05ct).</p>
      <p style="margin:0 0 12px;"><strong>Statistical method.</strong> Each published figure is the median total stone price across all qualifying listings within that cell. A minimum of 30 qualifying listings is required for publication.</p>
    </div>
  </div>

  <!--
    lgd-footer-wrap: pre-rendered footer for crawlers.
    JS replaces this with the interactive version.
  -->
  <div id="lgd-footer-wrap" style="border-top:1px solid #eee;padding-top:16px;">
    <p style="font-size:0.78rem;color:#999;margin:0;line-height:1.6;">
      Rings.com Lab-Grown Diamond Price Index. &copy; 2026 Rings.com. All rights reserved.
    </p>
  </div>

</div>

<!--
  Dataset JSON-LD: derived from the same price data as the static table above.
  variableMeasured min/max = total stone price (median_price_per_carat * weight)
  for IGI retailers only (VRAI excluded; Clean Origin excluded from 1ct when no qualifying data).
  datePublished and dateModified = data["last_updated"] ({pub_date}).
  DO NOT hand-edit min/max or dates here separately from the table above.
  To regenerate: python generate_us_gempages_block.py [path/to/index-data.json]
-->
<script type="application/ld+json">
{jsonld_str}
</script>
"""
    return html


def main():
    data_path = sys.argv[1] if len(sys.argv) > 1 else None
    data = load_data(data_path)
    pub_date = data.get("last_updated", "unknown")

    html = generate_html(data)

    out_name = f"us-index-gempages-{pub_date}.html"
    out_path = os.path.join(SCRIPT_DIR, out_name)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)

    # Also print a summary for verification
    ranges = band_ranges(data)
    print(f"Generated: {out_path}")
    print(f"datePublished / dateModified: {pub_date} (from data['last_updated'])")
    print("variableMeasured min/max (derived from retailer cells):")
    for key, wt, rng in BANDS:
        if key in ranges:
            r = ranges[key]
            print(f"  {key} ({rng}): min=${r['min']:,}  max=${r['max']:,}")
    print("Paste the output file into the GemPages custom code block.")


if __name__ == "__main__":
    main()
