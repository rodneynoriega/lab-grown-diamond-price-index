"""
UK LGD benchmark scraper -- reusable monthly runner (first built for the July 2026 cycle).
Retailers: Diamond Labs, 77 Diamonds, Quality Diamonds (confirmed prior cycles)
         + Serendipity Jewellery - Isle of Wight (added 2026-07-30, via the
           diamond-proxy Shopify app at isle-of-wight-jewellery.co.uk;
           serendipitydiamonds.com redirects there).
Benchmark: G VS1 Excellent IGI, round, bands 0.95-1.05 / 1.45-1.55 / 1.95-2.05ct.
All prices captured ex-VAT GBP per stone; median computed on price-per-carat.

Run each cycle with `python3 run_uk_benchmark.py`. Re-verify each retailer's
API/filter codes still work before trusting the output blind -- see
MEMORY/project_uk_index.md for known quirks and prior-month figures to
sanity-check against.
"""
import json
import statistics
import time
from curl_cffi import requests

BANDS = {"1ct": (0.95, 1.05), "1.5ct": (1.45, 1.55), "2ct": (1.95, 2.05)}

# Raw API rows per retailer/band, captured for the listing-level snapshot
# (Phase 0 data retention). Written to data/raw/uk_<slug>_<date>.jsonl at the
# end of a run, then gzipped into data/snapshots/ by snapshot.sync().
RAW_ROWS: dict[str, dict[str, list]] = {}


def median_ppc(prices_carats):
    ppc = [p / c for p, c in prices_carats]
    return round(statistics.median(ppc)) if ppc else None


# ---------------------------------------------------------------------------
# Diamond Labs
# ---------------------------------------------------------------------------
def scrape_diamond_labs():
    results = {}
    for label, (cf, ct) in BANDS.items():
        all_rows = []
        page = 1
        while True:
            r = requests.get(
                "https://backend.diamond-labs.co.uk/api/v1/diamonds",
                params={
                    "shape": "Round", "color": "G", "clarity": "VS1",
                    "cut": "Excellent,Ideal", "minCarat": cf, "maxCarat": ct,
                    "isFancy": "false", "sortField": "salePriceGbp",
                    "sortDirection": "asc", "pageSize": 200, "pageNumber": page,
                },
                headers={"Referer": "https://diamond-labs.co.uk/"},
                impersonate="chrome", timeout=30,
            )
            data = r.json()
            rows = data.get("data", [])
            all_rows.extend(rows)
            if not data.get("hasMore") or not rows:
                break
            page += 1
        RAW_ROWS.setdefault("diamond_labs", {})[label] = all_rows
        pc = [(row["salePriceGbp"], row["carat"]) for row in all_rows if row.get("cut") == "Excellent"]
        results[label] = {"n": len(pc), "median_ppc": median_ppc(pc)}
        print(f"  Diamond Labs {label}: n={len(pc)} median={results[label]['median_ppc']}")
    return results


# ---------------------------------------------------------------------------
# Quality Diamonds
# ---------------------------------------------------------------------------
def scrape_quality_diamonds():
    results = {}
    for label, (cf, ct) in BANDS.items():
        all_rows = []
        start = 0
        length = 100
        while True:
            r = requests.post(
                "https://www.qualitydiamonds.co.uk/umbraco/Surface/CategorySurface/GetDiamondListJsonData",
                json={
                    "Shape": "Round Brilliant", "DiamondType": "Lab-Grown",
                    "SelectedColourVals": ["2826"], "SelectedClarityVals": ["2737"],
                    "SelectedCutVals": ["2746"], "Start": start, "Length": length,
                    "MinCarat": cf, "MaxCarat": ct,
                },
                headers={
                    "Referer": "https://www.qualitydiamonds.co.uk/loose-diamonds/buy-lab-grown-diamonds/",
                    "X-Requested-With": "XMLHttpRequest",
                },
                impersonate="chrome", timeout=30,
            )
            data = r.json()
            rows = data.get("data", [])
            all_rows.extend(rows)
            total = data.get("recordsTotal", 0)
            start += length
            if start >= total or not rows:
                break
        RAW_ROWS.setdefault("quality_diamonds", {})[label] = all_rows
        pc = [(row["PriceExVatValue"], float(row["Carats"])) for row in all_rows]
        results[label] = {"n": len(pc), "median_ppc": median_ppc(pc)}
        print(f"  Quality Diamonds {label}: n={len(pc)} median={results[label]['median_ppc']}")
    return results


# ---------------------------------------------------------------------------
# 77 Diamonds (needs Playwright session for cross-origin cookies)
# ---------------------------------------------------------------------------
def scrape_77diamonds():
    from playwright.sync_api import sync_playwright
    results = {}
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto("https://www.77diamonds.com/loose-lab-grown-diamonds#shape=ROUND",
                   wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(2000)
        for label, (cf, ct) in BANDS.items():
            # NOTE (2026-07-30): currentPage is ignored server-side -- every page
            # returns the identical top-N rows (confirmed via 100% Code overlap
            # across currentPage=1/2/3). Workaround: request pageSize/ResultsPerPage
            # >= the true total (probed first via a size=1 call) to get every row
            # in a single request instead of paginating.
            probe_body = {
                "categoryId": 7, "stoneType": 3,
                "shapes": [1], "colors": [22], "clarities": [44], "cuts": [56], "Certificates": [51],
                "minCarat": cf, "maxCarat": ct,
                "country": 0, "currency": 0, "language": 0,
                "CountryId": 826, "CurrencyId": 1, "LanguageId": 1,
                "UserPreference": {"CountryId": 826, "CurrencyId": 1, "LanguageId": 1},
                "currentPage": 1, "pageSize": 1, "ResultsPerPage": 1,
                "diamondType": -1, "isGroupedShapes": False, "itemId": -1,
                "maxDepth": 0, "maxPrice": 0, "maxRatio": 5, "maxTable": 0,
                "minDepth": 0, "minPrice": 0, "minRatio": 1, "minTable": 0,
                "quickShipping": False, "searchBlocked": False, "showPairs": False, "withMedia": False,
                "Intensities": [], "Polishes": [], "Symmetries": [], "Fluorescences": [],
                "GemClarity": [], "GemIntensity": [], "GemTreatment": [], "GemClarityTreatment": [],
                "Url": "https://www.77diamonds.com/loose-lab-grown-diamonds#shape=ROUND",
            }
            probe_resp = page.request.post(
                "https://www.77diamonds.com/api/bff/shop/diamond-list",
                data=json.dumps(probe_body), headers={"Content-Type": "application/json"},
            )
            total = json.loads(probe_resp.text()).get("Total", 0)
            full_size = max(total, 1)
            body = dict(probe_body, pageSize=full_size, ResultsPerPage=full_size)
            resp = page.request.post(
                "https://www.77diamonds.com/api/bff/shop/diamond-list",
                data=json.dumps(body), headers={"Content-Type": "application/json"},
            )
            data = json.loads(resp.text())
            all_rows = data.get("Diamonds", [])
            seen_codes = set()
            deduped = []
            for row in all_rows:
                if row["Code"] not in seen_codes:
                    seen_codes.add(row["Code"])
                    deduped.append(row)
            all_rows = deduped
            RAW_ROWS.setdefault("77_diamonds", {})[label] = all_rows
            pc = []
            for row in all_rows:
                total_price = row["DiamondPrice"]["TotalDiamondPrice"]
                final = total_price.get("FinalPrice") or total_price["Price"]
                price = final["NumericPrice"]["WithoutVat"]
                pc.append((price, float(row["CaratWeight"])))
            results[label] = {"n": len(pc), "median_ppc": median_ppc(pc)}
            print(f"  77 Diamonds {label}: n={len(pc)} median={results[label]['median_ppc']}")
        browser.close()
    return results


# ---------------------------------------------------------------------------
# Serendipity Jewellery - Isle of Wight -- NOT CURRENTLY USED, DO NOT RE-ENABLE
# WITHOUT FIXING THE CURRENCY BUG FIRST.
#
# row["price"] from this API is USD, not GBP, despite the .co.uk domain and
# GBP-plausible price magnitudes. Confirmed 2026-07-30: forcing the Shopify
# Markets session to GBP (POST /localization {"country_code":"GB","_method":
# "PUT"}) correctly flips the STOREFRONT's own rendered prices, but the
# diamond-proxy API's price field does not change at all -- it's a separate
# backend (Kong gateway) with a fixed base currency. Same-stone cross-check:
# API returned 106, GBP-forced storefront displayed £79 for it (ratio 0.745,
# matches a USD->GBP conversion almost exactly). See MEMORY/project_uk_index.md
# "Excluded Retailers" for the full writeup. Fix before reusing: either apply a
# real, dated USD/GBP rate, or scrape the rendered UI instead of this API.
# ---------------------------------------------------------------------------
def scrape_iow_jewellery():
    results = {}
    s = requests.Session(impersonate="chrome")
    s.get("https://isle-of-wight-jewellery.co.uk/pages/diamond-and-gemstone-search?activeTab=Lab+Grown&queryLabGrown=true",
          timeout=30)
    headers = {
        "Content-Type": "application/json",
        "Referer": "https://isle-of-wight-jewellery.co.uk/pages/diamond-and-gemstone-search",
    }
    for label, (cf, ct) in BANDS.items():
        base_query = {
            "labgrown": True, "shapes": ["ROUND"], "certificate_lab": ["GIA", "IGI"],
            "color": ["G"], "sizes": {"from": cf, "to": ct}, "clarity": ["VS1"],
            "cut": ["EX"], "polish": ["VG", "EX"], "symmetry": ["VG", "EX"],
            "flouresence": ["NON", "FNT", "MED", "STG", "VST"], "labgrown_type": [],
            "ratio": {"from": 0.8, "to": 3},
        }
        all_rows = []
        offset = 0
        while True:
            body = {"showTarrifFreeFeeds": False, "callFrom": "feed", "query": base_query,
                    "offset": offset, "order": {"type": "price", "direction": "ASC"}}
            r = s.post("https://isle-of-wight-jewellery.co.uk/apps/diamond-proxy/api/v2/feed",
                       json=body, headers=headers, timeout=30)
            rows = r.json()
            if not rows:
                break
            all_rows.extend(rows)
            offset += len(rows)
            if len(rows) < 16:  # short page = last page (observed page size ~16-20)
                break
            time.sleep(0.3)
        pc = []
        for row in all_rows:
            carat = None
            for part in row["title"].split("ct", 1):
                try:
                    carat = float(part.strip())
                except ValueError:
                    pass
                break
            if carat:
                pc.append((row["price"], carat))
        results[label] = {"n": len(pc), "median_ppc": median_ppc(pc)}
        print(f"  Isle of Wight Jewellery {label}: n={len(pc)} median={results[label]['median_ppc']}")
    return results


if __name__ == "__main__":
    out = {}
    print("Diamond Labs...")
    out["diamond_labs"] = scrape_diamond_labs()
    print("Quality Diamonds...")
    out["quality_diamonds"] = scrape_quality_diamonds()
    print("77 Diamonds...")
    out["77_diamonds"] = scrape_77diamonds()
    # Serendipity Jewellery - Isle of Wight deliberately excluded -- see the
    # scrape_iow_jewellery() docstring/comment above before re-enabling.

    with open("uk_benchmark_results.json", "w") as f:
        json.dump(out, f, indent=2)
    print("\nSaved to uk_benchmark_results.json")

    # Listing-level retention: dump the raw API rows per retailer, then gzip
    # into data/snapshots/. Prices in these rows are ex-VAT GBP except where
    # the retailer notes above say otherwise.
    from datetime import date as _date
    from pathlib import Path as _Path
    run_date = _date.today().isoformat()
    raw_dir = _Path(__file__).parent / "data" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    for slug, bands in RAW_ROWS.items():
        jsonl_path = raw_dir / f"uk_{slug}_{run_date}.jsonl"
        with jsonl_path.open("w", encoding="utf-8") as f:
            for band_label, rows in bands.items():
                for row in rows:
                    f.write(json.dumps({"band": band_label, "listing": row}) + "\n")
        n = sum(len(rows) for rows in bands.values())
        print(f"Raw listings: {n} rows -> {jsonl_path.name}")
    from snapshot import sync as _snapshot_sync
    _snapshot_sync()

    print(json.dumps(out, indent=2))
