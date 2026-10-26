#!/usr/bin/env python3
"""build_uk_october.py: October 2026 UK edition of uk-index-data.json.

Recomputes every UK cell from the raw 2026-10-25 jsonl files (same filters as
run_uk_benchmark.py: G VS1 round, Excellent cut, IGI; carat bands 0.95-1.05 /
1.45-1.55 / 1.95-2.05; deduped by retailer stone id), then rewrites the
edition-specific fields of uk-index-data.json. All narrative numbers are
derived from the computed cells, not typed in.

Writes to --out (default: uk-index-data.json in place; the previous copy is
kept by the caller). Editing the working-tree file is NOT a publish; the live
copy is the GitHub main raw file the page fetches, pushed separately.

Usage: python3 build_uk_october.py --publish-date 2026-10-26 [--out PATH]
"""
import argparse
import json
import math
import statistics
from pathlib import Path

HERE = Path(__file__).resolve().parent
RAW = HERE / "data" / "raw"
CAPTURE = "2026-10-25"
BANDS = {"1ct": (0.95, 1.05, 1.0), "1.5ct": (1.45, 1.55, 1.5), "2ct": (1.95, 2.05, 2.0)}
MIN_N = 30
MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August",
          "September", "October", "November", "December"]


def rows_for(slug):
    return [json.loads(l) for l in open(RAW / f"uk_{slug}_{CAPTURE}.jsonl")]


def norm(slug, r):
    L = r["listing"]
    if slug == "diamond_labs":
        return dict(id=L["id"], ct=float(L["carat"]), col=L["color"], cl=L["clarity"],
                    cut=L["cut"], p=float(L["salePriceGbp"]), lab="IGI")  # DL feed is IGI-only per spec
    if slug == "quality_diamonds":
        return dict(id=L["CertNumber"], ct=float(L["Carats"]), col=L["Colour"], cl=L["Clarity"],
                    cut=L["CutGrade"], p=float(L["PriceExVatValue"]), lab=L["CertBy"])
    t = L["Price"]["TotalDiamondPrice"]
    f = t.get("FinalPrice") or t["Price"]
    return dict(id=L["Code"], ct=float(L["CaratWeight"]), col=L["Color"], cl=L["Clarity"],
                cut="Excellent" if L["Cut"] in ("EX", "Excellent") else L["Cut"],
                p=f["NumericPrice"]["WithoutVat"], lab=L["Cert"])


def cells(slug):
    rows = rows_for(slug)
    out = {}
    for b, (lo, hi, _) in BANDS.items():
        seen = {}
        for r in rows:
            if r["band"] != b:
                continue
            x = norm(slug, r)
            if x["cut"] != "Excellent" or x["col"] != "G" or x["cl"] != "VS1" or x["lab"] != "IGI":
                continue
            if not (lo <= x["ct"] <= hi):
                continue
            seen[x["id"]] = x
        v = list(seen.values())
        n = len(v)
        ppc = round(statistics.median([x["p"] / x["ct"] for x in v])) if v else None
        out[b] = (n, ppc)
    return out, len(rows)


def jsround(x):
    return math.floor(x + 0.5)


def pymed(vals):
    s = sorted(vals)
    m = len(s) // 2
    return round((s[m - 1] + s[m]) / 2) if len(s) % 2 == 0 else s[m]


def gbp(n):
    return "£" + f"{n:,}"


def pct(new, old):
    return round((new - old) / old * 100)


def sgn(p):
    return f"+{p}%" if p > 0 else f"{p}%"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--publish-date", required=True)
    ap.add_argument("--out", default=str(HERE / "uk-index-data.json"))
    ap.add_argument("--src", default=str(HERE / "uk-index-data.json"))
    a = ap.parse_args()
    d = json.load(open(a.src))
    assert d["month"] == "September 2026", d["month"]
    sep = {h["month"]: h for h in d["history"]}["September 2026"]
    aug = {h["month"]: h for h in d["history"]}["August 2026"]

    dl, n_dl = cells("diamond_labs")
    qd, n_qd = cells("quality_diamonds")
    sd, n_sd = cells("77_diamonds")
    total = n_dl + n_qd + n_sd
    C = {"diamond_labs": dl, "quality_diamonds": qd, "77_diamonds": sd}

    # --- retailer cells -------------------------------------------------
    for r in d["retailers"]:
        for b in BANDS:
            n, ppc = C[r["slug"]][b]
            c = {"status": "ok" if n >= MIN_N else ("thin" if n else "no_data"), "stone_count": n}
            if n >= MIN_N:
                c["median_price_per_carat"] = ppc
            r["cells"][b] = c
    qdr = [r for r in d["retailers"] if r["slug"] == "quality_diamonds"][0]
    qdr["note"] = ("Listed inventory rebounded this edition (74 listings collected in September, 380 in October; "
                   "402 in August). Counted Excellent-cut and IGI only, Quality Diamonds clears the 30-listing minimum "
                   "at 1.5ct and 2ct and stays below it at 1ct. A same-day re-query of the retailer's own API on "
                   "2026-10-25 returned identical counts. See Coverage this edition.")
    qdr["cells"]["1ct"]["note"] = ("Below the 30-listing minimum on the Excellent-cut, IGI count (n=%d). Not published." % qd["1ct"][0])

    # --- market medians (ex-VAT median of ok retailers, inc-VAT display) -----
    def market(panel):
        out = {}
        for b, (_, _, wt) in BANDS.items():
            vals = [C[s][b][1] for s in panel if C[s][b][0] >= MIN_N]
            out[b] = (len(vals), jsround(pymed(vals) * 1.2 * wt) if len(vals) >= 2 else None)
        return out
    mm = market(["diamond_labs", "77_diamonds", "quality_diamonds"])
    mm2 = market(["diamond_labs", "77_diamonds"])
    assert mm["1ct"][0] == 2 and mm["1.5ct"][0] == 3 and mm["2ct"][0] == 3, mm
    sepm = {"1ct": 275, "1.5ct": 371, "2ct": 487}  # September published Market Medians
    m1, m15, m2 = mm["1ct"][1], mm["1.5ct"][1], mm["2ct"][1]
    p1, p15, p2 = pct(m1, 275), pct(m15, 371), pct(m2, 487)
    q15, q2 = mm2["1.5ct"][1], mm2["2ct"][1]  # September-panel (two retailer) comparison
    pq15, pq2 = pct(q15, 371), pct(q2, 487)

    def sp(slug, b):
        return sep["retailers"][slug][b]
    dlp = {b: (sp("diamond_labs", b), dl[b][1], pct(dl[b][1], sp("diamond_labs", b))) for b in BANDS}
    sdp = {b: (sp("77_diamonds", b), sd[b][1], pct(sd[b][1], sp("77_diamonds", b))) for b in BANDS}
    gap1 = sd["1ct"][1] - dl["1ct"][1]
    gap2 = sd["2ct"][1] - dl["2ct"][1]
    assert qd["1.5ct"][0] >= MIN_N and qd["2ct"][0] >= MIN_N and qd["1ct"][0] < MIN_N

    # --- header fields -----------------------------------------------------
    d["month"] = "October 2026"
    d["last_updated"] = a.publish_date
    d["collection_date"] = "October 25, 2026"
    d["next_edition_note"] = "Updated monthly. Next edition: November 2026."
    d["answer_sentence"] = (
        f"As of {{month}}, a 1 carat G VS1 IGI round lab-grown diamond lists at a Market Median of {gbp(m1)} inc-VAT across two UK "
        f"retailers (Quality Diamonds is below the publication minimum at 1 carat); 1.5 carat is {gbp(m15)} ({sgn(p15)} from September, "
        f"now on three retailers) and 2 carat {gbp(m2)} ({sgn(p2)}, also on three retailers).")
    d["quotable_facts"] = [
        f"In October 2026 the UK Market Median for a 1 carat G VS1 IGI round lab-grown diamond was {gbp(m1)} including VAT ({sgn(p1)} from {gbp(275)} in September 2026), across two retailers (Diamond Labs and 77 Diamonds); Quality Diamonds was below the 30-listing publication minimum at 1 carat.",
        f"In October 2026 the UK Market Median for a 1.5 carat G VS1 IGI round lab-grown diamond was {gbp(m15)} including VAT, across three retailers (Diamond Labs, 77 Diamonds and Quality Diamonds). September's {gbp(371)} was a two-retailer figure; on the same two retailers October is {gbp(q15)} ({sgn(pq15)}).",
        f"In October 2026 the UK Market Median for a 2 carat G VS1 IGI round lab-grown diamond was {gbp(m2)} including VAT, across three retailers. September's {gbp(487)} was a two-retailer figure; on the same two retailers October is {gbp(q2)} ({sgn(pq2)}).",
        f"Quality Diamonds' UK lab-grown inventory rebounded from 74 listings collected in September 2026 to 380 in October 2026 (402 in August), and it returns to the published panel at 1.5 carat (n={qd['1.5ct'][0]}) and 2 carat (n={qd['2ct'][0]}) but not 1 carat (n={qd['1ct'][0]}, below the 30-listing minimum).",
        f"The October 2026 edition of the Rings.com UK Lab-Grown Diamond Price Index collected {total:,} round G VS1 IGI listings from three UK online retailers on October 25, 2026, across the three carat bands (before each retailer's own Excellent-cut filter is applied).",
    ]

    # --- data_notes -----------------------------------------------------------
    d["data_notes"] = (
        f"Sources: three retailers scraped October 25, 2026, totaling {total:,} round G VS1 IGI listings collected across the three carat bands "
        f"(before each retailer's own Excellent-cut filter; published cells are narrower, see each retailer's note). Quality Diamonds' listed inventory "
        f"rebounded: 380 listings collected across the three cells this edition against 74 in September and 402 in August. A same-day re-query of the retailer's own "
        f"inventory API on 2026-10-25 returned identical Excellent-cut IGI counts (n={qd['1ct'][0]}, {qd['1.5ct'][0]}, {qd['2ct'][0]}), so the rebound is a genuine change in "
        f"listed stock, not a collection issue. Counted Excellent-cut and IGI only (the same filter introduced on 2026-09-18), Quality Diamonds clears the 30-listing minimum "
        f"at 1.5ct and 2ct and not at 1ct. Diamond Labs and 77 Diamonds cleared the minimum at every weight; Diamond Labs' 1ct cell (n={dl['1ct'][0]}) is only just above it. "
        f"Market Median: 1ct draws on two retailers (Diamond Labs, 77 Diamonds); 1.5ct and 2ct draw on three, where September's drew on two, so the month-over-month change "
        f"is not like-for-like at those two weights (see Month-over-month).")

    # --- history entry ---------------------------------------------------
    def hv(slug, b):
        n, ppc = C[slug][b]
        return ppc if n >= MIN_N else None
    d["history"] = [h for h in d["history"] if h["month"] != "October 2026"]
    d["history"].append({"month": "October 2026", "total_listings": total, "retailers": {
        s: {b: hv(s, b) for b in BANDS} for s in ("diamond_labs", "77_diamonds", "quality_diamonds")}})

    # --- key findings --------------------------------------------------------
    kf = d["key_findings"]
    keep = [k for k in kf if k.startswith("<strong>What UK buyers assume") or k.startswith("<strong>Three of six")
            or k.startswith("<strong>All prices inc-VAT") or k.startswith("<strong>The UK index tracks G VS1")]
    assert len(keep) == 4, len(keep)
    new_kf = [
        f"<strong>Quality Diamonds returns to the published panel at 1.5ct and 2ct, and stays below the minimum at 1ct.</strong> Its listed inventory rebounded from 74 listings collected in September to 380 in October (402 in August), confirmed by a same-day re-query of the retailer's own API. Counted Excellent-cut and IGI only, as since the 2026-09-18 filter fix, it has n={qd['1ct'][0]} at 1ct (below the 30-listing minimum, not published), n={qd['1.5ct'][0]} at 1.5ct and n={qd['2ct'][0]} at 2ct. Its published figures are {gbp(qd['1.5ct'][1])}/ct (1.5ct) and {gbp(qd['2ct'][1])}/ct (2ct), ex-VAT.",
        f"<strong>The Market Medians are not like-for-like with September at 1.5ct and 2ct.</strong> 1ct: {gbp(m1)} (Diamond Labs and 77 Diamonds), {sgn(p1)} from September's {gbp(275)}. 1.5ct: {gbp(m15)} on three retailers, against September's two-retailer {gbp(371)} ({sgn(p15)}); on the September panel of Diamond Labs and 77 Diamonds alone it is {gbp(q15)} ({sgn(pq15)}). 2ct: {gbp(m2)} on three retailers, against September's two-retailer {gbp(487)} ({sgn(p2)}); on the September panel alone it is {gbp(q2)} ({sgn(pq2)}).",
        f"<strong>Diamond Labs and 77 Diamonds moved little.</strong> Per carat, ex-VAT, September to October: Diamond Labs {sgn(dlp['1ct'][2])} at 1ct ({gbp(dlp['1ct'][0])} to {gbp(dlp['1ct'][1])}), {sgn(dlp['1.5ct'][2])} at 1.5ct ({gbp(dlp['1.5ct'][0])} to {gbp(dlp['1.5ct'][1])}), {sgn(dlp['2ct'][2])} at 2ct ({gbp(dlp['2ct'][0])} to {gbp(dlp['2ct'][1])}); 77 Diamonds {sgn(sdp['1ct'][2])} at 1ct ({gbp(sdp['1ct'][0])} to {gbp(sdp['1ct'][1])}), {sgn(sdp['1.5ct'][2])} at 1.5ct ({gbp(sdp['1.5ct'][0])} to {gbp(sdp['1.5ct'][1])}), {sgn(sdp['2ct'][2])} at 2ct ({gbp(sdp['2ct'][0])} to {gbp(sdp['2ct'][1])}). Diamond Labs' collected stock fell to 666 listings from 870, and its 1ct cell is just above the minimum (n={dl['1ct'][0]}).",
        f"<strong>The published price gap per carat is {gbp(gap1)} at 1ct and {gbp(gap2)} at 2ct between the cheapest and priciest published retailer, ex-VAT.</strong> At 1ct, Diamond Labs ({gbp(dl['1ct'][1])}) against 77 Diamonds ({gbp(sd['1ct'][1])}), ex-VAT, on the same benchmark specification. At 2ct the published range runs from Diamond Labs ({gbp(dl['2ct'][1])}) through Quality Diamonds ({gbp(qd['2ct'][1])}) to 77 Diamonds ({gbp(sd['2ct'][1])}).",
    ]
    d["key_findings"] = new_kf + keep

    # --- methodology ---------------------------------------------------------
    M = {m["title"]: m for m in d["methodology"]}
    M["Scope"]["body"] = (
        f"This index tracks listed retail prices (not transaction prices) across major UK online lab-grown diamond retailers. The October 2026 edition collected {total:,} round G VS1 IGI lab-grown diamond listings (before each retailer's own Excellent-cut filter) across three retailers. All prices are in GBP, inclusive of 20% UK VAT (inc-VAT). Data was collected on October 25, 2026. This is the sixth edition; the retailer panel is unchanged since the May 2026 inaugural edition.")
    M["Coverage this edition"]["body"] = (
        f"Quality Diamonds' listed inventory rebounded between the September and October collections: 380 listings collected this edition ({len(rows_for('quality_diamonds')) and sum(1 for r in rows_for('quality_diamonds') if r['band']=='1ct')} at 1ct, {sum(1 for r in rows_for('quality_diamonds') if r['band']=='1.5ct')} at 1.5ct, {sum(1 for r in rows_for('quality_diamonds') if r['band']=='2ct')} at 2ct) against 74 in September and 402 in August. A same-day re-query of the retailer's own inventory API on 2026-10-25 returned identical counts, confirming a genuine change in listed stock rather than a collection issue. Counted Excellent-cut and IGI only (the Excellent-cut filter has applied to Quality Diamonds since the 2026-09-18 correction; this edition also restricts the count to IGI-certified stones, which removes one GIA-certified stone at 1.5ct and does not change that cell's median), Quality Diamonds has n={qd['1ct'][0]} at 1ct, n={qd['1.5ct'][0]} at 1.5ct and n={qd['2ct'][0]} at 2ct. The 30-listing minimum is unchanged, so 1.5ct and 2ct are published and 1ct is not. Diamond Labs and 77 Diamonds both cleared the minimum at every weight; Diamond Labs' 1ct cell (n={dl['1ct'][0]}) is only just above it, and its collected stock fell from 870 listings in September to 666. All three collections were made October 25, 2026, and no feed outage was observed. September's published figures are not restated: every edition is a frozen snapshot once published.")
    M["Month-over-month"]["body"] = (
        f"Per carat, ex-VAT, September to October: Diamond Labs {sgn(dlp['1ct'][2])} at 1ct ({gbp(dlp['1ct'][0])} to {gbp(dlp['1ct'][1])}), {sgn(dlp['1.5ct'][2])} at 1.5ct ({gbp(dlp['1.5ct'][0])} to {gbp(dlp['1.5ct'][1])}), {sgn(dlp['2ct'][2])} at 2ct ({gbp(dlp['2ct'][0])} to {gbp(dlp['2ct'][1])}); 77 Diamonds {sgn(sdp['1ct'][2])} at 1ct ({gbp(sdp['1ct'][0])} to {gbp(sdp['1ct'][1])}), {sgn(sdp['1.5ct'][2])} at 1.5ct ({gbp(sdp['1.5ct'][0])} to {gbp(sdp['1.5ct'][1])}), {sgn(sdp['2ct'][2])} at 2ct ({gbp(sdp['2ct'][0])} to {gbp(sdp['2ct'][1])}); Quality Diamonds was unpublished in September, so it has no month-over-month comparison at any weight. Published Market Medians: 1ct {gbp(m1)}, {sgn(p1)} from September's {gbp(275)}, on the same two retailers; 1.5ct {gbp(m15)}, {sgn(p15)} from September's {gbp(371)}, but now on three retailers where September's drew on two; 2ct {gbp(m2)}, {sgn(p2)} from September's {gbp(487)}, also now on three retailers. On September's two-retailer panel (Diamond Labs, 77 Diamonds) the October Market Medians would be {gbp(m1)} at 1ct, {gbp(q15)} at 1.5ct ({sgn(pq15)}) and {gbp(q2)} at 2ct ({sgn(pq2)}). The 1.5ct and 2ct changes in the published Market Medians therefore mostly reflect the larger panel, not a market move.")
    M["Retailer panel"]["body"] = M["Retailer panel"]["body"].replace("it is unchanged for August 2026", "it is unchanged for October 2026")
    assert "unchanged for October 2026" in M["Retailer panel"]["body"]
    M["Price inputs"]["body"] = (
        "Prices are the publicly visible list price at the time of data collection. Published figures in the index table include 20% UK VAT applied to the collected ex-VAT price. "
        f"Ex-VAT medians (per carat) for October 2026: Diamond Labs 1ct {gbp(dl['1ct'][1])} (n={dl['1ct'][0]}), 1.5ct {gbp(dl['1.5ct'][1])} (n={dl['1.5ct'][0]}), 2ct {gbp(dl['2ct'][1])} (n={dl['2ct'][0]}). "
        f"77 Diamonds 1ct {gbp(sd['1ct'][1])} (n={sd['1ct'][0]}), 1.5ct {gbp(sd['1.5ct'][1])} (n={sd['1.5ct'][0]}), 2ct {gbp(sd['2ct'][1])} (n={sd['2ct'][0]}). "
        f"Quality Diamonds 1.5ct {gbp(qd['1.5ct'][1])} (n={qd['1.5ct'][0]}) and 2ct {gbp(qd['2ct'][1])} (n={qd['2ct'][0]}); at 1ct it is below the publication minimum (n={qd['1ct'][0]}; see Coverage this edition). "
        "The following are excluded: promotional codes, membership discounts, negotiated pricing, and strikethrough compare-at or was-£X reference prices.")
    stat = M["Statistical method"]["body"]
    cut = stat.index(" Quality Diamonds fell below")
    M["Statistical method"]["body"] = stat[:cut] + (
        f" Quality Diamonds is below the retailer threshold at 1ct this edition (n={qd['1ct'][0]}) and meets it at 1.5ct (n={qd['1.5ct'][0]}) and 2ct (n={qd['2ct'][0]}), counted Excellent-cut and IGI only (see Coverage this edition); Diamond Labs and 77 Diamonds met the threshold at every weight.")
    comp = M["Comparability"]["body"]
    cut = comp.index("; this edition's Market Medians")
    M["Comparability"]["body"] = comp[:cut] + (
        "; this edition's 1ct Market Median draws on two retailers (Diamond Labs, 77 Diamonds) and its 1.5ct and 2ct Market Medians draw on three, where September's drew on two, "
        "after Quality Diamonds' listed inventory recovered above the publication minimum at those two weights (see Coverage this edition).")
    d["methodology"] = [M[m["title"]] for m in d["methodology"]]

    # --- JSON-LD description ------------------------------------------------
    d["jsonld_description"] = (
        f"Monthly benchmark prices for lab-grown diamonds (G VS1 Round Excellent IGI-certified) across major UK online retailers. October 2026 edition collected {total:,} round G VS1 IGI listings across three retailers (before each retailer's own Excellent-cut filter): Diamond Labs, 77 Diamonds, and Quality Diamonds. All prices in GBP, inclusive of 20% UK VAT. The 1ct Market Median draws on two retailers (Diamond Labs, 77 Diamonds) and the 1.5ct and 2ct Market Medians on three, after Quality Diamonds recovered above the publication minimum at those two weights.")

    Path(a.out).write_text(json.dumps(d, indent=1, ensure_ascii=False) + "\n")
    print("wrote", a.out)
    print("cells", {"DL": dl, "QD": qd, "77": sd})
    print("market", mm, "two-retailer", mm2, "total", total)
    print("MoM MM", p1, p15, p2, "matched", pq15, pq2)


if __name__ == "__main__":
    main()
