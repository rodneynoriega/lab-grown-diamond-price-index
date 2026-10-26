#!/usr/bin/env python3
"""Build the "Same certified stone, different prices" block (OFF by default).

Experiment approved by Rodney 2026-10-10 (OUTBOX/2026-10-10-october-refresh-
plan-and-spread-experiment.md, section 3). This script only STAGES files; it
never publishes. Output (under --out):

  spread_block_<cycle>.html      the fragment (insertion-only into the hub)
  spread_block_<cycle>.json      selection record: stones, prices, product
                                 URLs, pool stats (the gate evidence)
  hub_with_spread_block.html     preview: last published hub body + fragment

Selection rule (fixed in advance, not tuned to the output):
  * pool = stones that have a LIVE stone page (sitemap) and >= 2 displayed
    current-cycle prices (page_pipeline.select_stones, which already
    requires matching shape/carat/color/clarity across retailers)
  * guard added 2026-10-10 after adversarial review: retailer-assigned
    cut/polish/symmetry must not CONFLICT across the displayed retailers
    (blank is not a conflict), because a "same stone" gap that is partly a
    different cut label is not a clean comparison; stone pages already
    follow this convention
  * one stone per cell: carat band (1.5, 2.0) x shape (round, oval), the
    largest dollar gap in the cell. No live stone page sits within 0.09 ct
    of 1.00, so the 1 carat slot cannot be filled.
"""
import argparse
import csv
import datetime
import html as _html
import json
import re
import statistics
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import page_pipeline as pp  # noqa: E402

CELLS = [(1.5, "round"), (1.5, "oval"), (2.0, "round"), (2.0, "oval")]


def hub_retailer_medians(hub_html):
    """{retailer: {1.0: n, 1.5: n, 2.0: n}} from the hub's price table (all colors/clarities)."""
    t = hub_html[hub_html.find("<table"):hub_html.find("</table>")]
    out = {}
    for row in re.findall(r"<tr.*?</tr>", t, re.S):
        cells = [_html.unescape(re.sub(r"<[^>]+>", " ", c)).strip() for c in re.findall(r"<t[dh].*?</t[dh]>", row, re.S)]
        if len(cells) < 4 or cells[0].lower().startswith("retailer"):
            continue
        name = re.sub(r"[^A-Za-z ]", "", cells[0]).strip()
        vals = []
        for c in cells[1:4]:
            m = re.search(r"\$([\d,]+)", c)
            vals.append(float(m.group(1).replace(",", "")) if m else None)
        out[name] = dict(zip((1.0, 1.5, 2.0), vals))
    return out


def live_stone_handles():
    ua = "Mozilla/5.0 (compatible; Googlebot/2.1)"
    sm = subprocess.run(["curl", "-s", "-A", ua, "https://rings.com/sitemap.xml"],
                        capture_output=True, text=True).stdout
    live = set()
    for u in re.findall(r"<loc>([^<]*sitemap_pages[^<]*)</loc>", sm):
        u = u.replace("&amp;", "&")
        t = subprocess.run(["curl", "-s", "-A", ua, u],
                           capture_output=True, text=True).stdout
        live |= set(re.findall(r"/pages/(lab-diamond-igi-[0-9a-z]+)", t))
    return live


def S(v):
    return str(v).strip().upper() if isinstance(v, str) and str(v).lower() != "nan" else ""


def conflict(rows):
    for f in ("cut", "polish", "symmetry"):
        if len({S(getattr(r, f, "")) for r in rows} - {""}) > 1:
            return True
    return False


def band(c):
    for p in (1.5, 2.0):
        if round(abs(c - p), 2) <= pp.CARAT_BAND:
            return p
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cycle", default="2026-09")
    ap.add_argument("--out", default=str(HERE / "page_output_2026-10-spread"))
    ap.add_argument("--hub-base", default=str(
        HERE / "page_output_2026-10-hub-mobile3" / "pages" /
        "lab-grown-diamond-price-comparison.html"))
    a = ap.parse_args()
    pp.configure(a.cycle)
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)

    frame = pp.load_frame()
    stones, _ = pp.build_stone_parquet(frame)
    strict, mism = pp.select_stones(stones, pinned=pp.pinned_cert_keys())
    live = live_stone_handles()

    def handle(k):
        lab, num = k.split(":")
        return f"lab-diamond-{lab.lower()}-{num.lower()}"

    pool = [s for s in strict if handle(s["cert_key"]) in live]
    gaps = [s["spread"] for s in pool]
    picked, skipped, tie_log = [], [], {}
    for (b, shp) in CELLS:
        cell = []
        for s in pool:
            rows = list(s["displayed"].values())
            if band(rows[0].carat) != b or rows[0].shape != shp:
                continue
            if conflict(rows):
                continue
            cell.append(s)
        cell.sort(key=lambda s: (-s["spread"], s["cert_key"]))
        if not cell:
            skipped.append((b, shp))
            continue
        top = cell[0]["spread"]
        ties = [x["cert_key"] for x in cell if x["spread"] == top]
        tie_log[f"{b}-{shp}"] = ties
        picked.append(cell[0])

    env = pp.jenv()
    ctx_stones, rec = [], []
    for s in picked:
        rows = [s["displayed"][r] for r in pp.STONE_DISPLAY if r in s["displayed"]]
        r0 = rows[0]
        prices = sorted((r.price_usd, r.retailer) for r in rows)
        lo_p, lo_r = prices[0]
        hi_p, hi_r = prices[-1]
        h = handle(s["cert_key"])
        lab, num = s["cert_key"].split(":")
        label = f"{pp.carat_str(r0.carat)} carat {r0.shape}, {r0.color} {r0.clarity}"
        ctx_stones.append({
            "href": f"/pages/{h}", "label": label, "cert": num,
            "rows": [{"retailer": r.retailer, "price": r.price_usd} for r in
                     sorted(rows, key=lambda r: r.price_usd)],
            "gap": s["spread"], "gap_pct": f"{s['spread_pct'] * 100:,.0f}",
            "hi_r": hi_r, "lo_r": lo_r})
        rec.append({"handle": h, "cert": f"{lab} {num}", "label": label,
                    "cut": S(r0.cut), "polish": S(r0.polish),
                    "symmetry": S(r0.symmetry),
                    "prices": {r.retailer: r.price_usd for r in rows},
                    "product_urls": {r.retailer: r.product_url for r in rows},
                    "scraped_at": {r.retailer: str(r.scraped_at) for r in rows},
                    "gap": s["spread"]})

    all_top_grade = all(r["label"].endswith("D IF") for r in rec)
    his = {c["hi_r"] for c in ctx_stones}
    common_hi = his.pop() if len(his) == 1 else None
    ties_ctx = []
    for (b, shp) in CELLS:
        t = tie_log.get(f"{b}-{shp}", [])
        if len(t) > 1:
            m = next(c for c in ctx_stones if c["cert"] == picked[CELLS.index((b, shp))]["cert_key"].split(":")[1])
            ties_ctx.append({"label": f"{m['label'].split(',')[0]}", "n": len(t)})
    html = env.get_template("spread_block.html.j2").render(
        **pp.base_ctx(), stones=ctx_stones, n_stones=len(pool), common_hi=common_hi, ties=ties_ctx, all_top_grade=all_top_grade,
        median_gap=statistics.median(gaps))
    # --- wording fixes applied to the rendered fragment (template is owned by another chain) ---
    old_a = "These are the largest dollar gaps we found in each of four groups"
    assert html.count(old_a) == 1, "intro anchor changed"
    html = html.replace(old_a, "These are the largest dollar gaps among stones with a page on this site in each of four groups")
    old_b = f"Across the {len(pool)} stones that are listed"
    assert html.count(old_b) == 1, "pool sentence anchor changed"
    html = html.replace(old_b, f"Across the {len(pool)} IGI-certified stones that are listed")  # pool is IGI-only; 3 GIA stones also qualify (round-2 review)
    hub_text = Path(a.hub_base).read_text()
    hub_med = hub_retailer_medians(hub_text)
    above_all, above_log = True, []
    for st_, c_ in zip(rec, ctx_stones):
        bnd = 1.5 if "1.5" in st_["label"].split()[0][:3] or float(st_["label"].split()[0]) < 1.75 else 2.0
        for ret, price in st_["prices"].items():
            med = (hub_med.get(ret) or {}).get(bnd)
            ok = med is not None and price > med
            above_log.append((st_["cert"], ret, price, bnd, med, ok))
            above_all &= ok
    if not all_top_grade:
        cols = {r["label"].split(", ")[1].split()[0] for r in rec}
        clars = [r["label"].split(", ")[1].split()[1] for r in rec]
        from collections import Counter
        cc_ = Counter(clars)
        order = [k for k in ("FL", "IF", "VVS1", "VVS2", "VS1", "VS2") if k in cc_]
        clar_txt = ", ".join(f"{cc_[k]} {k}" for k in order[:-1]) + (" and " if len(order) > 1 else "") + f"{cc_[order[-1]]} {order[-1]}"
        col_txt = (f"{list(cols)[0]} color" if len(cols) == 1 else "colors " + ", ".join(sorted(cols)))
        cav = f" All {len(rec)} are {col_txt}, {clar_txt}, among the highest grades."
        if above_all:
            cav += " At every retailer shown, each stone's price is above that retailer's own median for the weight in the table above, which covers all colors and clarities."
        else:
            cav += " Their prices are not typical of the medians in the table above, which cover all colors and clarities."
        anchor = "not a ranking of retailers."
        assert html.count(anchor) == 1, "grade-caveat anchor changed"
        html = html.replace(anchor, anchor + cav)
    frag = out / f"spread_block_{a.cycle}.html"
    frag.write_text(html)
    # reproducibility: the exact pool behind "N stones" and the median gap
    with open(out / f"spread_pool_{a.cycle}.csv", "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["handle", "cert_key", "shape", "carat", "retailers", "prices_by_retailer", "spread_usd", "conflict_guard_excludes"])
        for s_ in sorted(pool, key=lambda x: x["cert_key"]):
            rows_ = list(s_["displayed"].values())
            w.writerow([handle(s_["cert_key"]), s_["cert_key"], rows_[0].shape, rows_[0].carat, "|".join(r.retailer for r in rows_),
                        "|".join(f"{r.retailer}:{r.price_usd:g}" for r in rows_), s_["spread"], conflict(rows_)])
    n_oval = sum(1 for s_ in pool if list(s_["displayed"].values())[0].shape == "oval")
    n_round = sum(1 for s_ in pool if list(s_["displayed"].values())[0].shape == "round")
    n_excl = sum(1 for s_ in pool if conflict(list(s_["displayed"].values())))
    (out / f"spread_block_{a.cycle}.json").write_text(json.dumps({
        "cycle": a.cycle, "pool_live_stones": len(pool),
        "median_gap": statistics.median(gaps), "cells_unfilled": skipped,
        "ties_at_top_gap": tie_log,
        "tie_break": "lexicographically first cert_key among equal top gaps (deterministic, not tuned)",
        "scraped_at_note": f"Grown Brilliance, Ritani and Clean Origin scraped_at values are a 12:00:00Z placeholder in the {a.cycle} files, so 'listed {pp.DATA_DATE_H}' rests on the cycle date ({pp.DATA_TIMESTAMP}) recorded in the scrape log and file names, not on a per-row timestamp. Brilliant Earth, Blue Nile and VRAI carry real timestamps (2026-10-24 UTC).",
        "retailer_grade_mapping": "retailers/base.py maps Ideal and Super Ideal to Excellent before the conflict guard",
        "pool_note": f"pool = {len(pool)} stones with a live page in the sitemap fetched at {datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M UTC')} AND in the strict list of page_pipeline.select_stones(stones, pinned=pinned_cert_keys()) for cycle {a.cycle}: {n_oval} ovals, {n_round} rounds. The median gap in the block text is statistics.median of the 'spread' (highest minus lowest displayed price) of ALL {len(pool)} pool stones, including {n_excl} that the cut/polish/symmetry conflict guard would exclude from the picked four (see spread_pool_{a.cycle}.csv, column spread_usd). The live-page test depends on the sitemap at run time, so re-running later can change the count.",
        "median_gap_how": "statistics.median(s['spread'] for s in pool); spread = highest minus lowest displayed price across the displayed retailers (page_pipeline.STONE_DISPLAY order) for that cert. Not computed from raw CSVs directly: select_stones requires a cert present at 2+ displayed retailers with matching shape/carat/color/clarity.",
        "above_median_check": [dict(zip(("cert", "retailer", "price", "band", "hub_median", "above"), x)) for x in above_log],
        "stones": rec}, indent=1) + "\n")

    base = Path(a.hub_base).read_text()
    # insertion-only: after the price-table footnote, before the email capture
    t = base.find("</table>")
    at = base.find('\n\n<div class="lgd-email-capture"', t)
    assert t > 0 and at > t, "hub base anchor not found"
    assert "lgd-spread-block" not in base, "hub base already has the block"
    staged = base[:at] + "\n\n" + html.rstrip("\n") + base[at:]
    (out / "hub_with_spread_block.html").write_text(staged)
    print(f"pool {len(pool)} live stones; picked {len(picked)}; unfilled {skipped}")
    for r in rec:
        print(r["handle"], r["label"], r["prices"], r["gap"])
    print("wrote", frag)


if __name__ == "__main__":
    main()
