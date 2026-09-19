"""Sold-Price Index back-test v1 — delist-as-sale over the existing archive.

Spec: CONTEXT/2026-08-30-from-ideas-generator-sold-price-index-mvp-spec.md
(Ideas Generator, 2026-08-30, incl. same-day coverage-swing addendum).
Computation only: reads data/processed/stones_pilot_2026-08.parquet, writes
the internal delist-event table to data/processed/ (gitignored; the
cert-level sold-event table stays internal per the spec's A6 boundary) and
prints the back-test report tables. Zero scraping, zero publishes.

Method, as pre-committed by the spec, with two implementation decisions the
spec leaves open (both disclosed in the report):

1. Delist event (retailer-level, spec step 1) = cert_key present at
   retailer R in cycle N, absent at R in N+1, taken only from
   retailer-pairs passing the coverage guard.
2. Coverage guard (addendum): per-retailer count swing beyond +/-30%
   between N and N+1 = COVERAGE BREAK; that retailer-pair contributes no
   events. Counts are the parquet's cert-bearing deduped rows (one per
   cert x retailer x cycle), the universe cert matching runs on.
3. Reappearance filter (spec step 2): the cert observed at ANY panel
   retailer within the 2 cycles after N = NOT a sale (same retailer =
   relist, takes precedence; else transfer). Positive presence is read
   from ALL data including break cycles (partial coverage corrupts
   absence, not presence). G2 = reappearance share of retailer-level
   delist events with a COMPLETE window (N+1 and N+2 both collected);
   the last pair's events are provisional until September exists.
4. IMPLEMENTATION DECISION A — sale inference is CERT-level, not
   event-level: an inferred sale = a cert whose last panel-wide
   observation is cycle N (absent everywhere in N+1 and N+2, window
   complete) and whose cycle-N listings sit entirely at clean-pair
   retailers (a cross-listing at a break-pair retailer makes the exit
   unjudgeable and excludes the cert, counted separately). This avoids
   double-counting a cross-listed stone's panel exit as two sales and
   avoids calling it a transfer twice (which the naive event-level
   reading does). Sale price = the cert's median cycle-N price.
5. Benchmark buckets: round, E, VS1, Excellent, IGI, carat point +/-0.09
   (the published cell definition) at 1.0 / 1.5 / 2.0ct. G1 widen-once
   rule: pool E-F color (VS1-VS2 pooling reported as sensitivity only).
6. IMPLEMENTATION DECISION B — the ask-sold spread is computed WITHIN
   retailer (sold and ask from the same retailer, single-retailer sales
   only), because the panel's price levels differ by 4-6x (Ritani ~$250/ct
   vs BE ~$1,300/ct) and any cross-retailer pooling measures retailer mix,
   not clearing behaviour (same lesson as the repricing study's
   "within-retailer" robustness row). Cells need n>=30 on both sides; a
   pair's G3 sign = sign of the median spread across its qualifying
   (retailer x bucket) cells.
7. Tenure (days-on-market) at N = capture date of N minus capture date of
   the first cycle of the contiguous presence run at that retailer; runs
   starting 2026-04 are left-censored and excluded from medians (share
   reported).

Panel limitation, disclosed: only the cert-capable retailers (Brilliant
Earth, Grown Brilliance, Ritani, With Clarity) can be cert-matched; Blue
Nile / VRAI / Clean Origin(<Aug) publish no usable cert numbers, so a stone
moving to those retailers is invisible here and would count as clean.
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
PARQUET = HERE / "data" / "processed" / "stones_pilot_2026-08.parquet"
OUT_EVENTS = HERE / "data" / "processed" / "delist_events_backtest_2026-08.csv.gz"
OUT_SALES = HERE / "data" / "processed" / "inferred_sales_backtest_2026-08.csv.gz"

CYCLES = ["2026-04", "2026-05", "2026-06", "2026-07", "2026-08"]
BREAK_THRESHOLD = 0.30          # +/-30% count swing = coverage break
REAPPEAR_CYCLES = 2             # reappearance window: N+1, N+2
BENCH_POINTS = [1.0, 1.5, 2.0]
CARAT_BAND = 0.09               # published cell definition: point +/- 0.09
G1_MIN_CLEAN = 30
G2_MAX_REAPPEAR = 0.40
MIN_CELL = 30                   # both sides of a spread cell


def load():
    df = pd.read_parquet(PARQUET)
    df = df[df["cycle"].isin(CYCLES)].copy()
    df["ppc"] = df["price_usd"] / df["carat"]
    df["obs_date"] = pd.to_datetime(df["scraped_at"].str[:10])
    return df


def capture_dates(df):
    """Capture date per (retailer, cycle) = modal observation date (files
    are single-run scrapes; max would let a stray resume row shift it)."""
    return (df.groupby(["retailer", "cycle"])["obs_date"]
              .agg(lambda s: s.mode().iloc[0]).to_dict())


def coverage(df):
    counts = (df.groupby(["retailer", "cycle"]).size()
                .unstack(fill_value=0).reindex(columns=CYCLES, fill_value=0))
    swings, breaks = {}, set()
    for r in counts.index:
        for i in range(len(CYCLES) - 1):
            a, b = counts.loc[r, CYCLES[i]], counts.loc[r, CYCLES[i + 1]]
            swing = (b - a) / a if a else np.inf
            swings[(r, i)] = swing
            if a == 0 or b == 0 or abs(swing) > BREAK_THRESHOLD:
                breaks.add((r, i))
    return counts, swings, breaks


def presence_maps(df):
    """retailer -> cert -> set(cycles present)."""
    return {r: g.groupby("cert_key")["cycle"].agg(set).to_dict()
            for r, g in df.groupby("retailer")}


def first_seen_contiguous(cycles_present, n_idx):
    j = n_idx
    while j > 0 and CYCLES[j - 1] in cycles_present:
        j -= 1
    return j


def build_events(df, breaks, per_ret, capdates):
    """Retailer-level delist events (spec step 1) with reappearance class."""
    retailers = sorted(df["retailer"].unique())
    rows_n = {(r, c): g.set_index("cert_key")
              for (r, c), g in df.groupby(["retailer", "cycle"])}
    events = []
    for i in range(len(CYCLES) - 1):
        cn, cn1 = CYCLES[i], CYCLES[i + 1]
        window = CYCLES[i + 1: i + 1 + REAPPEAR_CYCLES]
        window_complete = len(window) == REAPPEAR_CYCLES
        for r in retailers:
            if (r, i) in breaks:
                continue
            cur = rows_n.get((r, cn))
            if cur is None:
                continue
            nxt_certs = set(rows_n.get((r, cn1), pd.DataFrame()).index)
            gone = cur[~cur.index.isin(nxt_certs)]
            pr = per_ret[r]
            for cert, row in gone.iterrows():
                pres_here = pr.get(cert, set())
                same_re = any(w in pres_here for w in window)
                other_re = any(w in per_ret[o].get(cert, set())
                               for o in retailers if o != r
                               for w in window)
                n_at_n = 1 + sum(cn in per_ret[o].get(cert, set())
                                 for o in retailers if o != r)
                if same_re:
                    cls = "relist_same"
                elif other_re:
                    cls = "transfer_other"
                elif window_complete:
                    cls = "clean"
                else:
                    cls = "provisional_clean"
                fs = first_seen_contiguous(pres_here, i)
                dom = (capdates[(r, cn)] - capdates[(r, CYCLES[fs])]).days
                events.append({
                    "pair": f"{cn}->{cn1}", "pair_idx": i, "retailer": r,
                    "cert_key": cert, "carat": row["carat"],
                    "shape": row["shape"], "color": row["color"],
                    "clarity": row["clarity"], "cut": row["cut"],
                    "cert_lab": row["cert_lab"],
                    "price_usd": row["price_usd"], "ppc": row["ppc"],
                    "first_seen": CYCLES[fs], "dom_days": dom,
                    "dom_censored": fs == 0, "class": cls,
                    "cross_listed_at_n": n_at_n > 1,
                    "n_panel_listings_at_n": n_at_n,
                    "window_complete": window_complete,
                })
    return pd.DataFrame(events)


def infer_sales(ev):
    """Cert-level inferred sales (implementation decision A). Input: the
    clean/provisional retailer-level events. A cert with a break-pair
    cross-listing never generates a clean event AND absence of a
    same-cycle event at the break retailer cannot be trusted, so certs
    flagged cross_listed_at_n whose partner event is missing from the
    clean set are excluded (partner sat at a break retailer or was itself
    relisted/transferred, contradicting the panel exit)."""
    cand = ev[ev["class"].isin(["clean", "provisional_clean"])].copy()
    out, excluded_cross_break = [], 0
    for (pair, cert), g in cand.groupby(["pair", "cert_key"], sort=False):
        # every cycle-N listing of this cert must appear here as a
        # clean/provisional event; a cross-listing at a clean retailer
        # that did NOT delist would have classed these events as
        # transfer_other already, so the only failure mode left is a
        # partner listing at a break-pair retailer: unjudgeable.
        if len(g) < g["n_panel_listings_at_n"].max():
            excluded_cross_break += 1
            continue
        r = g.sort_values("retailer")
        out.append({
            "pair": pair, "pair_idx": g["pair_idx"].iloc[0],
            "cert_key": cert,
            "retailers": ",".join(r["retailer"]),
            "n_listings": len(g),
            "single_retailer": len(g) == 1,
            "carat": r["carat"].iloc[0], "shape": r["shape"].iloc[0],
            "color": r["color"].iloc[0], "clarity": r["clarity"].iloc[0],
            "cut": r["cut"].iloc[0], "cert_lab": r["cert_lab"].iloc[0],
            "price_usd": r["price_usd"].median(),
            "ppc": r["ppc"].median(),
            "dom_days": r["dom_days"].max(),
            "dom_censored": r["dom_censored"].any(),
            "provisional": (g["class"] == "provisional_clean").any(),
        })
    return pd.DataFrame(out), excluded_cross_break


def bucket_mask(f, pt, widened=False, vs_pool=False):
    colors = ["E", "F"] if widened else ["E"]
    clar = ["VS1", "VS2"] if vs_pool else ["VS1"]
    return ((f["shape"] == "round") & (f["color"].isin(colors))
            & (f["clarity"].isin(clar)) & (f["cut"] == "Excellent")
            & (f["cert_lab"] == "IGI")
            & (f["carat"] >= pt - CARAT_BAND) & (f["carat"] <= pt + CARAT_BAND))


def still_listed(df, breaks, capdates, per_ret):
    """Stones present at N and N+1 (same retailer, clean pairs): the 'ask'
    comparison set, priced at N, with tenure at N."""
    retailers = sorted(df["retailer"].unique())
    rows = []
    by_rc = {(r, c): g for (r, c), g in df.groupby(["retailer", "cycle"])}
    for i in range(len(CYCLES) - 1):
        cn, cn1 = CYCLES[i], CYCLES[i + 1]
        for r in retailers:
            if (r, i) in breaks:
                continue
            cur, nxt = by_rc.get((r, cn)), by_rc.get((r, cn1))
            if cur is None or nxt is None:
                continue
            keep = cur[cur["cert_key"].isin(set(nxt["cert_key"]))].copy()
            pr = per_ret[r]
            fs_idx = [first_seen_contiguous(pr.get(k, set()), i)
                      for k in keep["cert_key"]]
            keep["pair"], keep["pair_idx"] = f"{cn}->{cn1}", i
            keep["dom_days"] = [(capdates[(r, cn)]
                                 - capdates[(r, CYCLES[j])]).days
                                for j in fs_idx]
            keep["dom_censored"] = [j == 0 for j in fs_idx]
            rows.append(keep)
    return pd.concat(rows, ignore_index=True)


def fmt_pct(x):
    return f"{100 * x:+.1f}%" if pd.notna(x) else "n/a"


def main():
    df = load()
    capdates = capture_dates(df)
    counts, swings, breaks = coverage(df)

    print("=" * 72)
    print("COVERAGE GUARD (parquet cert-row counts; |swing| > 30% = BREAK)")
    print(counts.to_string())
    for i in range(len(CYCLES) - 1):
        line = []
        for r in counts.index:
            mark = " BREAK" if (r, i) in breaks else ""
            line.append(f"{r}: {fmt_pct(swings[(r, i)])}{mark}")
        print(f"  {CYCLES[i]}->{CYCLES[i+1]}: " + " | ".join(line))

    per_ret = presence_maps(df)
    ev = build_events(df, breaks, per_ret, capdates)
    sales, excluded_cross = infer_sales(ev)
    ask = still_listed(df, breaks, capdates, per_ret)

    OUT_EVENTS.parent.mkdir(parents=True, exist_ok=True)
    ev.to_csv(OUT_EVENTS, index=False, compression="gzip")
    sales.to_csv(OUT_SALES, index=False, compression="gzip")
    print(f"\nINTERNAL tables: {OUT_EVENTS.name} ({len(ev):,} events), "
          f"{OUT_SALES.name} ({len(sales):,} cert-level inferred sales; "
          f"{excluded_cross:,} certs excluded for break-retailer "
          f"cross-listing)")

    print("\n" + "=" * 72)
    print("DELIST EVENTS by pair / retailer / class (retailer-level)")
    print(ev.groupby(["pair", "retailer", "class"]).size()
            .unstack(fill_value=0).to_string())

    print("\nCross-listed-at-N share of delist events: "
          f"{ev['cross_listed_at_n'].mean():.1%}")

    print("\nREAPPEARANCE RATE (G2 basis: complete-window retailer events)")
    for pair, g in ev.groupby("pair"):
        re_n = (g["class"].isin(["relist_same", "transfer_other"])).sum()
        tot = len(g)
        comp = g["window_complete"].all()
        tag = "" if comp else "  [window INCOMPLETE: rate is a lower bound]"
        print(f"  {pair}: {re_n:,}/{tot:,} = {re_n / tot:.1%}"
              f" (relist {(g['class'] == 'relist_same').mean():.1%}, "
              f"transfer {(g['class'] == 'transfer_other').mean():.1%})"
              f"{tag}")

    print("\n" + "=" * 72)
    print("G1: cert-level inferred sales per pair per benchmark bucket")
    header = f"  {'pair':<28}" + "".join(f"{p:>10}ct" for p in BENCH_POINTS)
    for label, kw in [("primary (E VS1)", {}),
                      ("widened (E-F VS1)", {"widened": True}),
                      ("sensitivity (E VS1-VS2)", {"vs_pool": True})]:
        print(f"  -- {label}")
        print(header)
        for pair, g in sales.groupby("pair"):
            cells = [int(bucket_mask(g, p, **kw).sum()) for p in BENCH_POINTS]
            prov = " (provisional)" if g["provisional"].any() else ""
            print(f"  {pair + prov:<28}" + "".join(f"{c:>12}" for c in cells))

    print("\n" + "=" * 72)
    print("ASK-SOLD SPREAD, WITHIN RETAILER ($/ct medians at cycle N; sold ="
          " single-retailer inferred sales, ask = still listed at N+1;"
          f" cells need n>={MIN_CELL} both sides)")
    g3_signs = {}
    ss = sales[sales["single_retailer"]].copy()
    ss["retailer"] = ss["retailers"]
    for pair_idx in sorted(ev["pair_idx"].unique()):
        pair = CYCLES[pair_idx] + "->" + CYCLES[pair_idx + 1]
        e = ss[ss["pair_idx"] == pair_idx]
        a = ask[ask["pair_idx"] == pair_idx]
        print(f"  {pair}:")
        cell_spreads = []
        for r in sorted(e["retailer"].unique()):
            er, ar = e[e["retailer"] == r], a[a["retailer"] == r]
            for p in BENCH_POINTS:
                sm, am = er[bucket_mask(er, p)], ar[bucket_mask(ar, p)]
                if len(sm) >= MIN_CELL and len(am) >= MIN_CELL:
                    ms, ma = sm["ppc"].median(), am["ppc"].median()
                    spread = ms / ma - 1
                    cell_spreads.append(spread)
                    print(f"    {r} {p}ct: sold ${ms:,.0f}/ct (n={len(sm)}) "
                          f"vs ask ${ma:,.0f}/ct (n={len(am):,}) -> "
                          f"{fmt_pct(spread)}")
                else:
                    print(f"    {r} {p}ct: below cell floor "
                          f"(sold n={len(sm)}, ask n={len(am):,})")
        if cell_spreads:
            med = float(np.median(cell_spreads))
            provisional = pair_idx + 1 + REAPPEAR_CYCLES > len(CYCLES)
            g3_signs[pair] = (np.sign(med), provisional)
            print(f"    pair median cell spread: {fmt_pct(med)}"
                  + (" [PROVISIONAL: window incomplete]" if provisional
                     else ""))
        # tenure comparison, within the same qualifying population
        sd = e[~e["dom_censored"]]["dom_days"]
        ad = a[~a["dom_censored"]]["dom_days"]
        if len(e) and len(a):
            print(f"    tenure at N (uncensored): sold median "
                  f"{sd.median() if len(sd) else float('nan'):.0f}d "
                  f"(censored {e['dom_censored'].mean():.0%}) vs "
                  f"still-listed "
                  f"{ad.median() if len(ad) else float('nan'):.0f}d "
                  f"(censored {a['dom_censored'].mean():.0%})")

    print("\n" + "=" * 72)
    print("GATES")
    comp = sales[~sales["provisional"]]
    for pair, g in comp.groupby("pair"):
        prim = max(int(bucket_mask(g, p).sum()) for p in BENCH_POINTS)
        wide = max(int(bucket_mask(g, p, widened=True).sum())
                   for p in BENCH_POINTS)
        ok = prim >= G1_MIN_CLEAN or wide >= G1_MIN_CLEAN
        print(f"  G1 {pair}: best bucket primary={prim} widened={wide} -> "
              f"{'PASS' if ok else 'FAIL'}")
    comp_ev = ev[ev["window_complete"]]
    for pair, g in comp_ev.groupby("pair"):
        rate = (g["class"].isin(["relist_same", "transfer_other"])).mean()
        print(f"  G2 {pair}: reappearance {rate:.1%} -> "
              f"{'PASS' if rate < G2_MAX_REAPPEAR else 'FAIL'} "
              f"(bar: < {G2_MAX_REAPPEAR:.0%})")
    print(f"  G3 within-retailer median-spread signs by pair: "
          f"{ {k: int(v) for k, (v, _) in g3_signs.items()} }")
    complete_signs = [v for v, prov in g3_signs.values()
                      if not prov and v != 0]
    prov_signs = [v for v, prov in g3_signs.values() if prov and v != 0]
    if complete_signs:
        dom = max(set(complete_signs), key=complete_signs.count)
        print(f"  G3: {complete_signs.count(dom)} of {len(complete_signs)} "
              f"COMPLETE pairs share sign {int(dom)} (spec bar: 3 of 4; "
              f"the 4th pair is provisional"
              + (f", also sign {int(prov_signs[0])}" if prov_signs else "")
              + ")")


if __name__ == "__main__":
    sys.exit(main())
