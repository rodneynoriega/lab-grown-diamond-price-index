#!/usr/bin/env python3
"""
sync_uk_gempages_block.py
Regenerate the edition-specific regions of the UK GemPages custom-code block
(OUTBOX/uk-index-gempages-YYYY-MM-DD.html) from uk-index-data.json, using
the previous edition's block as the template. Everything not listed here
(styles, render logic, share/email code) is copied through untouched.

Regions regenerated (each located by an anchored pattern that must match
exactly once; the script refuses otherwise):
  static:  edition line, gap callout, price table (+ Market Median row),
           Key Findings block, Methodology block, footer line
  script:  DATA_URL ?v=N, FALLBACK object, METHODOLOGY array,
           Key Findings <li>s inside render()
  json-ld: name, description, datePublished/dateModified, temporalCoverage,
           measurementTechnique "Data collected" month, variableMeasured

Editorial copy comes from these uk-index-data.json fields (added 2026-08-29
so the block is reproducible from the JSON, like the US surfaces):
  key_findings: [html strings]     methodology: [{"title","body"}]
  jsonld_description: str          jsonld_name: str (optional)
Placeholders {month}, {listings}, {gap2ct} are substituted in copy.

All prices in the JSON are ex-VAT GBP per carat; display = round(ppc*1.2*wt),
identical to the block's fmtTotal(). Market Median = median of "ok" retailer
ppc medians, needs >= 2 (MIN_MARKET_RETAILERS) retailers, else a dash.

Usage:
  python3 page-assets/sync_uk_gempages_block.py --template OUTBOX/uk-index-gempages-2026-07-30.html \
      --json uk-index-data.json --data-url-v 7 --out OUTBOX/uk-index-gempages-2026-08-29.html
"""
import argparse
import json
import re
import statistics

BANDS = [("1ct", 1.0, "1.45-1.55ct"), ("1.5ct", 1.5, "1.45-1.55ct"), ("2ct", 2.0, "1.95-2.05ct")]
BAND_RANGE = {"1ct": "0.95-1.05ct", "1.5ct": "1.45-1.55ct", "2ct": "1.95-2.05ct"}
MIN_MARKET_RETAILERS = 2
MONTHS = ["January", "February", "March", "April", "May", "June", "July",
          "August", "September", "October", "November", "December"]
NUMWORD = ["zero", "one", "two", "three", "four", "five", "six", "seven", "eight"]


def total(ppc, wt):
    # JS Math.round semantics (half up)
    import math
    return math.floor(ppc * 1.2 * wt + 0.5)


def gbp(n):
    return "\u00a3" + f"{n:,}"


def median_of(vals):
    if not vals:
        return None
    s = sorted(vals)
    m = len(s) // 2
    return round((s[m - 1] + s[m]) / 2) if len(s) % 2 == 0 else s[m]


def js_str(s):
    s = (s.replace("\u2013", "-").replace("\u2014", "-").replace("\u2026", "...")
          .replace("\u2018", "'").replace("\u2019", "'").replace("\u00a0", " ")
          .replace("\u00a3", "\\xa3"))
    if any(ord(c) > 127 for c in s):
        raise SystemExit("non-ASCII in JS string: " + repr(sorted({c for c in s if ord(c) > 127})))
    s = s.replace("\\", "\\\\").replace("\\\\xa3", "\\xa3").replace('"', '\\"').replace("\n", " ")
    if " <" in s:
        raise SystemExit("space before '<' inside a JS string: " + s[:100])
    return '"' + s + '"'


def js_val(v):
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return json.dumps(v)
    return js_str(str(v))


def once(text, pattern, repl, label, flags=re.S):
    n = len(re.findall(pattern, text, flags))
    if n != 1:
        raise SystemExit(f"region '{label}': expected 1 match, found {n}")
    return re.sub(pattern, lambda m: repl, text, count=1, flags=flags)


def compute(data):
    cells = {}
    for key, wt, _ in BANDS:
        ok = [(r["name"], r["cells"][key]["median_price_per_carat"])
              for r in data["retailers"] if r["cells"].get(key, {}).get("status") == "ok"]
        med = median_of([p for _, p in ok]) if len(ok) >= MIN_MARKET_RETAILERS else None
        cells[key] = {"ok": ok, "market_ppc": med,
                      "market_total": total(med, wt) if med else None,
                      "min": min(ok, key=lambda x: x[1]) if ok else None,
                      "max": max(ok, key=lambda x: x[1]) if ok else None}
    gap_cell = next((k for k, _, _ in BANDS if len(cells[k]["ok"]) >= MIN_MARKET_RETAILERS), None)
    two = cells["2ct"]
    gap2 = (total(two["max"][1], 2) - total(two["min"][1], 2)) if two["min"] else None
    listings = next(h["total_listings"] for h in reversed(data["history"]) if h["month"] == data["month"])
    return cells, gap_cell, gap2, listings


def static_table(data, cells):
    rows = []
    for i, r in enumerate(data["retailers"]):
        bg = "#fafafa" if i % 2 == 0 else "#ffffff"
        tds = []
        for key, wt, _ in BANDS:
            c = r["cells"].get(key)
            if not c or c.get("status") != "ok":
                tip = c.get("note") if c and c.get("note") else ("Fewer than 30 stones, not published." if c and c.get("status") == "thin" else "No qualifying inventory in this cell.")
                tds.append(f'          <td style="text-align: right; padding: 10px 12px; background: {bg}; color: #aaa;" title="{tip}">-</td>')
            else:
                tip = f'n={c["stone_count"]}' + (f'; {c["note"]}' if c.get("note") else "")
                tds.append(f'          <td style="text-align: right; padding: 10px 12px; background: {bg}; font-variant-numeric: tabular-nums;" title="{tip}">{gbp(total(c["median_price_per_carat"], wt))}</td>')
        rows.append("        <tr>\n" + f'          <td style="padding: 10px 12px; font-weight: 700; white-space: nowrap; background: {bg}; color: #1a1a1a;">{r["name"]}</td>\n' + "\n".join(tds) + "\n        </tr>")
    med = []
    for key, wt, _ in BANDS:
        c = cells[key]
        if c["market_total"]:
            med.append(f'          <td style="text-align: right; padding: 10px 12px; background: #f0f0f0; color: #555; font-style: italic; font-variant-numeric: tabular-nums;">{gbp(c["market_total"])}</td>')
        else:
            n_ok = len(c["ok"])
            tip = (f"Only one retailer ({c['ok'][0][0]}) met the 30-listing minimum at {key} this edition -- not enough independent sources for a market figure."
                   if n_ok == 1 else f"Fewer than {MIN_MARKET_RETAILERS} retailers met the 30-listing minimum in this cell -- not enough independent sources for a market figure.")
            med.append(f'          <td style="text-align: right; padding: 10px 12px; background: #f0f0f0; color: #aaa;" title="{tip}">-</td>')
    rows.append('        <tr style="border-top: 2px solid #ccc;">\n          <td style="padding: 10px 12px; font-weight: 600; font-style: italic; background: #f0f0f0; color: #555;">Market Median</td>\n' + "\n".join(med) + "\n        </tr>")
    return "\n".join(rows)


def gap_callout(cells, gap_cell, gap2):
    if not gap_cell:
        return ""
    wt = dict((k, w) for k, w, _ in BANDS)[gap_cell]
    c = cells[gap_cell]
    lo, hi = total(c["min"][1], wt), total(c["max"][1], wt)
    s = (f"The same {gap_cell} G VS1 IGI round costs <strong>{gbp(lo)}</strong> at {c['min'][0]} and "
         f"<strong>{gbp(hi)}</strong> at {c['max'][0]}, <strong>{gbp(hi - lo)}</strong> apart for identical specs.")
    if gap_cell != "2ct" and gap2 is not None:
        s += f" At 2ct, that gap reaches <strong>{gbp(gap2)}</strong> on a single stone."
    return s


def fallback_block(data):
    L = ["  var FALLBACK = {", f"    month: {js_val(data['month'])},", f"    last_updated: {js_val(data['last_updated'])},",
         f"    benchmark_spec: {js_val(data['benchmark_spec'])},", '    currency: "GBP",',
         '    benchmark_cells: ["1ct", "1.5ct", "2ct"],',
         '    cell_ranges: {"1ct": "0.95-1.05ct", "1.5ct": "1.45-1.55ct", "2ct": "1.95-2.05ct"},',
         "    history: ["]
    hist = data["history"]
    for hi, h in enumerate(hist):
        L.append("      {")
        L.append(f"        month: {js_val(h['month'])}, total_listings: {js_val(h['total_listings'])},")
        L.append("        retailers: {")
        items = list(h["retailers"].items())
        for ri, (slug, cm) in enumerate(items):
            key = slug if re.fullmatch(r"[A-Za-z_]\w*", slug) else js_str(slug)
            L.append(f"          {key}: {{" + ", ".join(f"{js_str(k)}: {js_val(v)}" for k, v in cm.items()) + "}" + ("," if ri < len(items) - 1 else ""))
        L.append("        }")
        L.append("      }" + ("," if hi < len(hist) - 1 else ""))
    L += ["    ],", "    retailers: ["]
    rets = data["retailers"]
    for ri, r in enumerate(rets):
        L.append("      {")
        L.append(f"        name: {js_val(r['name'])}, slug: {js_val(r['slug'])},")
        L.append("        cells: {")
        ck = [k for k, _, _ in BANDS if k in r["cells"]]
        for ci, key in enumerate(ck):
            c = r["cells"][key]
            kv = [f"status:{js_val(c.get('status'))}"]
            if c.get("stone_count") is not None:
                kv.append(f"stone_count:{js_val(c['stone_count'])}")
            if c.get("median_price_per_carat") is not None:
                kv.append(f"median_price_per_carat:{js_val(c['median_price_per_carat'])}")
            if c.get("note"):
                kv.append(f"note:{js_val(c['note'])}")
            L.append(f"          {js_str(key)}: {{" + ", ".join(kv) + "}" + ("," if ci < len(ck) - 1 else ""))
        L.append("        }")
        L.append("      }" + ("," if ri < len(rets) - 1 else ""))
    L += ["    ]", "  };"]
    return "\n".join(L)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--template", required=True)
    ap.add_argument("--json", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--data-url-v", type=int, required=True)
    args = ap.parse_args()
    data = json.load(open(args.json))
    src = open(args.template, encoding="utf-8").read()
    cells, gap_cell, gap2, listings = compute(data)
    month = data["month"]
    mname, myear = month.split()
    ctx = {"month": month, "listings": f"{listings:,}", "gap2ct": gbp(gap2) if gap2 is not None else "n/a",
           "n_retailers_word": NUMWORD[len(data["retailers"])]}

    def sub(t):
        for k, v in ctx.items():
            t = t.replace("{" + k + "}", str(v))
        return t

    kf = [sub(b) for b in data["key_findings"]]
    meth = [(sub(p["title"]), sub(p["body"])) for p in data["methodology"]]

    # ---- static
    m = re.search(r'(<p id="lgd-published-date"[^>]*>\n\s*)([^\n]*?)( edition &nbsp;)', src)
    assert m, "edition line"
    src = src[:m.start(2)] + month + src[m.end(2):]
    m = re.search(r'(<div id="lgd-table-wrap"[^>]*>\n\n    <p style="font-size: 0.95rem; color: #333; margin: 0 0 20px; line-height: 1.6;">\n      )(.*?)(\n    </p>)', src, re.S)
    assert m, "gap callout region"
    src = src[:m.start(2)] + gap_callout(cells, gap_cell, gap2) + src[m.end(2):]
    m = re.search(r'(      <tbody>\n)(.*?)(\n      </tbody>)', src, re.S)
    assert m, "static table body"
    src = src[:m.start(2)] + static_table(data, cells) + src[m.end(2):]
    # key findings (static)
    kf_html = "\n".join(f'        <li style="margin: 0 0 9px;">{b}</li>' if i < len(kf) - 1 else f'        <li style="margin: 0;">{b}</li>' for i, b in enumerate(kf))
    m = re.search(r'(<strong style="display: block; margin-bottom: 10px; font-size: 1rem; color: #ffffff;">Key Findings: )([^<]*)(</strong>\n      <p style="margin: 0 0 14px; color: #c8c8c8; font-size: 0.9rem; line-height: 1.6;">)([^<]*)(</p>\n      <ul[^>]*>\n)(.*?)(\n      </ul>)', src, re.S)
    assert m, "static key findings"
    src = (src[:m.start(2)] + month + m.group(3) + f"{ctx['n_retailers_word'].capitalize()} retailers. {ctx['listings']} listings. G VS1 Round Excellent IGI."
           + m.group(5) + kf_html + src[m.end(6):])
    meth_html = "\n".join(f'      <p style="margin: 0 0 12px;"><strong>{t}.</strong> {b}</p>' for t, b in meth)
    m = re.search(r'(<div id="lgd-method-wrap".*?<div style="font-size: 0.9rem; color: #333; line-height: 1.7;">\n)(.*?)(\n    </div>\n  </div>)', src, re.S)
    assert m, "static methodology"
    src = src[:m.start(2)] + meth_html + src[m.end(2):]
    nxt = MONTHS[(MONTHS.index(mname) + 1) % 12] + " " + (str(int(myear) + 1) if mname == "December" else myear)
    src = once(src, r'Rings\.com UK Lab-Grown Diamond Price Index\. Data: [^.]*\. Next edition: [^.]*\. &copy;',
               f"Rings.com UK Lab-Grown Diamond Price Index. Data: {month}. Next edition: {nxt}. &copy;", "static footer")

    # ---- script
    src = once(src, r'uk-index-data\.json\?v=\d+"', f'uk-index-data.json?v={args.data_url_v}"', "DATA_URL")
    src = once(src, r"  var FALLBACK = \{.*?\n  \};\n", fallback_block(data) + "\n", "FALLBACK")
    meth_js = "  var METHODOLOGY = [\n" + ",\n".join("    " + js_str(f"<strong>{t}.</strong> {b}") for t, b in meth) + "\n  ];"
    src = once(src, r"  var METHODOLOGY = \[\n.*?\n  \];", meth_js, "METHODOLOGY")
    kf_js = "\n".join("      + " + js_str(f'<li style="{"margin:0 0 9px;" if i < len(kf) - 1 else "margin:0;"}">{b}</li>') for i, b in enumerate(kf))
    m = re.search(r'(      \+ "<p style=\\"margin:0 0 14px;color:#c8c8c8;font-size:0\.9rem;line-height:1\.6;\\">)([^\n]*)\n(      \+ "<ul[^\n]*\n)(.*?)(\n      \+ "</ul>")', src, re.S)
    assert m, "KF js region"
    head = f'{ctx["n_retailers_word"].capitalize()} retailers. " + data.history[data.history.length - 1].total_listings.toLocaleString("en-GB") + " listings. G VS1 Round Excellent IGI.</p>"'
    src = src[:m.start(2)] + head + "\n" + m.group(3) + kf_js + src[m.end(4):]

    # ---- json-ld
    ld_name = data.get("jsonld_name") or f"Rings UK Lab-Grown Diamond Price Index, {month}"
    src = once(src, r'"name": "Rings UK Lab-Grown Diamond Price Index, [^"]*"', f'"name": "{ld_name}"', "ld name")
    src = once(src, r'"description": "Monthly benchmark prices for lab-grown diamonds \(G VS1[^\n]*",', f'"description": {json.dumps(sub(data["jsonld_description"]), ensure_ascii=False)},', "ld description")
    src = once(src, r'"datePublished": "[^"]*",\n  "dateModified": "[^"]*",', f'"datePublished": "{data["last_updated"]}",\n  "dateModified": "{data["last_updated"]}",', "ld dates")
    src = once(src, r'"temporalCoverage": "\d{4}-\d{2}"', f'"temporalCoverage": "{myear}-{MONTHS.index(mname) + 1:02d}"', "ld temporal")
    src = once(src, r'Data collected [A-Z][a-z]+ \d{4}\."', f'Data collected {month}."', "ld collected")
    vm = []
    for key, wt, _ in BANDS:
        c = cells[key]
        if len(c["ok"]) >= MIN_MARKET_RETAILERS:
            vm.append(f'    {{ "@type": "PropertyValue", "name": "Median total stone price inc-VAT, {key} cell ({BAND_RANGE[key]})", "unitCode": "GBP", "minValue": {total(c["min"][1], wt)}, "maxValue": {total(c["max"][1], wt)} }}')
    src = once(src, r'"variableMeasured": \[\n.*?\n  \],', '"variableMeasured": [\n' + ",\n".join(vm) + "\n  ],", "ld variableMeasured")

    open(args.out, "w", encoding="utf-8").write(src)
    print(f"wrote {args.out}: {month}, listings {listings:,}, gap cell {gap_cell}, market medians "
          + ", ".join(f"{k}={gbp(cells[k]['market_total']) if cells[k]['market_total'] else 'n/a'}" for k, _, _ in BANDS))


if __name__ == "__main__":
    main()
