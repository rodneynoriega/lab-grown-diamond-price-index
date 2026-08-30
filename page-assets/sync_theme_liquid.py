#!/usr/bin/env python3
"""
sync_theme_liquid.py
Regenerate the hand-edited, edition-specific regions of
page-assets/theme-liquid-lgd-body.liquid from index-data.json, so the
interactive JS surface can never drift from the JSON (the "two surfaces"
caveat in MEMORY/project_price_index_page.md). Regions touched:

  1. DATA_URL cache-buster (?v=N)                     -> --data-url-v N
  2. FALLBACK object (month, last_updated, next_edition_note, benchmark_*,
     market_median_suppressed, history[], retailers[])
  3. METHODOLOGY array (<strong>Title.</strong> body, {ctx} substituted,
     stat_notes auto-built exactly as generate_us_gempages_block.py does)
  4. Key Findings block (the "N retailers. X listings." line + <li>s)
  5. shareText (1ct range + month)
  6. nextEdNote hardcoded fallback string
  7. Table footnote paragraph, when the JSON carries `table_footnote`

Everything else in the file (render logic, styles, share buttons, email
capture) is left byte-for-byte untouched. Every region is located by an
anchored regex that must match exactly once; the script refuses otherwise.

JS string rules enforced (see MEMORY/feedback_gempages_shopify.md):
no line breaks in strings, `"` escaped, ASCII only, never a space before
`<` inside a string (Flex theme splits there and kills the script).

Usage:
    python3 page-assets/sync_theme_liquid.py --data-url-v 18 \
        [--json index-data.json] [--liquid page-assets/theme-liquid-lgd-body.liquid] [--out ...]
"""
import argparse
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from generate_us_gempages_block import (  # noqa: E402
    BANDS, band_ranges, build_stat_notes, num_to_word, retailer_list,
    total_listings_for_month, apply_ctx, collected_retailers, panel_retailers,
)

ROOT = os.path.dirname(HERE)


def js_str(s):
    """Escape for a double-quoted JS string on ONE line, ASCII only."""
    s = (s.replace("\u2013", "-").replace("\u2014", "-").replace("\u2026", "...")
          .replace("\u2018", "'").replace("\u2019", "'")
          .replace("\u201c", '\\"').replace("\u201d", '\\"')
          .replace("\u00a0", " ").replace("\u00d7", "x"))
    if any(ord(c) > 127 for c in s):
        bad = sorted({c for c in s if ord(c) > 127})
        raise SystemExit(f"non-ASCII character(s) in JS string: {bad!r}")
    s = s.replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")
    if " <" in s:
        raise SystemExit("space before '<' inside a JS string (Flex split bug): " + s[:120])
    return '"' + s + '"'


def js_val(v):
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return json.dumps(v)
    return js_str(str(v))


def fallback_block(data):
    L = []
    L.append("  var FALLBACK = {")
    L.append(f"    month: {js_val(data['month'])},")
    L.append(f"    last_updated: {js_val(data['last_updated'])},")
    if data.get("next_edition_note"):
        L.append(f"    next_edition_note: {js_val(data['next_edition_note'])},")
    if data.get("correction_note"):
        L.append(f"    correction_note: {js_val(data['correction_note'])},")
    L.append(f"    benchmark_spec: {js_val(data.get('benchmark_spec', 'E VS1 Excellent IGI'))},")
    cells = data.get("benchmark_cells", ["1ct", "1.5ct", "2ct"])
    L.append("    benchmark_cells: [" + ", ".join(js_val(c) for c in cells) + "],")
    L.append("    market_median_suppressed: [" + ", ".join(js_val(c) for c in data.get("market_median_suppressed", [])) + "],")
    L.append("    history: [")
    hist = data.get("history", [])
    for hi, h in enumerate(hist):
        L.append("      {")
        L.append(f"        month: {js_val(h['month'])},")
        L.append(f"        total_listings: {js_val(h.get('total_listings'))},")
        L.append("        retailers: {")
        rets = list(h["retailers"].items())
        for ri, (slug, cellmap) in enumerate(rets):
            parts = []
            for k, v in cellmap.items():
                key = k if re.fullmatch(r"[A-Za-z_]\w*", k) else js_str(k)
                parts.append(f"{key}: {js_val(v)}")
            comma = "," if ri < len(rets) - 1 else ""
            L.append(f"          {slug}: {{" + ", ".join(parts) + "}" + comma)
        L.append("        }")
        L.append("      }" + ("," if hi < len(hist) - 1 else ""))
    L.append("    ],")
    L.append("    retailers: [")
    rets = data["retailers"]
    for ri, r in enumerate(rets):
        head = f"name: {js_val(r['name'])}, slug: {js_val(r['slug'])}"
        if r.get("non_igi"):
            head += ", non_igi: true"
        if r.get("approximate_windows"):
            head += ", approximate_windows: true"
        if r.get("reference_row_of"):
            head += f", reference_row_of: {js_val(r['reference_row_of'])}"
        if r.get("row_label"):
            head += f", row_label: {js_val(r['row_label'])}"
        cellparts = []
        for key, _, _ in BANDS:
            c = r["cells"].get(key)
            if c is None:
                continue
            kv = [f"status:{js_val(c.get('status'))}"]
            if c.get("stone_count") is not None:
                kv.append(f"stone_count:{js_val(c['stone_count'])}")
            if c.get("median_price_per_carat") is not None:
                kv.append(f"median_price_per_carat:{js_val(c['median_price_per_carat'])}")
            if c.get("median_total_price") is not None:
                kv.append(f"median_total_price:{js_val(c['median_total_price'])}")
            if c.get("note"):
                kv.append(f"note:{js_val(c['note'])}")
            if c.get("partial_capture"):
                kv.append("partial_capture:true")
            if c.get("actual_range"):
                kv.append(f"actual_range:{js_val(c['actual_range'])}")
            cellparts.append(f"{js_str(key)}: {{" + ", ".join(kv) + "}")
        comma = "," if ri < len(rets) - 1 else ""
        L.append(f"      {{ {head}, cells: {{" + ", ".join(cellparts) + "} }" + comma)
    L.append("    ]")
    L.append("  };")
    return "\n".join(L)


def build_ctx(data):
    retailers = data["retailers"]
    ranges = band_ranges(data)
    listings = total_listings_for_month(data)
    n = len(panel_retailers(retailers))
    return {
        "month": data.get("month", ""),
        "listings": f"{listings:,}" if listings else "",
        "n_retailers": n,
        "n_retailers_word": num_to_word(n),
        "collection_date": data.get("collection_date", ""),
        "retailer_panel_list": retailer_list(panel_retailers(retailers)),
        "stat_notes": build_stat_notes(retailers),
        "min1ct": f"{ranges['1ct']['min']:,}" if "1ct" in ranges else "",
        "max1ct": f"{ranges['1ct']['max']:,}" if "1ct" in ranges else "",
        "min2ct": f"{ranges['2ct']['min']:,}" if "2ct" in ranges else "",
        "max2ct": f"{ranges['2ct']['max']:,}" if "2ct" in ranges else "",
    }, ranges, listings


def methodology_block(data, ctx):
    items = [f"<strong>{apply_ctx(p['title'], ctx)}.</strong> {apply_ctx(p['body'], ctx)}"
             for p in data["methodology"]]
    inner = ",\n".join("    " + js_str(t) for t in items)
    return "  var METHODOLOGY = [\n" + inner + "\n  ];"


def key_findings_block(data, ctx):
    n_panel = len(panel_retailers(data["retailers"]))
    n_word_cap = num_to_word(n_panel).capitalize()
    n_collected = len(collected_retailers(data["retailers"]))
    panel_line = (f"{n_word_cap} retailers." if n_collected == n_panel
                  else f"{n_word_cap} retailers tracked, {num_to_word(n_collected)} collected.")
    lines = [
        '      + "<p style=\\"margin:0 0 14px;color:#c8c8c8;font-size:0.9rem;line-height:1.6;\\">'
        + f'{panel_line} {ctx["listings"]} listings. E VS1 Round Excellent IGI.</p>"',
        '      + "<ul style=\\"margin:0;padding:0 0 0 18px;color:#ffffff;line-height:1.65;\\">"',
    ]
    bullets = [apply_ctx(b, ctx) for b in data["key_findings"]]
    for i, b in enumerate(bullets):
        style = "margin:0 0 9px;" if i < len(bullets) - 1 else "margin:0;"
        inner = js_str(f'<li style="{style}">{b}</li>')
        lines.append("      + " + inner)
    return "\n".join(lines)


def replace_once(text, pattern, repl, label, flags=re.S):
    n = len(re.findall(pattern, text, flags))
    if n != 1:
        raise SystemExit(f"region '{label}': expected exactly 1 match, found {n}")
    return re.sub(pattern, lambda m: repl, text, count=1, flags=flags)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", default=os.path.join(ROOT, "index-data.json"))
    ap.add_argument("--liquid", default=os.path.join(HERE, "theme-liquid-lgd-body.liquid"))
    ap.add_argument("--out", default=None, help="default: overwrite --liquid")
    ap.add_argument("--data-url-v", type=int, required=True)
    args = ap.parse_args()

    data = json.load(open(args.json))
    src = open(args.liquid, encoding="utf-8").read()
    ctx, ranges, listings = build_ctx(data)

    # 1. DATA_URL ?v=N
    src = replace_once(src, r'(index-data\.json\?v=)\d+"', f"\\g<1>{args.data_url_v}\"", "DATA_URL")
    src = src.replace("\\g<1>", "index-data.json?v=")  # (lambda repl is literal; fix token)

    # 2. FALLBACK
    src = replace_once(src, r"  var FALLBACK = \{.*?\n  \};\n", fallback_block(data) + "\n", "FALLBACK")

    # 3. METHODOLOGY
    src = replace_once(src, r"  var METHODOLOGY = \[\n.*?\n  \];", methodology_block(data, ctx), "METHODOLOGY")

    # 4. Key Findings (from the retailers/listings <p> through the last <li>)
    src = replace_once(
        src,
        r'      \+ "<p style=\\"margin:0 0 14px;color:#c8c8c8;font-size:0\.9rem;line-height:1\.6;\\">.*?</li>"\n(?=      \+ "</ul></div>";)',
        key_findings_block(data, ctx) + "\n", "KEY_FINDINGS")

    # 5. shareText
    share = (f"Lab-grown diamond prices vary across major U.S. retailers: published 1ct E VS1 IGI "
             f"medians run from ${ranges['1ct']['min']:,} to ${ranges['1ct']['max']:,} by retailer. "
             f"Rings.com Lab-Grown Diamond Price Index, {data['month']}. ")
    src = replace_once(src, r'var shareText = encodeURIComponent\("[^"]*" \+ PAGE_URL\);',
                       f'var shareText = encodeURIComponent({js_str(share)} + PAGE_URL);', "shareText")

    # 6. nextEdNote fallback
    if data.get("next_edition_note"):
        src = replace_once(src, r'var nextEdNote = data\.next_edition_note \|\| "[^"]*";',
                           f'var nextEdNote = data.next_edition_note || {js_str(data["next_edition_note"])};',
                           "nextEdNote")

    # 7. table footnote (optional, JSON-driven)
    if data.get("table_footnote"):
        fn = js_str(f'<p style="font-size:0.8rem;color:#666;margin:8px 0 0;line-height:1.4;">{data["table_footnote"]}</p>')
        src = replace_once(src, r'      \+ "<p style=\\"font-size:0\.8rem;color:#666;margin:8px 0 0;line-height:1\.4;\\">Total stone price\..*?</p>";',
                           "      + " + fn + ";", "table_footnote")

    # Whole-script guard (not just the regions synced here): no double-quoted
    # JS string literal anywhere may contain a space before "<" (Flex theme
    # split bug), and the script must be ASCII. Hand edits get caught too.
    m = re.search(r"<script>\n(.*?)\n</script>", src, re.S)
    script = m.group(1) if m else src
    bad = [ln.strip()[:120] for ln in script.splitlines()
           for lit in re.findall(r'"(?:[^"\\]|\\.)*"|\'(?:[^\'\\]|\\.)*\'', ln) if " <" in lit]
    if bad:
        raise SystemExit("space before '<' inside a JS string literal:\n  " + "\n  ".join(bad))
    non_ascii = sorted({c for c in script if ord(c) > 127})
    if non_ascii:
        raise SystemExit(f"non-ASCII in theme script: {non_ascii!r}")
    out = args.out or args.liquid
    open(out, "w", encoding="utf-8").write(src)
    # Sanity: the script must stay ASCII-clean in the regions we touched.
    print(f"wrote {out}  (DATA_URL v={args.data_url_v}, month={data['month']}, "
          f"listings={listings:,}, 1ct range ${ranges['1ct']['min']:,}-${ranges['1ct']['max']:,})")


if __name__ == "__main__":
    main()
