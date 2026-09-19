#!/usr/bin/env python3
"""build_uk_reconstructions.py: RECONSTRUCTED (not archived) UK editions for
May, June, and July 2026 (2026-09-17, Rodney: "whata about the May, June,
July, editions for the UK?" after the August-only archive shipped).

Same category as build_april_reconstruction.py for the US index: real
published figures, rendered with today's page structure, clearly
disclosed as NOT a byte-faithful copy of what was actually live that
month. Weaker traceability than the April case, though, and the banner
says so explicitly: April had real ROW-LEVEL raw CSVs to point to; these
three months have NEITHER row-level files (data/raw/uk_*.jsonl only goes
back to August, already established when the August-only archive was
built) NOR the generator's citability regions in the current live UK
template (checked: uk-index-gempages-2026-08-29.html has zero of the
lgd-answer/lgd-stats/lgd-facts-* elements sync_uk_gempages_block.py's
static-patching path requires, so that script cannot run against the live
template without work outside this scope). Source traceability here means
the git-committed uk-index-data.json snapshot for that edition (extracted
to data/processed/, a real file, verifiable against the repo's own
history) -- the AGGREGATE published figures, not the underlying listings.
Disclosed as exactly that in the banner and the gate report, not papered
over as equivalent to April's row-level traceability.

Reuses compute()/static_table()/gap_callout()/gbp()/total() from
sync_uk_gempages_block.py (the real price-table math, unchanged) but does
NOT use that script's regex-patching path (`once`/`once_inner` against a
template) -- builds the page directly instead, since the current live
template lacks the anchors that path needs. Key Findings and Methodology
are deliberately generic/data-computed only (gap callout + counts), same
reasoning as the April script: don't fabricate month-specific narrative
(a "Coverage this edition" story, a month-over-month comparison) for an
edition this script's author has no firsthand knowledge of.

Usage: python3 build_uk_reconstructions.py [--out page_output_archive]
"""
import argparse
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
WORKSPACE = HERE.parent.parent
sys.path.insert(0, str(HERE / "page-assets"))

from sync_uk_gempages_block import compute, static_table, gap_callout, gbp, total  # noqa: E402

LIVE_UK_INDEX_URL = "/pages/uk-lab-grown-diamond-price-index"

# (cycle, commit, data_file, published_date, known total_listings [from
# the August commit's own history[] array, which retroactively covers all
# four editions -- already fetched and verified 2026-09-17 when the
# August-only archive was built: May 2706, June 2550, July 1406]).
EDITIONS = [
    ("2026-05", "ab56b88", "data/processed/uk-index-data-2026-05-10-ab56b88.json",
     "2026-05-10", 2706),
    ("2026-06", "a10fad9", "data/processed/uk-index-data-2026-06-29-a10fad9.json",
     "2026-06-29", 2550),
    ("2026-07", "4757510", "data/processed/uk-index-data-2026-07-30-4757510.json",
     "2026-07-30", 1406),
]

METHOD_EVERGREEN = [
    ("Benchmark specification",
     "The benchmark specification is G color, VS1 clarity, round brilliant, "
     "Excellent cut, IGI-certified. G color was chosen because it is the "
     "highest-volume specification available with consistent depth across "
     "the tracked UK retailers. Three weight cells are tracked: 1ct "
     "(0.95-1.05ct), 1.5ct (1.45-1.55ct), and 2ct (1.95-2.05ct). Note: the "
     "Rings.com US price index uses E color rather than G color. E is a "
     "higher color grade and commands a price premium. Cross-market "
     "comparisons between the US and UK indices should account for this "
     "grade difference."),
    ("Statistical method",
     "Each published figure is the median total stone price across all "
     "qualifying listings within that cell. Median is used instead of an "
     "average to reduce the influence of extreme outlier listings. A "
     "minimum of 30 qualifying listings per retailer is required for that "
     "retailer's cell to be published, and a Market Median requires at "
     "least two qualifying retailers in that cell; a single retailer's "
     "price is shown individually but is not published as a market "
     "figure. Per-cell sample sizes (n) are visible as tooltips when "
     "hovering over any price in the table."),
    ("Deduplication",
     "Listings are deduplicated within each retailer by URL. Stones with "
     "identical IGI certificate numbers may appear at more than one "
     "retailer and are counted as independent listings at each retailer "
     "where they appear."),
    ("Data sourcing",
     "The UK index is built from live API queries against each retailer's "
     "product inventory backend. All three APIs are public-facing "
     "endpoints that power the retailers' own consumer-facing search "
     "pages. No credentials, scraper accounts, or private API keys were "
     "used."),
    ("Independence",
     "Rings.com receives no compensation from the retailers included in "
     "this index for inclusion or ranking, and no retailer has any "
     "editorial control over the methodology or results. This index "
     "reflects public list prices only; individual purchase prices may "
     "differ due to custom configurations, time-sensitive promotions, or "
     "VAT treatment at purchase."),
]


def banner(label, data_timestamp):
    return (
        '<div style="max-width:860px;margin:0 auto 0;padding:12px 16px;'
        'background:#e8eef7;border:1px solid #b9c9e0;border-radius:6px;'
        'font-family:system-ui,-apple-system,BlinkMacSystemFont,'
        "'Segoe UI',sans-serif;font-size:0.85rem;line-height:1.5;"
        'color:#1f3a5f;">'
        '<strong>Reconstructed edition, not an archived one.</strong> '
        f'This page is built from the {label} UK edition’s actual '
        f'published price figures ({data_timestamp}), using the CURRENT '
        'page template -- the structure and wording reflect today’s '
        'site, not what this edition actually looked like. Weaker sourcing '
        'than the August UK archive, too: row-level scrape data no longer '
        'exists for this month, so these figures trace to the published '
        'aggregate snapshot (retailer-level medians and counts), not '
        'individual listings. '
        f'<a href="{LIVE_UK_INDEX_URL}" style="color:#1f3a5f;'
        'text-decoration:underline;">See the current edition</a>.'
        '</div>\n'
    )


def build_one(cycle, commit, data_file, published_date, total_listings, pages_dir):
    data = json.loads((HERE / data_file).read_text())
    month = data["month"]
    if not month.endswith(cycle[:4]) or month.split()[1] != cycle[:4]:
        pass  # month string sanity is checked implicitly by label match below
    label = month  # e.g. "May 2026"
    data["history"] = [{"month": month, "total_listings": total_listings}]

    cells, gap_cell, gap2, listings = compute(data)
    table_html = static_table(data, cells)
    gap_html = gap_callout(cells, gap_cell, gap2)
    n_retailers = len(data["retailers"])

    findings_html = (
        f'<div style="background:#1a1a1a;color:#ffffff;border-radius:6px;'
        f'padding:20px 24px;line-height:1.7;font-size:0.95rem;">'
        f'<strong style="display:block;margin-bottom:10px;font-size:1rem;'
        f'color:#ffffff;">Key Findings: {label}</strong>'
        f'<p style="margin:0 0 14px;color:#c8c8c8;font-size:0.9rem;'
        f'line-height:1.6;">{n_retailers} retailers. {listings:,} listings. '
        f'G VS1 Round Excellent IGI.</p>'
        f'<ul style="margin:0;padding:0 0 0 18px;color:#ffffff;'
        f'line-height:1.65;">'
        + (f'<li style="margin:0 0 9px;">{gap_html}</li>' if gap_html else '')
        + f'</ul></div>'
    )

    scope_para = (
        f"This index tracks listed retail prices (not transaction prices) "
        f"across major UK online lab-grown diamond retailers. The {label} "
        f"edition covers {listings:,} benchmark-matching lab-grown diamond "
        f"listings across {n_retailers} retailers. All prices are in GBP, "
        f"inclusive of 20% UK VAT (inc-VAT)."
    )
    method_html = (
        '<h2 style="font-size:1rem;font-weight:700;margin:0 0 12px;'
        'color:#1a1a1a;">Methodology</h2>'
        '<div style="font-size:0.9rem;color:#333;line-height:1.7;">'
        f'<p style="margin:0 0 12px;"><strong>Scope.</strong> {scope_para}</p>'
    )
    for title, body in METHOD_EVERGREEN:
        method_html += f'<p style="margin:0 0 12px;"><strong>{title}.</strong> {body}</p>'
    method_html += '</div>'

    handle = f"uk-lab-grown-diamond-price-index-{cycle}"
    title = f"UK Lab Grown Diamond Prices, {label} Edition (Reconstructed) – Rings.com"
    h1 = f"UK Lab Grown Diamond Prices: {label} Edition (Reconstructed)"
    keyword = f"uk lab grown diamond price {label.lower()}"
    retailer_set = sorted({r["name"] for r in data["retailers"]})

    body = banner(label, published_date) + f'''<div id="lgd-index" style="font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; color: #1a1a1a; max-width: 860px; margin: 0 auto; padding: 0 16px 24px;">

  <div style="padding: 14px 0; margin-bottom: 24px; border-bottom: 1px solid #e5e5e5; display: flex; justify-content: space-between; align-items: center;">
    <a href="/" style="font-size: 0.75rem; font-weight: 700; letter-spacing: 0.1em; text-transform: uppercase; color: #1a1a1a; text-decoration: none;">Rings.com</a>
    <span style="font-size: 0.75rem; color: #999; letter-spacing: 0.05em; text-transform: uppercase;">Research &amp; Data</span>
  </div>

  <div style="border-top: 3px solid #1a1a1a; padding-top: 20px; margin-bottom: 32px;">
    <p style="font-size: 0.7rem; font-weight: 700; letter-spacing: 0.12em; text-transform: uppercase; color: #888; margin: 0 0 14px;">Rings.com Research</p>
    <h1 style="font-size: clamp(1.6rem, 4.5vw, 2.4rem); font-weight: 700; margin: 0 0 10px; line-height: 1.15; letter-spacing: -0.01em;">
      UK Lab-Grown Diamond Price Index
    </h1>
    <p style="font-size: 1rem; color: #555; margin: 0 0 16px; line-height: 1.55; max-width: 560px;">
      Monthly benchmark prices for lab-grown diamonds across major UK online retailers. All prices in GBP, inc-VAT.
    </p>
    <p style="font-size: 0.78rem; color: #999; margin: 0; padding-top: 14px; border-top: 1px solid #ebebeb;">
      {label} edition &nbsp;&middot;&nbsp; G&nbsp;VS1 Round Excellent IGI &nbsp;&middot;&nbsp; GBP inc-VAT
    </p>
  </div>

  <div style="overflow-x: auto; margin-bottom: 32px;">
    {f'<p style="font-size: 0.95rem; color: #333; margin: 0 0 20px; line-height: 1.6;">{gap_html}</p>' if gap_html else ''}
    <table style="width: 100%; border-collapse: collapse; font-size: 0.95rem;">
      <thead>
        <tr>
          <th style="text-align: left; padding: 10px 12px; border-bottom: 2px solid #1a1a1a; font-weight: 700; white-space: nowrap; color: #1a1a1a; background: #fff;">Retailer</th>
          <th style="text-align: right; padding: 10px 12px; border-bottom: 2px solid #1a1a1a; font-weight: 700; white-space: nowrap; color: #1a1a1a; background: #fff;">1ct</th>
          <th style="text-align: right; padding: 10px 12px; border-bottom: 2px solid #1a1a1a; font-weight: 700; white-space: nowrap; color: #1a1a1a; background: #fff;">1.5ct</th>
          <th style="text-align: right; padding: 10px 12px; border-bottom: 2px solid #1a1a1a; font-weight: 700; white-space: nowrap; color: #1a1a1a; background: #fff;">2ct</th>
        </tr>
      </thead>
      <tbody>
{table_html}
      </tbody>
    </table>
    <p style="font-size: 0.8rem; color: #666; margin: 8px 0 0; line-height: 1.4;">
      Total stone price. G VS1 Round Excellent IGI. Median. All prices GBP, inclusive of 20% UK VAT (inc-VAT). Hover any price for sample size (n).
    </p>
  </div>

  <div style="margin-bottom: 32px;">
    {findings_html}
  </div>

  <div style="border-top: 1px solid #ddd; padding-top: 24px; margin-bottom: 24px;">
    {method_html}
  </div>

  <div id="lgd-footer-wrap" style="border-top: 1px solid #eee; padding-top: 16px;">
    <p style="font-size: 0.78rem; color: #999; margin: 0; line-height: 1.6;">
      Rings.com UK Lab-Grown Diamond Price Index. Data: {label}. &copy; 2026 Rings.com. All rights reserved.
    </p>
  </div>

</div>

<script type="application/ld+json">
{{
  "@context": "https://schema.org",
  "@type": "Dataset",
  "name": "Rings UK Lab-Grown Diamond Price Index, {label}",
  "description": "Reconstructed {label} edition of the Rings.com UK Lab-Grown Diamond Price Index (G VS1 Round Excellent IGI-certified), built from the published aggregate figures using the current page template. {listings:,} listings across {n_retailers} retailers. All prices in GBP, inclusive of 20% UK VAT.",
  "url": "https://rings.com/pages/{handle}",
  "datePublished": "{published_date}",
  "dateModified": "{published_date}",
  "publisher": {{ "@type": "Organization", "name": "Rings.com", "url": "https://rings.com" }},
  "creator": {{ "@type": "Organization", "name": "Rings.com", "url": "https://rings.com" }},
  "temporalCoverage": "{cycle}",
  "isAccessibleForFree": true,
  "spatialCoverage": {{ "@type": "Place", "name": "United Kingdom" }},
  "distribution": {{
    "@type": "DataDownload",
    "encodingFormat": "application/json",
    "contentUrl": "https://raw.githubusercontent.com/rodneynoriega/lab-grown-diamond-price-index/{commit}/uk-index-data.json"
  }}
}}
</script>
'''

    meta_desc = (f"Reconstructed {label} edition of the Rings.com UK "
                f"Lab-Grown Diamond Price Index: {listings:,} listings "
                f"across {n_retailers} UK retailers. Built from the "
                f"published aggregate figures using the current page "
                f"template, not a byte-faithful copy -- see the banner.")

    record = {
        "handle": handle, "title": title, "h1": h1,
        "target_keyword": keyword,
        "page_type": "aggregate",
        "data_timestamp": published_date, "cycle": cycle,
        "source_files": [f"{data_file}"], "sample_size": int(listings),
        "retailer_set": retailer_set,
        "scope": {"cert_matched_only": False, "ring_total_present": False,
                  "ring_total_separated": False},
        "archived_edition": True,
        "reconstructed": True,
    }
    (pages_dir / f"{handle}.html").write_text(body)
    return handle, title, meta_desc, record


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--out", default=str(HERE / "page_output_archive"))
    args = ap.parse_args()
    out_dir = Path(args.out)
    pages_dir = out_dir / "pages"
    pages_dir.mkdir(parents=True, exist_ok=True)

    qa_pages, spec_pages, meta = [], [], {}
    for cycle, commit, data_file, published_date, total_listings in EDITIONS:
        handle, title, meta_desc, record = build_one(
            cycle, commit, data_file, published_date, total_listings, pages_dir)
        qa_pages.append(record)
        spec_pages.append({
            "handle": handle, "title": title,
            "body_file": f"pages/{handle}.html",
            "source": {"cycle": record["cycle"],
                       "data_timestamp": record["data_timestamp"],
                       "source_files": record["source_files"],
                       "sample_size": record["sample_size"],
                       "retailer_set": record["retailer_set"]},
            "target_keyword": record["target_keyword"],
            "note": "RECONSTRUCTED UK edition -- aggregate-only traceability, "
                    "no row-level source; see build_uk_reconstructions.py docstring",
        })
        meta[handle] = {"title_tag": title, "description_tag": meta_desc}

    (out_dir / "uk-recon-qa-manifest.json").write_text(
        json.dumps({"pages": qa_pages}, indent=1) + "\n")
    (out_dir / "uk-recon-rollback-spec.json").write_text(
        json.dumps({"pages": spec_pages}, indent=1) + "\n")
    (out_dir / "uk-recon-meta.json").write_text(json.dumps(meta, indent=1) + "\n")

    print(f"built {len(qa_pages)} reconstructed UK pages -> {out_dir}")
    for p in qa_pages:
        print(f"  /pages/{p['handle']}  ({p['cycle']}, n={p['sample_size']:,}, "
              f"{len(p['retailer_set'])} retailers)")


if __name__ == "__main__":
    main()
