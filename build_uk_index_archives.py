#!/usr/bin/env python3
"""build_uk_index_archives.py: frozen per-edition archive page(s) for the UK
Lab-Grown Diamond Price Index (2026-09-17, same Beyond Carat citation
request as the US archives; Rodney: "also what about the UK? please do the
same").

Only the August 2026 edition can be honestly archived right now: raw UK
scrape files (data/raw/uk_*.jsonl) only exist on disk back to August --
May/June/July's were never retained (checked data/raw/, data/snapshots/,
and git history for data/raw/uk_*: nothing). The per-cycle SUMMARY figures
for those months survive inside uk-index-data.json's own history[] array,
but the gate's source traceability check (files must exist on disk) has
nothing to point at for them, and fabricating a source_files list would
defeat the entire point of a verifiable archive. This is a real data-
retention gap, not a policy choice; extend EDITIONS below if the raw files
are ever recovered.

Unlike the US block (whose interactive history-table JS lives OUTSIDE the
page body, in theme.liquid, gated to page.handle == the live index's own
handle), the UK block embeds its interactive <script> directly in the page
body, ungated, always fetching DATA_URL's CURRENT "main"-branch JSON and
overwriting the static content client-side. Left in place, an "archived"
UK page would render correctly for an instant and then JS would silently
replace it with today's live numbers -- the opposite of frozen. This
script strips that script block entirely (verified boundaries: opens at
the literal line "<script>", closes at "</script>" for the render/fetch
IIFE; the SEPARATE application/ld+json script immediately after is kept
and repinned like the US archives). The static pre-rendered table/callout/
methodology/footer content already has everything a reader needs -- that
is what it exists for (JS-off / crawler fallback).

Usage: python3 build_uk_index_archives.py [--out page_output_archive]
"""
import argparse
import json
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
WORKSPACE = HERE.parent.parent  # .../Lab-Grown Diamond Price Index/

GITHUB_REPO = "rodneynoriega/lab-grown-diamond-price-index"
LIVE_UK_INDEX_URL = "/pages/uk-lab-grown-diamond-price-index"
OLD_CONTENT_URL = (f"https://raw.githubusercontent.com/{GITHUB_REPO}/"
                   f"main/uk-index-data.json")

CYCLE_LABELS = {"2026-08": "August 2026"}

# (cycle, commit, html_file, source_files): commit verified pushed to
# origin/main; last_updated cross-checked to agree with the html_file's
# own edition date before pairing (both 2026-08-30 -- the file keeps its
# original "08-29" name despite the 08-30 Diamond Labs re-collection, same
# divergence the US archives already handle).
EDITIONS = [
    ("2026-08", "c430a1f", "OUTBOX/uk-index-gempages-2026-08-29.html",
     ["data/raw/uk_diamond_labs_2026-08-30.jsonl",
      "data/raw/uk_77_diamonds_2026-08-29.jsonl",
      "data/raw/uk_quality_diamonds_2026-08-29.jsonl"]),
]

SCRIPT_OPEN = "<script>\n"
SCRIPT_CLOSE = "</script>\n"


def git_show(commit, path):
    return subprocess.run(["git", "show", f"{commit}:{path}"], cwd=HERE,
                          capture_output=True, check=True, text=True).stdout


def strip_interactive_script(html):
    """Remove the single embedded <script>...</script> IIFE (render/fetch/
    email-capture), keep everything else including the later
    application/ld+json script. Refuses on anything but exactly one match,
    so a future template change that adds a second plain <script> tag
    fails loudly instead of silently stripping the wrong block or leaving
    the live-data fetch in place."""
    start = html.find(SCRIPT_OPEN)
    if start == -1 or html.count(SCRIPT_OPEN) != 1:
        raise SystemExit(f"expected exactly one bare '<script>' line, "
                         f"found {html.count(SCRIPT_OPEN)}")
    end = html.find(SCRIPT_CLOSE, start)
    if end == -1:
        raise SystemExit("no matching </script> found after <script>")
    end += len(SCRIPT_CLOSE)
    return html[:start] + html[end:]


def pin_content_url(html, commit):
    n = html.count(OLD_CONTENT_URL)
    if n != 1:
        raise SystemExit(f"expected exactly 1 contentUrl occurrence, found {n}")
    new = f"https://raw.githubusercontent.com/{GITHUB_REPO}/{commit}/uk-index-data.json"
    return html.replace(OLD_CONTENT_URL, new)


def banner(label, data_timestamp):
    return (
        '<div style="max-width:860px;margin:0 auto 0;padding:12px 16px;'
        'background:#fff8e6;border:1px solid #e8d9a8;border-radius:6px;'
        'font-family:system-ui,-apple-system,BlinkMacSystemFont,'
        "'Segoe UI',sans-serif;font-size:0.85rem;line-height:1.5;"
        'color:#4a3f1f;">'
        f'<strong>Archived edition.</strong> This is the {label} edition of '
        'the Rings.com UK Lab-Grown Diamond Price Index, exactly as '
        f'published on {data_timestamp}. These figures are frozen and will '
        f'not change. <a href="{LIVE_UK_INDEX_URL}" style="color:#4a3f1f;'
        'text-decoration:underline;">See the current edition</a>.'
        '</div>\n'
    )


def build_one(cycle, commit, html_file, source_files, pages_dir):
    label = CYCLE_LABELS[cycle]
    html = Path(WORKSPACE / html_file).read_text()
    data = json.loads(git_show(commit, "uk-index-data.json"))
    published_date = data["last_updated"]
    if published_date[:7] != cycle:
        raise SystemExit(f"{cycle}: last_updated {published_date} "
                         f"doesn't match cycle")
    capture_date = next(f for f in source_files if "2026-08" in f).split(
        "_")[-1].replace(".jsonl", "")

    html = strip_interactive_script(html)
    html = pin_content_url(html, commit)
    body = banner(label, published_date) + html

    handle = f"uk-lab-grown-diamond-price-index-{cycle}"
    title = f"UK Lab Grown Diamond Prices, {label} Edition (Archived) – Rings.com"
    h1 = f"UK Lab Grown Diamond Prices: {label} Edition (Archived)"
    keyword = f"uk lab grown diamond price {label.lower()}"

    hist_entry = next((h for h in data.get("history", [])
                       if h.get("month") == label), None)
    total_n = (hist_entry or {}).get("total_listings") or sum(
        c.get("stone_count", 0) for r in data.get("retailers", [])
        for c in r.get("cells", {}).values())
    retailer_set = sorted({r["name"] for r in data.get("retailers", [])})
    meta_desc = (f"Archived {label} edition of the Rings.com UK Lab-Grown "
                f"Diamond Price Index: median lab-grown diamond prices per "
                f"carat across {len(retailer_set)} UK retailers, as "
                f"published on {published_date}. Figures are frozen; see "
                f"the current edition for today's data.")

    record = {
        "handle": handle, "title": title, "h1": h1,
        "target_keyword": keyword,
        "page_type": "aggregate",
        "data_timestamp": capture_date, "cycle": cycle,
        "source_files": sorted(source_files), "sample_size": int(total_n),
        "retailer_set": retailer_set,
        "scope": {"cert_matched_only": False, "ring_total_present": False,
                  "ring_total_separated": False},
        "archived_edition": True,
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
    for cycle, commit, html_file, source_files in EDITIONS:
        handle, title, meta_desc, record = build_one(
            cycle, commit, html_file, source_files, pages_dir)
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
            "note": "UK archived edition (2026-09-17, Beyond Carat citation "
                    "request); May/June/July not archivable -- raw source "
                    "files no longer exist on disk",
        })
        meta[handle] = {"title_tag": title, "description_tag": meta_desc}

    (out_dir / "uk-qa-manifest.json").write_text(
        json.dumps({"pages": qa_pages}, indent=1) + "\n")
    (out_dir / "uk-archive-rollback-spec.json").write_text(
        json.dumps({"pages": spec_pages}, indent=1) + "\n")
    (out_dir / "uk-archive-meta.json").write_text(
        json.dumps(meta, indent=1) + "\n")

    print(f"built {len(qa_pages)} UK archive page(s) -> {out_dir}")
    for p in qa_pages:
        print(f"  /pages/{p['handle']}  ({p['cycle']}, n={p['sample_size']:,}, "
              f"{len(p['retailer_set'])} retailers)")
    print("May/June/July 2026 NOT archived: raw UK scrape files no longer "
         "exist on disk (checked data/raw/, data/snapshots/, git history).")


if __name__ == "__main__":
    main()
