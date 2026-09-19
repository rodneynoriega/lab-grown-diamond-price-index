#!/usr/bin/env python3
"""build_april_reconstruction.py: a RECONSTRUCTED (not archived) April 2026
edition page for the US index (2026-09-17, Rodney: "sure please build it",
following up on the archive-editions work when he asked why April wasn't
covered).

Different in kind from build_index_archives.py's May-August pages, which
are byte-faithful copies of the real historical GemPages block for that
month. April predates that page format entirely -- its actual 2026-04
deliverable was a CSV + a methodology note, never a page. There is no real
historical April page to snapshot.

What this script does instead: runs the CURRENT generate_html() (the same
code the live index uses today) against April's actual price data (commit
ee4cb25, its final April state -- verified: last_updated 2026-04-28,
cross-checked against the two other April-dated commits touching
index-data.json). Every number on the page is real and traceable (8
retailer raw CSVs, still on disk, summing to 476,988 listings, matching
the "477k stones" figure in the original April commit message exactly).
But the STRUCTURE, wording, and template code are today's, not April's --
sections like the retailer-selection box and Key Findings/Methodology
styling did not exist yet in April. The banner below says so explicitly;
do not remove that disclosure or relabel this as "archived" alongside the
real archive pages.

Schema patch required: April's index-data.json predates several fields
generate_html() now reads (history, collection_date, next_edition_note --
key_findings/methodology are NOT patched, deliberately: generate_html()'s
own data-driven fallback for those, computed directly from the real cell
min/max values, is more honest than anything hand-written here would be
for a month this script's author does not have firsthand knowledge of).

Usage: python3 build_april_reconstruction.py [--out page_output_archive]
"""
import argparse
import json
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE / "page-assets"))

import generate_us_gempages_block as gen  # noqa: E402

GITHUB_REPO = "rodneynoriega/lab-grown-diamond-price-index"
COMMIT = "ee4cb25"
CYCLE = "2026-04"
LIVE_INDEX_URL = "/pages/lab-grown-diamond-price-index"
OLD_CONTENT_URL = (f"https://raw.githubusercontent.com/{GITHUB_REPO}/"
                   f"main/index-data.json")

# The 8 retailers actually in April's panel (incl. James Allen, retired
# 2026-05-24 -- never part of the later pilot/stone pipeline, but it WAS
# genuinely published on the April index). Verified present on disk.
SOURCE_FILES = [
    "data/raw/blue_nile_2026-04-22.csv",
    "data/raw/brilliant_earth_2026-04-22.csv",
    "data/raw/clean_origin_2026-04-22.csv",
    "data/raw/grown_brilliance_2026-04-23.csv",
    "data/raw/james_allen_2026-04-23.csv",
    "data/raw/ritani_2026-04-23.csv",
    "data/raw/vrai_2026-04-22.csv",
    "data/raw/with_clarity_2026-04-22.csv",
]


def git_show(commit, path):
    return subprocess.run(["git", "show", f"{commit}:{path}"], cwd=HERE,
                          capture_output=True, check=True, text=True).stdout


def count_listings():
    total = 0
    for f in SOURCE_FILES:
        p = HERE / f
        if not p.exists():
            raise SystemExit(f"missing source file: {p}")
        with p.open() as fh:
            total += sum(1 for _ in fh) - 1  # minus header
    return total


def banner(data_timestamp):
    return (
        '<div style="max-width:860px;margin:0 auto 0;padding:12px 16px;'
        'background:#e8eef7;border:1px solid #b9c9e0;border-radius:6px;'
        'font-family:system-ui,-apple-system,BlinkMacSystemFont,'
        "'Segoe UI',sans-serif;font-size:0.85rem;line-height:1.5;"
        'color:#1f3a5f;">'
        '<strong>Reconstructed edition, not an archived one.</strong> '
        'April 2026 was the inaugural edition, published before this page '
        'format existed (the original deliverable was a data file and a '
        'methodology note, not a page). This page is built from that '
        f'edition’s actual price data, published on {data_timestamp} '
        '(every number here is real and sourced), using the CURRENT page '
        'template -- so the structure and wording reflect today’s '
        'site, not what April actually looked like. The May-August '
        'editions linked below are byte-faithful copies of what was truly '
        f'live that month; this one is not. '
        f'<a href="{LIVE_INDEX_URL}" style="color:#1f3a5f;'
        'text-decoration:underline;">See the current edition</a>.'
        '</div>\n'
    )


def pin_content_url(html, commit):
    n = html.count(OLD_CONTENT_URL)
    if n != 1:
        raise SystemExit(f"expected exactly 1 contentUrl occurrence, found {n}")
    new = f"https://raw.githubusercontent.com/{GITHUB_REPO}/{commit}/index-data.json"
    return html.replace(OLD_CONTENT_URL, new)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--out", default=str(HERE / "page_output_archive"))
    args = ap.parse_args()
    out_dir = Path(args.out)
    pages_dir = out_dir / "pages"
    pages_dir.mkdir(parents=True, exist_ok=True)

    data = json.loads(git_show(COMMIT, "index-data.json"))
    published_date = data["last_updated"]
    if published_date[:7] != CYCLE:
        raise SystemExit(f"last_updated {published_date} doesn't match {CYCLE}")

    total_n = count_listings()
    # Schema patch: only the fields generate_html() needs to not crash.
    # key_findings/methodology deliberately NOT patched -- see docstring.
    data["history"] = [{"month": "April 2026", "total_listings": total_n}]
    data["collection_date"] = "April 22-23, 2026"
    data["next_edition_note"] = "Updated monthly. Next edition: May 2026."

    html = gen.generate_html(data)
    html = pin_content_url(html, COMMIT)
    body = banner(published_date) + html

    handle = "lab-grown-diamond-price-index-2026-04"
    title = "Lab Grown Diamond Prices, April 2026 Edition (Reconstructed) – Rings.com"
    h1 = "Lab Grown Diamond Prices: April 2026 Edition (Reconstructed)"
    keyword = "lab grown diamond price april 2026"
    retailer_set = sorted({r["name"] for r in data.get("retailers", [])})
    meta_desc = (f"Reconstructed April 2026 edition of the Rings.com "
                f"Lab-Grown Diamond Price Index: the inaugural edition's "
                f"real price data ({total_n:,} listings, {len(retailer_set)} "
                f"US retailers), rendered with the current page template "
                f"since the original April deliverable predated this page "
                f"format. See the banner for what that means.")

    record = {
        "handle": handle, "title": title, "h1": h1,
        "target_keyword": keyword,
        "page_type": "aggregate",
        "data_timestamp": "2026-04-22", "cycle": CYCLE,
        "source_files": sorted(SOURCE_FILES), "sample_size": int(total_n),
        "retailer_set": retailer_set,
        "scope": {"cert_matched_only": False, "ring_total_present": False,
                  "ring_total_separated": False},
        "archived_edition": True,  # reuses the same gate exemptions
        "reconstructed": True,      # informational only; gate doesn't key on this
    }
    (pages_dir / f"{handle}.html").write_text(body)

    spec = {"pages": [{
        "handle": handle, "title": title,
        "body_file": f"pages/{handle}.html",
        "source": {"cycle": CYCLE, "data_timestamp": record["data_timestamp"],
                   "source_files": record["source_files"],
                   "sample_size": record["sample_size"],
                   "retailer_set": record["retailer_set"]},
        "target_keyword": keyword,
        "note": "RECONSTRUCTED (not archived) April 2026 edition -- see "
                "the in-page banner and build_april_reconstruction.py's "
                "docstring for what that means",
    }]}
    meta = {handle: {"title_tag": title, "description_tag": meta_desc}}

    (out_dir / "april-qa-manifest.json").write_text(
        json.dumps({"pages": [record]}, indent=1) + "\n")
    (out_dir / "april-rollback-spec.json").write_text(
        json.dumps(spec, indent=1) + "\n")
    (out_dir / "april-meta.json").write_text(json.dumps(meta, indent=1) + "\n")

    print(f"built 1 reconstructed page -> {out_dir}")
    print(f"  /pages/{handle}  ({CYCLE}, n={total_n:,}, {len(retailer_set)} retailers)")


if __name__ == "__main__":
    main()
