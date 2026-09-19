#!/usr/bin/env python3
"""build_index_archives.py: frozen per-edition archive pages for the US
Lab-Grown Diamond Price Index (2026-09-17, Beyond Carat citation request:
"are prior editions kept accessible by month?").

Each archive page is the EXACT historical GemPages block that was actually
live that month (page-assets/us-index-gempages-<date>.html), not a
reconstruction from current template code run against old data -- the
generator script itself has changed shape several times, so replaying
today's code against May/June/July data would produce a page that never
actually existed. The body is pinned to the git commit whose index-data.json
produced it (both verified to agree on last_updated before pairing), and
wrapped with a frozen-edition banner. The JSON-LD Dataset distribution.
contentUrl is repointed from the live "main" branch at that exact commit's
raw file, so the archived dataset stays independently verifiable after the
live page's index-data.json moves on.

April 2026 has no archive: it predates this page format entirely (its
deliverable was a CSV + methodology note, not a GemPages page).

Outputs (under --out):
  pages/<handle>.html                 archived page bodies
  qa-manifest.json                    publish_gate.py record format,
                                       "archived_edition": true per page
  archive-rollback-spec.json          canary_create_drafts.py --spec format
  archive-meta.json                   canary_create_drafts.py --meta format

Usage: python3 build_index_archives.py [--out page_output_archive]
"""
import argparse
import json
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE / "page-assets"))

import page_pipeline as pp  # noqa: E402
import generate_us_gempages_block as gen  # noqa: E402

GITHUB_REPO = "rodneynoriega/lab-grown-diamond-price-index"
LIVE_INDEX_URL = "/pages/lab-grown-diamond-price-index"
OLD_CONTENT_URL = (f"https://raw.githubusercontent.com/{GITHUB_REPO}/"
                   f"main/index-data.json")

# (cycle, commit, html_file): commit verified to be the exact index-data.json
# state behind that html_file (last_updated fields cross-checked to agree,
# 2026-09-17), and to be pushed to origin/main (raw.githubusercontent.com
# only serves pushed content).
EDITIONS = [
    ("2026-05", "3cf9c2d", "page-assets/us-index-gempages-2026-05-21.html"),
    ("2026-06", "0d027dd", "page-assets/us-index-gempages-2026-06-29.html"),
    ("2026-07", "19afbcd", "page-assets/us-index-gempages-2026-07-27.html"),
    ("2026-08", "ef4d51a", "page-assets/us-index-gempages-2026-08-30.html"),
]

# The gate's data_timestamp must match a referenced source (raw scrape)
# filename, which is the CAPTURE date -- not index-data.json's last_updated
# (the page's PUBLISH date, a few days later; the banner shows that one).
# 2026-07/08 match page_pipeline.py's own CYCLE_TIMESTAMPS; 2026-05/06 have
# no page_pipeline.py entry (predates its cycle-selection support), taken
# from CYCLE_FILES' own majority date for that cycle instead.
CAPTURE_DATES = {"2026-05": "2026-05-20", "2026-06": "2026-06-27",
                 "2026-07": "2026-07-23", "2026-08": "2026-08-29"}


def git_show(commit, path):
    return subprocess.run(["git", "show", f"{commit}:{path}"], cwd=HERE,
                          capture_output=True, check=True, text=True).stdout


def pin_content_url(html, commit):
    n = html.count(OLD_CONTENT_URL)
    if n != 1:
        raise SystemExit(f"expected exactly 1 contentUrl occurrence, found {n}")
    new = f"https://raw.githubusercontent.com/{GITHUB_REPO}/{commit}/index-data.json"
    return html.replace(OLD_CONTENT_URL, new)


def banner(label, data_timestamp):
    return (
        '<div style="max-width:860px;margin:0 auto 0;padding:12px 16px;'
        'background:#fff8e6;border:1px solid #e8d9a8;border-radius:6px;'
        'font-family:system-ui,-apple-system,BlinkMacSystemFont,'
        "'Segoe UI',sans-serif;font-size:0.85rem;line-height:1.5;"
        'color:#4a3f1f;">'
        f'<strong>Archived edition.</strong> This is the {label} edition of '
        'the Rings.com Lab-Grown Diamond Price Index, exactly as published '
        f'on {data_timestamp}. These figures are frozen and will not change. '
        f'<a href="{LIVE_INDEX_URL}" style="color:#4a3f1f;'
        'text-decoration:underline;">See the current edition</a>.'
        '</div>\n'
    )


def build_one(cycle, commit, html_file, pages_dir):
    label = pp.CYCLE_LABELS[cycle]
    # The html_file itself is read from the working tree, not git-show'd at
    # `commit`: two of the four (May, July) were never committed at that
    # exact data commit (May's was added later, bundled into the June
    # commit; July's is untracked entirely), and August's received later
    # presentational-only fixes (verified against index-data.json's own
    # history: those later commits never touch the data file, only this
    # HTML). The on-disk copy is each edition's true final state; `commit`
    # is used only to pin the JSON-LD contentUrl and pull retailer_set/n.
    html = Path(HERE / html_file).read_text()
    data = json.loads(git_show(commit, "index-data.json"))
    published_date = data["last_updated"]  # what the page itself said
    capture_date = CAPTURE_DATES[cycle]    # matches raw source filenames
    if published_date[:7] != cycle:
        raise SystemExit(f"{cycle}: last_updated {published_date} "
                         f"doesn't match cycle")
    html = pin_content_url(html, commit)
    body = banner(label, published_date) + html

    handle = f"lab-grown-diamond-price-index-{cycle}"
    title = f"Lab Grown Diamond Prices, {label} Edition (Archived) – Rings.com"
    h1 = f"Lab Grown Diamond Prices: {label} Edition (Archived)"
    keyword = f"lab grown diamond price {label.lower()}"

    total_n = gen.total_listings_for_month(data)
    if not total_n:
        total_n = sum(c.get("stone_count", 0)
                      for r in data.get("retailers", [])
                      for c in r.get("cells", {}).values())
    retailer_set = sorted({r["name"] for r in data.get("retailers", [])})
    meta_desc = (f"Archived {label} edition of the Rings.com Lab-Grown "
                f"Diamond Price Index: median lab-grown diamond prices per "
                f"carat across {len(retailer_set)} US retailers, as "
                f"published on {published_date}. Figures are frozen; see "
                f"the current edition for today's data.")

    src = sorted(f"data/raw/{f}" for f in pp.CYCLE_FILES[cycle].values())
    record = {
        "handle": handle, "title": title, "h1": h1,
        "target_keyword": keyword,
        "page_type": "aggregate",
        "data_timestamp": capture_date, "cycle": cycle,
        "source_files": src, "sample_size": int(total_n),
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
    for cycle, commit, html_file in EDITIONS:
        handle, title, meta_desc, record = build_one(cycle, commit,
                                                      html_file, pages_dir)
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
            "note": "archived edition (2026-09-17, Beyond Carat citation request)",
        })
        meta[handle] = {"title_tag": title, "description_tag": meta_desc}

    (out_dir / "qa-manifest.json").write_text(
        json.dumps({"pages": qa_pages}, indent=1) + "\n")
    (out_dir / "archive-rollback-spec.json").write_text(
        json.dumps({"pages": spec_pages}, indent=1) + "\n")
    (out_dir / "archive-meta.json").write_text(
        json.dumps(meta, indent=1) + "\n")

    print(f"built {len(qa_pages)} archive pages -> {out_dir}")
    for p in qa_pages:
        print(f"  /pages/{p['handle']}  ({p['cycle']}, n={p['sample_size']:,}, "
              f"{len(p['retailer_set'])} retailers)")


if __name__ == "__main__":
    main()
