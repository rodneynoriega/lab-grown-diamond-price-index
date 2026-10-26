#!/usr/bin/env python3
"""build_sept_archives.py: frozen September 2026 archive pages (US + UK) for the
October refresh's "Previous editions" lists. Staging only; creates no Shopify
objects.

Composes the same final form the existing 9 archive pages have after the
2026-10-10 passes (red superseded block + amber archived banner, both moved
under the in-page header by layout v4, mobile + heading-hiding <style>), but
in one step from the September block as it was LIVE in September:

  US: git HEAD copy of page-assets/us-index-gempages-2026-09-18.html (the
      working-tree copy carries 2026-10-05 edits, so it is not used), data
      commit 936d1ec (index-data.json September 2026, last_updated 2026-09-18).
  UK: OUTBOX/2026-09-18-uk-index-gempages-2026-09-18.html (differs from the
      live 2026-10-05 block only by the signup-position line), interactive
      <script> stripped as for the other UK archives, data commit 8474da9.

Banner/transform helpers are copied from build_index_archives.py,
build_uk_index_archives.py and apply_archive_banner_v2/v3/layout_v4.py.

Usage: python3 build_sept_archives.py [--out page_output_2026-10-archive-sep]
"""
import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
WORKSPACE = HERE.parent.parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE / "page-assets"))
import page_pipeline as pp  # noqa: E402
import generate_us_gempages_block as gen  # noqa: E402

REPO = "rodneynoriega/lab-grown-diamond-price-index"
LABEL = "September 2026"
CYCLE = "2026-09"
US_COMMIT, UK_COMMIT = "936d1ec", "8474da9"
CAPTURE = "2026-09-17"
MARK = 'data-lgd-superseded="1"'
STYLE_V4 = ('<style data-lgd-mobile-fix="1">'
            '.heading-wrapper--page{display:none}'
            '@media (max-width:700px){'
            '.page__content{min-width:0}'
            '#lgd-index{max-width:100%;overflow-x:auto}'
            '[data-lgd-superseded]{font-size:.95rem!important;padding:14px 16px!important}'
            '}</style>\n')


def git_show(commit, path):
    return subprocess.run(["git", "show", f"{commit}:{path}"], cwd=HERE,
                          capture_output=True, check=True, text=True).stdout


def pin(html, commit, fname):
    old = f"https://raw.githubusercontent.com/{REPO}/main/{fname}"
    assert html.count(old) == 1, (fname, html.count(old))
    return html.replace(old, f"https://raw.githubusercontent.com/{REPO}/{commit}/{fname}")


def repoint_url(html, live_page, archive_handle):
    old = f'"url": "https://rings.com{live_page}"'
    assert html.count(old) == 1, (live_page, html.count(old))
    return html.replace(old, f'"url": "https://rings.com/pages/{archive_handle}"')


def amber(brand, published, live_url):
    return ('<div style="max-width:860px;margin:0 auto 0;padding:12px 16px;background:#fff8e6;'
            'border:1px solid #e8d9a8;border-radius:6px;font-family:system-ui,-apple-system,'
            "BlinkMacSystemFont,'Segoe UI',sans-serif;font-size:0.85rem;line-height:1.5;color:#4a3f1f;\">"
            f'<strong>Archived edition.</strong> This is the {LABEL} edition of the {brand} Lab-Grown '
            f'Diamond Price Index, exactly as published on {published}. These figures are frozen and will '
            f'not change. <a href="{live_url}" style="color:#4a3f1f;text-decoration:underline;">'
            'See the current edition</a>.</div>\n')


def red(brand, live_url):
    return ('<div role="note" ' + MARK + ' style="max-width:860px;margin:0 auto 8px;padding:16px 18px;'
            'background:#fdecea;border:2px solid #c0392b;border-radius:6px;font-family:system-ui,'
            "-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;font-size:1.05rem;line-height:1.5;"
            'color:#4a1511;"><strong>Superseded edition. These are not current prices.</strong> '
            f'This page is a frozen historical record of the {LABEL} edition of the {brand} Lab-Grown '
            'Diamond Price Index. Its figures stay as published, but a newer edition is now current. '
            'Do not quote the figures below as current lab-grown diamond prices. '
            f'<a href="{live_url}" style="color:#4a1511;font-weight:700;text-decoration:underline;">'
            'See the current edition</a>.</div>\n')


def layout_v4(boxes, idx):
    """Same placement as apply_archive_layout_v4.transform: boxes go right after
    the in-page title block (the div holding the H1)."""
    assert boxes.count(MARK) == 1 and boxes.count("<div") == 2, "boxes shape"
    # September blocks carry a leading <style> (stat tiles/folds, added 2026-09-03);
    # keep it ahead of #lgd-index, the earlier archives had none.
    k = idx.index('<div id="lgd-index"')
    lead, idx = idx[:k], idx[k:]
    assert re.fullmatch(r"\s*(<style>.*?</style>\s*)*", lead, re.S), lead[:80]
    h1 = idx.index("<h1")
    close = idx.index("</div>", h1)
    assert "<div" not in idx[h1:close], "title block has nested div"
    close += len("</div>")
    wrap = '<div data-lgd-edition-notices="1" style="margin:16px 0 20px;">\n'
    return STYLE_V4 + lead + idx[:close] + "\n" + wrap + boxes.rstrip("\n") + "\n</div>" + idx[close:]


def strip_script(html):
    s = html.find("<script>\n")
    assert s != -1 and html.count("<script>\n") == 1
    e = html.find("</script>\n", s)
    assert e != -1
    return html[:s] + html[e + len("</script>\n"):]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="page_output_2026-10-archive-sep")
    a = ap.parse_args()
    out = HERE / a.out
    (out / "pages").mkdir(parents=True, exist_ok=True)
    qa, spec, meta = [], [], {}

    # ---------------- US ----------------
    d = json.loads(git_show(US_COMMIT, "index-data.json"))
    assert d["month"] == LABEL and d["last_updated"] == "2026-09-18", (d["month"], d["last_updated"])
    html = git_show("HEAD", "page-assets/us-index-gempages-2026-09-18.html")
    html = pin(html, US_COMMIT, "index-data.json")
    html = repoint_url(html, "/pages/lab-grown-diamond-price-index", f"lab-grown-diamond-price-index-{CYCLE}")
    live = "/pages/lab-grown-diamond-price-index"
    body = layout_v4(red("Rings.com", live) + amber("Rings.com", d["last_updated"], live), html)
    handle = f"lab-grown-diamond-price-index-{CYCLE}"
    title = f"Lab Grown Diamond Prices, {LABEL} Edition (Archived) – Rings.com"
    total_n = gen.total_listings_for_month(d) or 0
    retailers = sorted({r["name"] for r in d["retailers"]})
    src = sorted(f"data/raw/{f}" for f in pp.CYCLE_FILES[CYCLE].values())
    (out / "pages" / f"{handle}.html").write_text(body)
    qa.append({"handle": handle, "title": title, "h1": f"Lab Grown Diamond Prices: {LABEL} Edition (Archived)",
               "target_keyword": f"lab grown diamond price {LABEL.lower()}", "page_type": "aggregate",
               "data_timestamp": CAPTURE, "cycle": CYCLE, "source_files": src, "sample_size": int(total_n),
               "retailer_set": retailers,
               "scope": {"cert_matched_only": False, "ring_total_present": False, "ring_total_separated": False},
               "archived_edition": True})
    spec.append({"handle": handle, "title": title, "body_file": f"pages/{handle}.html",
                 "source": {"cycle": CYCLE, "data_timestamp": CAPTURE, "source_files": src,
                            "sample_size": int(total_n), "retailer_set": retailers},
                 "target_keyword": f"lab grown diamond price {LABEL.lower()}",
                 "note": "archived edition, September 2026 (October refresh, 2026-10-26); superseded block + mobile + layout v4 applied at build"})
    meta[handle] = {"title_tag": title,
                    "description_tag": (f"Archived {LABEL} edition of the Rings.com Lab-Grown Diamond Price Index: median "
                                        f"lab-grown diamond prices per carat across {len(retailers)} US retailers, as published on "
                                        f"{d['last_updated']}. Figures are frozen; see the current edition for today's data.")}
    print("US", handle, "n=", total_n, "retailers", len(retailers))

    # ---------------- UK ----------------
    d = json.loads(git_show(UK_COMMIT, "uk-index-data.json"))
    assert d["month"] == LABEL and d["last_updated"] == "2026-09-18"
    html = (WORKSPACE / "OUTBOX/2026-09-18-uk-index-gempages-2026-09-18.html").read_text()
    html = pin(strip_script(html), UK_COMMIT, "uk-index-data.json")
    html = repoint_url(html, "/pages/uk-lab-grown-diamond-price-index", f"uk-lab-grown-diamond-price-index-{CYCLE}")
    live = "/pages/uk-lab-grown-diamond-price-index"
    body = layout_v4(red("Rings.com UK", live) + amber("Rings.com UK", d["last_updated"], live), html)
    handle = f"uk-lab-grown-diamond-price-index-{CYCLE}"
    title = f"UK Lab Grown Diamond Prices, {LABEL} Edition (Archived) – Rings.com"
    hist = next(h for h in d["history"] if h["month"] == LABEL)
    retailers = sorted({r["name"] for r in d["retailers"]})
    src = [f"data/raw/uk_{s}_{CAPTURE}.jsonl" for s in ("77_diamonds", "diamond_labs", "quality_diamonds")]
    (out / "pages" / f"{handle}.html").write_text(body)
    qa.append({"handle": handle, "title": title, "h1": f"UK Lab Grown Diamond Prices: {LABEL} Edition (Archived)",
               "target_keyword": f"uk lab grown diamond price {LABEL.lower()}", "page_type": "aggregate",
               "data_timestamp": CAPTURE, "cycle": CYCLE, "source_files": sorted(src),
               "sample_size": int(hist["total_listings"]), "retailer_set": retailers,
               "scope": {"cert_matched_only": False, "ring_total_present": False, "ring_total_separated": False},
               "archived_edition": True})
    spec.append({"handle": handle, "title": title, "body_file": f"pages/{handle}.html",
                 "source": {"cycle": CYCLE, "data_timestamp": CAPTURE, "source_files": sorted(src),
                            "sample_size": int(hist["total_listings"]), "retailer_set": retailers},
                 "target_keyword": f"uk lab grown diamond price {LABEL.lower()}",
                 "note": "UK archived edition, September 2026 (October refresh); interactive script stripped; superseded block + layout v4 applied at build"})
    meta[handle] = {"title_tag": title,
                    "description_tag": (f"Archived {LABEL} edition of the Rings.com UK Lab-Grown Diamond Price Index: median "
                                        f"lab-grown diamond prices per carat across {len(retailers)} UK retailers, as published on "
                                        f"{d['last_updated']}. Figures are frozen; see the current edition for today's data.")}
    print("UK", handle, "n=", hist["total_listings"], "retailers", len(retailers))

    (out / "qa-manifest.json").write_text(json.dumps({"pages": qa}, indent=1) + "\n")
    (out / "archive-rollback-spec.json").write_text(json.dumps({"pages": spec}, indent=1) + "\n")
    (out / "archive-meta.json").write_text(json.dumps(meta, indent=1) + "\n")
    print("wrote", out)


if __name__ == "__main__":
    main()
