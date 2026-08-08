"""Chunked, checkpointed Clean Origin scraper.

Why this exists: in this environment, long-running background scrape jobs get
silently killed somewhere around 45-60 minutes of wall time (confirmed twice
on 2026-07-23 — no crash, no OOM, no stack trace, just gone). Clean Origin's
detail-enrichment phase alone needs ~19,000 per-stone fetches at the existing
req_delay=1.0s pace, several hours total — far longer than one job survives.
The stock `clean_origin.scrape()` only writes output at the very end, so a
kill mid-run loses 100% of the fetched details, not just the tail.

This script breaks the same work (same helpers, same req_delay, same
retry/backoff — nothing about the scrape logic itself is changed) into small
resumable chunks with a JSON checkpoint flushed continuously, so a kill any
time after the first successful fetch never loses more than a few seconds
of work.

Usage:
    python3 run_co_chunked.py listing                  # one-time: fetch + cache listing rows
    python3 run_co_chunked.py details --limit 500       # fetch up to N more uncached detail pages
    python3 run_co_chunked.py status                    # report progress, no network calls
    python3 run_co_chunked.py finalize                  # write data/raw/clean_origin_{DATE}.csv

Checkpoints live in data/raw/.co_checkpoint_{DATE}/ (listing.json, details.json).
"""

from __future__ import annotations

import json
import sys
import time
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from playwright.sync_api import sync_playwright

from retailers.clean_origin import (
    LIST_URL, RETAILER, _SHAPE_MAP, ALLOWED_CUTS, ALLOWED_COLORS,
    ALLOWED_CLARITIES, PAGE_SIZE, _ajax_url, _page_fetch, _parse_listing_rows,
    _total_count, _fetch_detail, _LIST_HEADERS,
)
from retailers.base import Diamond, CSV_FIELDS, diamond_to_row, MIN_CARAT, MAX_CARAT, TARGET_SHAPES

DATE = date.today().isoformat()
DATA_DIR = Path(__file__).parent / "data"
RAW_DIR = DATA_DIR / "raw"
CKPT_DIR = RAW_DIR / f".co_checkpoint_{DATE}"
LISTING_FILE = CKPT_DIR / "listing.json"
DETAILS_FILE = CKPT_DIR / "details.json"
FINAL_CSV = RAW_DIR / f"clean_origin_{DATE}.csv"

REQ_DELAY = 1.0  # same politeness delay as the stock scraper — not reduced


def _fetch_page_with_retry(page, url: str, headers: dict, max_retries: int = 4, base_backoff: float = 10.0):
    """Wraps _page_fetch with backoff retry for transient statuses (503/429/5xx —
    consistent with this retailer's known tarpitting behavior, see
    MEMORY/feedback_co_oval_cooldown.md). A 403 is treated as a hard block and
    raised immediately, matching the With Clarity precedent from this same cycle
    — don't hammer a real block, only retry through transient server hiccups."""
    status, html = None, None
    for attempt in range(max_retries):
        status, html = _page_fetch(page, url, headers)
        if status < 400:
            return status, html
        if status == 403:
            raise Exception("HTTP Error 403: Forbidden (hard block — not retrying)")
        wait = base_backoff * (attempt + 1)
        print(f"  [listing] got HTTP {status}, backing off {wait:.0f}s "
              f"(attempt {attempt + 1}/{max_retries})")
        time.sleep(wait)
    raise Exception(f"HTTP Error {status} after {max_retries} retries")


def _new_page(pw):
    browser = pw.chromium.launch(
        channel="chrome",
        headless=True,
        args=["--disable-blink-features=AutomationControlled"],
    )
    context = browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
    )
    context.add_init_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    page = context.new_page()
    page.goto(LIST_URL, wait_until="commit", timeout=60000)
    time.sleep(REQ_DELAY)
    return browser, page


def _load_historical_cache() -> dict[str, dict]:
    """Same idea as scrape.py's _load_co_detail_cache: url-keyed detail cache
    built from every prior day's clean_origin_*.csv (excludes today's, which
    doesn't exist yet at this point in the pipeline)."""
    import csv
    cache: dict[str, dict] = {}
    for csv_path in sorted(RAW_DIR.glob("clean_origin_*.csv")):
        if csv_path.name == FINAL_CSV.name:
            continue
        try:
            with csv_path.open(newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    url = (row.get("product_url") or "").strip()
                    if not url or url in cache:
                        continue
                    detail = {
                        "polish":       row.get("polish") or None,
                        "symmetry":     row.get("symmetry") or None,
                        "fluorescence": row.get("fluorescence") or None,
                        "cert_lab":     row.get("certificate_lab") or None,
                        "cert_number":  row.get("certificate_number") or None,
                    }
                    if any(v is not None for v in detail.values()):
                        cache[url] = detail
        except Exception:
            continue
    return cache


def _load_checkpoint_details() -> dict[str, dict]:
    if DETAILS_FILE.exists():
        with DETAILS_FILE.open(encoding="utf-8") as f:
            return json.load(f)
    return {}


def _save_checkpoint_details(details: dict[str, dict]) -> None:
    tmp = DETAILS_FILE.with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(details, f)
    tmp.replace(DETAILS_FILE)  # atomic — never leaves a half-written checkpoint


def cmd_listing() -> int:
    CKPT_DIR.mkdir(parents=True, exist_ok=True)
    if LISTING_FILE.exists():
        with LISTING_FILE.open(encoding="utf-8") as f:
            items = json.load(f)
        print(f"[listing] already fetched: {len(items)} qualifying stones cached at {LISTING_FILE.name}")
        return 0

    # Resume from a per-shape partial checkpoint if one exists from a prior
    # failed attempt, so a 503/crash partway through doesn't lose earlier shapes.
    partial_file = CKPT_DIR / "listing_partial.json"
    listing_items: list[dict] = []
    done_shapes: set[str] = set()
    if partial_file.exists():
        with partial_file.open(encoding="utf-8") as f:
            saved = json.load(f)
        listing_items = saved.get("items", [])
        done_shapes = set(saved.get("done_shapes", []))
        print(f"[listing] resuming from partial checkpoint: {len(listing_items)} stones, "
              f"shapes already done: {sorted(done_shapes)}")
    seen_urls: set[str] = {item["url"] for item in listing_items}

    with sync_playwright() as pw:
        browser, page = _new_page(pw)
        try:
            for raw_shape in TARGET_SHAPES:
                if raw_shape in done_shapes:
                    print(f"  [listing] {raw_shape} already done this run, skipping")
                    continue
                co_shape = _SHAPE_MAP.get(raw_shape.lower())
                if co_shape is None:
                    print(f"  [listing] shape '{raw_shape}' not supported, skipping")
                    continue

                weight_param = f"{MIN_CARAT:.2f}-{MAX_CARAT:.2f}"
                page_num = 1
                shape_seen_count = 0

                try:
                    while True:
                        url = _ajax_url(page_num, co_shape, weight_param)
                        status, html = _fetch_page_with_retry(page, url, _LIST_HEADERS)

                        if page_num == 1:
                            total = _total_count(html)
                            print(f"  [listing] {co_shape} {weight_param}ct: {total or '?'} total listings")

                        rows = _parse_listing_rows(html)
                        if not rows:
                            break

                        for r in rows:
                            url_item = r.get("url")
                            if not url_item or url_item in seen_urls:
                                continue
                            seen_urls.add(url_item)
                            shape_seen_count += 1

                            try:
                                carat = float(r["carat"])
                            except (TypeError, ValueError):
                                continue
                            if not (MIN_CARAT <= carat <= MAX_CARAT):
                                continue
                            if r.get("cut") not in ALLOWED_CUTS:
                                continue
                            if r.get("color") not in ALLOWED_COLORS:
                                continue
                            if r.get("clarity") not in ALLOWED_CLARITIES:
                                continue
                            try:
                                price = float(r["price"])
                            except (TypeError, ValueError):
                                continue

                            listing_items.append({
                                "shape": r.get("shape") or raw_shape,
                                "carat": carat,
                                "cut": r.get("cut"),
                                "color": r.get("color"),
                                "clarity": r.get("clarity"),
                                "price": price,
                                "url": url_item,
                            })

                        total_count = _total_count(html)
                        if total_count is not None and shape_seen_count >= total_count:
                            break
                        if len(rows) < PAGE_SIZE:
                            break

                        page_num += 1
                        time.sleep(REQ_DELAY)
                except Exception:
                    # Save whatever this shape collected before the pagination
                    # loop failed, so a retry resumes from the NEXT shape
                    # instead of redoing all pages already fetched.
                    with partial_file.open("w", encoding="utf-8") as f:
                        json.dump({"items": listing_items, "done_shapes": sorted(done_shapes)}, f)
                    raise

                done_shapes.add(raw_shape)
                with partial_file.open("w", encoding="utf-8") as f:
                    json.dump({"items": listing_items, "done_shapes": sorted(done_shapes)}, f)
                print(f"  [listing] {raw_shape} done: {shape_seen_count} qualifying stones this shape "
                      f"({len(listing_items)} total so far) — checkpoint saved")
        finally:
            browser.close()

    with LISTING_FILE.open("w", encoding="utf-8") as f:
        json.dump(listing_items, f)
    partial_file.unlink(missing_ok=True)
    print(f"[listing] complete: {len(listing_items)} qualifying stones -> {LISTING_FILE.name}")
    return 0


def cmd_status() -> int:
    if not LISTING_FILE.exists():
        print("[status] listing not fetched yet — run: python3 run_co_chunked.py listing")
        return 0
    with LISTING_FILE.open(encoding="utf-8") as f:
        listing_items = json.load(f)
    historical = _load_historical_cache()
    today_ckpt = _load_checkpoint_details()
    combined = {**historical, **today_ckpt}
    urls = {item["url"] for item in listing_items}
    covered = sum(1 for u in urls if u in combined)
    print(f"[status] listing: {len(listing_items)} stones")
    print(f"[status] detail coverage: {covered}/{len(urls)} "
          f"({historical and len(historical) or 0} from historical cache, "
          f"{len(today_ckpt)} fetched this cycle)")
    print(f"[status] remaining: {len(urls) - covered}")
    return 0


def cmd_details(limit: int) -> int:
    if not LISTING_FILE.exists():
        print("[details] ERROR: no listing checkpoint — run 'listing' first", file=sys.stderr)
        return 1
    with LISTING_FILE.open(encoding="utf-8") as f:
        listing_items = json.load(f)

    historical = _load_historical_cache()
    today_ckpt = _load_checkpoint_details()
    combined = {**historical, **today_ckpt}

    all_urls = [item["url"] for item in listing_items]
    total_remaining_before = sum(1 for u in set(all_urls) if u not in combined)

    uncached_urls: list[str] = []
    seen_batch: set[str] = set()
    for u in all_urls:
        if u not in combined and u not in seen_batch:
            uncached_urls.append(u)
            seen_batch.add(u)
        if len(uncached_urls) >= limit:
            break

    if not uncached_urls:
        print("[details] ALL_DETAILS_FETCHED — nothing left to fetch")
        return 0

    print(f"[details] batch of {len(uncached_urls)} (of {total_remaining_before} remaining total, "
          f"{len(set(all_urls))} unique stones overall)")

    fetched_this_batch = 0
    with sync_playwright() as pw:
        browser, page = _new_page(pw)
        try:
            for i, url in enumerate(uncached_urls, 1):
                detail = _fetch_detail(page, url)
                today_ckpt[url] = detail
                fetched_this_batch += 1
                if i % 25 == 0:
                    _save_checkpoint_details(today_ckpt)  # flush periodically — survives a mid-batch kill
                    print(f"  [details] progress: {i}/{len(uncached_urls)} (checkpoint flushed)")
                time.sleep(REQ_DELAY)
        finally:
            _save_checkpoint_details(today_ckpt)  # final flush no matter what
            browser.close()

    final_combined = {**historical, **today_ckpt}
    total_covered = sum(1 for u in set(all_urls) if u in final_combined)
    print(f"BATCH_DONE: fetched {fetched_this_batch} new details this batch. "
          f"Total covered: {total_covered}/{len(set(all_urls))}. "
          f"Remaining: {len(set(all_urls)) - total_covered}")
    return 0


def cmd_finalize() -> int:
    if not LISTING_FILE.exists():
        print("[finalize] ERROR: no listing checkpoint — run 'listing' first", file=sys.stderr)
        return 1
    with LISTING_FILE.open(encoding="utf-8") as f:
        listing_items = json.load(f)

    historical = _load_historical_cache()
    today_ckpt = _load_checkpoint_details()
    combined = {**historical, **today_ckpt}

    all_diamonds: list[Diamond] = []
    with_details = 0
    for item in listing_items:
        detail = combined.get(item["url"]) or {}
        if detail:
            with_details += 1
        all_diamonds.append(
            Diamond.build(
                retailer=RETAILER,
                shape=item["shape"],
                carat=item["carat"],
                color=item["color"],
                clarity=item["clarity"],
                cut=item["cut"],
                polish=detail.get("polish"),
                symmetry=detail.get("symmetry"),
                fluorescence=detail.get("fluorescence"),
                certificate_lab=detail.get("cert_lab"),
                certificate_number=detail.get("cert_number"),
                price_usd=item["price"],
                product_url=item["url"],
                date=DATE,
                scraped_at=f"{DATE}T12:00:00Z",
            )
        )

    with FINAL_CSV.open("w", newline="", encoding="utf-8") as f:
        import csv
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows([diamond_to_row(d) for d in all_diamonds])

    print(f"[finalize] wrote {len(all_diamonds)} rows -> {FINAL_CSV.name} "
          f"({with_details} with detail data, {len(all_diamonds) - with_details} listing-only)")
    return 0


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "status"
    if cmd == "listing":
        sys.exit(cmd_listing())
    elif cmd == "details":
        limit_idx = next((i for i, a in enumerate(sys.argv) if a == "--limit"), None)
        limit = int(sys.argv[limit_idx + 1]) if limit_idx is not None else 500
        sys.exit(cmd_details(limit))
    elif cmd == "status":
        sys.exit(cmd_status())
    elif cmd == "finalize":
        _code = cmd_finalize()
        from snapshot import sync as _snapshot_sync
        _snapshot_sync()
        sys.exit(_code)
    else:
        print(f"Unknown command: {cmd}", file=sys.stderr)
        sys.exit(1)
