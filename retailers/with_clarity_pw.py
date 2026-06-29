"""With Clarity scraper — Playwright (real Chrome) variant.

Drop-in alternative to `with_clarity.py` for when the curl_cffi path gets
403'd / served non-JSON (Cloudflare bot-block). Mirrors the Clean Origin
recipe: drive a real installed Chrome and run the request via
`page.evaluate(fetch ...)` from *inside* a live withclarity.com page.

Why this beats curl_cffi when WC is defensive:
  - The fetch runs in the browser's JS context, sharing its cookies and TLS
    fingerprint, so the WAF sees an ordinary same-session XHR.
  - The API (vportalwithclarity.com) is a different origin from the page
    (withclarity.com). Issuing the fetch from within the withclarity.com page
    makes the browser set Origin/Referer to withclarity.com automatically —
    identical to what the real storefront JS does, so CORS passes too.

IMPORTANT: a real browser does NOT defeat an IP-level block. If the home IP
is flagged, this will still 403 — run it from a cooled-down IP or a fresh
*residential* IP (phone hotspot), never a datacenter VPN (Cloudflare distrusts
those). Use probe() to cheaply check an IP before a full run.

Same scrape() signature and output as with_clarity.py.
"""

from __future__ import annotations

import json
import sys
import time

from playwright.sync_api import sync_playwright

from .base import Diamond

RETAILER = "With Clarity"
SITE_ROOT = "https://www.withclarity.com"
WARMUP_URL = f"{SITE_ROOT}/collections/lab-diamonds"
API_URL = "https://vportalwithclarity.com/fetchdirectdiamond/"
DETAIL_URL_FMT = f"{SITE_ROOT}/products/diamonds?sku={{sku}}"

PAGE_SIZE = 20

_SHAPE_MAP: dict[str, str] = {
    "round": "Round",
    "oval": "Oval",
    "pear": "Pear",
    "cushion": "Cushion",
    "princess": "Princess",
    "emerald": "Emerald",
    "radiant": "Radiant",
    "marquise": "Marquise",
    "asscher": "Asscher",
    "heart": "Heart",
}

_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Third-party scripts on the storefront that recursively monkey-patch
# window.fetch and intermittently throw "TypeError: Failed to fetch". We abort
# their network requests so they never load and native fetch stays pristine.
# (Page rendering is irrelevant — we only need CF cookies + the correct origin.)
_FETCH_PATCHER_MARKERS = ("visually-io", "pushowl", "/wpm/", "shop_events_listener")

# POST run via native window.fetch (correct Origin/Referer, set by the browser —
# forbidden headers can't be overridden, so we only pass body + Content-Type).
# credentials:'omit' — the API returns a wildcard Access-Control-Allow-Origin;
# a credentialed cross-origin request against wildcard ACAO is hard-blocked.
# Cookies are scoped to withclarity.com and unused by the API anyway.
_FETCH_JS = """
    async ({url, body, timeoutMs}) => {
        // AbortController timeout so a stalled request (e.g. a flaky hotspot)
        // rejects instead of hanging the page.evaluate() forever. The Python
        // retry loop then re-issues the page.
        const ctrl = new AbortController();
        const timer = setTimeout(() => ctrl.abort(), timeoutMs);
        try {
            const resp = await fetch(url, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(body),
                credentials: 'omit',
                signal: ctrl.signal
            });
            return {status: resp.status, text: await resp.text()};
        } finally {
            clearTimeout(timer);
        }
    }
"""


# WC color codes (discovered 2026-06-28 by probing single-code requests and
# reading the returned color strings): 4=H, 5=G, 6=F, 7=E, 8=D, 9=YELLOW(fancy).
# Filtering color server-side is the key to dodging the per-IP volume block —
# the US benchmark is E only (code 7), which is a small slice of each band.
COLOR_CODE = {"H": 4, "G": 5, "F": 6, "E": 7, "D": 8}
_ALL_COLORS = [1, 2, 3, 4, 5, 6, 7, 8, 9]
_ALL_CUTS = [0, 1, 2, 3, 4, 5]
_ALL_CLARITIES = [1, 2, 3, 4, 5, 6, 7, 8, 9]

# Filled in at runtime by discover_codes() / hardcode once confirmed.
# clarity codes for the VS2-or-better floor (FL/IF/VVS1/VVS2/VS1/VS2) and the
# cut code(s) for Excellent — used to shrink the pull under WC's per-IP limit.
CLARITY_CODE: dict[str, int] = {}
CUT_CODE: dict[str, int] = {}


def _build_filter(shape_str: str, min_carat: float, max_carat: float, page: int,
                  colors: list[int] | None = None,
                  cuts: list[int] | None = None,
                  claritys: list[int] | None = None) -> list:
    # Positional filter array — identical to the curl_cffi scraper.
    return [
        {"shapes": [shape_str]},
        {"cuts": cuts if cuts else _ALL_CUTS},
        {"colors": colors if colors else _ALL_COLORS},
        {"claritys": claritys if claritys else _ALL_CLARITIES},
        {"labs": []},
        {"polish": [0, 1, 2, 3]},
        {"symmetrys": [0, 1, 2, 3]},
        {"price": "100,700000"},
        {"carat": f"{min_carat:.2f},{max_carat:.2f}"},
        {"page": page},
        {"orderBy": "Price"},
        {"sortBy": "ASC"},
        {"lwratio": "0.9,2.75"},
        {"fluorescences": [0, 1, 2, 3]},
        {"sku": ""},
        {"table": "40,90"},
        {"depth": "40,90"},
        {"type": ""},
        {"diamond_type": "lab"},
        {"cert_num": ""},
        {"quick_ship_diamonds": "N"},
        {"Appointment": ""},
        {"VaultDiscount": "No"},
        {"reports": ""},
        {"country": "US"},
        {"color_intensity": []},
    ]


def _post_filter(page, shape_str: str, min_carat: float, max_carat: float, page_num: int,
                 colors: list[int] | None = None, cuts: list[int] | None = None,
                 claritys: list[int] | None = None, retries: int = 3):
    """POST one page of results via in-browser fetch. Returns parsed payload dict.
    Retries transient "Failed to fetch" (third-party fetch-wrapper flakiness);
    raises on HTTP error, non-JSON, or exhausted retries so the caller can tell
    a real block (403) from a hiccup."""
    body = {"filter": _build_filter(shape_str, min_carat, max_carat, page_num,
                                    colors, cuts, claritys)}
    last_err: Exception | None = None
    for attempt in range(retries):
        try:
            result = page.evaluate(
                _FETCH_JS, {"url": API_URL, "body": body, "timeoutMs": 30000}
            )
        except Exception as e:  # Playwright "Failed to fetch" / abort / closed — transient
            last_err = e
            time.sleep(1.5 * (attempt + 1))
            continue
        status, text = result["status"], result["text"]
        if status >= 400:
            raise Exception(
                f"HTTP Error {status}: Forbidden" if status == 403 else f"HTTP Error {status}"
            )
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            snippet = (text or "")[:120].replace("\n", " ")
            raise Exception(f"non-JSON response (likely a block/challenge): {snippet!r}")
    raise Exception(f"fetch failed after {retries} attempts: {last_err}")


def _new_browser_page(pw):
    browser = pw.chromium.launch(
        channel="chrome",
        headless=True,
        args=["--disable-blink-features=AutomationControlled"],
    )
    context = browser.new_context(
        user_agent=_USER_AGENT,
        ignore_https_errors=True,  # vportalwithclarity.com has a broken cert chain
    )
    context.add_init_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    page = context.new_page()
    # Warm up on the page that actually uses this API: sets CF cookies and makes
    # the browser stamp Origin/Referer = withclarity.com on the cross-origin POST.
    # wait_until="commit" fires as soon as the response starts (cellular-friendly;
    # the heavy storefront DOM can exceed 60s over a hotspot). Retry on stall.
    last_err = None
    for attempt in range(3):
        try:
            page.goto(WARMUP_URL, wait_until="commit", timeout=45000)
            time.sleep(2)  # let CF cookies settle
            return browser, page
        except Exception as e:
            last_err = e
            print(f"  [with_clarity_pw] warm-up attempt {attempt + 1} failed: {e}")
            time.sleep(3)
    browser.close()
    raise Exception(f"warm-up failed after 3 attempts: {last_err}")


def probe(min_carat: float = 0.90, max_carat: float = 2.50, req_delay: float = 1.0) -> bool:
    """Cheap single-request check: does this IP/browser get data from WC?
    Returns True if page 1 returns diamonds. Use before committing to a full run."""
    with sync_playwright() as pw:
        browser, page = _new_browser_page(pw)
        time.sleep(req_delay)
        try:
            payload = _post_filter(page, "Round", min_carat, max_carat, 1)
            ld = payload["data"]["liveDiamondData"]
            diamonds = ld.get("diamond") or []
            total = ld.get("dataCount", 0)
            ok = len(diamonds) > 0
            print(
                f"  [with_clarity_pw] PROBE {'OK' if ok else 'EMPTY'}: "
                f"page1 returned {len(diamonds)} stones, dataCount={total}"
            )
            return ok
        except Exception as e:
            print(f"  [with_clarity_pw] PROBE BLOCKED: {e}")
            return False
        finally:
            browser.close()


def scrape(
    shapes: list[str],
    min_carat: float,
    max_carat: float,
    *,
    req_delay: float = 1.0,
    carat_bands: list[tuple[float, float]] | None = None,
    colors: list[int] | None = None,
    cuts: list[int] | None = None,
    claritys: list[int] | None = None,
) -> list[Diamond]:
    """Scrape WC via Playwright.

    carat_bands: optional list of (min, max) windows. When given, only those
    narrow windows are pulled instead of the full min_carat..max_carat range.
    This is the IP-block workaround — WC blocks an IP after too many requests,
    and the full range is thousands of pages. The benchmark bands are a small
    fraction, so a targeted pull finishes before the block trips. Grades are
    still filtered downstream (we pull all colors/clarities within each band).
    """
    bands = carat_bands or [(min_carat, max_carat)]
    all_diamonds: list[Diamond] = []
    seen_ids: set[str] = set()

    with sync_playwright() as pw:
        browser, page = _new_browser_page(pw)
        time.sleep(req_delay)

        for raw_shape in shapes:
            wc_shape = _SHAPE_MAP.get(raw_shape.lower())
            if wc_shape is None:
                print(f"  [with_clarity_pw] shape '{raw_shape}' not in shape map, skipping")
                continue

            for band_min, band_max in bands:
                page_num = 1
                band_total: int | None = None

                while True:
                    try:
                        payload = _post_filter(page, wc_shape, band_min, band_max, page_num,
                                               colors, cuts, claritys)
                    except Exception as e:
                        # Block/error mid-band: stop gracefully and keep what we
                        # have so far (caller saves the partial). Records the page
                        # we reached = a read on WC's per-IP budget.
                        print(f"  [with_clarity_pw] STOPPED at {wc_shape} "
                              f"{band_min:.2f}-{band_max:.2f}ct page {page_num} "
                              f"({len(all_diamonds)} collected so far): {e}")
                        browser.close()
                        print(f"  [with_clarity_pw] collected {len(all_diamonds)} diamonds (partial)")
                        return all_diamonds
                    ld = payload["data"]["liveDiamondData"]
                    diamonds_raw = ld.get("diamond") or []
                    total = ld.get("dataCount", 0)

                    if not diamonds_raw:
                        break

                    if page_num == 1:
                        band_total = total
                        print(
                            f"  [with_clarity_pw] {wc_shape} {band_min:.2f}-{band_max:.2f}ct: "
                            f"{band_total or '?'} stones (~{((band_total or 0) // PAGE_SIZE) + 1} pages)"
                        )

                    for d in diamonds_raw:
                        did = str(d.get("diamond_id") or d.get("cert_num") or "")
                        if not did or did in seen_ids:
                            continue
                        try:
                            carat = float(d["size"])
                        except (KeyError, ValueError, TypeError):
                            continue
                        price = d.get("total_discounted_sales_price")
                        if price is None:
                            continue
                        sku = d.get("cert_num") or did
                        seen_ids.add(did)
                        all_diamonds.append(
                            Diamond.build(
                                retailer=RETAILER,
                                shape=d.get("shape"),
                                carat=carat,
                                color=d.get("color"),
                                clarity=d.get("clarity"),
                                cut=d.get("cut"),
                                polish=d.get("polish"),
                                symmetry=d.get("symmetry"),
                                fluorescence=d.get("fluor_intensity"),
                                certificate_lab=d.get("lab"),
                                certificate_number=str(sku) if sku else None,
                                price_usd=float(price),
                                product_url=DETAIL_URL_FMT.format(sku=sku),
                            )
                        )

                    fetched_so_far = (page_num - 1) * PAGE_SIZE + len(diamonds_raw)
                    if band_total is not None and fetched_so_far >= band_total:
                        break
                    if len(diamonds_raw) < PAGE_SIZE:
                        break

                    page_num += 1
                    time.sleep(req_delay)

        browser.close()

    print(f"  [with_clarity_pw] collected {len(all_diamonds)} diamonds")
    return all_diamonds


if __name__ == "__main__":
    # `python3 -m retailers.with_clarity_pw`            -> single-request probe
    # `python3 -m retailers.with_clarity_pw scrape`     -> full round+oval pull
    mode = sys.argv[1] if len(sys.argv) > 1 else "probe"
    if mode == "scrape":
        scrape(["round", "oval"], 0.90, 2.50, req_delay=2.0)
    else:
        probe()
