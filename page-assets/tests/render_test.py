import re, sys, json, asyncio
from playwright.async_api import async_playwright
ROOT = "/Users/rodneynoriega/My Agents/Lab-Grown Diamond Price Index"
S = sys.argv[1]
def strip_liquid(s): return re.sub(r"{%.*?%}", "", s, flags=re.S)
head = strip_liquid(open(f"{ROOT}/PROJECTS/diamond-price-index/page-assets/theme-liquid-lgd-head.liquid").read())
body_script = strip_liquid(open(f"{ROOT}/OUTBOX/2026-08-29-theme-liquid-lgd-body.liquid").read())
block = open(f"{ROOT}/OUTBOX/2026-08-29-us-index-gempages-block.html").read()
us_html = f"<!doctype html><html><head><meta name=viewport content='width=device-width,initial-scale=1'>{head}</head><body style='margin:0;font-family:-apple-system,Helvetica,Arial,sans-serif'><div style='padding:16px'>{block}</div>{body_script}</body></html>"
uk_block = open(f"{ROOT}/OUTBOX/uk-index-gempages-2026-08-29.html").read()
uk_html = f"<!doctype html><html><head><meta name=viewport content='width=device-width,initial-scale=1'></head><body style='margin:0;font-family:-apple-system,Helvetica,Arial,sans-serif'><div style='padding:16px'>{uk_block}</div></body></html>"
open(f"{S}/us_test.html","w").write(us_html); open(f"{S}/uk_test.html","w").write(uk_html)
us_json = open(f"{ROOT}/PROJECTS/diamond-price-index/index-data.json").read()
uk_json = open(f"{ROOT}/PROJECTS/diamond-price-index/uk-index-data.json").read()

async def run(pw, name, path, data_json, width):
    b = await pw.chromium.launch()
    ctx = await b.new_context(viewport={"width": width, "height": 844}, device_scale_factor=2)
    page = await ctx.new_page()
    errors = []
    page.on("pageerror", lambda e: errors.append(str(e)))
    page.on("console", lambda m: errors.append("console."+m.type+": "+m.text) if m.type == "error" else None)
    async def route(r):
        if "index-data.json" in r.request.url: await r.fulfill(status=200, content_type="application/json", body=data_json)
        else: await r.abort()
    await page.route("**/*", lambda r: asyncio.ensure_future(route(r)) if r.request.url.startswith("http") else asyncio.ensure_future(r.continue_()))
    await page.goto("file://" + path)
    btn = page.get_by_text("Show monthly price history")
    await btn.wait_for(timeout=8000)
    await btn.click()
    await page.wait_for_timeout(200)
    res = {"name": name, "width": width}
    res["default_tier"] = await page.eval_on_selector("#lgd-history-picker [aria-pressed=true]", "e => e.getAttribute('data-tier')")
    res["page_overflow"] = await page.evaluate("document.documentElement.scrollWidth - window.innerWidth")
    res["table_w"] = await page.eval_on_selector("#lgd-history-table table", "e => e.getBoundingClientRect().width")
    res["wrap_w"] = await page.eval_on_selector("#lgd-history-table", "e => e.clientWidth")
    res["cols"] = await page.eval_on_selector("#lgd-history-table tbody tr", "e => e.children.length")
    await page.locator("#lgd-history-body").screenshot(path=f"{S}/{name}-{width}-default.png")
    # switch to All weights, scroll the wrapper, check the retailer column stays pinned
    await page.click("#lgd-history-picker [data-tier=all]")
    await page.wait_for_timeout(100)
    res["all_cols"] = await page.eval_on_selector("#lgd-history-table tbody tr", "e => e.children.length")
    res["all_table_w"] = await page.eval_on_selector("#lgd-history-table table", "e => e.getBoundingClientRect().width")
    res["all_page_overflow"] = await page.evaluate("document.documentElement.scrollWidth - window.innerWidth")
    before = await page.eval_on_selector("#lgd-history-table tbody tr td", "e => e.getBoundingClientRect().left")
    await page.eval_on_selector("#lgd-history-table", "e => e.scrollLeft = 400")
    await page.wait_for_timeout(100)
    after = await page.eval_on_selector("#lgd-history-table tbody tr td", "e => e.getBoundingClientRect().left")
    res["scrollLeft"] = await page.eval_on_selector("#lgd-history-table", "e => e.scrollLeft")
    res["sticky_holds"] = abs(before - after) < 1
    res["all_picker"] = await page.eval_on_selector("#lgd-history-picker [aria-pressed=true]", "e => e.getAttribute('data-tier')")
    await page.locator("#lgd-history-body").screenshot(path=f"{S}/{name}-{width}-all-scrolled.png")
    # back to 2ct, confirm re-render
    await page.click("#lgd-history-picker [data-tier='2ct']")
    await page.wait_for_timeout(100)
    res["2ct_cols"] = await page.eval_on_selector("#lgd-history-table tbody tr", "e => e.children.length")
    res["2ct_first_cell"] = await page.eval_on_selector("#lgd-history-table tbody tr td:nth-child(2)", "e => e.textContent")
    res["errors"] = errors
    await b.close()
    print(json.dumps(res))

async def main():
    async with async_playwright() as pw:
        for name, path, dj in [("us", f"{S}/us_test.html", us_json), ("uk", f"{S}/uk_test.html", uk_json)]:
            for w in (390, 1280):
                await run(pw, name, path, dj, w)
asyncio.run(main())
