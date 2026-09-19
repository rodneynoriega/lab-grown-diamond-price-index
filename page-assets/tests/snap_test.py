import sys, json, asyncio
from playwright.async_api import async_playwright
S = sys.argv[1]
ROOT = "/Users/rodneynoriega/My Agents/Lab-Grown Diamond Price Index"
us_json = open(f"{ROOT}/PROJECTS/diamond-price-index/index-data.json").read()
uk_json = open(f"{ROOT}/PROJECTS/diamond-price-index/uk-index-data.json").read()

ALIGN_JS = """(mode) => {
  const wrap = document.getElementById('lgd-history-table');
  const tds = [...wrap.querySelector('tbody tr').children];
  const sticky = tds[0].getBoundingClientRect();
  const wr = wrap.getBoundingClientRect();
  const step = mode === 'all' ? 3 : 1;
  const starts = tds.slice(1).filter((_, i) => i % step === 0).map(td => td.getBoundingClientRect().left);
  const aligned = wrap.scrollLeft === 0 || starts.some(l => Math.abs(l - sticky.right) < 1.5);
  const last = tds[tds.length - 1].getBoundingClientRect();
  return {scrollLeft: wrap.scrollLeft, maxScroll: wrap.scrollWidth - wrap.clientWidth, aligned, lastFullyVisible: last.right <= wr.right + 1,
          firstCellText: tds[1].textContent, spacer: document.getElementById("lgd-history-inner").style.paddingRight, stickyW: Math.round(sticky.width), snapType: getComputedStyle(wrap).scrollSnapType, pad: wrap.style.scrollPaddingLeft};
}"""

async def run(pw, engine, name, path, data_json, width):
    b = await getattr(pw, engine).launch()
    ctx = await b.new_context(viewport={"width": width, "height": 844}, device_scale_factor=2, has_touch=True)
    page = await ctx.new_page()
    errors = []
    page.on("pageerror", lambda e: errors.append(str(e)))
    async def route(r):
        if "index-data.json" in r.request.url: await r.fulfill(status=200, content_type="application/json", body=data_json)
        else: await r.abort()
    await page.route("**/*", lambda r: asyncio.ensure_future(route(r)) if r.request.url.startswith("http") else asyncio.ensure_future(r.continue_()))
    await page.goto("file://" + path)
    btn = page.get_by_text("Show monthly price history"); await btn.wait_for(timeout=8000); await btn.click()
    await page.wait_for_timeout(150)
    r = {"engine": engine, "page": name, "width": width}
    r["tier"] = await page.eval_on_selector("#lgd-history-picker [aria-pressed=true]", "e => e.getAttribute('data-tier')")
    r["page_overflow"] = await page.evaluate("document.documentElement.scrollWidth - window.innerWidth")
    r["open"] = await page.evaluate(ALIGN_JS, "single")
    # scroll to the end, let snap settle
    await page.eval_on_selector("#lgd-history-table", "e => e.scrollLeft = 9999"); await page.wait_for_timeout(400)
    r["end"] = await page.evaluate(ALIGN_JS, "single")
    await page.locator("#lgd-history-table").screenshot(path=f"{S}/snap-{engine}-{name}-{width}-end.png")
    # mid scroll
    await page.eval_on_selector("#lgd-history-table", "e => e.scrollLeft = 30"); await page.wait_for_timeout(400)
    r["mid30"] = await page.evaluate(ALIGN_JS, "single")
    # All weights, scrolled
    await page.click("#lgd-history-picker [data-tier=all]"); await page.wait_for_timeout(150)
    await page.eval_on_selector("#lgd-history-table", "e => e.scrollLeft = 200"); await page.wait_for_timeout(400)
    r["all200"] = await page.evaluate(ALIGN_JS, "all")
    await page.locator("#lgd-history-table").screenshot(path=f"{S}/snap-{engine}-{name}-{width}-all200.png")
    # keyboard: focus 1.5ct, press Enter, focus must stay on it
    await page.focus("#lgd-history-picker [data-tier='1.5ct']"); await page.keyboard.press("Enter"); await page.wait_for_timeout(150)
    r["focus_kept"] = await page.evaluate("document.activeElement && document.activeElement.getAttribute('data-tier')")
    r["tier_after"] = await page.eval_on_selector("#lgd-history-picker [aria-pressed=true]", "e => e.getAttribute('data-tier')")
    r["errors"] = errors
    await b.close(); print(json.dumps(r))

async def main():
    async with async_playwright() as pw:
        for engine in ("chromium", "webkit"):
            for name, path, dj in [("us", f"{S}/us_test.html", us_json), ("uk", f"{S}/uk_test.html", uk_json)]:
                for w in (390, 360):
                    try: await run(pw, engine, name, path, dj, w)
                    except Exception as e: print(json.dumps({"engine": engine, "page": name, "width": w, "EXC": str(e)[:200]}))
asyncio.run(main())
