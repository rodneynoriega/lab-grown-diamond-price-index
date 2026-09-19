import asyncio, json, sys
from playwright.async_api import async_playwright
S = sys.argv[1]; ROOT = "/Users/rodneynoriega/My Agents/Lab-Grown Diamond Price Index"
J = {"us": open(f"{ROOT}/PROJECTS/diamond-price-index/index-data.json").read(), "uk": open(f"{ROOT}/PROJECTS/diamond-price-index/uk-index-data.json").read()}
TIER = "() => document.querySelector('#lgd-history-table table').getAttribute('data-tier') + '/' + document.querySelector('#lgd-history-picker [aria-pressed=true]').getAttribute('data-tier')"
async def main():
    async with async_playwright() as pw:
        for engine in ("chromium", "webkit"):
            for name in ("us", "uk"):
                b = await getattr(pw, engine).launch(); ctx = await b.new_context(viewport={"width": 375, "height": 700}); page = await ctx.new_page(); errs = []
                page.on("pageerror", lambda e: errs.append(str(e)[:120]))
                async def route(r):
                    if "index-data.json" in r.request.url: await r.fulfill(status=200, content_type="application/json", body=J[name])
                    else: await r.abort()
                await page.route("**/*", lambda r: asyncio.ensure_future(route(r)) if r.request.url.startswith("http") else asyncio.ensure_future(r.continue_()))
                await page.goto(f"file://{S}/{name}_test.html")
                btn = page.locator("button[onclick*=lgdToggleHistory]"); await btn.wait_for(timeout=8000); await btn.click(); await page.wait_for_timeout(150)
                await page.click("#lgd-history-picker [data-tier='2ct']"); await page.wait_for_timeout(100)
                await btn.click(); await page.wait_for_timeout(100)   # collapse
                await page.set_viewport_size({"width": 800, "height": 700}); await page.wait_for_timeout(300)
                await btn.click(); await page.wait_for_timeout(200)   # reopen wide
                kept = await page.evaluate(TIER)
                await b.close()
                # fresh page: load wide, open narrow, expect 1ct default
                b = await getattr(pw, engine).launch(); ctx = await b.new_context(viewport={"width": 800, "height": 700}); page = await ctx.new_page()
                await page.route("**/*", lambda r: asyncio.ensure_future(route(r)) if r.request.url.startswith("http") else asyncio.ensure_future(r.continue_()))
                await page.goto(f"file://{S}/{name}_test.html"); btn = page.locator("button[onclick*=lgdToggleHistory]"); await btn.wait_for(timeout=8000)
                await page.set_viewport_size({"width": 375, "height": 700}); await page.wait_for_timeout(300); await btn.click(); await page.wait_for_timeout(200)
                fresh = await page.evaluate(TIER); await b.close()
                print(json.dumps({"engine": engine, "page": name, "picked_2ct_collapse_rotate_reopen": kept, "fresh_wide_load_open_narrow": fresh, "errors": errs}))
asyncio.run(main())
