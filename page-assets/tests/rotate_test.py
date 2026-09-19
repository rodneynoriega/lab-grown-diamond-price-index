import asyncio, json, sys
from playwright.async_api import async_playwright
S = sys.argv[1]; ROOT = "/Users/rodneynoriega/My Agents/Lab-Grown Diamond Price Index"
J = {"us": open(f"{ROOT}/PROJECTS/diamond-price-index/index-data.json").read(), "uk": open(f"{ROOT}/PROJECTS/diamond-price-index/uk-index-data.json").read()}
STATE = """() => { const w = document.getElementById('lgd-history-table'); const t = w.querySelector('table'); const td = w.querySelector('tbody td');
  return {tier: t.getAttribute('data-tier'), snap: getComputedStyle(w).scrollSnapType, nameW: Math.round(td.getBoundingClientRect().width), fs: t.style.fontSize,
          pageOverflow: document.documentElement.scrollWidth - window.innerWidth, picker: document.querySelector('#lgd-history-picker [aria-pressed=true]').getAttribute('data-tier')}; }"""
async def main():
    async with async_playwright() as pw:
        for engine in ("chromium", "webkit"):
            for name in ("us", "uk"):
                b = await getattr(pw, engine).launch(); ctx = await b.new_context(viewport={"width": 800, "height": 700}); page = await ctx.new_page(); errs = []
                page.on("pageerror", lambda e: errs.append(str(e)[:120]))
                async def route(r):
                    if "index-data.json" in r.request.url: await r.fulfill(status=200, content_type="application/json", body=J[name])
                    else: await r.abort()
                await page.route("**/*", lambda r: asyncio.ensure_future(route(r)) if r.request.url.startswith("http") else asyncio.ensure_future(r.continue_()))
                await page.goto(f"file://{S}/{name}_test.html")
                btn = page.get_by_text("Show monthly price history"); await btn.wait_for(timeout=8000)
                # A: loaded wide, opened narrow -> expect 1ct default + snap
                await page.set_viewport_size({"width": 375, "height": 700}); await page.wait_for_timeout(300)
                await btn.click(); await page.wait_for_timeout(200); a = await page.evaluate(STATE)
                # B: now rotate to wide while open -> expect same tier (1ct), no snap, desktop sizing
                await page.set_viewport_size({"width": 800, "height": 700}); await page.wait_for_timeout(400); bst = await page.evaluate(STATE)
                # C: pick All while wide, rotate back to narrow -> expect all kept, snap on, narrow sizing
                await page.click("#lgd-history-picker [data-tier=all]"); await page.wait_for_timeout(100)
                await page.set_viewport_size({"width": 375, "height": 700}); await page.wait_for_timeout(400); c = await page.evaluate(STATE)
                await page.locator("#lgd-history-body").screenshot(path=f"{S}/rotate-{engine}-{name}-C.png")
                await b.close()
                print(json.dumps({"engine": engine, "page": name, "A_open_narrow": a, "B_rotated_wide": bst, "C_all_rotated_narrow": c, "errors": errs}))
asyncio.run(main())
