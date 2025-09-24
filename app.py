import sys, re, html, asyncio, subprocess, random
from urllib.parse import urljoin
import pandas as pd
import streamlit as st
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

# ---------- Playwright bootstrap ----------
def _ensure_playwright_browser():
    try:
        async def _t():
            async with async_playwright() as p:
                b = await p.chromium.launch(headless=True)
                await b.close()
        asyncio.get_event_loop().run_until_complete(_t())
    except Exception:
        try:
            subprocess.check_call([sys.executable, "-m", "playwright", "install", "chromium"])
        except Exception as e:
            print("Playwright install fallback failed:", e)

_ensure_playwright_browser()

if sys.platform.startswith("win"):
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    except Exception:
        pass

def run_coro_resilient(coro):
    try:
        return asyncio.run(coro)
    except RuntimeError:
        loop = asyncio.new_event_loop()
        try:
            if sys.platform.startswith("win"):
                asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(coro)
        finally:
            try: loop.close()
            except Exception: pass

# ---------- helpers ----------
def clean_text(t): 
    try: return re.sub(r"\s+", " ", t or "").strip()
    except: return ""

def strip_fragment(u: str) -> str:
    if not u: return ""
    u = html.unescape(u).split("#", 1)[0].rstrip(").,;")
    return u[:-1] if u.endswith("/") else u

def strip_used_prefix(s: str) -> str:
    return re.sub(r'^\s*used\s*[:\-]?\s*', '', s or '', flags=re.I)

def is_money(s: str) -> bool:
    return bool(re.search(r"\$\s*[\d,]+(?:\.\d{2})?", s or ""))

# ---------- stealth context ----------
STEALTH_INIT = r"""
// Hide webdriver
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
// Fake plugins & languages
Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3]});
Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']});
// Chrome app stubs
window.chrome = window.chrome || { runtime: {} };
// Permissions query spoof
const originalQuery = window.navigator.permissions && window.navigator.permissions.query;
if (originalQuery) {
  window.navigator.permissions.query = (parameters) => (
    parameters.name === 'notifications'
      ? Promise.resolve({ state: Notification.permission })
      : originalQuery(parameters)
  );
}
"""

async def make_stealth_context(p):
    browser = await p.chromium.launch(
        headless=True,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox","--disable-dev-shm-usage",
            "--disable-gpu","--disable-features=IsolateOrigins,site-per-process",
        ],
    )
    context = await browser.new_context(
        user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"),
        viewport={"width": 1400, "height": 900},
        locale="en-US", timezone_id="America/Denver",
    )
    await context.add_init_script(STEALTH_INIT)
    # allow stylesheets, block heavy assets for speed only after first paint
    async def route_handler(route):
        rtype = route.request.resource_type
        if rtype in ("media","font","image"):
            return await route.abort()
        return await route.continue_()
    await context.route("**/*", route_handler)
    return browser, context

# ---------- scrolling ----------
async def autoscroll_until_stable(page, min_cycles=6, max_loops=260):
    async def count_cards():
        return await page.evaluate("""
            () => {
                const H = new Set();
                document.querySelectorAll("a[href*='/product/']").forEach(a => H.add(a.href));
                return H.size;
            }
        """)
    async def click_load_more_if_any():
        sels = [
            "button:has-text('Load More')","a:has-text('Load More')",
            "button:has-text('Show More')","a:has-text('Show More')",
            "[data-action='load-more']", ".load-more", ".show-more",
            "button[aria-label='Load more']", "button[aria-label='Show more']",
        ]
        for sel in sels:
            loc = page.locator(sel)
            try:
                if await loc.count() > 0 and await loc.first.is_visible():
                    await loc.first.click(timeout=2500)
                    try: await page.wait_for_load_state("networkidle", timeout=6000)
                    except PWTimeout: pass
                    return True
            except Exception:
                pass
        return False

    for sel in ["[class*='listing']", ".inventory", ".inventory-grid", ".results", "main", "#content"]:
        try:
            await page.wait_for_selector(sel, timeout=4500); break
        except Exception: pass

    stable, last = 0, -1
    for _ in range(max_loops):
        await page.evaluate("""async () => {
            const step = () => new Promise(r => { window.scrollBy(0, Math.max(1400, innerHeight*0.98)); setTimeout(r, 100); });
            for (let i=0;i<24;i++) await step();
        }""")
        try: await page.wait_for_load_state("networkidle", timeout=5000)
        except PWTimeout: pass
        if await click_load_more_if_any():
            try: await page.wait_for_load_state("networkidle", timeout=6000)
            except PWTimeout: pass
        curr = await count_cards()
        stable = stable + 1 if curr == last else 0
        last = curr
        if stable >= min_cycles: break

# ---------- card extraction from listing ----------
EXTRACT_JS = r"""
(base) => {
  const norm = (s) => (s || "").replace(/\s+/g, " ").trim();
  const money = (s) => { const m = (s||"").match(/\$\s*[\d,]+(?:\.\d{2})?/); return m ? m[0].replace(/\s+/g,"") : ""; };
  const permo = (s) => { const m = (s||"").match(/(\$\s*[\d,]+(?:\.\d{2})?)\s*\/\s*mo\.?/i); return m ? m[1].replace(/\s+/g,"") : ""; };
  const abs = (u) => { try { return new URL(u, base).href; } catch { return u || ""; } };

  const seen = new Set(), out = [];
  const anchors = Array.from(document.querySelectorAll("a[href*='/product/']"));
  for (const a of anchors) {
    const href = abs(a.href || "");
    if (!href || seen.has(href)) continue;
    // Only keep USED items (slug or surrounding text)
    if (!/\/product\/used-/i.test(href)) {
      const t = (a.textContent || "").toLowerCase();
      if (!/used/.test(t)) continue;
    }
    // find a reasonable card
    let card = a;
    for (let i=0; i<6 && card; i++) {
      const hasPrice = card.querySelector && card.querySelector(".price, .sale-price, .our-price, [class*='price'], [id*='price']");
      const hasPay   = card.querySelector && card.querySelector("[class*='payment'], [id*='payment'], .finance, .cta, .summary");
      if (hasPrice || hasPay) break;
      card = card.parentElement;
    }
    card = card || a.closest("article, .card, .inventory-item, .result, li, div");

    const text = norm(card ? card.textContent || "" : a.textContent || "");
    let title = "";
    const h = card && (card.querySelector("h1,h2,h3,.title,[itemprop='name'],.product-title,.vehicle-title"));
    if (h) title = norm(h.textContent);
    if (!title) title = norm(a.textContent || "");
    title = title.replace(/^\s*used\s*[:\-]?\s*/i, "");

    let list_price = "";
    const priceEl = card && card.querySelector(".price, .sale-price, .our-price, [class*='price'], [id*='price'], [data-price]");
    if (priceEl) list_price = money(priceEl.textContent);
    if (!list_price) list_price = money(text);

    let payments_from = "";
    const payEl = card && card.querySelector("[class*='payment'], [id*='payment'], .finance, .cta, .summary");
    if (payEl) payments_from = permo(payEl.textContent);
    if (!payments_from) payments_from = permo(text);

    let disclaimer = "";
    let up = card, steps = 0;
    while (up && steps < 6) {
      const hit = up.querySelector && up.querySelector(".payments-disclaimer-container, .payment-disclaimer, [class*='disclaimer']");
      if (hit && hit.textContent) { disclaimer = norm(hit.textContent); if (disclaimer) break; }
      up = up.parentElement; steps++;
    }

    let image_url = "";
    const img = card && (card.querySelector("img, picture source"));
    if (img) {
      image_url = img.getAttribute("data-src") || img.getAttribute("src") || "";
      const ss = img.getAttribute("srcset") || "";
      if (!image_url && ss) {
        const last = ss.split(",").pop().trim().split(" ")[0];
        if (last) image_url = last;
      }
    }
    if (!image_url) {
      const style = (card && card.getAttribute("style")) || "";
      const m = style.match(/url\((['"]?)([^)'"]+)\1\)/i);
      if (m) image_url = m[2];
    }
    image_url = abs(image_url);

    seen.add(href);
    out.push({ title, tagline: "", list_price, payments_from,
               payments_disclaimer: disclaimer, image_url, detail_url: href });
  }
  return out;
}
"""

async def extract_cards_on_listing_page(page, base_url: str):
    records = await page.evaluate(EXTRACT_JS, base_url)
    # clean + dedupe by URL
    uniq, out = {}, []
    for r in records:
        u = strip_fragment(r.get("detail_url",""))
        if not u: continue
        if u in uniq: continue
        t = strip_used_prefix(r.get("title",""))
        if not t or "verify you are human" in t.lower():  # filter challenge
            continue
        r["title"] = t
        uniq[u] = r
        out.append(r)
    return out

# ---------- pagination strategies ----------
async def click_through_pagination(page, base_url: str, max_clicks: int = 60):
    all_cards, seen_keys = [], set()

    async def page_key():
        try: href = await page.evaluate("() => location.href")
        except: href = base_url
        try:
            marker = await page.evaluate("""() => {
                const a = document.querySelector(".pagination .active, .pager .active, .page-item.active");
                return a ? (a.textContent || "").trim() : "";
            }""")
        except: marker = ""
        return f"{href}::{marker}"

    clicks = 0
    while clicks < max_clicks:
        await autoscroll_until_stable(page)
        key = await page_key()
        if key in seen_keys: break
        seen_keys.add(key)

        cards = await extract_cards_on_listing_page(page, base_url)
        all_cards.extend(cards)

        # try Next
        next_clicked = False
        for sel in ["a[rel='next']", "button[rel='next']",
                    ".pagination a:has-text('Next')", ".pager a:has-text('Next')",
                    "a.page-link:has-text('Next')"]:
            loc = page.locator(sel)
            try:
                if await loc.count() > 0 and await loc.first.is_visible():
                    await loc.first.click(timeout=2500)
                    try: await page.wait_for_load_state("networkidle", timeout=6000)
                    except PWTimeout: pass
                    next_clicked = True; break
            except Exception: pass

        # numbered pages (active -> next sibling)
        if not next_clicked:
            try:
                clicked = await page.evaluate("""() => {
                    const P = document.querySelector(".pagination, .pager");
                    if (!P) return false;
                    const act = P.querySelector(".active");
                    const clickEl = (el)=>{ if (!el || el.classList?.contains("disabled")) return false;
                        const a = el.querySelector("a,button"); if (a) { a.click(); return true; } return false; };
                    if (act) {
                        let n = act.nextElementSibling;
                        while (n) { if (clickEl(n)) return true; n = n.nextElementSibling; }
                    }
                    // fallback: click any higher number than active
                    const nums = [...P.querySelectorAll("a,button")].map(x => (x.textContent||"").trim()).filter(x => /^\d+$/.test(x));
                    if (nums.length) {
                        const cur = parseInt((act?.textContent||"1").trim(),10) || 1;
                        const cand = [...P.querySelectorAll("a,button")].find(x => (x.textContent||"").trim() == String(cur+1));
                        if (cand) { cand.click(); return true; }
                    }
                    return false;
                }""")
                if clicked:
                    try: await page.wait_for_load_state("networkidle", timeout=6000)
                    except PWTimeout: pass
                    next_clicked = True
            except Exception: pass

        if not next_clicked: break
        clicks += 1

    # dedupe
    uniq = {strip_fragment(r["detail_url"]): r for r in all_cards}
    return list(uniq.values())

async def iterate_pages_with_query_param(context, listing_url: str, max_pages: int = 50):
    all_cards, empty_streak = [], 0
    for page_num in range(1, max_pages + 1):
        url = (re.sub(r"([?&])page=\d+", rf"\1page={page_num}", listing_url)
               if "page=" in listing_url else
               (listing_url if page_num == 1 else f"{listing_url}&page={page_num}"))
        page = await context.new_page()
        try:
            cards = []
            for attempt in range(1, 4):
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    await autoscroll_until_stable(page)
                    cards = await extract_cards_on_listing_page(page, url)
                    if cards: break
                    await page.wait_for_timeout(300 + attempt*250)
                except Exception:
                    if attempt == 3: cards = []
                    else: await page.wait_for_timeout(300 + attempt*250)
            if cards:
                all_cards.extend(cards); empty_streak = 0
            else:
                empty_streak += 1
                if page_num > 1 and empty_streak >= 2: break
        finally:
            await page.close()
    uniq = {strip_fragment(r["detail_url"]): r for r in all_cards}
    return list(uniq.values())

# ---------- orchestrator ----------
async def collect_all_cards_across_pages(listing_url: str, max_pages: int = 50):
    async with async_playwright() as p:
        browser, context = await make_stealth_context(p)
        try:
            # try real pagination
            page = await context.new_page()
            try:
                await page.goto(listing_url, wait_until="domcontentloaded", timeout=60000)
                via_clicks = await click_through_pagination(page, listing_url)
            finally:
                await page.close()

            # fallback: &page=N
            via_pages = await iterate_pages_with_query_param(context, listing_url, max_pages=max_pages)

            # merge + dedupe
            merged = {strip_fragment(r["detail_url"]): r for r in via_clicks}
            for r in via_pages:
                merged[strip_fragment(r["detail_url"])] = r
            return list(merged.values())
        finally:
            await context.close()
            await browser.close()

def run_scrape_from_listing(listing_url: str, max_pages: int = 50) -> pd.DataFrame:
    cards = run_coro_resilient(collect_all_cards_across_pages(listing_url, max_pages=max_pages))
    st.write(f"Collected **{len(cards)}** unique cards across pages.")
    cols = ["title","tagline","list_price","payments_from","payments_disclaimer","image_url","detail_url"]
    for r in cards:
        for k in cols:
            r.setdefault(k, "")
    return pd.DataFrame([{k: r.get(k,"") for k in cols} for r in cards])

# ---------- UI ----------
st.set_page_config(page_title="ParrisRV Listing Scraper — Full", page_icon="🧹", layout="wide")
st.title("ParrisRV Listing Scraper — Listing-Only, Full Coverage")
st.caption("Grabs *all* units from the listing grid (no detail pages). Uses stealth + robust pagination.")

default_url = "https://www.parrisrv.com/used-rvs-for-sale?s=true&lots=1232&pagesize=72&sort=year-asc"
listing_url = st.text_input("Listing URL", value=default_url)

col_btn, col_info = st.columns([1,3])
with col_btn:
    go = st.button("Run scrape", type="primary")
with col_info:
    st.write("Outputs: **title**, **list_price**, **payments_from**, **payments_disclaimer**, **image_url**, **detail_url**")

if go:
    try:
        with st.spinner("Scraping listing pages..."):
            df = run_scrape_from_listing(listing_url.strip(), max_pages=50)
        st.success(f"Done! {len(df)} rows.")
        st.dataframe(df, use_container_width=True)
        csv_bytes = df.to_csv(index=False, lineterminator="\n", encoding="utf-8-sig").encode("utf-8-sig")
        st.download_button("Download CSV", data=csv_bytes, file_name="parrisrv_listings.csv", mime="text/csv")
        if df.empty:
            st.warning("No rows found. If this persists, try re-running or reducing pagesize to 24/36 (some themes lazy-load differently).")
    except Exception as e:
        st.error("Something went wrong:")
        st.exception(e)
