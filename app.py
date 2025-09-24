# app.py
# ParrisRV listing-grid scraper (no detail-page visits; avoids bot challenge)
# Grabs: title, list_price, payments_from, payments_disclaimer, image_url, detail_url
# Hardened: robust autoscroll, real pagination clicking, &page=N fallback, retries, dedupe

import sys
import re
import html
import asyncio
from urllib.parse import urljoin

import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup

from playwright.async_api import async_playwright, TimeoutError as PWTimeout
import subprocess
import random

# ---------- Ensure Playwright Chromium ----------
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

# ---------- Windows asyncio policy fix ----------
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
    except NotImplementedError:
        if sys.platform.startswith("win"):
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
            return asyncio.run(coro)
        raise

# ---------- HTTP headers (not used for fetching, but for UA consistency) ----------
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0.0.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.parrisrv.com/",
    "Connection": "keep-alive",
})

# ---------- Helpers ----------
def clean_text(t):
    try:
        return re.sub(r"\s+", " ", t or "").strip()
    except Exception:
        return ""

def strip_fragment(u: str) -> str:
    if not u:
        return ""
    u = html.unescape(u or "").split("#", 1)[0].rstrip(").,;")
    return u[:-1] if u.endswith("/") else u

def strip_used_prefix(title: str) -> str:
    return re.sub(r'^\s*used\s*[:\-]?\s*', '', title or '', flags=re.I)

# ---------- Autoscroll ----------
async def autoscroll_until_stable(page, min_cycles=6, max_loops=240):
    async def count_cards():
        return await page.evaluate("""
            () => {
                const hrefs = new Set();
                document.querySelectorAll("a[href*='/product/used-']").forEach(a => hrefs.add(a.href));
                return hrefs.size;
            }
        """)
    async def click_load_more_if_any():
        selectors = [
            "button:has-text('Load More')","a:has-text('Load More')",
            "button:has-text('Show More')","a:has-text('Show More')",
            "[data-action='load-more']", ".load-more", ".show-more",
            "button[aria-label='Load more']", "button[aria-label='Show more']",
        ]
        for sel in selectors:
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

    # Wait for grid-ish container
    for sel in ["[class*='listing']", ".inventory", ".inventory-grid", ".results", "main", "#content"]:
        try:
            await page.wait_for_selector(sel, timeout=4000)
            break
        except Exception:
            pass

    stable, last = 0, -1
    for _ in range(max_loops):
        await page.evaluate("""async () => {
            const step = () => new Promise(r => { window.scrollBy(0, Math.max(1200, innerHeight*0.98)); setTimeout(r, 110); });
            for (let i=0;i<20;i++) await step();
        }""")
        try: await page.wait_for_load_state("networkidle", timeout=4500)
        except PWTimeout: pass

        if await click_load_more_if_any():
            try: await page.wait_for_load_state("networkidle", timeout=6000)
            except PWTimeout: pass

        curr = await count_cards()
        stable = stable + 1 if curr == last else 0
        last = curr
        if stable >= min_cycles:
            break

# ---------- Extract cards on listing page ----------
async def extract_cards_on_listing_page(page, base_url: str):
    records = await page.evaluate(
        """(base) => {
            const norm = (s) => (s || "").replace(/\\s+/g, " ").trim();
            const pickMoney = (s) => {
                const m = (s || "").match(/\\$\\s*[\\d,]+(?:\\.\\d{2})?/);
                return m ? m[0].replace(/\\s+/g,"") : "";
            };
            const pickPerMo = (s) => {
                const m = (s || "").match(/(\\$\\s*[\\d,]+(?:\\.\\d{2})?)\\s*\\/\\s*mo\\.?/i);
                return m ? m[1].replace(/\\s+/g,"") : "";
            };
            const abs = (u) => {
                try { return new URL(u, base).href; } catch { return u || ""; }
            };

            const seen = new Set();
            const out = [];

            const anchors = Array.from(document.querySelectorAll("a[href*='/product/used-']"));
            for (const a of anchors) {
                // find a reasonable card ancestor
                let card = a;
                for (let i=0; i<6 && card; i++) {
                    const hasPriceish = card.querySelector && card.querySelector(".price, .sale-price, .our-price, [class*='price'], [id*='price']");
                    const hasPayish   = card.querySelector && card.querySelector("[class*='payment'], [id*='payment'], .finance, .cta, .summary");
                    if (hasPriceish || hasPayish) break;
                    card = card.parentElement;
                }
                card = card || a.closest("article, .card, .inventory-item, .result, li, div");

                const detail_url = abs(a.href || "");
                if (!detail_url || seen.has(detail_url)) continue;
                seen.add(detail_url);

                // collect text
                const text = norm(card ? card.textContent || "" : a.textContent || "");

                // title
                let title = "";
                const h = card && (card.querySelector("h1,h2,h3,.title,[itemprop='name'],.product-title,.vehicle-title"));
                if (h) title = norm(h.textContent);
                if (!title) title = norm(a.textContent || "");
                title = title.replace(/^\\s*used\\s*[:\\-]?\\s*/i, "");

                // price
                let list_price = "";
                const priceEl = card && card.querySelector(".price, .sale-price, .our-price, [class*='price'], [id*='price'], [data-price]");
                if (priceEl) list_price = pickMoney(priceEl.textContent);
                if (!list_price) list_price = pickMoney(text);

                // payments
                let payments_from = "";
                const payEl = card && card.querySelector("[class*='payment'], [id*='payment'], .finance, .cta, .summary");
                if (payEl) payments_from = pickPerMo(payEl.textContent);
                if (!payments_from) payments_from = pickPerMo(text);

                // disclaimer near the card
                let disclaimer = "";
                let up = card, steps = 0;
                while (up && steps < 6) {
                    const hit = up.querySelector && up.querySelector(".payments-disclaimer-container, .payment-disclaimer, [class*='disclaimer']");
                    if (hit && hit.textContent) { disclaimer = norm(hit.textContent); if (disclaimer) break; }
                    up = up.parentElement; steps++;
                }

                // image
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
                    const m = style.match(/url\\((['\\"]?)([^)'"]+)\\1\\)/i);
                    if (m) image_url = m[2];
                }
                image_url = abs(image_url);

                out.push({
                    title, tagline: "", list_price, payments_from,
                    payments_disclaimer: disclaimer, image_url, detail_url
                });
            }
            return out;
        }""",
        base_url
    )

    # Clean & dedupe
    cleaned = []
    seen = set()
    for r in records:
        u = strip_fragment(r.get("detail_url",""))
        if not u or u in seen:
            continue
        seen.add(u)
        r["title"] = strip_used_prefix(r.get("title",""))
        # drop bot challenge/placeholder-looking titles
        t = (r["title"] or "").lower()
        if not t or "verify you are human" in t or t in {"used", '""",""used"'}:
            continue
        cleaned.append(r)
    return cleaned

# ---------- Pagination handlers ----------
async def discover_and_click_through_pagination(page, base_url: str, max_clicks: int = 50):
    """Use on-page pagination controls (Next/numbered pages)."""
    all_cards = []
    seen_pages = set()

    async def page_key():
        # a rough key based on current URL and a page marker in DOM if present
        try:
            href = await page.evaluate("() => location.href")
        except Exception:
            href = base_url
        try:
            marker = await page.evaluate("""() => {
                const p = document.querySelector(".pagination .active, .pager .active, .page-item.active");
                return p ? (p.textContent || "").trim() : "";
            }""")
        except Exception:
            marker = ""
        return f"{href}::{marker}"

    clicks = 0
    while clicks < max_clicks:
        await autoscroll_until_stable(page)
        key = await page_key()
        if key in seen_pages:
            break
        seen_pages.add(key)

        cards = await extract_cards_on_listing_page(page, base_url)
        all_cards.extend(cards)

        # Try to click "Next"
        next_clicked = False
        for sel in ["a[rel='next']", "button[rel='next']",
                    ".pagination a:has-text('Next')",
                    ".pager a:has-text('Next')",
                    "a.page-link:has-text('Next')"]:
            loc = page.locator(sel)
            try:
                if await loc.count() > 0 and await loc.first.is_visible():
                    await loc.first.click(timeout=2500)
                    try: await page.wait_for_load_state("networkidle", timeout=6000)
                    except PWTimeout: pass
                    next_clicked = True
                    break
            except Exception:
                pass

        # If no explicit Next, try numbered pages: current active -> next sibling
        if not next_clicked:
            try:
                has_num_paging = await page.evaluate("""() => !!document.querySelector(".pagination, .pager")""")
            except Exception:
                has_num_paging = False

            if has_num_paging:
                try:
                    # Try to click the first enabled page number greater than the active one
                    clicked = await page.evaluate("""() => {
                        const pagers = document.querySelectorAll(".pagination, .pager");
                        const tryClick = (el) => {
                            if (!el || el.classList.contains("disabled")) return false;
                            const a = el.querySelector("a,button");
                            if (a) { a.click(); return true; }
                            return false;
                        };
                        for (const p of pagers) {
                            const active = p.querySelector(".active");
                            if (active) {
                                let n = active.nextElementSibling;
                                while (n) {
                                    if (tryClick(n)) return true;
                                    n = n.nextElementSibling;
                                }
                            }
                            // fallback: last link with higher number than smallest
                            const nums = [...p.querySelectorAll("a,button")].map(x => (x.textContent||"").trim()).filter(x => /^\\d+$/.test(x));
                            if (nums.length) {
                                const maxNum = Math.max(...nums.map(x => parseInt(x,10)));
                                const current = parseInt((p.querySelector(".active")?.textContent||"").trim(), 10) || 1;
                                if (maxNum > current) {
                                    const cand = [...p.querySelectorAll("a,button")].find(x => (x.textContent||"").trim() == String(current+1));
                                    if (cand) { cand.click(); return true; }
                                }
                            }
                        }
                        return false;
                    }""")
                    if clicked:
                        try: await page.wait_for_load_state("networkidle", timeout=6000)
                        except PWTimeout: pass
                        next_clicked = True
                except Exception:
                    pass

        if not next_clicked:
            break  # no more pages
        clicks += 1

    # de-duplicate by URL
    uniq = {}
    for r in all_cards:
        uniq[strip_fragment(r["detail_url"])] = r
    return list(uniq.values())

async def iterate_pages_with_query_param(context, listing_url: str, max_pages: int = 40):
    """Fallback paginator: appends/updates &page=N."""
    all_cards = []
    empty_in_a_row = 0
    for page_num in range(1, max_pages + 1):
        if "page=" in listing_url:
            url = re.sub(r"([?&])page=\d+", rf"\1page={page_num}", listing_url)
        else:
            url = listing_url if page_num == 1 else f"{listing_url}&page={page_num}"

        page = await context.new_page()
        try:
            # a couple of retries per page
            cards = []
            for attempt in range(1, 4):
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    await autoscroll_until_stable(page)
                    cards = await extract_cards_on_listing_page(page, url)
                    if cards:
                        break
                    # gentle backoff if empty
                    await page.wait_for_timeout(400 + attempt * 300)
                except Exception:
                    if attempt == 3:
                        cards = []
                    else:
                        await page.wait_for_timeout(400 + attempt * 300)
                        continue

            if cards:
                all_cards.extend(cards)
                empty_in_a_row = 0
            else:
                empty_in_a_row += 1
                if page_num > 1 and empty_in_a_row >= 2:
                    break
        finally:
            await page.close()

    uniq = {}
    for r in all_cards:
        uniq[strip_fragment(r["detail_url"])] = r
    return list(uniq.values())

# ---------- Orchestrator ----------
async def collect_all_cards_across_pages(listing_url: str, max_pages: int = 40):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=SESSION.headers["User-Agent"])
        try:
            # First try: click through real pagination controls
            page = await context.new_page()
            try:
                await page.goto(listing_url, wait_until="domcontentloaded", timeout=60000)
                cards_via_clicks = await discover_and_click_through_pagination(page, listing_url)
            finally:
                await page.close()

            # If that produced a healthy count (e.g., > 30), use it; else, do the &page=N fallback too
            if len(cards_via_clicks) >= 30:
                base_set = {strip_fragment(r["detail_url"]): r for r in cards_via_clicks}
            else:
                base_set = {}

            cards_via_pages = await iterate_pages_with_query_param(context, listing_url, max_pages=max_pages)
            for r in cards_via_pages:
                base_set[strip_fragment(r["detail_url"])] = r

            return list(base_set.values())
        finally:
            await browser.close()

# ---------- Streamlit runner ----------
def run_scrape_from_listing(listing_url: str, max_pages: int = 40) -> pd.DataFrame:
    cards = run_coro_resilient(collect_all_cards_across_pages(listing_url, max_pages=max_pages))

    st.write(f"Collected **{len(cards)}** unique cards across pages.")

    # Final cleanup
    cols = ["title","tagline","list_price","payments_from","payments_disclaimer","image_url","detail_url"]
    for r in cards:
        for k in cols:
            r.setdefault(k, "")
    df = pd.DataFrame([{k: r.get(k,"") for k in cols} for r in cards])
    return df

# ---------- Streamlit UI ----------
st.set_page_config(page_title="ParrisRV Listing Scraper (Listing-Only, Full Coverage)", page_icon="🧹", layout="wide")
st.title("ParrisRV Listing Scraper — Full Coverage")
st.caption("Scrapes all listing pages (clicks Next and tries &page=N fallback) to capture every unit without visiting detail pages.")

default_url = "https://www.parrisrv.com/used-rvs-for-sale?s=true&lots=1232&pagesize=72&sort=year-asc"
listing_url = st.text_input("Listing URL", value=default_url, help="Example: 'used-rvs-for-sale' with pagesize & sort")

col_btn, col_info = st.columns([1, 3])
with col_btn:
    go = st.button("Run scrape", type="primary")

with col_info:
    st.write("Output: **title**, **list_price**, **payments_from**, **payments_disclaimer**, **image_url**, **detail_url**")

if go:
    try:
        with st.spinner("Scraping listing pages..."):
            df = run_scrape_from_listing(listing_url.strip(), max_pages=40)
        st.success(f"Done! {len(df)} rows.")
        st.dataframe(df, use_container_width=True)

        csv_bytes = df.to_csv(index=False, lineterminator="\n", encoding="utf-8-sig").encode("utf-8-sig")
        st.download_button(
            "Download CSV",
            data=csv_bytes,
            file_name="parrisrv_listings.csv",
            mime="text/csv",
        )
    except Exception as e:
        st.error("Something went wrong:")
        st.exception(e)
