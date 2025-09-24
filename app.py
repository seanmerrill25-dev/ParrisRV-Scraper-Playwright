# app.py
# Streamlit UI for ParrisRV list-page scraper (Playwright-only, Streamlit Cloud friendly)
# New strategy: scrape EVERYTHING directly from the LISTING GRID (no detail-page fetches)
# Outputs: title, tagline(=blank here), list_price, payments_from, payments_disclaimer, image_url, detail_url

import sys
import re
import html
import asyncio
from urllib.parse import urlparse, urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup, NavigableString
import streamlit as st

from playwright.async_api import async_playwright, TimeoutError as PWTimeout
import subprocess

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

# ---------- HTTP session (headers only) ----------
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

def is_money(s: str) -> bool:
    return bool(re.search(r"\$\s*[\d,]+(?:\.\d{2})?", s or ""))

def extract_first_money(s: str) -> str:
    m = re.search(r"\$\s*[\d,]+(?:\.\d{2})?", s or "")
    return m.group(0).replace(" ", "") if m else ""

def extract_per_month(s: str) -> str:
    m = re.search(r"(\$\s*[\d,]+(?:\.\d{2})?)\s*/\s*mo\.?", s or "", re.I)
    return (m.group(1) or "").replace(" ", "") if m else ""

def best_img_from(el) -> str:
    # Try common attributes first
    for sel in ["img", "picture source", "[data-src]", "[data-image]", "[data-bg]"]:
        try:
            node = el.query_selector(sel)
            if node:
                for attr in ("src", "data-src", "data-image", "data-bg", "srcset"):
                    v = (node.get_attribute(attr) or "").strip()
                    if v:
                        # srcset -> pick the last (usually largest)
                        if attr == "srcset":
                            last = v.split(",")[-1].strip().split(" ")[0]
                            if last:
                                return last
                        return v
        except Exception:
            pass
    # fallback: search style background-image
    try:
        style = el.get_attribute("style") or ""
        m = re.search(r"url\((['\"]?)([^)'\"]+)\1\)", style, re.I)
        if m:
            return m.group(2)
    except Exception:
        pass
    return ""

# ---------- Playwright: listing-page auto-scroll ----------
async def autoscroll_until_stable(page, min_cycles=5, max_loops=200):
    async def count_cards():
        # Count likely unit cards (anchors with /product/used-)
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

    for sel in ["[class*='listing']", ".inventory", ".inventory-grid", ".results", "main", "#content"]:
        try:
            await page.wait_for_selector(sel, timeout=2500)
            break
        except Exception:
            pass

    stable, last = 0, -1
    for _ in range(max_loops):
        await page.evaluate("""async () => {
            const step = () => new Promise(r => { window.scrollBy(0, Math.max(900, innerHeight*0.98)); setTimeout(r, 120); });
            for (let i=0;i<18;i++) await step();
        }""")
        try: await page.wait_for_load_state("networkidle", timeout=4000)
        except PWTimeout: pass

        if await click_load_more_if_any():
            try: await page.wait_for_load_state("networkidle", timeout=5000)
            except PWTimeout: pass

        curr = await count_cards()
        stable = stable + 1 if curr == last else 0
        last = curr
        if stable >= min_cycles:
            break

# ---------- Extract cards on a single listing page (pure client-side) ----------
async def extract_cards_on_listing_page(page, base_url: str):
    """
    Returns a list of dicts with fields:
    title, list_price, payments_from, payments_disclaimer, image_url, detail_url
    """
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

            // A "card" is approximated by the nearest ancestor that contains a /product/used- link.
            const anchors = Array.from(document.querySelectorAll("a[href*='/product/used-']"));
            for (const a of anchors) {
                let card = a;
                for (let i=0;i<6 && card;i++){
                    if (card.querySelector && card.querySelector("a[href*='/product/used-']") && card.querySelector("[class*='price'], [id*='price'], .price, .sale-price, .our-price, [class*='payment'], .payment")) {
                        break;
                    }
                    card = card.parentElement;
                }
                card = card || a.closest("article, .card, .inventory-item, .result, li, div");
                const detail_url = abs(a.href || "");
                if (!detail_url || seen.has(detail_url)) continue;
                seen.add(detail_url);

                // text buckets
                const text = norm(card ? card.textContent || "" : a.textContent || "");

                // title: prefer heading inside the card, else anchor text
                let title = "";
                const h = card && (card.querySelector("h1,h2,h3,.title,[itemprop='name'],.product-title,.vehicle-title"));
                if (h) title = norm(h.textContent);
                if (!title) title = norm(a.textContent || "");
                // strip leading "Used"
                title = title.replace(/^\\s*used\\s*[:\\-]?\\s*/i, "");

                // price candidates
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
                    // try any style background on the card
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
    # dedupe and clean
    cleaned = []
    seen = set()
    for r in records:
        u = strip_fragment(r.get("detail_url",""))
        if not u or u in seen:
            continue
        seen.add(u)
        r["title"] = strip_used_prefix(r.get("title",""))
        cleaned.append(r)
    return cleaned

# ---------- Iterate across listing pages ----------
async def collect_all_cards_across_pages(listing_url: str, max_pages: int = 12):
    results = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=SESSION.headers["User-Agent"])
        try:
            for page_num in range(1, max_pages + 1):
                if "page=" in listing_url:
                    url = re.sub(r"([?&])page=\d+", rf"\\1page={page_num}", listing_url)
                else:
                    url = listing_url if page_num == 1 else f"{listing_url}&page={page_num}"

                page = await context.new_page()
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    await autoscroll_until_stable(page)
                    cards = await extract_cards_on_listing_page(page, url)
                    results.extend(cards)
                    # if a later page yields nothing, we assume we're done
                    if page_num > 1 and not cards:
                        break
                finally:
                    await page.close()
        finally:
            await browser.close()

    # Final dedupe by detail_url
    uniq = {}
    for r in results:
        uniq[strip_fragment(r["detail_url"])] = r
    return list(uniq.values())

# ---------- Streamlit scrape runner ----------
def run_scrape_from_listing(listing_url: str, max_pages: int = 12) -> pd.DataFrame:
    cards = run_coro_resilient(collect_all_cards_across_pages(listing_url, max_pages=max_pages))

    # Basic validation: drop rows that are clearly challenges/placeholders
    def bad_row(r):
        t = (r.get("title") or "").lower()
        return (not t) or ("verify you are human" in t) or (t in {"used", '""",""used"'})
    kept = [r for r in cards if not bad_row(r)]
    dropped = [r for r in cards if bad_row(r)]

    st.write(f"Found **{len(cards)}** cards across pages.")
    if dropped:
        st.warning(f"Dropped {len(dropped)} obvious non-units (bot challenges/placeholders).")

    cols = ["title","tagline","list_price","payments_from","payments_disclaimer","image_url","detail_url"]
    for r in kept:
        for k in cols:
            r.setdefault(k, "")
    df = pd.DataFrame([{k: r.get(k,"") for k in cols} for r in kept])
    return df

# ---------- Streamlit UI ----------
st.set_page_config(page_title="ParrisRV Scraper (Listing-Only)", page_icon="🧹", layout="wide")
st.title("ParrisRV Listing Scraper (Listing-Only Mode)")
st.caption("Scrapes directly from the listing grid (no detail-page visits) to avoid bot challenges.")

default_url = "https://www.parrisrv.com/used-rvs-for-sale?s=true&lots=1232&pagesize=72&sort=year-asc"
listing_url = st.text_input("Listing URL", value=default_url, help="Example: a 'used-rvs-for-sale' page with pagesize & sort")

col_btn, col_info = st.columns([1, 3])
with col_btn:
    go = st.button("Run scrape", type="primary")

with col_info:
    st.write("Output columns: **title**, **tagline** (blank), **list_price**, **payments_from** (e.g. $205), **payments_disclaimer**, **image_url**, **detail_url**")

if go:
    try:
        with st.spinner("Scraping listing pages..."):
            df = run_scrape_from_listing(listing_url.strip(), max_pages=20)  # allow more pages just in case
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
