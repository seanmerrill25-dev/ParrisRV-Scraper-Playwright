
# app.py
# Streamlit UI for ParrisRV list-page scraper (Playwright-only, Streamlit Cloud friendly)
# - One input: listing URL (the page with the grid of units)
# - Outputs: title, tagline, list_price, payments_from, payments_disclaimer, image_url
# - Removes leading "Used" from titles
# - payments_from is just the dollar amount (e.g., "$205")
# - Payment disclaimers captured from the listing cards via Playwright (async) across pages
# - Robust image extraction + tagline extraction w/ "Sleeps X!" edge-case bypass
# - Windows/Streamlit safe: Proactor loop + resilient coroutine runner

import sys
import re
import time
import html
import asyncio
import traceback
from urllib.parse import urlparse, urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup, NavigableString

import streamlit as st

# Playwright (async)
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

# --- Ensure Playwright Chromium is present ---
import subprocess

def _ensure_playwright_browser():
    """
    Try launching Chromium; if it fails, install it.
    """
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

# --- Windows asyncio policy fix (Playwright needs subprocess support) ---
if sys.platform.startswith("win"):
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    except Exception:
        pass


def run_coro_resilient(coro):
    """
    Run an async coroutine robustly under Streamlit/Windows.
    """
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
            try:
                loop.close()
            except Exception:
                pass
    except NotImplementedError:
        if sys.platform.startswith("win"):
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
            return asyncio.run(coro)
        raise


# --------------------------
# HTTP session (fallback for detail pages if needed)
# --------------------------
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


# --------------------------
# Helpers
# --------------------------
def clean_text(t):
    try:
        return re.sub(r"\s+", " ", t or "").strip()
    except Exception:
        return ""


def safe_get_attr(tag, attr, default=""):
    return tag.get(attr) if hasattr(tag, "get") else default


def safe_lower(s):
    return s.lower() if isinstance(s, str) else ""


def strip_fragment(u: str) -> str:
    """Remove #fragment and trailing punctuation; normalize trailing slash."""
    if not u:
        return ""
    u = html.unescape(u or "").split("#", 1)[0].rstrip(").,;")
    return u[:-1] if u.endswith("/") else u


def strip_used_prefix(title: str) -> str:
    """Remove leading 'Used', 'USED', 'Used:' or 'Used -' at the very start."""
    return re.sub(r'^\s*used\s*[:\-]?\s*', '', title or '', flags=re.I)


def is_floorplan_or_virtual_from_strings(*strings) -> bool:
    parts = []
    for s in strings:
        if isinstance(s, (list, tuple)):
            parts.extend([safe_lower(x) for x in s if isinstance(x, str)])
        else:
            parts.append(safe_lower(s))
    blob = " ".join(parts)
    return any(k in blob for k in ["floorplan", "floor plan", "virtual", "tour", "360"])


def pick_from_srcset(srcset: str) -> str:
    """Choose the largest width entry from a srcset."""
    if not srcset:
        return ""
    items = []
    for part in srcset.split(","):
        part = part.strip()
        m = re.match(r"(\S+)\s+(\d+)w", part)
        if m:
            url, w = m.group(1), int(m.group(2))
            items.append((w, url))
        else:
            toks = part.split()
            if toks:
                items.append((0, toks[0]))
    if not items:
        return ""
    items.sort(key=lambda x: x[0], reverse=True)
    return items[0][1]


def pick_img_url(img_tag) -> str:
    if not hasattr(img_tag, "get"):
        return ""
    cand = img_tag.get("data-src") or img_tag.get("src") or ""
    if not cand:
        ss = img_tag.get("srcset") or ""
        best = pick_from_srcset(ss)
        if best:
            cand = best
    return (cand or "").strip()


# ---- price/payment parsing (label-local & position-aware) ----
def _closest_amount_after_label(container_text: str, label_regex: re.Pattern, require_mo: bool = False):
    t = clean_text(container_text)
    mlab = label_regex.search(t) if t else None
    if not mlab:
        return ""
    start = mlab.end()
    if require_mo:
        m = re.search(r"\$\s*[\d,]+(?:\.\d{2})?\s*(?=/\s*mo\.?)", t[start:], flags=re.I)
        return m.group(0).replace(" ", "") if m else ""
    else:
        m = re.search(r"\$\s*[\d,]+(?:\.\d{2})?", t[start:])
        return m.group(0).replace(" ", "") if m else ""


def amount_near_label(soup, labels, mo_suffix=False):
    BLOCKLIST = ("disclaimer", "fine", "footnote", "legal", "terms", "finance")

    def is_blocklisted(tag):
        try:
            classes = " ".join(tag.get("class", [])).lower()
        except Exception:
            classes = ""
        return any(b in classes for b in BLOCKLIST)

    for lab in labels:
        lab_re = re.compile(lab, re.I)
        for node in soup.find_all(string=lab_re):
            parent = getattr(node, "parent", None)
            if not parent:
                continue

            # climb to a reasonable box
            box, hops = parent, 0
            while box and getattr(box, "name", None) not in ("div", "section", "article") and hops < 5:
                box = getattr(box, "parent", None)
                hops += 1
            if not box or is_blocklisted(box):
                continue

            # Robustly build text from the box
            try:
                if hasattr(box, "stripped_strings"):
                    _iter = getattr(box, "stripped_strings", []) or []
                    strings = list(_iter) if hasattr(_iter, "__iter__") else []
                    if not strings and hasattr(box, "get_text"):
                        strings = [box.get_text(" ", strip=True)]
                    text = " ".join(strings)
                else:
                    text = ""
            except Exception:
                text = clean_text(box.get_text(" ", strip=True)) if hasattr(box, "get_text") else ""

            amt = _closest_amount_after_label(text, lab_re, require_mo=mo_suffix)
            if amt:
                return amt

            # fallback: pick the largest $ in this box (with /mo when needed)
            candidates = []
            for m in re.finditer(r"\$\s*[\d,]+(?:\.\d{2})?", text):
                val = m.group(0).replace(" ", "")
                if mo_suffix:
                    after = text[m.end(): m.end()+20]
                    if not re.search(r"/\s*mo\.?", after, re.I):
                        continue
                candidates.append(val)
            if candidates:
                def to_num(s):
                    try:
                        return float(s.replace("$", "").replace(",", ""))
                    except Exception:
                        return 0.0
                candidates.sort(key=lambda s: to_num(s), reverse=True)
                return candidates[0]
    return ""


# ---- tagline parsing (with "Sleeps X!" edge-case allow) ----
def extract_tagline(soup, name_text: str) -> str:
    title = soup.find(["h1", "h2"])
    if title:
        redish, plain = [], []
        # handle element and text-node siblings
        for sib in list(getattr(title, "next_siblings", []))[:8]:
            if isinstance(sib, NavigableString):
                txt = clean_text(str(sib))
                if not txt:
                    continue
                low = safe_lower(txt)

                # Allow "Sleeps X!" as an explicit edge-case, even though "sleeps" is usually blacklisted
                if re.fullmatch(r"sleeps\s+\d+\s*!", low, flags=re.I):
                    return txt

                if any(k in low for k in [
                    "stock #", "length", "location", "sleeps",  # <- normally blacklisted
                    "list price", "sale price", "from:", "payment", "msrp",
                    "photos", "floorplan", "tour", "description", "specifications",
                    "contact", "call", "view", "video"
                ]):
                    continue
                if 2 <= len(txt) <= 90:
                    plain.append(txt)
                continue

            if not hasattr(sib, "get_text"):
                continue
            if safe_lower(getattr(sib, "name", "")) in ("script", "style"):
                continue
            txt = clean_text(sib.get_text())
            if not txt:
                continue
            low = safe_lower(txt)

            # Allow "Sleeps X!" edge-case
            if re.fullmatch(r"sleeps\s+\d+\s*!", low, flags=re.I):
                return txt

            if any(k in low for k in [
                "stock #", "length", "location", "sleeps",  # <- normally blacklisted
                "list price", "sale price", "from:", "payment", "msrp",
                "photos", "floorplan", "tour", "description", "specifications",
                "contact", "call", "view", "video"
            ]):
                continue
            if 2 <= len(txt) <= 90:
                classes = " ".join(safe_get_attr(sib, "class", [])).lower()
                style = safe_lower(safe_get_attr(sib, "style", ""))
                looks_red = ("red" in classes or "danger" in classes or
                             "subtitle" in classes or "subhead" in classes or
                             "color:#" in style or "color: rgb(" in style or "color:red" in style)
                (redish if looks_red else plain).append(txt)
        if redish:
            return redish[0]
        if plain:
            return plain[0]

    # fallback: first short non-spec line after the title in global text
    full_text = (soup.get_text("\n") or "").replace("\xa0", " ")
    lines = [ln.strip() for ln in full_text.split("\n") if ln.strip()]
    if name_text and name_text in lines:
        i = lines.index(name_text)
        for j in range(i + 1, min(i + 15, len(lines))):
            cand = lines[j].strip()
            low = safe_lower(cand)

            # Allow "Sleeps X!" edge-case
            if re.fullmatch(r"sleeps\s+\d+\s*!", low, flags=re.I):
                return cand

            if any(k in low for k in [
                "stock #", "length", "location", "sleeps",  # <- normally blacklisted
                "msrp", "list price", "sale price", "from:", "monthly", "payment",
                "photos", "floorplan", "tour", "description", "specifications",
                "contact", "call", "view", "video"
            ]):
                continue
            if 2 <= len(cand) <= 90:
                return cand
    return ""


# ---- image extraction (robust, many fallbacks) ----
IMG_BLACKLIST_KEYWORDS = (
    "logo", "header", "footer", "icon", "sprite", "map", "anniversary",
    "facebook", "twitter", "youtube", "instagram", "pinterest",
    "badge", "award", "favicon", "placeholder", "dummy", "pixel",
    "mfg_logo", "manufacturer", "certified", "seal", "floorplan"
)

def is_real_image(url: str) -> bool:
    return bool(re.search(r"\.(jpe?g|png|webp)(\?.*)?$", url, re.I))

def is_blacklisted(url_or_alt: str) -> bool:
    u = safe_lower(url_or_alt)
    return any(k in u for k in IMG_BLACKLIST_KEYWORDS)

def extract_main_image(soup: BeautifulSoup, detail_url: str) -> str:
    # 1) Meta tags
    meta_names = [
        ('property', 'og:image'),
        ('property', 'og:image:url'),
        ('name', 'twitter:image'),
        ('rel', 'image_src'),
    ]
    for attr, val in meta_names:
        if attr == 'rel':
            for link in soup.find_all('link', rel=re.compile(r"image_src", re.I)):
                href = link.get('href') or ""
                if href:
                    url = urljoin(detail_url, href)
                    if is_real_image(url) and not is_blacklisted(url):
                        return url
        else:
            for meta in soup.find_all('meta', attrs={attr: re.compile(val, re.I)}):
                content = meta.get('content') or ""
                if content:
                    url = urljoin(detail_url, content)
                    if is_real_image(url) and not is_blacklisted(url):
                        return url

    # 2) <picture><source srcset>
    for source in soup.find_all('source'):
        ss = source.get('srcset') or ""
        best = pick_from_srcset(ss)
        if best:
            url = urljoin(detail_url, best)
            if is_real_image(url) and not is_blacklisted(url):
                return url

    # 3) <noscript> with <img>
    for nos in soup.find_all('noscript'):
        try:
            ns = BeautifulSoup(nos.get_text() or "", "html.parser")
            img = ns.find('img')
            if img:
                cand = pick_img_url(img)
                if cand:
                    url = urljoin(detail_url, cand)
                    if is_real_image(url) and not is_blacklisted(url):
                        return url
        except Exception:
            pass

    # 4) background-image / data-* image attributes
    for el in soup.find_all(True):
        style = safe_get_attr(el, "style", "")
        for m in re.finditer(r"url\((['\"]?)([^)'\"]+)\1\)", style or "", re.I):
            url = urljoin(detail_url, m.group(2))
            if is_real_image(url) and not is_blacklisted(url):
                return url
        for attr in ("data-bg", "data-background", "data-image", "data-src"):
            v = safe_get_attr(el, attr, "")
            if v:
                url = urljoin(detail_url, v)
                if is_real_image(url) and not is_blacklisted(url):
                    return url

    # 5) plain <img> tags
    for img in soup.find_all("img"):
        url = pick_img_url(img)
        if not url:
            continue
        alt = safe_get_attr(img, "alt", "")
        src = safe_get_attr(img, "src", "")
        srcset = safe_get_attr(img, "srcset", "")
        abs_url = urljoin(detail_url, url)
        if not is_real_image(abs_url):
            continue
        if is_blacklisted(abs_url) or is_blacklisted(alt):
            continue
        if is_floorplan_or_virtual_from_strings(alt, src, srcset, abs_url):
            continue
        return abs_url

    # 6) inline scripts/JSON
    html_text = str(soup)
    for m in re.finditer(r"https?://[^\s\"'<>]+?\.(?:jpe?g|png|webp)(?:\?[^\s\"'<>]*)?", html_text, re.I):
        url = m.group(0)
        if not is_blacklisted(url) and "logo" not in url.lower() and "floorplan" not in url.lower():
            return url

    return ""


# --------------------------
# Parse detail page (robust)
# --------------------------
def parse_detail_html(detail_url: str, html_text: str):
    soup = BeautifulSoup(html_text or "", "html.parser")

    def norm(s: str) -> str:
        s = (s or "").replace("\xa0", " ")
        s = re.sub(r"\s+", " ", s).strip()
        return s

    # ---- Title (strip leading "Used", case-insensitive, handles unicode spaces)
    title_el = soup.find(["h1", "h2"]) or soup.select_one(".product-title, .vehicle-title, [itemprop='name']")
    raw_title = norm(title_el.get_text() if title_el else "")
    title = re.sub(r'^\s*used\s*[:\-]?\s*', '', raw_title, flags=re.I)

    # ---- Tagline
    tagline = extract_tagline(soup, raw_title)

    # ---- Prices
    def first_money(s: str) -> str:
        m = re.search(r"\$\s*[\d,]+(?:\.\d{2})?", s or "")
        return m.group(0).replace(" ", "") if m else ""

    # Try label-local strategy first (original approach, expanded labels)
    list_price = amount_near_label(
        soup,
        [r"\bList\s*Price\b", r"\bMSRP\b", r"\bSale\s*Price\b", r"\bPrice\b", r"Our\s*Price"],
        mo_suffix=False,
    )

    # If still empty, try common price containers
    if not list_price:
        price_candidates = []
        for el in soup.select(
            ".price, .our-price, .sale-price, .msrp, [class*='price'], [id*='price'], [data-price]"
        ):
            txt = norm(el.get_text())
            val = first_money(txt)
            if val:
                price_candidates.append(val)
        if price_candidates:
            # Pick the largest dollar figure—usually the main price
            def to_num(x):
                try: return float(x.replace("$","").replace(",",""))
                except: return 0.0
            price_candidates.sort(key=to_num, reverse=True)
            list_price = price_candidates[0]

    # ---- Payments (any $…/mo anywhere near payment-ish containers)
    payments_from = amount_near_label(
        soup, [r"\bPayments?\s*From\b", r"\bFrom:\b", r"As\s+low\s+as"], mo_suffix=True
    )
    if not payments_from:
        for el in soup.select("[class*='payment'], [id*='payment'], .details, .finance, .cta, .summary"):
            txt = norm(el.get_text())
            m = re.search(r"(\$\s*[\d,]+(?:\.\d{2})?)\s*/\s*mo\.?", txt, flags=re.I)
            if m:
                payments_from = m.group(1).replace(" ", "")
                break

    # ---- Image
    image_url = extract_main_image(soup, detail_url)

    return {
        "title": title,
        "tagline": tagline,
        "list_price": list_price,
        "payments_from": payments_from,
        "image_url": image_url,
    }


# --------------------------
# Playwright: listing-page auto-scroll + disclaimers
# --------------------------
async def autoscroll_until_stable(page, min_cycles=3, max_loops=60):
    async def count_links():
        # Count both product anchors and "View Details" CTAs
        return await page.evaluate("""
            () => {
                const a1 = Array.from(document.querySelectorAll("a[href*='/product/']"));
                const a2 = Array.from(document.querySelectorAll("a,button")).filter(el => /view\s+details/i.test(el.textContent || ""));
                const hrefs = new Set(a1.map(a => a.href).concat(a2.map(el => el.href).filter(Boolean)));
                return hrefs.size;
            }
        """)
    async def click_load_more_if_any():
        selectors = [
            "button:has-text('Load More')",
            "a:has-text('Load More')",
            "button:has-text('Show More')",
            "a:has-text('Show More')",
            "[data-action='load-more']",
            ".load-more",
            ".show-more",
        ]
        for sel in selectors:
            loc = page.locator(sel)
            try:
                if await loc.count() > 0 and await loc.first.is_visible():
                    await loc.first.click(timeout=2000)
                    try:
                        await page.wait_for_load_state("networkidle", timeout=5000)
                    except PWTimeout:
                        pass
                    return True
            except Exception:
                pass
        return False

    # Try to ensure the grid exists
    for sel in ["[class*='listing']", ".inventory", ".inventory-grid", ".results", "main"]:
        try:
            await page.wait_for_selector(sel, timeout=2000)
            break
        except Exception:
            pass

    stable = 0
    last = -1
    for _ in range(max_loops):
        await page.evaluate(
            """async () => {
                const step = () => new Promise(r => {
                    window.scrollBy(0, Math.max(600, innerHeight * 0.95));
                    setTimeout(r, 140);
                });
                for (let i = 0; i < 10; i++) await step();
            }"""
        )
        try:
            await page.wait_for_load_state("networkidle", timeout=2500)
        except PWTimeout:
            pass

        clicked = await click_load_more_if_any()
        if clicked:
            try:
                await page.wait_for_load_state("networkidle", timeout=4000)
            except PWTimeout:
                pass

        curr = await count_links()
        if curr == last:
            stable += 1
        else:
            stable = 0
        last = curr
        if stable >= min_cycles:
            break


async def fetch_disclaimers_on_page(context, url: str) -> dict:
    """Return {normalized detail URL -> disclaimer text} for a single listing page."""
    page = await context.new_page()
    disc_map = {}
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await autoscroll_until_stable(page)

        records = await page.evaluate(
            """() => {
                const norm = (u) => {
                    try {
                        const a = new URL(u, location.href);
                        a.hash = "";
                        let href = a.href;
                        if (href.endsWith("/")) href = href.slice(0, -1);
                        return href;
                    } catch { return ""; }
                };
                const isDetail = (h) => /\/product\/used-/i.test(h || "");
                const anchors = Array.from(document.querySelectorAll("a[href*='/product/used-']"));
                const allDiscs = Array.from(document.querySelectorAll(".payments-disclaimer-container"));
                const seen = new Set();
                const out = [];

                const disclaimerForAnchor = (a) => {
                    // up to 8 ancestors search
                    let up = a, steps = 0;
                    while (up && steps < 8) {
                        const hit = up.querySelector && up.querySelector(".payments-disclaimer-container");
                        if (hit && hit.textContent) {
                            const txt = hit.textContent.replace(/\s+/g, " ").trim();
                            if (txt) return txt;
                        }
                        up = up.parentElement; steps++;
                    }
                    // fallback: nearest-ish by DOM depth
                    let best = "", bestDist = 1e9;
                    for (const d of allDiscs) {
                        if (!d || !d.textContent) continue;
                        let p = d, depth = 0;
                        while (p && depth < 40) { p = p.parentElement; depth++; }
                        const txt = d.textContent.replace(/\s+/g, " ").trim();
                        if (txt && depth < bestDist) { best = txt; bestDist = depth; }
                    }
                    return best;
                };

                for (const a of anchors) {
                    const url = norm(a.href);
                    if (!url || !isDetail(url) || seen.has(url)) continue;
                    seen.add(url);
                    const disclaimer = disclaimerForAnchor(a);
                    out.push({ url, disclaimer });
                }
                return out;
            }"""
        )

        for r in records or []:
            u = strip_fragment(r.get("url", ""))
            if u:
                disc_map[u] = (r.get("disclaimer") or "").replace("\u00a0", " ").strip()
    finally:
        await page.close()
    return disc_map


async def fetch_disclaimers_across_pages(listing_url: str, max_pages: int = 12) -> dict:
    """
    Visit listing_url (&page=N for N>1) and aggregate {detail_url -> disclaimer}.
    """
    out = {}
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36")
        )
        try:
            for page in range(1, max_pages + 1):
                url = listing_url if page == 1 else (
                    f"{listing_url}&page={page}" if "page=" not in listing_url else listing_url.replace(re.search(r'(?:[?&])page=\d+', listing_url).group(0), f'?page={page}') if re.search(r'(?:[?&])page=\d+', listing_url) else listing_url
                )
                disc = await fetch_disclaimers_on_page(context, url)
                out.update(disc)
                if page > 1 and not disc:
                    break
        finally:
            await browser.close()
    return out


# --------------------------
# Playwright collection of detail URLs (replaces Selenium)
# --------------------------
def _filter_detail_urls(urls):
    cleaned = set()
    for u in urls:
        u = strip_fragment(u)
        if any(x in u for x in [' "', "'"]):
            continue
        parsed = urlparse(u)
        if "parrisrv.com" not in parsed.netloc or "/product/" not in parsed.path:
            continue
        if re.search(r"/product/[^/]+/used/?$", parsed.path, re.I):
            continue
        cleaned.add(u)
    return cleaned

async def collect_detail_urls_with_playwright(index_url: str, max_pages: int = 12):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36")
        )
        try:
            all_urls = set()
            for page_num in range(1, max_pages + 1):
                if "page=" in index_url:
                    # Replace existing page param with current
                    url = re.sub(r"([?&])page=\d+", rf"\g<1>page={page_num}", index_url)
                else:
                    url = index_url if page_num == 1 else f"{index_url}&page={page_num}"

                page = await context.new_page()
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    await autoscroll_until_stable(page)
                    # collect product anchors and explicit "View Details" CTAs
                    urls = await page.eval_on_selector_all(
                        "a[href*='/product/']",
                        "els => els.map(a => a.href)"
                    )
                    ctas = await page.eval_on_selector_all(
                        "a:has-text('View Details'), button:has-text('View Details')",
                        "els => els.map(el => el.href).filter(Boolean)"
                    )
                    urls = (urls or []) + (ctas or [])
                    urls = _filter_detail_urls(urls)
                    before = len(all_urls)
                    all_urls |= urls
                    if page_num > 1 and len(all_urls) == before:
                        break
                finally:
                    await page.close()
            return sorted(all_urls)
        finally:
            await browser.close()


# --------------------------
# Fetch detail pages HTML with Playwright (prevents 403 & waits for content)
# --------------------------
async def fetch_detail_pages_html_with_playwright(detail_urls, referer: str = "") -> dict:
    results = {}
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=SESSION.headers.get("User-Agent", None),
            extra_http_headers={"Referer": referer} if referer else None,
        )

        async def route_handler(route):
            rtype = route.request.resource_type
            if rtype in ("image", "media", "font", "stylesheet"):
                return await route.abort()
            return await route.continue_()
        await context.route("**/*", route_handler)

        page = await context.new_page()

        async def ensure_loaded():
            sels = [
                "h1, h2, .product-title",
                ".price, .our-price, .sale-price, [class*='price']",
                "[id*='price']",
                "[class*='payment'], [id*='payment']",
                ".content, main, [role='main']"
            ]
            for sel in sels:
                try:
                    await page.wait_for_selector(sel, timeout=2000)
                except Exception:
                    pass
            try:
                await page.evaluate("""async () => {
                    const step = () => new Promise(r => {
                        window.scrollBy(0, Math.max(400, innerHeight * 0.9));
                        setTimeout(r, 120);
                    });
                    for (let i = 0; i < 6; i++) await step();
                    window.scrollTo(0, 0);
                }""")
            except Exception:
                pass
            try:
                await page.wait_for_load_state("networkidle", timeout=3000)
            except Exception:
                pass
            await page.wait_for_timeout(150)

        try:
            for url in detail_urls:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    await ensure_loaded()
                    results[url] = await page.content()
                except Exception:
                    results[url] = ""
        finally:
            await page.close()
            await context.close()
            await browser.close()

    return results


# --------------------------
# Network fetch + row assembly
# --------------------------
def fetch_detail_html(url, retries=2, timeout=25):
    for attempt in range(retries + 1):
        try:
            resp = SESSION.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.text
        except Exception:
            if attempt == retries:
                raise
            time.sleep(0.5 * (attempt + 1))

def process_one(u, html_text, disc_map):
    try:
        if html_text:
            row = parse_detail_html(u, html_text)
        else:
            try:
                html_text = fetch_detail_html(u)
                row = parse_detail_html(u, html_text)
            except Exception:
                row = {
                    "title": strip_used_prefix(u.split("/")[-1].replace("-", " ")),
                    "tagline": "",
                    "list_price": "",
                    "payments_from": "",
                    "image_url": "",
                }

        row["title"] = strip_used_prefix(row.get("title", ""))
        row["payments_disclaimer"] = disc_map.get(strip_fragment(u), "")
        return row
    except Exception as e:
        print(f"  ! Error on {u}: {e.__class__.__name__}: {e}")
        traceback.print_exc(limit=1)
        return {
            "title": strip_used_prefix(u.split("/")[-1].replace("-", " ")),
            "tagline": "",
            "list_price": "",
            "payments_from": "",
            "image_url": "",
            "payments_disclaimer": disc_map.get(strip_fragment(u), ""),
        }

def run_scrape(listing_url: str, max_pages: int = 12) -> pd.DataFrame:
    detail_urls = run_coro_resilient(collect_detail_urls_with_playwright(listing_url, max_pages=max_pages))
    st.write(f"Found **{len(detail_urls)}** detail URLs across pages.")

    disc_map = run_coro_resilient(fetch_disclaimers_across_pages(listing_url, max_pages=max_pages))
    st.write(f"Captured **{sum(1 for v in disc_map.values() if v)}** payment disclaimers from listing cards.")

    html_map = run_coro_resilient(fetch_detail_pages_html_with_playwright(detail_urls, referer=listing_url))

    rows = []
    for i, u in enumerate(detail_urls, start=1):
        row = process_one(u, html_map.get(u, ""), disc_map)
        rows.append(row)
        if i % 10 == 0:
            st.write(f"Processed {i}/{len(detail_urls)}")

    cols = ["title", "tagline", "list_price", "payments_from", "payments_disclaimer", "image_url"]
    for r in rows:
        for k in cols:
            r.setdefault(k, "")
    df = pd.DataFrame([{k: r.get(k, "") for k in cols} for r in rows])
    return df


# --------------------------
# Streamlit UI
# --------------------------
st.set_page_config(page_title="ParrisRV Scraper", page_icon="🧹", layout="wide")

st.title("ParrisRV Listing Scraper")
st.caption("Enter a listing URL (the page that shows the grid of units). The app will scrape detail pages and fetch per-card payment disclaimers.")

default_url = "https://www.parrisrv.com/used-rvs-for-sale?s=true&lots=1232&pagesize=72&sort=year-asc"
listing_url = st.text_input("Listing URL", value=default_url, help="Example: a 'used-rvs-for-sale' page with pagesize & sort")

col_btn, col_info = st.columns([1, 3])
with col_btn:
    go = st.button("Run scrape", type="primary")

with col_info:
    st.write("Output columns: **title**, **tagline**, **list_price**, **payments_from** (just $ amount), **payments_disclaimer**, **image_url**")

if go:
    try:
        with st.spinner("Scraping..."):
            df = run_scrape(listing_url.strip())
        st.success(f"Done! {len(df)} rows.")

        st.dataframe(df, use_container_width=True)

        csv_bytes = df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download CSV",
            data=csv_bytes,
            file_name="parrisrv_listings.csv",
            mime="text/csv",
        )
    except Exception as e:
        st.error("Something went wrong:")
        st.exception(e)
