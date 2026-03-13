"""
scraper.py — WatchDNA knowledge base builder.
Fetches products across all 5 currency markets, blog articles, and site pages.
"""

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import json, os
from datetime import datetime, timezone

BASE_URL = os.environ.get("SHOPIFY_URL", "https://watchdna.com")
MAX_SITE_PAGES = 80
BASE_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; WatchDNAChatbot/1.0)"}

# All 5 Shopify markets
MARKETS = [
    {"currency": "CAD", "symbol": "$",   "country": "CA"},
    {"currency": "USD", "symbol": "$",   "country": "US"},
    {"currency": "GBP", "symbol": "£",   "country": "GB"},
    {"currency": "CHF", "symbol": "CHF", "country": "CH"},
    {"currency": "EUR", "symbol": "€",   "country": "FR"},
]

# Only watch/accessory collections — excludes boxes, tools, etc.
WATCH_COLLECTIONS = [
    "watches",
    "arilus", "boss", "calvin-klein", "coach", "dwiss", "ebel", "elka",
    "exaequo", "fortis", "lacoste", "luminox", "micromilspec", "mido",
    "movado", "naga-time-co", "normalzeit", "norqain", "raymond-weil",
    "reservoir", "solar-aqua", "sovrygn", "stil-timepieces",
    "tesse-watches", "u-boat", "withings", "worden",
]

BLOG_HANDLES = ["watch_enthusiast", "press"]

PRIORITY_PATHS = [
    "/", "/pages/brands-dna", "/pages/our-vision", "/pages/watchmaking",
    "/pages/watch-aficionados", "/pages/worldwatchday", "/pages/redbar",
    "/collections/watches", "/tools/storelocator/directory",
    "/pages/media-directory", "/pages/contributors", "/pages/groups",
    "/pages/platforms", "/pages/committee", "/pages/dailyroutine",
    "/pages/1fortheplanet", "/pages/b1g1-business-for-good",
    "/pages/blogs", "/pages/stories", "/pages/community-reads",
    "/blogs/press", "/blogs/watch-enthusiast",
    # Tradeshows
    "/pages/watchesandwonders", "/pages/windupwatchfair",
    "/pages/dubai-watch-week", "/pages/jck",
    "/pages/canadian-watches-jewelry-show", "/pages/coutureshow",
    "/pages/ephj-the-international-trade-show-for-high-precision",
    "/pages/hongkong-fair", "/pages/timepieceshow", "/pages/time-to-watches",
    "/pages/we-love-watches-2025-participating-brands",
    # Awards
    "/pages/timepiece-world-awards", "/pages/the-temporis-international-awards",
    "/pages/grand-prix-horlogerie-geneve",
    "/pages/the-42nd-hong-kong-watch-clock-design-competition",
    # Community & media
    "/pages/local-community", "/pages/faq",
    "/pages/favourite-rssfeeds", "/pages/accesories-directory",
]


def get_text(soup):
    for tag in soup(["script", "style", "noscript", "svg", "header", "footer", "nav"]):
        tag.decompose()
    return " ".join(soup.get_text(separator=" ").split())


def is_watch_product(p):
    """Filter out non-watch products like boxes, tools, accessories."""
    title = (p.get("title") or "").lower()
    product_type = (p.get("product_type") or "").lower()
    tags = " ".join(p.get("tags") or []).lower()
    # Exclude watch boxes, storage, straps-only, tools
    exclude_keywords = ["box", "watch box", "storage", "case box", "packaging", "gift box"]
    for kw in exclude_keywords:
        if kw in title or kw in product_type:
            return False
    return True


def format_product(p, base_url, market):
    title = p.get("title", "")
    vendor = p.get("vendor", "")
    product_type = p.get("product_type", "")
    handle = p.get("handle", "")
    body = BeautifulSoup(p.get("body_html", "") or "", "html.parser").get_text()
    tags = ", ".join(p.get("tags", []))
    variants = p.get("variants", [])
    price_str = variants[0].get("price", "0") if variants else "0"
    try:
        price_num = float(price_str)
    except:
        price_num = 0
    currency = market["currency"]
    symbol = market["symbol"]
    product_url = f"{base_url}/products/{handle}"
    content = (
        f"Product: {title}\n"
        f"Brand/Vendor: {vendor}\n"
        f"Type: {product_type}\n"
        f"Price: {symbol}{price_str} {currency}\n"
        f"URL: {product_url}\n"
        f"Tags: {tags}\n"
        f"Description: {body[:300]}"
    )
    return {
        "url": product_url,
        "title": title,
        "content": content,
        "handle": handle,
        "price": price_num,
        "currency": currency,
    }


def fetch_collection(base_url, handle, market, seen_keys):
    products = []
    page = 1
    params = {"limit": 250, "country": market["country"], "currency": market["currency"]}
    if handle == "all":
        api_url = f"{base_url}/products.json"
    else:
        api_url = f"{base_url}/collections/{handle}/products.json"

    while page <= 15:
        try:
            resp = requests.get(api_url, headers=BASE_HEADERS,
                                params={**params, "page": page}, timeout=15)
            if resp.status_code != 200:
                break
            batch = resp.json().get("products", [])
            if not batch:
                break
            new = 0
            for p in batch:
                h = p.get("handle")
                key = f"{h}_{market['currency']}"
                if h and key not in seen_keys and is_watch_product(p):
                    seen_keys.add(key)
                    products.append(format_product(p, base_url, market))
                    new += 1
            if new:
                print(f"    {market['currency']} /{handle} p{page}: {new} products")
            if len(batch) < 250:
                break
            page += 1
        except Exception as e:
            print(f"    ✗ {market['currency']} /{handle}: {e}")
            break
    return products


def scrape_products(base_url):
    seen_keys = set()
    all_products = []
    print("\n📦 Fetching products across all 5 markets...")
    for market in MARKETS:
        print(f"\n  [{market['currency']}]")
        # First hit /products.json for full catalog in this market
        found = fetch_collection(base_url, "all", market, seen_keys)
        all_products.extend(found)
        # Then brand collections for anything missed
        for handle in WATCH_COLLECTIONS:
            found = fetch_collection(base_url, handle, market, seen_keys)
            all_products.extend(found)
    print(f"\n  ✅ {len(all_products)} product-market entries")
    return all_products


def scrape_articles(base_url):
    articles = []
    seen_urls = set()
    print("\n📰 Fetching articles...")

    BLOG_INFO = {
        "watch_enthusiast": {
            "label": "Community Article (Watch Enthusiast)",
            "url_handle": "watch-enthusiast",   # hyphen for public URL
        },
        "press": {
            "label": "Press Release",
            "url_handle": "press",
        },
    }

    for blog_handle in BLOG_HANDLES:
        info = BLOG_INFO[blog_handle]
        blog_page_url = f"{base_url}/blogs/{info['url_handle']}"
        page = 1
        while page <= 15:
            try:
                # API uses underscore handle
                api_url = f"{base_url}/blogs/{blog_handle}.json?limit=50&page={page}"
                resp = requests.get(api_url, headers=BASE_HEADERS, timeout=12)
                if resp.status_code != 200:
                    print(f"  ✗ {blog_handle} HTTP {resp.status_code}")
                    break
                posts = resp.json().get("articles", [])
                if not posts:
                    break
                for post in posts:
                    title = post.get("title", "")
                    handle = post.get("handle", "")
                    if not handle:
                        continue
                    # Public URL uses hyphen handle
                    article_url = f"{base_url}/blogs/{info['url_handle']}/{handle}"
                    if article_url in seen_urls:
                        continue
                    seen_urls.add(article_url)
                    body = BeautifulSoup(post.get("body_html", "") or "", "html.parser").get_text()
                    published = post.get("published_at", "")[:10]
                    author = post.get("author", "")
                    content = (
                        f"Article Type: {info['label']}\n"
                        f"Article: {title}\n"
                        f"Published: {published}\n"
                        f"Author: {author}\n"
                        f"URL: {article_url}\n"
                        f"Blog Page: {blog_page_url}\n"
                        f"Content: {body[:600]}"
                    )
                    articles.append({
                        "url": article_url,
                        "title": title,
                        "content": content,
                        "published": published,
                        "blog": blog_handle,
                    })
                print(f"  ✓ {blog_handle} page {page}: {len(posts)} articles")
                if len(posts) < 50:
                    break
                page += 1
            except Exception as e:
                print(f"  ✗ {blog_handle}: {e}")
                break

    articles.sort(key=lambda x: x.get("published", ""), reverse=True)
    print(f"  ✅ {len(articles)} articles (newest first)")
    return articles


def scrape_site(base_url):
    visited = set()
    domain = urlparse(base_url).netloc
    to_visit = [base_url + p for p in PRIORITY_PATHS]
    pages = []
    print("\n🌐 Scraping site pages...")
    while to_visit and len(visited) < MAX_SITE_PAGES:
        url = to_visit.pop(0).split("#")[0].split("?")[0].rstrip("/") or base_url
        if url in visited:
            continue
        visited.add(url)
        try:
            resp = requests.get(url, headers=BASE_HEADERS, timeout=12)
            if resp.status_code != 200 or "text/html" not in resp.headers.get("Content-Type", ""):
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            text = get_text(soup)
            title = soup.title.string.strip() if soup.title else url
            if len(text) > 150:
                pages.append({"url": url, "title": title, "content": text[:3500]})
                print(f"  ✓ [{len(pages)}] {title[:60]}")
            for a in soup.find_all("a", href=True):
                href = urljoin(base_url, a["href"]).split("#")[0].split("?")[0]
                if urlparse(href).netloc == domain and href not in visited and href not in to_visit:
                    to_visit.append(href)
        except Exception as e:
            print(f"  ✗ {url}: {e}")
    return pages


def main():
    print(f"WatchDNA Scraper — {datetime.now(timezone.utc).isoformat()}")
    products = scrape_products(BASE_URL)
    articles = scrape_articles(BASE_URL)
    pages = scrape_site(BASE_URL)

    all_entries = products + articles + pages
    print(f"\n✅ {len(products)} products + {len(articles)} articles + {len(pages)} pages = {len(all_entries)} total")

    with open("knowledge_base.json", "w", encoding="utf-8") as f:
        json.dump({
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "base_url": BASE_URL,
            "product_count": len(products),
            "article_count": len(articles),
            "page_count": len(pages),
            "pages": all_entries,
        }, f, indent=2, ensure_ascii=False)

    print("Saved knowledge_base.json ✓")


if __name__ == "__main__":
    main()
