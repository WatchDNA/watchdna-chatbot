"""
scraper.py — Scrapes WatchDNA.com and builds the knowledge base.
Pulls products, pages, blog articles, and store locator data.
"""

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import json
import os
from datetime import datetime, timezone

BASE_URL = os.environ.get("SHOPIFY_URL", "https://watchdna.com")
MAX_SITE_PAGES = 80
MAX_PRODUCT_PAGES = 10
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; WatchDNAChatbot/1.0)"}

COLLECTION_HANDLES = [
    "watches", "accessories", "all",
    "arilus", "boss", "calvin-klein", "coach", "ebel", "elka",
    "exaequo", "lacoste", "luminox", "micromilspec", "mido",
    "movado", "naga-time-co", "normalzeit", "norqain", "raymond-weil",
    "reservoir", "solar-aqua", "sovrygn", "stil-timepieces",
    "tesse-watches", "u-boat", "withings", "worden",
]

BLOG_HANDLES = [
    "press", "watch_enthusiast", "news", "articles", "blog"
]

PRIORITY_PATHS = [
    "/", "/pages/brands-dna", "/pages/our-vision", "/pages/watchmaking101",
    "/pages/watch-aficionados", "/pages/worldwatchday", "/pages/redbar",
    "/collections/watches", "/collections/accessories", "/tools/storelocator/directory",
    "/pages/media-directory", "/pages/contributors", "/pages/groups",
    "/pages/platforms", "/pages/committee", "/pages/dailyroutine",
    "/pages/1fortheplanet", "/pages/b1g1-business-for-good",
    "/pages/watchesandwonders", "/pages/windupwatchfair",
    "/pages/dubai-watch-week", "/pages/jck", "/blogs/press", "/blogs/watch_enthusiast",
]


def get_text(soup):
    for tag in soup(["script", "style", "noscript", "svg", "header", "footer", "nav"]):
        tag.decompose()
    return " ".join(soup.get_text(separator=" ").split())


def format_product(p, base_url):
    title = p.get("title", "")
    vendor = p.get("vendor", "")
    product_type = p.get("product_type", "")
    handle = p.get("handle", "")
    body = BeautifulSoup(p.get("body_html", "") or "", "html.parser").get_text()
    tags = ", ".join(p.get("tags", []))
    variants = p.get("variants", [])
    price = variants[0].get("price") if variants else "N/A"
    product_url = f"{base_url}/products/{handle}"
    content = (
        f"Product: {title}\nBrand/Vendor: {vendor}\nType: {product_type}\n"
        f"Price: ${price} CAD\nURL: {product_url}\nTags: {tags}\nDescription: {body[:300]}"
    )
    return {"url": product_url, "title": title, "content": content, "handle": handle}


def fetch_collection(base_url, handle, seen_handles):
    products = []
    page = 1
    while page <= MAX_PRODUCT_PAGES:
        try:
            if handle == "all_products":
                url = f"{base_url}/products.json?limit=250&page={page}"
            else:
                url = f"{base_url}/collections/{handle}/products.json?limit=250&page={page}"
            resp = requests.get(url, headers=HEADERS, timeout=15)
            if resp.status_code != 200:
                break
            batch = resp.json().get("products", [])
            if not batch:
                break
            new = 0
            for p in batch:
                h = p.get("handle")
                if h and h not in seen_handles:
                    seen_handles.add(h)
                    products.append(format_product(p, base_url))
                    new += 1
            if new:
                print(f"  ✓ {handle} page {page}: {new} new products")
            if len(batch) < 250:
                break
            page += 1
        except Exception as e:
            print(f"  ✗ {handle} error: {e}")
            break
    return products


def scrape_products(base_url):
    seen_handles = set()
    all_products = []
    print("\n📦 Fetching products...")
    found = fetch_collection(base_url, "all_products", seen_handles)
    all_products.extend(found)
    for handle in COLLECTION_HANDLES:
        found = fetch_collection(base_url, handle, seen_handles)
        all_products.extend(found)
    print(f"\n  ✅ Total unique products: {len(all_products)}")
    return all_products


def scrape_articles(base_url):
    """Scrape blog articles from WatchDNA."""
    articles = []
    print("\n📰 Fetching blog articles...")
    for blog_handle in BLOG_HANDLES:
        page = 1
        while page <= 5:
            try:
                url = f"{base_url}/blogs/{blog_handle}.json?limit=50&page={page}"
                resp = requests.get(url, headers=HEADERS, timeout=12)
                if resp.status_code != 200:
                    break
                data = resp.json()
                posts = data.get("articles", [])
                if not posts:
                    break
                for post in posts:
                    title = post.get("title", "")
                    handle = post.get("handle", "")
                    body = BeautifulSoup(post.get("body_html", "") or "", "html.parser").get_text()
                    published = post.get("published_at", "")
                    article_url = f"{base_url}/blogs/{blog_handle}/{handle}"
                    content = f"Article: {title}\nPublished: {published}\nURL: {article_url}\nContent: {body[:800]}"
                    articles.append({"url": article_url, "title": title, "content": content})
                print(f"  ✓ {blog_handle} page {page}: {len(posts)} articles")
                if len(posts) < 50:
                    break
                page += 1
            except Exception as e:
                print(f"  ✗ {blog_handle}: {e}")
                break
    print(f"  ✅ Total articles: {len(articles)}")
    return articles


def scrape_stores(base_url):
    """Scrape authorized dealer/store locator data."""
    print("\n🏪 Fetching store locator data...")
    stores = []
    try:
        # Try the store directory page
        url = f"{base_url}/tools/storelocator/directory"
        resp = requests.get(url, headers=HEADERS, timeout=12)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            # Try to find store data in JSON embedded in page
            scripts = soup.find_all("script")
            for script in scripts:
                if script.string and ("latitude" in script.string or "stores" in script.string.lower()):
                    try:
                        # Look for JSON data
                        text = script.string
                        start = text.find("[{")
                        end = text.rfind("}]") + 2
                        if start > -1 and end > 1:
                            store_data = json.loads(text[start:end])
                            for s in store_data:
                                stores.append({
                                    "name": s.get("name", s.get("title", "")),
                                    "address": s.get("address", s.get("address1", "")),
                                    "city": s.get("city", ""),
                                    "country": s.get("country", ""),
                                    "lat": float(s.get("latitude", s.get("lat", 0)) or 0),
                                    "lon": float(s.get("longitude", s.get("lng", s.get("lon", 0))) or 0),
                                    "brands": s.get("brands", s.get("tags", "")),
                                    "url": s.get("url", s.get("link", f"{base_url}/tools/storelocator")),
                                })
                    except:
                        pass
    except Exception as e:
        print(f"  ✗ Store scrape error: {e}")

    print(f"  ✅ Total stores: {len(stores)}")
    return stores


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
            resp = requests.get(url, headers=HEADERS, timeout=12)
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
    stores = scrape_stores(BASE_URL)
    pages = scrape_site(BASE_URL)
    all_entries = products + articles + pages
    print(f"\n✅ {len(products)} products + {len(articles)} articles + {len(pages)} pages = {len(all_entries)} total")
    print(f"   {len(stores)} stores in locator")

    with open("knowledge_base.json", "w", encoding="utf-8") as f:
        json.dump({
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "base_url": BASE_URL,
            "product_count": len(products),
            "article_count": len(articles),
            "page_count": len(pages),
            "store_count": len(stores),
            "pages": all_entries,
            "stores": stores,
        }, f, indent=2, ensure_ascii=False)

    print("Saved to knowledge_base.json ✓")


if __name__ == "__main__":
    main()






