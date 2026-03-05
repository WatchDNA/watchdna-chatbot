"""
scraper.py — Scrapes WatchDNA.com and builds the knowledge base.
"""

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import json
import os
from datetime import datetime, timezone

BASE_URL = os.environ.get("SHOPIFY_URL", "https://watchdna.com")
MAX_SITE_PAGES = 80
MAX_PRODUCT_PAGES = 10  # Safety cap: 10 pages x 250 = 2500 products max
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; WatchDNAChatbot/1.0)"}

COLLECTION_HANDLES = [
    "watches", "accessories", "all",
    "arilus", "boss", "calvin-klein", "coach", "ebel", "elka",
    "exaequo", "lacoste", "luminox", "micromilspec", "mido",
    "movado", "naga-time-co", "normalzeit", "norqain", "raymond-weil",
    "reservoir", "solar-aqua", "sovrygn", "stil-timepieces",
    "tesse-watches", "u-boat", "withings", "worden",
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
        f"Product: {title}\n"
        f"Brand/Vendor: {vendor}\n"
        f"Type: {product_type}\n"
        f"Price: ${price} CAD\n"
        f"URL: {product_url}\n"
        f"Tags: {tags}\n"
        f"Description: {body[:300]}"
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

            print(f"  ✓ {handle} page {page}: {len(batch)} items, {new} new")

            # Stop if we got less than 250 — means no more pages
            if len(batch) < 250:
                break

            page += 1

        except Exception as e:
            print(f"  ✗ {handle} page {page} error: {e}")
            break

    return products


def scrape_products(base_url):
    seen_handles = set()
    all_products = []

    print("\n📦 Fetching products...")

    # Global endpoint first
    found = fetch_collection(base_url, "all_products", seen_handles)
    all_products.extend(found)
    print(f"  Global: {len(found)} products")

    # Per-brand collections
    for handle in COLLECTION_HANDLES:
        found = fetch_collection(base_url, handle, seen_handles)
        all_products.extend(found)

    print(f"\n  ✅ Total unique products: {len(all_products)}")
    return all_products


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
    pages = scrape_site(BASE_URL)
    all_entries = products + pages
    print(f"\n✅ {len(products)} products + {len(pages)} pages = {len(all_entries)} total")

    with open("knowledge_base.json", "w", encoding="utf-8") as f:
        json.dump({
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "base_url": BASE_URL,
            "product_count": len(products),
            "page_count": len(pages),
            "pages": all_entries,
        }, f, indent=2, ensure_ascii=False)

    print("Saved to knowledge_base.json ✓")


if __name__ == "__main__":
    main()







