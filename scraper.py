"""
scraper.py — Scrapes WatchDNA.com and builds the knowledge base.
Pulls products, pages, blog articles, and store directory data.
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

BLOG_HANDLES = ["press", "watch_enthusiast"]

# Country slugs for store directory scraping
COUNTRY_SLUGS = [
    "australia", "austria", "belgium", "canada", "denmark", "finland",
    "france", "germany", "hong-kong-sar", "ireland", "italy", "japan",
    "luxembourg", "netherlands", "new-zealand", "norway", "singapore",
    "spain", "sweden", "switzerland", "united-arab-emirates",
    "united-kingdom", "united-states"
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
                print(f"  ✓ {handle} p{page}: {new} new")
            if len(batch) < 250:
                break
            page += 1
        except Exception as e:
            print(f"  ✗ {handle}: {e}")
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
    print(f"  ✅ {len(all_products)} unique products")
    return all_products


def scrape_articles(base_url):
    """Scrape real blog articles with verified URLs."""
    articles = []
    seen_urls = set()
    print("\n📰 Fetching articles...")

    for blog_handle in BLOG_HANDLES:
        page = 1
        while page <= 10:
            try:
                url = f"{base_url}/blogs/{blog_handle}.json?limit=50&page={page}"
                resp = requests.get(url, headers=HEADERS, timeout=12)
                if resp.status_code != 200:
                    break
                posts = resp.json().get("articles", [])
                if not posts:
                    break
                for post in posts:
                    title = post.get("title", "")
                    handle = post.get("handle", "")
                    if not handle:
                        continue
                    article_url = f"{base_url}/blogs/{blog_handle}/{handle}"
                    if article_url in seen_urls:
                        continue
                    seen_urls.add(article_url)
                    body = BeautifulSoup(post.get("body_html", "") or "", "html.parser").get_text()
                    published = post.get("published_at", "")[:10]  # just date
                    author = post.get("author", "")
                    content = (
                        f"Article: {title}\n"
                        f"Published: {published}\n"
                        f"Author: {author}\n"
                        f"URL: {article_url}\n"
                        f"Content: {body[:600]}"
                    )
                    articles.append({"url": article_url, "title": title, "content": content})
                print(f"  ✓ {blog_handle} page {page}: {len(posts)} articles")
                if len(posts) < 50:
                    break
                page += 1
            except Exception as e:
                print(f"  ✗ {blog_handle}: {e}")
                break

    print(f"  ✅ {len(articles)} articles")
    return articles


def scrape_store_directory(base_url):
    """Scrape actual store names, addresses from directory pages."""
    stores = []
    seen = set()
    print("\n🏪 Scraping store directory...")

    # First get all country/region links from the main directory page
    try:
        resp = requests.get(f"{base_url}/tools/storelocator/directory", headers=HEADERS, timeout=12)
        soup = BeautifulSoup(resp.text, "html.parser")
        country_links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/tools/storelocator/countries/" in href or "/tools/storelocator/regions/" in href:
                full = urljoin(base_url, href)
                if full not in country_links:
                    country_links.append(full)
        print(f"  Found {len(country_links)} country/region pages")
    except Exception as e:
        print(f"  ✗ Directory error: {e}")
        country_links = []

    # Also add country slugs directly
    for slug in COUNTRY_SLUGS:
        url = f"{base_url}/tools/storelocator/countries/{slug}"
        if url not in country_links:
            country_links.append(url)

    # Scrape each country page for store listings
    for country_url in country_links[:50]:  # cap at 50 pages
        try:
            resp = requests.get(country_url, headers=HEADERS, timeout=12)
            if resp.status_code != 200:
                continue
            soup = BeautifulSoup(resp.text, "html.parser")

            # Find store links - they follow pattern /tools/storelocator/stores/HANDLE
            store_links = []
            for a in soup.find_all("a", href=True):
                if "/tools/storelocator/stores/" in a["href"]:
                    full = urljoin(base_url, a["href"])
                    if full not in seen:
                        seen.add(full)
                        store_links.append(full)

            # Also grab any store info directly from country page
            text = get_text(soup)
            if store_links:
                print(f"  ✓ {country_url.split('/')[-1]}: {len(store_links)} stores")

            # Scrape individual store pages
            for store_url in store_links:
                try:
                    sresp = requests.get(store_url, headers=HEADERS, timeout=10)
                    if sresp.status_code != 200:
                        continue
                    ssoup = BeautifulSoup(sresp.text, "html.parser")
                    stext = get_text(ssoup)
                    stitle = ssoup.title.string.strip() if ssoup.title else store_url
                    if len(stext) > 100:
                        stores.append({
                            "url": store_url,
                            "title": stitle,
                            "content": f"Authorized Dealer: {stitle}\nURL: {store_url}\n{stext[:500]}"
                        })
                except:
                    pass

        except Exception as e:
            print(f"  ✗ {country_url}: {e}")
            continue

    print(f"  ✅ {len(stores)} stores scraped")
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
    stores = scrape_store_directory(BASE_URL)
    pages = scrape_site(BASE_URL)

    all_entries = products + articles + stores + pages
    print(f"\n✅ {len(products)} products + {len(articles)} articles + {len(stores)} stores + {len(pages)} pages = {len(all_entries)} total")

    with open("knowledge_base.json", "w", encoding="utf-8") as f:
        json.dump({
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "base_url": BASE_URL,
            "product_count": len(products),
            "article_count": len(articles),
            "store_count": len(stores),
            "page_count": len(pages),
            "pages": all_entries,
        }, f, indent=2, ensure_ascii=False)

    print("Saved to knowledge_base.json ✓")


if __name__ == "__main__":
    main()





