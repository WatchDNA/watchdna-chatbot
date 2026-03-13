"""
scraper.py — WatchDNA knowledge base builder.
Uses Shopify Storefront API to fetch REAL prices per currency market.
Fetches only from /collections/watches to avoid unpublished/renamed products.
"""

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import json, os, time
from datetime import datetime, timezone

BASE_URL = os.environ.get("SHOPIFY_URL", "https://watchdna.com")
SHOP_DOMAIN = "watchdna.myshopify.com"
STOREFRONT_TOKEN = os.environ.get("SHOPIFY_STOREFRONT_TOKEN", "")
STOREFRONT_URL = f"https://{SHOP_DOMAIN}/api/2024-01/graphql.json"

MAX_SITE_PAGES = 80
BASE_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; WatchDNAChatbot/1.0)"}

MARKETS = [
    {"currency": "CAD", "symbol": "$",    "country": "CA"},
    {"currency": "USD", "symbol": "$",    "country": "US"},
    {"currency": "GBP", "symbol": "£",    "country": "GB"},
    {"currency": "CHF", "symbol": "CHF ", "country": "CH"},
    {"currency": "EUR", "symbol": "€",    "country": "FR"},
]

BLOG_HANDLES = ["watch-enthusiast", "press"]

BLOG_INFO = {
    "watch-enthusiast": {"label": "Community Article (Watch Enthusiast)", "url_handle": "watch-enthusiast"},
    "press":            {"label": "Press Release", "url_handle": "press"},
}

PRIORITY_PATHS = [
    "/", "/pages/brands-dna", "/pages/our-vision", "/pages/watchmaking",
    "/pages/watch-aficionados", "/pages/worldwatchday", "/pages/redbar",
    "/collections/watches", "/tools/storelocator/directory",
    "/pages/media-directory", "/pages/contributors", "/pages/groups",
    "/pages/platforms", "/pages/committee", "/pages/dailyroutine",
    "/pages/1fortheplanet", "/pages/b1g1-business-for-good",
    "/pages/blogs", "/pages/stories", "/pages/community-reads",
    "/blogs/press", "/blogs/watch-enthusiast",
    "/pages/watchesandwonders", "/pages/windupwatchfair",
    "/pages/dubai-watch-week", "/pages/jck",
    "/pages/canadian-watches-jewelry-show", "/pages/coutureshow",
    "/pages/ephj-the-international-trade-show-for-high-precision",
    "/pages/hongkong-fair", "/pages/timepieceshow", "/pages/time-to-watches",
    "/pages/we-love-watches-2025-participating-brands",
    "/pages/timepiece-world-awards", "/pages/the-temporis-international-awards",
    "/pages/grand-prix-horlogerie-geneve",
    "/pages/the-42nd-hong-kong-watch-clock-design-competition",
    "/pages/local-community", "/pages/faq",
    "/pages/favourite-rssfeeds", "/pages/accesories-directory",
]

# KEY CHANGE: query collection instead of all products
# This ensures only items actually published in /collections/watches are returned
COLLECTION_QUERY = """
query GetCollectionProducts($cursor: String, $country: CountryCode!) @inContext(country: $country) {
  collection(handle: "watches") {
    products(first: 50, after: $cursor) {
      pageInfo { hasNextPage endCursor }
      nodes {
        id
        title
        handle
        vendor
        productType
        tags
        availableForSale
        description(truncateAt: 300)
        priceRange {
          minVariantPrice {
            amount
            currencyCode
          }
        }
      }
    }
  }
}
"""


def storefront_fetch_all_products(market):
    """Fetch all products in /collections/watches for a market with correct local pricing."""
    if not STOREFRONT_TOKEN:
        raise RuntimeError(
            "SHOPIFY_STOREFRONT_TOKEN env var not set.\n"
            "Get it from: Shopify Admin → Apps → Develop apps → Storefront API access token\n"
            "Required permission: unauthenticated_read_product_listings"
        )

    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Storefront-Access-Token": STOREFRONT_TOKEN,
    }

    products = []
    cursor = None
    page = 1

    while True:
        variables = {"country": market["country"], "cursor": cursor}
        resp = requests.post(
            STOREFRONT_URL,
            json={"query": COLLECTION_QUERY, "variables": variables},
            headers=headers,
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()

        if "errors" in data:
            print(f"    GraphQL errors: {data['errors']}")
            break

        collection = data["data"].get("collection")
        if not collection:
            print(f"    ⚠️  collection 'watches' not found for {market['currency']}")
            break

        nodes = collection["products"]["nodes"]
        page_info = collection["products"]["pageInfo"]

        for node in nodes:
            price_info = node["priceRange"]["minVariantPrice"]
            price_num = float(price_info["amount"])
            currency = price_info["currencyCode"]
            symbol = market["symbol"]
            handle = node["handle"]
            product_url = f"{BASE_URL}/products/{handle}"

            # Skip if not available for sale in this market
            if not node.get("availableForSale", True):
                continue

            # Skip zero-price — not actually sold in this market
            if price_num == 0:
                continue

            tags = ", ".join(node.get("tags", []))
            content = (
                f"Product: {node['title']}\n"
                f"Brand/Vendor: {node['vendor']}\n"
                f"Type: {node['productType']}\n"
                f"Price: {symbol}{price_num:.2f} {currency}\n"
                f"URL: {product_url}\n"
                f"Tags: {tags}\n"
                f"Description: {node.get('description', '')}"
            )

            products.append({
                "url": product_url,
                "title": node["title"],
                "content": content,
                "handle": handle,
                "price": price_num,
                "currency": market["currency"],
            })

        print(f"    {market['currency']} page {page}: {len(nodes)} products fetched")
        page += 1

        if not page_info["hasNextPage"]:
            break
        cursor = page_info["endCursor"]
        time.sleep(0.3)

    return products


def scrape_products():
    seen_keys = set()
    all_products = []
    print("\n📦 Fetching products via Storefront API (collection: watches)...")

    for market in MARKETS:
        print(f"\n  [{market['currency']}] country={market['country']}")
        try:
            products = storefront_fetch_all_products(market)
            for p in products:
                key = f"{p['handle']}_{p['currency']}"
                if key not in seen_keys:
                    seen_keys.add(key)
                    all_products.append(p)
            print(f"  ✅ {market['currency']}: {len(products)} products")
        except Exception as e:
            print(f"  ✗ {market['currency']} failed: {e}")

    print(f"\n  ✅ Total: {len(all_products)} product-market entries")
    return all_products


def get_text(soup):
    for tag in soup(["script", "style", "noscript", "svg", "header", "footer", "nav"]):
        tag.decompose()
    return " ".join(soup.get_text(separator=" ").split())


def scrape_articles():
    articles = []
    seen_urls = set()
    print("\n📰 Fetching articles...")

    for blog_handle in BLOG_HANDLES:
        info = BLOG_INFO[blog_handle]
        blog_page_url = f"{BASE_URL}/blogs/{info['url_handle']}"
        page = 1
        while page <= 15:
            try:
                api_url = f"{BASE_URL}/blogs/{blog_handle}.json?limit=50&page={page}"
                resp = requests.get(api_url, headers=BASE_HEADERS, timeout=12)
                if resp.status_code != 200:
                    break
                posts = resp.json().get("articles", [])
                if not posts:
                    break
                for post in posts:
                    handle = post.get("handle", "")
                    if not handle:
                        continue
                    article_url = f"{BASE_URL}/blogs/{info['url_handle']}/{handle}"
                    if article_url in seen_urls:
                        continue
                    seen_urls.add(article_url)
                    body = BeautifulSoup(post.get("body_html", "") or "", "html.parser").get_text()

                    published_raw = post.get("published_at", "") or ""
                    updated_raw   = post.get("updated_at", "") or ""
                    published = published_raw[:10]
                    updated   = updated_raw[:10]
                    display_date = updated if published == updated[:10] else published

                    author = post.get("author", "") or "WatchDNA"
                    content = (
                        f"Article Type: {info['label']}\n"
                        f"Article: {post.get('title', '')}\n"
                        f"Published: {display_date}\n"
                        f"Author: {author}\n"
                        f"URL: {article_url}\n"
                        f"Blog Page: {blog_page_url}\n"
                        f"Content: {body[:600]}"
                    )
                    articles.append({
                        "url": article_url,
                        "title": post.get("title", ""),
                        "content": content,
                        "published": display_date,
                        "blog": blog_handle,
                    })
                print(f"  ✓ {blog_handle} page {page}: {len(posts)} articles")
                if len(posts) < 50:
                    break
                page += 1
            except Exception as e:
                print(f"  ✗ {blog_handle}: {e}")
                break

    # /pages/stories — only add internal blog links
    try:
        resp = requests.get(f"{BASE_URL}/pages/stories", headers=BASE_HEADERS, timeout=12)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            domain = urlparse(BASE_URL).netloc
            for a in soup.find_all("a", href=True):
                href = urljoin(BASE_URL, a["href"]).split("?")[0].split("#")[0]
                if urlparse(href).netloc == domain and "/blogs/" in href and href not in seen_urls:
                    seen_urls.add(href)
                    link_text = a.get_text(strip=True)
                    if len(link_text) > 10:
                        articles.append({
                            "url": href,
                            "title": link_text,
                            "content": f"Article Type: Stories Page Link\nArticle: {link_text}\nURL: {href}\nBlog Page: {BASE_URL}/pages/stories",
                            "published": "",
                            "blog": "stories",
                        })
            print(f"  ✓ /pages/stories: scraped additional article links")
    except Exception as e:
        print(f"  ✗ /pages/stories: {e}")

    articles.sort(key=lambda x: x.get("published", ""), reverse=True)
    print(f"  ✅ {len(articles)} articles")
    return articles


def scrape_site():
    visited = set()
    domain = urlparse(BASE_URL).netloc
    to_visit = [BASE_URL + p for p in PRIORITY_PATHS]
    pages = []
    print("\n🌐 Scraping site pages...")
    while to_visit and len(visited) < MAX_SITE_PAGES:
        url = to_visit.pop(0).split("#")[0].split("?")[0].rstrip("/") or BASE_URL
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
                href = urljoin(BASE_URL, a["href"]).split("#")[0].split("?")[0]
                if urlparse(href).netloc == domain and href not in visited and href not in to_visit:
                    to_visit.append(href)
        except Exception as e:
            print(f"  ✗ {url}: {e}")
    return pages


def main():
    print(f"WatchDNA Scraper — {datetime.now(timezone.utc).isoformat()}")
    products = scrape_products()
    articles = scrape_articles()
    pages = scrape_site()

    all_entries = products + articles + pages
    from collections import Counter
    by_market = Counter(p["currency"] for p in products)
    print(f"\n✅ {len(products)} products + {len(articles)} articles + {len(pages)} pages = {len(all_entries)} total")
    for mkt, cnt in sorted(by_market.items()):
        print(f"   {mkt}: {cnt} products")

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
