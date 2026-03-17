"""
scraper.py — WatchDNA knowledge base builder.
Uses Shopify Storefront API to fetch REAL prices per currency market.

WHY THE OLD SCRAPER WAS BROKEN:
  /products.json ignores ?currency= and ?country= params entirely.
  It always returns CAD prices. The scraper was just re-labelling the
  same CAD prices as USD/GBP/etc., which is why links only worked in CAD
  and all other markets showed wrong prices.

HOW THIS IS FIXED:
  We use Shopify's Storefront API with a @inContext(country: XX) directive
  which returns genuine local prices for each market.

REQUIRED: Set SHOPIFY_STOREFRONT_TOKEN env var.
  Get it from: Shopify Admin → Apps → Develop apps → your app → Storefront API access token
  The token needs: unauthenticated_read_product_listings permission
"""

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import json, os, time
from datetime import datetime, timezone

BASE_URL = os.environ.get("SHOPIFY_URL", "https://watchdna.com")
SHOP_DOMAIN = "watchdna.myshopify.com"  # UPDATE if different
STOREFRONT_TOKEN = os.environ.get("SHOPIFY_STOREFRONT_TOKEN", "")
STOREFRONT_URL = f"https://{SHOP_DOMAIN}/api/2024-01/graphql.json"

MAX_SITE_PAGES = 80
BASE_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; WatchDNAChatbot/1.0)"}

# Shopify country codes that trigger each currency
MARKETS = [
    {"currency": "CAD", "symbol": "$",    "country": "CA"},
    {"currency": "USD", "symbol": "$",    "country": "US"},
    {"currency": "GBP", "symbol": "£",    "country": "GB"},
    {"currency": "CHF", "symbol": "CHF ", "country": "CH"},
    {"currency": "EUR", "symbol": "€",    "country": "FR"},
]

BLOG_HANDLES = ["watch-enthusiast", "press", "history"]

# Products that appear in collection API but are not real active listings
BLOCKED_HANDLES = {
    "1513279", "hugo-boss-admiral-watch", "master",
    "calvin-klein-multifunction-rose-gold-plated-day-25200102",
}

PRIORITY_PATHS = [
    "/", "/pages/brands-dna", "/pages/our-vision", "/pages/watchmaking",
    "/pages/watch-aficionados", "/pages/worldwatchday", "/pages/redbar",
    "/collections/watches", "/tools/storelocator/directory",
    "/pages/media-directory", "/pages/contributors", "/pages/groups",
    "/pages/platforms", "/pages/committee", "/pages/dailyroutine",
    "/pages/1fortheplanet", "/pages/b1g1-business-for-good",
    "/pages/blogs", "/pages/stories", "/pages/community-reads",
    "/blogs/press", "/blogs/watch-enthusiast", "/blogs/history",
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

# GraphQL query — @inContext(country: $country) gives real local prices
PRODUCTS_QUERY = """
query GetProducts($cursor: String, $country: CountryCode!, $handle: String!) @inContext(country: $country) {
  collection(handle: $handle) {
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
        metafields(identifiers: [
          {namespace: "custom", key: "styles"},
          {namespace: "custom", key: "color"},
          {namespace: "custom", key: "case_material"},
          {namespace: "custom", key: "water_resistance"},
          {namespace: "custom", key: "strap_material"}
        ]) {
          key
          value
        }
      }
    }
  }
}
"""


def storefront_fetch_all_products(market):
    """Fetch all products for a market using Storefront API with correct local pricing."""
    all_products = []
    for collection_handle in ["watches", "accessories"]:
        all_products.extend(_fetch_collection(market, collection_handle))
    return all_products


def _fetch_collection(market, collection_handle):
    """Fetch all products from a single collection for a market."""
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
        variables = {"country": market["country"], "cursor": cursor, "handle": collection_handle}
        resp = requests.post(
            STOREFRONT_URL,
            json={"query": PRODUCTS_QUERY, "variables": variables},
            headers=headers,
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()

        if "errors" in data:
            print(f"    GraphQL errors: {data['errors']}")
            break

        nodes = data["data"]["collection"]["products"]["nodes"]
        page_info = data["data"]["collection"]["products"]["pageInfo"]

        for node in nodes:
            price_info = node["priceRange"]["minVariantPrice"]
            price_num = float(price_info["amount"])
            currency = price_info["currencyCode"]  # Real currency from Shopify
            symbol = market["symbol"]
            handle = node["handle"]
            product_url = f"{BASE_URL}/products/{handle}"

            # Skip non-watch products
            title_lower = node["title"].lower()
            type_lower = (node["productType"] or "").lower()
            if any(kw in title_lower or kw in type_lower
                   for kw in ["box", "watch box", "storage", "packaging", "gift box"]):
                continue

            tags = ", ".join(node.get("tags", []))

            # Extract metafields into a dict
            meta = {}
            for mf in (node.get("metafields") or []):
                if mf and mf.get("key") and mf.get("value"):
                    meta[mf["key"]] = mf["value"]

            desc = node.get("description", "") or ""

            # Extract case size from description (e.g. "42mm", "42 mm", "42MM")
            import re as _re
            case_size = ""
            size_match = _re.search(r'(\d{2}(?:\.\d)?)[\s]?[Mm][Mm]', desc)
            if size_match:
                case_size = size_match.group(1) + "mm"

            # Extract movement type from description
            movement = ""
            desc_lower = desc.lower()
            if any(w in desc_lower for w in ["self-winding", "automatic movement", "automatic", " automatic"]):
                movement = "Automatic"
            elif any(w in desc_lower for w in ["quartz movement", "quartz", "solar", "battery"]):
                movement = "Quartz"
            elif "chronograph" in desc_lower:
                movement = "Chronograph"

            # Build features string
            feature_lines = []
            if case_size:                 feature_lines.append(f"Case Size: {case_size}")
            if movement:                  feature_lines.append(f"Movement: {movement}")
            if meta.get("styles"):        feature_lines.append(f"Styles: {meta['styles']}")
            if meta.get("color"):         feature_lines.append(f"Color: {meta['color']}")
            if meta.get("case_material"): feature_lines.append(f"Case Material: {meta['case_material']}")
            if meta.get("water_resistance"): feature_lines.append(f"Water Resistance: {meta['water_resistance']}")
            if meta.get("strap_material"): feature_lines.append(f"Strap Material: {meta['strap_material']}")
            features_str = "\n".join(feature_lines)

            content = (
                f"Product: {node['title']}\n"
                f"Brand/Vendor: {node['vendor']}\n"
                f"Type: {node['productType']}\n"
                f"Price: {symbol}{price_num:.2f} {currency}\n"
                f"URL: {product_url}\n"
                f"Tags: {tags}\n"
                f"Description: {node.get('description', '')}\n"
                + (f"Features:\n{features_str}" if features_str else "")
            )

            products.append({
                "url": product_url,
                "title": node["title"],
                "content": content,
                "handle": handle,
                "price": price_num,
                "currency": market["currency"],  # market label for filtering
            })

        print(f"    {market['currency']} page {page}: {len(nodes)} products fetched")
        page += 1

        if not page_info["hasNextPage"]:
            break
        cursor = page_info["endCursor"]
        time.sleep(0.3)  # be polite

    return products


def scrape_products():
    seen_keys = set()
    all_products = []
    print("\n📦 Fetching products via Storefront API (real per-market prices)...")

    for market in MARKETS:
        print(f"\n  [{market['currency']}] country={market['country']}")
        try:
            products = storefront_fetch_all_products(market)
            # Deduplicate by handle+currency
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

    BLOG_INFO = {
        "watch-enthusiast": {"label": "Community Article (Watch Enthusiast)", "url_handle": "watch-enthusiast"},
        "press":            {"label": "Press Release", "url_handle": "press"},
        "history":          {"label": "Brand History", "url_handle": "history"},
    }

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
                    if not node.get("availableForSale", True):
                        continue
                    if handle in BLOCKED_HANDLES:
                        continue
                    if price_num == 0:
                        continue
                    article_url = f"{BASE_URL}/blogs/{info['url_handle']}/{handle}"
                    if article_url in seen_urls:
                        continue
                    seen_urls.add(article_url)
                    body = BeautifulSoup(post.get("body_html", "") or "", "html.parser").get_text()

                    # Fetch real date from article HTML meta tag
                    display_date = ""
                    try:
                        art_resp = requests.get(article_url, headers=BASE_HEADERS, timeout=10)
                        if art_resp.status_code == 200:
                            art_soup = BeautifulSoup(art_resp.text, "html.parser")
                            meta = art_soup.find("meta", {"property": "article:published_time"})
                            if meta and meta.get("content"):
                                display_date = meta["content"][:10]
                    except Exception:
                        pass
                    if not display_date:
                        display_date = (post.get("updated_at") or post.get("published_at") or "")[:10]

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

    # Also scrape /pages/stories which links to external & community articles
    try:
        resp = requests.get(f"{BASE_URL}/pages/stories", headers=BASE_HEADERS, timeout=12)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            domain = urlparse(BASE_URL).netloc
            for a in soup.find_all("a", href=True):
                href = urljoin(BASE_URL, a["href"]).split("?")[0].split("#")[0]
                # Only include blog article links from the same domain
                if urlparse(href).netloc == domain and "/blogs/" in href and href not in seen_urls:
                    seen_urls.add(href)
                    link_text = a.get_text(strip=True)
                    if len(link_text) > 10:  # skip nav links
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


def scrape_brand_pages() -> list:
    """Fetch all brand history pages by discovering slugs from the Shopify sitemap."""
    brand_pages = []
    seen = set()
    print("\n🏷️  Fetching brand history pages via sitemap...")

    # Shopify exposes a sitemap at /sitemap.xml with child sitemaps per blog
    sitemap_urls = []
    try:
        resp = requests.get(f"{BASE_URL}/sitemap.xml", headers=BASE_HEADERS, timeout=12)
        if resp.status_code == 200:
            # Find blog sitemap entries
            for line in resp.text.split("\n"):
                if "sitemap_blogs" in line or "blogs" in line:
                    import re as _re
                    urls = _re.findall(r'<loc>(https?://[^<]+)</loc>', line)
                    sitemap_urls.extend(urls)
            # Also try direct blog sitemap
            sitemap_urls.append(f"{BASE_URL}/blogs/history/sitemap.xml")
    except Exception as e:
        print(f"  Sitemap error: {e}")

    # Try the blog sitemap directly
    history_slugs = set()
    for sitemap_url in sitemap_urls:
        try:
            resp = requests.get(sitemap_url, headers=BASE_HEADERS, timeout=12)
            if resp.status_code == 200:
                import re as _re
                for url in _re.findall(r'<loc>(https?://[^<]+)</loc>', resp.text):
                    if "/blogs/history/" in url:
                        history_slugs.add(url.split("?")[0])
        except Exception:
            pass

    # Also try the article API with different handles that Shopify sometimes uses
    for api_handle in ["history", "brand-history", "brands"]:
        for pg in range(1, 10):
            try:
                api_url = f"{BASE_URL}/blogs/{api_handle}.json?limit=50&page={pg}"
                resp = requests.get(api_url, headers=BASE_HEADERS, timeout=10)
                if resp.status_code != 200:
                    break
                posts = resp.json().get("articles", [])
                if not posts:
                    break
                for post in posts:
                    handle = post.get("handle","")
                    if handle:
                        history_slugs.add(f"{BASE_URL}/blogs/history/{handle}")
                if len(posts) < 50:
                    break
            except Exception:
                break

    print(f"  Found {len(history_slugs)} brand URLs to fetch")

    for brand_url in sorted(history_slugs):
        if brand_url in seen:
            continue
        seen.add(brand_url)
        try:
            page_resp = requests.get(brand_url, headers=BASE_HEADERS, timeout=12)
            if page_resp.status_code != 200:
                continue
            soup = BeautifulSoup(page_resp.text, "html.parser")
            for tag in soup.find_all(["nav", "header", "footer", "script", "style"]):
                tag.decompose()
            lines = [l for l in soup.get_text(separator="\n", strip=True).split("\n") if l.strip()]
            clean = "\n".join(lines)
            title = soup.title.string.strip() if soup.title else brand_url
            title = title.replace(" – WatchDNA","").replace(" - WatchDNA","").strip()
            brand_pages.append({
                "url": brand_url,
                "title": title,
                "content": clean[:5000],
            })
            print(f"  ✓ {title}")
        except Exception as e:
            print(f"  ✗ {brand_url}: {e}")

    print(f"  ✅ {len(brand_pages)} brand pages scraped")
    return brand_pages


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
            # Give brands-dna a much higher limit since it has lots of brand entries
            limit = 20000 if "brands-dna" in url else 3500
            if len(text) > 150:
                pages.append({"url": url, "title": title, "content": text[:limit]})
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
    brand_pages = scrape_brand_pages()
    pages = scrape_site()
    # Merge brand pages — override any existing /blogs/history/ entries from site crawl
    existing_urls = {p["url"] for p in pages}
    for bp in brand_pages:
        if bp["url"] not in existing_urls:
            pages.append(bp)

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
