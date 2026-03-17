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

    # Parse blog listing pages to extract real article dates
    # The listing pages (watch-enthusiast, press) show: "TITLE... March 6, 2026 description"
    listing_blogs = ["watch-enthusiast", "press"]  # only the ones with clear dates
    import re as _re
    # Regex to match month date year patterns
    date_pattern = _re.compile(
        r'(January|February|March|April|May|June|July|August|September|October|November|December)'
        r'\s+(\d{1,2}),?\s+(20\d{2})'
    )
    # Build a url->date map from listing pages
    url_to_date = {}
    for blog_handle in set(listing_blogs):
        try:
            listing_url = f"{BASE_URL}/blogs/{blog_handle}"
            resp = requests.get(listing_url, headers=BASE_HEADERS, timeout=12)
            if resp.status_code != 200:
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            # Find all article links with nearby dates
            for a_tag in soup.find_all("a", href=True):
                href = a_tag.get("href","")
                if f"/blogs/{blog_handle}/" not in href:
                    continue
                full_url = f"{BASE_URL}{href}" if href.startswith("/") else href
                full_url = full_url.split("?")[0]
                # Look for a date in nearby text
                parent = a_tag.find_parent()
                for _ in range(4):
                    if parent is None:
                        break
                    text = parent.get_text(" ", strip=True)
                    date_match = date_pattern.search(text)
                    if date_match:
                        from datetime import datetime
                        try:
                            dt = datetime.strptime(date_match.group(0).replace(",",""), "%B %d %Y")
                            url_to_date[full_url] = dt.strftime("%Y-%m-%d")
                        except Exception:
                            pass
                        break
                    parent = parent.find_parent()
        except Exception as e:
            print(f"  ✗ listing page {blog_handle}: {e}")

    print(f"  📅 Found dates for {len(url_to_date)} articles from listing pages")

    # Apply dates to articles
    for article in articles:
        if not article.get("published") and article["url"] in url_to_date:
            article["published"] = url_to_date[article["url"]]
            # Update content to include the date
            if "Published:" not in article["content"]:
                article["content"] = article["content"].replace(
                    f"URL: {article['url']}",
                    f"URL: {article['url']}\nPublished: {article['published']}"
                )

    articles.sort(key=lambda x: x.get("published", ""), reverse=True)
    print(f"  ✅ {len(articles)} articles")
    return articles


def scrape_brand_pages() -> list:
    """Fetch all brand history pages using hardcoded slug list from brands-dna."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # Complete list of all brand slugs from watchdna.com/pages/brands-dna
    BRAND_URLS = [
    "https://watchdna.com/blogs/history/altsym",
    "https://watchdna.com/blogs/history/a-lange-and-sohne",
    "https://watchdna.com/blogs/history/abordage-horlogerie",
    "https://watchdna.com/blogs/history/abingdon-co",
    "https://watchdna.com/blogs/history/accutron",
    "https://watchdna.com/blogs/history/adidas",
    "https://watchdna.com/blogs/history/adriatica",
    "https://watchdna.com/blogs/history/aerowatch",
    "https://watchdna.com/blogs/history/agelocer",
    "https://watchdna.com/blogs/history/apple",
    "https://watchdna.com/blogs/history/aigi",
    "https://watchdna.com/blogs/history/alpina",
    "https://watchdna.com/blogs/history/alexander-shorokhoff",
    "https://watchdna.com/blogs/history/angelus",
    "https://watchdna.com/blogs/history/anonimo",
    "https://watchdna.com/blogs/history/anordain",
    "https://watchdna.com/blogs/history/appella",
    "https://watchdna.com/blogs/history/aquastar",
    "https://watchdna.com/blogs/history/arcanaut",
    "https://watchdna.com/blogs/history/ares",
    "https://watchdna.com/blogs/history/arilus",
    "https://watchdna.com/blogs/history/arken",
    "https://watchdna.com/blogs/history/arnold-and-son",
    "https://watchdna.com/blogs/history/armani-exchange",
    "https://watchdna.com/blogs/history/armin-strom",
    "https://watchdna.com/blogs/history/artya-geneve",
    "https://watchdna.com/blogs/history/ateliers-demonaco",
    "https://watchdna.com/blogs/history/atelier-jalaper",
    "https://watchdna.com/blogs/history/atelier-nossedh",
    "https://watchdna.com/blogs/history/atelier-wen",
    "https://watchdna.com/blogs/history/atlantic",
    "https://watchdna.com/blogs/history/audemars-piguet",
    "https://watchdna.com/blogs/history/auricoste",
    "https://watchdna.com/blogs/history/av86",
    "https://watchdna.com/blogs/history/avi-8",
    "https://watchdna.com/blogs/history/awake",
    "https://watchdna.com/blogs/history/backes-strauss",
    "https://watchdna.com/blogs/history/ball",
    "https://watchdna.com/blogs/history/balmain",
    "https://watchdna.com/blogs/history/baltic",
    "https://watchdna.com/blogs/history/bauhaus",
    "https://watchdna.com/blogs/history/baume-et-mercier",
    "https://watchdna.com/blogs/history/beaubleu",
    "https://watchdna.com/blogs/history/beaucroft",
    "https://watchdna.com/blogs/history/bell-and-ross",
    "https://watchdna.com/blogs/history/benrus",
    "https://watchdna.com/blogs/history/bering",
    "https://watchdna.com/blogs/history/berney",
    "https://watchdna.com/blogs/history/blackout-concept",
    "https://watchdna.com/blogs/history/blancpain",
    "https://watchdna.com/blogs/history/bohen",
    "https://watchdna.com/blogs/history/bomberg",
    "https://watchdna.com/blogs/history/boss",
    "https://watchdna.com/blogs/history/bouveret",
    "https://watchdna.com/blogs/history/breguet",
    "https://watchdna.com/blogs/history/breitling",
    "https://watchdna.com/blogs/history/bremont",
    "https://watchdna.com/blogs/history/brew-watch-co",
    "https://watchdna.com/blogs/history/briston",
    "https://watchdna.com/blogs/history/bruno-sohnle",
    "https://watchdna.com/blogs/history/bvlgari",
    "https://watchdna.com/blogs/history/bvor",
    "https://watchdna.com/blogs/history/bulova",
    "https://watchdna.com/blogs/history/calvin-klein",
    "https://watchdna.com/blogs/history/calypso",
    "https://watchdna.com/blogs/history/camp",
    "https://watchdna.com/blogs/history/campanola",
    "https://watchdna.com/blogs/history/candino",
    "https://watchdna.com/blogs/history/canopy",
    "https://watchdna.com/blogs/history/canuck-timepieces",
    "https://watchdna.com/blogs/history/carl-f-bucherer",
    "https://watchdna.com/blogs/history/carlingue",
    "https://watchdna.com/blogs/history/cartier",
    "https://watchdna.com/blogs/history/casio",
    "https://watchdna.com/blogs/history/certina",
    "https://watchdna.com/blogs/history/chanel",
    "https://watchdna.com/blogs/history/charlie-paris",
    "https://watchdna.com/blogs/history/charriol",
    "https://watchdna.com/blogs/history/chaumet",
    "https://watchdna.com/blogs/history/chopard",
    "https://watchdna.com/blogs/history/christiaan-van-der-klaauw",
    "https://watchdna.com/blogs/history/christopher-ward",
    "https://watchdna.com/blogs/history/chronoswiss",
    "https://watchdna.com/blogs/history/cimier",
    "https://watchdna.com/blogs/history/citizen",
    "https://watchdna.com/blogs/history/circula",
    "https://watchdna.com/blogs/history/claude-bernard",
    "https://watchdna.com/blogs/history/claude-meylan",
    "https://watchdna.com/blogs/history/clemence",
    "https://watchdna.com/blogs/history/cluse",
    "https://watchdna.com/blogs/history/clyda",
    "https://watchdna.com/blogs/history/coach",
    "https://watchdna.com/blogs/history/colorado",
    "https://watchdna.com/blogs/history/compass",
    "https://watchdna.com/blogs/history/concord",
    "https://watchdna.com/blogs/history/core-timepieces",
    "https://watchdna.com/blogs/history/corum",
    "https://watchdna.com/blogs/history/cyrus-geneve",
    "https://watchdna.com/blogs/history/czapek-and-cie",
    "https://watchdna.com/blogs/history/daniel-wellington",
    "https://watchdna.com/blogs/history/david-van-heim",
    "https://watchdna.com/blogs/history/delma",
    "https://watchdna.com/blogs/history/delhi-watch-company",
    "https://watchdna.com/blogs/history/depancel",
    "https://watchdna.com/blogs/history/diesel",
    "https://watchdna.com/blogs/history/dior",
    "https://watchdna.com/blogs/history/direnzo",
    "https://watchdna.com/blogs/history/diy-watch-club",
    "https://watchdna.com/blogs/history/dkny",
    "https://watchdna.com/blogs/history/doxa",
    "https://watchdna.com/blogs/history/duckworth-prestex",
    "https://watchdna.com/blogs/history/dufrane",
    "https://watchdna.com/blogs/history/ebel",
    "https://watchdna.com/blogs/history/eberhard-and-co",
    "https://watchdna.com/blogs/history/echo-neutra",
    "https://watchdna.com/blogs/history/edox",
    "https://watchdna.com/blogs/history/electra",
    "https://watchdna.com/blogs/history/elge",
    "https://watchdna.com/blogs/history/elliot-brown",
    "https://watchdna.com/blogs/history/elka",
    "https://watchdna.com/blogs/history/emporio-armani",
    "https://watchdna.com/blogs/history/epos",
    "https://watchdna.com/blogs/history/escudo",
    "https://watchdna.com/blogs/history/eska",
    "https://watchdna.com/blogs/history/eterna",
    "https://watchdna.com/blogs/history/etien",
    "https://watchdna.com/blogs/history/exaequo",
    "https://watchdna.com/blogs/history/farr-swit",
    "https://watchdna.com/blogs/history/farer",
    "https://watchdna.com/blogs/history/fathers",
    "https://watchdna.com/blogs/history/fears-bristol",
    "https://watchdna.com/blogs/history/ferdinand-berthoud",
    "https://watchdna.com/blogs/history/ferro-and-company",
    "https://watchdna.com/blogs/history/ferragamo",
    "https://watchdna.com/blogs/history/festina",
    "https://watchdna.com/blogs/history/feynman",
    "https://watchdna.com/blogs/history/fiori",
    "https://watchdna.com/blogs/history/flik-flak",
    "https://watchdna.com/blogs/history/fob-paris",
    "https://watchdna.com/blogs/history/formex",
    "https://watchdna.com/blogs/history/fortis",
    "https://watchdna.com/blogs/history/fossil",
    "https://watchdna.com/blogs/history/franck-muller",
    "https://watchdna.com/blogs/history/frederique-constant",
    "https://watchdna.com/blogs/history/furla",
    "https://watchdna.com/blogs/history/furlan-marri",
    "https://watchdna.com/blogs/history/g-shock",
    "https://watchdna.com/blogs/history/gallet",
    "https://watchdna.com/blogs/history/garmin",
    "https://watchdna.com/blogs/history/gc",
    "https://watchdna.com/blogs/history/genus",
    "https://watchdna.com/blogs/history/geo-shop",
    "https://watchdna.com/blogs/history/gerald-charles",
    "https://watchdna.com/blogs/history/gerald-genta",
    "https://watchdna.com/blogs/history/geylang-watch-co",
    "https://watchdna.com/blogs/history/girard-perregaux",
    "https://watchdna.com/blogs/history/glashutte-original",
    "https://watchdna.com/blogs/history/glock-watches",
    "https://watchdna.com/blogs/history/glycine",
    "https://watchdna.com/blogs/history/goodevil",
    "https://watchdna.com/blogs/history/google",
    "https://watchdna.com/blogs/history/graham",
    "https://watchdna.com/blogs/history/grand-seiko",
    "https://watchdna.com/blogs/history/grone",
    "https://watchdna.com/blogs/history/gronefeld",
    "https://watchdna.com/blogs/history/gruppo-gamma",
    "https://watchdna.com/blogs/history/gucci",
    "https://watchdna.com/blogs/history/guess",
    "https://watchdna.com/blogs/history/gustave-and-cie",
    "https://watchdna.com/blogs/history/h-moser-and-cie",
    "https://watchdna.com/blogs/history/haim",
    "https://watchdna.com/blogs/history/hamilton",
    "https://watchdna.com/blogs/history/hampden",
    "https://watchdna.com/blogs/history/hanhart",
    "https://watchdna.com/blogs/history/harry-winston",
    "https://watchdna.com/blogs/history/hautlence",
    "https://watchdna.com/blogs/history/havaan-tuvali",
    "https://watchdna.com/blogs/history/hegid",
    "https://watchdna.com/blogs/history/herbelin",
    "https://watchdna.com/blogs/history/hermes",
    "https://watchdna.com/blogs/history/heron-watches",
    "https://watchdna.com/blogs/history/hublot",
    "https://watchdna.com/blogs/history/hysek",
    "https://watchdna.com/blogs/history/hyt",
    "https://watchdna.com/blogs/history/hz-watches",
    "https://watchdna.com/blogs/history/ice-watch",
    "https://watchdna.com/blogs/history/imperial",
    "https://watchdna.com/blogs/history/invicta",
    "https://watchdna.com/blogs/history/iron-annie",
    "https://watchdna.com/blogs/history/isotope",
    "https://watchdna.com/blogs/history/iwc-schaffhausen",
    "https://watchdna.com/blogs/history/jack-mason",
    "https://watchdna.com/blogs/history/jacob-and-co",
    "https://watchdna.com/blogs/history/jaeger-lecoultre",
    "https://watchdna.com/blogs/history/jacques-bianchi",
    "https://watchdna.com/blogs/history/jaguar",
    "https://watchdna.com/blogs/history/jaipur-watch-company",
    "https://watchdna.com/blogs/history/jakob-eitan",
    "https://watchdna.com/blogs/history/jaquet-droz",
    "https://watchdna.com/blogs/history/jose-cermeno",
    "https://watchdna.com/blogs/history/jowissa",
    "https://watchdna.com/blogs/history/junghans",
    "https://watchdna.com/blogs/history/junkers",
    "https://watchdna.com/blogs/history/kate-spade",
    "https://watchdna.com/blogs/history/kelton",
    "https://watchdna.com/blogs/history/knis",
    "https://watchdna.com/blogs/history/kronaby",
    "https://watchdna.com/blogs/history/kross-studio",
    "https://watchdna.com/blogs/history/laco",
    "https://watchdna.com/blogs/history/lacoste",
    "https://watchdna.com/blogs/history/laurent-ferrier",
    "https://watchdna.com/blogs/history/lee-cooper",
    "https://watchdna.com/blogs/history/link2care",
    "https://watchdna.com/blogs/history/lip",
    "https://watchdna.com/blogs/history/locke-and-king",
    "https://watchdna.com/blogs/history/locman",
    "https://watchdna.com/blogs/history/longines",
    "https://watchdna.com/blogs/history/long-island-watch-company",
    "https://watchdna.com/blogs/history/lotus",
    "https://watchdna.com/blogs/history/louis-erard",
    "https://watchdna.com/blogs/history/louis-moinet",
    "https://watchdna.com/blogs/history/lucky-harvey",
    "https://watchdna.com/blogs/history/luminox",
    "https://watchdna.com/blogs/history/mwatch",
    "https://watchdna.com/blogs/history/maison-montignac",
    "https://watchdna.com/blogs/history/makoto",
    "https://watchdna.com/blogs/history/marathon",
    "https://watchdna.com/blogs/history/march-lab",
    "https://watchdna.com/blogs/history/marvin",
    "https://watchdna.com/blogs/history/maserati",
    "https://watchdna.com/blogs/history/mathey-tissot",
    "https://watchdna.com/blogs/history/maurice-de-mauriac",
    "https://watchdna.com/blogs/history/maurice-lacroix",
    "https://watchdna.com/blogs/history/mbandf",
    "https://watchdna.com/blogs/history/meistersinger",
    "https://watchdna.com/blogs/history/mezei-watch-company",
    "https://watchdna.com/blogs/history/michael-kors",
    "https://watchdna.com/blogs/history/michele",
    "https://watchdna.com/blogs/history/micromilspec",
    "https://watchdna.com/blogs/history/mido",
    "https://watchdna.com/blogs/history/minase",
    "https://watchdna.com/blogs/history/missoni",
    "https://watchdna.com/blogs/history/mona",
    "https://watchdna.com/blogs/history/mondaine",
    "https://watchdna.com/blogs/history/montblanc",
    "https://watchdna.com/blogs/history/montres-etoile",
    "https://watchdna.com/blogs/history/movado",
    "https://watchdna.com/blogs/history/muhle-glashutte",
    "https://watchdna.com/blogs/history/mvmt",
    "https://watchdna.com/blogs/history/naga",
    "https://watchdna.com/blogs/history/nautica",
    "https://watchdna.com/blogs/history/nepto",
    "https://watchdna.com/blogs/history/nivada-grenchen",
    "https://watchdna.com/blogs/history/noctua",
    "https://watchdna.com/blogs/history/nodus",
    "https://watchdna.com/blogs/history/nomos-glashutte",
    "https://watchdna.com/blogs/history/normalzeit",
    "https://watchdna.com/blogs/history/northern-star-watch",
    "https://watchdna.com/blogs/history/norqain",
    "https://watchdna.com/blogs/history/nubeo-watches",
    "https://watchdna.com/blogs/history/ocean-crawler",
    "https://watchdna.com/blogs/history/oceanus",
    "https://watchdna.com/blogs/history/olivia-burton",
    "https://watchdna.com/blogs/history/omega",
    "https://watchdna.com/blogs/history/oris",
    "https://watchdna.com/blogs/history/orlam",
    "https://watchdna.com/blogs/history/ovd",
    "https://watchdna.com/blogs/history/panerai",
    "https://watchdna.com/blogs/history/parmigiani-fleurier",
    "https://watchdna.com/blogs/history/patek-philippe",
    "https://watchdna.com/blogs/history/paulin",
    "https://watchdna.com/blogs/history/paul-hewitt",
    "https://watchdna.com/blogs/history/pequignet",
    "https://watchdna.com/blogs/history/philipp-plein",
    "https://watchdna.com/blogs/history/piaget",
    "https://watchdna.com/blogs/history/pierre-cardin",
    "https://watchdna.com/blogs/history/pierre-kunz",
    "https://watchdna.com/blogs/history/pierre-lannier",
    "https://watchdna.com/blogs/history/pilo-and-co-geneve",
    "https://watchdna.com/blogs/history/plein-sport",
    "https://watchdna.com/blogs/history/police",
    "https://watchdna.com/blogs/history/porsche-design",
    "https://watchdna.com/blogs/history/rado",
    "https://watchdna.com/blogs/history/raymond-weil",
    "https://watchdna.com/blogs/history/redwood",
    "https://watchdna.com/blogs/history/reservoir-watch",
    "https://watchdna.com/blogs/history/ressence",
    "https://watchdna.com/blogs/history/richard-mille",
    "https://watchdna.com/blogs/history/roamer",
    "https://watchdna.com/blogs/history/roger-dubuis",
    "https://watchdna.com/blogs/history/rolex",
    "https://watchdna.com/blogs/history/rosenbusch",
    "https://watchdna.com/blogs/history/rudis-sylva",
    "https://watchdna.com/blogs/history/ruhla",
    "https://watchdna.com/blogs/history/rze",
    "https://watchdna.com/blogs/history/samsung",
    "https://watchdna.com/blogs/history/schaefer-and-companions",
    "https://watchdna.com/blogs/history/seagull-1963",
    "https://watchdna.com/blogs/history/second-hour",
    "https://watchdna.com/blogs/history/seiko",
    "https://watchdna.com/blogs/history/selten",
    "https://watchdna.com/blogs/history/serica",
    "https://watchdna.com/blogs/history/shelby",
    "https://watchdna.com/blogs/history/shinola",
    "https://watchdna.com/blogs/history/sicis-jewels",
    "https://watchdna.com/blogs/history/sinn-spezialuhren",
    "https://watchdna.com/blogs/history/skagen",
    "https://watchdna.com/blogs/history/solar-aqua",
    "https://watchdna.com/blogs/history/solios",
    "https://watchdna.com/blogs/history/sovrygn",
    "https://watchdna.com/blogs/history/space-one",
    "https://watchdna.com/blogs/history/speake-marin",
    "https://watchdna.com/blogs/history/sphaera",
    "https://watchdna.com/blogs/history/spinnaker",
    "https://watchdna.com/blogs/history/stella",
    "https://watchdna.com/blogs/history/stil-timepieces",
    "https://watchdna.com/blogs/history/straton-watch-co",
    "https://watchdna.com/blogs/history/straum",
    "https://watchdna.com/blogs/history/studio-underd0g",
    "https://watchdna.com/blogs/history/sunrex",
    "https://watchdna.com/blogs/history/swarovski",
    "https://watchdna.com/blogs/history/swatch",
    "https://watchdna.com/blogs/history/swiss-military-hanowa",
    "https://watchdna.com/blogs/history/swiss-watch",
    "https://watchdna.com/blogs/history/sye",
    "https://watchdna.com/blogs/history/s-coifman",
    "https://watchdna.com/blogs/history/tag-heuer",
    "https://watchdna.com/blogs/history/technomarine",
    "https://watchdna.com/blogs/history/ted-baker",
    "https://watchdna.com/blogs/history/tesse",
    "https://watchdna.com/blogs/history/thacker-and-merali",
    "https://watchdna.com/blogs/history/thomas-sabo",
    "https://watchdna.com/blogs/history/tiffany-and-co",
    "https://watchdna.com/blogs/history/timeless",
    "https://watchdna.com/blogs/history/timex",
    "https://watchdna.com/blogs/history/tissot",
    "https://watchdna.com/blogs/history/titoni",
    "https://watchdna.com/blogs/history/tommy-hilfiger",
    "https://watchdna.com/blogs/history/tory-burch",
    "https://watchdna.com/blogs/history/trauffer",
    "https://watchdna.com/blogs/history/trilobe",
    "https://watchdna.com/blogs/history/tsar-bomba",
    "https://watchdna.com/blogs/history/tsikolia",
    "https://watchdna.com/blogs/history/tudor",
    "https://watchdna.com/blogs/history/tutima",
    "https://watchdna.com/blogs/history/tweedco",
    "https://watchdna.com/blogs/history/typsim",
    "https://watchdna.com/blogs/history/ubiq",
    "https://watchdna.com/blogs/history/u-boat",
    "https://watchdna.com/blogs/history/ulysse-nardin",
    "https://watchdna.com/blogs/history/undone",
    "https://watchdna.com/blogs/history/union-glashutte",
    "https://watchdna.com/blogs/history/unison",
    "https://watchdna.com/blogs/history/universal",
    "https://watchdna.com/blogs/history/urwerk",
    "https://watchdna.com/blogs/history/vacheron-constantin",
    "https://watchdna.com/blogs/history/vaer",
    "https://watchdna.com/blogs/history/van-cleef-and-arpels",
    "https://watchdna.com/blogs/history/vario",
    "https://watchdna.com/blogs/history/venezianico",
    "https://watchdna.com/blogs/history/ventura",
    "https://watchdna.com/blogs/history/verdure",
    "https://watchdna.com/blogs/history/vero",
    "https://watchdna.com/blogs/history/versace",
    "https://watchdna.com/blogs/history/victorinox",
    "https://watchdna.com/blogs/history/vieren",
    "https://watchdna.com/blogs/history/visitor",
    "https://watchdna.com/blogs/history/von-doren",
    "https://watchdna.com/blogs/history/vortic",
    "https://watchdna.com/blogs/history/vulcain",
    "https://watchdna.com/blogs/history/wancher",
    "https://watchdna.com/blogs/history/watchcraft",
    "https://watchdna.com/blogs/history/watchpeople",
    "https://watchdna.com/blogs/history/wenger",
    "https://watchdna.com/blogs/history/whitby",
    "https://watchdna.com/blogs/history/wilk-watchworks",
    "https://watchdna.com/blogs/history/wise",
    "https://watchdna.com/blogs/history/worden",
    "https://watchdna.com/blogs/history/yema",
    "https://watchdna.com/blogs/history/zenea",
    "https://watchdna.com/blogs/history/zenith",
    "https://watchdna.com/blogs/history/zeppelin",
    "https://watchdna.com/blogs/history/zodiac",
    "https://watchdna.com/blogs/history/5280-watch-company",
    ]

    brand_pages = []
    print(f"\n🏷️  Fetching {len(BRAND_URLS)} brand history pages (parallel)...")

    def fetch_brand(brand_url):
        try:
            resp = requests.get(brand_url, headers=BASE_HEADERS, timeout=8)
            if resp.status_code == 404:
                return None  # Brand page doesn't exist yet
            if resp.status_code != 200:
                return None
            soup = BeautifulSoup(resp.text, "html.parser")
            for tag in soup.find_all(["nav", "header", "footer", "script", "style"]):
                tag.decompose()
            lines = [l for l in soup.get_text(separator="\n", strip=True).split("\n") if l.strip()]
            clean = "\n".join(lines)
            title = soup.title.string.strip() if soup.title else brand_url
            title = title.replace(" – WatchDNA","").replace(" - WatchDNA","").strip()
            return {"url": brand_url, "title": title, "content": clean[:5000]}
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = {executor.submit(fetch_brand, url): url for url in BRAND_URLS}
        for future in as_completed(futures):
            result = future.result()
            if result:
                brand_pages.append(result)

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
