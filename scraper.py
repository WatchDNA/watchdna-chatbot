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
    # Contributor pages — each has bio + their articles
    "/pages/brent-robillard", "/pages/cagdas-onen", "/pages/carol-besler",
    "/pages/colin-potts", "/pages/david-carrington", "/pages/elizabeth-ionson",
    "/pages/george-sully", "/pages/gianpaolo-mazzotta", "/pages/grigor-garabedian",
    "/pages/hakim-el-kadiri", "/pages/ian-cognito", "/pages/jacky-ho",
    "/pages/jeremy-freed", "/pages/mark-fleminger", "/pages/mikhail-gomes",
    "/pages/nabil-amdan", "/pages/phillip-plimmer", "/pages/roberta-naas",
    "/pages/sanket-patel", "/pages/sean-shapiro", "/pages/smartwatch-dick",
    "/pages/spiro-mandylor", "/pages/sevan-khidichian", "/pages/thomas-brissiaud",
    "/pages/thomas-j-sandrin", "/pages/tyler-horologyobsessed", "/pages/tyler-worden",
    "/pages/victor-justwatchestv", "/pages/victoria-townsend", "/pages/watchguyglasgow",
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
    "/pages/ourvision", "/pages/contact", "/pages/1fortheplanet", "/pages/b1g1-business-for-good",
    "/pages/committee", "/blogs/history/franceclat",
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
        description(truncateAt: 600)
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


def fetch_rss_dates() -> dict:
    """
    Fetch https://watchdna.com/pages/all-blogs-rss and parse pub dates.
    Returns a dict of {article_url: "YYYY-MM-DD"} — highest priority date source.
    Also tries the native Shopify Atom feeds as fallback.
    """
    import xml.etree.ElementTree as ET
    url_to_date = {}

    def parse_feed(text):
        try:
            root = ET.fromstring(text)
            ns = {
                "atom": "http://www.w3.org/2005/Atom",
                "dc": "http://purl.org/dc/elements/1.1/",
            }
            # RSS 2.0 — <item> with <link> and <pubDate>
            for item in root.iter("item"):
                link = item.findtext("link", "").strip()
                pub = (item.findtext("pubDate", "") or item.findtext("dc:date", "", ns)).strip()
                if link and pub:
                    try:
                        from email.utils import parsedate_to_datetime
                        dt = parsedate_to_datetime(pub)
                        url_to_date[link.split("?")[0]] = dt.strftime("%Y-%m-%d")
                    except Exception:
                        # Try ISO format
                        url_to_date[link.split("?")[0]] = pub[:10]
            # Atom — <entry> with <link href> and <published>/<updated>
            for entry in root.iter("{http://www.w3.org/2005/Atom}entry"):
                link_el = entry.find("{http://www.w3.org/2005/Atom}link")
                link = (link_el.get("href", "") if link_el is not None else "").strip()
                pub = (
                    (entry.findtext("{http://www.w3.org/2005/Atom}published") or
                     entry.findtext("{http://www.w3.org/2005/Atom}updated") or "")
                ).strip()
                if link and pub:
                    url_to_date[link.split("?")[0]] = pub[:10]
        except Exception as e:
            print(f"    RSS parse error: {e}")

    # 1. Try the custom all-blogs RSS page
    try:
        resp = requests.get(f"{BASE_URL}/pages/all-blogs-rss", headers=BASE_HEADERS, timeout=15)
        if resp.status_code == 200:
            parse_feed(resp.text)
            print(f"  📡 all-blogs-rss: {len(url_to_date)} dates")
    except Exception as e:
        print(f"  ✗ all-blogs-rss: {e}")

    # 2. Also hit native Shopify Atom feeds for watch-enthusiast and press
    for handle in ["watch-enthusiast", "press"]:
        for feed_path in [f"/blogs/{handle}.atom", f"/blogs/{handle}/feed.atom"]:
            try:
                resp = requests.get(BASE_URL + feed_path, headers=BASE_HEADERS, timeout=12)
                if resp.status_code == 200:
                    before = len(url_to_date)
                    parse_feed(resp.text)
                    print(f"  📡 {feed_path}: +{len(url_to_date) - before} dates")
                    break
            except Exception:
                pass

    return url_to_date


def fetch_rss_articles(rss_dates: dict, seen_urls: set) -> list:
    """
    Discover and scrape any articles in the RSS feed that aren't already in seen_urls.
    This catches brand-new posts before the listing page crawler finds them.
    """
    STORIES_HANDLES = {"experts_story", "opendial", "ecosystem", "brand_experiences",
                       "industry-voices", "watchmaking", "education", "jewellers_story",
                       "community", "media", "connected", "watch-enthusiast"}
    new_articles = []
    for url, pub_date in rss_dates.items():
        url = url.split("?")[0].rstrip("/")
        if url in seen_urls:
            continue
        if "/blogs/" not in url:
            continue
        parts = url.split("/blogs/")
        if len(parts) < 2:
            continue
        handle = parts[1].split("/")[0]
        if handle not in STORIES_HANDLES and handle != "press":
            continue
        # Scrape the article page
        try:
            art_date, author, body = fetch_article_detail(url)
            date = art_date or pub_date or ""
            title = url.split("/")[-1].replace("-", " ").title()
            # Try to get real title from page
            try:
                r = requests.get(url, headers=BASE_HEADERS, timeout=10)
                if r.status_code == 200:
                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(r.text, "html.parser")
                    h1 = soup.find("h1")
                    if h1:
                        title = h1.get_text(strip=True)
            except Exception:
                pass
            if handle == "press":
                label = "Press Release"
                blog_h = "press"
            else:
                label = "Community Article (Watch Enthusiast)"
                blog_h = handle
            content_str = (
                "Article Type: " + label + "\n"
                "Article: " + title + "\n"
                "Published: " + date + "\n"
                "Author: " + (author or "WatchDNA") + "\n"
                "URL: " + url + "\n"
                "Content: " + body[:600]
            )
            new_articles.append({
                "url": url,
                "title": title,
                "content": content_str,
                "published": date,
                "blog": blog_h,
            })
            seen_urls.add(url)
            print(f"  📡 RSS new article: {title[:50]} ({date})")
        except Exception as e:
            print(f"  ✗ RSS article fetch {url}: {e}")
    return new_articles


def scrape_articles():
    """
    Scrape all blog articles from watch-enthusiast and press blogs.
    Uses HTML pagination (reliable) since Shopify JSON API may be disabled on this store.
    Dates come from: RSS feed > article HTML meta > listing page text > article URL date pattern.
    """
    import re as _re
    from concurrent.futures import ThreadPoolExecutor, as_completed as _as_completed

    articles = []
    seen_urls = set()
    print("\n📰 Fetching articles...")

    # Step 1: RSS dates — highest priority source
    rss_dates = fetch_rss_dates()

    date_pattern = _re.compile(
        r'(January|February|March|April|May|June|July|August|September|October|November|December)'
        r'\s+(\d{1,2}),?\s+(20\d{2})'
    )

    BLOG_LABEL = {
        "watch-enthusiast": "Community Article (Watch Enthusiast)",
        "press": "Press Release",
    }

    def parse_date_str(s):
        """Convert 'March 6, 2026' or 'March 6 2026' to '2026-03-06'."""
        try:
            return datetime.strptime(s.replace(",", "").strip(), "%B %d %Y").strftime("%Y-%m-%d")
        except Exception:
            return ""

    def fetch_article_detail(article_url):
        """Fetch a single article page and return (date, author, body_text).
        Tries multiple date sources since not all blog handles use standard meta tags.
        """
        try:
            r = requests.get(article_url, headers=BASE_HEADERS, timeout=12)
            if r.status_code != 200:
                return "", "", ""
            soup = BeautifulSoup(r.text, "html.parser")
            date = ""

            # 1. Standard Shopify article meta
            for prop in ["article:published_time", "article:modified_time"]:
                tag = soup.find("meta", {"property": prop})
                if tag and tag.get("content"):
                    date = tag["content"][:10]
                    break

            # 2. JSON-LD structured data (many themes use this)
            if not date:
                for script in soup.find_all("script", {"type": "application/ld+json"}):
                    try:
                        import json as _json
                        data = _json.loads(script.string or "")
                        # Could be a list or dict
                        items = data if isinstance(data, list) else [data]
                        for item in items:
                            for key in ["datePublished", "dateCreated", "dateModified"]:
                                val = item.get(key, "")
                                if val and len(val) >= 10:
                                    date = val[:10]
                                    break
                            if date:
                                break
                    except Exception:
                        pass
                    if date:
                        break

            # 3. <time> element with datetime attribute
            if not date:
                for time_el in soup.find_all("time", {"datetime": True}):
                    dt = time_el.get("datetime", "")
                    if dt and len(dt) >= 10 and dt[:4].isdigit():
                        date = dt[:10]
                        break

            # 4. Visible month/day/year pattern in page text
            if not date:
                m = date_pattern.search(r.text)
                if m:
                    date = parse_date_str(m.group(0))

            # Author
            author = ""
            for attr_name, attr_val in [("name", "author"), ("property", "article:author")]:
                tag = soup.find("meta", {attr_name: attr_val})
                if tag and tag.get("content"):
                    author = tag["content"].strip()
                    break

            # Body text
            for tag in soup(["nav", "header", "footer", "script", "style"]):
                tag.decompose()
            body = " ".join(soup.get_text(separator=" ").split())[:600]
            return date, author, body
        except Exception:
            return "", "", ""

    # Step 2: Discover ALL blog handles dynamically from /pages/stories
    # then also always include watch-enthusiast and press
    discovered_handles = set(["watch-enthusiast", "press"])
    try:
        stories_resp = requests.get(f"{BASE_URL}/pages/stories", headers=BASE_HEADERS, timeout=12)
        if stories_resp.status_code == 200:
            stories_soup = BeautifulSoup(stories_resp.text, "html.parser")
            for a_tag in stories_soup.find_all("a", href=True):
                href = a_tag.get("href", "")
                m = re.match(r"/blogs/([^/]+)/", href)
                if m:
                    handle = m.group(1)
                    if handle != "history":
                        discovered_handles.add(handle)
        print(f"  🔎 Discovered {len(discovered_handles)} blog handles: {sorted(discovered_handles)}")
    except Exception as e:
        print(f"  ✗ Couldn't discover blog handles: {e}")

    # Try atom feeds for all discovered handles to get dates
    for handle in discovered_handles:
        for feed_path in [f"/blogs/{handle}.atom", f"/blogs/{handle}/feed.atom"]:
            try:
                resp = requests.get(BASE_URL + feed_path, headers=BASE_HEADERS, timeout=12)
                if resp.status_code == 200:
                    before = len(rss_dates)
                    # reuse parse_feed from fetch_rss_dates via inline parse
                    import xml.etree.ElementTree as ET
                    root = ET.fromstring(resp.text)
                    for entry in root.iter("{http://www.w3.org/2005/Atom}entry"):
                        link_el = entry.find("{http://www.w3.org/2005/Atom}link")
                        link = (link_el.get("href", "") if link_el is not None else "").strip().split("?")[0]
                        pub = (
                            entry.findtext("{http://www.w3.org/2005/Atom}published") or
                            entry.findtext("{http://www.w3.org/2005/Atom}updated") or ""
                        ).strip()
                        if link and pub:
                            rss_dates[link] = pub[:10]
                    added = len(rss_dates) - before
                    if added:
                        print(f"  📡 {handle}.atom: +{added} dates")
                    break
            except Exception:
                pass

    # Helper to scrape one blog handle's listing pages
    def scrape_blog_handle(blog_handle):
        found = []
        listing_page = 1
        while listing_page <= 30:
            try:
                list_url = f"{BASE_URL}/blogs/{blog_handle}?page={listing_page}"
                resp = requests.get(list_url, headers=BASE_HEADERS, timeout=12)
                if resp.status_code != 200:
                    break
                soup = BeautifulSoup(resp.text, "html.parser")
                page_items = []
                for a_tag in soup.find_all("a", href=True):
                    href = a_tag.get("href", "")
                    if f"/blogs/{blog_handle}/" not in href:
                        continue
                    full_url = urljoin(BASE_URL, href).split("?")[0].split("#")[0]
                    if full_url in seen_urls:
                        continue
                    nearby_date = ""
                    parent = a_tag.find_parent()
                    for _ in range(5):
                        if parent is None:
                            break
                        dm = date_pattern.search(parent.get_text(" ", strip=True))
                        if dm:
                            nearby_date = parse_date_str(dm.group(0))
                            break
                        parent = parent.find_parent()
                    title = a_tag.get_text(strip=True)
                    if len(title) < 5:
                        continue
                    seen_urls.add(full_url)
                    page_items.append((full_url, title, rss_dates.get(full_url) or nearby_date))
                if not page_items:
                    break
                found.extend(page_items)
                listing_page += 1
                time.sleep(0.2)
            except Exception as e:
                print(f"  ✗ {blog_handle} page {listing_page}: {e}")
                break
        return found

    # Step 3: Scrape all discovered handles
    # Determine label for each handle
    def handle_label(h):
        if h == "press":
            return "Press Release"
        return "Community Article (Watch Enthusiast)"

    for blog_handle in sorted(discovered_handles):
        label = handle_label(blog_handle)
        blog_page_url = f"{BASE_URL}/blogs/{blog_handle}"
        found_on_this_blog = scrape_blog_handle(blog_handle)

        if not found_on_this_blog:
            continue

        print(f"  🔍 Fetching {len(found_on_this_blog)} {blog_handle} articles for details...")

        def _fetch(item, _handle=blog_handle, _label=label, _blog_page=blog_page_url):
            url, title, known_date = item
            art_date, author, body = fetch_article_detail(url)
            final_date = rss_dates.get(url) or art_date or known_date
            if not author:
                author = "WatchDNA"
            c = (
                f"Article Type: {_label}\n"
                f"Article: {title}\n"
                f"Published: {final_date}\n"
                f"Author: {author}\n"
                f"URL: {url}\n"
                f"Blog Page: {_blog_page}\n"
                f"Content: {body}"
            )
            return {"url": url, "title": title, "content": c,
                    "published": final_date, "blog": _handle}

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(_fetch, item) for item in found_on_this_blog]
            for future in _as_completed(futures):
                result = future.result()
                if result:
                    articles.append(result)

        print(f"  ✅ {blog_handle}: {len(found_on_this_blog)} articles scraped")

    # Step 4: Scrape /pages/stories for any extra articles not already seen
    try:
        resp = requests.get(f"{BASE_URL}/pages/stories", headers=BASE_HEADERS, timeout=12)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            domain = urlparse(BASE_URL).netloc
            extra_items = []  # (href, title)
            for a in soup.find_all("a", href=True):
                href = urljoin(BASE_URL, a["href"]).split("?")[0].split("#")[0]
                if urlparse(href).netloc != domain or "/blogs/" not in href:
                    continue
                if href in seen_urls:
                    continue
                link_text = a.get_text(strip=True)
                if len(link_text) < 10:
                    continue
                seen_urls.add(href)
                extra_items.append((href, link_text))

            # Parallel-fetch dates for all undated stories articles
            def _fetch_story(item):
                href, link_text = item
                pub_date = rss_dates.get(href, "")
                author = "WatchDNA"
                body = ""
                if not pub_date:
                    art_date, art_author, art_body = fetch_article_detail(href)
                    pub_date = art_date
                    if art_author:
                        author = art_author
                    body = art_body
                # Determine blog handle from URL
                # Skip bare listing pages like /blogs/press or /blogs/watch-enthusiast
                path = href.replace(BASE_URL, "").rstrip("/")
                if path in ("/blogs/press", "/blogs/watch-enthusiast", "/blogs/watch_enthusiast"):
                    return None  # skip listing pages
                STORIES_HANDLES = {"experts_story", "opendial", "ecosystem", "brand_experiences",
                                   "industry-voices", "watchmaking", "education", "jewellers_story",
                                   "community", "media", "connected"}
                url_handle = href.split("/blogs/")[1].split("/")[0] if "/blogs/" in href else ""
                if "/blogs/press" in href:
                    blog_h = "press"
                    label = "Press Release"
                elif url_handle in STORIES_HANDLES:
                    blog_h = url_handle  # keep original handle so filtering works
                    label = "Community Article (Watch Enthusiast)"
                else:
                    blog_h = "watch-enthusiast"
                    label = "Community Article (Watch Enthusiast)"
                return {
                    "url": href,
                    "title": link_text,
                    "content": (
                        f"Article Type: {label}\n"
                        f"Article: {link_text}\n"
                        f"Published: {pub_date}\n"
                        f"Author: {author}\n"
                        f"URL: {href}\n"
                        f"Blog Page: {BASE_URL}/pages/stories"
                    ),
                    "published": pub_date,
                    "blog": blog_h,
                }

            print(f"  🔍 Fetching dates for {len(extra_items)} /pages/stories articles...")
            with ThreadPoolExecutor(max_workers=15) as executor:
                futures = [executor.submit(_fetch_story, item) for item in extra_items]
                for future in _as_completed(futures):
                    result = future.result()
                    if result:
                        articles.append(result)

            dated_extras = sum(1 for a in articles if a.get("published") and a.get("blog") in ("watch-enthusiast","press") and "/pages/stories" in a.get("content",""))
            print(f"  ✓ /pages/stories: {len(extra_items)} articles added")
    except Exception as e:
        print(f"  ✗ /pages/stories: {e}")

    # Also pull any brand-new articles from RSS that listing pages haven't caught yet
    rss_article_extras = fetch_rss_articles(rss_dates, seen_urls)
    if rss_article_extras:
        articles.extend(rss_article_extras)
        print(f"  📡 +{len(rss_article_extras)} new articles from RSS")

    articles.sort(key=lambda x: x.get("published", ""), reverse=True)
    print(f"  ✅ {len(articles)} total articles")
    return articles


def scrape_brand_pages() -> list:
    """Fetch all brand history pages using hardcoded slug list from brands-dna."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # Complete list of all brand slugs from watchdna.com/pages/brands-dna
    BRAND_URLS = [
        "https://watchdna.com/blogs/history/5280watch_company",
        "https://watchdna.com/blogs/history/a-lange-sohne",
        "https://watchdna.com/blogs/history/abingdon-co",
        "https://watchdna.com/blogs/history/abordage-horlogerie",
        "https://watchdna.com/blogs/history/accutron",
        "https://watchdna.com/blogs/history/adidas",
        "https://watchdna.com/blogs/history/adriatica",
        "https://watchdna.com/blogs/history/aerowatch",
        "https://watchdna.com/blogs/history/agelocer",
        "https://watchdna.com/blogs/history/aigi",
        "https://watchdna.com/blogs/history/alexander-shorokhoff",
        "https://watchdna.com/blogs/history/alpina",
        "https://watchdna.com/blogs/history/alt-sym",
        "https://watchdna.com/blogs/history/angelus",
        "https://watchdna.com/blogs/history/anonimo",
        "https://watchdna.com/blogs/history/anordain",
        "https://watchdna.com/blogs/history/appella",
        "https://watchdna.com/blogs/history/apple",
        "https://watchdna.com/blogs/history/aquastar",
        "https://watchdna.com/blogs/history/arcanaut",
        "https://watchdna.com/blogs/history/ares",
        "https://watchdna.com/blogs/history/arilus",
        "https://watchdna.com/blogs/history/arken",
        "https://watchdna.com/blogs/history/armani-exchange",
        "https://watchdna.com/blogs/history/armin-strom",
        "https://watchdna.com/blogs/history/arnoldandson",
        "https://watchdna.com/blogs/history/artya-geneve",
        "https://watchdna.com/blogs/history/atelier-jalaper",
        "https://watchdna.com/blogs/history/atelier-nossedh",
        "https://watchdna.com/blogs/history/atelier-wen",
        "https://watchdna.com/blogs/history/ateliers-demonaco",
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
        "https://watchdna.com/blogs/history/baume-mercier",
        "https://watchdna.com/blogs/history/beaubleu",
        "https://watchdna.com/blogs/history/beaucroft",
        "https://watchdna.com/blogs/history/bell-ross",
        "https://watchdna.com/blogs/history/benrus",
        "https://watchdna.com/blogs/history/bering",
        "https://watchdna.com/blogs/history/berney",
        "https://watchdna.com/blogs/history/blackout-concept",
        "https://watchdna.com/blogs/history/blancpain",
        "https://watchdna.com/blogs/history/bohen",
        "https://watchdna.com/blogs/history/bomberg",
        "https://watchdna.com/blogs/history/boss",
        "https://watchdna.com/blogs/history/bouveret",
        "https://watchdna.com/blogs/history/brandmark",
        "https://watchdna.com/blogs/history/breguet",
        "https://watchdna.com/blogs/history/breitling",
        "https://watchdna.com/blogs/history/bremont",
        "https://watchdna.com/blogs/history/brew",
        "https://watchdna.com/blogs/history/briston",
        "https://watchdna.com/blogs/history/bruno-sohnle",
        "https://watchdna.com/blogs/history/bulgari",
        "https://watchdna.com/blogs/history/bulova",
        "https://watchdna.com/blogs/history/bvor",
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
        "https://watchdna.com/blogs/history/casio-computer-corp",
        "https://watchdna.com/blogs/history/certina",
        "https://watchdna.com/blogs/history/chanel",
        "https://watchdna.com/blogs/history/charlie-paris",
        "https://watchdna.com/blogs/history/charriol",
        "https://watchdna.com/blogs/history/chaumet",
        "https://watchdna.com/blogs/history/chopard",
        "https://watchdna.com/blogs/history/christiaan-vanderklaauw",
        "https://watchdna.com/blogs/history/christopher-ward",
        "https://watchdna.com/blogs/history/chronoswiss",
        "https://watchdna.com/blogs/history/cimier",
        "https://watchdna.com/blogs/history/circula",
        "https://watchdna.com/blogs/history/citizen",
        "https://watchdna.com/blogs/history/citizen-watch-co-ltd",
        "https://watchdna.com/blogs/history/claude-bernard",
        "https://watchdna.com/blogs/history/claude-meylan",
        "https://watchdna.com/blogs/history/clemence",
        "https://watchdna.com/blogs/history/cluse",
        "https://watchdna.com/blogs/history/clyda",
        "https://watchdna.com/blogs/history/coach",
        "https://watchdna.com/blogs/history/colorado",
        "https://watchdna.com/blogs/history/compagnie-financiere-richemont-sa",
        "https://watchdna.com/blogs/history/compass",
        "https://watchdna.com/blogs/history/concord",
        "https://watchdna.com/blogs/history/core-timepieces",
        "https://watchdna.com/blogs/history/corum",
        "https://watchdna.com/blogs/history/cyrus-geneve",
        "https://watchdna.com/blogs/history/czapek-cie",
        "https://watchdna.com/blogs/history/d1-milano",
        "https://watchdna.com/blogs/history/daniel-wellington",
        "https://watchdna.com/blogs/history/david-van-heim",
        "https://watchdna.com/blogs/history/delhi-watch-company",
        "https://watchdna.com/blogs/history/delma",
        "https://watchdna.com/blogs/history/depancel",
        "https://watchdna.com/blogs/history/diesel",
        "https://watchdna.com/blogs/history/dior",
        "https://watchdna.com/blogs/history/direnzo",
        "https://watchdna.com/blogs/history/diy-watch-club",
        "https://watchdna.com/blogs/history/dkny",
        "https://watchdna.com/blogs/history/doxa",
        "https://watchdna.com/blogs/history/duckworth-prestex",
        "https://watchdna.com/blogs/history/dufrane",
        "https://watchdna.com/blogs/history/dwiss",
        "https://watchdna.com/blogs/history/ebel",
        "https://watchdna.com/blogs/history/eberhard-co",
        "https://watchdna.com/blogs/history/echo-neutra",
        "https://watchdna.com/blogs/history/edox",
        "https://watchdna.com/blogs/history/electra",
        "https://watchdna.com/blogs/history/elge",
        "https://watchdna.com/blogs/history/elka",
        "https://watchdna.com/blogs/history/elliot-brown",
        "https://watchdna.com/blogs/history/emporio-armani",
        "https://watchdna.com/blogs/history/epos",
        "https://watchdna.com/blogs/history/escudo",
        "https://watchdna.com/blogs/history/eska",
        "https://watchdna.com/blogs/history/eterna",
        "https://watchdna.com/blogs/history/etien",
        "https://watchdna.com/blogs/history/exaequo",
        "https://watchdna.com/blogs/history/farer",
        "https://watchdna.com/blogs/history/farr-swit",
        "https://watchdna.com/blogs/history/fathers",
        "https://watchdna.com/blogs/history/fears",
        "https://watchdna.com/blogs/history/ferdinand-berthoud",
        "https://watchdna.com/blogs/history/ferragamo",
        "https://watchdna.com/blogs/history/ferro-company",
        "https://watchdna.com/blogs/history/festina",
        "https://watchdna.com/blogs/history/festina-group",
        "https://watchdna.com/blogs/history/feynman",
        "https://watchdna.com/blogs/history/fiori",
        "https://watchdna.com/blogs/history/flik-flak",
        "https://watchdna.com/blogs/history/fobparis",
        "https://watchdna.com/blogs/history/formex",
        "https://watchdna.com/blogs/history/fortis",
        "https://watchdna.com/blogs/history/fossil",
        "https://watchdna.com/blogs/history/fossil-group",
        "https://watchdna.com/blogs/history/franck-muller",
        "https://watchdna.com/blogs/history/franck-muller-group",
        "https://watchdna.com/blogs/history/frederique-constant",
        "https://watchdna.com/blogs/history/furla",
        "https://watchdna.com/blogs/history/furlan-marri",
        "https://watchdna.com/blogs/history/g-shock",
        "https://watchdna.com/blogs/history/gallet",
        "https://watchdna.com/blogs/history/garmin",
        "https://watchdna.com/blogs/history/gc",
        "https://watchdna.com/blogs/history/genus",
        "https://watchdna.com/blogs/history/geo",
        "https://watchdna.com/blogs/history/gerald-charles",
        "https://watchdna.com/blogs/history/gerald-genta",
        "https://watchdna.com/blogs/history/geylang-watch-co",
        "https://watchdna.com/blogs/history/girard-perregaux",
        "https://watchdna.com/blogs/history/glashutte-original",
        "https://watchdna.com/blogs/history/glock-watches",
        "https://watchdna.com/blogs/history/glycine",
        "https://watchdna.com/blogs/history/good-evil",
        "https://watchdna.com/blogs/history/google",
        "https://watchdna.com/blogs/history/graham",
        "https://watchdna.com/blogs/history/grand-seiko",
        "https://watchdna.com/blogs/history/grone",
        "https://watchdna.com/blogs/history/gronefeld",
        "https://watchdna.com/blogs/history/gruppo-gamma",
        "https://watchdna.com/blogs/history/gucci",
        "https://watchdna.com/blogs/history/guess",
        "https://watchdna.com/blogs/history/gustave-cie",
        "https://watchdna.com/blogs/history/h-moser-cie",
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
        "https://watchdna.com/blogs/history/ice-holding-group",
        "https://watchdna.com/blogs/history/ice-watch",
        "https://watchdna.com/blogs/history/imperial",
        "https://watchdna.com/blogs/history/invicta",
        "https://watchdna.com/blogs/history/invicta-watch-group",
        "https://watchdna.com/blogs/history/iron-annie",
        "https://watchdna.com/blogs/history/isotope",
        "https://watchdna.com/blogs/history/iwc",
        "https://watchdna.com/blogs/history/jack-mason",
        "https://watchdna.com/blogs/history/jacob-co",
        "https://watchdna.com/blogs/history/jacques-bianchi",
        "https://watchdna.com/blogs/history/jaeger-lecoultre",
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
        "https://watchdna.com/blogs/history/locke-king",
        "https://watchdna.com/blogs/history/locman",
        "https://watchdna.com/blogs/history/long-island-watch-company",
        "https://watchdna.com/blogs/history/longines",
        "https://watchdna.com/blogs/history/lotus",
        "https://watchdna.com/blogs/history/louis-erard",
        "https://watchdna.com/blogs/history/louis-moinet",
        "https://watchdna.com/blogs/history/louis-vuitton",
        "https://watchdna.com/blogs/history/lucky-harvey",
        "https://watchdna.com/blogs/history/luminox",
        "https://watchdna.com/blogs/history/lvmh-group",
        "https://watchdna.com/blogs/history/m-watch",
        "https://watchdna.com/blogs/history/maison-boanton",
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
        "https://watchdna.com/blogs/history/mondaine-group",
        "https://watchdna.com/blogs/history/montblanc",
        "https://watchdna.com/blogs/history/montres-etoile",
        "https://watchdna.com/blogs/history/movado",
        "https://watchdna.com/blogs/history/movado-group-inc",
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
        "https://watchdna.com/blogs/history/norqain",
        "https://watchdna.com/blogs/history/northern-star-watch",
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
        "https://watchdna.com/blogs/history/paul-hewitt",
        "https://watchdna.com/blogs/history/paulin",
        "https://watchdna.com/blogs/history/pequignet",
        "https://watchdna.com/blogs/history/philipp-plein",
        "https://watchdna.com/blogs/history/piaget",
        "https://watchdna.com/blogs/history/pierre-cardin",
        "https://watchdna.com/blogs/history/pierre-kunz",
        "https://watchdna.com/blogs/history/pierre-lannier",
        "https://watchdna.com/blogs/history/pilo-co-geneve",
        "https://watchdna.com/blogs/history/plein-sport",
        "https://watchdna.com/blogs/history/police",
        "https://watchdna.com/blogs/history/porsche-design",
        "https://watchdna.com/blogs/history/rado",
        "https://watchdna.com/blogs/history/raymond-weil",
        "https://watchdna.com/blogs/history/redwood",
        "https://watchdna.com/blogs/history/reservoir",
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
        "https://watchdna.com/blogs/history/schaefer-companions",
        "https://watchdna.com/blogs/history/seagull-1963",
        "https://watchdna.com/blogs/history/secondhour",
        "https://watchdna.com/blogs/history/seiko",
        "https://watchdna.com/blogs/history/selten",
        "https://watchdna.com/blogs/history/serica",
        "https://watchdna.com/blogs/history/shelby",
        "https://watchdna.com/blogs/history/shinola",
        "https://watchdna.com/blogs/history/sicis-jewels",
        "https://watchdna.com/blogs/history/sinn-spezialuhren",
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
        "https://watchdna.com/blogs/history/tag-heuer",
        "https://watchdna.com/blogs/history/ted-baker",
        "https://watchdna.com/blogs/history/tesse",
        "https://watchdna.com/blogs/history/thacker-merali",
        "https://watchdna.com/blogs/history/thomas-sabo",
        "https://watchdna.com/blogs/history/tiffany",
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
        "https://watchdna.com/blogs/history/tweed-co",
        "https://watchdna.com/blogs/history/typsim",
        "https://watchdna.com/blogs/history/u-boat",
        "https://watchdna.com/blogs/history/ubiq",
        "https://watchdna.com/blogs/history/ulysse-nardin",
        "https://watchdna.com/blogs/history/undone",
        "https://watchdna.com/blogs/history/union-glashutte",
        "https://watchdna.com/blogs/history/unison",
        "https://watchdna.com/blogs/history/universal",
        "https://watchdna.com/blogs/history/urwerk",
        "https://watchdna.com/blogs/history/vacheron-constantin",
        "https://watchdna.com/blogs/history/vaer",
        "https://watchdna.com/blogs/history/van-cleef-arpels",
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
    ]

    # Dynamically discover brand slugs from brands-dna page — catches new brands automatically
    dynamic_urls = set()
    try:
        resp = requests.get(f"{BASE_URL}/pages/brands-dna", headers=BASE_HEADERS, timeout=15)
        if resp.status_code == 200:
            from bs4 import BeautifulSoup as _BS
            soup = _BS(resp.text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "/blogs/history/" in href:
                    full = href if href.startswith("http") else BASE_URL + href
                    full = full.split("?")[0].rstrip("/")
                    dynamic_urls.add(full)
            print(f"  🔍 Discovered {len(dynamic_urls)} brand URLs from brands-dna page")
    except Exception as e:
        print(f"  ✗ brands-dna discovery: {e}")

    # Merge static list + dynamic discoveries (dynamic catches new brands)
    all_brand_urls = list(set(BRAND_URLS) | dynamic_urls)
    print(f"  📚 Total brand URLs (static + dynamic): {len(all_brand_urls)}")

    brand_pages = []
    print(f"\n🏷️  Fetching {len(all_brand_urls)} brand history pages (parallel)...")

    def fetch_brand(brand_url):
        try:
            resp = requests.get(brand_url, headers=BASE_HEADERS, timeout=8)
            if resp.status_code == 404:
                return None  # Brand page doesn't exist yet
            if resp.status_code != 200:
                return None
            soup = BeautifulSoup(resp.text, "html.parser")

            # Extract article links from the ARTICLES tab before stripping tags
            domain = urlparse(BASE_URL).netloc
            article_urls = []
            seen_article_urls = set()
            for a in soup.find_all("a", href=True):
                href = a["href"]
                full = urljoin(BASE_URL, href).split("?")[0].split("#")[0]
                if (urlparse(full).netloc == domain
                        and "/blogs/" in full
                        and "/blogs/history/" not in full
                        and full not in (BASE_URL + "/blogs/watch-enthusiast", BASE_URL + "/blogs/press")
                        and full not in seen_article_urls):
                    seen_article_urls.add(full)
                    article_urls.append(full)

            for tag in soup.find_all(["nav", "header", "footer", "script", "style"]):
                tag.decompose()
            lines = [l for l in soup.get_text(separator="\n", strip=True).split("\n") if l.strip()]
            clean = "\n".join(lines)
            title = soup.title.string.strip() if soup.title else brand_url
            title = title.replace(" – WatchDNA","").replace(" - WatchDNA","").strip()
            slug = brand_url.rstrip("/").split("/blogs/history/")[-1]
            return {
                "url": brand_url,
                "title": title,
                "content": clean[:5000],
                "slug": slug,
                "article_urls": article_urls,
            }
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = {executor.submit(fetch_brand, url): url for url in all_brand_urls}
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


def validate_product_urls(products: list) -> list:
    """
    Remove products whose Shopify URL returns 404.
    Uses the Admin API products.json to get the definitive list of live handles.
    Falls back to HEAD request validation if Admin API unavailable.
    """
    from collections import defaultdict

    print("\n🔍 Validating product URLs...")

    # Get all live handles via Storefront API (already authenticated)
    live_handles = set()
    try:
        headers = {
            "Content-Type": "application/json",
            "X-Shopify-Storefront-Access-Token": STOREFRONT_TOKEN,
        }
        query = """{ products(first: 250) { nodes { handle } pageInfo { hasNextPage endCursor } } }"""
        cursor = None
        while True:
            if cursor:
                q = """query($c: String!) { products(first: 250, after: $c) { nodes { handle } pageInfo { hasNextPage endCursor } } }"""
                body = {"query": q, "variables": {"c": cursor}}
            else:
                body = {"query": query}
            resp = requests.post(STOREFRONT_URL, json=body, headers=headers, timeout=20)
            data = resp.json()
            nodes = data["data"]["products"]["nodes"]
            page_info = data["data"]["products"]["pageInfo"]
            for n in nodes:
                live_handles.add(n["handle"])
            if not page_info["hasNextPage"]:
                break
            cursor = page_info["endCursor"]
        print(f"  ✅ {len(live_handles)} live handles from Storefront API")
    except Exception as e:
        print(f"  ✗ Could not fetch live handles: {e} — skipping URL validation")
        return products

    # Filter products — only keep those whose handle is still live
    before = len(products)
    valid = [p for p in products if p.get("handle","") in live_handles]
    removed = before - len(valid)
    print(f"  ✅ Removed {removed} stale/deleted products | {len(valid)} valid products remain")
    return valid


def scrape_priority_pages() -> list:
    """
    Directly scrape the most important pages that must always be in the KB.
    These are scraped individually to guarantee inclusion regardless of site crawl limits.
    """
    MUST_HAVE = [
        "/pages/committee", "/pages/ourvision", "/pages/contact",
        "/pages/watchmaking", "/pages/brands-dna", "/pages/groups",
        "/pages/redbar", "/pages/worldwatchday", "/pages/1fortheplanet",
        "/pages/b1g1-business-for-good", "/pages/faq", "/pages/contributors",
        "/pages/watch-aficionados", "/pages/our-vision",
        "/blogs/history/franceclat",
        "/pages/tradeshows",
    ]
    pages = []
    print("\n📌 Scraping priority pages...")
    for path in MUST_HAVE:
        url = BASE_URL + path
        try:
            resp = requests.get(url, headers=BASE_HEADERS, timeout=12)
            if resp.status_code == 200 and "text/html" in resp.headers.get("Content-Type", ""):
                soup = BeautifulSoup(resp.text, "html.parser")
                text = get_text(soup)
                title = soup.title.string.strip() if soup.title else url
                if len(text) > 100:
                    pages.append({"url": url, "title": title, "content": text[:5000]})
                    print(f"  ✓ {title[:60]}")
        except Exception as e:
            print(f"  ✗ {url}: {e}")
    return pages


def main():
    print(f"WatchDNA Scraper — {datetime.now(timezone.utc).isoformat()}")
    products = scrape_products()
    products = validate_product_urls(products)
    articles = scrape_articles()
    brand_pages = scrape_brand_pages()
    pages = scrape_site()
    # Always add priority pages — guarantees committee, ourvision, contact etc are in KB
    priority = scrape_priority_pages()
    existing_urls = {p["url"] for p in pages}
    for pp in priority:
        if pp["url"] not in existing_urls:
            pages.append(pp)
            existing_urls.add(pp["url"])
    # Merge brand pages
    for bp in brand_pages:
        if bp["url"] not in existing_urls:
            pages.append(bp)

    all_entries = products + articles + pages
    print(f"\n✅ {len(products)} products + {len(articles)} articles + {len(pages)} pages = {len(all_entries)} total")

    # Build brand_article_map: slug -> list of article URLs found on that brand page
    # Also build a url->published map from scraped articles for fast lookup
    article_pub_map = {a["url"]: a.get("published", "") for a in articles}
    brand_article_map = {}
    for bp in brand_pages:
        slug = bp.get("slug", "")
        raw_urls = bp.get("article_urls", [])
        if not slug or not raw_urls:
            continue
        # Attach published dates where known and deduplicate
        seen = set()
        entries = []
        for u in raw_urls:
            if u not in seen:
                seen.add(u)
                entries.append({"url": u, "published": article_pub_map.get(u, "")})
        # Sort newest first
        entries.sort(key=lambda x: x["published"], reverse=True)
        brand_article_map[slug] = entries

    with open("knowledge_base.json", "w", encoding="utf-8") as f:
        json.dump({
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "base_url": BASE_URL,
            "product_count": len(products),
            "article_count": len(articles),
            "page_count": len(pages),
            "brand_article_map": brand_article_map,
            "pages": all_entries,
        }, f, indent=2, ensure_ascii=False)

    print("Saved knowledge_base.json ✓")


if __name__ == "__main__":
    main()
