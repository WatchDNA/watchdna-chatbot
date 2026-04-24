from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from openai import OpenAI
import json, os, re, csv, io, urllib.request, random
from pathlib import Path

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
KNOWLEDGE_FILE = "knowledge_base.json"
STORE_BRANDS_FILE = "store_brands.csv"
GITHUB_KB_URL = "https://raw.githubusercontent.com/emmad24k/watchdna-chatbot/main/knowledge_base.json"
GITHUB_CSV_URL = "https://raw.githubusercontent.com/emmad24k/watchdna-chatbot/main/store_brands.csv"

_kb_cache = None
_brand_map_cache = None
CURRENCY_SYMBOLS = {"CAD": "$", "USD": "$", "GBP": "£", "CHF": "CHF ", "EUR": "€"}
VALID_CURRENCIES = ["CAD", "USD", "GBP", "CHF", "EUR"]


ACCESSORY_TYPES = {
    "watch winder", "watch roll", "watch case", "safe", "accessories", "accessory",
    "watch strap", "desk organizer", "desk organiser", "watch box", "legion safes",
    "watch certificate", "8 piece watch winder", "16 piece watch winder",
    "6 piece watch winder", "double watch winder", "quad watch winder",
}


PREMIUM_BRANDS = {
    "rolex", "patek philippe", "audemars piguet", "vacheron constantin",
    "a. lange & söhne", "a lange sohne", "richard mille", "fp journe",
    "jaeger-lecoultre", "jaeger lecoultre", "iwc", "iwc schaffhausen",
    "breguet", "blancpain", "glashütte original", "glashutte original",
    "chopard", "hublot", "tag heuer", "omega", "tudor", "breitling",
    "cartier", "panerai", "zenith", "longines", "rado", "tissot",
    "hamilton", "oris", "nomos", "nomos glashütte", "grand seiko",
    "mb&f", "urwerk", "h. moser", "h moser", "f.p. journe",
    "ulysse nardin", "girard perregaux", "piaget", "roger dubuis",
    "bell & ross", "bell and ross", "baume et mercier", "frederique constant",
    "frédérique constant", "alpina", "norqain", "certina", "mido",
    "raymond weil", "movado", "montblanc", "ebel", "eterna",
    "doxa", "tudor", "fortis", "sinn", "junghans", "stowa", "laco",
    "seiko", "grand seiko", "citizen", "orient"
}


def _is_accessory(page: dict) -> bool:
    content = page.get("content", "")
    # Check Type field
    for line in content.split("\n"):
        if line.startswith("Type:"):
            t = line.replace("Type:", "").strip().lower()
            if t in ACCESSORY_TYPES:
                return True
            if t == "watches":
                return False
    # Check URL — products from /collections/accessories are accessories
    url = page.get("url", "")
    if "/products/" in url:
        title = page.get("title", "").lower()
        if any(kw in title for kw in ["winder", "safe", "roll", "watch box", "watch case", "watch strap",
                                       "organizer", "organiser", "storage", "pouch"]):
            return True
    return False


def _patch_kb(kb):
    """
    Fix KB at load time — no HTTP calls, pure in-memory fixes:
    1. Sync published field → Published: line in content (fixes blank dates).
    2. Fix mislabelled blog/article-type fields for /pages/stories-scraped articles.
    3. Normalise all article URLs (strip query strings and trailing slashes).
    """
    patched = 0
    for page in kb.get("pages", []):
        url = page.get("url", "")
        if "/blogs/" not in url or "/blogs/history/" in url:
            continue

        # Normalise URL in page dict
        clean_url = url.split("?")[0].rstrip("/")
        if clean_url != url:
            page["url"] = clean_url
            url = clean_url

        content_str = page.get("content", "")
        pub = page.get("published", "")

        # Sync published field → Published: in content
        if pub:
            if re.search(r"Published:\s*(?:\n|$)", content_str):
                page["content"] = re.sub(
                    r"Published:\s*(?=\n|$)", f"Published: {pub}", content_str
                )
                patched += 1
            elif "Published:" not in content_str:
                page["content"] = content_str.replace(
                    f"URL: {url}", f"Published: {pub}\nURL: {url}", 1
                )
                patched += 1

        # Fix mislabelled blog field and article type
        if page.get("blog") in ("stories", None, ""):
            if "/blogs/press/" in url:
                page["blog"] = "press"
                page["content"] = page.get("content","").replace(
                    "Article Type: Stories Page Link", "Article Type: Press Release"
                )
            else:
                page["blog"] = "watch-enthusiast"
                page["content"] = page.get("content","").replace(
                    "Article Type: Stories Page Link",
                    "Article Type: Community Article (Watch Enthusiast)"
                )

    # Normalise brand_article_map URLs too
    bam = kb.get("brand_article_map", {})
    for slug, entries in bam.items():
        for entry in entries:
            entry["url"] = entry["url"].split("?")[0].rstrip("/")

    print(f"[KB PATCH] Synced dates/labels in {patched} articles")
    return kb


def get_knowledge_base():
    global _kb_cache
    if _kb_cache:
        return _kb_cache
    if Path(KNOWLEDGE_FILE).exists():
        try:
            with open(KNOWLEDGE_FILE) as f:
                _kb_cache = _patch_kb(json.load(f))
            print(f"KB loaded: {_kb_cache.get('product_count',0)} products")
            return _kb_cache
        except Exception as e:
            print(f"Local KB error: {e}")
    try:
        with urllib.request.urlopen(GITHUB_KB_URL, timeout=20) as r:
            _kb_cache = _patch_kb(json.loads(r.read().decode()))
        print(f"GitHub KB loaded: {_kb_cache.get('product_count',0)} products")
        return _kb_cache
    except Exception as e:
        print(f"GitHub KB error: {e}")
        return None


def get_most_expensive(currency: str):
    data = get_knowledge_base()
    if not data:
        return None
    best = None
    best_price = 0
    for page in data.get("pages", []):
        if "/products/" not in page.get("url", ""):
            continue
        if page.get("currency", "") != currency:
            continue
        if _is_accessory(page):
            continue
        if page.get("price", 0) == 0:
            continue
        price = page.get("price", 0)
        if price > best_price:
            best_price = price
            best = page
    return best


def get_brands_for_market(currency: str) -> list:
    """Return list of vendor names that have products in this currency market."""
    data = get_knowledge_base()
    if not data:
        return []
    bpm = data.get("brands_per_market", {})
    return bpm.get(currency.upper(), [])


def get_brand_history_links() -> str:
    data = get_knowledge_base()
    if not data:
        return ""
    lines = []
    for page in data.get("pages", []):
        url = page.get("url", "")
        if "/blogs/history/" not in url:
            continue
        title = page.get("title", "")
        if not title:
            for line in page.get("content", "").split("\n"):
                line = line.strip()
                if line and "WatchDNA" not in line and "Skip to content" not in line and len(line) > 2:
                    title = line
                    break
        if title:
            lines.append(f"- [{title}]({url})")
    return "\n".join(lines)


def get_brand_map():
    global _brand_map_cache
    if _brand_map_cache:
        return _brand_map_cache
    brand_map = {}

    def parse_csv(text):
        reader = csv.reader(io.StringIO(text))
        for row in reader:
            if len(row) >= 4:
                brand, url = row[0].strip(), row[3].strip()
                if brand and url.startswith("http"):
                    brand_map[brand.lower()] = {"name": brand, "url": url}
                    brand_map[brand.lower().replace("-", " ").replace("_", " ")] = {"name": brand, "url": url}

    if Path(STORE_BRANDS_FILE).exists():
        with open(STORE_BRANDS_FILE) as f:
            parse_csv(f.read())
    else:
        try:
            with urllib.request.urlopen(GITHUB_CSV_URL, timeout=10) as r:
                parse_csv(r.read().decode())
        except Exception as e:
            print(f"CSV error: {e}")
    _brand_map_cache = brand_map
    return brand_map


def find_brand_in_query(query: str):
    brand_map = get_brand_map()
    q = query.lower()
    matches = []
    for k, v in brand_map.items():
        # Word boundary match — prevents "longines" matching "long time"
        if re.search(r'(?<![a-z])' + re.escape(k) + r'(?![a-z])', q):
            matches.append((len(k), v))
    return sorted(matches, reverse=True)[0][1] if matches else None


def extract_budget(query: str):
    """
    Returns (amount, direction) where direction is "max" or "min".
    max = under/below/up to X
    min = over/above/more than X
    Returns (None, None) if no budget found.
    """
    q = query.lower().strip()

    MAX_WORDS = r"(?:under|below|less than|up to|budget|max|maximum|no more than|at most)"
    MIN_WORDS = r"(?:over|above|more than|at least|minimum|starting from|above)"

    def parse_amount(s):
        try: return float(s.replace(",", ""))
        except: return None

    # k shorthand with direction: "under 5k", "over 2k"
    k_max = re.search(MAX_WORDS + r"\s*~?\$?([\d.]+)\s*k\b", q)
    if k_max:
        v = parse_amount(k_max.group(1))
        if v: return (v * 1000, "max")

    k_min = re.search(MIN_WORDS + r"\s*~?\$?([\d.]+)\s*k\b", q)
    if k_min:
        v = parse_amount(k_min.group(1))
        if v: return (v * 1000, "min")

    # Bare "5k" with no direction = max (assume budget)
    k_bare = re.search(r"(?<![a-z])\$?([\d.]+)\s*k\b", q)
    if k_bare:
        v = parse_amount(k_bare.group(1))
        if v: return (v * 1000, "max")

    # grand: "2 grand", "over 3 grand"
    grand_min = re.search(MIN_WORDS + r"\s*(\d+)\s*grand", q)
    if grand_min:
        v = parse_amount(grand_min.group(1))
        if v: return (v * 1000, "min")
    grand_match = re.search(r"(\d+)\s*grand", q)
    if grand_match:
        v = parse_amount(grand_match.group(1))
        if v: return (v * 1000, "max")

    # Max patterns
    max_patterns = [
        r"under\s*~?\$?([\d,]+)",
        r"below\s*~?\$?([\d,]+)",
        r"less than\s*~?\$?([\d,]+)",
        r"up to\s*~?\$?([\d,]+)",
        r"\$?([\d,]+)\s*(?:or less|max|maximum)",
        r"budget\s*(?:of|is|:)?\s*~?\$?([\d,]+)",
        r"spend\s*~?\$?([\d,]+)",
    ]
    for p in max_patterns:
        m = re.search(p, q)
        if m:
            v = parse_amount(m.group(1))
            if v: return (v, "max")

    # Min patterns
    min_patterns = [
        r"over\s*~?\$?([\d,]+)",
        r"above\s*~?\$?([\d,]+)",
        r"more than\s*~?\$?([\d,]+)",
        r"at least\s*~?\$?([\d,]+)",
        r"starting\s*(?:from|at)?\s*~?\$?([\d,]+)",
        r"\$?([\d,]+)\s*(?:or more|and up|\+)",
    ]
    for p in min_patterns:
        m = re.search(p, q)
        if m:
            v = parse_amount(m.group(1))
            if v: return (v, "min")

    # Ambiguous: "$2000 CAD" etc — assume max
    m = re.search(r"\$?([\d,]+)\s*(?:cad|usd|gbp|chf|eur|dollars)", q)
    if m:
        v = parse_amount(m.group(1))
        if v: return (v, "max")

    return (None, None)


def load_knowledge(query: str = "", currency: str = "CAD") -> str:
    data = get_knowledge_base()
    if not data:
        return "Knowledge base not available."

    currency = currency.upper()
    budget_amount, budget_dir = extract_budget(query)
    keywords = [w for w in query.lower().split() if len(w) > 2]
    query_lower = query.lower()

    watches = []
    accessories = []
    articles = []
    other_pages = []

    for page in data.get("pages", []):
        url = page.get("url", "")
        is_product = "/products/" in url
        is_article = "/blogs/" in url

        if is_product:
            if page.get("currency", "") != currency:
                continue
            if page.get("price", 0) == 0:
                continue
            price = page.get("price", 0)
            if budget_amount and budget_dir == "max" and price > budget_amount:
                continue
            if budget_amount and budget_dir == "min" and price < budget_amount:
                continue
            if _is_accessory(page):
                accessories.append(page)
            else:
                watches.append(page)
        elif is_article:
            # Skip blog listing pages (e.g. /blogs/press, /blogs/watch-enthusiast) — not real articles
            url = page.get("url", "")
            path = url.replace("https://watchdna.com", "").rstrip("/")
            if path in ("/blogs/press", "/blogs/watch-enthusiast", "/blogs/watch_enthusiast"):
                other_pages.append(page)
            else:
                articles.append(page)
        else:
            other_pages.append(page)

    print(f"[LOAD_KNOWLEDGE] currency={currency} | watches={len(watches)} | accessories={len(accessories)} | articles={len(articles)}")

    # Color pre-filter — search Description: and Color: fields with word boundaries
    COLOR_KEYWORDS = ["red", "blue", "green", "black", "white", "gold", "silver", "brown",
                      "orange", "yellow", "grey", "gray", "pink", "purple", "bronze", "rose"]
    requested_colors = [c for c in COLOR_KEYWORDS if re.search(r'\b' + c + r'\b', query_lower)]

    def _has_color(page, colors):
        for line in page.get("content", "").split("\n"):
            if line.startswith("Description:") or line.startswith("Color:") or line.startswith("Product:"):
                for c in colors:
                    if re.search(r'\b' + c + r'\b', line, re.IGNORECASE):
                        return True
        return False

    if requested_colors:
        color_filtered = [w for w in watches if _has_color(w, requested_colors)]
        if color_filtered:
            watches = color_filtered

    def score(page):
        text = (page.get("title", "") + " " + page.get("content", "")).lower()
        return sum(1 for kw in keywords if kw in text)

    def is_premium(page):
        return "Premium Brands" in page.get("content", "")

    is_accessory_query = any(w in query_lower for w in [
        "winder", "safe", "roll", "watch case", "watch strap", "accessory", "accessories", "storage"
    ])
    is_article_query = any(w in query_lower for w in [
        "article", "latest article", "recent article", "watch enthusiast"
    ])
    is_blog_query = any(w in query_lower for w in [
        "blog", "story", "stories", "post", "posted"
    ]) and not is_article_query
    is_brand_query = any(w in query_lower for w in [
        "brand", "brands", "about", "history", "founded", "company", "who makes",
        "canadian", "swiss", "german", "french", "japanese", "american", "italian",
        "british", "danish", "norwegian", "greek", "georgian", "australian", "spanish",
        "country", "group", "groups", "cartier", "rolex", "omega", "seiko", "breitling",
        "hublot", "tag heuer", "patek", "tudor", "longines", "tissot", "rado",
        "hamilton", "certina", "mido", "norqain", "fortis", "luminox", "casio",
        "movado", "citizen", "bulova", "accutron", "alpina", "bering", "dwiss"
    ])

    # Brand + blog/article combo — e.g. "blogs about Rado", "latest Wolf posts"
    is_brand_blog_query = (is_blog_query or is_article_query) and is_brand_query

    if is_accessory_query:
        pool = sorted(accessories, key=score, reverse=True) + sorted(watches, key=score, reverse=True)[:10]
    elif is_brand_blog_query:
        # Build url_to_article with normalized URLs (no query string, no trailing slash)
        brand_article_map = data.get("brand_article_map", {})
        url_to_article = {
            p["url"].split("?")[0].rstrip("/"): p
            for p in articles
        }
        matched_articles = []

        # Find best matching brand slug using word-boundary matching
        # (substring matching causes "any" to match "company", "blog" to match "seablog" etc.)
        best_slug = None
        best_slug_score = 0
        for slug in brand_article_map:
            slug_text = slug.replace("-", " ").replace("_", " ")
            slug_score = sum(
                1 for w in keywords
                if re.search(r'\b' + re.escape(w) + r'\b', slug_text)
            )
            if slug_score > best_slug_score:
                best_slug_score = slug_score
                best_slug = slug

        if best_slug and best_slug_score > 0:
            for entry in brand_article_map[best_slug]:
                norm_url = entry["url"].split("?")[0].rstrip("/")
                article = url_to_article.get(norm_url)
                if article:
                    matched_articles.append(article)
            # Sort by date descending
            matched_articles.sort(key=lambda p: p.get("published", ""), reverse=True)

        # Always fall back to keyword scoring if map gave nothing
        if not matched_articles:
            matched_articles = sorted(
                [p for p in articles if score(p) > 0 and "/blogs/history/" not in p.get("url","")],
                key=lambda p: p.get("published", ""),
                reverse=True
            )

        # Include brand history page for context
        brand_history = sorted(
            [p for p in other_pages if score(p) > 0 and "/blogs/history/" in p.get("url", "")],
            key=score, reverse=True
        )
        pool = brand_history + matched_articles + other_pages
    elif is_article_query:
        # Articles = watch-enthusiast ONLY. Press releases are never articles.
        # Also exclude blog listing pages (URL is /blogs/watch-enthusiast with no slug)
        we_articles = sorted(
            [p for p in articles
             if p.get("blog","") == "watch-enthusiast"
             and p.get("title","").upper() not in ("PRESS RELEASES", "WATCH ENTHUSIAST", "WATCH ENTHUSIAST – WATCHDNA")
             and len(p.get("url","").split("/blogs/")[-1].split("/")) >= 2],
            key=lambda p: p.get("published",""), reverse=True
        )
        pool = we_articles + other_pages
    elif is_blog_query:
        # Sort blog articles newest-first — put them FIRST so AI sees latest immediately
        blog_articles_sorted = sorted(
            [p for p in articles if p.get("blog","") in ("watch-enthusiast", "stories")],
            key=lambda p: p.get("published", ""), reverse=True
        )
        pool = blog_articles_sorted + other_pages
    elif is_brand_query:
        # Put brands-dna, history pages, and groups page first
        brand_pages = [p for p in other_pages if any(x in p.get("url","") for x in
                       ["/blogs/history/", "brands-dna", "/pages/groups"])]
        rest_pages = [p for p in other_pages if p not in brand_pages]
        keyword_watches = sorted(watches, key=score, reverse=True)[:10]
        # Include brand-relevant articles sorted newest first
        brand_articles = sorted(
            [p for p in articles if score(p) > 0],
            key=lambda p: p.get("published", ""),
            reverse=True
        )
        # Always append some recent articles even if no keyword match, for related reading
        fallback_articles = sorted(
            [p for p in articles if p not in brand_articles and p.get("blog") in ("watch-enthusiast", "press")],
            key=lambda p: p.get("published", ""),
            reverse=True
        )[:5]
        pool = brand_pages + rest_pages + keyword_watches + brand_articles + fallback_articles
    else:
        def premium_score(page):
            vendor = ""
            for line in page.get("content", "").split("\n"):
                if line.startswith("Brand/Vendor:"):
                    vendor = line.replace("Brand/Vendor:", "").strip().lower()
                    break
            # Premium = tagged in KB OR vendor in hardcoded list
            return 1 if ("Premium Brands" in page.get("content","") or vendor in PREMIUM_BRANDS) else 0

        if requested_colors:
            scored = sorted(watches, key=lambda p: (score(p), premium_score(p)), reverse=True)
            pool = scored + other_pages + articles
        elif keywords:
            top = [w for w in watches if score(w) > 0]
            rest = [w for w in watches if score(w) == 0]
            top_sorted = sorted(top, key=lambda p: (premium_score(p), score(p)), reverse=True)
            random.shuffle(rest)
            pool = top_sorted + rest + other_pages + articles
        else:
            # No keywords: KB-tagged premium first, then hardcoded premium, then rest shuffled
            kb_premium   = [w for w in watches if "Premium Brands" in w.get("content","")]
            list_premium = [w for w in watches if "Premium Brands" not in w.get("content","") and premium_score(w) == 1]
            non_premium  = [w for w in watches if "Premium Brands" not in w.get("content","") and premium_score(w) == 0]
            random.shuffle(non_premium)
            pool = kb_premium + list_premium + non_premium + other_pages + articles

    context = ""
    for page in pool:
        entry = f"\n\n--- {page['url']} ---\n{page['content']}"
        if len(context) + len(entry) > 22000:
            break
        context += entry
    return context

CONTRIBUTORS = """
CONTRIBUTORS — for each person, use their individual URL listed below as the link.
Format every contributor as: [Full Name](their-url) — Role/bio

- [Brent Robillard](https://watchdna.com/pages/brent-robillard) (@Calibre321) — Watch Photography and Reviews. Writer, educator, craftsman and watch enthusiast, author of four novels.
- [Cagdas Onen](https://watchdna.com/pages/cagdas-onen) — Watch Enthusiast & Founder of The Catalyst podcast.
- [Carol Besler](https://watchdna.com/pages/carol-besler) — Journalist. Has written for Forbes, The Robb Report, Watch and Culture, Hollywood Reporter, Nuvo, Watch Time.
- [Colin Potts](https://watchdna.com/pages/colin-potts) — Horologist & Watch Enthusiast.
- [David Carrington](https://watchdna.com/pages/david-carrington) — Founder and CEO of COMPASS Timepieces.
- [Elizabeth Ionson](https://watchdna.com/pages/elizabeth-ionson) — Sales & Training Professional.
- [George Sully](https://watchdna.com/pages/george-sully) — Watch Enthusiast & Entrepreneur.
- [Gian-Paolo Mazzotta](https://watchdna.com/pages/gianpaolo-mazzotta) — Tailor, Designer, Stylist & Watch Enthusiast.
- [Grigor Garabedian](https://watchdna.com/pages/grigor-garabedian) — Head Watchmaker & Director of Service Operations, Fine Jewellery and Timepieces at Birks Group.
- [Hakim El Kadiri](https://watchdna.com/pages/hakim-el-kadiri) — Founder of ELKA Watch Co.
- [Ian Cognito](https://watchdna.com/pages/ian-cognito) (@IAN_COGNITO) — Watch Enthusiast.
- [Jacky Ho](https://watchdna.com/pages/jacky-ho) — Watchmaker & Artist.
- [Jeremy Freed](https://watchdna.com/pages/jeremy-freed) — Journalist.
- [Mark Fleminger](https://watchdna.com/pages/mark-fleminger) — Watch Enthusiast & RedBar Toronto Chapter Head.
- [Mikhail Gomes](https://watchdna.com/pages/mikhail-gomes) — Strategist - Marketing, PR & Content.
- [Nabil Amdan](https://watchdna.com/pages/nabil-amdan) — Watch Enthusiast.
- [Phillip Plimmer](https://watchdna.com/pages/phillip-plimmer) — Professional Product/Industrial Designer specialist in Watch Design.
- [Roberta Naas](https://watchdna.com/pages/roberta-naas) — Journalist, Author, Founder of ATimelyPerspective.com.
- [Sanket Patel](https://watchdna.com/pages/sanket-patel) — Watch Enthusiast.
- [Sean Shapiro](https://watchdna.com/pages/sean-shapiro) (@VOICEOVERCOP) — Watch Enthusiast, Public Speaker, Podcaster & Opinion Sharer.
- [Smartwatch Dick](https://watchdna.com/pages/smartwatch-dick) — Watch Enthusiast & Podcaster.
- [Spiro Mandylor](https://watchdna.com/pages/spiro-mandylor) — Fashion Photographer & Style Expert.
- [Sevan Khidichian](https://watchdna.com/pages/sevan-khidichian) (Trillium Watch Service) — Certified Watchmaker.
- [Thomas Brissiaud](https://watchdna.com/pages/thomas-brissiaud) — Founder of Tessé Watches.
- [Thomas J. Sandrin](https://watchdna.com/pages/thomas-j-sandrin) — Watch Enthusiast & Entrepreneur.
- [Tyler HorologyObsessed](https://watchdna.com/pages/tyler-horologyobsessed) — Watch Enthusiast.
- [Tyler Worden](https://watchdna.com/pages/tyler-worden) — Industrial Designer & Founder of Worden Watch Studio.
- [Victor JustWatchesTV](https://watchdna.com/pages/victor-justwatchestv) — Watch Enthusiast & Brand Distributor.
- [Victoria Townsend](https://watchdna.com/pages/victoria-townsend) — Watch Journalist & Horology Storyteller.
- [WatchGuyGlasgow](https://watchdna.com/pages/watchguyglasgow) — Watch Enthusiast.
"""

TRADESHOWS = """
TRADESHOWS & EVENTS on WatchDNA:
- [Canadian Watches & Jewelry Show](https://watchdna.com/pages/canadian-watches-jewelry-show)
- [Couture Show](https://watchdna.com/pages/coutureshow)
- [Dubai Watch Week](https://watchdna.com/pages/dubai-watch-week)
- [EPHJ – International Trade Show for High Precision](https://watchdna.com/pages/ephj-the-international-trade-show-for-high-precision)
- [Hong Kong Watch & Clock Fair](https://watchdna.com/pages/hongkong-fair)
- [JCK & Luxury](https://watchdna.com/pages/jck)
- [Timepiece Show](https://watchdna.com/pages/timepieceshow)
- [Time to Watches](https://watchdna.com/pages/time-to-watches)
- [Watches & Wonders](https://watchdna.com/pages/watchesandwonders)
- [Wind Up Watch Fair](https://watchdna.com/pages/windupwatchfair)
- [We Love Watches](https://watchdna.com/pages/we-love-watches-2025-participating-brands)
"""


AWARDS = """
WATCH AWARDS on WatchDNA (always link these when asked about awards):
- [Timepiece World Awards](https://watchdna.com/pages/timepiece-world-awards)
- [Temporis International Awards](https://watchdna.com/pages/the-temporis-international-awards)
- [Grand Prix d'Horlogerie de Genève](https://watchdna.com/pages/grand-prix-horlogerie-geneve)
- [Hong Kong Watch & Clock Design Competition](https://watchdna.com/pages/the-42nd-hong-kong-watch-clock-design-competition)
"""

BRANDS_BY_COUNTRY = """
WATCH BRANDS BY COUNTRY on WatchDNA — use this to answer questions about brands from specific countries.
For each brand, link to https://watchdna.com/blogs/history/[slug] (lowercase hyphenated).

AUSTRALIA: Hz Watches, Second Hour

AUSTRIA: Normalzeit, Sphaera

BELGIUM: Atelier Jalaper, Ice Watch, Ressence, Seagull 1963

CANADA: Ferro & Company, FIORI, Héron, Jakob Eitan, José Cermeño Montréal, Makoto Watch Company,
Noctua Watches, Locke & King, Marathon, Redwood, Shelby Watch Co., SOLIOS, SOVRYGN,
Tessé Watches, Thacker Merali, UNISON, VIEREN, Whitby Watch Co., Wilk Watchworks, Worden Watch Studio, ZENEA

CHINA: Agelocer, Lucky Harvey

DENMARK: Arcanaut, AV86, Bering

ENGLAND: BVOR

FRANCE: Abordage, Arilus, Auricoste, Awake, Baltic, BeauBleu, Bohen, Bouveret, Briston, Carlingue,
Charlie Paris, Charriol, Clyda, Depancel, Elgé, Eska, Fob Paris, Gustave & Cie, Hegid, Herbelin,
Hermès, Jacques Bianchi, Kelton, Lip, March Lab, Mona, Montignac, Nepto, Pequignet, Pierre Lannier,
Reservoir, Serica, Space One, SYE, Trilobe, Van Cleef & Arpels, Yema

GEORGIA: Tsikolia

GERMANY: Adidas, Bruno Sohnle, Circula, Glashütte Original, Hanhart, Iron Annie, Junghans, Junkers,
Laco, MeisterSinger, Nomos Glashütte, Paul Hewitt, Sinn Spezialuhren, Thomas Sabo, Union Glashütte, Zeppelin

Note: Glock Watches is headquartered in Tennessee, USA (not Austria — the firearms company is Austrian but the watch brand is based in the USA)

GREECE: Stil Timepieces

HONG KONG: DIY Watch Club, Electra, Link2Care, Nubeo, OVD, Schaefer & Companions, Tsar Bomba, Undone

INDIA: Jaipur Watch Company, Delhi Watch Company

ITALY: Bvlgari, Eberhard, Echo Neutra, Deadwood, Fathers, Ferragamo, Furla, Locman, Maserati,
Missoni, Naga, Police, Sicis Jewels, U-Boat, Verdure, Versace

JAPAN: Campanola, Casio, Citizen, G-Shock, Grand Seiko, KNIS, Minase, Oceanus, Seiko, Watches of Japan

NETHERLANDS: Cluse, Grone, Grönefeld

NORWAY: Aigi, Micromilspec, Straum, Von Doren

PUERTO RICO: Geo Shop

SINGAPORE: Etien, Feynman, Mondaine Group Singapore, RZE, Ubiq, Vario

SPAIN: Calypso, Festina, Lotus

SWEDEN: Filippa K/Rosenbusch, Daniel Wellington, Kronaby

SWITZERLAND: Abordage Horlogerie, Aerowatch, Alpina, Angelus, Anonimo, Appella, AquaStar, Armin Strom,
Arnold & Son, Atlantic, Audemars Piguet, Backes-Strauss, Ball, Balmain, Baume et Mercier, Bell & Ross,
Berney, Blackout Concept, Blancpain, Bomberg, Breguet, Breitling, Calvin Klein, Carl F. Bucherer,
Cartier, Certina, Chanel, Chopard, Christiaan Van Der Klaauw, Chronoswiss, Cimier, Claude Bernard,
Claude Meylan, Concord, Corum, Cyrus Genève, Czapek & Cie, David Van Heim, Delma, Dior, Direnzo,
Doxa, Ebel, Edox, Epos, Eterna, Exaequo, Farer, Ferdinand Berthoud, Formex, Fortis, Franck Muller,
Frédérique Constant, Furlan Marri, Gallet, Gerald Charles, Gerald Genta, Girard Perregaux,
Glashütte Original, Glycine, Graham, Gucci, H. Moser & Cie., Hamilton, Harry Winston, Hautlence,
Hublot, Hysek, HYT, IWC Schaffhausen, Jaeger-LeCoultre, Jaguar, Jaquet Droz, Jowissa, Kross Studio,
Laurent Ferrier, Longines, Louis Erard, Louis Moinet, Luminox, M+Watch, Marvin, Mathey-Tissot,
Maurice de Mauriac, Maurice Lacroix, MB&F, MeisterSinger, Mido, Mondaine, Montblanc, Montres Etoile,
Movado, Mühle Glashütte, Nivada Grenchen, Nomos Glashütte, Norqain, Oris, Officine Panerai,
Parmigiani Fleurier, Patek Philippe, Philipp Plein, Piaget, Pierre Kunz, Pilo & Co Genève,
Plein Sport, Rado, Raymond Weil, Richard Mille, Roamer, Roger Dubuis, Rolex, Rosenbusch,
Rudis Sylva, Solar Aqua, Speake Marin, Swatch, Swiss Military Hanowa, Swiss Watch Co.,
TAG Heuer, Tiffany & Co., Timeless, Tissot, Titoni, Trauffer, Trilobe, Tudor, Tweed Co.,
Ulysse Nardin, Universal Genève, Urwerk, Vacheron Constantin, Vanguart, Ventura, Victorinox,
Vulcain, Wancher, Wenger, Zenith, Zodiac

TAIWAN: Havaan Tuvali

THAILAND: Wise

UNITED KINGDOM: anOrdain, Arken, AVI-8, Beaucroft, Bremont, Christopher Ward, Clemence,
Duckworth Prestex, Elliot Brown, Escudo, Fears Bristol, Isotope, Le Coc, Mezei,
Olivia Burton, Paulin, Selten, Studio Underd0g, Ted Baker

UNITED STATES OF AMERICA: (A)LT|SYM, Abingdon, Ares, Benrus, Brew Watch Co, Bulova, Coach, Glock Watches,
Colorado, Compass, Core Timepieces, DKNY, Dufrane, Farr + Swit, Fossil, Garmin, GC, Good+evil,
Guess, Haim, Hampden, Invicta, Jack Mason, Jacob & Co, Kate Spade, Kirkland, Michael Kors,
Michele, MVMT, Nautica, Nodus, Shinola, Skagen, Stella, S. Coifman, Timex, Tommy Hilfiger,
Tory Burch, Traum, Vaer, Verdure, Vero Batch Company, Vortic, Watchcraft

Note: When asked about brands from a country, list from the relevant section above and link each one.
"""


ALL_BRANDS = """
Every brand on WatchDNA has a page at https://watchdna.com/blogs/history/[slug].
Use lowercase-hyphenated slugs (e.g. "A. Lange & Söhne" → "a-lange-sohne", "TAG Heuer" → "tag-heuer").

CONFIRMED BRANDS ON WATCHDNA (partial list — always try /blogs/history/[slug]):
A. Lange & Söhne, Accutron, Alpina, Arnold & Son, Audemars Piguet, Ball, Baume et Mercier,
Bering, Bell & Ross, Blancpain, Breguet, Breitling, Bulova, Calvin Klein, Cartier, Casio,
Certina, Chopard, Citizen, Christopher Ward, Doxa, Ebel, Eterna, Festina, Fortis, Fossil,
Franck Muller, Frédérique Constant, G-Shock, Girard Perregaux, Glashütte Original, Hamilton,
Hermès, Hublot, IWC Schaffhausen, Jaeger-LeCoultre, Junghans, Laco, Longines, Luminox,
Maurice Lacroix, MB&F, Mido, Mondaine, Montblanc, Movado, Nomos Glashütte, Norqain, Omega,
Oris, Panerai, Patek Philippe, Piaget, Rado, Raymond Weil, Richard Mille, Rolex, Seiko,
Sinn, Swatch, TAG Heuer, Tissot, Tudor, Ulysse Nardin, Vacheron Constantin, Zenith,
Elka, Tessé, Worden, Normalzeit, Fortis, DWISS, Luminox, Alpina, Bulova, Bering
"""

SYSTEM_PROMPT = """You are WatchBot, the AI assistant for WatchDNA.com — a global directory and community for watch lovers.

PERSONALITY: Passionate watch enthusiast, knowledgeable, direct, conversational, friendly. Never say "As an AI".

=== HOW TO ANSWER — MOST IMPORTANT RULE ===
- When asked a general question like "tell me about brands", "tell me about tradeshows", "tell me about contributors" — pick ONE interesting one and tell them about it in a conversational paragraph. Do NOT list everything.
- End with something like "Want to hear about another one?" or "Ask me about a specific one!"
- Only give a full list if the user explicitly asks "list all", "what are all the", "show me all", etc.
- When asked about a specific item ("tell me about Dubai Watch Week", "tell me about Norqain") — give a rich engaging paragraph about that one thing with its link. Not a bullet list.
- Keep responses concise and conversational — like a knowledgeable friend, not a directory.

=== LINK FORMAT — ABSOLUTE RULES ===
- Every link MUST be: [Descriptive Title](https://exact-url.com)
- Use the product/article TITLE as link text. NEVER "here", "View here", "Read article", "Check it out", "Explore here", "Discover it", "Check it out".
- ONLY use URLs from the WEBSITE CONTENT below. Never construct or guess URLs.
- No fake links. One link per item. Never link the same item twice.
- For product recommendations: the title IS the link. NEVER add a second link after the description.

=== CURRENCY & PRODUCTS ===
- User's selected currency: {currency}
- ALL products in WEBSITE CONTENT are already filtered to only those available in the {currency} market.
- Show prices exactly as in the content. Do NOT convert or calculate.
- CRITICAL: ONLY recommend products that physically appear in WEBSITE CONTENT below with a real URL. If you cannot find a matching product in WEBSITE CONTENT, say so — do NOT invent product names, brands, prices, or URLs from your training knowledge. Tissot, Seiko, TAG Heuer, Tudor, Rolex etc. may be brands you know but if their products are not in WEBSITE CONTENT for this market, do NOT recommend them.
- When asked to compare brands or show watches from multiple brands: ONLY list watches that appear in WEBSITE CONTENT for the current currency market. If a brand has no products in WEBSITE CONTENT, say "X is not currently available on WatchDNA in [currency]" — never invent products for that brand.
- EVERY product you recommend must have its exact title and URL copied verbatim from WEBSITE CONTENT. Product names like "Adventure Sport Automatic" or "Freedom 60 Automatic" that have no URL in WEBSITE CONTENT are HALLUCINATIONS — never do this.
- STRICT FORMAT for EACH watch recommendation — no exceptions:
  **[Product Name](url)** — {symbol}X.XX {currency}
  Brief description in 1-2 sentences.
- The product name MUST be a markdown link: [Product Name](url). NEVER write **Product Name** without a URL. NEVER bold a name without linking it.
- NEVER add a separate link after the description. NEVER write "Explore here", "Check it out", "Discover it" or any trailing link. The product name IS the only link.
- Most expensive watch: use the MOST EXPENSIVE NOTE below if provided — do not guess.

=== ACCESSORIES ===
- When asked about watch accessories (cases, winders, straps, boxes etc.) link to: [Watch Accessories](https://watchdna.com/collections/accessories)
- If there are accessory products in WEBSITE CONTENT, recommend them with price and link.
- Accessories are currency-specific just like watches — only show ones matching the user's currency.

WATCH RECOMMENDATION FLOW — CRITICAL:
- If the user asks for watch recommendations and has NOT specified a currency in this conversation, ALWAYS ask first:
  "Which market would you like recommendations in? 🌍 CAD, USD, GBP, CHF, or EUR?"
- Once they pick a currency, recommend ONLY watches from that market (already filtered in content).
- NEVER recommend watches from a different currency than what was asked — the same watch has different entries per market and only the correct one will work.
- PREMIUM BRANDS FIRST: When recommending watches, always lead with premium/luxury brands (Rolex, Omega, TAG Heuer, Breitling, IWC, Cartier, Patek Philippe, Audemars Piguet, Hublot, Tudor, Longines, Rado, Tissot, Hamilton, Seiko Grand Seiko, Oris, Nomos, etc.) before entry-level or fashion brands — as long as they match the user's request and budget.
- When the user asks for watches by feature (strap material, movement, case size, style, color, material etc.) ONLY recommend watches whose content in WEBSITE CONTENT explicitly mentions that feature. NEVER infer or assume a feature if it is not stated — e.g. if "leather" is not in the product content, do NOT recommend it as a leather strap watch. Examples:
  - "automatic movement" → only watches with "Movement: Automatic" in Features
  - "chronograph" → only watches with "Styles: Chronograph" in Features (NOT just mentioned in description)
  - "43mm" → only watches with "Case Size: 43" in Features
  - "pilot style" → only watches with "Styles: Pilot" in Features
  - "recycled steel" → only watches with "Case Material: Recycled Steel" in Features
- If no watches match the requested feature, say so honestly rather than recommending ones that don't match.

=== BRANDS ===
- Brand history pages in WEBSITE CONTENT contain the real facts: description, FOUNDED year, HEADQUARTERS location, WEBSITE, and timeline. ALWAYS use this scraped data — scraped data overrides your training knowledge completely.
- NEVER contradict what the scraped page says. If the page says "HEADQUARTERS: Tennessee, USA" use that. If it says "FOUNDED: 2014" use that.
- Every brand MUST have a link in the FIRST response — never wait to be asked. Format: [Brand Name](https://watchdna.com/blogs/history/[slug]). Examples: Rolex → /blogs/history/rolex, TAG Heuer → /blogs/history/tag-heuer, Glock Watches → /blogs/history/glock-watches.
- If a brand is NOT in WEBSITE CONTENT, immediately say it's not on WatchDNA yet and include this link: [Explore all brands on WatchDNA](https://watchdna.com/pages/brands-dna). Never make up a link for unknown brands.
- When listing multiple brands (e.g. "Indian watch brands"), every single brand in the list MUST have its link included — never list a brand name without a link.
- When asked about brands from a specific country, use BRANDS BY COUNTRY data.
- When asked about brand groups link to their history page: [Group Name](https://watchdna.com/blogs/history/[slug]). Also add: "You can explore all brand groups at [Brand Groups](https://watchdna.com/pages/groups)"

BRAND LINKS:
{brand_links}

=== ARTICLES (watch-enthusiast blog) ===
- "Article" or "latest article" refers EXCLUSIVELY to posts from https://watchdna.com/blogs/watch-enthusiast
- NEVER return a press release as an article. Press releases have "Article Type: Press Release" — ignore them for article queries.
- Find articles with "Article Type: Community Article (Watch Enthusiast)" — sort by Published date DESCENDING, the highest date is the most recent.
- Format: [Article Title](exact-url) — by Author, Published: YYYY-MM-DD
- NEVER invent titles, authors, dates, or URLs.

=== BRAND ARTICLES & BLOGS ===
- When asked "blogs about [brand]", "articles on [brand]", "latest [brand] news/posts":
  - Always direct the user to the brand's page on WatchDNA where all their articles are listed.
  - Format: "You can find all [Brand] articles here: [Brand Name on WatchDNA](https://watchdna.com/blogs/history/[slug])"
  - Use the slug from BRAND LINKS above (e.g. Rolex → rolex, TAG Heuer → tag-heuer, Rado → rado).
  - Do NOT mention or link any individual articles. Just give the brand page link and say all their articles are listed there.

=== BLOGS (watch-enthusiast) ===
- "Blog", "latest blog", "blog post", or "latest post" refers to posts from https://watchdna.com/blogs/watch-enthusiast
- Find blog posts in WEBSITE CONTENT with "Article Type: Community Article (Watch Enthusiast)" — these are the blog posts.
- Sort by Published date DESCENDING — the highest date (e.g. 2026-03-20) is the most recent.
- The most recent = the single entry with the largest Published date value.
- Format: [Blog Title](exact-url) — Published: YYYY-MM-DD
- NEVER invent titles, dates, or URLs. ONLY use URLs that appear in WEBSITE CONTENT.
- If asked for the "latest" one, return the single entry with the most recent Published date.

=== RELATED ARTICLES ===
- NEVER append a "Related reading" or "📖 Related reading" section to any response. Do not suggest articles unprompted.
- Only mention articles if the user explicitly asks for articles or blogs.

=== WHEN YOU DON'T HAVE THE ANSWER ===
- If WEBSITE CONTENT does not contain enough information to answer the question (e.g. upcoming tradeshow dates, external event schedules, general horology facts not on the site), do NOT make up an answer.
- Instead say: "I don't have that info on WatchDNA right now. Want me to answer using my general AI knowledge? Just say **yes** and I'll do my best! 🤖"
- Never say "I don't know" without offering the fallback.
- Once the user says yes (or any affirmative), the system will automatically use general AI knowledge to answer.

=== TRADESHOWS ===
- When ANYONE asks about tradeshows, upcoming shows, next tradeshow, tradeshow dates, tradeshow calendar, or where/when shows are: ALWAYS immediately give this link: [Tradeshows Calendar](https://watchdna.com/pages/tradeshows). Do NOT say you don't have that info.
- Then list individual shows if the user wants more detail:
  1. [Canadian Watches & Jewelry Show](https://watchdna.com/pages/canadian-watches-jewelry-show)
  2. [Couture Show](https://watchdna.com/pages/coutureshow)
  3. [Dubai Watch Week](https://watchdna.com/pages/dubai-watch-week)
  4. [EPHJ](https://watchdna.com/pages/ephj-the-international-trade-show-for-high-precision)
  5. [Hong Kong Watch & Clock Fair](https://watchdna.com/pages/hongkong-fair)
  6. [JCK & Luxury](https://watchdna.com/pages/jck)
  7. [Timepiece Show](https://watchdna.com/pages/timepieceshow)
  8. [Time to Watches](https://watchdna.com/pages/time-to-watches)
  9. [Watches & Wonders](https://watchdna.com/pages/watchesandwonders)
  10. [Wind Up Watch Fair](https://watchdna.com/pages/windupwatchfair)
  11. [We Love Watches](https://watchdna.com/pages/we-love-watches-2025-participating-brands)
- Never invent tradeshow names or URLs.

=== AWARDS ===
- Use the AWARDS DATA below for all award info and links.
- Always link to the award page when mentioned.

=== CONTRIBUTORS ===
- Use ONLY the CONTRIBUTORS DATA below to answer contributor questions.
- Each contributor has their own individual URL — always use that specific URL.
- Format: [Full Name](their-individual-url) — Role/bio
- Each contributor page in WEBSITE CONTENT shows their bio AND their articles listed under "BRENT'S ARTICLES" / "CAROL'S ARTICLES" etc.
- When asked "what articles did [contributor] write" — find their page in WEBSITE CONTENT and list the articles shown there with links.
- Articles in the watch-enthusiast blog have an "Author:" field — use this to match articles to contributors.

=== STORE LOCATOR ===
- Always immediately give this link: [Find a Store](https://watchdna.com/tools/storelocator)
- No need to ask for brand or location — just give the link and tell them to search on the map.

KEY PAGES:
- All Watches: https://watchdna.com/collections/watches
- Store Locator: https://watchdna.com/tools/storelocator
- Brands Directory: https://watchdna.com/pages/brands-dna
- Watch Enthusiast: https://watchdna.com/blogs/watch-enthusiast
- Press Releases: https://watchdna.com/blogs/press

CONTRIBUTORS DATA:
{contributors}

TRADESHOWS DATA:
{tradeshows}

AWARDS DATA:
{awards}

BRANDS BY COUNTRY:
{brands_by_country}

ALL BRANDS ON WATCHDNA:
{all_brands}

STORE LOCATOR LINKS BY BRAND:
{store_links}

WEBSITE CONTENT ({currency} market):
{knowledge}
"""


class ChatRequest(BaseModel):
    message: str
    history: list = []
    location: str = ""
    currency: str = "CAD"
    gpt_fallback_confirmed: bool = False


def detect_currency_in_text(text: str) -> str | None:
    """
    Detect currency from text. Handles both codes (USD, GBP) and
    natural language words (dollars, pounds, euros, francs, swiss).
    """
    text_upper = text.upper()

    # 1. Match currency codes (USD, CAD, GBP, CHF, EUR)
    for cur in VALID_CURRENCIES:
        if re.search(r"\b" + cur + r"\b", text_upper):
            return cur

    # 2. Match natural language currency words
    word_map = [
        (r"\bEUROS?\b",             "EUR"),
        (r"\bPOUNDS?\b|\bSTERLING\b", "GBP"),
        (r"\bSWISS\b|\bFRANCS?\b",    "CHF"),
        # "dollars" is ambiguous (CAD vs USD) so we skip it
    ]
    for pattern, cur in word_map:
        if re.search(pattern, text_upper):
            return cur

    return None


def resolve_currency(req: "ChatRequest") -> str:
    """
    Priority:
      1. Current user message
      2. Most recent USER turn in history (newest first) — skips assistant turns
      3. Fallback to CAD

    Intentionally ignores req.currency (Shopify widget) because it always
    sends the store default (CAD) and overrides what the user said in chat.
    """
    # 1. Current message
    found = detect_currency_in_text(req.message)
    if found:
        print(f"[CURRENCY] From current message: {found}")
        return found

    # 2. Most recent USER turn in history
    for h in reversed(req.history):
        if h.get("role") != "user":
            continue
        found = detect_currency_in_text(h.get("content", ""))
        if found:
            print(f"[CURRENCY] From history user turn: {found}")
            return found

    # 3. Shopify widget — reflects which market the user is browsing
    widget = req.currency.upper().strip()
    if widget in VALID_CURRENCIES:
        print(f"[CURRENCY] From Shopify widget: {widget}")
        return widget

    # 4. Fallback
    print("[CURRENCY] Defaulting to CAD")
    return "CAD"


GPT_FALLBACK_PROMPT = """You are WatchBot, a knowledgeable watch enthusiast assistant for WatchDNA.com.
The user has requested a general AI answer because the WatchDNA knowledge base didn't have this information.
Answer helpfully and conversationally about watches, horology, tradeshows, events, or whatever they asked.
Be honest that this is general knowledge, not WatchDNA-specific data.
Where relevant, suggest they explore https://watchdna.com for community articles and watch listings.
Never make up prices, dates, or product URLs."""

GPT_FALLBACK_TRIGGERS = {
    "yes", "yeah", "sure", "ok", "okay", "yep", "yup", "go ahead",
    "please", "do it", "use chatgpt", "use gpt", "yes please", "absolutely"
}

def _is_gpt_fallback_confirmation(message: str, history: list) -> bool:
    """Return True if the user just confirmed they want the GPT fallback."""
    msg_lower = message.strip().lower().rstrip("!.,?")
    if msg_lower not in GPT_FALLBACK_TRIGGERS:
        return False
    # Check the previous assistant message offered the fallback
    for h in reversed(history):
        if h.get("role") == "assistant":
            prev = h.get("content", "").lower()
            return "general ai knowledge" in prev or "use chatgpt" in prev or "chatgpt" in prev
    return False


@app.post("/chat")
async def chat(req: ChatRequest):
    currency = resolve_currency(req)
    print(f"[CURRENCY DETECTED] {currency} | message: {req.message[:60]}")

    # --- GPT FALLBACK PATH ---
    use_fallback = (
        req.gpt_fallback_confirmed
        or _is_gpt_fallback_confirmation(req.message, req.history)
    )
    if use_fallback:
        print("[GPT_FALLBACK] Answering without KB")
        messages = [{"role": "system", "content": GPT_FALLBACK_PROMPT}]
        for h in req.history[-8:]:
            messages.append(h)
        messages.append({"role": "user", "content": req.message})
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            max_tokens=500,
            temperature=0.7,
        )
        return {"reply": response.choices[0].message.content, "used_gpt_fallback": True}

    # --- NORMAL KB PATH ---
    symbol = CURRENCY_SYMBOLS.get(currency, "$")
    knowledge = load_knowledge(req.message, currency=currency)
    print(f"[KNOWLEDGE] loaded for currency={currency}")

    market_brands = get_brands_for_market(currency)
    brands_hint = ""
    if market_brands:
        brands_hint = (
            f"\n\nAVAILABLE BRANDS IN {currency} MARKET: {', '.join(market_brands)}\n"
            f"IMPORTANT: ONLY recommend watches from brands in this list. "
            f"If asked for a brand NOT in this list, say it is not available in the {currency} market "
            f"and suggest they check another currency or browse https://watchdna.com/collections/watches"
        )

    brand_map = get_brand_map()
    store_links = "\n".join([
        f"- {v['name']}: {v['url']}"
        for k, v in brand_map.items() if k == v["name"].lower()
    ])

    history_text = " ".join([h.get("content", "") for h in req.history[-6:]])
    brand_match = find_brand_in_query(req.message) or find_brand_in_query(history_text)
    is_store_query = any(w in req.message.lower() for w in [
        "store", "dealer", "where can i buy", "find a store", "find a dealer",
        "authorized", "retailer", "closest store", "nearby store"
    ])

    store_hint = ""
    if is_store_query:
        store_hint = "\n\nNOTE: Give user the store locator link directly: [Find a Store](https://watchdna.com/tools/storelocator) — tell them to search by brand or city on the map."

    expensive_hint = ""
    if any(w in req.message.lower() for w in ["most expensive", "priciest", "highest price", "most costly"]):
        best = get_most_expensive(currency)
        if best:
            sym = CURRENCY_SYMBOLS.get(currency, "$")
            expensive_hint = (
                f"\n\nMOST EXPENSIVE WATCH IN {currency}: "
                f"'{best['title']}' at {sym}{best['price']:,.2f} {currency}. "
                f"URL: {best['url']}. Use this exact data."
            )

    # Brand product hint — inject exact product list for any brand mentioned in query
    # This prevents GPT from hallucinating product names/URLs
    brand_product_hint = ""
    msg_lower_hint = req.message.lower()
    if any(w in msg_lower_hint for w in ["compare", "vs", "versus", "show me", "watches from", "collection"]):
        data = get_knowledge_base()
        if data:
            all_products = [p for p in data.get("pages", [])
                           if "/products/" in p.get("url", "") and p.get("currency", "") == currency]
            # Find which brands are mentioned
            mentioned_brands = []
            for word_combo in [msg_lower_hint, history_text.lower()]:
                for p in all_products:
                    for line in p.get("content","").split("\n"):
                        if line.startswith("Brand/Vendor:"):
                            vendor = line.replace("Brand/Vendor:","").strip()
                            if vendor.lower() in word_combo and vendor not in mentioned_brands:
                                mentioned_brands.append(vendor)
            mentioned_brands = list(dict.fromkeys(mentioned_brands))[:3]  # max 3 brands
            if mentioned_brands:
                hint_lines = ["\n\nEXACT PRODUCTS IN KB FOR THIS QUERY — USE ONLY THESE URLS:"]
                for brand in mentioned_brands:
                    brand_prods = [p for p in all_products
                                  if any(line.replace("Brand/Vendor:","").strip() == brand
                                        for line in p.get("content","").split("\n")
                                        if line.startswith("Brand/Vendor:"))][:5]
                    if brand_prods:
                        hint_lines.append(f"\n{brand}:")
                        sym = CURRENCY_SYMBOLS.get(currency, "$")
                        for p in brand_prods:
                            hint_lines.append(f"  - [{p['title']}]({p['url']}) — {sym}{p['price']:,.2f} {currency}")
                brand_product_hint = "\n".join(hint_lines)

    brand_links = get_brand_history_links()

    system = SYSTEM_PROMPT.format(
        currency=currency,
        symbol=symbol,
        contributors=CONTRIBUTORS,
        tradeshows=TRADESHOWS,
        awards=AWARDS,
        brands_by_country=BRANDS_BY_COUNTRY,
        all_brands=ALL_BRANDS,
        store_links=store_links,
        brand_links=brand_links,
        knowledge=knowledge + store_hint + expensive_hint + brand_product_hint + brands_hint,
    )

    messages = [{"role": "system", "content": system}]
    for h in req.history[-8:]:
        messages.append(h)
    messages.append({"role": "user", "content": req.message})

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=messages,
        max_tokens=450,
        temperature=0.7,
    )
    reply = response.choices[0].message.content

    # Fix lazy "here" links
    kb = get_knowledge_base()
    if kb:
        url_to_title = {
            p["url"]: p["title"]
            for p in kb.get("pages", [])
            if p.get("url") and p.get("title")
        }
        def fix_here_link(m):
            link_text = m.group(1).strip().lower()
            url = m.group(2).strip()
            lazy_words = {"here", "view here", "check it out", "see here", "read here",
                          "learn more", "read more", "view", "link", "click here"}
            if link_text in lazy_words and url in url_to_title:
                return f"[{url_to_title[url]}]({url})"
            return m.group(0)
        reply = re.sub(r'\[([^\]]+)\]\(([^)]+)\)', fix_here_link, reply)

    return {"reply": reply}


@app.post("/debug-currency")
async def debug_currency(req: ChatRequest):
    """Test endpoint — call this to see exactly what currency is resolved and how many products load."""
    currency = resolve_currency(req)
    data = get_knowledge_base()
    all_products = [p for p in data.get("pages", []) if "/products/" in p.get("url", "")]
    matching = [p for p in all_products if p.get("currency", "") == currency]
    currencies_in_kb = list(set(p.get("currency", "MISSING") for p in all_products))
    return {
        "resolved_currency": currency,
        "req_currency_field": req.currency,
        "message_scanned": req.message,
        "products_in_kb_for_currency": len(matching),
        "all_currencies_in_kb": sorted(currencies_in_kb),
        "sample_products": [
            {"title": p["title"], "price": p["price"], "currency": p["currency"]}
            for p in matching[:5]
        ]
    }


@app.get("/health")
async def health():
    kb_exists = Path(KNOWLEDGE_FILE).exists()
    last_scraped = None
    if kb_exists:
        with open(KNOWLEDGE_FILE) as f:
            last_scraped = json.load(f).get("scraped_at")
    return {"status": "ok", "knowledge_base": kb_exists, "last_scraped": last_scraped}
