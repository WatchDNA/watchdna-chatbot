from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from openai import OpenAI
import json, os, re, csv, io, urllib.request, asyncio
import requests
from bs4 import BeautifulSoup
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


def get_knowledge_base():
    global _kb_cache
    if _kb_cache:
        return _kb_cache
    if Path(KNOWLEDGE_FILE).exists():
        try:
            with open(KNOWLEDGE_FILE) as f:
                _kb_cache = json.load(f)
            print(f"KB loaded: {_kb_cache.get('product_count',0)} products")
            return _kb_cache
        except Exception as e:
            print(f"Local KB error: {e}")
    try:
        with urllib.request.urlopen(GITHUB_KB_URL, timeout=20) as r:
            _kb_cache = json.loads(r.read().decode())
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
        price = page.get("price", 0)
        if price > best_price:
            best_price = price
            best = page
    return best


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
    matches = [(len(k), v) for k, v in brand_map.items() if k in q]
    return sorted(matches, reverse=True)[0][1] if matches else None


def extract_budget(query: str):
    patterns = [
        r"under\s*\$?([\d,]+)",
        r"below\s*\$?([\d,]+)",
        r"less than\s*\$?([\d,]+)",
        r"\$?([\d,]+)\s*(?:cad|usd|gbp|chf|eur|dollars|budget|or less|max|maximum)",
        r"budget\s*(?:of|is|:)?\s*\$?([\d,]+)",
    ]
    for p in patterns:
        m = re.search(p, query.lower())
        if m:
            try:
                return float(m.group(1).replace(",", ""))
            except:
                pass
    return None


ARTICLE_QUERY_WORDS = [
    "article", "articles", "latest", "recent", "newest", "blog", "press",
    "release", "story", "stories", "post", "posts", "published", "news", "read"
]

def is_article_query(query: str) -> bool:
    return any(w in query.lower() for w in ARTICLE_QUERY_WORDS)


def load_knowledge(query: str = "", currency: str = "CAD", live_article_context: str = "") -> str:
    data = get_knowledge_base()
    if not data:
        return "Knowledge base not available."

    currency = currency.upper()
    budget = extract_budget(query)
    keywords = [w for w in query.lower().split() if len(w) > 2]

    articles = []
    non_articles = []

    for page in data.get("pages", []):
        url = page.get("url", "")
        is_product = "/products/" in url
        is_article = "/blogs/" in url

        if is_product:
            if page.get("currency", "") != currency:
                continue
            if budget and page.get("price", 0) > budget:
                continue

        if is_article:
            articles.append(page)
        else:
            non_articles.append(page)

    articles.sort(key=lambda p: p.get("published", ""), reverse=True)

    product_pages = [p for p in non_articles if "/products/" in p.get("url", "")]
    print(f"[LOAD_KNOWLEDGE] currency={currency} | products={len(product_pages)} | articles={len(articles)}")

    def score(page):
        text = (page.get("title", "") + " " + page.get("content", "")).lower()
        return sum(1 for kw in keywords if kw in text)

    if is_article_query(query):
        scored_non = sorted(non_articles, key=score, reverse=True)[:15]
        ordered = articles + scored_non
    elif keywords:
        all_pages = articles + non_articles
        ordered = sorted(all_pages, key=score, reverse=True)[:40]
    else:
        ordered = articles + non_articles

    # Prepend live article data if provided — this always wins over stale KB dates
    context = live_article_context + "\n\n" if live_article_context else ""

    for page in ordered:
        entry = f"\n\n--- {page['url']} ---\n{page['content']}"
        if len(context) + len(entry) > 22000:
            break
        context += entry
    return context

SYSTEM_PROMPT = """You are WatchBot, the AI assistant for WatchDNA.com — a global directory and community for watch lovers.

PERSONALITY: Passionate watch enthusiast, knowledgeable, direct, friendly. Never say "As an AI".

=== LINK FORMAT — ABSOLUTE RULES ===
- Every link MUST be: [Descriptive Title](https://exact-url.com)
- Use the product/article TITLE as link text. NEVER "here", "View here", "Read article", "Check it out".
- ONLY use URLs from the WEBSITE CONTENT below. Never construct or guess URLs.
- No fake links. One link per item. Never link the same item twice.

=== CURRENCY & PRODUCTS ===
- User's selected currency: {currency}
- ALL products in WEBSITE CONTENT are already filtered to only those available in the {currency} market.
- Show prices exactly as in the content. Do NOT convert or calculate.
- Only recommend products from WEBSITE CONTENT. Never invent product names or URLs.
- Format: [Product Name](url) — {symbol}X.XX {currency}
- Most expensive watch: use the MOST EXPENSIVE NOTE below if provided — do not guess.

WATCH RECOMMENDATION FLOW — CRITICAL:
- If the user asks for watch recommendations and has NOT specified a currency in this conversation, ALWAYS ask first:
  "Which market would you like recommendations in? 🌍 CAD, USD, GBP, CHF, or EUR?"
- Once they pick a currency, recommend ONLY watches from that market (already filtered in content).
- NEVER recommend watches from a different currency than what was asked — the same watch has different entries per market and only the correct one will work.

=== BRAND QUESTIONS ===
- Use BOTH site content AND your general watch knowledge for brand history, founders, country of origin.
- Always check if the brand has products on WatchDNA and mention with a link if so.

=== ARTICLES ===
- When asked for articles, just list the most recent ones from WEBSITE CONTENT — newest Published date first.
- Do NOT ask which section. Mix both blogs and show the latest.
- Format: [Article Title](exact-url) — by Author, Published: YYYY-MM-DD
- ONLY use articles from WEBSITE CONTENT with a real URL field. NEVER invent titles, authors, dates, or URLs.
- If an article has no URL in the content, do not mention it.

=== TRADESHOWS & AWARDS ===
- List ALL tradeshows or awards from the WEBSITE CONTENT with their real links.
- Never only show one. Never say "I don't have that link."
- Format: [Name](url) — one line each.

=== STORE LOCATOR ===
- Step 1: No brand → "Which brand are you looking for?"
- Step 2: Brand, no location → "What's your postal code or city?"
- Step 3: Both → give filtered link from STORE LOCATOR LINKS, tell them to type postal code in the map search bar.

KEY PAGES:
- All Watches: https://watchdna.com/collections/watches
- Store Locator: https://watchdna.com/tools/storelocator
- Brands Directory: https://watchdna.com/pages/brands-dna
- Watch Enthusiast: https://watchdna.com/blogs/watch-enthusiast
- Press Releases: https://watchdna.com/blogs/press

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


@app.post("/chat")
async def chat(req: ChatRequest):
    currency = resolve_currency(req)
    print(f"[CURRENCY DETECTED] {currency} | message: {req.message[:60]}")

    symbol = CURRENCY_SYMBOLS.get(currency, "$")
    # load_knowledge filters pages by page["currency"] == currency exactly
    # For article queries, fetch live from the site so dates are always accurate
    live_ctx = ""
    if is_article_query(req.message):
        try:
            live_arts = fetch_live_articles(limit=15)
            live_ctx = build_live_article_context(live_arts)
            print(f"[LIVE ARTICLES] fetched {len(live_arts)} articles")
        except Exception as e:
            print(f"[LIVE ARTICLES] failed: {e}")
    knowledge = load_knowledge(req.message, currency=currency, live_article_context=live_ctx)
    print(f"[KNOWLEDGE] loaded for currency={currency}")

    brand_map = get_brand_map()
    store_links = "\n".join([
        f"- {v['name']}: {v['url']}"
        for k, v in brand_map.items() if k == v["name"].lower()
    ])

    # Store locator hints
    history_text = " ".join([h.get("content", "") for h in req.history[-6:]])
    brand_match = find_brand_in_query(req.message) or find_brand_in_query(history_text)
    is_store_query = any(w in req.message.lower() for w in [
        "store", "dealer", "where can i buy", "find a store", "find a dealer",
        "authorized", "retailer", "closest store", "nearby store"
    ])

    store_hint = ""
    if is_store_query:
        if brand_match and req.location:
            store_hint = (
                f"\n\nNOTE: Give user filtered map link: "
                f"[{brand_match['name']} Dealers Near You]({brand_match['url']}) "
                f"Tell them to type '{req.location}' in the search bar on the map."
            )
        elif brand_match:
            store_hint = "\n\nNOTE: Ask user for their postal code or city."
        else:
            store_hint = "\n\nNOTE: Ask user which brand they're looking for."

    # Most expensive — computed in backend, not guessed by AI
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

    system = SYSTEM_PROMPT.format(
        currency=currency,
        symbol=symbol,
        store_links=store_links,
        knowledge=knowledge + store_hint + expensive_hint,
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
    return {"reply": response.choices[0].message.content}


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


# ---------------------------------------------------------------------------
# LIVE ARTICLE FETCHER — bypasses knowledge base, always returns current data
# ---------------------------------------------------------------------------
ARTICLE_SOURCES = [
    {"url": "https://watchdna.com/blogs/press",             "label": "Press Release"},
    {"url": "https://watchdna.com/blogs/watch-enthusiast",  "label": "Community Article"},
]
_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; WatchDNAChatbot/1.0)"}

def fetch_live_articles(limit: int = 10) -> list:
    """Scrape the blog listing pages live and return articles sorted newest-first."""
    results = []
    seen = set()
    for source in ARTICLE_SOURCES:
        try:
            resp = requests.get(source["url"], headers=_HEADERS, timeout=10)
            if resp.status_code != 200:
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            # Each article card has an <h3> or <h2> with an <a> inside
            for heading in soup.find_all(["h2", "h3"]):
                a = heading.find("a", href=True)
                if not a:
                    continue
                href = a["href"]
                if not href.startswith("http"):
                    href = "https://watchdna.com" + href
                if href in seen or "/blogs/" not in href:
                    continue
                seen.add(href)
                title = a.get_text(strip=True)
                if not title or len(title) < 5:
                    continue
                # Date: look for a sibling or nearby element with a date
                date_str = ""
                card = heading.parent
                for _ in range(4):          # walk up a few levels
                    if card is None:
                        break
                    time_tag = card.find("time")
                    if time_tag:
                        date_str = time_tag.get("datetime", time_tag.get_text(strip=True))[:10]
                        break
                    # also check text nodes that look like dates
                    text = card.get_text(" ", strip=True)
                    import re as _re
                    m = _re.search(r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}", text)
                    if m:
                        from datetime import datetime as _dt
                        try:
                            date_str = _dt.strptime(m.group(0), "%B %d, %Y").strftime("%Y-%m-%d")
                        except:
                            pass
                        break
                    card = card.parent
                results.append({
                    "title": title,
                    "url": href,
                    "date": date_str,
                    "type": source["label"],
                })
        except Exception as e:
            print(f"[LIVE ARTICLES] {source['url']}: {e}")

    # Sort newest-first (empty date falls to bottom)
    results.sort(key=lambda x: x.get("date", ""), reverse=True)
    return results[:limit]


def build_live_article_context(articles: list) -> str:
    lines = ["LIVE ARTICLES (fetched right now from watchdna.com — these are the REAL latest articles):"]
    for i, a in enumerate(articles, 1):
        lines.append(
            f"{i}. Article: {a['title']}\n"
            f"   Published: {a['date'] or 'recent'}\n"
            f"   Type: {a['type']}\n"
            f"   URL: {a['url']}"
        )
    return "\n".join(lines)


@app.get("/articles")
async def get_articles(limit: int = 10):
    """Debug endpoint — see what the live article fetcher returns."""
    articles = fetch_live_articles(limit)
    return {"count": len(articles), "articles": articles}


@app.post("/scrape")
async def trigger_scrape():
    """Trigger a knowledge base rescrape in the background."""
    async def run_scrape():
        try:
            proc = await asyncio.create_subprocess_exec(
                "python3", "scraper.py",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            stdout, _ = await proc.communicate()
            global _kb_cache
            _kb_cache = None   # bust cache so next request reloads
            print("[SCRAPE] Done:\n" + stdout.decode()[-2000:])
        except Exception as e:
            print(f"[SCRAPE] Error: {e}")
    asyncio.create_task(run_scrape())
    return {"status": "scrape started — check /health in ~2 minutes"}
