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
    "strap", "desk organizer", "desk organiser", "watch box", "legion safes",
    "watch certificate", "8 piece watch winder", "16 piece watch winder",
    "6 piece watch winder", "double watch winder", "quad watch winder",
}

# Hardcoded contributor data with individual page links
# URL format: https://watchdna.com/pages/firstname-lastname (slugified)
# If a contributor does not have their own page, falls back to /pages/contributors
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

NOTE: If any individual page returns a 404, link to https://watchdna.com/pages/contributors instead.
"""

# Hardcoded tradeshows with correct URLs
TRADESHOWS = """
TRADESHOWS & EVENTS on WatchDNA (always list ALL of these with their links):
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


def get_knowledge_base():
    global _kb_cache
    if _kb_cache:
        return _kb_cache
    if Path(KNOWLEDGE_FILE).exists():
        try:
            with open(KNOWLEDGE_FILE) as f:
                _kb_cache = json.load(f)
            print(f"KB loaded: {_kb_cache.get('product_count', 0)} products")
            return _kb_cache
        except Exception as e:
            print(f"Local KB error: {e}")
    try:
        with urllib.request.urlopen(GITHUB_KB_URL, timeout=20) as r:
            _kb_cache = json.loads(r.read().decode())
        print(f"GitHub KB loaded: {_kb_cache.get('product_count', 0)} products")
        return _kb_cache
    except Exception as e:
        print(f"GitHub KB error: {e}")
        return None


def get_most_expensive(currency: str):
    data = get_knowledge_base()
    if not data:
        return None
    best, best_price = None, 0
    for page in data.get("pages", []):
        if "/products/" not in page.get("url", ""):
            continue
        if page.get("currency", "") != currency:
            continue
        if _is_accessory(page):
            continue
        price = page.get("price", 0)
        if price > best_price:
            best_price = price
            best = page
    return best


def get_brand_history_links() -> str:
    """Build a brand-name -> /blogs/history/ URL map from KB, for system prompt injection."""
    data = get_knowledge_base()
    if not data:
        return ""
    lines = []
    for page in data.get("pages", []):
        url = page.get("url", "")
        if "/blogs/history/" not in url:
            continue
        # Extract brand name from first non-empty content line
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


def _is_accessory(page: dict) -> bool:
    for line in page.get("content", "").split("\n"):
        if line.startswith("Type:"):
            t = line.replace("Type:", "").strip().lower()
            if t in ACCESSORY_TYPES:
                return True
            if t == "watches":
                return False
    return False


def load_knowledge(query: str = "", currency: str = "CAD") -> str:
    data = get_knowledge_base()
    if not data:
        return "Knowledge base not available."

    currency = currency.upper()
    budget = extract_budget(query)
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
            if budget and page.get("price", 0) > budget:
                continue
            if _is_accessory(page):
                accessories.append(page)
            else:
                watches.append(page)
        elif is_article:
            articles.append(page)
        else:
            other_pages.append(page)

    print(f"[LOAD_KNOWLEDGE] currency={currency} | watches={len(watches)} | accessories={len(accessories)} | articles={len(articles)}")

    def score(page):
        text = (page.get("title", "") + " " + page.get("content", "")).lower()
        return sum(1 for kw in keywords if kw in text)

    is_accessory_query = any(w in query_lower for w in [
        "winder", "safe", "roll", "case", "strap", "accessory", "accessories", "storage"
    ])
    is_article_query = any(w in query_lower for w in [
        "article", "blog", "press", "news", "latest", "recent", "story", "post", "read"
    ])

    if is_accessory_query:
        pool = sorted(accessories, key=score, reverse=True) + sorted(watches, key=score, reverse=True)[:10]
    elif is_article_query:
        # Sort articles by published date descending so most recent come first
        def article_date(a):
            for line in a.get("content","").split("\n"):
                if line.startswith("Published:"):
                    return line.replace("Published:","").strip()
            return a.get("published","")
        sorted_articles = sorted(articles, key=article_date, reverse=True)
        pool = sorted_articles + other_pages + sorted(watches, key=score, reverse=True)[:5]
    else:
        if keywords:
            top = [w for w in sorted(watches, key=score, reverse=True) if score(w) > 0]
            rest = [w for w in watches if score(w) == 0]
            random.shuffle(rest)
            pool = top + rest + other_pages + articles
        else:
            shuffled = watches[:]
            random.shuffle(shuffled)
            pool = shuffled + other_pages + articles

    context = ""
    for page in pool:
        entry = f"\n\n--- {page['url']} ---\n{page['content']}"
        if len(context) + len(entry) > 22000:
            break
        context += entry
    return context


SYSTEM_PROMPT = """You are WatchBot, the AI assistant for WatchDNA.com — a global directory and community for watch lovers.

PERSONALITY: Passionate watch enthusiast, knowledgeable, direct, conversational, friendly. Never say "As an AI".

=== HOW TO ANSWER — MOST IMPORTANT RULE ===
- When asked a general question like "tell me about brands", "tell me about tradeshows", "tell me about contributors" — pick ONE interesting one and tell them about it in a conversational way. Do NOT list everything.
- End with something like "Want to hear about another one?" or "Ask me about a specific one!"
- Only give a full list if the user explicitly asks "list all", "what are all the", "show me all", etc.
- When asked about a specific item ("tell me about Dubai Watch Week", "tell me about Norqain") — give a rich, engaging paragraph about that one thing with a link. Not a bullet list.
- Keep responses concise and conversational — like a knowledgeable friend, not a directory.

=== LINK FORMAT — ABSOLUTE RULES ===
- Every link MUST be: [Descriptive Title](https://exact-url.com)
- Use the product/article TITLE as link text. NEVER "here", "View here", "Read article", "Check it out".
- ONLY use URLs from the WEBSITE CONTENT or hardcoded data below. Never construct or guess URLs.
- No fake links. One link per item. Never link the same item twice.

=== CURRENCY & PRODUCTS ===
- User's selected currency: {currency}
- ALL products in WEBSITE CONTENT are already filtered to only those available in the {currency} market.
- Show prices exactly as in the content. Do NOT convert or calculate.
- Only recommend products from WEBSITE CONTENT. Never invent product names or URLs.
- Format: [Product Name](url) — {symbol}X.XX {currency}

=== WATCH RECOMMENDATIONS — STRICT RULES ===
- If the user asks for watch recommendations and has NOT specified a currency in this conversation, ALWAYS ask first:
  "Which market would you like recommendations in? 🌍 CAD, USD, GBP, CHF, or EUR?"
- Once they pick a currency, recommend ONLY watches from that market (already filtered in content).
- NEVER recommend watches from a different currency than what was asked.
- ONLY recommend products with a /products/ URL from WEBSITE CONTENT — these are the only real store listings.
- NEVER recommend watches mentioned only in blog articles or press releases.
- When asked for accessories (winders, straps, safes), only recommend /products/ accessories — never watches.
- DWISS bracelets are WATCHES not accessories — never list them under accessories.
- Each time you give recommendations, vary your selections across different brands, price points, and styles.

=== BRANDS ===
- When asked about brands, ONLY talk about brands that appear on WatchDNA.
- For every brand mentioned, link directly to its WatchDNA page using BRAND LINKS below.
- If a brand has a /blogs/history/ page listed in BRAND LINKS, ALWAYS use that specific URL.
- If a brand has no /blogs/history/ page, link to https://watchdna.com/pages/brands-dna instead.
- Do NOT mention brands like Rolex, Patek Philippe, Omega, TAG Heuer, Seiko etc. unless they appear in BRAND LINKS or WEBSITE CONTENT.
- If asked about a brand not on WatchDNA, say it's not currently carried and suggest browsing https://watchdna.com/pages/brands-dna

BRAND LINKS:
{brand_links}

=== CONTRIBUTORS ===
- Use ONLY the CONTRIBUTORS DATA below to answer contributor questions.
- For each contributor mentioned, link to: https://watchdna.com/pages/contributors
- Format: [Contributor Name](https://watchdna.com/pages/contributors) — Role/bio

=== TRADESHOWS & AWARDS ===
- Use the TRADESHOWS DATA below — always list ALL of them with their links when asked.
- Never only show one or two. Never invent tradeshow names or URLs.

=== ARTICLES ===
- When asked for latest/recent articles, sort by the Published date in the content and show the most recent first.
- Format: [Article Title](exact-url) — by Author, Published: YYYY-MM-DD
- The Published date is in the article content under "Published:" — always use that exact date.
- If an article has no Published date in its content, omit the date entirely — NEVER invent or guess a date.
- ONLY use articles with a real /blogs/ URL. NEVER invent titles, dates, or URLs.

=== STORE LOCATOR ===
- Give them the link: https://watchdna.com/tools/storelocator

KEY PAGES:
- All Watches: https://watchdna.com/collections/watches
- Store Locator: https://watchdna.com/tools/storelocator
- Brands Directory: https://watchdna.com/pages/brands-dna
- Contributors: https://watchdna.com/pages/contributors
- Watch Enthusiast Blog: https://watchdna.com/blogs/watch-enthusiast
- Press Releases: https://watchdna.com/blogs/press

CONTRIBUTORS DATA:
{contributors}

TRADESHOWS DATA:
{tradeshows}

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
    text_upper = text.upper()
    for cur in VALID_CURRENCIES:
        if re.search(r"\b" + cur + r"\b", text_upper):
            return cur
    word_map = [
        (r"\bEUROS?\b",               "EUR"),
        (r"\bPOUNDS?\b|\bSTERLING\b", "GBP"),
        (r"\bSWISS\b|\bFRANCS?\b",    "CHF"),
    ]
    for pattern, cur in word_map:
        if re.search(pattern, text_upper):
            return cur
    return None


def resolve_currency(req: "ChatRequest") -> str:
    found = detect_currency_in_text(req.message)
    if found:
        return found
    for h in reversed(req.history):
        if h.get("role") != "user":
            continue
        found = detect_currency_in_text(h.get("content", ""))
        if found:
            return found
    widget = req.currency.upper().strip()
    if widget in VALID_CURRENCIES:
        return widget
    return "CAD"


@app.post("/chat")
async def chat(req: ChatRequest):
    currency = resolve_currency(req)
    print(f"[CURRENCY] {currency} | {req.message[:60]}")

    symbol = CURRENCY_SYMBOLS.get(currency, "$")
    knowledge = load_knowledge(req.message, currency=currency)

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

    brand_links = get_brand_history_links()

    system = SYSTEM_PROMPT.format(
        currency=currency,
        symbol=symbol,
        contributors=CONTRIBUTORS,
        tradeshows=TRADESHOWS,
        store_links=store_links,
        brand_links=brand_links,
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
    currency = resolve_currency(req)
    data = get_knowledge_base()
    all_products = [p for p in data.get("pages", []) if "/products/" in p.get("url", "")]
    matching = [p for p in all_products if p.get("currency", "") == currency and not _is_accessory(p)]
    currencies_in_kb = list(set(p.get("currency", "MISSING") for p in all_products))
    return {
        "resolved_currency": currency,
        "watches_for_currency": len(matching),
        "all_currencies_in_kb": sorted(currencies_in_kb),
        "sample_products": [{"title": p["title"], "price": p["price"]} for p in matching[:5]]
    }


@app.get("/health")
async def health():
    kb_exists = Path(KNOWLEDGE_FILE).exists()
    last_scraped, product_count = None, 0
    if kb_exists:
        with open(KNOWLEDGE_FILE) as f:
            kb = json.load(f)
            last_scraped = kb.get("scraped_at")
            product_count = kb.get("product_count", 0)
    return {"status": "ok", "knowledge_base": kb_exists, "last_scraped": last_scraped, "product_count": product_count}
