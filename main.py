from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from openai import OpenAI
import json, os, re, csv, io, urllib.request
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


def load_knowledge(query: str = "", currency: str = "CAD") -> str:
    data = get_knowledge_base()
    if not data:
        return "Knowledge base not available."

    currency = currency.upper()
    budget = extract_budget(query)
    keywords = [w for w in query.lower().split() if len(w) > 2]

    filtered = []
    for page in data.get("pages", []):
        is_product = "/products/" in page.get("url", "")
        if is_product:
            # Only include products available in the user's currency market
            if page.get("currency", "") != currency:
                continue
            # Filter by budget
            if budget and page.get("price", 0) > budget:
                continue
        filtered.append(page)

    def score(page):
        text = (page.get("title", "") + " " + page.get("content", "")).lower()
        return sum(1 for kw in keywords if kw in text)

    if keywords:
        scored = sorted(filtered, key=score, reverse=True)
        relevant = scored[:30]
        general = [p for p in filtered if p not in relevant][:10]
        ordered = relevant + general
    else:
        ordered = filtered

    context = ""
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

=== ARTICLES — ASK WHICH SECTION ONLY FOR ARTICLE REQUESTS ===
- ONLY ask "Which section?" when the user is clearly asking for articles, blog posts, or news.
- Do NOT ask "Which section?" for awards, tradeshows, brands, products, stores, or anything else.
- When asked for articles: "Which section? 👉 [Watch Enthusiast](https://watchdna.com/blogs/watch-enthusiast) (community stories) or [Press Releases](https://watchdna.com/blogs/press) (brand announcements)?"
- Once they pick: list most recent from that section, newest first.
- Format: [Article Title](exact-url) — by Author, Published: YYYY-MM-DD
- ONLY use articles from WEBSITE CONTENT. Never invent titles, authors, dates, or URLs.

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


@app.post("/chat")
async def chat(req: ChatRequest):
    # Determine currency — message overrides widget setting
    currency = req.currency.upper() if req.currency.upper() in VALID_CURRENCIES else "CAD"
    for cur in VALID_CURRENCIES:
        if cur.lower() in req.message.lower():
            currency = cur
            break
    # Check history if message didn't specify
    if currency == (req.currency.upper() if req.currency.upper() in VALID_CURRENCIES else "CAD"):
        for h in req.history[-6:]:
            for cur in VALID_CURRENCIES:
                if cur.lower() in h.get("content", "").lower():
                    currency = cur
                    break

    symbol = CURRENCY_SYMBOLS.get(currency, "$")
    knowledge = load_knowledge(req.message, currency=currency)

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


@app.get("/health")
async def health():
    kb_exists = Path(KNOWLEDGE_FILE).exists()
    last_scraped = None
    if kb_exists:
        with open(KNOWLEDGE_FILE) as f:
            last_scraped = json.load(f).get("scraped_at")
    return {"status": "ok", "knowledge_base": kb_exists, "last_scraped": last_scraped}
