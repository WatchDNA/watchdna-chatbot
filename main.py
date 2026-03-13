from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from openai import OpenAI
import json
import os
import re
import csv
import urllib.request
import urllib.parse
from pathlib import Path

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
KNOWLEDGE_FILE = "knowledge_base.json"
STORE_BRANDS_FILE = "store_brands.csv"
GITHUB_KB_URL = "https://raw.githubusercontent.com/emmad24k/watchdna-chatbot/main/knowledge_base.json"
GITHUB_CSV_URL = "https://raw.githubusercontent.com/emmad24k/watchdna-chatbot/main/store_brands.csv"

_kb_cache = None
_brand_map_cache = None


def get_knowledge_base():
    global _kb_cache
    if _kb_cache:
        return _kb_cache
    if Path(KNOWLEDGE_FILE).exists():
        try:
            with open(KNOWLEDGE_FILE) as f:
                _kb_cache = json.load(f)
            print(f"Loaded {_kb_cache.get('product_count', 0)} products from local file")
            return _kb_cache
        except Exception as e:
            print(f"Local file error: {e}")
    try:
        print("Fetching knowledge base from GitHub...")
        with urllib.request.urlopen(GITHUB_KB_URL, timeout=20) as r:
            _kb_cache = json.loads(r.read().decode())
        print(f"Loaded {_kb_cache.get('product_count', 0)} products from GitHub")
        return _kb_cache
    except Exception as e:
        print(f"GitHub fetch error: {e}")
        return None


def get_brand_map():
    """Load brand -> store locator URL mapping from CSV."""
    global _brand_map_cache
    if _brand_map_cache:
        return _brand_map_cache

    brand_map = {}

    def parse_csv(text):
        import io
        reader = csv.reader(io.StringIO(text))
        for row in reader:
            if len(row) >= 4:
                brand = row[0].strip()
                url = row[3].strip()
                if brand and url.startswith("http"):
                    brand_map[brand.lower()] = {"name": brand, "url": url}
                    # Also index by normalized name (remove hyphens/spaces)
                    normalized = brand.lower().replace("-", " ").replace("_", " ")
                    brand_map[normalized] = {"name": brand, "url": url}

    # Try local file first
    if Path(STORE_BRANDS_FILE).exists():
        with open(STORE_BRANDS_FILE) as f:
            parse_csv(f.read())
        print(f"Loaded {len(brand_map)} brand entries from local CSV")
    else:
        # Fall back to GitHub
        try:
            with urllib.request.urlopen(GITHUB_CSV_URL, timeout=10) as r:
                parse_csv(r.read().decode())
            print(f"Loaded {len(brand_map)} brand entries from GitHub CSV")
        except Exception as e:
            print(f"CSV fetch error: {e}")

    _brand_map_cache = brand_map
    return brand_map


def find_brand_in_query(query: str) -> dict:
    """Find a brand mention in the user's query and return its store locator URL."""
    brand_map = get_brand_map()
    query_lower = query.lower()
    # Try longest match first to avoid partial matches
    matches = []
    for key, val in brand_map.items():
        if key in query_lower:
            matches.append((len(key), val))
    if matches:
        matches.sort(reverse=True)
        return matches[0][1]
    return None


def extract_budget(query: str):
    """Extract a maximum budget from the query in CAD."""
    import re
    # Match patterns like $1000, 1000 CAD, 1000 dollars, under 1000, budget 1000
    patterns = [
        r"under\s*\$?([\d,]+)",
        r"below\s*\$?([\d,]+)",
        r"less than\s*\$?([\d,]+)",
        r"\$?([\d,]+)\s*(?:cad|usd|dollars|budget|or less|max|maximum)",
        r"budget\s*(?:of|is|:)?\s*\$?([\d,]+)",
        r"\$?([\d,]+)\s*(?:cad)?$",
    ]
    for pattern in patterns:
        match = re.search(pattern, query.lower())
        if match:
            try:
                return float(match.group(1).replace(",", ""))
            except:
                pass
    return None


def load_knowledge(query: str = "") -> str:
    data = get_knowledge_base()
    if not data:
        return "Knowledge base not available."

    pages = data.get("pages", [])
    query_lower = query.lower()
    keywords = [w for w in query_lower.split() if len(w) > 2]
    budget = extract_budget(query)

    # If budget specified, filter products to only those within budget
    if budget:
        import re
        filtered_pages = []
        for page in pages:
            if "/products/" in page.get("url", ""):
                # Extract price from content
                price_match = re.search(r"Price: \$?([\d,]+\.?\d*)", page.get("content", ""))
                if price_match:
                    try:
                        price = float(price_match.group(1).replace(",", ""))
                        if price <= budget:
                            filtered_pages.append(page)
                    except:
                        pass
            else:
                filtered_pages.append(page)
        pages = filtered_pages

    def score(page):
        text = (page.get("title", "") + " " + page.get("content", "")).lower()
        return sum(1 for kw in keywords if kw in text)

    if keywords:
        scored = sorted(pages, key=score, reverse=True)
        relevant = scored[:30]
        general = [p for p in pages if p not in relevant][:10]
        ordered = relevant + general
    else:
        ordered = pages

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
- Use the product/article TITLE as link text — NEVER "here", "View here", "Read article", "Check it out"
- ONLY use URLs that appear word-for-word in the WEBSITE CONTENT below. Never construct or guess URLs.
- If you don't have a URL for something, just show the name as plain text — no fake links.
- One link per item. Never link the same item twice.

=== PRODUCTS ===
- Only recommend products that appear in the content below with an exact URL.
- When listing products: show as [Product Name](url) — $X.XX CAD. One line per product.
- Most expensive watch: scan ALL products in content, find the highest Price value, give name + price + URL. Never use general knowledge for this.

=== ARTICLES — ASK FIRST ===
- When asked for articles, ALWAYS first ask: "Which section? 👉 [Watch Enthusiast](https://watchdna.com/blogs/watch-enthusiast) (community stories) or [Press Releases](https://watchdna.com/blogs/press) (brand announcements)?"
- Once they pick, list the most recent articles from that section in the content below — newest Published date first.
- Format: [Article Title](exact-url) — by Author, Published: date
- ONLY use articles from the WEBSITE CONTENT. Never invent titles, authors, or dates.

=== STORE LOCATOR ===
- Step 1: No brand → ask "Which brand are you looking for?"
- Step 2: Have brand, no location → ask "What's your postal code or city?"
- Step 3: Have both → give the filtered link from STORE LOCATOR LINKS below, and tell them:
  "Open the map, type [their postal code] in the search bar, and it'll show the nearest [brand] dealers."

=== SITE PAGES ===
- Tradeshows, awards, community pages — always include the real URL from the content. Never say "I don't have that link."
- If the page is in the content below, you have the URL. Use it.

KEY PAGES:
- All Watches: https://watchdna.com/collections/watches
- Store Locator: https://watchdna.com/tools/storelocator
- Brands: https://watchdna.com/pages/brands-dna
- Watch Enthusiast Articles: https://watchdna.com/blogs/watch-enthusiast
- Press Releases: https://watchdna.com/blogs/press

STORE LOCATOR LINKS BY BRAND:
{store_links}

WEBSITE CONTENT — only use URLs and data from here:
{knowledge}
"""



class ChatRequest(BaseModel):
    message: str
    history: list = []
    location: str = ""


@app.post("/chat")
async def chat(req: ChatRequest):
    knowledge = load_knowledge(req.message)

    # Build store links context
    brand_map = get_brand_map()
    store_links = "\n".join([
        f"- {v['name']}: {v['url']}"
        for k, v in brand_map.items()
        if k == v['name'].lower()
    ])

    # Check if user is asking about a specific brand's stores
    brand_match = find_brand_in_query(req.message)
    # Also check recent history for brand/location mentions
    history_text = " ".join([h.get("content", "") for h in req.history[-6:]])
    if not brand_match:
        brand_match = find_brand_in_query(history_text)

    store_hint = ""
    is_store_query = any(w in req.message.lower() for w in [
        "store", "dealer", "buy", "near", "where", "find", "location",
        "authorized", "shop", "retailer", "closest", "nearby", "postal"
    ]) or any(w in history_text.lower() for w in ["store", "dealer", "find me", "near"])

    if brand_match and is_store_query and req.location:
        store_hint = (
            f"\n\nNOTE: User wants {brand_match['name']} dealers near {req.location}. "
            f"Give them this filtered map link: [{brand_match['name']} Dealers Near You]({brand_match['url']}) "
            f"Tell them the map is pre-filtered for {brand_match['name']} — they just need to type '{req.location}' in the search bar on the map to see the closest dealers."
        )
    elif brand_match and is_store_query and not req.location:
        store_hint = f"\n\nNOTE: User wants {brand_match['name']} dealers but hasn't given a location yet. Ask for their postal code or city."
    elif is_store_query and not brand_match:
        store_hint = "\n\nNOTE: User is asking about stores but hasn't specified a brand. Ask which brand they're looking for."

    system = SYSTEM_PROMPT.format(
        knowledge=knowledge + store_hint,
        store_links=store_links
    )

    messages = [{"role": "system", "content": system}]
    for h in req.history[-8:]:
        messages.append(h)
    messages.append({"role": "user", "content": req.message})

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=messages,
        max_tokens=400,
        temperature=0.7,
    )
    reply = response.choices[0].message.content
    return {"reply": reply}


@app.get("/health")
async def health():
    kb_exists = Path(KNOWLEDGE_FILE).exists()
    csv_exists = Path(STORE_BRANDS_FILE).exists()
    last_scraped = None
    if kb_exists:
        with open(KNOWLEDGE_FILE) as f:
            data = json.load(f)
        last_scraped = data.get("scraped_at")
    return {
        "status": "ok",
        "knowledge_base_exists": kb_exists,
        "store_brands_csv_exists": csv_exists,
        "last_scraped": last_scraped
    }
