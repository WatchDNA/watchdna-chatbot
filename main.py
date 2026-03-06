from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from openai import OpenAI
import json
import os
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


def load_knowledge(query: str = "") -> str:
    data = get_knowledge_base()
    if not data:
        return "Knowledge base not available."

    pages = data.get("pages", [])
    query_lower = query.lower()
    keywords = [w for w in query_lower.split() if len(w) > 2]

    def score(page):
        text = (page.get("title", "") + " " + page.get("content", "")).lower()
        return sum(1 for kw in keywords if kw in text)

    if keywords:
        scored = sorted(pages, key=score, reverse=True)
        relevant = scored[:20]
        general = [p for p in pages if p not in relevant][:15]
        ordered = relevant + general
    else:
        ordered = pages

    context = ""
    for page in ordered:
        entry = f"\n\n--- {page['url']} ---\n{page['content']}"
        if len(context) + len(entry) > 18000:
            break
        context += entry

    return context


SYSTEM_PROMPT = """You are WatchBot, the AI assistant for WatchDNA.com — a global directory and community for watch lovers, run by Northern Watch Services Inc.

PERSONALITY:
- Passionate watch enthusiast — knowledgeable, direct, friendly
- Talk like a person, not a customer service bot
- Use proper watch terminology naturally

RESPONSE RULES:
- Keep answers SHORT. 2-4 sentences max for simple questions.
- Be direct. Lead with the actual answer.
- Never start with "As an AI..." — just answer.
- ALWAYS format links as markdown: [Link Text](https://full-url.com) so they are clickable.
- Every product mention should include its WatchDNA link if available.

SITE-FIRST RULES:
- Watch recommendations: ONLY recommend watches available on WatchDNA (in the content below). Never recommend watches not on the site.
- Brand/model info: Use site data first, then general knowledge only if site has nothing.
- Articles/news: Use article content from site data below.
- Store locations: Use the STORE LOCATOR INFO provided. Always give the direct filtered link for the brand.

STORE LOCATOR RULES — IMPORTANT:
- When someone asks for dealers/stores for a specific brand, use the STORE LOCATOR LINKS below to give them the exact filtered URL.
- If a location is mentioned, tell them to use the link and filter by their area on the map.
- If a brand isn't in the store locator list, direct them to https://watchdna.com/tools/storelocator
- Always present store locator links as clickable markdown links.
- The store locator is always up to date — new stores added by WatchDNA appear automatically.

STRICT TOPIC LIMITS:
- Only refuse questions CLEARLY unrelated to watches (sports scores, cooking, movies, politics, coding).
- When in doubt, answer it.

KEY PAGES:
- All Timepieces: https://watchdna.com/collections/watches
- Store Locator: https://watchdna.com/tools/storelocator
- Brands Directory: https://watchdna.com/pages/brands-dna
- Watchmaking 101: https://watchdna.com/pages/watchmaking101
- Authorized Dealers Directory: https://watchdna.com/tools/storelocator/directory

STORE LOCATOR LINKS BY BRAND:
{store_links}

WATCHDNA WEBSITE CONTENT (always use this first):
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
        if k == v['name'].lower()  # deduplicate
    ])

    # Check if user is asking about a specific brand's stores
    brand_match = find_brand_in_query(req.message)
    store_hint = ""
    if brand_match and any(w in req.message.lower() for w in ["store", "dealer", "buy", "near", "where", "find", "location", "authorized"]):
        store_hint = f"\n\nNOTE: User is asking about {brand_match['name']} dealers. Direct them to: {brand_match['url']}"
        if req.location:
            store_hint += f" — their location is {req.location}, tell them to use the map filter on that page."

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
        max_tokens=300,
        temperature=0.7,
    )
    return {"reply": response.choices[0].message.content}


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






