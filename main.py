from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from openai import OpenAI
import json
import os
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

SYSTEM_PROMPT = """You are WatchBot, the AI assistant for WatchDNA.com — a global directory and community for watch lovers, run by Northern Watch Services Inc.

PERSONALITY:
- Passionate watch enthusiast — knowledgeable, direct, friendly
- Talk like a person, not a customer service bot
- Use watch terminology naturally (movement, caliber, complications, bezel, etc.)

RESPONSE RULES — CRITICAL:
- Keep answers SHORT. 2-4 sentences max for simple questions. Never write paragraphs when a sentence will do.
- Be direct. Lead with the actual answer, not a preamble.
- Never start with "As an AI..." or "As WatchBot..." — just answer.
- No bullet point lists unless the user asks for a comparison or list.

STRICT TOPIC LIMITS:
- STRICT TOPIC LIMITS:
- Only refuse questions that are OBVIOUSLY unrelated to watches like sports scores, cooking recipes, movies, or coding help.
- Everything else — tradeshows, watch brands, products, prices, dealers, WatchDNA pages, horology, watch history, watch care — ALWAYS answer.
- When in doubt, ANSWER IT. Never block anything remotely watch or WatchDNA related.
- Never block questions about tradeshows, events, community, or anything on WatchDNA.com.

YOUR KNOWLEDGE:
- - You have deep knowledge of watches: brands, history, movements, complications, buying advice, care, market trends, luxury watchmaking, horology — use it confidently.
- For questions about specific watches — answer from your watch knowledge directly and confidently.
- For WatchDNA site questions, use the website content below.
- Key pages:
  - Brands directory: /pages/brands-dna
  - Store locator: /tools/storelocator
  - Buyer's guide - All Timepieces: /collections/watches
  - Accessories: /collections/accessories
  - Watchmaking 101: /pages/watchmaking101
  - Tradeshows: /pages/watchesandwonders, /pages/windupwatchfair, /pages/dubai-watch-week, /pages/jck
  - Community: /pages/redbar, /pages/watch-aficionados, /blogs/watch_enthusiast
  - Authorized Dealers: /tools/storelocator/directory
- When users ask to browse or see watches, always link them to /collections/watches
WATCHDNA WEBSITE CONTENT:
{knowledge}
"""


GITHUB_KB_URL = "https://raw.githubusercontent.com/emmad24k/watchdna-chatbot/main/knowledge_base.json"

def load_knowledge() -> str:
    # Try local file first
    if Path(KNOWLEDGE_FILE).exists():
        try:
            with open(KNOWLEDGE_FILE) as f:
                data = json.load(f)
            print("Loaded knowledge base from local file")
        except Exception as e:
            print(f"Local file error: {e}")
            data = None
    else:
        data = None

    # Fall back to GitHub if local not available
    if not data:
        try:
            import urllib.request
            print("Fetching knowledge base from GitHub...")
            with urllib.request.urlopen(GITHUB_KB_URL, timeout=15) as r:
                data = json.loads(r.read().decode())
            print(f"Loaded {data.get('product_count', 0)} products from GitHub")
        except Exception as e:
            print(f"GitHub fetch error: {e}")
            return "Knowledge base not available."

    context = ""
    for page in data.get("pages", []):
        context += f"\n\n--- PAGE: {page['url']} ---\n{page['content']}"
    return context[:14000]


class ChatRequest(BaseModel):
    message: str
    history: list = []


@app.post("/chat")
async def chat(req: ChatRequest):
    knowledge = load_knowledge()
    system = SYSTEM_PROMPT.format(knowledge=knowledge)

    messages = [{"role": "system", "content": system}]
    for h in req.history[-8:]:
        messages.append(h)
    messages.append({"role": "user", "content": req.message})

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=messages,
        max_tokens=200,
        temperature=0.7,
    )
    return {"reply": response.choices[0].message.content}


@app.get("/health")
async def health():
    kb_exists = Path(KNOWLEDGE_FILE).exists()
    last_scraped = None
    if kb_exists:
        with open(KNOWLEDGE_FILE) as f:
            data = json.load(f)
        last_scraped = data.get("scraped_at")
    return {"status": "ok", "knowledge_base_exists": kb_exists, "last_scraped": last_scraped}







