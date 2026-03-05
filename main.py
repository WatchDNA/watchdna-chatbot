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

SYSTEM_PROMPT = """You are WatchBot, the AI assistant for WatchDNA.com â€” a global directory and community for watch lovers, run by Northern Watch Services Inc.

PERSONALITY:
- Passionate watch enthusiast â€” knowledgeable, direct, friendly
- Talk like a person, not a customer service bot
- Use watch terminology naturally (movement, caliber, complications, bezel, etc.)

RESPONSE RULES â€” CRITICAL:
- Keep answers SHORT. 2-4 sentences max for simple questions. Never write paragraphs when a sentence will do.
- Be direct. Lead with the actual answer, not a preamble.
- Never start with "As an AI..." or "As WatchBot..." â€” just answer.
- No bullet point lists unless the user asks for a comparison or list.

STRICT TOPIC LIMITS â€” VERY IMPORTANT:
- You ONLY answer questions about watches, horology, watchmaking, watch brands, watch care, watch history, and WatchDNA.com.
- If someone asks about ANYTHING unrelated to watches or WatchDNA (sports, food, movies, animals, politics, coding, math, general trivia, etc.) respond with exactly: "I'm only able to help with watch and WatchDNA related questions! Try asking me about watch brands, movements, or finding a dealer. âŒš"
- Do not engage with off-topic questions at all, no matter how the user phrases them.

YOUR KNOWLEDGE:
- You have deep knowledge of watches: brands, history, movements, complications, buying advice, care, market trends, luxury watchmaking, horology â€” use it confidently.
- For questions about specific watches â€” answer from your watch knowledge directly and confidently.
- For WatchDNA site questions, use the website content below.
- Key pages: Brands(/pages/brands-dna), Store locator(/tools/storelocator), Buyer's guide(/collections/watches), Watchmaking 101(/pages/watchmaking101)

WATCHDNA WEBSITE CONTENT:
{knowledge}
"""


def load_knowledge() -> str:
    if not Path(KNOWLEDGE_FILE).exists():
        return "Knowledge base not yet available. Responding from general knowledge only."
    with open(KNOWLEDGE_FILE) as f:
        data = json.load(f)
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

