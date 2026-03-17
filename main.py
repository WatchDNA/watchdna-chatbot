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
        if any(kw in title for kw in ["winder", "safe", "roll", "box", "case", "strap",
                                       "organizer", "organiser", "storage", "pouch"]):
            return True
    return False


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
        if _is_accessory(page):
            continue
        if page.get("price", 0) == 0:
            continue
        price = page.get("price", 0)
        if price > best_price:
            best_price = price
            best = page
    return best


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
    is_brand_query = any(w in query_lower for w in [
        "brand", "brands", "about", "history", "founded", "company", "who makes",
        "canadian", "swiss", "german", "french", "japanese", "american", "italian",
        "british", "danish", "norwegian", "greek", "georgian", "australian", "spanish",
        "country", "group", "groups", "cartier", "rolex", "omega", "seiko", "breitling",
        "hublot", "tag heuer", "patek", "tudor", "longines", "tissot", "rado",
        "hamilton", "certina", "mido", "norqain", "fortis", "luminox", "casio",
        "movado", "citizen", "bulova", "accutron", "alpina", "bering", "dwiss"
    ])

    if is_accessory_query:
        pool = sorted(accessories, key=score, reverse=True) + sorted(watches, key=score, reverse=True)[:10]
    elif is_article_query:
        def article_date(a):
            for line in a.get("content", "").split("\n"):
                if line.startswith("Published:"):
                    return line.replace("Published:", "").strip()
            return a.get("published", "")
        pool = sorted(articles, key=article_date, reverse=True) + other_pages
    elif is_brand_query:
        # Put brands-dna, history pages, and groups page first
        brand_pages = [p for p in other_pages if any(x in p.get("url","") for x in
                       ["/blogs/history/", "brands-dna", "/pages/groups"])]
        rest_pages = [p for p in other_pages if p not in brand_pages]
        keyword_watches = sorted(watches, key=score, reverse=True)[:10]
        pool = brand_pages + rest_pages + keyword_watches + articles
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

CANADA: Ferro & Company, FIORI, Héron, Jakob Eitan, José Cermeño Montréal, Makoto Watch Company,
Noctua Watches, Locke & King, Marathon, Redwood, Shelby Watch Co., SOLIOS, SOVRYGN,
Tessé Watches, Thacker Merali, UNISON, VIEREN, Whitby Watch Co., Wilk Watchworks, Worden Watch Studio, ZENEA

SWITZERLAND: A. Lange & Söhne, Alpina, Arnold & Son, Audemars Piguet, Ball, Baume et Mercier,
Blancpain, Breguet, Breitling, Carl F. Bucherer, Cartier, Certina, Chopard, Christiaan Van Der Klaauw,
Christopher Ward, Chronoswiss, Concord, Corum, Czapek & Cie, Doxa, Ebel, Eterna, Fortis,
Franck Muller, Frédérique Constant, Girard Perregaux, H. Moser & Cie., Hamilton, Hautlence,
Hublot, HYT, IWC Schaffhausen, Jaeger-LeCoultre, Jaquet Droz, Laurent Ferrier, Longines,
Louis Erard, Luminox, Maurice Lacroix, MB&F, Mido, Mondaine, Montblanc, Norqain, Omega,
Oris, Panerai, Parmigiani Fleurier, Patek Philippe, Piaget, Rado, Raymond Weil,
Richard Mille, Rolex, Swatch, TAG Heuer, Tissot, Tudor, Ulysse Nardin, Vacheron Constantin,
Victorinox, Vulcain, Zenith

GERMANY: A. Lange & Söhne, Glashütte Original, Hanhart, Iron Annie, Junghans, Junkers,
Laco, MeisterSinger, Montblanc, Nomos Glashütte, Sinn Spezialuhren, Union Glashütte, Zeppelin

JAPAN: Casio, Citizen, G-Shock, Grand Seiko, Oceanus, Seiko

FRANCE: Bell & Ross, Cartier, Chaumet, Hermès, Lip, Louis Moinet

UNITED STATES: Accutron, Bulova, Fossil, Hamilton, Invicta, Marathon, MVMT, Shinola, Timex

UNITED KINGDOM: Bremont, Christopher Ward, Fears Bristol

DENMARK: Bering, Skagen

ITALY: Panerai, U-Boat, Locman

AUSTRALIA: RZE

SPAIN: Festina

GEORGIA: Tsikolia

NORWAY: Straum

GREECE: Sphaera

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

=== ACCESSORIES ===
- When asked about watch accessories (cases, winders, straps, boxes etc.) link to: [Watch Accessories](https://watchdna.com/collections/accessories)
- If there are accessory products in WEBSITE CONTENT, recommend them with price and link.
- Accessories are currency-specific just like watches — only show ones matching the user's currency.

WATCH RECOMMENDATION FLOW — CRITICAL:
- If the user asks for watch recommendations and has NOT specified a currency in this conversation, ALWAYS ask first:
  "Which market would you like recommendations in? 🌍 CAD, USD, GBP, CHF, or EUR?"
- Once they pick a currency, recommend ONLY watches from that market (already filtered in content).
- NEVER recommend watches from a different currency than what was asked — the same watch has different entries per market and only the correct one will work.
- When the user asks for watches by feature (movement, case size, style, color, material etc.) ONLY recommend watches whose Features section in WEBSITE CONTENT matches. Examples:
  - "automatic movement" → only watches with "Movement: Automatic" in Features
  - "chronograph" → only watches with "Styles: Chronograph" in Features (NOT just mentioned in description)
  - "43mm" → only watches with "Case Size: 43" in Features
  - "pilot style" → only watches with "Styles: Pilot" in Features
  - "recycled steel" → only watches with "Case Material: Recycled Steel" in Features
- If no watches match the requested feature, say so honestly rather than recommending ones that don't match.

=== BRANDS ===
- ONLY use facts from WEBSITE CONTENT for every brand. NEVER use your training knowledge to fill in founding dates, founders, countries, stories, or any other brand facts.
- Every brand you mention MUST have a link. Use the URL pattern: https://watchdna.com/blogs/history/[brand-slug] (e.g. Cartier → https://watchdna.com/blogs/history/cartier, Rolex → https://watchdna.com/blogs/history/rolex). If the brand has no history page, link to https://watchdna.com/pages/brands-dna
- If a brand is not in WEBSITE CONTENT at all, say it's not currently on WatchDNA and link to https://watchdna.com/pages/brands-dna
- If WEBSITE CONTENT has no detail about a brand beyond its products, say "I don't have detailed background info on this brand yet" — never invent facts.
- When asked about brands from a specific country, use BRANDS BY COUNTRY data — it has the confirmed list.
- When asked about brand groups (Casio Group, Swatch Group, LVMH etc.) always add: "You can explore all brand groups at [Brand Groups](https://watchdna.com/pages/groups)"

BRAND LINKS:
{brand_links}

=== ARTICLES ===
- When asked for latest/recent articles, use WEBSITE CONTENT — articles are sorted newest first.
- Pick the first article with a "Published:" date in its content.
- Format: [Article Title](url) — Published: YYYY-MM-DD
- ONLY use the exact "Published:" date from the article content. NEVER invent dates.
- ONLY use articles with a real /blogs/ URL. NEVER invent titles or URLs.

=== TRADESHOWS ===
- Use the TRADESHOWS DATA below for all tradeshow info and links.
- Follow the HOW TO ANSWER rule: pick one and describe it conversationally unless user asks for the full list.
- Never invent tradeshow names or URLs.

=== AWARDS ===
- Use the AWARDS DATA below for all award info and links.
- Always link to the award page when mentioned.

=== CONTRIBUTORS ===
- Use ONLY the CONTRIBUTORS DATA below to answer contributor questions.
- Each contributor has their own individual URL — always use that specific URL.
- Format: [Full Name](their-individual-url) — Role/bio

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
    knowledge = load_knowledge(req.message, currency=currency)
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
        store_hint = "\n\nNOTE: Give user the store locator link directly: [Find a Store](https://watchdna.com/tools/storelocator) — tell them to search by brand or city on the map."

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
