# WatchDNA Chatbot — Full Deployment Guide
**Stack: Python + Railway (free) + GitHub Actions (free) + OpenAI API**

---

## What You Need Before Starting
- A GitHub account (free) → github.com
- A Railway account (free) → railway.app
- Your OpenAI API key → platform.openai.com

---

## STEP 1 — Upload Files to GitHub (10 min)

1. Go to **github.com** → click **"New"** (green button) → create a repo called `watchdna-chatbot` → set to **Public** → click **"Create repository"**

2. On the next screen click **"uploading an existing file"**

3. Upload these files:
   - `main.py`
   - `scraper.py`
   - `requirements.txt`
   - `railway.toml`
   - `shopify-widget.html`

4. Click **"Commit changes"**

5. Now add the GitHub Actions workflow file. This one needs to be in a specific folder:
   - Click **"Add file"** → **"Create new file"**
   - In the filename box at the top type exactly: `.github/workflows/daily-scraper.yml`
   - GitHub will auto-create the folders as you type the slashes
   - Copy and paste the entire contents of `daily-scraper.yml` into the editor
   - Click **"Commit changes"**

✅ Your repo is ready.

---

## STEP 2 — Deploy on Railway (10 min)

1. Go to **railway.app** → Sign up with your GitHub account (free, no credit card needed to start)

2. Click **"New Project"** → **"Deploy from GitHub repo"**

3. Select your `watchdna-chatbot` repo

4. Railway will detect the `railway.toml` file and configure automatically

5. Once it appears in the dashboard, click on your service → go to **"Variables"** tab → click **"New Variable"** and add:
   - `OPENAI_API_KEY` = your OpenAI key

6. Go to **"Settings"** tab → scroll to **"Networking"** → click **"Generate Domain"**
   - You'll get a free URL like: `watchdna-chatbot-production.up.railway.app`
   - **Copy this URL — you'll need it in Step 4**

7. Go to **"Deployments"** tab and watch the build log. It will:
   - Install Python packages
   - Run the scraper (scrapes watchdna.com — takes ~2 min)
   - Start the API server

✅ Your backend is live.

---

## STEP 3 — Set Up Free Daily Scraping (5 min)

The scraper needs to re-run every day to keep the chatbot up to date. GitHub Actions does this for free.

1. In Railway: go to your service → **"Settings"** → scroll down to **"Deploy Webhook"** → click to reveal it → **copy the URL**

2. In GitHub: go to your `watchdna-chatbot` repo → **"Settings"** tab → left sidebar: **"Secrets and variables"** → **"Actions"** → **"New repository secret"**
   - Name: `RAILWAY_DEPLOY_HOOK`
   - Secret: paste the Railway webhook URL
   - Click **"Add secret"**

3. Test it: go to your repo → **"Actions"** tab → **"Daily WatchDNA Scraper"** → **"Run workflow"** → **"Run workflow"** (green button)
   - Watch it run — should take about 2-3 minutes
   - Green checkmark = working ✅

After this, it will automatically run every day at 4 AM UTC with no effort from you.

---

## STEP 4 — Add the Chat Widget to Shopify (10 min)

1. Log into **Shopify admin** → **"Online Store"** → **"Themes"**

2. Next to your active theme click **"..."** → **"Edit code"**

3. In the left file list, open **`layout/theme.liquid`**

4. Open `shopify-widget.html` from the files I gave you and copy everything inside it

5. In `theme.liquid`, scroll to the very bottom and find `</body>`

6. Paste the widget code **just before** `</body>`

7. Inside the pasted code, find this line:
   ```
   const BACKEND_URL = "YOUR_BACKEND_URL";
   ```
   Replace `YOUR_BACKEND_URL` with your Railway URL from Step 2, like:
   ```
   const BACKEND_URL = "https://watchdna-chatbot-production.up.railway.app";
   ```

8. Click **"Save"** (top right)

---

## STEP 5 — Test It

Visit **watchdna.com** — you should see a dark chat bubble with a gold clock icon in the bottom-right corner.

Try these test questions:
- *"What is WatchDNA?"*
- *"How do I find an authorized dealer near me?"*
- *"What watch brands are listed on the site?"*
- *"What is a tourbillon?"*
- *"Tell me about Watch & Wonders"*
- *"What's in the buyer's guide?"*

---

## Cost Summary

| Service | Cost |
|---------|------|
| GitHub (repo + Actions) | **$0 — free forever** |
| Railway hosting | **$0 — free $5 credits/month, your app uses ~$0.50** |
| OpenAI API (gpt-3.5-turbo) | **~$0.01–0.05/day** depending on traffic |

The only real cost is OpenAI API usage — pennies per day at normal traffic levels.

---

## Troubleshooting

**Chat bubble not showing up on Shopify:**
- Make sure you saved `theme.liquid`
- Double-check the `BACKEND_URL` has no typos and no trailing slash

**Bot says "trouble connecting":**
- Check Railway dashboard → Deployments → look at logs for errors
- Make sure `OPENAI_API_KEY` is set correctly in Railway Variables

**Bot doesn't know about the site:**
- Go to GitHub → Actions → run the scraper manually
- Check the Action logs — should say "Saved to knowledge_base.json ✓"

**Railway build failing:**
- Check the build logs in Railway dashboard
- Most common issue: `OPENAI_API_KEY` not set before first deploy
