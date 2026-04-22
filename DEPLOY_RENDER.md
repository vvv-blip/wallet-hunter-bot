# Deploy to Render + UptimeRobot (free forever)

The bot's already been patched to open an HTTP health endpoint on `$PORT`, which is everything Render's free Web Service needs.

## Step 1 — Push this folder to GitHub

```bash
cd /tmp/wallet_research/bot
git init
git add matcher.py bot.py requirements.txt render.yaml Dockerfile .dockerignore
git commit -m "Wallet Hunter bot"
# create a new repo on github.com (private is fine) then:
git branch -M main
git remote add origin git@github.com:<you>/wallet-hunter-bot.git
git push -u origin main
```

If you don't want to use GitHub, Render also supports connecting a GitLab/Bitbucket repo, or you can use `rsync` with their `render-cli` tool. GitHub is the path of least resistance.

## Step 2 — Render Blueprint deploy

1. https://render.com → Sign up (free; no card required for free tier).
2. **New → Blueprint** → connect the GitHub repo → **Apply**.
3. Render reads `render.yaml` and provisions a Web Service called `wallet-hunter-bot`.
4. In the service dashboard → **Environment** → set:
   - `TELEGRAM_BOT_TOKEN` = `<from @BotFather>`
   - `ETHERSCAN_API_KEY` = `AF7HK38VHNI22S69QYABSVTA6SFPZN7E5Z` (or your own)
5. Save → service redeploys. Within ~2 min you'll have a public URL like
   `https://wallet-hunter-bot-xxxx.onrender.com`.

Open that URL in a browser — you should see plain text `ok`. That's the health endpoint.

## Step 3 — UptimeRobot keep-alive

Render's free tier sleeps after **15 min of inbound traffic silence**. UptimeRobot pings keep it awake.

1. https://uptimerobot.com → sign up (free — 50 monitors).
2. **Add New Monitor**:
   - Type: **HTTPS**
   - Name: `wallet-hunter-bot`
   - URL: your Render public URL
   - Interval: **5 minutes** (free tier's minimum)
3. Save. UptimeRobot will ping every 5 min → Render keeps the service hot → bot stays connected to Telegram.

## Step 4 — Test

Open Telegram, find your bot, `/start`, then:
```
/find 0xcec8314cf4d448fbd3525696af045f13ccea7444 0.5 1.5
```

You should get matches within ~20 s.

## Logs / debugging

- **Render**: service dashboard → **Logs** tab — shows bot stdout/stderr live.
- **UptimeRobot**: dashboard shows up/down with latency history.

## Free-tier limits

- Render free web service: 750 h/mo (enough for 1 always-on service) and 500 build min/mo.
- UptimeRobot free: 50 monitors, 5-min interval.
- Bot itself is well under 256 MB RAM and uses ~1 % CPU idle, so you're nowhere near any limit.

## Cold-start caveat

When UptimeRobot pings a **sleeping** free service, Render cold-starts it (~30 s). If you set a 5-min interval, the service never sleeps in the first place. If it ever does (UptimeRobot outage, e.g.), your next `/find` will take ~30 s longer than normal for that one call.

## Updating the bot

Push to `main` on GitHub → Render auto-redeploys (`autoDeploy: true` in render.yaml).
