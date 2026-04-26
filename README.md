# Wallet Hunter Bot

Telegram bot for on-chain wallet discovery on Ethereum mainnet. Originally answered the single question *"Who bought this token for ~X and sold for ~Y?"* — now a full toolkit for finding and evaluating smart-money wallets.

Powered by **GeckoTerminal** (free, no key) + **Etherscan v2** (free key, 100k req/day) — no paid APIs required.

---

## Quick start

1. **Create the bot:** Telegram → `@BotFather` → `/newbot` → copy the token
2. **Get an Etherscan v2 key:** https://etherscan.io/myapikey (free, 5 req/s)
3. **Install:**
   ```bash
   cd /tmp/wallet_research/bot
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```
4. **Configure:**
   ```bash
   export TELEGRAM_BOT_TOKEN="paste-from-botfather"
   export ETHERSCAN_API_KEY="paste-from-etherscan"
   # MORALIS_API_KEY is no longer required — only used if you opt in via:
   # export WALLET_BOT_USE_MORALIS=1
   ```
5. **Run:** `python bot.py` → open the bot in Telegram → `/start`

---

## Commands

### Discover hot tokens
| Command | Example | What it does |
|---|---|---|
| `/trending [N]` | `/trending 12` | Top trending Ethereum tokens — 24h+6h+1h price change, volume, FDV, liquidity, buyer/seller balance |

### Discover top traders on a token
| Command | Example | What it does |
|---|---|---|
| `/topwallets <ca>` | `/topwallets 0x6982508145454ce325ddbe47a25d4ec3d2311933` | Leaderboard by total PnL (realized + unrealized mark-to-market) |
| `/earlybuyers <ca>` | `/earlybuyers 0x...` | First wallets to ever buy this token |
| `/diamondhands <ca>` | `/diamondhands 0x...` | Wallets that bought and held 14+ days |
| `/prepump <ca> [Nx]` | `/prepump 0x... 5` | Wallets that bought before the first 5× pump in 24h |
| `/insider <ca> [age=60] [score=65]` | `/insider 0x...` | Fresh + high-quality early buyers — *the alpha-hunter* |
| `/soldnear <ca>` | `/soldnear 0x...` | Wallets that sold within 80% of all-time peak price |

### Profile / follow a wallet
| Command | Example | What it does |
|---|---|---|
| `/scout <wallet>` | `/scout 0x5b43...` | One-page research: profile + recent buys + clones |
| `/profile <wallet>` | `/profile 0x...` | Quality score (0–100) + flags (age, funding, bot/rug signals) |
| `/copytrade <wallet>` | `/copytrade 0x...` | What is this wallet currently buying? |
| `/clones <wallet>` | `/clones 0x...` | Wallets sharing the same funding source (sybil cluster) |

### Match a PnL card to a wallet
| Command | Example | What it does |
|---|---|---|
| `/find <ca> <inv> <sold>` | `/find 0x... 0.5 2.3` | Top 5 wallets whose ETH-in/out matches |
| `/findsmart <ca> <inv> <sold>` | `/findsmart 0x... 0.5 2.3` | `/find` overlaid with quality scoring |
| `/findwallet <ca> <wallet> <inv> <sold>` | — | Verify a specific wallet against a target |
| `/searchtimes <ca> <minBuy> <maxBuy> <minSell> <maxSell>` | — | Find wallets matching time windows (use `_` for open) |
| `/hunt` | — | Step-by-step interactive flow (asks contract → invested → sold) |
| `/debug <ca> <wallet>` | — | Show every trade for a wallet on a token |

Amount formats: `0.5`, `0.5 eth`, `$500`, `500usd`. Raw numbers ≥ 50 are treated as USD; smaller as ETH.

---

## Quality scoring (0–100)

Each wallet score is the weighted composite of 6 orthogonal signals computed entirely from on-chain behavior:

| Signal | Weight | What it measures |
|---|---|---|
| **bot_avoid** | 25% | Tx velocity, gas-price entropy, MEV-builder usage |
| **funding** | 20% | First deposit source — CEX hot wallet (good) vs Tornado mixer (bad) |
| **diversity** | 15% | Distinct ERC-20s touched in 90d (more = real human) |
| **rug_avoid** | 15% | Contract-deployment count (many = farmer/rugger) |
| **activity** | 15% | Average per-token holding time (sub-hour = bot, 30d+ = HODLer) |
| **age** | 10% | Days since first transaction |

Ratings: **avoid** (<30) → **risky** (<50) → **normal** (<65) → **good** (<80) → **great** (<90) → **elite** (90+)

---

## Architecture

```
┌──────────┐
│ bot.py   │  Telegram handlers, render formatters
└────┬─────┘
     │
     ├──► matcher.py       PnL-card amount matching (legacy /find, /hunt)
     ├──► discovery.py     10 wallet-finding patterns + /scout meta-cmd
     ├──► quality.py       6-signal wallet quality scorer
     │
     └──► sources.py       Free-API data layer
          ├── GTSource       GeckoTerminal: pools, trades, OHLCV, trending
          │                  Two-axis throttle (25/min + 1.5s spacing)
          └── EtherscanSource Etherscan v2: tokentx, txlist, internal,
                              wallet age/funding/diversity/deployers
                              5/sec leaky-bucket rate limit
```

Key design choices:
- **GT only returns ~300 recent trades per pool**, so historical patterns (early buyers, full lifecycle) walk Etherscan `tokentx` instead.
- **No Moralis dependency** in the default boot path. Set `WALLET_BOT_USE_MORALIS=1` to opt back in to the legacy paid path.
- **All caches skip empty/error responses** to avoid poisoning lookups for the full TTL.
- **`wallet_age_days` caches the immutable `first_ts`** and computes age dynamically — so cached scores don't drift over time.

---

## Caveats

- Match commands (`/find` etc.) only see swaps against the pair directly. Multi-hop routes that don't touch the pair are invisible (~1% of memecoin trades).
- `/topwallets` ranks on realized + unrealized PnL using a median price-per-token from the GT trade sample. Wallets we observe selling but never buying (cost basis outside the GT window) are dropped to prevent fake +∞ ROIs.
- `/insider` requires Etherscan key for the genesis-walk + scoring. Other commands degrade gracefully if the key is missing.
- Quality scoring is cached 6h per wallet. Run `/clearcache` to force re-fetch.

---

## Deploy

The bot is designed to run as a Telegram polling worker with a tiny HTTP keep-alive endpoint. Render and Fly are both supported via the included `render.yaml` and `fly.toml`. See `DEPLOY_RENDER.md` for the step-by-step Render deploy.

Required env on the host:
- `TELEGRAM_BOT_TOKEN`
- `ETHERSCAN_API_KEY`
- (optional) `PORT` — set by Render automatically; bot serves a `200 ok` on `GET /` for UptimeRobot
- (optional) `WALLET_BOT_USE_MORALIS=1` — opts back into the legacy Moralis path

---

## Run 24/7 (self-host)

```bash
# systemd (Linux)
sudo tee /etc/systemd/system/wallet-bot.service <<'EOF'
[Unit]
Description=Wallet Hunter Bot
After=network-online.target

[Service]
User=%i
Environment=TELEGRAM_BOT_TOKEN=...
Environment=ETHERSCAN_API_KEY=...
WorkingDirectory=/opt/wallet-hunter-bot
ExecStart=/opt/wallet-hunter-bot/venv/bin/python bot.py
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF
sudo systemctl enable --now wallet-bot
```

On macOS, use `launchd` or simply `nohup python bot.py &`.
