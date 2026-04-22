# Wallet Hunter Bot

Telegram bot that answers: *"Who bought this token for ~X and sold for ~Y?"*

Given a token contract + your target invested/sold amounts, it scans every swap on the token's Uniswap WETH pair(s) and returns the 5 wallets whose aggregated trade footprint is closest to yours.

## 1. Create the Telegram bot

1. Open Telegram → `@BotFather`
2. Send `/newbot`, pick a name, pick a username ending in `bot`
3. Copy the token it gives you (looks like `7981234567:ABC…`)

## 2. Install

```bash
cd /tmp/wallet_research/bot
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## 3. Configure

```bash
export TELEGRAM_BOT_TOKEN="paste-from-botfather"
export ETHERSCAN_API_KEY="AF7HK38VHNI22S69QYABSVTA6SFPZN7E5Z"   # or your own
```

## 4. Run

```bash
python bot.py
```

You'll see `Bot starting...`. Open Telegram, find your bot by its username, hit **Start**.

## Commands

| Command | Example | Notes |
|--------|--------|-------|
| `/start` or `/help` | — | Usage help |
| `/find <contract> <invested> <sold>` | `/find 0xf280b16ef293d8e534e370794ef26bf312694126 0.5 2.3` | One-shot query |
| `/hunt` | — | Step-by-step: asks contract → invested → sold |
| `/clearcache` | — | Flush cached Etherscan/DexScreener data |
| `/cancel` | — | Exit a `/hunt` flow |

Amount formats: `0.5`, `0.5 eth`, `$500`, `500usd`. Raw numbers ≥ 50 are treated as USD (it's too large for ETH); smaller as ETH.

## How it works

1. **DexScreener** → list the token's Uniswap V2/V3 pairs that quote against WETH (sorted by liquidity, top 3).
2. **Etherscan v2 `tokentx`** filtered to `(token, pair)` → every token transfer in/out of the pair = every buy/sell.
3. **Etherscan v2 `tokentx`** filtered to `(WETH, pair)` → every WETH flow in/out of the same pair.
4. **Join by transaction hash**:
   - token leaves pair + WETH enters pair → **BUY** by `to` address, ETH spent = WETH in
   - token enters pair + WETH leaves pair → **SELL** by `from` address, ETH received = WETH out
5. Drop router/aggregator addresses (Uniswap/1inch/0x/CoW/ParaSwap/LI.FI/Maestro/Metamask Swap).
6. Rank by relative distance: `|invested − target| / target + |sold − target| / target`.

Each reply gives the top 5 matches with bought / sold / PnL / ROI and links to Etherscan, Debank, GMGN for that wallet.

## Caveats

- Matches only swaps against the **pair directly**. If a wallet routed through multiple hops that did not touch this pair, it's invisible. (Fine for 99% of memecoin trades.)
- Bot-routed trades (Maestro, Banana, Sigma, Unibot) show the **end wallet** as the buyer because the bot forwards the token, so this still works as long as the bot address itself is in the router list.
- Results are aggregated per wallet across **all time** within the last ~50k events per pair. If your target did many small trades, the summed total is what matches.
- USD amounts are converted to ETH using live WETH price from DexScreener — not historical per-trade. For precision, give amounts in ETH.
- Data is cached for 30 min per (contract, pair). Use `/clearcache` after a very fresh trade.

## Run 24/7 (optional)

```bash
# with systemd (Linux)
sudo tee /etc/systemd/system/wallet-bot.service <<'EOF'
[Unit]
Description=Wallet Hunter Bot
After=network-online.target

[Service]
User=%i
Environment=TELEGRAM_BOT_TOKEN=...
Environment=ETHERSCAN_API_KEY=...
WorkingDirectory=/tmp/wallet_research/bot
ExecStart=/tmp/wallet_research/bot/venv/bin/python bot.py
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF
sudo systemctl enable --now wallet-bot
```

On macOS, use `launchd` or just `nohup python bot.py &`.
