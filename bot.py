"""Telegram bot: /find <contract> <invested> <sold>  ->  wallets with closest trade footprint.

Accepts:
  - ETH amounts:  "0.5", "0.5eth", "0.5 ETH"
  - USD amounts:  "$500", "500usd", "500 USD"

Env vars required:
  TELEGRAM_BOT_TOKEN   (from @BotFather)
  MORALIS_API_KEY      (admin.moralis.com — primary trade-history source)

Optional:
  ETHERSCAN_API_KEY    (only used for is_contract checks)
  BOT_CACHE_DIR        (default: /tmp/wallet_bot_cache)
"""
import os, re, asyncio, html, logging, threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, ConversationHandler

from matcher import Matcher, get_eth_price_usd
from sources import Cache, GTSource, EtherscanSource
from quality import WalletQualityScorer
from discovery import Discovery

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
log = logging.getLogger('walletbot')

TELEGRAM_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN') or ''
ETHERSCAN_KEY = os.environ.get('ETHERSCAN_API_KEY') or ''
MORALIS_KEY = os.environ.get('MORALIS_API_KEY') or ''
CACHE_DIR = os.environ.get('BOT_CACHE_DIR', '/tmp/wallet_bot_cache')

matcher = Matcher(etherscan_key=ETHERSCAN_KEY, moralis_key=MORALIS_KEY, cache_dir=CACHE_DIR)

# Shared sources/quality/discovery instances (reuse Matcher's cache+sources).
_cache = matcher.cache or Cache(CACHE_DIR)
_gt = matcher.gt or GTSource(_cache)
_es = matcher.es or EtherscanSource(ETHERSCAN_KEY, _cache) if ETHERSCAN_KEY else None
_scorer = WalletQualityScorer(_es, _gt, _cache) if _es else WalletQualityScorer(None, _gt, _cache)
_disc = Discovery(_es, _gt, _cache)

# ---- parsing ----
AMOUNT_RE = re.compile(r'^\s*\$?\s*([0-9]*\.?[0-9]+)\s*(eth|weth|usd|usdc|\$)?\s*$', re.I)

def parse_amount(s):
    """Return (unit: 'eth'|'usd', value_float) or (None, None)."""
    if s is None:
        return None, None
    s = s.strip().replace(',', '')
    if s.startswith('$'):
        m = AMOUNT_RE.match(s)
        if not m: return None, None
        return 'usd', float(m.group(1))
    m = AMOUNT_RE.match(s)
    if not m: return None, None
    val = float(m.group(1))
    unit = (m.group(2) or '').lower()
    if unit in ('usd', 'usdc', '$'):
        return 'usd', val
    if unit in ('eth', 'weth', ''):
        # heuristic: no unit + big number -> USD
        if unit == '' and val >= 50:
            return 'usd', val
        return 'eth', val
    return None, None


def fmt_short(addr):
    return addr[:6] + '…' + addr[-4:]


# ---- time parsing (for /searchtimes) ----
# Accept:  unix ts in seconds OR ms  |  YYYY-MM-DD  |  YYYY-MM-DDTHH:MM  |  YYYY-MM-DDTHH:MM:SS
# Use "_" as wildcard to leave a bound open, e.g.  /searchtimes 0xTOKEN 2026-04-01 _ 2026-04-10 _
_TIME_FMTS = ['%Y-%m-%d', '%Y-%m-%dT%H:%M', '%Y-%m-%dT%H:%M:%S']

def parse_time(s):
    """Returns unix-seconds int, or None to mean 'no bound', or raises ValueError."""
    if s is None:
        return None
    s = s.strip()
    if s in ('', '_', '-', '*'):
        return None
    if re.match(r'^\d{9,11}$', s):
        return int(s)
    if re.match(r'^\d{12,14}$', s):
        return int(s) // 1000  # ms -> s
    for fmt in _TIME_FMTS:
        try:
            dt = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except ValueError:
            continue
    raise ValueError(f"Can't parse time: {s}")


def fmt_ts(ts):
    if not ts:
        return '—'
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')


# ---- state for conversation flow ----
WAIT_CONTRACT, WAIT_INVEST, WAIT_SOLD = range(3)


async def start_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🔎 *Wallet Hunter Bot* — find smart wallets, dodge bots/sybils/ruggers.\n\n"
        "*Match by amounts (PnL-card style):*\n"
        "`/find 0xTOKEN 0.5 2.3` — wallets that bought 0.5 ETH, sold 2.3 ETH (±5%)\n"
        "`/findsmart 0xTOKEN 0.5 2.3` — same as /find, but only *quality-scored* "
        "wallets (real humans, not bots)\n\n"
        "*Discover top traders on a token:*\n"
        "`/topwallets 0xTOKEN` — leaderboard by realized PnL\n"
        "`/earlybuyers 0xTOKEN` — first wallets to ever buy this token\n"
        "`/diamondhands 0xTOKEN` — bought and held 14+ days\n"
        "`/prepump 0xTOKEN` — wallets that bought *before* the first 5x pump\n"
        "`/insider 0xTOKEN` — fresh + high-quality early buyers (alpha hunter)\n"
        "`/soldnear 0xTOKEN` — wallets that sold within 80% of the all-time peak\n\n"
        "*Profile / follow a wallet:*\n"
        "`/profile 0xWALLET` — quality score + signals (age, funding, bot/rug flags)\n"
        "`/copytrade 0xWALLET` — what is this wallet currently buying?\n"
        "`/clones 0xWALLET` — wallets sharing the same funding source (sybil check)\n\n"
        "*By time windows:*\n"
        "`/searchtimes 0xTOKEN <minBuy> <maxBuy> <minSell> <maxSell>`\n"
        "Use `_` for an open bound. Example: "
        "`/searchtimes 0xTOKEN 2026-03-01 2026-03-15 2026-04-01 2026-04-20`\n\n"
        "*Direct verify:*\n"
        "`/findwallet 0xTOKEN 0xWALLET <invested> <sold>` — verify a specific wallet\n"
        "`/debug 0xTOKEN 0xWALLET` — show every trade for a wallet on a token\n\n"
        "*Step-by-step amount search:*\n"
        "`/hunt` → I'll ask you 3 questions.\n\n"
        "*Tips:*\n"
        "Add `since:30d` or `since:2026-04-01` to /find to narrow the scan window:\n"
        "`/find 0xTOKEN 0.5 2.3 since:7d`\n\n"
        "*Other:*\n"
        "`/help` — show this\n"
        "`/clearcache` — flush cached data",
        parse_mode=ParseMode.MARKDOWN,
    )


async def clearcache_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    import shutil
    try:
        shutil.rmtree(CACHE_DIR, ignore_errors=True)
        os.makedirs(CACHE_DIR, exist_ok=True)
        await update.message.reply_text("✅ Cache cleared.")
    except Exception as e:
        await update.message.reply_text(f"Error: {e}")


# ---- /hunt conversation ----
async def hunt_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.clear()
    await update.message.reply_text(
        "Send the *token contract address* (0x…):", parse_mode=ParseMode.MARKDOWN
    )
    return WAIT_CONTRACT


async def hunt_got_contract(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    addr = (update.message.text or '').strip().lower()
    if not re.match(r'^0x[0-9a-f]{40}$', addr):
        await update.message.reply_text("Not a valid 0x address. Try again, or /cancel.")
        return WAIT_CONTRACT
    ctx.user_data['token'] = addr
    await update.message.reply_text(
        "How much did you *invest*? (e.g. `0.5`, `0.5 eth`, `$500`)",
        parse_mode=ParseMode.MARKDOWN,
    )
    return WAIT_INVEST


async def hunt_got_invest(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u, v = parse_amount(update.message.text)
    if v is None:
        await update.message.reply_text("Couldn't parse. Send `0.5` or `$500`, or /cancel.")
        return WAIT_INVEST
    ctx.user_data['inv_unit'] = u
    ctx.user_data['inv_val'] = v
    await update.message.reply_text(
        "And the *sold* amount? (same format)", parse_mode=ParseMode.MARKDOWN
    )
    return WAIT_SOLD


async def hunt_got_sold(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u, v = parse_amount(update.message.text)
    if v is None:
        await update.message.reply_text("Couldn't parse. Send `2.3` or `$2300`, or /cancel.")
        return WAIT_SOLD
    ctx.user_data['sold_unit'] = u
    ctx.user_data['sold_val'] = v
    token = ctx.user_data['token']
    await _run_match(update, ctx, token,
                     ctx.user_data['inv_unit'], ctx.user_data['inv_val'],
                     ctx.user_data['sold_unit'], ctx.user_data['sold_val'])
    return ConversationHandler.END


async def hunt_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.clear()
    await update.message.reply_text("Cancelled.")
    return ConversationHandler.END


# ---- /find direct command ----
def _parse_since(s):
    """Parse '7d' / '30d' / '24h' / '2026-04-01' / '2026-04-01T12:00' to unix seconds.
    Returns None if unparseable."""
    if not s:
        return None
    s = s.strip().lower()
    m = re.match(r'^(\d+)\s*([dhwm])$', s)
    if m:
        n = int(m.group(1))
        unit = m.group(2)
        secs = {'h': 3600, 'd': 86400, 'w': 86400 * 7, 'm': 86400 * 30}[unit]
        return int(datetime.now(timezone.utc).timestamp()) - n * secs
    for fmt in ('%Y-%m-%d', '%Y-%m-%dT%H:%M', '%Y-%m-%dT%H:%M:%S'):
        try:
            return int(datetime.strptime(s, fmt).replace(tzinfo=timezone.utc).timestamp())
        except ValueError:
            continue
    return None


async def find_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = ctx.args
    if len(args) < 3:
        await update.message.reply_text(
            "Usage: `/find <contract> <invested> <sold> [since:<window>]`\n"
            "Example: `/find 0xf280b16e… 0.5 2.3`\n"
            "         `/find 0xf280b16e… 0.5 2.3 since:30d`\n"
            "         `/find 0xf280b16e… 0.5 2.3 since:2026-03-01`\n\n"
            "`since:` optional — defaults to all-time scan (capped at 20K most-recent swaps).\n"
            "Use it on very-active tokens to bound the search window.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    token = args[0].lower()
    if not re.match(r'^0x[0-9a-f]{40}$', token):
        await update.message.reply_text("First arg must be a 0x… contract address.")
        return
    iu, iv = parse_amount(args[1])
    su, sv = parse_amount(args[2])
    if iv is None or sv is None:
        await update.message.reply_text("Couldn't parse the amounts. Try `0.5` or `$500`.")
        return

    # Optional since:<window> flag (anywhere after the required args)
    since_ts = None
    for extra in args[3:]:
        if extra.lower().startswith('since:'):
            since_ts = _parse_since(extra.split(':', 1)[1])
            if since_ts is None:
                await update.message.reply_text(
                    f"Couldn't parse `{extra}`. Use e.g. `since:7d`, `since:30d`, `since:2026-04-01`.",
                    parse_mode=ParseMode.MARKDOWN,
                )
                return

    await _run_match(update, ctx, token, iu, iv, su, sv, since_ts=since_ts)


async def _run_match(update, ctx, token, inv_unit, inv_val, sold_unit, sold_val, since_ts=None):
    win_label = ''
    if since_ts:
        days = (datetime.now(timezone.utc).timestamp() - since_ts) / 86400
        win_label = f' (last {days:.0f}d)'
    status = await update.message.reply_text(
        f"⏳ Scanning {fmt_short(token)}{win_label}… (10–90 s)"
    )

    loop = asyncio.get_running_loop()

    def work():
        eth_price = None
        if inv_unit == 'usd' or sold_unit == 'usd':
            eth_price = get_eth_price_usd()
        inv_eth = inv_val / eth_price if inv_unit == 'usd' else inv_val
        sold_eth = sold_val / eth_price if sold_unit == 'usd' else sold_val
        results, pairs, filt = matcher.find_matches(
            token, inv_eth, sold_eth, top_n=5, since_ts=since_ts,
        )
        return results, pairs, filt, inv_eth, sold_eth, eth_price

    try:
        results, pairs, filt, inv_eth, sold_eth, eth_price = await loop.run_in_executor(None, work)
    except Exception as e:
        log.exception("match error")
        await status.edit_text(f"❌ Error: {html.escape(str(e)[:200])}")
        return

    if not pairs:
        await status.edit_text("❌ No Ethereum/WETH Uniswap pair found for that token on DexScreener.")
        return
    if not results:
        await status.edit_text(
            f"❌ *No wallets matched within ±5%* of your targets "
            f"({inv_eth:.4f} ETH bought / {sold_eth:.4f} ETH sold).\n\n"
            f"Filter funnel:\n"
            f"• {filt.get('total', 0)} pool-scan wallets\n"
            f"• {filt.get('prefilter', 0)} in loose prefilter (within 10× of target)\n"
            f"• +{filt.get('top_gainers_seeded', 0)} from Moralis PnL leaderboard\n"
            f"• +{filt.get('top_long_holders', 0)} top long-holders / "
            f"{filt.get('top_recent_buyers', 0)} recent-buyers / "
            f"{filt.get('top_active', 0)} most-active\n"
            f"• +{filt.get('gt_traders', 0)} GeckoTerminal traders (top 15 pools)\n"
            f"• {filt.get('single_leg', 0)} flagged as single-leg-close (one leg ±20%)\n"
            f"• {filt.get('verified', 0)} verified via wallet-centric totals\n"
            f"• {filt.get('inv_ok', 0)} within ±5% on invested\n"
            f"• {filt.get('sell_ok', 0)} also within ±5% on sold\n\n"
            f"If you know a specific wallet, try `/findwallet {token} <wallet> {inv_eth} {sold_eth}` to verify it directly.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    eth_price = eth_price or get_eth_price_usd()

    lines = [
        f"🎯 *Matches for* `{fmt_short(token)}`",
        f"_Target: bought {inv_eth:.4f} ETH · sold {sold_eth:.4f} ETH_  (≈${inv_eth*eth_price:,.0f} / ${sold_eth*eth_price:,.0f})",
        f"_{filt.get('total', 0)} pool-scan → "
        f"{filt.get('prefilter', 0)} prefilter → "
        f"{filt.get('verified', 0)} verified → "
        f"{filt.get('sell_ok', 0)} match_",
        "",
    ]
    for i, r in enumerate(results, 1):
        pnl_eth = r['pnl_eth']
        pnl_usd = pnl_eth * eth_price
        roi = (pnl_eth / r['invested_eth'] * 100) if r['invested_eth'] > 0.00001 else 0
        tag = '💰 sold' if r.get('bucket') == 'seller' else '📦 still holding'
        lines.append(
            f"*#{i}*  {tag}  `{r['wallet']}`\n"
            f"   bought: {r['invested_eth']:.4f} ETH ({r['n_buys']}×)\n"
            f"   sold:   {r['sold_eth']:.4f} ETH ({r['n_sells']}×)\n"
            f"   pnl:    {pnl_eth:+.4f} ETH  (${pnl_usd:+,.0f} · {roi:+.0f}%)\n"
            f"   dist:   {r['dist']:.3f}\n"
            f"   [etherscan](https://etherscan.io/address/{r['wallet']}) · "
            f"[debank](https://debank.com/profile/{r['wallet']}) · "
            f"[gmgn](https://gmgn.ai/eth/address/{r['wallet']})"
        )
        lines.append("")

    await status.edit_text(
        "\n".join(lines),
        parse_mode=ParseMode.MARKDOWN,
        disable_web_page_preview=True,
    )


# ---- /searchtimes <token> <min_buy> <max_buy> <min_sell> <max_sell> ----
async def searchtimes_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = ctx.args
    if len(args) < 5:
        await update.message.reply_text(
            "Usage:\n"
            "`/searchtimes <contract> <min_buy> <max_buy> <min_sell> <max_sell>`\n\n"
            "Dates: `YYYY-MM-DD`, `YYYY-MM-DDTHH:MM`, or unix seconds. "
            "Use `_` for an open bound.\n\n"
            "Example:\n"
            "`/searchtimes 0xce82…95de 2026-03-01 2026-03-15 2026-04-01 2026-04-20`\n"
            "(wallets that bought in early March and sold in April)",
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    token = args[0].lower()
    if not re.match(r'^0x[0-9a-f]{40}$', token):
        await update.message.reply_text("First arg must be a 0x… contract address.")
        return
    try:
        min_buy = parse_time(args[1])
        max_buy = parse_time(args[2])
        min_sell = parse_time(args[3])
        max_sell = parse_time(args[4])
    except ValueError as e:
        await update.message.reply_text(f"❌ {e}")
        return

    status = await update.message.reply_text(
        f"⏳ Scanning {fmt_short(token)} for wallets matching those time windows… (10–90 s)"
    )

    loop = asyncio.get_running_loop()

    def work():
        return matcher.search_by_times(token, min_buy, max_buy, min_sell, max_sell, top_n=10)

    try:
        results, pairs, total = await loop.run_in_executor(None, work)
    except Exception as e:
        log.exception("searchtimes error")
        await status.edit_text(f"❌ Error: {html.escape(str(e)[:200])}")
        return

    if not pairs:
        await status.edit_text("❌ No Ethereum/WETH Uniswap pair found for that token on DexScreener.")
        return
    if not results:
        await status.edit_text(
            f"❌ No wallets match those time windows across {total} scanned wallets."
        )
        return

    eth_price = get_eth_price_usd()
    lines = [
        f"⏱ *Time-match for* `{fmt_short(token)}`",
        f"_Bought in {fmt_ts(min_buy)} → {fmt_ts(max_buy)}_",
        f"_Sold in  {fmt_ts(min_sell)} → {fmt_ts(max_sell)}_",
        f"_Scanned {total} wallets · {len(results)} match_",
        "",
    ]
    for i, r in enumerate(results, 1):
        pnl_usd = r['pnl_eth'] * eth_price
        roi = (r['pnl_eth'] / r['invested_eth'] * 100) if r['invested_eth'] > 0.00001 else 0
        lines.append(
            f"*#{i}*  `{r['wallet']}`\n"
            f"   bought: {r['invested_eth']:.4f} ETH ({r['n_buys']}×, {r['n_buys_in_window']}× in window)\n"
            f"   sold:   {r['sold_eth']:.4f} ETH ({r['n_sells']}×, {r['n_sells_in_window']}× in window)\n"
            f"   pnl:    {r['pnl_eth']:+.4f} ETH  (${pnl_usd:+,.0f} · {roi:+.0f}%)\n"
            f"   first buy in win: {fmt_ts(r['first_buy_in_window'])}\n"
            f"   last sell in win: {fmt_ts(r['last_sell_in_window'])}\n"
            f"   [etherscan](https://etherscan.io/address/{r['wallet']}) · "
            f"[debank](https://debank.com/profile/{r['wallet']}) · "
            f"[gmgn](https://gmgn.ai/eth/address/{r['wallet']})"
        )
        lines.append("")

    await status.edit_text(
        "\n".join(lines),
        parse_mode=ParseMode.MARKDOWN,
        disable_web_page_preview=True,
    )


# ---- /findwallet <token> <wallet> <invested> <sold>  -- direct verify (no scan) ----
async def findwallet_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = ctx.args
    if len(args) < 4:
        await update.message.reply_text(
            "Usage: `/findwallet <contract> <wallet> <invested> <sold>`\n"
            "Verifies a specific wallet against your targets — fast & always accurate.\n"
            "Example: `/findwallet 0xTOKEN 0xWALLET 0.5 2.3`",
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    token = args[0].lower()
    wallet = args[1].lower()
    if not re.match(r'^0x[0-9a-f]{40}$', token) or not re.match(r'^0x[0-9a-f]{40}$', wallet):
        await update.message.reply_text("First two args must be 0x… addresses.")
        return
    iu, iv = parse_amount(args[2])
    su, sv = parse_amount(args[3])
    if iv is None or sv is None:
        await update.message.reply_text("Couldn't parse amounts. Try `0.5` or `$500`.")
        return

    status = await update.message.reply_text(
        f"🎯 Verifying {fmt_short(wallet)} on {fmt_short(token)}…"
    )
    loop = asyncio.get_running_loop()
    try:
        eth_price = get_eth_price_usd() if (iu == 'usd' or su == 'usd') else None
        inv_eth = iv / eth_price if iu == 'usd' else iv
        sold_eth = sv / eth_price if su == 'usd' else sv
        totals = await loop.run_in_executor(None,
            lambda: matcher.wallet_token_totals(token, wallet))
    except Exception as e:
        log.exception("findwallet error")
        await status.edit_text(f"❌ Error: {html.escape(str(e)[:200])}")
        return

    if totals.get('n_buys', 0) + totals.get('n_sells', 0) == 0:
        await status.edit_text(
            f"❌ Moralis returned 0 swaps for `{wallet}` on this token.\n\n"
            f"Possible: wrong contract, different chain, or trades older than the index window.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    ei = totals['eth_in']; eo = totals['eth_out']
    inv_ok = abs(ei - inv_eth) / max(inv_eth, 1e-9) <= 0.05
    sold_ok = abs(eo - sold_eth) / max(sold_eth, 1e-9) <= 0.05
    pnl = eo - ei
    icon_inv = '✅' if inv_ok else '❌'
    icon_sold = '✅' if sold_ok else '❌'
    overall = '🎯 *MATCH*' if (inv_ok and sold_ok) else '⚠️ *MISMATCH*'

    lines = [
        f"{overall}  `{wallet}`",
        f"Token: `{fmt_short(token)}`",
        "",
        f"*Authoritative totals (Moralis):*",
        f"  bought: {ei:.4f} ETH  (target {inv_eth:.4f}, {icon_inv})",
        f"  sold:   {eo:.4f} ETH  (target {sold_eth:.4f}, {icon_sold})",
        f"  pnl:    {pnl:+.4f} ETH",
        f"  trades: {totals['n_buys']} buys / {totals['n_sells']} sells",
        "",
        f"[etherscan](https://etherscan.io/address/{wallet}) · "
        f"[debank](https://debank.com/profile/{wallet}) · "
        f"[gmgn](https://gmgn.ai/eth/address/{wallet})",
    ]
    await status.edit_text("\n".join(lines)[:4000],
                           parse_mode=ParseMode.MARKDOWN,
                           disable_web_page_preview=True)


# ---- /debug <token> <wallet>  -- show what the matcher actually sees for this wallet ----
async def debug_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = ctx.args
    if len(args) < 2:
        await update.message.reply_text(
            "Usage: `/debug <contract> <wallet>`\n"
            "Shows exactly what buys/sells the bot is capturing for a specific wallet, "
            "useful to figure out *why* it didn't show up in `/find`.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    token = args[0].lower()
    wallet = args[1].lower()
    if not re.match(r'^0x[0-9a-f]{40}$', token) or not re.match(r'^0x[0-9a-f]{40}$', wallet):
        await update.message.reply_text("Both args must be 0x… addresses.")
        return

    status = await update.message.reply_text(
        f"🔍 Debugging {fmt_short(wallet)} on {fmt_short(token)}… (10–90 s)"
    )

    loop = asyncio.get_running_loop()
    try:
        info = await loop.run_in_executor(None, lambda: matcher.debug_wallet(token, wallet))
    except Exception as e:
        log.exception("debug error")
        await status.edit_text(f"❌ Error: {html.escape(str(e)[:200])}")
        return

    is_c = info['is_contract']
    totals = info.get('totals') or {}
    native = info.get('native_label', 'ETH')
    n_pools = info.get('n_pools_total', 0)

    lines = [
        f"🔍 *Debug* `{wallet}`",
        f"Token: `{fmt_short(token)}`",
        f"Is contract: {is_c}",
        f"Pools seen: {n_pools}",
        "",
    ]

    if totals.get('error'):
        lines.append(f"⚠️ totals query failed: {totals['error']}")
    elif (totals.get('n_buys', 0) + totals.get('n_sells', 0)) == 0:
        lines += [
            f"❌ Moralis returned 0 swaps for this wallet on this token.",
            "",
            "Possible causes:",
            "• wrong contract address?",
            "• wallet traded on another chain (Base / BSC / Solana)?",
            "• trades older than Moralis' history window for this token?",
        ]
    else:
        pnl = totals['eth_out'] - totals['eth_in']
        lines += [
            f"*Totals (authoritative — Moralis):*",
            f"  bought: *{totals['eth_in']:.4f} {native}* ({totals['n_buys']}×)",
            f"  sold:   *{totals['eth_out']:.4f} {native}* ({totals['n_sells']}×)",
            f"  pnl:    {pnl:+.4f} {native}",
        ]
        trades = totals.get('trades') or []
        if trades:
            lines.append("")
            lines.append(f"_Trades ({len(trades)}):_")
            for t in trades[:15]:
                lines.append(f"• {fmt_ts(t['ts'])}  {t['kind']:4} {t['eth']:.4f} {native}")

    # for-reference: token-scan view of this wallet (may be partial if very old)
    s = info.get('stats')
    if s and (s.get('n_buys', 0) + s.get('n_sells', 0)) > 0:
        lines += [
            "",
            f"_(token-scan: {s['eth_in']:.4f} in / {s['eth_out']:.4f} out — "
            f"a subset of authoritative totals if wallet has older trades)_",
        ]

    text = "\n".join(lines)[:4000]
    await status.edit_text(text, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)


# ════════════════════════════════════════════════════════════════════════════
# NEW COMMANDS — quality scoring + discovery patterns
# ════════════════════════════════════════════════════════════════════════════

def _fmt_quality_block(q):
    """Compact rendering of a quality score for /profile and /findsmart."""
    rating_emoji = {
        'avoid': '🚨', 'risky': '⚠️', 'normal': '🟡',
        'good': '🟢', 'great': '⭐', 'elite': '💎',
        'unknown': '❔',
    }.get(q.get('rating', 'unknown'), '❔')
    lines = [
        f"{rating_emoji} *{q.get('rating', 'unknown').upper()}* "
        f"({q.get('overall', 0):.0f}/100)",
    ]
    sub = q.get('subscores') or {}
    if sub:
        lines.append(
            f"  age {sub.get('age', 0):.0f} · "
            f"div {sub.get('diversity', 0):.0f} · "
            f"fund {sub.get('funding', 0):.0f} · "
            f"rug {sub.get('rug_avoid', 0):.0f} · "
            f"bot {sub.get('bot_avoid', 0):.0f} · "
            f"act {sub.get('activity', 0):.0f}"
        )
    flags = q.get('flags') or []
    for f in flags[:5]:
        lines.append(f"  {f}")
    return "\n".join(lines)


async def profile_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Quality breakdown for one wallet."""
    args = ctx.args
    if len(args) < 1:
        await update.message.reply_text(
            "Usage: `/profile <wallet>`\n"
            "Shows quality score: age, token diversity, funding source, "
            "bot/sybil/rug flags.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    wallet = args[0].lower()
    if not re.match(r'^0x[0-9a-f]{40}$', wallet):
        await update.message.reply_text("Arg must be a 0x… wallet address.")
        return

    status = await update.message.reply_text(
        f"🔬 Profiling {fmt_short(wallet)}…"
    )
    loop = asyncio.get_running_loop()
    try:
        q = await loop.run_in_executor(None, lambda: _scorer.score(wallet))
    except Exception as e:
        log.exception("profile error")
        await status.edit_text(f"❌ Error: {html.escape(str(e)[:200])}")
        return

    meta = q.get('meta') or {}
    lines = [
        f"🔬 *Profile* `{wallet}`",
        "",
        _fmt_quality_block(q),
    ]
    if meta:
        lines += ["", "*Raw signals:*"]
        if meta.get('age_days') is not None:
            lines.append(f"  • age: {meta['age_days']:.0f} days")
        if meta.get('distinct_tokens_90d') is not None:
            lines.append(f"  • distinct tokens 90d: {meta['distinct_tokens_90d']}")
        if meta.get('funding_label'):
            lines.append(f"  • funded by: {meta['funding_label']}")
        if meta.get('deployed_contracts') is not None:
            lines.append(f"  • contracts deployed: {meta['deployed_contracts']}")
        if meta.get('tx_count_30d') is not None:
            lines.append(f"  • txs (30d): {meta['tx_count_30d']}")
        if meta.get('mev_builder_uses') is not None:
            lines.append(f"  • MEV-builder uses: {meta['mev_builder_uses']}")
        if meta.get('avg_hold_days') is not None:
            lines.append(f"  • avg hold: {meta['avg_hold_days']:.1f}d")
    lines += [
        "",
        f"[etherscan](https://etherscan.io/address/{wallet}) · "
        f"[debank](https://debank.com/profile/{wallet}) · "
        f"[gmgn](https://gmgn.ai/eth/address/{wallet})",
    ]
    await status.edit_text("\n".join(lines)[:4000],
                           parse_mode=ParseMode.MARKDOWN,
                           disable_web_page_preview=True)


async def findsmart_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Same as /find but only quality-scored wallets."""
    args = ctx.args
    if len(args) < 3:
        await update.message.reply_text(
            "Usage: `/findsmart <contract> <invested> <sold> [since:<window>]`\n"
            "Like /find, but enriches each result with a quality score and "
            "filters out bots/sybils/ruggers.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    token = args[0].lower()
    if not re.match(r'^0x[0-9a-f]{40}$', token):
        await update.message.reply_text("First arg must be a 0x… contract address.")
        return
    iu, iv = parse_amount(args[1])
    su, sv = parse_amount(args[2])
    if iv is None or sv is None:
        await update.message.reply_text("Couldn't parse amounts.")
        return

    since_ts = None
    for extra in args[3:]:
        if extra.lower().startswith('since:'):
            since_ts = _parse_since(extra.split(':', 1)[1])

    status = await update.message.reply_text(
        f"🧠 Scanning {fmt_short(token)} for quality matches… (15–60s)"
    )
    loop = asyncio.get_running_loop()

    def work():
        eth_price = None
        if iu == 'usd' or su == 'usd':
            eth_price = get_eth_price_usd()
        inv_eth = iv / eth_price if iu == 'usd' else iv
        sold_eth = sv / eth_price if su == 'usd' else sv
        results, pairs, filt = matcher.find_matches(
            token, inv_eth, sold_eth, top_n=15, since_ts=since_ts,
        )
        # Enrich with quality scores
        enriched = []
        for r in results:
            try:
                q = _scorer.score(r['wallet'])
            except Exception:
                q = {'overall': 50, 'rating': 'unknown', 'flags': [], 'subscores': {}}
            r2 = {**r, 'quality': q}
            # Composite: dist (lower=better) + (100-quality)/50 (lower=better)
            r2['composite'] = r['dist'] + (100 - q.get('overall', 50)) / 50.0
            enriched.append(r2)
        enriched.sort(key=lambda r: r['composite'])
        return enriched[:5], pairs, filt, inv_eth, sold_eth, eth_price

    try:
        results, pairs, filt, inv_eth, sold_eth, eth_price = await loop.run_in_executor(None, work)
    except Exception as e:
        log.exception("findsmart error")
        await status.edit_text(f"❌ Error: {html.escape(str(e)[:200])}")
        return

    if not results:
        await status.edit_text(
            f"❌ No quality wallets matched ±5% on both legs.\n"
            f"Funnel: {filt.get('total', 0)} scan → "
            f"{filt.get('verified', 0)} verified → "
            f"{filt.get('sell_ok', 0)} match.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    eth_price = eth_price or get_eth_price_usd()
    lines = [
        f"🧠 *Quality matches for* `{fmt_short(token)}`",
        f"_Target: {inv_eth:.4f} ETH bought · {sold_eth:.4f} ETH sold_",
        "",
    ]
    for i, r in enumerate(results, 1):
        pnl = r['pnl_eth']
        roi = (pnl / r['invested_eth'] * 100) if r['invested_eth'] > 0.00001 else 0
        lines.append(
            f"*#{i}*  `{r['wallet']}`\n"
            f"   bought: {r['invested_eth']:.4f} ETH ({r['n_buys']}×) · "
            f"sold: {r['sold_eth']:.4f} ETH ({r['n_sells']}×)\n"
            f"   pnl: {pnl:+.4f} ETH ({roi:+.0f}%)\n"
            f"{_fmt_quality_block(r['quality'])}\n"
            f"   [etherscan](https://etherscan.io/address/{r['wallet']}) · "
            f"[debank](https://debank.com/profile/{r['wallet']})"
        )
        lines.append("")

    await status.edit_text("\n".join(lines)[:4000],
                           parse_mode=ParseMode.MARKDOWN,
                           disable_web_page_preview=True)


async def topwallets_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Top traders by realized PnL on a token."""
    args = ctx.args
    if len(args) < 1:
        await update.message.reply_text(
            "Usage: `/topwallets <contract>`\nLeaderboard by realized PnL.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    token = args[0].lower()
    if not re.match(r'^0x[0-9a-f]{40}$', token):
        await update.message.reply_text("First arg must be a 0x… contract address.")
        return

    status = await update.message.reply_text(
        f"🏆 Loading top traders for {fmt_short(token)}…"
    )
    loop = asyncio.get_running_loop()
    try:
        results = await loop.run_in_executor(
            None, lambda: _disc.top_traders_by_pnl(token, top_n=10))
    except Exception as e:
        log.exception("topwallets error")
        await status.edit_text(f"❌ Error: {html.escape(str(e)[:200])}")
        return

    if not results:
        await status.edit_text(
            "❌ No trades found. Token may be inactive or unindexed by GT.")
        return

    eth_price = get_eth_price_usd()
    lines = [
        f"🏆 *Top traders by PnL* — `{fmt_short(token)}`",
        f"_(realized — visible recent trades only)_",
        "",
    ]
    for i, r in enumerate(results, 1):
        pnl = r['pnl_eth']
        roi = r.get('roi', 0) * 100
        lines.append(
            f"*#{i}*  `{r['wallet']}`\n"
            f"   in: {r['eth_in']:.3f} ETH ({r['n_buys']}×) · "
            f"out: {r['eth_out']:.3f} ETH ({r['n_sells']}×)\n"
            f"   pnl: {pnl:+.3f} ETH (${pnl*eth_price:+,.0f}, {roi:+.0f}%)\n"
            f"   [etherscan](https://etherscan.io/address/{r['wallet']})"
        )
    await status.edit_text("\n".join(lines)[:4000],
                           parse_mode=ParseMode.MARKDOWN,
                           disable_web_page_preview=True)


async def earlybuyers_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """First N unique wallets to buy a token (insider detection)."""
    args = ctx.args
    if len(args) < 1:
        await update.message.reply_text(
            "Usage: `/earlybuyers <contract>`\nFirst wallets to buy this token.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    token = args[0].lower()
    if not re.match(r'^0x[0-9a-f]{40}$', token):
        await update.message.reply_text("First arg must be a 0x… contract address.")
        return
    if not _es:
        await update.message.reply_text(
            "❌ Needs ETHERSCAN_API_KEY for full transfer history (free tier).")
        return

    status = await update.message.reply_text(
        f"⏳ Walking history of {fmt_short(token)}…"
    )
    loop = asyncio.get_running_loop()
    try:
        results = await loop.run_in_executor(
            None, lambda: _disc.early_buyers(token, limit=20))
    except Exception as e:
        log.exception("earlybuyers error")
        await status.edit_text(f"❌ Error: {html.escape(str(e)[:200])}")
        return

    if not results:
        await status.edit_text("❌ No early buyers found.")
        return

    lines = [f"🎯 *Early buyers* — `{fmt_short(token)}`", ""]
    for i, r in enumerate(results, 1):
        ts = fmt_ts(r.get('first_buy_ts', 0))
        eth = r.get('eth_at_first_buy')
        eth_str = f"{eth:.4f} ETH" if eth else "unknown ETH amount"
        lines.append(
            f"*#{i}* {ts}  `{r['wallet']}`\n"
            f"   first buy: {eth_str}"
        )
    await status.edit_text("\n".join(lines)[:4000],
                           parse_mode=ParseMode.MARKDOWN,
                           disable_web_page_preview=True)


async def diamondhands_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Wallets that bought and held."""
    args = ctx.args
    if len(args) < 1:
        await update.message.reply_text(
            "Usage: `/diamondhands <contract> [min_days=14]`",
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    token = args[0].lower()
    if not re.match(r'^0x[0-9a-f]{40}$', token):
        await update.message.reply_text("First arg must be a 0x… contract address.")
        return
    min_days = 14
    if len(args) >= 2:
        try: min_days = int(args[1])
        except: pass
    if not _es:
        await update.message.reply_text(
            "❌ Needs ETHERSCAN_API_KEY for full transfer history.")
        return

    status = await update.message.reply_text(
        f"💎 Looking for diamond hands on {fmt_short(token)}…"
    )
    loop = asyncio.get_running_loop()
    try:
        results = await loop.run_in_executor(
            None, lambda: _disc.diamond_hands(token, min_hold_days=min_days, top_n=15))
    except Exception as e:
        log.exception("diamondhands error")
        await status.edit_text(f"❌ Error: {html.escape(str(e)[:200])}")
        return

    if not results:
        await status.edit_text(f"❌ No wallets held ≥{min_days} days.")
        return

    lines = [f"💎 *Diamond hands* — `{fmt_short(token)}` (≥{min_days}d)", ""]
    for i, r in enumerate(results, 1):
        held = r.get('current_held', 0)
        hold_d = r.get('hold_days', 0)
        lines.append(
            f"*#{i}*  `{r['wallet']}`\n"
            f"   holds: {held:,.0f} tokens · {hold_d:.0f}d"
        )
    await status.edit_text("\n".join(lines)[:4000],
                           parse_mode=ParseMode.MARKDOWN,
                           disable_web_page_preview=True)


async def insider_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Fresh + high-quality wallets that bought a token early — alpha hunter."""
    args = ctx.args
    if len(args) < 1:
        await update.message.reply_text(
            "Usage: `/insider <contract> [max_age_days=60] [min_score=65]`\n"
            "Fresh, high-quality wallets that bought early — alpha-hunter signal.\n"
            "Combines `/earlybuyers` + quality scoring to surface insider plants.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    token = args[0].lower()
    if not re.match(r'^0x[0-9a-f]{40}$', token):
        await update.message.reply_text("First arg must be a 0x… contract address.")
        return
    max_age = 60
    min_score = 65
    if len(args) >= 2:
        try: max_age = int(args[1])
        except: pass
    if len(args) >= 3:
        try: min_score = int(args[2])
        except: pass
    if not _es:
        await update.message.reply_text(
            "❌ Needs ETHERSCAN_API_KEY for genesis transfer walk + scoring.")
        return

    status = await update.message.reply_text(
        f"🕵️ Hunting insiders on {fmt_short(token)} "
        f"(age≤{max_age}d, score≥{min_score})…"
    )
    loop = asyncio.get_running_loop()
    try:
        results = await loop.run_in_executor(
            None, lambda: _disc.insider_buyers(
                token, _scorer, max_age_days=max_age,
                min_score=min_score, top_n=15))
    except Exception as e:
        log.exception("insider error")
        await status.edit_text(f"❌ Error: {html.escape(str(e)[:200])}")
        return

    if isinstance(results, dict):
        await status.edit_text(
            f"ℹ️ {results.get('reason', 'no insider buyers found')}")
        return
    if not results:
        await status.edit_text(
            f"❌ No fresh, high-score buyers found for {fmt_short(token)}.")
        return

    lines = [
        f"🕵️ *Insider buyers* — `{fmt_short(token)}`",
        f"_filters: age ≤ {max_age}d, score ≥ {min_score}_",
        "",
    ]
    for i, r in enumerate(results, 1):
        flags_str = ', '.join(r.get('quality_flags', [])[:3]) or '—'
        eth_at = r.get('eth_at_first_buy')
        eth_str = f"{eth_at:.3f} ETH" if eth_at else "—"
        lines.append(
            f"*#{i}* `{r['wallet']}`\n"
            f"   age: {r['age_days']:.0f}d · "
            f"score: {r['quality_score']:.0f} ({r['quality_rating']})\n"
            f"   first buy: {fmt_ts(r.get('first_buy_ts', 0))} · "
            f"eth: {eth_str}\n"
            f"   flags: {flags_str}"
        )
    await status.edit_text("\n".join(lines)[:4000],
                           parse_mode=ParseMode.MARKDOWN,
                           disable_web_page_preview=True)


async def prepump_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Wallets that bought before the first big pump."""
    args = ctx.args
    if len(args) < 1:
        await update.message.reply_text(
            "Usage: `/prepump <contract> [multiple=5]`\n"
            "Wallets that bought before first {N}x pump in 24h.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    token = args[0].lower()
    if not re.match(r'^0x[0-9a-f]{40}$', token):
        await update.message.reply_text("First arg must be a 0x… contract address.")
        return
    mult = 5.0
    if len(args) >= 2:
        try: mult = float(args[1])
        except: pass

    status = await update.message.reply_text(
        f"🚀 Locating pump on {fmt_short(token)}…"
    )
    loop = asyncio.get_running_loop()
    try:
        results = await loop.run_in_executor(
            None, lambda: _disc.pre_pump_buyers(token, pump_multiple=mult))
    except Exception as e:
        log.exception("prepump error")
        await status.edit_text(f"❌ Error: {html.escape(str(e)[:200])}")
        return

    # pre_pump_buyers may return list[dict] OR a dict-with-reason envelope
    # like {'reason': 'no pump detected', 'wallets': []} when the OHLCV
    # window doesn't show the requested multiple.  Normalize before slicing.
    reason = None
    if isinstance(results, dict):
        reason = results.get('reason')
        results = results.get('wallets') or []

    if not results:
        msg = f"❌ No {mult:.0f}x pump detected, or no buyers before it."
        if reason:
            msg += f"\n_{html.escape(reason)}_"
        await status.edit_text(msg, parse_mode=ParseMode.MARKDOWN)
        return

    lines = [f"🚀 *Pre-pump buyers* — `{fmt_short(token)}` (>{mult:.0f}x)", ""]
    for i, r in enumerate(results[:20], 1):
        lines.append(
            f"*#{i}* {fmt_ts(r.get('first_buy_ts', 0))}  `{r['wallet']}`"
        )
    await status.edit_text("\n".join(lines)[:4000],
                           parse_mode=ParseMode.MARKDOWN,
                           disable_web_page_preview=True)


async def soldnear_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Wallets that sold within 80% of all-time peak."""
    args = ctx.args
    if len(args) < 1:
        await update.message.reply_text(
            "Usage: `/soldnear <contract> [threshold=0.8]`",
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    token = args[0].lower()
    if not re.match(r'^0x[0-9a-f]{40}$', token):
        await update.message.reply_text("First arg must be a 0x… contract address.")
        return
    threshold = 0.8
    if len(args) >= 2:
        try: threshold = float(args[1])
        except: pass

    status = await update.message.reply_text(
        f"📈 Finding peak-sellers on {fmt_short(token)}…"
    )
    loop = asyncio.get_running_loop()
    try:
        results = await loop.run_in_executor(
            None, lambda: _disc.sold_near_top(token, threshold=threshold, top_n=15))
    except Exception as e:
        log.exception("soldnear error")
        await status.edit_text(f"❌ Error: {html.escape(str(e)[:200])}")
        return

    if not results:
        await status.edit_text(
            f"❌ No sells within {threshold*100:.0f}% of peak found in recent trades.")
        return

    lines = [f"📈 *Sold near top* — `{fmt_short(token)}` (≥{threshold*100:.0f}% of peak)", ""]
    for i, r in enumerate(results, 1):
        lines.append(
            f"*#{i}*  `{r['wallet']}`\n"
            f"   peak sells: {r.get('top_eth_out', 0):.3f} ETH ({r.get('n_top_sells', 0)}×)"
        )
    await status.edit_text("\n".join(lines)[:4000],
                           parse_mode=ParseMode.MARKDOWN,
                           disable_web_page_preview=True)


async def copytrade_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """What is this wallet recently buying?"""
    args = ctx.args
    if len(args) < 1:
        await update.message.reply_text(
            "Usage: `/copytrade <wallet> [days=7]`",
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    wallet = args[0].lower()
    if not re.match(r'^0x[0-9a-f]{40}$', wallet):
        await update.message.reply_text("First arg must be a 0x… wallet.")
        return
    days = 7
    if len(args) >= 2:
        try: days = int(args[1])
        except: pass
    if not _es:
        await update.message.reply_text(
            "❌ Needs ETHERSCAN_API_KEY for wallet trade history.")
        return

    status = await update.message.reply_text(
        f"👀 Reading recent buys for {fmt_short(wallet)}…"
    )
    loop = asyncio.get_running_loop()
    try:
        results = await loop.run_in_executor(
            None, lambda: _disc.copytrade(wallet, days=days, top_n=15))
    except Exception as e:
        log.exception("copytrade error")
        await status.edit_text(f"❌ Error: {html.escape(str(e)[:200])}")
        return

    if not results:
        await status.edit_text(
            f"❌ No recent net-buys (last {days}d) — wallet may be selling or inactive.")
        return

    lines = [f"👀 *Copytrade* `{wallet}` (last {days}d)", ""]
    for i, r in enumerate(results, 1):
        sym = r.get('token_symbol') or '?'
        addr = r.get('token_addr')
        net = r.get('net_received', 0)
        lines.append(
            f"*#{i}* {sym}  `{addr}`\n"
            f"   net bought: {net:,.0f} · last: {fmt_ts(r.get('last_buy_ts', 0))}"
        )
    await status.edit_text("\n".join(lines)[:4000],
                           parse_mode=ParseMode.MARKDOWN,
                           disable_web_page_preview=True)


async def clones_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Wallets sharing a funding source — sybil/cluster detection."""
    args = ctx.args
    if len(args) < 1:
        await update.message.reply_text(
            "Usage: `/clones <wallet>`\n"
            "Wallets funded by the same source as the input wallet.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    wallet = args[0].lower()
    if not re.match(r'^0x[0-9a-f]{40}$', wallet):
        await update.message.reply_text("First arg must be a 0x… wallet.")
        return
    if not _es:
        await update.message.reply_text(
            "❌ Needs ETHERSCAN_API_KEY for funding source lookup.")
        return

    status = await update.message.reply_text(
        f"🧬 Looking for clones of {fmt_short(wallet)}…"
    )
    loop = asyncio.get_running_loop()
    try:
        results = await loop.run_in_executor(
            None, lambda: _disc.clones(wallet, top_n=15))
    except Exception as e:
        log.exception("clones error")
        await status.edit_text(f"❌ Error: {html.escape(str(e)[:200])}")
        return

    # clones() always returns a dict.  Two shapes:
    #   failure: {'reason': 'foo', 'wallets': []}
    #   success: {'funder': '0x..', 'funder_label': 'binance', 'wallets': [...]}
    funder_info = ''
    wallets_list = results
    if isinstance(results, dict):
        if results.get('reason') and not results.get('wallets'):
            await status.edit_text(f"ℹ️ {results['reason']}")
            return
        wallets_list = results.get('wallets') or []
        f_addr = results.get('funder') or ''
        f_label = results.get('funder_label') or ''
        if f_addr:
            funder_info = f"\n_funder: `{fmt_short(f_addr)}`"
            if f_label and f_label != 'unknown':
                funder_info += f" ({f_label})"
            funder_info += "_\n"

    if not wallets_list:
        await status.edit_text("❌ No clones found.")
        return

    lines = [f"🧬 *Clones of* `{wallet}`{funder_info}", ""]
    for i, r in enumerate(wallets_list, 1):
        eth = r.get('eth_received_from_funder', 0)
        lines.append(
            f"*#{i}*  `{r['wallet']}`\n"
            f"   funded: {fmt_ts(r.get('first_funded_ts', 0))} · "
            f"received: {eth:.3f} ETH"
        )
    await status.edit_text("\n".join(lines)[:4000],
                           parse_mode=ParseMode.MARKDOWN,
                           disable_web_page_preview=True)


class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.end_headers()
        self.wfile.write(b'ok')

    def log_message(self, fmt, *args):
        pass  # silence


def start_health_server(port):
    """Run a tiny HTTP server so Render's free web service stays alive
    and so UptimeRobot has something to ping."""
    srv = HTTPServer(('0.0.0.0', port), _HealthHandler)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    log.info(f"health server listening on :{port}")


def main():
    if not TELEGRAM_TOKEN:
        raise SystemExit("Set TELEGRAM_BOT_TOKEN env var (from @BotFather).")
    # Moralis is now optional — only required if WALLET_BOT_USE_MORALIS=1.
    # Free-API stack (Etherscan v2 + GeckoTerminal) replaces it by default.
    if os.environ.get('WALLET_BOT_USE_MORALIS') == '1' and not MORALIS_KEY:
        raise SystemExit("WALLET_BOT_USE_MORALIS=1 requires MORALIS_API_KEY env var.")
    if not ETHERSCAN_KEY:
        log.warning("ETHERSCAN_API_KEY not set — wallet verification + scoring will be degraded.")

    # Keep-alive HTTP endpoint — required by Render/UptimeRobot flow.
    port = int(os.environ.get('PORT') or 0)
    if port:
        start_health_server(port)

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    # Core matching / hunting commands (legacy)
    app.add_handler(CommandHandler('start', start_cmd))
    app.add_handler(CommandHandler('help', start_cmd))
    app.add_handler(CommandHandler('find', find_cmd))
    app.add_handler(CommandHandler('findwallet', findwallet_cmd))
    app.add_handler(CommandHandler('searchtimes', searchtimes_cmd))
    app.add_handler(CommandHandler('debug', debug_cmd))
    app.add_handler(CommandHandler('clearcache', clearcache_cmd))

    # Smart-wallet discovery + quality scoring commands (new)
    app.add_handler(CommandHandler('profile', profile_cmd))
    app.add_handler(CommandHandler('findsmart', findsmart_cmd))
    app.add_handler(CommandHandler('topwallets', topwallets_cmd))
    app.add_handler(CommandHandler('earlybuyers', earlybuyers_cmd))
    app.add_handler(CommandHandler('diamondhands', diamondhands_cmd))
    app.add_handler(CommandHandler('prepump', prepump_cmd))
    app.add_handler(CommandHandler('insider', insider_cmd))
    app.add_handler(CommandHandler('soldnear', soldnear_cmd))
    app.add_handler(CommandHandler('copytrade', copytrade_cmd))
    app.add_handler(CommandHandler('clones', clones_cmd))

    conv = ConversationHandler(
        entry_points=[CommandHandler('hunt', hunt_start)],
        states={
            WAIT_CONTRACT: [MessageHandler(filters.TEXT & ~filters.COMMAND, hunt_got_contract)],
            WAIT_INVEST:   [MessageHandler(filters.TEXT & ~filters.COMMAND, hunt_got_invest)],
            WAIT_SOLD:     [MessageHandler(filters.TEXT & ~filters.COMMAND, hunt_got_sold)],
        },
        fallbacks=[CommandHandler('cancel', hunt_cancel)],
    )
    app.add_handler(conv)

    log.info("Bot starting...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()
