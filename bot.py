"""Telegram bot: /find <contract> <invested> <sold>  ->  wallets with closest trade footprint.

Accepts:
  - ETH amounts:  "0.5", "0.5eth", "0.5 ETH"
  - USD amounts:  "$500", "500usd", "500 USD"

Env vars required:
  TELEGRAM_BOT_TOKEN   (from @BotFather)
  ETHERSCAN_API_KEY    (etherscan.io/v2 key)

Optional:
  BOT_CACHE_DIR        (default: /tmp/wallet_bot_cache)
"""
import os, re, asyncio, html, logging, threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, ConversationHandler

from matcher import Matcher, get_eth_price_usd

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
log = logging.getLogger('walletbot')

TELEGRAM_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN') or ''
ETHERSCAN_KEY = os.environ.get('ETHERSCAN_API_KEY') or ''
CACHE_DIR = os.environ.get('BOT_CACHE_DIR', '/tmp/wallet_bot_cache')

matcher = Matcher(etherscan_key=ETHERSCAN_KEY, cache_dir=CACHE_DIR)

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
        "🔎 *Wallet Hunter Bot*\n\n"
        "Find wallets that traded a token with footprints similar to yours.\n\n"
        "*By amounts:*\n"
        "`/find 0xTOKEN 0.5 2.3`\n"
        "(bought 0.5 ETH, sold 2.3 ETH — matches within ±5%)\n"
        "Or in USD: `/find 0xTOKEN $500 $2300`\n\n"
        "*By time windows:*\n"
        "`/searchtimes 0xTOKEN <minBuy> <maxBuy> <minSell> <maxSell>`\n"
        "Dates: `YYYY-MM-DD`, `YYYY-MM-DDTHH:MM`, or unix seconds. "
        "Use `_` for an open bound.\n"
        "Example: `/searchtimes 0xTOKEN 2026-03-01 2026-03-15 2026-04-01 2026-04-20`\n\n"
        "*Step-by-step amount search:*\n"
        "`/hunt`   → I'll ask you 3 questions.\n\n"
        "*Diagnostics:*\n"
        "`/debug 0xTOKEN 0xWALLET` — show every buy/sell the bot sees for a wallet "
        "(use when a wallet you expect isn't appearing).\n\n"
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
async def find_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = ctx.args
    if len(args) < 3:
        await update.message.reply_text(
            "Usage: `/find <contract> <invested> <sold>`\n"
            "Example: `/find 0xf280b16ef293d8e534e370794ef26bf312694126 0.5 2.3`",
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
    await _run_match(update, ctx, token, iu, iv, su, sv)


async def _run_match(update, ctx, token, inv_unit, inv_val, sold_unit, sold_val):
    status = await update.message.reply_text(
        f"⏳ Scanning {fmt_short(token)} on Uniswap… (10–90 s)"
    )

    loop = asyncio.get_running_loop()

    def work():
        eth_price = None
        if inv_unit == 'usd' or sold_unit == 'usd':
            eth_price = get_eth_price_usd()
        inv_eth = inv_val / eth_price if inv_unit == 'usd' else inv_val
        sold_eth = sold_val / eth_price if sold_unit == 'usd' else sold_val
        results, pairs, filt = matcher.find_matches(token, inv_eth, sold_eth, top_n=5)
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
            f"• {filt['total']} total wallets scanned\n"
            f"• {filt['inv_ok']} within ±5% on invested\n"
            f"• {filt['sell_ok']} also within ±5% on sold\n\n"
            f"Try widening the amounts, or the target wallet may have traded via a router we "
            f"couldn't resolve.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    eth_price = eth_price or get_eth_price_usd()

    lines = [
        f"🎯 *Matches for* `{fmt_short(token)}`",
        f"_Target: bought {inv_eth:.4f} ETH · sold {sold_eth:.4f} ETH_  (≈${inv_eth*eth_price:,.0f} / ${sold_eth*eth_price:,.0f})",
        f"_Scanned {filt['total']} wallets · {filt['inv_ok']} matched invest · {filt['sell_ok']} matched both_",
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

    s = info['stats']
    is_c = info['is_contract']
    direct_n = info['direct_n']
    direct_buys = info['direct_buys']
    direct_sells = info['direct_sells']
    cpties = info['direct_counterparties']
    missed = info['missed_counterparties']
    pool_health = info.get('pool_health', [])
    hash_diag = info.get('hash_diag', [])

    lines = [
        f"🔍 *Debug* `{wallet}` on `{fmt_short(token)}`",
        f"Is contract: {is_c}",
        f"Pools scanned: {len(info['pools_scanned'])} / {info['n_pools_total']}",
        "",
        f"*Direct Etherscan tokentx (ground truth):*",
        f"  {direct_n} transfers · {direct_buys} received · {direct_sells} sent",
    ]

    if direct_n == 0:
        lines += [
            "",
            "❌ Etherscan says this wallet never moved this token on mainnet.",
            "",
            "Possible causes:",
            "• wrong token contract address?",
            "• wallet traded on another chain (Base / BSC / etc)?",
        ]
        await status.edit_text("\n".join(lines)[:4000],
                               parse_mode=ParseMode.MARKDOWN,
                               disable_web_page_preview=True)
        return

    # counterparties
    lines.append("")
    lines.append("*Counterparties:*")
    for addr, n in sorted(cpties.items(), key=lambda x: -x[1])[:6]:
        flag = "❌" if addr in missed else "✅"
        lines.append(f"  {flag} `{fmt_short(addr)}` ×{n}")

    # pool health — is each scanned "pool" really a WETH pool?
    if pool_health:
        lines.append("")
        lines.append("*Pool health (tokTx / wethTx):*")
        for ph in pool_health[:6]:
            ok = "✅" if ph['weth_tx'] > 10 else "⚠️ NOT WETH"
            lines.append(f"  {ok} `{fmt_short(ph['addr'])}` {ph['token_tx']} / {ph['weth_tx']}")

    # per-hash: where did the WETH leg land?
    if hash_diag:
        lines.append("")
        lines.append(f"*WETH-leg join (first {len(hash_diag)} of wallet's trades):*")
        for hd in hash_diag:
            found = fmt_short(hd['found_pool']) if hd['found_pool'] else '— NOT FOUND —'
            lines.append(
                f"  {hd['kind']:4} {hd['eth_leg']:.4f} ETH  via `{found}`"
            )

    # aggregated stats
    if s:
        lines += [
            "",
            f"*Matcher aggregated:*",
            f"  bought: {s['eth_in']:.4f} ETH ({s['n_buys']}×)",
            f"  sold:   {s['eth_out']:.4f} ETH ({s['n_sells']}×)",
        ]
    else:
        lines += ["", "⚠️ Matcher aggregated nothing."]

    text = "\n".join(lines)[:4000]
    await status.edit_text(text, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)


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
    if not ETHERSCAN_KEY:
        raise SystemExit("Set ETHERSCAN_API_KEY env var.")

    # Keep-alive HTTP endpoint — required by Render/UptimeRobot flow.
    port = int(os.environ.get('PORT') or 0)
    if port:
        start_health_server(port)

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler('start', start_cmd))
    app.add_handler(CommandHandler('help', start_cmd))
    app.add_handler(CommandHandler('find', find_cmd))
    app.add_handler(CommandHandler('searchtimes', searchtimes_cmd))
    app.add_handler(CommandHandler('debug', debug_cmd))
    app.add_handler(CommandHandler('clearcache', clearcache_cmd))

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
