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


# ---- state for conversation flow ----
WAIT_CONTRACT, WAIT_INVEST, WAIT_SOLD = range(3)


async def start_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🔎 *Wallet Hunter Bot*\n\n"
        "Find the wallet(s) that bought and sold a token with amounts closest to yours.\n\n"
        "*Quick usage:*\n"
        "`/find 0xTOKEN 0.5 2.3`\n"
        "(invested 0.5 ETH, sold 2.3 ETH)\n\n"
        "Or use USD:\n"
        "`/find 0xTOKEN $500 $2300`\n\n"
        "*Step-by-step:*\n"
        "`/hunt`   → I'll ask you 3 questions.\n\n"
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
        results, pairs, total = matcher.find_matches(token, inv_eth, sold_eth, top_n=5)
        return results, pairs, total, inv_eth, sold_eth, eth_price

    try:
        results, pairs, total, inv_eth, sold_eth, eth_price = await loop.run_in_executor(None, work)
    except Exception as e:
        log.exception("match error")
        await status.edit_text(f"❌ Error: {html.escape(str(e)[:200])}")
        return

    if not pairs:
        await status.edit_text("❌ No Ethereum/WETH Uniswap pair found for that token on DexScreener.")
        return
    if not results:
        await status.edit_text(
            f"❌ No wallets within ±5% of the targets "
            f"({inv_eth:.4f} ETH bought / {sold_eth:.4f} ETH sold) across {total} scanned wallets. "
            f"Try slightly different amounts."
        )
        return

    eth_price = eth_price or get_eth_price_usd()

    lines = [
        f"🎯 *Matches for* `{fmt_short(token)}`",
        f"_Target: bought {inv_eth:.4f} ETH · sold {sold_eth:.4f} ETH_  (≈${inv_eth*eth_price:,.0f} / ${sold_eth*eth_price:,.0f})",
        f"_Scanned {total} unique wallets across {len(pairs)} pair(s)_",
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
