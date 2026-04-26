"""Wake-up-call signal bot — Telegram entrypoint.

Runs a 60-second background watcher that polls GeckoTerminal for trending +
new pools, computes velocity, enriches survivors with safety data
(honeypot.is + Etherscan), scores, and broadcasts to subscribers.

Commands:
  /start     - subscribe this chat to alerts
  /stop      - unsubscribe
  /status    - show subscriber count, last scan, last alert
  /threshold - print current detection thresholds
  /scan      - run a one-shot scan now (useful for debugging)
  /test      - send a sample card to verify formatting

Env vars:
  SIGNAL_BOT_TOKEN     - Telegram bot token from @BotFather  (REQUIRED)
  ETHERSCAN_API_KEY    - Etherscan v2 key                    (recommended)
  SIGNAL_BROADCAST_TO  - comma-separated chat IDs that always receive alerts
                         (e.g. a private channel ID for durable broadcast that
                         survives subscriber-file resets).  Optional.
  SIGNAL_SCAN_SECONDS  - watcher tick interval, default 60
  PORT                 - if set, serves a tiny `200 ok` for Render health
"""
import os, sys, time, json, asyncio, logging, html
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

# Reuse the existing free-API stack from the wallet bot
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sources import Cache, GTSource, EtherscanSource
from discovery import Discovery
from quality import WalletQualityScorer
from signal_enrich import SignalEnricher
from signal_engine import SignalEngine, DEFAULT_THRESHOLDS
from signal_format import format_card

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (Application, CommandHandler, ContextTypes)

logging.basicConfig(
    format='%(asctime)s %(name)s %(levelname)s: %(message)s',
    level=logging.INFO,
)
log = logging.getLogger('signal_bot')

# Quiet down libraries
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('telegram').setLevel(logging.WARNING)


# ──────────────────────────────────────────────────────────────────────
# config
# ──────────────────────────────────────────────────────────────────────
BOT_TOKEN     = os.environ.get('SIGNAL_BOT_TOKEN') or ''
ETHERSCAN_KEY = os.environ.get('ETHERSCAN_API_KEY') or ''
BROADCAST_TO  = [
    int(s.strip()) for s in (os.environ.get('SIGNAL_BROADCAST_TO') or '').split(',')
    if s.strip().lstrip('-').isdigit()
]
SCAN_SECONDS  = int(os.environ.get('SIGNAL_SCAN_SECONDS') or '60')

CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         'signal_cache')
SUBS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         'signal_subscribers.json')

cache = Cache(CACHE_DIR, default_ttl=300)
gt = GTSource(cache)
es = EtherscanSource(ETHERSCAN_KEY, cache) if ETHERSCAN_KEY else None
discovery = Discovery(es, gt, cache)
scorer = WalletQualityScorer(es, gt, cache) if es else None
enricher = SignalEnricher(es, cache)
engine = SignalEngine(gt, enricher, es, discovery, cache)

# State
_state = {
    'last_scan_ts':    0,
    'last_alert_ts':   0,
    'scan_runs':       0,
    'alerts_sent':     0,
    'last_signals':    [],
}


# ──────────────────────────────────────────────────────────────────────
# subscribers — file-backed JSON, in-memory dedupe
# ──────────────────────────────────────────────────────────────────────
def _load_subs():
    try:
        if os.path.exists(SUBS_FILE):
            with open(SUBS_FILE) as f:
                v = json.load(f)
                if isinstance(v, list):
                    return set(int(x) for x in v)
    except Exception:
        pass
    return set()


def _save_subs(subs):
    try:
        with open(SUBS_FILE, 'w') as f:
            json.dump(sorted(subs), f)
    except Exception:
        log.exception("failed to save subscribers")


subscribers = _load_subs()


# ──────────────────────────────────────────────────────────────────────
# command handlers
# ──────────────────────────────────────────────────────────────────────
async def start_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    subscribers.add(chat_id)
    _save_subs(subscribers)
    await update.message.reply_text(
        "🔥 *Wake-Up Call Bot*\n\n"
        "You are now subscribed.  I scan Ethereum trending + new pools every "
        f"{SCAN_SECONDS}s and post WAKE-UP CALL cards for tokens with sudden "
        "buy activity that pass safety checks (no honeypots, low tax, low "
        "clog, open-source).\n\n"
        "Commands:\n"
        "`/stop` — unsubscribe\n"
        "`/status` — bot health + last scan info\n"
        "`/threshold` — show detection thresholds\n"
        "`/scan` — force a one-shot scan now\n"
        "`/test` — send a sample card to verify formatting",
        parse_mode=ParseMode.MARKDOWN,
    )


async def stop_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if chat_id in subscribers:
        subscribers.discard(chat_id)
        _save_subs(subscribers)
        await update.message.reply_text("✅ Unsubscribed.")
    else:
        await update.message.reply_text("You weren't subscribed.")


async def status_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    n_subs = len(subscribers)
    last_scan = _state['last_scan_ts']
    last_alert = _state['last_alert_ts']
    age = (time.time() - last_scan) if last_scan else 0
    last_n = len(_state['last_signals'])
    msg = (
        f"*Signal Bot Status*\n\n"
        f"Subscribers: {n_subs}\n"
        f"Broadcast targets: {len(BROADCAST_TO)}\n"
        f"Scan interval: {SCAN_SECONDS}s\n"
        f"Total scans: {_state['scan_runs']}\n"
        f"Alerts sent: {_state['alerts_sent']}\n"
        f"Last scan: {age:.0f}s ago\n"
        f"Last scan signals: {last_n}\n"
        f"Last alert: {(time.time() - last_alert):.0f}s ago"
        if last_alert else f"Last alert: never"
    )
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def threshold_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    lines = ["*Current detection thresholds:*"]
    for k, v in engine.thr.items():
        lines.append(f"• `{k}`: {v}")
    await update.message.reply_text("\n".join(lines),
                                    parse_mode=ParseMode.MARKDOWN)


async def scan_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Force a one-shot scan and report what came back."""
    status = await update.message.reply_text("🔍 Running scan…")
    loop = asyncio.get_running_loop()
    try:
        signals = await loop.run_in_executor(None, lambda: engine.scan())
    except Exception as e:
        log.exception("scan_cmd error")
        await status.edit_text(f"❌ Scan error: {html.escape(str(e)[:200])}")
        return
    if not signals:
        await status.edit_text(
            "✅ Scan complete — 0 signals passed thresholds.\n"
            "_Try `/threshold` to inspect or relax the cutoffs._",
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    eth_px = _safe_eth_price()
    await status.edit_text(f"✅ {len(signals)} signal(s) found:")
    for s in signals:
        try:
            await update.message.reply_text(
                format_card(s, eth_price_usd=eth_px),
                parse_mode=ParseMode.MARKDOWN,
                disable_web_page_preview=True,
            )
        except Exception as e:
            log.exception("send card error")
            await update.message.reply_text(
                f"⚠️ format error for {s.get('token')}: "
                f"{html.escape(str(e)[:100])}"
            )


async def test_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Send a hand-crafted sample card to verify formatting."""
    sample = _sample_signal()
    await update.message.reply_text(
        format_card(sample, eth_price_usd=2300.0),
        parse_mode=ParseMode.MARKDOWN,
        disable_web_page_preview=True,
    )


# ──────────────────────────────────────────────────────────────────────
# watcher — runs on the asyncio loop
# ──────────────────────────────────────────────────────────────────────
async def watcher_loop(app: Application):
    """Background scan loop.  Posts cards to all subscribers + broadcast targets."""
    log.info("watcher started — scan every %ds", SCAN_SECONDS)
    # Stagger the first scan so the bot has time to register handlers
    await asyncio.sleep(10)
    while True:
        try:
            loop = asyncio.get_running_loop()
            t0 = time.time()
            signals = await loop.run_in_executor(None, lambda: engine.scan())
            _state['last_scan_ts'] = int(t0)
            _state['scan_runs']   += 1
            _state['last_signals'] = [s.get('token') for s in signals]
            log.info("scan #%d -> %d signals in %.1fs",
                     _state['scan_runs'], len(signals), time.time() - t0)

            if signals:
                eth_px = _safe_eth_price()
                targets = subscribers | set(BROADCAST_TO)
                for s in signals:
                    text = format_card(s, eth_price_usd=eth_px)
                    for chat_id in list(targets):
                        try:
                            await app.bot.send_message(
                                chat_id=chat_id, text=text,
                                parse_mode=ParseMode.MARKDOWN,
                                disable_web_page_preview=True,
                            )
                            _state['alerts_sent'] += 1
                            _state['last_alert_ts'] = int(time.time())
                        except Exception as e:
                            # If the chat blocked the bot, drop them
                            err = str(e).lower()
                            if 'blocked' in err or 'forbidden' in err:
                                subscribers.discard(chat_id)
                                _save_subs(subscribers)
                                log.info("dropped blocked chat %s", chat_id)
                            else:
                                log.warning("send_message to %s failed: %s",
                                            chat_id, e)
        except Exception:
            log.exception("watcher tick error")
        await asyncio.sleep(SCAN_SECONDS)


# ──────────────────────────────────────────────────────────────────────
# health endpoint (for Render)
# ──────────────────────────────────────────────────────────────────────
class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.end_headers()
        self.wfile.write(b'ok')

    def log_message(self, *_a, **_k):
        return  # silence access logs


def start_health_server(port):
    def _serve():
        try:
            httpd = HTTPServer(('0.0.0.0', port), _HealthHandler)
            log.info("health server listening on :%d", port)
            httpd.serve_forever()
        except Exception:
            log.exception("health server crashed")
    Thread(target=_serve, daemon=True).start()


# ──────────────────────────────────────────────────────────────────────
# helpers
# ──────────────────────────────────────────────────────────────────────
def _safe_eth_price():
    try:
        return float(gt.eth_price_at(int(time.time())) or 0.0)
    except Exception:
        return 0.0


def _sample_signal():
    """Hand-crafted signal that exercises every formatter branch."""
    return {
        'token':            '0x6982508145454ce325ddbe47a25d4ec3d2311933',
        'pool':             '0xa43fe16908251ee70ef74718545e4fe6c5ccec9f',
        'symbol':           'PEPE',
        'name':             'Pepe',
        'source':           'trending',
        'fdv_usd':          17_160.0,
        'reserve_usd':      4_400.0,
        'price_usd':        0.0,
        'pct_1h':           5.0,
        'pct_6h':           8.0,
        'pct_24h':          12.0,
        'pool_age_hours':   18.0,
        'vol_1m_eth':       3.96,
        'vol_5m_eth':       8.0,
        'vol_5m_usd':       18_400.0,
        'n_buys_5m':        26,
        'n_buys_1m':        5,
        'n_sells_5m':       3,
        'unique_buyers_5m': 18,
        'whale_count_5m':   2,
        'buy_eth_5m':       6.5,
        'sell_eth_5m':      1.5,
        'net_buy_pressure': 0.77,
        'velocity_ratio':   4.2,
        'mc_liq_ratio':     3.87,
        'score':            87.0,
        'tier':             '🔥🔥 ELITE',
        'smart_money_overlap': 3,
        'smart_money_pct':     22.0,
        'fresh_wallet_pct':    40.0,
        'avg_buyer_age_days':  62.0,
        'veteran_count':       2,
        'lenses': {
            'velocity':    21.0,
            'dispersion':  15.0,
            'smart_money': 15.0,
            'fresh':       40.0,
            'pressure':    0.77,
            'whales':      2,
            'pool_h':      18.0,
        },
        'enrich': {
            'isHoneypot':      False,
            'sellable':        True,
            'buy_tax':         0.0,
            'sell_tax':        0.0,
            'transfer_tax':    0.0,
            'risk':            'low',
            'risk_level':      1,
            'flags':           [],
            'holders':         495_000,
            'snipers_failed':  0,
            'snipers_success': 0,
            'siphoned':        0,
            'open_source':     True,
            'is_proxy':        False,
            'contract_age_days': 730.0,
            'clog_pct':        0.95,
            'unknown_functions': 2,
        },
    }


# ──────────────────────────────────────────────────────────────────────
# main
# ──────────────────────────────────────────────────────────────────────
def main():
    if not BOT_TOKEN:
        raise SystemExit("Set SIGNAL_BOT_TOKEN env var (from @BotFather).")
    if not ETHERSCAN_KEY:
        log.warning("ETHERSCAN_API_KEY not set — enrichment will be degraded.")

    port = int(os.environ.get('PORT') or 0)
    if port:
        start_health_server(port)

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler('start',     start_cmd))
    app.add_handler(CommandHandler('help',      start_cmd))
    app.add_handler(CommandHandler('stop',      stop_cmd))
    app.add_handler(CommandHandler('status',    status_cmd))
    app.add_handler(CommandHandler('threshold', threshold_cmd))
    app.add_handler(CommandHandler('scan',      scan_cmd))
    app.add_handler(CommandHandler('test',      test_cmd))

    # Spin up the watcher as a post-init background task
    async def _post_init(app):
        asyncio.create_task(watcher_loop(app))
    app.post_init = _post_init

    log.info("Signal bot starting — %d initial subscribers, %d broadcast targets",
             len(subscribers), len(BROADCAST_TO))
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()
