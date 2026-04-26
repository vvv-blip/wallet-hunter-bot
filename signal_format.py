"""WAKE-UP CALL card formatter for the signal bot.

Renders the dict produced by signal_engine.SignalEngine into a Telegram
Markdown message that mirrors the reference card layout:

  🔥 *WAKE-UP CALL*
  $SYM — Display name
  `0xCONTRACT`

  💰 MC $17.16K · 💧 1.9 ETH · ⚖️ MC/LIQ 3.87
  📊 1m vol 3.96 ETH · 26 recent buys (R:5)
  🟢 Buy 0% 🔴 Sell 0% 🐻 Clog 0.95%
  🕐 Deployed 2 years ago · 💧 Initial LP — · 💎 Sellable now
  ⚠️ N unknown functions in contract — DYOR
  📈 score: 87 (velocity 4.2x · 18 unique buyers in 5m)
"""
import time


def format_card(signal, eth_price_usd=None):
    """Return a Telegram-Markdown string for a single signal dict."""
    e = signal.get('enrich') or {}
    sym = (signal.get('symbol') or '').strip() or 'TOKEN'
    name = (signal.get('name') or '').strip()
    # name comes back as "WORD / WETH" — strip the pair half
    if '/' in name:
        name = name.split('/')[0].strip()
    name_part = f" — _{_md_escape(name)}_" if name and name.upper() != sym.upper() else ''

    tier = signal.get('tier') or '🔥 SIGNAL'

    # ── Header ─────────────────────────────────────────────────────────
    lines = []
    lines.append(f"{tier}  *WAKE-UP CALL*")
    lines.append(f"${_md_escape(sym)}{name_part}")
    lines.append(f"`{signal['token']}`")
    lines.append("")

    # ── Row 1: financials ─────────────────────────────────────────────
    mc = _fmt_usd(signal.get('fdv_usd'))
    liq_eth = _eth_amount(signal.get('reserve_usd'), eth_price_usd)
    mcliq = signal.get('mc_liq_ratio')
    mcliq_str = f"{mcliq:.2f}" if mcliq else "—"
    lines.append(f"💰 MC {mc}  ·  💧 {liq_eth}  ·  ⚖️ MC/LIQ {mcliq_str}")

    # ── Row 2: volume / activity ──────────────────────────────────────
    v1 = signal.get('vol_1m_eth') or 0.0
    v5 = signal.get('vol_5m_eth') or 0.0
    n5 = signal.get('n_buys_5m') or 0
    n1 = signal.get('n_buys_1m') or 0
    ub = signal.get('unique_buyers_5m') or 0
    vr = signal.get('velocity_ratio') or 0.0
    turn = signal.get('liq_turnover_1m') or 0.0
    # Liquidity turnover — % of pool turning over in 1m.  Headline metric
    # for tiny-pool firehoses where vol_1m absolute is small but means a lot.
    turn_pct = turn * 100.0
    parts2 = [f"📊 1m: {v1:.2f} ETH"]
    parts2.append(f"5m: {v5:.2f} ETH")
    parts2.append(f"⚡ {vr:.1f}× velocity")
    if turn_pct >= 5.0:
        parts2.append(f"🌊 {turn_pct:.0f}% liq turnover")
    lines.append("  ·  ".join(parts2))
    lines.append(
        f"👥 {n5} buys / {signal.get('n_sells_5m', 0)} sells in 5m  ·  "
        f"{ub} unique buyers (R:{n1})"
    )

    # ── Row 3: WALLET QUALITY (the differentiator) ───────────────────
    sm_overlap = signal.get('smart_money_overlap') or 0
    sm_pct     = signal.get('smart_money_pct') or 0.0
    fresh_pct  = signal.get('fresh_wallet_pct')
    avg_age    = signal.get('avg_buyer_age_days') or 0.0
    veteran    = signal.get('veteran_count') or 0
    whales     = signal.get('whale_count_5m') or 0
    parts3 = []
    if sm_overlap > 0:
        parts3.append(f"💎 *{sm_overlap} smart-money* in buyers ({sm_pct:.0f}%)")
    if fresh_pct is not None:
        if fresh_pct >= 80:
            parts3.append(f"🚨 {fresh_pct:.0f}% fresh wallets (sybil-suspect)")
        elif fresh_pct >= 20:
            parts3.append(f"🆕 {fresh_pct:.0f}% fresh wallets (sample)")
        else:
            parts3.append(f"🧓 {fresh_pct:.0f}% fresh (avg age {avg_age:.0f}d)")
    if whales >= 1:
        parts3.append(f"🐳 {whales} whale buy{'s' if whales > 1 else ''} (≥1 ETH)")
    if parts3:
        lines.append("  ·  ".join(parts3))

    # ── Row 4: net pressure + momentum ──────────────────────────────
    pressure = signal.get('net_buy_pressure') or 0.0
    pressure_emoji = '🟢' if pressure > 0.2 else ('🟡' if pressure > -0.2 else '🔴')
    pct_1h = signal.get('pct_1h') or 0.0
    pct_6h = signal.get('pct_6h') or 0.0
    pct_24h = signal.get('pct_24h') or 0.0
    lines.append(
        f"{pressure_emoji} pressure {pressure:+.2f}  ·  "
        f"1h {_fmt_pct(pct_1h)}  ·  6h {_fmt_pct(pct_6h)}  ·  24h {_fmt_pct(pct_24h)}"
    )

    # ── Row 5: taxes + clog ──────────────────────────────────────────
    bt = e.get('buy_tax')
    st = e.get('sell_tax')
    bt_s = f"{bt:.0f}%" if bt is not None else "?"
    st_s = f"{st:.0f}%" if st is not None else "?"
    clog = e.get('clog_pct')
    clog_s = f"{clog:.2f}%" if clog is not None else "?"
    snip = e.get('snipers_failed') or 0
    parts5 = [f"🟢 Buy {bt_s}", f"🔴 Sell {st_s}", f"🐻 Clog {clog_s}"]
    if snip > 0:
        parts5.append(f"❌ {snip} snipers failed")
    lines.append("  ·  ".join(parts5))

    # ── Row 6: provenance + sellability ─────────────────────────────
    age_str = _fmt_age(e.get('contract_age_days'))
    pool_h = signal.get('pool_age_hours') or 0
    pool_s = f"pool {pool_h:.0f}h old" if 0 < pool_h < 999 else ''
    sellable = e.get('sellable')
    sell_s = "💎 Sellable" if sellable else ("⛔ Not sellable" if sellable is False else "💎 ?")
    risk = e.get('risk') or '?'
    parts6 = [f"🕐 Token deployed {age_str}"]
    if pool_s:
        parts6.append(pool_s)
    if e.get('is_proxy'):
        parts6.append("🔁 proxy")
    parts6.append(sell_s)
    parts6.append(f"risk: {risk}")
    lines.append("  ·  ".join(parts6))

    # ── Row 7: warnings ──────────────────────────────────────────────
    nuf = e.get('unknown_functions') or 0
    if nuf >= 5:
        lines.append(f"⚠️ {nuf} non-standard functions in contract — DYOR")
    if e.get('flags'):
        flags_short = ', '.join(_md_escape(f) for f in e['flags'][:3])
        lines.append(f"⚠️ flags: {flags_short}")

    # ── Row 8: score breakdown ──────────────────────────────────────
    sc = signal.get('score') or 0.0
    lenses = signal.get('lenses') or {}
    lines.append(
        f"\n📈 *Score: {sc:.0f}*  ·  "
        f"vel {lenses.get('velocity', 0):.0f}  ·  "
        f"turn {lenses.get('liq_turn', 0):.0f}  ·  "
        f"disp {lenses.get('dispersion', 0):.0f}  ·  "
        f"smart {lenses.get('smart_money', 0):.0f}"
    )

    # ── Row 9: source label ─────────────────────────────────────────
    src = signal.get('source') or ''
    if src == 'new':
        lines.append("_🆕 sourced from GT new_pools — fresh launch_")
    elif src == 'trending':
        lines.append("_📈 sourced from GT trending_pools_")

    # ── Footer: quick links ─────────────────────────────────────────
    ca = signal['token']
    lines.append(
        f"\n[chart](https://www.geckoterminal.com/eth/pools/{signal['pool']}) · "
        f"[etherscan](https://etherscan.io/address/{ca}) · "
        f"[honeypot](https://honeypot.is/?address={ca}) · "
        f"[dexscreener](https://dexscreener.com/ethereum/{ca})"
    )

    return "\n".join(lines)


def _fmt_pct(x):
    try:
        v = float(x or 0.0)
    except Exception:
        return '0%'
    sign = '+' if v >= 0 else ''
    return f"{sign}{v:.0f}%"


# ----------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------
def _fmt_usd(x):
    try:
        x = float(x or 0)
    except Exception:
        return '—'
    if x >= 1e9:  return f"${x/1e9:.2f}B"
    if x >= 1e6:  return f"${x/1e6:.2f}M"
    if x >= 1e3:  return f"${x/1e3:.2f}K"
    return f"${x:.0f}"


def _eth_amount(usd_amount, eth_price_usd):
    """Convert a USD value to a human ETH string."""
    try:
        if usd_amount is None:
            return '—'
        usd = float(usd_amount)
        if not eth_price_usd or eth_price_usd <= 0:
            return _fmt_usd(usd)
        eth = usd / eth_price_usd
        if eth >= 1000:
            return f"{eth:,.0f} ETH"
        if eth >= 1:
            return f"{eth:.2f} ETH"
        return f"{eth:.3f} ETH"
    except Exception:
        return '—'


def _fmt_age(days):
    try:
        d = float(days or 0)
    except Exception:
        return 'unknown'
    if d <= 0:
        return 'unknown'
    if d < 1.0:
        h = d * 24.0
        if h < 1.0:
            return f"{int(h*60)}m ago"
        return f"{h:.0f}h ago"
    if d < 30:
        return f"{int(d)}d ago"
    if d < 365:
        return f"{int(d/30)}mo ago"
    yrs = d / 365.0
    return f"{yrs:.1f}y ago"


def _md_escape(s):
    """Light Markdown escape for v1 syntax used by python-telegram-bot."""
    if not s:
        return ''
    out = []
    for ch in s:
        if ch in ('_', '*', '`', '['):
            out.append('\\' + ch)
        else:
            out.append(ch)
    return ''.join(out)
