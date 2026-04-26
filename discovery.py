"""Creative wallet-discovery patterns for the Telegram wallet-hunter bot.

Goes beyond the simple amount-matching pipeline in `matcher.py` by exposing
eight different lenses on a token's trader population.  All methods are
cached in the shared `Cache` and parallelize calls where useful.

Methods (most useful first):

  1. top_traders_by_pnl(token, top_n, network)
     Highest realized PnL traders on a token.  Aggregates GT trades into
     per-wallet eth_in/eth_out and sorts by realized eth_out - eth_in.

  2. early_buyers(token, limit, network)
     First N unique wallets to ever buy a token.  Walks chronological
     ERC20 transfers from any pool to non-pool/non-contract wallets.

  3. diamond_hands(token, min_hold_days, top_n, network)
     Wallets that bought and held — currently still hold most of what
     they bought.  Bought - sold via pool, with a hold-time filter.

  4. pre_pump_buyers(token, pump_multiple, network)
     Wallets that bought before the token's first big pump (Nx in 24h).
     Detects pump hour via hourly OHLCV close ratios.

  5. sold_near_top(token, threshold, top_n, network)
     Wallets that sold within `threshold` * peak_price.  Identifies the
     all-time peak high in OHLCV, then scans GT sells for near-top exits.

  6. copytrade(wallet, days, top_n)
     What is this wallet recently buying?  Net-token-received per
     ERC20 in the last N days, blue-chip-filtered, sorted by recency.

  7. clones(wallet, top_n)
     Wallets sharing a funding source with the given wallet — potential
     sybil cluster or collaborative team.

  8. find_with_score(token, invested_eth, sold_eth, scorer, ...)
     Lightweight find-matches alternative: GT-only candidate discovery
     with composite ranking by amount-distance plus quality score.
"""
import time, math, collections
from concurrent.futures import ThreadPoolExecutor, as_completed


# Tokens excluded from copytrade (stablecoins + blue chips).  Lower-case.
SKIP_TOKENS = {
    '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',  # USDC
    '0xdac17f958d2ee523a2206206994597c13d831ec7',  # USDT
    '0x6b175474e89094c44da98b954eedeac495271d0f',  # DAI
    '0x4fabb145d64652a948d72533023f6e7a623c7c53',  # BUSD
    '0x853d955acef822db058eb8505911ed77f175b99e',  # FRAX
    '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',  # WETH
    '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599',  # WBTC
    '0x5f98805a4e8be255a32880fdec7f6728c6568ba0',  # LUSD
    '0x6c3ea9036406852006290770bedfcaba0e23a0e8',  # PYUSD
}


class Discovery:
    """Eight wallet-discovery methods sharing a Cache, GTSource, and EtherscanSource."""

    def __init__(self, etherscan, gt, cache):
        self.es = etherscan
        self.gt = gt
        self.cache = cache

    # ──────────────────────────────────────────────────────────────────────
    # helpers
    # ──────────────────────────────────────────────────────────────────────
    def _is_pool(self, addr, pool_set):
        return (addr or '').lower() in pool_set

    def _safe_int(self, v, default=0):
        try:
            return int(v)
        except Exception:
            return default

    def _safe_float(self, v, default=0.0):
        try:
            return float(v)
        except Exception:
            return default

    def _token_decimals(self, rows, default=18):
        """Pull decimals from any row that has it (Etherscan tokentx)."""
        for r in rows:
            d = r.get('tokenDecimal')
            if d is not None and d != '':
                try:
                    return int(d)
                except Exception:
                    continue
        return default

    # ──────────────────────────────────────────────────────────────────────
    # 1) top_traders_by_pnl
    # ──────────────────────────────────────────────────────────────────────
    def top_traders_by_pnl(self, token, top_n=20, network='eth'):
        """Highest total-PnL traders on a token (realized + unrealized).

        Diamond hands look like losers when ranked on `eth_out - eth_in`
        alone — they bought but haven't sold yet.  We mark-to-market the
        net token holdings using a robust median price-per-token from the
        same GT trade window, so a wallet that bought 10 ETH worth and
        still holds it at 2x reads as +10 ETH unrealized, not -10 realized.

        Inputs:  token (address), top_n, network
        Outputs: list[{wallet, eth_in, eth_out, net_tokens,
                       pnl_realized_eth, pnl_unrealized_eth, pnl_total_eth,
                       roi, n_buys, n_sells, first_ts, last_ts}]
        Time budget: ~5-15s (GT only).  Cache 5 min per token.
        """
        ck = f'disc_pnl2_{network}_{token.lower()}_n{top_n}'
        cached = self.cache.get(ck, ttl=300)
        if cached is not None:
            return cached

        try:
            trades = self.gt.token_trades(token, network=network, n_pools=15)
        except Exception:
            trades = []

        # Median price-per-token in ETH from the trade sample.  Using the
        # median makes us robust against single-tx outliers (sandwich attacks,
        # tiny dust trades that distort price/token).  Computed BEFORE
        # aggregation so we have a single mark-to-market price per token.
        eth_per_token_samples = []
        for tr in trades:
            eth = self._safe_float(tr.get('eth'))
            tok = self._safe_float(tr.get('token_amt'))
            if eth > 0 and tok > 0:
                eth_per_token_samples.append(eth / tok)
        eth_per_token = (sorted(eth_per_token_samples)
                         [len(eth_per_token_samples) // 2]
                         if eth_per_token_samples else 0.0)

        agg = {}
        for tr in trades:
            w = (tr.get('wallet') or '').lower()
            if not w:
                continue
            kind = tr.get('kind') or ''
            eth = self._safe_float(tr.get('eth'))
            tok = self._safe_float(tr.get('token_amt'))
            ts = self._safe_int(tr.get('ts'))
            d = agg.get(w)
            if d is None:
                d = {'eth_in': 0.0, 'eth_out': 0.0,
                     'tokens_bought': 0.0, 'tokens_sold': 0.0,
                     'n_buys': 0, 'n_sells': 0,
                     'first_ts': ts or 0, 'last_ts': ts or 0}
                agg[w] = d
            if kind == 'buy':
                d['eth_in'] += eth
                d['tokens_bought'] += tok
                d['n_buys'] += 1
            elif kind == 'sell':
                d['eth_out'] += eth
                d['tokens_sold'] += tok
                d['n_sells'] += 1
            if ts:
                if d['first_ts'] == 0 or ts < d['first_ts']:
                    d['first_ts'] = ts
                if ts > d['last_ts']:
                    d['last_ts'] = ts

        out = []
        for w, d in agg.items():
            if d['eth_in'] == 0.0 and d['eth_out'] == 0.0:
                continue
            # GT trade window is finite (~300 trades/pool, recent only).  If we
            # observed a wallet's sells but not their buys, eth_in=0 and the
            # apparent realized PnL is meaningless free money — they paid for
            # the tokens earlier in unrecorded history.  Drop these from the
            # leaderboard so a real diamond-hand (eth_out=0, eth_in>0) ranks
            # correctly while a missing-cost-basis seller doesn't pollute it.
            if d['eth_in'] == 0.0 and d['eth_out'] > 0.0:
                continue
            net_tokens = max(0.0, d['tokens_bought'] - d['tokens_sold'])
            pnl_realized = d['eth_out'] - d['eth_in']
            pnl_unrealized = net_tokens * eth_per_token
            pnl_total = pnl_realized + pnl_unrealized
            # ROI uses total invested (eth_in) as cost basis
            roi = pnl_total / max(d['eth_in'], 0.0001)
            out.append({
                'wallet': w,
                'eth_in': d['eth_in'],
                'eth_out': d['eth_out'],
                'net_tokens': net_tokens,
                'pnl_realized_eth': pnl_realized,
                'pnl_unrealized_eth': pnl_unrealized,
                'pnl_total_eth': pnl_total,
                # Keep `pnl_eth` for backward-compat (old callers / cached UI):
                'pnl_eth': pnl_total,
                'roi': roi,
                'n_buys': d['n_buys'],
                'n_sells': d['n_sells'],
                'first_ts': d['first_ts'],
                'last_ts': d['last_ts'],
            })
        out.sort(key=lambda x: -x['pnl_total_eth'])
        out = out[:top_n]
        if out:
            self.cache.set(ck, out)
        return out

    # ──────────────────────────────────────────────────────────────────────
    # 2) early_buyers
    # ──────────────────────────────────────────────────────────────────────
    def early_buyers(self, token, limit=50, network='eth'):
        """First N unique wallets to ever buy a token.

        Inputs:  token (address), limit, network
        Outputs: list[{wallet, first_buy_ts, eth_at_first_buy, tokens_received}]
        Time budget: ~30s.  Cache 30 min per token.
        Slices to first chronological 5k transfers if dataset is huge.
        """
        ck = f'disc_early_{network}_{token.lower()}_l{limit}'
        cached = self.cache.get(ck, ttl=1800)
        if cached is not None:
            return cached

        t0 = time.time()
        # Pool set
        try:
            pools = self.gt.pools(token, network=network, n=20)
        except Exception:
            pools = []
        pool_set = set(p.lower() for p in pools)

        # Token transfers
        try:
            rows = self.es.tokentx_for_token(token, max_pages=20)
        except Exception:
            rows = []
        if not rows:
            return []

        # Sort ASC by timeStamp
        try:
            rows.sort(key=lambda r: self._safe_int(r.get('timeStamp')))
        except Exception:
            pass

        # If huge, slice to chronological first 5k
        if len(rows) > 20000:
            rows = rows[:5000]

        decimals = self._token_decimals(rows, default=18)
        divisor = 10 ** decimals if decimals else 1

        seen = set()
        first_buys = []  # (wallet, ts, tokens_received, hash)
        for r in rows:
            if time.time() - t0 > 25:
                break
            from_addr = (r.get('from') or '').lower()
            to_addr = (r.get('to') or '').lower()
            if not from_addr or not to_addr:
                continue
            if from_addr not in pool_set:
                continue
            if to_addr in pool_set:
                continue
            if to_addr in seen:
                continue
            # filter contracts (best effort — only check up to limit*2 calls)
            if len(seen) < limit * 2:
                try:
                    if self.es.is_contract(to_addr):
                        # still mark seen so we don't repeatedly check
                        seen.add(to_addr)
                        continue
                except Exception:
                    pass
            seen.add(to_addr)
            ts = self._safe_int(r.get('timeStamp'))
            try:
                amount_raw = self._safe_float(r.get('value'))
                tokens = amount_raw / divisor if divisor else amount_raw
            except Exception:
                tokens = 0.0
            first_buys.append({
                'wallet': to_addr,
                'first_buy_ts': ts,
                'tokens_received': tokens,
                'hash': (r.get('hash') or '').lower(),
            })
            if len(first_buys) >= limit:
                break

        # Best-effort enrichment for first 20: try to find ETH leg via wallet_token_totals.
        # We avoid hammering Etherscan: only do it when we have an api key set.
        enriched = []
        max_enrich = 20 if getattr(self.es, 'key', '') else 0
        for i, fb in enumerate(first_buys):
            eth_at = None
            if i < max_enrich and (time.time() - t0) < 28:
                try:
                    totals = self.es.wallet_token_totals(fb['wallet'], token)
                    # Find first buy in trades list
                    trades = totals.get('trades') or []
                    trades_sorted = sorted(trades, key=lambda t: t.get('ts', 0))
                    for t in trades_sorted:
                        if t.get('kind') == 'buy' and t.get('hash') == fb['hash']:
                            eth_at = t.get('eth')
                            break
                    if eth_at is None and trades_sorted:
                        # fall back to earliest buy
                        for t in trades_sorted:
                            if t.get('kind') == 'buy':
                                eth_at = t.get('eth')
                                break
                except Exception:
                    eth_at = None
            enriched.append({
                'wallet': fb['wallet'],
                'first_buy_ts': fb['first_buy_ts'],
                'eth_at_first_buy': eth_at,
                'tokens_received': fb['tokens_received'],
            })

        if enriched:
            self.cache.set(ck, enriched)
        return enriched

    # ──────────────────────────────────────────────────────────────────────
    # 3) diamond_hands
    # ──────────────────────────────────────────────────────────────────────
    def diamond_hands(self, token, min_hold_days=14, top_n=30, network='eth'):
        """Wallets that bought and held — still hold most of what they bought.

        Inputs:  token, min_hold_days, top_n, network
        Outputs: list[{wallet, total_bought, total_sold, current_held,
                       first_buy_ts, last_activity_ts, hold_days, conviction}]
        Time budget: ~30s.  Cache 30 min per token.
        """
        ck = f'disc_diamond_{network}_{token.lower()}_h{min_hold_days}_n{top_n}'
        cached = self.cache.get(ck, ttl=1800)
        if cached is not None:
            return cached

        try:
            pools = self.gt.pools(token, network=network, n=20)
        except Exception:
            pools = []
        pool_set = set(p.lower() for p in pools)
        if not pool_set:
            return []

        try:
            rows = self.es.tokentx_for_token(token, max_pages=10)
        except Exception:
            rows = []
        if not rows:
            return []

        decimals = self._token_decimals(rows, default=18)
        divisor = 10 ** decimals if decimals else 1

        # Aggregate per-wallet bought/sold via pool
        agg = {}
        for r in rows:
            from_addr = (r.get('from') or '').lower()
            to_addr = (r.get('to') or '').lower()
            if not from_addr or not to_addr:
                continue
            try:
                amt = self._safe_float(r.get('value')) / divisor if divisor else self._safe_float(r.get('value'))
            except Exception:
                amt = 0.0
            ts = self._safe_int(r.get('timeStamp'))

            wallet = None
            mode = None
            if from_addr in pool_set and to_addr not in pool_set:
                wallet = to_addr
                mode = 'buy'
            elif to_addr in pool_set and from_addr not in pool_set:
                wallet = from_addr
                mode = 'sell'
            else:
                continue

            d = agg.get(wallet)
            if d is None:
                d = {'total_bought': 0.0, 'total_sold': 0.0,
                     'n_buys': 0, 'n_sells': 0,
                     'first_buy_ts': 0, 'last_activity_ts': 0}
                agg[wallet] = d

            if mode == 'buy':
                d['total_bought'] += amt
                d['n_buys'] += 1
                if d['first_buy_ts'] == 0 or (ts and ts < d['first_buy_ts']):
                    d['first_buy_ts'] = ts
            else:
                d['total_sold'] += amt
                d['n_sells'] += 1
            if ts and ts > d['last_activity_ts']:
                d['last_activity_ts'] = ts

        now = int(time.time())
        out = []
        for w, d in agg.items():
            current = d['total_bought'] - d['total_sold']
            if current <= 0:
                continue
            if d['total_bought'] <= 0:
                continue
            held_ratio = current / d['total_bought']
            # diamond filter: holding >50% of what they bought, and either no sells
            # or sold less than half.
            if held_ratio < 0.5:
                continue
            if d['n_sells'] > 0 and d['total_sold'] >= d['total_bought'] * 0.5:
                continue
            if d['first_buy_ts'] <= 0:
                continue
            hold_days = (now - d['first_buy_ts']) / 86400.0
            if hold_days < min_hold_days:
                continue
            # filter out the pools themselves and other contracts (best-effort)
            if w in pool_set:
                continue
            conviction = hold_days * math.sqrt(max(current, 0.0))
            out.append({
                'wallet': w,
                'total_bought': d['total_bought'],
                'total_sold': d['total_sold'],
                'current_held': current,
                'n_buys': d['n_buys'],
                'n_sells': d['n_sells'],
                'first_buy_ts': d['first_buy_ts'],
                'last_activity_ts': d['last_activity_ts'],
                'hold_days': hold_days,
                'conviction': conviction,
            })
        # Sort by conviction (hold_days * sqrt(current_held))
        out.sort(key=lambda x: -x['conviction'])
        out = out[:top_n]
        if out:
            self.cache.set(ck, out)
        return out

    # ──────────────────────────────────────────────────────────────────────
    # 4) pre_pump_buyers
    # ──────────────────────────────────────────────────────────────────────
    def pre_pump_buyers(self, token, pump_multiple=5.0, network='eth'):
        """Wallets that bought before the token's first big pump (Nx in <24h).

        Inputs:  token, pump_multiple, network
        Outputs: list[{wallet, first_buy_ts, current_pos}] OR
                 {'reason': 'no pump detected', 'wallets': []}
        Time budget: ~30s.  Cache 1h.
        """
        ck = f'disc_prepump_{network}_{token.lower()}_m{pump_multiple}'
        cached = self.cache.get(ck, ttl=3600)
        if cached is not None:
            return cached

        # Find pump start
        try:
            pools = self.gt.pools(token, network=network, n=5)
        except Exception:
            pools = []
        if not pools:
            res = {'reason': 'no pools found', 'wallets': []}
            return res

        ohlcv = []
        for p in pools[:3]:
            try:
                ohlcv = self.gt.pool_ohlcv(p, timeframe='hour', limit=1000, network=network)
                if ohlcv:
                    break
            except Exception:
                continue
        if not ohlcv:
            return {'reason': 'no ohlcv data', 'wallets': []}

        pump_start_ts = None
        for i in range(24, len(ohlcv)):
            close_now = ohlcv[i][4]
            close_prev = ohlcv[i - 24][4]
            if close_prev > 0 and (close_now / close_prev) >= pump_multiple:
                pump_start_ts = ohlcv[i - 24][0]
                break

        if pump_start_ts is None:
            res = {'reason': 'no pump detected', 'wallets': []}
            self.cache.set(ck, res)
            return res

        # Pull token-wide transfers and pool set
        pool_set = set(p.lower() for p in pools)
        try:
            rows = self.es.tokentx_for_token(token, max_pages=15)
        except Exception:
            rows = []
        if not rows:
            res = {'pump_start_ts': pump_start_ts, 'reason': 'no transfer data',
                   'wallets': []}
            return res

        # Sort ASC
        rows.sort(key=lambda r: self._safe_int(r.get('timeStamp')))
        decimals = self._token_decimals(rows, default=18)
        divisor = 10 ** decimals if decimals else 1

        seen = set()
        out = []
        for r in rows:
            ts = self._safe_int(r.get('timeStamp'))
            if ts >= pump_start_ts:
                break
            from_addr = (r.get('from') or '').lower()
            to_addr = (r.get('to') or '').lower()
            if from_addr not in pool_set or to_addr in pool_set:
                continue
            if to_addr in seen:
                continue
            seen.add(to_addr)
            try:
                tokens = self._safe_float(r.get('value')) / divisor if divisor else 0.0
            except Exception:
                tokens = 0.0
            out.append({
                'wallet': to_addr,
                'first_buy_ts': ts,
                'tokens_received': tokens,
                'current_pos': None,  # filled in optionally below
            })
            if len(out) >= 50:
                break

        # Optional best-effort current_pos for first 10 with etherscan key
        if getattr(self.es, 'key', ''):
            for i, w in enumerate(out[:10]):
                try:
                    totals = self.es.wallet_token_totals(w['wallet'], token)
                    w['current_pos'] = {
                        'eth_in': totals.get('eth_in'),
                        'eth_out': totals.get('eth_out'),
                        'n_buys': totals.get('n_buys'),
                        'n_sells': totals.get('n_sells'),
                    }
                except Exception:
                    pass

        result = {'pump_start_ts': pump_start_ts, 'wallets': out}
        self.cache.set(ck, result)
        return result

    # ──────────────────────────────────────────────────────────────────────
    # 5) sold_near_top
    # ──────────────────────────────────────────────────────────────────────
    def sold_near_top(self, token, threshold=0.8, top_n=30, network='eth'):
        """Wallets that sold within `threshold` * peak_price.

        Inputs:  token, threshold (0..1), top_n, network
        Outputs: list[{wallet, top_eth_out, n_top_sells, peak_ts, peak_price}]
        Time budget: ~10s (GT only).  Cache 15 min.
        """
        ck = f'disc_sotop_{network}_{token.lower()}_t{threshold}_n{top_n}'
        cached = self.cache.get(ck, ttl=900)
        if cached is not None:
            return cached

        try:
            pools = self.gt.pools(token, network=network, n=5)
        except Exception:
            pools = []
        if not pools:
            return []

        ohlcv = []
        for p in pools[:3]:
            try:
                ohlcv = self.gt.pool_ohlcv(p, timeframe='hour', limit=1000, network=network)
                if ohlcv:
                    break
            except Exception:
                continue
        if not ohlcv:
            return []

        # Peak high
        peak_price = 0.0
        peak_ts = 0
        for row in ohlcv:
            if row[2] > peak_price:
                peak_price = row[2]
                peak_ts = row[0]
        if peak_price <= 0:
            return []

        threshold_price = peak_price * threshold

        # Aggregate near-top sells from GT trades
        try:
            trades = self.gt.token_trades(token, network=network, n_pools=15)
        except Exception:
            trades = []

        agg = {}
        for tr in trades:
            if (tr.get('kind') or '') != 'sell':
                continue
            token_amt = self._safe_float(tr.get('token_amt'))
            usd = self._safe_float(tr.get('usd'))
            if token_amt <= 0 or usd <= 0:
                continue
            # Inferred USD-per-token
            price_at_trade = usd / token_amt
            if price_at_trade < threshold_price:
                continue
            w = (tr.get('wallet') or '').lower()
            if not w:
                continue
            d = agg.get(w)
            if d is None:
                d = {'top_eth_out': 0.0, 'n_top_sells': 0}
                agg[w] = d
            d['top_eth_out'] += self._safe_float(tr.get('eth'))
            d['n_top_sells'] += 1

        out = []
        for w, d in agg.items():
            if d['top_eth_out'] <= 0:
                continue
            out.append({
                'wallet': w,
                'top_eth_out': d['top_eth_out'],
                'n_top_sells': d['n_top_sells'],
                'peak_ts': peak_ts,
                'peak_price': peak_price,
            })
        out.sort(key=lambda x: -x['top_eth_out'])
        out = out[:top_n]
        if out:
            self.cache.set(ck, out)
        return out

    # ──────────────────────────────────────────────────────────────────────
    # 6) copytrade
    # ──────────────────────────────────────────────────────────────────────
    def copytrade(self, wallet, days=7, top_n=20):
        """What is this wallet recently buying?

        Inputs:  wallet, days, top_n
        Outputs: list[{token_addr, token_symbol, last_buy_ts, net_received,
                       n_buys, current_price_usd}]
        Time budget: ~20s.  Cache 10 min per wallet.
        """
        wlow = wallet.lower()
        ck = f'disc_copy_{wlow}_d{days}_n{top_n}'
        cached = self.cache.get(ck, ttl=600)
        if cached is not None:
            return cached

        cutoff = int(time.time()) - days * 86400

        try:
            rows = self.es.tokentx_for_wallet(wallet, max_pages=2)
        except Exception:
            rows = []
        if not rows:
            return []

        # Group by contractAddress
        groups = collections.defaultdict(lambda: {
            'received': 0.0, 'sent': 0.0,
            'n_buys': 0, 'n_sells': 0,
            'last_buy_ts': 0, 'symbol': '', 'decimals': 18,
        })
        for r in rows:
            ts = self._safe_int(r.get('timeStamp'))
            if ts < cutoff:
                continue
            ca = (r.get('contractAddress') or '').lower()
            if not ca or ca in SKIP_TOKENS:
                continue
            to_addr = (r.get('to') or '').lower()
            from_addr = (r.get('from') or '').lower()
            try:
                decimals = int(r.get('tokenDecimal') or 18)
            except Exception:
                decimals = 18
            divisor = 10 ** decimals if decimals else 1
            try:
                amt = self._safe_float(r.get('value')) / divisor if divisor else 0.0
            except Exception:
                amt = 0.0
            g = groups[ca]
            g['symbol'] = r.get('tokenSymbol') or g['symbol']
            g['decimals'] = decimals
            if to_addr == wlow and from_addr != wlow:
                g['received'] += amt
                g['n_buys'] += 1
                if ts > g['last_buy_ts']:
                    g['last_buy_ts'] = ts
            elif from_addr == wlow and to_addr != wlow:
                g['sent'] += amt
                g['n_sells'] += 1

        candidates = []
        for ca, g in groups.items():
            net = g['received'] - g['sent']
            if net <= 0:
                continue  # net sold/closed
            if g['n_buys'] <= 0:
                continue
            candidates.append({
                'token_addr': ca,
                'token_symbol': g['symbol'],
                'last_buy_ts': g['last_buy_ts'],
                'net_received': net,
                'n_buys': g['n_buys'],
                'current_price_usd': None,
            })

        # Sort by recency × log(net_received)
        candidates.sort(key=lambda x: -(x['last_buy_ts'] + math.log(max(x['net_received'], 1)) * 100))
        candidates = candidates[:top_n]

        # Best-effort price enrichment in parallel
        def _price(token_addr):
            try:
                pools = self.gt.pools(token_addr, n=1)
                if not pools:
                    return None
                rows = self.gt.pool_ohlcv(pools[0], timeframe='hour', limit=2)
                if not rows:
                    return None
                return rows[-1][4]
            except Exception:
                return None

        deadline = time.time() + 8
        with ThreadPoolExecutor(max_workers=4) as ex:
            futs = {ex.submit(_price, c['token_addr']): c for c in candidates}
            for fut in as_completed(futs):
                if time.time() > deadline:
                    break
                c = futs[fut]
                try:
                    px = fut.result()
                    if px is not None:
                        c['current_price_usd'] = px
                except Exception:
                    pass

        if candidates:
            self.cache.set(ck, candidates)
        return candidates

    # ──────────────────────────────────────────────────────────────────────
    # 7) clones
    # ──────────────────────────────────────────────────────────────────────
    def clones(self, wallet, top_n=20):
        """Wallets sharing a funding source with the given wallet.

        Inputs:  wallet, top_n
        Outputs: list[{wallet, first_funded_ts, eth_received_from_funder}]
                 OR {'reason': '...', 'wallets': []}
        Time budget: ~15s.  Cache 1h.
        """
        wlow = wallet.lower()
        ck = f'disc_clones_{wlow}_n{top_n}'
        cached = self.cache.get(ck, ttl=3600)
        if cached is not None:
            return cached

        try:
            funder, label = self.es.wallet_funding_source(wallet)
        except Exception:
            return {'reason': 'funder lookup failed', 'wallets': []}

        if not funder:
            return {'reason': 'no funder found', 'wallets': []}

        # If this is a labeled CEX or it's a generic 'unknown' EOA we might still scan
        # because the spec says: CEX or unknown EOA → too broad.  Detect contract.
        is_cex = label and label != 'unknown'
        if is_cex:
            res = {'reason': f'funding source too generic (CEX: {label})', 'wallets': []}
            self.cache.set(ck, res)
            return res

        # Determine if funder is a contract or unknown EOA.  If unknown EOA we still
        # check its outflow count via txlist; if it's a huge wallet we bail.
        try:
            funder_txs = self.es.txlist(funder, max_pages=3, sort='desc')
        except Exception:
            funder_txs = []

        if not funder_txs:
            res = {'reason': 'funder has no txs', 'wallets': []}
            self.cache.set(ck, res)
            return res

        # If funder has 25k+ outflows, it's effectively another exchange/router
        if len(funder_txs) >= 25000:
            res = {'reason': 'funding source too generic (high-volume EOA)', 'wallets': []}
            self.cache.set(ck, res)
            return res

        # Aggregate outgoing recipients
        agg = {}
        for r in funder_txs:
            from_addr = (r.get('from') or '').lower()
            to_addr = (r.get('to') or '').lower()
            if from_addr != funder.lower():
                continue
            if not to_addr:
                continue
            try:
                val = self._safe_float(r.get('value')) / 1e18
            except Exception:
                val = 0.0
            ts = self._safe_int(r.get('timeStamp'))
            d = agg.get(to_addr)
            if d is None:
                d = {'eth_received': 0.0, 'first_funded_ts': ts or 0}
                agg[to_addr] = d
            d['eth_received'] += val
            if ts and (d['first_funded_ts'] == 0 or ts < d['first_funded_ts']):
                d['first_funded_ts'] = ts

        out = []
        for w, d in agg.items():
            if w == wlow:
                continue
            if d['eth_received'] <= 0:
                continue
            out.append({
                'wallet': w,
                'first_funded_ts': d['first_funded_ts'],
                'eth_received_from_funder': d['eth_received'],
            })
        out.sort(key=lambda x: -x['eth_received_from_funder'])
        out = out[:top_n]

        result = {'funder': funder, 'funder_label': label, 'wallets': out}
        self.cache.set(ck, result)
        return result

    # ──────────────────────────────────────────────────────────────────────
    # 8) find_with_score
    # ──────────────────────────────────────────────────────────────────────
    def find_with_score(self, token, invested_eth, sold_eth, scorer,
                        top_n=10, tol=0.05, network='eth'):
        """Find amount-matching wallets enriched by a quality scorer.

        Inputs:  token, invested_eth, sold_eth target, scorer (must expose
                 scorer.score(wallet) -> {overall, rating, flags}),
                 top_n, tol, network
        Outputs: list[{wallet, eth_in, eth_out, match_dist, quality_score,
                       quality_rating, quality_flags}]
        Time budget: ~30-60s (depends on scorer).  No cache (scorer is mutable).
        """
        try:
            trades = self.gt.token_trades(token, network=network, n_pools=15)
        except Exception:
            trades = []

        agg = {}
        for tr in trades:
            w = (tr.get('wallet') or '').lower()
            if not w:
                continue
            kind = tr.get('kind') or ''
            eth = self._safe_float(tr.get('eth'))
            d = agg.get(w)
            if d is None:
                d = {'eth_in': 0.0, 'eth_out': 0.0}
                agg[w] = d
            if kind == 'buy':
                d['eth_in'] += eth
            elif kind == 'sell':
                d['eth_out'] += eth

        inv_lo = invested_eth * (1 - tol)
        inv_hi = invested_eth * (1 + tol)
        sold_lo = sold_eth * (1 - tol) if sold_eth > 0 else 0.0
        sold_hi = sold_eth * (1 + tol) if sold_eth > 0 else 0.0

        # Filter
        candidates = []
        for w, d in agg.items():
            if not (inv_lo <= d['eth_in'] <= inv_hi):
                continue
            if sold_eth > 0 and not (sold_lo <= d['eth_out'] <= sold_hi):
                continue
            # match distance: relative L2 between target and observed
            di = (d['eth_in'] - invested_eth) / max(invested_eth, 0.0001)
            ds = 0.0
            if sold_eth > 0:
                ds = (d['eth_out'] - sold_eth) / max(sold_eth, 0.0001)
            match_dist = math.sqrt(di * di + ds * ds)
            candidates.append({
                'wallet': w,
                'eth_in': d['eth_in'],
                'eth_out': d['eth_out'],
                'match_dist': match_dist,
            })

        # Limit to top 30 by amount-distance for scoring
        candidates.sort(key=lambda x: x['match_dist'])
        scoring_pool = candidates[:30]

        # Score in parallel (max_workers=4 — etherscan is the bottleneck)
        def _score(c):
            try:
                q = scorer.score(c['wallet'])
            except Exception:
                q = None
            return c, q

        scored = []
        with ThreadPoolExecutor(max_workers=4) as ex:
            futs = [ex.submit(_score, c) for c in scoring_pool]
            for fut in as_completed(futs):
                try:
                    c, q = fut.result()
                except Exception:
                    continue
                overall = 50.0
                rating = 'unknown'
                flags = []
                if isinstance(q, dict):
                    overall = self._safe_float(q.get('overall'), 50.0)
                    rating = q.get('rating') or 'unknown'
                    flags = q.get('flags') or []
                # Composite: lower is better.  match_dist is unitless; quality term
                # in the same scale.
                composite = c['match_dist'] + (100.0 - overall) / 50.0
                scored.append({
                    'wallet': c['wallet'],
                    'eth_in': c['eth_in'],
                    'eth_out': c['eth_out'],
                    'match_dist': c['match_dist'],
                    'quality_score': overall,
                    'quality_rating': rating,
                    'quality_flags': flags,
                    'composite': composite,
                })

        scored.sort(key=lambda x: x['composite'])
        return scored[:top_n]

    # ──────────────────────────────────────────────────────────────────────
    # 9) insider_buyers
    # ──────────────────────────────────────────────────────────────────────
    def insider_buyers(self, token, scorer, max_age_days=60, min_score=65,
                       early_pool_size=100, top_n=15, network='eth'):
        """Fresh wallets (age <= max_age_days) that bought the token early
        AND have high overall quality scores.  Highest-signal alpha pattern:
        a fresh wallet buying chronologically-first frequently indicates
        insider knowledge or pre-launch tipoff.  Combines `early_buyers`
        (chronological priority) with `scorer.score()` (quality filter).

        Inputs:  token, scorer, max_age_days, min_score, early_pool_size, top_n
        Outputs: list[{wallet, first_buy_ts, age_days, quality_score,
                       quality_rating, quality_flags, tokens_received,
                       composite (lower=better)}]
        Time budget: ~30-60s.  Cache 30 min per token+threshold combo.
        """
        ck = (f'disc_insider_{network}_{token.lower()}'
              f'_age{max_age_days}_score{min_score}_n{top_n}')
        cached = self.cache.get(ck, ttl=1800)
        if cached is not None:
            return cached

        # Need both an Etherscan source (for tokentx genesis walk and
        # wallet-age) and a scorer.  Without etherscan early_buyers returns [].
        if not self.es:
            return {'reason': 'requires ETHERSCAN_API_KEY for genesis walk',
                    'wallets': []}
        if scorer is None:
            return {'reason': 'requires WalletQualityScorer', 'wallets': []}

        try:
            early = self.early_buyers(token, limit=early_pool_size,
                                      network=network)
        except Exception:
            early = []
        if not early:
            return {'reason': 'no early buyers found (token may be too new '
                              'or transfers not indexed yet)', 'wallets': []}

        # Score the early-buyer pool.  Etherscan is the bottleneck so cap
        # parallelism at 4.
        def _score(entry):
            try:
                q = scorer.score(entry['wallet'])
            except Exception:
                q = None
            return entry, q

        scored = []
        with ThreadPoolExecutor(max_workers=4) as ex:
            futs = [ex.submit(_score, e) for e in early]
            for fut in as_completed(futs):
                try:
                    e, q = fut.result()
                except Exception:
                    continue
                if not isinstance(q, dict):
                    continue
                overall = self._safe_float(q.get('overall'), 0.0)
                meta = q.get('meta') or {}
                age_days = self._safe_float(meta.get('age_days'), 999.0)

                # Filter: must be fresh AND high quality
                if age_days > max_age_days:
                    continue
                if overall < min_score:
                    continue

                # Composite: prefer younger wallets with higher scores.
                # age_days/10 gives 0..6 penalty for 0..60 days; we then
                # subtract from a high baseline so lower=better is consistent
                # with other discovery rankers.
                composite = (100.0 - overall) + (age_days / 10.0)

                scored.append({
                    'wallet': e['wallet'],
                    'first_buy_ts': e.get('first_buy_ts', 0),
                    'tokens_received': e.get('tokens_received', 0.0),
                    'eth_at_first_buy': e.get('eth_at_first_buy'),
                    'age_days': age_days,
                    'quality_score': overall,
                    'quality_rating': q.get('rating') or 'unknown',
                    'quality_flags': q.get('flags') or [],
                    'composite': composite,
                })

        scored.sort(key=lambda x: x['composite'])
        scored = scored[:top_n]
        if scored:
            self.cache.set(ck, scored)
        return scored

    # ──────────────────────────────────────────────────────────────────────
    # 10) scout_wallet — one-shot wallet research summary
    # ──────────────────────────────────────────────────────────────────────
    def scout_wallet(self, wallet, scorer, copytrade_days=7,
                     copytrade_top=8, clones_top=5):
        """One-page consolidated wallet research.  Returns the union of
        /profile + /copytrade + /clones in a single response so users
        don't have to run three commands to evaluate a wallet.

        Inputs:  wallet, scorer (WalletQualityScorer)
        Outputs: {wallet, profile, recent_buys, clones, profile_error,
                  copytrade_error, clones_error}
        Time budget: ~30-45s.  Cache 30 min per wallet.
        """
        wlow = wallet.lower()
        ck = f'disc_scout_{wlow}_d{copytrade_days}'
        cached = self.cache.get(ck, ttl=1800)
        if cached is not None:
            return cached

        result = {
            'wallet': wlow,
            'profile': None,
            'recent_buys': [],
            'clones': None,
            'profile_error': None,
            'copytrade_error': None,
            'clones_error': None,
        }

        # Three sub-tasks parallelized — they share Etherscan rate limit
        # but the leaky bucket inside EtherscanSource sequences them
        # safely.  Each is wrapped in try/except so a partial failure
        # still returns whatever else succeeded.
        def _profile():
            try:
                if scorer is None:
                    return ('error', 'no scorer')
                return ('ok', scorer.score(wallet))
            except Exception as e:
                return ('error', f'{type(e).__name__}: {str(e)[:80]}')

        def _copy():
            try:
                return ('ok', self.copytrade(wallet, days=copytrade_days,
                                             top_n=copytrade_top))
            except Exception as e:
                return ('error', f'{type(e).__name__}: {str(e)[:80]}')

        def _cln():
            try:
                if not self.es:
                    return ('error', 'no etherscan')
                return ('ok', self.clones(wallet, top_n=clones_top))
            except Exception as e:
                return ('error', f'{type(e).__name__}: {str(e)[:80]}')

        with ThreadPoolExecutor(max_workers=3) as ex:
            f_prof = ex.submit(_profile)
            f_copy = ex.submit(_copy)
            f_cln = ex.submit(_cln)
            for which, fut in [('profile', f_prof), ('copy', f_copy),
                               ('cln', f_cln)]:
                try:
                    status, val = fut.result(timeout=60)
                except Exception as e:
                    status, val = 'error', str(e)[:80]
                if which == 'profile':
                    if status == 'ok':
                        result['profile'] = val
                    else:
                        result['profile_error'] = val
                elif which == 'copy':
                    if status == 'ok':
                        result['recent_buys'] = val or []
                    else:
                        result['copytrade_error'] = val
                else:  # cln
                    if status == 'ok':
                        result['clones'] = val
                    else:
                        result['clones_error'] = val

        self.cache.set(ck, result)
        return result
