"""Wallet match logic, backed by Moralis decoded-swaps APIs.

For every (token, wallet) pair we fetch already-decoded swap rows from Moralis:
  - token-level scan       -> /erc20/{token}/swaps          (or Solana equivalent)
  - per-wallet exact totals -> /wallets/{wallet}/swaps       (or Solana equivalent)

Moralis returns each swap with the wallet that signed, the bought/sold legs
(amount + symbol + USD), the pair address, and the DEX name.  This works
across every Uniswap version (incl. V4 singleton pools), Sushi, Curve, etc.,
and on Solana across Raydium, Orca, Meteora, Jupiter aggregator, Pump.fun, etc.

The chain layer is a thin dict, so adding Solana later is a 5-line change.
"""
import os, time, json, math, requests
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---- chain config -----------------------------------------------------------

WETH = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'

CHAINS = {
    'eth': {
        'moralis_base': 'https://deep-index.moralis.io/api/v2.2',
        'wallet_swaps_path': '/wallets/{wallet}/swaps',
        'token_swaps_path': '/erc20/{token}/swaps',
        'chain_param': 'eth',
        'native_symbols': ('WETH', 'ETH'),
        'native_label': 'ETH',
    },
    'sol': {
        'moralis_base': 'https://solana-gateway.moralis.io',
        'wallet_swaps_path': '/account/mainnet/{wallet}/swaps',
        'token_swaps_path': '/token/mainnet/{token}/swaps',
        'chain_param': None,
        'native_symbols': ('SOL', 'WSOL'),
        'native_label': 'SOL',
    },
}

# ----------------------------------------------------------------------------


class Matcher:
    def __init__(self, etherscan_key='', moralis_key='', cache_dir='/tmp/wallet_bot_cache',
                 cache_ttl=1800):
        self.esk = etherscan_key
        self.mk = moralis_key
        self.cache_dir = cache_dir
        self.cache_ttl = cache_ttl
        os.makedirs(cache_dir, exist_ok=True)
        if not moralis_key:
            # Not fatal — caller can still hit get_pairs / pricing endpoints.
            # But anything that needs trade history will return empty.
            pass

    # ---- cache helpers -----------------------------------------------------
    def _cache_path(self, key):
        return os.path.join(self.cache_dir, key + '.json')

    def _cache_get(self, key, ttl=None):
        p = self._cache_path(key)
        if ttl is None:
            ttl = self.cache_ttl
        if os.path.exists(p) and (time.time() - os.path.getmtime(p) < ttl):
            try: return json.load(open(p))
            except Exception: pass
        return None

    def _cache_set(self, key, val):
        try: json.dump(val, open(self._cache_path(key), 'w'))
        except Exception: pass

    # ---- moralis http -----------------------------------------------------
    def _moralis(self, chain, path, params=None):
        cfg = CHAINS[chain]
        url = cfg['moralis_base'] + path
        p = dict(params or {})
        if cfg.get('chain_param'):
            p['chain'] = cfg['chain_param']
        headers = {'X-API-Key': self.mk, 'accept': 'application/json'}
        last_err = None
        for attempt in range(3):
            try:
                r = requests.get(url, headers=headers, params=p, timeout=20)
                if r.status_code == 200:
                    try: return r.json()
                    except Exception: return {}
                if r.status_code == 429:
                    time.sleep(0.5 * (attempt + 1))
                    last_err = 'http_429'; continue
                last_err = f'http_{r.status_code}'
                break
            except Exception as e:
                last_err = str(e)[:120]; time.sleep(0.3)
        return {'_error': last_err}

    def _moralis_paginate(self, chain, path, params, max_pages=15):
        """Walks `cursor` pagination, returns flat list of swap rows."""
        out = []
        cursor = None
        for page in range(max_pages):
            p = dict(params)
            p['limit'] = 100
            if cursor: p['cursor'] = cursor
            j = self._moralis(chain, path, p)
            if not isinstance(j, dict) or '_error' in j:
                break
            rows = j.get('result') or []
            out.extend(rows)
            cursor = j.get('cursor')
            if not cursor or not rows:
                break
        return out

    # ---- normalize one moralis swap row ----------------------------------
    def _normalize_swap(self, row, token, chain):
        """Pull out the trade in (kind, eth_amt, ts, wallet, hash) form.

        Returns None if the swap isn't priced in the chain's native token (e.g.
        a USDC-only-leg trade on a stablecoin pool — we currently skip those).
        """
        cfg = CHAINS[chain]
        kind = row.get('transactionType')  # 'buy' | 'sell'
        wallet = (row.get('walletAddress') or '').lower()
        hash_ = row.get('transactionHash')
        ts_str = row.get('blockTimestamp') or ''
        ts = 0
        if ts_str:
            # ISO8601 with Z; cheap parse without datetime
            try:
                from datetime import datetime
                ts = int(datetime.fromisoformat(ts_str.replace('Z', '+00:00')).timestamp())
            except Exception:
                ts = 0

        bought = row.get('bought') or {}
        sold = row.get('sold') or {}
        bsym = (bought.get('symbol') or '').upper()
        ssym = (sold.get('symbol') or '').upper()

        def _f(x):
            try: return abs(float(x))
            except Exception: return 0.0

        token_lc = (token or '').lower()
        b_addr = (bought.get('address') or '').lower()
        s_addr = (sold.get('address') or '').lower()

        eth_amt = 0.0
        if kind == 'buy':
            # wallet paid quote (native), got the token
            if ssym in cfg['native_symbols']:
                eth_amt = _f(sold.get('amount'))
            # If token filter requested, check the bought side matches
            if token_lc and b_addr and b_addr != token_lc:
                return None
        elif kind == 'sell':
            # wallet sold the token, got native
            if bsym in cfg['native_symbols']:
                eth_amt = _f(bought.get('amount'))
            if token_lc and s_addr and s_addr != token_lc:
                return None
        else:
            return None

        if eth_amt <= 0 or not wallet or not kind:
            return None

        return {
            'wallet': wallet,
            'kind': kind,
            'eth': eth_amt,
            'usd': _f(row.get('totalValueUsd')),
            'ts': ts,
            'hash': hash_,
            'pair_addr': (row.get('pairAddress') or '').lower(),
            'pair_label': row.get('pairLabel') or '',
            'exchange': row.get('exchangeName') or '',
        }

    # ---- public: token-level swap aggregation ---------------------------
    def token_swaps(self, token, chain='eth', max_pages=200, ttl=600,
                    since_ts=None, until_ts=None):
        """All swaps on a token across every DEX, normalized.

        Pagination is DESC-by-time (newest first).  We stop as soon as one of:
          - cursor is None (we've seen everything in the window)
          - max_pages reached
          - the oldest row in the latest page is older than `since_ts`

        `since_ts` / `until_ts` are unix-seconds; if given they pass through to
        Moralis as `fromDate` / `toDate` so the server only paginates inside
        the window. Use this for "scan only the last 30 days" type queries
        on very-active tokens where the full history would be huge.

        Returns the normalized list. Caches under a key derived from the
        window so different windows don't clobber each other.
        """
        from datetime import datetime, timezone
        ck = f'tswaps_{chain}_{token.lower()}_s{since_ts or 0}_u{until_ts or 0}_p{max_pages}'
        cached = self._cache_get(ck, ttl=ttl)
        if cached is not None:
            return cached
        cfg = CHAINS[chain]
        path = cfg['token_swaps_path'].replace('{token}', token)

        params = {}
        if since_ts:
            params['fromDate'] = datetime.fromtimestamp(since_ts, tz=timezone.utc).isoformat().replace('+00:00', 'Z')
        if until_ts:
            params['toDate'] = datetime.fromtimestamp(until_ts, tz=timezone.utc).isoformat().replace('+00:00', 'Z')
        params['order'] = 'DESC'

        out = []
        cursor = None
        for page in range(max_pages):
            p = dict(params)
            p['limit'] = 100
            if cursor: p['cursor'] = cursor
            j = self._moralis(chain, path, p)
            if not isinstance(j, dict) or '_error' in j:
                break
            rows = j.get('result') or []
            for r in rows:
                n = self._normalize_swap(r, token, chain)
                if n: out.append(n)
            cursor = j.get('cursor')
            if not cursor or not rows:
                break
        self._cache_set(ck, out)
        return out

    def token_top_gainers(self, token, chain='eth', ttl=600):
        """Returns up to ~97 wallets pre-aggregated by Moralis (sorted by
        realized PnL %).  Each entry has total_usd_invested / total_sold_usd /
        count_of_trades / realized_profit_usd — exactly what /find needs.

        Free bonus discovery seed.  Not paginated by Moralis."""
        ck = f'topgain_{chain}_{token.lower()}'
        cached = self._cache_get(ck, ttl=ttl)
        if cached is not None:
            return cached
        if chain != 'eth':
            self._cache_set(ck, []); return []
        j = self._moralis(chain, f'/erc20/{token}/top-gainers', {'days': 'all'})
        rows = (j.get('result') or []) if isinstance(j, dict) else []
        self._cache_set(ck, rows)
        return rows

    def wallet_swaps(self, wallet, token, chain='eth', max_pages=10, ttl=300):
        """All swaps by `wallet` on `token`, normalized.  Cached 5 min."""
        ck = f'wswaps_{chain}_{wallet.lower()}_{token.lower()}_p{max_pages}'
        cached = self._cache_get(ck, ttl=ttl)
        if cached is not None:
            return cached
        cfg = CHAINS[chain]
        path = cfg['wallet_swaps_path'].replace('{wallet}', wallet)
        params = {}
        if chain == 'eth':
            params['tokenAddress'] = token
        else:
            params['tokenAddress'] = token  # solana endpoint also supports this
        rows = self._moralis_paginate(chain, path, params, max_pages=max_pages)
        norm = []
        for r in rows:
            n = self._normalize_swap(r, token, chain)
            if n: norm.append(n)
        self._cache_set(ck, norm)
        return norm

    # ---- aggregation helpers ---------------------------------------------
    def _empty_stats(self):
        return {
            'eth_in': 0.0, 'eth_out': 0.0,
            'n_buys': 0, 'n_sells': 0,
            'first_block': 0, 'last_block': 0,
            'last_buy_ts': 0, 'last_sell_ts': 0,
            'buy_ts': [], 'sell_ts': [],
        }

    def _add_swap_to_stats(self, stats, sw):
        s = stats[sw['wallet']]
        if sw['kind'] == 'buy':
            s['eth_in'] += sw['eth']
            s['n_buys'] += 1
            s['buy_ts'].append(sw['ts'])
            if sw['ts'] > s['last_buy_ts']:
                s['last_buy_ts'] = sw['ts']
        else:
            s['eth_out'] += sw['eth']
            s['n_sells'] += 1
            s['sell_ts'].append(sw['ts'])
            if sw['ts'] > s['last_sell_ts']:
                s['last_sell_ts'] = sw['ts']

    # ---- pool listing (DexScreener) — used for /find header info -------
    def get_pairs(self, token):
        """Ethereum WETH pairs sorted by liquidity desc.  Used for display only."""
        key = f'pairs_{token.lower()}'
        cached = self._cache_get(key)
        if cached is not None:
            return cached
        try:
            r = requests.get(f'https://api.dexscreener.com/latest/dex/tokens/{token}', timeout=15)
            data = r.json() if r.status_code == 200 else {}
        except Exception:
            data = {}
        pairs = []
        for p in data.get('pairs') or []:
            if p.get('chainId') != 'ethereum':
                continue
            base = (p.get('baseToken') or {}).get('address', '').lower()
            quote = (p.get('quoteToken') or {}).get('address', '').lower()
            if WETH.lower() not in (base, quote):
                continue
            pairs.append({
                'addr': p['pairAddress'].lower(),
                'dex': p.get('dexId', ''),
                'labels': p.get('labels') or [],
                'liquidity_usd': float((p.get('liquidity') or {}).get('usd', 0) or 0),
                'price_usd': float(p.get('priceUsd') or 0),
                'base_is_weth': base == WETH.lower(),
            })
        pairs.sort(key=lambda x: -x['liquidity_usd'])
        self._cache_set(key, pairs)
        return pairs

    # ---- (optional) etherscan helpers — used only for is_contract -------
    def _es(self, params):
        if not self.esk:
            return {'_error': 'no_etherscan_key'}
        params = {**params, 'chainid': 1, 'apikey': self.esk}
        last_err = None
        for attempt in range(2):
            try:
                r = requests.get('https://api.etherscan.io/v2/api', params=params, timeout=15)
                if r.status_code == 200:
                    return r.json()
                last_err = f'http_{r.status_code}'
                if r.status_code == 429:
                    time.sleep(0.3); continue
                break
            except Exception as e:
                last_err = str(e)[:120]; time.sleep(0.3)
        return {'_error': last_err}

    def _is_contract(self, addr):
        if not self.esk:
            return False
        addr = addr.lower()
        key = f'code_{addr}'
        cached = self._cache_get(key, ttl=86400 * 30)
        if cached is not None:
            return cached
        js = self._es({'module': 'proxy', 'action': 'eth_getCode', 'address': addr, 'tag': 'latest'})
        code = js.get('result') if isinstance(js, dict) else ''
        is_c = isinstance(code, str) and code not in ('0x', '', '0x0')
        if isinstance(code, str):
            self._cache_set(key, is_c)
        return is_c

    # ---- main pipeline ----------------------------------------------------
    def build_wallet_stats(self, token, chain='eth', trace_wallet=None,
                           max_pages=200, since_ts=None, until_ts=None):
        """Aggregate per-wallet stats from Moralis token-swaps.
        Returns ({wallet: stats}, pools[, trace]) when trace_wallet given.
        `pools` is filled from DexScreener for display only (we no longer scan them).

        `since_ts` / `until_ts` (unix seconds) bound the scan window.  Default
        is no bound, capped only by max_pages (200 = up to 20k swaps).
        """
        if not self.mk:
            return ({}, [], []) if trace_wallet else ({}, [])

        swaps = self.token_swaps(token, chain=chain, max_pages=max_pages,
                                 since_ts=since_ts, until_ts=until_ts)

        # build pools list from observed swaps (deduped by pair_addr) so we have
        # a reasonable display even if DexScreener hasn't indexed yet
        pool_idx = {}
        for sw in swaps:
            pa = sw.get('pair_addr')
            if not pa: continue
            pool_idx.setdefault(pa, {
                'addr': pa,
                'name': sw.get('pair_label') or '',
                'dex': sw.get('exchange') or '',
                'liquidity_usd': 0.0,
            })

        # enrich with dexscreener liquidity for display (eth only)
        if chain == 'eth':
            ds = self.get_pairs(token)
            ds_by = {p['addr']: p for p in ds}
            for addr, info in pool_idx.items():
                if addr in ds_by:
                    info['liquidity_usd'] = ds_by[addr]['liquidity_usd']

        pools = sorted(pool_idx.values(), key=lambda x: -x['liquidity_usd'])

        stats = defaultdict(self._empty_stats)
        trace = []
        twl = (trace_wallet or '').lower()
        for sw in swaps:
            self._add_swap_to_stats(stats, sw)
            if twl and sw['wallet'] == twl:
                trace.append({**sw, 'source': 'moralis_token'})

        if trace_wallet:
            return dict(stats), pools, trace
        return dict(stats), pools

    def wallet_token_totals(self, token, wallet, chain='eth'):
        """Exact buy/sell totals for (wallet, token).  Returns dict with
        eth_in/eth_out/n_buys/n_sells/buy_ts/sell_ts/trades."""
        swaps = self.wallet_swaps(wallet, token, chain=chain, max_pages=10)
        eth_in = eth_out = 0.0
        n_buys = n_sells = 0
        buy_ts, sell_ts = [], []
        trades = []
        for sw in swaps:
            if sw['kind'] == 'buy':
                eth_in += sw['eth']; n_buys += 1
                buy_ts.append(sw['ts'])
            else:
                eth_out += sw['eth']; n_sells += 1
                sell_ts.append(sw['ts'])
            trades.append({'kind': sw['kind'], 'ts': sw['ts'],
                           'eth': sw['eth'], 'usd': sw.get('usd', 0),
                           'hash': sw['hash']})
        return {
            'eth_in': eth_in, 'eth_out': eth_out,
            'n_buys': n_buys, 'n_sells': n_sells,
            'buy_ts': buy_ts, 'sell_ts': sell_ts,
            'trades': trades,
            'n_swaps': len(swaps),
        }

    def debug_wallet(self, token, wallet, chain='eth'):
        """One-shot diagnostic for a (token, wallet) pair."""
        w = wallet.lower()
        # authoritative wallet-centric query
        try:
            totals = self.wallet_token_totals(token, wallet, chain=chain)
        except Exception as e:
            totals = {'error': str(e)[:200]}

        # for-reference: also compute the wallet's row in the token-scan stats
        try:
            stats, pools, trace = self.build_wallet_stats(token, chain=chain, trace_wallet=wallet)
        except Exception:
            stats, pools, trace = {}, [], []
        s = stats.get(w)

        return {
            'wallet': w,
            'token': token.lower(),
            'chain': chain,
            'native_label': CHAINS[chain]['native_label'],
            'is_contract': self._is_contract(wallet) if chain == 'eth' else False,
            'totals': totals,
            'stats': s,
            'pools_scanned': [p['addr'] for p in pools[:8]],
            'n_pools_total': len(pools),
            'trace': trace,
        }

    def search_by_times(self, token, min_buy_ts, max_buy_ts, min_sell_ts, max_sell_ts,
                        top_n=10, chain='eth'):
        """Wallets with buy in [min_buy, max_buy] AND sell in [min_sell, max_sell]."""
        stats, pairs = self.build_wallet_stats(token, chain=chain)
        require_buy = (min_buy_ts is not None) or (max_buy_ts is not None)
        require_sell = (min_sell_ts is not None) or (max_sell_ts is not None)
        lb, ub = min_buy_ts or 0, max_buy_ts or 10**12
        ls, us = min_sell_ts or 0, max_sell_ts or 10**12
        results = []
        for wallet, s in stats.items():
            buys_in = [t for t in s['buy_ts'] if lb <= t <= ub] if require_buy else s['buy_ts']
            sells_in = [t for t in s['sell_ts'] if ls <= t <= us] if require_sell else s['sell_ts']
            if require_buy and not buys_in: continue
            if require_sell and not sells_in: continue
            if not require_buy and not require_sell: continue
            results.append({
                'wallet': wallet,
                'invested_eth': s['eth_in'],
                'sold_eth': s['eth_out'],
                'pnl_eth': s['eth_out'] - s['eth_in'],
                'n_buys': s['n_buys'],
                'n_sells': s['n_sells'],
                'n_buys_in_window': len(buys_in),
                'n_sells_in_window': len(sells_in),
                'first_buy_in_window': min(buys_in) if buys_in else 0,
                'last_buy_in_window': max(buys_in) if buys_in else 0,
                'first_sell_in_window': min(sells_in) if sells_in else 0,
                'last_sell_in_window': max(sells_in) if sells_in else 0,
            })
        results.sort(key=lambda r: (-r['pnl_eth'], -r['invested_eth']))
        return results[:top_n], pairs, len(stats)

    def find_matches(self, token, invested_eth, sold_eth, top_n=5,
                     min_activity=True, tol=0.05, verify_top_k=50,
                     verify_budget_seconds=75, chain='eth',
                     since_ts=None, until_ts=None, scan_max_pages=200):
        """Return wallets within ±tol of both (invested, sold) targets.

        Pipeline:
          1. Token-scan via Moralis -> per-wallet aggregate stats.
             `since_ts`/`until_ts` (unix s) bound the scan window.
          2. Loose prefilter: must have activity AND be within 5x of target.
          3. Always include wallets already matching ±tol on aggregate stats.
          4. Top up to verify_top_k by closeness.
          5. Per-wallet exact lifetime totals via Moralis wallet-scan, in parallel.
          6. Filter survivors to ±tol on the exact numbers.
        """
        eps = 1e-6
        stats, pairs = self.build_wallet_stats(
            token, chain=chain,
            since_ts=since_ts, until_ts=until_ts, max_pages=scan_max_pages,
        )
        inv_lo = invested_eth * (1 - tol)
        inv_hi = invested_eth * (1 + tol)
        sold_lo = sold_eth * (1 - tol)
        sold_hi = sold_eth * (1 + tol)

        # Free aggregated seed: Moralis' top-gainers leaderboard.  These wallets
        # weren't necessarily caught by the recent-swaps scan, so always include
        # them as candidates if their pre-aggregated USD totals are roughly in
        # range.  Verification step will compute exact ETH totals.
        eth_price_proxy = None  # lazy
        seeded_via_topgainers = set()
        if chain == 'eth':
            try:
                tg = self.token_top_gainers(token, chain=chain)
            except Exception:
                tg = []
            for r in tg:
                addr = (r.get('address') or '').lower()
                if not addr:
                    continue
                # Convert pre-aggregated USD to rough ETH using live price
                try:
                    inv_usd = float(r.get('total_usd_invested') or 0)
                    sold_usd = float(r.get('total_sold_usd') or 0)
                except Exception:
                    continue
                # Lazily fetch ETH price to convert
                if eth_price_proxy is None:
                    try:
                        eth_price_proxy = get_eth_price_usd()
                    except Exception:
                        eth_price_proxy = 3500.0
                rough_in = inv_usd / max(eth_price_proxy, 1.0)
                rough_out = sold_usd / max(eth_price_proxy, 1.0)
                # 10x loose seed window — verify will tighten
                if rough_in > max(invested_eth * 10, 10) + 1:
                    continue
                if sold_eth > 0 and rough_out > max(sold_eth * 10, 10) + 1:
                    continue
                if rough_in <= 0 and rough_out <= 0:
                    continue
                # If wallet not already in stats, inject a thin entry so prefilter sees it
                if addr not in stats:
                    stats[addr] = {
                        'eth_in': rough_in, 'eth_out': rough_out,
                        'n_buys': r.get('count_of_trades') or 0,
                        'n_sells': r.get('count_of_trades') or 0,
                        'first_block': 0, 'last_block': 0,
                        'last_buy_ts': 0, 'last_sell_ts': 0,
                        'buy_ts': [], 'sell_ts': [],
                    }
                seeded_via_topgainers.add(addr)

        # Asymmetric scoring: when one leg is 0 in the scan window (buy or sell
        # happened outside the window), score only the visible leg + a small
        # constant penalty.  This is critical so long-holders (eth_in=0,
        # eth_out~target) and recent-buyers (eth_in~target, eth_out=0) surface
        # near the top instead of being ranked last by log(eps).
        SINGLE_LEG_PENALTY = 1.0  # ≈ a 2.7× miss on the unseen leg
        prefilter = []
        for wallet, s in stats.items():
            if s['n_buys'] + s['n_sells'] == 0:
                continue
            if s['eth_in'] > max(invested_eth * 10, 10) + 1:
                continue
            if sold_eth > 0 and s['eth_out'] > max(sold_eth * 10, 10) + 1:
                continue
            has_in = s['eth_in'] > 0
            has_out = s['eth_out'] > 0
            score_in = abs(math.log((s['eth_in'] + eps) / (invested_eth + eps))) if has_in else None
            score_out = (abs(math.log((s['eth_out'] + eps) / (sold_eth + eps)))
                         if (sold_eth > 0 and has_out) else None)
            if score_in is not None and score_out is not None:
                rel = score_in + score_out
            elif score_in is not None:
                rel = score_in + SINGLE_LEG_PENALTY
            elif score_out is not None:
                rel = score_out + SINGLE_LEG_PENALTY
            else:
                continue  # neither leg gives signal
            prefilter.append((rel, wallet, s))
        prefilter.sort(key=lambda x: x[0])

        filt = {
            'total': len(stats),
            'prefilter': len(prefilter),
            'verified': 0,
            'inv_ok': 0,
            'sell_ok': 0,
            'top_gainers_seeded': len(seeded_via_topgainers),
            'single_leg': 0,
        }

        # Always-verify wallets already matching ±tol on aggregate stats.
        must_verify = set()
        for _, wallet, s in prefilter:
            if inv_lo <= s['eth_in'] <= inv_hi:
                if sold_eth == 0 or (s['eth_out'] > 0 and sold_lo <= s['eth_out'] <= sold_hi):
                    must_verify.add(wallet)

        # Single-leg-close set: wallets where ONE leg lands within ±20% of
        # target.  These are very likely candidates whose other leg is just
        # outside the scan window — exactly the case where verifying via
        # wallet-centric lifetime totals is needed.
        single_leg_set = set()
        wide_lo_inv, wide_hi_inv = invested_eth * 0.8, invested_eth * 1.2
        wide_lo_out = sold_eth * 0.8 if sold_eth > 0 else 0
        wide_hi_out = sold_eth * 1.2 if sold_eth > 0 else 0
        for _, wallet, s in prefilter:
            if wallet in must_verify:
                continue
            inv_close = wide_lo_inv <= s['eth_in'] <= wide_hi_inv
            sold_close = sold_eth > 0 and wide_lo_out <= s['eth_out'] <= wide_hi_out
            if inv_close or sold_close:
                single_leg_set.add(wallet)
        filt['single_leg'] = len(single_leg_set)

        # Auto-verify all top-gainers seeded wallets — leaderboard-rank already
        # implies they're high-PnL, finite cost (≤100 wallets).
        verify_set = list(must_verify)
        for w in seeded_via_topgainers:
            if w not in must_verify and w not in verify_set:
                verify_set.append(w)
        for w in single_leg_set:
            if w not in must_verify and w not in verify_set:
                verify_set.append(w)
        for _, wallet, _ in prefilter:
            if wallet in must_verify or wallet in seeded_via_topgainers or wallet in single_leg_set:
                continue
            if len(verify_set) >= verify_top_k:
                break
            verify_set.append(wallet)

        results = []
        deadline = time.time() + verify_budget_seconds

        def _verify(w):
            if time.time() > deadline:
                return None
            try:
                return w, self.wallet_token_totals(token, w, chain=chain)
            except Exception:
                return None

        with ThreadPoolExecutor(max_workers=5) as ex:
            futs = {ex.submit(_verify, w): w for w in verify_set}
            for fut in as_completed(futs):
                if time.time() > deadline:
                    break
                out = fut.result()
                if not out:
                    continue
                wallet, tot = out
                filt['verified'] += 1
                ei = tot['eth_in']; eo = tot['eth_out']
                if not (inv_lo <= ei <= inv_hi):
                    continue
                filt['inv_ok'] += 1
                if sold_eth > 0:
                    if tot['n_sells'] == 0 or eo == 0:
                        continue
                    if not (sold_lo <= eo <= sold_hi):
                        continue
                    rel_sold = abs(math.log((eo + eps) / (sold_eth + eps)))
                else:
                    rel_sold = 0
                filt['sell_ok'] += 1
                rel_inv = abs(math.log((ei + eps) / (invested_eth + eps)))
                results.append({
                    'wallet': wallet,
                    'invested_eth': ei,
                    'sold_eth': eo,
                    'pnl_eth': eo - ei,
                    'n_buys': tot['n_buys'],
                    'n_sells': tot['n_sells'],
                    'first_block': 0, 'last_block': 0,
                    'last_buy_ts': max(tot['buy_ts']) if tot['buy_ts'] else 0,
                    'last_sell_ts': max(tot['sell_ts']) if tot['sell_ts'] else 0,
                    'dist': rel_inv + rel_sold,
                    'bucket': 'seller' if tot['n_sells'] > 0 else 'holder',
                })
        results.sort(key=lambda r: r['dist'])
        return results[:top_n], pairs, filt


# ---- ETH price helper (used by bot.py for $ <-> ETH conversion) -----------
def get_eth_price_usd(cache_ttl=300, _cache=[0, 0]):
    """Cheap live ETH/USD via DexScreener WETH page."""
    now = time.time()
    if _cache[1] and (now - _cache[1] < cache_ttl):
        return _cache[0]
    try:
        r = requests.get('https://api.dexscreener.com/latest/dex/tokens/' + WETH, timeout=10)
        data = r.json()
        prices = []
        for p in (data.get('pairs') or []):
            if p.get('chainId') != 'ethereum':
                continue
            pu = p.get('priceUsd')
            if pu:
                try:
                    pu = float(pu)
                    if 100 < pu < 20000:
                        prices.append(pu)
                except Exception:
                    pass
        if prices:
            prices.sort()
            mid = prices[len(prices) // 2]
            _cache[0] = mid
            _cache[1] = now
            return mid
    except Exception:
        pass
    return 3500.0  # safe fallback
