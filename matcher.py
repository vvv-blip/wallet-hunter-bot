"""Wallet match logic — free-API stack (Etherscan v2 + GeckoTerminal).

Pivoted off Moralis after credit burn on hyper-active tokens.  The default
trade-history backend is now:
  - Token-wide discovery       -> GeckoTerminal token_trades (free, no key)
                                   + optional Etherscan tokentx for deep history
  - Per-wallet exact totals    -> EtherscanSource.wallet_token_totals
                                   (tokentx + WETH-leg + internal-tx reconstruction)

Moralis is now OPTIONAL.  If MORALIS_API_KEY is set, it's only used as a
fast-path for `wallet_token_totals` on a per-call basis (no token-wide
scans → that's where credits got burned).  When the key is absent, the bot
runs entirely on free APIs.

The chain layer is still a thin dict so Solana support is a small change.
"""
import os, time, json, math, requests
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# Optional sources layer — gracefully degrades if files missing.
try:
    from sources import Cache, GTSource, EtherscanSource
    _SOURCES_AVAILABLE = True
except Exception:
    _SOURCES_AVAILABLE = False

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

        # New sources layer — instantiated once, shared across all calls.
        # Falls back to legacy code paths if the import failed.
        if _SOURCES_AVAILABLE:
            self.cache = Cache(cache_dir, default_ttl=cache_ttl)
            self.gt = GTSource(self.cache)
            self.es = EtherscanSource(etherscan_key, self.cache) if etherscan_key else None
        else:
            self.cache = None
            self.gt = None
            self.es = None

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

    # ---- GeckoTerminal: free, no-key trader discovery -------------------
    # GT's public API exposes per-pool trade rows that include `tx_from_address`
    # (the actual EOA, not the router contract).  300 trades per pool, free,
    # up to ~30 r/s.  We pull from the top N pools to widen coverage on tokens
    # that get split across many pairs, then dedupe EOAs.  The freshness window
    # depends on token activity — on busy memecoins, 300 trades may only reach
    # back hours; on quiet tokens, weeks.  Best used as a complement to the
    # Moralis token-scan, not a replacement.
    def _gt(self, path, params=None, timeout=10):
        url = 'https://api.geckoterminal.com/api/v2' + path
        try:
            r = requests.get(url, params=(params or {}),
                             headers={'accept': 'application/json'},
                             timeout=timeout)
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
        return {}

    def gt_token_pools(self, token, chain='eth', n_pools=15, ttl=900):
        """Top N pools for a token, ranked by GT (volume/liquidity heuristic)."""
        ck = f'gtpools_{chain}_{token.lower()}_n{n_pools}'
        cached = self._cache_get(ck, ttl=ttl)
        if cached is not None:
            return cached
        net = 'eth' if chain == 'eth' else chain
        # /networks/{net}/tokens/{token}/pools paginates 20/page
        pools = []
        page = 1
        while len(pools) < n_pools and page <= 5:
            j = self._gt(f'/networks/{net}/tokens/{token}/pools',
                         params={'page': page})
            rows = (j.get('data') or []) if isinstance(j, dict) else []
            if not rows:
                break
            for row in rows:
                addr = ((row.get('attributes') or {}).get('address') or '').lower()
                if addr:
                    pools.append(addr)
                if len(pools) >= n_pools:
                    break
            page += 1
        self._cache_set(ck, pools)
        return pools

    def gt_pool_trader_eoas(self, pool_addr, chain='eth', ttl=300):
        """Up to ~300 most-recent trader EOAs on one pool (free, no key)."""
        ck = f'gttraders_{chain}_{pool_addr.lower()}'
        cached = self._cache_get(ck, ttl=ttl)
        if cached is not None:
            return cached
        net = 'eth' if chain == 'eth' else chain
        j = self._gt(f'/networks/{net}/pools/{pool_addr}/trades')
        rows = (j.get('data') or []) if isinstance(j, dict) else []
        eoas = []
        seen = set()
        for row in rows:
            attrs = row.get('attributes') or {}
            eoa = (attrs.get('tx_from_address') or '').lower()
            if eoa and eoa not in seen:
                seen.add(eoa)
                eoas.append(eoa)
        self._cache_set(ck, eoas)
        return eoas

    def gt_token_traders(self, token, chain='eth', n_pools=15, max_traders=1500):
        """Aggregate unique trader EOAs across the top N pools.  Free, ~3-6s
        for n_pools=15.  Returns a deduped list ordered by first-seen
        (top-pool first → newer trades first within pool)."""
        if chain != 'eth':
            return []
        try:
            pools = self.gt_token_pools(token, chain=chain, n_pools=n_pools)
        except Exception:
            pools = []
        seen = set()
        out = []
        # Pull pools in parallel — each is one HTTP call.
        with ThreadPoolExecutor(max_workers=min(8, max(1, len(pools)))) as ex:
            futs = {ex.submit(self.gt_pool_trader_eoas, p, chain): p
                    for p in pools}
            # Order results by pool order (not completion order) so the top
            # pool's traders come first.
            results_by_pool = {}
            for fut in as_completed(futs):
                pool = futs[fut]
                try:
                    results_by_pool[pool] = fut.result()
                except Exception:
                    results_by_pool[pool] = []
        for pool in pools:
            for eoa in results_by_pool.get(pool, []):
                if eoa not in seen:
                    seen.add(eoa)
                    out.append(eoa)
                if len(out) >= max_traders:
                    return out
        return out

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
        """Aggregate per-wallet stats from token-wide trade flow.

        DEFAULT: GT trades (free, ~5K most-recent across top 15 pools).
        If MORALIS_API_KEY is set AND the user opts in via env
        WALLET_BOT_USE_MORALIS=1, falls through to the legacy Moralis path
        (deeper history but burns paid credits — disabled by default).

        Returns ({wallet: stats}, pools[, trace]) when trace_wallet given.
        """
        twl = (trace_wallet or '').lower()
        use_moralis = (self.mk and os.environ.get('WALLET_BOT_USE_MORALIS') == '1'
                       and chain == 'eth')

        # Legacy Moralis path — only when explicitly opted in.
        if use_moralis:
            swaps = self.token_swaps(token, chain=chain, max_pages=max_pages,
                                     since_ts=since_ts, until_ts=until_ts)
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
            if chain == 'eth':
                ds = self.get_pairs(token)
                ds_by = {p['addr']: p for p in ds}
                for addr, info in pool_idx.items():
                    if addr in ds_by:
                        info['liquidity_usd'] = ds_by[addr]['liquidity_usd']
            pools = sorted(pool_idx.values(), key=lambda x: -x['liquidity_usd'])
            stats = defaultdict(self._empty_stats)
            trace = []
            for sw in swaps:
                self._add_swap_to_stats(stats, sw)
                if twl and sw['wallet'] == twl:
                    trace.append({**sw, 'source': 'moralis_token'})
            if trace_wallet:
                return dict(stats), pools, trace
            return dict(stats), pools

        # Default: GT-only token-wide scan (free, no key, parallel pools).
        if not self.gt or chain != 'eth':
            return ({}, [], []) if trace_wallet else ({}, [])
        gt_trades = self.gt.token_trades(token, network='eth', n_pools=15)
        # Optionally bound by since_ts (until_ts) — GT data is freshness-biased
        if since_ts:
            gt_trades = [t for t in gt_trades if (t.get('ts') or 0) >= since_ts]
        if until_ts:
            gt_trades = [t for t in gt_trades if (t.get('ts') or 0) <= until_ts]

        # Build pools list (display only) from DexScreener for liquidity info.
        ds = self.get_pairs(token) if chain == 'eth' else []
        pools = ds[:20]

        stats = defaultdict(self._empty_stats)
        trace = []
        for tr in gt_trades:
            sw = {
                'wallet': tr['wallet'],
                'kind': tr['kind'],
                'eth': tr['eth'],
                'usd': tr.get('usd', 0),
                'ts': tr['ts'],
                'hash': tr.get('tx_hash', ''),
            }
            self._add_swap_to_stats(stats, sw)
            if twl and sw['wallet'] == twl:
                trace.append({**sw, 'source': 'gt_token'})

        if trace_wallet:
            return dict(stats), pools, trace
        return dict(stats), pools

    def wallet_token_totals(self, token, wallet, chain='eth'):
        """Exact buy/sell totals for (wallet, token).

        Backend selection:
          1. Etherscan (free, default) — full historical via tokentx + WETH-leg
             reconstruction.  Authoritative, ~600ms/wallet rate-limited.
          2. Moralis (only if WALLET_BOT_USE_MORALIS=1 + key present).
          3. Empty stub if neither is available.
        """
        use_moralis = (self.mk and os.environ.get('WALLET_BOT_USE_MORALIS') == '1'
                       and chain == 'eth')
        if use_moralis:
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
                'source': 'moralis',
            }

        # Default: Etherscan reconstruction (free, full history).
        if self.es and chain == 'eth':
            try:
                t = self.es.wallet_token_totals(wallet, token)
                t['source'] = 'etherscan'
                return t
            except Exception:
                pass

        # Last resort: empty stub
        return {
            'eth_in': 0.0, 'eth_out': 0.0,
            'n_buys': 0, 'n_sells': 0,
            'buy_ts': [], 'sell_ts': [], 'trades': [],
            'n_swaps': 0,
            'source': 'unavailable',
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
                     min_activity=True, tol=0.05, verify_top_k=30,
                     verify_budget_seconds=150, chain='eth',
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
        # Skipped unless explicitly opted in (burns Moralis credits).
        eth_price_proxy = None  # lazy
        seeded_via_topgainers = set()
        use_moralis = (self.mk and os.environ.get('WALLET_BOT_USE_MORALIS') == '1'
                       and chain == 'eth')
        if use_moralis:
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
            'top_long_holders': 0,
            'top_recent_buyers': 0,
            'top_active': 0,
            'gt_traders': 0,
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

        # ── Top-traders-first buckets (key for hyper-active tokens where the
        #    target wallet's full activity isn't in the scan window) ──────────
        #
        # Three parallel views of "high-probability" candidates.  We rank each
        # by activity in the scan window and verify the top N of each:
        #
        #   • top_long_holders   — bought BEFORE scan window (eth_in=0),
        #                          dumping now.  Sorted by n_sells DESC then
        #                          eth_out DESC.  Captures pump-and-dump
        #                          winners whose buy is older than scan reach.
        #   • top_recent_buyers  — bought during scan, hasn't sold yet
        #                          (eth_out=0).  Sorted by n_buys DESC then
        #                          eth_in DESC.  Captures accumulators.
        #   • top_active         — high turnover (n_buys + n_sells), regardless
        #                          of eth_in/eth_out=0.  Captures swing traders
        #                          and big partial-window participants.
        #
        # Caps sized for thoroughness on hyper-active tokens — we accept a
        # bigger time budget so the candidate pool is deep enough that
        # partial-window wallets (like long-holders dumping after weeks) still
        # surface.  Combined unique pool ≤ ~700 wallets; with 10 parallel
        # workers fits in ~150s.
        TOP_LONG_HOLDERS_CAP  = 300
        TOP_RECENT_BUYERS_CAP = 200
        TOP_ACTIVE_CAP        = 300

        long_holder_pool = [(w, s) for w, s in stats.items()
                            if s['eth_in'] == 0 and s['eth_out'] > 0
                            and s['n_sells'] > 0]
        long_holder_pool.sort(key=lambda kv: (-kv[1]['n_sells'], -kv[1]['eth_out']))
        top_long_holders = {w for w, _ in long_holder_pool[:TOP_LONG_HOLDERS_CAP]}

        recent_buyer_pool = [(w, s) for w, s in stats.items()
                             if s['eth_out'] == 0 and s['eth_in'] > 0
                             and s['n_buys'] > 0]
        recent_buyer_pool.sort(key=lambda kv: (-kv[1]['n_buys'], -kv[1]['eth_in']))
        top_recent_buyers = {w for w, _ in recent_buyer_pool[:TOP_RECENT_BUYERS_CAP]}

        active_pool = [(w, s) for w, s in stats.items()
                       if (s['n_buys'] + s['n_sells']) > 0]
        active_pool.sort(key=lambda kv: (-(kv[1]['n_buys'] + kv[1]['n_sells']),
                                         -(kv[1]['eth_in'] + kv[1]['eth_out'])))
        top_active = {w for w, _ in active_pool[:TOP_ACTIVE_CAP]}

        filt['top_long_holders'] = len(top_long_holders)
        filt['top_recent_buyers'] = len(top_recent_buyers)
        filt['top_active'] = len(top_active)

        # ── GeckoTerminal supplemental bucket (free, no key) ────────────────
        # GT exposes `tx_from_address` on per-pool trade rows — the actual EOA
        # (not the router).  Pulling ~300 trades from each of the top 15 pools
        # gives ~1.5k unique recent trader EOAs in 3-6s, no API key cost.
        # Complements Moralis: GT is freshness-biased (newest trades first per
        # pool) so it catches whales the Moralis token-scan window may have
        # rolled past.  These wallets are added with thin entries so they go
        # through the same wallet-centric verify step downstream.
        GT_BUCKET_CAP = 300
        gt_added = set()
        if chain == 'eth':
            try:
                gt_eoas = self.gt_token_traders(token, chain=chain, n_pools=15,
                                                max_traders=GT_BUCKET_CAP)
            except Exception:
                gt_eoas = []
            for eoa in gt_eoas[:GT_BUCKET_CAP]:
                gt_added.add(eoa)
                if eoa not in stats:
                    # Thin entry — verify step computes exact totals.
                    stats[eoa] = {
                        'eth_in': 0.0, 'eth_out': 0.0,
                        'n_buys': 0, 'n_sells': 0,
                        'first_block': 0, 'last_block': 0,
                        'last_buy_ts': 0, 'last_sell_ts': 0,
                        'buy_ts': [], 'sell_ts': [],
                    }
        filt['gt_traders'] = len(gt_added)

        # Build verify_set in priority order, deduped.  Order matters because
        # we cut at the budget deadline — earlier slots are guaranteed to run.
        verify_set = list(must_verify)
        seen = set(must_verify)

        def _add_bucket(bucket, label):
            added = 0
            for w in bucket:
                if w in seen:
                    continue
                seen.add(w)
                verify_set.append(w)
                added += 1
            return added

        _add_bucket(seeded_via_topgainers, 'top_gainers')   # ≤100
        _add_bucket(top_long_holders,      'long_holders')  # ≤300
        _add_bucket(top_recent_buyers,     'recent_buyers') # ≤200
        _add_bucket(top_active,            'active')        # ≤300
        _add_bucket(gt_added,              'gt_traders')    # ≤300
        _add_bucket(single_leg_set,        'single_leg')

        # Fill remaining slots from regular prefilter ranking up to verify_top_k
        # extra (i.e., normal prefilter top adds on top of priority buckets).
        added_from_rank = 0
        for _, wallet, _ in prefilter:
            if wallet in seen:
                continue
            if added_from_rank >= verify_top_k:
                break
            seen.add(wallet)
            verify_set.append(wallet)
            added_from_rank += 1

        results = []
        deadline = time.time() + verify_budget_seconds

        def _verify(w):
            if time.time() > deadline:
                return None
            try:
                return w, self.wallet_token_totals(token, w, chain=chain)
            except Exception:
                return None

        with ThreadPoolExecutor(max_workers=8) as ex:
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
