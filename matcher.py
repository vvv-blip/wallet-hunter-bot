"""Core matching logic: given a token + target (invested, sold) in ETH,
find wallets on the token's main pair with the closest bought/sold footprint.

Method:
  1) DexScreener -> list the token's Uniswap V2/V3 pairs (WETH side only).
  2) Etherscan tokentx filtered to (token, pair) -> every buy/sell of the token in/out of the pair.
  3) Etherscan tokentx filtered to (WETH, pair) -> every ETH in/out of the same pair.
  4) Join by tx_hash:
       - token OUT of pair + WETH IN to pair  =>  BUY by `to` of the token transfer
       - token IN to pair  + WETH OUT of pair =>  SELL by `from` of the token transfer
  5) Aggregate per wallet, rank by relative distance to the user's (invested, sold) target.
"""
import os, time, json, math, requests
from collections import defaultdict
from datetime import datetime

WETH = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'

# DEX routers + aggregators — origin-wallet addresses we DO NOT treat as traders
ROUTERS = {a.lower() for a in [
    '0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D',  # Uniswap V2
    '0xE592427A0AEce92De3Edee1F18E0157C05861564',  # Uniswap V3
    '0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45',  # Uniswap V3 swap router 02
    '0x3fC91A3afd70395Cd496C647d5a6CC9D4B2b7FAD',  # Universal V1
    '0x66a9893cC07D91D95644AEDD05D03f95e1dBA8Af',  # Universal V2
    '0xEf1c6E67703c7BD7107eed8303Fbe6EC2554BF6B',  # Universal V3
    '0x1111111254EEB25477B68fb85Ed929f73A960582',  # 1inch v5
    '0x111111125421cA6dc452d289314280a0f8842A65',  # 1inch v6
    '0xDef1C0ded9bec7F1a1670819833240f027b25EfF',  # 0x
    '0x9008D19f58AAbD9eD0D60971565AA8510560ab41',  # CoW
    '0xb2ecfE4E4D61f8790bbb9DE2D1259B9e2410CEA5',  # ParaSwap
    '0x1231deb6f5749ef6ce6943a275a1d3e7486f4eae',  # LI.FI Diamond
    '0x881D40237659C251811CEC9c364ef91dC08D300C',  # Metamask Swap
    '0x74de5d4fcbf63e00296fd95d33236b9794016631',  # Banana Gun router
    '0x1a0a18ac4becddbd6389559687d1a73d8927e416',  # Maestro router
    '0xd1742b3c4fbb096990c8950fa635aec75b30781a',  # Maestro
    '0x80a64c6d7f12c47b7c66c5b4e20e72bc1fcd5d9e',  # Maestro v2
    '0x00000000009726632680fb29d3f7a9734e3010e2',  # Rainbow router
    '0x881d4032abe4188e2237efcd27ab435e81fc6bb1',  # Metamask Swaps (alt)
    '0xa69babef1ca67a37ffaf7a485dfff3382056e78c',  # Matcha fee wrapper
]}


class Matcher:
    def __init__(self, etherscan_key, cache_dir='/tmp/wallet_bot_cache', cache_ttl=1800):
        assert etherscan_key, "Etherscan key required"
        self.esk = etherscan_key
        self.cache_dir = cache_dir
        self.cache_ttl = cache_ttl
        os.makedirs(cache_dir, exist_ok=True)

    # ---- cache helpers ----
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

    # ---- api helpers ----
    def _es(self, params):
        params = {**params, 'chainid': 1, 'apikey': self.esk}
        last_err = None
        for _ in range(3):
            try:
                r = requests.get('https://api.etherscan.io/v2/api', params=params, timeout=25)
                if r.status_code == 200:
                    return r.json()
                last_err = f'http_{r.status_code}'
            except Exception as e:
                last_err = str(e)
                time.sleep(1)
        return {'_error': last_err}

    def get_pairs(self, token):
        """Return list of Ethereum WETH pairs for the token, sorted by liquidity desc."""
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

    def _is_contract(self, addr):
        """True if addr has deployed code on-chain. Cached ~forever (code rarely changes)."""
        addr = addr.lower()
        key = f'code_{addr}'
        cached = self._cache_get(key, ttl=86400 * 30)
        if cached is not None:
            return cached
        js = self._es({'module': 'proxy', 'action': 'eth_getCode', 'address': addr, 'tag': 'latest'})
        code = js.get('result') if isinstance(js, dict) else ''
        is_c = isinstance(code, str) and code not in ('0x', '', '0x0')
        self._cache_set(key, is_c)
        return is_c

    def _get_tx_origin(self, tx_hash):
        """EOA signer (tx.from) for a tx hash. Cached forever."""
        key = f'origin_{tx_hash.lower()}'
        cached = self._cache_get(key, ttl=86400 * 365)
        if cached is not None:
            return cached
        js = self._es({'module': 'proxy', 'action': 'eth_getTransactionByHash', 'txhash': tx_hash})
        r = js.get('result') if isinstance(js, dict) else None
        origin = ''
        if isinstance(r, dict):
            origin = (r.get('from') or '').lower()
        if origin:
            self._cache_set(key, origin)
        return origin

    def _tokentx_pair(self, contract, pair):
        """Fetch all token transfers of `contract` where `pair` is sender or recipient.
        Up to ~50k transfers (5 pages × 10k)."""
        key = f'tkn_{contract.lower()}_{pair.lower()}'
        cached = self._cache_get(key)
        if cached is not None:
            return cached
        all_tx = []
        for page in range(1, 6):
            js = self._es({
                'module': 'account', 'action': 'tokentx',
                'contractaddress': contract, 'address': pair,
                'page': page, 'offset': 10000, 'sort': 'desc',
            })
            res = js.get('result', [])
            if not isinstance(res, list) or not res:
                break
            all_tx.extend(res)
            if len(res) < 10000:
                break
            time.sleep(0.22)
        self._cache_set(key, all_tx)
        return all_tx

    # ---- GeckoTerminal data source: each trade already carries tx_from_address (the real EOA),
    #      so router resolution isn't needed. Covers ~600 most-recent trades per pool.
    def _gecko(self, path, params=None):
        try:
            r = requests.get(f'https://api.geckoterminal.com/api/v2/{path}', params=params or {}, timeout=20)
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
        return {}

    def _gecko_pools(self, token):
        """Top WETH-paired pools on Ethereum for the token, sorted by reserve desc."""
        key = f'gpools_{token.lower()}'
        cached = self._cache_get(key)
        if cached is not None:
            return cached
        d = self._gecko(f'networks/eth/tokens/{token}/pools')
        pools = []
        for p in d.get('data') or []:
            a = p.get('attributes') or {}
            rel = (p.get('relationships') or {}).get('base_token', {}).get('data') or {}
            try:
                reserve = float(a.get('reserve_in_usd') or 0)
            except Exception:
                reserve = 0
            pools.append({
                'addr': (a.get('address') or '').lower(),
                'name': a.get('name') or '',
                'liquidity_usd': reserve,
                'dex': (p.get('relationships', {}).get('dex', {}).get('data') or {}).get('id', ''),
            })
        # keep only WETH-paired (or WETH/-like named) pools
        pools = [p for p in pools if 'WETH' in p['name'].upper() or 'ETH' in p['name'].upper()]
        pools.sort(key=lambda x: -x['liquidity_usd'])
        self._cache_set(key, pools)
        return pools

    def _gecko_trades(self, pool, max_pages=2):
        """Up to 600 most-recent trades for a pool (Gecko caps at ~300/page, 2 pages)."""
        key = f'gtrades_{pool.lower()}'
        cached = self._cache_get(key)
        if cached is not None:
            return cached
        all_trades = []
        for page in range(1, max_pages + 1):
            d = self._gecko(f'networks/eth/pools/{pool}/trades', {'page': page})
            arr = d.get('data') or []
            if not arr:
                break
            all_trades.extend(arr)
            if len(arr) < 300:
                break
            time.sleep(0.3)  # gentle on the free tier
        self._cache_set(key, all_trades)
        return all_trades

    def _add_trade(self, stats, wallet, is_buy, eth_amt, blk, ts):
        s = stats[wallet]
        if is_buy:
            s['eth_in'] += eth_amt
            s['n_buys'] += 1
            s['buy_ts'].append(ts)
            if ts > s['last_buy_ts']:
                s['last_buy_ts'] = ts
        else:
            s['eth_out'] += eth_amt
            s['n_sells'] += 1
            s['sell_ts'].append(ts)
            if ts > s['last_sell_ts']:
                s['last_sell_ts'] = ts
        if s['first_block'] == 0 or (blk and blk < s['first_block']):
            s['first_block'] = blk
        if blk > s['last_block']:
            s['last_block'] = blk

    def _etherscan_extend(self, token, pools, stats, ts_cutoff_per_pool,
                          origin_budget_seconds=60, origin_max_lookups=800,
                          router_threshold=8, trace_wallet=None):
        """Augment `stats` with older trades from Etherscan (ts < cutoff per pool).

        Two-pass to keep API cost bounded:
          Pass 1: parse all tokentx rows into `events`. Count appearances per wallet.
          Pass 2: for wallets appearing >= router_threshold times OR in ROUTERS,
                  _is_contract-check them (bounded ~20 calls per token). Contracts
                  are flagged as routers. Then replay events, resolving router events
                  via tx.from with a time/call budget.

        Real traders almost never appear 8+ times on a single pool's tokentx; routers
        appear hundreds of times. This focuses expensive lookups where they matter.
        """
        deadline = time.time() + origin_budget_seconds
        lookups = 0
        trace = []

        for pool_addr in pools:
            cutoff = ts_cutoff_per_pool.get(pool_addr, 10**12)
            tok_txs = self._tokentx_pair(token, pool_addr)
            if not tok_txs:
                continue
            weth_txs = self._tokentx_pair(WETH, pool_addr)
            weth_by_hash = defaultdict(list)
            for w in weth_txs:
                try:
                    weth_by_hash[w['hash']].append({
                        'to': w['to'].lower(), 'from': w['from'].lower(),
                        'value': int(w['value']) / 1e18,
                    })
                except Exception:
                    continue

            # ---- Pass 1: parse events + count per-wallet appearances ----
            events = []  # (wallet, is_buy, eth_amt, blk, ts, h)
            appearance = defaultdict(int)
            for t in tok_txs:
                try:
                    to_a = t['to'].lower()
                    from_a = t['from'].lower()
                    h = t['hash']
                    blk = int(t['blockNumber'])
                    ts = int(t.get('timeStamp', 0) or 0)
                except Exception:
                    continue
                if ts >= cutoff:
                    continue

                is_buy = (from_a == pool_addr and to_a != pool_addr)
                is_sell = (to_a == pool_addr and from_a != pool_addr)
                if not (is_buy or is_sell):
                    continue
                wallet = to_a if is_buy else from_a
                if wallet == pool_addr:
                    continue

                eth_amt = 0.0
                for w in weth_by_hash.get(h, []):
                    if is_buy and w['to'] == pool_addr:
                        eth_amt += w['value']
                    elif is_sell and w['from'] == pool_addr:
                        eth_amt += w['value']
                if eth_amt <= 0:
                    continue

                events.append((wallet, is_buy, eth_amt, blk, ts, h))
                appearance[wallet] += 1

            # ---- Pass 2: flag suspicious wallets (heavy hitters) as routers ----
            # _is_contract is perma-cached; only called for wallets with many trades.
            is_router = {a: True for a in ROUTERS if a in appearance}
            suspicious = [w for w, n in appearance.items()
                          if n >= router_threshold and w not in is_router]
            # Hard cap so we never spam the RPC
            for w in suspicious[:30]:
                if time.time() >= deadline:
                    break
                try:
                    if self._is_contract(w):
                        is_router[w] = True
                except Exception:
                    continue

            # ---- Pass 3: commit events, resolving router events to EOA ----
            for wallet, is_buy, eth_amt, blk, ts, h in events:
                orig_wallet = wallet
                if wallet in is_router:
                    cached = self._cache_get(f'origin_{h}', ttl=86400 * 365)
                    if cached:
                        wallet = cached
                    elif lookups < origin_max_lookups and time.time() < deadline:
                        origin = self._get_tx_origin(h)
                        lookups += 1
                        if origin and origin not in ROUTERS and origin not in is_router:
                            wallet = origin
                        elif origin and origin not in ROUTERS:
                            wallet = origin  # best effort
                        else:
                            continue
                    else:
                        continue

                self._add_trade(stats, wallet, is_buy, eth_amt, blk, ts)

                if trace_wallet and wallet == trace_wallet.lower():
                    trace.append({
                        'source': 'etherscan',
                        'pool': pool_addr, 'hash': h, 'ts': ts, 'blk': blk,
                        'kind': 'buy' if is_buy else 'sell',
                        'eth': eth_amt, 'orig_from_to': orig_wallet,
                        'resolved': orig_wallet != wallet,
                    })

        return trace

    def build_wallet_stats(self, token, trace_wallet=None):
        """Return {wallet: stats}, pools[, trace]. Combines two sources:
          1) GeckoTerminal — last ~600 trades/pool, EOA already attributed (fast, free).
          2) Etherscan — older history, with router/contract→EOA resolution. Any wallet
             that has deployed code on-chain (routers, aggregators, sniper bots, Safes)
             gets its tx origin resolved so we attribute the trade to the real EOA.
        Sources are merged by per-pool timestamp cutoff so trades aren't double-counted.

        If `trace_wallet` is given, also returns a list of every trade row encountered
        for that wallet (for /debug command).
        """
        # DexScreener is the authoritative source for pool/pair addresses (every DEX,
        # every chain). Use it as the primary. Gecko/DS pair addresses are identical
        # for pools that both index, so we don't need Gecko for pool discovery.
        dpairs = self.get_pairs(token)
        pools = [
            {'addr': p['addr'], 'name': 'WETH pair', 'liquidity_usd': p.get('liquidity_usd', 0)}
            for p in dpairs
        ]
        pools.sort(key=lambda x: -x['liquidity_usd'])

        # Fallback: if DexScreener hasn't indexed the token yet, try Gecko
        if not pools:
            gpools = self._gecko_pools(token)
            pools = [
                {'addr': p['addr'], 'name': p.get('name', ''),
                 'liquidity_usd': p.get('liquidity_usd', 0)}
                for p in gpools
            ]
        if not pools:
            return ({}, [], []) if trace_wallet else ({}, [])

        weth = WETH.lower()
        stats = defaultdict(lambda: {
            'eth_in': 0.0, 'eth_out': 0.0,
            'n_buys': 0, 'n_sells': 0,
            'first_block': 0, 'last_block': 0,
            'last_buy_ts': 0, 'last_sell_ts': 0,
            'buy_ts': [], 'sell_ts': [],
        })
        gecko_min_ts_per_pool = {}  # pool_addr -> earliest ts Gecko gave us
        gecko_trace = []

        # Pass A: GeckoTerminal (recent trades with EOA already)
        for pool in pools[:8]:
            trades = self._gecko_trades(pool['addr'])
            min_ts = None
            for t in trades:
                a = t.get('attributes') or {}
                wallet = (a.get('tx_from_address') or '').lower()
                if not wallet or wallet in ROUTERS:
                    continue
                kind = a.get('kind')
                from_tok = (a.get('from_token_address') or '').lower()
                to_tok = (a.get('to_token_address') or '').lower()
                try:
                    from_amt = float(a.get('from_token_amount') or 0)
                    to_amt = float(a.get('to_token_amount') or 0)
                    blk = int(a.get('block_number') or 0)
                except Exception:
                    continue
                ts_str = a.get('block_timestamp') or ''
                ts = 0
                if ts_str:
                    try:
                        ts = int(datetime.fromisoformat(ts_str.replace('Z', '+00:00')).timestamp())
                    except Exception:
                        ts = 0

                if kind == 'buy' and from_tok == weth:
                    eth_amt, is_buy = from_amt, True
                elif kind == 'sell' and to_tok == weth:
                    eth_amt, is_buy = to_amt, False
                else:
                    continue

                self._add_trade(stats, wallet, is_buy, eth_amt, blk, ts)
                if trace_wallet and wallet == trace_wallet.lower():
                    gecko_trace.append({
                        'source': 'gecko', 'pool': pool['addr'],
                        'ts': ts, 'blk': blk, 'hash': (a.get('tx_hash') or ''),
                        'kind': 'buy' if is_buy else 'sell', 'eth': eth_amt,
                    })
                if min_ts is None or ts < min_ts:
                    min_ts = ts
            if min_ts is not None:
                gecko_min_ts_per_pool[pool['addr']] = min_ts

        # Pass B: Etherscan (older history) — only for pools where the Etherscan key is set
        es_trace = []
        if self.esk and self.esk != 'x' and self.esk != 'notneeded':
            es_trace = self._etherscan_extend(
                token,
                [p['addr'] for p in pools[:8]],
                stats,
                gecko_min_ts_per_pool,
                trace_wallet=trace_wallet,
            ) or []

        if trace_wallet:
            return dict(stats), pools, gecko_trace + es_trace
        return dict(stats), pools

    def wallet_token_totals(self, token, wallet):
        """Exact buy/sell totals for a (token, wallet) pair using wallet-centric
        Etherscan queries. Unlike pool-centric scans, this isn't capped by pool
        activity (pools on active tokens have 100k+ tx, we only see the latest
        10k). A wallet's own history fits in one page.

        Handles all router patterns because we query:
          1) tokentx(token, wallet)     — every time the wallet moved this token
          2) tokentx(WETH, wallet)      — every time the wallet moved WETH
          3) txlist(wallet)             — native ETH sent (buys pay ETH as tx.value)
          4) txlistinternal(wallet)     — native ETH received (sells get ETH
                                          from the router via unwrap/send)

        For each token transfer tx hash, sum the wallet's ETH leg from those
        sources — works regardless of which router / sniper bot was used.
        """
        w = wallet.lower()
        cache_key = f'wtot_{token.lower()}_{w}'
        cached = self._cache_get(cache_key, ttl=300)  # 5 min cache
        if cached is not None:
            return cached

        def fetch(params):
            js = self._es(params)
            r = js.get('result', []) if isinstance(js, dict) else []
            return r if isinstance(r, list) else []

        tok_txs = fetch({
            'module': 'account', 'action': 'tokentx',
            'contractaddress': token, 'address': wallet,
            'sort': 'desc', 'offset': 1000, 'page': 1,
        })
        weth_txs = fetch({
            'module': 'account', 'action': 'tokentx',
            'contractaddress': WETH, 'address': wallet,
            'sort': 'desc', 'offset': 1000, 'page': 1,
        })
        norm_txs = fetch({
            'module': 'account', 'action': 'txlist',
            'address': wallet, 'sort': 'desc', 'offset': 1000, 'page': 1,
        })
        int_txs = fetch({
            'module': 'account', 'action': 'txlistinternal',
            'address': wallet, 'sort': 'desc', 'offset': 1000, 'page': 1,
        })

        weth_by_hash = defaultdict(list)
        for x in weth_txs:
            weth_by_hash[x.get('hash', '')].append(x)
        norm_by_hash = {x.get('hash', ''): x for x in norm_txs}
        int_by_hash = defaultdict(list)
        for x in int_txs:
            int_by_hash[x.get('hash', '')].append(x)

        eth_in = 0.0
        eth_out = 0.0
        n_buys = 0
        n_sells = 0
        buy_ts = []
        sell_ts = []
        seen = set()  # dedupe multi-transfers within same tx (tax rows)
        trades = []

        for t in tok_txs:
            try:
                h = t['hash']
                ts = int(t.get('timeStamp', 0) or 0)
                f_a = t['from'].lower()
                t_a = t['to'].lower()
            except Exception:
                continue
            is_buy = (t_a == w)
            is_sell = (f_a == w)
            if not (is_buy or is_sell):
                continue
            key = (h, is_buy)
            if key in seen:
                continue
            seen.add(key)

            eth_amt = 0.0
            if is_buy:
                # native ETH paid: tx.value from wallet
                nt = norm_by_hash.get(h)
                if nt and nt.get('from', '').lower() == w:
                    try:
                        eth_amt += int(nt.get('value', '0')) / 1e18
                    except Exception:
                        pass
                # or WETH sent from wallet
                for wt in weth_by_hash.get(h, []):
                    if wt.get('from', '').lower() == w:
                        try:
                            eth_amt += int(wt.get('value', '0')) / 1e18
                        except Exception:
                            pass
                if eth_amt > 0:
                    eth_in += eth_amt
                    n_buys += 1
                    buy_ts.append(ts)
                    trades.append({'kind': 'buy', 'ts': ts, 'eth': eth_amt, 'hash': h})
            else:  # sell
                # native ETH received via internal tx (router unwrap+send)
                for it in int_by_hash.get(h, []):
                    if it.get('to', '').lower() == w and it.get('isError', '0') == '0':
                        try:
                            eth_amt += int(it.get('value', '0')) / 1e18
                        except Exception:
                            pass
                # or WETH received
                for wt in weth_by_hash.get(h, []):
                    if wt.get('to', '').lower() == w:
                        try:
                            eth_amt += int(wt.get('value', '0')) / 1e18
                        except Exception:
                            pass
                if eth_amt > 0:
                    eth_out += eth_amt
                    n_sells += 1
                    sell_ts.append(ts)
                    trades.append({'kind': 'sell', 'ts': ts, 'eth': eth_amt, 'hash': h})

        result = {
            'eth_in': eth_in, 'eth_out': eth_out,
            'n_buys': n_buys, 'n_sells': n_sells,
            'buy_ts': buy_ts, 'sell_ts': sell_ts,
            'trades': trades,
            'n_tokentx': len(tok_txs),
        }
        self._cache_set(cache_key, result)
        return result

    def debug_wallet(self, token, wallet):
        """Ground-truth diagnostic for a specific (token, wallet) pair.

        1) Direct Etherscan query: every tokentx row where this wallet moved
           this token. No pool filter, no router resolution — just the raw facts.
        2) Run the regular pool-based matcher and compare.
        3) Report which counter-party addresses (pools/routers) the wallet
           touched, so we can see whether our scan missed a pool.
        """
        w = wallet.lower()
        # --- Direct wallet-token query ---
        js = self._es({
            'module': 'account', 'action': 'tokentx',
            'contractaddress': token, 'address': wallet,
            'sort': 'desc', 'offset': 1000, 'page': 1,
        })
        direct_txs = js.get('result', []) if isinstance(js, dict) else []
        if not isinstance(direct_txs, list):
            direct_txs = []

        direct_buys = 0
        direct_sells = 0
        counterparties = defaultdict(int)  # pool/router -> n tx
        direct_trades = []
        for t in direct_txs:
            try:
                to_a = t['to'].lower()
                from_a = t['from'].lower()
                ts = int(t.get('timeStamp', 0) or 0)
                h = t['hash']
            except Exception:
                continue
            if from_a == w:
                direct_sells += 1
                counterparties[to_a] += 1
                direct_trades.append({'kind': 'sell', 'ts': ts, 'cpty': to_a, 'hash': h})
            elif to_a == w:
                direct_buys += 1
                counterparties[from_a] += 1
                direct_trades.append({'kind': 'buy', 'ts': ts, 'cpty': from_a, 'hash': h})

        # --- Regular matcher output for comparison ---
        stats, pools, trace = self.build_wallet_stats(token, trace_wallet=wallet)
        s = stats.get(w)

        scanned_set = {p['addr'] for p in pools[:8]}
        missed_cpties = [a for a in counterparties if a not in scanned_set]

        # --- Per-pool health check: is each "pool" a real WETH pool? ---
        # A real Uniswap WETH pool has many token-txs AND many WETH-txs. A mis-
        # classified address (router/bot/bridge) will have lots of token-txs
        # but 0 or very few WETH transfers where *it* is counterparty.
        pool_health = []
        for pool_addr in [p['addr'] for p in pools[:8]]:
            try:
                tok_n = len(self._tokentx_pair(token, pool_addr))
                weth_n = len(self._tokentx_pair(WETH, pool_addr))
            except Exception:
                tok_n, weth_n = -1, -1
            pool_health.append({'addr': pool_addr, 'token_tx': tok_n, 'weth_tx': weth_n})

        # --- Per-trade trace: for each of the wallet's direct hashes, see where
        #     the WETH leg landed (if anywhere in our scanned pools). ---
        hash_diag = []
        for dt in direct_trades[:10]:
            h = dt['hash']
            kind = dt['kind']
            found_pool = None
            eth_leg = 0.0
            for pool_addr in [p['addr'] for p in pools[:8]]:
                try:
                    weth_txs = self._tokentx_pair(WETH, pool_addr)
                except Exception:
                    continue
                for weth_t in weth_txs:
                    if weth_t.get('hash') != h:
                        continue
                    wf = weth_t['from'].lower()
                    wt_ = weth_t['to'].lower()
                    val = int(weth_t['value']) / 1e18
                    # buy: WETH to pool; sell: WETH from pool
                    if kind == 'buy' and wt_ == pool_addr:
                        eth_leg += val
                        found_pool = pool_addr
                    elif kind == 'sell' and wf == pool_addr:
                        eth_leg += val
                        found_pool = pool_addr
                if found_pool:
                    break
            hash_diag.append({
                'hash': h, 'kind': kind,
                'cpty': dt['cpty'], 'found_pool': found_pool, 'eth_leg': eth_leg,
            })

        # --- Wallet-centric totals (THE ground-truth number) ---
        try:
            totals = self.wallet_token_totals(token, wallet)
        except Exception as e:
            totals = {'error': str(e)[:200]}

        return {
            'wallet': w,
            'stats': s,
            'pools_scanned': [p['addr'] for p in pools[:8]],
            'n_pools_total': len(pools),
            'trace': trace,
            'is_contract': self._is_contract(wallet),
            # direct-query diagnostics:
            'direct_n': len(direct_txs),
            'direct_buys': direct_buys,
            'direct_sells': direct_sells,
            'direct_counterparties': dict(counterparties),
            'missed_counterparties': missed_cpties,
            'direct_trades': direct_trades[:20],
            'pool_health': pool_health,
            'hash_diag': hash_diag,
            # authoritative wallet-centric totals
            'totals': totals,
        }

    def search_by_times(self, token, min_buy_ts, max_buy_ts, min_sell_ts, max_sell_ts, top_n=10):
        """Return wallets that bought in [min_buy_ts, max_buy_ts] AND sold in [min_sell_ts,
        max_sell_ts]. If a bound is None, that side is ignored. Ranks by total eth invested desc.
        """
        stats, pairs = self.build_wallet_stats(token)
        require_buy = (min_buy_ts is not None) or (max_buy_ts is not None)
        require_sell = (min_sell_ts is not None) or (max_sell_ts is not None)
        lb, ub = min_buy_ts or 0, max_buy_ts or 10**12
        ls, us = min_sell_ts or 0, max_sell_ts or 10**12
        results = []
        for wallet, s in stats.items():
            buys_in = [t for t in s['buy_ts'] if lb <= t <= ub] if require_buy else s['buy_ts']
            sells_in = [t for t in s['sell_ts'] if ls <= t <= us] if require_sell else s['sell_ts']
            if require_buy and not buys_in:
                continue
            if require_sell and not sells_in:
                continue
            if not require_buy and not require_sell:
                continue
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
        # rank by realized pnl desc, then invested desc as tiebreaker
        results.sort(key=lambda r: (-r['pnl_eth'], -r['invested_eth']))
        return results[:top_n], pairs, len(stats)

    def find_matches(self, token, invested_eth, sold_eth, top_n=5,
                     min_activity=True, tol=0.05, verify_top_k=40):
        """Return wallets within ±tol of the invested target AND (if sold_eth>0) ±tol
        of the sold target. `tol` is relative — 0.05 = ±5%.

        Two-stage: pool-scan for candidate discovery, then wallet-centric
        verification for the top-K loosest matches. Pool-scan is capped by
        Etherscan's 10k-rows-per-pool return, so totals can be partial on busy
        tokens. Wallet-centric verification (wallet_token_totals) has no such
        cap and uses txlist+txlistinternal to catch native-ETH flows.
        """
        stats, pairs = self.build_wallet_stats(token)
        eps = 1e-6
        inv_lo = invested_eth * (1 - tol)
        inv_hi = invested_eth * (1 + tol)
        sold_lo = sold_eth * (1 - tol)
        sold_hi = sold_eth * (1 + tol)

        # Loose prefilter: wallets where pool-scan shows some activity AND at least
        # rough order-of-magnitude match. Pool totals can be partial (capped), so
        # we cast a wide net (5×) before the exact wallet-centric verify pass.
        prefilter = []
        for wallet, s in stats.items():
            if s['n_buys'] + s['n_sells'] == 0:
                continue
            # wallets with *at most* 5× the target amounts are candidates
            if s['eth_in'] > max(invested_eth * 5, 5) + 1:
                continue
            if sold_eth > 0 and s['eth_out'] > max(sold_eth * 5, 5) + 1:
                continue
            # rough distance based on pool-scan values (sort to prioritize verify)
            rel = abs(math.log((s['eth_in'] + eps) / (invested_eth + eps)))
            if sold_eth > 0:
                rel += abs(math.log((s['eth_out'] + eps) / (sold_eth + eps)))
            prefilter.append((rel, wallet, s))
        prefilter.sort(key=lambda x: x[0])

        filt = {
            'total': len(stats),
            'prefilter': len(prefilter),
            'verified': 0,
            'inv_ok': 0,
            'sell_ok': 0,
        }

        # Verify up to verify_top_k candidates via wallet-centric exact totals.
        # Always include ALL wallets whose pool-scan stats already match ±tol on both
        # axes (they're likely real matches — don't drop them just to fit K).
        must_verify = set()
        for _, wallet, s in prefilter:
            if inv_lo <= s['eth_in'] <= inv_hi:
                if sold_eth == 0 or (s['eth_out'] > 0 and sold_lo <= s['eth_out'] <= sold_hi):
                    must_verify.add(wallet)

        verify_set = list(must_verify)
        for _, wallet, _ in prefilter:
            if wallet in must_verify:
                continue
            if len(verify_set) >= verify_top_k:
                break
            verify_set.append(wallet)

        results = []
        for wallet in verify_set:
            try:
                tot = self.wallet_token_totals(token, wallet)
            except Exception:
                continue
            filt['verified'] += 1
            ei = tot['eth_in']
            eo = tot['eth_out']

            if not (inv_lo <= ei <= inv_hi):
                continue
            filt['inv_ok'] += 1
            if sold_eth > 0:
                if tot['n_sells'] == 0 or eo == 0:
                    continue
                if not (sold_lo <= eo <= sold_hi):
                    continue
                filt['sell_ok'] += 1
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
                'first_block': 0,
                'last_block': 0,
                'last_buy_ts': max(tot['buy_ts']) if tot['buy_ts'] else 0,
                'last_sell_ts': max(tot['sell_ts']) if tot['sell_ts'] else 0,
                'dist': rel_inv + rel_sold,
                'bucket': 'seller' if tot['n_sells'] > 0 else 'holder',
            })
        results.sort(key=lambda r: r['dist'])
        return results[:top_n], pairs, filt


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
