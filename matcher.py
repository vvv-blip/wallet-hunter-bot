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

    # ---- main aggregation ----
    def build_wallet_stats(self, token, origin_budget_seconds=25, origin_max_lookups=400):
        pairs = self.get_pairs(token)
        if not pairs:
            return {}, []

        # Pass 1: collect raw trades (don't skip routers yet — we'll reassign them)
        trades = []
        for pair in pairs[:3]:  # cap at top-3 liquid pairs
            pair_low = pair['addr']
            tok_txs = self._tokentx_pair(token, pair_low)
            weth_txs = self._tokentx_pair(WETH, pair_low)
            if not tok_txs:
                continue

            weth_by_hash = defaultdict(list)
            for w in weth_txs:
                try:
                    weth_by_hash[w['hash']].append({
                        'to': w['to'].lower(),
                        'from': w['from'].lower(),
                        'value': int(w['value']) / 1e18,
                    })
                except Exception:
                    continue

            for t in tok_txs:
                try:
                    to_a = t['to'].lower()
                    from_a = t['from'].lower()
                    h = t['hash']
                    blk = int(t['blockNumber'])
                    ts = int(t.get('timeStamp', 0) or 0)
                except Exception:
                    continue

                is_buy = (from_a == pair_low and to_a != pair_low)
                is_sell = (to_a == pair_low and from_a != pair_low)
                if not (is_buy or is_sell):
                    continue

                weth_evs = weth_by_hash.get(h, [])
                eth_amt = 0.0
                if is_buy:
                    for w in weth_evs:
                        if w['to'] == pair_low:
                            eth_amt += w['value']
                    wallet = to_a
                else:
                    for w in weth_evs:
                        if w['from'] == pair_low:
                            eth_amt += w['value']
                    wallet = from_a

                if wallet == pair_low:
                    continue

                trades.append({
                    'wallet': wallet, 'is_buy': is_buy, 'eth': eth_amt,
                    'hash': h, 'blk': blk, 'ts': ts,
                })

        # Pass 2: detect unknown router/aggregator contracts (wallets with both buys & sells
        # that happen to have on-chain code) and re-attribute their trades to tx origins
        per_wallet = defaultdict(lambda: {'buys': 0, 'sells': 0})
        for tr in trades:
            k = 'buys' if tr['is_buy'] else 'sells'
            per_wallet[tr['wallet']][k] += 1

        suspicious = set(ROUTERS)
        # a real router handles many trades on BOTH sides; require at least ~8 total activity
        # to avoid flagging a retail trader who did one buy and one sell as a router
        router_candidates = [
            w for w, a in per_wallet.items()
            if w not in ROUTERS and a['buys'] > 0 and a['sells'] > 0
            and (a['buys'] + a['sells']) >= 8
        ]
        router_candidates.sort(key=lambda w: -(per_wallet[w]['buys'] + per_wallet[w]['sells']))
        for w in router_candidates[:40]:
            if self._is_contract(w):
                suspicious.add(w)

        # Resolve tx origins for suspicious-wallet trades, bounded by time + count budget
        if suspicious:
            deadline = time.time() + origin_budget_seconds
            lookups = 0
            origin_by_hash = {}
            reassigned = []
            for tr in trades:
                if tr['wallet'] not in suspicious:
                    reassigned.append(tr)
                    continue
                h = tr['hash']
                if h not in origin_by_hash:
                    cached = self._cache_get(f'origin_{h}', ttl=86400 * 365)
                    if cached is not None:
                        origin_by_hash[h] = cached
                    elif lookups < origin_max_lookups and time.time() < deadline:
                        origin_by_hash[h] = self._get_tx_origin(h) or ''
                        lookups += 1
                    else:
                        origin_by_hash[h] = ''
                real = origin_by_hash[h]
                if not real or real in suspicious or real == tr['wallet']:
                    continue  # drop — couldn't resolve to an EOA
                reassigned.append({**tr, 'wallet': real})
            trades = reassigned

        # Pass 3: aggregate per (final) wallet
        stats = defaultdict(lambda: {
            'eth_in': 0.0, 'eth_out': 0.0,
            'n_buys': 0, 'n_sells': 0,
            'first_block': 0, 'last_block': 0,
            'last_buy_ts': 0, 'last_sell_ts': 0,
            'buy_ts': [], 'sell_ts': [],
        })
        for tr in trades:
            s = stats[tr['wallet']]
            if tr['is_buy']:
                s['eth_in'] += tr['eth']
                s['n_buys'] += 1
                s['buy_ts'].append(tr['ts'])
                if tr['ts'] > s['last_buy_ts']:
                    s['last_buy_ts'] = tr['ts']
            else:
                s['eth_out'] += tr['eth']
                s['n_sells'] += 1
                s['sell_ts'].append(tr['ts'])
                if tr['ts'] > s['last_sell_ts']:
                    s['last_sell_ts'] = tr['ts']
            if s['first_block'] == 0 or tr['blk'] < s['first_block']:
                s['first_block'] = tr['blk']
            if tr['blk'] > s['last_block']:
                s['last_block'] = tr['blk']

        return dict(stats), pairs

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

    def find_matches(self, token, invested_eth, sold_eth, top_n=5, min_activity=True, tol=0.05):
        """Return wallets within ±tol of the invested target AND (if sold_eth>0) ±tol of the
        sold target. When sold_eth is 0, returns matches that only bought the target amount.
        `tol` is relative — 0.05 = ±5%.
        """
        stats, pairs = self.build_wallet_stats(token)
        eps = 1e-6
        inv_lo = invested_eth * (1 - tol)
        inv_hi = invested_eth * (1 + tol)
        sold_lo = sold_eth * (1 - tol)
        sold_hi = sold_eth * (1 + tol)

        # Diagnostics so the bot can tell the user WHY nothing matched
        filt = {'total': len(stats), 'inv_ok': 0, 'sell_ok': 0}
        results = []
        for wallet, s in stats.items():
            if min_activity and s['n_buys'] == 0:
                continue
            if not (inv_lo <= s['eth_in'] <= inv_hi):
                continue
            filt['inv_ok'] += 1

            if sold_eth > 0:
                # hard filter: must have actually sold within ±tol of target
                if s['n_sells'] == 0 or s['eth_out'] == 0:
                    continue
                if not (sold_lo <= s['eth_out'] <= sold_hi):
                    continue
                filt['sell_ok'] += 1
                rel_sold = abs(math.log((s['eth_out'] + eps) / (sold_eth + eps)))
            else:
                rel_sold = 0
                filt['sell_ok'] += 1

            rel_inv = abs(math.log((s['eth_in'] + eps) / (invested_eth + eps)))
            results.append({
                'wallet': wallet,
                'invested_eth': s['eth_in'],
                'sold_eth': s['eth_out'],
                'pnl_eth': s['eth_out'] - s['eth_in'],
                'n_buys': s['n_buys'],
                'n_sells': s['n_sells'],
                'first_block': s['first_block'],
                'last_block': s['last_block'],
                'last_buy_ts': s['last_buy_ts'],
                'last_sell_ts': s['last_sell_ts'],
                'dist': rel_inv + rel_sold,
                'bucket': 'seller' if s['n_sells'] > 0 else 'holder',
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
