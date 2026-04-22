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

    def _cache_get(self, key):
        p = self._cache_path(key)
        if os.path.exists(p) and (time.time() - os.path.getmtime(p) < self.cache_ttl):
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
    def build_wallet_stats(self, token):
        pairs = self.get_pairs(token)
        if not pairs:
            return {}, []

        stats = defaultdict(lambda: {
            'eth_in': 0.0, 'eth_out': 0.0,
            'n_buys': 0, 'n_sells': 0,
            'first_block': 0, 'last_block': 0,
            'last_buy_ts': 0, 'last_sell_ts': 0,
        })

        for pair in pairs[:3]:  # cap at top-3 liquid pairs
            pair_low = pair['addr']
            tok_txs = self._tokentx_pair(token, pair_low)
            weth_txs = self._tokentx_pair(WETH, pair_low)
            if not tok_txs:
                continue

            # Build WETH events indexed by tx hash
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

                # Match WETH flow in the same tx
                weth_evs = weth_by_hash.get(h, [])
                eth_amt = 0.0
                if is_buy:
                    for w in weth_evs:
                        if w['to'] == pair_low:
                            eth_amt += w['value']
                    wallet = to_a
                    if wallet in ROUTERS or wallet == pair_low:
                        continue
                    s = stats[wallet]
                    s['eth_in'] += eth_amt
                    s['n_buys'] += 1
                    if ts > s['last_buy_ts']:
                        s['last_buy_ts'] = ts
                else:  # sell
                    for w in weth_evs:
                        if w['from'] == pair_low:
                            eth_amt += w['value']
                    wallet = from_a
                    if wallet in ROUTERS or wallet == pair_low:
                        continue
                    s = stats[wallet]
                    s['eth_out'] += eth_amt
                    s['n_sells'] += 1
                    if ts > s['last_sell_ts']:
                        s['last_sell_ts'] = ts

                if s['first_block'] == 0 or blk < s['first_block']:
                    s['first_block'] = blk
                if blk > s['last_block']:
                    s['last_block'] = blk

        return dict(stats), pairs

    def find_matches(self, token, invested_eth, sold_eth, top_n=5, min_activity=True):
        stats, pairs = self.build_wallet_stats(token)
        results = []
        for wallet, s in stats.items():
            if min_activity and s['n_buys'] == 0:
                continue
            if sold_eth > 0 and s['n_sells'] == 0:
                continue
            # symmetric log-distance — treats 2× and 0.5× the target as equal misses,
            # and no-sells (eth_out=0) get a huge penalty instead of saturating at 1.0
            eps = 1e-6
            rel_inv = abs(math.log((s['eth_in'] + eps) / (invested_eth + eps)))
            if sold_eth > 0:
                rel_sold = abs(math.log((s['eth_out'] + eps) / (sold_eth + eps)))
            else:
                rel_sold = 0 if s['eth_out'] == 0 else 10
            dist = rel_inv + rel_sold
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
                'dist': dist,
            })
        results.sort(key=lambda r: r['dist'])
        return results[:top_n], pairs, len(stats)


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
