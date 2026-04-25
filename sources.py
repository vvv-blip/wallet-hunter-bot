"""Free-API data layer for the wallet-hunter bot.

Three classes:
  - Cache         : tiny TTL JSON file cache (mtime-based).
  - GTSource      : GeckoTerminal (free, no key, ~30 r/s).  Pools, per-pool
                    trades parsed to (wallet, kind, eth, token_amt, usd, ts,
                    tx_hash), token-wide aggregation across top pools, OHLCV,
                    and historical ETH price lookup via WETH/USDC daily candles.
  - EtherscanSource : Etherscan v2 (free, 5 r/s, 100k/day) wrapped with a
                    thread-safe leaky-bucket rate limiter, retries on the
                    "Max rate limit reached" string, and aggressive caching.
                    High-level helpers: wallet_token_totals (buy/sell ETH
                    reconciled against WETH transfers + internal ETH legs),
                    wallet_age_days, wallet_distinct_tokens, wallet_funding_source
                    (with built-in CEX / mixer label dict), wallet_deployed_contracts.

Drop-in for matcher.py — public methods are cached or take an explicit ttl.
"""
import os, time, json, math, threading, collections, requests
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed


WETH = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
USDC_WETH_V2 = '0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc'  # Uniswap V2 WETH/USDC

# ──────────────────────────────────────────────────────────────────────────────
# 1) Cache
# ──────────────────────────────────────────────────────────────────────────────


class Cache:
    """Tiny TTL JSON file cache.  mtime is the freshness clock; all errors
    are swallowed so a corrupt cache file just becomes a miss."""

    def __init__(self, cache_dir, default_ttl=1800):
        self.cache_dir = cache_dir
        self.default_ttl = default_ttl
        try:
            os.makedirs(cache_dir, exist_ok=True)
        except Exception:
            pass

    def _path(self, key):
        # sanitize: keys can include addresses w/ colons etc.
        safe = ''.join(c if c.isalnum() or c in '_-.' else '_' for c in key)
        return os.path.join(self.cache_dir, safe + '.json')

    def get(self, key, ttl=None):
        if ttl is None:
            ttl = self.default_ttl
        p = self._path(key)
        try:
            if os.path.exists(p) and (time.time() - os.path.getmtime(p) < ttl):
                with open(p) as f:
                    return json.load(f)
        except Exception:
            pass
        return None

    def set(self, key, val):
        try:
            with open(self._path(key), 'w') as f:
                json.dump(val, f)
        except Exception:
            pass


# ──────────────────────────────────────────────────────────────────────────────
# 2) GTSource — GeckoTerminal
# ──────────────────────────────────────────────────────────────────────────────


class GTSource:
    """GeckoTerminal client.  Free, no key, ~30 r/s."""

    BASE = 'https://api.geckoterminal.com/api/v2'

    def __init__(self, cache):
        self.cache = cache

    # ---- http -------------------------------------------------------------
    def _req(self, path, params=None, timeout=10):
        url = self.BASE + path
        last = None
        for attempt in range(3):
            try:
                r = requests.get(url, params=(params or {}),
                                 headers={'accept': 'application/json'},
                                 timeout=timeout)
                if r.status_code == 200:
                    try:
                        return r.json()
                    except Exception:
                        return {}
                if r.status_code == 429 or 500 <= r.status_code < 600:
                    last = f'http_{r.status_code}'
                    time.sleep(0.5 * (attempt + 1))
                    continue
                last = f'http_{r.status_code}'
                break
            except requests.exceptions.Timeout:
                last = 'timeout'
                time.sleep(0.5 * (attempt + 1))
            except Exception as e:
                last = str(e)[:120]
                time.sleep(0.3)
        return {}

    # ---- pools ------------------------------------------------------------
    def pools(self, token, network='eth', n=15, ttl=900):
        """Top N pool addresses for a token, in GT's default order
        (a volume/liquidity proxy).  Paginates `?page=1..5` (20/page)."""
        ck = f'gt_pools_{network}_{token.lower()}_n{n}'
        cached = self.cache.get(ck, ttl=ttl)
        if cached is not None:
            return cached
        out = []
        page = 1
        while len(out) < n and page <= 5:
            j = self._req(f'/networks/{network}/tokens/{token}/pools',
                          params={'page': page})
            rows = (j.get('data') or []) if isinstance(j, dict) else []
            if not rows:
                break
            for row in rows:
                addr = ((row.get('attributes') or {}).get('address') or '').lower()
                if addr:
                    out.append(addr)
                if len(out) >= n:
                    break
            page += 1
        # Only cache non-empty results — avoid poisoning cache on transient API errors.
        if out:
            self.cache.set(ck, out)
        return out

    # ---- trades -----------------------------------------------------------
    @staticmethod
    def _parse_iso_ts(s):
        if not s:
            return 0
        try:
            return int(datetime.fromisoformat(s.replace('Z', '+00:00')).timestamp())
        except Exception:
            return 0

    def pool_trades(self, pool, network='eth', ttl=300):
        """Up to ~300 most-recent trades on a pool, parsed to a flat list of
        dicts keyed on the WETH leg.  Skips rows where neither side is WETH.

        Each dict: {wallet, kind, eth, token_amt, usd, ts, tx_hash}.
        """
        pool_lc = pool.lower()
        ck = f'gt_trades_{network}_{pool_lc}'
        cached = self.cache.get(ck, ttl=ttl)
        if cached is not None:
            return cached
        j = self._req(f'/networks/{network}/pools/{pool}/trades')
        rows = (j.get('data') or []) if isinstance(j, dict) else []
        out = []
        weth = WETH
        for row in rows:
            attrs = row.get('attributes') or {}
            from_addr = (attrs.get('from_token_address') or '').lower()
            to_addr = (attrs.get('to_token_address') or '').lower()
            if from_addr != weth and to_addr != weth:
                continue
            try:
                from_amt = float(attrs.get('from_token_amount') or 0)
                to_amt = float(attrs.get('to_token_amount') or 0)
            except Exception:
                from_amt = to_amt = 0.0
            if from_addr == weth:
                eth_amt = from_amt
                token_amt = to_amt
            else:
                eth_amt = to_amt
                token_amt = from_amt
            try:
                usd = float(attrs.get('volume_in_usd') or 0)
            except Exception:
                usd = 0.0
            wallet = (attrs.get('tx_from_address') or '').lower()
            ts = self._parse_iso_ts(attrs.get('block_timestamp') or '')
            kind = attrs.get('kind') or ''
            tx_hash = attrs.get('tx_hash') or ''
            if not wallet or not tx_hash:
                continue
            out.append({
                'wallet': wallet,
                'kind': kind,
                'eth': eth_amt,
                'token_amt': token_amt,
                'usd': usd,
                'ts': ts,
                'tx_hash': tx_hash,
            })
        # Only cache non-empty results — avoid poisoning cache on transient API errors.
        if out:
            self.cache.set(ck, out)
        return out

    def token_trades(self, token, network='eth', n_pools=15, max_workers=8):
        """All recent trades for a token, aggregated across the top N pools.
        Deduped on tx_hash, sorted by ts desc."""
        try:
            pools = self.pools(token, network=network, n=n_pools)
        except Exception:
            pools = []
        if not pools:
            return []
        results = {}
        with ThreadPoolExecutor(max_workers=min(max_workers, max(1, len(pools)))) as ex:
            futs = {ex.submit(self.pool_trades, p, network): p for p in pools}
            for fut in as_completed(futs):
                pool = futs[fut]
                try:
                    results[pool] = fut.result()
                except Exception:
                    results[pool] = []
        merged = {}
        for pool in pools:
            for tr in results.get(pool, []):
                h = tr.get('tx_hash')
                if not h:
                    continue
                # keep the first occurrence (top-pool wins on ties)
                if h not in merged:
                    merged[h] = tr
        out = list(merged.values())
        out.sort(key=lambda x: -(x.get('ts') or 0))
        return out

    # ---- ohlcv + price ----------------------------------------------------
    def pool_ohlcv(self, pool, timeframe='minute', limit=1000, network='eth', ttl=600):
        """Raw OHLCV rows from /networks/{net}/pools/{pool}/ohlcv/{tf}.
        Returns [[ts, open, high, low, close, volume_usd], ...]."""
        ck = f'gt_ohlcv_{network}_{pool.lower()}_{timeframe}_{limit}'
        cached = self.cache.get(ck, ttl=ttl)
        if cached is not None:
            return cached
        j = self._req(f'/networks/{network}/pools/{pool}/ohlcv/{timeframe}',
                      params={'limit': limit})
        attrs = ((j.get('data') or {}).get('attributes') or {}) if isinstance(j, dict) else {}
        rows = attrs.get('ohlcv_list') or []
        out = []
        for r in rows:
            try:
                if len(r) >= 6:
                    out.append([int(r[0]), float(r[1]), float(r[2]),
                                float(r[3]), float(r[4]), float(r[5])])
            except Exception:
                continue
        # GT returns newest-first; sort ascending so binary-search by ts works.
        out.sort(key=lambda x: x[0])
        # Only cache non-empty results — avoid poisoning cache on transient API errors.
        if out:
            self.cache.set(ck, out)
        return out

    def eth_price_at(self, ts, network='eth'):
        """USD per ETH at unix `ts`.  Pulls hour-granularity OHLCV from the
        Uniswap V2 WETH/USDC pool and bucket-locates `ts`.  Falls back to
        3500.0 if the lookup fails."""
        try:
            ts = int(ts)
        except Exception:
            return 3500.0
        try:
            rows = self.pool_ohlcv(USDC_WETH_V2, timeframe='hour',
                                   limit=1000, network=network, ttl=3600 * 6)
        except Exception:
            rows = []
        if not rows:
            return 3500.0
        # binary-search for the bucket whose ts <= target
        lo, hi = 0, len(rows) - 1
        if ts <= rows[0][0]:
            close = rows[0][4]
            return float(close) if close and 100 < close < 20000 else 3500.0
        if ts >= rows[-1][0]:
            close = rows[-1][4]
            return float(close) if close and 100 < close < 20000 else 3500.0
        while lo < hi:
            mid = (lo + hi + 1) // 2
            if rows[mid][0] <= ts:
                lo = mid
            else:
                hi = mid - 1
        close = rows[lo][4]
        try:
            v = float(close)
            if 100 < v < 20000:
                return v
        except Exception:
            pass
        return 3500.0


# ──────────────────────────────────────────────────────────────────────────────
# 3) EtherscanSource — Etherscan v2
# ──────────────────────────────────────────────────────────────────────────────


# Known CEX hot wallets / mixer addresses for funding-source labelling.
CEX_LABELS = {
    '0x28c6c06298d514db089934071355e5743bf21d60': 'Binance 14',
    '0x21a31ee1afc51d94c2efccaa2092ad1028285549': 'Binance 15',
    '0x9696f59e4d72e237be84ffd425dcad154bf96976': 'Binance 18',
    '0x71660c4005ba85c37ccec55d0c4493e66fe775d3': 'Coinbase 1',
    '0x503828976d22510aad0201ac7ec88293211d23da': 'Coinbase 2',
    '0xddfabcdc4d8ffc6d5beaf154f18b778f892a0740': 'Coinbase 3',
    '0x3cd751e6b0078be393132286c442345e5dc49699': 'Coinbase 4',
    '0x77696bb39917c91a0c3908d577d5e322095425ca': 'Coinbase 5',
    '0x77c33b8a52e23ee9a47b6f6a1b8d7a18b4f78c8d': 'Coinbase 6',
    '0xa910f92acdaf488fa6ef02174fb86208ad7722ba': 'Coinbase 8',
    '0x267be1c1d684f78cb4f6a176c4911b741e4ffdc0': 'Kraken 1',
    '0xae2d4617c862309a3d75a0ffb358c7a5009c673f': 'Kraken 2',
    '0xfa52274dd61e1643d2205169732f29114bc240b3': 'Kraken 3',
    '0xa1d8d972560c2f8144af871db508f0b0b10a3fbf': 'Kraken 4',
    '0xb01e8d534508a1cdee93dadd7f648c30b71f4f1b': 'Bitfinex',
    '0x47ac0fb4f2d84898e4d9e7b4dab3c24507a6d503': 'Bitfinex 2',
    '0x742d35cc6634c0532925a3b844bc454e4438f44e': 'Bitfinex 3',
    '0x876eabf441b2ee5b5b0554fd502a8e0600950cfa': 'Bitfinex',
    '0x0d0707963952f2fba59dd06f2b425ace40b492fe': 'Gate.io',
    '0x46340b20830761efd32832a74d7169b29feb9758': 'Crypto.com',
    '0x46f80018211d5cbbc988e853a8683501fca4ee9b': 'Bybit',
    '0xf89d7b9c864f589bbf53a82105107622b35eaa40': 'Bybit Hot',
    '0xf977814e90da44bfa03b6295a0616a897441acec': 'Binance 8',
    '0xdfd5293d8e347dfe59e90efd55b2956a1343963d': 'Binance 16',
    '0x56eddb7aa87536c09ccc2793473599fd21a8b17f': 'Binance 17',
    '0x4e9ce36e442e55ecd9025b9a6e0d88485d628a67': 'Binance 19',
    '0x4976a4a02f38326660d17bf34b431dc6e2eb2327': 'OKX',
    '0x6cc5f688a315f3dc28a7781717a9a798a59fda7b': 'OKX 2',
    '0xa7efae728d2936e78bda97dc267687568dd593f3': 'OKX 3',
    '0x236f9f97e0e62388479bf9e5ba4889e46b0273c3': 'OKX 4',
    '0x176f3dab24a159341c0509bb36b833e7fdd0a132': 'OKX 6',
    '0xa294cca691e4c83b1fc0c8d63d9a3eef0a196de1': 'OKX 7',
    '0x68841a1806ff291314946eebd0cda8b348e73d6d': 'OKX 8',
    '0x47ce0c6ed5b0ce3d3a51fdb1c52dc66a7c3c2936': 'Tornado 0.1 ETH',
    '0x910cbd523d972eb0a6f4cae4618ad62622b39dbf': 'Tornado 1 ETH',
    '0xa160cdab225685da1d56aa342ad8841c3b53f291': 'Tornado 10 ETH',
    '0xb20c66c4de72433f3ce747b58b86fdbc1bea4d18': 'Tornado 100 ETH',
}


class EtherscanSource:
    """Etherscan v2 client.  5 r/s shared rate limit, retries on
    'Max rate limit reached', cache-aggressive."""

    BASE = 'https://api.etherscan.io/v2/api'

    def __init__(self, api_key, cache, rate_limit_per_sec=5):
        self.key = api_key
        self.cache = cache
        self.rate = max(1, int(rate_limit_per_sec))
        self._lock = threading.Lock()
        self._calls = collections.deque()  # timestamps of recent _get calls

    # ---- rate limit -------------------------------------------------------
    def _throttle(self):
        """Leaky bucket: at most `self.rate` calls per rolling 1-second window."""
        with self._lock:
            now = time.time()
            # prune older than 1s
            while self._calls and (now - self._calls[0]) >= 1.0:
                self._calls.popleft()
            if len(self._calls) >= self.rate:
                wait = 1.0 - (now - self._calls[0]) + 0.005
                if wait > 0:
                    time.sleep(wait)
                    now = time.time()
                    while self._calls and (now - self._calls[0]) >= 1.0:
                        self._calls.popleft()
            self._calls.append(time.time())

    # ---- http -------------------------------------------------------------
    def _get(self, params, chain_id=1):
        if not self.key:
            return {'_error': 'no_key'}
        p = dict(params)
        p.setdefault('chainid', chain_id)
        p['apikey'] = self.key
        last = None
        for attempt in range(3):
            self._throttle()
            try:
                r = requests.get(self.BASE, params=p, timeout=15)
                if r.status_code == 200:
                    try:
                        j = r.json()
                    except Exception:
                        return {'_error': 'json_decode'}
                    res = j.get('result') if isinstance(j, dict) else None
                    if isinstance(res, str) and 'Max rate limit reached' in res:
                        time.sleep(1.0)
                        last = 'rate_limited'
                        continue
                    return j
                if r.status_code == 429 or 500 <= r.status_code < 600:
                    last = f'http_{r.status_code}'
                    time.sleep(0.5 * (attempt + 1))
                    continue
                last = f'http_{r.status_code}'
                break
            except requests.exceptions.Timeout:
                last = 'timeout'
                time.sleep(0.5 * (attempt + 1))
            except Exception as e:
                last = str(e)[:120]
                time.sleep(0.3)
        return {'_error': last or 'unknown'}

    def _paginate(self, params, max_pages=20, page_size=10000, chain_id=1):
        """Walks `page=1..max_pages` with `offset=page_size`.
        Stops when result is empty or shorter than page_size."""
        out = []
        for page in range(1, max_pages + 1):
            p = dict(params)
            p['page'] = page
            p['offset'] = page_size
            j = self._get(p, chain_id=chain_id)
            if not isinstance(j, dict) or '_error' in j:
                break
            res = j.get('result')
            if not isinstance(res, list):
                break
            out.extend(res)
            if len(res) < page_size:
                break
        return out

    # ---- raw fetchers (cached) -------------------------------------------
    def tokentx_for_address_token(self, wallet, token, max_pages=10):
        ck = f'es_tt_at_{wallet.lower()}_{token.lower()}_p{max_pages}'
        cached = self.cache.get(ck, ttl=300)
        if cached is not None:
            return cached
        rows = self._paginate({
            'module': 'account', 'action': 'tokentx',
            'address': wallet, 'contractaddress': token,
            'sort': 'desc',
        }, max_pages=max_pages)
        self.cache.set(ck, rows)
        return rows

    def tokentx_for_token(self, token, max_pages=20, since_block=0):
        ck = f'es_tt_t_{token.lower()}_b{since_block}_p{max_pages}'
        cached = self.cache.get(ck, ttl=300)
        if cached is not None:
            return cached
        rows = self._paginate({
            'module': 'account', 'action': 'tokentx',
            'contractaddress': token,
            'startblock': since_block,
            'sort': 'desc',
        }, max_pages=max_pages)
        self.cache.set(ck, rows)
        return rows

    def tokentx_for_wallet(self, wallet, max_pages=10):
        ck = f'es_tt_w_{wallet.lower()}_p{max_pages}'
        cached = self.cache.get(ck, ttl=300)
        if cached is not None:
            return cached
        rows = self._paginate({
            'module': 'account', 'action': 'tokentx',
            'address': wallet,
            'sort': 'desc',
        }, max_pages=max_pages)
        self.cache.set(ck, rows)
        return rows

    def txlist(self, wallet, max_pages=2, sort='asc'):
        ck = f'es_txl_{wallet.lower()}_{sort}_p{max_pages}'
        cached = self.cache.get(ck, ttl=3600)
        if cached is not None:
            return cached
        rows = self._paginate({
            'module': 'account', 'action': 'txlist',
            'address': wallet, 'sort': sort,
        }, max_pages=max_pages)
        self.cache.set(ck, rows)
        return rows

    def txlistinternal(self, wallet, max_pages=5):
        ck = f'es_txli_{wallet.lower()}_p{max_pages}'
        cached = self.cache.get(ck, ttl=1800)
        if cached is not None:
            return cached
        rows = self._paginate({
            'module': 'account', 'action': 'txlistinternal',
            'address': wallet, 'sort': 'desc',
        }, max_pages=max_pages)
        self.cache.set(ck, rows)
        return rows

    def is_contract(self, addr):
        addr = addr.lower()
        ck = f'es_code_{addr}'
        cached = self.cache.get(ck, ttl=86400 * 30)
        if cached is not None:
            return cached
        j = self._get({'module': 'proxy', 'action': 'eth_getCode',
                       'address': addr, 'tag': 'latest'})
        code = j.get('result') if isinstance(j, dict) else ''
        is_c = isinstance(code, str) and code not in ('0x', '', '0x0')
        if isinstance(code, str):
            self.cache.set(ck, is_c)
        return is_c

    # ---- high-level: wallet ⨯ token totals -------------------------------
    def wallet_token_totals(self, wallet, token):
        """Exact lifetime ETH-in/ETH-out for (wallet, token), reconciled
        against WETH transfers + internal ETH legs in the same tx.

        Algorithm:
          1. Pull tokentx rows for (wallet, token).
          2. Pull WETH tokentx for the wallet — index by hash for O(1) lookup.
          3. Pull internal-tx list for the wallet — index by hash, summed.
          4. For each token row:
              - is_buy if to == wallet, else sell.
              - First try the WETH leg in the same hash (router unwraps to
                WETH for a buy, wraps to WETH for a sell).
              - Else try the internal-tx leg (raw ETH router, e.g. Uniswap V2).
              - If neither yields >0 ETH, skip (P2P transfer, not a swap).
          5. Aggregate.
        """
        wlow = wallet.lower()
        tlow = token.lower()

        token_rows = self.tokentx_for_address_token(wallet, token, max_pages=10)
        weth_rows = self.tokentx_for_address_token(wallet, WETH, max_pages=10)
        internal_rows = self.txlistinternal(wallet, max_pages=5)

        # Index WETH txs by hash, with a list per side
        weth_by_hash = collections.defaultdict(list)
        for r in weth_rows:
            h = (r.get('hash') or '').lower()
            if h:
                weth_by_hash[h].append(r)

        # Index internal txs by hash
        internal_by_hash = collections.defaultdict(list)
        for r in internal_rows:
            h = (r.get('hash') or '').lower()
            if h:
                internal_by_hash[h].append(r)

        eth_in = 0.0
        eth_out = 0.0
        n_buys = n_sells = 0
        buy_ts, sell_ts = [], []
        trades = []
        n_swaps = 0
        seen_hashes = set()

        for row in token_rows:
            h = (row.get('hash') or '').lower()
            if not h or h in seen_hashes:
                continue
            seen_hashes.add(h)
            to_addr = (row.get('to') or '').lower()
            from_addr = (row.get('from') or '').lower()
            if to_addr != wlow and from_addr != wlow:
                continue
            is_buy = (to_addr == wlow)
            try:
                ts = int(row.get('timeStamp') or 0)
            except Exception:
                ts = 0

            eth_amt = 0.0
            # ---- WETH leg ----
            for wr in weth_by_hash.get(h, []):
                wto = (wr.get('to') or '').lower()
                wfrom = (wr.get('from') or '').lower()
                # buy: wallet sent WETH (to router) OR received WETH unwrap.
                # Most common: wallet's WETH out (from==wallet) on a buy,
                # or WETH in (to==wallet) on a sell.
                try:
                    val = float(wr.get('value') or 0) / (10 ** 18)
                except Exception:
                    val = 0.0
                if is_buy and wfrom == wlow:
                    eth_amt += val
                elif (not is_buy) and wto == wlow:
                    eth_amt += val

            # ---- internal ETH leg (raw ETH router) ----
            if eth_amt <= 0:
                for ir in internal_by_hash.get(h, []):
                    ito = (ir.get('to') or '').lower()
                    ifrom = (ir.get('from') or '').lower()
                    try:
                        ival = float(ir.get('value') or 0) / (10 ** 18)
                    except Exception:
                        ival = 0.0
                    # buy: wallet sent ETH (from==wallet) — but txlistinternal
                    # only lists internal traces, so the typical match is
                    # wallet receives ETH on a sell (to==wallet).
                    if is_buy and ifrom == wlow:
                        eth_amt += ival
                    elif (not is_buy) and ito == wlow:
                        eth_amt += ival

            if eth_amt <= 0:
                # Skip — looks like a P2P / airdrop / non-swap movement.
                continue

            n_swaps += 1
            if is_buy:
                eth_in += eth_amt
                n_buys += 1
                buy_ts.append(ts)
            else:
                eth_out += eth_amt
                n_sells += 1
                sell_ts.append(ts)
            trades.append({'kind': 'buy' if is_buy else 'sell',
                           'ts': ts, 'eth': eth_amt, 'hash': h})

        return {
            'eth_in': eth_in, 'eth_out': eth_out,
            'n_buys': n_buys, 'n_sells': n_sells,
            'buy_ts': buy_ts, 'sell_ts': sell_ts,
            'trades': trades,
            'n_swaps': n_swaps,
        }

    # ---- wallet age, diversity, funding, deployed contracts --------------
    def wallet_age_days(self, wallet):
        """Days since wallet's first outbound tx (txlist asc page 1)."""
        wlow = wallet.lower()
        ck = f'es_age_{wlow}'
        cached = self.cache.get(ck, ttl=86400 * 7)
        if cached is not None:
            return cached
        j = self._get({'module': 'account', 'action': 'txlist',
                       'address': wallet, 'sort': 'asc',
                       'page': 1, 'offset': 1})
        rows = j.get('result') if isinstance(j, dict) else []
        if not isinstance(rows, list) or not rows:
            self.cache.set(ck, 0.0)
            return 0.0
        try:
            first_ts = int(rows[0].get('timeStamp') or 0)
        except Exception:
            first_ts = 0
        if first_ts <= 0:
            self.cache.set(ck, 0.0)
            return 0.0
        days = (time.time() - first_ts) / 86400.0
        self.cache.set(ck, days)
        return days

    def wallet_distinct_tokens(self, wallet, since_ts=None):
        """Number of distinct ERC-20s the wallet has touched, optionally since
        unix `since_ts`."""
        wlow = wallet.lower()
        ck = f'es_distinct_{wlow}_s{since_ts or 0}'
        cached = self.cache.get(ck, ttl=3600 * 6)
        if cached is not None:
            return cached
        rows = self.tokentx_for_wallet(wallet, max_pages=5)
        seen = set()
        for r in rows:
            try:
                ts = int(r.get('timeStamp') or 0)
            except Exception:
                ts = 0
            if since_ts and ts < since_ts:
                continue
            ca = (r.get('contractAddress') or '').lower()
            if ca:
                seen.add(ca)
        n = len(seen)
        self.cache.set(ck, n)
        return n

    def wallet_funding_source(self, wallet):
        """First incoming ETH or token tx → (funder_addr, label).
        Label is from the built-in CEX/mixer dict, else 'unknown'."""
        wlow = wallet.lower()
        ck = f'es_funder_{wlow}'
        cached = self.cache.get(ck, ttl=86400 * 7)
        if cached is not None:
            return tuple(cached) if isinstance(cached, list) else cached

        # Earliest incoming ETH (txlist asc) — scan up to 2 pages.
        funder = None
        eth_first_ts = None
        try:
            txs = self.txlist(wallet, max_pages=2, sort='asc')
        except Exception:
            txs = []
        for r in txs:
            to_addr = (r.get('to') or '').lower()
            try:
                val = float(r.get('value') or 0)
            except Exception:
                val = 0.0
            if to_addr == wlow and val > 0:
                funder = (r.get('from') or '').lower()
                try:
                    eth_first_ts = int(r.get('timeStamp') or 0)
                except Exception:
                    eth_first_ts = 0
                break

        # Earliest incoming token — also a valid funding signal (e.g. CEX
        # deposits as USDC, or airdrops).  Prefer whichever is older.
        try:
            tt = self.tokentx_for_wallet(wallet, max_pages=2)
        except Exception:
            tt = []
        # tokentx_for_wallet returns desc-sorted, walk back to oldest with to==wallet
        token_funder = None
        token_first_ts = None
        for r in reversed(tt):
            to_addr = (r.get('to') or '').lower()
            if to_addr == wlow:
                token_funder = (r.get('from') or '').lower()
                try:
                    token_first_ts = int(r.get('timeStamp') or 0)
                except Exception:
                    token_first_ts = 0
                break

        chosen = funder
        chosen_ts = eth_first_ts
        if token_first_ts is not None:
            if chosen_ts is None or (token_first_ts > 0 and token_first_ts < chosen_ts):
                chosen = token_funder
                chosen_ts = token_first_ts

        if not chosen:
            res = ('', 'unknown')
        else:
            label = CEX_LABELS.get(chosen, 'unknown')
            res = (chosen, label)
        self.cache.set(ck, list(res))
        return res

    def wallet_deployed_contracts(self, wallet):
        """List of contracts deployed by `wallet` (txlist rows where to=='' and
        contractAddress != '')."""
        wlow = wallet.lower()
        ck = f'es_deployed_{wlow}'
        cached = self.cache.get(ck, ttl=86400)
        if cached is not None:
            return cached
        try:
            txs = self.txlist(wallet, max_pages=5, sort='asc')
        except Exception:
            txs = []
        out = []
        for r in txs:
            to_addr = (r.get('to') or '').strip().lower()
            ca = (r.get('contractAddress') or '').strip().lower()
            if to_addr == '' and ca:
                out.append(ca)
        self.cache.set(ck, out)
        return out
