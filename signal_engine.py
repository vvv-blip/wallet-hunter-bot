"""Signal detection engine for the wake-up-call bot.

Most signal bots are noise machines: they fire on *any* volume spike and let
users filter the rugs themselves.  This one tries to be the opposite —
catch the same spikes but only forward tokens with genuine signal underneath.

Per scan tick (default 60s):

  1) SEED candidates from GT trending_pools + new_pools  (~25 GT calls)
  2) VELOCITY pass — pool_trades x candidates, compute 1m/5m volume + buyer
     dispersion + net buy pressure + whale entries.  GT-only, fast.
  3) PREFILTER — drop candidates failing liquidity/volume/velocity floors
  4) SMART-MONEY pass on survivors — for each candidate, fetch top_traders
     for THIS token via GT and check whether the recent buyers overlap with
     the all-time top earners.  GT-only.
  5) FRESH-WALLET pass on survivors — sample up to 5 recent unique buyers,
     fetch wallet_age_days for each (Etherscan), compute fresh_wallet_pct.
  6) ENRICH survivors with safety data — honeypot.is + Etherscan contract
     metadata (taxes, clog, age, ABI selector count).
  7) SAFETY filter — drop honeypots, high-tax, deep-clog, closed-source.
  8) SCORE composite from 6 lenses (velocity, dispersion, smart-money,
     fresh-wallet, multi-timeframe, safety) + 0-100 score with tier badge.
  9) DEDUPE — never re-alert same token within `dedup_minutes`.

All heavy operations only run AFTER cheap filters drop obvious noise, so a
typical tick costs ~25 GT + ~15 Etherscan calls regardless of how busy the
market is.  Both layers are throttle-aware (GTSource: 25/min + 1.5s spacing,
EtherscanSource: 5 r/s leaky bucket).
"""
import time, logging, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed


log = logging.getLogger('signal_engine')


# Default thresholds — tuned against live PEPE/WIF-class data.  All numbers
# are USD unless stated.  Override per-instance via SignalEngine(thresholds=...)
DEFAULT_THRESHOLDS = {
    # ---- prefilter (cheap, GT-only) -----------------------------------
    'min_liq_usd':            5_000,    # ~1.5 ETH
    'min_vol_5m_usd':         3_000,
    'min_vol_1m_eth':         0.3,
    'min_unique_buyers_5m':   4,
    'velocity_ratio':         2.5,      # 1m vol >= 2.5x avg per minute over 5m
    # ---- safety gate (post-enrich) ------------------------------------
    'max_buy_tax_pct':        12.0,
    'max_sell_tax_pct':       12.0,
    'max_clog_pct':           5.0,
    'max_mc_liq_ratio':       40.0,
    'max_unknown_functions':  25,
    # ---- score thresholds + tiers -------------------------------------
    'tier_elite':             85,
    'tier_hot':               70,
    'tier_notable':           55,
    'min_score_to_alert':     55,
    'dedup_minutes':          240,
}


class SignalEngine:

    def __init__(self, gt, enricher, etherscan, discovery, cache,
                 thresholds=None):
        self.gt = gt
        self.enricher = enricher
        self.es = etherscan        # may be None — graceful degrade
        self.disc = discovery      # for top_traders_by_pnl smart-money lookup
        self.cache = cache
        self.thr = dict(DEFAULT_THRESHOLDS)
        if thresholds:
            self.thr.update(thresholds)

    # ──────────────────────────────────────────────────────────────────
    # public — one scan tick
    # ──────────────────────────────────────────────────────────────────
    def scan(self, network='eth', max_signals=5):
        t0 = time.time()
        candidates = self._seed_candidates(network=network)
        log.info("seeded %d candidate pools", len(candidates))

        # 2) Velocity pass (GT, parallel)
        rows = self._parallel(
            candidates, self._velocity_for_pool, workers=4, label='velocity')

        # 3) Prefilter
        survivors = [r for r in rows if r and self._prefilter(r)]
        log.info("prefilter survivors: %d", len(survivors))

        # 3b) Drop dedup hits BEFORE expensive enrichment
        survivors = [r for r in survivors if not self._is_recently_alerted(r['token'])]
        if not survivors:
            log.info("no fresh survivors after dedup")
            return []

        # Cap survivors by raw velocity to bound enrichment cost
        survivors.sort(key=lambda r: -r['velocity_ratio'])
        survivors = survivors[:8]

        # 4) Smart-money overlap (GT, parallel)
        self._parallel(
            survivors, self._smart_money_for_signal,
            workers=3, label='smart-money')

        # 5) Fresh-wallet pass (Etherscan, sequential — cheap per call)
        for r in survivors:
            try:
                self._fresh_wallet_for_signal(r)
            except Exception:
                log.exception("fresh_wallet error for %s", r['token'])

        # 6) Enrichment (honeypot.is + Etherscan contract metadata)
        for r in survivors:
            try:
                r['enrich'] = self.enricher.enrich(r['token'])
            except Exception:
                log.exception("enrich error for %s", r['token'])
                r['enrich'] = {}

        # 7) Safety filter
        safe = [r for r in survivors if self._safety_filter(r)]
        log.info("safety survivors: %d/%d", len(safe), len(survivors))

        # 8) Composite scoring + tier badge + min-score cutoff
        for r in safe:
            r['score'] = self._composite_score(r)
            r['tier']  = self._tier(r['score'])
            r['lenses'] = self._lens_breakdown(r)
        safe = [r for r in safe if r['score'] >= self.thr['min_score_to_alert']]
        safe.sort(key=lambda r: -r['score'])
        winners = safe[:max_signals]

        # 9) Mark dedup
        for r in winners:
            self._mark_alerted(r['token'])

        log.info("scan done in %.2fs — %d alerts", time.time() - t0, len(winners))
        return winners

    # ──────────────────────────────────────────────────────────────────
    # parallel helper
    # ──────────────────────────────────────────────────────────────────
    def _parallel(self, items, fn, workers=4, label=''):
        out = []
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(fn, x): x for x in items}
            for fut in as_completed(futs):
                try:
                    res = fut.result()
                except Exception as e:
                    log.exception("%s failed: %s", label, e)
                    continue
                if res:
                    out.append(res)
        return out

    # ──────────────────────────────────────────────────────────────────
    # candidate seeding
    # ──────────────────────────────────────────────────────────────────
    def _seed_candidates(self, network='eth'):
        try:
            trending = self.gt.trending_pools(network=network, n=20)
        except Exception:
            log.exception("trending fetch failed")
            trending = []
        try:
            newp = self._gt_new_pools(network=network, n=20)
        except Exception:
            log.exception("new_pools fetch failed")
            newp = []
        seen = set()
        out = []
        for src, rows in (('trending', trending), ('new', newp)):
            for r in rows:
                pa = r.get('pool_addr')
                if not pa or pa in seen:
                    continue
                seen.add(pa)
                r['source'] = src
                out.append(r)
        return out

    def _gt_new_pools(self, network='eth', n=20, ttl=120):
        ck = f'gt_newpools_{network}_n{n}'
        cached = self.cache.get(ck, ttl=ttl)
        if cached is not None:
            return cached
        out = []
        page = 1
        while len(out) < n and page <= 3:
            j = self.gt._req(f'/networks/{network}/new_pools',
                             params={'page': page})
            data = (j.get('data') or []) if isinstance(j, dict) else []
            if not data:
                break
            for row in data:
                attr = row.get('attributes') or {}
                rel = row.get('relationships') or {}
                base = ((rel.get('base_token') or {}).get('data') or {})
                bid = base.get('id') or ''
                token_addr = bid.split('_', 1)[-1].lower()
                if not token_addr.startswith('0x'):
                    continue
                pct = attr.get('price_change_percentage') or {}
                vol = attr.get('volume_usd') or {}
                txc = (attr.get('transactions') or {}).get('h1') or {}
                name = attr.get('name') or ''
                symbol = name.split('/')[0].strip() if '/' in name else name
                out.append({
                    'pool_addr':        (attr.get('address') or '').lower(),
                    'token_addr':       token_addr,
                    'name':             name,
                    'symbol':           symbol[:24],
                    'price_usd':        float(attr.get('base_token_price_usd') or 0),
                    'pct_1h':           float(pct.get('h1') or 0),
                    'pct_6h':           float(pct.get('h6') or 0),
                    'pct_24h':          float(pct.get('h24') or 0),
                    'volume_24h_usd':   float(vol.get('h24') or 0),
                    'fdv_usd':          float(attr.get('fdv_usd') or 0),
                    'reserve_usd':      float(attr.get('reserve_in_usd') or 0),
                    'pool_created_at':  attr.get('pool_created_at') or '',
                    'buys_h1':          int(txc.get('buys') or 0),
                    'buyers_h1':        int(txc.get('buyers') or 0),
                })
                if len(out) >= n:
                    break
            page += 1
        if out:
            self.cache.set(ck, out)
        return out

    # ──────────────────────────────────────────────────────────────────
    # 2) velocity per pool (GT-only)
    # ──────────────────────────────────────────────────────────────────
    def _velocity_for_pool(self, cand):
        pool = cand['pool_addr']
        try:
            trades = self.gt.pool_trades(pool)
        except Exception:
            trades = []
        now = time.time()
        win_1m = now - 60
        win_5m = now - 300

        vol_1m_eth = vol_5m_eth = 0.0
        vol_1m_usd = vol_5m_usd = 0.0
        buy_eth_5m = sell_eth_5m = 0.0
        n_buys_5m = n_sells_5m = n_buys_1m = 0
        unique_buyers_5m = []   # list (preserve order, dedupe later) for sampling
        seen_buyers = set()
        whale_count_5m = 0      # buys >= 1 ETH

        for t in trades:
            ts = t.get('ts') or 0
            if ts < win_5m:
                continue
            kind = t.get('kind') or ''
            eth = float(t.get('eth') or 0.0)
            usd = float(t.get('usd') or 0.0)
            w = (t.get('wallet') or '').lower()
            if ts >= win_5m:
                vol_5m_eth += eth
                vol_5m_usd += usd
                if kind == 'buy':
                    n_buys_5m += 1
                    buy_eth_5m += eth
                    if w and w not in seen_buyers:
                        seen_buyers.add(w)
                        unique_buyers_5m.append(w)
                    if eth >= 1.0:
                        whale_count_5m += 1
                elif kind == 'sell':
                    n_sells_5m += 1
                    sell_eth_5m += eth
            if ts >= win_1m:
                vol_1m_eth += eth
                vol_1m_usd += usd
                if kind == 'buy':
                    n_buys_1m += 1

        avg_per_min_5m = (vol_5m_eth / 5.0) if vol_5m_eth > 0 else 0.0
        velocity_ratio = (vol_1m_eth / avg_per_min_5m) if avg_per_min_5m > 0 else 0.0

        # Net buy pressure ∈ [-1, +1].  Positive = buys outweigh sells in ETH.
        denom = max(buy_eth_5m, sell_eth_5m)
        net_buy_pressure = ((buy_eth_5m - sell_eth_5m) / denom) if denom > 0 else 0.0

        return {
            'token':            cand['token_addr'],
            'pool':             pool,
            'symbol':           cand.get('symbol') or '',
            'name':             cand.get('name') or '',
            'source':           cand.get('source') or '',
            'fdv_usd':          cand.get('fdv_usd') or 0.0,
            'reserve_usd':      cand.get('reserve_usd') or 0.0,
            'price_usd':        cand.get('price_usd') or 0.0,
            'pct_1h':           cand.get('pct_1h') or 0.0,
            'pct_6h':           cand.get('pct_6h') or 0.0,
            'pct_24h':          cand.get('pct_24h') or 0.0,
            'pool_created_at':  cand.get('pool_created_at') or '',
            'pool_age_hours':   _pool_age_hours(cand.get('pool_created_at')),
            'vol_1m_eth':       round(vol_1m_eth, 4),
            'vol_5m_eth':       round(vol_5m_eth, 4),
            'vol_1m_usd':       round(vol_1m_usd, 0),
            'vol_5m_usd':       round(vol_5m_usd, 0),
            'n_buys_5m':        n_buys_5m,
            'n_sells_5m':       n_sells_5m,
            'n_buys_1m':        n_buys_1m,
            'unique_buyers_5m_list': unique_buyers_5m,    # full list for later
            'unique_buyers_5m': len(unique_buyers_5m),
            'whale_count_5m':   whale_count_5m,
            'buy_eth_5m':       round(buy_eth_5m, 4),
            'sell_eth_5m':      round(sell_eth_5m, 4),
            'net_buy_pressure': round(net_buy_pressure, 3),
            'velocity_ratio':   round(velocity_ratio, 2),
        }

    # ──────────────────────────────────────────────────────────────────
    # 4) smart-money overlap (per-token, GT-only)
    # ──────────────────────────────────────────────────────────────────
    def _smart_money_for_signal(self, r):
        """For this token, fetch top-PnL traders and check how many of the
        recent unique buyers are in that set.

        High overlap = same wallets that historically made money on this
        token are buying again — strong "they know something" signal.
        """
        token = r['token']
        recent = set(r.get('unique_buyers_5m_list') or [])
        smart_overlap = 0
        smart_overlap_wallets = []

        if not recent or not self.disc:
            r['smart_money_overlap']   = 0
            r['smart_money_wallets']   = []
            r['smart_money_pct']       = 0.0
            return r

        try:
            top = self.disc.top_traders_by_pnl(token, top_n=20)
        except Exception:
            top = []
        top_set = set((t.get('wallet') or '').lower() for t in top)
        for w in recent:
            if w in top_set:
                smart_overlap += 1
                smart_overlap_wallets.append(w)

        r['smart_money_overlap'] = smart_overlap
        r['smart_money_wallets'] = smart_overlap_wallets[:5]
        r['smart_money_pct']     = round(
            100.0 * smart_overlap / max(1, len(recent)), 1)
        return r

    # ──────────────────────────────────────────────────────────────────
    # 5) fresh-wallet pass (Etherscan)
    # ──────────────────────────────────────────────────────────────────
    def _fresh_wallet_for_signal(self, r, sample_size=5, fresh_max_days=7.0):
        """Sample up to N recent unique buyers and check their wallet ages.

        fresh_wallet_pct = % of sample with age < fresh_max_days.
        Mid range (30-60%) = healthy mix of new + experienced.
        Very high (>80%) = sybil-suspect.
        Very low (<10%) = only veterans buying — often quiet accumulation.
        """
        recent = list(r.get('unique_buyers_5m_list') or [])[:sample_size]
        if not recent or not self.es:
            r['fresh_wallet_count']  = 0
            r['fresh_wallet_pct']    = 0.0
            r['avg_buyer_age_days']  = 0.0
            r['veteran_count']       = 0
            return r

        ages = []
        for w in recent:
            try:
                age = self.es.wallet_age_days(w)
            except Exception:
                age = 0.0
            ages.append(age or 0.0)

        fresh = sum(1 for a in ages if 0 < a < fresh_max_days)
        veteran = sum(1 for a in ages if a > 90)
        valid_ages = [a for a in ages if a > 0]

        r['fresh_wallet_count']  = fresh
        r['fresh_wallet_pct']    = round(100.0 * fresh / max(1, len(ages)), 1)
        r['veteran_count']       = veteran
        r['avg_buyer_age_days']  = round(
            sum(valid_ages) / max(1, len(valid_ages)), 1)
        return r

    # ──────────────────────────────────────────────────────────────────
    # filters
    # ──────────────────────────────────────────────────────────────────
    def _prefilter(self, r):
        if r['reserve_usd'] < self.thr['min_liq_usd']:
            return False
        if r['vol_5m_usd'] < self.thr['min_vol_5m_usd']:
            return False
        if r['vol_1m_eth'] < self.thr['min_vol_1m_eth']:
            return False
        if r['unique_buyers_5m'] < self.thr['min_unique_buyers_5m']:
            return False
        if r['velocity_ratio'] < self.thr['velocity_ratio']:
            return False
        # Net negative pressure = sells beating buys.  Don't alert on dumps.
        if r['net_buy_pressure'] < -0.2:
            return False
        return True

    def _safety_filter(self, r):
        e = r.get('enrich') or {}
        if e.get('isHoneypot') is True:
            return False
        if e.get('sellable') is False:
            return False
        bt = e.get('buy_tax') or 0.0
        st = e.get('sell_tax') or 0.0
        if bt > self.thr['max_buy_tax_pct'] or st > self.thr['max_sell_tax_pct']:
            return False
        if (e.get('clog_pct') or 0.0) > self.thr['max_clog_pct']:
            return False
        if r['reserve_usd'] > 0 and r['fdv_usd'] > 0:
            mcliq = r['fdv_usd'] / r['reserve_usd']
            r['mc_liq_ratio'] = round(mcliq, 2)
            if mcliq > self.thr['max_mc_liq_ratio']:
                return False
        if (e.get('unknown_functions') or 0) > self.thr['max_unknown_functions']:
            return False
        if e.get('open_source') is False:
            return False
        # Hard reject if honeypot.is reports any siphoning
        if (e.get('siphoned') or 0) > 0:
            return False
        return True

    # ──────────────────────────────────────────────────────────────────
    # 8) composite score — six lenses
    # ──────────────────────────────────────────────────────────────────
    def _composite_score(self, r):
        """Combine six independent signal lenses into 0-100.

        Each lens returns 0-X points; total is summed and clamped.

          velocity:      0-25  raw spike
          dispersion:    0-15  unique buyers in 5m
          pressure:      0-10  net buy ETH pressure
          momentum:      0-10  multi-timeframe alignment
          fresh:         0-10  fresh-wallet sweet spot (20-60%)
          smart_money:   0-15  recent buyer overlap with top earners
          whale:         0-5   whale (>=1 ETH) buy count
          freshness:     0-5   pool age <24h with sustained activity
          safety_bonus:  0-5   clean tax + clog + risk profile

        Total range: 0-100.
        """
        e = r.get('enrich') or {}

        # 1) velocity (max 25)
        vel = min(25.0, r['velocity_ratio'] * 5.0)

        # 2) dispersion (max 15)
        disp = min(15.0, r['unique_buyers_5m'] * 1.0)

        # 3) net buy pressure (max 10).  pressure ∈ [-1, +1] → 0-10
        pressure = max(0.0, min(10.0, (r['net_buy_pressure'] + 0.5) * 10.0))

        # 4) multi-timeframe alignment (max 10).  Reward h1+h6+h24 all positive
        aligned = sum(1 for k in ('pct_1h', 'pct_6h', 'pct_24h')
                      if (r.get(k) or 0) > 0)
        momentum = aligned * 3.0
        if aligned == 3 and (r.get('pct_24h') or 0) > 5:
            momentum += 1.0

        # 5) fresh-wallet sweet spot (max 10).  20-60% pct → full credit.
        fwp = r.get('fresh_wallet_pct') or 0.0
        if fwp == 0:
            fresh_score = 0.0     # likely no signal collected
        elif 20.0 <= fwp <= 60.0:
            fresh_score = 10.0
        elif fwp < 20.0:
            fresh_score = 4.0     # quiet veteran accumulation — still ok
        elif fwp <= 80.0:
            fresh_score = 5.0     # leaning hot but not sybil-suspect yet
        else:
            fresh_score = 0.0     # 80%+ fresh = sybil farm warning

        # 6) smart-money overlap (max 15)
        sm_count = r.get('smart_money_overlap') or 0
        sm = min(15.0, sm_count * 5.0)

        # 7) whale count (max 5)
        whale = min(5.0, (r.get('whale_count_5m') or 0) * 2.0)

        # 8) pool freshness (max 5)
        ph = r.get('pool_age_hours') or 0
        freshness = 5.0 if (0 < ph < 24 and r['vol_5m_usd'] >= 5000) else 0.0

        # 9) safety bonus (max 5)
        safety = 0.0
        bt = e.get('buy_tax') or 0.0
        st = e.get('sell_tax') or 0.0
        if bt + st <= 5.0:    safety += 2.0
        if (e.get('clog_pct') or 0.0) <= 1.0: safety += 1.0
        if (e.get('snipers_failed') or 0) == 0: safety += 1.0
        if (e.get('risk_level') or 99) <= 1: safety += 1.0

        total = (vel + disp + pressure + momentum + fresh_score
                 + sm + whale + freshness + safety)
        return round(min(100.0, max(0.0, total)), 1)

    def _lens_breakdown(self, r):
        """Return per-lens scores for the formatter to render — preserves
        debuggability without forcing the formatter to redo the math."""
        e = r.get('enrich') or {}
        return {
            'velocity':    round(min(25.0, r['velocity_ratio'] * 5.0), 1),
            'dispersion':  round(min(15.0, r['unique_buyers_5m'] * 1.0), 1),
            'smart_money': round(min(15.0, (r.get('smart_money_overlap') or 0) * 5.0), 1),
            'fresh':       r.get('fresh_wallet_pct') or 0.0,
            'pressure':    r.get('net_buy_pressure') or 0.0,
            'whales':      r.get('whale_count_5m') or 0,
            'pool_h':      r.get('pool_age_hours') or 0,
        }

    def _tier(self, score):
        if score >= self.thr['tier_elite']:    return '🔥🔥 ELITE'
        if score >= self.thr['tier_hot']:      return '🔥 HOT'
        if score >= self.thr['tier_notable']:  return '⚡ NOTABLE'
        return ''

    # ──────────────────────────────────────────────────────────────────
    # dedup
    # ──────────────────────────────────────────────────────────────────
    def _is_recently_alerted(self, token):
        ck = f'sig_alert_{token.lower()}'
        return self.cache.get(ck, ttl=self.thr['dedup_minutes'] * 60) is not None

    def _mark_alerted(self, token):
        ck = f'sig_alert_{token.lower()}'
        self.cache.set(ck, {'ts': int(time.time())})


# ──────────────────────────────────────────────────────────────────────
# helpers
# ──────────────────────────────────────────────────────────────────────
def _pool_age_hours(iso_str):
    """Parse GT's pool_created_at like '2026-04-26T13:46:11Z' → hours since."""
    if not iso_str:
        return 0
    try:
        # Accept ...Z by replacing with +00:00 for fromisoformat
        s = iso_str.replace('Z', '+00:00')
        dt = datetime.datetime.fromisoformat(s)
        ts = dt.timestamp()
        return (time.time() - ts) / 3600.0
    except Exception:
        return 0
