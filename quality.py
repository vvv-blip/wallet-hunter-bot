"""Wallet-quality scoring for the Telegram wallet-hunter bot.

The bot finds wallets that traded a token in some time/PnL pattern.  But raw
PnL alone is a terrible signal: a wallet that bought $10 and sold $1000 might
be a sybil-farmed bot, a sniper, a Tornado-funded rug deployer, or a real
human with skill.  The downstream user wants the LAST one — actionable
human signal — and wants the first three filtered or at least flagged.

This module computes a single 0–100 quality score per wallet by composing
six sub-scores derived purely from on-chain behavior we can read for free
via Etherscan + GeckoTerminal:

  - age          : how long ago the wallet first transacted
  - diversity    : how many distinct ERC-20s it has touched
  - funding      : where its first deposit came from (CEX = good, mixer = bad)
  - rug_avoid    : whether it deploys contracts in a token-farmer pattern
  - bot_avoid    : MEV-builder usage, gas-price entropy, raw tx velocity
  - activity     : average holding time per token (sub-hour = bot, 30d+ = HODLer)

Composite weighting (defined in `WEIGHTS`) leans heaviest on bot_avoid + funding
because those have the highest correlation with "is this a real human".  The
final dict is cached for 6 hours per wallet so /profile commands are instant
on repeat lookups; first lookup of a fresh wallet should land in <5s with the
Etherscan layer's existing per-call cache + a single pass of bounded
pagination here.

Public surface is exactly one class — `WalletQualityScorer` — with a single
`.score(wallet)` method matching the doc-shape in the bot's spec.  Errors
never raise out: every sub-scorer wraps its work in try/except and returns
a neutral 50 with an "unable to score X" flag if something goes wrong.  When
the etherscan key is missing, `score()` returns a graceful-degrade dict so
the bot stays usable.
"""
import os, sys, time, math, collections


# Composite weights — bot/funding dominate because those most often
# determine whether the wallet is "a real person we want to follow".
WEIGHTS = {
    'age':       0.10,
    'diversity': 0.15,
    'funding':   0.20,
    'rug_avoid': 0.15,
    'bot_avoid': 0.25,
    'activity':  0.15,
}


# Known MEV builder + high-frequency arb-bot addresses.  A wallet that
# routes >10 txs through these in its recent history is almost certainly
# automated infrastructure, not a human trader.
MEV_BUILDERS = {
    '0xdafea492d9c6733ae3d56b7ed1adb60692c98bc5',  # Flashbots builder
    '0xa1c47b8d6b51c4d18497ed4a7e7d3a97b1c8e5b8',  # Flashbots
    '0x690b9a9e9aa1c9db991c7721a92d351db4fac990',  # rsync builder
    '0x1f9090aae28b8a3dceadf281b0f12828e676c326',  # bloXroute
    '0x95222290dd7278aa3ddd389cc1e1d165cc4bafe5',  # beaverbuild
    '0x000000000035b5e5ad9019092c665357240f594e',  # common high-freq MEV bot
}


SECONDS_PER_DAY = 86400.0


def _piecewise(x, points):
    """Piecewise-linear interpolation.

    `points` is a sorted list of (x_i, y_i).  Returns y clamped to the
    end-points outside [x_0, x_n].
    """
    if not points:
        return 0.0
    if x <= points[0][0]:
        return points[0][1]
    if x >= points[-1][0]:
        return points[-1][1]
    for i in range(len(points) - 1):
        x0, y0 = points[i]
        x1, y1 = points[i + 1]
        if x0 <= x <= x1:
            if x1 == x0:
                return y0
            return y0 + (y1 - y0) * (x - x0) / (x1 - x0)
    return points[-1][1]


def _shannon_entropy(values):
    """Shannon entropy in bits of a list of discrete values."""
    if not values:
        return 0.0
    counts = collections.Counter(values)
    total = float(len(values))
    h = 0.0
    for c in counts.values():
        p = c / total
        if p > 0:
            h -= p * math.log2(p)
    return h


class WalletQualityScorer:
    """Composite 0–100 quality score for an Ethereum wallet."""

    CACHE_TTL = 6 * 3600  # 6h

    def __init__(self, etherscan, gt, cache):
        self.es = etherscan
        self.gt = gt
        self.cache = cache

    # ---- public ----------------------------------------------------------
    def score(self, wallet):
        """Composite score dict.  Cached 6h per wallet."""
        wlow = (wallet or '').lower()
        if not wlow:
            return self._degraded(wlow, 'empty wallet')

        ck = f'wq_score_{wlow}'
        cached = self.cache.get(ck, ttl=self.CACHE_TTL)
        if cached is not None:
            return cached

        # Detect missing etherscan key — every es call returns {'_error':'no_key'}.
        # Quick probe: a no-cost call we can recognize as no_key.
        if not getattr(self.es, 'key', ''):
            return self._degraded(wlow, 'no_key')

        sub = {}
        flags = []
        meta = {}

        for name, fn in (('age',       self._score_age),
                         ('diversity', self._score_diversity),
                         ('funding',   self._score_funding),
                         ('rug_avoid', self._score_rug_avoid),
                         ('bot_avoid', self._score_bot_avoid),
                         ('activity',  self._score_activity)):
            t0 = time.time()
            try:
                s, fl, m = fn(wlow)
            except Exception as e:
                s, fl, m = 50.0, ['unable to score ' + name], {}
            dt = time.time() - t0
            if dt > 2.0:
                try:
                    sys.stderr.write('_score_%s took %.2fs\n' % (name, dt))
                except Exception:
                    pass
            sub[name] = float(s)
            flags.extend(fl or [])
            if isinstance(m, dict):
                meta.update(m)

        overall = 0.0
        for k, w in WEIGHTS.items():
            overall += sub.get(k, 50.0) * w
        overall = max(0.0, min(100.0, overall))

        rating = self._rating(overall)
        out = {
            'wallet':    wlow,
            'overall':   round(overall, 2),
            'rating':    rating,
            'subscores': {k: round(v, 2) for k, v in sub.items()},
            'flags':     flags,
            'meta':      meta,
        }
        self.cache.set(ck, out)
        return out

    # ---- helpers ---------------------------------------------------------
    def _rating(self, x):
        """Bucket overall score into a coarse rating label."""
        if x < 30:  return 'avoid'
        if x < 50:  return 'risky'
        if x < 65:  return 'normal'
        if x < 80:  return 'good'
        if x < 90:  return 'great'
        return 'elite'

    def _degraded(self, wallet, reason):
        """Graceful-degrade dict for missing key / empty input."""
        if reason == 'no_key':
            flag = '⚠️ etherscan key missing — quality scoring disabled'
        else:
            flag = '⚠️ ' + reason
        return {
            'wallet':    wallet,
            'overall':   50.0,
            'rating':    'unknown',
            'subscores': {k: 50.0 for k in WEIGHTS},
            'flags':     [flag],
            'meta':      {},
        }

    # =====================================================================
    # 1) AGE
    # =====================================================================
    def _score_age(self, wallet):
        """Score wallet age via piecewise curve.  Older = better."""
        try:
            age_days = float(self.es.wallet_age_days(wallet) or 0)
        except Exception:
            return 50.0, ['unable to score age'], {}

        # Approximate first_tx_ts from age for the meta block.
        first_tx_ts = int(time.time() - age_days * SECONDS_PER_DAY) if age_days > 0 else 0

        score = _piecewise(age_days, [
            (0,    0),
            (1,    10),
            (7,    30),
            (30,   60),
            (90,   80),
            (365,  95),
            (1000, 100),
        ])

        flags = []
        if age_days < 1:
            flags.append('🚨 brand-new wallet (<24h old)')
        elif age_days < 7:
            flags.append('⚠️ fresh wallet (<7 days old)')
        elif age_days > 365:
            flags.append('🏆 veteran wallet (1+ years)')

        return score, flags, {
            'age_days':    round(age_days, 2),
            'first_tx_ts': first_tx_ts,
        }

    # =====================================================================
    # 2) DIVERSITY
    # =====================================================================
    def _score_diversity(self, wallet):
        """Score breadth of distinct ERC-20s touched.  Wider = better."""
        try:
            since = int(time.time() - 90 * SECONDS_PER_DAY)
            distinct = int(self.es.wallet_distinct_tokens(wallet, since_ts=since) or 0)
        except Exception:
            return 50.0, ['unable to score diversity'], {}

        score = _piecewise(distinct, [
            (0,   0),
            (1,   5),
            (5,   30),
            (20,  60),
            (50,  85),
            (200, 100),
        ])

        flags = []
        if distinct < 3:
            flags.append('⚠️ trades very few tokens (likely sniper/sybil)')
        elif distinct > 50:
            flags.append('🎯 broad token portfolio')

        return score, flags, {'distinct_tokens_90d': distinct}

    # =====================================================================
    # 3) FUNDING
    # =====================================================================
    def _score_funding(self, wallet):
        """Score wallet funder.  CEX = trustworthy, Tornado = avoid."""
        try:
            funder, label = self.es.wallet_funding_source(wallet)
        except Exception:
            return 50.0, ['unable to score funding'], {}

        funder = (funder or '').lower()
        label = label or 'unknown'
        flags = []

        if label.startswith('Tornado'):
            score = 0.0
            flags.append('🚨 funded from Tornado Cash mixer')
        elif label == 'unknown' and funder:
            # Could be EOA-from-EOA (mid risk) or a contract (higher risk).
            try:
                is_c = bool(self.es.is_contract(funder))
            except Exception:
                is_c = False
            if is_c:
                score = 30.0
                flags.append('⚠️ funded from unknown contract')
            else:
                score = 50.0
                flags.append('funded from another wallet (unknown source)')
        elif label == 'unknown':
            # No funder found at all — wallet may have been bridged in or seeded
            # in genesis; treat as neutral-low rather than punishing.
            score = 50.0
            flags.append('funder not found')
        else:
            # Recognized label.  CEX hot wallets are the gold standard funder.
            score = 90.0
            flags.append(f'✅ funded from {label}')

        return score, flags, {
            'funding_addr':  funder,
            'funding_label': label,
        }

    # =====================================================================
    # 4) RUG AVOID
    # =====================================================================
    def _score_rug_avoid(self, wallet):
        """Score deployer-pattern risk.  Many deployments = farmer/rugger.

        TODO: detecting rug-tokens-currently-held would require pool-liquidity
        history per token (e.g. GT OHLCV with liquidity drops).  Out of scope
        for this iteration — we only flag the deployer-pattern signal here.
        """
        try:
            deployed = self.es.wallet_deployed_contracts(wallet) or []
            n = len(deployed)
        except Exception:
            return 50.0, ['unable to score rug_avoid'], {}

        # 0 deployed → 100; 5 → 30; 7 → 100-7*14=2 (clamped at 30 floor).
        # Use the formula straight: 100 - min(70, n*14), then floor at 30 if n>5.
        raw = 100 - min(70, n * 14)
        if n > 5:
            score = min(raw, 30.0)
        else:
            score = float(raw)

        flags = []
        if n > 5:
            flags.append(f'⚠️ has deployed {n} contracts (possible token deployer/farmer)')
        elif n == 0:
            # Don't add a flag for the common case — keeps output concise.
            pass

        return score, flags, {'deployed_contracts': n}

    # =====================================================================
    # 5) BOT AVOID
    # =====================================================================
    def _score_bot_avoid(self, wallet):
        """Score bot/MEV fingerprint via tx velocity, gas entropy, builder use."""
        try:
            txs = self.es.txlist(wallet, max_pages=1, sort='desc') or []
        except Exception:
            return 50.0, ['unable to score bot_avoid'], {}

        now = time.time()
        cutoff_30d = now - 30 * SECONDS_PER_DAY

        gas_buckets = []
        tx_count_30d = 0
        mev_builder_uses = 0

        for r in txs:
            try:
                ts = int(r.get('timeStamp') or 0)
            except Exception:
                ts = 0
            if ts >= cutoff_30d:
                tx_count_30d += 1

            # Bucket gasPrice into ~gwei integers (rounded to nearest gwei,
            # then mod-bucketed into ~20 buckets for the entropy calc).
            try:
                gp_wei = int(r.get('gasPrice') or 0)
            except Exception:
                gp_wei = 0
            if gp_wei > 0:
                gwei = gp_wei // 1_000_000_000  # nearest gwei (floor)
                # Bucket so bots that vary gas by 1 gwei still collapse together;
                # humans typically span dozens of gwei levels over time.
                bucket = gwei // 5  # 5-gwei buckets
                gas_buckets.append(bucket)

            to_addr = (r.get('to') or '').lower()
            if to_addr and to_addr in MEV_BUILDERS:
                mev_builder_uses += 1

        # Cap entropy sample to recent 2000 txs so a 10k-tx whale doesn't dilute
        # bot signal with 5-year-old casual usage.
        if len(gas_buckets) > 2000:
            gas_buckets = gas_buckets[:2000]
        gas_entropy = round(_shannon_entropy(gas_buckets), 3)

        flags = []

        # Decision tree — most-bot-like signals first.
        if tx_count_30d > 1000 and gas_entropy < 1.0:
            score = 10.0
            flags.append('🤖 high-volume bot signature')
        elif mev_builder_uses > 10:
            score = 20.0
            flags.append('🤖 frequent MEV-builder usage')
        elif gas_entropy < 1.5 and tx_count_30d > 100:
            score = 30.0
            flags.append('🤖 mechanical gas pattern')
        elif tx_count_30d > 1000:
            score = 50.0
            flags.append(f'🤖 high-volume wallet ({tx_count_30d} tx in 30d)')
        else:
            # Healthy human range: entropy 2–4 → 80–100 linearly.
            # Below 2.0 we drop down toward 60 so quiet-but-mechanical wallets
            # don't get a free pass.
            if gas_entropy >= 2.0:
                score = _piecewise(gas_entropy, [
                    (2.0, 80),
                    (3.0, 90),
                    (4.0, 100),
                    (6.0, 100),
                ])
            else:
                score = _piecewise(gas_entropy, [
                    (0.0, 60),
                    (1.5, 70),
                    (2.0, 80),
                ])

        return score, flags, {
            'tx_count_30d':     tx_count_30d,
            'mev_builder_uses': mev_builder_uses,
            'gas_entropy':      gas_entropy,
        }

    # =====================================================================
    # 6) ACTIVITY
    # =====================================================================
    def _score_activity(self, wallet):
        """Score health-of-trading via average per-token holding time."""
        try:
            tt = self.es.tokentx_for_wallet(wallet, max_pages=2) or []
        except Exception:
            return 50.0, ['unable to score activity'], {}

        wlow = wallet.lower()
        now = time.time()
        cutoff_30d = now - 30 * SECONDS_PER_DAY

        # Per-token, track first-buy ts (wallet receives) and last-sell ts
        # (wallet sends).  Hold = last_sell - first_buy.
        first_buy = {}
        last_sell = {}
        recent_tokens = set()
        rug_count = 0  # placeholder, see comment below

        for r in tt:
            ca = (r.get('contractAddress') or '').lower()
            if not ca:
                continue
            to_addr = (r.get('to') or '').lower()
            from_addr = (r.get('from') or '').lower()
            try:
                ts = int(r.get('timeStamp') or 0)
            except Exception:
                ts = 0
            if ts <= 0:
                continue

            if ts >= cutoff_30d:
                recent_tokens.add(ca)

            # Receiving = "buy" leg in our holding-time heuristic.
            if to_addr == wlow:
                prev = first_buy.get(ca)
                if prev is None or ts < prev:
                    first_buy[ca] = ts
            # Sending = "sell" leg.
            if from_addr == wlow:
                prev = last_sell.get(ca)
                if prev is None or ts > prev:
                    last_sell[ca] = ts

        # Rug-tokens-held count is left as 0 here — properly detecting rugs
        # would require pool-liquidity history per token (a token whose LP
        # got drained → its current holders are rug victims).  Out of scope
        # for this iteration; included in meta for forward compat.

        holds = []
        for ca, fb in first_buy.items():
            ls = last_sell.get(ca)
            if ls is None or ls <= fb:
                continue
            holds.append((ls - fb) / SECONDS_PER_DAY)

        if not holds:
            # No completed buy→sell cycles — wallet is either accumulating, or
            # only ever sold tokens it received as airdrops.  Treat as neutral.
            return 60.0, [], {
                'tokens_held_count': len(recent_tokens),
                'rug_count':         rug_count,
                'avg_hold_days':     0.0,
            }

        avg_hold_days = sum(holds) / len(holds)

        flags = []
        if avg_hold_days < (1.0 / 24.0):  # < 1 hour
            score = 0.0
            flags.append('🤖 sub-hour holds (bot/sniper)')
        elif avg_hold_days < 1.0:
            score = 30.0
        elif avg_hold_days < 7.0:
            score = 60.0
        elif avg_hold_days < 30.0:
            score = 85.0
        else:
            score = 95.0

        return score, flags, {
            'tokens_held_count': len(recent_tokens),
            'rug_count':         rug_count,
            'avg_hold_days':     round(avg_hold_days, 3),
        }
