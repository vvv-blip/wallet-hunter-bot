"""Per-token safety + metadata enrichment for the signal bot.

Wraps three free data sources to fill out the WAKE-UP-CALL card fields:
  - honeypot.is /v2/IsHoneypot     → buy/sell tax, sellability, sniper failures,
                                     holder count, openSource flag
  - Etherscan v2 contract endpoints → contract creation age, ABI selectors,
                                       contract token balance (clog %)
  - GT pool/token attributes        → MC (FDV), LIQ (reserve), price change

Caches aggressively (honeypot 30 min, contract age forever-effectively).  All
errors degrade gracefully — every helper returns a partially-populated dict so
the formatter can still render with `?` for missing fields.
"""
import time, requests


# Standard ERC-20 + Uniswap-pair selectors.  Anything else in the ABI flagged as
# "unknown function" — useful tax-token / honeypot heuristic.
ERC20_STANDARD_SELECTORS = {
    'totalSupply', 'balanceOf', 'transfer', 'transferFrom', 'approve',
    'allowance', 'name', 'symbol', 'decimals',
    # ownable
    'owner', 'renounceOwnership', 'transferOwnership',
    # common safe extensions
    'increaseAllowance', 'decreaseAllowance',
    # UniswapV2Pair-style read methods
    'getReserves', 'token0', 'token1', 'price0CumulativeLast',
    'price1CumulativeLast', 'kLast', 'factory',
    # standard events show up as functions in some ABIs
    'Transfer', 'Approval',
    # constructor / fallback
    '', 'constructor', 'fallback', 'receive',
}


class SignalEnricher:
    """Safety + metadata enrichment for one token at a time."""

    HONEYPOT_BASE = 'https://api.honeypot.is/v2'

    def __init__(self, etherscan, cache):
        self.es = etherscan
        self.cache = cache

    # ------------------------------------------------------------------
    # honeypot.is
    # ------------------------------------------------------------------
    def honeypot(self, token, chain_id=1):
        """Tax + honeypot simulation.  Returns a dict with tax %, risk,
        isHoneypot, holders, sniper stats, openSource."""
        ck = f'hpt_{chain_id}_{token.lower()}'
        cached = self.cache.get(ck, ttl=1800)  # 30 min — taxes can change
        if cached is not None:
            return cached

        out = {
            'isHoneypot':   None,
            'sellable':     None,
            'buy_tax':      None,
            'sell_tax':     None,
            'transfer_tax': None,
            'risk':         None,
            'risk_level':   None,
            'flags':        [],
            'holders':      None,
            'snipers_failed':  None,
            'snipers_success': None,
            'siphoned':     None,
            'open_source':  None,
            'is_proxy':     None,
        }

        try:
            r = requests.get(
                f'{self.HONEYPOT_BASE}/IsHoneypot',
                params={'address': token, 'chainID': chain_id},
                timeout=10,
            )
            if r.status_code != 200:
                return out
            j = r.json() if r.content else {}
        except Exception:
            return out

        sim = j.get('simulationResult') or {}
        out['buy_tax']      = _safe_pct(sim.get('buyTax'))
        out['sell_tax']     = _safe_pct(sim.get('sellTax'))
        out['transfer_tax'] = _safe_pct(sim.get('transferTax'))
        out['sellable']     = bool(j.get('simulationSuccess'))
        out['isHoneypot']   = bool((j.get('honeypotResult') or {}).get('isHoneypot'))

        summary = j.get('summary') or {}
        out['risk']         = summary.get('risk')
        out['risk_level']   = summary.get('riskLevel')
        out['flags']        = list(summary.get('flags') or [])

        token_blob = j.get('token') or {}
        out['holders']      = _safe_int(token_blob.get('totalHolders'))

        ha = j.get('holderAnalysis') or {}
        out['snipers_failed']  = _safe_int(ha.get('snipersFailed'))
        out['snipers_success'] = _safe_int(ha.get('snipersSuccess'))
        out['siphoned']        = _safe_int(ha.get('siphoned'))

        cc = j.get('contractCode') or {}
        out['open_source'] = bool(cc.get('openSource'))
        out['is_proxy']    = bool(cc.get('isProxy'))

        self.cache.set(ck, out)
        return out

    # ------------------------------------------------------------------
    # Etherscan-based metadata
    # ------------------------------------------------------------------
    def contract_age_days(self, token):
        """Days since contract creation.  Uses contractcreation endpoint."""
        ck = f'hpt_age_{token.lower()}'
        cached = self.cache.get(ck, ttl=86400 * 30)
        if cached is not None:
            return float(cached)

        if not self.es or not getattr(self.es, 'key', ''):
            return 0.0

        # contractcreation returns the creation tx hash; we then look up its
        # timestamp via eth_getTransactionByHash.  Two calls but both cached.
        j = self.es._get({
            'module': 'contract', 'action': 'getcontractcreation',
            'contractaddresses': token,
        })
        rows = j.get('result') if isinstance(j, dict) else None
        if not isinstance(rows, list) or not rows:
            return 0.0
        tx_hash = rows[0].get('txHash') or ''
        if not tx_hash:
            return 0.0

        j2 = self.es._get({
            'module': 'proxy', 'action': 'eth_getTransactionByHash',
            'txhash': tx_hash,
        })
        tx = j2.get('result') if isinstance(j2, dict) else None
        if not isinstance(tx, dict):
            return 0.0
        block_hex = tx.get('blockNumber') or '0x0'
        try:
            block_num = int(block_hex, 16)
        except Exception:
            return 0.0

        j3 = self.es._get({
            'module': 'proxy', 'action': 'eth_getBlockByNumber',
            'tag': hex(block_num), 'boolean': 'false',
        })
        blk = j3.get('result') if isinstance(j3, dict) else None
        if not isinstance(blk, dict):
            return 0.0
        ts_hex = blk.get('timestamp') or '0x0'
        try:
            block_ts = int(ts_hex, 16)
        except Exception:
            return 0.0

        days = (time.time() - block_ts) / 86400.0
        if days > 0:
            self.cache.set(ck, days)
        return days

    def clog_pct(self, token):
        """Percent of total supply held by the contract address itself.
        Real "clog" — tokens stuck in the contract waiting to be auto-swapped.
        High values can signal pending sell pressure."""
        ck = f'hpt_clog_{token.lower()}'
        cached = self.cache.get(ck, ttl=1800)
        if cached is not None:
            return float(cached)

        if not self.es or not getattr(self.es, 'key', ''):
            return 0.0

        try:
            # totalSupply
            j_ts = self.es._get({
                'module': 'stats', 'action': 'tokensupply',
                'contractaddress': token,
            })
            total = float(j_ts.get('result') or 0) if isinstance(j_ts, dict) else 0.0
            # balanceOf(token)
            j_bo = self.es._get({
                'module': 'account', 'action': 'tokenbalance',
                'contractaddress': token, 'address': token, 'tag': 'latest',
            })
            bal = float(j_bo.get('result') or 0) if isinstance(j_bo, dict) else 0.0
        except Exception:
            return 0.0
        if total <= 0:
            return 0.0
        pct = (bal / total) * 100.0
        self.cache.set(ck, pct)
        return pct

    def unknown_functions(self, token):
        """Count of ABI selectors that aren't in the standard ERC-20 set.
        Used as a 'tax token' / 'has hooks' heuristic.  Higher = more custom
        logic = more rug surface area."""
        ck = f'hpt_unkfn_{token.lower()}'
        cached = self.cache.get(ck, ttl=86400 * 30)
        if cached is not None:
            return int(cached)

        if not self.es or not getattr(self.es, 'key', ''):
            return 0

        try:
            j = self.es._get({
                'module': 'contract', 'action': 'getabi',
                'address': token,
            })
        except Exception:
            return 0
        abi_str = j.get('result') if isinstance(j, dict) else ''
        if not isinstance(abi_str, str) or len(abi_str) < 5:
            return 0
        try:
            import json as _json
            abi = _json.loads(abi_str)
        except Exception:
            return 0
        unknown = 0
        for entry in abi:
            if not isinstance(entry, dict):
                continue
            if entry.get('type') != 'function':
                continue
            name = entry.get('name') or ''
            if name not in ERC20_STANDARD_SELECTORS:
                unknown += 1
        self.cache.set(ck, unknown)
        return unknown

    # ------------------------------------------------------------------
    # Bundle
    # ------------------------------------------------------------------
    def enrich(self, token):
        """One-shot enrichment of all metadata.  Returns merged dict."""
        out = {'token': token}
        out.update(self.honeypot(token))
        try: out['contract_age_days'] = self.contract_age_days(token)
        except Exception: out['contract_age_days'] = 0.0
        try: out['clog_pct'] = self.clog_pct(token)
        except Exception: out['clog_pct'] = 0.0
        try: out['unknown_functions'] = self.unknown_functions(token)
        except Exception: out['unknown_functions'] = 0
        return out


def _safe_pct(v):
    try:
        if v is None: return None
        return round(float(v), 2)
    except Exception:
        return None


def _safe_int(v):
    try:
        if v is None: return None
        return int(v)
    except Exception:
        return None
