import sys
import asyncio
import json
import os
import re
import csv
import logging
import argparse
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any

# --- LOGGING CONFIGURATION ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

try:
    import ccxt.async_support as ccxt
except ImportError:
    logger.critical("Dependency missing: ccxt. Run 'pip install ccxt'")
    sys.exit(1)

# --- SYSTEM CONFIGURATION ---
CONFIG = {
    "retention_policy": {'kraken': 0.5, 'binance': 3650, 'coinbase': 3650, 'okx': 180, 'mexc': 30},
    "default_retention_days": 90,
    "exchange_tiers": {
        'tier_1': ['coinbase', 'gemini', 'cryptocom', 'kraken'],
        'tier_2': ['binance', 'kucoin', 'okx', 'bybit', 'gate'],
        'tier_3': ['mexc', 'bitget', 'htx']
    },
    "concurrency_limit": 5,
    "request_timeout_ms": 25000,
    "cache_filename": 'pricing_protocol_cache.json'
}

class PricingProtocolEngine:
    """
    The Pricing Protocol: Core engine for high-fidelity historical 
    cryptocurrency price retrieval and audit verification.
    """
    def __init__(self):
        self.active_exchanges: Dict[str, ccxt.Exchange] = {}
        self.cache: Dict[str, Any] = self._init_cache()
        self.semaphore = asyncio.Semaphore(CONFIG["concurrency_limit"])
        self.lock = asyncio.Lock()

    def _init_cache(self) -> Dict:
        if os.path.exists(CONFIG["cache_filename"]):
            try:
                with open(CONFIG["cache_filename"], 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                logger.warning("Cache load failed. Starting fresh.")
        return {}

    def commit_cache(self):
        try:
            with open(CONFIG["cache_filename"], 'w') as f:
                json.dump(self.cache, f, indent=2)
        except IOError as e:
            logger.error(f"Failed to save cache: {e}")

    @staticmethod
    def normalize_ticker(raw_input: str) -> dict:
        if not raw_input: return {"normalized": "", "base": "", "quote": ""}
        s = str(raw_input).strip().upper()
        special_cases = {'XBT': 'BTC', 'XBTUSD': 'BTC/USD', 'LUNA': 'LUNC', 'MATIC': 'POL'}
        s = special_cases.get(s, s)
        match = re.search(r'[-_\s/]([A-Z]{3,5})$', s)
        if match:
            quote = match.group(1)
            base = re.split(r'[-_\s/]', s)[0]
        elif len(s) > 4 and (s.endswith('USD') or s.endswith('USDT')):
            quote = 'USDT' if s.endswith('USDT') else 'USD'
            base = s[:-len(quote)]
        else:
            base, quote = s, None
        normalized = f"{base}/{quote}" if (quote and '/' not in s) else s
        return {"normalized": normalized, "base": base, "quote": quote}

    @staticmethod
    def parse_dt(date_str: str) -> Optional[datetime]:
        fmts = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d-%m-%Y %H:%M:%S", "%d/%m/%Y"]
        for fmt in fmts:
            try:
                dt = datetime.strptime(str(date_str).strip(), fmt)
                if len(str(date_str)) <= 10:
                    dt = dt.replace(hour=23, minute=59, second=59)
                return dt.replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        return None

    async def get_exchange_client(self, exchange_id: str) -> Optional[ccxt.Exchange]:
        if exchange_id in self.active_exchanges:
            return self.active_exchanges[exchange_id]
        
        async with self.lock:
            if exchange_id in self.active_exchanges:
                return self.active_exchanges[exchange_id]
            
            if exchange_id not in ccxt.exchanges:
                return None
            
            client = getattr(ccxt, exchange_id)({
                'enableRateLimit': True, 
                'timeout': CONFIG["request_timeout_ms"]
            })
            
            try:
                await client.load_markets()
                self.active_exchanges[exchange_id] = client
                return client
            except Exception:
                logger.error(f"Connection failed for {exchange_id}. Cleaning up session...")
                # Fix for "Unclosed client session": Close the failing client immediately
                await client.close()
                return None

    async def execute_ohlcv_fetch(self, exchange_id: str, ticker_data: dict, target_dt: datetime):
        client = await self.get_exchange_client(exchange_id)
        if not client: return None

        pairs = [ticker_data['normalized']]
        if not ticker_data['quote'] or ticker_data['quote'] == 'USD':
            pairs.extend([f"{ticker_data['base']}/USDT", f"{ticker_data['base']}/USDC"])
        
        verified_pairs = [p for p in pairs if p in client.markets]
        if not verified_pairs: return None

        target_ts = int(target_dt.timestamp() * 1000)
        age_days = (datetime.now(timezone.utc) - target_dt).days
        resolutions = ['1m', '1h', '1d'] if age_days < CONFIG['retention_policy'].get(exchange_id, CONFIG['default_retention_days']) else ['1h', '1d']

        for pair in verified_pairs:
            for tf in resolutions:
                if client.timeframes and tf not in client.timeframes: continue
                try:
                    interval_ms = {'1m': 60000, '1h': 3600000, '1d': 86400000}.get(tf, 60000)
                    since_ts = target_ts - (target_ts % interval_ms)
                    data = await client.fetch_ohlcv(pair, tf, since=since_ts, limit=5)
                    if not data: continue
                    valid = [c for c in data if c[0] <= target_ts + 1000]
                    if not valid: continue
                    best = sorted(valid, key=lambda x: abs(x[0] - target_ts))[0]
                    return {
                        'close': best[4],
                        'avg': sum(best[1:5]) / 4,
                        'source': f"{exchange_id.upper()} ({tf})",
                        'found_time': datetime.fromtimestamp(best[0]/1000, tz=timezone.utc),
                        'pair': pair
                    }
                except Exception:
                    continue
        return None

    async def process_audit_entry(self, row: dict):
        asset = row.get('Crypto Token Name', '')
        timestamp_raw = row.get('DateTime', '')
        user_exchange = str(row.get('Exchange Name', '')).lower().strip()
        
        target_dt = self.parse_dt(timestamp_raw)
        if not asset or not target_dt: return None

        ticker = self.normalize_ticker(asset)
        cache_key = f"{ticker['normalized']}_{int(target_dt.timestamp())}_{user_exchange}"
        
        if cache_key in self.cache: return self.cache[cache_key]

        async with self.semaphore:
            match = None
            is_fallback = False
            
            # 1. Try Preferred Exchange
            if user_exchange and user_exchange not in ['nan', 'none', '']:
                match = await self.execute_ohlcv_fetch(user_exchange, ticker, target_dt)
            
            # 2. Tiered Fallback
            if not match:
                if user_exchange and user_exchange not in ['nan', 'none', '']:
                    is_fallback = True
                for tier in ['tier_1', 'tier_2', 'tier_3']:
                    jobs = [self.execute_ohlcv_fetch(eid, ticker, target_dt) for eid in CONFIG['exchange_tiers'][tier]]
                    tier_results = [r for r in await asyncio.gather(*jobs) if r]
                    if tier_results:
                        match = sorted(tier_results, key=lambda x: abs((x['found_time'] - target_dt).total_seconds()))[0]
                        break

            output = {
                'Fair Value (Avg)': 0, 'Data Quality': 'Not Found', 'Final Rate (Close)': 0,
                'Audit Source': 'N/A', 'Found Time': 'N/A', 'Time Delta (s)': 'N/A'
            }
            
            if match:
                drift = abs((match['found_time'] - target_dt).total_seconds())
                quality = "High" if drift < 3600 else "Medium" if drift < 14400 else "Low"
                if is_fallback: quality += " (Fallback)"
                
                output.update({
                    'Fair Value (Avg)': round(match['avg'], 8),
                    'Data Quality': quality,
                    'Final Rate (Close)': round(match['close'], 8),
                    'Audit Source': f"{match['source']} {match['pair']}",
                    'Found Time': match['found_time'].strftime('%Y-%m-%d %H:%M:%S'),
                    'Time Delta (s)': round(drift)
                })
            
            self.cache[cache_key] = output
            return output

    async def shutdown(self):
        for client in self.active_exchanges.values():
            await client.close()

async def start_protocol(input_path: str, output_path: str):
    if not os.path.exists(input_path):
        logger.error(f"Input path invalid: {input_path}")
        return

    engine = PricingProtocolEngine()
    with open(input_path, mode='r', encoding='utf-8-sig') as f:
        rows = list(csv.DictReader(f))

    logger.info(f"The Pricing Protocol: Processing {len(rows)} records...")
    tasks = [engine.process_audit_entry(row) for row in rows]
    results = await asyncio.gather(*tasks)
    
    audit_fields = ['Fair Value (Avg)', 'Data Quality', 'Final Rate (Close)', 'Audit Source', 'Found Time', 'Time Delta (s)']
    header = list(rows[0].keys()) + [h for h in audit_fields if h not in rows[0].keys()]
    
    with open(output_path, mode='w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for row, res in zip(rows, results):
            writer.writerow({**row, **(res or {})})

    engine.commit_cache()
    await engine.shutdown()
    logger.info(f"Execution complete. Output: {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="The Pricing Protocol")
    parser.add_argument("input", help="Source CSV file")
    parser.add_argument("--output", default="pricing_protocol_results.csv", help="Destination CSV path")
    args = parser.parse_args()
    try:
        asyncio.run(start_protocol(args.input, args.output))
    except KeyboardInterrupt:
        logger.info("Terminated.")
    except Exception as e:
        logger.exception(f"Fatal protocol error: {e}")
