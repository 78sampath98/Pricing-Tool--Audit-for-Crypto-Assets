import sys
import asyncio
import json
import os
import re
import csv
from datetime import datetime, timezone, timedelta

# --- DEPENDENCY CHECK ---
try:
    import ccxt.async_support as ccxt
except ImportError:
    print("!" * 60 + "\n CRITICAL: CCXT LIBRARY MISSING. Run: pip install ccxt\n" + "!" * 60)
    sys.exit(1)

# ==========================================
# SYSTEM CONFIGURATION
# ==========================================

RETENTION_POLICIES = {'kraken': 0.5, 'binance': 3650, 'coinbase': 3650, 'okx': 180, 'mexc': 30}
DEFAULT_RETENTION = 90

EXCHANGE_TIERS = {
    '1_AUDIT_GRADE': ['coinbase', 'gemini', 'cryptocom', 'bitstamp', 'kraken'],
    '2_HIGH_VOLUME': ['binance', 'kucoin', 'okx', 'bybit', 'gate'],
    '3_SPECIALIST':  ['mexc', 'bitget', 'htx']
}

MAX_ROW_CONCURRENCY = 5
MAX_RETRIES = 2
CONNECTION_TIMEOUT = 25000
CACHE_FILE = 'saga_full_audit_cache.json'

# ==========================================
# CORE UTILITIES
# ==========================================

def analyze_user_input(raw_input):
    if not raw_input: return "", "", False, None
    s = str(raw_input).strip().upper()
    SPECIAL_MAP = {'XBT': 'BTC', 'XBTUSD': 'BTC/USD', 'LUNA': 'LUNC', 'MATIC': 'POL'}
    if s in SPECIAL_MAP: s = SPECIAL_MAP[s]

    has_quote = False
    clean_base = s
    target_quote = None

    match = re.search(r'[-_\s/]([A-Z]{3,5})$', s)
    if match:
        has_quote, target_quote = True, match.group(1)
        clean_base = re.split(r'[-_\s/]', s)[0]
    elif len(s) > 4 and (s.endswith('USD') or s.endswith('USDT')):
        has_quote = True
        target_quote = 'USDT' if s.endswith('USDT') else 'USD'
        clean_base = s[:-len(target_quote)]

    normalized = f"{clean_base}/{target_quote}" if (has_quote and '/' not in s) else s
    return normalized, clean_base, has_quote, target_quote

def parse_date_local(date_str):
    if not date_str: return None
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d-%m-%Y %H:%M:%S", "%d/%m/%Y"]:
        try:
            dt = datetime.strptime(str(date_str).strip(), fmt)
            if " " not in str(date_str) and ":" not in str(date_str):
                dt = dt.replace(hour=23, minute=59, second=59)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError: continue
    return None

# ==========================================
# FETCH & TIERED HUNTING ENGINE
# ==========================================

async def get_exchange_instance(exch_id, loaded_exchanges, lock):
    if exch_id in loaded_exchanges: return loaded_exchanges[exch_id]
    async with lock:
        if exch_id in loaded_exchanges: return loaded_exchanges[exch_id]
        if exch_id in ccxt.exchanges:
            try:
                ex = getattr(ccxt, exch_id)({'enableRateLimit': True, 'timeout': CONNECTION_TIMEOUT})
                await ex.load_markets()
                loaded_exchanges[exch_id] = ex
                return ex
            except: return None
    return None

async def fetch_from_exchange(exchange, normalized_symbol, clean_base, has_quote, target_quote, timestamp_ms, target_dt_utc):
    symbols_to_check = [normalized_symbol] if normalized_symbol in exchange.markets else []
    if target_quote == 'USD' or not has_quote:
        for alt in [f"{clean_base}/USDT", f"{clean_base}/USDC", f"{clean_base}/USD"]:
            if alt in exchange.markets and alt not in symbols_to_check: symbols_to_check.append(alt)
    
    if not symbols_to_check: return None

    age_days = (datetime.now(timezone.utc) - target_dt_utc).days
    active_ladder = ['1m', '1h', '1d'] if age_days < RETENTION_POLICIES.get(exchange.id, DEFAULT_RETENTION) else ['1h', '1d']

    for sym in symbols_to_check:
        for tf in active_ladder:
            if exchange.timeframes and tf not in exchange.timeframes: continue
            div = {'1m': 60000, '1h': 3600000, '1d': 86400000}.get(tf, 60000)
            snapped = timestamp_ms - (timestamp_ms % div)
            try:
                ohlcv = await exchange.fetch_ohlcv(sym, tf, since=snapped, limit=10)
                if not ohlcv: continue
                valid_candles = [c for c in ohlcv if c[5] > 0 and datetime.fromtimestamp(c[0]/1000, tz=timezone.utc) <= target_dt_utc + timedelta(seconds=1)]
                if not valid_candles: continue
                
                best = sorted(valid_candles, key=lambda c: abs(c[0] - target_dt_utc.timestamp()*1000))[0]
                c_dt = datetime.fromtimestamp(best[0]/1000, tz=timezone.utc)
                return {
                    'avg_price': (best[1]+best[2]+best[3]+best[4])/4,
                    'close_price': best[4], 'timeframe': tf, 'symbol': sym,
                    'found_dt': c_dt, 'diff_seconds': abs((c_dt - target_dt_utc).total_seconds()),
                    'exchange': exchange.id
                }
            except: continue
    return None

async def process_task(task_key, data, loaded_exchanges, semaphore, cache, factory_lock):
    if task_key in cache: return cache[task_key]
    async with semaphore:
        norm_sym, clean_base, has_quote, target_quote = analyze_user_input(data['symbol'])
        primary_result, is_fallback = None, False
        
        # User Choice First
        if data['exch_name'] and data['exch_name'].lower() not in ['nan', '', 'none']:
            ex = await get_exchange_instance(data['exch_name'].lower(), loaded_exchanges, factory_lock)
            if ex: primary_result = await fetch_from_exchange(ex, norm_sym, clean_base, has_quote, target_quote, data['start_ms'], data['dt_utc'])
        
        # Tiered Fallback
        if not primary_result:
            is_fallback = True if data['exch_name'] else False
            for tier_name in ['1_AUDIT_GRADE', '2_HIGH_VOLUME', '3_SPECIALIST']:
                tasks = [fetch_from_exchange(await get_exchange_instance(eid, loaded_exchanges, factory_lock), norm_sym, clean_base, has_quote, target_quote, data['start_ms'], data['dt_utc']) 
                         for eid in EXCHANGE_TIERS[tier_name] if (await get_exchange_instance(eid, loaded_exchanges, factory_lock))]
                results = [r for r in await asyncio.gather(*tasks) if r]
                if results:
                    primary_result = sorted(results, key=lambda x: x['diff_seconds'])[0]
                    break

        # Formatting Output
        output = {'Fair Value (Avg)': 0, 'Data Quality': 'Not Found', 'Final Rate (Close)': 0, 'Source Info': '', 'Found Time': '', 'Time Delta (s)': ''}
        if primary_result:
            diff = primary_result['diff_seconds']
            q = "High" if diff < 3600 else ("Medium" if diff < 14400 else "Low")
            if is_fallback: q += " (Fallback)"
            output.update({
                'Fair Value (Avg)': primary_result['avg_price'], 'Data Quality': q,
                'Final Rate (Close)': primary_result['close_price'],
                'Source Info': f"{primary_result['exchange'].upper()} {primary_result['timeframe']} {primary_result['symbol']}",
                'Found Time': primary_result['found_dt'].strftime('%Y-%m-%d %H:%M:%S'),
                'Time Delta (s)': round(diff)
            })
        cache[task_key] = output
        return output

async def main():
    print("\n" + "="*50 + "\n SAGA PROTOCOL: MARK XXVII (FULL AUDIT)\n" + "="*50)
    file_path = input("Enter Input CSV file path: ").strip()
    if not os.path.exists(file_path): return print("File not found!")

    rows = []
    with open(file_path, mode='r', encoding='utf-8-sig') as f: rows = list(csv.DictReader(f))

    cache = {}
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, 'r') as f: cache = json.load(f)

    loaded_exchanges, semaphore, factory_lock = {}, asyncio.Semaphore(MAX_ROW_CONCURRENCY), asyncio.Lock()
    tasks, task_map = [], []

    print(f"--- ANALYZING {len(rows)} ROWS ---")
    for idx, row in enumerate(rows):
        symbol, date_val, exch = row.get('Crypto Token Name', ''), row.get('DateTime', ''), row.get('Exchange Name', '')
        dt_utc = parse_date_local(date_val)
        if not symbol or not dt_utc: continue
        
        start_ms = int((dt_utc - timedelta(hours=12)).timestamp() * 1000)
        key = f"{symbol}_{start_ms}_{exch}"
        task_map.append({'idx': idx, 'key': key})
        tasks.append(process_task(key, {'symbol': symbol, 'dt_utc': dt_utc, 'start_ms': start_ms, 'exch_name': exch}, loaded_exchanges, semaphore, cache, factory_lock))

    await asyncio.gather(*tasks)
    for ex in loaded_exchanges.values(): await ex.close()
    with open(CACHE_FILE, 'w') as f: json.dump(cache, f)

    # Reconstruct CSV with all original + new headers
    out_path = "SAGA_Full_Audit_Results.csv"
    if rows:
        audit_headers = ['Fair Value (Avg)', 'Data Quality', 'Final Rate (Close)', 'Source Info', 'Found Time', 'Time Delta (s)']
        all_headers = list(rows[0].keys()) + [h for h in audit_headers if h not in rows[0].keys()]
        with open(out_path, mode='w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=all_headers)
            writer.writeheader()
            for i, row in enumerate(rows):
                res = cache.get(task_map[i]['key'], {}) if i < len(task_map) else {}
                writer.writerow({**row, **res})
    print(f"\n--- SUCCESS. OUTPUT: {out_path} ---")

if __name__ == "__main__":
    asyncio.run(main())