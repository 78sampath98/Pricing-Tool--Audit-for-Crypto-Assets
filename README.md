# Pricing-Tool--Audit-for-Crypto-Assets
### Professional Cryptocurrency Pricing & Audit Tool

**Objective:** Automated Fair Value discovery with tiered exchange hunting and audit-grade quality flagging.

---

## Project Overview
The Pricing tool is a Python-based automation tool designed to streamline the valuation of digital assets for financial audits. It eliminates manual price lookups by connecting directly to 100+ global exchanges via the CCXT library, ensuring precise price discovery at specific historical timestamps.

### Key Features:
* **Sovereign Hunt:** Prioritizes the user's specified exchange (e.g., Binance, Coinbase).
* **Tiered Fallback:** If a token is delisted or the primary exchange fails, the tool automatically hunts through 15+ "Audit-Grade" and "High-Volume" exchanges.
* **Smart Quality Flagging:** Automatically rates data as **High**, **Medium**, or **Low** based on time-delta variance and candle timeframe (1m, 1h, 1d).
* **Local Performance:** Optimized for local execution using standard Python CSV libraries with an intelligent JSON cache to prevent redundant API calls.

---

## 🛡️ Audit Methodology
To ensure compliance with financial reporting standards (IFRS/GAAP), the tool follows a structured discovery logic:

1.  **Direct Match:** Attempts to find the exact Symbol/Quote pair on the requested exchange.
2.  **Stablecoin Fallback:** If a direct Fiat pair (e.g., BTC/USD) is unavailable, it automatically checks liquid stablecoin pairs (e.g., BTC/USDT, BTC/USDC).
3.  **Tiered Discovery:** If no data is found on the primary exchange, it traverses:
    * **Tier 1 (Audit Grade):** Coinbase, Gemini, Bitstamp, Kraken.
    * **Tier 2 (High Volume):** Binance, OKX, KuCoin, Bybit.
4.  **Tolerance Check:** Only accepts candles within a strict 12-hour window of the target audit time to maintain valuation integrity.

---

## 🚀 Getting Started

### Prerequisites
* **Python 3.8+**
* **CCXT Library:** Run `pip install ccxt`

### Usage
1.  Prepare your input CSV with headers: `Crypto Token Name`, `DateTime`, and `Exchange Name`.
2.  Run the script:
    ```bash
    python Code.py
    ```
3.  Enter the name of your CSV file (e.g., `Input file.csv`) when prompted.

---

## 📊 Output Schema
The tool generates `SAGA_Full_Audit_Results.csv` including:
* **Fair Value (Avg):** Mean of Open, High, Low, and Close.
* **Final Rate (Close):** Standard closing price.
* **Data Quality:** Reliability rating (e.g., "High").
* **Source Info:** Exchange + Timeframe + Pair.
* **Time Delta (s):** Variance between requested time and data found.
