# scripts/import_symbols.py
# Robust importer for US stock symbols:
# - Fetches NASDAQ-listed and Otherlisted from nasdaqtrader.com
# - Falls back to community CSVs if needed
# - Filters obvious ETFs/ETNs
# - Upserts into Supabase

import csv
import requests
import os
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_ANON_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("Missing SUPABASE_URL or SUPABASE_ANON_KEY in environment")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Primary NasdaqTrader sources
NASDAQLISTED_URL     = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
OTHERLISTED_URL      = "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt"

# Fallback GitHub CSV sources
NASDAQLISTED_FALLBACK = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/nasdaq/nasdaq.csv"
NYSE_FALLBACK         = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/nyse/nyse.csv"
AMEX_FALLBACK         = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/amex/amex.csv"

ETF_KEYWORDS = ["ETF", "ETN", "TRUST", "FUND", "INDEX", "EXCHANGE TRADED"]


# ------------------------------
# Safe GET request
# ------------------------------
def safe_get(url):
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"Failed fetch {url}: {e}")
        return None


# ------------------------------
# Fetch NasdaqTrader TXT formats
# Auto-detects symbol column
# ------------------------------
def fetch_nasdaq_trader(primary, fallback):
    symbols = []

    text = safe_get(primary)
    if text is None:
        print(f"Primary source down → Using fallback: {fallback}")
        text = safe_get(fallback)

    if text is None:
        print(f"FAILED: Both primary and fallback unavailable for {primary}")
        return symbols

    lines = text.splitlines()
    
    # Skip malformed files
    if not lines:
        print("Empty response")
        return symbols

    header = lines[0].split("|")

    # Auto-detect correct symbol column
    possible_cols = ["Symbol", "ACT Symbol", "NASDAQ Symbol", "CQS Symbol"]

    symbol_index = None
    for col in possible_cols:
        if col in header:
            symbol_index = header.index(col)
            break

    if symbol_index is None:
        print(f"ERROR: Could not find a symbol column in header: {header}")
        return symbols

    # Parse all lines except header and footer
    for line in lines[1:-1]:
        parts = line.split("|")
        if len(parts) > symbol_index:
            sym = parts[symbol_index].strip().upper()
            if sym and sym not in ("", "SYMBOL"):
                symbols.append(sym)

    return symbols


# ------------------------------
# Fetch CSV-format fallbacks
# ------------------------------
def fetch_csv_file(url, cols=("Symbol", "symbol", "ACT Symbol", "NASDAQ Symbol")):
    symbols = []
    text = safe_get(url)
    if text is None:
        return symbols

    reader = csv.DictReader(text.splitlines())
    for row in reader:
        for col in cols:
            sym = row.get(col)
            if sym:
                symbols.append(sym.strip().upper())
                break
    return symbols


# ------------------------------
# Filtering
# ------------------------------
def looks_like_etf(sym):
    s = sym.upper()
    for k in ETF_KEYWORDS:
        if k in s:
            return True
    return False


# ------------------------------
# Supabase upsert batching
# ------------------------------
def upsert_batch(symbols):
    batch = []
    for sym in symbols:
        if not sym or len(sym) > 12:
            continue

        batch.append({
            "symbol": sym,
            "is_valid": None,
            "source": "loader",
            "last_checked": None
        })

        if len(batch) >= 500:
            supabase.table("symbols").upsert(batch).execute()
            batch = []

    if batch:
        supabase.table("symbols").upsert(batch).execute()


# ------------------------------
# MAIN
# ------------------------------
def main():
    print("Fetching symbol lists...")

    all_syms = []

    # NASDAQ
    all_syms += fetch_nasdaq_trader(
        NASDAQLISTED_URL,
        NASDAQLISTED_FALLBACK
    )

    # NYSE + AMEX via fallback CSVs only
    all_syms += fetch_csv_file(NYSE_FALLBACK)
    all_syms += fetch_csv_file(AMEX_FALLBACK)

    # Otherlisted (NYSE + AMEX mix)
    all_syms += fetch_nasdaq_trader(
        OTHERLISTED_URL,
        NYSE_FALLBACK
    )

    # Deduplicate
    unique_syms = sorted(set(all_syms))
    print(f"Total raw symbols fetched: {len(unique_syms)}")

    # Filter out ETFs
    filtered = [s for s in unique_syms if not looks_like_etf(s)]
    print(f"Symbols after ETF filtering: {len(filtered)}")

    print("Upserting into Supabase symbols table...")
    upsert_batch(filtered)
    print("Done.")


if __name__ == "__main__":
    main()
