# scripts/import_symbols.py
# Simple importer: fetches several free symbol lists, filters obvious ETFs/ETNs, upserts into Supabase.

import csv
import requests
import os
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_ANON_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("Missing SUPABASE_URL or SUPABASE_ANON_KEY in environment")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Simple data sources (free public CSVs / repos)
NASDAQ_URL = "https://raw.githubusercontent.com/datasets/us-stock-market/master/data/nasdaq.csv"
NYSE_URL   = "https://raw.githubusercontent.com/datasets/us-stock-market/master/data/nyse.csv"
AMEX_URL   = "https://raw.githubusercontent.com/datasets/us-stock-market/master/data/amex.csv"
OTC_URL    = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/otc/OTC.csv"

ETF_KEYWORDS = ["ETF", "ETN", "TRUST", "FUND", "INDEX", "EXCHANGE TRADED"]

def fetch_csv_symbols(url, col_names=("Symbol","symbol")):
    symbols = []
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        text = r.text.splitlines()
        reader = csv.DictReader(text)
        for row in reader:
            for col in col_names:
                sym = row.get(col)
                if sym:
                    symbols.append(sym.strip().upper())
                    break
    except Exception as e:
        print(f"Failed fetch {url}: {e}")
    return symbols

def looks_like_etf_or_fund(name_or_symbol):
    if not name_or_symbol:
        return False
    s = name_or_symbol.upper()
    for k in ETF_KEYWORDS:
        if k in s:
            return True
    # also exclude single-character weird indexes
    if len(s) > 0 and any(ch.isdigit() for ch in s):
        # contain digits -> often not a plain equity ticker (but some tickers do contain digits; this is conservative)
        return False
    return False

def upsert_batch(symbols):
    batch = []
    for sym in symbols:
        if not sym or len(sym) > 12:
            continue
        batch.append({"symbol": sym, "is_valid": None, "source": "loader", "last_checked": None})
        if len(batch) >= 500:
            supabase.table("symbols").upsert(batch).execute()
            batch = []
    if batch:
        supabase.table("symbols").upsert(batch).execute()

def main():
    print("Fetching CSV source lists...")
    all_syms = []
    all_syms += fetch_csv_symbols(NASDAQ_URL)
    all_syms += fetch_csv_symbols(NYSE_URL)
    all_syms += fetch_csv_symbols(AMEX_URL)
    all_syms += fetch_csv_symbols(OTC_URL)

    unique = sorted(set(all_syms))
    print(f"Total raw symbols fetched: {len(unique)}")

    # Basic filtering: remove obvious ETF/fund names if present in symbol (these sources may not include names)
    filtered = [s for s in unique if not looks_like_etf_or_fund(s)]
    print(f"Symbols after basic filtering: {len(filtered)}")

    print("Upserting into Supabase symbols table...")
    upsert_batch(filtered)
    print("Done.")

if __name__ == '__main__':
    main()
