# scripts/import_symbols.py
# Resilient symbol importer with retries + fallback sources.

import csv
import requests
import os
import time
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_ANON_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("Missing SUPABASE_URL or SUPABASE_ANON_KEY in environment")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Primary official sources (may be unstable)
NASDAQ_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
OTHERLISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"

# Fallback mirrors if NasdaqTrader is down
NASDAQ_FALLBACK = "https://raw.githubusercontent.com/arm61/nasdaq-listings/main/data/nasdaqlisted.txt"
OTHERLISTED_FALLBACK = "https://raw.githubusercontent.com/arm61/nasdaq-listings/main/data/otherlisted.txt"

# Stable OTC source
OTC_URL = "https://www.eoddata.com/Data/symbols/OTC.csv"

def safe_get(url, retries=3, delay=3):
    for i in range(retries):
        try:
            r = requests.get(url, timeout=15)
            r.raise_for_status()
            return r.text
        except Exception as e:
            print(f"[Attempt {i+1}/{retries}] Failed fetching {url}: {e}")
            time.sleep(delay)
    return None


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
    header = lines[0].split("|")
    symbol_index = header.index("Symbol")

    for line in lines[1:-1]:
        parts = line.split("|")
        if len(parts) > symbol_index:
            sym = parts[symbol_index].strip().upper()
            if sym and sym != "SYMBOL":
                symbols.append(sym)

    return symbols


def fetch_otc_csv(url):
    symbols = []
    text = safe_get(url)

    if text is None:
        print(f"FAILED: Could not fetch OTC symbols.")
        return symbols

    reader = csv.DictReader(text.splitlines())
    for row in reader:
        sym = row.get("Code")
        if sym:
            symbols.append(sym.strip().upper())

    return symbols


def upsert_batch(symbols):
    batch = []
    for sym in symbols:
        if not sym or len(sym) > 12:
            continue

        batch.append({
            "symbol": sym,
            "is_valid": None,
            "source": "importer",
            "last_checked": None
        })

        if len(batch) >= 500:
            supabase.table("symbols").upsert(batch).execute()
            batch = []

    if batch:
        supabase.table("symbols").upsert(batch).execute()


def main():
    print("Fetching symbol lists...")

    all_syms = []

    all_syms += fetch_nasdaq_trader(NASDAQ_URL, NASDAQ_FALLBACK)
    all_syms += fetch_nasdaq_trader(OTHERLISTED_URL, OTHERLISTED_FALLBACK)
    all_syms += fetch_otc_csv(OTC_URL)

    unique = sorted(set(all_syms))
    print(f"Total symbols fetched: {len(unique)}")

    print("Upserting into Supabase...")
    upsert_batch(unique)

    print("Done.")


if __name__ == "__main__":
    main()
