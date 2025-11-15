# scripts/import_symbols.py
"""
Hybrid symbol importer (GitHub primary, Wikipedia fallback) that:
- Pulls NASDAQ/NYSE/AMEX from NasdaqTrader (with fallbacks)
- Pulls OTC from community CSV mirrors
- Pulls S&P 500 / 400 / 600, Dow 30, and Russell indexes from GitHub (R1)
- Falls back to Wikipedia for S&P / Dow if GitHub sources fail
- Deduplicates, filters ETFs/funds, and upserts into Supabase in batches
"""

import csv
import time
import requests
import os
from typing import List, Set, Optional
from supabase import create_client

# Optional import for HTML parsing
try:
    from bs4 import BeautifulSoup
except Exception:
    BeautifulSoup = None  # we'll check at runtime and print helpful message

# -------------------------
# Configuration / Sources
# -------------------------
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_ANON_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("Missing SUPABASE_URL or SUPABASE_ANON_KEY in environment")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Official/primary sources
NASDAQTRADER_NASDAQ = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
NASDAQTRADER_OTHER  = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"

# OTC sources (community mirrors)
OTC_SOURCE_1 = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/otc/OTCbb.csv"
OTC_SOURCE_2 = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/otc/OTCmk.csv"

# Russell (R1) datasets (primary)
RUSSELL_1000 = "https://raw.githubusercontent.com/datasets/russell-index/master/data/russell-1000.csv"
RUSSELL_2000 = "https://raw.githubusercontent.com/datasets/russell-index/master/data/russell-2000.csv"
RUSSELL_3000 = "https://raw.githubusercontent.com/datasets/russell-index/master/data/russell-3000.csv"

# S&P and Dow: prefer GitHub mirrors, fallback to Wikipedia
SP500_GH = "https://raw.githubusercontent.com/datasets/s-and-p-500/master/data/constituents.csv"
SP400_GH = "https://raw.githubusercontent.com/angeloashmore/sandp400/master/data/sandp400.csv"  # community fallback
SP600_GH = "https://raw.githubusercontent.com/holtzy/data_to_viz/master/Example_dataset/1000_SNP600.csv"  # community fallback (if structure differs we fallback to wiki)
DOW_GH   = "https://raw.githubusercontent.com/datasets/dow-jones/master/data/dow-jones-index-components.csv"

# Wikipedia pages (fallback)
WIKI_SP500 = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
WIKI_SP400 = "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies"
WIKI_SP600 = "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies"
WIKI_DOW   = "https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average"

# Generic settings
RETRY_ATTEMPTS = 3
RETRY_DELAY = 3  # seconds
BATCH_SIZE = 500

ETF_KEYWORDS = ["ETF", "ETN", "FUND", "TRUST", "INDEX", "EXCHANGE TRADED"]

# -------------------------
# Utilities
# -------------------------
def safe_get_text(url: str, retries: int = RETRY_ATTEMPTS, delay: int = RETRY_DELAY) -> Optional[str]:
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last_err = e
            print(f"[{attempt}/{retries}] Failed to GET {url}: {e}")
            time.sleep(delay)
    print(f"All attempts failed for {url}: {last_err}")
    return None

def parse_pipe_txt_symbols(text: str, symbol_col_candidates=("Symbol","ACT Symbol","NASDAQ Symbol","CQS Symbol")) -> List[str]:
    lines = text.splitlines()
    if not lines:
        return []
    header = lines[0].split("|")
    symbol_index = None
    for candidate in symbol_col_candidates:
        if candidate in header:
            symbol_index = header.index(candidate)
            break
    if symbol_index is None:
        # last resort: assume first column
        symbol_index = 0

    syms = []
    for line in lines[1:-1]:  # skip header and footer
        parts = line.split("|")
        if len(parts) > symbol_index:
            s = parts[symbol_index].strip().upper()
            if s and s != "SYMBOL":
                syms.append(s)
    return syms

def parse_csv_symbols(text: str, possible_cols=("Symbol","symbol","Ticker","Code","Ticker symbol","ticker")) -> List[str]:
    syms = []
    try:
        reader = csv.DictReader(text.splitlines())
        for row in reader:
            for col in possible_cols:
                if col in row and row[col]:
                    sym = row[col].strip().upper()
                    if sym:
                        syms.append(sym)
                    break
    except Exception as e:
        print(f"CSV parse error: {e}")
    return syms

def parse_wikipedia_table_symbols(html_text: str) -> List[str]:
    if BeautifulSoup is None:
        print("BeautifulSoup not installed — cannot parse Wikipedia fallback. Add beautifulsoup4 to requirements.")
        return []

    soup = BeautifulSoup(html_text, "lxml")
    # find first table that contains a header cell with "Symbol" (case-insensitive)
    tables = soup.find_all("table")
    for table in tables:
        headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
        if any("symbol" in h for h in headers) or any("ticker" in h for h in headers):
            # find which header index contains 'symbol' or 'ticker'
            idx = None
            for i, h in enumerate(headers):
                if "symbol" in h or "ticker" in h:
                    idx = i
                    break
            if idx is None:
                continue
            # collect values from that column
            rows = table.find_all("tr")
            syms = []
            for row in rows[1:]:
                cols = row.find_all(["td","th"])
                if len(cols) > idx:
                    text = cols[idx].get_text(strip=True)
                    if text:
                        # Wikipedia often includes links or footnotes like "AAPL" or "AAPL[1]"
                        # take first token before any whitespace or bracket
                        token = text.split()[0]
                        token = token.split('[')[0]
                        syms.append(token.upper())
            return syms
    return []

def is_etf_symbol(sym: str) -> bool:
    if not sym:
        return False
    upper = sym.upper()
    # quick pattern-based checks (some ETFs end with X or have ETF in name; this is conservative)
    if any(k in upper for k in ETF_KEYWORDS):
        return True
    # typical ETF tickers often end with 'X' but many equities do too; keep conservative
    return False

# -------------------------
# Fetch functions for each universe
# -------------------------
def fetch_nasdaq_nyse_amex() -> List[str]:
    syms = []
    # try NASDAQ Trader NASDAQ list
    text = safe_get_text(NASDAQTRADER_NASDAQ)
    if text:
        syms += parse_pipe_txt_symbols(text)

    # try otherlisted (contains NYSE/AMEX)
    text2 = safe_get_text(NASDAQTRADER_OTHER)
    if text2:
        syms += parse_pipe_txt_symbols(text2)

    return syms

def fetch_otc_all() -> List[str]:
    syms = []
    t1 = safe_get_text(OTC_SOURCE_1)
    if t1:
        syms += parse_csv_symbols(t1, possible_cols=("Symbol","symbol","Ticker","Code"))

    t2 = safe_get_text(OTC_SOURCE_2)
    if t2:
        syms += parse_csv_symbols(t2, possible_cols=("Symbol","symbol","Ticker","Code"))

    return syms

def fetch_russell_all() -> List[str]:
    syms = []
    for url in (RUSSELL_1000, RUSSELL_2000, RUSSELL_3000):
        t = safe_get_text(url)
        if t:
            syms += parse_csv_symbols(t, possible_cols=("symbol","Symbol","Ticker"))
    return syms

def fetch_sp_and_dow_with_fallback() -> List[str]:
    syms = []
    # Primary: GitHub lists
    t = safe_get_text(SP500_GH)
    if t:
        syms += parse_csv_symbols(t, possible_cols=("Symbol","symbol","ticker"))
    else:
        # fallback to wiki
        print("SP500 GitHub source failed; trying Wikipedia fallback")
        wiki = safe_get_text(WIKI_SP500)
        if wiki:
            syms += parse_wikipedia_table_symbols(wiki)

    # S&P 400
    t = safe_get_text(SP400_GH)
    if t:
        syms += parse_csv_symbols(t, possible_cols=("Symbol","symbol","ticker"))
    else:
        print("SP400 GitHub source failed; trying Wikipedia fallback")
        wiki = safe_get_text(WIKI_SP400)
        if wiki:
            syms += parse_wikipedia_table_symbols(wiki)

    # S&P 600
    t = safe_get_text(SP600_GH)
    if t:
        syms += parse_csv_symbols(t, possible_cols=("Symbol","symbol","ticker"))
    else:
        print("SP600 GitHub source failed; trying Wikipedia fallback")
        wiki = safe_get_text(WIKI_SP600)
        if wiki:
            syms += parse_wikipedia_table_symbols(wiki)

    # Dow 30
    t = safe_get_text(DOW_GH)
    if t:
        syms += parse_csv_symbols(t, possible_cols=("Symbol","symbol","ticker","Ticker"))
    else:
        print("Dow GitHub source failed; trying Wikipedia fallback")
        wiki = safe_get_text(WIKI_DOW)
        if wiki:
            syms += parse_wikipedia_table_symbols(wiki)

    return syms

# -------------------------
# Upsert helper
# -------------------------
def upsert_symbols(symbols: List[str]):
    # dedupe & filter
    clean = []
    seen = set()
    for s in symbols:
        if not s:
            continue
        s = s.strip().upper()
        if len(s) > 12:
            continue
        if s in seen:
            continue
        if is_etf_symbol(s):
            continue
        seen.add(s)
        clean.append(s)

    print(f"Upserting {len(clean)} symbols (after de-dup + ETF filter)...")

    batch = []
    for sym in clean:
        batch.append({"symbol": sym, "is_valid": None, "source": "hybrid-import", "last_checked": None})
        if len(batch) >= BATCH_SIZE:
            supabase.table("symbols").upsert(batch).execute()
            batch = []
    if batch:
        supabase.table("symbols").upsert(batch).execute()

    print("Upsert complete.")

# -------------------------
# Main entry
# -------------------------
def main():
    print("Starting hybrid symbol import...")

    collected = []

    # 1) Core exchange lists
    print("Fetching NASDAQ/NYSE/AMEX lists...")
    try:
        collected += fetch_nasdaq_nyse_amex()
    except Exception as e:
        print(f"Error fetching core exchanges: {e}")

    # 2) OTC markets
    print("Fetching OTC lists...")
    try:
        collected += fetch_otc_all()
    except Exception as e:
        print(f"Error fetching OTC: {e}")

    # 3) Russell indexes (R1)
    print("Fetching Russell indexes (1000/2000/3000)...")
    try:
        collected += fetch_russell_all()
    except Exception as e:
        print(f"Error fetching Russell indexes: {e}")

    # 4) S&P and Dow (GitHub primary, Wikipedia fallback)
    print("Fetching S&P / Dow (with fallback)...")
    try:
        collected += fetch_sp_and_dow_with_fallback()
    except Exception as e:
        print(f"Error fetching S&P/Dow: {e}")

    print(f"Total raw symbols collected (pre-dedupe): {len(collected)}")

    # Upsert into Supabase
    upsert_symbols(collected)

    print("Hybrid import finished.")

if __name__ == "__main__":
    main()
