# scripts/import_symbols.py
"""
Hybrid symbol importer + logging + free ntfy error alerts.

What it does:
- Pulls NASDAQ/NYSE/AMEX (NasdaqTrader primary, with fallbacks)
- Pulls OTC from community CSV mirrors
- Pulls Russell 1000/2000/3000 from datasets/russell-index (primary)
- Pulls S&P500/400/600 + Dow (GitHub primary, Wikipedia fallback)
- Deduplicates, filters ETF-like names, and batch-upserts to Supabase
- Writes a run log row to `import_logs` in Supabase
- On ERROR: sends a free ntfy push notification (no API key required)
Notes:
- Requires Python packages: requests, supabase, beautifulsoup4, lxml
"""

import csv
import time
import traceback
import requests
import os
from typing import List, Optional
from supabase import create_client

# Optional: BeautifulSoup for Wikipedia fallback parsing
try:
    from bs4 import BeautifulSoup
except Exception:
    BeautifulSoup = None

# --------------------------
# Config / Env
# --------------------------
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_ANON_KEY")
NTFY_TOPIC = os.environ.get("NTFY_TOPIC", "market-data-pipeline")  # optional override

if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("Missing SUPABASE_URL or SUPABASE_ANON_KEY environment variables.")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Sources
NASDAQTRADER_NASDAQ = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
NASDAQTRADER_OTHER  = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"

OTC_SOURCE_1 = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/otc/OTCbb.csv"
OTC_SOURCE_2 = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/otc/OTCmk.csv"

RUSSELL_1000 = "https://raw.githubusercontent.com/datasets/russell-index/master/data/russell-1000.csv"
RUSSELL_2000 = "https://raw.githubusercontent.com/datasets/russell-index/master/data/russell-2000.csv"
RUSSELL_3000 = "https://raw.githubusercontent.com/datasets/russell-index/master/data/russell-3000.csv"

SP500_GH = "https://raw.githubusercontent.com/datasets/s-and-p-500/master/data/constituents.csv"
SP400_GH = "https://raw.githubusercontent.com/angeloashmore/sandp400/master/data/sandp400.csv"
SP600_GH = "https://raw.githubusercontent.com/holtzy/data_to_viz/master/Example_dataset/1000_SNP600.csv"
DOW_GH   = "https://raw.githubusercontent.com/datasets/dow-jones/master/data/dow-jones-index-components.csv"

WIKI_SP500 = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
WIKI_SP400 = "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies"
WIKI_SP600 = "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies"
WIKI_DOW   = "https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average"

# Behavior
RETRY_ATTEMPTS = 3
RETRY_DELAY = 2
BATCH_SIZE = 500
ETF_KEYWORDS = ["ETF", "ETN", "FUND", "TRUST", "INDEX", "EXCHANGE TRADED"]

# --------------------------
# Helpers
# --------------------------
def safe_get_text(url: str, retries: int = RETRY_ATTEMPTS, delay: int = RETRY_DELAY) -> Optional[str]:
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last_err = e
            print(f"[{attempt}/{retries}] GET {url} failed: {e}")
            time.sleep(delay)
    print(f"All attempts failed for {url}: {last_err}")
    return None

def parse_pipe_txt_symbols(text: str, symbol_candidates=("Symbol","ACT Symbol","NASDAQ Symbol","CQS Symbol")) -> List[str]:
    lines = text.splitlines()
    if not lines:
        return []
    header = lines[0].split("|")
    symbol_index = None
    for cand in symbol_candidates:
        if cand in header:
            symbol_index = header.index(cand)
            break
    if symbol_index is None:
        symbol_index = 0
    out = []
    for line in lines[1:-1]:  # skip header/footer lines
        parts = line.split("|")
        if len(parts) > symbol_index:
            s = parts[symbol_index].strip().upper()
            if s and s != "SYMBOL":
                out.append(s)
    return out

def parse_csv_symbols(text: str, possible_cols=("Symbol","symbol","Ticker","Code","ticker")) -> List[str]:
    out = []
    try:
        reader = csv.DictReader(text.splitlines())
        for row in reader:
            for col in possible_cols:
                if col in row and row[col]:
                    out.append(row[col].strip().upper())
                    break
    except Exception as e:
        print(f"CSV parse error: {e}")
    return out

def parse_wikipedia_table_symbols(html_text: str) -> List[str]:
    if BeautifulSoup is None:
        print("BeautifulSoup not installed; Wikipedia fallback skipped.")
        return []
    soup = BeautifulSoup(html_text, "lxml")
    tables = soup.find_all("table")
    for table in tables:
        headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
        if any("symbol" in h or "ticker" in h for h in headers):
            idx = None
            for i,h in enumerate(headers):
                if "symbol" in h or "ticker" in h:
                    idx = i
                    break
            if idx is None:
                continue
            syms = []
            rows = table.find_all("tr")
            for r in rows[1:]:
                cols = r.find_all(["td","th"])
                if len(cols) > idx:
                    text = cols[idx].get_text(strip=True)
                    token = text.split()[0].split('[')[0]
                    if token:
                        syms.append(token.upper())
            return syms
    return []

def is_etf_symbol(sym: str) -> bool:
    if not sym:
        return False
    upper = sym.upper()
    if any(k in upper for k in ETF_KEYWORDS):
        return True
    return False

# --------------------------
# Source fetchers
# --------------------------
def fetch_nasdaq_nyse_amex() -> List[str]:
    out = []
    t = safe_get_text(NASDAQTRADER_NASDAQ)
    if t:
        out += parse_pipe_txt_symbols(t)
    t2 = safe_get_text(NASDAQTRADER_OTHER)
    if t2:
        out += parse_pipe_txt_symbols(t2)
    return out

def fetch_otc_all() -> List[str]:
    out = []
    t1 = safe_get_text(OTC_SOURCE_1)
    if t1:
        out += parse_csv_symbols(t1, possible_cols=("Symbol","symbol","Ticker","Code"))
    t2 = safe_get_text(OTC_SOURCE_2)
    if t2:
        out += parse_csv_symbols(t2, possible_cols=("Symbol","symbol","Ticker","Code"))
    return out

def fetch_russell_all() -> List[str]:
    out = []
    for url in (RUSSELL_1000, RUSSELL_2000, RUSSELL_3000):
        t = safe_get_text(url)
        if t:
            out += parse_csv_symbols(t, possible_cols=("symbol","Symbol","Ticker"))
    return out

def fetch_sp_and_dow_with_fallback() -> List[str]:
    out = []
    t = safe_get_text(SP500_GH)
    if t:
        out += parse_csv_symbols(t, possible_cols=("Symbol","symbol","ticker"))
    else:
        print("SP500 GitHub failed — trying Wikipedia")
        wiki = safe_get_text(WIKI_SP500)
        if wiki:
            out += parse_wikipedia_table_symbols(wiki)

    t = safe_get_text(SP400_GH)
    if t:
        out += parse_csv_symbols(t, possible_cols=("Symbol","symbol","ticker"))
    else:
        print("SP400 GitHub failed — trying Wikipedia")
        wiki = safe_get_text(WIKI_SP400)
        if wiki:
            out += parse_wikipedia_table_symbols(wiki)

    t = safe_get_text(SP600_GH)
    if t:
        out += parse_csv_symbols(t, possible_cols=("Symbol","symbol","ticker"))
    else:
        print("SP600 GitHub failed — trying Wikipedia")
        wiki = safe_get_text(WIKI_SP600)
        if wiki:
            out += parse_wikipedia_table_symbols(wiki)

    t = safe_get_text(DOW_GH)
    if t:
        out += parse_csv_symbols(t, possible_cols=("Symbol","symbol","ticker","Ticker"))
    else:
        print("Dow GitHub failed — trying Wikipedia")
        wiki = safe_get_text(WIKI_DOW)
        if wiki:
            out += parse_wikipedia_table_symbols(wiki)

    return out

# --------------------------
# Upsert & logging
# --------------------------
def upsert_batch(symbols: List[str]):
    batch = []
    for sym in symbols:
        if not sym or len(sym) > 12:
            continue
        batch.append({"symbol": sym, "is_valid": None, "source": "hybrid-import", "last_checked": None})
        if len(batch) >= BATCH_SIZE:
            supabase.table("symbols").upsert(batch).execute()
            batch = []
    if batch:
        supabase.table("symbols").upsert(batch).execute()

def write_import_log(status: str, raw_count: int, filtered_count: int, failed_sources: List[str], err_text: Optional[str]):
    try:
        payload = {
            "status": status,
            "source_count": raw_count,
            "filtered_count": filtered_count,
            "error_message": err_text or None
        }
        supabase.table("import_logs").insert(payload).execute()
    except Exception as e:
        print(f"Failed to write import log to Supabase: {e}")

def send_ntfy_alert(title: str, body: str):
    try:
        url = f"https://ntfy.sh/{NTFY_TOPIC}"
        headers = {"Title": title}
        requests.post(url, data=body.encode("utf-8"), headers=headers, timeout=10)
    except Exception as e:
        print(f"Failed to send ntfy alert: {e}")

# --------------------------
# Main
# --------------------------
def main():
    start = time.time()
    collected = []
    failed_sources = []

    try:
        print("Fetching NASDAQ/NYSE/AMEX...")
        try:
            collected += fetch_nasdaq_nyse_amex()
        except Exception as e:
            failed_sources.append("nasdaqtrader")
            print(f"Error fetching nasdaq/other: {e}")

        print("Fetching OTC...")
        try:
            otc = fetch_otc_all()
            if not otc:
                failed_sources.append("otc")
            collected += otc
        except Exception as e:
            failed_sources.append("otc")
            print(f"Error fetching OTC: {e}")

        print("Fetching Russell indexes...")
        try:
            r = fetch_russell_all()
            if not r:
                failed_sources.append("russell")
            collected += r
        except Exception as e:
            failed_sources.append("russell")
            print(f"Error fetching Russell: {e}")

        print("Fetching S&P / Dow (with fallback)...")
        try:
            sp = fetch_sp_and_dow_with_fallback()
            if not sp:
                failed_sources.append("sp/dow")
            collected += sp
        except Exception as e:
            failed_sources.append("sp/dow")
            print(f"Error fetching sp/dow: {e}")

        raw_count = len(collected)
        print(f"Total raw symbols collected: {raw_count}")

        # dedupe & filter
        clean = []
        seen = set()
        for s in collected:
            if not s:
                continue
            st = s.strip().upper()
            if len(st) > 12:
                continue
            if st in seen:
                continue
            if is_etf_symbol(st):
                continue
            seen.add(st)
            clean.append(st)

        filtered_count = len(clean)
        print(f"Symbols after dedupe & ETF filter: {filtered_count}")

        # upsert
        upsert_batch(clean)

        duration = time.time() - start
        write_import_log("success", raw_count, filtered_count, failed_sources, None)
        print(f"Import finished in {duration:.1f}s. Raw: {raw_count} Filtered: {filtered_count}")

    except Exception as exc:
        duration = time.time() - start
        err_text = "".join(traceback.format_exception_only(type(exc), exc))
        print(f"Fatal error during import: {err_text}")
        write_import_log("error", len(collected), 0, failed_sources, err_text)
        # send ntfy alert (errors only)
        title = "[market-data-pipeline] 🚨 Import FAILED"
        body = f"Error: {err_text}\nFailed sources: {failed_sources}"
        send_ntfy_alert(title, body)
        raise

if __name__ == "__main__":
    main()
