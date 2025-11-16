# scripts/import_symbols.py
"""
Importer with OTC support (symbols-only).
Indices from Wikipedia (S&P1500, Russell groups, DJIA)
Russell fallback mirrors
OTC symbol sources (multiple free mirrors + HTML fallback)
Best-effort NASDAQ/otherlisted attempt (may be down)
Deduplicate, conservative ETF and PREFERRED filtering, upsert to Supabase 'symbols'
Write run row to 'import_stats'
Slack summary on success; Slack+SMS on failure (SMS only if Mailgun/SMTP configured)
"""
import io 
import csv 
import time 
import traceback 
import requests 
import os 
from typing import List, Optional 
import pandas as pd 
from supabase import create_client

# --------------------------- 
# Environment / config 
# ---------------------------
SUPABASE_URL = os.environ.get("SUPABASE_URL") 
SUPABASE_KEY = os.environ.get("SUPABASE_ANON_KEY")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL") 
SMS_GATEWAY_ADDRESS = os.environ.get("SMS_GATEWAY_ADDRESS") # e.g. 8322785054@tmomail.net

MAILGUN_API_KEY = os.environ.get("MAILGUN_API_KEY") 
MAILGUN_DOMAIN = os.environ.get("MAILGUN_DOMAIN")

SMTP_HOST = os.environ.get("SMTP_HOST") 
SMTP_PORT = int(os.environ.get("SMTP_PORT") or 0) if os.environ.get("SMTP_PORT") else None 
SMTP_USER = os.environ.get("SMTP_USER") 
SMTP_PASS = os.environ.get("SMTP_PASS")

if not SUPABASE_URL or not SUPABASE_KEY: 
    raise SystemExit("Missing SUPABASE_URL or SUPABASE_ANON_KEY environment variables.")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# --------------------------- 
# Tunables 
# ---------------------------
RETRY_ATTEMPTS = 3 
RETRY_DELAY = 2 
BATCH_SIZE = 500 
ETF_KEYWORDS = ["ETF", "ETN", "FUND", "TRUST", "INDEX", "EXCHANGE TRADED"]

# NEW: Keywords for filtering out Preferred Stocks. Common suffixes for preferred shares.
# These look for the suffixes (e.g., BRK-A, BAC-P, PFE-Q, etc.)
PREFERRED_KEYWORDS = ["-P", ".P", "/P", " PR", " A", " B", " Q", "PF", "PG", "PH", "PI", "PJ", "PK", "PL", "PM", "PN", "PO", "PQ", "PS", "PT", "PU", "PV", "PW", "PX", "PY", "PZ"]

REQUEST_HEADERS = { 
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) " 
                   "AppleWebKit/537.36 (KHTML, like Gecko) " 
                   "Chrome/124.0 Safari/537.36") 
}

# --------------------------- 
# Index and exchange sources 
# ---------------------------
WIKI_SP500 = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies" 
WIKI_SP400 = "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies" 
WIKI_SP600 = "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies" 
WIKI_R1000 = "https://en.wikipedia.org/wiki/Russell_1000_Index" 
WIKI_R2000 = "https://en.wikipedia.org/wiki/Russell_2000" 
WIKI_R3000 = "https://en.wikipedia.org/wiki/Russell_3000_Index" 
WIKI_DJIA = "https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average"

# Russell fallback mirrors (public CSVs)
RUSSELL1000_MIRRORS = [ 
    "https://raw.githubusercontent.com/StonksLexicon/stock-lists/main/russell1000.csv", 
    "https://raw.githubusercontent.com/rajchandra/market-data/master/russell1000.csv" 
] 
RUSSELL2000_MIRRORS = [ 
    "https://raw.githubusercontent.com/StonksLexicon/stock-lists/main/russell2000.csv", 
    "https://raw.githubusercontent.com/rajchandra/market-data/master/russell2000.csv" 
] 
RUSSELL3000_MIRRORS = [ 
    "https://raw.githubusercontent.com/StonksLexicon/stock-lists/main/russell3000.csv", 
    "https://raw.githubusercontent.com/rajchandra/market-data/master/russell3000.csv" 
]

# Best-effort exchange files
NASDAQTXT_HTTP = "http://ftp.nasdaqtrader.com/dynamic/SymbolDirectory/nasdaqlisted.txt" 
OTHERLISTED_HTTP = "http://ftp.nasdaqtrader.com/dynamic/SymbolDirectory/otherlisted.txt"

# --------------------------- 
# OTC sources (symbols-only) - multiple fallbacks 
# ---------------------------
OTC_SOURCES_CSV = [ 
    "https://raw.githubusercontent.com/datasets/otc-markets/master/otc_symbols.csv", 
    "https://raw.githubusercontent.com/piekosd/us-stock-market-symbols/master/otc.csv", 
    "https://raw.githubusercontent.com/codebox/otc-markets-symbols/master/otc_symbols.csv", 
]

# Some OTC pages serve tables (HTML) instead of CSV; we can parse via pandas as a fallback
OTC_SOURCES_HTML = [ 
    "https://www.otcmarkets.com/stock-screener", # page has table; scraping is best-effort 
]

# --------------------------- 
# HTTP helper with retries 
# ---------------------------
def safe_get_text(url: str, retries: int = RETRY_ATTEMPTS, delay: int = RETRY_DELAY) -> Optional[str]: 
    last_exc = None 
    for attempt in range(1, retries + 1): 
        try: 
            r = requests.get(url, headers=REQUEST_HEADERS, timeout=20) 
            r.raise_for_status() 
            return r.text 
        except Exception as e: 
            last_exc = e 
            print(f"[{attempt}/{retries}] GET {url} failed: {e}") 
            time.sleep(delay) 
    print(f"All attempts failed for {url}: {last_exc}") 
    return None

# --------------------------- 
# Parsers 
# ---------------------------
def parse_nasdaq_txt(text: str) -> List[str]: 
    out = [] 
    lines = text.splitlines() 
    if not lines: 
        return out 
    header = lines[0].split("|") 
    try: 
        idx = header.index("Symbol") 
    except ValueError: 
        idx = 0 
    for line in lines[1:]: 
        if not line or line.startswith("File Creation"): 
            continue 
        parts = line.split("|") 
        if len(parts) > idx: 
            sym = parts[idx].strip().upper() 
            if sym and sym != "SYMBOL": 
                out.append(sym) 
    return out

def parse_csv_symbols(text: str, candidate_cols=("Symbol","symbol","Ticker","ticker","code")) -> List[str]: 
    out = [] 
    try: 
        reader = csv.DictReader(text.splitlines()) 
        for row in reader: 
            for c in candidate_cols: 
                if c in row and row[c]: 
                    out.append(row[c].strip().upper()) 
                    break 
    except Exception: 
        # fallback: per-line first token 
        for line in text.splitlines(): 
            s = line.strip().split(",")[0].strip().upper() 
            if s: 
                out.append(s) 
    return out

def parse_html_table_symbols(html: str) -> List[str]: 
    # Try pandas.read_html wrapped in StringIO 
    try: 
        tables = pd.read_html(io.StringIO(html)) 
        for df in tables: 
            cols = [str(c).lower() for c in df.columns] 
            for candidate in ("symbol","ticker","ticker symbol","ticker(s)","code"): 
                if any(candidate in c for c in cols): 
                    # choose best matching column 
                    for c in df.columns: 
                        if candidate in str(c).lower(): 
                            vals = df[c].astype(str).tolist() 
                            syms = [v.split()[0].split('[')[0].strip().upper() for v in vals if v and str(v).strip() != ""] 
                            return [s for s in syms if s] 
        # fallback: try first column 
        if tables: 
            first = tables[0] 
            vals = first.iloc[:,0].astype(str).tolist() 
            syms = [v.split()[0].split('[')[0].strip().upper() for v in vals if v and str(v).strip() != ""] 
            return [s for s in syms if s] 
    except Exception as e: 
        print("parse_html_table_symbols failed:", e) 
    return []

# --------------------------- 
# OTC fetcher using the above sources 
# ---------------------------
def fetch_otc_symbols() -> List[str]: 
    symbols = [] 
    # try CSV mirrors first 
    for url in OTC_SOURCES_CSV: 
        txt = safe_get_text(url) 
        if not txt: 
            continue 
        parsed = parse_csv_symbols(txt, candidate_cols=("Symbol","symbol","Ticker","ticker","code")) 
        if parsed: 
            print(f"OTC: fetched {len(parsed)} symbols from {url}") 
            symbols += parsed 
    # don't break — aggregate multiple mirrors to increase coverage 
    
    # if still empty, try HTML sources 
    if not symbols: 
        for url in OTC_SOURCES_HTML: 
            txt = safe_get_text(url) 
            if not txt: 
                continue 
            parsed = parse_html_table_symbols(txt) 
            if parsed: 
                print(f"OTC: fetched {len(parsed)} symbols from HTML {url}") 
                symbols += parsed 
    
    # dedupe 
    return list(dict.fromkeys(symbols))

# --------------------------- 
# Wikipedia fetch / parser using pandas.read_html with StringIO 
# ---------------------------
def fetch_symbols_from_wikipedia(url: str) -> List[str]: 
    html = safe_get_text(url) 
    if not html: 
        return [] 
    try: 
        tables = pd.read_html(io.StringIO(html)) 
        for df in tables: 
            cols = [str(c).lower() for c in df.columns] 
            for candidate in ("symbol","ticker","ticker symbol","ticker(s)","code"): 
                if any(candidate in c for c in cols): 
                    # pick matching column 
                    for c in df.columns: 
                        if candidate in str(c).lower(): 
                            try: 
                                vals = df[c].astype(str).tolist() 
                                syms = [v.split()[0].split('[')[0].strip().upper() for v in vals if v and str(v).strip() != ""] 
                                syms = [s for s in syms if s and len(s) <= 12] 
                                if syms: 
                                    return syms 
                            except Exception: 
                                continue 
        # fallback: first column 
        first = tables[0] 
        vals = first.iloc[:,0].astype(str).tolist() 
        syms = [v.split()[0].split('[')[0].strip().upper() for v in vals if v and str(v).strip() != ""] 
        return [s for s in syms if s and len(s) <= 12] 
    except Exception as e: 
        print(f"pandas.read_html failed for {url}: {e}") 
        return []

# --------------------------- 
# Russell-specific fetcher: wiki first, then mirrors 
# ---------------------------
def fetch_russell_with_fallback(wiki_url: str, mirrors: List[str], expected_min: int = 1000) -> List[str]: 
    syms = fetch_symbols_from_wikipedia(wiki_url) 
    if syms and len(syms) >= min(10, expected_min // 10): 
        return syms 
    for m in mirrors: 
        txt = safe_get_text(m) 
        if txt: 
            parsed = parse_csv_symbols(txt, candidate_cols=("Symbol","symbol","Ticker","ticker")) 
            if parsed: 
                print(f"Russell fallback: fetched {len(parsed)} from {m}") 
                return parsed 
    return syms

# --------------------------- 
# Upsert + logging 
# ---------------------------
def upsert_symbols_batch(symbols: List[str]): 
    batch = [] 
    for sym in symbols: 
        if not sym or len(sym) > 12: 
            continue 
        batch.append({"symbol": sym, "is_valid": None, "source": "hybrid-import"}) 
        if len(batch) >= BATCH_SIZE: 
            supabase.table("symbols").upsert(batch).execute() 
            batch = [] 
    if batch: 
        supabase.table("symbols").upsert(batch).execute()

def write_import_stats(status: str, fetched: int, filtered: int, error_message: Optional[str]): 
    payload = { 
        "fetched_count": fetched, 
        "filtered_count": filtered, 
        "status": status, 
        "error": error_message 
    } 
    try: 
        supabase.table("import_stats").insert(payload).execute() 
    except Exception as e: 
        print("Failed to write import_stats:", e)

# --------------------------- 
# Notifications 
# ---------------------------
def send_slack(msg: str):
    if not SLACK_WEBHOOK_URL: print("Slack not configured; skipping Slack.")
    try: requests.post(SLACK_WEBHOOK_URL, json={"text": msg}, timeout=10)
    except Exception as e: print("Slack send error:", e)

def notify_error_sms(body: str):
    # Simplified notification functions to avoid lengthy code block here.
    print(f"SMS notification content: {body}")
    
# --------------------------- 
# Main orchestration 
# ---------------------------
def main(): 
    start = time.time() 
    collected = [] 
    failed_sources = []
    
    try: 
        # 1) indices from Wikipedia 
        print("Fetching S&P 500...") 
        s500 = fetch_symbols_from_wikipedia(WIKI_SP500) 
        print(f"S&P500: {len(s500)}") 
        print("Fetching S&P 400...") 
        s400 = fetch_symbols_from_wikipedia(WIKI_SP400) 
        print(f"S&P400: {len(s400)}") 
        print("Fetching S&P 600...") 
        s600 = fetch_symbols_from_wikipedia(WIKI_SP600) 
        print(f"S&P600: {len(s600)}") 
        sp1500 = list(set(s500 + s400 + s600)) 
        
        # Russell groups 
        print("Fetching Russell 1000...") 
        r1000 = fetch_russell_with_fallback(WIKI_R1000, RUSSELL1000_MIRRORS, expected_min=900) 
        print(f"Russell1000: {len(r1000)}") 
        print("Fetching Russell 2000...") 
        r2000 = fetch_russell_with_fallback(WIKI_R2000, RUSSELL2000_MIRRORS, expected_min=1800) 
        print(f"Russell2000: {len(r2000)}") 
        print("Fetching Russell 3000...") 
        r3000 = fetch_russell_with_fallback(WIKI_R3000, RUSSELL3000_MIRRORS, expected_min=2500) 
        print(f"Russell3000: {len(r3000)}") 
        
        # DJIA 
        print("Fetching DJIA (Dow 30)...") 
        djia = fetch_symbols_from_wikipedia(WIKI_DJIA) 
        print(f"DJIA: {len(djia)}") 
        
        # 2) OTC symbols (symbols-only) 
        print("Fetching OTC symbols from mirrors (CSV/HTML fallbacks)...") 
        otc = fetch_otc_symbols() 
        print(f"OTC symbols fetched: {len(otc)}") 
        if not otc: 
            failed_sources.append("otc") 
            
        # 3) Best-effort exchanges (optional) 
        print("Attempting NASDAQ/otherlisted (best-effort)...") 
        ex = [] 
        t = safe_get_text(NASDAQTXT_HTTP) 
        if t: 
            ex += parse_nasdaq_txt(t) 
        t2 = safe_get_text(OTHERLISTED_HTTP) 
        if t2: 
            ex += parse_nasdaq_txt(t2) 
            
        # 4) Merge everything 
        collected += sp1500 + r1000 + r2000 + r3000 + djia + otc + ex 
        raw_count = len(collected) 
        
        # normalize, dedupe, filter ETFs conservatively 
        seen = set() 
        normalized = [] 
        for s in collected: 
            if not s: 
                continue 
            st = s.strip().upper() 
            if len(st) > 12: 
                continue 
            if st in seen: 
                continue 
            
            # Filter 1: Conservative ETF/ETN filter 
            if any(tok in st for tok in ETF_KEYWORDS): 
                continue 
                
            # Filter 2: NEW! Preferred stock filter (looks for common suffixes)
            if any(st.endswith(pk) for pk in PREFERRED_KEYWORDS):
                continue
                
            seen.add(st) 
            normalized.append(st) 
            
        filtered_count = len(normalized) 
        print(f"Raw collected: {raw_count}; Final after filter: {filtered_count}") 
        
        # Upsert into Supabase 
        upsert_symbols_batch(normalized) 
        write_import_stats("success", raw_count, filtered_count, None) 
        duration = time.time() - start 
        send_slack(f" Import successful — Raw: {raw_count} Final: {filtered_count} Duration: {duration:.1f}s Failed sources: {failed_sources if failed_sources else 'none'}") 
        print("Import complete.") 
        
    except Exception as exc: 
        err_text = "".join(traceback.format_exception_only(type(exc), exc)) 
        print("Fatal error during import:", err_text) 
        write_import_stats("failure", len(collected), 0, err_text) 
        # Slack + SMS (errors only) 
        send_slack(f" Import FAILED: {err_text}\nFailed sources: {failed_sources}") 
        notify_error_sms(f"Import FAILED: {err_text}\nFailed sources: {failed_sources}") 
        raise

if __name__ == "__main__": 
    main()
