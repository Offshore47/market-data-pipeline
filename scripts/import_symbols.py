# scripts/import_symbols.py
"""
Clean rebuild importer (Option B).
- Multi-source (exchange lists + index CSV mirrors)
- No Wikipedia scraping
- Robust retries with browser User-Agent
- Upserts into Supabase 'symbols' table
- Writes run row to 'import_stats' table
- Slack summary on success
- Slack + SMS only on errors (SMS via Mailgun or SMTP if configured)
"""

import csv
import time
import traceback
import requests
import os
from typing import List, Optional
from supabase import create_client

# ------------------------
# Configuration (env)
# ------------------------
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_ANON_KEY")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")
SMS_GATEWAY_ADDRESS = os.environ.get("SMS_GATEWAY_ADDRESS")  # e.g. 8322785054@tmomail.net

# Mailgun (optional) - preferred for email->SMS
MAILGUN_API_KEY = os.environ.get("MAILGUN_API_KEY")
MAILGUN_DOMAIN = os.environ.get("MAILGUN_DOMAIN")  # e.g. mg.example.com

# SMTP fallback (optional)
SMTP_HOST = os.environ.get("SMTP_HOST")
SMTP_PORT = int(os.environ.get("SMTP_PORT") or 0) if os.environ.get("SMTP_PORT") else None
SMTP_USER = os.environ.get("SMTP_USER")
SMTP_PASS = os.environ.get("SMTP_PASS")

# Basic sanity check
if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("Missing SUPABASE_URL or SUPABASE_ANON_KEY environment variables.")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ------------------------
# Behavior tuning
# ------------------------
RETRY_ATTEMPTS = 3
RETRY_DELAY = 2
BATCH_SIZE = 500
ETF_KEYWORDS = ["ETF", "ETN", "FUND", "TRUST", "INDEX", "EXCHANGE TRADED"]

# Use a realistic browser User-Agent to reduce blocking
REQUEST_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0 Safari/537.36")
}

# ------------------------
# Candidate sources (multiple fallbacks)
# Keep these lists so the script tries the first working one.
# ------------------------

# Exchanges (NASDAQ/NYSE/AMEX)
EXCHANGE_PRIMARY = [
    "http://ftp.nasdaqtrader.com/dynamic/SymbolDirectory/nasdaqlisted.txt",   # HTTP sometimes required
    "https://ftp.nasdaqtrader.com/dynamic/SymbolDirectory/nasdaqlisted.txt"
]
EXCHANGE_OTHER = [
    "http://ftp.nasdaqtrader.com/dynamic/SymbolDirectory/otherlisted.txt",
    "https://ftp.nasdaqtrader.com/dynamic/SymbolDirectory/otherlisted.txt"
]
# Community mirrors fallback
EXCHANGE_MIRRORS = [
    "https://raw.githubusercontent.com/shadmansaleh/us-indices/main/data/nasdaq.csv",
    "https://raw.githubusercontent.com/shadmansaleh/us-indices/main/data/nyse.csv"
]

# OTC
OTC_SOURCES = [
    "https://raw.githubusercontent.com/codebox/otc-markets-symbols/master/otc_symbols.csv",
    "https://raw.githubusercontent.com/shadmansaleh/us-indices/main/data/otc.csv"
]

# S&P 500 / 400 / 600 (we'll merge into S&P 1500)
SP500_SOURCES = [
    "https://raw.githubusercontent.com/shadmansaleh/us-indices/main/data/sp500.csv",
    "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"
]
SP400_SOURCES = [
    "https://raw.githubusercontent.com/shadmansaleh/us-indices/main/data/sp400.csv",
    "https://raw.githubusercontent.com/angeloashmore/sandp400/master/data/sandp400.csv"
]
SP600_SOURCES = [
    "https://raw.githubusercontent.com/shadmansaleh/us-indices/main/data/sp600.csv",
    "https://raw.githubusercontent.com/shadmansaleh/us-indices/main/data/sp600.csv"  # same fallback if needed
]

# Dow Jones
DJIA_SOURCES = [
    "https://raw.githubusercontent.com/shadmansaleh/us-indices/main/data/djia.csv",
    "https://raw.githubusercontent.com/datasets/dow-jones-industrial-average/master/data/djia.csv"
]

# Russell 1000/2000/3000
RUSSELL_1000_SOURCES = [
    "https://raw.githubusercontent.com/shadmansaleh/us-indices/main/data/r1000.csv",
    "https://raw.githubusercontent.com/alexander-ponomaroff/russell-index-data/main/russell1000.csv"
]
RUSSELL_2000_SOURCES = [
    "https://raw.githubusercontent.com/shadmansaleh/us-indices/main/data/r2000.csv",
    "https://raw.githubusercontent.com/alexander-ponomaroff/russell-index-data/main/russell2000.csv"
]
RUSSELL_3000_SOURCES = [
    "https://raw.githubusercontent.com/shadmansaleh/us-indices/main/data/r3000.csv",
    "https://raw.githubusercontent.com/alexander-ponomaroff/russell-index-data/main/russell3000.csv"
]

# ------------------------
# HTTP helper with retries
# ------------------------
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

# ------------------------
# Parsers
# ------------------------
def parse_nasdaq_txt(text: str) -> List[str]:
    lines = text.splitlines()
    if not lines:
        return []
    header = lines[0].split("|")
    # find symbol col
    idx = 0
    for candidate in ("Symbol", "ACT Symbol", "NASDAQ Symbol", "CQS Symbol"):
        if candidate in header:
            idx = header.index(candidate)
            break
    out = []
    for line in lines[1:]:
        if not line or line.startswith("File Creation") or line.startswith("NASDAQ"):
            continue
        parts = line.split("|")
        if len(parts) > idx:
            s = parts[idx].strip()
            if s and s.upper() != "SYMBOL":
                out.append(s.upper())
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
    except Exception as e:
        print("CSV parse error:", e)
    return out

# ------------------------
# Fetch helpers that try many candidates
# ------------------------
def fetch_first_working(urls: List[str], parser_fn) -> List[str]:
    for url in urls:
        text = safe_get_text(url)
        if text:
            try:
                return parser_fn(text)
            except Exception as e:
                print(f"Parser failed for {url}: {e}")
    return []

def fetch_exchanges_all() -> List[str]:
    collected = []
    collected += fetch_first_working(EXCHANGE_PRIMARY, parse_nasdaq_txt)
    collected += fetch_first_working(EXCHANGE_OTHER, parse_nasdaq_txt)
    # community mirrors if above failed
    for mirror in EXCHANGE_MIRRORS:
        txt = safe_get_text(mirror)
        if txt:
            collected += parse_csv_symbols(txt)
    return collected

def fetch_otc_all() -> List[str]:
    return fetch_first_working(OTC_SOURCES, lambda t: parse_csv_symbols(t, ("Symbol","symbol","Ticker","code")))

def fetch_sp_group() -> List[str]:
    out = []
    out += fetch_first_working(SP500_SOURCES, lambda t: parse_csv_symbols(t, ("Symbol","symbol","Ticker")))
    out += fetch_first_working(SP400_SOURCES, lambda t: parse_csv_symbols(t, ("Symbol","symbol","Ticker")))
    out += fetch_first_working(SP600_SOURCES, lambda t: parse_csv_symbols(t, ("Symbol","symbol","Ticker")))
    return out

def fetch_djia_all() -> List[str]:
    return fetch_first_working(DJIA_SOURCES, lambda t: parse_csv_symbols(t, ("Symbol","symbol","Ticker")))

def fetch_russell_group() -> List[str]:
    out = []
    out += fetch_first_working(RUSSELL_1000_SOURCES, lambda t: parse_csv_symbols(t, ("symbol","Symbol","Ticker","ticker")))
    out += fetch_first_working(RUSSELL_2000_SOURCES, lambda t: parse_csv_symbols(t, ("symbol","Symbol","Ticker","ticker")))
    out += fetch_first_working(RUSSELL_3000_SOURCES, lambda t: parse_csv_symbols(t, ("symbol","Symbol","Ticker","ticker")))
    return out

# ------------------------
# Filters & upsert
# ------------------------
def looks_like_etf(sym: str) -> bool:
    if not sym:
        return False
    s = sym.upper()
    if any(k in s for k in ETF_KEYWORDS):
        return True
    return False

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

# ------------------------
# Logging
# ------------------------
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

# ------------------------
# Notifications
# ------------------------
def send_slack(msg: str):
    if not SLACK_WEBHOOK_URL:
        print("Slack not configured; skipping Slack.")
        return
    try:
        requests.post(SLACK_WEBHOOK_URL, json={"text": msg}, timeout=10)
    except Exception as e:
        print("Slack send error:", e)

def send_sms_via_mailgun(to_addr: str, body: str) -> bool:
    if not (MAILGUN_API_KEY and MAILGUN_DOMAIN):
        return False
    try:
        resp = requests.post(
            f"https://api.mailgun.net/v3/{MAILGUN_DOMAIN}/messages",
            auth=("api", MAILGUN_API_KEY),
            data={
                "from": f"market-pipeline@{MAILGUN_DOMAIN}",
                "to": [to_addr],
                "subject": "",
                "text": body
            },
            timeout=15,
        )
        return resp.status_code in (200, 201)
    except Exception as e:
        print("Mailgun error:", e)
        return False

def send_sms_via_smtp(to_addr: str, body: str) -> bool:
    if not (SMTP_HOST and SMTP_PORT and SMTP_USER and SMTP_PASS):
        return False
    try:
        import smtplib
        from email.message import EmailMessage
        msg = EmailMessage()
        msg["From"] = SMTP_USER
        msg["To"] = to_addr
        msg.set_content(body)
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as s:
            s.starttls()
            s.login(SMTP_USER, SMTP_PASS)
            s.send_message(msg)
        return True
    except Exception as e:
        print("SMTP error:", e)
        return False

def notify_error_sms(body: str):
    if not SMS_GATEWAY_ADDRESS:
        print("No SMS_GATEWAY_ADDRESS configured; skipping SMS.")
        return
    sent = False
    if MAILGUN_API_KEY and MAILGUN_DOMAIN:
        sent = send_sms_via_mailgun(SMS_GATEWAY_ADDRESS, body)
    if not sent and SMTP_HOST:
        sent = send_sms_via_smtp(SMS_GATEWAY_ADDRESS, body)
    if not sent:
        print("No SMS provider succeeded; SMS skipped.")

# ------------------------
# Main
# ------------------------
def main():
    start = time.time()
    collected = []
    failed_sources = []

    try:
        print("Fetching exchanges (NASDAQ/NYSE/AMEX)...")
        ex = fetch_exchanges_all()
        if not ex:
            failed_sources.append("exchanges")
        collected += ex

        print("Fetching OTC...")
        otc = fetch_otc_all()
        if not otc:
            failed_sources.append("otc")
        collected += otc

        print("Fetching Russell indexes...")
        r = fetch_russell_group()
        if not r:
            failed_sources.append("russell")
        collected += r

        print("Fetching S&P (500/400/600)...")
        sp = fetch_sp_group()
        if not sp:
            failed_sources.append("sp1500")
        collected += sp

        print("Fetching Dow (DJIA)...")
        dj = fetch_djia_all()
        if not dj:
            failed_sources.append("djia")
        collected += dj

        raw_count = len(collected)
        print(f"Raw collected: {raw_count}")

        # dedupe + filter
        seen = set()
        clean = []
        for s in collected:
            if not s:
                continue
            st = s.strip().upper()
            if len(st) > 12:
                continue
            if st in seen:
                continue
            if looks_like_etf(st):
                continue
            seen.add(st)
            clean.append(st)

        filtered_count = len(clean)
        print(f"Filtered count: {filtered_count}")

        # upsert to Supabase
        upsert_symbols_batch(clean)

        duration = time.time() - start
        write_import_stats("success", raw_count, filtered_count, None)

        # Slack success summary
        send_slack(f"✅ Import successful — Raw: {raw_count}  Final: {filtered_count}  Duration: {duration:.1f}s  Failed sources: {failed_sources if failed_sources else 'none'}")

        print("Import complete.")
    except Exception as exc:
        err_text = "".join(traceback.format_exception_only(type(exc), exc))
        print("Fatal error during import:", err_text)
        write_import_stats("failure", len(collected), 0, err_text)
        # send slack + sms
        send_slack(f"❌ Import FAILED: {err_text}\nFailed sources: {failed_sources}")
        notify_error_sms(f"Import FAILED: {err_text}\nFailed sources: {failed_sources}")
        raise

if __name__ == "__main__":
    main()
