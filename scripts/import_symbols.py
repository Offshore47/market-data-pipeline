# scripts/import_symbols.py
"""
Full hybrid importer:
- Exchanges: NASDAQ, NYSE, AMEX, OTC
- Indexes: S&P 500/400/600 (S&P1500), Russell 1000/2000/3000, DJIA
- Sources: GitHub primary where available, Wikipedia fallback parsing
- Writes import_stats, upserts symbols
- Slack summary on success; Slack+SMS on errors (SMS only if Mailgun or SMTP creds provided)
"""

import csv
import time
import traceback
import requests
import os
from typing import List, Optional
from supabase import create_client

# optional HTML parser
try:
    from bs4 import BeautifulSoup
except Exception:
    BeautifulSoup = None

# ---------------------------
# Configuration (from env)
# ---------------------------
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_ANON_KEY")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")
SMS_GATEWAY_ADDRESS = os.environ.get("SMS_GATEWAY_ADDRESS")  # e.g. 8322785054@tmomail.net

# Mailgun (preferred for email->SMS)
MAILGUN_API_KEY = os.environ.get("MAILGUN_API_KEY")
MAILGUN_DOMAIN = os.environ.get("MAILGUN_DOMAIN")  # e.g. mg.example.com

# SMTP fallback
SMTP_HOST = os.environ.get("SMTP_HOST")
SMTP_PORT = int(os.environ.get("SMTP_PORT") or 0) if os.environ.get("SMTP_PORT") else None
SMTP_USER = os.environ.get("SMTP_USER")
SMTP_PASS = os.environ.get("SMTP_PASS")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("Missing SUPABASE_URL or SUPABASE_ANON_KEY in environment")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ---------------------------
# Sources
# ---------------------------
# NasdaqTrader
NASDAQLISTED = "https://ftp.nasdaqtrader.com/dynamic/SymbolDirectory/nasdaqlisted.txt"
OTHERLISTED = "https://ftp.nasdaqtrader.com/dynamic/SymbolDirectory/otherlisted.txt"

# GitHub fallbacks (community mirrors)
NASDAQ_FALLBACK = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/nasdaq/nasdaq.csv"
NYSE_FALLBACK   = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/nyse/nyse.csv"
AMEX_FALLBACK   = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/amex/amex.csv"
OTC_FALLBACK    = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/otc/otc.csv"

# Russell (R1)
RUSSELL_1000 = "https://raw.githubusercontent.com/datasets/russell-index/master/data/russell-1000.csv"
RUSSELL_2000 = "https://raw.githubusercontent.com/datasets/russell-index/master/data/russell-2000.csv"
RUSSELL_3000 = "https://raw.githubusercontent.com/datasets/russell-index/master/data/russell-3000.csv"

# S&P / Dow (github primary)
SP500_GH = "https://raw.githubusercontent.com/datasets/s-and-p-500/master/data/constituents.csv"
SP400_GH = "https://raw.githubusercontent.com/angeloashmore/sandp400/master/data/sandp400.csv"
SP600_GH = "https://raw.githubusercontent.com/holtzy/data_to_viz/master/Example_dataset/1000_SNP600.csv"
DOW_GH   = "https://raw.githubusercontent.com/datasets/dow-jones/master/data/dow-jones-index-components.csv"

# Wikipedia fallbacks
WIKI_SP500 = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
WIKI_SP400 = "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies"
WIKI_SP600 = "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies"
WIKI_DOW   = "https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average"
WIKI_RUSSELL_1000 = "https://en.wikipedia.org/wiki/Russell_1000_Index"
WIKI_RUSSELL_2000 = "https://en.wikipedia.org/wiki/Russell_2000"
WIKI_RUSSELL_3000 = "https://en.wikipedia.org/wiki/Russell_3000_Index"

# Behavior
RETRY_ATTEMPTS = 3
RETRY_DELAY = 2
BATCH_SIZE = 500
ETF_KEYWORDS = ["ETF", "ETN", "FUND", "TRUST", "INDEX", "EXCHANGE TRADED"]

# ---------------------------
# HTTP helper
# ---------------------------
def safe_get(url: str, retries: int = RETRY_ATTEMPTS, delay: int = RETRY_DELAY) -> Optional[str]:
    last = None
    for i in range(1, retries + 1):
        try:
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last = e
            print(f"[{i}/{retries}] GET {url} failed: {e}")
            time.sleep(delay)
    print(f"All attempts failed for {url}: {last}")
    return None

# ---------------------------
# Parsers
# ---------------------------
def parse_nasdaq_txt(text: str) -> List[str]:
    lines = text.splitlines()
    if not lines:
        return []
    header = lines[0].split("|")
    # detect symbol column
    idx = None
    for name in ("Symbol", "ACT Symbol", "NASDAQ Symbol", "CQS Symbol"):
        if name in header:
            idx = header.index(name)
            break
    if idx is None:
        idx = 0
    out = []
    for line in lines[1:-1]:
        parts = line.split("|")
        if len(parts) > idx:
            s = parts[idx].strip().upper()
            if s and s != "SYMBOL":
                out.append(s)
    return out

def parse_csv_symbols(text: str, cols=("Symbol", "symbol", "Ticker", "Code")) -> List[str]:
    out = []
    try:
        reader = csv.DictReader(text.splitlines())
        for row in reader:
            for c in cols:
                if c in row and row[c]:
                    out.append(row[c].strip().upper())
                    break
    except Exception as e:
        print("CSV parse error:", e)
    return out

def parse_wikipedia_table(html: str) -> List[str]:
    if BeautifulSoup is None:
        print("BeautifulSoup missing; Wikipedia fallback skipped.")
        return []
    soup = BeautifulSoup(html, "lxml")
    tables = soup.find_all("table")
    for table in tables:
        headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
        if any("symbol" in h or "ticker" in h for h in headers):
            # find index
            idx = None
            for i, h in enumerate(headers):
                if "symbol" in h or "ticker" in h:
                    idx = i
                    break
            if idx is None:
                continue
            syms = []
            rows = table.find_all("tr")
            for r in rows[1:]:
                cols = r.find_all(["td", "th"])
                if len(cols) > idx:
                    txt = cols[idx].get_text(strip=True)
                    token = txt.split()[0].split('[')[0]
                    if token:
                        syms.append(token.upper())
            return syms
    return []

# ---------------------------
# Fetch functions for universes
# ---------------------------
def fetch_exchanges() -> List[str]:
    sym = []
    t = safe_get(NASDAQLISTED)
    if t:
        sym += parse_nasdaq_txt(t)
    t2 = safe_get(OTHERLISTED)
    if t2:
        sym += parse_nasdaq_txt(t2)
    # fallback small mirrors (if required)
    t3 = safe_get(NASDAQ_FALLBACK)
    if t3:
        sym += parse_csv_symbols(t3)
    t4 = safe_get(NYSE_FALLBACK)
    if t4:
        sym += parse_csv_symbols(t4)
    t5 = safe_get(AMEX_FALLBACK)
    if t5:
        sym += parse_csv_symbols(t5)
    return sym

def fetch_otc() -> List[str]:
    out = []
    t1 = safe_get(OTC_FALLBACK)
    if t1:
        out += parse_csv_symbols(t1, cols=("Symbol","symbol","Ticker","Code"))
    return out

def fetch_russells() -> List[str]:
    out = []
    for url in (RUSSELL_1000, RUSSELL_2000, RUSSELL_3000):
        t = safe_get(url)
        if t:
            out += parse_csv_symbols(t, cols=("symbol","Symbol","Ticker","ticker"))
    # fallback via wiki
    if not out and BeautifulSoup is not None:
        t = safe_get(WIKI_RUSSELL_3000)
        if t:
            out += parse_wikipedia_table(t)
    return out

def fetch_sp_and_dow() -> List[str]:
    out = []
    t = safe_get(SP500_GH)
    if t:
        out += parse_csv_symbols(t, cols=("Symbol","symbol","Ticker","ticker"))
    else:
        t = safe_get(WIKI_SP500)
        if t:
            out += parse_wikipedia_table(t)

    t = safe_get(SP400_GH)
    if t:
        out += parse_csv_symbols(t, cols=("Symbol","symbol","Ticker"))
    else:
        t = safe_get(WIKI_SP400)
        if t:
            out += parse_wikipedia_table(t)

    t = safe_get(SP600_GH)
    if t:
        out += parse_csv_symbols(t, cols=("Symbol","symbol","Ticker"))
    else:
        t = safe_get(WIKI_SP600)
        if t:
            out += parse_wikipedia_table(t)

    t = safe_get(DOW_GH)
    if t:
        out += parse_csv_symbols(t, cols=("Symbol","symbol","Ticker"))
    else:
        t = safe_get(WIKI_DOW)
        if t:
            out += parse_wikipedia_table(t)
    return out

# ---------------------------
# Utilities: filters, upsert, logging
# ---------------------------
def looks_like_etf(sym: str) -> bool:
    if not sym:
        return False
    s = sym.upper()
    if any(k in s for k in ETF_KEYWORDS):
        return True
    return False

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

def write_import_stats(status: str, fetched: int, filtered: int, error_msg: Optional[str]):
    payload = {
        "fetched_count": fetched,
        "filtered_count": filtered,
        "status": status,
        "error": error_msg
    }
    try:
        supabase.table("import_stats").insert(payload).execute()
    except Exception as e:
        print("Failed to write import_stats:", e)

# ---------------------------
# Notifications
# ---------------------------
def send_slack(text: str):
    if not SLACK_WEBHOOK_URL:
        print("No Slack webhook configured.")
        return
    try:
        requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=10)
    except Exception as e:
        print("Slack send failed:", e)

def send_sms_via_mailgun(to_address: str, body: str) -> bool:
    if not MAILGUN_API_KEY or not MAILGUN_DOMAIN:
        return False
    try:
        resp = requests.post(
            f"https://api.mailgun.net/v3/{MAILGUN_DOMAIN}/messages",
            auth=("api", MAILGUN_API_KEY),
            data={
                "from": f"market-pipeline@{MAILGUN_DOMAIN}",
                "to": [to_address],
                "subject": "",
                "text": body
            },
            timeout=15
        )
        return resp.status_code in (200, 201)
    except Exception as e:
        print("Mailgun SMS error:", e)
        return False

def send_sms_via_smtp(to_address: str, body: str) -> bool:
    if not (SMTP_HOST and SMTP_PORT and SMTP_USER and SMTP_PASS):
        return False
    try:
        import smtplib
        from email.message import EmailMessage
        msg = EmailMessage()
        msg["From"] = SMTP_USER
        msg["To"] = to_address
        msg.set_content(body)
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as s:
            s.starttls()
            s.login(SMTP_USER, SMTP_PASS)
            s.send_message(msg)
        return True
    except Exception as e:
        print("SMTP SMS error:", e)
        return False

def notify_error_sms(body: str):
    if not SMS_GATEWAY_ADDRESS:
        print("SMS_GATEWAY_ADDRESS not set; skipping SMS.")
        return
    sent = False
    if MAILGUN_API_KEY and MAILGUN_DOMAIN:
        sent = send_sms_via_mailgun(SMS_GATEWAY_ADDRESS, body)
    if not sent and SMTP_HOST:
        sent = send_sms_via_smtp(SMS_GATEWAY_ADDRESS, body)
    if not sent:
        print("SMS not sent (no working provider configured).")

# ---------------------------
# Main
# ---------------------------
def main():
    start = time.time()
    collected = []
    failed_sources = []

    try:
        # Exchanges
        print("Fetching exchanges (NASDAQ/NYSE/AMEX)...")
        ex = fetch_exchanges()
        if not ex:
            failed_sources.append("exchanges")
        collected += ex

        # OTC
        print("Fetching OTC...")
        otc = fetch_otc()
        if not otc:
            failed_sources.append("otc")
        collected += otc

        # Russell
        print("Fetching Russell indexes...")
        r = fetch_russells()
        if not r:
            failed_sources.append("russell")
        collected += r

        # S&P and Dow
        print("Fetching S&P / Dow...")
        spd = fetch_sp_and_dow()
        if not spd:
            failed_sources.append("sp/dow")
        collected += spd

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

        # upsert
        upsert_batch(clean)

        duration = time.time() - start
        write_import_stats("success", raw_count, filtered_count, None)

        # success Slack summary
        send_slack(f"✅ Import successful — Raw: {raw_count}  Final: {filtered_count}  Duration: {duration:.1f}s  Failed sources: {failed_sources if failed_sources else 'none'}")

        print("Import complete.")

    except Exception as exc:
        err_text = "".join(traceback.format_exception_only(type(exc), exc))
        print("Fatal error during import:", err_text)
        # log failure
        write_import_stats("failure", len(collected), 0, err_text)
        # slack + sms
        send_slack(f"❌ Import FAILED: {err_text}\nFailed sources: {failed_sources}")
        notify_error_sms(f"Import FAILED: {err_text}\nFailed sources: {failed_sources}")
        raise

if __name__ == "__main__":
    main()
