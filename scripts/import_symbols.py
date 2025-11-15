# scripts/import_symbols.py
"""
Final importer:
- Wikipedia primary for indices (S&P 500/400/600 -> S&P1500, Russell 1000/2000/3000, DJIA).
- Russell fallback: try known GitHub mirrors if Wikipedia returns too few rows.
- Best-effort NASDAQTrader attempt (not required).
- Dedupe, ETF filter, upsert into Supabase `symbols`.
- Write run row into `import_stats`.
- Slack summary on success; Slack + SMS on failure (SMS only if MAILGUN or SMTP configured).
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
SMS_GATEWAY_ADDRESS = os.environ.get("SMS_GATEWAY_ADDRESS")  # e.g. 8322785054@tmomail.net

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

REQUEST_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0 Safari/537.36")
}

# ---------------------------
# Sources
# ---------------------------
WIKI_SP500 = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
WIKI_SP400 = "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies"
WIKI_SP600 = "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies"
WIKI_R1000 = "https://en.wikipedia.org/wiki/Russell_1000_Index"
WIKI_R2000 = "https://en.wikipedia.org/wiki/Russell_2000"
WIKI_R3000 = "https://en.wikipedia.org/wiki/Russell_3000_Index"
WIKI_DJIA = "https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average"

# Russell fallback mirrors (stable public mirrors)
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

# Best-effort NasdaqTrader (may be blocked/time out)
NASDAQTXT_HTTP = "http://ftp.nasdaqtrader.com/dynamic/SymbolDirectory/nasdaqlisted.txt"
OTHERLISTED_HTTP = "http://ftp.nasdaqtrader.com/dynamic/SymbolDirectory/otherlisted.txt"

# ---------------------------
# HTTP helper
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
        # fallback: per-line
        for line in text.splitlines():
            s = line.strip().split(",")[0].strip().upper()
            if s:
                out.append(s)
    return out

# ---------------------------
# Wikipedia fetch / parser using pandas.read_html with StringIO
# ---------------------------
def fetch_symbols_from_wikipedia(url: str) -> List[str]:
    html = safe_get_text(url)
    if not html:
        return []
    try:
        # wrap literal html text in StringIO to avoid pandas FutureWarning
        tables = pd.read_html(io.StringIO(html))
        for df in tables:
            cols = [str(c).lower() for c in df.columns]
            for candidate in ("symbol","ticker","ticker symbol","ticker(s)","ticker(s)"):
                if any(candidate in c for c in cols):
                    # find the column
                    for c in df.columns:
                        if candidate in str(c).lower():
                            try:
                                vals = df[c].astype(str).tolist()
                                syms = [v.split()[0].split('[')[0].strip().upper() for v in vals if v and str(v).strip() != ""]
                                # filter out header-like values
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
        print(f"pandas.read_html parsing failed for {url}: {e}")
        return []

# ---------------------------
# Russell-specific fetcher: wiki first, then mirrors
# ---------------------------
def fetch_russell_with_fallback(wiki_url: str, mirrors: List[str], expected_min: int = 1000) -> List[str]:
    syms = fetch_symbols_from_wikipedia(wiki_url)
    if syms and len(syms) >= min(10, expected_min//10):  # if wiki yields reasonable chunk, accept it
        return syms
    # else try mirrors
    for m in mirrors:
        txt = safe_get_text(m)
        if txt:
            parsed = parse_csv_symbols(txt, candidate_cols=("Symbol","symbol","Ticker","ticker"))
            if parsed:
                return parsed
    # final: return whatever wiki had (even small)
    return syms

# ---------------------------
# Upsert + logging
# ---------------------------
def upsert_symbols_batch(symbols: List[str]):
    batch = []
    for sym in symbols:
        if not sym or len(sym) > 12:
            continue
        batch.append({"symbol": sym, "is_valid": None, "source": "wikipedia-import"})
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
    if not SLACK_WEBHOOK_URL:
        print("Slack not configured; skipping Slack.")
        return
    try:
        requests.post(SLACK_WEBHOOK_URL, json={"text": msg}, timeout=10)
    except Exception as e:
        print("Slack send error:", e)

def send_sms_mailgun(to_addr: str, body: str) -> bool:
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
            timeout=15
        )
        return resp.status_code in (200, 201)
    except Exception as e:
        print("Mailgun send error:", e)
        return False

def send_sms_smtp(to_addr: str, body: str) -> bool:
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
        print("SMTP send error:", e)
        return False

def notify_error_sms(body: str):
    if not SMS_GATEWAY_ADDRESS:
        print("SMS_GATEWAY_ADDRESS not set; skipping SMS.")
        return
    sent = False
    if MAILGUN_API_KEY and MAILGUN_DOMAIN:
        sent = send_sms_mailgun(SMS_GATEWAY_ADDRESS, body)
    if not sent and SMTP_HOST:
        sent = send_sms_smtp(SMS_GATEWAY_ADDRESS, body)
    if not sent:
        print("No SMS provider succeeded; SMS skipped.")

# ---------------------------
# Main orchestration
# ---------------------------
def main():
    start = time.time()
    collected = []
    failed_sources = []

    try:
        # S&P 500/400/600 (Wikipedia primary)
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

        # Russell (wiki first, then mirrors)
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

        # Best-effort exchanges (optional)
        print("Attempting NASDAQ/otherlisted (best-effort)...")
        ex = []
        t = safe_get_text(NASDAQTXT_HTTP)
        if t:
            ex += parse_nasdaq_txt(t)
        t2 = safe_get_text(OTHERLISTED_HTTP)
        if t2:
            ex += parse_nasdaq_txt(t2)

        # Merge everything
        collected += sp1500 + r1000 + r2000 + r3000 + djia + ex

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
            # conservative ETF filter: skip if symbol contains ETF-like tokens (rare)
            if any(tok in st for tok in ETF_KEYWORDS):
                continue
            seen.add(st)
            normalized.append(st)

        filtered_count = len(normalized)
        print(f"Raw collected: {raw_count}; Final after filter: {filtered_count}")

        # Upsert into Supabase
        upsert_symbols_batch(normalized)

        write_import_stats("success", raw_count, filtered_count, None)

        duration = time.time() - start
        send_slack(f"✅ Import successful — Raw: {raw_count}  Final: {filtered_count}  Duration: {duration:.1f}s  Failed sources: {failed_sources if failed_sources else 'none'}")

        print("Import complete.")
    except Exception as exc:
        err_text = "".join(traceback.format_exception_only(type(exc), exc))
        print("Fatal error during import:", err_text)
        write_import_stats("failure", len(collected), 0, err_text)
        # Slack + SMS (errors only)
        send_slack(f"❌ Import FAILED: {err_text}\nFailed sources: {failed_sources}")
        notify_error_sms(f"Import FAILED: {err_text}\nFailed sources: {failed_sources}")
        raise

if __name__ == "__main__":
    main()
