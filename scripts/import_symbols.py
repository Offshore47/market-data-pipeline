import csv
import requests
import os
from supabase import create_client
import json

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_ANON_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("Missing SUPABASE_URL or SUPABASE_ANON_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- Data sources (R1 hybrid extraction) ---
NASDAQ_URL = "https://github.com/rreichel3/US-Stock-Symbols/raw/main/nasdaq/nasdaq.csv"
NYSE_URL   = "https://github.com/rreichel3/US-Stock-Symbols/raw/main/nyse/nyse.csv"
AMEX_URL   = "https://github.com/rreichel3/US-Stock-Symbols/raw/main/amex/amex.csv"
OTC_URL    = "https://github.com/rreichel3/US-Stock-Symbols/raw/main/otc/otc.csv"

ETF_KEYWORDS = ["ETF", "ETN", "TRUST", "FUND", "INDEX", "EXCHANGE TRADED"]

def send_slack(text):
    """Send a message to Slack webhook."""
    if not SLACK_WEBHOOK_URL:
        print("No Slack webhook set; skipping Slack alert.")
        return

    try:
        requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=10)
    except Exception as e:
        print("Slack send failed:", e)

def fetch_csv_symbols(url):
    symbols = []
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        reader = csv.DictReader(r.text.splitlines())
        for row in reader:
            sym = row.get("Symbol") or row.get("symbol")
            if sym:
                symbols.append(sym.strip().upper())
    except Exception as e:
        print(f"Failed fetch {url}: {e}")
    return symbols

def is_etf(sym):
    s = sym.upper()
    return any(k in s for k in ETF_KEYWORDS)

def upsert_batch(symbols):
    batch = []
    for sym in symbols:
        batch.append({"symbol": sym, "source": "importer"})
        if len(batch) >= 500:
            supabase.table("symbols").upsert(batch).execute()
            batch = []
    if batch:
        supabase.table("symbols").upsert(batch).execute()

def main():
    try:
        print("Fetching lists...")
        all_syms = []
        all_syms += fetch_csv_symbols(NASDAQ_URL)
        all_syms += fetch_csv_symbols(NYSE_URL)
        all_syms += fetch_csv_symbols(AMEX_URL)
        all_syms += fetch_csv_symbols(OTC_URL)

        uniq = sorted(set(all_syms))
        before = len(uniq)

        filtered = [s for s in uniq if not is_etf(s)]
        after = len(filtered)

        print(f"Fetched: {before}, After filtering: {after}")

        upsert_batch(filtered)

        send_slack(f"""
✅ *Stock Symbol Import Completed*
• Raw fetched: *{before}*
• After filtering: *{after}*
• Successful upsert into Supabase.
""")

    except Exception as e:
        send_slack(f"❌ *Import FAILED:* {str(e)}")
        raise

if __name__ == "__main__":
    main()
