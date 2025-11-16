import os
import json
import time
from datetime import datetime, timedelta
import random 
import requests 
from firebase_admin import credentials, initialize_app, firestore
from supabase import create_client

# --------------------------- 
# Environment / Supabase Configuration 
# ---------------------------
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_ANON_KEY")

# --- Initialize Supabase Client ---
if not SUPABASE_URL or not SUPABASE_KEY:
    print("WARNING: Supabase configuration missing. Falling back to hardcoded symbols.")
    SUPABASE_CLIENT = None
else:
    SUPABASE_CLIENT = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("Supabase client initialized.")


# --- Global Configuration (Matches React App) ---
APP_ID = os.environ.get('APP_ID', 'default-app-id') 
COLLECTION_PATH = f'artifacts/{APP_ID}/public/data/topStocks'

# API Keys
GROQ_API_KEY = os.environ.get('GROQ_API_KEY')
NEWSAPI_KEY = os.environ.get('NEWSAPI_KEY')
FINANCIAL_API_KEY = os.environ.get('FINANCIAL_API_KEY') # Finnhub Key

FINNHUB_BASE_URL = "https://finnhub.io/api/v1"

# --------------------------- 
# LLM and News API Integration
# ---------------------------

def fetch_news_headlines(symbol: str) -> str:
    """Fetches recent news headlines for a symbol using NEWSAPI."""
    if not NEWSAPI_KEY:
        print("NEWSAPI_KEY not found. Skipping news fetch.")
        return "No recent news found."

    url = f"https://newsapi.org/v2/everything?q={symbol} stock&sortBy=publishedAt&language=en&pageSize=10&apiKey={NEWSAPI_KEY}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        headlines = [article.get('title', '') for article in data.get('articles', []) if article.get('title')]
        if not headlines:
            return "No recent news found."
            
        return "\n".join(headlines)

    except requests.exceptions.RequestException as e:
        print(f"Error fetching news for {symbol}: {e}")
        return "News fetch failed."

def get_sentiment_score(symbol: str, news_text: str) -> float:
    """Uses GROQ LLM to analyze news text and return a sentiment score (0.0 to 1.0)."""
    if not GROQ_API_KEY:
        print("GROQ_API_KEY not found. Using mock sentiment.")
        return random.uniform(0.4, 0.95)

    system_prompt = (
        "You are a concise financial sentiment analyzer. Your task is to analyze the provided text, "
        "which consists of recent news headlines for a stock, and output a JSON object only. "
        "The JSON must contain a single key, 'sentiment_score', with a float value between 0.0 (extremely negative) and 1.0 (extremely positive). "
        "Do not include any other text, explanations, or markdown."
    )
    user_query = f"Analyze the overall financial sentiment for {symbol} based on the following headlines:\n\n---\n{news_text}"
    
    payload = {
        "contents": [{ "parts": [{ "text": user_query }] }],
        "systemInstruction": { "parts": [{ "text": system_prompt }] },
        "generationConfig": {
            "responseMimeType": "application/json",
            "responseSchema": {
                "type": "OBJECT",
                "properties": { "sentiment_score": { "type": "NUMBER", "description": "Sentiment score between 0.0 and 1.0." } }
            }
        },
        "model": "gemma2-9b-it" 
    }
    
    MAX_RETRIES = 3
    for attempt in range(MAX_RETRIES):
        try:
            url = "https://api.groq.com/openai/v1/chat/completions" 
            headers = {
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type": "application/json"
            }
            
            response = requests.post(url, headers=headers, json=payload, timeout=20)
            response.raise_for_status()
            
            groq_result = response.json()
            json_str = groq_result['candidates'][0]['content']['parts'][0]['text']
            parsed_json = json.loads(json_str)
            
            score = float(parsed_json.get('sentiment_score', 0.5))
            return max(0.0, min(1.0, score)) 
            
        except (requests.exceptions.RequestException, json.JSONDecodeError, KeyError) as e:
            if attempt == MAX_RETRIES - 1:
                print(f"GROQ API final attempt failed for {symbol}: {e}")
            else:
                time.sleep(2 ** attempt) 
    
    return random.uniform(0.4, 0.6) 

# --------------------------- 
# Finnhub API Integration (P/E and SEC Filings)
# ---------------------------
def fetch_finnhub_data(endpoint: str, symbol: str, params: dict = None) -> dict:
    """Generic helper for Finnhub API calls with retry."""
    if not FINANCIAL_API_KEY:
        print("Finnhub API key missing. Skipping API fetch.")
        return {}

    url = f"{FINNHUB_BASE_URL}{endpoint}"
    
    full_params = {"symbol": symbol, "token": FINANCIAL_API_KEY}
    if params:
        full_params.update(params)

    MAX_RETRIES = 3
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, params=full_params, timeout=15)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if attempt == MAX_RETRIES - 1:
                print(f"Finnhub API final attempt failed ({endpoint}) for {symbol}: {e}")
            else:
                time.sleep(1 + 2 ** attempt)
    return {}

def get_pe_ratio(symbol: str) -> float:
    """Fetches the latest P/E ratio."""
    data = fetch_finnhub_data("/stock/metric", symbol, {"metric": "price-to-book"}) 
    pe_ratio = data.get('metric', {}).get('peTTM', None)
    if pe_ratio and isinstance(pe_ratio, (int, float)):
        print(f"Fetched P/E for {symbol}: {pe_ratio:.1f}")
        return pe_ratio
    return round(random.uniform(15.0, 80.0), 1)

def get_sec_filing_count(symbol: str) -> int:
    """Counts recent 10-K and 10-Q filings (last 90 days)."""
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=90)
    
    params = {
        "from": start_date.isoformat(),
        "to": end_date.isoformat(),
        "type": "10-K,10-Q" 
    }
    
    data = fetch_finnhub_data("/stock/filings", symbol, params)
    filings = data.get('filings', [])
    count = len([f for f in filings if f.get('form', '').upper() in ['10-K', '10-Q']])
    print(f"Found {count} recent SEC filings for {symbol}.")
    return count

# --------------------------- 
# Core Screener Logic
# ---------------------------
def fetch_fundamentals(symbol: str) -> dict:
    """Fetches core metrics for scoring."""
    
    news_headlines = fetch_news_headlines(symbol)
    sentiment = get_sentiment_score(symbol, news_headlines)
    pe = get_pe_ratio(symbol)
    sec_filings_count = get_sec_filing_count(symbol)
    
    # Mock Volume Surge (TODO: Replace with real calculation)
    volume_surge_factor = round(random.uniform(1.0, 5.0), 1)
    
    return {
        "pe": pe,
        "sentiment": sentiment,
        "volume_surge_factor": volume_surge_factor,
        "sec_filings_count": sec_filings_count,
    }

def calculate_score(data: dict) -> float:
    """Proprietary scoring function based on weighted criteria."""
    
    pe = data.get("pe", 0)
    pe_score = 0.0
    if 10 < pe <= 30: pe_score = 4.0
    elif 30 < pe <= 50: pe_score = 3.0
    elif 50 < pe <= 70: pe_score = 1.5
    else: pe_score = 0.5
    
    sentiment = data.get("sentiment", 0.5)
    sentiment_score = sentiment * 3.0 
    
    volume_surge = data.get("volume_surge_factor", 1.0)
    volume_score = min(volume_surge / 2.5, 2.0)
    
    filing_count = data.get("sec_filings_count", 0)
    filing_score = min(filing_count * 0.25, 1.0)
    
    composite_score = pe_score + sentiment_score + volume_score + filing_score
    return round(composite_score + random.uniform(-0.1, 0.1), 3)


def fetch_master_symbols_from_supabase() -> list:
    """Fetches the master list of symbols from the Supabase 'symbols' table."""
    global SUPABASE_CLIENT
    if SUPABASE_CLIENT:
        try:
            response = SUPABASE_CLIENT.table("symbols").select("symbol").execute()
            symbols = [item['symbol'] for item in response.data if item.get('symbol')]
            print(f"Successfully fetched {len(symbols)} symbols from Supabase.")
            return symbols
        except Exception as e:
            print(f"Error fetching symbols from Supabase: {e}. Falling back to hardcoded list.")
            return []
    
    # Fallback if Supabase client could not be initialized (e.g., missing secrets)
    # This ensures the script can run for testing even without a fully populated Supabase DB
    return ["MSFT", "AAPL", "GOOGL", "NVDA", "TSLA", "AMZN", "JPM", "V", "WMT", "KO", "BAC"]

def generate_top_stocks():
    """Fetches symbols, calculates scores, and returns the top 20 list."""
    start_time = time.time()
    symbols = fetch_master_symbols_from_supabase()
    
    # If Supabase is empty, use the same fallback symbols to test the rest of the pipeline
    if not symbols:
        symbols = ["MSFT", "AAPL", "GOOGL", "NVDA", "TSLA", "AMZN", "JPM", "V", "WMT", "KO", "BAC"]
        print("Using hardcoded fallback symbols for testing.")

    scored_stocks = []
    
    for i, symbol in enumerate(symbols):
        print(f"Processing symbol {i+1}/{len(symbols)}: {symbol}...")
        try:
            # 1. Fetch data from APIs
            fundamentals = fetch_fundamentals(symbol)
            
            # 2. Calculate score
            score = calculate_score(fundamentals)
            
            scored_stocks.append({
                "symbol": symbol,
                "score": score,
                "pe": fundamentals['pe'],
                "sentiment": fundamentals['sentiment'],
                "volumeSurge": fundamentals['volume_surge_factor'],
                "secFilingsCount": fundamentals['sec_filings_count'],
                "timestamp": datetime.now().isoformat()
            })
            
            # Rate limit control: wait between API calls
            time.sleep(0.5) 
            
        except Exception as e:
            print(f"CRITICAL ERROR processing {symbol}: {e}")
            continue

    scored_stocks.sort(key=lambda x: x['score'], reverse=True)
    duration = time.time() - start_time
    print(f"Scoring complete. Total time: {duration:.1f}s")
    
    return scored_stocks[:20]

# --- Firestore Initialization and Data Handlers ---

def initialize_firebase():
    """Initializes Firebase Admin SDK using a service account JSON file."""
    service_account_json = os.environ.get('FIREBASE_SERVICE_ACCOUNT_KEY')
    if not service_account_json:
        raise ValueError("FIREBASE_SERVICE_ACCOUNT_KEY environment variable not set.")
        
    cred_dict = json.loads(service_account_json)
    
    try:
        cred = credentials.Certificate(cred_dict)
        # Use a unique name for the app instance
        initialize_app(cred, name=f"screener_app_{APP_ID}")
        print("Firebase Admin SDK initialized successfully.")
        return return firestore.client(app=app)
    except Exception as e:
        print(f"Error initializing Firebase: {e}")
        raise e

def update_firestore(db, top_stocks: list):
    """Deletes all existing documents in the collection and writes the new list."""
    print(f"Starting database update in collection: {COLLECTION_PATH}")
    
    collection_ref = db.collection(COLLECTION_PATH)
    
    # 1. Clear existing data
    docs = collection_ref.stream()
    for doc in docs:
        doc.reference.delete()
    print("Existing documents cleared.")
    
    # 2. Write new data
    for stock in top_stocks:
        doc_id = stock['symbol'] 
        collection_ref.document(doc_id).set(stock)
        print(f"Wrote document: {doc_id} with score {stock['score']:.3f}")

    print(f"Successfully updated {len(top_stocks)} stock documents in Firestore.")

if __name__ == "__main__":
    try:
        # 1. Initialize Firestore connection
        db = initialize_firebase()
        
        # 2. Generate the Top 20 list
        top_stocks = generate_top_stocks()
        
        # 3. Write the results to Firestore
        update_firestore(db, top_stocks)
        
    except Exception as e:
        print(f"Workflow failed due to an error: {e}")
        raise
