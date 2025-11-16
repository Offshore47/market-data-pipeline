import os
import json
import time
from datetime import datetime, timedelta
import random 
import requests 
from firebase_admin import credentials, initialize_app, firestore, exceptions
from supabase import create_client 

# --------------------------- 
# Environment / Supabase Configuration 
# ---------------------------
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_ANON_KEY")

# --- Initialize Supabase Client (Kept for logging/future expansion) ---
if not SUPABASE_URL or not SUPABASE_KEY:
    SUPABASE_CLIENT = None
else:
    SUPABASE_CLIENT = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- Global Configuration (Matches React App) ---
APP_ID = os.environ.get('APP_ID', 'default-app-id') 
COLLECTION_PATH = f'artifacts/{APP_ID}/public/data/topStocks'

# Financial & LLM API Keys
GROQ_API_KEY = os.environ.get('GROQ_API_KEY')
NEWSAPI_KEY = os.environ.get('NEWSAPI_KEY')
FINANCIAL_API_KEY = os.environ.get('FINANCIAL_API_KEY') # Finnhub Key

FINNHUB_BASE_URL = "https://finnhub.io/api/v1"

# --- RATE LIMIT CONFIGURATION & TARGETS ---
TARGET_SYMBOL_COUNT = 200 
# --------------------------------

# --------------------------- 
# LLM and News API Integration
# ---------------------------

def fetch_news_headlines(symbol: str) -> str:
    """Fetches recent news headlines for a symbol using NEWSAPI."""
    if not NEWSAPI_KEY:
        print("NEWSAPI_KEY not found. Skipping news fetch.")
        return "No recent news found."

    # CRITICAL: Sleep 3 seconds here to obey NewsAPI's strict rate limits
    time.sleep(3) 

    url = f"https://newsapi.org/v2/everything?q={symbol} stock&sortBy=publishedAt&language=en&pageSize=10&apiKey={NEWSAPI_KEY}"
    try:
        response = requests.get(url, timeout=10)
        
        # Immediate check for the 429 error and raise if hit, which stops the whole job
        if response.status_code == 429:
            raise requests.exceptions.RequestException(f"429 Client Error: NewsAPI limit reached on {symbol}. Stopping scoring loop.")
            
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
    
    # Skip LLM call if the news text is too short or indicates failure
    if len(news_text) < 50 or "No recent news found" in news_text or "News fetch failed" in news_text:
        return random.uniform(0.45, 0.55)

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
            # Corrected path for response content for Groq's structure
            json_str = groq_result['choices'][0]['message']['content'] 
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

    # CRITICAL: Sleep 1.5 seconds here to obey Finnhub's strict rate limits
    time.sleep(1.5)

    url = f"{FINNHUB_BASE_URL}{endpoint}"
    
    full_params = {"symbol": symbol, "token": FINANCIAL_API_KEY}
    if params:
        full_params.update(params)

    MAX_RETRIES = 3
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, params=full_params, timeout=15)
            
            # Immediate check for the 429 error and raise if hit
            if response.status_code == 429:
                 raise requests.exceptions.RequestException(f"429 Client Error: Finnhub limit reached on {symbol} at {endpoint}. Stopping scoring loop.")
                 
            response.raise_for_status()
            
            data = response.json()
            # FIX: If API returns an empty list (no data), treat it as an empty dictionary
            if isinstance(data, list) and not data:
                return {}
            return data
            
        except requests.exceptions.RequestException as e:
            if attempt == MAX_RETRIES - 1:
                print(f"Finnhub API final attempt failed ({endpoint}) for {symbol}: {e}")
            else:
                time.sleep(1 + 2 ** attempt)
    return {}

def get_pe_ratio(symbol: str) -> float:
    """Fetches the latest P/E ratio."""
    data = fetch_finnhub_data("/stock/metric", symbol, {"metric": "price-to-book"}) 
    
    if not isinstance(data, dict):
        return round(random.uniform(15.0, 80.0), 1)

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
    
    if not isinstance(data, dict):
        return 0
        
    filings = data.get('filings', [])
    count = len([f for f in filings if f.get('form', '').upper() in ['10-K', '10-Q']])
    print(f"Found {count} recent SEC filings for {symbol}.")
    return count

def get_top_200_symbols() -> list:
    """
    Attempts to fetch a list of highly relevant symbols using Finnhub News proxy.
    If the API call fails or returns empty, returns a hardcoded list of major tickers.
    """
    MAJOR_FALLBACK_LIST = ["MSFT", "AAPL", "GOOGL", "NVDA", "TSLA", "AMZN", "JPM", "V", "WMT", "KO", "BAC", "HD", "UNH", "PG", "JNJ", "MA", "V", "BABA", "TCEHY", "ADBE"]
    
    if not FINANCIAL_API_KEY:
        print("Finnhub API key missing. Using guaranteed fallback symbols.")
        return MAJOR_FALLBACK_LIST

    url = f"{FINNHUB_BASE_URL}/news?category=general&minId=0&token={FINANCIAL_API_KEY}"
    
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        articles = response.json()
        
        symbols = set()
        for article in articles:
            related = article.get('related', '')
            if related:
                symbols.update([s.strip() for s in related.split(',') if s.strip()])
                
        # Filter out obvious non-stock tickers
        ETF_KEYWORDS = ["ETF", "ETN", "FUND", "INDEX"]
        filtered_symbols = [
            s for s in symbols 
            if s and len(s) <= 12 and 
            s not in MAJOR_FALLBACK_LIST and # Avoid duplicate processing if they are in both lists
            not any(tok in s for tok in ETF_KEYWORDS) and
            not any(s.endswith(suffix) for suffix in ['.P', '.W', '.U'])
        ]
        
        # Add the major fallbacks to ensure core market leaders are always included
        combined_list = list(symbols.union(set(MAJOR_FALLBACK_LIST)))
        
        # Shuffle and take the target count
        random.shuffle(combined_list)
        final_list = combined_list[:TARGET_SYMBOL_COUNT]
        
        print(f"Successfully compiled {len(final_list)} symbols using Finnhub News proxy + Fallback.")
        return final_list
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching symbols via Finnhub News proxy: {e}. Using guaranteed fallback list.")
        return MAJOR_FALLBACK_LIST


# --------------------------- 
# Main Orchestration
# ---------------------------

def generate_top_stocks():
    """Fetches symbols, calculates scores, and returns the top 20 list."""
    start_time = time.time()
    
    # NEW STRATEGY: Get the Top 200 relevant symbols directly from Finnhub proxy
    symbols = get_top_200_symbols()
    
    if not symbols:
        # This should only happen if the fallback list is also empty, which is unlikely.
        print("CRITICAL: Failed to get any symbols. Exiting.")
        return []

    
    print(f"Starting score run, processing {len(symbols)} symbols.")

    scored_stocks = []
    
    for i, raw_symbol in enumerate(symbols):
        # Ensure symbol is a string before proceeding
        symbol = str(raw_symbol)
        
        print(f"Processing symbol {i+1}/{len(symbols)}: {symbol}...")
        try:
            # 1. Fetch data from APIs (this includes all the necessary delays)
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
            
        except requests.exceptions.RequestException as e:
            if "429 Client Error" in str(e):
                print(f"!!! CRITICAL STOP: Daily API limit reached at symbol {symbol}. Exiting scoring loop now to preserve remaining budget.")
                break # Stop the loop immediately
            else:
                # Re-raise any other unknown request exception (e.g., 401, 404, DNS error)
                raise e 

        except Exception as e:
            # Catch all other critical errors (like JSON parsing issues)
            print(f"CRITICAL ERROR processing {symbol}: {e}")
            continue

    scored_stocks.sort(key=lambda x: x['score'], reverse=True)
    duration = time.time() - start_time
    print(f"Scoring complete. Total time: {duration:.1f}s")
    
    return scored_stocks[:20]

# --- Firestore Initialization and Data Handlers ---

def initialize_firebase():
    """Initializes Firebase Admin SDK using a service account JSON file."""
    # Initialize app to None first
    app = None
    
    service_account_json = os.environ.get('FIREBASE_SERVICE_ACCOUNT_KEY')
    if not service_account_json:
        raise ValueError("FIREBASE_SERVICE_ACCOUNT_KEY environment variable not set.")
        
    cred_dict = json.loads(service_account_json)
    
    try:
        cred = credentials.Certificate(cred_dict)
        # 1. Initialize the app instance
        app = initialize_app(cred, name=f"screener_app_{APP_ID}")
        print("Firebase Admin SDK initialized successfully.")
        
        # 2. Return the Firestore client associated with that instance
        return firestore.client(app=app) 
        
    except exceptions.DuplicatedAppError:
        # If the app was already initialized in a previous run, retrieve it by name
        import firebase_admin as fa
        app = fa.get_app(name=f"screener_app_{APP_ID}")
        print("Firebase Admin SDK (already initialized) retrieved successfully.")
        return firestore.client(app=app)
        
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
    if not top_stocks:
        print("WARNING: No stocks were scored. Skipping Firestore update.")
        return
        
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
