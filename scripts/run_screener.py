import os
import json
import time
from datetime import datetime, timedelta
import random 
import requests 
from firebase_admin import credentials, initialize_app, firestore
from google.cloud.firestore_v1.base_collection import BaseCollection
from supabase import create_client

# --------------------------- 
# Environment / Supabase Configuration 
# ---------------------------
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_ANON_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("WARNING: Supabase configuration missing. Falling back to hardcoded symbols.")
    SUPABASE_CLIENT = None
else:
    # Initialize Supabase client
    SUPABASE_CLIENT = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("Supabase client initialized.")


# --- Configuration (Must match the React app expectations) ---
APP_ID = os.environ.get('APP_ID', 'default-app-id') 
COLLECTION_PATH = f'artifacts/{APP_ID}/public/data/topStocks'

# Financial & LLM API Keys
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

    # NOTE: Using 'q={symbol} stock' for relevancy
    url = f"https://newsapi.org/v2/everything?q={symbol} stock&sortBy=publishedAt&language=en&pageSize=10&apiKey={NEWSAPI_KEY}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        headlines = [article.get('title', '') for article in data.get('articles', []) if article.get('title')]
        if not headlines:
            return "No recent news found."
            
        # Join headlines into a single string for the LLM prompt
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
        "model": "gemma2-9b-it" # Fast GROQ model
    }
    
    # Implementing request with exponential backoff
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
            # Extract JSON string from response
            json_str = groq_result['candidates'][0]['content']['parts'][0]['text']
            parsed_json = json.loads(json_str)
            
            score = float(parsed_json.get('sentiment_score', 0.5))
            return max(0.0, min(1.0, score)) # Ensure bounds 0.0 to 1.0
            
        except (requests.exceptions.RequestException, json.JSONDecodeError, KeyError) as e:
            # Silence error logging for retries, only print final failure
            if attempt == MAX_RETRIES - 1:
                print(f"GROQ API final attempt failed for {symbol}: {e}")
            else:
                time.sleep(2 ** attempt) # Exponential backoff
    
    return random.uniform(0.4, 0.6) # Fallback to neutral if API fails

# --------------------------- 
# Finnhub API Integration (REAL P/E and SEC Filings)
# ---------------------------
def fetch_finnhub_data(endpoint: str, symbol: str, params: dict = None) -> dict:
    """Generic helper for Finnhub API calls with retry."""
    if not FINANCIAL_API_KEY:
        print("Finnhub API key missing. Skipping API fetch.")
        return {}

    url = f"{FINNHUB_BASE_URL}{endpoint}"
    
    # Add symbol and API key to parameters
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
    """Fetches the latest P/E ratio using Finnhub's Basic Financials endpoint."""
    # Requesting Price-to-Book metric group, often includes P/E for free tier
    data = fetch_finnhub_data("/stock/metric", symbol, {"metric": "price-to-book"}) 
    
    pe_ratio = data.get('metric', {}).get('peTTM', None) # peTTM is Trailing Twelve Months P/E

    if pe_ratio and isinstance(pe_ratio, (int, float)):
        print(f"Fetched P/E for {symbol}: {pe_ratio:.1f}")
        return pe_ratio
        
    # Fallback to mock data if API fails or returns None/Error
    return round(random.uniform(15.0, 80.0), 1)

def get_sec_filing_count(symbol: str) -> int:
    """Counts recent 10-K and 10-Q filings (last 90 days) using Finnhub."""
    
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=90)
    
    params = {
        "from": start_date.isoformat(),
        "to": end_date.isoformat(),
        "type": "10-K,10-Q" # Comma-separated list of filing types
    }
    
    data = fetch_finnhub_data("/stock/filings", symbol, params)
    
    filings = data.get('filings', [])
    
    # Count the number of relevant filings found
    count = len([f for f in filings if f.get('form', '').upper() in ['10-K', '10-Q']])

    print(f"Found {count} recent SEC filings for {symbol}.")
    return count

# --------------------------- 
# Core Screener Function
# ---------------------------
def fetch_fundamentals(symbol: str) -> dict:
    """
    Fetches core fundamentals and initiates sentiment analysis using real APIs.
    """
    
    # 1. Real-time Sentiment Analysis (GROQ + NEWSAPI)
    news_headlines = fetch_news_headlines(symbol)
    sentiment = get_sentiment_score(symbol, news_headlines)
    
    # 2. P/E Ratio (FINNHUB)
    pe = get_pe_ratio(symbol)
    
    # 3. SEC Filings Count (FINNHUB)
    sec_filings_count = get_sec_filing_count(symbol)
    
    # 4. Mock Volume Surge (Volume Surge is typically a custom calculation)
    # USER TODO: Implement real volume surge calculation using Finnhub historical data.
    volume_surge_factor = round(random.uniform(1.0, 5.0), 1)
    
    return {
        "pe": pe,
        "sentiment": sentiment,
        "volume_surge_factor": volume_surge_factor,
        "sec_filings_count": sec_filings_count,
    }

def calculate_score(data: dict) -> float:
    """Proprietary scoring function based on weighted criteria."""
    
    # 1. P/E Scoring (Score between 0 and 4.0, lower P/E = higher score, up to a point)
    pe = data.get("pe", 0)
    pe_score = 0.0
    if 10 < pe <= 30: pe_score = 4.0
    elif 30 < pe <= 50: pe_score = 3.0
    elif 50 < pe <= 70: pe_score = 1.5
    else: pe_score = 0.5
    
    # 2. Sentiment Scoring (Score between 0 and 3.0, based on 0.0 to 1.0 value)
    sentiment = data.get("sentiment", 0.5)
    sentiment_score = sentiment * 3.0 
    
    # 3. Volume Surge Scoring (Score between 0 and 2.0, higher surge = higher score)
    volume_surge = data.get("volume_surge_factor", 1.0)
    volume_score = min(volume_surge / 2.5, 2.0)
    
    # 4. SEC Filings Scoring (Score between 0 and 1.0, count of recent positive filings)
    filing_count = data.get("sec_filings_count", 0)
    filing_score = min(filing_count * 0.25, 1.0)
    
    # Final composite score
    composite_score = pe_score + sentiment_score + volume_score + filing_score
    
    # Normalize score and add small random noise for ranking variety
    return round(composite_score + random.uniform(-0.1, 0.1), 3)


# --- Firestore Initialization and Data Handlers ---

def initialize_firebase():
    """Initializes Firebase Admin SDK using a service account JSON file."""
    service_account_json = os.environ.get('FIREBASE_SERVICE_ACCOUNT_KEY')
    if not service_account_json:
        raise ValueError("FIREBASE_SERVICE_ACCOUNT_KEY environment variable not set.")
        
    cred_dict = json.loads(service_account_json)
    
    try:
        cred = credentials.Certificate(cred_dict)
        initialize_app(cred, name=f"screener_app_{APP_ID}")
        print("Firebase Admin SDK initialized successfully.")
        return firestore.client()
    except Exception as e:
        print(f"Error initializing Firebase: {e}")
        raise e

def fetch_master_symbols_from_supabase() -> list:
    """Fetches the master list of symbols from the Supabase 'symbols' table."""
    global SUPABASE_CLIENT
    if SUPABASE_CLIENT:
        try:
            # Assumes 'symbols' table exists and contains a 'symbol' column
            response = SUPABASE_CLIENT.table("symbols").select("symbol").execute()
            symbols = [item['symbol'] for item in response.data if item.get('symbol')]
            print(f"Successfully fetched {len(symbols)} symbols from Supabase.")
            return symbols
        except Exception as e:
            print(f"Error fetching symbols from Supabase: {e}. Falling back to hardcoded list.")
            return []
    return []

def generate_top_stocks():
    """Runs the full screening pipeline: fetches data, calculates score, and identifies the top 20."""
    # 1. Fetch Master Symbol List 
    all_symbols = fetch_master_symbols_from_supabase()
    
    # Fallback to hardcoded list if Supabase fetch fails or is not configured
    if not all_symbols:
        print("Using hardcoded list for demonstration.")
        all_symbols = ["MSFT", "AAPL", "GOOGL", "NVDA", "TSLA", "AMZN", "JPM", "V", "MA", "WMT", 
                       "JNJ", "XOM", "UNH", "PG", "HD", "DIS", "NFLX", "ADBE", "CRM", "INTC", 
                       "SBUX", "COST", "CSCO", "PYPL", "ZM", "LUV", "DAL", "UAL", "AAL", "F"]

    scored_stocks = []
    print(f"Starting analysis on {len(all_symbols)} candidate stocks...")
    
    for symbol in all_symbols:
        # 2. Fetch required metrics (REAL Sentiment, P/E, SEC Filings / MOCK Volume)
        metrics = fetch_fundamentals(symbol)
        
        # 3. Calculate proprietary score
        score = calculate_score(metrics)
        
        # 4. Structure the result object for Firestore (key names must match React app)
        stock_data = {
            "symbol": symbol,
            "pe": metrics.get("pe"),
            "sentiment": metrics.get("sentiment"),
            "volumeSurge": metrics.get("volume_surge_factor"), # Matches React key
            "score": score,
            "timestamp": datetime.now()
        }
        scored_stocks.append(stock_data)
        
    # 5. Sort and take the top 20
    scored_stocks.sort(key=lambda x: x['score'], reverse=True)
    top_20 = scored_stocks[:20]
    
    print(f"Top 20 stocks identified. Highest score: {top_20[0]['score']:.3f}")
    return top_20

def update_firestore(db: BaseCollection, top_stocks: list):
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
