# market-data-pipeline
# Market Data Pipeline (Free, Automated Stock Intelligence)

This project is a fully automated, 100% free stock-data intelligence system built with:

- **Supabase** (database + API)
- **Python** (data collection + validation + analysis)
- **GitHub Actions** (cloud automation)
- **Free market data sources only**
- **No analyst ratings, no paid APIs**

The system collects all U.S. stock symbols (NYSE, NASDAQ, AMEX, + OTC), validates them, monitors market activity, scores sentiment, and prepares data for an upcoming web-based subscription dashboard.

---

## 🚀 Features

### **Symbol Acquisition**
- Fetches symbol lists from free sources:
  - NASDAQ + NYSE + AMEX
  - OTC & penny stocks
- Filters out ETFs/ETNs
- Stores to Supabase in table: `symbols`

### **Symbol Validation**
- Validates using:
  - `yfinance` (price check)
  - Finnhub (free tier symbol lookup)
- Verifies activity using:
  - Price > 0
  - Volume > 0
- Marks `is_valid` = TRUE/FALSE in Supabase

### **Sentiment Engine**
- Placeholder random scores (for now)
- Writes both:
  - Current sentiment → `symbols`
  - History → `sentiment_history`

### **Market Activity Monitor**
- Detects:
  - Volume surges
  - Price movements
  - Sector correlation / divergence

### **Automation**
All scripts run in the cloud via **GitHub Actions**, including:

- Daily symbol import
- Daily validation
- Continuous sentiment collection
- Market activity scanning

No local execution needed.

---

## 📂 Project Structure

