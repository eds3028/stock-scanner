# ASX Stock Scanner

A nightly ASX-200 stock screener that scores every stock across five dimensions — Value, Future, Past, Health, and Dividends — using a Simply Wall St–style 30-point model. Results are surfaced in a dark-mode Streamlit dashboard with optional AI-generated narratives via a local Ollama instance.

> **What this is:** A personal research tool that automates the tedious first pass of screening ASX stocks. It surfaces candidates worth investigating further.
>
> **What this is not:** Financial advice. Scores are purely mechanical — they do not account for qualitative factors, management quality, regulatory risk, or anything not captured in the public data fields returned by the configured providers. Always do your own due diligence.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Docker container                                           │
│                                                             │
│  scheduler.py  ──nightly 20:00 AWST──▶  scanner.py         │
│                                              │              │
│                                    DataOrchestrator         │
│                                    (orchestrator.py)        │
│                                              │              │
│                          ┌───────────────────┼───────────┐  │
│                          ▼           ▼       ▼           ▼  │
│                    YahooQuery  Finnhub   FMP   AlphaVantage  │
│                    (free)      (key)    (key)    (key)       │
│                          └───────────────────┴───────────┘  │
│                                              │              │
│                                       scorer.py             │
│                                       (30 checks)           │
│                                              │              │
│                                   ┌──────────▼──────────┐   │
│                                   │   SQLite /data/      │   │
│                                   │   stocks.db          │   │
│                                   └──────────┬──────────┘   │
│                                              │              │
│  dashboard.py  ◀──────── Streamlit :8501 ────┘              │
│                                                             │
│  Ollama (optional, external) ──▶ AI narratives              │
└─────────────────────────────────────────────────────────────┘
```

**Data flow:**

1. `scheduler.py` fires `scanner.py` at 20:00 AWST each night (and immediately on first boot if no data exists).
2. `DataOrchestrator` fetches each ASX-200 ticker from providers in priority order (YahooQuery → Finnhub → FMP → Alpha Vantage). Results are merged and cached in SQLite with a 24-hour TTL.
3. `scorer.py` runs 30 binary checks on the merged data and returns a 0–30 score.
4. If an Ollama host is configured, a ~400-word plain-English narrative is generated per stock.
5. `dashboard.py` reads from SQLite and renders the results.

---

## Getting started

### Prerequisites

- Docker and Docker Compose
- (Optional) An [Ollama](https://ollama.ai) instance for AI narratives
- (Optional) API keys for Finnhub, FMP, or Alpha Vantage

### 1. Clone and configure

```bash
git clone https://github.com/eds3028/stock-scanner.git
cd stock-scanner
cp .env.example .env
```

Edit `.env` and fill in any keys you have. The app works out of the box with no keys — YahooQuery is free and covers all ASX-200 tickers.

```
# .env
TZ=Australia/Perth

# Optional — enables AI narrative paragraphs
OLLAMA_HOST=http://192.168.1.10:11434
OLLAMA_MODEL=llama3.1:8b

# Optional — extra data sources (better fundamental coverage)
FINNHUB_API_KEY=your_key_here
FMP_API_KEY=your_key_here
ALPHA_VANTAGE_API_KEY=your_key_here
```

### 2. Build and run

```bash
docker compose up -d --build
```

The dashboard will be available at **http://localhost:8501** once the container is healthy (allow ~60 s for startup).

On first boot the scanner runs immediately and populates the database. Subsequent runs happen nightly at **20:00 AWST**.

### 3. Check logs

```bash
docker compose logs -f
```

Look for lines like:

```
Scan complete: 198 scored, 2 failed, 195 narratives, providers: {'YahooQuery': 180, 'Finnhub': 18}
```

---

## Local development (without Docker)

```bash
cd app
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install pytest   # for running tests

# Set env vars
export FINNHUB_API_KEY=...   # optional

# Run the dashboard against an existing database
streamlit run dashboard.py

# Run the scanner manually
python scanner.py

# Run tests
cd ..
pytest tests/
```

---

## Data providers

| Provider | Key required | ASX coverage | Notes |
|---|---|---|---|
| YahooQuery | No | Full ASX-200 | Primary free source; IP rate-limited |
| Finnhub | Yes (free tier) | Partial | Good for fundamentals and analyst targets |
| Financial Modelling Prep | Yes (free tier) | Partial | Strong financial statements |
| Alpha Vantage | Yes (free tier) | Limited | 25 req/day on free tier |

Providers are used in the order above. If a provider fails 5 times in a row its circuit breaker opens and it is skipped for the remainder of that scan. Fundamental data is cached for 24 hours; price data for 6 hours.

---

## Scoring model

Each stock receives a score from **0 to 30** made up of five dimensions (6 points each):

| Dimension | What it measures |
|---|---|
| **Value** | DCF vs price, P/E, P/B, EV/EBITDA, analyst target upside |
| **Future** | Earnings & revenue growth forecasts, ROE outlook, EPS trend, analyst coverage |
| **Past** | Historical ROE, ROA, operating margin, gross margin, earnings growth, 52-week momentum |
| **Health** | Debt/equity, current ratio, quick ratio, net cash position, interest coverage, FCF |
| **Dividends** | Pays dividend, yield >2%, payout ratio sustainability, FCF coverage, yield vs 5-yr average |

**Score interpretation:**

| Range | Meaning |
|---|---|
| 22–30 | Strong across most dimensions — worth detailed research |
| 15–21 | Mixed — has clear strengths and weaknesses |
| 8–14 | Weak in several areas — proceed with caution |
| 0–7 | Fails most checks — likely value trap or distressed |

**Important caveats:**

- Scores are calculated from data returned by public APIs. If a field is missing (common for small-caps), the corresponding check scores 0 rather than being excluded — this means thinly covered stocks are penalised by data absence, not by actual business weakness.
- The DCF check uses a simplified 10-year model with a fixed 10% discount rate and a capped growth rate. It is directional, not authoritative.
- Sector P/E thresholds are currently hardcoded proxies (< 20 vs sector, < 25 vs market). They are not dynamically adjusted per sector.
- The Dividends dimension naturally disadvantages growth companies that reinvest cash — a 0/6 dividends score is not necessarily a red flag.

---


## Model validation (Milestone 3)

The dashboard now includes a **Validation** tab to test whether the scoring model has predictive value.

### Backtest engine

- Monthly rebalance simulation over stored scan history
- Weighting modes: **equal weight** and **score weighted**
- Transaction cost assumptions in basis points (bps)
- KPI output: **CAGR, max drawdown, Sharpe, turnover, hit rate**
- CSV exports written to `/data/exports` for summary, monthly returns, and holdings
- Supports running by `scoring_model_version`

### Decile/quintile analysis

- Buckets stocks by total score into deciles (or quintiles)
- Compares forward returns over **1m / 3m / 6m / 12m**
- Repeats bucket tests for individual factor dimensions (Value/Future/Past/Health/Dividends)
- Visual charts in dashboard to assess monotonicity

### Survivorship bias note

Backtests and bucket studies use the rows available in the `scores` table. If historical scans exclude delisted companies or missing symbols, results can overstate real-world performance. Treat all outputs as directional research evidence, not executable performance.


## Project structure

```
stock-scanner/
├── docker-compose.yml       # Production container definition
├── .env.example             # Template — copy to .env
├── .gitignore
├── README.md
├── app/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── scorer.py            # 30-check scoring engine
│   ├── orchestrator.py      # Multi-provider data pipeline
│   ├── scanner.py           # Nightly scan orchestrator
│   ├── scheduler.py         # APScheduler cron wrapper
│   ├── dashboard.py         # Streamlit web UI
│   ├── env_check.py         # Startup environment validation
│   └── providers/
│       ├── base.py
│       ├── yahooquery_provider.py
│       ├── finnhub_provider.py
│       ├── fmp_provider.py
│       └── alpha_vantage_provider.py
└── tests/
    └── test_scorer.py       # pytest suite for scoring engine
```
