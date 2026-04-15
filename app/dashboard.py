"""ASX Stock Scanner Dashboard — merged & enhanced"""

import sqlite3
import json
import math
import os
import subprocess
import sys
import time
from datetime import date, datetime
from pathlib import Path

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

from backtest import run_backtest, forward_bucket_analysis, available_versions, export_backtest_csv
from portfolio import (
    add_watchlist_tickers,
    create_watchlist,
    get_watchlists,
    holdings_snapshot,
    import_holdings_csv,
    init_portfolio_tables,
    load_rules,
    save_rules,
)

DB_PATH = Path(os.environ.get("DB_PATH", "/data/stocks.db"))

st.set_page_config(
    page_title="ASX Scanner",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500;700&family=DM+Sans:wght@300;400;500;600;700&display=swap');

/* Hide Streamlit header toolbar */
header[data-testid="stHeader"] { display: none !important; }
header { display: none !important; }
#MainMenu { display: none !important; }
footer { display: none !important; }
[data-testid="stToolbar"] { display: none !important; }
.stDeployButton { display: none !important; }

/* Force dark mode universally */
:root, [data-theme], .stApp, .main, section[data-testid="stSidebar"] {
  color-scheme: dark !important;
  --bg: #070d1f;
  --panel: #0c1324;
  --panel-2: #121a30;
  --panel-3: #0f1629;
  --border: rgba(84,105,150,0.22);
  --border-soft: rgba(255,255,255,0.06);
  --text: #e8edf5;
  --muted: #7a90b5;
  --muted-2: #4a5d7a;
  --blue: #3b7dff;
  --green: #00d68f;
  --amber: #ffaa00;
  --red: #ff4d6a;
}

/* Dark backgrounds everywhere */
.stApp { background-color: #070d1f !important; }
.main .block-container { background-color: #070d1f !important; }
section[data-testid="stSidebar"] { background-color: #0c1324 !important; border-right: 1px solid #1e2d4a !important; }

/* Force all text to be light */
.stApp, .stApp p, .stApp span, .stApp div, .stApp label {
  color: #e8edf5 !important;
}

html, body, [class*="css"] { font-family: 'DM Sans', sans-serif !important; font-size: 14px; }
.block-container { padding-top: 1.0rem; padding-bottom: 2rem; max-width: 1580px; }

div[data-testid="stMetric"] {
  background: linear-gradient(180deg,rgba(255,255,255,0.02),rgba(255,255,255,0.01));
  border: 1px solid var(--border-soft); border-radius: 14px; padding: 10px 12px;
}
div[data-testid="stMetricValue"] { font-family: 'DM Mono', monospace !important; font-size: 1.12rem !important; color: #e8edf5 !important; }
div[data-testid="stMetricLabel"] { font-size: 0.72rem; color: #7a90b5 !important; }

hr { border-color: #1e2d4a !important; margin: 8px 0 !important; }

/* Tabs - larger touch targets for iPad */
.stTabs { position: relative; z-index: 1; }
.stTabs [data-baseweb="tab-list"] {
  border-bottom: 2px solid #1e2d4a !important; gap: 4px !important;
  background: transparent !important; padding: 0 !important;
}
.stTabs [data-baseweb="tab"] {
  color: #7a90b5 !important; font-size: 0.9rem !important; font-weight: 600 !important;
  padding: 16px 32px !important; border-radius: 0 !important;
  border-bottom: 3px solid transparent !important; background: transparent !important;
  min-width: 130px !important; text-align: center !important;
  margin-bottom: -2px !important;
}
.stTabs [aria-selected="true"] {
  color: #3b7dff !important; border-bottom: 3px solid #3b7dff !important;
  background: transparent !important;
}
.stTabs [data-baseweb="tab"]:hover { color: #b8d0ff !important; }

/* Tab content panels */
.stTabs [data-baseweb="tab-panel"] { padding-top: 1.5rem !important; }
.section-label {
  font-size: 0.65rem; font-weight: 700; letter-spacing: 0.12em;
  text-transform: uppercase; color: #4a5d7a; margin-bottom: 6px;
}
.badge, .filter-chip {
  font-family: monospace; font-size: 0.70rem; font-weight: 600;
  border-radius: 999px; padding: 3px 8px; display: inline-block;
}
.badge-high { background: #00d68f18; color: #00d68f; border: 1px solid #00d68f33; }
.badge-mid  { background: #ffaa0018; color: #ffaa00; border: 1px solid #ffaa0033; }
.badge-low  { background: #ff4d6a18; color: #ff4d6a; border: 1px solid #ff4d6a33; }
.badge-blue { background: #3b7dff18; color: #3b7dff; border: 1px solid #3b7dff33; }
.filter-chip { margin: 0 6px 6px 0; color: #b8c9e6; background: rgba(59,125,255,0.10); border: 1px solid rgba(59,125,255,0.16); }
.stock-card {
  background: radial-gradient(circle at top left,rgba(59,125,255,0.10),transparent 34%),
              linear-gradient(180deg,#121a30 0%,#0c1324 100%);
  border: 1px solid rgba(84,105,150,0.22); border-radius: 18px; overflow: hidden;
  position: relative; box-shadow: 0 10px 30px rgba(0,0,0,0.28),inset 0 1px 0 rgba(255,255,255,0.03);
  transition: transform 0.18s ease, box-shadow 0.18s ease; height: 100%;
}
.stock-card:hover { transform: translateY(-4px); box-shadow: 0 18px 40px rgba(0,0,0,0.34),0 0 0 1px rgba(59,125,255,0.10); }
.card-inner { padding: 14px 14px 10px; }
.card-topline { display: flex; justify-content: space-between; align-items: center; gap: 8px; }
.card-ticker {
  font-family: monospace; font-size: 0.68rem; color: #9fc0ff;
  background: linear-gradient(180deg,rgba(59,125,255,0.18),rgba(59,125,255,0.08));
  border: 1px solid rgba(59,125,255,0.28); border-radius: 999px; padding: 4px 9px; flex-shrink: 0;
}
.card-name { font-size: 1.02rem; font-weight: 700; color: #f4f7fb; line-height: 1.2; margin-top: 9px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.card-sector { font-size: 0.60rem; color: #667998; text-transform: uppercase; letter-spacing: 0.12em; }
.card-submeta { display: flex; justify-content: space-between; align-items: center; gap: 8px; margin-top: 7px; color: #7f92b3; font-size: 0.72rem; }
.card-hero { display: grid; grid-template-columns: 0.9fr 1.1fr; align-items: center; gap: 4px; padding-top: 8px; }
.card-score { font-size: 2.75rem; font-weight: 800; line-height: 0.92; font-family: monospace; letter-spacing: -0.03em; }
.card-score-denom { font-size: 0.78rem; color: #5e7291; font-family: monospace; }
.card-score-label { margin-top: 10px; font-size: 0.64rem; letter-spacing: 0.12em; text-transform: uppercase; color: #7f92b3; }
.card-score-detail { margin-top: 4px; color: #9db0cf; font-size: 0.70rem; }
.card-snowflake-wrap { position: relative; display: flex; justify-content: center; align-items: center; min-height: 192px; }
.dim-strip { display: grid; grid-template-columns: repeat(5,1fr); gap: 6px; padding-top: 4px; }
.dim-tile {
  background: linear-gradient(180deg,rgba(255,255,255,0.028),rgba(255,255,255,0.008));
  border: 1px solid rgba(255,255,255,0.06); border-radius: 10px;
  padding: 7px 4px 8px; text-align: center;
}
.dim-letter { font-size: 0.56rem; letter-spacing: 0.16em; font-weight: 700; opacity: 0.95; }
.dim-value { margin-top: 2px; font-size: 1.34rem; line-height: 1; font-family: monospace; font-weight: 800; }
.card-footer-note { margin-top: 8px; color: #7f92b3; font-size: 0.69rem; min-height: 1.2rem; }
div[data-testid="column"] div[data-testid="stButton"] > button {
  width: 100% !important;
  background: linear-gradient(180deg,#0f1830,#0b1020) !important;
  color: #b5c5df !important; border: 1px solid #1e2d4a !important;
  border-top: 1px solid rgba(255,255,255,0.05) !important;
  border-radius: 0 0 18px 18px !important;
  font-size: 0.76rem !important; font-weight: 600 !important; padding: 9px 0 !important;
}
div[data-testid="column"] div[data-testid="stButton"] > button:hover {
  background: linear-gradient(180deg,#152142,#101930) !important; color: #fff !important; border-color: #28406a !important;
}
.fin-card { background: linear-gradient(180deg,#10182d 0%,#0c1324 100%); border: 1px solid #1e2d4a; border-radius: 14px; padding: 16px 18px; margin-bottom: 8px; }

/* Panel - carries card theme through rest of UI */
.panel {
  background: radial-gradient(circle at top left,rgba(59,125,255,0.06),transparent 40%),
              linear-gradient(180deg,#121a30 0%,#0c1324 100%);
  border: 1px solid rgba(84,105,150,0.22); border-radius: 16px;
  padding: 20px 22px; margin-bottom: 16px;
  box-shadow: 0 4px 20px rgba(0,0,0,0.2),inset 0 1px 0 rgba(255,255,255,0.03);
}
.panel-title { font-size: 1.05rem; font-weight: 700; color: #f4f7fb; margin-bottom: 12px; }
.panel-subtitle { font-size: 0.78rem; color: #7a90b5; margin-bottom: 16px; }

.explain-box {
  background: linear-gradient(180deg,rgba(59,125,255,0.07),rgba(59,125,255,0.02));
  border: 1px solid rgba(59,125,255,0.15); border-radius: 10px;
  padding: 12px 16px; margin: 10px 0 4px; font-size: 0.82rem;
  color: #9db0cf; line-height: 1.6;
}
.explain-title { color: #b8d0ff; font-weight: 600; font-size: 0.78rem; margin-bottom: 4px; }

/* Expanders */
details { background: linear-gradient(180deg,#10182d,#0c1324) !important; border: 1px solid #1e2d4a !important; border-radius: 12px !important; margin-bottom: 8px !important; }
details summary { color: #e8edf5 !important; font-weight: 600 !important; padding: 12px 16px !important; }
details[open] summary { border-bottom: 1px solid #1e2d4a !important; }

/* Dataframes */
[data-testid="stDataFrame"] { border: 1px solid #1e2d4a !important; border-radius: 14px; overflow: hidden; background: #0c1324 !important; }
[data-testid="stDataFrame"] th { background: #10182d !important; color: #7a90b5 !important; font-size: 0.72rem !important; text-transform: uppercase !important; letter-spacing: 0.08em !important; }
[data-testid="stDataFrame"] td { color: #e8edf5 !important; background: #0c1324 !important; font-size: 0.82rem !important; }

/* Selectbox and inputs */
[data-testid="stSelectbox"] > div > div { background: #0c1324 !important; border-color: #1e2d4a !important; color: #e8edf5 !important; }
.stRadio label { color: #b8c9e6 !important; }

/* Sidebar */
section[data-testid="stSidebar"] { background-color: #0c1324 !important; border-right: 1px solid #1e2d4a !important; }
section[data-testid="stSidebar"] p,
section[data-testid="stSidebar"] span,
section[data-testid="stSidebar"] label { color: #b8c9e6 !important; }

/* Progress bar */
[data-testid="stProgress"] > div > div { background: #3b7dff !important; }
[data-testid="stProgress"] { background: #1e2d4a !important; border-radius: 999px !important; }

/* Captions and alerts */
.stCaption, [data-testid="stCaptionContainer"] { color: #4a5d7a !important; }
[data-testid="stAlert"] { background: #10182d !important; border-color: #1e2d4a !important; color: #e8edf5 !important; border-radius: 12px !important; }
</style>
""", unsafe_allow_html=True)


# ── Check explanations ────────────────────────────────────────────────────────

CHECK_EXPLANATIONS = {
    # Value
    "dcf_below_price": ("DCF Fair Value", "The stock's current price is below what our Discounted Cash Flow model estimates it's worth. DCF projects future cash flows and discounts them back to today - passing this means the market may be undervaluing the stock."),
    "pe_below_sector": ("P/E vs Sector", "The Price-to-Earnings ratio is lower than the industry average. A lower P/E can mean you're paying less for each dollar of profit compared to similar companies."),
    "pe_below_market": ("P/E vs Market", "The P/E ratio is below the broader market average (proxy: 25x). Stocks trading below market P/E are generally considered more attractively valued."),
    "pb_reasonable": ("Price-to-Book", "The stock trades below 3x its book value (net assets). A lower P/B can indicate the stock isn't wildly overpriced relative to what the company actually owns."),
    "ev_ebitda_reasonable": ("EV/EBITDA", "Enterprise Value to EBITDA is below 15x - a common threshold for reasonable valuation. This ratio compares total company value to operating earnings, useful across industries."),
    "analyst_target_upside": ("Analyst Target", "Analysts' consensus price target is at least 20% above the current price, with tight agreement between analysts. This suggests professional consensus on meaningful upside."),
    # Future
    "earnings_growth_positive": ("Earnings Growth", "Earnings are forecast to grow by more than 5% annually. Growing earnings typically drive share price appreciation over time."),
    "revenue_growth_positive": ("Revenue Growth", "Revenue is expected to grow more than 5%. Consistent top-line growth is a key indicator of a healthy, expanding business."),
    "roe_high": ("Return on Equity", "Return on Equity exceeds 15% - the company generates $15+ for every $100 of shareholder equity. High ROE indicates efficient use of investor capital."),
    "eps_improving": ("EPS Improving", "Forward earnings per share is higher than trailing EPS. This means the company is expected to earn more per share in the future than it did in the past."),
    "analyst_coverage": ("Analyst Coverage", "At least 3 analysts cover this stock. Broad analyst coverage generally means better information quality and more reliable consensus estimates."),
    "profit_margin_positive": ("Profit Margin", "Net profit margin exceeds 5%. This is the percentage of revenue that becomes actual profit after all costs - higher is better."),
    # Past
    "roe_strong": ("Historical ROE", "Return on Equity has historically been above 15%. Consistently high ROE over time indicates a durable competitive advantage."),
    "roa_positive": ("Return on Assets", "Return on Assets exceeds 5% - the company generates meaningful profit from its asset base. Strong ROA indicates operational efficiency."),
    "operating_margin_good": ("Operating Margin", "Operating margin exceeds 10%. This measures how much profit is made from core operations before interest and taxes."),
    "gross_margin_good": ("Gross Margin", "Gross margin exceeds 30%. High gross margins indicate pricing power and a business model with inherent profitability."),
    "earnings_growth_historic": ("Historic Earnings Growth", "Earnings have been growing historically. Consistent past earnings growth is a reasonable predictor of future performance."),
    "price_momentum": ("Price Momentum", "The stock is trading in the upper 40% of its 52-week range. Stocks closer to their highs often have positive momentum behind them."),
    # Health
    "debt_equity_low": ("Debt/Equity", "Total debt is less than 100% of equity. Lower leverage means less financial risk and more flexibility during downturns."),
    "current_ratio_good": ("Current Ratio", "Current assets exceed current liabilities (ratio > 1). This means the company can cover its short-term obligations without stress."),
    "quick_ratio_good": ("Quick Ratio", "The company can cover short-term debts using liquid assets (excluding inventory). A stricter test of short-term financial health."),
    "net_cash_positive": ("Net Cash", "The company holds more cash than total debt - a net cash position. This is a very strong balance sheet indicator."),
    "interest_coverage_good": ("Interest Coverage", "EBITDA covers interest payments by at least 3x. High coverage means the company can comfortably service its debt."),
    "positive_fcf": ("Free Cash Flow", "The company generates positive free cash flow after capital expenditure. FCF is the cash available to return to shareholders or reinvest."),
    # Dividends
    "pays_dividend": ("Pays Dividend", "The company pays a dividend. For income-focused investors, this is the starting point."),
    "yield_meaningful": ("Yield > 2%", "The dividend yield exceeds 2% - meaningful income compared to cash. Higher yields provide better income relative to your investment."),
    "payout_sustainable": ("Payout Ratio < 80%", "Less than 80% of earnings are paid out as dividends. A lower payout ratio leaves room for dividend growth and is less vulnerable to cuts."),
    "payout_coverage": ("Payout Coverage", "Coverage above 1.0 means earnings cover the dividend. Higher is safer, especially above 1.4x."),
    "franking_level": ("Franking Level", "Higher franking means more usable tax credits for Australian investors."),
    "grossed_up_yield": ("Grossed-up Yield", "Estimated dividend yield after adding franking credits (assumes 30% company tax rate)."),
    "future_payout_covered": ("Future Payout", "The projected payout ratio in 3 years remains below 90%, suggesting the dividend is sustainable as earnings grow."),
    "fcf_covers_dividend": ("FCF Covers Dividend", "Free cash flow per share exceeds the dividend per share. Dividends funded by cash flow are more reliable than those funded by debt."),
    "yield_above_average": ("Yield vs 5yr Average", "Current dividend yield is at or above the 5-year average yield. This can indicate the stock is attractively priced for income investors."),
}

DIM_EXPLANATIONS = {
    "value": ("💰 Value", "Is the stock priced attractively?",
              "Value checks assess whether you're paying a fair or attractive price for the business. A high value score means the stock appears undervalued relative to its earnings, assets, and analyst expectations. Important: a low value score doesn't mean a bad company - some great businesses always look expensive."),
    "future": ("🚀 Future", "Is growth expected?",
               "Future checks assess what analysts and financial data suggest about the company's growth prospects. A high score means earnings, revenue, and profitability are expected to grow. This is particularly important for long-term investors focused on compounding returns."),
    "past": ("📈 Past", "Has it performed historically?",
             "Past checks review the company's historical financial performance. Consistent past performance - strong margins, returns on equity, earnings growth - often indicates a durable business model. It's not a guarantee of future results, but a strong track record matters."),
    "health": ("🏥 Health", "Is the balance sheet sound?",
               "Health checks assess financial stability and risk. A healthy balance sheet means the company can weather downturns, has manageable debt, and generates real cash. For new investors, health is arguably the most important dimension - financially stressed companies can fail even if the business is otherwise good."),
    "dividends": ("💵 Dividends", "Is income reliable?",
                  "Dividend checks assess the quality, sustainability, and attractiveness of dividend payments. High scores indicate the company pays a meaningful, sustainable dividend covered by earnings and cash flow. Relevant primarily if you want income from your investments - growth companies often score zero here intentionally."),
}


# ── DB helpers ─────────────────────────────────────────────────────────────────

def get_db():
    if not DB_PATH.exists():
        return None
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        init_portfolio_tables(conn)
        init_custom_tickers_table(conn)
        return conn
    except Exception:
        return None


def get_latest_date(conn):
    # A partial/crashed scan can leave a new scan_date with very few rows,
    # which causes MAX(scan_date) to pick that date and hide all prior data.
    # Use the latest date that has at least half as many stocks as the
    # best historical scan (minimum floor of 5).
    max_row = conn.execute(
        "SELECT MAX(cnt) as mc FROM (SELECT COUNT(*) as cnt FROM scores GROUP BY scan_date)"
    ).fetchone()
    max_cnt = (max_row["mc"] if max_row and max_row["mc"] else 0) or 0
    threshold = max(5, int(max_cnt * 0.5))
    row = conn.execute(
        "SELECT scan_date FROM scores GROUP BY scan_date HAVING COUNT(*) >= ? ORDER BY scan_date DESC LIMIT 1",
        (threshold,)
    ).fetchone()
    if row:
        return row["scan_date"]
    # Fallback: any date at all (e.g. database has fewer than 5 stocks total)
    row = conn.execute("SELECT MAX(scan_date) as d FROM scores").fetchone()
    return row["d"] if row else None


def get_previous_date(conn, today):
    row = conn.execute("SELECT MAX(scan_date) as d FROM scores WHERE scan_date < ?", (today,)).fetchone()
    return row["d"] if row else None


def get_all_scores(conn, scan_date, filters):
    available_columns = {row["name"] for row in conn.execute("PRAGMA table_info(scores)").fetchall()}
    optional_columns = [
        "weighted_total",
        "adjusted_total",
        "confidence_score",
        "confidence_badge",
        "template_key",
        "template_name",
        "confidence_detail",
        "raw_info",
        "dimension_detail",
        "narrative",
        "data_provider",
        "data_completeness",
        "data_fetched_at",
    ]
    optional_selects = [
        col if col in available_columns else f"NULL as {col}"
        for col in optional_columns
    ]
    query = """
        SELECT ticker, company_name, sector, industry, market_cap, current_price,
               total_score, value_score, future_score, past_score,
               health_score, dividend_score, {optional_selects}
        FROM scores WHERE scan_date = ?
        AND total_score >= ? AND value_score >= ? AND future_score >= ?
        AND past_score >= ? AND health_score >= ? AND dividend_score >= ?
    """.format(optional_selects=", ".join(optional_selects))
    params = [scan_date, filters["min_total"], filters["min_value"], filters["min_future"],
              filters["min_past"], filters["min_health"], filters["min_dividend"]]
    if filters.get("sector") and not str(filters["sector"]).startswith("All"):
        sector = str(filters["sector"]).split(" (")[0]
        query += " AND sector = ?"
        params.append(sector)
    query += " ORDER BY total_score DESC, health_score DESC"
    rows = [dict(r) for r in conn.execute(query, params).fetchall()]
    return apply_portfolio_overlay(conn, rows, filters)


def apply_portfolio_overlay(conn, rows, filters):
    watchlists = get_watchlists(conn)
    wl_map = {w["id"]: w["name"] for w in watchlists}
    wl_items = conn.execute("SELECT watchlist_id, ticker FROM watchlist_items").fetchall()
    ticker_watchlists = {}
    for r in wl_items:
        ticker_watchlists.setdefault(r["ticker"], []).append(wl_map.get(r["watchlist_id"], ""))

    holdings = holdings_snapshot(conn, rows)
    rules = load_rules(conn)
    dividend_yield_map = {}
    for row in rows:
        try:
            raw = json.loads(row.get("raw_info") or "{}")
            dividend_yield_map[row["ticker"]] = raw.get("dividendYield")
        except Exception:
            dividend_yield_map[row["ticker"]] = None

    sector_weights = {}
    for row in rows:
        h = holdings.get(row["ticker"])
        if h:
            sector_weights[row.get("sector") or "Unknown"] = sector_weights.get(row.get("sector") or "Unknown", 0) + h.current_weight
    sector_counts = {}
    dividend_income_by_ticker = {}
    total_dividend_income = 0.0
    for row in rows:
        if row["ticker"] in holdings:
            sector = row.get("sector") or "Unknown"
            sector_counts[sector] = sector_counts.get(sector, 0) + 1
            h = holdings[row["ticker"]]
            est_income = (h.shares or 0) * (row.get("current_price") or 0) * (dividend_yield_map.get(row["ticker"]) or 0)
            dividend_income_by_ticker[row["ticker"]] = est_income
            total_dividend_income += est_income

    enriched = []
    for row in rows:
        ticker = row["ticker"]
        row["watchlists"] = [w for w in ticker_watchlists.get(ticker, []) if w]
        h = holdings.get(ticker)
        row["owned"] = h is not None
        row["target_weight"] = h.target_weight if h else 0.0
        row["current_weight"] = h.current_weight if h else 0.0
        row["cost_base"] = h.cost_base if h else None
        row["acquired_at"] = h.acquired_at if h else None
        row["portfolio_fit"] = "Good fit"
        fit_flags = []
        if row["current_weight"] > rules["max_position_weight"]:
            fit_flags.append("Position concentration")
        sector = row.get("sector") or "Unknown"
        if sector_weights.get(sector, 0) > rules["max_sector_weight"]:
            fit_flags.append("Sector cap exceeded")
        if (row.get("market_cap") or 0) < rules["liquidity_floor_market_cap"]:
            fit_flags.append("Liquidity floor breach")
        if sector_counts.get(sector, 0) >= rules["max_sector_names"]:
            fit_flags.append("Sector overlap warning")
        if fit_flags:
            row["portfolio_fit"] = "Good stock, poor portfolio fit"
        row["fit_flags"] = fit_flags

        gain_estimate = None
        gain_pct = None
        holding_days = None
        days_to_cgt_discount = None
        if row["owned"] and row.get("cost_base") is not None and row.get("current_price") is not None:
            gain_estimate = (row["current_price"] - row["cost_base"]) * (h.shares or 0)
            if row["cost_base"] > 0:
                gain_pct = (row["current_price"] - row["cost_base"]) / row["cost_base"]
        if row["owned"] and row.get("acquired_at"):
            try:
                acquired = datetime.fromisoformat(row["acquired_at"]).date()
                holding_days = (date.today() - acquired).days
                if holding_days < 365:
                    days_to_cgt_discount = 365 - holding_days
            except Exception:
                pass

        row["unrealised_gain_estimate"] = gain_estimate
        row["unrealised_gain_pct"] = gain_pct
        row["holding_days"] = holding_days
        row["days_to_cgt_discount"] = days_to_cgt_discount
        row["cgt_discount_eligible"] = bool(holding_days is not None and holding_days >= 365)

        income_share = 0.0
        if row["owned"] and total_dividend_income > 0:
            income_share = dividend_income_by_ticker.get(ticker, 0.0) / total_dividend_income
        row["dividend_income_share"] = income_share
        if row["owned"] and income_share >= 0.30:
            row["fit_flags"].append("Dividend concentration risk")

        if row["owned"]:
            tax_deferral = (
                row["current_weight"] > (row["target_weight"] + rules["rebalance_tolerance"])
                and (row.get("days_to_cgt_discount") is not None)
                and row["days_to_cgt_discount"] <= 60
                and (row.get("unrealised_gain_estimate") or 0) > 0
            )
            if tax_deferral:
                status = "Review Later (Tax)"
            elif row["current_weight"] > (row["target_weight"] + rules["rebalance_tolerance"]):
                status = "Trim Candidate"
            elif row["total_score"] < 14 or row["portfolio_fit"] != "Good fit":
                status = "Review"
            else:
                status = "Hold"
        else:
            status = "New"
        row["portfolio_status"] = status
        row["tax_prompt"] = (
            f"Approaching 12-month CGT discount in {row['days_to_cgt_discount']} days."
            if row.get("days_to_cgt_discount") is not None and row["days_to_cgt_discount"] <= 60 and (row.get("unrealised_gain_estimate") or 0) > 0
            else ""
        )
        row["rebalance_action"] = row["target_weight"] - row["current_weight"]
        enriched.append(row)

    status_filter = filters.get("portfolio_status", "All")
    if status_filter != "All":
        enriched = [r for r in enriched if r["portfolio_status"] == status_filter]

    watchlist_filter = filters.get("watchlist", "All watchlists")
    if watchlist_filter != "All watchlists":
        enriched = [r for r in enriched if watchlist_filter in r["watchlists"]]

    ownership_filter = filters.get("ownership", "All")
    if ownership_filter == "Owned":
        enriched = [r for r in enriched if r["owned"]]
    elif ownership_filter == "Discovery":
        enriched = [r for r in enriched if not r["owned"]]
    return enriched


def get_movers(conn, today, yesterday):
    if not yesterday:
        return [], []
    up = conn.execute("""
        SELECT t.ticker, t.company_name, t.sector,
            t.total_score as today_score, y.total_score as yesterday_score,
            (t.total_score - y.total_score) as change,
            t.current_price, t.market_cap,
            (t.value_score - y.value_score) as value_change,
            (t.future_score - y.future_score) as future_change,
            (t.past_score - y.past_score) as past_change,
            (t.health_score - y.health_score) as health_change,
            (t.dividend_score - y.dividend_score) as dividend_change
        FROM scores t JOIN scores y ON t.ticker = y.ticker
        WHERE t.scan_date = ? AND y.scan_date = ? AND t.total_score - y.total_score >= 2
        ORDER BY change DESC LIMIT 10
    """, (today, yesterday)).fetchall()
    down = conn.execute("""
        SELECT t.ticker, t.company_name, t.sector,
            t.total_score as today_score, y.total_score as yesterday_score,
            (t.total_score - y.total_score) as change,
            t.current_price, t.market_cap,
            (t.value_score - y.value_score) as value_change,
            (t.future_score - y.future_score) as future_change,
            (t.past_score - y.past_score) as past_change,
            (t.health_score - y.health_score) as health_change,
            (t.dividend_score - y.dividend_score) as dividend_change
        FROM scores t JOIN scores y ON t.ticker = y.ticker
        WHERE t.scan_date = ? AND y.scan_date = ? AND y.total_score - t.total_score >= 2
        ORDER BY change ASC LIMIT 10
    """, (today, yesterday)).fetchall()
    return [dict(r) for r in up], [dict(r) for r in down]


def get_sectors_with_count(conn, scan_date):
    rows = conn.execute("""
        SELECT sector, COUNT(*) as cnt FROM scores
        WHERE scan_date = ? AND sector != '' GROUP BY sector ORDER BY sector
    """, (scan_date,)).fetchall()
    return ["All sectors"] + [f"{r['sector']} ({r['cnt']})" for r in rows]


def get_stock_history(conn, ticker):
    rows = conn.execute("""
        SELECT scan_date, total_score, value_score, future_score,
               past_score, health_score, dividend_score, current_price
        FROM scores WHERE ticker = ? ORDER BY scan_date ASC
    """, (ticker,)).fetchall()
    return [dict(r) for r in rows]


def get_scan_log(conn):
    try:
        return [dict(r) for r in conn.execute("SELECT * FROM scan_log ORDER BY scan_date DESC LIMIT 5").fetchall()]
    except Exception:
        return []


def get_fetch_log(conn, limit=40):
    try:
        return [dict(r) for r in conn.execute("""
            SELECT ticker, provider, success, completeness, reason, fetched_at
            FROM fetch_log ORDER BY fetched_at DESC LIMIT ?
        """, (limit,)).fetchall()]
    except Exception:
        return []


def get_cache_stats(conn):
    try:
        total = conn.execute("SELECT COUNT(*) as c FROM data_cache").fetchone()["c"]
        stale = conn.execute("SELECT COUNT(*) as c FROM data_cache WHERE fetched_at < ?",
                             (time.time() - 86400,)).fetchone()["c"]
        return {"total": total, "stale": stale, "fresh": total - stale}
    except Exception:
        return {"total": 0, "stale": 0, "fresh": 0}


# ── Custom tickers (scanner universe additions) ───────────────────────────────

def init_custom_tickers_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS custom_tickers (
            ticker TEXT PRIMARY KEY,
            added_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()


def get_custom_tickers(conn) -> list:
    try:
        rows = conn.execute("SELECT ticker FROM custom_tickers ORDER BY ticker").fetchall()
        return [r["ticker"] for r in rows]
    except Exception:
        return []


def add_custom_tickers(conn, raw: str) -> tuple:
    added, skipped = [], []
    for part in raw.split(","):
        t = part.strip().upper()
        if not t:
            continue
        if not t.endswith(".AX"):
            t = t + ".AX"
        try:
            cur = conn.execute("INSERT OR IGNORE INTO custom_tickers (ticker) VALUES (?)", (t,))
            if cur.rowcount:
                added.append(t)
            else:
                skipped.append(t)
        except Exception:
            skipped.append(t)
    conn.commit()
    return added, skipped


def remove_custom_ticker(conn, ticker: str):
    conn.execute("DELETE FROM custom_tickers WHERE ticker = ?", (ticker,))
    conn.commit()


# ── Price history (live yfinance, cached 1h) ──────────────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_price_history_cached(ticker: str) -> dict:
    """Returns {"dates": [...], "closes": [...]} or empty dict on failure."""
    try:
        import yfinance as yf
        hist = yf.Ticker(ticker).history(period="1y")
        if hist.empty:
            return {}
        hist = hist.reset_index()
        return {
            "dates": [str(d.date()) for d in hist["Date"]],
            "closes": [round(float(c), 3) if c == c else None for c in hist["Close"]],
        }
    except Exception:
        return {}


def make_price_chart(price_data: dict, target: float, target_low: float,
                     target_high: float, ticker_label: str) -> "go.Figure | None":
    if not price_data or not price_data.get("closes"):
        return None
    dates = price_data["dates"]
    closes = price_data["closes"]
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=dates, y=closes, name="Price",
        line=dict(color="#3b7dff", width=2),
        fill="tozeroy", fillcolor="rgba(59,125,255,0.07)",
    ))
    if target_high and target_low:
        fig.add_trace(go.Scatter(
            x=dates + dates[::-1],
            y=[target_high] * len(dates) + [target_low] * len(dates),
            fill="toself", fillcolor="rgba(0,214,143,0.07)",
            line=dict(color="rgba(0,0,0,0)"), name="Analyst range", showlegend=True,
        ))
    if target:
        fig.add_hline(y=target, line_dash="dash", line_color="#00d68f", line_width=1.5,
                      annotation_text=f"  ${target:.2f} target", annotation_font_color="#00d68f",
                      annotation_position="top left")
    fig.update_layout(
        template="plotly_dark", height=270, paper_bgcolor="#0c1324",
        plot_bgcolor="#0c1324", margin=dict(l=0, r=10, t=10, b=10),
        xaxis=dict(gridcolor="#1a2540", showgrid=True),
        yaxis=dict(gridcolor="#1a2540", tickprefix="$"),
        legend=dict(orientation="h", y=1.08, font_size=11),
    )
    return fig


# ── Financial metrics snapshot (Simply Wall St style) ────────────────────────

def _pct(v):
    return f"{v*100:.1f}%" if v is not None else "—"

def _ratio(v, suffix="x", decimals=2):
    return f"{v:.{decimals}f}{suffix}" if v is not None else "—"

def _cap(v):
    if v is None: return "—"
    if abs(v) >= 1e9: return f"${v/1e9:.1f}B"
    if abs(v) >= 1e6: return f"${v/1e6:.0f}M"
    return f"${v:,.0f}"

def _metric_card(label: str, value: str, good: bool = None):
    if good is True:
        val_color = "#00d68f"
    elif good is False:
        val_color = "#ff4d6a"
    else:
        val_color = "#e8edf5"
    return (
        f'<div style="background:#0c1324;border:1px solid #1a2540;border-radius:10px;'
        f'padding:10px 12px;margin:4px 0;">'
        f'<div style="color:#4a5d7a;font-size:0.68rem;margin-bottom:3px">{label}</div>'
        f'<div style="color:{val_color};font-family:monospace;font-size:1.0rem;font-weight:600">{value}</div>'
        f'</div>'
    )

def render_financial_snapshot(raw: dict, row: dict):
    """Display financial metrics organized into sections, sourced from raw_info."""
    price = raw.get("currentPrice") or raw.get("regularMarketPrice") or row.get("current_price")
    target = raw.get("targetMeanPrice")
    target_low = raw.get("targetLowPrice")
    target_high = raw.get("targetHighPrice")
    n_analysts = raw.get("numberOfAnalystOpinions") or 0
    w52_high = raw.get("fiftyTwoWeekHigh")
    w52_low = raw.get("fiftyTwoWeekLow")

    # Upside calculation
    upside_str = "—"
    if price and target:
        up = ((target - price) / price) * 100
        upside_str = f"{up:+.1f}%"

    # Section header style
    sec = '<div style="font-size:0.7rem;font-weight:700;color:#4a5d7a;letter-spacing:0.08em;margin:12px 0 4px">%s</div>'

    # ── Valuation ──
    st.markdown(sec % "VALUATION", unsafe_allow_html=True)
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    with c1:
        analyst_note = f"({n_analysts} analysts)" if n_analysts else ""
        st.markdown(_metric_card("Analyst Target", f"${target:.2f} {analyst_note}" if target else "—"), unsafe_allow_html=True)
    with c2:
        good_up = (((target or 0) - (price or 0)) / max(price or 1, 1) * 100) > 15 if (target and price) else None
        st.markdown(_metric_card("Upside / Downside", upside_str, good=good_up), unsafe_allow_html=True)
    with c3:
        pe = raw.get("trailingPE")
        st.markdown(_metric_card("P/E (TTM)", _ratio(pe)), unsafe_allow_html=True)
    with c4:
        fpe = raw.get("forwardPE")
        st.markdown(_metric_card("Fwd P/E", _ratio(fpe)), unsafe_allow_html=True)
    with c5:
        ev_eb = raw.get("enterpriseToEbitda")
        st.markdown(_metric_card("EV/EBITDA", _ratio(ev_eb)), unsafe_allow_html=True)
    with c6:
        pb = raw.get("priceToBook")
        st.markdown(_metric_card("Price/Book", _ratio(pb)), unsafe_allow_html=True)

    # 52-week range bar
    if price and w52_high and w52_low and w52_high > w52_low:
        pos = min(max((price - w52_low) / (w52_high - w52_low), 0), 1)
        st.markdown(
            f'<div style="margin:6px 0 0">'
            f'<div style="display:flex;justify-content:space-between;font-size:0.68rem;color:#4a5d7a">'
            f'<span>52w Low ${w52_low:.2f}</span><span>52w High ${w52_high:.2f}</span></div>'
            f'<div style="background:#1a2540;border-radius:4px;height:6px;margin-top:3px;position:relative">'
            f'<div style="background:#3b7dff;border-radius:4px;height:6px;width:{pos*100:.1f}%"></div>'
            f'<div style="position:absolute;top:-2px;left:{pos*100:.1f}%;width:10px;height:10px;'
            f'background:#e8edf5;border-radius:50%;transform:translateX(-50%)"></div>'
            f'</div></div>',
            unsafe_allow_html=True,
        )

    # ── Profitability ──
    st.markdown(sec % "PROFITABILITY", unsafe_allow_html=True)
    p1, p2, p3, p4, p5 = st.columns(5)
    gm = raw.get("grossMargins")
    om = raw.get("operatingMargins")
    nm = raw.get("profitMargins")
    roe = raw.get("returnOnEquity")
    roa = raw.get("returnOnAssets")
    with p1: st.markdown(_metric_card("Gross Margin", _pct(gm), good=(gm or 0) > 0.30), unsafe_allow_html=True)
    with p2: st.markdown(_metric_card("Op Margin", _pct(om), good=(om or 0) > 0.10), unsafe_allow_html=True)
    with p3: st.markdown(_metric_card("Net Margin", _pct(nm), good=(nm or 0) > 0.05), unsafe_allow_html=True)
    with p4: st.markdown(_metric_card("ROE", _pct(roe), good=(roe or 0) > 0.15), unsafe_allow_html=True)
    with p5: st.markdown(_metric_card("ROA", _pct(roa), good=(roa or 0) > 0.05), unsafe_allow_html=True)

    # ── Growth ──
    st.markdown(sec % "GROWTH", unsafe_allow_html=True)
    g1, g2, g3, g4 = st.columns(4)
    rg = raw.get("revenueGrowth")
    eg = raw.get("earningsGrowth")
    eps_t = raw.get("trailingEps")
    eps_f = raw.get("forwardEps")
    with g1: st.markdown(_metric_card("Revenue Growth", _pct(rg), good=(rg or 0) > 0.05), unsafe_allow_html=True)
    with g2: st.markdown(_metric_card("Earnings Growth", _pct(eg), good=(eg or 0) > 0.05), unsafe_allow_html=True)
    with g3: st.markdown(_metric_card("EPS (TTM)", f"${eps_t:.2f}" if eps_t else "—"), unsafe_allow_html=True)
    with g4: st.markdown(_metric_card("EPS (Fwd)", f"${eps_f:.2f}" if eps_f else "—"), unsafe_allow_html=True)

    # ── Financial Health ──
    st.markdown(sec % "FINANCIAL HEALTH", unsafe_allow_html=True)
    h1, h2, h3, h4, h5 = st.columns(5)
    cr = raw.get("currentRatio")
    qr = raw.get("quickRatio")
    de = raw.get("debtToEquity")
    cash = raw.get("totalCash")
    debt = raw.get("totalDebt")
    fcf = raw.get("freeCashflow")
    net_cash = ((cash or 0) - (debt or 0)) if (cash is not None or debt is not None) else None
    with h1: st.markdown(_metric_card("Current Ratio", _ratio(cr), good=(cr or 0) > 1.0), unsafe_allow_html=True)
    with h2: st.markdown(_metric_card("Quick Ratio", _ratio(qr), good=(qr or 0) > 0.8), unsafe_allow_html=True)
    with h3:
        de_norm = (de / 100) if de else None  # yfinance returns D/E as percentage
        st.markdown(_metric_card("Debt/Equity", _ratio(de_norm), good=(de_norm or 99) < 1.0), unsafe_allow_html=True)
    with h4: st.markdown(_metric_card("Net Cash/(Debt)", _cap(net_cash), good=(net_cash or -1) > 0), unsafe_allow_html=True)
    with h5: st.markdown(_metric_card("Free Cash Flow", _cap(fcf), good=(fcf or -1) > 0), unsafe_allow_html=True)

    # ── Dividends (only if paying) ──
    div_yield = raw.get("dividendYield")
    div_rate = raw.get("dividendRate")
    if div_yield or div_rate:
        st.markdown(sec % "DIVIDENDS", unsafe_allow_html=True)
        dv1, dv2, dv3, dv4 = st.columns(4)
        payout = raw.get("payoutRatio")
        avg5y = raw.get("fiveYearAvgDividendYield")
        with dv1: st.markdown(_metric_card("Yield", _pct(div_yield), good=(div_yield or 0) > 0.02), unsafe_allow_html=True)
        with dv2: st.markdown(_metric_card("Annual Div", f"${div_rate:.2f}" if div_rate else "—"), unsafe_allow_html=True)
        with dv3: st.markdown(_metric_card("Payout Ratio", _pct(payout), good=(payout or 1) < 0.80), unsafe_allow_html=True)
        with dv4: st.markdown(_metric_card("5yr Avg Yield", _pct(avg5y / 100 if avg5y else None)), unsafe_allow_html=True)


# ── Presets ───────────────────────────────────────────────────────────────────

PRESETS = {
    "All":          {"min_total": 0,  "min_value": 0, "min_future": 0, "min_past": 0, "min_health": 0, "min_dividend": 0},
    "High quality": {"min_total": 20, "min_value": 3, "min_future": 3, "min_past": 3, "min_health": 4, "min_dividend": 0},
    "Income":       {"min_total": 16, "min_value": 2, "min_future": 0, "min_past": 2, "min_health": 3, "min_dividend": 4},
    "Growth":       {"min_total": 16, "min_value": 0, "min_future": 4, "min_past": 2, "min_health": 2, "min_dividend": 0},
    "Value":        {"min_total": 16, "min_value": 4, "min_future": 0, "min_past": 2, "min_health": 2, "min_dividend": 0},
}


def init_state():
    defaults = {"min_total": 0, "min_value": 0, "min_future": 0,
                "min_past": 0, "min_health": 0, "min_dividend": 0,
                "filter_preset": "All"}
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def apply_preset(name):
    for k, v in PRESETS.get(name, PRESETS["All"]).items():
        st.session_state[k] = v
    st.session_state["filter_preset"] = name


# ── Formatting ─────────────────────────────────────────────────────────────────

def total_color(score):
    if score >= 20: return "#00d68f"
    if score >= 14: return "#ffaa00"
    return "#ff4d6a"


def score_band(score):
    if score >= 22: return "High conviction"
    if score >= 18: return "Strong overall"
    if score >= 14: return "Balanced"
    return "Speculative"


def best_dimension_text(row):
    dims = {"Value": row.get("value_score", 0), "Future": row.get("future_score", 0),
            "Past": row.get("past_score", 0), "Health": row.get("health_score", 0),
            "Dividends": row.get("dividend_score", 0)}
    sorted_dims = sorted(dims.items(), key=lambda x: x[1], reverse=True)
    best = [n for n, v in sorted_dims if v == sorted_dims[0][1]]
    return " / ".join(best[:2])


def major_change_text(m):
    changes = {"Value": m.get("value_change", 0), "Future": m.get("future_change", 0),
               "Past": m.get("past_change", 0), "Health": m.get("health_change", 0),
               "Dividends": m.get("dividend_change", 0)}
    name, val = max(changes.items(), key=lambda x: abs(x[1]))
    if val == 0: return "No major dimension shift"
    return f"{name} {'+' if val > 0 else ''}{val}"


def dim_tiles_html(v, f, p, h, d):
    dims = [("V", v), ("F", f), ("P", p), ("H", h), ("D", d)]
    tiles = []
    for letter, score in dims:
        c = "#00d68f" if score >= 5 else "#ffaa00" if score >= 3 else "#ff4d6a"
        tiles.append(
            f'<div class="dim-tile" style="border-top:2px solid {c}55;">'
            f'<div class="dim-letter" style="color:{c};">{letter}</div>'
            f'<div class="dim-value" style="color:{c};">{score}</div>'
            f'</div>'
        )
    return f'<div class="dim-strip">{"".join(tiles)}</div>'


def fmt_market_cap(val):
    if not val: return "—"
    if val >= 1e12: return f"${val/1e12:.1f}T"
    if val >= 1e9: return f"${val/1e9:.1f}B"
    if val >= 1e6: return f"${val/1e6:.0f}M"
    return f"${val:,.0f}"


def fmt_age(ts):
    if not ts: return ""
    h = (time.time() - ts) / 3600
    if h < 1: return f"{int(h*60)}m ago"
    if h < 24: return f"{h:.0f}h ago"
    return f"{h/24:.0f}d ago"


def fmt_delta(val):
    if val is None: return "—"
    return f"{val:+.1f}" if isinstance(val, float) else f"{val:+d}"


def active_filter_chips(filters):
    chips = []
    if filters.get("sector") and not str(filters["sector"]).startswith("All"):
        chips.append(f"Sector: {str(filters['sector']).split(' (')[0]}")
    if filters.get("min_total", 0) > 0: chips.append(f"Total ≥ {filters['min_total']}")
    if filters.get("min_value", 0) > 0: chips.append(f"Value ≥ {filters['min_value']}")
    if filters.get("min_future", 0) > 0: chips.append(f"Future ≥ {filters['min_future']}")
    if filters.get("min_past", 0) > 0: chips.append(f"Past ≥ {filters['min_past']}")
    if filters.get("min_health", 0) > 0: chips.append(f"Health ≥ {filters['min_health']}")
    if filters.get("min_dividend", 0) > 0: chips.append(f"Dividends ≥ {filters['min_dividend']}")
    if filters.get("ownership", "All") != "All": chips.append(filters["ownership"])
    if filters.get("portfolio_status", "All") != "All": chips.append(f"Status: {filters['portfolio_status']}")
    if filters.get("watchlist", "All watchlists") != "All watchlists": chips.append(f"Watchlist: {filters['watchlist']}")
    return chips


# ── SVG Snowflake ──────────────────────────────────────────────────────────────

def svg_snowflake(scores_list, size=210):
    cx = cy = size / 2
    r = size * 0.29
    n = 5
    angles = [-math.pi/2 + (2*math.pi/n)*i for i in range(n)]
    total = sum(scores_list)
    color = "#00d68f" if total >= 20 else "#ffaa00" if total >= 14 else "#ff4d6a"
    glow_id = f"g{abs(hash(str(scores_list)))%9999}"
    grad_id = f"bg{abs(hash(str(scores_list)))%9999}"
    defs = (
        f'<defs>'
        f'<radialGradient id="{grad_id}" cx="50%" cy="45%" r="65%">'
        f'<stop offset="0%" stop-color="#14203b"/><stop offset="100%" stop-color="#09111f"/>'
        f'</radialGradient>'
        f'<filter id="{glow_id}" x="-40%" y="-40%" width="180%" height="180%">'
        f'<feGaussianBlur stdDeviation="3.2" result="blur"/>'
        f'<feMerge><feMergeNode in="blur"/><feMergeNode in="SourceGraphic"/></feMerge>'
        f'</filter></defs>'
    )
    bg = f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="{r*1.15:.1f}" fill="url(#{grad_id})"/>'
    rings = "".join(
        f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="{r*frac:.1f}" fill="none" stroke="#22314f" stroke-opacity="{0.65 if frac<1 else 0.9}" stroke-width="{0.7 if frac<1 else 0.9}"/>'
        for frac in (0.33, 0.67, 1.0)
    )
    spokes = "".join(
        f'<line x1="{cx:.1f}" y1="{cy:.1f}" x2="{cx+r*math.cos(angles[i]):.1f}" y2="{cy+r*math.sin(angles[i]):.1f}" stroke="#22314f" stroke-width="0.75" stroke-opacity="0.85"/>'
        for i in range(n)
    )
    axis_dots = "".join(
        f'<circle cx="{cx+r*math.cos(angles[i]):.1f}" cy="{cy+r*math.sin(angles[i]):.1f}" r="2.2" fill="#29406a"/>'
        for i in range(n)
    )
    score_pts = " ".join(
        f"{cx+r*(scores_list[i]/6)*math.cos(angles[i]):.1f},{cy+r*(scores_list[i]/6)*math.sin(angles[i]):.1f}"
        for i in range(n)
    )
    polygon = (
        f'<polygon points="{score_pts}" fill="{color}" fill-opacity="0.22" '
        f'stroke="{color}" stroke-width="2.1" stroke-linejoin="round" filter="url(#{glow_id})"/>'
    )
    dots = "".join(
        f'<circle cx="{cx+r*(scores_list[i]/6)*math.cos(angles[i]):.1f}" cy="{cy+r*(scores_list[i]/6)*math.sin(angles[i]):.1f}" r="3.0" fill="{color}" opacity="0.95"/>'
        for i in range(n)
    )
    center = f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="2.4" fill="#95b6ff" opacity="0.8"/>'
    dim_labels = ["VALUE", "FUTURE", "PAST", "HEALTH", "DIVS"]
    label_r = r + 24
    labels = ""
    for i, label in enumerate(dim_labels):
        lx = cx + label_r * math.cos(angles[i])
        ly = cy + label_r * math.sin(angles[i])
        anchor = "end" if lx < cx - 8 else "start" if lx > cx + 8 else "middle"
        labels += (
            f'<text x="{lx:.1f}" y="{ly:.1f}" text-anchor="{anchor}" dominant-baseline="central" '
            f'fill="#8ea5cb" fill-opacity="0.92" font-size="8" font-family="monospace" '
            f'font-weight="700" letter-spacing="0.10em">{label}</text>'
        )
    return (
        f'<svg width="{size}" height="{size}" viewBox="0 0 {size} {size}" '
        f'xmlns="http://www.w3.org/2000/svg">{defs}{bg}{rings}{spokes}{axis_dots}{polygon}{dots}{center}{labels}</svg>'
    )


# ── Charts ─────────────────────────────────────────────────────────────────────

def make_radar(row, ticker):
    dims = ["Value", "Future", "Past", "Health", "Dividends"]
    values = [row.get("value_score", 0), row.get("future_score", 0),
              row.get("past_score", 0), row.get("health_score", 0), row.get("dividend_score", 0)]
    color = total_color(sum(values))
    v_closed = values + [values[0]]
    d_closed = dims + [dims[0]]
    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(
        r=v_closed, theta=d_closed, fill="toself",
        fillcolor=color, opacity=0.14,
        line=dict(color=color, width=2), name=ticker,
        hovertemplate="%{theta}: %{r}/6<extra></extra>",
    ))
    fig.update_layout(
        polar=dict(
            bgcolor="#0f1629",
            radialaxis=dict(visible=True, range=[0, 6], tickvals=[2, 4, 6],
                           tickfont=dict(color="#4a5d7a", size=9), gridcolor="#1e2d4a", linecolor="#1e2d4a"),
            angularaxis=dict(tickfont=dict(color="#7a90b5", size=11), gridcolor="#1e2d4a", linecolor="#1e2d4a"),
        ),
        paper_bgcolor="#070d1f", plot_bgcolor="#070d1f",
        font=dict(color="#e8edf5"), margin=dict(l=30, r=30, t=30, b=30), height=290, showlegend=False,
    )
    return fig




def _fmt_pct(v):
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "—"
    return f"{v*100:.2f}%"


def _line_monotonicity(df):
    if df.empty:
        return None
    agg = df.groupby("bucket", as_index=False)["avg_return"].mean().sort_values("bucket")
    if len(agg) < 3:
        return None
    return agg["avg_return"].corr(agg["bucket"])

def make_history_chart(history):
    if len(history) < 2: return None
    df = pd.DataFrame(history)
    df["delta"] = df["total_score"].diff()
    fig = go.Figure()
    fig.add_hline(y=14, line_color="#ffaa00", line_width=1, opacity=0.25)
    fig.add_hline(y=20, line_color="#00d68f", line_width=1, opacity=0.25)
    fig.add_trace(go.Scatter(
        x=df["scan_date"], y=df["total_score"],
        mode="lines+markers", line=dict(color="#3b7dff", width=2),
        marker=dict(size=6, color="#3b7dff"), fill="tozeroy", fillcolor="rgba(59,125,255,0.08)",
        customdata=df[["current_price", "delta"]].fillna("—").values,
        hovertemplate="Date: %{x}<br>Score: %{y}/30<br>Price: %{customdata[0]}<br>Δ: %{customdata[1]}<extra></extra>",
    ))
    fig.update_layout(
        paper_bgcolor="#070d1f", plot_bgcolor="#0f1629", font=dict(color="#e8edf5"),
        xaxis=dict(gridcolor="#1e2d4a", linecolor="#1e2d4a", tickfont=dict(size=10, color="#7a90b5")),
        yaxis=dict(gridcolor="#1e2d4a", linecolor="#1e2d4a", range=[0, 30], tickfont=dict(size=10, color="#7a90b5")),
        margin=dict(l=10, r=10, t=10, b=10), height=180, showlegend=False,
    )
    return fig


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    init_state()
    conn = get_db()

    if conn is None:
        st.title("📈 ASX Scanner")
        st.warning(f"No database found at `{DB_PATH}`.")
        return

    today = get_latest_date(conn)
    if not today:
        st.title("📈 ASX Scanner")
        st.info("Scan in progress — no stocks scored yet. Check back in a few minutes.")
        conn.close()
        return

    yesterday = get_previous_date(conn, today)

    # Sidebar
    with st.sidebar:
        _active_universe = os.environ.get("UNIVERSE", "asx200")
        st.markdown(
            '<p style="font-size:1.08rem;font-weight:700;color:#e8edf5;margin:0 0 2px">ASX Scanner</p>'
            f'<p style="font-size:0.75rem;color:#7a90b5;margin:0">Last scan: <b style="color:#e8edf5">{today}</b></p>'
            f'<p style="font-size:0.72rem;color:#4a5d7a;margin:2px 0 0">Universe: '
            f'<b style="color:#7a90b5">{_active_universe}</b></p>',
            unsafe_allow_html=True,
        )
        st.divider()

        st.markdown('<div class="section-label">Filter Presets</div>', unsafe_allow_html=True)
        preset = st.radio("Preset", list(PRESETS.keys()),
                          index=list(PRESETS.keys()).index(st.session_state.get("filter_preset", "All")),
                          label_visibility="collapsed")
        if preset != st.session_state.get("filter_preset", "All"):
            apply_preset(preset)
            st.rerun()

        st.markdown('<div class="section-label" style="margin-top:8px">Sector</div>', unsafe_allow_html=True)
        sectors = get_sectors_with_count(conn, today)
        selected_sector = st.selectbox("Sector", sectors, label_visibility="collapsed")
        ownership_mode = st.selectbox("Mode", ["All", "Owned", "Discovery"], index=0)
        status_mode = st.selectbox("Portfolio status", ["All", "New", "Hold", "Review", "Trim Candidate", "Review Later (Tax)"], index=0)

        watchlist_names = ["All watchlists"] + [w["name"] for w in get_watchlists(conn)]
        selected_watchlist = st.selectbox("Watchlist", watchlist_names, index=0)

        with st.expander("Score thresholds", expanded=False):
            st.slider("Min total", 0, 30, key="min_total")
            st.slider("Value ≥", 0, 6, key="min_value")
            st.slider("Future ≥", 0, 6, key="min_future")
            st.slider("Past ≥", 0, 6, key="min_past")
            st.slider("Health ≥", 0, 6, key="min_health")
            st.slider("Dividends ≥", 0, 6, key="min_dividend")

        c1, c2 = st.columns(2)
        with c1:
            if st.button("Reset", use_container_width=True):
                apply_preset("All")
                st.rerun()
        with c2:
            if st.button("Refresh", use_container_width=True):
                st.rerun()

        st.markdown('<div class="section-label" style="margin-top:8px">Scanner</div>', unsafe_allow_html=True)

        with st.expander("Add tickers to universe", expanded=False):
            custom_existing = get_custom_tickers(conn)
            new_tickers_input = st.text_input(
                "Tickers (comma-separated, .AX appended automatically)",
                placeholder="e.g. PME, WTC, XRO",
                key="custom_ticker_input",
            )
            if st.button("Add to scanner", use_container_width=True) and new_tickers_input.strip():
                added, skipped = add_custom_tickers(conn, new_tickers_input)
                if added:
                    st.success(f"Added: {', '.join(added)}")
                if skipped:
                    st.caption(f"Already tracked: {', '.join(skipped)}")
                st.rerun()
            if custom_existing:
                st.markdown(
                    '<div style="font-size:0.72rem;color:#7a90b5;margin:6px 0 3px">Currently tracked:</div>',
                    unsafe_allow_html=True,
                )
                for ct in custom_existing:
                    col_t, col_x = st.columns([4, 1])
                    col_t.markdown(
                        f'<span style="font-family:monospace;font-size:0.78rem;color:#9fc0ff">{ct.replace(".AX","")}</span>',
                        unsafe_allow_html=True,
                    )
                    if col_x.button("✕", key=f"rm_{ct}", help=f"Remove {ct}"):
                        remove_custom_ticker(conn, ct)
                        st.rerun()
                st.caption("These tickers are included in every scan alongside the active universe.")

        trigger_scan = st.button("Run scanner now", use_container_width=True)
        if trigger_scan:
            conn.close()
            with st.spinner("Running manual scan. This can take several minutes..."):
                run = subprocess.run(
                    [sys.executable, "scanner.py"],
                    cwd=Path(__file__).parent,
                    capture_output=True,
                    text=True,
                    check=False,
                )
            if run.returncode == 0:
                st.success("Manual scan complete. Refreshing dashboard...")
                st.rerun()
            st.error("Manual scan failed. Review scanner logs below.")
            if run.stderr:
                st.code(run.stderr[-3000:], language="text")
            if run.stdout:
                st.code(run.stdout[-3000:], language="text")
            return

        with st.expander("Portfolio tools", expanded=False):
            new_watchlist = st.text_input("Create watchlist", placeholder="e.g. High Conviction")
            if st.button("Add watchlist", use_container_width=True) and new_watchlist:
                if create_watchlist(conn, new_watchlist):
                    conn.commit()
                    st.success("Watchlist added")
                    st.rerun()
            current_watchlists = get_watchlists(conn)
            if current_watchlists:
                wl_choice = st.selectbox("Edit watchlist", [w["name"] for w in current_watchlists], index=0)
                wl = next((w for w in current_watchlists if w["name"] == wl_choice), None)
                tickers = st.text_input("Add tickers (comma-separated)", placeholder="BHP.AX,CBA.AX")
                if st.button("Add tickers", use_container_width=True) and wl and tickers.strip():
                    added = add_watchlist_tickers(conn, wl["id"], tickers.split(","))
                    conn.commit()
                    st.success(f"Added {added} tickers")
            csv_file = st.file_uploader("Import holdings CSV", type=["csv"])
            if csv_file and st.button("Import holdings", use_container_width=True):
                imported, errors = import_holdings_csv(conn, csv_file.getvalue())
                conn.commit()
                if imported:
                    st.success(f"Imported {imported} holdings")
                for e in errors[:5]:
                    st.warning(e)

        with st.expander("Portfolio rules", expanded=False):
            rules = load_rules(conn)
            rules["max_position_weight"] = st.number_input("Max position weight", min_value=0.01, max_value=1.0, value=float(rules["max_position_weight"]), step=0.01)
            rules["max_sector_weight"] = st.number_input("Max sector weight", min_value=0.05, max_value=1.0, value=float(rules["max_sector_weight"]), step=0.01)
            rules["liquidity_floor_market_cap"] = st.number_input("Liquidity floor (market cap)", min_value=0.0, value=float(rules["liquidity_floor_market_cap"]), step=50_000_000.0)
            rules["max_sector_names"] = st.number_input("Sector overlap warning count", min_value=1, max_value=20, value=int(rules["max_sector_names"]), step=1)
            rules["rebalance_tolerance"] = st.number_input("Rebalance tolerance", min_value=0.0, max_value=0.25, value=float(rules["rebalance_tolerance"]), step=0.005)
            if st.button("Save rules", use_container_width=True):
                save_rules(conn, rules)
                conn.commit()
                st.success("Rules saved")

        st.divider()
        scan_log = get_scan_log(conn)
        if scan_log:
            st.markdown('<div class="section-label">Recent scans</div>', unsafe_allow_html=True)
            for s in scan_log[:3]:
                universe_label = s.get("universe") or ""
                subtitle = f"{s['stocks_scanned']} stocks" + (f" · {universe_label}" if universe_label else "")
                st.markdown(f"**{s['scan_date']}** · {subtitle}",
                            help=f"Failed: {s['stocks_failed']}")

    filters = {
        "min_total": st.session_state.min_total, "min_value": st.session_state.min_value,
        "min_future": st.session_state.min_future, "min_past": st.session_state.min_past,
        "min_health": st.session_state.min_health, "min_dividend": st.session_state.min_dividend,
        "sector": selected_sector,
        "ownership": ownership_mode,
        "portfolio_status": status_mode,
        "watchlist": selected_watchlist,
    }

    tab_discover, tab_movers, tab_detail, tab_compare, tab_validation, tab_health = st.tabs([
        "🔍 Discover", "📊 Movers", "🔎 Deep Dive", "⚖️ Compare", "🧪 Validation", "🩺 Data Health"
    ])

    # ── DISCOVER ──────────────────────────────────────────────────────────────
    with tab_discover:
        stocks = get_all_scores(conn, today, filters)

        chips = active_filter_chips(filters)
        if chips:
            st.markdown(" ".join(f'<span class="filter-chip">{c}</span>' for c in chips), unsafe_allow_html=True)

        st.markdown(f"### {len(stocks)} stocks")

        if not stocks:
            st.info("No stocks match the current filters. Try a different preset or lower the thresholds.")
        else:
            c1, c2, c3, c4 = st.columns(4)
            scores_list = [s["total_score"] for s in stocks]
            c1.metric("Shown", len(stocks))
            c2.metric("Avg score", f"{sum(scores_list)/len(scores_list):.1f}/30")
            c3.metric("Top score", f"{max(scores_list)}/30")
            top = sorted(stocks, key=lambda x: x["health_score"], reverse=True)[0]
            c4.metric("Healthiest balance sheet", top["ticker"].replace(".AX", ""))
            st.divider()

            cols_per_row = 3
            for row_start in range(0, len(stocks), cols_per_row):
                row_stocks = stocks[row_start:row_start + cols_per_row]
                cols = st.columns(cols_per_row)
                for col, stock in zip(cols, row_stocks):
                    with col:
                        ticker_short = stock["ticker"].replace(".AX", "")
                        score = stock["total_score"]
                        color = total_color(score)
                        band = score_band(score)
                        badge_cls = "badge-high" if score >= 20 else "badge-mid" if score >= 14 else "badge-low"
                        snowflake_svg = svg_snowflake([
                            stock["value_score"], stock["future_score"],
                            stock["past_score"], stock["health_score"], stock["dividend_score"]
                        ], size=192)

                        st.markdown(f"""
                        <div class="stock-card">
                          <div class="card-inner">
                            <div class="card-topline">
                              <div>
                                <div class="card-sector">{stock.get('sector','') or '—'}</div>
                              </div>
                              <div class="card-ticker">{ticker_short}</div>
                            </div>
                            <div class="card-name">{stock['company_name']}</div>
                            <div class="card-submeta">
                              <span>{fmt_market_cap(stock.get('market_cap'))}</span>
                              <span class="badge {badge_cls}">{band}</span>
                              <span class="badge badge-blue">{stock.get('portfolio_status','New')}</span>
                            </div>
                            <div class="card-hero">
                              <div class="card-score-wrap">
                                <div class="card-score" style="color:{color}">{score}</div>
                                <div class="card-score-denom">/30</div>
                                <div class="card-score-label">Total score</div>
                                <div class="card-score-detail">Best: {best_dimension_text(stock)}</div>
                              </div>
                              <div class="card-snowflake-wrap">{snowflake_svg}</div>
                            </div>
                            {dim_tiles_html(stock['value_score'],stock['future_score'],stock['past_score'],stock['health_score'],stock['dividend_score'])}
                            <div class="card-footer-note">
                              ${stock['current_price']:.2f} &nbsp;·&nbsp;
                              <span style="color:#4a5d7a;font-size:0.65rem">{(stock.get('template_name') or 'Template')[:18]} · {(stock.get('confidence_badge') or 'N/A')} conf · {(stock.get('data_provider') or '').split(' ')[0][:14]} {fmt_age(stock.get('data_fetched_at'))}</span>
                            </div>
                            <div class="card-footer-note">
                              <span style="color:#7a90b5;font-size:0.68rem">{'Owned' if stock.get('owned') else 'Discovery'} · Fit: {stock.get('portfolio_fit')}</span>
                            </div>
                            <div class="card-footer-note">
                              <span style="color:#9db0cf;font-size:0.66rem">{stock.get('tax_prompt') or ''}</span>
                            </div>
                          </div>
                        </div>
                        """, unsafe_allow_html=True)

                        if st.button("Deep dive →", key=f"dd_{stock['ticker']}"):
                            st.session_state["selected_ticker"] = stock["ticker"]
                            st.rerun()

    # ── MOVERS ────────────────────────────────────────────────────────────────
    with tab_movers:
        st.markdown("### Stocks that moved since last scan")
        if not yesterday:
            st.info("Need at least two scans to show movers.")
        else:
            movers_up, movers_down = get_movers(conn, today, yesterday)
            col_up, col_down = st.columns(2)
            with col_up:
                st.markdown("#### ⬆️ Improving")
                if not movers_up:
                    st.info("No significant improvements today")
                for m in movers_up:
                    ticker = m["ticker"].replace(".AX", "")
                    st.markdown(f"""
                    <div class="fin-card">
                      <div style="display:flex;justify-content:space-between;align-items:center">
                        <span style="font-family:monospace;font-size:0.78rem;color:#9fc0ff">{ticker}</span>
                        <span style="color:#00d68f;font-size:1.1rem;font-weight:700">+{m['change']} ▲</span>
                      </div>
                      <div style="color:#e8edf5;margin-top:4px">{m['company_name']}</div>
                      <div style="color:#7a90b5;font-size:0.78rem;margin-top:2px">{m['today_score']}/30 (was {m['yesterday_score']}) · {major_change_text(m)}</div>
                    </div>
                    """, unsafe_allow_html=True)
            with col_down:
                st.markdown("#### ⬇️ Declining")
                if not movers_down:
                    st.info("No significant declines today")
                for m in movers_down:
                    ticker = m["ticker"].replace(".AX", "")
                    st.markdown(f"""
                    <div class="fin-card">
                      <div style="display:flex;justify-content:space-between;align-items:center">
                        <span style="font-family:monospace;font-size:0.78rem;color:#9fc0ff">{ticker}</span>
                        <span style="color:#ff4d6a;font-size:1.1rem;font-weight:700">{m['change']} ▼</span>
                      </div>
                      <div style="color:#e8edf5;margin-top:4px">{m['company_name']}</div>
                      <div style="color:#7a90b5;font-size:0.78rem;margin-top:2px">{m['today_score']}/30 (was {m['yesterday_score']}) · {major_change_text(m)}</div>
                    </div>
                    """, unsafe_allow_html=True)

    # ── DEEP DIVE ─────────────────────────────────────────────────────────────
    with tab_detail:
        st.markdown("### Deep dive")

        conn2 = get_db()
        all_tickers = conn2.execute(
            "SELECT DISTINCT ticker, company_name FROM scores WHERE scan_date = ? ORDER BY ticker",
            (today,)
        ).fetchall()

        if not all_tickers:
            st.info("No stocks scored yet.")
            conn2.close()
        else:
            ticker_options = {f"{r['ticker'].replace('.AX','')} - {r['company_name']}": r['ticker'] for r in all_tickers}

            default_ticker = st.session_state.get("selected_ticker")
            default_label = next((k for k, v in ticker_options.items() if v == default_ticker), list(ticker_options.keys())[0])
            selected_label = st.selectbox("Choose a stock", list(ticker_options.keys()),
                                          index=list(ticker_options.keys()).index(default_label))
            selected_ticker = ticker_options[selected_label]

            row = conn2.execute(
                "SELECT * FROM scores WHERE ticker = ? AND scan_date = ?",
                (selected_ticker, today)
            ).fetchone()

            if row:
                row = dict(row)
                dims = json.loads(row["dimension_detail"])

                col1, col2 = st.columns([1, 1])
                with col1:
                    score = row["total_score"]
                    color = total_color(score)
                    st.markdown(
                        f'<div style="font-size:2.2rem;font-weight:800;color:{color};font-family:monospace;line-height:1">'
                        f'{score}<span style="font-size:1rem;color:#5e7291">/30</span></div>'
                        f'<div style="color:#7a90b5;font-size:0.78rem;margin-bottom:8px">{score_band(score)}</div>',
                        unsafe_allow_html=True,
                    )
                    st.markdown(f"## {selected_ticker.replace('.AX', '')}")
                    st.markdown(f"**{row['company_name']}**")
                    st.markdown(f"*{row.get('sector','') or ''}* › *{row.get('industry','') or ''}*")

                    provider = row.get("data_provider", "unknown")
                    fetched_at = row.get("data_fetched_at")
                    completeness = (row.get("data_completeness") or 0) * 100
                    conf_score = (row.get("confidence_score") or 0) * 100
                    conf_badge = row.get("confidence_badge") or "N/A"
                    age_str = fmt_age(fetched_at)
                    st.markdown(
                        f'<p style="color:#4a5d7a;font-size:0.73rem;margin-top:8px">'
                        f'📡 {provider} &nbsp;·&nbsp; {age_str} &nbsp;·&nbsp; {completeness:.0f}% complete &nbsp;·&nbsp; '
                        f'🧩 {(row.get("template_name") or "General")} &nbsp;·&nbsp; 🔒 {conf_badge} ({conf_score:.0f}%)</p>',
                        unsafe_allow_html=True,
                    )

                    m1, m2, m3, m4 = st.columns(4)
                    price = row.get("current_price")
                    m1.metric("Price", f"${price:.2f}" if price else "—")
                    m2.metric("Market Cap", fmt_market_cap(row.get("market_cap")))
                    m3.metric("Confidence", f"{conf_badge} ({conf_score:.0f}%)")
                    m4.metric("Adj Score", f"{(row.get('adjusted_total') or 0):.1f}/100")
                    p = apply_portfolio_overlay(conn2, [row], {"ownership": "All", "portfolio_status": "All", "watchlist": "All watchlists"})
                    if p:
                        prow = p[0]
                        m5, m6, m7, m8 = st.columns(4)
                        m5.metric("Portfolio status", prow.get("portfolio_status", "New"))
                        m6.metric("Current vs target", f"{prow.get('current_weight',0)*100:.1f}% / {prow.get('target_weight',0)*100:.1f}%")
                        gain_val = prow.get("unrealised_gain_estimate")
                        gain_pct = prow.get("unrealised_gain_pct")
                        gain_label = "—" if gain_val is None else f"${gain_val:,.0f} ({(gain_pct or 0)*100:+.1f}%)"
                        m7.metric("Unrealised gain est.", gain_label)
                        cgt_label = "Eligible" if prow.get("cgt_discount_eligible") else (
                            f"{prow.get('days_to_cgt_discount')} days" if prow.get("days_to_cgt_discount") is not None else "—"
                        )
                        m8.metric("12m CGT discount", cgt_label)
                        if prow.get("tax_prompt"):
                            st.caption(f"🧾 {prow.get('tax_prompt')}")
                        if prow.get("dividend_income_share", 0) >= 0.30:
                            st.caption(f"⚠️ Dividend concentration risk: {prow.get('dividend_income_share',0)*100:.1f}% of portfolio income from this holding.")

                with col2:
                    snowflake_svg_detail = svg_snowflake([
                        row.get('value_score',0), row.get('future_score',0),
                        row.get('past_score',0), row.get('health_score',0),
                        row.get('dividend_score',0)
                    ], size=260)
                    st.markdown(
                        f'<div style="display:flex;justify-content:center;align-items:center;padding:10px 0">{snowflake_svg_detail}</div>',
                        unsafe_allow_html=True
                    )

                # ── Financial snapshot (Simply Wall St style) ────────────────
                raw_detail = json.loads(row.get("raw_info") or "{}")
                if raw_detail:
                    st.divider()
                    render_financial_snapshot(raw_detail, row)

                # ── Price history chart ──────────────────────────────────────
                price_data = fetch_price_history_cached(selected_ticker)
                if price_data:
                    st.divider()
                    st.markdown("#### Price history (1 year)")
                    tgt = raw_detail.get("targetMeanPrice")
                    tgt_low = raw_detail.get("targetLowPrice")
                    tgt_high = raw_detail.get("targetHighPrice")
                    fig_price = make_price_chart(
                        price_data, tgt, tgt_low, tgt_high,
                        selected_ticker.replace(".AX", ""),
                    )
                    if fig_price:
                        st.plotly_chart(fig_price, use_container_width=True, key="detail_price_chart")
                        if tgt:
                            analyst_count = raw_detail.get("numberOfAnalystOpinions") or 0
                            caption_parts = [f"Analyst consensus target: ${tgt:.2f}"]
                            if tgt_low and tgt_high:
                                caption_parts.append(f"range ${tgt_low:.2f}–${tgt_high:.2f}")
                            if analyst_count:
                                caption_parts.append(f"{analyst_count} analysts")
                            st.caption(" · ".join(caption_parts))

                st.divider()

                for idx, (dim_key, label, desc, explanation) in enumerate([
                    (k, *v) for k, v in DIM_EXPLANATIONS.items()
                ]):
                    dim = dims.get(dim_key, {})
                    dim_score = dim.get("score", 0)
                    factors = dim.get("factors", {})
                    data = dim.get("data", {})

                    with st.expander(f"{label} · {dim_score}/6 · {desc}", expanded=(idx == 0)):

                        # Plain English explanation of this dimension
                        st.markdown(
                            f'<div class="explain-box">{explanation}</div>',
                            unsafe_allow_html=True
                        )

                        factor_items = list(factors.items())
                        strong = [(n, v) for n, v in factor_items if (v.get("score", 0) >= 0.8)]
                        weak = [(n, v) for n, v in factor_items if (v.get("score", 0) < 0.55)]

                        cpa, cpf = st.columns(2)
                        with cpa:
                            st.markdown("**Strong factors**")
                            for name, meta in strong:
                                exp_title, exp_text = CHECK_EXPLANATIONS.get(name, (name.replace("_", " ").title(), ""))
                                factor_score = meta.get("score", 0)
                                st.markdown(
                                    f'<div style="margin-bottom:6px">'
                                    f'✅ <b style="color:#e8edf5">{exp_title}</b> <span style="color:#7a90b5">({factor_score:.2f})</span>'
                                    f'<div style="color:#7a90b5;font-size:0.75rem;margin-left:20px;line-height:1.4">{exp_text}</div>'
                                    f'</div>',
                                    unsafe_allow_html=True
                                )
                        with cpf:
                            st.markdown("**Weak factors**")
                            for name, meta in weak:
                                exp_title, exp_text = CHECK_EXPLANATIONS.get(name, (name.replace("_", " ").title(), ""))
                                factor_score = meta.get("score", 0)
                                st.markdown(
                                    f'<div style="margin-bottom:6px">'
                                    f'❌ <b style="color:#e8edf5">{exp_title}</b> <span style="color:#7a90b5">({factor_score:.2f})</span>'
                                    f'<div style="color:#7a90b5;font-size:0.75rem;margin-left:20px;line-height:1.4">{exp_text}</div>'
                                    f'</div>',
                                    unsafe_allow_html=True
                                )

                        if data:
                            data_items = [(k, v) for k, v in data.items() if v is not None]
                            if data_items:
                                st.markdown("**Key metrics**")
                                n_cols = min(4, len(data_items))
                                d_cols = st.columns(n_cols)
                                for i, (k, v) in enumerate(data_items):
                                    lbl = k.replace("_", " ").title()
                                    if isinstance(v, float):
                                        display = f"{v:,.2f}"
                                    elif isinstance(v, int) and abs(v) > 1_000_000:
                                        display = fmt_market_cap(v)
                                    else:
                                        display = str(v)
                                    d_cols[i % n_cols].metric(lbl, display)

                st.divider()
                st.markdown("#### Score history")
                history = get_stock_history(conn2, selected_ticker)
                if len(history) > 1:
                    fig = make_history_chart(history)
                    if fig:
                        st.plotly_chart(fig, use_container_width=True, key="detail_history")
                        latest_delta = history[-1]["total_score"] - history[-2]["total_score"]
                        st.caption(f"Latest move: {fmt_delta(latest_delta)} vs prior scan. Reference bands: 14 = balanced, 20 = strong.")
                        st.markdown("#### Change signals")
                        hist_by_date = {h["scan_date"]: h for h in history}
                        from datetime import datetime, timedelta
                        today_dt = datetime.fromisoformat(today)
                        deltas = {}
                        for days in (30, 90, 180):
                            target = (today_dt - timedelta(days=days)).date().isoformat()
                            prior_dates = sorted(d for d in hist_by_date.keys() if d <= target)
                            if prior_dates:
                                prior = hist_by_date[prior_dates[-1]]
                                deltas[days] = round((history[-1]["total_score"] or 0) - (prior["total_score"] or 0), 2)
                            else:
                                deltas[days] = None
                        d1, d2, d3 = st.columns(3)
                        d1.metric("30-day Δ", fmt_delta(deltas[30]) if deltas[30] is not None else "—")
                        d2.metric("90-day Δ", fmt_delta(deltas[90]) if deltas[90] is not None else "—")
                        d3.metric("180-day Δ", fmt_delta(deltas[180]) if deltas[180] is not None else "—")

                        if len(history) >= 2:
                            prev = history[-2]
                            contributions = {
                                "Value": (history[-1].get("value_score", 0) or 0) - (prev.get("value_score", 0) or 0),
                                "Future": (history[-1].get("future_score", 0) or 0) - (prev.get("future_score", 0) or 0),
                                "Past": (history[-1].get("past_score", 0) or 0) - (prev.get("past_score", 0) or 0),
                                "Health": (history[-1].get("health_score", 0) or 0) - (prev.get("health_score", 0) or 0),
                                "Dividends": (history[-1].get("dividend_score", 0) or 0) - (prev.get("dividend_score", 0) or 0),
                            }
                            top_move = sorted(contributions.items(), key=lambda kv: abs(kv[1]), reverse=True)[:2]
                            st.caption("Factor attribution: " + ", ".join(f"{k} {fmt_delta(v)}" for k, v in top_move))
                else:
                    st.caption("Score history builds up over multiple scans.")

                exp = None
                try:
                    from scorer import score_stock
                    exp = score_stock(raw_detail, selected_ticker).get("explanation")
                except Exception:
                    exp = None

                if exp:
                    st.divider()
                    st.markdown("#### Why buy / avoid / review")
                    cexp1, cexp2, cexp3 = st.columns(3)
                    with cexp1:
                        st.markdown("**Top positives**")
                        for line in exp.get("why_buy", [])[:3]:
                            st.caption(line)
                    with cexp2:
                        st.markdown("**Top concerns**")
                        for line in exp.get("why_avoid", [])[:3]:
                            st.caption(line)
                    with cexp3:
                        st.markdown("**Why review now**")
                        for line in exp.get("why_review", [])[:3]:
                            st.caption(line)
                        st.caption(exp.get("confidence_note", ""))

                narrative = row.get("narrative")
                if narrative:
                    st.divider()
                    st.markdown("#### Narrative")
                    st.markdown(
                        f'<div style="background:#0f1629;border:1px solid #1e2d4a;border-left:3px solid #3b7dff;'
                        f'border-radius:12px;padding:20px 24px;line-height:1.9;color:#e8edf5;font-size:0.9rem;">'
                        f'{narrative}</div>',
                        unsafe_allow_html=True,
                    )
                else:
                    st.caption("Narrative will be generated on next scan.")

            conn2.close()

    # ── COMPARE ──────────────────────────────────────────────────────────────
    with tab_compare:
        st.markdown("### Compare two stocks side-by-side")

        conn_cmp = get_db()
        all_tickers_cmp = conn_cmp.execute(
            "SELECT DISTINCT ticker, company_name FROM scores WHERE scan_date = ? ORDER BY ticker",
            (today,)
        ).fetchall()

        if not all_tickers_cmp:
            st.info("No stocks scored yet.")
            conn_cmp.close()
        else:
            ticker_opts_cmp = {
                f"{r['ticker'].replace('.AX','')} — {r['company_name']}": r['ticker']
                for r in all_tickers_cmp
            }
            labels = list(ticker_opts_cmp.keys())

            cmp_col_a, cmp_col_b = st.columns(2)
            with cmp_col_a:
                label_a = st.selectbox("Stock A", labels, index=0, key="cmp_a")
            with cmp_col_b:
                label_b = st.selectbox("Stock B", labels, index=min(1, len(labels)-1), key="cmp_b")

            ticker_a = ticker_opts_cmp[label_a]
            ticker_b = ticker_opts_cmp[label_b]

            row_a = conn_cmp.execute(
                "SELECT * FROM scores WHERE ticker = ? AND scan_date = ?", (ticker_a, today)
            ).fetchone()
            row_b = conn_cmp.execute(
                "SELECT * FROM scores WHERE ticker = ? AND scan_date = ?", (ticker_b, today)
            ).fetchone()

            if row_a and row_b:
                row_a = dict(row_a)
                row_b = dict(row_b)
                raw_a = json.loads(row_a.get("raw_info") or "{}")
                raw_b = json.loads(row_b.get("raw_info") or "{}")

                st.divider()

                # ── Snowflakes + score overview side by side ──────────────────
                col_a, col_b = st.columns(2)
                for col, row_x, ticker_x, raw_x in [
                    (col_a, row_a, ticker_a, raw_a),
                    (col_b, row_b, ticker_b, raw_b),
                ]:
                    with col:
                        score_x = row_x["total_score"]
                        color_x = total_color(score_x)
                        sf_svg = svg_snowflake([
                            row_x.get('value_score', 0), row_x.get('future_score', 0),
                            row_x.get('past_score', 0), row_x.get('health_score', 0),
                            row_x.get('dividend_score', 0),
                        ], size=180)
                        st.markdown(
                            f'<div style="text-align:center">'
                            f'<div style="font-family:monospace;font-size:2rem;font-weight:800;color:{color_x}">'
                            f'{score_x}<span style="font-size:0.9rem;color:#5e7291">/30</span></div>'
                            f'<div style="font-size:0.75rem;color:#7a90b5;margin-bottom:4px">{score_band(score_x)}</div>'
                            f'<div style="font-size:1.0rem;font-weight:700;color:#e8edf5">{ticker_x.replace(".AX","")}</div>'
                            f'<div style="font-size:0.78rem;color:#7a90b5;margin-bottom:8px">{row_x["company_name"]}</div>'
                            f'<div style="display:flex;justify-content:center">{sf_svg}</div>'
                            f'</div>',
                            unsafe_allow_html=True,
                        )

                st.divider()

                # ── Score dimension comparison table ──────────────────────────
                st.markdown("#### Score breakdown")
                dims_labels = [("Value", "value_score"), ("Future", "future_score"),
                               ("Past", "past_score"), ("Health", "health_score"),
                               ("Dividends", "dividend_score"), ("Total", "total_score")]
                hdr = st.columns([2, 1, 1])
                hdr[0].markdown("**Dimension**")
                hdr[1].markdown(f"**{ticker_a.replace('.AX','')}**")
                hdr[2].markdown(f"**{ticker_b.replace('.AX','')}**")
                for label_d, key_d in dims_labels:
                    max_d = 30 if key_d == "total_score" else 6
                    va = row_a.get(key_d, 0) or 0
                    vb = row_b.get(key_d, 0) or 0
                    row_cols = st.columns([2, 1, 1])
                    row_cols[0].markdown(label_d)
                    color_va = "#00d68f" if va > vb else ("#ff4d6a" if va < vb else "#e8edf5")
                    color_vb = "#00d68f" if vb > va else ("#ff4d6a" if vb < va else "#e8edf5")
                    row_cols[1].markdown(
                        f'<span style="color:{color_va};font-family:monospace;font-weight:700">'
                        f'{va}/{max_d}</span>', unsafe_allow_html=True
                    )
                    row_cols[2].markdown(
                        f'<span style="color:{color_vb};font-family:monospace;font-weight:700">'
                        f'{vb}/{max_d}</span>', unsafe_allow_html=True
                    )

                st.divider()

                # ── Key metrics comparison ────────────────────────────────────
                st.markdown("#### Key metrics")

                def _cmp_metric(label_m, val_a, val_b, fmt_fn, higher_is_better=True):
                    row_m = st.columns([2, 1, 1])
                    row_m[0].markdown(f'<span style="color:#7a90b5;font-size:0.83rem">{label_m}</span>', unsafe_allow_html=True)
                    if val_a is not None and val_b is not None:
                        better_a = (val_a > val_b) if higher_is_better else (val_a < val_b)
                        ca = "#00d68f" if better_a else ("#ff4d6a" if val_a != val_b else "#e8edf5")
                        cb = "#00d68f" if not better_a else ("#ff4d6a" if val_a != val_b else "#e8edf5")
                    else:
                        ca = cb = "#e8edf5"
                    row_m[1].markdown(f'<span style="color:{ca};font-family:monospace">{fmt_fn(val_a)}</span>', unsafe_allow_html=True)
                    row_m[2].markdown(f'<span style="color:{cb};font-family:monospace">{fmt_fn(val_b)}</span>', unsafe_allow_html=True)

                _p = lambda v: f"{v*100:.1f}%" if v is not None else "—"
                _r = lambda v: f"{v:.2f}x" if v is not None else "—"
                _d = lambda v: f"${v:.2f}" if v is not None else "—"
                _c = lambda v: _cap(v) if v is not None else "—"

                _cmp_metric("Price", raw_a.get("currentPrice") or raw_a.get("regularMarketPrice"), raw_b.get("currentPrice") or raw_b.get("regularMarketPrice"), _d, higher_is_better=False)
                _cmp_metric("Analyst Target", raw_a.get("targetMeanPrice"), raw_b.get("targetMeanPrice"), _d)
                _cmp_metric("P/E (TTM)", raw_a.get("trailingPE"), raw_b.get("trailingPE"), _r, higher_is_better=False)
                _cmp_metric("Fwd P/E", raw_a.get("forwardPE"), raw_b.get("forwardPE"), _r, higher_is_better=False)
                _cmp_metric("EV/EBITDA", raw_a.get("enterpriseToEbitda"), raw_b.get("enterpriseToEbitda"), _r, higher_is_better=False)
                _cmp_metric("Price/Book", raw_a.get("priceToBook"), raw_b.get("priceToBook"), _r, higher_is_better=False)
                _cmp_metric("Gross Margin", raw_a.get("grossMargins"), raw_b.get("grossMargins"), _p)
                _cmp_metric("Op Margin", raw_a.get("operatingMargins"), raw_b.get("operatingMargins"), _p)
                _cmp_metric("Net Margin", raw_a.get("profitMargins"), raw_b.get("profitMargins"), _p)
                _cmp_metric("ROE", raw_a.get("returnOnEquity"), raw_b.get("returnOnEquity"), _p)
                _cmp_metric("Revenue Growth", raw_a.get("revenueGrowth"), raw_b.get("revenueGrowth"), _p)
                _cmp_metric("Earnings Growth", raw_a.get("earningsGrowth"), raw_b.get("earningsGrowth"), _p)
                _cmp_metric("Dividend Yield", raw_a.get("dividendYield"), raw_b.get("dividendYield"), _p)
                _cmp_metric("Current Ratio", raw_a.get("currentRatio"), raw_b.get("currentRatio"), _r)
                de_a = (raw_a.get("debtToEquity") or 0) / 100 if raw_a.get("debtToEquity") else None
                de_b = (raw_b.get("debtToEquity") or 0) / 100 if raw_b.get("debtToEquity") else None
                _cmp_metric("Debt/Equity", de_a, de_b, _r, higher_is_better=False)
                _cmp_metric("Free Cash Flow", raw_a.get("freeCashflow"), raw_b.get("freeCashflow"), _c)
                _cmp_metric("Market Cap", raw_a.get("marketCap"), raw_b.get("marketCap"), _c)

                st.divider()

                # ── Price histories on one chart ──────────────────────────────
                st.markdown("#### Price history (1 year)")
                pd_a = fetch_price_history_cached(ticker_a)
                pd_b = fetch_price_history_cached(ticker_b)
                if pd_a or pd_b:
                    fig_cmp = go.Figure()
                    if pd_a and pd_a.get("closes"):
                        # Normalise to 100 for fair comparison
                        closes_a = pd_a["closes"]
                        base_a = closes_a[0] or 1
                        norm_a = [c / base_a * 100 if c else None for c in closes_a]
                        fig_cmp.add_trace(go.Scatter(
                            x=pd_a["dates"], y=norm_a,
                            name=ticker_a.replace(".AX", ""),
                            line=dict(color="#3b7dff", width=2),
                        ))
                    if pd_b and pd_b.get("closes"):
                        closes_b = pd_b["closes"]
                        base_b = closes_b[0] or 1
                        norm_b = [c / base_b * 100 if c else None for c in closes_b]
                        fig_cmp.add_trace(go.Scatter(
                            x=pd_b["dates"], y=norm_b,
                            name=ticker_b.replace(".AX", ""),
                            line=dict(color="#00d68f", width=2),
                        ))
                    fig_cmp.add_hline(y=100, line_dash="dot", line_color="#4a5d7a", line_width=1)
                    fig_cmp.update_layout(
                        template="plotly_dark", height=300,
                        paper_bgcolor="#0c1324", plot_bgcolor="#0c1324",
                        margin=dict(l=0, r=10, t=10, b=10),
                        xaxis=dict(gridcolor="#1a2540"),
                        yaxis=dict(gridcolor="#1a2540", ticksuffix="%"),
                        legend=dict(orientation="h", y=1.08),
                    )
                    st.plotly_chart(fig_cmp, use_container_width=True, key="cmp_price_chart")
                    st.caption("Normalised to 100 at start of 1-year period for a like-for-like comparison.")
                else:
                    st.caption("Live price history unavailable — yfinance may not be installed.")

            conn_cmp.close()

    # ── VALIDATION ───────────────────────────────────────────────────────────
    with tab_validation:
        st.markdown("### Backtest & predictive power")
        versions = available_versions(conn)
        cva, cvb, cvc, cvd = st.columns([1.1, 1, 1, 1])
        with cva:
            selected_version = st.selectbox("Scoring model version", versions, index=max(len(versions)-1, 0))
        with cvb:
            weighting = st.selectbox("Weighting", ["equal", "score"])
        with cvc:
            tc_bps = st.number_input("Txn cost (bps)", min_value=0.0, max_value=200.0, value=10.0, step=1.0)
        with cvd:
            bucket_mode = st.selectbox("Buckets", ["Deciles (10)", "Quintiles (5)"])

        bt = run_backtest(conn, scoring_model_version=selected_version, weighting=weighting, transaction_cost_bps=tc_bps)
        if bt.summary.get("error"):
            st.info(bt.summary["error"])
        else:
            m1, m2, m3, m4, m5 = st.columns(5)
            m1.metric("CAGR", _fmt_pct(bt.summary.get("cagr")))
            m2.metric("Max drawdown", _fmt_pct(bt.summary.get("max_drawdown")))
            m3.metric("Sharpe", f"{bt.summary.get('sharpe', 0):.2f}")
            m4.metric("Avg turnover", _fmt_pct(bt.summary.get("avg_turnover")))
            m5.metric("Hit rate", _fmt_pct(bt.summary.get("hit_rate")))
            if bt.summary.get("benchmark_cagr") is not None:
                st.caption(f"Benchmark ({bt.summary.get('benchmark_ticker')}): CAGR {_fmt_pct(bt.summary.get('benchmark_cagr'))}")
            else:
                st.caption("Benchmark vs ASX 200 requires benchmark rows (ticker ^AXJO) in scores table.")

            eq = bt.monthly.copy()
            eq["period_end"] = pd.to_datetime(eq["period_end"])
            fig_eq = go.Figure()
            fig_eq.add_trace(go.Scatter(x=eq["period_end"], y=eq["equity"], name="Strategy", line=dict(color="#3b7dff")))
            fig_eq.update_layout(template="plotly_dark", height=320, margin=dict(l=10, r=10, t=10, b=10),
                                 paper_bgcolor="#0c1324", plot_bgcolor="#0c1324")
            st.plotly_chart(fig_eq, use_container_width=True, key="bt_equity")

            csv_paths = export_backtest_csv(bt, Path("/data/exports"), f"backtest_{selected_version}_{weighting}")
            st.caption(f"CSV export written: {csv_paths['summary']}")

        st.divider()
        st.markdown("### Decile/quintile forward returns")
        bucket_count = 10 if bucket_mode.startswith("Decile") else 5
        bucket_df, factor_df = forward_bucket_analysis(conn, scoring_model_version=selected_version, bucket_count=bucket_count)

        if bucket_df.empty:
            st.info("Need more historical scans to compute bucket analysis.")
        else:
            horizon = st.selectbox("Forward horizon", [1, 3, 6, 12], index=0, key="bucket_h")
            snap = bucket_df[bucket_df["horizon_m"] == horizon]
            agg = snap.groupby("bucket", as_index=False)["avg_return"].mean()
            mono = _line_monotonicity(snap)
            st.caption("Monotonicity (bucket vs avg return correlation): " + (f"{mono:.2f}" if mono is not None else "n/a"))
            fig_b = px.bar(agg, x="bucket", y="avg_return", template="plotly_dark", color="avg_return",
                           color_continuous_scale="Blues", labels={"avg_return": "Avg return", "bucket": "Bucket"})
            fig_b.update_layout(height=320, margin=dict(l=10, r=10, t=10, b=10), paper_bgcolor="#0c1324", plot_bgcolor="#0c1324")
            st.plotly_chart(fig_b, use_container_width=True, key="bucket_bar")

            st.markdown("#### Factor buckets")
            fac = factor_df[factor_df["horizon_m"] == horizon]
            if fac.empty:
                st.caption("No factor-bucket rows for selected horizon.")
            else:
                fac_agg = fac.groupby(["factor", "bucket"], as_index=False)["avg_return"].mean()
                fig_f = px.line(fac_agg, x="bucket", y="avg_return", color="factor", markers=True, template="plotly_dark")
                fig_f.update_layout(height=360, margin=dict(l=10, r=10, t=10, b=10), paper_bgcolor="#0c1324", plot_bgcolor="#0c1324")
                st.plotly_chart(fig_f, use_container_width=True, key="factor_lines")

        st.info("Survivorship-bias note: this analysis uses only tickers present in stored scans and does not include delisted names unless they were captured historically.")

    # ── DATA HEALTH ───────────────────────────────────────────────────────────
    with tab_health:
        st.markdown("#### Provider Status")

        providers_info = [
            ("yahooquery", None, "Yahoo Finance", "Free, no key"),
            ("finnhub", "FINNHUB_API_KEY", "Finnhub", "60 calls/min"),
            ("fmp", "FMP_API_KEY", "FMP", "250 calls/day"),
            ("alpha_vantage", "ALPHA_VANTAGE_API_KEY", "Alpha Vantage", "25 calls/day"),
        ]

        try:
            health_rows = conn.execute("""
                SELECT provider, COUNT(*) as total, SUM(success) as successes
                FROM fetch_log WHERE fetched_at > ? GROUP BY provider
            """, (time.time() - 86400,)).fetchall()
            health_lookup = {r["provider"]: dict(r) for r in health_rows}
        except Exception:
            health_lookup = {}

        cols = st.columns(4)
        for i, (name, key_env, display_name, limit_note) in enumerate(providers_info):
            with cols[i]:
                configured = key_env is None or bool(os.environ.get(key_env, ""))
                h = health_lookup.get(name, {})
                if not configured:
                    dot, status_text, status_color = "⚫", "No API key", "#4a5d7a"
                elif h:
                    rate = h.get("successes", 0) / max(h.get("total", 1), 1)
                    if rate >= 0.8: dot, status_text, status_color = "🟢", "Healthy", "#00d68f"
                    elif rate >= 0.5: dot, status_text, status_color = "🟡", "Degraded", "#ffaa00"
                    else: dot, status_text, status_color = "🔴", "Failing", "#ff4d6a"
                else:
                    dot, status_text, status_color = "⚪", "No data", "#7a90b5"
                total_calls = h.get("total", 0)
                rate_str = f"{total_calls} calls · {int(h.get('successes',0)/max(total_calls,1)*100)}% ok" if total_calls else "—"
                st.markdown(f"""
                <div class="fin-card" style="text-align:center;">
                  <div style="font-size:1.4rem">{dot}</div>
                  <div style="font-family:monospace;font-size:0.78rem;color:#e8edf5;margin-top:6px">{display_name}</div>
                  <div style="color:{status_color};font-size:0.72rem;margin-top:3px;font-weight:600">{status_text}</div>
                  <div style="color:#4a5d7a;font-size:0.68rem;margin-top:4px">{rate_str}</div>
                  <div style="color:#4a5d7a;font-size:0.65rem;margin-top:2px">{limit_note}</div>
                </div>
                """, unsafe_allow_html=True)

        st.divider()
        st.markdown("#### Cache")
        cache = get_cache_stats(conn)
        cc1, cc2, cc3 = st.columns(3)
        cc1.metric("Total cached", cache["total"])
        cc2.metric("Fresh (< 24h)", cache["fresh"])
        cc3.metric("Stale (> 24h)", cache["stale"])
        if cache["total"] > 0:
            fresh_pct = cache["fresh"] / cache["total"]
            st.progress(fresh_pct, text=f"{fresh_pct*100:.0f}% of cache is fresh")

        st.divider()
        st.markdown("#### Recent fetch activity")
        fetch_log = get_fetch_log(conn, limit=40)
        if fetch_log:
            log_data = []
            for entry in fetch_log:
                age = (time.time() - entry["fetched_at"]) / 60
                log_data.append({
                    "Age": f"{age:.0f}m ago", "Ticker": entry["ticker"],
                    "Provider": entry["provider"], "Result": "✅" if entry["success"] else "❌",
                    "Complete": f"{(entry['completeness'] or 0)*100:.0f}%",
                    "Note": (entry.get("reason") or "")[:50],
                })
            st.dataframe(pd.DataFrame(log_data), use_container_width=True, hide_index=True)
        else:
            st.caption("No fetch activity recorded yet.")

        st.divider()
        st.markdown("#### Scan history")
        scan_log = get_scan_log(conn)
        if scan_log:
            scan_data = []
            for s in scan_log:
                providers_str = ""
                if s.get("provider_summary"):
                    try:
                        ps = json.loads(s["provider_summary"])
                        # provider_summary is now {provider: {attempts, successes, ...}}
                        if isinstance(ps, dict) and ps and isinstance(next(iter(ps.values())), dict):
                            providers_str = ", ".join(
                                f"{k}: {v.get('successes', 0)}/{v.get('attempts', 0)}"
                                for k, v in ps.items()
                            )
                        else:
                            providers_str = ", ".join(f"{k}: {v}" for k, v in ps.get("counts", {}).items())
                    except Exception:
                        pass
                dur = s.get("duration_seconds")
                dur_str = f"{dur:.0f}s" if dur else "—"
                scan_data.append({
                    "Date": s["scan_date"],
                    "Universe": s.get("universe") or "—",
                    "Scanned": s["stocks_scanned"],
                    "Failed": s["stocks_failed"],
                    "Duration": dur_str,
                    "Providers": providers_str,
                    "Run ID": (s.get("run_id") or "")[:8],
                })
            st.dataframe(pd.DataFrame(scan_data), use_container_width=True, hide_index=True)
        else:
            st.caption("No completed scans yet.")

    conn.close()


if __name__ == "__main__":
    main()
