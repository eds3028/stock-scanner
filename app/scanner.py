"""
Nightly scanner - uses DataOrchestrator for resilient multi-provider data fetching.
Runs scoring engine, generates Ollama narratives, stores in SQLite.
"""

import sqlite3
import json
import time
import logging
import os
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import requests

from orchestrator import DataOrchestrator
from universe import get_universe
from run_logger import ScanRunLogger

OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://localhost:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "llama3.1:8b")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

DB_PATH = Path("/data/stocks.db")



def build_narrative_prompt(ticker: str, info: dict, score_result: dict) -> str:
    dims = score_result["dimensions"]
    name = info.get("longName") or info.get("shortName", ticker)
    sector = info.get("sector", "Unknown")
    industry = info.get("industry", "Unknown")
    price = info.get("currentPrice") or info.get("regularMarketPrice")
    market_cap = info.get("marketCap", 0)
    pe = info.get("trailingPE") or info.get("forwardPE")
    roe = info.get("returnOnEquity")
    de = info.get("debtToEquity")
    div_yield = info.get("dividendYield", 0)
    target = info.get("targetMeanPrice")
    fcf = info.get("freeCashflow")
    revenue_growth = info.get("revenueGrowth")
    earnings_growth = info.get("earningsGrowth")

    def fmt_cap(v):
        if not v: return "N/A"
        if v >= 1e12: return f"${v/1e12:.1f}T"
        if v >= 1e9: return f"${v/1e9:.1f}B"
        if v >= 1e6: return f"${v/1e6:.1f}M"
        return f"${v:,.0f}"

    def pct(v):
        return f"{v*100:.1f}%" if v is not None else "N/A"

    failed = []
    passed = []
    for dim_name, dim_data in dims.items():
        for check_name, result in dim_data.get("checks", {}).items():
            label = check_name.replace("_", " ").title()
            if result:
                passed.append(f"{dim_name.title()}: {label}")
            else:
                failed.append(f"{dim_name.title()}: {label}")

    upside = ""
    if price and target:
        pct_upside = ((target - price) / price) * 100
        upside = f"${target:.2f} ({pct_upside:+.1f}%)"

    return f"""You are a concise financial analyst writing plain-English investment narratives for an ASX stock screener. Write for an intelligent investor who is new to stock picking. Be direct, specific, and avoid generic filler phrases.

STOCK: {name} ({ticker.replace('.AX','')}.ASX)
SECTOR: {sector} | {industry}
PRICE: ${price:.2f} | MARKET CAP: {fmt_cap(market_cap)}
ANALYST TARGET: {upside or 'N/A'}

SCORES (out of 6 per dimension, 30 total):
- Value:     {dims['value']['score']}/6
- Future:    {dims['future']['score']}/6
- Past:      {dims['past']['score']}/6
- Health:    {dims['health']['score']}/6
- Dividends: {dims['dividends']['score']}/6
- TOTAL:     {score_result['total_score']}/30

KEY METRICS:
- P/E Ratio: {f"{pe:.1f}x" if pe else "N/A"}
- ROE: {pct(roe)}
- Debt/Equity: {f"{de/100:.2f}" if de else "N/A"}
- Dividend Yield: {pct(div_yield)}
- Revenue Growth: {pct(revenue_growth)}
- Earnings Growth: {pct(earnings_growth)}
- Free Cash Flow: {fmt_cap(fcf)}

CHECKS PASSED ({len(passed)}): {", ".join(passed[:8]) if passed else "None"}
CHECKS FAILED ({len(failed)}): {", ".join(failed[:8]) if failed else "None"}

Write exactly 3 paragraphs:
1. What this company does and what kind of business it is (2-3 sentences)
2. Where it scores well and why, and where the weaknesses or risks are (3-4 sentences)
3. What type of investor this suits - income, growth, value, or avoid - and one specific thing to watch (2-3 sentences)

Do not use bullet points. Do not use headings. Do not start with the company name. Write in plain English."""


def generate_narrative(ticker: str, info: dict, score_result: dict) -> str | None:
    prompt = build_narrative_prompt(ticker, info, score_result)
    try:
        response = requests.post(
            f"{OLLAMA_HOST}/api/generate",
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.4, "num_predict": 400}
            },
            timeout=120
        )
        if response.status_code == 200:
            narrative = response.json().get("response", "").strip()
            if narrative:
                return narrative
        return None
    except Exception as e:
        log.warning(f"Narrative generation failed for {ticker}: {e}")
        return None


def store_score(conn, ticker: str, score_result: dict, stock_data,
                scan_date: str, narrative: str = None):
    dims = score_result["dimensions"]
    info = score_result.get("_info", {})

    conn.execute("""
        INSERT OR REPLACE INTO scores (
            ticker, scan_date, total_score,
            value_score, future_score, past_score, health_score, dividend_score,
            raw_info, dimension_detail,
            company_name, sector, industry, market_cap, current_price,
            narrative, data_provider, data_completeness, data_fetched_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        ticker, scan_date,
        score_result["total_score"],
        dims["value"]["score"], dims["future"]["score"],
        dims["past"]["score"], dims["health"]["score"],
        dims["dividends"]["score"],
        json.dumps({k: v for k, v in info.items()
                    if isinstance(v, (str, int, float, bool, type(None)))}),
        json.dumps(dims),
        stock_data.company_name or ticker,
        stock_data.sector or "",
        stock_data.industry or "",
        stock_data.market_cap,
        stock_data.current_price,
        narrative,
        stock_data.provider,
        stock_data.completeness_score,
        stock_data.fetched_at,
    ))


def get_movers(conn, today: str, yesterday: str) -> list:
    rows = conn.execute("""
        SELECT t.ticker, t.company_name,
            t.total_score as today_score, y.total_score as yesterday_score,
            (t.total_score - y.total_score) as change,
            t.value_score, t.future_score, t.past_score,
            t.health_score, t.dividend_score,
            t.sector, t.market_cap, t.current_price
        FROM scores t
        JOIN scores y ON t.ticker = y.ticker
        WHERE t.scan_date = ? AND y.scan_date = ?
        AND ABS(t.total_score - y.total_score) >= 2
        ORDER BY ABS(t.total_score - y.total_score) DESC
    """, (today, yesterday)).fetchall()
    return [dict(r) for r in rows]


def store_ticker_metric(conn, run_id: str, scan_date: str, ticker: str,
                        provider: str, success: bool,
                        duration_s: float, score: Optional[int]):
    conn.execute("""
        INSERT INTO ticker_metrics
        (run_id, scan_date, ticker, provider, success, duration_seconds, score, scored_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (run_id, scan_date, ticker, provider, int(success),
          round(duration_s, 2), score, time.time()))


def run_scan(universe_name: Optional[str] = None):
    from scorer import score_stock

    display_name, tickers = get_universe(universe_name)
    run_logger = ScanRunLogger(universe=display_name, log_dir=DB_PATH.parent)
    run_id = run_logger.run_id

    orchestrator = DataOrchestrator(DB_PATH)
    scan_date = date.today().isoformat()
    started_at = datetime.now().isoformat()

    ollama_available = False
    try:
        r = requests.get(f"{OLLAMA_HOST}/api/tags", timeout=5)
        ollama_available = r.status_code == 200
        log.info(f"Ollama {'available' if ollama_available else 'not available'}")
    except Exception:
        log.warning("Ollama not reachable - skipping narratives")

    log.info(f"Starting scan run_id={run_id} universe='{display_name}' "
             f"date={scan_date} tickers={len(tickers)}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    scanned = 0
    failed = 0
    narratives_generated = 0

    for i, ticker in enumerate(tickers):
        log.info(f"[{i+1}/{len(tickers)}] {ticker}")
        run_logger.start_ticker(ticker)

        stock_data = orchestrator.fetch(ticker)

        if stock_data is None:
            failed += 1
            run_logger.end_ticker(ticker, provider="none", success=False)
            store_ticker_metric(conn, run_id, scan_date, ticker,
                                "none", False,
                                run_logger.ticker_outcomes[ticker]["duration_s"], None)
            conn.commit()
            log.warning(f"All providers failed for {ticker}")
            time.sleep(1.0)
            continue

        primary_provider = stock_data.provider.split("+")[0].split(" ")[0]

        try:
            info_dict = stock_data.to_scorer_dict()
            result = score_stock(info_dict, ticker)
            result["_info"] = info_dict

            narrative = None
            if ollama_available:
                narrative = generate_narrative(ticker, info_dict, result)
                if narrative:
                    narratives_generated += 1

            store_score(conn, ticker, result, stock_data, scan_date, narrative)
            run_logger.end_ticker(ticker, provider=primary_provider,
                                  success=True, score=result["total_score"])
            store_ticker_metric(conn, run_id, scan_date, ticker,
                                primary_provider, True,
                                run_logger.ticker_outcomes[ticker]["duration_s"],
                                result["total_score"])
            conn.commit()
            scanned += 1

        except Exception as e:
            log.error(f"Scoring failed for {ticker}: {e}")
            run_logger.end_ticker(ticker, provider=primary_provider, success=False)
            store_ticker_metric(conn, run_id, scan_date, ticker,
                                primary_provider, False,
                                run_logger.ticker_outcomes[ticker]["duration_s"], None)
            conn.commit()
            failed += 1

        time.sleep(5.0)

    run_logger.finish()
    summary = run_logger.summary()

    conn.execute("""
        INSERT INTO scan_log
        (scan_date, started_at, completed_at, stocks_scanned, stocks_failed,
         run_id, universe, duration_seconds, provider_summary)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (scan_date, started_at, datetime.now().isoformat(),
          scanned, failed,
          run_id, display_name,
          summary["duration_seconds"],
          json.dumps(summary["provider_stats"])))
    conn.commit()
    conn.close()

    log.info(f"Scan complete: {scanned} scored, {failed} failed, "
             f"{narratives_generated} narratives, run_id={run_id}")


if __name__ == "__main__":
    run_scan()
