"""
Nightly scanner - fetches ASX 200 constituents, pulls yfinance data,
runs scoring engine, generates Ollama narratives, stores in SQLite.
"""

import sqlite3
import json
import time
import logging
import os
from datetime import datetime, date
from pathlib import Path

import yfinance as yf
import pandas as pd
import requests

# --- Ollama config ---
# Set OLLAMA_HOST in docker-compose.yml environment to point at your Ollama container
# e.g. http://192.168.1.x:11434 or http://ollama:11434 if on the same Docker network
OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://localhost:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "llama3.1:8b")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

DB_PATH = Path("/data/stocks.db")


# --- ASX 200 constituents ---

ASX_200 = [
    "ANZ.AX", "BHP.AX", "CBA.AX", "CSL.AX", "NAB.AX", "WBC.AX", "WES.AX", "WOW.AX",
    "MQG.AX", "RIO.AX", "TLS.AX", "FMG.AX", "GMG.AX", "TCL.AX", "SCG.AX", "SUN.AX",
    "QBE.AX", "IAG.AX", "AMP.AX", "ASX.AX", "ALL.AX", "CWY.AX", "REA.AX", "WDS.AX",
    "STO.AX", "ORG.AX", "APA.AX", "AMC.AX", "BXB.AX", "COL.AX", "COH.AX", "ALX.AX",
    "MPL.AX", "NHF.AX", "RMD.AX", "ANN.AX", "JHX.AX", "SEK.AX", "CAR.AX", "CPU.AX",
    "XRO.AX", "WTC.AX", "APX.AX", "ALU.AX", "NEA.AX", "TNE.AX", "DXS.AX", "GPT.AX",
    "MGR.AX", "VCX.AX", "SGP.AX", "CLW.AX", "CIP.AX", "NST.AX", "NCM.AX", "EVN.AX",
    "OZL.AX", "IGO.AX", "MIN.AX", "LYC.AX", "PDN.AX", "PLS.AX", "AKE.AX", "S32.AX",
    "BSL.AX", "BLD.AX", "ABC.AX", "CSR.AX", "JHG.AX", "MFG.AX", "PPT.AX", "PTM.AX",
    "HUB.AX", "NWL.AX", "GQG.AX", "EQT.AX", "AFI.AX", "ARG.AX", "MLT.AX", "WHC.AX",
    "NEC.AX", "REH.AX", "SUL.AX", "UNI.AX", "WPR.AX", "LLC.AX", "CQR.AX", "HCW.AX",
    "RHC.AX", "SHL.AX", "ACF.AX", "IEL.AX", "KGN.AX", "MYX.AX", "MSB.AX", "APE.AX",
    "GUD.AX", "GWA.AX", "IPL.AX", "NUF.AX", "ORI.AX", "PNI.AX", "QAN.AX", "SYD.AX",
    "TAH.AX", "TWE.AX", "VVR.AX", "WOR.AX", "CCP.AX", "CLH.AX", "CVW.AX", "ELD.AX",
    "GNC.AX", "MTS.AX", "OML.AX", "SFR.AX", "SSM.AX", "SVW.AX", "SWM.AX", "TGR.AX",
    "VEA.AX", "VNT.AX", "AWC.AX", "DMP.AX", "DRR.AX", "HLO.AX", "HPI.AX", "IMD.AX",
    "ING.AX", "JIN.AX", "LNK.AX", "MVF.AX", "MWY.AX", "NWS.AX", "OFX.AX", "PGH.AX",
    "PMV.AX", "PPH.AX", "PRN.AX", "RDY.AX", "RWC.AX", "SCP.AX", "SKC.AX", "SPK.AX",
    "SRG.AX", "SSR.AX", "STX.AX", "SYR.AX", "TPW.AX", "TRS.AX", "UMG.AX", "URW.AX",
    "WGN.AX", "WSA.AX", "ALD.AX", "API.AX", "ARB.AX", "BEN.AX", "BOQ.AX", "CGF.AX",
    "CMW.AX", "CNU.AX", "DJW.AX", "DOW.AX", "EBO.AX", "ECX.AX", "FFI.AX", "FLT.AX",
    "GEM.AX", "HVN.AX", "IFL.AX", "ILU.AX", "JBH.AX", "KMD.AX", "LFS.AX", "LGL.AX",
    "MCY.AX", "MED.AX", "MRM.AX", "NBI.AX", "OGC.AX", "PAC.AX", "PBH.AX", "PPC.AX",
    "PSQ.AX", "PTB.AX", "RED.AX", "SBM.AX", "SDF.AX", "SIG.AX", "SKI.AX", "SLC.AX",
    "SOL.AX", "STW.AX", "THL.AX", "VOC.AX", "VRL.AX", "WEB.AX", "WGX.AX", "WPL.AX",
]


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            scan_date TEXT NOT NULL,
            total_score INTEGER,
            value_score INTEGER,
            future_score INTEGER,
            past_score INTEGER,
            health_score INTEGER,
            dividend_score INTEGER,
            raw_info TEXT,
            dimension_detail TEXT,
            company_name TEXT,
            sector TEXT,
            industry TEXT,
            market_cap REAL,
            current_price REAL,
            narrative TEXT,
            UNIQUE(ticker, scan_date)
        );

        CREATE TABLE IF NOT EXISTS scan_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_date TEXT,
            started_at TEXT,
            completed_at TEXT,
            stocks_scanned INTEGER,
            stocks_failed INTEGER
        );

        CREATE INDEX IF NOT EXISTS idx_scores_ticker ON scores(ticker);
        CREATE INDEX IF NOT EXISTS idx_scores_date ON scores(scan_date);
        CREATE INDEX IF NOT EXISTS idx_scores_total ON scores(total_score);
    """)

    # Add narrative column to existing DBs that pre-date this version
    try:
        conn.execute("ALTER TABLE scores ADD COLUMN narrative TEXT")
        conn.commit()
        log.info("Added narrative column to existing DB")
    except Exception:
        pass  # Column already exists

    conn.commit()
    conn.close()
    log.info("Database initialised")


def build_narrative_prompt(ticker: str, info: dict, score_result: dict) -> str:
    """Build a clean structured prompt for Ollama narrative generation."""
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

    # Collect failed checks across all dimensions
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

    prompt = f"""You are a concise financial analyst writing plain-English investment narratives for an ASX stock screener. Write for an intelligent investor who is new to stock picking. Be direct, specific, and avoid generic filler phrases.

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

    return prompt


def generate_narrative(ticker: str, info: dict, score_result: dict) -> str | None:
    """Call Ollama to generate a narrative. Returns None on failure."""
    prompt = build_narrative_prompt(ticker, info, score_result)
    try:
        response = requests.post(
            f"{OLLAMA_HOST}/api/generate",
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.4,
                    "num_predict": 400,
                }
            },
            timeout=120
        )
        if response.status_code == 200:
            data = response.json()
            narrative = data.get("response", "").strip()
            if narrative:
                log.info(f"Narrative generated for {ticker} ({len(narrative)} chars)")
                return narrative
        log.warning(f"Ollama returned status {response.status_code} for {ticker}")
        return None
    except requests.exceptions.ConnectionError:
        log.warning(f"Could not connect to Ollama at {OLLAMA_HOST} - skipping narratives")
        return None
    except Exception as e:
        log.warning(f"Narrative generation failed for {ticker}: {e}")
        return None


def fetch_stock_data(ticker: str) -> dict | None:
    """Fetch yfinance info for a ticker. Returns None on failure."""
    try:
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        })
        stock = yf.Ticker(ticker, session=session)
        info = stock.info
        if not info or info.get("regularMarketPrice") is None and info.get("currentPrice") is None:
            return None
        return info
    except Exception as e:
        log.warning(f"Failed to fetch {ticker}: {e}")
        return None


def store_score(conn, ticker: str, info: dict, score_result: dict, scan_date: str, narrative: str = None):
    dims = score_result["dimensions"]
    conn.execute("""
        INSERT OR REPLACE INTO scores (
            ticker, scan_date, total_score,
            value_score, future_score, past_score, health_score, dividend_score,
            raw_info, dimension_detail,
            company_name, sector, industry, market_cap, current_price,
            narrative
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        ticker,
        scan_date,
        score_result["total_score"],
        dims["value"]["score"],
        dims["future"]["score"],
        dims["past"]["score"],
        dims["health"]["score"],
        dims["dividends"]["score"],
        json.dumps({k: v for k, v in info.items() if isinstance(v, (str, int, float, bool, type(None)))}),
        json.dumps(dims),
        info.get("longName") or info.get("shortName", ticker),
        info.get("sector", ""),
        info.get("industry", ""),
        info.get("marketCap"),
        info.get("currentPrice") or info.get("regularMarketPrice"),
        narrative,
    ))


def get_movers(conn, today: str, yesterday: str) -> list:
    """Find stocks whose scores moved significantly since yesterday."""
    rows = conn.execute("""
        SELECT
            t.ticker,
            t.company_name,
            t.total_score as today_score,
            y.total_score as yesterday_score,
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


def run_scan():
    """Main scan function - runs all ASX 200 stocks."""
    from scorer import score_stock

    init_db()
    scan_date = date.today().isoformat()
    started_at = datetime.now().isoformat()

    # Check if Ollama is reachable before starting
    ollama_available = False
    try:
        r = requests.get(f"{OLLAMA_HOST}/api/tags", timeout=5)
        ollama_available = r.status_code == 200
        if ollama_available:
            log.info(f"Ollama available at {OLLAMA_HOST} - narratives will be generated")
        else:
            log.warning(f"Ollama not reachable at {OLLAMA_HOST} - skipping narratives")
    except Exception:
        log.warning(f"Ollama not reachable at {OLLAMA_HOST} - skipping narratives")

    log.info(f"Starting scan for {scan_date} - {len(ASX_200)} stocks")

    conn = get_db()
    scanned = 0
    failed = 0
    narratives_generated = 0

    for i, ticker in enumerate(ASX_200):
        log.info(f"[{i+1}/{len(ASX_200)}] Scanning {ticker}")
        info = fetch_stock_data(ticker)

        if info is None:
            failed += 1
            log.warning(f"Skipping {ticker} - no data")
            time.sleep(0.5)
            continue

        try:
            result = score_stock(info, ticker)

            # Generate narrative if Ollama is available
            narrative = None
            if ollama_available:
                narrative = generate_narrative(ticker, info, result)
                if narrative:
                    narratives_generated += 1

            store_score(conn, ticker, info, result, scan_date, narrative)
            conn.commit()
            scanned += 1
        except Exception as e:
            log.error(f"Scoring failed for {ticker}: {e}")
            failed += 1

        # Respectful rate limiting - gives Ollama time to breathe too
        time.sleep(3.0)

    conn.execute("""
        INSERT INTO scan_log (scan_date, started_at, completed_at, stocks_scanned, stocks_failed)
        VALUES (?, ?, ?, ?, ?)
    """, (scan_date, started_at, datetime.now().isoformat(), scanned, failed))
    conn.commit()
    conn.close()

    log.info(f"Scan complete: {scanned} scored, {failed} failed, {narratives_generated} narratives generated")


if __name__ == "__main__":
    run_scan()
