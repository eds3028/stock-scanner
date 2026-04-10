"""
Data Orchestrator - the core of the resilient data pipeline.

Manages:
- Provider priority and fallback
- TTL-based caching with provenance tracking
- Data merging (fill gaps from multiple providers)
- Health monitoring
- Automatic recovery
"""

import sqlite3
import json
import logging
import time
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from providers.base import StockData, ProviderStatus
from providers.yahooquery_provider import YahooQueryProvider
from providers.finnhub_provider import FinnhubProvider
from providers.fmp_provider import FMPProvider
from providers.alpha_vantage_provider import AlphaVantageProvider

log = logging.getLogger(__name__)

# Cache TTLs in seconds
PRICE_TTL = 6 * 3600        # 6 hours
FUNDAMENTALS_TTL = 24 * 3600  # 24 hours
STALE_WARNING_TTL = 48 * 3600  # 48 hours - show warning


class DataOrchestrator:
    """
    Manages multiple data providers with automatic fallback,
    caching, and data merging.
    """

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._init_providers()
        self._init_cache_db()

    def _init_providers(self):
        """Initialise all providers in priority order."""
        self.providers = [
            YahooQueryProvider(),
            FinnhubProvider(),
            FMPProvider(),
            AlphaVantageProvider(),
        ]
        configured = [p.name for p in self.providers if p.is_configured()]
        unavailable = [p.name for p in self.providers if not p.is_configured()]
        log.info(f"Providers configured: {configured}")
        if unavailable:
            log.info(f"Providers not configured (missing API keys): {unavailable}")

    def _init_cache_db(self):
        """Initialise the cache database."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = self._get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS data_cache (
                ticker TEXT NOT NULL,
                field_group TEXT NOT NULL,
                provider TEXT NOT NULL,
                data_json TEXT NOT NULL,
                fetched_at REAL NOT NULL,
                completeness REAL NOT NULL DEFAULT 0,
                PRIMARY KEY (ticker, field_group)
            );

            CREATE TABLE IF NOT EXISTS fetch_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                provider TEXT NOT NULL,
                success INTEGER NOT NULL,
                completeness REAL,
                reason TEXT,
                fetched_at REAL NOT NULL
            );

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
                data_provider TEXT,
                data_completeness REAL,
                data_fetched_at REAL,
                UNIQUE(ticker, scan_date)
            );

            CREATE TABLE IF NOT EXISTS scan_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scan_date TEXT,
                started_at TEXT,
                completed_at TEXT,
                stocks_scanned INTEGER,
                stocks_failed INTEGER,
                run_id TEXT,
                universe TEXT,
                duration_seconds REAL,
                provider_summary TEXT
            );

            CREATE TABLE IF NOT EXISTS ticker_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                scan_date TEXT NOT NULL,
                ticker TEXT NOT NULL,
                provider TEXT,
                success INTEGER NOT NULL,
                duration_seconds REAL,
                score INTEGER,
                scored_at REAL NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_cache_ticker ON data_cache(ticker);
            CREATE INDEX IF NOT EXISTS idx_scores_ticker ON scores(ticker);
            CREATE INDEX IF NOT EXISTS idx_scores_date ON scores(scan_date);
            CREATE INDEX IF NOT EXISTS idx_scores_total ON scores(total_score);
            CREATE INDEX IF NOT EXISTS idx_ticker_metrics_run ON ticker_metrics(run_id);
            CREATE INDEX IF NOT EXISTS idx_ticker_metrics_date ON ticker_metrics(scan_date);
        """)

        # Migrate: add columns to scores if they don't exist
        for col, coltype in [
            ("data_provider", "TEXT"),
            ("data_completeness", "REAL"),
            ("data_fetched_at", "REAL"),
            ("narrative", "TEXT"),
        ]:
            try:
                conn.execute(f"ALTER TABLE scores ADD COLUMN {col} {coltype}")
            except Exception:
                pass

        # Migrate: add new scan_log columns if they don't exist
        for col, coltype in [
            ("run_id", "TEXT"),
            ("universe", "TEXT"),
            ("duration_seconds", "REAL"),
        ]:
            try:
                conn.execute(f"ALTER TABLE scan_log ADD COLUMN {col} {coltype}")
            except Exception:
                pass

        conn.commit()
        conn.close()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def get_cached(self, ticker: str) -> Optional[StockData]:
        """Return cached data if still fresh enough."""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT * FROM data_cache WHERE ticker = ? ORDER BY fetched_at DESC LIMIT 1",
            (ticker,)
        ).fetchone()
        conn.close()

        if not row:
            return None

        age = time.time() - row["fetched_at"]
        if age > FUNDAMENTALS_TTL:
            return None

        try:
            data = json.loads(row["data_json"])
            stock = StockData(ticker=ticker, provider=f"{row['provider']} (cached)")
            for k, v in data.items():
                if hasattr(stock, k):
                    setattr(stock, k, v)
            stock.fetched_at = row["fetched_at"]
            return stock
        except Exception as e:
            log.warning(f"Cache deserialisation failed for {ticker}: {e}")
            return None

    def save_cache(self, data: StockData):
        """Store fetched data in cache."""
        conn = self._get_conn()
        data_dict = {
            k: v for k, v in data.__dict__.items()
            if k not in ("ticker", "provider", "fetched_at")
            and isinstance(v, (str, int, float, bool, type(None)))
        }
        conn.execute("""
            INSERT OR REPLACE INTO data_cache
            (ticker, field_group, provider, data_json, fetched_at, completeness)
            VALUES (?, 'full', ?, ?, ?, ?)
        """, (
            data.ticker, data.provider,
            json.dumps(data_dict),
            data.fetched_at,
            data.completeness_score
        ))
        conn.commit()
        conn.close()

    def log_fetch(self, ticker: str, provider: str, success: bool,
                  completeness: float = 0, reason: str = ""):
        """Log every fetch attempt for observability."""
        conn = self._get_conn()
        conn.execute("""
            INSERT INTO fetch_log (ticker, provider, success, completeness, reason, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (ticker, provider, int(success), completeness, reason, time.time()))
        conn.commit()
        conn.close()

    def merge_data(self, primary: StockData, secondary: StockData) -> StockData:
        """
        Fill gaps in primary data from secondary provider.
        Primary data takes precedence, secondary fills None fields.
        """
        fields = [f for f in primary.__dict__ if f not in ("ticker", "provider", "fetched_at")]
        for field in fields:
            if getattr(primary, field) is None and getattr(secondary, field) is not None:
                setattr(primary, field, getattr(secondary, field))
        primary.provider = f"{primary.provider}+{secondary.provider}"
        return primary

    def fetch(self, ticker: str, force: bool = False) -> Optional[StockData]:
        """
        Fetch stock data with full fallback chain.
        Returns best available data or None if all providers fail.
        """
        # Check cache first
        if not force:
            cached = self.get_cached(ticker)
            if cached:
                log.debug(f"[{ticker}] Cache hit from {cached.provider}")
                return cached

        result = None
        providers_tried = []

        for provider in self.providers:
            if not provider.health.is_available:
                log.debug(f"[{ticker}] Skipping {provider.name} - circuit open or unavailable")
                continue
            # Skip providers known to not cover ASX on free tier
            if ".AX" in ticker and provider.name in ("fmp", "alpha_vantage"):
                log.debug(f"[{ticker}] Skipping {provider.name} - no ASX free tier coverage")
                continue        

            log.info(f"[{ticker}] Trying {provider.name}...")
            providers_tried.append(provider.name)

            try:
                data = provider.safe_fetch(ticker)
                if data and data.current_price:
                    self.log_fetch(ticker, provider.name, True, data.completeness_score)
                    log.info(f"[{ticker}] Got data from {provider.name} "
                             f"(completeness: {data.completeness_score:.0%})")

                    if result is None:
                        result = data
                    else:
                        # Merge to fill gaps
                        result = self.merge_data(result, data)

                    # If we have good enough data, stop trying
                    if result.completeness_score >= 0.65:
                        break
                else:
                    self.log_fetch(ticker, provider.name, False, 0, "No data returned")
            except Exception as e:
                self.log_fetch(ticker, provider.name, False, 0, str(e))
                log.warning(f"[{ticker}] {provider.name} error: {e}")

            # Small delay between providers to be respectful
            time.sleep(2.0)

        if result:
            self.save_cache(result)

        return result

    def get_provider_health(self) -> list[dict]:
        """Return health status of all providers."""
        return [{
            "name": p.name,
            "status": p.health.status.value,
            "configured": p.is_configured(),
            "total_requests": p.health.total_requests,
            "success_rate": round(p.health.success_rate * 100, 1),
            "consecutive_failures": p.health.consecutive_failures,
            "last_success": p.health.last_success_at,
            "last_failure": p.health.last_failure_at,
            "last_failure_reason": p.health.last_failure_reason,
        } for p in self.providers]

    def get_cache_stats(self, ticker: str = None) -> dict:
        """Return cache statistics."""
        conn = self._get_conn()
        if ticker:
            row = conn.execute(
                "SELECT provider, fetched_at, completeness FROM data_cache WHERE ticker = ?",
                (ticker,)
            ).fetchone()
            conn.close()
            if not row:
                return {"cached": False}
            age_hours = (time.time() - row["fetched_at"]) / 3600
            return {
                "cached": True,
                "provider": row["provider"],
                "age_hours": round(age_hours, 1),
                "completeness": round(row["completeness"] * 100, 1),
                "stale": age_hours > FUNDAMENTALS_TTL / 3600,
            }
        else:
            total = conn.execute("SELECT COUNT(*) as c FROM data_cache").fetchone()["c"]
            stale = conn.execute(
                "SELECT COUNT(*) as c FROM data_cache WHERE fetched_at < ?",
                (time.time() - FUNDAMENTALS_TTL,)
            ).fetchone()["c"]
            conn.close()
            return {
                "total_cached": total,
                "stale_count": stale,
                "fresh_count": total - stale,
            }

    def get_fetch_log(self, limit: int = 50) -> list[dict]:
        """Return recent fetch log entries."""
        conn = self._get_conn()
        rows = conn.execute("""
            SELECT ticker, provider, success, completeness, reason, fetched_at
            FROM fetch_log
            ORDER BY fetched_at DESC
            LIMIT ?
        """, (limit,)).fetchall()
        conn.close()
        return [dict(r) for r in rows]
