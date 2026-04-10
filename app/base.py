"""
Base provider interface and circuit breaker implementation.
All data providers implement StockDataProvider.
Circuit breakers prevent hammering failing providers.
"""

import time
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

log = logging.getLogger(__name__)


class ProviderStatus(Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"      # Some failures but still trying
    CIRCUIT_OPEN = "circuit_open"  # Too many failures - cooling off
    UNAVAILABLE = "unavailable"    # No API key or not configured


@dataclass
class ProviderHealth:
    name: str
    status: ProviderStatus = ProviderStatus.HEALTHY
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    consecutive_failures: int = 0
    circuit_opened_at: Optional[float] = None
    last_success_at: Optional[float] = None
    last_failure_at: Optional[float] = None
    last_failure_reason: Optional[str] = None
    cooldown_seconds: int = 300  # 5 minutes before retrying after circuit open

    @property
    def success_rate(self) -> float:
        if self.total_requests == 0:
            return 1.0
        return self.successful_requests / self.total_requests

    @property
    def is_available(self) -> bool:
        if self.status == ProviderStatus.UNAVAILABLE:
            return False
        if self.status == ProviderStatus.CIRCUIT_OPEN:
            # Check if cooldown has elapsed
            if self.circuit_opened_at and time.time() - self.circuit_opened_at > self.cooldown_seconds:
                log.info(f"Circuit breaker for {self.name} - attempting reset after cooldown")
                self.status = ProviderStatus.DEGRADED
                self.consecutive_failures = 0
                return True
            return False
        return True

    def record_success(self):
        self.total_requests += 1
        self.successful_requests += 1
        self.consecutive_failures = 0
        self.last_success_at = time.time()
        if self.status == ProviderStatus.DEGRADED:
            self.status = ProviderStatus.HEALTHY
            log.info(f"Provider {self.name} recovered to HEALTHY")

    def record_failure(self, reason: str = ""):
        self.total_requests += 1
        self.failed_requests += 1
        self.consecutive_failures += 1
        self.last_failure_at = time.time()
        self.last_failure_reason = reason

        if self.consecutive_failures >= 5:
            if self.status != ProviderStatus.CIRCUIT_OPEN:
                log.warning(f"Circuit breaker OPEN for {self.name} after {self.consecutive_failures} failures: {reason}")
                self.status = ProviderStatus.CIRCUIT_OPEN
                self.circuit_opened_at = time.time()
        elif self.consecutive_failures >= 2:
            self.status = ProviderStatus.DEGRADED


@dataclass
class StockData:
    """Normalised stock data structure returned by all providers."""
    ticker: str
    provider: str
    fetched_at: float = field(default_factory=time.time)

    # Identity
    company_name: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None

    # Price
    current_price: Optional[float] = None
    market_cap: Optional[float] = None
    fifty_two_week_low: Optional[float] = None
    fifty_two_week_high: Optional[float] = None
    beta: Optional[float] = None

    # Valuation
    trailing_pe: Optional[float] = None
    forward_pe: Optional[float] = None
    price_to_book: Optional[float] = None
    ev_to_ebitda: Optional[float] = None
    trailing_eps: Optional[float] = None
    forward_eps: Optional[float] = None

    # Dividends
    dividend_yield: Optional[float] = None
    dividend_rate: Optional[float] = None
    payout_ratio: Optional[float] = None
    five_year_avg_dividend_yield: Optional[float] = None

    # Profitability
    profit_margins: Optional[float] = None
    gross_margins: Optional[float] = None
    operating_margins: Optional[float] = None
    return_on_equity: Optional[float] = None
    return_on_assets: Optional[float] = None

    # Growth
    revenue_growth: Optional[float] = None
    earnings_growth: Optional[float] = None
    total_revenue: Optional[float] = None

    # Health
    current_ratio: Optional[float] = None
    quick_ratio: Optional[float] = None
    debt_to_equity: Optional[float] = None
    total_cash: Optional[float] = None
    total_debt: Optional[float] = None
    free_cashflow: Optional[float] = None
    operating_cashflow: Optional[float] = None
    ebitda: Optional[float] = None
    interest_expense: Optional[float] = None

    # Analyst
    target_mean_price: Optional[float] = None
    target_low_price: Optional[float] = None
    target_high_price: Optional[float] = None
    number_of_analyst_opinions: Optional[int] = None
    shares_outstanding: Optional[float] = None

    @property
    def completeness_score(self) -> float:
        """Returns 0-1 indicating how complete this data is."""
        critical_fields = [
            self.current_price, self.market_cap, self.trailing_pe,
            self.return_on_equity, self.debt_to_equity, self.free_cashflow,
            self.profit_margins, self.revenue_growth, self.earnings_growth,
            self.current_ratio, self.dividend_yield, self.payout_ratio
        ]
        filled = sum(1 for f in critical_fields if f is not None)
        return filled / len(critical_fields)

    def to_scorer_dict(self) -> dict:
        """Convert to the dict format expected by scorer.py"""
        return {
            "currentPrice": self.current_price,
            "regularMarketPrice": self.current_price,
            "marketCap": self.market_cap,
            "longName": self.company_name,
            "shortName": self.company_name,
            "sector": self.sector or "",
            "industry": self.industry or "",
            "trailingPE": self.trailing_pe,
            "forwardPE": self.forward_pe,
            "priceToBook": self.price_to_book,
            "enterpriseToEbitda": self.ev_to_ebitda,
            "trailingEps": self.trailing_eps,
            "forwardEps": self.forward_eps,
            "sharesOutstanding": self.shares_outstanding,
            "dividendYield": self.dividend_yield,
            "dividendRate": self.dividend_rate,
            "payoutRatio": self.payout_ratio,
            "fiveYearAvgDividendYield": self.five_year_avg_dividend_yield,
            "beta": self.beta,
            "fiftyTwoWeekLow": self.fifty_two_week_low,
            "fiftyTwoWeekHigh": self.fifty_two_week_high,
            "profitMargins": self.profit_margins,
            "grossMargins": self.gross_margins,
            "operatingMargins": self.operating_margins,
            "returnOnEquity": self.return_on_equity,
            "returnOnAssets": self.return_on_assets,
            "revenueGrowth": self.revenue_growth,
            "earningsGrowth": self.earnings_growth,
            "totalRevenue": self.total_revenue,
            "currentRatio": self.current_ratio,
            "quickRatio": self.quick_ratio,
            "debtToEquity": self.debt_to_equity,
            "totalCash": self.total_cash,
            "totalDebt": self.total_debt,
            "freeCashflow": self.free_cashflow,
            "operatingCashflow": self.operating_cashflow,
            "ebitda": self.ebitda,
            "interestExpense": self.interest_expense,
            "targetMeanPrice": self.target_mean_price,
            "targetLowPrice": self.target_low_price,
            "targetHighPrice": self.target_high_price,
            "numberOfAnalystOpinions": self.number_of_analyst_opinions,
        }


class StockDataProvider(ABC):
    """Abstract base class all providers must implement."""

    def __init__(self, name: str):
        self.name = name
        self.health = ProviderHealth(name=name)

    @abstractmethod
    def is_configured(self) -> bool:
        """Returns True if provider has required API keys."""
        pass

    @abstractmethod
    def fetch(self, ticker: str) -> Optional[StockData]:
        """
        Fetch stock data for a ticker.
        Returns StockData or None if unavailable.
        Should NOT raise exceptions - handle internally.
        """
        pass

    def safe_fetch(self, ticker: str) -> Optional[StockData]:
        """Wraps fetch() with circuit breaker logic."""
        if not self.is_configured():
            self.health.status = ProviderStatus.UNAVAILABLE
            return None

        if not self.health.is_available:
            return None

        try:
            result = self.fetch(ticker)
            if result is not None:
                self.health.record_success()
            else:
                self.health.record_failure("No data returned")
            return result
        except Exception as e:
            self.health.record_failure(str(e))
            log.warning(f"[{self.name}] Unexpected error for {ticker}: {e}")
            return None
