"""
Alpha Vantage provider adapter.
Free tier: 25 calls/day.
Good US fundamentals, some ASX coverage.
Used as last-resort fallback.
Requires ALPHA_VANTAGE_API_KEY environment variable.
"""

import os
import logging
from typing import Optional

import requests

from providers.base import StockDataProvider, StockData

log = logging.getLogger(__name__)

AV_BASE = "https://www.alphavantage.co/query"


class AlphaVantageProvider(StockDataProvider):

    def __init__(self):
        super().__init__("alpha_vantage")
        self.api_key = os.environ.get("ALPHA_VANTAGE_API_KEY", "")
        self._daily_calls = 0
        self._daily_limit = 20  # Very conservative - only 25/day

    def is_configured(self) -> bool:
        return bool(self.api_key)

    def _get(self, function: str, symbol: str, extra: dict = {}) -> Optional[dict]:
        if self._daily_calls >= self._daily_limit:
            log.warning("[alpha_vantage] Daily call limit reached")
            raise Exception("Daily limit reached")
        try:
            params = {
                "function": function,
                "symbol": symbol,
                "apikey": self.api_key,
                **extra
            }
            r = requests.get(AV_BASE, params=params, timeout=15)
            self._daily_calls += 1
            if r.status_code != 200:
                raise Exception(f"HTTP {r.status_code}")
            data = r.json()
            if "Note" in data or "Information" in data:
                raise Exception("Rate limited or invalid key")
            return data
        except requests.exceptions.RequestException as e:
            raise Exception(f"Request failed: {e}")

    def fetch(self, ticker: str) -> Optional[StockData]:
        # Alpha Vantage ASX tickers use .AX suffix already
        # Overview gets most fundamentals in 1 call
        overview = self._get("OVERVIEW", ticker)
        if not overview or not overview.get("Symbol"):
            return None

        def safe_float(val):
            try:
                v = float(val)
                return v if v != 0 else None
            except (TypeError, ValueError):
                return None

        # Quote endpoint for current price
        quote_data = self._get("GLOBAL_QUOTE", ticker)
        quote = quote_data.get("Global Quote", {}) if quote_data else {}
        price = safe_float(quote.get("05. price"))

        if not price:
            return None

        de = safe_float(overview.get("DebtToEquityRatio"))

        return StockData(
            ticker=ticker,
            provider=self.name,
            company_name=overview.get("Name", ticker),
            sector=overview.get("Sector", ""),
            industry=overview.get("Industry", ""),
            current_price=price,
            market_cap=safe_float(overview.get("MarketCapitalization")),
            fifty_two_week_low=safe_float(overview.get("52WeekLow")),
            fifty_two_week_high=safe_float(overview.get("52WeekHigh")),
            beta=safe_float(overview.get("Beta")),
            trailing_pe=safe_float(overview.get("TrailingPE")),
            forward_pe=safe_float(overview.get("ForwardPE")),
            price_to_book=safe_float(overview.get("PriceToBookRatio")),
            ev_to_ebitda=safe_float(overview.get("EVToEBITDA")),
            trailing_eps=safe_float(overview.get("EPS")),
            dividend_yield=safe_float(overview.get("DividendYield")),
            dividend_rate=safe_float(overview.get("DividendPerShare")),
            payout_ratio=safe_float(overview.get("PayoutRatio")),
            profit_margins=safe_float(overview.get("ProfitMargin")),
            gross_margins=safe_float(overview.get("GrossProfitTTM")),
            operating_margins=safe_float(overview.get("OperatingMarginTTM")),
            return_on_equity=safe_float(overview.get("ReturnOnEquityTTM")),
            return_on_assets=safe_float(overview.get("ReturnOnAssetsTTM")),
            revenue_growth=safe_float(overview.get("QuarterlyRevenueGrowthYOY")),
            earnings_growth=safe_float(overview.get("QuarterlyEarningsGrowthYOY")),
            total_revenue=safe_float(overview.get("RevenueTTM")),
            debt_to_equity=de * 100 if de else None,
            shares_outstanding=safe_float(overview.get("SharesOutstanding")),
            target_mean_price=safe_float(overview.get("AnalystTargetPrice")),
            number_of_analyst_opinions=None,
        )
