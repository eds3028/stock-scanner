"""
Finnhub provider adapter.
Free tier: 60 calls/minute.
Good fundamentals, partial ASX coverage.
Requires FINNHUB_API_KEY environment variable.
"""

import os
import logging
import time
from typing import Optional

import requests

from providers.base import StockDataProvider, StockData

log = logging.getLogger(__name__)

FINNHUB_BASE = "https://finnhub.io/api/v1"


class FinnhubProvider(StockDataProvider):

    def __init__(self):
        super().__init__("finnhub")
        self.api_key = os.environ.get("FINNHUB_API_KEY", "")

    def is_configured(self) -> bool:
        return bool(self.api_key)

    def _get(self, endpoint: str, params: dict = {}) -> Optional[dict]:
        try:
            params["token"] = self.api_key
            r = requests.get(
                f"{FINNHUB_BASE}/{endpoint}",
                params=params,
                timeout=10
            )
            if r.status_code == 429:
                log.warning("[finnhub] Rate limited")
                time.sleep(60)
                raise Exception("Rate limited")
            if r.status_code != 200:
                raise Exception(f"HTTP {r.status_code}")
            data = r.json()
            if not data:
                return None
            return data
        except requests.exceptions.RequestException as e:
            raise Exception(f"Request failed: {e}")

    def fetch(self, ticker: str) -> Optional[StockData]:
        # Finnhub uses different ticker format for ASX
        fh_ticker = ticker.replace(".AX", "") if ".AX" in ticker else ticker

        # Basic metrics
        metrics = self._get("stock/metric", {"symbol": fh_ticker, "metric": "all"})
        if not metrics or "metric" not in metrics:
            return None

        m = metrics.get("metric", {})

        # Profile
        profile = self._get("stock/profile2", {"symbol": fh_ticker}) or {}

        # Quote
        quote = self._get("quote", {"symbol": fh_ticker}) or {}

        price = quote.get("c")  # current price
        if not price:
            return None

        # Financials basic
        financials = self._get("stock/financials-reported", {
            "symbol": fh_ticker,
            "freq": "annual"
        }) or {}

        de = m.get("totalDebt/totalEquityAnnual")

        return StockData(
            ticker=ticker,
            provider=self.name,
            company_name=profile.get("name", ticker),
            sector=profile.get("finnhubIndustry", ""),
            industry=profile.get("finnhubIndustry", ""),
            current_price=price,
            market_cap=profile.get("marketCapitalization", 0) * 1e6 if profile.get("marketCapitalization") else None,
            fifty_two_week_low=m.get("52WeekLow"),
            fifty_two_week_high=m.get("52WeekHigh"),
            beta=m.get("beta"),
            trailing_pe=m.get("peBasicExclExtraTTM") or m.get("peNormalizedAnnual"),
            forward_pe=m.get("peExclExtraAnnual"),
            price_to_book=m.get("pbAnnual"),
            ev_to_ebitda=m.get("evEbitdaTTM") or m.get("evEbitdaAnnual"),
            trailing_eps=m.get("epsBasicExclExtraTTM"),
            forward_eps=m.get("epsNormalizedAnnual"),
            dividend_yield=m.get("dividendYieldIndicatedAnnual"),
            dividend_rate=m.get("dividendPerShareAnnual"),
            payout_ratio=m.get("payoutRatioAnnual"),
            five_year_avg_dividend_yield=m.get("dividendYield5Y"),
            profit_margins=m.get("netProfitMarginTTM") or m.get("netProfitMarginAnnual"),
            gross_margins=m.get("grossMarginTTM") or m.get("grossMarginAnnual"),
            operating_margins=m.get("operatingMarginTTM") or m.get("operatingMarginAnnual"),
            return_on_equity=m.get("roeTTM") or m.get("roeRfy"),
            return_on_assets=m.get("roaTTM") or m.get("roaRfy"),
            revenue_growth=m.get("revenueGrowthTTMYoy"),
            earnings_growth=m.get("epsGrowthTTMYoy"),
            total_revenue=m.get("revenueTTM") or m.get("revenueAnnual"),
            current_ratio=m.get("currentRatioAnnual") or m.get("currentRatioQuarterly"),
            quick_ratio=m.get("quickRatioAnnual") or m.get("quickRatioQuarterly"),
            debt_to_equity=de * 100 if de else None,
            total_cash=m.get("cashAndEquivalentsAnnual"),
            total_debt=m.get("totalDebtAnnual") or m.get("totalDebtQuarterly"),
            free_cashflow=m.get("freeCashFlowTTM") or m.get("freeCashFlowAnnual"),
            operating_cashflow=m.get("cashFlowPerShareTTM"),
            ebitda=m.get("ebitdAnnual") or m.get("ebitdTTM"),
        )
