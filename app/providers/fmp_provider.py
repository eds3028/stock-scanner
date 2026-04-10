"""
Financial Modeling Prep provider adapter.
Free tier: 250 calls/day.
Good US fundamentals, limited ASX on free tier.
Requires FMP_API_KEY environment variable.
"""

import os
import logging
import time
from typing import Optional

import requests

from providers.base import StockDataProvider, StockData

log = logging.getLogger(__name__)

FMP_BASE = "https://financialmodelingprep.com/api/v3"


class FMPProvider(StockDataProvider):

    def __init__(self):
        super().__init__("fmp")
        self.api_key = os.environ.get("FMP_API_KEY", "")
        self._daily_calls = 0
        self._daily_limit = 240  # Leave buffer below 250

    def is_configured(self) -> bool:
        return bool(self.api_key)

    def _get(self, endpoint: str, params: dict = {}) -> Optional[list | dict]:
        if self._daily_calls >= self._daily_limit:
            log.warning("[fmp] Daily call limit reached")
            raise Exception("Daily limit reached")
        try:
            params["apikey"] = self.api_key
            r = requests.get(
                f"{FMP_BASE}/{endpoint}",
                params=params,
                timeout=10
            )
            self._daily_calls += 1
            if r.status_code == 429:
                log.warning("[fmp] Rate limited")
                raise Exception("Rate limited")
            if r.status_code != 200:
                raise Exception(f"HTTP {r.status_code}")
            return r.json()
        except requests.exceptions.RequestException as e:
            raise Exception(f"Request failed: {e}")

    def fetch(self, ticker: str) -> Optional[StockData]:
        # Profile (1 call)
        profile_data = self._get(f"profile/{ticker}")
        if not profile_data or not isinstance(profile_data, list) or not profile_data:
            return None
        profile = profile_data[0]

        price = profile.get("price")
        if not price:
            return None

        # Key metrics TTM (1 call)
        metrics_data = self._get(f"key-metrics-ttm/{ticker}")
        metrics = metrics_data[0] if metrics_data and isinstance(metrics_data, list) and metrics_data else {}

        # Ratios TTM (1 call)
        ratios_data = self._get(f"ratios-ttm/{ticker}")
        ratios = ratios_data[0] if ratios_data and isinstance(ratios_data, list) and ratios_data else {}

        # Parse 52w range
        low_52w = high_52w = None
        range_str = profile.get("range", "")
        if range_str and "-" in range_str:
            parts = range_str.split("-")
            try:
                low_52w = float(parts[0].strip())
                high_52w = float(parts[-1].strip())
            except Exception:
                pass

        de = ratios.get("debtEquityRatioTTM") or metrics.get("debtToEquityTTM")

        return StockData(
            ticker=ticker,
            provider=self.name,
            company_name=profile.get("companyName", ticker),
            sector=profile.get("sector", ""),
            industry=profile.get("industry", ""),
            current_price=price,
            market_cap=profile.get("mktCap"),
            fifty_two_week_low=low_52w,
            fifty_two_week_high=high_52w,
            beta=profile.get("beta"),
            trailing_pe=metrics.get("peRatioTTM"),
            forward_pe=metrics.get("peRatioTTM"),
            price_to_book=metrics.get("pbRatioTTM"),
            ev_to_ebitda=metrics.get("evToEbitdaTTM") or metrics.get("enterpriseValueOverEBITDATTM"),
            trailing_eps=metrics.get("epsTTM") or metrics.get("netIncomePerShareTTM"),
            forward_eps=metrics.get("epsTTM"),
            dividend_yield=ratios.get("dividendYieldTTM") or metrics.get("dividendYieldTTM"),
            dividend_rate=profile.get("lastDiv"),
            payout_ratio=ratios.get("payoutRatioTTM") or metrics.get("payoutRatioTTM"),
            five_year_avg_dividend_yield=metrics.get("dividendYieldTTM"),
            profit_margins=ratios.get("netProfitMarginTTM"),
            gross_margins=ratios.get("grossProfitMarginTTM"),
            operating_margins=ratios.get("operatingProfitMarginTTM"),
            return_on_equity=ratios.get("returnOnEquityTTM") or metrics.get("roeTTM"),
            return_on_assets=ratios.get("returnOnAssetsTTM") or metrics.get("returnOnAssetsTTM"),
            revenue_growth=metrics.get("revenueGrowthTTM"),
            earnings_growth=metrics.get("netIncomeGrowthTTM"),
            current_ratio=ratios.get("currentRatioTTM") or metrics.get("currentRatioTTM"),
            quick_ratio=ratios.get("quickRatioTTM") or metrics.get("quickRatioTTM"),
            debt_to_equity=de * 100 if de else None,
            total_cash=metrics.get("cashAndCashEquivalentsTTM"),
            total_debt=metrics.get("netDebtTTM"),
            free_cashflow=metrics.get("freeCashFlowTTM"),
            operating_cashflow=metrics.get("operatingCashFlowTTM"),
        )
