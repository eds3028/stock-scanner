"""
YahooQuery provider adapter.
Free, no API key, good ASX coverage.
Vulnerable to IP-based rate limiting.
"""

import logging
import time
from typing import Optional

from providers.base import StockDataProvider, StockData

log = logging.getLogger(__name__)


class YahooQueryProvider(StockDataProvider):

    def __init__(self):
        super().__init__("yahooquery")
        self._import_ok = False
        try:
            from yahooquery import Ticker
            self._Ticker = Ticker
            self._import_ok = True
        except ImportError:
            log.warning("yahooquery not installed")

    def is_configured(self) -> bool:
        return self._import_ok

    def fetch(self, ticker: str) -> Optional[StockData]:
        try:
            stock = self._Ticker(ticker, timeout=15)

            summary = stock.summary_detail.get(ticker, {})
            key_stats = stock.key_stats.get(ticker, {})
            financial_data = stock.financial_data.get(ticker, {})
            asset_profile = stock.asset_profile.get(ticker, {})
            price_data = stock.price.get(ticker, {})

            for module in [summary, key_stats, financial_data, price_data]:
                if isinstance(module, str):
                    log.warning(f"[yahooquery] Error module for {ticker}: {module}")
                    return None

            price = price_data.get("regularMarketPrice")
            if not price:
                return None

            return StockData(
                ticker=ticker,
                provider=self.name,
                company_name=price_data.get("longName") or price_data.get("shortName", ticker),
                sector=asset_profile.get("sector", "") if isinstance(asset_profile, dict) else "",
                industry=asset_profile.get("industry", "") if isinstance(asset_profile, dict) else "",
                current_price=price,
                market_cap=price_data.get("marketCap"),
                fifty_two_week_low=summary.get("fiftyTwoWeekLow"),
                fifty_two_week_high=summary.get("fiftyTwoWeekHigh"),
                beta=summary.get("beta"),
                trailing_pe=summary.get("trailingPE"),
                forward_pe=summary.get("forwardPE"),
                price_to_book=summary.get("priceToBook"),
                ev_to_ebitda=key_stats.get("enterpriseToEbitda"),
                trailing_eps=key_stats.get("trailingEps"),
                forward_eps=key_stats.get("forwardEps"),
                shares_outstanding=key_stats.get("sharesOutstanding"),
                dividend_yield=summary.get("dividendYield"),
                dividend_rate=summary.get("dividendRate"),
                payout_ratio=summary.get("payoutRatio"),
                five_year_avg_dividend_yield=summary.get("fiveYearAvgDividendYield"),
                profit_margins=financial_data.get("profitMargins"),
                gross_margins=financial_data.get("grossMargins"),
                operating_margins=financial_data.get("operatingMargins"),
                return_on_equity=financial_data.get("returnOnEquity"),
                return_on_assets=financial_data.get("returnOnAssets"),
                revenue_growth=financial_data.get("revenueGrowth"),
                earnings_growth=financial_data.get("earningsGrowth"),
                total_revenue=financial_data.get("totalRevenue"),
                current_ratio=financial_data.get("currentRatio"),
                quick_ratio=financial_data.get("quickRatio"),
                debt_to_equity=financial_data.get("debtToEquity"),
                total_cash=financial_data.get("totalCash"),
                total_debt=financial_data.get("totalDebt"),
                free_cashflow=financial_data.get("freeCashflow"),
                operating_cashflow=financial_data.get("operatingCashflow"),
                ebitda=financial_data.get("ebitda"),
                interest_expense=financial_data.get("interestExpense"),
                target_mean_price=financial_data.get("targetMeanPrice"),
                target_low_price=financial_data.get("targetLowPrice"),
                target_high_price=financial_data.get("targetHighPrice"),
                number_of_analyst_opinions=financial_data.get("numberOfAnalystOpinions"),
            )

        except Exception as e:
            log.warning(f"[yahooquery] Failed for {ticker}: {e}")
            return None
