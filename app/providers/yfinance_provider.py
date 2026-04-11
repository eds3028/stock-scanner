"""
yfinance provider adapter.
Free, no API key, good ASX coverage.
Actively maintained alternative to yahooquery.
"""

import logging
from typing import Optional

from providers.base import StockDataProvider, StockData

log = logging.getLogger(__name__)


class YFinanceProvider(StockDataProvider):

    def __init__(self):
        super().__init__("yfinance")
        self._import_ok = False
        try:
            import yfinance as yf
            self._yf = yf
            self._import_ok = True
        except ImportError:
            log.warning("yfinance not installed")

    def is_configured(self) -> bool:
        return self._import_ok

    def fetch(self, ticker: str) -> Optional[StockData]:
        try:
            stock = self._yf.Ticker(ticker)
            info = stock.info

            if not info or not isinstance(info, dict):
                return None

            price = info.get("currentPrice") or info.get("regularMarketPrice")
            if not price:
                return None

            return StockData(
                ticker=ticker,
                provider=self.name,
                company_name=info.get("longName") or info.get("shortName", ticker),
                sector=info.get("sector", ""),
                industry=info.get("industry", ""),
                current_price=price,
                market_cap=info.get("marketCap"),
                fifty_two_week_low=info.get("fiftyTwoWeekLow"),
                fifty_two_week_high=info.get("fiftyTwoWeekHigh"),
                beta=info.get("beta"),
                trailing_pe=info.get("trailingPE"),
                forward_pe=info.get("forwardPE"),
                price_to_book=info.get("priceToBook"),
                ev_to_ebitda=info.get("enterpriseToEbitda"),
                trailing_eps=info.get("trailingEps"),
                forward_eps=info.get("forwardEps"),
                shares_outstanding=info.get("sharesOutstanding"),
                dividend_yield=info.get("dividendYield"),
                dividend_rate=info.get("dividendRate"),
                payout_ratio=info.get("payoutRatio"),
                five_year_avg_dividend_yield=info.get("fiveYearAvgDividendYield"),
                profit_margins=info.get("profitMargins"),
                gross_margins=info.get("grossMargins"),
                operating_margins=info.get("operatingMargins"),
                return_on_equity=info.get("returnOnEquity"),
                return_on_assets=info.get("returnOnAssets"),
                revenue_growth=info.get("revenueGrowth"),
                earnings_growth=info.get("earningsGrowth"),
                total_revenue=info.get("totalRevenue"),
                current_ratio=info.get("currentRatio"),
                quick_ratio=info.get("quickRatio"),
                debt_to_equity=info.get("debtToEquity"),
                total_cash=info.get("totalCash"),
                total_debt=info.get("totalDebt"),
                free_cashflow=info.get("freeCashflow"),
                operating_cashflow=info.get("operatingCashflow"),
                ebitda=info.get("ebitda"),
                interest_expense=info.get("interestExpense"),
                target_mean_price=info.get("targetMeanPrice"),
                target_low_price=info.get("targetLowPrice"),
                target_high_price=info.get("targetHighPrice"),
                number_of_analyst_opinions=info.get("numberOfAnalystOpinions"),
            )

        except Exception as e:
            log.warning(f"[yfinance] Failed for {ticker}: {e}")
            return None
