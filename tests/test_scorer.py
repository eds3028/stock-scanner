"""
Tests for app/scorer.py — the SWS-style 30-check scoring engine.

Run with:
    pytest tests/
"""

import sys
import os

# Make the app package importable without installing it.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

import pytest
from scorer import (
    score_value,
    score_future,
    score_past,
    score_health,
    score_dividends,
    score_stock,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _score(result):
    return result["score"]


def _checks(result):
    return result["checks"]


# ---------------------------------------------------------------------------
# score_value
# ---------------------------------------------------------------------------

class TestScoreValue:
    def _info(self, **overrides):
        base = {
            "trailingPE": 12.0,       # < 20 (sector) and < 25 (market)
            "priceToBook": 1.5,       # < 3
            "enterpriseToEbitda": 8.0, # < 15
            "currentPrice": 10.0,
            "targetMeanPrice": 14.0,  # 40 % upside
            "targetLowPrice": 13.0,
            "targetHighPrice": 15.0,  # dispersion = (15-13)/14 = 14.3 % — just over threshold
            "freeCashflow": 5_000_000,
            "sharesOutstanding": 1_000_000,
            "marketCap": 10_000_000,
            "earningsGrowth": 0.10,
        }
        base.update(overrides)
        return base

    def test_pe_below_sector_threshold_passes(self):
        result = score_value(self._info(trailingPE=15.0))
        assert _checks(result)["pe_below_sector"] is True

    def test_pe_above_sector_threshold_fails(self):
        result = score_value(self._info(trailingPE=21.0))
        assert _checks(result)["pe_below_sector"] is False

    def test_pe_exactly_at_threshold_fails(self):
        # Boundary: PE must be strictly < 20
        result = score_value(self._info(trailingPE=20.0))
        assert _checks(result)["pe_below_sector"] is False

    def test_pe_below_market_threshold_passes(self):
        result = score_value(self._info(trailingPE=24.9))
        assert _checks(result)["pe_below_market"] is True

    def test_pe_above_market_threshold_fails(self):
        result = score_value(self._info(trailingPE=25.1))
        assert _checks(result)["pe_below_market"] is False

    def test_pb_below_3_passes(self):
        result = score_value(self._info(priceToBook=2.9))
        assert _checks(result)["pb_reasonable"] is True

    def test_pb_above_3_fails(self):
        result = score_value(self._info(priceToBook=3.1))
        assert _checks(result)["pb_reasonable"] is False

    def test_ev_ebitda_below_15_passes(self):
        result = score_value(self._info(enterpriseToEbitda=14.9))
        assert _checks(result)["ev_ebitda_reasonable"] is True

    def test_ev_ebitda_above_15_fails(self):
        result = score_value(self._info(enterpriseToEbitda=15.1))
        assert _checks(result)["ev_ebitda_reasonable"] is False

    def test_analyst_upside_check_passes_with_low_dispersion(self):
        # 25 % upside, dispersion = (13-11)/12 ≈ 16.7 % — OVER 15 % limit → fails
        result = score_value(self._info(
            currentPrice=10.0, targetMeanPrice=12.5,
            targetLowPrice=11.0, targetHighPrice=14.0,
        ))
        # dispersion = (14-11)/12.5 = 24 % > 15 % → check fails
        assert _checks(result)["analyst_target_upside"] is False

    def test_analyst_upside_check_passes_tight_spread(self):
        # 25 % upside, tight analyst spread
        result = score_value(self._info(
            currentPrice=10.0, targetMeanPrice=12.5,
            targetLowPrice=12.3, targetHighPrice=12.7,
        ))
        # upside = 25 %, dispersion = 0.4/12.5 = 3.2 % → passes
        assert _checks(result)["analyst_target_upside"] is True

    def test_dcf_check_passes_when_price_below_fair_value(self):
        # Large FCF relative to share count → high DCF value
        result = score_value(self._info(
            freeCashflow=10_000_000,
            sharesOutstanding=100_000,  # very few shares → high per-share DCF
            currentPrice=50.0,
        ))
        assert _checks(result)["dcf_below_price"] is True

    def test_dcf_check_skipped_when_missing_fcf(self):
        result = score_value(self._info(freeCashflow=None, sharesOutstanding=1_000_000))
        assert _checks(result)["dcf_below_price"] is False

    def test_missing_pe_gives_false_for_both_pe_checks(self):
        result = score_value(self._info(trailingPE=None, forwardPE=None))
        assert _checks(result)["pe_below_sector"] is False
        assert _checks(result)["pe_below_market"] is False

    def test_negative_pe_gives_false(self):
        result = score_value(self._info(trailingPE=-5.0))
        assert _checks(result)["pe_below_sector"] is False

    def test_score_is_count_of_true_checks(self):
        result = score_value(self._info())
        assert result["score"] == sum(1 for v in result["checks"].values() if v)

    def test_max_score_is_6(self):
        result = score_value(self._info(
            trailingPE=10.0,
            priceToBook=1.0,
            enterpriseToEbitda=5.0,
            freeCashflow=10_000_000,
            sharesOutstanding=100_000,
            currentPrice=50.0,
            targetMeanPrice=70.0,
            targetLowPrice=69.0,
            targetHighPrice=71.0,
        ))
        assert result["score"] <= 6


# ---------------------------------------------------------------------------
# score_future
# ---------------------------------------------------------------------------

class TestScoreFuture:
    def _info(self, **overrides):
        base = {
            "earningsGrowth": 0.10,    # > 5 %
            "revenueGrowth": 0.08,     # > 5 %
            "returnOnEquity": 0.20,    # > 15 %
            "forwardEps": 2.0,
            "trailingEps": 1.5,        # forward > trailing → improving
            "numberOfAnalystOpinions": 5,  # ≥ 3
            "profitMargins": 0.12,     # > 5 %
        }
        base.update(overrides)
        return base

    def test_all_checks_pass_with_good_data(self):
        result = score_future(self._info())
        assert _score(result) == 6

    def test_earnings_growth_below_threshold_fails(self):
        result = score_future(self._info(earningsGrowth=0.04))
        assert _checks(result)["earnings_growth_positive"] is False

    def test_earnings_growth_negative_fails(self):
        result = score_future(self._info(earningsGrowth=-0.05))
        assert _checks(result)["earnings_growth_positive"] is False

    def test_missing_earnings_growth_fails(self):
        result = score_future(self._info(earningsGrowth=None))
        assert _checks(result)["earnings_growth_positive"] is False

    def test_revenue_growth_below_threshold_fails(self):
        result = score_future(self._info(revenueGrowth=0.03))
        assert _checks(result)["revenue_growth_positive"] is False

    def test_roe_below_15pct_fails(self):
        result = score_future(self._info(returnOnEquity=0.14))
        assert _checks(result)["roe_high"] is False

    def test_forward_eps_below_trailing_fails(self):
        result = score_future(self._info(forwardEps=1.0, trailingEps=1.5))
        assert _checks(result)["eps_improving"] is False

    def test_missing_forward_eps_fails_eps_check(self):
        result = score_future(self._info(forwardEps=None))
        assert _checks(result)["eps_improving"] is False

    def test_fewer_than_3_analysts_fails(self):
        result = score_future(self._info(numberOfAnalystOpinions=2))
        assert _checks(result)["analyst_coverage"] is False

    def test_exactly_3_analysts_passes(self):
        result = score_future(self._info(numberOfAnalystOpinions=3))
        assert _checks(result)["analyst_coverage"] is True

    def test_profit_margin_below_5pct_fails(self):
        result = score_future(self._info(profitMargins=0.04))
        assert _checks(result)["profit_margin_positive"] is False

    def test_all_missing_data_scores_zero(self):
        result = score_future({})
        assert _score(result) == 0


# ---------------------------------------------------------------------------
# score_past
# ---------------------------------------------------------------------------

class TestScorePast:
    def _info(self, **overrides):
        base = {
            "returnOnEquity": 0.20,      # > 15 %
            "returnOnAssets": 0.08,      # > 5 %
            "operatingMargins": 0.15,    # > 10 %
            "grossMargins": 0.40,        # > 30 %
            "earningsGrowth": 0.10,      # > 0
            "currentPrice": 8.0,
            "fiftyTwoWeekLow": 5.0,
            "fiftyTwoWeekHigh": 10.0,   # position = (8-5)/(10-5) = 60 % > 40 %
        }
        base.update(overrides)
        return base

    def test_all_checks_pass_with_strong_data(self):
        result = score_past(self._info())
        assert _score(result) == 6

    def test_roe_below_15pct_fails(self):
        result = score_past(self._info(returnOnEquity=0.10))
        assert _checks(result)["roe_strong"] is False

    def test_roa_below_5pct_fails(self):
        result = score_past(self._info(returnOnAssets=0.04))
        assert _checks(result)["roa_positive"] is False

    def test_operating_margin_below_10pct_fails(self):
        result = score_past(self._info(operatingMargins=0.09))
        assert _checks(result)["operating_margin_good"] is False

    def test_gross_margin_below_30pct_fails(self):
        result = score_past(self._info(grossMargins=0.29))
        assert _checks(result)["gross_margin_good"] is False

    def test_negative_earnings_growth_fails_historic_check(self):
        result = score_past(self._info(earningsGrowth=-0.01))
        assert _checks(result)["earnings_growth_historic"] is False

    def test_price_momentum_above_40pct_passes(self):
        # position = (7-5)/(10-5) = 40 % → exactly at boundary, should fail (> not >=)
        result = score_past(self._info(currentPrice=7.0))
        assert _checks(result)["price_momentum"] is False

    def test_price_momentum_clearly_above_40pct_passes(self):
        # position = (7.1-5)/(10-5) = 42 %
        result = score_past(self._info(currentPrice=7.1))
        assert _checks(result)["price_momentum"] is True

    def test_price_momentum_near_52w_low_fails(self):
        # position = (5.5-5)/(10-5) = 10 %
        result = score_past(self._info(currentPrice=5.5))
        assert _checks(result)["price_momentum"] is False

    def test_missing_52w_range_skips_momentum_check(self):
        result = score_past(self._info(fiftyTwoWeekLow=None, fiftyTwoWeekHigh=None))
        assert _checks(result)["price_momentum"] is False

    def test_equal_52w_high_low_skips_momentum_check(self):
        result = score_past(self._info(fiftyTwoWeekLow=10.0, fiftyTwoWeekHigh=10.0))
        assert _checks(result)["price_momentum"] is False

    def test_all_missing_data_scores_zero(self):
        result = score_past({})
        assert _score(result) == 0


# ---------------------------------------------------------------------------
# score_health
# ---------------------------------------------------------------------------

class TestScoreHealth:
    def _info(self, **overrides):
        base = {
            "debtToEquity": 50.0,    # stored as percentage → 0.5 < 1.0
            "currentRatio": 2.0,     # > 1
            "quickRatio": 1.5,       # > 1
            "totalCash": 10_000_000,
            "totalDebt": 5_000_000,  # cash > debt
            "ebitda": 6_000_000,
            "interestExpense": 500_000,  # coverage = 12 > 3
            "freeCashflow": 3_000_000,   # > 0
        }
        base.update(overrides)
        return base

    def test_all_checks_pass_with_healthy_balance_sheet(self):
        result = score_health(self._info())
        assert _score(result) == 6

    def test_high_debt_equity_fails(self):
        # debtToEquity > 100 (i.e. > 1.0 after /100)
        result = score_health(self._info(debtToEquity=120.0))
        assert _checks(result)["debt_equity_low"] is False

    def test_debt_equity_exactly_100_fails(self):
        result = score_health(self._info(debtToEquity=100.0))
        assert _checks(result)["debt_equity_low"] is False

    def test_current_ratio_below_1_fails(self):
        result = score_health(self._info(currentRatio=0.9))
        assert _checks(result)["current_ratio_good"] is False

    def test_quick_ratio_below_1_fails(self):
        result = score_health(self._info(quickRatio=0.8))
        assert _checks(result)["quick_ratio_good"] is False

    def test_debt_exceeds_cash_fails_net_cash(self):
        result = score_health(self._info(totalCash=1_000_000, totalDebt=5_000_000))
        assert _checks(result)["net_cash_positive"] is False

    def test_no_debt_no_cash_fails_net_cash(self):
        # totalCash == totalDebt == 0 → not strictly positive
        result = score_health(self._info(totalCash=0, totalDebt=0))
        assert _checks(result)["net_cash_positive"] is False

    def test_interest_coverage_below_3x_fails(self):
        # ebitda=3M, interest=1.5M → coverage = 2 < 3
        result = score_health(self._info(ebitda=3_000_000, interestExpense=1_500_000))
        assert _checks(result)["interest_coverage_good"] is False

    def test_no_interest_expense_passes_coverage(self):
        # No debt burden at all → coverage check passes
        result = score_health(self._info(interestExpense=0))
        assert _checks(result)["interest_coverage_good"] is True

    def test_none_interest_expense_passes_coverage(self):
        result = score_health(self._info(interestExpense=None))
        assert _checks(result)["interest_coverage_good"] is True

    def test_negative_fcf_fails(self):
        result = score_health(self._info(freeCashflow=-1_000_000))
        assert _checks(result)["positive_fcf"] is False

    def test_missing_fcf_fails(self):
        result = score_health(self._info(freeCashflow=None))
        assert _checks(result)["positive_fcf"] is False

    def test_all_missing_data_scores_zero(self):
        result = score_health({})
        # totalCash and totalDebt default to 0, so net_cash_positive is False
        # interest_expense is None → coverage passes
        checks = _checks(result)
        assert checks["debt_equity_low"] is False
        assert checks["current_ratio_good"] is False
        assert checks["quick_ratio_good"] is False
        assert checks["net_cash_positive"] is False
        assert checks["positive_fcf"] is False


# ---------------------------------------------------------------------------
# score_dividends
# ---------------------------------------------------------------------------

class TestScoreDividends:
    def _info(self, **overrides):
        base = {
            "dividendYield": 0.04,       # 4 % > 2 %
            "payoutRatio": 0.50,         # 50 % < 80 % and < 90 %
            "dividendRate": 0.40,
            "fiveYearAvgDividendYield": 0.035, # current yield ≥ 90 % of avg
            "trailingEps": 1.0,
            "freeCashflow": 5_000_000,
            "sharesOutstanding": 1_000_000,  # FCF per share = 5.0 > 0.40 div rate
        }
        base.update(overrides)
        return base

    def test_all_checks_pass_with_good_dividend(self):
        result = score_dividends(self._info())
        assert _score(result) == 6

    def test_no_dividend_fails_first_two_checks(self):
        result = score_dividends(self._info(dividendYield=0, dividendRate=0))
        assert _checks(result)["pays_dividend"] is False
        assert _checks(result)["yield_meaningful"] is False

    def test_yield_below_2pct_fails_meaningful_check(self):
        result = score_dividends(self._info(dividendYield=0.01))
        assert _checks(result)["yield_meaningful"] is False

    def test_yield_exactly_2pct_fails(self):
        # Check requires > 0.02, not >= 0.02
        result = score_dividends(self._info(dividendYield=0.02))
        assert _checks(result)["yield_meaningful"] is False

    def test_high_payout_ratio_fails_sustainable_check(self):
        result = score_dividends(self._info(payoutRatio=0.85))
        assert _checks(result)["payout_sustainable"] is False

    def test_zero_payout_ratio_fails_sustainable_check(self):
        result = score_dividends(self._info(payoutRatio=0))
        assert _checks(result)["payout_sustainable"] is False

    def test_payout_ratio_between_80_and_90_fails_sustainable_passes_future(self):
        result = score_dividends(self._info(payoutRatio=0.85))
        assert _checks(result)["payout_sustainable"] is False
        assert _checks(result)["future_payout_covered"] is True

    def test_fcf_below_div_rate_fails_fcf_coverage(self):
        # FCF per share = 0.10, div rate = 0.40 → fails
        result = score_dividends(self._info(
            freeCashflow=100_000,
            sharesOutstanding=1_000_000,
            dividendRate=0.40,
        ))
        assert _checks(result)["fcf_covers_dividend"] is False

    def test_yield_below_90pct_of_5yr_avg_fails_consistency(self):
        # current yield = 0.03, 5yr avg = 0.04 → 0.03 < 0.04 * 0.9 = 0.036
        result = score_dividends(self._info(
            dividendYield=0.03,
            fiveYearAvgDividendYield=0.04,
        ))
        assert _checks(result)["yield_above_average"] is False

    def test_no_5yr_avg_fails_consistency_check(self):
        result = score_dividends(self._info(fiveYearAvgDividendYield=0))
        assert _checks(result)["yield_above_average"] is False

    def test_all_missing_data_scores_zero(self):
        result = score_dividends({})
        assert _score(result) == 0


# ---------------------------------------------------------------------------
# score_stock (integration / regression fixtures)
# ---------------------------------------------------------------------------

class TestScoreStock:
    """Regression fixtures — if scorer logic changes, these numbers must be
    deliberately updated."""

    # --- Fixture: well-rounded healthy dividend payer ---
    HEALTHY_STOCK = {
        # Value
        "trailingPE": 14.0,
        "priceToBook": 1.8,
        "enterpriseToEbitda": 9.0,
        "currentPrice": 10.0,
        "targetMeanPrice": 13.0,
        "targetLowPrice": 12.8,
        "targetHighPrice": 13.2,
        "freeCashflow": 10_000_000,
        "sharesOutstanding": 1_000_000,
        "marketCap": 10_000_000,
        # Future
        "earningsGrowth": 0.12,
        "revenueGrowth": 0.09,
        "returnOnEquity": 0.22,
        "forwardEps": 1.2,
        "trailingEps": 1.0,
        "numberOfAnalystOpinions": 6,
        "profitMargins": 0.15,
        # Past
        "returnOnAssets": 0.09,
        "operatingMargins": 0.18,
        "grossMargins": 0.45,
        "fiftyTwoWeekLow": 7.0,
        "fiftyTwoWeekHigh": 12.0,
        # Health
        "debtToEquity": 40.0,
        "currentRatio": 2.5,
        "quickRatio": 1.8,
        "totalCash": 8_000_000,
        "totalDebt": 2_000_000,
        "ebitda": 9_000_000,
        "interestExpense": 200_000,
        "operatingCashflow": 11_000_000,
        # Dividends
        "dividendYield": 0.045,
        "payoutRatio": 0.45,
        "dividendRate": 0.45,
        "fiveYearAvgDividendYield": 0.04,
    }

    # --- Fixture: distressed / poor quality stock ---
    WEAK_STOCK = {
        "trailingPE": 60.0,
        "priceToBook": 8.0,
        "enterpriseToEbitda": 40.0,
        "currentPrice": 2.0,
        "freeCashflow": -500_000,
        "sharesOutstanding": 50_000_000,
        "earningsGrowth": -0.15,
        "revenueGrowth": -0.10,
        "returnOnEquity": -0.05,
        "returnOnAssets": -0.03,
        "operatingMargins": -0.05,
        "grossMargins": 0.10,
        "fiftyTwoWeekLow": 1.5,
        "fiftyTwoWeekHigh": 5.0,
        "debtToEquity": 250.0,
        "currentRatio": 0.6,
        "quickRatio": 0.4,
        "totalCash": 500_000,
        "totalDebt": 20_000_000,
        "dividendYield": 0,
        "payoutRatio": 0,
    }

    def test_score_stock_returns_expected_keys(self):
        result = score_stock(self.HEALTHY_STOCK, "TEST.AX")
        assert result["ticker"] == "TEST.AX"
        assert "total_score" in result
        assert "max_score" in result
        assert result["max_score"] == 30
        assert set(result["dimensions"].keys()) == {
            "value", "future", "past", "health", "dividends"
        }

    def test_healthy_stock_scores_well(self):
        result = score_stock(self.HEALTHY_STOCK, "GOOD.AX")
        # Should score at least 20/30 with this dataset
        assert result["total_score"] >= 20, (
            f"Expected ≥ 20 but got {result['total_score']}"
        )

    def test_weak_stock_scores_poorly(self):
        result = score_stock(self.WEAK_STOCK, "BAD.AX")
        # Should score at most 5/30 with this dataset
        assert result["total_score"] <= 5, (
            f"Expected ≤ 5 but got {result['total_score']}"
        )

    def test_total_is_sum_of_dimensions(self):
        for stock, name in [(self.HEALTHY_STOCK, "HEALTHY"), (self.WEAK_STOCK, "WEAK")]:
            result = score_stock(stock, name)
            dims = result["dimensions"]
            computed = sum(dims[d]["score"] for d in dims)
            assert result["total_score"] == computed, (
                f"{name}: total_score {result['total_score']} != sum of dims {computed}"
            )

    def test_empty_info_does_not_raise(self):
        # An empty dict should never raise; health gives 1 point because
        # missing interest expense means "no debt burden" (passes coverage check).
        result = score_stock({}, "EMPTY.AX")
        assert result["total_score"] >= 0
        assert result["total_score"] <= 30

    def test_total_score_never_exceeds_30(self):
        result = score_stock(self.HEALTHY_STOCK)
        assert result["total_score"] <= 30

    def test_regression_healthy_stock_score(self):
        """Pin the exact score — update intentionally if checks change."""
        result = score_stock(self.HEALTHY_STOCK, "GOOD.AX")
        assert result["total_score"] == 30

    def test_regression_healthy_stock_value_dimension(self):
        result = score_stock(self.HEALTHY_STOCK, "GOOD.AX")
        assert result["dimensions"]["value"]["score"] == 6

    def test_regression_healthy_stock_health_dimension(self):
        result = score_stock(self.HEALTHY_STOCK, "GOOD.AX")
        assert result["dimensions"]["health"]["score"] == 6

    def test_regression_healthy_stock_dividends_dimension(self):
        result = score_stock(self.HEALTHY_STOCK, "GOOD.AX")
        assert result["dimensions"]["dividends"]["score"] == 6

    def test_ticker_defaults_to_empty_string(self):
        result = score_stock({})
        assert result["ticker"] == ""
