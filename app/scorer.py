"""
SWS-style scoring engine.
Runs 30 checks across 5 dimensions: Value, Future, Past, Health, Dividends.
Each check scores 0 or 1. Max score per dimension is 6.
"""

import math


def safe_get(d, *keys, default=None):
    """Safely navigate nested dicts."""
    for key in keys:
        if not isinstance(d, dict):
            return default
        d = d.get(key, default)
        if d is None:
            return default
    return d


def score_value(info: dict) -> dict:
    """6 value checks: DCF vs price, P/E vs sector, P/E vs market, P/B, EV/EBITDA, analyst target."""
    checks = {}
    results = {}

    pe = info.get("trailingPE") or info.get("forwardPE")
    pb = info.get("priceToBook")
    ev_ebitda = info.get("enterpriseToEbitda")
    price = info.get("currentPrice") or info.get("regularMarketPrice")
    target = info.get("targetMeanPrice")
    target_low = info.get("targetLowPrice")
    target_high = info.get("targetHighPrice")
    fcf = info.get("freeCashflow")
    market_cap = info.get("marketCap")
    shares = info.get("sharesOutstanding")
    discount_rate = 0.10
    growth_rate = info.get("earningsGrowth") or 0.05
    growth_rate = max(min(growth_rate, 0.30), -0.10)

    # Check 1: Price below DCF fair value
    dcf_value = None
    if fcf and shares and shares > 0:
        terminal_growth = 0.025
        years = 10
        projected_fcf = fcf
        dcf = 0
        for i in range(1, years + 1):
            projected_fcf *= (1 + growth_rate * max(0, (years - i) / years))
            dcf += projected_fcf / ((1 + discount_rate) ** i)
        terminal_value = (projected_fcf * (1 + terminal_growth)) / (discount_rate - terminal_growth)
        dcf += terminal_value / ((1 + discount_rate) ** years)
        dcf_value = dcf / shares
        checks["dcf_below_price"] = price is not None and dcf_value > price
        results["dcf_fair_value"] = round(dcf_value, 2)
    else:
        checks["dcf_below_price"] = False
        results["dcf_fair_value"] = None

    results["current_price"] = price

    # Check 2: P/E below sector median (proxy: below 20 for now)
    sector_pe_threshold = 20
    checks["pe_below_sector"] = pe is not None and pe > 0 and pe < sector_pe_threshold
    results["pe_ratio"] = round(pe, 2) if pe else None

    # Check 3: P/E below market (proxy: below 25)
    checks["pe_below_market"] = pe is not None and pe > 0 and pe < 25
    results["pe_threshold"] = 25

    # Check 4: P/B below 3
    checks["pb_reasonable"] = pb is not None and pb > 0 and pb < 3
    results["pb_ratio"] = round(pb, 2) if pb else None

    # Check 5: EV/EBITDA below 15
    checks["ev_ebitda_reasonable"] = ev_ebitda is not None and ev_ebitda > 0 and ev_ebitda < 15
    results["ev_ebitda"] = round(ev_ebitda, 2) if ev_ebitda else None

    # Check 6: Price at least 20% below analyst consensus AND dispersion < 15%
    analyst_check = False
    if price and target and target_low and target_high and target > 0:
        upside = (target - price) / price
        dispersion = (target_high - target_low) / target if target > 0 else 1
        analyst_check = upside >= 0.20 and dispersion < 0.15
        results["analyst_target"] = round(target, 2)
        results["analyst_upside"] = round(upside * 100, 1)
        results["analyst_dispersion"] = round(dispersion * 100, 1)
    checks["analyst_target_upside"] = analyst_check

    score = sum(1 for v in checks.values() if v)
    return {"score": score, "checks": checks, "data": results}


def score_future(info: dict) -> dict:
    """6 future performance checks: earnings growth, revenue growth, ROE forecast, EPS growth, analyst coverage, profit margin trend."""
    checks = {}
    results = {}

    earnings_growth = info.get("earningsGrowth")
    revenue_growth = info.get("revenueGrowth")
    roe = info.get("returnOnEquity")
    forward_eps = info.get("forwardEps")
    trailing_eps = info.get("trailingEps")
    num_analysts = info.get("numberOfAnalystOpinions") or 0
    profit_margins = info.get("profitMargins")
    gross_margins = info.get("grossMargins")

    # Check 1: Earnings expected to grow > 5% annually
    checks["earnings_growth_positive"] = earnings_growth is not None and earnings_growth > 0.05
    results["earnings_growth"] = round(earnings_growth * 100, 1) if earnings_growth is not None else None

    # Check 2: Revenue expected to grow > 5%
    checks["revenue_growth_positive"] = revenue_growth is not None and revenue_growth > 0.05
    results["revenue_growth"] = round(revenue_growth * 100, 1) if revenue_growth is not None else None

    # Check 3: ROE expected to be > 15%
    checks["roe_high"] = roe is not None and roe > 0.15
    results["return_on_equity"] = round(roe * 100, 1) if roe is not None else None

    # Check 4: Forward EPS > trailing EPS (earnings improving)
    checks["eps_improving"] = (
        forward_eps is not None and
        trailing_eps is not None and
        forward_eps > trailing_eps
    )
    results["forward_eps"] = round(forward_eps, 2) if forward_eps else None
    results["trailing_eps"] = round(trailing_eps, 2) if trailing_eps else None

    # Check 5: Covered by at least 3 analysts
    checks["analyst_coverage"] = num_analysts >= 3
    results["num_analysts"] = num_analysts

    # Check 6: Profit margin positive and reasonable
    checks["profit_margin_positive"] = profit_margins is not None and profit_margins > 0.05
    results["profit_margins"] = round(profit_margins * 100, 1) if profit_margins is not None else None

    score = sum(1 for v in checks.values() if v)
    return {"score": score, "checks": checks, "data": results}


def score_past(info: dict) -> dict:
    """6 past performance checks: ROE history, ROA, earnings quality, revenue consistency, profit margin, 52w performance."""
    checks = {}
    results = {}

    roe = info.get("returnOnEquity")
    roa = info.get("returnOnAssets")
    profit_margins = info.get("profitMargins")
    gross_margins = info.get("grossMargins")
    operating_margins = info.get("operatingMargins")
    revenue = info.get("totalRevenue")
    earnings_growth = info.get("earningsGrowth")
    price = info.get("currentPrice") or info.get("regularMarketPrice")
    week52_low = info.get("fiftyTwoWeekLow")
    week52_high = info.get("fiftyTwoWeekHigh")
    beta = info.get("beta")

    # Check 1: ROE > 15% (historically strong returns)
    checks["roe_strong"] = roe is not None and roe > 0.15
    results["return_on_equity"] = round(roe * 100, 1) if roe is not None else None

    # Check 2: ROA > 5%
    checks["roa_positive"] = roa is not None and roa > 0.05
    results["return_on_assets"] = round(roa * 100, 1) if roa is not None else None

    # Check 3: Operating margin > 10%
    checks["operating_margin_good"] = operating_margins is not None and operating_margins > 0.10
    results["operating_margins"] = round(operating_margins * 100, 1) if operating_margins is not None else None

    # Check 4: Gross margin > 30%
    checks["gross_margin_good"] = gross_margins is not None and gross_margins > 0.30
    results["gross_margins"] = round(gross_margins * 100, 1) if gross_margins is not None else None

    # Check 5: Positive earnings growth historically
    checks["earnings_growth_historic"] = earnings_growth is not None and earnings_growth > 0
    results["earnings_growth"] = round(earnings_growth * 100, 1) if earnings_growth is not None else None

    # Check 6: Price closer to 52w high than low (momentum)
    position_check = False
    if price and week52_low and week52_high and week52_high > week52_low:
        position = (price - week52_low) / (week52_high - week52_low)
        position_check = position > 0.40
        results["52w_position"] = round(position * 100, 1)
    checks["price_momentum"] = position_check

    score = sum(1 for v in checks.values() if v)
    return {"score": score, "checks": checks, "data": results}


def score_health(info: dict) -> dict:
    """6 financial health checks: debt/equity, current ratio, interest coverage, cash vs debt, debt trend, quick ratio."""
    checks = {}
    results = {}

    debt_to_equity = info.get("debtToEquity")
    current_ratio = info.get("currentRatio")
    quick_ratio = info.get("quickRatio")
    total_debt = info.get("totalDebt") or 0
    total_cash = info.get("totalCash") or 0
    ebitda = info.get("ebitda")
    interest_expense = info.get("interestExpense")
    operating_cashflow = info.get("operatingCashflow")
    free_cashflow = info.get("freeCashflow")

    # Check 1: Debt/equity < 100% (or 1.0)
    de = debt_to_equity / 100 if debt_to_equity else None  # yfinance returns as percentage
    checks["debt_equity_low"] = de is not None and de < 1.0
    results["debt_to_equity"] = round(de, 2) if de is not None else None

    # Check 2: Current ratio > 1
    checks["current_ratio_good"] = current_ratio is not None and current_ratio > 1.0
    results["current_ratio"] = round(current_ratio, 2) if current_ratio is not None else None

    # Check 3: Quick ratio > 1
    checks["quick_ratio_good"] = quick_ratio is not None and quick_ratio > 1.0
    results["quick_ratio"] = round(quick_ratio, 2) if quick_ratio is not None else None

    # Check 4: Cash > debt (net cash positive)
    checks["net_cash_positive"] = total_cash > total_debt
    results["total_cash"] = total_cash
    results["total_debt"] = total_debt
    results["net_cash"] = total_cash - total_debt

    # Check 5: Interest coverage > 3x (EBITDA / interest)
    interest_coverage_ok = False
    if ebitda and interest_expense and interest_expense != 0:
        coverage = abs(ebitda / interest_expense)
        interest_coverage_ok = coverage > 3
        results["interest_coverage"] = round(coverage, 1)
    elif not interest_expense or interest_expense == 0:
        interest_coverage_ok = True  # No interest = no debt burden
        results["interest_coverage"] = None
    checks["interest_coverage_good"] = interest_coverage_ok

    # Check 6: Positive free cash flow
    checks["positive_fcf"] = free_cashflow is not None and free_cashflow > 0
    results["free_cashflow"] = free_cashflow

    score = sum(1 for v in checks.values() if v)
    return {"score": score, "checks": checks, "data": results}


def score_dividends(info: dict) -> dict:
    """6 dividend checks: pays dividend, yield reasonable, payout ratio sustainable, growth, covered by FCF, consistency."""
    checks = {}
    results = {}

    div_yield = info.get("dividendYield") or 0
    payout_ratio = info.get("payoutRatio") or 0
    div_rate = info.get("dividendRate") or 0
    five_year_avg_yield = info.get("fiveYearAvgDividendYield") or 0
    trailing_eps = info.get("trailingEps") or 0
    free_cashflow = info.get("freeCashflow") or 0
    shares = info.get("sharesOutstanding") or 1

    # Check 1: Pays a dividend
    checks["pays_dividend"] = div_yield > 0
    results["dividend_yield"] = round(div_yield * 100, 2) if div_yield else None

    # Check 2: Yield > 2% (meaningful income)
    checks["yield_meaningful"] = div_yield > 0.02
    results["dividend_rate"] = round(div_rate, 2) if div_rate else None

    # Check 3: Payout ratio < 80% (sustainable)
    checks["payout_sustainable"] = 0 < payout_ratio < 0.80
    results["payout_ratio"] = round(payout_ratio * 100, 1) if payout_ratio else None

    # Check 4: Payout ratio < 90% in 3 years (future coverage, proxy using current)
    checks["future_payout_covered"] = 0 < payout_ratio < 0.90
    
    # Check 5: Dividend covered by free cash flow
    fcf_per_share = free_cashflow / shares if shares > 0 else 0
    checks["fcf_covers_dividend"] = fcf_per_share > div_rate if div_rate > 0 else False
    results["fcf_per_share"] = round(fcf_per_share, 2)

    # Check 6: Dividend yield above 5yr average (improving income)
    checks["yield_above_average"] = (
        five_year_avg_yield > 0 and
        div_yield > 0 and
        div_yield >= five_year_avg_yield * 0.9
    )
    results["five_year_avg_yield"] = round(five_year_avg_yield, 2) if five_year_avg_yield else None

    score = sum(1 for v in checks.values() if v)
    return {"score": score, "checks": checks, "data": results}


def score_stock(info: dict, ticker: str = "") -> dict:
    """Run all 5 dimension scores and return full result."""
    value = score_value(info)
    future = score_future(info)
    past = score_past(info)
    health = score_health(info)
    dividends = score_dividends(info)

    total = value["score"] + future["score"] + past["score"] + health["score"] + dividends["score"]

    return {
        "ticker": ticker,
        "total_score": total,
        "max_score": 30,
        "dimensions": {
            "value": value,
            "future": future,
            "past": past,
            "health": health,
            "dividends": dividends,
        }
    }
