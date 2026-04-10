import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

from scorer import score_stock, score_value, score_dividends


def _base(**overrides):
    d = {
        "sector": "Financial Services",
        "industry": "Banks",
        "trailingPE": 11.0,
        "priceToBook": 1.1,
        "enterpriseToEbitda": 11.0,
        "currentPrice": 20.0,
        "targetMeanPrice": 24.0,
        "targetLowPrice": 23.0,
        "targetHighPrice": 25.0,
        "freeCashflow": 1_000_000_000,
        "sharesOutstanding": 100_000_000,
        "earningsGrowth": 0.10,
        "revenueGrowth": 0.08,
        "returnOnEquity": 0.18,
        "returnOnAssets": 0.06,
        "operatingMargins": 0.15,
        "grossMargins": 0.35,
        "forwardEps": 1.5,
        "trailingEps": 1.2,
        "numberOfAnalystOpinions": 7,
        "fiftyTwoWeekLow": 14.0,
        "fiftyTwoWeekHigh": 24.0,
        "debtToEquity": 120,
        "currentRatio": 1.3,
        "quickRatio": 1.0,
        "totalCash": 5_000_000_000,
        "totalDebt": 4_000_000_000,
        "marketCap": 40_000_000_000,
        "dividendYield": 0.04,
        "dividendRate": 0.8,
        "payoutRatio": 0.55,
        "fiveYearAvgDividendYield": 0.035,
        "dataFetchedAt": None,
        "dataCompleteness": 0.9,
        "dataProvider": "finnhub",
    }
    d.update(overrides)
    return d


def test_sector_template_resolves_financials():
    res = score_stock(_base(), "CBA.AX")
    assert res["template_key"] == "financials"


def test_near_threshold_not_same_as_excellent():
    template = {
        "value": {"pe": [12, 18, 24], "pb": [1.5, 2.5, 4.0], "ev_ebitda": [8, 12, 18]},
        "health": {"de": [0.4, 0.8, 1.4]},
    }
    near = score_value(_base(trailingPE=17.9, priceToBook=2.49, enterpriseToEbitda=11.9), template)
    great = score_value(_base(trailingPE=9.0, priceToBook=1.0, enterpriseToEbitda=6.0), template)
    assert near["normalized"] < great["normalized"]


def test_confidence_penalty_applies():
    strong = score_stock(_base(dataCompleteness=0.95, dataProvider="finnhub"), "AAA.AX")
    weak = score_stock(_base(dataCompleteness=0.2, dataProvider="alpha_vantage (cached)", dataFetchedAt=0), "AAA.AX")
    assert weak["confidence"]["badge"] == "Low"
    assert weak["adjusted_total"] < strong["adjusted_total"]


def test_factor_data_has_raw_and_normalized_score():
    res = score_stock(_base(), "XYZ.AX")
    factor = res["dimensions"]["value"]["factors"]["pe_ratio"]
    assert "raw" in factor
    assert "score" in factor


def test_score_includes_model_version():
    res = score_stock(_base(), "VER.AX")
    assert res["scoring_model_version"] == "v1"


def test_explanation_layer_includes_auditable_points():
    res = score_stock(_base(), "WHY.AX")
    exp = res["explanation"]
    assert len(exp["why_buy"]) == 3
    assert len(exp["why_avoid"]) == 3
    assert "factor_score=" in exp["why_buy"][0]
    assert "Confidence is" in exp["confidence_note"]


def test_dividend_data_includes_grossed_up_yield_and_risk_flag():
    res = score_dividends(_base(dividendYield=0.05, payoutRatio=1.05, frankingLevel=1.0))
    assert res["data"]["grossed_up_yield"] > res["data"]["cash_yield"]
    assert res["data"]["payout_risk_flag"] == "Unsustainably high payout"


def test_asx_defaults_to_fully_franked_when_missing_franking_level():
    res = score_dividends(_base(symbol="WBC.AX", dividendYield=0.04, payoutRatio=0.6, frankingLevel=None))
    assert res["data"]["franking_level"] == 100.0
