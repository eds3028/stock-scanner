"""Weighted, sector-aware stock scoring engine with confidence penalties."""

from __future__ import annotations

import math
import time
from dataclasses import dataclass


SCORING_MODEL_VERSION = "v1"

CATEGORY_WEIGHTS = {
    "value": 0.26,
    "future": 0.24,
    "past": 0.18,
    "health": 0.22,
    "dividends": 0.10,
}


SECTOR_TEMPLATES = {
    "default": {
        "name": "General Equities",
        "value": {
            "pe": [12, 18, 24],
            "pb": [1.5, 2.5, 4.0],
            "ev_ebitda": [8, 12, 18],
        },
        "health": {"de": [0.4, 0.8, 1.4]},
    },
    "financials": {
        "name": "Financials",
        "value": {
            "pe": [10, 14, 20],
            "pb": [0.9, 1.4, 2.0],
            "ev_ebitda": [10, 14, 20],
        },
        "health": {"de": [1.0, 2.0, 3.5]},
    },
    "reit": {
        "name": "REITs",
        "value": {
            "pe": [10, 16, 24],
            "pb": [0.8, 1.2, 1.8],
            "ev_ebitda": [12, 18, 24],
        },
        "health": {"de": [0.8, 1.5, 2.2]},
    },
    "resources": {
        "name": "Resources & Miners",
        "value": {
            "pe": [8, 12, 18],
            "pb": [1.0, 1.8, 2.8],
            "ev_ebitda": [4, 8, 12],
        },
        "health": {"de": [0.3, 0.7, 1.2]},
    },
    "industrials": {
        "name": "Industrials",
        "value": {
            "pe": [12, 18, 26],
            "pb": [1.3, 2.3, 3.5],
            "ev_ebitda": [7, 11, 16],
        },
        "health": {"de": [0.5, 1.0, 1.8]},
    },
}


def _score_lower_better(value, bands):
    if value is None or value <= 0:
        return 0.0
    excellent, good, fair = bands
    if value <= excellent:
        return 1.0
    if value <= good:
        return 0.8
    if value <= fair:
        return 0.55
    return 0.2


def _score_higher_better(value, bands):
    if value is None:
        return 0.0
    excellent, good, fair = bands
    if value >= excellent:
        return 1.0
    if value >= good:
        return 0.8
    if value >= fair:
        return 0.55
    return 0.2


def _resolve_template(info: dict) -> tuple[str, dict]:
    sector = (info.get("sector") or "").lower()
    industry = (info.get("industry") or "").lower()
    if any(k in sector for k in ("financial", "bank", "insurance")):
        return "financials", SECTOR_TEMPLATES["financials"]
    if "reit" in sector or "reit" in industry or "real estate" in sector:
        return "reit", SECTOR_TEMPLATES["reit"]
    if any(k in sector for k in ("basic materials", "energy", "resource")) or any(
        k in industry for k in ("mining", "metals", "oil", "gas")
    ):
        return "resources", SECTOR_TEMPLATES["resources"]
    if "industrial" in sector:
        return "industrials", SECTOR_TEMPLATES["industrials"]
    return "default", SECTOR_TEMPLATES["default"]


def score_value(info: dict, template: dict | None = None) -> dict:
    t = template or SECTOR_TEMPLATES["default"]
    pe = info.get("trailingPE") or info.get("forwardPE")
    pb = info.get("priceToBook")
    ev = info.get("enterpriseToEbitda")
    price = info.get("currentPrice") or info.get("regularMarketPrice")
    target = info.get("targetMeanPrice")

    fcf = info.get("freeCashflow")
    shares = info.get("sharesOutstanding")
    growth_rate = max(min(info.get("earningsGrowth") or 0.05, 0.30), -0.10)

    dcf_score = 0.0
    dcf_value = None
    if fcf and shares and shares > 0:
        discount_rate, terminal_growth, years = 0.10, 0.025, 10
        projected, dcf = fcf, 0.0
        for i in range(1, years + 1):
            projected *= 1 + growth_rate * max(0, (years - i) / years)
            dcf += projected / ((1 + discount_rate) ** i)
        terminal = (projected * (1 + terminal_growth)) / (discount_rate - terminal_growth)
        dcf += terminal / ((1 + discount_rate) ** years)
        dcf_value = dcf / shares
        if price and price > 0:
            margin = (dcf_value - price) / price
            dcf_score = _score_higher_better(margin, [0.35, 0.15, 0.0])

    upside_score = 0.0
    dispersion = None
    upside = None
    if price and target and price > 0 and target > 0:
        upside = (target - price) / price
        low = info.get("targetLowPrice")
        high = info.get("targetHighPrice")
        if low and high:
            dispersion = (high - low) / target
        upside_score = _score_higher_better(upside, [0.30, 0.15, 0.05])
        if dispersion is not None:
            upside_score = max(0.0, upside_score - _score_higher_better(dispersion, [0.25, 0.18, 0.12]) + 0.2)

    factors = {
        "dcf_margin": {"raw": round((dcf_value - price) / price, 4) if dcf_value and price else None, "score": round(dcf_score, 3), "weight": 0.25},
        "pe_ratio": {"raw": pe, "score": round(_score_lower_better(pe, t["value"]["pe"]), 3), "weight": 0.2},
        "price_to_book": {"raw": pb, "score": round(_score_lower_better(pb, t["value"]["pb"]), 3), "weight": 0.2},
        "ev_ebitda": {"raw": ev, "score": round(_score_lower_better(ev, t["value"]["ev_ebitda"]), 3), "weight": 0.2},
        "analyst_upside": {"raw": upside, "score": round(upside_score, 3), "weight": 0.15},
    }
    weighted = sum(v["score"] * v["weight"] for v in factors.values())
    return {
        "score": round(weighted * 6, 2),
        "normalized": round(weighted * 100, 1),
        "factors": factors,
        "data": {
            "dcf_fair_value": round(dcf_value, 2) if dcf_value else None,
            "current_price": price,
            "analyst_upside": round((upside or 0) * 100, 1) if upside is not None else None,
            "analyst_dispersion": round((dispersion or 0) * 100, 1) if dispersion is not None else None,
        },
    }


def _generic_dimension(info: dict, specs: dict) -> dict:
    factors = {}
    for name, cfg in specs.items():
        raw = cfg["value"](info)
        fn = _score_higher_better if cfg.get("direction", "high") == "high" else _score_lower_better
        factors[name] = {
            "raw": raw,
            "score": round(fn(raw, cfg["bands"]), 3),
            "weight": cfg["weight"],
        }
    weighted = sum(v["score"] * v["weight"] for v in factors.values())
    return {"score": round(weighted * 6, 2), "normalized": round(weighted * 100, 1), "factors": factors, "data": {}}


def score_future(info: dict) -> dict:
    return _generic_dimension(info, {
        "earnings_growth": {"value": lambda i: i.get("earningsGrowth"), "bands": [0.15, 0.08, 0.03], "weight": 0.24},
        "revenue_growth": {"value": lambda i: i.get("revenueGrowth"), "bands": [0.12, 0.06, 0.02], "weight": 0.2},
        "roe": {"value": lambda i: i.get("returnOnEquity"), "bands": [0.20, 0.15, 0.10], "weight": 0.2},
        "eps_forward_delta": {"value": lambda i: ((i.get("forwardEps") or 0) - (i.get("trailingEps") or 0)) / abs(i.get("trailingEps") or 1), "bands": [0.20, 0.08, 0.0], "weight": 0.18},
        "analyst_coverage": {"value": lambda i: i.get("numberOfAnalystOpinions") or 0, "bands": [8, 4, 2], "weight": 0.18},
    })


def score_past(info: dict) -> dict:
    price = info.get("currentPrice") or info.get("regularMarketPrice")
    low, high = info.get("fiftyTwoWeekLow"), info.get("fiftyTwoWeekHigh")

    def pos(_):
        if price and low and high and high > low:
            return (price - low) / (high - low)
        return None

    return _generic_dimension(info, {
        "historical_roe": {"value": lambda i: i.get("returnOnEquity"), "bands": [0.20, 0.15, 0.10], "weight": 0.22},
        "roa": {"value": lambda i: i.get("returnOnAssets"), "bands": [0.08, 0.05, 0.02], "weight": 0.18},
        "operating_margin": {"value": lambda i: i.get("operatingMargins"), "bands": [0.18, 0.10, 0.05], "weight": 0.2},
        "gross_margin": {"value": lambda i: i.get("grossMargins"), "bands": [0.45, 0.30, 0.20], "weight": 0.18},
        "momentum_52w": {"value": pos, "bands": [0.75, 0.50, 0.30], "weight": 0.22},
    })


def score_health(info: dict, template: dict | None = None) -> dict:
    t = template or SECTOR_TEMPLATES["default"]
    return _generic_dimension(info, {
        "debt_to_equity": {
            "value": lambda i: (i.get("debtToEquity") / 100) if i.get("debtToEquity") is not None else None,
            "bands": t["health"]["de"],
            "direction": "low",
            "weight": 0.28,
        },
        "current_ratio": {"value": lambda i: i.get("currentRatio"), "bands": [2.0, 1.3, 1.0], "weight": 0.2},
        "quick_ratio": {"value": lambda i: i.get("quickRatio"), "bands": [1.6, 1.1, 0.8], "weight": 0.16},
        "net_cash_ratio": {
            "value": lambda i: ((i.get("totalCash") or 0) - (i.get("totalDebt") or 0)) / max(abs(i.get("marketCap") or 1), 1),
            "bands": [0.08, 0.0, -0.08],
            "weight": 0.18,
        },
        "free_cashflow_positive": {"value": lambda i: i.get("freeCashflow"), "bands": [1, 0.1, 0], "weight": 0.18},
    })


def score_dividends(info: dict) -> dict:
    return _generic_dimension(info, {
        "dividend_yield": {"value": lambda i: i.get("dividendYield"), "bands": [0.06, 0.03, 0.015], "weight": 0.3},
        "payout_ratio": {"value": lambda i: i.get("payoutRatio"), "bands": [0.45, 0.7, 0.9], "direction": "low", "weight": 0.3},
        "fcf_cover": {
            "value": lambda i: ((i.get("freeCashflow") or 0) / max(i.get("sharesOutstanding") or 1, 1)) - (i.get("dividendRate") or 0),
            "bands": [1.0, 0.2, 0],
            "weight": 0.25,
        },
        "yield_vs_5y": {
            "value": lambda i: (i.get("dividendYield") or 0) / max(i.get("fiveYearAvgDividendYield") or 0.0001, 0.0001),
            "bands": [1.2, 1.0, 0.85],
            "weight": 0.15,
        },
    })


def _confidence(info: dict) -> dict:
    fetched_at = info.get("dataFetchedAt")
    completeness = info.get("dataCompleteness")
    provider = (info.get("dataProvider") or "").lower()

    age_h = 999.0
    freshness = 0.2
    if fetched_at:
        age_h = max(0.0, (time.time() - fetched_at) / 3600)
        freshness = max(0.0, min(1.0, 1 - (age_h / 168)))
    if completeness is None:
        completeness = 0.5
    provenance = 0.75
    if "finnhub" in provider:
        provenance = 0.9
    elif "yahooquery" in provider:
        provenance = 0.85
    elif "alpha" in provider:
        provenance = 0.7
    if "cached" in provider:
        provenance -= 0.1

    score = 0.35 * freshness + 0.45 * completeness + 0.20 * max(0.0, provenance)
    badge = "High" if score >= 0.75 else "Medium" if score >= 0.5 else "Low"
    return {
        "score": round(score, 3),
        "badge": badge,
        "components": {
            "freshness": round(freshness, 3),
            "completeness": round(completeness, 3),
            "provenance": round(max(0.0, provenance), 3),
            "age_hours": round(age_h, 1),
        },
    }


def score_stock(info: dict, ticker: str = "") -> dict:
    template_key, template = _resolve_template(info)
    value = score_value(info, template)
    future = score_future(info)
    past = score_past(info)
    health = score_health(info, template)
    dividends = score_dividends(info)

    dims = {"value": value, "future": future, "past": past, "health": health, "dividends": dividends}
    weighted_total = sum(dims[k]["normalized"] * CATEGORY_WEIGHTS[k] for k in CATEGORY_WEIGHTS)

    confidence = _confidence(info)
    # Up to 20-point penalty for weak confidence.
    confidence_penalty = (1 - confidence["score"]) * 20
    adjusted = max(0.0, weighted_total - confidence_penalty)

    return {
        "ticker": ticker,
        "template_key": template_key,
        "template_name": template["name"],
        "weighted_total": round(weighted_total, 2),
        "confidence": confidence,
        "confidence_penalty": round(confidence_penalty, 2),
        "adjusted_total": round(adjusted, 2),
        # Backward-compat 30-point representation for legacy UI components.
        "total_score": round(adjusted * 0.3, 2),
        "max_score": 30,
        "dimensions": dims,
        "scoring_model_version": SCORING_MODEL_VERSION,
    }
