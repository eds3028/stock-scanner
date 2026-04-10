"""Backtest and predictive-bucket analysis over stored scan snapshots."""

from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd

DEFAULT_VERSION = "v1"
BENCHMARK_TICKER = "^AXJO"


@dataclass
class BacktestResult:
    summary: dict
    monthly: pd.DataFrame
    holdings: pd.DataFrame
    benchmark_monthly: pd.DataFrame


def _month_end_dates(scan_dates: list[str]) -> list[str]:
    if not scan_dates:
        return []
    dti = pd.to_datetime(pd.Series(scan_dates)).sort_values().drop_duplicates()
    return [d.strftime("%Y-%m-%d") for d in dti.groupby(dti.dt.to_period("M")).max()]


def _max_drawdown(equity: pd.Series) -> float:
    if equity.empty:
        return 0.0
    roll_max = equity.cummax()
    dd = (equity / roll_max) - 1.0
    return float(dd.min())


def _scores_has_column(conn: sqlite3.Connection, column: str) -> bool:
    rows = conn.execute("PRAGMA table_info(scores)").fetchall()
    return any(r[1] == column for r in rows)


def available_versions(conn: sqlite3.Connection) -> list[str]:
    if not _scores_has_column(conn, "scoring_model_version"):
        return [DEFAULT_VERSION]
    rows = conn.execute("SELECT DISTINCT scoring_model_version as v FROM scores ORDER BY v").fetchall()
    versions = [r[0] for r in rows if r[0]]
    return versions or [DEFAULT_VERSION]


def run_backtest(
    conn: sqlite3.Connection,
    scoring_model_version: str = DEFAULT_VERSION,
    weighting: str = "equal",
    transaction_cost_bps: float = 10.0,
    top_n: Optional[int] = None,
) -> BacktestResult:
    has_model_version = _scores_has_column(conn, "scoring_model_version")
    version_filter = "COALESCE(scoring_model_version, ?) = ?" if has_model_version else "? = ?"
    query_params = (DEFAULT_VERSION, scoring_model_version) if has_model_version else (DEFAULT_VERSION, scoring_model_version)
    df = pd.read_sql_query(
        f"""
        SELECT scan_date, ticker, total_score, current_price
        FROM scores
        WHERE {version_filter}
          AND current_price IS NOT NULL
          AND current_price > 0
        """,
        conn,
        params=query_params,
    )
    if df.empty:
        return BacktestResult(
            summary={"error": "No rows found for selected score version."},
            monthly=pd.DataFrame(),
            holdings=pd.DataFrame(),
            benchmark_monthly=pd.DataFrame(),
        )

    month_ends = _month_end_dates(df["scan_date"].tolist())
    if len(month_ends) < 2:
        return BacktestResult(
            summary={"error": "Need at least 2 month-end observations."},
            monthly=pd.DataFrame(),
            holdings=pd.DataFrame(),
            benchmark_monthly=pd.DataFrame(),
        )

    panel = df.pivot_table(index="scan_date", columns="ticker", values="current_price", aggfunc="last").sort_index()
    score_panel = df.pivot_table(index="scan_date", columns="ticker", values="total_score", aggfunc="last").sort_index()

    strategy_rows = []
    holdings_rows = []
    prev_weights = None
    equity = 1.0

    for i in range(len(month_ends) - 1):
        d0, d1 = month_ends[i], month_ends[i + 1]
        if d0 not in panel.index or d1 not in panel.index:
            continue

        p0 = panel.loc[d0].dropna()
        p1 = panel.loc[d1].dropna()
        s0 = score_panel.loc[d0].dropna()
        common = p0.index.intersection(p1.index).intersection(s0.index)
        if len(common) < 5:
            continue

        snapshot = pd.DataFrame({"p0": p0[common], "p1": p1[common], "score": s0[common]})
        snapshot["ret"] = (snapshot["p1"] / snapshot["p0"]) - 1.0
        snapshot = snapshot.sort_values("score", ascending=False)
        if top_n and top_n > 0:
            snapshot = snapshot.head(top_n)
        if snapshot.empty:
            continue

        if weighting == "score":
            raw = snapshot["score"].clip(lower=0.0)
            if raw.sum() <= 0:
                weights = pd.Series(1 / len(snapshot), index=snapshot.index)
            else:
                weights = raw / raw.sum()
        else:
            weights = pd.Series(1 / len(snapshot), index=snapshot.index)

        gross = float((weights * snapshot["ret"]).sum())
        if prev_weights is None:
            turnover = 1.0
        else:
            joined = pd.concat([prev_weights, weights], axis=1).fillna(0.0)
            turnover = float((joined.iloc[:, 0] - joined.iloc[:, 1]).abs().sum() / 2.0)

        tc = turnover * (transaction_cost_bps / 10000.0)
        net = gross - tc
        equity *= 1 + net
        hit_rate = float((snapshot["ret"] > 0).mean())

        strategy_rows.append(
            {
                "period_start": d0,
                "period_end": d1,
                "n_holdings": int(len(snapshot)),
                "gross_return": gross,
                "transaction_cost": tc,
                "net_return": net,
                "turnover": turnover,
                "hit_rate": hit_rate,
                "equity": equity,
            }
        )

        for t in snapshot.index:
            holdings_rows.append(
                {
                    "rebalance_date": d0,
                    "next_date": d1,
                    "ticker": t,
                    "score": float(snapshot.loc[t, "score"]),
                    "weight": float(weights.loc[t]),
                    "forward_return": float(snapshot.loc[t, "ret"]),
                }
            )
        prev_weights = weights

    monthly = pd.DataFrame(strategy_rows)
    holdings = pd.DataFrame(holdings_rows)
    if monthly.empty:
        return BacktestResult(
            summary={"error": "Insufficient overlapping history to run backtest."},
            monthly=monthly,
            holdings=holdings,
            benchmark_monthly=pd.DataFrame(),
        )

    monthly_rets = monthly["net_return"]
    years = max(len(monthly_rets) / 12.0, 1e-9)
    cagr = float((monthly["equity"].iloc[-1]) ** (1 / years) - 1)
    sharpe = float((monthly_rets.mean() / max(monthly_rets.std(ddof=1), 1e-9)) * math.sqrt(12)) if len(monthly_rets) > 1 else 0.0

    benchmark = pd.read_sql_query(
        """
        SELECT scan_date, current_price
        FROM scores
        WHERE ticker = ?
        ORDER BY scan_date
        """,
        conn,
        params=(BENCHMARK_TICKER,),
    )
    benchmark_monthly = pd.DataFrame()
    benchmark_cagr = None
    if not benchmark.empty:
        benchmark = benchmark.dropna().drop_duplicates("scan_date")
        benchmark = benchmark[benchmark["scan_date"].isin(monthly["period_start"].tolist() + [monthly["period_end"].iloc[-1]])]
        benchmark = benchmark.sort_values("scan_date")
        if len(benchmark) > 1:
            benchmark["return"] = benchmark["current_price"].pct_change()
            benchmark_monthly = benchmark.dropna().copy()
            b_eq = (1 + benchmark_monthly["return"]).cumprod()
            benchmark_cagr = float(b_eq.iloc[-1] ** (1 / years) - 1)

    summary = {
        "scoring_model_version": scoring_model_version,
        "weighting": weighting,
        "transaction_cost_bps": transaction_cost_bps,
        "period_start": monthly["period_start"].iloc[0],
        "period_end": monthly["period_end"].iloc[-1],
        "months": int(len(monthly)),
        "cagr": cagr,
        "max_drawdown": _max_drawdown(monthly["equity"]),
        "sharpe": sharpe,
        "avg_turnover": float(monthly["turnover"].mean()),
        "hit_rate": float(monthly["hit_rate"].mean()),
        "benchmark_ticker": BENCHMARK_TICKER,
        "benchmark_cagr": benchmark_cagr,
    }

    return BacktestResult(summary=summary, monthly=monthly, holdings=holdings, benchmark_monthly=benchmark_monthly)


def forward_bucket_analysis(
    conn: sqlite3.Connection,
    scoring_model_version: str = DEFAULT_VERSION,
    bucket_count: int = 10,
    horizons: tuple[int, ...] = (1, 3, 6, 12),
) -> tuple[pd.DataFrame, pd.DataFrame]:
    has_model_version = _scores_has_column(conn, "scoring_model_version")
    version_filter = "COALESCE(scoring_model_version, ?) = ?" if has_model_version else "? = ?"
    query_params = (DEFAULT_VERSION, scoring_model_version) if has_model_version else (DEFAULT_VERSION, scoring_model_version)
    raw = pd.read_sql_query(
        f"""
        SELECT scan_date, ticker, total_score, value_score, future_score, past_score, health_score, dividend_score, current_price
        FROM scores
        WHERE {version_filter}
          AND current_price IS NOT NULL
          AND current_price > 0
        ORDER BY scan_date
        """,
        conn,
        params=query_params,
    )
    if raw.empty:
        return pd.DataFrame(), pd.DataFrame()

    dates = sorted(raw["scan_date"].unique())
    date_to_i = {d: i for i, d in enumerate(dates)}
    panel = raw.pivot_table(index="scan_date", columns="ticker", values="current_price", aggfunc="last").sort_index()

    bucket_rows = []
    factor_rows = []
    factors = ["value_score", "future_score", "past_score", "health_score", "dividend_score"]

    for d in dates:
        i = date_to_i[d]
        cross = raw[raw["scan_date"] == d].copy()
        if len(cross) < bucket_count:
            continue
        cross["score_bucket"] = pd.qcut(cross["total_score"], bucket_count, labels=False, duplicates="drop") + 1

        for h in horizons:
            j = i + h
            if j >= len(dates):
                continue
            d_fwd = dates[j]
            p0 = panel.loc[d] if d in panel.index else None
            p1 = panel.loc[d_fwd] if d_fwd in panel.index else None
            if p0 is None or p1 is None:
                continue

            idx = cross["ticker"].isin(p0.dropna().index.intersection(p1.dropna().index))
            sample = cross[idx].copy()
            if sample.empty:
                continue
            sample["fwd_return"] = sample["ticker"].map((p1 / p0) - 1.0)

            grouped = sample.groupby("score_bucket")["fwd_return"].mean().reset_index()
            for _, r in grouped.iterrows():
                bucket_rows.append(
                    {
                        "scan_date": d,
                        "horizon_m": h,
                        "bucket": int(r["score_bucket"]),
                        "avg_return": float(r["fwd_return"]),
                    }
                )

            for factor in factors:
                fac = sample[[factor, "fwd_return"]].dropna()
                if len(fac) < bucket_count:
                    continue
                fac["bucket"] = pd.qcut(fac[factor], bucket_count, labels=False, duplicates="drop") + 1
                by_fac = fac.groupby("bucket")["fwd_return"].mean().reset_index()
                for _, fr in by_fac.iterrows():
                    factor_rows.append(
                        {
                            "scan_date": d,
                            "horizon_m": h,
                            "factor": factor,
                            "bucket": int(fr["bucket"]),
                            "avg_return": float(fr["fwd_return"]),
                        }
                    )

    return pd.DataFrame(bucket_rows), pd.DataFrame(factor_rows)


def export_backtest_csv(result: BacktestResult, out_dir: Path, prefix: str) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    monthly_path = out_dir / f"{prefix}_monthly.csv"
    holdings_path = out_dir / f"{prefix}_holdings.csv"
    summary_path = out_dir / f"{prefix}_summary.csv"
    result.monthly.to_csv(monthly_path, index=False)
    result.holdings.to_csv(holdings_path, index=False)
    pd.DataFrame([result.summary]).to_csv(summary_path, index=False)
    return {
        "monthly": str(monthly_path),
        "holdings": str(holdings_path),
        "summary": str(summary_path),
    }
