import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

from backtest import run_backtest, forward_bucket_analysis


def _seed(conn):
    conn.executescript(
        """
        CREATE TABLE scores (
            ticker TEXT,
            scan_date TEXT,
            total_score REAL,
            value_score REAL,
            future_score REAL,
            past_score REAL,
            health_score REAL,
            dividend_score REAL,
            current_price REAL,
            scoring_model_version TEXT
        );
        """
    )
    rows = [
        # Jan
        ("AAA.AX", "2025-01-31", 25, 5, 5, 5, 5, 5, 10, "v1"),
        ("BBB.AX", "2025-01-31", 20, 4, 4, 4, 4, 4, 10, "v1"),
        ("CCC.AX", "2025-01-31", 15, 3, 3, 3, 3, 3, 10, "v1"),
        ("DDD.AX", "2025-01-31", 10, 2, 2, 2, 2, 2, 10, "v1"),
        ("EEE.AX", "2025-01-31", 5, 1, 1, 1, 1, 1, 10, "v1"),
        # Feb
        ("AAA.AX", "2025-02-28", 25, 5, 5, 5, 5, 5, 11, "v1"),
        ("BBB.AX", "2025-02-28", 20, 4, 4, 4, 4, 4, 10.5, "v1"),
        ("CCC.AX", "2025-02-28", 15, 3, 3, 3, 3, 3, 10.1, "v1"),
        ("DDD.AX", "2025-02-28", 10, 2, 2, 2, 2, 2, 9.8, "v1"),
        ("EEE.AX", "2025-02-28", 5, 1, 1, 1, 1, 1, 9.6, "v1"),
        # Mar
        ("AAA.AX", "2025-03-31", 25, 5, 5, 5, 5, 5, 12, "v1"),
        ("BBB.AX", "2025-03-31", 20, 4, 4, 4, 4, 4, 11, "v1"),
        ("CCC.AX", "2025-03-31", 15, 3, 3, 3, 3, 3, 10.2, "v1"),
        ("DDD.AX", "2025-03-31", 10, 2, 2, 2, 2, 2, 9.7, "v1"),
        ("EEE.AX", "2025-03-31", 5, 1, 1, 1, 1, 1, 9.2, "v1"),
    ]
    conn.executemany("INSERT INTO scores VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    conn.commit()


def test_backtest_outputs_key_metrics():
    conn = sqlite3.connect(":memory:")
    _seed(conn)
    res = run_backtest(conn, scoring_model_version="v1", weighting="equal", transaction_cost_bps=0)
    assert "cagr" in res.summary
    assert "max_drawdown" in res.summary
    assert "sharpe" in res.summary
    assert "avg_turnover" in res.summary
    assert "hit_rate" in res.summary
    assert not res.monthly.empty


def test_bucket_analysis_outputs_rows():
    conn = sqlite3.connect(":memory:")
    _seed(conn)
    deciles, factors = forward_bucket_analysis(conn, scoring_model_version="v1", bucket_count=5, horizons=(1,))
    assert not deciles.empty
    assert not factors.empty
