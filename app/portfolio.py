"""Portfolio/watchlist helpers for dashboard workflows."""

from __future__ import annotations

import csv
import io
import sqlite3
from dataclasses import dataclass
from typing import Any

DEFAULT_RULES = {
    "max_position_weight": 0.12,
    "max_sector_weight": 0.30,
    "liquidity_floor_market_cap": 500_000_000,
    "max_sector_names": 4,
    "rebalance_tolerance": 0.02,
}


@dataclass
class HoldingSnapshot:
    ticker: str
    shares: float
    cost_base: float
    acquired_at: str | None
    target_weight: float
    current_weight: float


def init_portfolio_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS watchlists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS watchlist_items (
            watchlist_id INTEGER NOT NULL,
            ticker TEXT NOT NULL,
            note TEXT,
            added_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (watchlist_id, ticker),
            FOREIGN KEY (watchlist_id) REFERENCES watchlists(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS holdings (
            ticker TEXT PRIMARY KEY,
            shares REAL NOT NULL,
            cost_base REAL,
            acquired_at TEXT,
            target_weight REAL DEFAULT 0,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS portfolio_config (
            key TEXT PRIMARY KEY,
            value TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_watchlist_items_ticker ON watchlist_items(ticker);
        """
    )
    try:
        conn.execute("ALTER TABLE holdings ADD COLUMN acquired_at TEXT")
    except Exception:
        pass


def get_watchlists(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute("SELECT id, name FROM watchlists ORDER BY name").fetchall()
    return [dict(r) for r in rows]


def create_watchlist(conn: sqlite3.Connection, name: str) -> bool:
    if not name.strip():
        return False
    conn.execute("INSERT OR IGNORE INTO watchlists(name) VALUES (?)", (name.strip(),))
    return conn.total_changes > 0


def add_watchlist_tickers(conn: sqlite3.Connection, watchlist_id: int, tickers: list[str]) -> int:
    cleaned = sorted({t.strip().upper() for t in tickers if t.strip()})
    count = 0
    for ticker in cleaned:
        conn.execute(
            "INSERT OR IGNORE INTO watchlist_items(watchlist_id, ticker) VALUES(?, ?)",
            (watchlist_id, ticker),
        )
        if conn.total_changes > count:
            count += 1
    return count


def import_holdings_csv(conn: sqlite3.Connection, content: bytes) -> tuple[int, list[str]]:
    text = content.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    required = {"ticker", "shares", "cost_base", "target_weight"}
    missing = required - {h.strip() for h in (reader.fieldnames or [])}
    if missing:
        return 0, [f"Missing CSV columns: {', '.join(sorted(missing))}"]

    errors: list[str] = []
    imported = 0
    for i, row in enumerate(reader, start=2):
        try:
            ticker = (row.get("ticker") or "").strip().upper()
            shares = float(row.get("shares") or 0)
            cost_base = float(row.get("cost_base") or 0)
            target_weight = float(row.get("target_weight") or 0)
            acquired_at = (row.get("acquired_at") or "").strip() or None
            if not ticker:
                raise ValueError("empty ticker")
            conn.execute(
                """
                INSERT INTO holdings(ticker, shares, cost_base, acquired_at, target_weight, updated_at)
                VALUES(?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(ticker) DO UPDATE SET
                    shares=excluded.shares,
                    cost_base=excluded.cost_base,
                    acquired_at=excluded.acquired_at,
                    target_weight=excluded.target_weight,
                    updated_at=CURRENT_TIMESTAMP
                """,
                (ticker, shares, cost_base, acquired_at, target_weight),
            )
            imported += 1
        except Exception as exc:
            errors.append(f"Line {i}: {exc}")
    return imported, errors


def load_rules(conn: sqlite3.Connection) -> dict[str, float]:
    rows = conn.execute("SELECT key, value FROM portfolio_config").fetchall()
    rules = dict(DEFAULT_RULES)
    for r in rows:
        try:
            rules[r["key"]] = float(r["value"])
        except Exception:
            continue
    return rules


def save_rules(conn: sqlite3.Connection, rules: dict[str, float]) -> None:
    for key, value in rules.items():
        conn.execute(
            "INSERT INTO portfolio_config(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )


def save_user_preset(conn: sqlite3.Connection, name: str, preset: dict) -> None:
    import json as _json
    conn.execute(
        "INSERT INTO portfolio_config(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (f"preset:{name.strip()}", _json.dumps(preset)),
    )


def load_user_presets(conn: sqlite3.Connection) -> dict[str, dict]:
    import json as _json
    rows = conn.execute(
        "SELECT key, value FROM portfolio_config WHERE key LIKE 'preset:%'"
    ).fetchall()
    result = {}
    for r in rows:
        name = r["key"][7:]
        try:
            result[name] = _json.loads(r["value"])
        except Exception:
            pass
    return result


def delete_user_preset(conn: sqlite3.Connection, name: str) -> None:
    conn.execute("DELETE FROM portfolio_config WHERE key = ?", (f"preset:{name.strip()}",))


def holdings_snapshot(conn: sqlite3.Connection, score_rows: list[dict[str, Any]]) -> dict[str, HoldingSnapshot]:
    holdings = {r["ticker"]: dict(r) for r in conn.execute("SELECT ticker, shares, cost_base, acquired_at, target_weight FROM holdings").fetchall()}
    portfolio_value = 0.0
    for row in score_rows:
        h = holdings.get(row["ticker"])
        if h:
            portfolio_value += (h["shares"] or 0) * (row.get("current_price") or 0)
    if portfolio_value <= 0:
        portfolio_value = 1.0

    out: dict[str, HoldingSnapshot] = {}
    for row in score_rows:
        h = holdings.get(row["ticker"])
        if not h:
            continue
        current_val = (h["shares"] or 0) * (row.get("current_price") or 0)
        out[row["ticker"]] = HoldingSnapshot(
            ticker=row["ticker"],
            shares=h.get("shares") or 0,
            cost_base=h.get("cost_base") or 0,
            acquired_at=h.get("acquired_at"),
            target_weight=h.get("target_weight") or 0,
            current_weight=current_val / portfolio_value,
        )
    return out
