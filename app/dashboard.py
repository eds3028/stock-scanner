"""
ASX Stock Scanner Dashboard
Streamlit frontend for browsing scored stocks.
"""

import sqlite3
import json
import math
import os
from datetime import date, timedelta
from pathlib import Path

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

DB_PATH = Path("/data/stocks.db")

# --- Page config ---
st.set_page_config(
    page_title="ASX Scanner",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Styling ---
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=DM+Sans:wght@300;400;500;600&display=swap');

    html, body, [class*="css"] {
        font-family: 'DM Sans', sans-serif;
    }

    .main { background-color: #0d1117; }
    .block-container { padding-top: 1.5rem; padding-bottom: 1rem; }

    .metric-card {
        background: #161b22;
        border: 1px solid #30363d;
        border-radius: 8px;
        padding: 16px 20px;
        margin-bottom: 8px;
    }

    .ticker-badge {
        font-family: 'DM Mono', monospace;
        font-size: 0.75rem;
        background: #1f6feb22;
        color: #58a6ff;
        border: 1px solid #1f6feb44;
        border-radius: 4px;
        padding: 2px 8px;
        display: inline-block;
    }

    .score-pill {
        font-family: 'DM Mono', monospace;
        font-weight: 500;
        font-size: 0.85rem;
        border-radius: 20px;
        padding: 3px 12px;
        display: inline-block;
    }

    .up-arrow { color: #3fb950; font-size: 1.1rem; }
    .down-arrow { color: #f85149; font-size: 1.1rem; }

    h1 { font-family: 'DM Sans', sans-serif; font-weight: 600; color: #e6edf3; }
    h2, h3 { font-family: 'DM Sans', sans-serif; font-weight: 500; color: #c9d1d9; }

    .stSelectbox > div > div { background: #161b22; border-color: #30363d; }
    .stSlider { color: #58a6ff; }

    div[data-testid="stMetricValue"] { font-family: 'DM Mono', monospace; }

    .section-label {
        font-size: 0.7rem;
        font-weight: 600;
        letter-spacing: 0.1em;
        text-transform: uppercase;
        color: #8b949e;
        margin-bottom: 8px;
    }
</style>
""", unsafe_allow_html=True)


# --- DB helpers ---

def get_db():
    if not DB_PATH.exists():
        return None
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_latest_date(conn):
    row = conn.execute("SELECT MAX(scan_date) as d FROM scores").fetchone()
    return row["d"] if row else None


def get_previous_date(conn, today):
    row = conn.execute(
        "SELECT MAX(scan_date) as d FROM scores WHERE scan_date < ?", (today,)
    ).fetchone()
    return row["d"] if row else None


def get_all_scores(conn, scan_date, filters):
    query = """
        SELECT ticker, company_name, sector, industry, market_cap, current_price,
               total_score, value_score, future_score, past_score,
               health_score, dividend_score, dimension_detail
        FROM scores
        WHERE scan_date = ?
        AND total_score >= ?
        AND value_score >= ?
        AND future_score >= ?
        AND past_score >= ?
        AND health_score >= ?
        AND dividend_score >= ?
    """
    params = [
        scan_date,
        filters["min_total"],
        filters["min_value"],
        filters["min_future"],
        filters["min_past"],
        filters["min_health"],
        filters["min_dividend"],
    ]

    if filters.get("sector") and filters["sector"] != "All":
        query += " AND sector = ?"
        params.append(filters["sector"])

    query += " ORDER BY total_score DESC, health_score DESC"

    rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]


def get_movers(conn, today, yesterday):
    if not yesterday:
        return [], []
    rows = conn.execute("""
        SELECT
            t.ticker, t.company_name, t.sector,
            t.total_score as today_score,
            y.total_score as yesterday_score,
            (t.total_score - y.total_score) as change,
            t.value_score, t.future_score, t.past_score,
            t.health_score, t.dividend_score,
            t.current_price, t.market_cap
        FROM scores t
        JOIN scores y ON t.ticker = y.ticker
        WHERE t.scan_date = ? AND y.scan_date = ?
        AND t.total_score - y.total_score >= 2
        ORDER BY change DESC
        LIMIT 10
    """, (today, yesterday)).fetchall()
    movers_up = [dict(r) for r in rows]

    rows = conn.execute("""
        SELECT
            t.ticker, t.company_name, t.sector,
            t.total_score as today_score,
            y.total_score as yesterday_score,
            (t.total_score - y.total_score) as change,
            t.value_score, t.future_score, t.past_score,
            t.health_score, t.dividend_score,
            t.current_price, t.market_cap
        FROM scores t
        JOIN scores y ON t.ticker = y.ticker
        WHERE t.scan_date = ? AND y.scan_date = ?
        AND y.total_score - t.total_score >= 2
        ORDER BY change ASC
        LIMIT 10
    """, (today, yesterday)).fetchall()
    movers_down = [dict(r) for r in rows]

    return movers_up, movers_down


def get_scan_log(conn):
    rows = conn.execute(
        "SELECT * FROM scan_log ORDER BY scan_date DESC LIMIT 5"
    ).fetchall()
    return [dict(r) for r in rows]


def get_sectors(conn, scan_date):
    rows = conn.execute(
        "SELECT DISTINCT sector FROM scores WHERE scan_date = ? AND sector != '' ORDER BY sector",
        (scan_date,)
    ).fetchall()
    return ["All"] + [r["sector"] for r in rows]


def get_stock_history(conn, ticker):
    rows = conn.execute("""
        SELECT scan_date, total_score, value_score, future_score,
               past_score, health_score, dividend_score, current_price
        FROM scores WHERE ticker = ?
        ORDER BY scan_date ASC
    """, (ticker,)).fetchall()
    return [dict(r) for r in rows]


# --- Visualisation helpers ---

def score_color(score, max_score=6):
    pct = score / max_score
    if pct >= 0.75:
< truncated lines 219-397 >

        st.markdown(f"### {len(stocks)} stocks match your filters")

        if not stocks:
            st.info("No stocks match the current filters. Try lowering the minimum scores.")
        else:
            # Summary stats
            c1, c2, c3, c4 = st.columns(4)
            scores_list = [s["total_score"] for s in stocks]
            c1.metric("Stocks shown", len(stocks))
            c2.metric("Avg score", f"{sum(scores_list)/len(scores_list):.1f}/30")
            c3.metric("Top score", f"{max(scores_list)}/30")
            top_health = sorted(stocks, key=lambda x: x["health_score"], reverse=True)[0]
            c4.metric("Healthiest", top_health["ticker"].replace(".AX", ""))

            st.divider()

            for stock in stocks:
                with st.expander(
                    f"**{stock['ticker'].replace('.AX','')}** · {stock['company_name']} · Score {stock['total_score']}/30",
                    expanded=False
                ):
                    col_left, col_right = st.columns([2, 1])

                    with col_left:
                        st.markdown(f'<span class="ticker-badge">{stock["ticker"]}</span> &nbsp; {stock.get("sector","") or ""} › {stock.get("industry","") or ""}', unsafe_allow_html=True)

                        m1, m2, m3 = st.columns(3)
                        m1.metric("Price", f"${stock['current_price']:.2f}" if stock['current_price'] else "N/A")
                        m2.metric("Market Cap", fmt_market_cap(stock['market_cap']))
                        m3.metric("Total Score", f"{stock['total_score']}/30")

                        st.markdown("**Dimension scores**")
                        dims = [
                            ("Value", stock["value_score"]),
                            ("Future", stock["future_score"]),
                            ("Past", stock["past_score"]),
                            ("Health", stock["health_score"]),
                            ("Dividends", stock["dividend_score"]),
                        ]
                        for name, score in dims:
                            st.markdown(
                                f'<div style="margin-bottom:4px"><span style="display:inline-block;width:70px;color:#8b949e;font-size:0.85rem">{name}</span> {dim_bar(score)}</div>',
                                unsafe_allow_html=True
                            )

                    with col_right:
                        st.plotly_chart(make_radar(stock, stock["ticker"]), use_container_width=True, key=f"radar_{stock['ticker']}")

                    # Score history
                    history = get_stock_history(conn, stock["ticker"])
                    if len(history) > 1:
                        st.markdown("**Score history**")
                        fig = make_history_chart(history)
                        if fig:
                            st.plotly_chart(fig, use_container_width=True, key=f"hist_{stock['ticker']}")

    # --- MOVERS TAB ---
    with tab_movers:
        st.markdown("### Stocks that moved since last scan")

        if not yesterday:
            st.info("Need at least two scans to show movers. Come back tomorrow!")
        else:
            movers_up, movers_down = get_movers(conn, today, yesterday)

            col_up, col_down = st.columns(2)

            with col_up:
                st.markdown("#### ⬆️ Improving")
                if not movers_up:
                    st.info("No significant improvements today")
                for m in movers_up:
                    change = m["change"]
                    ticker = m["ticker"].replace(".AX", "")
                    st.markdown(f"""
                    <div class="metric-card">
                        <span class="ticker-badge">{ticker}</span>
                        <span class="up-arrow" style="float:right">+{change} ▲</span>
                        <div style="color:#c9d1d9;margin-top:6px;font-size:0.9rem">{m['company_name']}</div>
                        <div style="color:#8b949e;font-size:0.8rem;margin-top:2px">{m['today_score']}/30 (was {m['yesterday_score']})</div>
                    </div>
                    """, unsafe_allow_html=True)

            with col_down:
                st.markdown("#### ⬇️ Declining")
                if not movers_down:
                    st.info("No significant declines today")
                for m in movers_down:
                    change = m["change"]
                    ticker = m["ticker"].replace(".AX", "")
                    st.markdown(f"""
                    <div class="metric-card">
                        <span class="ticker-badge">{ticker}</span>
                        <span class="down-arrow" style="float:right">{change} ▼</span>
                        <div style="color:#c9d1d9;margin-top:6px;font-size:0.9rem">{m['company_name']}</div>
                        <div style="color:#8b949e;font-size:0.8rem;margin-top:2px">{m['today_score']}/30 (was {m['yesterday_score']})</div>
                    </div>
                    """, unsafe_allow_html=True)

    # --- DEEP DIVE TAB ---
    with tab_detail:
        st.markdown("### Deep dive on a specific stock")

        conn2 = get_db()
        all_tickers = conn2.execute(
            "SELECT DISTINCT ticker, company_name FROM scores WHERE scan_date = ? ORDER BY ticker",
            (today,)
        ).fetchall()
        ticker_options = {f"{r['ticker'].replace('.AX','')} - {r['company_name']}": r['ticker'] for r in all_tickers}

        selected_label = st.selectbox("Choose a stock", list(ticker_options.keys()))
        selected_ticker = ticker_options[selected_label]

        row = conn2.execute(
            "SELECT * FROM scores WHERE ticker = ? AND scan_date = ?",
            (selected_ticker, today)
        ).fetchone()

        if row:
            row = dict(row)
            dims = json.loads(row["dimension_detail"])

            col1, col2 = st.columns([1, 1])

            with col1:
                st.markdown(f"## {selected_ticker.replace('.AX', '')}")
                st.markdown(f"**{row['company_name']}**")
                st.markdown(f"*{row['sector']}* › *{row['industry']}*")

                m1, m2 = st.columns(2)
                m1.metric("Total Score", f"{row['total_score']}/30")
                m2.metric("Price", f"${row['current_price']:.2f}" if row['current_price'] else "N/A")
                st.metric("Market Cap", fmt_market_cap(row['market_cap']))

            with col2:
                st.plotly_chart(make_radar(row, selected_ticker), use_container_width=True, key="detail_radar")

            st.divider()

            # Dimension detail cards
            dim_map = [
                ("value", "💰 Value", "Is the stock priced attractively?"),
                ("future", "🚀 Future", "Is growth expected?"),
                ("past", "📈 Past", "Has it performed historically?"),
                ("health", "🏥 Health", "Is the balance sheet sound?"),
                ("dividends", "💵 Dividends", "Is income reliable?"),
            ]

            for key, label, description in dim_map:
                dim = dims.get(key, {})
                dim_score = dim.get("score", 0)
                checks = dim.get("checks", {})
                data = dim.get("data", {})

                with st.expander(f"{label} · {dim_score}/6 · {description}", expanded=True):
                    check_cols = st.columns(3)
                    for i, (check_name, passed) in enumerate(checks.items()):
                        icon = "✅" if passed else "❌"
                        label_clean = check_name.replace("_", " ").title()
                        check_cols[i % 3].markdown(f"{icon} {label_clean}")

                    if data:
                        st.markdown("**Key metrics**")
                        data_items = [(k, v) for k, v in data.items() if v is not None]
                        d_cols = st.columns(4)
                        for i, (k, v) in enumerate(data_items):
                            label_clean = k.replace("_", " ").title()
                            if isinstance(v, float):
                                display = f"{v:,.2f}"
                            elif isinstance(v, int) and abs(v) > 1000000:
                                display = fmt_market_cap(v)
                            else:
                                display = str(v)
                            d_cols[i % 4].metric(label_clean, display)

            # History
            st.divider()
            st.markdown("### Score history")
            history = get_stock_history(conn2, selected_ticker)
            if len(history) > 1:
                fig = make_history_chart(history)
                if fig:
                    st.plotly_chart(fig, use_container_width=True, key="detail_history")
            else:
                st.info("Score history builds up over time as daily scans accumulate.")

            # Narrative
            st.divider()
            st.markdown("### Narrative")
            narrative = row.get("narrative")
            if narrative:
                st.markdown(f"""
                <div style="
                    background:#161b22;
                    border:1px solid #30363d;
                    border-left: 3px solid #58a6ff;
                    border-radius:8px;
                    padding:20px 24px;
                    line-height:1.8;
                    color:#c9d1d9;
                    font-size:0.95rem;
                ">
                {narrative.replace(chr(10), '<br><br>')}
                </div>
                """, unsafe_allow_html=True)
                st.caption(f"Generated by {os.environ.get('OLLAMA_MODEL','llama3.1:8b')} via Ollama · {row.get('scan_date','')}")
            else:
                st.info("No narrative yet. It will be generated on the next nightly scan if Ollama is reachable.")

        conn2.close()

    conn.close()


if __name__ == "__main__":
    main()
