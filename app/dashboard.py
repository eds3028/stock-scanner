"""
ASX Stock Scanner Dashboard
"""

import sqlite3
import json
import os
import time
from datetime import date
from pathlib import Path

import streamlit as st
import pandas as pd
import plotly.graph_objects as go

DB_PATH = Path("/data/stocks.db")

st.set_page_config(
    page_title="ASX Scanner",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=DM+Sans:wght@300;400;500;600&display=swap');
    html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
    .block-container { padding-top: 1.5rem; }
    .metric-card {
        background: #161b22; border: 1px solid #30363d;
        border-radius: 8px; padding: 16px 20px; margin-bottom: 8px;
    }
    .ticker-badge {
        font-family: 'DM Mono', monospace; font-size: 0.75rem;
        background: #1f6feb22; color: #58a6ff;
        border: 1px solid #1f6feb44; border-radius: 4px;
        padding: 2px 8px; display: inline-block;
    }
    .provider-badge {
        font-family: 'DM Mono', monospace; font-size: 0.65rem;
        background: #21262d; color: #8b949e;
        border: 1px solid #30363d; border-radius: 3px;
        padding: 1px 6px; display: inline-block; margin-left: 4px;
    }
    div[data-testid="stMetricValue"] { font-family: 'DM Mono', monospace; }
    .section-label {
        font-size: 0.7rem; font-weight: 600; letter-spacing: 0.1em;
        text-transform: uppercase; color: #8b949e; margin-bottom: 8px;
    }
    .up-arrow { color: #3fb950; }
    .down-arrow { color: #f85149; }
</style>
""", unsafe_allow_html=True)


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
               health_score, dividend_score, dimension_detail,
               data_provider, data_completeness, data_fetched_at
        FROM scores
        WHERE scan_date = ?
        AND total_score >= ? AND value_score >= ? AND future_score >= ?
        AND past_score >= ? AND health_score >= ? AND dividend_score >= ?
    """
    params = [scan_date, filters["min_total"], filters["min_value"],
              filters["min_future"], filters["min_past"],
              filters["min_health"], filters["min_dividend"]]

    if filters.get("sector") and filters["sector"] != "All":
        query += " AND sector = ?"
        params.append(filters["sector"])

    query += " ORDER BY total_score DESC, health_score DESC"
    rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]


def get_movers(conn, today, yesterday):
    if not yesterday:
        return [], []
    up = conn.execute("""
        SELECT t.ticker, t.company_name, t.sector,
            t.total_score as today_score, y.total_score as yesterday_score,
            (t.total_score - y.total_score) as change,
            t.current_price, t.market_cap, t.data_provider, t.data_fetched_at
        FROM scores t JOIN scores y ON t.ticker = y.ticker
        WHERE t.scan_date = ? AND y.scan_date = ?
        AND t.total_score - y.total_score >= 2
        ORDER BY change DESC LIMIT 10
    """, (today, yesterday)).fetchall()
    down = conn.execute("""
        SELECT t.ticker, t.company_name, t.sector,
            t.total_score as today_score, y.total_score as yesterday_score,
            (t.total_score - y.total_score) as change,
            t.current_price, t.market_cap, t.data_provider, t.data_fetched_at
        FROM scores t JOIN scores y ON t.ticker = y.ticker
        WHERE t.scan_date = ? AND y.scan_date = ?
        AND y.total_score - t.total_score >= 2
        ORDER BY change ASC LIMIT 10
    """, (today, yesterday)).fetchall()
    return [dict(r) for r in up], [dict(r) for r in down]


def get_sectors(conn, scan_date):
    rows = conn.execute(
        "SELECT DISTINCT sector FROM scores WHERE scan_date = ? AND sector != '' ORDER BY sector",
        (scan_date,)
    ).fetchall()
    return ["All"] + [r["sector"] for r in rows]


def get_stock_history(conn, ticker):
    rows = conn.execute("""
        SELECT scan_date, total_score, value_score, future_score,
               past_score, health_score, dividend_score, current_price,
               data_provider, data_completeness
        FROM scores WHERE ticker = ? ORDER BY scan_date ASC
    """, (ticker,)).fetchall()
    return [dict(r) for r in rows]


def get_scan_log(conn):
    try:
        rows = conn.execute(
            "SELECT * FROM scan_log ORDER BY scan_date DESC LIMIT 5"
        ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []


def get_fetch_log(conn, limit=40):
    try:
        rows = conn.execute("""
            SELECT ticker, provider, success, completeness, reason, fetched_at
            FROM fetch_log ORDER BY fetched_at DESC LIMIT ?
        """, (limit,)).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []


def get_cache_stats(conn):
    try:
        total = conn.execute("SELECT COUNT(*) as c FROM data_cache").fetchone()["c"]
        stale = conn.execute(
            "SELECT COUNT(*) as c FROM data_cache WHERE fetched_at < ?",
            (time.time() - 86400,)
        ).fetchone()["c"]
        return {"total": total, "stale": stale, "fresh": total - stale}
    except Exception:
        return {"total": 0, "stale": 0, "fresh": 0}


def score_color(score, max_score=6):
    pct = score / max_score
    if pct >= 0.75:
        return "#3fb950"
    elif pct >= 0.50:
        return "#d29922"
    return "#f85149"


def total_color(score):
    if score >= 20:
        return "#3fb950"
    elif score >= 14:
        return "#d29922"
    return "#f85149"


def make_radar(scores, ticker):
    dims = ["Value", "Future", "Past", "Health", "Dividends"]
    values = [
        scores.get("value_score", 0),
        scores.get("future_score", 0),
        scores.get("past_score", 0),
        scores.get("health_score", 0),
        scores.get("dividend_score", 0),
    ]
    values_closed = values + [values[0]]
    dims_closed = dims + [dims[0]]
    color = total_color(sum(values))
    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(
        r=values_closed,
        theta=dims_closed,
        fill='toself',
        fillcolor=color,
        opacity=0.2,
        line=dict(color=color, width=2),
        name=ticker,
    ))
    fig.update_layout(
        polar=dict(
            bgcolor="#161b22",
            radialaxis=dict(
                visible=True, range=[0, 6], tickvals=[1, 2, 3, 4, 5, 6],
                tickfont=dict(color="#8b949e", size=10),
                gridcolor="#30363d", linecolor="#30363d"
            ),
            angularaxis=dict(
                tickfont=dict(color="#c9d1d9", size=12),
                gridcolor="#30363d", linecolor="#30363d"
            )
        ),
        paper_bgcolor="#0d1117",
        plot_bgcolor="#0d1117",
        font=dict(color="#c9d1d9"),
        margin=dict(l=40, r=40, t=40, b=40),
        height=300,
        showlegend=False,
    )
    return fig


def make_history_chart(history):
    if len(history) < 2:
        return None
    df = pd.DataFrame(history)
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["scan_date"], y=df["total_score"],
        mode="lines+markers",
        line=dict(color="#58a6ff", width=2),
        marker=dict(size=6, color="#58a6ff"),
        name="Total Score",
    ))
    fig.update_layout(
        paper_bgcolor="#0d1117", plot_bgcolor="#161b22",
        font=dict(color="#c9d1d9"),
        xaxis=dict(gridcolor="#30363d", linecolor="#30363d"),
        yaxis=dict(gridcolor="#30363d", linecolor="#30363d", range=[0, 30]),
        margin=dict(l=10, r=10, t=10, b=10),
        height=180,
        showlegend=False,
    )
    return fig


def fmt_market_cap(val):
    if not val:
        return "N/A"
    if val >= 1e12:
        return f"${val/1e12:.1f}T"
    if val >= 1e9:
        return f"${val/1e9:.1f}B"
    if val >= 1e6:
        return f"${val/1e6:.1f}M"
    return f"${val:,.0f}"


def dim_bar(score, max_score=6):
    filled = "█" * score
    empty = "░" * (max_score - score)
    color = score_color(score, max_score)
    return (
        f'<span style="color:{color};font-family:monospace">{filled}{empty}</span> '
        f'<span style="color:#8b949e;font-size:0.8rem">{score}/{max_score}</span>'
    )


def main():
    st.write("Loading...")

    conn = get_db()

    if conn is None:
        st.title("📈 ASX Scanner")
        st.warning("No data yet. The first scan hasn't run.")
        return

    today = get_latest_date(conn)

    if not today:
        st.title("📈 ASX Scanner")
        st.info("Scan in progress - no stocks scored yet. Check back in a few minutes.")
        st.caption(f"Database found at {DB_PATH}")
        conn.close()
        return

    yesterday = get_previous_date(conn, today)

    st.write(f"Found data for {today}")

    # Sidebar
    with st.sidebar:
        st.markdown("## 🔬 ASX Scanner")
        st.markdown(f'<div class="section-label">Last scan</div>', unsafe_allow_html=True)
        st.markdown(f"**{today}**")
        if st.button("🔄 Refresh"):
            st.rerun()

        st.divider()
        st.markdown('<div class="section-label">Filters</div>', unsafe_allow_html=True)

        sectors = get_sectors(conn, today)
        selected_sector = st.selectbox("Sector", sectors)
        min_total = st.slider("Min total score", 0, 30, 0)

        col1, col2 = st.columns(2)
        with col1:
            min_value = st.slider("Value", 0, 6, 0)
            min_future = st.slider("Future", 0, 6, 0)
            min_past = st.slider("Past", 0, 6, 0)
        with col2:
            min_health = st.slider("Health", 0, 6, 0)
            min_dividend = st.slider("Divs", 0, 6, 0)

        filters = {
            "min_total": min_total, "min_value": min_value,
            "min_future": min_future, "min_past": min_past,
            "min_health": min_health, "min_dividend": min_dividend,
            "sector": selected_sector,
        }

    # Tabs
    tab_discover, tab_movers, tab_detail, tab_health = st.tabs([
        "🔍 Discover", "📊 Movers", "🔎 Deep Dive", "🩺 Data Health"
    ])

    # DISCOVER
    with tab_discover:
        stocks = get_all_scores(conn, today, filters)
        st.markdown(f"### {len(stocks)} stocks scored so far")

        if not stocks:
            st.info("No stocks match the current filters. Try lowering the minimum score.")
        else:
            c1, c2, c3, c4 = st.columns(4)
            scores_list = [s["total_score"] for s in stocks]
            c1.metric("Stocks shown", len(stocks))
            c2.metric("Avg score", f"{sum(scores_list)/len(scores_list):.1f}/30")
            c3.metric("Top score", f"{max(scores_list)}/30")
            top = sorted(stocks, key=lambda x: x["health_score"], reverse=True)[0]
            c4.metric("Healthiest", top["ticker"].replace(".AX", ""))

            st.divider()

            for stock in stocks:
                provider = stock.get("data_provider", "")
                completeness = stock.get("data_completeness", 0) or 0

                with st.expander(
                    f"**{stock['ticker'].replace('.AX','')}** · {stock['company_name']} · Score {stock['total_score']}/30",
                    expanded=False
                ):
                    col_left, col_right = st.columns([2, 1])
                    with col_left:
                        st.markdown(
                            f'<span class="ticker-badge">{stock["ticker"]}</span>'
                            f' &nbsp; {stock.get("sector","") or ""} › {stock.get("industry","") or ""}'
                            f' <span class="provider-badge">{provider.split(" ")[0][:20] if provider else ""}</span>',
                            unsafe_allow_html=True
                        )

                        m1, m2, m3, m4 = st.columns(4)
                        m1.metric("Price", f"${stock['current_price']:.2f}" if stock['current_price'] else "N/A")
                        m2.metric("Market Cap", fmt_market_cap(stock['market_cap']))
                        m3.metric("Score", f"{stock['total_score']}/30")
                        m4.metric("Data", f"{completeness*100:.0f}%")

                        st.markdown("**Scores**")
                        for dim_name, key in [
                            ("Value", "value_score"), ("Future", "future_score"),
                            ("Past", "past_score"), ("Health", "health_score"),
                            ("Dividends", "dividend_score")
                        ]:
                            st.markdown(
                                f'<div style="margin-bottom:4px">'
                                f'<span style="display:inline-block;width:70px;color:#8b949e;font-size:0.85rem">{dim_name}</span>'
                                f' {dim_bar(stock[key])}</div>',
                                unsafe_allow_html=True
                            )

                    with col_right:
                        st.plotly_chart(
                            make_radar(stock, stock["ticker"]),
                            use_container_width=True,
                            key=f"radar_{stock['ticker']}"
                        )

                    history = get_stock_history(conn, stock["ticker"])
                    if len(history) > 1:
                        fig = make_history_chart(history)
                        if fig:
                            st.plotly_chart(fig, use_container_width=True, key=f"hist_{stock['ticker']}")

    # MOVERS
    with tab_movers:
        st.markdown("### Stocks that moved since last scan")
        if not yesterday:
            st.info("Need at least two scans to show movers.")
        else:
            movers_up, movers_down = get_movers(conn, today, yesterday)
            col_up, col_down = st.columns(2)
            with col_up:
                st.markdown("#### ⬆️ Improving")
                if not movers_up:
                    st.info("No significant improvements today")
                for m in movers_up:
                    ticker = m["ticker"].replace(".AX", "")
                    st.markdown(f"""
                    <div class="metric-card">
                        <span class="ticker-badge">{ticker}</span>
                        <span class="up-arrow" style="float:right">+{m['change']} ▲</span>
                        <div style="color:#c9d1d9;margin-top:6px">{m['company_name']}</div>
                        <div style="color:#8b949e;font-size:0.8rem">{m['today_score']}/30 (was {m['yesterday_score']})</div>
                    </div>
                    """, unsafe_allow_html=True)
            with col_down:
                st.markdown("#### ⬇️ Declining")
                if not movers_down:
                    st.info("No significant declines today")
                for m in movers_down:
                    ticker = m["ticker"].replace(".AX", "")
                    st.markdown(f"""
                    <div class="metric-card">
                        <span class="ticker-badge">{ticker}</span>
                        <span class="down-arrow" style="float:right">{m['change']} ▼</span>
                        <div style="color:#c9d1d9;margin-top:6px">{m['company_name']}</div>
                        <div style="color:#8b949e;font-size:0.8rem">{m['today_score']}/30 (was {m['yesterday_score']})</div>
                    </div>
                    """, unsafe_allow_html=True)

    # DEEP DIVE
    with tab_detail:
        st.markdown("### Deep dive on a specific stock")

        conn2 = get_db()
        all_tickers = conn2.execute(
            "SELECT DISTINCT ticker, company_name FROM scores WHERE scan_date = ? ORDER BY ticker",
            (today,)
        ).fetchall()

        if not all_tickers:
            st.info("No stocks scored yet.")
            conn2.close()
        else:
            ticker_options = {
                f"{r['ticker'].replace('.AX','')} - {r['company_name']}": r['ticker']
                for r in all_tickers
            }
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
                    st.markdown(f"*{row.get('sector','') or ''}* › *{row.get('industry','') or ''}*")

                    provider = row.get("data_provider", "unknown")
                    fetched_at = row.get("data_fetched_at")
                    completeness = row.get("data_completeness", 0) or 0
                    if fetched_at:
                        age_hours = (time.time() - fetched_at) / 3600
                        age_str = f"{age_hours:.0f}h ago" if age_hours >= 1 else "just now"
                        st.caption(f"📡 {provider} · {age_str} · {completeness*100:.0f}% complete")

                    m1, m2 = st.columns(2)
                    m1.metric("Total Score", f"{row['total_score']}/30")
                    price = row.get('current_price')
                    m2.metric("Price", f"${price:.2f}" if price else "N/A")
                    st.metric("Market Cap", fmt_market_cap(row.get('market_cap')))

                with col2:
                    st.plotly_chart(
                        make_radar(row, selected_ticker),
                        use_container_width=True,
                        key="detail_radar"
                    )

                st.divider()

                for key, label, description in [
                    ("value", "💰 Value", "Is the stock priced attractively?"),
                    ("future", "🚀 Future", "Is growth expected?"),
                    ("past", "📈 Past", "Has it performed historically?"),
                    ("health", "🏥 Health", "Is the balance sheet sound?"),
                    ("dividends", "💵 Dividends", "Is income reliable?"),
                ]:
                    dim = dims.get(key, {})
                    dim_score = dim.get("score", 0)
                    checks = dim.get("checks", {})
                    data = dim.get("data", {})

                    with st.expander(f"{label} · {dim_score}/6 · {description}", expanded=True):
                        check_cols = st.columns(3)
                        for i, (check_name, passed) in enumerate(checks.items()):
                            icon = "✅" if passed else "❌"
                            check_cols[i % 3].markdown(f"{icon} {check_name.replace('_',' ').title()}")

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

                st.divider()
                st.markdown("### Score history")
                history = get_stock_history(conn2, selected_ticker)
                if len(history) > 1:
                    fig = make_history_chart(history)
                    if fig:
                        st.plotly_chart(fig, use_container_width=True, key="detail_history")
                else:
                    st.info("Score history builds up over time.")

                st.divider()
                st.markdown("### Narrative")
                narrative = row.get("narrative")
                if narrative:
                    st.markdown(f"""
                    <div style="background:#161b22;border:1px solid #30363d;
                    border-left:3px solid #58a6ff;border-radius:8px;
                    padding:20px 24px;line-height:1.8;color:#c9d1d9;">
                    {narrative.replace(chr(10), '<br><br>')}
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    st.info("Narrative will be generated on next scan.")

            conn2.close()

    # HEALTH
    with tab_health:
        st.markdown("### 🩺 Data Pipeline Health")

        # Provider status
        st.markdown("#### Provider Status")
        providers_info = [
            ("yahooquery", None, "Yahoo Finance - free, no key"),
            ("finnhub", "FINNHUB_API_KEY", "Finnhub - 60 calls/min"),
            ("fmp", "FMP_API_KEY", "FMP - 250 calls/day"),
            ("alpha_vantage", "ALPHA_VANTAGE_API_KEY", "Alpha Vantage - 25 calls/day"),
        ]

        try:
            conn_h = get_db()
            health_rows = conn_h.execute("""
                SELECT provider,
                    COUNT(*) as total,
                    SUM(success) as successes
                FROM fetch_log
                WHERE fetched_at > ?
                GROUP BY provider
            """, (time.time() - 86400,)).fetchall()
            health_lookup = {r["provider"]: dict(r) for r in health_rows}
            conn_h.close()
        except Exception:
            health_lookup = {}

        cols = st.columns(4)
        for i, (name, key_env, desc) in enumerate(providers_info):
            with cols[i]:
                configured = key_env is None or bool(os.environ.get(key_env, ""))
                h = health_lookup.get(name, {})

                if not configured:
                    dot, status_text, status_color = "⚫", "No key", "#484f58"
                elif h:
                    rate = h.get("successes", 0) / max(h.get("total", 1), 1)
                    if rate >= 0.8:
                        dot, status_text, status_color = "🟢", "Healthy", "#3fb950"
                    elif rate >= 0.5:
                        dot, status_text, status_color = "🟡", "Degraded", "#d29922"
                    else:
                        dot, status_text, status_color = "🔴", "Failing", "#f85149"
                else:
                    dot, status_text, status_color = "⚪", "No data yet", "#8b949e"

                total_calls = h.get("total", 0)
                success_calls = h.get("successes", 0)
                rate_str = ""
                if total_calls:
                    rate_pct = int(success_calls / total_calls * 100)
                    rate_str = f"{total_calls} calls · {rate_pct}% success"

                st.markdown(f"""
                <div style="background:#161b22;border:1px solid #30363d;
                border-radius:8px;padding:14px;text-align:center;">
                    <div style="font-size:1.4rem">{dot}</div>
                    <div style="font-family:'DM Mono',monospace;font-size:0.8rem;
                    color:#c9d1d9;margin-top:4px">{name}</div>
                    <div style="color:{status_color};font-size:0.75rem;margin-top:2px">{status_text}</div>
                    <div style="color:#8b949e;font-size:0.7rem;margin-top:4px">{rate_str}</div>
                </div>
                """, unsafe_allow_html=True)

        st.divider()

        # Cache stats
        st.markdown("#### Cache Status")
        cache = get_cache_stats(conn)
        cc1, cc2, cc3 = st.columns(3)
        cc1.metric("Cached stocks", cache["total"])
        cc2.metric("Fresh (< 24h)", cache["fresh"])
        cc3.metric("Stale (> 24h)", cache["stale"])

        st.divider()

        # Fetch log
        st.markdown("#### Recent Fetch Activity")
        fetch_log = get_fetch_log(conn, limit=40)
        if fetch_log:
            log_data = []
            for entry in fetch_log:
                age = (time.time() - entry["fetched_at"]) / 60
                log_data.append({
                    "Time": f"{age:.0f}m ago",
                    "Ticker": entry["ticker"],
                    "Provider": entry["provider"],
                    "Result": "✅" if entry["success"] else "❌",
                    "Complete": f"{(entry['completeness'] or 0)*100:.0f}%",
                    "Note": (entry.get("reason") or "")[:40],
                })
            st.dataframe(pd.DataFrame(log_data), use_container_width=True, hide_index=True)
        else:
            st.info("No fetch activity yet.")

        st.divider()

        # Scan history
        st.markdown("#### Scan History")
        scan_log = get_scan_log(conn)
        if scan_log:
            scan_data = []
            for s in scan_log:
                providers_str = ""
                if s.get("provider_summary"):
                    try:
                        ps = json.loads(s["provider_summary"])
                        counts = ps.get("counts", {})
                        providers_str = ", ".join(f"{k}: {v}" for k, v in counts.items())
                    except Exception:
                        pass
                scan_data.append({
                    "Date": s["scan_date"],
                    "Scanned": s["stocks_scanned"],
                    "Failed": s["stocks_failed"],
                    "Providers": providers_str,
                })
            st.dataframe(pd.DataFrame(scan_data), use_container_width=True, hide_index=True)
        else:
            st.info("No completed scans yet - scan still running.")

    conn.close()


if __name__ == "__main__":
    main()
