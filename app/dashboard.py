"""
ASX Stock Scanner Dashboard - with provider health monitoring.
"""

import sqlite3
import json
import os
import time
from datetime import date, datetime
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
    .main { background-color: #0d1117; }
    .block-container { padding-top: 1.5rem; padding-bottom: 1rem; }
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
    .stale-badge {
        font-family: 'DM Mono', monospace; font-size: 0.65rem;
        background: #3d2009; color: #d29922;
        border: 1px solid #d2992244; border-radius: 3px;
        padding: 1px 6px; display: inline-block; margin-left: 4px;
    }
    .health-healthy { color: #3fb950; }
    .health-degraded { color: #d29922; }
    .health-circuit_open { color: #f85149; }
    .health-unavailable { color: #484f58; }
    div[data-testid="stMetricValue"] { font-family: 'DM Mono', monospace; }
    .section-label {
        font-size: 0.7rem; font-weight: 600; letter-spacing: 0.1em;
        text-transform: uppercase; color: #8b949e; margin-bottom: 8px;
    }
    .up-arrow { color: #3fb950; }
    .down-arrow { color: #f85149; }
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
            t.value_score, t.future_score, t.past_score,
            t.health_score, t.dividend_score,
            t.current_price, t.market_cap,
            t.data_provider, t.data_fetched_at
        FROM scores t JOIN scores y ON t.ticker = y.ticker
        WHERE t.scan_date = ? AND y.scan_date = ?
        AND t.total_score - y.total_score >= 2
        ORDER BY change DESC LIMIT 10
    """, (today, yesterday)).fetchall()
    down = conn.execute("""
        SELECT t.ticker, t.company_name, t.sector,
            t.total_score as today_score, y.total_score as yesterday_score,
            (t.total_score - y.total_score) as change,
            t.value_score, t.future_score, t.past_score,
            t.health_score, t.dividend_score,
            t.current_price, t.market_cap,
            t.data_provider, t.data_fetched_at
        FROM scores t JOIN scores y ON t.ticker = y.ticker
        WHERE t.scan_date = ? AND y.scan_date = ?
        AND y.total_score - t.total_score >= 2
        ORDER BY change ASC LIMIT 10
    """, (today, yesterday)).fetchall()
    return [dict(r) for r in up], [dict(r) for r in down]


def get_scan_log(conn):
    rows = conn.execute(
        "SELECT * FROM scan_log ORDER BY scan_date DESC LIMIT 5"
    ).fetchall()
    return [dict(r) for r in rows]


def get_fetch_log(conn, limit=30):
    try:
        rows = conn.execute("""
            SELECT ticker, provider, success, completeness, reason, fetched_at
            FROM fetch_log ORDER BY fetched_at DESC LIMIT ?
        """, (limit,)).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []


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


def get_provider_health_from_log(conn) -> list:
    """Derive provider health from fetch_log since orchestrator state is in-memory."""
    try:
        rows = conn.execute("""
            SELECT provider,
                COUNT(*) as total,
                SUM(success) as successes,
                MAX(fetched_at) as last_attempt,
                AVG(completeness) as avg_completeness
            FROM fetch_log
            WHERE fetched_at > ?
            GROUP BY provider
        """, (time.time() - 86400,)).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []


# --- Visualisation helpers ---

def score_color(score, max_score=6):
    pct = score / max_score
    if pct >= 0.75: return "#3fb950"
    elif pct >= 0.50: return "#d29922"
    return "#f85149"


def total_color(score):
    if score >= 20: return "#3fb950"
    elif score >= 14: return "#d29922"
    return "#f85149"


def make_radar(scores, ticker):
    dims = ["Value", "Future", "Past", "Health", "Dividends"]
    values = [scores.get("value_score", 0), scores.get("future_score", 0),
              scores.get("past_score", 0), scores.get("health_score", 0),
              scores.get("dividend_score", 0)]
    values_closed = values + [values[0]]
    dims_closed = dims + [dims[0]]
    color = total_color(sum(values))
    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(
        r=values_closed, theta=dims_closed, fill='toself',
        fillcolor=color, opacity=0.2, line=dict(color=color, width=2), name=ticker,
    ))
    fig.update_layout(
        polar=dict(
            bgcolor="#161b22",
            radialaxis=dict(visible=True, range=[0, 6], tickvals=[1,2,3,4,5,6],
                           tickfont=dict(color="#8b949e", size=10),
                           gridcolor="#30363d", linecolor="#30363d"),
            angularaxis=dict(tickfont=dict(color="#c9d1d9", size=12),
                            gridcolor="#30363d", linecolor="#30363d")
        ),
        paper_bgcolor="#0d1117", plot_bgcolor="#0d1117",
        font=dict(color="#c9d1d9"),
        margin=dict(l=40, r=40, t=40, b=40), height=300, showlegend=False,
    )
    return fig


def make_history_chart(history):
    if len(history) < 2: return None
    df = pd.DataFrame(history)
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["scan_date"], y=df["total_score"],
        mode="lines+markers",
        line=dict(color="#58a6ff", width=2),
        marker=dict(size=6, color="#58a6ff"), name="Total Score",
    ))
    fig.update_layout(
        paper_bgcolor="#0d1117", plot_bgcolor="#161b22",
        font=dict(color="#c9d1d9"),
        xaxis=dict(gridcolor="#30363d", linecolor="#30363d"),
        yaxis=dict(gridcolor="#30363d", linecolor="#30363d", range=[0, 30]),
        margin=dict(l=10, r=10, t=10, b=10), height=180, showlegend=False,
    )
    return fig


def fmt_market_cap(val):
    if not val: return "N/A"
    if val >= 1e12: return f"${val/1e12:.1f}T"
    if val >= 1e9: return f"${val/1e9:.1f}B"
    if val >= 1e6: return f"${val/1e6:.1f}M"
    return f"${val:,.0f}"


def fmt_provider(provider, fetched_at=None):
    if not provider: return ""
    age_hours = (time.time() - fetched_at) / 3600 if fetched_at else 0
    stale = age_hours > 26
    badge_class = "stale-badge" if stale else "provider-badge"
    age_str = f" {age_hours:.0f}h ago" if fetched_at else ""
    short = provider.split(" ")[0][:20]
    return f'<span class="{badge_class}">{short}{age_str}</span>'


def dim_bar(score, max_score=6):
    filled = "█" * score
    empty = "░" * (max_score - score)
    color = score_color(score, max_score)
    return f'<span style="color:{color};font-family:monospace">{filled}{empty}</span> <span style="color:#8b949e;font-size:0.8rem">{score}/{max_score}</span>'


def status_dot(status: str) -> str:
    colors = {
        "healthy": "#3fb950",
        "degraded": "#d29922",
        "circuit_open": "#f85149",
        "unavailable": "#484f58",
    }
    color = colors.get(status, "#484f58")
    return f'<span style="color:{color}">●</span>'


# --- Main app ---

def main():
    conn = get_db()

    if conn is None:
        st.title("📈 ASX Scanner")
        st.warning("No data yet. The first scan hasn't run.")
        return

    today = get_latest_date(conn)
    yesterday = get_previous_date(conn, today)

    if not today:
        st.warning("Database exists but no scan data found yet.")
        return

    # --- Sidebar ---
    with st.sidebar:
        st.markdown("## 🔬 ASX Scanner")
        st.markdown(f'<div class="section-label">Last scan</div>', unsafe_allow_html=True)
        st.markdown(f"**{today}**")
        if st.button("🔄 Refresh data"):
            st.rerun()
            
        st.divider()
        st.markdown('<div class="section-label">Filters</div>', unsafe_allow_html=True)
        sectors = get_sectors(conn, today)
        selected_sector = st.selectbox("Sector", sectors)
        min_total = st.slider("Min total score", 0, 30, 10)
        st.markdown('<div class="section-label">Min dimension scores</div>', unsafe_allow_html=True)
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

        st.divider()
        log_rows = get_scan_log(conn)
        if log_rows:
            st.markdown('<div class="section-label">Recent scans</div>', unsafe_allow_html=True)
            for row in log_rows[:3]:
                ps = ""
                if row.get("provider_summary"):
                    try:
                        ps_data = json.loads(row["provider_summary"])
                        counts = ps_data.get("counts", {})
                        ps = " · ".join(f"{k}: {v}" for k, v in counts.items())
                    except Exception:
                        pass
                st.markdown(
                    f"**{row['scan_date']}** · {row['stocks_scanned']} stocks",
                    help=f"Failed: {row['stocks_failed']}\nProviders: {ps}"
                )

    # --- Tabs ---
    tab_discover, tab_movers, tab_detail, tab_health = st.tabs([
        "🔍 Discover", "📊 Movers", "🔎 Deep Dive", "🩺 Data Health"
    ])

    # --- DISCOVER TAB ---
    with tab_discover:
        stocks = get_all_scores(conn, today, filters)
        st.markdown(f"### {len(stocks)} stocks match your filters")

        if not stocks:
            st.info("No stocks match the current filters.")
        else:
            c1, c2, c3, c4 = st.columns(4)
            scores_list = [s["total_score"] for s in stocks]
            c1.metric("Stocks shown", len(stocks))
            c2.metric("Avg score", f"{sum(scores_list)/len(scores_list):.1f}/30")
            c3.metric("Top score", f"{max(scores_list)}/30")
            top_health = sorted(stocks, key=lambda x: x["health_score"], reverse=True)[0]
            c4.metric("Healthiest", top_health["ticker"].replace(".AX", ""))
            st.divider()

            for stock in stocks:
                provider_html = fmt_provider(
                    stock.get("data_provider"),
                    stock.get("data_fetched_at")
                )
                completeness = stock.get("data_completeness", 0) or 0
                comp_str = f"{completeness*100:.0f}% complete"

                with st.expander(
                    f"**{stock['ticker'].replace('.AX','')}** · {stock['company_name']} · Score {stock['total_score']}/30",
                    expanded=False
                ):
                    col_left, col_right = st.columns([2, 1])
                    with col_left:
                        st.markdown(
                            f'<span class="ticker-badge">{stock["ticker"]}</span> '
                            f'&nbsp; {stock.get("sector","") or ""} › {stock.get("industry","") or ""}'
                            f' {provider_html}',
                            unsafe_allow_html=True
                        )
                        m1, m2, m3, m4 = st.columns(4)
                        m1.metric("Price", f"${stock['current_price']:.2f}" if stock['current_price'] else "N/A")
                        m2.metric("Market Cap", fmt_market_cap(stock['market_cap']))
                        m3.metric("Total Score", f"{stock['total_score']}/30")
                        m4.metric("Data Quality", comp_str)

                        st.markdown("**Dimension scores**")
                        for name, key in [("Value", "value_score"), ("Future", "future_score"),
                                          ("Past", "past_score"), ("Health", "health_score"),
                                          ("Dividends", "dividend_score")]:
                            st.markdown(
                                f'<div style="margin-bottom:4px"><span style="display:inline-block;width:70px;color:#8b949e;font-size:0.85rem">{name}</span> {dim_bar(stock[key])}</div>',
                                unsafe_allow_html=True
                            )
                    with col_right:
                        st.plotly_chart(make_radar(stock, stock["ticker"]),
                                       use_container_width=True, key=f"radar_{stock['ticker']}")

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
            st.info("Need at least two scans to show movers.")
        else:
            movers_up, movers_down = get_movers(conn, today, yesterday)
            col_up, col_down = st.columns(2)
            with col_up:
                st.markdown("#### ⬆️ Improving")
                if not movers_up:
                    st.info("No significant improvements today")
                for m in movers_up:
                    provider_html = fmt_provider(m.get("data_provider"), m.get("data_fetched_at"))
                    ticker = m["ticker"].replace(".AX", "")
                    st.markdown(f"""
                    <div class="metric-card">
                        <span class="ticker-badge">{ticker}</span>
                        {provider_html}
                        <span class="up-arrow" style="float:right">+{m['change']} ▲</span>
                        <div style="color:#c9d1d9;margin-top:6px;font-size:0.9rem">{m['company_name']}</div>
                        <div style="color:#8b949e;font-size:0.8rem;margin-top:2px">{m['today_score']}/30 (was {m['yesterday_score']})</div>
                    </div>
                    """, unsafe_allow_html=True)
            with col_down:
                st.markdown("#### ⬇️ Declining")
                if not movers_down:
                    st.info("No significant declines today")
                for m in movers_down:
                    provider_html = fmt_provider(m.get("data_provider"), m.get("data_fetched_at"))
                    ticker = m["ticker"].replace(".AX", "")
                    st.markdown(f"""
                    <div class="metric-card">
                        <span class="ticker-badge">{ticker}</span>
                        {provider_html}
                        <span class="down-arrow" style="float:right">{m['change']} ▼</span>
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
        ticker_options = {f"{r['ticker'].replace('.AX','')} - {r['company_name']}": r['ticker']
                         for r in all_tickers}
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

                # Data provenance note
                provider = row.get("data_provider", "unknown")
                fetched_at = row.get("data_fetched_at")
                completeness = row.get("data_completeness", 0) or 0
                if fetched_at:
                    age_hours = (time.time() - fetched_at) / 3600
                    age_str = f"{age_hours:.0f}h ago" if age_hours >= 1 else "just now"
                    stale_warning = " ⚠️ stale" if age_hours > 26 else ""
                    st.caption(f"📡 Data: **{provider}** · {age_str}{stale_warning} · {completeness*100:.0f}% complete")

                m1, m2 = st.columns(2)
                m1.metric("Total Score", f"{row['total_score']}/30")
                m2.metric("Price", f"${row['current_price']:.2f}" if row['current_price'] else "N/A")
                st.metric("Market Cap", fmt_market_cap(row['market_cap']))

            with col2:
                st.plotly_chart(make_radar(row, selected_ticker),
                               use_container_width=True, key="detail_radar")

            st.divider()

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

            st.divider()
            st.markdown("### Score history")
            history = get_stock_history(conn2, selected_ticker)
            if len(history) > 1:
                fig = make_history_chart(history)
                if fig:
                    st.plotly_chart(fig, use_container_width=True, key="detail_history")

                # Show data source history
                st.markdown("**Data source history**")
                source_rows = []
                for h in history[-10:]:
                    source_rows.append({
                        "Date": h["scan_date"],
                        "Provider": h.get("data_provider", "unknown"),
                        "Completeness": f"{(h.get('data_completeness') or 0)*100:.0f}%",
                        "Score": h["total_score"],
                    })
                st.dataframe(pd.DataFrame(source_rows), use_container_width=True, hide_index=True)
            else:
                st.info("Score history builds up over time.")

            st.divider()
            st.markdown("### Narrative")
            narrative = row.get("narrative")
            if narrative:
                st.markdown(f"""
                <div style="background:#161b22;border:1px solid #30363d;border-left:3px solid #58a6ff;
                border-radius:8px;padding:20px 24px;line-height:1.8;color:#c9d1d9;font-size:0.95rem;">
                {narrative.replace(chr(10), '<br><br>')}
                </div>
                """, unsafe_allow_html=True)
                st.caption(f"Generated by {os.environ.get('OLLAMA_MODEL','llama3.1:8b')} · {row.get('scan_date','')}")
            else:
                st.info("No narrative yet - will be generated on next scan if Ollama is reachable.")

        conn2.close()

    # --- HEALTH TAB ---
    with tab_health:
        st.markdown("### 🩺 Data Pipeline Health")
        st.caption("Real-time status of data providers, cache, and recent fetch activity.")

        # Provider health from fetch log
        provider_health = get_provider_health_from_log(conn)

        st.markdown("#### Provider Status")

        # Static provider list with configured status
        providers_info = [
            {"name": "yahooquery", "key_env": None, "description": "Yahoo Finance (free, no key)"},
            {"name": "finnhub", "key_env": "FINNHUB_API_KEY", "description": "Finnhub (60 calls/min)"},
            {"name": "fmp", "key_env": "FMP_API_KEY", "description": "FMP (250 calls/day)"},
            {"name": "alpha_vantage", "key_env": "ALPHA_VANTAGE_API_KEY", "description": "Alpha Vantage (25 calls/day)"},
        ]

        health_lookup = {r["provider"]: r for r in provider_health}

        cols = st.columns(4)
        for i, pinfo in enumerate(providers_info):
            with cols[i]:
                name = pinfo["name"]
                configured = pinfo["key_env"] is None or bool(os.environ.get(pinfo["key_env"], ""))
                h = health_lookup.get(name, {})

                if not configured:
                    status_color = "#484f58"
                    status_text = "No API key"
                    dot = "⚫"
                elif h:
                    success_rate = h.get("successes", 0) / max(h.get("total", 1), 1)
                    if success_rate >= 0.8:
                        status_color = "#3fb950"
                        status_text = "Healthy"
                        dot = "🟢"
                    elif success_rate >= 0.5:
                        status_color = "#d29922"
                        status_text = "Degraded"
                        dot = "🟡"
                    else:
                        status_color = "#f85149"
                        status_text = "Failing"
                        dot = "🔴"
                else:
                    status_color = "#8b949e"
                    status_text = "No data yet"
                    dot = "⚪"

                st.markdown(f"""
                <div style="background:#161b22;border:1px solid #30363d;border-radius:8px;padding:14px;text-align:center;">
                    <div style="font-size:1.4rem">{dot}</div>
                    <div style="font-family:'DM Mono',monospace;font-size:0.8rem;color:#c9d1d9;margin-top:4px">{name}</div>
                    <div style="color:{status_color};font-size:0.75rem;margin-top:2px">{status_text}</div>
                    {f'<div style="color:#8b949e;font-size:0.7rem;margin-top:4px">{h.get("total",0)} calls · {h.get("successes",0)/(max(h.get("total",1),1))*100:.0f}% success</div>' if h else ''}
                </div>
                """, unsafe_allow_html=True)

        st.divider()

        # Cache statistics
        st.markdown("#### Cache Status")
        conn3 = get_db()
        try:
            cache_total = conn3.execute("SELECT COUNT(*) as c FROM data_cache").fetchone()["c"]
            cache_stale = conn3.execute(
                "SELECT COUNT(*) as c FROM data_cache WHERE fetched_at < ?",
                (time.time() - 86400,)
            ).fetchone()["c"]
            cache_fresh = cache_total - cache_stale

            cc1, cc2, cc3 = st.columns(3)
            cc1.metric("Cached stocks", cache_total)
            cc2.metric("Fresh (< 24h)", cache_fresh)
            cc3.metric("Stale (> 24h)", cache_stale)

            # Provider breakdown in cache
            cache_by_provider = conn3.execute("""
                SELECT provider, COUNT(*) as count, AVG(completeness) as avg_completeness
                FROM data_cache GROUP BY provider ORDER BY count DESC
            """).fetchall()

            if cache_by_provider:
                st.markdown("**Cache by provider**")
                rows_data = []
                for r in cache_by_provider:
                    rows_data.append({
                        "Provider": r["provider"],
                        "Stocks cached": r["count"],
                        "Avg completeness": f"{(r['avg_completeness'] or 0)*100:.0f}%",
                    })
                st.dataframe(pd.DataFrame(rows_data), use_container_width=True, hide_index=True)
        except Exception:
            st.info("No cache data yet.")

        conn3.close()

        st.divider()

        # Recent fetch log
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
                    "Completeness": f"{(entry['completeness'] or 0)*100:.0f}%",
                    "Note": entry.get("reason", "")[:40],
                })
            st.dataframe(pd.DataFrame(log_data), use_container_width=True, hide_index=True)
        else:
            st.info("No fetch activity logged yet.")

        st.divider()

        # Last scan summary
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

    conn.close()


if __name__ == "__main__":
    main()
