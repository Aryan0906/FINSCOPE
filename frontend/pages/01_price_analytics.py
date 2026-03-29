"""
frontend/pages/01_price_analytics.py
======================================
Price Analytics dashboard — Sprint 4.

Data sources (gold layer, Delta-backed):
  gold.stock_summary    — latest metrics per symbol (PySpark-computed)
  gold.v_latest_prices  — most-recent silver.prices row per symbol
  silver.prices         — full OHLCV history for candlestick chart
  gold.v_market_pulse   — daily market breadth (advance/decline)
"""

import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from frontend.components.db_connector import fetch_data, get_tickers

st.set_page_config(page_title="Price Analytics", layout="wide", page_icon="📊")
st.title("📊 Technical Price Analytics")
st.caption("Data source: gold.stock_summary (PySpark / Delta Lake) + silver.prices (history)")

# ─────────────────────────────────────────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────────────────────────────────────────

tickers = get_tickers()
if not tickers:
    st.error("No tickers in gold.stock_summary. Has the DAG run with spark_gold_summary?")
    st.stop()

selected = st.sidebar.selectbox("Select Symbol", tickers)
lookback = st.sidebar.slider("History (days)", min_value=30, max_value=365, value=90, step=30)

symbol_clean = selected

# ─────────────────────────────────────────────────────────────────────────────
# Gold summary — metrics panel (PySpark-computed)
# ─────────────────────────────────────────────────────────────────────────────

summary_df = fetch_data(f"""
    SELECT
        symbol, as_of_date, close_price, daily_return,
        normalised_close, sma_20, sma_50, rsi_14,
        market_cap, trailing_pe, price_to_book
    FROM gold.stock_summary
    WHERE symbol = '{symbol_clean}'
""")

if summary_df.empty:
    st.warning(f"No gold summary for {selected}. Trigger the DAG.")
    st.stop()

row = summary_df.iloc[0]

st.subheader(f"{selected} — Latest Snapshot ({row['as_of_date']})")
st.caption("Computed by PySpark Silver/Gold jobs from Delta Lake")

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Close",        f"₹{float(row['close_price']):.2f}" if pd.notna(row['close_price']) else "N/A")
col2.metric("Daily Return", f"{float(row['daily_return'])*100:.2f}%" if pd.notna(row['daily_return']) else "N/A")
col3.metric("RSI 14",       f"{float(row['rsi_14']):.1f}" if pd.notna(row['rsi_14']) else "N/A",
            delta="Oversold"   if pd.notna(row['rsi_14']) and float(row['rsi_14']) < 30 else
                  ("Overbought" if pd.notna(row['rsi_14']) and float(row['rsi_14']) > 70 else None))
col4.metric("SMA 20",       f"₹{float(row['sma_20']):.2f}" if pd.notna(row['sma_20']) else "N/A")
col5.metric("SMA 50",       f"₹{float(row['sma_50']):.2f}" if pd.notna(row['sma_50']) else "N/A")

# Normalised close progress bar
if pd.notna(row['normalised_close']):
    norm_val = float(row['normalised_close'])
    st.markdown(f"**52-Week Position:** {norm_val*100:.1f}th percentile")
    st.progress(norm_val)

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# Candlestick + volume (silver.prices historical data)
# ─────────────────────────────────────────────────────────────────────────────

price_df = fetch_data(f"""
    SELECT
        trade_date, open_price, high_price, low_price,
        close_price, volume, sma_20, sma_50, rsi_14,
        daily_return, is_outlier
    FROM silver.prices
    WHERE symbol = '{symbol_clean.replace(".NS", "")}'
      AND trade_date >= CURRENT_DATE - INTERVAL '{lookback} days'
    ORDER BY trade_date ASC
""")

if price_df.empty:
    st.info("No historical price data in silver.prices for this symbol.")
else:
    fig = make_subplots(
        rows=3, cols=1,
        shared_xaxes=True,
        row_heights=[0.6, 0.2, 0.2],
        vertical_spacing=0.03,
        subplot_titles=("Price & Moving Averages", "Volume", "RSI 14"),
    )

    # Candlestick
    fig.add_trace(go.Candlestick(
        x=price_df["trade_date"],
        open=price_df["open_price"],
        high=price_df["high_price"],
        low=price_df["low_price"],
        close=price_df["close_price"],
        name="OHLC",
        increasing_line_color="#26a641",
        decreasing_line_color="#e05252",
    ), row=1, col=1)

    if price_df["sma_20"].notna().any():
        fig.add_trace(go.Scatter(
            x=price_df["trade_date"], y=price_df["sma_20"],
            line=dict(color="#f0a500", width=1.5),
            name="SMA 20",
        ), row=1, col=1)

    if price_df["sma_50"].notna().any():
        fig.add_trace(go.Scatter(
            x=price_df["trade_date"], y=price_df["sma_50"],
            line=dict(color="#5b8ef0", width=1.5),
            name="SMA 50",
        ), row=1, col=1)

    # Outlier markers (Rule 7)
    outliers = price_df[price_df["is_outlier"] == True]
    if not outliers.empty:
        fig.add_trace(go.Scatter(
            x=outliers["trade_date"], y=outliers["close_price"],
            mode="markers",
            marker=dict(symbol="x", size=10, color="red"),
            name="Outlier (Rule 7)",
        ), row=1, col=1)

    # Volume bars
    colors = [
        "#26a641" if r >= 0 else "#e05252"
        for r in price_df["daily_return"].fillna(0)
    ]
    fig.add_trace(go.Bar(
        x=price_df["trade_date"], y=price_df["volume"],
        marker_color=colors, name="Volume", showlegend=False,
    ), row=2, col=1)

    # RSI
    if price_df["rsi_14"].notna().any():
        fig.add_trace(go.Scatter(
            x=price_df["trade_date"], y=price_df["rsi_14"],
            line=dict(color="#a855f7", width=1.5),
            name="RSI 14", showlegend=False,
        ), row=3, col=1)
        fig.add_hline(y=70, line_dash="dash", line_color="red",   row=3, col=1)
        fig.add_hline(y=30, line_dash="dash", line_color="green", row=3, col=1)
        fig.update_yaxes(range=[0, 100], row=3, col=1)

    fig.update_layout(
        height=700,
        template="plotly_white",
        xaxis_rangeslider_visible=False,
        margin=dict(l=20, r=20, t=40, b=20),
        legend=dict(orientation="h", y=1.05),
    )
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# Market Pulse — gold.v_market_pulse
# ─────────────────────────────────────────────────────────────────────────────

st.subheader("🌡️ Market Pulse (Last 10 Trading Days)")
st.caption("Source: gold.v_market_pulse — advance/decline breadth across all symbols")

pulse_df = fetch_data("""
    SELECT
        trade_date, total_symbols, advancing, declining, unchanged,
        advance_pct, avg_return_pct, outlier_count,
        overbought_count, oversold_count
    FROM gold.v_market_pulse
    ORDER BY trade_date DESC
""")

if not pulse_df.empty:
    pulse_df = pulse_df.head(10).sort_values("trade_date")

    fig2 = go.Figure()
    fig2.add_trace(go.Bar(
        x=pulse_df["trade_date"], y=pulse_df["advancing"],
        name="Advancing", marker_color="#26a641",
    ))
    fig2.add_trace(go.Bar(
        x=pulse_df["trade_date"], y=pulse_df["declining"],
        name="Declining", marker_color="#e05252",
    ))
    fig2.update_layout(
        barmode="stack", height=280, template="plotly_white",
        margin=dict(l=20, r=20, t=20, b=20),
        xaxis_title="Date", yaxis_title="Symbol Count",
    )
    st.plotly_chart(fig2, use_container_width=True)

    st.dataframe(
        pulse_df[["trade_date", "advancing", "declining", "advance_pct",
                  "avg_return_pct", "overbought_count", "oversold_count"]]
        .rename(columns={
            "trade_date":       "Date",
            "advancing":        "Up",
            "declining":        "Down",
            "advance_pct":      "Adv %",
            "avg_return_pct":   "Avg Ret %",
            "overbought_count": "Overbought",
            "oversold_count":   "Oversold",
        }),
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info("No market pulse data. Create views task must run first.")
