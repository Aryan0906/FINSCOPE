"""
frontend/pages/01_price_analytics.py
======================================
Stock Price Analysis — retail-investor-friendly rewrite.

Changes vs previous version:
  • All DB/layer references removed from UI text
  • RSI shows plain-English interpretation (not just a number)
  • 52-week position labelled Low ← → High
  • Chart legend uses plain terms ("Price", "Unusual movement")
  • Market overview section rewritten in plain English
  • Fundamental metrics shown with labels, not raw column names
  • Educational tooltips for RSI and SMA

Internal notes (not shown to users):
  • Reads from gold.stock_summary (PySpark-computed gold layer)
  • Historical chart from silver.prices
  • Market breadth from gold.v_market_pulse
  • "Unusual movement" markers were previously labelled "Outlier (Rule 7)"
"""

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from frontend.components.db_connector import fetch_data, get_tickers
from frontend.components.live_quote import get_live_quote
from streamlit_autorefresh import st_autorefresh

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Price Analysis · FinScope India", layout="wide", page_icon="📊")

st.markdown("""
<style>
    .block-container { padding-top: 1.5rem; }
    .metric-label { font-size: 0.78rem; color: #666; font-weight: 600; text-transform: uppercase; }
    .section-head { font-size: 1.1rem; font-weight: 700; margin-bottom: 0; }
    .explain-box {
        background: #f6f8fa; border-left: 3px solid #0969da;
        padding: 0.6rem 0.9rem; border-radius: 0 6px 6px 0;
        font-size: 0.84rem; color: #444; margin-top: 0.4rem;
    }
</style>
""", unsafe_allow_html=True)

# ── Sidebar ─────────────────────────────────────────────────────────────────────
try:
    from frontend.components.sidebar import render_sidebar
    render_sidebar()
except Exception:
    pass

st.sidebar.divider()
st.sidebar.markdown("**Chart settings**")

# ── Ticker + date range selection ──────────────────────────────────────────────
tickers = get_tickers()
if not tickers:
    st.error(
        "⚠️ No stock data available yet.\n\n"
        "The system fetches data every weekday at 6:30 PM IST. "
        "If you are seeing this during market hours, please check back after 7 PM."
    )
    st.stop()

selected = st.sidebar.selectbox("Choose a stock", tickers, format_func=lambda x: x)
lookback = st.sidebar.select_slider(
    "Show history for",
    options=[30, 60, 90, 180, 365],
    value=90,
    format_func=lambda x: f"{x} days",
)

symbol_clean = selected.replace(".NS", "")

# ── Helper functions ───────────────────────────────────────────────────────────

def _rsi_label(rsi: float) -> tuple:
    """Return (badge_text, colour) for an RSI value."""
    if rsi < 30:
        return "Oversold 💡", "#1a7f37"       # green — possible buying zone
    elif rsi < 50:
        return "Weakening 📉", "#cf8f00"       # amber
    elif rsi < 70:
        return "Healthy 📊", "#0969da"         # blue
    else:
        return "Overbought ⚠️", "#cf222e"      # red — caution zone


def _position_label(norm: float) -> str:
    pct = norm * 100
    if pct < 20:
        return "Near 52-week low"
    elif pct < 50:
        return "Lower half of range"
    elif pct < 80:
        return "Upper half of range"
    else:
        return "Near 52-week high"


def _fmt_crore(val) -> str:
    """Format large number as ₹X,XXX Cr."""
    if pd.isna(val):
        return "N/A"
    try:
        cr = float(val) / 1e7
        if cr >= 1_00_000:
            return f"₹{cr/1e5:.1f}L Cr"
        return f"₹{cr:,.0f} Cr"
    except Exception:
        return "N/A"


# ── Fetch latest snapshot & Live Quote ─────────────────────────────────────────
live_data = get_live_quote(symbol_clean)

if live_data and live_data.get("is_live"):
    st_autorefresh(interval=15000, key="data_refresh")

# Source: gold.stock_summary (PySpark-computed gold layer — not shown to users)
summary_df = fetch_data(f"""
    SELECT symbol, as_of_date, close_price, daily_return,
           normalised_close, sma_20, sma_50, rsi_14,
           market_cap, trailing_pe, price_to_book,
           fifty_two_week_high, fifty_two_week_low
    FROM gold.stock_summary
    WHERE symbol = '{symbol_clean}'
""")

if summary_df.empty:
    st.warning(
        f"⚠️ No data found for **{selected}**. "
        "Data is updated every weekday at 6:30 PM IST."
    )
    st.stop()

row = summary_df.iloc[0].to_dict()

# Override DB values with Live YFinance Data if available
if live_data:
    if live_data.get("live_price"):
        row["close_price"] = live_data["live_price"]
    if live_data.get("change_pct") is not None:
        row["daily_return"] = live_data["change_pct"] / 100.0
    if live_data.get("market_cap"):
        row["market_cap"] = live_data["market_cap"]
    if live_data.get("trailing_pe"):
        row["trailing_pe"] = live_data["trailing_pe"]
    if live_data.get("price_to_book"):
        row["price_to_book"] = live_data["price_to_book"]
    if live_data.get("fifty_two_week_high"):
        row["fifty_two_week_high"] = live_data["fifty_two_week_high"]
    if live_data.get("fifty_two_week_low"):
        row["fifty_two_week_low"] = live_data["fifty_two_week_low"]

# ── Page header ────────────────────────────────────────────────────────────────
st.markdown(f"## 📊 {symbol_clean}")
st.caption(f"Last updated: {'Live' if live_data and live_data.get('is_live') else row['as_of_date']}  ·  Prices in ₹ (INR)  ·  Source: NSE India & YFinance")
st.divider()

# ── KPI strip ──────────────────────────────────────────────────────────────────
k1, k2, k3, k4, k5 = st.columns(5)

# 1 — Current price
with k1:
    price = float(row["close_price"]) if pd.notna(row["close_price"]) else None
    st.metric("Current Price", f"₹{price:,.2f}" if price else "N/A")

# 2 — Day change
with k2:
    ret = float(row["daily_return"]) * 100 if pd.notna(row["daily_return"]) else None
    delta_str = f"{ret:+.2f}%" if ret is not None else None
    st.metric(
        "Today's Change",
        f"{ret:+.2f}%" if ret is not None else "N/A",
        delta=delta_str,
        delta_color="normal",
    )

# 3 — RSI with plain-English badge
with k3:
    rsi_val = float(row["rsi_14"]) if pd.notna(row["rsi_14"]) else None
    if rsi_val is not None:
        badge, badge_color = _rsi_label(rsi_val)
        st.metric("RSI (Momentum)", f"{rsi_val:.1f}")
        st.markdown(
            f'<span style="background:{badge_color};color:white;'
            f'padding:2px 10px;border-radius:12px;font-size:0.78rem;font-weight:600;">'
            f'{badge}</span>',
            unsafe_allow_html=True,
        )
    else:
        st.metric("RSI (Momentum)", "N/A")

# 4 — 52-week position
with k4:
    norm = float(row["normalised_close"]) if pd.notna(row["normalised_close"]) else None
    if norm is not None:
        st.metric("52-Week Position", f"{norm*100:.0f}th percentile")
        st.progress(norm)
        st.caption(_position_label(norm))
    else:
        st.metric("52-Week Position", "N/A")

# 5 — P/E ratio
with k5:
    pe = float(row["trailing_pe"]) if pd.notna(row["trailing_pe"]) else None
    st.metric("P/E Ratio", f"{pe:.1f}x" if pe else "N/A")
    if pe:
        st.caption("Price ÷ annual earnings per share")

st.divider()

# ── Fundamentals strip ─────────────────────────────────────────────────────────
with st.expander("📋 Company fundamentals", expanded=True):
    f1, f2, f3, f4 = st.columns(4)
    f1.metric("Market Cap", _fmt_crore(row.get("market_cap")))
    f1.caption("Total company value")

    pb = float(row["price_to_book"]) if pd.notna(row.get("price_to_book")) else None
    f2.metric("Price-to-Book", f"{pb:.2f}x" if pb else "N/A")
    f2.caption("Share price vs. book value")

    h52 = float(row["fifty_two_week_high"]) if pd.notna(row.get("fifty_two_week_high")) else None
    l52 = float(row["fifty_two_week_low"])  if pd.notna(row.get("fifty_two_week_low")) else None
    f3.metric("52-Week High", f"₹{h52:,.2f}" if h52 else "N/A")
    f4.metric("52-Week Low",  f"₹{l52:,.2f}" if l52 else "N/A")

    sma20 = float(row["sma_20"]) if pd.notna(row.get("sma_20")) else None
    if sma20 and price:
        direction = "above" if price > sma20 else "below"
        diff = abs(price - sma20)
        st.caption(
            f"📌 Current price is **{direction}** the 20-day average (₹{sma20:,.2f}) "
            f"by ₹{diff:,.2f}"
        )

# ── RSI + SMA explanation ──────────────────────────────────────────────────────
with st.expander("📖 What do RSI and moving averages mean?", expanded=False):
    st.markdown("""
**RSI (Relative Strength Index)**  
A score from 0–100 that shows *how fast* a stock has been moving.
- **Below 30** → Stock has fallen quickly. May be oversold — worth watching.
- **30–70** → Normal territory. No extreme signals.
- **Above 70** → Stock has risen quickly. May be overbought — caution advised.

RSI doesn't tell you *when* to buy or sell. It's just one signal among many.

---

**Moving Averages (20-Day / 50-Day)**  
The average closing price over the last 20 or 50 trading days.
- When price is **above** the moving average → short-term uptrend.
- When price **crosses below** the moving average → possible trend reversal.
- The **20-day** average is more sensitive to recent moves; the **50-day** shows the bigger trend.
    """)

st.divider()

# ── Price chart ────────────────────────────────────────────────────────────────
# Source: silver.prices (full OHLCV history — referenced in code only, not UI)
st.markdown("### Price chart")
price_df = fetch_data(f"""
    SELECT trade_date, open_price, high_price, low_price,
           close_price, volume, sma_20, sma_50, rsi_14,
           daily_return, is_outlier
    FROM silver.prices
    WHERE symbol = '{symbol_clean}'
      AND trade_date >= CURRENT_DATE - INTERVAL '{lookback} days'
    ORDER BY trade_date ASC
""")

if price_df.empty:
    st.info("No historical chart data available for this period.")
else:
    # Append Live Data Candle to Chart
    if live_data and live_data.get("live_price"):
        last_date = price_df["trade_date"].max()
        today_date = pd.Timestamp.now().date()
        if pd.to_datetime(last_date).date() < today_date:
            live_price = float(live_data["live_price"])
            prev_close = float(live_data.get("prev_close") or price_df.iloc[-1]["close_price"])
            new_row = pd.DataFrame([{
                "trade_date": today_date,
                "open_price": prev_close,
                "high_price": max(prev_close, live_price),
                "low_price": min(prev_close, live_price),
                "close_price": live_price,
                "volume": 0,
                "sma_20": None,
                "sma_50": None,
                "rsi_14": None,
                "daily_return": (live_data.get("change_pct") or 0.0) / 100.0,
                "is_outlier": False
            }])
            price_df = pd.concat([price_df, new_row], ignore_index=True)

    fig = make_subplots(
        rows=3, cols=1,
        shared_xaxes=True,
        row_heights=[0.60, 0.20, 0.20],
        vertical_spacing=0.04,
        subplot_titles=(
            f"{symbol_clean} — Price & Moving Averages {'(Live)' if live_data.get('is_live') else '(Delayed)'}",
            "Trading Volume",
            "RSI (Momentum Indicator)",
        ),
    )

    # ── Candlestick ────────────────────────────────────────────────
    fig.add_trace(go.Candlestick(
        x=price_df["trade_date"],
        open=price_df["open_price"],
        high=price_df["high_price"],
        low=price_df["low_price"],
        close=price_df["close_price"],
        name="Price",
        increasing_line_color="#2ca02c",
        decreasing_line_color="#d62728",
    ), row=1, col=1)

    if price_df["sma_20"].notna().any():
        fig.add_trace(go.Scatter(
            x=price_df["trade_date"], y=price_df["sma_20"],
            line=dict(color="#f0a500", width=1.8),
            name="20-Day Average",
        ), row=1, col=1)

    if price_df["sma_50"].notna().any():
        fig.add_trace(go.Scatter(
            x=price_df["trade_date"], y=price_df["sma_50"],
            line=dict(color="#5b8ef0", width=1.8),
            name="50-Day Average",
        ), row=1, col=1)

    # Unusual price movements (internal: is_outlier flag from Rule 7 detection)
    outliers = price_df[price_df["is_outlier"] == True]
    if not outliers.empty:
        fig.add_trace(go.Scatter(
            x=outliers["trade_date"], y=outliers["close_price"],
            mode="markers",
            marker=dict(symbol="x", size=10, color="purple"),
            name="Unusual movement",
        ), row=1, col=1)

    # ── Volume bars ────────────────────────────────────────────────
    bar_colors = [
        "#2ca02c" if r >= 0 else "#d62728"
        for r in price_df["daily_return"].fillna(0)
    ]
    fig.add_trace(go.Bar(
        x=price_df["trade_date"], y=price_df["volume"],
        marker_color=bar_colors, name="Volume", showlegend=False,
    ), row=2, col=1)

    # ── RSI line ───────────────────────────────────────────────────
    if price_df["rsi_14"].notna().any():
        fig.add_trace(go.Scatter(
            x=price_df["trade_date"], y=price_df["rsi_14"],
            line=dict(color="#9467bd", width=1.8),
            name="RSI", showlegend=False,
        ), row=3, col=1)
        # Overbought / oversold bands
        fig.add_hrect(y0=70, y1=100, fillcolor="rgba(214,39,40,0.07)",
                      line_width=0, row=3, col=1)
        fig.add_hrect(y0=0,  y1=30,  fillcolor="rgba(44,160,44,0.07)",
                      line_width=0, row=3, col=1)
        fig.add_hline(y=70, line_dash="dot", line_color="rgba(214,39,40,0.5)",
                      row=3, col=1)
        fig.add_hline(y=30, line_dash="dot", line_color="rgba(44,160,44,0.5)",
                      row=3, col=1)
        fig.update_yaxes(range=[0, 100], row=3, col=1)

    fig.update_layout(
        height=680,
        template="plotly_white",
        xaxis_rangeslider_visible=False,
        margin=dict(l=20, r=20, t=80, b=20),
        legend=dict(orientation="h", y=1.10, x=0, yanchor="bottom"),
        font=dict(family="Inter, sans-serif"),
    )
    fig.update_yaxes(title_text="Price (₹)", row=1, col=1)
    fig.update_yaxes(title_text="Volume",    row=2, col=1)
    fig.update_yaxes(title_text="RSI",       row=3, col=1)

    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Market Overview ────────────────────────────────────────────────────────────
# Source: gold.v_market_pulse (advance/decline breadth view — referenced in code only)
st.markdown("### Today's market overview")
st.caption("How the broader NSE market moved — not just this stock.")

pulse_df = fetch_data("""
    SELECT trade_date, total_symbols, advancing, declining, unchanged,
           advance_pct, avg_return_pct, overbought_count, oversold_count
    FROM gold.v_market_pulse
    ORDER BY trade_date DESC
""")

if pulse_df.empty:
    st.info("Market overview will appear after the pipeline runs.")
else:
    latest = pulse_df.iloc[0]
    adv   = int(latest["advancing"])
    dec   = int(latest["declining"])
    total = int(latest["total_symbols"])
    adv_pct = float(latest["advance_pct"]) if pd.notna(latest["advance_pct"]) else 0

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Stocks rising today",  f"{adv} / {total}")
    m2.metric("Stocks falling today", f"{dec} / {total}")
    m3.metric(
        "Advance ratio", f"{adv_pct:.1f}%",
        delta="Broad rally" if adv_pct > 60 else ("Broad sell-off" if adv_pct < 40 else "Mixed"),
        delta_color="normal" if adv_pct > 60 else ("inverse" if adv_pct < 40 else "off"),
    )
    ob  = int(latest["overbought_count"]) if pd.notna(latest["overbought_count"]) else 0
    os_ = int(latest["oversold_count"])   if pd.notna(latest["oversold_count"])   else 0
    m4.metric("Overbought / Oversold", f"{ob} / {os_}")

    # Mini advance/decline bar chart
    if len(pulse_df) > 1:
        history = pulse_df.head(10).sort_values("trade_date")
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(
            x=history["trade_date"], y=history["advancing"],
            name="Rising", marker_color="#2ca02c",
        ))
        fig2.add_trace(go.Bar(
            x=history["trade_date"], y=history["declining"],
            name="Falling", marker_color="#d62728",
        ))
        fig2.update_layout(
            barmode="stack", height=220, template="plotly_white",
            margin=dict(l=10, r=10, t=20, b=20),
            legend=dict(orientation="h", y=1.1),
            xaxis_title=None, yaxis_title="Number of stocks",
        )
        st.plotly_chart(fig2, use_container_width=True)
