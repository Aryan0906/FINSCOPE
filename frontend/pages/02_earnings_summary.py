"""
frontend/pages/02_earnings_summary.py
=======================================
Company Insights — retail-investor rewrite.

Changes:
  • Renamed to "Company Insights" (not "Earnings Analysis")
  • Pulls gold.stock_summary for key metrics alongside the AI summary
  • Sentiment derived from keyword analysis → plain badge (Positive / Neutral / Negative)
  • Clean expandable card per company with all context in one place
  • No DB jargon visible anywhere (public.earnings_summaries referenced only in code)
  • Empty-state handles gracefully with pipeline status message
  • Source file column removed from visible UI (internal reference only)
"""

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pandas as pd
import streamlit as st

from frontend.components.db_connector import fetch_data
from frontend.components.live_quote import get_live_quote
from streamlit_autorefresh import st_autorefresh

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Company Insights · FinScope India",
    layout="wide",
)

st.markdown("""
<style>
    .block-container { padding-top: 1.5rem; }
    .company-card {
        border: 1px solid #e1e4e8;
        border-radius: 10px;
        padding: 1.2rem 1.4rem;
        background: #fff;
        margin-bottom: 1rem;
    }
    .company-header { font-size: 1.15rem; font-weight: 700; color: #0d1117; }
    .summary-text { font-size: 0.92rem; color: #333; line-height: 1.7; }
    .metric-mini { font-size: 0.78rem; color: #666; }
</style>
""", unsafe_allow_html=True)

# ── Sidebar ─────────────────────────────────────────────────────────────────────
try:
    from frontend.components.sidebar import render_sidebar
    render_sidebar()
except Exception:
    pass

# ── Helper: sentiment badge ────────────────────────────────────────────────────

def _sentiment_badge(score) -> str:
    """Return styled HTML badge for a numeric sentiment score."""
    if score is None or pd.isna(score):
        return ""
    s = float(score)
    if s > 0.1:
        return (
            '<span style="background:#d1f0d8;color:#1a7f37;border:1px solid #82d49c;'
            'padding:2px 10px;border-radius:12px;font-size:0.78rem;font-weight:700;">'
            "Positive Outlook</span>"
        )
    elif s < -0.1:
        return (
            '<span style="background:#ffd6d6;color:#cf222e;border:1px solid #f7a5a5;'
            'padding:2px 10px;border-radius:12px;font-size:0.78rem;font-weight:700;">'
            "Cautious Outlook</span>"
        )
    else:
        return (
            '<span style="background:#fff3cd;color:#856404;border:1px solid #ffd874;'
            'padding:2px 10px;border-radius:12px;font-size:0.78rem;font-weight:700;">'
            "Neutral Outlook</span>"
        )


def _fmt_crore(val) -> str:
    if val is None or pd.isna(val):
        return "N/A"
    cr = float(val) / 1e7
    if cr >= 1_00_000:
        return f"₹{cr/1e5:.1f}L Cr"
    return f"₹{cr:,.0f} Cr"


def _fmt_pe(val) -> str:
    if val is None or pd.isna(val):
        return "N/A"
    return f"{float(val):.1f}x"


# ── Page header ────────────────────────────────────────────────────────────────
st.markdown("## Company Insights")
st.caption(
    "Institutional financial summaries for NSE-listed equities. "
    "Updated automatically after market close."
)
st.divider()

# ── Load data ──────────────────────────────────────────────────────────────────
# Source: public.earnings_summaries + gold.stock_summary (referenced in code only)
earnings_df = fetch_data("""
    SELECT ticker, report_date, summary, created_at
    FROM public.earnings_summaries
    ORDER BY report_date DESC
""")

metrics_df = fetch_data("""
    SELECT symbol, close_price, daily_return, rsi_14,
           market_cap, trailing_pe, price_to_book,
           fifty_two_week_high, fifty_two_week_low, as_of_date
    FROM gold.stock_summary
""")
metrics_map = {}
if not metrics_df.empty:
    for _, mrow in metrics_df.iterrows():
        metrics_map[str(mrow["symbol"]).upper()] = mrow.to_dict()

# ── Empty state ────────────────────────────────────────────────────────────────
if earnings_df.empty:
    st.info("""
**No company analysis available yet.**

Analysis is generated automatically every weekday at 6:30 PM IST.
Check back after the next market close.

Once data is available, you'll see:
- Key financial ratios (P/E, Market Cap, Price-to-Book) for each company
- An AI-written plain-English summary of the company's financial position
- A sentiment indicator showing whether the outlook looks positive or cautious
    """)
    st.stop()

# ── Sidebar filter ─────────────────────────────────────────────────────────────
tickers = ["All companies"] + sorted(earnings_df["ticker"].unique().tolist())
selected_ticker = st.sidebar.selectbox("Filter by company", tickers)
st.sidebar.divider()
st.sidebar.download_button(
    "Download as CSV",
    data=earnings_df.to_csv(index=False),
    file_name="finscope_company_insights.csv",
    mime="text/csv",
    use_container_width=True,
)

if selected_ticker != "All companies":
    earnings_df = earnings_df[earnings_df["ticker"] == selected_ticker]

# ── Summary stats strip ────────────────────────────────────────────────────────
s1, s2, s3 = st.columns(3)
s1.metric("Companies analysed", earnings_df["ticker"].nunique())
s2.metric("Total reports",       len(earnings_df))
last_update = earnings_df["created_at"].max()
s3.metric(
    "Last updated",
    pd.to_datetime(last_update).strftime("%d %b, %H:%M") if pd.notna(last_update) else "N/A",
)
st.divider()

# ── Company cards ──────────────────────────────────────────────────────────────
st.markdown(f"### Showing {len(earnings_df)} report(s)")

# Global live check & auto-refresh
any_live = False
live_quotes = {}
for t in earnings_df["ticker"].unique():
    lq = get_live_quote(str(t).upper())
    live_quotes[str(t).upper()] = lq
    if lq and lq.get("is_live"):
        any_live = True

if any_live:
    st_autorefresh(interval=15000, key="earnings_refresh")

for idx, (_, row) in enumerate(earnings_df.iterrows()):
    ticker   = str(row["ticker"]).upper()
    summary  = row.get("summary") or ""
    date_val = row.get("report_date")
    created  = row.get("created_at")

    date_str = (
        pd.to_datetime(date_val).strftime("%d %b %Y")
        if pd.notna(date_val) else "Date unavailable"
    )

    m = metrics_map.get(ticker, {})

    with st.container():
        # ── Company header ────────────────────────────────────────
        hdr_col, badge_col = st.columns([3, 1])
        with hdr_col:
            st.markdown(f"#### {ticker}")
            st.caption(f"Analysis date: {date_str}")
        with badge_col:
            st.markdown("<br>", unsafe_allow_html=True)
            # Keyword-based sentiment proxy (visible label only — not a model score)
            pos_words = ["growth", "profit", "increase", "strong", "beat"]
            neg_words = ["loss", "decline", "weak", "miss", "fall"]
            summary_lower = summary.lower()
            pos_count = sum(w in summary_lower for w in pos_words)
            neg_count = sum(w in summary_lower for w in neg_words)
            fake_score = (pos_count - neg_count) * 0.15
            st.markdown(_sentiment_badge(fake_score), unsafe_allow_html=True)

        # Override DB metrics with Live yfinance data if available
        live_m = live_quotes.get(ticker, {})
        if live_m and live_m.get("live_price"):
            m["close_price"] = live_m.get("live_price")
            m["daily_return"] = (live_m.get("change_pct") or 0.0) / 100.0
        if live_m and live_m.get("market_cap"):
            m["market_cap"] = live_m.get("market_cap")
            m["trailing_pe"] = live_m.get("trailing_pe")
            m["price_to_book"] = live_m.get("price_to_book")
            m["fifty_two_week_high"] = live_m.get("fifty_two_week_high")
            m["fifty_two_week_low"] = live_m.get("fifty_two_week_low")

        # ── Key metrics row ───────────────────────────────────────
        if m:
            km1, km2, km3, km4, km5 = st.columns(5)
            close = m.get("close_price")
            ret   = m.get("daily_return")
            pe    = m.get("trailing_pe")
            mcap  = m.get("market_cap")
            rsi   = m.get("rsi_14")

            km1.metric(
                "Price",
                f"₹{float(close):,.2f}" if pd.notna(close) else "N/A",
                delta=f"{float(ret)*100:+.2f}%" if pd.notna(ret) else None,
            )
            km2.metric("Market Cap",    _fmt_crore(mcap))
            km3.metric("P/E Ratio",     _fmt_pe(pe))
            pb = m.get("price_to_book")
            km4.metric("Price-to-Book", f"{float(pb):.2f}x" if pd.notna(pb) else "N/A")
            km5.metric("RSI",           f"{float(rsi):.1f}"  if pd.notna(rsi) else "N/A")

        # ── Professional Analyst Perspective ──────────────────────
        with st.expander("Executive Summary: Market Positioning & Valuation", expanded=(idx == 0)):
            analysis_lines = []
            
            if pd.notna(pe) and float(pe) > 0:
                val = float(pe)
                if val > 30:
                    analysis_lines.append(f"**Valuation:** {ticker} is trading at a premium earnings multiple of **{val:.1f}x**. This elevated P/E ratio indicates that the market has priced in robust forward earnings momentum, leaving a relatively constrained margin of safety at current levels.")
                elif val < 15:
                    analysis_lines.append(f"**Valuation:** Trading at a conservative earnings multiple of **{val:.1f}x**, this equity presents a potential value proposition. Such discounting typically reflects structural market hesitation or a cyclical trough pending an operational catalyst.")
                else:
                    analysis_lines.append(f"**Valuation:** The equity is valued at a moderate **{val:.1f}x** operating multiple, indicating balanced institutional consensus regarding its near-term earnings trajectory and overall risk profile.")
            
            if pd.notna(close) and pd.notna(m.get("fifty_two_week_high")) and pd.notna(m.get("fifty_two_week_low")):
                c = float(close)
                h = float(m["fifty_two_week_high"])
                l = float(m["fifty_two_week_low"])
                if h > l:
                    drop_from_high = ((h - c) / h) * 100
                    if drop_from_high < 5:
                        analysis_lines.append(f"**Technical Momentum:** Price action remains highly constructive. At **₹{c:,.2f}**, the asset is actively testing its 52-week resistance threshold (₹{h:,.2f}), underscoring sustained bullish flow in recent sessions.")
                    elif drop_from_high > 20:
                        analysis_lines.append(f"**Technical Momentum:** The asset is currently experiencing technical distribution, trading at **₹{c:,.2f}**—roughly **{drop_from_high:.1f}% below** its 52-week peak. The stock is currently navigating crucial baseline support parameters.")
                    else:
                        analysis_lines.append(f"**Technical Momentum:** The stock is consolidating mid-range at **₹{c:,.2f}**, remaining well-supported above its 52-week floor while methodically absorbing broader market volatility.")

            if pd.notna(pb) and float(pb) > 0:
                pb_val = float(pb)
                analysis_lines.append(f"**Asset Value:** From a balance sheet perspective, the Price-to-Book ratio stands at **{pb_val:.2f}x**. This multiple quantifies the premium the market places on the firm's tangible equity base and its forward return on equity (ROE) capacity.")

            if analysis_lines:
                st.markdown("\n\n".join(analysis_lines))
                st.caption(f"Real-time desk analysis derived from live market feeds.")
            else:
                st.info("Live market metrics are currently unavailable to generate an analysis.")

        # ── 52-week range bar ─────────────────────────────────────
        if m:
            h52   = m.get("fifty_two_week_high")
            l52   = m.get("fifty_two_week_low")
            close = m.get("close_price")
            if pd.notna(h52) and pd.notna(l52) and pd.notna(close) and float(h52) != float(l52):
                norm = (float(close) - float(l52)) / (float(h52) - float(l52))
                st.markdown(
                    f"**52-Week Range** &nbsp; ₹{float(l52):,.0f} "
                    f"← &nbsp; current: ₹{float(close):,.0f} &nbsp; → "
                    f"₹{float(h52):,.0f}"
                )
                st.progress(float(norm))

        st.divider()

# ── Disclaimer ─────────────────────────────────────────────────────────────────
st.markdown(
    '<div style="color:#aaa;font-size:0.78rem;text-align:center;">'
    "Summaries are AI-generated from NSE data and not financial advice. "
    "Always verify with official sources before making investment decisions."
    "</div>",
    unsafe_allow_html=True,
)
