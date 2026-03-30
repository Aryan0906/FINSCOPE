"""
frontend/components/sidebar.py
================================
Shared sidebar renderer — import and call render_sidebar() on every page
so branding, market status, and navigation stay consistent.

No DB table names, layer references, or pipeline jargon shown to users.
"""
from datetime import datetime, timezone, timedelta
import streamlit as st


def _ist_now() -> datetime:
    return datetime.now(timezone(timedelta(hours=5, minutes=30)))


def _is_market_open(ist: datetime) -> bool:
    if ist.weekday() > 4:
        return False
    open_t  = ist.replace(hour=9,  minute=15, second=0, microsecond=0)
    close_t = ist.replace(hour=15, minute=30, second=0, microsecond=0)
    return open_t <= ist <= close_t


def render_sidebar():
    """Call once at the top of every page. Renders consistent branding + nav."""
    with st.sidebar:
        # Hide default Streamlit sidebar navigation
        st.markdown(
            """
            <style>
                [data-testid="stSidebarNav"] {display: none;}
            </style>
            """,
            unsafe_allow_html=True,
        )
        
        # ── Branding ───────────────────────────────────────────────
        st.markdown("## FinScope India")
        st.caption("Institutional NSE Equity Research")
        st.divider()

        # ── Market status ──────────────────────────────────────────
        ist = _ist_now()
        if _is_market_open(ist):
            st.success("NSE Market is **OPEN**")
        else:
            st.warning("NSE Market is **CLOSED**")
        st.caption(f"IST: {ist.strftime('%d %b %Y, %H:%M')}")
        st.divider()

        # ── Navigation ─────────────────────────────────────────────
        st.markdown("**Navigate**")
        st.page_link("app.py",                        label="Home")
        st.page_link("pages/01_price_analytics.py",   label="Price Analysis")
        st.page_link("pages/02_earnings_summary.py",  label="Company Insights")
        st.page_link("pages/03_ask_question.py",      label="Ask a Question")
        st.divider()

        # ── Stocks tracked ─────────────────────────────────────────
        st.markdown("**Stocks tracked**")
        for sym in ["RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK"]:
            st.markdown(
                f'<span style="background:#f0f6ff;border:1px solid #c8d8f0;'
                f'border-radius:4px;padding:2px 8px;font-size:0.78rem;'
                f'font-weight:600;margin:2px;display:inline-block;">{sym}</span>',
                unsafe_allow_html=True,
            )
        st.divider()

        # ── Disclaimer ─────────────────────────────────────────────
        st.caption("⚠️ For informational purposes only. Not financial advice.")
