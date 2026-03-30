"""
frontend/app.py
===============
FinScope India — Home / Landing page.

Design decisions:
  • Hero section with value proposition (not a markdown bullet list)
  • Feature cards with plain-English descriptions for each module
  • Tech stack collapsed into expander — visible to recruiters, invisible to retail investors
  • Shared sidebar via render_sidebar()
  • No DB table names, layer names, or pipeline jargon exposed to users
"""

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import streamlit as st

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="FinScope India — NSE Equity Research",
    page_icon="📈",
    layout="wide",
)

# ── Global styles ──────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
    .block-container { padding-top: 2rem; max-width: 1100px; }

    /* Hero */
    .hero-tag {
        display: inline-block;
        background: #dbeafe; color: #1d4ed8;
        font-size: 0.78rem; font-weight: 700;
        padding: 3px 12px; border-radius: 20px;
        letter-spacing: 0.05em; text-transform: uppercase;
        margin-bottom: 0.8rem;
    }
    .hero-title {
        font-size: 2.6rem; font-weight: 800;
        color: #0d1117; line-height: 1.2;
        margin: 0 0 0.6rem 0;
    }
    .hero-sub {
        font-size: 1.1rem; color: #555;
        line-height: 1.7; max-width: 620px;
        margin-bottom: 1.6rem;
    }

    /* Feature cards */
    .feat-card {
        background: #fff;
        border: 1px solid #e1e4e8;
        border-radius: 12px;
        padding: 1.4rem 1.5rem;
        height: 100%;
        transition: box-shadow 0.15s;
    }
    .feat-card:hover { box-shadow: 0 4px 16px rgba(0,0,0,0.08); }
    .feat-icon { font-size: 2rem; margin-bottom: 0.5rem; }
    .feat-title { font-size: 1rem; font-weight: 700; color: #0d1117; margin-bottom: 0.3rem; }
    .feat-desc  { font-size: 0.88rem; color: #555; line-height: 1.6; }

    /* Stat strip */
    .stat-box {
        background: #f6f8fa;
        border-radius: 10px;
        padding: 1rem 1.2rem;
        text-align: center;
    }
    .stat-num  { font-size: 1.8rem; font-weight: 800; color: #0969da; }
    .stat-label{ font-size: 0.8rem; color: #666; margin-top: 0.2rem; }

    /* Status dot */
    .live-dot {
        display: inline-block; width: 8px; height: 8px;
        background: #2ca02c; border-radius: 50%;
        margin-right: 5px;
        animation: pulse 1.8s infinite;
    }
    @keyframes pulse {
        0%   { box-shadow: 0 0 0 0 rgba(44,160,44,0.5); }
        70%  { box-shadow: 0 0 0 6px rgba(44,160,44,0);  }
        100% { box-shadow: 0 0 0 0 rgba(44,160,44,0);   }
    }
</style>
""", unsafe_allow_html=True)

# ── Sidebar ─────────────────────────────────────────────────────────────────────
try:
    from frontend.components.sidebar import render_sidebar
    render_sidebar()
except Exception:
    pass

# ── Hero section ────────────────────────────────────────────────────────────────
st.markdown('<div class="hero-tag">Free · NSE India · Real-time data</div>', unsafe_allow_html=True)
st.markdown('<h1 class="hero-title">Understand the market.<br>Without the noise.</h1>', unsafe_allow_html=True)
st.markdown(
    '<p class="hero-sub">'
    'FinScope India gives retail investors plain-English analysis of NSE-listed stocks — '
    'price trends, company financials, and news-powered answers, updated automatically '
    'after every trading day.'
    '</p>',
    unsafe_allow_html=True,
)

cta1, cta2, _ = st.columns([1, 1, 3])
with cta1:
    st.page_link("pages/01_price_analytics.py", label="📊 View Price Charts")
with cta2:
    st.page_link("pages/03_ask_question.py",    label="🤖 Ask a Question")

st.divider()

# ── Stats strip ─────────────────────────────────────────────────────────────────
s1, s2, s3, s4 = st.columns(4)
stats = [
    ("5",         "NSE stocks tracked"),
    ("Daily",     "Automatic updates"),
    ("3",         "Analysis modules"),
    ("AI-powered","News Q&A engine"),
]
for col, (num, label) in zip([s1, s2, s3, s4], stats):
    col.markdown(
        f'<div class="stat-box">'
        f'<div class="stat-num">{num}</div>'
        f'<div class="stat-label">{label}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )

st.markdown("<br>", unsafe_allow_html=True)
st.divider()

# ── Feature cards ───────────────────────────────────────────────────────────────
st.markdown("### What you can do")
st.caption("Three tools, all in plain English — no financial background required.")
st.markdown("<br>", unsafe_allow_html=True)

c1, c2, c3 = st.columns(3)

with c1:
    st.markdown("""
<div class="feat-card">
    <div class="feat-icon">📊</div>
    <div class="feat-title">Price Analysis</div>
    <div class="feat-desc">
        Interactive candlestick charts with moving averages, momentum scores,
        and 52-week range — all labelled in terms you can act on.
        <br><br>
        <b>Answers:</b> Is this stock rising or falling? Is it overbought?
        How does it compare to its annual range?
    </div>
</div>
""", unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)
    st.page_link("pages/01_price_analytics.py", label="Open Price Analysis →")

with c2:
    st.markdown("""
<div class="feat-card">
    <div class="feat-icon">💼</div>
    <div class="feat-title">Company Insights</div>
    <div class="feat-desc">
        AI-written plain-English summaries of each company's financial position,
        alongside key metrics like market cap, P/E ratio, and 52-week range.
        <br><br>
        <b>Answers:</b> Is this company profitable? Is it cheap or expensive
        relative to earnings?
    </div>
</div>
""", unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)
    st.page_link("pages/02_earnings_summary.py", label="Open Company Insights →")

with c3:
    st.markdown("""
<div class="feat-card">
    <div class="feat-icon">🤖</div>
    <div class="feat-title">Ask a Question</div>
    <div class="feat-desc">
        Type any question about the stocks we track. Our AI searches through
        thousands of financial news articles and gives you a sourced answer —
        no hallucinations, no guessing.
        <br><br>
        <b>Answers:</b> What did TCS say about AI? Any concerns about Infosys?
    </div>
</div>
""", unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)
    st.page_link("pages/03_ask_question.py", label="Open Ask a Question →")

st.divider()

# ── How it works ────────────────────────────────────────────────────────────────
st.markdown("### How it works")
h1, h2, h3, h4 = st.columns(4)
steps = [
    ("1", "Data collected", "NSE price data is fetched automatically after every trading session."),
    ("2", "Processing",     "Indicators like RSI and moving averages are computed and stored."),
    ("3", "AI analysis",    "Company financials are summarised by an AI model in plain English."),
    ("4", "Dashboard",      "Everything surfaces here — updated by 7 PM IST every weekday."),
]
for col, (num, title, desc) in zip([h1, h2, h3, h4], steps):
    col.markdown(
        f'<div style="text-align:center;padding:0.8rem;">'
        f'<div style="width:32px;height:32px;border-radius:50%;background:#0969da;'
        f'color:white;font-weight:700;font-size:0.9rem;display:flex;align-items:center;'
        f'justify-content:center;margin:0 auto 0.5rem auto;">{num}</div>'
        f'<div style="font-weight:700;font-size:0.9rem;color:#0d1117;">{title}</div>'
        f'<div style="font-size:0.82rem;color:#666;margin-top:0.3rem;">{desc}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )

st.divider()

# ── Tech stack (collapsed — for recruiters) ────────────────────────────────────
with st.expander("🔧 Tech stack & architecture (for recruiters / developers)"):
    st.markdown("""
**Data pipeline**
- **Apache Airflow** — orchestrates all daily ETL tasks (11-task DAG)
- **Apache Spark (PySpark)** — data cleaning, indicator computation (RSI, SMA, normalisation)
- **Delta Lake** — medallion architecture: Bronze → Silver → Gold layers in PostgreSQL

**Storage & serving**
- **PostgreSQL** — primary store for all processed data; gold-layer views power the dashboard
- **ChromaDB** — vector store for news article embeddings (RAG engine)

**ML & NLP**
- **HuggingFace BART** (`facebook/bart-large-cnn`) — earnings text summarisation
- **Mistral-7B** — answer generation for the Q&A module
- **FinBERT** — sentiment classification on financial text

**Dashboard**
- **Streamlit** — UI framework
- **Plotly** — interactive charts
- **Docker Compose** — local orchestration of all services

**Data sources**
- NSE India via `yfinance` (price data)
- News articles via NewsAPI (for Q&A embedding)
    """)

# ── Disclaimer ──────────────────────────────────────────────────────────────────
st.markdown(
    '<div style="color:#aaa;font-size:0.76rem;text-align:center;padding:1rem 0;">'
    "FinScope India is a personal portfolio project. "
    "Nothing on this platform constitutes financial advice. "
    "Always verify data with official NSE sources before making investment decisions."
    "</div>",
    unsafe_allow_html=True,
)
