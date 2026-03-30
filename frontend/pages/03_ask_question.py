"""
frontend/pages/03_ask_question.py
==================================
Ask a Question — retail-investor rewrite.

Changes:
  • Added example question chips (click to use)
  • Plain-English explanation of what the tool does and its limitations
  • Better answer layout with source cards
  • "Insufficient data" handled gracefully with helpful message
  • Similarity threshold (0.4) and "Rule 4" moved to code comments — not shown to users
  • Loading state with context-appropriate spinner text
"""

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import streamlit as st

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Ask a Question · FinScope India",
    page_icon="🤖",
    layout="wide",
)

st.markdown("""
<style>
    .block-container { padding-top: 1.5rem; }
    .answer-box {
        background: #f6f8fa;
        border-left: 4px solid #0969da;
        border-radius: 0 8px 8px 0;
        padding: 1.2rem 1.4rem;
        font-size: 0.95rem;
        line-height: 1.7;
        color: #1f2328;
    }
    .source-card {
        background: #fff;
        border: 1px solid #e1e4e8;
        border-radius: 8px;
        padding: 0.7rem 1rem;
        margin-top: 0.5rem;
        font-size: 0.84rem;
    }
    .example-pill {
        display: inline-block;
        background: #f0f6ff;
        border: 1px solid #c8d8f0;
        color: #0550ae;
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 0.82rem;
        cursor: pointer;
        margin: 3px;
    }
</style>
""", unsafe_allow_html=True)

# ── Sidebar ─────────────────────────────────────────────────────────────────────
try:
    from frontend.components.sidebar import render_sidebar
    render_sidebar()
except Exception:
    pass

# ── Sidebar info (plain English — no RAG config jargon) ───────────────────────
st.sidebar.divider()
with st.sidebar.expander("ℹ️ How does this work?", expanded=False):
    st.markdown("""
**What it does:**
We've collected thousands of financial news articles about the stocks we track.
When you ask a question, our AI:
1. Finds the most relevant articles
2. Reads them carefully
3. Gives you an answer based only on those articles

**What it doesn't do:**
- It cannot predict stock prices
- It cannot give you buy/sell advice
- If relevant articles aren't available, it says so honestly

**Why "We couldn't find relevant articles"?**
This means we couldn't find news articles relevant enough to answer your question.
Try rephrasing or asking about a different topic.
    """)
    # Internal note: similarity threshold = 0.4 (Rule 4 hallucination guard)
    # Documents below this cosine-similarity threshold are rejected.

# ── Page header ────────────────────────────────────────────────────────────────
st.markdown("## 🤖 Ask a Question")
st.caption(
    "Ask anything about the stocks we track — our AI searches through "
    "thousands of financial news articles to find a sourced answer."
)
st.divider()

# ── Example questions ──────────────────────────────────────────────────────────
st.markdown("**Try one of these:**")

EXAMPLES = [
    "What are the recent developments for HDFC Bank?",
    "Has TCS reported any major contract wins recently?",
    "What is Reliance's latest expansion strategy?",
    "Are there any concerns about Infosys's revenue growth?",
    "What do analysts say about ICICI Bank?",
]

example_cols = st.columns(len(EXAMPLES))
selected_example = None
for col, q in zip(example_cols, EXAMPLES):
    short = q if len(q) < 35 else q[:32] + "…"
    if col.button(short, use_container_width=True):
        selected_example = q

st.markdown("<br>", unsafe_allow_html=True)

# ── Question input ─────────────────────────────────────────────────────────────
question = st.text_input(
    "Or type your own question:",
    value=selected_example or "",
    placeholder="e.g. What has TCS said about AI investments?",
    max_chars=300,
)

ask_button = st.button("🔍 Search & Answer", type="primary", use_container_width=False)

# ── Run query ──────────────────────────────────────────────────────────────────
if ask_button:
    if not question or not question.strip():
        st.warning("Please type a question or choose one of the examples above.")
        st.stop()

    st.divider()

    with st.spinner("Reading financial news articles and drafting an answer…"):
        try:
            from backend.ml.rag_query import RAGQueryEngine
            engine = RAGQueryEngine()
            response = engine.query(question.strip())
        except Exception as exc:
            st.error(
                f"❌ Something went wrong: {exc}\n\n"
                "Make sure ChromaDB is running and news data has been ingested."
            )
            st.stop()

    answer  = response.get("answer", "")
    sources = response.get("sources", [])

    # ── Answer display ─────────────────────────────────────────────────────────
    if answer == "Insufficient data":
        st.warning(
            "**We couldn't find relevant articles to answer this question.**\n\n"
            "This happens when:\n"
            "- The topic isn't covered in our news database yet\n"
            "- The question is too specific or uses unusual phrasing\n\n"
            "**Try:** rephrasing the question, or ask about one of the example topics above."
        )
    else:
        st.markdown("### 💬 Answer")
        st.markdown(f'<div class="answer-box">{answer}</div>', unsafe_allow_html=True)

        # ── Source articles ────────────────────────────────────────────────────
        if sources:
            st.markdown("<br>", unsafe_allow_html=True)
            st.markdown(f"**📚 Based on {len(sources)} source article(s):**")
            for i, source in enumerate(sources, 1):
                url = source.get("url", "")
                pub = source.get("published_at", "")
                tkr = source.get("ticker", "")

                try:
                    from datetime import datetime
                    pub_dt  = datetime.fromisoformat(str(pub).replace("Z", "+00:00"))
                    pub_str = pub_dt.strftime("%d %b %Y")
                except Exception:
                    pub_str = str(pub)[:10] if pub else "Unknown date"

                st.markdown(
                    f'<div class="source-card">'
                    f'<b>Source {i}</b>'
                    f'{"  ·  <b>" + tkr + "</b>" if tkr else ""}  '
                    f'·  Published: {pub_str}<br>'
                    f'{"<a href=" + url + " target=_blank>" + url[:80] + ("…" if len(url) > 80 else "") + "</a>" if url else "Link unavailable"}'
                    f'</div>',
                    unsafe_allow_html=True,
                )
        else:
            st.caption("No source article links available for this response.")

        # ── Disclaimer ─────────────────────────────────────────────────────────
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown(
            '<div style="color:#aaa;font-size:0.78rem;">'
            "⚠️ This answer is generated by AI based on news articles and is for informational "
            "purposes only. It is not financial advice. Always verify with authoritative sources "
            "before making any investment decision."
            "</div>",
            unsafe_allow_html=True,
        )

# ── Empty state ────────────────────────────────────────────────────────────────
else:
    st.markdown("""
<div style="text-align:center;padding:2rem 0;color:#666;">
    <div style="font-size:3rem;">🔍</div>
    <p style="font-size:1rem;margin-top:0.5rem;">
        Ask a question above and our AI will search through financial news to answer it.
    </p>
    <p style="font-size:0.85rem;color:#aaa;">
        Best for: recent company news, earnings updates, analyst views, sector developments.
    </p>
</div>
    """, unsafe_allow_html=True)
