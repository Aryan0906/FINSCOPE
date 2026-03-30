"""
frontend/components/live_quote.py
===================================
Fetches live / delayed market data directly from yfinance for the dashboard.

Why this exists:
  The Airflow pipeline runs once daily at 6:30 PM IST, so during market hours
  (9:15 AM – 3:30 PM) the DB has yesterday's closing price. This module
  overlays real-time data on top of the DB snapshot without touching the
  pipeline at all.

What it provides:
  - Live price (15-min delayed via yfinance) during market hours
  - Fundamentals (marketCap, PE, P/B) which aren't stored in silver.fundamentals
  - 52-week high/low as a fallback when DB values are NULL

Cached for 60 seconds (st.cache_data) to avoid hammering yfinance on every
Streamlit rerun.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

import streamlit as st

logger = logging.getLogger(__name__)

NSE_SUFFIX = ".NS"

# Mapping from dashboard symbol (no suffix) → yfinance ticker
def _yf_ticker(symbol: str) -> str:
    sym = symbol.upper().replace(".NS", "")
    return f"{sym}{NSE_SUFFIX}"


def _ist_now() -> datetime:
    return datetime.now(timezone(timedelta(hours=5, minutes=30)))


def _market_open() -> bool:
    ist = _ist_now()
    if ist.weekday() > 4:
        return False
    open_t  = ist.replace(hour=9,  minute=15, second=0, microsecond=0)
    close_t = ist.replace(hour=15, minute=30, second=0, microsecond=0)
    return open_t <= ist <= close_t


@st.cache_data(ttl=60, show_spinner=False)
def get_live_quote(symbol: str) -> dict:
    """
    Return a dict with live/delayed price and fundamentals from yfinance.
    Cached for 60 seconds.  Returns {} on failure (never raises).

    Keys returned (all optional — check with .get()):
      live_price        float  — current market price (15-min delayed)
      prev_close        float  — previous day's official close
      change_pct        float  — intraday % change vs prev_close
      market_cap        float  — in INR
      trailing_pe       float
      price_to_book     float
      fifty_two_week_high float
      fifty_two_week_low  float
      dividend_yield    float
      is_live           bool   — True if market is currently open
    """
    try:
        import yfinance as yf
        ticker_str = _yf_ticker(symbol)
        t = yf.Ticker(ticker_str)
        info = t.info  # one network call — cached by yf internally

        live_price  = info.get("currentPrice") or info.get("regularMarketPrice")
        prev_close  = info.get("previousClose") or info.get("regularMarketPreviousClose")
        change_pct  = None
        if live_price and prev_close and prev_close != 0:
            change_pct = (live_price - prev_close) / prev_close * 100

        return {
            "live_price":          live_price,
            "prev_close":          prev_close,
            "change_pct":          change_pct,
            "market_cap":          info.get("marketCap"),
            "trailing_pe":         info.get("trailingPE"),
            "price_to_book":       info.get("priceToBook"),
            "fifty_two_week_high": info.get("fiftyTwoWeekHigh"),
            "fifty_two_week_low":  info.get("fiftyTwoWeekLow"),
            "dividend_yield":      info.get("dividendYield"),
            "is_live":             _market_open(),
        }

    except Exception as exc:
        logger.warning("yfinance quote failed for %s: %s", symbol, exc)
        return {}
