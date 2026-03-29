"""
backend/pipeline/nse_fetcher.py
================================
Fetches NSE prices and fundamentals using jugaad-data ONLY.
No requests.Session. No cookies. No direct scraping.

Usage:
    from backend.pipeline.nse_fetcher import get_nse_prices, get_nse_fundamentals
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta
from typing import Optional, Dict, Any

import pandas as pd

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────
# Price fetcher
# ─────────────────────────────────────────────────

def get_nse_prices(
    symbol: str,
    start: date,
    end: date,
    retries: int = 3,
) -> pd.DataFrame:
    """
    Fetch OHLCV data via yfinance (fallback due to API breakage in jugaad-data).
    """
    try:
        import yfinance as yf
    except ImportError:
        logger.error("yfinance not installed")
        return pd.DataFrame()

    symbol_clean = symbol.replace(".NS", "").upper()
    yf_symbol = symbol_clean + ".NS"
    logger.info("Fetching yfinance prices for %s [%s → %s]", yf_symbol, start, end)

    from datetime import timedelta
    for attempt in range(1, retries + 1):
        try:
            df = yf.download(yf_symbol, period="2y", progress=False)

            if df is None or df.empty:
                logger.warning("No data returned for %s (attempt %d)", yf_symbol, attempt)
                import time
                time.sleep(2 ** attempt)
                continue

            df = df.reset_index()
            if hasattr(pd, "MultiIndex") and isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            rename_map = {"Datetime": "Date", "index": "Date"}
            df = df.rename(columns=rename_map)

            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"]).dt.date

            required = ["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"]
            for col in required:
                if col not in df.columns and col == "Adj Close" and "Close" in df.columns:
                    df["Adj Close"] = df["Close"]

            available = [c for c in required if c in df.columns]
            df = df[available]

            latest_close = float(df["Close"].iloc[-1]) if not df.empty and "Close" in df.columns else 0.0
            logger.info("✓ %s: %d rows fetched (latest close: ₹%.2f)", symbol_clean, len(df), latest_close)
            return df

        except Exception as exc:
            logger.warning("Attempt %d/%d failed for %s: %s", attempt, retries, yf_symbol, exc)
            import time
            if attempt < retries:
                time.sleep(2 ** attempt)
            else:
                logger.error("All %d attempts failed for %s", retries, yf_symbol)

    return pd.DataFrame()


# ─────────────────────────────────────────────────
# Safe float conversion helper
# ─────────────────────────────────────────────────

def _f(val) -> Optional[float]:
    """Safe float conversion. Handles None, '-', and empty string."""
    if val is None or val == "" or val == "-":
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


# ─────────────────────────────────────────────────
# Fundamentals fetcher
# ─────────────────────────────────────────────────

def get_nse_fundamentals(
    symbol: str,
    retries: int = 3,
) -> Optional[Dict[str, Any]]:
    """
    Fetch fundamental metrics from NSE via jugaad-data.

    Args:
        symbol: NSE symbol (with or without .NS suffix)
        retries: Number of retry attempts (default 3)

    Returns:
        Dict of metric_name → float value, or None on total failure.
        None values are filtered out before returning.
    """
    from jugaad_data.nse import NSELive

    # Strip .NS suffix and uppercase
    symbol_clean = symbol.replace(".NS", "").upper()
    logger.info("Fetching NSE fundamentals for %s", symbol_clean)

    for attempt in range(1, retries + 1):
        try:
            nse_live = NSELive()
            quote = nse_live.stock_quote(symbol_clean)

            if not quote:
                logger.warning("Empty quote for %s (attempt %d)", symbol_clean, attempt)
                sleep_time = 2 ** attempt
                time.sleep(sleep_time)
                continue

            # Extract nested structures
            price_info = quote.get("priceInfo", {})
            security_info = quote.get("securityInfo", {})
            metadata = quote.get("metadata", {})
            trade_info = quote.get("tradeInfo", {})
            week_hl = price_info.get("weekHighLow", {})

            fundamentals: Dict[str, Any] = {
                # FLAG: verify key path on your jugaad-data version before Sprint 3
                "high52": _f(week_hl.get("max")),

                # FLAG: verify key path on your jugaad-data version before Sprint 3
                "low52": _f(week_hl.get("min")),

                # FLAG: verify key path on your jugaad-data version before Sprint 3
                "pChange365d": _f(price_info.get("pChange365d")),

                # FLAG: verify key path on your jugaad-data version before Sprint 3
                "pChange30d": _f(price_info.get("pChange30d")),

                # FLAG: verify key path on your jugaad-data version before Sprint 3
                "pe": _f(price_info.get("pe")),

                # FLAG: verify key path on your jugaad-data version before Sprint 3
                "eps": _f(price_info.get("eps")),

                # FLAG: verify key path on your jugaad-data version before Sprint 3
                "bookValue": _f(price_info.get("bookValue")),

                # FLAG: verify key path on your jugaad-data version before Sprint 3
                "priceToBook": _f(price_info.get("pbRatio")),

                # FLAG: verify key path on your jugaad-data version before Sprint 3
                "marketCap": _f(metadata.get("marketCap")),

                # FLAG: verify key path on your jugaad-data version before Sprint 3
                "faceValue": _f(security_info.get("faceValue")),

                # FLAG: verify key path on your jugaad-data version before Sprint 3
                "industryPE": _f(metadata.get("pdSectorInd")),

                # FLAG: verify key path on your jugaad-data version before Sprint 3
                "deliveryToTradedQty": _f(trade_info.get("deliveryToTradedQuantity")),
            }

            # Filter out None values before returning
            fundamentals = {k: v for k, v in fundamentals.items() if v is not None}

            logger.info(
                "✓ %s: %d fundamental metrics fetched",
                symbol_clean, len(fundamentals)
            )
            return fundamentals

        except Exception as exc:
            logger.warning(
                "Fundamentals attempt %d/%d failed for %s: %s",
                attempt, retries, symbol_clean, exc
            )
            if attempt < retries:
                sleep_time = 2 ** attempt  # 2s, 4s, 8s
                time.sleep(sleep_time)
            else:
                logger.error(
                    "All %d fundamentals attempts failed for %s — returning None",
                    retries, symbol_clean
                )

    # Return None on total failure
    return None
