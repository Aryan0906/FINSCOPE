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
    Fetch OHLCV data from NSE via jugaad-data.

    Args:
        symbol: NSE symbol (with or without .NS suffix)
        start: Start date (inclusive)
        end: End date (inclusive)
        retries: Number of retry attempts (default 3)

    Returns:
        DataFrame with columns: Date, Open, High, Low, Close, Adj Close, Volume
        Returns empty DataFrame on total failure — never raises.
    """
    from jugaad_data.nse import stock_df

    # Strip .NS suffix and uppercase
    symbol_clean = symbol.replace(".NS", "").upper()
    logger.info("Fetching NSE prices for %s [%s → %s]", symbol_clean, start, end)

    for attempt in range(1, retries + 1):
        try:
            df = stock_df(
                symbol=symbol_clean,
                from_date=start,
                to_date=end,
                series="EQ",
            )

            if df is None or df.empty:
                logger.warning("No data returned for %s (attempt %d)", symbol_clean, attempt)
                sleep_time = 2 ** attempt  # 2s, 4s, 8s
                time.sleep(sleep_time)
                continue

            # Rename columns from jugaad-data format to standard OHLCV
            # jugaad-data stock_df returns CH_* prefixed columns
            rename_map = {
                "CH_TIMESTAMP": "Date",
                "CH_OPENING_PRICE": "Open",
                "CH_HIGH_PRICE": "High",
                "CH_LOW_PRICE": "Low",
                "CH_CLOSING_PRICE": "Close",
                "CH_TOT_TRADED_QTY": "Volume",
                "CH_LAST_TRADED_PRICE": "Adj Close",
            }
            df = df.rename(columns=rename_map)

            # If Adj Close not present after rename, copy Close
            if "Adj Close" not in df.columns and "Close" in df.columns:
                df["Adj Close"] = df["Close"]

            # Sort ascending by Date
            if "Date" in df.columns:
                df = df.sort_values("Date", ascending=True).reset_index(drop=True)

            # Cast all price columns to numeric
            price_cols = ["Open", "High", "Low", "Close", "Adj Close"]
            for col in price_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            # Cast Volume: fillna(0) then int
            if "Volume" in df.columns:
                df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce").fillna(0).astype(int)

            # Keep only required columns
            required = ["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"]
            available = [c for c in required if c in df.columns]
            df = df[available]

            # Log success: symbol, row count, latest close price
            latest_close = df["Close"].iloc[-1] if not df.empty and "Close" in df.columns else 0.0
            logger.info(
                "✓ %s: %d rows fetched (latest close: ₹%.2f)",
                symbol_clean, len(df), latest_close
            )
            return df

        except Exception as exc:
            logger.warning(
                "Attempt %d/%d failed for %s: %s",
                attempt, retries, symbol_clean, exc
            )
            if attempt < retries:
                sleep_time = 2 ** attempt  # 2s, 4s, 8s
                time.sleep(sleep_time)
            else:
                logger.error(
                    "All %d attempts failed for %s — returning empty DataFrame",
                    retries, symbol_clean
                )

    # Return empty DataFrame on total failure — never raise
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
