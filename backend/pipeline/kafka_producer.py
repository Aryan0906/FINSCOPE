"""
backend/pipeline/kafka_producer.py
===================================
Kafka producer for NSE price data.
Fetches data via jugaad-data and publishes to Kafka topic.

Sprint 2: NSE prices → Kafka → Spark Streaming → Delta Lake Bronze

Usage:
    python -m backend.pipeline.kafka_producer

Environment:
    KAFKA_BOOTSTRAP_SERVERS: kafka:9092 (inside container) or localhost:9092 (host)
    NSE_SYMBOLS: comma-separated list of NSE symbols
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import date, datetime, timedelta, timezone
from typing import List, Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError

from backend.pipeline.nse_fetcher import get_nse_prices
from backend.pipeline.settings import settings

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

TOPIC_NSE_PRICES = "nse_prices_raw"
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# IST timezone offset: UTC+5:30
IST_OFFSET = timedelta(hours=5, minutes=30)


# ─────────────────────────────────────────────────────────────────────────────
# Market hours logic (IST)
# ─────────────────────────────────────────────────────────────────────────────

def get_ist_now() -> datetime:
    """Get current time in IST (Indian Standard Time, UTC+5:30)."""
    utc_now = datetime.now(timezone.utc)
    # Create IST timezone
    ist_tz = timezone(IST_OFFSET)
    return utc_now.astimezone(ist_tz)


def is_market_hours() -> bool:
    """
    Check if NSE market is currently open.
    
    NSE Trading Hours:
    - Pre-open: 9:00 AM - 9:15 AM IST
    - Normal trading: 9:15 AM - 3:30 PM IST
    - Closing session: 3:30 PM - 4:00 PM IST
    
    For data fetching, we consider 9:00 AM - 4:00 PM IST on weekdays.
    """
    ist_now = get_ist_now()
    
    # Check if weekday (Monday=0, Sunday=6)
    if ist_now.weekday() >= 5:  # Saturday or Sunday
        return False
    
    # Market hours: 9:00 AM to 4:00 PM IST
    market_open = ist_now.replace(hour=9, minute=0, second=0, microsecond=0)
    market_close = ist_now.replace(hour=16, minute=0, second=0, microsecond=0)
    
    return market_open <= ist_now <= market_close


def is_trading_day(check_date: date) -> bool:
    """
    Check if a given date is a trading day (weekday, not holiday).
    
    Note: This does not account for NSE holidays (Diwali, Republic Day, etc.)
    A production system should use a holiday calendar.
    """
    # weekday() returns 0 for Monday, 6 for Sunday
    return check_date.weekday() < 5


def get_last_trading_day() -> date:
    """Get the most recent trading day (today if market is open, else previous weekday)."""
    today = get_ist_now().date()
    
    # If today is a weekday and market has data, use today
    if is_trading_day(today):
        return today
    
    # Otherwise, find the previous weekday
    check = today
    while not is_trading_day(check):
        check -= timedelta(days=1)
    
    return check


# ─────────────────────────────────────────────────────────────────────────────
# Kafka producer setup
# ─────────────────────────────────────────────────────────────────────────────

def create_producer() -> KafkaProducer:
    """Create and return a Kafka producer with JSON serialization."""
    logger.info("Connecting to Kafka at %s", BOOTSTRAP_SERVERS)
    
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",  # Wait for all replicas
        retries=3,
        retry_backoff_ms=1000,
        max_in_flight_requests_per_connection=1,  # Ensure ordering
    )
    
    logger.info("✓ Kafka producer connected")
    return producer


# ─────────────────────────────────────────────────────────────────────────────
# Message publishing
# ─────────────────────────────────────────────────────────────────────────────

def publish_price_record(
    producer: KafkaProducer,
    symbol: str,
    record: dict,
) -> bool:
    """
    Publish a single price record to Kafka.
    
    Message format:
    {
        "symbol": "RELIANCE",
        "date": "2026-03-28",
        "open": 2450.50,
        "high": 2475.00,
        "low": 2445.00,
        "close": 2468.75,
        "adj_close": 2468.75,
        "volume": 5234567,
        "ingested_at": "2026-03-28T12:30:45.123456+05:30"
    }
    """
    try:
        message = {
            "symbol": symbol.upper(),
            "date": str(record.get("Date", "")),
            "open": float(record.get("Open", 0)),
            "high": float(record.get("High", 0)),
            "low": float(record.get("Low", 0)),
            "close": float(record.get("Close", 0)),
            "adj_close": float(record.get("Adj Close", record.get("Close", 0))),
            "volume": int(record.get("Volume", 0)),
            "ingested_at": get_ist_now().isoformat(),
        }
        
        # Use symbol as partition key for ordering
        future = producer.send(
            TOPIC_NSE_PRICES,
            key=symbol.upper(),
            value=message,
        )
        
        # Block until sent (with timeout)
        future.get(timeout=10)
        return True
        
    except KafkaError as e:
        logger.error("Failed to publish %s record: %s", symbol, e)
        return False
    except Exception as e:
        logger.error("Unexpected error publishing %s: %s", symbol, e)
        return False


def fetch_and_publish(
    producer: KafkaProducer,
    symbols: List[str],
    lookback_days: int = 7,
) -> dict:
    """
    Fetch price data for symbols and publish to Kafka.
    
    Returns:
        dict with success/failure counts per symbol
    """
    results = {}
    end_date = get_last_trading_day()
    start_date = end_date - timedelta(days=lookback_days)
    
    logger.info(
        "Fetching prices for %d symbols [%s → %s]",
        len(symbols), start_date, end_date
    )
    
    for symbol in symbols:
        symbol_clean = symbol.replace(".NS", "").upper()
        
        try:
            df = get_nse_prices(symbol_clean, start_date, end_date)
            
            if df.empty:
                logger.warning("No data for %s", symbol_clean)
                results[symbol_clean] = {"status": "no_data", "records": 0}
                continue
            
            # Publish each row
            success_count = 0
            for _, row in df.iterrows():
                if publish_price_record(producer, symbol_clean, row.to_dict()):
                    success_count += 1
            
            results[symbol_clean] = {
                "status": "success",
                "records": success_count,
                "total": len(df),
            }
            logger.info(
                "✓ Published %d/%d records for %s",
                success_count, len(df), symbol_clean
            )
            
            # Rate limiting: 1 second between symbols
            time.sleep(1)
            
        except Exception as e:
            logger.error("Failed to process %s: %s", symbol_clean, e)
            results[symbol_clean] = {"status": "error", "error": str(e)}
    
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────────────

def run_producer(
    symbols: Optional[List[str]] = None,
    lookback_days: int = 7,
    continuous: bool = False,
    interval_minutes: int = 15,
) -> None:
    """
    Run the Kafka producer.
    
    Args:
        symbols: List of NSE symbols. Defaults to settings.nse_symbols.
        lookback_days: Number of days of historical data to fetch.
        continuous: If True, run continuously during market hours.
        interval_minutes: Minutes between fetches in continuous mode.
    """
    if symbols is None:
        symbols = settings.nse_symbols
    
    if not symbols:
        logger.error("No symbols configured. Set NSE_SYMBOLS env var.")
        return
    
    logger.info("Starting NSE price producer for %d symbols", len(symbols))
    logger.info("Kafka bootstrap: %s", BOOTSTRAP_SERVERS)
    logger.info("Topic: %s", TOPIC_NSE_PRICES)
    
    producer = create_producer()
    
    try:
        if continuous:
            # Continuous mode: run during market hours
            while True:
                if is_market_hours():
                    logger.info("Market is open. Fetching prices...")
                    results = fetch_and_publish(producer, symbols, lookback_days)
                    
                    total_records = sum(
                        r.get("records", 0) for r in results.values()
                    )
                    logger.info(
                        "Batch complete: %d total records published",
                        total_records
                    )
                else:
                    logger.info(
                        "Market closed. IST time: %s. Sleeping...",
                        get_ist_now().strftime("%Y-%m-%d %H:%M:%S")
                    )
                
                # Wait for next interval
                time.sleep(interval_minutes * 60)
        else:
            # One-shot mode
            results = fetch_and_publish(producer, symbols, lookback_days)
            
            total_records = sum(r.get("records", 0) for r in results.values())
            success_symbols = sum(
                1 for r in results.values() if r.get("status") == "success"
            )
            
            logger.info(
                "Complete: %d symbols, %d records published",
                success_symbols, total_records
            )
            
    finally:
        producer.flush()
        producer.close()
        logger.info("Producer closed")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    
    parser = argparse.ArgumentParser(description="NSE Price Kafka Producer")
    parser.add_argument(
        "--symbols",
        type=str,
        help="Comma-separated list of symbols (overrides NSE_SYMBOLS)",
    )
    parser.add_argument(
        "--lookback",
        type=int,
        default=7,
        help="Days of historical data to fetch (default: 7)",
    )
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Run continuously during market hours",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=15,
        help="Minutes between fetches in continuous mode (default: 15)",
    )
    
    args = parser.parse_args()
    
    symbols = None
    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(",")]
    
    run_producer(
        symbols=symbols,
        lookback_days=args.lookback,
        continuous=args.continuous,
        interval_minutes=args.interval,
    )
