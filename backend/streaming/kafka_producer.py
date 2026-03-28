"""
backend/streaming/kafka_producer.py
====================================
Kafka producer that sends NSE price data to the nse_prices_raw topic.

On startup: fetches last 365 days of history for all tickers.
Then polls: every 60 seconds during market hours, fetches latest 5 days.

Usage:
    python -m backend.streaming.kafka_producer
"""

from __future__ import annotations

import json
import logging
import sys
import time
from datetime import date, datetime, timedelta, timezone
from typing import List

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Add project root to path for imports
sys.path.insert(0, str(__file__).rsplit("backend", 1)[0])

from backend.pipeline.nse_fetcher import get_nse_prices
from backend.pipeline.settings import settings

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

# ─────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────

# Tickers to fetch
TICKERS: List[str] = ["RELIANCE.NS", "TCS.NS", "INFY.NS", "HDFCBANK.NS", "ITC.NS"]

# Kafka topic
TOPIC = settings.kafka_topic_prices

# Polling interval in seconds
POLL_INTERVAL_SECONDS = 60

# Historical lookback on startup
STARTUP_LOOKBACK_DAYS = 365

# Polling lookback during market hours
POLL_LOOKBACK_DAYS = 5


# ─────────────────────────────────────────────────
# Market hours check (IST)
# ─────────────────────────────────────────────────

def is_market_hours() -> bool:
    """
    Check if NSE is currently open.
    Market hours: 9:15 AM – 3:30 PM IST, weekdays only.
    """
    utc_now = datetime.now(timezone.utc)
    # IST = UTC + 5:30
    ist_now = utc_now + timedelta(hours=5, minutes=30)
    
    # Weekdays only (Monday=0, Friday=4)
    if ist_now.weekday() > 4:
        return False
    
    # Market open: 9:15 AM IST
    market_open = ist_now.replace(hour=9, minute=15, second=0, microsecond=0)
    # Market close: 3:30 PM IST
    market_close = ist_now.replace(hour=15, minute=30, second=0, microsecond=0)
    
    return market_open <= ist_now <= market_close


def get_ist_now() -> datetime:
    """Get current time in IST."""
    utc_now = datetime.now(timezone.utc)
    ist_offset = timezone(timedelta(hours=5, minutes=30))
    return utc_now.astimezone(ist_offset)


# ─────────────────────────────────────────────────
# Message serialization
# ─────────────────────────────────────────────────

def row_to_message(ticker: str, row: pd.Series) -> dict:
    """
    Convert a DataFrame row to the JSON message format.
    
    Output format:
    {
        "ticker": "RELIANCE.NS",
        "trade_date": "2024-03-28",
        "open": 2800.50,
        "high": 2850.00,
        "low": 2790.00,
        "close": 2830.75,
        "adj_close": 2830.75,
        "volume": 1250000,
        "ingested_at": "2024-03-28T18:05:00+05:30"
    }
    """
    # Handle Date field - could be date, datetime, or string
    trade_date = row.get("Date")
    if hasattr(trade_date, "strftime"):
        trade_date_str = trade_date.strftime("%Y-%m-%d")
    else:
        trade_date_str = str(trade_date)[:10]
    
    # Current timestamp in IST
    ingested_at = get_ist_now().isoformat()
    
    return {
        "ticker": ticker,
        "trade_date": trade_date_str,
        "open": float(row.get("Open", 0)) if pd.notna(row.get("Open")) else 0.0,
        "high": float(row.get("High", 0)) if pd.notna(row.get("High")) else 0.0,
        "low": float(row.get("Low", 0)) if pd.notna(row.get("Low")) else 0.0,
        "close": float(row.get("Close", 0)) if pd.notna(row.get("Close")) else 0.0,
        "adj_close": float(row.get("Adj Close", 0)) if pd.notna(row.get("Adj Close")) else 0.0,
        "volume": int(row.get("Volume", 0)) if pd.notna(row.get("Volume")) else 0,
        "ingested_at": ingested_at,
    }


# ─────────────────────────────────────────────────
# Kafka Producer Class
# ─────────────────────────────────────────────────

class NSEKafkaProducer:
    """
    Kafka producer for NSE price data.
    
    - On startup: fetches last 365 days of history for all tickers
    - Then polls: every 60 seconds during market hours
    - Outside market hours: sleeps and logs
    """
    
    def __init__(self):
        self.bootstrap_servers = settings.kafka_bootstrap_servers
        self.topic = TOPIC
        self.producer = None
        self._sent_keys: set = set()  # Track sent (ticker, date) to avoid duplicates
    
    def _create_producer(self) -> KafkaProducer:
        """Create and configure Kafka producer."""
        return KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=5,
            enable_idempotence=True,
        )
    
    def _ensure_producer(self):
        """Ensure producer is connected."""
        if self.producer is None:
            logger.info("Connecting to Kafka at %s", self.bootstrap_servers)
            self.producer = self._create_producer()
            logger.info("✓ Kafka producer connected")
    
    def send_prices(self, ticker: str, df: pd.DataFrame) -> int:
        """
        Send price data for a ticker to Kafka.
        Returns number of messages sent.
        """
        if df.empty:
            logger.warning("Empty DataFrame for %s — skipping", ticker)
            return 0
        
        self._ensure_producer()
        sent_count = 0
        
        for _, row in df.iterrows():
            try:
                message = row_to_message(ticker, row)
                
                # Dedup key: (ticker, trade_date)
                dedup_key = (ticker, message["trade_date"])
                if dedup_key in self._sent_keys:
                    continue
                
                # Send to Kafka
                future = self.producer.send(
                    self.topic,
                    key=ticker,
                    value=message,
                )
                # Wait for send to complete (with timeout)
                future.get(timeout=10)
                
                self._sent_keys.add(dedup_key)
                sent_count += 1
                
            except KafkaError as e:
                logger.error("Kafka send failed for %s: %s", ticker, e)
                # Continue to next row — don't crash
                continue
            except Exception as e:
                logger.error("Unexpected error sending %s: %s", ticker, e)
                continue
        
        if sent_count > 0:
            logger.info("✓ Sent %d messages for %s to %s", sent_count, ticker, self.topic)
        
        return sent_count
    
    def fetch_and_send_historical(self):
        """Fetch and send last 365 days of history for all tickers."""
        logger.info("=" * 60)
        logger.info("STARTUP: Fetching %d days of historical data", STARTUP_LOOKBACK_DAYS)
        logger.info("=" * 60)
        
        end_date = date.today()
        start_date = end_date - timedelta(days=STARTUP_LOOKBACK_DAYS)
        
        total_sent = 0
        for ticker in TICKERS:
            logger.info("Fetching history for %s [%s → %s]", ticker, start_date, end_date)
            df = get_nse_prices(ticker, start_date, end_date)
            sent = self.send_prices(ticker, df)
            total_sent += sent
            # Small delay between tickers to avoid rate limiting
            time.sleep(1)
        
        logger.info("=" * 60)
        logger.info("STARTUP COMPLETE: %d total messages sent", total_sent)
        logger.info("=" * 60)
    
    def fetch_and_send_latest(self):
        """Fetch and send latest 5 days for all tickers."""
        end_date = date.today()
        start_date = end_date - timedelta(days=POLL_LOOKBACK_DAYS)
        
        total_sent = 0
        for ticker in TICKERS:
            df = get_nse_prices(ticker, start_date, end_date)
            sent = self.send_prices(ticker, df)
            total_sent += sent
        
        if total_sent > 0:
            logger.info("Poll cycle: %d new messages sent", total_sent)
    
    def run(self):
        """
        Main run loop.
        - On startup: fetch historical data
        - Then poll every 60 seconds during market hours
        - Outside market hours: log and sleep
        """
        logger.info("NSE Kafka Producer starting...")
        logger.info("Tickers: %s", TICKERS)
        logger.info("Kafka: %s → topic: %s", self.bootstrap_servers, self.topic)
        
        # Startup: fetch historical data
        self.fetch_and_send_historical()
        
        # Main polling loop
        logger.info("Entering polling loop (every %d seconds)...", POLL_INTERVAL_SECONDS)
        
        while True:
            try:
                if is_market_hours():
                    logger.info("Market is OPEN — fetching latest prices")
                    self.fetch_and_send_latest()
                else:
                    logger.info("Market closed. Next check in %ds.", POLL_INTERVAL_SECONDS)
                
                time.sleep(POLL_INTERVAL_SECONDS)
                
            except KeyboardInterrupt:
                logger.info("Shutdown requested — exiting")
                break
            except Exception as e:
                logger.error("Error in polling loop: %s", e)
                time.sleep(POLL_INTERVAL_SECONDS)
        
        # Cleanup
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer closed")


# ─────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────

if __name__ == "__main__":
    producer = NSEKafkaProducer()
    producer.run()
