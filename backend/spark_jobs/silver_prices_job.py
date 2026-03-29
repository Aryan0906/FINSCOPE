"""
backend/spark_jobs/silver_prices_job.py
=========================================
PySpark batch job: Delta Lake Bronze → Delta Lake Silver (prices).

Reads partitioned parquet from /opt/delta-lake/bronze/stock_prices,
applies technical indicators and quality rules, writes Silver Delta table.

Rules enforced:
  Rule 4 : 52-week normalisation via rolling window
  Rule 7 : Z-score outlier flag (|z| > 4) via applyInPandas
  
Idempotent: uses Delta MERGE on (ticker, trade_date) — safe to re-run.

Usage (standalone):
    spark-submit \
      --packages io.delta:delta-spark_2.12:3.0.0 \
      backend/spark_jobs/silver_prices_job.py

Usage (from Python):
    from backend.spark_jobs.silver_prices_job import run
    run()
"""

from __future__ import annotations

import logging
import math
import os
from datetime import date

import pandas as pd
from pyspark.sql import SparkSession, Window, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DateType, DoubleType, LongType, BooleanType,
)

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | silver_prices | %(message)s",
)

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

def _get_paths() -> dict:
    try:
        from backend.pipeline.settings import settings
        return {
            "bronze": settings.bronze_prices_path,
            "silver": settings.silver_prices_path,
            "checkpoint": f"{settings.delta_lake_base_path}/checkpoints/silver_prices",
            "spark_master": settings.spark_master_url,
        }
    except ImportError:
        return {
            "bronze": "/opt/delta-lake/bronze/stock_prices",
            "silver": "/opt/delta-lake/silver/stock_prices",
            "checkpoint": "/opt/delta-lake/checkpoints/silver_prices",
            "spark_master": "spark://spark-master:7077",
        }


# ─────────────────────────────────────────────────────────────────────────────
# Spark session
# ─────────────────────────────────────────────────────────────────────────────

def _get_spark(master: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName("finscope-silver-prices")
        .master(master)
        .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────────────────────
# SMA / EMA / RSI / normalisation via applyInPandas
#
# Why applyInPandas for EMA and RSI:
#   EMA is recursive (each value depends on previous EMA, not just a window).
#   RSI requires computing avg_gain / avg_loss over a rolling period, then
#   applying Wilder smoothing — also recursive. Neither maps cleanly to
#   PySpark's static window functions. applyInPandas gives us pandas semantics
#   per-ticker without pulling all data to the driver.
# ─────────────────────────────────────────────────────────────────────────────

# Output schema for the applyInPandas UDF
_ENRICHED_SCHEMA = StructType([
    StructField("ticker",         StringType(),  False),
    StructField("trade_date",     DateType(),    False),
    StructField("open",           DoubleType(),  True),
    StructField("high",           DoubleType(),  True),
    StructField("low",            DoubleType(),  True),
    StructField("close",          DoubleType(),  True),
    StructField("adj_close",      DoubleType(),  True),
    StructField("volume",         LongType(),    True),
    # Returns
    StructField("daily_return",   DoubleType(),  True),
    StructField("log_return",     DoubleType(),  True),
    StructField("is_outlier",     BooleanType(), True),
    # Moving averages
    StructField("sma_20",         DoubleType(),  True),
    StructField("sma_50",         DoubleType(),  True),
    StructField("sma_200",        DoubleType(),  True),
    StructField("ema_20",         DoubleType(),  True),
    StructField("rsi_14",         DoubleType(),  True),
    # 52-week
    StructField("high_52w",       DoubleType(),  True),
    StructField("low_52w",        DoubleType(),  True),
    StructField("price_range_52w",DoubleType(),  True),
    StructField("normalised_close",DoubleType(), True),
])


def _enrich_ticker_group(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Per-ticker enrichment function applied via applyInPandas.
    Receives all rows for one ticker, sorted ascending by trade_date.
    Returns the same rows with indicator columns added.
    """
    pdf = pdf.sort_values("trade_date").reset_index(drop=True)
    closes = pdf["close"].tolist()
    n = len(closes)

    # ── Daily return & log return ────────────────────────────────────────────
    daily_returns = [None] * n
    log_returns   = [None] * n
    for i in range(1, n):
        if closes[i - 1] and closes[i]:
            dr = (closes[i] - closes[i - 1]) / closes[i - 1]
            daily_returns[i] = round(dr, 6)
            if closes[i] > 0 and closes[i - 1] > 0:
                log_returns[i] = round(math.log(closes[i] / closes[i - 1]), 6)

    # ── Z-score outlier (Rule 7) ─────────────────────────────────────────────
    valid = [r for r in daily_returns if r is not None]
    mean_r = sum(valid) / len(valid) if valid else 0.0
    var_r  = sum((r - mean_r) ** 2 for r in valid) / len(valid) if valid else 0.0
    std_r  = math.sqrt(var_r) if var_r > 0 else 1e-9
    is_outlier = [
        bool(abs((r - mean_r) / std_r) > 4.0) if r is not None else False
        for r in daily_returns
    ]

    # ── SMA (simple window average) ──────────────────────────────────────────
    def sma(idx: int, w: int):
        start = max(0, idx - w + 1)
        window = [c for c in closes[start: idx + 1] if c]
        return round(sum(window) / len(window), 4) if len(window) >= w else None

    # ── EMA (recursive multiplier) ───────────────────────────────────────────
    def ema_series(window: int) -> list:
        result = [None] * n
        k = 2.0 / (window + 1)
        seed_idx = window - 1
        if seed_idx >= n:
            return result
        seed_vals = [c for c in closes[:window] if c]
        if len(seed_vals) < window:
            return result
        ema_val = sum(seed_vals) / window
        result[seed_idx] = round(ema_val, 4)
        for i in range(window, n):
            if closes[i]:
                ema_val = closes[i] * k + ema_val * (1 - k)
                result[i] = round(ema_val, 4)
        return result

    ema_20_vals = ema_series(20)

    # ── RSI 14 (Wilder smoothing) ─────────────────────────────────────────────
    def rsi_series(period: int = 14) -> list:
        result = [None] * n
        if n < period + 1:
            return result
        gains, losses = [], []
        for i in range(1, n):
            diff = (closes[i] or 0) - (closes[i - 1] or 0)
            gains.append(max(diff, 0))
            losses.append(max(-diff, 0))
        # Seed averages (simple mean for first period)
        avg_gain = sum(gains[:period]) / period
        avg_loss = sum(losses[:period]) / period
        if avg_loss == 0:
            result[period] = 100.0
        else:
            rs = avg_gain / avg_loss
            result[period] = round(100 - 100 / (1 + rs), 4)
        # Wilder smoothing for subsequent periods
        for i in range(period + 1, n):
            avg_gain = (avg_gain * (period - 1) + gains[i - 1]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i - 1]) / period
            if avg_loss == 0:
                result[i] = 100.0
            else:
                rs = avg_gain / avg_loss
                result[i] = round(100 - 100 / (1 + rs), 4)
        return result

    rsi_14_vals = rsi_series(14)

    # ── 52-week rolling (252 trading days) ──────────────────────────────────
    high_52w_vals     = []
    low_52w_vals      = []
    price_range_vals  = []
    normalised_vals   = []
    for i in range(n):
        start = max(0, i - 251)
        window_c = [c for c in closes[start: i + 1] if c]
        h52 = max(window_c) if window_c else None
        l52 = min(window_c) if window_c else None
        rng = round(h52 - l52, 4) if h52 and l52 else None
        # Rule 4: normalise to [0, 1]
        if h52 and l52 and h52 != l52 and closes[i]:
            norm = round(max(0.0, min(1.0, (closes[i] - l52) / (h52 - l52))), 4)
        else:
            norm = None
        high_52w_vals.append(h52)
        low_52w_vals.append(l52)
        price_range_vals.append(rng)
        normalised_vals.append(norm)

    pdf["daily_return"]    = daily_returns
    pdf["log_return"]      = log_returns
    pdf["is_outlier"]      = is_outlier
    pdf["sma_20"]          = [sma(i, 20)  for i in range(n)]
    pdf["sma_50"]          = [sma(i, 50)  for i in range(n)]
    pdf["sma_200"]         = [sma(i, 200) for i in range(n)]
    pdf["ema_20"]          = ema_20_vals
    pdf["rsi_14"]          = rsi_14_vals
    pdf["high_52w"]        = high_52w_vals
    pdf["low_52w"]         = low_52w_vals
    pdf["price_range_52w"] = price_range_vals
    pdf["normalised_close"]= normalised_vals

    # Cast trade_date back to python date (applyInPandas can lose type)
    pdf["trade_date"] = pd.to_datetime(pdf["trade_date"]).dt.date

    return pdf[[f.name for f in _ENRICHED_SCHEMA]]


# ─────────────────────────────────────────────────────────────────────────────
# Main transform
# ─────────────────────────────────────────────────────────────────────────────

def transform_bronze_to_silver(spark: SparkSession, bronze_path: str) -> DataFrame:
    """Read Bronze Delta Lake, enrich per ticker, return Silver DataFrame."""
    logger.info("Reading Bronze: %s", bronze_path)

    bronze_df = (
        spark.read
        .format("delta")
        .load(bronze_path)
        .select(
            F.col("ticker"),
            F.to_date(F.col("trade_date")).alias("trade_date"),
            F.col("open").cast(DoubleType()),
            F.col("high").cast(DoubleType()),
            F.col("low").cast(DoubleType()),
            F.col("close").cast(DoubleType()),
            F.col("adj_close").cast(DoubleType()),
            F.col("volume").cast(LongType()),
        )
        .filter(F.col("close").isNotNull() & (F.col("close") > 0))
        .dropDuplicates(["ticker", "trade_date"])
    )

    row_count = bronze_df.count()
    logger.info("Bronze rows loaded: %d", row_count)
    if row_count == 0:
        raise ValueError("Bronze Delta Lake is empty — run Kafka producer first.")

    # applyInPandas: sends each ticker's full history to _enrich_ticker_group
    silver_df = bronze_df.groupby("ticker").applyInPandas(
        _enrich_ticker_group,
        schema=_ENRICHED_SCHEMA,
    )

    return silver_df


def write_silver(df: DataFrame, silver_path: str) -> int:
    """
    Write enriched data to Delta Lake Silver using MERGE for idempotency.
    Returns row count written.
    """
    from delta.tables import DeltaTable  # type: ignore[import]

    logger.info("Writing Silver: %s", silver_path)

    # First write: if table doesn't exist yet, create it
    if not DeltaTable.isDeltaTable(df.sparkSession, silver_path):
        logger.info("Silver table does not exist — creating.")
        (
            df.write
            .format("delta")
            .partitionBy("ticker")
            .mode("overwrite")
            .save(silver_path)
        )
    else:
        # Subsequent writes: MERGE on (ticker, trade_date)
        delta_table = DeltaTable.forPath(df.sparkSession, silver_path)
        (
            delta_table.alias("silver")
            .merge(
                df.alias("updates"),
                "silver.ticker = updates.ticker AND silver.trade_date = updates.trade_date",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    count = df.count()
    logger.info("Silver rows written: %d", count)
    return count


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def run() -> int:
    """Callable from Airflow BashOperator or spark-submit."""
    paths = _get_paths()
    spark = _get_spark(paths["spark_master"])
    spark.sparkContext.setLogLevel("WARN")

    try:
        silver_df = transform_bronze_to_silver(spark, paths["bronze"])
        count = write_silver(silver_df, paths["silver"])
        logger.info("silver_prices_job complete: %d rows", count)
        return count
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
