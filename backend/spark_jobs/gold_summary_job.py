"""
backend/spark_jobs/gold_summary_job.py
=========================================
PySpark batch job: Delta Lake Silver → PostgreSQL gold.stock_summary.

Reads Silver Delta table, computes latest snapshot per ticker,
writes to PostgreSQL gold.stock_summary via JDBC.
Idempotent: uses ON CONFLICT DO UPDATE (via mode="overwrite" on a temp
staging table + MERGE SQL executed via psycopg2).

Usage (standalone):
    spark-submit \
      --packages io.delta:delta-spark_2.12:3.0.0,org.postgresql:postgresql:42.6.0 \
      backend/spark_jobs/gold_summary_job.py
"""

from __future__ import annotations

import logging
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | gold_summary | %(message)s",
)


def _get_config() -> dict:
    try:
        from backend.pipeline.settings import settings
        return {
            "silver":       settings.silver_prices_path,
            "spark_master": settings.spark_master_url,
            "jdbc_url":     f"jdbc:postgresql://{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}",
            "jdbc_user":    settings.postgres_user,
            "jdbc_password":settings.postgres_password,
        }
    except ImportError:
        return {
            "silver":        "/opt/delta-lake/silver/stock_prices",
            "spark_master":  "spark://spark-master:7077",
            "jdbc_url":      "jdbc:postgresql://postgres:5432/finscope",
            "jdbc_user":     "finscope_admin",
            "jdbc_password": os.getenv("POSTGRES_PASSWORD", ""),
        }


def _get_spark(master: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName("finscope-gold-summary")
        .master(master)
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.13:4.0.0,"
                "org.postgresql:postgresql:42.6.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def build_gold_summary(spark: SparkSession, silver_path: str):
    """
    Read Silver, take the latest row per ticker, return Gold summary DataFrame.
    """
    logger.info("Reading Silver: %s", silver_path)
    silver_df = spark.read.format("delta").load(silver_path)

    # Latest row per ticker (DISTINCT ON equivalent in PySpark)
    window = Window.partitionBy("ticker").orderBy(F.col("trade_date").desc())
    latest_df = (
        silver_df
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .select(
            F.col("ticker").alias("symbol"),
            F.col("trade_date").alias("as_of_date"),
            F.col("close").alias("close_price"),
            F.col("daily_return"),
            F.col("normalised_close"),
            F.col("sma_20"),
            F.col("sma_50"),
            F.col("rsi_14"),
        )
    )

    count = latest_df.count()
    logger.info("Gold summary rows: %d tickers", count)
    return latest_df


def write_to_postgres(df, jdbc_url: str, user: str, password: str) -> int:
    """
    Write gold summary to PostgreSQL staging table, then upsert into
    gold.stock_summary via psycopg2 MERGE.
    """
    staging_table = "gold.stock_summary_staging"

    logger.info("Writing to PostgreSQL staging: %s", staging_table)
    (
        df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", staging_table)
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )

    # Upsert from staging → gold.stock_summary via psycopg2
    import psycopg2
    from urllib.parse import urlparse

    parsed = urlparse(jdbc_url.replace("jdbc:", ""))
    conn = psycopg2.connect(
        host=parsed.hostname,
        port=parsed.port or 5432,
        dbname=parsed.path.lstrip("/"),
        user=user,
        password=password,
        connect_timeout=10,
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        # Upsert from staging → gold.stock_summary
        cur.execute("""
            INSERT INTO gold.stock_summary (
                symbol, as_of_date, close_price, daily_return,
                normalised_close, sma_20, sma_50, rsi_14, updated_at
            )
            SELECT
                symbol, as_of_date, close_price, daily_return,
                normalised_close, sma_20, sma_50, rsi_14, NOW()
            FROM gold.stock_summary_staging
            ON CONFLICT (symbol) DO UPDATE SET
                as_of_date       = EXCLUDED.as_of_date,
                close_price      = EXCLUDED.close_price,
                daily_return     = EXCLUDED.daily_return,
                normalised_close = EXCLUDED.normalised_close,
                sma_20           = EXCLUDED.sma_20,
                sma_50           = EXCLUDED.sma_50,
                rsi_14           = EXCLUDED.rsi_14,
                updated_at       = NOW();
        """)
        cur.execute("DROP TABLE IF EXISTS gold.stock_summary_staging;")
    conn.close()

    count = df.count()
    logger.info("Gold stock_summary upserted: %d rows", count)
    return count


def run() -> int:
    config = _get_config()
    spark = _get_spark(config["spark_master"])
    spark.sparkContext.setLogLevel("WARN")

    try:
        gold_df = build_gold_summary(spark, config["silver"])
        count = write_to_postgres(
            gold_df,
            config["jdbc_url"],
            config["jdbc_user"],
            config["jdbc_password"],
        )
        logger.info("gold_summary_job complete: %d rows", count)
        return count
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
