"""
backend/streaming/spark_bronze_consumer.py
============================================
Spark Structured Streaming job that consumes nse_prices_raw topic
and writes to Delta Lake Bronze layer.

Usage:
    spark-submit --packages io.delta:delta-spark_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        backend/streaming/spark_bronze_consumer.py
"""

from __future__ import annotations

import logging
import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    from_json,
    to_date,
    to_timestamp,
    year,
    month,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
)

# Add project root to path for imports
sys.path.insert(0, str(__file__).rsplit("backend", 1)[0])

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

# ─────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────

def get_config() -> dict:
    """Get configuration from settings or defaults."""
    try:
        from backend.pipeline.settings import settings
        return {
            "kafka_bootstrap_servers": getattr(settings, "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            "kafka_topic": getattr(settings, "KAFKA_TOPIC_PRICES", "nse_prices_raw"),
            "spark_master": getattr(settings, "SPARK_MASTER_URL", "spark://spark-master:7077"),
            "bronze_path": getattr(settings, "BRONZE_PRICES_PATH", "/opt/delta-lake/bronze/stock_prices"),
            "delta_base": getattr(settings, "DELTA_LAKE_BASE_PATH", "/opt/delta-lake"),
        }
    except ImportError:
        logger.warning("Settings not available — using defaults")
        return {
            "kafka_bootstrap_servers": "kafka:9092",
            "kafka_topic": "nse_prices_raw",
            "spark_master": "spark://spark-master:7077",
            "bronze_path": "/opt/delta-lake/bronze/stock_prices",
            "delta_base": "/opt/delta-lake",
        }


# ─────────────────────────────────────────────────
# Schema Definition
# ─────────────────────────────────────────────────

# Schema for JSON messages from Kafka
PRICE_SCHEMA = StructType([
    StructField("ticker", StringType(), nullable=False),
    StructField("trade_date", StringType(), nullable=True),  # Cast to DateType after parse
    StructField("open", DoubleType(), nullable=True),
    StructField("high", DoubleType(), nullable=True),
    StructField("low", DoubleType(), nullable=True),
    StructField("close", DoubleType(), nullable=True),
    StructField("adj_close", DoubleType(), nullable=True),
    StructField("volume", LongType(), nullable=True),
    StructField("ingested_at", StringType(), nullable=True),  # Cast to TimestampType after parse
])


# ─────────────────────────────────────────────────
# Spark Bronze Consumer Class
# ─────────────────────────────────────────────────

class SparkBronzeConsumer:
    """
    Spark Structured Streaming consumer for NSE price data.
    
    Reads from Kafka topic nse_prices_raw and writes to Delta Lake Bronze.
    """
    
    def __init__(self):
        self.config = get_config()
        self.spark: SparkSession = None
    
    def _create_spark_session(self) -> SparkSession:
        """Create and configure SparkSession with Delta Lake support."""
        logger.info("Creating SparkSession...")
        logger.info("  Master: %s", self.config["spark_master"])
        logger.info("  Kafka: %s", self.config["kafka_bootstrap_servers"])
        
        spark = (
            SparkSession.builder
            .appName("finscope-bronze-consumer")
            .master(self.config["spark_master"])
            .config(
                "spark.jars.packages",
                "io.delta:delta-spark_2.12:3.0.0,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
            )
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            .config("spark.sql.shuffle.partitions", "4")  # Low for local dev
            .getOrCreate()
        )
        
        spark.sparkContext.setLogLevel("WARN")
        logger.info("✓ SparkSession created")
        return spark
    
    def _read_kafka_stream(self) -> DataFrame:
        """Read streaming data from Kafka topic."""
        logger.info("Subscribing to Kafka topic: %s", self.config["kafka_topic"])
        
        return (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.config["kafka_bootstrap_servers"])
            .option("subscribe", self.config["kafka_topic"])
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )
    
    def _transform_stream(self, kafka_df: DataFrame) -> tuple[DataFrame, DataFrame]:
        """
        Transform Kafka messages and split into valid/invalid rows.
        
        Returns:
            (valid_df, dead_letter_df)
        """
        # Parse JSON from Kafka value column
        parsed_df = (
            kafka_df
            .selectExpr("CAST(value AS STRING) as json_value")
            .select(from_json(col("json_value"), PRICE_SCHEMA).alias("data"))
            .select("data.*")
        )
        
        # Cast trade_date and ingested_at
        transformed_df = (
            parsed_df
            .withColumn("trade_date", to_date(col("trade_date"), "yyyy-MM-dd"))
            .withColumn("ingested_at", to_timestamp(col("ingested_at")))
        )
        
        # Add partition columns
        with_partitions = (
            transformed_df
            .withColumn("year", year(col("trade_date")))
            .withColumn("month", month(col("trade_date")))
        )
        
        # Data quality filter: close must be > 0 and not null
        valid_df = with_partitions.filter(
            (col("close").isNotNull()) & (col("close") > 0)
        )
        
        # Dead letter: rows with close <= 0 or null
        dead_letter_df = with_partitions.filter(
            (col("close").isNull()) | (col("close") <= 0)
        )
        
        return valid_df, dead_letter_df
    
    def _write_to_bronze(self, df: DataFrame) -> None:
        """Write valid data to Delta Lake Bronze."""
        bronze_path = self.config["bronze_path"]
        checkpoint_path = f"{self.config['delta_base']}/checkpoints/bronze_stock_prices"
        
        logger.info("Writing to Bronze: %s", bronze_path)
        logger.info("Checkpoint: %s", checkpoint_path)
        
        query = (
            df.writeStream
            .format("delta")
            .outputMode("append")
            .partitionBy("year", "month", "ticker")
            .option("checkpointLocation", checkpoint_path)
            .trigger(processingTime="30 seconds")
            .start(bronze_path)
        )
        
        return query
    
    def _write_to_dead_letter(self, df: DataFrame) -> None:
        """Write invalid data to dead letter path."""
        dead_letter_path = f"{self.config['delta_base']}/dead_letter/stock_prices"
        checkpoint_path = f"{self.config['delta_base']}/checkpoints/dead_letter_stock_prices"
        
        logger.info("Writing dead letters to: %s", dead_letter_path)
        
        query = (
            df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_path)
            .trigger(processingTime="30 seconds")
            .start(dead_letter_path)
        )
        
        return query
    
    def run(self):
        """
        Main run method. Starts the streaming job and blocks until stopped.
        """
        logger.info("=" * 60)
        logger.info("FINSCOPE Bronze Consumer Starting")
        logger.info("=" * 60)
        
        # Create Spark session
        self.spark = self._create_spark_session()
        
        # Read from Kafka
        kafka_stream = self._read_kafka_stream()
        
        # Transform and split
        valid_df, dead_letter_df = self._transform_stream(kafka_stream)
        
        # Write to Bronze (valid data)
        bronze_query = self._write_to_bronze(valid_df)
        
        # Write to dead letter (invalid data)
        dead_letter_query = self._write_to_dead_letter(dead_letter_df)
        
        logger.info("=" * 60)
        logger.info("Streaming jobs started — awaiting termination")
        logger.info("Bronze path: %s", self.config["bronze_path"])
        logger.info("Dead letter: %s/dead_letter/stock_prices", self.config["delta_base"])
        logger.info("=" * 60)
        
        # Block until termination
        try:
            bronze_query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("Shutdown requested")
            bronze_query.stop()
            dead_letter_query.stop()
        finally:
            if self.spark:
                self.spark.stop()
                logger.info("SparkSession stopped")


# ─────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────

if __name__ == "__main__":
    consumer = SparkBronzeConsumer()
    consumer.run()  # blocking — runs until stopped
