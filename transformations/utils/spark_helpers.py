"""
spark_helpers.py
Reusable PySpark utilities: SparkSession factory, metadata, optimisation helpers.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import Optional


def get_spark(app_name: str = "LogAnalyticsPipeline") -> SparkSession:
    """Creates or retrieves SparkSession with Delta Lake + AQE enabled."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )


def add_ingestion_metadata(df: DataFrame, source_file: Optional[str] = None) -> DataFrame:
    """Adds _ingested_at and _source_file audit columns."""
    df = df.withColumn("_ingested_at", F.current_timestamp())
    df = df.withColumn(
        "_source_file",
        F.lit(source_file) if source_file else F.input_file_name()
    )
    return df


def repartition_by_date(df: DataFrame, date_col: str = "event_date",
                        num_partitions: int = 8) -> DataFrame:
    return df.repartition(num_partitions, date_col)


def cache_if_reused(df: DataFrame, name: str) -> DataFrame:
    df.cache()
    print(f"💾 Cached: {name}")
    return df


def print_stats(df: DataFrame, label: str) -> None:
    print(f"📊 [{label}] Rows: {df.count():,} | Partitions: {df.rdd.getNumPartitions()}")
