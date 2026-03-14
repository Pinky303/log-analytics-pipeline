"""
spark_helpers.py
Reusable PySpark utilities: SparkSession factory, S3 mounts, optimisation helpers.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import Optional


# ── SparkSession ──────────────────────────────────────────────────────────────

def get_spark(app_name: str = "LogAnalyticsPipeline") -> SparkSession:
    """
    Creates or retrieves a SparkSession.
    On Databricks, spark is already available — this is for local dev.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        # Performance tuning
        .config("spark.sql.adaptive.enabled", "true")                    # AQE: auto-optimises joins & shuffles
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") # AQE: merges small partitions
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ── S3 Mount (Databricks) ─────────────────────────────────────────────────────

def mount_s3_bucket(bucket_name: str, mount_point: str, iam_role: str) -> None:
    """
    Mounts an S3 bucket in Databricks using an IAM role.
    Run once per workspace — skip if already mounted.

    Args:
        bucket_name: e.g. "my-log-pipeline-bucket"
        mount_point: e.g. "/mnt/logs"
        iam_role:    AWS IAM role ARN with S3 read/write access
    """
    try:
        dbutils.fs.ls(mount_point)   # noqa: F821  (dbutils is Databricks-injected)
        print(f"ℹ️  {mount_point} is already mounted. Skipping.")
    except Exception:
        dbutils.fs.mount(            # noqa: F821
            source=f"s3a://{bucket_name}",
            mount_point=mount_point,
            extra_configs={"fs.s3a.credentialsType": "AssumeRole",
                           "fs.s3a.stsAssumeRole.arn": iam_role}
        )
        print(f"✅ Mounted s3://{bucket_name} → {mount_point}")


# ── Optimisation Helpers ──────────────────────────────────────────────────────

def repartition_by_date(df: DataFrame, date_col: str = "event_date", num_partitions: int = 10) -> DataFrame:
    """Repartitions data by date for efficient downstream writes."""
    return df.repartition(num_partitions, date_col)


def cache_if_reused(df: DataFrame, name: str) -> DataFrame:
    """Caches a DataFrame and prints confirmation."""
    df.cache()
    print(f"💾 Cached DataFrame: {name}")
    return df


def add_ingestion_metadata(df: DataFrame, source_file: Optional[str] = None) -> DataFrame:
    """Adds standard audit columns to any DataFrame."""
    df = df.withColumn("_ingested_at", F.current_timestamp())
    if source_file:
        df = df.withColumn("_source_file", F.lit(source_file))
    else:
        # Use Spark's built-in input_file_name() when reading from files
        df = df.withColumn("_source_file", F.input_file_name())
    return df


def print_stats(df: DataFrame, label: str) -> None:
    """Prints row count and partition count for a DataFrame."""
    row_count  = df.count()
    part_count = df.rdd.getNumPartitions()
    print(f"📊 [{label}] Rows: {row_count:,} | Partitions: {part_count}")
