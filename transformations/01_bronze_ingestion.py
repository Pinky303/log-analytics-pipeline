"""
01_bronze_ingestion.py
Stage 1: Raw JSON from S3 → Bronze Delta Table

Run this as a Databricks notebook cell or scheduled job.
In Databricks, 'spark' is pre-injected — no need to call get_spark().
"""

# ── Uncomment for local dev only ──────────────────────────────────────────────
# import sys; sys.path.append('..')
# from transformations.utils.spark_helpers import get_spark
# spark = get_spark()

from pyspark.sql import functions as F
from transformations.utils.spark_helpers import add_ingestion_metadata, print_stats

# ── Config — update these with your actual bucket names ───────────────────────
RAW_S3_PATH  = "s3://pinky-log-pipeline-raw/raw/logs/"
BRONZE_PATH  = "s3://pinky-log-pipeline-delta/delta/bronze/raw_logs/"
BRONZE_TABLE = "bronze.raw_logs"

# ── Read Raw JSON ─────────────────────────────────────────────────────────────
print("📥 Reading raw logs from S3...")

raw_df = (
    spark.read
    .option("inferSchema", "true")
    .option("mode", "PERMISSIVE")
    .json(RAW_S3_PATH)
)
print_stats(raw_df, "Raw Input")

# ── Add Metadata ──────────────────────────────────────────────────────────────
bronze_df = add_ingestion_metadata(raw_df)
bronze_df = bronze_df.withColumn("event_date", F.to_date(F.col("timestamp")))

print_stats(bronze_df, "Bronze After Metadata")

# ── Write to Delta ────────────────────────────────────────────────────────────
print(f"💾 Writing Bronze table: {BRONZE_TABLE}")

(
    bronze_df.write
    .format("delta")
    .mode("append")
    .partitionBy("event_date")
    .option("path", BRONZE_PATH)
    .saveAsTable(BRONZE_TABLE)
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_TABLE}
    USING DELTA LOCATION '{BRONZE_PATH}'
""")

print(f"✅ Bronze ingestion complete → {BRONZE_TABLE}")
