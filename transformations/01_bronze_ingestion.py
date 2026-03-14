"""
01_bronze_ingestion.py
Stage 1: Raw JSON → Bronze Delta Table

Reads raw log files from S3, enforces schema, adds metadata, writes to Bronze Delta table.
Run this as a Databricks notebook or scheduled job.
"""

# ── In Databricks, 'spark' is pre-injected. For local dev, uncomment below: ──
# from transformations.utils.spark_helpers import get_spark
# spark = get_spark()

from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from transformations.utils.spark_helpers import add_ingestion_metadata, print_stats
from data_simulator.schema_definitions import BRONZE_SCHEMA

# ── Config ────────────────────────────────────────────────────────────────────

RAW_S3_PATH    = "s3a://your-bucket/raw/logs/"          # Update with your bucket
BRONZE_TABLE   = "bronze.raw_logs"
BRONZE_PATH    = "s3a://your-bucket/delta/bronze/raw_logs/"
CHECKPOINT_DIR = "s3a://your-bucket/checkpoints/bronze/"

# ── Read Raw JSON from S3 ─────────────────────────────────────────────────────

print("📥 Reading raw logs from S3...")

raw_df = (
    spark.read
    .schema(BRONZE_SCHEMA)                 # Enforce schema — reject bad records
    .option("badRecordsPath", "s3a://your-bucket/bad_records/bronze/")
    .option("mode", "PERMISSIVE")          # Log bad records, don't fail the job
    .json(RAW_S3_PATH)
)

print_stats(raw_df, "Raw Ingestion")

# ── Add Metadata Columns ──────────────────────────────────────────────────────

bronze_df = add_ingestion_metadata(raw_df)

# Add partition column
bronze_df = bronze_df.withColumn(
    "event_date",
    F.to_date(F.col("timestamp"))   # will be cast properly in Silver
)

print_stats(bronze_df, "Bronze After Metadata")

# ── Write to Delta Table ──────────────────────────────────────────────────────

print(f"💾 Writing to Bronze Delta table: {BRONZE_TABLE}")

(
    bronze_df
    .write
    .format("delta")
    .mode("append")                        # Append — Bronze is immutable raw store
    .partitionBy("event_date")             # Partition by date for pruning
    .option("path", BRONZE_PATH)
    .saveAsTable(BRONZE_TABLE)
)

print(f"✅ Bronze ingestion complete → {BRONZE_TABLE}")

# ── Register Table in Hive Metastore (first run only) ─────────────────────────

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_TABLE}
    USING DELTA
    LOCATION '{BRONZE_PATH}'
""")
