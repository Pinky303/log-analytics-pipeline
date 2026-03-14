"""
02_silver_cleaning.py
Stage 2: Bronze → Silver Delta Table

Cleans, types, deduplicates, and quality-checks Bronze data.
Produces a reliable, analysis-ready Silver layer.
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from transformations.utils.data_quality import check_nulls, remove_duplicates, detect_schema_drift, check_value_ranges
from transformations.utils.spark_helpers import repartition_by_date, print_stats, cache_if_reused
from data_simulator.schema_definitions import BRONZE_SCHEMA, SILVER_SCHEMA

# ── Config ────────────────────────────────────────────────────────────────────

BRONZE_TABLE      = "bronze.raw_logs"
SILVER_TABLE      = "silver.logs_cleaned"
SILVER_PATH       = "s3a://your-bucket/delta/silver/logs_cleaned/"
QUARANTINE_PATH   = "s3a://your-bucket/quarantine/silver/"
CRITICAL_COLS     = ["event_id", "user_id", "timestamp", "status_code"]

# ── Read Bronze ───────────────────────────────────────────────────────────────

print("📖 Reading Bronze table...")
bronze_df = spark.table(BRONZE_TABLE)
print_stats(bronze_df, "Bronze Input")

# ── Schema Drift Check ────────────────────────────────────────────────────────

drift_report = detect_schema_drift(bronze_df, BRONZE_SCHEMA)
if drift_report["drift_detected"]:
    print("🚨 Schema drift detected — check before proceeding!")

# ── Null / Quality Checks ─────────────────────────────────────────────────────

valid_df, quarantine_df = check_nulls(bronze_df, CRITICAL_COLS)

# Write quarantined records for investigation
if quarantine_df.count() > 0:
    (
        quarantine_df
        .write.format("delta")
        .mode("append")
        .save(QUARANTINE_PATH)
    )
    print(f"🗃️  Quarantined records written to {QUARANTINE_PATH}")

# ── Deduplication ─────────────────────────────────────────────────────────────

deduped_df = remove_duplicates(valid_df, dedupe_cols=["event_id"], order_col="_ingested_at")

# ── Type Casting & Transformations ────────────────────────────────────────────

silver_df = (
    deduped_df

    # Parse timestamp string → proper TimestampType
    .withColumn("event_timestamp",
                F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))

    # Derive date & hour for partitioning and time-based aggregations
    .withColumn("event_date", F.to_date(F.col("event_timestamp")))
    .withColumn("event_hour", F.hour(F.col("event_timestamp")))

    # Categorise HTTP status codes: 2xx / 3xx / 4xx / 5xx
    .withColumn("status_category",
                F.when(F.col("status_code").between(200, 299), "2xx")
                 .when(F.col("status_code").between(300, 399), "3xx")
                 .when(F.col("status_code").between(400, 499), "4xx")
                 .when(F.col("status_code").between(500, 599), "5xx")
                 .otherwise("unknown"))

    # Drop original raw timestamp string (replaced by event_timestamp)
    .drop("timestamp")
)

# ── Value Range Validation ────────────────────────────────────────────────────

silver_df = check_value_ranges(silver_df)

# ── Select Final Columns (enforce Silver schema order) ────────────────────────

silver_df = silver_df.select(
    "event_id", "session_id", "user_id",
    "event_timestamp", "event_date", "event_hour",
    "service", "endpoint", "method",
    "status_code", "status_category",
    "latency_ms", "region",
    "request_size_bytes", "response_size_bytes",
    "is_error", "error_message", "trace_id",
    "_ingested_at", "_source_file"
)

# ── Repartition for Efficient Writes ─────────────────────────────────────────

silver_df = repartition_by_date(silver_df, date_col="event_date", num_partitions=8)
silver_df = cache_if_reused(silver_df, "Silver")

print_stats(silver_df, "Silver Final")

# ── Write to Silver Delta Table ───────────────────────────────────────────────

print(f"💾 Writing to Silver Delta table: {SILVER_TABLE}")

(
    silver_df
    .write
    .format("delta")
    .mode("append")
    .partitionBy("event_date")
    .option("path", SILVER_PATH)
    .option("mergeSchema", "true")    # Allow schema evolution
    .saveAsTable(SILVER_TABLE)
)

print(f"✅ Silver cleaning complete → {SILVER_TABLE}")

# ── Register Table ────────────────────────────────────────────────────────────

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_TABLE}
    USING DELTA
    LOCATION '{SILVER_PATH}'
""")
