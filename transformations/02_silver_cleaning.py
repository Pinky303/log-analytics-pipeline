"""
02_silver_cleaning.py
Stage 2: Bronze → Silver Delta Table

Cleans, deduplicates, type-casts and quality-checks Bronze data.
"""

from pyspark.sql import functions as F
from transformations.utils.data_quality  import check_nulls, remove_duplicates, detect_schema_drift, check_value_ranges
from transformations.utils.spark_helpers import repartition_by_date, print_stats, cache_if_reused

# ── Config ────────────────────────────────────────────────────────────────────
BRONZE_TABLE    = "bronze.raw_logs"
SILVER_TABLE    = "silver.logs_cleaned"
SILVER_PATH     = "s3://pinky-log-pipeline-delta/delta/silver/logs_cleaned/"
QUARANTINE_PATH = "s3://pinky-log-pipeline-delta/quarantine/silver/"
CRITICAL_COLS   = ["event_id", "user_id", "timestamp", "status_code"]

# ── Read Bronze ───────────────────────────────────────────────────────────────
print("📖 Reading Bronze table...")
bronze_df = spark.table(BRONZE_TABLE)
print_stats(bronze_df, "Bronze Input")

# ── Quality Checks ────────────────────────────────────────────────────────────
valid_df, quarantine_df = check_nulls(bronze_df, CRITICAL_COLS)

if quarantine_df.count() > 0:
    quarantine_df.write.format("delta").mode("append").save(QUARANTINE_PATH)
    print(f"🗃️  Quarantine written to {QUARANTINE_PATH}")

# ── Deduplication ─────────────────────────────────────────────────────────────
deduped_df = remove_duplicates(valid_df, dedupe_cols=["event_id"])

# ── Type Casting + Transformations ────────────────────────────────────────────
silver_df = (
    deduped_df
    .withColumn("event_timestamp",
                F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
    .withColumn("event_date", F.to_date(F.col("event_timestamp")))
    .withColumn("event_hour", F.hour(F.col("event_timestamp")))
    .withColumn("status_category",
                F.when(F.col("status_code").between(200, 299), "2xx")
                 .when(F.col("status_code").between(300, 399), "3xx")
                 .when(F.col("status_code").between(400, 499), "4xx")
                 .when(F.col("status_code").between(500, 599), "5xx")
                 .otherwise("unknown"))
    .drop("timestamp")
)

silver_df = check_value_ranges(silver_df)
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

silver_df = repartition_by_date(silver_df)
silver_df = cache_if_reused(silver_df, "Silver")
print_stats(silver_df, "Silver Final")

# ── Write to Delta ────────────────────────────────────────────────────────────
print(f"💾 Writing Silver table: {SILVER_TABLE}")

(
    silver_df.write
    .format("delta")
    .mode("append")
    .partitionBy("event_date")
    .option("path", SILVER_PATH)
    .option("mergeSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_TABLE}
    USING DELTA LOCATION '{SILVER_PATH}'
""")

print(f"✅ Silver cleaning complete → {SILVER_TABLE}")
silver_df.unpersist()
