"""
03_gold_aggregations.py
Stage 3: Silver → Gold Delta Tables

Produces business-level KPIs and session summaries.
Uses incremental processing — only processes new Silver data since last run.
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

from transformations.utils.spark_helpers import print_stats

# ── Config ────────────────────────────────────────────────────────────────────

SILVER_TABLE          = "silver.logs_cleaned"
GOLD_DAILY_KPIS       = "gold.daily_kpis"
GOLD_SESSION_SUMMARY  = "gold.session_summary"
GOLD_KPIS_PATH        = "s3a://your-bucket/delta/gold/daily_kpis/"
GOLD_SESSION_PATH     = "s3a://your-bucket/delta/gold/session_summary/"

SESSION_TIMEOUT_MINS  = 30    # 30-minute idle timeout for sessionization

# ── Incremental Read from Silver ──────────────────────────────────────────────
# Only process yesterday's and today's data (change to watermark logic for prod)

print("📖 Reading Silver table (incremental)...")

silver_df = (
    spark.table(SILVER_TABLE)
    .filter(F.col("event_date") >= F.date_sub(F.current_date(), 1))
)
silver_df.cache()
print_stats(silver_df, "Silver Input (incremental)")

# ─────────────────────────────────────────────────────────────────────────────
# GOLD TABLE 1: Daily KPIs
# ─────────────────────────────────────────────────────────────────────────────

print("📊 Computing Daily KPIs...")

daily_kpis_df = (
    silver_df
    .groupBy("event_date", "service", "region")
    .agg(
        F.count("event_id")                              .alias("total_requests"),
        F.sum(F.col("is_error").cast("int"))             .alias("total_errors"),
        F.round(
            F.sum(F.col("is_error").cast("int")) /
            F.count("event_id") * 100, 2
        )                                                .alias("error_rate_pct"),
        F.round(F.avg("latency_ms"), 2)                  .alias("avg_latency_ms"),
        F.round(F.percentile_approx("latency_ms", 0.95), 2).alias("p95_latency_ms"),
        F.round(F.percentile_approx("latency_ms", 0.99), 2).alias("p99_latency_ms"),
        F.countDistinct("user_id")                       .alias("unique_users"),
        F.countDistinct("session_id")                    .alias("unique_sessions"),
        F.sum("request_size_bytes")                      .alias("total_request_bytes"),
        F.sum("response_size_bytes")                     .alias("total_response_bytes"),
    )
    .withColumn("_computed_at", F.current_timestamp())
)

print_stats(daily_kpis_df, "Daily KPIs")

# ── MERGE into Gold (upsert — avoid duplicates on re-run) ─────────────────────

if DeltaTable.isDeltaTable(spark, GOLD_KPIS_PATH):
    gold_kpis = DeltaTable.forPath(spark, GOLD_KPIS_PATH)
    (
        gold_kpis.alias("target")
        .merge(
            daily_kpis_df.alias("source"),
            "target.event_date = source.event_date AND "
            "target.service    = source.service    AND "
            "target.region     = source.region"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"✅ MERGE complete → {GOLD_DAILY_KPIS}")
else:
    # First run — just write
    (
        daily_kpis_df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("event_date")
        .option("path", GOLD_KPIS_PATH)
        .saveAsTable(GOLD_DAILY_KPIS)
    )
    print(f"✅ Initial write complete → {GOLD_DAILY_KPIS}")

# ─────────────────────────────────────────────────────────────────────────────
# GOLD TABLE 2: Session Summary (Sessionization)
# ─────────────────────────────────────────────────────────────────────────────

print("🔗 Computing Session Summaries...")

# Window by user, ordered by timestamp
user_window = Window.partitionBy("user_id").orderBy("event_timestamp")

# Detect session boundaries: new session if gap > SESSION_TIMEOUT_MINS
sessions_df = (
    silver_df
    .withColumn("prev_ts", F.lag("event_timestamp").over(user_window))
    .withColumn("gap_mins",
                (F.unix_timestamp("event_timestamp") - F.unix_timestamp("prev_ts")) / 60)
    .withColumn("is_new_session",
                (F.col("gap_mins") > SESSION_TIMEOUT_MINS) | F.col("prev_ts").isNull())
    .withColumn("session_num",
                F.sum(F.col("is_new_session").cast("int")).over(
                    user_window.rowsBetween(Window.unboundedPreceding, 0)
                ))
    # Composite session key
    .withColumn("derived_session_id",
                F.concat_ws("_", F.col("user_id"), F.col("session_num")))
)

session_summary_df = (
    sessions_df
    .groupBy("derived_session_id", "user_id", "event_date")
    .agg(
        F.min("event_timestamp")                         .alias("session_start"),
        F.max("event_timestamp")                         .alias("session_end"),
        F.count("event_id")                              .alias("total_events"),
        F.round(
            (F.max(F.unix_timestamp("event_timestamp")) -
             F.min(F.unix_timestamp("event_timestamp"))) / 60, 2
        )                                                .alias("session_duration_mins"),
        F.countDistinct("endpoint")                      .alias("unique_endpoints"),
        F.sum(F.col("is_error").cast("int"))             .alias("errors_in_session"),
        F.round(F.avg("latency_ms"), 2)                  .alias("avg_latency_ms"),
        F.first("region")                                .alias("region"),
    )
    .withColumn("_computed_at", F.current_timestamp())
)

print_stats(session_summary_df, "Session Summary")

# ── MERGE Session Summary ─────────────────────────────────────────────────────

if DeltaTable.isDeltaTable(spark, GOLD_SESSION_PATH):
    gold_sessions = DeltaTable.forPath(spark, GOLD_SESSION_PATH)
    (
        gold_sessions.alias("target")
        .merge(
            session_summary_df.alias("source"),
            "target.derived_session_id = source.derived_session_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"✅ MERGE complete → {GOLD_SESSION_SUMMARY}")
else:
    (
        session_summary_df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("event_date")
        .option("path", GOLD_SESSION_PATH)
        .saveAsTable(GOLD_SESSION_SUMMARY)
    )
    print(f"✅ Initial write complete → {GOLD_SESSION_SUMMARY}")

silver_df.unpersist()
print("\n🏁 Gold aggregations pipeline complete!")
