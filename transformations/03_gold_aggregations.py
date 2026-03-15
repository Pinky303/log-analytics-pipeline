"""
03_gold_aggregations.py
Stage 3: Silver → Gold Delta Tables

Produces Daily KPIs and Session Summaries using incremental MERGE.
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from transformations.utils.spark_helpers import print_stats

# ── Config ────────────────────────────────────────────────────────────────────
SILVER_TABLE         = "silver.logs_cleaned"
GOLD_KPIS_TABLE      = "gold.daily_kpis"
GOLD_SESSION_TABLE   = "gold.session_summary"
GOLD_KPIS_PATH       = "s3://pinky-log-pipeline-delta/delta/gold/daily_kpis/"
GOLD_SESSION_PATH    = "s3://pinky-log-pipeline-delta/delta/gold/session_summary/"
SESSION_TIMEOUT_MINS = 30

# ── Incremental Read from Silver ──────────────────────────────────────────────
print("📖 Reading Silver (last 2 days)...")
silver_df = (
    spark.table(SILVER_TABLE)
    .filter(F.col("event_date") >= F.date_sub(F.current_date(), 1))
)
silver_df.cache()
print_stats(silver_df, "Silver Input")

# ─────────────────────────────────────────────────────────────────────────────
# GOLD 1 — Daily KPIs
# ─────────────────────────────────────────────────────────────────────────────
print("📊 Computing Daily KPIs...")

daily_kpis_df = (
    silver_df
    .groupBy("event_date", "service", "region")
    .agg(
        F.count("event_id")                                  .alias("total_requests"),
        F.sum(F.col("is_error").cast("int"))                 .alias("total_errors"),
        F.round(F.sum(F.col("is_error").cast("int")) /
                F.count("event_id") * 100, 2)                .alias("error_rate_pct"),
        F.round(F.avg("latency_ms"), 2)                      .alias("avg_latency_ms"),
        F.round(F.percentile_approx("latency_ms", 0.95), 2)  .alias("p95_latency_ms"),
        F.round(F.percentile_approx("latency_ms", 0.99), 2)  .alias("p99_latency_ms"),
        F.countDistinct("user_id")                           .alias("unique_users"),
        F.countDistinct("session_id")                        .alias("unique_sessions"),
        F.sum("request_size_bytes")                          .alias("total_request_bytes"),
        F.sum("response_size_bytes")                         .alias("total_response_bytes"),
    )
    .withColumn("_computed_at", F.current_timestamp())
)

print_stats(daily_kpis_df, "Daily KPIs")

if DeltaTable.isDeltaTable(spark, GOLD_KPIS_PATH):
    DeltaTable.forPath(spark, GOLD_KPIS_PATH).alias("t").merge(
        daily_kpis_df.alias("s"),
        "t.event_date=s.event_date AND t.service=s.service AND t.region=s.region"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    print(f"✅ MERGE complete → {GOLD_KPIS_TABLE}")
else:
    (daily_kpis_df.write.format("delta").mode("overwrite")
     .partitionBy("event_date").option("path", GOLD_KPIS_PATH)
     .saveAsTable(GOLD_KPIS_TABLE))
    print(f"✅ Initial write → {GOLD_KPIS_TABLE}")

# ─────────────────────────────────────────────────────────────────────────────
# GOLD 2 — Session Summary (Sessionization)
# ─────────────────────────────────────────────────────────────────────────────
print("🔗 Computing Session Summaries...")

user_win = Window.partitionBy("user_id").orderBy("event_timestamp")

sessions_df = (
    silver_df
    .withColumn("prev_ts", F.lag("event_timestamp").over(user_win))
    .withColumn("gap_mins",
                (F.unix_timestamp("event_timestamp") - F.unix_timestamp("prev_ts")) / 60)
    .withColumn("is_new_session",
                (F.col("gap_mins") > SESSION_TIMEOUT_MINS) | F.col("prev_ts").isNull())
    .withColumn("session_num",
                F.sum(F.col("is_new_session").cast("int")).over(
                    user_win.rowsBetween(Window.unboundedPreceding, 0)))
    .withColumn("derived_session_id",
                F.concat_ws("_", F.col("user_id"), F.col("session_num")))
)

session_summary_df = (
    sessions_df
    .groupBy("derived_session_id", "user_id", "event_date")
    .agg(
        F.min("event_timestamp")                              .alias("session_start"),
        F.max("event_timestamp")                              .alias("session_end"),
        F.count("event_id")                                   .alias("total_events"),
        F.round((F.max(F.unix_timestamp("event_timestamp")) -
                 F.min(F.unix_timestamp("event_timestamp"))) / 60, 2).alias("session_duration_mins"),
        F.countDistinct("endpoint")                           .alias("unique_endpoints"),
        F.sum(F.col("is_error").cast("int"))                  .alias("errors_in_session"),
        F.round(F.avg("latency_ms"), 2)                       .alias("avg_latency_ms"),
        F.first("region")                                     .alias("region"),
    )
    .withColumn("_computed_at", F.current_timestamp())
)

print_stats(session_summary_df, "Session Summary")

if DeltaTable.isDeltaTable(spark, GOLD_SESSION_PATH):
    DeltaTable.forPath(spark, GOLD_SESSION_PATH).alias("t").merge(
        session_summary_df.alias("s"),
        "t.derived_session_id=s.derived_session_id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    print(f"✅ MERGE complete → {GOLD_SESSION_TABLE}")
else:
    (session_summary_df.write.format("delta").mode("overwrite")
     .partitionBy("event_date").option("path", GOLD_SESSION_PATH)
     .saveAsTable(GOLD_SESSION_TABLE))
    print(f"✅ Initial write → {GOLD_SESSION_TABLE}")

silver_df.unpersist()
print("\n🏁 Gold aggregations complete!")
