"""
merge_scd2.py
SCD Type 2 (Slowly Changing Dimension) implementation using Delta Lake MERGE.

Tracks historical changes to user dimension data over time.
Each change creates a new row; the current record has is_current = True.
"""

from pyspark.sql import functions as F, DataFrame
from pyspark.sql.types import BooleanType, TimestampType
from delta.tables import DeltaTable

# ── Config ────────────────────────────────────────────────────────────────────

USER_DIM_TABLE = "gold.dim_users"
USER_DIM_PATH  = "s3a://your-bucket/delta/gold/dim_users/"


def upsert_scd2(
    spark,
    new_data_df:  DataFrame,
    target_path:  str,
    target_table: str,
    natural_key:  str,           # Business key (e.g. user_id)
    compare_cols: list           # Columns that trigger a new SCD2 row when changed
) -> None:
    """
    Applies SCD Type 2 logic using Delta Lake MERGE.

    Workflow:
      1. Match on natural_key
      2. If changed columns differ → expire old row (set end_date, is_current=False)
      3. Insert new row (start_date=today, end_date=NULL, is_current=True)
      4. Insert completely new records
    """

    now = F.current_timestamp()

    # Add SCD2 columns to incoming data
    staged_df = (
        new_data_df
        .withColumn("effective_start_date", now)
        .withColumn("effective_end_date",   F.lit(None).cast(TimestampType()))
        .withColumn("is_current",           F.lit(True))
        .withColumn("_checksum",            F.md5(F.concat_ws("|", *[F.col(c) for c in compare_cols])))
    )

    if not DeltaTable.isDeltaTable(spark, target_path):
        # First load — write directly
        print(f"🆕 First load — writing to {target_table}")
        (
            staged_df.write
            .format("delta")
            .mode("overwrite")
            .option("path", target_path)
            .saveAsTable(target_table)
        )
        return

    target = DeltaTable.forPath(spark, target_path)

    # Step 1: Expire changed rows
    change_condition = " OR ".join([
        f"target.{c} <> source.{c}" for c in compare_cols
    ])

    (
        target.alias("target")
        .merge(
            staged_df.alias("source"),
            f"target.{natural_key} = source.{natural_key} AND target.is_current = true"
        )
        .whenMatchedUpdate(
            condition=change_condition,
            set={
                "effective_end_date": "source.effective_start_date",
                "is_current":         "false"
            }
        )
        .execute()
    )

    # Step 2: Insert new/changed rows
    existing_current = spark.table(target_table).filter(F.col("is_current") == True)

    # Only insert rows that are new or have changed checksum
    rows_to_insert = (
        staged_df.alias("new")
        .join(existing_current.alias("curr"),
              (F.col(f"new.{natural_key}") == F.col(f"curr.{natural_key}")) &
              (F.col("new._checksum") == F.col("curr._checksum")),
              how="left_anti")    # Anti-join: keep only rows NOT matching existing
    )

    rows_to_insert.write.format("delta").mode("append").save(target_path)
    print(f"✅ SCD2 MERGE complete → {target_table}")


# ── Optimize & Vacuum ─────────────────────────────────────────────────────────

def optimize_delta_table(spark, table_name: str, z_order_cols: list = None) -> None:
    """
    Runs OPTIMIZE (compacts small files) and optionally Z-Orders for fast queries.
    """
    print(f"⚙️  Running OPTIMIZE on {table_name}...")
    if z_order_cols:
        cols = ", ".join(z_order_cols)
        spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({cols})")
        print(f"✅ OPTIMIZE + ZORDER BY ({cols}) complete")
    else:
        spark.sql(f"OPTIMIZE {table_name}")
        print(f"✅ OPTIMIZE complete")


def vacuum_delta_table(spark, table_name: str, retention_hours: int = 168) -> None:
    """
    Removes old file versions. Default retention = 7 days (168 hours).
    WARNING: Do not go below 7 days in production (breaks time travel).
    """
    print(f"🧹 Running VACUUM on {table_name} (retain {retention_hours}h)...")
    spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS")
    print(f"✅ VACUUM complete")


def time_travel_query(spark, table_name: str, version: int = None, timestamp: str = None):
    """
    Reads a historical snapshot of a Delta table.

    Args:
        version:   Read as of this version number
        timestamp: Read as of this timestamp string (e.g. '2024-01-15 00:00:00')
    """
    if version is not None:
        df = spark.read.format("delta").option("versionAsOf", version).table(table_name)
        print(f"⏪ Time travel: {table_name} @ version {version}")
    elif timestamp:
        df = spark.read.format("delta").option("timestampAsOf", timestamp).table(table_name)
        print(f"⏪ Time travel: {table_name} @ {timestamp}")
    else:
        raise ValueError("Provide either version or timestamp")

    return df


# ── Example usage (run in Databricks notebook) ────────────────────────────────

if __name__ == "__main__":
    # Optimise Gold tables after each pipeline run
    optimize_delta_table(spark, "gold.daily_kpis",      z_order_cols=["event_date", "service"])
    optimize_delta_table(spark, "gold.session_summary",  z_order_cols=["event_date", "user_id"])

    # Weekly vacuum (run as a separate scheduled job)
    # vacuum_delta_table(spark, "gold.daily_kpis")

    # Time travel example
    # historical = time_travel_query(spark, "gold.daily_kpis", version=3)
    # historical.show()
