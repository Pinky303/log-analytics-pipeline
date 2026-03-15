"""
merge_scd2.py
SCD Type 2 MERGE + Delta Lake utility operations:
  - upsert_scd2()         : SCD2 historical tracking
  - optimize_delta_table(): OPTIMIZE + ZORDER
  - vacuum_delta_table()  : Remove old file versions
  - time_travel_query()   : Read historical snapshots
"""

from pyspark.sql import functions as F, DataFrame
from pyspark.sql.types import BooleanType, TimestampType
from delta.tables import DeltaTable


def upsert_scd2(spark, new_data_df: DataFrame, target_path: str,
                target_table: str, natural_key: str, compare_cols: list) -> None:
    """Applies SCD Type 2 using Delta MERGE — tracks historical row changes."""
    now = F.current_timestamp()
    staged_df = (
        new_data_df
        .withColumn("effective_start_date", now)
        .withColumn("effective_end_date",   F.lit(None).cast(TimestampType()))
        .withColumn("is_current",           F.lit(True))
        .withColumn("_checksum", F.md5(F.concat_ws("|", *[F.col(c) for c in compare_cols])))
    )

    if not DeltaTable.isDeltaTable(spark, target_path):
        (staged_df.write.format("delta").mode("overwrite")
         .option("path", target_path).saveAsTable(target_table))
        print(f"🆕 First SCD2 load → {target_table}")
        return

    target         = DeltaTable.forPath(spark, target_path)
    change_cond    = " OR ".join([f"target.{c} <> source.{c}" for c in compare_cols])

    # Step 1: Expire changed rows
    (target.alias("target")
     .merge(staged_df.alias("source"),
            f"target.{natural_key}=source.{natural_key} AND target.is_current=true")
     .whenMatchedUpdate(condition=change_cond,
                        set={"effective_end_date": "source.effective_start_date",
                             "is_current": "false"})
     .execute())

    # Step 2: Insert new/changed rows
    existing = spark.table(target_table).filter(F.col("is_current") == True)
    to_insert = (
        staged_df.alias("n")
        .join(existing.alias("e"),
              (F.col(f"n.{natural_key}") == F.col(f"e.{natural_key}")) &
              (F.col("n._checksum") == F.col("e._checksum")), how="left_anti")
    )
    to_insert.write.format("delta").mode("append").save(target_path)
    print(f"✅ SCD2 MERGE complete → {target_table}")


def optimize_delta_table(spark, table_name: str, z_order_cols: list = None) -> None:
    """OPTIMIZE + optional ZORDER to compact small files and improve query speed."""
    if z_order_cols:
        spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({', '.join(z_order_cols)})")
        print(f"✅ OPTIMIZE + ZORDER ({z_order_cols}) → {table_name}")
    else:
        spark.sql(f"OPTIMIZE {table_name}")
        print(f"✅ OPTIMIZE → {table_name}")


def vacuum_delta_table(spark, table_name: str, retention_hours: int = 168) -> None:
    """Remove old file versions. Default = 7 days. Never go below 168h in prod."""
    spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS")
    print(f"✅ VACUUM ({retention_hours}h) → {table_name}")


def time_travel_query(spark, table_name: str,
                      version: int = None, timestamp: str = None) -> DataFrame:
    """Read a historical Delta snapshot by version number or timestamp."""
    if version is not None:
        return spark.read.format("delta").option("versionAsOf", version).table(table_name)
    elif timestamp:
        return spark.read.format("delta").option("timestampAsOf", timestamp).table(table_name)
    else:
        raise ValueError("Provide version or timestamp")


# ── Run post-pipeline optimisation (call from Databricks notebook) ────────────
if __name__ == "__main__":
    optimize_delta_table(spark, "gold.daily_kpis",     z_order_cols=["event_date", "service"])
    optimize_delta_table(spark, "gold.session_summary", z_order_cols=["event_date", "user_id"])
    # vacuum_delta_table(spark, "gold.daily_kpis")     # run weekly
