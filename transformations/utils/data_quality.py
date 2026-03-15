"""
data_quality.py
Reusable PySpark data quality checks used across all pipeline stages.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from pyspark.sql.window import Window
from typing import List, Dict, Tuple


def check_nulls(df: DataFrame, critical_cols: List[str]) -> Tuple[DataFrame, DataFrame]:
    """
    Splits DataFrame into valid and quarantined records based on null checks.
    Returns (valid_df, quarantine_df).
    """
    null_cond = F.lit(False)
    for col in critical_cols:
        null_cond = null_cond | F.col(col).isNull()

    quarantine_df = df.filter(null_cond).withColumn(
        "_dq_reason", F.lit(f"Null in: {critical_cols}")
    )
    valid_df = df.filter(~null_cond)

    print(f"🔍 DQ Nulls | Valid: {valid_df.count()} | Quarantined: {quarantine_df.count()}")
    return valid_df, quarantine_df


def remove_duplicates(df: DataFrame, dedupe_cols: List[str], order_col: str = "_ingested_at") -> DataFrame:
    """Keeps the latest record per dedupe key."""
    window  = Window.partitionBy(*dedupe_cols).orderBy(F.col(order_col).desc())
    deduped = (
        df
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )
    print(f"🗑️  Dedup | Removed: {df.count() - deduped.count()} duplicates")
    return deduped


def detect_schema_drift(df: DataFrame, expected_schema: StructType) -> Dict:
    """Compares actual vs expected schema. Returns drift report dict."""
    actual   = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    expected = {f.name: f.dataType.simpleString() for f in expected_schema.fields}

    missing    = [c for c in expected if c not in actual]
    extra      = [c for c in actual   if c not in expected]
    mismatched = {
        c: {"expected": expected[c], "actual": actual[c]}
        for c in expected if c in actual and expected[c] != actual[c]
    }
    drift = bool(missing or extra or mismatched)

    if drift:
        print(f"⚠️  Schema drift! Missing: {missing} | Extra: {extra} | Mismatched: {mismatched}")
    else:
        print("✅ Schema OK — no drift")

    return {"drift_detected": drift, "missing_cols": missing,
            "extra_cols": extra, "type_mismatches": mismatched}


def check_value_ranges(df: DataFrame) -> DataFrame:
    """Adds _dq_flags column flagging suspicious values."""
    flags = F.lit("")
    flags = F.when(F.col("latency_ms") < 0,
                   F.concat(flags, F.lit("negative_latency,"))).otherwise(flags)
    flags = F.when(~F.col("status_code").between(100, 599),
                   F.concat(flags, F.lit("invalid_status,"))).otherwise(flags)
    flags = F.when(F.col("latency_ms") > 60000,
                   F.concat(flags, F.lit("extreme_latency,"))).otherwise(flags)
    return df.withColumn("_dq_flags", flags)
