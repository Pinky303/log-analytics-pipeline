"""
data_quality.py
Reusable PySpark data quality checks: null validation, schema drift, anomaly detection.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from typing import List, Dict, Tuple


# ── Null / Completeness Checks ────────────────────────────────────────────────

def check_nulls(df: DataFrame, critical_cols: List[str]) -> Tuple[DataFrame, DataFrame]:
    """
    Splits DataFrame into valid and quarantined records based on null checks.

    Args:
        df:            Input DataFrame
        critical_cols: Columns that must be non-null

    Returns:
        (valid_df, quarantine_df)
    """
    null_condition = F.lit(False)
    for col in critical_cols:
        null_condition = null_condition | F.col(col).isNull()

    quarantine_df = df.filter(null_condition).withColumn(
        "_dq_reason", F.lit(f"Null in critical column(s): {critical_cols}")
    )
    valid_df = df.filter(~null_condition)

    total     = df.count()
    quarantined = quarantine_df.count()
    print(f"🔍 DQ Check | Total: {total} | Valid: {total - quarantined} | Quarantined: {quarantined}")

    return valid_df, quarantine_df


# ── Duplicate Detection ───────────────────────────────────────────────────────

def remove_duplicates(df: DataFrame, dedupe_cols: List[str], order_col: str = "_ingested_at") -> DataFrame:
    """
    Removes duplicates keeping the latest record per dedupe key.

    Args:
        df:          Input DataFrame
        dedupe_cols: Columns forming the unique key (e.g. ["event_id"])
        order_col:   Column to determine which record to keep (latest)
    """
    from pyspark.sql.window import Window

    window = Window.partitionBy(*dedupe_cols).orderBy(F.col(order_col).desc())

    deduped = (
        df
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    removed = df.count() - deduped.count()
    print(f"🗑️  Deduplication | Removed {removed} duplicate records")
    return deduped


# ── Schema Drift Detection ────────────────────────────────────────────────────

def detect_schema_drift(df: DataFrame, expected_schema: StructType) -> Dict:
    """
    Compares actual DataFrame schema against expected schema.

    Returns a dict with:
        - missing_cols:  In expected but not in df
        - extra_cols:    In df but not in expected
        - type_mismatches: Columns with wrong types
    """
    actual_fields   = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    expected_fields = {f.name: f.dataType.simpleString() for f in expected_schema.fields}

    missing_cols    = [c for c in expected_fields if c not in actual_fields]
    extra_cols      = [c for c in actual_fields   if c not in expected_fields]
    type_mismatches = {
        c: {"expected": expected_fields[c], "actual": actual_fields[c]}
        for c in expected_fields
        if c in actual_fields and expected_fields[c] != actual_fields[c]
    }

    drift_detected = bool(missing_cols or extra_cols or type_mismatches)

    report = {
        "drift_detected":  drift_detected,
        "missing_cols":    missing_cols,
        "extra_cols":      extra_cols,
        "type_mismatches": type_mismatches
    }

    if drift_detected:
        print(f"⚠️  Schema drift detected!")
        if missing_cols:    print(f"   Missing columns:    {missing_cols}")
        if extra_cols:      print(f"   Extra columns:      {extra_cols}")
        if type_mismatches: print(f"   Type mismatches:    {type_mismatches}")
    else:
        print("✅ Schema validation passed — no drift detected")

    return report


# ── Value Range Checks ────────────────────────────────────────────────────────

def check_value_ranges(df: DataFrame) -> DataFrame:
    """
    Flags records with suspicious values (negative latency, invalid status codes, etc.)
    Adds a _dq_flags column with comma-separated issues.
    """
    flags = F.lit("")

    # Latency should be positive
    flags = F.when(F.col("latency_ms") < 0,
                   F.concat(flags, F.lit("negative_latency,"))) \
             .otherwise(flags)

    # Status code should be in valid HTTP range
    flags = F.when(
        ~F.col("status_code").between(100, 599),
        F.concat(flags, F.lit("invalid_status_code,"))
    ).otherwise(flags)

    # Latency outlier > 60 seconds (likely a bug)
    flags = F.when(F.col("latency_ms") > 60000,
                   F.concat(flags, F.lit("extreme_latency,"))) \
             .otherwise(flags)

    return df.withColumn("_dq_flags", F.rtrim(F.col("_dq_flags").cast("string"), ","))
