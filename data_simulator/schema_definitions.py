"""
schema_definitions.py
Centralised PySpark schema definitions for all pipeline layers.
"""

from pyspark.sql.types import (
    StructType, StructField,
    StringType, BooleanType, DoubleType, IntegerType, TimestampType, LongType
)

# ── Bronze Schema (raw JSON ingestion) ───────────────────────────────────────

BRONZE_SCHEMA = StructType([
    StructField("event_id",             StringType(),  nullable=False),
    StructField("session_id",           StringType(),  nullable=True),
    StructField("user_id",              StringType(),  nullable=True),
    StructField("timestamp",            StringType(),  nullable=True),   # raw string; cast in Silver
    StructField("service",              StringType(),  nullable=True),
    StructField("endpoint",             StringType(),  nullable=True),
    StructField("method",               StringType(),  nullable=True),
    StructField("status_code",          IntegerType(), nullable=True),
    StructField("latency_ms",           DoubleType(),  nullable=True),
    StructField("region",               StringType(),  nullable=True),
    StructField("user_agent",           StringType(),  nullable=True),
    StructField("request_size_bytes",   LongType(),    nullable=True),
    StructField("response_size_bytes",  LongType(),    nullable=True),
    StructField("is_error",             BooleanType(), nullable=True),
    StructField("error_message",        StringType(),  nullable=True),
    StructField("trace_id",             StringType(),  nullable=True),
])

# ── Silver Schema (cleaned, typed) ───────────────────────────────────────────

SILVER_SCHEMA = StructType([
    StructField("event_id",             StringType(),    nullable=False),
    StructField("session_id",           StringType(),    nullable=True),
    StructField("user_id",              StringType(),    nullable=True),
    StructField("event_timestamp",      TimestampType(), nullable=True),
    StructField("event_date",           StringType(),    nullable=True),   # for partitioning
    StructField("event_hour",           IntegerType(),   nullable=True),
    StructField("service",              StringType(),    nullable=True),
    StructField("endpoint",             StringType(),    nullable=True),
    StructField("method",               StringType(),    nullable=True),
    StructField("status_code",          IntegerType(),   nullable=True),
    StructField("status_category",      StringType(),    nullable=True),   # 2xx / 4xx / 5xx
    StructField("latency_ms",           DoubleType(),    nullable=True),
    StructField("region",               StringType(),    nullable=True),
    StructField("request_size_bytes",   LongType(),      nullable=True),
    StructField("response_size_bytes",  LongType(),      nullable=True),
    StructField("is_error",             BooleanType(),   nullable=True),
    StructField("error_message",        StringType(),    nullable=True),
    StructField("trace_id",             StringType(),    nullable=True),
    StructField("_ingested_at",         TimestampType(), nullable=True),
    StructField("_source_file",         StringType(),    nullable=True),
])
