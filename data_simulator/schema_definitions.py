"""
schema_definitions.py
Centralised PySpark schema definitions for Bronze and Silver layers.
"""

from pyspark.sql.types import (
    StructType, StructField,
    StringType, BooleanType, DoubleType,
    IntegerType, TimestampType, LongType
)

BRONZE_SCHEMA = StructType([
    StructField("event_id",            StringType(),  False),
    StructField("session_id",          StringType(),  True),
    StructField("user_id",             StringType(),  True),
    StructField("timestamp",           StringType(),  True),
    StructField("service",             StringType(),  True),
    StructField("endpoint",            StringType(),  True),
    StructField("method",              StringType(),  True),
    StructField("status_code",         IntegerType(), True),
    StructField("latency_ms",          DoubleType(),  True),
    StructField("region",              StringType(),  True),
    StructField("user_agent",          StringType(),  True),
    StructField("request_size_bytes",  LongType(),    True),
    StructField("response_size_bytes", LongType(),    True),
    StructField("is_error",            BooleanType(), True),
    StructField("error_message",       StringType(),  True),
    StructField("trace_id",            StringType(),  True),
])

SILVER_SCHEMA = StructType([
    StructField("event_id",            StringType(),    False),
    StructField("session_id",          StringType(),    True),
    StructField("user_id",             StringType(),    True),
    StructField("event_timestamp",     TimestampType(), True),
    StructField("event_date",          StringType(),    True),
    StructField("event_hour",          IntegerType(),   True),
    StructField("service",             StringType(),    True),
    StructField("endpoint",            StringType(),    True),
    StructField("method",              StringType(),    True),
    StructField("status_code",         IntegerType(),   True),
    StructField("status_category",     StringType(),    True),
    StructField("latency_ms",          DoubleType(),    True),
    StructField("region",              StringType(),    True),
    StructField("request_size_bytes",  LongType(),      True),
    StructField("response_size_bytes", LongType(),      True),
    StructField("is_error",            BooleanType(),   True),
    StructField("error_message",       StringType(),    True),
    StructField("trace_id",            StringType(),    True),
    StructField("_ingested_at",        TimestampType(), True),
    StructField("_source_file",        StringType(),    True),
])
