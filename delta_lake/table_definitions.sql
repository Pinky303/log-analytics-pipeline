-- =============================================================================
-- table_definitions.sql
-- Run once in Databricks SQL Editor to create all databases and tables
-- Update bucket names to match your actual S3 buckets
-- =============================================================================

-- Databases
CREATE DATABASE IF NOT EXISTS bronze COMMENT 'Raw ingested data';
CREATE DATABASE IF NOT EXISTS silver COMMENT 'Cleaned and validated data';
CREATE DATABASE IF NOT EXISTS gold   COMMENT 'Business KPIs and aggregates';

-- ── Bronze ────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bronze.raw_logs (
    event_id              STRING        NOT NULL,
    session_id            STRING,
    user_id               STRING,
    timestamp             STRING,
    service               STRING,
    endpoint              STRING,
    method                STRING,
    status_code           INT,
    latency_ms            DOUBLE,
    region                STRING,
    user_agent            STRING,
    request_size_bytes    BIGINT,
    response_size_bytes   BIGINT,
    is_error              BOOLEAN,
    error_message         STRING,
    trace_id              STRING,
    _ingested_at          TIMESTAMP,
    _source_file          STRING,
    event_date            DATE
)
USING DELTA
PARTITIONED BY (event_date)
LOCATION 's3://pinky-log-pipeline-delta/delta/bronze/raw_logs/'
COMMENT 'Bronze: raw log events as received from S3';

-- ── Silver ────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS silver.logs_cleaned (
    event_id              STRING        NOT NULL,
    session_id            STRING,
    user_id               STRING,
    event_timestamp       TIMESTAMP,
    event_date            DATE,
    event_hour            INT,
    service               STRING,
    endpoint              STRING,
    method                STRING,
    status_code           INT,
    status_category       STRING,
    latency_ms            DOUBLE,
    region                STRING,
    request_size_bytes    BIGINT,
    response_size_bytes   BIGINT,
    is_error              BOOLEAN,
    error_message         STRING,
    trace_id              STRING,
    _ingested_at          TIMESTAMP,
    _source_file          STRING
)
USING DELTA
PARTITIONED BY (event_date)
LOCATION 's3://pinky-log-pipeline-delta/delta/silver/logs_cleaned/'
COMMENT 'Silver: cleaned, typed, deduplicated log events';

-- ── Gold: Daily KPIs ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.daily_kpis (
    event_date            DATE          NOT NULL,
    service               STRING        NOT NULL,
    region                STRING        NOT NULL,
    total_requests        BIGINT,
    total_errors          BIGINT,
    error_rate_pct        DOUBLE,
    avg_latency_ms        DOUBLE,
    p95_latency_ms        DOUBLE,
    p99_latency_ms        DOUBLE,
    unique_users          BIGINT,
    unique_sessions       BIGINT,
    total_request_bytes   BIGINT,
    total_response_bytes  BIGINT,
    _computed_at          TIMESTAMP
)
USING DELTA
PARTITIONED BY (event_date)
LOCATION 's3://pinky-log-pipeline-delta/delta/gold/daily_kpis/'
COMMENT 'Gold: daily KPI aggregates per service and region';

-- ── Gold: Session Summary ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.session_summary (
    derived_session_id    STRING        NOT NULL,
    user_id               STRING,
    event_date            DATE,
    session_start         TIMESTAMP,
    session_end           TIMESTAMP,
    total_events          BIGINT,
    session_duration_mins DOUBLE,
    unique_endpoints      BIGINT,
    errors_in_session     BIGINT,
    avg_latency_ms        DOUBLE,
    region                STRING,
    _computed_at          TIMESTAMP
)
USING DELTA
PARTITIONED BY (event_date)
LOCATION 's3://pinky-log-pipeline-delta/delta/gold/session_summary/'
COMMENT 'Gold: per-user session summaries';
