-- =============================================================================
-- table_definitions.sql
-- Delta Lake table CREATE statements for all pipeline layers
-- Run once during initial environment setup in Databricks SQL
-- =============================================================================


-- ── Databases ─────────────────────────────────────────────────────────────────

CREATE DATABASE IF NOT EXISTS bronze COMMENT 'Raw ingested data — immutable';
CREATE DATABASE IF NOT EXISTS silver COMMENT 'Cleaned and validated data';
CREATE DATABASE IF NOT EXISTS gold   COMMENT 'Business-level aggregates and KPIs';


-- ── Bronze: Raw Logs ──────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS bronze.raw_logs (
    event_id              STRING        NOT NULL  COMMENT 'Unique event identifier (UUID)',
    session_id            STRING                  COMMENT 'User session identifier',
    user_id               STRING                  COMMENT 'User identifier',
    timestamp             STRING                  COMMENT 'Raw timestamp string from source',
    service               STRING                  COMMENT 'Microservice that emitted the log',
    endpoint              STRING                  COMMENT 'API endpoint path',
    method                STRING                  COMMENT 'HTTP method: GET, POST, PUT, DELETE',
    status_code           INT                     COMMENT 'HTTP response status code',
    latency_ms            DOUBLE                  COMMENT 'Request latency in milliseconds',
    region                STRING                  COMMENT 'AWS region',
    user_agent            STRING                  COMMENT 'Client user-agent string',
    request_size_bytes    BIGINT                  COMMENT 'Request payload size',
    response_size_bytes   BIGINT                  COMMENT 'Response payload size',
    is_error              BOOLEAN                 COMMENT 'True if status_code >= 400',
    error_message         STRING                  COMMENT 'Error description if applicable',
    trace_id              STRING                  COMMENT 'Distributed trace identifier',
    _ingested_at          TIMESTAMP               COMMENT 'When this record was ingested',
    _source_file          STRING                  COMMENT 'Source S3 file path',
    event_date            DATE                    COMMENT 'Partition column (derived from timestamp)'
)
USING DELTA
PARTITIONED BY (event_date)
LOCATION 's3a://your-bucket/delta/bronze/raw_logs/'
COMMENT 'Bronze layer: raw log events as received from S3';


-- ── Silver: Cleaned Logs ──────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS silver.logs_cleaned (
    event_id              STRING        NOT NULL  COMMENT 'Unique event identifier (UUID)',
    session_id            STRING                  COMMENT 'User session identifier',
    user_id               STRING                  COMMENT 'User identifier',
    event_timestamp       TIMESTAMP               COMMENT 'Parsed UTC event timestamp',
    event_date            DATE                    COMMENT 'Event date (partition key)',
    event_hour            INT                     COMMENT 'Hour of event (0-23)',
    service               STRING                  COMMENT 'Microservice name',
    endpoint              STRING                  COMMENT 'API endpoint path',
    method                STRING                  COMMENT 'HTTP method',
    status_code           INT                     COMMENT 'HTTP response status code',
    status_category       STRING                  COMMENT 'Status bucket: 2xx / 3xx / 4xx / 5xx',
    latency_ms            DOUBLE                  COMMENT 'Request latency in milliseconds',
    region                STRING                  COMMENT 'AWS region',
    request_size_bytes    BIGINT                  COMMENT 'Request payload size',
    response_size_bytes   BIGINT                  COMMENT 'Response payload size',
    is_error              BOOLEAN                 COMMENT 'True if status_code >= 400',
    error_message         STRING                  COMMENT 'Error description if applicable',
    trace_id              STRING                  COMMENT 'Distributed trace identifier',
    _ingested_at          TIMESTAMP               COMMENT 'Bronze ingestion time',
    _source_file          STRING                  COMMENT 'Source S3 file path'
)
USING DELTA
PARTITIONED BY (event_date)
LOCATION 's3a://your-bucket/delta/silver/logs_cleaned/'
COMMENT 'Silver layer: cleaned, typed, deduplicated log events';


-- ── Gold: Daily KPIs ──────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS gold.daily_kpis (
    event_date            DATE          NOT NULL  COMMENT 'Date of aggregation (partition key)',
    service               STRING        NOT NULL  COMMENT 'Microservice name',
    region                STRING        NOT NULL  COMMENT 'AWS region',
    total_requests        BIGINT                  COMMENT 'Total number of API requests',
    total_errors          BIGINT                  COMMENT 'Total number of error requests',
    error_rate_pct        DOUBLE                  COMMENT 'Error rate as percentage',
    avg_latency_ms        DOUBLE                  COMMENT 'Average request latency (ms)',
    p95_latency_ms        DOUBLE                  COMMENT '95th percentile latency (ms)',
    p99_latency_ms        DOUBLE                  COMMENT '99th percentile latency (ms)',
    unique_users          BIGINT                  COMMENT 'Distinct users active on this date',
    unique_sessions       BIGINT                  COMMENT 'Distinct sessions on this date',
    total_request_bytes   BIGINT                  COMMENT 'Total inbound data volume (bytes)',
    total_response_bytes  BIGINT                  COMMENT 'Total outbound data volume (bytes)',
    _computed_at          TIMESTAMP               COMMENT 'When this row was last computed'
)
USING DELTA
PARTITIONED BY (event_date)
LOCATION 's3a://your-bucket/delta/gold/daily_kpis/'
COMMENT 'Gold layer: daily KPI aggregates per service and region';


-- ── Gold: Session Summary ─────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS gold.session_summary (
    derived_session_id    STRING        NOT NULL  COMMENT 'Composite session key (user_id + session_num)',
    user_id               STRING                  COMMENT 'User identifier',
    event_date            DATE                    COMMENT 'Session date (partition key)',
    session_start         TIMESTAMP               COMMENT 'First event timestamp in session',
    session_end           TIMESTAMP               COMMENT 'Last event timestamp in session',
    total_events          BIGINT                  COMMENT 'Number of events in session',
    session_duration_mins DOUBLE                  COMMENT 'Session length in minutes',
    unique_endpoints      BIGINT                  COMMENT 'Distinct endpoints visited',
    errors_in_session     BIGINT                  COMMENT 'Number of errors during session',
    avg_latency_ms        DOUBLE                  COMMENT 'Average latency during session',
    region                STRING                  COMMENT 'User region',
    _computed_at          TIMESTAMP               COMMENT 'When this row was last computed'
)
USING DELTA
PARTITIONED BY (event_date)
LOCATION 's3a://your-bucket/delta/gold/session_summary/'
COMMENT 'Gold layer: per-user session summaries with behavioural metrics';
