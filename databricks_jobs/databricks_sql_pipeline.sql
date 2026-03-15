-- =============================================================================
-- databricks_sql_pipeline.sql
-- Complete Bronze → Silver → Gold pipeline using ONLY Databricks SQL
-- Use this if you don't have an All-Purpose Cluster (Community Edition)
-- Run each block sequentially in Databricks SQL Editor
-- =============================================================================

-- ── STEP 0: Create Databases ──────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;


-- ── STEP 1: Bronze — point to raw S3 files ────────────────────────────────────
-- NOTE: Requires External Location setup in Catalog first
CREATE TABLE IF NOT EXISTS bronze.raw_logs
USING json
OPTIONS (
  path          "s3://pinky-log-pipeline-raw/raw/logs/",
  inferSchema   "true",
  multiLine     "false"
);

-- Verify Bronze
SELECT COUNT(*) AS bronze_rows FROM bronze.raw_logs;
SELECT * FROM bronze.raw_logs LIMIT 5;


-- ── STEP 2: Silver — clean and type-cast ─────────────────────────────────────
CREATE OR REPLACE TABLE silver.logs_cleaned AS
SELECT
    event_id,
    session_id,
    user_id,
    CAST(timestamp AS TIMESTAMP)                         AS event_timestamp,
    CAST(CAST(timestamp AS TIMESTAMP) AS DATE)           AS event_date,
    HOUR(CAST(timestamp AS TIMESTAMP))                   AS event_hour,
    service,
    endpoint,
    method,
    CAST(status_code AS INT)                             AS status_code,
    CASE
        WHEN CAST(status_code AS INT) BETWEEN 200 AND 299 THEN '2xx'
        WHEN CAST(status_code AS INT) BETWEEN 300 AND 399 THEN '3xx'
        WHEN CAST(status_code AS INT) BETWEEN 400 AND 499 THEN '4xx'
        WHEN CAST(status_code AS INT) BETWEEN 500 AND 599 THEN '5xx'
        ELSE 'unknown'
    END                                                  AS status_category,
    CAST(latency_ms AS DOUBLE)                           AS latency_ms,
    region,
    CAST(request_size_bytes  AS BIGINT)                  AS request_size_bytes,
    CAST(response_size_bytes AS BIGINT)                  AS response_size_bytes,
    CAST(is_error AS BOOLEAN)                            AS is_error,
    error_message,
    trace_id,
    current_timestamp()                                  AS _ingested_at
FROM bronze.raw_logs
WHERE event_id   IS NOT NULL
  AND user_id    IS NOT NULL
  AND timestamp  IS NOT NULL
  AND status_code IS NOT NULL;

-- Verify Silver
SELECT COUNT(*) AS silver_rows FROM silver.logs_cleaned;
SELECT status_category, COUNT(*) AS cnt FROM silver.logs_cleaned GROUP BY status_category;


-- ── STEP 3: Gold — Daily KPIs ─────────────────────────────────────────────────
CREATE OR REPLACE TABLE gold.daily_kpis AS
SELECT
    event_date,
    service,
    region,
    COUNT(*)                                                AS total_requests,
    SUM(CAST(is_error AS INT))                              AS total_errors,
    ROUND(SUM(CAST(is_error AS INT)) / COUNT(*) * 100, 2)   AS error_rate_pct,
    ROUND(AVG(latency_ms), 2)                               AS avg_latency_ms,
    ROUND(PERCENTILE(latency_ms, 0.95), 2)                  AS p95_latency_ms,
    ROUND(PERCENTILE(latency_ms, 0.99), 2)                  AS p99_latency_ms,
    COUNT(DISTINCT user_id)                                 AS unique_users,
    COUNT(DISTINCT session_id)                              AS unique_sessions,
    SUM(request_size_bytes)                                 AS total_request_bytes,
    SUM(response_size_bytes)                                AS total_response_bytes,
    current_timestamp()                                     AS _computed_at
FROM silver.logs_cleaned
GROUP BY event_date, service, region;

-- Verify Gold KPIs
SELECT * FROM gold.daily_kpis ORDER BY event_date DESC LIMIT 10;


-- ── STEP 4: Gold — Session Summary ───────────────────────────────────────────
CREATE OR REPLACE TABLE gold.session_summary AS
SELECT
    CONCAT(user_id, '_', session_id)            AS derived_session_id,
    user_id,
    session_id,
    event_date,
    MIN(event_timestamp)                        AS session_start,
    MAX(event_timestamp)                        AS session_end,
    COUNT(*)                                    AS total_events,
    ROUND(
        (UNIX_TIMESTAMP(MAX(event_timestamp)) -
         UNIX_TIMESTAMP(MIN(event_timestamp))) / 60, 2
    )                                           AS session_duration_mins,
    COUNT(DISTINCT endpoint)                    AS unique_endpoints,
    SUM(CAST(is_error AS INT))                  AS errors_in_session,
    ROUND(AVG(latency_ms), 2)                   AS avg_latency_ms,
    FIRST(region)                               AS region,
    current_timestamp()                         AS _computed_at
FROM silver.logs_cleaned
GROUP BY user_id, session_id, event_date;

-- Verify Session Summary
SELECT * FROM gold.session_summary ORDER BY session_duration_mins DESC LIMIT 10;


-- ── STEP 5: Pipeline Health Check ────────────────────────────────────────────
SELECT 'bronze.raw_logs'      AS table_name, COUNT(*) AS rows, MAX(_ingested_at) AS last_updated FROM bronze.raw_logs
UNION ALL
SELECT 'silver.logs_cleaned', COUNT(*), MAX(_ingested_at) FROM silver.logs_cleaned
UNION ALL
SELECT 'gold.daily_kpis',     COUNT(*), MAX(_computed_at) FROM gold.daily_kpis
UNION ALL
SELECT 'gold.session_summary',COUNT(*), MAX(_computed_at) FROM gold.session_summary;
