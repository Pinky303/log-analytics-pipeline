-- =============================================================================
-- kpi_queries.sql
-- All dashboard queries for Databricks SQL Dashboard
-- Paste each query block into a separate visualization widget
-- =============================================================================


-- ── 1. Error Rate Trend (Line Chart) ─────────────────────────────────────────
-- X: event_date | Y: error_rate_pct | Group by: service
SELECT
    event_date,
    service,
    SUM(total_requests)  AS total_requests,
    SUM(total_errors)    AS total_errors,
    ROUND(SUM(total_errors) / SUM(total_requests) * 100, 2) AS error_rate_pct
FROM gold.daily_kpis
GROUP BY event_date, service
ORDER BY event_date DESC;


-- ── 2. P95 Latency by Service (Bar Chart) ────────────────────────────────────
-- X: service | Y: p95_latency_ms
SELECT
    event_date,
    service,
    region,
    ROUND(AVG(avg_latency_ms), 2) AS avg_latency_ms,
    ROUND(MAX(p95_latency_ms), 2) AS p95_latency_ms,
    ROUND(MAX(p99_latency_ms), 2) AS p99_latency_ms
FROM gold.daily_kpis
GROUP BY event_date, service, region
ORDER BY p95_latency_ms DESC;


-- ── 3. Daily Active Users (Area Chart) ───────────────────────────────────────
-- X: event_date | Y: daily_active_users
SELECT
    event_date,
    SUM(unique_users)    AS daily_active_users,
    SUM(unique_sessions) AS daily_sessions
FROM gold.daily_kpis
GROUP BY event_date
ORDER BY event_date ASC;


-- ── 4. Top 10 Endpoints by Traffic (Horizontal Bar) ──────────────────────────
-- X: total_requests | Y: endpoint
SELECT
    endpoint,
    COUNT(*)                              AS total_requests,
    ROUND(AVG(latency_ms), 2)             AS avg_latency_ms,
    ROUND(SUM(CAST(is_error AS INT)) /
          COUNT(*) * 100, 2)              AS error_rate_pct
FROM silver.logs_cleaned
GROUP BY endpoint
ORDER BY total_requests DESC
LIMIT 10;


-- ── 5. Traffic by Region (Pie Chart) ─────────────────────────────────────────
-- Label: region | Value: total_requests
SELECT
    region,
    SUM(total_requests)                                        AS total_requests,
    ROUND(SUM(total_requests) * 100.0 /
          SUM(SUM(total_requests)) OVER (), 2)                 AS traffic_pct,
    ROUND(AVG(avg_latency_ms), 2)                              AS avg_latency_ms,
    ROUND(AVG(error_rate_pct), 2)                              AS avg_error_rate_pct
FROM gold.daily_kpis
GROUP BY region
ORDER BY total_requests DESC;


-- ── 6. Error Heatmap (Hour × Day of Week) ────────────────────────────────────
-- X: event_hour | Y: day_name | Color: error_rate_pct
SELECT
    event_hour,
    CASE DAYOFWEEK(event_date)
        WHEN 1 THEN 'Sunday'    WHEN 2 THEN 'Monday'
        WHEN 3 THEN 'Tuesday'   WHEN 4 THEN 'Wednesday'
        WHEN 5 THEN 'Thursday'  WHEN 6 THEN 'Friday'
        WHEN 7 THEN 'Saturday'
    END                                     AS day_name,
    DAYOFWEEK(event_date)                   AS day_num,
    COUNT(*)                                AS total_requests,
    SUM(CAST(is_error AS INT))              AS total_errors,
    ROUND(SUM(CAST(is_error AS INT)) /
          COUNT(*) * 100, 2)                AS error_rate_pct
FROM silver.logs_cleaned
GROUP BY event_hour, DAYOFWEEK(event_date),
    CASE DAYOFWEEK(event_date)
        WHEN 1 THEN 'Sunday'    WHEN 2 THEN 'Monday'
        WHEN 3 THEN 'Tuesday'   WHEN 4 THEN 'Wednesday'
        WHEN 5 THEN 'Thursday'  WHEN 6 THEN 'Friday'
        WHEN 7 THEN 'Saturday'
    END
ORDER BY day_num, event_hour;


-- ── 7. Session Duration Distribution (Bar Chart) ─────────────────────────────
SELECT
    CASE
        WHEN session_duration_mins < 1   THEN '< 1 min'
        WHEN session_duration_mins < 5   THEN '1-5 mins'
        WHEN session_duration_mins < 15  THEN '5-15 mins'
        WHEN session_duration_mins < 30  THEN '15-30 mins'
        ELSE '> 30 mins'
    END                                  AS duration_bucket,
    COUNT(*)                             AS session_count,
    ROUND(AVG(total_events), 1)          AS avg_events_per_session,
    ROUND(AVG(errors_in_session), 2)     AS avg_errors_per_session
FROM gold.session_summary
GROUP BY duration_bucket
ORDER BY session_count DESC;


-- ── 8. Summary Counters (Counter Widgets) ────────────────────────────────────
-- Create one Counter widget per metric from this query
SELECT
    SUM(total_requests)              AS total_requests,
    SUM(total_errors)                AS total_errors,
    ROUND(AVG(error_rate_pct), 2)    AS avg_error_rate_pct,
    ROUND(AVG(avg_latency_ms), 2)    AS avg_latency_ms,
    SUM(unique_users)                AS total_users,
    SUM(unique_sessions)             AS total_sessions
FROM gold.daily_kpis;


-- ── 9. Pipeline Health Check ──────────────────────────────────────────────────
SELECT 'bronze.raw_logs'       AS table_name, COUNT(*) AS row_count, MAX(_ingested_at) AS last_updated FROM bronze.raw_logs
UNION ALL
SELECT 'silver.logs_cleaned',   COUNT(*), MAX(_ingested_at) FROM silver.logs_cleaned
UNION ALL
SELECT 'gold.daily_kpis',       COUNT(*), MAX(_computed_at) FROM gold.daily_kpis
UNION ALL
SELECT 'gold.session_summary',  COUNT(*), MAX(_computed_at) FROM gold.session_summary;
