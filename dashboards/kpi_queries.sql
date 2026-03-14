-- =============================================================================
-- kpi_queries.sql
-- Databricks SQL Dashboard queries
-- Paste each query into a separate Databricks SQL widget
-- =============================================================================


-- ── 1. Error Rate Trend (last 7 days) ────────────────────────────────────────

SELECT
    event_date,
    service,
    SUM(total_requests)   AS total_requests,
    SUM(total_errors)     AS total_errors,
    ROUND(SUM(total_errors) / SUM(total_requests) * 100, 2) AS error_rate_pct
FROM gold.daily_kpis
WHERE event_date >= CURRENT_DATE - INTERVAL 7 DAYS
GROUP BY event_date, service
ORDER BY event_date DESC, error_rate_pct DESC;


-- ── 2. P95 Latency by Service ─────────────────────────────────────────────────

SELECT
    event_date,
    service,
    region,
    ROUND(AVG(avg_latency_ms), 2) AS avg_latency_ms,
    ROUND(MAX(p95_latency_ms), 2) AS p95_latency_ms,
    ROUND(MAX(p99_latency_ms), 2) AS p99_latency_ms
FROM gold.daily_kpis
WHERE event_date >= CURRENT_DATE - INTERVAL 7 DAYS
GROUP BY event_date, service, region
ORDER BY p95_latency_ms DESC;


-- ── 3. Daily Active Users (with 7-day rolling average) ───────────────────────

SELECT
    event_date,
    SUM(unique_users) AS daily_active_users,
    ROUND(AVG(SUM(unique_users)) OVER (
        ORDER BY event_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 0) AS rolling_7d_avg
FROM gold.daily_kpis
GROUP BY event_date
ORDER BY event_date DESC;


-- ── 4. Top 10 Endpoints by Request Volume ────────────────────────────────────

SELECT
    endpoint,
    COUNT(*)                                 AS total_requests,
    ROUND(AVG(latency_ms), 2)                AS avg_latency_ms,
    ROUND(SUM(CAST(is_error AS INT)) /
          COUNT(*) * 100, 2)                 AS error_rate_pct,
    COUNT(DISTINCT user_id)                  AS unique_users
FROM silver.logs_cleaned
WHERE event_date >= CURRENT_DATE - INTERVAL 1 DAY
GROUP BY endpoint
ORDER BY total_requests DESC
LIMIT 10;


-- ── 5. Error Heatmap (Hour of Day × Day of Week) ─────────────────────────────

SELECT
    DAYOFWEEK(event_date)                    AS day_of_week,   -- 1=Sun, 7=Sat
    DATE_FORMAT(event_timestamp, 'EEEE')     AS day_name,
    event_hour,
    COUNT(*)                                 AS total_requests,
    SUM(CAST(is_error AS INT))               AS total_errors,
    ROUND(SUM(CAST(is_error AS INT)) /
          COUNT(*) * 100, 2)                 AS error_rate_pct
FROM silver.logs_cleaned
WHERE event_date >= CURRENT_DATE - INTERVAL 7 DAYS
GROUP BY DAYOFWEEK(event_date), DATE_FORMAT(event_timestamp, 'EEEE'), event_hour
ORDER BY day_of_week, event_hour;


-- ── 6. Session Duration Distribution ─────────────────────────────────────────

SELECT
    CASE
        WHEN session_duration_mins < 1   THEN '< 1 min'
        WHEN session_duration_mins < 5   THEN '1-5 mins'
        WHEN session_duration_mins < 15  THEN '5-15 mins'
        WHEN session_duration_mins < 30  THEN '15-30 mins'
        ELSE '> 30 mins'
    END                                      AS duration_bucket,
    COUNT(*)                                 AS session_count,
    ROUND(AVG(total_events), 1)              AS avg_events_per_session,
    ROUND(AVG(errors_in_session), 2)         AS avg_errors_per_session
FROM gold.session_summary
WHERE event_date >= CURRENT_DATE - INTERVAL 7 DAYS
GROUP BY duration_bucket
ORDER BY session_count DESC;


-- ── 7. Regional Traffic Split ─────────────────────────────────────────────────

SELECT
    region,
    SUM(total_requests)   AS total_requests,
    ROUND(SUM(total_requests) * 100.0 /
          SUM(SUM(total_requests)) OVER (), 2) AS traffic_pct,
    ROUND(AVG(avg_latency_ms), 2)             AS avg_latency_ms,
    ROUND(AVG(error_rate_pct), 2)             AS avg_error_rate_pct
FROM gold.daily_kpis
WHERE event_date >= CURRENT_DATE - INTERVAL 7 DAYS
GROUP BY region
ORDER BY total_requests DESC;


-- ── 8. Pipeline Health Check ──────────────────────────────────────────────────

SELECT
    'bronze.raw_logs'        AS table_name,
    COUNT(*)                 AS row_count,
    MAX(_ingested_at)        AS last_ingested_at,
    MAX(event_date)          AS latest_event_date
FROM bronze.raw_logs

UNION ALL

SELECT
    'silver.logs_cleaned',
    COUNT(*),
    MAX(_ingested_at),
    MAX(event_date)
FROM silver.logs_cleaned

UNION ALL

SELECT
    'gold.daily_kpis',
    COUNT(*),
    MAX(_computed_at),
    MAX(event_date)
FROM gold.daily_kpis;
