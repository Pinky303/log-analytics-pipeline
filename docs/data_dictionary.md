# Data Dictionary

## bronze.raw_logs
| Column | Type | Description |
|---|---|---|
| event_id | STRING | Unique event UUID |
| session_id | STRING | User session identifier |
| user_id | STRING | User identifier |
| timestamp | STRING | Raw UTC timestamp string |
| service | STRING | Microservice name |
| endpoint | STRING | API endpoint path |
| method | STRING | HTTP method |
| status_code | INT | HTTP response code |
| latency_ms | DOUBLE | Latency in milliseconds |
| region | STRING | AWS region |
| user_agent | STRING | Client user agent |
| request_size_bytes | BIGINT | Request payload bytes |
| response_size_bytes | BIGINT | Response payload bytes |
| is_error | BOOLEAN | True if status >= 400 |
| error_message | STRING | Error description |
| trace_id | STRING | Distributed trace ID |
| _ingested_at | TIMESTAMP | Pipeline ingestion time |
| _source_file | STRING | Source S3 file path |
| event_date | DATE | Partition column |

## silver.logs_cleaned
All Bronze columns plus:

| Column | Type | Description |
|---|---|---|
| event_timestamp | TIMESTAMP | Parsed UTC timestamp |
| event_hour | INT | Hour of event (0-23) |
| status_category | STRING | 2xx / 3xx / 4xx / 5xx |

## gold.daily_kpis
| Column | Type | Description |
|---|---|---|
| event_date | DATE | Aggregation date (PK) |
| service | STRING | Microservice (PK) |
| region | STRING | AWS region (PK) |
| total_requests | BIGINT | Total API calls |
| total_errors | BIGINT | Total error calls |
| error_rate_pct | DOUBLE | Error % |
| avg_latency_ms | DOUBLE | Mean latency |
| p95_latency_ms | DOUBLE | 95th percentile latency |
| p99_latency_ms | DOUBLE | 99th percentile latency |
| unique_users | BIGINT | Distinct users |
| unique_sessions | BIGINT | Distinct sessions |
| _computed_at | TIMESTAMP | Last computation time |

## gold.session_summary
| Column | Type | Description |
|---|---|---|
| derived_session_id | STRING | user_id + session_id key |
| user_id | STRING | User identifier |
| event_date | DATE | Session date (PK) |
| session_start | TIMESTAMP | First event in session |
| session_end | TIMESTAMP | Last event in session |
| total_events | BIGINT | Events in session |
| session_duration_mins | DOUBLE | Session length (mins) |
| unique_endpoints | BIGINT | Endpoints visited |
| errors_in_session | BIGINT | Errors during session |
| avg_latency_ms | DOUBLE | Mean latency in session |
| region | STRING | User region |
