# 🚀 Real-Time Log Analytics Pipeline

> An end-to-end data engineering project built on **AWS + Databricks + PySpark + Delta Lake**  
>  Author: Pinky Somwani

---

## 📌 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Pipeline Stages](#pipeline-stages)
- [Setup & Installation](#setup--installation)
- [Running the Pipeline](#running-the-pipeline)
- [Dashboard & Outputs](#dashboard--outputs)
- [Key Concepts Demonstrated](#key-concepts-demonstrated)
- [Week-by-Week Build Plan](#week-by-week-build-plan)

---

## Overview

This project simulates a **production-grade log analytics pipeline** that ingests application/server logs, processes them using distributed computing, and serves business insights via a dashboard.

**Use Case:** A tech platform wants to monitor user activity, detect error spikes, measure API latency, and track session behaviour — all in near real-time.

**Why this project?**
- Mirrors real-world data engineering engagements at consulting firms
- Covers the full data lifecycle: ingestion → transformation → storage → serving
- Showcases best practices: Delta Lake ACID transactions, PySpark optimization, layered S3 architecture

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        REAL-TIME LOG ANALYTICS PIPELINE                     │
└─────────────────────────────────────────────────────────────────────────────┘

  ┌──────────────┐     ┌──────────────────────────────────────────────────┐
  │  DATA SOURCE │     │                  AWS LAYER                       │
  │              │     │                                                  │
  │  Python Log  │────▶│  ┌─────────────┐      ┌──────────────────────┐  │
  │  Simulator   │     │  │  AWS Kinesis │─────▶│      AWS S3          │  │
  │              │     │  │  (Streaming) │      │                      │  │
  │  - App logs  │     │  └─────────────┘      │  /raw/       (Bronze)│  │
  │  - API calls │     │                        │  /processed/ (Silver)│  │
  │  - Errors    │     │  ┌─────────────┐      │  /curated/   (Gold)  │  │
  │  - Latency   │─────│─▶│   AWS IAM   │      └──────────────────────┘  │
  └──────────────┘     │  │  (Security) │                                 │
                       │  └─────────────┘                                 │
                       └──────────────────────────────────────────────────┘
                                              │
                                              ▼
                       ┌──────────────────────────────────────────────────┐
                       │              DATABRICKS LAYER                    │
                       │                                                  │
                       │  ┌──────────────────────────────────────────┐   │
                       │  │           PySpark Processing             │   │
                       │  │                                          │   │
                       │  │  Bronze ──▶ Silver ──▶ Gold             │   │
                       │  │                                          │   │
                       │  │  • Schema enforcement                    │   │
                       │  │  • Null / quality checks                 │   │
                       │  │  • Sessionization logic                  │   │
                       │  │  • Aggregations & KPIs                   │   │
                       │  │  • Broadcast joins & optimisation        │   │
                       │  └──────────────────────────────────────────┘   │
                       │                   │                              │
                       │                   ▼                              │
                       │  ┌──────────────────────────────────────────┐   │
                       │  │            Delta Lake Tables             │   │
                       │  │                                          │   │
                       │  │  • ACID transactions                     │   │
                       │  │  • Time travel / versioning              │   │
                       │  │  • MERGE (SCD Type 2 / upserts)         │   │
                       │  │  • Partition pruning                     │   │
                       │  └──────────────────────────────────────────┘   │
                       │                   │                              │
                       │                   ▼                              │
                       │  ┌──────────────────────────────────────────┐   │
                       │  │        Databricks Workflows              │   │
                       │  │  (Scheduled job orchestration)           │   │
                       │  └──────────────────────────────────────────┘   │
                       └──────────────────────────────────────────────────┘
                                              │
                                              ▼
                       ┌──────────────────────────────────────────────────┐
                       │              SERVING LAYER                       │
                       │                                                  │
                       │   Databricks SQL Dashboard  /  Power BI         │
                       │                                                  │
                       │   • Error rate trends          • Top endpoints  │
                       │   • Session duration KPIs      • Latency P95    │
                       │   • Daily active users         • Anomaly alerts │
                       └──────────────────────────────────────────────────┘
```

### Data Flow (Medallion Architecture)

```
RAW LOGS (JSON)
     │
     ▼
┌─────────┐     ┌──────────────────────────────────────────────────┐
│ BRONZE  │────▶│ Raw ingestion, schema-on-read, no transformations│
│  Layer  │     │ S3: s3://bucket/raw/logs/year=*/month=*/day=*/   │
└─────────┘     └──────────────────────────────────────────────────┘
     │
     ▼
┌─────────┐     ┌──────────────────────────────────────────────────┐
│ SILVER  │────▶│ Cleaned, typed, deduplicated, quality-checked    │
│  Layer  │     │ Delta Table: silver.logs_cleaned                 │
└─────────┘     └──────────────────────────────────────────────────┘
     │
     ▼
┌─────────┐     ┌──────────────────────────────────────────────────┐
│  GOLD   │────▶│ Business-level aggregates, KPIs, ready to serve  │
│  Layer  │     │ Delta Table: gold.session_summary, gold.kpis     │
└─────────┘     └──────────────────────────────────────────────────┘
```

---

## Tech Stack

| Technology | Version | Purpose |
|---|---|---|
| **AWS S3** | — | Data lake storage (raw / processed / curated zones) |
| **AWS Kinesis** | — | Real-time log stream ingestion |
| **AWS IAM** | — | Secure access control & role management |
| **Databricks** | Runtime 13.x+ | Cluster management, notebooks, workflow orchestration |
| **Apache Spark / PySpark** | 3.4+ | Distributed data transformation |
| **Delta Lake** | 2.x+ | ACID transactions, time travel, MERGE operations |
| **Python** | 3.10+ | Log simulation scripts, utilities |
| **Databricks SQL** | — | Dashboard and ad-hoc querying |

---

## Project Structure

```
log-analytics-pipeline/
│
├── 📁 data_simulator/
│   ├── generate_logs.py          # Simulates app/server log events
│   ├── schema_definitions.py     # Log schema definitions
│   └── upload_to_s3.py           # Batches logs to S3 raw zone
│
├── 📁 ingestion/
│   ├── kinesis_producer.py       # Streams events to AWS Kinesis
│   ├── s3_batch_loader.py        # Batch upload fallback
│   └── config.py                 # AWS credentials & bucket config
│
├── 📁 transformations/
│   ├── 01_bronze_ingestion.py    # Raw → Bronze (schema enforcement)
│   ├── 02_silver_cleaning.py     # Bronze → Silver (cleaning, dedup)
│   ├── 03_gold_aggregations.py   # Silver → Gold (KPIs, session logic)
│   └── utils/
│       ├── data_quality.py       # Null checks, schema drift detection
│       └── spark_helpers.py      # Reusable PySpark utilities
│
├── 📁 delta_lake/
│   ├── table_definitions.sql     # Delta table CREATE statements
│   ├── merge_scd2.py             # SCD Type 2 MERGE logic
│   └── optimize_vacuum.py        # OPTIMIZE & VACUUM scripts
│
├── 📁 databricks_jobs/
│   ├── pipeline_workflow.json    # Databricks Workflow definition
│   └── job_config.yaml           # Job cluster settings
│
├── 📁 dashboards/
│   ├── kpi_queries.sql           # SQL for Databricks SQL Dashboard
│   └── dashboard_export.json     # Exported dashboard definition
│
├── 📁 docs/
│   ├── architecture_diagram.png  # Visual architecture diagram
│   ├── data_dictionary.md        # Column definitions & lineage
│   └── setup_guide.md            # Step-by-step environment setup
│
├── requirements.txt              # Python dependencies
├── .env.example                  # Environment variable template
└── README.md                     # This file
```

---

## Pipeline Stages

### Stage 1 — Log Simulation
Generates realistic JSON log events including:
- **User events:** page views, clicks, searches
- **API calls:** endpoint, method, response code, latency (ms)
- **Errors:** 4xx / 5xx with stack trace fragments
- **Sessions:** user_id, session_id, timestamps

### Stage 2 — Ingestion to S3
- Logs streamed via **AWS Kinesis Data Streams** or batch-uploaded to **S3**
- Raw files land in `s3://bucket/raw/logs/` partitioned by `year/month/day`
- IAM roles restrict access — no credentials hardcoded

### Stage 3 — Bronze Layer (PySpark)
- Read raw JSON from S3 using PySpark with schema enforcement
- Cast types, add ingestion metadata (`_ingested_at`, `_source_file`)
- Write to Delta table: `bronze.raw_logs`

### Stage 4 — Silver Layer (PySpark)
- Remove duplicates using `dropDuplicates()` on composite key
- Null checks on critical fields; quarantine bad records
- Parse nested JSON fields, standardise timestamps to UTC
- Write to Delta table: `silver.logs_cleaned`

### Stage 5 — Gold Layer (PySpark)
- **Sessionization:** group events into sessions (30-min idle timeout)
- **KPI aggregations:** error rates, p95 latency, daily active users
- **Incremental MERGE:** upsert using Delta Lake `MERGE INTO`
- Write to Delta tables: `gold.session_summary`, `gold.daily_kpis`

### Stage 6 — Orchestration
- Databricks Workflow runs stages 3→5 in sequence
- Scheduled to run every hour (configurable)
- Email alerts on failure

---

## Setup & Installation

### Prerequisites
- AWS account with S3, Kinesis, and IAM access
- Databricks workspace (Community Edition works for dev)
- Python 3.10+

### 1. Clone the repository
```bash
git clone https://github.com/yourusername/log-analytics-pipeline.git
cd log-analytics-pipeline
```

### 2. Install Python dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure environment variables
```bash
cp .env.example .env
# Edit .env with your AWS credentials and S3 bucket names
```

### 4. Set up AWS resources
```bash
# Create S3 buckets
aws s3 mb s3://your-bucket-raw
aws s3 mb s3://your-bucket-processed

# Create Kinesis stream
aws kinesis create-stream --stream-name log-stream --shard-count 1
```

### 5. Configure Databricks
- Mount S3 bucket in Databricks using instance profiles or secret scope
- Import notebooks from `/transformations/` into your Databricks workspace
- Update `config.py` with your cluster and bucket details

---

## Running the Pipeline

### Step 1: Generate and upload logs
```bash
python data_simulator/generate_logs.py --num-events 10000
python data_simulator/upload_to_s3.py --bucket your-bucket-raw
```

### Step 2: Run transformations (manual)
```bash
# Run in Databricks notebooks in order:
# 01_bronze_ingestion.py → 02_silver_cleaning.py → 03_gold_aggregations.py
```

### Step 3: Schedule via Databricks Workflow
- Import `databricks_jobs/pipeline_workflow.json` into Databricks Workflows
- Set trigger schedule (e.g., every hour)
- Monitor run history in the Databricks UI

---

## Dashboard & Outputs

Databricks SQL Dashboard includes:

| Metric | Description |
|---|---|
| **Error Rate (%)** | % of API calls returning 4xx/5xx, trended by hour |
| **P95 API Latency** | 95th percentile response time per endpoint |
| **Daily Active Users** | Unique users per day with 7-day rolling average |
| **Session Duration** | Average and median session lengths |
| **Top Endpoints** | Most called API endpoints by volume |
| **Error Heatmap** | Error distribution by hour-of-day and day-of-week |

---

## Key Concepts Demonstrated

| Concept | Where |
|---|---|
| **Medallion Architecture** (Bronze/Silver/Gold) | `transformations/` |
| **Delta Lake MERGE** (SCD Type 2 / upserts) | `delta_lake/merge_scd2.py` |
| **PySpark Optimisation** (broadcast joins, caching, partitioning) | `transformations/utils/` |
| **Data Quality Checks** (nulls, schema drift) | `transformations/utils/data_quality.py` |
| **AWS IAM Least Privilege** | `docs/setup_guide.md` |
| **Incremental Processing** (avoid full reloads) | `03_gold_aggregations.py` |
| **Job Orchestration** (Databricks Workflows) | `databricks_jobs/` |
| **Delta Time Travel** (audit & rollback) | `delta_lake/` |

---

## Week-by-Week Build Plan

| Week | Focus | Deliverables |
|---|---|---|
| **Week 1** | AWS setup + data simulation | S3 buckets, IAM roles, log simulator, raw data in S3 |
| **Week 2** | Databricks + PySpark processing | Bronze & Silver Delta tables, cleaning pipeline |
| **Week 3** | Advanced features | Gold layer, MERGE logic, Databricks Workflow, optimisation |
| **Week 4** | Polish + docs | Dashboard, README, GitHub, walkthrough deck |



