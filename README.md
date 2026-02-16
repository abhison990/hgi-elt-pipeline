# HGI Customer Support ELT Pipeline

## Overview

This project demonstrates a production-oriented ELT pipeline using
open-source technologies and containerized deployment.

The pipeline:

-   Ingests customer support ticket data from CSV
-   Loads raw data into a staging schema
-   Transforms and anonymizes PII fields
-   Builds analytical mart tables
-   Executes automated data quality checks
-   Orchestrates execution via Airflow (hourly schedule)
-   Visualizes results using Metabase
-   Runs entirely via Docker Compose

This solution is designed from a seasoned data engineer's perspective,
emphasizing modularity, observability, governance, and scalability.

------------------------------------------------------------------------

## Architecture

CSV (Raw Data)\
↓\
Airflow (Orchestration)\
↓\
PostgreSQL\
- staging layer\
- mart layer\
- dq layer\
↓\
Metabase (BI Dashboard)

------------------------------------------------------------------------

## Technology Stack

-   PostgreSQL 16 -- Data Warehouse
-   Apache Airflow -- Orchestration
-   Metabase -- BI & Dashboarding
-   Docker Compose -- Containerization
-   Python + SQL -- Data Processing

------------------------------------------------------------------------

## Data Layer Design

### Staging Layer

Tables: - staging.raw_customer_support -
staging.cleaned_customer_support - staging.dq_results

Purpose: - Preserve raw ingestion - Perform transformations - Store DQ
outputs - Maintain auditability

------------------------------------------------------------------------

### Mart Layer

Table: - mart.ticket_summary

Purpose: - Aggregated analytics model - Optimized for reporting -
Decoupled from raw ingestion logic

------------------------------------------------------------------------

## PII Protection Strategy

Sensitive fields handled as:

-   Customer Name → Masked (First letter + \*\*\*)
-   Customer Email → SHA256 hash with pepper
-   Age → Cast & validated
-   Null values → Handled via COALESCE

Example hashing logic:

encode(digest(lower(trim("Customer Email")) \|\| 'pepper', 'sha256'),
'hex')

------------------------------------------------------------------------

## Data Quality Checks

-   Null email hash
-   Null ticket_id
-   Invalid age
-   Invalid rating
-   Record counts

Stored in: staging.dq_results

------------------------------------------------------------------------

## Incremental Strategy (Design Explanation)

Current implementation performs full refresh due to static CSV dataset.

In production, the following incremental strategy would be implemented:

1.  Maintain control.pipeline_watermark table
2.  Track last_ticket_id processed
3.  Insert new records using: INSERT ... ON CONFLICT DO UPDATE
4.  Incrementally update mart tables by deleting only impacted
    partitions

This ensures scalability, idempotency, and efficient processing.

------------------------------------------------------------------------

## Airflow DAG

DAG Name: elt_pipeline

Tasks: 1. create_schemas 2. load_csv_to_staging 3. transform_data 4.
build_mart 5. run_dq_checks

Schedule: Hourly (0 \* \* \* \*)

Features: - Retry logic - Task-level logging - Clear dependency chaining

------------------------------------------------------------------------

## How to Run Locally

### Prerequisites

Docker Desktop installed and running.

### Start Services

docker compose up -d

### Access

Airflow: http://localhost:8080 Username: admin Password: admin

Metabase: http://localhost:3000

### Stop Services

docker compose down

To remove volumes:

docker compose down -v

------------------------------------------------------------------------

## Engineering Principles Demonstrated

-   ELT architecture
-   Layered modeling
-   Orchestration best practices
-   PII anonymization
-   Data Quality validation
-   Containerized reproducibility
-   Incremental-ready design

------------------------------------------------------------------------

Author: Abhijeet Sondkar Senior Data Engineer
