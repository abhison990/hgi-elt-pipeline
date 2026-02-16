# HGI Customer Support ELT Pipeline

## Overview

This project demonstrates a complete ELT data pipeline using:

- PostgreSQL (Data Warehouse)
- Airflow (Orchestration)
- Docker (Containerization)
- Metabase (BI & Dashboarding)

The pipeline ingests raw customer support ticket data, transforms it into cleaned staging tables, 
builds analytical marts, and runs data quality checks.

---

## Architecture

CSV → Airflow → Postgres (staging → mart) → Metabase Dashboard

---

## Components

### 1. Postgres
Schemas:
- staging
- mart

Tables:
- staging.raw_customer_support
- staging.cleaned_customer_support
- staging.dq_results
- mart.ticket_summary

---

### 2. Airflow DAG

DAG: `elt_pipeline`

Tasks:
1. create_schemas
2. load_csv_to_staging
3. transform_data
4. build_mart
5. run_dq_checks

Schedule: Hourly

---

### 3. Transformations

- PII masking (customer name)
- Email hashing (SHA256 + pepper)
- Null handling
- Data type casting
- Aggregated mart creation

---

### 4. Data Quality Checks

- Null email hash
- Null ticket_id
- Invalid age
- Invalid satisfaction rating
- Record counts

Stored in:
staging.dq_results

---

### 5. BI Dashboard (Metabase)

Dashboard: HGI Customer Support Analytics

Includes:
- Ticket Volume Over Time (Line Chart)
- Additional charts can be added

---

## How to Run

### Start services
docker compose up -d

### Stop services
docker compose down

### Access

Airflow UI:
http://localhost:8080
Username: admin
Password: admin

Metabase:
http://localhost:3000

---

## Key Engineering Concepts Demonstrated

- ELT architecture
- Orchestration with Airflow
- SQL transformations
- Data mart modeling
- PII protection
- Data Quality validation
- Containerized deployment
- BI integration

---

Author: Abhijeet Sondkar
