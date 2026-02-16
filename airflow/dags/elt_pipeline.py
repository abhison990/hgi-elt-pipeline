from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

default_args = {
    "owner": "hgi",
    "start_date": datetime(2024, 1, 1),
}


def load_csv():
    engine = create_engine("postgresql://hgi:hgi@postgres:5432/hgi")
    df = pd.read_csv("/opt/airflow/data/raw/customer_support_tickets.csv")

    df.to_sql(
        "raw_customer_support",
        engine,
        schema="staging",
        if_exists="replace",
        index=False,
    )

with DAG(
    dag_id="elt_pipeline",
    schedule_interval="0 * * * *",  # hourly
    catchup=False,
    default_args=default_args,
) as dag:

    create_schemas = PostgresOperator(
        task_id="create_schemas",
        postgres_conn_id="hgi_postgres",
        sql="""
              CREATE SCHEMA IF NOT EXISTS staging;
              CREATE SCHEMA IF NOT EXISTS mart;
          """
    )

    load_task = PythonOperator(
        task_id="load_csv_to_staging",
        python_callable=load_csv,
    )

    transform_task = PostgresOperator(
        task_id="transform_data",
        postgres_conn_id="hgi_postgres",
        sql="""
        CREATE EXTENSION IF NOT EXISTS pgcrypto;

        DROP TABLE IF EXISTS staging.cleaned_customer_support;

        CREATE TABLE staging.cleaned_customer_support AS
        SELECT
            "Ticket ID"::int AS ticket_id,
            LEFT("Customer Name", 1) || '***' AS customer_name_masked,
            encode(digest(lower(trim("Customer Email")) || 'pepper', 'sha256'), 'hex') AS customer_email_hash,
            "Customer Age"::int AS customer_age,
            COALESCE(NULLIF("Customer Gender", ''), 'Unknown') AS customer_gender,
            "Product Purchased" AS product_purchased,
            TO_DATE("Date of Purchase", 'DD/MM/YY') AS date_of_purchase,
            "Ticket Type" AS ticket_type,
            "Ticket Subject" AS ticket_subject,
            COALESCE("Ticket Status", 'Open') AS ticket_status,
            COALESCE("Resolution", 'Unresolved') AS resolution,
            COALESCE("Ticket Priority", 'Medium') AS ticket_priority,
            COALESCE("Ticket Channel", 'Unknown') AS ticket_channel,
            COALESCE("Customer Satisfaction Rating", 0)::int AS customer_satisfaction_rating,
            CURRENT_TIMESTAMP AS processed_at
        FROM staging.raw_customer_support;
        """,
    )

    mart_task = PostgresOperator(
        task_id="build_mart",
        postgres_conn_id="hgi_postgres",
        sql="""
        DROP TABLE IF EXISTS mart.ticket_summary;

        CREATE TABLE mart.ticket_summary AS
        SELECT
            date_of_purchase,
            ticket_type,
            ticket_priority,
            ticket_channel,
            COUNT(*) AS total_tickets,
            AVG(customer_satisfaction_rating) AS avg_satisfaction
        FROM staging.cleaned_customer_support
        GROUP BY 1,2,3,4;
        """,
    )

    dq_task = PostgresOperator(
        task_id="run_dq_checks",
        postgres_conn_id="hgi_postgres",
        sql="""
        DROP TABLE IF EXISTS staging.dq_results;

        CREATE TABLE staging.dq_results AS
        SELECT
            COUNT(*) AS total_records,
            COUNT(*) FILTER (WHERE customer_email_hash IS NULL) AS null_email_hash,
            COUNT(*) FILTER (WHERE ticket_id IS NULL) AS null_ticket_id,
            COUNT(*) FILTER (WHERE customer_age < 0) AS invalid_age,
            COUNT(*) FILTER (WHERE customer_satisfaction_rating NOT BETWEEN 0 AND 5) AS invalid_rating,
            CURRENT_TIMESTAMP AS dq_run_time
        FROM staging.cleaned_customer_support;
        """,
    )

    create_schemas >> load_task >> transform_task >> mart_task >> dq_task
