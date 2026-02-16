"""
Local ingestion script to load raw CSV data into Postgres staging schema.

This script is intended for manual/local execution outside Airflow.
It reads the raw customer support CSV file and loads it into
staging.raw_customer_support.

Author: Abhijeet Sondkar
"""

import logging
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

DB_URI = "postgresql://hgi:hgi@localhost:5432/hgi"
CSV_PATH = Path("data/raw/customer_support_tickets.csv")


def load_raw_csv_to_staging(db_uri: str, csv_path: Path) -> None:
    """
    Load raw customer support CSV data into Postgres staging schema.

    Args:
        db_uri (str): SQLAlchemy database connection URI.
        csv_path (Path): Path to the raw CSV file.

    Raises:
        FileNotFoundError: If CSV file does not exist.
        Exception: If database load fails.
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found at {csv_path}")

    logging.info("Creating database engine.")
    engine = create_engine(db_uri)

    logging.info("Reading CSV file.")
    df = pd.read_csv(csv_path)
    logging.info("Loaded %d rows from CSV.", len(df))

    logging.info("Writing data to staging.raw_customer_support.")
    df.to_sql(
        "raw_customer_support",
        engine,
        schema="staging",
        if_exists="replace",
        index=False,
    )

    logging.info("Successfully loaded data into staging.raw_customer_support.")


if __name__ == "__main__":
    load_raw_csv_to_staging(DB_URI, CSV_PATH)
