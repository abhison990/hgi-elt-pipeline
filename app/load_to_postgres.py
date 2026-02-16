import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql://hgi:hgi@localhost:5432/hgi")

df = pd.read_csv("data/raw/customer_support_tickets.csv")

df.to_sql(
    "raw_customer_support",
    engine,
    schema="staging",
    if_exists="replace",
    index=False
)

print(f"Loaded {len(df)} rows into staging.raw_customer_support")
