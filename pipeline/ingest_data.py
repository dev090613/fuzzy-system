#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

year = 2021
month = 1
prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"
file_name = f"yellow_tripdata_{year}-{month:02d}.csv.gz"
url = prefix + file_name

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64",
}

parse_dates = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]


@click.command()
@click.option("--pg-user", "pg_user", required=True, help="PostgreSQL username")
@click.option("--pg-pass", "pg_pass", required=True, help="PostgreSQL password")
@click.option("--pg-host", "pg_host", default="localhost", help="PostgreSQL host")
@click.option("--pg-port", "pg_port", default="5432", help="PostgreSQL port")
@click.option("--pg-db", "pg_db", required=True, help="PostgreSQL database name")
@click.option("--target-table", "target_table", required=True, help="Target table name")
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table):

    chunksize = 100000
    engine = create_engine(
        f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    )

    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize,
    )

    first = True
    for df_chunk in tqdm(df_iter):
        if first:
            # Create table schema (no data)
            df_chunk.head(0).to_sql(name=target_table, con=engine, if_exists="replace")
            first = False
            print("Table created")

        # Insert chunk
        df_chunk.to_sql(name=target_table, con=engine, if_exists="append")

        print("Inserted:", len(df_chunk))


if __name__ == "__main__":
    run()
