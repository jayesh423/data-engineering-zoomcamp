#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from time import time
from datetime import timedelta
import argparse
import os
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector

@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url):
    if url.endswith('.csv.gz'):
        csv_name="yellow_tripdata_2021-01.csv.gz"
    else:
        csv_name="output.csv"

    os.system(f"wget {url} -O {csv_name}")

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)
    len(df)

    df.tpep_pickup_datetime=pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime=pd.to_datetime(df.tpep_dropoff_datetime)

    return df

@task(log_prints=True)
def transform_data(df):
    print(f"pre:missing passenger count:{df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] !=0 ]
    print(f"post:missing passenger count:{df['passenger_count'].isin([0]).sum()}")
    return df


@task(log_prints=True, retries=3)
def ingest_data(user, password, host, port, db, table_name, df):
    # engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    connection_block = SqlAlchemyConnector.load("postgres-connector")
    with connection_block.get_connection(begin=False) as engine:
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        df.to_sql(name=table_name, con=engine, if_exists='append')

    # while True:
    #     try:
    #         t_start=time()
    #         df=next(df_iter)
    #         df.tpep_pickup_datetime=pd.to_datetime(df.tpep_pickup_datetime)
    #         df.tpep_dropoff_datetime=pd.to_datetime(df.tpep_dropoff_datetime)
    #         df.to_sql(name=table_name, con=engine, if_exists='append')
    #         t_end = time()
    #         print("inserted another chunk..., took %.3f second" % (t_end-t_start))
    #     except StopIteration:
    #         print("Finished ingesting data into postgres")
    #         break

@flow(name="Subflow",log_prints=True)
def log_subflow(params):
    print("Logging subflow for user: {0}".format(params.user))

@flow(name="Ingest Flow")
def main_flow(params):
    user=params.user
    password=params.password
    host=params.host
    port=params.port
    db=params.db
    table_name=params.table_name
    url=params.url

    log_subflow(params)
    raw_data = extract_data(url)
    data = transform_data(raw_data)
    ingest_data(user, password, host, port, db, table_name, data)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Ingest CSV")

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='target table name for postgres')
    parser.add_argument('--url', help='File path for CSV')

    params = parser.parse_args()
    main_flow(params)



        