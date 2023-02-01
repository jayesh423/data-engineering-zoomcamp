from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect.tasks import task_input_hash
from datetime import timedelta

@task(retries=3, log_prints=True, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url:str) -> pd.DataFrame:
    """ Read data from web into pandas dataframe"""
    # To test retries 
    # if randint(0,1) > 0:
    #     raise Exception

    df=pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df=pd.DataFrame) -> pd.DataFrame:
    """ Fix dtype issues """
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task()
def write_local(df: pd.DataFrame, color:str, dataset_file:str) -> Path:
    """ Write Dataframe out locally as parquet file """
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path

@task()
def write_gcs(path:Path) ->None:
    """ Upload local parquet file to GCS """
    gcp_cloud_storage_bucket_block = GcsBucket.load("gcs-connector")
    gcp_cloud_storage_bucket_block.upload_from_path(
        from_path=path,
        to_path=path
    )
    return

@flow(log_prints=True)
def etl_web_to_gcs(color:str, year:int, month:int) -> None:
    """ The main ETL Function """

    dataset_file=f"{color}_tripdata_{year}-{month:02}"
    dataset_url=f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)

@flow()
def etl_parent_flow(
    color: str = 'yellow',
    year: int = 2021,
    months: list[int] = [1,2]    
):
    for month in months:
        etl_web_to_gcs(color, year, month)


if __name__ == '__main__':
    
    color ="yellow"
    year=2021
    months=[1,2,3]    
    etl_parent_flow(color, year, months)
