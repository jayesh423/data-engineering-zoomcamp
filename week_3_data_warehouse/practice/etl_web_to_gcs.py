from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
import urllib.request
import os

@task()
def download_file(dataset_url:str, file_name:str) -> None:
    # filename, headers = urllib.request.urlretrieve(dataset_url, filename="fhv_tripdata_2019-01.csv.gz")
    url=f"{dataset_url}/{file_name}"
    filename, headers = urllib.request.urlretrieve(url,filename=f"data/{file_name}")
    # os.system(f"wget {dataset_url} -O {file_name}")

    print("File is downloaded")
    return


@task(log_prints=True)
def clean(df=pd.DataFrame) -> pd.DataFrame:
    """ Fix dtype issues """
    # df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    # df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
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
def write_gcs(dataset_file:str) ->None:
    """ Upload local parquet file to GCS """
    source_path = Path(f"data/{dataset_file}")
    target_path = Path(f"week3_data/{dataset_file}")
    gcp_cloud_storage_bucket_block = GcsBucket.load("gcs-connector")
    gcp_cloud_storage_bucket_block.upload_from_path(
        from_path=source_path,
        to_path=target_path
    )
    return

@flow(log_prints=True)
def etl_web_to_gcs():
    """ The main ETL Function """
    color ="yellow"
    year=2021
    month=1
    for year in [2019,2020,2021]:
        for month in range(1,13):
            dataset_file=f"fhv_tripdata_{year}-{month:02}.csv.gz"
            print(f"Processing file: {dataset_file}")
            # dataset_url=f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
            dataset_url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv"
            download_file(dataset_url, dataset_file)
            # df = fetch(dataset_url)
            # df_clean = clean(df)
            # path = write_local(df_clean, color, dataset_file)
            write_gcs(dataset_file)

if __name__ == '__main__':
    etl_web_to_gcs()