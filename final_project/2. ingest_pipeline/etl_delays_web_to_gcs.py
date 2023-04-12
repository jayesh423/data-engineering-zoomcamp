import os 
from pathlib import Path
import pandas as pd
# from prefect import flow, task
# from prefect_gcp.cloud_storage import GcsBucket
from random import randint

# @task(retries=3, log_prints=True)
def fetch(dataset_url:str) -> pd.DataFrame:
    """ Read data from web into pandas dataframe"""
    # To test retries 
    # if randint(0,1) > 0:
    #     raise Exception

    print(f"dataset url: {dataset_url}")
    df=pd.read_csv(dataset_url)
    return df

# @task(log_prints=True)
def clean(df=pd.DataFrame) -> pd.DataFrame:
    """ Fix dtype issues """
    df['ORIGIN_DEPARTURE_DATE'] = pd.to_datetime(df['ORIGIN_DEPARTURE_DATE'])
    df['INCIDENT_CREATE_DATE'] = pd.to_datetime(df['INCIDENT_CREATE_DATE'])
    df['INCIDENT_START_DATETIME'] = pd.to_datetime(df['INCIDENT_START_DATETIME'])
    df['INCIDENT_END_DATETIME'] = pd.to_datetime(df['INCIDENT_END_DATETIME'])
    df['EVENT_DATETIME'] = pd.to_datetime(df['EVENT_DATETIME'])

    print(df.head(2))

    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

# # @task()
def write_local(df: pd.DataFrame, year_str:str, dataset_file:str) -> Path:
    """ Write Dataframe out locally as parquet file """
    f=f"data/{year_str}"
    path = Path(f"data/{year_str}/{dataset_file}.parquet")
    if not os.path.exists(f):
        print('Folder doesnt exist. Creating folder {0}'.format(f))
        os.makedirs(f)
    df.to_parquet(path, compression="gzip")
    return path

# # @task()
# def write_gcs(path:Path) ->None:
#     """ Upload local parquet file to GCS """
#     gcp_cloud_storage_bucket_block = GcsBucket.load("gcs-connector")
#     gcp_cloud_storage_bucket_block.upload_from_path(
#         from_path=path,
#         to_path=path
#     )
#     return

# @flow(log_prints=True)
def etl_delays_web_to_gcs():
    """ The main ETL Function """
    year = 2018
    year_str = str(year) + "-" + str((year % 1000) + 1)
    max_index = 12 if year == 2022 else 14
    for i in range(1, max_index):
        index = f"P{i:02}"
        dataset_file = f"All-Delays-{year_str}-{index}"  # All-delays-2021-22-P01
        # dataset_url=f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
        dataset_url = f"https://sacuksprodnrdigital0001.blob.core.windows.net/historic-delay-attribution/{year_str}/{dataset_file}.zip"
        print(dataset_url)

        df = fetch(dataset_url)
        df_clean = clean(df)
        path = write_local(df_clean, year_str, dataset_file)
        # write_gcs(path)
        break

if __name__ == '__main__':
    etl_delays_web_to_gcs()