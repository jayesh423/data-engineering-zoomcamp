from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_from_gcs(color:str, year:int, month:int) -> Path:
    """ Download Trip Data from GCS """
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("gcs-connector")
    gcs_block.get_directory(
        from_path=gcs_path,
        local_path=f"."
    )
    return gcs_path

@task()
def transform(path: Path) -> pd.DataFrame:
    """ Data Cleansing Example """
    df = pd.read_parquet(path)
    # print(f"pre:missing passenger count:{df['passenger_count'].isna().sum()}")
    # df['passenger_count'].fillna(0, inplace=True)
    # print(f"post:missing passenger count:{df['passenger_count'].isna().sum()}")
    return df

@task
def write_to_bq(df: pd.DataFrame) -> int :
    """ Write dataframe to Big query """

    gcp_credentials_block = GcpCredentials.load("gcp-creds")
    print(f"rows: {len(df)}")

    df.to_gbq(
        destination_table='dezoomcamp.rides' ,
        project_id='ny-rides-jpatel',
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )

    return len(df)

@flow()
def etl_gcs_to_bq(
    color: str = 'yellow',
    year: int = 2021,
    month: int = 1       
) -> int:
    """ Main ETL to load data into Big Query """
    # color="yellow"
    # year=2019
    # month=2

    path = extract_from_gcs(color,year,month)
    df = transform(path)
    row_count=write_to_bq(df)
    return row_count

@flow()
def etl_gcs_bq_parent(
    color: str = 'yellow',
    year: int = 2021,
    months: list[int] = [1,2]    
) -> None:
    row_processed = 0
    for month in months:
        row_processed += etl_gcs_to_bq(color, year, month)

    print(f"Total rows processed by this flow: {row_processed}")    

if __name__ == '__main__':
    # etl_gcs_to_bq()    
    color ="yellow"
    year=2019
    months=[2,3]    
    etl_gcs_bq_parent(color, year, months)    