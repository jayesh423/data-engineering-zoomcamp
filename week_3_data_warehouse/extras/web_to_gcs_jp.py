import io
import os
import requests
import pandas as pd
import pyarrow
from google.cloud import storage

"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

# services = ['fhv','green','yellow']
# init_url = 'https://nyc-tlc.s3.amazonaws.com/trip+data/'

# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", "dtc_data_lake_ny-rides-jpatel")


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def web_to_gcs(year, service):
    for i in range(1,13):
        
        # sets the month part of the file_name string
        month = '0'+str(i)
        month = month[-2:]

        # csv file_name 
        file_name = service + '_tripdata_' + year + '-' + month + '.csv.gz'

        print(f"Reading file: {file_name}")
        init_url=f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{service}/"

        # download it using requests via a pandas df
        request_url = init_url + file_name
        # read it back into a parquet file
        df=pd.read_csv(request_url)

        if service == 'yellow':
            df.tpep_pickup_datetime=pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime=pd.to_datetime(df.tpep_dropoff_datetime)            
        elif service == 'green':
            df.lpep_pickup_datetime=pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime=pd.to_datetime(df.lpep_dropoff_datetime)

        # df.passenger_count=pd.to_numeric(df.passenger_count, downcast='signed')       
        df['passenger_count'].fillna(0, inplace=True)
        df = df.astype({'passenger_count':'int'})

        df['payment_type'].fillna(0, inplace=True)
        df = df.astype({'payment_type':'int'})

        df['RatecodeID'].fillna(0, inplace=True)
        df['PULocationID'].fillna(0, inplace=True)
        df['DOLocationID'].fillna(0, inplace=True)
        df['VendorID'].fillna(0, inplace=True)
        df['trip_type'].fillna(0, inplace=True)

        df = df.astype({'RatecodeID':'int'})
        df = df.astype({'PULocationID':'int'})
        df = df.astype({'DOLocationID':'int'})
        df = df.astype({'VendorID':'int'})
        df = df.astype({'trip_type':'int'})


        # df = df.astype({'Discount':'int'})

        print(df.head(2))
        file_name = file_name.replace('csv.gz', 'parquet')
        df.to_parquet(file_name, engine='pyarrow')
        print(f"Parquet: {file_name}")

        # upload it to gcs 
        upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name)
        print(f"GCS: {service}/{file_name}")

web_to_gcs('2019', 'green')
web_to_gcs('2020', 'green')
# web_to_gcs('2019', 'yellow')
# web_to_gcs('2020', 'yellow')
