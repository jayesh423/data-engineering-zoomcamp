CREATE OR REPLACE TABLE `ny-rides-jpatel.trips_data_all.yellow_tripdata_ml` (
`passenger_count` INTEGER,
`trip_distance` FLOAT64,
`PULocationID` STRING,
`DOLocationID` STRING,
`payment_type` STRING,
`fare_amount` FLOAT64,
`tolls_amount` FLOAT64,
`tip_amount` FLOAT64
) AS (
SELECT passenger_count, trip_distance, cast(PULocationID AS STRING), CAST(DOLocationID AS STRING),
CAST(payment_type AS STRING), fare_amount, tolls_amount, tip_amount
FROM `ny-rides-jpatel.trips_data_all.yellow_tripdata_partitoned` WHERE fare_amount != 0
);

select count(8) from `ny-rides-jpatel.trips_data_all.yellow_tripdata_ml`;



