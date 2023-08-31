WITH

source AS (
    SELECT * FROM {{ source('default', 'yellow_taxi_tripdata') }}
),

transformations AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['VendorID', 'tpep_pickup_datetime','tpep_dropoff_datetime','PULocationID','DOLocationID','total_amount','trip_distance','passenger_count']) }} AS trip_id,
        VendorID AS vendor_id,
        tpep_pickup_datetime AS pickup_datetime,
        DATE(pickup_datetime) AS pickup_date,
        date_format(pickup_datetime, 'HH:mm:ss') AS pickup_time,
        HOUR(pickup_datetime) AS pickup_hour,
        tpep_dropoff_datetime AS dropoff_datetime,
        DATE(dropoff_datetime) AS dropoff_date,
        date_format(dropoff_datetime, 'HH:mm:ss') AS dropoff_time,
        HOUR(dropoff_datetime) AS dropoff_hour,
        DATEDIFF(MINUTE, pickup_datetime, dropoff_datetime) AS trip_duration_minutes,
        store_and_fwd_flag,
        RatecodeID AS rate_code_id,
        PULocationID AS pickup_location_id,
        DOLocationID AS dropoff_location_id,
        passenger_count,
        trip_distance,
        payment_type,
        total_amount,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        airport_fee,
        'yellow' AS taxi_type
    FROM source
)

SELECT * FROM transformations
