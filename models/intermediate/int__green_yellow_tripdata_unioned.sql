WITH

-- staging
green_taxi_tripdata AS (
    SELECT * FROM {{ ref('stg__green_taxi_tripdata') }}
),

yellow_taxi_tripdata AS (
    SELECT * FROM {{ ref('stg__yellow_taxi_tripdata') }}
),

-- filter columns
green_columns_selected AS (
    SELECT
        trip_id,
        vendor_id,
        pickup_datetime,
        pickup_date,
        pickup_time,
        pickup_hour,
        dropoff_datetime,
        dropoff_date,
        dropoff_time,
        dropoff_hour,
        trip_duration_minutes,
        store_and_fwd_flag,
        rate_code_id,
        pickup_location_id,
        dropoff_location_id,
        passenger_count,
        trip_distance,
        payment_type,
        trip_type,
        taxi_type,
        total_amount,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        ehail_fee,
        NULL AS airport_fee
    FROM green_taxi_tripdata
),

yellow_columns_selected AS (
    SELECT
        trip_id,
        vendor_id,
        pickup_datetime,
        pickup_date,
        pickup_time,
        pickup_hour,
        dropoff_datetime,
        dropoff_date,
        dropoff_time,
        dropoff_hour,
        trip_duration_minutes,
        store_and_fwd_flag,
        rate_code_id,
        pickup_location_id,
        dropoff_location_id,
        passenger_count,
        trip_distance,
        payment_type,
        NULL AS trip_type,
        taxi_type,
        total_amount,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        NULL AS ehail_fee,
        airport_fee
    FROM yellow_taxi_tripdata
),

-- union
green_yellow_tripdata_unioned AS (
    SELECT * FROM green_columns_selected
    UNION ALL
    SELECT * FROM yellow_columns_selected
),

-- filters
tripdata_filtered AS (
    SELECT *
    FROM green_yellow_tripdata_unioned
    WHERE 
        trip_distance > 0
        AND total_amount > 0
        AND passenger_count > 0
        AND tip_amount >= 0
        AND trip_duration_minutes > 0
        AND pickup_date BETWEEN DATE('2021-01-01') AND DATE('2021-12-31') 
        AND dropoff_date BETWEEN DATE('2021-01-01') AND DATE('2021-12-31')
)

SELECT * FROM tripdata_filtered