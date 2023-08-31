WITH

-- intermediate
tripdata_unioned AS (
    SELECT * FROM {{ ref('int__green_yellow_tripdata_unioned') }}
),

-- staging
taxi_zone_lookup AS (
    SELECT * FROM {{ ref('stg__taxi_zone_lookup') }}
),

-- join
tripdata_zone_joined AS (
    SELECT
        -- tripdata_unioned
        tripdata_unioned.trip_id,
        tripdata_unioned.vendor_id,
        tripdata_unioned.pickup_datetime,
        tripdata_unioned.pickup_date,
        tripdata_unioned.pickup_time,
        tripdata_unioned.pickup_hour,
        tripdata_unioned.dropoff_datetime,
        tripdata_unioned.dropoff_date,
        tripdata_unioned.dropoff_time,
        tripdata_unioned.dropoff_hour,
        tripdata_unioned.trip_duration_minutes,
        tripdata_unioned.store_and_fwd_flag,
        tripdata_unioned.rate_code_id,
        tripdata_unioned.pickup_location_id,
        tripdata_unioned.dropoff_location_id,
        tripdata_unioned.passenger_count,
        tripdata_unioned.trip_distance,
        tripdata_unioned.payment_type,
        tripdata_unioned.trip_type,
        tripdata_unioned.taxi_type,
        tripdata_unioned.total_amount,
        tripdata_unioned.fare_amount,
        tripdata_unioned.extra,
        tripdata_unioned.mta_tax,
        tripdata_unioned.tip_amount,
        tripdata_unioned.tolls_amount,
        tripdata_unioned.improvement_surcharge,
        tripdata_unioned.congestion_surcharge,
        tripdata_unioned.ehail_fee,
        tripdata_unioned.airport_fee,

        -- pickup_location
        pickup_location.borough AS pickup_borough,
        pickup_location.zone AS pickup_zone,
        pickup_location.service_zone AS pickup_service_zone,

        -- dropoff_location
        dropoff_location.borough AS dropoff_borough,
        dropoff_location.zone AS dropoff_zone,
        dropoff_location.service_zone AS dropoff_service_zone,

        -- calculated
        CONCAT(pickup_borough, ' - ', dropoff_borough) AS traject_borough,
        CONCAT(pickup_zone, ' - ', dropoff_zone) AS traject_zone,
        CONCAT(pickup_service_zone, ' - ', dropoff_service_zone) AS traject_service_zone

    FROM tripdata_unioned
    LEFT JOIN taxi_zone_lookup AS pickup_location
        ON tripdata_unioned.pickup_location_id = pickup_location.location_id
    LEFT JOIN taxi_zone_lookup AS dropoff_location
        ON tripdata_unioned.dropoff_location_id = dropoff_location.location_id
)

SELECT * FROM tripdata_zone_joined