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
        tripdata_unioned.*,

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