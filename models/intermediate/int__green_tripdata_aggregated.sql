WITH

-- staging
green_taxi_tripdata AS (
    SELECT * FROM {{ ref('stg__green_taxi_tripdata') }}
),

taxi_zone_lookup AS (
    SELECT * FROM {{ ref('stg__taxi_zone_lookup') }}
),

-- join
green_taxi_tripdata_zone_joined AS (
    SELECT
        -- tripdata_unioned
        green_taxi_tripdata.trip_id,
        green_taxi_tripdata.pickup_date,

        -- taxi_zone_lookup
        taxi_zone_lookup.borough AS pickup_borough

    FROM green_taxi_tripdata
    LEFT JOIN taxi_zone_lookup
        ON green_taxi_tripdata.pickup_location_id = taxi_zone_lookup.location_id
),

-- filter
green_tripdata_filtered AS (
    SELECT *
    FROM green_taxi_tripdata_zone_joined
    WHERE pickup_borough <> 'Unknown'
        AND pickup_date BETWEEN DATE('2021-01-01') AND DATE('2021-12-31') 
),

-- aggregate
green_tripdata_aggregated AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['pickup_date', 'pickup_borough']) }} AS id,
        pickup_date,
        pickup_borough,
        COUNT(trip_id) AS trip_volume
    FROM green_tripdata_filtered
    GROUP BY pickup_date, pickup_borough
)


SELECT * FROM green_tripdata_aggregated