WITH

-- intermediate
green_tripdata_aggregated AS (
    SELECT * FROM {{ ref('int__green_tripdata_aggregated') }}
),

-- staging
predictions AS (
    SELECT * FROM {{ ref('stg__predictions') }}
),

-- join
green_tripdata_predictions_joined AS (
    SELECT
        -- green_tripdata_aggregated
        green_tripdata_aggregated.id,
        green_tripdata_aggregated.pickup_date,
        green_tripdata_aggregated.pickup_borough,
        green_tripdata_aggregated.trip_volume,

        -- predictions
        predictions.trip_volume AS trip_volume_forecast

    FROM green_tripdata_aggregated
    LEFT JOIN predictions ON
        green_tripdata_aggregated.pickup_date = predictions.pickup_date
        AND green_tripdata_aggregated.pickup_borough = predictions.pickup_borough
)

SELECT * FROM green_tripdata_predictions_joined