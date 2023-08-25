WITH

source AS (
    SELECT * FROM {{ source('default', 'taxi_zone_lookup') }}
),

transformations AS (
    SELECT
        LocationID AS location_id,
        Borough AS borough,
        Zone AS zone,
        service_zone
    FROM source
)

SELECT * FROM transformations
