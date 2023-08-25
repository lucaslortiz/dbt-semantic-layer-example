WITH

source AS (
    SELECT * FROM {{ source('raw', 'taxi_zone_lookup') }}
),

transformations AS (
    SELECT
        LocationID,
        Borough,
        Zone,
        service_zone
    FROM source
)

SELECT * FROM transformations
