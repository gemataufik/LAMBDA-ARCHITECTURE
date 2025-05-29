SELECT
    LocationID AS location_id,
    Borough AS borough,
    Zone AS zone,
    service_zone 
FROM {{ source('jdeol003_capstone3_gema', 'taxi_zone_lookup') }}
