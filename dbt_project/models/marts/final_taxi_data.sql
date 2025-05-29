WITH trip AS (
    SELECT * FROM {{ ref('trip_taxi_transform') }}
),
payment AS (
    select * from {{ source('jdeol003_capstone3_gema', 'payment_type') }}
),
zone AS (
    SELECT * FROM {{ ref('taxi_zone_lookup_transform') }}
)

SELECT
    t.*,
    p.description AS payment_description,
    z.zone AS pickup_zone,
    z.borough AS pickup_borough,
    z2.zone AS dropoff_zone,
    z2.borough AS dropoff_borough
FROM trip t
LEFT JOIN payment p
    ON t.payment_type = p.payment_type
LEFT JOIN zone z
    ON t.pu_location_id = z.location_id
LEFT JOIN zone z2
    ON t.do_location_id = z2.location_id
