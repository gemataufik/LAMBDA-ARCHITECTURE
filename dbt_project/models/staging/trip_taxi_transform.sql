{{ config(
    materialized='incremental',
    partition_by={
      "field": "trip_day",  
      "data_type": "date"
    },
    unique_key='trip_id'
) }}

WITH base_data AS (
    SELECT
        passenger_count,
        trip_distance,
        congestion_surcharge,
        store_and_fwd_flag,
        improvement_surcharge,
        total_amount,
        trip_type,
        payment_type,
        mta_tax,
        DOLocationID,
        lpep_pickup_datetime,
        extra,
        VendorID,
        tolls_amount,
        ehail_fee,
        RatecodeID,
        tip_amount,
        fare_amount,
        PULocationID,
        lpep_dropoff_datetime
    FROM {{ source('jdeol003_capstone3_gema', 'taxi_data') }}
    {% if is_incremental() %}
      WHERE i > (SELECT MAX(lpep_pickup_datetime) FROM {{ this }})
    {% endif %}
),

transformed_data AS (
    SELECT
        
        md5(
            concat(
                cast(lpep_pickup_datetime as string),
                cast(PULocationID as string),
                cast(DOLocationID as string)
            )
        ) AS trip_id,

        
        DATE(lpep_pickup_datetime) AS trip_day,

        passenger_count,
        trip_distance * 1.60934 AS trip_distance_km,
        congestion_surcharge,
        store_and_fwd_flag,
        improvement_surcharge,
        total_amount,
        trip_type,
        payment_type,
        mta_tax,
        DOLocationID AS do_location_id,
        lpep_pickup_datetime,
        extra,

        CASE VendorID
            WHEN 1 THEN 'grab'
            WHEN 2 THEN 'citra'
            ELSE 'unknown'
        END AS vendor_id,

        tolls_amount,
        ehail_fee,
        RatecodeID AS ratecode_id,
        tip_amount,
        fare_amount,
        PULocationID AS pu_location_id,
        lpep_dropoff_datetime,

        TIMESTAMP_DIFF(lpep_dropoff_datetime, lpep_pickup_datetime, SECOND) AS trip_duration_second
    FROM base_data
)

SELECT
    trip_id,
    trip_day,
    passenger_count,
    trip_distance_km,
    congestion_surcharge,
    store_and_fwd_flag,
    improvement_surcharge,
    total_amount,
    trip_type,
    payment_type,
    mta_tax,
    do_location_id,
    lpep_pickup_datetime,
    extra,
    vendor_id,
    tolls_amount,
    ehail_fee,
    ratecode_id,
    tip_amount,
    fare_amount,
    pu_location_id,
    lpep_dropoff_datetime,
    trip_duration_second
FROM transformed_data
