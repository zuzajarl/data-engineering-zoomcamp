-- models/staging/stg_fhv_tripdata.sql
with source as (
    select *
    from {{ source('raw', 'fhv') }}
    where dispatching_base_num is not null
)

select
    dispatching_base_num,
    pickup_datetime,
    dropoff_datetime,
    PULocationID    as pickup_location_id,
    DOLocationID    as dropoff_location_id,
    SR_Flag         as sr_flag,
    Affiliated_base_number as affiliated_base_number
from source
