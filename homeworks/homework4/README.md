# Initial setup
I ran load_yellow_taxi_data.py script and created an external BigQuery table using loaded csv files

# Question 1

Answer:
int_trips_unioned only

# Question 2

I added 
- name: payment_type
        data_tests:
          - accepted_values:
              arguments:
                values: [1, 2, 3, 4, 5, 6]
                quote: false

to my marts/schema.yml file.
All existing values must be inside this list, so after adding 6 to the data the test will fail.

Answer:
dbt will fail the test, returning a non-zero exit code


# Question 3

select count(*) as record_count
from {{ ref('fct_monthly_zone_revenue') }}

Answer:
12,184

# Question 4

select
    pickup_zone,
    sum(revenue_monthly_total_amount) as total_revenue_2020
from {{ ref('fct_monthly_zone_revenue') }}
where service_type = 'Green'
  and extract(year from revenue_month) = 2020
group by pickup_zone
order by total_revenue_2020 desc

Answer:
East Harlem North


# Question 5
select
    sum(total_monthly_trips) as total_trips_oct_2019
from {{ ref('fct_monthly_zone_revenue') }}
where service_type = 'Green'
  and revenue_month = '2019-10-01'


Answer:
384,624

# Question 6
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


select count(*) as record_count
from {{ ref('stg_fhv_tripdata') }}


Answer: 
43,244,693