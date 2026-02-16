-- select count(*) as record_count
-- from {{ ref('fct_monthly_zone_revenue') }}


select pickup_zone, sum(revenue_monthly_total_amount) as total_revenue
from {{ ref('fct_monthly_zone_revenue') }}
where service_type = 'Green'
  and extract(year from pickup_datetime) = 2020
group by pickup_zone
order by total_revenue desc
limit 1
