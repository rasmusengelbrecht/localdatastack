{{
  config(
    materialized='table'
  )
}}

select
    o.order_id,
    o.customer_id,
    c.customer_name,
    c.customer_segment,
    o.order_date,
    o.order_status,
    extract(year from o.order_date) as order_year,
    extract(month from o.order_date) as order_month,
    extract(dow from o.order_date) as order_day_of_week,
    case 
        when extract(dow from o.order_date) in (0, 6) then 'Weekend'
        else 'Weekday'
    end as is_weekend

from {{ ref('stg_orders') }} o
left join {{ ref('dim_customers') }} c
    on o.customer_id = c.customer_id