{{
  config(
    materialized='table'
  )
}}

with customer_stats as (
    select
        customer_id,
        count(order_id) as total_orders,
        count(case when order_status = 'completed' then 1 end) as completed_orders,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        max(order_date) < current_date - interval '90 days' as is_churned

    from {{ ref('int_customer_orders') }}
    where order_id is not null
    group by customer_id
)

select
    c.customer_id,
    c.customer_name,
    c.first_name,
    c.last_name,
    coalesce(cs.total_orders, 0) as total_orders,
    coalesce(cs.completed_orders, 0) as completed_orders,
    cs.first_order_date,
    cs.most_recent_order_date,
    coalesce(cs.is_churned, false) as is_churned,
    case
        when cs.customer_id is null then 'Never Ordered'
        when cs.total_orders = 1 then 'One Time'
        when cs.total_orders between 2 and 5 then 'Regular'
        when cs.total_orders > 5 then 'Frequent'
    end as customer_segment

from {{ ref('stg_customers') }} c
left join customer_stats cs
    on c.customer_id = cs.customer_id