{{
  config(
    materialized='view'
  )
}}

select
    id as order_id,
    customer_id,
    ordered_at::date as order_date,
    ordered_at,
    subtotal::numeric as subtotal,
    tax_paid::numeric as tax_paid,
    order_total::numeric as order_total,
    'completed' as order_status, -- All orders appear to be completed
    _dlt_load_id,
    _dlt_id

from {{ source('raw_jaffle_shop', 'orders') }}