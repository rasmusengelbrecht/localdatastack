{{
  config(
    materialized='view'
  )
}}

select
    id as supply_id,
    name as supply_name,
    cost::numeric as supply_cost,
    perishable as is_perishable,
    _dlt_load_id,
    _dlt_id

from {{ source('raw_jaffle_shop', 'supplies') }}