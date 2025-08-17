{{
  config(
    materialized='view'
  )
}}

select
    id as store_id,
    name as store_name,
    opened_at as store_opened_date,
    _dlt_load_id,
    _dlt_id

from {{ source('raw_jaffle_shop', 'stores') }}