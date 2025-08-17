{{
  config(
    materialized='view'
  )
}}

select
    sku as product_id,
    name as product_name,
    price::numeric as product_price,
    type as product_type,
    description,
    _dlt_load_id,
    _dlt_id

from {{ source('raw_jaffle_shop', 'products') }}