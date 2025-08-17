{{
  config(
    materialized='table'
  )
}}

select
    product_id,
    product_name,
    product_price,
    product_type,
    case
        when product_price < 5.00 then 'Low'
        when product_price between 5.00 and 15.00 then 'Medium'
        when product_price > 15.00 then 'High'
    end as price_tier

from {{ ref('stg_products') }}