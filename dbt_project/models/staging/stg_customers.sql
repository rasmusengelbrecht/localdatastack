{{
  config(
    materialized='view'
  )
}}

select
    id as customer_id,
    name as customer_name,
    split_part(name, ' ', 1) as first_name,
    case 
        when length(name) - length(replace(name, ' ', '')) > 0
        then substring(name, position(' ' in name) + 1)
        else '' 
    end as last_name,
    _dlt_load_id,
    _dlt_id

from {{ source('raw_jaffle_shop', 'customers') }}