select 
    product_id,
    product_name,
    product_price,
    product_type,
    price_tier
from jaffle_shop.dim_products
order by product_price desc