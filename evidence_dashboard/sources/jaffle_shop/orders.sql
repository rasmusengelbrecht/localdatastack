select 
    order_id,
    customer_id,
    customer_name,
    customer_segment,
    order_date,
    order_status,
    order_year,
    order_month,
    order_day_of_week,
    is_weekend
from main.fct_orders
order by order_date desc