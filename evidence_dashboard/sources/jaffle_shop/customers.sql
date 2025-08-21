select 
    customer_id,
    customer_name,
    first_name,
    last_name,
    total_orders,
    completed_orders,
    first_order_date,
    most_recent_order_date,
    is_churned,
    customer_segment
from main.dim_customers
order by total_orders desc