---
title: Order Analysis
---

## 📊 Order Overview

```sql order_overview
select 
    count(*) as total_orders,
    count(distinct customer_id) as unique_customers,
    round(count(*)::float / count(distinct customer_id), 1) as avg_orders_per_customer,
    min(order_date) as first_order_date,
    max(order_date) as last_order_date
from jaffle_shop.orders
```

<div class="grid grid-cols-2 md:grid-cols-4 gap-4">
    <BigValue 
        data={order_overview} 
        value=total_orders 
        title="Total Orders"
        fmt=num0
    />
    <BigValue 
        data={order_overview} 
        value=unique_customers 
        title="Unique Customers"
    />
    <BigValue 
        data={order_overview} 
        value=avg_orders_per_customer 
        title="Avg Orders/Customer"
        fmt=num1
    />
    <BigValue 
        data={order_overview} 
        value=first_order_date 
        title="First Order Date"
    />
</div>

## 📅 Orders by Day of Week

```sql orders_by_dow
select 
    case order_day_of_week
        when 0 then 'Sunday'
        when 1 then 'Monday' 
        when 2 then 'Tuesday'
        when 3 then 'Wednesday'
        when 4 then 'Thursday'
        when 5 then 'Friday'
        when 6 then 'Saturday'
    end as day_of_week,
    count(*) as order_count
from jaffle_shop.orders
group by order_day_of_week
order by order_day_of_week
```

<BarChart 
    data={orders_by_dow} 
    x=day_of_week 
    y=order_count
    title="Orders by Day of Week"
/>

## 📋 Recent Orders

```sql orders_detail
select 
    order_id,
    customer_name,
    customer_segment,
    order_date,
    order_status,
    is_weekend
from jaffle_shop.orders
order by order_date desc
limit 50
```

<DataTable data={orders_detail} search=true rows=50>
    <Column id=order_id title="Order ID" />
    <Column id=customer_name title="Customer" />
    <Column id=customer_segment title="Segment" />
    <Column id=order_date title="Order Date" />
    <Column id=order_status title="Status" />
    <Column id=is_weekend title="Weekend" />
</DataTable>