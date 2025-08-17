---
title: Customer Analysis
---

## 📈 Customer Overview

```sql customer_overview
select 
    count(*) as total_customers,
    count(case when total_orders > 0 then 1 end) as active_customers,
    round(avg(total_orders), 1) as avg_orders_per_customer,
    count(case when customer_segment = 'Frequent' then 1 end) as frequent_customers
from jaffle_shop.customers
```

<div class="grid grid-cols-2 md:grid-cols-4 gap-4">
    <BigValue 
        data={customer_overview} 
        value=total_customers 
        title="Total Customers"
    />
    <BigValue 
        data={customer_overview} 
        value=active_customers 
        title="Active Customers"
    />
    <BigValue 
        data={customer_overview} 
        value=avg_orders_per_customer 
        title="Avg Orders/Customer"
        fmt=num1
    />
    <BigValue 
        data={customer_overview} 
        value=frequent_customers 
        title="Frequent Customers"
    />
</div>

## 🎯 Customer Segments

```sql customer_segments
select 
    customer_segment,
    count(*) as customer_count
from jaffle_shop.customers
group by customer_segment
```

<BarChart 
    data={customer_segments} 
    x=customer_segment 
    y=customer_count
    title="Customer Count by Segment"
/>

## 🏆 Top Customers

```sql top_customers
select 
    customer_name,
    customer_segment,
    total_orders,
    completed_orders,
    first_order_date,
    most_recent_order_date
from jaffle_shop.customers
order by total_orders desc
limit 50
```

<DataTable data={top_customers} search=true rows=50>
    <Column id=customer_name title="Customer Name" />
    <Column id=customer_segment title="Segment" />
    <Column id=total_orders title="Total Orders" />
    <Column id=completed_orders title="Completed Orders" />
    <Column id=first_order_date title="First Order" />
    <Column id=most_recent_order_date title="Latest Order" />
</DataTable>