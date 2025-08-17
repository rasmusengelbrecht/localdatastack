---
title: Jaffle Shop Analytics Dashboard
---

Welcome to the Jaffle Shop Analytics Dashboard! Explore customer behavior, order patterns, and product performance.

## 📊 Key Performance Indicators

```sql overview_stats
select 
    count(distinct customer_id) as total_customers,
    count(order_id) as total_orders,
    round(count(order_id)::float / count(distinct customer_id), 1) as avg_orders_per_customer
from jaffle_shop.orders
```

<div class="grid grid-cols-3 gap-4">
    <BigValue 
        data={overview_stats} 
        value=total_customers 
        title="Total Customers"
    />
    <BigValue 
        data={overview_stats} 
        value=total_orders 
        title="Total Orders"
        fmt=num0
    />
    <BigValue 
        data={overview_stats} 
        value=avg_orders_per_customer 
        title="Avg Orders/Customer"
        fmt=num1
    />
</div>

## 📈 Monthly Order Trends

```sql monthly_orders
select 
    date_trunc('month', order_date) as month,
    count(*) as orders
from jaffle_shop.orders
group by date_trunc('month', order_date)
order by month
```

<LineChart 
    data={monthly_orders} 
    x=month 
    y=orders
    title="Monthly Order Volume"
    yAxisTitle="Number of Orders"
/>

## 👥 Customer Segments

```sql customer_segments
select 
    customer_segment,
    count(*) as customer_count
from jaffle_shop.customers
group by customer_segment
order by customer_count desc
```

<BarChart 
    data={customer_segments} 
    x=customer_segment 
    y=customer_count
    title="Customer Distribution by Segment"
/>

## 🛍️ Product Categories

```sql product_categories
select 
    product_type,
    count(*) as product_count,
    round(avg(product_price), 2) as avg_price
from jaffle_shop.products
group by product_type
order by product_count desc
```

<BarChart 
    data={product_categories} 
    x=product_type 
    y=product_count
    title="Products by Category"
/>

## 📋 Recent Orders

```sql recent_orders
select 
    *
from jaffle_shop.orders
order by order_date desc
limit 20
```

<DataTable data={recent_orders} rows=20>
    <Column id=order_id title="Order ID"/>
    <Column id=customer_name title="Customer"/>
    <Column id=customer_segment title="Segment"/>
    <Column id=order_date title="Order Date"/>
    <Column id=order_status title="Status"/>
    <Column id=order_total title="Order Total" fmt=usd/>
</DataTable>

## 🔗 Navigation

<div class="grid grid-cols-3 gap-4 mt-6">
    <div class="p-4 border rounded-lg">
        <h3 class="text-lg font-semibold">👥 Customers</h3>
        <p class="text-sm text-gray-600 mt-2">View customer data and segments</p>
        <a href="/customers" class="text-blue-600 hover:text-blue-800 mt-2 inline-block">View →</a>
    </div>
    <div class="p-4 border rounded-lg">
        <h3 class="text-lg font-semibold">📦 Orders</h3>
        <p class="text-sm text-gray-600 mt-2">Browse order history and patterns</p>
        <a href="/orders" class="text-blue-600 hover:text-blue-800 mt-2 inline-block">View →</a>
    </div>
    <div class="p-4 border rounded-lg">
        <h3 class="text-lg font-semibold">🛍️ Products</h3>
        <p class="text-sm text-gray-600 mt-2">Explore product catalog and performance</p>
        <a href="/products" class="text-blue-600 hover:text-blue-800 mt-2 inline-block">View →</a>
    </div>
</div>