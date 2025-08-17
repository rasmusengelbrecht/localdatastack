---
title: Product Analysis
---

## 📊 Product Overview

```sql product_overview
select 
    count(*) as total_products,
    count(distinct product_type) as product_types,
    round(avg(product_price), 2) as avg_price,
    min(product_price) as min_price,
    max(product_price) as max_price
from jaffle_shop.products
```

<BigValue 
    data={product_overview} 
    value=total_products 
    title="Total Products"
/>
<BigValue 
    data={product_overview} 
    value=product_types 
    title="Product Categories"
/>
<BigValue 
    data={product_overview} 
    value=avg_price 
    title="Average Price"
    fmt=usd
/>

## 🏷️ Products by Category

```sql products_by_type
select 
    product_type,
    count(*) as product_count,
    round(avg(product_price), 2) as avg_price
from jaffle_shop.products
group by product_type
order by product_count desc
```

<BarChart 
    data={products_by_type} 
    x=product_type 
    y=product_count
    title="Product Count by Category"
/>

## 📋 Product Catalog

```sql product_catalog
select 
    product_id,
    product_name,
    product_type,
    product_price
from jaffle_shop.products
order by product_name
```

<DataTable data={product_catalog} search=true>
    <Column id=product_id title="Product ID" />
    <Column id=product_name title="Product Name" />
    <Column id=product_type title="Category" />
    <Column id=product_price title="Price" fmt=usd />
</DataTable>