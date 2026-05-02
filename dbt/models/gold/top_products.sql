{{ config(materialized='table') }}

select
    product_id,
    product_name,
    category,
    brand,
    sum(quantity) as units_sold,
    round(sum(order_total), 2) as revenue,
    count(distinct order_id) as orders
from {{ ref('stg_orders') }}
group by 1, 2, 3, 4
order by revenue desc

