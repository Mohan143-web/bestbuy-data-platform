{{ config(materialized='table') }}

select
    order_date,
    category,
    channel,
    count(distinct order_id) as orders,
    sum(quantity) as units_sold,
    round(sum(order_total), 2) as revenue,
    round(sum(gross_margin), 2) as gross_margin
from {{ ref('stg_orders') }}
group by 1, 2, 3

