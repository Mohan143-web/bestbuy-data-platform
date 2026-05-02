{{ config(materialized='table') }}

select
    order_id,
    order_date,
    event_ts,
    user_id,
    product_id,
    product_name,
    category,
    brand,
    store_id,
    channel,
    quantity,
    unit_price,
    order_total,
    gross_margin,
    status
from {{ ref('stg_orders') }}

