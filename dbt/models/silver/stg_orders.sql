{{ config(materialized='view') }}

select
    order_id,
    cast(event_ts as timestamp) as event_ts,
    cast(event_ts as date) as order_date,
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
from {{ source('lakehouse', 'orders_silver') }}
where _dq_is_valid = true

