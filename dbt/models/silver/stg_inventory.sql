{{ config(materialized='view') }}

select
    product_id,
    sku,
    store_id,
    cast(event_ts as timestamp) as event_ts,
    on_hand,
    available_to_promise,
    reorder_point,
    change_quantity,
    reason
from {{ source('lakehouse', 'inventory_silver') }}
where _dq_is_valid = true

